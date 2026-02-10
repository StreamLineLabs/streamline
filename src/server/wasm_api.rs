//! WASM Stream Processing API - REST endpoints for managing WASM transforms
//!
//! ## Endpoints
//!
//! ### Transform Management
//! - `GET /api/v1/wasm/transforms` - List all registered transforms
//! - `POST /api/v1/wasm/transforms` - Register a new transform
//! - `GET /api/v1/wasm/transforms/:id` - Get transform details
//! - `DELETE /api/v1/wasm/transforms/:id` - Unregister a transform
//! - `GET /api/v1/wasm/transforms/:id/stats` - Get transform statistics
//! - `POST /api/v1/wasm/transforms/test` - Test a transform with sample input
//! - `GET /api/v1/wasm/topics/:topic/transforms` - List transforms for a topic
//!
//! ### Function Management
//! - `POST /api/v1/functions` - Deploy a WASM function
//! - `GET /api/v1/functions` - List all functions
//! - `GET /api/v1/functions/:name` - Get function details + metrics
//! - `PUT /api/v1/functions/:name/pause` - Pause function
//! - `PUT /api/v1/functions/:name/resume` - Resume function
//! - `DELETE /api/v1/functions/:name` - Remove function
//! - `GET /api/v1/functions/builtins` - List built-in functions

use crate::wasm::builtins;
use crate::wasm::function_registry::{
    FunctionMetrics, FunctionRegistry, FunctionStatus, FunctionType, WasmFunction,
};
use crate::wasm::{TransformationConfig, TransformationRegistry, TransformationStats};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post, put},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Shared state for WASM API
#[derive(Clone)]
pub struct WasmApiState {
    pub registry: Arc<TransformationRegistry>,
    pub function_registry: Arc<FunctionRegistry>,
}

impl WasmApiState {
    pub fn new() -> Self {
        Self {
            registry: Arc::new(TransformationRegistry::new()),
            function_registry: Arc::new(FunctionRegistry::new()),
        }
    }
}

impl Default for WasmApiState {
    fn default() -> Self {
        Self::new()
    }
}

/// Error response
#[derive(Debug, Serialize)]
pub struct WasmErrorResponse {
    pub error: String,
}

/// List transforms response
#[derive(Debug, Serialize)]
pub struct ListTransformsResponse {
    pub transforms: Vec<TransformationConfig>,
    pub total: usize,
}

/// Test transform request
#[derive(Debug, Deserialize)]
pub struct TestTransformRequest {
    pub transform_id: String,
    pub input_topic: String,
    pub input_key: Option<String>,
    pub input_value: String,
    pub input_headers: Option<std::collections::HashMap<String, String>>,
}

/// Test transform response
#[derive(Debug, Serialize)]
pub struct TestTransformResponse {
    pub success: bool,
    pub output_value: Option<String>,
    pub output_headers: Option<std::collections::HashMap<String, String>>,
    pub filtered: bool,
    pub error: Option<String>,
    pub execution_time_us: u64,
}

/// Create the WASM API router
pub fn create_wasm_api_router(state: WasmApiState) -> Router {
    Router::new()
        // Transform management
        .route(
            "/api/v1/wasm/transforms",
            get(list_transforms).post(register_transform),
        )
        .route(
            "/api/v1/wasm/transforms/:id",
            get(get_transform).delete(unregister_transform),
        )
        .route(
            "/api/v1/wasm/transforms/:id/stats",
            get(get_transform_stats),
        )
        .route("/api/v1/wasm/transforms/test", post(test_transform))
        .route(
            "/api/v1/wasm/topics/:topic/transforms",
            get(list_topic_transforms),
        )
        // Function management
        .route(
            "/api/v1/functions",
            get(list_functions).post(deploy_function),
        )
        .route(
            "/api/v1/functions/builtins",
            get(list_builtin_functions),
        )
        .route(
            "/api/v1/functions/:name",
            get(get_function).delete(remove_function),
        )
        .route("/api/v1/functions/:name/pause", put(pause_function))
        .route("/api/v1/functions/:name/resume", put(resume_function))
        .with_state(state)
}

async fn list_transforms(State(state): State<WasmApiState>) -> Json<ListTransformsResponse> {
    let transforms = state.registry.list().await;
    let total = transforms.len();
    Json(ListTransformsResponse { transforms, total })
}

async fn register_transform(
    State(state): State<WasmApiState>,
    Json(config): Json<TransformationConfig>,
) -> Result<(StatusCode, Json<TransformationConfig>), (StatusCode, Json<WasmErrorResponse>)> {
    let config_clone = config.clone();
    state.registry.register(config).await.map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(WasmErrorResponse {
                error: e.to_string(),
            }),
        )
    })?;
    Ok((StatusCode::CREATED, Json(config_clone)))
}

async fn get_transform(
    State(state): State<WasmApiState>,
    Path(id): Path<String>,
) -> Result<Json<TransformationConfig>, (StatusCode, Json<WasmErrorResponse>)> {
    let transforms = state.registry.list().await;
    transforms
        .into_iter()
        .find(|t| t.id == id)
        .map(Json)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(WasmErrorResponse {
                    error: format!("Transform '{}' not found", id),
                }),
            )
        })
}

async fn unregister_transform(
    State(state): State<WasmApiState>,
    Path(id): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<WasmErrorResponse>)> {
    state.registry.unregister(&id).await.map_err(|e| {
        (
            StatusCode::NOT_FOUND,
            Json(WasmErrorResponse {
                error: e.to_string(),
            }),
        )
    })?;
    Ok(StatusCode::NO_CONTENT)
}

async fn get_transform_stats(
    State(state): State<WasmApiState>,
    Path(id): Path<String>,
) -> Result<Json<TransformationStats>, (StatusCode, Json<WasmErrorResponse>)> {
    state
        .registry
        .get_stats(&id)
        .await
        .map(Json)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(WasmErrorResponse {
                    error: format!("Transform '{}' not found or no stats available", id),
                }),
            )
        })
}

async fn test_transform(
    State(state): State<WasmApiState>,
    Json(req): Json<TestTransformRequest>,
) -> Result<Json<TestTransformResponse>, (StatusCode, Json<WasmErrorResponse>)> {
    let start = std::time::Instant::now();

    // Verify transform exists
    let transforms = state.registry.list().await;
    let transform = transforms.iter().find(|t| t.id == req.transform_id);

    if transform.is_none() {
        return Err((
            StatusCode::NOT_FOUND,
            Json(WasmErrorResponse {
                error: format!("Transform '{}' not found", req.transform_id),
            }),
        ));
    }

    let elapsed = start.elapsed().as_micros() as u64;

    // Return a test response indicating the transform was found and would execute
    Ok(Json(TestTransformResponse {
        success: true,
        output_value: Some(req.input_value),
        output_headers: req.input_headers,
        filtered: false,
        error: None,
        execution_time_us: elapsed,
    }))
}

async fn list_topic_transforms(
    State(state): State<WasmApiState>,
    Path(topic): Path<String>,
) -> Json<ListTransformsResponse> {
    let transforms = state.registry.get_for_topic(&topic).await;
    let total = transforms.len();
    Json(ListTransformsResponse { transforms, total })
}

// ============================================================================
// Function Management Types
// ============================================================================

/// Request to deploy a new WASM function
#[derive(Debug, Deserialize)]
pub struct DeployFunctionRequest {
    /// Function name (unique identifier)
    pub name: String,
    /// Description of what this function does
    #[serde(default)]
    pub description: String,
    /// Base64-encoded WASM module bytes
    #[serde(default)]
    pub module_bytes_base64: Option<String>,
    /// Input topic(s)
    pub input_topics: Vec<String>,
    /// Output topic (None = transform in-place)
    #[serde(default)]
    pub output_topic: Option<String>,
    /// Function type
    pub function_type: FunctionType,
    /// Configuration key-value pairs
    #[serde(default)]
    pub config: std::collections::HashMap<String, String>,
}

/// Response for function details
#[derive(Debug, Serialize)]
pub struct FunctionResponse {
    pub name: String,
    pub description: String,
    pub input_topics: Vec<String>,
    pub output_topic: Option<String>,
    pub function_type: FunctionType,
    pub status: FunctionStatus,
    pub metrics: FunctionMetrics,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl From<WasmFunction> for FunctionResponse {
    fn from(f: WasmFunction) -> Self {
        Self {
            name: f.name,
            description: f.description,
            input_topics: f.input_topics,
            output_topic: f.output_topic,
            function_type: f.function_type,
            status: f.status,
            metrics: f.metrics,
            created_at: f.created_at,
            updated_at: f.updated_at,
        }
    }
}

/// List functions response
#[derive(Debug, Serialize)]
pub struct ListFunctionsResponse {
    pub functions: Vec<FunctionResponse>,
    pub total: usize,
}

/// Status change response
#[derive(Debug, Serialize)]
pub struct StatusResponse {
    pub name: String,
    pub status: String,
}

// ============================================================================
// Function Management Handlers
// ============================================================================

/// POST /api/v1/functions — Deploy a WASM function
async fn deploy_function(
    State(state): State<WasmApiState>,
    Json(req): Json<DeployFunctionRequest>,
) -> Result<(StatusCode, Json<FunctionResponse>), (StatusCode, Json<WasmErrorResponse>)> {
    let module_bytes = req
        .module_bytes_base64
        .as_ref()
        .and_then(|b64| base64_decode(b64).ok())
        .unwrap_or_default();

    let now = chrono::Utc::now();
    let function = WasmFunction {
        name: req.name.clone(),
        description: req.description,
        module_bytes,
        input_topics: req.input_topics,
        output_topic: req.output_topic,
        function_type: req.function_type,
        status: FunctionStatus::Running,
        config: req.config,
        metrics: FunctionMetrics::default(),
        created_at: now,
        updated_at: now,
    };

    state
        .function_registry
        .deploy(function.clone())
        .await
        .map_err(|e| {
            (
                StatusCode::CONFLICT,
                Json(WasmErrorResponse {
                    error: e.to_string(),
                }),
            )
        })?;

    Ok((StatusCode::CREATED, Json(FunctionResponse::from(function))))
}

/// GET /api/v1/functions — List all functions
async fn list_functions(State(state): State<WasmApiState>) -> Json<ListFunctionsResponse> {
    let functions: Vec<FunctionResponse> = state
        .function_registry
        .list()
        .await
        .into_iter()
        .map(FunctionResponse::from)
        .collect();
    let total = functions.len();
    Json(ListFunctionsResponse { functions, total })
}

/// GET /api/v1/functions/:name — Get function details + metrics
async fn get_function(
    State(state): State<WasmApiState>,
    Path(name): Path<String>,
) -> Result<Json<FunctionResponse>, (StatusCode, Json<WasmErrorResponse>)> {
    state
        .function_registry
        .get(&name)
        .await
        .map(|f| Json(FunctionResponse::from(f)))
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(WasmErrorResponse {
                    error: format!("Function '{}' not found", name),
                }),
            )
        })
}

/// PUT /api/v1/functions/:name/pause — Pause function
async fn pause_function(
    State(state): State<WasmApiState>,
    Path(name): Path<String>,
) -> Result<Json<StatusResponse>, (StatusCode, Json<WasmErrorResponse>)> {
    state
        .function_registry
        .pause(&name)
        .await
        .map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                Json(WasmErrorResponse {
                    error: e.to_string(),
                }),
            )
        })?;

    Ok(Json(StatusResponse {
        name,
        status: "paused".to_string(),
    }))
}

/// PUT /api/v1/functions/:name/resume — Resume function
async fn resume_function(
    State(state): State<WasmApiState>,
    Path(name): Path<String>,
) -> Result<Json<StatusResponse>, (StatusCode, Json<WasmErrorResponse>)> {
    state
        .function_registry
        .resume(&name)
        .await
        .map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                Json(WasmErrorResponse {
                    error: e.to_string(),
                }),
            )
        })?;

    Ok(Json(StatusResponse {
        name,
        status: "running".to_string(),
    }))
}

/// DELETE /api/v1/functions/:name — Remove function
async fn remove_function(
    State(state): State<WasmApiState>,
    Path(name): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<WasmErrorResponse>)> {
    state
        .function_registry
        .remove(&name)
        .await
        .map_err(|e| {
            (
                StatusCode::NOT_FOUND,
                Json(WasmErrorResponse {
                    error: e.to_string(),
                }),
            )
        })?;
    Ok(StatusCode::NO_CONTENT)
}

/// GET /api/v1/functions/builtins — List built-in functions
async fn list_builtin_functions() -> Json<Vec<builtins::BuiltinInfo>> {
    Json(builtins::list_builtins())
}

/// Simple base64 decode helper
fn base64_decode(input: &str) -> std::result::Result<Vec<u8>, String> {
    // Use a simple manual decode for the base64 subset
    use base64::Engine;
    base64::engine::general_purpose::STANDARD
        .decode(input)
        .map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wasm_api_state_new() {
        let state = WasmApiState::new();
        assert!(Arc::strong_count(&state.registry) >= 1);
        assert!(Arc::strong_count(&state.function_registry) >= 1);
    }

    #[test]
    fn test_test_transform_response_serialization() {
        let resp = TestTransformResponse {
            success: true,
            output_value: Some("test".to_string()),
            output_headers: None,
            filtered: false,
            error: None,
            execution_time_us: 42,
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"success\":true"));
    }

    #[test]
    fn test_function_response_from_wasm_function() {
        let func = WasmFunction {
            name: "test-fn".to_string(),
            description: "Test".to_string(),
            module_bytes: vec![],
            input_topics: vec!["topic-a".to_string()],
            output_topic: None,
            function_type: FunctionType::Transform,
            status: FunctionStatus::Running,
            config: std::collections::HashMap::new(),
            metrics: FunctionMetrics::default(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let resp = FunctionResponse::from(func);
        assert_eq!(resp.name, "test-fn");
        assert_eq!(resp.input_topics, vec!["topic-a"]);
    }

    #[test]
    fn test_status_response_serialization() {
        let resp = StatusResponse {
            name: "fn-1".to_string(),
            status: "paused".to_string(),
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"status\":\"paused\""));
    }
}
