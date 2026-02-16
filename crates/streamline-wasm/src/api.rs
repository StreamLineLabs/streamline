//! REST API for managing WASM transformations
//!
//! This module provides HTTP endpoints for managing WASM transformation modules.
//!
//! ## Endpoints
//!
//! - `GET /api/v1/transformations` - List all transformations
//! - `POST /api/v1/transformations` - Register a new transformation
//! - `GET /api/v1/transformations/{id}` - Get transformation details
//! - `PUT /api/v1/transformations/{id}` - Update transformation config
//! - `DELETE /api/v1/transformations/{id}` - Unregister a transformation
//! - `POST /api/v1/transformations/{id}/reload` - Hot-reload a transformation
//! - `GET /api/v1/transformations/{id}/stats` - Get transformation statistics
//! - `POST /api/v1/transformations/{id}/test` - Test a transformation with sample input
//! - `GET /api/v1/wasm/info` - Get WASM runtime info

use super::runtime::{
    get_wasm_runtime_info, ModuleStats, RuntimeStats, WasmRuntime, WasmRuntimeInfo,
};
use super::{
    TransformInput, TransformOutput, TransformationConfig, TransformationRegistry,
    TransformationStats, TransformationType,
};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, get, post, put},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{error, info};

/// Shared state for the WASM API
#[derive(Clone)]
pub struct WasmApiState {
    /// WASM runtime
    pub runtime: Arc<WasmRuntime>,
    /// Transformation registry
    pub registry: Arc<TransformationRegistry>,
}

/// Transformation info response
#[derive(Debug, Clone, Serialize)]
pub struct TransformationInfo {
    /// Transformation ID
    pub id: String,
    /// Human-readable name
    pub name: String,
    /// Description
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Transformation type
    pub transform_type: String,
    /// Topics this applies to
    pub topics: Vec<String>,
    /// Whether enabled
    pub enabled: bool,
    /// WASM file path
    pub wasm_path: String,
    /// Entry point function
    pub entry_point: String,
    /// Timeout in milliseconds
    pub timeout_ms: u64,
    /// Max memory in bytes
    pub max_memory_bytes: usize,
    /// Whether module is loaded
    pub is_loaded: bool,
}

/// Create transformation request
#[derive(Debug, Clone, Deserialize)]
pub struct CreateTransformationRequest {
    /// Transformation ID
    pub id: String,
    /// Human-readable name
    pub name: String,
    /// Description
    #[serde(default)]
    pub description: Option<String>,
    /// WASM file path
    pub wasm_path: String,
    /// Entry point function (default: "transform")
    #[serde(default = "default_entry_point")]
    pub entry_point: String,
    /// Topics to apply to (empty = all topics)
    #[serde(default)]
    pub topics: Vec<String>,
    /// Transformation type
    #[serde(default)]
    pub transform_type: TransformationType,
    /// Whether enabled
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// Timeout in milliseconds
    #[serde(default = "default_timeout")]
    pub timeout_ms: u64,
    /// Max memory in bytes
    #[serde(default = "default_max_memory")]
    pub max_memory_bytes: usize,
    /// Additional config
    #[serde(default)]
    pub config: HashMap<String, String>,
}

fn default_entry_point() -> String {
    "transform".to_string()
}

fn default_enabled() -> bool {
    true
}

fn default_timeout() -> u64 {
    1000
}

fn default_max_memory() -> usize {
    64 * 1024 * 1024
}

/// Update transformation request
#[derive(Debug, Clone, Deserialize)]
pub struct UpdateTransformationRequest {
    /// Human-readable name
    #[serde(default)]
    pub name: Option<String>,
    /// Description
    #[serde(default)]
    pub description: Option<String>,
    /// Topics to apply to
    #[serde(default)]
    pub topics: Option<Vec<String>>,
    /// Whether enabled
    #[serde(default)]
    pub enabled: Option<bool>,
    /// Timeout in milliseconds
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    /// Max memory in bytes
    #[serde(default)]
    pub max_memory_bytes: Option<usize>,
    /// Additional config
    #[serde(default)]
    pub config: Option<HashMap<String, String>>,
}

/// Test transformation request
#[derive(Debug, Clone, Deserialize)]
pub struct TestTransformationRequest {
    /// Topic name
    pub topic: String,
    /// Partition
    #[serde(default)]
    pub partition: i32,
    /// Message key
    #[serde(default)]
    pub key: Option<String>,
    /// Message value (JSON)
    pub value: serde_json::Value,
    /// Message headers
    #[serde(default)]
    pub headers: HashMap<String, String>,
}

/// Test transformation response
#[derive(Debug, Clone, Serialize)]
pub struct TestTransformationResponse {
    /// Input that was processed
    pub input: TestInput,
    /// Output from transformation
    pub output: TestOutput,
    /// Execution time in microseconds
    pub execution_time_us: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct TestInput {
    pub topic: String,
    pub partition: i32,
    pub key: Option<String>,
    pub value_size: usize,
    pub header_count: usize,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TestOutput {
    Pass,
    Drop,
    Transformed { message_count: usize },
    Route { destination_count: usize },
    Error { message: String },
}

/// Error response
#[derive(Debug, Clone, Serialize)]
pub struct WasmApiError {
    /// Error code
    pub error: String,
    /// Error message
    pub message: String,
}

impl WasmApiError {
    fn not_found(id: &str) -> Self {
        Self {
            error: "TRANSFORMATION_NOT_FOUND".to_string(),
            message: format!("Transformation '{}' not found", id),
        }
    }

    fn already_exists(id: &str) -> Self {
        Self {
            error: "TRANSFORMATION_EXISTS".to_string(),
            message: format!("Transformation '{}' already exists", id),
        }
    }

    fn internal(msg: impl Into<String>) -> Self {
        Self {
            error: "INTERNAL_ERROR".to_string(),
            message: msg.into(),
        }
    }

    fn bad_request(msg: impl Into<String>) -> Self {
        Self {
            error: "BAD_REQUEST".to_string(),
            message: msg.into(),
        }
    }
}

// ============================================================================
// Handlers
// ============================================================================

/// Get WASM runtime info
async fn get_runtime_info() -> Json<RuntimeInfoResponse> {
    let wasm_info = get_wasm_runtime_info();
    Json(RuntimeInfoResponse {
        wasm: wasm_info,
        status: if cfg!(feature = "wasm-runtime") {
            "enabled".to_string()
        } else {
            "disabled (stub mode)".to_string()
        },
    })
}

#[derive(Debug, Serialize)]
struct RuntimeInfoResponse {
    wasm: WasmRuntimeInfo,
    status: String,
}

/// List all transformations
async fn list_transformations(State(state): State<WasmApiState>) -> Json<Vec<TransformationInfo>> {
    let configs = state.registry.list().await;
    let mut infos = Vec::new();

    for config in configs {
        let is_loaded = state.runtime.is_loaded(&config.id).await;
        infos.push(TransformationInfo {
            id: config.id,
            name: config.name,
            description: config.description,
            transform_type: config.transform_type.to_string(),
            topics: config.topics,
            enabled: config.enabled,
            wasm_path: config.wasm_path.to_string_lossy().to_string(),
            entry_point: config.entry_point,
            timeout_ms: config.timeout_ms,
            max_memory_bytes: config.max_memory_bytes,
            is_loaded,
        });
    }

    Json(infos)
}

/// Create a transformation
async fn create_transformation(
    State(state): State<WasmApiState>,
    Json(request): Json<CreateTransformationRequest>,
) -> Response {
    // Check if already exists
    if state.runtime.is_loaded(&request.id).await {
        let error = WasmApiError::already_exists(&request.id);
        return (StatusCode::CONFLICT, Json(error)).into_response();
    }

    let config = TransformationConfig {
        id: request.id.clone(),
        name: request.name,
        description: request.description,
        wasm_path: std::path::PathBuf::from(&request.wasm_path),
        entry_point: request.entry_point,
        topics: request.topics.clone(),
        transform_type: request.transform_type,
        enabled: request.enabled,
        timeout_ms: request.timeout_ms,
        max_memory_bytes: request.max_memory_bytes,
        config: request.config,
    };

    // Register in registry
    if let Err(e) = state.registry.register(config.clone()).await {
        error!(error = %e, "Failed to register transformation");
        let error = WasmApiError::internal(e.to_string());
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(error)).into_response();
    }

    // Load module
    if let Err(e) = state.runtime.load_module(config.clone()).await {
        error!(error = %e, "Failed to load WASM module");
        // Still keep it registered, just not loaded
    }

    let is_loaded = state.runtime.is_loaded(&request.id).await;

    info!(
        id = %request.id,
        is_loaded = is_loaded,
        "Transformation created"
    );

    let info = TransformationInfo {
        id: config.id,
        name: config.name,
        description: config.description,
        transform_type: config.transform_type.to_string(),
        topics: config.topics,
        enabled: config.enabled,
        wasm_path: config.wasm_path.to_string_lossy().to_string(),
        entry_point: config.entry_point,
        timeout_ms: config.timeout_ms,
        max_memory_bytes: config.max_memory_bytes,
        is_loaded,
    };

    (StatusCode::CREATED, Json(info)).into_response()
}

/// Get transformation details
async fn get_transformation(State(state): State<WasmApiState>, Path(id): Path<String>) -> Response {
    match state.runtime.get_module_config(&id).await {
        Some(config) => {
            let is_loaded = state.runtime.is_loaded(&id).await;
            let info = TransformationInfo {
                id: config.id,
                name: config.name,
                description: config.description,
                transform_type: config.transform_type.to_string(),
                topics: config.topics,
                enabled: config.enabled,
                wasm_path: config.wasm_path.to_string_lossy().to_string(),
                entry_point: config.entry_point,
                timeout_ms: config.timeout_ms,
                max_memory_bytes: config.max_memory_bytes,
                is_loaded,
            };
            (StatusCode::OK, Json(info)).into_response()
        }
        None => {
            let error = WasmApiError::not_found(&id);
            (StatusCode::NOT_FOUND, Json(error)).into_response()
        }
    }
}

/// Update transformation
async fn update_transformation(
    State(state): State<WasmApiState>,
    Path(id): Path<String>,
    Json(request): Json<UpdateTransformationRequest>,
) -> Response {
    let current = match state.runtime.get_module_config(&id).await {
        Some(c) => c,
        None => {
            let error = WasmApiError::not_found(&id);
            return (StatusCode::NOT_FOUND, Json(error)).into_response();
        }
    };

    let updated = TransformationConfig {
        id: current.id.clone(),
        name: request.name.unwrap_or(current.name),
        description: request.description.or(current.description),
        wasm_path: current.wasm_path,
        entry_point: current.entry_point,
        topics: request.topics.unwrap_or(current.topics),
        transform_type: current.transform_type,
        enabled: request.enabled.unwrap_or(current.enabled),
        timeout_ms: request.timeout_ms.unwrap_or(current.timeout_ms),
        max_memory_bytes: request.max_memory_bytes.unwrap_or(current.max_memory_bytes),
        config: request.config.unwrap_or(current.config),
    };

    if let Err(e) = state
        .runtime
        .update_module_config(&id, updated.clone())
        .await
    {
        error!(error = %e, "Failed to update transformation");
        let error = WasmApiError::internal(e.to_string());
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(error)).into_response();
    }

    let is_loaded = state.runtime.is_loaded(&id).await;

    info!(id = %id, "Transformation updated");

    let info = TransformationInfo {
        id: updated.id,
        name: updated.name,
        description: updated.description,
        transform_type: updated.transform_type.to_string(),
        topics: updated.topics,
        enabled: updated.enabled,
        wasm_path: updated.wasm_path.to_string_lossy().to_string(),
        entry_point: updated.entry_point,
        timeout_ms: updated.timeout_ms,
        max_memory_bytes: updated.max_memory_bytes,
        is_loaded,
    };

    (StatusCode::OK, Json(info)).into_response()
}

/// Delete transformation
async fn delete_transformation(
    State(state): State<WasmApiState>,
    Path(id): Path<String>,
) -> Response {
    if !state.runtime.is_loaded(&id).await {
        let error = WasmApiError::not_found(&id);
        return (StatusCode::NOT_FOUND, Json(error)).into_response();
    }

    // Unload from runtime
    if let Err(e) = state.runtime.unload_module(&id).await {
        error!(error = %e, "Failed to unload module");
    }

    // Unregister from registry
    if let Err(e) = state.registry.unregister(&id).await {
        error!(error = %e, "Failed to unregister transformation");
    }

    info!(id = %id, "Transformation deleted");

    (StatusCode::NO_CONTENT, "").into_response()
}

/// Hot-reload transformation
async fn reload_transformation(
    State(state): State<WasmApiState>,
    Path(id): Path<String>,
) -> Response {
    if !state.runtime.is_loaded(&id).await {
        let error = WasmApiError::not_found(&id);
        return (StatusCode::NOT_FOUND, Json(error)).into_response();
    }

    if let Err(e) = state.runtime.reload_module(&id).await {
        error!(error = %e, "Failed to reload module");
        let error = WasmApiError::internal(e.to_string());
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(error)).into_response();
    }

    info!(id = %id, "Transformation reloaded");

    #[derive(Serialize)]
    struct ReloadResponse {
        id: String,
        status: String,
    }

    (
        StatusCode::OK,
        Json(ReloadResponse {
            id,
            status: "reloaded".to_string(),
        }),
    )
        .into_response()
}

/// Get transformation statistics
async fn get_transformation_stats(
    State(state): State<WasmApiState>,
    Path(id): Path<String>,
) -> Response {
    match state.runtime.get_stats(&id).await {
        Some(stats) => {
            let registry_stats = state.registry.get_stats(&id).await;

            #[derive(Serialize)]
            struct StatsResponse {
                module: ModuleStats,
                #[serde(skip_serializing_if = "Option::is_none")]
                registry: Option<TransformationStats>,
            }

            (
                StatusCode::OK,
                Json(StatsResponse {
                    module: stats,
                    registry: registry_stats,
                }),
            )
                .into_response()
        }
        None => {
            let error = WasmApiError::not_found(&id);
            (StatusCode::NOT_FOUND, Json(error)).into_response()
        }
    }
}

/// Test transformation with sample input
async fn test_transformation(
    State(state): State<WasmApiState>,
    Path(id): Path<String>,
    Json(request): Json<TestTransformationRequest>,
) -> Response {
    if !state.runtime.is_loaded(&id).await {
        let error = WasmApiError::not_found(&id);
        return (StatusCode::NOT_FOUND, Json(error)).into_response();
    }

    // Serialize value to bytes
    let value_bytes = match serde_json::to_vec(&request.value) {
        Ok(v) => v,
        Err(e) => {
            let error = WasmApiError::bad_request(format!("Invalid JSON value: {}", e));
            return (StatusCode::BAD_REQUEST, Json(error)).into_response();
        }
    };

    let input = TransformInput {
        topic: request.topic.clone(),
        partition: request.partition,
        key: request.key.clone().map(|k| k.into_bytes()),
        value: value_bytes.clone(),
        headers: request
            .headers
            .iter()
            .map(|(k, v)| (k.clone(), v.clone().into_bytes()))
            .collect(),
        timestamp_ms: chrono::Utc::now().timestamp_millis(),
    };

    let start = std::time::Instant::now();
    let result = state.runtime.execute(&id, input).await;
    let execution_time_us = start.elapsed().as_micros() as u64;

    let output = match result {
        Ok(TransformOutput::Pass(_)) => TestOutput::Pass,
        Ok(TransformOutput::Drop) => TestOutput::Drop,
        Ok(TransformOutput::Transformed(msgs)) => TestOutput::Transformed {
            message_count: msgs.len(),
        },
        Ok(TransformOutput::Route(dests)) => TestOutput::Route {
            destination_count: dests.len(),
        },
        Ok(TransformOutput::Error(msg)) => TestOutput::Error { message: msg },
        Err(e) => TestOutput::Error {
            message: e.to_string(),
        },
    };

    let response = TestTransformationResponse {
        input: TestInput {
            topic: request.topic,
            partition: request.partition,
            key: request.key,
            value_size: value_bytes.len(),
            header_count: request.headers.len(),
        },
        output,
        execution_time_us,
    };

    (StatusCode::OK, Json(response)).into_response()
}

/// Get runtime statistics
async fn get_runtime_stats(State(state): State<WasmApiState>) -> Json<RuntimeStats> {
    Json(state.runtime.get_runtime_stats())
}

/// Create the WASM API router
pub fn create_wasm_router(state: WasmApiState) -> Router {
    Router::new()
        // Runtime info
        .route("/api/v1/wasm/info", get(get_runtime_info))
        .route("/api/v1/wasm/stats", get(get_runtime_stats))
        // Transformations
        .route("/api/v1/transformations", get(list_transformations))
        .route("/api/v1/transformations", post(create_transformation))
        .route("/api/v1/transformations/:id", get(get_transformation))
        .route("/api/v1/transformations/:id", put(update_transformation))
        .route("/api/v1/transformations/:id", delete(delete_transformation))
        .route(
            "/api/v1/transformations/:id/reload",
            post(reload_transformation),
        )
        .route(
            "/api/v1/transformations/:id/stats",
            get(get_transformation_stats),
        )
        .route(
            "/api/v1/transformations/:id/test",
            post(test_transformation),
        )
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_request_defaults() {
        let json = r#"{"id": "test", "name": "Test", "wasm_path": "/path/to/test.wasm"}"#;
        let request: CreateTransformationRequest = serde_json::from_str(json).unwrap();

        assert_eq!(request.id, "test");
        assert_eq!(request.entry_point, "transform");
        assert!(request.enabled);
        assert_eq!(request.timeout_ms, 1000);
    }

    #[test]
    fn test_error_responses() {
        let not_found = WasmApiError::not_found("test-id");
        assert_eq!(not_found.error, "TRANSFORMATION_NOT_FOUND");

        let already_exists = WasmApiError::already_exists("test-id");
        assert_eq!(already_exists.error, "TRANSFORMATION_EXISTS");
    }
}
