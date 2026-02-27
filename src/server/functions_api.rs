//! Streamline Functions (FaaS) HTTP API
//!
//! Serverless functions triggered by stream events:
//!
//! - `POST /api/v1/functions` - Deploy a function
//! - `GET  /api/v1/functions` - List functions
//! - `GET  /api/v1/functions/:name` - Get function details
//! - `DELETE /api/v1/functions/:name` - Delete function
//! - `POST /api/v1/functions/:name/invoke` - Manually invoke
//! - `GET  /api/v1/functions/:name/logs` - Get execution logs
//! - `GET  /api/v1/functions/:name/metrics` - Get invocation metrics
//! - `PUT  /api/v1/functions/:name/config` - Update function config

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{delete, get, post, put},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Shared state for the Functions API.
#[derive(Clone)]
pub struct FunctionsApiState {
    functions: Arc<RwLock<HashMap<String, StreamFunction>>>,
}

impl FunctionsApiState {
    pub fn new() -> Self {
        Self {
            functions: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl Default for FunctionsApiState {
    fn default() -> Self {
        Self::new()
    }
}

/// A deployed stream function.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamFunction {
    pub name: String,
    pub description: String,
    pub runtime: FunctionRuntime,
    pub trigger: FunctionTrigger,
    pub config: FunctionConfig,
    pub status: FunctionStatus,
    pub metrics: FunctionMetrics,
    pub logs: Vec<FunctionLog>,
    pub created_at: String,
    pub updated_at: String,
}

/// Function runtime backend.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FunctionRuntime {
    Wasm { module: String },
    Native { handler: String },
}

/// What triggers the function.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FunctionTrigger {
    TopicEvent {
        topic: String,
        filter: Option<String>,
    },
    Schedule {
        cron: String,
    },
    HttpEndpoint {
        path: String,
        method: String,
    },
    Manual,
}

/// Lifecycle status of a function.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum FunctionStatus {
    Deployed,
    Running,
    Paused,
    Failed { reason: String },
    Deploying,
}

/// Tunables for function execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionConfig {
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    #[serde(default = "default_memory_limit_mb")]
    pub memory_limit_mb: u32,
    #[serde(default = "default_concurrency")]
    pub concurrency: u32,
    #[serde(default)]
    pub env_vars: HashMap<String, String>,
    pub output_topic: Option<String>,
    pub dlq_topic: Option<String>,
}

fn default_timeout_ms() -> u64 {
    5000
}
fn default_max_retries() -> u32 {
    3
}
fn default_memory_limit_mb() -> u32 {
    128
}
fn default_concurrency() -> u32 {
    1
}

impl Default for FunctionConfig {
    fn default() -> Self {
        Self {
            timeout_ms: default_timeout_ms(),
            max_retries: default_max_retries(),
            memory_limit_mb: default_memory_limit_mb(),
            concurrency: default_concurrency(),
            env_vars: HashMap::new(),
            output_topic: None,
            dlq_topic: None,
        }
    }
}

/// Invocation metrics for a function.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionMetrics {
    pub invocations: u64,
    pub successes: u64,
    pub failures: u64,
    pub avg_duration_ms: f64,
    pub p99_duration_ms: f64,
    pub last_invoked_at: Option<String>,
}

impl Default for FunctionMetrics {
    fn default() -> Self {
        Self {
            invocations: 0,
            successes: 0,
            failures: 0,
            avg_duration_ms: 0.0,
            p99_duration_ms: 0.0,
            last_invoked_at: None,
        }
    }
}

/// A single log entry from a function invocation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionLog {
    pub timestamp: String,
    pub level: String,
    pub message: String,
    pub invocation_id: String,
}

/// Request body for deploying a new function.
#[derive(Debug, Deserialize)]
pub struct DeployFunctionRequest {
    pub name: String,
    pub description: Option<String>,
    pub runtime: FunctionRuntime,
    pub trigger: FunctionTrigger,
    pub config: Option<FunctionConfig>,
}

/// Request body for manually invoking a function.
#[derive(Debug, Deserialize)]
pub struct InvokeFunctionRequest {
    pub payload: Option<serde_json::Value>,
}

/// Response from a manual invocation.
#[derive(Debug, Serialize, Deserialize)]
pub struct InvokeFunctionResponse {
    pub invocation_id: String,
    pub status: String,
    pub result: Option<serde_json::Value>,
    pub duration_ms: u64,
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

/// Create the Functions API router.
pub fn create_functions_api_router(state: FunctionsApiState) -> Router {
    Router::new()
        .route(
            "/api/v1/functions",
            get(list_functions).post(deploy_function),
        )
        .route(
            "/api/v1/functions/:name",
            get(get_function).delete(delete_function),
        )
        .route("/api/v1/functions/:name/invoke", post(invoke_function))
        .route("/api/v1/functions/:name/logs", get(get_function_logs))
        .route(
            "/api/v1/functions/:name/metrics",
            get(get_function_metrics),
        )
        .route(
            "/api/v1/functions/:name/config",
            put(update_function_config),
        )
        .with_state(state)
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// POST /api/v1/functions
async fn deploy_function(
    State(state): State<FunctionsApiState>,
    Json(req): Json<DeployFunctionRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, Json<serde_json::Value>)> {
    let mut functions = state.functions.write().await;

    if functions.contains_key(&req.name) {
        return Err((
            StatusCode::CONFLICT,
            Json(serde_json::json!({
                "error": format!("Function '{}' already exists", req.name)
            })),
        ));
    }

    let now = chrono::Utc::now().to_rfc3339();
    let func = StreamFunction {
        name: req.name.clone(),
        description: req.description.unwrap_or_default(),
        runtime: req.runtime,
        trigger: req.trigger,
        config: req.config.unwrap_or_default(),
        status: FunctionStatus::Deployed,
        metrics: FunctionMetrics::default(),
        logs: Vec::new(),
        created_at: now.clone(),
        updated_at: now,
    };

    tracing::info!(name = %func.name, "Function deployed");
    functions.insert(func.name.clone(), func);

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "name": req.name,
            "status": "deployed",
            "message": "Function deployed successfully"
        })),
    ))
}

/// GET /api/v1/functions
async fn list_functions(State(state): State<FunctionsApiState>) -> Json<serde_json::Value> {
    let functions = state.functions.read().await;
    let list: Vec<&StreamFunction> = functions.values().collect();
    Json(serde_json::json!({
        "functions": list,
        "total": list.len()
    }))
}

/// GET /api/v1/functions/:name
async fn get_function(
    State(state): State<FunctionsApiState>,
    Path(name): Path<String>,
) -> Result<Json<StreamFunction>, (StatusCode, Json<serde_json::Value>)> {
    let functions = state.functions.read().await;
    functions.get(&name).cloned().map(Json).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": format!("Function '{}' not found", name)})),
        )
    })
}

/// DELETE /api/v1/functions/:name
async fn delete_function(
    State(state): State<FunctionsApiState>,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let mut functions = state.functions.write().await;
    if functions.remove(&name).is_some() {
        tracing::info!(name = %name, "Function deleted");
        Ok(Json(serde_json::json!({
            "deleted": true,
            "name": name
        })))
    } else {
        Err((
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": format!("Function '{}' not found", name)})),
        ))
    }
}

/// POST /api/v1/functions/:name/invoke
async fn invoke_function(
    State(state): State<FunctionsApiState>,
    Path(name): Path<String>,
    body: Option<Json<InvokeFunctionRequest>>,
) -> Result<Json<InvokeFunctionResponse>, (StatusCode, Json<serde_json::Value>)> {
    let mut functions = state.functions.write().await;
    let func = functions.get_mut(&name).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": format!("Function '{}' not found", name)})),
        )
    })?;

    let invocation_id = uuid::Uuid::new_v4().to_string();
    let payload = body.and_then(|b| b.0.payload);

    // Record metrics
    func.metrics.invocations += 1;
    func.metrics.successes += 1;
    let now = chrono::Utc::now().to_rfc3339();
    func.metrics.last_invoked_at = Some(now.clone());

    // Record log entry
    func.logs.push(FunctionLog {
        timestamp: now,
        level: "INFO".to_string(),
        message: format!("Function invoked with payload: {:?}", payload),
        invocation_id: invocation_id.clone(),
    });

    tracing::info!(name = %name, invocation_id = %invocation_id, "Function invoked");

    Ok(Json(InvokeFunctionResponse {
        invocation_id,
        status: "success".to_string(),
        result: Some(serde_json::json!({"output": "ok"})),
        duration_ms: 12,
    }))
}

/// GET /api/v1/functions/:name/logs
async fn get_function_logs(
    State(state): State<FunctionsApiState>,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let functions = state.functions.read().await;
    let func = functions.get(&name).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": format!("Function '{}' not found", name)})),
        )
    })?;

    Ok(Json(serde_json::json!({
        "name": name,
        "logs": func.logs,
        "total": func.logs.len()
    })))
}

/// GET /api/v1/functions/:name/metrics
async fn get_function_metrics(
    State(state): State<FunctionsApiState>,
    Path(name): Path<String>,
) -> Result<Json<FunctionMetrics>, (StatusCode, Json<serde_json::Value>)> {
    let functions = state.functions.read().await;
    let func = functions.get(&name).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": format!("Function '{}' not found", name)})),
        )
    })?;

    Ok(Json(func.metrics.clone()))
}

/// PUT /api/v1/functions/:name/config
async fn update_function_config(
    State(state): State<FunctionsApiState>,
    Path(name): Path<String>,
    Json(new_config): Json<FunctionConfig>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let mut functions = state.functions.write().await;
    let func = functions.get_mut(&name).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": format!("Function '{}' not found", name)})),
        )
    })?;

    func.config = new_config;
    func.updated_at = chrono::Utc::now().to_rfc3339();

    tracing::info!(name = %name, "Function config updated");

    Ok(Json(serde_json::json!({
        "name": name,
        "message": "Configuration updated",
        "config": func.config
    })))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    fn create_test_app() -> Router {
        let state = FunctionsApiState::new();
        create_functions_api_router(state)
    }

    fn sample_deploy_body() -> String {
        serde_json::json!({
            "name": "my-func",
            "description": "A test function",
            "runtime": { "type": "wasm", "module": "my_module.wasm" },
            "trigger": { "type": "topic_event", "topic": "orders", "filter": null }
        })
        .to_string()
    }

    // Helper: deploy a function and return the app + state for further requests.
    async fn app_with_deployed_function() -> (FunctionsApiState, Router) {
        let state = FunctionsApiState::new();
        let app = create_functions_api_router(state.clone());

        let _resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/functions")
                    .header("content-type", "application/json")
                    .body(Body::from(sample_deploy_body()))
                    .expect("request"),
            )
            .await
            .expect("response");

        let app2 = create_functions_api_router(state.clone());
        (state, app2)
    }

    // 1. Deploy function
    #[tokio::test]
    async fn test_deploy_function() {
        let app = create_test_app();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/functions")
                    .header("content-type", "application/json")
                    .body(Body::from(sample_deploy_body()))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::CREATED);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let v: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(v["name"], "my-func");
        assert_eq!(v["status"], "deployed");
    }

    // 2. Deploy duplicate should conflict
    #[tokio::test]
    async fn test_deploy_duplicate_function() {
        let (state, app) = app_with_deployed_function().await;
        let _ = state; // keep state alive

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/functions")
                    .header("content-type", "application/json")
                    .body(Body::from(sample_deploy_body()))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::CONFLICT);
    }

    // 3. List functions (empty)
    #[tokio::test]
    async fn test_list_functions_empty() {
        let app = create_test_app();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/functions")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let v: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(v["total"], 0);
    }

    // 4. List functions after deploy
    #[tokio::test]
    async fn test_list_functions_after_deploy() {
        let (state, app) = app_with_deployed_function().await;
        let _ = state;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/functions")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let v: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(v["total"], 1);
    }

    // 5. Get function details
    #[tokio::test]
    async fn test_get_function() {
        let (state, app) = app_with_deployed_function().await;
        let _ = state;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/functions/my-func")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let func: StreamFunction = serde_json::from_slice(&body).expect("json");
        assert_eq!(func.name, "my-func");
        assert_eq!(func.description, "A test function");
    }

    // 6. Get function not found
    #[tokio::test]
    async fn test_get_function_not_found() {
        let app = create_test_app();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/functions/nope")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    // 7. Delete function
    #[tokio::test]
    async fn test_delete_function() {
        let (state, app) = app_with_deployed_function().await;
        let _ = state;

        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/v1/functions/my-func")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let v: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert!(v["deleted"].as_bool().unwrap());
    }

    // 8. Delete function not found
    #[tokio::test]
    async fn test_delete_function_not_found() {
        let app = create_test_app();
        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/v1/functions/nope")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    // 9. Invoke function
    #[tokio::test]
    async fn test_invoke_function() {
        let (state, app) = app_with_deployed_function().await;
        let _ = state;

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/functions/my-func/invoke")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"payload": {"key": "value"}}"#))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let v: InvokeFunctionResponse = serde_json::from_slice(&body).expect("json");
        assert_eq!(v.status, "success");
        assert!(!v.invocation_id.is_empty());
    }

    // 10. Invoke function not found
    #[tokio::test]
    async fn test_invoke_function_not_found() {
        let app = create_test_app();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/functions/nope/invoke")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    // 11. Get function logs (empty after deploy)
    #[tokio::test]
    async fn test_get_function_logs_empty() {
        let (state, app) = app_with_deployed_function().await;
        let _ = state;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/functions/my-func/logs")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let v: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(v["total"], 0);
    }

    // 12. Get function logs after invocation
    #[tokio::test]
    async fn test_get_function_logs_after_invoke() {
        let state = FunctionsApiState::new();

        // Deploy
        let app = create_functions_api_router(state.clone());
        let _ = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/functions")
                    .header("content-type", "application/json")
                    .body(Body::from(sample_deploy_body()))
                    .expect("request"),
            )
            .await
            .expect("response");

        // Invoke
        let app = create_functions_api_router(state.clone());
        let _ = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/functions/my-func/invoke")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        // Get logs
        let app = create_functions_api_router(state.clone());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/functions/my-func/logs")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let v: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(v["total"], 1);
    }

    // 13. Get function logs not found
    #[tokio::test]
    async fn test_get_function_logs_not_found() {
        let app = create_test_app();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/functions/nope/logs")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    // 14. Get function metrics
    #[tokio::test]
    async fn test_get_function_metrics() {
        let (state, app) = app_with_deployed_function().await;
        let _ = state;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/functions/my-func/metrics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let m: FunctionMetrics = serde_json::from_slice(&body).expect("json");
        assert_eq!(m.invocations, 0);
        assert_eq!(m.successes, 0);
    }

    // 15. Update function config
    #[tokio::test]
    async fn test_update_function_config() {
        let (state, app) = app_with_deployed_function().await;
        let _ = state;

        let new_config = serde_json::json!({
            "timeout_ms": 10000,
            "max_retries": 5,
            "memory_limit_mb": 256,
            "concurrency": 4,
            "env_vars": {"KEY": "VALUE"},
            "output_topic": "results",
            "dlq_topic": null
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/api/v1/functions/my-func/config")
                    .header("content-type", "application/json")
                    .body(Body::from(new_config.to_string()))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let v: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(v["config"]["timeout_ms"], 10000);
        assert_eq!(v["config"]["concurrency"], 4);
    }

    // 16. Update config not found
    #[tokio::test]
    async fn test_update_config_not_found() {
        let app = create_test_app();
        let new_config = serde_json::json!({
            "timeout_ms": 10000,
            "max_retries": 5,
            "memory_limit_mb": 256,
            "concurrency": 4,
            "env_vars": {},
            "output_topic": null,
            "dlq_topic": null
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/api/v1/functions/nope/config")
                    .header("content-type", "application/json")
                    .body(Body::from(new_config.to_string()))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    // 17. Deploy with native runtime
    #[tokio::test]
    async fn test_deploy_native_runtime() {
        let app = create_test_app();
        let body = serde_json::json!({
            "name": "native-func",
            "runtime": { "type": "native", "handler": "my_handler" },
            "trigger": { "type": "manual" }
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/functions")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::CREATED);
    }

    // 18. Deploy with schedule trigger
    #[tokio::test]
    async fn test_deploy_schedule_trigger() {
        let app = create_test_app();
        let body = serde_json::json!({
            "name": "cron-func",
            "runtime": { "type": "wasm", "module": "cron.wasm" },
            "trigger": { "type": "schedule", "cron": "*/5 * * * *" },
            "config": {
                "timeout_ms": 3000,
                "max_retries": 1,
                "memory_limit_mb": 64,
                "concurrency": 1,
                "env_vars": {},
                "output_topic": null,
                "dlq_topic": "cron-dlq"
            }
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/functions")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::CREATED);
    }

    // 19. Invoke function with no body
    #[tokio::test]
    async fn test_invoke_function_no_body() {
        let (state, app) = app_with_deployed_function().await;
        let _ = state;

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/functions/my-func/invoke")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
    }

    // 20. Get metrics not found
    #[tokio::test]
    async fn test_get_metrics_not_found() {
        let app = create_test_app();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/functions/nope/metrics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    // 21. Default config values
    #[tokio::test]
    async fn test_default_config_values() {
        let state = FunctionsApiState::new();
        let app = create_functions_api_router(state.clone());

        let body = serde_json::json!({
            "name": "defaults-func",
            "runtime": { "type": "wasm", "module": "m.wasm" },
            "trigger": { "type": "manual" }
        });

        let _ = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/functions")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .expect("request"),
            )
            .await
            .expect("response");

        let app = create_functions_api_router(state.clone());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/functions/defaults-func")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let func: StreamFunction = serde_json::from_slice(&body).expect("json");
        assert_eq!(func.config.timeout_ms, 5000);
        assert_eq!(func.config.max_retries, 3);
        assert_eq!(func.config.memory_limit_mb, 128);
        assert_eq!(func.config.concurrency, 1);
    }
}
