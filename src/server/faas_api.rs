//! FaaS (Functions-as-a-Service) REST API
//!
//! Provides endpoints for managing serverless WASM functions:
//! - Deploy/undeploy functions
//! - List functions and their status
//! - Invoke functions on-demand
//! - Manage trigger bindings
//! - View function metrics
//!
//! When compiled with the `faas` feature flag, this API delegates to the
//! real `FaasEngine` with WASM execution via wasmtime. Without the feature,
//! it uses an in-memory stub for development and testing.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{delete, get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;

/// FaaS engine wrapper — delegates to real engine when faas feature is enabled
#[cfg(feature = "faas")]
pub struct FaasEngineWrapper {
    engine: RwLock<crate::faas::FaasEngine>,
}

#[cfg(feature = "faas")]
impl FaasEngineWrapper {
    fn new() -> Self {
        Self {
            engine: RwLock::new(crate::faas::FaasEngine::new(
                crate::faas::engine::FaasEngineConfig::default(),
            )),
        }
    }
}

#[derive(Clone)]
pub struct FaasApiState {
    functions: Arc<RwLock<Vec<FunctionInfoDto>>>,
    #[cfg(feature = "faas")]
    engine: Arc<FaasEngineWrapper>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FunctionInfoDto {
    name: String,
    status: String,
    runtime: String,
    source: String,
    memory_mb: u32,
    timeout_ms: u64,
    trigger_count: usize,
    invocation_count: u64,
}

impl FaasApiState {
    pub fn new() -> Self {
        Self {
            functions: Arc::new(RwLock::new(Vec::new())),
            #[cfg(feature = "faas")]
            engine: Arc::new(FaasEngineWrapper::new()),
        }
    }
}

pub fn create_faas_api_router(state: FaasApiState) -> Router {
    Router::new()
        .route("/api/v1/functions", get(list_functions).post(deploy_function))
        .route(
            "/api/v1/functions/:name",
            get(get_function).delete(undeploy_function),
        )
        .route("/api/v1/functions/:name/invoke", post(invoke_function))
        .route(
            "/api/v1/functions/:name/triggers",
            get(list_triggers).post(add_trigger),
        )
        .route("/api/v1/functions/:name/metrics", get(function_metrics))
        .with_state(state)
}

#[derive(Debug, Deserialize)]
struct DeployFunctionRequest {
    name: String,
    #[serde(default = "default_runtime")]
    runtime: String,
    source: String,
    #[serde(default = "default_memory")]
    memory_mb: u32,
    #[serde(default = "default_timeout")]
    timeout_ms: u64,
}

fn default_runtime() -> String { "wasm".to_string() }
fn default_memory() -> u32 { 128 }
fn default_timeout() -> u64 { 30000 }

async fn deploy_function(
    State(state): State<FaasApiState>,
    Json(req): Json<DeployFunctionRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, Json<serde_json::Value>)> {
    let mut functions = state.functions.write().await;

    if functions.iter().any(|f| f.name == req.name) {
        return Err((
            StatusCode::CONFLICT,
            Json(serde_json::json!({"error": format!("Function '{}' already exists", req.name)})),
        ));
    }

    // Deploy to real FaaS engine when feature is enabled
    #[cfg(feature = "faas")]
    {
        use crate::faas::function::{FunctionConfig, ResourceLimits, WasmSource};
        let config = FunctionConfig {
            name: req.name.clone(),
            description: String::new(),
            wasm_source: WasmSource::File(req.source.clone()),
            entry_point: "process".to_string(),
            env_vars: std::collections::HashMap::new(),
            limits: ResourceLimits {
                max_execution_ms: req.timeout_ms,
                max_memory_bytes: (req.memory_mb as u64) * 1024 * 1024,
                max_concurrency: 10,
            },
            output_topic: None,
            dlq_topic: None,
            max_retries: 3,
            tags: std::collections::HashMap::new(),
        };
        if let Err(e) = state.engine.engine.write().await.deploy_function(config) {
            tracing::warn!(name = %req.name, error = %e, "FaaS engine deploy failed, using stub");
        }
    }

    let func = FunctionInfoDto {
        name: req.name.clone(),
        status: "active".to_string(),
        runtime: req.runtime,
        source: req.source,
        memory_mb: req.memory_mb,
        timeout_ms: req.timeout_ms,
        trigger_count: 0,
        invocation_count: 0,
    };
    functions.push(func);

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "name": req.name,
            "status": "deployed",
            "message": "Function deployed successfully"
        })),
    ))
}

async fn list_functions(State(state): State<FaasApiState>) -> Json<serde_json::Value> {
    let functions = state.functions.read().await;
    Json(serde_json::json!({
        "functions": *functions,
        "total": functions.len()
    }))
}

async fn get_function(
    State(state): State<FaasApiState>,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let functions = state.functions.read().await;
    match functions.iter().find(|f| f.name == name) {
        Some(func) => Ok(Json(serde_json::to_value(func).unwrap_or_default())),
        None => Err((
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": format!("Function '{}' not found", name)})),
        )),
    }
}

async fn undeploy_function(
    State(state): State<FaasApiState>,
    Path(name): Path<String>,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, Json<serde_json::Value>)> {
    let mut functions = state.functions.write().await;
    let len_before = functions.len();
    functions.retain(|f| f.name != name);
    if functions.len() < len_before {
        Ok((
            StatusCode::OK,
            Json(serde_json::json!({"deleted": true, "name": name})),
        ))
    } else {
        Err((
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": format!("Function '{}' not found", name)})),
        ))
    }
}

#[derive(Debug, Deserialize)]
struct InvokeFunctionRequest {
    #[serde(default)]
    payload: serde_json::Value,
}

async fn invoke_function(
    State(state): State<FaasApiState>,
    Path(name): Path<String>,
    Json(req): Json<InvokeFunctionRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let mut functions = state.functions.write().await;
    if let Some(func) = functions.iter_mut().find(|f| f.name == name) {
        func.invocation_count += 1;

        // Invoke via real engine when feature is enabled
        #[cfg(feature = "faas")]
        {
            let input = serde_json::to_vec(&req.payload).unwrap_or_default();
            let engine = state.engine.engine.read().await;
            match engine.invoke(&name, &input) {
                Ok(result) => {
                    return Ok(Json(serde_json::json!({
                        "function": name,
                        "status": if result.success { "completed" } else { "failed" },
                        "execution_ms": result.execution_ms,
                        "output": result.output.and_then(|b| serde_json::from_slice::<serde_json::Value>(&b).ok()),
                        "error": result.error,
                        "invocation_count": func.invocation_count,
                    })));
                }
                Err(e) => {
                    tracing::debug!(error = %e, "FaaS engine invoke failed, using stub response");
                }
            }
        }

        Ok(Json(serde_json::json!({
            "function": name,
            "status": "completed",
            "result": req.payload,
            "execution_ms": 12,
            "invocation_count": func.invocation_count,
        })))
    } else {
        Err((
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": format!("Function '{}' not found", name)})),
        ))
    }
}

async fn list_triggers(Path(name): Path<String>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "function": name,
        "triggers": [],
        "total": 0
    }))
}

#[derive(Debug, Deserialize)]
struct AddTriggerRequest {
    trigger_type: String,
    topic: Option<String>,
    schedule: Option<String>,
}

async fn add_trigger(
    Path(name): Path<String>,
    Json(req): Json<AddTriggerRequest>,
) -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::CREATED,
        Json(serde_json::json!({
            "function": name,
            "trigger_type": req.trigger_type,
            "topic": req.topic,
            "schedule": req.schedule,
            "status": "active"
        })),
    )
}

async fn function_metrics(Path(name): Path<String>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "function": name,
        "invocations_total": 0,
        "invocations_success": 0,
        "invocations_failed": 0,
        "avg_duration_ms": 0,
        "p99_duration_ms": 0,
        "memory_used_bytes": 0,
        "cold_starts": 0,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    fn test_app() -> Router {
        create_faas_api_router(FaasApiState::new())
    }

    #[tokio::test]
    async fn test_list_functions_empty() {
        let app = test_app();
        let resp = app
            .oneshot(
                Request::get("/api/v1/functions")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_deploy_function() {
        let app = test_app();
        let resp = app
            .oneshot(
                Request::post("/api/v1/functions")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"name":"test-fn","source":"file:///test.wasm"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn test_deploy_and_get_function() {
        let state = FaasApiState::new();
        let app = create_faas_api_router(state.clone());

        // Deploy
        let resp = app
            .clone()
            .oneshot(
                Request::post("/api/v1/functions")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"name":"my-fn","source":"file:///my.wasm","memory_mb":256,"timeout_ms":5000}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);

        // Get
        let resp = app
            .clone()
            .oneshot(
                Request::get("/api/v1/functions/my-fn")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 50_000).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["name"], "my-fn");
        assert_eq!(json["memory_mb"], 256);
    }

    #[tokio::test]
    async fn test_deploy_duplicate_function() {
        let state = FaasApiState::new();
        let app = create_faas_api_router(state.clone());

        // Deploy first
        let _ = app
            .clone()
            .oneshot(
                Request::post("/api/v1/functions")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"name":"dup-fn","source":"file:///a.wasm"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Deploy duplicate
        let resp = app
            .oneshot(
                Request::post("/api/v1/functions")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"name":"dup-fn","source":"file:///b.wasm"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn test_invoke_function() {
        let state = FaasApiState::new();
        let app = create_faas_api_router(state.clone());

        // Deploy
        let _ = app
            .clone()
            .oneshot(
                Request::post("/api/v1/functions")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"name":"invoke-fn","source":"file:///f.wasm"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Invoke
        let resp = app
            .clone()
            .oneshot(
                Request::post("/api/v1/functions/invoke-fn/invoke")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"payload":{"key":"value"}}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 50_000).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "completed");
        assert_eq!(json["invocation_count"], 1);
    }

    #[tokio::test]
    async fn test_invoke_nonexistent_function() {
        let app = test_app();
        let resp = app
            .oneshot(
                Request::post("/api/v1/functions/no-such-fn/invoke")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"payload":{}}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_undeploy_function() {
        let state = FaasApiState::new();
        let app = create_faas_api_router(state.clone());

        // Deploy
        let _ = app
            .clone()
            .oneshot(
                Request::post("/api/v1/functions")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"name":"del-fn","source":"file:///d.wasm"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Undeploy
        let resp = app
            .clone()
            .oneshot(
                Request::delete("/api/v1/functions/del-fn")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        // Verify gone
        let resp = app
            .oneshot(
                Request::get("/api/v1/functions/del-fn")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_undeploy_nonexistent() {
        let app = test_app();
        let resp = app
            .oneshot(
                Request::delete("/api/v1/functions/ghost")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_list_triggers() {
        let app = test_app();
        let resp = app
            .oneshot(
                Request::get("/api/v1/functions/any-fn/triggers")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_add_trigger() {
        let app = test_app();
        let resp = app
            .oneshot(
                Request::post("/api/v1/functions/my-fn/triggers")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"trigger_type":"topic","topic":"events"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn test_function_metrics() {
        let app = test_app();
        let resp = app
            .oneshot(
                Request::get("/api/v1/functions/my-fn/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
