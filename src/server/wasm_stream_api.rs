//! WASM Stream Processing Runtime API
//!
//! REST endpoints for applying WASM transforms inline on streaming data.
//!
//! ## Endpoints
//!
//! ### Transform Management
//! - `POST /api/v1/streams/transforms` - Apply a WASM transform to a stream
//! - `GET /api/v1/streams/transforms` - List active transforms
//! - `GET /api/v1/streams/transforms/:id` - Get transform status
//! - `DELETE /api/v1/streams/transforms/:id` - Remove transform
//! - `POST /api/v1/streams/transforms/:id/pause` - Pause transform
//! - `POST /api/v1/streams/transforms/:id/resume` - Resume transform
//! - `GET /api/v1/streams/transforms/:id/metrics` - Get transform metrics
//!
//! ### Pipeline Management
//! - `POST /api/v1/streams/pipelines` - Create multi-step pipeline
//! - `GET /api/v1/streams/pipelines` - List pipelines
//! - `GET /api/v1/streams/pipelines/:id` - Get pipeline details
//! - `DELETE /api/v1/streams/pipelines/:id` - Delete pipeline

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{delete, get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing;

// ============================================================================
// Types
// ============================================================================

/// Shared state for the WASM stream processing API.
#[derive(Clone)]
pub struct WasmStreamApiState {
    transforms: Arc<RwLock<HashMap<String, ActiveTransform>>>,
    pipelines: Arc<RwLock<HashMap<String, StreamPipeline>>>,
}

impl WasmStreamApiState {
    pub fn new() -> Self {
        Self {
            transforms: Arc::new(RwLock::new(HashMap::new())),
            pipelines: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl Default for WasmStreamApiState {
    fn default() -> Self {
        Self::new()
    }
}

/// An active WASM transform applied to a stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActiveTransform {
    pub id: String,
    pub name: String,
    pub source_topic: String,
    pub target_topic: String,
    pub wasm_module: String,
    pub config: HashMap<String, String>,
    pub status: TransformStatus,
    pub metrics: TransformMetrics,
    pub created_at: String,
}

/// Current status of a transform or pipeline.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "state", content = "detail")]
pub enum TransformStatus {
    Running,
    Paused,
    Error(String),
    Starting,
}

/// Runtime metrics for a transform.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformMetrics {
    pub records_in: u64,
    pub records_out: u64,
    pub records_filtered: u64,
    pub records_errored: u64,
    pub avg_latency_us: f64,
    pub last_processed_at: Option<String>,
}

impl Default for TransformMetrics {
    fn default() -> Self {
        Self {
            records_in: 0,
            records_out: 0,
            records_filtered: 0,
            records_errored: 0,
            avg_latency_us: 0.0,
            last_processed_at: None,
        }
    }
}

/// A multi-step streaming pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamPipeline {
    pub id: String,
    pub name: String,
    pub source_topic: String,
    pub sink_topic: String,
    pub steps: Vec<PipelineStep>,
    pub status: TransformStatus,
    pub created_at: String,
}

/// A single step within a pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineStep {
    pub order: u32,
    pub transform_name: String,
    pub config: HashMap<String, String>,
}

// ============================================================================
// Request / Response types
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct CreateTransformRequest {
    pub name: String,
    pub source_topic: String,
    pub target_topic: String,
    pub wasm_module: String,
    #[serde(default)]
    pub config: Option<HashMap<String, String>>,
}

#[derive(Debug, Deserialize)]
pub struct CreatePipelineRequest {
    pub name: String,
    pub source_topic: String,
    pub sink_topic: String,
    pub steps: Vec<PipelineStep>,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

#[derive(Debug, Serialize)]
struct ListTransformsResponse {
    transforms: Vec<ActiveTransform>,
    total: usize,
}

#[derive(Debug, Serialize)]
struct ListPipelinesResponse {
    pipelines: Vec<StreamPipeline>,
    total: usize,
}

#[derive(Debug, Serialize)]
struct MessageResponse {
    message: String,
}

// ============================================================================
// Router
// ============================================================================

/// Create the WASM stream processing API router.
pub fn create_wasm_stream_api_router(state: WasmStreamApiState) -> Router {
    Router::new()
        // Transform endpoints
        .route(
            "/api/v1/streams/transforms",
            get(list_transforms).post(create_transform),
        )
        .route(
            "/api/v1/streams/transforms/:id",
            get(get_transform).delete(delete_transform),
        )
        .route(
            "/api/v1/streams/transforms/:id/pause",
            post(pause_transform),
        )
        .route(
            "/api/v1/streams/transforms/:id/resume",
            post(resume_transform),
        )
        .route(
            "/api/v1/streams/transforms/:id/metrics",
            get(get_transform_metrics),
        )
        // Pipeline endpoints
        .route(
            "/api/v1/streams/pipelines",
            get(list_pipelines).post(create_pipeline),
        )
        .route(
            "/api/v1/streams/pipelines/:id",
            get(get_pipeline).delete(delete_pipeline),
        )
        .with_state(state)
}

// ============================================================================
// Transform handlers
// ============================================================================

async fn create_transform(
    State(state): State<WasmStreamApiState>,
    Json(req): Json<CreateTransformRequest>,
) -> Result<(StatusCode, Json<ActiveTransform>), (StatusCode, Json<ErrorResponse>)> {
    let id = uuid::Uuid::new_v4().to_string();
    let now = chrono::Utc::now().to_rfc3339();

    let transform = ActiveTransform {
        id: id.clone(),
        name: req.name.clone(),
        source_topic: req.source_topic,
        target_topic: req.target_topic,
        wasm_module: req.wasm_module,
        config: req.config.unwrap_or_default(),
        status: TransformStatus::Starting,
        metrics: TransformMetrics::default(),
        created_at: now,
    };

    let mut transforms = state.transforms.write().await;

    if transforms.values().any(|t| t.name == req.name) {
        return Err((
            StatusCode::CONFLICT,
            Json(ErrorResponse {
                error: format!("Transform with name '{}' already exists", req.name),
            }),
        ));
    }

    tracing::info!(id = %id, name = %transform.name, "Creating WASM stream transform");
    transforms.insert(id, transform.clone());

    Ok((StatusCode::CREATED, Json(transform)))
}

async fn list_transforms(
    State(state): State<WasmStreamApiState>,
) -> Json<ListTransformsResponse> {
    let transforms = state.transforms.read().await;
    let list: Vec<ActiveTransform> = transforms.values().cloned().collect();
    let total = list.len();
    Json(ListTransformsResponse {
        transforms: list,
        total,
    })
}

async fn get_transform(
    State(state): State<WasmStreamApiState>,
    Path(id): Path<String>,
) -> Result<Json<ActiveTransform>, (StatusCode, Json<ErrorResponse>)> {
    let transforms = state.transforms.read().await;
    transforms
        .get(&id)
        .cloned()
        .map(Json)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: format!("Transform '{}' not found", id),
                }),
            )
        })
}

async fn delete_transform(
    State(state): State<WasmStreamApiState>,
    Path(id): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let mut transforms = state.transforms.write().await;
    if transforms.remove(&id).is_some() {
        tracing::info!(id = %id, "Deleted WASM stream transform");
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Transform '{}' not found", id),
            }),
        ))
    }
}

async fn pause_transform(
    State(state): State<WasmStreamApiState>,
    Path(id): Path<String>,
) -> Result<Json<ActiveTransform>, (StatusCode, Json<ErrorResponse>)> {
    let mut transforms = state.transforms.write().await;
    let transform = transforms.get_mut(&id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Transform '{}' not found", id),
            }),
        )
    })?;

    match &transform.status {
        TransformStatus::Running | TransformStatus::Starting => {
            transform.status = TransformStatus::Paused;
            tracing::info!(id = %id, "Paused WASM stream transform");
            Ok(Json(transform.clone()))
        }
        TransformStatus::Paused => Err((
            StatusCode::CONFLICT,
            Json(ErrorResponse {
                error: "Transform is already paused".to_string(),
            }),
        )),
        TransformStatus::Error(e) => Err((
            StatusCode::CONFLICT,
            Json(ErrorResponse {
                error: format!("Cannot pause transform in error state: {}", e),
            }),
        )),
    }
}

async fn resume_transform(
    State(state): State<WasmStreamApiState>,
    Path(id): Path<String>,
) -> Result<Json<ActiveTransform>, (StatusCode, Json<ErrorResponse>)> {
    let mut transforms = state.transforms.write().await;
    let transform = transforms.get_mut(&id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Transform '{}' not found", id),
            }),
        )
    })?;

    match &transform.status {
        TransformStatus::Paused => {
            transform.status = TransformStatus::Running;
            tracing::info!(id = %id, "Resumed WASM stream transform");
            Ok(Json(transform.clone()))
        }
        TransformStatus::Running => Err((
            StatusCode::CONFLICT,
            Json(ErrorResponse {
                error: "Transform is already running".to_string(),
            }),
        )),
        TransformStatus::Starting => Err((
            StatusCode::CONFLICT,
            Json(ErrorResponse {
                error: "Transform is still starting".to_string(),
            }),
        )),
        TransformStatus::Error(e) => Err((
            StatusCode::CONFLICT,
            Json(ErrorResponse {
                error: format!("Cannot resume transform in error state: {}", e),
            }),
        )),
    }
}

async fn get_transform_metrics(
    State(state): State<WasmStreamApiState>,
    Path(id): Path<String>,
) -> Result<Json<TransformMetrics>, (StatusCode, Json<ErrorResponse>)> {
    let transforms = state.transforms.read().await;
    transforms
        .get(&id)
        .map(|t| Json(t.metrics.clone()))
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: format!("Transform '{}' not found", id),
                }),
            )
        })
}

// ============================================================================
// Pipeline handlers
// ============================================================================

async fn create_pipeline(
    State(state): State<WasmStreamApiState>,
    Json(req): Json<CreatePipelineRequest>,
) -> Result<(StatusCode, Json<StreamPipeline>), (StatusCode, Json<ErrorResponse>)> {
    if req.steps.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "Pipeline must have at least one step".to_string(),
            }),
        ));
    }

    let id = uuid::Uuid::new_v4().to_string();
    let now = chrono::Utc::now().to_rfc3339();

    let pipeline = StreamPipeline {
        id: id.clone(),
        name: req.name.clone(),
        source_topic: req.source_topic,
        sink_topic: req.sink_topic,
        steps: req.steps,
        status: TransformStatus::Starting,
        created_at: now,
    };

    let mut pipelines = state.pipelines.write().await;

    if pipelines.values().any(|p| p.name == req.name) {
        return Err((
            StatusCode::CONFLICT,
            Json(ErrorResponse {
                error: format!("Pipeline with name '{}' already exists", req.name),
            }),
        ));
    }

    tracing::info!(id = %id, name = %pipeline.name, "Creating stream pipeline");
    pipelines.insert(id, pipeline.clone());

    Ok((StatusCode::CREATED, Json(pipeline)))
}

async fn list_pipelines(
    State(state): State<WasmStreamApiState>,
) -> Json<ListPipelinesResponse> {
    let pipelines = state.pipelines.read().await;
    let list: Vec<StreamPipeline> = pipelines.values().cloned().collect();
    let total = list.len();
    Json(ListPipelinesResponse {
        pipelines: list,
        total,
    })
}

async fn get_pipeline(
    State(state): State<WasmStreamApiState>,
    Path(id): Path<String>,
) -> Result<Json<StreamPipeline>, (StatusCode, Json<ErrorResponse>)> {
    let pipelines = state.pipelines.read().await;
    pipelines
        .get(&id)
        .cloned()
        .map(Json)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: format!("Pipeline '{}' not found", id),
                }),
            )
        })
}

async fn delete_pipeline(
    State(state): State<WasmStreamApiState>,
    Path(id): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let mut pipelines = state.pipelines.write().await;
    if pipelines.remove(&id).is_some() {
        tracing::info!(id = %id, "Deleted stream pipeline");
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Pipeline '{}' not found", id),
            }),
        ))
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{self, Request};
    use tower::ServiceExt;

    fn test_state() -> WasmStreamApiState {
        WasmStreamApiState::new()
    }

    fn app() -> Router {
        create_wasm_stream_api_router(test_state())
    }

    // ------------------------------------------------------------------
    // State / type unit tests
    // ------------------------------------------------------------------

    #[test]
    fn test_state_new() {
        let state = WasmStreamApiState::new();
        assert!(Arc::strong_count(&state.transforms) >= 1);
        assert!(Arc::strong_count(&state.pipelines) >= 1);
    }

    #[test]
    fn test_state_default() {
        let state = WasmStreamApiState::default();
        assert!(Arc::strong_count(&state.transforms) >= 1);
    }

    #[test]
    fn test_state_clone() {
        let state = WasmStreamApiState::new();
        let cloned = state.clone();
        assert!(Arc::strong_count(&state.transforms) >= 2);
        assert!(Arc::strong_count(&cloned.transforms) >= 2);
    }

    #[test]
    fn test_transform_metrics_default() {
        let m = TransformMetrics::default();
        assert_eq!(m.records_in, 0);
        assert_eq!(m.records_out, 0);
        assert_eq!(m.records_filtered, 0);
        assert_eq!(m.records_errored, 0);
        assert_eq!(m.avg_latency_us, 0.0);
        assert!(m.last_processed_at.is_none());
    }

    #[test]
    fn test_transform_status_serialize_running() {
        let s = TransformStatus::Running;
        let json = serde_json::to_string(&s).unwrap();
        assert!(json.contains("Running"));
    }

    #[test]
    fn test_transform_status_serialize_error() {
        let s = TransformStatus::Error("oops".to_string());
        let json = serde_json::to_string(&s).unwrap();
        assert!(json.contains("Error"));
        assert!(json.contains("oops"));
    }

    #[test]
    fn test_transform_status_equality() {
        assert_eq!(TransformStatus::Running, TransformStatus::Running);
        assert_eq!(TransformStatus::Paused, TransformStatus::Paused);
        assert_ne!(TransformStatus::Running, TransformStatus::Paused);
    }

    #[test]
    fn test_active_transform_serialization() {
        let t = ActiveTransform {
            id: "t1".into(),
            name: "filter".into(),
            source_topic: "input".into(),
            target_topic: "output".into(),
            wasm_module: "filter.wasm".into(),
            config: HashMap::new(),
            status: TransformStatus::Running,
            metrics: TransformMetrics::default(),
            created_at: "2025-01-01T00:00:00Z".into(),
        };
        let json = serde_json::to_string(&t).unwrap();
        assert!(json.contains("\"name\":\"filter\""));
        assert!(json.contains("\"source_topic\":\"input\""));
    }

    #[test]
    fn test_pipeline_step_serialization() {
        let step = PipelineStep {
            order: 1,
            transform_name: "enrich".into(),
            config: HashMap::from([("key".into(), "val".into())]),
        };
        let json = serde_json::to_string(&step).unwrap();
        assert!(json.contains("\"order\":1"));
        assert!(json.contains("\"transform_name\":\"enrich\""));
    }

    #[test]
    fn test_stream_pipeline_serialization() {
        let p = StreamPipeline {
            id: "p1".into(),
            name: "etl".into(),
            source_topic: "raw".into(),
            sink_topic: "clean".into(),
            steps: vec![PipelineStep {
                order: 1,
                transform_name: "parse".into(),
                config: HashMap::new(),
            }],
            status: TransformStatus::Starting,
            created_at: "2025-01-01T00:00:00Z".into(),
        };
        let json = serde_json::to_string(&p).unwrap();
        assert!(json.contains("\"name\":\"etl\""));
        assert!(json.contains("\"source_topic\":\"raw\""));
    }

    #[test]
    fn test_create_transform_request_deserialization() {
        let json = r#"{
            "name": "my-transform",
            "source_topic": "in",
            "target_topic": "out",
            "wasm_module": "mod.wasm"
        }"#;
        let req: CreateTransformRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.name, "my-transform");
        assert!(req.config.is_none());
    }

    #[test]
    fn test_create_transform_request_with_config() {
        let json = r#"{
            "name": "my-transform",
            "source_topic": "in",
            "target_topic": "out",
            "wasm_module": "mod.wasm",
            "config": {"key": "value"}
        }"#;
        let req: CreateTransformRequest = serde_json::from_str(json).unwrap();
        assert_eq!(
            req.config.as_ref().unwrap().get("key").unwrap(),
            "value"
        );
    }

    #[test]
    fn test_create_pipeline_request_deserialization() {
        let json = r#"{
            "name": "pipe",
            "source_topic": "src",
            "sink_topic": "sink",
            "steps": [{"order": 1, "transform_name": "t1", "config": {}}]
        }"#;
        let req: CreatePipelineRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.name, "pipe");
        assert_eq!(req.steps.len(), 1);
    }

    // ------------------------------------------------------------------
    // Integration tests (handler-level via tower::ServiceExt)
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn test_list_transforms_empty() {
        let resp = app()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/streams/transforms")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["total"], 0);
        assert_eq!(v["transforms"].as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_create_and_get_transform() {
        let state = test_state();
        let router = create_wasm_stream_api_router(state);

        let resp = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(http::Method::POST)
                    .uri("/api/v1/streams/transforms")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_string(&serde_json::json!({
                            "name": "filter-nulls",
                            "source_topic": "raw-events",
                            "target_topic": "clean-events",
                            "wasm_module": "filter.wasm"
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::CREATED);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let created: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let id = created["id"].as_str().unwrap().to_string();

        // GET the created transform
        let resp = router
            .oneshot(
                Request::builder()
                    .uri(format!("/api/v1/streams/transforms/{}", id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let fetched: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(fetched["name"], "filter-nulls");
        assert_eq!(fetched["source_topic"], "raw-events");
    }

    #[tokio::test]
    async fn test_create_transform_duplicate_name() {
        let state = test_state();
        let router = create_wasm_stream_api_router(state);

        let body_json = serde_json::to_string(&serde_json::json!({
            "name": "dup",
            "source_topic": "a",
            "target_topic": "b",
            "wasm_module": "m.wasm"
        }))
        .unwrap();

        let resp = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(http::Method::POST)
                    .uri("/api/v1/streams/transforms")
                    .header("content-type", "application/json")
                    .body(Body::from(body_json.clone()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);

        let resp = router
            .oneshot(
                Request::builder()
                    .method(http::Method::POST)
                    .uri("/api/v1/streams/transforms")
                    .header("content-type", "application/json")
                    .body(Body::from(body_json))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn test_get_transform_not_found() {
        let resp = app()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/streams/transforms/nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_delete_transform() {
        let state = test_state();
        let router = create_wasm_stream_api_router(state);

        // Create
        let resp = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(http::Method::POST)
                    .uri("/api/v1/streams/transforms")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_string(&serde_json::json!({
                            "name": "to-delete",
                            "source_topic": "a",
                            "target_topic": "b",
                            "wasm_module": "m.wasm"
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let created: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let id = created["id"].as_str().unwrap();

        // Delete
        let resp = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(http::Method::DELETE)
                    .uri(format!("/api/v1/streams/transforms/{}", id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);

        // Confirm gone
        let resp = router
            .oneshot(
                Request::builder()
                    .uri(format!("/api/v1/streams/transforms/{}", id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_delete_transform_not_found() {
        let resp = app()
            .oneshot(
                Request::builder()
                    .method(http::Method::DELETE)
                    .uri("/api/v1/streams/transforms/nope")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_pause_and_resume_transform() {
        let state = test_state();
        let router = create_wasm_stream_api_router(state.clone());

        // Create
        let resp = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(http::Method::POST)
                    .uri("/api/v1/streams/transforms")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_string(&serde_json::json!({
                            "name": "pausable",
                            "source_topic": "a",
                            "target_topic": "b",
                            "wasm_module": "m.wasm"
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let created: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let id = created["id"].as_str().unwrap().to_string();

        // Set status to Running so pause works
        {
            let mut transforms = state.transforms.write().await;
            transforms.get_mut(&id).unwrap().status = TransformStatus::Running;
        }

        // Pause
        let resp = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(http::Method::POST)
                    .uri(format!("/api/v1/streams/transforms/{}/pause", id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["status"]["state"], "Paused");

        // Resume
        let resp = router
            .oneshot(
                Request::builder()
                    .method(http::Method::POST)
                    .uri(format!("/api/v1/streams/transforms/{}/resume", id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["status"]["state"], "Running");
    }

    #[tokio::test]
    async fn test_pause_already_paused() {
        let state = test_state();
        let router = create_wasm_stream_api_router(state.clone());

        // Insert a paused transform directly
        {
            let mut transforms = state.transforms.write().await;
            transforms.insert(
                "t-paused".into(),
                ActiveTransform {
                    id: "t-paused".into(),
                    name: "paused-one".into(),
                    source_topic: "a".into(),
                    target_topic: "b".into(),
                    wasm_module: "m.wasm".into(),
                    config: HashMap::new(),
                    status: TransformStatus::Paused,
                    metrics: TransformMetrics::default(),
                    created_at: "2025-01-01T00:00:00Z".into(),
                },
            );
        }

        let resp = router
            .oneshot(
                Request::builder()
                    .method(http::Method::POST)
                    .uri("/api/v1/streams/transforms/t-paused/pause")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn test_resume_already_running() {
        let state = test_state();
        let router = create_wasm_stream_api_router(state.clone());

        {
            let mut transforms = state.transforms.write().await;
            transforms.insert(
                "t-running".into(),
                ActiveTransform {
                    id: "t-running".into(),
                    name: "running-one".into(),
                    source_topic: "a".into(),
                    target_topic: "b".into(),
                    wasm_module: "m.wasm".into(),
                    config: HashMap::new(),
                    status: TransformStatus::Running,
                    metrics: TransformMetrics::default(),
                    created_at: "2025-01-01T00:00:00Z".into(),
                },
            );
        }

        let resp = router
            .oneshot(
                Request::builder()
                    .method(http::Method::POST)
                    .uri("/api/v1/streams/transforms/t-running/resume")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn test_pause_not_found() {
        let resp = app()
            .oneshot(
                Request::builder()
                    .method(http::Method::POST)
                    .uri("/api/v1/streams/transforms/nope/pause")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_resume_not_found() {
        let resp = app()
            .oneshot(
                Request::builder()
                    .method(http::Method::POST)
                    .uri("/api/v1/streams/transforms/nope/resume")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_get_transform_metrics() {
        let state = test_state();
        let router = create_wasm_stream_api_router(state.clone());

        {
            let mut transforms = state.transforms.write().await;
            transforms.insert(
                "t-metrics".into(),
                ActiveTransform {
                    id: "t-metrics".into(),
                    name: "metrics-one".into(),
                    source_topic: "a".into(),
                    target_topic: "b".into(),
                    wasm_module: "m.wasm".into(),
                    config: HashMap::new(),
                    status: TransformStatus::Running,
                    metrics: TransformMetrics {
                        records_in: 100,
                        records_out: 95,
                        records_filtered: 3,
                        records_errored: 2,
                        avg_latency_us: 42.5,
                        last_processed_at: Some("2025-01-01T00:00:00Z".into()),
                    },
                    created_at: "2025-01-01T00:00:00Z".into(),
                },
            );
        }

        let resp = router
            .oneshot(
                Request::builder()
                    .uri("/api/v1/streams/transforms/t-metrics/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["records_in"], 100);
        assert_eq!(v["records_out"], 95);
        assert_eq!(v["records_filtered"], 3);
        assert_eq!(v["records_errored"], 2);
    }

    #[tokio::test]
    async fn test_get_metrics_not_found() {
        let resp = app()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/streams/transforms/nope/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    // ------------------------------------------------------------------
    // Pipeline tests
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn test_list_pipelines_empty() {
        let resp = app()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/streams/pipelines")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["total"], 0);
    }

    #[tokio::test]
    async fn test_create_and_get_pipeline() {
        let state = test_state();
        let router = create_wasm_stream_api_router(state);

        let resp = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(http::Method::POST)
                    .uri("/api/v1/streams/pipelines")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_string(&serde_json::json!({
                            "name": "etl-pipeline",
                            "source_topic": "raw",
                            "sink_topic": "processed",
                            "steps": [
                                {"order": 1, "transform_name": "parse", "config": {}},
                                {"order": 2, "transform_name": "enrich", "config": {"key": "val"}}
                            ]
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let created: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let id = created["id"].as_str().unwrap();
        assert_eq!(created["name"], "etl-pipeline");
        assert_eq!(created["steps"].as_array().unwrap().len(), 2);

        // GET
        let resp = router
            .oneshot(
                Request::builder()
                    .uri(format!("/api/v1/streams/pipelines/{}", id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_create_pipeline_empty_steps() {
        let resp = app()
            .oneshot(
                Request::builder()
                    .method(http::Method::POST)
                    .uri("/api/v1/streams/pipelines")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_string(&serde_json::json!({
                            "name": "empty",
                            "source_topic": "a",
                            "sink_topic": "b",
                            "steps": []
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_create_pipeline_duplicate_name() {
        let state = test_state();
        let router = create_wasm_stream_api_router(state);

        let body_json = serde_json::to_string(&serde_json::json!({
            "name": "dup-pipe",
            "source_topic": "a",
            "sink_topic": "b",
            "steps": [{"order": 1, "transform_name": "t1", "config": {}}]
        }))
        .unwrap();

        let resp = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(http::Method::POST)
                    .uri("/api/v1/streams/pipelines")
                    .header("content-type", "application/json")
                    .body(Body::from(body_json.clone()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);

        let resp = router
            .oneshot(
                Request::builder()
                    .method(http::Method::POST)
                    .uri("/api/v1/streams/pipelines")
                    .header("content-type", "application/json")
                    .body(Body::from(body_json))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn test_get_pipeline_not_found() {
        let resp = app()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/streams/pipelines/nope")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_delete_pipeline() {
        let state = test_state();
        let router = create_wasm_stream_api_router(state.clone());

        {
            let mut pipelines = state.pipelines.write().await;
            pipelines.insert(
                "p-del".into(),
                StreamPipeline {
                    id: "p-del".into(),
                    name: "to-delete".into(),
                    source_topic: "a".into(),
                    sink_topic: "b".into(),
                    steps: vec![PipelineStep {
                        order: 1,
                        transform_name: "t".into(),
                        config: HashMap::new(),
                    }],
                    status: TransformStatus::Running,
                    created_at: "2025-01-01T00:00:00Z".into(),
                },
            );
        }

        let resp = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(http::Method::DELETE)
                    .uri("/api/v1/streams/pipelines/p-del")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);

        // Confirm gone
        let resp = router
            .oneshot(
                Request::builder()
                    .uri("/api/v1/streams/pipelines/p-del")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_delete_pipeline_not_found() {
        let resp = app()
            .oneshot(
                Request::builder()
                    .method(http::Method::DELETE)
                    .uri("/api/v1/streams/pipelines/nope")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_pause_transform_in_starting_state() {
        let state = test_state();
        let router = create_wasm_stream_api_router(state.clone());

        // Insert a transform in Starting state (the default from create)
        {
            let mut transforms = state.transforms.write().await;
            transforms.insert(
                "t-starting".into(),
                ActiveTransform {
                    id: "t-starting".into(),
                    name: "starting-one".into(),
                    source_topic: "a".into(),
                    target_topic: "b".into(),
                    wasm_module: "m.wasm".into(),
                    config: HashMap::new(),
                    status: TransformStatus::Starting,
                    metrics: TransformMetrics::default(),
                    created_at: "2025-01-01T00:00:00Z".into(),
                },
            );
        }

        let resp = router
            .oneshot(
                Request::builder()
                    .method(http::Method::POST)
                    .uri("/api/v1/streams/transforms/t-starting/pause")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        // Starting state can be paused
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
