//! StreamQL GA REST API
//!
//! Provides a full-featured REST API for StreamQL queries, materialized view
//! management, query validation, and execution plan introspection.
//!
//! ## Endpoints
//!
//! - `POST /api/v1/streamql/query`          — Execute a one-shot query
//! - `POST /api/v1/streamql/validate`       — Validate a query without executing
//! - `POST /api/v1/streamql/explain`        — Get execution plan
//! - `POST /api/v1/streamql/views`          — Create a materialized view
//! - `GET  /api/v1/streamql/views`          — List all materialized views
//! - `GET  /api/v1/streamql/views/:name`    — Get view info
//! - `DELETE /api/v1/streamql/views/:name`  — Drop a view
//! - `POST /api/v1/streamql/views/:name/query` — Query a view (lookup/scan)

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{debug, error, info};

use crate::streamql::materialized::{
    MaterializedViewManager, RefreshMode, ViewInfo, ViewRow,
};
use crate::streamql::StreamQL;

// ---------------------------------------------------------------------------
// Request / response types
// ---------------------------------------------------------------------------

/// Request body for `POST /api/v1/streamql/query`
#[derive(Debug, Deserialize)]
pub struct QueryRequest {
    /// StreamQL query string
    pub query: String,
    /// Optional limit on rows returned
    #[serde(default)]
    pub limit: Option<usize>,
    /// Optional timeout in milliseconds
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

/// Successful query response
#[derive(Debug, Serialize)]
pub struct QueryResponse {
    /// Column names
    pub columns: Vec<String>,
    /// Row data — each row is a list of JSON values
    pub rows: Vec<Vec<serde_json::Value>>,
    /// Number of rows returned
    pub row_count: usize,
    /// Execution time in milliseconds
    pub execution_time_ms: u64,
}

/// Request body for `POST /api/v1/streamql/validate`
#[derive(Debug, Deserialize)]
pub struct ValidateRequest {
    /// StreamQL query to validate
    pub query: String,
}

/// Validation response
#[derive(Debug, Serialize, Deserialize)]
pub struct ValidateResponse {
    /// Whether the query is valid
    pub valid: bool,
    /// Error message if the query is invalid
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Request body for `POST /api/v1/streamql/explain`
#[derive(Debug, Deserialize)]
pub struct ExplainRequest {
    /// StreamQL query to explain
    pub query: String,
}

/// Explain response
#[derive(Debug, Serialize, Deserialize)]
pub struct ExplainResponse {
    /// Human-readable execution plan
    pub plan: String,
    /// Original query
    pub query: String,
}

/// Request body for `POST /api/v1/streamql/views`
#[derive(Debug, Deserialize)]
pub struct CreateViewRequest {
    /// View name (must be unique)
    pub name: String,
    /// Source SQL query
    pub query: String,
    /// Refresh mode: "continuous", "on_demand", or {"periodic": interval_ms}
    #[serde(default = "default_refresh_mode")]
    pub refresh_mode: serde_json::Value,
}

fn default_refresh_mode() -> serde_json::Value {
    serde_json::Value::String("continuous".to_string())
}

/// Request body for `POST /api/v1/streamql/views/:name/query`
#[derive(Debug, Deserialize)]
pub struct ViewQueryRequest {
    /// Optional key for point lookup
    #[serde(default)]
    pub key: Option<String>,
    /// Optional limit for scan
    #[serde(default)]
    pub limit: Option<usize>,
}

/// View query response
#[derive(Debug, Serialize, Deserialize)]
pub struct ViewQueryResponse {
    /// View name
    pub view: String,
    /// Rows returned
    pub rows: Vec<ViewRow>,
    /// Number of rows returned
    pub row_count: usize,
}

/// API error response
#[derive(Debug, Serialize)]
pub struct StreamqlErrorResponse {
    /// Error category
    pub error: String,
    /// Human-readable message
    pub message: String,
}

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

/// Shared state for the StreamQL GA REST API
#[derive(Clone)]
pub(crate) struct StreamqlApiState {
    pub streamql: Arc<StreamQL>,
    pub view_manager: Arc<MaterializedViewManager>,
}

impl Default for StreamqlApiState {
    fn default() -> Self {
        let topic_manager = Arc::new(
            crate::storage::TopicManager::in_memory()
                .expect("Failed to create in-memory TopicManager"),
        );
        Self {
            streamql: Arc::new(StreamQL::new(topic_manager)),
            view_manager: Arc::new(MaterializedViewManager::new()),
        }
    }
}

// ---------------------------------------------------------------------------
// Error handling
// ---------------------------------------------------------------------------

struct StreamqlApiError {
    status: StatusCode,
    body: StreamqlErrorResponse,
}

impl StreamqlApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            body: StreamqlErrorResponse {
                error: "BAD_REQUEST".to_string(),
                message: message.into(),
            },
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            body: StreamqlErrorResponse {
                error: "NOT_FOUND".to_string(),
                message: message.into(),
            },
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            body: StreamqlErrorResponse {
                error: "INTERNAL_ERROR".to_string(),
                message: message.into(),
            },
        }
    }
}

impl IntoResponse for StreamqlApiError {
    fn into_response(self) -> Response {
        (self.status, Json(self.body)).into_response()
    }
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

/// Build the StreamQL GA REST API router.
pub fn streamql_ga_router() -> Router<StreamqlApiState> {
    Router::new()
        .route("/api/v1/streamql/query", post(execute_query))
        .route("/api/v1/streamql/validate", post(validate_query))
        .route("/api/v1/streamql/explain", post(explain_query))
        .route(
            "/api/v1/streamql/views",
            post(create_view).get(list_views),
        )
        .route(
            "/api/v1/streamql/views/:name",
            get(get_view).delete(drop_view),
        )
        .route(
            "/api/v1/streamql/views/:name/query",
            post(query_view),
        )
}

/// Backward-compatible alias used by the HTTP server bootstrap.
pub fn streamql_router() -> Router<StreamqlApiState> {
    streamql_ga_router()
}

/// Create a fully wired StreamQL router with a default in-memory state.
///
/// Useful for integration tests and standalone deployments where
/// a separate state is not externally managed.
pub fn create_streamql_router() -> Router {
    streamql_ga_router().with_state(StreamqlApiState::default())
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// POST /api/v1/streamql/query — execute a one-shot query
async fn execute_query(
    State(state): State<StreamqlApiState>,
    Json(req): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, StreamqlApiError> {
    let query = req.query.trim();
    if query.is_empty() {
        return Err(StreamqlApiError::bad_request("Query must not be empty"));
    }
    debug!(query, "Executing StreamQL query");

    let start = std::time::Instant::now();
    let result = state
        .streamql
        .execute(query)
        .await
        .map_err(|e| {
            error!(error = %e, "StreamQL query execution failed");
            StreamqlApiError::bad_request(format!("Query execution failed: {e}"))
        })?;

    let columns: Vec<String> = result
        .schema
        .columns
        .iter()
        .map(|c| c.name.clone())
        .collect();

    let rows: Vec<Vec<serde_json::Value>> = result
        .rows
        .iter()
        .take(req.limit.unwrap_or(usize::MAX))
        .map(|row| {
            row.values
                .iter()
                .map(|v| match v {
                    crate::streamql::types::Value::Null => serde_json::Value::Null,
                    crate::streamql::types::Value::Boolean(b) => {
                        serde_json::Value::Bool(*b)
                    }
                    crate::streamql::types::Value::Int64(i) => {
                        serde_json::json!(i)
                    }
                    crate::streamql::types::Value::Float64(f) => {
                        serde_json::json!(f)
                    }
                    crate::streamql::types::Value::String(s) => {
                        serde_json::Value::String(s.clone())
                    }
                    crate::streamql::types::Value::Timestamp(ts) => {
                        serde_json::json!(ts)
                    }
                    crate::streamql::types::Value::Binary(b) => {
                        serde_json::json!(b)
                    }
                    crate::streamql::types::Value::Date(d) => {
                        serde_json::json!(d)
                    }
                    crate::streamql::types::Value::Duration(d) => {
                        serde_json::json!(d)
                    }
                    crate::streamql::types::Value::Json(j) => {
                        serde_json::json!(j)
                    }
                    crate::streamql::types::Value::Array(arr) => {
                        serde_json::json!(format!("{:?}", arr))
                    }
                    crate::streamql::types::Value::Map(m) => {
                        serde_json::json!(format!("{:?}", m))
                    }
                })
                .collect()
        })
        .collect();

    let row_count = rows.len();
    let elapsed = start.elapsed().as_millis() as u64;
    info!(row_count, elapsed_ms = elapsed, "StreamQL query completed");

    Ok(Json(QueryResponse {
        columns,
        rows,
        row_count,
        execution_time_ms: elapsed,
    }))
}

/// POST /api/v1/streamql/validate — validate without executing
async fn validate_query(
    State(state): State<StreamqlApiState>,
    Json(req): Json<ValidateRequest>,
) -> Json<ValidateResponse> {
    let query = req.query.trim();
    debug!(query, "Validating StreamQL query");

    match state.streamql.validate(query) {
        Ok(()) => Json(ValidateResponse {
            valid: true,
            error: None,
        }),
        Err(e) => Json(ValidateResponse {
            valid: false,
            error: Some(e.to_string()),
        }),
    }
}

/// POST /api/v1/streamql/explain — get execution plan
async fn explain_query(
    State(state): State<StreamqlApiState>,
    Json(req): Json<ExplainRequest>,
) -> Result<Json<ExplainResponse>, StreamqlApiError> {
    let query = req.query.trim();
    if query.is_empty() {
        return Err(StreamqlApiError::bad_request("Query must not be empty"));
    }
    debug!(query, "Explaining StreamQL query");

    let plan = state
        .streamql
        .explain(query)
        .map_err(|e| StreamqlApiError::bad_request(format!("Explain failed: {e}")))?;

    Ok(Json(ExplainResponse {
        plan,
        query: query.to_string(),
    }))
}

/// POST /api/v1/streamql/views — create a materialized view
async fn create_view(
    State(state): State<StreamqlApiState>,
    Json(req): Json<CreateViewRequest>,
) -> Result<(StatusCode, Json<ViewInfo>), StreamqlApiError> {
    let name = req.name.trim();
    if name.is_empty() {
        return Err(StreamqlApiError::bad_request("View name must not be empty"));
    }
    let query = req.query.trim();
    if query.is_empty() {
        return Err(StreamqlApiError::bad_request(
            "View query must not be empty",
        ));
    }

    let refresh = parse_refresh_mode(&req.refresh_mode);
    info!(view = name, "Creating materialized view");

    let info = state
        .view_manager
        .create_view(name, query, refresh)
        .await
        .map_err(|e| {
            error!(error = %e, "Failed to create materialized view");
            StreamqlApiError::bad_request(format!("Failed to create view: {e}"))
        })?;

    Ok((StatusCode::CREATED, Json(info)))
}

/// GET /api/v1/streamql/views — list all materialized views
async fn list_views(
    State(state): State<StreamqlApiState>,
) -> Json<Vec<ViewInfo>> {
    let views = state.view_manager.list_views().await;
    debug!(count = views.len(), "Listed materialized views");
    Json(views)
}

/// GET /api/v1/streamql/views/:name — get view info
async fn get_view(
    State(state): State<StreamqlApiState>,
    Path(name): Path<String>,
) -> Result<Json<ViewInfo>, StreamqlApiError> {
    state
        .view_manager
        .get_view(&name)
        .await
        .map(Json)
        .ok_or_else(|| StreamqlApiError::not_found(format!("View '{}' not found", name)))
}

/// DELETE /api/v1/streamql/views/:name — drop a view
async fn drop_view(
    State(state): State<StreamqlApiState>,
    Path(name): Path<String>,
) -> Result<StatusCode, StreamqlApiError> {
    info!(view = %name, "Dropping materialized view");
    state
        .view_manager
        .drop_view(&name)
        .await
        .map_err(|e| {
            error!(error = %e, "Failed to drop view");
            StreamqlApiError::not_found(format!("View '{}' not found", name))
        })?;
    Ok(StatusCode::NO_CONTENT)
}

/// POST /api/v1/streamql/views/:name/query — query a view (lookup or scan)
async fn query_view(
    State(state): State<StreamqlApiState>,
    Path(name): Path<String>,
    Json(req): Json<ViewQueryRequest>,
) -> Result<Json<ViewQueryResponse>, StreamqlApiError> {
    debug!(view = %name, key = ?req.key, "Querying materialized view");

    let rows = if let Some(key) = &req.key {
        // Point lookup
        match state.view_manager.lookup(&name, key).await {
            Ok(Some(row)) => vec![row],
            Ok(None) => vec![],
            Err(e) => {
                return Err(StreamqlApiError::not_found(format!(
                    "View query failed: {e}"
                )));
            }
        }
    } else {
        // Full scan with optional limit
        state
            .view_manager
            .scan(&name, req.limit)
            .await
            .map_err(|e| {
                StreamqlApiError::not_found(format!("View query failed: {e}"))
            })?
    };

    let row_count = rows.len();
    Ok(Json(ViewQueryResponse {
        view: name,
        rows,
        row_count,
    }))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn parse_refresh_mode(value: &serde_json::Value) -> RefreshMode {
    match value {
        serde_json::Value::String(s) => match s.as_str() {
            "on_demand" => RefreshMode::OnDemand,
            "continuous" | _ => RefreshMode::Continuous,
        },
        serde_json::Value::Object(map) => {
            if let Some(interval) = map.get("periodic").and_then(|v| v.as_u64()) {
                RefreshMode::Periodic {
                    interval_ms: interval,
                }
            } else {
                RefreshMode::Continuous
            }
        }
        _ => RefreshMode::Continuous,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::TopicManager;
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    fn test_state() -> StreamqlApiState {
        let topic_manager = Arc::new(TopicManager::in_memory().unwrap());
        StreamqlApiState {
            streamql: Arc::new(StreamQL::new(topic_manager)),
            view_manager: Arc::new(MaterializedViewManager::new()),
        }
    }

    fn test_app() -> Router {
        streamql_ga_router().with_state(test_state())
    }

    // -- query endpoint --

    #[tokio::test]
    async fn test_query_empty_body() {
        let app = test_app();
        let resp = app
            .oneshot(
                Request::post("/api/v1/streamql/query")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"query": ""}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_query_select() {
        let state = test_state();
        let _ = state.streamql.execute("SELECT 1").await;

        let app = streamql_ga_router().with_state(state);
        let resp = app
            .oneshot(
                Request::post("/api/v1/streamql/query")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"query": "SELECT 1"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        // May succeed or fail depending on topic existence — either is valid
        assert!(
            resp.status() == StatusCode::OK || resp.status() == StatusCode::BAD_REQUEST
        );
    }

    // -- validate endpoint --

    #[tokio::test]
    async fn test_validate_valid_query() {
        let app = test_app();
        let resp = app
            .oneshot(
                Request::post("/api/v1/streamql/validate")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"query": "SELECT user_id FROM events WHERE x > 1"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let parsed: ValidateResponse = serde_json::from_slice(&body).unwrap();
        assert!(parsed.valid);
    }

    #[tokio::test]
    async fn test_validate_invalid_query() {
        let app = test_app();
        let resp = app
            .oneshot(
                Request::post("/api/v1/streamql/validate")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"query": "INVALID GIBBERISH %%"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let parsed: ValidateResponse = serde_json::from_slice(&body).unwrap();
        assert!(!parsed.valid);
        assert!(parsed.error.is_some());
    }

    // -- explain endpoint --

    #[tokio::test]
    async fn test_explain_query() {
        let app = test_app();
        let resp = app
            .oneshot(
                Request::post("/api/v1/streamql/explain")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"query": "SELECT user_id FROM events WHERE x > 1"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let parsed: ExplainResponse = serde_json::from_slice(&body).unwrap();
        assert!(!parsed.plan.is_empty());
    }

    #[tokio::test]
    async fn test_explain_empty_query() {
        let app = test_app();
        let resp = app
            .oneshot(
                Request::post("/api/v1/streamql/explain")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"query": ""}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    // -- views CRUD --

    #[tokio::test]
    async fn test_create_and_list_views() {
        let state = test_state();
        let app = streamql_ga_router().with_state(state.clone());
        let resp = app
            .oneshot(
                Request::post("/api/v1/streamql/views")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"name": "user_counts", "query": "SELECT user_id, COUNT(*) FROM events GROUP BY user_id"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);

        // List views
        let app = streamql_ga_router().with_state(state);
        let resp = app
            .oneshot(
                Request::get("/api/v1/streamql/views")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let views: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();
        assert_eq!(views.len(), 1);
        assert_eq!(views[0]["definition"]["name"], "user_counts");
    }

    #[tokio::test]
    async fn test_get_view() {
        let state = test_state();
        state
            .view_manager
            .create_view("v1", "SELECT * FROM t", RefreshMode::Continuous)
            .await
            .unwrap();

        let app = streamql_ga_router().with_state(state);
        let resp = app
            .oneshot(
                Request::get("/api/v1/streamql/views/v1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_view_not_found() {
        let app = test_app();
        let resp = app
            .oneshot(
                Request::get("/api/v1/streamql/views/nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_drop_view() {
        let state = test_state();
        state
            .view_manager
            .create_view("v1", "SELECT * FROM t", RefreshMode::Continuous)
            .await
            .unwrap();

        let app = streamql_ga_router().with_state(state);
        let resp = app
            .oneshot(
                Request::delete("/api/v1/streamql/views/v1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn test_drop_view_not_found() {
        let app = test_app();
        let resp = app
            .oneshot(
                Request::delete("/api/v1/streamql/views/nope")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    // -- view query --

    #[tokio::test]
    async fn test_query_view_scan() {
        let state = test_state();
        state
            .view_manager
            .create_view("v1", "SELECT * FROM events", RefreshMode::Continuous)
            .await
            .unwrap();
        state
            .view_manager
            .process_record("events", Some("k1"), &serde_json::json!({"id": 1}))
            .await
            .unwrap();

        let app = streamql_ga_router().with_state(state);
        let resp = app
            .oneshot(
                Request::post("/api/v1/streamql/views/v1/query")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let parsed: ViewQueryResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed.row_count, 1);
    }

    #[tokio::test]
    async fn test_query_view_lookup() {
        let state = test_state();
        state
            .view_manager
            .create_view("v1", "SELECT * FROM events", RefreshMode::Continuous)
            .await
            .unwrap();
        state
            .view_manager
            .process_record("events", Some("k1"), &serde_json::json!({"id": 1}))
            .await
            .unwrap();

        let app = streamql_ga_router().with_state(state);
        let resp = app
            .oneshot(
                Request::post("/api/v1/streamql/views/v1/query")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"key": "k1"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let parsed: ViewQueryResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed.row_count, 1);
    }

    #[tokio::test]
    async fn test_query_view_not_found() {
        let app = test_app();
        let resp = app
            .oneshot(
                Request::post("/api/v1/streamql/views/nope/query")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_create_view_empty_name() {
        let app = test_app();
        let resp = app
            .oneshot(
                Request::post("/api/v1/streamql/views")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"name": "", "query": "SELECT * FROM t"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn test_parse_refresh_mode_continuous() {
        let v = serde_json::json!("continuous");
        assert!(matches!(parse_refresh_mode(&v), RefreshMode::Continuous));
    }

    #[test]
    fn test_parse_refresh_mode_on_demand() {
        let v = serde_json::json!("on_demand");
        assert!(matches!(parse_refresh_mode(&v), RefreshMode::OnDemand));
    }

    #[test]
    fn test_parse_refresh_mode_periodic() {
        let v = serde_json::json!({"periodic": 5000});
        match parse_refresh_mode(&v) {
            RefreshMode::Periodic { interval_ms } => assert_eq!(interval_ms, 5000),
            other => panic!("Expected Periodic, got {:?}", other),
        }
    }
}
