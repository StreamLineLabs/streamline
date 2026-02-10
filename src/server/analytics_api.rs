//! Analytics REST API for SQL queries on streaming data.
//!
//! This module provides HTTP endpoints for executing SQL queries
//! on Streamline topics using the embedded DuckDB analytics engine.
//!
//! # Stability
//!
//! **Stable** -- Breaking changes only in major versions.
//!
//! # Endpoints
//!
//! | Method | Path | Description |
//! |--------|------|-------------|
//! | `POST` | `/api/v1/query` | Execute a SQL query |
//! | `POST` | `/api/v1/query/explain` | Get query execution plan |
//! | `GET` | `/api/v1/query/cache/stats` | Get query cache statistics |
//! | `DELETE` | `/api/v1/query/cache` | Clear the query cache |
//! | `POST` | `/api/v1/views` | Create a materialized view |
//! | `GET` | `/api/v1/views` | List all materialized views |
//! | `GET` | `/api/v1/views/{name}` | Get a specific materialized view |
//! | `DELETE` | `/api/v1/views/{name}` | Delete a materialized view |
//! | `POST` | `/api/v1/views/{name}/refresh` | Refresh a materialized view |
//! | `GET` | `/api/v1/views/{name}/query` | Query a materialized view |
//!
//! # Example
//!
//! ```bash
//! # Execute a query
//! curl -X POST http://localhost:9094/api/v1/query \
//!   -H 'Content-Type: application/json' \
//!   -d '{"sql": "SELECT COUNT(*) FROM streamline_topic_events", "max_rows": 100}'
//!
//! # Paginate results
//! curl -X POST http://localhost:9094/api/v1/query \
//!   -H 'Content-Type: application/json' \
//!   -d '{"sql": "SELECT * FROM streamline_topic_events", "max_rows": 50, "offset": 100}'
//! ```

#[cfg(feature = "analytics")]
use crate::analytics::{DuckDBEngine, QueryOptions};
#[cfg(not(feature = "analytics"))]
use crate::storage::TopicManager;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
#[cfg(feature = "analytics")]
use tracing::info;

/// Shared state for the analytics API.
#[derive(Clone)]
pub struct AnalyticsApiState {
    /// DuckDB engine (only present when the `analytics` feature is enabled).
    #[cfg(feature = "analytics")]
    pub engine: Arc<DuckDBEngine>,

    /// Topic manager fallback (used to return a helpful error when analytics is disabled).
    #[cfg(not(feature = "analytics"))]
    pub topic_manager: Arc<TopicManager>,
}

// ─── Request / Response types ────────────────────────────────────────────────

/// SQL query request body for `POST /api/v1/query`.
///
/// # Fields
///
/// | Field | Type | Default | Description |
/// |-------|------|---------|-------------|
/// | `sql` | `String` | (required) | SQL query to execute |
/// | `max_rows` | `usize?` | `10 000` | Maximum rows to return |
/// | `offset` | `usize` | `0` | Pagination offset |
/// | `cursor` | `String?` | `None` | Cursor for cursor-based pagination |
/// | `enable_cache` | `bool` | `true` | Enable result caching |
/// | `cache_ttl_seconds` | `u64` | `60` | Cache TTL |
/// | `timeout_ms` | `u64?` | `30 000` | Query timeout in ms |
#[derive(Debug, Deserialize)]
pub struct QueryRequest {
    /// SQL query to execute.
    pub sql: String,

    /// Maximum number of rows to return.
    #[serde(default)]
    pub max_rows: Option<usize>,

    /// Pagination offset (number of rows to skip).
    #[serde(default)]
    pub offset: Option<usize>,

    /// Opaque cursor for cursor-based pagination. When provided, `offset`
    /// is ignored and pagination resumes from the cursor position.
    #[serde(default)]
    pub cursor: Option<String>,

    /// Enable query caching.
    #[serde(default = "default_true")]
    pub enable_cache: bool,

    /// Cache TTL in seconds.
    #[serde(default = "default_cache_ttl")]
    pub cache_ttl_seconds: u64,

    /// Query timeout in milliseconds.
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

fn default_true() -> bool {
    true
}

fn default_cache_ttl() -> u64 {
    60
}

/// Request body for `POST /api/v1/views`.
#[derive(Debug, Deserialize)]
pub struct CreateViewRequest {
    /// View name (must be a valid SQL identifier).
    pub name: String,

    /// SQL query defining the view.
    pub query: String,

    /// Refresh interval in seconds.
    #[serde(default = "default_refresh_interval")]
    pub refresh_interval_seconds: u64,
}

fn default_refresh_interval() -> u64 {
    300 // 5 minutes
}

/// Request body for `POST /api/v1/query/explain`.
#[derive(Debug, Deserialize)]
pub struct ExplainRequest {
    /// SQL query whose execution plan should be returned.
    pub sql: String,
}

/// Wrapper for query responses that includes an optional pagination cursor.
#[derive(Debug, Serialize)]
pub struct PaginatedQueryResponse {
    /// The query result.
    #[serde(flatten)]
    pub result: serde_json::Value,
    /// Opaque cursor that can be passed in the next request to continue
    /// paginating. `None` when there are no more rows.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

/// Standard error response body.
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    /// Human-readable error message.
    pub error: String,
    /// Optional error code for programmatic handling.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
}

// ─── Router ──────────────────────────────────────────────────────────────────

/// Create the analytics API router with all endpoints.
pub fn create_analytics_api_router(state: AnalyticsApiState) -> Router {
    Router::new()
        .route("/api/v1/query", post(execute_query_handler))
        .route("/api/v1/query/explain", post(explain_query_handler))
        .route("/api/v1/query/cache/stats", get(cache_stats_handler))
        .route("/api/v1/query/cache", delete(clear_cache_handler))
        .route("/api/v1/views", post(create_view_handler))
        .route("/api/v1/views", get(list_views_handler))
        .route("/api/v1/views/:name", get(get_view_handler))
        .route("/api/v1/views/:name", delete(delete_view_handler))
        .route("/api/v1/views/:name/refresh", post(refresh_view_handler))
        .route("/api/v1/views/:name/query", get(query_view_handler))
        .with_state(state)
}

// ─── Handlers (analytics enabled) ────────────────────────────────────────────

/// Execute SQL query handler.
#[cfg(feature = "analytics")]
async fn execute_query_handler(
    State(state): State<AnalyticsApiState>,
    Json(request): Json<QueryRequest>,
) -> Result<Json<PaginatedQueryResponse>, ApiError> {
    info!(sql = %request.sql, "Executing SQL query");

    // Decode cursor to an offset if provided
    let offset = if let Some(ref cursor) = request.cursor {
        decode_cursor(cursor).map_err(|e| ApiError::Analytics(e))?
    } else {
        request.offset.unwrap_or(0)
    };

    let options = QueryOptions {
        max_rows: request.max_rows,
        enable_cache: request.enable_cache,
        cache_ttl_seconds: request.cache_ttl_seconds,
        timeout_ms: request.timeout_ms,
        offset,
    };

    let result = state
        .engine
        .execute_query(&request.sql, options)
        .await
        .map_err(|e| ApiError::from_analytics_error(e))?;

    info!(
        rows = result.row_count,
        total_rows = result.total_rows,
        execution_time_ms = result.execution_time_ms,
        from_cache = result.from_cache,
        "Query executed successfully"
    );

    // Build next cursor when there are more rows
    let next_cursor = if result.has_more {
        let next_offset = offset + result.row_count;
        Some(encode_cursor(next_offset))
    } else {
        None
    };

    let result_value = serde_json::to_value(&result)
        .map_err(|e| ApiError::Analytics(format!("Failed to serialize result: {}", e)))?;

    Ok(Json(PaginatedQueryResponse {
        result: result_value,
        next_cursor,
    }))
}

/// Fallback handler when analytics feature is disabled.
#[cfg(not(feature = "analytics"))]
async fn execute_query_handler(
    State(_state): State<AnalyticsApiState>,
    Json(_request): Json<QueryRequest>,
) -> Result<Json<PaginatedQueryResponse>, ApiError> {
    Err(ApiError::FeatureNotEnabled(
        "Analytics feature is not enabled. Compile with --features analytics".to_string(),
    ))
}

/// Get cache statistics handler.
#[cfg(feature = "analytics")]
async fn cache_stats_handler(
    State(state): State<AnalyticsApiState>,
) -> Result<Json<crate::analytics::duckdb::CacheStats>, ApiError> {
    let stats = state.engine.cache_stats();
    Ok(Json(stats))
}

#[cfg(not(feature = "analytics"))]
async fn cache_stats_handler(
    State(_state): State<AnalyticsApiState>,
) -> Result<Json<serde_json::Value>, ApiError> {
    Err(ApiError::FeatureNotEnabled(
        "Analytics feature is not enabled".to_string(),
    ))
}

/// Clear cache handler.
#[cfg(feature = "analytics")]
async fn clear_cache_handler(
    State(state): State<AnalyticsApiState>,
) -> Result<StatusCode, ApiError> {
    state.engine.clear_cache();
    info!("Query cache cleared");
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(not(feature = "analytics"))]
async fn clear_cache_handler(
    State(_state): State<AnalyticsApiState>,
) -> Result<StatusCode, ApiError> {
    Err(ApiError::FeatureNotEnabled(
        "Analytics feature is not enabled".to_string(),
    ))
}

/// EXPLAIN query plan handler.
#[cfg(feature = "analytics")]
async fn explain_query_handler(
    State(state): State<AnalyticsApiState>,
    Json(request): Json<ExplainRequest>,
) -> Result<Json<crate::analytics::ExplainResult>, ApiError> {
    info!(sql = %request.sql, "Explaining SQL query plan");

    let result = state
        .engine
        .explain_query(&request.sql)
        .await
        .map_err(|e| ApiError::from_analytics_error(e))?;

    Ok(Json(result))
}

#[cfg(not(feature = "analytics"))]
async fn explain_query_handler(
    State(_state): State<AnalyticsApiState>,
    Json(_request): Json<ExplainRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    Err(ApiError::FeatureNotEnabled(
        "Analytics feature is not enabled".to_string(),
    ))
}

/// Create materialized view handler.
#[cfg(feature = "analytics")]
async fn create_view_handler(
    State(state): State<AnalyticsApiState>,
    Json(request): Json<CreateViewRequest>,
) -> Result<StatusCode, ApiError> {
    info!(view = %request.name, "Creating materialized view");

    state
        .engine
        .create_materialized_view(
            &request.name,
            &request.query,
            request.refresh_interval_seconds,
        )
        .map_err(|e| ApiError::from_analytics_error(e))?;

    Ok(StatusCode::CREATED)
}

#[cfg(not(feature = "analytics"))]
async fn create_view_handler(
    State(_state): State<AnalyticsApiState>,
    Json(_request): Json<CreateViewRequest>,
) -> Result<StatusCode, ApiError> {
    Err(ApiError::FeatureNotEnabled(
        "Analytics feature is not enabled".to_string(),
    ))
}

/// List materialized views handler.
#[cfg(feature = "analytics")]
async fn list_views_handler(
    State(state): State<AnalyticsApiState>,
) -> Result<Json<Vec<crate::analytics::MaterializedView>>, ApiError> {
    let views = state.engine.list_materialized_views();
    Ok(Json(views))
}

#[cfg(not(feature = "analytics"))]
async fn list_views_handler(
    State(_state): State<AnalyticsApiState>,
) -> Result<Json<Vec<serde_json::Value>>, ApiError> {
    Err(ApiError::FeatureNotEnabled(
        "Analytics feature is not enabled".to_string(),
    ))
}

/// Refresh materialized view handler.
#[cfg(feature = "analytics")]
async fn refresh_view_handler(
    State(state): State<AnalyticsApiState>,
    Path(name): Path<String>,
) -> Result<StatusCode, ApiError> {
    info!(view = %name, "Refreshing materialized view");

    state
        .engine
        .refresh_materialized_view(&name)
        .await
        .map_err(|e| ApiError::from_analytics_error(e))?;

    Ok(StatusCode::NO_CONTENT)
}

#[cfg(not(feature = "analytics"))]
async fn refresh_view_handler(
    State(_state): State<AnalyticsApiState>,
    Path(_name): Path<String>,
) -> Result<StatusCode, ApiError> {
    Err(ApiError::FeatureNotEnabled(
        "Analytics feature is not enabled".to_string(),
    ))
}

/// Get a specific materialized view.
#[cfg(feature = "analytics")]
async fn get_view_handler(
    State(state): State<AnalyticsApiState>,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let views = state.engine.list_materialized_views();
    views
        .into_iter()
        .find(|v| v.name == name)
        .map(|v| {
            Json(serde_json::json!({
                "name": v.name,
                "query": v.query,
                "refresh_interval_seconds": v.refresh_interval_seconds,
                "last_refreshed": v.last_refreshed,
            }))
        })
        .ok_or_else(|| ApiError::NotFound(format!("View '{}' not found", name)))
}

#[cfg(not(feature = "analytics"))]
async fn get_view_handler(
    State(_state): State<AnalyticsApiState>,
    Path(_name): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    Err(ApiError::FeatureNotEnabled(
        "Analytics feature is not enabled".to_string(),
    ))
}

/// Delete a materialized view.
#[cfg(feature = "analytics")]
async fn delete_view_handler(
    State(state): State<AnalyticsApiState>,
    Path(name): Path<String>,
) -> Result<StatusCode, ApiError> {
    state
        .engine
        .drop_materialized_view(&name)
        .await
        .map_err(|e| ApiError::from_analytics_error(e))?;
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(not(feature = "analytics"))]
async fn delete_view_handler(
    State(_state): State<AnalyticsApiState>,
    Path(_name): Path<String>,
) -> Result<StatusCode, ApiError> {
    Err(ApiError::FeatureNotEnabled(
        "Analytics feature is not enabled".to_string(),
    ))
}

/// Query the current contents of a materialized view.
#[cfg(feature = "analytics")]
async fn query_view_handler(
    State(state): State<AnalyticsApiState>,
    Path(name): Path<String>,
) -> Result<Json<crate::analytics::QueryResult>, ApiError> {
    let views = state.engine.list_materialized_views();
    let view = views
        .into_iter()
        .find(|v| v.name == name)
        .ok_or_else(|| ApiError::NotFound(format!("View '{}' not found", name)))?;

    let result = state
        .engine
        .execute_query(&view.query, QueryOptions::default())
        .await
        .map_err(|e| ApiError::from_analytics_error(e))?;

    Ok(Json(result))
}

#[cfg(not(feature = "analytics"))]
async fn query_view_handler(
    State(_state): State<AnalyticsApiState>,
    Path(_name): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    Err(ApiError::FeatureNotEnabled(
        "Analytics feature is not enabled".to_string(),
    ))
}

// ─── Error handling ──────────────────────────────────────────────────────────

/// API error type with appropriate HTTP status code mapping.
#[derive(Debug)]
#[allow(dead_code)]
enum ApiError {
    /// An analytics engine error (invalid SQL, DuckDB failure, etc.).
    Analytics(String),
    /// The analytics feature is not compiled in.
    FeatureNotEnabled(String),
    /// A specific resource (e.g. materialized view) was not found.
    NotFound(String),
    /// The SQL query was invalid.
    InvalidSql(String),
    /// The query timed out.
    QueryTimeout(String),
}

#[cfg(feature = "analytics")]
impl ApiError {
    /// Map an `AnalyticsError` to the appropriate `ApiError` variant.
    fn from_analytics_error(e: streamline_analytics::error::AnalyticsError) -> Self {
        use streamline_analytics::error::AnalyticsError as AE;
        match e {
            AE::InvalidSql(msg) => ApiError::InvalidSql(msg),
            AE::TopicNotFound(topic) => ApiError::NotFound(format!("Topic '{}' not found", topic)),
            AE::QueryTimeout { timeout_ms } => {
                ApiError::QueryTimeout(format!("Query timed out after {}ms", timeout_ms))
            }
            other => ApiError::Analytics(other.to_string()),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, code, message) = match self {
            ApiError::Analytics(msg) => (StatusCode::INTERNAL_SERVER_ERROR, "ANALYTICS_ERROR", msg),
            ApiError::FeatureNotEnabled(msg) => (StatusCode::NOT_IMPLEMENTED, "FEATURE_DISABLED", msg),
            ApiError::NotFound(msg) => (StatusCode::NOT_FOUND, "NOT_FOUND", msg),
            ApiError::InvalidSql(msg) => (StatusCode::BAD_REQUEST, "INVALID_SQL", msg),
            ApiError::QueryTimeout(msg) => (StatusCode::GATEWAY_TIMEOUT, "QUERY_TIMEOUT", msg),
        };

        let body = Json(ErrorResponse {
            error: message,
            code: Some(code.to_string()),
        });

        (status, body).into_response()
    }
}

// ─── Cursor helpers ──────────────────────────────────────────────────────────

/// Encode a pagination offset into an opaque Base64 cursor string.
fn encode_cursor(offset: usize) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(offset.to_string())
}

/// Decode an opaque cursor string back into a pagination offset.
fn decode_cursor(cursor: &str) -> std::result::Result<usize, String> {
    use base64::Engine as _;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|e| format!("Invalid cursor: {}", e))?;
    let s = String::from_utf8(bytes).map_err(|e| format!("Invalid cursor encoding: {}", e))?;
    s.parse::<usize>()
        .map_err(|e| format!("Invalid cursor value: {}", e))
}
