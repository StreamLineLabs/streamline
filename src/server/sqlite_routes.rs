//! HTTP API routes for the SQLite query interface
//!
//! Provides REST endpoints for executing SQL queries against Streamline topics
//! using the embedded SQLite engine.
//!
//! ## Endpoints
//!
//! - `POST /api/v1/sqlite/query`       - Execute a SQL query
//! - `GET  /api/v1/sqlite/tables`      - List available tables (topics)
//! - `POST /api/v1/sqlite/sync/{topic}` - Force-sync a topic into SQLite

#[cfg(feature = "sqlite-queries")]
use crate::sqlite::SQLiteQueryEngine;

use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};

// ── Shared state ────────────────────────────────────────────────────────

/// Shared state for SQLite API handlers.
#[derive(Clone)]
pub struct SqliteApiState {
    #[cfg(feature = "sqlite-queries")]
    pub engine: Arc<SQLiteQueryEngine>,
}

// ── Request / Response types ────────────────────────────────────────────

/// Request body for `POST /api/v1/sqlite/query`.
#[derive(Debug, Deserialize)]
pub struct SqliteQueryRequest {
    /// The SQL query to execute.
    pub sql: String,
    /// Optional query timeout in milliseconds (default: 30 000).
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

/// Successful query response.
#[derive(Debug, Serialize)]
pub struct SqliteQueryResponse {
    /// Column names.
    pub columns: Vec<String>,
    /// Row data (each row is a list of nullable strings).
    pub rows: Vec<Vec<Option<String>>>,
    /// Number of rows returned.
    pub row_count: usize,
    /// Wall-clock execution time in milliseconds.
    pub execution_ms: u64,
}

/// Table listing entry.
#[derive(Debug, Serialize)]
pub struct SqliteTableEntry {
    pub name: String,
    pub columns: Vec<SqliteColumnEntry>,
    pub row_count: usize,
}

/// Column metadata.
#[derive(Debug, Serialize)]
pub struct SqliteColumnEntry {
    pub name: String,
    pub data_type: String,
}

/// Sync response.
#[derive(Debug, Serialize)]
pub struct SqliteSyncResponse {
    pub topic: String,
    pub records_synced: usize,
}

/// Error response body.
#[derive(Debug, Serialize)]
pub struct SqliteErrorResponse {
    pub error: String,
}

// ── Router creation ─────────────────────────────────────────────────────

/// Build the axum router for SQLite API routes.
pub fn create_sqlite_api_router(state: SqliteApiState) -> Router {
    Router::new()
        .route("/api/v1/sqlite/query", post(execute_query_handler))
        .route("/api/v1/query/sqlite", post(execute_query_alt_handler))
        .route("/api/v1/sqlite/tables", get(list_tables_handler))
        .route("/api/v1/sqlite/sync/:topic", post(sync_topic_handler))
        .with_state(state)
}

// ── Handler implementations (feature enabled) ───────────────────────────

/// `POST /api/v1/sqlite/query` -- execute a SQL query.
#[cfg(feature = "sqlite-queries")]
async fn execute_query_handler(
    State(state): State<SqliteApiState>,
    Json(request): Json<SqliteQueryRequest>,
) -> Result<Json<SqliteQueryResponse>, SqliteApiError> {
    execute_sqlite_query(state, request).await
}

/// `POST /api/v1/query/sqlite` -- alternate route for SQLite queries.
#[cfg(feature = "sqlite-queries")]
async fn execute_query_alt_handler(
    State(state): State<SqliteApiState>,
    Json(request): Json<SqliteQueryRequest>,
) -> Result<Json<SqliteQueryResponse>, SqliteApiError> {
    execute_sqlite_query(state, request).await
}

#[cfg(feature = "sqlite-queries")]
async fn execute_sqlite_query(
    state: SqliteApiState,
    request: SqliteQueryRequest,
) -> Result<Json<SqliteQueryResponse>, SqliteApiError> {
    tracing::info!(sql = %request.sql, "Executing SQLite query");

    let result = state
        .engine
        .execute_query(&request.sql, request.timeout_ms)
        .map_err(|e: crate::error::StreamlineError| SqliteApiError::Query(e.to_string()))?;

    tracing::info!(
        rows = result.row_count,
        execution_ms = result.execution_ms,
        "SQLite query executed"
    );

    Ok(Json(SqliteQueryResponse {
        columns: result.columns,
        rows: result.rows.into_iter().map(|r| r.values).collect(),
        row_count: result.row_count,
        execution_ms: result.execution_ms,
    }))
}

/// `GET /api/v1/sqlite/tables` -- list available tables.
#[cfg(feature = "sqlite-queries")]
async fn list_tables_handler(
    State(state): State<SqliteApiState>,
) -> Result<Json<Vec<SqliteTableEntry>>, SqliteApiError> {
    let tables = state
        .engine
        .list_tables()
        .map_err(|e: crate::error::StreamlineError| SqliteApiError::Query(e.to_string()))?;

    let entries: Vec<SqliteTableEntry> = tables
        .into_iter()
        .map(|t| SqliteTableEntry {
            name: t.name,
            columns: t
                .columns
                .into_iter()
                .map(|c| SqliteColumnEntry {
                    name: c.name,
                    data_type: c.data_type,
                })
                .collect(),
            row_count: t.row_count,
        })
        .collect();

    Ok(Json(entries))
}

/// `POST /api/v1/sqlite/sync/{topic}` -- force-sync a topic.
#[cfg(feature = "sqlite-queries")]
async fn sync_topic_handler(
    State(state): State<SqliteApiState>,
    Path(topic): Path<String>,
) -> Result<Json<SqliteSyncResponse>, SqliteApiError> {
    tracing::info!(topic = %topic, "Force-syncing topic to SQLite");

    let records_synced = state
        .engine
        .sync_topic(&topic)
        .map_err(|e: crate::error::StreamlineError| SqliteApiError::Query(e.to_string()))?;

    Ok(Json(SqliteSyncResponse {
        topic,
        records_synced,
    }))
}

// ── Fallback handlers (feature disabled) ────────────────────────────────

#[cfg(not(feature = "sqlite-queries"))]
async fn execute_query_handler(
    State(_state): State<SqliteApiState>,
    Json(_request): Json<SqliteQueryRequest>,
) -> Result<Json<SqliteQueryResponse>, SqliteApiError> {
    Err(SqliteApiError::FeatureNotEnabled(
        "sqlite-queries feature is not enabled. Compile with --features sqlite-queries".to_string(),
    ))
}

#[cfg(not(feature = "sqlite-queries"))]
async fn execute_query_alt_handler(
    State(_state): State<SqliteApiState>,
    Json(_request): Json<SqliteQueryRequest>,
) -> Result<Json<SqliteQueryResponse>, SqliteApiError> {
    Err(SqliteApiError::FeatureNotEnabled(
        "sqlite-queries feature is not enabled. Compile with --features sqlite-queries".to_string(),
    ))
}

#[cfg(not(feature = "sqlite-queries"))]
async fn list_tables_handler(
    State(_state): State<SqliteApiState>,
) -> Result<Json<Vec<SqliteTableEntry>>, SqliteApiError> {
    Err(SqliteApiError::FeatureNotEnabled(
        "sqlite-queries feature is not enabled. Compile with --features sqlite-queries".to_string(),
    ))
}

#[cfg(not(feature = "sqlite-queries"))]
async fn sync_topic_handler(
    State(_state): State<SqliteApiState>,
    Path(_topic): Path<String>,
) -> Result<Json<SqliteSyncResponse>, SqliteApiError> {
    Err(SqliteApiError::FeatureNotEnabled(
        "sqlite-queries feature is not enabled. Compile with --features sqlite-queries".to_string(),
    ))
}

// ── Error type ──────────────────────────────────────────────────────────

/// API error type for SQLite endpoints.
#[derive(Debug)]
pub enum SqliteApiError {
    /// A query-time error (bad SQL, timeout, storage issue).
    Query(String),
    /// The feature flag is not compiled in.
    FeatureNotEnabled(String),
}

impl IntoResponse for SqliteApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            SqliteApiError::Query(msg) => (StatusCode::BAD_REQUEST, msg),
            SqliteApiError::FeatureNotEnabled(msg) => (StatusCode::NOT_IMPLEMENTED, msg),
        };
        let body = Json(SqliteErrorResponse { error: message });
        (status, body).into_response()
    }
}
