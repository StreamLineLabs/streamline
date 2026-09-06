//! Unified Query API — POST /api/v1/query
//!
//! Implements ADR-008: single SQL endpoint for batch and streaming queries.
//! Delegates to the StreamQL engine for actual query execution.

use axum::{
    extract::{Json, State},
    http::{header::CONTENT_TYPE, StatusCode},
    response::IntoResponse,
    routing::post,
    Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{debug, error, info};

use crate::streamql::StreamQL;
use base64::Engine;

#[derive(Clone)]
pub struct QueryApiState {
    pub streamql: Arc<StreamQL>,
}

impl QueryApiState {
    pub fn new(streamql: Arc<StreamQL>) -> Self {
        Self { streamql }
    }
}

#[derive(Debug, Deserialize)]
pub struct QueryRequest {
    pub sql: String,
    #[serde(default = "default_timeout")]
    pub timeout_ms: u64,
    #[serde(default = "default_max_rows")]
    pub max_rows: usize,
    #[serde(default)]
    pub format: QueryFormat,
}

fn default_timeout() -> u64 {
    30000
}
fn default_max_rows() -> usize {
    10000
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum QueryFormat {
    #[default]
    Json,
    Csv,
}

#[derive(Debug, Serialize)]
pub struct QueryResponse {
    pub columns: Vec<ColumnInfo>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub metadata: QueryMetadata,
}

#[derive(Debug, Serialize)]
pub struct ColumnInfo {
    pub name: String,
    #[serde(rename = "type")]
    pub col_type: String,
}

#[derive(Debug, Serialize)]
pub struct QueryMetadata {
    pub execution_time_ms: u64,
    pub rows_scanned: u64,
    pub rows_returned: usize,
    pub truncated: bool,
}

#[derive(Debug, Serialize)]
pub struct QueryError {
    pub code: String,
    pub message: String,
    pub line: Option<usize>,
    pub column: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct ExplainRequest {
    pub sql: String,
}

#[derive(Debug, Serialize)]
pub struct ExplainResponse {
    pub plan: String,
    pub estimated_cost: f64,
}

async fn execute_query(
    State(state): State<QueryApiState>,
    Json(request): Json<QueryRequest>,
) -> impl IntoResponse {
    let start = std::time::Instant::now();

    // Validate SQL is not empty
    let sql = request.sql.trim();
    if sql.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": { "code": "EMPTY_QUERY", "message": "SQL query cannot be empty" }
            })),
        )
            .into_response();
    }

    // Streaming queries should use the dedicated stream endpoint
    if sql.to_uppercase().contains("EMIT CHANGES") {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({
            "error": { "code": "USE_STREAM_ENDPOINT", "message": "Streaming queries should use POST /api/v1/query/stream" }
        }))).into_response();
    }

    debug!(sql, "Executing unified query");

    // StreamQL currently performs synchronous storage scans internally. Run it
    // on the blocking pool so the request deadline is observable and the async
    // HTTP worker is not occupied by the scan.
    let streamql = state.streamql.clone();
    let sql_owned = sql.to_string();
    let runtime = tokio::runtime::Handle::current();
    let query_task =
        tokio::task::spawn_blocking(move || runtime.block_on(streamql.execute(&sql_owned)));

    match tokio::time::timeout(
        std::time::Duration::from_millis(request.timeout_ms),
        query_task,
    )
    .await
    {
        Ok(Ok(Ok(result))) => {
            let columns: Vec<ColumnInfo> = result
                .schema
                .columns
                .iter()
                .map(|c| ColumnInfo {
                    name: c.name.clone(),
                    col_type: format!("{:?}", c.data_type).to_lowercase(),
                })
                .collect();

            let max_rows = request.max_rows;
            let total_rows = result.rows.len();
            let truncated = total_rows > max_rows;

            let rows: Vec<Vec<serde_json::Value>> = result
                .rows
                .iter()
                .take(max_rows)
                .map(|row| {
                    row.values
                        .iter()
                        .map(|v| match v {
                            crate::streamql::types::Value::Null => serde_json::Value::Null,
                            crate::streamql::types::Value::Boolean(b) => {
                                serde_json::Value::Bool(*b)
                            }
                            crate::streamql::types::Value::Int64(i) => serde_json::json!(i),
                            crate::streamql::types::Value::Float64(f) => serde_json::json!(f),
                            crate::streamql::types::Value::String(s) => {
                                serde_json::Value::String(s.clone())
                            }
                            crate::streamql::types::Value::Timestamp(ts) => serde_json::json!(ts),
                            crate::streamql::types::Value::Binary(b) => serde_json::json!(
                                base64::engine::general_purpose::STANDARD.encode(b)
                            ),
                            crate::streamql::types::Value::Date(d) => serde_json::json!(d),
                            crate::streamql::types::Value::Duration(d) => serde_json::json!(d),
                            crate::streamql::types::Value::Json(j) => serde_json::json!(j),
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

            let rows_returned = rows.len();
            let elapsed = start.elapsed().as_millis() as u64;
            info!(
                rows_returned,
                elapsed_ms = elapsed,
                "Unified query completed"
            );

            let response = QueryResponse {
                columns,
                rows,
                metadata: QueryMetadata {
                    execution_time_ms: elapsed,
                    rows_scanned: result.stats.rows_scanned,
                    rows_returned,
                    truncated,
                },
            };

            match request.format {
                QueryFormat::Json => (StatusCode::OK, Json(response)).into_response(),
                QueryFormat::Csv => (
                    StatusCode::OK,
                    [(CONTENT_TYPE, "text/csv; charset=utf-8")],
                    render_csv(&response),
                )
                    .into_response(),
            }
        }
        Ok(Ok(Err(e))) => {
            error!(error = %e, "Query execution failed");
            (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": { "code": "QUERY_ERROR", "message": format!("{e}") }
                })),
            )
                .into_response()
        }
        Ok(Err(e)) => {
            error!(error = %e, "Query task failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": {
                        "code": "QUERY_TASK_FAILED",
                        "message": "Query execution task failed"
                    }
                })),
            )
                .into_response()
        }
        Err(_) => (
            StatusCode::REQUEST_TIMEOUT,
            Json(serde_json::json!({
                "error": {
                    "code": "QUERY_TIMEOUT",
                    "message": format!("Query exceeded timeout of {}ms", request.timeout_ms)
                }
            })),
        )
            .into_response(),
    }
}

fn render_csv(response: &QueryResponse) -> String {
    let mut output = String::new();
    output.push_str(
        &response
            .columns
            .iter()
            .map(|column| escape_csv(&column.name))
            .collect::<Vec<_>>()
            .join(","),
    );
    output.push('\n');

    for row in &response.rows {
        output.push_str(
            &row.iter()
                .map(|value| {
                    let value = match value {
                        serde_json::Value::Null => String::new(),
                        serde_json::Value::String(value) => value.clone(),
                        value => value.to_string(),
                    };
                    escape_csv(&value)
                })
                .collect::<Vec<_>>()
                .join(","),
        );
        output.push('\n');
    }

    output
}

fn escape_csv(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') || value.contains('\r') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

async fn explain_query(
    State(state): State<QueryApiState>,
    Json(request): Json<ExplainRequest>,
) -> impl IntoResponse {
    match state.streamql.explain(&request.sql) {
        Ok(plan) => (
            StatusCode::OK,
            Json(ExplainResponse {
                plan,
                estimated_cost: 1.0,
            }),
        )
            .into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": { "code": "EXPLAIN_ERROR", "message": format!("{e}") }
            })),
        )
            .into_response(),
    }
}

/// Build the unified query API router.
pub fn query_router() -> Router<QueryApiState> {
    Router::new()
        .route("/api/v1/query", post(execute_query))
        .route("/api/v1/query/explain", post(explain_query))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_format_default() {
        let format = QueryFormat::default();
        matches!(format, QueryFormat::Json);
    }

    #[test]
    fn test_query_request_deserialization() {
        let json = r#"{"sql": "SELECT 1", "timeout_ms": 5000, "max_rows": 100}"#;
        let req: QueryRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.sql, "SELECT 1");
        assert_eq!(req.timeout_ms, 5000);
        assert_eq!(req.max_rows, 100);
    }

    #[test]
    fn test_query_request_defaults() {
        let json = r#"{"sql": "SELECT 1"}"#;
        let req: QueryRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.timeout_ms, 30000);
        assert_eq!(req.max_rows, 10000);
    }

    #[test]
    fn test_render_csv_escapes_values() {
        let response = QueryResponse {
            columns: vec![
                ColumnInfo {
                    name: "name".to_string(),
                    col_type: "string".to_string(),
                },
                ColumnInfo {
                    name: "count".to_string(),
                    col_type: "int".to_string(),
                },
            ],
            rows: vec![vec![
                serde_json::json!("quoted, \"value\""),
                serde_json::json!(2),
            ]],
            metadata: QueryMetadata {
                execution_time_ms: 1,
                rows_scanned: 1,
                rows_returned: 1,
                truncated: false,
            },
        };

        assert_eq!(
            render_csv(&response),
            "name,count\n\"quoted, \"\"value\"\"\",2\n"
        );
    }

    #[test]
    fn test_query_response_serialization() {
        let response = QueryResponse {
            columns: vec![ColumnInfo {
                name: "value".to_string(),
                col_type: "integer".to_string(),
            }],
            rows: vec![vec![serde_json::json!(1)]],
            metadata: QueryMetadata {
                execution_time_ms: 2,
                rows_scanned: 1,
                rows_returned: 1,
                truncated: false,
            },
        };

        let value = serde_json::to_value(response).unwrap();
        assert_eq!(value["columns"][0]["name"], "value");
        assert_eq!(value["rows"][0], serde_json::json!([1]));
        assert_eq!(value["metadata"]["rows_returned"], 1);
    }

    #[test]
    fn test_explain_request() {
        let json = r#"{"sql": "SELECT * FROM topic('events')"}"#;
        let req: ExplainRequest = serde_json::from_str(json).unwrap();
        assert!(req.sql.contains("events"));
    }
}
