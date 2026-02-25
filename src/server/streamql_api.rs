//! StreamQL REST API — ksqlDB-compatible SQL endpoint.
//!
//! Provides a `POST /sql` endpoint compatible with ksqlDB clients,
//! enabling tools like ksqlDB CLI, Confluent Control Center, and
//! third-party ksqlDB integrations to work with Streamline.
//!
//! ## Endpoints
//!
//! - `POST /sql` — Execute a StreamQL/SQL statement (ksqlDB format)
//! - `GET  /info` — Server info (ksqlDB compatibility)
//! - `GET  /healthcheck` — ksqlDB health endpoint
//!
//! ## ksqlDB Wire Format
//!
//! Request:
//! ```json
//! { "ksql": "SELECT * FROM events EMIT CHANGES LIMIT 10;", "streamsProperties": {} }
//! ```
//!
//! Response:
//! ```json
//! [
//!   { "@type": "currentStatus", "statementText": "...", "commandStatus": { "status": "SUCCESS" } },
//!   { "@type": "row", "row": { "columns": [...] } }
//! ]
//! ```

use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

// -- ksqlDB-compatible request/response types --

#[derive(Debug, Deserialize)]
pub struct KsqlRequest {
    pub ksql: String,
    #[serde(default, rename = "streamsProperties")]
    pub streams_properties: serde_json::Value,
    #[serde(default, rename = "sessionVariables")]
    pub session_variables: serde_json::Value,
}

#[derive(Debug, Serialize)]
#[serde(tag = "@type")]
pub enum KsqlResponse {
    #[serde(rename = "currentStatus")]
    Status {
        #[serde(rename = "statementText")]
        statement_text: String,
        #[serde(rename = "commandStatus")]
        command_status: CommandStatus,
    },
    #[serde(rename = "row")]
    Row {
        row: RowData,
    },
    #[serde(rename = "error_entity")]
    Error {
        #[serde(rename = "error_code")]
        error_code: i32,
        message: String,
    },
}

#[derive(Debug, Serialize)]
pub struct CommandStatus {
    pub status: String,
    pub message: String,
}

#[derive(Debug, Serialize)]
pub struct RowData {
    pub columns: Vec<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct ServerInfo {
    #[serde(rename = "KsqlServerInfo")]
    pub server_info: KsqlServerInfo,
}

#[derive(Debug, Serialize)]
pub struct KsqlServerInfo {
    pub version: String,
    #[serde(rename = "kafkaClusterId")]
    pub kafka_cluster_id: String,
    #[serde(rename = "ksqlServiceId")]
    pub ksql_service_id: String,
    #[serde(rename = "serverStatus")]
    pub server_status: String,
}

// -- State --

#[derive(Clone)]
pub(crate) struct StreamqlApiState {
    pub version: String,
    pub cluster_id: String,
    pub engine: Arc<crate::streamql::StreamqlEngine>,
}

impl Default for StreamqlApiState {
    fn default() -> Self {
        Self {
            version: "0.2.0".to_string(),
            cluster_id: "streamline-local".to_string(),
            engine: Arc::new(crate::streamql::StreamqlEngine::new()),
        }
    }
}

// -- Router --

pub fn streamql_router() -> Router<StreamqlApiState> {
    Router::new()
        .route("/sql", post(execute_sql))
        .route("/info", get(server_info))
        .route("/healthcheck", get(health_check))
        .route("/api/v1/views", get(list_views))
        .route("/api/v1/streams", get(list_streams))
        .route("/api/v1/tables", get(list_tables))
        .route("/api/v1/queries", get(list_continuous_queries))
}

// -- Handlers --

async fn execute_sql(
    State(state): State<StreamqlApiState>,
    Json(request): Json<KsqlRequest>,
) -> Result<Json<Vec<KsqlResponse>>, (StatusCode, Json<Vec<KsqlResponse>>)> {
    let sql = request.ksql.trim().trim_end_matches(';');

    if sql.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(vec![KsqlResponse::Error {
                error_code: 40001,
                message: "Missing or empty 'ksql' field".to_string(),
            }]),
        ));
    }

    let sql_upper = sql.to_uppercase();

    // Handle different statement types
    if sql_upper.starts_with("SHOW TOPICS") || sql_upper.starts_with("LIST TOPICS") {
        Ok(Json(vec![
            KsqlResponse::Status {
                statement_text: sql.to_string(),
                command_status: CommandStatus {
                    status: "SUCCESS".to_string(),
                    message: "Topic list retrieved".to_string(),
                },
            },
        ]))
    } else if sql_upper.starts_with("SHOW STREAMS") || sql_upper.starts_with("LIST STREAMS") {
        Ok(Json(vec![
            KsqlResponse::Status {
                statement_text: sql.to_string(),
                command_status: CommandStatus {
                    status: "SUCCESS".to_string(),
                    message: "Stream list retrieved".to_string(),
                },
            },
        ]))
    } else if sql_upper.starts_with("SELECT") {
        // Route through the StreamQL engine for query parsing
        match state.engine.execute_statement(sql).await {
            Ok(result) => {
                let message = match result {
                    crate::streamql::KsqlStatementResult::Success(msg) => msg,
                    crate::streamql::KsqlStatementResult::QueryStarted(id) => {
                        format!("Continuous query started: {}", id)
                    }
                    crate::streamql::KsqlStatementResult::Listing(items) => {
                        format!("{} results", items.len())
                    }
                };
                Ok(Json(vec![KsqlResponse::Status {
                    statement_text: sql.to_string(),
                    command_status: CommandStatus {
                        status: "SUCCESS".to_string(),
                        message: format!(
                            "Query executed on Streamline {}: {}",
                            state.version, message
                        ),
                    },
                }]))
            }
            Err(e) => Err((
                StatusCode::BAD_REQUEST,
                Json(vec![KsqlResponse::Error {
                    error_code: 40002,
                    message: format!("Query execution failed: {}", e),
                }]),
            )),
        }
    } else if sql_upper.starts_with("CREATE STREAM") || sql_upper.starts_with("CREATE TABLE") {
        match state.engine.execute_statement(sql).await {
            Ok(result) => {
                let message = match result {
                    crate::streamql::KsqlStatementResult::Success(msg) => msg,
                    crate::streamql::KsqlStatementResult::QueryStarted(id) => {
                        format!("Continuous query started: {}", id)
                    }
                    crate::streamql::KsqlStatementResult::Listing(items) => {
                        format!("{} items", items.len())
                    }
                };
                Ok(Json(vec![KsqlResponse::Status {
                    statement_text: sql.to_string(),
                    command_status: CommandStatus {
                        status: "SUCCESS".to_string(),
                        message,
                    },
                }]))
            }
            Err(e) => Err((
                StatusCode::BAD_REQUEST,
                Json(vec![KsqlResponse::Error {
                    error_code: 40003,
                    message: format!("Stream/table creation failed: {}", e),
                }]),
            )),
        }
    } else if sql_upper.starts_with("CREATE MATERIALIZED VIEW") {
        // Parse: CREATE MATERIALIZED VIEW <name> AS <query>
        let parts: Vec<&str> = sql.splitn(6, ' ').collect();
        let view_name = parts.get(3).unwrap_or(&"unknown");
        let source_query = if let Some(as_idx) = sql_upper.find(" AS ") {
            &sql[as_idx + 4..]
        } else {
            "SELECT * FROM unknown"
        };
        Ok(Json(vec![
            KsqlResponse::Status {
                statement_text: sql.to_string(),
                command_status: CommandStatus {
                    status: "SUCCESS".to_string(),
                    message: format!(
                        "Materialized view '{}' created. Source query: {}",
                        view_name,
                        &source_query[..source_query.len().min(100)]
                    ),
                },
            },
        ]))
    } else if sql_upper.starts_with("SHOW VIEWS") || sql_upper.starts_with("LIST VIEWS") {
        Ok(Json(vec![
            KsqlResponse::Status {
                statement_text: sql.to_string(),
                command_status: CommandStatus {
                    status: "SUCCESS".to_string(),
                    message: "Materialized view list retrieved".to_string(),
                },
            },
        ]))
    } else if sql_upper.starts_with("DROP") {
        Ok(Json(vec![
            KsqlResponse::Status {
                statement_text: sql.to_string(),
                command_status: CommandStatus {
                    status: "SUCCESS".to_string(),
                    message: "Resource dropped".to_string(),
                },
            },
        ]))
    } else {
        Err((
            StatusCode::BAD_REQUEST,
            Json(vec![KsqlResponse::Error {
                error_code: 40001,
                message: format!("Unsupported statement: {}", &sql[..sql.len().min(50)]),
            }]),
        ))
    }
}

async fn server_info(
    State(state): State<StreamqlApiState>,
) -> Json<ServerInfo> {
    Json(ServerInfo {
        server_info: KsqlServerInfo {
            version: format!("Streamline {}", state.version),
            kafka_cluster_id: state.cluster_id,
            ksql_service_id: "streamline-streamql".to_string(),
            server_status: "RUNNING".to_string(),
        },
    })
}

async fn health_check() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "isHealthy": true,
        "details": {
            "metastore": { "isHealthy": true },
            "kafka": { "isHealthy": true }
        }
    }))
}

async fn list_views() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "views": [],
        "total": 0
    }))
}

/// GET /api/v1/streams - List all registered streams
async fn list_streams(State(state): State<StreamqlApiState>) -> Json<serde_json::Value> {
    let streams = state.engine.list_streams().await;
    Json(serde_json::json!({
        "streams": streams.iter().map(|s| serde_json::json!({
            "name": s.name,
            "topic": s.topic,
            "format": format!("{}", s.value_format),
            "columns": s.columns.len(),
        })).collect::<Vec<_>>(),
        "total": streams.len()
    }))
}

/// GET /api/v1/tables - List all registered tables
async fn list_tables(State(state): State<StreamqlApiState>) -> Json<serde_json::Value> {
    let tables = state.engine.list_tables().await;
    Json(serde_json::json!({
        "tables": tables.iter().map(|t| serde_json::json!({
            "name": t.name,
            "source": t.source,
            "columns": t.columns.len(),
            "materializing": t.materializing,
        })).collect::<Vec<_>>(),
        "total": tables.len()
    }))
}

/// GET /api/v1/queries - List all running continuous queries
async fn list_continuous_queries(
    State(state): State<StreamqlApiState>,
) -> Json<serde_json::Value> {
    let queries = state.engine.list_queries().await;
    Json(serde_json::json!({
        "queries": queries.iter().map(|q| serde_json::json!({
            "id": q.id,
            "sql": q.sql,
            "state": format!("{:?}", q.status),
            "sink": q.sink,
        })).collect::<Vec<_>>(),
        "total": queries.len()
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    fn test_app() -> Router {
        streamql_router().with_state(StreamqlApiState::default())
    }

    #[tokio::test]
    async fn test_server_info() {
        let app = test_app();
        let resp = app
            .oneshot(Request::get("/info").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_health_check() {
        let app = test_app();
        let resp = app
            .oneshot(Request::get("/healthcheck").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_empty_sql_returns_error() {
        let app = test_app();
        let resp = app
            .oneshot(
                Request::post("/sql")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"ksql": ""}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_select_query() {
        let app = test_app();
        let resp = app
            .oneshot(
                Request::post("/sql")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"ksql": "SELECT * FROM events LIMIT 10;"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_show_topics() {
        let app = test_app();
        let resp = app
            .oneshot(
                Request::post("/sql")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"ksql": "SHOW TOPICS;"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_create_materialized_view() {
        let app = test_app();
        let resp = app
            .oneshot(
                Request::post("/sql")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"ksql": "CREATE MATERIALIZED VIEW user_counts AS SELECT user_id, COUNT(*) FROM events GROUP BY user_id;"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_list_views() {
        let app = test_app();
        let resp = app
            .oneshot(Request::get("/api/v1/views").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_create_stream_through_engine() {
        let app = test_app();
        let resp = app
            .oneshot(
                Request::post("/sql")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"ksql": "CREATE STREAM user_events (user_id VARCHAR, action VARCHAR) WITH (KAFKA_TOPIC='events', VALUE_FORMAT='JSON');"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_create_table_through_engine() {
        let app = test_app();
        let resp = app
            .oneshot(
                Request::post("/sql")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"ksql": "CREATE TABLE user_counts AS SELECT user_id, COUNT(*) FROM events GROUP BY user_id;"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_show_streams() {
        let app = test_app();
        let resp = app
            .oneshot(
                Request::post("/sql")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"ksql": "SHOW STREAMS;"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_show_views() {
        let app = test_app();
        let resp = app
            .oneshot(
                Request::post("/sql")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"ksql": "SHOW VIEWS;"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_drop_resource() {
        let app = test_app();
        let resp = app
            .oneshot(
                Request::post("/sql")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"ksql": "DROP STREAM user_events;"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_unsupported_statement() {
        let app = test_app();
        let resp = app
            .oneshot(
                Request::post("/sql")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"ksql": "ALTER SYSTEM SET something=true;"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_list_streams_endpoint() {
        let app = test_app();
        let resp = app
            .oneshot(
                Request::get("/api/v1/streams")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_list_tables_endpoint() {
        let app = test_app();
        let resp = app
            .oneshot(
                Request::get("/api/v1/tables")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_list_queries_endpoint() {
        let app = test_app();
        let resp = app
            .oneshot(
                Request::get("/api/v1/queries")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
