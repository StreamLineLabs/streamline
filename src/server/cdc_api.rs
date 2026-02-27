//! CDC Management REST API — GA operational surface for CDC connectors
//!
//! ## Connector Endpoints
//!
//! - `GET  /api/v1/cdc/connectors`               - List all CDC connectors
//! - `POST /api/v1/cdc/connectors`               - Create a new CDC connector
//! - `GET  /api/v1/cdc/connectors/:name`          - Get connector info/status
//! - `DELETE /api/v1/cdc/connectors/:name`        - Delete a connector
//! - `POST /api/v1/cdc/connectors/:name/start`    - Start capturing changes
//! - `POST /api/v1/cdc/connectors/:name/stop`     - Stop capturing changes
//! - `POST /api/v1/cdc/connectors/:name/restart`  - Restart connector (snapshot + stream)
//! - `GET  /api/v1/cdc/connectors/:name/status`   - Detailed connector status with lag/position
//! - `GET  /api/v1/cdc/connectors/:name/schema`   - Get captured table schemas
//!
//! ## Dead Letter Queue (DLQ) Endpoints
//!
//! - `GET  /api/v1/cdc/dlq`                       - List DLQ entries across all connectors
//! - `POST /api/v1/cdc/dlq/:id/retry`             - Retry a DLQ entry
//! - `POST /api/v1/cdc/dlq/:id/skip`              - Skip/discard a DLQ entry

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::storage::TopicManager;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Supported CDC database connector types.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum CdcConnectorType {
    PostgreSQL,
    MySQL,
    MongoDB,
    SqlServer,
}

/// Lifecycle status of a CDC connector.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ConnectorStatus {
    Created,
    Running,
    Stopped,
    Failed(String),
    Snapshotting,
}

/// Configuration for a CDC connector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcConnectorConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub tables: Vec<String>,
    pub output_topic_prefix: String,
    /// PostgreSQL replication slot name.
    #[serde(default)]
    pub slot_name: Option<String>,
    /// Snapshot mode: initial, never, when_needed.
    #[serde(default = "default_snapshot_mode")]
    pub snapshot_mode: String,
    /// Output format: debezium, plain.
    #[serde(default = "default_output_format")]
    pub output_format: String,
}

fn default_snapshot_mode() -> String {
    "initial".to_string()
}
fn default_output_format() -> String {
    "debezium".to_string()
}

/// Runtime statistics for a CDC connector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorStats {
    pub events_captured: u64,
    pub events_published: u64,
    pub events_failed: u64,
    pub lag_ms: u64,
    pub last_event_at: Option<String>,
    /// Snapshot progress from 0.0 to 1.0 while snapshotting.
    pub snapshot_progress: Option<f64>,
}

impl Default for ConnectorStats {
    fn default() -> Self {
        Self {
            events_captured: 0,
            events_published: 0,
            events_failed: 0,
            lag_ms: 0,
            last_event_at: None,
            snapshot_progress: None,
        }
    }
}

/// Full information about a CDC connector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcConnectorInfo {
    pub name: String,
    pub connector_type: CdcConnectorType,
    pub config: CdcConnectorConfig,
    pub status: ConnectorStatus,
    pub created_at: String,
    pub tables: Vec<String>,
    pub stats: ConnectorStats,
}

/// Request body for creating a new CDC connector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateConnectorRequest {
    pub name: String,
    pub connector_type: CdcConnectorType,
    pub host: String,
    pub port: u16,
    pub database: String,
    #[serde(default)]
    pub tables: Vec<String>,
    #[serde(default = "default_topic_prefix")]
    pub output_topic_prefix: String,
    #[serde(default)]
    pub slot_name: Option<String>,
    #[serde(default = "default_snapshot_mode")]
    pub snapshot_mode: String,
    #[serde(default = "default_output_format")]
    pub output_format: String,
}

fn default_topic_prefix() -> String {
    "cdc".to_string()
}

/// A dead-letter queue entry for a failed CDC event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqEntry {
    pub id: String,
    pub connector_name: String,
    pub error: String,
    pub event_data: serde_json::Value,
    pub failed_at: String,
    pub retry_count: u32,
}

/// Captured table schema information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapturedTableSchema {
    pub table: String,
    pub columns: Vec<SchemaColumn>,
    pub primary_key: Vec<String>,
    pub topic_name: String,
}

/// Column metadata within a captured table schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaColumn {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

/// Shared state for the CDC management API.
#[derive(Clone)]
pub struct CdcApiState {
    connectors: Arc<RwLock<HashMap<String, CdcConnectorInfo>>>,
    dlq_entries: Arc<RwLock<Vec<DlqEntry>>>,
    /// Optional topic manager for reading/writing captured changes.
    pub topic_manager: Option<Arc<TopicManager>>,
}

impl CdcApiState {
    pub fn new() -> Self {
        Self {
            connectors: Arc::new(RwLock::new(HashMap::new())),
            dlq_entries: Arc::new(RwLock::new(Vec::new())),
            topic_manager: None,
        }
    }

    /// Create a new CdcApiState wired to the given TopicManager.
    pub fn with_topic_manager(topic_manager: Arc<TopicManager>) -> Self {
        Self {
            connectors: Arc::new(RwLock::new(HashMap::new())),
            dlq_entries: Arc::new(RwLock::new(Vec::new())),
            topic_manager: Some(topic_manager),
        }
    }
}

impl Default for CdcApiState {
    fn default() -> Self {
        Self::new()
    }
}

/// Standard error response body.
#[derive(Debug, Serialize, Deserialize)]
struct ErrorResponse {
    error: String,
}

/// Generic message response body.
#[derive(Debug, Serialize, Deserialize)]
struct MessageResponse {
    message: String,
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

/// Create the CDC management router with all connector and DLQ routes.
pub fn create_cdc_router() -> Router {
    let state = CdcApiState::new();
    create_cdc_router_with_state(state)
}

/// Create the CDC management router with an externally-provided state.
pub fn create_cdc_router_with_state(state: CdcApiState) -> Router {
    Router::new()
        // Connector CRUD
        .route(
            "/api/v1/cdc/connectors",
            get(list_connectors).post(create_connector),
        )
        .route(
            "/api/v1/cdc/connectors/:name",
            get(get_connector).delete(delete_connector),
        )
        // Connector lifecycle
        .route(
            "/api/v1/cdc/connectors/:name/start",
            post(start_connector),
        )
        .route("/api/v1/cdc/connectors/:name/stop", post(stop_connector))
        .route(
            "/api/v1/cdc/connectors/:name/restart",
            post(restart_connector),
        )
        // Connector observability
        .route(
            "/api/v1/cdc/connectors/:name/status",
            get(get_connector_status),
        )
        .route(
            "/api/v1/cdc/connectors/:name/schema",
            get(get_connector_schema),
        )
        // DLQ
        .route("/api/v1/cdc/dlq", get(list_dlq_entries))
        .route("/api/v1/cdc/dlq/:id/retry", post(retry_dlq_entry))
        .route("/api/v1/cdc/dlq/:id/skip", post(skip_dlq_entry))
        .with_state(state)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339()
}

fn not_found(name: &str) -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::NOT_FOUND,
        Json(ErrorResponse {
            error: format!("Connector '{}' not found", name),
        }),
    )
}

// ---------------------------------------------------------------------------
// Connector handlers
// ---------------------------------------------------------------------------

/// GET /api/v1/cdc/connectors
async fn list_connectors(State(state): State<CdcApiState>) -> Json<Vec<CdcConnectorInfo>> {
    let connectors = state.connectors.read().await;
    let list: Vec<CdcConnectorInfo> = connectors.values().cloned().collect();
    Json(list)
}

/// POST /api/v1/cdc/connectors
async fn create_connector(
    State(state): State<CdcApiState>,
    Json(req): Json<CreateConnectorRequest>,
) -> Result<(StatusCode, Json<CdcConnectorInfo>), (StatusCode, Json<ErrorResponse>)> {
    if req.name.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "Connector name must not be empty".to_string(),
            }),
        ));
    }

    let mut connectors = state.connectors.write().await;
    if connectors.contains_key(&req.name) {
        return Err((
            StatusCode::CONFLICT,
            Json(ErrorResponse {
                error: format!("Connector '{}' already exists", req.name),
            }),
        ));
    }

    let info = CdcConnectorInfo {
        name: req.name.clone(),
        connector_type: req.connector_type.clone(),
        config: CdcConnectorConfig {
            host: req.host,
            port: req.port,
            database: req.database,
            tables: req.tables.clone(),
            output_topic_prefix: req.output_topic_prefix,
            slot_name: req.slot_name,
            snapshot_mode: req.snapshot_mode,
            output_format: req.output_format,
        },
        status: ConnectorStatus::Created,
        created_at: now_rfc3339(),
        tables: req.tables,
        stats: ConnectorStats::default(),
    };

    tracing::info!(connector = %info.name, "CDC connector created");
    connectors.insert(info.name.clone(), info.clone());

    Ok((StatusCode::CREATED, Json(info)))
}

/// GET /api/v1/cdc/connectors/:name
async fn get_connector(
    State(state): State<CdcApiState>,
    Path(name): Path<String>,
) -> Result<Json<CdcConnectorInfo>, (StatusCode, Json<ErrorResponse>)> {
    let connectors = state.connectors.read().await;
    connectors
        .get(&name)
        .cloned()
        .map(Json)
        .ok_or_else(|| not_found(&name))
}

/// DELETE /api/v1/cdc/connectors/:name
async fn delete_connector(
    State(state): State<CdcApiState>,
    Path(name): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let mut connectors = state.connectors.write().await;
    if connectors.remove(&name).is_some() {
        tracing::info!(connector = %name, "CDC connector deleted");
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(not_found(&name))
    }
}

/// POST /api/v1/cdc/connectors/:name/start
async fn start_connector(
    State(state): State<CdcApiState>,
    Path(name): Path<String>,
) -> Result<Json<CdcConnectorInfo>, (StatusCode, Json<ErrorResponse>)> {
    let mut connectors = state.connectors.write().await;
    let connector = connectors.get_mut(&name).ok_or_else(|| not_found(&name))?;

    connector.status = ConnectorStatus::Running;
    tracing::info!(connector = %name, "CDC connector started");

    Ok(Json(connector.clone()))
}

/// POST /api/v1/cdc/connectors/:name/stop
async fn stop_connector(
    State(state): State<CdcApiState>,
    Path(name): Path<String>,
) -> Result<Json<CdcConnectorInfo>, (StatusCode, Json<ErrorResponse>)> {
    let mut connectors = state.connectors.write().await;
    let connector = connectors.get_mut(&name).ok_or_else(|| not_found(&name))?;

    connector.status = ConnectorStatus::Stopped;
    tracing::info!(connector = %name, "CDC connector stopped");

    Ok(Json(connector.clone()))
}

/// POST /api/v1/cdc/connectors/:name/restart
async fn restart_connector(
    State(state): State<CdcApiState>,
    Path(name): Path<String>,
) -> Result<Json<CdcConnectorInfo>, (StatusCode, Json<ErrorResponse>)> {
    let mut connectors = state.connectors.write().await;
    let connector = connectors.get_mut(&name).ok_or_else(|| not_found(&name))?;

    connector.status = ConnectorStatus::Snapshotting;
    connector.stats.snapshot_progress = Some(0.0);
    tracing::info!(connector = %name, "CDC connector restarting with snapshot");

    Ok(Json(connector.clone()))
}

/// GET /api/v1/cdc/connectors/:name/status
async fn get_connector_status(
    State(state): State<CdcApiState>,
    Path(name): Path<String>,
) -> Result<Json<CdcConnectorInfo>, (StatusCode, Json<ErrorResponse>)> {
    let connectors = state.connectors.read().await;
    connectors
        .get(&name)
        .cloned()
        .map(Json)
        .ok_or_else(|| not_found(&name))
}

/// GET /api/v1/cdc/connectors/:name/schema
async fn get_connector_schema(
    State(state): State<CdcApiState>,
    Path(name): Path<String>,
) -> Result<Json<Vec<CapturedTableSchema>>, (StatusCode, Json<ErrorResponse>)> {
    let connectors = state.connectors.read().await;
    let connector = connectors.get(&name).ok_or_else(|| not_found(&name))?;

    let schemas: Vec<CapturedTableSchema> = connector
        .tables
        .iter()
        .map(|table| CapturedTableSchema {
            table: table.clone(),
            columns: Vec::new(),
            primary_key: Vec::new(),
            topic_name: format!(
                "{}.{}.{}",
                connector.config.output_topic_prefix, connector.config.database, table
            ),
        })
        .collect();

    Ok(Json(schemas))
}

// ---------------------------------------------------------------------------
// DLQ handlers
// ---------------------------------------------------------------------------

/// GET /api/v1/cdc/dlq
async fn list_dlq_entries(State(state): State<CdcApiState>) -> Json<Vec<DlqEntry>> {
    let entries = state.dlq_entries.read().await;
    Json(entries.clone())
}

/// POST /api/v1/cdc/dlq/:id/retry
async fn retry_dlq_entry(
    State(state): State<CdcApiState>,
    Path(id): Path<String>,
) -> Result<Json<MessageResponse>, (StatusCode, Json<ErrorResponse>)> {
    let mut entries = state.dlq_entries.write().await;
    let entry = entries.iter_mut().find(|e| e.id == id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("DLQ entry '{}' not found", id),
            }),
        )
    })?;

    entry.retry_count += 1;
    let connector = entry.connector_name.clone();
    tracing::info!(dlq_id = %id, connector = %connector, "Retrying DLQ entry");

    Ok(Json(MessageResponse {
        message: format!(
            "DLQ entry '{}' queued for retry (attempt {})",
            id, entry.retry_count
        ),
    }))
}

/// POST /api/v1/cdc/dlq/:id/skip
async fn skip_dlq_entry(
    State(state): State<CdcApiState>,
    Path(id): Path<String>,
) -> Result<Json<MessageResponse>, (StatusCode, Json<ErrorResponse>)> {
    let mut entries = state.dlq_entries.write().await;
    let idx = entries.iter().position(|e| e.id == id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("DLQ entry '{}' not found", id),
            }),
        )
    })?;

    entries.remove(idx);
    tracing::info!(dlq_id = %id, "DLQ entry skipped/discarded");

    Ok(Json(MessageResponse {
        message: format!("DLQ entry '{}' skipped", id),
    }))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    fn app() -> Router {
        create_cdc_router()
    }

    fn app_with_state(state: CdcApiState) -> Router {
        create_cdc_router_with_state(state)
    }

    fn sample_request() -> CreateConnectorRequest {
        CreateConnectorRequest {
            name: "pg-orders".to_string(),
            connector_type: CdcConnectorType::PostgreSQL,
            host: "localhost".to_string(),
            port: 5432,
            database: "shop".to_string(),
            tables: vec!["orders".to_string(), "customers".to_string()],
            output_topic_prefix: "cdc".to_string(),
            slot_name: Some("streamline_slot".to_string()),
            snapshot_mode: "initial".to_string(),
            output_format: "debezium".to_string(),
        }
    }

    async fn json_body<T: serde::de::DeserializeOwned>(resp: axum::http::Response<Body>) -> T {
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    async fn seed_connector(state: &CdcApiState) {
        let req = sample_request();
        let info = CdcConnectorInfo {
            name: req.name.clone(),
            connector_type: req.connector_type,
            config: CdcConnectorConfig {
                host: req.host,
                port: req.port,
                database: req.database,
                tables: req.tables.clone(),
                output_topic_prefix: req.output_topic_prefix,
                slot_name: req.slot_name,
                snapshot_mode: req.snapshot_mode,
                output_format: req.output_format,
            },
            status: ConnectorStatus::Created,
            created_at: now_rfc3339(),
            tables: req.tables,
            stats: ConnectorStats::default(),
        };
        state
            .connectors
            .write()
            .await
            .insert(info.name.clone(), info);
    }

    // ---- Connector CRUD ----

    #[tokio::test]
    async fn test_list_connectors_empty() {
        let resp = app()
            .oneshot(
                Request::get("/api/v1/cdc/connectors")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let list: Vec<CdcConnectorInfo> = json_body(resp).await;
        assert!(list.is_empty());
    }

    #[tokio::test]
    async fn test_create_connector() {
        let req = sample_request();
        let resp = app()
            .oneshot(
                Request::post("/api/v1/cdc/connectors")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
        let info: CdcConnectorInfo = json_body(resp).await;
        assert_eq!(info.name, "pg-orders");
        assert_eq!(info.connector_type, CdcConnectorType::PostgreSQL);
        assert_eq!(info.status, ConnectorStatus::Created);
        assert_eq!(info.tables, vec!["orders", "customers"]);
    }

    #[tokio::test]
    async fn test_create_connector_duplicate() {
        let state = CdcApiState::new();
        seed_connector(&state).await;
        let req = sample_request();
        let resp = app_with_state(state)
            .oneshot(
                Request::post("/api/v1/cdc/connectors")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn test_create_connector_empty_name() {
        let mut req = sample_request();
        req.name = String::new();
        let resp = app()
            .oneshot(
                Request::post("/api/v1/cdc/connectors")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_list_connectors_after_create() {
        let state = CdcApiState::new();
        seed_connector(&state).await;
        let resp = app_with_state(state)
            .oneshot(
                Request::get("/api/v1/cdc/connectors")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let list: Vec<CdcConnectorInfo> = json_body(resp).await;
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].name, "pg-orders");
    }

    #[tokio::test]
    async fn test_get_connector() {
        let state = CdcApiState::new();
        seed_connector(&state).await;
        let resp = app_with_state(state)
            .oneshot(
                Request::get("/api/v1/cdc/connectors/pg-orders")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let info: CdcConnectorInfo = json_body(resp).await;
        assert_eq!(info.name, "pg-orders");
        assert_eq!(info.config.host, "localhost");
    }

    #[tokio::test]
    async fn test_get_connector_not_found() {
        let resp = app()
            .oneshot(
                Request::get("/api/v1/cdc/connectors/nope")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_delete_connector() {
        let state = CdcApiState::new();
        seed_connector(&state).await;
        let resp = app_with_state(state)
            .oneshot(
                Request::delete("/api/v1/cdc/connectors/pg-orders")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn test_delete_connector_not_found() {
        let resp = app()
            .oneshot(
                Request::delete("/api/v1/cdc/connectors/nope")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    // ---- Connector lifecycle ----

    #[tokio::test]
    async fn test_start_connector() {
        let state = CdcApiState::new();
        seed_connector(&state).await;
        let resp = app_with_state(state)
            .oneshot(
                Request::post("/api/v1/cdc/connectors/pg-orders/start")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let info: CdcConnectorInfo = json_body(resp).await;
        assert_eq!(info.status, ConnectorStatus::Running);
    }

    #[tokio::test]
    async fn test_start_connector_not_found() {
        let resp = app()
            .oneshot(
                Request::post("/api/v1/cdc/connectors/nope/start")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_stop_connector() {
        let state = CdcApiState::new();
        seed_connector(&state).await;
        let resp = app_with_state(state)
            .oneshot(
                Request::post("/api/v1/cdc/connectors/pg-orders/stop")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let info: CdcConnectorInfo = json_body(resp).await;
        assert_eq!(info.status, ConnectorStatus::Stopped);
    }

    #[tokio::test]
    async fn test_restart_connector() {
        let state = CdcApiState::new();
        seed_connector(&state).await;
        let resp = app_with_state(state)
            .oneshot(
                Request::post("/api/v1/cdc/connectors/pg-orders/restart")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let info: CdcConnectorInfo = json_body(resp).await;
        assert_eq!(info.status, ConnectorStatus::Snapshotting);
        assert_eq!(info.stats.snapshot_progress, Some(0.0));
    }

    // ---- Status & schema ----

    #[tokio::test]
    async fn test_get_connector_status() {
        let state = CdcApiState::new();
        seed_connector(&state).await;
        let resp = app_with_state(state)
            .oneshot(
                Request::get("/api/v1/cdc/connectors/pg-orders/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let info: CdcConnectorInfo = json_body(resp).await;
        assert_eq!(info.name, "pg-orders");
        assert_eq!(info.stats.events_captured, 0);
    }

    #[tokio::test]
    async fn test_get_connector_schema() {
        let state = CdcApiState::new();
        seed_connector(&state).await;
        let resp = app_with_state(state)
            .oneshot(
                Request::get("/api/v1/cdc/connectors/pg-orders/schema")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let schemas: Vec<CapturedTableSchema> = json_body(resp).await;
        assert_eq!(schemas.len(), 2);
        assert_eq!(schemas[0].topic_name, "cdc.shop.orders");
        assert_eq!(schemas[1].topic_name, "cdc.shop.customers");
    }

    // ---- DLQ ----

    #[tokio::test]
    async fn test_list_dlq_empty() {
        let resp = app()
            .oneshot(
                Request::get("/api/v1/cdc/dlq")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let entries: Vec<DlqEntry> = json_body(resp).await;
        assert!(entries.is_empty());
    }

    #[tokio::test]
    async fn test_retry_dlq_entry() {
        let state = CdcApiState::new();
        let dlq_id = Uuid::new_v4().to_string();
        state.dlq_entries.write().await.push(DlqEntry {
            id: dlq_id.clone(),
            connector_name: "pg-orders".to_string(),
            error: "Connection refused".to_string(),
            event_data: serde_json::json!({"table": "orders", "op": "c"}),
            failed_at: now_rfc3339(),
            retry_count: 0,
        });
        let resp = app_with_state(state)
            .oneshot(
                Request::post(format!("/api/v1/cdc/dlq/{}/retry", dlq_id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let msg: MessageResponse = json_body(resp).await;
        assert!(msg.message.contains("retry"));
    }

    #[tokio::test]
    async fn test_retry_dlq_not_found() {
        let resp = app()
            .oneshot(
                Request::post("/api/v1/cdc/dlq/nonexistent/retry")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_skip_dlq_entry() {
        let state = CdcApiState::new();
        let dlq_id = Uuid::new_v4().to_string();
        state.dlq_entries.write().await.push(DlqEntry {
            id: dlq_id.clone(),
            connector_name: "pg-orders".to_string(),
            error: "Parse error".to_string(),
            event_data: serde_json::json!({"bad": "data"}),
            failed_at: now_rfc3339(),
            retry_count: 3,
        });
        let resp = app_with_state(state.clone())
            .oneshot(
                Request::post(format!("/api/v1/cdc/dlq/{}/skip", dlq_id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let msg: MessageResponse = json_body(resp).await;
        assert!(msg.message.contains("skipped"));
        assert!(state.dlq_entries.read().await.is_empty());
    }

    #[tokio::test]
    async fn test_skip_dlq_not_found() {
        let resp = app()
            .oneshot(
                Request::post("/api/v1/cdc/dlq/nonexistent/skip")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_connector_full_lifecycle() {
        let state = CdcApiState::new();
        let req = sample_request();

        // Create
        let resp = app_with_state(state.clone())
            .oneshot(
                Request::post("/api/v1/cdc/connectors")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);

        // Start
        let resp = app_with_state(state.clone())
            .oneshot(
                Request::post("/api/v1/cdc/connectors/pg-orders/start")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let info: CdcConnectorInfo = json_body(resp).await;
        assert_eq!(info.status, ConnectorStatus::Running);

        // Stop
        let resp = app_with_state(state.clone())
            .oneshot(
                Request::post("/api/v1/cdc/connectors/pg-orders/stop")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let info: CdcConnectorInfo = json_body(resp).await;
        assert_eq!(info.status, ConnectorStatus::Stopped);

        // Delete
        let resp = app_with_state(state.clone())
            .oneshot(
                Request::delete("/api/v1/cdc/connectors/pg-orders")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);

        assert!(state.connectors.read().await.is_empty());
    }

    #[tokio::test]
    async fn test_connector_type_serialization() {
        let types = vec![
            (CdcConnectorType::PostgreSQL, "\"PostgreSQL\""),
            (CdcConnectorType::MySQL, "\"MySQL\""),
            (CdcConnectorType::MongoDB, "\"MongoDB\""),
            (CdcConnectorType::SqlServer, "\"SqlServer\""),
        ];
        for (ct, expected) in types {
            let json = serde_json::to_string(&ct).unwrap();
            assert_eq!(json, expected);
            let back: CdcConnectorType = serde_json::from_str(&json).unwrap();
            assert_eq!(back, ct);
        }
    }

    #[tokio::test]
    async fn test_connector_status_serialization() {
        let created = serde_json::to_string(&ConnectorStatus::Created).unwrap();
        assert_eq!(created, "\"Created\"");
        let failed =
            serde_json::to_string(&ConnectorStatus::Failed("timeout".to_string())).unwrap();
        assert!(failed.contains("timeout"));
        let back: ConnectorStatus = serde_json::from_str(&failed).unwrap();
        assert_eq!(back, ConnectorStatus::Failed("timeout".to_string()));
    }

    #[tokio::test]
    async fn test_dlq_list_with_multiple_entries() {
        let state = CdcApiState::new();
        {
            let mut entries = state.dlq_entries.write().await;
            entries.push(DlqEntry {
                id: Uuid::new_v4().to_string(),
                connector_name: "pg-orders".to_string(),
                error: "Connection refused".to_string(),
                event_data: serde_json::json!({"op": "c"}),
                failed_at: now_rfc3339(),
                retry_count: 0,
            });
            entries.push(DlqEntry {
                id: Uuid::new_v4().to_string(),
                connector_name: "mysql-users".to_string(),
                error: "Schema mismatch".to_string(),
                event_data: serde_json::json!({"op": "u"}),
                failed_at: now_rfc3339(),
                retry_count: 1,
            });
        }
        let resp = app_with_state(state)
            .oneshot(
                Request::get("/api/v1/cdc/dlq")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let entries: Vec<DlqEntry> = json_body(resp).await;
        assert_eq!(entries.len(), 2);
    }
}
