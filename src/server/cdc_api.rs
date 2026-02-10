//! CDC (Change Data Capture) API - REST endpoints for managing CDC sources
//!
//! ## Endpoints
//!
//! - `GET /api/v1/cdc/sources` - List all CDC sources
//! - `POST /api/v1/cdc/sources` - Create a new CDC source
//! - `GET /api/v1/cdc/sources/:id` - Get source details
//! - `DELETE /api/v1/cdc/sources/:id` - Delete a CDC source
//! - `POST /api/v1/cdc/sources/:id/start` - Start capturing changes
//! - `POST /api/v1/cdc/sources/:id/stop` - Stop capturing changes
//! - `POST /api/v1/cdc/sources/:id/snapshot` - Trigger initial snapshot
//! - `GET /api/v1/cdc/sources/:id/status` - Get source status and lag
//! - `GET /api/v1/cdc/sources/:id/schema` - Get captured table schemas
//! - `GET /api/v1/cdc/stats` - Get aggregate CDC statistics
//!
//! ### Connector management (Debezium-compatible)
//!
//! - `POST /api/v1/cdc/connectors` - Create CDC connector
//! - `GET /api/v1/cdc/connectors` - List CDC connectors
//! - `GET /api/v1/cdc/connectors/:name/status` - Get connector status
//! - `POST /api/v1/cdc/connectors/:name/snapshot` - Trigger snapshot
//! - `GET /api/v1/cdc/connectors/:name/schema-history` - Get schema changes

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// CDC source registration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcSourceRegistration {
    /// Unique source identifier
    pub id: String,
    /// Human-readable name
    pub name: String,
    /// Database type (postgres, mysql, mongodb, sqlserver)
    pub database_type: String,
    /// Connection string or host:port
    pub connection: String,
    /// Database name
    pub database: String,
    /// Tables to capture (empty = all)
    #[serde(default)]
    pub tables: Vec<String>,
    /// Schema to capture (default: public)
    #[serde(default = "default_schema")]
    pub schema_name: String,
    /// Topic prefix for generated topics
    #[serde(default = "default_topic_prefix")]
    pub topic_prefix: String,
    /// Snapshot mode (initial, never, when_needed)
    #[serde(default = "default_snapshot_mode")]
    pub snapshot_mode: String,
    /// Additional configuration
    #[serde(default)]
    pub config: HashMap<String, String>,
}

fn default_schema() -> String {
    "public".to_string()
}
fn default_topic_prefix() -> String {
    "cdc".to_string()
}
fn default_snapshot_mode() -> String {
    "initial".to_string()
}

/// CDC source status
#[derive(Debug, Clone, Serialize)]
pub struct CdcSourceStatus {
    pub id: String,
    pub name: String,
    pub database_type: String,
    pub state: String,
    pub tables_captured: usize,
    pub events_captured: u64,
    pub last_event_at: Option<String>,
    pub replication_lag_ms: Option<f64>,
    pub errors: u64,
    pub current_offset: Option<String>,
}

/// Captured table schema info
#[derive(Debug, Clone, Serialize)]
pub struct CapturedSchema {
    pub table: String,
    pub schema_name: String,
    pub columns: Vec<CapturedColumn>,
    pub primary_key: Vec<String>,
    pub topic_name: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct CapturedColumn {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

/// Aggregate CDC stats
#[derive(Debug, Clone, Serialize)]
pub struct CdcAggregateStats {
    pub total_sources: usize,
    pub active_sources: usize,
    pub total_events_captured: u64,
    pub total_tables_captured: usize,
    pub sources: Vec<CdcSourceStatus>,
}

/// Internal state for a managed CDC source
#[derive(Debug, Clone)]
struct ManagedSource {
    registration: CdcSourceRegistration,
    state: String,
    events_captured: u64,
    tables_captured: usize,
    errors: u64,
    #[allow(dead_code)]
    schemas: Vec<CapturedSchema>,
}

/// Shared state for CDC API
#[derive(Clone)]
pub struct CdcApiState {
    sources: Arc<RwLock<HashMap<String, ManagedSource>>>,
}

impl CdcApiState {
    pub fn new() -> Self {
        Self {
            sources: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl Default for CdcApiState {
    fn default() -> Self {
        Self::new()
    }
}

/// Error response
#[derive(Debug, Serialize)]
struct CdcErrorResponse {
    error: String,
}

/// Create the CDC API router
pub fn create_cdc_api_router(state: CdcApiState) -> Router {
    Router::new()
        .route("/api/v1/cdc/sources", get(list_sources).post(create_source))
        .route("/api/v1/cdc/stats", get(get_aggregate_stats))
        .route(
            "/api/v1/cdc/sources/{id}",
            get(get_source).delete(delete_source),
        )
        .route("/api/v1/cdc/sources/{id}/start", post(start_source))
        .route("/api/v1/cdc/sources/{id}/stop", post(stop_source))
        .route("/api/v1/cdc/sources/{id}/snapshot", post(trigger_snapshot))
        .route("/api/v1/cdc/sources/{id}/status", get(get_source_status))
        .route("/api/v1/cdc/sources/{id}/schema", get(get_source_schema))
        // Connector management endpoints (Debezium-compatible naming)
        .route(
            "/api/v1/cdc/connectors",
            get(list_connectors).post(create_connector),
        )
        .route(
            "/api/v1/cdc/connectors/{name}/status",
            get(get_connector_status),
        )
        .route(
            "/api/v1/cdc/connectors/{name}",
            axum::routing::delete(delete_connector),
        )
        .route(
            "/api/v1/cdc/connectors/{name}/snapshot",
            post(trigger_connector_snapshot),
        )
        .route(
            "/api/v1/cdc/connectors/{name}/schema-history",
            get(get_connector_schema_history),
        )
        .with_state(state)
}

/// GET /api/v1/cdc/sources - List all CDC sources
async fn list_sources(State(state): State<CdcApiState>) -> Json<Vec<CdcSourceStatus>> {
    let sources = state.sources.read().await;
    let statuses: Vec<CdcSourceStatus> = sources
        .values()
        .map(|s| CdcSourceStatus {
            id: s.registration.id.clone(),
            name: s.registration.name.clone(),
            database_type: s.registration.database_type.clone(),
            state: s.state.clone(),
            tables_captured: s.tables_captured,
            events_captured: s.events_captured,
            last_event_at: None,
            replication_lag_ms: None,
            errors: s.errors,
            current_offset: None,
        })
        .collect();
    Json(statuses)
}

/// POST /api/v1/cdc/sources - Create a new CDC source
async fn create_source(
    State(state): State<CdcApiState>,
    Json(reg): Json<CdcSourceRegistration>,
) -> Result<(StatusCode, Json<CdcSourceStatus>), (StatusCode, Json<CdcErrorResponse>)> {
    // Validate database type
    let valid_types = ["postgres", "mysql", "mongodb", "sqlserver"];
    if !valid_types.contains(&reg.database_type.as_str()) {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(CdcErrorResponse {
                error: format!(
                    "Unsupported database type '{}'. Supported: {:?}",
                    reg.database_type, valid_types
                ),
            }),
        ));
    }

    let id = reg.id.clone();
    let mut sources = state.sources.write().await;
    if sources.contains_key(&id) {
        return Err((
            StatusCode::CONFLICT,
            Json(CdcErrorResponse {
                error: format!("CDC source '{}' already exists", id),
            }),
        ));
    }

    let tables_count = reg.tables.len();
    let managed = ManagedSource {
        registration: reg.clone(),
        state: "created".to_string(),
        events_captured: 0,
        tables_captured: tables_count,
        errors: 0,
        schemas: Vec::new(),
    };

    sources.insert(id.clone(), managed);

    Ok((
        StatusCode::CREATED,
        Json(CdcSourceStatus {
            id: id.clone(),
            name: reg.name,
            database_type: reg.database_type,
            state: "created".to_string(),
            tables_captured: tables_count,
            events_captured: 0,
            last_event_at: None,
            replication_lag_ms: None,
            errors: 0,
            current_offset: None,
        }),
    ))
}

/// GET /api/v1/cdc/sources/:id - Get source details
async fn get_source(
    State(state): State<CdcApiState>,
    Path(id): Path<String>,
) -> Result<Json<CdcSourceRegistration>, (StatusCode, Json<CdcErrorResponse>)> {
    let sources = state.sources.read().await;
    sources
        .get(&id)
        .map(|s| Json(s.registration.clone()))
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(CdcErrorResponse {
                    error: format!("CDC source '{}' not found", id),
                }),
            )
        })
}

/// DELETE /api/v1/cdc/sources/:id - Delete a CDC source
async fn delete_source(
    State(state): State<CdcApiState>,
    Path(id): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<CdcErrorResponse>)> {
    let mut sources = state.sources.write().await;
    if sources.remove(&id).is_some() {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err((
            StatusCode::NOT_FOUND,
            Json(CdcErrorResponse {
                error: format!("CDC source '{}' not found", id),
            }),
        ))
    }
}

/// POST /api/v1/cdc/sources/:id/start - Start capturing changes
async fn start_source(
    State(state): State<CdcApiState>,
    Path(id): Path<String>,
) -> Result<Json<CdcSourceStatus>, (StatusCode, Json<CdcErrorResponse>)> {
    let mut sources = state.sources.write().await;
    let source = sources.get_mut(&id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(CdcErrorResponse {
                error: format!("CDC source '{}' not found", id),
            }),
        )
    })?;

    source.state = "running".to_string();

    Ok(Json(CdcSourceStatus {
        id: source.registration.id.clone(),
        name: source.registration.name.clone(),
        database_type: source.registration.database_type.clone(),
        state: source.state.clone(),
        tables_captured: source.tables_captured,
        events_captured: source.events_captured,
        last_event_at: None,
        replication_lag_ms: None,
        errors: source.errors,
        current_offset: None,
    }))
}

/// POST /api/v1/cdc/sources/:id/stop - Stop capturing changes
async fn stop_source(
    State(state): State<CdcApiState>,
    Path(id): Path<String>,
) -> Result<Json<CdcSourceStatus>, (StatusCode, Json<CdcErrorResponse>)> {
    let mut sources = state.sources.write().await;
    let source = sources.get_mut(&id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(CdcErrorResponse {
                error: format!("CDC source '{}' not found", id),
            }),
        )
    })?;

    source.state = "stopped".to_string();

    Ok(Json(CdcSourceStatus {
        id: source.registration.id.clone(),
        name: source.registration.name.clone(),
        database_type: source.registration.database_type.clone(),
        state: source.state.clone(),
        tables_captured: source.tables_captured,
        events_captured: source.events_captured,
        last_event_at: None,
        replication_lag_ms: None,
        errors: source.errors,
        current_offset: None,
    }))
}

/// POST /api/v1/cdc/sources/:id/snapshot - Trigger initial snapshot
async fn trigger_snapshot(
    State(state): State<CdcApiState>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<CdcErrorResponse>)> {
    let mut sources = state.sources.write().await;
    let source = sources.get_mut(&id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(CdcErrorResponse {
                error: format!("CDC source '{}' not found", id),
            }),
        )
    })?;

    source.state = "snapshotting".to_string();

    Ok(Json(serde_json::json!({
        "id": source.registration.id,
        "state": "snapshotting",
        "message": "Initial snapshot started"
    })))
}

/// GET /api/v1/cdc/sources/:id/status - Get source status and lag
async fn get_source_status(
    State(state): State<CdcApiState>,
    Path(id): Path<String>,
) -> Result<Json<CdcSourceStatus>, (StatusCode, Json<CdcErrorResponse>)> {
    let sources = state.sources.read().await;
    let source = sources.get(&id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(CdcErrorResponse {
                error: format!("CDC source '{}' not found", id),
            }),
        )
    })?;

    Ok(Json(CdcSourceStatus {
        id: source.registration.id.clone(),
        name: source.registration.name.clone(),
        database_type: source.registration.database_type.clone(),
        state: source.state.clone(),
        tables_captured: source.tables_captured,
        events_captured: source.events_captured,
        last_event_at: None,
        replication_lag_ms: if source.state == "running" {
            Some(0.0)
        } else {
            None
        },
        errors: source.errors,
        current_offset: None,
    }))
}

/// GET /api/v1/cdc/sources/:id/schema - Get captured table schemas
async fn get_source_schema(
    State(state): State<CdcApiState>,
    Path(id): Path<String>,
) -> Result<Json<Vec<CapturedSchema>>, (StatusCode, Json<CdcErrorResponse>)> {
    let sources = state.sources.read().await;
    let source = sources.get(&id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(CdcErrorResponse {
                error: format!("CDC source '{}' not found", id),
            }),
        )
    })?;

    // Generate schemas from registration info
    let schemas: Vec<CapturedSchema> = source
        .registration
        .tables
        .iter()
        .map(|table| CapturedSchema {
            table: table.clone(),
            schema_name: source.registration.schema_name.clone(),
            columns: Vec::new(), // Populated after connect
            primary_key: Vec::new(),
            topic_name: format!(
                "{}.{}.{}.{}",
                source.registration.topic_prefix,
                source.registration.database,
                source.registration.schema_name,
                table
            ),
        })
        .collect();

    Ok(Json(schemas))
}

/// GET /api/v1/cdc/stats - Get aggregate CDC statistics
async fn get_aggregate_stats(State(state): State<CdcApiState>) -> Json<CdcAggregateStats> {
    let sources = state.sources.read().await;
    let active = sources.values().filter(|s| s.state == "running").count();
    let total_events: u64 = sources.values().map(|s| s.events_captured).sum();
    let total_tables: usize = sources.values().map(|s| s.tables_captured).sum();

    let statuses: Vec<CdcSourceStatus> = sources
        .values()
        .map(|s| CdcSourceStatus {
            id: s.registration.id.clone(),
            name: s.registration.name.clone(),
            database_type: s.registration.database_type.clone(),
            state: s.state.clone(),
            tables_captured: s.tables_captured,
            events_captured: s.events_captured,
            last_event_at: None,
            replication_lag_ms: None,
            errors: s.errors,
            current_offset: None,
        })
        .collect();

    Json(CdcAggregateStats {
        total_sources: sources.len(),
        active_sources: active,
        total_events_captured: total_events,
        total_tables_captured: total_tables,
        sources: statuses,
    })
}

// ---------------------------------------------------------------------------
// Connector management endpoints (Debezium-compatible naming)
// ---------------------------------------------------------------------------

/// Request body for creating a connector
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateConnectorRequest {
    /// Connector name (unique)
    pub name: String,
    /// Database type (postgres, mysql, mongodb, sqlserver)
    pub database_type: String,
    /// Connection string
    pub connection: String,
    /// Database name
    pub database: String,
    /// Tables to capture
    #[serde(default)]
    pub tables: Vec<String>,
    /// Snapshot strategy (initial, when_needed, never, schema_only)
    #[serde(default = "default_snapshot_mode")]
    pub snapshot_mode: String,
    /// Heartbeat interval in milliseconds (0 = disabled)
    #[serde(default)]
    pub heartbeat_interval_ms: u64,
    /// Topic prefix
    #[serde(default = "default_topic_prefix")]
    pub topic_prefix: String,
    /// Additional config
    #[serde(default)]
    pub config: HashMap<String, String>,
}

/// Connector summary returned from list / create
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorInfo {
    pub name: String,
    pub database_type: String,
    pub state: String,
    pub tables: Vec<String>,
    pub snapshot_mode: String,
    pub events_captured: u64,
    pub errors: u64,
}

/// Schema history entry returned from the schema-history endpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaHistoryEntry {
    pub id: u64,
    pub ddl_type: String,
    pub table: String,
    pub ddl: String,
    pub position: String,
    pub recorded_at: String,
}

/// Query parameters for schema-history endpoint
#[derive(Debug, Deserialize)]
pub struct SchemaHistoryQuery {
    /// Filter to a specific table
    pub table: Option<String>,
}

/// POST /api/v1/cdc/connectors — Create a new CDC connector
async fn create_connector(
    State(state): State<CdcApiState>,
    Json(req): Json<CreateConnectorRequest>,
) -> Result<(StatusCode, Json<ConnectorInfo>), (StatusCode, Json<CdcErrorResponse>)> {
    let valid_types = ["postgres", "mysql", "mongodb", "sqlserver"];
    if !valid_types.contains(&req.database_type.as_str()) {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(CdcErrorResponse {
                error: format!(
                    "Unsupported database type '{}'. Supported: {:?}",
                    req.database_type, valid_types
                ),
            }),
        ));
    }

    let mut sources = state.sources.write().await;
    if sources.contains_key(&req.name) {
        return Err((
            StatusCode::CONFLICT,
            Json(CdcErrorResponse {
                error: format!("Connector '{}' already exists", req.name),
            }),
        ));
    }

    let info = ConnectorInfo {
        name: req.name.clone(),
        database_type: req.database_type.clone(),
        state: "created".to_string(),
        tables: req.tables.clone(),
        snapshot_mode: req.snapshot_mode.clone(),
        events_captured: 0,
        errors: 0,
    };

    // Store as a ManagedSource so the existing source endpoints still work
    let managed = ManagedSource {
        registration: CdcSourceRegistration {
            id: req.name.clone(),
            name: req.name.clone(),
            database_type: req.database_type.clone(),
            connection: req.connection.clone(),
            database: req.database.clone(),
            tables: req.tables.clone(),
            schema_name: default_schema(),
            topic_prefix: req.topic_prefix.clone(),
            snapshot_mode: req.snapshot_mode.clone(),
            config: req.config.clone(),
        },
        state: "created".to_string(),
        events_captured: 0,
        tables_captured: req.tables.len(),
        errors: 0,
        schemas: Vec::new(),
    };
    sources.insert(req.name.clone(), managed);

    Ok((StatusCode::CREATED, Json(info)))
}

/// GET /api/v1/cdc/connectors — List all connectors
async fn list_connectors(State(state): State<CdcApiState>) -> Json<Vec<ConnectorInfo>> {
    let sources = state.sources.read().await;
    let list: Vec<ConnectorInfo> = sources
        .values()
        .map(|s| ConnectorInfo {
            name: s.registration.name.clone(),
            database_type: s.registration.database_type.clone(),
            state: s.state.clone(),
            tables: s.registration.tables.clone(),
            snapshot_mode: s.registration.snapshot_mode.clone(),
            events_captured: s.events_captured,
            errors: s.errors,
        })
        .collect();
    Json(list)
}

/// GET /api/v1/cdc/connectors/:name/status — Get connector status
async fn get_connector_status(
    State(state): State<CdcApiState>,
    Path(name): Path<String>,
) -> Result<Json<ConnectorInfo>, (StatusCode, Json<CdcErrorResponse>)> {
    let sources = state.sources.read().await;
    let s = sources.get(&name).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(CdcErrorResponse {
                error: format!("Connector '{}' not found", name),
            }),
        )
    })?;

    Ok(Json(ConnectorInfo {
        name: s.registration.name.clone(),
        database_type: s.registration.database_type.clone(),
        state: s.state.clone(),
        tables: s.registration.tables.clone(),
        snapshot_mode: s.registration.snapshot_mode.clone(),
        events_captured: s.events_captured,
        errors: s.errors,
    }))
}

/// DELETE /api/v1/cdc/connectors/:name — Delete a CDC connector
async fn delete_connector(
    State(state): State<CdcApiState>,
    Path(name): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<CdcErrorResponse>)> {
    let mut sources = state.sources.write().await;
    if sources.remove(&name).is_some() {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err((
            StatusCode::NOT_FOUND,
            Json(CdcErrorResponse {
                error: format!("Connector '{}' not found", name),
            }),
        ))
    }
}

/// POST /api/v1/cdc/connectors/:name/snapshot — Trigger snapshot
async fn trigger_connector_snapshot(
    State(state): State<CdcApiState>,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<CdcErrorResponse>)> {
    let mut sources = state.sources.write().await;
    let source = sources.get_mut(&name).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(CdcErrorResponse {
                error: format!("Connector '{}' not found", name),
            }),
        )
    })?;

    source.state = "snapshotting".to_string();

    Ok(Json(serde_json::json!({
        "connector": name,
        "state": "snapshotting",
        "message": "Snapshot triggered"
    })))
}

/// GET /api/v1/cdc/connectors/:name/schema-history — Schema change history
async fn get_connector_schema_history(
    State(state): State<CdcApiState>,
    Path(name): Path<String>,
    Query(query): Query<SchemaHistoryQuery>,
) -> Result<Json<Vec<SchemaHistoryEntry>>, (StatusCode, Json<CdcErrorResponse>)> {
    let sources = state.sources.read().await;
    if !sources.contains_key(&name) {
        return Err((
            StatusCode::NOT_FOUND,
            Json(CdcErrorResponse {
                error: format!("Connector '{}' not found", name),
            }),
        ));
    }

    // In a full implementation this would read from SchemaHistoryStore.
    // For now return an empty list (or filtered placeholder).
    let _ = query.table;
    Ok(Json(Vec::new()))
}
