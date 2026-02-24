//! Fleet Management API — REST endpoints for edge device lifecycle
//!
//! ## Endpoints
//!
//! - `POST /api/v1/fleet/register` — Register a new edge device
//! - `POST /api/v1/fleet/heartbeat` — Process edge heartbeat
//! - `GET /api/v1/fleet/devices` — List all edge devices
//! - `GET /api/v1/fleet/devices/:id` — Get device details
//! - `GET /api/v1/fleet/summary` — Fleet overview for dashboard
//! - `POST /api/v1/fleet/devices/:id/command` — Send command to device

#[cfg(feature = "edge")]
use crate::edge::fleet::{
    EdgeHeartbeat, EdgeRegistration, FleetCommand, FleetManager, HeartbeatResponse,
    RegistrationResponse,
};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Shared state for Fleet API
#[derive(Clone)]
pub struct FleetApiState {
    #[cfg(feature = "edge")]
    pub manager: Arc<FleetManager>,
}

/// Error response
#[derive(Serialize)]
struct FleetError {
    error: String,
}

/// Command request
#[derive(Deserialize)]
struct CommandRequest {
    command: String,
    #[serde(default)]
    payload: Option<String>,
}

/// Create fleet API router
pub fn create_fleet_api_router(state: FleetApiState) -> Router {
    Router::new()
        .route("/api/v1/fleet/register", post(register_device))
        .route("/api/v1/fleet/heartbeat", post(heartbeat))
        .route("/api/v1/fleet/devices", get(list_devices))
        .route("/api/v1/fleet/devices/:id", get(get_device))
        .route("/api/v1/fleet/summary", get(fleet_summary))
        .route("/api/v1/fleet/devices/:id/command", post(send_command))
        .with_state(state)
}

#[cfg(feature = "edge")]
async fn register_device(
    State(state): State<FleetApiState>,
    Json(reg): Json<EdgeRegistration>,
) -> Result<Json<RegistrationResponse>, (StatusCode, Json<FleetError>)> {
    let response = state.manager.register(reg).await;
    Ok(Json(response))
}

#[cfg(not(feature = "edge"))]
async fn register_device() -> (StatusCode, Json<FleetError>) {
    (StatusCode::NOT_IMPLEMENTED, Json(FleetError { error: "Edge feature not enabled".into() }))
}

#[cfg(feature = "edge")]
async fn heartbeat(
    State(state): State<FleetApiState>,
    Json(hb): Json<EdgeHeartbeat>,
) -> Result<Json<HeartbeatResponse>, (StatusCode, Json<FleetError>)> {
    match state.manager.heartbeat(hb).await {
        Some(resp) => Ok(Json(resp)),
        None => Err((StatusCode::NOT_FOUND, Json(FleetError { error: "Device not registered".into() }))),
    }
}

#[cfg(not(feature = "edge"))]
async fn heartbeat() -> (StatusCode, Json<FleetError>) {
    (StatusCode::NOT_IMPLEMENTED, Json(FleetError { error: "Edge feature not enabled".into() }))
}

#[cfg(feature = "edge")]
async fn list_devices(
    State(state): State<FleetApiState>,
) -> Json<serde_json::Value> {
    let summary = state.manager.summary().await;
    Json(serde_json::json!({
        "devices": summary.devices,
        "total": summary.total_devices,
    }))
}

#[cfg(not(feature = "edge"))]
async fn list_devices() -> Json<serde_json::Value> {
    Json(serde_json::json!({"devices": [], "total": 0, "note": "Edge feature not enabled"}))
}

#[cfg(feature = "edge")]
async fn get_device(
    State(state): State<FleetApiState>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<FleetError>)> {
    match state.manager.get_device(&id).await {
        Some(device) => Ok(Json(serde_json::to_value(device).unwrap_or_default())),
        None => Err((StatusCode::NOT_FOUND, Json(FleetError { error: format!("Device '{}' not found", id) }))),
    }
}

#[cfg(not(feature = "edge"))]
async fn get_device(Path(_id): Path<String>) -> (StatusCode, Json<FleetError>) {
    (StatusCode::NOT_IMPLEMENTED, Json(FleetError { error: "Edge feature not enabled".into() }))
}

#[cfg(feature = "edge")]
async fn fleet_summary(
    State(state): State<FleetApiState>,
) -> Json<serde_json::Value> {
    let summary = state.manager.summary().await;
    Json(serde_json::to_value(summary).unwrap_or_default())
}

#[cfg(not(feature = "edge"))]
async fn fleet_summary() -> Json<serde_json::Value> {
    Json(serde_json::json!({"total_devices": 0, "note": "Edge feature not enabled"}))
}

#[cfg(feature = "edge")]
async fn send_command(
    State(state): State<FleetApiState>,
    Path(id): Path<String>,
    Json(req): Json<CommandRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<FleetError>)> {
    let command = match req.command.as_str() {
        "restart" => FleetCommand::Restart,
        "diagnostics" => FleetCommand::RunDiagnostics,
        "drain" => FleetCommand::DrainAndShutdown,
        "update_config" => FleetCommand::UpdateConfig {
            config_yaml: req.payload.unwrap_or_default(),
        },
        other => return Err((StatusCode::BAD_REQUEST, Json(FleetError {
            error: format!("Unknown command: '{}'. Use: restart, diagnostics, drain, update_config", other),
        }))),
    };

    if state.manager.send_command(&id, command).await {
        Ok(Json(serde_json::json!({"status": "queued", "device": id})))
    } else {
        Err((StatusCode::NOT_FOUND, Json(FleetError { error: format!("Device '{}' not found", id) })))
    }
}

#[cfg(not(feature = "edge"))]
async fn send_command(Path(_id): Path<String>, Json(_req): Json<CommandRequest>) -> (StatusCode, Json<FleetError>) {
    (StatusCode::NOT_IMPLEMENTED, Json(FleetError { error: "Edge feature not enabled".into() }))
}
