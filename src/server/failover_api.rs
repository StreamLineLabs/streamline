//! REST API for Multi-Region Failover management
//!
//! ## Endpoints
//!
//! - `GET /api/v1/failover/regions` - List regions with health status
//! - `POST /api/v1/failover/regions` - Register a new region
//! - `GET /api/v1/failover/events` - Get failover event history
//! - `POST /api/v1/failover/trigger` - Manually trigger failover
//! - `GET /api/v1/failover/split-brain` - Check for split-brain
//! - `GET /api/v1/failover/routing` - Get consumer routing table

use crate::replication::failover::{
    FailoverEvent, FailoverOrchestrator, RegionFailoverState, RegionRole,
};
use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct FailoverApiState {
    pub orchestrator: Arc<FailoverOrchestrator>,
}

#[derive(Debug, Serialize)]
pub struct RegionsResponse {
    pub regions: Vec<RegionFailoverState>,
    pub total: usize,
}

#[derive(Debug, Deserialize)]
pub struct RegisterRegionRequest {
    pub region_id: String,
    pub endpoint: String,
}

#[derive(Debug, Deserialize)]
pub struct TriggerFailoverRequest {
    pub from_region: String,
    pub reason: String,
}

#[derive(Debug, Serialize)]
pub struct SplitBrainResponse {
    pub detected: bool,
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

pub(crate) fn create_failover_api_router(state: FailoverApiState) -> Router {
    Router::new()
        .route(
            "/api/v1/failover/regions",
            get(list_regions).post(register_region),
        )
        .route("/api/v1/failover/events", get(get_events))
        .route("/api/v1/failover/trigger", post(trigger_failover))
        .route("/api/v1/failover/split-brain", get(check_split_brain))
        .route("/api/v1/failover/routing", get(get_routing))
        .with_state(state)
}

async fn list_regions(State(state): State<FailoverApiState>) -> Json<RegionsResponse> {
    let regions = state.orchestrator.region_states().await;
    let total = regions.len();
    Json(RegionsResponse { regions, total })
}

async fn register_region(
    State(state): State<FailoverApiState>,
    Json(request): Json<RegisterRegionRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, Json<ErrorResponse>)> {
    state
        .orchestrator
        .register_region(&request.region_id, &request.endpoint, RegionRole::Standby)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: e.to_string(),
                }),
            )
        })?;
    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "region_id": request.region_id,
            "message": "Region registered"
        })),
    ))
}

async fn get_events(State(state): State<FailoverApiState>) -> Json<Vec<FailoverEvent>> {
    Json(state.orchestrator.failover_history().await)
}

async fn trigger_failover(
    State(state): State<FailoverApiState>,
    Json(request): Json<TriggerFailoverRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    // Record failures on the source region to trigger automatic failover
    let event = state
        .orchestrator
        .record_failure(&request.from_region)
        .await
        .map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: e.to_string(),
                }),
            )
        })?;

    match event {
        Some(e) => Ok(Json(serde_json::json!({
            "failover_triggered": true,
            "from_region": e.from_region,
            "to_region": e.to_region,
            "fencing_epoch": e.fencing_epoch,
        }))),
        None => Ok(Json(serde_json::json!({
            "failover_triggered": false,
            "message": "Failure recorded but threshold not reached"
        }))),
    }
}

async fn check_split_brain(State(state): State<FailoverApiState>) -> Json<SplitBrainResponse> {
    let detected = state.orchestrator.detect_split_brain().await;
    Json(SplitBrainResponse { detected })
}

async fn get_routing(State(state): State<FailoverApiState>) -> Json<serde_json::Value> {
    let local = state.orchestrator.local_region().to_string();
    let regions = state.orchestrator.region_states().await;
    Json(serde_json::json!({
        "local_region": local,
        "regions": regions.iter().map(|r| serde_json::json!({
            "region_id": r.region_id,
            "role": format!("{}", r.role),
            "health": format!("{}", r.health),
            "endpoint": r.endpoint,
        })).collect::<Vec<_>>(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_failover_api_state_is_clone() {
        fn assert_clone<T: Clone>() {}
        assert_clone::<FailoverApiState>();
    }
}
