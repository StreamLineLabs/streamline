//! REST API for Resource Governor management
//!
//! ## Endpoints
//!
//! - `GET /api/v1/governor/status` - Get governor status with actions history
//! - `GET /api/v1/governor/snapshot` - Get current resource snapshot
//! - `GET /api/v1/governor/recommendations` - Get auto-partition recommendations
//! - `POST /api/v1/governor/evaluate` - Trigger evaluation with current metrics

use crate::autotuning::governor::{
    GovernorAction, PartitionRecommendation, ResourceGovernor, ResourceSnapshot,
};
use axum::{
    extract::State,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct GovernorApiState {
    pub governor: Arc<ResourceGovernor>,
}

#[derive(Debug, Serialize)]
pub struct GovernorStatusResponse {
    pub active: bool,
    pub buffer_pool_bytes: u64,
    pub recent_actions: Vec<GovernorAction>,
}

#[derive(Debug, Serialize)]
pub struct RecommendationsResponse {
    pub recommendations: Vec<PartitionRecommendation>,
}

#[derive(Debug, Deserialize)]
pub struct EvaluateRequest {
    pub memory_pct: f64,
    pub disk_pct: f64,
}

#[derive(Debug, Serialize)]
pub struct EvaluateResponse {
    pub actions: Vec<GovernorAction>,
    pub snapshot: ResourceSnapshot,
}

pub(crate) fn create_governor_api_router(state: GovernorApiState) -> Router {
    Router::new()
        .route("/api/v1/governor/status", get(get_status))
        .route("/api/v1/governor/snapshot", post(get_snapshot))
        .route("/api/v1/governor/recommendations", get(get_recommendations))
        .route("/api/v1/governor/evaluate", post(evaluate))
        .with_state(state)
}

async fn get_status(State(state): State<GovernorApiState>) -> Json<GovernorStatusResponse> {
    Json(GovernorStatusResponse {
        active: state.governor.is_active(),
        buffer_pool_bytes: state.governor.buffer_pool_bytes(),
        recent_actions: state.governor.actions_history(),
    })
}

async fn get_snapshot(
    State(state): State<GovernorApiState>,
    Json(req): Json<EvaluateRequest>,
) -> Json<ResourceSnapshot> {
    Json(state.governor.snapshot(req.memory_pct, req.disk_pct))
}

async fn get_recommendations(
    State(state): State<GovernorApiState>,
) -> Json<RecommendationsResponse> {
    let recommendations = state.governor.partition_recommendations();
    Json(RecommendationsResponse { recommendations })
}

async fn evaluate(
    State(state): State<GovernorApiState>,
    Json(req): Json<EvaluateRequest>,
) -> Json<EvaluateResponse> {
    let actions = state.governor.evaluate(req.memory_pct, req.disk_pct);
    let snapshot = state.governor.snapshot(req.memory_pct, req.disk_pct);
    Json(EvaluateResponse { actions, snapshot })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_governor_api_state_is_clone() {
        fn assert_clone<T: Clone>() {}
        assert_clone::<GovernorApiState>();
    }
}
