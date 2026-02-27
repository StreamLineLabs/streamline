//! Tiered Storage Policy Management REST API

use crate::storage::tiering_policy::{
    GlobalTieringPolicy, TieringPolicyEngine, TieringStatsSnapshot, TopicTieringPolicy,
};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{delete, get, put},
    Json, Router,
};
use std::sync::Arc;

#[derive(Clone)]
pub struct TieringApiState {
    pub engine: Arc<TieringPolicyEngine>,
}

impl TieringApiState {
    pub fn new() -> Self {
        Self {
            engine: Arc::new(TieringPolicyEngine::new(GlobalTieringPolicy::default())),
        }
    }
}

impl Default for TieringApiState {
    fn default() -> Self {
        Self::new()
    }
}

pub fn create_tiering_api_router(state: TieringApiState) -> Router {
    Router::new()
        .route(
            "/api/v1/tiering/policies",
            get(list_policies).post(set_policy),
        )
        .route(
            "/api/v1/tiering/policies/:topic",
            get(get_policy).delete(remove_policy),
        )
        .route(
            "/api/v1/tiering/global",
            get(get_global_policy).put(update_global_policy),
        )
        .route("/api/v1/tiering/stats", get(get_stats))
        .with_state(state)
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

async fn list_policies(State(state): State<TieringApiState>) -> Json<Vec<TopicTieringPolicy>> {
    Json(state.engine.list_policies())
}

async fn get_policy(
    State(state): State<TieringApiState>,
    Path(topic): Path<String>,
) -> Result<Json<TopicTieringPolicy>, StatusCode> {
    state
        .engine
        .get_topic_policy(&topic)
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn set_policy(
    State(state): State<TieringApiState>,
    Json(policy): Json<TopicTieringPolicy>,
) -> Result<StatusCode, (StatusCode, String)> {
    state
        .engine
        .set_topic_policy(policy)
        .map(|_| StatusCode::CREATED)
        .map_err(|e: crate::error::StreamlineError| (StatusCode::BAD_REQUEST, e.to_string()))
}

async fn remove_policy(
    State(state): State<TieringApiState>,
    Path(topic): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    state
        .engine
        .remove_topic_policy(&topic)
        .map(|_| StatusCode::NO_CONTENT)
        .map_err(|e: crate::error::StreamlineError| (StatusCode::NOT_FOUND, e.to_string()))
}

async fn get_global_policy(State(state): State<TieringApiState>) -> Json<GlobalTieringPolicy> {
    Json(state.engine.get_global_policy())
}

async fn update_global_policy(
    State(state): State<TieringApiState>,
    Json(policy): Json<GlobalTieringPolicy>,
) -> StatusCode {
    state.engine.update_global_policy(policy);
    StatusCode::OK
}

async fn get_stats(State(state): State<TieringApiState>) -> Json<TieringStatsSnapshot> {
    Json(state.engine.stats())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    fn test_app() -> Router {
        create_tiering_api_router(TieringApiState::new())
    }

    #[tokio::test]
    async fn test_list_policies_empty() {
        let app = test_app();
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/tiering/policies")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_global_policy() {
        let app = test_app();
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/tiering/global")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_stats() {
        let app = test_app();
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/tiering/stats")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_policy_not_found() {
        let app = test_app();
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/tiering/policies/nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
}
