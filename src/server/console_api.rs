//! REST API for Cloud Console management
//!
//! ## Endpoints
//!
//! - `GET /api/v1/console/api-keys/:tenant` - List API keys for tenant
//! - `POST /api/v1/console/api-keys` - Create API key
//! - `DELETE /api/v1/console/api-keys/:id` - Revoke API key
//! - `POST /api/v1/console/onboard` - Onboard a new tenant
//! - `POST /api/v1/console/usage` - Get usage dashboard for tenant

use crate::cloud::console::{
    ApiKey, ApiScope, ConsoleManager, OnboardingRequest, OnboardingResponse, UsageDashboard,
};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct ConsoleApiState {
    pub console: Arc<ConsoleManager>,
}

#[derive(Debug, Serialize)]
pub struct ApiKeysResponse {
    pub keys: Vec<ApiKey>,
    pub total: usize,
}

#[derive(Debug, Deserialize)]
pub struct CreateApiKeyRequest {
    pub tenant_id: String,
    pub name: String,
    pub scopes: Vec<ApiScope>,
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize)]
pub struct UsageRequest {
    pub tenant_id: String,
    pub period_start: DateTime<Utc>,
    pub period_end: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
pub struct DeleteResponse {
    pub message: String,
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

pub(crate) fn create_console_api_router(state: ConsoleApiState) -> Router {
    Router::new()
        .route("/api/v1/console/api-keys/:tenant", get(list_api_keys))
        .route("/api/v1/console/api-keys", post(create_api_key))
        .route("/api/v1/console/api-keys/:id/revoke", post(revoke_api_key))
        .route("/api/v1/console/onboard", post(onboard_tenant))
        .route("/api/v1/console/usage", post(get_usage))
        .with_state(state)
}

async fn list_api_keys(
    State(state): State<ConsoleApiState>,
    Path(tenant): Path<String>,
) -> Json<ApiKeysResponse> {
    let keys = state.console.list_api_keys(&tenant).await;
    let total = keys.len();
    Json(ApiKeysResponse { keys, total })
}

async fn create_api_key(
    State(state): State<ConsoleApiState>,
    Json(request): Json<CreateApiKeyRequest>,
) -> Result<(StatusCode, Json<ApiKey>), (StatusCode, Json<ErrorResponse>)> {
    state
        .console
        .create_api_key(
            &request.tenant_id,
            &request.name,
            request.scopes,
            request.expires_at,
        )
        .await
        .map(|key| (StatusCode::CREATED, Json(key)))
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: e.to_string(),
                }),
            )
        })
}

async fn revoke_api_key(
    State(state): State<ConsoleApiState>,
    Path(id): Path<String>,
) -> Result<Json<DeleteResponse>, (StatusCode, Json<ErrorResponse>)> {
    state
        .console
        .revoke_api_key(&id)
        .await
        .map(|_| {
            Json(DeleteResponse {
                message: format!("API key '{}' revoked", id),
            })
        })
        .map_err(|e| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: e.to_string(),
                }),
            )
        })
}

async fn onboard_tenant(
    State(state): State<ConsoleApiState>,
    Json(request): Json<OnboardingRequest>,
) -> Result<(StatusCode, Json<OnboardingResponse>), (StatusCode, Json<ErrorResponse>)> {
    state
        .console
        .onboard_tenant(request)
        .await
        .map(|response| (StatusCode::CREATED, Json(response)))
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: e.to_string(),
                }),
            )
        })
}

async fn get_usage(
    State(state): State<ConsoleApiState>,
    Json(request): Json<UsageRequest>,
) -> Result<Json<UsageDashboard>, (StatusCode, Json<ErrorResponse>)> {
    state
        .console
        .get_usage_dashboard(&request.tenant_id, request.period_start, request.period_end)
        .await
        .map(Json)
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: e.to_string(),
                }),
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_console_api_state_is_clone() {
        fn assert_clone<T: Clone>() {}
        assert_clone::<ConsoleApiState>();
    }
}
