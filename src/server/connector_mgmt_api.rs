//! REST API for Declarative Connector Management
//!
//! ## Endpoints
//!
//! - `GET /api/v1/connectors/managed` - List managed connectors
//! - `POST /api/v1/connectors/apply` - Apply declarative connector spec (YAML/JSON)
//! - `GET /api/v1/connectors/managed/:name` - Get connector status
//! - `DELETE /api/v1/connectors/managed/:name` - Remove managed connector
//! - `POST /api/v1/connectors/managed/:name/restart` - Restart connector

use crate::marketplace::declarative::{ConnectorManager, ConnectorRuntimeState, ManagedConnector};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct ConnectorMgmtApiState {
    pub manager: Arc<ConnectorManager>,
}

#[derive(Debug, Serialize)]
pub struct ManagedConnectorsResponse {
    pub connectors: Vec<ManagedConnector>,
    pub total: usize,
}

#[derive(Debug, Serialize)]
pub struct ApplyResponse {
    pub name: String,
    pub state: ConnectorRuntimeState,
    pub message: String,
}

#[derive(Debug, Deserialize)]
pub struct ApplyRequest {
    pub spec_yaml: String,
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

#[derive(Debug, Serialize)]
pub struct DeleteResponse {
    pub message: String,
}

pub(crate) fn create_connector_mgmt_api_router(state: ConnectorMgmtApiState) -> Router {
    Router::new()
        .route(
            "/api/v1/connectors/managed",
            get(list_managed).post(apply_spec),
        )
        .route(
            "/api/v1/connectors/managed/:name",
            get(get_connector).delete(remove_connector),
        )
        .route(
            "/api/v1/connectors/managed/:name/restart",
            post(restart_connector),
        )
        .with_state(state)
}

async fn list_managed(
    State(state): State<ConnectorMgmtApiState>,
) -> Json<ManagedConnectorsResponse> {
    let connectors = state.manager.list().await;
    let total = connectors.len();
    Json(ManagedConnectorsResponse { connectors, total })
}

async fn apply_spec(
    State(state): State<ConnectorMgmtApiState>,
    Json(request): Json<ApplyRequest>,
) -> Result<(StatusCode, Json<ApplyResponse>), (StatusCode, Json<ErrorResponse>)> {
    let connector = state
        .manager
        .apply_yaml(&request.spec_yaml)
        .await
        .map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: format!("Failed to apply connector: {}", e),
                }),
            )
        })?;

    Ok((
        StatusCode::CREATED,
        Json(ApplyResponse {
            name: connector.name.clone(),
            state: connector.state.clone(),
            message: "Connector applied successfully".to_string(),
        }),
    ))
}

async fn get_connector(
    State(state): State<ConnectorMgmtApiState>,
    Path(name): Path<String>,
) -> Result<Json<ManagedConnector>, (StatusCode, Json<ErrorResponse>)> {
    state.manager.status(&name).await.map(Json).map_err(|e| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )
    })
}

async fn remove_connector(
    State(state): State<ConnectorMgmtApiState>,
    Path(name): Path<String>,
) -> Result<Json<DeleteResponse>, (StatusCode, Json<ErrorResponse>)> {
    state.manager.delete(&name).await.map_err(|e| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )
    })?;
    Ok(Json(DeleteResponse {
        message: format!("Connector '{}' removed", name),
    }))
}

async fn restart_connector(
    State(state): State<ConnectorMgmtApiState>,
    Path(name): Path<String>,
) -> Result<Json<ApplyResponse>, (StatusCode, Json<ErrorResponse>)> {
    // Stop then start to simulate restart
    state.manager.stop(&name).await.map_err(|e| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )
    })?;
    state.manager.start(&name).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )
    })?;
    Ok(Json(ApplyResponse {
        name: name.clone(),
        state: ConnectorRuntimeState::Running,
        message: "Connector restarted".to_string(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connector_mgmt_api_state_is_clone() {
        fn assert_clone<T: Clone>() {}
        assert_clone::<ConnectorMgmtApiState>();
    }
}
