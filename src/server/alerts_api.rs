//! REST API for alert configuration and management
//!
//! This module provides endpoints for managing alerts.
//!
//! ## Endpoints
//!
//! - `GET /api/v1/alerts` - List all alerts
//! - `POST /api/v1/alerts` - Create a new alert
//! - `GET /api/v1/alerts/:id` - Get alert by ID
//! - `PUT /api/v1/alerts/:id` - Update alert
//! - `DELETE /api/v1/alerts/:id` - Delete alert
//! - `POST /api/v1/alerts/:id/toggle` - Toggle alert enabled state
//! - `GET /api/v1/alerts/history` - Get alert history
//! - `GET /api/v1/alerts/stats` - Get alert statistics

use crate::server::alerts::{AlertConfig, AlertEvent, AlertStats, AlertStore};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Shared state for alerts API
#[derive(Clone)]
pub(crate) struct AlertsApiState {
    /// Alert store
    pub store: Arc<AlertStore>,
}

/// Query parameters for alert history
#[derive(Debug, Deserialize)]
pub struct HistoryQuery {
    /// Maximum number of entries to return
    #[serde(default = "default_limit")]
    pub limit: usize,
    /// Filter by alert ID
    #[serde(default)]
    pub alert_id: Option<String>,
}

fn default_limit() -> usize {
    100
}

/// Response for list alerts
#[derive(Debug, Serialize)]
pub struct ListAlertsResponse {
    pub alerts: Vec<AlertConfig>,
    pub total: usize,
}

/// Response for alert history
#[derive(Debug, Serialize)]
pub struct HistoryResponse {
    pub events: Vec<AlertEvent>,
    pub total: usize,
}

/// Response for toggle operation
#[derive(Debug, Serialize)]
pub struct ToggleResponse {
    pub id: String,
    pub enabled: bool,
}

/// Response for delete operation
#[derive(Debug, Serialize)]
pub struct DeleteResponse {
    pub message: String,
}

/// Error response
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

/// Create the alerts API router
pub(crate) fn create_alerts_api_router(state: AlertsApiState) -> Router {
    Router::new()
        .route("/api/v1/alerts", get(list_alerts).post(create_alert))
        .route(
            "/api/v1/alerts/:id",
            get(get_alert).put(update_alert).delete(delete_alert),
        )
        .route("/api/v1/alerts/:id/toggle", post(toggle_alert))
        .route("/api/v1/alerts/history", get(get_history))
        .route("/api/v1/alerts/stats", get(get_stats))
        .with_state(state)
}

/// List all alerts
async fn list_alerts(State(state): State<AlertsApiState>) -> Json<ListAlertsResponse> {
    let alerts = state.store.list_alerts();
    let total = alerts.len();
    Json(ListAlertsResponse { alerts, total })
}

/// Create a new alert
async fn create_alert(
    State(state): State<AlertsApiState>,
    Json(config): Json<AlertConfig>,
) -> (StatusCode, Json<AlertConfig>) {
    let created = state.store.create_alert(config);
    (StatusCode::CREATED, Json(created))
}

/// Get alert by ID
async fn get_alert(
    State(state): State<AlertsApiState>,
    Path(id): Path<String>,
) -> Result<Json<AlertConfig>, (StatusCode, Json<ErrorResponse>)> {
    state.store.get_alert(&id).map(Json).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Alert '{}' not found", id),
            }),
        )
    })
}

/// Update an alert
async fn update_alert(
    State(state): State<AlertsApiState>,
    Path(id): Path<String>,
    Json(config): Json<AlertConfig>,
) -> Result<Json<AlertConfig>, (StatusCode, Json<ErrorResponse>)> {
    state
        .store
        .update_alert(&id, config)
        .map(Json)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: format!("Alert '{}' not found", id),
                }),
            )
        })
}

/// Delete an alert
async fn delete_alert(
    State(state): State<AlertsApiState>,
    Path(id): Path<String>,
) -> Result<Json<DeleteResponse>, (StatusCode, Json<ErrorResponse>)> {
    if state.store.delete_alert(&id) {
        Ok(Json(DeleteResponse {
            message: format!("Alert '{}' deleted", id),
        }))
    } else {
        Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Alert '{}' not found", id),
            }),
        ))
    }
}

/// Toggle alert enabled state
async fn toggle_alert(
    State(state): State<AlertsApiState>,
    Path(id): Path<String>,
) -> Result<Json<ToggleResponse>, (StatusCode, Json<ErrorResponse>)> {
    state
        .store
        .toggle_alert(&id)
        .map(|enabled| {
            Json(ToggleResponse {
                id: id.clone(),
                enabled,
            })
        })
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: format!("Alert '{}' not found", id),
                }),
            )
        })
}

/// Get alert history
async fn get_history(
    State(state): State<AlertsApiState>,
    Query(query): Query<HistoryQuery>,
) -> Json<HistoryResponse> {
    let events = if let Some(alert_id) = query.alert_id {
        state.store.get_alert_history(&alert_id, query.limit)
    } else {
        state.store.get_history(query.limit)
    };

    let total = events.len();
    Json(HistoryResponse { events, total })
}

/// Get alert statistics
async fn get_stats(State(state): State<AlertsApiState>) -> Json<AlertStats> {
    Json(state.store.stats())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::alerts::{AlertCondition, AlertConfig};

    fn create_test_state() -> AlertsApiState {
        AlertsApiState {
            store: AlertStore::new_shared(),
        }
    }

    #[tokio::test]
    async fn test_list_alerts_empty() {
        let state = create_test_state();
        let response = list_alerts(State(state)).await;
        assert_eq!(response.0.total, 0);
        assert!(response.0.alerts.is_empty());
    }

    #[tokio::test]
    async fn test_create_and_get_alert() {
        let state = create_test_state();

        let alert = AlertConfig::new(
            "Test Alert".to_string(),
            AlertCondition::OfflinePartitions,
            5.0,
        );
        let id = alert.id.clone();

        // Create
        let (status, created) = create_alert(State(state.clone()), Json(alert)).await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(created.0.name, "Test Alert");

        // Get
        let result = get_alert(State(state), Path(id)).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().0.name, "Test Alert");
    }

    #[tokio::test]
    async fn test_toggle_alert() {
        let state = create_test_state();

        let alert = AlertConfig::new("Test".to_string(), AlertCondition::OfflinePartitions, 5.0);
        let id = alert.id.clone();
        state.store.create_alert(alert);

        // Toggle off
        let result = toggle_alert(State(state.clone()), Path(id.clone())).await;
        assert!(result.is_ok());
        assert!(!result.unwrap().0.enabled);

        // Toggle on
        let result = toggle_alert(State(state), Path(id)).await;
        assert!(result.is_ok());
        assert!(result.unwrap().0.enabled);
    }

    #[tokio::test]
    async fn test_delete_alert() {
        let state = create_test_state();

        let alert = AlertConfig::new("Test".to_string(), AlertCondition::OfflinePartitions, 5.0);
        let id = alert.id.clone();
        state.store.create_alert(alert);

        // Delete
        let result = delete_alert(State(state.clone()), Path(id.clone())).await;
        assert!(result.is_ok());

        // Verify deleted
        let result = get_alert(State(state), Path(id)).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_stats() {
        let state = create_test_state();

        let alert1 = AlertConfig::new(
            "Alert 1".to_string(),
            AlertCondition::OfflinePartitions,
            5.0,
        );
        let mut alert2 = AlertConfig::new(
            "Alert 2".to_string(),
            AlertCondition::OfflinePartitions,
            10.0,
        );
        alert2.enabled = false;

        state.store.create_alert(alert1);
        state.store.create_alert(alert2);

        let stats = get_stats(State(state)).await;
        assert_eq!(stats.0.total_alerts, 2);
        assert_eq!(stats.0.enabled_alerts, 1);
    }
}
