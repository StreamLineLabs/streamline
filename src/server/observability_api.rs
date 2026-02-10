//! Observability API - Unified monitoring dashboard endpoints
//!
//! Provides a single pane of glass for metrics, traces, logs, and alerts:
//!
//! ## Endpoints
//!
//! - `GET /api/v1/observability/overview` - Full dashboard snapshot
//! - `GET /api/v1/observability/metrics` - List all metric series
//! - `GET /api/v1/observability/metrics/:name` - Get specific metric
//! - `GET /api/v1/observability/system` - System resource metrics
//! - `GET /api/v1/observability/alerts` - Active alerts
//! - `POST /api/v1/observability/alerts/rules` - Add an alert rule
//! - `GET /api/v1/observability/alerts/rules` - List alert rules
//! - `DELETE /api/v1/observability/alerts/rules/:name` - Remove alert rule
//! - `GET /api/v1/observability/connections` - Active connections

use crate::observability::dashboard::{
    default_alert_rules, ActiveAlert, AlertEvaluator, AlertRule, DashboardConfig,
    DashboardSnapshot, MetricsAggregator,
};
use crate::observability::{ObservabilityConfig, ObservabilityManager, SystemMetrics};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{delete, get},
    Json, Router,
};
use serde::Serialize;
use std::sync::Arc;

/// Shared state for the Observability API
#[derive(Clone)]
pub struct ObservabilityApiState {
    pub obs_manager: Arc<ObservabilityManager>,
    pub aggregator: Arc<MetricsAggregator>,
    pub alert_evaluator: Arc<AlertEvaluator>,
}

impl ObservabilityApiState {
    pub fn new() -> Self {
        let alert_evaluator = AlertEvaluator::new();
        // Register default alert rules
        for rule in default_alert_rules() {
            let _ = alert_evaluator.add_rule(rule);
        }

        Self {
            obs_manager: Arc::new(ObservabilityManager::new(ObservabilityConfig::default())),
            aggregator: Arc::new(MetricsAggregator::new(DashboardConfig::default())),
            alert_evaluator: Arc::new(alert_evaluator),
        }
    }
}

impl Default for ObservabilityApiState {
    fn default() -> Self {
        Self::new()
    }
}

/// Overview response combining all observability data
#[derive(Debug, Serialize)]
pub struct OverviewResponse {
    pub system: SystemMetrics,
    pub dashboard: DashboardSnapshot,
    pub active_alerts: Vec<ActiveAlert>,
    pub alert_rules_count: usize,
    pub connections_count: usize,
}

/// Error response
#[derive(Debug, Serialize)]
pub struct ObsErrorResponse {
    pub error: String,
}

/// Delete response
#[derive(Debug, Serialize)]
pub struct ObsDeleteResponse {
    pub message: String,
}

/// Create the Observability API router
pub fn create_observability_api_router(state: ObservabilityApiState) -> Router {
    Router::new()
        .route("/api/v1/observability/overview", get(overview))
        .route("/api/v1/observability/metrics", get(list_metrics))
        .route("/api/v1/observability/metrics/:name", get(get_metric))
        .route("/api/v1/observability/system", get(system_metrics))
        .route("/api/v1/observability/alerts", get(active_alerts))
        .route(
            "/api/v1/observability/alerts/rules",
            get(list_alert_rules).post(add_alert_rule),
        )
        .route(
            "/api/v1/observability/alerts/rules/:name",
            delete(remove_alert_rule),
        )
        .route("/api/v1/observability/connections", get(connections))
        .with_state(state)
}

async fn overview(State(state): State<ObservabilityApiState>) -> Json<OverviewResponse> {
    let system = state.obs_manager.collect();
    let dashboard = state.aggregator.get_dashboard_snapshot();
    let active_alerts = state.alert_evaluator.evaluate(&state.aggregator);
    let rules_count = state.alert_evaluator.list_rules().len();

    Json(OverviewResponse {
        system,
        dashboard,
        active_alerts,
        alert_rules_count: rules_count,
        connections_count: 0,
    })
}

async fn list_metrics(State(state): State<ObservabilityApiState>) -> Json<serde_json::Value> {
    let metrics = state.aggregator.get_all_metrics();
    Json(serde_json::json!({
        "metrics": metrics,
        "total": metrics.len(),
    }))
}

async fn get_metric(
    State(state): State<ObservabilityApiState>,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ObsErrorResponse>)> {
    state
        .aggregator
        .get_metric(&name, None)
        .map(|series| {
            Json(serde_json::json!({
                "name": series.name,
                "data_points": series.data_points.len(),
                "series": series,
            }))
        })
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ObsErrorResponse {
                    error: format!("Metric '{}' not found", name),
                }),
            )
        })
}

async fn system_metrics(State(state): State<ObservabilityApiState>) -> Json<SystemMetrics> {
    Json(state.obs_manager.collect())
}

async fn active_alerts(State(state): State<ObservabilityApiState>) -> Json<serde_json::Value> {
    let alerts = state.alert_evaluator.evaluate(&state.aggregator);
    Json(serde_json::json!({
        "alerts": alerts,
        "total": alerts.len(),
    }))
}

async fn list_alert_rules(State(state): State<ObservabilityApiState>) -> Json<serde_json::Value> {
    let rules = state.alert_evaluator.list_rules();
    Json(serde_json::json!({
        "rules": rules,
        "total": rules.len(),
    }))
}

async fn add_alert_rule(
    State(state): State<ObservabilityApiState>,
    Json(rule): Json<AlertRule>,
) -> Result<(StatusCode, Json<AlertRule>), (StatusCode, Json<ObsErrorResponse>)> {
    let rule_clone = rule.clone();
    state.alert_evaluator.add_rule(rule).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ObsErrorResponse {
                error: e.to_string(),
            }),
        )
    })?;
    Ok((StatusCode::CREATED, Json(rule_clone)))
}

async fn remove_alert_rule(
    State(state): State<ObservabilityApiState>,
    Path(name): Path<String>,
) -> Result<Json<ObsDeleteResponse>, (StatusCode, Json<ObsErrorResponse>)> {
    state.alert_evaluator.remove_rule(&name).map_err(|e| {
        (
            StatusCode::NOT_FOUND,
            Json(ObsErrorResponse {
                error: e.to_string(),
            }),
        )
    })?;
    Ok(Json(ObsDeleteResponse {
        message: format!("Alert rule '{}' removed", name),
    }))
}

async fn connections(State(_state): State<ObservabilityApiState>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "connections": [],
        "total": 0,
        "message": "Connection tracking available when integrated with server shutdown coordinator",
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_observability_state_new() {
        let state = ObservabilityApiState::new();
        let rules = state.alert_evaluator.list_rules();
        // Should have default alert rules loaded
        assert!(!rules.is_empty());
    }

    #[test]
    fn test_overview_response_serialization() {
        let resp = ObsDeleteResponse {
            message: "test".to_string(),
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("test"));
    }
}
