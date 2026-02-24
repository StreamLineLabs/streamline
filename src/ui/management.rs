//! Management and alerting API routes for the web UI.
//!
//! Provides a dedicated router for topic management, consumer group
//! offset resets, and alert rule CRUD operations. These endpoints proxy
//! requests to the Streamline server HTTP API.

use super::client::{AlertConfig, ClientError, CreateTopicRequest, ResetOffsetsRequest};
use super::WebUiState;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Request / Response types
// ---------------------------------------------------------------------------

/// Request body for creating a topic via the management API.
#[derive(Debug, Clone, Deserialize)]
pub struct MgmtCreateTopicRequest {
    pub name: String,
    pub partitions: usize,
    pub replication_factor: usize,
}

/// Request body for resetting consumer group offsets.
#[derive(Debug, Clone, Deserialize)]
pub struct MgmtResetOffsetsRequest {
    pub topic: String,
    pub strategy: ResetStrategy,
    #[serde(default)]
    pub offset: Option<i64>,
}

/// Supported reset strategies.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ResetStrategy {
    Earliest,
    Latest,
    ToOffset,
}

/// Request body for creating an alert rule.
#[derive(Debug, Clone, Deserialize)]
pub struct MgmtCreateAlertRuleRequest {
    pub name: String,
    pub metric: String,
    pub threshold: f64,
    pub severity: AlertSeverity,
}

/// Alert severity levels.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum AlertSeverity {
    Info,
    Warning,
    Critical,
}

/// Lightweight representation of an active alert.
#[derive(Debug, Clone, Serialize)]
pub struct ActiveAlert {
    pub id: String,
    pub name: String,
    pub alert_type: String,
    pub enabled: bool,
    pub created_at: u64,
}

/// Response listing active alerts.
#[derive(Debug, Clone, Serialize)]
pub struct ActiveAlertsResponse {
    pub alerts: Vec<ActiveAlert>,
    pub total: usize,
}

/// Lightweight representation of an alert rule.
#[derive(Debug, Clone, Serialize)]
pub struct AlertRule {
    pub id: String,
    pub name: String,
    pub metric: String,
    pub threshold: f64,
    pub severity: AlertSeverity,
    pub enabled: bool,
}

/// Response listing alert rules.
#[derive(Debug, Clone, Serialize)]
pub struct AlertRulesResponse {
    pub rules: Vec<AlertRule>,
    pub total: usize,
}

/// Generic success message.
#[derive(Debug, Clone, Serialize)]
pub struct MessageResponse {
    pub message: String,
}

// ---------------------------------------------------------------------------
// Error mapping helper
// ---------------------------------------------------------------------------

fn client_error_status(err: &ClientError) -> StatusCode {
    match err {
        ClientError::ApiError(msg) if msg.contains("404") || msg.contains("not found") => {
            StatusCode::NOT_FOUND
        }
        ClientError::ApiError(msg) if msg.contains("409") || msg.contains("conflict") => {
            StatusCode::CONFLICT
        }
        ClientError::ApiError(_) => StatusCode::BAD_REQUEST,
        ClientError::Http(_) => StatusCode::BAD_GATEWAY,
        ClientError::Parse(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

/// Build the management sub-router.
///
/// Routes are mounted under `/api/management/` to avoid conflicts with
/// existing `/api/` routes in the main UI router.
pub fn management_router() -> Router<WebUiState> {
    Router::new()
        // Topic management (enhanced versions with better error handling)
        .route("/api/management/topics", post(mgmt_create_topic))
        .route("/api/management/topics/:name", delete(mgmt_delete_topic))
        // Consumer group management
        .route(
            "/api/management/groups/:group_id/reset",
            post(mgmt_reset_group_offsets),
        )
        // Alert management
        .route("/api/management/alerts", get(mgmt_get_active_alerts))
        .route(
            "/api/management/alerts/rules",
            get(mgmt_list_alert_rules).post(mgmt_create_alert_rule),
        )
        .route(
            "/api/management/alerts/rules/:rule_id",
            delete(mgmt_delete_alert_rule),
        )
}

// ---------------------------------------------------------------------------
// Handlers — Topics
// ---------------------------------------------------------------------------

async fn mgmt_create_topic(
    State(state): State<WebUiState>,
    Json(req): Json<MgmtCreateTopicRequest>,
) -> impl IntoResponse {
    let create_req = CreateTopicRequest {
        name: req.name,
        partitions: req.partitions,
        replication_factor: req.replication_factor,
        config: Default::default(),
    };

    match state.client.create_topic(create_req).await {
        Ok(()) => (
            StatusCode::CREATED,
            Json(MessageResponse {
                message: "Topic created".to_string(),
            }),
        )
            .into_response(),
        Err(e) => (client_error_status(&e), e.to_string()).into_response(),
    }
}

async fn mgmt_delete_topic(
    State(state): State<WebUiState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    match state.client.delete_topic(&name).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (client_error_status(&e), e.to_string()).into_response(),
    }
}

// ---------------------------------------------------------------------------
// Handlers — Consumer Groups
// ---------------------------------------------------------------------------

async fn mgmt_reset_group_offsets(
    State(state): State<WebUiState>,
    Path(group_id): Path<String>,
    Json(req): Json<MgmtResetOffsetsRequest>,
) -> impl IntoResponse {
    let strategy = match req.strategy {
        ResetStrategy::Earliest => super::client::ResetStrategy::Earliest,
        ResetStrategy::Latest => super::client::ResetStrategy::Latest,
        ResetStrategy::ToOffset => super::client::ResetStrategy::Offset,
    };

    let reset_req = ResetOffsetsRequest {
        topic: req.topic,
        strategy,
        partitions: None,
        timestamp_ms: None,
        offset: req.offset,
    };

    match state.client.reset_offsets(&group_id, reset_req).await {
        Ok(response) => Json(response).into_response(),
        Err(e) => (client_error_status(&e), e.to_string()).into_response(),
    }
}

// ---------------------------------------------------------------------------
// Handlers — Alerts
// ---------------------------------------------------------------------------

async fn mgmt_get_active_alerts(State(state): State<WebUiState>) -> impl IntoResponse {
    match state.client.list_alerts().await {
        Ok(list) => {
            let alerts: Vec<ActiveAlert> = list
                .alerts
                .into_iter()
                .map(|a| ActiveAlert {
                    id: a.id,
                    name: a.name,
                    alert_type: a.alert_type,
                    enabled: a.enabled,
                    created_at: a.created_at,
                })
                .collect();
            let total = alerts.len();
            Json(ActiveAlertsResponse { alerts, total }).into_response()
        }
        Err(e) => (client_error_status(&e), e.to_string()).into_response(),
    }
}

async fn mgmt_list_alert_rules(State(state): State<WebUiState>) -> impl IntoResponse {
    match state.client.list_alerts().await {
        Ok(list) => {
            let rules: Vec<AlertRule> = list
                .alerts
                .into_iter()
                .map(|a| {
                    let threshold = a
                        .condition
                        .get("threshold")
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.0);
                    let metric = a
                        .condition
                        .get("metric")
                        .and_then(|v| v.as_str())
                        .unwrap_or(&a.alert_type)
                        .to_string();
                    let severity_str = a
                        .condition
                        .get("severity")
                        .and_then(|v| v.as_str())
                        .unwrap_or("warning");
                    let severity = match severity_str {
                        "info" => AlertSeverity::Info,
                        "critical" => AlertSeverity::Critical,
                        _ => AlertSeverity::Warning,
                    };
                    AlertRule {
                        id: a.id,
                        name: a.name,
                        metric,
                        threshold,
                        severity,
                        enabled: a.enabled,
                    }
                })
                .collect();
            let total = rules.len();
            Json(AlertRulesResponse { rules, total }).into_response()
        }
        Err(e) => (client_error_status(&e), e.to_string()).into_response(),
    }
}

async fn mgmt_create_alert_rule(
    State(state): State<WebUiState>,
    Json(req): Json<MgmtCreateAlertRuleRequest>,
) -> impl IntoResponse {
    let severity_str = match req.severity {
        AlertSeverity::Info => "info",
        AlertSeverity::Warning => "warning",
        AlertSeverity::Critical => "critical",
    };

    let condition = serde_json::json!({
        "metric": req.metric,
        "threshold": req.threshold,
        "severity": severity_str,
    });

    let config = AlertConfig {
        id: String::new(),
        name: req.name,
        description: None,
        alert_type: req.metric.clone(),
        condition,
        actions: vec![],
        enabled: true,
        created_at: 0,
        updated_at: 0,
    };

    match state.client.create_alert(config).await {
        Ok(alert) => (StatusCode::CREATED, Json(alert)).into_response(),
        Err(e) => (client_error_status(&e), e.to_string()).into_response(),
    }
}

async fn mgmt_delete_alert_rule(
    State(state): State<WebUiState>,
    Path(rule_id): Path<String>,
) -> impl IntoResponse {
    match state.client.delete_alert(&rule_id).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (client_error_status(&e), e.to_string()).into_response(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_management_router_builds() {
        let config = super::super::WebUiConfig::default();
        let state = WebUiState::new(config).unwrap();
        let router: Router = management_router().with_state(state);
        assert!(std::mem::size_of_val(&router) > 0);
    }

    #[test]
    fn test_mgmt_create_topic_request_deserialize() {
        let json = r#"{"name":"test-topic","partitions":3,"replication_factor":1}"#;
        let req: MgmtCreateTopicRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.name, "test-topic");
        assert_eq!(req.partitions, 3);
        assert_eq!(req.replication_factor, 1);
    }

    #[test]
    fn test_mgmt_reset_offsets_request_earliest() {
        let json = r#"{"topic":"events","strategy":"earliest"}"#;
        let req: MgmtResetOffsetsRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.topic, "events");
        assert_eq!(req.strategy, ResetStrategy::Earliest);
        assert!(req.offset.is_none());
    }

    #[test]
    fn test_mgmt_reset_offsets_request_to_offset() {
        let json = r#"{"topic":"events","strategy":"to_offset","offset":42}"#;
        let req: MgmtResetOffsetsRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.strategy, ResetStrategy::ToOffset);
        assert_eq!(req.offset, Some(42));
    }

    #[test]
    fn test_mgmt_create_alert_rule_request() {
        let json = r#"{"name":"High Lag","metric":"consumer_lag","threshold":1000.0,"severity":"critical"}"#;
        let req: MgmtCreateAlertRuleRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.name, "High Lag");
        assert_eq!(req.metric, "consumer_lag");
        assert_eq!(req.threshold, 1000.0);
        assert_eq!(req.severity, AlertSeverity::Critical);
    }

    #[test]
    fn test_alert_severity_variants() {
        let info: AlertSeverity = serde_json::from_str(r#""info""#).unwrap();
        assert_eq!(info, AlertSeverity::Info);
        let warning: AlertSeverity = serde_json::from_str(r#""warning""#).unwrap();
        assert_eq!(warning, AlertSeverity::Warning);
        let critical: AlertSeverity = serde_json::from_str(r#""critical""#).unwrap();
        assert_eq!(critical, AlertSeverity::Critical);
    }

    #[test]
    fn test_reset_strategy_variants() {
        let earliest: ResetStrategy = serde_json::from_str(r#""earliest""#).unwrap();
        assert_eq!(earliest, ResetStrategy::Earliest);
        let latest: ResetStrategy = serde_json::from_str(r#""latest""#).unwrap();
        assert_eq!(latest, ResetStrategy::Latest);
        let to_offset: ResetStrategy = serde_json::from_str(r#""to_offset""#).unwrap();
        assert_eq!(to_offset, ResetStrategy::ToOffset);
    }

    #[test]
    fn test_client_error_status_mapping() {
        let not_found = ClientError::ApiError("HTTP 404: not found".to_string());
        assert_eq!(client_error_status(&not_found), StatusCode::NOT_FOUND);

        let conflict = ClientError::ApiError("HTTP 409: conflict".to_string());
        assert_eq!(client_error_status(&conflict), StatusCode::CONFLICT);

        let bad_req = ClientError::ApiError("bad request".to_string());
        assert_eq!(client_error_status(&bad_req), StatusCode::BAD_REQUEST);

        let parse = ClientError::Parse("bad json".to_string());
        assert_eq!(
            client_error_status(&parse),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[test]
    fn test_active_alert_serialization() {
        let alert = ActiveAlert {
            id: "a1".to_string(),
            name: "High Lag".to_string(),
            alert_type: "consumer_lag".to_string(),
            enabled: true,
            created_at: 1234567890,
        };
        let json = serde_json::to_value(&alert).unwrap();
        assert_eq!(json["id"], "a1");
        assert_eq!(json["enabled"], true);
    }

    #[test]
    fn test_alert_rules_response_serialization() {
        let resp = AlertRulesResponse {
            rules: vec![AlertRule {
                id: "r1".to_string(),
                name: "Test Rule".to_string(),
                metric: "bytes_in".to_string(),
                threshold: 500.0,
                severity: AlertSeverity::Warning,
                enabled: true,
            }],
            total: 1,
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["total"], 1);
        assert_eq!(json["rules"][0]["metric"], "bytes_in");
    }

    #[test]
    fn test_message_response_serialization() {
        let resp = MessageResponse {
            message: "Topic created".to_string(),
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["message"], "Topic created");
    }
}
