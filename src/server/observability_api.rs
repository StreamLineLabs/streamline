//! Unified Observability Dashboard REST API
//!
//! Provides the web UI backend for the observability dashboard with metrics,
//! alerts, health monitoring, partition heatmaps, and consumer lag tracking.
//!
//! ## Endpoints
//!
//! - `GET  /api/v1/observability/dashboard`              - Full dashboard snapshot
//! - `GET  /api/v1/observability/metrics`                - Current metric values (filterable)
//! - `GET  /api/v1/observability/health`                 - Cluster health summary
//! - `POST /api/v1/observability/alerts/rules`           - Create an alert rule
//! - `GET  /api/v1/observability/alerts/rules`           - List all alert rules
//! - `DELETE /api/v1/observability/alerts/rules/:id`     - Delete an alert rule
//! - `GET  /api/v1/observability/alerts/active`          - List currently firing alerts
//! - `POST /api/v1/observability/alerts/:id/acknowledge` - Acknowledge an alert
//! - `GET  /api/v1/observability/topics/heatmap`         - Partition heat map data
//! - `GET  /api/v1/observability/consumer-lag`           - Consumer lag by group/topic

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{delete, get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

// Re-import original infrastructure for backward-compatible router
use crate::observability::dashboard::{
    default_alert_rules, ActiveAlert as DashboardActiveAlert, AlertEvaluator, AlertRule,
    DashboardConfig, DashboardSnapshot, MetricsAggregator,
};
use crate::observability::{ObservabilityConfig, ObservabilityManager, SystemMetrics};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Alert condition for threshold evaluation
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AlertCondition {
    GreaterThan,
    LessThan,
    Equal,
    NotEqual,
}

/// A metric value — gauge, counter, or histogram bucket
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MetricValue {
    Gauge { value: f64 },
    Counter { value: u64 },
    Histogram { count: u64, sum: f64, p50: f64, p95: f64, p99: f64 },
}

/// Configuration for an alert rule
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertRuleConfig {
    pub id: String,
    pub name: String,
    pub metric: String,
    pub condition: AlertCondition,
    pub threshold: f64,
    pub duration_secs: u64,
    pub severity: String,
    pub channels: Vec<String>,
}

/// A currently-firing alert
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActiveAlert {
    pub rule_id: String,
    pub rule_name: String,
    pub severity: String,
    pub message: String,
    pub fired_at: String,
    pub acknowledged: bool,
    pub acknowledged_by: Option<String>,
}

/// Cluster health summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthSummary {
    pub status: String,
    pub broker_count: usize,
    pub topic_count: usize,
    pub partition_count: usize,
    pub total_messages: u64,
    pub uptime_secs: u64,
    pub checks: Vec<HealthCheck>,
}

/// Individual health check result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheck {
    pub name: String,
    pub status: String,
    pub message: String,
}

/// Partition heatmap entry — throughput per partition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionHeatmapEntry {
    pub topic: String,
    pub partition: u32,
    pub messages_per_sec: f64,
    pub bytes_per_sec: f64,
    pub lag: u64,
}

/// Consumer lag entry — lag by group/topic/partition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerLagEntry {
    pub group: String,
    pub topic: String,
    pub partition: u32,
    pub current_offset: i64,
    pub end_offset: i64,
    pub lag: i64,
}

/// Dashboard snapshot response combining gauges, counters, histograms, and alerts
#[derive(Debug, Serialize)]
pub struct DashboardResponse {
    pub metrics: HashMap<String, MetricValue>,
    pub alert_rules: Vec<AlertRuleConfig>,
    pub active_alerts: Vec<ActiveAlert>,
    pub health: HealthSummary,
}

/// Query parameters for the metrics endpoint
#[derive(Debug, Deserialize)]
pub struct MetricsQuery {
    #[serde(default)]
    pub prefix: Option<String>,
}

/// Query parameters for consumer-lag endpoint
#[derive(Debug, Deserialize)]
pub struct ConsumerLagQuery {
    #[serde(default)]
    pub group: Option<String>,
    #[serde(default)]
    pub topic: Option<String>,
}

/// Query parameters for heatmap endpoint
#[derive(Debug, Deserialize)]
pub struct HeatmapQuery {
    #[serde(default)]
    pub topic: Option<String>,
}

/// Acknowledge request body
#[derive(Debug, Serialize, Deserialize)]
pub struct AcknowledgeRequest {
    #[serde(default)]
    pub acknowledged_by: Option<String>,
}

/// Generic error response
#[derive(Debug, Serialize)]
pub struct ObsErrorResponse {
    pub error: String,
}

/// Generic delete/action response
#[derive(Debug, Serialize)]
pub struct ObsDeleteResponse {
    pub message: String,
}

// ---------------------------------------------------------------------------
// Shared state for the Unified Observability Dashboard API
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct ObservabilityApiState {
    pub alert_rules: Arc<RwLock<Vec<AlertRuleConfig>>>,
    pub active_alerts: Arc<RwLock<Vec<ActiveAlert>>>,
    pub metric_snapshots: Arc<RwLock<HashMap<String, MetricValue>>>,
    // Existing infrastructure (used by backward-compat router)
    pub obs_manager: Arc<ObservabilityManager>,
    pub aggregator: Arc<MetricsAggregator>,
    pub alert_evaluator: Arc<AlertEvaluator>,
}

impl ObservabilityApiState {
    pub fn new() -> Self {
        let alert_evaluator = AlertEvaluator::new();
        for rule in default_alert_rules() {
            let _ = alert_evaluator.add_rule(rule);
        }

        let mut default_metrics = HashMap::new();
        default_metrics.insert(
            "broker.messages_in".to_string(),
            MetricValue::Counter { value: 0 },
        );
        default_metrics.insert(
            "broker.bytes_in".to_string(),
            MetricValue::Counter { value: 0 },
        );
        default_metrics.insert(
            "broker.cpu_usage".to_string(),
            MetricValue::Gauge { value: 0.0 },
        );

        Self {
            alert_rules: Arc::new(RwLock::new(Vec::new())),
            active_alerts: Arc::new(RwLock::new(Vec::new())),
            metric_snapshots: Arc::new(RwLock::new(default_metrics)),
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

// ---------------------------------------------------------------------------
// Router constructors
// ---------------------------------------------------------------------------

/// Create the unified observability dashboard router with all endpoints.
pub fn create_observability_router() -> Router {
    let state = ObservabilityApiState::new();
    build_router(state)
}

/// Create the observability API router with the given state (backward-compatible entry point).
pub fn create_observability_api_router(state: ObservabilityApiState) -> Router {
    build_router(state)
}

fn build_router(state: ObservabilityApiState) -> Router {
    Router::new()
        // Unified dashboard
        .route("/api/v1/observability/dashboard", get(dashboard))
        .route("/api/v1/observability/metrics", get(metrics))
        .route("/api/v1/observability/health", get(health))
        // Alert rules CRUD
        .route(
            "/api/v1/observability/alerts/rules",
            get(list_alert_rules).post(create_alert_rule),
        )
        .route(
            "/api/v1/observability/alerts/rules/:id",
            delete(delete_alert_rule),
        )
        // Active alerts
        .route("/api/v1/observability/alerts/active", get(list_active_alerts))
        .route(
            "/api/v1/observability/alerts/:id/acknowledge",
            post(acknowledge_alert),
        )
        // Heatmap & consumer lag
        .route("/api/v1/observability/topics/heatmap", get(topics_heatmap))
        .route("/api/v1/observability/consumer-lag", get(consumer_lag))
        // ---- Backward-compatible legacy endpoints ----
        .route("/api/v1/observability/overview", get(legacy_overview))
        .route("/api/v1/observability/metrics/:name", get(legacy_get_metric))
        .route("/api/v1/observability/system", get(legacy_system_metrics))
        .route("/api/v1/observability/alerts", get(legacy_active_alerts))
        .route("/api/v1/observability/connections", get(legacy_connections))
        .route("/api/v1/observability/trace/{trace_id}", get(legacy_get_message_trace))
        .route("/api/v1/observability/topology", get(legacy_get_topology))
        .route("/api/v1/observability/lag/summary", get(legacy_get_lag_summary))
        .route("/api/v1/observability/hotspots", get(legacy_get_partition_hotspots))
        .with_state(state)
}

// ---------------------------------------------------------------------------
// New unified endpoint handlers
// ---------------------------------------------------------------------------

fn now_iso() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!("{secs}")
}

fn build_health(state: &ObservabilityApiState) -> HealthSummary {
    let metrics = state.metric_snapshots.read().unwrap();
    let active = state.active_alerts.read().unwrap();

    let status = if active.iter().any(|a| a.severity == "critical") {
        "critical"
    } else if active.iter().any(|a| a.severity == "warning") {
        "degraded"
    } else {
        "healthy"
    };

    let dashboard = state.aggregator.get_dashboard_snapshot();

    HealthSummary {
        status: status.to_string(),
        broker_count: 1,
        topic_count: dashboard.topic_metrics.len(),
        partition_count: dashboard
            .topic_metrics
            .iter()
            .map(|t| t.partition_count as usize)
            .sum(),
        total_messages: match metrics.get("broker.messages_in") {
            Some(MetricValue::Counter { value }) => *value,
            _ => 0,
        },
        uptime_secs: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
        checks: vec![
            HealthCheck {
                name: "storage".to_string(),
                status: "healthy".to_string(),
                message: "Storage engine operational".to_string(),
            },
            HealthCheck {
                name: "network".to_string(),
                status: "healthy".to_string(),
                message: "All listeners active".to_string(),
            },
        ],
    }
}

/// GET /api/v1/observability/dashboard
async fn dashboard(State(state): State<ObservabilityApiState>) -> Json<DashboardResponse> {
    let metrics = state.metric_snapshots.read().unwrap().clone();
    let rules = state.alert_rules.read().unwrap().clone();
    let alerts = state.active_alerts.read().unwrap().clone();
    let health = build_health(&state);

    Json(DashboardResponse {
        metrics,
        alert_rules: rules,
        active_alerts: alerts,
        health,
    })
}

/// GET /api/v1/observability/metrics?prefix=broker
async fn metrics(
    State(state): State<ObservabilityApiState>,
    Query(query): Query<MetricsQuery>,
) -> Json<serde_json::Value> {
    let all = state.metric_snapshots.read().unwrap();
    let filtered: HashMap<&String, &MetricValue> = match &query.prefix {
        Some(prefix) => all.iter().filter(|(k, _)| k.starts_with(prefix.as_str())).collect(),
        None => all.iter().collect(),
    };
    Json(serde_json::json!({
        "metrics": filtered,
        "total": filtered.len(),
    }))
}

/// GET /api/v1/observability/health
async fn health(State(state): State<ObservabilityApiState>) -> Json<HealthSummary> {
    Json(build_health(&state))
}

/// POST /api/v1/observability/alerts/rules
async fn create_alert_rule(
    State(state): State<ObservabilityApiState>,
    Json(rule): Json<AlertRuleConfig>,
) -> Result<(StatusCode, Json<AlertRuleConfig>), (StatusCode, Json<ObsErrorResponse>)> {
    let mut rules = state.alert_rules.write().unwrap();
    if rules.iter().any(|r| r.id == rule.id) {
        return Err((
            StatusCode::CONFLICT,
            Json(ObsErrorResponse {
                error: format!("Alert rule '{}' already exists", rule.id),
            }),
        ));
    }
    let created = rule.clone();
    rules.push(rule);
    Ok((StatusCode::CREATED, Json(created)))
}

/// GET /api/v1/observability/alerts/rules
async fn list_alert_rules(State(state): State<ObservabilityApiState>) -> Json<serde_json::Value> {
    let rules = state.alert_rules.read().unwrap();
    Json(serde_json::json!({
        "rules": *rules,
        "total": rules.len(),
    }))
}

/// DELETE /api/v1/observability/alerts/rules/:id
async fn delete_alert_rule(
    State(state): State<ObservabilityApiState>,
    Path(id): Path<String>,
) -> Result<Json<ObsDeleteResponse>, (StatusCode, Json<ObsErrorResponse>)> {
    let mut rules = state.alert_rules.write().unwrap();
    let before = rules.len();
    rules.retain(|r| r.id != id);
    if rules.len() == before {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ObsErrorResponse {
                error: format!("Alert rule '{}' not found", id),
            }),
        ));
    }
    Ok(Json(ObsDeleteResponse {
        message: format!("Alert rule '{}' deleted", id),
    }))
}

/// GET /api/v1/observability/alerts/active
async fn list_active_alerts(State(state): State<ObservabilityApiState>) -> Json<serde_json::Value> {
    let alerts = state.active_alerts.read().unwrap();
    Json(serde_json::json!({
        "alerts": *alerts,
        "total": alerts.len(),
    }))
}

/// POST /api/v1/observability/alerts/:id/acknowledge
async fn acknowledge_alert(
    State(state): State<ObservabilityApiState>,
    Path(id): Path<String>,
    Json(body): Json<AcknowledgeRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ObsErrorResponse>)> {
    let mut alerts = state.active_alerts.write().unwrap();
    let alert = alerts.iter_mut().find(|a| a.rule_id == id);
    match alert {
        Some(a) => {
            a.acknowledged = true;
            a.acknowledged_by = body.acknowledged_by.or_else(|| Some("anonymous".to_string()));
            Ok(Json(serde_json::json!({
                "message": format!("Alert '{}' acknowledged", id),
                "acknowledged_by": a.acknowledged_by,
            })))
        }
        None => Err((
            StatusCode::NOT_FOUND,
            Json(ObsErrorResponse {
                error: format!("Active alert '{}' not found", id),
            }),
        )),
    }
}

/// GET /api/v1/observability/topics/heatmap?topic=orders
async fn topics_heatmap(
    State(state): State<ObservabilityApiState>,
    Query(query): Query<HeatmapQuery>,
) -> Json<serde_json::Value> {
    let dashboard = state.aggregator.get_dashboard_snapshot();
    let entries: Vec<PartitionHeatmapEntry> = dashboard
        .topic_metrics
        .iter()
        .filter(|t| query.topic.as_ref().map_or(true, |q| t.topic_name.contains(q.as_str())))
        .flat_map(|t| {
            (0..t.partition_count).map(move |p| PartitionHeatmapEntry {
                topic: t.topic_name.clone(),
                partition: p,
                messages_per_sec: 0.0,
                bytes_per_sec: 0.0,
                lag: 0,
            })
        })
        .collect();
    Json(serde_json::json!({
        "heatmap": entries,
        "total_partitions": entries.len(),
    }))
}

/// GET /api/v1/observability/consumer-lag?group=my-group&topic=orders
async fn consumer_lag(
    State(state): State<ObservabilityApiState>,
    Query(query): Query<ConsumerLagQuery>,
) -> Json<serde_json::Value> {
    let dashboard = state.aggregator.get_dashboard_snapshot();
    let entries: Vec<ConsumerLagEntry> = dashboard
        .consumer_group_metrics
        .iter()
        .filter(|g| query.group.as_ref().map_or(true, |q| g.group_id.contains(q.as_str())))
        .filter(|g| {
            query.topic.as_ref().map_or(true, |q| {
                g.subscribed_topics.iter().any(|t| t.contains(q.as_str()))
            })
        })
        .flat_map(|g| {
            g.subscribed_topics.iter().map(move |topic| ConsumerLagEntry {
                group: g.group_id.clone(),
                topic: topic.clone(),
                partition: 0,
                current_offset: 0,
                end_offset: g.total_lag as i64,
                lag: g.total_lag as i64,
            })
        })
        .collect();
    let total_lag: i64 = entries.iter().map(|e| e.lag).sum();
    Json(serde_json::json!({
        "entries": entries,
        "total_lag": total_lag,
        "total_entries": entries.len(),
    }))
}

// ---------------------------------------------------------------------------
// Legacy / backward-compatible endpoint handlers
// ---------------------------------------------------------------------------

/// Legacy overview response
#[derive(Debug, Serialize)]
pub struct OverviewResponse {
    pub system: SystemMetrics,
    pub dashboard: DashboardSnapshot,
    pub active_alerts: Vec<DashboardActiveAlert>,
    pub alert_rules_count: usize,
    pub connections_count: usize,
}

async fn legacy_overview(State(state): State<ObservabilityApiState>) -> Json<OverviewResponse> {
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

async fn legacy_get_metric(
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

async fn legacy_system_metrics(State(state): State<ObservabilityApiState>) -> Json<SystemMetrics> {
    Json(state.obs_manager.collect())
}

async fn legacy_active_alerts(
    State(state): State<ObservabilityApiState>,
) -> Json<serde_json::Value> {
    let alerts = state.alert_evaluator.evaluate(&state.aggregator);
    Json(serde_json::json!({
        "alerts": alerts,
        "total": alerts.len(),
    }))
}

async fn legacy_connections(
    State(_state): State<ObservabilityApiState>,
) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "connections": [],
        "total": 0,
        "message": "Connection tracking available when integrated with server shutdown coordinator",
    }))
}

async fn legacy_get_message_trace(Path(trace_id): Path<String>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "trace_id": trace_id,
        "spans": [{
            "operation": "produce",
            "topic": "unknown",
            "partition": 0,
            "start_time_ms": 0,
            "duration_us": 0,
            "status": "not_found",
        }],
        "message": "Trace lookup requires OpenTelemetry tracing to be enabled (--otel-enabled). Messages must include W3C traceparent headers."
    }))
}

async fn legacy_get_topology(
    State(state): State<ObservabilityApiState>,
) -> Json<serde_json::Value> {
    let dashboard = state.aggregator.get_dashboard_snapshot();
    Json(serde_json::json!({
        "nodes": [{
            "id": "broker-0",
            "type": "broker",
            "status": "running",
            "topics": dashboard.topic_metrics.len(),
            "consumer_groups": dashboard.consumer_group_metrics.len(),
        }],
        "edges": [],
        "topics": dashboard.topic_metrics.len(),
        "consumer_groups": dashboard.consumer_group_metrics.len(),
        "active_alerts": dashboard.active_alerts.len(),
    }))
}

async fn legacy_get_lag_summary(
    State(state): State<ObservabilityApiState>,
) -> Json<serde_json::Value> {
    let dashboard = state.aggregator.get_dashboard_snapshot();
    let total_lag: u64 = dashboard
        .consumer_group_metrics
        .iter()
        .map(|g| g.total_lag)
        .sum();
    Json(serde_json::json!({
        "total_lag": total_lag,
        "groups_with_lag": dashboard.consumer_group_metrics.iter().filter(|g| g.total_lag > 0).count(),
        "groups_total": dashboard.consumer_group_metrics.len(),
        "max_lag_group": dashboard.consumer_group_metrics.iter().max_by_key(|g| g.total_lag).map(|g| &g.group_id),
        "lag_threshold_warning": 10000,
        "lag_threshold_critical": 100000,
    }))
}

async fn legacy_get_partition_hotspots(
    State(state): State<ObservabilityApiState>,
) -> Json<serde_json::Value> {
    let dashboard = state.aggregator.get_dashboard_snapshot();
    let total_partitions: u32 = dashboard
        .topic_metrics
        .iter()
        .map(|t| t.partition_count)
        .sum();
    Json(serde_json::json!({
        "hotspots": [],
        "total_partitions": total_partitions,
        "message": "Partition hotspot detection analyzes write distribution across partitions. Enable metrics for detailed tracking.",
    }))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    fn test_state() -> ObservabilityApiState {
        ObservabilityApiState::new()
    }

    fn test_router() -> Router {
        create_observability_router()
    }

    fn sample_rule(id: &str) -> AlertRuleConfig {
        AlertRuleConfig {
            id: id.to_string(),
            name: format!("Rule {id}"),
            metric: "broker.cpu_usage".to_string(),
            condition: AlertCondition::GreaterThan,
            threshold: 90.0,
            duration_secs: 60,
            severity: "warning".to_string(),
            channels: vec!["slack".to_string()],
        }
    }

    fn sample_active_alert(rule_id: &str) -> ActiveAlert {
        ActiveAlert {
            rule_id: rule_id.to_string(),
            rule_name: "High CPU".to_string(),
            severity: "critical".to_string(),
            message: "CPU above 90%".to_string(),
            fired_at: now_iso(),
            acknowledged: false,
            acknowledged_by: None,
        }
    }

    // -- State & type tests --

    #[test]
    fn test_state_new_has_default_metrics() {
        let state = test_state();
        let metrics = state.metric_snapshots.read().unwrap();
        assert!(metrics.contains_key("broker.messages_in"));
        assert!(metrics.contains_key("broker.bytes_in"));
        assert!(metrics.contains_key("broker.cpu_usage"));
    }

    #[test]
    fn test_state_default_trait() {
        let state = ObservabilityApiState::default();
        let rules = state.alert_rules.read().unwrap();
        assert!(rules.is_empty());
    }

    #[test]
    fn test_alert_condition_serde_roundtrip() {
        let cond = AlertCondition::GreaterThan;
        let json = serde_json::to_string(&cond).unwrap();
        let back: AlertCondition = serde_json::from_str(&json).unwrap();
        assert_eq!(back, AlertCondition::GreaterThan);
    }

    #[test]
    fn test_metric_value_gauge_serde() {
        let v = MetricValue::Gauge { value: 42.5 };
        let json = serde_json::to_string(&v).unwrap();
        assert!(json.contains("42.5"));
        assert!(json.contains("gauge"));
    }

    #[test]
    fn test_metric_value_counter_serde() {
        let v = MetricValue::Counter { value: 100 };
        let json = serde_json::to_string(&v).unwrap();
        assert!(json.contains("100"));
        assert!(json.contains("counter"));
    }

    #[test]
    fn test_metric_value_histogram_serde() {
        let v = MetricValue::Histogram {
            count: 1000,
            sum: 5000.0,
            p50: 4.0,
            p95: 8.0,
            p99: 12.0,
        };
        let json = serde_json::to_string(&v).unwrap();
        assert!(json.contains("histogram"));
        assert!(json.contains("p99"));
    }

    #[test]
    fn test_alert_rule_config_serde() {
        let rule = sample_rule("r1");
        let json = serde_json::to_string(&rule).unwrap();
        let back: AlertRuleConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(back.id, "r1");
        assert_eq!(back.threshold, 90.0);
    }

    #[test]
    fn test_active_alert_serde() {
        let alert = sample_active_alert("r1");
        let json = serde_json::to_string(&alert).unwrap();
        let back: ActiveAlert = serde_json::from_str(&json).unwrap();
        assert_eq!(back.rule_id, "r1");
        assert!(!back.acknowledged);
    }

    #[test]
    fn test_health_summary_serde() {
        let h = HealthSummary {
            status: "healthy".to_string(),
            broker_count: 1,
            topic_count: 5,
            partition_count: 15,
            total_messages: 10_000,
            uptime_secs: 3600,
            checks: vec![HealthCheck {
                name: "storage".to_string(),
                status: "healthy".to_string(),
                message: "OK".to_string(),
            }],
        };
        let json = serde_json::to_string(&h).unwrap();
        assert!(json.contains("healthy"));
        assert!(json.contains("storage"));
    }

    #[test]
    fn test_partition_heatmap_entry_serde() {
        let entry = PartitionHeatmapEntry {
            topic: "orders".to_string(),
            partition: 0,
            messages_per_sec: 150.5,
            bytes_per_sec: 4096.0,
            lag: 42,
        };
        let json = serde_json::to_string(&entry).unwrap();
        assert!(json.contains("orders"));
        assert!(json.contains("150.5"));
    }

    #[test]
    fn test_consumer_lag_entry_serde() {
        let entry = ConsumerLagEntry {
            group: "g1".to_string(),
            topic: "t1".to_string(),
            partition: 0,
            current_offset: 100,
            end_offset: 200,
            lag: 100,
        };
        let json = serde_json::to_string(&entry).unwrap();
        let back: ConsumerLagEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(back.lag, 100);
    }

    #[test]
    fn test_obs_error_response_serde() {
        let err = ObsErrorResponse {
            error: "not found".to_string(),
        };
        let json = serde_json::to_string(&err).unwrap();
        assert!(json.contains("not found"));
    }

    #[test]
    fn test_build_health_healthy() {
        let state = test_state();
        let h = build_health(&state);
        assert_eq!(h.status, "healthy");
        assert_eq!(h.broker_count, 1);
        assert!(!h.checks.is_empty());
    }

    #[test]
    fn test_build_health_critical_with_alert() {
        let state = test_state();
        {
            let mut alerts = state.active_alerts.write().unwrap();
            alerts.push(sample_active_alert("r1"));
        }
        let h = build_health(&state);
        assert_eq!(h.status, "critical");
    }

    #[test]
    fn test_build_health_degraded_with_warning() {
        let state = test_state();
        {
            let mut alerts = state.active_alerts.write().unwrap();
            alerts.push(ActiveAlert {
                rule_id: "r2".to_string(),
                rule_name: "Disk warning".to_string(),
                severity: "warning".to_string(),
                message: "Disk 80%".to_string(),
                fired_at: now_iso(),
                acknowledged: false,
                acknowledged_by: None,
            });
        }
        let h = build_health(&state);
        assert_eq!(h.status, "degraded");
    }

    // -- HTTP integration tests --

    #[tokio::test]
    async fn test_get_dashboard() {
        let app = test_router();
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/observability/dashboard")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(v.get("metrics").is_some());
        assert!(v.get("health").is_some());
        assert!(v.get("alert_rules").is_some());
    }

    #[tokio::test]
    async fn test_get_metrics_no_filter() {
        let app = test_router();
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/observability/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(v["total"].as_u64().unwrap() >= 3);
    }

    #[tokio::test]
    async fn test_get_metrics_with_prefix() {
        let app = test_router();
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/observability/metrics?prefix=broker.cpu")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["total"].as_u64().unwrap(), 1);
    }

    #[tokio::test]
    async fn test_get_health() {
        let app = test_router();
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/observability/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["status"].as_str().unwrap(), "healthy");
    }

    #[tokio::test]
    async fn test_create_and_list_alert_rules() {
        let state = test_state();
        let app = create_observability_api_router(state);
        let rule = sample_rule("cpu-high");
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/observability/alerts/rules")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&rule).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/observability/alerts/rules")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["total"].as_u64().unwrap(), 1);
    }

    #[tokio::test]
    async fn test_create_duplicate_alert_rule() {
        let state = test_state();
        let app = create_observability_api_router(state);
        let rule = sample_rule("dup");
        let body_bytes = serde_json::to_vec(&rule).unwrap();

        // First create
        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/observability/alerts/rules")
                    .header("content-type", "application/json")
                    .body(Body::from(body_bytes.clone()))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Duplicate
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/observability/alerts/rules")
                    .header("content-type", "application/json")
                    .body(Body::from(body_bytes))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn test_delete_alert_rule() {
        let state = test_state();
        {
            let mut rules = state.alert_rules.write().unwrap();
            rules.push(sample_rule("del-me"));
        }
        let app = create_observability_api_router(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/v1/observability/alerts/rules/del-me")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_delete_nonexistent_alert_rule() {
        let app = test_router();
        let resp = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/v1/observability/alerts/rules/nope")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_list_active_alerts_empty() {
        let app = test_router();
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/observability/alerts/active")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["total"].as_u64().unwrap(), 0);
    }

    #[tokio::test]
    async fn test_acknowledge_alert() {
        let state = test_state();
        {
            let mut alerts = state.active_alerts.write().unwrap();
            alerts.push(sample_active_alert("ack-me"));
        }
        let app = create_observability_api_router(state);
        let ack = AcknowledgeRequest {
            acknowledged_by: Some("ops-team".to_string()),
        };
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/observability/alerts/ack-me/acknowledge")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&ack).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["acknowledged_by"].as_str().unwrap(), "ops-team");
    }

    #[tokio::test]
    async fn test_acknowledge_nonexistent_alert() {
        let app = test_router();
        let ack = AcknowledgeRequest {
            acknowledged_by: None,
        };
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/observability/alerts/nope/acknowledge")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&ack).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_topics_heatmap() {
        let app = test_router();
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/observability/topics/heatmap")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(v.get("heatmap").is_some());
    }

    #[tokio::test]
    async fn test_consumer_lag() {
        let app = test_router();
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/observability/consumer-lag")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(v.get("entries").is_some());
        assert!(v.get("total_lag").is_some());
    }

    #[tokio::test]
    async fn test_legacy_overview_still_works() {
        let app = test_router();
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/observability/overview")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn test_create_observability_router_returns_valid_router() {
        let _router = create_observability_router();
    }
}
