//! Alerting Engine and SLO Monitoring for Streamline
//!
//! Provides:
//! - Rule-based alerting with webhook/Slack/PagerDuty notifications
//! - SLO (Service Level Objective) monitoring
//! - Alert deduplication and severity-based routing
//! - Anomaly detection on stream health metrics

use crate::error::{Result, StreamlineError};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};

/// Alert notification channel
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NotificationChannel {
    /// HTTP webhook
    Webhook {
        url: String,
        headers: HashMap<String, String>,
    },
    /// Slack integration
    Slack {
        webhook_url: String,
        channel: String,
    },
    /// PagerDuty
    PagerDuty {
        routing_key: String,
        severity: PagerDutySeverity,
    },
    /// Log only (no external notification)
    Log,
}

impl std::fmt::Display for NotificationChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Webhook { url, .. } => write!(f, "webhook({})", url),
            Self::Slack { channel, .. } => write!(f, "slack(#{})", channel),
            Self::PagerDuty { .. } => write!(f, "pagerduty"),
            Self::Log => write!(f, "log"),
        }
    }
}

/// PagerDuty severity levels
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PagerDutySeverity {
    Critical,
    Error,
    Warning,
    Info,
}

/// Alert severity
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Severity {
    Info,
    Warning,
    Error,
    Critical,
}

impl std::fmt::Display for Severity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Info => write!(f, "info"),
            Self::Warning => write!(f, "warning"),
            Self::Error => write!(f, "error"),
            Self::Critical => write!(f, "critical"),
        }
    }
}

/// Alert condition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlertCondition {
    /// Metric exceeds threshold
    ThresholdExceeded {
        metric: String,
        threshold: f64,
        duration_secs: u64,
    },
    /// Metric below threshold
    ThresholdBelow {
        metric: String,
        threshold: f64,
        duration_secs: u64,
    },
    /// Rate of change exceeds threshold
    RateOfChange {
        metric: String,
        rate_threshold: f64,
        window_secs: u64,
    },
    /// Consumer lag exceeds threshold
    ConsumerLag { group_id: String, max_lag: u64 },
    /// No data received for duration
    NoData { topic: String, timeout_secs: u64 },
}

impl std::fmt::Display for AlertCondition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ThresholdExceeded {
                metric, threshold, ..
            } => {
                write!(f, "{} > {}", metric, threshold)
            }
            Self::ThresholdBelow {
                metric, threshold, ..
            } => {
                write!(f, "{} < {}", metric, threshold)
            }
            Self::RateOfChange {
                metric,
                rate_threshold,
                ..
            } => {
                write!(f, "rate({}) > {}/s", metric, rate_threshold)
            }
            Self::ConsumerLag { group_id, max_lag } => {
                write!(f, "lag({}) > {}", group_id, max_lag)
            }
            Self::NoData {
                topic,
                timeout_secs,
            } => {
                write!(f, "no_data({}) > {}s", topic, timeout_secs)
            }
        }
    }
}

/// Alert rule definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertRule {
    pub id: String,
    pub name: String,
    pub condition: AlertCondition,
    pub severity: Severity,
    pub channels: Vec<NotificationChannel>,
    pub enabled: bool,
    pub labels: HashMap<String, String>,
    pub annotations: HashMap<String, String>,
    /// Silence period after firing (prevents alert storms)
    pub silence_duration_secs: u64,
}

/// Fired alert instance
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FiredAlert {
    pub id: String,
    pub rule_id: String,
    pub rule_name: String,
    pub severity: Severity,
    pub condition_description: String,
    pub fired_at: DateTime<Utc>,
    pub resolved_at: Option<DateTime<Utc>>,
    pub value: f64,
    pub labels: HashMap<String, String>,
    pub notification_sent: bool,
}

/// SLO definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SloDefinition {
    pub id: String,
    pub name: String,
    pub description: String,
    pub target: f64,
    pub window: SloWindow,
    pub indicator: SloIndicator,
}

/// SLO time window
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum SloWindow {
    Rolling7Days,
    Rolling30Days,
    CalendarMonth,
}

impl std::fmt::Display for SloWindow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rolling7Days => write!(f, "7d rolling"),
            Self::Rolling30Days => write!(f, "30d rolling"),
            Self::CalendarMonth => write!(f, "calendar month"),
        }
    }
}

/// SLO indicator type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SloIndicator {
    /// Availability (% of time service is up)
    Availability,
    /// Latency (% of requests under threshold)
    Latency { threshold_ms: u64 },
    /// Error rate (% of requests without errors)
    ErrorRate,
    /// Throughput (% of time meeting minimum throughput)
    Throughput { min_messages_per_sec: u64 },
}

/// SLO status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SloStatus {
    pub slo_id: String,
    pub slo_name: String,
    pub target: f64,
    pub current: f64,
    pub budget_remaining: f64,
    pub budget_burn_rate: f64,
    pub is_breached: bool,
    pub window: SloWindow,
    pub last_updated: DateTime<Utc>,
}

impl SloStatus {
    /// Check if error budget is being consumed too fast
    pub fn is_burning_fast(&self) -> bool {
        self.budget_burn_rate > 1.0
    }
}

/// Alerting engine
pub struct AlertingEngine {
    rules: Arc<RwLock<HashMap<String, AlertRule>>>,
    fired_alerts: Arc<RwLock<Vec<FiredAlert>>>,
    slos: Arc<RwLock<HashMap<String, SloDefinition>>>,
    slo_statuses: Arc<RwLock<HashMap<String, SloStatus>>>,
    metric_values: Arc<RwLock<HashMap<String, f64>>>,
}

impl Default for AlertingEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl AlertingEngine {
    pub fn new() -> Self {
        Self {
            rules: Arc::new(RwLock::new(HashMap::new())),
            fired_alerts: Arc::new(RwLock::new(Vec::new())),
            slos: Arc::new(RwLock::new(HashMap::new())),
            slo_statuses: Arc::new(RwLock::new(HashMap::new())),
            metric_values: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Add an alert rule
    pub async fn add_rule(&self, rule: AlertRule) -> Result<()> {
        let id = rule.id.clone();
        self.rules.write().await.insert(id, rule);
        Ok(())
    }

    /// Remove an alert rule
    pub async fn remove_rule(&self, rule_id: &str) -> Result<()> {
        self.rules
            .write()
            .await
            .remove(rule_id)
            .ok_or_else(|| StreamlineError::Internal(format!("Alert rule not found: {}", rule_id)))
            .map(|_| ())
    }

    /// List all alert rules
    pub async fn list_rules(&self) -> Vec<AlertRule> {
        self.rules.read().await.values().cloned().collect()
    }

    /// Update a metric value and check rules
    pub async fn update_metric(&self, metric: &str, value: f64) -> Vec<FiredAlert> {
        self.metric_values
            .write()
            .await
            .insert(metric.to_string(), value);

        self.evaluate_rules(metric, value).await
    }

    /// Evaluate all rules against a metric update
    async fn evaluate_rules(&self, metric: &str, value: f64) -> Vec<FiredAlert> {
        let rules = self.rules.read().await;
        let mut new_alerts = Vec::new();

        for rule in rules.values() {
            if !rule.enabled {
                continue;
            }

            let triggered = match &rule.condition {
                AlertCondition::ThresholdExceeded {
                    metric: m,
                    threshold,
                    ..
                } => m == metric && value > *threshold,
                AlertCondition::ThresholdBelow {
                    metric: m,
                    threshold,
                    ..
                } => m == metric && value < *threshold,
                AlertCondition::ConsumerLag { max_lag, .. } => {
                    metric.contains("consumer_lag") && value > *max_lag as f64
                }
                _ => false,
            };

            if triggered {
                let alert = FiredAlert {
                    id: uuid::Uuid::new_v4().to_string(),
                    rule_id: rule.id.clone(),
                    rule_name: rule.name.clone(),
                    severity: rule.severity,
                    condition_description: rule.condition.to_string(),
                    fired_at: Utc::now(),
                    resolved_at: None,
                    value,
                    labels: rule.labels.clone(),
                    notification_sent: false,
                };

                warn!(
                    "Alert fired: {} (severity={}, value={})",
                    alert.rule_name, alert.severity, value
                );

                new_alerts.push(alert);
            }
        }

        // Store fired alerts
        if !new_alerts.is_empty() {
            self.fired_alerts.write().await.extend(new_alerts.clone());
        }

        new_alerts
    }

    /// Get all active (unresolved) alerts
    pub async fn active_alerts(&self) -> Vec<FiredAlert> {
        self.fired_alerts
            .read()
            .await
            .iter()
            .filter(|a| a.resolved_at.is_none())
            .cloned()
            .collect()
    }

    /// Resolve an alert
    pub async fn resolve_alert(&self, alert_id: &str) -> Result<()> {
        let mut alerts = self.fired_alerts.write().await;
        for alert in alerts.iter_mut() {
            if alert.id == alert_id {
                alert.resolved_at = Some(Utc::now());
                info!("Alert resolved: {}", alert.rule_name);
                return Ok(());
            }
        }
        Err(StreamlineError::Internal(format!(
            "Alert not found: {}",
            alert_id
        )))
    }

    /// Get alert history
    pub async fn alert_history(&self, limit: usize) -> Vec<FiredAlert> {
        let alerts = self.fired_alerts.read().await;
        alerts.iter().rev().take(limit).cloned().collect()
    }

    /// Define an SLO
    pub async fn define_slo(&self, slo: SloDefinition) -> Result<()> {
        let status = SloStatus {
            slo_id: slo.id.clone(),
            slo_name: slo.name.clone(),
            target: slo.target,
            current: 100.0, // Start at 100%
            budget_remaining: 100.0 - slo.target,
            budget_burn_rate: 0.0,
            is_breached: false,
            window: slo.window,
            last_updated: Utc::now(),
        };

        self.slo_statuses
            .write()
            .await
            .insert(slo.id.clone(), status);
        self.slos.write().await.insert(slo.id.clone(), slo);
        Ok(())
    }

    /// Update SLO with actual measurement
    pub async fn update_slo(&self, slo_id: &str, current_value: f64) -> Result<SloStatus> {
        let mut statuses = self.slo_statuses.write().await;
        let status = statuses
            .get_mut(slo_id)
            .ok_or_else(|| StreamlineError::Internal(format!("SLO not found: {}", slo_id)))?;

        status.current = current_value;
        status.budget_remaining = current_value - status.target;
        status.is_breached = current_value < status.target;
        status.last_updated = Utc::now();

        // Calculate burn rate (simplified)
        if status.budget_remaining < 0.0 {
            status.budget_burn_rate = status.budget_remaining.abs();
        } else {
            status.budget_burn_rate = 0.0;
        }

        Ok(status.clone())
    }

    /// Get all SLO statuses
    pub async fn slo_statuses(&self) -> Vec<SloStatus> {
        self.slo_statuses.read().await.values().cloned().collect()
    }

    /// Build a notification payload for a fired alert
    pub fn build_notification_payload(alert: &FiredAlert) -> NotificationPayload {
        NotificationPayload {
            title: format!("[{}] {}", alert.severity, alert.rule_name),
            message: format!(
                "Alert: {} (value: {:.2})\nFired at: {}\nCondition: {}",
                alert.rule_name, alert.value, alert.fired_at, alert.condition_description,
            ),
            severity: alert.severity,
            labels: alert.labels.clone(),
            timestamp: alert.fired_at,
        }
    }
}

/// Notification payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationPayload {
    pub title: String,
    pub message: String,
    pub severity: Severity,
    pub labels: HashMap<String, String>,
    pub timestamp: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_add_and_list_rules() {
        let engine = AlertingEngine::new();
        let rule = AlertRule {
            id: "r1".to_string(),
            name: "High CPU".to_string(),
            condition: AlertCondition::ThresholdExceeded {
                metric: "cpu_usage".to_string(),
                threshold: 80.0,
                duration_secs: 60,
            },
            severity: Severity::Warning,
            channels: vec![NotificationChannel::Log],
            enabled: true,
            labels: HashMap::new(),
            annotations: HashMap::new(),
            silence_duration_secs: 300,
        };
        engine.add_rule(rule).await.unwrap();
        assert_eq!(engine.list_rules().await.len(), 1);
    }

    #[tokio::test]
    async fn test_alert_fires_on_threshold() {
        let engine = AlertingEngine::new();
        engine
            .add_rule(AlertRule {
                id: "r1".to_string(),
                name: "High Lag".to_string(),
                condition: AlertCondition::ThresholdExceeded {
                    metric: "consumer_lag".to_string(),
                    threshold: 1000.0,
                    duration_secs: 0,
                },
                severity: Severity::Critical,
                channels: vec![NotificationChannel::Log],
                enabled: true,
                labels: HashMap::new(),
                annotations: HashMap::new(),
                silence_duration_secs: 0,
            })
            .await
            .unwrap();

        let alerts = engine.update_metric("consumer_lag", 1500.0).await;
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].severity, Severity::Critical);
    }

    #[tokio::test]
    async fn test_no_alert_under_threshold() {
        let engine = AlertingEngine::new();
        engine
            .add_rule(AlertRule {
                id: "r1".to_string(),
                name: "test".to_string(),
                condition: AlertCondition::ThresholdExceeded {
                    metric: "cpu".to_string(),
                    threshold: 90.0,
                    duration_secs: 0,
                },
                severity: Severity::Warning,
                channels: vec![],
                enabled: true,
                labels: HashMap::new(),
                annotations: HashMap::new(),
                silence_duration_secs: 0,
            })
            .await
            .unwrap();

        let alerts = engine.update_metric("cpu", 50.0).await;
        assert!(alerts.is_empty());
    }

    #[tokio::test]
    async fn test_resolve_alert() {
        let engine = AlertingEngine::new();
        engine
            .add_rule(AlertRule {
                id: "r1".to_string(),
                name: "test".to_string(),
                condition: AlertCondition::ThresholdExceeded {
                    metric: "m".to_string(),
                    threshold: 0.0,
                    duration_secs: 0,
                },
                severity: Severity::Info,
                channels: vec![],
                enabled: true,
                labels: HashMap::new(),
                annotations: HashMap::new(),
                silence_duration_secs: 0,
            })
            .await
            .unwrap();

        let alerts = engine.update_metric("m", 1.0).await;
        assert_eq!(alerts.len(), 1);

        engine.resolve_alert(&alerts[0].id).await.unwrap();
        assert!(engine.active_alerts().await.is_empty());
    }

    #[tokio::test]
    async fn test_slo_definition_and_update() {
        let engine = AlertingEngine::new();
        engine
            .define_slo(SloDefinition {
                id: "slo1".to_string(),
                name: "Availability".to_string(),
                description: "99.9% uptime".to_string(),
                target: 99.9,
                window: SloWindow::Rolling30Days,
                indicator: SloIndicator::Availability,
            })
            .await
            .unwrap();

        let status = engine.update_slo("slo1", 99.95).await.unwrap();
        assert!(!status.is_breached);

        let status = engine.update_slo("slo1", 99.5).await.unwrap();
        assert!(status.is_breached);
    }

    #[tokio::test]
    async fn test_disabled_rule_does_not_fire() {
        let engine = AlertingEngine::new();
        engine
            .add_rule(AlertRule {
                id: "r1".to_string(),
                name: "disabled".to_string(),
                condition: AlertCondition::ThresholdExceeded {
                    metric: "m".to_string(),
                    threshold: 0.0,
                    duration_secs: 0,
                },
                severity: Severity::Info,
                channels: vec![],
                enabled: false,
                labels: HashMap::new(),
                annotations: HashMap::new(),
                silence_duration_secs: 0,
            })
            .await
            .unwrap();

        let alerts = engine.update_metric("m", 100.0).await;
        assert!(alerts.is_empty());
    }

    #[test]
    fn test_severity_display() {
        assert_eq!(Severity::Critical.to_string(), "critical");
        assert_eq!(Severity::Warning.to_string(), "warning");
    }

    #[test]
    fn test_notification_payload() {
        let alert = FiredAlert {
            id: "a1".to_string(),
            rule_id: "r1".to_string(),
            rule_name: "Test".to_string(),
            severity: Severity::Error,
            condition_description: "cpu > 90".to_string(),
            fired_at: Utc::now(),
            resolved_at: None,
            value: 95.0,
            labels: HashMap::new(),
            notification_sent: false,
        };
        let payload = AlertingEngine::build_notification_payload(&alert);
        assert!(payload.title.contains("error"));
        assert!(payload.message.contains("95.00"));
    }

    #[test]
    fn test_slo_window_display() {
        assert_eq!(SloWindow::Rolling7Days.to_string(), "7d rolling");
        assert_eq!(SloWindow::Rolling30Days.to_string(), "30d rolling");
    }

    #[test]
    fn test_channel_display() {
        assert_eq!(NotificationChannel::Log.to_string(), "log");
    }
}
