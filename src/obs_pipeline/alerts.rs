//! Alerting System
//!
//! Provides alerting rules and notification management.

use super::metrics::MetricsSnapshot;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Alert severity
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Serialize, Deserialize)]
pub enum AlertSeverity {
    /// Informational
    Info,
    /// Warning
    #[default]
    Warning,
    /// Critical
    Critical,
}

/// Alert state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlertState {
    /// Alert is pending (conditions met, waiting for duration)
    Pending,
    /// Alert is firing
    Firing,
    /// Alert has resolved
    Resolved,
}

/// Comparison operator for alert conditions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ComparisonOp {
    /// Greater than
    GreaterThan,
    /// Greater than or equal
    GreaterThanOrEqual,
    /// Less than
    LessThan,
    /// Less than or equal
    LessThanOrEqual,
    /// Equal
    Equal,
    /// Not equal
    NotEqual,
}

impl ComparisonOp {
    /// Evaluate the comparison
    pub fn evaluate(&self, left: f64, right: f64) -> bool {
        match self {
            ComparisonOp::GreaterThan => left > right,
            ComparisonOp::GreaterThanOrEqual => left >= right,
            ComparisonOp::LessThan => left < right,
            ComparisonOp::LessThanOrEqual => left <= right,
            ComparisonOp::Equal => (left - right).abs() < f64::EPSILON,
            ComparisonOp::NotEqual => (left - right).abs() >= f64::EPSILON,
        }
    }
}

/// Alert condition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertCondition {
    /// Metric name
    pub metric: String,
    /// Labels to match (empty = all)
    pub labels: HashMap<String, String>,
    /// Comparison operator
    pub op: ComparisonOp,
    /// Threshold value
    pub threshold: f64,
}

impl AlertCondition {
    /// Create a new condition
    pub fn new(metric: impl Into<String>, op: ComparisonOp, threshold: f64) -> Self {
        Self {
            metric: metric.into(),
            labels: HashMap::new(),
            op,
            threshold,
        }
    }

    /// Add label filter
    pub fn with_label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.insert(key.into(), value.into());
        self
    }

    /// Evaluate condition against metrics snapshot
    pub fn evaluate(&self, snapshot: &MetricsSnapshot) -> Option<bool> {
        // Try counters
        if let Some(counter_values) = snapshot.counters.get(&self.metric) {
            for (labels, value) in counter_values {
                if self.labels_match(labels) {
                    return Some(self.op.evaluate(*value as f64, self.threshold));
                }
            }
        }

        // Try gauges
        if let Some(gauge_values) = snapshot.gauges.get(&self.metric) {
            for (labels, value) in gauge_values {
                if self.labels_match(labels) {
                    return Some(self.op.evaluate(*value, self.threshold));
                }
            }
        }

        // Try histograms (use count)
        if let Some(histogram_values) = snapshot.histograms.get(&self.metric) {
            for (labels, value) in histogram_values {
                if self.labels_match(labels) {
                    return Some(self.op.evaluate(value.count as f64, self.threshold));
                }
            }
        }

        None
    }

    /// Check if labels match
    fn labels_match(&self, labels_str: &str) -> bool {
        if self.labels.is_empty() {
            return true;
        }

        for (key, value) in &self.labels {
            let expected = format!("{}={}", key, value);
            if !labels_str.contains(&expected) {
                return false;
            }
        }
        true
    }
}

/// Alert rule
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertRule {
    /// Rule name
    pub name: String,
    /// Rule description
    pub description: String,
    /// Condition to evaluate
    pub condition: AlertCondition,
    /// Severity
    pub severity: AlertSeverity,
    /// Duration condition must be true before firing (seconds)
    pub for_duration_secs: u64,
    /// Labels to add to alerts
    pub labels: HashMap<String, String>,
    /// Annotations
    pub annotations: HashMap<String, String>,
    /// Is rule enabled?
    pub enabled: bool,
}

impl AlertRule {
    /// Create a new alert rule
    pub fn new(name: impl Into<String>, condition: AlertCondition) -> Self {
        Self {
            name: name.into(),
            description: String::new(),
            condition,
            severity: AlertSeverity::Warning,
            for_duration_secs: 0,
            labels: HashMap::new(),
            annotations: HashMap::new(),
            enabled: true,
        }
    }

    /// Set description
    pub fn with_description(mut self, desc: impl Into<String>) -> Self {
        self.description = desc.into();
        self
    }

    /// Set severity
    pub fn with_severity(mut self, severity: AlertSeverity) -> Self {
        self.severity = severity;
        self
    }

    /// Set for duration
    pub fn for_duration(mut self, secs: u64) -> Self {
        self.for_duration_secs = secs;
        self
    }

    /// Add label
    pub fn with_label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.insert(key.into(), value.into());
        self
    }

    /// Add annotation
    pub fn with_annotation(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.annotations.insert(key.into(), value.into());
        self
    }

    /// Enable or disable
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }
}

/// An active or resolved alert
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    /// Alert name (from rule)
    pub name: String,
    /// Rule that triggered this alert
    pub rule_name: String,
    /// Current state
    pub state: AlertState,
    /// Severity
    pub severity: AlertSeverity,
    /// Metric value that triggered the alert
    pub value: f64,
    /// Threshold value
    pub threshold: f64,
    /// Labels
    pub labels: HashMap<String, String>,
    /// Annotations
    pub annotations: HashMap<String, String>,
    /// Time when alert started pending
    pub pending_at: Option<i64>,
    /// Time when alert started firing
    pub firing_at: Option<i64>,
    /// Time when alert resolved
    pub resolved_at: Option<i64>,
    /// Fingerprint for deduplication
    pub fingerprint: String,
}

impl Alert {
    /// Create a new alert from a rule
    pub fn from_rule(rule: &AlertRule, value: f64) -> Self {
        let fingerprint = format!("{}-{}", rule.name, rule.condition.metric);

        Self {
            name: rule.name.clone(),
            rule_name: rule.name.clone(),
            state: AlertState::Pending,
            severity: rule.severity,
            value,
            threshold: rule.condition.threshold,
            labels: rule.labels.clone(),
            annotations: rule.annotations.clone(),
            pending_at: Some(chrono::Utc::now().timestamp_millis()),
            firing_at: None,
            resolved_at: None,
            fingerprint,
        }
    }

    /// Transition to firing state
    pub fn fire(&mut self) {
        self.state = AlertState::Firing;
        self.firing_at = Some(chrono::Utc::now().timestamp_millis());
    }

    /// Transition to resolved state
    pub fn resolve(&mut self) {
        self.state = AlertState::Resolved;
        self.resolved_at = Some(chrono::Utc::now().timestamp_millis());
    }

    /// Get duration in pending state (ms)
    pub fn pending_duration_ms(&self) -> Option<i64> {
        self.pending_at.map(|t| {
            let now = chrono::Utc::now().timestamp_millis();
            now - t
        })
    }

    /// Is alert firing?
    pub fn is_firing(&self) -> bool {
        self.state == AlertState::Firing
    }
}

/// Notification channel types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NotificationChannel {
    /// Webhook notification
    Webhook {
        url: String,
        headers: HashMap<String, String>,
    },
    /// Email notification
    Email {
        to: Vec<String>,
        smtp_server: String,
    },
    /// Slack notification
    Slack {
        webhook_url: String,
        channel: String,
    },
    /// PagerDuty notification
    PagerDuty {
        integration_key: String,
        severity_mapping: HashMap<String, String>,
    },
    /// Console/log notification
    Console,
}

impl NotificationChannel {
    /// Create a webhook channel
    pub fn webhook(url: impl Into<String>) -> Self {
        NotificationChannel::Webhook {
            url: url.into(),
            headers: HashMap::new(),
        }
    }

    /// Create a Slack channel
    pub fn slack(webhook_url: impl Into<String>, channel: impl Into<String>) -> Self {
        NotificationChannel::Slack {
            webhook_url: webhook_url.into(),
            channel: channel.into(),
        }
    }

    /// Create a console channel
    pub fn console() -> Self {
        NotificationChannel::Console
    }
}

/// Alert manager
pub struct AlertManager {
    rules: Vec<AlertRule>,
    active_alerts: HashMap<String, Alert>,
    notification_channels: Vec<NotificationChannel>,
    alert_history: Vec<Alert>,
    max_history: usize,
}

impl AlertManager {
    /// Create a new alert manager
    pub fn new() -> Self {
        Self {
            rules: Vec::new(),
            active_alerts: HashMap::new(),
            notification_channels: vec![NotificationChannel::Console],
            alert_history: Vec::new(),
            max_history: 1000,
        }
    }

    /// Add an alert rule
    pub fn add_rule(&mut self, rule: AlertRule) {
        self.rules.push(rule);
    }

    /// Remove a rule by name
    pub fn remove_rule(&mut self, name: &str) {
        self.rules.retain(|r| r.name != name);
    }

    /// Add a notification channel
    pub fn add_notification_channel(&mut self, channel: NotificationChannel) {
        self.notification_channels.push(channel);
    }

    /// Evaluate all rules against current metrics
    pub fn evaluate(&mut self, snapshot: &MetricsSnapshot) -> Vec<Alert> {
        let mut new_alerts = Vec::new();
        let mut resolved_alerts = Vec::new();

        // Collect rule data to avoid borrowing conflicts
        let rules_data: Vec<_> = self
            .rules
            .iter()
            .filter(|r| r.enabled)
            .map(|r| {
                (
                    r.name.clone(),
                    r.condition.clone(),
                    r.for_duration_secs,
                    r.severity,
                    r.labels.clone(),
                    r.annotations.clone(),
                )
            })
            .collect();

        for (name, condition, for_duration_secs, severity, labels, annotations) in rules_data {
            let condition_result = condition.evaluate(snapshot);
            let fingerprint = format!("{}-{}", name, condition.metric);

            match condition_result {
                Some(true) => {
                    // Condition is true
                    if let Some(existing) = self.active_alerts.get_mut(&fingerprint) {
                        // Alert already exists
                        if existing.state == AlertState::Pending {
                            // Check if duration has passed
                            if let Some(duration_ms) = existing.pending_duration_ms() {
                                if duration_ms >= (for_duration_secs * 1000) as i64 {
                                    existing.fire();
                                    new_alerts.push(existing.clone());
                                }
                            }
                        }
                    } else {
                        // Create new alert
                        let value = self.get_metric_value(snapshot, &condition);
                        let mut alert = Alert {
                            name: name.clone(),
                            rule_name: name.clone(),
                            state: AlertState::Pending,
                            severity,
                            value,
                            threshold: condition.threshold,
                            labels,
                            annotations,
                            pending_at: Some(chrono::Utc::now().timestamp_millis()),
                            firing_at: None,
                            resolved_at: None,
                            fingerprint: fingerprint.clone(),
                        };

                        if for_duration_secs == 0 {
                            alert.fire();
                            new_alerts.push(alert.clone());
                        }

                        self.active_alerts.insert(fingerprint, alert);
                    }
                }
                Some(false) | None => {
                    // Condition is false or metric not found
                    if let Some(mut alert) = self.active_alerts.remove(&fingerprint) {
                        if alert.is_firing() {
                            alert.resolve();
                            resolved_alerts.push(alert.clone());
                            new_alerts.push(alert);
                        }
                    }
                }
            }
        }

        // Add resolved alerts to history
        for alert in resolved_alerts {
            self.add_to_history(alert);
        }

        // Send notifications for new alerts
        for alert in &new_alerts {
            self.notify(alert);
        }

        new_alerts
    }

    /// Get metric value for condition
    fn get_metric_value(&self, snapshot: &MetricsSnapshot, condition: &AlertCondition) -> f64 {
        if let Some(counter_values) = snapshot.counters.get(&condition.metric) {
            for (labels, value) in counter_values {
                if condition.labels.is_empty() || self.labels_match(labels, &condition.labels) {
                    return *value as f64;
                }
            }
        }

        if let Some(gauge_values) = snapshot.gauges.get(&condition.metric) {
            for (labels, value) in gauge_values {
                if condition.labels.is_empty() || self.labels_match(labels, &condition.labels) {
                    return *value;
                }
            }
        }

        0.0
    }

    /// Check if labels match
    fn labels_match(&self, labels_str: &str, expected: &HashMap<String, String>) -> bool {
        for (key, value) in expected {
            let expected = format!("{}={}", key, value);
            if !labels_str.contains(&expected) {
                return false;
            }
        }
        true
    }

    /// Send notification for alert
    fn notify(&self, alert: &Alert) {
        for channel in &self.notification_channels {
            match channel {
                NotificationChannel::Console => {
                    tracing::info!(
                        "Alert [{}] {} - {} ({} = {} threshold {})",
                        alert.severity as u8,
                        alert.state as u8,
                        alert.name,
                        alert.value,
                        if alert.state == AlertState::Firing {
                            "exceeds"
                        } else {
                            "below"
                        },
                        alert.threshold
                    );
                }
                _ => {
                    // Other channels would send actual notifications
                }
            }
        }
    }

    /// Add alert to history
    fn add_to_history(&mut self, alert: Alert) {
        self.alert_history.push(alert);
        while self.alert_history.len() > self.max_history {
            self.alert_history.remove(0);
        }
    }

    /// Get active alerts
    pub fn active_alerts(&self) -> Vec<Alert> {
        self.active_alerts
            .values()
            .filter(|a| a.is_firing())
            .cloned()
            .collect()
    }

    /// Get all alerts (including pending)
    pub fn all_alerts(&self) -> Vec<Alert> {
        self.active_alerts.values().cloned().collect()
    }

    /// Get alert history
    pub fn history(&self) -> &[Alert] {
        &self.alert_history
    }

    /// Silence an alert
    pub fn silence(&mut self, fingerprint: &str) {
        if let Some(alert) = self.active_alerts.get_mut(fingerprint) {
            alert.resolve();
        }
    }

    /// Get rule count
    pub fn rule_count(&self) -> usize {
        self.rules.len()
    }
}

impl Default for AlertManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_snapshot() -> MetricsSnapshot {
        let mut snapshot = MetricsSnapshot::default();
        snapshot
            .gauges
            .insert("cpu_usage".to_string(), [("".to_string(), 85.0)].into());
        snapshot
            .counters
            .insert("error_count".to_string(), [("".to_string(), 100)].into());
        snapshot
    }

    #[test]
    fn test_comparison_operators() {
        assert!(ComparisonOp::GreaterThan.evaluate(10.0, 5.0));
        assert!(!ComparisonOp::GreaterThan.evaluate(5.0, 10.0));
        assert!(ComparisonOp::LessThan.evaluate(5.0, 10.0));
        assert!(ComparisonOp::Equal.evaluate(5.0, 5.0));
        assert!(ComparisonOp::NotEqual.evaluate(5.0, 10.0));
    }

    #[test]
    fn test_alert_condition() {
        let snapshot = create_test_snapshot();

        let condition = AlertCondition::new("cpu_usage", ComparisonOp::GreaterThan, 80.0);
        assert_eq!(condition.evaluate(&snapshot), Some(true));

        let condition2 = AlertCondition::new("cpu_usage", ComparisonOp::GreaterThan, 90.0);
        assert_eq!(condition2.evaluate(&snapshot), Some(false));
    }

    #[test]
    fn test_alert_rule() {
        let condition = AlertCondition::new("cpu_usage", ComparisonOp::GreaterThan, 80.0);
        let rule = AlertRule::new("HighCPU", condition)
            .with_description("CPU usage is high")
            .with_severity(AlertSeverity::Warning)
            .for_duration(60);

        assert_eq!(rule.name, "HighCPU");
        assert_eq!(rule.severity, AlertSeverity::Warning);
        assert_eq!(rule.for_duration_secs, 60);
    }

    #[test]
    fn test_alert_from_rule() {
        let condition = AlertCondition::new("cpu_usage", ComparisonOp::GreaterThan, 80.0);
        let rule = AlertRule::new("HighCPU", condition);

        let alert = Alert::from_rule(&rule, 85.0);

        assert_eq!(alert.name, "HighCPU");
        assert_eq!(alert.value, 85.0);
        assert_eq!(alert.state, AlertState::Pending);
    }

    #[test]
    fn test_alert_lifecycle() {
        let condition = AlertCondition::new("cpu_usage", ComparisonOp::GreaterThan, 80.0);
        let rule = AlertRule::new("HighCPU", condition);

        let mut alert = Alert::from_rule(&rule, 85.0);
        assert_eq!(alert.state, AlertState::Pending);

        alert.fire();
        assert_eq!(alert.state, AlertState::Firing);
        assert!(alert.firing_at.is_some());

        alert.resolve();
        assert_eq!(alert.state, AlertState::Resolved);
        assert!(alert.resolved_at.is_some());
    }

    #[test]
    fn test_alert_manager_evaluate() {
        let mut manager = AlertManager::new();

        let condition = AlertCondition::new("cpu_usage", ComparisonOp::GreaterThan, 80.0);
        let rule = AlertRule::new("HighCPU", condition).for_duration(0);
        manager.add_rule(rule);

        let snapshot = create_test_snapshot();
        let alerts = manager.evaluate(&snapshot);

        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].name, "HighCPU");
        assert!(alerts[0].is_firing());
    }

    #[test]
    fn test_alert_manager_resolve() {
        let mut manager = AlertManager::new();

        let condition = AlertCondition::new("cpu_usage", ComparisonOp::GreaterThan, 90.0);
        let rule = AlertRule::new("HighCPU", condition).for_duration(0);
        manager.add_rule(rule);

        // First evaluation - condition is false (85 < 90)
        let snapshot = create_test_snapshot();
        let alerts = manager.evaluate(&snapshot);

        assert_eq!(alerts.len(), 0);
    }

    #[test]
    fn test_notification_channel() {
        let webhook = NotificationChannel::webhook("https://example.com/webhook");
        assert!(matches!(webhook, NotificationChannel::Webhook { .. }));

        let slack = NotificationChannel::slack("https://hooks.slack.com/xxx", "#alerts");
        assert!(matches!(slack, NotificationChannel::Slack { .. }));
    }

    #[test]
    fn test_alert_severity_ordering() {
        assert!(AlertSeverity::Critical > AlertSeverity::Warning);
        assert!(AlertSeverity::Warning > AlertSeverity::Info);
    }
}
