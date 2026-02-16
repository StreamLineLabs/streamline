//! Alert rules engine for the analytics dashboard.
//!
//! Provides configurable alerting on streaming metrics with support for
//! threshold-based conditions, cooldowns, and multiple notification channels.
//!
//! # Stability
//!
//! **Stable** -- Breaking changes only in major versions.

use crate::error::{AnalyticsError, Result};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, info, warn};

/// Simple key-value metrics snapshot for alert evaluation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricSnapshot {
    /// Timestamp of the snapshot (epoch millis)
    pub timestamp: i64,
    /// Metric name to value mappings
    pub metrics: HashMap<String, f64>,
}

/// Alert severity levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AlertSeverity {
    Critical,
    Warning,
    Info,
}

impl std::fmt::Display for AlertSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AlertSeverity::Critical => write!(f, "critical"),
            AlertSeverity::Warning => write!(f, "warning"),
            AlertSeverity::Info => write!(f, "info"),
        }
    }
}

/// Conditions that trigger an alert
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AlertCondition {
    /// Metric value exceeds an upper threshold
    ThresholdAbove { metric: String, threshold: f64 },
    /// Metric value drops below a lower threshold
    ThresholdBelow { metric: String, threshold: f64 },
    /// Metric rate of change exceeds percent per minute
    RateOfChange {
        metric: String,
        percent_per_minute: f64,
    },
    /// Consumer group lag exceeds threshold
    ConsumerLagAbove {
        group: String,
        topic: String,
        threshold: u64,
    },
    /// Topic size in bytes exceeds threshold
    TopicSizeAbove { topic: String, bytes_threshold: u64 },
    /// Error rate exceeds per-second threshold
    ErrorRateAbove { threshold_per_second: f64 },
    /// Custom expression evaluated against metrics
    Custom { expression: String },
}

impl AlertCondition {
    /// Build the metric key used for snapshot lookup
    fn metric_key(&self) -> String {
        match self {
            AlertCondition::ThresholdAbove { metric, .. }
            | AlertCondition::ThresholdBelow { metric, .. }
            | AlertCondition::RateOfChange { metric, .. } => metric.clone(),
            AlertCondition::ConsumerLagAbove { group, topic, .. } => {
                format!("consumer_lag.{}.{}", group, topic)
            }
            AlertCondition::TopicSizeAbove { topic, .. } => {
                format!("topic_size.{}", topic)
            }
            AlertCondition::ErrorRateAbove { .. } => "error_rate".to_string(),
            AlertCondition::Custom { expression } => expression.clone(),
        }
    }

    /// Evaluate this condition against a metrics snapshot.
    /// Returns `Some((current_value, threshold))` if the condition is met.
    fn evaluate(&self, metrics: &MetricSnapshot) -> Option<(f64, f64)> {
        let key = self.metric_key();
        let value = metrics.metrics.get(&key).copied()?;

        match self {
            AlertCondition::ThresholdAbove { threshold, .. } => {
                if value > *threshold {
                    Some((value, *threshold))
                } else {
                    None
                }
            }
            AlertCondition::ThresholdBelow { threshold, .. } => {
                if value < *threshold {
                    Some((value, *threshold))
                } else {
                    None
                }
            }
            AlertCondition::RateOfChange {
                percent_per_minute, ..
            } => {
                if value.abs() > *percent_per_minute {
                    Some((value, *percent_per_minute))
                } else {
                    None
                }
            }
            AlertCondition::ConsumerLagAbove { threshold, .. } => {
                let thresh = *threshold as f64;
                if value > thresh {
                    Some((value, thresh))
                } else {
                    None
                }
            }
            AlertCondition::TopicSizeAbove {
                bytes_threshold, ..
            } => {
                let thresh = *bytes_threshold as f64;
                if value > thresh {
                    Some((value, thresh))
                } else {
                    None
                }
            }
            AlertCondition::ErrorRateAbove {
                threshold_per_second,
            } => {
                if value > *threshold_per_second {
                    Some((value, *threshold_per_second))
                } else {
                    None
                }
            }
            AlertCondition::Custom { .. } => {
                // Custom expressions treat value > 0 as triggered
                if value > 0.0 {
                    Some((value, 0.0))
                } else {
                    None
                }
            }
        }
    }
}

/// Notification channel for delivering alerts
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum NotificationChannel {
    /// POST alert payload to a webhook URL
    Webhook { url: String },
    /// Log the alert via tracing
    Log,
    /// Send an email notification
    Email { address: String },
    /// Post to a Slack channel
    Slack {
        webhook_url: String,
        channel: String,
    },
}

/// A configured alert rule
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertRule {
    pub id: String,
    pub name: String,
    pub description: String,
    pub condition: AlertCondition,
    pub severity: AlertSeverity,
    pub enabled: bool,
    /// Minimum seconds between consecutive triggers
    pub cooldown_seconds: u64,
    pub notification_channels: Vec<NotificationChannel>,
    pub created_at: i64,
    pub last_triggered: Option<i64>,
}

/// A fired alert event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertEvent {
    pub rule_id: String,
    pub rule_name: String,
    pub severity: AlertSeverity,
    pub message: String,
    pub metric_value: f64,
    pub threshold: f64,
    pub triggered_at: i64,
    pub resolved_at: Option<i64>,
}

/// Summary statistics for the alert engine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertEngineStats {
    pub total_rules: usize,
    pub enabled_rules: usize,
    pub active_alerts: usize,
    pub total_triggered: usize,
    pub total_resolved: usize,
}

/// Alert rules engine that evaluates conditions against metric snapshots
pub struct AlertEngine {
    rules: RwLock<HashMap<String, AlertRule>>,
    active_alerts: RwLock<HashMap<String, AlertEvent>>,
    history: RwLock<Vec<AlertEvent>>,
    max_history: usize,
}

impl AlertEngine {
    /// Create a new alert engine that retains up to `max_history` past events.
    pub fn new(max_history: usize) -> Self {
        Self {
            rules: RwLock::new(HashMap::new()),
            active_alerts: RwLock::new(HashMap::new()),
            history: RwLock::new(Vec::new()),
            max_history,
        }
    }

    /// Register a new alert rule.
    pub fn add_rule(&self, rule: AlertRule) -> Result<()> {
        let mut rules = self.rules.write();
        if rules.contains_key(&rule.id) {
            return Err(AnalyticsError::Config(format!(
                "Alert rule '{}' already exists",
                rule.id
            )));
        }
        info!(rule_id = %rule.id, name = %rule.name, "Adding alert rule");
        rules.insert(rule.id.clone(), rule);
        Ok(())
    }

    /// Remove an alert rule by id.
    pub fn remove_rule(&self, id: &str) -> Result<()> {
        let mut rules = self.rules.write();
        if rules.remove(id).is_none() {
            return Err(AnalyticsError::Config(format!(
                "Alert rule '{}' not found",
                id
            )));
        }
        self.active_alerts.write().remove(id);
        info!(rule_id = %id, "Removed alert rule");
        Ok(())
    }

    /// Enable a previously disabled rule.
    pub fn enable_rule(&self, id: &str) -> Result<()> {
        let mut rules = self.rules.write();
        let rule = rules
            .get_mut(id)
            .ok_or_else(|| AnalyticsError::Config(format!("Alert rule '{}' not found", id)))?;
        rule.enabled = true;
        debug!(rule_id = %id, "Enabled alert rule");
        Ok(())
    }

    /// Disable a rule so it is skipped during evaluation.
    pub fn disable_rule(&self, id: &str) -> Result<()> {
        let mut rules = self.rules.write();
        let rule = rules
            .get_mut(id)
            .ok_or_else(|| AnalyticsError::Config(format!("Alert rule '{}' not found", id)))?;
        rule.enabled = false;
        debug!(rule_id = %id, "Disabled alert rule");
        Ok(())
    }

    /// Evaluate all enabled rules against the provided metrics snapshot.
    /// Returns newly triggered alert events.
    pub fn evaluate(&self, metrics: &MetricSnapshot) -> Vec<AlertEvent> {
        let mut rules = self.rules.write();
        let mut active = self.active_alerts.write();
        let mut history = self.history.write();
        let mut events = Vec::new();
        let now = metrics.timestamp;

        // Resolve active alerts whose condition is no longer met
        let rule_ids: Vec<String> = active.keys().cloned().collect();
        for rule_id in &rule_ids {
            let still_firing = rules
                .get(rule_id)
                .map(|r| r.enabled && r.condition.evaluate(metrics).is_some())
                .unwrap_or(false);

            if !still_firing {
                if let Some(mut event) = active.remove(rule_id) {
                    event.resolved_at = Some(now);
                    debug!(rule_id = %rule_id, "Alert resolved");
                    Self::push_history(&mut history, event, self.max_history);
                }
            }
        }

        // Evaluate each enabled rule
        for rule in rules.values_mut() {
            if !rule.enabled {
                continue;
            }

            if let Some((value, threshold)) = rule.condition.evaluate(metrics) {
                // Respect cooldown
                if let Some(last) = rule.last_triggered {
                    if (now - last) < (rule.cooldown_seconds as i64) {
                        continue;
                    }
                }

                // Skip if already active
                if active.contains_key(&rule.id) {
                    continue;
                }

                let event = AlertEvent {
                    rule_id: rule.id.clone(),
                    rule_name: rule.name.clone(),
                    severity: rule.severity,
                    message: format!(
                        "[{}] {} - value {:.2} exceeded threshold {:.2}",
                        rule.severity, rule.name, value, threshold
                    ),
                    metric_value: value,
                    threshold,
                    triggered_at: now,
                    resolved_at: None,
                };

                rule.last_triggered = Some(now);

                for channel in &rule.notification_channels {
                    Self::notify(channel, &event);
                }

                active.insert(rule.id.clone(), event.clone());
                events.push(event);
            }
        }

        events
    }

    /// Return a snapshot of currently active (unresolved) alerts.
    pub fn get_active_alerts(&self) -> Vec<AlertEvent> {
        self.active_alerts.read().values().cloned().collect()
    }

    /// Return the most recent `limit` events from history.
    pub fn get_history(&self, limit: usize) -> Vec<AlertEvent> {
        let history = self.history.read();
        history.iter().rev().take(limit).cloned().collect()
    }

    /// Acknowledge and resolve an active alert by rule id.
    pub fn acknowledge(&self, rule_id: &str) -> Result<()> {
        let mut active = self.active_alerts.write();
        let mut event = active.remove(rule_id).ok_or_else(|| {
            AnalyticsError::Config(format!("No active alert for rule '{}'", rule_id))
        })?;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        event.resolved_at = Some(now);

        let mut history = self.history.write();
        Self::push_history(&mut history, event, self.max_history);

        info!(rule_id = %rule_id, "Alert acknowledged");
        Ok(())
    }

    /// Return summary statistics about the engine state.
    pub fn stats(&self) -> AlertEngineStats {
        let rules = self.rules.read();
        let active = self.active_alerts.read();
        let history = self.history.read();

        let enabled_rules = rules.values().filter(|r| r.enabled).count();
        let total_resolved = history.iter().filter(|e| e.resolved_at.is_some()).count();

        AlertEngineStats {
            total_rules: rules.len(),
            enabled_rules,
            active_alerts: active.len(),
            total_triggered: history.len() + active.len(),
            total_resolved,
        }
    }

    fn push_history(history: &mut Vec<AlertEvent>, event: AlertEvent, max: usize) {
        history.push(event);
        if history.len() > max {
            history.remove(0);
        }
    }

    fn notify(channel: &NotificationChannel, event: &AlertEvent) {
        match channel {
            NotificationChannel::Log => {
                warn!(
                    rule_id = %event.rule_id,
                    severity = %event.severity,
                    value = event.metric_value,
                    threshold = event.threshold,
                    "{}",
                    event.message
                );
            }
            NotificationChannel::Webhook { url } => {
                debug!(rule_id = %event.rule_id, url = %url, "Webhook notification queued");
            }
            NotificationChannel::Email { address } => {
                debug!(rule_id = %event.rule_id, address = %address, "Email notification queued");
            }
            NotificationChannel::Slack { channel, .. } => {
                debug!(rule_id = %event.rule_id, channel = %channel, "Slack notification queued");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now_secs() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
    }

    fn make_rule(id: &str, condition: AlertCondition) -> AlertRule {
        AlertRule {
            id: id.to_string(),
            name: format!("Rule {}", id),
            description: "test rule".to_string(),
            condition,
            severity: AlertSeverity::Warning,
            enabled: true,
            cooldown_seconds: 0,
            notification_channels: vec![NotificationChannel::Log],
            created_at: now_secs(),
            last_triggered: None,
        }
    }

    fn snapshot(pairs: &[(&str, f64)]) -> MetricSnapshot {
        MetricSnapshot {
            timestamp: now_secs(),
            metrics: pairs.iter().map(|(k, v)| (k.to_string(), *v)).collect(),
        }
    }

    #[test]
    fn test_threshold_above_triggers() {
        let engine = AlertEngine::new(100);
        let rule = make_rule(
            "cpu-high",
            AlertCondition::ThresholdAbove {
                metric: "cpu".to_string(),
                threshold: 90.0,
            },
        );
        engine.add_rule(rule).unwrap();

        let events = engine.evaluate(&snapshot(&[("cpu", 95.0)]));
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].rule_id, "cpu-high");
        assert!(events[0].metric_value > events[0].threshold);
    }

    #[test]
    fn test_threshold_below_triggers() {
        let engine = AlertEngine::new(100);
        let rule = make_rule(
            "disk-low",
            AlertCondition::ThresholdBelow {
                metric: "disk_free".to_string(),
                threshold: 10.0,
            },
        );
        engine.add_rule(rule).unwrap();

        let events = engine.evaluate(&snapshot(&[("disk_free", 5.0)]));
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].rule_id, "disk-low");
    }

    #[test]
    fn test_cooldown_prevents_retrigger() {
        let engine = AlertEngine::new(100);
        let mut rule = make_rule(
            "mem",
            AlertCondition::ThresholdAbove {
                metric: "mem".to_string(),
                threshold: 80.0,
            },
        );
        rule.cooldown_seconds = 300;
        engine.add_rule(rule).unwrap();

        let ts = now_secs();
        let snap = MetricSnapshot {
            timestamp: ts,
            metrics: [("mem".to_string(), 90.0)].into_iter().collect(),
        };
        let events = engine.evaluate(&snap);
        assert_eq!(events.len(), 1);

        // Acknowledge so the alert is no longer active, then re-evaluate
        engine.acknowledge("mem").unwrap();

        let snap2 = MetricSnapshot {
            timestamp: ts + 10, // within cooldown
            metrics: [("mem".to_string(), 95.0)].into_iter().collect(),
        };
        let events2 = engine.evaluate(&snap2);
        assert!(events2.is_empty(), "Should not re-trigger during cooldown");
    }

    #[test]
    fn test_disable_rule_skips_evaluation() {
        let engine = AlertEngine::new(100);
        let rule = make_rule(
            "r1",
            AlertCondition::ThresholdAbove {
                metric: "x".to_string(),
                threshold: 1.0,
            },
        );
        engine.add_rule(rule).unwrap();
        engine.disable_rule("r1").unwrap();

        let events = engine.evaluate(&snapshot(&[("x", 100.0)]));
        assert!(events.is_empty());

        engine.enable_rule("r1").unwrap();
        let events = engine.evaluate(&snapshot(&[("x", 100.0)]));
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn test_acknowledge_resolves_and_records_history() {
        let engine = AlertEngine::new(100);
        let rule = make_rule(
            "a1",
            AlertCondition::ThresholdAbove {
                metric: "val".to_string(),
                threshold: 0.0,
            },
        );
        engine.add_rule(rule).unwrap();

        engine.evaluate(&snapshot(&[("val", 5.0)]));
        assert_eq!(engine.get_active_alerts().len(), 1);

        engine.acknowledge("a1").unwrap();
        assert!(engine.get_active_alerts().is_empty());

        let hist = engine.get_history(10);
        assert_eq!(hist.len(), 1);
        assert!(hist[0].resolved_at.is_some());
    }

    #[test]
    fn test_stats_reports_correctly() {
        let engine = AlertEngine::new(100);
        let r1 = make_rule(
            "s1",
            AlertCondition::ThresholdAbove {
                metric: "a".to_string(),
                threshold: 0.0,
            },
        );
        let mut r2 = make_rule(
            "s2",
            AlertCondition::ThresholdAbove {
                metric: "b".to_string(),
                threshold: 0.0,
            },
        );
        r2.enabled = false;
        engine.add_rule(r1).unwrap();
        engine.add_rule(r2).unwrap();

        engine.evaluate(&snapshot(&[("a", 1.0), ("b", 1.0)]));

        let stats = engine.stats();
        assert_eq!(stats.total_rules, 2);
        assert_eq!(stats.enabled_rules, 1);
        assert_eq!(stats.active_alerts, 1);
    }

    #[test]
    fn test_remove_rule_clears_active_alert() {
        let engine = AlertEngine::new(100);
        let rule = make_rule(
            "rm",
            AlertCondition::ThresholdAbove {
                metric: "v".to_string(),
                threshold: 0.0,
            },
        );
        engine.add_rule(rule).unwrap();
        engine.evaluate(&snapshot(&[("v", 5.0)]));
        assert_eq!(engine.get_active_alerts().len(), 1);

        engine.remove_rule("rm").unwrap();
        assert!(engine.get_active_alerts().is_empty());
        assert!(engine.remove_rule("rm").is_err());
    }

    #[test]
    fn test_consumer_lag_condition() {
        let engine = AlertEngine::new(100);
        let rule = make_rule(
            "lag",
            AlertCondition::ConsumerLagAbove {
                group: "g1".to_string(),
                topic: "t1".to_string(),
                threshold: 1000,
            },
        );
        engine.add_rule(rule).unwrap();

        let events = engine.evaluate(&snapshot(&[("consumer_lag.g1.t1", 2000.0)]));
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn test_alert_resolves_when_condition_clears() {
        let engine = AlertEngine::new(100);
        let rule = make_rule(
            "res",
            AlertCondition::ThresholdAbove {
                metric: "cpu".to_string(),
                threshold: 80.0,
            },
        );
        engine.add_rule(rule).unwrap();

        engine.evaluate(&snapshot(&[("cpu", 95.0)]));
        assert_eq!(engine.get_active_alerts().len(), 1);

        // Value drops below threshold
        engine.evaluate(&snapshot(&[("cpu", 50.0)]));
        assert!(engine.get_active_alerts().is_empty());

        let hist = engine.get_history(10);
        assert_eq!(hist.len(), 1);
        assert!(hist[0].resolved_at.is_some());
    }
}
