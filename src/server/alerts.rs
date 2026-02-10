//! Alert configuration and evaluation system
//!
//! This module provides alerting capabilities for monitoring Kafka metrics.
//!
//! ## Features
//!
//! - Configurable alert conditions (consumer lag, offline partitions, etc.)
//! - Multiple notification channels (webhook, log)
//! - Alert history tracking
//! - Periodic evaluation of alert conditions

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/// Alert severity level
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AlertSeverity {
    Warning,
    Critical,
}

/// Alert condition types
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AlertCondition {
    /// Consumer lag exceeds threshold
    ConsumerLagExceeds {
        /// Consumer group ID (or "*" for all groups)
        group: String,
        /// Topic name (or "*" for all topics)
        topic: String,
    },
    /// Number of offline partitions exceeds threshold
    OfflinePartitions,
    /// Under-replicated partitions exceed threshold
    UnderReplicatedPartitions,
    /// Messages per second exceeds threshold
    MessageRateExceeds,
    /// Bytes per second exceeds threshold
    ByteRateExceeds,
}

impl AlertCondition {
    /// Get a human-readable description of the condition
    pub fn description(&self) -> String {
        match self {
            AlertCondition::ConsumerLagExceeds { group, topic } => {
                format!("Consumer lag for group '{}' on topic '{}'", group, topic)
            }
            AlertCondition::OfflinePartitions => "Offline partitions count".to_string(),
            AlertCondition::UnderReplicatedPartitions => {
                "Under-replicated partitions count".to_string()
            }
            AlertCondition::MessageRateExceeds => "Message throughput rate".to_string(),
            AlertCondition::ByteRateExceeds => "Byte throughput rate".to_string(),
        }
    }
}

/// Notification channel configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum NotificationChannel {
    /// Log notification to server logs
    Log,
    /// Send webhook POST request
    Webhook {
        /// Webhook URL
        url: String,
        /// Optional custom headers
        #[serde(default)]
        headers: HashMap<String, String>,
    },
}

/// Alert configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertConfig {
    /// Unique alert ID
    pub id: String,
    /// Alert name
    pub name: String,
    /// Alert description
    #[serde(default)]
    pub description: String,
    /// Alert condition
    pub condition: AlertCondition,
    /// Threshold value
    pub threshold: f64,
    /// Comparison operator (gt, gte, lt, lte, eq)
    #[serde(default = "default_operator")]
    pub operator: String,
    /// Alert severity
    #[serde(default = "default_severity")]
    pub severity: AlertSeverity,
    /// Whether the alert is enabled
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// Notification channels
    #[serde(default)]
    pub channels: Vec<NotificationChannel>,
    /// Evaluation interval in seconds
    #[serde(default = "default_interval")]
    pub interval_secs: u64,
    /// Created timestamp
    pub created_at: u64,
    /// Last modified timestamp
    pub updated_at: u64,
}

fn default_operator() -> String {
    "gt".to_string()
}

fn default_severity() -> AlertSeverity {
    AlertSeverity::Warning
}

fn default_enabled() -> bool {
    true
}

fn default_interval() -> u64 {
    60 // 1 minute
}

impl AlertConfig {
    /// Create a new alert configuration
    pub fn new(name: String, condition: AlertCondition, threshold: f64) -> Self {
        let now = current_timestamp_ms();
        Self {
            id: Uuid::new_v4().to_string(),
            name,
            description: String::new(),
            condition,
            threshold,
            operator: default_operator(),
            severity: default_severity(),
            enabled: true,
            channels: vec![NotificationChannel::Log],
            interval_secs: default_interval(),
            created_at: now,
            updated_at: now,
        }
    }

    /// Check if a value triggers this alert
    pub fn check(&self, value: f64) -> bool {
        match self.operator.as_str() {
            "gt" => value > self.threshold,
            "gte" => value >= self.threshold,
            "lt" => value < self.threshold,
            "lte" => value <= self.threshold,
            "eq" => (value - self.threshold).abs() < f64::EPSILON,
            _ => value > self.threshold, // Default to gt
        }
    }
}

/// Alert event (triggered alert)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertEvent {
    /// Event ID
    pub id: String,
    /// Alert ID that triggered
    pub alert_id: String,
    /// Alert name
    pub alert_name: String,
    /// Severity
    pub severity: AlertSeverity,
    /// Condition description
    pub condition: String,
    /// Threshold value
    pub threshold: f64,
    /// Actual value that triggered the alert
    pub actual_value: f64,
    /// Event timestamp
    pub timestamp: u64,
    /// Whether notification was sent
    pub notified: bool,
    /// Notification error (if any)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notification_error: Option<String>,
}

impl AlertEvent {
    /// Create a new alert event
    pub fn new(config: &AlertConfig, actual_value: f64) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            alert_id: config.id.clone(),
            alert_name: config.name.clone(),
            severity: config.severity,
            condition: config.condition.description(),
            threshold: config.threshold,
            actual_value,
            timestamp: current_timestamp_ms(),
            notified: false,
            notification_error: None,
        }
    }
}

/// Alert store configuration
#[derive(Debug, Clone)]
pub struct AlertStoreConfig {
    /// Maximum number of alert events to keep in history
    pub max_history_entries: usize,
}

impl Default for AlertStoreConfig {
    fn default() -> Self {
        Self {
            max_history_entries: 1000,
        }
    }
}

/// In-memory alert configuration and history store
pub struct AlertStore {
    /// Alert configurations
    configs: RwLock<HashMap<String, AlertConfig>>,
    /// Alert event history (ring buffer)
    history: RwLock<Vec<AlertEvent>>,
    /// Store configuration
    config: AlertStoreConfig,
}

impl AlertStore {
    /// Create a new alert store with default config
    pub fn new() -> Self {
        Self::with_config(AlertStoreConfig::default())
    }

    /// Create a new alert store with custom config
    pub fn with_config(config: AlertStoreConfig) -> Self {
        Self {
            configs: RwLock::new(HashMap::new()),
            history: RwLock::new(Vec::new()),
            config,
        }
    }

    /// Create as shared Arc
    pub fn new_shared() -> Arc<Self> {
        Arc::new(Self::new())
    }

    /// List all alert configurations
    pub fn list_alerts(&self) -> Vec<AlertConfig> {
        self.configs.read().values().cloned().collect()
    }

    /// Get an alert by ID
    pub fn get_alert(&self, id: &str) -> Option<AlertConfig> {
        self.configs.read().get(id).cloned()
    }

    /// Create a new alert
    pub fn create_alert(&self, config: AlertConfig) -> AlertConfig {
        let mut configs = self.configs.write();
        configs.insert(config.id.clone(), config.clone());
        config
    }

    /// Update an existing alert
    pub fn update_alert(&self, id: &str, mut config: AlertConfig) -> Option<AlertConfig> {
        let mut configs = self.configs.write();
        if configs.contains_key(id) {
            config.id = id.to_string();
            config.updated_at = current_timestamp_ms();
            configs.insert(id.to_string(), config.clone());
            Some(config)
        } else {
            None
        }
    }

    /// Delete an alert
    pub fn delete_alert(&self, id: &str) -> bool {
        self.configs.write().remove(id).is_some()
    }

    /// Toggle alert enabled state
    pub fn toggle_alert(&self, id: &str) -> Option<bool> {
        let mut configs = self.configs.write();
        if let Some(config) = configs.get_mut(id) {
            config.enabled = !config.enabled;
            config.updated_at = current_timestamp_ms();
            Some(config.enabled)
        } else {
            None
        }
    }

    /// Get enabled alerts
    pub fn get_enabled_alerts(&self) -> Vec<AlertConfig> {
        self.configs
            .read()
            .values()
            .filter(|c| c.enabled)
            .cloned()
            .collect()
    }

    /// Record an alert event
    pub fn record_event(&self, event: AlertEvent) {
        let mut history = self.history.write();
        history.push(event);

        // Trim to max size
        while history.len() > self.config.max_history_entries {
            history.remove(0);
        }
    }

    /// Get alert history
    pub fn get_history(&self, limit: usize) -> Vec<AlertEvent> {
        let history = self.history.read();
        history.iter().rev().take(limit).cloned().collect()
    }

    /// Get history for a specific alert
    pub fn get_alert_history(&self, alert_id: &str, limit: usize) -> Vec<AlertEvent> {
        let history = self.history.read();
        history
            .iter()
            .rev()
            .filter(|e| e.alert_id == alert_id)
            .take(limit)
            .cloned()
            .collect()
    }

    /// Clear all history
    pub fn clear_history(&self) {
        self.history.write().clear();
    }

    /// Get alert stats
    pub fn stats(&self) -> AlertStats {
        let configs = self.configs.read();
        let history = self.history.read();

        AlertStats {
            total_alerts: configs.len(),
            enabled_alerts: configs.values().filter(|c| c.enabled).count(),
            total_events: history.len(),
            recent_events: history.iter().rev().take(10).cloned().collect(),
        }
    }
}

impl Default for AlertStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Alert statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertStats {
    /// Total configured alerts
    pub total_alerts: usize,
    /// Number of enabled alerts
    pub enabled_alerts: usize,
    /// Total events in history
    pub total_events: usize,
    /// Recent alert events
    pub recent_events: Vec<AlertEvent>,
}

/// Get current timestamp in milliseconds since Unix epoch
fn current_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_alert_config_new() {
        let alert = AlertConfig::new(
            "Test Alert".to_string(),
            AlertCondition::ConsumerLagExceeds {
                group: "test-group".to_string(),
                topic: "test-topic".to_string(),
            },
            1000.0,
        );

        assert_eq!(alert.name, "Test Alert");
        assert!(alert.enabled);
        assert_eq!(alert.threshold, 1000.0);
    }

    #[test]
    fn test_alert_config_check() {
        let mut alert =
            AlertConfig::new("Test".to_string(), AlertCondition::OfflinePartitions, 100.0);

        // Test gt (default)
        assert!(alert.check(150.0));
        assert!(!alert.check(50.0));
        assert!(!alert.check(100.0));

        // Test gte
        alert.operator = "gte".to_string();
        assert!(alert.check(100.0));
        assert!(alert.check(150.0));
        assert!(!alert.check(50.0));

        // Test lt
        alert.operator = "lt".to_string();
        assert!(alert.check(50.0));
        assert!(!alert.check(100.0));
        assert!(!alert.check(150.0));
    }

    #[test]
    fn test_alert_store_crud() {
        let store = AlertStore::new();

        // Create
        let alert = AlertConfig::new(
            "Test Alert".to_string(),
            AlertCondition::OfflinePartitions,
            5.0,
        );
        let id = alert.id.clone();
        store.create_alert(alert);

        // Read
        let alerts = store.list_alerts();
        assert_eq!(alerts.len(), 1);
        assert!(store.get_alert(&id).is_some());

        // Update
        let mut updated = store.get_alert(&id).unwrap();
        updated.threshold = 10.0;
        store.update_alert(&id, updated);
        assert_eq!(store.get_alert(&id).unwrap().threshold, 10.0);

        // Delete
        assert!(store.delete_alert(&id));
        assert!(store.get_alert(&id).is_none());
    }

    #[test]
    fn test_alert_store_history() {
        let config = AlertStoreConfig {
            max_history_entries: 3,
        };
        let store = AlertStore::with_config(config);

        let alert = AlertConfig::new("Test".to_string(), AlertCondition::OfflinePartitions, 5.0);

        // Add 5 events, should only keep last 3
        for i in 0..5 {
            let mut event = AlertEvent::new(&alert, i as f64);
            event.id = format!("event-{}", i);
            store.record_event(event);
        }

        let history = store.get_history(10);
        assert_eq!(history.len(), 3);
    }

    #[test]
    fn test_alert_toggle() {
        let store = AlertStore::new();
        let alert = AlertConfig::new("Test".to_string(), AlertCondition::OfflinePartitions, 5.0);
        let id = alert.id.clone();
        store.create_alert(alert);

        // Initial state: enabled
        assert!(store.get_alert(&id).unwrap().enabled);

        // Toggle off
        assert_eq!(store.toggle_alert(&id), Some(false));
        assert!(!store.get_alert(&id).unwrap().enabled);

        // Toggle on
        assert_eq!(store.toggle_alert(&id), Some(true));
        assert!(store.get_alert(&id).unwrap().enabled);
    }
}
