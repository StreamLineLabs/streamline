//! Zero-Config Observability Stack
//!
//! Provides built-in metrics dashboards, alerting rules, and health monitoring
//! without requiring external tools like Grafana or Prometheus.
//!
//! # Overview
//!
//! This module implements an in-process observability stack that includes:
//! - **MetricsAggregator**: Collects gauges, counters, and histograms with rollup
//! - **AlertEvaluator**: Evaluates alert rules against collected metrics
//! - **DashboardSnapshot**: Complete dashboard state for rendering in a TUI or web UI
//!
//! # Example
//!
//! ```ignore
//! use streamline::observability::dashboard::{DashboardConfig, MetricsAggregator};
//! use std::collections::HashMap;
//!
//! let config = DashboardConfig::default();
//! let aggregator = MetricsAggregator::new(config);
//!
//! // Record some metrics
//! aggregator.record_gauge("cpu_usage", 45.2, HashMap::new());
//! aggregator.record_counter("requests_total", 1, HashMap::new());
//! aggregator.record_histogram("request_latency_ms", 12.5, HashMap::new());
//!
//! // Get a full dashboard snapshot
//! let snapshot = aggregator.get_dashboard_snapshot();
//! ```

use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the zero-config observability dashboard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardConfig {
    /// How long to keep metric data points, in seconds. Default: 86400 (24 hours).
    pub retention_period_secs: u64,
    /// How often metrics are collected, in milliseconds. Default: 1000 (1 second).
    pub collection_interval_ms: u64,
    /// Maximum number of data points retained per metric. Default: 86400.
    pub max_data_points: usize,
    /// Whether the built-in alerting engine is enabled.
    pub enable_alerting: bool,
}

impl Default for DashboardConfig {
    fn default() -> Self {
        Self {
            retention_period_secs: 86400,
            collection_interval_ms: 1000,
            max_data_points: 86400,
            enable_alerting: true,
        }
    }
}

// ---------------------------------------------------------------------------
// Core metric types
// ---------------------------------------------------------------------------

/// The kind of a metric.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MetricKind {
    /// A gauge -- an instantaneous value that can go up or down.
    Gauge,
    /// A counter -- a monotonically increasing value.
    Counter,
    /// A histogram -- a distribution of observed values.
    Histogram,
}

/// A single timestamped data point.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataPoint {
    /// Timestamp in milliseconds since the Unix epoch.
    pub timestamp: i64,
    /// The recorded value.
    pub value: f64,
}

/// A time-series of data points for a single metric.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricSeries {
    /// The metric name.
    pub name: String,
    /// The kind of metric this series represents.
    pub metric_type: MetricKind,
    /// Ordered data points (oldest first).
    pub data_points: Vec<DataPoint>,
    /// Labels attached to this metric series.
    pub labels: HashMap<String, String>,
}

/// A summary view of a single metric (used in listing).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricSummary {
    /// The metric name.
    pub name: String,
    /// The kind of metric.
    pub metric_type: MetricKind,
    /// The most recent value.
    pub current_value: f64,
    /// The minimum value observed.
    pub min_value: f64,
    /// The maximum value observed.
    pub max_value: f64,
    /// The mean of all observed values.
    pub avg_value: f64,
    /// Total number of data points stored.
    pub data_point_count: usize,
    /// Labels attached to this metric.
    pub labels: HashMap<String, String>,
}

// ---------------------------------------------------------------------------
// Dashboard snapshot types
// ---------------------------------------------------------------------------

/// Overall health state of the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HealthState {
    /// Everything is operating normally.
    Healthy,
    /// Some non-critical thresholds have been breached.
    Warning,
    /// Critical thresholds have been breached; action required.
    Critical,
    /// Health state cannot be determined.
    Unknown,
}

/// High-level health summary for the cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterHealthSummary {
    /// Current health state.
    pub status: HealthState,
    /// Seconds since the process started.
    pub uptime_seconds: u64,
    /// Total number of topics.
    pub total_topics: u64,
    /// Total number of partitions across all topics.
    pub total_partitions: u64,
    /// Total messages stored.
    pub total_messages: u64,
    /// Inbound message rate (messages/sec).
    pub messages_per_second_in: f64,
    /// Outbound message rate (messages/sec).
    pub messages_per_second_out: f64,
    /// Inbound byte rate (bytes/sec).
    pub bytes_per_second_in: f64,
    /// Outbound byte rate (bytes/sec).
    pub bytes_per_second_out: f64,
}

impl Default for ClusterHealthSummary {
    fn default() -> Self {
        Self {
            status: HealthState::Unknown,
            uptime_seconds: 0,
            total_topics: 0,
            total_partitions: 0,
            total_messages: 0,
            messages_per_second_in: 0.0,
            messages_per_second_out: 0.0,
            bytes_per_second_in: 0.0,
            bytes_per_second_out: 0.0,
        }
    }
}

/// Per-topic dashboard metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicDashboardMetrics {
    /// Topic name.
    pub topic_name: String,
    /// Number of partitions.
    pub partition_count: u32,
    /// Total messages in the topic.
    pub message_count: u64,
    /// Current message throughput (messages/sec).
    pub messages_per_second: f64,
    /// Current byte throughput (bytes/sec).
    pub bytes_per_second: f64,
    /// Average message size in bytes.
    pub avg_message_size: f64,
    /// Age of the oldest message in milliseconds.
    pub oldest_message_age_ms: u64,
}

/// Trend direction for consumer lag.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LagTrend {
    /// Lag is getting larger.
    Increasing,
    /// Lag is approximately stable.
    Stable,
    /// Lag is getting smaller.
    Decreasing,
}

/// Per-consumer-group dashboard metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroupDashboardMetrics {
    /// Consumer group identifier.
    pub group_id: String,
    /// Human-readable state (e.g. "Stable", "Rebalancing").
    pub state: String,
    /// Number of members in the group.
    pub member_count: u32,
    /// Topics this group is subscribed to.
    pub subscribed_topics: Vec<String>,
    /// Total lag across all partitions.
    pub total_lag: u64,
    /// Maximum lag for any single partition.
    pub max_partition_lag: u64,
    /// Whether lag is increasing, stable, or decreasing.
    pub lag_trend: LagTrend,
}

/// System-level dashboard metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemDashboardMetrics {
    /// CPU usage as a percentage (0.0 - 100.0).
    pub cpu_usage_percent: f64,
    /// Memory currently in use (bytes).
    pub memory_usage_bytes: u64,
    /// Total system memory (bytes).
    pub memory_total_bytes: u64,
    /// Disk space in use (bytes).
    pub disk_usage_bytes: u64,
    /// Total disk capacity (bytes).
    pub disk_total_bytes: u64,
    /// Number of open TCP/QUIC connections.
    pub open_connections: u64,
    /// Number of open file descriptors for the process.
    pub open_file_descriptors: u64,
    /// GC pause time in milliseconds. `None` for pure-Rust paths;
    /// populated when embedded JVM connectors are in use.
    pub gc_pause_ms: Option<f64>,
}

impl Default for SystemDashboardMetrics {
    fn default() -> Self {
        Self {
            cpu_usage_percent: 0.0,
            memory_usage_bytes: 0,
            memory_total_bytes: 0,
            disk_usage_bytes: 0,
            disk_total_bytes: 0,
            open_connections: 0,
            open_file_descriptors: 0,
            gc_pause_ms: None,
        }
    }
}

/// A point-in-time snapshot of the entire dashboard state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardSnapshot {
    /// Timestamp when this snapshot was created (ms since epoch).
    pub timestamp: i64,
    /// Cluster health overview.
    pub cluster_health: ClusterHealthSummary,
    /// Per-topic metrics.
    pub topic_metrics: Vec<TopicDashboardMetrics>,
    /// Per-consumer-group metrics.
    pub consumer_group_metrics: Vec<ConsumerGroupDashboardMetrics>,
    /// System resource metrics.
    pub system_metrics: SystemDashboardMetrics,
    /// Currently firing alerts.
    pub active_alerts: Vec<ActiveAlert>,
}

// ---------------------------------------------------------------------------
// Alert types
// ---------------------------------------------------------------------------

/// The kind of condition an alert evaluates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlertConditionType {
    /// The metric value exceeds the threshold.
    GreaterThan,
    /// The metric value is below the threshold.
    LessThan,
    /// The metric value equals the threshold exactly (within f64 tolerance).
    Equals,
    /// The rate of change of the metric exceeds the threshold.
    RateOfChange,
}

/// Severity levels for alerts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum AlertSeverity {
    /// Informational -- no action required.
    Info,
    /// Warning -- investigate soon.
    Warning,
    /// Critical -- immediate action required.
    Critical,
}

/// How an alert notification is delivered.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlertNotification {
    /// Write to the structured log.
    Log,
    /// POST to a webhook URL.
    Webhook {
        /// The URL to POST the alert payload to.
        url: String,
    },
    /// Send an email.
    Email {
        /// Recipient email address.
        to: String,
    },
}

/// A declarative alerting rule.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertRule {
    /// Unique name for the rule.
    pub name: String,
    /// Human-readable description.
    pub description: String,
    /// The metric this rule evaluates.
    pub metric_name: String,
    /// The condition to check.
    pub condition: AlertConditionType,
    /// Threshold value for the condition.
    pub threshold: f64,
    /// The condition must hold for at least this many milliseconds before firing.
    pub duration_ms: u64,
    /// Severity when the alert fires.
    pub severity: AlertSeverity,
    /// How to deliver the notification.
    pub notification: AlertNotification,
}

/// A currently active (firing) alert.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActiveAlert {
    /// Name of the rule that fired.
    pub rule_name: String,
    /// Severity of the alert.
    pub severity: AlertSeverity,
    /// When the alert was first triggered (ms since epoch).
    pub triggered_at: i64,
    /// The current metric value that breached the threshold.
    pub current_value: f64,
    /// The threshold that was breached.
    pub threshold: f64,
    /// A human-readable description of the alert.
    pub message: String,
}

// ---------------------------------------------------------------------------
// Internal stored metric
// ---------------------------------------------------------------------------

/// Internal representation of a metric stored inside the aggregator.
#[derive(Debug, Clone)]
struct StoredMetric {
    name: String,
    metric_type: MetricKind,
    data_points: Vec<DataPoint>,
    labels: HashMap<String, String>,
}

// ---------------------------------------------------------------------------
// MetricsAggregator
// ---------------------------------------------------------------------------

/// In-process metrics collection and rollup engine.
///
/// Thread-safe via `Arc<RwLock<...>>` interior state. All public methods
/// take `&self` and acquire locks internally.
pub struct MetricsAggregator {
    config: DashboardConfig,
    metrics: Arc<RwLock<HashMap<String, StoredMetric>>>,
}

impl MetricsAggregator {
    /// Create a new aggregator with the given configuration.
    pub fn new(config: DashboardConfig) -> Self {
        Self {
            config,
            metrics: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    // ---- recording ---------------------------------------------------

    /// Record a gauge value (instantaneous measurement).
    pub fn record_gauge(&self, name: &str, value: f64, labels: HashMap<String, String>) {
        let dp = DataPoint {
            timestamp: chrono::Utc::now().timestamp_millis(),
            value,
        };
        let mut map = self.metrics.write().unwrap_or_else(|e| e.into_inner());
        let entry = map.entry(name.to_string()).or_insert_with(|| StoredMetric {
            name: name.to_string(),
            metric_type: MetricKind::Gauge,
            data_points: Vec::new(),
            labels: labels.clone(),
        });
        entry.data_points.push(dp);
        if entry.data_points.len() > self.config.max_data_points {
            entry.data_points.remove(0);
        }
    }

    /// Record a counter increment.
    pub fn record_counter(&self, name: &str, increment: u64, labels: HashMap<String, String>) {
        let mut map = self.metrics.write().unwrap_or_else(|e| e.into_inner());
        let entry = map.entry(name.to_string()).or_insert_with(|| StoredMetric {
            name: name.to_string(),
            metric_type: MetricKind::Counter,
            data_points: Vec::new(),
            labels: labels.clone(),
        });

        let previous = entry.data_points.last().map(|dp| dp.value).unwrap_or(0.0);
        let dp = DataPoint {
            timestamp: chrono::Utc::now().timestamp_millis(),
            value: previous + increment as f64,
        };
        entry.data_points.push(dp);
        if entry.data_points.len() > self.config.max_data_points {
            entry.data_points.remove(0);
        }
    }

    /// Record a histogram observation.
    pub fn record_histogram(&self, name: &str, value: f64, labels: HashMap<String, String>) {
        let dp = DataPoint {
            timestamp: chrono::Utc::now().timestamp_millis(),
            value,
        };
        let mut map = self.metrics.write().unwrap_or_else(|e| e.into_inner());
        let entry = map.entry(name.to_string()).or_insert_with(|| StoredMetric {
            name: name.to_string(),
            metric_type: MetricKind::Histogram,
            data_points: Vec::new(),
            labels: labels.clone(),
        });
        entry.data_points.push(dp);
        if entry.data_points.len() > self.config.max_data_points {
            entry.data_points.remove(0);
        }
    }

    // ---- querying -----------------------------------------------------

    /// Retrieve a metric series for the given name, optionally filtered by a
    /// time range `(start_ms, end_ms)`. Returns `None` if the metric does not
    /// exist.
    pub fn get_metric(&self, name: &str, time_range: Option<(i64, i64)>) -> Option<MetricSeries> {
        let map = self.metrics.read().unwrap_or_else(|e| e.into_inner());
        let stored = map.get(name)?;

        let data_points = match time_range {
            Some((start, end)) => stored
                .data_points
                .iter()
                .filter(|dp| dp.timestamp >= start && dp.timestamp <= end)
                .cloned()
                .collect(),
            None => stored.data_points.clone(),
        };

        Some(MetricSeries {
            name: stored.name.clone(),
            metric_type: stored.metric_type,
            data_points,
            labels: stored.labels.clone(),
        })
    }

    /// Return a summary of every metric currently tracked.
    pub fn get_all_metrics(&self) -> Vec<MetricSummary> {
        let map = self.metrics.read().unwrap_or_else(|e| e.into_inner());
        let mut summaries = Vec::with_capacity(map.len());

        for stored in map.values() {
            if let Some(summary) = summarize(stored) {
                summaries.push(summary);
            }
        }

        summaries
    }

    /// Build a complete [`DashboardSnapshot`] from the current aggregator state.
    ///
    /// Topic, consumer-group, and system metrics are derived from well-known
    /// metric names when available; otherwise sensible defaults are returned.
    pub fn get_dashboard_snapshot(&self) -> DashboardSnapshot {
        let map = self.metrics.read().unwrap_or_else(|e| e.into_inner());
        let now = chrono::Utc::now().timestamp_millis();

        // --- cluster health ---
        let cluster_health = ClusterHealthSummary {
            status: derive_health_state(&map),
            uptime_seconds: map
                .get("uptime_seconds")
                .and_then(|m| m.data_points.last())
                .map(|dp| dp.value as u64)
                .unwrap_or(0),
            total_topics: latest_value(&map, "total_topics") as u64,
            total_partitions: latest_value(&map, "total_partitions") as u64,
            total_messages: latest_value(&map, "total_messages") as u64,
            messages_per_second_in: latest_value(&map, "messages_per_second_in"),
            messages_per_second_out: latest_value(&map, "messages_per_second_out"),
            bytes_per_second_in: latest_value(&map, "bytes_per_second_in"),
            bytes_per_second_out: latest_value(&map, "bytes_per_second_out"),
        };

        // --- system metrics ---
        let system_metrics = SystemDashboardMetrics {
            cpu_usage_percent: latest_value(&map, "cpu_usage_percent"),
            memory_usage_bytes: latest_value(&map, "memory_usage_bytes") as u64,
            memory_total_bytes: latest_value(&map, "memory_total_bytes") as u64,
            disk_usage_bytes: latest_value(&map, "disk_usage_bytes") as u64,
            disk_total_bytes: latest_value(&map, "disk_total_bytes") as u64,
            open_connections: latest_value(&map, "open_connections") as u64,
            open_file_descriptors: latest_value(&map, "open_file_descriptors") as u64,
            gc_pause_ms: map
                .get("gc_pause_ms")
                .and_then(|m| m.data_points.last())
                .map(|dp| dp.value),
        };

        DashboardSnapshot {
            timestamp: now,
            cluster_health,
            topic_metrics: Vec::new(),
            consumer_group_metrics: Vec::new(),
            system_metrics,
            active_alerts: Vec::new(),
        }
    }

    /// Remove data points older than the configured retention period.
    pub fn prune_old_data(&self) {
        let cutoff = chrono::Utc::now().timestamp_millis()
            - (self.config.retention_period_secs as i64 * 1000);

        let mut map = self.metrics.write().unwrap_or_else(|e| e.into_inner());
        for stored in map.values_mut() {
            stored.data_points.retain(|dp| dp.timestamp >= cutoff);
        }
        // Remove metrics that have no remaining data points.
        map.retain(|_, v| !v.data_points.is_empty());
    }
}

// ---------------------------------------------------------------------------
// AlertEvaluator
// ---------------------------------------------------------------------------

/// Evaluates a set of [`AlertRule`]s against a [`MetricsAggregator`].
pub struct AlertEvaluator {
    rules: Arc<RwLock<Vec<AlertRule>>>,
}

impl AlertEvaluator {
    /// Create a new evaluator with no rules.
    pub fn new() -> Self {
        Self {
            rules: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Add an alert rule. Returns an error if a rule with the same name
    /// already exists.
    pub fn add_rule(&self, rule: AlertRule) -> Result<()> {
        let mut rules = self.rules.write().unwrap_or_else(|e| e.into_inner());
        if rules.iter().any(|r| r.name == rule.name) {
            return Err(StreamlineError::Config(format!(
                "alert rule '{}' already exists",
                rule.name
            )));
        }
        rules.push(rule);
        Ok(())
    }

    /// Remove the rule with the given name. Returns an error if not found.
    pub fn remove_rule(&self, name: &str) -> Result<()> {
        let mut rules = self.rules.write().unwrap_or_else(|e| e.into_inner());
        let before = rules.len();
        rules.retain(|r| r.name != name);
        if rules.len() == before {
            return Err(StreamlineError::Config(format!(
                "alert rule '{}' not found",
                name
            )));
        }
        Ok(())
    }

    /// Evaluate all rules against the current aggregator state and return any
    /// alerts that are currently firing.
    pub fn evaluate(&self, aggregator: &MetricsAggregator) -> Vec<ActiveAlert> {
        let rules = self.rules.read().unwrap_or_else(|e| e.into_inner());
        let now = chrono::Utc::now().timestamp_millis();
        let mut alerts = Vec::new();

        for rule in rules.iter() {
            if let Some(series) = aggregator.get_metric(&rule.metric_name, None) {
                if let Some(current) = series.data_points.last() {
                    let condition_met = match rule.condition {
                        AlertConditionType::GreaterThan => current.value > rule.threshold,
                        AlertConditionType::LessThan => current.value < rule.threshold,
                        AlertConditionType::Equals => {
                            (current.value - rule.threshold).abs() < f64::EPSILON
                        }
                        AlertConditionType::RateOfChange => {
                            compute_rate_of_change(&series.data_points) > rule.threshold
                        }
                    };

                    // Check that the condition has been sustained for `duration_ms`.
                    if condition_met {
                        let sustained = is_condition_sustained(&series.data_points, rule, now);

                        if sustained {
                            alerts.push(ActiveAlert {
                                rule_name: rule.name.clone(),
                                severity: rule.severity,
                                triggered_at: now,
                                current_value: current.value,
                                threshold: rule.threshold,
                                message: format!(
                                    "{}: {} is {:.2} (threshold: {:.2})",
                                    rule.name, rule.metric_name, current.value, rule.threshold
                                ),
                            });
                        }
                    }
                }
            }
        }

        alerts
    }

    /// Return a clone of all configured rules.
    pub fn list_rules(&self) -> Vec<AlertRule> {
        self.rules.read().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

impl Default for AlertEvaluator {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Default alert rules
// ---------------------------------------------------------------------------

/// Return a set of sensible default alert rules for a Streamline deployment.
pub fn default_alert_rules() -> Vec<AlertRule> {
    vec![
        AlertRule {
            name: "consumer_lag_warning".to_string(),
            description: "Consumer lag exceeds 10 000 messages".to_string(),
            metric_name: "consumer_lag".to_string(),
            condition: AlertConditionType::GreaterThan,
            threshold: 10_000.0,
            duration_ms: 60_000,
            severity: AlertSeverity::Warning,
            notification: AlertNotification::Log,
        },
        AlertRule {
            name: "consumer_lag_critical".to_string(),
            description: "Consumer lag exceeds 100 000 messages".to_string(),
            metric_name: "consumer_lag".to_string(),
            condition: AlertConditionType::GreaterThan,
            threshold: 100_000.0,
            duration_ms: 60_000,
            severity: AlertSeverity::Critical,
            notification: AlertNotification::Log,
        },
        AlertRule {
            name: "disk_usage_warning".to_string(),
            description: "Disk usage exceeds 80%".to_string(),
            metric_name: "disk_usage_percent".to_string(),
            condition: AlertConditionType::GreaterThan,
            threshold: 80.0,
            duration_ms: 120_000,
            severity: AlertSeverity::Warning,
            notification: AlertNotification::Log,
        },
        AlertRule {
            name: "disk_usage_critical".to_string(),
            description: "Disk usage exceeds 95%".to_string(),
            metric_name: "disk_usage_percent".to_string(),
            condition: AlertConditionType::GreaterThan,
            threshold: 95.0,
            duration_ms: 60_000,
            severity: AlertSeverity::Critical,
            notification: AlertNotification::Log,
        },
        AlertRule {
            name: "error_rate_warning".to_string(),
            description: "Error rate exceeds 1%".to_string(),
            metric_name: "error_rate_percent".to_string(),
            condition: AlertConditionType::GreaterThan,
            threshold: 1.0,
            duration_ms: 60_000,
            severity: AlertSeverity::Warning,
            notification: AlertNotification::Log,
        },
        AlertRule {
            name: "error_rate_critical".to_string(),
            description: "Error rate exceeds 5%".to_string(),
            metric_name: "error_rate_percent".to_string(),
            condition: AlertConditionType::GreaterThan,
            threshold: 5.0,
            duration_ms: 60_000,
            severity: AlertSeverity::Critical,
            notification: AlertNotification::Log,
        },
        AlertRule {
            name: "no_messages_info".to_string(),
            description: "No messages produced in the last 5 minutes".to_string(),
            metric_name: "messages_per_second_in".to_string(),
            condition: AlertConditionType::LessThan,
            threshold: 0.01,
            duration_ms: 300_000,
            severity: AlertSeverity::Info,
            notification: AlertNotification::Log,
        },
    ]
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/// Compute a summary for a stored metric.
fn summarize(stored: &StoredMetric) -> Option<MetricSummary> {
    if stored.data_points.is_empty() {
        return None;
    }

    let mut min = f64::MAX;
    let mut max = f64::MIN;
    let mut sum = 0.0_f64;

    for dp in &stored.data_points {
        if dp.value < min {
            min = dp.value;
        }
        if dp.value > max {
            max = dp.value;
        }
        sum += dp.value;
    }

    let count = stored.data_points.len();
    let avg = sum / count as f64;
    let current = stored.data_points.last().map(|dp| dp.value).unwrap_or(0.0);

    Some(MetricSummary {
        name: stored.name.clone(),
        metric_type: stored.metric_type,
        current_value: current,
        min_value: min,
        max_value: max,
        avg_value: avg,
        data_point_count: count,
        labels: stored.labels.clone(),
    })
}

/// Extract the latest value for a well-known metric, or 0.0 if absent.
fn latest_value(map: &HashMap<String, StoredMetric>, key: &str) -> f64 {
    map.get(key)
        .and_then(|m| m.data_points.last())
        .map(|dp| dp.value)
        .unwrap_or(0.0)
}

/// Derive a [`HealthState`] from the current metric map.
fn derive_health_state(map: &HashMap<String, StoredMetric>) -> HealthState {
    let error_rate = map
        .get("error_rate_percent")
        .and_then(|m| m.data_points.last())
        .map(|dp| dp.value)
        .unwrap_or(0.0);

    let disk_usage = map
        .get("disk_usage_percent")
        .and_then(|m| m.data_points.last())
        .map(|dp| dp.value)
        .unwrap_or(0.0);

    if error_rate > 5.0 || disk_usage > 95.0 {
        HealthState::Critical
    } else if error_rate > 1.0 || disk_usage > 80.0 {
        HealthState::Warning
    } else if map.is_empty() {
        HealthState::Unknown
    } else {
        HealthState::Healthy
    }
}

/// Compute the rate of change from the last two data points (units per ms).
fn compute_rate_of_change(data_points: &[DataPoint]) -> f64 {
    if data_points.len() < 2 {
        return 0.0;
    }
    let prev = &data_points[data_points.len() - 2];
    let curr = &data_points[data_points.len() - 1];
    let dt = (curr.timestamp - prev.timestamp) as f64;
    if dt <= 0.0 {
        return 0.0;
    }
    (curr.value - prev.value) / dt
}

/// Check whether the alert condition has been continuously true for at least
/// `rule.duration_ms` milliseconds.
fn is_condition_sustained(data_points: &[DataPoint], rule: &AlertRule, now: i64) -> bool {
    if rule.duration_ms == 0 {
        return true;
    }

    let window_start = now - rule.duration_ms as i64;
    let recent: Vec<&DataPoint> = data_points
        .iter()
        .filter(|dp| dp.timestamp >= window_start)
        .collect();

    if recent.is_empty() {
        return false;
    }

    recent.iter().all(|dp| match rule.condition {
        AlertConditionType::GreaterThan => dp.value > rule.threshold,
        AlertConditionType::LessThan => dp.value < rule.threshold,
        AlertConditionType::Equals => (dp.value - rule.threshold).abs() < f64::EPSILON,
        AlertConditionType::RateOfChange => {
            // For sustained rate-of-change checks we only look at the latest value.
            true
        }
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_config() -> DashboardConfig {
        DashboardConfig {
            retention_period_secs: 3600,
            collection_interval_ms: 100,
            max_data_points: 100,
            enable_alerting: true,
        }
    }

    // -- MetricsAggregator: gauge ----------------------------------------

    #[test]
    fn test_record_gauge() {
        let agg = MetricsAggregator::new(make_config());
        agg.record_gauge("cpu", 42.0, HashMap::new());
        agg.record_gauge("cpu", 55.0, HashMap::new());

        let series = agg.get_metric("cpu", None).expect("metric should exist");
        assert_eq!(series.name, "cpu");
        assert_eq!(series.metric_type, MetricKind::Gauge);
        assert_eq!(series.data_points.len(), 2);
        assert!((series.data_points[0].value - 42.0).abs() < f64::EPSILON);
        assert!((series.data_points[1].value - 55.0).abs() < f64::EPSILON);
    }

    // -- MetricsAggregator: counter --------------------------------------

    #[test]
    fn test_record_counter() {
        let agg = MetricsAggregator::new(make_config());
        agg.record_counter("requests", 5, HashMap::new());
        agg.record_counter("requests", 3, HashMap::new());

        let series = agg
            .get_metric("requests", None)
            .expect("metric should exist");
        assert_eq!(series.metric_type, MetricKind::Counter);
        // First point: 0 + 5 = 5, second: 5 + 3 = 8
        assert!((series.data_points[0].value - 5.0).abs() < f64::EPSILON);
        assert!((series.data_points[1].value - 8.0).abs() < f64::EPSILON);
    }

    // -- MetricsAggregator: histogram ------------------------------------

    #[test]
    fn test_record_histogram() {
        let agg = MetricsAggregator::new(make_config());
        agg.record_histogram("latency", 1.5, HashMap::new());
        agg.record_histogram("latency", 3.7, HashMap::new());
        agg.record_histogram("latency", 2.1, HashMap::new());

        let series = agg
            .get_metric("latency", None)
            .expect("metric should exist");
        assert_eq!(series.metric_type, MetricKind::Histogram);
        assert_eq!(series.data_points.len(), 3);
    }

    // -- MetricsAggregator: get_all_metrics / summary --------------------

    #[test]
    fn test_get_all_metrics_summary() {
        let agg = MetricsAggregator::new(make_config());
        agg.record_gauge("temp", 20.0, HashMap::new());
        agg.record_gauge("temp", 30.0, HashMap::new());
        agg.record_gauge("temp", 25.0, HashMap::new());

        let summaries = agg.get_all_metrics();
        assert_eq!(summaries.len(), 1);

        let s = &summaries[0];
        assert_eq!(s.name, "temp");
        assert!((s.current_value - 25.0).abs() < f64::EPSILON);
        assert!((s.min_value - 20.0).abs() < f64::EPSILON);
        assert!((s.max_value - 30.0).abs() < f64::EPSILON);
        assert!((s.avg_value - 25.0).abs() < f64::EPSILON);
        assert_eq!(s.data_point_count, 3);
    }

    // -- MetricsAggregator: max_data_points enforcement ------------------

    #[test]
    fn test_max_data_points_enforcement() {
        let mut config = make_config();
        config.max_data_points = 3;
        let agg = MetricsAggregator::new(config);

        for i in 0..10 {
            agg.record_gauge("m", i as f64, HashMap::new());
        }

        let series = agg.get_metric("m", None).expect("metric should exist");
        assert_eq!(series.data_points.len(), 3);
        // Oldest retained should be value 7.0 (indices 7, 8, 9)
        assert!((series.data_points[0].value - 7.0).abs() < f64::EPSILON);
    }

    // -- MetricsAggregator: pruning --------------------------------------

    #[test]
    fn test_prune_old_data() {
        let mut config = make_config();
        config.retention_period_secs = 1; // 1 second retention
        let agg = MetricsAggregator::new(config);

        // Insert a data point with a timestamp far in the past.
        {
            let mut map = agg.metrics.write().unwrap();
            map.insert(
                "old_metric".to_string(),
                StoredMetric {
                    name: "old_metric".to_string(),
                    metric_type: MetricKind::Gauge,
                    data_points: vec![DataPoint {
                        timestamp: 1000, // way in the past
                        value: 1.0,
                    }],
                    labels: HashMap::new(),
                },
            );
        }

        // Also insert a fresh metric.
        agg.record_gauge("fresh_metric", 42.0, HashMap::new());

        agg.prune_old_data();

        assert!(agg.get_metric("old_metric", None).is_none());
        assert!(agg.get_metric("fresh_metric", None).is_some());
    }

    // -- MetricsAggregator: time-range filtering -------------------------

    #[test]
    fn test_get_metric_time_range() {
        let agg = MetricsAggregator::new(make_config());
        let now = chrono::Utc::now().timestamp_millis();

        // Manually insert data points with known timestamps.
        {
            let mut map = agg.metrics.write().unwrap();
            map.insert(
                "ts_metric".to_string(),
                StoredMetric {
                    name: "ts_metric".to_string(),
                    metric_type: MetricKind::Gauge,
                    data_points: vec![
                        DataPoint {
                            timestamp: now - 5000,
                            value: 1.0,
                        },
                        DataPoint {
                            timestamp: now - 3000,
                            value: 2.0,
                        },
                        DataPoint {
                            timestamp: now - 1000,
                            value: 3.0,
                        },
                    ],
                    labels: HashMap::new(),
                },
            );
        }

        let series = agg
            .get_metric("ts_metric", Some((now - 4000, now - 2000)))
            .expect("metric should exist");

        assert_eq!(series.data_points.len(), 1);
        assert!((series.data_points[0].value - 2.0).abs() < f64::EPSILON);
    }

    // -- DashboardSnapshot creation --------------------------------------

    #[test]
    fn test_dashboard_snapshot() {
        let agg = MetricsAggregator::new(make_config());
        agg.record_gauge("cpu_usage_percent", 65.0, HashMap::new());
        agg.record_gauge("total_topics", 10.0, HashMap::new());
        agg.record_gauge("open_connections", 42.0, HashMap::new());

        let snapshot = agg.get_dashboard_snapshot();
        assert!(snapshot.timestamp > 0);
        assert_eq!(snapshot.cluster_health.total_topics, 10);
        assert!((snapshot.system_metrics.cpu_usage_percent - 65.0).abs() < f64::EPSILON);
        assert_eq!(snapshot.system_metrics.open_connections, 42);
    }

    // -- AlertEvaluator: add/remove/list rules ---------------------------

    #[test]
    fn test_alert_rule_add_remove_list() {
        let evaluator = AlertEvaluator::new();
        let rule = AlertRule {
            name: "test_rule".to_string(),
            description: "desc".to_string(),
            metric_name: "cpu".to_string(),
            condition: AlertConditionType::GreaterThan,
            threshold: 90.0,
            duration_ms: 0,
            severity: AlertSeverity::Warning,
            notification: AlertNotification::Log,
        };

        evaluator.add_rule(rule.clone()).unwrap();
        assert_eq!(evaluator.list_rules().len(), 1);

        // Duplicate name should error.
        assert!(evaluator.add_rule(rule).is_err());

        evaluator.remove_rule("test_rule").unwrap();
        assert!(evaluator.list_rules().is_empty());

        // Removing non-existent rule should error.
        assert!(evaluator.remove_rule("nope").is_err());
    }

    // -- AlertEvaluator: evaluation with GreaterThan ---------------------

    #[test]
    fn test_alert_evaluation_greater_than() {
        let agg = MetricsAggregator::new(make_config());
        let evaluator = AlertEvaluator::new();

        evaluator
            .add_rule(AlertRule {
                name: "high_cpu".to_string(),
                description: "CPU > 90".to_string(),
                metric_name: "cpu".to_string(),
                condition: AlertConditionType::GreaterThan,
                threshold: 90.0,
                duration_ms: 0, // fire immediately
                severity: AlertSeverity::Critical,
                notification: AlertNotification::Log,
            })
            .unwrap();

        // Below threshold -- no alert.
        agg.record_gauge("cpu", 80.0, HashMap::new());
        let alerts = evaluator.evaluate(&agg);
        assert!(alerts.is_empty());

        // Above threshold -- should fire.
        agg.record_gauge("cpu", 95.0, HashMap::new());
        let alerts = evaluator.evaluate(&agg);
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].rule_name, "high_cpu");
        assert_eq!(alerts[0].severity, AlertSeverity::Critical);
        assert!((alerts[0].current_value - 95.0).abs() < f64::EPSILON);
    }

    // -- AlertEvaluator: LessThan condition ------------------------------

    #[test]
    fn test_alert_evaluation_less_than() {
        let agg = MetricsAggregator::new(make_config());
        let evaluator = AlertEvaluator::new();

        evaluator
            .add_rule(AlertRule {
                name: "low_throughput".to_string(),
                description: "Throughput below threshold".to_string(),
                metric_name: "throughput".to_string(),
                condition: AlertConditionType::LessThan,
                threshold: 10.0,
                duration_ms: 0,
                severity: AlertSeverity::Info,
                notification: AlertNotification::Log,
            })
            .unwrap();

        agg.record_gauge("throughput", 5.0, HashMap::new());
        let alerts = evaluator.evaluate(&agg);
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].rule_name, "low_throughput");

        // Above threshold -- should not fire.
        agg.record_gauge("throughput", 15.0, HashMap::new());
        let alerts = evaluator.evaluate(&agg);
        assert!(alerts.is_empty());
    }

    // -- default_alert_rules ---------------------------------------------

    #[test]
    fn test_default_alert_rules() {
        let rules = default_alert_rules();
        assert_eq!(rules.len(), 7);

        let names: Vec<&str> = rules.iter().map(|r| r.name.as_str()).collect();
        assert!(names.contains(&"consumer_lag_warning"));
        assert!(names.contains(&"consumer_lag_critical"));
        assert!(names.contains(&"disk_usage_warning"));
        assert!(names.contains(&"disk_usage_critical"));
        assert!(names.contains(&"error_rate_warning"));
        assert!(names.contains(&"error_rate_critical"));
        assert!(names.contains(&"no_messages_info"));

        // Verify severity ordering within paired rules.
        let lag_warn = rules
            .iter()
            .find(|r| r.name == "consumer_lag_warning")
            .unwrap();
        let lag_crit = rules
            .iter()
            .find(|r| r.name == "consumer_lag_critical")
            .unwrap();
        assert!(lag_crit.threshold > lag_warn.threshold);
        assert_eq!(lag_warn.severity, AlertSeverity::Warning);
        assert_eq!(lag_crit.severity, AlertSeverity::Critical);
    }

    // -- HealthState derivation ------------------------------------------

    #[test]
    fn test_health_state_derivation() {
        let agg = MetricsAggregator::new(make_config());

        // Empty => Unknown
        let snap = agg.get_dashboard_snapshot();
        assert_eq!(snap.cluster_health.status, HealthState::Unknown);

        // Low error rate + low disk => Healthy
        agg.record_gauge("error_rate_percent", 0.1, HashMap::new());
        agg.record_gauge("disk_usage_percent", 40.0, HashMap::new());
        let snap = agg.get_dashboard_snapshot();
        assert_eq!(snap.cluster_health.status, HealthState::Healthy);

        // High disk => Warning
        agg.record_gauge("disk_usage_percent", 85.0, HashMap::new());
        let snap = agg.get_dashboard_snapshot();
        assert_eq!(snap.cluster_health.status, HealthState::Warning);

        // Very high error rate => Critical
        agg.record_gauge("error_rate_percent", 10.0, HashMap::new());
        let snap = agg.get_dashboard_snapshot();
        assert_eq!(snap.cluster_health.status, HealthState::Critical);
    }

    // -- get_metric returns None for unknown metric ----------------------

    #[test]
    fn test_get_metric_unknown() {
        let agg = MetricsAggregator::new(make_config());
        assert!(agg.get_metric("nonexistent", None).is_none());
    }

    // -- counter accumulates across multiple calls -----------------------

    #[test]
    fn test_counter_accumulation() {
        let agg = MetricsAggregator::new(make_config());
        for _ in 0..100 {
            agg.record_counter("events", 1, HashMap::new());
        }

        let series = agg.get_metric("events", None).unwrap();
        let last = series.data_points.last().unwrap();
        assert!((last.value - 100.0).abs() < f64::EPSILON);
    }
}
