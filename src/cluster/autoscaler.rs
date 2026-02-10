//! Auto-scaling controller for dynamic cluster sizing
//!
//! This module provides automatic cluster scaling based on metrics and policies.
//! It supports both scale-up and scale-down operations with configurable thresholds.
//!
//! ## Features
//!
//! - **Metric-based triggers**: Scale based on CPU, memory, throughput, or lag
//! - **Configurable policies**: Define min/max nodes, cooldown periods, scaling steps
//! - **Node draining**: Graceful scale-down with partition migration
//! - **Kubernetes HPA integration**: Expose custom metrics for external HPA
//!
//! ## Example
//!
//! ```
//! use streamline::cluster::autoscaler::{AutoScaler, AutoScalerConfig, ScalingPolicy};
//!
//! let config = AutoScalerConfig {
//!     enabled: true,
//!     min_nodes: 3,
//!     max_nodes: 10,
//!     scale_up_policies: vec![ScalingPolicy::cpu_based(80.0, 1)],
//!     scale_down_policies: vec![ScalingPolicy::cpu_based(20.0, 1)],
//!     ..Default::default()
//! };
//!
//! let autoscaler = AutoScaler::new(config);
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Configuration for the auto-scaler
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoScalerConfig {
    /// Whether auto-scaling is enabled
    pub enabled: bool,

    /// Minimum number of nodes (won't scale below this)
    pub min_nodes: usize,

    /// Maximum number of nodes (won't scale above this)
    pub max_nodes: usize,

    /// Cooldown period between scale-up operations (milliseconds)
    pub scale_up_cooldown_ms: u64,

    /// Cooldown period between scale-down operations (milliseconds)
    pub scale_down_cooldown_ms: u64,

    /// Stabilization window - how long metrics must stay above/below threshold
    pub stabilization_window_ms: u64,

    /// Evaluation interval in milliseconds
    pub evaluation_interval_ms: u64,

    /// Scale-up policies (any trigger can cause scale-up)
    pub scale_up_policies: Vec<ScalingPolicy>,

    /// Scale-down policies (all must be satisfied to scale down)
    pub scale_down_policies: Vec<ScalingPolicy>,

    /// Grace period for node draining during scale-down (milliseconds)
    pub drain_grace_period_ms: u64,

    /// Whether to expose metrics for Kubernetes HPA
    pub expose_hpa_metrics: bool,
}

impl Default for AutoScalerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            min_nodes: 1,
            max_nodes: 10,
            scale_up_cooldown_ms: 300_000,    // 5 minutes
            scale_down_cooldown_ms: 600_000,  // 10 minutes
            stabilization_window_ms: 300_000, // 5 minutes
            evaluation_interval_ms: 30_000,   // 30 seconds
            scale_up_policies: vec![
                ScalingPolicy::cpu_based(80.0, 1),
                ScalingPolicy::throughput_based(90.0, 1),
            ],
            scale_down_policies: vec![
                ScalingPolicy::cpu_based(30.0, 1),
                ScalingPolicy::throughput_based(30.0, 1),
            ],
            drain_grace_period_ms: 300_000, // 5 minutes
            expose_hpa_metrics: true,
        }
    }
}

impl AutoScalerConfig {
    /// Check if the configuration is valid
    pub fn validate(&self) -> Result<(), String> {
        if self.min_nodes == 0 {
            return Err("min_nodes must be at least 1".to_string());
        }
        if self.max_nodes < self.min_nodes {
            return Err("max_nodes must be >= min_nodes".to_string());
        }
        if self.scale_up_policies.is_empty() && self.enabled {
            return Err("At least one scale-up policy is required".to_string());
        }
        if self.scale_down_policies.is_empty() && self.enabled {
            return Err("At least one scale-down policy is required".to_string());
        }
        Ok(())
    }
}

/// A scaling policy that determines when to scale
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScalingPolicy {
    /// Type of metric to monitor
    pub metric_type: MetricType,

    /// Threshold value (percentage or absolute depending on metric type)
    pub threshold: f64,

    /// Number of nodes to add/remove when triggered
    pub step: usize,

    /// Optional target value for the metric (for target-based scaling)
    pub target_value: Option<f64>,

    /// Priority (higher = evaluated first)
    pub priority: i32,
}

impl ScalingPolicy {
    /// Create a CPU-based scaling policy
    pub fn cpu_based(threshold_percent: f64, step: usize) -> Self {
        Self {
            metric_type: MetricType::CpuUtilization,
            threshold: threshold_percent,
            step,
            target_value: None,
            priority: 10,
        }
    }

    /// Create a memory-based scaling policy
    pub fn memory_based(threshold_percent: f64, step: usize) -> Self {
        Self {
            metric_type: MetricType::MemoryUtilization,
            threshold: threshold_percent,
            step,
            target_value: None,
            priority: 10,
        }
    }

    /// Create a throughput-based scaling policy
    pub fn throughput_based(threshold_percent: f64, step: usize) -> Self {
        Self {
            metric_type: MetricType::ThroughputUtilization,
            threshold: threshold_percent,
            step,
            target_value: None,
            priority: 5,
        }
    }

    /// Create a consumer lag-based scaling policy
    pub fn lag_based(max_lag_threshold: f64, step: usize) -> Self {
        Self {
            metric_type: MetricType::ConsumerLag,
            threshold: max_lag_threshold,
            step,
            target_value: None,
            priority: 15,
        }
    }

    /// Create a partition count-based scaling policy
    pub fn partition_based(partitions_per_node: f64, step: usize) -> Self {
        Self {
            metric_type: MetricType::PartitionsPerNode,
            threshold: partitions_per_node,
            step,
            target_value: Some(partitions_per_node),
            priority: 3,
        }
    }

    /// Create a custom metric-based policy
    pub fn custom(metric_name: &str, threshold: f64, step: usize) -> Self {
        Self {
            metric_type: MetricType::Custom(metric_name.to_string()),
            threshold,
            step,
            target_value: None,
            priority: 1,
        }
    }
}

/// Types of metrics that can trigger scaling
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum MetricType {
    /// CPU utilization (0-100%)
    CpuUtilization,
    /// Memory utilization (0-100%)
    MemoryUtilization,
    /// Throughput utilization (0-100% of max)
    ThroughputUtilization,
    /// Consumer lag (messages behind)
    ConsumerLag,
    /// Number of partitions per node
    PartitionsPerNode,
    /// Number of connections per node
    ConnectionsPerNode,
    /// Under-replicated partition count
    UnderReplicatedPartitions,
    /// Custom metric by name
    Custom(String),
}

impl std::fmt::Display for MetricType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetricType::CpuUtilization => write!(f, "cpu_utilization"),
            MetricType::MemoryUtilization => write!(f, "memory_utilization"),
            MetricType::ThroughputUtilization => write!(f, "throughput_utilization"),
            MetricType::ConsumerLag => write!(f, "consumer_lag"),
            MetricType::PartitionsPerNode => write!(f, "partitions_per_node"),
            MetricType::ConnectionsPerNode => write!(f, "connections_per_node"),
            MetricType::UnderReplicatedPartitions => write!(f, "under_replicated_partitions"),
            MetricType::Custom(name) => write!(f, "custom:{}", name),
        }
    }
}

/// Direction of scaling
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScaleDirection {
    Up,
    Down,
    None,
}

impl std::fmt::Display for ScaleDirection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScaleDirection::Up => write!(f, "up"),
            ScaleDirection::Down => write!(f, "down"),
            ScaleDirection::None => write!(f, "none"),
        }
    }
}

/// A scaling decision made by the auto-scaler
#[derive(Debug, Clone)]
pub struct ScalingDecision {
    /// Direction of scaling
    pub direction: ScaleDirection,
    /// Number of nodes to add/remove
    pub delta: usize,
    /// Policy that triggered the decision
    pub triggered_by: Option<MetricType>,
    /// Current metric values
    pub metrics: HashMap<MetricType, f64>,
    /// Timestamp of the decision
    pub timestamp: Instant,
    /// Reason for the decision
    pub reason: String,
}

impl ScalingDecision {
    /// Create a no-scaling decision
    pub fn no_scale(metrics: HashMap<MetricType, f64>, reason: &str) -> Self {
        Self {
            direction: ScaleDirection::None,
            delta: 0,
            triggered_by: None,
            metrics,
            timestamp: Instant::now(),
            reason: reason.to_string(),
        }
    }

    /// Create a scale-up decision
    pub fn scale_up(
        delta: usize,
        metric: MetricType,
        metrics: HashMap<MetricType, f64>,
        reason: &str,
    ) -> Self {
        Self {
            direction: ScaleDirection::Up,
            delta,
            triggered_by: Some(metric),
            metrics,
            timestamp: Instant::now(),
            reason: reason.to_string(),
        }
    }

    /// Create a scale-down decision
    pub fn scale_down(
        delta: usize,
        metric: MetricType,
        metrics: HashMap<MetricType, f64>,
        reason: &str,
    ) -> Self {
        Self {
            direction: ScaleDirection::Down,
            delta,
            triggered_by: Some(metric),
            metrics,
            timestamp: Instant::now(),
            reason: reason.to_string(),
        }
    }
}

/// State of a node during draining
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DrainState {
    /// Node is active and handling requests
    Active,
    /// Node is being drained (partitions being moved)
    Draining,
    /// Node has been drained and is ready for removal
    Drained,
    /// Drain was cancelled
    Cancelled,
}

impl std::fmt::Display for DrainState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DrainState::Active => write!(f, "active"),
            DrainState::Draining => write!(f, "draining"),
            DrainState::Drained => write!(f, "drained"),
            DrainState::Cancelled => write!(f, "cancelled"),
        }
    }
}

/// Information about a node being drained
#[derive(Debug, Clone)]
pub struct DrainInfo {
    /// Node ID being drained
    pub node_id: u64,
    /// Current drain state
    pub state: DrainState,
    /// Partitions remaining to be moved
    pub partitions_remaining: usize,
    /// When draining started
    pub started_at: Instant,
    /// Target completion time
    pub deadline: Instant,
}

/// Metrics snapshot for scaling decisions
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    /// Per-metric values
    pub values: HashMap<MetricType, f64>,
    /// Current number of nodes
    pub current_nodes: usize,
    /// Timestamp of snapshot
    pub timestamp: Instant,
}

impl Default for MetricsSnapshot {
    fn default() -> Self {
        Self {
            values: HashMap::new(),
            current_nodes: 0,
            timestamp: Instant::now(),
        }
    }
}

impl MetricsSnapshot {
    /// Create a new metrics snapshot
    pub fn new(current_nodes: usize) -> Self {
        Self {
            values: HashMap::new(),
            current_nodes,
            timestamp: Instant::now(),
        }
    }

    /// Set a metric value
    pub fn set(&mut self, metric: MetricType, value: f64) {
        self.values.insert(metric, value);
    }

    /// Get a metric value
    pub fn get(&self, metric: &MetricType) -> Option<f64> {
        self.values.get(metric).copied()
    }
}

/// Auto-scaler state
#[derive(Debug, Default)]
struct AutoScalerState {
    /// Last scale-up time
    last_scale_up: Option<Instant>,
    /// Last scale-down time
    last_scale_down: Option<Instant>,
    /// Recent metric history for stabilization
    metric_history: Vec<MetricsSnapshot>,
    /// Nodes currently being drained
    draining_nodes: HashMap<u64, DrainInfo>,
    /// Current desired node count
    desired_nodes: usize,
    /// Whether a scaling operation is in progress
    scaling_in_progress: bool,
}

/// Auto-scaler controller
pub struct AutoScaler {
    /// Configuration
    config: AutoScalerConfig,
    /// Internal state
    state: Arc<RwLock<AutoScalerState>>,
}

impl std::fmt::Debug for AutoScaler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AutoScaler")
            .field("config", &self.config)
            .field("enabled", &self.config.enabled)
            .finish_non_exhaustive()
    }
}

impl AutoScaler {
    /// Create a new auto-scaler
    pub fn new(config: AutoScalerConfig) -> Self {
        Self {
            config,
            state: Arc::new(RwLock::new(AutoScalerState::default())),
        }
    }

    /// Check if auto-scaling is enabled
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Get the configuration
    pub fn config(&self) -> &AutoScalerConfig {
        &self.config
    }

    /// Initialize with current node count
    pub async fn initialize(&self, current_nodes: usize) {
        let mut state = self.state.write().await;
        state.desired_nodes = current_nodes;
        info!(current_nodes, "Auto-scaler initialized");
    }

    /// Evaluate metrics and decide whether to scale
    pub async fn evaluate(&self, snapshot: MetricsSnapshot) -> ScalingDecision {
        if !self.config.enabled {
            return ScalingDecision::no_scale(snapshot.values, "Auto-scaling is disabled");
        }

        let mut state = self.state.write().await;

        // Add to history
        state.metric_history.push(snapshot.clone());

        // Trim old history
        let stabilization_window = Duration::from_millis(self.config.stabilization_window_ms);
        let cutoff = Instant::now() - stabilization_window;
        state.metric_history.retain(|s| s.timestamp > cutoff);

        // Don't scale if a scaling operation is in progress
        if state.scaling_in_progress {
            return ScalingDecision::no_scale(
                snapshot.values.clone(),
                "Scaling operation in progress",
            );
        }

        // Check cooldowns
        let now = Instant::now();
        let scale_up_cooldown = Duration::from_millis(self.config.scale_up_cooldown_ms);
        let scale_down_cooldown = Duration::from_millis(self.config.scale_down_cooldown_ms);

        let can_scale_up = state
            .last_scale_up
            .map(|t| now.duration_since(t) >= scale_up_cooldown)
            .unwrap_or(true);

        let can_scale_down = state
            .last_scale_down
            .map(|t| now.duration_since(t) >= scale_down_cooldown)
            .unwrap_or(true);

        // Check node limits
        let current_nodes = snapshot.current_nodes;

        // Evaluate scale-up policies (any trigger can cause scale-up)
        if can_scale_up && current_nodes < self.config.max_nodes {
            for policy in &self.config.scale_up_policies {
                if let Some(value) = snapshot.get(&policy.metric_type) {
                    if value >= policy.threshold {
                        // Check if metric has been above threshold for stabilization window
                        if self.is_metric_stable_above(
                            &state.metric_history,
                            &policy.metric_type,
                            policy.threshold,
                        ) {
                            let new_nodes =
                                (current_nodes + policy.step).min(self.config.max_nodes);
                            let delta = new_nodes - current_nodes;
                            if delta > 0 {
                                return ScalingDecision::scale_up(
                                    delta,
                                    policy.metric_type.clone(),
                                    snapshot.values,
                                    &format!(
                                        "{} ({:.1}%) exceeded threshold ({:.1}%)",
                                        policy.metric_type, value, policy.threshold
                                    ),
                                );
                            }
                        }
                    }
                }
            }
        }

        // Evaluate scale-down policies (all must be satisfied)
        if can_scale_down && current_nodes > self.config.min_nodes {
            let all_below_threshold = self.config.scale_down_policies.iter().all(|policy| {
                snapshot
                    .get(&policy.metric_type)
                    .map(|v| v <= policy.threshold)
                    .unwrap_or(false)
            });

            if all_below_threshold {
                // Check if all metrics have been below threshold for stabilization window
                let all_stable = self.config.scale_down_policies.iter().all(|policy| {
                    self.is_metric_stable_below(
                        &state.metric_history,
                        &policy.metric_type,
                        policy.threshold,
                    )
                });

                if all_stable {
                    // Find the policy with the largest step
                    if let Some(policy) = self
                        .config
                        .scale_down_policies
                        .iter()
                        .max_by_key(|p| p.step)
                    {
                        let new_nodes = current_nodes
                            .saturating_sub(policy.step)
                            .max(self.config.min_nodes);
                        let delta = current_nodes - new_nodes;
                        if delta > 0 {
                            return ScalingDecision::scale_down(
                                delta,
                                policy.metric_type.clone(),
                                snapshot.values,
                                "All metrics below scale-down thresholds",
                            );
                        }
                    }
                }
            }
        }

        ScalingDecision::no_scale(snapshot.values, "No scaling needed")
    }

    /// Check if a metric has been consistently above threshold
    fn is_metric_stable_above(
        &self,
        history: &[MetricsSnapshot],
        metric: &MetricType,
        threshold: f64,
    ) -> bool {
        if history.len() < 3 {
            // Not enough data points
            return false;
        }

        history.iter().all(|snapshot| {
            snapshot
                .get(metric)
                .map(|v| v >= threshold)
                .unwrap_or(false)
        })
    }

    /// Check if a metric has been consistently below threshold
    fn is_metric_stable_below(
        &self,
        history: &[MetricsSnapshot],
        metric: &MetricType,
        threshold: f64,
    ) -> bool {
        if history.len() < 3 {
            // Not enough data points
            return false;
        }

        history.iter().all(|snapshot| {
            snapshot
                .get(metric)
                .map(|v| v <= threshold)
                .unwrap_or(false)
        })
    }

    /// Start draining a node for scale-down
    pub async fn start_drain(&self, node_id: u64, partitions: usize) -> DrainInfo {
        let mut state = self.state.write().await;

        let drain_info = DrainInfo {
            node_id,
            state: DrainState::Draining,
            partitions_remaining: partitions,
            started_at: Instant::now(),
            deadline: Instant::now() + Duration::from_millis(self.config.drain_grace_period_ms),
        };

        state.draining_nodes.insert(node_id, drain_info.clone());
        info!(node_id, partitions, "Started draining node");

        drain_info
    }

    /// Update drain progress
    pub async fn update_drain_progress(&self, node_id: u64, partitions_remaining: usize) {
        let mut state = self.state.write().await;

        if let Some(drain_info) = state.draining_nodes.get_mut(&node_id) {
            drain_info.partitions_remaining = partitions_remaining;

            if partitions_remaining == 0 {
                drain_info.state = DrainState::Drained;
                info!(node_id, "Node drain complete");
            } else {
                debug!(node_id, partitions_remaining, "Drain progress");
            }
        }
    }

    /// Cancel draining a node
    pub async fn cancel_drain(&self, node_id: u64) {
        let mut state = self.state.write().await;

        if let Some(drain_info) = state.draining_nodes.get_mut(&node_id) {
            drain_info.state = DrainState::Cancelled;
            warn!(node_id, "Node drain cancelled");
        }
    }

    /// Get drain status for a node
    pub async fn get_drain_status(&self, node_id: u64) -> Option<DrainInfo> {
        let state = self.state.read().await;
        state.draining_nodes.get(&node_id).cloned()
    }

    /// Get all draining nodes
    pub async fn get_draining_nodes(&self) -> Vec<DrainInfo> {
        let state = self.state.read().await;
        state.draining_nodes.values().cloned().collect()
    }

    /// Record that a scale-up occurred
    pub async fn record_scale_up(&self, new_desired: usize) {
        let mut state = self.state.write().await;
        state.last_scale_up = Some(Instant::now());
        state.desired_nodes = new_desired;
        state.scaling_in_progress = false;
        info!(new_desired, "Recorded scale-up");
    }

    /// Record that a scale-down occurred
    pub async fn record_scale_down(&self, new_desired: usize) {
        let mut state = self.state.write().await;
        state.last_scale_down = Some(Instant::now());
        state.desired_nodes = new_desired;
        state.scaling_in_progress = false;
        info!(new_desired, "Recorded scale-down");
    }

    /// Mark scaling as in progress
    pub async fn set_scaling_in_progress(&self, in_progress: bool) {
        let mut state = self.state.write().await;
        state.scaling_in_progress = in_progress;
    }

    /// Get current desired node count
    pub async fn get_desired_nodes(&self) -> usize {
        let state = self.state.read().await;
        state.desired_nodes
    }

    /// Get auto-scaler statistics
    pub async fn stats(&self) -> AutoScalerStats {
        let state = self.state.read().await;
        AutoScalerStats {
            enabled: self.config.enabled,
            min_nodes: self.config.min_nodes,
            max_nodes: self.config.max_nodes,
            desired_nodes: state.desired_nodes,
            draining_nodes: state.draining_nodes.len(),
            last_scale_up: state.last_scale_up,
            last_scale_down: state.last_scale_down,
            metric_history_size: state.metric_history.len(),
            scaling_in_progress: state.scaling_in_progress,
        }
    }
}

/// Statistics from the auto-scaler
#[derive(Debug, Clone)]
pub struct AutoScalerStats {
    /// Whether auto-scaling is enabled
    pub enabled: bool,
    /// Minimum nodes
    pub min_nodes: usize,
    /// Maximum nodes
    pub max_nodes: usize,
    /// Current desired node count
    pub desired_nodes: usize,
    /// Number of nodes being drained
    pub draining_nodes: usize,
    /// Last scale-up time
    pub last_scale_up: Option<Instant>,
    /// Last scale-down time
    pub last_scale_down: Option<Instant>,
    /// Size of metric history
    pub metric_history_size: usize,
    /// Whether scaling is in progress
    pub scaling_in_progress: bool,
}

/// Kubernetes HPA metrics adapter
///
/// Exposes custom metrics in a format suitable for Kubernetes
/// Horizontal Pod Autoscaler external metrics.
#[derive(Debug)]
pub struct HpaMetricsAdapter {
    /// Auto-scaler reference
    autoscaler: Arc<AutoScaler>,
}

impl HpaMetricsAdapter {
    /// Create a new HPA metrics adapter
    pub fn new(autoscaler: Arc<AutoScaler>) -> Self {
        Self { autoscaler }
    }

    /// Get metrics in HPA-compatible format
    pub async fn get_external_metrics(&self) -> HashMap<String, HpaMetric> {
        let stats = self.autoscaler.stats().await;
        let mut metrics = HashMap::new();

        // Basic scaling metrics
        metrics.insert(
            "streamline_desired_replicas".to_string(),
            HpaMetric {
                value: stats.desired_nodes as f64,
                timestamp: chrono::Utc::now(),
            },
        );

        metrics.insert(
            "streamline_min_replicas".to_string(),
            HpaMetric {
                value: stats.min_nodes as f64,
                timestamp: chrono::Utc::now(),
            },
        );

        metrics.insert(
            "streamline_max_replicas".to_string(),
            HpaMetric {
                value: stats.max_nodes as f64,
                timestamp: chrono::Utc::now(),
            },
        );

        metrics.insert(
            "streamline_scaling_in_progress".to_string(),
            HpaMetric {
                value: if stats.scaling_in_progress { 1.0 } else { 0.0 },
                timestamp: chrono::Utc::now(),
            },
        );

        metrics
    }
}

/// A metric value for HPA
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HpaMetric {
    /// The metric value
    pub value: f64,
    /// When the metric was collected
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_autoscaler_config_default() {
        let config = AutoScalerConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.min_nodes, 1);
        assert_eq!(config.max_nodes, 10);
    }

    #[test]
    fn test_autoscaler_config_validation() {
        // Valid config
        let config = AutoScalerConfig {
            enabled: true,
            ..Default::default()
        };
        assert!(config.validate().is_ok());

        // Invalid: min_nodes = 0
        let config_zero_min = AutoScalerConfig {
            enabled: true,
            min_nodes: 0,
            ..Default::default()
        };
        assert!(config_zero_min.validate().is_err());

        // Invalid: max < min
        let config_max_lt_min = AutoScalerConfig {
            enabled: true,
            min_nodes: 5,
            max_nodes: 3,
            ..Default::default()
        };
        assert!(config_max_lt_min.validate().is_err());
    }

    #[test]
    fn test_scaling_policy_cpu_based() {
        let policy = ScalingPolicy::cpu_based(80.0, 2);
        assert_eq!(policy.metric_type, MetricType::CpuUtilization);
        assert_eq!(policy.threshold, 80.0);
        assert_eq!(policy.step, 2);
    }

    #[test]
    fn test_scaling_policy_memory_based() {
        let policy = ScalingPolicy::memory_based(90.0, 1);
        assert_eq!(policy.metric_type, MetricType::MemoryUtilization);
        assert_eq!(policy.threshold, 90.0);
    }

    #[test]
    fn test_scaling_policy_lag_based() {
        let policy = ScalingPolicy::lag_based(10000.0, 3);
        assert_eq!(policy.metric_type, MetricType::ConsumerLag);
        assert_eq!(policy.threshold, 10000.0);
    }

    #[test]
    fn test_scaling_policy_custom() {
        let policy = ScalingPolicy::custom("my_metric", 50.0, 1);
        assert_eq!(
            policy.metric_type,
            MetricType::Custom("my_metric".to_string())
        );
    }

    #[test]
    fn test_metric_type_display() {
        assert_eq!(format!("{}", MetricType::CpuUtilization), "cpu_utilization");
        assert_eq!(format!("{}", MetricType::ConsumerLag), "consumer_lag");
        assert_eq!(
            format!("{}", MetricType::Custom("foo".to_string())),
            "custom:foo"
        );
    }

    #[test]
    fn test_scale_direction_display() {
        assert_eq!(format!("{}", ScaleDirection::Up), "up");
        assert_eq!(format!("{}", ScaleDirection::Down), "down");
        assert_eq!(format!("{}", ScaleDirection::None), "none");
    }

    #[test]
    fn test_drain_state_display() {
        assert_eq!(format!("{}", DrainState::Active), "active");
        assert_eq!(format!("{}", DrainState::Draining), "draining");
        assert_eq!(format!("{}", DrainState::Drained), "drained");
        assert_eq!(format!("{}", DrainState::Cancelled), "cancelled");
    }

    #[test]
    fn test_scaling_decision_no_scale() {
        let metrics = HashMap::new();
        let decision = ScalingDecision::no_scale(metrics.clone(), "test");
        assert_eq!(decision.direction, ScaleDirection::None);
        assert_eq!(decision.delta, 0);
    }

    #[test]
    fn test_scaling_decision_scale_up() {
        let metrics = HashMap::new();
        let decision =
            ScalingDecision::scale_up(2, MetricType::CpuUtilization, metrics.clone(), "high cpu");
        assert_eq!(decision.direction, ScaleDirection::Up);
        assert_eq!(decision.delta, 2);
    }

    #[test]
    fn test_scaling_decision_scale_down() {
        let metrics = HashMap::new();
        let decision =
            ScalingDecision::scale_down(1, MetricType::CpuUtilization, metrics.clone(), "low cpu");
        assert_eq!(decision.direction, ScaleDirection::Down);
        assert_eq!(decision.delta, 1);
    }

    #[test]
    fn test_metrics_snapshot() {
        let mut snapshot = MetricsSnapshot::new(5);
        snapshot.set(MetricType::CpuUtilization, 75.0);
        snapshot.set(MetricType::MemoryUtilization, 60.0);

        assert_eq!(snapshot.current_nodes, 5);
        assert_eq!(snapshot.get(&MetricType::CpuUtilization), Some(75.0));
        assert_eq!(snapshot.get(&MetricType::MemoryUtilization), Some(60.0));
        assert_eq!(snapshot.get(&MetricType::ConsumerLag), None);
    }

    #[tokio::test]
    async fn test_autoscaler_disabled() {
        let config = AutoScalerConfig::default();
        let autoscaler = AutoScaler::new(config);
        assert!(!autoscaler.is_enabled());

        let snapshot = MetricsSnapshot::new(3);
        let decision = autoscaler.evaluate(snapshot).await;
        assert_eq!(decision.direction, ScaleDirection::None);
        assert!(decision.reason.contains("disabled"));
    }

    #[tokio::test]
    async fn test_autoscaler_initialize() {
        let config = AutoScalerConfig::default();
        let autoscaler = AutoScaler::new(config);
        autoscaler.initialize(5).await;

        assert_eq!(autoscaler.get_desired_nodes().await, 5);
    }

    #[tokio::test]
    async fn test_autoscaler_stats() {
        let config = AutoScalerConfig::default();
        let autoscaler = AutoScaler::new(config);
        autoscaler.initialize(3).await;

        let stats = autoscaler.stats().await;
        assert!(!stats.enabled);
        assert_eq!(stats.min_nodes, 1);
        assert_eq!(stats.max_nodes, 10);
        assert_eq!(stats.desired_nodes, 3);
    }

    #[tokio::test]
    async fn test_autoscaler_drain() {
        let config = AutoScalerConfig::default();
        let autoscaler = AutoScaler::new(config);

        let drain_info = autoscaler.start_drain(1, 10).await;
        assert_eq!(drain_info.node_id, 1);
        assert_eq!(drain_info.state, DrainState::Draining);
        assert_eq!(drain_info.partitions_remaining, 10);

        // Update progress
        autoscaler.update_drain_progress(1, 5).await;
        let info = autoscaler.get_drain_status(1).await.unwrap();
        assert_eq!(info.partitions_remaining, 5);

        // Complete drain
        autoscaler.update_drain_progress(1, 0).await;
        let info = autoscaler.get_drain_status(1).await.unwrap();
        assert_eq!(info.state, DrainState::Drained);
    }

    #[tokio::test]
    async fn test_autoscaler_cancel_drain() {
        let config = AutoScalerConfig::default();
        let autoscaler = AutoScaler::new(config);

        autoscaler.start_drain(2, 20).await;
        autoscaler.cancel_drain(2).await;

        let info = autoscaler.get_drain_status(2).await.unwrap();
        assert_eq!(info.state, DrainState::Cancelled);
    }

    #[tokio::test]
    async fn test_autoscaler_record_scale_up() {
        let config = AutoScalerConfig::default();
        let autoscaler = AutoScaler::new(config);
        autoscaler.initialize(3).await;

        autoscaler.record_scale_up(5).await;
        assert_eq!(autoscaler.get_desired_nodes().await, 5);
    }

    #[tokio::test]
    async fn test_autoscaler_record_scale_down() {
        let config = AutoScalerConfig::default();
        let autoscaler = AutoScaler::new(config);
        autoscaler.initialize(5).await;

        autoscaler.record_scale_down(3).await;
        assert_eq!(autoscaler.get_desired_nodes().await, 3);
    }

    #[tokio::test]
    async fn test_autoscaler_draining_nodes_list() {
        let config = AutoScalerConfig::default();
        let autoscaler = AutoScaler::new(config);

        autoscaler.start_drain(1, 10).await;
        autoscaler.start_drain(2, 20).await;

        let draining = autoscaler.get_draining_nodes().await;
        assert_eq!(draining.len(), 2);
    }

    #[tokio::test]
    async fn test_hpa_metrics_adapter() {
        let config = AutoScalerConfig::default();
        let autoscaler = Arc::new(AutoScaler::new(config));
        autoscaler.initialize(5).await;

        let adapter = HpaMetricsAdapter::new(autoscaler);
        let metrics = adapter.get_external_metrics().await;

        assert!(metrics.contains_key("streamline_desired_replicas"));
        assert_eq!(
            metrics.get("streamline_desired_replicas").unwrap().value,
            5.0
        );
    }

    #[tokio::test]
    async fn test_autoscaler_scaling_in_progress() {
        let config = AutoScalerConfig::default();
        let autoscaler = AutoScaler::new(config);

        let stats = autoscaler.stats().await;
        assert!(!stats.scaling_in_progress);

        autoscaler.set_scaling_in_progress(true).await;
        let stats = autoscaler.stats().await;
        assert!(stats.scaling_in_progress);
    }
}
