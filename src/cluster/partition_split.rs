//! Dynamic partition splitting support
//!
//! This module provides infrastructure for automatically splitting partitions
//! when they exceed configured thresholds. Partition splitting helps distribute
//! load more evenly across the cluster.
//!
//! ## Architecture
//!
//! ```text
//! PartitionMetrics → SplitDetector → SplitPolicy → SplitExecutor
//!       ↓                  ↓              ↓              ↓
//!   Collect         Evaluate       Decide         Execute
//!   metrics         triggers       if split       the split
//! ```
//!
//! ## Split Strategies
//!
//! - **KeyHash**: Distribute records based on key hash (most common)
//! - **RoundRobin**: Simple alternating distribution
//! - **OffsetBased**: Split at specific offset boundaries

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, info};

// ============================================================================
// Partition Metrics
// ============================================================================

/// Metrics collected for each partition to inform splitting decisions
#[derive(Debug, Clone)]
pub struct PartitionMetrics {
    /// Topic name
    pub topic: String,
    /// Partition ID
    pub partition_id: i32,
    /// Current size in bytes
    pub size_bytes: u64,
    /// Total record count
    pub record_count: u64,
    /// Throughput in messages per second (rolling average)
    pub throughput_msgs_sec: f64,
    /// Throughput in bytes per second (rolling average)
    pub throughput_bytes_sec: f64,
    /// Total consumer lag across all consumer groups
    pub consumer_lag: i64,
    /// P99 request latency in milliseconds
    pub request_latency_p99_ms: f64,
    /// When this snapshot was taken
    pub snapshot_time: Instant,
}

impl Default for PartitionMetrics {
    fn default() -> Self {
        Self {
            topic: String::new(),
            partition_id: 0,
            size_bytes: 0,
            record_count: 0,
            throughput_msgs_sec: 0.0,
            throughput_bytes_sec: 0.0,
            consumer_lag: 0,
            request_latency_p99_ms: 0.0,
            snapshot_time: Instant::now(),
        }
    }
}

/// Thread-safe partition metrics collector
#[derive(Debug)]
pub struct PartitionMetricsCollector {
    /// Metrics indexed by (topic, partition)
    metrics: RwLock<HashMap<(String, i32), PartitionMetricsState>>,
    /// Rolling window for throughput calculation
    #[allow(dead_code)]
    window_duration: Duration,
}

/// Internal state for tracking metrics over time
#[derive(Debug)]
struct PartitionMetricsState {
    /// Current metrics
    current: PartitionMetrics,
    /// Bytes written in current window
    window_bytes: AtomicU64,
    /// Messages written in current window
    window_msgs: AtomicU64,
    /// Requests processed in current window
    window_requests: AtomicU64,
    /// Total latency in current window (for averaging)
    window_latency_sum_ms: AtomicU64,
    /// Max latency in current window (for p99 estimation)
    window_latency_max_ms: AtomicU64,
    /// Consumer lag (sum across groups)
    consumer_lag: AtomicI64,
    /// Window start time
    window_start: Instant,
}

impl PartitionMetricsCollector {
    /// Create a new metrics collector
    pub fn new(window_duration: Duration) -> Self {
        Self {
            metrics: RwLock::new(HashMap::new()),
            window_duration,
        }
    }

    /// Record bytes and messages written to a partition
    pub fn record_write(&self, topic: &str, partition: i32, bytes: u64, msgs: u64) {
        let mut metrics = self.metrics.write();
        let key = (topic.to_string(), partition);

        let state = metrics
            .entry(key.clone())
            .or_insert_with(|| PartitionMetricsState::new(topic.to_string(), partition));

        state.window_bytes.fetch_add(bytes, Ordering::Relaxed);
        state.window_msgs.fetch_add(msgs, Ordering::Relaxed);
        state.current.size_bytes += bytes;
        state.current.record_count += msgs;
    }

    /// Record request latency for a partition
    pub fn record_latency(&self, topic: &str, partition: i32, latency_ms: u64) {
        let mut metrics = self.metrics.write();
        let key = (topic.to_string(), partition);

        if let Some(state) = metrics.get_mut(&key) {
            state.window_requests.fetch_add(1, Ordering::Relaxed);
            state
                .window_latency_sum_ms
                .fetch_add(latency_ms, Ordering::Relaxed);
            state
                .window_latency_max_ms
                .fetch_max(latency_ms, Ordering::Relaxed);
        }
    }

    /// Update consumer lag for a partition
    pub fn update_consumer_lag(&self, topic: &str, partition: i32, lag: i64) {
        let mut metrics = self.metrics.write();
        let key = (topic.to_string(), partition);

        if let Some(state) = metrics.get_mut(&key) {
            state.consumer_lag.store(lag, Ordering::Relaxed);
            state.current.consumer_lag = lag;
        }
    }

    /// Get current metrics snapshot for a partition
    pub fn get_metrics(&self, topic: &str, partition: i32) -> Option<PartitionMetrics> {
        let metrics = self.metrics.read();
        let key = (topic.to_string(), partition);
        metrics.get(&key).map(|state| state.current.clone())
    }

    /// Get all metrics snapshots
    pub fn get_all_metrics(&self) -> Vec<PartitionMetrics> {
        let metrics = self.metrics.read();
        metrics
            .values()
            .map(|state| state.current.clone())
            .collect()
    }

    /// Roll the metrics window and calculate throughput rates
    pub fn roll_window(&self) {
        let mut metrics = self.metrics.write();
        let now = Instant::now();

        for state in metrics.values_mut() {
            let elapsed = now.duration_since(state.window_start);
            let elapsed_secs = elapsed.as_secs_f64().max(0.001); // Avoid division by zero

            // Calculate throughput
            let bytes = state.window_bytes.swap(0, Ordering::Relaxed);
            let msgs = state.window_msgs.swap(0, Ordering::Relaxed);
            let requests = state.window_requests.swap(0, Ordering::Relaxed);
            let latency_sum = state.window_latency_sum_ms.swap(0, Ordering::Relaxed);
            let latency_max = state.window_latency_max_ms.swap(0, Ordering::Relaxed);

            state.current.throughput_bytes_sec = bytes as f64 / elapsed_secs;
            state.current.throughput_msgs_sec = msgs as f64 / elapsed_secs;

            // Approximate P99 using max (simple heuristic)
            if requests > 0 {
                let avg_latency = latency_sum as f64 / requests as f64;
                // P99 is approximated as between avg and max
                state.current.request_latency_p99_ms = (avg_latency + latency_max as f64) / 2.0;
            }

            state.current.snapshot_time = now;
            state.window_start = now;
        }
    }
}

impl PartitionMetricsState {
    fn new(topic: String, partition_id: i32) -> Self {
        Self {
            current: PartitionMetrics {
                topic,
                partition_id,
                ..Default::default()
            },
            window_bytes: AtomicU64::new(0),
            window_msgs: AtomicU64::new(0),
            window_requests: AtomicU64::new(0),
            window_latency_sum_ms: AtomicU64::new(0),
            window_latency_max_ms: AtomicU64::new(0),
            consumer_lag: AtomicI64::new(0),
            window_start: Instant::now(),
        }
    }
}

// ============================================================================
// Split Configuration
// ============================================================================

/// Trigger condition for partition splitting
#[derive(Debug, Clone, PartialEq)]
pub enum SplitTrigger {
    /// Partition size exceeds threshold
    SizeBytes(u64),
    /// Record count exceeds threshold
    RecordCount(u64),
    /// Throughput (msgs/sec) exceeds threshold
    ThroughputMsgsPerSec(f64),
    /// Throughput (bytes/sec) exceeds threshold
    ThroughputBytesPerSec(f64),
    /// Consumer lag exceeds threshold
    ConsumerLag(i64),
    /// Request latency exceeds threshold
    RequestLatencyP99Ms(f64),
}

impl SplitTrigger {
    /// Check if this trigger condition is met
    pub fn is_triggered(&self, metrics: &PartitionMetrics) -> bool {
        match self {
            SplitTrigger::SizeBytes(threshold) => metrics.size_bytes >= *threshold,
            SplitTrigger::RecordCount(threshold) => metrics.record_count >= *threshold,
            SplitTrigger::ThroughputMsgsPerSec(threshold) => {
                metrics.throughput_msgs_sec >= *threshold
            }
            SplitTrigger::ThroughputBytesPerSec(threshold) => {
                metrics.throughput_bytes_sec >= *threshold
            }
            SplitTrigger::ConsumerLag(threshold) => metrics.consumer_lag >= *threshold,
            SplitTrigger::RequestLatencyP99Ms(threshold) => {
                metrics.request_latency_p99_ms >= *threshold
            }
        }
    }

    /// Get a human-readable description of the trigger
    pub fn description(&self, metrics: &PartitionMetrics) -> String {
        match self {
            SplitTrigger::SizeBytes(threshold) => {
                format!("size {} >= {} bytes", metrics.size_bytes, threshold)
            }
            SplitTrigger::RecordCount(threshold) => {
                format!("record count {} >= {}", metrics.record_count, threshold)
            }
            SplitTrigger::ThroughputMsgsPerSec(threshold) => {
                format!(
                    "throughput {:.1} >= {:.1} msgs/sec",
                    metrics.throughput_msgs_sec, threshold
                )
            }
            SplitTrigger::ThroughputBytesPerSec(threshold) => {
                format!(
                    "throughput {:.1} >= {:.1} bytes/sec",
                    metrics.throughput_bytes_sec, threshold
                )
            }
            SplitTrigger::ConsumerLag(threshold) => {
                format!("consumer lag {} >= {}", metrics.consumer_lag, threshold)
            }
            SplitTrigger::RequestLatencyP99Ms(threshold) => {
                format!(
                    "latency p99 {:.1}ms >= {:.1}ms",
                    metrics.request_latency_p99_ms, threshold
                )
            }
        }
    }
}

/// Strategy for distributing records during split
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum SplitStrategy {
    /// Distribute based on record key hash (maintains key ordering)
    #[default]
    KeyHash,
    /// Round-robin distribution
    RoundRobin,
    /// Split at specific offset (all records before go to partition A, after to B)
    OffsetBased { split_offset: i64 },
    /// Time-based split (records before timestamp go to partition A)
    TimeBased { split_timestamp: i64 },
}

/// Configuration for partition split policy
#[derive(Debug, Clone)]
pub struct SplitPolicy {
    /// Whether automatic splitting is enabled
    pub enabled: bool,
    /// Triggers that can cause a split (any trigger can cause split)
    pub triggers: Vec<SplitTrigger>,
    /// Strategy for distributing records
    pub strategy: SplitStrategy,
    /// Number of new partitions to create (split 1 into N)
    pub split_factor: i32,
    /// Maximum total partitions per topic
    pub max_partitions_per_topic: i32,
    /// Minimum time between split evaluations
    pub evaluation_interval: Duration,
    /// Cooldown period after a split before another can occur
    pub cooldown: Duration,
}

impl Default for SplitPolicy {
    fn default() -> Self {
        Self {
            enabled: false,
            triggers: vec![
                // Default: split when partition exceeds 100GB
                SplitTrigger::SizeBytes(100 * 1024 * 1024 * 1024),
            ],
            strategy: SplitStrategy::KeyHash,
            split_factor: 2, // Double the partition count
            max_partitions_per_topic: 1000,
            evaluation_interval: Duration::from_secs(60),
            cooldown: Duration::from_secs(300), // 5 minutes
        }
    }
}

// ============================================================================
// Serde-Compatible Configuration
// ============================================================================

/// Serializable partition split configuration
///
/// This is the configuration format used in TOML/YAML files.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionSplitConfig {
    /// Whether automatic partition splitting is enabled
    #[serde(default)]
    pub enabled: bool,

    /// Split when partition size exceeds this many bytes (default: 100GB)
    #[serde(default = "default_size_threshold_bytes")]
    pub size_threshold_bytes: u64,

    /// Split when throughput exceeds this many messages/second (0 = disabled)
    #[serde(default)]
    pub throughput_threshold_msgs_per_sec: f64,

    /// Split when throughput exceeds this many bytes/second (0 = disabled)
    #[serde(default)]
    pub throughput_threshold_bytes_per_sec: f64,

    /// Split when consumer lag exceeds this many messages (0 = disabled)
    #[serde(default)]
    pub consumer_lag_threshold: i64,

    /// Split when P99 latency exceeds this many milliseconds (0 = disabled)
    #[serde(default)]
    pub latency_threshold_p99_ms: f64,

    /// Split strategy: "key_hash", "round_robin" (default: "key_hash")
    #[serde(default = "default_strategy")]
    pub strategy: String,

    /// Number of partitions to create per split (1 → N) (default: 2)
    #[serde(default = "default_split_factor")]
    pub split_factor: i32,

    /// Maximum partitions per topic (default: 1000)
    #[serde(default = "default_max_partitions")]
    pub max_partitions_per_topic: i32,

    /// Evaluation interval in seconds (default: 60)
    #[serde(default = "default_evaluation_interval_secs")]
    pub evaluation_interval_secs: u64,

    /// Cooldown period after split in seconds (default: 300)
    #[serde(default = "default_cooldown_secs")]
    pub cooldown_secs: u64,
}

fn default_size_threshold_bytes() -> u64 {
    100 * 1024 * 1024 * 1024 // 100GB
}

fn default_strategy() -> String {
    "key_hash".to_string()
}

fn default_split_factor() -> i32 {
    2
}

fn default_max_partitions() -> i32 {
    1000
}

fn default_evaluation_interval_secs() -> u64 {
    60
}

fn default_cooldown_secs() -> u64 {
    300
}

impl Default for PartitionSplitConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            size_threshold_bytes: default_size_threshold_bytes(),
            throughput_threshold_msgs_per_sec: 0.0,
            throughput_threshold_bytes_per_sec: 0.0,
            consumer_lag_threshold: 0,
            latency_threshold_p99_ms: 0.0,
            strategy: default_strategy(),
            split_factor: default_split_factor(),
            max_partitions_per_topic: default_max_partitions(),
            evaluation_interval_secs: default_evaluation_interval_secs(),
            cooldown_secs: default_cooldown_secs(),
        }
    }
}

impl PartitionSplitConfig {
    /// Convert to a SplitPolicy
    pub fn to_policy(&self) -> SplitPolicy {
        let mut triggers = Vec::new();

        if self.size_threshold_bytes > 0 {
            triggers.push(SplitTrigger::SizeBytes(self.size_threshold_bytes));
        }
        if self.throughput_threshold_msgs_per_sec > 0.0 {
            triggers.push(SplitTrigger::ThroughputMsgsPerSec(
                self.throughput_threshold_msgs_per_sec,
            ));
        }
        if self.throughput_threshold_bytes_per_sec > 0.0 {
            triggers.push(SplitTrigger::ThroughputBytesPerSec(
                self.throughput_threshold_bytes_per_sec,
            ));
        }
        if self.consumer_lag_threshold > 0 {
            triggers.push(SplitTrigger::ConsumerLag(self.consumer_lag_threshold));
        }
        if self.latency_threshold_p99_ms > 0.0 {
            triggers.push(SplitTrigger::RequestLatencyP99Ms(
                self.latency_threshold_p99_ms,
            ));
        }

        let strategy = match self.strategy.as_str() {
            "round_robin" => SplitStrategy::RoundRobin,
            _ => SplitStrategy::KeyHash,
        };

        SplitPolicy {
            enabled: self.enabled,
            triggers,
            strategy,
            split_factor: self.split_factor,
            max_partitions_per_topic: self.max_partitions_per_topic,
            evaluation_interval: Duration::from_secs(self.evaluation_interval_secs),
            cooldown: Duration::from_secs(self.cooldown_secs),
        }
    }
}

// ============================================================================
// Split Detection
// ============================================================================

/// Result of split evaluation
#[derive(Debug, Clone)]
pub struct SplitDecision {
    /// Topic name
    pub topic: String,
    /// Partition to split
    pub partition_id: i32,
    /// Current partition count for the topic
    pub current_partition_count: i32,
    /// Proposed new partition count
    pub new_partition_count: i32,
    /// Strategy to use
    pub strategy: SplitStrategy,
    /// Triggers that caused this decision
    pub triggered_by: Vec<String>,
    /// Metrics snapshot at decision time
    pub metrics_snapshot: PartitionMetrics,
}

/// Detects when partitions should be split based on policy
pub struct SplitDetector {
    /// Split policy configuration
    policy: SplitPolicy,
    /// Metrics collector
    metrics_collector: Arc<PartitionMetricsCollector>,
    /// Last split time per topic (for cooldown)
    last_split_times: RwLock<HashMap<String, Instant>>,
    /// Last evaluation time
    last_evaluation: RwLock<Instant>,
}

impl SplitDetector {
    /// Create a new split detector
    pub fn new(policy: SplitPolicy, metrics_collector: Arc<PartitionMetricsCollector>) -> Self {
        Self {
            policy,
            metrics_collector,
            last_split_times: RwLock::new(HashMap::new()),
            last_evaluation: RwLock::new(Instant::now() - Duration::from_secs(3600)),
        }
    }

    /// Evaluate all partitions and return split decisions
    pub fn evaluate(&self, topic_partition_counts: &HashMap<String, i32>) -> Vec<SplitDecision> {
        if !self.policy.enabled {
            return vec![];
        }

        // Check evaluation interval
        {
            let last = self.last_evaluation.read();
            if last.elapsed() < self.policy.evaluation_interval {
                return vec![];
            }
        }

        // Update evaluation time
        *self.last_evaluation.write() = Instant::now();

        let mut decisions = Vec::new();
        let all_metrics = self.metrics_collector.get_all_metrics();

        for metrics in all_metrics {
            // Check cooldown
            {
                let last_splits = self.last_split_times.read();
                if let Some(last_split) = last_splits.get(&metrics.topic) {
                    if last_split.elapsed() < self.policy.cooldown {
                        debug!(
                            topic = %metrics.topic,
                            partition = metrics.partition_id,
                            "Skipping split check - in cooldown period"
                        );
                        continue;
                    }
                }
            }

            // Check partition limit
            let current_count = topic_partition_counts
                .get(&metrics.topic)
                .copied()
                .unwrap_or(1);
            if current_count >= self.policy.max_partitions_per_topic {
                debug!(
                    topic = %metrics.topic,
                    current = current_count,
                    max = self.policy.max_partitions_per_topic,
                    "Skipping split - at max partition limit"
                );
                continue;
            }

            // Check triggers
            let mut triggered_by = Vec::new();
            for trigger in &self.policy.triggers {
                if trigger.is_triggered(&metrics) {
                    triggered_by.push(trigger.description(&metrics));
                }
            }

            if !triggered_by.is_empty() {
                let new_partition_count = (current_count * self.policy.split_factor)
                    .min(self.policy.max_partitions_per_topic);

                info!(
                    topic = %metrics.topic,
                    partition = metrics.partition_id,
                    triggers = ?triggered_by,
                    current_count = current_count,
                    new_count = new_partition_count,
                    "Partition split triggered"
                );

                decisions.push(SplitDecision {
                    topic: metrics.topic.clone(),
                    partition_id: metrics.partition_id,
                    current_partition_count: current_count,
                    new_partition_count,
                    strategy: self.policy.strategy.clone(),
                    triggered_by,
                    metrics_snapshot: metrics,
                });
            }
        }

        decisions
    }

    /// Record that a split was executed (for cooldown tracking)
    pub fn record_split(&self, topic: &str) {
        let mut last_splits = self.last_split_times.write();
        last_splits.insert(topic.to_string(), Instant::now());
    }

    /// Get the current policy
    pub fn policy(&self) -> &SplitPolicy {
        &self.policy
    }

    /// Update the policy
    pub fn set_policy(&mut self, policy: SplitPolicy) {
        self.policy = policy;
    }
}

// ============================================================================
// Split Execution Plan
// ============================================================================

/// Plan for executing a partition split
#[derive(Debug, Clone)]
pub struct SplitPlan {
    /// The decision that triggered this plan
    pub decision: SplitDecision,
    /// List of new partition IDs to create
    pub new_partition_ids: Vec<i32>,
    /// Mapping of old offset ranges to new partitions (for data migration)
    pub offset_mappings: Vec<OffsetMapping>,
    /// Status of the plan
    pub status: SplitPlanStatus,
    /// Creation time
    pub created_at: Instant,
}

/// Mapping of offset ranges during split
#[derive(Debug, Clone)]
pub struct OffsetMapping {
    /// Source partition
    pub source_partition: i32,
    /// Start offset (inclusive)
    pub start_offset: i64,
    /// End offset (exclusive), None means to the end
    pub end_offset: Option<i64>,
    /// Target partition
    pub target_partition: i32,
}

/// Status of a split plan
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SplitPlanStatus {
    /// Plan is pending execution
    Pending,
    /// Plan is being executed
    InProgress {
        /// Progress percentage (0-100)
        progress_percent: u8,
        /// Current phase description
        phase: String,
    },
    /// Plan completed successfully
    Completed {
        /// Duration of execution
        duration: Duration,
    },
    /// Plan failed
    Failed {
        /// Error message
        error: String,
    },
    /// Plan was cancelled
    Cancelled,
}

impl SplitPlan {
    /// Create a new split plan from a decision
    pub fn from_decision(decision: SplitDecision) -> Self {
        // Calculate new partition IDs
        let new_partition_ids: Vec<i32> =
            (decision.current_partition_count..decision.new_partition_count).collect();

        Self {
            decision,
            new_partition_ids,
            offset_mappings: vec![], // Filled during execution planning
            status: SplitPlanStatus::Pending,
            created_at: Instant::now(),
        }
    }

    /// Check if the plan is in a terminal state
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.status,
            SplitPlanStatus::Completed { .. }
                | SplitPlanStatus::Failed { .. }
                | SplitPlanStatus::Cancelled
        )
    }
}

// ============================================================================
// Split Events (for monitoring)
// ============================================================================

/// Events emitted during partition splitting
#[derive(Debug, Clone)]
pub enum SplitEvent {
    /// Split evaluation started
    EvaluationStarted { topic: String, partition: i32 },
    /// Split triggered
    SplitTriggered {
        topic: String,
        partition: i32,
        triggers: Vec<String>,
        new_partition_count: i32,
    },
    /// Split plan created
    PlanCreated {
        topic: String,
        partition: i32,
        plan_id: String,
    },
    /// Split execution started
    ExecutionStarted {
        topic: String,
        partition: i32,
        plan_id: String,
    },
    /// Progress update
    Progress {
        topic: String,
        partition: i32,
        plan_id: String,
        percent: u8,
        phase: String,
    },
    /// Split completed
    Completed {
        topic: String,
        partition: i32,
        new_partition_ids: Vec<i32>,
        duration_ms: u64,
    },
    /// Split failed
    Failed {
        topic: String,
        partition: i32,
        error: String,
    },
    /// Split cancelled
    Cancelled {
        topic: String,
        partition: i32,
        reason: String,
    },
}

impl SplitEvent {
    /// Get the topic for this event
    pub fn topic(&self) -> &str {
        match self {
            Self::EvaluationStarted { topic, .. } => topic,
            Self::SplitTriggered { topic, .. } => topic,
            Self::PlanCreated { topic, .. } => topic,
            Self::ExecutionStarted { topic, .. } => topic,
            Self::Progress { topic, .. } => topic,
            Self::Completed { topic, .. } => topic,
            Self::Failed { topic, .. } => topic,
            Self::Cancelled { topic, .. } => topic,
        }
    }

    /// Get a short description of the event
    pub fn description(&self) -> String {
        match self {
            Self::EvaluationStarted { partition, .. } => {
                format!("Evaluating partition {}", partition)
            }
            Self::SplitTriggered {
                partition,
                new_partition_count,
                ..
            } => {
                format!(
                    "Split triggered for partition {}, expanding to {} partitions",
                    partition, new_partition_count
                )
            }
            Self::PlanCreated {
                partition, plan_id, ..
            } => {
                format!("Split plan {} created for partition {}", plan_id, partition)
            }
            Self::ExecutionStarted {
                partition, plan_id, ..
            } => {
                format!(
                    "Executing split plan {} for partition {}",
                    plan_id, partition
                )
            }
            Self::Progress {
                partition,
                percent,
                phase,
                ..
            } => {
                format!(
                    "Partition {} split progress: {}% ({})",
                    partition, percent, phase
                )
            }
            Self::Completed {
                partition,
                new_partition_ids,
                duration_ms,
                ..
            } => {
                format!(
                    "Partition {} split complete: new partitions {:?} in {}ms",
                    partition, new_partition_ids, duration_ms
                )
            }
            Self::Failed {
                partition, error, ..
            } => {
                format!("Partition {} split failed: {}", partition, error)
            }
            Self::Cancelled {
                partition, reason, ..
            } => {
                format!("Partition {} split cancelled: {}", partition, reason)
            }
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_collector_basic() {
        let collector = PartitionMetricsCollector::new(Duration::from_secs(10));

        // Record some writes
        collector.record_write("test-topic", 0, 1000, 10);
        collector.record_write("test-topic", 0, 500, 5);

        let metrics = collector.get_metrics("test-topic", 0).unwrap();
        assert_eq!(metrics.size_bytes, 1500);
        assert_eq!(metrics.record_count, 15);
    }

    #[test]
    fn test_metrics_collector_window_roll() {
        let collector = PartitionMetricsCollector::new(Duration::from_millis(100));

        collector.record_write("test-topic", 0, 1000, 100);

        // Wait a bit and roll
        std::thread::sleep(Duration::from_millis(50));
        collector.roll_window();

        let metrics = collector.get_metrics("test-topic", 0).unwrap();
        assert!(metrics.throughput_bytes_sec > 0.0);
        assert!(metrics.throughput_msgs_sec > 0.0);
    }

    #[test]
    fn test_split_trigger_size() {
        let trigger = SplitTrigger::SizeBytes(1000);

        let metrics = PartitionMetrics {
            size_bytes: 500,
            ..Default::default()
        };
        assert!(!trigger.is_triggered(&metrics));

        let metrics = PartitionMetrics {
            size_bytes: 1500,
            ..Default::default()
        };
        assert!(trigger.is_triggered(&metrics));
    }

    #[test]
    fn test_split_trigger_throughput() {
        let trigger = SplitTrigger::ThroughputMsgsPerSec(1000.0);

        let metrics = PartitionMetrics {
            throughput_msgs_sec: 500.0,
            ..Default::default()
        };
        assert!(!trigger.is_triggered(&metrics));

        let metrics = PartitionMetrics {
            throughput_msgs_sec: 1500.0,
            ..Default::default()
        };
        assert!(trigger.is_triggered(&metrics));
    }

    #[test]
    fn test_split_detector_disabled() {
        let collector = Arc::new(PartitionMetricsCollector::new(Duration::from_secs(10)));
        let policy = SplitPolicy {
            enabled: false,
            ..Default::default()
        };
        let detector = SplitDetector::new(policy, collector.clone());

        // Record high metrics
        collector.record_write("test-topic", 0, 200 * 1024 * 1024 * 1024, 1_000_000);

        let mut counts = HashMap::new();
        counts.insert("test-topic".to_string(), 1);

        let decisions = detector.evaluate(&counts);
        assert!(decisions.is_empty());
    }

    #[test]
    fn test_split_detector_triggers() {
        let collector = Arc::new(PartitionMetricsCollector::new(Duration::from_secs(10)));
        let policy = SplitPolicy {
            enabled: true,
            triggers: vec![SplitTrigger::SizeBytes(1000)],
            split_factor: 2,
            evaluation_interval: Duration::from_millis(0), // No delay for testing
            cooldown: Duration::from_millis(0),
            ..Default::default()
        };
        let detector = SplitDetector::new(policy, collector.clone());

        // Record metrics that exceed threshold
        collector.record_write("test-topic", 0, 2000, 100);

        let mut counts = HashMap::new();
        counts.insert("test-topic".to_string(), 1);

        let decisions = detector.evaluate(&counts);
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].topic, "test-topic");
        assert_eq!(decisions[0].new_partition_count, 2);
    }

    #[test]
    fn test_split_detector_max_partitions() {
        let collector = Arc::new(PartitionMetricsCollector::new(Duration::from_secs(10)));
        let policy = SplitPolicy {
            enabled: true,
            triggers: vec![SplitTrigger::SizeBytes(1000)],
            split_factor: 2,
            max_partitions_per_topic: 4,
            evaluation_interval: Duration::from_millis(0),
            cooldown: Duration::from_millis(0),
            ..Default::default()
        };
        let detector = SplitDetector::new(policy, collector.clone());

        collector.record_write("test-topic", 0, 2000, 100);

        // Already at max partitions
        let mut counts = HashMap::new();
        counts.insert("test-topic".to_string(), 4);

        let decisions = detector.evaluate(&counts);
        assert!(decisions.is_empty());
    }

    #[test]
    fn test_split_plan_creation() {
        let decision = SplitDecision {
            topic: "test-topic".to_string(),
            partition_id: 0,
            current_partition_count: 2,
            new_partition_count: 4,
            strategy: SplitStrategy::KeyHash,
            triggered_by: vec!["size threshold".to_string()],
            metrics_snapshot: PartitionMetrics::default(),
        };

        let plan = SplitPlan::from_decision(decision);
        assert_eq!(plan.new_partition_ids, vec![2, 3]);
        assert_eq!(plan.status, SplitPlanStatus::Pending);
        assert!(!plan.is_terminal());
    }

    #[test]
    fn test_split_plan_terminal_states() {
        let decision = SplitDecision {
            topic: "test".to_string(),
            partition_id: 0,
            current_partition_count: 1,
            new_partition_count: 2,
            strategy: SplitStrategy::default(),
            triggered_by: vec![],
            metrics_snapshot: PartitionMetrics::default(),
        };

        let mut plan = SplitPlan::from_decision(decision);

        assert!(!plan.is_terminal());

        plan.status = SplitPlanStatus::InProgress {
            progress_percent: 50,
            phase: "copying".to_string(),
        };
        assert!(!plan.is_terminal());

        plan.status = SplitPlanStatus::Completed {
            duration: Duration::from_secs(10),
        };
        assert!(plan.is_terminal());

        plan.status = SplitPlanStatus::Failed {
            error: "test error".to_string(),
        };
        assert!(plan.is_terminal());

        plan.status = SplitPlanStatus::Cancelled;
        assert!(plan.is_terminal());
    }

    #[test]
    fn test_split_event_descriptions() {
        let event = SplitEvent::Completed {
            topic: "orders".to_string(),
            partition: 0,
            new_partition_ids: vec![1, 2],
            duration_ms: 5000,
        };

        let desc = event.description();
        assert!(desc.contains("0"));
        assert!(desc.contains("[1, 2]"));
        assert!(desc.contains("5000"));
    }
}
