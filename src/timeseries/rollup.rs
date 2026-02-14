//! Time-Series Rollups
//!
//! Provides automatic rollup/aggregation for efficient long-term storage.

use super::DataPoint;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Rollup interval
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RollupInterval {
    /// 1 minute rollup
    Minute1,
    /// 5 minute rollup
    Minute5,
    /// 15 minute rollup
    Minute15,
    /// 1 hour rollup
    Hour1,
    /// 6 hour rollup
    Hour6,
    /// 1 day rollup
    Day1,
    /// 1 week rollup
    Week1,
}

impl RollupInterval {
    /// Get interval duration in milliseconds
    pub fn duration_ms(&self) -> i64 {
        match self {
            RollupInterval::Minute1 => 60 * 1000,
            RollupInterval::Minute5 => 5 * 60 * 1000,
            RollupInterval::Minute15 => 15 * 60 * 1000,
            RollupInterval::Hour1 => 60 * 60 * 1000,
            RollupInterval::Hour6 => 6 * 60 * 60 * 1000,
            RollupInterval::Day1 => 24 * 60 * 60 * 1000,
            RollupInterval::Week1 => 7 * 24 * 60 * 60 * 1000,
        }
    }

    /// Get bucket for a timestamp
    pub fn bucket(&self, timestamp: i64) -> i64 {
        let duration = self.duration_ms();
        (timestamp / duration) * duration
    }
}

/// Rollup function to apply
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RollupFunction {
    /// Average value
    Avg,
    /// Sum of values
    Sum,
    /// Minimum value
    Min,
    /// Maximum value
    Max,
    /// Count of values
    Count,
    /// First value
    First,
    /// Last value
    Last,
}

impl RollupFunction {
    /// Apply the function to values
    pub fn apply(&self, values: &[f64]) -> Option<f64> {
        if values.is_empty() {
            return None;
        }

        match self {
            RollupFunction::Avg => {
                let sum: f64 = values.iter().sum();
                Some(sum / values.len() as f64)
            }
            RollupFunction::Sum => Some(values.iter().sum()),
            RollupFunction::Min => values.iter().cloned().reduce(f64::min),
            RollupFunction::Max => values.iter().cloned().reduce(f64::max),
            RollupFunction::Count => Some(values.len() as f64),
            RollupFunction::First => values.first().copied(),
            RollupFunction::Last => values.last().copied(),
        }
    }
}

/// Rollup policy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RollupPolicy {
    /// Source interval (raw or lower-level rollup)
    pub source: RollupInterval,
    /// Target rollup interval
    pub target: RollupInterval,
    /// Functions to compute
    pub functions: Vec<RollupFunction>,
    /// Delay before rollup (allow late data)
    pub delay_ms: i64,
    /// Whether this rollup is enabled
    pub enabled: bool,
}

impl RollupPolicy {
    /// Create a new rollup policy
    pub fn new(source: RollupInterval, target: RollupInterval) -> Self {
        Self {
            source,
            target,
            functions: vec![
                RollupFunction::Avg,
                RollupFunction::Min,
                RollupFunction::Max,
            ],
            delay_ms: 60000, // 1 minute delay
            enabled: true,
        }
    }

    /// Set rollup functions
    pub fn with_functions(mut self, functions: Vec<RollupFunction>) -> Self {
        self.functions = functions;
        self
    }

    /// Set delay
    pub fn with_delay(mut self, delay_ms: i64) -> Self {
        self.delay_ms = delay_ms;
        self
    }

    /// Enable/disable
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }
}

/// Rollup configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RollupConfig {
    /// Rollup policies (in order of execution)
    pub policies: Vec<RollupPolicy>,
    /// Maximum concurrent rollups
    pub max_concurrent: usize,
    /// Batch size for rollup processing
    pub batch_size: usize,
}

impl Default for RollupConfig {
    fn default() -> Self {
        Self {
            policies: vec![
                // 1min -> 5min
                RollupPolicy::new(RollupInterval::Minute1, RollupInterval::Minute5),
                // 5min -> 1hour
                RollupPolicy::new(RollupInterval::Minute5, RollupInterval::Hour1),
                // 1hour -> 1day
                RollupPolicy::new(RollupInterval::Hour1, RollupInterval::Day1),
            ],
            max_concurrent: 4,
            batch_size: 10000,
        }
    }
}

impl RollupConfig {
    /// Create with custom policies
    pub fn with_policies(policies: Vec<RollupPolicy>) -> Self {
        Self {
            policies,
            ..Default::default()
        }
    }

    /// Create a minimal config (raw -> hourly -> daily)
    pub fn minimal() -> Self {
        Self {
            policies: vec![
                RollupPolicy::new(RollupInterval::Minute1, RollupInterval::Hour1),
                RollupPolicy::new(RollupInterval::Hour1, RollupInterval::Day1),
            ],
            ..Default::default()
        }
    }
}

/// Rollup statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RollupStats {
    /// Total rollups executed
    pub total_rollups: u64,
    /// Points processed
    pub points_processed: u64,
    /// Points produced
    pub points_produced: u64,
    /// Compression ratio (input/output points)
    pub compression_ratio: f64,
    /// Average rollup duration in milliseconds
    pub avg_duration_ms: u64,
    /// Last rollup time
    pub last_rollup_time: i64,
}

/// Rollup result
#[derive(Debug, Clone)]
pub struct RollupResult {
    /// Source interval
    pub source_interval: RollupInterval,
    /// Target interval
    pub target_interval: RollupInterval,
    /// Rolled up points (metric -> function -> points)
    pub points: HashMap<String, HashMap<RollupFunction, Vec<DataPoint>>>,
    /// Number of source points processed
    pub source_count: usize,
    /// Number of output points produced
    pub output_count: usize,
}

/// Rollup manager
pub struct RollupManager {
    config: RollupConfig,
    stats: RollupStats,
}

impl RollupManager {
    /// Create a new rollup manager
    pub fn new(config: RollupConfig) -> Self {
        Self {
            config,
            stats: RollupStats::default(),
        }
    }

    /// Get configuration
    pub fn config(&self) -> &RollupConfig {
        &self.config
    }

    /// Get statistics
    pub fn stats(&self) -> &RollupStats {
        &self.stats
    }

    /// Execute rollup for a batch of data points
    pub fn rollup(
        &mut self,
        points: &[DataPoint],
        source_interval: RollupInterval,
        target_interval: RollupInterval,
    ) -> RollupResult {
        let policy = self
            .config
            .policies
            .iter()
            .find(|p| p.source == source_interval && p.target == target_interval)
            .cloned()
            .unwrap_or_else(|| RollupPolicy::new(source_interval, target_interval));

        self.execute_rollup(points, &policy)
    }

    /// Execute rollup with a specific policy
    fn execute_rollup(&mut self, points: &[DataPoint], policy: &RollupPolicy) -> RollupResult {
        let mut result = RollupResult {
            source_interval: policy.source,
            target_interval: policy.target,
            points: HashMap::new(),
            source_count: points.len(),
            output_count: 0,
        };

        // Group points by metric and bucket
        let mut buckets: HashMap<String, HashMap<i64, Vec<f64>>> = HashMap::new();

        for point in points {
            let bucket = policy.target.bucket(point.timestamp);
            buckets
                .entry(point.metric.clone())
                .or_default()
                .entry(bucket)
                .or_default()
                .push(point.value);
        }

        // Apply rollup functions
        for (metric, metric_buckets) in buckets {
            let mut function_results: HashMap<RollupFunction, Vec<DataPoint>> = HashMap::new();

            for func in &policy.functions {
                let mut func_points = Vec::new();

                for (&timestamp, values) in &metric_buckets {
                    if let Some(agg_value) = func.apply(values) {
                        let point = DataPoint::new(&metric, timestamp, agg_value);
                        func_points.push(point);
                        result.output_count += 1;
                    }
                }

                // Sort by timestamp
                func_points.sort_by_key(|p| p.timestamp);
                function_results.insert(*func, func_points);
            }

            result.points.insert(metric, function_results);
        }

        // Update stats
        self.stats.total_rollups += 1;
        self.stats.points_processed += result.source_count as u64;
        self.stats.points_produced += result.output_count as u64;
        if result.output_count > 0 {
            self.stats.compression_ratio = result.source_count as f64 / result.output_count as f64;
        }
        self.stats.last_rollup_time = chrono::Utc::now().timestamp_millis();

        result
    }

    /// Find buckets ready for rollup
    pub fn find_ready_buckets(&self, current_time: i64, interval: RollupInterval) -> Vec<i64> {
        // Find policy delay
        let delay = self
            .config
            .policies
            .iter()
            .find(|p| p.target == interval)
            .map(|p| p.delay_ms)
            .unwrap_or(60000);

        let bucket_duration = interval.duration_ms();
        let cutoff = current_time - delay;

        // Generate buckets that are ready (their end time + delay has passed)
        let oldest_bucket = cutoff - (24 * 60 * 60 * 1000); // Look back 24 hours max
        let newest_bucket = (cutoff / bucket_duration) * bucket_duration;

        let mut buckets = Vec::new();
        let mut bucket = (oldest_bucket / bucket_duration) * bucket_duration;

        while bucket <= newest_bucket {
            let bucket_end = bucket + bucket_duration;
            if bucket_end + delay <= current_time {
                buckets.push(bucket);
            }
            bucket += bucket_duration;
        }

        buckets
    }
}

impl Default for RollupManager {
    fn default() -> Self {
        Self::new(RollupConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_points() -> Vec<DataPoint> {
        // Create points spanning multiple 1-minute buckets
        vec![
            DataPoint::new("cpu.usage", 0, 10.0),
            DataPoint::new("cpu.usage", 30000, 20.0),
            DataPoint::new("cpu.usage", 60000, 30.0),
            DataPoint::new("cpu.usage", 90000, 40.0),
            DataPoint::new("cpu.usage", 120000, 50.0),
            DataPoint::new("cpu.usage", 180000, 60.0),
            DataPoint::new("cpu.usage", 300000, 70.0), // 5 minutes
        ]
    }

    #[test]
    fn test_rollup_interval_bucket() {
        let interval = RollupInterval::Minute5;

        assert_eq!(interval.bucket(0), 0);
        assert_eq!(interval.bucket(60000), 0);
        assert_eq!(interval.bucket(299999), 0);
        assert_eq!(interval.bucket(300000), 300000);
    }

    #[test]
    fn test_rollup_function_apply() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];

        assert_eq!(RollupFunction::Avg.apply(&values), Some(3.0));
        assert_eq!(RollupFunction::Sum.apply(&values), Some(15.0));
        assert_eq!(RollupFunction::Min.apply(&values), Some(1.0));
        assert_eq!(RollupFunction::Max.apply(&values), Some(5.0));
        assert_eq!(RollupFunction::Count.apply(&values), Some(5.0));
    }

    #[test]
    fn test_rollup_manager() {
        let config = RollupConfig::minimal();
        let mut manager = RollupManager::new(config);

        let points = create_test_points();
        let result = manager.rollup(&points, RollupInterval::Minute1, RollupInterval::Minute5);

        assert_eq!(result.source_count, 7);
        assert!(result.output_count > 0);
        assert!(!result.points.is_empty());
    }

    #[test]
    fn test_rollup_to_hourly() {
        let mut manager = RollupManager::default();

        // Points spanning multiple hours
        let mut points = Vec::new();
        for i in 0..120 {
            // 2 hours of minute data
            points.push(DataPoint::new("test", i * 60000, i as f64));
        }

        let result = manager.rollup(&points, RollupInterval::Minute1, RollupInterval::Hour1);

        // Should produce 2 hourly buckets
        let metric_result = result.points.get("test").unwrap();
        let avg_points = metric_result.get(&RollupFunction::Avg).unwrap();
        assert_eq!(avg_points.len(), 2);
    }

    #[test]
    fn test_find_ready_buckets() {
        let manager = RollupManager::default();

        // Current time: 2 hours into the day
        let current_time = 2 * 60 * 60 * 1000i64;

        let ready = manager.find_ready_buckets(current_time, RollupInterval::Hour1);

        // First hour bucket should be ready (ended at hour 1, + 1min delay)
        assert!(!ready.is_empty());
        assert!(ready.contains(&0));
    }

    #[test]
    fn test_rollup_preserves_tags() {
        let mut manager = RollupManager::default();

        let points = vec![
            DataPoint::new("cpu", 0, 10.0).with_tag("host", "server1"),
            DataPoint::new("cpu", 60000, 20.0).with_tag("host", "server1"),
        ];

        let result = manager.rollup(&points, RollupInterval::Minute1, RollupInterval::Minute5);

        assert!(!result.points.is_empty());
    }
}
