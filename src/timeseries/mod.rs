//! Time-Series Native Storage Module
//!
//! Provides specialized storage optimizations for time-series data including:
//! - Time-based partitioning (hourly, daily, monthly)
//! - Columnar storage with compression (delta encoding, run-length)
//! - Time-range queries with index acceleration
//! - Downsampling and retention policies
//! - Automatic rollups for aggregation
//!
//! # Overview
//!
//! The time-series module enables efficient storage and retrieval of time-stamped
//! data with optimizations specifically designed for IoT, metrics, and event data.
//!
//! # Example
//!
//! ```ignore
//! use streamline::timeseries::{TimeSeriesConfig, TimeSeriesStore, TimePartition};
//!
//! let config = TimeSeriesConfig::new()
//!     .with_partition_strategy(TimePartition::Hourly)
//!     .with_compression(true)
//!     .with_retention_days(30);
//!
//! let store = TimeSeriesStore::new(config)?;
//!
//! // Write time-series data
//! store.write("metrics", timestamp, &data)?;
//!
//! // Query by time range
//! let results = store.query_range("metrics", start_time, end_time)?;
//! ```

pub mod compression;
pub mod partition;
pub mod query;
pub mod rollup;
pub mod store;
pub mod timetravel;

// Re-export main types
pub use store::TimeSeriesPoint;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A time-series data point
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataPoint {
    /// Timestamp in milliseconds
    pub timestamp: i64,
    /// Metric name/key
    pub metric: String,
    /// Tags for dimensionality
    pub tags: HashMap<String, String>,
    /// Value (numeric)
    pub value: f64,
}

impl DataPoint {
    /// Create a new data point
    pub fn new(metric: &str, timestamp: i64, value: f64) -> Self {
        Self {
            timestamp,
            metric: metric.to_string(),
            tags: HashMap::new(),
            value,
        }
    }

    /// Add a tag
    pub fn with_tag(mut self, key: &str, value: &str) -> Self {
        self.tags.insert(key.to_string(), value.to_string());
        self
    }

    /// Add multiple tags
    pub fn with_tags(mut self, tags: HashMap<String, String>) -> Self {
        self.tags.extend(tags);
        self
    }

    /// Get tag value
    pub fn tag(&self, key: &str) -> Option<&str> {
        self.tags.get(key).map(|s| s.as_str())
    }
}

/// Time-series batch for bulk operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataPointBatch {
    /// Points in the batch
    pub points: Vec<DataPoint>,
    /// Batch timestamp
    pub batch_time: i64,
}

impl DataPointBatch {
    /// Create a new batch
    pub fn new() -> Self {
        Self {
            points: Vec::new(),
            batch_time: chrono::Utc::now().timestamp_millis(),
        }
    }

    /// Add a point to the batch
    pub fn add(&mut self, point: DataPoint) {
        self.points.push(point);
    }

    /// Get number of points
    pub fn len(&self) -> usize {
        self.points.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.points.is_empty()
    }

    /// Sort by timestamp
    pub fn sort_by_time(&mut self) {
        self.points.sort_by_key(|p| p.timestamp);
    }
}

impl Default for DataPointBatch {
    fn default() -> Self {
        Self::new()
    }
}

impl std::iter::FromIterator<DataPoint> for DataPointBatch {
    fn from_iter<I: IntoIterator<Item = DataPoint>>(iter: I) -> Self {
        Self {
            points: iter.into_iter().collect(),
            batch_time: chrono::Utc::now().timestamp_millis(),
        }
    }
}

/// Time-series retention policy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionPolicy {
    /// Raw data retention in days
    pub raw_retention_days: u32,
    /// Hourly rollup retention in days
    pub hourly_retention_days: u32,
    /// Daily rollup retention in days
    pub daily_retention_days: u32,
    /// Delete data after retention period
    pub delete_on_expire: bool,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            raw_retention_days: 7,
            hourly_retention_days: 30,
            daily_retention_days: 365,
            delete_on_expire: true,
        }
    }
}

impl RetentionPolicy {
    /// Create a policy for hot data (short retention)
    pub fn hot() -> Self {
        Self {
            raw_retention_days: 1,
            hourly_retention_days: 7,
            daily_retention_days: 30,
            delete_on_expire: true,
        }
    }

    /// Create a policy for warm data (medium retention)
    pub fn warm() -> Self {
        Self {
            raw_retention_days: 7,
            hourly_retention_days: 30,
            daily_retention_days: 180,
            delete_on_expire: true,
        }
    }

    /// Create a policy for cold data (long retention)
    pub fn cold() -> Self {
        Self {
            raw_retention_days: 30,
            hourly_retention_days: 90,
            daily_retention_days: 730, // 2 years
            delete_on_expire: true,
        }
    }

    /// Create infinite retention policy
    pub fn infinite() -> Self {
        Self {
            raw_retention_days: u32::MAX,
            hourly_retention_days: u32::MAX,
            daily_retention_days: u32::MAX,
            delete_on_expire: false,
        }
    }
}

/// Time-series statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TimeSeriesStats {
    /// Total points stored
    pub total_points: u64,
    /// Total bytes on disk
    pub total_bytes: u64,
    /// Number of metrics
    pub metric_count: u64,
    /// Number of partitions
    pub partition_count: u32,
    /// Compression ratio
    pub compression_ratio: f64,
    /// Average points per second (write rate)
    pub avg_write_rate: f64,
    /// Average query latency in microseconds
    pub avg_query_latency_us: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_point_creation() {
        let point = DataPoint::new("cpu.usage", 1234567890, 45.5)
            .with_tag("host", "server1")
            .with_tag("region", "us-west");

        assert_eq!(point.metric, "cpu.usage");
        assert_eq!(point.value, 45.5);
        assert_eq!(point.tag("host"), Some("server1"));
        assert_eq!(point.tag("region"), Some("us-west"));
    }

    #[test]
    fn test_data_point_batch() {
        let mut batch = DataPointBatch::new();
        batch.add(DataPoint::new("metric1", 1000, 1.0));
        batch.add(DataPoint::new("metric1", 500, 2.0));
        batch.add(DataPoint::new("metric1", 1500, 3.0));

        assert_eq!(batch.len(), 3);

        batch.sort_by_time();
        assert_eq!(batch.points[0].timestamp, 500);
        assert_eq!(batch.points[1].timestamp, 1000);
        assert_eq!(batch.points[2].timestamp, 1500);
    }

    #[test]
    fn test_retention_policies() {
        let hot = RetentionPolicy::hot();
        assert_eq!(hot.raw_retention_days, 1);

        let cold = RetentionPolicy::cold();
        assert_eq!(cold.raw_retention_days, 30);

        let infinite = RetentionPolicy::infinite();
        assert_eq!(infinite.raw_retention_days, u32::MAX);
        assert!(!infinite.delete_on_expire);
    }
}
