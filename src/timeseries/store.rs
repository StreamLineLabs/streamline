//! Time-Series Store
//!
//! Main storage interface for time-series data.

use super::compression::{CompressionCodec, TimeSeriesCompressor};
use super::partition::{PartitionManager, TimePartition};
use super::query::{execute_query, QueryResult, TimeSeriesQuery};
use super::rollup::{RollupConfig, RollupManager};
use super::{DataPoint, DataPointBatch, RetentionPolicy, TimeSeriesStats};
use crate::error::Result;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Time-series store configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeSeriesConfig {
    /// Data directory
    pub data_dir: PathBuf,
    /// Partition strategy
    pub partition_strategy: TimePartition,
    /// Compression codec
    pub compression: CompressionCodec,
    /// Retention policy
    pub retention: RetentionPolicy,
    /// Rollup configuration
    pub rollup: RollupConfig,
    /// Enable in-memory caching
    pub enable_cache: bool,
    /// Cache size in MB
    pub cache_size_mb: usize,
    /// Write buffer size (points)
    pub write_buffer_size: usize,
    /// Flush interval in milliseconds
    pub flush_interval_ms: u64,
}

impl Default for TimeSeriesConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./timeseries_data"),
            partition_strategy: TimePartition::Hourly,
            compression: CompressionCodec::TimeSeriesOptimal,
            retention: RetentionPolicy::default(),
            rollup: RollupConfig::default(),
            enable_cache: true,
            cache_size_mb: 100,
            write_buffer_size: 10000,
            flush_interval_ms: 5000,
        }
    }
}

impl TimeSeriesConfig {
    /// Create a new configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set data directory
    pub fn with_data_dir(mut self, path: impl Into<PathBuf>) -> Self {
        self.data_dir = path.into();
        self
    }

    /// Set partition strategy
    pub fn with_partition_strategy(mut self, strategy: TimePartition) -> Self {
        self.partition_strategy = strategy;
        self
    }

    /// Set compression
    pub fn with_compression(mut self, enabled: bool) -> Self {
        self.compression = if enabled {
            CompressionCodec::TimeSeriesOptimal
        } else {
            CompressionCodec::None
        };
        self
    }

    /// Set compression codec
    pub fn with_compression_codec(mut self, codec: CompressionCodec) -> Self {
        self.compression = codec;
        self
    }

    /// Set retention policy
    pub fn with_retention(mut self, retention: RetentionPolicy) -> Self {
        self.retention = retention;
        self
    }

    /// Set retention days for raw data
    pub fn with_retention_days(mut self, days: u32) -> Self {
        self.retention.raw_retention_days = days;
        self
    }

    /// Enable/disable caching
    pub fn with_cache(mut self, enabled: bool) -> Self {
        self.enable_cache = enabled;
        self
    }
}

/// Write options
#[derive(Debug, Clone, Default)]
pub struct WriteOptions {
    /// Skip validation
    pub skip_validation: bool,
    /// Async write (don't wait for flush)
    pub async_write: bool,
    /// Custom tags to add to all points
    pub default_tags: HashMap<String, String>,
}

/// Time-series point (simplified version for API)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeSeriesPoint {
    /// Timestamp
    pub timestamp: i64,
    /// Value
    pub value: f64,
    /// Tags
    pub tags: HashMap<String, String>,
}

/// Time-series metrics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TimeSeriesMetrics {
    /// Write operations
    pub writes: u64,
    /// Read operations
    pub reads: u64,
    /// Points written
    pub points_written: u64,
    /// Points read
    pub points_read: u64,
    /// Write errors
    pub write_errors: u64,
    /// Read errors
    pub read_errors: u64,
    /// Bytes written
    pub bytes_written: u64,
    /// Bytes read
    pub bytes_read: u64,
    /// Cache hits
    pub cache_hits: u64,
    /// Cache misses
    pub cache_misses: u64,
}

/// Time-series writer
#[allow(dead_code)]
pub struct TimeSeriesWriter {
    config: TimeSeriesConfig,
    compressor: TimeSeriesCompressor,
    buffer: Vec<DataPoint>,
    buffer_size: usize,
}

impl TimeSeriesWriter {
    /// Create a new writer
    pub fn new(config: TimeSeriesConfig) -> Self {
        let compressor = TimeSeriesCompressor::new(config.compression);
        let buffer_size = config.write_buffer_size;

        Self {
            config,
            compressor,
            buffer: Vec::with_capacity(buffer_size),
            buffer_size,
        }
    }

    /// Write a single point
    pub fn write(&mut self, point: DataPoint) -> Result<()> {
        self.buffer.push(point);

        if self.buffer.len() >= self.buffer_size {
            self.flush()?;
        }

        Ok(())
    }

    /// Write a batch of points
    pub fn write_batch(&mut self, batch: &DataPointBatch) -> Result<()> {
        self.buffer.extend(batch.points.clone());

        if self.buffer.len() >= self.buffer_size {
            self.flush()?;
        }

        Ok(())
    }

    /// Flush buffer to storage
    pub fn flush(&mut self) -> Result<Bytes> {
        if self.buffer.is_empty() {
            return Ok(Bytes::new());
        }

        let batch = DataPointBatch::from_iter(self.buffer.drain(..));
        let compressed = self.compressor.compress(&batch);

        Ok(compressed)
    }

    /// Get buffer size
    pub fn buffer_len(&self) -> usize {
        self.buffer.len()
    }
}

/// Time-series store
#[allow(dead_code)]
pub struct TimeSeriesStore {
    config: TimeSeriesConfig,
    partition_manager: PartitionManager,
    rollup_manager: RollupManager,
    compressor: TimeSeriesCompressor,
    /// In-memory data store (metric -> points)
    data: Arc<RwLock<HashMap<String, Vec<DataPoint>>>>,
    /// Metrics
    metrics: Arc<TimeSeriesStoreMetrics>,
}

/// Atomic metrics
struct TimeSeriesStoreMetrics {
    writes: AtomicU64,
    reads: AtomicU64,
    points_written: AtomicU64,
    points_read: AtomicU64,
    write_errors: AtomicU64,
    read_errors: AtomicU64,
}

impl Default for TimeSeriesStoreMetrics {
    fn default() -> Self {
        Self {
            writes: AtomicU64::new(0),
            reads: AtomicU64::new(0),
            points_written: AtomicU64::new(0),
            points_read: AtomicU64::new(0),
            write_errors: AtomicU64::new(0),
            read_errors: AtomicU64::new(0),
        }
    }
}

impl TimeSeriesStore {
    /// Create a new time-series store
    pub fn new(config: TimeSeriesConfig) -> Result<Self> {
        let partition_manager =
            PartitionManager::new(config.partition_strategy, config.data_dir.clone());
        let rollup_manager = RollupManager::new(config.rollup.clone());
        let compressor = TimeSeriesCompressor::new(config.compression);

        Ok(Self {
            config,
            partition_manager,
            rollup_manager,
            compressor,
            data: Arc::new(RwLock::new(HashMap::new())),
            metrics: Arc::new(TimeSeriesStoreMetrics::default()),
        })
    }

    /// Create an in-memory store
    pub fn in_memory() -> Result<Self> {
        let config = TimeSeriesConfig {
            enable_cache: true,
            ..Default::default()
        };
        Self::new(config)
    }

    /// Write a single point
    pub async fn write(&self, metric: &str, point: TimeSeriesPoint) -> Result<()> {
        let data_point = DataPoint::new(metric, point.timestamp, point.value).with_tags(point.tags);
        self.write_point(data_point).await
    }

    /// Write a data point
    pub async fn write_point(&self, point: DataPoint) -> Result<()> {
        self.metrics.writes.fetch_add(1, Ordering::Relaxed);

        let mut data = self.data.write().await;
        data.entry(point.metric.clone()).or_default().push(point);

        self.metrics.points_written.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Write a batch of points
    pub async fn write_batch(&self, batch: DataPointBatch) -> Result<()> {
        self.metrics.writes.fetch_add(1, Ordering::Relaxed);

        let mut data = self.data.write().await;
        for point in batch.points {
            data.entry(point.metric.clone()).or_default().push(point);
            self.metrics.points_written.fetch_add(1, Ordering::Relaxed);
        }

        Ok(())
    }

    /// Query data
    pub async fn query(&self, query: TimeSeriesQuery) -> Result<QueryResult> {
        self.metrics.reads.fetch_add(1, Ordering::Relaxed);

        let data = self.data.read().await;

        // Collect all points matching the metric pattern
        let mut all_points = Vec::new();
        for (metric, points) in data.iter() {
            if metric.contains(&query.metric) || query.metric == "*" {
                all_points.extend(points.clone());
            }
        }

        let result = execute_query(&query, &all_points);
        self.metrics
            .points_read
            .fetch_add(result.points.len() as u64, Ordering::Relaxed);

        Ok(result)
    }

    /// Get points in a time range
    pub async fn query_range(&self, metric: &str, start: i64, end: i64) -> Result<Vec<DataPoint>> {
        let data = self.data.read().await;

        let points = data
            .get(metric)
            .map(|pts| {
                pts.iter()
                    .filter(|p| p.timestamp >= start && p.timestamp < end)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();

        Ok(points)
    }

    /// Get the latest point for a metric
    pub async fn latest(&self, metric: &str) -> Result<Option<DataPoint>> {
        let data = self.data.read().await;

        Ok(data
            .get(metric)
            .and_then(|pts| pts.iter().max_by_key(|p| p.timestamp).cloned()))
    }

    /// Get metrics for a time range with tags
    pub async fn query_with_tags(
        &self,
        metric: &str,
        start: i64,
        end: i64,
        tags: HashMap<String, String>,
    ) -> Result<Vec<DataPoint>> {
        let data = self.data.read().await;

        let points = data
            .get(metric)
            .map(|pts| {
                pts.iter()
                    .filter(|p| {
                        if p.timestamp < start || p.timestamp >= end {
                            return false;
                        }
                        tags.iter().all(|(k, v)| p.tag(k) == Some(v.as_str()))
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();

        Ok(points)
    }

    /// List all metrics
    pub async fn list_metrics(&self) -> Vec<String> {
        let data = self.data.read().await;
        data.keys().cloned().collect()
    }

    /// Get statistics
    pub async fn stats(&self) -> TimeSeriesStats {
        let data = self.data.read().await;

        let mut total_points = 0u64;
        let mut metric_count = 0u64;

        for (_, points) in data.iter() {
            total_points += points.len() as u64;
            metric_count += 1;
        }

        TimeSeriesStats {
            total_points,
            total_bytes: 0, // Would need actual storage tracking
            metric_count,
            partition_count: self.partition_manager.stats().total_partitions,
            compression_ratio: 0.0,
            avg_write_rate: 0.0,
            avg_query_latency_us: 0,
        }
    }

    /// Get metrics snapshot
    pub fn metrics(&self) -> TimeSeriesMetrics {
        TimeSeriesMetrics {
            writes: self.metrics.writes.load(Ordering::Relaxed),
            reads: self.metrics.reads.load(Ordering::Relaxed),
            points_written: self.metrics.points_written.load(Ordering::Relaxed),
            points_read: self.metrics.points_read.load(Ordering::Relaxed),
            write_errors: self.metrics.write_errors.load(Ordering::Relaxed),
            read_errors: self.metrics.read_errors.load(Ordering::Relaxed),
            ..Default::default()
        }
    }

    /// Delete old data based on retention policy
    pub async fn enforce_retention(&self) -> Result<usize> {
        let retention_ms = self.config.retention.raw_retention_days as i64 * 24 * 60 * 60 * 1000;
        let cutoff = chrono::Utc::now().timestamp_millis() - retention_ms;

        let mut data = self.data.write().await;
        let mut deleted = 0;

        for points in data.values_mut() {
            let before = points.len();
            points.retain(|p| p.timestamp >= cutoff);
            deleted += before - points.len();
        }

        Ok(deleted)
    }

    /// Compact data (remove duplicates, sort)
    pub async fn compact(&self) -> Result<()> {
        let mut data = self.data.write().await;

        for points in data.values_mut() {
            // Sort by timestamp
            points.sort_by_key(|p| p.timestamp);

            // Remove duplicates (same timestamp and value)
            points.dedup_by(|a, b| {
                a.timestamp == b.timestamp && (a.value - b.value).abs() < f64::EPSILON
            });
        }

        Ok(())
    }

    /// Get configuration
    pub fn config(&self) -> &TimeSeriesConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_timeseries_store_write_read() {
        let store = TimeSeriesStore::in_memory().unwrap();

        let point = TimeSeriesPoint {
            timestamp: 1000,
            value: 45.5,
            tags: HashMap::new(),
        };

        store.write("cpu.usage", point).await.unwrap();

        let result = store.query_range("cpu.usage", 0, 2000).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, 45.5);
    }

    #[tokio::test]
    async fn test_timeseries_store_batch_write() {
        let store = TimeSeriesStore::in_memory().unwrap();

        let mut batch = DataPointBatch::new();
        batch.add(DataPoint::new("cpu", 1000, 10.0));
        batch.add(DataPoint::new("cpu", 2000, 20.0));
        batch.add(DataPoint::new("cpu", 3000, 30.0));

        store.write_batch(batch).await.unwrap();

        let result = store.query_range("cpu", 0, 5000).await.unwrap();
        assert_eq!(result.len(), 3);
    }

    #[tokio::test]
    async fn test_timeseries_store_query() {
        let store = TimeSeriesStore::in_memory().unwrap();

        for i in 0..100 {
            let point = DataPoint::new("test", i * 1000, i as f64);
            store.write_point(point).await.unwrap();
        }

        let query = TimeSeriesQuery::new("test", super::super::query::TimeRange::new(0, 50000));
        let result = store.query(query).await.unwrap();

        assert_eq!(result.points.len(), 50);
    }

    #[tokio::test]
    async fn test_timeseries_store_latest() {
        let store = TimeSeriesStore::in_memory().unwrap();

        store
            .write_point(DataPoint::new("test", 1000, 1.0))
            .await
            .unwrap();
        store
            .write_point(DataPoint::new("test", 3000, 3.0))
            .await
            .unwrap();
        store
            .write_point(DataPoint::new("test", 2000, 2.0))
            .await
            .unwrap();

        let latest = store.latest("test").await.unwrap().unwrap();
        assert_eq!(latest.timestamp, 3000);
        assert_eq!(latest.value, 3.0);
    }

    #[tokio::test]
    async fn test_timeseries_store_retention() {
        let store = TimeSeriesStore::in_memory().unwrap();

        // Write old and new data
        let now = chrono::Utc::now().timestamp_millis();
        let old = now - (10 * 24 * 60 * 60 * 1000); // 10 days ago

        store
            .write_point(DataPoint::new("test", old, 1.0))
            .await
            .unwrap();
        store
            .write_point(DataPoint::new("test", now, 2.0))
            .await
            .unwrap();

        // Default retention is 7 days
        let deleted = store.enforce_retention().await.unwrap();
        assert_eq!(deleted, 1);

        let result = store.query_range("test", 0, now + 1000).await.unwrap();
        assert_eq!(result.len(), 1);
    }

    #[tokio::test]
    async fn test_timeseries_writer() {
        let config = TimeSeriesConfig {
            write_buffer_size: 3,
            ..Default::default()
        };
        let mut writer = TimeSeriesWriter::new(config);

        writer.write(DataPoint::new("test", 1000, 1.0)).unwrap();
        writer.write(DataPoint::new("test", 2000, 2.0)).unwrap();
        assert_eq!(writer.buffer_len(), 2);

        writer.write(DataPoint::new("test", 3000, 3.0)).unwrap();
        // Should have flushed
        assert_eq!(writer.buffer_len(), 0);
    }

    #[test]
    fn test_timeseries_config_builder() {
        let config = TimeSeriesConfig::new()
            .with_data_dir("/tmp/ts")
            .with_partition_strategy(TimePartition::Daily)
            .with_compression(true)
            .with_retention_days(30);

        assert_eq!(config.data_dir, PathBuf::from("/tmp/ts"));
        assert_eq!(config.partition_strategy, TimePartition::Daily);
        assert_eq!(config.retention.raw_retention_days, 30);
    }
}
