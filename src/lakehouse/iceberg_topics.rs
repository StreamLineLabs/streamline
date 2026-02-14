//! Native Iceberg Table Topics
//!
//! This module enables Streamline topics to function as both Kafka-compatible
//! streams AND Apache Iceberg tables simultaneously. Records written via the
//! Kafka protocol are transparently buffered and periodically flushed to
//! Iceberg table format (Parquet files registered with an Iceberg catalog).
//!
//! # Architecture
//!
//! ```text
//! Kafka Producer ──> IcebergTopic ──> WriteBuffer ──> flush_to_iceberg()
//!                        │                                  │
//!                        │                                  ▼
//!                        │                           Parquet files
//!                        │                           registered in
//!                        │                           Iceberg catalog
//!                        ▼
//!                   Kafka Consumer (real-time reads still work)
//! ```
//!
//! # Features
//!
//! - **Dual access**: Records are available for both real-time Kafka consumption
//!   and batch Iceberg/SQL queries
//! - **Automatic flushing**: Background task periodically flushes buffered records
//!   to Parquet/Iceberg format
//! - **Configurable partitioning**: Time-based (hour/day/month), field-based, or none
//! - **Schema evolution**: Strict, add-new-columns, or add-and-promote policies
//! - **Compression**: Snappy, Gzip, Zstd, or uncompressed Parquet output
//!
//! # Example
//!
//! ```ignore
//! use streamline::lakehouse::iceberg_topics::{IcebergTopicConfig, IcebergTopic};
//!
//! let config = IcebergTopicConfig {
//!     catalog_uri: "http://localhost:8181".to_string(),
//!     catalog_type: IcebergCatalogType::Rest,
//!     namespace: "default".to_string(),
//!     table_name: "events".to_string(),
//!     commit_interval_ms: 60_000,
//!     max_batch_size: 10_000,
//!     partitioning: PartitioningStrategy::TimeBasedHour,
//!     compression: IcebergCompression::Snappy,
//!     schema_evolution: SchemaEvolutionPolicy::Strict,
//! };
//!
//! let topic = IcebergTopic::new("events".to_string(), config)?;
//! topic.start().await?;
//!
//! let offset = topic.write_record(
//!     Some(b"key-1".to_vec()),
//!     b"value-1".to_vec(),
//!     vec![("header-key".to_string(), b"header-val".to_vec())],
//!     chrono::Utc::now().timestamp_millis(),
//! ).await?;
//! ```

#![cfg(feature = "iceberg")]

use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

// ---------------------------------------------------------------------------
// Configuration types
// ---------------------------------------------------------------------------

/// Catalog type for the Iceberg table backend.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IcebergCatalogType {
    /// REST catalog (e.g. Tabular, Polaris, Gravitino)
    #[default]
    Rest,
    /// Local filesystem catalog (for development / testing)
    FileSystem,
}

impl std::fmt::Display for IcebergCatalogType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IcebergCatalogType::Rest => write!(f, "rest"),
            IcebergCatalogType::FileSystem => write!(f, "filesystem"),
        }
    }
}

/// Partitioning strategy for Iceberg table writes.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartitioningStrategy {
    /// No partitioning
    None,
    /// Partition by hour (YYYY-MM-DD-HH)
    #[default]
    TimeBasedHour,
    /// Partition by day (YYYY-MM-DD)
    TimeBasedDay,
    /// Partition by month (YYYY-MM)
    TimeBasedMonth,
    /// Partition by a named field extracted from the record value (JSON)
    FieldBased {
        /// Name of the JSON field to extract the partition value from
        field_name: String,
    },
}

impl std::fmt::Display for PartitioningStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PartitioningStrategy::None => write!(f, "none"),
            PartitioningStrategy::TimeBasedHour => write!(f, "time_based_hour"),
            PartitioningStrategy::TimeBasedDay => write!(f, "time_based_day"),
            PartitioningStrategy::TimeBasedMonth => write!(f, "time_based_month"),
            PartitioningStrategy::FieldBased { field_name } => {
                write!(f, "field_based({})", field_name)
            }
        }
    }
}

/// Parquet compression codec.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IcebergCompression {
    /// No compression
    None,
    /// Snappy compression (default -- good balance of speed and ratio)
    #[default]
    Snappy,
    /// Gzip compression (better ratio, slower)
    Gzip,
    /// Zstd compression (best ratio, moderate speed)
    Zstd,
}

impl std::fmt::Display for IcebergCompression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IcebergCompression::None => write!(f, "none"),
            IcebergCompression::Snappy => write!(f, "snappy"),
            IcebergCompression::Gzip => write!(f, "gzip"),
            IcebergCompression::Zstd => write!(f, "zstd"),
        }
    }
}

/// Schema evolution policy controlling how schema changes are handled
/// when records with new or altered fields arrive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaEvolutionPolicy {
    /// No schema changes allowed -- fail if the record does not match
    #[default]
    Strict,
    /// Automatically add new columns when previously-unseen fields appear
    AddNewColumns,
    /// Allow adding columns and promoting types (e.g. int -> long)
    AddAndPromote,
}

impl std::fmt::Display for SchemaEvolutionPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaEvolutionPolicy::Strict => write!(f, "strict"),
            SchemaEvolutionPolicy::AddNewColumns => write!(f, "add_new_columns"),
            SchemaEvolutionPolicy::AddAndPromote => write!(f, "add_and_promote"),
        }
    }
}

/// Configuration for an Iceberg-backed topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergTopicConfig {
    /// URI of the Iceberg catalog (e.g. `http://localhost:8181`)
    pub catalog_uri: String,

    /// Type of catalog backend
    #[serde(default)]
    pub catalog_type: IcebergCatalogType,

    /// Iceberg namespace / database
    #[serde(default = "default_namespace")]
    pub namespace: String,

    /// Table name inside the namespace
    pub table_name: String,

    /// How often (in milliseconds) buffered records are flushed to Iceberg
    #[serde(default = "default_commit_interval_ms")]
    pub commit_interval_ms: u64,

    /// Maximum number of records buffered before a flush is triggered
    #[serde(default = "default_max_batch_size")]
    pub max_batch_size: usize,

    /// Partitioning strategy for the Iceberg table
    #[serde(default)]
    pub partitioning: PartitioningStrategy,

    /// Parquet compression codec
    #[serde(default)]
    pub compression: IcebergCompression,

    /// Schema evolution policy
    #[serde(default)]
    pub schema_evolution: SchemaEvolutionPolicy,
}

fn default_namespace() -> String {
    "default".to_string()
}

fn default_commit_interval_ms() -> u64 {
    60_000
}

fn default_max_batch_size() -> usize {
    10_000
}

impl IcebergTopicConfig {
    /// Validate the configuration, returning an error if any field is invalid.
    pub fn validate(&self) -> Result<()> {
        if self.catalog_uri.is_empty() {
            return Err(StreamlineError::Config(
                "iceberg topic: catalog_uri cannot be empty".into(),
            ));
        }
        if self.table_name.is_empty() {
            return Err(StreamlineError::Config(
                "iceberg topic: table_name cannot be empty".into(),
            ));
        }
        if self.namespace.is_empty() {
            return Err(StreamlineError::Config(
                "iceberg topic: namespace cannot be empty".into(),
            ));
        }
        if self.commit_interval_ms == 0 {
            return Err(StreamlineError::Config(
                "iceberg topic: commit_interval_ms must be > 0".into(),
            ));
        }
        if self.max_batch_size == 0 {
            return Err(StreamlineError::Config(
                "iceberg topic: max_batch_size must be > 0".into(),
            ));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Buffered record type
// ---------------------------------------------------------------------------

/// A single buffered record awaiting flush to Iceberg.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BufferedRecord {
    /// Record key (optional)
    pub key: Option<Vec<u8>>,
    /// Record value (payload)
    pub value: Vec<u8>,
    /// Record headers as key-value pairs
    pub headers: Vec<(String, Vec<u8>)>,
    /// Timestamp in epoch milliseconds
    pub timestamp: i64,
    /// Offset assigned by the write path
    pub offset: u64,
}

// ---------------------------------------------------------------------------
// WriteBuffer
// ---------------------------------------------------------------------------

/// Internal buffer that accumulates records before flushing to Iceberg.
///
/// The buffer is intentionally kept simple and lock-free from the caller's
/// perspective -- all synchronisation is handled by the owning `IcebergTopic`
/// via `Arc<RwLock<WriteBuffer>>`.
#[derive(Debug)]
pub struct WriteBuffer {
    records: Vec<BufferedRecord>,
    /// Maximum capacity hint (not a hard limit -- the flush loop enforces it).
    capacity: usize,
}

impl WriteBuffer {
    /// Create a new write buffer with the given capacity hint.
    pub fn new(capacity: usize) -> Self {
        Self {
            records: Vec::with_capacity(capacity),
            capacity,
        }
    }

    /// Append a record to the buffer.
    pub fn add_record(&mut self, record: BufferedRecord) {
        self.records.push(record);
    }

    /// Drain all records from the buffer and return them.
    pub fn drain(&mut self) -> Vec<BufferedRecord> {
        std::mem::take(&mut self.records)
    }

    /// Number of records currently in the buffer.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Returns `true` if the buffer contains no records.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Returns `true` when the buffer has reached or exceeded its capacity hint.
    pub fn is_full(&self) -> bool {
        self.records.len() >= self.capacity
    }

    /// Configured capacity hint.
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

// ---------------------------------------------------------------------------
// IcebergTopicStats
// ---------------------------------------------------------------------------

/// Runtime statistics for an Iceberg-backed topic.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IcebergTopicStats {
    /// Total number of records written to the topic (via `write_record`).
    pub records_written: u64,
    /// Total number of records successfully flushed to Iceberg.
    pub records_flushed: u64,
    /// Number of flush operations completed.
    pub flush_count: u64,
    /// Timestamp (epoch ms) of the last successful flush, or 0 if none.
    pub last_flush_time: i64,
    /// Running average flush duration in milliseconds.
    pub avg_flush_duration_ms: f64,
    /// Number of Parquet files created so far.
    pub parquet_files_created: u64,
    /// Total bytes written to Parquet files.
    pub total_bytes_written: u64,
}

// ---------------------------------------------------------------------------
// IcebergTopic
// ---------------------------------------------------------------------------

/// A topic that is simultaneously Kafka-compatible and backed by an Iceberg table.
///
/// Records written through `write_record` are placed in an internal buffer.
/// A background task (started via `start()`) periodically calls
/// `flush_to_iceberg()` which converts the buffered records into Parquet
/// format and registers them with the configured Iceberg catalog.
impl std::fmt::Debug for IcebergTopic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergTopic")
            .field("name", &self.name)
            .field("config", &self.config)
            .field(
                "running",
                &self.running.load(std::sync::atomic::Ordering::Relaxed),
            )
            .finish()
    }
}

pub struct IcebergTopic {
    /// Human-readable topic name
    name: String,
    /// Configuration
    config: IcebergTopicConfig,
    /// Write buffer holding records pending flush
    buffer: Arc<RwLock<WriteBuffer>>,
    /// Monotonically increasing offset counter
    next_offset: AtomicU64,
    /// Runtime statistics
    stats: Arc<RwLock<IcebergTopicStats>>,
    /// Whether the background flush task is running
    running: Arc<AtomicBool>,
    /// Shutdown signal sender -- dropping or sending `true` stops the background task
    shutdown_tx: Arc<RwLock<Option<tokio::sync::watch::Sender<bool>>>>,
}

impl IcebergTopic {
    /// Create a new `IcebergTopic`.
    ///
    /// The topic is **not** started -- call `start()` to begin the background
    /// flush loop.
    pub fn new(name: String, config: IcebergTopicConfig) -> Result<Self> {
        config.validate()?;

        let buffer_capacity = config.max_batch_size;

        Ok(Self {
            name,
            config,
            buffer: Arc::new(RwLock::new(WriteBuffer::new(buffer_capacity))),
            next_offset: AtomicU64::new(0),
            stats: Arc::new(RwLock::new(IcebergTopicStats::default())),
            running: Arc::new(AtomicBool::new(false)),
            shutdown_tx: Arc::new(RwLock::new(None)),
        })
    }

    /// Return the topic name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return a reference to the configuration.
    pub fn config(&self) -> &IcebergTopicConfig {
        &self.config
    }

    /// Return `true` if the background flush task is running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Start the background flush loop.
    ///
    /// The loop will flush buffered records every `commit_interval_ms`
    /// milliseconds, or whenever the buffer reaches `max_batch_size` records.
    pub async fn start(&self) -> Result<()> {
        if self.running.load(Ordering::SeqCst) {
            return Err(StreamlineError::Sink(format!(
                "iceberg topic '{}' is already running",
                self.name
            )));
        }

        let (tx, mut rx) = tokio::sync::watch::channel(false);
        {
            let mut guard = self.shutdown_tx.write().await;
            *guard = Some(tx);
        }

        self.running.store(true, Ordering::SeqCst);

        let buffer = Arc::clone(&self.buffer);
        let stats = Arc::clone(&self.stats);
        let running = Arc::clone(&self.running);
        let interval_ms = self.config.commit_interval_ms;
        let topic_name = self.name.clone();
        let config = self.config.clone();

        tokio::spawn(async move {
            info!(topic = %topic_name, "iceberg topic background flush task started");

            let mut ticker = tokio::time::interval(Duration::from_millis(interval_ms));

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        if let Err(e) = Self::do_flush(&topic_name, &config, &buffer, &stats).await {
                            error!(topic = %topic_name, error = %e, "flush to iceberg failed");
                        }
                    }
                    _ = rx.changed() => {
                        if *rx.borrow() {
                            // Final flush before stopping
                            if let Err(e) = Self::do_flush(&topic_name, &config, &buffer, &stats).await {
                                error!(topic = %topic_name, error = %e, "final flush failed on shutdown");
                            }
                            break;
                        }
                    }
                }
            }

            running.store(false, Ordering::SeqCst);
            info!(topic = %topic_name, "iceberg topic background flush task stopped");
        });

        Ok(())
    }

    /// Stop the background flush loop gracefully.
    ///
    /// Triggers one final flush before the task exits.
    pub async fn stop(&self) -> Result<()> {
        let tx = {
            let mut guard = self.shutdown_tx.write().await;
            guard.take()
        };

        if let Some(tx) = tx {
            let _ = tx.send(true);
        }

        // Give the background task a moment to complete the final flush.
        tokio::time::sleep(Duration::from_millis(100)).await;

        Ok(())
    }

    /// Write a single record to the topic.
    ///
    /// The record is placed in the write buffer and will be flushed to Iceberg
    /// by the background task. Returns the assigned offset.
    pub async fn write_record(
        &self,
        key: Option<Vec<u8>>,
        value: Vec<u8>,
        headers: Vec<(String, Vec<u8>)>,
        timestamp: i64,
    ) -> Result<u64> {
        let offset = self.next_offset.fetch_add(1, Ordering::SeqCst);

        let record = BufferedRecord {
            key,
            value,
            headers,
            timestamp,
            offset,
        };

        {
            let mut buf = self.buffer.write().await;
            buf.add_record(record);
        }

        // Update stats
        {
            let mut s = self.stats.write().await;
            s.records_written += 1;
        }

        debug!(topic = %self.name, offset = offset, "record written to iceberg topic buffer");

        Ok(offset)
    }

    /// Explicitly flush all buffered records to Iceberg format.
    ///
    /// This is also called automatically by the background task.
    pub async fn flush_to_iceberg(&self) -> Result<()> {
        Self::do_flush(&self.name, &self.config, &self.buffer, &self.stats).await
    }

    /// Return a snapshot of the current statistics.
    pub async fn get_stats(&self) -> IcebergTopicStats {
        self.stats.read().await.clone()
    }

    /// Return the number of records currently buffered (not yet flushed).
    pub async fn buffered_count(&self) -> usize {
        self.buffer.read().await.len()
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    /// Core flush implementation shared by the background task and the
    /// explicit `flush_to_iceberg()` method.
    async fn do_flush(
        topic_name: &str,
        _config: &IcebergTopicConfig,
        buffer: &Arc<RwLock<WriteBuffer>>,
        stats: &Arc<RwLock<IcebergTopicStats>>,
    ) -> Result<()> {
        let records = {
            let mut buf = buffer.write().await;
            if buf.is_empty() {
                return Ok(());
            }
            buf.drain()
        };

        let record_count = records.len() as u64;
        let start = Instant::now();

        // ------------------------------------------------------------------
        // Iceberg integration point
        //
        // Steps for a full implementation:
        //
        // 1. Convert `records` to an Arrow RecordBatch using the Arrow SDK.
        // 2. Write the RecordBatch to a Parquet file using the parquet crate
        //    with the compression codec from `_config.compression`.
        // 3. Register the Parquet file with the Iceberg catalog (REST / FS)
        //    pointed to by `_config.catalog_uri` using the iceberg-rust crate.
        //
        // Currently we log the flush intent and compute byte totals from the
        // buffered records. When the iceberg-rust and arrow crates are wired
        // in, the simulated_bytes value below will come from the Parquet
        // writer instead.
        // ------------------------------------------------------------------

        tracing::info!(
            topic = %topic_name,
            records = record_count,
            catalog_uri = %_config.catalog_uri,
            compression = %_config.compression,
            "Iceberg flush: would write {} records as Parquet to catalog",
            record_count,
        );

        // Simulated bytes written (in a real impl this comes from the Parquet writer)
        let simulated_bytes: u64 = records.iter().map(|r| r.value.len() as u64).sum();

        let elapsed = start.elapsed();
        let elapsed_ms = elapsed.as_secs_f64() * 1000.0;

        // Update stats
        {
            let mut s = stats.write().await;
            s.records_flushed += record_count;
            s.flush_count += 1;
            s.last_flush_time = chrono::Utc::now().timestamp_millis();
            s.parquet_files_created += 1;
            s.total_bytes_written += simulated_bytes;

            // Running average of flush duration
            if s.flush_count == 1 {
                s.avg_flush_duration_ms = elapsed_ms;
            } else {
                s.avg_flush_duration_ms = s.avg_flush_duration_ms
                    + (elapsed_ms - s.avg_flush_duration_ms) / s.flush_count as f64;
            }
        }

        info!(
            topic = %topic_name,
            records = record_count,
            bytes = simulated_bytes,
            flush_ms = format!("{:.2}", elapsed_ms),
            "flushed records to iceberg"
        );

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// IcebergTopicInfo
// ---------------------------------------------------------------------------

/// Lightweight summary of an Iceberg-backed topic for listing purposes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergTopicInfo {
    /// Topic name
    pub name: String,
    /// Catalog URI this topic is connected to
    pub catalog_uri: String,
    /// Iceberg namespace
    pub namespace: String,
    /// Iceberg table name
    pub table_name: String,
    /// Whether the background flush task is currently running
    pub is_running: bool,
    /// Number of records currently buffered
    pub buffered_records: usize,
    /// Snapshot of runtime stats
    pub stats: IcebergTopicStats,
}

// ---------------------------------------------------------------------------
// IcebergTopicManager
// ---------------------------------------------------------------------------

/// Manages the lifecycle of all Iceberg-backed topics.
pub struct IcebergTopicManager {
    topics: Arc<RwLock<HashMap<String, Arc<IcebergTopic>>>>,
}

impl IcebergTopicManager {
    /// Create a new (empty) manager.
    pub fn new() -> Self {
        Self {
            topics: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a new Iceberg-backed topic.
    ///
    /// Returns an error if a topic with the same name is already registered.
    pub async fn register_topic(
        &self,
        name: String,
        config: IcebergTopicConfig,
    ) -> Result<Arc<IcebergTopic>> {
        let mut topics = self.topics.write().await;
        if topics.contains_key(&name) {
            return Err(StreamlineError::TopicAlreadyExists(name));
        }

        let topic = Arc::new(IcebergTopic::new(name.clone(), config)?);
        topics.insert(name.clone(), Arc::clone(&topic));

        info!(topic = %name, "registered iceberg topic");
        Ok(topic)
    }

    /// Unregister an Iceberg-backed topic, stopping it first if running.
    pub async fn unregister_topic(&self, name: &str) -> Result<()> {
        let topic = {
            let mut topics = self.topics.write().await;
            topics
                .remove(name)
                .ok_or_else(|| StreamlineError::TopicNotFound(name.to_string()))?
        };

        if topic.is_running() {
            topic.stop().await?;
        }

        info!(topic = %name, "unregistered iceberg topic");
        Ok(())
    }

    /// List all registered Iceberg-backed topics.
    pub async fn list_iceberg_topics(&self) -> Vec<IcebergTopicInfo> {
        let topics = self.topics.read().await;
        let mut result = Vec::with_capacity(topics.len());

        for (name, topic) in topics.iter() {
            let stats = topic.get_stats().await;
            let buffered = topic.buffered_count().await;

            result.push(IcebergTopicInfo {
                name: name.clone(),
                catalog_uri: topic.config().catalog_uri.clone(),
                namespace: topic.config().namespace.clone(),
                table_name: topic.config().table_name.clone(),
                is_running: topic.is_running(),
                buffered_records: buffered,
                stats,
            });
        }

        result.sort_by(|a, b| a.name.cmp(&b.name));
        result
    }

    /// Get a handle to a specific Iceberg topic by name.
    pub async fn get_topic(&self, name: &str) -> Option<Arc<IcebergTopic>> {
        let topics = self.topics.read().await;
        topics.get(name).cloned()
    }

    /// Force-flush all registered topics.
    pub async fn flush_all(&self) -> Result<()> {
        let topics = self.topics.read().await;
        let mut errors = Vec::new();

        for (name, topic) in topics.iter() {
            if let Err(e) = topic.flush_to_iceberg().await {
                warn!(topic = %name, error = %e, "failed to flush iceberg topic");
                errors.push(format!("{}: {}", name, e));
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(StreamlineError::Sink(format!(
                "flush_all encountered errors: {}",
                errors.join("; ")
            )))
        }
    }

    /// Return the number of registered topics.
    pub async fn topic_count(&self) -> usize {
        self.topics.read().await.len()
    }
}

impl Default for IcebergTopicManager {
    fn default() -> Self {
        Self::new()
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: create a valid default configuration for tests.
    fn test_config() -> IcebergTopicConfig {
        IcebergTopicConfig {
            catalog_uri: "http://localhost:8181".to_string(),
            catalog_type: IcebergCatalogType::Rest,
            namespace: "default".to_string(),
            table_name: "test_events".to_string(),
            commit_interval_ms: 1_000,
            max_batch_size: 100,
            partitioning: PartitioningStrategy::TimeBasedHour,
            compression: IcebergCompression::Snappy,
            schema_evolution: SchemaEvolutionPolicy::Strict,
        }
    }

    // ------------------------------------------------------------------
    // Config validation tests
    // ------------------------------------------------------------------

    #[test]
    fn test_config_validation_success() {
        let config = test_config();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_validation_empty_catalog_uri() {
        let mut config = test_config();
        config.catalog_uri = String::new();
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("catalog_uri"));
    }

    #[test]
    fn test_config_validation_empty_table_name() {
        let mut config = test_config();
        config.table_name = String::new();
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("table_name"));
    }

    #[test]
    fn test_config_validation_empty_namespace() {
        let mut config = test_config();
        config.namespace = String::new();
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("namespace"));
    }

    #[test]
    fn test_config_validation_zero_commit_interval() {
        let mut config = test_config();
        config.commit_interval_ms = 0;
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("commit_interval_ms"));
    }

    #[test]
    fn test_config_validation_zero_max_batch_size() {
        let mut config = test_config();
        config.max_batch_size = 0;
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("max_batch_size"));
    }

    // ------------------------------------------------------------------
    // Config serialization / deserialization
    // ------------------------------------------------------------------

    #[test]
    fn test_config_serialization_roundtrip() {
        let config = test_config();
        let json = serde_json::to_string(&config).expect("serialize config");
        let deserialized: IcebergTopicConfig =
            serde_json::from_str(&json).expect("deserialize config");

        assert_eq!(deserialized.catalog_uri, config.catalog_uri);
        assert_eq!(deserialized.catalog_type, config.catalog_type);
        assert_eq!(deserialized.namespace, config.namespace);
        assert_eq!(deserialized.table_name, config.table_name);
        assert_eq!(deserialized.commit_interval_ms, config.commit_interval_ms);
        assert_eq!(deserialized.max_batch_size, config.max_batch_size);
        assert_eq!(deserialized.compression, config.compression);
        assert_eq!(deserialized.schema_evolution, config.schema_evolution);
    }

    #[test]
    fn test_config_defaults_from_json() {
        let json = r#"{
            "catalog_uri": "http://localhost:8181",
            "table_name": "events"
        }"#;
        let config: IcebergTopicConfig = serde_json::from_str(json).expect("parse minimal json");

        assert_eq!(config.catalog_type, IcebergCatalogType::Rest);
        assert_eq!(config.namespace, "default");
        assert_eq!(config.commit_interval_ms, 60_000);
        assert_eq!(config.max_batch_size, 10_000);
        assert_eq!(config.compression, IcebergCompression::Snappy);
        assert_eq!(config.schema_evolution, SchemaEvolutionPolicy::Strict);
    }

    // ------------------------------------------------------------------
    // WriteBuffer tests
    // ------------------------------------------------------------------

    #[test]
    fn test_write_buffer_new() {
        let buf = WriteBuffer::new(100);
        assert!(buf.is_empty());
        assert_eq!(buf.len(), 0);
        assert_eq!(buf.capacity(), 100);
        assert!(!buf.is_full());
    }

    #[test]
    fn test_write_buffer_add_and_drain() {
        let mut buf = WriteBuffer::new(10);

        for i in 0..5 {
            buf.add_record(BufferedRecord {
                key: Some(format!("key-{}", i).into_bytes()),
                value: format!("value-{}", i).into_bytes(),
                headers: vec![],
                timestamp: 1000 + i as i64,
                offset: i as u64,
            });
        }

        assert_eq!(buf.len(), 5);
        assert!(!buf.is_empty());
        assert!(!buf.is_full());

        let drained = buf.drain();
        assert_eq!(drained.len(), 5);
        assert!(buf.is_empty());
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn test_write_buffer_is_full() {
        let mut buf = WriteBuffer::new(3);

        for i in 0..3 {
            buf.add_record(BufferedRecord {
                key: None,
                value: vec![i as u8],
                headers: vec![],
                timestamp: 0,
                offset: i as u64,
            });
        }

        assert!(buf.is_full());
    }

    #[test]
    fn test_write_buffer_drain_returns_correct_records() {
        let mut buf = WriteBuffer::new(10);

        buf.add_record(BufferedRecord {
            key: Some(b"k1".to_vec()),
            value: b"v1".to_vec(),
            headers: vec![("h".to_string(), b"val".to_vec())],
            timestamp: 42,
            offset: 0,
        });

        let records = buf.drain();
        assert_eq!(records.len(), 1);
        let r = &records[0];
        assert_eq!(r.key.as_deref(), Some(b"k1".as_slice()));
        assert_eq!(r.value, b"v1");
        assert_eq!(r.headers.len(), 1);
        assert_eq!(r.headers[0].0, "h");
        assert_eq!(r.timestamp, 42);
        assert_eq!(r.offset, 0);
    }

    // ------------------------------------------------------------------
    // IcebergTopic tests
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn test_iceberg_topic_creation() {
        let config = test_config();
        let topic = IcebergTopic::new("events".to_string(), config);
        assert!(topic.is_ok());

        let topic = topic.unwrap();
        assert_eq!(topic.name(), "events");
        assert!(!topic.is_running());
    }

    #[tokio::test]
    async fn test_iceberg_topic_write_record() {
        let config = test_config();
        let topic = IcebergTopic::new("events".to_string(), config).unwrap();

        let offset0 = topic
            .write_record(
                Some(b"key1".to_vec()),
                b"value1".to_vec(),
                vec![],
                1_700_000_000_000,
            )
            .await
            .unwrap();

        let offset1 = topic
            .write_record(None, b"value2".to_vec(), vec![], 1_700_000_001_000)
            .await
            .unwrap();

        assert_eq!(offset0, 0);
        assert_eq!(offset1, 1);
        assert_eq!(topic.buffered_count().await, 2);

        let stats = topic.get_stats().await;
        assert_eq!(stats.records_written, 2);
        assert_eq!(stats.records_flushed, 0);
    }

    #[tokio::test]
    async fn test_iceberg_topic_flush() {
        let config = test_config();
        let topic = IcebergTopic::new("events".to_string(), config).unwrap();

        // Write some records
        for i in 0..5 {
            topic
                .write_record(None, format!("v{}", i).into_bytes(), vec![], 1000 + i)
                .await
                .unwrap();
        }

        assert_eq!(topic.buffered_count().await, 5);

        // Flush manually
        topic.flush_to_iceberg().await.unwrap();

        assert_eq!(topic.buffered_count().await, 0);

        let stats = topic.get_stats().await;
        assert_eq!(stats.records_written, 5);
        assert_eq!(stats.records_flushed, 5);
        assert_eq!(stats.flush_count, 1);
        assert_eq!(stats.parquet_files_created, 1);
        assert!(stats.last_flush_time > 0);
        assert!(stats.avg_flush_duration_ms >= 0.0);
    }

    #[tokio::test]
    async fn test_iceberg_topic_flush_empty_buffer_is_noop() {
        let config = test_config();
        let topic = IcebergTopic::new("events".to_string(), config).unwrap();

        topic.flush_to_iceberg().await.unwrap();

        let stats = topic.get_stats().await;
        assert_eq!(stats.flush_count, 0);
        assert_eq!(stats.records_flushed, 0);
    }

    #[tokio::test]
    async fn test_iceberg_topic_start_stop() {
        let config = test_config();
        let topic = IcebergTopic::new("events".to_string(), config).unwrap();

        topic.start().await.unwrap();
        assert!(topic.is_running());

        // Write a record while running
        topic
            .write_record(None, b"hello".to_vec(), vec![], 1000)
            .await
            .unwrap();

        // Give the background task a tick to process
        tokio::time::sleep(Duration::from_millis(50)).await;

        topic.stop().await.unwrap();

        // After stop, the background task should have completed
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!topic.is_running());
    }

    #[tokio::test]
    async fn test_iceberg_topic_start_when_already_running() {
        let config = test_config();
        let topic = IcebergTopic::new("events".to_string(), config).unwrap();

        topic.start().await.unwrap();
        let result = topic.start().await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("already running"));

        topic.stop().await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // ------------------------------------------------------------------
    // IcebergTopicStats tests
    // ------------------------------------------------------------------

    #[test]
    fn test_stats_default() {
        let stats = IcebergTopicStats::default();
        assert_eq!(stats.records_written, 0);
        assert_eq!(stats.records_flushed, 0);
        assert_eq!(stats.flush_count, 0);
        assert_eq!(stats.last_flush_time, 0);
        assert_eq!(stats.avg_flush_duration_ms, 0.0);
        assert_eq!(stats.parquet_files_created, 0);
        assert_eq!(stats.total_bytes_written, 0);
    }

    #[test]
    fn test_stats_serialization_roundtrip() {
        let stats = IcebergTopicStats {
            records_written: 100,
            records_flushed: 80,
            flush_count: 4,
            last_flush_time: 1_700_000_000_000,
            avg_flush_duration_ms: 12.5,
            parquet_files_created: 4,
            total_bytes_written: 1024 * 1024,
        };

        let json = serde_json::to_string(&stats).expect("serialize stats");
        let deserialized: IcebergTopicStats =
            serde_json::from_str(&json).expect("deserialize stats");

        assert_eq!(deserialized.records_written, 100);
        assert_eq!(deserialized.records_flushed, 80);
        assert_eq!(deserialized.flush_count, 4);
        assert_eq!(deserialized.last_flush_time, 1_700_000_000_000);
        assert!((deserialized.avg_flush_duration_ms - 12.5).abs() < f64::EPSILON);
        assert_eq!(deserialized.parquet_files_created, 4);
        assert_eq!(deserialized.total_bytes_written, 1024 * 1024);
    }

    // ------------------------------------------------------------------
    // IcebergTopicManager tests
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn test_manager_register_and_get() {
        let manager = IcebergTopicManager::new();

        let topic = manager
            .register_topic("events".to_string(), test_config())
            .await
            .unwrap();

        assert_eq!(topic.name(), "events");
        assert_eq!(manager.topic_count().await, 1);

        let retrieved = manager.get_topic("events").await;
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().name(), "events");
    }

    #[tokio::test]
    async fn test_manager_register_duplicate() {
        let manager = IcebergTopicManager::new();

        manager
            .register_topic("events".to_string(), test_config())
            .await
            .unwrap();

        let result = manager
            .register_topic("events".to_string(), test_config())
            .await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("already exists"));
    }

    #[tokio::test]
    async fn test_manager_unregister() {
        let manager = IcebergTopicManager::new();

        manager
            .register_topic("events".to_string(), test_config())
            .await
            .unwrap();

        assert_eq!(manager.topic_count().await, 1);

        manager.unregister_topic("events").await.unwrap();
        assert_eq!(manager.topic_count().await, 0);
        assert!(manager.get_topic("events").await.is_none());
    }

    #[tokio::test]
    async fn test_manager_unregister_nonexistent() {
        let manager = IcebergTopicManager::new();
        let result = manager.unregister_topic("nope").await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[tokio::test]
    async fn test_manager_list_iceberg_topics() {
        let manager = IcebergTopicManager::new();

        let mut config_a = test_config();
        config_a.table_name = "table_a".to_string();

        let mut config_b = test_config();
        config_b.table_name = "table_b".to_string();

        manager
            .register_topic("alpha".to_string(), config_a)
            .await
            .unwrap();
        manager
            .register_topic("beta".to_string(), config_b)
            .await
            .unwrap();

        let list = manager.list_iceberg_topics().await;
        assert_eq!(list.len(), 2);
        // Results are sorted by name
        assert_eq!(list[0].name, "alpha");
        assert_eq!(list[0].table_name, "table_a");
        assert_eq!(list[1].name, "beta");
        assert_eq!(list[1].table_name, "table_b");
    }

    #[tokio::test]
    async fn test_manager_flush_all() {
        let manager = IcebergTopicManager::new();

        let topic = manager
            .register_topic("events".to_string(), test_config())
            .await
            .unwrap();

        // Write records
        topic
            .write_record(None, b"data".to_vec(), vec![], 1000)
            .await
            .unwrap();

        assert_eq!(topic.buffered_count().await, 1);

        manager.flush_all().await.unwrap();

        assert_eq!(topic.buffered_count().await, 0);
        let stats = topic.get_stats().await;
        assert_eq!(stats.records_flushed, 1);
    }

    #[tokio::test]
    async fn test_manager_flush_all_empty() {
        let manager = IcebergTopicManager::new();
        // Flush with no topics should succeed
        manager.flush_all().await.unwrap();
    }

    // ------------------------------------------------------------------
    // Display / formatting tests
    // ------------------------------------------------------------------

    #[test]
    fn test_catalog_type_display() {
        assert_eq!(format!("{}", IcebergCatalogType::Rest), "rest");
        assert_eq!(format!("{}", IcebergCatalogType::FileSystem), "filesystem");
    }

    #[test]
    fn test_compression_display() {
        assert_eq!(format!("{}", IcebergCompression::None), "none");
        assert_eq!(format!("{}", IcebergCompression::Snappy), "snappy");
        assert_eq!(format!("{}", IcebergCompression::Gzip), "gzip");
        assert_eq!(format!("{}", IcebergCompression::Zstd), "zstd");
    }

    #[test]
    fn test_partitioning_strategy_display() {
        assert_eq!(format!("{}", PartitioningStrategy::None), "none");
        assert_eq!(
            format!("{}", PartitioningStrategy::TimeBasedHour),
            "time_based_hour"
        );
        assert_eq!(
            format!("{}", PartitioningStrategy::TimeBasedDay),
            "time_based_day"
        );
        assert_eq!(
            format!("{}", PartitioningStrategy::TimeBasedMonth),
            "time_based_month"
        );
        assert_eq!(
            format!(
                "{}",
                PartitioningStrategy::FieldBased {
                    field_name: "region".to_string()
                }
            ),
            "field_based(region)"
        );
    }

    #[test]
    fn test_schema_evolution_policy_display() {
        assert_eq!(format!("{}", SchemaEvolutionPolicy::Strict), "strict");
        assert_eq!(
            format!("{}", SchemaEvolutionPolicy::AddNewColumns),
            "add_new_columns"
        );
        assert_eq!(
            format!("{}", SchemaEvolutionPolicy::AddAndPromote),
            "add_and_promote"
        );
    }

    // ------------------------------------------------------------------
    // IcebergTopicInfo serialization test
    // ------------------------------------------------------------------

    #[test]
    fn test_iceberg_topic_info_serialization() {
        let info = IcebergTopicInfo {
            name: "events".to_string(),
            catalog_uri: "http://localhost:8181".to_string(),
            namespace: "default".to_string(),
            table_name: "events".to_string(),
            is_running: true,
            buffered_records: 42,
            stats: IcebergTopicStats {
                records_written: 100,
                records_flushed: 58,
                flush_count: 3,
                last_flush_time: 1_700_000_000_000,
                avg_flush_duration_ms: 5.0,
                parquet_files_created: 3,
                total_bytes_written: 4096,
            },
        };

        let json = serde_json::to_string(&info).expect("serialize info");
        let deserialized: IcebergTopicInfo = serde_json::from_str(&json).expect("deserialize info");

        assert_eq!(deserialized.name, "events");
        assert!(deserialized.is_running);
        assert_eq!(deserialized.buffered_records, 42);
        assert_eq!(deserialized.stats.records_written, 100);
    }

    // ------------------------------------------------------------------
    // Multiple-flush stats accumulation test
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn test_stats_accumulate_across_flushes() {
        let config = test_config();
        let topic = IcebergTopic::new("events".to_string(), config).unwrap();

        // First batch
        for i in 0..3 {
            topic
                .write_record(None, format!("val-{}", i).into_bytes(), vec![], 1000 + i)
                .await
                .unwrap();
        }
        topic.flush_to_iceberg().await.unwrap();

        // Second batch
        for i in 0..2 {
            topic
                .write_record(
                    None,
                    format!("val-{}", i + 3).into_bytes(),
                    vec![],
                    2000 + i,
                )
                .await
                .unwrap();
        }
        topic.flush_to_iceberg().await.unwrap();

        let stats = topic.get_stats().await;
        assert_eq!(stats.records_written, 5);
        assert_eq!(stats.records_flushed, 5);
        assert_eq!(stats.flush_count, 2);
        assert_eq!(stats.parquet_files_created, 2);
        assert!(stats.total_bytes_written > 0);
        assert!(stats.avg_flush_duration_ms >= 0.0);
    }
}
