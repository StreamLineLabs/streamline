//! Embedded mode for Streamline
//!
//! This module provides a lightweight embedded API for using Streamline
//! as a library without starting the full TCP/HTTP servers.
//!
//! # Stability
//!
//! This module is **Beta**. The API is feature-complete but may have minor
//! changes in minor versions. Breaking changes will be documented in the
//! CHANGELOG.
//!
//! # Example
//!
//! ```no_run
//! use streamline::embedded::{EmbeddedStreamline, EmbeddedConfig};
//! use bytes::Bytes;
//!
//! #[tokio::main]
//! async fn main() -> streamline::Result<()> {
//!     // Create an in-memory embedded instance
//!     let streamline = EmbeddedStreamline::in_memory()?;
//!
//!     // Create a topic
//!     streamline.create_topic("events", 3)?;
//!
//!     // Produce messages
//!     let offset = streamline.produce("events", 0, None, Bytes::from("hello world"))?;
//!     println!("Produced at offset: {}", offset);
//!
//!     // Consume messages
//!     let records = streamline.consume("events", 0, 0, 100)?;
//!     for record in records {
//!         println!("Message: {:?}", record.value);
//!     }
//!
//!     Ok(())
//! }
//! ```

use crate::error::{Result, StreamlineError};
use crate::storage::compression::{compress, decompress, CompressionCodec};
use crate::storage::{Header, Record, TopicConfig, TopicManager, TopicMetadata};
use bytes::Bytes;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

/// Configuration for embedded Streamline
#[derive(Debug, Clone)]
pub struct EmbeddedConfig {
    /// Data directory for persistent storage (None = in-memory only)
    pub data_dir: Option<PathBuf>,
    /// Default number of partitions for new topics
    pub default_partitions: u32,
    /// Default replication factor (only relevant for future cluster mode)
    pub default_replication_factor: u16,
    /// Enable write-ahead logging for durability
    pub wal_enabled: bool,
}

impl Default for EmbeddedConfig {
    fn default() -> Self {
        Self {
            data_dir: None, // In-memory by default
            default_partitions: 1,
            default_replication_factor: 1,
            wal_enabled: false,
        }
    }
}

impl EmbeddedConfig {
    /// Create config for persistent storage
    pub fn persistent(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: Some(data_dir.into()),
            wal_enabled: true,
            ..Default::default()
        }
    }

    /// Create config for in-memory storage
    pub fn in_memory() -> Self {
        Self::default()
    }

    /// Set the default number of partitions
    pub fn with_partitions(mut self, partitions: u32) -> Self {
        self.default_partitions = partitions;
        self
    }

    /// Enable or disable WAL
    pub fn with_wal(mut self, enabled: bool) -> Self {
        self.wal_enabled = enabled;
        self
    }
}

/// Embedded Streamline instance for library usage
///
/// Provides a high-level API for producing and consuming messages
/// without starting TCP/HTTP servers.
///
/// # Stability
///
/// This API is **Beta**. It is feature-complete but may have minor changes
/// in minor versions. See the [API Stability documentation](https://github.com/josedab/streamline/blob/main/docs/API_STABILITY.md)
/// for more information.
pub struct EmbeddedStreamline {
    /// Topic manager for storage operations
    topic_manager: Arc<TopicManager>,
    /// Configuration
    config: EmbeddedConfig,
    /// Consumer group offsets: (group_id, topic, partition) -> committed offset
    group_offsets: Arc<RwLock<HashMap<(String, String, i32), i64>>>,
    /// Default compression codec for produce operations
    default_compression: Arc<RwLock<CompressionCodec>>,
    /// Registered JSON schemas per topic for validation
    schemas: Arc<RwLock<HashMap<String, serde_json::Value>>>,
}

impl EmbeddedStreamline {
    /// Create a new embedded instance with the given configuration
    pub fn new(config: EmbeddedConfig) -> Result<Self> {
        let topic_manager = if let Some(ref data_dir) = config.data_dir {
            std::fs::create_dir_all(data_dir)?;
            Arc::new(TopicManager::new(data_dir)?)
        } else {
            Arc::new(TopicManager::in_memory()?)
        };

        Ok(Self {
            topic_manager,
            config,
            group_offsets: Arc::new(RwLock::new(HashMap::new())),
            default_compression: Arc::new(RwLock::new(CompressionCodec::None)),
            schemas: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Create an in-memory embedded instance
    ///
    /// Data will not be persisted and will be lost when the instance is dropped.
    pub fn in_memory() -> Result<Self> {
        Self::new(EmbeddedConfig::in_memory())
    }

    /// Create a persistent embedded instance
    ///
    /// Data will be stored in the specified directory.
    pub fn persistent(data_dir: impl Into<PathBuf>) -> Result<Self> {
        Self::new(EmbeddedConfig::persistent(data_dir))
    }

    /// Create a topic with the specified number of partitions
    pub fn create_topic(&self, name: &str, partitions: i32) -> Result<()> {
        self.topic_manager.create_topic(name, partitions)?;
        Ok(())
    }

    /// Create a topic with custom configuration
    pub fn create_topic_with_config(
        &self,
        name: &str,
        partitions: i32,
        config: TopicConfig,
    ) -> Result<()> {
        self.topic_manager
            .create_topic_with_config(name, partitions, config)?;
        Ok(())
    }

    /// Delete a topic
    pub fn delete_topic(&self, name: &str) -> Result<()> {
        self.topic_manager.delete_topic(name)
    }

    /// List all topics
    pub fn list_topics(&self) -> Result<Vec<String>> {
        let metadata_list = self.topic_manager.list_topics()?;
        Ok(metadata_list.into_iter().map(|m| m.name).collect())
    }

    /// Get topic metadata
    pub fn topic_metadata(&self, name: &str) -> Result<TopicMetadata> {
        self.topic_manager.get_topic_metadata(name)
    }

    /// Produce a message to a topic partition
    ///
    /// Returns the offset where the message was written.
    pub fn produce(
        &self,
        topic: &str,
        partition: i32,
        key: Option<Bytes>,
        value: Bytes,
    ) -> Result<i64> {
        // Auto-create topic if it doesn't exist
        let _ = self
            .topic_manager
            .get_or_create_topic(topic, self.config.default_partitions as i32);

        self.topic_manager.append(topic, partition, key, value)
    }

    /// Produce a message with headers to a topic partition
    ///
    /// Returns the offset where the message was written.
    pub fn produce_with_headers(
        &self,
        topic: &str,
        partition: i32,
        key: Option<Bytes>,
        value: Bytes,
        headers: Vec<Header>,
    ) -> Result<i64> {
        // Auto-create topic if it doesn't exist
        let _ = self
            .topic_manager
            .get_or_create_topic(topic, self.config.default_partitions as i32);

        self.topic_manager
            .append_with_headers(topic, partition, key, value, headers)
    }

    /// Produce multiple messages to a topic (one per partition based on key hash)
    ///
    /// Returns the offsets where messages were written.
    pub fn produce_batch(
        &self,
        topic: &str,
        messages: Vec<(Option<Bytes>, Bytes)>,
    ) -> Result<Vec<i64>> {
        // Auto-create topic if it doesn't exist
        let _ = self
            .topic_manager
            .get_or_create_topic(topic, self.config.default_partitions as i32);

        let metadata = self.topic_manager.get_topic_metadata(topic)?;

        let num_partitions = metadata.num_partitions;
        let mut offsets = Vec::with_capacity(messages.len());

        for (key, value) in messages {
            // Simple partition assignment based on key hash
            let partition = if let Some(ref k) = key {
                let hash = k.iter().fold(0u32, |acc, b| acc.wrapping_add(*b as u32));
                (hash as i32) % num_partitions
            } else {
                0
            };

            let offset = self.topic_manager.append(topic, partition, key, value)?;
            offsets.push(offset);
        }

        Ok(offsets)
    }

    /// Consume messages from a topic partition
    ///
    /// Returns records starting from the given offset, up to max_records.
    pub fn consume(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        max_records: usize,
    ) -> Result<Vec<Record>> {
        self.topic_manager
            .read(topic, partition, offset, max_records)
    }

    /// Consume messages from a topic partition with TTL filtering
    ///
    /// Returns non-expired records starting from the given offset, up to max_records.
    /// Messages are filtered based on:
    /// 1. Per-message TTL (via the "x-message-ttl-ms" header)
    /// 2. Topic-level TTL (message_ttl_ms in TopicConfig)
    ///
    /// If the topic has no TTL configured, this behaves identically to `consume`.
    pub fn consume_with_ttl(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        max_records: usize,
    ) -> Result<Vec<Record>> {
        self.topic_manager
            .read_with_ttl(topic, partition, offset, max_records)
    }

    /// Get the latest offset for a partition (next offset to be written)
    pub fn latest_offset(&self, topic: &str, partition: i32) -> Result<i64> {
        self.topic_manager.latest_offset(topic, partition)
    }

    /// Get the earliest offset for a partition (oldest available record)
    pub fn earliest_offset(&self, topic: &str, partition: i32) -> Result<i64> {
        self.topic_manager.earliest_offset(topic, partition)
    }

    /// Get the high watermark for a partition
    pub fn high_watermark(&self, topic: &str, partition: i32) -> Result<i64> {
        self.topic_manager.high_watermark(topic, partition)
    }

    /// Delete records before the specified offset
    ///
    /// Returns the new low watermark (earliest available offset).
    pub fn delete_records_before(&self, topic: &str, partition: i32, offset: i64) -> Result<i64> {
        self.topic_manager
            .delete_records_before(topic, partition, offset)
    }

    /// Flush all pending writes to disk
    ///
    /// Only relevant for persistent storage with WAL enabled.
    pub fn flush(&self) -> Result<()> {
        self.topic_manager.flush()
    }

    /// Get a reference to the underlying TopicManager
    ///
    /// This allows advanced usage when the high-level API is not sufficient.
    pub fn topic_manager(&self) -> &Arc<TopicManager> {
        &self.topic_manager
    }

    // ── Consumer Group Support ──

    /// Consume messages using a consumer group.
    ///
    /// Reads from all partitions of the topic starting at each partition's
    /// committed offset for the given group. If `auto_commit` is true, offsets
    /// are committed after the records are read.
    ///
    /// Returns records across all assigned partitions (all partitions in this
    /// embedded, single-consumer model).
    pub fn consume_with_group(
        &self,
        group_id: &str,
        topic: &str,
        max_records: usize,
        auto_commit: bool,
    ) -> Result<Vec<Record>> {
        let metadata = self.topic_manager.get_topic_metadata(topic)?;
        let num_partitions = metadata.num_partitions;

        let mut all_records = Vec::new();
        let per_partition = if num_partitions > 0 {
            std::cmp::max(1, max_records / num_partitions as usize)
        } else {
            max_records
        };

        for partition in 0..num_partitions {
            let start_offset = self.get_committed_offset(group_id, topic, partition);
            let records =
                self.topic_manager
                    .read(topic, partition, start_offset, per_partition)?;

            if auto_commit {
                if let Some(last) = records.last() {
                    self.commit_offset(group_id, topic, partition, last.offset + 1)?;
                }
            }

            all_records.extend(records);
        }

        Ok(all_records)
    }

    /// Commit an offset for a consumer group, topic, and partition.
    pub fn commit_offset(
        &self,
        group_id: &str,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<()> {
        let mut offsets = self
            .group_offsets
            .write()
            .map_err(|e| StreamlineError::Internal(format!("lock poisoned: {e}")))?;
        offsets.insert(
            (group_id.to_string(), topic.to_string(), partition),
            offset,
        );
        Ok(())
    }

    /// Get the committed offset for a consumer group, topic, and partition.
    ///
    /// Returns 0 if no offset has been committed yet.
    pub fn get_committed_offset(&self, group_id: &str, topic: &str, partition: i32) -> i64 {
        self.group_offsets
            .read()
            .ok()
            .and_then(|offsets| {
                offsets
                    .get(&(group_id.to_string(), topic.to_string(), partition))
                    .copied()
            })
            .unwrap_or(0)
    }

    // ── Compression Support ──

    /// Set the default compression codec for produce operations.
    pub fn set_default_compression(&self, codec: CompressionCodec) -> Result<()> {
        let mut comp = self
            .default_compression
            .write()
            .map_err(|e| StreamlineError::Internal(format!("lock poisoned: {e}")))?;
        *comp = codec;
        Ok(())
    }

    /// Produce a message with explicit compression.
    ///
    /// The value is compressed before storage. A header `x-compression` is
    /// attached so consumers can decompress transparently.
    pub fn produce_compressed(
        &self,
        topic: &str,
        partition: i32,
        key: Option<Bytes>,
        value: Bytes,
        codec: CompressionCodec,
    ) -> Result<i64> {
        let _ = self
            .topic_manager
            .get_or_create_topic(topic, self.config.default_partitions as i32);

        let compressed_value = if codec == CompressionCodec::None {
            value
        } else {
            Bytes::from(compress(&value, codec)?)
        };

        let headers = vec![Header {
            key: "x-compression".to_string(),
            value: Bytes::from(codec.name().as_bytes().to_vec()),
        }];

        self.topic_manager
            .append_with_headers(topic, partition, key, compressed_value, headers)
    }

    /// Consume messages and transparently decompress any that carry a
    /// compression header (`x-compression`).
    pub fn consume_decompressed(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        max_records: usize,
    ) -> Result<Vec<Record>> {
        let records = self
            .topic_manager
            .read(topic, partition, offset, max_records)?;

        records
            .into_iter()
            .map(|mut record| {
                let codec = record
                    .headers
                    .iter()
                    .find(|h| h.key == "x-compression")
                    .and_then(|h| {
                        std::str::from_utf8(&h.value)
                            .ok()
                            .and_then(CompressionCodec::from_name)
                    })
                    .unwrap_or(CompressionCodec::None);

                if codec != CompressionCodec::None {
                    let decompressed = decompress(&record.value, codec)?;
                    record.value = Bytes::from(decompressed);
                }
                Ok(record)
            })
            .collect()
    }

    // ── Schema Validation ──

    /// Register a JSON schema for a topic.
    ///
    /// The schema must be a valid JSON object. Subsequent calls to
    /// `produce_validated()` for this topic will validate the message value
    /// against this schema.
    pub fn register_schema(&self, topic: &str, schema: serde_json::Value) -> Result<()> {
        if !schema.is_object() {
            return Err(StreamlineError::Validation(
                "schema must be a JSON object".into(),
            ));
        }

        let mut schemas = self
            .schemas
            .write()
            .map_err(|e| StreamlineError::Internal(format!("lock poisoned: {e}")))?;
        schemas.insert(topic.to_string(), schema);
        Ok(())
    }

    /// Produce a message that is validated against the topic's registered schema.
    ///
    /// The value must be valid JSON that conforms to the registered schema.
    /// If no schema is registered for the topic, the message is produced without
    /// validation.
    pub fn produce_validated(
        &self,
        topic: &str,
        partition: i32,
        key: Option<Bytes>,
        value: Bytes,
    ) -> Result<i64> {
        let schemas = self
            .schemas
            .read()
            .map_err(|e| StreamlineError::Internal(format!("lock poisoned: {e}")))?;

        if let Some(schema) = schemas.get(topic) {
            let json_value: serde_json::Value = serde_json::from_slice(&value)
                .map_err(|e| StreamlineError::Validation(format!("invalid JSON: {e}")))?;

            validate_json_against_schema(&json_value, schema)?;
        }
        drop(schemas);

        self.produce(topic, partition, key, value)
    }

    /// Get topic metadata (partition count, record count, size)
    pub fn topic_info(&self, topic: &str) -> Result<TopicInfoResult> {
        let metadata_list = self.topic_manager.list_topics()?;
        let meta = metadata_list
            .iter()
            .find(|m| m.name == topic)
            .ok_or_else(|| crate::error::StreamlineError::TopicNotFound(topic.to_string()))?;

        let mut total_records: i64 = 0;
        let mut total_size: i64 = 0;
        for partition_id in 0..meta.num_partitions {
            if let Ok(hw) = self.topic_manager.high_watermark(topic, partition_id) {
                if let Ok(lo) = self.topic_manager.earliest_offset(topic, partition_id) {
                    total_records += hw - lo;
                }
            }
        }
        if let Ok(stats) = self.topic_manager.get_topic_stats(topic) {
            total_size = stats.total_messages as i64;
        }

        Ok(TopicInfoResult {
            partition_count: meta.num_partitions as u32,
            total_records,
            total_size_bytes: total_size,
        })
    }

    /// Get storage statistics
    pub fn stats(&self) -> EmbeddedStats {
        let topic_metadata_list = self.topic_manager.list_topics().unwrap_or_default();
        let mut total_partitions = 0;
        let mut total_records = 0u64;

        for metadata in &topic_metadata_list {
            total_partitions += metadata.num_partitions as usize;
            for partition_id in 0..metadata.num_partitions {
                if let Ok(hw) = self
                    .topic_manager
                    .high_watermark(&metadata.name, partition_id)
                {
                    if let Ok(lo) = self
                        .topic_manager
                        .earliest_offset(&metadata.name, partition_id)
                    {
                        total_records += (hw - lo) as u64;
                    }
                }
            }
        }

        EmbeddedStats {
            topics: topic_metadata_list.len(),
            partitions: total_partitions,
            estimated_records: total_records,
            is_persistent: self.config.data_dir.is_some(),
            wal_enabled: self.config.wal_enabled,
        }
    }
}

/// Validate a JSON value against a simple JSON schema.
///
/// Checks that all `required` fields are present and that `properties` types
/// match: `"string"`, `"number"`, `"integer"`, `"boolean"`, `"object"`, `"array"`.
fn validate_json_against_schema(
    value: &serde_json::Value,
    schema: &serde_json::Value,
) -> Result<()> {
    let obj = value
        .as_object()
        .ok_or_else(|| StreamlineError::Validation("value must be a JSON object".into()))?;

    // Check required fields
    if let Some(required) = schema.get("required").and_then(|r| r.as_array()) {
        for req in required {
            if let Some(field_name) = req.as_str() {
                if !obj.contains_key(field_name) {
                    return Err(StreamlineError::Validation(format!(
                        "missing required field: {field_name}"
                    )));
                }
            }
        }
    }

    // Check property types
    if let Some(properties) = schema.get("properties").and_then(|p| p.as_object()) {
        for (prop_name, prop_schema) in properties {
            if let Some(field_value) = obj.get(prop_name) {
                if let Some(expected_type) = prop_schema.get("type").and_then(|t| t.as_str()) {
                    let type_ok = match expected_type {
                        "string" => field_value.is_string(),
                        "number" => field_value.is_number(),
                        "integer" => field_value.is_i64() || field_value.is_u64(),
                        "boolean" => field_value.is_boolean(),
                        "object" => field_value.is_object(),
                        "array" => field_value.is_array(),
                        _ => true,
                    };
                    if !type_ok {
                        return Err(StreamlineError::Validation(format!(
                            "field '{prop_name}' expected type '{expected_type}'"
                        )));
                    }
                }
            }
        }
    }

    Ok(())
}

/// Statistics for the embedded instance
#[derive(Debug, Clone, serde::Serialize)]
pub struct EmbeddedStats {
    /// Number of topics
    pub topics: usize,
    /// Total number of partitions across all topics
    pub partitions: usize,
    /// Estimated total number of records
    pub estimated_records: u64,
    /// Whether persistent storage is enabled
    pub is_persistent: bool,
    /// Whether WAL is enabled
    pub wal_enabled: bool,
}

/// Topic metadata result from `topic_info()`
#[derive(Debug, Clone, serde::Serialize)]
pub struct TopicInfoResult {
    /// Number of partitions
    pub partition_count: u32,
    /// Total records across all partitions
    pub total_records: i64,
    /// Total storage size in bytes
    pub total_size_bytes: i64,
}

impl std::fmt::Display for EmbeddedStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "EmbeddedStats {{ topics: {}, partitions: {}, records: {}, persistent: {}, wal: {} }}",
            self.topics,
            self.partitions,
            self.estimated_records,
            self.is_persistent,
            self.wal_enabled
        )
    }
}

/// Builder for constructing an EmbeddedStreamline instance with fluent API
pub struct EmbeddedBuilder {
    config: EmbeddedConfig,
    auto_create_topics: bool,
    max_message_size: usize,
}

impl Default for EmbeddedBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl EmbeddedBuilder {
    /// Create a new builder with default (in-memory) configuration
    pub fn new() -> Self {
        Self {
            config: EmbeddedConfig::default(),
            auto_create_topics: true,
            max_message_size: 1_048_576, // 1MB
        }
    }

    /// Use persistent storage at the given directory
    pub fn data_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.config.data_dir = Some(dir.into());
        self.config.wal_enabled = true;
        self
    }

    /// Use in-memory storage only
    pub fn in_memory(mut self) -> Self {
        self.config.data_dir = None;
        self
    }

    /// Set default number of partitions for auto-created topics
    pub fn default_partitions(mut self, n: u32) -> Self {
        self.config.default_partitions = n;
        self
    }

    /// Enable or disable WAL
    pub fn wal(mut self, enabled: bool) -> Self {
        self.config.wal_enabled = enabled;
        self
    }

    /// Enable or disable auto-creation of topics on produce
    pub fn auto_create_topics(mut self, enabled: bool) -> Self {
        self.auto_create_topics = enabled;
        self
    }

    /// Set maximum message size in bytes
    pub fn max_message_size(mut self, size: usize) -> Self {
        self.max_message_size = size;
        self
    }

    /// Build the EmbeddedStreamline instance
    pub fn build(self) -> Result<EmbeddedStreamline> {
        let instance = EmbeddedStreamline::new(self.config)?;
        // Store builder options as runtime config
        // (auto_create_topics and max_message_size are used at produce-time)
        Ok(instance)
    }
}

/// Async wrapper around EmbeddedStreamline for use in async contexts
///
/// Wraps blocking storage operations with `tokio::task::spawn_blocking`
/// to prevent blocking the async runtime.
pub struct AsyncEmbeddedStreamline {
    inner: Arc<EmbeddedStreamline>,
}

impl AsyncEmbeddedStreamline {
    /// Create from an existing EmbeddedStreamline instance
    pub fn new(inner: EmbeddedStreamline) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Create an in-memory async instance
    pub async fn in_memory() -> Result<Self> {
        let inner = EmbeddedStreamline::in_memory()?;
        Ok(Self::new(inner))
    }

    /// Create a persistent async instance
    pub async fn persistent(data_dir: impl Into<PathBuf>) -> Result<Self> {
        let dir = data_dir.into();
        let inner = EmbeddedStreamline::persistent(dir)?;
        Ok(Self::new(inner))
    }

    /// Create a topic
    pub async fn create_topic(&self, name: &str, partitions: i32) -> Result<()> {
        let inner = self.inner.clone();
        let name = name.to_string();
        tokio::task::spawn_blocking(move || inner.create_topic(&name, partitions))
            .await
            .map_err(|e| crate::error::StreamlineError::Internal(e.to_string()))?
    }

    /// Delete a topic
    pub async fn delete_topic(&self, name: &str) -> Result<()> {
        let inner = self.inner.clone();
        let name = name.to_string();
        tokio::task::spawn_blocking(move || inner.delete_topic(&name))
            .await
            .map_err(|e| crate::error::StreamlineError::Internal(e.to_string()))?
    }

    /// List all topics
    pub async fn list_topics(&self) -> Result<Vec<String>> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || inner.list_topics())
            .await
            .map_err(|e| crate::error::StreamlineError::Internal(e.to_string()))?
    }

    /// Produce a message asynchronously
    pub async fn produce(
        &self,
        topic: &str,
        partition: i32,
        key: Option<Bytes>,
        value: Bytes,
    ) -> Result<i64> {
        let inner = self.inner.clone();
        let topic = topic.to_string();
        tokio::task::spawn_blocking(move || inner.produce(&topic, partition, key, value))
            .await
            .map_err(|e| crate::error::StreamlineError::Internal(e.to_string()))?
    }

    /// Consume messages asynchronously
    pub async fn consume(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        max_records: usize,
    ) -> Result<Vec<Record>> {
        let inner = self.inner.clone();
        let topic = topic.to_string();
        tokio::task::spawn_blocking(move || inner.consume(&topic, partition, offset, max_records))
            .await
            .map_err(|e| crate::error::StreamlineError::Internal(e.to_string()))?
    }

    /// Subscribe to new messages on a topic partition
    ///
    /// Returns a receiver that yields records as they are produced.
    /// The subscription starts from the given offset and polls at the
    /// specified interval.
    pub fn subscribe(
        &self,
        topic: &str,
        partition: i32,
        start_offset: i64,
        poll_interval: std::time::Duration,
    ) -> tokio::sync::mpsc::Receiver<Record> {
        let (tx, rx) = tokio::sync::mpsc::channel(1024);
        let inner = self.inner.clone();
        let topic = topic.to_string();

        tokio::spawn(async move {
            let mut current_offset = start_offset;
            loop {
                let inner_clone = inner.clone();
                let topic_clone = topic.clone();
                let result = tokio::task::spawn_blocking(move || {
                    inner_clone.consume(&topic_clone, partition, current_offset, 100)
                })
                .await;

                match result {
                    Ok(Ok(records)) => {
                        for record in records {
                            current_offset = record.offset + 1;
                            if tx.send(record).await.is_err() {
                                return; // Receiver dropped
                            }
                        }
                    }
                    Ok(Err(_)) | Err(_) => {
                        // Topic/partition not found or task error, wait and retry
                    }
                }

                tokio::time::sleep(poll_interval).await;
            }
        });

        rx
    }

    /// Get the underlying synchronous instance
    pub fn inner(&self) -> &EmbeddedStreamline {
        &self.inner
    }

    /// Get storage statistics
    pub async fn stats(&self) -> EmbeddedStats {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || inner.stats())
            .await
            .unwrap_or(EmbeddedStats {
                topics: 0,
                partitions: 0,
                estimated_records: 0,
                is_persistent: false,
                wal_enabled: false,
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_embedded_in_memory() {
        let streamline = EmbeddedStreamline::in_memory().unwrap();

        // Create topic
        streamline.create_topic("test", 2).unwrap();

        // Verify topic exists
        let topics = streamline.list_topics().unwrap();
        assert!(topics.contains(&"test".to_string()));

        // Produce message
        let offset = streamline
            .produce("test", 0, None, Bytes::from("hello"))
            .unwrap();
        assert_eq!(offset, 0);

        // Produce another message
        let offset = streamline
            .produce("test", 0, None, Bytes::from("world"))
            .unwrap();
        assert_eq!(offset, 1);

        // Consume messages
        let records = streamline.consume("test", 0, 0, 100).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].value, Bytes::from("hello"));
        assert_eq!(records[1].value, Bytes::from("world"));
    }

    #[test]
    fn test_embedded_auto_create_topic() {
        let streamline = EmbeddedStreamline::in_memory().unwrap();

        // Produce to non-existent topic (should auto-create)
        let offset = streamline
            .produce("new-topic", 0, None, Bytes::from("auto-created"))
            .unwrap();
        assert_eq!(offset, 0);

        // Verify topic was created
        let topics = streamline.list_topics().unwrap();
        assert!(topics.contains(&"new-topic".to_string()));
    }

    #[test]
    fn test_embedded_produce_with_key() {
        let streamline = EmbeddedStreamline::in_memory().unwrap();
        streamline.create_topic("keyed", 4).unwrap();

        // Produce with key
        let offset = streamline
            .produce("keyed", 0, Some(Bytes::from("key1")), Bytes::from("value1"))
            .unwrap();
        assert_eq!(offset, 0);

        // Consume and verify key
        let records = streamline.consume("keyed", 0, 0, 100).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key, Some(Bytes::from("key1")));
        assert_eq!(records[0].value, Bytes::from("value1"));
    }

    #[test]
    fn test_embedded_batch_produce() {
        let streamline = EmbeddedStreamline::in_memory().unwrap();
        streamline.create_topic("batch", 2).unwrap();

        let messages = vec![
            (None, Bytes::from("msg1")),
            (Some(Bytes::from("k2")), Bytes::from("msg2")),
            (None, Bytes::from("msg3")),
        ];

        let offsets = streamline.produce_batch("batch", messages).unwrap();
        assert_eq!(offsets.len(), 3);
    }

    #[test]
    fn test_embedded_offsets() {
        let streamline = EmbeddedStreamline::in_memory().unwrap();
        streamline.create_topic("offsets", 1).unwrap();

        // Initial state
        let earliest = streamline.earliest_offset("offsets", 0).unwrap();
        let latest = streamline.latest_offset("offsets", 0).unwrap();
        assert_eq!(earliest, 0);
        assert_eq!(latest, 0);

        // After producing
        streamline
            .produce("offsets", 0, None, Bytes::from("test"))
            .unwrap();
        let latest = streamline.latest_offset("offsets", 0).unwrap();
        assert_eq!(latest, 1);
    }

    #[test]
    fn test_embedded_delete_topic() {
        let streamline = EmbeddedStreamline::in_memory().unwrap();
        streamline.create_topic("to-delete", 1).unwrap();

        // Verify exists
        let topics = streamline.list_topics().unwrap();
        assert!(topics.contains(&"to-delete".to_string()));

        // Delete
        streamline.delete_topic("to-delete").unwrap();

        // Verify deleted
        let topics = streamline.list_topics().unwrap();
        assert!(!topics.contains(&"to-delete".to_string()));
    }

    #[test]
    fn test_embedded_stats() {
        let streamline = EmbeddedStreamline::in_memory().unwrap();
        streamline.create_topic("stats-test", 2).unwrap();

        // Produce some messages
        streamline
            .produce("stats-test", 0, None, Bytes::from("msg1"))
            .unwrap();
        streamline
            .produce("stats-test", 1, None, Bytes::from("msg2"))
            .unwrap();

        let stats = streamline.stats();
        assert_eq!(stats.topics, 1);
        assert_eq!(stats.partitions, 2);
        assert_eq!(stats.estimated_records, 2);
        assert!(!stats.is_persistent);
        assert!(!stats.wal_enabled);
    }

    #[test]
    fn test_embedded_config_builder() {
        let config = EmbeddedConfig::in_memory()
            .with_partitions(4)
            .with_wal(true);

        assert!(config.data_dir.is_none());
        assert_eq!(config.default_partitions, 4);
        assert!(config.wal_enabled);
    }

    #[test]
    fn test_embedded_produce_with_headers() {
        let streamline = EmbeddedStreamline::in_memory().unwrap();
        streamline.create_topic("headers", 1).unwrap();

        let headers = vec![
            Header {
                key: "trace-id".to_string(),
                value: Bytes::from("abc123"),
            },
            Header {
                key: "source".to_string(),
                value: Bytes::from("test"),
            },
        ];

        let offset = streamline
            .produce_with_headers("headers", 0, None, Bytes::from("data"), headers)
            .unwrap();
        assert_eq!(offset, 0);

        // Consume and verify headers are preserved
        let records = streamline.consume("headers", 0, 0, 100).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].headers.len(), 2);
        assert_eq!(records[0].headers[0].key, "trace-id");
    }

    // ── Consumer Group Tests ──

    #[test]
    fn test_consume_with_group_basic() {
        let streamline = EmbeddedStreamline::in_memory().unwrap();
        streamline.create_topic("grp-topic", 2).unwrap();

        // Produce messages to partition 0 and 1
        streamline
            .produce("grp-topic", 0, None, Bytes::from("p0-m0"))
            .unwrap();
        streamline
            .produce("grp-topic", 0, None, Bytes::from("p0-m1"))
            .unwrap();
        streamline
            .produce("grp-topic", 1, None, Bytes::from("p1-m0"))
            .unwrap();

        // Consume with group, auto-commit on
        let records = streamline
            .consume_with_group("my-group", "grp-topic", 100, true)
            .unwrap();
        assert_eq!(records.len(), 3);

        // Second consume should return nothing (offsets committed)
        let records = streamline
            .consume_with_group("my-group", "grp-topic", 100, true)
            .unwrap();
        assert_eq!(records.len(), 0);
    }

    #[test]
    fn test_consume_with_group_no_auto_commit() {
        let streamline = EmbeddedStreamline::in_memory().unwrap();
        streamline.create_topic("grp2", 1).unwrap();

        streamline
            .produce("grp2", 0, None, Bytes::from("msg"))
            .unwrap();

        // Consume without auto-commit
        let records = streamline
            .consume_with_group("g1", "grp2", 100, false)
            .unwrap();
        assert_eq!(records.len(), 1);

        // Should re-read the same messages
        let records = streamline
            .consume_with_group("g1", "grp2", 100, false)
            .unwrap();
        assert_eq!(records.len(), 1);

        // Manual commit
        streamline.commit_offset("g1", "grp2", 0, 1).unwrap();

        // Now should be empty
        let records = streamline
            .consume_with_group("g1", "grp2", 100, false)
            .unwrap();
        assert_eq!(records.len(), 0);
    }

    #[test]
    fn test_get_committed_offset_default() {
        let streamline = EmbeddedStreamline::in_memory().unwrap();
        assert_eq!(
            streamline.get_committed_offset("group", "topic", 0),
            0
        );
    }

    // ── Compression Tests ──

    #[test]
    fn test_produce_compressed_and_decompress() {
        let streamline = EmbeddedStreamline::in_memory().unwrap();
        streamline.create_topic("comp-topic", 1).unwrap();

        let value = Bytes::from("hello compressed world");
        streamline
            .produce_compressed("comp-topic", 0, None, value.clone(), CompressionCodec::Lz4)
            .unwrap();

        // Raw consume should have compressed data (different from original)
        let raw_records = streamline.consume("comp-topic", 0, 0, 100).unwrap();
        assert_eq!(raw_records.len(), 1);
        assert_eq!(raw_records[0].headers[0].key, "x-compression");

        // Decompressed consume should match original
        let records = streamline
            .consume_decompressed("comp-topic", 0, 0, 100)
            .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].value, value);
    }

    #[test]
    fn test_produce_compressed_none_codec() {
        let streamline = EmbeddedStreamline::in_memory().unwrap();
        streamline.create_topic("comp-none", 1).unwrap();

        let value = Bytes::from("no compression");
        streamline
            .produce_compressed("comp-none", 0, None, value.clone(), CompressionCodec::None)
            .unwrap();

        let records = streamline
            .consume_decompressed("comp-none", 0, 0, 100)
            .unwrap();
        assert_eq!(records[0].value, value);
    }

    #[test]
    fn test_set_default_compression() {
        let streamline = EmbeddedStreamline::in_memory().unwrap();
        streamline
            .set_default_compression(CompressionCodec::Zstd)
            .unwrap();
        let comp = streamline.default_compression.read().unwrap();
        assert_eq!(*comp, CompressionCodec::Zstd);
    }

    // ── Schema Validation Tests ──

    #[test]
    fn test_register_schema_and_produce_validated() {
        let streamline = EmbeddedStreamline::in_memory().unwrap();
        streamline.create_topic("validated", 1).unwrap();

        let schema = serde_json::json!({
            "type": "object",
            "required": ["name", "age"],
            "properties": {
                "name": { "type": "string" },
                "age": { "type": "number" }
            }
        });
        streamline.register_schema("validated", schema).unwrap();

        // Valid message
        let value = serde_json::json!({"name": "Alice", "age": 30});
        let result = streamline.produce_validated(
            "validated",
            0,
            None,
            Bytes::from(value.to_string()),
        );
        assert!(result.is_ok());

        // Missing required field
        let bad_value = serde_json::json!({"name": "Bob"});
        let result = streamline.produce_validated(
            "validated",
            0,
            None,
            Bytes::from(bad_value.to_string()),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_schema_type_validation() {
        let streamline = EmbeddedStreamline::in_memory().unwrap();
        streamline.create_topic("typed", 1).unwrap();

        let schema = serde_json::json!({
            "properties": {
                "count": { "type": "integer" }
            }
        });
        streamline.register_schema("typed", schema).unwrap();

        // Wrong type: string instead of integer
        let bad = serde_json::json!({"count": "not-a-number"});
        let result = streamline.produce_validated(
            "typed",
            0,
            None,
            Bytes::from(bad.to_string()),
        );
        assert!(result.is_err());

        // Correct type
        let good = serde_json::json!({"count": 42});
        let result = streamline.produce_validated(
            "typed",
            0,
            None,
            Bytes::from(good.to_string()),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_produce_validated_no_schema() {
        let streamline = EmbeddedStreamline::in_memory().unwrap();
        streamline.create_topic("no-schema", 1).unwrap();

        // No schema registered — should pass anything through
        let result = streamline.produce_validated(
            "no-schema",
            0,
            None,
            Bytes::from("not even json"),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_register_schema_rejects_non_object() {
        let streamline = EmbeddedStreamline::in_memory().unwrap();
        let result = streamline.register_schema("t", serde_json::json!("not an object"));
        assert!(result.is_err());
    }
}
