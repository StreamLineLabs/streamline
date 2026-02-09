//! Storage backend traits for Streamline
//!
//! This module defines the core traits that abstract storage backends,
//! enabling pluggable implementations for local disk, object storage,
//! and hybrid configurations.
//!
//! ## Backend Hierarchy
//!
//! - `WalBackend`: Write-ahead log for durability
//! - `SegmentBackend`: Segment storage for records
//! - `BufferStrategy`: In-memory buffering for batching
//!
//! Each trait has implementations for different storage modes:
//! - Local: Traditional disk-based storage
//! - S3/Object Storage: Remote object storage
//! - Hybrid: Combination of local and remote

use crate::error::Result;
use crate::storage::record::{Record, RecordBatch};
use crate::storage::storage_mode::StorageMode;
use crate::storage::wal::WalEntry;
use async_trait::async_trait;
use bytes::Bytes;
use std::fmt::Debug;

/// Write-ahead log backend trait
///
/// Implementations provide durability guarantees by persisting records
/// before acknowledgment. Different backends offer different trade-offs:
///
/// - `LocalWalWriter`: Low latency, requires local disk
/// - `S3WalWriter`: Higher latency, no local disk required
#[async_trait]
pub trait WalBackend: Send + Sync + Debug {
    /// Append an entry to the WAL
    ///
    /// Returns the sequence number of the appended entry.
    async fn append(&mut self, entry: WalEntry) -> Result<u64>;

    /// Flush pending entries to durable storage
    ///
    /// For local storage, this performs an fsync.
    /// For object storage, this uploads buffered entries.
    async fn flush(&mut self) -> Result<()>;

    /// Read entries starting from the given sequence number
    ///
    /// Used for recovery after restart.
    async fn read_from(&self, sequence: u64) -> Result<Vec<WalEntry>>;

    /// Get the storage mode this backend operates in
    fn storage_mode(&self) -> StorageMode;

    /// Get the current sequence number (next to be written)
    fn current_sequence(&self) -> u64;

    /// Check if there are pending entries that need flushing
    fn has_pending(&self) -> bool;
}

/// Segment storage backend trait
///
/// Implementations store record batches in segments. Different backends
/// provide different persistence guarantees and performance characteristics:
///
/// - `LocalSegment`: Fast local disk storage
/// - `S3Segment`: Object storage with batching
#[async_trait]
pub trait SegmentBackend: Send + Sync + Debug {
    /// Append a record batch to the segment
    ///
    /// Returns the base offset of the appended batch.
    async fn append_batch(&mut self, batch: &RecordBatch) -> Result<i64>;

    /// Read records starting from the given offset
    ///
    /// Returns up to `max_bytes` worth of records.
    async fn read_from_offset(&self, offset: i64, max_bytes: usize) -> Result<Vec<Record>>;

    /// Seal the segment, preventing further writes
    ///
    /// For object storage backends, this may trigger a final upload.
    async fn seal(&mut self) -> Result<()>;

    /// Get the base offset of this segment
    fn base_offset(&self) -> i64;

    /// Get the log end offset (next offset to be written)
    fn log_end_offset(&self) -> i64;

    /// Get the storage mode this backend operates in
    fn storage_mode(&self) -> StorageMode;

    /// Get the size of the segment in bytes
    fn size_bytes(&self) -> u64;

    /// Check if the segment is sealed (read-only)
    fn is_sealed(&self) -> bool;

    /// Get the path or identifier for this segment
    fn identifier(&self) -> String;
}

/// Buffer configuration for controlling memory usage and batching behavior
#[derive(Debug, Clone)]
pub struct BufferConfig {
    /// Maximum buffer size in bytes before forcing a flush
    pub max_size_bytes: usize,

    /// Maximum number of records to buffer
    pub max_records: usize,

    /// Maximum time to hold records before flushing (milliseconds)
    pub max_linger_ms: u64,

    /// Enable ring buffer mode (overwrite oldest on overflow)
    pub ring_buffer_mode: bool,
}

impl Default for BufferConfig {
    fn default() -> Self {
        Self {
            max_size_bytes: 16 * 1024 * 1024, // 16MB
            max_records: 100_000,
            max_linger_ms: 100,
            ring_buffer_mode: false,
        }
    }
}

impl BufferConfig {
    /// Create a config for write-through mode (minimal buffering)
    pub fn write_through() -> Self {
        Self {
            max_size_bytes: 0,
            max_records: 0,
            max_linger_ms: 0,
            ring_buffer_mode: false,
        }
    }

    /// Create a config for batching mode (used by diskless)
    pub fn batching(batch_size_bytes: usize, batch_timeout_ms: u64) -> Self {
        Self {
            max_size_bytes: batch_size_bytes,
            max_records: 100_000,
            max_linger_ms: batch_timeout_ms,
            ring_buffer_mode: false,
        }
    }

    /// Create a config with ring buffer for hot data retention
    pub fn with_ring_buffer(size_bytes: usize) -> Self {
        Self {
            max_size_bytes: size_bytes,
            max_records: 1_000_000,
            max_linger_ms: 0,
            ring_buffer_mode: true,
        }
    }
}

/// Statistics for buffer operations
#[derive(Debug, Clone, Default)]
pub struct BufferStats {
    /// Current number of records in buffer
    pub record_count: usize,

    /// Current buffer size in bytes
    pub size_bytes: usize,

    /// Total records buffered since start
    pub total_records_buffered: u64,

    /// Total bytes buffered since start
    pub total_bytes_buffered: u64,

    /// Number of flushes performed
    pub flush_count: u64,

    /// Number of records dropped (ring buffer overflow)
    pub records_dropped: u64,
}

/// Buffer strategy trait for managing in-flight records
///
/// Different strategies provide different trade-offs:
///
/// - `WriteThrough`: No buffering, immediate writes
/// - `BatchingBuffer`: Accumulates records for efficient batch writes
/// - `RingBuffer`: Fixed-size buffer that overwrites oldest data
pub trait BufferStrategy: Send + Sync + Debug {
    /// Push records into the buffer
    ///
    /// May trigger automatic flush if thresholds are exceeded.
    fn push(&mut self, records: Vec<Record>) -> Result<()>;

    /// Check if the flush threshold has been reached
    fn should_flush(&self) -> bool;

    /// Take all buffered records for flushing
    ///
    /// Returns the records and clears the buffer.
    fn take_batch(&mut self) -> Vec<Record>;

    /// Peek at buffered records without removing them
    fn peek(&self) -> &[Record];

    /// Get current memory usage in bytes
    fn memory_usage(&self) -> usize;

    /// Get the buffer configuration
    fn config(&self) -> &BufferConfig;

    /// Get buffer statistics
    fn stats(&self) -> BufferStats;

    /// Clear all buffered records
    fn clear(&mut self);

    /// Check if the buffer is empty
    fn is_empty(&self) -> bool;

    /// Get the number of buffered records
    fn len(&self) -> usize;
}

/// Partition manifest for tracking segments in object storage
///
/// Used by diskless mode to maintain partition state without local storage.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PartitionManifest {
    /// Topic name
    pub topic: String,

    /// Partition ID
    pub partition: i32,

    /// List of segments in this partition
    pub segments: Vec<SegmentManifestEntry>,

    /// Log end offset (next offset to be written)
    pub log_end_offset: i64,

    /// High watermark (last committed offset)
    pub high_watermark: i64,

    /// Manifest version for optimistic locking
    pub version: u64,

    /// Last modified timestamp (milliseconds since epoch)
    pub last_modified: i64,
}

use serde::{Deserialize, Serialize};

/// Entry in the partition manifest describing a segment
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SegmentManifestEntry {
    /// Base offset of the segment
    pub base_offset: i64,

    /// Last offset in the segment
    pub end_offset: i64,

    /// Size of the segment in bytes
    pub size_bytes: u64,

    /// Object storage path/key for the segment
    pub object_path: String,

    /// Whether the segment is sealed (complete)
    pub sealed: bool,

    /// Creation timestamp (milliseconds since epoch)
    pub created_at: i64,

    /// Optional index object path
    pub index_path: Option<String>,
}

impl PartitionManifest {
    /// Create a new empty manifest
    pub fn new(topic: &str, partition: i32) -> Self {
        Self {
            topic: topic.to_string(),
            partition,
            segments: Vec::new(),
            log_end_offset: 0,
            high_watermark: 0,
            version: 0,
            last_modified: chrono::Utc::now().timestamp_millis(),
        }
    }

    /// Add a new segment to the manifest
    pub fn add_segment(&mut self, entry: SegmentManifestEntry) {
        if entry.end_offset >= self.log_end_offset {
            self.log_end_offset = entry.end_offset + 1;
        }
        self.segments.push(entry);
        self.version += 1;
        self.last_modified = chrono::Utc::now().timestamp_millis();
    }

    /// Find the segment containing the given offset
    pub fn find_segment(&self, offset: i64) -> Option<&SegmentManifestEntry> {
        self.segments
            .iter()
            .find(|s| offset >= s.base_offset && offset <= s.end_offset)
    }

    /// Get the object path for the manifest
    pub fn manifest_path(topic: &str, partition: i32) -> String {
        format!("v1/topics/{}/partition-{}/manifest.json", topic, partition)
    }

    /// Serialize the manifest to JSON
    pub fn to_json(&self) -> Result<Bytes> {
        let json = serde_json::to_vec_pretty(self)
            .map_err(|e| crate::error::StreamlineError::storage_msg(e.to_string()))?;
        Ok(Bytes::from(json))
    }

    /// Deserialize the manifest from JSON
    pub fn from_json(data: &[u8]) -> Result<Self> {
        serde_json::from_slice(data)
            .map_err(|e| crate::error::StreamlineError::storage_msg(e.to_string()))
    }
}

impl SegmentManifestEntry {
    /// Create a new segment manifest entry
    pub fn new(base_offset: i64, object_path: String) -> Self {
        Self {
            base_offset,
            end_offset: base_offset,
            size_bytes: 0,
            object_path,
            sealed: false,
            created_at: chrono::Utc::now().timestamp_millis(),
            index_path: None,
        }
    }

    /// Get the object path for a segment
    pub fn segment_path(topic: &str, partition: i32, base_offset: i64) -> String {
        format!(
            "v1/topics/{}/partition-{}/{:020}/data.segment",
            topic, partition, base_offset
        )
    }

    /// Get the object path for a segment index
    pub fn index_path_for(topic: &str, partition: i32, base_offset: i64) -> String {
        format!(
            "v1/topics/{}/partition-{}/{:020}/index.sparse",
            topic, partition, base_offset
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_config_default() {
        let config = BufferConfig::default();
        assert_eq!(config.max_size_bytes, 16 * 1024 * 1024);
        assert!(!config.ring_buffer_mode);
    }

    #[test]
    fn test_buffer_config_presets() {
        let write_through = BufferConfig::write_through();
        assert_eq!(write_through.max_size_bytes, 0);

        let batching = BufferConfig::batching(1024 * 1024, 100);
        assert_eq!(batching.max_size_bytes, 1024 * 1024);
        assert_eq!(batching.max_linger_ms, 100);

        let ring = BufferConfig::with_ring_buffer(8 * 1024 * 1024);
        assert!(ring.ring_buffer_mode);
    }

    #[test]
    fn test_partition_manifest_new() {
        let manifest = PartitionManifest::new("test-topic", 0);
        assert_eq!(manifest.topic, "test-topic");
        assert_eq!(manifest.partition, 0);
        assert_eq!(manifest.log_end_offset, 0);
        assert!(manifest.segments.is_empty());
    }

    #[test]
    fn test_partition_manifest_add_segment() {
        let mut manifest = PartitionManifest::new("test-topic", 0);

        let entry = SegmentManifestEntry {
            base_offset: 0,
            end_offset: 99,
            size_bytes: 1024,
            object_path: "v1/topics/test-topic/partition-0/00000000000000000000/data.segment"
                .to_string(),
            sealed: true,
            created_at: 0,
            index_path: None,
        };

        manifest.add_segment(entry);

        assert_eq!(manifest.segments.len(), 1);
        assert_eq!(manifest.log_end_offset, 100);
        assert_eq!(manifest.version, 1);
    }

    #[test]
    fn test_partition_manifest_find_segment() {
        let mut manifest = PartitionManifest::new("test-topic", 0);

        manifest.add_segment(SegmentManifestEntry {
            base_offset: 0,
            end_offset: 99,
            size_bytes: 1024,
            object_path: "segment1".to_string(),
            sealed: true,
            created_at: 0,
            index_path: None,
        });

        manifest.add_segment(SegmentManifestEntry {
            base_offset: 100,
            end_offset: 199,
            size_bytes: 1024,
            object_path: "segment2".to_string(),
            sealed: true,
            created_at: 0,
            index_path: None,
        });

        assert!(manifest.find_segment(50).is_some());
        assert_eq!(manifest.find_segment(50).unwrap().object_path, "segment1");

        assert!(manifest.find_segment(150).is_some());
        assert_eq!(manifest.find_segment(150).unwrap().object_path, "segment2");

        assert!(manifest.find_segment(200).is_none());
    }

    #[test]
    fn test_manifest_paths() {
        let path = PartitionManifest::manifest_path("my-topic", 5);
        assert_eq!(path, "v1/topics/my-topic/partition-5/manifest.json");

        let segment_path = SegmentManifestEntry::segment_path("my-topic", 5, 1000);
        assert_eq!(
            segment_path,
            "v1/topics/my-topic/partition-5/00000000000000001000/data.segment"
        );
    }

    #[test]
    fn test_manifest_serialization() {
        let mut manifest = PartitionManifest::new("test-topic", 0);
        manifest.add_segment(SegmentManifestEntry::new(
            0,
            "v1/topics/test-topic/partition-0/00000000000000000000/data.segment".to_string(),
        ));

        let json = manifest.to_json().unwrap();
        let parsed = PartitionManifest::from_json(&json).unwrap();

        assert_eq!(parsed.topic, manifest.topic);
        assert_eq!(parsed.partition, manifest.partition);
        assert_eq!(parsed.segments.len(), manifest.segments.len());
    }
}
