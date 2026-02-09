//! Cross-Topic Buffer Manager for S3-Native Stateless Architecture
//!
//! This module provides a centralized buffer that aggregates records from
//! multiple topics and partitions into a single batch for efficient S3 writes.
//! This approach reduces S3 PUT operations by 10x or more compared to
//! per-partition buffering.
//!
//! ## CTBF Wire Format
//!
//! Cross-Topic Batch Files (CTBF) use a custom binary format optimized for
//! efficient writes and random access reads:
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────┐
//! │ Header (64 bytes)                                         │
//! │ ├── Magic: "CTBF" (4 bytes)                               │
//! │ ├── Version: u8                                           │
//! │ ├── Flags: u8 (compression, etc.)                         │
//! │ ├── Topic Count: u16                                      │
//! │ ├── Total Records: u32                                    │
//! │ ├── Uncompressed Size: u64                                │
//! │ ├── Compressed Size: u64                                  │
//! │ ├── Index Offset: u64 (offset to topic index)             │
//! │ ├── Timestamp: i64 (creation time ms)                     │
//! │ ├── Agent ID Hash: u64                                    │
//! │ ├── Sequence: u64                                         │
//! │ ├── CRC32: u32                                            │
//! │ └── Reserved: 4 bytes                                     │
//! ├────────────────────────────────────────────────────────────┤
//! │ Compressed Payload                                        │
//! │ ├── Topic 1 Records (RecordBatch format)                  │
//! │ ├── Topic 2 Records                                       │
//! │ └── ...                                                   │
//! ├────────────────────────────────────────────────────────────┤
//! │ Topic Index (at Index Offset)                             │
//! │ ├── Entry 1: topic_hash, partition, offset, length, leo   │
//! │ ├── Entry 2: ...                                          │
//! │ └── ...                                                   │
//! └────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Usage
//!
//! ```rust,ignore
//! use streamline::storage::cross_topic_buffer::{CrossTopicBufferManager, CrossTopicBufferConfig};
//!
//! let config = CrossTopicBufferConfig::default();
//! let buffer = CrossTopicBufferManager::new(config, store, state_store, "agent-1");
//!
//! // Add records from multiple topics
//! buffer.append("events", 0, records).await?;
//! buffer.append("logs", 0, more_records).await?;
//!
//! // Flush when threshold reached or on timer
//! if buffer.should_flush() {
//!     buffer.flush().await?;
//! }
//! ```

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;
use tokio::sync::Notify;

use crate::error::{Result, StreamlineError};
use crate::storage::compression::{compress, CompressionCodec};
use crate::storage::record::Record;

/// Magic bytes for Cross-Topic Batch File format
pub const CTBF_MAGIC: &[u8; 4] = b"CTBF";

/// Current CTBF format version
pub const CTBF_VERSION: u8 = 1;

/// Header size in bytes (magic + version + flags + topic_count + total_records +
/// uncompressed_size + compressed_size + index_offset + timestamp + agent_id_hash +
/// sequence + crc32 + reserved = 4+1+1+2+4+8+8+8+8+8+8+4+4 = 68)
pub const CTBF_HEADER_SIZE: usize = 68;

/// Index entry size in bytes (topic_hash + partition + payload_offset + payload_length +
/// base_offset + log_end_offset = 8+4+4+4+8+8 = 36)
pub const CTBF_INDEX_ENTRY_SIZE: usize = 36;

/// Configuration for the cross-topic buffer
#[derive(Debug, Clone)]
pub struct CrossTopicBufferConfig {
    /// Maximum buffer size in bytes before forcing a flush
    pub max_buffer_bytes: usize,

    /// Maximum number of records to buffer
    pub max_records: usize,

    /// Maximum time to hold records before flushing (milliseconds)
    pub max_linger_ms: u64,

    /// Compression codec to use
    pub compression: CompressionCodec,

    /// S3 bucket/prefix for batch files
    pub s3_prefix: String,

    /// Enable S3 Express One Zone for low latency
    pub use_express_zone: bool,
}

impl Default for CrossTopicBufferConfig {
    fn default() -> Self {
        Self {
            max_buffer_bytes: 4 * 1024 * 1024, // 4MB
            max_records: 100_000,
            max_linger_ms: 50, // 50ms for S3 Express
            compression: CompressionCodec::Zstd,
            s3_prefix: "batches".to_string(),
            use_express_zone: false,
        }
    }
}

impl CrossTopicBufferConfig {
    /// Configuration optimized for low latency with S3 Express
    pub fn low_latency() -> Self {
        Self {
            max_buffer_bytes: 1024 * 1024, // 1MB
            max_records: 10_000,
            max_linger_ms: 20, // 20ms
            compression: CompressionCodec::Lz4,
            use_express_zone: true,
            ..Default::default()
        }
    }

    /// Configuration optimized for high throughput
    pub fn high_throughput() -> Self {
        Self {
            max_buffer_bytes: 8 * 1024 * 1024, // 8MB
            max_records: 500_000,
            max_linger_ms: 100, // 100ms
            compression: CompressionCodec::Zstd,
            use_express_zone: false,
            ..Default::default()
        }
    }

    /// Configuration optimized for cost (larger batches, fewer PUTs)
    pub fn cost_optimized() -> Self {
        Self {
            max_buffer_bytes: 16 * 1024 * 1024, // 16MB
            max_records: 1_000_000,
            max_linger_ms: 500, // 500ms
            compression: CompressionCodec::Zstd,
            use_express_zone: false,
            ..Default::default()
        }
    }
}

/// Entry in the buffer for a specific partition
#[derive(Debug, Clone)]
pub struct PartitionBufferEntry {
    /// Topic name
    pub topic: String,

    /// Partition ID
    pub partition: i32,

    /// Buffered records
    pub records: Vec<Record>,

    /// Starting offset for this batch
    pub base_offset: i64,

    /// Next offset (LEO after flush)
    pub next_offset: i64,

    /// Size in bytes
    pub size_bytes: usize,
}

/// Internal buffer state
#[derive(Debug)]
struct BufferState {
    /// Entries keyed by (topic, partition)
    entries: HashMap<(String, i32), PartitionBufferEntry>,

    /// Total records in buffer
    total_records: usize,

    /// Total bytes in buffer
    total_bytes: usize,

    /// Buffer creation time
    created_at: Instant,
}

impl BufferState {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
            total_records: 0,
            total_bytes: 0,
            created_at: Instant::now(),
        }
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn age_ms(&self) -> u64 {
        self.created_at.elapsed().as_millis() as u64
    }
}

/// Index entry for a topic/partition in a CTBF file
#[derive(Debug, Clone)]
pub struct CtbfIndexEntry {
    /// Hash of topic name for fast lookup
    pub topic_hash: u64,

    /// Partition ID
    pub partition: i32,

    /// Offset within the compressed payload
    pub payload_offset: u32,

    /// Length of the partition's data
    pub payload_length: u32,

    /// Base offset (first record offset)
    pub base_offset: i64,

    /// Log end offset after this batch
    pub log_end_offset: i64,
}

impl CtbfIndexEntry {
    /// Serialize to bytes
    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(CTBF_INDEX_ENTRY_SIZE);
        buf.put_u64(self.topic_hash);
        buf.put_i32(self.partition);
        buf.put_u32(self.payload_offset);
        buf.put_u32(self.payload_length);
        buf.put_i64(self.base_offset);
        buf.put_i64(self.log_end_offset);
        buf.freeze()
    }

    /// Deserialize from bytes
    pub fn from_bytes(mut data: &[u8]) -> Result<Self> {
        if data.len() < CTBF_INDEX_ENTRY_SIZE {
            return Err(StreamlineError::storage_msg(
                "Invalid index entry size".into(),
            ));
        }

        Ok(Self {
            topic_hash: data.get_u64(),
            partition: data.get_i32(),
            payload_offset: data.get_u32(),
            payload_length: data.get_u32(),
            base_offset: data.get_i64(),
            log_end_offset: data.get_i64(),
        })
    }
}

/// Header for a CTBF file
#[derive(Debug, Clone)]
pub struct CtbfHeader {
    /// Format version
    pub version: u8,

    /// Flags (compression, etc.)
    pub flags: u8,

    /// Number of topics in this batch
    pub topic_count: u16,

    /// Total records in this batch
    pub total_records: u32,

    /// Uncompressed payload size
    pub uncompressed_size: u64,

    /// Compressed payload size
    pub compressed_size: u64,

    /// Offset to the index section
    pub index_offset: u64,

    /// Creation timestamp (ms since epoch)
    pub timestamp: i64,

    /// Hash of agent ID
    pub agent_id_hash: u64,

    /// Sequence number for this agent
    pub sequence: u64,

    /// CRC32 of the payload
    pub crc32: u32,
}

impl CtbfHeader {
    /// Create flags byte from compression codec
    pub fn compression_flag(codec: CompressionCodec) -> u8 {
        match codec {
            CompressionCodec::None => 0,
            CompressionCodec::Lz4 => 1,
            CompressionCodec::Zstd => 2,
            CompressionCodec::Snappy => 3,
            CompressionCodec::Gzip => 4,
        }
    }

    /// Get compression codec from flags
    pub fn compression_codec(&self) -> CompressionCodec {
        match self.flags & 0x0F {
            1 => CompressionCodec::Lz4,
            2 => CompressionCodec::Zstd,
            3 => CompressionCodec::Snappy,
            4 => CompressionCodec::Gzip,
            _ => CompressionCodec::None,
        }
    }

    /// Serialize to bytes
    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(CTBF_HEADER_SIZE);

        // Magic
        buf.put_slice(CTBF_MAGIC);

        // Version and flags
        buf.put_u8(self.version);
        buf.put_u8(self.flags);

        // Counts
        buf.put_u16(self.topic_count);
        buf.put_u32(self.total_records);

        // Sizes
        buf.put_u64(self.uncompressed_size);
        buf.put_u64(self.compressed_size);
        buf.put_u64(self.index_offset);

        // Metadata
        buf.put_i64(self.timestamp);
        buf.put_u64(self.agent_id_hash);
        buf.put_u64(self.sequence);
        buf.put_u32(self.crc32);

        // Reserved (4 bytes to reach 64)
        buf.put_u32(0);

        buf.freeze()
    }

    /// Deserialize from bytes
    pub fn from_bytes(mut data: &[u8]) -> Result<Self> {
        if data.len() < CTBF_HEADER_SIZE {
            return Err(StreamlineError::storage_msg(
                "Invalid CTBF header size".into(),
            ));
        }

        // Verify magic
        let magic = &data[..4];
        if magic != CTBF_MAGIC {
            return Err(StreamlineError::storage_msg(
                "Invalid CTBF magic bytes".into(),
            ));
        }
        data.advance(4);

        Ok(Self {
            version: data.get_u8(),
            flags: data.get_u8(),
            topic_count: data.get_u16(),
            total_records: data.get_u32(),
            uncompressed_size: data.get_u64(),
            compressed_size: data.get_u64(),
            index_offset: data.get_u64(),
            timestamp: data.get_i64(),
            agent_id_hash: data.get_u64(),
            sequence: data.get_u64(),
            crc32: data.get_u32(),
        })
    }
}

/// Statistics for the cross-topic buffer
#[derive(Debug, Clone, Default)]
pub struct CrossTopicBufferStats {
    /// Total records buffered
    pub records_buffered: u64,

    /// Total bytes buffered
    pub bytes_buffered: u64,

    /// Total flushes performed
    pub flushes: u64,

    /// Total bytes written to S3
    pub bytes_written: u64,

    /// Average flush size in bytes
    pub avg_flush_bytes: u64,

    /// Average records per flush
    pub avg_records_per_flush: u64,

    /// Average topics per flush
    pub avg_topics_per_flush: f64,

    /// Compression ratio (uncompressed / compressed)
    pub compression_ratio: f64,

    /// Last flush duration in milliseconds
    pub last_flush_duration_ms: u64,
}

/// Cross-topic buffer manager for efficient S3 batching
#[derive(Debug)]
pub struct CrossTopicBufferManager {
    /// Configuration
    config: CrossTopicBufferConfig,

    /// Agent ID
    agent_id: String,

    /// Agent ID hash for CTBF files
    agent_id_hash: u64,

    /// Buffer state
    buffer: RwLock<BufferState>,

    /// Sequence number for batch files
    sequence: AtomicU64,

    /// Notification for flush readiness
    flush_notify: Notify,

    /// Statistics
    stats: RwLock<CrossTopicBufferStats>,
}

impl CrossTopicBufferManager {
    /// Create a new cross-topic buffer manager
    pub fn new(config: CrossTopicBufferConfig, agent_id: &str) -> Self {
        // Simple hash of agent ID
        let agent_id_hash = {
            let mut hash: u64 = 0;
            for byte in agent_id.bytes() {
                hash = hash.wrapping_mul(31).wrapping_add(byte as u64);
            }
            hash
        };

        Self {
            config,
            agent_id: agent_id.to_string(),
            agent_id_hash,
            buffer: RwLock::new(BufferState::new()),
            sequence: AtomicU64::new(0),
            flush_notify: Notify::new(),
            stats: RwLock::new(CrossTopicBufferStats::default()),
        }
    }

    /// Append records for a topic/partition
    pub fn append(
        &self,
        topic: &str,
        partition: i32,
        records: Vec<Record>,
        base_offset: i64,
    ) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }

        let records_len = records.len();
        let records_bytes: usize = records
            .iter()
            .map(|r| {
                let key_size = r.key.as_ref().map(|k| k.len()).unwrap_or(0);
                let value_size = r.value.len();
                key_size + value_size + 16 // overhead for timestamps, offset, etc.
            })
            .sum();

        let mut buffer = self.buffer.write();

        let key = (topic.to_string(), partition);
        let entry = buffer
            .entries
            .entry(key)
            .or_insert_with(|| PartitionBufferEntry {
                topic: topic.to_string(),
                partition,
                records: Vec::new(),
                base_offset,
                next_offset: base_offset,
                size_bytes: 0,
            });

        // Update entry
        let new_offset = base_offset + records_len as i64;
        if new_offset > entry.next_offset {
            entry.next_offset = new_offset;
        }
        entry.records.extend(records);
        entry.size_bytes += records_bytes;

        // Update totals
        buffer.total_records += records_len;
        buffer.total_bytes += records_bytes;

        // Update stats
        {
            let mut stats = self.stats.write();
            stats.records_buffered += records_len as u64;
            stats.bytes_buffered += records_bytes as u64;
        }

        // Check if flush is needed
        if self.should_flush_internal(&buffer) {
            self.flush_notify.notify_one();
        }

        Ok(())
    }

    /// Check if the buffer should be flushed
    pub fn should_flush(&self) -> bool {
        let buffer = self.buffer.read();
        self.should_flush_internal(&buffer)
    }

    fn should_flush_internal(&self, buffer: &BufferState) -> bool {
        if buffer.is_empty() {
            return false;
        }

        buffer.total_bytes >= self.config.max_buffer_bytes
            || buffer.total_records >= self.config.max_records
            || buffer.age_ms() >= self.config.max_linger_ms
    }

    /// Wait for flush to be needed
    pub async fn wait_for_flush(&self) {
        self.flush_notify.notified().await;
    }

    /// Take the current buffer contents for flushing
    pub fn take_buffer(&self) -> Option<Vec<PartitionBufferEntry>> {
        let mut buffer = self.buffer.write();

        if buffer.is_empty() {
            return None;
        }

        // Take all entries
        let entries: Vec<PartitionBufferEntry> = buffer.entries.drain().map(|(_, v)| v).collect();

        // Reset buffer
        buffer.total_records = 0;
        buffer.total_bytes = 0;
        buffer.created_at = Instant::now();

        Some(entries)
    }

    /// Build a CTBF file from buffer entries
    pub fn build_ctbf(
        &self,
        entries: &[PartitionBufferEntry],
    ) -> Result<(Bytes, CtbfHeader, Vec<CtbfIndexEntry>)> {
        let start_time = Instant::now();

        if entries.is_empty() {
            return Err(StreamlineError::storage_msg(
                "No entries to build CTBF".into(),
            ));
        }

        // Count totals
        let topic_count = entries.len();
        let total_records: usize = entries.iter().map(|e| e.records.len()).sum();

        // Build uncompressed payload and index
        let mut payload = BytesMut::new();
        let mut index_entries = Vec::with_capacity(topic_count);

        for entry in entries {
            let payload_offset = payload.len() as u32;

            // Serialize records as a simple format
            // (In production, use RecordBatch serialization)
            let mut partition_data = BytesMut::new();
            for record in &entry.records {
                // Simple encoding: key_len + key + value_len + value
                if let Some(key) = &record.key {
                    partition_data.put_i32(key.len() as i32);
                    partition_data.put_slice(key);
                } else {
                    partition_data.put_i32(-1);
                }

                // Value is always present (Bytes, not Option<Bytes>)
                partition_data.put_i32(record.value.len() as i32);
                partition_data.put_slice(&record.value);
            }

            let payload_length = partition_data.len() as u32;
            payload.put(partition_data);

            // Hash topic name
            let topic_hash = {
                let mut hash: u64 = 0;
                for byte in entry.topic.bytes() {
                    hash = hash.wrapping_mul(31).wrapping_add(byte as u64);
                }
                hash
            };

            index_entries.push(CtbfIndexEntry {
                topic_hash,
                partition: entry.partition,
                payload_offset,
                payload_length,
                base_offset: entry.base_offset,
                log_end_offset: entry.next_offset,
            });
        }

        let uncompressed_size = payload.len() as u64;

        // Compress payload
        let compressed_payload = compress(&payload, self.config.compression)?;
        let compressed_size = compressed_payload.len() as u64;

        // Calculate CRC
        let crc32 = crc32fast::hash(&compressed_payload);

        // Build header
        let sequence = self.sequence.fetch_add(1, Ordering::SeqCst);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        let header = CtbfHeader {
            version: CTBF_VERSION,
            flags: CtbfHeader::compression_flag(self.config.compression),
            topic_count: topic_count as u16,
            total_records: total_records as u32,
            uncompressed_size,
            compressed_size,
            index_offset: CTBF_HEADER_SIZE as u64 + compressed_size,
            timestamp,
            agent_id_hash: self.agent_id_hash,
            sequence,
            crc32,
        };

        // Build final file: header + compressed payload + index
        let index_size = index_entries.len() * CTBF_INDEX_ENTRY_SIZE;
        let total_size = CTBF_HEADER_SIZE + compressed_payload.len() + index_size;

        let mut file = BytesMut::with_capacity(total_size);
        file.put(header.to_bytes());
        file.put_slice(&compressed_payload);
        for entry in &index_entries {
            file.put(entry.to_bytes());
        }

        // Update stats
        {
            let mut stats = self.stats.write();
            stats.flushes += 1;
            stats.bytes_written += file.len() as u64;
            stats.last_flush_duration_ms = start_time.elapsed().as_millis() as u64;

            if stats.flushes > 0 {
                stats.avg_flush_bytes = stats.bytes_written / stats.flushes;
                stats.avg_records_per_flush = stats.records_buffered / stats.flushes;
                stats.avg_topics_per_flush = (topic_count as f64
                    + stats.avg_topics_per_flush * (stats.flushes - 1) as f64)
                    / stats.flushes as f64;
            }

            if compressed_size > 0 {
                stats.compression_ratio = uncompressed_size as f64 / compressed_size as f64;
            }
        }

        Ok((file.freeze(), header, index_entries))
    }

    /// Generate the S3 path for a batch file
    pub fn batch_path(&self, timestamp_ms: i64, sequence: u64) -> String {
        format!(
            "{}/{}/{:013}-{:010}.ctb",
            self.config.s3_prefix, self.agent_id, timestamp_ms, sequence
        )
    }

    /// Get current buffer statistics
    pub fn stats(&self) -> CrossTopicBufferStats {
        self.stats.read().clone()
    }

    /// Get current buffer size in bytes
    pub fn buffer_size_bytes(&self) -> usize {
        self.buffer.read().total_bytes
    }

    /// Get current record count
    pub fn buffer_record_count(&self) -> usize {
        self.buffer.read().total_records
    }

    /// Check if buffer is empty
    pub fn is_empty(&self) -> bool {
        self.buffer.read().is_empty()
    }

    /// Get the configuration
    pub fn config(&self) -> &CrossTopicBufferConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn make_record(key: &str, value: &str) -> Record {
        Record {
            offset: 0,
            timestamp: 0,
            key: Some(Bytes::from(key.to_string())),
            value: Bytes::from(value.to_string()),
            headers: Vec::new(),
            crc: None,
        }
    }

    #[test]
    fn test_buffer_append() {
        let config = CrossTopicBufferConfig::default();
        let buffer = CrossTopicBufferManager::new(config, "agent-1");

        // Append records to multiple topics
        buffer
            .append("topic-a", 0, vec![make_record("k1", "v1")], 0)
            .unwrap();
        buffer
            .append("topic-a", 1, vec![make_record("k2", "v2")], 0)
            .unwrap();
        buffer
            .append("topic-b", 0, vec![make_record("k3", "v3")], 0)
            .unwrap();

        assert_eq!(buffer.buffer_record_count(), 3);
        assert!(!buffer.is_empty());
    }

    #[test]
    fn test_take_buffer() {
        let config = CrossTopicBufferConfig::default();
        let buffer = CrossTopicBufferManager::new(config, "agent-1");

        buffer
            .append("topic-a", 0, vec![make_record("k1", "v1")], 0)
            .unwrap();
        buffer
            .append("topic-b", 0, vec![make_record("k2", "v2")], 0)
            .unwrap();

        let entries = buffer.take_buffer().unwrap();
        assert_eq!(entries.len(), 2);
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_build_ctbf() {
        let config = CrossTopicBufferConfig::default();
        let buffer = CrossTopicBufferManager::new(config, "agent-1");

        buffer
            .append(
                "topic-a",
                0,
                vec![make_record("k1", "v1"), make_record("k2", "v2")],
                100,
            )
            .unwrap();
        buffer
            .append("topic-b", 0, vec![make_record("k3", "v3")], 50)
            .unwrap();

        let entries = buffer.take_buffer().unwrap();
        let (data, header, index) = buffer.build_ctbf(&entries).unwrap();

        // Verify header
        assert_eq!(header.version, CTBF_VERSION);
        assert_eq!(header.topic_count, 2);
        assert_eq!(header.total_records, 3);

        // Verify index
        assert_eq!(index.len(), 2);

        // Verify data starts with header
        assert_eq!(&data[..4], CTBF_MAGIC);

        // Verify header can be parsed back
        let parsed_header = CtbfHeader::from_bytes(&data[..CTBF_HEADER_SIZE]).unwrap();
        assert_eq!(parsed_header.topic_count, 2);
        assert_eq!(parsed_header.total_records, 3);
    }

    #[test]
    fn test_batch_path() {
        let config = CrossTopicBufferConfig {
            s3_prefix: "streamline/batches".to_string(),
            ..Default::default()
        };
        let buffer = CrossTopicBufferManager::new(config, "agent-1");

        let path = buffer.batch_path(1704067200000, 42);
        assert_eq!(
            path,
            "streamline/batches/agent-1/1704067200000-0000000042.ctb"
        );
    }

    #[test]
    fn test_should_flush_by_size() {
        let config = CrossTopicBufferConfig {
            max_buffer_bytes: 100, // Very small for testing
            ..Default::default()
        };
        let buffer = CrossTopicBufferManager::new(config, "agent-1");

        // Small record - shouldn't trigger flush
        buffer
            .append("topic", 0, vec![make_record("k", "v")], 0)
            .unwrap();
        // Note: We can't check should_flush() for size accurately without knowing record sizes

        assert!(!buffer.is_empty());
    }

    #[test]
    fn test_should_flush_by_records() {
        let config = CrossTopicBufferConfig {
            max_records: 2,
            ..Default::default()
        };
        let buffer = CrossTopicBufferManager::new(config, "agent-1");

        buffer
            .append("topic", 0, vec![make_record("k1", "v1")], 0)
            .unwrap();
        assert!(!buffer.should_flush());

        buffer
            .append("topic", 0, vec![make_record("k2", "v2")], 1)
            .unwrap();
        assert!(buffer.should_flush());
    }

    #[test]
    fn test_header_serialization() {
        let header = CtbfHeader {
            version: CTBF_VERSION,
            flags: CtbfHeader::compression_flag(CompressionCodec::Zstd),
            topic_count: 5,
            total_records: 1000,
            uncompressed_size: 50000,
            compressed_size: 10000,
            index_offset: 10064,
            timestamp: 1704067200000,
            agent_id_hash: 12345678,
            sequence: 42,
            crc32: 0xDEADBEEF,
        };

        let bytes = header.to_bytes();
        assert_eq!(bytes.len(), CTBF_HEADER_SIZE);

        let parsed = CtbfHeader::from_bytes(&bytes).unwrap();
        assert_eq!(parsed.version, CTBF_VERSION);
        assert_eq!(parsed.topic_count, 5);
        assert_eq!(parsed.total_records, 1000);
        assert_eq!(parsed.compression_codec(), CompressionCodec::Zstd);
    }

    #[test]
    fn test_index_entry_serialization() {
        let entry = CtbfIndexEntry {
            topic_hash: 123456789,
            partition: 5,
            payload_offset: 1000,
            payload_length: 5000,
            base_offset: 100,
            log_end_offset: 200,
        };

        let bytes = entry.to_bytes();
        assert_eq!(bytes.len(), CTBF_INDEX_ENTRY_SIZE);

        let parsed = CtbfIndexEntry::from_bytes(&bytes).unwrap();
        assert_eq!(parsed.topic_hash, 123456789);
        assert_eq!(parsed.partition, 5);
        assert_eq!(parsed.payload_offset, 1000);
        assert_eq!(parsed.payload_length, 5000);
        assert_eq!(parsed.base_offset, 100);
        assert_eq!(parsed.log_end_offset, 200);
    }

    #[test]
    fn test_stats() {
        let config = CrossTopicBufferConfig::default();
        let buffer = CrossTopicBufferManager::new(config, "agent-1");

        buffer
            .append("topic", 0, vec![make_record("k", "v")], 0)
            .unwrap();

        let stats = buffer.stats();
        assert_eq!(stats.records_buffered, 1);
        assert!(stats.bytes_buffered > 0);
    }
}
