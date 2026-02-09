//! Batching buffer for object storage
//!
//! This buffer accumulates records until size or time thresholds are reached,
//! enabling efficient batch writes to object storage.

use crate::error::Result;
use crate::storage::backend::{BufferConfig, BufferStats, BufferStrategy};
use crate::storage::record::Record;
use std::fmt::Debug;
use std::time::Instant;

/// Batching buffer for object storage modes (Hybrid/Diskless)
///
/// Accumulates records until either:
/// - The size threshold is reached (`batch_size_bytes`)
/// - The time threshold is reached (`batch_timeout_ms`)
///
/// This enables efficient batch uploads to object storage while
/// bounding latency.
#[derive(Debug)]
pub(crate) struct BatchingBuffer {
    /// Buffered records
    records: Vec<Record>,

    /// Current size in bytes
    size_bytes: usize,

    /// Buffer configuration
    config: BufferConfig,

    /// Time when the first record was added to the current batch
    batch_start: Option<Instant>,

    /// Statistics
    stats: BufferStatsInternal,
}

#[derive(Debug, Default)]
struct BufferStatsInternal {
    total_records_buffered: u64,
    total_bytes_buffered: u64,
    flush_count: u64,
    records_dropped: u64,
}

impl BatchingBuffer {
    /// Create a new batching buffer with specified thresholds
    ///
    /// # Arguments
    /// * `batch_size_bytes` - Flush when buffer reaches this size
    /// * `batch_timeout_ms` - Flush after this time even if size not reached
    pub fn new(batch_size_bytes: usize, batch_timeout_ms: u64) -> Self {
        Self {
            records: Vec::new(),
            size_bytes: 0,
            config: BufferConfig::batching(batch_size_bytes, batch_timeout_ms),
            batch_start: None,
            stats: BufferStatsInternal::default(),
        }
    }

    /// Create a batching buffer from configuration
    pub fn from_config(config: BufferConfig) -> Self {
        Self {
            records: Vec::new(),
            size_bytes: 0,
            config,
            batch_start: None,
            stats: BufferStatsInternal::default(),
        }
    }

    /// Create a batching buffer with default settings
    pub fn with_defaults() -> Self {
        Self::new(
            1024 * 1024, // 1MB default batch size
            100,         // 100ms default timeout
        )
    }

    /// Check if the time threshold has been exceeded
    fn is_timeout_exceeded(&self) -> bool {
        if let Some(start) = self.batch_start {
            let elapsed_ms = start.elapsed().as_millis() as u64;
            elapsed_ms >= self.config.max_linger_ms
        } else {
            false
        }
    }

    /// Check if the size threshold has been exceeded
    fn is_size_exceeded(&self) -> bool {
        self.size_bytes >= self.config.max_size_bytes
    }

    /// Check if the record count threshold has been exceeded
    fn is_count_exceeded(&self) -> bool {
        self.records.len() >= self.config.max_records
    }

    /// Estimate the size of a record in bytes
    fn estimate_record_size(record: &Record) -> usize {
        // Base overhead for record metadata
        let mut size = 32;

        // Key size
        if let Some(ref key) = record.key {
            size += key.len();
        }

        // Value size
        size += record.value.len();

        // Headers size
        for header in &record.headers {
            size += header.key.len();
            size += header.value.len();
        }

        size
    }

    /// Get the time elapsed since the batch started
    pub fn batch_age_ms(&self) -> u64 {
        self.batch_start
            .map(|start| start.elapsed().as_millis() as u64)
            .unwrap_or(0)
    }

    /// Get the configured batch size threshold
    pub fn batch_size_threshold(&self) -> usize {
        self.config.max_size_bytes
    }

    /// Get the configured batch timeout threshold
    pub fn batch_timeout_threshold(&self) -> u64 {
        self.config.max_linger_ms
    }

    /// Get the percentage of batch size filled
    pub fn fill_percentage(&self) -> f64 {
        if self.config.max_size_bytes == 0 {
            0.0
        } else {
            (self.size_bytes as f64 / self.config.max_size_bytes as f64) * 100.0
        }
    }
}

impl Default for BatchingBuffer {
    fn default() -> Self {
        Self::with_defaults()
    }
}

impl BufferStrategy for BatchingBuffer {
    fn push(&mut self, records: Vec<Record>) -> Result<()> {
        // Start batch timer on first record
        if self.records.is_empty() {
            self.batch_start = Some(Instant::now());
        }

        for record in records {
            let size = Self::estimate_record_size(&record);

            // Check if we need to drop records (ring buffer mode)
            if self.config.ring_buffer_mode && self.size_bytes + size > self.config.max_size_bytes {
                // Drop oldest records to make room
                while self.size_bytes + size > self.config.max_size_bytes
                    && !self.records.is_empty()
                {
                    if let Some(old_record) = self.records.first() {
                        let old_size = Self::estimate_record_size(old_record);
                        self.size_bytes = self.size_bytes.saturating_sub(old_size);
                        self.stats.records_dropped += 1;
                    }
                    self.records.remove(0);
                }
            }

            self.size_bytes += size;
            self.stats.total_bytes_buffered += size as u64;
            self.stats.total_records_buffered += 1;
            self.records.push(record);
        }

        Ok(())
    }

    fn should_flush(&self) -> bool {
        if self.records.is_empty() {
            return false;
        }

        // Check all thresholds
        self.is_size_exceeded() || self.is_count_exceeded() || self.is_timeout_exceeded()
    }

    fn take_batch(&mut self) -> Vec<Record> {
        self.stats.flush_count += 1;
        self.size_bytes = 0;
        self.batch_start = None;
        std::mem::take(&mut self.records)
    }

    fn peek(&self) -> &[Record] {
        &self.records
    }

    fn memory_usage(&self) -> usize {
        self.size_bytes
    }

    fn config(&self) -> &BufferConfig {
        &self.config
    }

    fn stats(&self) -> BufferStats {
        BufferStats {
            record_count: self.records.len(),
            size_bytes: self.size_bytes,
            total_records_buffered: self.stats.total_records_buffered,
            total_bytes_buffered: self.stats.total_bytes_buffered,
            flush_count: self.stats.flush_count,
            records_dropped: self.stats.records_dropped,
        }
    }

    fn clear(&mut self) {
        self.records.clear();
        self.size_bytes = 0;
        self.batch_start = None;
    }

    fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    fn len(&self) -> usize {
        self.records.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::thread;
    use std::time::Duration;

    fn create_test_record(value: &str) -> Record {
        Record {
            offset: 0,
            timestamp: chrono::Utc::now().timestamp_millis(),
            key: None,
            value: Bytes::from(value.to_string()),
            headers: Vec::new(),
            crc: None,
        }
    }

    fn create_sized_record(size: usize) -> Record {
        Record {
            offset: 0,
            timestamp: chrono::Utc::now().timestamp_millis(),
            key: None,
            value: Bytes::from(vec![0u8; size]),
            headers: Vec::new(),
            crc: None,
        }
    }

    #[test]
    fn test_batching_buffer_new() {
        let buffer = BatchingBuffer::new(1024, 100);
        assert!(buffer.is_empty());
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.memory_usage(), 0);
        assert_eq!(buffer.batch_size_threshold(), 1024);
        assert_eq!(buffer.batch_timeout_threshold(), 100);
    }

    #[test]
    fn test_batching_buffer_push() {
        let mut buffer = BatchingBuffer::new(1024 * 1024, 1000);

        buffer
            .push(vec![
                create_test_record("hello"),
                create_test_record("world"),
            ])
            .unwrap();

        assert!(!buffer.is_empty());
        assert_eq!(buffer.len(), 2);
        assert!(buffer.memory_usage() > 0);
    }

    #[test]
    fn test_batching_buffer_size_threshold() {
        let mut buffer = BatchingBuffer::new(100, 10000); // 100 bytes threshold

        // Small record - shouldn't trigger flush
        buffer.push(vec![create_test_record("hi")]).unwrap();
        assert!(!buffer.should_flush());

        // Large record - should trigger flush
        buffer.push(vec![create_sized_record(100)]).unwrap();
        assert!(buffer.should_flush());
    }

    #[test]
    fn test_batching_buffer_timeout_threshold() {
        let mut buffer = BatchingBuffer::new(1024 * 1024, 10); // 10ms timeout

        buffer.push(vec![create_test_record("timeout")]).unwrap();

        // Initially shouldn't flush
        assert!(!buffer.should_flush());

        // Wait for timeout
        thread::sleep(Duration::from_millis(20));

        // Now should flush
        assert!(buffer.should_flush());
    }

    #[test]
    fn test_batching_buffer_take_batch() {
        let mut buffer = BatchingBuffer::new(1024, 100);

        buffer
            .push(vec![create_test_record("one"), create_test_record("two")])
            .unwrap();

        let batch = buffer.take_batch();

        assert_eq!(batch.len(), 2);
        assert!(buffer.is_empty());
        assert_eq!(buffer.memory_usage(), 0);
        assert_eq!(buffer.batch_age_ms(), 0);
    }

    #[test]
    fn test_batching_buffer_fill_percentage() {
        let mut buffer = BatchingBuffer::new(1000, 100);

        // Empty buffer
        assert_eq!(buffer.fill_percentage(), 0.0);

        // Add some data (approximately 50% of threshold)
        // Note: estimate_record_size adds ~32 bytes overhead
        buffer.push(vec![create_sized_record(450)]).unwrap();

        let fill = buffer.fill_percentage();
        assert!(fill > 40.0 && fill < 60.0);
    }

    #[test]
    fn test_batching_buffer_stats() {
        let mut buffer = BatchingBuffer::new(1024 * 1024, 1000);

        buffer.push(vec![create_test_record("stats")]).unwrap();
        buffer.take_batch();
        buffer.push(vec![create_test_record("more")]).unwrap();

        let stats = buffer.stats();
        assert_eq!(stats.total_records_buffered, 2);
        assert_eq!(stats.flush_count, 1);
        assert_eq!(stats.record_count, 1);
    }

    #[test]
    fn test_batching_buffer_ring_mode() {
        let mut config = BufferConfig::with_ring_buffer(200);
        config.max_size_bytes = 200;
        let mut buffer = BatchingBuffer::from_config(config);

        // Add records that exceed buffer size
        buffer.push(vec![create_sized_record(80)]).unwrap();
        buffer.push(vec![create_sized_record(80)]).unwrap();
        buffer.push(vec![create_sized_record(80)]).unwrap();

        // Should have dropped oldest records
        let stats = buffer.stats();
        assert!(stats.records_dropped > 0);
    }

    #[test]
    fn test_batching_buffer_clear() {
        let mut buffer = BatchingBuffer::new(1024, 100);

        buffer.push(vec![create_test_record("clear")]).unwrap();
        assert!(!buffer.is_empty());

        buffer.clear();
        assert!(buffer.is_empty());
        assert_eq!(buffer.memory_usage(), 0);
        assert_eq!(buffer.batch_age_ms(), 0);
    }

    #[test]
    fn test_batching_buffer_empty_should_not_flush() {
        let buffer = BatchingBuffer::new(0, 0); // Zero thresholds
        assert!(!buffer.should_flush()); // Empty buffer never flushes
    }
}
