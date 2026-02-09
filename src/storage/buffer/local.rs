//! Local buffer for write-through storage
//!
//! This buffer provides minimal buffering for local storage mode.
//! Records are accumulated until flush is called, with no automatic
//! size or time-based triggers.

use crate::error::Result;
use crate::storage::backend::{BufferConfig, BufferStats, BufferStrategy};
use crate::storage::record::Record;
use std::fmt::Debug;

/// Write-through buffer for local storage mode
///
/// This buffer accumulates records but doesn't trigger automatic flushes.
/// It's designed for local disk storage where writes are fast and we want
/// minimal latency.
#[derive(Debug)]
pub(crate) struct LocalBuffer {
    /// Buffered records
    records: Vec<Record>,

    /// Current size in bytes
    size_bytes: usize,

    /// Buffer configuration
    config: BufferConfig,

    /// Statistics
    stats: BufferStatsInternal,
}

#[derive(Debug, Default)]
struct BufferStatsInternal {
    total_records_buffered: u64,
    total_bytes_buffered: u64,
    flush_count: u64,
}

impl LocalBuffer {
    /// Create a new local buffer
    pub fn new() -> Self {
        Self {
            records: Vec::new(),
            size_bytes: 0,
            config: BufferConfig::write_through(),
            stats: BufferStatsInternal::default(),
        }
    }

    /// Create a local buffer with specified capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            records: Vec::with_capacity(capacity),
            size_bytes: 0,
            config: BufferConfig::write_through(),
            stats: BufferStatsInternal::default(),
        }
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
}

impl Default for LocalBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl BufferStrategy for LocalBuffer {
    fn push(&mut self, records: Vec<Record>) -> Result<()> {
        for record in records {
            let size = Self::estimate_record_size(&record);
            self.size_bytes += size;
            self.stats.total_bytes_buffered += size as u64;
            self.stats.total_records_buffered += 1;
            self.records.push(record);
        }
        Ok(())
    }

    fn should_flush(&self) -> bool {
        // Write-through mode: always ready if there are records
        !self.records.is_empty()
    }

    fn take_batch(&mut self) -> Vec<Record> {
        self.stats.flush_count += 1;
        self.size_bytes = 0;
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
            records_dropped: 0,
        }
    }

    fn clear(&mut self) {
        self.records.clear();
        self.size_bytes = 0;
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

    #[test]
    fn test_local_buffer_new() {
        let buffer = LocalBuffer::new();
        assert!(buffer.is_empty());
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.memory_usage(), 0);
    }

    #[test]
    fn test_local_buffer_push() {
        let mut buffer = LocalBuffer::new();

        let records = vec![create_test_record("hello"), create_test_record("world")];

        buffer.push(records).unwrap();

        assert!(!buffer.is_empty());
        assert_eq!(buffer.len(), 2);
        assert!(buffer.memory_usage() > 0);
    }

    #[test]
    fn test_local_buffer_should_flush() {
        let mut buffer = LocalBuffer::new();

        // Empty buffer shouldn't flush
        assert!(!buffer.should_flush());

        // Buffer with records should flush
        buffer.push(vec![create_test_record("test")]).unwrap();
        assert!(buffer.should_flush());
    }

    #[test]
    fn test_local_buffer_take_batch() {
        let mut buffer = LocalBuffer::new();

        buffer
            .push(vec![create_test_record("one"), create_test_record("two")])
            .unwrap();

        let batch = buffer.take_batch();

        assert_eq!(batch.len(), 2);
        assert!(buffer.is_empty());
        assert_eq!(buffer.memory_usage(), 0);
    }

    #[test]
    fn test_local_buffer_peek() {
        let mut buffer = LocalBuffer::new();

        buffer.push(vec![create_test_record("peek")]).unwrap();

        let peeked = buffer.peek();
        assert_eq!(peeked.len(), 1);

        // Buffer should still have records after peek
        assert!(!buffer.is_empty());
    }

    #[test]
    fn test_local_buffer_clear() {
        let mut buffer = LocalBuffer::new();

        buffer.push(vec![create_test_record("clear")]).unwrap();
        assert!(!buffer.is_empty());

        buffer.clear();
        assert!(buffer.is_empty());
        assert_eq!(buffer.memory_usage(), 0);
    }

    #[test]
    fn test_local_buffer_stats() {
        let mut buffer = LocalBuffer::new();

        buffer.push(vec![create_test_record("stats")]).unwrap();
        buffer.take_batch();
        buffer.push(vec![create_test_record("more")]).unwrap();

        let stats = buffer.stats();
        assert_eq!(stats.total_records_buffered, 2);
        assert_eq!(stats.flush_count, 1);
        assert_eq!(stats.record_count, 1);
    }
}
