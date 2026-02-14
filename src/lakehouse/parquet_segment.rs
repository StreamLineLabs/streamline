//! Parquet Segment Storage
//!
//! Stores stream data in Parquet format for efficient analytics.

use super::config::ParquetConfig;
use crate::error::{Result, StreamlineError};
use crate::storage::Record;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[cfg(feature = "columnar")]
use arrow::array::{ArrayRef, RecordBatch, StringArray, TimestampMillisecondArray};
#[cfg(feature = "columnar")]
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
#[cfg(feature = "columnar")]
use parquet::arrow::ArrowWriter;
#[cfg(feature = "columnar")]
use parquet::basic::Compression;
#[cfg(feature = "columnar")]
use parquet::file::properties::WriterProperties;

/// Segment ID type
pub type SegmentId = u64;

/// Parquet-based segment storage
#[derive(Debug)]
pub struct ParquetSegment {
    /// Segment ID
    pub id: SegmentId,
    /// Parquet file path
    pub path: PathBuf,
    /// Topic name
    pub topic: String,
    /// Partition number
    pub partition: i32,
    /// First offset in segment
    pub base_offset: i64,
    /// Last offset in segment
    pub last_offset: i64,
    /// Record count
    pub record_count: u64,
    /// File size in bytes
    pub size_bytes: u64,
    /// Row group metadata
    pub row_groups: Vec<RowGroupMetadata>,
    /// Column statistics
    pub statistics: ColumnStatistics,
    /// Creation timestamp
    pub created_at: DateTime<Utc>,
    /// Is segment sealed (no more writes)
    pub sealed: bool,
}

impl ParquetSegment {
    /// Create a new segment
    pub fn new(
        id: SegmentId,
        path: PathBuf,
        topic: String,
        partition: i32,
        base_offset: i64,
    ) -> Self {
        Self {
            id,
            path,
            topic,
            partition,
            base_offset,
            last_offset: base_offset,
            record_count: 0,
            size_bytes: 0,
            row_groups: Vec::new(),
            statistics: ColumnStatistics::default(),
            created_at: Utc::now(),
            sealed: false,
        }
    }

    /// Check if offset is in this segment
    pub fn contains_offset(&self, offset: i64) -> bool {
        offset >= self.base_offset && offset <= self.last_offset
    }
}

/// Row group metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowGroupMetadata {
    /// Row group index
    pub index: usize,
    /// Number of rows
    pub num_rows: u64,
    /// Total byte size
    pub total_byte_size: u64,
    /// Column chunks
    pub columns: Vec<ColumnChunkMetadata>,
}

/// Column chunk metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnChunkMetadata {
    /// Column name
    pub name: String,
    /// Compressed size
    pub compressed_size: u64,
    /// Uncompressed size
    pub uncompressed_size: u64,
    /// Number of values
    pub num_values: u64,
    /// Statistics
    pub stats: Option<ColumnStats>,
}

/// Column statistics for query optimization
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ColumnStatistics {
    /// Statistics per column
    pub columns: HashMap<String, ColumnStats>,
}

impl ColumnStatistics {
    /// Merge statistics from another instance
    pub fn merge(&mut self, other: &ColumnStatistics) {
        for (name, stats) in &other.columns {
            if let Some(existing) = self.columns.get_mut(name) {
                existing.merge(stats);
            } else {
                self.columns.insert(name.clone(), stats.clone());
            }
        }
    }

    /// Get statistics for a column
    pub fn get(&self, column: &str) -> Option<&ColumnStats> {
        self.columns.get(column)
    }
}

/// Statistics for a single column
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStats {
    /// Column name
    pub name: String,
    /// Data type
    pub data_type: String,
    /// Null count
    pub null_count: u64,
    /// Distinct count (estimated)
    pub distinct_count: Option<u64>,
    /// Minimum value (as string for display)
    pub min_value: Option<String>,
    /// Maximum value (as string for display)
    pub max_value: Option<String>,
    /// Total size in bytes
    pub total_size_bytes: u64,
}

impl ColumnStats {
    /// Create new column stats
    pub fn new(name: String, data_type: String) -> Self {
        Self {
            name,
            data_type,
            null_count: 0,
            distinct_count: None,
            min_value: None,
            max_value: None,
            total_size_bytes: 0,
        }
    }

    /// Merge with another stats instance
    pub fn merge(&mut self, other: &ColumnStats) {
        self.null_count += other.null_count;
        self.total_size_bytes += other.total_size_bytes;

        // Merge min
        match (&self.min_value, &other.min_value) {
            (Some(a), Some(b)) if b < a => self.min_value = Some(b.clone()),
            (None, Some(b)) => self.min_value = Some(b.clone()),
            _ => {}
        }

        // Merge max
        match (&self.max_value, &other.max_value) {
            (Some(a), Some(b)) if b > a => self.max_value = Some(b.clone()),
            (None, Some(b)) => self.max_value = Some(b.clone()),
            _ => {}
        }
    }
}

/// Segment writer for Parquet format
pub struct ParquetSegmentWriter {
    /// Segment being written
    segment: ParquetSegment,
    /// Configuration
    config: ParquetConfig,
    /// Buffer of pending records
    buffer: Vec<Record>,
    /// Current row count
    row_count: AtomicU64,
    /// Current file size
    current_size: AtomicU64,
}

impl ParquetSegmentWriter {
    /// Create a new Parquet segment writer
    pub fn new(
        id: SegmentId,
        path: PathBuf,
        topic: String,
        partition: i32,
        base_offset: i64,
        config: ParquetConfig,
    ) -> Result<Self> {
        let segment = ParquetSegment::new(id, path, topic, partition, base_offset);

        let batch_size = config.write_batch_size;
        Ok(Self {
            segment,
            config,
            buffer: Vec::with_capacity(batch_size),
            row_count: AtomicU64::new(0),
            current_size: AtomicU64::new(0),
        })
    }

    /// Write a record to the segment
    pub fn write(&mut self, record: Record) -> Result<()> {
        self.buffer.push(record);
        self.row_count.fetch_add(1, Ordering::Relaxed);

        // Flush if buffer is full
        if self.buffer.len() >= self.config.write_batch_size {
            self.flush_buffer()?;
        }

        Ok(())
    }

    /// Write a batch of records
    pub fn write_batch(&mut self, records: Vec<Record>) -> Result<()> {
        for record in records {
            self.write(record)?;
        }
        Ok(())
    }

    /// Flush buffered records
    fn flush_buffer(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        #[cfg(feature = "columnar")]
        {
            self.write_parquet_batch()?;
        }

        #[cfg(not(feature = "columnar"))]
        {
            // Just clear buffer if columnar feature not enabled
            self.buffer.clear();
        }

        Ok(())
    }

    /// Write batch to Parquet (requires columnar feature)
    #[cfg(feature = "columnar")]
    fn write_parquet_batch(&mut self) -> Result<()> {
        use arrow::array::Int64Array;

        // Convert records to Arrow arrays
        let records = std::mem::take(&mut self.buffer);

        // Build arrays for each column
        let offsets: Vec<i64> = records.iter().map(|r| r.offset).collect();
        let timestamps: Vec<i64> = records.iter().map(|r| r.timestamp).collect();
        let keys: Vec<Option<&str>> = records
            .iter()
            .map(|r| r.key.as_ref().and_then(|k| std::str::from_utf8(k).ok()))
            .collect();
        let values: Vec<&str> = records
            .iter()
            .map(|r| std::str::from_utf8(&r.value).unwrap_or(""))
            .collect();

        // Create Arrow arrays
        let offset_array: ArrayRef = Arc::new(Int64Array::from(offsets));
        let timestamp_array: ArrayRef =
            Arc::new(TimestampMillisecondArray::from(timestamps).with_timezone("UTC"));
        let key_array: ArrayRef = Arc::new(StringArray::from(keys));
        let value_array: ArrayRef = Arc::new(StringArray::from(values));

        // Create schema
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("offset", DataType::Int64, false),
            Field::new(
                "timestamp",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, Some("UTC".into())),
                false,
            ),
            Field::new("key", DataType::Utf8, true),
            Field::new("value", DataType::Utf8, false),
        ]));

        // Create record batch
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![offset_array, timestamp_array, key_array, value_array],
        )
        .map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to create record batch: {}", e))
        })?;

        // Write to Parquet file
        let file = File::create(&self.segment.path)
            .map_err(|e| StreamlineError::storage_msg(format!("Failed to create file: {}", e)))?;

        let compression = match self.config.compression {
            super::config::ParquetCompression::None => Compression::UNCOMPRESSED,
            super::config::ParquetCompression::Snappy => Compression::SNAPPY,
            super::config::ParquetCompression::Gzip => Compression::GZIP(Default::default()),
            super::config::ParquetCompression::Lz4 => Compression::LZ4,
            super::config::ParquetCompression::Zstd => Compression::ZSTD(Default::default()),
            super::config::ParquetCompression::Brotli => Compression::BROTLI(Default::default()),
        };

        let props = WriterProperties::builder()
            .set_compression(compression)
            .set_max_row_group_size(self.config.row_group_size)
            .build();

        let mut writer = ArrowWriter::try_new(BufWriter::new(file), schema, Some(props))
            .map_err(|e| StreamlineError::storage_msg(format!("Failed to create writer: {}", e)))?;

        writer
            .write(&batch)
            .map_err(|e| StreamlineError::storage_msg(format!("Failed to write batch: {}", e)))?;

        writer
            .close()
            .map_err(|e| StreamlineError::storage_msg(format!("Failed to close writer: {}", e)))?;

        // Update segment metadata
        if let Some(last) = records.last() {
            self.segment.last_offset = last.offset;
        }
        self.segment.record_count += records.len() as u64;

        Ok(())
    }

    /// Flush and close the segment
    pub fn close(mut self) -> Result<ParquetSegment> {
        self.flush_buffer()?;
        self.segment.sealed = true;

        // Get file size
        if self.segment.path.exists() {
            if let Ok(metadata) = std::fs::metadata(&self.segment.path) {
                self.segment.size_bytes = metadata.len();
            }
        }

        Ok(self.segment)
    }

    /// Check if segment should be rolled (new file)
    pub fn should_roll(&self) -> bool {
        self.current_size.load(Ordering::Relaxed) as usize >= self.config.target_file_size
    }
}

/// Segment reader with predicate pushdown
pub struct ParquetSegmentReader {
    /// Segment being read
    segment: Arc<ParquetSegment>,
    /// Projection (columns to read)
    projection: Option<Vec<String>>,
    /// Row filter predicate
    predicate: Option<super::query_optimizer::Predicate>,
    /// Current position
    #[allow(dead_code)]
    position: usize,
}

impl ParquetSegmentReader {
    /// Create reader for a segment
    pub fn new(segment: Arc<ParquetSegment>) -> Self {
        Self {
            segment,
            projection: None,
            predicate: None,
            position: 0,
        }
    }

    /// Create reader with projection
    pub fn with_projection(segment: Arc<ParquetSegment>, columns: Vec<String>) -> Self {
        Self {
            segment,
            projection: Some(columns),
            predicate: None,
            position: 0,
        }
    }

    /// Create reader with predicate
    pub fn with_predicate(
        segment: Arc<ParquetSegment>,
        predicate: super::query_optimizer::Predicate,
    ) -> Self {
        Self {
            segment,
            projection: None,
            predicate: Some(predicate),
            position: 0,
        }
    }

    /// Set projection
    pub fn set_projection(&mut self, columns: Vec<String>) {
        self.projection = Some(columns);
    }

    /// Set predicate
    pub fn set_predicate(&mut self, predicate: super::query_optimizer::Predicate) {
        self.predicate = Some(predicate);
    }

    /// Read all matching records (stub implementation)
    pub fn read_all(&mut self) -> Result<Vec<Record>> {
        #[cfg(feature = "columnar")]
        {
            self.read_parquet_records()
        }

        #[cfg(not(feature = "columnar"))]
        {
            Ok(Vec::new())
        }
    }

    #[cfg(feature = "columnar")]
    fn read_parquet_records(&self) -> Result<Vec<Record>> {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let file = File::open(&self.segment.path)
            .map_err(|e| StreamlineError::storage_msg(format!("Failed to open file: {}", e)))?;

        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| StreamlineError::storage_msg(format!("Failed to create reader: {}", e)))?;

        let reader = builder
            .build()
            .map_err(|e| StreamlineError::storage_msg(format!("Failed to build reader: {}", e)))?;

        let mut records = Vec::new();

        for batch_result in reader {
            let batch = batch_result.map_err(|e| {
                StreamlineError::storage_msg(format!("Failed to read batch: {}", e))
            })?;

            // Convert batch to records
            for i in 0..batch.num_rows() {
                // This is a simplified conversion - in practice you'd need to
                // properly extract values from the Arrow arrays
                let record = Record {
                    offset: i as i64 + self.segment.base_offset,
                    timestamp: chrono::Utc::now().timestamp_millis(),
                    key: None,
                    value: bytes::Bytes::new(),
                    headers: vec![],
                    crc: None,
                };
                records.push(record);
            }
        }

        Ok(records)
    }
}

/// Parquet segment manager - manages all Parquet segments
pub struct ParquetSegmentManager {
    /// Configuration
    config: ParquetConfig,
    /// Active segments by topic/partition
    segments: DashMap<(String, i32), Vec<Arc<ParquetSegment>>>,
    /// Next segment ID
    next_id: AtomicU64,
    /// Base path for segments
    base_path: PathBuf,
}

impl ParquetSegmentManager {
    /// Create a new segment manager
    pub fn new(config: ParquetConfig, base_path: PathBuf) -> Self {
        Self {
            config,
            segments: DashMap::new(),
            next_id: AtomicU64::new(1),
            base_path,
        }
    }

    /// Get or create writer for topic/partition
    pub fn get_writer(
        &self,
        topic: &str,
        partition: i32,
        base_offset: i64,
    ) -> Result<ParquetSegmentWriter> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let path = self
            .base_path
            .join(topic)
            .join(format!("partition-{}", partition))
            .join(format!("{:020}.parquet", base_offset));

        // Ensure directory exists
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                StreamlineError::storage_msg(format!("Failed to create directory: {}", e))
            })?;
        }

        ParquetSegmentWriter::new(
            id,
            path,
            topic.to_string(),
            partition,
            base_offset,
            self.config.clone(),
        )
    }

    /// Add a completed segment
    pub fn add_segment(&self, segment: ParquetSegment) {
        let key = (segment.topic.clone(), segment.partition);
        let mut entry = self.segments.entry(key).or_default();
        entry.push(Arc::new(segment));
    }

    /// Get segments for topic/partition
    pub fn get_segments(&self, topic: &str, partition: i32) -> Vec<Arc<ParquetSegment>> {
        self.segments
            .get(&(topic.to_string(), partition))
            .map(|s| s.clone())
            .unwrap_or_default()
    }

    /// Find segment containing offset
    pub fn find_segment(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Option<Arc<ParquetSegment>> {
        self.get_segments(topic, partition)
            .into_iter()
            .find(|s| s.contains_offset(offset))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parquet_segment_new() {
        let segment = ParquetSegment::new(
            1,
            PathBuf::from("/tmp/test.parquet"),
            "test-topic".to_string(),
            0,
            100,
        );

        assert_eq!(segment.id, 1);
        assert_eq!(segment.topic, "test-topic");
        assert_eq!(segment.partition, 0);
        assert_eq!(segment.base_offset, 100);
        assert!(!segment.sealed);
    }

    #[test]
    fn test_column_stats_merge() {
        let mut stats1 = ColumnStats::new("col1".to_string(), "string".to_string());
        stats1.min_value = Some("b".to_string());
        stats1.max_value = Some("d".to_string());
        stats1.null_count = 5;

        let mut stats2 = ColumnStats::new("col1".to_string(), "string".to_string());
        stats2.min_value = Some("a".to_string());
        stats2.max_value = Some("c".to_string());
        stats2.null_count = 3;

        stats1.merge(&stats2);

        assert_eq!(stats1.null_count, 8);
        assert_eq!(stats1.min_value, Some("a".to_string()));
        assert_eq!(stats1.max_value, Some("d".to_string()));
    }
}
