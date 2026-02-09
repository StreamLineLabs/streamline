//! Columnar storage with Apache Arrow
//!
//! Provides efficient columnar storage format for analytical workloads.
//! Records can be converted to Arrow RecordBatches and stored in Parquet format.
//!
//! Features:
//! - Row-to-columnar conversion
//! - Parquet file format for persistence
//! - Efficient range scans and projections
//! - Schema evolution support
//! - Compression (Snappy, Zstd, LZ4)

use crate::error::{Result, StreamlineError};
use crate::storage::record::{Header, Record};
use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, Int64Array, Int64Builder, ListArray, ListBuilder,
    StringBuilder, StructBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use parking_lot::RwLock;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{debug, info};

/// Columnar storage configuration
#[derive(Debug, Clone)]
pub struct ColumnarConfig {
    /// Directory for columnar data files
    pub data_dir: PathBuf,
    /// Batch size for record batches
    pub batch_size: usize,
    /// Compression type (Snappy, Zstd, LZ4, None)
    pub compression: ColumnarCompression,
    /// Row group size for Parquet files
    pub row_group_size: usize,
    /// Maximum file size before rotation (bytes)
    pub max_file_size: usize,
    /// Enable dictionary encoding for keys
    pub dictionary_encoding: bool,
    /// Page size for Parquet files
    pub page_size: usize,
}

impl Default for ColumnarConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data/columnar"),
            batch_size: 10000,
            compression: ColumnarCompression::Snappy,
            row_group_size: 100000,
            max_file_size: 256 * 1024 * 1024, // 256MB
            dictionary_encoding: true,
            page_size: 1024 * 1024, // 1MB
        }
    }
}

/// Compression types for columnar storage
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnarCompression {
    None,
    Snappy,
    Zstd,
    Lz4,
    Gzip,
}

impl From<ColumnarCompression> for Compression {
    fn from(c: ColumnarCompression) -> Self {
        match c {
            ColumnarCompression::None => Compression::UNCOMPRESSED,
            ColumnarCompression::Snappy => Compression::SNAPPY,
            ColumnarCompression::Zstd => Compression::ZSTD(Default::default()),
            ColumnarCompression::Lz4 => Compression::LZ4,
            ColumnarCompression::Gzip => Compression::GZIP(Default::default()),
        }
    }
}

/// Columnar storage manager
pub struct ColumnarStorage {
    /// Configuration
    config: ColumnarConfig,
    /// Schema for records
    schema: Arc<Schema>,
    /// Writers per topic/partition
    writers: RwLock<HashMap<(String, i32), ColumnarWriter>>,
    /// Statistics
    stats: RwLock<ColumnarStats>,
}

/// Statistics for columnar storage
#[derive(Debug, Clone, Default)]
pub struct ColumnarStats {
    /// Total records written
    pub records_written: u64,
    /// Total bytes written (uncompressed)
    pub bytes_uncompressed: u64,
    /// Total bytes written (compressed)
    pub bytes_compressed: u64,
    /// Total files created
    pub files_created: u64,
    /// Total record batches written
    pub batches_written: u64,
}

impl ColumnarStorage {
    /// Create a new columnar storage manager
    pub fn new(config: ColumnarConfig) -> Result<Self> {
        std::fs::create_dir_all(&config.data_dir).map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to create columnar directory: {}", e))
        })?;

        let schema = Arc::new(Self::create_schema());

        info!("Columnar storage initialized at {:?}", config.data_dir);

        Ok(Self {
            config,
            schema,
            writers: RwLock::new(HashMap::new()),
            stats: RwLock::new(ColumnarStats::default()),
        })
    }

    /// Create the Arrow schema for records
    fn create_schema() -> Schema {
        Schema::new(vec![
            Field::new("offset", DataType::Int64, false),
            Field::new("timestamp", DataType::Int64, false),
            Field::new("key", DataType::Binary, true),
            Field::new("value", DataType::Binary, false),
            Field::new(
                "headers",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Binary, false),
                        ]
                        .into(),
                    ),
                    true,
                ))),
                true,
            ),
        ])
    }

    /// Convert records to Arrow RecordBatch
    pub fn records_to_batch(&self, records: &[Record]) -> Result<RecordBatch> {
        if records.is_empty() {
            return Err(StreamlineError::storage_msg(
                "No records to convert".to_string(),
            ));
        }

        let len = records.len();

        // Build offset array
        let mut offset_builder = Int64Builder::with_capacity(len);
        for record in records {
            offset_builder.append_value(record.offset);
        }
        let offsets: ArrayRef = Arc::new(offset_builder.finish());

        // Build timestamp array
        let mut timestamp_builder = Int64Builder::with_capacity(len);
        for record in records {
            timestamp_builder.append_value(record.timestamp);
        }
        let timestamps: ArrayRef = Arc::new(timestamp_builder.finish());

        // Build key array (nullable binary)
        let mut key_builder = BinaryBuilder::with_capacity(len, len * 64);
        for record in records {
            match &record.key {
                Some(k) => key_builder.append_value(k.as_ref()),
                None => key_builder.append_null(),
            }
        }
        let keys: ArrayRef = Arc::new(key_builder.finish());

        // Build value array
        let mut value_builder = BinaryBuilder::with_capacity(len, len * 256);
        for record in records {
            value_builder.append_value(record.value.as_ref());
        }
        let values: ArrayRef = Arc::new(value_builder.finish());

        // Build headers array (list of structs)
        let header_fields = vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Binary, false),
        ];
        let header_struct_fields: arrow::datatypes::Fields = header_fields.into();

        let header_key_builder = StringBuilder::new();
        let header_value_builder = BinaryBuilder::new();
        let struct_builder = StructBuilder::new(
            header_struct_fields.clone(),
            vec![Box::new(header_key_builder), Box::new(header_value_builder)],
        );

        let mut list_builder = ListBuilder::new(struct_builder);

        for record in records {
            let struct_builder = list_builder.values();
            for header in &record.headers {
                struct_builder
                    .field_builder::<StringBuilder>(0)
                    .ok_or_else(|| StreamlineError::storage_msg("Missing header key field builder".to_string()))?
                    .append_value(&header.key);
                struct_builder
                    .field_builder::<BinaryBuilder>(1)
                    .ok_or_else(|| StreamlineError::storage_msg("Missing header value field builder".to_string()))?
                    .append_value(header.value.as_ref());
                struct_builder.append(true);
            }
            list_builder.append(true);
        }
        let headers: ArrayRef = Arc::new(list_builder.finish());

        RecordBatch::try_new(
            self.schema.clone(),
            vec![offsets, timestamps, keys, values, headers],
        )
        .map_err(|e| StreamlineError::storage_msg(format!("Failed to create record batch: {}", e)))
    }

    /// Convert Arrow RecordBatch back to records
    pub fn batch_to_records(&self, batch: &RecordBatch) -> Result<Vec<Record>> {
        let len = batch.num_rows();
        let mut records = Vec::with_capacity(len);

        let offsets = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| StreamlineError::storage_msg("Invalid offset column".to_string()))?;

        let timestamps = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| StreamlineError::storage_msg("Invalid timestamp column".to_string()))?;

        let keys = batch
            .column(2)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| StreamlineError::storage_msg("Invalid key column".to_string()))?;

        let values = batch
            .column(3)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| StreamlineError::storage_msg("Invalid value column".to_string()))?;

        let headers_list = batch
            .column(4)
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| StreamlineError::storage_msg("Invalid headers column".to_string()))?;

        for i in 0..len {
            let offset = offsets.value(i);
            let timestamp = timestamps.value(i);
            let key = if keys.is_null(i) {
                None
            } else {
                Some(Bytes::copy_from_slice(keys.value(i)))
            };
            let value = Bytes::copy_from_slice(values.value(i));

            // Parse headers
            let mut headers = Vec::new();
            if !headers_list.is_null(i) {
                let start = headers_list.value_offsets()[i] as usize;
                let end = headers_list.value_offsets()[i + 1] as usize;
                let header_struct = headers_list
                    .values()
                    .as_any()
                    .downcast_ref::<arrow::array::StructArray>()
                    .ok_or_else(|| {
                        StreamlineError::storage_msg("Invalid header struct".to_string())
                    })?;

                for j in start..end {
                    let key_arr = header_struct
                        .column(0)
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                        .ok_or_else(|| {
                            StreamlineError::storage_msg("Invalid header key".to_string())
                        })?;
                    let value_arr = header_struct
                        .column(1)
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .ok_or_else(|| {
                            StreamlineError::storage_msg("Invalid header value".to_string())
                        })?;

                    headers.push(Header {
                        key: key_arr.value(j).to_string(),
                        value: Bytes::copy_from_slice(value_arr.value(j)),
                    });
                }
            }

            records.push(Record::with_headers(offset, timestamp, key, value, headers));
        }

        Ok(records)
    }

    /// Write records to columnar storage
    pub fn write_records(&self, topic: &str, partition: i32, records: &[Record]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }

        let batch = self.records_to_batch(records)?;
        let key = (topic.to_string(), partition);

        let mut writers = self.writers.write();
        if !writers.contains_key(&key) {
            let new_writer = ColumnarWriter::new(&self.config, topic, partition, self.schema.clone())?;
            writers.insert(key.clone(), new_writer);
        }
        let writer = writers.get_mut(&key).ok_or_else(|| {
            StreamlineError::storage_msg("Writer unexpectedly missing after insert".to_string())
        })?;

        writer.write_batch(&batch)?;

        let mut stats = self.stats.write();
        stats.records_written += records.len() as u64;
        stats.batches_written += 1;

        debug!(
            "Wrote {} records to columnar storage for {}/{}",
            records.len(),
            topic,
            partition
        );

        Ok(())
    }

    /// Read records from columnar storage in a time range
    pub fn read_records(
        &self,
        topic: &str,
        partition: i32,
        start_timestamp: Option<i64>,
        end_timestamp: Option<i64>,
        limit: Option<usize>,
    ) -> Result<Vec<Record>> {
        let partition_dir = self
            .config
            .data_dir
            .join(topic)
            .join(format!("partition-{}", partition));

        if !partition_dir.exists() {
            return Ok(Vec::new());
        }

        let mut all_records = Vec::new();
        let mut files: Vec<_> = std::fs::read_dir(&partition_dir)
            .map_err(|e| {
                StreamlineError::storage_msg(format!("Failed to read partition dir: {}", e))
            })?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "parquet"))
            .collect();

        files.sort_by_key(|e| e.path());

        for entry in files {
            let file = File::open(entry.path()).map_err(|e| {
                StreamlineError::storage_msg(format!("Failed to open parquet file: {}", e))
            })?;

            let reader = ParquetRecordBatchReaderBuilder::try_new(file)
                .map_err(|e| {
                    StreamlineError::storage_msg(format!("Failed to create reader: {}", e))
                })?
                .build()
                .map_err(|e| {
                    StreamlineError::storage_msg(format!("Failed to build reader: {}", e))
                })?;

            for batch_result in reader {
                let batch = batch_result.map_err(|e| {
                    StreamlineError::storage_msg(format!("Failed to read batch: {}", e))
                })?;

                let records = self.batch_to_records(&batch)?;

                // Apply timestamp filtering
                let filtered: Vec<_> = records
                    .into_iter()
                    .filter(|r| {
                        if let Some(start) = start_timestamp {
                            if r.timestamp < start {
                                return false;
                            }
                        }
                        if let Some(end) = end_timestamp {
                            if r.timestamp > end {
                                return false;
                            }
                        }
                        true
                    })
                    .collect();

                all_records.extend(filtered);

                if let Some(limit) = limit {
                    if all_records.len() >= limit {
                        all_records.truncate(limit);
                        return Ok(all_records);
                    }
                }
            }
        }

        Ok(all_records)
    }

    /// Flush all pending writes
    pub fn flush(&self) -> Result<()> {
        let mut writers = self.writers.write();
        for (_, writer) in writers.iter_mut() {
            writer.flush()?;
        }
        Ok(())
    }

    /// Get storage statistics
    pub fn stats(&self) -> ColumnarStats {
        self.stats.read().clone()
    }

    /// Create a columnar reader for a specific topic/partition
    pub fn reader(&self, topic: &str, partition: i32) -> Result<ColumnarReader> {
        ColumnarReader::new(&self.config, topic, partition)
    }

    /// Compact multiple files into a single file
    pub fn compact(&self, topic: &str, partition: i32) -> Result<usize> {
        let partition_dir = self
            .config
            .data_dir
            .join(topic)
            .join(format!("partition-{}", partition));

        if !partition_dir.exists() {
            return Ok(0);
        }

        let mut files: Vec<_> = std::fs::read_dir(&partition_dir)
            .map_err(|e| {
                StreamlineError::storage_msg(format!("Failed to read partition dir: {}", e))
            })?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "parquet"))
            .collect();

        if files.len() <= 1 {
            return Ok(0);
        }

        files.sort_by_key(|e| e.path());

        // Read all batches
        let mut all_records = Vec::new();
        let mut files_to_remove = Vec::new();

        for entry in &files {
            let file = File::open(entry.path()).map_err(|e| {
                StreamlineError::storage_msg(format!("Failed to open parquet file: {}", e))
            })?;

            let reader = ParquetRecordBatchReaderBuilder::try_new(file)
                .map_err(|e| {
                    StreamlineError::storage_msg(format!("Failed to create reader: {}", e))
                })?
                .build()
                .map_err(|e| {
                    StreamlineError::storage_msg(format!("Failed to build reader: {}", e))
                })?;

            for batch_result in reader {
                let batch = batch_result.map_err(|e| {
                    StreamlineError::storage_msg(format!("Failed to read batch: {}", e))
                })?;
                let records = self.batch_to_records(&batch)?;
                all_records.extend(records);
            }

            files_to_remove.push(entry.path());
        }

        if all_records.is_empty() {
            return Ok(0);
        }

        // Write compacted file
        let compacted_file = partition_dir.join(format!(
            "compacted_{}.parquet",
            chrono::Utc::now().timestamp_millis()
        ));
        let file = File::create(&compacted_file).map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to create compacted file: {}", e))
        })?;

        let props = WriterProperties::builder()
            .set_compression(self.config.compression.into())
            .build();

        let mut writer = ArrowWriter::try_new(file, self.schema.clone(), Some(props))
            .map_err(|e| StreamlineError::storage_msg(format!("Failed to create writer: {}", e)))?;

        // Write in batches
        for chunk in all_records.chunks(self.config.batch_size) {
            let batch = self.records_to_batch(chunk)?;
            writer.write(&batch).map_err(|e| {
                StreamlineError::storage_msg(format!("Failed to write batch: {}", e))
            })?;
        }

        writer
            .close()
            .map_err(|e| StreamlineError::storage_msg(format!("Failed to close writer: {}", e)))?;

        // Remove old files
        let files_removed = files_to_remove.len();
        for path in files_to_remove {
            std::fs::remove_file(path).ok();
        }

        info!(
            "Compacted {} files into 1 for {}/{}",
            files_removed, topic, partition
        );

        Ok(files_removed)
    }
}

/// Columnar writer for a specific topic/partition
pub struct ColumnarWriter {
    /// Partition directory
    partition_dir: PathBuf,
    /// Current file path
    current_file: Option<PathBuf>,
    /// Arrow writer
    writer: Option<ArrowWriter<File>>,
    /// Schema
    schema: Arc<Schema>,
    /// Configuration
    config: ColumnarConfig,
    /// Records written to current file
    records_in_file: usize,
    /// Bytes written to current file
    bytes_in_file: usize,
}

impl ColumnarWriter {
    /// Create a new columnar writer
    pub fn new(
        config: &ColumnarConfig,
        topic: &str,
        partition: i32,
        schema: Arc<Schema>,
    ) -> Result<Self> {
        let partition_dir = config
            .data_dir
            .join(topic)
            .join(format!("partition-{}", partition));
        std::fs::create_dir_all(&partition_dir).map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to create partition dir: {}", e))
        })?;

        Ok(Self {
            partition_dir,
            current_file: None,
            writer: None,
            schema,
            config: config.clone(),
            records_in_file: 0,
            bytes_in_file: 0,
        })
    }

    /// Write a record batch
    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        // Rotate file if needed
        if self.bytes_in_file >= self.config.max_file_size {
            self.flush()?;
        }

        // Create new file if needed
        if self.writer.is_none() {
            self.create_new_file()?;
        }

        let writer = self.writer.as_mut().ok_or_else(|| {
            StreamlineError::storage_msg("Writer not initialized after create_new_file".to_string())
        })?;
        writer
            .write(batch)
            .map_err(|e| StreamlineError::storage_msg(format!("Failed to write batch: {}", e)))?;

        self.records_in_file += batch.num_rows();
        // Estimate bytes written
        self.bytes_in_file += batch.get_array_memory_size();

        Ok(())
    }

    /// Create a new parquet file
    fn create_new_file(&mut self) -> Result<()> {
        let file_name = format!("{}.parquet", chrono::Utc::now().timestamp_millis());
        let file_path = self.partition_dir.join(&file_name);

        let file = File::create(&file_path).map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to create parquet file: {}", e))
        })?;

        let props = WriterProperties::builder()
            .set_compression(self.config.compression.into())
            .set_max_row_group_size(self.config.row_group_size)
            .set_data_page_size_limit(self.config.page_size)
            .build();

        let writer = ArrowWriter::try_new(file, self.schema.clone(), Some(props)).map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to create arrow writer: {}", e))
        })?;

        self.current_file = Some(file_path);
        self.writer = Some(writer);
        self.records_in_file = 0;
        self.bytes_in_file = 0;

        debug!("Created new columnar file: {}", file_name);

        Ok(())
    }

    /// Flush and close current file
    pub fn flush(&mut self) -> Result<()> {
        if let Some(writer) = self.writer.take() {
            writer.close().map_err(|e| {
                StreamlineError::storage_msg(format!("Failed to close writer: {}", e))
            })?;
            info!(
                "Flushed columnar file with {} records",
                self.records_in_file
            );
        }
        self.current_file = None;
        Ok(())
    }
}

impl Drop for ColumnarWriter {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}

/// Columnar reader for a specific topic/partition
#[allow(dead_code)]
pub struct ColumnarReader {
    /// Partition directory
    partition_dir: PathBuf,
    /// Available parquet files
    files: Vec<PathBuf>,
    /// Schema
    schema: Arc<Schema>,
}

impl ColumnarReader {
    /// Create a new columnar reader
    pub fn new(config: &ColumnarConfig, topic: &str, partition: i32) -> Result<Self> {
        let partition_dir = config
            .data_dir
            .join(topic)
            .join(format!("partition-{}", partition));
        let schema = Arc::new(ColumnarStorage::create_schema());

        let mut files = Vec::new();
        if partition_dir.exists() {
            for entry in std::fs::read_dir(&partition_dir).map_err(|e| {
                StreamlineError::storage_msg(format!("Failed to read partition dir: {}", e))
            })? {
                let entry = entry.map_err(|e| {
                    StreamlineError::storage_msg(format!("Failed to read dir entry: {}", e))
                })?;
                if entry.path().extension().is_some_and(|ext| ext == "parquet") {
                    files.push(entry.path());
                }
            }
        }

        files.sort();

        Ok(Self {
            partition_dir,
            files,
            schema,
        })
    }

    /// Read all record batches as an iterator
    pub fn read_batches(&self) -> Result<impl Iterator<Item = Result<RecordBatch>> + '_> {
        let iter = self.files.iter().flat_map(move |file_path| {
            let file = match File::open(file_path) {
                Ok(f) => f,
                Err(e) => {
                    return vec![Err(StreamlineError::storage_msg(format!(
                        "Failed to open file: {}",
                        e
                    )))]
                    .into_iter()
                }
            };

            let reader = match ParquetRecordBatchReaderBuilder::try_new(file) {
                Ok(builder) => match builder.build() {
                    Ok(r) => r,
                    Err(e) => {
                        return vec![Err(StreamlineError::storage_msg(format!(
                            "Failed to build reader: {}",
                            e
                        )))]
                        .into_iter()
                    }
                },
                Err(e) => {
                    return vec![Err(StreamlineError::storage_msg(format!(
                        "Failed to create reader: {}",
                        e
                    )))]
                    .into_iter()
                }
            };

            reader
                .map(|r| {
                    r.map_err(|e| {
                        StreamlineError::storage_msg(format!("Failed to read batch: {}", e))
                    })
                })
                .collect::<Vec<_>>()
                .into_iter()
        });

        Ok(iter)
    }

    /// Get file count
    pub fn file_count(&self) -> usize {
        self.files.len()
    }

    /// Get total size in bytes
    pub fn total_size(&self) -> Result<u64> {
        let mut total = 0;
        for file in &self.files {
            let meta = std::fs::metadata(file).map_err(|e| {
                StreamlineError::storage_msg(format!("Failed to get file metadata: {}", e))
            })?;
            total += meta.len();
        }
        Ok(total)
    }
}

/// Projection for reading specific columns
#[derive(Debug, Clone)]
pub struct Projection {
    /// Column indices to read
    pub columns: Vec<usize>,
}

impl Projection {
    /// Create a projection with specific column indices
    pub fn new(columns: Vec<usize>) -> Self {
        Self { columns }
    }

    /// Create a projection for all columns
    pub fn all() -> Self {
        Self {
            columns: vec![0, 1, 2, 3, 4],
        }
    }

    /// Create a projection for just offsets and timestamps
    pub fn metadata_only() -> Self {
        Self {
            columns: vec![0, 1],
        }
    }

    /// Create a projection for offset, timestamp, and value (no key or headers)
    pub fn minimal() -> Self {
        Self {
            columns: vec![0, 1, 3],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_records(count: usize) -> Vec<Record> {
        (0..count)
            .map(|i| {
                Record::with_headers(
                    i as i64,
                    chrono::Utc::now().timestamp_millis() + i as i64,
                    Some(Bytes::from(format!("key-{}", i))),
                    Bytes::from(format!("value-{}", i)),
                    vec![Header {
                        key: "header-key".to_string(),
                        value: Bytes::from(format!("header-value-{}", i)),
                    }],
                )
            })
            .collect()
    }

    #[test]
    fn test_columnar_config_default() {
        let config = ColumnarConfig::default();
        assert_eq!(config.batch_size, 10000);
        assert_eq!(config.compression, ColumnarCompression::Snappy);
    }

    #[test]
    fn test_records_to_batch_roundtrip() {
        let temp_dir = TempDir::new().unwrap();
        let config = ColumnarConfig {
            data_dir: temp_dir.path().to_path_buf(),
            ..Default::default()
        };
        let storage = ColumnarStorage::new(config).unwrap();

        let records = create_test_records(100);
        let batch = storage.records_to_batch(&records).unwrap();

        assert_eq!(batch.num_rows(), 100);
        assert_eq!(batch.num_columns(), 5);

        let recovered = storage.batch_to_records(&batch).unwrap();
        assert_eq!(recovered.len(), 100);

        for (orig, rec) in records.iter().zip(recovered.iter()) {
            assert_eq!(orig.offset, rec.offset);
            assert_eq!(orig.key, rec.key);
            assert_eq!(orig.value, rec.value);
        }
    }

    #[test]
    fn test_write_and_read() {
        let temp_dir = TempDir::new().unwrap();
        let config = ColumnarConfig {
            data_dir: temp_dir.path().to_path_buf(),
            batch_size: 100,
            ..Default::default()
        };
        let storage = ColumnarStorage::new(config).unwrap();

        let records = create_test_records(50);
        storage.write_records("test-topic", 0, &records).unwrap();
        storage.flush().unwrap();

        let read_records = storage
            .read_records("test-topic", 0, None, None, None)
            .unwrap();
        assert_eq!(read_records.len(), 50);
    }

    #[test]
    fn test_compression_conversion() {
        assert_eq!(
            Compression::UNCOMPRESSED,
            Compression::from(ColumnarCompression::None)
        );
        assert_eq!(
            Compression::SNAPPY,
            Compression::from(ColumnarCompression::Snappy)
        );
    }

    #[test]
    fn test_columnar_stats() {
        let temp_dir = TempDir::new().unwrap();
        let config = ColumnarConfig {
            data_dir: temp_dir.path().to_path_buf(),
            ..Default::default()
        };
        let storage = ColumnarStorage::new(config).unwrap();

        let stats = storage.stats();
        assert_eq!(stats.records_written, 0);
        assert_eq!(stats.batches_written, 0);
    }

    #[test]
    fn test_reader_file_count() {
        let temp_dir = TempDir::new().unwrap();
        let config = ColumnarConfig {
            data_dir: temp_dir.path().to_path_buf(),
            ..Default::default()
        };
        let storage = ColumnarStorage::new(config.clone()).unwrap();

        let records = create_test_records(10);
        storage.write_records("test-topic", 0, &records).unwrap();
        storage.flush().unwrap();

        let reader = storage.reader("test-topic", 0).unwrap();
        assert_eq!(reader.file_count(), 1);
    }

    #[test]
    fn test_projection() {
        let proj = Projection::all();
        assert_eq!(proj.columns.len(), 5);

        let proj = Projection::metadata_only();
        assert_eq!(proj.columns, vec![0, 1]);

        let proj = Projection::minimal();
        assert_eq!(proj.columns, vec![0, 1, 3]);
    }
}
