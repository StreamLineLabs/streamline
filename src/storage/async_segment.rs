//! Async segment implementation using io_backend abstraction
//!
//! This module provides an async-compatible segment implementation that works with
//! both the standard I/O backend and io_uring (on Linux). It mirrors the functionality
//! of the synchronous `Segment` but uses the `AsyncFile` trait for all I/O operations.
//!
//! # Example
//!
//! ```rust,ignore
//! use streamline::storage::{AsyncSegment, get_standard_backend, should_use_uring};
//!
//! // Choose backend based on platform
//! let fs = if should_use_uring() {
//!     get_uring_backend()
//! } else {
//!     get_standard_backend()
//! };
//!
//! // Create a new segment
//! let segment = AsyncSegment::create(&fs, path, base_offset).await?;
//!
//! // Append records
//! segment.append_batch(&batch).await?;
//!
//! // Read records
//! let records = segment.read_from_offset(100, 1000).await?;
//! ```

use crate::error::{Result, StreamlineError};
use crate::storage::compression::{compress, decompress, CompressionCodec};
use crate::storage::io_backend::{AsyncFile, AsyncFileSystem, IoBufferPool};
use crate::storage::record::{Record, RecordBatch};
use crate::storage::segment::SegmentHeader;
#[cfg(test)]
use bytes::Bytes;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, warn};

/// Size of segment header in bytes (same as synchronous Segment)
const HEADER_SIZE: usize = 64;

/// Flag indicating bincode serialization format
const FLAG_BINCODE_FORMAT: u16 = 0x0001;

/// Async segment implementation using io_backend traits
///
/// This struct provides async I/O operations for segment files, allowing
/// integration with io_uring on Linux for better performance.
pub struct AsyncSegment<F: AsyncFile> {
    /// Path to the segment file
    path: PathBuf,

    /// The async file handle
    file: Arc<F>,

    /// Segment header (cached in memory)
    header: RwLock<SegmentHeader>,

    /// Current write position in the file
    write_position: RwLock<u64>,

    /// Whether this segment is sealed (read-only)
    sealed: RwLock<bool>,

    /// Compression codec for writing batches
    compression: CompressionCodec,

    /// Buffer pool for I/O operations
    buffer_pool: Option<Arc<IoBufferPool>>,

    /// Running CRC32 hash for segment-level integrity checking
    running_crc: RwLock<u32>,
}

impl<F: AsyncFile + 'static> AsyncSegment<F> {
    /// Create a new async segment file
    ///
    /// This creates a new segment file at the specified path with the given base offset.
    /// The segment is initialized with a header and is ready for appending records.
    pub async fn create<FS>(fs: &FS, path: &Path, base_offset: i64) -> Result<Self>
    where
        FS: AsyncFileSystem<File = F>,
    {
        Self::create_with_compression(fs, path, base_offset, CompressionCodec::None).await
    }

    /// Create a new async segment file with specified compression
    pub async fn create_with_compression<FS>(
        fs: &FS,
        path: &Path,
        base_offset: i64,
        compression: CompressionCodec,
    ) -> Result<Self>
    where
        FS: AsyncFileSystem<File = F>,
    {
        let file = fs.create(path).await?;

        let header = SegmentHeader {
            version: 1,
            flags: FLAG_BINCODE_FORMAT,
            base_offset,
            max_offset: -1,
            base_timestamp: chrono::Utc::now().timestamp_millis(),
            max_timestamp: 0,
            record_count: 0,
            crc32: 0,
            compression: compression.as_byte(),
        };

        // Write header
        let header_bytes = header.to_bytes();
        let (result, _) = file.write_at(header_bytes.to_vec(), 0).await;
        result?;

        // Sync to ensure header is persisted
        file.sync_all().await?;

        Ok(Self {
            path: path.to_path_buf(),
            file: Arc::new(file),
            header: RwLock::new(header),
            write_position: RwLock::new(HEADER_SIZE as u64),
            sealed: RwLock::new(false),
            compression,
            buffer_pool: None,
            running_crc: RwLock::new(0),
        })
    }

    /// Open an existing async segment file for reading
    pub async fn open<FS>(fs: &FS, path: &Path) -> Result<Self>
    where
        FS: AsyncFileSystem<File = F>,
    {
        let file = fs.open(path).await?;

        // Read header
        let buf = vec![0u8; HEADER_SIZE];
        let (result, buf) = file.read_at(buf, 0).await;
        let bytes_read = result?;

        if bytes_read < HEADER_SIZE {
            return Err(StreamlineError::CorruptedData(
                "Segment header too short".to_string(),
            ));
        }

        let header = SegmentHeader::from_bytes(&buf)?;
        let compression =
            CompressionCodec::from_byte(header.compression).unwrap_or(CompressionCodec::None);

        // Get file size
        let write_position = file.size().await?;

        Ok(Self {
            path: path.to_path_buf(),
            file: Arc::new(file),
            header: RwLock::new(header),
            write_position: RwLock::new(write_position),
            sealed: RwLock::new(true), // Opened segments are read-only by default
            compression,
            buffer_pool: None,
            running_crc: RwLock::new(0),
        })
    }

    /// Open an existing async segment file for appending
    pub async fn open_for_append<FS>(fs: &FS, path: &Path) -> Result<Self>
    where
        FS: AsyncFileSystem<File = F>,
    {
        let file = fs.open_rw(path).await?;

        // Read header
        let buf = vec![0u8; HEADER_SIZE];
        let (result, buf) = file.read_at(buf, 0).await;
        let bytes_read = result?;

        if bytes_read < HEADER_SIZE {
            return Err(StreamlineError::CorruptedData(
                "Segment header too short".to_string(),
            ));
        }

        let header = SegmentHeader::from_bytes(&buf)?;
        let compression =
            CompressionCodec::from_byte(header.compression).unwrap_or(CompressionCodec::None);

        // Get file size
        let write_position = file.size().await?;

        // Validate batches and find last valid position
        let (valid_position, running_crc) =
            Self::validate_batches_async(&file, write_position, &header).await?;

        Ok(Self {
            path: path.to_path_buf(),
            file: Arc::new(file),
            header: RwLock::new(header),
            write_position: RwLock::new(valid_position),
            sealed: RwLock::new(false),
            compression,
            buffer_pool: None,
            running_crc: RwLock::new(running_crc),
        })
    }

    /// Set the buffer pool for I/O operations
    pub fn with_buffer_pool(mut self, pool: Arc<IoBufferPool>) -> Self {
        self.buffer_pool = Some(pool);
        self
    }

    /// Append a record batch to this segment
    pub async fn append_batch(&self, batch: &RecordBatch) -> Result<()> {
        {
            let sealed = self.sealed.read().await;
            if *sealed {
                return Err(StreamlineError::storage_msg(
                    "Cannot write to sealed segment".to_string(),
                ));
            }
        }

        // Record position before writing
        let batch_position = *self.write_position.read().await;

        // Serialize batch to bincode
        let batch_bytes = bincode::serialize(batch).map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to serialize batch: {}", e))
        })?;

        // Compress if enabled
        let batch_data = compress(&batch_bytes, self.compression)?;

        // Build the write buffer: compression flag (1) + length (4) + data + CRC (4)
        let batch_len = batch_data.len() as u32;
        let crc = crc32fast::hash(&batch_data);

        let total_size = 1 + 4 + batch_data.len() + 4;
        let mut write_buf = Vec::with_capacity(total_size);

        // Compression flag
        write_buf.push(self.compression.as_byte());

        // Length
        write_buf.extend_from_slice(&batch_len.to_le_bytes());

        // Data
        write_buf.extend_from_slice(&batch_data);

        // CRC
        write_buf.extend_from_slice(&crc.to_le_bytes());

        // Write to file
        let (result, _) = self.file.write_at(write_buf, batch_position).await;
        result?;

        // Sync data (not metadata for performance)
        self.file.sync_data().await?;

        // Update running CRC
        {
            let mut running_crc = self.running_crc.write().await;
            let mut hasher = crc32fast::Hasher::new_with_initial(*running_crc);
            hasher.update(&crc.to_le_bytes());
            *running_crc = hasher.finalize();
        }

        // Update header stats
        {
            let mut header = self.header.write().await;
            header.record_count += batch.len() as u32;
            if let Some(last_offset) = batch.last_offset() {
                header.max_offset = last_offset;
            }
            if !batch.records.is_empty() {
                header.max_timestamp = batch.timestamp;
            }
        }

        // Update write position
        {
            let mut pos = self.write_position.write().await;
            *pos = batch_position + total_size as u64;
        }

        Ok(())
    }

    /// Append a single record to this segment
    pub async fn append_record(&self, record: Record) -> Result<()> {
        let mut batch = RecordBatch::new(record.offset, record.timestamp);
        batch.add_record(record);
        self.append_batch(&batch).await
    }

    /// Read all records from this segment
    pub async fn read_all(&self) -> Result<Vec<Record>> {
        let write_pos = *self.write_position.read().await;
        self.read_from_position(HEADER_SIZE as u64, 0, write_pos, usize::MAX)
            .await
    }

    /// Read records starting from a specific offset
    pub async fn read_from_offset(
        &self,
        start_offset: i64,
        max_records: usize,
    ) -> Result<Vec<Record>> {
        let write_pos = *self.write_position.read().await;
        // For simplicity, start from header (a proper implementation would use an index)
        self.read_from_position(HEADER_SIZE as u64, start_offset, write_pos, max_records)
            .await
    }

    /// Internal method to read records from a file position
    async fn read_from_position(
        &self,
        start_position: u64,
        start_offset: i64,
        end_position: u64,
        max_records: usize,
    ) -> Result<Vec<Record>> {
        let header = self.header.read().await;
        let use_bincode = header.flags & FLAG_BINCODE_FORMAT != 0;
        drop(header);

        let mut records = Vec::new();
        let mut position = start_position;

        while position < end_position && records.len() < max_records {
            // Read batch header: compression (1) + length (4)
            let header_buf = vec![0u8; 5];
            let (result, header_buf) = self.file.read_at(header_buf, position).await;
            let bytes_read = result?;

            if bytes_read < 5 {
                break; // End of file or incomplete batch
            }

            let batch_compression =
                CompressionCodec::from_byte(header_buf[0]).unwrap_or(CompressionCodec::None);
            let batch_len =
                u32::from_le_bytes([header_buf[1], header_buf[2], header_buf[3], header_buf[4]])
                    as usize;

            // Sanity check
            if batch_len > 256 * 1024 * 1024 {
                warn!(
                    path = %self.path.display(),
                    position = position,
                    batch_len = batch_len,
                    "Invalid batch length, likely corruption"
                );
                break;
            }

            // Read batch data + CRC
            let data_buf = vec![0u8; batch_len + 4];
            let (result, data_buf) = self.file.read_at(data_buf, position + 5).await;
            let bytes_read = result?;

            if bytes_read < batch_len + 4 {
                break;
            }

            let batch_data = &data_buf[..batch_len];
            let stored_crc = u32::from_le_bytes([
                data_buf[batch_len],
                data_buf[batch_len + 1],
                data_buf[batch_len + 2],
                data_buf[batch_len + 3],
            ]);

            // Verify CRC
            let computed_crc = crc32fast::hash(batch_data);
            if stored_crc != computed_crc {
                return Err(StreamlineError::CorruptedData(
                    "Batch CRC mismatch".to_string(),
                ));
            }

            // Decompress
            let batch_bytes = decompress(batch_data, batch_compression)?;

            // Deserialize
            let batch: RecordBatch = if use_bincode {
                bincode::deserialize(&batch_bytes).map_err(|e| {
                    StreamlineError::CorruptedData(format!("Failed to deserialize batch: {}", e))
                })?
            } else {
                serde_json::from_slice(&batch_bytes).map_err(|e| {
                    StreamlineError::CorruptedData(format!(
                        "Failed to deserialize batch (JSON): {}",
                        e
                    ))
                })?
            };

            // Filter by offset and collect
            for record in batch.records {
                if record.offset >= start_offset {
                    records.push(record);
                    if records.len() >= max_records {
                        break;
                    }
                }
            }

            // Move to next batch: 1 + 4 + data + 4
            position += 1 + 4 + batch_len as u64 + 4;
        }

        Ok(records)
    }

    /// Validate batches and find the last valid position (for recovery)
    async fn validate_batches_async(
        file: &F,
        file_size: u64,
        _header: &SegmentHeader,
    ) -> Result<(u64, u32)> {
        let mut position = HEADER_SIZE as u64;
        let mut last_valid_position = HEADER_SIZE as u64;
        let mut running_crc: u32 = 0;

        while position < file_size {
            // Read batch header
            let header_buf = vec![0u8; 5];
            let (result, header_buf) = file.read_at(header_buf, position).await;

            match result {
                Ok(n) if n >= 5 => {}
                _ => break,
            }

            let batch_len =
                u32::from_le_bytes([header_buf[1], header_buf[2], header_buf[3], header_buf[4]])
                    as usize;

            // Sanity check
            if batch_len > 256 * 1024 * 1024 {
                break;
            }

            // Read data + CRC
            let data_buf = vec![0u8; batch_len + 4];
            let (result, data_buf) = file.read_at(data_buf, position + 5).await;

            match result {
                Ok(n) if n >= batch_len + 4 => {}
                _ => break,
            }

            let batch_data = &data_buf[..batch_len];
            let stored_crc = u32::from_le_bytes([
                data_buf[batch_len],
                data_buf[batch_len + 1],
                data_buf[batch_len + 2],
                data_buf[batch_len + 3],
            ]);

            // Verify CRC
            let computed_crc = crc32fast::hash(batch_data);
            if stored_crc != computed_crc {
                debug!(
                    position = position,
                    "CRC mismatch during validation, truncating"
                );
                break;
            }

            // Chain CRC
            let mut hasher = crc32fast::Hasher::new_with_initial(running_crc);
            hasher.update(&stored_crc.to_le_bytes());
            running_crc = hasher.finalize();

            position += 1 + 4 + batch_len as u64 + 4;
            last_valid_position = position;
        }

        Ok((last_valid_position, running_crc))
    }

    /// Seal the segment (make it read-only)
    pub async fn seal(&self) -> Result<()> {
        {
            let sealed = self.sealed.read().await;
            if *sealed {
                return Ok(());
            }
        }

        // Persist header with final metadata
        self.persist_header().await?;

        // Final sync
        self.file.sync_all().await?;

        // Mark as sealed
        {
            let mut sealed = self.sealed.write().await;
            *sealed = true;
        }

        Ok(())
    }

    /// Persist the segment header to disk
    pub async fn persist_header(&self) -> Result<()> {
        let header = self.header.read().await;
        let header_bytes = header.to_bytes();
        drop(header);

        let (result, _) = self.file.write_at(header_bytes.to_vec(), 0).await;
        result?;

        self.file.sync_all().await?;

        Ok(())
    }

    /// Sync segment data to disk
    pub async fn sync(&self) -> Result<()> {
        self.file.sync_data().await
    }

    /// Get the base offset of this segment
    pub async fn base_offset(&self) -> i64 {
        self.header.read().await.base_offset
    }

    /// Get the max offset of this segment
    pub async fn max_offset(&self) -> i64 {
        self.header.read().await.max_offset
    }

    /// Get the record count in this segment
    pub async fn record_count(&self) -> u32 {
        self.header.read().await.record_count
    }

    /// Check if this segment is sealed
    pub async fn is_sealed(&self) -> bool {
        *self.sealed.read().await
    }

    /// Get the path to this segment file
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Get the current size of this segment in bytes
    pub async fn size(&self) -> u64 {
        *self.write_position.read().await
    }

    /// Get the running CRC for this segment
    pub async fn get_running_crc(&self) -> u32 {
        *self.running_crc.read().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::io_backend::StandardFileSystem;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_async_segment_create_and_read() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.segment");
        let fs = StandardFileSystem::new();

        // Create segment
        let segment = AsyncSegment::create(&fs, &path, 0).await.unwrap();

        // Add records
        let record1 = Record::new(0, 1234567890, None, Bytes::from("value 1"));
        let record2 = Record::new(1, 1234567891, None, Bytes::from("value 2"));

        segment.append_record(record1).await.unwrap();
        segment.append_record(record2).await.unwrap();

        // Read records back
        let records = segment.read_all().await.unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].offset, 0);
        assert_eq!(records[1].offset, 1);
        assert_eq!(records[0].value, Bytes::from("value 1"));
        assert_eq!(records[1].value, Bytes::from("value 2"));
    }

    #[tokio::test]
    async fn test_async_segment_read_from_offset() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.segment");
        let fs = StandardFileSystem::new();

        let segment = AsyncSegment::create(&fs, &path, 0).await.unwrap();

        for i in 0..10 {
            let record = Record::new(i, 1234567890 + i, None, Bytes::from(format!("value {}", i)));
            segment.append_record(record).await.unwrap();
        }

        let records = segment.read_from_offset(5, 100).await.unwrap();
        assert_eq!(records.len(), 5);
        assert_eq!(records[0].offset, 5);
        assert_eq!(records[4].offset, 9);
    }

    #[tokio::test]
    async fn test_async_segment_seal() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.segment");
        let fs = StandardFileSystem::new();

        let segment = AsyncSegment::create(&fs, &path, 0).await.unwrap();
        segment
            .append_record(Record::new(0, 1234567890, None, Bytes::from("test")))
            .await
            .unwrap();

        assert!(!segment.is_sealed().await);
        segment.seal().await.unwrap();
        assert!(segment.is_sealed().await);

        // Writing to sealed segment should fail
        let result = segment
            .append_record(Record::new(1, 1234567891, None, Bytes::from("test")))
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_async_segment_reopen() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.segment");
        let fs = StandardFileSystem::new();

        // Create and write
        {
            let segment = AsyncSegment::create(&fs, &path, 0).await.unwrap();
            for i in 0..5 {
                let record =
                    Record::new(i, 1234567890 + i, None, Bytes::from(format!("value {}", i)));
                segment.append_record(record).await.unwrap();
            }
            segment.seal().await.unwrap();
        }

        // Reopen and verify
        let segment: AsyncSegment<_> = AsyncSegment::open(&fs, &path).await.unwrap();
        let records = segment.read_all().await.unwrap();
        assert_eq!(records.len(), 5);
        assert_eq!(records[0].value, Bytes::from("value 0"));
        assert_eq!(records[4].value, Bytes::from("value 4"));
    }

    #[tokio::test]
    async fn test_async_segment_with_compression() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_lz4.segment");
        let fs = StandardFileSystem::new();

        let segment = AsyncSegment::create_with_compression(&fs, &path, 0, CompressionCodec::Lz4)
            .await
            .unwrap();

        for i in 0..10 {
            let record = Record::new(i, 1234567890 + i, None, Bytes::from(format!("value {}", i)));
            segment.append_record(record).await.unwrap();
        }

        let records = segment.read_all().await.unwrap();
        assert_eq!(records.len(), 10);
        assert_eq!(records[0].value, Bytes::from("value 0"));
        assert_eq!(records[9].value, Bytes::from("value 9"));
    }

    #[tokio::test]
    async fn test_async_segment_batch_write() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_batch.segment");
        let fs = StandardFileSystem::new();

        let segment = AsyncSegment::create(&fs, &path, 0).await.unwrap();

        // Write a batch of records
        let mut batch = RecordBatch::new(0, 1234567890);
        for i in 0..100 {
            batch.add_record(Record::new(
                i,
                1234567890 + i,
                None,
                Bytes::from(format!("value {}", i)),
            ));
        }
        segment.append_batch(&batch).await.unwrap();

        // Read back
        let records = segment.read_all().await.unwrap();
        assert_eq!(records.len(), 100);
        assert_eq!(segment.record_count().await, 100);
    }
}
