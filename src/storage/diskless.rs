//! Diskless storage backend for Streamline
//!
//! This module implements a fully diskless storage mode where all data is
//! written directly to S3 (or S3-compatible object storage) with no local
//! disk requirements. This enables "bring your own cloud" (BYOC) deployments
//! where customers use their own S3 buckets.
//!
//! ## Architecture
//!
//! ```text
//! Producer → WriteBuffer (memory) → S3 Multipart Upload → S3 Bucket
//!                ↓
//!          Flush on timeout
//!          or buffer full
//!
//! Consumer ← ReadCache (memory) ← S3 GetObject ← S3 Bucket
//!                ↑
//!           Cache miss triggers
//!           S3 read + cache populate
//! ```
//!
//! ## Key Features
//!
//! - **Zero Local Disk**: All data stored in customer's S3 bucket
//! - **Write Buffering**: Batches writes in memory before flushing to S3
//! - **Multipart Uploads**: Efficient handling of large segments
//! - **Read Caching**: LRU cache reduces S3 read latency
//! - **Prefetching**: Anticipates sequential reads for better performance
//! - **Retry Logic**: Exponential backoff for transient failures
//!
//! ## Configuration
//!
//! ```bash
//! streamline --storage-mode diskless \
//!            --s3-bucket my-bucket \
//!            --s3-region us-east-1 \
//!            --s3-write-buffer-mb 64 \
//!            --s3-flush-interval-ms 1000 \
//!            --s3-read-cache-mb 256
//! ```

use crate::error::{Result, StreamlineError};
use crate::storage::backend::{
    BufferConfig, PartitionManifest, SegmentBackend, SegmentManifestEntry,
};
use crate::storage::cache::{SegmentCache, SharedSegmentCache};
use crate::storage::record::{Record, RecordBatch};
use crate::storage::storage_mode::RemoteStorageConfig;
use async_trait::async_trait;
use bytes::Bytes;
use object_store::{ObjectStore, PutPayload};
use parking_lot::RwLock;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};

/// Diskless storage backend configuration
#[derive(Clone)]
pub struct DisklessConfig {
    /// S3 bucket name
    pub bucket: String,

    /// S3 key prefix (e.g., "streamline/prod/")
    pub prefix: String,

    /// Remote storage configuration (batch size, timeouts, etc.)
    pub remote: RemoteStorageConfig,

    /// Read cache shared across partitions
    pub read_cache: SharedSegmentCache,
}

impl std::fmt::Debug for DisklessConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DisklessConfig")
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .field("remote", &self.remote)
            .field("read_cache", &"<SharedSegmentCache>")
            .finish()
    }
}

impl DisklessConfig {
    /// Create a new diskless configuration
    pub fn new(bucket: String, prefix: String) -> Self {
        Self {
            bucket,
            prefix,
            remote: RemoteStorageConfig::default(),
            read_cache: Arc::new(SegmentCache::with_defaults()),
        }
    }

    /// Set remote storage configuration
    pub fn with_remote_config(mut self, remote: RemoteStorageConfig) -> Self {
        self.remote = remote;
        self
    }

    /// Set read cache
    pub fn with_read_cache(mut self, cache: SharedSegmentCache) -> Self {
        self.read_cache = cache;
        self
    }
}

/// Write buffer for batching records before S3 upload
struct WriteBuffer {
    /// Buffered records
    records: Vec<Record>,

    /// Current buffer size in bytes
    size_bytes: usize,

    /// Time when first record was added to current batch
    batch_start: Option<Instant>,

    /// Buffer configuration
    config: BufferConfig,
}

impl WriteBuffer {
    fn new(config: BufferConfig) -> Self {
        Self {
            records: Vec::new(),
            size_bytes: 0,
            batch_start: None,
            config,
        }
    }

    fn push(&mut self, record: Record) {
        if self.records.is_empty() {
            self.batch_start = Some(Instant::now());
        }

        self.size_bytes += record.value.len() + record.key.as_ref().map(|k| k.len()).unwrap_or(0);
        self.records.push(record);
    }

    fn should_flush(&self) -> bool {
        // Check size threshold
        if self.size_bytes >= self.config.max_size_bytes {
            return true;
        }

        // Check record count threshold
        if self.records.len() >= self.config.max_records {
            return true;
        }

        // Check time threshold
        if let Some(start) = self.batch_start {
            let elapsed = start.elapsed();
            if elapsed.as_millis() as u64 >= self.config.max_linger_ms {
                return true;
            }
        }

        false
    }

    fn take_batch(&mut self) -> Vec<Record> {
        self.size_bytes = 0;
        self.batch_start = None;
        std::mem::take(&mut self.records)
    }

    fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    #[allow(dead_code)]
    fn len(&self) -> usize {
        self.records.len()
    }
}

/// Statistics for diskless storage operations
#[derive(Debug, Clone, Default)]
pub struct DisklessStats {
    /// Total bytes written to S3
    pub bytes_written: u64,

    /// Total bytes read from S3
    pub bytes_read: u64,

    /// Number of S3 uploads
    pub upload_count: u64,

    /// Number of S3 downloads
    pub download_count: u64,

    /// Number of multipart uploads
    pub multipart_upload_count: u64,

    /// Number of cache hits
    pub cache_hits: u64,

    /// Number of cache misses
    pub cache_misses: u64,

    /// Number of prefetches performed
    pub prefetch_count: u64,

    /// Number of failed uploads (after retries)
    pub failed_uploads: u64,

    /// Number of failed downloads (after retries)
    pub failed_downloads: u64,
}

/// Diskless segment backend - stores data directly in S3
pub struct DisklessSegment {
    /// Topic name
    topic: String,

    /// Partition ID
    partition: i32,

    /// Base offset of this segment
    base_offset: i64,

    /// Current log end offset (next offset to write)
    log_end_offset: Arc<RwLock<i64>>,

    /// Object store (S3) instance
    object_store: Arc<dyn ObjectStore>,

    /// Configuration
    config: DisklessConfig,

    /// Write buffer
    write_buffer: Arc<RwLock<WriteBuffer>>,

    /// Whether this segment is sealed (read-only)
    sealed: Arc<RwLock<bool>>,

    /// Partition manifest (tracks segments)
    manifest: Arc<RwLock<PartitionManifest>>,

    /// Upload semaphore to limit parallelism
    upload_semaphore: Arc<Semaphore>,

    /// Statistics
    stats: Arc<RwLock<DisklessStats>>,

    /// Prefetch queue for sequential reads
    prefetch_queue: Arc<RwLock<VecDeque<i64>>>,
}

impl DisklessSegment {
    /// Create a new diskless segment
    pub fn new(
        topic: String,
        partition: i32,
        base_offset: i64,
        object_store: Arc<dyn ObjectStore>,
        config: DisklessConfig,
    ) -> Self {
        let buffer_config = BufferConfig::batching(
            config.remote.batch_size_bytes,
            config.remote.batch_timeout_ms,
        );

        let upload_semaphore = Arc::new(Semaphore::new(config.remote.upload_parallelism));

        let manifest = PartitionManifest::new(&topic, partition);

        Self {
            topic: topic.clone(),
            partition,
            base_offset,
            log_end_offset: Arc::new(RwLock::new(base_offset)),
            object_store,
            config,
            write_buffer: Arc::new(RwLock::new(WriteBuffer::new(buffer_config))),
            sealed: Arc::new(RwLock::new(false)),
            manifest: Arc::new(RwLock::new(manifest)),
            upload_semaphore,
            stats: Arc::new(RwLock::new(DisklessStats::default())),
            prefetch_queue: Arc::new(RwLock::new(VecDeque::new())),
        }
    }

    /// Get statistics
    pub fn stats(&self) -> DisklessStats {
        self.stats.read().clone()
    }

    /// Upload buffered data to S3 with retry logic
    async fn upload_with_retry(&self, path: &str, data: Bytes) -> Result<()> {
        let max_retries = self.config.remote.max_retries;
        let mut backoff_ms = self.config.remote.retry_backoff_ms;
        let max_backoff_ms = self.config.remote.max_retry_backoff_ms;

        for attempt in 0..=max_retries {
            match self.upload_internal(path, data.clone()).await {
                Ok(()) => {
                    if attempt > 0 {
                        info!("Upload succeeded after {} retries: {}", attempt, path);
                    }
                    return Ok(());
                }
                Err(e) if attempt < max_retries => {
                    warn!(
                        "Upload attempt {} failed for {}: {}. Retrying in {}ms...",
                        attempt + 1,
                        path,
                        e,
                        backoff_ms
                    );
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;

                    // Exponential backoff with cap
                    backoff_ms = (backoff_ms * 2).min(max_backoff_ms);
                }
                Err(e) => {
                    self.stats.write().failed_uploads += 1;
                    return Err(StreamlineError::storage_msg(format!(
                        "Upload failed after {} retries: {}",
                        max_retries, e
                    )));
                }
            }
        }

        unreachable!()
    }

    /// Internal upload implementation
    async fn upload_internal(&self, path: &str, data: Bytes) -> Result<()> {
        let object_path = object_store::path::Path::from(path);

        // Acquire semaphore permit to limit concurrent uploads
        let _permit = self.upload_semaphore.acquire().await.map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to acquire upload semaphore: {}", e))
        })?;

        // Check if we should use multipart upload
        let should_use_multipart = self.config.remote.multipart_enabled
            && data.len() >= self.config.remote.multipart_threshold_bytes;

        if should_use_multipart {
            self.upload_multipart(path, &data).await?;
            self.stats.write().multipart_upload_count += 1;
        } else {
            // Single-part upload
            let payload = PutPayload::from(data.clone());
            self.object_store
                .put(&object_path, payload)
                .await
                .map_err(|e| {
                    StreamlineError::storage_msg(format!("Failed to upload to S3: {}", e))
                })?;
        }

        self.stats.write().bytes_written += data.len() as u64;
        self.stats.write().upload_count += 1;

        debug!(
            "Uploaded {} bytes to S3: {} (multipart: {})",
            data.len(),
            path,
            should_use_multipart
        );

        Ok(())
    }

    /// Upload data using S3 multipart upload
    async fn upload_multipart(&self, path: &str, data: &Bytes) -> Result<()> {
        let object_path = object_store::path::Path::from(path);
        let part_size = self.config.remote.multipart_part_size_bytes;

        // Start multipart upload
        let mut upload_id = self
            .object_store
            .put_multipart(&object_path)
            .await
            .map_err(|e| {
                StreamlineError::storage_msg(format!("Failed to start multipart upload: {}", e))
            })?;

        let mut part_number = 0;
        let mut offset = 0;

        // Upload parts
        while offset < data.len() {
            let end = (offset + part_size).min(data.len());
            let part_data = data.slice(offset..end);

            let part_payload = PutPayload::from(part_data);
            upload_id.put_part(part_payload).await.map_err(|e| {
                StreamlineError::storage_msg(format!(
                    "Failed to upload part {} of multipart upload: {}",
                    part_number, e
                ))
            })?;

            part_number += 1;
            offset = end;

            debug!(
                "Uploaded part {}/{} for {}",
                part_number,
                data.len().div_ceil(part_size),
                path
            );
        }

        // Complete multipart upload
        upload_id.complete().await.map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to complete multipart upload: {}", e))
        })?;

        info!(
            "Completed multipart upload of {} bytes ({} parts) to {}",
            data.len(),
            part_number,
            path
        );

        Ok(())
    }

    /// Download data from S3 with retry logic
    async fn download_with_retry(&self, path: &str) -> Result<Bytes> {
        let max_retries = self.config.remote.max_retries;
        let mut backoff_ms = self.config.remote.retry_backoff_ms;
        let max_backoff_ms = self.config.remote.max_retry_backoff_ms;

        for attempt in 0..=max_retries {
            match self.download_internal(path).await {
                Ok(data) => {
                    if attempt > 0 {
                        info!("Download succeeded after {} retries: {}", attempt, path);
                    }
                    return Ok(data);
                }
                Err(e) if attempt < max_retries => {
                    warn!(
                        "Download attempt {} failed for {}: {}. Retrying in {}ms...",
                        attempt + 1,
                        path,
                        e,
                        backoff_ms
                    );
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;

                    // Exponential backoff with cap
                    backoff_ms = (backoff_ms * 2).min(max_backoff_ms);
                }
                Err(e) => {
                    self.stats.write().failed_downloads += 1;
                    return Err(StreamlineError::storage_msg(format!(
                        "Download failed after {} retries: {}",
                        max_retries, e
                    )));
                }
            }
        }

        unreachable!()
    }

    /// Internal download implementation
    async fn download_internal(&self, path: &str) -> Result<Bytes> {
        let object_path = object_store::path::Path::from(path);

        let result = self.object_store.get(&object_path).await.map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to download from S3: {}", e))
        })?;

        let data = result.bytes().await.map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to read S3 response: {}", e))
        })?;

        self.stats.write().bytes_read += data.len() as u64;
        self.stats.write().download_count += 1;

        debug!("Downloaded {} bytes from S3: {}", data.len(), path);

        Ok(data)
    }

    /// Flush write buffer to S3
    async fn flush_buffer(&self) -> Result<()> {
        let (records, record_count, base_offset) = {
            let mut buffer = self.write_buffer.write();

            if buffer.is_empty() {
                return Ok(());
            }

            let records = buffer.take_batch();
            let record_count = records.len();
            let base_offset = {
                let offset = self.log_end_offset.read();
                *offset
            };

            (records, record_count, base_offset)
        };

        // Serialize records into a batch
        let batch = RecordBatch {
            base_offset,
            timestamp: chrono::Utc::now().timestamp_millis(),
            records,
        };

        // Serialize batch to bytes using bincode
        let data = bincode::serialize(&batch).map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to serialize batch: {}", e))
        })?;
        let data = Bytes::from(data);

        // Generate S3 path
        let path =
            SegmentManifestEntry::segment_path(&self.topic, self.partition, batch.base_offset);

        // Upload to S3
        self.upload_with_retry(&path, data.clone()).await?;

        // Update manifest
        let (manifest_path, manifest_data) = {
            let mut manifest = self.manifest.write();
            let entry = SegmentManifestEntry {
                base_offset: batch.base_offset,
                end_offset: batch.base_offset + record_count as i64 - 1,
                size_bytes: data.len() as u64,
                object_path: path.clone(),
                sealed: false,
                created_at: chrono::Utc::now().timestamp_millis(),
                index_path: None,
            };
            manifest.add_segment(entry);

            // Prepare manifest for upload
            let manifest_path = PartitionManifest::manifest_path(&self.topic, self.partition);
            let manifest_data = manifest.to_json()?;

            (manifest_path, manifest_data)
        };

        // Upload updated manifest
        self.upload_with_retry(&manifest_path, manifest_data)
            .await?;

        info!(
            "Flushed {} records ({} bytes) to S3: {}",
            record_count,
            data.len(),
            path
        );

        Ok(())
    }

    /// Trigger prefetch of next segment (if enabled)
    fn maybe_prefetch(&self, current_offset: i64) {
        if !self.config.remote.prefetch_enabled {
            return;
        }

        let manifest = self.manifest.read();
        let current_segment = manifest.find_segment(current_offset);

        if let Some(current) = current_segment {
            // Find next N segments to prefetch
            let next_offset = current.end_offset + 1;
            let prefetch_count = self.config.remote.prefetch_segments;

            for i in 0..prefetch_count {
                let target_offset = next_offset + (i as i64 * 1000); // Approximate
                if let Some(segment) = manifest.find_segment(target_offset) {
                    let mut queue = self.prefetch_queue.write();
                    if !queue.contains(&segment.base_offset) {
                        queue.push_back(segment.base_offset);

                        // Spawn prefetch task
                        let segment_path = segment.object_path.clone();
                        let cache = self.config.read_cache.clone();
                        let topic = self.topic.clone();
                        let partition = self.partition;
                        let base_offset = segment.base_offset;
                        let end_offset = segment.end_offset;
                        let stats = self.stats.clone();
                        let object_store = self.object_store.clone();
                        let max_retries = self.config.remote.max_retries;
                        let retry_backoff_ms = self.config.remote.retry_backoff_ms;
                        let max_retry_backoff_ms = self.config.remote.max_retry_backoff_ms;

                        tokio::spawn(async move {
                            // Inline download with retry
                            let mut backoff_ms = retry_backoff_ms;
                            for attempt in 0..=max_retries {
                                match async {
                                    let object_path =
                                        object_store::path::Path::from(segment_path.as_str());
                                    let result =
                                        object_store.get(&object_path).await.map_err(|e| {
                                            StreamlineError::storage_msg(format!(
                                                "Failed to prefetch from S3: {}",
                                                e
                                            ))
                                        })?;
                                    result.bytes().await.map_err(|e| {
                                        StreamlineError::storage_msg(format!(
                                            "Failed to read prefetch response: {}",
                                            e
                                        ))
                                    })
                                }
                                .await
                                {
                                    Ok(data) => {
                                        cache.put(&topic, partition, base_offset, end_offset, data);
                                        stats.write().prefetch_count += 1;
                                        debug!("Prefetched segment at offset {}", base_offset);
                                        return;
                                    }
                                    Err(e) if attempt < max_retries => {
                                        warn!(
                                            "Prefetch attempt {} failed: {}. Retrying in {}ms...",
                                            attempt + 1,
                                            e,
                                            backoff_ms
                                        );
                                        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                                        backoff_ms = (backoff_ms * 2).min(max_retry_backoff_ms);
                                    }
                                    Err(e) => {
                                        warn!(
                                            "Prefetch failed after {} retries for offset {}: {}",
                                            max_retries, base_offset, e
                                        );
                                        return;
                                    }
                                }
                            }
                        });
                    }
                }
            }
        }
    }
}

#[async_trait]
impl SegmentBackend for DisklessSegment {
    async fn append_batch(&mut self, batch: &RecordBatch) -> Result<i64> {
        let is_sealed = {
            let sealed = self.sealed.read();
            *sealed
        };

        if is_sealed {
            return Err(StreamlineError::storage_msg(
                "Cannot append to sealed segment".to_string(),
            ));
        }

        let base_offset = {
            let offset = self.log_end_offset.read();
            *offset
        };

        // Add records to write buffer
        let should_flush = {
            let mut buffer = self.write_buffer.write();
            for record in &batch.records {
                buffer.push(record.clone());
            }

            // Update log end offset
            {
                let mut offset = self.log_end_offset.write();
                *offset += batch.records.len() as i64;
            }

            // Check if we should flush
            buffer.should_flush()
        };

        if should_flush {
            self.flush_buffer().await?;
        }

        Ok(base_offset)
    }

    async fn read_from_offset(&self, offset: i64, _max_bytes: usize) -> Result<Vec<Record>> {
        // Check read cache first
        let cached_data = self
            .config
            .read_cache
            .get(&self.topic, self.partition, offset);

        if let Some(cached_data) = cached_data {
            {
                let mut stats = self.stats.write();
                stats.cache_hits += 1;
            }

            // Deserialize records from cached data
            let batch: RecordBatch = bincode::deserialize(&cached_data).map_err(|e| {
                StreamlineError::storage_msg(format!("Failed to deserialize batch: {}", e))
            })?;

            // Maybe trigger prefetch
            self.maybe_prefetch(offset);

            return Ok(batch.records);
        }

        {
            let mut stats = self.stats.write();
            stats.cache_misses += 1;
        }

        // Cache miss - download from S3
        let (path, base_offset, end_offset) = {
            let manifest = self.manifest.read();
            let segment = manifest.find_segment(offset).ok_or_else(|| {
                StreamlineError::storage_msg(format!("Segment not found for offset {}", offset))
            })?;

            (
                segment.object_path.clone(),
                segment.base_offset,
                segment.end_offset,
            )
        };

        // Download from S3
        let data = self.download_with_retry(&path).await?;

        // Populate cache
        self.config.read_cache.put(
            &self.topic,
            self.partition,
            base_offset,
            end_offset,
            data.clone(),
        );

        // Deserialize records
        let batch: RecordBatch = bincode::deserialize(&data).map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to deserialize batch: {}", e))
        })?;

        // Maybe trigger prefetch
        self.maybe_prefetch(offset);

        Ok(batch.records)
    }

    async fn seal(&mut self) -> Result<()> {
        // Flush any pending data
        self.flush_buffer().await?;

        // Mark as sealed
        {
            let mut sealed = self.sealed.write();
            *sealed = true;
        }

        info!(
            "Sealed diskless segment: {}/partition-{}/offset-{}",
            self.topic, self.partition, self.base_offset
        );

        Ok(())
    }

    fn base_offset(&self) -> i64 {
        self.base_offset
    }

    fn log_end_offset(&self) -> i64 {
        let offset = self.log_end_offset.read();
        *offset
    }

    fn storage_mode(&self) -> crate::storage::storage_mode::StorageMode {
        crate::storage::storage_mode::StorageMode::Diskless
    }

    fn size_bytes(&self) -> u64 {
        let stats = self.stats.read();
        stats.bytes_written
    }

    fn is_sealed(&self) -> bool {
        let sealed = self.sealed.read();
        *sealed
    }

    fn identifier(&self) -> String {
        format!(
            "s3://{}/{}/topics/{}/partition-{}/{}",
            self.config.bucket, self.config.prefix, self.topic, self.partition, self.base_offset
        )
    }
}

impl std::fmt::Debug for DisklessSegment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DisklessSegment")
            .field("topic", &self.topic)
            .field("partition", &self.partition)
            .field("base_offset", &self.base_offset)
            .field("log_end_offset", &self.log_end_offset.read())
            .field("sealed", &self.sealed.read())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_diskless_config_creation() {
        let config = DisklessConfig::new("my-bucket".to_string(), "prod/".to_string());
        assert_eq!(config.bucket, "my-bucket");
        assert_eq!(config.prefix, "prod/");
    }

    #[test]
    fn test_write_buffer_push() {
        let config = BufferConfig::batching(1024, 100);
        let mut buffer = WriteBuffer::new(config);

        let record = Record {
            offset: 0,
            timestamp: 0,
            key: None,
            value: Bytes::from(vec![1, 2, 3]),
            headers: Vec::new(),
            crc: None,
        };

        buffer.push(record);
        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer.size_bytes, 3);
    }

    #[test]
    fn test_write_buffer_should_flush_size() {
        let config = BufferConfig::batching(10, 10000);
        let mut buffer = WriteBuffer::new(config);

        // Add data to exceed size threshold
        for _ in 0..5 {
            buffer.push(Record {
                offset: 0,
                timestamp: 0,
                key: None,
                value: Bytes::from(vec![1, 2, 3]),
                headers: Vec::new(),
                crc: None,
            });
        }

        assert!(buffer.should_flush());
    }

    #[test]
    fn test_write_buffer_take_batch() {
        let config = BufferConfig::batching(1024, 100);
        let mut buffer = WriteBuffer::new(config);

        buffer.push(Record {
            offset: 0,
            timestamp: 0,
            key: None,
            value: Bytes::from(vec![1, 2, 3]),
            headers: Vec::new(),
            crc: None,
        });

        let batch = buffer.take_batch();
        assert_eq!(batch.len(), 1);
        assert!(buffer.is_empty());
        assert_eq!(buffer.size_bytes, 0);
    }
}
