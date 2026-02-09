//! S3-backed segment storage for diskless mode
//!
//! This module provides a segment backend that writes directly to object storage (S3/Azure/GCS)
//! instead of local disk. It's used in diskless mode where no local storage is available.
//!
//! ## Design
//!
//! - Record batches are buffered in memory until size or time thresholds are reached
//! - Segments are uploaded as individual objects with offset-based naming
//! - A manifest tracks all segment files for the partition
//! - Reading fetches the required segment and caches it in memory
//!
//! ## Object Storage Layout
//!
//! ```text
//! segments/{topic}/partition-{n}/
//!   manifest.json                    # Partition manifest with segment list
//!   {base_offset}-{end_offset}.seg   # Individual segment files
//! ```

use crate::error::{Result, StreamlineError};
use crate::storage::backend::{PartitionManifest, SegmentBackend, SegmentManifestEntry};
use crate::storage::cache::SegmentCache;
use crate::storage::compression::{compress, decompress, CompressionCodec};
use crate::storage::record::{Record, RecordBatch};
use crate::storage::storage_mode::StorageMode;
use crate::storage::tiering::{TieringBackend, TieringConfig};
use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use futures_util::StreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutPayload};
use std::collections::VecDeque;
use std::fmt::Debug;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tracing::{debug, info};

/// Magic bytes for S3 segment format
const S3_SEGMENT_MAGIC: &[u8; 4] = b"S3SG";

/// S3 segment format version
const S3_SEGMENT_VERSION: u16 = 1;

/// S3-backed segment for diskless mode
///
/// Buffers records and uploads them to object storage in segments.
pub struct S3Segment {
    /// Object store backend
    store: Arc<dyn ObjectStore>,

    /// Topic name
    topic: String,

    /// Partition ID
    partition: i32,

    /// Base offset for this segment
    base_offset: i64,

    /// Log end offset (next offset to be written)
    log_end_offset: AtomicI64,

    /// Whether this segment is sealed
    sealed: AtomicBool,

    /// Buffered record batches waiting to be uploaded
    buffer: RwLock<SegmentBuffer>,

    /// Current buffer size in bytes
    buffer_size: AtomicU64,

    /// Batch size threshold (bytes)
    batch_size_bytes: usize,

    /// Batch timeout (milliseconds)
    batch_timeout_ms: u64,

    /// Compression codec
    compression: CompressionCodec,

    /// Current manifest for this partition (shared reference)
    manifest: RwLock<PartitionManifest>,

    /// Statistics
    stats: S3SegmentStats,

    /// Optional shared read cache for segments
    cache: Option<Arc<SegmentCache>>,
}

/// Internal buffer for segment data
#[derive(Default)]
struct SegmentBuffer {
    /// Buffered record batches
    batches: VecDeque<RecordBatch>,

    /// Time when first batch was added
    batch_start: Option<Instant>,

    /// Cached segment data (downloaded from S3) - reserved for future caching implementation
    #[allow(dead_code)]
    cached_records: Vec<Record>,

    /// Whether cache is valid - reserved for future caching implementation
    #[allow(dead_code)]
    cache_valid: bool,
}

/// Statistics for S3 segment operations
#[derive(Debug, Default)]
struct S3SegmentStats {
    batches_buffered: AtomicU64,
    records_buffered: AtomicU64,
    uploads: AtomicU64,
    bytes_uploaded: AtomicU64,
    downloads: AtomicU64,
    bytes_downloaded: AtomicU64,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
}

impl S3Segment {
    /// Create a new S3 segment
    ///
    /// # Arguments
    /// * `config` - Tiering configuration with backend settings
    /// * `topic` - Topic name
    /// * `partition` - Partition ID
    /// * `base_offset` - First offset in this segment
    /// * `batch_size_bytes` - Upload when buffer reaches this size
    /// * `batch_timeout_ms` - Upload after this time even if size not reached
    pub async fn new(
        config: &TieringConfig,
        topic: String,
        partition: i32,
        base_offset: i64,
        batch_size_bytes: usize,
        batch_timeout_ms: u64,
    ) -> Result<Self> {
        let store = create_object_store(&config.backend)?;

        let segment = Self {
            store,
            topic: topic.clone(),
            partition,
            base_offset,
            log_end_offset: AtomicI64::new(base_offset),
            sealed: AtomicBool::new(false),
            buffer: RwLock::new(SegmentBuffer::default()),
            buffer_size: AtomicU64::new(0),
            batch_size_bytes,
            batch_timeout_ms,
            compression: CompressionCodec::None,
            manifest: RwLock::new(PartitionManifest::new(&topic, partition)),
            stats: S3SegmentStats::default(),
            cache: None,
        };

        // Try to load existing manifest
        segment.load_manifest().await?;

        info!(
            topic = %topic,
            partition = partition,
            base_offset = base_offset,
            batch_size_bytes = batch_size_bytes,
            "S3 segment initialized"
        );

        Ok(segment)
    }

    /// Create with custom compression
    pub async fn with_compression(
        config: &TieringConfig,
        topic: String,
        partition: i32,
        base_offset: i64,
        batch_size_bytes: usize,
        batch_timeout_ms: u64,
        compression: CompressionCodec,
    ) -> Result<Self> {
        let mut segment = Self::new(
            config,
            topic,
            partition,
            base_offset,
            batch_size_bytes,
            batch_timeout_ms,
        )
        .await?;
        segment.compression = compression;
        Ok(segment)
    }

    /// Create a new S3 segment with an existing object store
    ///
    /// This is useful when the object store has already been created by a factory.
    ///
    /// # Arguments
    /// * `store` - Pre-configured object store
    /// * `topic` - Topic name
    /// * `partition` - Partition ID
    /// * `base_offset` - First offset in this segment
    /// * `batch_size_bytes` - Upload when buffer reaches this size
    /// * `batch_timeout_ms` - Upload after this time even if size not reached
    pub async fn new_with_store(
        store: Arc<dyn ObjectStore>,
        topic: &str,
        partition: i32,
        base_offset: i64,
        batch_size_bytes: usize,
        batch_timeout_ms: u64,
    ) -> Result<Self> {
        let segment = Self {
            store,
            topic: topic.to_string(),
            partition,
            base_offset,
            log_end_offset: AtomicI64::new(base_offset),
            sealed: AtomicBool::new(false),
            buffer: RwLock::new(SegmentBuffer::default()),
            buffer_size: AtomicU64::new(0),
            batch_size_bytes,
            batch_timeout_ms,
            compression: CompressionCodec::None,
            manifest: RwLock::new(PartitionManifest::new(topic, partition)),
            stats: S3SegmentStats::default(),
            cache: None,
        };

        // Try to load existing manifest
        segment.load_manifest().await?;

        info!(
            topic = %topic,
            partition = partition,
            base_offset = base_offset,
            batch_size_bytes = batch_size_bytes,
            "S3 segment initialized (with existing store)"
        );

        Ok(segment)
    }

    /// Set the shared read cache
    ///
    /// When a cache is set, downloaded segments will be cached and subsequent
    /// reads will be served from cache if available.
    pub fn with_cache(mut self, cache: Arc<SegmentCache>) -> Self {
        self.cache = Some(cache);
        self
    }

    /// Set the cache on an existing segment
    pub fn set_cache(&mut self, cache: Arc<SegmentCache>) {
        self.cache = Some(cache);
    }

    /// Get cache hit statistics
    pub fn cache_hits(&self) -> u64 {
        self.stats.cache_hits.load(Ordering::Relaxed)
    }

    /// Get cache miss statistics
    pub fn cache_misses(&self) -> u64 {
        self.stats.cache_misses.load(Ordering::Relaxed)
    }

    /// Get the object path prefix for this partition
    fn segment_prefix(&self) -> String {
        format!("segments/{}/partition-{}", self.topic, self.partition)
    }

    /// Get the manifest object path
    fn manifest_path(&self) -> ObjectPath {
        ObjectPath::from(format!("{}/manifest.json", self.segment_prefix()))
    }

    /// Get the path for a segment file
    fn segment_path(&self, base_offset: i64, end_offset: i64) -> ObjectPath {
        ObjectPath::from(format!(
            "{}/{:020}-{:020}.seg",
            self.segment_prefix(),
            base_offset,
            end_offset
        ))
    }

    /// Load manifest from object storage
    async fn load_manifest(&self) -> Result<()> {
        let path = self.manifest_path();

        match self.store.get(&path).await {
            Ok(result) => {
                let data = result.bytes().await.map_err(|e| {
                    StreamlineError::storage_msg(format!("Failed to read manifest: {}", e))
                })?;

                let manifest: PartitionManifest = serde_json::from_slice(&data)?;

                // Update log end offset based on manifest
                self.log_end_offset
                    .store(manifest.log_end_offset, Ordering::SeqCst);

                *self.manifest.write().await = manifest;

                debug!(
                    topic = %self.topic,
                    partition = self.partition,
                    "Loaded segment manifest from S3"
                );
            }
            Err(object_store::Error::NotFound { .. }) => {
                debug!(
                    topic = %self.topic,
                    partition = self.partition,
                    "No existing segment manifest found"
                );
            }
            Err(e) => {
                return Err(StreamlineError::storage_msg(format!(
                    "Failed to load manifest: {}",
                    e
                )));
            }
        }

        Ok(())
    }

    /// Save manifest to object storage
    async fn save_manifest(&self) -> Result<()> {
        let manifest = self.manifest.read().await;
        let data = serde_json::to_vec_pretty(&*manifest)?;

        let path = self.manifest_path();
        self.store
            .put(&path, PutPayload::from_bytes(Bytes::from(data)))
            .await
            .map_err(|e| StreamlineError::storage_msg(format!("Failed to save manifest: {}", e)))?;

        debug!(
            topic = %self.topic,
            partition = self.partition,
            segments = manifest.segments.len(),
            "Saved segment manifest to S3"
        );

        Ok(())
    }

    /// Serialize batches to segment format
    fn serialize_segment(batches: &[RecordBatch], compression: CompressionCodec) -> Result<Bytes> {
        let mut buf = BytesMut::new();

        // Magic header
        buf.put_slice(S3_SEGMENT_MAGIC);

        // Version
        buf.put_u16_le(S3_SEGMENT_VERSION);

        // Compression
        buf.put_u8(compression.as_byte());

        // Reserved
        buf.put_u8(0);

        // Batch count
        buf.put_u32_le(batches.len() as u32);

        // Serialize each batch
        for batch in batches {
            let batch_json = serde_json::to_vec(batch)?;
            let batch_data = compress(&batch_json, compression)?;

            // Batch length
            buf.put_u32_le(batch_data.len() as u32);

            // Batch data
            buf.put_slice(&batch_data);

            // CRC32
            let crc = crc32fast::hash(&batch_data);
            buf.put_u32_le(crc);
        }

        Ok(buf.freeze())
    }

    /// Deserialize segment to batches
    fn deserialize_segment(data: &[u8]) -> Result<Vec<RecordBatch>> {
        if data.len() < 12 {
            return Err(StreamlineError::CorruptedData(
                "S3 segment too short".to_string(),
            ));
        }

        // Check magic
        if &data[0..4] != S3_SEGMENT_MAGIC {
            return Err(StreamlineError::CorruptedData(
                "Invalid S3 segment magic".to_string(),
            ));
        }

        // Version
        let _version = u16::from_le_bytes([data[4], data[5]]);

        // Compression
        let compression = CompressionCodec::from_byte(data[6]).unwrap_or(CompressionCodec::None);

        // Batch count
        let batch_count = u32::from_le_bytes([data[8], data[9], data[10], data[11]]) as usize;

        let mut batches = Vec::with_capacity(batch_count);
        let mut pos = 12;

        for _ in 0..batch_count {
            if pos + 4 > data.len() {
                return Err(StreamlineError::CorruptedData(
                    "S3 segment batch length truncated".to_string(),
                ));
            }

            let batch_len =
                u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
                    as usize;
            pos += 4;

            if pos + batch_len + 4 > data.len() {
                return Err(StreamlineError::CorruptedData(
                    "S3 segment batch truncated".to_string(),
                ));
            }

            // Read batch data
            let batch_data = &data[pos..pos + batch_len];
            pos += batch_len;

            // Verify CRC
            let stored_crc =
                u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
            let computed_crc = crc32fast::hash(batch_data);
            pos += 4;

            if stored_crc != computed_crc {
                return Err(StreamlineError::CorruptedData(
                    "S3 segment batch CRC mismatch".to_string(),
                ));
            }

            // Decompress and deserialize
            let batch_json = decompress(batch_data, compression)?;
            let batch: RecordBatch = serde_json::from_slice(&batch_json)?;
            batches.push(batch);
        }

        Ok(batches)
    }

    /// Check if buffer should be flushed
    fn should_flush(&self, buffer: &SegmentBuffer) -> bool {
        if buffer.batches.is_empty() {
            return false;
        }

        // Size threshold
        if self.buffer_size.load(Ordering::Relaxed) >= self.batch_size_bytes as u64 {
            return true;
        }

        // Time threshold
        if let Some(start) = buffer.batch_start {
            if start.elapsed().as_millis() as u64 >= self.batch_timeout_ms {
                return true;
            }
        }

        false
    }

    /// Flush buffer to S3
    async fn flush_buffer(&self) -> Result<Option<SegmentManifestEntry>> {
        let batches: Vec<RecordBatch> = {
            let mut buffer = self.buffer.write().await;
            if buffer.batches.is_empty() {
                return Ok(None);
            }
            buffer.batches.drain(..).collect()
        };

        if batches.is_empty() {
            return Ok(None);
        }

        let base_offset = batches.first().and_then(|b| b.first_offset()).unwrap_or(0);
        let end_offset = batches.last().and_then(|b| b.last_offset()).unwrap_or(0);

        // Serialize segment
        let data = Self::serialize_segment(&batches, self.compression)?;
        let size_bytes = data.len() as u64;

        // Upload to S3
        let path = self.segment_path(base_offset, end_offset);
        self.store
            .put(&path, PutPayload::from_bytes(data))
            .await
            .map_err(|e| {
                StreamlineError::storage_msg(format!("Failed to upload segment: {}", e))
            })?;

        // Update stats
        self.stats.uploads.fetch_add(1, Ordering::Relaxed);
        self.stats
            .bytes_uploaded
            .fetch_add(size_bytes, Ordering::Relaxed);

        // Create manifest entry
        let entry = SegmentManifestEntry {
            base_offset,
            end_offset,
            size_bytes,
            object_path: path.to_string(),
            sealed: self.sealed.load(Ordering::Relaxed),
            created_at: chrono::Utc::now().timestamp_millis(),
            index_path: None,
        };

        // Update manifest
        {
            let mut manifest = self.manifest.write().await;
            manifest.add_segment(entry.clone());
            manifest.log_end_offset = end_offset + 1;
            manifest.last_modified = chrono::Utc::now().timestamp_millis();
        }

        // Save manifest
        self.save_manifest().await?;

        // Reset buffer state
        {
            let mut buffer = self.buffer.write().await;
            buffer.batch_start = None;
            buffer.cache_valid = false;
        }
        self.buffer_size.store(0, Ordering::Relaxed);

        info!(
            topic = %self.topic,
            partition = self.partition,
            base_offset = base_offset,
            end_offset = end_offset,
            size_bytes = size_bytes,
            "Segment uploaded to S3"
        );

        Ok(Some(entry))
    }

    /// Download and cache a segment
    ///
    /// If a cache is configured, this method will:
    /// 1. Check the cache first for segment data
    /// 2. On cache miss, download from object storage and cache the result
    async fn download_segment(&self, entry: &SegmentManifestEntry) -> Result<Vec<Record>> {
        // Check cache first if available
        if let Some(ref cache) = self.cache {
            if let Some(cached_data) = cache.get(&self.topic, self.partition, entry.base_offset) {
                self.stats.cache_hits.fetch_add(1, Ordering::Relaxed);
                let batches = Self::deserialize_segment(&cached_data)?;
                let records: Vec<Record> = batches.into_iter().flat_map(|b| b.records).collect();
                return Ok(records);
            }
            self.stats.cache_misses.fetch_add(1, Ordering::Relaxed);
        }

        // Cache miss or no cache - download from object storage
        let path = ObjectPath::from(entry.object_path.clone());

        let result = self.store.get(&path).await.map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to download segment: {}", e))
        })?;

        let data = result
            .bytes()
            .await
            .map_err(|e| StreamlineError::storage_msg(format!("Failed to read segment: {}", e)))?;

        self.stats.downloads.fetch_add(1, Ordering::Relaxed);
        self.stats
            .bytes_downloaded
            .fetch_add(data.len() as u64, Ordering::Relaxed);

        // Cache the segment data for future reads
        if let Some(ref cache) = self.cache {
            cache.put(
                &self.topic,
                self.partition,
                entry.base_offset,
                entry.end_offset + 1, // end_offset is exclusive in cache
                data.clone(),
            );
        }

        let batches = Self::deserialize_segment(&data)?;
        let records: Vec<Record> = batches.into_iter().flat_map(|b| b.records).collect();

        Ok(records)
    }

    /// Get all records from buffered batches
    fn get_buffered_records(buffer: &SegmentBuffer) -> Vec<Record> {
        buffer
            .batches
            .iter()
            .flat_map(|b| b.records.clone())
            .collect()
    }

    /// Estimate size of a record batch
    fn estimate_batch_size(batch: &RecordBatch) -> usize {
        // Base overhead
        let mut size = 64;

        for record in &batch.records {
            size += 32; // Record overhead
            if let Some(ref key) = record.key {
                size += key.len();
            }
            size += record.value.len();
            for header in &record.headers {
                size += header.key.len() + header.value.len();
            }
        }

        size
    }
}

impl Debug for S3Segment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Segment")
            .field("topic", &self.topic)
            .field("partition", &self.partition)
            .field("base_offset", &self.base_offset)
            .field(
                "log_end_offset",
                &self.log_end_offset.load(Ordering::Relaxed),
            )
            .field("sealed", &self.sealed.load(Ordering::Relaxed))
            .field("batch_size_bytes", &self.batch_size_bytes)
            .finish()
    }
}

#[async_trait]
impl SegmentBackend for S3Segment {
    async fn append_batch(&mut self, batch: &RecordBatch) -> Result<i64> {
        if self.sealed.load(Ordering::Relaxed) {
            return Err(StreamlineError::storage_msg(
                "Cannot write to sealed segment".to_string(),
            ));
        }

        let base_offset = batch
            .first_offset()
            .unwrap_or(self.log_end_offset.load(Ordering::Relaxed));
        let batch_size = Self::estimate_batch_size(batch);

        // Add to buffer
        {
            let mut buffer = self.buffer.write().await;
            if buffer.batch_start.is_none() {
                buffer.batch_start = Some(Instant::now());
            }
            buffer.batches.push_back(batch.clone());
            buffer.cache_valid = false;
        }

        self.buffer_size
            .fetch_add(batch_size as u64, Ordering::Relaxed);
        self.stats.batches_buffered.fetch_add(1, Ordering::Relaxed);
        self.stats
            .records_buffered
            .fetch_add(batch.records.len() as u64, Ordering::Relaxed);

        // Update log end offset
        if let Some(last_offset) = batch.last_offset() {
            self.log_end_offset
                .fetch_max(last_offset + 1, Ordering::SeqCst);
        }

        // Check if we should flush
        let should_flush = {
            let buffer = self.buffer.read().await;
            self.should_flush(&buffer)
        };

        if should_flush {
            self.flush_buffer().await?;
        }

        Ok(base_offset)
    }

    async fn read_from_offset(&self, offset: i64, max_bytes: usize) -> Result<Vec<Record>> {
        let mut records = Vec::new();
        let mut bytes_read = 0usize;

        // First, check buffered records
        {
            let buffer = self.buffer.read().await;
            for record in Self::get_buffered_records(&buffer) {
                if record.offset >= offset && bytes_read < max_bytes {
                    bytes_read += record.value.len();
                    records.push(record);
                }
            }
        }

        // Then check segments in manifest
        let manifest = self.manifest.read().await;
        for segment_entry in &manifest.segments {
            if segment_entry.end_offset < offset {
                continue; // Skip segments entirely before our offset
            }

            if bytes_read >= max_bytes {
                break;
            }

            let segment_records = self.download_segment(segment_entry).await?;
            for record in segment_records {
                if record.offset >= offset && bytes_read < max_bytes {
                    bytes_read += record.value.len();
                    records.push(record);
                }
            }
        }

        // Sort by offset to ensure correct order
        records.sort_by_key(|r| r.offset);

        Ok(records)
    }

    async fn seal(&mut self) -> Result<()> {
        // Flush any remaining buffered data
        self.flush_buffer().await?;

        self.sealed.store(true, Ordering::SeqCst);

        // Update manifest to mark segment as sealed
        {
            let mut manifest = self.manifest.write().await;
            if let Some(last_segment) = manifest.segments.last_mut() {
                last_segment.sealed = true;
            }
            manifest.last_modified = chrono::Utc::now().timestamp_millis();
        }

        self.save_manifest().await?;

        info!(
            topic = %self.topic,
            partition = self.partition,
            base_offset = self.base_offset,
            "S3 segment sealed"
        );

        Ok(())
    }

    fn base_offset(&self) -> i64 {
        self.base_offset
    }

    fn log_end_offset(&self) -> i64 {
        self.log_end_offset.load(Ordering::Relaxed)
    }

    fn storage_mode(&self) -> StorageMode {
        StorageMode::Diskless
    }

    fn size_bytes(&self) -> u64 {
        // Sum of all uploaded segments plus buffer
        let manifest_size: u64 = {
            // Using try_read to avoid async in sync context
            // This is a best-effort size calculation
            0 // Will be updated when we have access to manifest
        };

        manifest_size + self.buffer_size.load(Ordering::Relaxed)
    }

    fn is_sealed(&self) -> bool {
        self.sealed.load(Ordering::Relaxed)
    }

    fn identifier(&self) -> String {
        format!(
            "s3://{}/partition-{}/{}",
            self.topic, self.partition, self.base_offset
        )
    }
}

/// Local segment backend wrapper for compatibility
///
/// Wraps the existing local Segment to implement the SegmentBackend trait.
pub struct LocalSegmentBackend {
    /// Wrapped local segment
    segment: crate::storage::segment::Segment,
}

impl LocalSegmentBackend {
    /// Create a new local segment backend
    pub fn create(path: &std::path::Path, base_offset: i64) -> Result<Self> {
        let segment = crate::storage::segment::Segment::create(path, base_offset)?;
        Ok(Self { segment })
    }

    /// Open an existing segment
    pub fn open(path: &std::path::Path) -> Result<Self> {
        let segment = crate::storage::segment::Segment::open(path)?;
        Ok(Self { segment })
    }

    /// Open for append
    pub fn open_for_append(path: &std::path::Path) -> Result<Self> {
        let segment = crate::storage::segment::Segment::open_for_append(path)?;
        Ok(Self { segment })
    }
}

impl Debug for LocalSegmentBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalSegmentBackend")
            .field("base_offset", &self.segment.base_offset())
            .field("sealed", &self.segment.is_sealed())
            .finish()
    }
}

#[async_trait]
impl SegmentBackend for LocalSegmentBackend {
    async fn append_batch(&mut self, batch: &RecordBatch) -> Result<i64> {
        let base_offset = batch
            .first_offset()
            .unwrap_or(self.segment.max_offset() + 1);
        self.segment.append_batch(batch)?;
        Ok(base_offset)
    }

    async fn read_from_offset(&self, offset: i64, max_bytes: usize) -> Result<Vec<Record>> {
        // Estimate max records based on average record size
        let estimated_avg_size = 1024; // 1KB average
        let max_records = max_bytes / estimated_avg_size;
        self.segment.read_from_offset(offset, max_records.max(100))
    }

    async fn seal(&mut self) -> Result<()> {
        self.segment.seal()
    }

    fn base_offset(&self) -> i64 {
        self.segment.base_offset()
    }

    fn log_end_offset(&self) -> i64 {
        // log_end_offset is max_offset + 1 (next offset to be written)
        self.segment.max_offset() + 1
    }

    fn storage_mode(&self) -> StorageMode {
        StorageMode::Local
    }

    fn size_bytes(&self) -> u64 {
        self.segment.size()
    }

    fn is_sealed(&self) -> bool {
        self.segment.is_sealed()
    }

    fn identifier(&self) -> String {
        self.segment.path().display().to_string()
    }
}

/// Create an object store from the backend configuration
fn create_object_store(backend: &TieringBackend) -> Result<Arc<dyn ObjectStore>> {
    match backend {
        TieringBackend::Local { path } => {
            std::fs::create_dir_all(path).map_err(|e| {
                StreamlineError::storage_msg(format!("Failed to create segment directory: {}", e))
            })?;

            let store =
                object_store::local::LocalFileSystem::new_with_prefix(path).map_err(|e| {
                    StreamlineError::storage_msg(format!("Failed to create local store: {}", e))
                })?;

            Ok(Arc::new(store))
        }

        TieringBackend::S3 {
            bucket,
            region,
            endpoint,
            access_key_id,
            secret_access_key,
        } => {
            let mut builder = object_store::aws::AmazonS3Builder::new()
                .with_bucket_name(bucket)
                .with_region(region);

            if let Some(endpoint) = endpoint {
                builder = builder.with_endpoint(endpoint);
            }

            if let Some(key_id) = access_key_id {
                builder = builder.with_access_key_id(key_id);
            }

            if let Some(secret) = secret_access_key {
                builder = builder.with_secret_access_key(secret);
            }

            let store = builder.build().map_err(|e| {
                StreamlineError::storage_msg(format!("Failed to create S3 store: {}", e))
            })?;

            Ok(Arc::new(store))
        }

        TieringBackend::Azure {
            account,
            container,
            access_key,
        } => {
            let mut builder = object_store::azure::MicrosoftAzureBuilder::new()
                .with_account(account)
                .with_container_name(container);

            if let Some(key) = access_key {
                builder = builder.with_access_key(key);
            }

            let store = builder.build().map_err(|e| {
                StreamlineError::storage_msg(format!("Failed to create Azure store: {}", e))
            })?;

            Ok(Arc::new(store))
        }

        TieringBackend::Gcs {
            bucket,
            service_account_key,
        } => {
            let mut builder =
                object_store::gcp::GoogleCloudStorageBuilder::new().with_bucket_name(bucket);

            if let Some(key_path) = service_account_key {
                builder = builder.with_service_account_path(key_path.to_string_lossy());
            }

            let store = builder.build().map_err(|e| {
                StreamlineError::storage_msg(format!("Failed to create GCS store: {}", e))
            })?;

            Ok(Arc::new(store))
        }
    }
}

// =============================================================================
// S3SegmentStateless - Externalized State for Multi-Agent Deployments
// =============================================================================

use crate::storage::cross_topic_buffer::CrossTopicBufferManager;
use crate::storage::state_store::{PartitionState, StateStore};

/// S3 segment with externalized state for truly stateless agents
///
/// Unlike `S3Segment`, this variant stores partition state (LEO, HWM, version)
/// in an external state store (e.g., DynamoDB), enabling:
///
/// - **Instant failover**: New agent reads state from external store (~5ms)
/// - **Multi-agent coordination**: Lease-based ownership prevents conflicts
/// - **Cross-topic batching**: Shared buffer manager reduces S3 PUT costs
///
/// ## Architecture
///
/// ```text
/// ┌──────────────────────────────────────────────────────────────┐
/// │                    S3SegmentStateless                         │
/// ├──────────────────────────────────────────────────────────────┤
/// │  state_store: Arc<dyn StateStore>  ← LEO, HWM, version       │
/// │  buffer_manager: Arc<CrossTopicBufferManager>  ← Shared      │
/// │  store: Arc<dyn ObjectStore>  ← S3/Azure/GCS reads           │
/// │  cached_state: RwLock<CachedPartitionState>  ← Local cache   │
/// └──────────────────────────────────────────────────────────────┘
///                            │
///         ┌──────────────────┼──────────────────┐
///         ▼                  ▼                  ▼
/// ┌───────────────┐  ┌───────────────┐  ┌───────────────┐
/// │  DynamoDB     │  │  S3 Bucket    │  │  Read Cache   │
/// │  (State)      │  │  (Data)       │  │  (Optional)   │
/// └───────────────┘  └───────────────┘  └───────────────┘
/// ```
pub struct S3SegmentStateless {
    /// External state store for partition metadata
    state_store: Arc<dyn StateStore>,

    /// Cross-topic buffer manager for efficient S3 batching
    buffer_manager: Arc<CrossTopicBufferManager>,

    /// Object store for reading segment data
    store: Arc<dyn ObjectStore>,

    /// Topic name
    topic: String,

    /// Partition ID
    partition: i32,

    /// Locally cached partition state (refreshed on each operation)
    cached_state: RwLock<CachedPartitionState>,

    /// Whether this segment is sealed (no more writes)
    sealed: AtomicBool,

    /// Compression codec (reserved for future segment serialization)
    #[allow(dead_code)]
    compression: CompressionCodec,

    /// Optional shared read cache
    cache: Option<Arc<SegmentCache>>,

    /// Agent ID for lease tracking
    agent_id: String,

    /// Statistics
    stats: S3StatelessStats,
}

/// Cached partition state with version tracking
#[derive(Debug, Clone, Default)]
struct CachedPartitionState {
    /// Log end offset
    log_end_offset: i64,
    /// High watermark
    high_watermark: i64,
    /// Version for optimistic locking
    version: u64,
    /// When the cache was last refreshed
    last_refresh_ms: i64,
}

/// Statistics for stateless segment operations
#[derive(Debug, Default)]
struct S3StatelessStats {
    /// State store reads
    state_reads: AtomicU64,
    /// State store writes
    state_writes: AtomicU64,
    /// Version conflicts (optimistic locking)
    version_conflicts: AtomicU64,
    /// Records appended
    records_appended: AtomicU64,
    /// Segment downloads
    downloads: AtomicU64,
    /// Cache hits
    cache_hits: AtomicU64,
    /// Cache misses
    cache_misses: AtomicU64,
}

/// Configuration for S3SegmentStateless
#[derive(Debug, Clone)]
pub struct S3StatelessConfig {
    /// Compression codec for segments
    pub compression: CompressionCodec,
    /// Whether to initialize partition state if it doesn't exist
    pub auto_initialize: bool,
    /// Cache refresh interval in milliseconds (0 = always refresh)
    pub cache_refresh_interval_ms: u64,
}

impl Default for S3StatelessConfig {
    fn default() -> Self {
        Self {
            compression: CompressionCodec::Lz4,
            auto_initialize: true,
            cache_refresh_interval_ms: 1000, // Refresh every second
        }
    }
}

impl S3SegmentStateless {
    /// Create a new stateless S3 segment
    pub async fn new(
        state_store: Arc<dyn StateStore>,
        buffer_manager: Arc<CrossTopicBufferManager>,
        store: Arc<dyn ObjectStore>,
        topic: String,
        partition: i32,
        agent_id: String,
        config: S3StatelessConfig,
    ) -> Result<Self> {
        let segment = Self {
            state_store,
            buffer_manager,
            store,
            topic: topic.clone(),
            partition,
            cached_state: RwLock::new(CachedPartitionState::default()),
            sealed: AtomicBool::new(false),
            compression: config.compression,
            cache: None,
            agent_id,
            stats: S3StatelessStats::default(),
        };

        // Initialize or load partition state
        if config.auto_initialize {
            segment.ensure_partition_state().await?;
        }

        // Refresh cache from state store
        segment.refresh_state().await?;

        info!(
            topic = %topic,
            partition = partition,
            agent_id = %segment.agent_id,
            "S3SegmentStateless initialized"
        );

        Ok(segment)
    }

    /// Set the shared read cache
    pub fn with_cache(mut self, cache: Arc<SegmentCache>) -> Self {
        self.cache = Some(cache);
        self
    }

    /// Ensure partition state exists in the state store
    async fn ensure_partition_state(&self) -> Result<()> {
        let existing = self
            .state_store
            .get_partition_state(&self.topic, self.partition)
            .await?;

        if existing.is_none() {
            let state = PartitionState::new(&self.topic, self.partition);
            if let Err(e) = self.state_store.create_partition_state(state).await {
                // Ignore "already exists" errors (race condition with another agent)
                if !e.to_string().contains("already exists") {
                    return Err(e);
                }
            }
        }

        Ok(())
    }

    /// Refresh cached state from the state store
    async fn refresh_state(&self) -> Result<()> {
        self.stats.state_reads.fetch_add(1, Ordering::Relaxed);

        let state = self
            .state_store
            .get_partition_state(&self.topic, self.partition)
            .await?
            .ok_or_else(|| {
                StreamlineError::storage_msg(format!(
                    "Partition {}:{} not found in state store",
                    self.topic, self.partition
                ))
            })?;

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        let mut cached = self.cached_state.write().await;
        cached.log_end_offset = state.log_end_offset;
        cached.high_watermark = state.high_watermark;
        cached.version = state.version;
        cached.last_refresh_ms = now_ms;

        Ok(())
    }

    /// Get the current log end offset
    pub async fn log_end_offset(&self) -> i64 {
        self.cached_state.read().await.log_end_offset
    }

    /// Get the current high watermark
    pub async fn high_watermark(&self) -> i64 {
        self.cached_state.read().await.high_watermark
    }

    /// Get the cached version for optimistic locking
    pub async fn version(&self) -> u64 {
        self.cached_state.read().await.version
    }

    /// Get the topic name
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Get the partition ID
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Check if this segment is sealed
    pub fn is_sealed(&self) -> bool {
        self.sealed.load(Ordering::Acquire)
    }

    /// Append a record batch to the buffer
    ///
    /// Records are buffered in the CrossTopicBufferManager and will be
    /// flushed to S3 when size/time thresholds are reached.
    pub async fn append_batch(&self, batch: &RecordBatch) -> Result<i64> {
        if self.sealed.load(Ordering::Acquire) {
            return Err(StreamlineError::storage_msg("Segment is sealed".into()));
        }

        // Get current state for offset assignment
        let cached = self.cached_state.read().await;
        let base_offset = cached.log_end_offset;
        let version = cached.version;
        drop(cached);

        // Calculate new LEO
        let record_count = batch.records.len() as i64;
        let new_leo = base_offset + record_count;

        // Add to cross-topic buffer
        self.buffer_manager.append(
            &self.topic,
            self.partition,
            batch.records.clone(),
            base_offset,
        )?;

        // Update LEO in state store with optimistic locking
        match self
            .state_store
            .update_leo(&self.topic, self.partition, new_leo, version)
            .await
        {
            Ok(new_version) => {
                self.stats.state_writes.fetch_add(1, Ordering::Relaxed);

                // Update local cache
                let mut cached = self.cached_state.write().await;
                cached.log_end_offset = new_leo;
                cached.version = new_version;

                self.stats
                    .records_appended
                    .fetch_add(record_count as u64, Ordering::Relaxed);

                debug!(
                    topic = %self.topic,
                    partition = self.partition,
                    leo = new_leo,
                    version = new_version,
                    "Appended batch to stateless segment"
                );

                Ok(base_offset)
            }
            Err(e) => {
                self.stats.version_conflicts.fetch_add(1, Ordering::Relaxed);
                // Refresh state and return error for retry
                let _ = self.refresh_state().await;
                Err(e)
            }
        }
    }

    /// Read records starting from an offset
    pub async fn read_from_offset(
        &self,
        start_offset: i64,
        max_bytes: usize,
    ) -> Result<Vec<Record>> {
        // Refresh state to get current LEO
        self.refresh_state().await?;

        let cached = self.cached_state.read().await;
        if start_offset >= cached.log_end_offset {
            return Ok(Vec::new());
        }
        drop(cached);

        // Read from S3 (using the same format as S3Segment)
        let prefix = format!("segments/{}/partition-{}/", self.topic, self.partition);

        // List segment files to find the one containing start_offset
        let list_result = self
            .store
            .list(Some(&ObjectPath::from(prefix.clone())))
            .collect::<Vec<_>>()
            .await;

        let mut records = Vec::new();
        let mut bytes_read = 0;

        for result in list_result {
            let meta = result.map_err(|e| {
                StreamlineError::storage_msg(format!("Failed to list segments: {}", e))
            })?;

            // Parse segment file name to get offset range
            let path_str = meta.location.to_string();
            if !path_str.ends_with(".seg") {
                continue;
            }

            // Extract base and end offset from filename
            let filename = path_str.rsplit('/').next().unwrap_or("");
            let parts: Vec<&str> = filename.trim_end_matches(".seg").split('-').collect();
            if parts.len() != 2 {
                continue;
            }

            let seg_base: i64 = parts[0].parse().unwrap_or(-1);
            let seg_end: i64 = parts[1].parse().unwrap_or(-1);

            if seg_base < 0 || seg_end < start_offset {
                continue;
            }

            // Check cache first
            if let Some(ref cache) = self.cache {
                if let Some(cached_data) = cache.get(&self.topic, self.partition, seg_base) {
                    self.stats.cache_hits.fetch_add(1, Ordering::Relaxed);
                    let batches = S3Segment::deserialize_segment(&cached_data)?;
                    for batch in batches {
                        for record in batch.records {
                            if record.offset >= start_offset && bytes_read < max_bytes {
                                bytes_read += record.value.len();
                                records.push(record);
                            }
                        }
                    }
                    continue;
                }
            }

            // Download segment from S3
            self.stats.cache_misses.fetch_add(1, Ordering::Relaxed);
            self.stats.downloads.fetch_add(1, Ordering::Relaxed);

            let data = self
                .store
                .get(&meta.location)
                .await
                .map_err(|e| {
                    StreamlineError::storage_msg(format!("Failed to download segment: {}", e))
                })?
                .bytes()
                .await
                .map_err(|e| {
                    StreamlineError::storage_msg(format!("Failed to read segment: {}", e))
                })?;

            // Cache the segment data
            if let Some(ref cache) = self.cache {
                cache.put(&self.topic, self.partition, seg_base, seg_end, data.clone());
            }

            // Deserialize and extract records
            let batches = S3Segment::deserialize_segment(&data)?;
            for batch in batches {
                for record in batch.records {
                    if record.offset >= start_offset && bytes_read < max_bytes {
                        bytes_read += record.value.len();
                        records.push(record);
                    }
                }
            }

            if bytes_read >= max_bytes {
                break;
            }
        }

        records.sort_by_key(|r| r.offset);
        Ok(records)
    }

    /// Seal the segment (no more writes allowed)
    pub async fn seal(&self) -> Result<()> {
        self.sealed.store(true, Ordering::Release);
        info!(
            topic = %self.topic,
            partition = self.partition,
            "Stateless segment sealed"
        );
        Ok(())
    }

    /// Get statistics
    pub fn stats(&self) -> S3StatelessStatsSnapshot {
        S3StatelessStatsSnapshot {
            state_reads: self.stats.state_reads.load(Ordering::Relaxed),
            state_writes: self.stats.state_writes.load(Ordering::Relaxed),
            version_conflicts: self.stats.version_conflicts.load(Ordering::Relaxed),
            records_appended: self.stats.records_appended.load(Ordering::Relaxed),
            downloads: self.stats.downloads.load(Ordering::Relaxed),
            cache_hits: self.stats.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.stats.cache_misses.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of S3SegmentStateless statistics
#[derive(Debug, Clone)]
pub struct S3StatelessStatsSnapshot {
    pub state_reads: u64,
    pub state_writes: u64,
    pub version_conflicts: u64,
    pub records_appended: u64,
    pub downloads: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::tiering::TieringPolicy;
    use tempfile::tempdir;

    fn create_test_batch(base_offset: i64, count: usize) -> RecordBatch {
        let mut batch = RecordBatch::new(base_offset, chrono::Utc::now().timestamp_millis());
        for i in 0..count {
            let record = Record {
                offset: base_offset + i as i64,
                timestamp: chrono::Utc::now().timestamp_millis(),
                key: Some(Bytes::from(format!("key-{}", i))),
                value: Bytes::from(format!("value-{}", i)),
                headers: Vec::new(),
                crc: None,
            };
            batch.add_record(record);
        }
        batch
    }

    #[test]
    fn test_segment_serialization() {
        let batches = vec![create_test_batch(0, 3), create_test_batch(3, 2)];

        let serialized = S3Segment::serialize_segment(&batches, CompressionCodec::None).unwrap();
        let deserialized = S3Segment::deserialize_segment(&serialized).unwrap();

        assert_eq!(deserialized.len(), 2);
        assert_eq!(deserialized[0].records.len(), 3);
        assert_eq!(deserialized[1].records.len(), 2);
    }

    #[test]
    fn test_segment_serialization_with_compression() {
        let batches = vec![create_test_batch(0, 10)];

        let serialized = S3Segment::serialize_segment(&batches, CompressionCodec::Lz4).unwrap();
        let deserialized = S3Segment::deserialize_segment(&serialized).unwrap();

        assert_eq!(deserialized.len(), 1);
        assert_eq!(deserialized[0].records.len(), 10);
    }

    #[tokio::test]
    async fn test_s3_segment_basic() {
        let temp_dir = tempdir().unwrap();
        let config = TieringConfig {
            enabled: true,
            backend: TieringBackend::Local {
                path: temp_dir.path().to_path_buf(),
            },
            policy: TieringPolicy::default(),
            cache: crate::storage::tiering::CacheConfig::default(),
        };

        let mut segment = S3Segment::new(&config, "test-topic".to_string(), 0, 0, 1024, 100)
            .await
            .unwrap();

        // Append batches
        let batch1 = create_test_batch(0, 3);
        let batch2 = create_test_batch(3, 2);

        segment.append_batch(&batch1).await.unwrap();
        segment.append_batch(&batch2).await.unwrap();

        assert_eq!(segment.log_end_offset(), 5);

        // Flush to force upload
        segment.flush_buffer().await.unwrap();

        // Read back
        let records = segment.read_from_offset(0, 1024 * 1024).await.unwrap();
        assert_eq!(records.len(), 5);
        assert_eq!(records[0].offset, 0);
        assert_eq!(records[4].offset, 4);
    }

    #[tokio::test]
    async fn test_s3_segment_read_from_offset() {
        let temp_dir = tempdir().unwrap();
        let config = TieringConfig {
            enabled: true,
            backend: TieringBackend::Local {
                path: temp_dir.path().to_path_buf(),
            },
            policy: TieringPolicy::default(),
            cache: crate::storage::tiering::CacheConfig::default(),
        };

        let mut segment = S3Segment::new(&config, "test-topic".to_string(), 0, 0, 1024, 100)
            .await
            .unwrap();

        // Append batch
        let batch = create_test_batch(0, 10);
        segment.append_batch(&batch).await.unwrap();
        segment.flush_buffer().await.unwrap();

        // Read from offset 5
        let records = segment.read_from_offset(5, 1024 * 1024).await.unwrap();
        assert_eq!(records.len(), 5);
        assert_eq!(records[0].offset, 5);
    }

    #[tokio::test]
    async fn test_s3_segment_seal() {
        let temp_dir = tempdir().unwrap();
        let config = TieringConfig {
            enabled: true,
            backend: TieringBackend::Local {
                path: temp_dir.path().to_path_buf(),
            },
            policy: TieringPolicy::default(),
            cache: crate::storage::tiering::CacheConfig::default(),
        };

        let mut segment = S3Segment::new(&config, "test-topic".to_string(), 0, 0, 1024, 100)
            .await
            .unwrap();

        // Append and seal
        let batch = create_test_batch(0, 5);
        segment.append_batch(&batch).await.unwrap();
        segment.seal().await.unwrap();

        assert!(segment.is_sealed());

        // Should fail to append after seal
        let result = segment.append_batch(&create_test_batch(5, 1)).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_s3_segment_size_threshold() {
        let temp_dir = tempdir().unwrap();
        let config = TieringConfig {
            enabled: true,
            backend: TieringBackend::Local {
                path: temp_dir.path().to_path_buf(),
            },
            policy: TieringPolicy::default(),
            cache: crate::storage::tiering::CacheConfig::default(),
        };

        // Small batch size to trigger automatic flush
        let mut segment = S3Segment::new(&config, "test-topic".to_string(), 0, 0, 100, 10000)
            .await
            .unwrap();

        // Append batches that exceed threshold
        for i in 0..5 {
            let batch = create_test_batch(i * 10, 10);
            segment.append_batch(&batch).await.unwrap();
        }

        // Check manifest has segments (auto-flushed)
        let manifest = segment.manifest.read().await;
        assert!(!manifest.segments.is_empty());
    }

    #[tokio::test]
    async fn test_s3_segment_cache_integration() {
        use crate::storage::cache::{CacheConfig, SegmentCache};

        let temp_dir = tempdir().unwrap();
        let config = TieringConfig {
            enabled: true,
            backend: TieringBackend::Local {
                path: temp_dir.path().to_path_buf(),
            },
            policy: TieringPolicy::default(),
            cache: crate::storage::tiering::CacheConfig::default(),
        };

        // Create a shared cache
        let cache = Arc::new(SegmentCache::new(CacheConfig::default()));

        let mut segment = S3Segment::new(&config, "test-topic".to_string(), 0, 0, 1024, 100)
            .await
            .unwrap()
            .with_cache(cache.clone());

        // Append and flush to create a segment
        let batch = create_test_batch(0, 5);
        segment.append_batch(&batch).await.unwrap();
        segment.flush_buffer().await.unwrap();

        // First read - should be a cache miss
        let records1 = segment.read_from_offset(0, 1024 * 1024).await.unwrap();
        assert_eq!(records1.len(), 5);
        assert_eq!(segment.cache_misses(), 1);
        assert_eq!(segment.cache_hits(), 0);

        // Second read - should be a cache hit
        let records2 = segment.read_from_offset(0, 1024 * 1024).await.unwrap();
        assert_eq!(records2.len(), 5);
        assert_eq!(segment.cache_misses(), 1);
        assert_eq!(segment.cache_hits(), 1);

        // Verify data is identical
        assert_eq!(records1[0].offset, records2[0].offset);
        assert_eq!(records1[4].value, records2[4].value);
    }

    #[tokio::test]
    async fn test_s3_segment_set_cache() {
        use crate::storage::cache::{CacheConfig, SegmentCache};

        let temp_dir = tempdir().unwrap();
        let config = TieringConfig {
            enabled: true,
            backend: TieringBackend::Local {
                path: temp_dir.path().to_path_buf(),
            },
            policy: TieringPolicy::default(),
            cache: crate::storage::tiering::CacheConfig::default(),
        };

        let mut segment = S3Segment::new(&config, "test-topic".to_string(), 0, 0, 1024, 100)
            .await
            .unwrap();

        // Append and flush
        let batch = create_test_batch(0, 3);
        segment.append_batch(&batch).await.unwrap();
        segment.flush_buffer().await.unwrap();

        // Read without cache
        let _records = segment.read_from_offset(0, 1024 * 1024).await.unwrap();
        assert_eq!(segment.cache_hits(), 0);
        assert_eq!(segment.cache_misses(), 0); // No cache, so no miss recorded

        // Now set a cache
        let cache = Arc::new(SegmentCache::new(CacheConfig::default()));
        segment.set_cache(cache.clone());

        // Read with cache - should be a miss first time
        let _records = segment.read_from_offset(0, 1024 * 1024).await.unwrap();
        assert_eq!(segment.cache_misses(), 1);

        // Read again - should be a hit
        let _records = segment.read_from_offset(0, 1024 * 1024).await.unwrap();
        assert_eq!(segment.cache_hits(), 1);
    }
}
