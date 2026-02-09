//! S3-backed Write-Ahead Log for diskless storage mode
//!
//! This module provides a WAL backend that writes directly to object storage (S3/Azure/GCS)
//! instead of local disk. It's used in diskless mode where no local storage is available.
//!
//! ## Design
//!
//! - WAL entries are batched in memory until size or time thresholds are reached
//! - Batches are uploaded as individual objects with sequence-based naming
//! - A manifest object tracks all WAL batch files for recovery
//! - Reading walks the manifest and fetches required batch files
//!
//! ## Object Storage Layout
//!
//! ```text
//! wal/{topic}/partition-{n}/
//!   manifest.json           # Lists all batch files and their sequence ranges
//!   batch-{seq_start}-{seq_end}.wal  # Individual batch files
//! ```

use crate::error::{Result, StreamlineError};
use crate::storage::backend::WalBackend;
use crate::storage::storage_mode::StorageMode;
use crate::storage::tiering::{TieringBackend, TieringConfig};
use crate::storage::wal::WalEntry;
use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutPayload};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
use tracing::{debug, info};

/// S3 WAL manifest tracking uploaded batch files
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct WalManifest {
    /// Topic name
    pub topic: String,

    /// Partition ID
    pub partition: i32,

    /// List of batch files in order
    pub batches: Vec<WalBatchInfo>,

    /// Last checkpoint sequence
    pub last_checkpoint: Option<u64>,

    /// Version for optimistic locking
    pub version: u64,
}

/// Information about a single WAL batch file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WalBatchInfo {
    /// Batch file name
    pub filename: String,

    /// First sequence number in batch
    pub seq_start: u64,

    /// Last sequence number in batch
    pub seq_end: u64,

    /// Number of entries in batch
    pub entry_count: usize,

    /// Batch size in bytes
    pub size_bytes: usize,

    /// Upload timestamp (millis since epoch)
    pub uploaded_at: i64,
}

/// S3-backed WAL writer for diskless mode
///
/// Buffers WAL entries and uploads them to object storage in batches.
pub struct S3WalWriter {
    /// Object store backend
    store: Arc<dyn ObjectStore>,

    /// Topic name
    topic: String,

    /// Partition ID
    partition: i32,

    /// Buffered entries waiting to be uploaded
    buffer: Mutex<WalBuffer>,

    /// Next sequence number
    next_sequence: AtomicU64,

    /// Current manifest (cached)
    manifest: Mutex<WalManifest>,

    /// Batch size threshold (bytes)
    batch_size_bytes: usize,

    /// Batch timeout (milliseconds)
    batch_timeout_ms: u64,

    /// Statistics
    stats: S3WalStats,
}

/// Internal buffer state
#[derive(Default)]
struct WalBuffer {
    /// Buffered entries
    entries: VecDeque<WalEntry>,

    /// Current buffer size in bytes
    size_bytes: usize,

    /// Time when first entry was buffered
    batch_start: Option<Instant>,
}

/// S3 WAL statistics
#[derive(Debug, Default)]
struct S3WalStats {
    entries_buffered: AtomicU64,
    entries_uploaded: AtomicU64,
    batches_uploaded: AtomicU64,
    bytes_uploaded: AtomicU64,
}

impl S3WalWriter {
    /// Create a new S3 WAL writer
    ///
    /// # Arguments
    /// * `config` - Tiering configuration with backend settings
    /// * `topic` - Topic name
    /// * `partition` - Partition ID
    /// * `batch_size_bytes` - Flush when buffer reaches this size
    /// * `batch_timeout_ms` - Flush after this time even if size not reached
    pub async fn new(
        config: &TieringConfig,
        topic: String,
        partition: i32,
        batch_size_bytes: usize,
        batch_timeout_ms: u64,
    ) -> Result<Self> {
        let store = create_object_store(&config.backend)?;

        let writer = Self {
            store,
            topic: topic.clone(),
            partition,
            buffer: Mutex::new(WalBuffer::default()),
            next_sequence: AtomicU64::new(0),
            manifest: Mutex::new(WalManifest {
                topic: topic.clone(),
                partition,
                batches: Vec::new(),
                last_checkpoint: None,
                version: 0,
            }),
            batch_size_bytes,
            batch_timeout_ms,
            stats: S3WalStats::default(),
        };

        // Try to load existing manifest
        writer.load_manifest().await?;

        info!(
            topic = %topic,
            partition = partition,
            batch_size_bytes = batch_size_bytes,
            batch_timeout_ms = batch_timeout_ms,
            "S3 WAL writer initialized"
        );

        Ok(writer)
    }

    /// Create a new S3 WAL writer with an existing object store
    ///
    /// This is useful when the object store has already been created by a factory.
    ///
    /// # Arguments
    /// * `store` - Pre-configured object store
    /// * `topic` - Topic name
    /// * `partition` - Partition ID
    /// * `batch_size_bytes` - Flush when buffer reaches this size
    /// * `batch_timeout_ms` - Flush after this time even if size not reached
    pub async fn new_with_store(
        store: Arc<dyn ObjectStore>,
        topic: &str,
        partition: i32,
        batch_size_bytes: usize,
        batch_timeout_ms: u64,
    ) -> Result<Self> {
        let writer = Self {
            store,
            topic: topic.to_string(),
            partition,
            buffer: Mutex::new(WalBuffer::default()),
            next_sequence: AtomicU64::new(0),
            manifest: Mutex::new(WalManifest {
                topic: topic.to_string(),
                partition,
                batches: Vec::new(),
                last_checkpoint: None,
                version: 0,
            }),
            batch_size_bytes,
            batch_timeout_ms,
            stats: S3WalStats::default(),
        };

        // Try to load existing manifest
        writer.load_manifest().await?;

        info!(
            topic = %topic,
            partition = partition,
            batch_size_bytes = batch_size_bytes,
            batch_timeout_ms = batch_timeout_ms,
            "S3 WAL writer initialized (with existing store)"
        );

        Ok(writer)
    }

    /// Get the object path prefix for this partition's WAL
    fn wal_prefix(&self) -> String {
        format!("wal/{}/partition-{}", self.topic, self.partition)
    }

    /// Get the manifest object path
    fn manifest_path(&self) -> ObjectPath {
        ObjectPath::from(format!("{}/manifest.json", self.wal_prefix()))
    }

    /// Get the path for a batch file
    fn batch_path(&self, seq_start: u64, seq_end: u64) -> ObjectPath {
        ObjectPath::from(format!(
            "{}/batch-{:020}-{:020}.wal",
            self.wal_prefix(),
            seq_start,
            seq_end
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

                let manifest: WalManifest = serde_json::from_slice(&data).map_err(|e| {
                    StreamlineError::storage_msg(format!("Failed to parse manifest: {}", e))
                })?;

                // Update next sequence based on manifest
                if let Some(last_batch) = manifest.batches.last() {
                    self.next_sequence
                        .store(last_batch.seq_end + 1, Ordering::SeqCst);
                }

                *self.manifest.lock().await = manifest;

                debug!(
                    topic = %self.topic,
                    partition = self.partition,
                    "Loaded WAL manifest from S3"
                );
            }
            Err(object_store::Error::NotFound { .. }) => {
                debug!(
                    topic = %self.topic,
                    partition = self.partition,
                    "No existing WAL manifest found"
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
        let manifest = self.manifest.lock().await;
        let data = serde_json::to_vec_pretty(&*manifest)?;

        let path = self.manifest_path();
        self.store
            .put(&path, PutPayload::from_bytes(Bytes::from(data)))
            .await
            .map_err(|e| StreamlineError::storage_msg(format!("Failed to save manifest: {}", e)))?;

        debug!(
            topic = %self.topic,
            partition = self.partition,
            batches = manifest.batches.len(),
            "Saved WAL manifest to S3"
        );

        Ok(())
    }

    /// Serialize a batch of entries to bytes
    fn serialize_batch(entries: &[WalEntry]) -> Bytes {
        let mut buf = BytesMut::new();

        // Magic header
        buf.put_slice(b"S3WL"); // S3 WAL

        // Version
        buf.put_u16_le(1);

        // Entry count
        buf.put_u32_le(entries.len() as u32);

        // Entries
        for entry in entries {
            let entry_bytes = entry.to_bytes();
            buf.put_u32_le(entry_bytes.len() as u32);
            buf.put_slice(&entry_bytes);
        }

        buf.freeze()
    }

    /// Deserialize a batch of entries from bytes
    fn deserialize_batch(data: &[u8]) -> Result<Vec<WalEntry>> {
        if data.len() < 10 {
            return Err(StreamlineError::CorruptedData(
                "S3 WAL batch too short".to_string(),
            ));
        }

        // Check magic
        if &data[0..4] != b"S3WL" {
            return Err(StreamlineError::CorruptedData(
                "Invalid S3 WAL batch magic".to_string(),
            ));
        }

        // Check version
        let _version = u16::from_le_bytes([data[4], data[5]]);

        // Entry count
        let entry_count = u32::from_le_bytes([data[6], data[7], data[8], data[9]]) as usize;

        let mut entries = Vec::with_capacity(entry_count);
        let mut pos = 10;

        for _ in 0..entry_count {
            if pos + 4 > data.len() {
                return Err(StreamlineError::CorruptedData(
                    "S3 WAL batch entry length truncated".to_string(),
                ));
            }

            let entry_len =
                u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
                    as usize;
            pos += 4;

            if pos + entry_len > data.len() {
                return Err(StreamlineError::CorruptedData(
                    "S3 WAL batch entry truncated".to_string(),
                ));
            }

            let (entry, _) = WalEntry::from_bytes(&data[pos..pos + entry_len])?;
            entries.push(entry);
            pos += entry_len;
        }

        Ok(entries)
    }

    /// Check if buffer should be flushed
    fn should_flush_buffer(
        buffer: &WalBuffer,
        batch_size_bytes: usize,
        batch_timeout_ms: u64,
    ) -> bool {
        if buffer.entries.is_empty() {
            return false;
        }

        // Size threshold
        if buffer.size_bytes >= batch_size_bytes {
            return true;
        }

        // Time threshold
        if let Some(start) = buffer.batch_start {
            if start.elapsed().as_millis() as u64 >= batch_timeout_ms {
                return true;
            }
        }

        false
    }

    /// Flush buffer to S3
    async fn flush_buffer_internal(&self) -> Result<Option<WalBatchInfo>> {
        let entries: Vec<WalEntry> = {
            let mut buffer = self.buffer.lock().await;
            if buffer.entries.is_empty() {
                return Ok(None);
            }
            buffer.entries.drain(..).collect()
        };

        if entries.is_empty() {
            return Ok(None);
        }

        let seq_start = entries.first().map(|e| e.sequence).unwrap_or(0);
        let seq_end = entries.last().map(|e| e.sequence).unwrap_or(0);
        let entry_count = entries.len();

        // Serialize batch
        let data = Self::serialize_batch(&entries);
        let size_bytes = data.len();

        // Upload to S3
        let path = self.batch_path(seq_start, seq_end);
        self.store
            .put(&path, PutPayload::from_bytes(data))
            .await
            .map_err(|e| {
                StreamlineError::storage_msg(format!("Failed to upload WAL batch: {}", e))
            })?;

        // Update stats
        self.stats
            .entries_uploaded
            .fetch_add(entry_count as u64, Ordering::Relaxed);
        self.stats.batches_uploaded.fetch_add(1, Ordering::Relaxed);
        self.stats
            .bytes_uploaded
            .fetch_add(size_bytes as u64, Ordering::Relaxed);

        // Create batch info
        let batch_info = WalBatchInfo {
            filename: path.to_string(),
            seq_start,
            seq_end,
            entry_count,
            size_bytes,
            uploaded_at: chrono::Utc::now().timestamp_millis(),
        };

        // Update manifest
        {
            let mut manifest = self.manifest.lock().await;
            manifest.batches.push(batch_info.clone());
            manifest.version += 1;
        }

        // Save manifest
        self.save_manifest().await?;

        // Reset buffer state
        {
            let mut buffer = self.buffer.lock().await;
            buffer.size_bytes = 0;
            buffer.batch_start = None;
        }

        info!(
            topic = %self.topic,
            partition = self.partition,
            seq_start = seq_start,
            seq_end = seq_end,
            entries = entry_count,
            size_bytes = size_bytes,
            "WAL batch uploaded to S3"
        );

        Ok(Some(batch_info))
    }

    /// Download and parse a batch file
    async fn download_batch(&self, batch: &WalBatchInfo) -> Result<Vec<WalEntry>> {
        let path = ObjectPath::from(batch.filename.clone());

        let result = self.store.get(&path).await.map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to download WAL batch: {}", e))
        })?;

        let data = result.bytes().await.map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to read WAL batch: {}", e))
        })?;

        Self::deserialize_batch(&data)
    }
}

impl Debug for S3WalWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3WalWriter")
            .field("topic", &self.topic)
            .field("partition", &self.partition)
            .field("batch_size_bytes", &self.batch_size_bytes)
            .field("batch_timeout_ms", &self.batch_timeout_ms)
            .field("next_sequence", &self.next_sequence.load(Ordering::Relaxed))
            .finish()
    }
}

#[async_trait]
impl WalBackend for S3WalWriter {
    async fn append(&mut self, entry: WalEntry) -> Result<u64> {
        let sequence = entry.sequence;
        let entry_size = entry.to_bytes().len();

        // Add to buffer
        {
            let mut buffer = self.buffer.lock().await;
            if buffer.batch_start.is_none() {
                buffer.batch_start = Some(Instant::now());
            }
            buffer.size_bytes += entry_size;
            buffer.entries.push_back(entry);
        }

        self.stats.entries_buffered.fetch_add(1, Ordering::Relaxed);

        // Update next sequence
        let next = sequence + 1;
        self.next_sequence.fetch_max(next, Ordering::SeqCst);

        // Check if we should flush
        let should_flush = {
            let buffer = self.buffer.lock().await;
            Self::should_flush_buffer(&buffer, self.batch_size_bytes, self.batch_timeout_ms)
        };

        if should_flush {
            self.flush_buffer_internal().await?;
        }

        Ok(sequence)
    }

    async fn flush(&mut self) -> Result<()> {
        self.flush_buffer_internal().await?;
        Ok(())
    }

    async fn read_from(&self, sequence: u64) -> Result<Vec<WalEntry>> {
        let manifest = self.manifest.lock().await;
        let mut entries = Vec::new();

        // Find batches that contain entries >= sequence
        for batch in &manifest.batches {
            if batch.seq_end >= sequence {
                let batch_entries = self.download_batch(batch).await?;
                for entry in batch_entries {
                    if entry.sequence >= sequence {
                        entries.push(entry);
                    }
                }
            }
        }

        // Also include buffered entries
        {
            let buffer = self.buffer.lock().await;
            for entry in &buffer.entries {
                if entry.sequence >= sequence {
                    entries.push(entry.clone());
                }
            }
        }

        // Sort by sequence
        entries.sort_by_key(|e| e.sequence);

        Ok(entries)
    }

    fn storage_mode(&self) -> StorageMode {
        StorageMode::Diskless
    }

    fn current_sequence(&self) -> u64 {
        self.next_sequence.load(Ordering::SeqCst)
    }

    fn has_pending(&self) -> bool {
        // Check if buffer has pending entries synchronously
        // This is a best-effort check for non-async context
        true // Conservative: always assume there might be pending data
    }
}

/// Create an object store from the backend configuration
/// (Reuses pattern from tiering module)
fn create_object_store(backend: &TieringBackend) -> Result<Arc<dyn ObjectStore>> {
    match backend {
        TieringBackend::Local { path } => {
            std::fs::create_dir_all(path).map_err(|e| {
                StreamlineError::storage_msg(format!("Failed to create WAL directory: {}", e))
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

/// Local WAL backend wrapper for compatibility
///
/// Wraps the existing local WalWriter to implement the WalBackend trait.
pub struct LocalWalBackend {
    /// Wrapped local WAL writer
    writer: crate::storage::wal::WalWriter,

    /// Buffered entries (for read_from)
    entries_cache: Mutex<Vec<WalEntry>>,

    /// WAL directory for reading
    wal_dir: std::path::PathBuf,
}

impl LocalWalBackend {
    /// Create a new local WAL backend
    pub fn new(data_dir: &std::path::Path, config: crate::config::WalConfig) -> Result<Self> {
        let wal_dir = data_dir.join("wal");
        let writer = crate::storage::wal::WalWriter::new(data_dir, config)?;

        Ok(Self {
            writer,
            entries_cache: Mutex::new(Vec::new()),
            wal_dir,
        })
    }
}

impl Debug for LocalWalBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalWalBackend")
            .field("enabled", &self.writer.is_enabled())
            .field("next_sequence", &self.writer.next_sequence())
            .finish()
    }
}

#[async_trait]
impl WalBackend for LocalWalBackend {
    async fn append(&mut self, entry: WalEntry) -> Result<u64> {
        let sequence = self.writer.append(&entry)?;

        // Cache for read_from
        self.entries_cache.lock().await.push(entry);

        Ok(sequence)
    }

    async fn flush(&mut self) -> Result<()> {
        self.writer.sync()
    }

    async fn read_from(&self, sequence: u64) -> Result<Vec<WalEntry>> {
        // First check cache
        let cached: Vec<WalEntry> = {
            let cache = self.entries_cache.lock().await;
            cache
                .iter()
                .filter(|e| e.sequence >= sequence)
                .cloned()
                .collect()
        };

        if !cached.is_empty() {
            return Ok(cached);
        }

        // Fall back to reading from disk
        let entries = crate::storage::wal::WalReader::read_after_sequence(&self.wal_dir, sequence)?;
        Ok(entries)
    }

    fn storage_mode(&self) -> StorageMode {
        StorageMode::Local
    }

    fn current_sequence(&self) -> u64 {
        self.writer.next_sequence()
    }

    fn has_pending(&self) -> bool {
        // Local writer doesn't buffer significantly
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::tiering::TieringPolicy;
    use tempfile::tempdir;

    fn create_test_entry(sequence: u64, topic: &str, partition: i32, value: &str) -> WalEntry {
        WalEntry::new_record(
            sequence,
            topic,
            partition,
            None,
            &Bytes::from(value.to_string()),
        )
    }

    #[test]
    fn test_batch_serialization() {
        let entries = vec![
            create_test_entry(0, "test", 0, "value1"),
            create_test_entry(1, "test", 0, "value2"),
            create_test_entry(2, "test", 0, "value3"),
        ];

        let serialized = S3WalWriter::serialize_batch(&entries);
        let deserialized = S3WalWriter::deserialize_batch(&serialized).unwrap();

        assert_eq!(deserialized.len(), 3);
        assert_eq!(deserialized[0].sequence, 0);
        assert_eq!(deserialized[1].sequence, 1);
        assert_eq!(deserialized[2].sequence, 2);
    }

    #[tokio::test]
    async fn test_s3_wal_basic() {
        let temp_dir = tempdir().unwrap();
        let config = TieringConfig {
            enabled: true,
            backend: TieringBackend::Local {
                path: temp_dir.path().to_path_buf(),
            },
            policy: TieringPolicy::default(),
            cache: crate::storage::tiering::CacheConfig::default(),
        };

        let mut writer = S3WalWriter::new(&config, "test-topic".to_string(), 0, 1024, 100)
            .await
            .unwrap();

        // Append entries
        let entry1 = create_test_entry(0, "test-topic", 0, "value1");
        let entry2 = create_test_entry(1, "test-topic", 0, "value2");

        writer.append(entry1).await.unwrap();
        writer.append(entry2).await.unwrap();

        // Flush to force upload
        writer.flush().await.unwrap();

        // Read back
        let entries = writer.read_from(0).await.unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].sequence, 0);
        assert_eq!(entries[1].sequence, 1);
    }

    #[tokio::test]
    async fn test_s3_wal_size_threshold() {
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
        let mut writer = S3WalWriter::new(&config, "test-topic".to_string(), 0, 100, 10000)
            .await
            .unwrap();

        // Append entries until size threshold triggers
        for i in 0..10 {
            let entry = create_test_entry(i, "test-topic", 0, &format!("value{}", i));
            writer.append(entry).await.unwrap();
        }

        // Check that batches were uploaded
        let manifest = writer.manifest.lock().await;
        assert!(!manifest.batches.is_empty());
    }

    #[tokio::test]
    async fn test_s3_wal_read_from_sequence() {
        let temp_dir = tempdir().unwrap();
        let config = TieringConfig {
            enabled: true,
            backend: TieringBackend::Local {
                path: temp_dir.path().to_path_buf(),
            },
            policy: TieringPolicy::default(),
            cache: crate::storage::tiering::CacheConfig::default(),
        };

        let mut writer = S3WalWriter::new(&config, "test-topic".to_string(), 0, 1024, 100)
            .await
            .unwrap();

        // Append entries
        for i in 0..5 {
            let entry = create_test_entry(i, "test-topic", 0, &format!("value{}", i));
            writer.append(entry).await.unwrap();
        }

        writer.flush().await.unwrap();

        // Read from sequence 3
        let entries = writer.read_from(3).await.unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].sequence, 3);
        assert_eq!(entries[1].sequence, 4);
    }

    #[tokio::test]
    async fn test_s3_wal_manifest_persistence() {
        let temp_dir = tempdir().unwrap();
        let config = TieringConfig {
            enabled: true,
            backend: TieringBackend::Local {
                path: temp_dir.path().to_path_buf(),
            },
            policy: TieringPolicy::default(),
            cache: crate::storage::tiering::CacheConfig::default(),
        };

        // Create writer and append entries
        {
            let mut writer = S3WalWriter::new(&config, "test-topic".to_string(), 0, 1024, 100)
                .await
                .unwrap();

            for i in 0..3 {
                let entry = create_test_entry(i, "test-topic", 0, &format!("value{}", i));
                writer.append(entry).await.unwrap();
            }

            writer.flush().await.unwrap();
        }

        // Create new writer - should load manifest
        let writer = S3WalWriter::new(&config, "test-topic".to_string(), 0, 1024, 100)
            .await
            .unwrap();

        // Sequence should continue from where we left off
        assert_eq!(writer.current_sequence(), 3);

        // Should be able to read old entries
        let entries = writer.read_from(0).await.unwrap();
        assert_eq!(entries.len(), 3);
    }

    #[test]
    fn test_wal_manifest_serde() {
        let manifest = WalManifest {
            topic: "test".to_string(),
            partition: 0,
            batches: vec![WalBatchInfo {
                filename: "batch-0-2.wal".to_string(),
                seq_start: 0,
                seq_end: 2,
                entry_count: 3,
                size_bytes: 100,
                uploaded_at: 12345,
            }],
            last_checkpoint: Some(2),
            version: 1,
        };

        let json = serde_json::to_string(&manifest).unwrap();
        let parsed: WalManifest = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.topic, "test");
        assert_eq!(parsed.batches.len(), 1);
        assert_eq!(parsed.batches[0].seq_start, 0);
    }
}
