//! Async Write-Ahead Log (WAL) implementation using io_backend
//!
//! This module provides an async-compatible WAL implementation that uses the
//! io_backend abstraction for I/O operations. This enables:
//!
//! - **io_uring support**: On Linux 5.11+, uses io_uring for lower latency
//! - **Non-blocking I/O**: Integrates with Tokio's async runtime
//! - **Buffer pool integration**: Reuses buffers to reduce allocations
//!
//! # Usage
//!
//! ```ignore
//! use streamline::storage::{AsyncWalWriter, get_standard_backend};
//!
//! let fs = get_standard_backend();
//! let mut wal = AsyncWalWriter::new(&fs, &data_dir, config).await?;
//!
//! // Append records
//! let seq = wal.append_record("topic", 0, Some(&key), &value).await?;
//!
//! // Sync to disk
//! wal.sync().await?;
//! ```

use crate::config::WalConfig;
use crate::error::{Result, StreamlineError};
use crate::storage::io_backend::{AsyncFile, AsyncFileSystem, IoBufferPool};
use crate::storage::wal::{SyncMode, WalEntry, WalRecoveryStats};
use bytes::Bytes;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Magic bytes for WAL file header
const WAL_MAGIC: &[u8; 4] = b"WLOG";

/// Current WAL format version
const WAL_VERSION: u16 = 1;

/// Size of WAL header in bytes
const WAL_HEADER_SIZE: usize = 16;

/// Async WAL writer using io_backend for I/O operations
///
/// This provides the same functionality as `WalWriter` but uses async I/O
/// through the io_backend abstraction, enabling io_uring on Linux.
pub struct AsyncWalWriter<F: AsyncFile> {
    /// WAL directory path
    wal_dir: PathBuf,

    /// Configuration
    config: WalConfig,

    /// Current WAL file
    file: Arc<RwLock<Option<Arc<F>>>>,

    /// Current WAL file path
    current_path: PathBuf,

    /// Current file size
    current_size: Arc<RwLock<u64>>,

    /// Next sequence number
    next_sequence: AtomicU64,

    /// Sync mode
    sync_mode: SyncMode,

    /// Whether WAL is enabled
    enabled: bool,

    /// Number of pending writes (not yet synced)
    pending_writes: Arc<RwLock<usize>>,

    /// Optional buffer pool for I/O operations
    buffer_pool: Option<Arc<IoBufferPool>>,
}

impl<F: AsyncFile + 'static> AsyncWalWriter<F> {
    /// Create a new async WAL writer
    pub async fn new<FS>(fs: &FS, data_dir: &Path, config: WalConfig) -> Result<Self>
    where
        FS: AsyncFileSystem<File = F>,
    {
        let wal_dir = data_dir.join("wal");
        crate::storage::async_io::create_dir_all_async(wal_dir.clone()).await?;

        let sync_mode = parse_sync_mode(&config.sync_mode);
        let enabled = config.enabled;
        let current_path = wal_dir.join("current.wal");

        let mut writer = Self {
            wal_dir: wal_dir.clone(),
            config,
            file: Arc::new(RwLock::new(None)),
            current_path: current_path.clone(),
            current_size: Arc::new(RwLock::new(0)),
            next_sequence: AtomicU64::new(0),
            sync_mode,
            enabled,
            pending_writes: Arc::new(RwLock::new(0)),
            buffer_pool: None,
        };

        if enabled {
            // Find max sequence from existing WAL files
            if let Some(max_seq) = writer.find_max_sequence().await? {
                writer.next_sequence.store(max_seq + 1, Ordering::SeqCst);
            }

            // Open or create current WAL file
            writer.open_current_wal(fs).await?;
        }

        info!(
            wal_dir = %wal_dir.display(),
            enabled = enabled,
            sync_mode = ?sync_mode,
            "Async WAL initialized"
        );

        Ok(writer)
    }

    /// Create with a buffer pool for reduced allocations
    pub async fn with_buffer_pool<FS>(
        fs: &FS,
        data_dir: &Path,
        config: WalConfig,
        buffer_pool: Arc<IoBufferPool>,
    ) -> Result<Self>
    where
        FS: AsyncFileSystem<File = F>,
    {
        let mut writer = Self::new(fs, data_dir, config).await?;
        writer.buffer_pool = Some(buffer_pool);
        Ok(writer)
    }

    /// Check if WAL is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Get the next sequence number
    pub fn next_sequence(&self) -> u64 {
        self.next_sequence.load(Ordering::SeqCst)
    }

    /// Get the WAL directory path
    pub fn wal_dir(&self) -> &Path {
        &self.wal_dir
    }

    /// Find maximum sequence number from existing WAL files
    async fn find_max_sequence(&self) -> Result<Option<u64>> {
        use crate::storage::wal::WalReader;

        let mut max_seq: Option<u64> = None;

        // Check current WAL
        if crate::storage::async_io::exists_async(self.current_path.clone())
            .await
            .unwrap_or(false)
        {
            if let Ok(entries) = WalReader::read_file(&self.current_path) {
                if let Some(last) = entries.last() {
                    max_seq = Some(max_seq.unwrap_or(0).max(last.sequence));
                }
            }
        }

        // Check archived WAL files
        if let Ok(entries) = crate::storage::async_io::read_dir_async(self.wal_dir.clone()).await {
            for (path, _is_dir) in entries {
                if path.extension().map(|e| e == "wal").unwrap_or(false)
                    && path != self.current_path
                {
                    if let Ok(wal_entries) = WalReader::read_file(&path) {
                        if let Some(last) = wal_entries.last() {
                            max_seq = Some(max_seq.unwrap_or(0).max(last.sequence));
                        }
                    }
                }
            }
        }

        Ok(max_seq)
    }

    /// Open or create the current WAL file
    async fn open_current_wal<FS>(&mut self, fs: &FS) -> Result<()>
    where
        FS: AsyncFileSystem<File = F>,
    {
        if crate::storage::async_io::exists_async(self.current_path.clone())
            .await
            .unwrap_or(false)
        {
            // Open existing file for append
            let file = fs.open_append(&self.current_path).await?;
            let size = file.size().await?;
            *self.current_size.write().await = size;
            *self.file.write().await = Some(Arc::new(file));
        } else {
            // Create new file with header
            let file = fs.create(&self.current_path).await?;

            // Write header
            let header = create_wal_header();
            let (result, _) = file.write_at(header.to_vec(), 0).await;
            result?;
            file.sync_data().await?;

            *self.current_size.write().await = WAL_HEADER_SIZE as u64;
            *self.file.write().await = Some(Arc::new(file));
        }

        Ok(())
    }

    /// Append an entry to the WAL
    pub async fn append(&self, entry: &WalEntry) -> Result<u64> {
        if !self.enabled {
            return Ok(entry.sequence);
        }

        // Check backpressure limit
        let pending = *self.pending_writes.read().await;
        if pending >= self.config.max_pending_writes {
            return Err(StreamlineError::storage_msg(format!(
                "WAL backpressure: {} pending writes exceeds limit of {}",
                pending, self.config.max_pending_writes
            )));
        }

        // Check if we need to rotate
        let current_size = *self.current_size.read().await;
        if current_size >= self.config.max_file_size {
            // Rotation requires mutable access - for now we log a warning
            // In production, this should be handled by a background task
            warn!(
                current_size = current_size,
                max_size = self.config.max_file_size,
                "WAL file size limit reached, rotation needed"
            );
        }

        let file_guard = self.file.read().await;
        let file = file_guard.as_ref().ok_or_else(|| {
            StreamlineError::storage_msg("WAL writer not initialized".to_string())
        })?;

        // Serialize entry with length prefix
        let entry_bytes = entry.to_bytes();
        let entry_len = entry_bytes.len() as u32;

        // Prepare buffer with length prefix + entry
        let total_size = 4 + entry_bytes.len();
        let mut buf = if let Some(pool) = &self.buffer_pool {
            let mut b = pool.acquire(total_size);
            // Buffer from pool may be cleared (length 0), resize to needed size
            b.resize(total_size, 0);
            b
        } else {
            vec![0u8; total_size]
        };

        buf[0..4].copy_from_slice(&entry_len.to_le_bytes());
        buf[4..total_size].copy_from_slice(&entry_bytes);

        // Write at current position
        let write_pos = *self.current_size.read().await;
        let (result, buf) = file.write_at(buf, write_pos).await;
        result?;

        // Update size
        *self.current_size.write().await = write_pos + buf.len() as u64;

        // Return buffer to pool
        if let Some(pool) = &self.buffer_pool {
            pool.release(buf);
        }

        // Sync based on mode
        match self.sync_mode {
            SyncMode::EveryWrite => {
                file.sync_data().await?;
                *self.pending_writes.write().await = 0;
            }
            SyncMode::Interval | SyncMode::OsDefault => {
                *self.pending_writes.write().await += 1;
            }
        }

        debug!(
            sequence = entry.sequence,
            entry_type = ?entry.entry_type,
            "Async WAL entry written"
        );

        Ok(entry.sequence)
    }

    /// Append a record entry to the WAL
    pub async fn append_record(
        &self,
        topic: &str,
        partition: i32,
        key: Option<&Bytes>,
        value: &Bytes,
    ) -> Result<u64> {
        let sequence = self.next_sequence.fetch_add(1, Ordering::SeqCst);
        let entry = WalEntry::new_record(sequence, topic, partition, key, value);
        self.append(&entry).await
    }

    /// Write a checkpoint entry
    pub async fn checkpoint(&self) -> Result<u64> {
        let sequence = self.next_sequence.fetch_add(1, Ordering::SeqCst);
        let entry = WalEntry::new_checkpoint(sequence);
        self.append(&entry).await?;

        // Force sync on checkpoint
        if let Some(file) = self.file.read().await.as_ref() {
            file.sync_all().await?;
        }
        *self.pending_writes.write().await = 0;

        Ok(sequence)
    }

    /// Sync WAL to disk
    pub async fn sync(&self) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        if let Some(file) = self.file.read().await.as_ref() {
            file.sync_data().await?;
        }
        *self.pending_writes.write().await = 0;
        Ok(())
    }

    /// Close the WAL
    pub async fn close(&self) -> Result<()> {
        if let Some(file) = self.file.read().await.as_ref() {
            file.sync_all().await?;
        }
        *self.file.write().await = None;
        Ok(())
    }
}

/// Async WAL reader for recovery
pub struct AsyncWalReader;

impl AsyncWalReader {
    /// Read all entries from a WAL file asynchronously
    ///
    /// Note: This currently uses the synchronous reader internally but
    /// wraps it in spawn_blocking to avoid blocking the async runtime.
    pub async fn read_file(path: &Path) -> Result<Vec<WalEntry>> {
        use crate::storage::wal::WalReader;

        let path = path.to_path_buf();
        tokio::task::spawn_blocking(move || WalReader::read_file(&path))
            .await
            .map_err(|e| StreamlineError::storage_msg(format!("Failed to read WAL: {}", e)))?
    }

    /// Read all entries from a WAL file with recovery statistics
    pub async fn read_file_with_stats(path: &Path) -> Result<(Vec<WalEntry>, WalRecoveryStats)> {
        use crate::storage::wal::WalReader;

        let path = path.to_path_buf();
        tokio::task::spawn_blocking(move || WalReader::read_file_with_stats(&path))
            .await
            .map_err(|e| StreamlineError::storage_msg(format!("Failed to read WAL: {}", e)))?
    }

    /// Read all entries from all WAL files in a directory
    pub async fn read_all(wal_dir: &Path) -> Result<Vec<WalEntry>> {
        use crate::storage::wal::WalReader;

        let wal_dir = wal_dir.to_path_buf();
        tokio::task::spawn_blocking(move || WalReader::read_all(&wal_dir))
            .await
            .map_err(|e| StreamlineError::storage_msg(format!("Failed to read WAL: {}", e)))?
    }

    /// Read all entries with recovery statistics
    pub async fn read_all_with_stats(wal_dir: &Path) -> Result<(Vec<WalEntry>, WalRecoveryStats)> {
        use crate::storage::wal::WalReader;

        let wal_dir = wal_dir.to_path_buf();
        tokio::task::spawn_blocking(move || WalReader::read_all_with_stats(&wal_dir))
            .await
            .map_err(|e| StreamlineError::storage_msg(format!("Failed to read WAL: {}", e)))?
    }

    /// Read entries after a given sequence number
    pub async fn read_after_sequence(wal_dir: &Path, after_sequence: u64) -> Result<Vec<WalEntry>> {
        let all_entries = Self::read_all(wal_dir).await?;
        Ok(all_entries
            .into_iter()
            .filter(|e| e.sequence > after_sequence)
            .collect())
    }
}

/// Create a WAL header
fn create_wal_header() -> [u8; WAL_HEADER_SIZE] {
    let mut buf = [0u8; WAL_HEADER_SIZE];
    buf[0..4].copy_from_slice(WAL_MAGIC);
    buf[4..6].copy_from_slice(&WAL_VERSION.to_le_bytes());
    buf[6..8].copy_from_slice(&0u16.to_le_bytes()); // flags
    buf[8..16].copy_from_slice(&chrono::Utc::now().timestamp_millis().to_le_bytes());
    buf
}

// Helper function to parse SyncMode from string
fn parse_sync_mode(s: &str) -> SyncMode {
    match s.to_lowercase().as_str() {
        "every_write" => SyncMode::EveryWrite,
        "os_default" => SyncMode::OsDefault,
        _ => SyncMode::Interval,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::io_backend::get_standard_backend;
    use crate::storage::wal::WalEntryType;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_async_wal_basic() {
        let dir = tempdir().unwrap();
        let config = WalConfig {
            enabled: true,
            sync_mode: "interval".to_string(),
            ..Default::default()
        };

        let fs = get_standard_backend();
        let wal = AsyncWalWriter::new(&fs, dir.path(), config).await.unwrap();

        // Write some entries
        let seq1 = wal
            .append_record("topic1", 0, Some(&Bytes::from("k1")), &Bytes::from("v1"))
            .await
            .unwrap();
        let seq2 = wal
            .append_record("topic1", 0, None, &Bytes::from("v2"))
            .await
            .unwrap();

        assert_eq!(seq1, 0);
        assert_eq!(seq2, 1);

        // Sync and close
        wal.sync().await.unwrap();
        wal.close().await.unwrap();

        // Read back
        let wal_dir = dir.path().join("wal");
        let entries = AsyncWalReader::read_all(&wal_dir).await.unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].sequence, 0);
        assert_eq!(entries[1].sequence, 1);
    }

    #[tokio::test]
    async fn test_async_wal_disabled() {
        let dir = tempdir().unwrap();
        let config = WalConfig {
            enabled: false,
            ..Default::default()
        };

        let fs = get_standard_backend();
        let wal = AsyncWalWriter::new(&fs, dir.path(), config).await.unwrap();
        assert!(!wal.is_enabled());

        // Writes should succeed but not persist
        wal.append_record("topic1", 0, None, &Bytes::from("v1"))
            .await
            .unwrap();
        wal.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_async_wal_checkpoint() {
        let dir = tempdir().unwrap();
        let config = WalConfig {
            enabled: true,
            ..Default::default()
        };

        let fs = get_standard_backend();
        let wal = AsyncWalWriter::new(&fs, dir.path(), config).await.unwrap();

        wal.append_record("topic1", 0, None, &Bytes::from("v1"))
            .await
            .unwrap();
        let checkpoint_seq = wal.checkpoint().await.unwrap();
        wal.append_record("topic1", 0, None, &Bytes::from("v2"))
            .await
            .unwrap();

        wal.close().await.unwrap();

        let wal_dir = dir.path().join("wal");
        let entries = AsyncWalReader::read_all(&wal_dir).await.unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[1].entry_type, WalEntryType::Checkpoint);
        assert_eq!(entries[1].sequence, checkpoint_seq);
    }

    #[tokio::test]
    async fn test_async_wal_with_buffer_pool() {
        let dir = tempdir().unwrap();
        let config = WalConfig {
            enabled: true,
            ..Default::default()
        };

        let fs = get_standard_backend();
        let pool = Arc::new(IoBufferPool::default_pool());
        let wal = AsyncWalWriter::with_buffer_pool(&fs, dir.path(), config, pool.clone())
            .await
            .unwrap();

        // Write some entries
        for i in 0..10 {
            wal.append_record("topic1", 0, None, &Bytes::from(format!("value-{}", i)))
                .await
                .unwrap();
        }

        wal.sync().await.unwrap();
        wal.close().await.unwrap();

        // Verify buffer pool was used
        let stats = pool.stats();
        assert!(stats.allocations > 0 || stats.reuses > 0);

        // Verify entries were written
        let wal_dir = dir.path().join("wal");
        let entries = AsyncWalReader::read_all(&wal_dir).await.unwrap();
        assert_eq!(entries.len(), 10);
    }

    #[tokio::test]
    async fn test_async_wal_every_write_sync() {
        let dir = tempdir().unwrap();
        let config = WalConfig {
            enabled: true,
            sync_mode: "every_write".to_string(),
            ..Default::default()
        };

        let fs = get_standard_backend();
        let wal = AsyncWalWriter::new(&fs, dir.path(), config).await.unwrap();

        // Each write should be immediately synced
        for i in 0..5 {
            wal.append_record("topic1", 0, None, &Bytes::from(format!("value-{}", i)))
                .await
                .unwrap();
        }

        wal.close().await.unwrap();

        let wal_dir = dir.path().join("wal");
        let entries = AsyncWalReader::read_all(&wal_dir).await.unwrap();
        assert_eq!(entries.len(), 5);
    }
}
