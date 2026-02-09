//! Async sparse index implementation for efficient offset lookups
//!
//! This module provides an async version of SegmentIndex that uses io_backend
//! traits for io_uring compatibility on Linux 5.11+.
//!
//! Each segment has an accompanying `.index` file that maps offsets to file positions,
//! enabling O(log n) offset lookups instead of O(n) linear scans.

use crate::error::{Result, StreamlineError};
use crate::storage::async_io;
use crate::storage::index::{IndexEntry, DEFAULT_MAX_INDEX_ENTRIES};
use crate::storage::io_backend::{AsyncFile, AsyncFileSystem, IoBufferPool};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::{debug, warn};

/// Magic bytes for index file header
const INDEX_MAGIC: &[u8; 4] = b"SIDX";

/// Current index format version
const INDEX_VERSION: u16 = 1;

/// Size of index header in bytes
const INDEX_HEADER_SIZE: usize = 16;

/// Size of each index entry in bytes (offset: i64 + position: u64 = 16 bytes)
const INDEX_ENTRY_SIZE: usize = 16;

/// Create the index header bytes
fn create_index_header(interval_bytes: u64) -> [u8; INDEX_HEADER_SIZE] {
    let mut header = [0u8; INDEX_HEADER_SIZE];
    header[0..4].copy_from_slice(INDEX_MAGIC);
    header[4..6].copy_from_slice(&INDEX_VERSION.to_le_bytes());
    // header[6..8] reserved (flags)
    header[8..16].copy_from_slice(&interval_bytes.to_le_bytes());
    header
}

/// Async sparse index for a segment file
///
/// The sparse index stores entries at intervals, allowing efficient
/// binary search to find the approximate position of an offset,
/// followed by a short linear scan.
///
/// This async version uses io_backend traits for io_uring support.
pub struct AsyncSegmentIndex {
    /// Path to the index file
    path: PathBuf,

    /// Index entries (sorted by offset)
    entries: Vec<IndexEntry>,

    /// Interval in bytes between index entries
    interval_bytes: u64,

    /// Bytes written since last index entry
    bytes_since_last_entry: u64,

    /// Maximum number of entries to store (prevents unbounded growth)
    max_entries: usize,

    /// Optional buffer pool for reduced allocations
    buffer_pool: Option<Arc<IoBufferPool>>,
}

impl std::fmt::Debug for AsyncSegmentIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncSegmentIndex")
            .field("path", &self.path)
            .field("entries", &self.entries.len())
            .field("interval_bytes", &self.interval_bytes)
            .field("bytes_since_last_entry", &self.bytes_since_last_entry)
            .field("max_entries", &self.max_entries)
            .field("has_buffer_pool", &self.buffer_pool.is_some())
            .finish()
    }
}

impl AsyncSegmentIndex {
    /// Default interval between index entries (4 KB)
    pub const DEFAULT_INTERVAL_BYTES: u64 = 4096;

    /// Create a new empty index for a segment
    pub fn new(path: &Path) -> Self {
        Self {
            path: path.to_path_buf(),
            entries: Vec::new(),
            interval_bytes: Self::DEFAULT_INTERVAL_BYTES,
            bytes_since_last_entry: 0,
            max_entries: DEFAULT_MAX_INDEX_ENTRIES,
            buffer_pool: None,
        }
    }

    /// Create a new index with custom interval
    pub fn with_interval(path: &Path, interval_bytes: u64) -> Self {
        Self {
            path: path.to_path_buf(),
            entries: Vec::new(),
            interval_bytes: interval_bytes.max(1024), // Minimum 1 KB
            bytes_since_last_entry: 0,
            max_entries: DEFAULT_MAX_INDEX_ENTRIES,
            buffer_pool: None,
        }
    }

    /// Create a new index with custom interval and max entries
    pub fn with_config(path: &Path, interval_bytes: u64, max_entries: usize) -> Self {
        Self {
            path: path.to_path_buf(),
            entries: Vec::new(),
            interval_bytes: interval_bytes.max(1024),
            bytes_since_last_entry: 0,
            max_entries: max_entries.max(100), // Minimum 100 entries
            buffer_pool: None,
        }
    }

    /// Set the buffer pool for I/O operations
    pub fn with_buffer_pool(mut self, pool: Arc<IoBufferPool>) -> Self {
        self.buffer_pool = Some(pool);
        self
    }

    /// Load an existing index from disk using async I/O
    pub async fn load<F, FS>(fs: &FS, path: &Path) -> Result<Self>
    where
        F: AsyncFile + 'static,
        FS: AsyncFileSystem<File = F>,
    {
        Self::load_with_config(fs, path, DEFAULT_MAX_INDEX_ENTRIES, None).await
    }

    /// Load an existing index with custom configuration
    pub async fn load_with_config<F, FS>(
        fs: &FS,
        path: &Path,
        max_entries: usize,
        buffer_pool: Option<Arc<IoBufferPool>>,
    ) -> Result<Self>
    where
        F: AsyncFile + 'static,
        FS: AsyncFileSystem<File = F>,
    {
        let file = fs.open(path).await?;
        let file_size = file.size().await?;

        if file_size < INDEX_HEADER_SIZE as u64 {
            return Err(StreamlineError::CorruptedData(
                "Index file too small for header".to_string(),
            ));
        }

        // Read header
        let header_buf = Self::acquire_buffer(&buffer_pool, INDEX_HEADER_SIZE);
        let (result, header_buf) = file.read_at(header_buf, 0).await;
        let bytes_read = result?;

        if bytes_read < INDEX_HEADER_SIZE {
            return Err(StreamlineError::CorruptedData(
                "Failed to read index header".to_string(),
            ));
        }

        // Verify magic
        if &header_buf[0..4] != INDEX_MAGIC {
            return Err(StreamlineError::CorruptedData(
                "Invalid index magic".to_string(),
            ));
        }

        // Read version
        let version = u16::from_le_bytes([header_buf[4], header_buf[5]]);
        if version != INDEX_VERSION {
            return Err(StreamlineError::CorruptedData(format!(
                "Unsupported index version: {}",
                version
            )));
        }

        // Read interval_bytes
        let interval_bytes_arr: [u8; 8] = header_buf[8..16]
            .try_into()
            .map_err(|_| StreamlineError::CorruptedData("Index header malformed".to_string()))?;
        let interval_bytes = u64::from_le_bytes(interval_bytes_arr);

        // Return header buffer to pool
        Self::release_buffer(&buffer_pool, header_buf);

        // Calculate number of entries
        let entries_size = file_size - INDEX_HEADER_SIZE as u64;
        let num_entries = entries_size as usize / INDEX_ENTRY_SIZE;

        // Read all entries
        let mut entries = Vec::with_capacity(num_entries);

        if num_entries > 0 {
            let data_size = num_entries * INDEX_ENTRY_SIZE;
            let data_buf = Self::acquire_buffer(&buffer_pool, data_size);
            let (result, data_buf) = file.read_at(data_buf, INDEX_HEADER_SIZE as u64).await;
            let bytes_read = result?;

            // Parse entries
            for i in 0..(bytes_read / INDEX_ENTRY_SIZE) {
                let start = i * INDEX_ENTRY_SIZE;
                let end = start + INDEX_ENTRY_SIZE;
                if let Some(entry) = IndexEntry::from_bytes(&data_buf[start..end]) {
                    entries.push(entry);
                }
            }

            Self::release_buffer(&buffer_pool, data_buf);
        }

        debug!(
            path = ?path,
            entries = entries.len(),
            interval_bytes = interval_bytes,
            "Loaded async index"
        );

        Ok(Self {
            path: path.to_path_buf(),
            entries,
            interval_bytes,
            bytes_since_last_entry: 0,
            max_entries: max_entries.max(100),
            buffer_pool,
        })
    }

    /// Save the index to disk atomically using async I/O
    ///
    /// Uses write-to-temp-then-rename pattern to ensure the index file
    /// is never in a partially-written state.
    pub async fn save<F, FS>(&self, fs: &FS) -> Result<()>
    where
        F: AsyncFile + 'static,
        FS: AsyncFileSystem<File = F>,
    {
        // Write to a temporary file first (atomic write pattern)
        let temp_path = self.path.with_extension("index.tmp");

        let file = fs.create(&temp_path).await?;

        // Calculate total size
        let total_size = INDEX_HEADER_SIZE + self.entries.len() * INDEX_ENTRY_SIZE;
        let mut buf = Self::acquire_buffer(&self.buffer_pool, total_size);
        buf.resize(total_size, 0);

        // Write header
        let header = create_index_header(self.interval_bytes);
        buf[0..INDEX_HEADER_SIZE].copy_from_slice(&header);

        // Write entries
        for (i, entry) in self.entries.iter().enumerate() {
            let start = INDEX_HEADER_SIZE + i * INDEX_ENTRY_SIZE;
            buf[start..start + INDEX_ENTRY_SIZE].copy_from_slice(&entry.to_bytes());
        }

        // Write to file
        let (result, buf) = file.write_at(buf, 0).await;
        result?;

        // Sync to disk
        file.sync_all().await?;

        // Return buffer to pool
        Self::release_buffer(&self.buffer_pool, buf);

        // Atomically rename temp file to target
        async_io::rename_async(temp_path.clone(), self.path.clone()).await?;

        // Sync the parent directory to ensure rename is durable
        if let Some(parent) = self.path.parent() {
            if let Err(e) = async_io::sync_dir_async(parent.to_path_buf()).await {
                warn!(
                    path = ?parent,
                    error = %e,
                    "Directory fsync failed - rename durability not guaranteed"
                );
            }
        }

        debug!(
            path = ?self.path,
            entries = self.entries.len(),
            "Saved async index"
        );

        Ok(())
    }

    /// Add an entry to the index
    ///
    /// Call this when writing a batch to the segment.
    /// The entry is only added if enough bytes have been written since the last entry
    /// and the index hasn't reached its maximum entry limit.
    pub fn maybe_add_entry(&mut self, offset: i64, position: u64, batch_size: u64) {
        // Don't add if we've reached the maximum entries
        if self.entries.len() >= self.max_entries {
            return;
        }

        // Always add the first entry
        if self.entries.is_empty() {
            self.entries.push(IndexEntry::new(offset, position));
            self.bytes_since_last_entry = batch_size;
            return;
        }

        self.bytes_since_last_entry += batch_size;

        // Add entry if we've exceeded the interval
        if self.bytes_since_last_entry >= self.interval_bytes {
            self.entries.push(IndexEntry::new(offset, position));
            self.bytes_since_last_entry = 0;
        }
    }

    /// Force add an entry (used when sealing segments)
    pub fn add_entry(&mut self, offset: i64, position: u64) {
        // Don't add if we've reached the maximum entries
        if self.entries.len() >= self.max_entries {
            return;
        }

        // Avoid duplicate entries
        if let Some(last) = self.entries.last() {
            if last.offset == offset && last.position == position {
                return;
            }
        }
        self.entries.push(IndexEntry::new(offset, position));
    }

    /// Lookup the file position for a given offset using binary search
    ///
    /// Returns the position of the batch that contains or precedes the target offset.
    pub fn lookup(&self, target_offset: i64) -> Option<u64> {
        if self.entries.is_empty() {
            return None;
        }

        // Binary search for the largest entry with offset <= target_offset
        match self
            .entries
            .binary_search_by_key(&target_offset, |e| e.offset)
        {
            Ok(idx) => Some(self.entries[idx].position),
            Err(idx) => {
                if idx == 0 {
                    Some(self.entries[0].position)
                } else {
                    Some(self.entries[idx - 1].position)
                }
            }
        }
    }

    /// Get the number of entries in the index
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if the index is empty
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Get all entries (for debugging/testing)
    #[allow(dead_code)]
    pub(crate) fn entries(&self) -> &[IndexEntry] {
        &self.entries
    }

    /// Get the path to this index file
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Delete the index file from disk
    pub async fn delete(self) -> Result<()> {
        if async_io::exists_async(self.path.clone())
            .await
            .unwrap_or(false)
        {
            async_io::remove_file_async(self.path.clone()).await?;
        }
        Ok(())
    }

    /// Acquire a buffer of the given size
    fn acquire_buffer(pool: &Option<Arc<IoBufferPool>>, size: usize) -> Vec<u8> {
        if let Some(pool) = pool {
            let mut buf = pool.acquire(size);
            buf.resize(size, 0);
            buf
        } else {
            vec![0u8; size]
        }
    }

    /// Release a buffer back to the pool
    fn release_buffer(pool: &Option<Arc<IoBufferPool>>, buf: Vec<u8>) {
        if let Some(pool) = pool {
            pool.release(buf);
        }
    }
}

/// Async index builder for constructing indexes from existing segments
pub struct AsyncIndexBuilder {
    entries: Vec<IndexEntry>,
    interval_bytes: u64,
    bytes_since_last_entry: u64,
    max_entries: usize,
}

impl AsyncIndexBuilder {
    /// Create a new index builder
    pub fn new(interval_bytes: u64) -> Self {
        Self {
            entries: Vec::new(),
            interval_bytes: interval_bytes.max(1024),
            bytes_since_last_entry: 0,
            max_entries: DEFAULT_MAX_INDEX_ENTRIES,
        }
    }

    /// Create a new index builder with custom max entries
    pub fn with_max_entries(interval_bytes: u64, max_entries: usize) -> Self {
        Self {
            entries: Vec::new(),
            interval_bytes: interval_bytes.max(1024),
            bytes_since_last_entry: 0,
            max_entries: max_entries.max(100),
        }
    }

    /// Record a batch at the given position
    pub fn record_batch(&mut self, first_offset: i64, position: u64, batch_size: u64) {
        if self.entries.len() >= self.max_entries {
            return;
        }

        if self.entries.is_empty() {
            self.entries.push(IndexEntry::new(first_offset, position));
            self.bytes_since_last_entry = batch_size;
            return;
        }

        self.bytes_since_last_entry += batch_size;

        if self.bytes_since_last_entry >= self.interval_bytes {
            self.entries.push(IndexEntry::new(first_offset, position));
            self.bytes_since_last_entry = 0;
        }
    }

    /// Build the final async index
    pub fn build(self, path: &Path) -> AsyncSegmentIndex {
        AsyncSegmentIndex {
            path: path.to_path_buf(),
            entries: self.entries,
            interval_bytes: self.interval_bytes,
            bytes_since_last_entry: 0,
            max_entries: self.max_entries,
            buffer_pool: None,
        }
    }

    /// Build the final async index with a buffer pool
    pub fn build_with_pool(self, path: &Path, pool: Arc<IoBufferPool>) -> AsyncSegmentIndex {
        AsyncSegmentIndex {
            path: path.to_path_buf(),
            entries: self.entries,
            interval_bytes: self.interval_bytes,
            bytes_since_last_entry: 0,
            max_entries: self.max_entries,
            buffer_pool: Some(pool),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::io_backend::get_standard_backend;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_async_index_basic() {
        let dir = tempdir().unwrap();
        let index_path = dir.path().join("test.index");

        let fs = get_standard_backend();

        // Create and populate index
        let mut index = AsyncSegmentIndex::new(&index_path);
        index.add_entry(0, 64);
        index.add_entry(100, 1000);
        index.add_entry(200, 2000);

        // Save
        index.save(&fs).await.unwrap();

        // Load and verify
        let loaded = AsyncSegmentIndex::load(&fs, &index_path).await.unwrap();
        assert_eq!(loaded.len(), 3);
        assert_eq!(loaded.lookup(0), Some(64));
        assert_eq!(loaded.lookup(100), Some(1000));
        assert_eq!(loaded.lookup(150), Some(1000)); // Between 100 and 200
        assert_eq!(loaded.lookup(200), Some(2000));
    }

    #[tokio::test]
    async fn test_async_index_with_buffer_pool() {
        let dir = tempdir().unwrap();
        let index_path = dir.path().join("test.index");

        let fs = get_standard_backend();
        let pool = Arc::new(IoBufferPool::default_pool());

        // Create and populate index with buffer pool
        let mut index =
            AsyncSegmentIndex::with_interval(&index_path, 1024).with_buffer_pool(pool.clone());

        for i in 0..10 {
            index.add_entry(i * 100, i as u64 * 1000);
        }

        // Save
        index.save(&fs).await.unwrap();

        // Load with buffer pool
        let loaded = AsyncSegmentIndex::load_with_config(
            &fs,
            &index_path,
            DEFAULT_MAX_INDEX_ENTRIES,
            Some(pool.clone()),
        )
        .await
        .unwrap();

        assert_eq!(loaded.len(), 10);

        // Verify buffer pool was used
        let stats = pool.stats();
        assert!(stats.allocations > 0 || stats.reuses > 0);
    }

    #[tokio::test]
    async fn test_async_index_binary_search() {
        let dir = tempdir().unwrap();
        let index_path = dir.path().join("test.index");

        let mut index = AsyncSegmentIndex::new(&index_path);

        // Add entries at offsets 0, 100, 200, 300, 400
        index.add_entry(0, 64);
        index.add_entry(100, 1000);
        index.add_entry(200, 2000);
        index.add_entry(300, 3000);
        index.add_entry(400, 4000);

        // Exact matches
        assert_eq!(index.lookup(0), Some(64));
        assert_eq!(index.lookup(100), Some(1000));
        assert_eq!(index.lookup(200), Some(2000));

        // Between entries
        assert_eq!(index.lookup(50), Some(64));
        assert_eq!(index.lookup(150), Some(1000));

        // Beyond all entries
        assert_eq!(index.lookup(500), Some(4000));
    }

    #[tokio::test]
    async fn test_async_index_interval_based_addition() {
        let dir = tempdir().unwrap();
        let index_path = dir.path().join("test.index");

        let mut index = AsyncSegmentIndex::with_interval(&index_path, 1024);

        // First entry is always added
        index.maybe_add_entry(0, 64, 100);
        assert_eq!(index.len(), 1);

        // Not enough bytes yet
        index.maybe_add_entry(10, 200, 100);
        assert_eq!(index.len(), 1);

        // Add more until we exceed interval
        for i in 0..10 {
            index.maybe_add_entry(20 + i * 10, 300 + i as u64 * 100, 100);
        }
        // After 10 more entries of 100 bytes each: 200 + 1000 = 1200 >= 1024
        assert_eq!(index.len(), 2);
    }

    #[tokio::test]
    async fn test_async_index_builder() {
        let dir = tempdir().unwrap();
        let index_path = dir.path().join("test.index");

        let mut builder = AsyncIndexBuilder::new(1000);

        builder.record_batch(0, 64, 500);
        builder.record_batch(10, 600, 500);
        builder.record_batch(20, 1100, 500); // Triggers new entry
        builder.record_batch(30, 1600, 500);

        let index = builder.build(&index_path);

        assert_eq!(index.len(), 2);
        assert_eq!(index.entries()[0].offset, 0);
        assert_eq!(index.entries()[1].offset, 20);
    }

    #[tokio::test]
    async fn test_async_index_delete() {
        let dir = tempdir().unwrap();
        let index_path = dir.path().join("test.index");

        let fs = get_standard_backend();

        // Create and save
        let mut index = AsyncSegmentIndex::new(&index_path);
        index.add_entry(0, 64);
        index.save(&fs).await.unwrap();

        assert!(index_path.exists());

        // Delete
        index.delete().await.unwrap();
        assert!(!index_path.exists());
    }

    #[tokio::test]
    async fn test_async_index_max_entries_limit() {
        let dir = tempdir().unwrap();
        let index_path = dir.path().join("test.index");

        let mut index = AsyncSegmentIndex::with_config(&index_path, 1024, 100);

        // Add entries up to the limit
        for i in 0..100 {
            index.add_entry(i * 10, i as u64 * 100);
        }
        assert_eq!(index.len(), 100);

        // Try to add more - should be ignored
        index.add_entry(1000, 10000);
        assert_eq!(index.len(), 100);
    }
}
