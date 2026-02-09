// Sparse index scaffolding (not yet fully integrated)
#![allow(dead_code)]

//! Sparse index implementation for efficient offset lookups
//!
//! This module provides O(log n) offset lookups instead of O(n) linear scans.
//! Each segment has an accompanying `.index` file that maps offsets to file positions.

use crate::error::{Result, StreamlineError};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use tracing::warn;

/// Magic bytes for index file header
const INDEX_MAGIC: &[u8; 4] = b"SIDX";

/// Current index format version
const INDEX_VERSION: u16 = 1;

/// Default maximum number of entries in an index (10,000)
/// This prevents unbounded memory growth for very large segments
pub const DEFAULT_MAX_INDEX_ENTRIES: usize = 10_000;

/// Size of index header in bytes
const INDEX_HEADER_SIZE: usize = 16;

/// Size of each index entry in bytes (offset: i64 + position: u64 = 16 bytes)
const INDEX_ENTRY_SIZE: usize = 16;

/// An entry in the sparse index
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IndexEntry {
    /// The message offset
    pub offset: i64,
    /// The file position where this batch starts
    pub position: u64,
}

impl IndexEntry {
    /// Create a new index entry
    pub fn new(offset: i64, position: u64) -> Self {
        Self { offset, position }
    }

    /// Serialize entry to bytes
    pub fn to_bytes(self) -> [u8; INDEX_ENTRY_SIZE] {
        let mut buf = [0u8; INDEX_ENTRY_SIZE];
        buf[0..8].copy_from_slice(&self.offset.to_le_bytes());
        buf[8..16].copy_from_slice(&self.position.to_le_bytes());
        buf
    }

    /// Deserialize entry from bytes
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < INDEX_ENTRY_SIZE {
            return None;
        }
        let offset = i64::from_le_bytes(data[0..8].try_into().ok()?);
        let position = u64::from_le_bytes(data[8..16].try_into().ok()?);
        Some(Self { offset, position })
    }
}

/// Sparse index for a segment file
///
/// The sparse index stores entries at intervals, allowing efficient
/// binary search to find the approximate position of an offset,
/// followed by a short linear scan.
#[derive(Debug)]
pub(crate) struct SegmentIndex {
    /// Path to the index file
    path: PathBuf,

    /// Index entries (sorted by offset)
    entries: Vec<IndexEntry>,

    /// Interval in bytes between index entries
    /// (an entry is added every `interval_bytes` of data written)
    interval_bytes: u64,

    /// Bytes written since last index entry
    bytes_since_last_entry: u64,

    /// Maximum number of entries to store (prevents unbounded growth)
    max_entries: usize,
}

impl SegmentIndex {
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
        }
    }

    /// Create a new index with custom interval and max entries
    pub fn with_config(path: &Path, interval_bytes: u64, max_entries: usize) -> Self {
        Self {
            path: path.to_path_buf(),
            entries: Vec::new(),
            interval_bytes: interval_bytes.max(1024), // Minimum 1 KB
            bytes_since_last_entry: 0,
            max_entries: max_entries.max(100), // Minimum 100 entries
        }
    }

    /// Load an existing index from disk
    pub fn load(path: &Path) -> Result<Self> {
        let mut file = BufReader::new(File::open(path)?);

        // Read header
        let mut header = [0u8; INDEX_HEADER_SIZE];
        file.read_exact(&mut header)?;

        // Verify magic
        if &header[0..4] != INDEX_MAGIC {
            return Err(StreamlineError::CorruptedData(
                "Invalid index magic".to_string(),
            ));
        }

        // Read version
        let version = u16::from_le_bytes([header[4], header[5]]);
        if version != INDEX_VERSION {
            return Err(StreamlineError::CorruptedData(format!(
                "Unsupported index version: {}",
                version
            )));
        }

        // Read interval_bytes (safe: header is exactly 16 bytes)
        let interval_bytes_arr: [u8; 8] = header[8..16]
            .try_into()
            .map_err(|_| StreamlineError::CorruptedData("Index header malformed".to_string()))?;
        let interval_bytes = u64::from_le_bytes(interval_bytes_arr);

        // Read entries
        let mut entries = Vec::new();
        let mut entry_buf = [0u8; INDEX_ENTRY_SIZE];

        while file.read_exact(&mut entry_buf).is_ok() {
            if let Some(entry) = IndexEntry::from_bytes(&entry_buf) {
                entries.push(entry);
            }
        }

        Ok(Self {
            path: path.to_path_buf(),
            entries,
            interval_bytes,
            bytes_since_last_entry: 0,
            max_entries: DEFAULT_MAX_INDEX_ENTRIES,
        })
    }

    /// Load an existing index from disk with custom max entries
    pub fn load_with_max_entries(path: &Path, max_entries: usize) -> Result<Self> {
        let mut index = Self::load(path)?;
        index.max_entries = max_entries.max(100);
        Ok(index)
    }

    /// Save the index to disk atomically
    ///
    /// Uses write-to-temp-then-rename pattern to ensure the index file
    /// is never in a partially-written state. This prevents corruption
    /// if the process crashes during save.
    pub fn save(&self) -> Result<()> {
        // Write to a temporary file first (atomic write pattern)
        let temp_path = self.path.with_extension("index.tmp");

        {
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&temp_path)?;

            let mut writer = BufWriter::new(file);

            // Write header
            let mut header = [0u8; INDEX_HEADER_SIZE];
            header[0..4].copy_from_slice(INDEX_MAGIC);
            header[4..6].copy_from_slice(&INDEX_VERSION.to_le_bytes());
            // header[6..8] reserved (flags)
            header[8..16].copy_from_slice(&self.interval_bytes.to_le_bytes());
            writer.write_all(&header)?;

            // Write entries
            for entry in &self.entries {
                writer.write_all(&entry.to_bytes())?;
            }

            writer.flush()?;
            writer.get_ref().sync_all()?;
        }

        // Atomically rename temp file to target (atomic on POSIX systems)
        std::fs::rename(&temp_path, &self.path)?;

        // Sync the parent directory to ensure rename is durable
        if let Some(parent) = self.path.parent() {
            if let Ok(dir) = std::fs::File::open(parent) {
                if let Err(e) = dir.sync_all() {
                    warn!(
                        path = ?parent,
                        error = %e,
                        "Directory fsync failed - rename durability not guaranteed"
                    );
                }
            }
        }

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
    ///
    /// Note: This respects the max_entries limit to prevent unbounded growth.
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
    /// The caller should seek to this position and scan forward to find the exact offset.
    pub fn lookup(&self, target_offset: i64) -> Option<u64> {
        if self.entries.is_empty() {
            return None;
        }

        // Binary search for the largest entry with offset <= target_offset
        match self
            .entries
            .binary_search_by_key(&target_offset, |e| e.offset)
        {
            Ok(idx) => {
                // Exact match
                Some(self.entries[idx].position)
            }
            Err(idx) => {
                if idx == 0 {
                    // Target is before all entries, return first position
                    Some(self.entries[0].position)
                } else {
                    // Return the entry just before where target would be inserted
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
    pub fn entries(&self) -> &[IndexEntry] {
        &self.entries
    }

    /// Get the path to this index file
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Delete the index file from disk
    pub fn delete(self) -> Result<()> {
        if self.path.exists() {
            std::fs::remove_file(&self.path)?;
        }
        Ok(())
    }
}

/// Generate index filename from segment base offset
pub fn index_filename(base_offset: i64) -> String {
    format!("{:020}.index", base_offset)
}

/// Get the index path for a segment path
pub fn index_path_for_segment(segment_path: &Path) -> PathBuf {
    segment_path.with_extension("index")
}

/// Index builder for constructing indexes from existing segments
pub(crate) struct IndexBuilder {
    entries: Vec<IndexEntry>,
    interval_bytes: u64,
    bytes_since_last_entry: u64,
    max_entries: usize,
}

impl IndexBuilder {
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
        // Don't record if we've reached max entries
        if self.entries.len() >= self.max_entries {
            return;
        }

        // Always record the first entry
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

    /// Build the final index
    pub fn build(self, path: &Path) -> SegmentIndex {
        SegmentIndex {
            path: path.to_path_buf(),
            entries: self.entries,
            interval_bytes: self.interval_bytes,
            bytes_since_last_entry: 0,
            max_entries: self.max_entries,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_index_entry_serialization() {
        let entry = IndexEntry::new(12345, 67890);
        let bytes = entry.to_bytes();
        let parsed = IndexEntry::from_bytes(&bytes).unwrap();

        assert_eq!(parsed.offset, 12345);
        assert_eq!(parsed.position, 67890);
    }

    #[test]
    fn test_index_empty() {
        let dir = tempdir().unwrap();
        let index_path = dir.path().join("test.index");
        let index = SegmentIndex::new(&index_path);

        assert!(index.is_empty());
        assert_eq!(index.len(), 0);
        assert_eq!(index.lookup(100), None);
    }

    #[test]
    fn test_index_single_entry() {
        let dir = tempdir().unwrap();
        let index_path = dir.path().join("test.index");
        let mut index = SegmentIndex::new(&index_path);

        index.add_entry(0, 64);

        assert_eq!(index.len(), 1);
        assert_eq!(index.lookup(0), Some(64));
        assert_eq!(index.lookup(100), Some(64)); // Returns last valid entry
    }

    #[test]
    fn test_index_binary_search() {
        let dir = tempdir().unwrap();
        let index_path = dir.path().join("test.index");
        let mut index = SegmentIndex::new(&index_path);

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
        assert_eq!(index.lookup(300), Some(3000));
        assert_eq!(index.lookup(400), Some(4000));

        // Between entries - should return position of preceding entry
        assert_eq!(index.lookup(50), Some(64)); // Between 0 and 100
        assert_eq!(index.lookup(150), Some(1000)); // Between 100 and 200
        assert_eq!(index.lookup(250), Some(2000)); // Between 200 and 300

        // Beyond all entries
        assert_eq!(index.lookup(500), Some(4000));
        assert_eq!(index.lookup(1000), Some(4000));
    }

    #[test]
    fn test_index_persistence() {
        let dir = tempdir().unwrap();
        let index_path = dir.path().join("test.index");

        // Create and save index
        {
            let mut index = SegmentIndex::with_interval(&index_path, 1024);
            index.add_entry(0, 64);
            index.add_entry(100, 1064);
            index.add_entry(200, 2064);
            index.save().unwrap();
        }

        // Load and verify
        {
            let index = SegmentIndex::load(&index_path).unwrap();
            assert_eq!(index.len(), 3);
            assert_eq!(index.lookup(0), Some(64));
            assert_eq!(index.lookup(100), Some(1064));
            assert_eq!(index.lookup(200), Some(2064));
        }
    }

    #[test]
    fn test_index_interval_based_addition() {
        let dir = tempdir().unwrap();
        let index_path = dir.path().join("test.index");
        // Note: minimum interval is 1024 bytes, so we use that
        let mut index = SegmentIndex::with_interval(&index_path, 1024);

        // First entry is always added (bytes_since_last_entry = 100)
        index.maybe_add_entry(0, 64, 100);
        assert_eq!(index.len(), 1);

        // Second entry not added (bytes_since_last_entry = 200)
        index.maybe_add_entry(10, 200, 100);
        assert_eq!(index.len(), 1);

        // Keep adding until we get close to 1024 bytes
        // After 9 more entries of 100 bytes: 200 + 900 = 1100 >= 1024
        for i in 0..8 {
            index.maybe_add_entry(20 + i * 10, 300 + i as u64 * 100, 100);
        }
        // Now bytes_since_last_entry = 200 + 800 = 1000 < 1024
        assert_eq!(index.len(), 1);

        // One more 100 byte entry should trigger (1000 + 100 = 1100, >= 1024)
        index.maybe_add_entry(100, 1100, 100);
        assert_eq!(index.len(), 2);

        // Next entry resets counter to 0, need another 1024 bytes
        index.maybe_add_entry(110, 1200, 500);
        assert_eq!(index.len(), 2); // 500 < 1024

        index.maybe_add_entry(120, 1700, 600);
        assert_eq!(index.len(), 3); // 500 + 600 = 1100 >= 1024, triggers new entry
    }

    #[test]
    fn test_index_builder() {
        let dir = tempdir().unwrap();
        let index_path = dir.path().join("test.index");

        let mut builder = IndexBuilder::new(1000);

        // Simulate building index from segment scan
        builder.record_batch(0, 64, 500);
        builder.record_batch(10, 600, 500);
        builder.record_batch(20, 1100, 500); // This should trigger new entry
        builder.record_batch(30, 1600, 500);

        let index = builder.build(&index_path);

        // Should have 2 entries (first + one after interval)
        assert_eq!(index.len(), 2);
        assert_eq!(index.entries()[0].offset, 0);
        assert_eq!(index.entries()[1].offset, 20);
    }

    #[test]
    fn test_index_path_generation() {
        assert_eq!(index_filename(0), "00000000000000000000.index");
        assert_eq!(index_filename(12345), "00000000000000012345.index");

        let segment_path = Path::new("/data/topics/test/partition-0/00000000000000000000.segment");
        let index_path = index_path_for_segment(segment_path);
        assert_eq!(
            index_path,
            Path::new("/data/topics/test/partition-0/00000000000000000000.index")
        );
    }

    #[test]
    fn test_index_delete() {
        let dir = tempdir().unwrap();
        let index_path = dir.path().join("test.index");

        // Create and save
        let mut index = SegmentIndex::new(&index_path);
        index.add_entry(0, 64);
        index.save().unwrap();

        assert!(index_path.exists());

        // Delete
        index.delete().unwrap();
        assert!(!index_path.exists());
    }

    #[test]
    fn test_index_max_entries_limit() {
        let dir = tempdir().unwrap();
        let index_path = dir.path().join("test.index");

        // Create index with small max_entries (minimum is 100)
        let mut index = SegmentIndex::with_config(&index_path, 1024, 100);

        // Add entries up to the limit
        for i in 0..100 {
            index.add_entry(i * 10, i as u64 * 100);
        }
        assert_eq!(index.len(), 100);

        // Try to add more - should be ignored
        index.add_entry(1000, 10000);
        assert_eq!(index.len(), 100);

        // Also test maybe_add_entry respects the limit
        index.maybe_add_entry(1010, 10100, 5000);
        assert_eq!(index.len(), 100);
    }

    #[test]
    fn test_index_builder_max_entries() {
        let dir = tempdir().unwrap();
        let index_path = dir.path().join("test.index");

        // Create builder with small max_entries (minimum is 100)
        // Note: interval_bytes has minimum of 1024
        let mut builder = IndexBuilder::with_max_entries(1024, 100);

        // Add batches until we reach max entries
        // Each batch of 2000 bytes exceeds interval (1024), so each adds an entry
        for i in 0..150 {
            builder.record_batch(i * 10, i as u64 * 100, 2000);
        }

        let index = builder.build(&index_path);

        // Should be capped at 100 entries
        assert_eq!(index.len(), 100);
    }
}
