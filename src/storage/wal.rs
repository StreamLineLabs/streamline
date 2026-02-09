//! Write-Ahead Log (WAL) implementation for Streamline
//!
//! The WAL ensures durability by writing all changes to disk before acknowledging
//! to the client. It supports crash recovery by replaying uncommitted entries.
//!
//! # Sync Modes and Durability Guarantees
//!
//! The WAL supports three sync modes, each with different durability and performance
//! trade-offs. Choose the mode that matches your requirements:
//!
//! ## `EveryWrite` - Maximum Durability (Default for critical workloads)
//!
//! Calls `fsync()` after every write operation, ensuring data is persisted to stable
//! storage before acknowledging to the client.
//!
//! - **Durability**: Survives power failure, OS crash, and hardware failure (with proper storage)
//! - **Performance**: Slowest option, ~1-5ms latency per write depending on storage
//! - **Use case**: Financial transactions, audit logs, any data that cannot be lost
//!
//! ## `Interval` - Balanced (Recommended Default)
//!
//! Batches writes and calls `fsync()` periodically (configurable via `sync_interval_ms`).
//! Data is buffered and flushed to OS page cache immediately, but only synced to disk
//! on intervals.
//!
//! - **Durability**: Data in OS page cache survives process crash but may be lost on
//!   power failure or OS crash (up to `sync_interval_ms` worth of data)
//! - **Performance**: Good throughput with bounded latency, ~100-500μs per write
//! - **Use case**: Most streaming workloads where occasional data loss is acceptable
//!
//! ## `OsDefault` - Maximum Performance (Use with caution)
//!
//! Only calls `flush()` to push data to OS page cache, relying on the OS to sync
//! to disk on its own schedule (typically 5-30 seconds).
//!
//! - **Durability**: Survives process crash, but **data loss on power failure or OS crash**
//!   can be substantial (seconds to minutes of data)
//! - **Performance**: Fastest option, sub-100μs per write
//! - **Use case**: Development, testing, or workloads where data can be reconstructed
//!
//! # Configuration Example
//!
//! ```toml
//! [wal]
//! enabled = true
//! sync_mode = "interval"     # "every_write", "interval", or "os_default"
//! sync_interval_ms = 100     # For interval mode: sync every 100ms
//! max_file_size = 67108864   # 64 MB before rotation
//! retention_files = 5        # Keep 5 old WAL files
//! ```
//!
//! # Choosing a Sync Mode
//!
//! | Requirement | Recommended Mode |
//! |-------------|------------------|
//! | Zero data loss | `every_write` |
//! | Balanced durability/performance | `interval` (100-500ms) |
//! | Development/testing | `os_default` |
//! | High throughput with bounded loss | `interval` (1000ms) |
//!
//! # Recovery Behavior
//!
//! On startup, the WAL reader scans all WAL files and replays entries that were
//! written after the last checkpoint. Entries are validated using CRC32 checksums
//! to detect corruption. Truncated entries at EOF (from incomplete writes) are
//! safely skipped with a warning.

use crate::config::WalConfig;
use crate::error::{Result, StreamlineError};
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{debug, info, warn};

/// Magic bytes for WAL file header
const WAL_MAGIC: &[u8; 4] = b"WLOG";

/// Current WAL format version
const WAL_VERSION: u16 = 1;

/// Size of WAL header in bytes
const WAL_HEADER_SIZE: usize = 16;

/// WAL entry type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum WalEntryType {
    /// Normal record write
    Record = 1,
    /// Checkpoint marker
    Checkpoint = 2,
    /// Log truncation marker
    Truncate = 3,
}

impl TryFrom<u8> for WalEntryType {
    type Error = StreamlineError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            1 => Ok(WalEntryType::Record),
            2 => Ok(WalEntryType::Checkpoint),
            3 => Ok(WalEntryType::Truncate),
            _ => Err(StreamlineError::CorruptedData(format!(
                "Invalid WAL entry type: {}",
                value
            ))),
        }
    }
}

/// A single entry in the WAL
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEntry {
    /// Monotonic sequence number
    pub sequence: u64,

    /// Write timestamp (milliseconds since epoch)
    pub timestamp: i64,

    /// Entry type
    pub entry_type: WalEntryType,

    /// Topic name
    pub topic: String,

    /// Partition ID
    pub partition: i32,

    /// Record data (key + value serialized)
    pub data: Bytes,

    /// Entry checksum
    pub crc32: u32,
}

impl WalEntry {
    /// Create a new record entry
    pub fn new_record(
        sequence: u64,
        topic: &str,
        partition: i32,
        key: Option<&Bytes>,
        value: &Bytes,
    ) -> Self {
        let timestamp = chrono::Utc::now().timestamp_millis();

        // Serialize key and value together
        let mut data = BytesMut::new();
        if let Some(k) = key {
            data.put_u32_le(k.len() as u32);
            data.put_slice(k);
        } else {
            data.put_i32_le(-1); // -1 indicates null key
        }
        data.put_u32_le(value.len() as u32);
        data.put_slice(value);

        let data = data.freeze();
        let crc32 = Self::compute_crc(
            sequence,
            timestamp,
            WalEntryType::Record,
            topic,
            partition,
            &data,
        );

        Self {
            sequence,
            timestamp,
            entry_type: WalEntryType::Record,
            topic: topic.to_string(),
            partition,
            data,
            crc32,
        }
    }

    /// Create a checkpoint entry
    pub fn new_checkpoint(sequence: u64) -> Self {
        let timestamp = chrono::Utc::now().timestamp_millis();
        let data = Bytes::new();
        let crc32 = Self::compute_crc(sequence, timestamp, WalEntryType::Checkpoint, "", 0, &data);

        Self {
            sequence,
            timestamp,
            entry_type: WalEntryType::Checkpoint,
            topic: String::new(),
            partition: 0,
            data,
            crc32,
        }
    }

    /// Compute CRC32 checksum for entry
    fn compute_crc(
        sequence: u64,
        timestamp: i64,
        entry_type: WalEntryType,
        topic: &str,
        partition: i32,
        data: &Bytes,
    ) -> u32 {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&sequence.to_le_bytes());
        hasher.update(&timestamp.to_le_bytes());
        hasher.update(&[entry_type as u8]);
        hasher.update(topic.as_bytes());
        hasher.update(&partition.to_le_bytes());
        hasher.update(data);
        hasher.finalize()
    }

    /// Verify entry checksum
    pub fn verify_crc(&self) -> bool {
        let computed = Self::compute_crc(
            self.sequence,
            self.timestamp,
            self.entry_type,
            &self.topic,
            self.partition,
            &self.data,
        );
        computed == self.crc32
    }

    /// Serialize entry to bytes
    pub fn to_bytes(&self) -> Bytes {
        let topic_bytes = self.topic.as_bytes();
        let total_size = 8 + 8 + 1 + 2 + topic_bytes.len() + 4 + 4 + self.data.len() + 4;

        let mut buf = BytesMut::with_capacity(total_size);

        // Sequence (8 bytes)
        buf.put_u64_le(self.sequence);

        // Timestamp (8 bytes)
        buf.put_i64_le(self.timestamp);

        // Entry type (1 byte)
        buf.put_u8(self.entry_type as u8);

        // Topic length and topic (2 + N bytes)
        buf.put_u16_le(topic_bytes.len() as u16);
        buf.put_slice(topic_bytes);

        // Partition (4 bytes)
        buf.put_i32_le(self.partition);

        // Data length and data (4 + N bytes)
        buf.put_u32_le(self.data.len() as u32);
        buf.put_slice(&self.data);

        // CRC32 (4 bytes)
        buf.put_u32_le(self.crc32);

        buf.freeze()
    }

    /// Deserialize entry from bytes
    pub fn from_bytes(data: &[u8]) -> Result<(Self, usize)> {
        // Minimum size: 8(seq) + 8(ts) + 1(type) + 2(topic_len) + 4(partition) + 4(data_len) + 4(crc) = 31
        const MIN_ENTRY_SIZE: usize = 31;
        if data.len() < MIN_ENTRY_SIZE {
            return Err(StreamlineError::CorruptedData(
                "WAL entry too short".to_string(),
            ));
        }

        let mut pos = 0;

        // Sequence
        let sequence = u64::from_le_bytes(data[pos..pos + 8].try_into().map_err(|_| {
            StreamlineError::CorruptedData("WAL entry sequence corrupted".to_string())
        })?);
        pos += 8;

        // Timestamp
        let timestamp = i64::from_le_bytes(data[pos..pos + 8].try_into().map_err(|_| {
            StreamlineError::CorruptedData("WAL entry timestamp corrupted".to_string())
        })?);
        pos += 8;

        // Entry type
        let entry_type = WalEntryType::try_from(data[pos])?;
        pos += 1;

        // Topic
        if pos + 2 > data.len() {
            return Err(StreamlineError::CorruptedData(
                "WAL entry topic length truncated".to_string(),
            ));
        }
        let topic_len = u16::from_le_bytes(data[pos..pos + 2].try_into().map_err(|_| {
            StreamlineError::CorruptedData("WAL entry topic length corrupted".to_string())
        })?) as usize;
        pos += 2;

        if pos + topic_len > data.len() {
            return Err(StreamlineError::CorruptedData(
                "WAL entry topic truncated".to_string(),
            ));
        }
        let topic = String::from_utf8_lossy(&data[pos..pos + topic_len]).to_string();
        pos += topic_len;

        // Partition
        if pos + 4 > data.len() {
            return Err(StreamlineError::CorruptedData(
                "WAL entry partition truncated".to_string(),
            ));
        }
        let partition = i32::from_le_bytes(data[pos..pos + 4].try_into().map_err(|_| {
            StreamlineError::CorruptedData("WAL entry partition corrupted".to_string())
        })?);
        pos += 4;

        // Data
        if pos + 4 > data.len() {
            return Err(StreamlineError::CorruptedData(
                "WAL entry data length truncated".to_string(),
            ));
        }
        let data_len = u32::from_le_bytes(data[pos..pos + 4].try_into().map_err(|_| {
            StreamlineError::CorruptedData("WAL entry data length corrupted".to_string())
        })?) as usize;
        pos += 4;

        if pos + data_len > data.len() {
            return Err(StreamlineError::CorruptedData(
                "WAL entry data truncated".to_string(),
            ));
        }
        let entry_data = Bytes::copy_from_slice(&data[pos..pos + data_len]);
        pos += data_len;

        // CRC32
        if pos + 4 > data.len() {
            return Err(StreamlineError::CorruptedData(
                "WAL entry CRC truncated".to_string(),
            ));
        }
        let crc32 =
            u32::from_le_bytes(data[pos..pos + 4].try_into().map_err(|_| {
                StreamlineError::CorruptedData("WAL entry CRC corrupted".to_string())
            })?);
        pos += 4;

        let entry = Self {
            sequence,
            timestamp,
            entry_type,
            topic,
            partition,
            data: entry_data,
            crc32,
        };

        if !entry.verify_crc() {
            return Err(StreamlineError::CorruptedData(
                "WAL entry CRC mismatch".to_string(),
            ));
        }

        Ok((entry, pos))
    }

    /// Extract key and value from record entry data
    pub fn extract_key_value(&self) -> Result<(Option<Bytes>, Bytes)> {
        if self.data.len() < 4 {
            return Err(StreamlineError::CorruptedData(
                "WAL entry data too short".to_string(),
            ));
        }

        let mut pos = 0;

        // Read key
        let key_len = i32::from_le_bytes(self.data[pos..pos + 4].try_into().map_err(|_| {
            StreamlineError::CorruptedData("WAL entry key length corrupted".to_string())
        })?);
        pos += 4;

        let key = if key_len >= 0 {
            let len = key_len as usize;
            if pos + len > self.data.len() {
                return Err(StreamlineError::CorruptedData(
                    "WAL entry key truncated".to_string(),
                ));
            }
            let key = Bytes::copy_from_slice(&self.data[pos..pos + len]);
            pos += len;
            Some(key)
        } else {
            None
        };

        // Read value
        if pos + 4 > self.data.len() {
            return Err(StreamlineError::CorruptedData(
                "WAL entry value length truncated".to_string(),
            ));
        }
        let value_len = u32::from_le_bytes(self.data[pos..pos + 4].try_into().map_err(|_| {
            StreamlineError::CorruptedData("WAL entry value length corrupted".to_string())
        })?) as usize;
        pos += 4;

        if pos + value_len > self.data.len() {
            return Err(StreamlineError::CorruptedData(
                "WAL entry value truncated".to_string(),
            ));
        }
        let value = Bytes::copy_from_slice(&self.data[pos..pos + value_len]);

        Ok((key, value))
    }
}

/// WAL file header
struct WalHeader {
    version: u16,
    flags: u16,
    created_at: i64,
}

impl WalHeader {
    fn new() -> Self {
        Self {
            version: WAL_VERSION,
            flags: 0,
            created_at: chrono::Utc::now().timestamp_millis(),
        }
    }

    fn to_bytes(&self) -> [u8; WAL_HEADER_SIZE] {
        let mut buf = [0u8; WAL_HEADER_SIZE];
        buf[0..4].copy_from_slice(WAL_MAGIC);
        buf[4..6].copy_from_slice(&self.version.to_le_bytes());
        buf[6..8].copy_from_slice(&self.flags.to_le_bytes());
        buf[8..16].copy_from_slice(&self.created_at.to_le_bytes());
        buf
    }

    fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < WAL_HEADER_SIZE {
            return Err(StreamlineError::CorruptedData(
                "WAL header too short".to_string(),
            ));
        }

        if &data[0..4] != WAL_MAGIC {
            return Err(StreamlineError::CorruptedData(
                "Invalid WAL magic".to_string(),
            ));
        }

        // Safe: data length validated above, slice is exactly 8 bytes
        let created_at_bytes: [u8; 8] = data[8..16]
            .try_into()
            .map_err(|_| StreamlineError::CorruptedData("WAL header malformed".to_string()))?;

        Ok(Self {
            version: u16::from_le_bytes([data[4], data[5]]),
            flags: u16::from_le_bytes([data[6], data[7]]),
            created_at: i64::from_le_bytes(created_at_bytes),
        })
    }
}

/// WAL sync mode - controls durability vs performance trade-off.
///
/// See module-level documentation for detailed comparison of modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMode {
    /// Fsync every N milliseconds (batched for performance).
    ///
    /// Recommended default. Provides good balance of durability and performance.
    /// Data may be lost up to `sync_interval_ms` on power failure.
    Interval,

    /// Fsync after every produce request (maximum durability).
    ///
    /// Use for critical workloads where no data loss is acceptable.
    /// Highest latency but strongest durability guarantee.
    EveryWrite,

    /// Rely on OS buffer flushing (maximum performance).
    ///
    /// **WARNING**: Significant data loss possible on power failure or OS crash.
    /// Only use for development, testing, or reconstructible data.
    OsDefault,
}

impl SyncMode {
    fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "every_write" => SyncMode::EveryWrite,
            "os_default" => SyncMode::OsDefault,
            _ => SyncMode::Interval,
        }
    }
}

/// WAL writer for appending entries
pub struct WalWriter {
    /// WAL directory path
    wal_dir: PathBuf,

    /// Configuration
    config: WalConfig,

    /// Current WAL file writer
    writer: Option<BufWriter<File>>,

    /// Current WAL file path
    current_path: PathBuf,

    /// Current file size
    current_size: u64,

    /// Next sequence number
    next_sequence: AtomicU64,

    /// Sync mode
    sync_mode: SyncMode,

    /// Whether WAL is enabled
    enabled: bool,

    /// Number of pending writes (not yet synced to disk)
    /// Used for backpressure to prevent unbounded memory growth
    pending_writes: usize,
}

impl WalWriter {
    /// Create a new WAL writer
    pub fn new(data_dir: &Path, config: WalConfig) -> Result<Self> {
        let wal_dir = data_dir.join("wal");
        fs::create_dir_all(&wal_dir)?;

        let sync_mode = SyncMode::from_str(&config.sync_mode);
        let enabled = config.enabled;

        let mut writer = Self {
            wal_dir: wal_dir.clone(),
            config,
            writer: None,
            current_path: wal_dir.join("current.wal"),
            current_size: 0,
            next_sequence: AtomicU64::new(0),
            sync_mode,
            enabled,
            pending_writes: 0,
        };

        if enabled {
            // Find max sequence from existing WAL files
            if let Some(max_seq) = writer.find_max_sequence()? {
                writer.next_sequence.store(max_seq + 1, Ordering::SeqCst);
            }
            // If no existing entries, next_sequence stays at 0

            // Open or create current WAL file
            writer.open_current_wal()?;
        }

        info!(
            wal_dir = %wal_dir.display(),
            enabled = enabled,
            sync_mode = ?sync_mode,
            "WAL initialized"
        );

        Ok(writer)
    }

    /// Create a new WAL writer with the specified directory used directly (no subdirectory)
    ///
    /// This is used by ShardedWalWriter where each shard directory should contain
    /// WAL files directly without creating a nested `wal` subdirectory.
    pub fn with_wal_dir(wal_dir: &Path, config: WalConfig) -> Result<Self> {
        fs::create_dir_all(wal_dir)?;

        let sync_mode = SyncMode::from_str(&config.sync_mode);
        let enabled = config.enabled;

        let mut writer = Self {
            wal_dir: wal_dir.to_path_buf(),
            config,
            writer: None,
            current_path: wal_dir.join("current.wal"),
            current_size: 0,
            next_sequence: AtomicU64::new(0),
            sync_mode,
            enabled,
            pending_writes: 0,
        };

        if enabled {
            // Find max sequence from existing WAL files
            if let Some(max_seq) = writer.find_max_sequence()? {
                writer.next_sequence.store(max_seq + 1, Ordering::SeqCst);
            }

            // Open or create current WAL file
            writer.open_current_wal()?;
        }

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

    /// Find maximum sequence number from existing WAL files
    fn find_max_sequence(&self) -> Result<Option<u64>> {
        let mut max_seq: Option<u64> = None;

        // Check current WAL
        if self.current_path.exists() {
            if let Ok(entries) = WalReader::read_file(&self.current_path) {
                if let Some(last) = entries.last() {
                    max_seq = Some(max_seq.unwrap_or(0).max(last.sequence));
                }
            }
        }

        // Check archived WAL files
        if let Ok(entries) = fs::read_dir(&self.wal_dir) {
            for entry in entries.filter_map(|e| e.ok()) {
                let path = entry.path();
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
    fn open_current_wal(&mut self) -> Result<()> {
        if self.current_path.exists() {
            // Open existing file for append
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&self.current_path)?;

            self.current_size = file.metadata()?.len();
            let mut file = file;
            file.seek(SeekFrom::End(0))?;
            self.writer = Some(BufWriter::new(file));
        } else {
            // Create new file
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&self.current_path)?;

            let mut writer = BufWriter::new(file);
            let header = WalHeader::new();
            writer.write_all(&header.to_bytes())?;
            writer.flush()?;

            self.current_size = WAL_HEADER_SIZE as u64;
            self.writer = Some(writer);
        }

        Ok(())
    }

    /// Append an entry to the WAL
    ///
    /// Returns the entry sequence number on success, or an error if:
    /// - WAL writer is not initialized
    /// - The pending writes limit has been reached (backpressure)
    /// - An I/O error occurs
    ///
    /// When backpressure is applied (pending writes >= max_pending_writes),
    /// callers should retry after a short delay or call `sync()` to drain
    /// the pending writes.
    pub fn append(&mut self, entry: &WalEntry) -> Result<u64> {
        if !self.enabled {
            return Ok(entry.sequence);
        }

        // Check backpressure limit
        if self.pending_writes >= self.config.max_pending_writes {
            return Err(StreamlineError::storage_msg(format!(
                "WAL backpressure: {} pending writes exceeds limit of {}. \
                 Call sync() or wait for interval sync to drain the queue.",
                self.pending_writes, self.config.max_pending_writes
            )));
        }

        // Check if we need to rotate
        if self.current_size >= self.config.max_file_size {
            self.rotate()?;
        }

        let writer = self.writer.as_mut().ok_or_else(|| {
            StreamlineError::storage_msg("WAL writer not initialized".to_string())
        })?;

        // Serialize and write entry
        let entry_bytes = entry.to_bytes();
        let entry_len = entry_bytes.len() as u32;

        // Write entry length prefix (4 bytes)
        writer.write_all(&entry_len.to_le_bytes())?;

        // Write entry
        writer.write_all(&entry_bytes)?;

        // Sync based on mode
        match self.sync_mode {
            SyncMode::EveryWrite => {
                writer.flush()?;
                writer.get_ref().sync_data()?;
                // Reset pending writes after sync
                self.pending_writes = 0;
            }
            SyncMode::Interval | SyncMode::OsDefault => {
                writer.flush()?;
                // Increment pending writes (will be reset on next sync)
                self.pending_writes += 1;
            }
        }

        self.current_size += 4 + entry_bytes.len() as u64;

        debug!(
            sequence = entry.sequence,
            entry_type = ?entry.entry_type,
            pending_writes = self.pending_writes,
            "WAL entry written"
        );

        Ok(entry.sequence)
    }

    /// Append a record entry to the WAL
    pub fn append_record(
        &mut self,
        topic: &str,
        partition: i32,
        key: Option<&Bytes>,
        value: &Bytes,
    ) -> Result<u64> {
        let sequence = self.next_sequence.fetch_add(1, Ordering::SeqCst);
        let entry = WalEntry::new_record(sequence, topic, partition, key, value);
        self.append(&entry)
    }

    /// Write a checkpoint entry
    pub fn checkpoint(&mut self) -> Result<u64> {
        let sequence = self.next_sequence.fetch_add(1, Ordering::SeqCst);
        let entry = WalEntry::new_checkpoint(sequence);
        self.append(&entry)?;

        // Force sync on checkpoint
        if let Some(writer) = self.writer.as_mut() {
            writer.flush()?;
            writer.get_ref().sync_all()?;
        }
        // Reset pending writes counter after checkpoint sync
        self.pending_writes = 0;

        Ok(sequence)
    }

    /// Sync WAL to disk (for interval sync mode)
    ///
    /// This resets the pending writes counter, allowing new writes to proceed
    /// if backpressure was previously applied.
    pub fn sync(&mut self) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        if let Some(writer) = self.writer.as_mut() {
            writer.flush()?;
            writer.get_ref().sync_data()?;
        }
        // Reset pending writes counter after successful sync
        self.pending_writes = 0;
        Ok(())
    }

    /// Rotate WAL file
    fn rotate(&mut self) -> Result<()> {
        if let Some(mut writer) = self.writer.take() {
            writer.flush()?;
            writer.get_ref().sync_all()?;
        }
        // Reset pending writes after rotation sync
        self.pending_writes = 0;

        // Archive current file
        let timestamp = chrono::Utc::now().timestamp_millis();
        let archived_name = format!("{:020}.wal", timestamp);
        let archived_path = self.wal_dir.join(&archived_name);

        fs::rename(&self.current_path, &archived_path)?;

        // Sync the parent directory to ensure rename is durable
        // This is CRITICAL for crash consistency on Linux/POSIX - directory fsync is REQUIRED
        // to persist file renames. Without this, the rename could be lost on power failure.
        let dir = fs::File::open(&self.wal_dir).map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to open WAL directory for sync: {}", e))
        })?;
        dir.sync_all().map_err(|e| {
            StreamlineError::storage_msg(format!(
                "Failed to sync WAL directory - rename durability not guaranteed: {}",
                e
            ))
        })?;

        info!(
            archived = %archived_path.display(),
            "WAL file rotated"
        );

        // Create new current file
        self.open_current_wal()?;

        // Clean up old files
        self.cleanup_old_files()?;

        Ok(())
    }

    /// Clean up old WAL files beyond retention limit
    fn cleanup_old_files(&self) -> Result<()> {
        let mut wal_files: Vec<PathBuf> = fs::read_dir(&self.wal_dir)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                p.extension().map(|e| e == "wal").unwrap_or(false) && *p != self.current_path
            })
            .collect();

        // Sort by name (which is timestamp-based)
        wal_files.sort();

        // Remove oldest files if over retention limit
        let retention = self.config.retention_files as usize;
        while wal_files.len() > retention {
            if let Some(oldest) = wal_files.first().cloned() {
                fs::remove_file(&oldest)?;
                info!(file = %oldest.display(), "Old WAL file removed");
                wal_files.remove(0);
            }
        }

        Ok(())
    }

    /// Flush and close the WAL
    pub fn close(&mut self) -> Result<()> {
        if let Some(mut writer) = self.writer.take() {
            writer.flush()?;
            writer.get_ref().sync_all()?;
        }
        Ok(())
    }
}

impl Drop for WalWriter {
    fn drop(&mut self) {
        if let Err(e) = self.close() {
            warn!(error = %e, "Failed to close WAL writer");
        }
    }
}

/// Statistics about WAL recovery
#[derive(Debug, Clone, Default)]
pub struct WalRecoveryStats {
    /// Number of records successfully recovered
    pub records_recovered: usize,

    /// Number of records lost due to corruption or truncation
    pub records_lost: usize,

    /// Number of bytes truncated from the end of the WAL
    pub bytes_truncated: u64,

    /// Number of corrupted entries encountered
    pub corrupted_entries: usize,

    /// Number of truncated entries at EOF
    pub truncated_entries: usize,
}

impl WalRecoveryStats {
    /// Create a new empty recovery stats
    pub fn new() -> Self {
        Self::default()
    }

    /// Check if recovery encountered any issues
    pub fn has_issues(&self) -> bool {
        self.records_lost > 0
            || self.bytes_truncated > 0
            || self.corrupted_entries > 0
            || self.truncated_entries > 0
    }
}

/// WAL reader for recovery
pub struct WalReader;

impl WalReader {
    /// Read all entries from a WAL file
    pub fn read_file(path: &Path) -> Result<Vec<WalEntry>> {
        let (entries, _stats) = Self::read_file_with_stats(path)?;
        Ok(entries)
    }

    /// Read all entries from a WAL file with recovery statistics
    pub fn read_file_with_stats(path: &Path) -> Result<(Vec<WalEntry>, WalRecoveryStats)> {
        let file_size = std::fs::metadata(path)?.len();
        let mut file = BufReader::new(File::open(path)?);

        // Read and verify header
        let mut header_buf = [0u8; WAL_HEADER_SIZE];
        file.read_exact(&mut header_buf)?;
        let _header = WalHeader::from_bytes(&header_buf)?;

        let mut entries = Vec::new();
        let mut stats = WalRecoveryStats::new();
        let mut position = WAL_HEADER_SIZE as u64;

        loop {
            // Read entry length
            let mut len_buf = [0u8; 4];
            if file.read_exact(&mut len_buf).is_err() {
                // Normal EOF
                break;
            }
            let entry_len = u32::from_le_bytes(len_buf) as usize;
            position += 4;

            // Read entry data
            let mut entry_buf = vec![0u8; entry_len];
            match file.read_exact(&mut entry_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    warn!(
                        file = %path.display(),
                        position = position,
                        expected_bytes = entry_len,
                        "Truncated WAL entry at end of file, ignoring remaining data"
                    );
                    stats.truncated_entries += 1;
                    stats.bytes_truncated = file_size - position;
                    // Estimate records lost (assume 1 record per entry)
                    stats.records_lost += 1;
                    break;
                }
                Err(e) => return Err(e.into()),
            }
            position += entry_len as u64;

            // Parse entry
            match WalEntry::from_bytes(&entry_buf) {
                Ok((entry, _)) => {
                    entries.push(entry);
                    stats.records_recovered += 1;
                }
                Err(e) => {
                    warn!(
                        file = %path.display(),
                        position = position - entry_len as u64,
                        error = %e,
                        "Corrupted WAL entry, stopping read"
                    );
                    stats.corrupted_entries += 1;
                    stats.bytes_truncated = file_size - (position - entry_len as u64);
                    stats.records_lost += 1;
                    break;
                }
            }
        }

        Ok((entries, stats))
    }

    /// Read all entries from all WAL files in a directory
    pub fn read_all(wal_dir: &Path) -> Result<Vec<WalEntry>> {
        let (entries, _stats) = Self::read_all_with_stats(wal_dir)?;
        Ok(entries)
    }

    /// Read all entries from all WAL files in a directory with recovery statistics
    pub fn read_all_with_stats(wal_dir: &Path) -> Result<(Vec<WalEntry>, WalRecoveryStats)> {
        let mut all_entries = Vec::new();
        let mut combined_stats = WalRecoveryStats::new();

        // Get all WAL files sorted by name
        let mut wal_files: Vec<PathBuf> = fs::read_dir(wal_dir)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map(|e| e == "wal").unwrap_or(false))
            .collect();

        wal_files.sort();

        // Read entries from each file
        for path in wal_files {
            match Self::read_file_with_stats(&path) {
                Ok((entries, stats)) => {
                    all_entries.extend(entries);
                    // Accumulate stats
                    combined_stats.records_recovered += stats.records_recovered;
                    combined_stats.records_lost += stats.records_lost;
                    combined_stats.bytes_truncated += stats.bytes_truncated;
                    combined_stats.corrupted_entries += stats.corrupted_entries;
                    combined_stats.truncated_entries += stats.truncated_entries;
                }
                Err(e) => {
                    warn!(file = %path.display(), error = %e, "Failed to read WAL file");
                }
            }
        }

        // Sort by sequence number
        all_entries.sort_by_key(|e| e.sequence);

        // Log recovery summary if there were issues
        if combined_stats.has_issues() {
            warn!(
                records_recovered = combined_stats.records_recovered,
                records_lost = combined_stats.records_lost,
                bytes_truncated = combined_stats.bytes_truncated,
                corrupted_entries = combined_stats.corrupted_entries,
                truncated_entries = combined_stats.truncated_entries,
                "WAL recovery completed with issues"
            );
        } else {
            info!(
                records_recovered = combined_stats.records_recovered,
                "WAL recovery completed successfully"
            );
        }

        Ok((all_entries, combined_stats))
    }

    /// Read entries after a given sequence number
    pub fn read_after_sequence(wal_dir: &Path, after_sequence: u64) -> Result<Vec<WalEntry>> {
        let all_entries = Self::read_all(wal_dir)?;
        Ok(all_entries
            .into_iter()
            .filter(|e| e.sequence > after_sequence)
            .collect())
    }
}

// =============================================================================
// Sharded WAL Support
// =============================================================================

/// Strategy for selecting which shard handles a given topic/partition
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardStrategy {
    /// Hash by topic name - all partitions of a topic go to the same shard
    Topic,
    /// Hash by topic + partition - distributes partitions across shards
    Partition,
}

impl ShardStrategy {
    fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "partition" => ShardStrategy::Partition,
            _ => ShardStrategy::Topic,
        }
    }
}

/// Selects which WAL shard should handle a given topic/partition
#[derive(Debug)]
pub struct ShardSelector {
    shard_count: u32,
    strategy: ShardStrategy,
}

impl ShardSelector {
    /// Create a new shard selector
    pub fn new(shard_count: u32, strategy: ShardStrategy) -> Self {
        Self {
            shard_count,
            strategy,
        }
    }

    /// Select the shard index for a given topic and partition
    ///
    /// Uses FNV-1a hash for fast, well-distributed hashing
    pub fn select(&self, topic: &str, partition: i32) -> usize {
        let hash = match self.strategy {
            ShardStrategy::Topic => {
                // Hash only the topic name
                fnv_hash(topic.as_bytes())
            }
            ShardStrategy::Partition => {
                // Hash topic + partition for finer distribution
                let mut data = topic.as_bytes().to_vec();
                data.extend_from_slice(&partition.to_le_bytes());
                fnv_hash(&data)
            }
        };
        (hash % self.shard_count as u64) as usize
    }
}

/// FNV-1a hash function (fast, good distribution for string keys)
fn fnv_hash(data: &[u8]) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;

    let mut hash = FNV_OFFSET;
    for byte in data {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// Sharded WAL writer for improved throughput
///
/// Distributes writes across multiple WAL shards to eliminate the single-WAL
/// bottleneck. Each shard has its own file and lock, allowing parallel writes
/// to different topics/partitions.
///
/// # Architecture
///
/// ```text
/// data/wal/
/// ├── shard-00/
/// │   ├── current.wal
/// │   └── *.wal (archived)
/// ├── shard-01/
/// │   └── current.wal
/// ├── shard-02/
/// │   └── current.wal
/// └── shard-03/
///     └── current.wal
/// ```
///
/// # Thread Safety
///
/// Each shard is protected by its own mutex, so concurrent writes to different
/// shards do not contend. The global sequence number is an AtomicU64 for
/// lock-free ordering across shards.
pub struct ShardedWalWriter {
    /// Individual WAL writers for each shard
    shards: Vec<parking_lot::Mutex<WalWriter>>,

    /// Shard selector for routing writes
    selector: ShardSelector,

    /// Global sequence counter (atomic for lock-free access)
    global_sequence: AtomicU64,

    /// Configuration
    config: WalConfig,

    /// Base WAL directory
    wal_dir: PathBuf,
}

impl ShardedWalWriter {
    /// Create a new sharded WAL writer
    ///
    /// Creates `shard_count` subdirectories under `data_dir/wal/` and
    /// initializes a WAL writer for each shard.
    pub fn new(data_dir: &Path, config: WalConfig) -> Result<Self> {
        let shard_count = config.effective_shard_count();
        let strategy = ShardStrategy::from_str(&config.shard_strategy);
        let selector = ShardSelector::new(shard_count, strategy);

        let wal_dir = data_dir.join("wal");
        fs::create_dir_all(&wal_dir)?;

        let mut shards = Vec::with_capacity(shard_count as usize);
        let mut max_sequence: u64 = 0;

        // Create or open each shard
        for shard_id in 0..shard_count {
            let shard_dir = wal_dir.join(format!("shard-{:02}", shard_id));

            // Use with_wal_dir to create WalWriter directly in the shard directory
            // (without creating a nested `wal` subdirectory)
            let shard_config = config.clone();
            let writer = WalWriter::with_wal_dir(&shard_dir, shard_config)?;

            // Track the maximum sequence across all shards
            max_sequence = max_sequence.max(writer.next_sequence());

            shards.push(parking_lot::Mutex::new(writer));
        }

        info!(
            shard_count = shard_count,
            strategy = ?strategy,
            max_sequence = max_sequence,
            "Sharded WAL initialized"
        );

        Ok(Self {
            shards,
            selector,
            global_sequence: AtomicU64::new(max_sequence),
            config,
            wal_dir,
        })
    }

    /// Check if WAL is enabled
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Get the next global sequence number
    pub fn next_sequence(&self) -> u64 {
        self.global_sequence.load(Ordering::SeqCst)
    }

    /// Get the number of shards
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Append a record to the appropriate shard
    ///
    /// The shard is selected based on the topic/partition and the configured
    /// strategy. A global sequence number is assigned before the write.
    pub fn append_record(
        &self,
        topic: &str,
        partition: i32,
        key: Option<&Bytes>,
        value: &Bytes,
    ) -> Result<u64> {
        if !self.config.enabled {
            return Ok(0);
        }

        // Get global sequence number first (before acquiring shard lock)
        let sequence = self.global_sequence.fetch_add(1, Ordering::SeqCst);

        // Select shard and write
        let shard_idx = self.selector.select(topic, partition);
        let entry = WalEntry::new_record(sequence, topic, partition, key, value);

        let mut shard = self.shards[shard_idx].lock();
        shard.append(&entry)?;

        debug!(
            sequence = sequence,
            shard = shard_idx,
            topic = topic,
            partition = partition,
            "Sharded WAL record appended"
        );

        Ok(sequence)
    }

    /// Write a checkpoint to all shards
    ///
    /// Checkpoints are written to all shards to ensure recovery can find the
    /// latest checkpoint regardless of which shard is read first.
    pub fn checkpoint(&self) -> Result<u64> {
        let sequence = self.global_sequence.fetch_add(1, Ordering::SeqCst);
        let entry = WalEntry::new_checkpoint(sequence);

        // Write checkpoint to all shards
        for (idx, shard) in self.shards.iter().enumerate() {
            let mut writer = shard.lock();
            if let Err(e) = writer.append(&entry) {
                warn!(shard = idx, error = %e, "Failed to write checkpoint to shard");
            }
            // Force sync
            if let Err(e) = writer.sync() {
                warn!(shard = idx, error = %e, "Failed to sync checkpoint on shard");
            }
        }

        debug!(sequence = sequence, "Checkpoint written to all shards");
        Ok(sequence)
    }

    /// Sync all shards to disk
    pub fn sync(&self) -> Result<()> {
        for (idx, shard) in self.shards.iter().enumerate() {
            if let Err(e) = shard.lock().sync() {
                warn!(shard = idx, error = %e, "Failed to sync shard");
            }
        }
        Ok(())
    }

    /// Close all shard writers
    pub fn close(&self) -> Result<()> {
        for (idx, shard) in self.shards.iter().enumerate() {
            if let Err(e) = shard.lock().close() {
                warn!(shard = idx, error = %e, "Failed to close shard");
            }
        }
        Ok(())
    }

    /// Get the WAL directory path
    pub fn wal_dir(&self) -> &Path {
        &self.wal_dir
    }
}

/// Sharded WAL reader for recovery
///
/// Reads entries from all shards and merges them in sequence order.
pub struct ShardedWalReader;

impl ShardedWalReader {
    /// Read all entries from all shards, merged and sorted by sequence
    pub fn read_all(wal_dir: &Path) -> Result<Vec<WalEntry>> {
        let (entries, _stats) = Self::read_all_with_stats(wal_dir)?;
        Ok(entries)
    }

    /// Read all entries from all shards with recovery statistics
    pub fn read_all_with_stats(wal_dir: &Path) -> Result<(Vec<WalEntry>, WalRecoveryStats)> {
        let mut all_entries = Vec::new();
        let mut combined_stats = WalRecoveryStats::new();

        // Check if this is a sharded WAL (has shard-XX directories)
        let has_shards = fs::read_dir(wal_dir)?
            .filter_map(|e| e.ok())
            .any(|e| e.file_name().to_string_lossy().starts_with("shard-"));

        if has_shards {
            // Read from each shard directory
            for entry in fs::read_dir(wal_dir)?.filter_map(|e| e.ok()) {
                let name = entry.file_name();
                if name.to_string_lossy().starts_with("shard-") {
                    let shard_path = entry.path();
                    if shard_path.is_dir() {
                        match WalReader::read_all_with_stats(&shard_path) {
                            Ok((entries, stats)) => {
                                all_entries.extend(entries);
                                // Accumulate stats
                                combined_stats.records_recovered += stats.records_recovered;
                                combined_stats.records_lost += stats.records_lost;
                                combined_stats.bytes_truncated += stats.bytes_truncated;
                                combined_stats.corrupted_entries += stats.corrupted_entries;
                                combined_stats.truncated_entries += stats.truncated_entries;
                            }
                            Err(e) => {
                                warn!(
                                    shard = %shard_path.display(),
                                    error = %e,
                                    "Failed to read shard"
                                );
                            }
                        }
                    }
                }
            }
        } else {
            // Legacy non-sharded WAL - read directly
            let (entries, stats) = WalReader::read_all_with_stats(wal_dir)?;
            all_entries = entries;
            combined_stats = stats;
        }

        // Sort by sequence number to merge entries from different shards
        all_entries.sort_by_key(|e| e.sequence);

        Ok((all_entries, combined_stats))
    }

    /// Read entries after a given sequence number
    pub fn read_after_sequence(wal_dir: &Path, after_sequence: u64) -> Result<Vec<WalEntry>> {
        let all_entries = Self::read_all(wal_dir)?;
        Ok(all_entries
            .into_iter()
            .filter(|e| e.sequence > after_sequence)
            .collect())
    }

    /// Detect if a WAL directory contains a sharded or legacy layout
    pub fn is_sharded(wal_dir: &Path) -> bool {
        if !wal_dir.exists() {
            return false;
        }

        fs::read_dir(wal_dir)
            .map(|entries| {
                entries
                    .filter_map(|e| e.ok())
                    .any(|e| e.file_name().to_string_lossy().starts_with("shard-"))
            })
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_wal_entry_serialization() {
        let entry = WalEntry::new_record(
            1,
            "test-topic",
            0,
            Some(&Bytes::from("key")),
            &Bytes::from("value"),
        );

        let bytes = entry.to_bytes();
        let (parsed, _) = WalEntry::from_bytes(&bytes).unwrap();

        assert_eq!(parsed.sequence, 1);
        assert_eq!(parsed.topic, "test-topic");
        assert_eq!(parsed.partition, 0);
        assert!(parsed.verify_crc());
    }

    #[test]
    fn test_wal_entry_extract_key_value() {
        let entry = WalEntry::new_record(
            1,
            "test-topic",
            0,
            Some(&Bytes::from("my-key")),
            &Bytes::from("my-value"),
        );

        let (key, value) = entry.extract_key_value().unwrap();
        assert_eq!(key, Some(Bytes::from("my-key")));
        assert_eq!(value, Bytes::from("my-value"));
    }

    #[test]
    fn test_wal_entry_null_key() {
        let entry = WalEntry::new_record(1, "test-topic", 0, None, &Bytes::from("value"));

        let (key, value) = entry.extract_key_value().unwrap();
        assert!(key.is_none());
        assert_eq!(value, Bytes::from("value"));
    }

    #[test]
    fn test_wal_writer_basic() {
        let dir = tempdir().unwrap();
        let config = WalConfig {
            enabled: true,
            sync_interval_ms: 100,
            sync_mode: "interval".to_string(),
            max_file_size: 1024 * 1024,
            retention_files: 5,
            max_pending_writes: 1000,
            ..Default::default()
        };

        let mut writer = WalWriter::new(dir.path(), config).unwrap();

        // Write some entries
        let seq1 = writer
            .append_record("topic1", 0, Some(&Bytes::from("k1")), &Bytes::from("v1"))
            .unwrap();
        let seq2 = writer
            .append_record("topic1", 0, None, &Bytes::from("v2"))
            .unwrap();

        assert_eq!(seq1, 0);
        assert_eq!(seq2, 1);

        // Close and read back
        writer.close().unwrap();

        let wal_dir = dir.path().join("wal");
        let entries = WalReader::read_all(&wal_dir).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].sequence, 0);
        assert_eq!(entries[1].sequence, 1);
    }

    #[test]
    fn test_wal_disabled() {
        let dir = tempdir().unwrap();
        let config = WalConfig {
            enabled: false,
            sync_interval_ms: 100,
            sync_mode: "interval".to_string(),
            max_file_size: 1024 * 1024,
            retention_files: 5,
            max_pending_writes: 1000,
            ..Default::default()
        };

        let mut writer = WalWriter::new(dir.path(), config).unwrap();
        assert!(!writer.is_enabled());

        // Should not write anything
        writer
            .append_record("topic1", 0, None, &Bytes::from("v1"))
            .unwrap();
        writer.close().unwrap();

        // WAL directory should be empty or have no entries
        let wal_dir = dir.path().join("wal");
        let entries = WalReader::read_all(&wal_dir).unwrap_or_default();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_wal_checkpoint() {
        let dir = tempdir().unwrap();
        let config = WalConfig::default();

        let mut writer = WalWriter::new(dir.path(), config).unwrap();

        writer
            .append_record("topic1", 0, None, &Bytes::from("v1"))
            .unwrap();
        let checkpoint_seq = writer.checkpoint().unwrap();
        writer
            .append_record("topic1", 0, None, &Bytes::from("v2"))
            .unwrap();

        writer.close().unwrap();

        let wal_dir = dir.path().join("wal");
        let entries = WalReader::read_all(&wal_dir).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[1].entry_type, WalEntryType::Checkpoint);
        assert_eq!(entries[1].sequence, checkpoint_seq);
    }

    #[test]
    fn test_wal_rotation() {
        let dir = tempdir().unwrap();
        let config = WalConfig {
            enabled: true,
            sync_interval_ms: 100,
            sync_mode: "interval".to_string(),
            max_file_size: 100, // Very small for testing
            retention_files: 2,
            max_pending_writes: 1000,
            ..Default::default()
        };

        let mut writer = WalWriter::new(dir.path(), config).unwrap();

        // Write enough to trigger rotation
        for i in 0..10 {
            writer
                .append_record("topic1", 0, None, &Bytes::from(format!("value-{}", i)))
                .unwrap();
        }

        writer.close().unwrap();

        // Check that files were created
        let wal_dir = dir.path().join("wal");
        let wal_files: Vec<_> = fs::read_dir(&wal_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .map(|ext| ext == "wal")
                    .unwrap_or(false)
            })
            .collect();

        // Should have current + some archived files (limited by retention)
        assert!(!wal_files.is_empty());
    }

    #[test]
    fn test_wal_entry_type_conversion() {
        assert_eq!(WalEntryType::try_from(1).unwrap(), WalEntryType::Record);
        assert_eq!(WalEntryType::try_from(2).unwrap(), WalEntryType::Checkpoint);
        assert_eq!(WalEntryType::try_from(3).unwrap(), WalEntryType::Truncate);
        assert!(WalEntryType::try_from(99).is_err());
    }

    #[test]
    fn test_wal_backpressure() {
        let dir = tempdir().unwrap();
        let config = WalConfig {
            enabled: true,
            sync_interval_ms: 100,
            sync_mode: "interval".to_string(),
            max_file_size: 1024 * 1024,
            retention_files: 5,
            max_pending_writes: 5, // Very low limit for testing
            ..Default::default()
        };

        let mut writer = WalWriter::new(dir.path(), config).unwrap();

        // Write up to the limit (5 writes)
        for i in 0..5 {
            writer
                .append_record("topic1", 0, None, &Bytes::from(format!("value-{}", i)))
                .unwrap();
        }

        // The 6th write should fail with backpressure error
        let result = writer.append_record("topic1", 0, None, &Bytes::from("value-5"));
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("backpressure"),
            "Expected backpressure error, got: {}",
            err_msg
        );

        // After sync, writes should succeed again
        writer.sync().unwrap();

        // Now the write should succeed
        writer
            .append_record("topic1", 0, None, &Bytes::from("value-after-sync"))
            .unwrap();

        writer.close().unwrap();
    }

    #[test]
    fn test_wal_backpressure_every_write_mode() {
        let dir = tempdir().unwrap();
        let config = WalConfig {
            enabled: true,
            sync_interval_ms: 100,
            sync_mode: "every_write".to_string(),
            max_file_size: 1024 * 1024,
            retention_files: 5,
            max_pending_writes: 5, // Low limit - but shouldn't matter in every_write mode
            ..Default::default()
        };

        let mut writer = WalWriter::new(dir.path(), config).unwrap();

        // In every_write mode, pending writes are reset after each write
        // so we should be able to write many more than the limit
        for i in 0..20 {
            writer
                .append_record("topic1", 0, None, &Bytes::from(format!("value-{}", i)))
                .unwrap();
        }

        writer.close().unwrap();

        // Verify all entries were written
        let wal_dir = dir.path().join("wal");
        let entries = WalReader::read_all(&wal_dir).unwrap();
        assert_eq!(entries.len(), 20);
    }

    // =============================================================================
    // Sharded WAL Tests
    // =============================================================================

    #[test]
    fn test_shard_selector_topic_strategy() {
        let selector = ShardSelector::new(4, ShardStrategy::Topic);

        // Same topic should always go to same shard regardless of partition
        let shard1 = selector.select("my-topic", 0);
        let shard2 = selector.select("my-topic", 1);
        let shard3 = selector.select("my-topic", 99);
        assert_eq!(shard1, shard2);
        assert_eq!(shard2, shard3);

        // Different topics may go to different shards
        let shard_a = selector.select("topic-a", 0);
        let shard_b = selector.select("topic-b", 0);
        // Not asserting inequality - they might hash to same shard by chance
        assert!(shard_a < 4);
        assert!(shard_b < 4);
    }

    #[test]
    fn test_shard_selector_partition_strategy() {
        let selector = ShardSelector::new(4, ShardStrategy::Partition);

        // Different partitions of same topic may go to different shards
        let shard0 = selector.select("my-topic", 0);
        let shard1 = selector.select("my-topic", 1);
        // Shard selection depends on hash - just verify valid range
        assert!(shard0 < 4);
        assert!(shard1 < 4);
    }

    #[test]
    fn test_sharded_wal_basic() {
        let dir = tempdir().unwrap();
        let config = WalConfig {
            enabled: true,
            sharding_enabled: true,
            shard_count: 2,
            shard_strategy: "topic".to_string(),
            ..Default::default()
        };

        let writer = ShardedWalWriter::new(dir.path(), config).unwrap();
        assert_eq!(writer.shard_count(), 2);
        assert!(writer.is_enabled());

        // Write some records
        let seq1 = writer
            .append_record("topic1", 0, Some(&Bytes::from("k1")), &Bytes::from("v1"))
            .unwrap();
        let seq2 = writer
            .append_record("topic2", 0, None, &Bytes::from("v2"))
            .unwrap();
        let seq3 = writer
            .append_record("topic1", 1, None, &Bytes::from("v3"))
            .unwrap();

        // Sequences should be globally ordered
        assert_eq!(seq1, 0);
        assert_eq!(seq2, 1);
        assert_eq!(seq3, 2);

        writer.close().unwrap();

        // Read back using sharded reader
        let wal_dir = dir.path().join("wal");
        let entries = ShardedWalReader::read_all(&wal_dir).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].sequence, 0);
        assert_eq!(entries[1].sequence, 1);
        assert_eq!(entries[2].sequence, 2);
    }

    #[test]
    fn test_sharded_wal_checkpoint() {
        let dir = tempdir().unwrap();
        let config = WalConfig {
            enabled: true,
            sharding_enabled: true,
            shard_count: 2,
            ..Default::default()
        };

        let writer = ShardedWalWriter::new(dir.path(), config).unwrap();

        writer
            .append_record("topic1", 0, None, &Bytes::from("v1"))
            .unwrap();
        let checkpoint_seq = writer.checkpoint().unwrap();
        writer
            .append_record("topic1", 0, None, &Bytes::from("v2"))
            .unwrap();

        writer.close().unwrap();

        // Read and verify checkpoint
        let wal_dir = dir.path().join("wal");
        let entries = ShardedWalReader::read_all(&wal_dir).unwrap();

        // We should see 3+ entries (record, checkpoint in both shards, record)
        // Checkpoint is written to all shards, so we'll see it multiple times
        let checkpoint_entries: Vec<_> = entries
            .iter()
            .filter(|e| e.entry_type == WalEntryType::Checkpoint)
            .collect();
        assert!(!checkpoint_entries.is_empty());
        assert_eq!(checkpoint_entries[0].sequence, checkpoint_seq);
    }

    #[test]
    fn test_sharded_wal_disabled() {
        let dir = tempdir().unwrap();
        let config = WalConfig {
            enabled: false,
            sharding_enabled: true,
            shard_count: 2,
            ..Default::default()
        };

        let writer = ShardedWalWriter::new(dir.path(), config).unwrap();
        assert!(!writer.is_enabled());

        // Writes should succeed but return 0
        let seq = writer
            .append_record("topic1", 0, None, &Bytes::from("v1"))
            .unwrap();
        assert_eq!(seq, 0);
    }

    #[test]
    fn test_sharded_wal_single_shard() {
        let dir = tempdir().unwrap();
        let config = WalConfig {
            enabled: true,
            sharding_enabled: false, // Disabling uses 1 shard (backward compatible)
            ..Default::default()
        };

        let writer = ShardedWalWriter::new(dir.path(), config).unwrap();
        assert_eq!(writer.shard_count(), 1);

        writer
            .append_record("topic1", 0, None, &Bytes::from("v1"))
            .unwrap();
        writer
            .append_record("topic2", 0, None, &Bytes::from("v2"))
            .unwrap();

        writer.close().unwrap();

        let wal_dir = dir.path().join("wal");
        let entries = ShardedWalReader::read_all(&wal_dir).unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn test_sharded_wal_reader_detects_layout() {
        let dir = tempdir().unwrap();

        // Empty directory - not sharded
        let wal_dir = dir.path().join("wal");
        fs::create_dir_all(&wal_dir).unwrap();
        assert!(!ShardedWalReader::is_sharded(&wal_dir));

        // Create shard directories
        fs::create_dir_all(wal_dir.join("shard-00")).unwrap();
        assert!(ShardedWalReader::is_sharded(&wal_dir));
    }
}
