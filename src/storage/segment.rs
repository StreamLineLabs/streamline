//! Segment file implementation for Streamline storage
//!
//! Segments are the primary storage unit for Streamline, containing a range of records
//! in an append-only format optimized for streaming workloads.
//!
//! # Durability Guarantees
//!
//! Segment durability depends on the storage configuration and when data is synced to disk:
//!
//! ## Write Path
//!
//! 1. **Append**: Records are written to a buffered writer
//! 2. **Flush**: `flush()` pushes data to OS page cache (survives process crash)
//! 3. **Sync**: `sync_all()` persists data to stable storage (survives power failure)
//!
//! ## Sync Modes
//!
//! The storage layer supports different sync modes (configured via WAL or partition settings):
//!
//! - **Immediate sync**: Every write is followed by `sync_all()`, guaranteeing durability
//!   at the cost of ~1-5ms latency per write
//! - **Batched sync**: Writes are batched and synced periodically for better throughput
//! - **OS-managed sync**: Relies on OS to sync, offering maximum performance but data
//!   loss risk on power failure
//!
//! ## Header Persistence
//!
//! **Important**: The segment header (containing metadata like `max_offset`, `record_count`)
//! is updated in memory during writes but only persisted to disk when the segment is sealed.
//! This means:
//!
//! - On crash before sealing: The header on disk may show stale metadata
//! - On recovery: The segment is scanned to rebuild accurate header metadata
//! - After sealing: Header is persisted and the segment becomes immutable
//!
//! ## Data Integrity
//!
//! - **CRC32 checksums**: Both header and record batches have CRC32 checksums
//! - **Magic bytes**: Segment files start with `STRM` magic bytes for validation
//! - **Recovery scanning**: Corrupt or truncated records at EOF are detected and skipped
//!
//! # File Format
//!
//! ```text
//! ┌─────────────────────────────────────────┐
//! │ Header (64 bytes)                       │
//! │   Magic: "STRM" (4 bytes)               │
//! │   Version: u16                          │
//! │   Flags: u16                            │
//! │   Base Offset: i64                      │
//! │   Max Offset: i64                       │
//! │   Timestamps: i64 × 2                   │
//! │   Record Count: u32                     │
//! │   CRC32: u32                            │
//! │   Compression: u8                       │
//! │   Reserved: padding to 64 bytes         │
//! ├─────────────────────────────────────────┤
//! │ Record Batch 1                          │
//! │   Length: u32                           │
//! │   CRC32: u32                            │
//! │   Data: [Record...]                     │
//! ├─────────────────────────────────────────┤
//! │ Record Batch 2...N                      │
//! └─────────────────────────────────────────┘
//! ```
//!
//! # Configuration Impact
//!
//! | Setting | Impact on Durability |
//! |---------|---------------------|
//! | `storage.sync_mode = "every_write"` | Maximum durability, minimum throughput |
//! | `storage.sync_mode = "interval"` | Balanced, bounded data loss window |
//! | `storage.sync_mode = "os_default"` | Maximum throughput, unbounded loss risk |
//! | `storage.segment_max_bytes` | Larger segments = longer time to seal |

use crate::error::{Result, StreamlineError};
use crate::storage::compression::{compress, decompress, CompressionCodec};
use crate::storage::index::{index_path_for_segment, IndexBuilder, SegmentIndex};
use crate::storage::io_backend::{SyncDirectConfig, SyncDirectFile};
use crate::storage::mmap::MmapSegmentReader;
use crate::storage::prefetch::{self, PrefetchConfig, SegmentPrefetcher};
use crate::storage::record::{Record, RecordBatch};
use crate::storage::zerocopy::ZeroCopyConfig;
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use tracing::{debug, warn};

/// Magic bytes for segment file header
const SEGMENT_MAGIC: &[u8; 4] = b"STRM";

/// Magic bytes for segment file footer (reserved for future use)
#[allow(dead_code)]
const SEGMENT_END_MAGIC: &[u8; 4] = b"ENDS";

/// Current segment format version
const SEGMENT_VERSION: u16 = 1;

/// Size of segment header in bytes
const HEADER_SIZE: usize = 64;

/// Size of segment footer in bytes (reserved for future use)
#[allow(dead_code)]
const FOOTER_SIZE: usize = 32;

/// Flag indicating bincode serialization format (bit 0 of header flags)
/// When set, record batches are serialized with bincode; otherwise legacy JSON is used.
const FLAG_BINCODE_FORMAT: u16 = 0x0001;

/// Sync mode for segment writes
///
/// Controls when data is synced to stable storage (fsync). More aggressive
/// sync modes provide better durability but lower throughput.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SegmentSyncMode {
    /// Only flush to OS buffer - fastest but data loss risk on power failure
    None,
    /// Sync when segment is sealed - balanced durability (default)
    #[default]
    OnSeal,
    /// Sync every N milliseconds - configurable durability
    Interval,
    /// Sync after every batch write - maximum durability, slowest
    EveryWrite,
}

impl SegmentSyncMode {
    /// Parse sync mode from string configuration
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "none" => Ok(Self::None),
            "on_seal" | "onseal" => Ok(Self::OnSeal),
            "interval" => Ok(Self::Interval),
            "every_write" | "everywrite" => Ok(Self::EveryWrite),
            _ => Err(StreamlineError::Config(format!(
                "Invalid segment sync mode '{}'. Valid options: none, on_seal, interval, every_write",
                s
            ))),
        }
    }
}

impl std::str::FromStr for SegmentSyncMode {
    type Err = StreamlineError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Self::parse(s)
    }
}

/// Configuration for segment I/O
///
/// Controls how segments open and write files, including optional O_DIRECT
/// support for bypassing the OS page cache.
#[derive(Debug, Clone)]
pub struct SegmentConfig {
    /// Compression codec for writing batches
    pub compression: CompressionCodec,
    /// Sync mode for this segment
    pub sync_mode: SegmentSyncMode,
    /// Sync interval in milliseconds (used with Interval mode)
    pub sync_interval_ms: u64,
    /// Enable O_DIRECT I/O to bypass OS page cache
    pub use_direct_io: bool,
    /// Direct I/O configuration (used when use_direct_io is true)
    pub direct_io_config: Option<SyncDirectConfig>,
}

impl Default for SegmentConfig {
    fn default() -> Self {
        Self {
            compression: CompressionCodec::None,
            sync_mode: SegmentSyncMode::OnSeal,
            sync_interval_ms: 100,
            use_direct_io: false,
            direct_io_config: None,
        }
    }
}

impl SegmentConfig {
    /// Create config with Direct I/O enabled
    pub fn with_direct_io(direct_config: SyncDirectConfig) -> Self {
        Self {
            use_direct_io: true,
            direct_io_config: Some(direct_config),
            ..Default::default()
        }
    }

    /// Create config from storage IoBackendConfig
    pub fn from_io_backend_config(config: &crate::config::IoBackendConfig) -> Self {
        if config.enable_direct_io {
            Self {
                use_direct_io: true,
                direct_io_config: Some(SyncDirectConfig::from_io_backend_config(config)),
                ..Default::default()
            }
        } else {
            Self::default()
        }
    }
}

/// Writer abstraction for segments
///
/// Supports both standard buffered I/O and O_DIRECT I/O. When using Direct I/O,
/// the OS page cache is bypassed for predictable memory usage.
pub enum SegmentWriter {
    /// Standard buffered I/O
    Standard(BufWriter<File>),
    /// Direct I/O (O_DIRECT on Linux, F_NOCACHE on macOS)
    Direct(SyncDirectFile),
}

impl SegmentWriter {
    /// Create a standard buffered writer
    pub fn standard(file: File) -> Self {
        Self::Standard(BufWriter::new(file))
    }

    /// Create a direct I/O writer
    pub fn direct(file: SyncDirectFile) -> Self {
        Self::Direct(file)
    }

    /// Write all bytes to the writer
    pub fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        match self {
            Self::Standard(w) => w.write_all(buf),
            Self::Direct(w) => w.write_all(buf),
        }
    }

    /// Flush buffered data
    pub fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Standard(w) => w.flush(),
            Self::Direct(w) => w.flush(),
        }
    }

    /// Sync data to disk (fdatasync)
    pub fn sync_data(&self) -> std::io::Result<()> {
        match self {
            Self::Standard(w) => w.get_ref().sync_data(),
            Self::Direct(w) => w.as_file().sync_data(),
        }
    }

    /// Sync data and metadata to disk (fsync)
    pub fn sync_all(&self) -> std::io::Result<()> {
        match self {
            Self::Standard(w) => w.get_ref().sync_all(),
            Self::Direct(w) => w.as_file().sync_all(),
        }
    }

    /// Check if using Direct I/O
    pub fn is_direct(&self) -> bool {
        matches!(self, Self::Direct(_))
    }
}

/// Segment file header
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentHeader {
    /// Format version
    pub version: u16,

    /// Flags (reserved for future use)
    pub flags: u16,

    /// Base offset (first offset in this segment)
    pub base_offset: i64,

    /// Maximum offset (last offset in this segment)
    pub max_offset: i64,

    /// Base timestamp
    pub base_timestamp: i64,

    /// Maximum timestamp
    pub max_timestamp: i64,

    /// Number of records in this segment
    pub record_count: u32,

    /// CRC32 checksum of the header
    pub crc32: u32,

    /// Compression type (0=none, 1=lz4, 2=zstd)
    pub compression: u8,
}

impl Default for SegmentHeader {
    fn default() -> Self {
        Self {
            version: SEGMENT_VERSION,
            flags: 0,
            base_offset: 0,
            max_offset: -1,
            base_timestamp: 0,
            max_timestamp: 0,
            record_count: 0,
            crc32: 0,
            compression: 0,
        }
    }
}

impl SegmentHeader {
    /// Serialize header to bytes
    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(HEADER_SIZE);

        // Magic (4 bytes)
        buf.put_slice(SEGMENT_MAGIC);

        // Version (2 bytes)
        buf.put_u16_le(self.version);

        // Flags (2 bytes)
        buf.put_u16_le(self.flags);

        // Base offset (8 bytes)
        buf.put_i64_le(self.base_offset);

        // Max offset (8 bytes)
        buf.put_i64_le(self.max_offset);

        // Base timestamp (8 bytes)
        buf.put_i64_le(self.base_timestamp);

        // Max timestamp (8 bytes)
        buf.put_i64_le(self.max_timestamp);

        // Record count (4 bytes)
        buf.put_u32_le(self.record_count);

        // Compression (1 byte)
        buf.put_u8(self.compression);

        // Reserved (15 bytes to reach 60 bytes)
        buf.put_slice(&[0u8; 15]);

        // CRC32 (4 bytes) - calculated over all previous bytes
        let crc = crc32fast::hash(&buf[..]);
        buf.put_u32_le(crc);

        buf.freeze()
    }

    /// Deserialize header from bytes
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < HEADER_SIZE {
            return Err(StreamlineError::CorruptedData(
                "Segment header too short".to_string(),
            ));
        }

        // Check magic
        if &data[0..4] != SEGMENT_MAGIC {
            return Err(StreamlineError::CorruptedData(
                "Invalid segment magic".to_string(),
            ));
        }

        let version = u16::from_le_bytes([data[4], data[5]]);
        let flags = u16::from_le_bytes([data[6], data[7]]);
        let base_offset = i64::from_le_bytes(data[8..16].try_into().map_err(|_| {
            StreamlineError::CorruptedData("Invalid base_offset bytes in segment header".into())
        })?);
        let max_offset = i64::from_le_bytes(data[16..24].try_into().map_err(|_| {
            StreamlineError::CorruptedData("Invalid max_offset bytes in segment header".into())
        })?);
        let base_timestamp = i64::from_le_bytes(data[24..32].try_into().map_err(|_| {
            StreamlineError::CorruptedData("Invalid base_timestamp bytes in segment header".into())
        })?);
        let max_timestamp = i64::from_le_bytes(data[32..40].try_into().map_err(|_| {
            StreamlineError::CorruptedData("Invalid max_timestamp bytes in segment header".into())
        })?);
        let record_count = u32::from_le_bytes(data[40..44].try_into().map_err(|_| {
            StreamlineError::CorruptedData("Invalid record_count bytes in segment header".into())
        })?);
        let compression = data[44];
        let crc32 = u32::from_le_bytes(data[60..64].try_into().map_err(|_| {
            StreamlineError::CorruptedData("Invalid crc32 bytes in segment header".into())
        })?);

        // Verify CRC
        let computed_crc = crc32fast::hash(&data[..60]);
        if computed_crc != crc32 {
            return Err(StreamlineError::CorruptedData(
                "Segment header CRC mismatch".to_string(),
            ));
        }

        Ok(Self {
            version,
            flags,
            base_offset,
            max_offset,
            base_timestamp,
            max_timestamp,
            record_count,
            crc32,
            compression,
        })
    }
}

/// Represents a segment file that stores records
pub struct Segment {
    /// Path to the segment file
    path: PathBuf,

    /// Segment header
    header: SegmentHeader,

    /// Current write position in the file
    write_position: u64,

    /// File handle for writing (None if read-only)
    writer: Option<SegmentWriter>,

    /// Whether this segment is sealed (read-only)
    sealed: bool,

    /// Compression codec for writing batches
    compression: CompressionCodec,

    /// Sparse index for efficient offset lookups
    index: Option<SegmentIndex>,

    /// Memory-mapped reader for zero-copy access (only for sealed segments)
    mmap_reader: Option<MmapSegmentReader>,

    /// Running CRC32 hash for segment-level integrity checking.
    /// This chains all batch CRCs together, allowing validation of the
    /// entire segment's integrity without reading each batch individually.
    running_crc: u32,

    /// Number of records written since last header persistence.
    /// Used for periodic header persistence to reduce data loss on crash.
    records_since_header_persist: u32,

    /// Persist header to disk every N records (0 = only on seal)
    persist_header_interval: u32,

    /// Sync mode for this segment
    sync_mode: SegmentSyncMode,

    /// Sync interval in milliseconds (used with Interval mode)
    sync_interval_ms: u64,

    /// Time of last sync (used with Interval mode)
    last_sync: std::time::Instant,

    /// Cached file handle for reads (None if not cached, Some for active segments)
    /// This reduces syscalls for frequently read active segments by reusing
    /// the file descriptor instead of opening on every read.
    read_file_cache: Option<std::sync::Mutex<BufReader<File>>>,

    /// Prefetcher for sequential read-ahead (optional)
    prefetcher: Option<SegmentPrefetcher>,

    /// Whether Direct I/O is enabled for this segment
    use_direct_io: bool,
}

impl Segment {
    /// Create a new segment file with default (no) compression
    pub fn create(path: &Path, base_offset: i64) -> Result<Self> {
        Self::create_with_compression(path, base_offset, CompressionCodec::None)
    }

    /// Create a new segment file with specified compression
    pub fn create_with_compression(
        path: &Path,
        base_offset: i64,
        compression: CompressionCodec,
    ) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .truncate(true)
            .open(path)?;

        let header = SegmentHeader {
            base_offset,
            base_timestamp: chrono::Utc::now().timestamp_millis(),
            compression: compression.as_byte(),
            flags: FLAG_BINCODE_FORMAT, // Use bincode serialization for new segments
            ..Default::default()
        };

        let mut writer = SegmentWriter::standard(file);

        // Write header
        let header_bytes = header.to_bytes();
        writer.write_all(&header_bytes)?;
        writer.flush()?;
        // Use sync_data for durability (fsync equivalent)
        writer.sync_data()?;

        // Create a new sparse index for this segment
        let index_path = index_path_for_segment(path);
        let index = SegmentIndex::new(&index_path);

        // Enable read cache for active (unsealed) segments
        let read_file_cache = Some(std::sync::Mutex::new(BufReader::new(File::open(path)?)));

        Ok(Self {
            path: path.to_path_buf(),
            header,
            write_position: HEADER_SIZE as u64,
            writer: Some(writer),
            sealed: false,
            compression,
            index: Some(index),
            mmap_reader: None,
            running_crc: 0,
            records_since_header_persist: 0,
            persist_header_interval: 0, // Default: only persist on seal
            sync_mode: SegmentSyncMode::OnSeal,
            sync_interval_ms: 100,
            last_sync: std::time::Instant::now(),
            read_file_cache,
            prefetcher: None, // Prefetcher disabled by default, enable via configure_prefetch()
            use_direct_io: false,
        })
    }

    /// Create a new segment file with specified compression and sync configuration
    pub fn create_with_config(
        path: &Path,
        base_offset: i64,
        compression: CompressionCodec,
        sync_mode: SegmentSyncMode,
        sync_interval_ms: u64,
    ) -> Result<Self> {
        let mut segment = Self::create_with_compression(path, base_offset, compression)?;
        segment.sync_mode = sync_mode;
        segment.sync_interval_ms = sync_interval_ms;
        Ok(segment)
    }

    /// Create a new segment file with full segment configuration (including DirectIO)
    pub fn create_with_segment_config(
        path: &Path,
        base_offset: i64,
        config: &SegmentConfig,
    ) -> Result<Self> {
        let header = SegmentHeader {
            base_offset,
            base_timestamp: chrono::Utc::now().timestamp_millis(),
            compression: config.compression.as_byte(),
            flags: FLAG_BINCODE_FORMAT,
            ..Default::default()
        };

        // Create writer - either DirectIO or standard
        let mut writer = if config.use_direct_io {
            let direct_config = config.direct_io_config.clone().unwrap_or_default();
            let direct_file = SyncDirectFile::create(path, direct_config)?;
            debug!(
                path = %path.display(),
                direct_enabled = direct_file.is_direct_enabled(),
                "Created segment with Direct I/O"
            );
            SegmentWriter::direct(direct_file)
        } else {
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .read(true)
                .truncate(true)
                .open(path)?;
            SegmentWriter::standard(file)
        };

        // Write header
        let header_bytes = header.to_bytes();
        writer.write_all(&header_bytes)?;
        writer.flush()?;
        writer.sync_data()?;

        // Create a new sparse index for this segment
        let index_path = index_path_for_segment(path);
        let index = SegmentIndex::new(&index_path);

        // Enable read cache for active (unsealed) segments
        let read_file_cache = Some(std::sync::Mutex::new(BufReader::new(File::open(path)?)));

        Ok(Self {
            path: path.to_path_buf(),
            header,
            write_position: HEADER_SIZE as u64,
            writer: Some(writer),
            sealed: false,
            compression: config.compression,
            index: Some(index),
            mmap_reader: None,
            running_crc: 0,
            records_since_header_persist: 0,
            persist_header_interval: 0,
            sync_mode: config.sync_mode,
            sync_interval_ms: config.sync_interval_ms,
            last_sync: std::time::Instant::now(),
            read_file_cache,
            prefetcher: None,
            use_direct_io: config.use_direct_io,
        })
    }

    /// Open an existing segment file
    pub fn open(path: &Path) -> Result<Self> {
        let mut file = File::open(path)?;

        // Read header
        let mut header_buf = [0u8; HEADER_SIZE];
        file.read_exact(&mut header_buf)?;
        let header = SegmentHeader::from_bytes(&header_buf)?;

        // Get compression codec from header
        let compression =
            CompressionCodec::from_byte(header.compression).unwrap_or(CompressionCodec::None);

        // Get file size for write position
        let write_position = file.seek(SeekFrom::End(0))?;

        // Try to load existing index, or build one if it doesn't exist
        let index_path = index_path_for_segment(path);
        let index = if index_path.exists() {
            match SegmentIndex::load(&index_path) {
                Ok(idx) => Some(idx),
                Err(e) => {
                    warn!(
                        path = %path.display(),
                        error = %e,
                        "Segment index corrupted or incompatible, rebuilding from segment data"
                    );
                    // Fall through to rebuild
                    None
                }
            }
        } else {
            debug!(path = %path.display(), "No index file found, building from segment");
            None
        };

        // Build index if we don't have one (either missing or failed to load)
        let index = if index.is_none() {
            let segment_for_index = Self {
                path: path.to_path_buf(),
                header: header.clone(),
                write_position,
                writer: None,
                sealed: true,
                compression,
                index: None,
                mmap_reader: None,
                running_crc: 0,
                records_since_header_persist: 0,
                persist_header_interval: 0,
                sync_mode: SegmentSyncMode::OnSeal,
                sync_interval_ms: 100,
                last_sync: std::time::Instant::now(),
                read_file_cache: None, // Sealed segments don't need read cache
                prefetcher: None,
                use_direct_io: false,
            };
            let built_index = segment_for_index.build_index()?;
            // Save the newly built index
            if let Err(e) = built_index.save() {
                warn!(path = %path.display(), error = %e, "Failed to save rebuilt index");
            }
            Some(built_index)
        } else {
            index
        };

        Ok(Self {
            path: path.to_path_buf(),
            header,
            write_position,
            writer: None,
            sealed: true,
            compression,
            index,
            mmap_reader: None,
            running_crc: 0,
            records_since_header_persist: 0,
            persist_header_interval: 0,
            sync_mode: SegmentSyncMode::OnSeal,
            sync_interval_ms: 100,
            last_sync: std::time::Instant::now(),
            read_file_cache: None, // Sealed segments don't need read cache
            prefetcher: None,
            use_direct_io: false,
        })
    }

    /// Open an existing segment file with zero-copy configuration
    ///
    /// If the segment is sealed and meets the mmap threshold, a memory-mapped
    /// reader is created for zero-copy access.
    pub fn open_with_config(path: &Path, config: &ZeroCopyConfig) -> Result<Self> {
        let mut segment = Self::open(path)?;

        // Enable mmap for sealed segments that meet the threshold
        if segment.sealed && config.enabled && config.enable_mmap {
            let file_size = std::fs::metadata(path)?.len();
            if file_size >= config.mmap_threshold {
                match MmapSegmentReader::open(path, config.mmap_threshold) {
                    Ok(mut reader) => {
                        // Advise the kernel about sequential access pattern for consumer reads
                        // This hints to the OS to prefetch ahead for better sequential read performance
                        #[cfg(unix)]
                        if let Err(e) = reader.advise_sequential() {
                            tracing::debug!(
                                "Failed to set MADV_SEQUENTIAL for {}: {}",
                                path.display(),
                                e
                            );
                        }

                        // Optionally prefetch the data into memory for faster initial access
                        // This is best-effort and may help reduce latency for frequently accessed segments
                        #[cfg(unix)]
                        if let Err(e) = reader.advise_willneed() {
                            tracing::debug!(
                                "Failed to set MADV_WILLNEED for {}: {}",
                                path.display(),
                                e
                            );
                        }

                        segment.mmap_reader = Some(reader);
                        tracing::debug!(
                            "Enabled mmap for segment {} ({} bytes)",
                            path.display(),
                            file_size
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Failed to create mmap reader for {}: {}",
                            path.display(),
                            e
                        );
                    }
                }
            }
        }

        Ok(segment)
    }

    /// Open an existing segment file for appending (writable)
    pub fn open_for_append(path: &Path) -> Result<Self> {
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;

        // Read header
        let mut header_buf = [0u8; HEADER_SIZE];
        file.read_exact(&mut header_buf)?;
        let mut header = SegmentHeader::from_bytes(&header_buf)?;

        // Get compression codec from header
        let compression =
            CompressionCodec::from_byte(header.compression).unwrap_or(CompressionCodec::None);

        // Get file size for write position
        let file_size = file.seek(SeekFrom::End(0))?;

        // Validate all batches by scanning and checking CRCs
        // This handles crash recovery by truncating at the last valid batch
        let (valid_write_position, records, running_crc) =
            Self::validate_and_recover_batches(path, file_size, &header)?;

        // If we found corruption, truncate the file to the last valid position
        if valid_write_position < file_size {
            warn!(
                path = %path.display(),
                file_size = file_size,
                truncating_to = valid_write_position,
                "Detected corrupted data during recovery, truncating segment"
            );
            // Reopen file for truncation
            let mut file = OpenOptions::new().write(true).open(path)?;
            file.set_len(valid_write_position)?;
            file.sync_all()?;

            // After truncation, recalculate and persist the updated header
            if let Some(last_record) = records.last() {
                header.max_offset = last_record.offset;
                header.record_count = records.len() as u32;
                header.max_timestamp = last_record.timestamp;
            } else {
                // No valid records - reset to initial state
                header.max_offset = -1;
                header.record_count = 0;
                header.max_timestamp = header.base_timestamp;
            }

            // Persist updated header to disk
            file.seek(SeekFrom::Start(0))?;
            let header_bytes = header.to_bytes();
            file.write_all(&header_bytes)?;
            file.sync_all()?;

            debug!(
                path = %path.display(),
                max_offset = header.max_offset,
                record_count = header.record_count,
                "Segment header persisted after truncation"
            );
        } else {
            // No truncation needed, but still update header with validated stats
            if let Some(last_record) = records.last() {
                header.max_offset = last_record.offset;
                header.record_count = records.len() as u32;
            }
        }
        // Create writer positioned at end of valid data
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        file.seek(SeekFrom::Start(valid_write_position))?;
        let writer = SegmentWriter::standard(file);

        // Try to load existing index, or create a new one
        let index_path = index_path_for_segment(path);
        let index = if index_path.exists() {
            match SegmentIndex::load(&index_path) {
                Ok(idx) => Some(idx),
                Err(e) => {
                    warn!(
                        path = %path.display(),
                        error = %e,
                        "Segment index corrupted or incompatible, creating new index"
                    );
                    Some(SegmentIndex::new(&index_path))
                }
            }
        } else {
            debug!(path = %path.display(), "Creating new index for writable segment");
            Some(SegmentIndex::new(&index_path))
        };

        // Enable read cache for active (unsealed) segments
        let read_file_cache = Some(std::sync::Mutex::new(BufReader::new(File::open(path)?)));

        Ok(Self {
            path: path.to_path_buf(),
            header,
            write_position: valid_write_position,
            writer: Some(writer),
            sealed: false,
            compression,
            index,
            mmap_reader: None,
            running_crc,
            records_since_header_persist: 0,
            persist_header_interval: 0,
            sync_mode: SegmentSyncMode::OnSeal,
            sync_interval_ms: 100,
            last_sync: std::time::Instant::now(),
            read_file_cache,
            prefetcher: None,
            use_direct_io: false,
        })
    }

    /// Validate all batches in a segment, returning the position of the last valid batch.
    /// This is used during recovery to detect and truncate corrupted data from partial writes.
    fn validate_and_recover_batches(
        path: &Path,
        file_size: u64,
        header: &SegmentHeader,
    ) -> Result<(u64, Vec<Record>, u32)> {
        let mut file = BufReader::new(File::open(path)?);

        // Skip header
        file.seek(SeekFrom::Start(HEADER_SIZE as u64))?;

        let mut records = Vec::new();
        let mut position = HEADER_SIZE as u64;
        let mut last_valid_position = HEADER_SIZE as u64;
        let mut running_crc: u32 = 0;

        while position < file_size {
            let batch_start = position;

            // Read compression flag (1 byte)
            let mut compression_buf = [0u8; 1];
            if file.read_exact(&mut compression_buf).is_err() {
                debug!(
                    path = %path.display(),
                    position = position,
                    "Incomplete compression flag read during recovery"
                );
                break;
            }
            let batch_compression =
                CompressionCodec::from_byte(compression_buf[0]).unwrap_or(CompressionCodec::None);

            // Read batch length
            let mut len_buf = [0u8; 4];
            if file.read_exact(&mut len_buf).is_err() {
                debug!(
                    path = %path.display(),
                    position = position,
                    "Incomplete length read during recovery"
                );
                break;
            }
            let batch_len = u32::from_le_bytes(len_buf) as usize;

            // Sanity check batch length to avoid huge allocations on corrupted data
            if batch_len > 256 * 1024 * 1024 {
                warn!(
                    path = %path.display(),
                    position = position,
                    batch_len = batch_len,
                    "Invalid batch length during recovery, likely corruption"
                );
                break;
            }

            // Read batch data
            let mut batch_data = vec![0u8; batch_len];
            if file.read_exact(&mut batch_data).is_err() {
                debug!(
                    path = %path.display(),
                    position = position,
                    batch_len = batch_len,
                    "Incomplete batch data read during recovery"
                );
                break;
            }

            // Read CRC
            let mut crc_buf = [0u8; 4];
            if file.read_exact(&mut crc_buf).is_err() {
                debug!(
                    path = %path.display(),
                    position = position,
                    "Incomplete CRC read during recovery"
                );
                break;
            }
            let stored_crc = u32::from_le_bytes(crc_buf);
            let computed_crc = crc32fast::hash(&batch_data);

            // Verify CRC
            if stored_crc != computed_crc {
                warn!(
                    path = %path.display(),
                    position = batch_start,
                    stored_crc = stored_crc,
                    computed_crc = computed_crc,
                    "CRC mismatch during recovery, truncating at last valid batch"
                );
                break;
            }

            // Chain this batch's CRC into the running segment CRC
            let mut hasher = crc32fast::Hasher::new_with_initial(running_crc);
            hasher.update(&stored_crc.to_le_bytes());
            running_crc = hasher.finalize();

            // Decompress if needed
            let batch_bytes = match decompress(&batch_data, batch_compression) {
                Ok(bytes) => bytes,
                Err(e) => {
                    warn!(
                        path = %path.display(),
                        position = batch_start,
                        error = %e,
                        "Decompression failed during recovery"
                    );
                    break;
                }
            };

            // Deserialize batch
            let batch: RecordBatch = if header.flags & FLAG_BINCODE_FORMAT != 0 {
                match bincode::deserialize(&batch_bytes) {
                    Ok(b) => b,
                    Err(e) => {
                        warn!(
                            path = %path.display(),
                            position = batch_start,
                            error = %e,
                            "Batch deserialization failed during recovery"
                        );
                        break;
                    }
                }
            } else {
                match serde_json::from_slice(&batch_bytes) {
                    Ok(b) => b,
                    Err(e) => {
                        warn!(
                            path = %path.display(),
                            position = batch_start,
                            error = %e,
                            "Legacy JSON batch deserialization failed during recovery"
                        );
                        break;
                    }
                }
            };

            records.extend(batch.records);

            // Update position: 1 byte compression + 4 bytes length + data + 4 bytes CRC
            position += 1 + 4 + batch_len as u64 + 4;
            last_valid_position = position;
        }

        Ok((last_valid_position, records, running_crc))
    }

    /// Append a record batch to this segment
    pub fn append_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        if self.sealed {
            return Err(StreamlineError::storage_msg(
                "Cannot write to sealed segment".to_string(),
            ));
        }

        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| StreamlineError::storage_msg("No writer available".to_string()))?;

        // Record the position before writing (for index)
        let batch_position = self.write_position;

        // Serialize batch to bincode (more efficient than JSON)
        let batch_bytes = bincode::serialize(batch).map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to serialize batch: {}", e))
        })?;

        // Compress if compression is enabled
        let batch_data = compress(&batch_bytes, self.compression)?;

        // Write compression flag (1 byte) - needed to know how to decompress
        writer.write_all(&[self.compression.as_byte()])?;

        // Write batch length (4 bytes)
        let batch_len = batch_data.len() as u32;
        writer.write_all(&batch_len.to_le_bytes())?;

        // Write batch data
        writer.write_all(&batch_data)?;

        // Write CRC32 (4 bytes) - computed on compressed data
        let crc = crc32fast::hash(&batch_data);
        writer.write_all(&crc.to_le_bytes())?;

        // Chain this batch's CRC into the running segment CRC.
        // This allows validating the entire segment's integrity
        // without reading each batch individually.
        let mut hasher = crc32fast::Hasher::new_with_initial(self.running_crc);
        hasher.update(&crc.to_le_bytes());
        self.running_crc = hasher.finalize();

        writer.flush()?;

        // Sync to stable storage based on configured sync mode
        match self.sync_mode {
            SegmentSyncMode::EveryWrite => {
                // Maximum durability: sync after every batch
                writer.sync_data()?;
                self.last_sync = std::time::Instant::now();
            }
            SegmentSyncMode::Interval => {
                // Sync if interval has elapsed since last sync
                if self.last_sync.elapsed().as_millis() >= self.sync_interval_ms as u128 {
                    writer.sync_data()?;
                    self.last_sync = std::time::Instant::now();
                }
            }
            SegmentSyncMode::OnSeal | SegmentSyncMode::None => {
                // OnSeal: will sync when segment is sealed
                // None: never sync (only flush to OS buffer)
            }
        }

        // Calculate batch size on disk
        // 1 byte compression flag + 4 bytes length + data + 4 bytes CRC
        let batch_size_on_disk = 1 + 4 + batch_data.len() as u64 + 4;

        // Update index with the first offset in this batch
        if let Some(ref mut index) = self.index {
            if let Some(first_offset) = batch.first_offset() {
                index.maybe_add_entry(first_offset, batch_position, batch_size_on_disk);
            }
        }

        // Update header stats
        let batch_record_count = batch.len() as u32;
        self.header.record_count += batch_record_count;
        if let Some(last_offset) = batch.last_offset() {
            self.header.max_offset = last_offset;
        }
        if !batch.records.is_empty() {
            self.header.max_timestamp = batch.timestamp;
        }

        self.write_position += batch_size_on_disk;

        // Track records for periodic header persistence
        self.records_since_header_persist += batch_record_count;

        // Optionally persist header if interval is configured
        if self.persist_header_interval > 0
            && self.records_since_header_persist >= self.persist_header_interval
        {
            // Best-effort: log warning but don't fail the append
            if let Err(e) = self.persist_header() {
                warn!(
                    path = %self.path.display(),
                    error = %e,
                    "Failed to persist segment header during periodic persistence"
                );
            }
        }

        Ok(())
    }

    /// Append a single record to this segment
    pub fn append_record(&mut self, record: Record) -> Result<()> {
        let mut batch = RecordBatch::new(record.offset, record.timestamp);
        batch.add_record(record);
        self.append_batch(&batch)
    }

    /// Read all records from this segment
    pub fn read_all(&self) -> Result<Vec<Record>> {
        // Use cached file handle if available (for active segments), otherwise open fresh
        if let Some(ref cache) = self.read_file_cache {
            let mut file = cache.lock().unwrap_or_else(|e| e.into_inner());
            file.seek(SeekFrom::Start(HEADER_SIZE as u64))?;
            return self.read_all_from_file(&mut file);
        }

        let mut file = BufReader::new(File::open(&self.path)?);
        file.seek(SeekFrom::Start(HEADER_SIZE as u64))?;
        self.read_all_from_file(&mut file)
    }

    /// Internal helper to read all records from a file handle
    fn read_all_from_file(&self, file: &mut BufReader<File>) -> Result<Vec<Record>> {
        let mut records = Vec::new();
        let mut position = HEADER_SIZE as u64;

        while position < self.write_position {
            // Read compression flag (1 byte)
            let mut compression_buf = [0u8; 1];
            if file.read_exact(&mut compression_buf).is_err() {
                break;
            }
            let batch_compression =
                CompressionCodec::from_byte(compression_buf[0]).unwrap_or(CompressionCodec::None);

            // Read batch length
            let mut len_buf = [0u8; 4];
            if file.read_exact(&mut len_buf).is_err() {
                break;
            }
            let batch_len = u32::from_le_bytes(len_buf) as usize;

            // Read batch data
            let mut batch_data = vec![0u8; batch_len];
            file.read_exact(&mut batch_data)?;

            // Read and verify CRC
            let mut crc_buf = [0u8; 4];
            file.read_exact(&mut crc_buf)?;
            let stored_crc = u32::from_le_bytes(crc_buf);
            let computed_crc = crc32fast::hash(&batch_data);

            if stored_crc != computed_crc {
                return Err(StreamlineError::CorruptedData(
                    "Batch CRC mismatch".to_string(),
                ));
            }

            // Decompress if needed
            let batch_bytes = decompress(&batch_data, batch_compression)?;

            // Deserialize batch - use bincode if flag is set, otherwise legacy JSON
            let batch: RecordBatch = if self.header.flags & FLAG_BINCODE_FORMAT != 0 {
                bincode::deserialize(&batch_bytes).map_err(|e| {
                    StreamlineError::CorruptedData(format!("Failed to deserialize batch: {}", e))
                })?
            } else {
                // Legacy JSON format for backward compatibility
                serde_json::from_slice(&batch_bytes).map_err(|e| {
                    StreamlineError::CorruptedData(format!(
                        "Failed to deserialize batch (JSON): {}",
                        e
                    ))
                })?
            };
            records.extend(batch.records);

            // 1 byte compression + 4 bytes length + data + 4 bytes CRC
            position += 1 + 4 + batch_len as u64 + 4;
        }

        Ok(records)
    }

    /// Read records starting from a specific offset
    ///
    /// Uses the sparse index for O(log n) lookup to find the approximate starting position,
    /// then performs a short linear scan to find the exact offset.
    pub fn read_from_offset(&self, start_offset: i64, max_records: usize) -> Result<Vec<Record>> {
        // Try to use the index for efficient lookup
        let start_position = self
            .index
            .as_ref()
            .and_then(|idx| idx.lookup(start_offset))
            .unwrap_or(HEADER_SIZE as u64);

        self.read_from_position(start_position, start_offset, max_records)
    }

    /// Read records starting from a specific file position
    ///
    /// This is the internal method that reads from a known file position and filters
    /// records by offset. Used by `read_from_offset` after index lookup.
    fn read_from_position(
        &self,
        start_position: u64,
        start_offset: i64,
        max_records: usize,
    ) -> Result<Vec<Record>> {
        // Use cached file handle if available (for active segments), otherwise open fresh
        if let Some(ref cache) = self.read_file_cache {
            let mut file = cache.lock().unwrap_or_else(|e| e.into_inner());
            file.seek(SeekFrom::Start(start_position))?;
            return self.read_from_position_with_file(
                &mut file,
                start_position,
                start_offset,
                max_records,
            );
        }

        let mut file = BufReader::new(File::open(&self.path)?);
        file.seek(SeekFrom::Start(start_position))?;
        self.read_from_position_with_file(&mut file, start_position, start_offset, max_records)
    }

    /// Internal helper to read records from a file handle starting at a position
    fn read_from_position_with_file(
        &self,
        file: &mut BufReader<File>,
        start_position: u64,
        start_offset: i64,
        max_records: usize,
    ) -> Result<Vec<Record>> {
        let mut records = Vec::new();
        let mut position = start_position;

        while position < self.write_position && records.len() < max_records {
            // Read compression flag (1 byte)
            let mut compression_buf = [0u8; 1];
            if file.read_exact(&mut compression_buf).is_err() {
                break;
            }
            let batch_compression =
                CompressionCodec::from_byte(compression_buf[0]).unwrap_or(CompressionCodec::None);

            // Read batch length
            let mut len_buf = [0u8; 4];
            if file.read_exact(&mut len_buf).is_err() {
                break;
            }
            let batch_len = u32::from_le_bytes(len_buf) as usize;

            // Read batch data
            let mut batch_data = vec![0u8; batch_len];
            file.read_exact(&mut batch_data)?;

            // Read and verify CRC
            let mut crc_buf = [0u8; 4];
            file.read_exact(&mut crc_buf)?;
            let stored_crc = u32::from_le_bytes(crc_buf);
            let computed_crc = crc32fast::hash(&batch_data);

            if stored_crc != computed_crc {
                return Err(StreamlineError::CorruptedData(
                    "Batch CRC mismatch".to_string(),
                ));
            }

            // Decompress if needed
            let batch_bytes = decompress(&batch_data, batch_compression)?;

            // Deserialize batch - use bincode if flag is set, otherwise legacy JSON
            let batch: RecordBatch = if self.header.flags & FLAG_BINCODE_FORMAT != 0 {
                bincode::deserialize(&batch_bytes).map_err(|e| {
                    StreamlineError::CorruptedData(format!("Failed to deserialize batch: {}", e))
                })?
            } else {
                // Legacy JSON format for backward compatibility
                serde_json::from_slice(&batch_bytes).map_err(|e| {
                    StreamlineError::CorruptedData(format!(
                        "Failed to deserialize batch (JSON): {}",
                        e
                    ))
                })?
            };

            // Filter records by offset and collect up to max_records
            for record in batch.records {
                if record.offset >= start_offset {
                    records.push(record);
                    if records.len() >= max_records {
                        break;
                    }
                }
            }

            // 1 byte compression + 4 bytes length + data + 4 bytes CRC
            position += 1 + 4 + batch_len as u64 + 4;
        }

        Ok(records)
    }

    /// Persist the segment header to disk.
    ///
    /// This rewrites the header at the beginning of the segment file with
    /// updated metadata (max_offset, record_count, etc.). Useful for reducing
    /// data loss on crash - without this, header metadata is only persisted
    /// when the segment is sealed.
    ///
    /// # Returns
    /// Ok(()) on success, or error if header persistence fails.
    pub fn persist_header(&mut self) -> Result<()> {
        if self.sealed {
            // Sealed segments are immutable
            return Ok(());
        }

        // Need to flush writer first to ensure all data is written
        if let Some(ref mut writer) = self.writer {
            writer.flush()?;
        }

        // Open file directly to rewrite header at offset 0
        // We use OpenOptions to open for write without truncating
        let mut file = OpenOptions::new().write(true).open(&self.path)?;

        // Seek to beginning
        file.seek(SeekFrom::Start(0))?;

        // Write updated header
        let header_bytes = self.header.to_bytes();
        file.write_all(&header_bytes)?;
        file.sync_all()?;

        self.records_since_header_persist = 0;
        debug!(
            path = %self.path.display(),
            record_count = self.header.record_count,
            max_offset = self.header.max_offset,
            "Persisted segment header"
        );

        Ok(())
    }

    /// Set the interval for automatic header persistence.
    ///
    /// When set to a value > 0, the header will be persisted to disk after
    /// every N records are appended. This provides a trade-off between
    /// durability (lower values = less data loss on crash) and performance
    /// (higher values = fewer disk syncs).
    ///
    /// A value of 0 disables automatic persistence (header only persisted on seal).
    ///
    /// Recommended values:
    /// - 0: Maximum performance, header only persisted on seal (default)
    /// - 1000-10000: Balanced durability/performance for most workloads
    /// - 100-1000: Higher durability for critical data
    pub fn set_persist_header_interval(&mut self, interval: u32) {
        self.persist_header_interval = interval;
    }

    /// Seal the segment (make it read-only)
    pub fn seal(&mut self) -> Result<()> {
        if self.sealed {
            return Ok(());
        }

        // Persist header with final metadata before sealing
        self.persist_header()?;

        // Flush and sync writer
        if let Some(mut writer) = self.writer.take() {
            writer.flush()?;
            writer.sync_all()?;
        }

        // Save the index to disk
        if let Some(ref index) = self.index {
            if let Err(e) = index.save() {
                tracing::warn!("Failed to save segment index: {}", e);
            }
        }

        // Clear read cache when sealing - sealed segments don't need it
        // (they can use mmap instead for zero-copy reads)
        self.read_file_cache = None;

        self.sealed = true;
        Ok(())
    }

    /// Build a sparse index by scanning the segment file
    ///
    /// This method scans through all batches in the segment and creates
    /// an index with entries at regular intervals.
    fn build_index(&self) -> Result<SegmentIndex> {
        let mut file = BufReader::new(File::open(&self.path)?);

        // Skip header
        file.seek(SeekFrom::Start(HEADER_SIZE as u64))?;

        let index_path = index_path_for_segment(&self.path);
        let mut builder = IndexBuilder::new(SegmentIndex::DEFAULT_INTERVAL_BYTES);
        let mut position = HEADER_SIZE as u64;

        while position < self.write_position {
            let batch_position = position;

            // Read compression flag (1 byte)
            let mut compression_buf = [0u8; 1];
            if file.read_exact(&mut compression_buf).is_err() {
                break;
            }
            let batch_compression =
                CompressionCodec::from_byte(compression_buf[0]).unwrap_or(CompressionCodec::None);

            // Read batch length
            let mut len_buf = [0u8; 4];
            if file.read_exact(&mut len_buf).is_err() {
                break;
            }
            let batch_len = u32::from_le_bytes(len_buf) as usize;

            // Read batch data
            let mut batch_data = vec![0u8; batch_len];
            file.read_exact(&mut batch_data)?;

            // Read CRC (4 bytes) - we skip verification for index building
            let mut crc_buf = [0u8; 4];
            file.read_exact(&mut crc_buf)?;

            // Decompress if needed
            let batch_bytes = decompress(&batch_data, batch_compression)?;

            // Deserialize batch to get first offset (format depends on header flags)
            let batch: RecordBatch = if self.header.flags & FLAG_BINCODE_FORMAT != 0 {
                bincode::deserialize(&batch_bytes).map_err(|e| {
                    StreamlineError::CorruptedData(format!("Failed to deserialize batch: {}", e))
                })?
            } else {
                // Legacy JSON format for backward compatibility
                serde_json::from_slice(&batch_bytes).map_err(|e| {
                    StreamlineError::CorruptedData(format!(
                        "Failed to deserialize batch (JSON): {}",
                        e
                    ))
                })?
            };

            // Calculate batch size on disk
            // 1 byte compression flag + 4 bytes length + data + 4 bytes CRC
            let batch_size = 1 + 4 + batch_len as u64 + 4;

            // Record this batch in the index builder
            if let Some(first_offset) = batch.first_offset() {
                builder.record_batch(first_offset, batch_position, batch_size);
            }

            position += batch_size;
        }

        Ok(builder.build(&index_path))
    }

    /// Validate segment integrity by computing the running CRC.
    ///
    /// This method reads through all batches in the segment, verifies each
    /// batch's individual CRC, and computes the cumulative running CRC.
    /// Returns the computed running CRC which can be compared against
    /// `get_running_crc()` for a segment that's been written to.
    ///
    /// Returns an error if any batch CRC is invalid.
    pub fn validate_segment(&self) -> Result<u32> {
        let mut file = BufReader::new(File::open(&self.path)?);

        // Skip header
        file.seek(SeekFrom::Start(HEADER_SIZE as u64))?;

        let mut position = HEADER_SIZE as u64;
        let mut running_crc: u32 = 0;

        while position < self.write_position {
            // Read compression flag (1 byte)
            let mut compression_buf = [0u8; 1];
            if file.read_exact(&mut compression_buf).is_err() {
                break;
            }

            // Read batch length (4 bytes)
            let mut len_buf = [0u8; 4];
            if file.read_exact(&mut len_buf).is_err() {
                break;
            }
            let batch_len = u32::from_le_bytes(len_buf) as usize;

            // Read batch data
            let mut batch_data = vec![0u8; batch_len];
            file.read_exact(&mut batch_data)?;

            // Read stored CRC (4 bytes)
            let mut crc_buf = [0u8; 4];
            file.read_exact(&mut crc_buf)?;
            let stored_crc = u32::from_le_bytes(crc_buf);

            // Verify batch CRC
            let computed_crc = crc32fast::hash(&batch_data);
            if stored_crc != computed_crc {
                return Err(StreamlineError::CorruptedData(format!(
                    "Batch CRC mismatch at position {}: stored={:#x}, computed={:#x}",
                    position, stored_crc, computed_crc
                )));
            }

            // Chain batch CRC into running CRC
            let mut hasher = crc32fast::Hasher::new_with_initial(running_crc);
            hasher.update(&stored_crc.to_le_bytes());
            running_crc = hasher.finalize();

            // 1 byte compression + 4 bytes length + data + 4 bytes CRC
            position += 1 + 4 + batch_len as u64 + 4;
        }

        Ok(running_crc)
    }

    /// Get the current running CRC hash for this segment.
    ///
    /// This value is only meaningful for segments that have been written to
    /// in the current session. For segments opened from disk, use
    /// `validate_segment()` to compute the CRC from stored data.
    pub fn get_running_crc(&self) -> u32 {
        self.running_crc
    }

    /// Sync segment data to disk
    pub fn sync(&mut self) -> Result<()> {
        if let Some(writer) = self.writer.as_mut() {
            writer.flush()?;
            writer.sync_data()?;
        }
        Ok(())
    }

    /// Get the base offset of this segment
    pub fn base_offset(&self) -> i64 {
        self.header.base_offset
    }

    /// Get the max offset of this segment
    pub fn max_offset(&self) -> i64 {
        self.header.max_offset
    }

    /// Get the record count in this segment
    pub fn record_count(&self) -> u32 {
        self.header.record_count
    }

    /// Check if this segment is sealed
    pub fn is_sealed(&self) -> bool {
        self.sealed
    }

    /// Check if Direct I/O is enabled for this segment
    pub fn is_direct_io_enabled(&self) -> bool {
        self.use_direct_io
    }

    /// Get the path to this segment file
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Get the memory-mapped reader if available
    ///
    /// Returns `Some` if mmap is enabled and the segment was opened with
    /// `open_with_config`. Returns `None` otherwise.
    pub fn mmap_reader(&self) -> Option<&MmapSegmentReader> {
        self.mmap_reader.as_ref()
    }

    /// Check if this segment has a memory-mapped reader
    pub fn has_mmap(&self) -> bool {
        self.mmap_reader.is_some()
    }

    /// Get the current size of this segment in bytes
    pub fn size(&self) -> u64 {
        self.write_position
    }

    /// Get the base (minimum) timestamp in this segment
    pub fn base_timestamp(&self) -> i64 {
        self.header.base_timestamp
    }

    /// Get the maximum timestamp in this segment
    pub fn max_timestamp(&self) -> i64 {
        self.header.max_timestamp
    }

    /// Configure prefetching for this segment
    ///
    /// Enables read-ahead prefetching to improve sequential read performance.
    /// Call this before performing sequential reads (e.g., consumer operations).
    ///
    /// # Arguments
    /// * `config` - Prefetch configuration
    pub fn configure_prefetch(&mut self, config: PrefetchConfig) {
        self.prefetcher = Some(SegmentPrefetcher::new(config));
    }

    /// Enable prefetching with default configuration
    pub fn enable_prefetch(&mut self) {
        self.prefetcher = Some(SegmentPrefetcher::with_defaults());
    }

    /// Disable prefetching
    pub fn disable_prefetch(&mut self) {
        self.prefetcher = None;
    }

    /// Advise the kernel that this segment will be accessed sequentially
    ///
    /// This enables aggressive read-ahead at the OS level.
    /// Best called before starting a sequential read operation.
    pub fn advise_sequential(&self) -> std::io::Result<()> {
        let file = File::open(&self.path)?;
        prefetch::advise_sequential(&file)
    }

    /// Advise the kernel that this segment will be accessed randomly
    ///
    /// This disables read-ahead to avoid wasting I/O bandwidth.
    pub fn advise_random(&self) -> std::io::Result<()> {
        let file = File::open(&self.path)?;
        prefetch::advise_random(&file)
    }

    /// Prefetch a range of data in this segment
    ///
    /// Triggers asynchronous read-ahead for the specified range.
    /// The call returns immediately without blocking.
    ///
    /// # Arguments
    /// * `offset` - Starting file offset (not record offset)
    /// * `len` - Number of bytes to prefetch
    pub fn prefetch_file_range(&self, offset: i64, len: usize) -> std::io::Result<()> {
        let file = File::open(&self.path)?;
        prefetch::prefetch_range(&file, offset, len)
    }

    /// Prefetch data from a record offset
    ///
    /// Uses the index to find the file position for the given record offset,
    /// then prefetches from that position.
    ///
    /// # Arguments
    /// * `record_offset` - The record offset to prefetch from
    /// * `len` - Number of bytes to prefetch (or 0 for default window size)
    pub fn prefetch_from_offset(&self, record_offset: i64, len: usize) -> std::io::Result<()> {
        let file_position = self
            .index
            .as_ref()
            .and_then(|idx| idx.lookup(record_offset))
            .unwrap_or(HEADER_SIZE as u64);

        let prefetch_len = if len == 0 {
            prefetch::DEFAULT_PREFETCH_SIZE
        } else {
            len
        };

        let file = File::open(&self.path)?;
        prefetch::prefetch_range(&file, file_position as i64, prefetch_len)
    }

    /// Release pages from the page cache for a range
    ///
    /// Call this after reading data that won't be needed again soon
    /// to reduce memory pressure.
    ///
    /// # Arguments
    /// * `offset` - Starting file offset
    /// * `len` - Length of range to release (0 means entire file from offset)
    pub fn release_cache(&self, offset: i64, len: usize) -> std::io::Result<()> {
        let file = File::open(&self.path)?;
        prefetch::advise_dontneed(&file, offset, len)
    }

    /// Check if prefetching is enabled for this segment
    pub fn has_prefetch(&self) -> bool {
        self.prefetcher.is_some()
    }

    /// Delete this segment from disk
    ///
    /// This method removes the segment file and its index. The segment should not be used after deletion.
    /// The segment must be sealed before deletion.
    pub fn delete(self) -> Result<()> {
        if !self.sealed {
            return Err(StreamlineError::storage_msg(
                "Cannot delete unsealed segment".to_string(),
            ));
        }

        // Delete the index file if it exists
        if let Some(index) = self.index {
            let _ = index.delete(); // Ignore errors for index deletion
        }

        std::fs::remove_file(&self.path)?;

        Ok(())
    }

    /// Force delete this segment from disk, regardless of sealed state
    ///
    /// This is used during log truncation where we need to remove the active segment.
    /// Use with caution - only for truncation/recovery scenarios.
    pub fn force_delete(self) -> Result<()> {
        // Delete the index file if it exists
        if let Some(index) = self.index {
            let _ = index.delete(); // Ignore errors for index deletion
        }

        std::fs::remove_file(&self.path)?;
        Ok(())
    }
}

/// Generate segment filename from base offset
pub fn segment_filename(base_offset: i64) -> String {
    format!("{:020}.segment", base_offset)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_segment_sync_mode_parse_valid() {
        assert_eq!(
            SegmentSyncMode::parse("none").unwrap(),
            SegmentSyncMode::None
        );
        assert_eq!(
            SegmentSyncMode::parse("on_seal").unwrap(),
            SegmentSyncMode::OnSeal
        );
        assert_eq!(
            SegmentSyncMode::parse("OnSeal").unwrap(),
            SegmentSyncMode::OnSeal
        );
        assert_eq!(
            SegmentSyncMode::parse("interval").unwrap(),
            SegmentSyncMode::Interval
        );
        assert_eq!(
            SegmentSyncMode::parse("every_write").unwrap(),
            SegmentSyncMode::EveryWrite
        );
    }

    #[test]
    fn test_segment_sync_mode_parse_invalid() {
        let err = SegmentSyncMode::parse("invalid_mode").unwrap_err();
        assert!(matches!(err, StreamlineError::Config(_)));
    }

    #[test]
    fn test_segment_header_serialization() {
        let header = SegmentHeader {
            version: 1,
            flags: 0,
            base_offset: 100,
            max_offset: 200,
            base_timestamp: 1234567890,
            max_timestamp: 1234567900,
            record_count: 100,
            crc32: 0,
            compression: 0,
        };

        let bytes = header.to_bytes();
        assert_eq!(bytes.len(), HEADER_SIZE);

        let parsed = SegmentHeader::from_bytes(&bytes).unwrap();
        assert_eq!(parsed.version, 1);
        assert_eq!(parsed.base_offset, 100);
        assert_eq!(parsed.max_offset, 200);
        assert_eq!(parsed.record_count, 100);
    }

    #[test]
    fn test_segment_create_and_read() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.segment");

        // Create segment
        let mut segment = Segment::create(&path, 0).unwrap();

        // Add records
        let record1 = Record::new(0, 1234567890, None, Bytes::from("value 1"));
        let record2 = Record::new(1, 1234567891, None, Bytes::from("value 2"));

        segment.append_record(record1).unwrap();
        segment.append_record(record2).unwrap();

        // Read records back
        let records = segment.read_all().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].offset, 0);
        assert_eq!(records[1].offset, 1);
        assert_eq!(records[0].value, Bytes::from("value 1"));
        assert_eq!(records[1].value, Bytes::from("value 2"));
    }

    #[test]
    fn test_segment_read_from_offset() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.segment");

        let mut segment = Segment::create(&path, 0).unwrap();

        for i in 0..10 {
            let record = Record::new(i, 1234567890 + i, None, Bytes::from(format!("value {}", i)));
            segment.append_record(record).unwrap();
        }

        let records = segment.read_from_offset(5, 100).unwrap();
        assert_eq!(records.len(), 5);
        assert_eq!(records[0].offset, 5);
        assert_eq!(records[4].offset, 9);
    }

    #[test]
    fn test_segment_seal() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.segment");

        let mut segment = Segment::create(&path, 0).unwrap();
        segment
            .append_record(Record::new(0, 1234567890, None, Bytes::from("test")))
            .unwrap();

        assert!(!segment.is_sealed());
        segment.seal().unwrap();
        assert!(segment.is_sealed());

        // Writing to sealed segment should fail
        let result = segment.append_record(Record::new(1, 1234567891, None, Bytes::from("test")));
        assert!(result.is_err());
    }

    #[test]
    fn test_segment_with_lz4_compression() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_lz4.segment");

        let mut segment =
            Segment::create_with_compression(&path, 0, CompressionCodec::Lz4).unwrap();

        // Write some records
        for i in 0..10 {
            let record = Record::new(i, 1234567890 + i, None, Bytes::from(format!("value {}", i)));
            segment.append_record(record).unwrap();
        }

        // Read them back
        let records = segment.read_all().unwrap();
        assert_eq!(records.len(), 10);
        assert_eq!(records[0].value, Bytes::from("value 0"));
        assert_eq!(records[9].value, Bytes::from("value 9"));
    }

    #[test]
    fn test_segment_with_zstd_compression() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_zstd.segment");

        let mut segment =
            Segment::create_with_compression(&path, 0, CompressionCodec::Zstd).unwrap();

        // Write some records
        for i in 0..10 {
            let record = Record::new(i, 1234567890 + i, None, Bytes::from(format!("value {}", i)));
            segment.append_record(record).unwrap();
        }

        // Read them back
        let records = segment.read_all().unwrap();
        assert_eq!(records.len(), 10);
        assert_eq!(records[0].value, Bytes::from("value 0"));
        assert_eq!(records[9].value, Bytes::from("value 9"));
    }

    #[test]
    fn test_segment_with_snappy_compression() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_snappy.segment");

        let mut segment =
            Segment::create_with_compression(&path, 0, CompressionCodec::Snappy).unwrap();

        // Write some records
        for i in 0..10 {
            let record = Record::new(i, 1234567890 + i, None, Bytes::from(format!("value {}", i)));
            segment.append_record(record).unwrap();
        }

        // Read them back
        let records = segment.read_all().unwrap();
        assert_eq!(records.len(), 10);
        assert_eq!(records[0].value, Bytes::from("value 0"));
        assert_eq!(records[9].value, Bytes::from("value 9"));
    }

    #[test]
    fn test_segment_compression_reopen() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_reopen.segment");

        // Create segment with compression and write data
        {
            let mut segment =
                Segment::create_with_compression(&path, 0, CompressionCodec::Lz4).unwrap();

            for i in 0..5 {
                let record =
                    Record::new(i, 1234567890 + i, None, Bytes::from(format!("value {}", i)));
                segment.append_record(record).unwrap();
            }
            segment.seal().unwrap();
        }

        // Reopen and verify data
        let segment = Segment::open(&path).unwrap();
        let records = segment.read_all().unwrap();
        assert_eq!(records.len(), 5);
        assert_eq!(records[0].value, Bytes::from("value 0"));
        assert_eq!(records[4].value, Bytes::from("value 4"));
    }

    #[test]
    fn test_segment_index_creation() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_index.segment");
        let index_path = index_path_for_segment(&path);

        // Create segment and write records
        {
            let mut segment = Segment::create(&path, 0).unwrap();

            for i in 0..100 {
                let record =
                    Record::new(i, 1234567890 + i, None, Bytes::from(format!("value {}", i)));
                segment.append_record(record).unwrap();
            }

            // Seal should save the index
            segment.seal().unwrap();
        }

        // Verify index file was created
        assert!(index_path.exists());

        // Reopen segment and verify index is loaded
        let segment = Segment::open(&path).unwrap();
        assert!(segment.index.is_some());

        // Test that read_from_offset works correctly
        let records = segment.read_from_offset(50, 10).unwrap();
        assert_eq!(records.len(), 10);
        assert_eq!(records[0].offset, 50);
        assert_eq!(records[9].offset, 59);
    }

    #[test]
    fn test_segment_index_build_on_open() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_index_build.segment");
        let index_path = index_path_for_segment(&path);

        // Create segment and write records without sealing (no index saved)
        {
            let mut segment = Segment::create(&path, 0).unwrap();

            for i in 0..50 {
                let record =
                    Record::new(i, 1234567890 + i, None, Bytes::from(format!("value {}", i)));
                segment.append_record(record).unwrap();
            }

            // Don't seal - just sync and drop
            segment.sync().unwrap();
        }

        // Remove index file if it exists (shouldn't, but just in case)
        let _ = std::fs::remove_file(&index_path);
        assert!(!index_path.exists());

        // Open segment - should build index
        let segment = Segment::open(&path).unwrap();
        assert!(segment.index.is_some());

        // Index file should now exist (built and saved)
        assert!(index_path.exists());

        // Verify read_from_offset works with built index
        let records = segment.read_from_offset(25, 10).unwrap();
        assert_eq!(records.len(), 10);
        assert_eq!(records[0].offset, 25);
    }

    #[test]
    fn test_segment_delete_removes_index() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_delete_index.segment");
        let index_path = index_path_for_segment(&path);

        // Create segment, write records, seal
        {
            let mut segment = Segment::create(&path, 0).unwrap();
            segment
                .append_record(Record::new(0, 1234567890, None, Bytes::from("test")))
                .unwrap();
            segment.seal().unwrap();
        }

        assert!(path.exists());
        assert!(index_path.exists());

        // Open and delete
        let segment = Segment::open(&path).unwrap();
        segment.delete().unwrap();

        // Both files should be gone
        assert!(!path.exists());
        assert!(!index_path.exists());
    }

    #[test]
    fn test_segment_running_crc() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_running_crc.segment");

        // Create segment and write records
        let mut segment = Segment::create(&path, 0).unwrap();

        // Initial running CRC should be 0
        assert_eq!(segment.get_running_crc(), 0);

        // Write some records
        for i in 0..5 {
            let record = Record::new(i, 1234567890 + i, None, Bytes::from(format!("value {}", i)));
            segment.append_record(record).unwrap();
        }

        // Running CRC should be non-zero after writes
        let crc_after_writes = segment.get_running_crc();
        assert_ne!(crc_after_writes, 0);

        // Validate segment should return the same CRC
        let validated_crc = segment.validate_segment().unwrap();
        assert_eq!(crc_after_writes, validated_crc);
    }

    #[test]
    fn test_segment_running_crc_incremental() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_running_crc_incremental.segment");

        let mut segment = Segment::create(&path, 0).unwrap();

        // Write first batch
        segment
            .append_record(Record::new(0, 1234567890, None, Bytes::from("value 0")))
            .unwrap();
        let crc_after_first = segment.get_running_crc();

        // Write second batch
        segment
            .append_record(Record::new(1, 1234567891, None, Bytes::from("value 1")))
            .unwrap();
        let crc_after_second = segment.get_running_crc();

        // CRC should change after each write
        assert_ne!(crc_after_first, crc_after_second);

        // Validate should match
        let validated_crc = segment.validate_segment().unwrap();
        assert_eq!(crc_after_second, validated_crc);
    }

    #[test]
    fn test_segment_validate_catches_corruption() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_corrupt.segment");

        // Create segment and write records
        {
            let mut segment = Segment::create(&path, 0).unwrap();
            for i in 0..3 {
                let record =
                    Record::new(i, 1234567890 + i, None, Bytes::from(format!("value {}", i)));
                segment.append_record(record).unwrap();
            }
            segment.seal().unwrap();
        }

        // Corrupt the segment file by modifying some bytes
        {
            use std::fs::OpenOptions;
            use std::io::Write;

            let mut file = OpenOptions::new().write(true).open(&path).unwrap();
            // Seek past header (64 bytes) + compression (1) + length (4)
            file.seek(SeekFrom::Start(69)).unwrap();
            // Write garbage to corrupt the batch data
            file.write_all(&[0xFF, 0xFF, 0xFF, 0xFF]).unwrap();
        }

        // Validation should fail
        let segment = Segment::open(&path).unwrap();
        let result = segment.validate_segment();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Batch CRC mismatch"));
    }
}
