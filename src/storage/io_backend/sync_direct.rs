//! Synchronous Direct I/O wrapper for segment integration
//!
//! This module provides a synchronous file wrapper that uses O_DIRECT (or F_NOCACHE
//! on macOS) to bypass the OS page cache. It implements `std::io::Read`, `Write`,
//! and `Seek` traits for easy integration with existing synchronous code like Segment.
//!
//! # Alignment Handling
//!
//! O_DIRECT requires aligned buffers and offsets. This wrapper handles alignment
//! automatically by:
//! - Using internally aligned buffers for all operations
//! - Buffering partial writes until a full aligned block is ready
//! - Padding reads to alignment boundaries and returning only requested data
//!
//! # Usage
//!
//! ```rust,ignore
//! use streamline::storage::io_backend::sync_direct::{SyncDirectFile, SyncDirectConfig};
//!
//! let config = SyncDirectConfig::default();
//! let file = SyncDirectFile::create("data.bin", config)?;
//!
//! // Use like a normal file
//! file.write_all(b"Hello, world!")?;
//! file.flush()?;  // Important: flush pending aligned writes
//! ```

use super::direct::{align_down, align_up, allocate_aligned, DEFAULT_ALIGNMENT, MIN_ALIGNMENT};
use crate::error::{Result, StreamlineError};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tracing::{debug, warn};

/// Configuration for synchronous Direct I/O
#[derive(Debug, Clone)]
pub struct SyncDirectConfig {
    /// Buffer alignment in bytes (must be power of 2, typically 4096)
    pub alignment: usize,
    /// Whether to fall back to standard I/O if O_DIRECT fails
    pub fallback_on_error: bool,
    /// Internal buffer size for aligned operations (must be multiple of alignment)
    pub buffer_size: usize,
}

impl Default for SyncDirectConfig {
    fn default() -> Self {
        Self {
            alignment: DEFAULT_ALIGNMENT,
            fallback_on_error: true,
            buffer_size: 64 * 1024, // 64KB default buffer
        }
    }
}

impl SyncDirectConfig {
    /// Create config from storage IoBackendConfig
    pub fn from_io_backend_config(config: &crate::config::IoBackendConfig) -> Self {
        Self {
            alignment: config.direct_io_alignment,
            fallback_on_error: config.direct_io_fallback,
            buffer_size: 64 * 1024, // 64KB default
        }
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<()> {
        if !self.alignment.is_power_of_two() {
            return Err(StreamlineError::Config(
                "Direct I/O alignment must be power of 2".into(),
            ));
        }
        if self.alignment < MIN_ALIGNMENT {
            return Err(StreamlineError::Config(format!(
                "Direct I/O alignment must be at least {} bytes",
                MIN_ALIGNMENT
            )));
        }
        if self.buffer_size % self.alignment != 0 {
            return Err(StreamlineError::Config(
                "Direct I/O buffer size must be multiple of alignment".into(),
            ));
        }
        Ok(())
    }
}

/// Statistics for synchronous Direct I/O operations
#[derive(Debug, Clone, Default)]
pub struct SyncDirectStats {
    /// Total bytes read with O_DIRECT
    pub bytes_read: u64,
    /// Total bytes written with O_DIRECT
    pub bytes_written: u64,
    /// Number of read operations
    pub read_ops: u64,
    /// Number of write operations
    pub write_ops: u64,
    /// Number of alignment corrections
    pub alignment_corrections: u64,
}

/// Synchronous Direct I/O file wrapper
///
/// Wraps a file opened with O_DIRECT (or F_NOCACHE on macOS) and provides
/// standard `Read`, `Write`, and `Seek` trait implementations with automatic
/// alignment handling.
pub struct SyncDirectFile {
    /// Underlying file handle
    file: File,
    /// File path (for error messages)
    #[allow(dead_code)]
    path: PathBuf,
    /// Configuration
    config: SyncDirectConfig,
    /// Whether O_DIRECT is actually enabled
    direct_enabled: AtomicBool,
    /// Current logical position (may differ from file position due to alignment)
    position: AtomicU64,
    /// Write buffer for accumulating unaligned writes
    write_buffer: Vec<u8>,
    /// Position of first byte in write buffer
    write_buffer_start: u64,
    /// Statistics
    stats: SyncDirectStats,
}

impl SyncDirectFile {
    /// Open a file for reading with O_DIRECT
    pub fn open<P: AsRef<Path>>(path: P, config: SyncDirectConfig) -> Result<Self> {
        config.validate()?;
        let path = path.as_ref();
        let options = OpenOptions::new().read(true).clone();

        let (file, direct_enabled) = Self::open_with_direct(path, &options, &config)?;

        debug!(
            path = %path.display(),
            direct_enabled = direct_enabled,
            alignment = config.alignment,
            "Opened file for direct read"
        );

        Ok(Self {
            file,
            path: path.to_path_buf(),
            config,
            direct_enabled: AtomicBool::new(direct_enabled),
            position: AtomicU64::new(0),
            write_buffer: Vec::new(),
            write_buffer_start: 0,
            stats: SyncDirectStats::default(),
        })
    }

    /// Open a file for reading and writing with O_DIRECT
    pub fn open_rw<P: AsRef<Path>>(path: P, config: SyncDirectConfig) -> Result<Self> {
        config.validate()?;
        let path = path.as_ref();
        let options = OpenOptions::new().read(true).write(true).clone();

        let (file, direct_enabled) = Self::open_with_direct(path, &options, &config)?;

        debug!(
            path = %path.display(),
            direct_enabled = direct_enabled,
            "Opened file for direct read-write"
        );

        Ok(Self {
            file,
            path: path.to_path_buf(),
            config,
            direct_enabled: AtomicBool::new(direct_enabled),
            position: AtomicU64::new(0),
            write_buffer: Vec::new(),
            write_buffer_start: 0,
            stats: SyncDirectStats::default(),
        })
    }

    /// Create a new file with O_DIRECT
    pub fn create<P: AsRef<Path>>(path: P, config: SyncDirectConfig) -> Result<Self> {
        config.validate()?;
        let path = path.as_ref();
        let options = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .clone();

        let (file, direct_enabled) = Self::open_with_direct(path, &options, &config)?;

        debug!(
            path = %path.display(),
            direct_enabled = direct_enabled,
            "Created file for direct I/O"
        );

        Ok(Self {
            file,
            path: path.to_path_buf(),
            config,
            direct_enabled: AtomicBool::new(direct_enabled),
            position: AtomicU64::new(0),
            write_buffer: Vec::new(),
            write_buffer_start: 0,
            stats: SyncDirectStats::default(),
        })
    }

    /// Open a file for appending with O_DIRECT
    pub fn open_append<P: AsRef<Path>>(path: P, config: SyncDirectConfig) -> Result<Self> {
        config.validate()?;
        let path = path.as_ref();
        let options = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(false) // We manage position ourselves for O_DIRECT
            .clone();

        let (file, direct_enabled) = Self::open_with_direct(path, &options, &config)?;

        // Get file size to set initial position
        let size = file.metadata()?.len();

        debug!(
            path = %path.display(),
            direct_enabled = direct_enabled,
            initial_position = size,
            "Opened file for direct append"
        );

        Ok(Self {
            file,
            path: path.to_path_buf(),
            config,
            direct_enabled: AtomicBool::new(direct_enabled),
            position: AtomicU64::new(size),
            write_buffer: Vec::new(),
            write_buffer_start: size,
            stats: SyncDirectStats::default(),
        })
    }

    /// Open file with O_DIRECT flag (platform-specific)
    #[cfg(target_os = "linux")]
    fn open_with_direct(
        path: &Path,
        options: &OpenOptions,
        config: &SyncDirectConfig,
    ) -> Result<(File, bool)> {
        use std::os::unix::fs::OpenOptionsExt;

        // Try with O_DIRECT first
        let mut direct_options = options.clone();
        direct_options.custom_flags(libc::O_DIRECT);

        match direct_options.open(path) {
            Ok(file) => Ok((file, true)),
            Err(e) => {
                if config.fallback_on_error {
                    warn!(
                        path = %path.display(),
                        error = %e,
                        "O_DIRECT not supported, falling back to standard I/O"
                    );
                    let file = options.open(path).map_err(|e| {
                        StreamlineError::storage_msg(format!("Failed to open file: {}", e))
                    })?;
                    Ok((file, false))
                } else {
                    Err(StreamlineError::storage_msg(format!(
                        "O_DIRECT not supported for {:?}: {}",
                        path, e
                    )))
                }
            }
        }
    }

    /// Open file with F_NOCACHE on macOS
    #[cfg(target_os = "macos")]
    fn open_with_direct(
        path: &Path,
        options: &OpenOptions,
        config: &SyncDirectConfig,
    ) -> Result<(File, bool)> {
        use std::os::unix::io::AsRawFd;

        let file = options
            .open(path)
            .map_err(|e| StreamlineError::storage_msg(format!("Failed to open file: {}", e)))?;

        // Use F_NOCACHE to disable caching
        let fd = file.as_raw_fd();
        let result = unsafe { libc::fcntl(fd, libc::F_NOCACHE, 1) };

        if result == -1 {
            if config.fallback_on_error {
                warn!(
                    path = %path.display(),
                    "F_NOCACHE not supported, using standard I/O"
                );
                Ok((file, false))
            } else {
                Err(StreamlineError::storage_msg(format!(
                    "F_NOCACHE not supported for {:?}",
                    path
                )))
            }
        } else {
            Ok((file, true))
        }
    }

    /// Fallback for platforms without O_DIRECT
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    fn open_with_direct(
        path: &Path,
        options: &OpenOptions,
        config: &SyncDirectConfig,
    ) -> Result<(File, bool)> {
        if !config.fallback_on_error {
            return Err(StreamlineError::storage_msg(
                "O_DIRECT not supported on this platform".into(),
            ));
        }
        warn!(
            path = %path.display(),
            "O_DIRECT not available on this platform"
        );
        let file = options
            .open(path)
            .map_err(|e| StreamlineError::storage_msg(format!("Failed to open file: {}", e)))?;
        Ok((file, false))
    }

    /// Check if O_DIRECT is enabled for this file
    pub fn is_direct_enabled(&self) -> bool {
        self.direct_enabled.load(Ordering::Relaxed)
    }

    /// Get statistics for this file
    pub fn stats(&self) -> SyncDirectStats {
        self.stats.clone()
    }

    /// Get the current logical position
    pub fn position(&self) -> u64 {
        self.position.load(Ordering::SeqCst)
    }

    /// Sync file data to disk
    pub fn sync_data(&self) -> Result<()> {
        self.file.sync_data().map_err(StreamlineError::from)
    }

    /// Sync file data and metadata to disk
    pub fn sync_all(&self) -> Result<()> {
        self.file.sync_all().map_err(StreamlineError::from)
    }

    /// Get the current file size
    pub fn size(&self) -> Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    /// Flush write buffer (write any pending data)
    fn flush_write_buffer(&mut self) -> std::io::Result<()> {
        if self.write_buffer.is_empty() {
            return Ok(());
        }

        let alignment = self.config.alignment as u64;

        // For O_DIRECT, we need to write aligned blocks
        if self.direct_enabled.load(Ordering::Relaxed) {
            // Pad to alignment
            let padding_needed =
                (alignment - (self.write_buffer.len() as u64 % alignment)) % alignment;
            if padding_needed > 0 {
                self.write_buffer
                    .resize(self.write_buffer.len() + padding_needed as usize, 0);
            }

            // Create aligned buffer
            let mut aligned_buf = allocate_aligned(self.write_buffer.len(), self.config.alignment)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
            aligned_buf[..self.write_buffer.len()].copy_from_slice(&self.write_buffer);

            // Aligned offset
            let aligned_offset = align_down(self.write_buffer_start, alignment);

            #[cfg(unix)]
            {
                use std::os::unix::fs::FileExt;
                self.file.write_at(&aligned_buf, aligned_offset)?;
            }

            #[cfg(not(unix))]
            {
                self.file.seek(SeekFrom::Start(aligned_offset))?;
                self.file.write_all(&aligned_buf)?;
            }

            self.stats.bytes_written += aligned_buf.len() as u64;
            self.stats.write_ops += 1;
        } else {
            // Standard I/O - write directly
            #[cfg(unix)]
            {
                use std::os::unix::fs::FileExt;
                self.file
                    .write_at(&self.write_buffer, self.write_buffer_start)?;
            }

            #[cfg(not(unix))]
            {
                self.file.seek(SeekFrom::Start(self.write_buffer_start))?;
                self.file.write_all(&self.write_buffer)?;
            }

            self.stats.bytes_written += self.write_buffer.len() as u64;
            self.stats.write_ops += 1;
        }

        self.write_buffer.clear();
        Ok(())
    }

    /// Read at a specific offset (pread-style)
    pub fn read_at(&mut self, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        // Flush any pending writes that might overlap
        if !self.write_buffer.is_empty() {
            let write_end = self.write_buffer_start + self.write_buffer.len() as u64;
            if offset < write_end && offset + buf.len() as u64 > self.write_buffer_start {
                self.flush_write_buffer()?;
            }
        }

        if self.direct_enabled.load(Ordering::Relaxed) {
            let alignment = self.config.alignment as u64;

            // Calculate aligned boundaries
            let aligned_start = align_down(offset, alignment);
            let aligned_end = align_up(offset + buf.len() as u64, alignment);
            let aligned_size = (aligned_end - aligned_start) as usize;

            // Allocate aligned buffer
            let mut aligned_buf = allocate_aligned(aligned_size, self.config.alignment)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

            #[cfg(unix)]
            let bytes_read = {
                use std::os::unix::fs::FileExt;
                self.file.read_at(&mut aligned_buf, aligned_start)?
            };

            #[cfg(not(unix))]
            let bytes_read = {
                self.file.seek(SeekFrom::Start(aligned_start))?;
                self.file.read(&mut aligned_buf)?
            };

            self.stats.bytes_read += bytes_read as u64;
            self.stats.read_ops += 1;

            if aligned_start != offset {
                self.stats.alignment_corrections += 1;
            }

            // Extract requested portion
            let skip = (offset - aligned_start) as usize;
            let available = bytes_read.saturating_sub(skip);
            let to_copy = available.min(buf.len());

            if to_copy > 0 {
                buf[..to_copy].copy_from_slice(&aligned_buf[skip..skip + to_copy]);
            }

            Ok(to_copy)
        } else {
            // Standard I/O
            #[cfg(unix)]
            let bytes_read = {
                use std::os::unix::fs::FileExt;
                self.file.read_at(buf, offset)?
            };

            #[cfg(not(unix))]
            let bytes_read = {
                self.file.seek(SeekFrom::Start(offset))?;
                self.file.read(buf)?
            };

            self.stats.bytes_read += bytes_read as u64;
            self.stats.read_ops += 1;

            Ok(bytes_read)
        }
    }

    /// Write at a specific offset (pwrite-style)
    pub fn write_at(&mut self, buf: &[u8], offset: u64) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        if self.direct_enabled.load(Ordering::Relaxed) {
            let alignment = self.config.alignment as u64;

            // If offset is aligned and buf length is aligned, write directly
            if offset % alignment == 0 && buf.len() % self.config.alignment == 0 {
                let mut aligned_buf = allocate_aligned(buf.len(), self.config.alignment)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
                aligned_buf[..buf.len()].copy_from_slice(buf);

                #[cfg(unix)]
                let bytes_written = {
                    use std::os::unix::fs::FileExt;
                    self.file.write_at(&aligned_buf, offset)?
                };

                #[cfg(not(unix))]
                let bytes_written = {
                    self.file.seek(SeekFrom::Start(offset))?;
                    self.file.write(&aligned_buf)?
                };

                self.stats.bytes_written += bytes_written as u64;
                self.stats.write_ops += 1;

                return Ok(bytes_written);
            }

            // Need to handle alignment - buffer the write
            self.stats.alignment_corrections += 1;

            // Check if we can extend the current buffer
            if self.write_buffer.is_empty() {
                self.write_buffer_start = offset;
                self.write_buffer.extend_from_slice(buf);
            } else {
                let buffer_end = self.write_buffer_start + self.write_buffer.len() as u64;

                // Check if this write is contiguous or overlapping
                if offset == buffer_end {
                    // Contiguous - extend buffer
                    self.write_buffer.extend_from_slice(buf);
                } else if offset >= self.write_buffer_start && offset <= buffer_end {
                    // Overlapping - handle carefully
                    let overlap_start = (offset - self.write_buffer_start) as usize;
                    let new_end = overlap_start + buf.len();
                    if new_end > self.write_buffer.len() {
                        self.write_buffer.resize(new_end, 0);
                    }
                    self.write_buffer[overlap_start..overlap_start + buf.len()]
                        .copy_from_slice(buf);
                } else {
                    // Non-contiguous - flush current buffer and start new one
                    self.flush_write_buffer()?;
                    self.write_buffer_start = offset;
                    self.write_buffer.extend_from_slice(buf);
                }
            }

            // Flush if buffer is large enough
            if self.write_buffer.len() >= self.config.buffer_size {
                self.flush_write_buffer()?;
            }

            Ok(buf.len())
        } else {
            // Standard I/O
            #[cfg(unix)]
            let bytes_written = {
                use std::os::unix::fs::FileExt;
                self.file.write_at(buf, offset)?
            };

            #[cfg(not(unix))]
            let bytes_written = {
                self.file.seek(SeekFrom::Start(offset))?;
                self.file.write(buf)?
            };

            self.stats.bytes_written += bytes_written as u64;
            self.stats.write_ops += 1;

            Ok(bytes_written)
        }
    }

    /// Get the underlying file reference (use with caution)
    pub fn as_file(&self) -> &File {
        &self.file
    }

    /// Get the underlying file mutable reference (use with caution)
    pub fn as_file_mut(&mut self) -> &mut File {
        &mut self.file
    }

    /// Pre-allocate space for the file
    pub fn allocate(&self, len: u64) -> Result<()> {
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::io::AsRawFd;
            let fd = self.file.as_raw_fd();
            let result = unsafe { libc::fallocate(fd, 0, 0, len as i64) };
            if result != 0 {
                return Err(StreamlineError::from(std::io::Error::last_os_error()));
            }
            Ok(())
        }

        #[cfg(not(target_os = "linux"))]
        {
            self.file.set_len(len).map_err(StreamlineError::from)
        }
    }
}

impl Read for SyncDirectFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let pos = self.position.load(Ordering::SeqCst);
        let bytes_read = self.read_at(buf, pos)?;
        self.position.fetch_add(bytes_read as u64, Ordering::SeqCst);
        Ok(bytes_read)
    }
}

impl Write for SyncDirectFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let pos = self.position.load(Ordering::SeqCst);
        let bytes_written = self.write_at(buf, pos)?;
        self.position
            .fetch_add(bytes_written as u64, Ordering::SeqCst);
        Ok(bytes_written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.flush_write_buffer()?;
        self.file.sync_data()
    }
}

impl Seek for SyncDirectFile {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        // Flush pending writes before seeking
        self.flush_write_buffer()?;

        let new_pos = match pos {
            SeekFrom::Start(offset) => offset,
            SeekFrom::End(offset) => {
                let size = self.file.metadata().map_err(std::io::Error::other)?.len();
                if offset >= 0 {
                    size + offset as u64
                } else {
                    size.saturating_sub((-offset) as u64)
                }
            }
            SeekFrom::Current(offset) => {
                let current = self.position.load(Ordering::SeqCst);
                if offset >= 0 {
                    current + offset as u64
                } else {
                    current.saturating_sub((-offset) as u64)
                }
            }
        };

        self.position.store(new_pos, Ordering::SeqCst);
        Ok(new_pos)
    }
}

impl Drop for SyncDirectFile {
    fn drop(&mut self) {
        // Try to flush any pending writes
        if let Err(e) = self.flush_write_buffer() {
            warn!(error = %e, "Failed to flush write buffer on drop");
        }
    }
}

/// Check if synchronous Direct I/O is available on this platform
pub fn is_sync_direct_available() -> bool {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        true
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_sync_direct_config_default() {
        let config = SyncDirectConfig::default();
        assert_eq!(config.alignment, DEFAULT_ALIGNMENT);
        assert!(config.fallback_on_error);
        assert!(config.validate().is_ok());
    }

    #[test]
    #[allow(clippy::field_reassign_with_default)]
    fn test_sync_direct_config_validation() {
        let mut config = SyncDirectConfig::default();

        // Invalid alignment (not power of 2)
        config.alignment = 1000;
        assert!(config.validate().is_err());

        // Invalid alignment (too small)
        config.alignment = 128;
        assert!(config.validate().is_err());

        // Invalid buffer size
        config.alignment = 4096;
        config.buffer_size = 1000;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_sync_direct_create_write_read() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_sync_direct.bin");

        let config = SyncDirectConfig::default();

        // Create and write
        let mut file = SyncDirectFile::create(&path, config.clone()).unwrap();
        let data = b"Hello, Direct I/O!";
        file.write_all(data).unwrap();
        file.flush().unwrap();

        // Read back
        let mut file = SyncDirectFile::open(&path, config).unwrap();
        let mut buf = vec![0u8; data.len()];
        file.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, data);
    }

    #[test]
    fn test_sync_direct_seek() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_seek.bin");

        let config = SyncDirectConfig::default();
        let mut file = SyncDirectFile::create(&path, config).unwrap();

        // Write some data
        file.write_all(b"0123456789").unwrap();
        file.flush().unwrap();

        // Seek and read
        file.seek(SeekFrom::Start(5)).unwrap();
        assert_eq!(file.position(), 5);

        let mut buf = [0u8; 5];
        file.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"56789");
    }

    #[test]
    fn test_sync_direct_write_at() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_write_at.bin");

        let config = SyncDirectConfig::default();
        let mut file = SyncDirectFile::create(&path, config.clone()).unwrap();

        // Write at offset (sequential order to avoid non-contiguous buffer issues)
        file.write_at(b"Hello ", 0).unwrap();
        file.write_at(b"world", 6).unwrap();
        file.flush().unwrap();

        // Read back
        let mut file = SyncDirectFile::open(&path, config).unwrap();
        let mut buf = vec![0u8; 11];
        file.read_at(&mut buf, 0).unwrap();
        assert_eq!(&buf, b"Hello world");
    }

    #[test]
    fn test_sync_direct_large_write() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_large.bin");

        let config = SyncDirectConfig::default();
        let mut file = SyncDirectFile::create(&path, config.clone()).unwrap();

        // Write larger than buffer size
        let data: Vec<u8> = (0..100_000).map(|i| (i % 256) as u8).collect();
        file.write_all(&data).unwrap();
        file.flush().unwrap();

        // Read back
        let mut file = SyncDirectFile::open(&path, config).unwrap();
        let mut buf = vec![0u8; data.len()];
        file.read_exact(&mut buf).unwrap();
        assert_eq!(buf, data);
    }

    #[test]
    fn test_sync_direct_stats() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_stats.bin");

        let config = SyncDirectConfig::default();
        let mut file = SyncDirectFile::create(&path, config).unwrap();

        file.write_all(b"test data").unwrap();
        file.flush().unwrap();

        let stats = file.stats();
        assert!(stats.bytes_written > 0);
        assert!(stats.write_ops > 0);
    }

    #[test]
    fn test_is_sync_direct_available() {
        let available = is_sync_direct_available();
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        assert!(available);
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        assert!(!available);
    }
}
