//! O_DIRECT I/O backend for bypassing OS page cache
//!
//! This backend uses O_DIRECT to bypass the operating system's page cache,
//! providing predictable memory usage and reduced double-buffering. This is
//! a key performance technique used by Redpanda for high-throughput streaming.
//!
//! # Requirements
//!
//! - Linux: O_DIRECT flag is fully supported
//! - macOS: Uses F_NOCACHE fcntl instead (similar effect)
//! - Other platforms: Falls back to standard I/O
//!
//! # Alignment Requirements
//!
//! O_DIRECT requires all I/O operations to be aligned:
//! - Buffer address must be aligned (typically 512 bytes or 4KB)
//! - Offset must be aligned to the filesystem block size
//! - Length must be a multiple of the filesystem block size
//!
//! This module handles alignment automatically using aligned buffer allocation.

use super::{AsyncFile, AsyncFileSystem, IoResult};
use crate::error::{Result, StreamlineError};
use async_trait::async_trait;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, warn};

/// Default alignment for O_DIRECT operations (4KB - typical page size)
pub const DEFAULT_ALIGNMENT: usize = 4096;

/// Minimum alignment (512 bytes - sector size)
pub const MIN_ALIGNMENT: usize = 512;

/// Configuration for Direct I/O backend
#[derive(Debug, Clone)]
pub struct DirectIoConfig {
    /// Buffer alignment in bytes (must be power of 2, typically 4096)
    pub alignment: usize,
    /// Whether to fall back to standard I/O if O_DIRECT fails
    pub fallback_on_error: bool,
    /// Enable O_DIRECT for reads
    pub direct_reads: bool,
    /// Enable O_DIRECT for writes
    pub direct_writes: bool,
}

impl Default for DirectIoConfig {
    fn default() -> Self {
        Self {
            alignment: DEFAULT_ALIGNMENT,
            fallback_on_error: true,
            direct_reads: true,
            direct_writes: true,
        }
    }
}

/// Statistics for Direct I/O operations
#[derive(Debug, Clone, Default)]
pub struct DirectIoStats {
    /// Total bytes read with O_DIRECT
    pub direct_reads_bytes: u64,
    /// Total bytes written with O_DIRECT
    pub direct_writes_bytes: u64,
    /// Number of fallback operations (when O_DIRECT failed)
    pub fallback_operations: u64,
    /// Number of alignment corrections performed
    pub alignment_corrections: u64,
}

/// Allocate an aligned buffer for O_DIRECT operations
///
/// # Arguments
/// * `size` - Minimum size of the buffer
/// * `alignment` - Required alignment (must be power of 2)
///
/// # Returns
/// An aligned Vec<u8> with capacity rounded up to alignment
#[inline]
pub fn allocate_aligned(size: usize, alignment: usize) -> Result<Vec<u8>> {
    debug_assert!(alignment.is_power_of_two(), "alignment must be power of 2");

    // Round up size to alignment
    let aligned_size = (size + alignment - 1) & !(alignment - 1);

    // Use Layout for aligned allocation
    let layout = std::alloc::Layout::from_size_align(aligned_size, alignment)
        .map_err(|e| StreamlineError::Storage(format!("Invalid layout for aligned allocation (size={}, align={}): {}", aligned_size, alignment, e)))?;

    // SAFETY: We're creating a properly aligned buffer and initializing it
    unsafe {
        let ptr = std::alloc::alloc_zeroed(layout);
        if ptr.is_null() {
            std::alloc::handle_alloc_error(layout);
        }
        Ok(Vec::from_raw_parts(ptr, aligned_size, aligned_size))
    }
}

/// Check if a buffer is properly aligned
#[inline]
pub fn is_aligned(buf: &[u8], alignment: usize) -> bool {
    (buf.as_ptr() as usize) % alignment == 0
}

/// Round up a value to the nearest alignment boundary
#[inline]
pub fn align_up(value: u64, alignment: u64) -> u64 {
    (value + alignment - 1) & !(alignment - 1)
}

/// Round down a value to the nearest alignment boundary
#[inline]
pub fn align_down(value: u64, alignment: u64) -> u64 {
    value & !(alignment - 1)
}

/// Direct I/O file implementation
///
/// Wraps a file opened with O_DIRECT (or equivalent) for bypassing page cache.
pub struct DirectFile {
    /// The underlying file
    file: Arc<Mutex<File>>,
    /// File path (for error messages)
    #[allow(dead_code)]
    path: PathBuf,
    /// Configuration
    config: DirectIoConfig,
    /// Whether O_DIRECT is actually enabled (may be false on unsupported FS)
    direct_enabled: AtomicBool,
    /// Statistics
    stats: Arc<DirectFileStats>,
}

struct DirectFileStats {
    direct_read_bytes: AtomicU64,
    direct_write_bytes: AtomicU64,
    #[allow(dead_code)]
    fallback_ops: AtomicU64,
}

impl Default for DirectFileStats {
    fn default() -> Self {
        Self {
            direct_read_bytes: AtomicU64::new(0),
            direct_write_bytes: AtomicU64::new(0),
            fallback_ops: AtomicU64::new(0),
        }
    }
}

impl DirectFile {
    /// Create a new DirectFile
    pub fn new(file: File, path: PathBuf, config: DirectIoConfig, direct_enabled: bool) -> Self {
        Self {
            file: Arc::new(Mutex::new(file)),
            path,
            config,
            direct_enabled: AtomicBool::new(direct_enabled),
            stats: Arc::new(DirectFileStats::default()),
        }
    }

    /// Check if O_DIRECT is enabled for this file
    pub fn is_direct_enabled(&self) -> bool {
        self.direct_enabled.load(Ordering::Relaxed)
    }

    /// Get statistics for this file
    pub fn stats(&self) -> DirectIoStats {
        DirectIoStats {
            direct_reads_bytes: self.stats.direct_read_bytes.load(Ordering::Relaxed),
            direct_writes_bytes: self.stats.direct_write_bytes.load(Ordering::Relaxed),
            fallback_operations: 0,
            alignment_corrections: 0,
        }
    }

    /// Ensure buffer is properly aligned, reallocating if necessary
    fn ensure_aligned(&self, buf: Vec<u8>) -> Vec<u8> {
        if is_aligned(&buf, self.config.alignment) {
            buf
        } else {
            // Need to reallocate with proper alignment
            match allocate_aligned(buf.len(), self.config.alignment) {
                Ok(mut aligned) => {
                    aligned[..buf.len()].copy_from_slice(&buf);
                    aligned
                }
                Err(e) => {
                    warn!("Failed to allocate aligned buffer, using unaligned: {}", e);
                    buf
                }
            }
        }
    }
}

#[async_trait]
impl AsyncFile for DirectFile {
    async fn read_at(&self, buf: Vec<u8>, offset: u64) -> IoResult<usize> {
        let file = self.file.clone();
        let alignment = self.config.alignment as u64;
        let direct_enabled = self.direct_enabled.load(Ordering::Relaxed);
        let stats = self.stats.clone();

        // Ensure buffer is aligned for O_DIRECT
        let mut buf = if direct_enabled {
            self.ensure_aligned(buf)
        } else {
            buf
        };

        let result = tokio::task::spawn_blocking(move || {
            let file = file.blocking_lock();

            #[cfg(unix)]
            {
                use std::os::unix::fs::FileExt;

                // For O_DIRECT, offset should be aligned
                let aligned_offset = if direct_enabled {
                    align_down(offset, alignment)
                } else {
                    offset
                };

                match file.read_at(&mut buf, aligned_offset) {
                    Ok(n) => {
                        if direct_enabled {
                            stats
                                .direct_read_bytes
                                .fetch_add(n as u64, Ordering::Relaxed);
                        }
                        (Ok(n), buf)
                    }
                    Err(e) => (Err(e), buf),
                }
            }

            #[cfg(not(unix))]
            {
                use std::io::Read;
                let mut file_guard = file;
                let result = file_guard
                    .seek(SeekFrom::Start(offset))
                    .and_then(|_| file_guard.read(&mut buf));
                match result {
                    Ok(n) => (Ok(n), buf),
                    Err(e) => (Err(e), buf),
                }
            }
        })
        .await;

        match result {
            Ok((Ok(n), buf)) => (Ok(n), buf),
            Ok((Err(e), buf)) => (Err(StreamlineError::from(e)), buf),
            Err(e) => (
                Err(StreamlineError::storage_msg(format!(
                    "Task join error: {}",
                    e
                ))),
                vec![],
            ),
        }
    }

    async fn write_at(&self, buf: Vec<u8>, offset: u64) -> IoResult<usize> {
        let file = self.file.clone();
        let alignment = self.config.alignment as u64;
        let direct_enabled = self.direct_enabled.load(Ordering::Relaxed);
        let stats = self.stats.clone();

        // Ensure buffer is aligned for O_DIRECT
        let buf = if direct_enabled {
            self.ensure_aligned(buf)
        } else {
            buf
        };

        let result = tokio::task::spawn_blocking(move || {
            #[allow(unused_mut)]
            let mut file = file.blocking_lock();

            #[cfg(unix)]
            {
                use std::os::unix::fs::FileExt;

                // For O_DIRECT, offset should be aligned
                let aligned_offset = if direct_enabled {
                    align_down(offset, alignment)
                } else {
                    offset
                };

                match file.write_at(&buf, aligned_offset) {
                    Ok(n) => {
                        if direct_enabled {
                            stats
                                .direct_write_bytes
                                .fetch_add(n as u64, Ordering::Relaxed);
                        }
                        (Ok(n), buf)
                    }
                    Err(e) => (Err(e), buf),
                }
            }

            #[cfg(not(unix))]
            {
                let result = file
                    .seek(SeekFrom::Start(offset))
                    .and_then(|_| file.write(&buf));
                match result {
                    Ok(n) => (Ok(n), buf),
                    Err(e) => (Err(e), buf),
                }
            }
        })
        .await;

        match result {
            Ok((Ok(n), buf)) => (Ok(n), buf),
            Ok((Err(e), buf)) => (Err(StreamlineError::from(e)), buf),
            Err(e) => (
                Err(StreamlineError::storage_msg(format!(
                    "Task join error: {}",
                    e
                ))),
                vec![],
            ),
        }
    }

    async fn append(&self, buf: Vec<u8>) -> IoResult<usize> {
        let file = self.file.clone();
        let direct_enabled = self.direct_enabled.load(Ordering::Relaxed);
        let stats = self.stats.clone();

        // Ensure buffer is aligned for O_DIRECT
        let buf = if direct_enabled {
            self.ensure_aligned(buf)
        } else {
            buf
        };

        let result = tokio::task::spawn_blocking(move || {
            let mut file = file.blocking_lock();
            let result = file.seek(SeekFrom::End(0)).and_then(|_| file.write(&buf));
            match result {
                Ok(n) => {
                    if direct_enabled {
                        stats
                            .direct_write_bytes
                            .fetch_add(n as u64, Ordering::Relaxed);
                    }
                    (Ok(n), buf)
                }
                Err(e) => (Err(e), buf),
            }
        })
        .await;

        match result {
            Ok((Ok(n), buf)) => (Ok(n), buf),
            Ok((Err(e), buf)) => (Err(StreamlineError::from(e)), buf),
            Err(e) => (
                Err(StreamlineError::storage_msg(format!(
                    "Task join error: {}",
                    e
                ))),
                vec![],
            ),
        }
    }

    async fn sync_data(&self) -> Result<()> {
        let file = self.file.clone();
        tokio::task::spawn_blocking(move || file.blocking_lock().sync_data())
            .await
            .map_err(|e| StreamlineError::storage_msg(format!("Task join error: {}", e)))?
            .map_err(StreamlineError::from)
    }

    async fn sync_all(&self) -> Result<()> {
        let file = self.file.clone();
        tokio::task::spawn_blocking(move || file.blocking_lock().sync_all())
            .await
            .map_err(|e| StreamlineError::storage_msg(format!("Task join error: {}", e)))?
            .map_err(StreamlineError::from)
    }

    async fn size(&self) -> Result<u64> {
        let file = self.file.clone();
        tokio::task::spawn_blocking(move || {
            let file = file.blocking_lock();
            file.metadata().map(|m| m.len())
        })
        .await
        .map_err(|e| StreamlineError::storage_msg(format!("Task join error: {}", e)))?
        .map_err(StreamlineError::from)
    }

    async fn allocate(&self, len: u64) -> Result<()> {
        let file = self.file.clone();

        #[cfg(target_os = "linux")]
        {
            tokio::task::spawn_blocking(move || {
                use std::os::unix::io::AsRawFd;
                let file = file.blocking_lock();
                let fd = file.as_raw_fd();
                // SAFETY: fallocate is safe because the file descriptor is valid and
                // owned by the mutex-locked File. The offset (0) and length are
                // non-negative. Errors are checked via the return value.
                unsafe {
                    let result = libc::fallocate(fd, 0, 0, len as i64);
                    if result != 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                }
                Ok(())
            })
            .await
            .map_err(|e| StreamlineError::storage_msg(format!("Task join error: {}", e)))?
            .map_err(StreamlineError::from)
        }

        #[cfg(not(target_os = "linux"))]
        {
            tokio::task::spawn_blocking(move || {
                let file = file.blocking_lock();
                file.set_len(len)
            })
            .await
            .map_err(|e| StreamlineError::storage_msg(format!("Task join error: {}", e)))?
            .map_err(StreamlineError::from)
        }
    }
}

/// Direct I/O filesystem implementation
///
/// Opens files with O_DIRECT flag (or equivalent) to bypass the OS page cache.
pub struct DirectFileSystem {
    config: DirectIoConfig,
    stats: Arc<DirectFsStats>,
}

struct DirectFsStats {
    files_opened_direct: AtomicU64,
    files_opened_fallback: AtomicU64,
}

impl Default for DirectFsStats {
    fn default() -> Self {
        Self {
            files_opened_direct: AtomicU64::new(0),
            files_opened_fallback: AtomicU64::new(0),
        }
    }
}

impl DirectFileSystem {
    /// Create a new DirectFileSystem with default configuration
    pub fn new() -> Self {
        Self::with_config(DirectIoConfig::default())
    }

    /// Create a new DirectFileSystem with custom configuration
    pub fn with_config(config: DirectIoConfig) -> Self {
        debug!(
            alignment = config.alignment,
            fallback = config.fallback_on_error,
            "Initializing Direct I/O filesystem"
        );
        Self {
            config,
            stats: Arc::new(DirectFsStats::default()),
        }
    }

    /// Get filesystem statistics
    pub fn stats(&self) -> (u64, u64) {
        (
            self.stats.files_opened_direct.load(Ordering::Relaxed),
            self.stats.files_opened_fallback.load(Ordering::Relaxed),
        )
    }

    /// Open a file with O_DIRECT flag (Linux)
    #[cfg(target_os = "linux")]
    fn open_direct(path: &Path, options: &OpenOptions) -> std::io::Result<(File, bool)> {
        use std::os::unix::fs::OpenOptionsExt;

        // Clone options and add O_DIRECT
        let mut direct_options = options.clone();
        direct_options.custom_flags(libc::O_DIRECT);

        match direct_options.open(path) {
            Ok(file) => Ok((file, true)),
            Err(e) => {
                // O_DIRECT might not be supported (e.g., on tmpfs)
                warn!(
                    path = %path.display(),
                    error = %e,
                    "O_DIRECT not supported, falling back to standard I/O"
                );
                let file = options.open(path)?;
                Ok((file, false))
            }
        }
    }

    /// Open a file with F_NOCACHE on macOS
    #[cfg(target_os = "macos")]
    fn open_direct(path: &Path, options: &OpenOptions) -> std::io::Result<(File, bool)> {
        use std::os::unix::io::AsRawFd;

        let file = options.open(path)?;

        // Use F_NOCACHE to disable caching (macOS equivalent of O_DIRECT)
        let fd = file.as_raw_fd();
        // SAFETY: fcntl with F_NOCACHE is safe because the file descriptor is valid
        // and owned by the caller. F_NOCACHE is a macOS-specific advisory flag that
        // disables caching. Errors are checked via the return value.
        let result = unsafe { libc::fcntl(fd, libc::F_NOCACHE, 1) };

        if result == -1 {
            warn!(
                path = %path.display(),
                "F_NOCACHE not supported, using standard I/O"
            );
            Ok((file, false))
        } else {
            Ok((file, true))
        }
    }

    /// Fallback for platforms without O_DIRECT support
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    fn open_direct(path: &Path, options: &OpenOptions) -> std::io::Result<(File, bool)> {
        warn!(
            path = %path.display(),
            "O_DIRECT not supported on this platform"
        );
        let file = options.open(path)?;
        Ok((file, false))
    }
}

impl Default for DirectFileSystem {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl AsyncFileSystem for DirectFileSystem {
    type File = DirectFile;

    async fn open(&self, path: &Path) -> Result<Self::File> {
        let path_buf = path.to_path_buf();
        let config = self.config.clone();
        let stats = self.stats.clone();

        let (file, direct_enabled, final_path) = tokio::task::spawn_blocking(move || {
            let options = OpenOptions::new().read(true).clone();

            match Self::open_direct(&path_buf, &options) {
                Ok((file, direct)) => {
                    if direct {
                        stats.files_opened_direct.fetch_add(1, Ordering::Relaxed);
                    } else {
                        stats.files_opened_fallback.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok((file, direct, path_buf))
                }
                Err(e) => Err(StreamlineError::storage_msg(format!(
                    "Failed to open file {:?}: {}",
                    path_buf, e
                ))),
            }
        })
        .await
        .map_err(|e| StreamlineError::storage_msg(format!("Task join error: {}", e)))??;

        Ok(DirectFile::new(file, final_path, config, direct_enabled))
    }

    async fn open_rw(&self, path: &Path) -> Result<Self::File> {
        let path_buf = path.to_path_buf();
        let config = self.config.clone();
        let stats = self.stats.clone();

        let (file, direct_enabled, final_path) = tokio::task::spawn_blocking(move || {
            let options = OpenOptions::new().read(true).write(true).clone();

            match Self::open_direct(&path_buf, &options) {
                Ok((file, direct)) => {
                    if direct {
                        stats.files_opened_direct.fetch_add(1, Ordering::Relaxed);
                    } else {
                        stats.files_opened_fallback.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok((file, direct, path_buf))
                }
                Err(e) => Err(StreamlineError::storage_msg(format!(
                    "Failed to open file {:?} for read-write: {}",
                    path_buf, e
                ))),
            }
        })
        .await
        .map_err(|e| StreamlineError::storage_msg(format!("Task join error: {}", e)))??;

        Ok(DirectFile::new(file, final_path, config, direct_enabled))
    }

    async fn create(&self, path: &Path) -> Result<Self::File> {
        let path_buf = path.to_path_buf();
        let config = self.config.clone();
        let stats = self.stats.clone();

        let (file, direct_enabled, final_path) = tokio::task::spawn_blocking(move || {
            let options = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .clone();

            match Self::open_direct(&path_buf, &options) {
                Ok((file, direct)) => {
                    if direct {
                        stats.files_opened_direct.fetch_add(1, Ordering::Relaxed);
                    } else {
                        stats.files_opened_fallback.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok((file, direct, path_buf))
                }
                Err(e) => Err(StreamlineError::storage_msg(format!(
                    "Failed to create file {:?}: {}",
                    path_buf, e
                ))),
            }
        })
        .await
        .map_err(|e| StreamlineError::storage_msg(format!("Task join error: {}", e)))??;

        Ok(DirectFile::new(file, final_path, config, direct_enabled))
    }

    async fn open_append(&self, path: &Path) -> Result<Self::File> {
        let path_buf = path.to_path_buf();
        let config = self.config.clone();
        let stats = self.stats.clone();

        let (file, direct_enabled, final_path) = tokio::task::spawn_blocking(move || {
            let options = OpenOptions::new()
                .read(true)
                .create(true)
                .append(true)
                .clone();

            match Self::open_direct(&path_buf, &options) {
                Ok((file, direct)) => {
                    if direct {
                        stats.files_opened_direct.fetch_add(1, Ordering::Relaxed);
                    } else {
                        stats.files_opened_fallback.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok((file, direct, path_buf))
                }
                Err(e) => Err(StreamlineError::storage_msg(format!(
                    "Failed to open file {:?} for append: {}",
                    path_buf, e
                ))),
            }
        })
        .await
        .map_err(|e| StreamlineError::storage_msg(format!("Task join error: {}", e)))??;

        Ok(DirectFile::new(file, final_path, config, direct_enabled))
    }
}

/// Check if O_DIRECT is available on this platform
pub fn is_direct_io_available() -> bool {
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
    fn test_allocate_aligned() {
        let buf = allocate_aligned(1000, 4096).unwrap();
        assert!(buf.len() >= 1000);
        assert_eq!(buf.len() % 4096, 0);
        assert!(is_aligned(&buf, 4096));
    }

    #[test]
    fn test_align_up_down() {
        assert_eq!(align_up(1000, 4096), 4096);
        assert_eq!(align_up(4096, 4096), 4096);
        assert_eq!(align_up(4097, 4096), 8192);

        assert_eq!(align_down(1000, 4096), 0);
        assert_eq!(align_down(4096, 4096), 4096);
        assert_eq!(align_down(5000, 4096), 4096);
    }

    #[test]
    fn test_is_aligned() {
        let aligned = allocate_aligned(4096, 4096).unwrap();
        assert!(is_aligned(&aligned, 4096));

        // Regular vec may not be aligned
        let regular = vec![0u8; 4096];
        // Can't guarantee alignment, just test function works
        let _ = is_aligned(&regular, 4096);
    }

    #[tokio::test]
    async fn test_direct_file_read_write() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_direct.txt");

        let fs = DirectFileSystem::new();

        // Create and write
        let file = fs.create(&path).await.unwrap();
        let mut data = allocate_aligned(4096, 4096).unwrap();
        data[..11].copy_from_slice(b"hello world");

        let (result, _buf) = file.write_at(data, 0).await;
        let bytes_written = result.unwrap();
        assert!(bytes_written > 0);

        // Sync
        file.sync_all().await.unwrap();

        // Read back
        let file = fs.open(&path).await.unwrap();
        let buf = allocate_aligned(4096, 4096).unwrap();
        let (result, buf) = file.read_at(buf, 0).await;
        let bytes_read = result.unwrap();
        assert!(bytes_read > 0);
        assert_eq!(&buf[..11], b"hello world");
    }

    #[tokio::test]
    async fn test_direct_file_size() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_size.txt");

        let fs = DirectFileSystem::new();
        let file = fs.create(&path).await.unwrap();

        // Write some data
        let data = allocate_aligned(4096, 4096).unwrap();
        let (result, _) = file.write_at(data, 0).await;
        result.unwrap();
        file.sync_all().await.unwrap();

        let size = file.size().await.unwrap();
        assert_eq!(size, 4096);
    }

    #[test]
    fn test_direct_io_available() {
        let available = is_direct_io_available();
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        assert!(available);
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        assert!(!available);
    }

    #[test]
    fn test_direct_io_config_default() {
        let config = DirectIoConfig::default();
        assert_eq!(config.alignment, DEFAULT_ALIGNMENT);
        assert!(config.fallback_on_error);
        assert!(config.direct_reads);
        assert!(config.direct_writes);
    }
}
