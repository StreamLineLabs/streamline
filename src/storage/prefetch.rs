//! Prefetching utilities for standard I/O operations
//!
//! This module provides fadvise and readahead functionality to hint the kernel
//! about expected I/O access patterns. These hints can significantly improve
//! sequential read performance for consumers.
//!
//! ## Usage
//!
//! ```no_run
//! use streamline::storage::prefetch::{advise_sequential, prefetch_range};
//! use std::fs::File;
//!
//! let file = File::open("data.segment").unwrap();
//! advise_sequential(&file).ok();  // Hint: sequential access pattern
//! prefetch_range(&file, 0, 1024 * 1024).ok();  // Prefetch first 1MB
//! ```

use std::fs::File;
use std::io;
#[cfg(unix)]
use std::os::unix::io::AsRawFd;

/// Default prefetch window size (256KB)
pub const DEFAULT_PREFETCH_SIZE: usize = 256 * 1024;

/// posix_fadvise advice values (Linux)
#[cfg(target_os = "linux")]
mod fadvise_flags {
    pub const POSIX_FADV_NORMAL: libc::c_int = 0;
    pub const POSIX_FADV_RANDOM: libc::c_int = 1;
    pub const POSIX_FADV_SEQUENTIAL: libc::c_int = 2;
    pub const POSIX_FADV_WILLNEED: libc::c_int = 3;
    pub const POSIX_FADV_DONTNEED: libc::c_int = 4;
    pub const POSIX_FADV_NOREUSE: libc::c_int = 5;
}

/// Advise the kernel that the file will be accessed sequentially
///
/// This enables aggressive read-ahead, which can significantly improve
/// sequential read performance. The kernel will prefetch more data
/// in anticipation of sequential access.
///
/// # Arguments
/// * `file` - The file to advise
///
/// # Platform Support
/// - Linux: Uses posix_fadvise(FADV_SEQUENTIAL)
/// - macOS: Uses fcntl(F_RDAHEAD) - limited effect
/// - Others: No-op
#[cfg(target_os = "linux")]
pub fn advise_sequential(file: &File) -> io::Result<()> {
    use std::os::unix::io::AsRawFd;
    let fd = file.as_raw_fd();
    // SAFETY: posix_fadvise is safe because the file descriptor is valid and
    // obtained from an open File. The offset (0) and length (0 = entire file)
    // are valid. posix_fadvise is advisory and does not modify file data.
    // Errors are checked via the return value.
    let result = unsafe {
        libc::posix_fadvise(
            fd,
            0,
            0, // 0 means entire file
            fadvise_flags::POSIX_FADV_SEQUENTIAL,
        )
    };
    if result != 0 {
        return Err(io::Error::from_raw_os_error(result));
    }
    Ok(())
}

#[cfg(target_os = "macos")]
pub fn advise_sequential(file: &File) -> io::Result<()> {
    // macOS doesn't have posix_fadvise, but we can enable read-ahead via fcntl
    let fd = file.as_raw_fd();
    // SAFETY: fcntl with F_RDAHEAD is safe because the file descriptor is valid
    // and obtained from an open File. F_RDAHEAD is a macOS-specific advisory flag
    // to enable read-ahead. Errors are checked via the return value.
    let result = unsafe { libc::fcntl(fd, libc::F_RDAHEAD, 1) };
    if result < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn advise_sequential(_file: &File) -> io::Result<()> {
    // No-op on other platforms
    Ok(())
}

/// Advise the kernel that the file will be accessed randomly
///
/// This disables read-ahead, which can reduce wasted I/O bandwidth
/// for random access patterns.
#[cfg(target_os = "linux")]
pub fn advise_random(file: &File) -> io::Result<()> {
    let fd = file.as_raw_fd();
    // SAFETY: posix_fadvise is safe because the file descriptor is valid and
    // obtained from an open File. The offset (0) and length (0 = entire file)
    // are valid. posix_fadvise is advisory and does not modify file data.
    let result = unsafe { libc::posix_fadvise(fd, 0, 0, fadvise_flags::POSIX_FADV_RANDOM) };
    if result != 0 {
        return Err(io::Error::from_raw_os_error(result));
    }
    Ok(())
}

#[cfg(target_os = "macos")]
pub fn advise_random(file: &File) -> io::Result<()> {
    // macOS: disable read-ahead for random access
    let fd = file.as_raw_fd();
    // SAFETY: fcntl with F_RDAHEAD is safe because the file descriptor is valid
    // and obtained from an open File. Passing 0 disables read-ahead (advisory).
    // Errors are checked via the return value.
    let result = unsafe { libc::fcntl(fd, libc::F_RDAHEAD, 0) };
    if result < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn advise_random(_file: &File) -> io::Result<()> {
    Ok(())
}

/// Advise the kernel to prefetch a range of data
///
/// This hints to the kernel that the specified range will be accessed soon,
/// triggering an asynchronous read-ahead. The call returns immediately
/// without blocking.
///
/// # Arguments
/// * `file` - The file to prefetch from
/// * `offset` - Starting offset in bytes
/// * `len` - Length of the range to prefetch
#[cfg(target_os = "linux")]
pub fn advise_willneed(file: &File, offset: i64, len: usize) -> io::Result<()> {
    let fd = file.as_raw_fd();
    // SAFETY: posix_fadvise is safe because the file descriptor is valid and
    // obtained from an open File. The offset and length are caller-provided but
    // posix_fadvise is advisory — out-of-range values are harmless. Errors are
    // checked via the return value.
    let result = unsafe {
        libc::posix_fadvise(
            fd,
            offset as libc::off_t,
            len as libc::off_t,
            fadvise_flags::POSIX_FADV_WILLNEED,
        )
    };
    if result != 0 {
        return Err(io::Error::from_raw_os_error(result));
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn advise_willneed(_file: &File, _offset: i64, _len: usize) -> io::Result<()> {
    // No direct equivalent on other platforms
    Ok(())
}

/// Advise the kernel that a range of data is no longer needed
///
/// This hints to the kernel that the specified pages can be released
/// from the page cache. Useful after processing data to reduce memory
/// pressure.
///
/// # Arguments
/// * `file` - The file to advise
/// * `offset` - Starting offset in bytes
/// * `len` - Length of the range (0 means to end of file)
#[cfg(target_os = "linux")]
pub fn advise_dontneed(file: &File, offset: i64, len: usize) -> io::Result<()> {
    let fd = file.as_raw_fd();
    // SAFETY: posix_fadvise is safe because the file descriptor is valid and
    // obtained from an open File. posix_fadvise is advisory — it hints that
    // the specified range is no longer needed. Errors are checked via return value.
    let result = unsafe {
        libc::posix_fadvise(
            fd,
            offset as libc::off_t,
            len as libc::off_t,
            fadvise_flags::POSIX_FADV_DONTNEED,
        )
    };
    if result != 0 {
        return Err(io::Error::from_raw_os_error(result));
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn advise_dontneed(_file: &File, _offset: i64, _len: usize) -> io::Result<()> {
    Ok(())
}

/// Advise the kernel that data will be accessed once and not reused
///
/// This is useful for one-time scans where data shouldn't pollute
/// the page cache.
#[cfg(target_os = "linux")]
pub fn advise_noreuse(file: &File, offset: i64, len: usize) -> io::Result<()> {
    let fd = file.as_raw_fd();
    // SAFETY: posix_fadvise is safe because the file descriptor is valid and
    // obtained from an open File. FADV_NOREUSE is advisory, hinting that the
    // data will be accessed once. Errors are checked via the return value.
    let result = unsafe {
        libc::posix_fadvise(
            fd,
            offset as libc::off_t,
            len as libc::off_t,
            fadvise_flags::POSIX_FADV_NOREUSE,
        )
    };
    if result != 0 {
        return Err(io::Error::from_raw_os_error(result));
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn advise_noreuse(_file: &File, _offset: i64, _len: usize) -> io::Result<()> {
    Ok(())
}

/// Prefetch a range of data into the page cache using readahead()
///
/// This is more aggressive than fadvise(FADV_WILLNEED) and is designed
/// specifically for read-ahead operations. The kernel will read the
/// specified range into memory asynchronously.
///
/// # Arguments
/// * `file` - The file to prefetch from
/// * `offset` - Starting offset in bytes
/// * `len` - Length of the range to prefetch
///
/// # Platform Support
/// - Linux: Uses readahead() syscall
/// - Others: Falls back to fadvise(WILLNEED) or no-op
#[cfg(target_os = "linux")]
pub fn prefetch_range(file: &File, offset: i64, len: usize) -> io::Result<()> {
    let fd = file.as_raw_fd();
    // SAFETY: readahead is safe because the file descriptor is valid and obtained
    // from an open File. The offset and length define the range to prefetch into
    // the page cache. Out-of-range values are handled by the kernel. Errors are
    // checked via the return value.
    let result = unsafe { libc::readahead(fd, offset as libc::off64_t, len) };
    if result < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn prefetch_range(file: &File, offset: i64, len: usize) -> io::Result<()> {
    // Fall back to fadvise on other platforms
    advise_willneed(file, offset, len)
}

/// Prefetch configuration for consumer read-ahead
#[derive(Debug, Clone)]
pub struct PrefetchConfig {
    /// Whether prefetching is enabled
    pub enabled: bool,
    /// Number of batches to prefetch ahead
    pub prefetch_count: usize,
    /// Size of each prefetch window in bytes
    pub window_size: usize,
}

impl Default for PrefetchConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            prefetch_count: 2,
            window_size: DEFAULT_PREFETCH_SIZE,
        }
    }
}

/// Prefetcher for segment reads
///
/// Tracks read position and automatically prefetches ahead
/// for sequential consumer reads.
pub struct SegmentPrefetcher {
    config: PrefetchConfig,
    last_prefetch_end: i64,
}

impl SegmentPrefetcher {
    /// Create a new segment prefetcher
    pub fn new(config: PrefetchConfig) -> Self {
        Self {
            config,
            last_prefetch_end: 0,
        }
    }

    /// Create a prefetcher with default configuration
    pub fn with_defaults() -> Self {
        Self::new(PrefetchConfig::default())
    }

    /// Prefetch ahead based on current read position
    ///
    /// Call this after reading to trigger prefetch of upcoming data.
    ///
    /// # Arguments
    /// * `file` - The file to prefetch from
    /// * `current_position` - Current read position in the file
    /// * `file_size` - Total size of the file
    pub fn prefetch_ahead(
        &mut self,
        file: &File,
        current_position: i64,
        file_size: i64,
    ) -> io::Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        // Calculate prefetch start position
        let prefetch_start = if current_position > self.last_prefetch_end {
            current_position
        } else {
            self.last_prefetch_end
        };

        // Calculate total prefetch size
        let total_prefetch_size = (self.config.prefetch_count * self.config.window_size) as i64;

        // Don't prefetch past end of file
        let prefetch_end = std::cmp::min(prefetch_start + total_prefetch_size, file_size);
        let actual_prefetch_size = prefetch_end - prefetch_start;

        if actual_prefetch_size <= 0 {
            return Ok(());
        }

        // Perform prefetch
        prefetch_range(file, prefetch_start, actual_prefetch_size as usize)?;

        self.last_prefetch_end = prefetch_end;
        Ok(())
    }

    /// Reset prefetch state (e.g., after seeking to a new position)
    pub fn reset(&mut self) {
        self.last_prefetch_end = 0;
    }

    /// Check if prefetching is enabled
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_prefetch_config_default() {
        let config = PrefetchConfig::default();
        assert!(config.enabled);
        assert_eq!(config.prefetch_count, 2);
        assert_eq!(config.window_size, DEFAULT_PREFETCH_SIZE);
    }

    #[test]
    fn test_segment_prefetcher_creation() {
        let prefetcher = SegmentPrefetcher::with_defaults();
        assert!(prefetcher.is_enabled());
        assert_eq!(prefetcher.last_prefetch_end, 0);
    }

    #[test]
    fn test_advise_sequential_on_real_file() {
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(b"test data for prefetch").unwrap();
        temp_file.flush().unwrap();

        let file = File::open(temp_file.path()).unwrap();
        // Should not error on any platform
        let result = advise_sequential(&file);
        assert!(result.is_ok());
    }

    #[test]
    fn test_advise_random_on_real_file() {
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(b"test data for prefetch").unwrap();
        temp_file.flush().unwrap();

        let file = File::open(temp_file.path()).unwrap();
        let result = advise_random(&file);
        assert!(result.is_ok());
    }

    #[test]
    fn test_prefetch_range_on_real_file() {
        let mut temp_file = NamedTempFile::new().unwrap();
        let data = vec![0u8; 4096];
        temp_file.write_all(&data).unwrap();
        temp_file.flush().unwrap();

        let file = File::open(temp_file.path()).unwrap();
        let result = prefetch_range(&file, 0, 4096);
        assert!(result.is_ok());
    }

    #[test]
    fn test_segment_prefetcher_prefetch_ahead() {
        let mut temp_file = NamedTempFile::new().unwrap();
        let data = vec![0u8; 1024 * 1024]; // 1MB
        temp_file.write_all(&data).unwrap();
        temp_file.flush().unwrap();

        let file = File::open(temp_file.path()).unwrap();
        let mut prefetcher = SegmentPrefetcher::with_defaults();

        // Simulate reading at position 0
        let result = prefetcher.prefetch_ahead(&file, 0, 1024 * 1024);
        assert!(result.is_ok());
        assert!(prefetcher.last_prefetch_end > 0);

        // Simulate reading further
        let result = prefetcher.prefetch_ahead(&file, 512 * 1024, 1024 * 1024);
        assert!(result.is_ok());
    }

    #[test]
    fn test_segment_prefetcher_reset() {
        let mut prefetcher = SegmentPrefetcher::with_defaults();
        prefetcher.last_prefetch_end = 1000;

        prefetcher.reset();
        assert_eq!(prefetcher.last_prefetch_end, 0);
    }

    #[test]
    fn test_disabled_prefetcher() {
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(b"test").unwrap();
        temp_file.flush().unwrap();

        let file = File::open(temp_file.path()).unwrap();
        let config = PrefetchConfig {
            enabled: false,
            ..Default::default()
        };
        let mut prefetcher = SegmentPrefetcher::new(config);

        // Should succeed but not actually prefetch
        let result = prefetcher.prefetch_ahead(&file, 0, 100);
        assert!(result.is_ok());
        assert_eq!(prefetcher.last_prefetch_end, 0); // Should not have been updated
    }
}
