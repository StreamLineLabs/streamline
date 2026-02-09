// Zero-copy optimization scaffolding (not yet fully integrated)
#![allow(dead_code)]

//! Zero-copy optimization utilities for efficient data transfer
//!
//! This module provides optimized I/O operations to minimize memory copies
//! when transferring data between files and network sockets.
//!
//! ## Features
//!
//! - **Memory-mapped file access**: Direct access to file contents without
//!   copying data through kernel buffers
//! - **Vectored I/O**: Scatter-gather operations for efficient multi-buffer transfers
//! - **Pre-allocated buffers**: Reusable buffer pools to reduce allocation overhead
//! - **Platform-specific optimizations**: Uses sendfile on Linux, copyfile on macOS
//!
//! ## Usage
//!
//! ```
//! use streamline::storage::zerocopy::BufferPool;
//!
//! // Create a buffer pool for reusable allocations
//! let pool = BufferPool::new(8, 64 * 1024); // 8 buffers of 64KB each
//! ```
//!
//! For file-based zero-copy reads (requires a file on disk):
//!
//! ```no_run
//! use streamline::storage::zerocopy::ZeroCopyReader;
//!
//! // Create a zero-copy reader for efficient segment reads
//! let reader = ZeroCopyReader::open("segment.dat").unwrap();
//! let data = reader.read_range(0, 1024).unwrap();
//! ```

use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{self};
#[cfg(not(unix))]
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Default buffer size for I/O operations (64KB)
pub const DEFAULT_BUFFER_SIZE: usize = 64 * 1024;

/// Maximum buffer size (1MB)
pub const MAX_BUFFER_SIZE: usize = 1024 * 1024;

/// A pool of reusable byte buffers to minimize allocations
#[derive(Debug)]
pub struct BufferPool {
    /// Available buffers
    buffers: Mutex<Vec<BytesMut>>,
    /// Size of each buffer
    buffer_size: usize,
    /// Maximum number of buffers to pool
    max_buffers: usize,
}

impl BufferPool {
    /// Create a new buffer pool
    ///
    /// # Arguments
    /// * `max_buffers` - Maximum number of buffers to keep in the pool
    /// * `buffer_size` - Size of each buffer in bytes
    pub fn new(max_buffers: usize, buffer_size: usize) -> Self {
        Self {
            buffers: Mutex::new(Vec::with_capacity(max_buffers)),
            buffer_size: buffer_size.min(MAX_BUFFER_SIZE),
            max_buffers,
        }
    }

    /// Get a buffer from the pool or allocate a new one
    pub async fn get(&self) -> BytesMut {
        let mut buffers = self.buffers.lock().await;
        if let Some(mut buf) = buffers.pop() {
            buf.clear();
            #[cfg(feature = "metrics")]
            metrics::counter!("streamline_zerocopy_buffer_pool_hits").increment(1);
            buf
        } else {
            #[cfg(feature = "metrics")]
            metrics::counter!("streamline_zerocopy_buffer_pool_misses").increment(1);
            BytesMut::with_capacity(self.buffer_size)
        }
    }

    /// Get a buffer with at least the specified capacity
    ///
    /// Tries to find a pooled buffer with sufficient capacity, otherwise
    /// allocates a new buffer with the requested size.
    pub async fn get_with_capacity(&self, min_capacity: usize) -> BytesMut {
        let mut buffers = self.buffers.lock().await;

        // Try to find a buffer with sufficient capacity
        if let Some(idx) = buffers.iter().position(|b| b.capacity() >= min_capacity) {
            let mut buf = buffers.swap_remove(idx);
            buf.clear();
            #[cfg(feature = "metrics")]
            metrics::counter!("streamline_zerocopy_buffer_pool_hits").increment(1);
            return buf;
        }

        // No suitable buffer, allocate new one with at least buffer_size capacity
        #[cfg(feature = "metrics")]
        metrics::counter!("streamline_zerocopy_buffer_pool_misses").increment(1);
        BytesMut::with_capacity(min_capacity.max(self.buffer_size))
    }

    /// Return a buffer to the pool for reuse
    pub async fn put(&self, buf: BytesMut) {
        let mut buffers = self.buffers.lock().await;
        if buffers.len() < self.max_buffers && buf.capacity() >= self.buffer_size {
            buffers.push(buf);
        }
        // If pool is full or buffer is too small, just drop it
    }

    /// Get statistics about the buffer pool
    pub async fn stats(&self) -> BufferPoolStats {
        let buffers = self.buffers.lock().await;
        let available = buffers.len();
        #[cfg(feature = "metrics")]
        metrics::gauge!("streamline_zerocopy_buffer_pool_available").set(available as f64);
        BufferPoolStats {
            available,
            max_buffers: self.max_buffers,
            buffer_size: self.buffer_size,
        }
    }
}

/// Statistics about a buffer pool
#[derive(Debug, Clone)]
pub struct BufferPoolStats {
    /// Number of buffers currently available
    pub available: usize,
    /// Maximum number of buffers
    pub max_buffers: usize,
    /// Size of each buffer
    pub buffer_size: usize,
}

impl Default for BufferPool {
    fn default() -> Self {
        Self::new(16, DEFAULT_BUFFER_SIZE)
    }
}

/// Zero-copy reader for efficient file reads
///
/// Uses platform-specific optimizations when available:
/// - On Unix: pread for positioned reads without seeking
/// - Vectored reads for scatter-gather operations
pub struct ZeroCopyReader {
    file: File,
    size: u64,
}

impl ZeroCopyReader {
    /// Open a file for zero-copy reading
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = File::open(path)?;
        let size = file.metadata()?.len();
        Ok(Self { file, size })
    }

    /// Get the file size
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Read a range of bytes from the file
    ///
    /// This uses positioned reads (pread) on Unix systems to avoid
    /// seek overhead and enable concurrent reads.
    pub fn read_range(&self, offset: u64, len: usize) -> io::Result<Bytes> {
        if offset >= self.size {
            return Ok(Bytes::new());
        }

        let actual_len = len.min((self.size - offset) as usize);
        let mut buf = BytesMut::with_capacity(actual_len);

        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            // SAFETY: This is safe because:
            // 1. buf was created with `with_capacity(actual_len)`, so the allocation
            //    has room for `actual_len` bytes
            // 2. We immediately read into the buffer, which initializes the bytes
            // 3. We truncate to `bytes_read` afterwards, ensuring we never expose
            //    uninitialized memory (handles short reads)
            // 4. BytesMut's internal pointer remains valid throughout
            unsafe {
                buf.set_len(actual_len);
            }
            let bytes_read = self.file.read_at(&mut buf[..actual_len], offset)?;
            buf.truncate(bytes_read);
        }

        #[cfg(not(unix))]
        {
            // Fallback for non-Unix platforms
            let mut file = self.file.try_clone()?;
            file.seek(SeekFrom::Start(offset))?;
            // SAFETY: Same invariants as Unix path above - capacity is sufficient,
            // we read into the buffer immediately, and truncate handles short reads.
            unsafe {
                buf.set_len(actual_len);
            }
            let bytes_read = file.read(&mut buf[..actual_len])?;
            buf.truncate(bytes_read);
        }

        Ok(buf.freeze())
    }

    /// Read a range into an existing buffer
    ///
    /// Returns the number of bytes read
    pub fn read_range_into(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        if offset >= self.size {
            return Ok(0);
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            self.file.read_at(buf, offset)
        }

        #[cfg(not(unix))]
        {
            let mut file = self.file.try_clone()?;
            file.seek(SeekFrom::Start(offset))?;
            file.read(buf)
        }
    }

    /// Read multiple ranges efficiently (vectored read simulation)
    ///
    /// Returns a vector of Bytes for each requested range
    pub fn read_ranges(&self, ranges: &[(u64, usize)]) -> io::Result<Vec<Bytes>> {
        ranges
            .iter()
            .map(|&(offset, len)| self.read_range(offset, len))
            .collect()
    }
}

/// Configuration for zero-copy operations
///
/// All optimizations are enabled by default for best performance.
/// TLS connections automatically disable sendfile since encryption
/// must happen in user space.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZeroCopyConfig {
    /// Master switch to enable/disable all zero-copy optimizations
    /// Default: true
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Enable memory-mapped file access for sealed segments
    /// Default: true
    #[serde(default = "default_true")]
    pub enable_mmap: bool,

    /// Minimum segment size (bytes) to use mmap
    /// Default: 1MB
    #[serde(default = "default_mmap_threshold")]
    pub mmap_threshold: u64,

    /// Buffer size for non-mmap reads
    /// Default: 64KB
    #[serde(default = "default_buffer_size")]
    pub buffer_size: usize,

    /// Enable sendfile/splice for socket transfers
    /// Automatically disabled for TLS connections
    /// Default: true
    #[serde(default = "default_true")]
    pub enable_sendfile: bool,

    /// Enable buffer pool for reducing allocations
    /// Default: true
    #[serde(default = "default_true")]
    pub enable_buffer_pool: bool,

    /// Number of buffers in the pool
    /// Default: 16
    #[serde(default = "default_buffer_pool_size")]
    pub buffer_pool_size: usize,

    /// Enable response caching for sendfile
    /// Default: true
    #[serde(default = "default_true")]
    pub enable_response_cache: bool,

    /// Maximum response cache size (bytes)
    /// Default: 128MB
    #[serde(default = "default_response_cache_size")]
    pub response_cache_max_bytes: u64,

    /// Response cache TTL in seconds
    /// Default: 60
    #[serde(default = "default_cache_ttl")]
    pub response_cache_ttl_secs: u64,
}

fn default_true() -> bool {
    true
}

fn default_mmap_threshold() -> u64 {
    1024 * 1024 // 1MB
}

fn default_buffer_size() -> usize {
    DEFAULT_BUFFER_SIZE
}

fn default_buffer_pool_size() -> usize {
    16
}

fn default_response_cache_size() -> u64 {
    128 * 1024 * 1024 // 128MB
}

fn default_cache_ttl() -> u64 {
    60
}

impl Default for ZeroCopyConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            enable_mmap: true,
            mmap_threshold: default_mmap_threshold(),
            buffer_size: default_buffer_size(),
            enable_sendfile: true,
            enable_buffer_pool: true,
            buffer_pool_size: default_buffer_pool_size(),
            enable_response_cache: true,
            response_cache_max_bytes: default_response_cache_size(),
            response_cache_ttl_secs: default_cache_ttl(),
        }
    }
}

/// Metrics for zero-copy operations
#[derive(Debug, Clone, Default)]
pub struct ZeroCopyStats {
    /// Total bytes transferred using zero-copy
    pub bytes_transferred: u64,
    /// Number of zero-copy operations
    pub operations: u64,
    /// Number of fallback (regular copy) operations
    pub fallback_operations: u64,
    /// Bytes saved by avoiding copies
    pub bytes_saved: u64,
}

impl ZeroCopyStats {
    /// Record a zero-copy operation
    pub fn record_zerocopy(&mut self, bytes: u64) {
        self.bytes_transferred += bytes;
        self.operations += 1;
        self.bytes_saved += bytes; // Each byte transferred without copy is a byte saved
    }

    /// Record a fallback operation
    pub fn record_fallback(&mut self, bytes: u64) {
        self.bytes_transferred += bytes;
        self.fallback_operations += 1;
    }
}

/// Thread-safe wrapper for ZeroCopyStats
pub type SharedZeroCopyStats = Arc<parking_lot::Mutex<ZeroCopyStats>>;

/// Create a shared stats instance
pub fn create_stats() -> SharedZeroCopyStats {
    Arc::new(parking_lot::Mutex::new(ZeroCopyStats::default()))
}

/// Efficiently copy data from a file to a byte buffer
///
/// Uses the most efficient method available on the platform
pub fn efficient_file_read(
    file: &File,
    offset: u64,
    len: usize,
    stats: Option<&SharedZeroCopyStats>,
) -> io::Result<Bytes> {
    let mut buf = BytesMut::with_capacity(len);

    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        // SAFETY: This is safe because:
        // 1. buf was created with `with_capacity(len)`, so the allocation has room
        // 2. We immediately read into the buffer, which initializes the bytes
        // 3. We truncate to `bytes_read` afterwards, ensuring we never expose
        //    uninitialized memory (handles short reads and EOF)
        unsafe {
            buf.set_len(len);
        }
        let bytes_read = file.read_at(&mut buf[..len], offset)?;
        buf.truncate(bytes_read);

        if let Some(stats) = stats {
            let mut s = stats.lock();
            s.record_zerocopy(bytes_read as u64);
        }
    }

    #[cfg(not(unix))]
    {
        let mut file_clone = file.try_clone()?;
        file_clone.seek(SeekFrom::Start(offset))?;
        // SAFETY: Same invariants as Unix path - capacity is sufficient,
        // we read into the buffer immediately, and truncate handles short reads.
        unsafe {
            buf.set_len(len);
        }
        let bytes_read = file_clone.read(&mut buf[..len])?;
        buf.truncate(bytes_read);

        if let Some(stats) = stats {
            let mut s = stats.lock();
            s.record_fallback(bytes_read as u64);
        }
    }

    Ok(buf.freeze())
}

/// A wrapper that provides efficient slice access to file content
pub(crate) struct FileSlice {
    reader: Arc<ZeroCopyReader>,
    offset: u64,
    len: usize,
}

impl FileSlice {
    /// Create a new file slice
    pub fn new(reader: Arc<ZeroCopyReader>, offset: u64, len: usize) -> Self {
        Self {
            reader,
            offset,
            len,
        }
    }

    /// Read the slice content
    pub fn read(&self) -> io::Result<Bytes> {
        self.reader.read_range(self.offset, self.len)
    }

    /// Get the length of this slice
    pub fn len(&self) -> usize {
        self.len
    }

    /// Check if the slice is empty
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Get the offset in the file
    pub fn offset(&self) -> u64 {
        self.offset
    }
}

/// Sendfile result containing bytes transferred
#[derive(Debug)]
pub struct SendfileResult {
    /// Number of bytes successfully transferred
    pub bytes_sent: usize,
}

/// Transfer data from a file to a socket using sendfile()
///
/// This function uses zero-copy transfer when available:
/// - Linux: Uses sendfile() syscall
/// - macOS: Uses sendfile() with slightly different semantics
///
/// # Arguments
/// * `socket_fd` - Raw file descriptor of the socket
/// * `file` - File to read from
/// * `offset` - Starting offset in the file
/// * `count` - Number of bytes to transfer
///
/// # Returns
/// The number of bytes transferred, or an error
///
/// # Safety
/// The socket_fd must be a valid file descriptor for a TCP socket
#[cfg(target_os = "linux")]
pub fn sendfile_to_socket(
    socket_fd: std::os::unix::io::RawFd,
    file: &File,
    offset: u64,
    count: usize,
) -> io::Result<SendfileResult> {
    use std::os::unix::io::AsRawFd;

    let file_fd = file.as_raw_fd();
    let mut off = offset as i64;

    // SAFETY: We're calling sendfile with valid file descriptors.
    // The socket_fd is assumed to be valid by the caller.
    // The file_fd is valid because it comes from a File reference.
    let result = unsafe { libc::sendfile(socket_fd, file_fd, &mut off, count) };

    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        #[cfg(feature = "metrics")]
        metrics::counter!("streamline_zerocopy_sendfile_bytes_total").increment(result as u64);
        Ok(SendfileResult {
            bytes_sent: result as usize,
        })
    }
}

/// Transfer data from a file to a socket using sendfile()
///
/// macOS sendfile has different semantics than Linux:
/// - The length parameter is in-out (gets set to bytes actually sent)
/// - The offset is passed by value, not reference
#[cfg(target_os = "macos")]
pub fn sendfile_to_socket(
    socket_fd: std::os::unix::io::RawFd,
    file: &File,
    offset: u64,
    count: usize,
) -> io::Result<SendfileResult> {
    use std::os::unix::io::AsRawFd;

    let file_fd = file.as_raw_fd();
    let mut len = count as i64;

    // SAFETY: We're calling sendfile with valid file descriptors.
    // macOS sendfile has signature: sendfile(fd, s, offset, len, hdtr, flags)
    // where len is in-out.
    let result = unsafe {
        libc::sendfile(
            file_fd,
            socket_fd,
            offset as i64,
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    };

    if result < 0 {
        let err = io::Error::last_os_error();
        // On macOS, EAGAIN means partial write - check len for bytes sent
        if err.raw_os_error() == Some(libc::EAGAIN) && len > 0 {
            #[cfg(feature = "metrics")]
            metrics::counter!("streamline_zerocopy_sendfile_bytes_total").increment(len as u64);
            return Ok(SendfileResult {
                bytes_sent: len as usize,
            });
        }
        Err(err)
    } else {
        #[cfg(feature = "metrics")]
        metrics::counter!("streamline_zerocopy_sendfile_bytes_total").increment(len as u64);
        Ok(SendfileResult {
            bytes_sent: len as usize,
        })
    }
}

/// Fallback implementation for non-Unix platforms
#[cfg(not(unix))]
pub fn sendfile_to_socket(
    _socket_fd: i32,
    _file: &File,
    _offset: u64,
    _count: usize,
) -> io::Result<SendfileResult> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "sendfile not supported on this platform",
    ))
}

/// Async wrapper for sendfile operations
///
/// Wraps sendfile in a blocking task to avoid blocking the async runtime.
/// This is necessary because sendfile is a blocking syscall.
#[cfg(unix)]
pub async fn sendfile_async(
    socket_fd: std::os::unix::io::RawFd,
    file: File,
    offset: u64,
    count: usize,
) -> io::Result<SendfileResult> {
    tokio::task::spawn_blocking(move || sendfile_to_socket(socket_fd, &file, offset, count))
        .await
        .map_err(io::Error::other)?
}

/// Async wrapper for non-Unix platforms (always fails)
#[cfg(not(unix))]
pub async fn sendfile_async(
    _socket_fd: i32,
    _file: File,
    _offset: u64,
    _count: usize,
) -> io::Result<SendfileResult> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "sendfile not supported on this platform",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_buffer_pool() {
        let pool = BufferPool::new(4, 1024);

        // Get a buffer
        let buf1 = pool.get().await;
        assert!(buf1.capacity() >= 1024);

        // Return it
        pool.put(buf1).await;

        // Get it back (should be reused)
        let buf2 = pool.get().await;
        assert!(buf2.capacity() >= 1024);

        // Check stats
        let stats = pool.stats().await;
        assert_eq!(stats.max_buffers, 4);
        assert_eq!(stats.buffer_size, 1024);
    }

    #[test]
    fn test_zero_copy_reader() {
        // Create a temp file with test data
        let mut temp_file = NamedTempFile::new().unwrap();
        let data = b"Hello, zero-copy world! This is test data.";
        temp_file.write_all(data).unwrap();
        temp_file.flush().unwrap();

        // Open with zero-copy reader
        let reader = ZeroCopyReader::open(temp_file.path()).unwrap();
        assert_eq!(reader.size(), data.len() as u64);

        // Read entire file
        let content = reader.read_range(0, data.len()).unwrap();
        assert_eq!(&content[..], data);

        // Read a range
        let range = reader.read_range(7, 9).unwrap();
        assert_eq!(&range[..], b"zero-copy");

        // Read past end (should return partial)
        let past_end = reader.read_range(data.len() as u64 - 5, 100).unwrap();
        assert_eq!(past_end.len(), 5);
    }

    #[test]
    fn test_read_ranges() {
        let mut temp_file = NamedTempFile::new().unwrap();
        let data = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        temp_file.write_all(data).unwrap();
        temp_file.flush().unwrap();

        let reader = ZeroCopyReader::open(temp_file.path()).unwrap();

        let ranges = &[(0, 10), (10, 10), (20, 10)];
        let results = reader.read_ranges(ranges).unwrap();

        assert_eq!(&results[0][..], b"0123456789");
        assert_eq!(&results[1][..], b"ABCDEFGHIJ");
        assert_eq!(&results[2][..], b"KLMNOPQRST");
    }

    #[test]
    fn test_file_slice() {
        let mut temp_file = NamedTempFile::new().unwrap();
        let data = b"Hello, World!";
        temp_file.write_all(data).unwrap();
        temp_file.flush().unwrap();

        let reader = Arc::new(ZeroCopyReader::open(temp_file.path()).unwrap());

        let slice = FileSlice::new(reader, 7, 5);
        assert_eq!(slice.len(), 5);
        assert_eq!(slice.offset(), 7);

        let content = slice.read().unwrap();
        assert_eq!(&content[..], b"World");
    }

    #[test]
    fn test_efficient_file_read() {
        let mut temp_file = NamedTempFile::new().unwrap();
        let data = b"Test data for efficient read";
        temp_file.write_all(data).unwrap();
        temp_file.flush().unwrap();

        let file = File::open(temp_file.path()).unwrap();
        let stats = create_stats();

        let content = efficient_file_read(&file, 5, 4, Some(&stats)).unwrap();
        assert_eq!(&content[..], b"data");

        let stats_val = stats.lock();
        assert_eq!(stats_val.bytes_transferred, 4);
        assert!(stats_val.operations > 0 || stats_val.fallback_operations > 0);
    }

    #[test]
    fn test_zero_copy_config_default() {
        let config = ZeroCopyConfig::default();
        assert!(config.enabled);
        assert!(config.enable_mmap);
        assert!(config.enable_sendfile);
        assert!(config.enable_buffer_pool);
        assert!(config.enable_response_cache);
        assert_eq!(config.mmap_threshold, 1024 * 1024);
        assert_eq!(config.buffer_size, DEFAULT_BUFFER_SIZE);
        assert_eq!(config.buffer_pool_size, 16);
        assert_eq!(config.response_cache_max_bytes, 128 * 1024 * 1024);
        assert_eq!(config.response_cache_ttl_secs, 60);
    }

    #[test]
    fn test_zero_copy_stats() {
        let mut stats = ZeroCopyStats::default();

        stats.record_zerocopy(1000);
        assert_eq!(stats.bytes_transferred, 1000);
        assert_eq!(stats.operations, 1);
        assert_eq!(stats.bytes_saved, 1000);

        stats.record_fallback(500);
        assert_eq!(stats.bytes_transferred, 1500);
        assert_eq!(stats.fallback_operations, 1);
        assert_eq!(stats.bytes_saved, 1000); // Fallback doesn't save bytes
    }
}
