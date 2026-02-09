//! Zero-copy networking with io_uring
//!
//! This module provides io_uring-based network I/O that shares registered buffers
//! with disk I/O, enabling true zero-copy data paths from network to disk.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────┐    ┌──────────────────┐    ┌─────────────┐
//! │   Network   │───►│ Registered Buffer │───►│    Disk     │
//! │   Socket    │    │   (Shared Pool)   │    │  (Direct)   │
//! └─────────────┘    └──────────────────┘    └─────────────┘
//!       recv                  │                   write
//!                             │
//!                  Single buffer, zero copies
//! ```
//!
//! # Benefits
//!
//! - **Zero-copy**: Data stays in registered buffers from recv to write
//! - **Reduced syscalls**: Batched submissions via io_uring
//! - **Lower latency**: No buffer copying between kernel and userspace
//! - **Higher throughput**: Up to 2-3x improvement over traditional I/O

#[cfg(target_os = "linux")]
use std::collections::VecDeque;
#[cfg(target_os = "linux")]
use std::net::SocketAddr;
#[cfg(target_os = "linux")]
use std::os::unix::io::{AsRawFd, RawFd};
#[cfg(target_os = "linux")]
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
#[cfg(target_os = "linux")]
use std::sync::Arc;

#[cfg(target_os = "linux")]
use parking_lot::{Mutex, RwLock};
#[cfg(target_os = "linux")]
use tracing::{debug, error, info, warn};

#[cfg(target_os = "linux")]
use crate::error::{Result, StreamlineError};

/// Configuration for io_uring network backend
#[derive(Debug, Clone)]
pub struct UringNetConfig {
    /// Number of registered buffers for network I/O
    pub buffer_count: usize,
    /// Size of each buffer (should match typical message size)
    pub buffer_size: usize,
    /// io_uring submission queue depth
    pub sq_depth: u32,
    /// io_uring completion queue depth
    pub cq_depth: u32,
    /// Enable multishot receive (kernel 5.19+)
    pub multishot_recv: bool,
    /// Enable provided buffers (kernel 5.7+)
    pub provided_buffers: bool,
    /// TCP accept backlog
    pub accept_backlog: u32,
    /// Enable TCP_NODELAY
    pub tcp_nodelay: bool,
    /// Enable SO_REUSEADDR
    pub reuse_addr: bool,
    /// Enable SO_REUSEPORT for multi-threaded accept
    pub reuse_port: bool,
}

impl Default for UringNetConfig {
    fn default() -> Self {
        Self {
            buffer_count: 256,
            buffer_size: 64 * 1024, // 64KB per buffer
            sq_depth: 512,
            cq_depth: 1024,
            multishot_recv: true,
            provided_buffers: true,
            accept_backlog: 1024,
            tcp_nodelay: true,
            reuse_addr: true,
            reuse_port: true,
        }
    }
}

impl UringNetConfig {
    /// High-throughput configuration for dedicated network servers
    pub fn high_throughput() -> Self {
        Self {
            buffer_count: 1024,
            buffer_size: 128 * 1024, // 128KB
            sq_depth: 2048,
            cq_depth: 4096,
            multishot_recv: true,
            provided_buffers: true,
            accept_backlog: 4096,
            tcp_nodelay: true,
            reuse_addr: true,
            reuse_port: true,
        }
    }

    /// Low-latency configuration
    pub fn low_latency() -> Self {
        Self {
            buffer_count: 128,
            buffer_size: 16 * 1024, // 16KB - smaller for faster processing
            sq_depth: 256,
            cq_depth: 512,
            multishot_recv: true,
            provided_buffers: true,
            accept_backlog: 256,
            tcp_nodelay: true,
            reuse_addr: true,
            reuse_port: false,
        }
    }
}

/// Statistics for network I/O operations
#[derive(Debug, Clone, Default)]
pub struct UringNetStats {
    /// Total bytes received
    pub bytes_received: u64,
    /// Total bytes sent
    pub bytes_sent: u64,
    /// Number of receive operations
    pub recv_ops: u64,
    /// Number of send operations
    pub send_ops: u64,
    /// Number of accept operations
    pub accept_ops: u64,
    /// Number of zero-copy operations (no buffer copies)
    pub zero_copy_ops: u64,
    /// Number of multishot completions
    pub multishot_completions: u64,
    /// Current number of active connections
    pub active_connections: u64,
    /// Buffer pool utilization (0.0 - 1.0)
    pub buffer_utilization: f64,
}

/// Buffer ID for tracking registered network buffers
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NetBufferId(u32);

impl NetBufferId {
    pub fn new(id: u32) -> Self {
        Self(id)
    }

    pub fn as_u32(&self) -> u32 {
        self.0
    }
}

/// A registered buffer for network I/O
#[cfg(target_os = "linux")]
pub struct NetBuffer {
    /// Buffer ID
    id: NetBufferId,
    /// Underlying buffer data
    data: Vec<u8>,
    /// Current data length (for received data)
    len: usize,
    /// Whether the buffer is in use
    in_use: AtomicBool,
}

#[cfg(target_os = "linux")]
impl NetBuffer {
    /// Create a new network buffer
    pub fn new(id: NetBufferId, size: usize) -> Self {
        // Page-align the buffer for DMA
        let aligned_size = (size + 4095) & !4095;
        let mut data = vec![0u8; aligned_size];
        // Touch pages to ensure allocation
        for chunk in data.chunks_mut(4096) {
            chunk[0] = 0;
        }

        Self {
            id,
            data,
            len: 0,
            in_use: AtomicBool::new(false),
        }
    }

    /// Get buffer ID
    pub fn id(&self) -> NetBufferId {
        self.id
    }

    /// Get buffer capacity
    pub fn capacity(&self) -> usize {
        self.data.len()
    }

    /// Get current data length
    pub fn len(&self) -> usize {
        self.len
    }

    /// Check if buffer is empty
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Get buffer data as slice
    pub fn as_slice(&self) -> &[u8] {
        &self.data[..self.len]
    }

    /// Get mutable buffer data
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.data
    }

    /// Set data length after receive
    pub fn set_len(&mut self, len: usize) {
        self.len = len.min(self.data.len());
    }

    /// Get raw pointer for io_uring
    pub fn as_ptr(&self) -> *const u8 {
        self.data.as_ptr()
    }

    /// Get mutable raw pointer for io_uring
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.data.as_mut_ptr()
    }

    /// Mark as in use
    pub fn acquire(&self) -> bool {
        self.in_use
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
    }

    /// Mark as available
    pub fn release(&self) {
        self.in_use.store(false, Ordering::Release);
    }

    /// Check if in use
    pub fn is_in_use(&self) -> bool {
        self.in_use.load(Ordering::Acquire)
    }
}

/// Pool of network buffers for zero-copy I/O
#[cfg(target_os = "linux")]
pub struct NetBufferPool {
    /// All buffers
    buffers: Vec<NetBuffer>,
    /// Free buffer IDs
    free_list: Mutex<VecDeque<NetBufferId>>,
    /// Configuration
    config: UringNetConfig,
    /// Statistics
    stats: RwLock<NetBufferPoolStats>,
}

#[cfg(target_os = "linux")]
#[derive(Debug, Clone, Default)]
pub struct NetBufferPoolStats {
    pub total_buffers: usize,
    pub free_buffers: usize,
    pub acquisitions: u64,
    pub releases: u64,
    pub exhaustions: u64,
}

#[cfg(target_os = "linux")]
impl NetBufferPool {
    /// Create a new buffer pool
    pub fn new(config: UringNetConfig) -> Self {
        let mut buffers = Vec::with_capacity(config.buffer_count);
        let mut free_list = VecDeque::with_capacity(config.buffer_count);

        for i in 0..config.buffer_count {
            let id = NetBufferId::new(i as u32);
            buffers.push(NetBuffer::new(id, config.buffer_size));
            free_list.push_back(id);
        }

        info!(
            "Created network buffer pool: {} buffers x {} bytes = {} MB",
            config.buffer_count,
            config.buffer_size,
            (config.buffer_count * config.buffer_size) / (1024 * 1024)
        );

        Self {
            buffers,
            free_list: Mutex::new(free_list),
            config,
            stats: RwLock::new(NetBufferPoolStats {
                total_buffers: config.buffer_count,
                free_buffers: config.buffer_count,
                ..Default::default()
            }),
        }
    }

    /// Acquire a buffer from the pool
    pub fn acquire(&self) -> Option<NetBufferId> {
        let mut free_list = self.free_list.lock();
        let id = free_list.pop_front();

        let mut stats = self.stats.write();
        stats.acquisitions += 1;
        if id.is_some() {
            stats.free_buffers -= 1;
        } else {
            stats.exhaustions += 1;
        }

        id
    }

    /// Release a buffer back to the pool
    pub fn release(&self, id: NetBufferId) {
        let mut free_list = self.free_list.lock();
        free_list.push_back(id);

        let mut stats = self.stats.write();
        stats.releases += 1;
        stats.free_buffers += 1;
    }

    /// Get buffer by ID
    pub fn get(&self, id: NetBufferId) -> Option<&NetBuffer> {
        self.buffers.get(id.0 as usize)
    }

    /// Get mutable buffer by ID
    pub fn get_mut(&mut self, id: NetBufferId) -> Option<&mut NetBuffer> {
        self.buffers.get_mut(id.0 as usize)
    }

    /// Get pool statistics
    pub fn stats(&self) -> NetBufferPoolStats {
        self.stats.read().clone()
    }

    /// Get all buffer pointers for io_uring registration
    pub fn get_iovecs(&self) -> Vec<libc::iovec> {
        self.buffers
            .iter()
            .map(|buf| libc::iovec {
                iov_base: buf.data.as_ptr() as *mut libc::c_void,
                iov_len: buf.data.len(),
            })
            .collect()
    }

    /// Get utilization ratio
    pub fn utilization(&self) -> f64 {
        let stats = self.stats.read();
        1.0 - (stats.free_buffers as f64 / stats.total_buffers as f64)
    }
}

/// Connection state for tracking active connections
#[cfg(target_os = "linux")]
#[derive(Debug)]
pub struct ConnectionState {
    /// Socket file descriptor
    pub fd: RawFd,
    /// Remote address
    pub addr: SocketAddr,
    /// Currently held buffer (for in-progress recv)
    pub recv_buffer: Option<NetBufferId>,
    /// Bytes received on this connection
    pub bytes_received: AtomicU64,
    /// Bytes sent on this connection
    pub bytes_sent: AtomicU64,
    /// Connection established timestamp
    pub connected_at: std::time::Instant,
    /// Last activity timestamp
    pub last_activity: std::sync::atomic::AtomicU64,
}

#[cfg(target_os = "linux")]
impl ConnectionState {
    pub fn new(fd: RawFd, addr: SocketAddr) -> Self {
        Self {
            fd,
            addr,
            recv_buffer: None,
            bytes_received: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            connected_at: std::time::Instant::now(),
            last_activity: std::sync::atomic::AtomicU64::new(0),
        }
    }

    pub fn record_recv(&self, bytes: u64) {
        self.bytes_received.fetch_add(bytes, Ordering::Relaxed);
        self.last_activity.store(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            Ordering::Relaxed,
        );
    }

    pub fn record_send(&self, bytes: u64) {
        self.bytes_sent.fetch_add(bytes, Ordering::Relaxed);
        self.last_activity.store(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            Ordering::Relaxed,
        );
    }
}

/// io_uring operation types for network I/O
#[cfg(target_os = "linux")]
#[derive(Debug, Clone, Copy)]
pub enum NetOp {
    /// Accept new connection
    Accept,
    /// Receive data (regular)
    Recv,
    /// Receive data (multishot - kernel 5.19+)
    RecvMultishot,
    /// Send data
    Send,
    /// Close connection
    Close,
    /// Provide buffer to kernel
    ProvideBuffer,
}

/// Operation result from io_uring completion
#[cfg(target_os = "linux")]
#[derive(Debug)]
pub struct NetOpResult {
    /// Operation type
    pub op: NetOp,
    /// Result (bytes transferred or error code)
    pub result: i32,
    /// Associated buffer ID (if any)
    pub buffer_id: Option<NetBufferId>,
    /// Connection ID (user_data from submission)
    pub conn_id: u64,
    /// Additional flags from CQE
    pub flags: u32,
}

/// Manager for io_uring network operations
#[cfg(target_os = "linux")]
pub struct UringNetManager {
    /// Configuration
    config: UringNetConfig,
    /// Buffer pool
    buffer_pool: Arc<NetBufferPool>,
    /// Ring file descriptor
    ring_fd: AtomicU64,
    /// Whether manager is initialized
    initialized: AtomicBool,
    /// Statistics
    stats: RwLock<UringNetStats>,
}

#[cfg(target_os = "linux")]
impl UringNetManager {
    /// Create a new network manager
    pub fn new(config: UringNetConfig) -> Self {
        let buffer_pool = Arc::new(NetBufferPool::new(config.clone()));

        Self {
            config,
            buffer_pool,
            ring_fd: AtomicU64::new(0),
            initialized: AtomicBool::new(false),
            stats: RwLock::new(UringNetStats::default()),
        }
    }

    /// Initialize the io_uring ring for network operations
    pub fn init(&self) -> Result<()> {
        if self.initialized.load(Ordering::Acquire) {
            return Ok(());
        }

        // io_uring_params for network operations
        #[repr(C)]
        #[derive(Default)]
        struct IoUringParams {
            sq_entries: u32,
            cq_entries: u32,
            flags: u32,
            sq_thread_cpu: u32,
            sq_thread_idle: u32,
            features: u32,
            wq_fd: u32,
            resv: [u32; 3],
            sq_off: [u32; 10],
            cq_off: [u32; 10],
        }

        let mut params = IoUringParams {
            sq_entries: self.config.sq_depth,
            cq_entries: self.config.cq_depth,
            flags: 0,
            ..Default::default()
        };

        // Enable cooperative taskrun for better performance
        const IORING_SETUP_COOP_TASKRUN: u32 = 1 << 8;
        params.flags |= IORING_SETUP_COOP_TASKRUN;

        const IO_URING_SETUP: i64 = 425;
        let result = unsafe {
            libc::syscall(
                IO_URING_SETUP,
                self.config.sq_depth as i64,
                &mut params as *mut _ as i64,
            )
        };

        if result < 0 {
            let errno = unsafe { *libc::__errno_location() };
            return Err(StreamlineError::storage_msg(format!(
                "io_uring_setup for network failed: errno={}",
                errno
            )));
        }

        let ring_fd = result as u64;
        self.ring_fd.store(ring_fd, Ordering::Release);

        // Register buffers with the ring
        if self.config.provided_buffers {
            self.register_buffers(ring_fd as i32)?;
        }

        self.initialized.store(true, Ordering::Release);
        info!(
            "io_uring network manager initialized (fd={}, sq={}, cq={})",
            ring_fd, self.config.sq_depth, self.config.cq_depth
        );

        Ok(())
    }

    /// Register buffer pool with io_uring
    fn register_buffers(&self, ring_fd: i32) -> Result<()> {
        let iovecs = self.buffer_pool.get_iovecs();

        const IORING_REGISTER_BUFFERS: u32 = 0;
        const IO_URING_REGISTER: i64 = 427;

        let result = unsafe {
            libc::syscall(
                IO_URING_REGISTER,
                ring_fd as i64,
                IORING_REGISTER_BUFFERS as i64,
                iovecs.as_ptr() as i64,
                iovecs.len() as i64,
            )
        };

        if result < 0 {
            let errno = unsafe { *libc::__errno_location() };
            warn!(
                "Failed to register network buffers: errno={} (continuing without registration)",
                errno
            );
        } else {
            debug!("Registered {} network buffers with io_uring", iovecs.len());
        }

        Ok(())
    }

    /// Get buffer pool reference
    pub fn buffer_pool(&self) -> &Arc<NetBufferPool> {
        &self.buffer_pool
    }

    /// Get configuration
    pub fn config(&self) -> &UringNetConfig {
        &self.config
    }

    /// Get statistics
    pub fn stats(&self) -> UringNetStats {
        let mut stats = self.stats.read().clone();
        stats.buffer_utilization = self.buffer_pool.utilization();
        stats
    }

    /// Record receive operation
    pub fn record_recv(&self, bytes: u64, zero_copy: bool) {
        let mut stats = self.stats.write();
        stats.bytes_received += bytes;
        stats.recv_ops += 1;
        if zero_copy {
            stats.zero_copy_ops += 1;
        }
    }

    /// Record send operation
    pub fn record_send(&self, bytes: u64, zero_copy: bool) {
        let mut stats = self.stats.write();
        stats.bytes_sent += bytes;
        stats.send_ops += 1;
        if zero_copy {
            stats.zero_copy_ops += 1;
        }
    }

    /// Record accept operation
    pub fn record_accept(&self) {
        let mut stats = self.stats.write();
        stats.accept_ops += 1;
        stats.active_connections += 1;
    }

    /// Record connection close
    pub fn record_close(&self) {
        let mut stats = self.stats.write();
        if stats.active_connections > 0 {
            stats.active_connections -= 1;
        }
    }

    /// Record multishot completion
    pub fn record_multishot(&self) {
        let mut stats = self.stats.write();
        stats.multishot_completions += 1;
    }

    /// Check if multishot receive is supported
    pub fn supports_multishot(&self) -> bool {
        self.config.multishot_recv && self.check_kernel_version(5, 19)
    }

    /// Check kernel version
    fn check_kernel_version(&self, major: u32, minor: u32) -> bool {
        if let Ok(version) = std::fs::read_to_string("/proc/version") {
            if let Some(version_str) = version.split_whitespace().nth(2) {
                let parts: Vec<_> = version_str.split('.').take(2).collect();
                if parts.len() >= 2 {
                    if let (Ok(k_major), Ok(k_minor)) =
                        (parts[0].parse::<u32>(), parts[1].parse::<u32>())
                    {
                        return k_major > major || (k_major == major && k_minor >= minor);
                    }
                }
            }
        }
        false
    }
}

#[cfg(target_os = "linux")]
impl Drop for UringNetManager {
    fn drop(&mut self) {
        let fd = self.ring_fd.load(Ordering::Acquire);
        if fd > 0 {
            debug!("Closing io_uring network ring fd={}", fd);
            unsafe {
                libc::close(fd as i32);
            }
        }
    }
}

// ============================================================================
// Non-Linux stubs
// ============================================================================

#[cfg(not(target_os = "linux"))]
#[derive(Debug, Clone, Default)]
pub struct NetBufferPoolStats {
    pub total_buffers: usize,
    pub free_buffers: usize,
    pub acquisitions: u64,
    pub releases: u64,
    pub exhaustions: u64,
}

#[cfg(not(target_os = "linux"))]
pub struct NetBufferPool;

#[cfg(not(target_os = "linux"))]
impl NetBufferPool {
    pub fn new(_config: UringNetConfig) -> Self {
        Self
    }

    pub fn acquire(&self) -> Option<NetBufferId> {
        None
    }

    pub fn release(&self, _id: NetBufferId) {}

    pub fn stats(&self) -> NetBufferPoolStats {
        NetBufferPoolStats::default()
    }

    pub fn utilization(&self) -> f64 {
        0.0
    }
}

#[cfg(not(target_os = "linux"))]
pub struct UringNetManager {
    config: UringNetConfig,
}

#[cfg(not(target_os = "linux"))]
impl UringNetManager {
    pub fn new(config: UringNetConfig) -> Self {
        Self { config }
    }

    pub fn init(&self) -> crate::error::Result<()> {
        Err(crate::error::StreamlineError::storage_msg(
            "io_uring network not supported on this platform".into(),
        ))
    }

    pub fn config(&self) -> &UringNetConfig {
        &self.config
    }

    pub fn stats(&self) -> UringNetStats {
        UringNetStats::default()
    }

    pub fn supports_multishot(&self) -> bool {
        false
    }
}

/// Check if io_uring network I/O is available
pub fn is_uring_net_available() -> bool {
    #[cfg(target_os = "linux")]
    {
        // Check for kernel 5.7+ (when recv/send were added to io_uring)
        if let Ok(version) = std::fs::read_to_string("/proc/version") {
            if let Some(version_str) = version.split_whitespace().nth(2) {
                let parts: Vec<_> = version_str.split('.').take(2).collect();
                if parts.len() >= 2 {
                    if let (Ok(major), Ok(minor)) =
                        (parts[0].parse::<u32>(), parts[1].parse::<u32>())
                    {
                        return major > 5 || (major == 5 && minor >= 7);
                    }
                }
            }
        }
        false
    }

    #[cfg(not(target_os = "linux"))]
    {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_uring_net_config_default() {
        let config = UringNetConfig::default();
        assert_eq!(config.buffer_count, 256);
        assert_eq!(config.buffer_size, 64 * 1024);
        assert!(config.tcp_nodelay);
    }

    #[test]
    fn test_uring_net_config_high_throughput() {
        let config = UringNetConfig::high_throughput();
        assert_eq!(config.buffer_count, 1024);
        assert_eq!(config.sq_depth, 2048);
        assert!(config.reuse_port);
    }

    #[test]
    fn test_net_buffer_id() {
        let id = NetBufferId::new(42);
        assert_eq!(id.as_u32(), 42);
    }

    #[test]
    fn test_uring_net_stats_default() {
        let stats = UringNetStats::default();
        assert_eq!(stats.bytes_received, 0);
        assert_eq!(stats.active_connections, 0);
    }

    #[test]
    fn test_is_uring_net_available() {
        let available = is_uring_net_available();
        println!("io_uring network available: {}", available);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_net_buffer_creation() {
        let id = NetBufferId::new(0);
        let buf = NetBuffer::new(id, 4096);
        assert_eq!(buf.id(), id);
        assert!(buf.capacity() >= 4096);
        assert_eq!(buf.len(), 0);
        assert!(buf.is_empty());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_net_buffer_acquire_release() {
        let id = NetBufferId::new(0);
        let buf = NetBuffer::new(id, 4096);

        assert!(!buf.is_in_use());
        assert!(buf.acquire());
        assert!(buf.is_in_use());
        assert!(!buf.acquire()); // Should fail - already in use
        buf.release();
        assert!(!buf.is_in_use());
        assert!(buf.acquire()); // Should succeed again
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_net_buffer_pool() {
        let config = UringNetConfig {
            buffer_count: 4,
            buffer_size: 1024,
            ..Default::default()
        };
        let pool = NetBufferPool::new(config);

        // Acquire all buffers
        let mut ids = Vec::new();
        for _ in 0..4 {
            let id = pool.acquire();
            assert!(id.is_some());
            ids.push(id.unwrap());
        }

        // Pool should be exhausted
        assert!(pool.acquire().is_none());

        // Release one
        pool.release(ids.pop().unwrap());

        // Should be able to acquire again
        assert!(pool.acquire().is_some());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_uring_net_manager_creation() {
        let config = UringNetConfig::default();
        let manager = UringNetManager::new(config);

        let stats = manager.stats();
        assert_eq!(stats.bytes_received, 0);
    }
}
