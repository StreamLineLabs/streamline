//! SQPOLL (Submission Queue Polling) support for io_uring
//!
//! SQPOLL mode enables kernel-side polling of the submission queue, eliminating
//! the need for io_uring_enter syscalls to submit operations. This provides the
//! lowest possible latency for high-throughput I/O workloads.
//!
//! # Requirements
//!
//! - Linux kernel 5.11+ recommended (5.4+ minimum for SQPOLL)
//! - Root or CAP_SYS_NICE capability for CPU pinning
//! - Sufficient system resources for the kernel polling thread
//!
//! # Performance Characteristics
//!
//! - **Latency**: Submitting operations has near-zero latency (no syscalls)
//! - **CPU Usage**: Kernel thread consumes CPU while polling, even when idle
//! - **Idle Timeout**: Thread idles after configurable timeout (sq_thread_idle)
//! - **Best for**: High-throughput, latency-sensitive workloads with continuous I/O
//!
//! # Trade-offs
//!
//! SQPOLL is not always better:
//! - Uses more CPU due to continuous polling
//! - May interfere with CPU scheduling on busy systems
//! - Better suited for dedicated I/O servers than shared systems
//!
//! # Example
//!
//! ```rust,ignore
//! use streamline::storage::io_backend::sqpoll::{SqpollConfig, SqpollManager};
//!
//! let config = SqpollConfig::builder()
//!     .idle_timeout_ms(2000)  // Idle after 2 seconds of no I/O
//!     .cpu_affinity(Some(0)) // Pin to CPU 0
//!     .build();
//!
//! let manager = SqpollManager::new(config)?;
//! ```

#[cfg(target_os = "linux")]
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32, Ordering};
#[cfg(target_os = "linux")]
use std::sync::Arc;
#[cfg(target_os = "linux")]
use tracing::{debug, info, warn};

#[cfg(target_os = "linux")]
use crate::error::{Result, StreamlineError};

/// SQPOLL setup flags (io_uring_setup flags)
#[cfg(target_os = "linux")]
mod flags {
    /// Create submission queue polling thread
    pub const IORING_SETUP_SQPOLL: u32 = 1 << 1;
    /// sq_thread_cpu is valid
    pub const IORING_SETUP_SQ_AFF: u32 = 1 << 2;
    /// Enable completion queue polling
    pub const IORING_SETUP_CQSIZE: u32 = 1 << 3;
    /// Use COOP_TASKRUN semantics
    pub const IORING_SETUP_COOP_TASKRUN: u32 = 1 << 8;
    /// Use single issuer optimization
    pub const IORING_SETUP_SINGLE_ISSUER: u32 = 1 << 12;
    /// Defer taskrun to io_uring_enter
    pub const IORING_SETUP_DEFER_TASKRUN: u32 = 1 << 13;
}

/// Configuration for SQPOLL mode
#[derive(Debug, Clone)]
pub struct SqpollConfig {
    /// Enable SQPOLL mode
    pub enabled: bool,

    /// Idle timeout in milliseconds before kernel thread sleeps
    /// Default: 1000ms (1 second)
    /// Lower values save CPU but may increase latency for bursts
    pub idle_timeout_ms: u32,

    /// Pin the kernel polling thread to a specific CPU
    /// None = no affinity (OS decides)
    /// Some(cpu) = pin to specific CPU core
    pub cpu_affinity: Option<u32>,

    /// Submission queue size (entries)
    /// Default: 256
    pub sq_entries: u32,

    /// Completion queue size (entries)
    /// Default: 256 (must be >= sq_entries)
    pub cq_entries: u32,

    /// Enable cooperative taskrun (reduces interrupts, requires 5.19+)
    pub coop_taskrun: bool,

    /// Single issuer optimization (thread safety trade-off for performance)
    pub single_issuer: bool,

    /// Defer taskrun until io_uring_enter (reduces context switches)
    pub defer_taskrun: bool,
}

impl Default for SqpollConfig {
    fn default() -> Self {
        Self {
            enabled: true, // Enabled by default for 10-15% I/O improvement
            idle_timeout_ms: 1000,
            cpu_affinity: None,
            sq_entries: 256,
            cq_entries: 256,
            coop_taskrun: false,
            single_issuer: false,
            defer_taskrun: false,
        }
    }
}

impl SqpollConfig {
    /// Create a builder for SQPOLL configuration
    pub fn builder() -> SqpollConfigBuilder {
        SqpollConfigBuilder::new()
    }

    /// Create a high-performance config for dedicated I/O servers
    pub fn high_performance() -> Self {
        Self {
            enabled: true,
            idle_timeout_ms: 2000, // 2 second timeout
            cpu_affinity: None,    // Let OS schedule
            sq_entries: 1024,      // Large queues
            cq_entries: 2048,      // CQ should be >= 2x SQ for safety
            coop_taskrun: true,    // Modern kernel optimization
            single_issuer: true,   // Single-threaded optimization
            defer_taskrun: true,   // Reduce context switches
        }
    }

    /// Create a balanced config suitable for most workloads
    pub fn balanced() -> Self {
        Self {
            enabled: true,
            idle_timeout_ms: 500, // Quick idle
            cpu_affinity: None,
            sq_entries: 256,
            cq_entries: 512,
            coop_taskrun: false, // Safer defaults
            single_issuer: false,
            defer_taskrun: false,
        }
    }

    /// Compute the setup flags for io_uring_setup
    #[cfg(target_os = "linux")]
    pub fn setup_flags(&self) -> u32 {
        let mut flags = 0u32;

        if self.enabled {
            flags |= flags::IORING_SETUP_SQPOLL;
        }

        if self.cpu_affinity.is_some() {
            flags |= flags::IORING_SETUP_SQ_AFF;
        }

        if self.cq_entries != self.sq_entries {
            flags |= flags::IORING_SETUP_CQSIZE;
        }

        if self.coop_taskrun {
            flags |= flags::IORING_SETUP_COOP_TASKRUN;
        }

        if self.single_issuer {
            flags |= flags::IORING_SETUP_SINGLE_ISSUER;
        }

        if self.defer_taskrun {
            flags |= flags::IORING_SETUP_DEFER_TASKRUN;
        }

        flags
    }
}

/// Builder for SQPOLL configuration
#[derive(Debug, Default)]
pub struct SqpollConfigBuilder {
    config: SqpollConfig,
}

impl SqpollConfigBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            config: SqpollConfig {
                enabled: true, // Builder implies intent to enable
                ..Default::default()
            },
        }
    }

    /// Set the idle timeout in milliseconds
    pub fn idle_timeout_ms(mut self, timeout: u32) -> Self {
        self.config.idle_timeout_ms = timeout;
        self
    }

    /// Set CPU affinity for the kernel polling thread
    pub fn cpu_affinity(mut self, cpu: Option<u32>) -> Self {
        self.config.cpu_affinity = cpu;
        self
    }

    /// Set submission queue size
    pub fn sq_entries(mut self, entries: u32) -> Self {
        self.config.sq_entries = entries;
        self
    }

    /// Set completion queue size
    pub fn cq_entries(mut self, entries: u32) -> Self {
        self.config.cq_entries = entries;
        self
    }

    /// Enable cooperative taskrun (requires kernel 5.19+)
    pub fn coop_taskrun(mut self, enabled: bool) -> Self {
        self.config.coop_taskrun = enabled;
        self
    }

    /// Enable single issuer optimization
    pub fn single_issuer(mut self, enabled: bool) -> Self {
        self.config.single_issuer = enabled;
        self
    }

    /// Enable deferred taskrun
    pub fn defer_taskrun(mut self, enabled: bool) -> Self {
        self.config.defer_taskrun = enabled;
        self
    }

    /// Build the configuration
    pub fn build(self) -> SqpollConfig {
        self.config
    }
}

/// Statistics for SQPOLL operation
#[derive(Debug, Clone, Default)]
pub struct SqpollStats {
    /// Whether SQPOLL is currently active
    pub active: bool,
    /// Number of submissions without syscalls
    pub zero_syscall_submits: u64,
    /// Number of times kernel thread was woken
    pub wakeups: u64,
    /// Current kernel thread state (if available)
    pub thread_idle: bool,
}

/// SQPOLL manager for creating and managing SQPOLL-enabled io_uring rings
#[cfg(target_os = "linux")]
pub struct SqpollManager {
    /// Configuration
    config: SqpollConfig,
    /// Ring file descriptor (if setup was successful)
    ring_fd: AtomicI32,
    /// Whether SQPOLL is active
    active: AtomicBool,
    /// Submission count (for statistics)
    submission_count: AtomicU32,
}

#[cfg(target_os = "linux")]
impl SqpollManager {
    /// Create a new SQPOLL manager with the given configuration
    pub fn new(config: SqpollConfig) -> Result<Self> {
        info!(
            "Creating SQPOLL manager (enabled={}, timeout={}ms, cpu={:?})",
            config.enabled, config.idle_timeout_ms, config.cpu_affinity
        );

        Ok(Self {
            config,
            ring_fd: AtomicI32::new(-1),
            active: AtomicBool::new(false),
            submission_count: AtomicU32::new(0),
        })
    }

    /// Setup io_uring with SQPOLL mode
    ///
    /// This performs the io_uring_setup syscall with SQPOLL flags.
    /// Returns the ring file descriptor on success.
    pub fn setup_ring(&self) -> Result<i32> {
        if !self.config.enabled {
            return Err(StreamlineError::Config(
                "SQPOLL is not enabled in configuration".into(),
            ));
        }

        let flags = self.config.setup_flags();
        debug!("Setting up io_uring with flags: 0x{:x}", flags);

        // io_uring_params structure
        #[repr(C)]
        struct IoUringParams {
            sq_entries: u32,
            cq_entries: u32,
            flags: u32,
            sq_thread_cpu: u32,
            sq_thread_idle: u32,
            features: u32,
            wq_fd: u32,
            resv: [u32; 3],
            sq_off: SqRingOffsets,
            cq_off: CqRingOffsets,
        }

        #[repr(C)]
        #[derive(Default)]
        struct SqRingOffsets {
            head: u32,
            tail: u32,
            ring_mask: u32,
            ring_entries: u32,
            flags: u32,
            dropped: u32,
            array: u32,
            resv1: u32,
            user_addr: u64,
        }

        #[repr(C)]
        #[derive(Default)]
        struct CqRingOffsets {
            head: u32,
            tail: u32,
            ring_mask: u32,
            ring_entries: u32,
            overflow: u32,
            cqes: u32,
            flags: u32,
            resv1: u32,
            user_addr: u64,
        }

        let mut params = IoUringParams {
            sq_entries: self.config.sq_entries,
            cq_entries: self.config.cq_entries,
            flags,
            sq_thread_cpu: self.config.cpu_affinity.unwrap_or(0),
            sq_thread_idle: self.config.idle_timeout_ms,
            features: 0,
            wq_fd: 0,
            resv: [0; 3],
            sq_off: SqRingOffsets::default(),
            cq_off: CqRingOffsets::default(),
        };

        // io_uring_setup syscall number
        const IO_URING_SETUP: i64 = 425;

        // SAFETY: io_uring_setup syscall is safe because params is a valid, properly
        // initialized IoUringParams struct with correct layout (#[repr(C)]), and
        // sq_entries is a valid u32. The kernel validates all parameters and returns
        // an error code on failure. The returned fd is stored and closed in Drop.
        let result = unsafe {
            libc::syscall(
                IO_URING_SETUP,
                self.config.sq_entries as i64,
                &mut params as *mut _ as i64,
            )
        };

        if result < 0 {
            // SAFETY: __errno_location returns a valid pointer to the thread-local
            // errno value. Reading it immediately after a failed syscall is correct.
            let errno = unsafe { *libc::__errno_location() };
            let error_msg = match errno {
                libc::EPERM => "Permission denied - SQPOLL requires root or CAP_SYS_NICE",
                libc::EINVAL => "Invalid parameters - check kernel version and flags",
                libc::ENOMEM => "Out of memory - reduce queue sizes",
                libc::ENOSYS => "io_uring not supported on this kernel",
                _ => "Unknown error",
            };
            warn!("io_uring_setup failed: {} (errno={})", error_msg, errno);
            return Err(StreamlineError::storage_msg(format!(
                "io_uring_setup failed: {} (errno={})",
                error_msg, errno
            )));
        }

        let fd = result as i32;
        self.ring_fd.store(fd, Ordering::Release);
        self.active.store(true, Ordering::Release);

        info!(
            "io_uring SQPOLL setup successful (fd={}, features=0x{:x})",
            fd, params.features
        );

        Ok(fd)
    }

    /// Get the ring file descriptor
    pub fn ring_fd(&self) -> Option<i32> {
        let fd = self.ring_fd.load(Ordering::Acquire);
        if fd >= 0 {
            Some(fd)
        } else {
            None
        }
    }

    /// Check if SQPOLL is active
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }

    /// Get the configuration
    pub fn config(&self) -> &SqpollConfig {
        &self.config
    }

    /// Get statistics
    pub fn stats(&self) -> SqpollStats {
        SqpollStats {
            active: self.is_active(),
            zero_syscall_submits: self.submission_count.load(Ordering::Relaxed) as u64,
            wakeups: 0,         // Would need kernel interface to track
            thread_idle: false, // Would need to read from ring
        }
    }

    /// Increment submission count
    pub fn record_submission(&self) {
        self.submission_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Wake up the kernel SQPOLL thread if it's idle
    ///
    /// This is useful when you know the thread may be sleeping and you want
    /// to ensure prompt processing of new submissions.
    pub fn wakeup(&self) -> Result<()> {
        let fd = self.ring_fd.load(Ordering::Acquire);
        if fd < 0 {
            return Err(StreamlineError::storage_msg(
                "SQPOLL ring not initialized".into(),
            ));
        }

        // io_uring_enter with IORING_ENTER_SQ_WAKEUP flag
        const IO_URING_ENTER: i64 = 426;
        const IORING_ENTER_SQ_WAKEUP: u32 = 1 << 1;

        // SAFETY: io_uring_enter syscall is safe because the ring fd was obtained
        // from a successful io_uring_setup call and is still valid (checked above).
        // The IORING_ENTER_SQ_WAKEUP flag is a valid flag for waking the SQPOLL
        // thread. All other arguments are zero/null. Errors are checked via return value.
        let result = unsafe {
            libc::syscall(
                IO_URING_ENTER,
                fd as i64,
                0i64, // to_submit
                0i64, // min_complete
                IORING_ENTER_SQ_WAKEUP as i64,
                std::ptr::null::<libc::c_void>() as i64,
                0i64,
            )
        };

        if result < 0 {
            // SAFETY: __errno_location returns a valid pointer to the thread-local
            // errno value. Reading it immediately after a failed syscall is correct.
            let errno = unsafe { *libc::__errno_location() };
            return Err(StreamlineError::storage_msg(format!(
                "SQPOLL wakeup failed: errno={}",
                errno
            )));
        }

        Ok(())
    }
}

#[cfg(target_os = "linux")]
impl Drop for SqpollManager {
    fn drop(&mut self) {
        let fd = self.ring_fd.load(Ordering::Acquire);
        if fd >= 0 {
            debug!("Closing SQPOLL ring fd={}", fd);
            // SAFETY: The fd was obtained from a successful io_uring_setup syscall
            // and is valid. Drop is called only once, so double-close cannot occur.
            unsafe {
                libc::close(fd);
            }
        }
    }
}

// Non-Linux stub implementation
#[cfg(not(target_os = "linux"))]
pub struct SqpollManager {
    config: SqpollConfig,
}

#[cfg(not(target_os = "linux"))]
impl SqpollManager {
    pub fn new(config: SqpollConfig) -> crate::error::Result<Self> {
        Ok(Self { config })
    }

    pub fn setup_ring(&self) -> crate::error::Result<i32> {
        Err(crate::error::StreamlineError::storage_msg(
            "SQPOLL not supported on this platform".into(),
        ))
    }

    pub fn ring_fd(&self) -> Option<i32> {
        None
    }

    pub fn is_active(&self) -> bool {
        false
    }

    pub fn config(&self) -> &SqpollConfig {
        &self.config
    }

    pub fn stats(&self) -> SqpollStats {
        SqpollStats::default()
    }

    pub fn record_submission(&self) {}

    pub fn wakeup(&self) -> crate::error::Result<()> {
        Err(crate::error::StreamlineError::storage_msg(
            "SQPOLL not supported on this platform".into(),
        ))
    }
}

/// Check if SQPOLL is available on this system
pub fn is_sqpoll_available() -> bool {
    #[cfg(target_os = "linux")]
    {
        // Check kernel version - SQPOLL requires 5.4+
        if let Ok(version) = std::fs::read_to_string("/proc/version") {
            if let Some(version_str) = version.split_whitespace().nth(2) {
                let parts: Vec<_> = version_str.split('.').take(2).collect();
                if parts.len() >= 2 {
                    if let (Ok(major), Ok(minor)) =
                        (parts[0].parse::<u32>(), parts[1].parse::<u32>())
                    {
                        return major > 5 || (major == 5 && minor >= 4);
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

/// Check if SQPOLL requires elevated privileges on this system
pub fn sqpoll_requires_root() -> bool {
    #[cfg(target_os = "linux")]
    {
        // Check if we have CAP_SYS_NICE capability
        use std::process::Command;
        if let Ok(output) = Command::new("capsh").args(["--print"]).output() {
            let stdout = String::from_utf8_lossy(&output.stdout);
            if stdout.contains("cap_sys_nice") {
                return false;
            }
        }
        // Check if we're root
        // SAFETY: geteuid is always safe to call — it has no preconditions,
        // takes no arguments, and simply returns the effective user ID.
        unsafe { libc::geteuid() != 0 }
    }

    #[cfg(not(target_os = "linux"))]
    {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sqpoll_config_default() {
        let config = SqpollConfig::default();
        assert!(config.enabled); // Enabled by default for better I/O performance
        assert_eq!(config.idle_timeout_ms, 1000);
        assert_eq!(config.sq_entries, 256);
    }

    #[test]
    fn test_sqpoll_config_builder() {
        let config = SqpollConfig::builder()
            .idle_timeout_ms(500)
            .cpu_affinity(Some(0))
            .sq_entries(512)
            .cq_entries(1024)
            .build();

        assert!(config.enabled);
        assert_eq!(config.idle_timeout_ms, 500);
        assert_eq!(config.cpu_affinity, Some(0));
        assert_eq!(config.sq_entries, 512);
        assert_eq!(config.cq_entries, 1024);
    }

    #[test]
    fn test_sqpoll_config_high_performance() {
        let config = SqpollConfig::high_performance();
        assert!(config.enabled);
        assert!(config.coop_taskrun);
        assert!(config.single_issuer);
        assert!(config.defer_taskrun);
    }

    #[test]
    fn test_sqpoll_config_balanced() {
        let config = SqpollConfig::balanced();
        assert!(config.enabled);
        assert!(!config.coop_taskrun);
        assert!(!config.single_issuer);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_sqpoll_setup_flags() {
        let config = SqpollConfig {
            enabled: true,
            cpu_affinity: Some(0),
            coop_taskrun: true,
            ..Default::default()
        };

        let flags = config.setup_flags();
        assert!(flags & flags::IORING_SETUP_SQPOLL != 0);
        assert!(flags & flags::IORING_SETUP_SQ_AFF != 0);
        assert!(flags & flags::IORING_SETUP_COOP_TASKRUN != 0);
    }

    #[test]
    fn test_sqpoll_availability() {
        let available = is_sqpoll_available();
        println!("SQPOLL available: {}", available);
    }

    #[test]
    fn test_sqpoll_manager_creation() {
        let config = SqpollConfig::default();
        let manager = SqpollManager::new(config).unwrap();
        assert!(!manager.is_active());
        assert!(manager.ring_fd().is_none());
    }

    #[test]
    fn test_sqpoll_stats_default() {
        let stats = SqpollStats::default();
        assert!(!stats.active);
        assert_eq!(stats.zero_syscall_submits, 0);
    }
}
