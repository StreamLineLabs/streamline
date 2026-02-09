//! I/O backend abstraction for Streamline storage
//!
//! This module provides a platform-independent abstraction over file I/O operations,
//! with support for both standard I/O (all platforms) and io_uring (Linux).
//!
//! # Design
//!
//! The abstraction is built around two main traits:
//!
//! - `AsyncFile`: Operations on an open file (read, write, sync)
//! - `AsyncFileSystem`: Opening and creating files
//!
//! # Buffer Ownership Model
//!
//! Unlike traditional I/O APIs, this abstraction uses an ownership-based model
//! where buffers are moved to the I/O operation and returned with the result:
//!
//! ```rust,ignore
//! let buf = vec![0u8; 4096];
//! let (result, buf) = file.read_at(buf, offset).await;
//! let bytes_read = result?;
//! // buf is returned and can be reused
//! ```
//!
//! This ownership model is required for io_uring compatibility, where buffers
//! must be owned by the kernel during the I/O operation.
//!
//! # Backend Selection
//!
//! The backend is selected at compile time based on the platform and feature flags:
//!
//! - **Linux with `io-uring` feature**: Uses io_uring backend
//! - **All other platforms**: Uses standard backend (spawn_blocking)
//!
//! # Example
//!
//! ```rust,ignore
//! use streamline::storage::io_backend::{AsyncFileSystem, get_default_backend};
//!
//! let backend = get_default_backend();
//! let file = backend.open("/path/to/file").await?;
//!
//! // Read with buffer ownership
//! let buf = vec![0u8; 1024];
//! let (result, buf) = file.read_at(buf, 0).await;
//! let bytes_read = result?;
//! ```

pub mod buffer_pool;
pub mod direct;
pub mod sqpoll;
pub mod standard;
pub mod sync_direct;
pub mod types;
pub mod uring_advanced;
pub mod uring_net;
pub mod uring_registration;

#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub mod uring;
#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub mod worker;

use crate::error::Result;
use async_trait::async_trait;
use std::path::Path;

pub use buffer_pool::{BufferPoolStats, IoBufferPool};
pub use direct::{
    align_down, align_up, allocate_aligned, is_aligned, is_direct_io_available, DirectFile,
    DirectFileSystem, DirectIoConfig, DirectIoStats, DEFAULT_ALIGNMENT, MIN_ALIGNMENT,
};
pub use sqpoll::{
    is_sqpoll_available, sqpoll_requires_root, SqpollConfig, SqpollConfigBuilder, SqpollManager,
    SqpollStats,
};
pub use standard::{StandardFile, StandardFileSystem};
pub use sync_direct::{
    is_sync_direct_available, SyncDirectConfig, SyncDirectFile, SyncDirectStats,
};
pub use types::{FileId, IoResult, IoStats, OpenMode};
pub use uring_advanced::{
    AdvancedUringManager, AdvancedUringManagerStats, Batch, BatchBuilder, BatchConfig, BatchOp,
    BatchOpResult, FixedFileConfig, FixedFileId, FixedFileRegistry, FixedFileStats,
    RegisteredBufferConfig, RegisteredBufferId, RegisteredBufferPool, RegisteredBufferStats,
};
pub use uring_net::{
    is_uring_net_available, NetBufferId, NetBufferPoolStats, UringNetConfig, UringNetManager,
    UringNetStats,
};
pub use uring_registration::{RegistrationStats, UringRegistrationManager, UringRingHandle};

#[cfg(target_os = "linux")]
pub use uring_net::{ConnectionState, NetBuffer, NetBufferPool, NetOp, NetOpResult};

// Linux-only registration types
#[cfg(target_os = "linux")]
pub use uring_registration::{BufferRegistration, FileRegistration, RegisteredBuffer};

#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub use uring::{
    EnhancedUringConfig, EnhancedUringFile, EnhancedUringFileSystem, EnhancedUringStats, UringFile,
    UringFileSystem,
};
#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub use worker::{IoWorker, IoWorkerConfig};

/// Async file operations trait
///
/// This trait provides an async interface for file I/O operations that works
/// across both standard I/O and io_uring backends. The ownership model (moving
/// buffers in and out) is required for io_uring compatibility.
#[async_trait]
pub trait AsyncFile: Send + Sync {
    /// Read bytes at a specific offset
    ///
    /// The buffer is moved to the kernel and returned after the operation.
    /// This ownership model is required for io_uring compatibility.
    ///
    /// # Returns
    ///
    /// A tuple of (Result<bytes_read>, buffer). The buffer is always returned
    /// even if the operation fails, allowing it to be reused.
    async fn read_at(&self, buf: Vec<u8>, offset: u64) -> IoResult<usize>;

    /// Write bytes at a specific offset
    ///
    /// The buffer is moved to the kernel and returned after the operation.
    ///
    /// # Returns
    ///
    /// A tuple of (Result<bytes_written>, buffer). The buffer is always returned
    /// even if the operation fails.
    async fn write_at(&self, buf: Vec<u8>, offset: u64) -> IoResult<usize>;

    /// Append bytes to the end of the file
    ///
    /// # Returns
    ///
    /// A tuple of (Result<bytes_written>, buffer).
    async fn append(&self, buf: Vec<u8>) -> IoResult<usize>;

    /// Sync file data to disk (fdatasync)
    ///
    /// This is faster than sync_all as it doesn't sync metadata.
    async fn sync_data(&self) -> Result<()>;

    /// Sync file data and metadata to disk (fsync)
    async fn sync_all(&self) -> Result<()>;

    /// Get the current file size
    async fn size(&self) -> Result<u64>;

    /// Pre-allocate space for the file (fallocate on Linux)
    ///
    /// This can improve performance by reducing fragmentation and ensuring
    /// space is available before writes.
    async fn allocate(&self, len: u64) -> Result<()>;
}

/// File system operations trait
///
/// This trait provides an async interface for opening and creating files.
#[async_trait]
pub trait AsyncFileSystem: Send + Sync {
    /// The file type returned by this filesystem
    type File: AsyncFile;

    /// Open a file for reading
    async fn open(&self, path: &Path) -> Result<Self::File>;

    /// Open a file for reading and writing
    async fn open_rw(&self, path: &Path) -> Result<Self::File>;

    /// Create a new file (truncate if exists)
    async fn create(&self, path: &Path) -> Result<Self::File>;

    /// Open or create a file for appending
    async fn open_append(&self, path: &Path) -> Result<Self::File>;
}

/// Get the default standard I/O backend
///
/// Returns a StandardFileSystem that works on all platforms.
pub fn get_standard_backend() -> StandardFileSystem {
    StandardFileSystem::new()
}

/// Check if io_uring backend is available and recommended
///
/// Returns true if running on Linux with kernel 5.11+ and io-uring feature enabled.
pub fn should_use_uring() -> bool {
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    {
        uring::is_uring_available()
    }

    #[cfg(not(all(target_os = "linux", feature = "io-uring")))]
    {
        false
    }
}

/// Get the recommended io_uring backend (Linux only)
///
/// Returns UringFileSystem if available, panics otherwise.
/// Use `should_use_uring()` to check availability first.
#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub fn get_uring_backend() -> UringFileSystem {
    UringFileSystem::new()
}

/// Backend configuration
#[derive(Debug, Clone)]
pub struct IoBackendConfig {
    /// Enable io_uring backend (if available)
    pub enable_io_uring: bool,

    /// Buffer pool configuration
    pub buffer_pool_size: usize,

    /// io_uring specific configuration
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    pub uring: IoWorkerConfig,
}

impl Default for IoBackendConfig {
    fn default() -> Self {
        Self {
            enable_io_uring: true,
            buffer_pool_size: 64,
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            uring: IoWorkerConfig::default(),
        }
    }
}

// =============================================================================
// Automatic Backend Selection
// =============================================================================

/// Available I/O backend types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoBackendType {
    /// Standard I/O using spawn_blocking (all platforms)
    Standard,
    /// Direct I/O bypassing OS page cache (O_DIRECT on Linux, F_NOCACHE on macOS)
    Direct,
    /// io_uring (Linux 5.11+ only)
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    Uring,
    /// Enhanced io_uring with registered buffers and fixed files
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    EnhancedUring,
}

impl std::fmt::Display for IoBackendType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IoBackendType::Standard => write!(f, "standard"),
            IoBackendType::Direct => write!(f, "direct"),
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            IoBackendType::Uring => write!(f, "io_uring"),
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            IoBackendType::EnhancedUring => write!(f, "enhanced_io_uring"),
        }
    }
}

/// Backend selection strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BackendSelectionStrategy {
    /// Automatically select the best available backend
    #[default]
    Auto,
    /// Force standard I/O backend
    ForceStandard,
    /// Force direct I/O backend (O_DIRECT on Linux, F_NOCACHE on macOS)
    ForceDirect,
    /// Force io_uring backend (will fail if not available)
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    ForceUring,
    /// Force enhanced io_uring backend (will fail if not available)
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    ForceEnhancedUring,
}

/// Configuration for automatic backend selection
#[derive(Debug, Clone)]
pub struct AutoBackendConfig {
    /// Selection strategy
    pub strategy: BackendSelectionStrategy,
    /// Direct I/O configuration (used when strategy is ForceDirect)
    pub direct_config: Option<DirectIoConfig>,
    /// Enable enhanced features when using io_uring
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    pub enhanced_config: Option<EnhancedUringConfig>,
    /// Fallback to standard I/O if io_uring initialization fails
    pub fallback_on_error: bool,
}

impl Default for AutoBackendConfig {
    fn default() -> Self {
        Self {
            strategy: BackendSelectionStrategy::Auto,
            direct_config: None,
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            enhanced_config: None,
            fallback_on_error: true,
        }
    }
}

/// Runtime information about backend selection
#[derive(Debug, Clone)]
pub struct BackendInfo {
    /// Selected backend type
    pub backend_type: IoBackendType,
    /// Whether io_uring is available on this system
    pub uring_available: bool,
    /// Kernel version (Linux only)
    pub kernel_version: Option<String>,
    /// Reason for backend selection
    pub selection_reason: String,
}

/// Detect runtime environment and determine best backend
pub fn detect_backend() -> BackendInfo {
    let uring_available = should_use_uring();

    // Get kernel version on Linux
    let kernel_version = {
        #[cfg(target_os = "linux")]
        {
            std::fs::read_to_string("/proc/version")
                .ok()
                .and_then(|v| v.split_whitespace().nth(2).map(|s| s.to_string()))
        }
        #[cfg(not(target_os = "linux"))]
        {
            None
        }
    };

    let (backend_type, selection_reason) = if uring_available {
        #[cfg(all(target_os = "linux", feature = "io-uring"))]
        {
            (
                IoBackendType::Uring,
                "io_uring available on Linux 5.11+".to_string(),
            )
        }
        #[cfg(not(all(target_os = "linux", feature = "io-uring")))]
        {
            (
                IoBackendType::Standard,
                "io_uring feature not compiled".to_string(),
            )
        }
    } else {
        let reason = if cfg!(not(target_os = "linux")) {
            "Not running on Linux"
        } else if cfg!(not(feature = "io-uring")) {
            "io-uring feature not enabled"
        } else {
            "Kernel version < 5.11 or io_uring not available"
        };
        (IoBackendType::Standard, reason.to_string())
    };

    BackendInfo {
        backend_type,
        uring_available,
        kernel_version,
        selection_reason,
    }
}

/// Dynamic file that wraps either standard or io_uring file
pub enum DynamicFile {
    /// Standard I/O file
    Standard(StandardFile),
    /// Direct I/O file (O_DIRECT)
    Direct(DirectFile),
    /// io_uring file
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    Uring(UringFile),
    /// Enhanced io_uring file
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    EnhancedUring(EnhancedUringFile),
}

impl DynamicFile {
    /// Get the backend type for this file
    pub fn backend_type(&self) -> IoBackendType {
        match self {
            DynamicFile::Standard(_) => IoBackendType::Standard,
            DynamicFile::Direct(_) => IoBackendType::Direct,
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            DynamicFile::Uring(_) => IoBackendType::Uring,
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            DynamicFile::EnhancedUring(_) => IoBackendType::EnhancedUring,
        }
    }
}

// Implement Send + Sync for DynamicFile
unsafe impl Send for DynamicFile {}
unsafe impl Sync for DynamicFile {}

#[async_trait]
impl AsyncFile for DynamicFile {
    async fn read_at(&self, buf: Vec<u8>, offset: u64) -> IoResult<usize> {
        match self {
            DynamicFile::Standard(f) => f.read_at(buf, offset).await,
            DynamicFile::Direct(f) => f.read_at(buf, offset).await,
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            DynamicFile::Uring(f) => f.read_at(buf, offset).await,
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            DynamicFile::EnhancedUring(f) => f.read_at(buf, offset).await,
        }
    }

    async fn write_at(&self, buf: Vec<u8>, offset: u64) -> IoResult<usize> {
        match self {
            DynamicFile::Standard(f) => f.write_at(buf, offset).await,
            DynamicFile::Direct(f) => f.write_at(buf, offset).await,
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            DynamicFile::Uring(f) => f.write_at(buf, offset).await,
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            DynamicFile::EnhancedUring(f) => f.write_at(buf, offset).await,
        }
    }

    async fn append(&self, buf: Vec<u8>) -> IoResult<usize> {
        match self {
            DynamicFile::Standard(f) => f.append(buf).await,
            DynamicFile::Direct(f) => f.append(buf).await,
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            DynamicFile::Uring(f) => f.append(buf).await,
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            DynamicFile::EnhancedUring(f) => f.append(buf).await,
        }
    }

    async fn sync_data(&self) -> Result<()> {
        match self {
            DynamicFile::Standard(f) => f.sync_data().await,
            DynamicFile::Direct(f) => f.sync_data().await,
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            DynamicFile::Uring(f) => f.sync_data().await,
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            DynamicFile::EnhancedUring(f) => f.sync_data().await,
        }
    }

    async fn sync_all(&self) -> Result<()> {
        match self {
            DynamicFile::Standard(f) => f.sync_all().await,
            DynamicFile::Direct(f) => f.sync_all().await,
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            DynamicFile::Uring(f) => f.sync_all().await,
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            DynamicFile::EnhancedUring(f) => f.sync_all().await,
        }
    }

    async fn size(&self) -> Result<u64> {
        match self {
            DynamicFile::Standard(f) => f.size().await,
            DynamicFile::Direct(f) => f.size().await,
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            DynamicFile::Uring(f) => f.size().await,
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            DynamicFile::EnhancedUring(f) => f.size().await,
        }
    }

    async fn allocate(&self, len: u64) -> Result<()> {
        match self {
            DynamicFile::Standard(f) => f.allocate(len).await,
            DynamicFile::Direct(f) => f.allocate(len).await,
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            DynamicFile::Uring(f) => f.allocate(len).await,
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            DynamicFile::EnhancedUring(f) => f.allocate(len).await,
        }
    }
}

/// Dynamic filesystem that automatically selects the best backend
pub struct AutoFileSystem {
    inner: AutoFileSystemInner,
    info: BackendInfo,
}

enum AutoFileSystemInner {
    Standard(StandardFileSystem),
    Direct(DirectFileSystem),
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    Uring(UringFileSystem),
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    EnhancedUring(EnhancedUringFileSystem),
}

impl AutoFileSystem {
    /// Create with automatic backend selection
    pub fn new() -> Self {
        Self::with_config(AutoBackendConfig::default())
    }

    /// Create with custom configuration
    pub fn with_config(config: AutoBackendConfig) -> Self {
        let info = detect_backend();

        let inner = match config.strategy {
            BackendSelectionStrategy::Auto => Self::select_auto(&info, &config),
            BackendSelectionStrategy::ForceStandard => {
                tracing::info!("Forcing standard I/O backend");
                AutoFileSystemInner::Standard(StandardFileSystem::new())
            }
            BackendSelectionStrategy::ForceDirect => {
                tracing::info!("Forcing direct I/O backend (O_DIRECT)");
                let direct = if let Some(direct_config) = config.direct_config {
                    DirectFileSystem::with_config(direct_config)
                } else {
                    DirectFileSystem::new()
                };
                AutoFileSystemInner::Direct(direct)
            }
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            BackendSelectionStrategy::ForceUring => {
                tracing::info!("Forcing io_uring backend");
                AutoFileSystemInner::Uring(UringFileSystem::new())
            }
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            BackendSelectionStrategy::ForceEnhancedUring => {
                tracing::info!("Forcing enhanced io_uring backend");
                let enhanced = if let Some(enhanced_config) = config.enhanced_config {
                    EnhancedUringFileSystem::with_config(enhanced_config)
                } else {
                    EnhancedUringFileSystem::new()
                };
                AutoFileSystemInner::EnhancedUring(enhanced)
            }
        };

        tracing::info!(
            backend = %info.backend_type,
            reason = %info.selection_reason,
            "I/O backend initialized"
        );

        Self { inner, info }
    }

    fn select_auto(
        #[allow(unused_variables)] info: &BackendInfo,
        config: &AutoBackendConfig,
    ) -> AutoFileSystemInner {
        #[cfg(all(target_os = "linux", feature = "io-uring"))]
        {
            if info.uring_available {
                // Use enhanced io_uring if configured, otherwise basic io_uring
                if let Some(ref enhanced_config) = config.enhanced_config {
                    tracing::debug!("Auto-selecting enhanced io_uring backend");
                    return AutoFileSystemInner::EnhancedUring(
                        EnhancedUringFileSystem::with_config(enhanced_config.clone()),
                    );
                }
                tracing::debug!("Auto-selecting io_uring backend");
                return AutoFileSystemInner::Uring(UringFileSystem::new());
            }
        }

        #[cfg(not(all(target_os = "linux", feature = "io-uring")))]
        let _ = config; // Suppress unused variable warning

        tracing::debug!("Auto-selecting standard I/O backend");
        AutoFileSystemInner::Standard(StandardFileSystem::new())
    }

    /// Get information about the selected backend
    pub fn info(&self) -> &BackendInfo {
        &self.info
    }

    /// Get the backend type
    pub fn backend_type(&self) -> IoBackendType {
        self.info.backend_type
    }

    /// Check if using io_uring
    pub fn is_uring(&self) -> bool {
        #[cfg(all(target_os = "linux", feature = "io-uring"))]
        {
            matches!(
                self.inner,
                AutoFileSystemInner::Uring(_) | AutoFileSystemInner::EnhancedUring(_)
            )
        }
        #[cfg(not(all(target_os = "linux", feature = "io-uring")))]
        {
            false
        }
    }
}

impl Default for AutoFileSystem {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl AsyncFileSystem for AutoFileSystem {
    type File = DynamicFile;

    async fn open(&self, path: &Path) -> Result<Self::File> {
        match &self.inner {
            AutoFileSystemInner::Standard(fs) => Ok(DynamicFile::Standard(fs.open(path).await?)),
            AutoFileSystemInner::Direct(fs) => Ok(DynamicFile::Direct(fs.open(path).await?)),
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            AutoFileSystemInner::Uring(fs) => Ok(DynamicFile::Uring(fs.open(path).await?)),
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            AutoFileSystemInner::EnhancedUring(fs) => {
                Ok(DynamicFile::EnhancedUring(fs.open(path).await?))
            }
        }
    }

    async fn open_rw(&self, path: &Path) -> Result<Self::File> {
        match &self.inner {
            AutoFileSystemInner::Standard(fs) => Ok(DynamicFile::Standard(fs.open_rw(path).await?)),
            AutoFileSystemInner::Direct(fs) => Ok(DynamicFile::Direct(fs.open_rw(path).await?)),
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            AutoFileSystemInner::Uring(fs) => Ok(DynamicFile::Uring(fs.open_rw(path).await?)),
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            AutoFileSystemInner::EnhancedUring(fs) => {
                Ok(DynamicFile::EnhancedUring(fs.open_rw(path).await?))
            }
        }
    }

    async fn create(&self, path: &Path) -> Result<Self::File> {
        match &self.inner {
            AutoFileSystemInner::Standard(fs) => Ok(DynamicFile::Standard(fs.create(path).await?)),
            AutoFileSystemInner::Direct(fs) => Ok(DynamicFile::Direct(fs.create(path).await?)),
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            AutoFileSystemInner::Uring(fs) => Ok(DynamicFile::Uring(fs.create(path).await?)),
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            AutoFileSystemInner::EnhancedUring(fs) => {
                Ok(DynamicFile::EnhancedUring(fs.create(path).await?))
            }
        }
    }

    async fn open_append(&self, path: &Path) -> Result<Self::File> {
        match &self.inner {
            AutoFileSystemInner::Standard(fs) => {
                Ok(DynamicFile::Standard(fs.open_append(path).await?))
            }
            AutoFileSystemInner::Direct(fs) => Ok(DynamicFile::Direct(fs.open_append(path).await?)),
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            AutoFileSystemInner::Uring(fs) => Ok(DynamicFile::Uring(fs.open_append(path).await?)),
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            AutoFileSystemInner::EnhancedUring(fs) => {
                Ok(DynamicFile::EnhancedUring(fs.open_append(path).await?))
            }
        }
    }
}

/// Get the recommended filesystem with automatic backend selection
///
/// This is the primary entry point for obtaining an I/O backend.
/// It automatically detects the best available backend for the current platform.
pub fn get_auto_backend() -> AutoFileSystem {
    AutoFileSystem::new()
}

/// Get the recommended filesystem with custom configuration
pub fn get_auto_backend_with_config(config: AutoBackendConfig) -> AutoFileSystem {
    AutoFileSystem::with_config(config)
}

/// Get a direct I/O filesystem (O_DIRECT on Linux, F_NOCACHE on macOS)
///
/// This filesystem bypasses the OS page cache for predictable memory usage.
pub fn get_direct_backend() -> DirectFileSystem {
    DirectFileSystem::new()
}

/// Get a direct I/O filesystem with custom configuration
pub fn get_direct_backend_with_config(config: DirectIoConfig) -> DirectFileSystem {
    DirectFileSystem::with_config(config)
}

#[cfg(test)]
mod auto_backend_tests {
    use super::*;

    #[test]
    fn test_backend_detection() {
        let info = detect_backend();
        println!("Detected backend: {}", info.backend_type);
        println!("io_uring available: {}", info.uring_available);
        println!("Kernel version: {:?}", info.kernel_version);
        println!("Selection reason: {}", info.selection_reason);
    }

    #[test]
    fn test_auto_backend_config_default() {
        let config = AutoBackendConfig::default();
        assert_eq!(config.strategy, BackendSelectionStrategy::Auto);
        assert!(config.fallback_on_error);
    }

    #[test]
    fn test_backend_type_display() {
        assert_eq!(format!("{}", IoBackendType::Standard), "standard");
    }

    #[tokio::test]
    async fn test_auto_filesystem_standard() {
        // Force standard backend for testing
        let config = AutoBackendConfig {
            strategy: BackendSelectionStrategy::ForceStandard,
            direct_config: None,
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            enhanced_config: None,
            fallback_on_error: true,
        };
        let fs = AutoFileSystem::with_config(config);
        assert_eq!(fs.backend_type(), IoBackendType::Standard);
        assert!(!fs.is_uring());
    }

    #[tokio::test]
    async fn test_dynamic_file_operations() {
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test_dynamic.txt");

        // Use standard backend for cross-platform testing
        let config = AutoBackendConfig {
            strategy: BackendSelectionStrategy::ForceStandard,
            direct_config: None,
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            enhanced_config: None,
            fallback_on_error: true,
        };
        let fs = AutoFileSystem::with_config(config);

        // Create and write
        let file = fs.create(&file_path).await.unwrap();
        let data = b"Hello, auto backend!".to_vec();
        let (result, _) = file.write_at(data, 0).await;
        assert_eq!(result.unwrap(), 20);

        // Read back
        let buf = vec![0u8; 20];
        let (result, buf) = file.read_at(buf, 0).await;
        assert_eq!(result.unwrap(), 20);
        assert_eq!(&buf[..], b"Hello, auto backend!");

        // Verify backend type
        assert_eq!(file.backend_type(), IoBackendType::Standard);
    }
}
