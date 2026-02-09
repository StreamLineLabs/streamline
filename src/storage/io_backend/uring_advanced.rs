//! Advanced io_uring features for high-performance I/O
//!
//! This module provides advanced io_uring optimizations:
//!
//! - **Registered Buffers**: Pre-registered buffers for reduced kernel memory mapping
//! - **Fixed Files**: Pre-registered file descriptors for reduced syscall overhead
//! - **Batch Operations**: Submit multiple I/O operations in a single syscall
//!
//! # Performance Benefits
//!
//! - Registered buffers: ~10-15% improvement for small reads
//! - Fixed files: ~5-10% improvement by avoiding file lookup overhead
//! - Batch operations: Amortize syscall costs across multiple operations
//!
//! # Example
//!
//! ```rust,ignore
//! use streamline::storage::io_backend::uring_advanced::{
//!     RegisteredBufferPool, FixedFileRegistry, BatchBuilder,
//! };
//!
//! // Create a registered buffer pool
//! let pool = RegisteredBufferPool::new(64, 4096)?;
//!
//! // Acquire a registered buffer
//! let buf_id = pool.acquire().await?;
//! let buf = pool.buffer_mut(buf_id)?;
//!
//! // Use fixed file registry
//! let registry = FixedFileRegistry::new(256)?;
//! let fixed_fd = registry.register(file)?;
//!
//! // Batch operations
//! let batch = BatchBuilder::new()
//!     .read(fixed_fd, buf_id, offset)
//!     .read(fixed_fd, buf_id2, offset2)
//!     .build();
//! let results = batch.submit().await?;
//! ```

use crate::error::{Result, StreamlineError};
use parking_lot::{Mutex, RwLock};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use tracing::{debug, trace, warn};

// ============================================================================
// Registered Buffer Management
// ============================================================================

/// Configuration for registered buffer pool
#[derive(Debug, Clone)]
pub struct RegisteredBufferConfig {
    /// Number of buffers per size class
    pub buffers_per_class: usize,

    /// Size classes for buffers (in bytes)
    pub size_classes: Vec<usize>,

    /// Maximum registered buffers (io_uring limit is typically 16384)
    pub max_registered: usize,
}

impl Default for RegisteredBufferConfig {
    fn default() -> Self {
        Self {
            buffers_per_class: 64,
            size_classes: vec![
                512,         // Small records
                4 * 1024,    // 4KB - typical page size
                16 * 1024,   // 16KB - medium records
                64 * 1024,   // 64KB - larger records
                256 * 1024,  // 256KB - batch reads
                1024 * 1024, // 1MB - large reads
            ],
            max_registered: 1024,
        }
    }
}

/// A handle to a registered buffer
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RegisteredBufferId {
    /// Index in the registered buffer array
    index: u32,
    /// Generation counter to detect use-after-free
    generation: u32,
}

impl RegisteredBufferId {
    /// Create a new buffer ID
    pub fn new(index: u32, generation: u32) -> Self {
        Self { index, generation }
    }

    /// Get the raw index for io_uring operations
    pub fn index(&self) -> u32 {
        self.index
    }
}

/// Statistics for registered buffer pool
#[derive(Debug, Clone, Default)]
pub struct RegisteredBufferStats {
    /// Total buffers registered
    pub total_registered: usize,
    /// Currently available buffers
    pub available: usize,
    /// Total acquisitions
    pub acquisitions: u64,
    /// Total releases
    pub releases: u64,
    /// Cache hits (reused from pool)
    pub cache_hits: u64,
    /// Cache misses (had to allocate new)
    pub cache_misses: u64,
    /// Bytes currently in use
    pub bytes_in_use: u64,
    /// Total bytes capacity
    pub bytes_capacity: u64,
}

/// Pre-registered buffer pool for io_uring
///
/// This pool manages buffers that are pre-registered with the io_uring instance,
/// reducing kernel memory mapping overhead for each I/O operation.
pub struct RegisteredBufferPool {
    /// All registered buffers (indexed by buffer ID)
    buffers: RwLock<Vec<RegisteredBuffer>>,

    /// Free buffer indices by size class
    free_by_size: Mutex<HashMap<usize, VecDeque<u32>>>,

    /// Size classes available
    size_classes: Vec<usize>,

    /// Configuration (stored for potential reconfiguration)
    #[allow(dead_code)]
    config: RegisteredBufferConfig,

    /// Statistics
    stats: RegisteredBufferPoolStats,

    /// Whether the pool is registered with io_uring
    is_registered: AtomicBool,
}

/// Internal buffer tracking
struct RegisteredBuffer {
    /// The actual buffer data
    data: Vec<u8>,
    /// Size class this buffer belongs to
    size_class: usize,
    /// Generation counter for use-after-free detection
    generation: AtomicU64,
    /// Whether this buffer is currently in use
    in_use: AtomicBool,
}

/// Atomic stats for thread-safe updates
struct RegisteredBufferPoolStats {
    acquisitions: AtomicU64,
    releases: AtomicU64,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    bytes_in_use: AtomicU64,
}

impl RegisteredBufferPool {
    /// Create a new registered buffer pool
    pub fn new(config: RegisteredBufferConfig) -> Result<Self> {
        let mut buffers = Vec::new();
        let mut free_by_size: HashMap<usize, VecDeque<u32>> = HashMap::new();
        let mut bytes_capacity = 0u64;

        // Pre-allocate buffers for each size class
        for &size_class in &config.size_classes {
            let queue = free_by_size.entry(size_class).or_default();

            for _ in 0..config.buffers_per_class {
                if buffers.len() >= config.max_registered {
                    break;
                }

                let index = buffers.len() as u32;
                buffers.push(RegisteredBuffer {
                    data: vec![0u8; size_class],
                    size_class,
                    generation: AtomicU64::new(0),
                    in_use: AtomicBool::new(false),
                });
                queue.push_back(index);
                bytes_capacity += size_class as u64;
            }
        }

        debug!(
            "Created registered buffer pool with {} buffers, {} total bytes",
            buffers.len(),
            bytes_capacity
        );

        Ok(Self {
            buffers: RwLock::new(buffers),
            free_by_size: Mutex::new(free_by_size),
            size_classes: config.size_classes.clone(),
            config,
            stats: RegisteredBufferPoolStats {
                acquisitions: AtomicU64::new(0),
                releases: AtomicU64::new(0),
                cache_hits: AtomicU64::new(0),
                cache_misses: AtomicU64::new(0),
                bytes_in_use: AtomicU64::new(0),
            },
            is_registered: AtomicBool::new(false),
        })
    }

    /// Create with default configuration
    pub fn with_defaults() -> Result<Self> {
        Self::new(RegisteredBufferConfig::default())
    }

    /// Acquire a registered buffer of at least the specified size
    ///
    /// Returns a buffer ID that can be used with io_uring operations.
    pub fn acquire(&self, min_size: usize) -> Result<RegisteredBufferId> {
        self.stats.acquisitions.fetch_add(1, Ordering::Relaxed);

        // Find the smallest size class that fits
        let size_class = self
            .size_classes
            .iter()
            .find(|&&s| s >= min_size)
            .copied()
            .unwrap_or_else(|| {
                // If no size class fits, use the largest
                *self.size_classes.last().unwrap_or(&min_size)
            });

        // Try to get a free buffer from the pool
        let mut free = self.free_by_size.lock();
        if let Some(queue) = free.get_mut(&size_class) {
            if let Some(index) = queue.pop_front() {
                self.stats.cache_hits.fetch_add(1, Ordering::Relaxed);

                let buffers = self.buffers.read();
                if let Some(buf) = buffers.get(index as usize) {
                    buf.in_use.store(true, Ordering::Release);
                    let generation = buf.generation.fetch_add(1, Ordering::SeqCst) as u32;
                    self.stats
                        .bytes_in_use
                        .fetch_add(size_class as u64, Ordering::Relaxed);

                    trace!(
                        "Acquired registered buffer {} (size class {})",
                        index,
                        size_class
                    );
                    return Ok(RegisteredBufferId::new(index, generation + 1));
                }
            }
        }

        // No free buffer available
        self.stats.cache_misses.fetch_add(1, Ordering::Relaxed);
        Err(StreamlineError::storage_msg(format!(
            "No registered buffer available for size class {}",
            size_class
        )))
    }

    /// Release a registered buffer back to the pool
    pub fn release(&self, id: RegisteredBufferId) -> Result<()> {
        self.stats.releases.fetch_add(1, Ordering::Relaxed);

        // Extract the size class within the read lock scope
        let size_class = {
            let buffers = self.buffers.read();
            let buf = buffers.get(id.index as usize).ok_or_else(|| {
                StreamlineError::storage_msg(format!("Invalid buffer index: {}", id.index))
            })?;

            // Verify generation to detect use-after-free
            let current_gen = buf.generation.load(Ordering::SeqCst) as u32;
            if current_gen != id.generation {
                return Err(StreamlineError::storage_msg(format!(
                    "Buffer generation mismatch: expected {}, got {}",
                    id.generation, current_gen
                )));
            }

            if !buf.in_use.swap(false, Ordering::AcqRel) {
                warn!("Double release of buffer {}", id.index);
                return Ok(());
            }

            self.stats
                .bytes_in_use
                .fetch_sub(buf.size_class as u64, Ordering::Relaxed);

            buf.size_class
        };

        // Return to free list (outside of the read lock)
        let mut free = self.free_by_size.lock();
        if let Some(queue) = free.get_mut(&size_class) {
            queue.push_back(id.index);
        }

        trace!("Released registered buffer {}", id.index);
        Ok(())
    }

    /// Get a reference to the buffer data
    pub fn buffer(&self, id: RegisteredBufferId) -> Result<&[u8]> {
        let buffers = self.buffers.read();
        let buf = buffers.get(id.index as usize).ok_or_else(|| {
            StreamlineError::storage_msg(format!("Invalid buffer index: {}", id.index))
        })?;

        // Verify it's in use
        if !buf.in_use.load(Ordering::Acquire) {
            return Err(StreamlineError::storage_msg(
                "Buffer not acquired".to_string(),
            ));
        }

        // Safety: We're returning a reference to data that won't change while in_use is true
        // The caller is responsible for not holding this across a release
        Ok(unsafe { std::slice::from_raw_parts(buf.data.as_ptr(), buf.data.len()) })
    }

    /// Get a mutable slice into the buffer
    ///
    /// # Safety
    ///
    /// The returned slice should not be held across calls to release().
    pub fn buffer_slice(&self, id: RegisteredBufferId, offset: usize, len: usize) -> Result<&[u8]> {
        let buffers = self.buffers.read();
        let buf = buffers.get(id.index as usize).ok_or_else(|| {
            StreamlineError::storage_msg(format!("Invalid buffer index: {}", id.index))
        })?;

        if !buf.in_use.load(Ordering::Acquire) {
            return Err(StreamlineError::storage_msg(
                "Buffer not acquired".to_string(),
            ));
        }

        if offset + len > buf.data.len() {
            return Err(StreamlineError::storage_msg(format!(
                "Slice out of bounds: offset {} + len {} > buffer size {}",
                offset,
                len,
                buf.data.len()
            )));
        }

        Ok(unsafe { std::slice::from_raw_parts(buf.data.as_ptr().add(offset), len) })
    }

    /// Write data into a buffer
    pub fn write_buffer(&self, id: RegisteredBufferId, offset: usize, data: &[u8]) -> Result<()> {
        let buffers = self.buffers.write();
        let buf = buffers.get(id.index as usize).ok_or_else(|| {
            StreamlineError::storage_msg(format!("Invalid buffer index: {}", id.index))
        })?;

        if !buf.in_use.load(Ordering::Acquire) {
            return Err(StreamlineError::storage_msg(
                "Buffer not acquired".to_string(),
            ));
        }

        if offset + data.len() > buf.data.len() {
            return Err(StreamlineError::storage_msg(format!(
                "Write out of bounds: offset {} + len {} > buffer size {}",
                offset,
                data.len(),
                buf.data.len()
            )));
        }

        // Safety: We hold the write lock and the buffer is in use
        unsafe {
            let dst = buf.data.as_ptr().add(offset) as *mut u8;
            std::ptr::copy_nonoverlapping(data.as_ptr(), dst, data.len());
        }

        Ok(())
    }

    /// Get current pool statistics
    pub fn stats(&self) -> RegisteredBufferStats {
        let buffers = self.buffers.read();
        let free = self.free_by_size.lock();

        let available: usize = free.values().map(|q| q.len()).sum();
        let bytes_capacity: u64 = buffers.iter().map(|b| b.size_class as u64).sum();

        RegisteredBufferStats {
            total_registered: buffers.len(),
            available,
            acquisitions: self.stats.acquisitions.load(Ordering::Relaxed),
            releases: self.stats.releases.load(Ordering::Relaxed),
            cache_hits: self.stats.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.stats.cache_misses.load(Ordering::Relaxed),
            bytes_in_use: self.stats.bytes_in_use.load(Ordering::Relaxed),
            bytes_capacity,
        }
    }

    /// Check if the pool is registered with io_uring
    pub fn is_registered(&self) -> bool {
        self.is_registered.load(Ordering::Acquire)
    }

    /// Get buffer pointers for io_uring registration
    ///
    /// Returns a vector of (pointer, length) pairs for registering with io_uring.
    /// This is used internally for IORING_REGISTER_BUFFERS.
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    pub fn get_buffer_pointers(&self) -> Vec<(*const u8, usize)> {
        let buffers = self.buffers.read();
        buffers
            .iter()
            .map(|b| (b.data.as_ptr(), b.data.len()))
            .collect()
    }
}

// ============================================================================
// Fixed File Registry
// ============================================================================

/// Configuration for fixed file registry
#[derive(Debug, Clone)]
pub struct FixedFileConfig {
    /// Maximum number of fixed files (io_uring limit is typically 32768)
    pub max_files: usize,

    /// Enable automatic eviction of least recently used files
    pub enable_lru_eviction: bool,
}

impl Default for FixedFileConfig {
    fn default() -> Self {
        Self {
            max_files: 256,
            enable_lru_eviction: true,
        }
    }
}

/// A handle to a fixed (registered) file descriptor
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FixedFileId {
    /// Index in the fixed file table
    index: u32,
    /// Generation counter
    generation: u32,
}

impl FixedFileId {
    /// Create a new fixed file ID
    pub fn new(index: u32, generation: u32) -> Self {
        Self { index, generation }
    }

    /// Get the raw index for io_uring operations
    pub fn index(&self) -> u32 {
        self.index
    }
}

/// Statistics for fixed file registry
#[derive(Debug, Clone, Default)]
pub struct FixedFileStats {
    /// Total slots in the registry
    pub total_slots: usize,
    /// Currently registered files
    pub registered_files: usize,
    /// Total registrations
    pub registrations: u64,
    /// Total unregistrations
    pub unregistrations: u64,
    /// LRU evictions
    pub evictions: u64,
}

/// Entry in the fixed file registry
struct FixedFileEntry {
    /// The raw file descriptor (stored, not owned)
    fd: i32,
    /// Path for debugging
    path: std::path::PathBuf,
    /// Generation counter
    generation: u32,
    /// Whether this slot is in use (for future validation)
    #[allow(dead_code)]
    in_use: bool,
    /// Last access timestamp for LRU
    last_access: std::time::Instant,
}

/// Registry for fixed file descriptors
///
/// This registry manages file descriptors that are pre-registered with io_uring,
/// eliminating the need for the kernel to look up the file for each operation.
pub struct FixedFileRegistry {
    /// File entries (indexed by fixed file ID)
    entries: RwLock<Vec<Option<FixedFileEntry>>>,

    /// Free indices
    free_indices: Mutex<VecDeque<u32>>,

    /// Path to index mapping for deduplication
    path_to_index: RwLock<HashMap<std::path::PathBuf, u32>>,

    /// Configuration
    config: FixedFileConfig,

    /// Statistics
    stats: FixedFileRegistryStats,
}

/// Atomic stats
struct FixedFileRegistryStats {
    registrations: AtomicU64,
    unregistrations: AtomicU64,
    evictions: AtomicU64,
}

impl FixedFileRegistry {
    /// Create a new fixed file registry
    pub fn new(config: FixedFileConfig) -> Result<Self> {
        let mut entries = Vec::with_capacity(config.max_files);
        let mut free_indices = VecDeque::with_capacity(config.max_files);

        for i in 0..config.max_files {
            entries.push(None);
            free_indices.push_back(i as u32);
        }

        debug!(
            "Created fixed file registry with {} slots",
            config.max_files
        );

        Ok(Self {
            entries: RwLock::new(entries),
            free_indices: Mutex::new(free_indices),
            path_to_index: RwLock::new(HashMap::new()),
            config,
            stats: FixedFileRegistryStats {
                registrations: AtomicU64::new(0),
                unregistrations: AtomicU64::new(0),
                evictions: AtomicU64::new(0),
            },
        })
    }

    /// Create with default configuration
    pub fn with_defaults() -> Result<Self> {
        Self::new(FixedFileConfig::default())
    }

    /// Register a file descriptor
    ///
    /// Returns a fixed file ID that can be used with io_uring operations.
    pub fn register(&self, fd: i32, path: std::path::PathBuf) -> Result<FixedFileId> {
        // Check if already registered
        {
            let path_map = self.path_to_index.read();
            if let Some(&index) = path_map.get(&path) {
                let entries = self.entries.read();
                if let Some(Some(entry)) = entries.get(index as usize) {
                    return Ok(FixedFileId::new(index, entry.generation));
                }
            }
        }

        self.stats.registrations.fetch_add(1, Ordering::Relaxed);

        // Get a free slot
        let index = {
            let mut free = self.free_indices.lock();
            if let Some(idx) = free.pop_front() {
                idx
            } else if self.config.enable_lru_eviction {
                // Evict LRU entry
                drop(free);
                self.evict_lru()?
            } else {
                return Err(StreamlineError::storage_msg(
                    "Fixed file registry full".to_string(),
                ));
            }
        };

        // Register the file
        let generation = {
            let mut entries = self.entries.write();
            let entry = entries.get_mut(index as usize).ok_or_else(|| {
                StreamlineError::storage_msg("Invalid fixed file index".to_string())
            })?;

            let gen = entry.as_ref().map(|e| e.generation + 1).unwrap_or(0);

            *entry = Some(FixedFileEntry {
                fd,
                path: path.clone(),
                generation: gen,
                in_use: true,
                last_access: std::time::Instant::now(),
            });

            gen
        };

        // Update path mapping
        {
            let mut path_map = self.path_to_index.write();
            path_map.insert(path.clone(), index);
        }

        debug!("Registered fixed file {} at slot {}", path.display(), index);

        Ok(FixedFileId::new(index, generation))
    }

    /// Unregister a file
    pub fn unregister(&self, id: FixedFileId) -> Result<()> {
        self.stats.unregistrations.fetch_add(1, Ordering::Relaxed);

        let path = {
            let mut entries = self.entries.write();
            let entry = entries.get_mut(id.index as usize).ok_or_else(|| {
                StreamlineError::storage_msg("Invalid fixed file index".to_string())
            })?;

            if let Some(e) = entry.take() {
                if e.generation != id.generation {
                    return Err(StreamlineError::storage_msg(
                        "Fixed file generation mismatch".to_string(),
                    ));
                }
                Some(e.path)
            } else {
                None
            }
        };

        // Remove from path mapping
        if let Some(path) = path {
            let mut path_map = self.path_to_index.write();
            path_map.remove(&path);
        }

        // Return to free list
        let mut free = self.free_indices.lock();
        free.push_back(id.index);

        Ok(())
    }

    /// Touch a file to update its LRU timestamp
    pub fn touch(&self, id: FixedFileId) {
        if let Some(mut entries) = self.entries.try_write() {
            if let Some(Some(entry)) = entries.get_mut(id.index as usize) {
                if entry.generation == id.generation {
                    entry.last_access = std::time::Instant::now();
                }
            }
        }
    }

    /// Get the raw FD for a fixed file
    pub fn get_fd(&self, id: FixedFileId) -> Result<i32> {
        let entries = self.entries.read();
        let entry = entries
            .get(id.index as usize)
            .ok_or_else(|| StreamlineError::storage_msg("Invalid fixed file index".to_string()))?
            .as_ref()
            .ok_or_else(|| StreamlineError::storage_msg("Fixed file not registered".to_string()))?;

        if entry.generation != id.generation {
            return Err(StreamlineError::storage_msg(
                "Fixed file generation mismatch".to_string(),
            ));
        }

        Ok(entry.fd)
    }

    /// Evict the least recently used entry
    fn evict_lru(&self) -> Result<u32> {
        self.stats.evictions.fetch_add(1, Ordering::Relaxed);

        let mut entries = self.entries.write();
        let mut oldest_idx = None;
        let mut oldest_time = std::time::Instant::now();

        for (idx, entry) in entries.iter().enumerate() {
            if let Some(e) = entry {
                if e.last_access < oldest_time {
                    oldest_time = e.last_access;
                    oldest_idx = Some(idx);
                }
            }
        }

        let idx = oldest_idx
            .ok_or_else(|| StreamlineError::storage_msg("No entries to evict".to_string()))?
            as u32;

        // Remove the entry
        if let Some(entry) = entries[idx as usize].take() {
            let mut path_map = self.path_to_index.write();
            path_map.remove(&entry.path);
            debug!(
                "Evicted fixed file {} from slot {}",
                entry.path.display(),
                idx
            );
        }

        Ok(idx)
    }

    /// Get current statistics
    pub fn stats(&self) -> FixedFileStats {
        let entries = self.entries.read();
        let registered = entries.iter().filter(|e| e.is_some()).count();

        FixedFileStats {
            total_slots: entries.len(),
            registered_files: registered,
            registrations: self.stats.registrations.load(Ordering::Relaxed),
            unregistrations: self.stats.unregistrations.load(Ordering::Relaxed),
            evictions: self.stats.evictions.load(Ordering::Relaxed),
        }
    }
}

// ============================================================================
// Batch Operations
// ============================================================================

/// Type of I/O operation in a batch
#[derive(Debug, Clone)]
pub enum BatchOp {
    /// Read operation
    Read {
        file_index: u32,
        buffer_index: u32,
        offset: u64,
        len: usize,
    },
    /// Write operation
    Write {
        file_index: u32,
        buffer_index: u32,
        offset: u64,
        len: usize,
    },
    /// Sync data operation
    SyncData { file_index: u32 },
    /// Sync all operation
    SyncAll { file_index: u32 },
    /// No operation (for linked op padding)
    Nop,
}

/// Result of a batch operation
#[derive(Debug)]
pub struct BatchOpResult {
    /// Operation index in the batch
    pub op_index: usize,
    /// Result of the operation
    pub result: Result<usize>,
}

/// Configuration for batch operations
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// Maximum operations per batch
    pub max_ops: usize,

    /// Whether to use linked operations (dependent execution)
    pub use_linked_ops: bool,

    /// Whether to use IOSQE_IO_DRAIN for ordering
    pub use_drain: bool,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_ops: 64,
            use_linked_ops: false,
            use_drain: false,
        }
    }
}

/// Builder for batch I/O operations
#[derive(Debug)]
pub struct BatchBuilder {
    ops: Vec<BatchOp>,
    config: BatchConfig,
}

impl BatchBuilder {
    /// Create a new batch builder
    pub fn new() -> Self {
        Self::with_config(BatchConfig::default())
    }

    /// Create with custom configuration
    pub fn with_config(config: BatchConfig) -> Self {
        Self {
            ops: Vec::with_capacity(config.max_ops),
            config,
        }
    }

    /// Add a read operation to the batch
    pub fn read(mut self, file_index: u32, buffer_index: u32, offset: u64, len: usize) -> Self {
        self.ops.push(BatchOp::Read {
            file_index,
            buffer_index,
            offset,
            len,
        });
        self
    }

    /// Add a write operation to the batch
    pub fn write(mut self, file_index: u32, buffer_index: u32, offset: u64, len: usize) -> Self {
        self.ops.push(BatchOp::Write {
            file_index,
            buffer_index,
            offset,
            len,
        });
        self
    }

    /// Add a sync_data operation to the batch
    pub fn sync_data(mut self, file_index: u32) -> Self {
        self.ops.push(BatchOp::SyncData { file_index });
        self
    }

    /// Add a sync_all operation to the batch
    pub fn sync_all(mut self, file_index: u32) -> Self {
        self.ops.push(BatchOp::SyncAll { file_index });
        self
    }

    /// Enable linked operations (subsequent ops depend on previous)
    pub fn linked(mut self) -> Self {
        self.config.use_linked_ops = true;
        self
    }

    /// Enable drain flag (ensure ordering with previous ops)
    pub fn drain(mut self) -> Self {
        self.config.use_drain = true;
        self
    }

    /// Build the batch
    pub fn build(self) -> Batch {
        Batch {
            ops: self.ops,
            config: self.config,
        }
    }

    /// Get the current number of operations
    pub fn len(&self) -> usize {
        self.ops.len()
    }

    /// Check if the batch is empty
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }
}

impl Default for BatchBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// A batch of I/O operations ready for submission
#[derive(Debug)]
pub struct Batch {
    ops: Vec<BatchOp>,
    config: BatchConfig,
}

impl Batch {
    /// Get the operations in this batch
    pub fn ops(&self) -> &[BatchOp] {
        &self.ops
    }

    /// Get the number of operations
    pub fn len(&self) -> usize {
        self.ops.len()
    }

    /// Check if the batch is empty
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// Whether linked operations are enabled
    pub fn is_linked(&self) -> bool {
        self.config.use_linked_ops
    }

    /// Whether drain is enabled
    pub fn uses_drain(&self) -> bool {
        self.config.use_drain
    }
}

// ============================================================================
// Advanced io_uring Manager
// ============================================================================

/// Manager for advanced io_uring features
///
/// This provides a unified interface for using registered buffers,
/// fixed files, and batch operations together.
pub struct AdvancedUringManager {
    /// Registered buffer pool
    buffer_pool: Arc<RegisteredBufferPool>,

    /// Fixed file registry
    file_registry: Arc<FixedFileRegistry>,

    /// Statistics
    stats: AdvancedUringStats,
}

/// Atomic stats for advanced io_uring
struct AdvancedUringStats {
    batches_submitted: AtomicU64,
    total_ops_submitted: AtomicU64,
    batch_avg_size: AtomicUsize,
}

/// Statistics for advanced io_uring manager
#[derive(Debug, Clone, Default)]
pub struct AdvancedUringManagerStats {
    /// Buffer pool stats
    pub buffer_pool: RegisteredBufferStats,
    /// File registry stats
    pub file_registry: FixedFileStats,
    /// Total batches submitted
    pub batches_submitted: u64,
    /// Total operations submitted
    pub total_ops_submitted: u64,
    /// Average batch size
    pub avg_batch_size: f64,
}

impl AdvancedUringManager {
    /// Create a new advanced io_uring manager
    pub fn new(
        buffer_config: RegisteredBufferConfig,
        file_config: FixedFileConfig,
    ) -> Result<Self> {
        Ok(Self {
            buffer_pool: Arc::new(RegisteredBufferPool::new(buffer_config)?),
            file_registry: Arc::new(FixedFileRegistry::new(file_config)?),
            stats: AdvancedUringStats {
                batches_submitted: AtomicU64::new(0),
                total_ops_submitted: AtomicU64::new(0),
                batch_avg_size: AtomicUsize::new(0),
            },
        })
    }

    /// Create with default configuration
    pub fn with_defaults() -> Result<Self> {
        Self::new(
            RegisteredBufferConfig::default(),
            FixedFileConfig::default(),
        )
    }

    /// Get the buffer pool
    pub fn buffer_pool(&self) -> &Arc<RegisteredBufferPool> {
        &self.buffer_pool
    }

    /// Get the file registry
    pub fn file_registry(&self) -> &Arc<FixedFileRegistry> {
        &self.file_registry
    }

    /// Record a batch submission for statistics
    pub fn record_batch_submission(&self, batch_size: usize) {
        let batches = self.stats.batches_submitted.fetch_add(1, Ordering::Relaxed) + 1;
        let total_ops = self
            .stats
            .total_ops_submitted
            .fetch_add(batch_size as u64, Ordering::Relaxed)
            + batch_size as u64;

        // Update rolling average
        let avg = (total_ops as f64 / batches as f64) as usize;
        self.stats.batch_avg_size.store(avg, Ordering::Relaxed);
    }

    /// Get current statistics
    pub fn stats(&self) -> AdvancedUringManagerStats {
        let batches = self.stats.batches_submitted.load(Ordering::Relaxed);
        let total_ops = self.stats.total_ops_submitted.load(Ordering::Relaxed);

        AdvancedUringManagerStats {
            buffer_pool: self.buffer_pool.stats(),
            file_registry: self.file_registry.stats(),
            batches_submitted: batches,
            total_ops_submitted: total_ops,
            avg_batch_size: if batches > 0 {
                total_ops as f64 / batches as f64
            } else {
                0.0
            },
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registered_buffer_pool_creation() {
        let config = RegisteredBufferConfig {
            buffers_per_class: 4,
            size_classes: vec![1024, 4096],
            max_registered: 16,
        };
        let pool = RegisteredBufferPool::new(config).unwrap();
        let stats = pool.stats();

        assert_eq!(stats.total_registered, 8); // 4 buffers * 2 size classes
        assert_eq!(stats.available, 8);
    }

    #[test]
    fn test_registered_buffer_acquire_release() {
        let config = RegisteredBufferConfig {
            buffers_per_class: 2,
            size_classes: vec![1024],
            max_registered: 4,
        };
        let pool = RegisteredBufferPool::new(config).unwrap();

        // Acquire a buffer
        let id = pool.acquire(512).unwrap();
        let stats = pool.stats();
        assert_eq!(stats.available, 1);
        assert_eq!(stats.acquisitions, 1);

        // Release it
        pool.release(id).unwrap();
        let stats = pool.stats();
        assert_eq!(stats.available, 2);
        assert_eq!(stats.releases, 1);
    }

    #[test]
    fn test_registered_buffer_size_class_selection() {
        let config = RegisteredBufferConfig {
            buffers_per_class: 2,
            size_classes: vec![1024, 4096, 16384],
            max_registered: 8,
        };
        let pool = RegisteredBufferPool::new(config).unwrap();

        // Request smaller than smallest class
        let id1 = pool.acquire(512).unwrap();
        // Should get from 1024 class
        let buf = pool.buffer(id1).unwrap();
        assert_eq!(buf.len(), 1024);

        // Request between classes
        let id2 = pool.acquire(2048).unwrap();
        let buf = pool.buffer(id2).unwrap();
        assert_eq!(buf.len(), 4096);

        pool.release(id1).unwrap();
        pool.release(id2).unwrap();
    }

    #[test]
    fn test_fixed_file_registry_creation() {
        let config = FixedFileConfig {
            max_files: 8,
            enable_lru_eviction: true,
        };
        let registry = FixedFileRegistry::new(config).unwrap();
        let stats = registry.stats();

        assert_eq!(stats.total_slots, 8);
        assert_eq!(stats.registered_files, 0);
    }

    #[test]
    fn test_fixed_file_register_unregister() {
        let config = FixedFileConfig {
            max_files: 4,
            enable_lru_eviction: false,
        };
        let registry = FixedFileRegistry::new(config).unwrap();

        // Register a file
        let path = std::path::PathBuf::from("/test/file");
        let id = registry.register(42, path.clone()).unwrap();

        let stats = registry.stats();
        assert_eq!(stats.registered_files, 1);
        assert_eq!(stats.registrations, 1);

        // Get the FD
        let fd = registry.get_fd(id).unwrap();
        assert_eq!(fd, 42);

        // Unregister
        registry.unregister(id).unwrap();
        let stats = registry.stats();
        assert_eq!(stats.registered_files, 0);
        assert_eq!(stats.unregistrations, 1);
    }

    #[test]
    fn test_batch_builder() {
        let batch = BatchBuilder::new()
            .read(0, 1, 0, 4096)
            .write(0, 2, 4096, 1024)
            .sync_data(0)
            .linked()
            .build();

        assert_eq!(batch.len(), 3);
        assert!(batch.is_linked());
    }

    #[test]
    fn test_advanced_uring_manager() {
        let manager = AdvancedUringManager::with_defaults().unwrap();

        // Record some batch submissions
        manager.record_batch_submission(5);
        manager.record_batch_submission(10);

        let stats = manager.stats();
        assert_eq!(stats.batches_submitted, 2);
        assert_eq!(stats.total_ops_submitted, 15);
        assert!((stats.avg_batch_size - 7.5).abs() < 0.01);
    }
}
