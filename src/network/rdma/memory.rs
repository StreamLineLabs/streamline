//! RDMA memory region management
//!
//! This module handles memory registration, pinning, and buffer management
//! for RDMA operations. Memory regions must be registered with the NIC
//! to enable DMA operations.

use super::{AccessFlags, RdmaError, RdmaResult};
use parking_lot::RwLock;
use std::alloc::{alloc, dealloc, Layout};
use std::collections::HashMap;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{debug, warn};

/// Memory region handle
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MemoryRegionHandle(pub u32);

/// Remote memory key for RDMA operations
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RemoteKey(pub u32);

/// Local memory key for RDMA operations
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LocalKey(pub u32);

/// A registered memory region for RDMA operations
pub struct MemoryRegion {
    /// Unique handle
    handle: MemoryRegionHandle,

    /// Base address
    addr: NonNull<u8>,

    /// Size in bytes
    size: usize,

    /// Local key (for local operations)
    lkey: LocalKey,

    /// Remote key (for remote operations)
    rkey: RemoteKey,

    /// Access flags
    access: AccessFlags,

    /// Whether this region owns the memory
    owned: bool,

    /// Layout for deallocation (if owned)
    layout: Option<Layout>,

    /// Reference count for zero-copy sharing
    ref_count: AtomicU32,

    /// Bytes written to this region
    bytes_written: AtomicU64,

    /// Bytes read from this region
    bytes_read: AtomicU64,
}

impl MemoryRegion {
    /// Get the handle
    pub fn handle(&self) -> MemoryRegionHandle {
        self.handle
    }

    /// Get the base address
    pub fn addr(&self) -> *mut u8 {
        self.addr.as_ptr()
    }

    /// Get the size
    pub fn size(&self) -> usize {
        self.size
    }

    /// Get the local key
    pub fn lkey(&self) -> LocalKey {
        self.lkey
    }

    /// Get the remote key
    pub fn rkey(&self) -> RemoteKey {
        self.rkey
    }

    /// Get the access flags
    pub fn access(&self) -> AccessFlags {
        self.access
    }

    /// Get a slice of the memory region
    ///
    /// # Safety
    /// Caller must ensure no concurrent writes are happening
    pub unsafe fn as_slice(&self) -> &[u8] {
        std::slice::from_raw_parts(self.addr.as_ptr(), self.size)
    }

    /// Get a mutable slice of the memory region
    ///
    /// # Safety
    /// Caller must ensure exclusive access
    pub unsafe fn as_mut_slice(&mut self) -> &mut [u8] {
        std::slice::from_raw_parts_mut(self.addr.as_ptr(), self.size)
    }

    /// Increment reference count
    pub fn add_ref(&self) {
        self.ref_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement reference count, returns true if this was the last reference
    pub fn release(&self) -> bool {
        self.ref_count.fetch_sub(1, Ordering::Release) == 1
    }

    /// Get reference count
    pub fn ref_count(&self) -> u32 {
        self.ref_count.load(Ordering::Relaxed)
    }

    /// Record bytes written
    pub fn record_write(&self, bytes: u64) {
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record bytes read
    pub fn record_read(&self, bytes: u64) {
        self.bytes_read.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Get statistics
    pub fn stats(&self) -> MemoryRegionStats {
        MemoryRegionStats {
            handle: self.handle,
            size: self.size,
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
            ref_count: self.ref_count.load(Ordering::Relaxed),
        }
    }
}

impl Drop for MemoryRegion {
    fn drop(&mut self) {
        if self.owned {
            if let Some(layout) = self.layout {
                // SAFETY: dealloc is safe because: (1) self.owned is true, meaning this
                // MemoryRegion owns the allocation, (2) self.addr was allocated with
                // std::alloc::alloc using the same layout stored in self.layout, and
                // (3) Drop is called only once, preventing double-free.
                unsafe {
                    dealloc(self.addr.as_ptr(), layout);
                }
            }
        }
    }
}

// Safety: MemoryRegion can be sent between threads
unsafe impl Send for MemoryRegion {}
unsafe impl Sync for MemoryRegion {}

/// Memory region statistics
#[derive(Debug, Clone, Default)]
pub struct MemoryRegionStats {
    /// Handle
    pub handle: MemoryRegionHandle,
    /// Size in bytes
    pub size: usize,
    /// Total bytes written
    pub bytes_written: u64,
    /// Total bytes read
    pub bytes_read: u64,
    /// Current reference count
    pub ref_count: u32,
}

impl Default for MemoryRegionHandle {
    fn default() -> Self {
        Self(0)
    }
}

/// Memory pool for efficient buffer allocation
pub struct MemoryPool {
    /// Pool name
    name: String,

    /// Buffer size for this pool
    buffer_size: usize,

    /// Free buffers
    free_list: RwLock<Vec<Arc<MemoryRegion>>>,

    /// All allocated buffers
    allocated: RwLock<HashMap<MemoryRegionHandle, Arc<MemoryRegion>>>,

    /// Maximum pool size
    max_size: usize,

    /// Current pool size
    current_size: AtomicU64,

    /// Next handle ID
    next_handle: AtomicU32,

    /// Allocations count
    allocations: AtomicU64,

    /// Deallocations count
    deallocations: AtomicU64,

    /// Pool hits (reused buffer)
    pool_hits: AtomicU64,

    /// Pool misses (new allocation)
    pool_misses: AtomicU64,
}

impl MemoryPool {
    /// Create a new memory pool
    pub fn new(name: &str, buffer_size: usize, max_size: usize) -> Self {
        Self {
            name: name.to_string(),
            buffer_size,
            free_list: RwLock::new(Vec::new()),
            allocated: RwLock::new(HashMap::new()),
            max_size,
            current_size: AtomicU64::new(0),
            next_handle: AtomicU32::new(1),
            allocations: AtomicU64::new(0),
            deallocations: AtomicU64::new(0),
            pool_hits: AtomicU64::new(0),
            pool_misses: AtomicU64::new(0),
        }
    }

    /// Allocate a buffer from the pool
    pub fn allocate(&self, access: AccessFlags) -> RdmaResult<Arc<MemoryRegion>> {
        // Try to get from free list first
        if let Some(region) = self.free_list.write().pop() {
            self.pool_hits.fetch_add(1, Ordering::Relaxed);
            region.add_ref();
            return Ok(region);
        }

        self.pool_misses.fetch_add(1, Ordering::Relaxed);

        // Check if we can allocate more
        let current = self.current_size.load(Ordering::Relaxed) as usize;
        if current >= self.max_size {
            return Err(RdmaError::MemoryError("Memory pool exhausted".to_string()));
        }

        // Allocate new buffer
        let region = self.allocate_new(access)?;
        self.current_size
            .fetch_add(self.buffer_size as u64, Ordering::Relaxed);
        self.allocations.fetch_add(1, Ordering::Relaxed);

        let region = Arc::new(region);
        self.allocated
            .write()
            .insert(region.handle(), region.clone());

        Ok(region)
    }

    /// Return a buffer to the pool
    pub fn release(&self, region: Arc<MemoryRegion>) {
        if region.release() {
            // Last reference, return to free list
            self.deallocations.fetch_add(1, Ordering::Relaxed);
            self.free_list.write().push(region);
        }
    }

    /// Allocate a new memory region
    fn allocate_new(&self, access: AccessFlags) -> RdmaResult<MemoryRegion> {
        let handle = MemoryRegionHandle(self.next_handle.fetch_add(1, Ordering::Relaxed));

        // Allocate aligned memory
        let layout = Layout::from_size_align(self.buffer_size, 4096)
            .map_err(|e| RdmaError::MemoryError(format!("Invalid layout: {}", e)))?;

        // SAFETY: alloc is safe because layout was successfully created above with
        // valid size (self.buffer_size) and 4096-byte alignment. The returned pointer
        // is checked for null before being wrapped in NonNull.
        let ptr = unsafe { alloc(layout) };
        if ptr.is_null() {
            return Err(RdmaError::MemoryError("Allocation failed".to_string()));
        }

        let addr = NonNull::new(ptr).ok_or_else(|| {
            RdmaError::MemoryError("Failed to create non-null pointer".to_string())
        })?;

        // Generate keys (in real implementation, these come from ibv_reg_mr)
        let lkey = LocalKey(handle.0);
        let rkey = RemoteKey(handle.0);

        debug!(
            pool = %self.name,
            handle = handle.0,
            size = self.buffer_size,
            "Allocated memory region"
        );

        Ok(MemoryRegion {
            handle,
            addr,
            size: self.buffer_size,
            lkey,
            rkey,
            access,
            owned: true,
            layout: Some(layout),
            ref_count: AtomicU32::new(1),
            bytes_written: AtomicU64::new(0),
            bytes_read: AtomicU64::new(0),
        })
    }

    /// Pre-allocate buffers
    pub fn preallocate(&self, count: usize, access: AccessFlags) -> RdmaResult<()> {
        let mut free_list = self.free_list.write();

        for _ in 0..count {
            let region = self.allocate_new(access)?;
            self.current_size
                .fetch_add(self.buffer_size as u64, Ordering::Relaxed);
            self.allocations.fetch_add(1, Ordering::Relaxed);

            let region = Arc::new(region);
            self.allocated
                .write()
                .insert(region.handle(), region.clone());
            free_list.push(region);
        }

        debug!(
            pool = %self.name,
            count = count,
            "Pre-allocated memory regions"
        );

        Ok(())
    }

    /// Get pool statistics
    pub fn stats(&self) -> MemoryPoolStats {
        MemoryPoolStats {
            name: self.name.clone(),
            buffer_size: self.buffer_size,
            max_size: self.max_size,
            current_size: self.current_size.load(Ordering::Relaxed) as usize,
            free_count: self.free_list.read().len(),
            allocated_count: self.allocated.read().len(),
            allocations: self.allocations.load(Ordering::Relaxed),
            deallocations: self.deallocations.load(Ordering::Relaxed),
            pool_hits: self.pool_hits.load(Ordering::Relaxed),
            pool_misses: self.pool_misses.load(Ordering::Relaxed),
        }
    }

    /// Clear the pool
    pub fn clear(&self) {
        self.free_list.write().clear();
        self.allocated.write().clear();
        self.current_size.store(0, Ordering::Relaxed);
    }
}

/// Memory pool statistics
#[derive(Debug, Clone, Default)]
pub struct MemoryPoolStats {
    /// Pool name
    pub name: String,
    /// Buffer size
    pub buffer_size: usize,
    /// Maximum pool size in bytes
    pub max_size: usize,
    /// Current pool size in bytes
    pub current_size: usize,
    /// Free buffers count
    pub free_count: usize,
    /// Allocated buffers count
    pub allocated_count: usize,
    /// Total allocations
    pub allocations: u64,
    /// Total deallocations
    pub deallocations: u64,
    /// Pool hits
    pub pool_hits: u64,
    /// Pool misses
    pub pool_misses: u64,
}

impl MemoryPoolStats {
    /// Calculate hit rate
    pub fn hit_rate(&self) -> f64 {
        let total = self.pool_hits + self.pool_misses;
        if total == 0 {
            0.0
        } else {
            self.pool_hits as f64 / total as f64
        }
    }
}

/// Memory manager for RDMA operations
pub struct MemoryManager {
    /// Small buffer pool (for control messages)
    small_pool: MemoryPool,

    /// Medium buffer pool (for typical messages)
    medium_pool: MemoryPool,

    /// Large buffer pool (for bulk transfers)
    large_pool: MemoryPool,

    /// Custom registered regions
    custom_regions: RwLock<HashMap<MemoryRegionHandle, Arc<MemoryRegion>>>,

    /// Next custom handle
    next_custom_handle: AtomicU32,

    /// Total memory registered
    total_registered: AtomicU64,

    /// Use huge pages for large allocations
    use_huge_pages: bool,
}

impl MemoryManager {
    /// Default small buffer size (4KB)
    pub const SMALL_SIZE: usize = 4 * 1024;
    /// Default medium buffer size (64KB)
    pub const MEDIUM_SIZE: usize = 64 * 1024;
    /// Default large buffer size (1MB)
    pub const LARGE_SIZE: usize = 1024 * 1024;

    /// Create a new memory manager
    pub fn new(use_huge_pages: bool) -> Self {
        Self {
            small_pool: MemoryPool::new("small", Self::SMALL_SIZE, 64 * 1024 * 1024), // 64MB max
            medium_pool: MemoryPool::new("medium", Self::MEDIUM_SIZE, 256 * 1024 * 1024), // 256MB max
            large_pool: MemoryPool::new("large", Self::LARGE_SIZE, 1024 * 1024 * 1024),   // 1GB max
            custom_regions: RwLock::new(HashMap::new()),
            next_custom_handle: AtomicU32::new(1_000_000), // High range for custom
            total_registered: AtomicU64::new(0),
            use_huge_pages,
        }
    }

    /// Allocate a buffer of appropriate size
    pub fn allocate(&self, size: usize, access: AccessFlags) -> RdmaResult<Arc<MemoryRegion>> {
        if size <= Self::SMALL_SIZE {
            self.small_pool.allocate(access)
        } else if size <= Self::MEDIUM_SIZE {
            self.medium_pool.allocate(access)
        } else if size <= Self::LARGE_SIZE {
            self.large_pool.allocate(access)
        } else {
            // Custom allocation for very large buffers
            self.allocate_custom(size, access)
        }
    }

    /// Allocate a custom-sized region
    fn allocate_custom(&self, size: usize, access: AccessFlags) -> RdmaResult<Arc<MemoryRegion>> {
        let handle = MemoryRegionHandle(self.next_custom_handle.fetch_add(1, Ordering::Relaxed));

        // Use huge pages if available and size is appropriate
        let (layout, use_huge) = if self.use_huge_pages && size >= 2 * 1024 * 1024 {
            // 2MB huge page alignment
            (
                Layout::from_size_align(size, 2 * 1024 * 1024)
                    .map_err(|e| RdmaError::MemoryError(format!("Invalid layout: {}", e)))?,
                true,
            )
        } else {
            (
                Layout::from_size_align(size, 4096)
                    .map_err(|e| RdmaError::MemoryError(format!("Invalid layout: {}", e)))?,
                false,
            )
        };

        // SAFETY: alloc is safe because layout was successfully created above with
        // valid size and alignment (either huge-page or 4096-byte). The returned
        // pointer is checked for null before being wrapped in NonNull.
        let ptr = unsafe { alloc(layout) };
        if ptr.is_null() {
            return Err(RdmaError::MemoryError(
                "Custom allocation failed".to_string(),
            ));
        }

        let addr = NonNull::new(ptr).ok_or_else(|| {
            RdmaError::MemoryError("Failed to create non-null pointer".to_string())
        })?;

        let lkey = LocalKey(handle.0);
        let rkey = RemoteKey(handle.0);

        let region = Arc::new(MemoryRegion {
            handle,
            addr,
            size,
            lkey,
            rkey,
            access,
            owned: true,
            layout: Some(layout),
            ref_count: AtomicU32::new(1),
            bytes_written: AtomicU64::new(0),
            bytes_read: AtomicU64::new(0),
        });

        self.custom_regions.write().insert(handle, region.clone());
        self.total_registered
            .fetch_add(size as u64, Ordering::Relaxed);

        debug!(
            handle = handle.0,
            size = size,
            huge_pages = use_huge,
            "Allocated custom memory region"
        );

        Ok(region)
    }

    /// Register existing memory (for zero-copy from user buffers)
    pub fn register(
        &self,
        ptr: *mut u8,
        size: usize,
        access: AccessFlags,
    ) -> RdmaResult<Arc<MemoryRegion>> {
        if ptr.is_null() {
            return Err(RdmaError::MemoryError(
                "Cannot register null pointer".to_string(),
            ));
        }

        let handle = MemoryRegionHandle(self.next_custom_handle.fetch_add(1, Ordering::Relaxed));

        let addr = NonNull::new(ptr).ok_or_else(|| {
            RdmaError::MemoryError("Failed to create non-null pointer".to_string())
        })?;

        let lkey = LocalKey(handle.0);
        let rkey = RemoteKey(handle.0);

        let region = Arc::new(MemoryRegion {
            handle,
            addr,
            size,
            lkey,
            rkey,
            access,
            owned: false, // Don't free on drop
            layout: None,
            ref_count: AtomicU32::new(1),
            bytes_written: AtomicU64::new(0),
            bytes_read: AtomicU64::new(0),
        });

        self.custom_regions.write().insert(handle, region.clone());
        self.total_registered
            .fetch_add(size as u64, Ordering::Relaxed);

        debug!(
            handle = handle.0,
            size = size,
            "Registered external memory region"
        );

        Ok(region)
    }

    /// Release a buffer back to the pool
    pub fn release(&self, region: Arc<MemoryRegion>) {
        let size = region.size();

        if size <= Self::SMALL_SIZE {
            self.small_pool.release(region);
        } else if size <= Self::MEDIUM_SIZE {
            self.medium_pool.release(region);
        } else if size <= Self::LARGE_SIZE {
            self.large_pool.release(region);
        } else {
            // Custom region - remove from map, will be freed on drop
            if region.release() {
                self.custom_regions.write().remove(&region.handle());
                self.total_registered
                    .fetch_sub(size as u64, Ordering::Relaxed);
            }
        }
    }

    /// Deregister a memory region
    pub fn deregister(&self, handle: MemoryRegionHandle) -> RdmaResult<()> {
        if let Some(region) = self.custom_regions.write().remove(&handle) {
            self.total_registered
                .fetch_sub(region.size() as u64, Ordering::Relaxed);
            debug!(handle = handle.0, "Deregistered memory region");
            Ok(())
        } else {
            warn!(handle = handle.0, "Attempted to deregister unknown region");
            Err(RdmaError::MemoryError(format!(
                "Unknown region handle: {}",
                handle.0
            )))
        }
    }

    /// Pre-allocate pools
    pub fn preallocate(&self) -> RdmaResult<()> {
        let access = AccessFlags::remote_full();

        // Pre-allocate some buffers in each pool
        self.small_pool.preallocate(64, access)?; // 256KB
        self.medium_pool.preallocate(16, access)?; // 1MB
        self.large_pool.preallocate(4, access)?; // 4MB

        Ok(())
    }

    /// Get statistics
    pub fn stats(&self) -> MemoryManagerStats {
        MemoryManagerStats {
            small_pool: self.small_pool.stats(),
            medium_pool: self.medium_pool.stats(),
            large_pool: self.large_pool.stats(),
            custom_count: self.custom_regions.read().len(),
            total_registered: self.total_registered.load(Ordering::Relaxed),
            use_huge_pages: self.use_huge_pages,
        }
    }
}

impl Default for MemoryManager {
    fn default() -> Self {
        Self::new(false)
    }
}

/// Memory manager statistics
#[derive(Debug, Clone, Default)]
pub struct MemoryManagerStats {
    /// Small pool stats
    pub small_pool: MemoryPoolStats,
    /// Medium pool stats
    pub medium_pool: MemoryPoolStats,
    /// Large pool stats
    pub large_pool: MemoryPoolStats,
    /// Custom regions count
    pub custom_count: usize,
    /// Total memory registered
    pub total_registered: u64,
    /// Using huge pages
    pub use_huge_pages: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_pool() {
        let pool = MemoryPool::new("test", 4096, 1024 * 1024);
        let access = AccessFlags::local_only();

        let region = pool.allocate(access).unwrap();
        assert_eq!(region.size(), 4096);

        let stats = pool.stats();
        assert_eq!(stats.allocations, 1);
        assert_eq!(stats.pool_misses, 1);

        pool.release(region);
    }

    #[test]
    fn test_memory_pool_reuse() {
        let pool = MemoryPool::new("test", 4096, 1024 * 1024);
        let access = AccessFlags::local_only();

        // Allocate and release
        let region = pool.allocate(access).unwrap();
        let handle = region.handle();
        pool.release(region);

        // Allocate again - should reuse
        let region2 = pool.allocate(access).unwrap();
        assert_eq!(region2.handle(), handle);

        let stats = pool.stats();
        assert_eq!(stats.pool_hits, 1);
        assert_eq!(stats.pool_misses, 1);
    }

    #[test]
    fn test_memory_manager() {
        let manager = MemoryManager::new(false);
        let access = AccessFlags::local_only();

        // Small allocation
        let small = manager.allocate(1024, access).unwrap();
        assert!(small.size() >= 1024);

        // Medium allocation
        let medium = manager.allocate(32 * 1024, access).unwrap();
        assert!(medium.size() >= 32 * 1024);

        // Large allocation
        let large = manager.allocate(512 * 1024, access).unwrap();
        assert!(large.size() >= 512 * 1024);

        // Release
        manager.release(small);
        manager.release(medium);
        manager.release(large);
    }

    #[test]
    fn test_memory_region_stats() {
        let pool = MemoryPool::new("test", 4096, 1024 * 1024);
        let access = AccessFlags::local_only();

        let region = pool.allocate(access).unwrap();
        region.record_write(100);
        region.record_read(50);

        let stats = region.stats();
        assert_eq!(stats.bytes_written, 100);
        assert_eq!(stats.bytes_read, 50);
    }
}
