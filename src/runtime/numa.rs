//! NUMA-aware memory allocation
//!
//! This module provides utilities for allocating memory on specific NUMA nodes,
//! which is critical for achieving optimal performance in multi-socket systems.
//! When memory is allocated on the same NUMA node as the CPU accessing it,
//! memory access latency can be 2-3x faster.
//!
//! ## Usage
//!
//! ```rust,ignore
//! use streamline::runtime::numa::{NumaAllocator, NumaConfig};
//!
//! // Create allocator with detection
//! let allocator = NumaAllocator::new()?;
//!
//! // Allocate on specific node
//! let buffer = allocator.alloc_on_node(4096, 0)?;
//!
//! // Allocate on local node (auto-detected)
//! let local_buffer = allocator.alloc_local(4096)?;
//! ```
//!
//! ## Platform Support
//!
//! - **Linux**: Full support via libnuma (if available) or mbind syscall
//! - **macOS**: Falls back to standard allocation (single NUMA domain)
//! - **Other platforms**: Falls back to standard allocation

use super::affinity::CpuTopology;
use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::io;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::debug;

/// Default page size (4KB)
pub const PAGE_SIZE: usize = 4096;

/// Large page size (2MB) for huge pages
#[allow(dead_code)]
pub const HUGE_PAGE_SIZE: usize = 2 * 1024 * 1024;

/// NUMA allocation configuration
#[derive(Debug, Clone)]
pub struct NumaConfig {
    /// Whether NUMA-aware allocation is enabled
    pub enabled: bool,
    /// Prefer huge pages when available
    pub prefer_huge_pages: bool,
    /// Pre-fault pages on allocation (to ensure they're actually allocated)
    pub pre_fault: bool,
}

impl Default for NumaConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            prefer_huge_pages: false,
            pre_fault: true,
        }
    }
}

/// Statistics for NUMA allocations
#[derive(Debug, Clone, Default)]
pub struct NumaStats {
    /// Total bytes allocated
    pub bytes_allocated: u64,
    /// Total allocations
    pub allocations: u64,
    /// Allocations per NUMA node
    pub allocations_per_node: Vec<u64>,
    /// Bytes per NUMA node
    pub bytes_per_node: Vec<u64>,
}

/// NUMA-aware memory allocator
pub struct NumaAllocator {
    /// Configuration
    config: NumaConfig,
    /// Detected CPU topology
    topology: CpuTopology,
    /// Whether NUMA is actually available
    numa_available: bool,
    /// Statistics
    total_bytes: AtomicU64,
    total_allocations: AtomicU64,
    bytes_per_node: Vec<AtomicU64>,
    allocations_per_node: Vec<AtomicU64>,
}

impl NumaAllocator {
    /// Create a new NUMA allocator with default configuration
    pub fn new() -> io::Result<Self> {
        Self::with_config(NumaConfig::default())
    }

    /// Create a new NUMA allocator with custom configuration
    pub fn with_config(config: NumaConfig) -> io::Result<Self> {
        let topology = CpuTopology::detect();
        let numa_available = config.enabled && is_numa_available();

        let numa_nodes = topology.numa_nodes.max(1);
        let bytes_per_node = (0..numa_nodes).map(|_| AtomicU64::new(0)).collect();
        let allocations_per_node = (0..numa_nodes).map(|_| AtomicU64::new(0)).collect();

        if numa_available {
            debug!(numa_nodes = numa_nodes, "NUMA-aware allocation enabled");
        } else if config.enabled {
            debug!("NUMA not available, using standard allocation");
        }

        Ok(Self {
            config,
            topology,
            numa_available,
            total_bytes: AtomicU64::new(0),
            total_allocations: AtomicU64::new(0),
            bytes_per_node,
            allocations_per_node,
        })
    }

    /// Check if NUMA is available and enabled
    pub fn is_numa_enabled(&self) -> bool {
        self.numa_available
    }

    /// Get the number of NUMA nodes
    pub fn numa_node_count(&self) -> usize {
        self.topology.numa_nodes
    }

    /// Get the NUMA node for a specific CPU
    pub fn node_for_cpu(&self, cpu: usize) -> usize {
        self.topology.node_for_cpu(cpu)
    }

    /// Get the NUMA node for the current thread
    #[cfg(target_os = "linux")]
    pub fn current_node(&self) -> usize {
        // SAFETY: sched_getcpu is always safe to call — it has no preconditions
        // and returns the CPU number the calling thread is running on, or -1 on
        // error. We handle the error case by falling back to node 0.
        unsafe {
            let cpu = libc::sched_getcpu();
            if cpu >= 0 {
                self.node_for_cpu(cpu as usize)
            } else {
                0
            }
        }
    }

    #[cfg(not(target_os = "linux"))]
    pub fn current_node(&self) -> usize {
        0
    }

    /// Allocate memory on a specific NUMA node
    ///
    /// Returns a pointer to the allocated memory, which must be deallocated
    /// using `dealloc_on_node`.
    pub fn alloc_on_node(&self, size: usize, node: usize) -> io::Result<NonNull<u8>> {
        if !self.numa_available || node >= self.topology.numa_nodes {
            return self.alloc_standard(size);
        }

        let ptr = alloc_on_node_impl(size, node, &self.config)?;

        // Update statistics
        self.total_bytes.fetch_add(size as u64, Ordering::Relaxed);
        self.total_allocations.fetch_add(1, Ordering::Relaxed);
        if let Some(counter) = self.bytes_per_node.get(node) {
            counter.fetch_add(size as u64, Ordering::Relaxed);
        }
        if let Some(counter) = self.allocations_per_node.get(node) {
            counter.fetch_add(1, Ordering::Relaxed);
        }

        Ok(ptr)
    }

    /// Allocate memory on the local (current thread's) NUMA node
    pub fn alloc_local(&self, size: usize) -> io::Result<NonNull<u8>> {
        let node = self.current_node();
        self.alloc_on_node(size, node)
    }

    /// Allocate standard (non-NUMA-aware) memory
    fn alloc_standard(&self, size: usize) -> io::Result<NonNull<u8>> {
        let layout = Layout::from_size_align(size, PAGE_SIZE)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;

        let ptr = unsafe { alloc_zeroed(layout) };

        NonNull::new(ptr)
            .ok_or_else(|| io::Error::new(io::ErrorKind::OutOfMemory, "Allocation failed"))
    }

    /// Deallocate memory allocated with this allocator
    ///
    /// # Safety
    /// The caller must ensure:
    /// - `ptr` was allocated by this allocator
    /// - `size` matches the original allocation size
    /// - The memory has not been deallocated already
    pub unsafe fn dealloc(&self, ptr: NonNull<u8>, size: usize) {
        dealloc_impl(ptr, size);
    }

    /// Get allocation statistics
    pub fn stats(&self) -> NumaStats {
        NumaStats {
            bytes_allocated: self.total_bytes.load(Ordering::Relaxed),
            allocations: self.total_allocations.load(Ordering::Relaxed),
            allocations_per_node: self
                .allocations_per_node
                .iter()
                .map(|c| c.load(Ordering::Relaxed))
                .collect(),
            bytes_per_node: self
                .bytes_per_node
                .iter()
                .map(|c| c.load(Ordering::Relaxed))
                .collect(),
        }
    }

    /// Get the CPU topology
    pub fn topology(&self) -> &CpuTopology {
        &self.topology
    }
}

impl Default for NumaAllocator {
    fn default() -> Self {
        Self::new().unwrap_or_else(|_| {
            // Fallback if detection fails
            Self {
                config: NumaConfig::default(),
                topology: CpuTopology::default(),
                numa_available: false,
                total_bytes: AtomicU64::new(0),
                total_allocations: AtomicU64::new(0),
                bytes_per_node: vec![AtomicU64::new(0)],
                allocations_per_node: vec![AtomicU64::new(0)],
            }
        })
    }
}

/// Check if NUMA is available on this system
#[cfg(target_os = "linux")]
pub fn is_numa_available() -> bool {
    use std::path::Path;
    // Check for NUMA sysfs interface
    let numa_path = Path::new("/sys/devices/system/node/node0");
    numa_path.exists()
}

#[cfg(not(target_os = "linux"))]
pub fn is_numa_available() -> bool {
    false
}

/// Get the NUMA node mask for memory binding
#[cfg(target_os = "linux")]
fn get_node_mask(node: usize) -> u64 {
    1u64 << node
}

/// Allocate memory on a specific NUMA node (Linux implementation)
#[cfg(target_os = "linux")]
fn alloc_on_node_impl(size: usize, node: usize, config: &NumaConfig) -> io::Result<NonNull<u8>> {
    use std::ptr;

    // Align size to page boundary
    let aligned_size = (size + PAGE_SIZE - 1) & !(PAGE_SIZE - 1);

    // Use mmap with NUMA policy
    let flags = libc::MAP_PRIVATE | libc::MAP_ANONYMOUS;

    // SAFETY: mmap with MAP_PRIVATE | MAP_ANONYMOUS is safe because: (1) we pass
    // null for the address hint (kernel chooses), (2) aligned_size is > 0 and
    // page-aligned, (3) fd is -1 and offset is 0 (required for MAP_ANONYMOUS).
    // We check for MAP_FAILED before using the returned pointer.
    let ptr = unsafe {
        libc::mmap(
            ptr::null_mut(),
            aligned_size,
            libc::PROT_READ | libc::PROT_WRITE,
            flags,
            -1,
            0,
        )
    };

    if ptr == libc::MAP_FAILED {
        return Err(io::Error::last_os_error());
    }

    // Bind to NUMA node using mbind
    let nodemask = get_node_mask(node);
    let maxnode = 64; // Max NUMA nodes

    // SAFETY: mbind syscall is safe because: (1) ptr is a valid pointer obtained
    // from a successful mmap call above, (2) aligned_size matches the mmap
    // allocation, (3) nodemask is a valid stack-allocated bitmask, and (4)
    // maxnode (64) covers the bitmask size. Failure is non-fatal (logged as warning).
    let result = unsafe {
        libc::syscall(
            libc::SYS_mbind,
            ptr,
            aligned_size,
            libc::MPOL_BIND,
            &nodemask as *const u64,
            maxnode,
            0, // flags
        )
    };

    if result < 0 {
        // mbind failed, but we still have valid memory
        // Log warning but don't fail
        warn!(
            node = node,
            error = %io::Error::last_os_error(),
            "mbind failed, memory may not be NUMA-local"
        );
    }

    // Pre-fault pages if requested
    if config.pre_fault {
        // SAFETY: Pre-faulting is safe because ptr points to a valid mmap region
        // of aligned_size bytes (checked for MAP_FAILED above). We write to the
        // first byte of each page within bounds. write_volatile ensures the
        // compiler does not optimize away the writes.
        unsafe {
            // Touch each page to ensure it's allocated on the correct node
            let page_count = aligned_size / PAGE_SIZE;
            for i in 0..page_count {
                let page_ptr = (ptr as *mut u8).add(i * PAGE_SIZE);
                ptr::write_volatile(page_ptr, 0);
            }
        }
    }

    NonNull::new(ptr as *mut u8)
        .ok_or_else(|| io::Error::new(io::ErrorKind::OutOfMemory, "mmap returned null"))
}

/// Allocate memory (non-Linux fallback)
#[cfg(not(target_os = "linux"))]
fn alloc_on_node_impl(size: usize, _node: usize, _config: &NumaConfig) -> io::Result<NonNull<u8>> {
    // Fallback to standard allocation on non-Linux
    let layout = Layout::from_size_align(size, PAGE_SIZE)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;

    // SAFETY: alloc_zeroed is safe because layout was successfully created above
    // with valid size and page alignment. We check the returned pointer for null.
    let ptr = unsafe { alloc_zeroed(layout) };

    NonNull::new(ptr).ok_or_else(|| io::Error::new(io::ErrorKind::OutOfMemory, "Allocation failed"))
}

/// Deallocate memory
#[cfg(target_os = "linux")]
unsafe fn dealloc_impl(ptr: NonNull<u8>, size: usize) {
    // If it was allocated via mmap, use munmap
    let aligned_size = (size + PAGE_SIZE - 1) & !(PAGE_SIZE - 1);
    // SAFETY: munmap is safe because ptr was obtained from mmap in alloc_on_node_impl
    // and aligned_size matches the original mmap allocation size. The pointer is
    // NonNull, guaranteeing it is not null. This is only called once per allocation.
    let result = libc::munmap(ptr.as_ptr() as *mut libc::c_void, aligned_size);
    if result != 0 {
        warn!(
            error = %io::Error::last_os_error(),
            "munmap failed"
        );
    }
}

#[cfg(not(target_os = "linux"))]
unsafe fn dealloc_impl(ptr: NonNull<u8>, size: usize) {
    // SAFETY: from_size_align_unchecked is safe because alloc_on_node_impl (non-Linux)
    // used the same size and PAGE_SIZE alignment to allocate. dealloc is safe because
    // ptr was allocated by alloc_zeroed with this same layout and has not been freed.
    let layout = Layout::from_size_align_unchecked(size, PAGE_SIZE);
    dealloc(ptr.as_ptr(), layout);
}

/// NUMA-local buffer for shard-local allocations
///
/// This is a convenience wrapper that allocates a buffer on a specific
/// NUMA node and ensures proper deallocation.
pub struct NumaBuffer {
    ptr: NonNull<u8>,
    size: usize,
    node: usize,
}

impl NumaBuffer {
    /// Create a new NUMA-local buffer
    pub fn new(allocator: &NumaAllocator, size: usize, node: usize) -> io::Result<Self> {
        let ptr = allocator.alloc_on_node(size, node)?;
        Ok(Self { ptr, size, node })
    }

    /// Create a buffer on the local NUMA node
    pub fn new_local(allocator: &NumaAllocator, size: usize) -> io::Result<Self> {
        let node = allocator.current_node();
        Self::new(allocator, size, node)
    }

    /// Get a pointer to the buffer
    pub fn as_ptr(&self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    /// Get a slice view of the buffer
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.size) }
    }

    /// Get a mutable slice view of the buffer
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.size) }
    }

    /// Get the size of the buffer
    pub fn size(&self) -> usize {
        self.size
    }

    /// Get the NUMA node
    pub fn node(&self) -> usize {
        self.node
    }
}

impl Drop for NumaBuffer {
    fn drop(&mut self) {
        unsafe {
            dealloc_impl(self.ptr, self.size);
        }
    }
}

// Safety: NumaBuffer contains raw memory that can be sent between threads
// The underlying memory is owned and will be properly deallocated
unsafe impl Send for NumaBuffer {}

// Safety: NumaBuffer does not provide shared mutable access without synchronization
// The as_mut_slice method requires &mut self
unsafe impl Sync for NumaBuffer {}

/// NUMA-aware vector-like container
///
/// A growable buffer that allocates on a specific NUMA node.
pub struct NumaVec<T> {
    buffer: NumaBuffer,
    len: usize,
    capacity: usize,
    _marker: std::marker::PhantomData<T>,
}

impl<T: Copy + Default> NumaVec<T> {
    /// Create a new NUMA-local vector with the given capacity
    pub fn with_capacity(
        allocator: &NumaAllocator,
        capacity: usize,
        node: usize,
    ) -> io::Result<Self> {
        let size = capacity * std::mem::size_of::<T>();
        let buffer = NumaBuffer::new(allocator, size, node)?;
        Ok(Self {
            buffer,
            len: 0,
            capacity,
            _marker: std::marker::PhantomData,
        })
    }

    /// Create a new NUMA-local vector on the current node
    pub fn with_capacity_local(allocator: &NumaAllocator, capacity: usize) -> io::Result<Self> {
        let node = allocator.current_node();
        Self::with_capacity(allocator, capacity, node)
    }

    /// Push an element
    pub fn push(&mut self, value: T) -> Result<(), T> {
        if self.len >= self.capacity {
            return Err(value);
        }

        unsafe {
            let ptr = (self.buffer.as_ptr() as *mut T).add(self.len);
            std::ptr::write(ptr, value);
        }
        self.len += 1;
        Ok(())
    }

    /// Pop an element
    pub fn pop(&mut self) -> Option<T> {
        if self.len == 0 {
            return None;
        }

        self.len -= 1;
        unsafe {
            let ptr = (self.buffer.as_ptr() as *mut T).add(self.len);
            Some(std::ptr::read(ptr))
        }
    }

    /// Get the length
    pub fn len(&self) -> usize {
        self.len
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Get the capacity
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Get a slice view
    pub fn as_slice(&self) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.buffer.as_ptr() as *const T, self.len) }
    }

    /// Get a mutable slice view
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        unsafe { std::slice::from_raw_parts_mut(self.buffer.as_ptr() as *mut T, self.len) }
    }

    /// Clear the vector
    pub fn clear(&mut self) {
        self.len = 0;
    }
}

// Safety: NumaVec<T> is Send if T is Send
unsafe impl<T: Send + Copy + Default> Send for NumaVec<T> {}
// Safety: NumaVec<T> is Sync if T is Sync
unsafe impl<T: Sync + Copy + Default> Sync for NumaVec<T> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_numa_allocator_creation() {
        let allocator = NumaAllocator::new().unwrap();
        assert!(allocator.numa_node_count() >= 1);
    }

    #[test]
    fn test_numa_allocator_stats() {
        let allocator = NumaAllocator::new().unwrap();
        let stats = allocator.stats();
        assert_eq!(stats.bytes_allocated, 0);
        assert_eq!(stats.allocations, 0);
    }

    #[test]
    fn test_alloc_local() {
        let allocator = NumaAllocator::new().unwrap();
        let ptr = allocator.alloc_local(4096).unwrap();
        // NonNull is guaranteed to be non-null by the type system

        // Deallocate
        unsafe {
            allocator.dealloc(ptr, 4096);
        }
    }

    #[test]
    fn test_numa_buffer() {
        let allocator = NumaAllocator::new().unwrap();
        let mut buffer = NumaBuffer::new_local(&allocator, 4096).unwrap();

        assert_eq!(buffer.size(), 4096);

        // Write some data
        let slice = buffer.as_mut_slice();
        slice[0] = 42;
        slice[4095] = 99;

        // Read it back
        let slice = buffer.as_slice();
        assert_eq!(slice[0], 42);
        assert_eq!(slice[4095], 99);
    }

    #[test]
    fn test_numa_vec() {
        let allocator = NumaAllocator::new().unwrap();
        let mut vec: NumaVec<u64> = NumaVec::with_capacity_local(&allocator, 100).unwrap();

        assert_eq!(vec.len(), 0);
        assert_eq!(vec.capacity(), 100);
        assert!(vec.is_empty());

        // Push some elements
        vec.push(1).unwrap();
        vec.push(2).unwrap();
        vec.push(3).unwrap();

        assert_eq!(vec.len(), 3);
        assert!(!vec.is_empty());

        // Check values
        assert_eq!(vec.as_slice(), &[1, 2, 3]);

        // Pop
        assert_eq!(vec.pop(), Some(3));
        assert_eq!(vec.len(), 2);

        // Clear
        vec.clear();
        assert!(vec.is_empty());
    }

    #[test]
    fn test_is_numa_available() {
        // Just check it doesn't panic
        let _available = is_numa_available();
    }

    #[test]
    fn test_disabled_numa() {
        let config = NumaConfig {
            enabled: false,
            ..Default::default()
        };
        let allocator = NumaAllocator::with_config(config).unwrap();
        assert!(!allocator.is_numa_enabled());

        // Allocation should still work (falls back to standard)
        let ptr = allocator.alloc_local(4096).unwrap();
        // NonNull is guaranteed to be non-null by the type system

        unsafe {
            allocator.dealloc(ptr, 4096);
        }
    }
}
