//! Buffer pool for reducing allocation overhead in I/O operations
//!
//! The buffer pool maintains pre-allocated buffers organized by size classes
//! to reduce allocation overhead during I/O operations. This is particularly
//! important for io_uring where buffers are frequently allocated and deallocated.

use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// Pre-allocated buffer pool to reduce allocation overhead
pub struct IoBufferPool {
    /// Available buffers by size class
    pools: DashMap<usize, Vec<Vec<u8>>>,
    /// Size classes (e.g., 4KB, 64KB, 1MB)
    size_classes: Vec<usize>,
    /// Maximum buffers per size class
    max_per_class: usize,
    /// Statistics
    allocations: AtomicU64,
    reuses: AtomicU64,
}

impl IoBufferPool {
    /// Create a new buffer pool
    ///
    /// # Arguments
    ///
    /// * `size_classes` - Buffer size classes (e.g., [4096, 65536, 1048576])
    /// * `max_per_class` - Maximum buffers to keep per size class
    pub fn new(size_classes: Vec<usize>, max_per_class: usize) -> Self {
        let pools = DashMap::new();

        // Pre-populate pools
        for &size in &size_classes {
            pools.insert(size, Vec::new());
        }

        Self {
            pools,
            size_classes,
            max_per_class,
            allocations: AtomicU64::new(0),
            reuses: AtomicU64::new(0),
        }
    }

    /// Create a default buffer pool with common size classes
    pub fn default_pool() -> Self {
        Self::new(
            vec![
                4 * 1024,    // 4 KB
                16 * 1024,   // 16 KB
                64 * 1024,   // 64 KB
                256 * 1024,  // 256 KB
                1024 * 1024, // 1 MB
            ],
            64, // Keep 64 buffers per class
        )
    }

    /// Get a buffer of at least the specified size
    ///
    /// Returns a buffer from the pool if available, otherwise allocates a new one.
    pub fn acquire(&self, min_size: usize) -> Vec<u8> {
        let size_class = self.round_up_to_class(min_size);

        if let Some(mut pool) = self.pools.get_mut(&size_class) {
            if let Some(buf) = pool.pop() {
                self.reuses.fetch_add(1, Ordering::Relaxed);
                return buf;
            }
        }

        self.allocations.fetch_add(1, Ordering::Relaxed);
        vec![0u8; size_class]
    }

    /// Return a buffer to the pool
    ///
    /// The buffer is cleared and added back to the pool if there's space,
    /// otherwise it's dropped.
    pub fn release(&self, mut buf: Vec<u8>) {
        let capacity = buf.capacity();
        let size_class = self.round_down_to_class(capacity);

        if let Some(mut pool) = self.pools.get_mut(&size_class) {
            if pool.len() < self.max_per_class {
                buf.clear();
                pool.push(buf);
            }
        }
    }

    /// Round up size to the nearest size class
    fn round_up_to_class(&self, size: usize) -> usize {
        for &class in &self.size_classes {
            if size <= class {
                return class;
            }
        }
        // If size is larger than all classes, use the exact size
        size
    }

    /// Round down capacity to the nearest size class
    fn round_down_to_class(&self, capacity: usize) -> usize {
        for &class in self.size_classes.iter().rev() {
            if capacity >= class {
                return class;
            }
        }
        // If capacity is smaller than all classes, use smallest class
        self.size_classes.first().copied().unwrap_or(4096)
    }

    /// Get pool statistics
    pub fn stats(&self) -> BufferPoolStats {
        let mut total_buffers = 0;
        let mut buffers_by_class = Vec::new();

        for &size in &self.size_classes {
            if let Some(pool) = self.pools.get(&size) {
                let count = pool.len();
                total_buffers += count;
                buffers_by_class.push((size, count));
            }
        }

        BufferPoolStats {
            total_buffers,
            buffers_by_class,
            allocations: self.allocations.load(Ordering::Relaxed),
            reuses: self.reuses.load(Ordering::Relaxed),
        }
    }

    /// Clear all buffers from the pool
    pub fn clear(&self) {
        for mut entry in self.pools.iter_mut() {
            entry.value_mut().clear();
        }
    }
}

/// Buffer pool statistics
#[derive(Debug, Clone)]
pub struct BufferPoolStats {
    /// Total number of buffers in pool
    pub total_buffers: usize,
    /// Number of buffers per size class
    pub buffers_by_class: Vec<(usize, usize)>,
    /// Total number of allocations (cache misses)
    pub allocations: u64,
    /// Total number of reuses (cache hits)
    pub reuses: u64,
}

impl BufferPoolStats {
    /// Calculate the hit rate (percentage of reuses)
    pub fn hit_rate(&self) -> f64 {
        let total = self.allocations + self.reuses;
        if total == 0 {
            0.0
        } else {
            (self.reuses as f64 / total as f64) * 100.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_pool_acquire_release() {
        let pool = IoBufferPool::default_pool();

        // Acquire a buffer
        let buf = pool.acquire(8192);
        assert!(buf.capacity() >= 8192);

        // Release it back
        pool.release(buf);

        // Stats should show allocation and reuse
        let stats = pool.stats();
        assert_eq!(stats.allocations, 1);

        // Acquire again - should reuse
        let _buf = pool.acquire(8192);
        let stats = pool.stats();
        assert_eq!(stats.reuses, 1);
    }

    #[test]
    fn test_size_class_rounding() {
        let pool = IoBufferPool::default_pool();

        // Should round up to 4KB
        let buf = pool.acquire(2048);
        assert_eq!(buf.capacity(), 4 * 1024);

        // Should round up to 64KB
        let buf = pool.acquire(40 * 1024);
        assert_eq!(buf.capacity(), 64 * 1024);
    }

    #[test]
    fn test_max_per_class_limit() {
        let pool = IoBufferPool::new(vec![4096], 2);

        // Acquire and release 3 buffers
        let buf1 = pool.acquire(4096);
        let buf2 = pool.acquire(4096);
        let buf3 = pool.acquire(4096);

        pool.release(buf1);
        pool.release(buf2);
        pool.release(buf3);

        // Should only keep 2
        let stats = pool.stats();
        assert_eq!(stats.total_buffers, 2);
    }

    #[test]
    fn test_hit_rate_calculation() {
        let pool = IoBufferPool::default_pool();

        let buf1 = pool.acquire(4096);
        pool.release(buf1);

        let _buf2 = pool.acquire(4096); // reuse
        let _buf3 = pool.acquire(4096); // new allocation

        let stats = pool.stats();
        assert_eq!(stats.allocations, 2);
        assert_eq!(stats.reuses, 1);
        assert_eq!(stats.hit_rate(), 33.33333333333333);
    }
}
