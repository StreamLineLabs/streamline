//! DPDK mbuf (memory buffer) management

use super::{DpdkError, DpdkResult, MempoolConfig};
use parking_lot::Mutex;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{debug, trace, warn};

/// DPDK memory pool for packet buffers
///
/// A mempool is a fixed-size pool of mbufs (memory buffers) used
/// for zero-copy packet processing. Each mbuf contains:
/// - Header with metadata (length, port, offsets)
/// - Headroom for prepending headers
/// - Data room for packet payload
pub struct MbufPool {
    /// Pool name
    name: String,

    /// Configuration
    config: MempoolConfig,

    /// Base memory address (simulated)
    base_addr: *mut u8,

    /// Total memory size
    total_size: usize,

    /// Free mbuf indices
    free_list: Mutex<Vec<u32>>,

    /// Statistics
    stats: MbufPoolStats,
}

// SAFETY: MbufPool manages raw memory but access is synchronized
unsafe impl Send for MbufPool {}
unsafe impl Sync for MbufPool {}

/// Mbuf pool statistics
#[derive(Debug, Default)]
pub struct MbufPoolStats {
    /// Allocations
    pub alloc_count: AtomicU64,
    /// Frees
    pub free_count: AtomicU64,
    /// Allocation failures
    pub alloc_failures: AtomicU64,
    /// Peak usage
    pub peak_usage: AtomicU64,
}

impl MbufPool {
    /// Create a new mbuf pool
    pub fn new(name: &str, config: MempoolConfig) -> DpdkResult<Self> {
        config.validate().map_err(DpdkError::Config)?;

        let mbuf_size = Self::mbuf_size(&config);
        let total_size = config.mbuf_count as usize * mbuf_size;

        // Allocate memory (in production, from huge pages)
        let layout = std::alloc::Layout::from_size_align(total_size, 4096)
            .map_err(|e| DpdkError::Memory(format!("Invalid layout: {}", e)))?;

        let base_addr = unsafe { std::alloc::alloc_zeroed(layout) };
        if base_addr.is_null() {
            return Err(DpdkError::Memory(
                "Failed to allocate mbuf pool".to_string(),
            ));
        }

        // Initialize free list
        let free_list: Vec<u32> = (0..config.mbuf_count).collect();

        debug!(
            name = name,
            mbuf_count = config.mbuf_count,
            mbuf_size = mbuf_size,
            total_size = total_size,
            "Created mbuf pool"
        );

        Ok(Self {
            name: name.to_string(),
            config,
            base_addr,
            total_size,
            free_list: Mutex::new(free_list),
            stats: MbufPoolStats::default(),
        })
    }

    /// Calculate single mbuf size
    fn mbuf_size(config: &MempoolConfig) -> usize {
        // mbuf = header (128) + headroom (128) + data room + priv
        128 + 128 + config.mbuf_data_size as usize + config.priv_size as usize
    }

    /// Allocate a single mbuf
    pub fn alloc(&self) -> Option<Mbuf> {
        let mut free_list = self.free_list.lock();

        if let Some(index) = free_list.pop() {
            self.stats.alloc_count.fetch_add(1, Ordering::Relaxed);

            // Update peak usage
            let in_use = self.config.mbuf_count - free_list.len() as u32;
            let _ = self
                .stats
                .peak_usage
                .fetch_max(in_use as u64, Ordering::Relaxed);

            let mbuf_size = Self::mbuf_size(&self.config);
            let offset = index as usize * mbuf_size;

            Some(Mbuf {
                pool_index: index,
                data_offset: 128 + 128, // After header and headroom
                data_len: 0,
                headroom: 128,
                data_room: self.config.mbuf_data_size,
                port: 0,
                queue: 0,
                hash: 0,
                timestamp: 0,
            })
        } else {
            self.stats.alloc_failures.fetch_add(1, Ordering::Relaxed);
            warn!(pool = %self.name, "Mbuf pool exhausted");
            None
        }
    }

    /// Allocate multiple mbufs in bulk
    pub fn alloc_bulk(&self, count: usize) -> Vec<Mbuf> {
        let mut free_list = self.free_list.lock();
        let available = std::cmp::min(count, free_list.len());

        let mut mbufs = Vec::with_capacity(available);
        let mbuf_size = Self::mbuf_size(&self.config);

        for _ in 0..available {
            if let Some(index) = free_list.pop() {
                mbufs.push(Mbuf {
                    pool_index: index,
                    data_offset: 128 + 128,
                    data_len: 0,
                    headroom: 128,
                    data_room: self.config.mbuf_data_size,
                    port: 0,
                    queue: 0,
                    hash: 0,
                    timestamp: 0,
                });
            }
        }

        self.stats
            .alloc_count
            .fetch_add(mbufs.len() as u64, Ordering::Relaxed);

        if mbufs.len() < count {
            self.stats
                .alloc_failures
                .fetch_add((count - mbufs.len()) as u64, Ordering::Relaxed);
        }

        // Update peak usage
        let in_use = self.config.mbuf_count - free_list.len() as u32;
        let _ = self
            .stats
            .peak_usage
            .fetch_max(in_use as u64, Ordering::Relaxed);

        mbufs
    }

    /// Free an mbuf back to the pool
    pub fn free(&self, mbuf: Mbuf) {
        self.stats.free_count.fetch_add(1, Ordering::Relaxed);
        self.free_list.lock().push(mbuf.pool_index);
    }

    /// Free multiple mbufs in bulk
    pub fn free_bulk(&self, mbufs: Vec<Mbuf>) {
        self.stats
            .free_count
            .fetch_add(mbufs.len() as u64, Ordering::Relaxed);
        let mut free_list = self.free_list.lock();
        for mbuf in mbufs {
            free_list.push(mbuf.pool_index);
        }
    }

    /// Get number of free mbufs
    pub fn free_count(&self) -> usize {
        self.free_list.lock().len()
    }

    /// Get number of mbufs in use
    pub fn in_use(&self) -> usize {
        self.config.mbuf_count as usize - self.free_count()
    }

    /// Get pool name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get statistics snapshot
    pub fn stats(&self) -> MbufPoolStatsSnapshot {
        MbufPoolStatsSnapshot {
            alloc_count: self.stats.alloc_count.load(Ordering::Relaxed),
            free_count: self.stats.free_count.load(Ordering::Relaxed),
            alloc_failures: self.stats.alloc_failures.load(Ordering::Relaxed),
            peak_usage: self.stats.peak_usage.load(Ordering::Relaxed),
            current_free: self.free_count() as u64,
            current_in_use: self.in_use() as u64,
        }
    }
}

impl Drop for MbufPool {
    fn drop(&mut self) {
        if !self.base_addr.is_null() {
            if let Ok(layout) = std::alloc::Layout::from_size_align(self.total_size, 4096) {
                unsafe {
                    std::alloc::dealloc(self.base_addr, layout);
                }
            }
        }
    }
}

/// Memory buffer (mbuf) for packet data
///
/// An mbuf represents a packet or packet segment in DPDK.
/// It contains metadata and a reference to the data buffer.
#[derive(Debug, Clone)]
pub struct Mbuf {
    /// Index in the pool
    pool_index: u32,

    /// Offset to data start within buffer
    data_offset: u16,

    /// Data length
    pub data_len: u16,

    /// Headroom available
    headroom: u16,

    /// Data room size
    data_room: u16,

    /// Source port
    pub port: u16,

    /// Source queue
    pub queue: u16,

    /// RSS hash value
    pub hash: u32,

    /// Hardware timestamp (if available)
    pub timestamp: u64,
}

impl Mbuf {
    /// Get available headroom
    pub fn headroom(&self) -> u16 {
        self.headroom
    }

    /// Get available tailroom
    pub fn tailroom(&self) -> u16 {
        self.data_room.saturating_sub(self.data_len)
    }

    /// Get data room size
    pub fn data_room(&self) -> u16 {
        self.data_room
    }

    /// Prepend data (reduce headroom)
    pub fn prepend(&mut self, len: u16) -> bool {
        if len > self.headroom {
            return false;
        }
        self.data_offset -= len;
        self.headroom -= len;
        self.data_len += len;
        true
    }

    /// Append data (reduce tailroom)
    pub fn append(&mut self, len: u16) -> bool {
        if len > self.tailroom() {
            return false;
        }
        self.data_len += len;
        true
    }

    /// Trim data from end
    pub fn trim(&mut self, len: u16) -> bool {
        if len > self.data_len {
            return false;
        }
        self.data_len -= len;
        true
    }

    /// Adjust data start (positive = skip bytes)
    pub fn adj(&mut self, offset: i16) {
        if offset >= 0 {
            let offset = offset as u16;
            if offset <= self.data_len {
                self.data_offset += offset;
                self.data_len -= offset;
                self.headroom += offset;
            }
        } else {
            let offset = (-offset) as u16;
            if offset <= self.headroom {
                self.data_offset -= offset;
                self.data_len += offset;
                self.headroom -= offset;
            }
        }
    }

    /// Reset mbuf to initial state
    pub fn reset(&mut self) {
        self.data_offset = 128 + 128; // Header + default headroom
        self.data_len = 0;
        self.headroom = 128;
        self.port = 0;
        self.queue = 0;
        self.hash = 0;
        self.timestamp = 0;
    }

    /// Get pool index (for internal use)
    pub(crate) fn pool_index(&self) -> u32 {
        self.pool_index
    }
}

/// Snapshot of mbuf pool statistics
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct MbufPoolStatsSnapshot {
    /// Total allocations
    pub alloc_count: u64,
    /// Total frees
    pub free_count: u64,
    /// Allocation failures
    pub alloc_failures: u64,
    /// Peak usage
    pub peak_usage: u64,
    /// Current free count
    pub current_free: u64,
    /// Current in-use count
    pub current_in_use: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mbuf_pool_creation() {
        let config = MempoolConfig {
            mbuf_count: 64,
            ..Default::default()
        };

        let pool = MbufPool::new("test", config).unwrap();
        assert_eq!(pool.free_count(), 64);
        assert_eq!(pool.in_use(), 0);
    }

    #[test]
    fn test_mbuf_alloc_free() {
        let config = MempoolConfig {
            mbuf_count: 4,
            ..Default::default()
        };

        let pool = MbufPool::new("test", config).unwrap();

        // Allocate all
        let mut mbufs = Vec::new();
        for _ in 0..4 {
            let mbuf = pool.alloc().expect("Should allocate");
            mbufs.push(mbuf);
        }

        assert_eq!(pool.free_count(), 0);
        assert_eq!(pool.in_use(), 4);

        // Should fail
        assert!(pool.alloc().is_none());

        // Free one
        pool.free(mbufs.pop().unwrap());
        assert_eq!(pool.free_count(), 1);

        // Should succeed again
        assert!(pool.alloc().is_some());
    }

    #[test]
    fn test_mbuf_bulk_operations() {
        let config = MempoolConfig {
            mbuf_count: 16,
            ..Default::default()
        };

        let pool = MbufPool::new("test", config).unwrap();

        // Bulk allocate
        let mbufs = pool.alloc_bulk(8);
        assert_eq!(mbufs.len(), 8);
        assert_eq!(pool.free_count(), 8);

        // Bulk free
        pool.free_bulk(mbufs);
        assert_eq!(pool.free_count(), 16);
    }

    #[test]
    fn test_mbuf_prepend_append() {
        let config = MempoolConfig::default();
        let pool = MbufPool::new("test", config).unwrap();
        let mut mbuf = pool.alloc().unwrap();

        // Initial state
        assert_eq!(mbuf.data_len, 0);
        assert_eq!(mbuf.headroom(), 128);

        // Append some data
        assert!(mbuf.append(100));
        assert_eq!(mbuf.data_len, 100);

        // Prepend header
        assert!(mbuf.prepend(14));
        assert_eq!(mbuf.data_len, 114);
        assert_eq!(mbuf.headroom(), 114);

        // Trim
        assert!(mbuf.trim(14));
        assert_eq!(mbuf.data_len, 100);
    }

    #[test]
    fn test_mbuf_adj() {
        let config = MempoolConfig::default();
        let pool = MbufPool::new("test", config).unwrap();
        let mut mbuf = pool.alloc().unwrap();

        mbuf.append(100);
        assert_eq!(mbuf.data_len, 100);

        // Adjust forward (skip bytes)
        mbuf.adj(14);
        assert_eq!(mbuf.data_len, 86);
        assert_eq!(mbuf.headroom(), 142);

        // Adjust backward
        mbuf.adj(-14);
        assert_eq!(mbuf.data_len, 100);
        assert_eq!(mbuf.headroom(), 128);
    }

    #[test]
    fn test_pool_stats() {
        let config = MempoolConfig {
            mbuf_count: 8,
            ..Default::default()
        };

        let pool = MbufPool::new("test", config).unwrap();

        let _ = pool.alloc();
        let _ = pool.alloc();

        let stats = pool.stats();
        assert_eq!(stats.alloc_count, 2);
        assert_eq!(stats.current_in_use, 2);
        assert_eq!(stats.current_free, 6);
    }
}
