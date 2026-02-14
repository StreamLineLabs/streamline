//! AF_XDP socket implementation for zero-copy packet I/O

use super::{AfXdpSocketConfig, UmemConfig, XdpError, XdpResult};
use parking_lot::{Mutex, RwLock};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{debug, info, trace, warn};

/// UMEM (User Memory) region for AF_XDP
///
/// UMEM is a shared memory region between kernel and userspace
/// for zero-copy packet transfer.
pub struct Umem {
    /// Configuration
    config: UmemConfig,

    /// Base address of UMEM region
    base_addr: *mut u8,

    /// Size of UMEM region
    size: usize,

    /// Free frame indices
    free_frames: Mutex<VecDeque<u32>>,

    /// Number of frames in use
    frames_in_use: AtomicU64,

    /// UMEM file descriptor (when registered with kernel)
    fd: Option<i32>,
}

// SAFETY: Umem manages raw memory but access is synchronized through Mutex
unsafe impl Send for Umem {}
unsafe impl Sync for Umem {}

impl Umem {
    /// Create a new UMEM region
    pub fn new(config: UmemConfig) -> XdpResult<Self> {
        config.validate().map_err(XdpError::Config)?;

        let size = config.total_size();

        // Allocate UMEM memory
        // In production, we'd use mmap with MAP_HUGETLB if huge pages enabled
        let base_addr = Self::allocate_memory(size, config.use_huge_pages)?;

        // Initialize free frame list
        let mut free_frames = VecDeque::with_capacity(config.frame_count as usize);
        for i in 0..config.frame_count {
            free_frames.push_back(i);
        }

        info!(
            size = size,
            frame_count = config.frame_count,
            frame_size = config.frame_size,
            "Created UMEM region"
        );

        Ok(Self {
            config,
            base_addr,
            size,
            free_frames: Mutex::new(free_frames),
            frames_in_use: AtomicU64::new(0),
            fd: None,
        })
    }

    /// Allocate aligned memory for UMEM
    fn allocate_memory(size: usize, _huge_pages: bool) -> XdpResult<*mut u8> {
        // In production with huge pages:
        // let ptr = unsafe { libc::mmap(
        //     std::ptr::null_mut(),
        //     size,
        //     libc::PROT_READ | libc::PROT_WRITE,
        //     libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_HUGETLB,
        //     -1,
        //     0,
        // ) };

        // For now, use regular aligned allocation
        let layout = std::alloc::Layout::from_size_align(size, 4096)
            .map_err(|e| XdpError::Umem(format!("Invalid layout: {}", e)))?;

        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };

        if ptr.is_null() {
            return Err(XdpError::Umem("Failed to allocate UMEM memory".to_string()));
        }

        Ok(ptr)
    }

    /// Register UMEM with the kernel
    pub fn register(&mut self) -> XdpResult<()> {
        // In production: call setsockopt(SOL_XDP, XDP_UMEM_REG, ...)
        debug!(size = self.size, "Registering UMEM with kernel");
        self.fd = Some(300); // Simulated FD
        Ok(())
    }

    /// Allocate a frame from UMEM
    pub fn alloc_frame(&self) -> Option<UmemFrame> {
        let mut free_frames = self.free_frames.lock();
        let frame_idx = free_frames.pop_front()?;

        self.frames_in_use.fetch_add(1, Ordering::Relaxed);

        let offset = frame_idx as usize * self.config.frame_size as usize;
        let data_offset = offset + self.config.frame_headroom as usize;

        Some(UmemFrame {
            index: frame_idx,
            addr: offset as u64,
            data_addr: data_offset as u64,
            len: 0,
            headroom: self.config.frame_headroom,
            frame_size: self.config.frame_size,
        })
    }

    /// Free a frame back to UMEM
    pub fn free_frame(&self, frame: UmemFrame) {
        let mut free_frames = self.free_frames.lock();
        free_frames.push_back(frame.index);
        self.frames_in_use.fetch_sub(1, Ordering::Relaxed);
    }

    /// Get pointer to frame data
    ///
    /// # Safety
    /// Caller must ensure frame is valid and not already freed
    pub unsafe fn frame_ptr(&self, frame: &UmemFrame) -> *mut u8 {
        self.base_addr.add(frame.data_addr as usize)
    }

    /// Get frame data as slice
    ///
    /// # Safety
    /// Caller must ensure frame is valid and length is correct
    pub unsafe fn frame_data(&self, frame: &UmemFrame) -> &[u8] {
        let ptr = self.frame_ptr(frame);
        std::slice::from_raw_parts(ptr, frame.len as usize)
    }

    /// Get frame data as mutable slice
    ///
    /// # Safety
    /// Caller must ensure frame is valid and has exclusive access
    pub unsafe fn frame_data_mut(&self, frame: &mut UmemFrame) -> &mut [u8] {
        let ptr = self.frame_ptr(frame);
        let max_len = (frame.frame_size - frame.headroom) as usize;
        std::slice::from_raw_parts_mut(ptr, max_len)
    }

    /// Get number of free frames
    pub fn free_frame_count(&self) -> usize {
        self.free_frames.lock().len()
    }

    /// Get number of frames in use
    pub fn frames_in_use(&self) -> u64 {
        self.frames_in_use.load(Ordering::Relaxed)
    }

    /// Get configuration
    pub fn config(&self) -> &UmemConfig {
        &self.config
    }
}

impl Drop for Umem {
    fn drop(&mut self) {
        // Deallocate memory
        if !self.base_addr.is_null() {
            if let Ok(layout) = std::alloc::Layout::from_size_align(self.size, 4096) {
                unsafe {
                    std::alloc::dealloc(self.base_addr, layout);
                }
            }
        }
    }
}

/// A frame in UMEM
#[derive(Debug, Clone)]
pub struct UmemFrame {
    /// Frame index in UMEM
    pub index: u32,
    /// Base address (offset from UMEM start)
    pub addr: u64,
    /// Data address (after headroom)
    pub data_addr: u64,
    /// Current data length
    pub len: u32,
    /// Headroom size
    pub headroom: u32,
    /// Total frame size
    pub frame_size: u32,
}

impl UmemFrame {
    /// Get maximum data size
    pub fn max_data_size(&self) -> u32 {
        self.frame_size - self.headroom
    }

    /// Set data length
    pub fn set_len(&mut self, len: u32) {
        debug_assert!(len <= self.max_data_size());
        self.len = len;
    }
}

/// AF_XDP socket for zero-copy packet reception
pub struct AfXdpSocket {
    /// Configuration
    config: AfXdpSocketConfig,

    /// Shared UMEM region
    umem: Arc<Umem>,

    /// Socket file descriptor
    fd: Option<i32>,

    /// Queue ID this socket is bound to
    queue_id: u32,

    /// Interface index
    if_index: u32,

    /// Whether socket is bound
    bound: AtomicBool,

    /// RX ring (filled by kernel with received packets)
    rx_ring: RwLock<XskRing>,

    /// TX ring (filled by userspace with packets to send)
    tx_ring: RwLock<XskRing>,

    /// Fill ring (userspace provides empty frames to kernel)
    fill_ring: RwLock<XskRing>,

    /// Completion ring (kernel returns sent frames to userspace)
    comp_ring: RwLock<XskRing>,

    /// Statistics
    stats: AfXdpSocketStats,
}

/// XSK ring (shared ring buffer between kernel and userspace)
#[derive(Debug)]
pub struct XskRing {
    /// Producer index (owned by producer side)
    producer: u32,
    /// Consumer index (owned by consumer side)
    consumer: u32,
    /// Ring size (power of 2)
    size: u32,
    /// Ring mask for efficient modulo
    mask: u32,
    /// Ring entries (frame descriptors)
    entries: Vec<XskDesc>,
}

/// XSK descriptor (frame reference in ring)
#[derive(Debug, Clone, Copy, Default)]
pub struct XskDesc {
    /// Address in UMEM
    pub addr: u64,
    /// Length of data
    pub len: u32,
    /// Options (reserved)
    pub options: u32,
}

impl XskRing {
    /// Create a new ring
    pub fn new(size: u32) -> Self {
        debug_assert!(size.is_power_of_two());
        Self {
            producer: 0,
            consumer: 0,
            size,
            mask: size - 1,
            entries: vec![XskDesc::default(); size as usize],
        }
    }

    /// Get number of available entries for consumption
    pub fn available(&self) -> u32 {
        self.producer.wrapping_sub(self.consumer)
    }

    /// Get number of free entries for production
    pub fn free(&self) -> u32 {
        self.size - self.available()
    }

    /// Produce entries into the ring
    pub fn produce(&mut self, descs: &[XskDesc]) -> u32 {
        let n = std::cmp::min(descs.len() as u32, self.free());
        for i in 0..n as usize {
            let idx = (self.producer + i as u32) & self.mask;
            self.entries[idx as usize] = descs[i];
        }
        self.producer = self.producer.wrapping_add(n);
        n
    }

    /// Consume entries from the ring
    pub fn consume(&mut self, max: u32) -> Vec<XskDesc> {
        let n = std::cmp::min(max, self.available());
        let mut descs = Vec::with_capacity(n as usize);
        for i in 0..n {
            let idx = (self.consumer + i) & self.mask;
            descs.push(self.entries[idx as usize]);
        }
        self.consumer = self.consumer.wrapping_add(n);
        descs
    }

    /// Peek at entries without consuming
    pub fn peek(&self, max: u32) -> Vec<XskDesc> {
        let n = std::cmp::min(max, self.available());
        let mut descs = Vec::with_capacity(n as usize);
        for i in 0..n {
            let idx = (self.consumer + i) & self.mask;
            descs.push(self.entries[idx as usize]);
        }
        descs
    }
}

/// AF_XDP socket statistics
#[derive(Debug, Default)]
pub struct AfXdpSocketStats {
    /// Packets received
    pub rx_packets: AtomicU64,
    /// Bytes received
    pub rx_bytes: AtomicU64,
    /// Packets sent
    pub tx_packets: AtomicU64,
    /// Bytes sent
    pub tx_bytes: AtomicU64,
    /// RX ring full events (dropped packets)
    pub rx_dropped: AtomicU64,
    /// TX ring full events
    pub tx_dropped: AtomicU64,
    /// Fill ring empty events
    pub fill_empty: AtomicU64,
    /// Invalid descriptors
    pub invalid_descs: AtomicU64,
}

impl AfXdpSocket {
    /// Create a new AF_XDP socket
    pub fn new(
        config: AfXdpSocketConfig,
        umem: Arc<Umem>,
        if_index: u32,
        queue_id: u32,
    ) -> XdpResult<Self> {
        let rx_ring = XskRing::new(config.rx_size);
        let tx_ring = XskRing::new(config.tx_size);
        let fill_ring = XskRing::new(umem.config().fill_size);
        let comp_ring = XskRing::new(umem.config().comp_size);

        Ok(Self {
            config,
            umem,
            fd: None,
            queue_id,
            if_index,
            bound: AtomicBool::new(false),
            rx_ring: RwLock::new(rx_ring),
            tx_ring: RwLock::new(tx_ring),
            fill_ring: RwLock::new(fill_ring),
            comp_ring: RwLock::new(comp_ring),
            stats: AfXdpSocketStats::default(),
        })
    }

    /// Bind the socket to the interface and queue
    pub fn bind(&mut self, xsk_map_fd: Option<i32>) -> XdpResult<()> {
        if self.bound.load(Ordering::SeqCst) {
            return Ok(());
        }

        info!(
            if_index = self.if_index,
            queue_id = self.queue_id,
            "Binding AF_XDP socket"
        );

        // In production:
        // 1. Create AF_XDP socket: socket(AF_XDP, SOCK_RAW, 0)
        // 2. Set socket options: setsockopt(SOL_XDP, ...)
        // 3. Bind to interface/queue: bind(fd, &sxdp, sizeof(sxdp))
        // 4. Register with XSK map if provided

        self.fd = Some(400 + self.queue_id as i32); // Simulated FD

        // Pre-fill the fill ring with empty frames
        self.refill_fill_ring()?;

        self.bound.store(true, Ordering::SeqCst);

        info!(
            if_index = self.if_index,
            queue_id = self.queue_id,
            fd = ?self.fd,
            "AF_XDP socket bound"
        );

        Ok(())
    }

    /// Receive packets from the socket
    pub fn recv(&self, max_packets: u32) -> XdpResult<Vec<ReceivedPacket>> {
        if !self.bound.load(Ordering::SeqCst) {
            return Err(XdpError::Socket("Socket not bound".to_string()));
        }

        // In production:
        // 1. Check RX ring for available descriptors
        // 2. Process each descriptor (contains UMEM frame reference)
        // 3. Return frames to fill ring when done

        let mut rx_ring = self.rx_ring.write();
        let descs = rx_ring.consume(max_packets);

        let mut packets = Vec::with_capacity(descs.len());
        for desc in descs {
            // Allocate a frame to hold the received data
            if let Some(mut frame) = self.umem.alloc_frame() {
                frame.set_len(desc.len);

                packets.push(ReceivedPacket {
                    frame,
                    timestamp: std::time::Instant::now(),
                });

                self.stats.rx_packets.fetch_add(1, Ordering::Relaxed);
                self.stats
                    .rx_bytes
                    .fetch_add(desc.len as u64, Ordering::Relaxed);
            } else {
                self.stats.rx_dropped.fetch_add(1, Ordering::Relaxed);
                warn!("UMEM frame exhaustion during receive");
            }
        }

        // Refill the fill ring
        if let Err(e) = self.refill_fill_ring() {
            warn!(error = %e, "Failed to refill fill ring");
        }

        Ok(packets)
    }

    /// Send packets through the socket
    pub fn send(&self, packets: &[UmemFrame]) -> XdpResult<u32> {
        if !self.bound.load(Ordering::SeqCst) {
            return Err(XdpError::Socket("Socket not bound".to_string()));
        }

        let descs: Vec<XskDesc> = packets
            .iter()
            .map(|f| XskDesc {
                addr: f.data_addr,
                len: f.len,
                options: 0,
            })
            .collect();

        let mut tx_ring = self.tx_ring.write();
        let sent = tx_ring.produce(&descs);

        if sent > 0 {
            // In production: trigger kernel with sendto() or kick the TX ring
            self.stats
                .tx_packets
                .fetch_add(sent as u64, Ordering::Relaxed);

            let bytes: u64 = packets[..sent as usize].iter().map(|p| p.len as u64).sum();
            self.stats.tx_bytes.fetch_add(bytes, Ordering::Relaxed);
        }

        if (sent as usize) < packets.len() {
            self.stats
                .tx_dropped
                .fetch_add((packets.len() - sent as usize) as u64, Ordering::Relaxed);
        }

        Ok(sent)
    }

    /// Complete TX operations (reclaim frames from completion ring)
    pub fn complete_tx(&self) -> Vec<UmemFrame> {
        let mut comp_ring = self.comp_ring.write();
        let descs = comp_ring.consume(self.config.batch_size);

        // In production, convert descriptors back to frames
        // For now, return empty (frames would be tracked separately)
        Vec::new()
    }

    /// Refill the fill ring with empty frames
    fn refill_fill_ring(&self) -> XdpResult<()> {
        let mut fill_ring = self.fill_ring.write();
        let needed = fill_ring.free();

        if needed == 0 {
            return Ok(());
        }

        let mut descs = Vec::with_capacity(needed as usize);
        for _ in 0..needed {
            if let Some(frame) = self.umem.alloc_frame() {
                descs.push(XskDesc {
                    addr: frame.addr,
                    len: 0,
                    options: 0,
                });
                // Note: In production, we'd track these frames
            } else {
                self.stats.fill_empty.fetch_add(1, Ordering::Relaxed);
                break;
            }
        }

        if !descs.is_empty() {
            fill_ring.produce(&descs);
        }

        Ok(())
    }

    /// Check if socket is bound
    pub fn is_bound(&self) -> bool {
        self.bound.load(Ordering::SeqCst)
    }

    /// Get socket file descriptor
    pub fn fd(&self) -> Option<i32> {
        self.fd
    }

    /// Get queue ID
    pub fn queue_id(&self) -> u32 {
        self.queue_id
    }

    /// Get statistics snapshot
    pub fn stats(&self) -> AfXdpSocketStatsSnapshot {
        AfXdpSocketStatsSnapshot {
            rx_packets: self.stats.rx_packets.load(Ordering::Relaxed),
            rx_bytes: self.stats.rx_bytes.load(Ordering::Relaxed),
            tx_packets: self.stats.tx_packets.load(Ordering::Relaxed),
            tx_bytes: self.stats.tx_bytes.load(Ordering::Relaxed),
            rx_dropped: self.stats.rx_dropped.load(Ordering::Relaxed),
            tx_dropped: self.stats.tx_dropped.load(Ordering::Relaxed),
            fill_empty: self.stats.fill_empty.load(Ordering::Relaxed),
            invalid_descs: self.stats.invalid_descs.load(Ordering::Relaxed),
        }
    }
}

/// Received packet from AF_XDP socket
#[derive(Debug)]
pub struct ReceivedPacket {
    /// UMEM frame containing packet data
    pub frame: UmemFrame,
    /// Receive timestamp
    pub timestamp: std::time::Instant,
}

impl ReceivedPacket {
    /// Get packet data
    ///
    /// # Safety
    /// Caller must ensure the packet's UMEM is still valid
    pub unsafe fn data<'a>(&self, umem: &'a Umem) -> &'a [u8] {
        umem.frame_data(&self.frame)
    }
}

/// Snapshot of AF_XDP socket statistics
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct AfXdpSocketStatsSnapshot {
    pub rx_packets: u64,
    pub rx_bytes: u64,
    pub tx_packets: u64,
    pub tx_bytes: u64,
    pub rx_dropped: u64,
    pub tx_dropped: u64,
    pub fill_empty: u64,
    pub invalid_descs: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_umem_creation() {
        let config = UmemConfig {
            frame_count: 64,
            frame_size: 4096,
            frame_headroom: 256,
            fill_size: 32,
            comp_size: 32,
            use_huge_pages: false,
        };

        let umem = Umem::new(config).unwrap();
        assert_eq!(umem.free_frame_count(), 64);
        assert_eq!(umem.frames_in_use(), 0);
    }

    #[test]
    fn test_umem_frame_allocation() {
        let config = UmemConfig {
            frame_count: 4,
            ..Default::default()
        };

        let umem = Umem::new(config).unwrap();

        // Allocate all frames
        let mut frames = Vec::new();
        for _ in 0..4 {
            let frame = umem.alloc_frame().expect("Should allocate");
            frames.push(frame);
        }

        assert_eq!(umem.free_frame_count(), 0);
        assert_eq!(umem.frames_in_use(), 4);

        // Should fail to allocate more
        assert!(umem.alloc_frame().is_none());

        // Free one frame
        umem.free_frame(frames.pop().unwrap());
        assert_eq!(umem.free_frame_count(), 1);
        assert_eq!(umem.frames_in_use(), 3);

        // Should allocate again
        assert!(umem.alloc_frame().is_some());
    }

    #[test]
    fn test_xsk_ring() {
        let mut ring = XskRing::new(8);

        assert_eq!(ring.available(), 0);
        assert_eq!(ring.free(), 8);

        // Produce some entries
        let descs = vec![
            XskDesc {
                addr: 0,
                len: 100,
                options: 0,
            },
            XskDesc {
                addr: 4096,
                len: 200,
                options: 0,
            },
        ];
        let produced = ring.produce(&descs);
        assert_eq!(produced, 2);
        assert_eq!(ring.available(), 2);

        // Consume entries
        let consumed = ring.consume(1);
        assert_eq!(consumed.len(), 1);
        assert_eq!(consumed[0].len, 100);
        assert_eq!(ring.available(), 1);
    }

    #[test]
    fn test_umem_frame() {
        let frame = UmemFrame {
            index: 0,
            addr: 0,
            data_addr: 256,
            len: 0,
            headroom: 256,
            frame_size: 4096,
        };

        assert_eq!(frame.max_data_size(), 4096 - 256);
    }
}
