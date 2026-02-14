//! XDP data plane - high-level API combining XDP components

use super::{
    AfXdpSocket, AfXdpSocketConfig, AfXdpSocketStatsSnapshot, FilterStatsSnapshot, PacketMeta,
    ReceivedPacket, Umem, UmemConfig, UmemFrame, XdpCapabilities, XdpConfig, XdpConfigMode,
    XdpError, XdpFilter, XdpMode, XdpProgram, XdpProgramStatsSnapshot, XdpResult,
};
use parking_lot::RwLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// XDP data plane for kernel-bypass packet processing
///
/// This is the main entry point for using XDP networking in Streamline.
/// It manages the XDP program, AF_XDP sockets, and packet processing.
pub struct XdpDataPlane {
    /// Configuration
    config: XdpConfig,

    /// System capabilities
    capabilities: XdpCapabilities,

    /// XDP program
    program: RwLock<Option<XdpProgram>>,

    /// Shared UMEM region
    umem: Arc<Umem>,

    /// AF_XDP sockets (one per queue)
    sockets: RwLock<Vec<AfXdpSocket>>,

    /// Packet filter
    filter: Arc<XdpFilter>,

    /// Whether data plane is running
    running: AtomicBool,

    /// Packets received
    packets_received: AtomicU64,

    /// Packets sent
    packets_sent: AtomicU64,

    /// Bytes received
    bytes_received: AtomicU64,

    /// Bytes sent
    bytes_sent: AtomicU64,
}

impl XdpDataPlane {
    /// Create a new XDP data plane
    pub fn new(config: XdpConfig) -> XdpResult<Self> {
        let capabilities = XdpCapabilities::detect();

        if !capabilities.is_supported() {
            return Err(XdpError::NotSupported(
                "XDP not supported on this system".to_string(),
            ));
        }

        // Validate interface
        capabilities.validate_interface(&config.interface)?;

        // Create UMEM
        let umem = Arc::new(Umem::new(config.umem.clone())?);

        // Create filter
        let filter = Arc::new(XdpFilter::new(config.filter.clone(), config.port));

        info!(
            interface = %config.interface,
            port = config.port,
            mode = ?config.mode,
            "Created XDP data plane"
        );

        Ok(Self {
            config,
            capabilities,
            program: RwLock::new(None),
            umem,
            sockets: RwLock::new(Vec::new()),
            filter,
            running: AtomicBool::new(false),
            packets_received: AtomicU64::new(0),
            packets_sent: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
        })
    }

    /// Start the XDP data plane
    pub async fn start(&self) -> XdpResult<()> {
        if self.running.load(Ordering::SeqCst) {
            return Ok(());
        }

        info!(interface = %self.config.interface, "Starting XDP data plane");

        // Determine XDP mode
        let mode = self.determine_mode()?;

        // Load and attach XDP program
        let mut program = XdpProgram::new(&self.config.interface, mode)?;
        program.attach()?;

        // Create AF_XDP sockets
        let iface_caps = self
            .capabilities
            .validate_interface(&self.config.interface)?;
        let num_queues = if self.config.rx_queues > 0 {
            self.config.rx_queues
        } else {
            iface_caps.rx_queues
        };

        let mut sockets = Vec::with_capacity(num_queues as usize);
        for queue_id in 0..num_queues {
            let mut socket = AfXdpSocket::new(
                self.config.socket.clone(),
                Arc::clone(&self.umem),
                iface_caps.index,
                queue_id,
            )?;

            socket.bind(program.xsk_map_fd())?;

            // Register socket with XDP program
            if let Some(fd) = socket.fd() {
                program.register_xsk(queue_id, fd)?;
            }

            sockets.push(socket);
        }

        // Store state
        *self.program.write() = Some(program);
        *self.sockets.write() = sockets;
        self.running.store(true, Ordering::SeqCst);

        info!(
            interface = %self.config.interface,
            mode = %mode,
            queues = num_queues,
            "XDP data plane started"
        );

        Ok(())
    }

    /// Stop the XDP data plane
    pub async fn stop(&self) -> XdpResult<()> {
        if !self.running.load(Ordering::SeqCst) {
            return Ok(());
        }

        info!(interface = %self.config.interface, "Stopping XDP data plane");

        // Clear sockets
        self.sockets.write().clear();

        // Detach program
        if let Some(mut program) = self.program.write().take() {
            program.detach()?;
        }

        self.running.store(false, Ordering::SeqCst);

        info!(interface = %self.config.interface, "XDP data plane stopped");
        Ok(())
    }

    /// Check if data plane is running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Receive packets from a specific queue
    pub fn recv(&self, queue_id: u32, max_packets: u32) -> XdpResult<Vec<ReceivedPacket>> {
        let sockets = self.sockets.read();
        let socket = sockets
            .get(queue_id as usize)
            .ok_or_else(|| XdpError::Socket(format!("Queue {} not found", queue_id)))?;

        let packets = socket.recv(max_packets)?;

        // Update stats
        self.packets_received
            .fetch_add(packets.len() as u64, Ordering::Relaxed);

        let bytes: u64 = packets.iter().map(|p| p.frame.len as u64).sum();
        self.bytes_received.fetch_add(bytes, Ordering::Relaxed);

        Ok(packets)
    }

    /// Send packets through a specific queue
    pub fn send(&self, queue_id: u32, frames: &[UmemFrame]) -> XdpResult<u32> {
        let sockets = self.sockets.read();
        let socket = sockets
            .get(queue_id as usize)
            .ok_or_else(|| XdpError::Socket(format!("Queue {} not found", queue_id)))?;

        let sent = socket.send(frames)?;

        // Update stats
        self.packets_sent.fetch_add(sent as u64, Ordering::Relaxed);

        let bytes: u64 = frames[..sent as usize].iter().map(|f| f.len as u64).sum();
        self.bytes_sent.fetch_add(bytes, Ordering::Relaxed);

        Ok(sent)
    }

    /// Get the packet filter
    pub fn filter(&self) -> &Arc<XdpFilter> {
        &self.filter
    }

    /// Get UMEM for frame allocation
    pub fn umem(&self) -> &Arc<Umem> {
        &self.umem
    }

    /// Get number of active queues
    pub fn queue_count(&self) -> usize {
        self.sockets.read().len()
    }

    /// Get data plane statistics
    pub fn stats(&self) -> XdpDataPlaneStats {
        let program_stats = self
            .program
            .read()
            .as_ref()
            .map(|p| p.stats().snapshot())
            .unwrap_or_default();

        let socket_stats: Vec<AfXdpSocketStatsSnapshot> =
            self.sockets.read().iter().map(|s| s.stats()).collect();

        let filter_stats = self.filter.stats();

        XdpDataPlaneStats {
            running: self.running.load(Ordering::Relaxed),
            interface: self.config.interface.clone(),
            mode: self.current_mode(),
            queue_count: self.queue_count(),
            packets_received: self.packets_received.load(Ordering::Relaxed),
            packets_sent: self.packets_sent.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
            umem_free_frames: self.umem.free_frame_count(),
            umem_frames_in_use: self.umem.frames_in_use() as usize,
            program_stats,
            socket_stats,
            filter_stats,
        }
    }

    /// Get capabilities
    pub fn capabilities(&self) -> &XdpCapabilities {
        &self.capabilities
    }

    /// Get current XDP mode
    fn current_mode(&self) -> Option<XdpMode> {
        self.program.read().as_ref().map(|p| p.mode())
    }

    /// Determine the best XDP mode to use
    fn determine_mode(&self) -> XdpResult<XdpMode> {
        match self.config.mode {
            XdpConfigMode::Skb => Ok(XdpMode::Skb),
            XdpConfigMode::Native => {
                let iface = self
                    .capabilities
                    .validate_interface(&self.config.interface)?;
                if iface.native_mode {
                    Ok(XdpMode::Native)
                } else {
                    warn!(
                        interface = %self.config.interface,
                        "Native XDP mode requested but not supported, falling back to SKB"
                    );
                    Ok(XdpMode::Skb)
                }
            }
            XdpConfigMode::Offload => {
                let iface = self
                    .capabilities
                    .validate_interface(&self.config.interface)?;
                if iface.offload_mode {
                    Ok(XdpMode::Offload)
                } else if iface.native_mode {
                    warn!(
                        interface = %self.config.interface,
                        "XDP offload requested but not supported, falling back to native"
                    );
                    Ok(XdpMode::Native)
                } else {
                    warn!(
                        interface = %self.config.interface,
                        "XDP offload requested but not supported, falling back to SKB"
                    );
                    Ok(XdpMode::Skb)
                }
            }
            XdpConfigMode::Auto => self
                .capabilities
                .best_mode_for(&self.config.interface)
                .ok_or_else(|| XdpError::InterfaceNotFound(self.config.interface.clone())),
        }
    }
}

impl Drop for XdpDataPlane {
    fn drop(&mut self) {
        if self.running.load(Ordering::SeqCst) {
            // Synchronous cleanup
            self.sockets.write().clear();
            if let Some(mut program) = self.program.write().take() {
                let _ = program.detach();
            }
        }
    }
}

/// XDP data plane statistics
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct XdpDataPlaneStats {
    /// Whether data plane is running
    pub running: bool,
    /// Interface name
    pub interface: String,
    /// Current XDP mode
    pub mode: Option<XdpMode>,
    /// Number of active queues
    pub queue_count: usize,
    /// Total packets received
    pub packets_received: u64,
    /// Total packets sent
    pub packets_sent: u64,
    /// Total bytes received
    pub bytes_received: u64,
    /// Total bytes sent
    pub bytes_sent: u64,
    /// UMEM free frames
    pub umem_free_frames: usize,
    /// UMEM frames in use
    pub umem_frames_in_use: usize,
    /// XDP program statistics
    pub program_stats: XdpProgramStatsSnapshot,
    /// Per-socket statistics
    pub socket_stats: Vec<AfXdpSocketStatsSnapshot>,
    /// Filter statistics
    pub filter_stats: FilterStatsSnapshot,
}

/// XDP packet receiver for async packet processing
pub struct XdpReceiver {
    data_plane: Arc<XdpDataPlane>,
    queue_id: u32,
    batch_size: u32,
}

impl XdpReceiver {
    /// Create a new receiver for a specific queue
    pub fn new(data_plane: Arc<XdpDataPlane>, queue_id: u32, batch_size: u32) -> Self {
        Self {
            data_plane,
            queue_id,
            batch_size,
        }
    }

    /// Receive a batch of packets
    pub fn recv(&self) -> XdpResult<Vec<ReceivedPacket>> {
        self.data_plane.recv(self.queue_id, self.batch_size)
    }

    /// Process received packets with a handler
    pub fn recv_with<F>(&self, mut handler: F) -> XdpResult<usize>
    where
        F: FnMut(ReceivedPacket),
    {
        let packets = self.recv()?;
        let count = packets.len();
        for packet in packets {
            handler(packet);
        }
        Ok(count)
    }
}

/// XDP packet sender for async packet transmission
pub struct XdpSender {
    data_plane: Arc<XdpDataPlane>,
    queue_id: u32,
}

impl XdpSender {
    /// Create a new sender for a specific queue
    pub fn new(data_plane: Arc<XdpDataPlane>, queue_id: u32) -> Self {
        Self {
            data_plane,
            queue_id,
        }
    }

    /// Allocate a frame for sending
    pub fn alloc_frame(&self) -> Option<UmemFrame> {
        self.data_plane.umem().alloc_frame()
    }

    /// Send frames
    pub fn send(&self, frames: &[UmemFrame]) -> XdpResult<u32> {
        self.data_plane.send(self.queue_id, frames)
    }

    /// Free a frame back to UMEM
    pub fn free_frame(&self, frame: UmemFrame) {
        self.data_plane.umem().free_frame(frame);
    }
}

/// Builder for XDP data plane
pub struct XdpDataPlaneBuilder {
    config: XdpConfig,
}

impl XdpDataPlaneBuilder {
    /// Create a new builder with default config
    pub fn new() -> Self {
        Self {
            config: XdpConfig::default(),
        }
    }

    /// Set interface name
    pub fn interface(mut self, interface: &str) -> Self {
        self.config.interface = interface.to_string();
        self
    }

    /// Set port to filter
    pub fn port(mut self, port: u16) -> Self {
        self.config.port = port;
        self
    }

    /// Set XDP mode
    pub fn mode(mut self, mode: XdpConfigMode) -> Self {
        self.config.mode = mode;
        self
    }

    /// Set number of RX queues
    pub fn rx_queues(mut self, queues: u32) -> Self {
        self.config.rx_queues = queues;
        self
    }

    /// Enable zero-copy mode
    pub fn zero_copy(mut self, enable: bool) -> Self {
        self.config.zero_copy = enable;
        self
    }

    /// Enable busy polling
    pub fn busy_poll(mut self, enable: bool) -> Self {
        self.config.busy_poll = enable;
        self
    }

    /// Set UMEM frame count
    pub fn umem_frames(mut self, count: u32) -> Self {
        self.config.umem.frame_count = count;
        self
    }

    /// Set batch size for packet processing
    pub fn batch_size(mut self, size: u32) -> Self {
        self.config.socket.batch_size = size;
        self
    }

    /// Enable rate limiting
    pub fn rate_limit(mut self, pps: u32) -> Self {
        self.config.filter.rate_limit_enabled = true;
        self.config.filter.rate_limit_pps = pps;
        self
    }

    /// Enable SYN flood protection
    pub fn syn_flood_protection(mut self, threshold: u32) -> Self {
        self.config.filter.syn_flood_protection = true;
        self.config.filter.syn_flood_threshold = threshold;
        self
    }

    /// Add blocked source prefix
    pub fn block_source(mut self, prefix: &str) -> Self {
        self.config
            .filter
            .blocked_source_prefixes
            .push(prefix.to_string());
        self
    }

    /// Add allowed source prefix
    pub fn allow_source(mut self, prefix: &str) -> Self {
        self.config
            .filter
            .allowed_source_prefixes
            .push(prefix.to_string());
        self
    }

    /// Build the XDP data plane
    pub fn build(self) -> XdpResult<XdpDataPlane> {
        XdpDataPlane::new(self.config)
    }
}

impl Default for XdpDataPlaneBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_plane_builder() {
        let builder = XdpDataPlaneBuilder::new()
            .interface("eth0")
            .port(9092)
            .mode(XdpConfigMode::Auto)
            .rx_queues(4)
            .zero_copy(true)
            .batch_size(64);

        // Building will fail without XDP support, but we can verify the builder works
        assert!(builder.config.interface == "eth0");
        assert!(builder.config.port == 9092);
        assert!(builder.config.rx_queues == 4);
    }

    #[test]
    fn test_xdp_mode_serialization() {
        // Test that XdpMode can be serialized
        let mode = XdpMode::Native;
        assert_eq!(format!("{}", mode), "native");
    }

    #[test]
    fn test_stats_default() {
        let stats = XdpDataPlaneStats {
            running: false,
            interface: "lo".to_string(),
            mode: None,
            queue_count: 0,
            packets_received: 0,
            packets_sent: 0,
            bytes_received: 0,
            bytes_sent: 0,
            umem_free_frames: 0,
            umem_frames_in_use: 0,
            program_stats: XdpProgramStatsSnapshot::default(),
            socket_stats: vec![],
            filter_stats: FilterStatsSnapshot::default(),
        };

        assert!(!stats.running);
    }
}
