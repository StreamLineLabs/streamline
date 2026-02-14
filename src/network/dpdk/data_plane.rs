//! DPDK data plane - high-level API for DPDK networking

use super::{
    check_requirements, DpdkConfig, DpdkError, DpdkPort, DpdkPortConfig, DpdkResult, Eal,
    LinkStatus, Mbuf, MbufPool, MbufPoolStatsSnapshot, PortStatsSnapshot,
};
use parking_lot::RwLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{debug, error, info, warn};

/// DPDK data plane for kernel-bypass networking
///
/// This is the main entry point for using DPDK in Streamline.
/// It manages EAL initialization, ports, and packet processing.
pub struct DpdkDataPlane {
    /// Configuration
    config: DpdkConfig,

    /// EAL instance
    eal: RwLock<Option<Eal>>,

    /// Memory pool for packet buffers
    mbuf_pool: RwLock<Option<Arc<MbufPool>>>,

    /// Managed ports
    ports: RwLock<Vec<DpdkPort>>,

    /// Whether data plane is running
    running: AtomicBool,

    /// Total packets received across all ports
    total_rx_packets: AtomicU64,

    /// Total packets transmitted across all ports
    total_tx_packets: AtomicU64,

    /// Total bytes received
    total_rx_bytes: AtomicU64,

    /// Total bytes transmitted
    total_tx_bytes: AtomicU64,
}

impl DpdkDataPlane {
    /// Create a new DPDK data plane (does not initialize yet)
    pub fn new(config: DpdkConfig) -> Self {
        Self {
            config,
            eal: RwLock::new(None),
            mbuf_pool: RwLock::new(None),
            ports: RwLock::new(Vec::new()),
            running: AtomicBool::new(false),
            total_rx_packets: AtomicU64::new(0),
            total_tx_packets: AtomicU64::new(0),
            total_rx_bytes: AtomicU64::new(0),
            total_tx_bytes: AtomicU64::new(0),
        }
    }

    /// Check if DPDK is available on this system
    pub fn check_available() -> DpdkAvailability {
        let reqs = check_requirements();

        DpdkAvailability {
            supported: reqs.is_supported(),
            huge_pages: reqs.huge_pages_available,
            vfio: reqs.vfio_available,
            uio: reqs.uio_available,
            permissions: reqs.is_root || reqs.has_net_admin,
            iommu: reqs.iommu_enabled,
            compatible_nics: reqs.compatible_nics,
            issues: reqs.unmet_requirements(),
        }
    }

    /// Initialize the DPDK data plane
    pub fn init(&self) -> DpdkResult<()> {
        if self.running.load(Ordering::SeqCst) {
            return Ok(());
        }

        info!("Initializing DPDK data plane");

        // Initialize EAL
        let eal = Eal::init(self.config.eal.clone())?;
        *self.eal.write() = Some(eal);

        // Create memory pool
        let pool = Arc::new(MbufPool::new("pktmbuf_pool", self.config.mempool.clone())?);
        *self.mbuf_pool.write() = Some(pool.clone());

        // Create and configure ports
        let mut ports = Vec::new();
        for port_config in &self.config.ports {
            let mut port = DpdkPort::new(port_config.port_id, port_config.clone(), pool.clone())?;
            port.configure()?;
            ports.push(port);
        }
        *self.ports.write() = ports;

        info!("DPDK data plane initialized");
        Ok(())
    }

    /// Start the data plane (start all ports)
    pub fn start(&self) -> DpdkResult<()> {
        if self.running.load(Ordering::SeqCst) {
            return Ok(());
        }

        info!("Starting DPDK data plane");

        // Start all ports
        for port in self.ports.write().iter() {
            port.start()?;

            if self.config.promiscuous {
                port.set_promiscuous(true)?;
            }
        }

        self.running.store(true, Ordering::SeqCst);

        info!(
            port_count = self.ports.read().len(),
            "DPDK data plane started"
        );

        Ok(())
    }

    /// Stop the data plane
    pub fn stop(&self) -> DpdkResult<()> {
        if !self.running.load(Ordering::SeqCst) {
            return Ok(());
        }

        info!("Stopping DPDK data plane");

        // Stop all ports
        for port in self.ports.write().iter() {
            port.stop()?;
        }

        self.running.store(false, Ordering::SeqCst);

        info!("DPDK data plane stopped");
        Ok(())
    }

    /// Cleanup all resources
    pub fn cleanup(&self) -> DpdkResult<()> {
        self.stop()?;

        // Clear ports
        self.ports.write().clear();

        // Clear pool
        *self.mbuf_pool.write() = None;

        // Cleanup EAL
        if let Some(mut eal) = self.eal.write().take() {
            eal.cleanup()?;
        }

        info!("DPDK data plane cleaned up");
        Ok(())
    }

    /// Check if data plane is running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Receive packets from a port/queue
    pub fn rx_burst(&self, port_id: u16, queue_id: u16, max_pkts: u16) -> DpdkResult<Vec<Mbuf>> {
        let ports = self.ports.read();
        let port = ports
            .iter()
            .find(|p| p.port_id() == port_id)
            .ok_or(DpdkError::PortNotFound { port_id })?;

        let mbufs = port.rx_burst(queue_id, max_pkts)?;

        // Update global stats
        let count = mbufs.len() as u64;
        let bytes: u64 = mbufs.iter().map(|m| m.data_len as u64).sum();
        self.total_rx_packets.fetch_add(count, Ordering::Relaxed);
        self.total_rx_bytes.fetch_add(bytes, Ordering::Relaxed);

        Ok(mbufs)
    }

    /// Transmit packets through a port/queue
    pub fn tx_burst(&self, port_id: u16, queue_id: u16, mbufs: &[Mbuf]) -> DpdkResult<u16> {
        let ports = self.ports.read();
        let port = ports
            .iter()
            .find(|p| p.port_id() == port_id)
            .ok_or(DpdkError::PortNotFound { port_id })?;

        let sent = port.tx_burst(queue_id, mbufs)?;

        // Update global stats
        let bytes: u64 = mbufs[..sent as usize]
            .iter()
            .map(|m| m.data_len as u64)
            .sum();
        self.total_tx_packets
            .fetch_add(sent as u64, Ordering::Relaxed);
        self.total_tx_bytes.fetch_add(bytes, Ordering::Relaxed);

        Ok(sent)
    }

    /// Allocate mbufs from the pool
    pub fn alloc_mbufs(&self, count: usize) -> DpdkResult<Vec<Mbuf>> {
        let pool = self
            .mbuf_pool
            .read()
            .as_ref()
            .cloned()
            .ok_or_else(|| DpdkError::Memory("Pool not initialized".to_string()))?;

        Ok(pool.alloc_bulk(count))
    }

    /// Free mbufs back to the pool
    pub fn free_mbufs(&self, mbufs: Vec<Mbuf>) -> DpdkResult<()> {
        let pool = self
            .mbuf_pool
            .read()
            .as_ref()
            .cloned()
            .ok_or_else(|| DpdkError::Memory("Pool not initialized".to_string()))?;

        pool.free_bulk(mbufs);
        Ok(())
    }

    /// Get number of configured ports
    pub fn port_count(&self) -> usize {
        self.ports.read().len()
    }

    /// Get port IDs
    pub fn port_ids(&self) -> Vec<u16> {
        self.ports.read().iter().map(|p| p.port_id()).collect()
    }

    /// Get port link status
    pub fn port_link_status(&self, port_id: u16) -> Option<LinkStatus> {
        self.ports
            .read()
            .iter()
            .find(|p| p.port_id() == port_id)
            .map(|p| p.state().link_status)
    }

    /// Get data plane statistics
    pub fn stats(&self) -> DpdkDataPlaneStats {
        let ports = self.ports.read();
        let port_stats: Vec<_> = ports.iter().map(|p| p.stats()).collect();

        let pool_stats = self
            .mbuf_pool
            .read()
            .as_ref()
            .map(|p| p.stats())
            .unwrap_or_default();

        DpdkDataPlaneStats {
            running: self.running.load(Ordering::Relaxed),
            port_count: ports.len(),
            total_rx_packets: self.total_rx_packets.load(Ordering::Relaxed),
            total_tx_packets: self.total_tx_packets.load(Ordering::Relaxed),
            total_rx_bytes: self.total_rx_bytes.load(Ordering::Relaxed),
            total_tx_bytes: self.total_tx_bytes.load(Ordering::Relaxed),
            port_stats,
            pool_stats,
        }
    }
}

impl Drop for DpdkDataPlane {
    fn drop(&mut self) {
        if self.running.load(Ordering::SeqCst) {
            if let Err(e) = self.cleanup() {
                error!(error = %e, "Failed to cleanup DPDK data plane");
            }
        }
    }
}

/// DPDK availability check result
#[derive(Debug, Clone)]
pub struct DpdkAvailability {
    /// Whether DPDK is fully supported
    pub supported: bool,
    /// Huge pages available
    pub huge_pages: bool,
    /// VFIO driver available
    pub vfio: bool,
    /// UIO driver available
    pub uio: bool,
    /// Sufficient permissions
    pub permissions: bool,
    /// IOMMU enabled
    pub iommu: bool,
    /// Number of compatible NICs
    pub compatible_nics: usize,
    /// Issues preventing use
    pub issues: Vec<String>,
}

/// DPDK data plane statistics
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct DpdkDataPlaneStats {
    /// Whether data plane is running
    pub running: bool,
    /// Number of ports
    pub port_count: usize,
    /// Total RX packets
    pub total_rx_packets: u64,
    /// Total TX packets
    pub total_tx_packets: u64,
    /// Total RX bytes
    pub total_rx_bytes: u64,
    /// Total TX bytes
    pub total_tx_bytes: u64,
    /// Per-port statistics
    pub port_stats: Vec<PortStatsSnapshot>,
    /// Memory pool statistics
    pub pool_stats: MbufPoolStatsSnapshot,
}

/// Builder for DPDK data plane
pub struct DpdkDataPlaneBuilder {
    config: DpdkConfig,
}

impl DpdkDataPlaneBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            config: DpdkConfig::default(),
        }
    }

    /// Set Kafka port to process
    pub fn kafka_port(mut self, port: u16) -> Self {
        self.config.kafka_port = port;
        self
    }

    /// Add a port configuration
    pub fn add_port(mut self, port_config: DpdkPortConfig) -> Self {
        self.config.ports.push(port_config);
        self
    }

    /// Set CPU cores
    pub fn cores(mut self, cores: &str) -> Self {
        self.config.eal.cores = cores.to_string();
        self
    }

    /// Set huge page directory
    pub fn huge_dir(mut self, dir: &str) -> Self {
        self.config.eal.huge_dir = dir.to_string();
        self
    }

    /// Add PCI device to whitelist
    pub fn pci_device(mut self, pci_addr: &str) -> Self {
        self.config.eal.pci_whitelist.push(pci_addr.to_string());
        self
    }

    /// Enable VFIO mode
    pub fn vfio(mut self, enable: bool) -> Self {
        self.config.eal.vfio_enabled = enable;
        self
    }

    /// Set mbuf count
    pub fn mbuf_count(mut self, count: u32) -> Self {
        self.config.mempool.mbuf_count = count;
        self
    }

    /// Enable promiscuous mode
    pub fn promiscuous(mut self, enable: bool) -> Self {
        self.config.promiscuous = enable;
        self
    }

    /// Enable checksum offload
    pub fn checksum_offload(mut self, enable: bool) -> Self {
        self.config.checksum_offload = enable;
        self
    }

    /// Build the data plane
    pub fn build(self) -> DpdkDataPlane {
        DpdkDataPlane::new(self.config)
    }
}

impl Default for DpdkDataPlaneBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// DPDK lcore (logical core) worker
pub struct DpdkWorker {
    /// Core ID
    core_id: u32,

    /// Assigned port ID
    port_id: u16,

    /// Assigned RX queue ID
    rx_queue_id: u16,

    /// Assigned TX queue ID
    tx_queue_id: u16,

    /// Reference to data plane
    data_plane: Arc<DpdkDataPlane>,

    /// Running flag
    running: AtomicBool,

    /// Packets processed
    packets_processed: AtomicU64,
}

impl DpdkWorker {
    /// Create a new worker
    pub fn new(
        core_id: u32,
        port_id: u16,
        rx_queue_id: u16,
        tx_queue_id: u16,
        data_plane: Arc<DpdkDataPlane>,
    ) -> Self {
        Self {
            core_id,
            port_id,
            rx_queue_id,
            tx_queue_id,
            data_plane,
            running: AtomicBool::new(false),
            packets_processed: AtomicU64::new(0),
        }
    }

    /// Run the worker loop
    pub fn run<F>(&self, mut handler: F) -> DpdkResult<()>
    where
        F: FnMut(&[Mbuf]) -> Vec<Mbuf>,
    {
        self.running.store(true, Ordering::SeqCst);

        info!(
            core_id = self.core_id,
            port_id = self.port_id,
            "Starting DPDK worker"
        );

        while self.running.load(Ordering::SeqCst) {
            // Receive packets
            let rx_mbufs = self
                .data_plane
                .rx_burst(self.port_id, self.rx_queue_id, 32)?;

            if !rx_mbufs.is_empty() {
                // Process packets
                let tx_mbufs = handler(&rx_mbufs);

                self.packets_processed
                    .fetch_add(rx_mbufs.len() as u64, Ordering::Relaxed);

                // Transmit responses
                if !tx_mbufs.is_empty() {
                    let _sent =
                        self.data_plane
                            .tx_burst(self.port_id, self.tx_queue_id, &tx_mbufs)?;
                }

                // Free received mbufs
                self.data_plane.free_mbufs(rx_mbufs)?;
            }
        }

        info!(core_id = self.core_id, "DPDK worker stopped");
        Ok(())
    }

    /// Stop the worker
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    /// Get packets processed
    pub fn packets_processed(&self) -> u64 {
        self.packets_processed.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_availability_check() {
        let avail = DpdkDataPlane::check_available();
        // Just verify it runs without panicking
        let _ = avail.supported;
        let _ = avail.issues;
    }

    #[test]
    fn test_builder() {
        let dp = DpdkDataPlaneBuilder::new()
            .kafka_port(9092)
            .cores("0-3")
            .mbuf_count(4096)
            .promiscuous(true)
            .build();

        assert!(!dp.is_running());
        assert_eq!(dp.port_count(), 0); // No ports added
    }

    #[test]
    fn test_stats_default() {
        let stats = DpdkDataPlaneStats::default();
        assert!(!stats.running);
        assert_eq!(stats.total_rx_packets, 0);
    }
}
