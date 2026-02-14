//! DPDK port (NIC) management

use super::{
    DpdkError, DpdkPortConfig, DpdkResult, LinkSpeed, LinkStatus, Mbuf, MbufPool, RssHashType,
};
use parking_lot::RwLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{debug, error, info, warn};

/// DPDK Ethernet port
///
/// Represents a network interface managed by DPDK.
/// Each port has multiple RX/TX queues for parallel processing.
pub struct DpdkPort {
    /// Port ID
    port_id: u16,

    /// Configuration
    config: DpdkPortConfig,

    /// Memory pool for packet buffers
    mbuf_pool: Arc<MbufPool>,

    /// RX queues
    rx_queues: Vec<RxQueue>,

    /// TX queues
    tx_queues: Vec<TxQueue>,

    /// Port state
    state: RwLock<PortState>,

    /// Statistics
    stats: PortStats,

    /// Whether port is started
    started: AtomicBool,
}

/// Port state
#[derive(Debug, Clone)]
pub struct PortState {
    /// Link status
    pub link_status: LinkStatus,

    /// Link speed
    pub link_speed: LinkSpeed,

    /// Full duplex
    pub full_duplex: bool,

    /// Autoneg enabled
    pub autoneg: bool,

    /// Promiscuous mode
    pub promiscuous: bool,

    /// MAC address
    pub mac_addr: [u8; 6],
}

impl Default for PortState {
    fn default() -> Self {
        Self {
            link_status: LinkStatus::Down,
            link_speed: LinkSpeed::Unknown,
            full_duplex: true,
            autoneg: true,
            promiscuous: false,
            mac_addr: [0; 6],
        }
    }
}

/// RX queue
pub struct RxQueue {
    /// Queue ID
    queue_id: u16,

    /// Port ID
    port_id: u16,

    /// Ring size
    ring_size: u16,

    /// CPU core affinity (if set)
    core_affinity: Option<u32>,

    /// Statistics
    stats: QueueStats,
}

/// TX queue
pub struct TxQueue {
    /// Queue ID
    queue_id: u16,

    /// Port ID
    port_id: u16,

    /// Ring size
    ring_size: u16,

    /// Statistics
    stats: QueueStats,
}

/// Queue statistics
#[derive(Debug, Default)]
pub struct QueueStats {
    /// Packets processed
    pub packets: AtomicU64,
    /// Bytes processed
    pub bytes: AtomicU64,
    /// Errors
    pub errors: AtomicU64,
    /// Drops
    pub drops: AtomicU64,
}

/// Port statistics
#[derive(Debug, Default)]
pub struct PortStats {
    /// RX packets
    pub rx_packets: AtomicU64,
    /// RX bytes
    pub rx_bytes: AtomicU64,
    /// TX packets
    pub tx_packets: AtomicU64,
    /// TX bytes
    pub tx_bytes: AtomicU64,
    /// RX errors
    pub rx_errors: AtomicU64,
    /// TX errors
    pub tx_errors: AtomicU64,
    /// RX drops (no buffer)
    pub rx_no_mbuf: AtomicU64,
    /// TX drops (ring full)
    pub tx_dropped: AtomicU64,
}

impl DpdkPort {
    /// Create a new DPDK port
    pub fn new(port_id: u16, config: DpdkPortConfig, mbuf_pool: Arc<MbufPool>) -> DpdkResult<Self> {
        // Validate configuration
        if config.rx_queues == 0 || config.tx_queues == 0 {
            return Err(DpdkError::PortConfig {
                port_id,
                reason: "Must have at least one RX and TX queue".to_string(),
            });
        }

        // Create RX queues
        let mut rx_queues = Vec::with_capacity(config.rx_queues as usize);
        for i in 0..config.rx_queues {
            let core = config.rx_core_affinity.get(i as usize).copied();
            rx_queues.push(RxQueue {
                queue_id: i,
                port_id,
                ring_size: config.rx_ring_size,
                core_affinity: core,
                stats: QueueStats::default(),
            });
        }

        // Create TX queues
        let mut tx_queues = Vec::with_capacity(config.tx_queues as usize);
        for i in 0..config.tx_queues {
            tx_queues.push(TxQueue {
                queue_id: i,
                port_id,
                ring_size: config.tx_ring_size,
                stats: QueueStats::default(),
            });
        }

        debug!(
            port_id = port_id,
            rx_queues = config.rx_queues,
            tx_queues = config.tx_queues,
            "Created DPDK port"
        );

        Ok(Self {
            port_id,
            config,
            mbuf_pool,
            rx_queues,
            tx_queues,
            state: RwLock::new(PortState::default()),
            stats: PortStats::default(),
            started: AtomicBool::new(false),
        })
    }

    /// Configure the port
    pub fn configure(&mut self) -> DpdkResult<()> {
        info!(port_id = self.port_id, "Configuring DPDK port");

        // In production, this would call:
        // - rte_eth_dev_configure()
        // - rte_eth_rx_queue_setup() for each RX queue
        // - rte_eth_tx_queue_setup() for each TX queue

        // Configure RSS if enabled
        if self.config.rss_enabled {
            self.configure_rss()?;
        }

        debug!(port_id = self.port_id, "Port configured");
        Ok(())
    }

    /// Configure RSS (Receive Side Scaling)
    fn configure_rss(&self) -> DpdkResult<()> {
        let hash_types = RssHashType::kafka_optimized();

        debug!(
            port_id = self.port_id,
            hash_types = hash_types,
            "Configuring RSS"
        );

        // In production, this would configure the RSS hash key and types
        Ok(())
    }

    /// Start the port
    pub fn start(&self) -> DpdkResult<()> {
        if self.started.load(Ordering::SeqCst) {
            return Ok(());
        }

        info!(port_id = self.port_id, "Starting DPDK port");

        // In production: rte_eth_dev_start()

        // Update link state
        {
            let mut state = self.state.write();
            state.link_status = LinkStatus::Up;
            state.link_speed = LinkSpeed::Speed10G;
        }

        self.started.store(true, Ordering::SeqCst);

        info!(
            port_id = self.port_id,
            link_speed = %self.state.read().link_speed,
            "Port started"
        );

        Ok(())
    }

    /// Stop the port
    pub fn stop(&self) -> DpdkResult<()> {
        if !self.started.load(Ordering::SeqCst) {
            return Ok(());
        }

        info!(port_id = self.port_id, "Stopping DPDK port");

        // In production: rte_eth_dev_stop()

        {
            let mut state = self.state.write();
            state.link_status = LinkStatus::Down;
        }

        self.started.store(false, Ordering::SeqCst);
        Ok(())
    }

    /// Enable promiscuous mode
    pub fn set_promiscuous(&self, enable: bool) -> DpdkResult<()> {
        debug!(
            port_id = self.port_id,
            enable = enable,
            "Setting promiscuous mode"
        );

        // In production: rte_eth_promiscuous_enable/disable()
        self.state.write().promiscuous = enable;
        Ok(())
    }

    /// Receive packets from a queue
    pub fn rx_burst(&self, queue_id: u16, max_pkts: u16) -> DpdkResult<Vec<Mbuf>> {
        if queue_id >= self.rx_queues.len() as u16 {
            return Err(DpdkError::QueueConfig {
                port_id: self.port_id,
                queue_id,
                reason: "Queue does not exist".to_string(),
            });
        }

        let queue = &self.rx_queues[queue_id as usize];

        // Allocate mbufs from pool
        let mut mbufs = self.mbuf_pool.alloc_bulk(max_pkts as usize);

        // In production: rte_eth_rx_burst()
        // This would fill the mbufs with received packet data

        // Simulate some packets received
        let received = mbufs.len();
        let mut bytes = 0u64;

        for mbuf in &mut mbufs {
            mbuf.port = self.port_id;
            mbuf.queue = queue_id;
            // Simulate a 64-byte packet
            mbuf.append(64);
            bytes += 64;
        }

        // Update statistics
        if received > 0 {
            queue
                .stats
                .packets
                .fetch_add(received as u64, Ordering::Relaxed);
            queue.stats.bytes.fetch_add(bytes, Ordering::Relaxed);
            self.stats
                .rx_packets
                .fetch_add(received as u64, Ordering::Relaxed);
            self.stats.rx_bytes.fetch_add(bytes, Ordering::Relaxed);
        }

        Ok(mbufs)
    }

    /// Transmit packets through a queue
    pub fn tx_burst(&self, queue_id: u16, mbufs: &[Mbuf]) -> DpdkResult<u16> {
        if queue_id >= self.tx_queues.len() as u16 {
            return Err(DpdkError::QueueConfig {
                port_id: self.port_id,
                queue_id,
                reason: "Queue does not exist".to_string(),
            });
        }

        let queue = &self.tx_queues[queue_id as usize];

        // In production: rte_eth_tx_burst()
        let sent = mbufs.len() as u16;
        let bytes: u64 = mbufs.iter().map(|m| m.data_len as u64).sum();

        // Update statistics
        queue
            .stats
            .packets
            .fetch_add(sent as u64, Ordering::Relaxed);
        queue.stats.bytes.fetch_add(bytes, Ordering::Relaxed);
        self.stats
            .tx_packets
            .fetch_add(sent as u64, Ordering::Relaxed);
        self.stats.tx_bytes.fetch_add(bytes, Ordering::Relaxed);

        Ok(sent)
    }

    /// Get port ID
    pub fn port_id(&self) -> u16 {
        self.port_id
    }

    /// Get port state
    pub fn state(&self) -> PortState {
        self.state.read().clone()
    }

    /// Check if port is started
    pub fn is_started(&self) -> bool {
        self.started.load(Ordering::SeqCst)
    }

    /// Get number of RX queues
    pub fn rx_queue_count(&self) -> usize {
        self.rx_queues.len()
    }

    /// Get number of TX queues
    pub fn tx_queue_count(&self) -> usize {
        self.tx_queues.len()
    }

    /// Get statistics snapshot
    pub fn stats(&self) -> PortStatsSnapshot {
        PortStatsSnapshot {
            rx_packets: self.stats.rx_packets.load(Ordering::Relaxed),
            rx_bytes: self.stats.rx_bytes.load(Ordering::Relaxed),
            tx_packets: self.stats.tx_packets.load(Ordering::Relaxed),
            tx_bytes: self.stats.tx_bytes.load(Ordering::Relaxed),
            rx_errors: self.stats.rx_errors.load(Ordering::Relaxed),
            tx_errors: self.stats.tx_errors.load(Ordering::Relaxed),
            rx_no_mbuf: self.stats.rx_no_mbuf.load(Ordering::Relaxed),
            tx_dropped: self.stats.tx_dropped.load(Ordering::Relaxed),
        }
    }

    /// Get per-queue statistics
    pub fn queue_stats(&self) -> (Vec<QueueStatsSnapshot>, Vec<QueueStatsSnapshot>) {
        let rx_stats: Vec<_> = self
            .rx_queues
            .iter()
            .map(|q| QueueStatsSnapshot {
                queue_id: q.queue_id,
                packets: q.stats.packets.load(Ordering::Relaxed),
                bytes: q.stats.bytes.load(Ordering::Relaxed),
                errors: q.stats.errors.load(Ordering::Relaxed),
                drops: q.stats.drops.load(Ordering::Relaxed),
            })
            .collect();

        let tx_stats: Vec<_> = self
            .tx_queues
            .iter()
            .map(|q| QueueStatsSnapshot {
                queue_id: q.queue_id,
                packets: q.stats.packets.load(Ordering::Relaxed),
                bytes: q.stats.bytes.load(Ordering::Relaxed),
                errors: q.stats.errors.load(Ordering::Relaxed),
                drops: q.stats.drops.load(Ordering::Relaxed),
            })
            .collect();

        (rx_stats, tx_stats)
    }
}

/// Snapshot of port statistics
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct PortStatsSnapshot {
    pub rx_packets: u64,
    pub rx_bytes: u64,
    pub tx_packets: u64,
    pub tx_bytes: u64,
    pub rx_errors: u64,
    pub tx_errors: u64,
    pub rx_no_mbuf: u64,
    pub tx_dropped: u64,
}

/// Snapshot of queue statistics
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct QueueStatsSnapshot {
    pub queue_id: u16,
    pub packets: u64,
    pub bytes: u64,
    pub errors: u64,
    pub drops: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::dpdk::MempoolConfig;

    fn create_test_pool() -> Arc<MbufPool> {
        Arc::new(MbufPool::new("test", MempoolConfig::default()).unwrap())
    }

    #[test]
    fn test_port_creation() {
        let pool = create_test_pool();
        let config = DpdkPortConfig::default();
        let port = DpdkPort::new(0, config, pool).unwrap();

        assert_eq!(port.port_id(), 0);
        assert!(!port.is_started());
        assert_eq!(port.rx_queue_count(), 4);
        assert_eq!(port.tx_queue_count(), 4);
    }

    #[test]
    fn test_port_start_stop() {
        let pool = create_test_pool();
        let config = DpdkPortConfig::default();
        let port = DpdkPort::new(0, config, pool).unwrap();

        assert!(!port.is_started());

        port.start().unwrap();
        assert!(port.is_started());
        assert_eq!(port.state().link_status, LinkStatus::Up);

        port.stop().unwrap();
        assert!(!port.is_started());
        assert_eq!(port.state().link_status, LinkStatus::Down);
    }

    #[test]
    fn test_rx_tx_burst() {
        let pool = create_test_pool();
        let config = DpdkPortConfig::default();
        let port = DpdkPort::new(0, config, pool.clone()).unwrap();
        port.start().unwrap();

        // Receive some packets
        let mbufs = port.rx_burst(0, 4).unwrap();
        assert!(!mbufs.is_empty());

        // Transmit them back
        let sent = port.tx_burst(0, &mbufs).unwrap();
        assert!(sent > 0);

        // Check stats
        let stats = port.stats();
        assert!(stats.rx_packets > 0);
        assert!(stats.tx_packets > 0);
    }

    #[test]
    fn test_promiscuous_mode() {
        let pool = create_test_pool();
        let config = DpdkPortConfig::default();
        let port = DpdkPort::new(0, config, pool).unwrap();

        assert!(!port.state().promiscuous);

        port.set_promiscuous(true).unwrap();
        assert!(port.state().promiscuous);

        port.set_promiscuous(false).unwrap();
        assert!(!port.state().promiscuous);
    }
}
