//! DPDK (Data Plane Development Kit) support for userspace networking
//!
//! This module provides DPDK-based networking for maximum throughput
//! and minimal latency by bypassing the kernel network stack entirely.
//!
//! # Overview
//!
//! DPDK operates entirely in userspace, taking direct control of NICs
//! through UIO (Userspace I/O) or VFIO drivers. This eliminates kernel
//! context switches and enables features like:
//!
//! - Zero-copy packet processing
//! - Huge page memory for DMA efficiency
//! - Per-core packet processing (no locks)
//! - Hardware RSS for multi-queue distribution
//!
//! # Requirements
//!
//! - Linux with UIO or VFIO driver support
//! - Huge pages enabled (at least 1GB recommended)
//! - DPDK-compatible NIC (Intel, Mellanox, AWS ENA, etc.)
//! - Root or CAP_NET_ADMIN + CAP_SYS_RAWIO capabilities
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                      Application (Streamline)                    │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  DpdkDataPlane                                                   │
//! │  ├── DpdkConfig (port, queue, memory config)                     │
//! │  ├── DpdkPort (per-NIC abstraction)                              │
//! │  │   ├── RxQueue[] (receive rings)                               │
//! │  │   └── TxQueue[] (transmit rings)                              │
//! │  ├── MbufPool (packet buffer memory pool)                        │
//! │  └── DpdkFilter (protocol filtering)                             │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  EAL (Environment Abstraction Layer)                             │
//! │  ├── Core management (CPU affinity)                              │
//! │  ├── Memory management (huge pages)                              │
//! │  └── PCI device binding                                          │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  PMD (Poll Mode Driver) - NIC Drivers                            │
//! │  └── Direct DMA to/from NIC rings                                │
//! └─────────────────────────────────────────────────────────────────┘
//! ```

mod config;
mod data_plane;
mod eal;
mod mbuf;
mod port;

pub use config::*;
pub use data_plane::*;
pub use eal::*;
pub use mbuf::*;
pub use port::*;

use std::io;

/// DPDK error types
#[derive(Debug, thiserror::Error)]
pub enum DpdkError {
    #[error("DPDK not supported on this system: {0}")]
    NotSupported(String),

    #[error("EAL initialization failed: {0}")]
    EalInit(String),

    #[error("Port {port_id} configuration failed: {reason}")]
    PortConfig { port_id: u16, reason: String },

    #[error("Port {port_id} not found")]
    PortNotFound { port_id: u16 },

    #[error("Queue {queue_id} configuration failed on port {port_id}: {reason}")]
    QueueConfig {
        port_id: u16,
        queue_id: u16,
        reason: String,
    },

    #[error("Memory allocation failed: {0}")]
    Memory(String),

    #[error("Packet buffer allocation failed")]
    MbufAlloc,

    #[error("Invalid configuration: {0}")]
    Config(String),

    #[error("Permission denied: {0}")]
    Permission(String),

    #[error("Device binding failed: {0}")]
    DeviceBinding(String),

    #[error("IO error: {0}")]
    Io(#[from] io::Error),
}

pub type DpdkResult<T> = Result<T, DpdkError>;

/// DPDK link speed
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkSpeed {
    /// 1 Gbps
    Speed1G,
    /// 10 Gbps
    Speed10G,
    /// 25 Gbps
    Speed25G,
    /// 40 Gbps
    Speed40G,
    /// 100 Gbps
    Speed100G,
    /// Unknown speed
    Unknown,
}

impl std::fmt::Display for LinkSpeed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Speed1G => write!(f, "1G"),
            Self::Speed10G => write!(f, "10G"),
            Self::Speed25G => write!(f, "25G"),
            Self::Speed40G => write!(f, "40G"),
            Self::Speed100G => write!(f, "100G"),
            Self::Unknown => write!(f, "unknown"),
        }
    }
}

/// DPDK link status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkStatus {
    /// Link is up
    Up,
    /// Link is down
    Down,
}

/// DPDK RSS (Receive Side Scaling) hash types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u64)]
pub enum RssHashType {
    /// Hash on IPv4 addresses
    Ipv4 = 0x01,
    /// Hash on IPv4 + TCP ports
    Ipv4Tcp = 0x02,
    /// Hash on IPv4 + UDP ports
    Ipv4Udp = 0x04,
    /// Hash on IPv6 addresses
    Ipv6 = 0x08,
    /// Hash on IPv6 + TCP ports
    Ipv6Tcp = 0x10,
    /// Hash on IPv6 + UDP ports
    Ipv6Udp = 0x20,
}

impl RssHashType {
    /// Get RSS hash mask for Kafka-optimized distribution
    pub fn kafka_optimized() -> u64 {
        // TCP/IP hashing for Kafka connections
        Self::Ipv4Tcp as u64 | Self::Ipv6Tcp as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_link_speed_display() {
        assert_eq!(format!("{}", LinkSpeed::Speed10G), "10G");
        assert_eq!(format!("{}", LinkSpeed::Speed100G), "100G");
    }

    #[test]
    fn test_rss_hash_kafka() {
        let mask = RssHashType::kafka_optimized();
        assert!(mask & (RssHashType::Ipv4Tcp as u64) != 0);
        assert!(mask & (RssHashType::Ipv6Tcp as u64) != 0);
    }
}
