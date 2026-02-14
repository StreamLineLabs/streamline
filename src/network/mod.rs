//! Network module for high-performance networking
//!
//! This module provides advanced networking capabilities including:
//! - XDP (eXpress Data Path) for kernel-bypass packet processing
//! - AF_XDP sockets for zero-copy packet reception
//! - DPDK (Data Plane Development Kit) for userspace networking
//! - Hardware acceleration for sub-millisecond latencies
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                     Network Stack                                │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  Standard Path (Tokio TCP)                                       │
//! │  └── Good for compatibility, reasonable performance             │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  XDP Path (Kernel Bypass)                                        │
//! │  └── XDP program in NIC driver → AF_XDP socket → userspace      │
//! │  └── Sub-millisecond latencies, zero kernel copies               │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  DPDK Path (Full Userspace)                                      │
//! │  └── PMD driver → DPDK mbuf pool → userspace                     │
//! │  └── Complete kernel bypass, highest throughput                  │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Platform Support
//!
//! - XDP: Linux 4.18+ with XDP-capable NIC driver
//! - AF_XDP: Linux 4.18+ with UMEM support
//! - DPDK: Linux with huge pages and DPDK-compatible NIC
//! - Fallback: Standard Tokio TCP on unsupported platforms

#[cfg(target_os = "linux")]
pub mod xdp;

#[cfg(target_os = "linux")]
pub mod dpdk;

#[cfg(target_os = "linux")]
pub mod rdma;

#[cfg(target_os = "linux")]
pub use xdp::*;

/// Network backend selection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetworkBackend {
    /// Standard Tokio TCP (default, all platforms)
    Standard,
    /// XDP with AF_XDP sockets (Linux only, requires capable NIC)
    Xdp,
    /// DPDK userspace networking (future)
    Dpdk,
    /// RDMA for inter-broker communication (future)
    Rdma,
}

impl Default for NetworkBackend {
    fn default() -> Self {
        Self::Standard
    }
}

impl std::fmt::Display for NetworkBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Standard => write!(f, "standard"),
            Self::Xdp => write!(f, "xdp"),
            Self::Dpdk => write!(f, "dpdk"),
            Self::Rdma => write!(f, "rdma"),
        }
    }
}

/// Check if XDP is available on this system
#[cfg(target_os = "linux")]
pub fn is_xdp_available() -> bool {
    xdp::XdpCapabilities::detect().is_supported()
}

#[cfg(not(target_os = "linux"))]
pub fn is_xdp_available() -> bool {
    false
}

/// Check if DPDK is available on this system
#[cfg(target_os = "linux")]
pub fn is_dpdk_available() -> bool {
    dpdk::DpdkDataPlane::check_available().supported
}

#[cfg(not(target_os = "linux"))]
pub fn is_dpdk_available() -> bool {
    false
}

/// Check if RDMA is available on this system
#[cfg(target_os = "linux")]
pub fn is_rdma_available() -> bool {
    rdma::transport::is_rdma_available()
}

#[cfg(not(target_os = "linux"))]
pub fn is_rdma_available() -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_network_backend_default() {
        assert_eq!(NetworkBackend::default(), NetworkBackend::Standard);
    }

    #[test]
    fn test_network_backend_display() {
        assert_eq!(format!("{}", NetworkBackend::Standard), "standard");
        assert_eq!(format!("{}", NetworkBackend::Xdp), "xdp");
    }
}
