//! XDP (eXpress Data Path) support for kernel-bypass networking
//!
//! This module provides XDP-based packet processing for achieving
//! sub-millisecond latencies in the Kafka protocol data path.
//!
//! # Overview
//!
//! XDP runs BPF programs at the earliest point in the network stack,
//! directly in the NIC driver, before any kernel networking code.
//! Combined with AF_XDP sockets, this enables zero-copy packet reception.
//!
//! # Components
//!
//! - [`XdpConfig`]: Configuration for XDP programs and AF_XDP sockets
//! - [`XdpCapabilities`]: Detection of system XDP capabilities
//! - [`XdpProgram`]: Loader and manager for XDP BPF programs
//! - [`AfXdpSocket`]: AF_XDP socket for zero-copy packet I/O
//! - [`XdpFilter`]: Packet filtering rules for Kafka protocol
//! - [`XdpDataPlane`]: High-level data plane combining all components
//!
//! # Requirements
//!
//! - Linux kernel 4.18+ (5.x+ recommended)
//! - XDP-capable NIC driver (Intel, Mellanox, AWS ENA, etc.)
//! - CAP_NET_ADMIN and CAP_SYS_ADMIN capabilities
//! - Optional: XDP native mode support in driver
//!
//! # Example
//!
//! ```ignore
//! use streamline::network::xdp::{XdpConfig, XdpDataPlane};
//!
//! let config = XdpConfig {
//!     interface: "eth0".to_string(),
//!     port: 9092,
//!     ..Default::default()
//! };
//!
//! let data_plane = XdpDataPlane::new(config)?;
//! data_plane.start().await?;
//! ```

mod capabilities;
mod config;
mod data_plane;
mod filter;
mod program;
mod socket;

pub use capabilities::*;
pub use config::*;
pub use data_plane::*;
pub use filter::*;
pub use program::*;
pub use socket::*;

use std::io;

/// XDP error types
#[derive(Debug, thiserror::Error)]
pub enum XdpError {
    #[error("XDP not supported on this system: {0}")]
    NotSupported(String),

    #[error("Failed to load XDP program: {0}")]
    ProgramLoad(String),

    #[error("Failed to attach XDP program to interface {interface}: {reason}")]
    ProgramAttach { interface: String, reason: String },

    #[error("Failed to detach XDP program from interface {interface}: {reason}")]
    ProgramDetach { interface: String, reason: String },

    #[error("AF_XDP socket error: {0}")]
    Socket(String),

    #[error("UMEM allocation error: {0}")]
    Umem(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Permission denied: {0}")]
    Permission(String),

    #[error("Interface {0} not found")]
    InterfaceNotFound(String),

    #[error("IO error: {0}")]
    Io(#[from] io::Error),
}

pub type XdpResult<T> = Result<T, XdpError>;

/// XDP program attach mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum XdpMode {
    /// SKB mode (software fallback, works on all interfaces)
    #[default]
    Skb,
    /// Native mode (driver support required, best performance)
    Native,
    /// Hardware offload mode (NIC support required, highest performance)
    Offload,
}

impl std::fmt::Display for XdpMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Skb => write!(f, "skb"),
            Self::Native => write!(f, "native"),
            Self::Offload => write!(f, "offload"),
        }
    }
}

/// XDP action returned by BPF program
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum XdpAction {
    /// Drop the packet
    Drop = 1,
    /// Pass to normal network stack
    Pass = 2,
    /// Send back out the same interface (TX)
    Tx = 3,
    /// Redirect to another interface or AF_XDP socket
    Redirect = 4,
    /// Error processing - drop
    Aborted = 0,
}

impl From<u32> for XdpAction {
    fn from(value: u32) -> Self {
        match value {
            1 => Self::Drop,
            2 => Self::Pass,
            3 => Self::Tx,
            4 => Self::Redirect,
            _ => Self::Aborted,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_xdp_mode_display() {
        assert_eq!(format!("{}", XdpMode::Skb), "skb");
        assert_eq!(format!("{}", XdpMode::Native), "native");
        assert_eq!(format!("{}", XdpMode::Offload), "offload");
    }

    #[test]
    fn test_xdp_action_from_u32() {
        assert_eq!(XdpAction::from(1), XdpAction::Drop);
        assert_eq!(XdpAction::from(2), XdpAction::Pass);
        assert_eq!(XdpAction::from(3), XdpAction::Tx);
        assert_eq!(XdpAction::from(4), XdpAction::Redirect);
        assert_eq!(XdpAction::from(99), XdpAction::Aborted);
    }

    #[test]
    fn test_xdp_mode_default() {
        assert_eq!(XdpMode::default(), XdpMode::Skb);
    }
}
