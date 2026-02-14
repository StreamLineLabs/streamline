//! RDMA (Remote Direct Memory Access) support for ultra-low latency networking
//!
//! This module provides RDMA-based networking for inter-broker communication,
//! enabling sub-microsecond latencies through kernel bypass and zero-copy transfers.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────┐
//! │                         RDMA Data Plane                                      │
//! ├─────────────────────────────────────────────────────────────────────────────┤
//! │                                                                              │
//! │  ┌─────────────────────┐     ┌─────────────────────┐                        │
//! │  │   Broker A          │     │   Broker B          │                        │
//! │  │  ┌───────────────┐  │     │  ┌───────────────┐  │                        │
//! │  │  │ RDMA Context  │  │     │  │ RDMA Context  │  │                        │
//! │  │  │ ┌───────────┐ │  │     │  │ ┌───────────┐ │  │                        │
//! │  │  │ │ Queue Pair│◄├──┼─────┼──┼─┤ Queue Pair│ │  │                        │
//! │  │  │ └───────────┘ │  │     │  │ └───────────┘ │  │                        │
//! │  │  │ ┌───────────┐ │  │     │  │ ┌───────────┐ │  │                        │
//! │  │  │ │Memory Reg.│ │  │     │  │ │Memory Reg.│ │  │                        │
//! │  │  │ └───────────┘ │  │     │  │ └───────────┘ │  │                        │
//! │  │  └───────────────┘  │     │  └───────────────┘  │                        │
//! │  └─────────────────────┘     └─────────────────────┘                        │
//! │           │                           │                                      │
//! │           ▼                           ▼                                      │
//! │  ┌─────────────────────────────────────────────────┐                        │
//! │  │             RDMA NIC (InfiniBand/RoCE)          │                        │
//! │  │    Hardware-based zero-copy DMA transfers       │                        │
//! │  └─────────────────────────────────────────────────┘                        │
//! │                                                                              │
//! └─────────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Features
//!
//! - **Zero-copy**: DMA directly to/from application memory
//! - **Kernel bypass**: No kernel involvement in data path
//! - **Sub-μs latency**: Typical round-trip times < 2 microseconds
//! - **High bandwidth**: 100+ Gbps with InfiniBand HDR
//! - **CPU offload**: NIC handles protocol processing
//!
//! # Supported Transports
//!
//! - InfiniBand (IB)
//! - RoCE v2 (RDMA over Converged Ethernet)
//! - iWARP (Internet Wide Area RDMA Protocol)
//!
//! # Platform Support
//!
//! - Linux with RDMA-capable NIC and rdma-core userspace libraries
//! - Requires appropriate drivers (mlx4, mlx5 for Mellanox)

pub mod config;
pub mod connection;
pub mod device;
pub mod memory;
pub mod transport;

pub use config::*;
pub use connection::*;
pub use device::*;
pub use memory::*;
pub use transport::*;

use thiserror::Error;

/// RDMA errors
#[derive(Debug, Error)]
pub enum RdmaError {
    #[error("RDMA not available: {0}")]
    NotAvailable(String),

    #[error("Device error: {0}")]
    DeviceError(String),

    #[error("Memory registration error: {0}")]
    MemoryError(String),

    #[error("Connection error: {0}")]
    ConnectionError(String),

    #[error("Queue pair error: {0}")]
    QueuePairError(String),

    #[error("Completion error: {0}")]
    CompletionError(String),

    #[error("Send error: {0}")]
    SendError(String),

    #[error("Receive error: {0}")]
    ReceiveError(String),

    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Address resolution error: {0}")]
    AddressError(String),

    #[error("Timeout: {0}")]
    Timeout(String),

    #[error("Internal error: {0}")]
    InternalError(String),
}

/// Result type for RDMA operations
pub type RdmaResult<T> = Result<T, RdmaError>;

/// RDMA transport type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RdmaTransport {
    /// InfiniBand
    InfiniBand,
    /// RDMA over Converged Ethernet (RoCE v2)
    #[default]
    RoCEv2,
    /// Internet Wide Area RDMA Protocol
    IWarp,
}

impl std::fmt::Display for RdmaTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InfiniBand => write!(f, "InfiniBand"),
            Self::RoCEv2 => write!(f, "RoCE v2"),
            Self::IWarp => write!(f, "iWARP"),
        }
    }
}

/// RDMA link type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RdmaLinkType {
    /// InfiniBand link
    InfiniBand,
    /// Ethernet link (for RoCE/iWARP)
    Ethernet,
    /// Unknown link type
    Unknown,
}

/// RDMA port state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RdmaPortState {
    /// Port is down
    Down,
    /// Port is initializing
    Initializing,
    /// Port is armed (ready for transition)
    Armed,
    /// Port is active and ready
    Active,
    /// Port is in active deferred state
    ActiveDefer,
}

impl Default for RdmaPortState {
    fn default() -> Self {
        Self::Down
    }
}

/// RDMA MTU size
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RdmaMtu {
    /// 256 bytes
    Mtu256,
    /// 512 bytes
    Mtu512,
    /// 1024 bytes
    Mtu1024,
    /// 2048 bytes
    Mtu2048,
    /// 4096 bytes (default for IB)
    Mtu4096,
}

impl Default for RdmaMtu {
    fn default() -> Self {
        Self::Mtu4096
    }
}

impl RdmaMtu {
    /// Get MTU size in bytes
    pub fn bytes(&self) -> usize {
        match self {
            Self::Mtu256 => 256,
            Self::Mtu512 => 512,
            Self::Mtu1024 => 1024,
            Self::Mtu2048 => 2048,
            Self::Mtu4096 => 4096,
        }
    }
}

/// Queue pair type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueuePairType {
    /// Reliable Connection (most common)
    RC,
    /// Unreliable Connection
    UC,
    /// Unreliable Datagram
    UD,
    /// Extended Reliable Connection (for RDMA CM)
    XRC,
}

impl Default for QueuePairType {
    fn default() -> Self {
        Self::RC
    }
}

impl std::fmt::Display for QueuePairType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RC => write!(f, "RC (Reliable Connection)"),
            Self::UC => write!(f, "UC (Unreliable Connection)"),
            Self::UD => write!(f, "UD (Unreliable Datagram)"),
            Self::XRC => write!(f, "XRC (Extended RC)"),
        }
    }
}

/// Access flags for memory regions
#[derive(Debug, Clone, Copy, Default)]
pub struct AccessFlags {
    /// Allow local writes
    pub local_write: bool,
    /// Allow remote writes
    pub remote_write: bool,
    /// Allow remote reads
    pub remote_read: bool,
    /// Allow remote atomic operations
    pub remote_atomic: bool,
    /// Allow memory window binding
    pub mw_bind: bool,
}

impl AccessFlags {
    /// Create flags for local access only
    pub fn local_only() -> Self {
        Self {
            local_write: true,
            ..Default::default()
        }
    }

    /// Create flags for full remote access
    pub fn remote_full() -> Self {
        Self {
            local_write: true,
            remote_write: true,
            remote_read: true,
            remote_atomic: true,
            mw_bind: false,
        }
    }

    /// Create flags for remote read-only
    pub fn remote_read_only() -> Self {
        Self {
            local_write: true,
            remote_read: true,
            ..Default::default()
        }
    }
}

/// Work request operation type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkRequestOp {
    /// RDMA Send
    Send,
    /// RDMA Send with immediate data
    SendWithImm,
    /// RDMA Write
    Write,
    /// RDMA Write with immediate data
    WriteWithImm,
    /// RDMA Read
    Read,
    /// Atomic Compare and Swap
    AtomicCmpSwp,
    /// Atomic Fetch and Add
    AtomicFetchAdd,
    /// Local Invalidate
    LocalInv,
    /// Bind Memory Window
    BindMw,
}

/// Work completion status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkCompletionStatus {
    /// Success
    Success,
    /// Local length error
    LocalLengthError,
    /// Local QP operation error
    LocalQpOpError,
    /// Local EE context operation error
    LocalEeContextOpError,
    /// Local protection error
    LocalProtectionError,
    /// Work request flushed
    WrFlushError,
    /// Memory window bind error
    MwBindError,
    /// Bad response error
    BadResponseError,
    /// Local access error
    LocalAccessError,
    /// Remote invalid request error
    RemoteInvalidRequestError,
    /// Remote access error
    RemoteAccessError,
    /// Remote operation error
    RemoteOperationError,
    /// Retry exceeded
    RetryExceeded,
    /// RNR retry exceeded
    RnrRetryExceeded,
    /// Local RDD violation error
    LocalRddViolationError,
    /// Remote invalid RD request
    RemoteInvalidRdRequest,
    /// Remote aborted error
    RemoteAbortedError,
    /// Invalid EE context number
    InvalidEeContextNum,
    /// Invalid EE context state
    InvalidEeContextState,
    /// Fatal error
    FatalError,
    /// Response timeout error
    ResponseTimeoutError,
    /// General error
    GeneralError,
}

impl WorkCompletionStatus {
    /// Check if status is success
    pub fn is_success(&self) -> bool {
        matches!(self, Self::Success)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rdma_transport_display() {
        assert_eq!(format!("{}", RdmaTransport::InfiniBand), "InfiniBand");
        assert_eq!(format!("{}", RdmaTransport::RoCEv2), "RoCE v2");
        assert_eq!(format!("{}", RdmaTransport::IWarp), "iWARP");
    }

    #[test]
    fn test_rdma_mtu() {
        assert_eq!(RdmaMtu::Mtu256.bytes(), 256);
        assert_eq!(RdmaMtu::Mtu4096.bytes(), 4096);
    }

    #[test]
    fn test_access_flags() {
        let local = AccessFlags::local_only();
        assert!(local.local_write);
        assert!(!local.remote_write);

        let remote = AccessFlags::remote_full();
        assert!(remote.local_write);
        assert!(remote.remote_write);
        assert!(remote.remote_read);
        assert!(remote.remote_atomic);
    }

    #[test]
    fn test_work_completion_status() {
        assert!(WorkCompletionStatus::Success.is_success());
        assert!(!WorkCompletionStatus::FatalError.is_success());
    }
}
