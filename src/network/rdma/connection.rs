//! RDMA connection and queue pair management
//!
//! This module handles the lifecycle of RDMA connections including
//! queue pair creation, state transitions, and connection management.

use super::{
    config::{QueuePairConfig, RdmaConfig},
    device::Gid,
    memory::{LocalKey, MemoryRegion, RemoteKey},
    AccessFlags, QueuePairType, RdmaError, RdmaMtu, RdmaResult,
};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Queue pair number
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct QueuePairNum(pub u32);

/// Queue pair state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueuePairState {
    /// Reset state (initial)
    Reset,
    /// Initialized
    Init,
    /// Ready to Receive
    Rtr,
    /// Ready to Send
    Rts,
    /// Send Queue Drained
    Sqd,
    /// Send Queue Error
    Sqe,
    /// Error state
    Error,
}

impl Default for QueuePairState {
    fn default() -> Self {
        Self::Reset
    }
}

impl std::fmt::Display for QueuePairState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Reset => write!(f, "RESET"),
            Self::Init => write!(f, "INIT"),
            Self::Rtr => write!(f, "RTR"),
            Self::Rts => write!(f, "RTS"),
            Self::Sqd => write!(f, "SQD"),
            Self::Sqe => write!(f, "SQE"),
            Self::Error => write!(f, "ERROR"),
        }
    }
}

/// Work request for send operations
#[derive(Debug, Clone)]
pub struct SendWorkRequest {
    /// Work request ID
    pub id: u64,
    /// Local memory region
    pub local_addr: u64,
    /// Length of data
    pub length: u32,
    /// Local key
    pub lkey: LocalKey,
    /// Remote address (for RDMA write/read)
    pub remote_addr: Option<u64>,
    /// Remote key (for RDMA write/read)
    pub rkey: Option<RemoteKey>,
    /// Operation type
    pub opcode: SendOpcode,
    /// Immediate data (optional)
    pub imm_data: Option<u32>,
    /// Signal completion
    pub signaled: bool,
    /// Inline data (for small payloads)
    pub inline: bool,
}

/// Send operation type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendOpcode {
    /// Send
    Send,
    /// Send with immediate
    SendWithImm,
    /// RDMA Write
    RdmaWrite,
    /// RDMA Write with immediate
    RdmaWriteWithImm,
    /// RDMA Read
    RdmaRead,
    /// Atomic Compare and Swap
    AtomicCmpSwp,
    /// Atomic Fetch and Add
    AtomicFetchAdd,
}

/// Work request for receive operations
#[derive(Debug, Clone)]
pub struct RecvWorkRequest {
    /// Work request ID
    pub id: u64,
    /// Local memory region
    pub local_addr: u64,
    /// Maximum length
    pub length: u32,
    /// Local key
    pub lkey: LocalKey,
}

/// Work completion
#[derive(Debug, Clone)]
pub struct WorkCompletion {
    /// Work request ID
    pub id: u64,
    /// Status
    pub status: CompletionStatus,
    /// Operation type
    pub opcode: CompletionOpcode,
    /// Bytes transferred
    pub byte_len: u32,
    /// Queue pair number
    pub qp_num: QueuePairNum,
    /// Source queue pair (for UD)
    pub src_qp: Option<QueuePairNum>,
    /// Immediate data
    pub imm_data: Option<u32>,
    /// Invalidated remote key
    pub invalidated_rkey: Option<RemoteKey>,
}

/// Completion status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionStatus {
    /// Success
    Success,
    /// Local length error
    LocalLenErr,
    /// Local QP operation error
    LocalQpOpErr,
    /// Local protection error
    LocalProtErr,
    /// Work request flushed
    WrFlushErr,
    /// Memory window bind error
    MwBindErr,
    /// Bad response error
    BadRespErr,
    /// Local access error
    LocalAccessErr,
    /// Remote invalid request
    RemInvReqErr,
    /// Remote access error
    RemAccessErr,
    /// Remote operation error
    RemOpErr,
    /// Retry exceeded
    RetryExcErr,
    /// RNR retry exceeded
    RnrRetryExcErr,
    /// Fatal error
    FatalErr,
}

impl CompletionStatus {
    /// Check if successful
    pub fn is_success(&self) -> bool {
        matches!(self, Self::Success)
    }
}

/// Completion operation type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionOpcode {
    /// Send completed
    Send,
    /// RDMA Write completed
    RdmaWrite,
    /// RDMA Read completed
    RdmaRead,
    /// Atomic operation completed
    Atomic,
    /// Receive completed
    Recv,
    /// Receive with immediate
    RecvRdmaWithImm,
}

/// Queue pair handle
pub struct QueuePair {
    /// Queue pair number
    qp_num: QueuePairNum,

    /// Queue pair type
    qp_type: QueuePairType,

    /// Current state
    state: RwLock<QueuePairState>,

    /// Configuration
    config: QueuePairConfig,

    /// Send queue depth
    send_depth: u32,

    /// Receive queue depth
    recv_depth: u32,

    /// Outstanding send work requests
    outstanding_sends: AtomicU32,

    /// Outstanding receive work requests
    outstanding_recvs: AtomicU32,

    /// Posted send work requests
    posted_sends: AtomicU64,

    /// Posted receive work requests
    posted_recvs: AtomicU64,

    /// Completed send work requests
    completed_sends: AtomicU64,

    /// Completed receive work requests
    completed_recvs: AtomicU64,

    /// Bytes sent
    bytes_sent: AtomicU64,

    /// Bytes received
    bytes_received: AtomicU64,

    /// Send errors
    send_errors: AtomicU64,

    /// Receive errors
    recv_errors: AtomicU64,

    /// Remote queue pair number (for connected QPs)
    remote_qpn: RwLock<Option<QueuePairNum>>,

    /// Remote LID (for IB)
    remote_lid: RwLock<Option<u16>>,

    /// Remote GID (for RoCE)
    remote_gid: RwLock<Option<Gid>>,
}

impl QueuePair {
    /// Create a new queue pair
    pub fn new(qp_num: QueuePairNum, qp_type: QueuePairType, config: QueuePairConfig) -> Self {
        Self {
            qp_num,
            qp_type,
            state: RwLock::new(QueuePairState::Reset),
            config: config.clone(),
            send_depth: config.max_send_wr,
            recv_depth: config.max_recv_wr,
            outstanding_sends: AtomicU32::new(0),
            outstanding_recvs: AtomicU32::new(0),
            posted_sends: AtomicU64::new(0),
            posted_recvs: AtomicU64::new(0),
            completed_sends: AtomicU64::new(0),
            completed_recvs: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            send_errors: AtomicU64::new(0),
            recv_errors: AtomicU64::new(0),
            remote_qpn: RwLock::new(None),
            remote_lid: RwLock::new(None),
            remote_gid: RwLock::new(None),
        }
    }

    /// Get queue pair number
    pub fn qp_num(&self) -> QueuePairNum {
        self.qp_num
    }

    /// Get queue pair type
    pub fn qp_type(&self) -> QueuePairType {
        self.qp_type
    }

    /// Get current state
    pub fn state(&self) -> QueuePairState {
        *self.state.read()
    }

    /// Transition to Init state
    pub fn init(&self) -> RdmaResult<()> {
        let mut state = self.state.write();
        if *state != QueuePairState::Reset {
            return Err(RdmaError::QueuePairError(format!(
                "Cannot init QP in state {}",
                *state
            )));
        }
        *state = QueuePairState::Init;
        debug!(qp_num = self.qp_num.0, "QP transitioned to INIT");
        Ok(())
    }

    /// Transition to RTR (Ready to Receive) state
    pub fn ready_to_receive(
        &self,
        remote_qpn: QueuePairNum,
        remote_lid: Option<u16>,
        remote_gid: Option<Gid>,
    ) -> RdmaResult<()> {
        let mut state = self.state.write();
        if *state != QueuePairState::Init {
            return Err(RdmaError::QueuePairError(format!(
                "Cannot RTR QP in state {}",
                *state
            )));
        }

        *self.remote_qpn.write() = Some(remote_qpn);
        *self.remote_lid.write() = remote_lid;
        *self.remote_gid.write() = remote_gid;

        *state = QueuePairState::Rtr;
        debug!(
            qp_num = self.qp_num.0,
            remote_qpn = remote_qpn.0,
            "QP transitioned to RTR"
        );
        Ok(())
    }

    /// Transition to RTS (Ready to Send) state
    pub fn ready_to_send(&self) -> RdmaResult<()> {
        let mut state = self.state.write();
        if *state != QueuePairState::Rtr {
            return Err(RdmaError::QueuePairError(format!(
                "Cannot RTS QP in state {}",
                *state
            )));
        }
        *state = QueuePairState::Rts;
        debug!(qp_num = self.qp_num.0, "QP transitioned to RTS");
        Ok(())
    }

    /// Transition to Error state
    pub fn to_error(&self) {
        let mut state = self.state.write();
        *state = QueuePairState::Error;
        warn!(qp_num = self.qp_num.0, "QP transitioned to ERROR");
    }

    /// Post a send work request
    pub fn post_send(&self, wr: &SendWorkRequest) -> RdmaResult<()> {
        let state = *self.state.read();
        if state != QueuePairState::Rts {
            return Err(RdmaError::QueuePairError(format!(
                "Cannot post send in state {}",
                state
            )));
        }

        let outstanding = self.outstanding_sends.load(Ordering::Relaxed);
        if outstanding >= self.send_depth {
            return Err(RdmaError::QueuePairError("Send queue full".to_string()));
        }

        self.outstanding_sends.fetch_add(1, Ordering::Relaxed);
        self.posted_sends.fetch_add(1, Ordering::Relaxed);

        debug!(
            qp_num = self.qp_num.0,
            wr_id = wr.id,
            opcode = ?wr.opcode,
            length = wr.length,
            "Posted send WR"
        );

        Ok(())
    }

    /// Post a receive work request
    pub fn post_recv(&self, wr: &RecvWorkRequest) -> RdmaResult<()> {
        let state = *self.state.read();
        if state != QueuePairState::Rtr && state != QueuePairState::Rts {
            return Err(RdmaError::QueuePairError(format!(
                "Cannot post recv in state {}",
                state
            )));
        }

        let outstanding = self.outstanding_recvs.load(Ordering::Relaxed);
        if outstanding >= self.recv_depth {
            return Err(RdmaError::QueuePairError("Receive queue full".to_string()));
        }

        self.outstanding_recvs.fetch_add(1, Ordering::Relaxed);
        self.posted_recvs.fetch_add(1, Ordering::Relaxed);

        debug!(
            qp_num = self.qp_num.0,
            wr_id = wr.id,
            length = wr.length,
            "Posted recv WR"
        );

        Ok(())
    }

    /// Handle a work completion
    pub fn complete(&self, wc: &WorkCompletion) {
        match wc.opcode {
            CompletionOpcode::Send
            | CompletionOpcode::RdmaWrite
            | CompletionOpcode::RdmaRead
            | CompletionOpcode::Atomic => {
                self.outstanding_sends.fetch_sub(1, Ordering::Relaxed);
                self.completed_sends.fetch_add(1, Ordering::Relaxed);
                if wc.status.is_success() {
                    self.bytes_sent
                        .fetch_add(wc.byte_len as u64, Ordering::Relaxed);
                } else {
                    self.send_errors.fetch_add(1, Ordering::Relaxed);
                }
            }
            CompletionOpcode::Recv | CompletionOpcode::RecvRdmaWithImm => {
                self.outstanding_recvs.fetch_sub(1, Ordering::Relaxed);
                self.completed_recvs.fetch_add(1, Ordering::Relaxed);
                if wc.status.is_success() {
                    self.bytes_received
                        .fetch_add(wc.byte_len as u64, Ordering::Relaxed);
                } else {
                    self.recv_errors.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    /// Get queue pair statistics
    pub fn stats(&self) -> QueuePairStats {
        QueuePairStats {
            qp_num: self.qp_num,
            qp_type: self.qp_type,
            state: *self.state.read(),
            outstanding_sends: self.outstanding_sends.load(Ordering::Relaxed),
            outstanding_recvs: self.outstanding_recvs.load(Ordering::Relaxed),
            posted_sends: self.posted_sends.load(Ordering::Relaxed),
            posted_recvs: self.posted_recvs.load(Ordering::Relaxed),
            completed_sends: self.completed_sends.load(Ordering::Relaxed),
            completed_recvs: self.completed_recvs.load(Ordering::Relaxed),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
            send_errors: self.send_errors.load(Ordering::Relaxed),
            recv_errors: self.recv_errors.load(Ordering::Relaxed),
        }
    }
}

/// Queue pair statistics
#[derive(Debug, Clone, Default)]
pub struct QueuePairStats {
    /// Queue pair number
    pub qp_num: QueuePairNum,
    /// Queue pair type
    pub qp_type: QueuePairType,
    /// Current state
    pub state: QueuePairState,
    /// Outstanding sends
    pub outstanding_sends: u32,
    /// Outstanding receives
    pub outstanding_recvs: u32,
    /// Total posted sends
    pub posted_sends: u64,
    /// Total posted receives
    pub posted_recvs: u64,
    /// Total completed sends
    pub completed_sends: u64,
    /// Total completed receives
    pub completed_recvs: u64,
    /// Total bytes sent
    pub bytes_sent: u64,
    /// Total bytes received
    pub bytes_received: u64,
    /// Send errors
    pub send_errors: u64,
    /// Receive errors
    pub recv_errors: u64,
}

impl Default for QueuePairNum {
    fn default() -> Self {
        Self(0)
    }
}

/// Connection information for exchange
#[derive(Debug, Clone)]
pub struct ConnectionInfo {
    /// Queue pair number
    pub qp_num: QueuePairNum,
    /// Local ID (for IB)
    pub lid: u16,
    /// GID (for RoCE)
    pub gid: Gid,
    /// PSN (Packet Sequence Number)
    pub psn: u32,
    /// MTU
    pub mtu: RdmaMtu,
}

/// Connection manager for RDMA connections
pub struct ConnectionManager {
    /// Queue pairs by number
    queue_pairs: RwLock<HashMap<QueuePairNum, Arc<QueuePair>>>,

    /// Connections by remote address
    connections: RwLock<HashMap<SocketAddr, Arc<QueuePair>>>,

    /// Next QP number
    next_qpn: AtomicU32,

    /// Configuration
    config: RdmaConfig,

    /// Local GID
    local_gid: Gid,

    /// Local LID
    local_lid: u16,

    /// Total connections created
    connections_created: AtomicU64,

    /// Total connections closed
    connections_closed: AtomicU64,
}

impl ConnectionManager {
    /// Create a new connection manager
    pub fn new(config: RdmaConfig, local_gid: Gid, local_lid: u16) -> Self {
        Self {
            queue_pairs: RwLock::new(HashMap::new()),
            connections: RwLock::new(HashMap::new()),
            next_qpn: AtomicU32::new(1),
            config,
            local_gid,
            local_lid,
            connections_created: AtomicU64::new(0),
            connections_closed: AtomicU64::new(0),
        }
    }

    /// Create a new queue pair
    pub fn create_qp(&self, qp_type: QueuePairType) -> RdmaResult<Arc<QueuePair>> {
        let qp_num = QueuePairNum(self.next_qpn.fetch_add(1, Ordering::Relaxed));
        let qp = Arc::new(QueuePair::new(
            qp_num,
            qp_type,
            self.config.queue_pair.clone(),
        ));

        self.queue_pairs.write().insert(qp_num, qp.clone());

        info!(qp_num = qp_num.0, qp_type = %qp_type, "Created queue pair");
        Ok(qp)
    }

    /// Get a queue pair by number
    pub fn get_qp(&self, qp_num: QueuePairNum) -> Option<Arc<QueuePair>> {
        self.queue_pairs.read().get(&qp_num).cloned()
    }

    /// Connect to a remote peer
    pub fn connect(
        &self,
        remote_addr: SocketAddr,
        remote_info: ConnectionInfo,
    ) -> RdmaResult<Arc<QueuePair>> {
        // Check if already connected
        if let Some(qp) = self.connections.read().get(&remote_addr) {
            return Ok(qp.clone());
        }

        // Create new QP
        let qp = self.create_qp(QueuePairType::RC)?;

        // Initialize QP
        qp.init()?;

        // Transition to RTR
        qp.ready_to_receive(
            remote_info.qp_num,
            Some(remote_info.lid),
            Some(remote_info.gid),
        )?;

        // Transition to RTS
        qp.ready_to_send()?;

        // Store connection
        self.connections.write().insert(remote_addr, qp.clone());
        self.connections_created.fetch_add(1, Ordering::Relaxed);

        info!(
            remote = %remote_addr,
            qp_num = qp.qp_num().0,
            remote_qp = remote_info.qp_num.0,
            "Established RDMA connection"
        );

        Ok(qp)
    }

    /// Accept a connection from a remote peer
    pub fn accept(
        &self,
        remote_addr: SocketAddr,
        remote_info: ConnectionInfo,
    ) -> RdmaResult<ConnectionInfo> {
        // Create new QP
        let qp = self.create_qp(QueuePairType::RC)?;

        // Initialize QP
        qp.init()?;

        // Transition to RTR
        qp.ready_to_receive(
            remote_info.qp_num,
            Some(remote_info.lid),
            Some(remote_info.gid),
        )?;

        // Transition to RTS
        qp.ready_to_send()?;

        // Store connection
        self.connections.write().insert(remote_addr, qp.clone());
        self.connections_created.fetch_add(1, Ordering::Relaxed);

        // Return local connection info
        let local_info = ConnectionInfo {
            qp_num: qp.qp_num(),
            lid: self.local_lid,
            gid: self.local_gid,
            psn: 0, // Would be random in real implementation
            mtu: self.config.queue_pair.mtu,
        };

        info!(
            remote = %remote_addr,
            qp_num = qp.qp_num().0,
            "Accepted RDMA connection"
        );

        Ok(local_info)
    }

    /// Disconnect from a remote peer
    pub fn disconnect(&self, remote_addr: &SocketAddr) -> RdmaResult<()> {
        if let Some(qp) = self.connections.write().remove(remote_addr) {
            qp.to_error();
            self.queue_pairs.write().remove(&qp.qp_num());
            self.connections_closed.fetch_add(1, Ordering::Relaxed);

            info!(
                remote = %remote_addr,
                qp_num = qp.qp_num().0,
                "Closed RDMA connection"
            );
        }

        Ok(())
    }

    /// Get connection by remote address
    pub fn get_connection(&self, remote_addr: &SocketAddr) -> Option<Arc<QueuePair>> {
        self.connections.read().get(remote_addr).cloned()
    }

    /// Get local connection info for exchange
    pub fn local_info(&self, qp: &QueuePair) -> ConnectionInfo {
        ConnectionInfo {
            qp_num: qp.qp_num(),
            lid: self.local_lid,
            gid: self.local_gid,
            psn: 0, // Would be random in real implementation
            mtu: self.config.queue_pair.mtu,
        }
    }

    /// Get statistics
    pub fn stats(&self) -> ConnectionManagerStats {
        let qp_stats: Vec<_> = self
            .queue_pairs
            .read()
            .values()
            .map(|qp| qp.stats())
            .collect();

        ConnectionManagerStats {
            queue_pair_count: self.queue_pairs.read().len(),
            connection_count: self.connections.read().len(),
            connections_created: self.connections_created.load(Ordering::Relaxed),
            connections_closed: self.connections_closed.load(Ordering::Relaxed),
            queue_pair_stats: qp_stats,
        }
    }
}

/// Connection manager statistics
#[derive(Debug, Clone, Default)]
pub struct ConnectionManagerStats {
    /// Number of queue pairs
    pub queue_pair_count: usize,
    /// Number of active connections
    pub connection_count: usize,
    /// Total connections created
    pub connections_created: u64,
    /// Total connections closed
    pub connections_closed: u64,
    /// Per-QP statistics
    pub queue_pair_stats: Vec<QueuePairStats>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_queue_pair_state_transitions() {
        let qp_num = QueuePairNum(1);
        let config = QueuePairConfig::default();
        let qp = QueuePair::new(qp_num, QueuePairType::RC, config);

        assert_eq!(qp.state(), QueuePairState::Reset);

        qp.init().unwrap();
        assert_eq!(qp.state(), QueuePairState::Init);

        let remote_qpn = QueuePairNum(2);
        let remote_gid = Gid::default();
        qp.ready_to_receive(remote_qpn, Some(1), Some(remote_gid))
            .unwrap();
        assert_eq!(qp.state(), QueuePairState::Rtr);

        qp.ready_to_send().unwrap();
        assert_eq!(qp.state(), QueuePairState::Rts);
    }

    #[test]
    fn test_queue_pair_invalid_transition() {
        let qp_num = QueuePairNum(1);
        let config = QueuePairConfig::default();
        let qp = QueuePair::new(qp_num, QueuePairType::RC, config);

        // Can't go directly to RTR from RESET
        let remote_qpn = QueuePairNum(2);
        let remote_gid = Gid::default();
        assert!(qp
            .ready_to_receive(remote_qpn, Some(1), Some(remote_gid))
            .is_err());
    }

    #[test]
    fn test_connection_manager() {
        let config = RdmaConfig::default();
        let local_gid = Gid::default();
        let manager = ConnectionManager::new(config, local_gid, 1);

        let qp = manager.create_qp(QueuePairType::RC).unwrap();
        assert_eq!(qp.state(), QueuePairState::Reset);

        let stats = manager.stats();
        assert_eq!(stats.queue_pair_count, 1);
    }

    #[test]
    fn test_completion_status() {
        assert!(CompletionStatus::Success.is_success());
        assert!(!CompletionStatus::FatalErr.is_success());
    }
}
