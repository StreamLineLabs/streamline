//! High-level RDMA transport API
//!
//! This module provides a high-level interface for RDMA operations,
//! abstracting away the complexity of queue pairs and memory regions.

use super::{
    config::{RdmaConfig, TransportMode},
    connection::{
        CompletionOpcode, CompletionStatus, ConnectionInfo, ConnectionManager, QueuePair,
        QueuePairNum, RecvWorkRequest, SendOpcode, SendWorkRequest, WorkCompletion,
    },
    device::{DeviceManager, Gid, RdmaDevice},
    memory::{LocalKey, MemoryManager, MemoryRegion, RemoteKey},
    AccessFlags, RdmaError, RdmaResult, RdmaTransport as TransportType,
};
use parking_lot::{Mutex, RwLock};
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{debug, error, info, warn};

/// RDMA transport for inter-broker communication
pub struct RdmaTransportLayer {
    /// Configuration
    config: RdmaConfig,

    /// Device manager
    device_manager: DeviceManager,

    /// Active device
    active_device: Option<String>,

    /// Memory manager
    memory_manager: Arc<MemoryManager>,

    /// Connection manager
    connection_manager: Option<ConnectionManager>,

    /// Whether transport is running
    running: AtomicBool,

    /// Local endpoint address
    local_addr: RwLock<Option<SocketAddr>>,

    /// Pending completions
    pending_completions: Mutex<VecDeque<WorkCompletion>>,

    /// Next work request ID
    next_wr_id: AtomicU64,

    /// Total messages sent
    messages_sent: AtomicU64,

    /// Total messages received
    messages_received: AtomicU64,

    /// Total bytes sent
    bytes_sent: AtomicU64,

    /// Total bytes received
    bytes_received: AtomicU64,

    /// Send errors
    send_errors: AtomicU64,

    /// Receive errors
    recv_errors: AtomicU64,
}

impl RdmaTransportLayer {
    /// Create a new RDMA transport
    pub fn new(config: RdmaConfig) -> Self {
        let memory_manager = Arc::new(MemoryManager::new(config.memory.huge_pages));

        Self {
            config,
            device_manager: DeviceManager::new(),
            active_device: None,
            memory_manager,
            connection_manager: None,
            running: AtomicBool::new(false),
            local_addr: RwLock::new(None),
            pending_completions: Mutex::new(VecDeque::new()),
            next_wr_id: AtomicU64::new(1),
            messages_sent: AtomicU64::new(0),
            messages_received: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            send_errors: AtomicU64::new(0),
            recv_errors: AtomicU64::new(0),
        }
    }

    /// Create transport with a preset mode
    pub fn with_mode(mode: TransportMode) -> Self {
        Self::new(mode.to_config())
    }

    /// Initialize the transport
    pub fn init(&mut self) -> RdmaResult<()> {
        if !self.config.enabled {
            return Err(RdmaError::ConfigError("RDMA not enabled".to_string()));
        }

        info!("Initializing RDMA transport");

        // Discover devices
        let devices = self.device_manager.discover()?;
        if devices.is_empty() {
            return Err(RdmaError::NotAvailable("No RDMA devices found".to_string()));
        }

        info!(count = devices.len(), "Discovered RDMA devices");

        // Select device
        let device_name = if let Some(ref name) = self.config.device_name {
            if !devices.contains(name) {
                return Err(RdmaError::DeviceError(format!(
                    "Specified device {} not found",
                    name
                )));
            }
            name.clone()
        } else {
            // Auto-select based on transport type
            if let Some(device) = self
                .device_manager
                .find_device_for_transport(self.config.transport)
            {
                device.name.clone()
            } else {
                devices.first().cloned().ok_or_else(|| {
                    RdmaError::NotAvailable("No suitable RDMA device found".to_string())
                })?
            }
        };

        info!(device = %device_name, "Selected RDMA device");
        self.active_device = Some(device_name);

        // Pre-allocate memory pools
        self.memory_manager.preallocate()?;

        // Create connection manager
        let local_gid = self.get_local_gid()?;
        let local_lid = self.get_local_lid();
        self.connection_manager = Some(ConnectionManager::new(
            self.config.clone(),
            local_gid,
            local_lid,
        ));

        self.running.store(true, Ordering::SeqCst);
        info!("RDMA transport initialized");

        Ok(())
    }

    /// Shutdown the transport
    pub fn shutdown(&self) -> RdmaResult<()> {
        if !self.running.swap(false, Ordering::SeqCst) {
            return Ok(());
        }

        info!("Shutting down RDMA transport");

        // Close all connections
        // In real implementation, would iterate and close each

        Ok(())
    }

    /// Check if transport is running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Bind to a local address
    pub fn bind(&self, addr: SocketAddr) -> RdmaResult<()> {
        *self.local_addr.write() = Some(addr);
        info!(addr = %addr, "RDMA transport bound");
        Ok(())
    }

    /// Connect to a remote peer
    pub fn connect(&self, remote_addr: SocketAddr) -> RdmaResult<RdmaConnection> {
        let conn_manager = self.connection_manager.as_ref().ok_or_else(|| {
            RdmaError::InternalError("Connection manager not initialized".to_string())
        })?;

        // In real implementation, would exchange connection info over TCP
        // and then create the RDMA connection
        let local_gid = self.get_local_gid()?;
        let remote_info = ConnectionInfo {
            qp_num: QueuePairNum(0), // Would come from peer
            lid: 0,
            gid: local_gid, // Would come from peer
            psn: 0,
            mtu: self.config.queue_pair.mtu,
        };

        let qp = conn_manager.connect(remote_addr, remote_info)?;

        Ok(RdmaConnection {
            remote_addr,
            qp,
            memory_manager: self.memory_manager.clone(),
            config: self.config.clone(),
        })
    }

    /// Accept an incoming connection
    pub fn accept(
        &self,
        remote_addr: SocketAddr,
        remote_info: ConnectionInfo,
    ) -> RdmaResult<(RdmaConnection, ConnectionInfo)> {
        let conn_manager = self.connection_manager.as_ref().ok_or_else(|| {
            RdmaError::InternalError("Connection manager not initialized".to_string())
        })?;

        let local_info = conn_manager.accept(remote_addr, remote_info)?;

        let qp = conn_manager.get_connection(&remote_addr).ok_or_else(|| {
            RdmaError::ConnectionError("Connection not found after accept".to_string())
        })?;

        let connection = RdmaConnection {
            remote_addr,
            qp,
            memory_manager: self.memory_manager.clone(),
            config: self.config.clone(),
        };

        Ok((connection, local_info))
    }

    /// Get an existing connection
    pub fn get_connection(&self, remote_addr: &SocketAddr) -> Option<RdmaConnection> {
        let conn_manager = self.connection_manager.as_ref()?;
        let qp = conn_manager.get_connection(remote_addr)?;

        Some(RdmaConnection {
            remote_addr: *remote_addr,
            qp,
            memory_manager: self.memory_manager.clone(),
            config: self.config.clone(),
        })
    }

    /// Allocate a send buffer
    pub fn alloc_send_buffer(&self, size: usize) -> RdmaResult<Arc<MemoryRegion>> {
        self.memory_manager
            .allocate(size, AccessFlags::local_only())
    }

    /// Allocate a receive buffer
    pub fn alloc_recv_buffer(&self, size: usize) -> RdmaResult<Arc<MemoryRegion>> {
        self.memory_manager
            .allocate(size, AccessFlags::remote_full())
    }

    /// Release a buffer
    pub fn release_buffer(&self, buffer: Arc<MemoryRegion>) {
        self.memory_manager.release(buffer);
    }

    /// Poll for completions
    pub fn poll_completions(&self, max: usize) -> Vec<WorkCompletion> {
        let mut completions = self.pending_completions.lock();
        let count = completions.len().min(max);
        completions.drain(..count).collect()
    }

    /// Get the active device
    pub fn active_device(&self) -> Option<&RdmaDevice> {
        self.active_device
            .as_ref()
            .and_then(|name| self.device_manager.get_device(name))
    }

    /// Get local GID
    fn get_local_gid(&self) -> RdmaResult<Gid> {
        let device = self
            .active_device()
            .ok_or_else(|| RdmaError::DeviceError("No active device".to_string()))?;

        // Get first port's GID
        device
            .ports
            .first()
            .and_then(|p| p.gids.first().copied())
            .unwrap_or_else(Gid::default)
            .pipe(Ok)
    }

    /// Get local LID
    fn get_local_lid(&self) -> u16 {
        self.active_device()
            .and_then(|d| d.ports.first())
            .map(|p| p.lid)
            .unwrap_or(0)
    }

    /// Get transport statistics
    pub fn stats(&self) -> TransportStats {
        let conn_stats = self.connection_manager.as_ref().map(|cm| cm.stats());
        let memory_stats = self.memory_manager.stats();

        TransportStats {
            running: self.running.load(Ordering::Relaxed),
            device: self.active_device.clone(),
            transport_type: self.config.transport,
            messages_sent: self.messages_sent.load(Ordering::Relaxed),
            messages_received: self.messages_received.load(Ordering::Relaxed),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
            send_errors: self.send_errors.load(Ordering::Relaxed),
            recv_errors: self.recv_errors.load(Ordering::Relaxed),
            connection_count: conn_stats.as_ref().map(|s| s.connection_count).unwrap_or(0),
            queue_pair_count: conn_stats.as_ref().map(|s| s.queue_pair_count).unwrap_or(0),
            memory_registered: memory_stats.total_registered,
        }
    }
}

/// Helper trait for pipe operator
trait Pipe: Sized {
    fn pipe<F, R>(self, f: F) -> R
    where
        F: FnOnce(Self) -> R,
    {
        f(self)
    }
}

impl<T> Pipe for T {}

/// An established RDMA connection
pub struct RdmaConnection {
    /// Remote address
    remote_addr: SocketAddr,

    /// Queue pair
    qp: Arc<QueuePair>,

    /// Memory manager
    memory_manager: Arc<MemoryManager>,

    /// Configuration
    config: RdmaConfig,
}

impl RdmaConnection {
    /// Get remote address
    pub fn remote_addr(&self) -> SocketAddr {
        self.remote_addr
    }

    /// Get queue pair number
    pub fn qp_num(&self) -> QueuePairNum {
        self.qp.qp_num()
    }

    /// Send data using RDMA Send
    pub fn send(&self, buffer: &MemoryRegion) -> RdmaResult<u64> {
        let wr_id = self.next_wr_id();

        let wr = SendWorkRequest {
            id: wr_id,
            local_addr: buffer.addr() as u64,
            length: buffer.size() as u32,
            lkey: buffer.lkey(),
            remote_addr: None,
            rkey: None,
            opcode: SendOpcode::Send,
            imm_data: None,
            signaled: true,
            inline: buffer.size() <= self.config.max_inline_size as usize,
        };

        self.qp.post_send(&wr)?;
        buffer.record_write(buffer.size() as u64);

        debug!(
            wr_id = wr_id,
            remote = %self.remote_addr,
            size = buffer.size(),
            "Sent RDMA message"
        );

        Ok(wr_id)
    }

    /// Send data with immediate value
    pub fn send_with_imm(&self, buffer: &MemoryRegion, imm_data: u32) -> RdmaResult<u64> {
        let wr_id = self.next_wr_id();

        let wr = SendWorkRequest {
            id: wr_id,
            local_addr: buffer.addr() as u64,
            length: buffer.size() as u32,
            lkey: buffer.lkey(),
            remote_addr: None,
            rkey: None,
            opcode: SendOpcode::SendWithImm,
            imm_data: Some(imm_data),
            signaled: true,
            inline: buffer.size() <= self.config.max_inline_size as usize,
        };

        self.qp.post_send(&wr)?;

        Ok(wr_id)
    }

    /// RDMA Write to remote memory
    pub fn write(
        &self,
        local_buffer: &MemoryRegion,
        remote_addr: u64,
        rkey: RemoteKey,
    ) -> RdmaResult<u64> {
        let wr_id = self.next_wr_id();

        let wr = SendWorkRequest {
            id: wr_id,
            local_addr: local_buffer.addr() as u64,
            length: local_buffer.size() as u32,
            lkey: local_buffer.lkey(),
            remote_addr: Some(remote_addr),
            rkey: Some(rkey),
            opcode: SendOpcode::RdmaWrite,
            imm_data: None,
            signaled: true,
            inline: false,
        };

        self.qp.post_send(&wr)?;
        local_buffer.record_write(local_buffer.size() as u64);

        debug!(
            wr_id = wr_id,
            remote = %self.remote_addr,
            remote_addr = remote_addr,
            size = local_buffer.size(),
            "Posted RDMA Write"
        );

        Ok(wr_id)
    }

    /// RDMA Read from remote memory
    pub fn read(
        &self,
        local_buffer: &MemoryRegion,
        remote_addr: u64,
        rkey: RemoteKey,
        length: u32,
    ) -> RdmaResult<u64> {
        let wr_id = self.next_wr_id();

        let wr = SendWorkRequest {
            id: wr_id,
            local_addr: local_buffer.addr() as u64,
            length,
            lkey: local_buffer.lkey(),
            remote_addr: Some(remote_addr),
            rkey: Some(rkey),
            opcode: SendOpcode::RdmaRead,
            imm_data: None,
            signaled: true,
            inline: false,
        };

        self.qp.post_send(&wr)?;

        debug!(
            wr_id = wr_id,
            remote = %self.remote_addr,
            remote_addr = remote_addr,
            length = length,
            "Posted RDMA Read"
        );

        Ok(wr_id)
    }

    /// Post a receive buffer
    pub fn post_recv(&self, buffer: &MemoryRegion) -> RdmaResult<u64> {
        let wr_id = self.next_wr_id();

        let wr = RecvWorkRequest {
            id: wr_id,
            local_addr: buffer.addr() as u64,
            length: buffer.size() as u32,
            lkey: buffer.lkey(),
        };

        self.qp.post_recv(&wr)?;

        debug!(wr_id = wr_id, size = buffer.size(), "Posted receive buffer");

        Ok(wr_id)
    }

    /// Atomic Compare and Swap
    pub fn atomic_cas(
        &self,
        local_buffer: &MemoryRegion,
        remote_addr: u64,
        rkey: RemoteKey,
        compare: u64,
        swap: u64,
    ) -> RdmaResult<u64> {
        let wr_id = self.next_wr_id();

        let wr = SendWorkRequest {
            id: wr_id,
            local_addr: local_buffer.addr() as u64,
            length: 8, // 64-bit atomic
            lkey: local_buffer.lkey(),
            remote_addr: Some(remote_addr),
            rkey: Some(rkey),
            opcode: SendOpcode::AtomicCmpSwp,
            imm_data: None,
            signaled: true,
            inline: false,
        };

        self.qp.post_send(&wr)?;

        debug!(
            wr_id = wr_id,
            remote_addr = remote_addr,
            compare = compare,
            swap = swap,
            "Posted atomic CAS"
        );

        Ok(wr_id)
    }

    /// Atomic Fetch and Add
    pub fn atomic_fetch_add(
        &self,
        local_buffer: &MemoryRegion,
        remote_addr: u64,
        rkey: RemoteKey,
        add_value: u64,
    ) -> RdmaResult<u64> {
        let wr_id = self.next_wr_id();

        let wr = SendWorkRequest {
            id: wr_id,
            local_addr: local_buffer.addr() as u64,
            length: 8, // 64-bit atomic
            lkey: local_buffer.lkey(),
            remote_addr: Some(remote_addr),
            rkey: Some(rkey),
            opcode: SendOpcode::AtomicFetchAdd,
            imm_data: None,
            signaled: true,
            inline: false,
        };

        self.qp.post_send(&wr)?;

        debug!(
            wr_id = wr_id,
            remote_addr = remote_addr,
            add_value = add_value,
            "Posted atomic fetch-add"
        );

        Ok(wr_id)
    }

    /// Get connection statistics
    pub fn stats(&self) -> ConnectionStats {
        let qp_stats = self.qp.stats();

        ConnectionStats {
            remote_addr: self.remote_addr,
            qp_num: self.qp.qp_num(),
            state: format!("{}", self.qp.state()),
            bytes_sent: qp_stats.bytes_sent,
            bytes_received: qp_stats.bytes_received,
            messages_sent: qp_stats.completed_sends,
            messages_received: qp_stats.completed_recvs,
            send_errors: qp_stats.send_errors,
            recv_errors: qp_stats.recv_errors,
            outstanding_sends: qp_stats.outstanding_sends,
            outstanding_recvs: qp_stats.outstanding_recvs,
        }
    }

    /// Get next work request ID
    fn next_wr_id(&self) -> u64 {
        static COUNTER: AtomicU64 = AtomicU64::new(1);
        COUNTER.fetch_add(1, Ordering::Relaxed)
    }
}

/// Transport statistics
#[derive(Debug, Clone, Default)]
pub struct TransportStats {
    /// Whether transport is running
    pub running: bool,
    /// Active device name
    pub device: Option<String>,
    /// Transport type
    pub transport_type: TransportType,
    /// Total messages sent
    pub messages_sent: u64,
    /// Total messages received
    pub messages_received: u64,
    /// Total bytes sent
    pub bytes_sent: u64,
    /// Total bytes received
    pub bytes_received: u64,
    /// Send errors
    pub send_errors: u64,
    /// Receive errors
    pub recv_errors: u64,
    /// Number of active connections
    pub connection_count: usize,
    /// Number of queue pairs
    pub queue_pair_count: usize,
    /// Total memory registered
    pub memory_registered: u64,
}

/// Connection statistics
#[derive(Debug, Clone)]
pub struct ConnectionStats {
    /// Remote address
    pub remote_addr: SocketAddr,
    /// Queue pair number
    pub qp_num: QueuePairNum,
    /// Connection state
    pub state: String,
    /// Bytes sent
    pub bytes_sent: u64,
    /// Bytes received
    pub bytes_received: u64,
    /// Messages sent
    pub messages_sent: u64,
    /// Messages received
    pub messages_received: u64,
    /// Send errors
    pub send_errors: u64,
    /// Receive errors
    pub recv_errors: u64,
    /// Outstanding sends
    pub outstanding_sends: u32,
    /// Outstanding receives
    pub outstanding_recvs: u32,
}

/// Check if RDMA is available on this system
pub fn is_rdma_available() -> bool {
    DeviceManager::is_available()
}

/// Get available RDMA devices
pub fn get_rdma_devices() -> RdmaResult<Vec<String>> {
    let mut manager = DeviceManager::new();
    manager.discover()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transport_creation() {
        let config = RdmaConfig::default();
        let transport = RdmaTransportLayer::new(config);

        assert!(!transport.is_running());
    }

    #[test]
    fn test_transport_with_mode() {
        let transport = RdmaTransportLayer::with_mode(TransportMode::LowLatency);
        assert!(!transport.is_running());
    }

    #[test]
    fn test_is_rdma_available() {
        // Just check it doesn't panic
        let _ = is_rdma_available();
    }

    #[test]
    fn test_transport_stats() {
        let config = RdmaConfig::default();
        let transport = RdmaTransportLayer::new(config);

        let stats = transport.stats();
        assert!(!stats.running);
        assert_eq!(stats.messages_sent, 0);
        assert_eq!(stats.bytes_sent, 0);
    }
}
