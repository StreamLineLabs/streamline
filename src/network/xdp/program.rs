//! XDP program loader and manager

use super::{XdpError, XdpMode, XdpResult};
use std::collections::HashMap;
use std::ffi::CString;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{debug, error, info, warn};

/// XDP program manager
///
/// Handles loading, attaching, and managing XDP BPF programs.
/// The actual BPF bytecode would be compiled separately and loaded here.
pub struct XdpProgram {
    /// Interface name
    interface: String,

    /// Interface index
    if_index: u32,

    /// Current attach mode
    mode: XdpMode,

    /// Whether program is attached
    attached: AtomicBool,

    /// BPF program file descriptor (when attached)
    prog_fd: Option<i32>,

    /// BPF maps for configuration and stats
    maps: HashMap<String, BpfMap>,

    /// Program statistics
    stats: Arc<XdpProgramStats>,
}

/// BPF map handle
#[derive(Debug)]
pub struct BpfMap {
    /// Map name
    pub name: String,

    /// Map file descriptor
    pub fd: i32,

    /// Map type
    pub map_type: BpfMapType,

    /// Key size in bytes
    pub key_size: u32,

    /// Value size in bytes
    pub value_size: u32,

    /// Maximum entries
    pub max_entries: u32,
}

/// BPF map types used by XDP programs
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BpfMapType {
    /// Hash map
    Hash,
    /// Array
    Array,
    /// Per-CPU hash map
    PerCpuHash,
    /// Per-CPU array
    PerCpuArray,
    /// LRU hash map
    LruHash,
    /// XSK (AF_XDP socket) map
    XskMap,
    /// Device map for XDP redirect
    DevMap,
    /// CPU redirect map
    CpuMap,
}

/// XDP program statistics
#[derive(Debug, Default)]
pub struct XdpProgramStats {
    /// Packets received
    pub rx_packets: AtomicU64,
    /// Packets passed to userspace
    pub rx_pass: AtomicU64,
    /// Packets dropped
    pub rx_drop: AtomicU64,
    /// Packets redirected to AF_XDP
    pub rx_redirect: AtomicU64,
    /// Packets transmitted
    pub tx_packets: AtomicU64,
    /// Processing errors
    pub errors: AtomicU64,
}

impl XdpProgramStats {
    /// Create new stats instance
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Record a packet received
    pub fn record_rx(&self) {
        self.rx_packets.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a packet passed to stack
    pub fn record_pass(&self) {
        self.rx_pass.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a packet dropped
    pub fn record_drop(&self) {
        self.rx_drop.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a packet redirected
    pub fn record_redirect(&self) {
        self.rx_redirect.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a packet transmitted
    pub fn record_tx(&self) {
        self.tx_packets.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an error
    pub fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Get snapshot of stats
    pub fn snapshot(&self) -> XdpProgramStatsSnapshot {
        XdpProgramStatsSnapshot {
            rx_packets: self.rx_packets.load(Ordering::Relaxed),
            rx_pass: self.rx_pass.load(Ordering::Relaxed),
            rx_drop: self.rx_drop.load(Ordering::Relaxed),
            rx_redirect: self.rx_redirect.load(Ordering::Relaxed),
            tx_packets: self.tx_packets.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of XDP program statistics
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct XdpProgramStatsSnapshot {
    pub rx_packets: u64,
    pub rx_pass: u64,
    pub rx_drop: u64,
    pub rx_redirect: u64,
    pub tx_packets: u64,
    pub errors: u64,
}

impl XdpProgram {
    /// Create a new XDP program instance
    pub fn new(interface: &str, mode: XdpMode) -> XdpResult<Self> {
        let if_index = Self::get_interface_index(interface)?;

        Ok(Self {
            interface: interface.to_string(),
            if_index,
            mode,
            attached: AtomicBool::new(false),
            prog_fd: None,
            maps: HashMap::new(),
            stats: XdpProgramStats::new(),
        })
    }

    /// Get interface index from name
    fn get_interface_index(interface: &str) -> XdpResult<u32> {
        let path = format!("/sys/class/net/{}/ifindex", interface);
        std::fs::read_to_string(&path)
            .map_err(|_| XdpError::InterfaceNotFound(interface.to_string()))?
            .trim()
            .parse()
            .map_err(|_| XdpError::InterfaceNotFound(interface.to_string()))
    }

    /// Load and attach the XDP program
    ///
    /// In a production implementation, this would:
    /// 1. Load pre-compiled BPF bytecode (or compile with BCC/libbpf)
    /// 2. Create necessary BPF maps
    /// 3. Attach the program to the interface
    pub fn attach(&mut self) -> XdpResult<()> {
        if self.attached.load(Ordering::SeqCst) {
            return Ok(());
        }

        info!(
            interface = %self.interface,
            mode = %self.mode,
            "Attaching XDP program"
        );

        // In a real implementation, we would:
        // 1. Load BPF program bytecode
        // 2. Create XSK map for AF_XDP redirection
        // 3. Call bpf_xdp_attach() via libbpf

        // Simulate BPF map creation
        self.create_xsk_map()?;
        self.create_stats_map()?;
        self.create_filter_map()?;

        // Simulate program attachment
        self.prog_fd = Some(self.simulate_bpf_load()?);
        self.simulate_xdp_attach()?;

        self.attached.store(true, Ordering::SeqCst);

        info!(
            interface = %self.interface,
            mode = %self.mode,
            if_index = self.if_index,
            "XDP program attached successfully"
        );

        Ok(())
    }

    /// Detach the XDP program
    pub fn detach(&mut self) -> XdpResult<()> {
        if !self.attached.load(Ordering::SeqCst) {
            return Ok(());
        }

        info!(interface = %self.interface, "Detaching XDP program");

        // In a real implementation, call bpf_xdp_detach()
        self.simulate_xdp_detach()?;

        // Close BPF maps
        self.maps.clear();
        self.prog_fd = None;
        self.attached.store(false, Ordering::SeqCst);

        info!(interface = %self.interface, "XDP program detached");
        Ok(())
    }

    /// Check if program is attached
    pub fn is_attached(&self) -> bool {
        self.attached.load(Ordering::SeqCst)
    }

    /// Get program statistics
    pub fn stats(&self) -> Arc<XdpProgramStats> {
        Arc::clone(&self.stats)
    }

    /// Get the XSK map file descriptor for AF_XDP socket registration
    pub fn xsk_map_fd(&self) -> Option<i32> {
        self.maps.get("xsk_map").map(|m| m.fd)
    }

    /// Register an AF_XDP socket with the XSK map
    pub fn register_xsk(&self, queue_id: u32, socket_fd: i32) -> XdpResult<()> {
        let xsk_map = self
            .maps
            .get("xsk_map")
            .ok_or_else(|| XdpError::ProgramLoad("XSK map not created".to_string()))?;

        debug!(
            queue_id = queue_id,
            socket_fd = socket_fd,
            "Registering AF_XDP socket with XSK map"
        );

        // In real implementation: bpf_map_update_elem(xsk_map.fd, &queue_id, &socket_fd, 0)
        Ok(())
    }

    /// Update filter rules in the BPF map
    pub fn update_filter(&self, port: u16, allow: bool) -> XdpResult<()> {
        let filter_map = self
            .maps
            .get("filter_map")
            .ok_or_else(|| XdpError::ProgramLoad("Filter map not created".to_string()))?;

        debug!(port = port, allow = allow, "Updating XDP filter");

        // In real implementation: bpf_map_update_elem(filter_map.fd, &port, &allow, 0)
        Ok(())
    }

    /// Get current mode
    pub fn mode(&self) -> XdpMode {
        self.mode
    }

    /// Get interface name
    pub fn interface(&self) -> &str {
        &self.interface
    }

    // --- Simulation methods for development ---
    // In production, these would call actual libbpf functions

    fn create_xsk_map(&mut self) -> XdpResult<()> {
        // XSK map for AF_XDP socket redirection
        let map = BpfMap {
            name: "xsk_map".to_string(),
            fd: 100, // Simulated FD
            map_type: BpfMapType::XskMap,
            key_size: 4,     // u32 queue_id
            value_size: 4,   // i32 socket_fd
            max_entries: 64, // Max RX queues
        };
        self.maps.insert("xsk_map".to_string(), map);
        Ok(())
    }

    fn create_stats_map(&mut self) -> XdpResult<()> {
        // Per-CPU stats array
        let map = BpfMap {
            name: "stats_map".to_string(),
            fd: 101, // Simulated FD
            map_type: BpfMapType::PerCpuArray,
            key_size: 4,
            value_size: 48, // 6 x u64 counters
            max_entries: 1,
        };
        self.maps.insert("stats_map".to_string(), map);
        Ok(())
    }

    fn create_filter_map(&mut self) -> XdpResult<()> {
        // Port filter hash map
        let map = BpfMap {
            name: "filter_map".to_string(),
            fd: 102, // Simulated FD
            map_type: BpfMapType::Hash,
            key_size: 2,   // u16 port
            value_size: 1, // bool allow
            max_entries: 256,
        };
        self.maps.insert("filter_map".to_string(), map);
        Ok(())
    }

    fn simulate_bpf_load(&self) -> XdpResult<i32> {
        // In production: bpf_prog_load()
        debug!("Simulating BPF program load");
        Ok(200) // Simulated program FD
    }

    fn simulate_xdp_attach(&self) -> XdpResult<()> {
        // In production: bpf_xdp_attach(if_index, prog_fd, flags, NULL)
        let flags = match self.mode {
            XdpMode::Skb => 2,     // XDP_FLAGS_SKB_MODE
            XdpMode::Native => 4,  // XDP_FLAGS_DRV_MODE
            XdpMode::Offload => 8, // XDP_FLAGS_HW_MODE
        };
        debug!(
            if_index = self.if_index,
            flags = flags,
            "Simulating XDP attach"
        );
        Ok(())
    }

    fn simulate_xdp_detach(&self) -> XdpResult<()> {
        // In production: bpf_xdp_detach(if_index, flags, NULL)
        debug!(if_index = self.if_index, "Simulating XDP detach");
        Ok(())
    }
}

impl Drop for XdpProgram {
    fn drop(&mut self) {
        if self.is_attached() {
            if let Err(e) = self.detach() {
                error!(error = %e, "Failed to detach XDP program on drop");
            }
        }
    }
}

/// XDP program skeleton for Kafka protocol processing
///
/// This represents the BPF program logic that would run in the kernel.
/// The actual BPF code would be written in C or Rust (with redbpf/aya).
#[derive(Debug)]
pub struct KafkaXdpProgram {
    /// Target port to filter
    pub target_port: u16,
    /// Whether to process established connections only
    pub established_only: bool,
    /// Rate limit per source IP (pps, 0 = disabled)
    pub rate_limit_pps: u32,
}

impl KafkaXdpProgram {
    /// Generate the BPF program bytecode
    ///
    /// In production, this would either:
    /// 1. Return pre-compiled eBPF bytecode
    /// 2. Compile from C source using BCC
    /// 3. Generate using aya or redbpf
    pub fn generate_bytecode(&self) -> Vec<u8> {
        // This would contain actual eBPF bytecode
        // For now, return empty to indicate simulation mode
        Vec::new()
    }

    /// Get the expected BPF program section name
    pub fn section_name(&self) -> &'static str {
        "xdp/kafka_filter"
    }
}

impl Default for KafkaXdpProgram {
    fn default() -> Self {
        Self {
            target_port: 9092,
            established_only: false,
            rate_limit_pps: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_xdp_program_stats() {
        let stats = XdpProgramStats::new();

        stats.record_rx();
        stats.record_rx();
        stats.record_pass();
        stats.record_drop();
        stats.record_redirect();
        stats.record_tx();
        stats.record_error();

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.rx_packets, 2);
        assert_eq!(snapshot.rx_pass, 1);
        assert_eq!(snapshot.rx_drop, 1);
        assert_eq!(snapshot.rx_redirect, 1);
        assert_eq!(snapshot.tx_packets, 1);
        assert_eq!(snapshot.errors, 1);
    }

    #[test]
    fn test_kafka_xdp_program_default() {
        let prog = KafkaXdpProgram::default();
        assert_eq!(prog.target_port, 9092);
        assert!(!prog.established_only);
        assert_eq!(prog.rate_limit_pps, 0);
    }

    #[test]
    fn test_bpf_map_type() {
        let map = BpfMap {
            name: "test".to_string(),
            fd: 1,
            map_type: BpfMapType::XskMap,
            key_size: 4,
            value_size: 4,
            max_entries: 64,
        };
        assert_eq!(map.map_type, BpfMapType::XskMap);
    }
}
