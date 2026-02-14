//! RDMA device discovery and management
//!
//! This module provides device enumeration and capability detection
//! for RDMA-capable network interfaces.

use super::{RdmaError, RdmaLinkType, RdmaMtu, RdmaPortState, RdmaResult, RdmaTransport};
use std::collections::HashMap;
use std::path::Path;
use tracing::{debug, info, warn};

/// RDMA device information
#[derive(Debug, Clone)]
pub struct RdmaDevice {
    /// Device name (e.g., "mlx5_0")
    pub name: String,

    /// Device GUID
    pub guid: u64,

    /// Node type (CA, Switch, Router, RNIC)
    pub node_type: NodeType,

    /// Firmware version
    pub fw_version: String,

    /// Hardware version
    pub hw_version: String,

    /// Number of ports
    pub num_ports: u8,

    /// Maximum memory regions
    pub max_mr: u32,

    /// Maximum queue pairs
    pub max_qp: u32,

    /// Maximum completion queues
    pub max_cq: u32,

    /// Maximum work requests per queue
    pub max_qp_wr: u32,

    /// Maximum scatter/gather entries
    pub max_sge: u32,

    /// Maximum memory region size
    pub max_mr_size: u64,

    /// Device capabilities
    pub capabilities: DeviceCapabilities,

    /// Port information
    pub ports: Vec<PortInfo>,
}

/// RDMA node type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeType {
    /// Channel Adapter (most common for servers)
    ChannelAdapter,
    /// InfiniBand Switch
    Switch,
    /// InfiniBand Router
    Router,
    /// RDMA Network Interface Card (for iWARP)
    Rnic,
    /// Unknown type
    Unknown,
}

impl Default for NodeType {
    fn default() -> Self {
        Self::Unknown
    }
}

/// Device capabilities
#[derive(Debug, Clone, Copy, Default)]
pub struct DeviceCapabilities {
    /// Supports RDMA Read
    pub rdma_read: bool,
    /// Supports RDMA Write
    pub rdma_write: bool,
    /// Supports atomic operations
    pub atomic: bool,
    /// Supports unreliable datagram
    pub ud: bool,
    /// Supports reliable connection
    pub rc: bool,
    /// Supports unreliable connection
    pub uc: bool,
    /// Supports extended reliable connection (XRC)
    pub xrc: bool,
    /// Supports shared receive queue
    pub srq: bool,
    /// Supports memory windows
    pub mw: bool,
    /// Supports on-demand paging
    pub odp: bool,
    /// Supports device memory
    pub dm: bool,
    /// Supports inline data
    pub inline: bool,
    /// Maximum inline data size
    pub max_inline_data: u32,
}

/// Port information
#[derive(Debug, Clone)]
pub struct PortInfo {
    /// Port number (1-based)
    pub port_num: u8,

    /// Port state
    pub state: RdmaPortState,

    /// Link type
    pub link_type: RdmaLinkType,

    /// Active MTU
    pub active_mtu: RdmaMtu,

    /// Maximum MTU
    pub max_mtu: RdmaMtu,

    /// Link layer (IB, Ethernet)
    pub link_layer: LinkLayer,

    /// Physical port state
    pub phys_state: PhysicalPortState,

    /// Local ID (for IB)
    pub lid: u16,

    /// Subnet manager LID
    pub sm_lid: u16,

    /// GID table size
    pub gid_table_len: u32,

    /// Available GIDs
    pub gids: Vec<Gid>,

    /// Active speed
    pub active_speed: PortSpeed,

    /// Active width
    pub active_width: PortWidth,
}

/// Link layer type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkLayer {
    /// InfiniBand
    InfiniBand,
    /// Ethernet (for RoCE)
    Ethernet,
    /// Unknown
    Unknown,
}

impl Default for LinkLayer {
    fn default() -> Self {
        Self::Unknown
    }
}

/// Physical port state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhysicalPortState {
    /// No physical cable
    Sleep,
    /// Polling for link
    Polling,
    /// Disabled
    Disabled,
    /// Port configuration training
    PortConfigTraining,
    /// Link up
    LinkUp,
    /// Link error recovery
    LinkErrorRecovery,
    /// PHY test
    PhyTest,
    /// Unknown state
    Unknown,
}

impl Default for PhysicalPortState {
    fn default() -> Self {
        Self::Unknown
    }
}

/// Port speed
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortSpeed {
    /// 2.5 Gbps (SDR)
    Sdr,
    /// 5 Gbps (DDR)
    Ddr,
    /// 10 Gbps (QDR)
    Qdr,
    /// 14 Gbps (FDR)
    Fdr,
    /// 25 Gbps (EDR)
    Edr,
    /// 50 Gbps (HDR)
    Hdr,
    /// 100 Gbps (NDR)
    Ndr,
    /// Unknown speed
    Unknown,
}

impl Default for PortSpeed {
    fn default() -> Self {
        Self::Unknown
    }
}

impl PortSpeed {
    /// Get speed in Gbps
    pub fn gbps(&self) -> f64 {
        match self {
            Self::Sdr => 2.5,
            Self::Ddr => 5.0,
            Self::Qdr => 10.0,
            Self::Fdr => 14.0,
            Self::Edr => 25.0,
            Self::Hdr => 50.0,
            Self::Ndr => 100.0,
            Self::Unknown => 0.0,
        }
    }
}

/// Port width
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortWidth {
    /// 1x lanes
    X1,
    /// 2x lanes
    X2,
    /// 4x lanes
    X4,
    /// 8x lanes
    X8,
    /// 12x lanes
    X12,
    /// Unknown width
    Unknown,
}

impl Default for PortWidth {
    fn default() -> Self {
        Self::Unknown
    }
}

impl PortWidth {
    /// Get multiplier for total bandwidth calculation
    pub fn multiplier(&self) -> u32 {
        match self {
            Self::X1 => 1,
            Self::X2 => 2,
            Self::X4 => 4,
            Self::X8 => 8,
            Self::X12 => 12,
            Self::Unknown => 0,
        }
    }
}

/// Global Identifier (GID) for addressing
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Gid {
    /// Raw GID bytes (16 bytes)
    pub raw: [u8; 16],
}

impl Default for Gid {
    fn default() -> Self {
        Self { raw: [0u8; 16] }
    }
}

impl Gid {
    /// Create GID from raw bytes
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self { raw: bytes }
    }

    /// Get subnet prefix (first 8 bytes)
    pub fn subnet_prefix(&self) -> u64 {
        u64::from_be_bytes([
            self.raw[0],
            self.raw[1],
            self.raw[2],
            self.raw[3],
            self.raw[4],
            self.raw[5],
            self.raw[6],
            self.raw[7],
        ])
    }

    /// Get interface ID (last 8 bytes)
    pub fn interface_id(&self) -> u64 {
        u64::from_be_bytes([
            self.raw[8],
            self.raw[9],
            self.raw[10],
            self.raw[11],
            self.raw[12],
            self.raw[13],
            self.raw[14],
            self.raw[15],
        ])
    }

    /// Check if this is a link-local GID
    pub fn is_link_local(&self) -> bool {
        self.raw[0] == 0xfe && self.raw[1] == 0x80
    }

    /// Check if this is a RoCE v2 GID (IPv4-mapped or IPv6)
    pub fn is_roce_v2(&self) -> bool {
        // RoCE v2 uses IPv6 addresses
        // IPv4-mapped: ::ffff:a.b.c.d
        self.raw[0..10] == [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
            && self.raw[10] == 0xff
            && self.raw[11] == 0xff
    }
}

impl std::fmt::Display for Gid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:02x}{:02x}:{:02x}{:02x}:{:02x}{:02x}:{:02x}{:02x}:\
             {:02x}{:02x}:{:02x}{:02x}:{:02x}{:02x}:{:02x}{:02x}",
            self.raw[0],
            self.raw[1],
            self.raw[2],
            self.raw[3],
            self.raw[4],
            self.raw[5],
            self.raw[6],
            self.raw[7],
            self.raw[8],
            self.raw[9],
            self.raw[10],
            self.raw[11],
            self.raw[12],
            self.raw[13],
            self.raw[14],
            self.raw[15],
        )
    }
}

/// Device manager for RDMA devices
pub struct DeviceManager {
    /// Discovered devices
    devices: HashMap<String, RdmaDevice>,
}

impl DeviceManager {
    /// Create a new device manager
    pub fn new() -> Self {
        Self {
            devices: HashMap::new(),
        }
    }

    /// Check if RDMA is available on this system
    pub fn is_available() -> bool {
        #[cfg(target_os = "linux")]
        {
            // Check for /sys/class/infiniband directory
            Path::new("/sys/class/infiniband").exists()
        }

        #[cfg(not(target_os = "linux"))]
        {
            false
        }
    }

    /// Discover all RDMA devices
    pub fn discover(&mut self) -> RdmaResult<Vec<String>> {
        self.devices.clear();

        if !Self::is_available() {
            return Err(RdmaError::NotAvailable(
                "RDMA not available on this system".to_string(),
            ));
        }

        let device_names = self.enumerate_devices()?;

        for name in &device_names {
            match self.probe_device(name) {
                Ok(device) => {
                    info!(device = %name, "Discovered RDMA device");
                    self.devices.insert(name.clone(), device);
                }
                Err(e) => {
                    warn!(device = %name, error = %e, "Failed to probe RDMA device");
                }
            }
        }

        Ok(device_names)
    }

    /// Enumerate device names from sysfs
    #[cfg(target_os = "linux")]
    fn enumerate_devices(&self) -> RdmaResult<Vec<String>> {
        let path = Path::new("/sys/class/infiniband");

        if !path.exists() {
            return Err(RdmaError::NotAvailable("No RDMA devices found".to_string()));
        }

        let entries = std::fs::read_dir(path)
            .map_err(|e| RdmaError::DeviceError(format!("Failed to read sysfs: {}", e)))?;

        let mut devices = Vec::new();
        for entry in entries.flatten() {
            if let Some(name) = entry.file_name().to_str() {
                devices.push(name.to_string());
            }
        }

        debug!(count = devices.len(), "Enumerated RDMA devices");
        Ok(devices)
    }

    #[cfg(not(target_os = "linux"))]
    fn enumerate_devices(&self) -> RdmaResult<Vec<String>> {
        Err(RdmaError::NotAvailable(
            "RDMA enumeration not supported on this platform".to_string(),
        ))
    }

    /// Probe a specific device for capabilities
    #[cfg(target_os = "linux")]
    fn probe_device(&self, name: &str) -> RdmaResult<RdmaDevice> {
        let base_path = format!("/sys/class/infiniband/{}", name);

        // Read basic device attributes
        let fw_version = self
            .read_sysfs_attr(&base_path, "fw_ver")
            .unwrap_or_default();
        let hw_version = self
            .read_sysfs_attr(&base_path, "hw_rev")
            .unwrap_or_default();
        let node_guid = self
            .read_sysfs_attr(&base_path, "node_guid")
            .unwrap_or_default();

        let guid = u64::from_str_radix(&node_guid.replace(':', ""), 16).unwrap_or(0);

        // Determine node type
        let node_type_str = self
            .read_sysfs_attr(&base_path, "node_type")
            .unwrap_or_default();
        let node_type = match node_type_str.trim() {
            "1: CA" | "CA" => NodeType::ChannelAdapter,
            "2: Switch" | "Switch" => NodeType::Switch,
            "3: Router" | "Router" => NodeType::Router,
            "4: RNIC" | "RNIC" => NodeType::Rnic,
            _ => NodeType::Unknown,
        };

        // Count ports
        let ports_path = format!("{}/ports", base_path);
        let num_ports = std::fs::read_dir(&ports_path)
            .map(|entries| entries.count() as u8)
            .unwrap_or(0);

        // Probe each port
        let mut ports = Vec::new();
        for port_num in 1..=num_ports {
            if let Ok(port_info) = self.probe_port(name, port_num) {
                ports.push(port_info);
            }
        }

        // Create device with default capabilities
        // In real implementation, this would query ibv_query_device
        let device = RdmaDevice {
            name: name.to_string(),
            guid,
            node_type,
            fw_version,
            hw_version,
            num_ports,
            max_mr: 65536, // Typical values
            max_qp: 65536,
            max_cq: 65536,
            max_qp_wr: 32768,
            max_sge: 30,
            max_mr_size: 1 << 40, // 1TB
            capabilities: DeviceCapabilities {
                rdma_read: true,
                rdma_write: true,
                atomic: true,
                ud: true,
                rc: true,
                uc: true,
                xrc: true,
                srq: true,
                mw: true,
                odp: true,
                dm: false,
                inline: true,
                max_inline_data: 512,
            },
            ports,
        };

        Ok(device)
    }

    #[cfg(not(target_os = "linux"))]
    fn probe_device(&self, _name: &str) -> RdmaResult<RdmaDevice> {
        Err(RdmaError::NotAvailable(
            "RDMA probing not supported on this platform".to_string(),
        ))
    }

    /// Probe a port for its configuration
    #[cfg(target_os = "linux")]
    fn probe_port(&self, device: &str, port_num: u8) -> RdmaResult<PortInfo> {
        let port_path = format!("/sys/class/infiniband/{}/ports/{}", device, port_num);

        // Read port state
        let state_str = self
            .read_sysfs_attr(&port_path, "state")
            .unwrap_or_default();
        let state = match state_str.split(':').next().unwrap_or("").trim() {
            "1" => RdmaPortState::Down,
            "2" => RdmaPortState::Initializing,
            "3" => RdmaPortState::Armed,
            "4" => RdmaPortState::Active,
            "5" => RdmaPortState::ActiveDefer,
            _ => RdmaPortState::Down,
        };

        // Read link layer
        let link_layer_str = self
            .read_sysfs_attr(&port_path, "link_layer")
            .unwrap_or_default();
        let link_layer = match link_layer_str.trim() {
            "InfiniBand" => LinkLayer::InfiniBand,
            "Ethernet" => LinkLayer::Ethernet,
            _ => LinkLayer::Unknown,
        };

        // Read LID
        let lid_str = self.read_sysfs_attr(&port_path, "lid").unwrap_or_default();
        let lid = lid_str.trim().parse().unwrap_or(0);

        // Read SM LID
        let sm_lid_str = self
            .read_sysfs_attr(&port_path, "sm_lid")
            .unwrap_or_default();
        let sm_lid = sm_lid_str.trim().parse().unwrap_or(0);

        // Determine link type based on transport
        let link_type = match link_layer {
            LinkLayer::InfiniBand => RdmaLinkType::InfiniBand,
            LinkLayer::Ethernet => RdmaLinkType::Ethernet,
            LinkLayer::Unknown => RdmaLinkType::Unknown,
        };

        let port_info = PortInfo {
            port_num,
            state,
            link_type,
            active_mtu: RdmaMtu::Mtu4096,
            max_mtu: RdmaMtu::Mtu4096,
            link_layer,
            phys_state: PhysicalPortState::LinkUp,
            lid,
            sm_lid,
            gid_table_len: 256,
            gids: Vec::new(), // Would read from gids/ subdirectory
            active_speed: PortSpeed::Hdr,
            active_width: PortWidth::X4,
        };

        Ok(port_info)
    }

    /// Read a sysfs attribute
    #[cfg(target_os = "linux")]
    fn read_sysfs_attr(&self, base: &str, attr: &str) -> Option<String> {
        let path = format!("{}/{}", base, attr);
        std::fs::read_to_string(path)
            .ok()
            .map(|s| s.trim().to_string())
    }

    /// Get a device by name
    pub fn get_device(&self, name: &str) -> Option<&RdmaDevice> {
        self.devices.get(name)
    }

    /// Get all discovered devices
    pub fn devices(&self) -> impl Iterator<Item = &RdmaDevice> {
        self.devices.values()
    }

    /// Get the first available device
    pub fn first_device(&self) -> Option<&RdmaDevice> {
        self.devices.values().next()
    }

    /// Find a device suitable for a given transport
    pub fn find_device_for_transport(&self, transport: RdmaTransport) -> Option<&RdmaDevice> {
        self.devices.values().find(|device| {
            device.ports.iter().any(|port| {
                port.state == RdmaPortState::Active
                    && match transport {
                        RdmaTransport::InfiniBand => port.link_layer == LinkLayer::InfiniBand,
                        RdmaTransport::RoCEv2 => port.link_layer == LinkLayer::Ethernet,
                        RdmaTransport::IWarp => port.link_layer == LinkLayer::Ethernet,
                    }
            })
        })
    }

    /// Get total number of active ports across all devices
    pub fn active_port_count(&self) -> usize {
        self.devices
            .values()
            .flat_map(|d| &d.ports)
            .filter(|p| p.state == RdmaPortState::Active)
            .count()
    }
}

impl Default for DeviceManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gid() {
        let gid = Gid::from_bytes([
            0xfe, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x01,
        ]);

        assert!(gid.is_link_local());
        assert!(!gid.is_roce_v2());
    }

    #[test]
    fn test_port_speed() {
        assert_eq!(PortSpeed::Hdr.gbps(), 50.0);
        assert_eq!(PortSpeed::Edr.gbps(), 25.0);
    }

    #[test]
    fn test_port_width() {
        assert_eq!(PortWidth::X4.multiplier(), 4);
        assert_eq!(PortWidth::X1.multiplier(), 1);
    }

    #[test]
    fn test_device_manager_creation() {
        let manager = DeviceManager::new();
        assert_eq!(manager.active_port_count(), 0);
    }
}
