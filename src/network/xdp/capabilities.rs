//! XDP capabilities detection

use super::{XdpError, XdpMode, XdpResult};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use tracing::{debug, info, warn};

/// XDP system capabilities
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XdpCapabilities {
    /// Kernel version
    pub kernel_version: String,

    /// Whether XDP is supported at all
    pub xdp_supported: bool,

    /// Whether AF_XDP sockets are supported
    pub af_xdp_supported: bool,

    /// Whether BTF (BPF Type Format) is available
    pub btf_available: bool,

    /// Whether BPF JIT is enabled
    pub bpf_jit_enabled: bool,

    /// Whether CAP_NET_ADMIN capability is present
    pub has_net_admin: bool,

    /// Whether CAP_SYS_ADMIN capability is present
    pub has_sys_admin: bool,

    /// Whether CAP_BPF capability is present (Linux 5.8+)
    pub has_cap_bpf: bool,

    /// Per-interface XDP support
    pub interfaces: HashMap<String, InterfaceXdpCapabilities>,

    /// Available XDP modes
    pub available_modes: Vec<XdpMode>,

    /// Maximum XDP frame size
    pub max_frame_size: u32,

    /// Huge page support for UMEM
    pub huge_pages_available: bool,
}

/// Per-interface XDP capabilities
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InterfaceXdpCapabilities {
    /// Interface name
    pub name: String,

    /// Interface index
    pub index: u32,

    /// Driver name
    pub driver: String,

    /// Whether native XDP mode is supported
    pub native_mode: bool,

    /// Whether XDP offload is supported
    pub offload_mode: bool,

    /// Number of RX queues
    pub rx_queues: u32,

    /// Number of TX queues
    pub tx_queues: u32,

    /// Whether XDP redirect is supported
    pub redirect_supported: bool,

    /// Whether AF_XDP zero-copy is supported
    pub zero_copy: bool,

    /// Current link state
    pub link_up: bool,

    /// MTU
    pub mtu: u32,
}

impl XdpCapabilities {
    /// Detect XDP capabilities on the current system
    pub fn detect() -> Self {
        let kernel_version = Self::detect_kernel_version();
        let btf_available = Self::detect_btf();
        let bpf_jit_enabled = Self::detect_bpf_jit();
        let xdp_supported = Self::detect_xdp_support(&kernel_version);
        let af_xdp_supported = Self::detect_af_xdp_support(&kernel_version);
        let has_net_admin = Self::check_capability("net_admin");
        let has_sys_admin = Self::check_capability("sys_admin");
        let has_cap_bpf = Self::check_capability("bpf");
        let huge_pages_available = Self::detect_huge_pages();
        let interfaces = Self::detect_interface_capabilities();

        let available_modes = Self::determine_available_modes(&interfaces);
        let max_frame_size = Self::detect_max_frame_size();

        let caps = Self {
            kernel_version,
            xdp_supported,
            af_xdp_supported,
            btf_available,
            bpf_jit_enabled,
            has_net_admin,
            has_sys_admin,
            has_cap_bpf,
            interfaces,
            available_modes,
            max_frame_size,
            huge_pages_available,
        };

        debug!(?caps, "Detected XDP capabilities");
        caps
    }

    /// Check if XDP is supported on this system
    pub fn is_supported(&self) -> bool {
        self.xdp_supported && self.af_xdp_supported && (self.has_net_admin || self.has_cap_bpf)
    }

    /// Get the best available XDP mode for an interface
    pub fn best_mode_for(&self, interface: &str) -> Option<XdpMode> {
        self.interfaces.get(interface).and_then(|iface| {
            if iface.offload_mode {
                Some(XdpMode::Offload)
            } else if iface.native_mode {
                Some(XdpMode::Native)
            } else {
                Some(XdpMode::Skb)
            }
        })
    }

    /// Validate that XDP can be used with the given interface
    pub fn validate_interface(&self, interface: &str) -> XdpResult<&InterfaceXdpCapabilities> {
        if !self.is_supported() {
            return Err(XdpError::NotSupported(format!(
                "XDP not supported: kernel={}, af_xdp={}, permissions={}",
                self.xdp_supported,
                self.af_xdp_supported,
                self.has_net_admin || self.has_cap_bpf
            )));
        }

        self.interfaces
            .get(interface)
            .ok_or_else(|| XdpError::InterfaceNotFound(interface.to_string()))
    }

    fn detect_kernel_version() -> String {
        fs::read_to_string("/proc/version")
            .ok()
            .and_then(|v| {
                v.split_whitespace()
                    .nth(2)
                    .map(|s| s.split('-').next().unwrap_or(s).to_string())
            })
            .unwrap_or_else(|| "unknown".to_string())
    }

    fn detect_btf() -> bool {
        Path::new("/sys/kernel/btf/vmlinux").exists()
    }

    fn detect_bpf_jit() -> bool {
        fs::read_to_string("/proc/sys/net/core/bpf_jit_enable")
            .ok()
            .map(|v| v.trim() == "1" || v.trim() == "2")
            .unwrap_or(false)
    }

    fn detect_xdp_support(kernel_version: &str) -> bool {
        // XDP was introduced in kernel 4.8, but AF_XDP needs 4.18+
        let parts: Vec<u32> = kernel_version
            .split('.')
            .take(2)
            .filter_map(|s| s.parse().ok())
            .collect();

        if parts.len() >= 2 {
            let (major, minor) = (parts[0], parts[1]);
            major > 4 || (major == 4 && minor >= 18)
        } else {
            false
        }
    }

    fn detect_af_xdp_support(kernel_version: &str) -> bool {
        // AF_XDP introduced in 4.18
        Self::detect_xdp_support(kernel_version)
    }

    fn check_capability(_cap: &str) -> bool {
        // In a real implementation, we'd use capget() syscall or parse /proc/self/status
        // For now, check if we can access /sys/kernel/debug (requires CAP_SYS_ADMIN)
        // or if we're root
        let uid = unsafe { libc::getuid() };
        if uid == 0 {
            return true;
        }

        // Check /proc/self/status for capabilities
        if let Ok(status) = fs::read_to_string("/proc/self/status") {
            for line in status.lines() {
                if line.starts_with("CapEff:") {
                    if let Some(hex) = line.split_whitespace().nth(1) {
                        if let Ok(caps) = u64::from_str_radix(hex, 16) {
                            // CAP_NET_ADMIN = 12, CAP_SYS_ADMIN = 21, CAP_BPF = 39
                            let net_admin = caps & (1 << 12) != 0;
                            let sys_admin = caps & (1 << 21) != 0;
                            let bpf = caps & (1 << 39) != 0;
                            return net_admin || sys_admin || bpf;
                        }
                    }
                }
            }
        }

        false
    }

    fn detect_huge_pages() -> bool {
        Path::new("/sys/kernel/mm/hugepages").exists()
    }

    fn detect_interface_capabilities() -> HashMap<String, InterfaceXdpCapabilities> {
        let mut interfaces = HashMap::new();

        if let Ok(entries) = fs::read_dir("/sys/class/net") {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().to_string();

                // Skip loopback and virtual interfaces
                if name == "lo" || name.starts_with("veth") || name.starts_with("docker") {
                    continue;
                }

                if let Some(caps) = Self::detect_single_interface(&name) {
                    interfaces.insert(name, caps);
                }
            }
        }

        interfaces
    }

    fn detect_single_interface(name: &str) -> Option<InterfaceXdpCapabilities> {
        let base = format!("/sys/class/net/{}", name);

        let index = fs::read_to_string(format!("{}/ifindex", base))
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(0);

        let driver = fs::read_link(format!("{}/device/driver", base))
            .ok()
            .and_then(|p| p.file_name().map(|s| s.to_string_lossy().to_string()))
            .unwrap_or_else(|| "unknown".to_string());

        let operstate = fs::read_to_string(format!("{}/operstate", base))
            .ok()
            .map(|s| s.trim().to_string())
            .unwrap_or_else(|| "unknown".to_string());

        let mtu = fs::read_to_string(format!("{}/mtu", base))
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(1500);

        // Detect native XDP support based on driver
        let (native_mode, zero_copy) = Self::detect_driver_xdp_support(&driver);

        // Detect number of queues
        let rx_queues = Self::count_queues(&format!("{}/queues", base), "rx-");
        let tx_queues = Self::count_queues(&format!("{}/queues", base), "tx-");

        Some(InterfaceXdpCapabilities {
            name: name.to_string(),
            index,
            driver,
            native_mode,
            offload_mode: false, // Would need to query with ethtool
            rx_queues,
            tx_queues,
            redirect_supported: native_mode,
            zero_copy,
            link_up: operstate == "up",
            mtu,
        })
    }

    fn detect_driver_xdp_support(driver: &str) -> (bool, bool) {
        // Known XDP-capable drivers and their capabilities
        // (native_mode, zero_copy)
        match driver {
            // Intel drivers
            "i40e" | "ice" | "ixgbe" | "ixgbevf" => (true, true),
            "igb" | "igc" => (true, false),

            // Mellanox
            "mlx4_en" | "mlx5_core" => (true, true),

            // AWS
            "ena" => (true, true),

            // Broadcom
            "bnxt_en" => (true, true),

            // Cavium/Marvell
            "thunderx" | "qede" => (true, false),

            // Virtio (VMs)
            "virtio_net" => (true, false),

            // veth (containers)
            "veth" => (true, false),

            _ => (false, false),
        }
    }

    fn count_queues(queues_path: &str, prefix: &str) -> u32 {
        fs::read_dir(queues_path)
            .ok()
            .map(|entries| {
                entries
                    .filter_map(Result::ok)
                    .filter(|e| e.file_name().to_string_lossy().starts_with(prefix))
                    .count() as u32
            })
            .unwrap_or(1)
    }

    fn determine_available_modes(
        interfaces: &HashMap<String, InterfaceXdpCapabilities>,
    ) -> Vec<XdpMode> {
        let mut modes = vec![XdpMode::Skb]; // SKB always available

        if interfaces.values().any(|i| i.native_mode) {
            modes.push(XdpMode::Native);
        }
        if interfaces.values().any(|i| i.offload_mode) {
            modes.push(XdpMode::Offload);
        }

        modes
    }

    fn detect_max_frame_size() -> u32 {
        // Most XDP implementations support at least 4KB frames
        // Some support larger with certain configurations
        4096
    }

    /// Print a summary of XDP capabilities
    pub fn print_summary(&self) {
        info!("XDP Capabilities Summary:");
        info!("  Kernel: {}", self.kernel_version);
        info!("  XDP supported: {}", self.xdp_supported);
        info!("  AF_XDP supported: {}", self.af_xdp_supported);
        info!("  BTF available: {}", self.btf_available);
        info!("  BPF JIT enabled: {}", self.bpf_jit_enabled);
        info!(
            "  Permissions: net_admin={}, sys_admin={}, cap_bpf={}",
            self.has_net_admin, self.has_sys_admin, self.has_cap_bpf
        );
        info!("  Available modes: {:?}", self.available_modes);

        for (name, iface) in &self.interfaces {
            info!(
                "  Interface {}: driver={}, native={}, zero_copy={}, queues={}rx/{}tx",
                name,
                iface.driver,
                iface.native_mode,
                iface.zero_copy,
                iface.rx_queues,
                iface.tx_queues
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_capabilities_detection() {
        let caps = XdpCapabilities::detect();
        // Should at least have a kernel version
        assert!(!caps.kernel_version.is_empty());
    }

    #[test]
    fn test_driver_xdp_support() {
        let (native, zc) = XdpCapabilities::detect_driver_xdp_support("i40e");
        assert!(native);
        assert!(zc);

        let (native, zc) = XdpCapabilities::detect_driver_xdp_support("unknown_driver");
        assert!(!native);
        assert!(!zc);
    }

    #[test]
    fn test_kernel_version_parsing() {
        // XDP support detection
        assert!(XdpCapabilities::detect_xdp_support("5.4.0"));
        assert!(XdpCapabilities::detect_xdp_support("4.18.0"));
        assert!(!XdpCapabilities::detect_xdp_support("4.17.0"));
        assert!(!XdpCapabilities::detect_xdp_support("3.10.0"));
    }

    #[test]
    fn test_best_mode_for() {
        let mut interfaces = HashMap::new();
        interfaces.insert(
            "eth0".to_string(),
            InterfaceXdpCapabilities {
                name: "eth0".to_string(),
                index: 2,
                driver: "i40e".to_string(),
                native_mode: true,
                offload_mode: false,
                rx_queues: 16,
                tx_queues: 16,
                redirect_supported: true,
                zero_copy: true,
                link_up: true,
                mtu: 1500,
            },
        );

        let caps = XdpCapabilities {
            kernel_version: "5.4.0".to_string(),
            xdp_supported: true,
            af_xdp_supported: true,
            btf_available: true,
            bpf_jit_enabled: true,
            has_net_admin: true,
            has_sys_admin: true,
            has_cap_bpf: true,
            interfaces,
            available_modes: vec![XdpMode::Skb, XdpMode::Native],
            max_frame_size: 4096,
            huge_pages_available: true,
        };

        assert_eq!(caps.best_mode_for("eth0"), Some(XdpMode::Native));
        assert_eq!(caps.best_mode_for("eth1"), None);
    }
}
