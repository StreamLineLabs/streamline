//! DPDK EAL (Environment Abstraction Layer) management

use super::{DpdkError, DpdkResult, EalConfig, HugePageSize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Once;
use tracing::{debug, error, info, warn};

/// Global EAL initialization flag
static EAL_INITIALIZED: AtomicBool = AtomicBool::new(false);
static EAL_INIT: Once = Once::new();

/// DPDK EAL (Environment Abstraction Layer)
///
/// Manages DPDK initialization and system resources.
/// EAL can only be initialized once per process.
pub struct Eal {
    /// Configuration
    config: EalConfig,

    /// Number of available lcores
    lcore_count: AtomicU32,

    /// Detected DPDK ports
    detected_ports: Vec<EalPortInfo>,

    /// Huge page info
    huge_pages: HugePageInfo,

    /// Whether EAL is initialized
    initialized: bool,
}

/// Port information detected by EAL
#[derive(Debug, Clone)]
pub struct EalPortInfo {
    /// Port ID
    pub port_id: u16,

    /// PCI address (e.g., "0000:00:1f.0")
    pub pci_addr: String,

    /// Driver name
    pub driver: String,

    /// MAC address
    pub mac_addr: [u8; 6],

    /// Maximum RX queues
    pub max_rx_queues: u16,

    /// Maximum TX queues
    pub max_tx_queues: u16,

    /// Supported link speeds
    pub speed_capabilities: u32,

    /// Device name
    pub device_name: String,
}

impl EalPortInfo {
    /// Format MAC address as string
    pub fn mac_address_string(&self) -> String {
        format!(
            "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
            self.mac_addr[0],
            self.mac_addr[1],
            self.mac_addr[2],
            self.mac_addr[3],
            self.mac_addr[4],
            self.mac_addr[5]
        )
    }
}

/// Huge page information
#[derive(Debug, Clone, Default)]
pub struct HugePageInfo {
    /// Total huge pages available
    pub total_pages: u32,

    /// Free huge pages
    pub free_pages: u32,

    /// Huge page size
    pub page_size: usize,

    /// Total memory in bytes
    pub total_memory: usize,

    /// Free memory in bytes
    pub free_memory: usize,

    /// Huge page mount point
    pub mount_point: String,
}

impl Eal {
    /// Check if EAL has been initialized
    pub fn is_initialized() -> bool {
        EAL_INITIALIZED.load(Ordering::SeqCst)
    }

    /// Initialize EAL with the given configuration
    ///
    /// This can only be called once per process. Subsequent calls will fail.
    pub fn init(config: EalConfig) -> DpdkResult<Self> {
        if EAL_INITIALIZED.load(Ordering::SeqCst) {
            return Err(DpdkError::EalInit("EAL already initialized".to_string()));
        }

        // Validate configuration
        config.validate().map_err(DpdkError::Config)?;

        // Check huge page availability
        let huge_pages = Self::detect_huge_pages(&config.huge_dir, config.huge_page_size)?;

        if huge_pages.free_pages < config.huge_pages {
            return Err(DpdkError::Memory(format!(
                "Insufficient huge pages: need {}, have {} free",
                config.huge_pages, huge_pages.free_pages
            )));
        }

        // Parse cores
        let lcore_count = Self::count_cores(&config.cores)?;

        info!(
            cores = %config.cores,
            huge_pages = huge_pages.total_pages,
            "Initializing DPDK EAL"
        );

        // In production, this would call rte_eal_init() with the arguments
        let args = config.to_args();
        debug!(args = ?args, "EAL arguments");

        // Simulate EAL initialization
        let detected_ports = Self::detect_dpdk_ports(&config)?;

        // Mark as initialized
        EAL_INIT.call_once(|| {
            EAL_INITIALIZED.store(true, Ordering::SeqCst);
        });

        info!(
            lcore_count = lcore_count,
            port_count = detected_ports.len(),
            "DPDK EAL initialized"
        );

        Ok(Self {
            config,
            lcore_count: AtomicU32::new(lcore_count),
            detected_ports,
            huge_pages,
            initialized: true,
        })
    }

    /// Get number of lcores
    pub fn lcore_count(&self) -> u32 {
        self.lcore_count.load(Ordering::Relaxed)
    }

    /// Get detected ports
    pub fn ports(&self) -> &[EalPortInfo] {
        &self.detected_ports
    }

    /// Get port by ID
    pub fn port(&self, port_id: u16) -> Option<&EalPortInfo> {
        self.detected_ports.iter().find(|p| p.port_id == port_id)
    }

    /// Get huge page info
    pub fn huge_pages(&self) -> &HugePageInfo {
        &self.huge_pages
    }

    /// Get configuration
    pub fn config(&self) -> &EalConfig {
        &self.config
    }

    /// Cleanup EAL resources
    pub fn cleanup(&mut self) -> DpdkResult<()> {
        if !self.initialized {
            return Ok(());
        }

        info!("Cleaning up DPDK EAL");

        // In production, this would call rte_eal_cleanup()
        self.initialized = false;

        Ok(())
    }

    /// Parse core list and count cores
    fn count_cores(cores: &str) -> DpdkResult<u32> {
        let mut count = 0;

        for part in cores.split(',') {
            let part = part.trim();
            if part.contains('-') {
                // Range like "0-3"
                let range: Vec<&str> = part.split('-').collect();
                if range.len() != 2 {
                    return Err(DpdkError::Config(format!("Invalid core range: {}", part)));
                }
                let start: u32 = range[0]
                    .parse()
                    .map_err(|_| DpdkError::Config(format!("Invalid core: {}", range[0])))?;
                let end: u32 = range[1]
                    .parse()
                    .map_err(|_| DpdkError::Config(format!("Invalid core: {}", range[1])))?;
                count += end - start + 1;
            } else {
                // Single core like "0"
                let _: u32 = part
                    .parse()
                    .map_err(|_| DpdkError::Config(format!("Invalid core: {}", part)))?;
                count += 1;
            }
        }

        if count == 0 {
            return Err(DpdkError::Config(
                "At least one core must be specified".to_string(),
            ));
        }

        Ok(count)
    }

    /// Detect available huge pages
    fn detect_huge_pages(huge_dir: &str, page_size: HugePageSize) -> DpdkResult<HugePageInfo> {
        let mut info = HugePageInfo {
            page_size: page_size.bytes(),
            mount_point: huge_dir.to_string(),
            ..Default::default()
        };

        // Check if huge pages are mounted
        if !Path::new(huge_dir).exists() {
            warn!(path = huge_dir, "Huge page mount point does not exist");
            // Return default (simulated for development)
            info.total_pages = 1024;
            info.free_pages = 1024;
            info.total_memory = info.total_pages as usize * info.page_size;
            info.free_memory = info.free_pages as usize * info.page_size;
            return Ok(info);
        }

        // Read huge page info from sysfs
        let size_dir = match page_size {
            HugePageSize::Size2MB => "hugepages-2048kB",
            HugePageSize::Size1GB => "hugepages-1048576kB",
        };

        let base_path = format!("/sys/kernel/mm/hugepages/{}", size_dir);

        if let Ok(total) = fs::read_to_string(format!("{}/nr_hugepages", base_path)) {
            info.total_pages = total.trim().parse().unwrap_or(0);
        }

        if let Ok(free) = fs::read_to_string(format!("{}/free_hugepages", base_path)) {
            info.free_pages = free.trim().parse().unwrap_or(0);
        }

        info.total_memory = info.total_pages as usize * info.page_size;
        info.free_memory = info.free_pages as usize * info.page_size;

        debug!(
            total = info.total_pages,
            free = info.free_pages,
            size = info.page_size,
            "Detected huge pages"
        );

        Ok(info)
    }

    /// Detect DPDK-compatible ports
    fn detect_dpdk_ports(config: &EalConfig) -> DpdkResult<Vec<EalPortInfo>> {
        let mut ports = Vec::new();

        // In production, this would call rte_eth_dev_count_avail()
        // and iterate through available ports

        // Simulate port detection based on PCI whitelist
        for (idx, pci) in config.pci_whitelist.iter().enumerate() {
            ports.push(EalPortInfo {
                port_id: idx as u16,
                pci_addr: pci.clone(),
                driver: "net_ixgbe".to_string(), // Simulated
                mac_addr: [0x00, 0x11, 0x22, 0x33, 0x44, idx as u8],
                max_rx_queues: 16,
                max_tx_queues: 16,
                speed_capabilities: 0x0F, // 10G capable
                device_name: format!("dpdk{}", idx),
            });
        }

        // If no whitelist, detect from sysfs
        if ports.is_empty() {
            if let Ok(entries) = fs::read_dir("/sys/class/net") {
                for (idx, entry) in entries.flatten().enumerate() {
                    let name = entry.file_name().to_string_lossy().to_string();

                    // Skip virtual interfaces
                    if name.starts_with("lo")
                        || name.starts_with("docker")
                        || name.starts_with("veth")
                    {
                        continue;
                    }

                    // Read device info
                    let pci = fs::read_link(format!("/sys/class/net/{}/device", name))
                        .ok()
                        .and_then(|p| p.file_name().map(|s| s.to_string_lossy().to_string()))
                        .unwrap_or_default();

                    if !pci.is_empty() {
                        ports.push(EalPortInfo {
                            port_id: idx as u16,
                            pci_addr: pci,
                            driver: "unknown".to_string(),
                            mac_addr: [0x00, 0x00, 0x00, 0x00, 0x00, idx as u8],
                            max_rx_queues: 8,
                            max_tx_queues: 8,
                            speed_capabilities: 0x0F,
                            device_name: name,
                        });
                    }
                }
            }
        }

        debug!(count = ports.len(), "Detected DPDK-compatible ports");
        Ok(ports)
    }
}

impl Drop for Eal {
    fn drop(&mut self) {
        if self.initialized {
            if let Err(e) = self.cleanup() {
                error!(error = %e, "Failed to cleanup EAL");
            }
        }
    }
}

/// Check DPDK system requirements
pub fn check_requirements() -> DpdkRequirements {
    DpdkRequirements {
        huge_pages_available: check_huge_pages(),
        vfio_available: check_vfio(),
        uio_available: check_uio(),
        is_root: check_root(),
        has_net_admin: check_capability("net_admin"),
        iommu_enabled: check_iommu(),
        compatible_nics: detect_compatible_nics(),
    }
}

/// DPDK system requirements check result
#[derive(Debug, Clone)]
pub struct DpdkRequirements {
    /// Huge pages are available and mounted
    pub huge_pages_available: bool,
    /// VFIO driver is available
    pub vfio_available: bool,
    /// UIO driver is available
    pub uio_available: bool,
    /// Running as root
    pub is_root: bool,
    /// Has CAP_NET_ADMIN capability
    pub has_net_admin: bool,
    /// IOMMU is enabled (for VFIO)
    pub iommu_enabled: bool,
    /// Number of compatible NICs detected
    pub compatible_nics: usize,
}

impl DpdkRequirements {
    /// Check if minimum requirements are met
    pub fn is_supported(&self) -> bool {
        self.huge_pages_available
            && (self.vfio_available || self.uio_available)
            && (self.is_root || self.has_net_admin)
            && self.compatible_nics > 0
    }

    /// Get summary of unmet requirements
    pub fn unmet_requirements(&self) -> Vec<String> {
        let mut issues = Vec::new();

        if !self.huge_pages_available {
            issues.push("Huge pages not available or mounted".to_string());
        }
        if !self.vfio_available && !self.uio_available {
            issues.push("Neither VFIO nor UIO driver available".to_string());
        }
        if !self.is_root && !self.has_net_admin {
            issues.push("Need root or CAP_NET_ADMIN capability".to_string());
        }
        if self.compatible_nics == 0 {
            issues.push("No DPDK-compatible NICs detected".to_string());
        }

        issues
    }
}

fn check_huge_pages() -> bool {
    Path::new("/sys/kernel/mm/hugepages/hugepages-2048kB").exists()
        || Path::new("/sys/kernel/mm/hugepages/hugepages-1048576kB").exists()
}

fn check_vfio() -> bool {
    Path::new("/dev/vfio").exists()
}

fn check_uio() -> bool {
    Path::new("/sys/class/uio").exists()
}

fn check_root() -> bool {
    unsafe { libc::getuid() == 0 }
}

fn check_capability(cap: &str) -> bool {
    // Check /proc/self/status for capabilities
    if let Ok(status) = fs::read_to_string("/proc/self/status") {
        for line in status.lines() {
            if line.starts_with("CapEff:") {
                if let Some(hex) = line.split_whitespace().nth(1) {
                    if let Ok(caps) = u64::from_str_radix(hex, 16) {
                        // CAP_NET_ADMIN = 12
                        return caps & (1 << 12) != 0;
                    }
                }
            }
        }
    }
    false
}

fn check_iommu() -> bool {
    if let Ok(cmdline) = fs::read_to_string("/proc/cmdline") {
        cmdline.contains("iommu=on") || cmdline.contains("intel_iommu=on")
    } else {
        false
    }
}

fn detect_compatible_nics() -> usize {
    // Known DPDK-compatible drivers
    let dpdk_drivers = [
        "ixgbe",
        "i40e",
        "ice",
        "mlx5_core",
        "ena",
        "bnxt_en",
        "virtio_net",
    ];

    let mut count = 0;
    if let Ok(entries) = fs::read_dir("/sys/class/net") {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            let driver_path = format!("/sys/class/net/{}/device/driver", name);

            if let Ok(driver_link) = fs::read_link(&driver_path) {
                if let Some(driver) = driver_link.file_name() {
                    let driver_name = driver.to_string_lossy();
                    if dpdk_drivers.iter().any(|d| driver_name.contains(d)) {
                        count += 1;
                    }
                }
            }
        }
    }
    count
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count_cores() {
        assert_eq!(Eal::count_cores("0").unwrap(), 1);
        assert_eq!(Eal::count_cores("0,1,2").unwrap(), 3);
        assert_eq!(Eal::count_cores("0-3").unwrap(), 4);
        assert_eq!(Eal::count_cores("0-3,8,12-15").unwrap(), 9);
        assert!(Eal::count_cores("").is_err());
    }

    #[test]
    fn test_eal_port_info_mac_string() {
        let info = EalPortInfo {
            port_id: 0,
            pci_addr: "0000:00:1f.0".to_string(),
            driver: "test".to_string(),
            mac_addr: [0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF],
            max_rx_queues: 8,
            max_tx_queues: 8,
            speed_capabilities: 0,
            device_name: "test0".to_string(),
        };

        assert_eq!(info.mac_address_string(), "aa:bb:cc:dd:ee:ff");
    }

    #[test]
    fn test_huge_page_info_default() {
        let info = HugePageInfo::default();
        assert_eq!(info.total_pages, 0);
    }

    #[test]
    fn test_requirements_check() {
        let reqs = check_requirements();
        // Just verify it runs without panicking
        let _ = reqs.is_supported();
        let _ = reqs.unmet_requirements();
    }
}
