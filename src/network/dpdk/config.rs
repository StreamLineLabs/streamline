//! DPDK configuration

use serde::{Deserialize, Serialize};

/// DPDK data plane configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DpdkConfig {
    /// EAL (Environment Abstraction Layer) configuration
    #[serde(default)]
    pub eal: EalConfig,

    /// Port configurations
    #[serde(default)]
    pub ports: Vec<DpdkPortConfig>,

    /// Memory pool configuration
    #[serde(default)]
    pub mempool: MempoolConfig,

    /// Kafka protocol port to process
    #[serde(default = "default_kafka_port")]
    pub kafka_port: u16,

    /// Enable promiscuous mode on ports
    #[serde(default)]
    pub promiscuous: bool,

    /// Enable hardware checksum offload
    #[serde(default = "default_true")]
    pub checksum_offload: bool,

    /// Enable LRO (Large Receive Offload)
    #[serde(default)]
    pub lro_enabled: bool,

    /// Enable TSO (TCP Segmentation Offload)
    #[serde(default = "default_true")]
    pub tso_enabled: bool,
}

fn default_kafka_port() -> u16 {
    9092
}

fn default_true() -> bool {
    true
}

impl Default for DpdkConfig {
    fn default() -> Self {
        Self {
            eal: EalConfig::default(),
            ports: Vec::new(),
            mempool: MempoolConfig::default(),
            kafka_port: 9092,
            promiscuous: false,
            checksum_offload: true,
            lro_enabled: false,
            tso_enabled: true,
        }
    }
}

/// EAL (Environment Abstraction Layer) configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EalConfig {
    /// CPU cores to use (comma-separated or range, e.g., "0-3" or "0,2,4")
    #[serde(default = "default_cores")]
    pub cores: String,

    /// Number of memory channels
    #[serde(default = "default_memory_channels")]
    pub memory_channels: u32,

    /// Huge page mount point
    #[serde(default = "default_huge_dir")]
    pub huge_dir: String,

    /// Number of huge pages to allocate
    #[serde(default = "default_huge_pages")]
    pub huge_pages: u32,

    /// Huge page size (2MB or 1GB)
    #[serde(default = "default_huge_page_size")]
    pub huge_page_size: HugePageSize,

    /// PCI device whitelist (specific NICs to use)
    #[serde(default)]
    pub pci_whitelist: Vec<String>,

    /// PCI device blacklist (NICs to ignore)
    #[serde(default)]
    pub pci_blacklist: Vec<String>,

    /// VFIO driver mode (for IOMMU support)
    #[serde(default)]
    pub vfio_enabled: bool,

    /// Log level for DPDK
    #[serde(default = "default_log_level")]
    pub log_level: u32,

    /// Custom EAL arguments
    #[serde(default)]
    pub extra_args: Vec<String>,
}

fn default_cores() -> String {
    "0".to_string() // Use core 0 by default
}

fn default_memory_channels() -> u32 {
    4
}

fn default_huge_dir() -> String {
    "/dev/hugepages".to_string()
}

fn default_huge_pages() -> u32 {
    1024 // 2GB with 2MB pages
}

fn default_huge_page_size() -> HugePageSize {
    HugePageSize::Size2MB
}

fn default_log_level() -> u32 {
    5 // RTE_LOG_INFO
}

impl Default for EalConfig {
    fn default() -> Self {
        Self {
            cores: "0".to_string(),
            memory_channels: 4,
            huge_dir: "/dev/hugepages".to_string(),
            huge_pages: 1024,
            huge_page_size: HugePageSize::Size2MB,
            pci_whitelist: Vec::new(),
            pci_blacklist: Vec::new(),
            vfio_enabled: false,
            log_level: 5,
            extra_args: Vec::new(),
        }
    }
}

impl EalConfig {
    /// Convert to EAL command-line arguments
    pub fn to_args(&self) -> Vec<String> {
        let mut args = vec![
            "streamline".to_string(), // argv[0]
            "-l".to_string(),
            self.cores.clone(),
            "-n".to_string(),
            self.memory_channels.to_string(),
            "--huge-dir".to_string(),
            self.huge_dir.clone(),
            "--log-level".to_string(),
            self.log_level.to_string(),
        ];

        // Add VFIO if enabled
        if self.vfio_enabled {
            args.push("--vfio-intr=msix".to_string());
        }

        // Add PCI whitelist
        for pci in &self.pci_whitelist {
            args.push("-a".to_string());
            args.push(pci.clone());
        }

        // Add PCI blacklist
        for pci in &self.pci_blacklist {
            args.push("-b".to_string());
            args.push(pci.clone());
        }

        // Add extra args
        args.extend(self.extra_args.clone());

        args
    }

    /// Validate EAL configuration
    pub fn validate(&self) -> Result<(), String> {
        // Validate cores
        if self.cores.is_empty() {
            return Err("At least one core must be specified".to_string());
        }

        // Validate huge pages
        if self.huge_pages == 0 {
            return Err("At least one huge page must be allocated".to_string());
        }

        Ok(())
    }
}

/// Huge page size
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HugePageSize {
    /// 2MB huge pages
    #[default]
    Size2MB,
    /// 1GB huge pages
    Size1GB,
}

impl HugePageSize {
    /// Get size in bytes
    pub fn bytes(&self) -> usize {
        match self {
            Self::Size2MB => 2 * 1024 * 1024,
            Self::Size1GB => 1024 * 1024 * 1024,
        }
    }
}

/// DPDK port configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DpdkPortConfig {
    /// Port ID (DPDK port number)
    pub port_id: u16,

    /// Number of RX queues
    #[serde(default = "default_queues")]
    pub rx_queues: u16,

    /// Number of TX queues
    #[serde(default = "default_queues")]
    pub tx_queues: u16,

    /// RX ring size (descriptors per queue)
    #[serde(default = "default_ring_size")]
    pub rx_ring_size: u16,

    /// TX ring size (descriptors per queue)
    #[serde(default = "default_ring_size")]
    pub tx_ring_size: u16,

    /// CPU cores to pin RX queues to (one per queue)
    #[serde(default)]
    pub rx_core_affinity: Vec<u32>,

    /// Enable hardware RSS
    #[serde(default = "default_true")]
    pub rss_enabled: bool,

    /// RSS hash key (40 bytes, hex-encoded)
    #[serde(default)]
    pub rss_key: Option<String>,

    /// MTU size
    #[serde(default = "default_mtu")]
    pub mtu: u16,
}

fn default_queues() -> u16 {
    4
}

fn default_ring_size() -> u16 {
    1024
}

fn default_mtu() -> u16 {
    1500
}

impl Default for DpdkPortConfig {
    fn default() -> Self {
        Self {
            port_id: 0,
            rx_queues: 4,
            tx_queues: 4,
            rx_ring_size: 1024,
            tx_ring_size: 1024,
            rx_core_affinity: Vec::new(),
            rss_enabled: true,
            rss_key: None,
            mtu: 1500,
        }
    }
}

/// Memory pool configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MempoolConfig {
    /// Number of mbufs in the pool
    #[serde(default = "default_mbuf_count")]
    pub mbuf_count: u32,

    /// Size of each mbuf data room (payload area)
    #[serde(default = "default_mbuf_data_size")]
    pub mbuf_data_size: u16,

    /// Cache size per lcore
    #[serde(default = "default_cache_size")]
    pub cache_size: u32,

    /// Private data size per mbuf
    #[serde(default)]
    pub priv_size: u16,
}

fn default_mbuf_count() -> u32 {
    8192 // 8K mbufs
}

fn default_mbuf_data_size() -> u16 {
    2048 // 2KB per mbuf
}

fn default_cache_size() -> u32 {
    256 // Per-lcore cache
}

impl Default for MempoolConfig {
    fn default() -> Self {
        Self {
            mbuf_count: 8192,
            mbuf_data_size: 2048,
            cache_size: 256,
            priv_size: 0,
        }
    }
}

impl MempoolConfig {
    /// Calculate total memory required
    pub fn total_memory(&self) -> usize {
        let mbuf_size = self.mbuf_data_size as usize + 128; // Header + data
        self.mbuf_count as usize * mbuf_size
    }

    /// Validate mempool configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.mbuf_count == 0 {
            return Err("mbuf_count must be greater than 0".to_string());
        }
        if !self.mbuf_count.is_power_of_two() {
            return Err("mbuf_count should be power of 2 for best performance".to_string());
        }
        if self.mbuf_data_size < 64 {
            return Err("mbuf_data_size must be at least 64 bytes".to_string());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dpdk_config_default() {
        let config = DpdkConfig::default();
        assert_eq!(config.kafka_port, 9092);
        assert!(config.checksum_offload);
    }

    #[test]
    fn test_eal_config_to_args() {
        let config = EalConfig {
            cores: "0-3".to_string(),
            memory_channels: 4,
            pci_whitelist: vec!["0000:00:1f.0".to_string()],
            ..Default::default()
        };

        let args = config.to_args();
        assert!(args.contains(&"-l".to_string()));
        assert!(args.contains(&"0-3".to_string()));
        assert!(args.contains(&"-a".to_string()));
    }

    #[test]
    fn test_mempool_config_total_memory() {
        let config = MempoolConfig {
            mbuf_count: 1024,
            mbuf_data_size: 2048,
            ..Default::default()
        };

        // (2048 + 128) * 1024 = 2,228,224 bytes
        assert_eq!(config.total_memory(), (2048 + 128) * 1024);
    }

    #[test]
    fn test_mempool_config_validate() {
        let valid = MempoolConfig::default();
        assert!(valid.validate().is_ok());

        let invalid = MempoolConfig {
            mbuf_count: 0,
            ..Default::default()
        };
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn test_huge_page_size() {
        assert_eq!(HugePageSize::Size2MB.bytes(), 2 * 1024 * 1024);
        assert_eq!(HugePageSize::Size1GB.bytes(), 1024 * 1024 * 1024);
    }
}
