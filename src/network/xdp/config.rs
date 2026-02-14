//! XDP configuration

use super::XdpMode;
use serde::{Deserialize, Serialize};

/// XDP data plane configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XdpConfig {
    /// Network interface to attach XDP program to
    pub interface: String,

    /// Kafka protocol port to filter
    pub port: u16,

    /// XDP attach mode
    #[serde(default)]
    pub mode: XdpConfigMode,

    /// Number of RX queues to use (0 = auto-detect)
    #[serde(default)]
    pub rx_queues: u32,

    /// UMEM configuration
    #[serde(default)]
    pub umem: UmemConfig,

    /// AF_XDP socket configuration
    #[serde(default)]
    pub socket: AfXdpSocketConfig,

    /// Packet filtering configuration
    #[serde(default)]
    pub filter: XdpFilterConfig,

    /// Enable busy-polling for lowest latency
    #[serde(default)]
    pub busy_poll: bool,

    /// Busy-poll budget (iterations before yield)
    #[serde(default = "default_busy_poll_budget")]
    pub busy_poll_budget: u32,

    /// Enable zero-copy mode (requires driver support)
    #[serde(default = "default_true")]
    pub zero_copy: bool,

    /// Enable need wakeup mode (more efficient than busy poll for mixed workloads)
    #[serde(default = "default_true")]
    pub need_wakeup: bool,
}

fn default_busy_poll_budget() -> u32 {
    256
}

fn default_true() -> bool {
    true
}

impl Default for XdpConfig {
    fn default() -> Self {
        Self {
            interface: "eth0".to_string(),
            port: 9092,
            mode: XdpConfigMode::default(),
            rx_queues: 0,
            umem: UmemConfig::default(),
            socket: AfXdpSocketConfig::default(),
            filter: XdpFilterConfig::default(),
            busy_poll: false,
            busy_poll_budget: 256,
            zero_copy: true,
            need_wakeup: true,
        }
    }
}

/// XDP mode configuration (serde-friendly wrapper)
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum XdpConfigMode {
    /// SKB (software) mode - works on all interfaces
    #[default]
    Skb,
    /// Native mode - requires driver support
    Native,
    /// Offload mode - requires NIC support
    Offload,
    /// Auto-detect best available mode
    Auto,
}

impl From<XdpConfigMode> for Option<XdpMode> {
    fn from(mode: XdpConfigMode) -> Self {
        match mode {
            XdpConfigMode::Skb => Some(XdpMode::Skb),
            XdpConfigMode::Native => Some(XdpMode::Native),
            XdpConfigMode::Offload => Some(XdpMode::Offload),
            XdpConfigMode::Auto => None, // Will be determined at runtime
        }
    }
}

/// UMEM (user memory) configuration for AF_XDP
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UmemConfig {
    /// Number of frames in UMEM region
    #[serde(default = "default_frame_count")]
    pub frame_count: u32,

    /// Size of each frame in bytes (must be power of 2)
    #[serde(default = "default_frame_size")]
    pub frame_size: u32,

    /// Headroom in each frame for packet headers
    #[serde(default = "default_frame_headroom")]
    pub frame_headroom: u32,

    /// Fill ring size
    #[serde(default = "default_ring_size")]
    pub fill_size: u32,

    /// Completion ring size
    #[serde(default = "default_ring_size")]
    pub comp_size: u32,

    /// Use huge pages for UMEM allocation
    #[serde(default)]
    pub use_huge_pages: bool,
}

fn default_frame_count() -> u32 {
    4096
}

fn default_frame_size() -> u32 {
    4096 // XDP_UMEM_MIN_CHUNK_SIZE on most kernels
}

fn default_frame_headroom() -> u32 {
    256 // Room for metadata before packet
}

fn default_ring_size() -> u32 {
    2048
}

impl Default for UmemConfig {
    fn default() -> Self {
        Self {
            frame_count: 4096,
            frame_size: 4096,
            frame_headroom: 256,
            fill_size: 2048,
            comp_size: 2048,
            use_huge_pages: false,
        }
    }
}

impl UmemConfig {
    /// Calculate total UMEM size in bytes
    pub fn total_size(&self) -> usize {
        self.frame_count as usize * self.frame_size as usize
    }

    /// Validate UMEM configuration
    pub fn validate(&self) -> Result<(), String> {
        if !self.frame_size.is_power_of_two() {
            return Err(format!(
                "frame_size must be power of 2, got {}",
                self.frame_size
            ));
        }
        if self.frame_size < 2048 {
            return Err(format!(
                "frame_size must be at least 2048, got {}",
                self.frame_size
            ));
        }
        if self.frame_headroom >= self.frame_size {
            return Err(format!(
                "frame_headroom ({}) must be less than frame_size ({})",
                self.frame_headroom, self.frame_size
            ));
        }
        if !self.fill_size.is_power_of_two() {
            return Err(format!(
                "fill_size must be power of 2, got {}",
                self.fill_size
            ));
        }
        if !self.comp_size.is_power_of_two() {
            return Err(format!(
                "comp_size must be power of 2, got {}",
                self.comp_size
            ));
        }
        Ok(())
    }
}

/// AF_XDP socket configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AfXdpSocketConfig {
    /// RX ring size
    #[serde(default = "default_ring_size")]
    pub rx_size: u32,

    /// TX ring size
    #[serde(default = "default_ring_size")]
    pub tx_size: u32,

    /// Batch size for packet processing
    #[serde(default = "default_batch_size")]
    pub batch_size: u32,

    /// Socket receive timeout in milliseconds (0 = blocking)
    #[serde(default)]
    pub recv_timeout_ms: u32,

    /// Enable XDP_SHARED_UMEM flag for multiple sockets sharing UMEM
    #[serde(default)]
    pub shared_umem: bool,
}

fn default_batch_size() -> u32 {
    64
}

impl Default for AfXdpSocketConfig {
    fn default() -> Self {
        Self {
            rx_size: 2048,
            tx_size: 2048,
            batch_size: 64,
            recv_timeout_ms: 0,
            shared_umem: false,
        }
    }
}

/// XDP packet filter configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XdpFilterConfig {
    /// Enable TCP filtering (only process TCP packets)
    #[serde(default = "default_true")]
    pub tcp_only: bool,

    /// Allowed source IP prefixes (empty = allow all)
    #[serde(default)]
    pub allowed_source_prefixes: Vec<String>,

    /// Blocked source IP prefixes
    #[serde(default)]
    pub blocked_source_prefixes: Vec<String>,

    /// Enable rate limiting per source IP
    #[serde(default)]
    pub rate_limit_enabled: bool,

    /// Rate limit (packets per second per source IP)
    #[serde(default = "default_rate_limit")]
    pub rate_limit_pps: u32,

    /// Enable SYN flood protection
    #[serde(default)]
    pub syn_flood_protection: bool,

    /// SYN flood threshold (SYNs per second)
    #[serde(default = "default_syn_threshold")]
    pub syn_flood_threshold: u32,
}

fn default_rate_limit() -> u32 {
    100_000 // 100k pps per source
}

fn default_syn_threshold() -> u32 {
    10_000 // 10k SYNs per second
}

impl Default for XdpFilterConfig {
    fn default() -> Self {
        Self {
            tcp_only: true,
            allowed_source_prefixes: Vec::new(),
            blocked_source_prefixes: Vec::new(),
            rate_limit_enabled: false,
            rate_limit_pps: 100_000,
            syn_flood_protection: false,
            syn_flood_threshold: 10_000,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_xdp_config_default() {
        let config = XdpConfig::default();
        assert_eq!(config.interface, "eth0");
        assert_eq!(config.port, 9092);
        assert!(config.zero_copy);
    }

    #[test]
    fn test_umem_config_total_size() {
        let config = UmemConfig {
            frame_count: 1024,
            frame_size: 4096,
            ..Default::default()
        };
        assert_eq!(config.total_size(), 1024 * 4096);
    }

    #[test]
    fn test_umem_config_validate() {
        let valid = UmemConfig::default();
        assert!(valid.validate().is_ok());

        let invalid_frame_size = UmemConfig {
            frame_size: 1000, // Not power of 2
            ..Default::default()
        };
        assert!(invalid_frame_size.validate().is_err());

        let too_small = UmemConfig {
            frame_size: 1024, // Too small
            ..Default::default()
        };
        assert!(too_small.validate().is_err());

        let headroom_too_large = UmemConfig {
            frame_headroom: 5000, // Larger than frame
            ..Default::default()
        };
        assert!(headroom_too_large.validate().is_err());
    }

    #[test]
    fn test_xdp_config_mode_conversion() {
        assert_eq!(
            Option::<XdpMode>::from(XdpConfigMode::Skb),
            Some(XdpMode::Skb)
        );
        assert_eq!(
            Option::<XdpMode>::from(XdpConfigMode::Native),
            Some(XdpMode::Native)
        );
        assert_eq!(Option::<XdpMode>::from(XdpConfigMode::Auto), None);
    }

    #[test]
    fn test_xdp_filter_config_default() {
        let config = XdpFilterConfig::default();
        assert!(config.tcp_only);
        assert!(!config.rate_limit_enabled);
        assert!(!config.syn_flood_protection);
    }
}
