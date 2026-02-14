//! RDMA configuration

use super::{QueuePairType, RdmaMtu, RdmaTransport};
use serde::{Deserialize, Serialize};

/// RDMA configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RdmaConfig {
    /// Enable RDMA networking
    #[serde(default)]
    pub enabled: bool,

    /// RDMA transport type
    #[serde(default)]
    pub transport: RdmaTransport,

    /// Device name (e.g., "mlx5_0")
    #[serde(default)]
    pub device_name: Option<String>,

    /// Port number (1-based)
    #[serde(default = "default_port")]
    pub port: u8,

    /// GID index for RoCE
    #[serde(default)]
    pub gid_index: u32,

    /// Queue pair configuration
    #[serde(default)]
    pub queue_pair: QueuePairConfig,

    /// Completion queue configuration
    #[serde(default)]
    pub completion_queue: CompletionQueueConfig,

    /// Memory region configuration
    #[serde(default)]
    pub memory: MemoryConfig,

    /// Connection configuration
    #[serde(default)]
    pub connection: ConnectionConfig,

    /// Enable inline data
    #[serde(default = "default_true")]
    pub inline_data: bool,

    /// Maximum inline data size
    #[serde(default = "default_inline_size")]
    pub max_inline_size: u32,
}

fn default_port() -> u8 {
    1
}

fn default_true() -> bool {
    true
}

fn default_inline_size() -> u32 {
    512
}

impl Default for RdmaConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            transport: RdmaTransport::default(),
            device_name: None,
            port: 1,
            gid_index: 0,
            queue_pair: QueuePairConfig::default(),
            completion_queue: CompletionQueueConfig::default(),
            memory: MemoryConfig::default(),
            connection: ConnectionConfig::default(),
            inline_data: true,
            max_inline_size: 512,
        }
    }
}

impl RdmaConfig {
    /// Validate the configuration
    pub fn validate(&self) -> Result<(), String> {
        if !self.enabled {
            return Ok(());
        }

        if self.port == 0 {
            return Err("Port must be >= 1".to_string());
        }

        self.queue_pair.validate()?;
        self.completion_queue.validate()?;
        self.memory.validate()?;

        Ok(())
    }
}

/// Queue pair configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueuePairConfig {
    /// Queue pair type
    #[serde(default)]
    pub qp_type: QueuePairType,

    /// Maximum send work requests
    #[serde(default = "default_max_send_wr")]
    pub max_send_wr: u32,

    /// Maximum receive work requests
    #[serde(default = "default_max_recv_wr")]
    pub max_recv_wr: u32,

    /// Maximum send scatter/gather entries
    #[serde(default = "default_max_send_sge")]
    pub max_send_sge: u32,

    /// Maximum receive scatter/gather entries
    #[serde(default = "default_max_recv_sge")]
    pub max_recv_sge: u32,

    /// MTU size
    #[serde(default)]
    pub mtu: RdmaMtu,

    /// Minimum RNR NAK timer
    #[serde(default = "default_min_rnr_timer")]
    pub min_rnr_timer: u8,

    /// Retry count
    #[serde(default = "default_retry_count")]
    pub retry_count: u8,

    /// RNR retry count
    #[serde(default = "default_rnr_retry")]
    pub rnr_retry: u8,

    /// ACK timeout (as power of 2 * 4.096 microseconds)
    #[serde(default = "default_timeout")]
    pub timeout: u8,
}

fn default_max_send_wr() -> u32 {
    4096
}

fn default_max_recv_wr() -> u32 {
    4096
}

fn default_max_send_sge() -> u32 {
    4
}

fn default_max_recv_sge() -> u32 {
    4
}

fn default_min_rnr_timer() -> u8 {
    12 // ~0.01 ms
}

fn default_retry_count() -> u8 {
    7
}

fn default_rnr_retry() -> u8 {
    7
}

fn default_timeout() -> u8 {
    14 // ~67 ms
}

impl Default for QueuePairConfig {
    fn default() -> Self {
        Self {
            qp_type: QueuePairType::default(),
            max_send_wr: 4096,
            max_recv_wr: 4096,
            max_send_sge: 4,
            max_recv_sge: 4,
            mtu: RdmaMtu::default(),
            min_rnr_timer: 12,
            retry_count: 7,
            rnr_retry: 7,
            timeout: 14,
        }
    }
}

impl QueuePairConfig {
    /// Validate queue pair configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.max_send_wr == 0 {
            return Err("max_send_wr must be > 0".to_string());
        }
        if self.max_recv_wr == 0 {
            return Err("max_recv_wr must be > 0".to_string());
        }
        if self.max_send_sge == 0 {
            return Err("max_send_sge must be > 0".to_string());
        }
        if self.max_recv_sge == 0 {
            return Err("max_recv_sge must be > 0".to_string());
        }
        Ok(())
    }
}

/// Completion queue configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionQueueConfig {
    /// Number of completion queue entries
    #[serde(default = "default_cq_size")]
    pub cq_size: u32,

    /// Completion channel enabled
    #[serde(default)]
    pub completion_channel: bool,

    /// Moderation count (completions before notification)
    #[serde(default = "default_comp_count")]
    pub comp_count: u32,

    /// Moderation period (microseconds)
    #[serde(default = "default_comp_period")]
    pub comp_period: u32,

    /// Enable shared receive queue
    #[serde(default)]
    pub srq_enabled: bool,

    /// SRQ size (if enabled)
    #[serde(default = "default_srq_size")]
    pub srq_size: u32,
}

fn default_cq_size() -> u32 {
    8192
}

fn default_comp_count() -> u32 {
    64
}

fn default_comp_period() -> u32 {
    10 // 10 microseconds
}

fn default_srq_size() -> u32 {
    4096
}

impl Default for CompletionQueueConfig {
    fn default() -> Self {
        Self {
            cq_size: 8192,
            completion_channel: false,
            comp_count: 64,
            comp_period: 10,
            srq_enabled: false,
            srq_size: 4096,
        }
    }
}

impl CompletionQueueConfig {
    /// Validate completion queue configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.cq_size == 0 {
            return Err("cq_size must be > 0".to_string());
        }
        if self.srq_enabled && self.srq_size == 0 {
            return Err("srq_size must be > 0 when SRQ is enabled".to_string());
        }
        Ok(())
    }
}

/// Memory configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryConfig {
    /// Number of memory regions to pre-allocate
    #[serde(default = "default_mr_count")]
    pub mr_count: usize,

    /// Size of each memory region
    #[serde(default = "default_mr_size")]
    pub mr_size: usize,

    /// Use huge pages for memory allocation
    #[serde(default)]
    pub huge_pages: bool,

    /// Enable on-demand paging
    #[serde(default)]
    pub odp_enabled: bool,

    /// Use device memory (for supported NICs)
    #[serde(default)]
    pub device_memory: bool,
}

fn default_mr_count() -> usize {
    16
}

fn default_mr_size() -> usize {
    4 * 1024 * 1024 // 4MB
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            mr_count: 16,
            mr_size: 4 * 1024 * 1024,
            huge_pages: false,
            odp_enabled: false,
            device_memory: false,
        }
    }
}

impl MemoryConfig {
    /// Validate memory configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.mr_size == 0 {
            return Err("mr_size must be > 0".to_string());
        }
        if self.mr_size > 1024 * 1024 * 1024 {
            return Err("mr_size exceeds 1GB limit".to_string());
        }
        Ok(())
    }

    /// Calculate total memory needed
    pub fn total_memory(&self) -> usize {
        self.mr_count * self.mr_size
    }
}

/// Connection configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionConfig {
    /// Connection timeout (milliseconds)
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout_ms: u64,

    /// Maximum number of connections per peer
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,

    /// Enable connection pooling
    #[serde(default = "default_true")]
    pub connection_pooling: bool,

    /// Minimum idle connections in pool
    #[serde(default = "default_min_idle")]
    pub min_idle_connections: usize,

    /// Keep-alive interval (milliseconds)
    #[serde(default = "default_keepalive")]
    pub keepalive_ms: u64,

    /// Enable path MTU discovery
    #[serde(default = "default_true")]
    pub path_mtu_discovery: bool,
}

fn default_connect_timeout() -> u64 {
    5000 // 5 seconds
}

fn default_max_connections() -> usize {
    16
}

fn default_min_idle() -> usize {
    2
}

fn default_keepalive() -> u64 {
    30000 // 30 seconds
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            connect_timeout_ms: 5000,
            max_connections: 16,
            connection_pooling: true,
            min_idle_connections: 2,
            keepalive_ms: 30000,
            path_mtu_discovery: true,
        }
    }
}

/// RDMA transport mode (configuration preset)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportMode {
    /// Low latency mode (smaller buffers, aggressive completion)
    LowLatency,
    /// High throughput mode (larger buffers, batched completion)
    HighThroughput,
    /// Balanced mode (default)
    Balanced,
    /// Custom (use provided configuration)
    Custom,
}

impl Default for TransportMode {
    fn default() -> Self {
        Self::Balanced
    }
}

impl TransportMode {
    /// Get recommended configuration for this mode
    pub fn to_config(&self) -> RdmaConfig {
        match self {
            Self::LowLatency => RdmaConfig {
                enabled: true,
                queue_pair: QueuePairConfig {
                    max_send_wr: 1024,
                    max_recv_wr: 1024,
                    ..Default::default()
                },
                completion_queue: CompletionQueueConfig {
                    cq_size: 2048,
                    comp_count: 1,
                    comp_period: 0,
                    ..Default::default()
                },
                memory: MemoryConfig {
                    mr_count: 8,
                    mr_size: 1024 * 1024, // 1MB
                    ..Default::default()
                },
                ..Default::default()
            },
            Self::HighThroughput => RdmaConfig {
                enabled: true,
                queue_pair: QueuePairConfig {
                    max_send_wr: 8192,
                    max_recv_wr: 8192,
                    ..Default::default()
                },
                completion_queue: CompletionQueueConfig {
                    cq_size: 16384,
                    comp_count: 256,
                    comp_period: 100,
                    ..Default::default()
                },
                memory: MemoryConfig {
                    mr_count: 64,
                    mr_size: 16 * 1024 * 1024, // 16MB
                    huge_pages: true,
                    ..Default::default()
                },
                ..Default::default()
            },
            Self::Balanced | Self::Custom => RdmaConfig::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = RdmaConfig::default();
        assert!(!config.enabled);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validation() {
        let mut config = RdmaConfig::default();
        config.enabled = true;
        config.port = 0;
        assert!(config.validate().is_err());

        config.port = 1;
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_transport_mode_configs() {
        let low_latency = TransportMode::LowLatency.to_config();
        assert!(low_latency.enabled);
        assert_eq!(low_latency.completion_queue.comp_count, 1);

        let high_throughput = TransportMode::HighThroughput.to_config();
        assert!(high_throughput.memory.huge_pages);
    }

    #[test]
    fn test_memory_total() {
        let config = MemoryConfig {
            mr_count: 4,
            mr_size: 1024 * 1024,
            ..Default::default()
        };
        assert_eq!(config.total_memory(), 4 * 1024 * 1024);
    }
}
