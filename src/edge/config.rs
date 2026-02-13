//! Edge configuration types
//!
//! Configuration for edge-first streaming deployments.

use serde::{Deserialize, Serialize};
use std::time::Duration;

use super::discovery::DiscoveryProtocol;

/// Conflict resolution strategy for distributed writes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum ConflictResolution {
    /// Last write wins based on timestamp
    #[default]
    LastWriteWins,
    /// Primary (cloud) always wins conflicts
    CloudWins,
    /// Edge always wins conflicts
    EdgeWins,
    /// Higher offset wins
    HigherOffsetWins,
    /// Custom resolution via callback
    Custom,
}

/// Edge sync mode - how often and when to sync
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum EdgeSyncMode {
    /// Sync continuously when connected
    #[default]
    Continuous,
    /// Sync at fixed intervals
    Interval,
    /// Manual sync only (via API call)
    Manual,
    /// Batch sync when threshold reached
    Batched,
}

/// Edge retention policy for offline data
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum EdgeRetentionPolicy {
    /// Keep all data until synced
    #[default]
    UntilSynced,
    /// Keep last N records per partition
    RecordCount,
    /// Keep data for N milliseconds
    TimeBased,
    /// Keep data up to N bytes total
    SizeBased,
    /// Ring buffer (overwrite oldest)
    RingBuffer,
}

/// Compression strategy for edge storage and sync
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum EdgeCompressionStrategy {
    /// No compression
    None,
    /// Fast compression for real-time (zstd level 1)
    #[default]
    Fast,
    /// Balanced compression (zstd level 3)
    Balanced,
    /// Maximum compression for bandwidth-constrained (zstd level 9)
    Maximum,
}

/// Main edge configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeConfig {
    /// Unique identifier for this edge device
    pub edge_id: String,

    /// Cloud endpoint for sync (e.g., "https://cloud.streamline.io:9092")
    pub cloud_endpoint: Option<String>,

    /// Sync configuration
    pub sync: EdgeSyncConfig,

    /// Storage configuration
    pub storage: EdgeStorageConfig,

    /// Resource constraints
    pub resources: EdgeResourceConfig,

    /// Network configuration
    pub network: EdgeNetworkConfig,

    /// Whether peer-to-peer mesh networking is enabled
    pub mesh_enabled: bool,

    /// Discovery protocol for finding peer edge nodes
    pub discovery_protocol: DiscoveryProtocol,

    /// Whether resource monitoring is enabled
    pub resource_monitoring: bool,
}

impl Default for EdgeConfig {
    fn default() -> Self {
        Self {
            edge_id: format!("edge-{}", uuid::Uuid::new_v4().simple()),
            cloud_endpoint: None,
            sync: EdgeSyncConfig::default(),
            storage: EdgeStorageConfig::default(),
            resources: EdgeResourceConfig::default(),
            network: EdgeNetworkConfig::default(),
            mesh_enabled: false,
            discovery_protocol: DiscoveryProtocol::Manual,
            resource_monitoring: true,
        }
    }
}

impl EdgeConfig {
    /// Create a minimal edge configuration for resource-constrained devices
    pub fn minimal() -> Self {
        Self {
            edge_id: format!("edge-{}", uuid::Uuid::new_v4().simple()),
            cloud_endpoint: None,
            sync: EdgeSyncConfig::minimal(),
            storage: EdgeStorageConfig::minimal(),
            resources: EdgeResourceConfig::minimal(),
            network: EdgeNetworkConfig::default(),
            mesh_enabled: false,
            discovery_protocol: DiscoveryProtocol::Manual,
            resource_monitoring: true,
        }
    }

    /// Create an edge configuration for offline-first operation
    pub fn offline_first() -> Self {
        Self {
            edge_id: format!("edge-{}", uuid::Uuid::new_v4().simple()),
            cloud_endpoint: None,
            sync: EdgeSyncConfig {
                mode: EdgeSyncMode::Manual,
                ..Default::default()
            },
            storage: EdgeStorageConfig {
                retention_policy: EdgeRetentionPolicy::UntilSynced,
                max_offline_records: 1_000_000,
                ..Default::default()
            },
            resources: EdgeResourceConfig::default(),
            network: EdgeNetworkConfig::default(),
            mesh_enabled: false,
            discovery_protocol: DiscoveryProtocol::Manual,
            resource_monitoring: true,
        }
    }

    /// Set the cloud endpoint for sync
    pub fn with_cloud_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.cloud_endpoint = Some(endpoint.into());
        self
    }

    /// Check if this edge is configured for cloud sync
    pub fn has_cloud_sync(&self) -> bool {
        self.cloud_endpoint.is_some()
    }
}

/// Sync configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeSyncConfig {
    /// Sync mode (continuous, interval, manual, batched)
    pub mode: EdgeSyncMode,

    /// Sync interval in milliseconds (for Interval mode)
    pub interval_ms: u64,

    /// Batch size threshold for syncing (for Batched mode)
    pub batch_size: usize,

    /// Conflict resolution strategy
    pub conflict_resolution: ConflictResolution,

    /// Whether to compress data during sync
    pub compression: EdgeCompressionStrategy,

    /// Maximum retries for failed syncs
    pub max_retries: u32,

    /// Retry backoff base in milliseconds
    pub retry_backoff_ms: u64,

    /// Whether to enable delta sync (only changed records)
    pub delta_sync_enabled: bool,

    /// Topics to sync (empty = all topics)
    pub sync_topics: Vec<String>,
}

impl Default for EdgeSyncConfig {
    fn default() -> Self {
        Self {
            mode: EdgeSyncMode::Continuous,
            interval_ms: 5000,
            batch_size: 1000,
            conflict_resolution: ConflictResolution::LastWriteWins,
            compression: EdgeCompressionStrategy::Fast,
            max_retries: 3,
            retry_backoff_ms: 1000,
            delta_sync_enabled: true,
            sync_topics: Vec::new(),
        }
    }
}

impl EdgeSyncConfig {
    /// Create minimal sync config for resource-constrained devices
    pub fn minimal() -> Self {
        Self {
            mode: EdgeSyncMode::Batched,
            interval_ms: 30000,
            batch_size: 100,
            conflict_resolution: ConflictResolution::LastWriteWins,
            compression: EdgeCompressionStrategy::Maximum,
            max_retries: 2,
            retry_backoff_ms: 2000,
            delta_sync_enabled: true,
            sync_topics: Vec::new(),
        }
    }

    /// Get sync interval as Duration
    pub fn interval(&self) -> Duration {
        Duration::from_millis(self.interval_ms)
    }

    /// Get retry backoff as Duration
    pub fn retry_backoff(&self) -> Duration {
        Duration::from_millis(self.retry_backoff_ms)
    }
}

/// Edge storage configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeStorageConfig {
    /// Data directory for edge storage
    pub data_dir: String,

    /// Whether to use in-memory storage (no persistence)
    pub in_memory: bool,

    /// Retention policy for offline data
    pub retention_policy: EdgeRetentionPolicy,

    /// Maximum offline records per partition (for RecordCount policy)
    pub max_offline_records: u64,

    /// Maximum offline duration in milliseconds (for TimeBased policy)
    pub max_offline_duration_ms: u64,

    /// Maximum storage size in bytes (for SizeBased policy)
    pub max_storage_bytes: u64,

    /// Whether WAL is enabled
    pub wal_enabled: bool,

    /// Segment size in bytes
    pub segment_size: u64,

    /// Compression for storage
    pub compression: EdgeCompressionStrategy,
}

impl Default for EdgeStorageConfig {
    fn default() -> Self {
        Self {
            data_dir: "./edge-data".to_string(),
            in_memory: false,
            retention_policy: EdgeRetentionPolicy::UntilSynced,
            max_offline_records: 100_000,
            max_offline_duration_ms: 7 * 24 * 60 * 60 * 1000, // 7 days
            max_storage_bytes: 1024 * 1024 * 1024,            // 1GB
            wal_enabled: true,
            segment_size: 16 * 1024 * 1024, // 16MB
            compression: EdgeCompressionStrategy::Fast,
        }
    }
}

impl EdgeStorageConfig {
    /// Create minimal storage config for resource-constrained devices
    pub fn minimal() -> Self {
        Self {
            data_dir: "./edge-data".to_string(),
            in_memory: false,
            retention_policy: EdgeRetentionPolicy::RingBuffer,
            max_offline_records: 10_000,
            max_offline_duration_ms: 24 * 60 * 60 * 1000, // 1 day
            max_storage_bytes: 100 * 1024 * 1024,         // 100MB
            wal_enabled: false,                           // Disable for minimal footprint
            segment_size: 4 * 1024 * 1024,                // 4MB
            compression: EdgeCompressionStrategy::Maximum,
        }
    }

    /// Get max offline duration as Duration
    pub fn max_offline_duration(&self) -> Duration {
        Duration::from_millis(self.max_offline_duration_ms)
    }
}

/// Resource constraints configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeResourceConfig {
    /// Maximum memory usage in bytes
    pub max_memory_bytes: u64,

    /// Maximum CPU usage percentage (0.0 - 1.0)
    pub max_cpu_percent: f32,

    /// Maximum concurrent operations
    pub max_concurrent_ops: usize,

    /// Buffer pool size for batch operations
    pub buffer_pool_size: usize,

    /// Whether to enable adaptive resource management
    pub adaptive_enabled: bool,
}

impl Default for EdgeResourceConfig {
    fn default() -> Self {
        Self {
            max_memory_bytes: 256 * 1024 * 1024, // 256MB
            max_cpu_percent: 0.5,
            max_concurrent_ops: 4,
            buffer_pool_size: 16,
            adaptive_enabled: true,
        }
    }
}

impl EdgeResourceConfig {
    /// Create minimal resource config for constrained devices
    pub fn minimal() -> Self {
        Self {
            max_memory_bytes: 64 * 1024 * 1024, // 64MB
            max_cpu_percent: 0.25,
            max_concurrent_ops: 2,
            buffer_pool_size: 4,
            adaptive_enabled: true,
        }
    }
}

/// Network configuration for edge sync
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeNetworkConfig {
    /// Connection timeout in milliseconds
    pub connect_timeout_ms: u64,

    /// Request timeout in milliseconds
    pub request_timeout_ms: u64,

    /// Whether to enable TLS
    pub tls_enabled: bool,

    /// Path to TLS certificate (for mTLS)
    pub tls_cert_path: Option<String>,

    /// Path to TLS key (for mTLS)
    pub tls_key_path: Option<String>,

    /// Whether to verify server certificate
    pub tls_verify: bool,

    /// Maximum bandwidth in bytes per second (0 = unlimited)
    pub max_bandwidth_bps: u64,

    /// Whether to enable connection pooling
    pub connection_pooling: bool,

    /// Maximum connections in pool
    pub max_connections: usize,
}

impl Default for EdgeNetworkConfig {
    fn default() -> Self {
        Self {
            connect_timeout_ms: 10000,
            request_timeout_ms: 30000,
            tls_enabled: true,
            tls_cert_path: None,
            tls_key_path: None,
            tls_verify: true,
            max_bandwidth_bps: 0,
            connection_pooling: true,
            max_connections: 4,
        }
    }
}

impl EdgeNetworkConfig {
    /// Get connect timeout as Duration
    pub fn connect_timeout(&self) -> Duration {
        Duration::from_millis(self.connect_timeout_ms)
    }

    /// Get request timeout as Duration
    pub fn request_timeout(&self) -> Duration {
        Duration::from_millis(self.request_timeout_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_edge_config_default() {
        let config = EdgeConfig::default();
        assert!(config.edge_id.starts_with("edge-"));
        assert!(config.cloud_endpoint.is_none());
        assert!(!config.has_cloud_sync());
    }

    #[test]
    fn test_edge_config_with_cloud() {
        let config = EdgeConfig::default().with_cloud_endpoint("https://cloud.example.com:9092");
        assert!(config.has_cloud_sync());
        assert_eq!(
            config.cloud_endpoint,
            Some("https://cloud.example.com:9092".to_string())
        );
    }

    #[test]
    fn test_edge_config_minimal() {
        let config = EdgeConfig::minimal();
        assert_eq!(config.sync.mode, EdgeSyncMode::Batched);
        assert_eq!(
            config.storage.retention_policy,
            EdgeRetentionPolicy::RingBuffer
        );
        assert_eq!(config.resources.max_memory_bytes, 64 * 1024 * 1024);
    }

    #[test]
    fn test_edge_config_offline_first() {
        let config = EdgeConfig::offline_first();
        assert_eq!(config.sync.mode, EdgeSyncMode::Manual);
        assert_eq!(
            config.storage.retention_policy,
            EdgeRetentionPolicy::UntilSynced
        );
    }

    #[test]
    fn test_sync_config_intervals() {
        let config = EdgeSyncConfig::default();
        assert_eq!(config.interval(), Duration::from_millis(5000));
        assert_eq!(config.retry_backoff(), Duration::from_millis(1000));
    }

    #[test]
    fn test_storage_config_durations() {
        let config = EdgeStorageConfig::default();
        assert_eq!(
            config.max_offline_duration(),
            Duration::from_millis(7 * 24 * 60 * 60 * 1000)
        );
    }

    #[test]
    fn test_network_config_timeouts() {
        let config = EdgeNetworkConfig::default();
        assert_eq!(config.connect_timeout(), Duration::from_millis(10000));
        assert_eq!(config.request_timeout(), Duration::from_millis(30000));
    }
}
