//! Storage mode configuration for Streamline
//!
//! This module defines the storage modes available for topics and the
//! configuration options for remote storage (S3/Azure/GCS).
//!
//! ## Storage Modes
//!
//! - **Local**: Traditional local disk storage with optional tiering (default)
//! - **Hybrid**: Local WAL for low-latency acks, segments written to object storage
//! - **Diskless**: All data written directly to object storage (highest cost savings)

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Storage mode for a topic
///
/// Determines where and how data is persisted. Each mode offers different
/// trade-offs between latency, cost, and durability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum StorageMode {
    /// Traditional local storage with optional tiering (default)
    ///
    /// - Active segments on local disk
    /// - Cold segments optionally tiered to object storage
    /// - Full replication for HA
    /// - Lowest latency, highest local storage cost
    #[default]
    Local,

    /// Hybrid mode - local WAL with S3-backed segments
    ///
    /// - WAL written locally (for acks=1 low-latency path)
    /// - Segments written directly to object storage
    /// - Reduced replication (WAL only)
    /// - Balanced latency/cost trade-off
    Hybrid,

    /// Full diskless - all writes go to object storage
    ///
    /// - No local disk usage (except optional cache)
    /// - Higher latency (~100-500ms produce)
    /// - Zero replication traffic
    /// - Lowest cost, suitable for analytics/batch workloads
    Diskless,
}

impl StorageMode {
    /// Returns true if this mode requires object storage configuration
    pub fn requires_remote_storage(&self) -> bool {
        matches!(self, StorageMode::Hybrid | StorageMode::Diskless)
    }

    /// Returns true if this mode uses local WAL
    pub fn uses_local_wal(&self) -> bool {
        matches!(self, StorageMode::Local | StorageMode::Hybrid)
    }

    /// Returns true if this mode uses local segments
    pub fn uses_local_segments(&self) -> bool {
        matches!(self, StorageMode::Local)
    }

    /// Returns true if this mode requires replication
    ///
    /// Diskless mode doesn't require replication since data is in shared storage
    pub fn requires_replication(&self) -> bool {
        matches!(self, StorageMode::Local | StorageMode::Hybrid)
    }

    /// Returns true if this mode is the default local mode
    pub fn is_local(&self) -> bool {
        matches!(self, StorageMode::Local)
    }

    /// Returns true if this mode writes directly to object storage
    pub fn is_diskless(&self) -> bool {
        matches!(self, StorageMode::Diskless)
    }

    /// Returns true if this is hybrid mode
    pub fn is_hybrid(&self) -> bool {
        matches!(self, StorageMode::Hybrid)
    }
}

impl std::fmt::Display for StorageMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageMode::Local => write!(f, "local"),
            StorageMode::Hybrid => write!(f, "hybrid"),
            StorageMode::Diskless => write!(f, "diskless"),
        }
    }
}

impl std::str::FromStr for StorageMode {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "local" => Ok(StorageMode::Local),
            "hybrid" => Ok(StorageMode::Hybrid),
            "diskless" => Ok(StorageMode::Diskless),
            _ => Err(format!(
                "Invalid storage mode '{}'. Valid options: local, hybrid, diskless",
                s
            )),
        }
    }
}

/// Configuration for remote storage (used by Hybrid and Diskless modes)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteStorageConfig {
    /// Batch size before flushing to object storage (bytes)
    ///
    /// Larger batches improve throughput but increase latency and memory usage.
    /// Default: 1MB
    #[serde(default = "default_batch_size_bytes")]
    pub batch_size_bytes: usize,

    /// Maximum time to wait before flushing to object storage (milliseconds)
    ///
    /// Even if batch_size_bytes is not reached, data will be flushed after this timeout.
    /// Default: 100ms
    #[serde(default = "default_batch_timeout_ms")]
    pub batch_timeout_ms: u64,

    /// Number of parallel uploads to object storage
    ///
    /// Higher values improve throughput for high-volume topics.
    /// Default: 4
    #[serde(default = "default_upload_parallelism")]
    pub upload_parallelism: usize,

    /// Local read cache size in bytes (0 to disable)
    ///
    /// Caches recently read data to reduce object storage latency.
    /// Default: 100MB
    #[serde(default = "default_read_cache_bytes")]
    pub read_cache_bytes: u64,

    /// Use S3 Express One Zone for lower latency (S3 only)
    ///
    /// S3 Express provides single-digit millisecond latency but is more expensive.
    /// Default: false
    #[serde(default)]
    pub use_express_zone: bool,

    /// Prefetch next segments on sequential reads
    ///
    /// Improves read performance for consumers reading sequentially.
    /// Default: true
    #[serde(default = "default_prefetch_enabled")]
    pub prefetch_enabled: bool,

    /// Number of segments to prefetch ahead
    ///
    /// Default: 2
    #[serde(default = "default_prefetch_segments")]
    pub prefetch_segments: usize,

    /// Enable S3 multipart uploads for large segments
    ///
    /// Multipart uploads provide better performance and reliability for large segments.
    /// Default: true
    #[serde(default = "default_multipart_enabled")]
    pub multipart_enabled: bool,

    /// Minimum segment size in bytes to trigger multipart upload
    ///
    /// Segments smaller than this will use single-part uploads.
    /// Default: 64MB
    #[serde(default = "default_multipart_threshold")]
    pub multipart_threshold_bytes: usize,

    /// Part size for multipart uploads in bytes
    ///
    /// Must be at least 5MB (S3 requirement).
    /// Default: 16MB
    #[serde(default = "default_multipart_part_size")]
    pub multipart_part_size_bytes: usize,

    /// Maximum retry attempts for S3 operations
    ///
    /// Failed operations will be retried up to this many times with exponential backoff.
    /// Default: 3
    #[serde(default = "default_max_retries")]
    pub max_retries: usize,

    /// Initial retry backoff in milliseconds
    ///
    /// Backoff will double after each retry (exponential backoff).
    /// Default: 100ms
    #[serde(default = "default_retry_backoff_ms")]
    pub retry_backoff_ms: u64,

    /// Maximum retry backoff in milliseconds
    ///
    /// Caps the exponential backoff growth.
    /// Default: 30000ms (30 seconds)
    #[serde(default = "default_max_retry_backoff_ms")]
    pub max_retry_backoff_ms: u64,
}

fn default_batch_size_bytes() -> usize {
    1024 * 1024 // 1MB
}

fn default_batch_timeout_ms() -> u64 {
    100
}

fn default_upload_parallelism() -> usize {
    4
}

fn default_read_cache_bytes() -> u64 {
    100 * 1024 * 1024 // 100MB
}

fn default_prefetch_enabled() -> bool {
    true
}

fn default_prefetch_segments() -> usize {
    2
}

fn default_multipart_enabled() -> bool {
    true
}

fn default_multipart_threshold() -> usize {
    64 * 1024 * 1024 // 64MB
}

fn default_multipart_part_size() -> usize {
    16 * 1024 * 1024 // 16MB (must be >= 5MB for S3)
}

fn default_max_retries() -> usize {
    3
}

fn default_retry_backoff_ms() -> u64 {
    100
}

fn default_max_retry_backoff_ms() -> u64 {
    30000 // 30 seconds
}

impl Default for RemoteStorageConfig {
    fn default() -> Self {
        Self {
            batch_size_bytes: default_batch_size_bytes(),
            batch_timeout_ms: default_batch_timeout_ms(),
            upload_parallelism: default_upload_parallelism(),
            read_cache_bytes: default_read_cache_bytes(),
            use_express_zone: false,
            prefetch_enabled: default_prefetch_enabled(),
            prefetch_segments: default_prefetch_segments(),
            multipart_enabled: default_multipart_enabled(),
            multipart_threshold_bytes: default_multipart_threshold(),
            multipart_part_size_bytes: default_multipart_part_size(),
            max_retries: default_max_retries(),
            retry_backoff_ms: default_retry_backoff_ms(),
            max_retry_backoff_ms: default_max_retry_backoff_ms(),
        }
    }
}

impl RemoteStorageConfig {
    /// Create a new remote storage config with defaults
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a config optimized for low latency
    ///
    /// Uses smaller batches and shorter timeouts for faster acknowledgments.
    pub fn low_latency() -> Self {
        Self {
            batch_size_bytes: 256 * 1024, // 256KB
            batch_timeout_ms: 50,
            upload_parallelism: 8,
            read_cache_bytes: 256 * 1024 * 1024, // 256MB
            use_express_zone: true,
            prefetch_enabled: true,
            prefetch_segments: 4,
            multipart_enabled: true,
            multipart_threshold_bytes: 32 * 1024 * 1024, // 32MB
            multipart_part_size_bytes: 8 * 1024 * 1024,  // 8MB
            max_retries: 5,
            retry_backoff_ms: 50,
            max_retry_backoff_ms: 10000,
        }
    }

    /// Create a config optimized for high throughput
    ///
    /// Uses larger batches for better efficiency at the cost of latency.
    pub fn high_throughput() -> Self {
        Self {
            batch_size_bytes: 4 * 1024 * 1024, // 4MB
            batch_timeout_ms: 500,
            upload_parallelism: 16,
            read_cache_bytes: 512 * 1024 * 1024, // 512MB
            use_express_zone: false,
            prefetch_enabled: true,
            prefetch_segments: 4,
            multipart_enabled: true,
            multipart_threshold_bytes: 128 * 1024 * 1024, // 128MB
            multipart_part_size_bytes: 32 * 1024 * 1024,  // 32MB
            max_retries: 3,
            retry_backoff_ms: 100,
            max_retry_backoff_ms: 30000,
        }
    }

    /// Create a config optimized for cost savings
    ///
    /// Minimizes object storage requests at the cost of higher latency.
    pub fn cost_optimized() -> Self {
        Self {
            batch_size_bytes: 8 * 1024 * 1024, // 8MB
            batch_timeout_ms: 1000,
            upload_parallelism: 2,
            read_cache_bytes: 64 * 1024 * 1024, // 64MB
            use_express_zone: false,
            prefetch_enabled: false,
            prefetch_segments: 1,
            multipart_enabled: true,
            multipart_threshold_bytes: 256 * 1024 * 1024, // 256MB
            multipart_part_size_bytes: 64 * 1024 * 1024,  // 64MB
            max_retries: 2,
            retry_backoff_ms: 200,
            max_retry_backoff_ms: 60000,
        }
    }

    /// Get the batch timeout as a Duration
    pub fn batch_timeout(&self) -> Duration {
        Duration::from_millis(self.batch_timeout_ms)
    }

    /// Validate the configuration
    pub fn validate(&self) -> std::result::Result<(), String> {
        if self.batch_size_bytes == 0 {
            return Err("batch_size_bytes must be greater than 0".to_string());
        }
        if self.batch_size_bytes > 128 * 1024 * 1024 {
            return Err("batch_size_bytes cannot exceed 128MB".to_string());
        }
        if self.upload_parallelism == 0 {
            return Err("upload_parallelism must be greater than 0".to_string());
        }
        if self.upload_parallelism > 64 {
            return Err("upload_parallelism cannot exceed 64".to_string());
        }

        // Validate multipart upload settings
        if self.multipart_enabled {
            // S3 requires parts to be at least 5MB
            const MIN_PART_SIZE: usize = 5 * 1024 * 1024;
            if self.multipart_part_size_bytes < MIN_PART_SIZE {
                return Err(format!(
                    "multipart_part_size_bytes must be at least 5MB (S3 requirement), got {}",
                    self.multipart_part_size_bytes
                ));
            }

            // Maximum part size is 5GB for S3
            const MAX_PART_SIZE: usize = 5 * 1024 * 1024 * 1024;
            if self.multipart_part_size_bytes > MAX_PART_SIZE {
                return Err(format!(
                    "multipart_part_size_bytes cannot exceed 5GB (S3 limit), got {}",
                    self.multipart_part_size_bytes
                ));
            }

            // Threshold should be at least as large as part size
            if self.multipart_threshold_bytes < self.multipart_part_size_bytes {
                return Err(format!(
                    "multipart_threshold_bytes ({}) should be at least as large as multipart_part_size_bytes ({})",
                    self.multipart_threshold_bytes,
                    self.multipart_part_size_bytes
                ));
            }
        }

        // Validate retry settings
        if self.retry_backoff_ms == 0 {
            return Err("retry_backoff_ms must be greater than 0".to_string());
        }
        if self.max_retry_backoff_ms < self.retry_backoff_ms {
            return Err(format!(
                "max_retry_backoff_ms ({}) must be >= retry_backoff_ms ({})",
                self.max_retry_backoff_ms, self.retry_backoff_ms
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_mode_default() {
        let mode = StorageMode::default();
        assert_eq!(mode, StorageMode::Local);
        assert!(mode.is_local());
        assert!(!mode.requires_remote_storage());
    }

    #[test]
    fn test_storage_mode_properties() {
        assert!(StorageMode::Local.uses_local_wal());
        assert!(StorageMode::Local.uses_local_segments());
        assert!(StorageMode::Local.requires_replication());

        assert!(StorageMode::Hybrid.uses_local_wal());
        assert!(!StorageMode::Hybrid.uses_local_segments());
        assert!(StorageMode::Hybrid.requires_replication());

        assert!(!StorageMode::Diskless.uses_local_wal());
        assert!(!StorageMode::Diskless.uses_local_segments());
        assert!(!StorageMode::Diskless.requires_replication());
    }

    #[test]
    fn test_storage_mode_parse() {
        assert_eq!("local".parse::<StorageMode>().unwrap(), StorageMode::Local);
        assert_eq!(
            "hybrid".parse::<StorageMode>().unwrap(),
            StorageMode::Hybrid
        );
        assert_eq!(
            "diskless".parse::<StorageMode>().unwrap(),
            StorageMode::Diskless
        );
        assert_eq!(
            "DISKLESS".parse::<StorageMode>().unwrap(),
            StorageMode::Diskless
        );
        assert!("invalid".parse::<StorageMode>().is_err());
    }

    #[test]
    fn test_storage_mode_display() {
        assert_eq!(StorageMode::Local.to_string(), "local");
        assert_eq!(StorageMode::Hybrid.to_string(), "hybrid");
        assert_eq!(StorageMode::Diskless.to_string(), "diskless");
    }

    #[test]
    fn test_remote_storage_config_default() {
        let config = RemoteStorageConfig::default();
        assert_eq!(config.batch_size_bytes, 1024 * 1024);
        assert_eq!(config.batch_timeout_ms, 100);
        assert_eq!(config.upload_parallelism, 4);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_remote_storage_config_presets() {
        let low_latency = RemoteStorageConfig::low_latency();
        assert!(low_latency.use_express_zone);
        assert!(low_latency.batch_size_bytes < RemoteStorageConfig::default().batch_size_bytes);

        let high_throughput = RemoteStorageConfig::high_throughput();
        assert!(high_throughput.batch_size_bytes > RemoteStorageConfig::default().batch_size_bytes);

        let cost_opt = RemoteStorageConfig::cost_optimized();
        assert!(!cost_opt.prefetch_enabled);
    }

    #[test]
    fn test_remote_storage_config_validation() {
        // Test batch_size_bytes = 0
        let config = RemoteStorageConfig {
            batch_size_bytes: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());

        // Test upload_parallelism = 0
        let config = RemoteStorageConfig {
            upload_parallelism: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());

        // Test upload_parallelism too high
        let config = RemoteStorageConfig {
            upload_parallelism: 100,
            ..Default::default()
        };
        assert!(config.validate().is_err());

        // Test valid config
        let config = RemoteStorageConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_storage_mode_serde() {
        let mode = StorageMode::Diskless;
        let json = serde_json::to_string(&mode).unwrap();
        assert_eq!(json, "\"diskless\"");

        let parsed: StorageMode = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, StorageMode::Diskless);
    }
}
