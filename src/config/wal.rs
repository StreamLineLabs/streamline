//! Write-Ahead Log (WAL) configuration
//!
//! The WAL ensures durability by persisting records before acknowledging writes to clients.

use serde::{Deserialize, Serialize};

use super::defaults::{
    DEFAULT_WAL_ENABLED, DEFAULT_WAL_MAX_FILE_SIZE, DEFAULT_WAL_MAX_PENDING_WRITES,
    DEFAULT_WAL_RETENTION_FILES, DEFAULT_WAL_SHARDING_ENABLED, DEFAULT_WAL_SHARD_COUNT,
    DEFAULT_WAL_SHARD_STRATEGY, DEFAULT_WAL_SYNC_MODE, DEFAULT_WAL_SYNC_MS, MAX_WAL_SHARD_COUNT,
};

/// WAL (Write-Ahead Log) configuration
///
/// The Write-Ahead Log ensures durability by persisting records before acknowledging
/// writes to clients. The `sync_mode` setting controls when data is flushed to disk,
/// which directly impacts both durability guarantees and performance.
///
/// ## Durability vs Performance Trade-offs
///
/// | Sync Mode     | Durability              | Performance | Use Case                              |
/// |---------------|-------------------------|-------------|---------------------------------------|
/// | `every_write` | Highest (no data loss)  | Slowest     | Financial systems, audit logs         |
/// | `interval`    | Bounded loss (100ms)    | Balanced    | Most production workloads (default)   |
/// | `os_default`  | Unbounded (OS-dependent)| Fastest     | Development, non-critical data        |
///
/// ## Data Loss Scenarios
///
/// - **`every_write`**: No data loss on crash (fsync after every produce request)
/// - **`interval`**: Up to `sync_interval_ms` worth of data may be lost on crash
/// - **`os_default`**: Data loss depends on OS buffer settings (potentially minutes of data)
///
/// For production deployments requiring strong durability, use `every_write` or `interval`
/// with a low sync interval. For development or ephemeral workloads, `os_default` provides
/// maximum throughput.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalConfig {
    /// Enable WAL (default: true)
    ///
    /// **PRODUCTION WARNING**: Disabling WAL removes durability guarantees.
    /// Data exists only in memory until segments are written to disk, which
    /// can take several seconds. A crash will lose all uncommitted data.
    /// Only disable for development/testing or ephemeral workloads.
    pub enabled: bool,

    /// Sync interval in milliseconds (default: 100ms)
    ///
    /// Only used when `sync_mode` is `"interval"`.
    ///
    /// **Trade-offs**:
    /// - Lower values (10-50ms): Better durability, higher disk I/O, ~10-20% throughput reduction
    /// - Default (100ms): Balanced - max 100ms data loss on crash, good throughput
    /// - Higher values (500ms+): Better throughput, but more data at risk on crash
    ///
    /// Values below 10ms may cause significant performance degradation due to
    /// frequent fsync operations.
    pub sync_interval_ms: u64,

    /// Sync mode: "interval", "every_write", "os_default"
    ///
    /// Controls when WAL data is flushed to disk:
    ///
    /// - `"every_write"`: Fsync after every produce request
    ///   - **Durability**: Maximum (no data loss possible)
    ///   - **Performance**: Lowest (~50-70% throughput reduction)
    ///   - **Use case**: Financial data, audit logs, critical systems
    ///
    /// - `"interval"` (default): Fsync every `sync_interval_ms` milliseconds
    ///   - **Durability**: Up to `sync_interval_ms` of data may be lost on crash
    ///   - **Performance**: Good (~10-20% reduction with 100ms interval)
    ///   - **Use case**: Most production workloads
    ///
    /// - `"os_default"`: Rely on OS buffer flushing
    ///   - **Durability**: Several minutes of data may be lost on crash
    ///   - **Performance**: Maximum (no fsync overhead)
    ///   - **Use case**: Development, testing, or when data can be regenerated
    ///
    /// # Production Warning
    ///
    /// **DO NOT use `os_default` in production** unless data loss is acceptable.
    /// The OS may buffer writes for 5-30+ seconds before flushing to disk.
    /// A power failure or OS crash will lose ALL buffered data with no recovery.
    pub sync_mode: String,

    /// Max WAL file size before rotation (default: 100MB)
    pub max_file_size: u64,

    /// Number of WAL files to retain
    pub retention_files: u32,

    /// Maximum pending writes before backpressure is applied (default: 1000)
    ///
    /// When writes arrive faster than they can be persisted, this limit prevents
    /// unbounded memory growth by rejecting new writes with an error. The caller
    /// should retry after a short delay when this occurs.
    pub max_pending_writes: usize,

    /// Enable WAL sharding for improved throughput (default: true)
    ///
    /// When enabled, writes are distributed across multiple WAL shards based on the
    /// configured strategy. This eliminates the single WAL bottleneck and allows
    /// parallel writes to different topics/partitions.
    ///
    /// **Benefits**:
    /// - Higher throughput: Multiple disk I/O operations in parallel
    /// - Reduced lock contention: Each shard has its own lock
    /// - Better SSD utilization: Parallel writes to different disk regions
    ///
    /// **Trade-offs**:
    /// - More file handles: `shard_count` files instead of 1
    /// - Slightly more complex recovery: Must merge shards on restart
    pub sharding_enabled: bool,

    /// Number of WAL shards (default: 4, max: 64)
    ///
    /// More shards = better parallelism but more file handles and slightly higher
    /// memory overhead. Recommended values:
    /// - 1: Equivalent to non-sharded WAL (backward compatible)
    /// - 4: Good default for most workloads
    /// - 8-16: High-throughput systems with many topics
    /// - 32-64: Extreme throughput requirements (many thousands of topics)
    pub shard_count: u32,

    /// Shard selection strategy: "topic" (default) or "partition"
    ///
    /// - `"topic"`: All partitions of a topic go to the same shard
    ///   - Better locality for topic-centric workloads
    ///   - Simpler recovery (topic data in fewer shards)
    ///
    /// - `"partition"`: Each partition is assigned independently
    ///   - Better distribution for topics with many partitions
    ///   - May spread single topic across all shards
    pub shard_strategy: String,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            enabled: DEFAULT_WAL_ENABLED,
            sync_interval_ms: DEFAULT_WAL_SYNC_MS,
            sync_mode: DEFAULT_WAL_SYNC_MODE.to_string(),
            max_file_size: DEFAULT_WAL_MAX_FILE_SIZE,
            retention_files: DEFAULT_WAL_RETENTION_FILES,
            max_pending_writes: DEFAULT_WAL_MAX_PENDING_WRITES,
            sharding_enabled: DEFAULT_WAL_SHARDING_ENABLED,
            shard_count: DEFAULT_WAL_SHARD_COUNT,
            shard_strategy: DEFAULT_WAL_SHARD_STRATEGY.to_string(),
        }
    }
}

impl WalConfig {
    /// Validate the configuration values
    pub fn validate(&self) -> Result<(), String> {
        if self.shard_count == 0 {
            return Err("shard_count must be at least 1".to_string());
        }
        if self.shard_count > MAX_WAL_SHARD_COUNT {
            return Err(format!(
                "shard_count {} exceeds maximum of {}",
                self.shard_count, MAX_WAL_SHARD_COUNT
            ));
        }
        if self.shard_strategy != "topic" && self.shard_strategy != "partition" {
            return Err(format!(
                "Invalid shard_strategy '{}': must be 'topic' or 'partition'",
                self.shard_strategy
            ));
        }
        Ok(())
    }

    /// Get the effective shard count (1 if sharding is disabled)
    pub fn effective_shard_count(&self) -> u32 {
        if self.sharding_enabled {
            self.shard_count
        } else {
            1
        }
    }
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;

    #[test]
    fn test_default_values() {
        let config = WalConfig::default();

        assert!(config.enabled);
        assert_eq!(config.sync_interval_ms, 100);
        assert_eq!(config.sync_mode, "interval");
        assert_eq!(config.max_file_size, 100 * 1024 * 1024); // 100 MB
        assert_eq!(config.retention_files, 5);
        assert_eq!(config.max_pending_writes, 1000);
        assert!(config.sharding_enabled);
        assert_eq!(config.shard_count, 4);
        assert_eq!(config.shard_strategy, "topic");
    }

    #[test]
    fn test_default_config_validates() {
        let config = WalConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_shard_count_zero() {
        let mut config = WalConfig::default();
        config.shard_count = 0;

        let result = config.validate();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "shard_count must be at least 1");
    }

    #[test]
    fn test_validate_shard_count_exceeds_max() {
        let mut config = WalConfig::default();
        config.shard_count = 65; // MAX_WAL_SHARD_COUNT is 64

        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("exceeds maximum"));
    }

    #[test]
    fn test_validate_shard_count_at_max() {
        let mut config = WalConfig::default();
        config.shard_count = 64; // MAX_WAL_SHARD_COUNT

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_shard_strategy_topic() {
        let mut config = WalConfig::default();
        config.shard_strategy = "topic".to_string();

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_shard_strategy_partition() {
        let mut config = WalConfig::default();
        config.shard_strategy = "partition".to_string();

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_shard_strategy_invalid() {
        let mut config = WalConfig::default();
        config.shard_strategy = "invalid".to_string();

        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid shard_strategy"));
    }

    #[test]
    fn test_effective_shard_count_sharding_enabled() {
        let mut config = WalConfig::default();
        config.sharding_enabled = true;
        config.shard_count = 8;

        assert_eq!(config.effective_shard_count(), 8);
    }

    #[test]
    fn test_effective_shard_count_sharding_disabled() {
        let mut config = WalConfig::default();
        config.sharding_enabled = false;
        config.shard_count = 8;

        assert_eq!(config.effective_shard_count(), 1);
    }

    #[test]
    fn test_serde_roundtrip() {
        let config = WalConfig::default();
        let serialized = serde_json::to_string(&config).unwrap();
        let deserialized: WalConfig = serde_json::from_str(&serialized).unwrap();

        assert_eq!(config.enabled, deserialized.enabled);
        assert_eq!(config.sync_interval_ms, deserialized.sync_interval_ms);
        assert_eq!(config.sync_mode, deserialized.sync_mode);
        assert_eq!(config.max_file_size, deserialized.max_file_size);
        assert_eq!(config.retention_files, deserialized.retention_files);
        assert_eq!(config.max_pending_writes, deserialized.max_pending_writes);
        assert_eq!(config.sharding_enabled, deserialized.sharding_enabled);
        assert_eq!(config.shard_count, deserialized.shard_count);
        assert_eq!(config.shard_strategy, deserialized.shard_strategy);
    }

    #[test]
    fn test_serde_deserialize_custom_values() {
        let json = r#"{
            "enabled": false,
            "sync_interval_ms": 50,
            "sync_mode": "every_write",
            "max_file_size": 52428800,
            "retention_files": 10,
            "max_pending_writes": 500,
            "sharding_enabled": false,
            "shard_count": 16,
            "shard_strategy": "partition"
        }"#;

        let config: WalConfig = serde_json::from_str(json).unwrap();

        assert!(!config.enabled);
        assert_eq!(config.sync_interval_ms, 50);
        assert_eq!(config.sync_mode, "every_write");
        assert_eq!(config.max_file_size, 50 * 1024 * 1024); // 50 MB
        assert_eq!(config.retention_files, 10);
        assert_eq!(config.max_pending_writes, 500);
        assert!(!config.sharding_enabled);
        assert_eq!(config.shard_count, 16);
        assert_eq!(config.shard_strategy, "partition");
    }
}
