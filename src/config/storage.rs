//! Storage configuration for Streamline
//!
//! This module provides configuration for the storage layer, including
//! WAL settings, segment sizes, and encryption options.

use serde::{Deserialize, Serialize};

use super::defaults::{
    DEFAULT_IN_MEMORY, DEFAULT_MAX_INDEX_ENTRIES, DEFAULT_MAX_MESSAGE_BYTES,
    DEFAULT_SEGMENT_MAX_BYTES, DEFAULT_SEGMENT_SYNC_INTERVAL_MS, DEFAULT_SEGMENT_SYNC_MODE,
};
use super::wal::WalConfig;

/// Storage configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// WAL configuration
    pub wal: WalConfig,

    /// Maximum segment size in bytes
    pub segment_max_bytes: u64,

    /// Segment sync mode ("none", "on_seal", "interval", "every_write")
    /// - "none": Only flush to OS buffer (fastest, least durable)
    /// - "on_seal": Sync when segment rotates (balanced)
    /// - "interval": Sync every N ms (configurable durability)
    /// - "every_write": Sync every batch (slowest, most durable)
    pub segment_sync_mode: String,

    /// Segment sync interval in milliseconds (used with "interval" mode)
    pub segment_sync_interval_ms: u64,

    /// Maximum message size in bytes
    pub max_message_bytes: u64,

    /// Maximum index entries per segment (prevents unbounded memory growth)
    pub max_index_entries: usize,

    /// Enable in-memory mode (no data persisted to disk)
    pub in_memory: bool,

    /// Encryption at rest configuration
    pub encryption: crate::storage::EncryptionConfig,

    /// Zero-copy networking configuration
    pub zerocopy: crate::storage::ZeroCopyConfig,

    /// I/O backend configuration
    pub io_backend: IoBackendConfig,
}

/// I/O backend configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IoBackendConfig {
    /// Enable io_uring backend on Linux (default: true)
    #[serde(default = "default_enable_io_uring")]
    pub enable_io_uring: bool,

    /// Enable SQPOLL mode for io_uring (default: true)
    /// SQPOLL eliminates syscalls for I/O submission, reducing latency by 10-15%.
    /// Requires Linux 5.4+ kernel and may need root or CAP_SYS_NICE capability.
    #[serde(default = "default_enable_sqpoll")]
    pub enable_sqpoll: bool,

    /// SQPOLL idle timeout in milliseconds (default: 1000)
    /// The kernel polling thread will idle after this period of no I/O activity.
    #[serde(default = "default_sqpoll_idle_timeout_ms")]
    pub sqpoll_idle_timeout_ms: u32,

    /// Buffer pool size per size class (default: 64)
    #[serde(default = "default_buffer_pool_size")]
    pub buffer_pool_size: usize,

    /// io_uring submission queue depth (default: 256, Linux only)
    #[serde(default = "default_sq_depth")]
    pub sq_depth: u32,

    /// Request queue depth for I/O operations (default: 1024)
    #[serde(default = "default_queue_depth")]
    pub queue_depth: usize,

    /// Pre-allocate WAL files for better performance (default: true)
    #[serde(default = "default_preallocate_wal")]
    pub preallocate_wal: bool,

    /// Pre-allocation size for WAL files in bytes (default: 64MB)
    #[serde(default = "default_preallocate_size")]
    pub preallocate_size: u64,

    /// Enable O_DIRECT I/O for bypassing the OS page cache (default: false)
    /// When enabled, all file I/O bypasses the kernel page cache, providing:
    /// - Predictable memory usage (no surprise evictions)
    /// - Reduced double-buffering overhead
    /// - Lower P99.99 latency spikes
    ///
    /// Note: Requires aligned buffers and may not work on all filesystems.
    #[serde(default = "default_enable_direct_io")]
    pub enable_direct_io: bool,

    /// Buffer alignment for O_DIRECT I/O (default: 4096 bytes)
    /// Buffers must be aligned to this boundary for O_DIRECT to work.
    /// Most filesystems require 512 or 4096 byte alignment.
    #[serde(default = "default_direct_io_alignment")]
    pub direct_io_alignment: usize,

    /// Fallback to buffered I/O if O_DIRECT is not supported (default: true)
    /// If the filesystem doesn't support O_DIRECT, fall back gracefully
    /// to standard buffered I/O instead of failing.
    #[serde(default = "default_direct_io_fallback")]
    pub direct_io_fallback: bool,
}

fn default_enable_io_uring() -> bool {
    true
}
fn default_enable_sqpoll() -> bool {
    true // Enabled by default for 10-15% I/O improvement
}
fn default_sqpoll_idle_timeout_ms() -> u32 {
    1000 // 1 second idle timeout
}
fn default_buffer_pool_size() -> usize {
    64
}
fn default_sq_depth() -> u32 {
    256
}
fn default_queue_depth() -> usize {
    1024
}
fn default_preallocate_wal() -> bool {
    true
}
fn default_preallocate_size() -> u64 {
    64 * 1024 * 1024
}
fn default_enable_direct_io() -> bool {
    false // Disabled by default for backwards compatibility
}
fn default_direct_io_alignment() -> usize {
    4096 // Standard page size alignment
}
fn default_direct_io_fallback() -> bool {
    true // Gracefully fall back to buffered I/O if O_DIRECT fails
}

impl Default for IoBackendConfig {
    fn default() -> Self {
        Self {
            enable_io_uring: default_enable_io_uring(),
            enable_sqpoll: default_enable_sqpoll(),
            sqpoll_idle_timeout_ms: default_sqpoll_idle_timeout_ms(),
            buffer_pool_size: default_buffer_pool_size(),
            sq_depth: default_sq_depth(),
            queue_depth: default_queue_depth(),
            preallocate_wal: default_preallocate_wal(),
            preallocate_size: default_preallocate_size(),
            enable_direct_io: default_enable_direct_io(),
            direct_io_alignment: default_direct_io_alignment(),
            direct_io_fallback: default_direct_io_fallback(),
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            wal: WalConfig::default(),
            segment_max_bytes: DEFAULT_SEGMENT_MAX_BYTES,
            segment_sync_mode: DEFAULT_SEGMENT_SYNC_MODE.to_string(),
            segment_sync_interval_ms: DEFAULT_SEGMENT_SYNC_INTERVAL_MS,
            max_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
            max_index_entries: DEFAULT_MAX_INDEX_ENTRIES,
            in_memory: DEFAULT_IN_MEMORY,
            encryption: crate::storage::EncryptionConfig::default(),
            zerocopy: crate::storage::ZeroCopyConfig::default(),
            io_backend: IoBackendConfig::default(),
        }
    }
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_config_default_values() {
        let config = StorageConfig::default();

        // Check segment/message size defaults
        assert_eq!(config.segment_max_bytes, 1024 * 1024 * 1024); // 1 GB
        assert_eq!(config.segment_sync_mode, "on_seal");
        assert_eq!(config.segment_sync_interval_ms, 100);
        assert_eq!(config.max_message_bytes, 1024 * 1024); // 1 MB
        assert_eq!(config.max_index_entries, 10_000);
        assert!(!config.in_memory);

        // Check encryption defaults
        assert!(!config.encryption.enabled);

        // Check zerocopy defaults
        assert!(config.zerocopy.enabled);
    }

    #[test]
    fn test_storage_config_wal_nested() {
        let config = StorageConfig::default();

        // Verify WAL defaults are properly nested
        assert!(config.wal.enabled);
        assert_eq!(config.wal.sync_interval_ms, 100);
        assert_eq!(config.wal.sync_mode, "interval");
    }

    #[test]
    fn test_storage_config_clone() {
        let original = StorageConfig::default();
        let cloned = original.clone();

        assert_eq!(original.segment_max_bytes, cloned.segment_max_bytes);
        assert_eq!(original.max_message_bytes, cloned.max_message_bytes);
        assert_eq!(original.max_index_entries, cloned.max_index_entries);
        assert_eq!(original.in_memory, cloned.in_memory);
    }

    #[test]
    fn test_storage_config_in_memory_mode() {
        let mut config = StorageConfig::default();
        config.in_memory = true;

        assert!(config.in_memory);
        // Other settings should still be configurable
        config.max_message_bytes = 2 * 1024 * 1024;
        assert_eq!(config.max_message_bytes, 2 * 1024 * 1024);
    }

    #[test]
    fn test_storage_config_custom_segment_size() {
        let mut config = StorageConfig::default();
        config.segment_max_bytes = 512 * 1024 * 1024; // 512 MB

        assert_eq!(config.segment_max_bytes, 512 * 1024 * 1024);
    }

    #[test]
    fn test_storage_config_custom_message_size() {
        let mut config = StorageConfig::default();
        config.max_message_bytes = 10 * 1024 * 1024; // 10 MB

        assert_eq!(config.max_message_bytes, 10 * 1024 * 1024);
    }

    #[test]
    fn test_storage_config_custom_index_entries() {
        let mut config = StorageConfig::default();
        config.max_index_entries = 50_000;

        assert_eq!(config.max_index_entries, 50_000);
    }

    #[test]
    fn test_storage_config_serde_roundtrip() {
        let config = StorageConfig::default();
        let serialized = serde_json::to_string(&config).unwrap();
        let deserialized: StorageConfig = serde_json::from_str(&serialized).unwrap();

        assert_eq!(config.segment_max_bytes, deserialized.segment_max_bytes);
        assert_eq!(config.max_message_bytes, deserialized.max_message_bytes);
        assert_eq!(config.max_index_entries, deserialized.max_index_entries);
        assert_eq!(config.in_memory, deserialized.in_memory);
        assert_eq!(config.wal.enabled, deserialized.wal.enabled);
        assert_eq!(config.encryption.enabled, deserialized.encryption.enabled);
        assert_eq!(config.zerocopy.enabled, deserialized.zerocopy.enabled);
    }

    #[test]
    fn test_storage_config_wal_validation() {
        let mut config = StorageConfig::default();

        // Default WAL config should validate
        assert!(config.wal.validate().is_ok());

        // Invalid WAL config should fail
        config.wal.shard_count = 0;
        assert!(config.wal.validate().is_err());
    }

    #[test]
    fn test_storage_config_with_encryption_enabled() {
        let mut config = StorageConfig::default();
        config.encryption.enabled = true;

        assert!(config.encryption.enabled);
    }

    #[test]
    fn test_storage_config_with_zerocopy_disabled() {
        let mut config = StorageConfig::default();
        config.zerocopy.enabled = false;

        assert!(!config.zerocopy.enabled);
    }
}
