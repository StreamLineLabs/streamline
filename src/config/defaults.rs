//! Default constants for Streamline configuration
//!
//! These constants define the default values used throughout the configuration
//! system when no explicit value is provided.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

/// Default listen address for Kafka protocol
pub const DEFAULT_KAFKA_ADDR: &str = "0.0.0.0:9092";

/// Default listen address for HTTP API
pub const DEFAULT_HTTP_ADDR: &str = "0.0.0.0:9094";

/// Default Kafka socket address (const, no parsing needed)
pub(crate) const DEFAULT_KAFKA_SOCKET_ADDR: SocketAddr =
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 9092);

/// Default HTTP socket address (const, no parsing needed)
pub(crate) const DEFAULT_HTTP_SOCKET_ADDR: SocketAddr =
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 9094);

/// Default data directory
pub const DEFAULT_DATA_DIR: &str = "./data";

/// Default log level
pub const DEFAULT_LOG_LEVEL: &str = "info";

/// Default WAL enabled state
pub const DEFAULT_WAL_ENABLED: bool = true;

/// Default WAL sync interval in milliseconds
pub const DEFAULT_WAL_SYNC_MS: u64 = 100;

/// Default WAL sync mode ("interval", "every_write", "os_default")
pub const DEFAULT_WAL_SYNC_MODE: &str = "interval";

/// Default WAL max file size in bytes (100 MB)
pub const DEFAULT_WAL_MAX_FILE_SIZE: u64 = 100 * 1024 * 1024;

/// Default number of WAL files to retain
pub const DEFAULT_WAL_RETENTION_FILES: u32 = 5;

/// Default maximum pending WAL writes before backpressure is applied
pub const DEFAULT_WAL_MAX_PENDING_WRITES: usize = 1000;

/// Default WAL sharding enabled state
pub const DEFAULT_WAL_SHARDING_ENABLED: bool = true;

/// Default number of WAL shards (for parallel writes)
pub const DEFAULT_WAL_SHARD_COUNT: u32 = 4;

/// Maximum number of WAL shards allowed
pub const MAX_WAL_SHARD_COUNT: u32 = 64;

/// Default WAL shard strategy ("topic" or "partition")
pub const DEFAULT_WAL_SHARD_STRATEGY: &str = "topic";

/// Default segment max size in bytes (1 GB)
pub const DEFAULT_SEGMENT_MAX_BYTES: u64 = 1024 * 1024 * 1024;

/// Default segment sync mode ("none", "on_seal", "interval", "every_write")
/// "on_seal" provides good balance - data syncs when segment rotates
pub const DEFAULT_SEGMENT_SYNC_MODE: &str = "on_seal";

/// Default segment sync interval in milliseconds (when using "interval" mode)
pub const DEFAULT_SEGMENT_SYNC_INTERVAL_MS: u64 = 100;

/// Default max message size in bytes (1 MB)
pub const DEFAULT_MAX_MESSAGE_BYTES: u64 = 1024 * 1024;

/// Default max index entries per segment (10,000)
/// Prevents unbounded memory growth for very large segments
pub const DEFAULT_MAX_INDEX_ENTRIES: usize = 10_000;

/// Default TLS minimum version
pub const DEFAULT_TLS_MIN_VERSION: &str = "1.2";

/// Default audit log path
pub const DEFAULT_AUDIT_LOG_PATH: &str = "./data/audit.log";

/// Default audit log format
pub const DEFAULT_AUDIT_LOG_FORMAT: &str = "json";

/// Default max connections (0 = unlimited)
pub const DEFAULT_MAX_CONNECTIONS: u32 = 10000;

/// Default max connections per IP (0 = unlimited)
pub const DEFAULT_MAX_CONNECTIONS_PER_IP: u32 = 100;

/// Default max request size in bytes (100 MB)
pub const DEFAULT_MAX_REQUEST_SIZE: u64 = 104857600;

/// Default connection idle timeout in seconds (10 minutes)
pub const DEFAULT_CONNECTION_IDLE_TIMEOUT_SECS: u64 = 600;

/// Default produce rate limit in bytes per second (50 MB/s)
pub const DEFAULT_PRODUCE_RATE_LIMIT_BYTES: u64 = 50_000_000;

/// Default shutdown timeout in seconds
pub const DEFAULT_SHUTDOWN_TIMEOUT_SECS: u64 = 30;

/// Default in-memory mode (false = persistent storage)
pub const DEFAULT_IN_MEMORY: bool = false;

/// Default auto-create topics setting (true for backward compatibility)
/// Set to false in production to prevent accidental topic creation from typos
pub const DEFAULT_AUTO_CREATE_TOPICS: bool = true;

/// Default runtime mode ("tokio" or "sharded")
/// - tokio: Standard Tokio multi-threaded runtime (default, backwards compatible)
/// - sharded: Thread-per-core runtime with CPU affinity for high performance
pub const DEFAULT_RUNTIME_MODE: &str = "tokio";

// TCP socket tuning options
/// Default TCP keepalive enabled state
pub const DEFAULT_TCP_KEEPALIVE_ENABLED: bool = true;

/// Default TCP keepalive idle time in seconds (time before first keepalive probe)
pub const DEFAULT_TCP_KEEPALIVE_IDLE_SECS: u32 = 60;

/// Default TCP keepalive interval in seconds (time between keepalive probes)
pub const DEFAULT_TCP_KEEPALIVE_INTERVAL_SECS: u32 = 10;

/// Default TCP keepalive retry count (number of failed probes before closing)
pub const DEFAULT_TCP_KEEPALIVE_RETRIES: u32 = 3;

/// Default TCP receive buffer size in bytes (256KB)
/// Set to 0 to use OS default
pub const DEFAULT_TCP_RECV_BUFFER_SIZE: u32 = 256 * 1024;

/// Default TCP send buffer size in bytes (256KB)
/// Set to 0 to use OS default
pub const DEFAULT_TCP_SEND_BUFFER_SIZE: u32 = 256 * 1024;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_addresses() {
        assert_eq!(DEFAULT_KAFKA_ADDR, "0.0.0.0:9092");
        assert_eq!(DEFAULT_HTTP_ADDR, "0.0.0.0:9094");
        assert_eq!(DEFAULT_KAFKA_SOCKET_ADDR.port(), 9092);
        assert_eq!(DEFAULT_HTTP_SOCKET_ADDR.port(), 9094);
    }

    #[test]
    fn test_default_paths() {
        assert_eq!(DEFAULT_DATA_DIR, "./data");
        assert_eq!(DEFAULT_AUDIT_LOG_PATH, "./data/audit.log");
    }

    #[test]
    #[allow(clippy::bool_assert_comparison)]
    fn test_wal_defaults() {
        assert_eq!(DEFAULT_WAL_ENABLED, true);
        assert_eq!(DEFAULT_WAL_SYNC_MS, 100);
        assert_eq!(DEFAULT_WAL_SYNC_MODE, "interval");
        assert_eq!(DEFAULT_WAL_MAX_FILE_SIZE, 100 * 1024 * 1024); // 100 MB
        assert_eq!(DEFAULT_WAL_RETENTION_FILES, 5);
        assert_eq!(DEFAULT_WAL_MAX_PENDING_WRITES, 1000);
    }

    #[test]
    #[allow(clippy::bool_assert_comparison, clippy::assertions_on_constants)]
    fn test_wal_sharding_defaults() {
        assert_eq!(DEFAULT_WAL_SHARDING_ENABLED, true);
        assert_eq!(DEFAULT_WAL_SHARD_COUNT, 4);
        assert!(DEFAULT_WAL_SHARD_COUNT <= MAX_WAL_SHARD_COUNT);
        assert_eq!(DEFAULT_WAL_SHARD_STRATEGY, "topic");
    }

    #[test]
    fn test_segment_defaults() {
        assert_eq!(DEFAULT_SEGMENT_MAX_BYTES, 1024 * 1024 * 1024); // 1 GB
        assert_eq!(DEFAULT_SEGMENT_SYNC_MODE, "on_seal");
        assert_eq!(DEFAULT_SEGMENT_SYNC_INTERVAL_MS, 100);
    }

    #[test]
    fn test_message_size_defaults() {
        assert_eq!(DEFAULT_MAX_MESSAGE_BYTES, 1024 * 1024); // 1 MB
        assert_eq!(DEFAULT_MAX_REQUEST_SIZE, 104857600); // 100 MB
    }

    #[test]
    fn test_connection_defaults() {
        assert_eq!(DEFAULT_MAX_CONNECTIONS, 10000);
        assert_eq!(DEFAULT_MAX_CONNECTIONS_PER_IP, 100);
        assert_eq!(DEFAULT_CONNECTION_IDLE_TIMEOUT_SECS, 600); // 10 minutes
    }

    #[test]
    #[allow(clippy::assertions_on_constants)]
    fn test_tcp_defaults() {
        assert!(DEFAULT_TCP_KEEPALIVE_ENABLED);
        assert_eq!(DEFAULT_TCP_KEEPALIVE_IDLE_SECS, 60);
        assert_eq!(DEFAULT_TCP_KEEPALIVE_INTERVAL_SECS, 10);
        assert_eq!(DEFAULT_TCP_KEEPALIVE_RETRIES, 3);
        assert_eq!(DEFAULT_TCP_RECV_BUFFER_SIZE, 256 * 1024); // 256 KB
        assert_eq!(DEFAULT_TCP_SEND_BUFFER_SIZE, 256 * 1024); // 256 KB
    }

    #[test]
    #[allow(clippy::assertions_on_constants)]
    fn test_misc_defaults() {
        assert_eq!(DEFAULT_LOG_LEVEL, "info");
        assert_eq!(DEFAULT_TLS_MIN_VERSION, "1.2");
        assert_eq!(DEFAULT_AUDIT_LOG_FORMAT, "json");
        assert!(!DEFAULT_IN_MEMORY);
        assert!(DEFAULT_AUTO_CREATE_TOPICS);
        assert_eq!(DEFAULT_PRODUCE_RATE_LIMIT_BYTES, 50_000_000); // 50 MB/s
        assert_eq!(DEFAULT_SHUTDOWN_TIMEOUT_SECS, 30);
    }

    #[test]
    fn test_max_index_entries() {
        assert_eq!(DEFAULT_MAX_INDEX_ENTRIES, 10_000);
    }

    #[test]
    fn test_socket_addr_ipv4() {
        assert!(DEFAULT_KAFKA_SOCKET_ADDR.ip().is_ipv4());
        assert!(DEFAULT_HTTP_SOCKET_ADDR.ip().is_ipv4());
        // 0.0.0.0 binds to all interfaces
        assert!(DEFAULT_KAFKA_SOCKET_ADDR.ip().is_unspecified());
        assert!(DEFAULT_HTTP_SOCKET_ADDR.ip().is_unspecified());
    }
}
