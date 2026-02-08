//! TCP socket configuration
//!
//! This module provides configuration for TCP socket options used by Streamline's
//! network connections. It controls keepalive settings and buffer sizes for optimal
//! network performance.
//!
//! # Example
//!
//! ```
//! use streamline::config::TcpConfig;
//!
//! let config = TcpConfig {
//!     keepalive_enabled: true,
//!     keepalive_idle_secs: 60,
//!     keepalive_interval_secs: 10,
//!     keepalive_retries: 3,
//!     recv_buffer_size: 256 * 1024,
//!     send_buffer_size: 256 * 1024,
//! };
//! ```

use serde::{Deserialize, Serialize};

use super::defaults::{
    DEFAULT_TCP_KEEPALIVE_ENABLED, DEFAULT_TCP_KEEPALIVE_IDLE_SECS,
    DEFAULT_TCP_KEEPALIVE_INTERVAL_SECS, DEFAULT_TCP_KEEPALIVE_RETRIES,
    DEFAULT_TCP_RECV_BUFFER_SIZE, DEFAULT_TCP_SEND_BUFFER_SIZE,
};

/// TCP socket configuration for network connections.
///
/// Controls keepalive behavior and buffer sizes for TCP connections.
/// These settings affect connection reliability and throughput performance.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TcpConfig {
    /// Enable TCP keepalive
    pub keepalive_enabled: bool,

    /// TCP keepalive idle time in seconds (time before first keepalive probe)
    pub keepalive_idle_secs: u32,

    /// TCP keepalive interval in seconds (time between keepalive probes)
    pub keepalive_interval_secs: u32,

    /// TCP keepalive retry count (number of failed probes before closing connection)
    pub keepalive_retries: u32,

    /// TCP receive buffer size in bytes (SO_RCVBUF)
    /// Set to 0 to use OS default
    pub recv_buffer_size: u32,

    /// TCP send buffer size in bytes (SO_SNDBUF)
    /// Set to 0 to use OS default
    pub send_buffer_size: u32,
}

impl Default for TcpConfig {
    fn default() -> Self {
        Self {
            keepalive_enabled: DEFAULT_TCP_KEEPALIVE_ENABLED,
            keepalive_idle_secs: DEFAULT_TCP_KEEPALIVE_IDLE_SECS,
            keepalive_interval_secs: DEFAULT_TCP_KEEPALIVE_INTERVAL_SECS,
            keepalive_retries: DEFAULT_TCP_KEEPALIVE_RETRIES,
            recv_buffer_size: DEFAULT_TCP_RECV_BUFFER_SIZE,
            send_buffer_size: DEFAULT_TCP_SEND_BUFFER_SIZE,
        }
    }
}

impl TcpConfig {
    /// Create a new TcpConfig with default settings
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a configuration optimized for high-throughput workloads
    pub fn high_throughput() -> Self {
        Self {
            keepalive_enabled: true,
            keepalive_idle_secs: 120,
            keepalive_interval_secs: 30,
            keepalive_retries: 5,
            recv_buffer_size: 1024 * 1024, // 1MB
            send_buffer_size: 1024 * 1024, // 1MB
        }
    }

    /// Create a configuration optimized for low-latency workloads
    pub fn low_latency() -> Self {
        Self {
            keepalive_enabled: true,
            keepalive_idle_secs: 30,
            keepalive_interval_secs: 5,
            keepalive_retries: 3,
            recv_buffer_size: 64 * 1024, // 64KB
            send_buffer_size: 64 * 1024, // 64KB
        }
    }

    /// Check if the configuration uses OS default buffer sizes
    pub fn uses_os_default_buffers(&self) -> bool {
        self.recv_buffer_size == 0 && self.send_buffer_size == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = TcpConfig::default();
        assert!(config.keepalive_enabled);
        assert_eq!(config.keepalive_idle_secs, 60);
        assert_eq!(config.keepalive_interval_secs, 10);
        assert_eq!(config.keepalive_retries, 3);
        assert_eq!(config.recv_buffer_size, 256 * 1024);
        assert_eq!(config.send_buffer_size, 256 * 1024);
    }

    #[test]
    fn test_new_equals_default() {
        assert_eq!(TcpConfig::new(), TcpConfig::default());
    }

    #[test]
    fn test_high_throughput_config() {
        let config = TcpConfig::high_throughput();
        assert!(config.keepalive_enabled);
        assert_eq!(config.recv_buffer_size, 1024 * 1024);
        assert_eq!(config.send_buffer_size, 1024 * 1024);
        // Longer keepalive intervals for stable connections
        assert!(config.keepalive_idle_secs > TcpConfig::default().keepalive_idle_secs);
    }

    #[test]
    fn test_low_latency_config() {
        let config = TcpConfig::low_latency();
        assert!(config.keepalive_enabled);
        // Smaller buffers for lower latency
        assert!(config.recv_buffer_size < TcpConfig::default().recv_buffer_size);
        // Faster keepalive detection
        assert!(config.keepalive_idle_secs < TcpConfig::default().keepalive_idle_secs);
    }

    #[test]
    fn test_uses_os_default_buffers() {
        let config = TcpConfig::default();
        assert!(!config.uses_os_default_buffers());

        let os_default = TcpConfig {
            recv_buffer_size: 0,
            send_buffer_size: 0,
            ..Default::default()
        };
        assert!(os_default.uses_os_default_buffers());
    }

    #[test]
    fn test_serialization() {
        let config = TcpConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: TcpConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config, deserialized);
    }

    #[test]
    fn test_clone() {
        let config = TcpConfig::high_throughput();
        let cloned = config.clone();
        assert_eq!(config, cloned);
    }

    #[test]
    fn test_debug_format() {
        let config = TcpConfig::default();
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("TcpConfig"));
        assert!(debug_str.contains("keepalive_enabled"));
    }
}
