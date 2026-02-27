//! QUIC transport layer for high-performance inter-broker and client connections.
//!
//! Provides a connection manager with 0-RTT support, traffic accounting,
//! and per-connection statistics. This module handles the management plane;
//! the actual QUIC protocol handling is delegated to a separate layer.

#![cfg(feature = "quic")]

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the QUIC transport layer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuicTransportConfig {
    /// Address to bind the QUIC listener to.
    #[serde(default = "default_bind_addr")]
    pub bind_addr: String,

    /// Maximum number of concurrent bidirectional streams per connection.
    #[serde(default = "default_max_concurrent_streams")]
    pub max_concurrent_streams: u32,

    /// Whether to accept 0-RTT data from resumed connections.
    #[serde(default = "default_enable_0rtt")]
    pub enable_0rtt: bool,

    /// Connection idle timeout in milliseconds.
    #[serde(default = "default_idle_timeout_ms")]
    pub idle_timeout_ms: u64,

    /// Keep-alive probe interval in milliseconds.
    #[serde(default = "default_keep_alive_interval_ms")]
    pub keep_alive_interval_ms: u64,

    /// Maximum UDP datagram payload size in bytes.
    #[serde(default = "default_max_datagram_size")]
    pub max_datagram_size: usize,

    /// Initial congestion window size in bytes.
    #[serde(default = "default_initial_window_size")]
    pub initial_window_size: u32,
}

fn default_bind_addr() -> String {
    "0.0.0.0:9093".to_string()
}
fn default_max_concurrent_streams() -> u32 {
    100
}
fn default_enable_0rtt() -> bool {
    true
}
fn default_idle_timeout_ms() -> u64 {
    30_000
}
fn default_keep_alive_interval_ms() -> u64 {
    10_000
}
fn default_max_datagram_size() -> usize {
    1350
}
fn default_initial_window_size() -> u32 {
    14_720
}

impl Default for QuicTransportConfig {
    fn default() -> Self {
        Self {
            bind_addr: default_bind_addr(),
            max_concurrent_streams: default_max_concurrent_streams(),
            enable_0rtt: default_enable_0rtt(),
            idle_timeout_ms: default_idle_timeout_ms(),
            keep_alive_interval_ms: default_keep_alive_interval_ms(),
            max_datagram_size: default_max_datagram_size(),
            initial_window_size: default_initial_window_size(),
        }
    }
}

// ---------------------------------------------------------------------------
// Connection info
// ---------------------------------------------------------------------------

/// Metadata for a single tracked QUIC connection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuicConnectionInfo {
    /// Unique connection identifier.
    pub id: String,
    /// Remote peer address.
    pub remote_addr: String,
    /// Local address the connection was accepted on.
    pub local_addr: String,
    /// Protocol version string.
    pub protocol: String,
    /// Smoothed round-trip time in microseconds.
    pub rtt_us: u64,
    /// Unix timestamp (seconds) when the connection was established.
    pub established_at: u64,
    /// Cumulative bytes sent on this connection.
    pub bytes_sent: u64,
    /// Cumulative bytes received on this connection.
    pub bytes_received: u64,
    /// Total number of streams opened on this connection.
    pub streams_opened: u64,
    /// Whether the connection was established via 0-RTT.
    pub is_0rtt: bool,
}

// ---------------------------------------------------------------------------
// Aggregate stats (lock-free)
// ---------------------------------------------------------------------------

/// Aggregate, lock-free statistics for the QUIC transport layer.
#[derive(Debug, Default)]
pub struct QuicTransportStats {
    pub total_connections: AtomicU64,
    pub active_connections: AtomicU64,
    pub bytes_sent: AtomicU64,
    pub bytes_received: AtomicU64,
    pub zero_rtt_connections: AtomicU64,
    pub handshake_failures: AtomicU64,
    pub total_streams: AtomicU64,
}

/// Serialisable point-in-time snapshot of [`QuicTransportStats`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QuicTransportStatsSnapshot {
    pub total_connections: u64,
    pub active_connections: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub zero_rtt_connections: u64,
    pub handshake_failures: u64,
    pub total_streams: u64,
}

impl QuicTransportStats {
    /// Capture a consistent snapshot of the current counters.
    pub fn snapshot(&self) -> QuicTransportStatsSnapshot {
        QuicTransportStatsSnapshot {
            total_connections: self.total_connections.load(Ordering::Relaxed),
            active_connections: self.active_connections.load(Ordering::Relaxed),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
            zero_rtt_connections: self.zero_rtt_connections.load(Ordering::Relaxed),
            handshake_failures: self.handshake_failures.load(Ordering::Relaxed),
            total_streams: self.total_streams.load(Ordering::Relaxed),
        }
    }
}

// ---------------------------------------------------------------------------
// Transport manager
// ---------------------------------------------------------------------------

/// Manages the set of active QUIC connections and their aggregate statistics.
pub struct QuicTransportManager {
    config: QuicTransportConfig,
    connections: Arc<RwLock<HashMap<String, QuicConnectionInfo>>>,
    stats: Arc<QuicTransportStats>,
}

impl QuicTransportManager {
    /// Create a new manager with the given configuration.
    pub fn new(config: QuicTransportConfig) -> Self {
        info!(
            bind_addr = %config.bind_addr,
            max_streams = config.max_concurrent_streams,
            enable_0rtt = config.enable_0rtt,
            "Initialising QUIC transport manager"
        );
        Self {
            config,
            connections: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(QuicTransportStats::default()),
        }
    }

    /// Register a newly established connection.
    ///
    /// Updates aggregate counters (total, active, 0-RTT) and stores the
    /// connection metadata for later retrieval.
    pub async fn register_connection(&self, info: QuicConnectionInfo) {
        let id = info.id.clone();
        let is_0rtt = info.is_0rtt;

        self.connections.write().await.insert(id.clone(), info);

        self.stats.total_connections.fetch_add(1, Ordering::Relaxed);
        self.stats.active_connections.fetch_add(1, Ordering::Relaxed);
        if is_0rtt {
            self.stats
                .zero_rtt_connections
                .fetch_add(1, Ordering::Relaxed);
        }

        debug!(connection_id = %id, "QUIC connection registered");
    }

    /// Remove a connection by its identifier.
    ///
    /// Returns the removed [`QuicConnectionInfo`] if found and decrements the
    /// active-connections counter.
    pub async fn remove_connection(&self, id: &str) -> Option<QuicConnectionInfo> {
        let removed = self.connections.write().await.remove(id);
        if removed.is_some() {
            self.stats.active_connections.fetch_sub(1, Ordering::Relaxed);
            debug!(connection_id = %id, "QUIC connection removed");
        } else {
            warn!(connection_id = %id, "Attempted to remove unknown QUIC connection");
        }
        removed
    }

    /// Look up a connection by its identifier.
    pub async fn get_connection(&self, id: &str) -> Option<QuicConnectionInfo> {
        self.connections.read().await.get(id).cloned()
    }

    /// Return a snapshot of all currently tracked connections.
    pub async fn list_connections(&self) -> Vec<QuicConnectionInfo> {
        self.connections.read().await.values().cloned().collect()
    }

    /// Account for traffic on a given connection.
    ///
    /// Both per-connection and aggregate counters are updated. If the
    /// connection is not found the aggregate counters are still bumped so
    /// that global accounting remains accurate.
    pub async fn record_traffic(&self, id: &str, bytes_sent: u64, bytes_received: u64) {
        // Aggregate counters (always updated)
        self.stats.bytes_sent.fetch_add(bytes_sent, Ordering::Relaxed);
        self.stats
            .bytes_received
            .fetch_add(bytes_received, Ordering::Relaxed);

        // Per-connection counters
        let mut conns = self.connections.write().await;
        if let Some(conn) = conns.get_mut(id) {
            conn.bytes_sent += bytes_sent;
            conn.bytes_received += bytes_received;
        } else {
            warn!(connection_id = %id, "Traffic recorded for unknown connection");
        }
    }

    /// Return a reference-counted handle to the aggregate stats.
    pub fn stats(&self) -> Arc<QuicTransportStats> {
        Arc::clone(&self.stats)
    }

    /// Produce a serialisable snapshot of the aggregate statistics.
    pub fn snapshot(&self) -> QuicTransportStatsSnapshot {
        self.stats.snapshot()
    }

    /// Access the current configuration.
    pub fn config(&self) -> &QuicTransportConfig {
        &self.config
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Return the current Unix timestamp in seconds.
fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Convenience builder for [`QuicConnectionInfo`] used in tests and internal
/// callers.
pub fn make_connection_info(
    id: impl Into<String>,
    remote_addr: impl Into<String>,
    local_addr: impl Into<String>,
    is_0rtt: bool,
) -> QuicConnectionInfo {
    QuicConnectionInfo {
        id: id.into(),
        remote_addr: remote_addr.into(),
        local_addr: local_addr.into(),
        protocol: "quic-v1".to_string(),
        rtt_us: 0,
        established_at: unix_now(),
        bytes_sent: 0,
        bytes_received: 0,
        streams_opened: 0,
        is_0rtt,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // Helper: build a default manager.
    fn manager() -> QuicTransportManager {
        QuicTransportManager::new(QuicTransportConfig::default())
    }

    fn conn(id: &str, is_0rtt: bool) -> QuicConnectionInfo {
        make_connection_info(id, "127.0.0.1:5000", "0.0.0.0:9093", is_0rtt)
    }

    // -- Config defaults -----------------------------------------------------

    #[test]
    fn test_config_defaults() {
        let cfg = QuicTransportConfig::default();
        assert_eq!(cfg.bind_addr, "0.0.0.0:9093");
        assert_eq!(cfg.max_concurrent_streams, 100);
        assert!(cfg.enable_0rtt);
        assert_eq!(cfg.idle_timeout_ms, 30_000);
        assert_eq!(cfg.keep_alive_interval_ms, 10_000);
        assert_eq!(cfg.max_datagram_size, 1350);
        assert_eq!(cfg.initial_window_size, 14_720);
    }

    #[test]
    fn test_config_serde_roundtrip() {
        let cfg = QuicTransportConfig::default();
        let json = serde_json::to_string(&cfg).expect("serialize");
        let cfg2: QuicTransportConfig = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(cfg2.bind_addr, cfg.bind_addr);
        assert_eq!(cfg2.max_concurrent_streams, cfg.max_concurrent_streams);
        assert_eq!(cfg2.enable_0rtt, cfg.enable_0rtt);
    }

    #[test]
    fn test_config_serde_defaults_from_empty_object() {
        let cfg: QuicTransportConfig = serde_json::from_str("{}").expect("deserialize");
        assert_eq!(cfg.bind_addr, "0.0.0.0:9093");
        assert_eq!(cfg.max_concurrent_streams, 100);
    }

    // -- Connection registration / removal -----------------------------------

    #[tokio::test]
    async fn test_register_connection() {
        let mgr = manager();
        mgr.register_connection(conn("c1", false)).await;

        let c = mgr.get_connection("c1").await;
        assert!(c.is_some());
        assert_eq!(c.unwrap().remote_addr, "127.0.0.1:5000");
    }

    #[tokio::test]
    async fn test_remove_connection() {
        let mgr = manager();
        mgr.register_connection(conn("c1", false)).await;

        let removed = mgr.remove_connection("c1").await;
        assert!(removed.is_some());
        assert!(mgr.get_connection("c1").await.is_none());
    }

    #[tokio::test]
    async fn test_remove_unknown_connection_returns_none() {
        let mgr = manager();
        assert!(mgr.remove_connection("nonexistent").await.is_none());
    }

    #[tokio::test]
    async fn test_get_unknown_connection_returns_none() {
        let mgr = manager();
        assert!(mgr.get_connection("nope").await.is_none());
    }

    // -- List connections ----------------------------------------------------

    #[tokio::test]
    async fn test_list_connections() {
        let mgr = manager();
        mgr.register_connection(conn("c1", false)).await;
        mgr.register_connection(conn("c2", true)).await;

        let list = mgr.list_connections().await;
        assert_eq!(list.len(), 2);
    }

    #[tokio::test]
    async fn test_list_empty() {
        let mgr = manager();
        assert!(mgr.list_connections().await.is_empty());
    }

    // -- Traffic accounting --------------------------------------------------

    #[tokio::test]
    async fn test_record_traffic() {
        let mgr = manager();
        mgr.register_connection(conn("c1", false)).await;
        mgr.record_traffic("c1", 100, 200).await;

        let c = mgr.get_connection("c1").await.unwrap();
        assert_eq!(c.bytes_sent, 100);
        assert_eq!(c.bytes_received, 200);
    }

    #[tokio::test]
    async fn test_record_traffic_accumulates() {
        let mgr = manager();
        mgr.register_connection(conn("c1", false)).await;
        mgr.record_traffic("c1", 50, 60).await;
        mgr.record_traffic("c1", 50, 40).await;

        let c = mgr.get_connection("c1").await.unwrap();
        assert_eq!(c.bytes_sent, 100);
        assert_eq!(c.bytes_received, 100);
    }

    #[tokio::test]
    async fn test_record_traffic_unknown_still_updates_aggregate() {
        let mgr = manager();
        mgr.record_traffic("ghost", 10, 20).await;

        let snap = mgr.snapshot();
        assert_eq!(snap.bytes_sent, 10);
        assert_eq!(snap.bytes_received, 20);
    }

    // -- Stats / snapshot ----------------------------------------------------

    #[tokio::test]
    async fn test_stats_after_register_and_remove() {
        let mgr = manager();
        mgr.register_connection(conn("c1", false)).await;
        mgr.register_connection(conn("c2", true)).await;

        let snap = mgr.snapshot();
        assert_eq!(snap.total_connections, 2);
        assert_eq!(snap.active_connections, 2);
        assert_eq!(snap.zero_rtt_connections, 1);

        mgr.remove_connection("c1").await;
        let snap = mgr.snapshot();
        assert_eq!(snap.active_connections, 1);
        assert_eq!(snap.total_connections, 2); // total never decreases
    }

    #[tokio::test]
    async fn test_stats_handshake_failures() {
        let mgr = manager();
        mgr.stats().handshake_failures.fetch_add(3, Ordering::Relaxed);
        assert_eq!(mgr.snapshot().handshake_failures, 3);
    }

    #[tokio::test]
    async fn test_stats_total_streams() {
        let mgr = manager();
        mgr.stats().total_streams.fetch_add(42, Ordering::Relaxed);
        assert_eq!(mgr.snapshot().total_streams, 42);
    }

    #[test]
    fn test_stats_snapshot_serde_roundtrip() {
        let snap = QuicTransportStatsSnapshot {
            total_connections: 10,
            active_connections: 5,
            bytes_sent: 1024,
            bytes_received: 2048,
            zero_rtt_connections: 3,
            handshake_failures: 1,
            total_streams: 20,
        };
        let json = serde_json::to_string(&snap).expect("serialize");
        let snap2: QuicTransportStatsSnapshot = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(snap, snap2);
    }

    // -- Misc ----------------------------------------------------------------

    #[test]
    fn test_connection_info_protocol_default() {
        let c = conn("c1", false);
        assert_eq!(c.protocol, "quic-v1");
    }

    #[test]
    fn test_make_connection_info_0rtt_flag() {
        let c = make_connection_info("x", "1.2.3.4:1234", "0.0.0.0:9093", true);
        assert!(c.is_0rtt);
        let c2 = make_connection_info("y", "1.2.3.4:1234", "0.0.0.0:9093", false);
        assert!(!c2.is_0rtt);
    }
}
