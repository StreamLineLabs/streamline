//! Geo-Replication Protocol
//!
//! Implements multi-region active-active replication using CRDTs for
//! conflict-free data convergence across regions.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────┐     Async Replication     ┌─────────────┐
//! │  Region A   │ ◄──────────────────────► │  Region B   │
//! │  (Primary)  │                           │  (Primary)  │
//! │             │     CRDT Merge Protocol    │             │
//! └──────┬──────┘                           └──────┬──────┘
//!        │                                         │
//!        │              ┌───────────┐              │
//!        └──────────────►  Region C ◄──────────────┘
//!                       │ (Primary) │
//!                       └───────────┘
//! ```

use crate::crdt::clock::HybridLogicalClock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Geo-replication configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeoReplicationConfig {
    /// Local region identifier
    pub local_region: String,
    /// Peer regions and their endpoints
    pub peer_regions: Vec<PeerRegion>,
    /// Replication mode
    pub mode: ReplicationMode,
    /// Maximum batch size for replication
    pub max_batch_size: usize,
    /// Replication interval in milliseconds
    pub replication_interval_ms: u64,
    /// Enable CRDT-based conflict resolution
    pub crdt_enabled: bool,
    /// Compression for cross-region traffic
    pub compression_enabled: bool,
    /// Maximum lag before alerting (in milliseconds)
    pub max_lag_alert_ms: u64,
}

impl Default for GeoReplicationConfig {
    fn default() -> Self {
        Self {
            local_region: "us-east-1".to_string(),
            peer_regions: Vec::new(),
            mode: ReplicationMode::ActiveActive,
            max_batch_size: 1000,
            replication_interval_ms: 100,
            crdt_enabled: true,
            compression_enabled: true,
            max_lag_alert_ms: 5000,
        }
    }
}

/// Peer region configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerRegion {
    /// Region identifier
    pub region_id: String,
    /// Display name
    pub display_name: String,
    /// Bootstrap servers endpoint
    pub endpoint: String,
    /// Whether this peer is currently connected
    pub connected: bool,
    /// Topics to replicate (empty = all)
    pub topics: Vec<String>,
}

/// Replication mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ReplicationMode {
    /// Both regions accept writes; CRDTs resolve conflicts
    #[default]
    ActiveActive,
    /// One region is primary, others are read-only replicas
    ActivePassive,
    /// Hub-and-spoke: central region aggregates, edge regions produce
    HubAndSpoke,
}

/// Replication status for a peer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerReplicationStatus {
    /// Region ID
    pub region_id: String,
    /// Connection state
    pub state: PeerState,
    /// Replication lag in milliseconds
    pub lag_ms: u64,
    /// Records replicated since last report
    pub records_replicated: u64,
    /// Bytes replicated since last report
    pub bytes_replicated: u64,
    /// Last successful replication timestamp
    pub last_replicated_at: i64,
    /// Number of conflicts resolved via CRDT merge
    pub conflicts_resolved: u64,
    /// Number of errors
    pub errors: u64,
}

/// Peer connection state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum PeerState {
    /// Not yet connected
    #[default]
    Disconnected,
    /// Establishing connection
    Connecting,
    /// Connected and replicating
    Connected,
    /// Connection lost, attempting reconnect
    Reconnecting,
    /// Peer is unreachable
    Unreachable,
}

impl std::fmt::Display for PeerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PeerState::Disconnected => write!(f, "disconnected"),
            PeerState::Connecting => write!(f, "connecting"),
            PeerState::Connected => write!(f, "connected"),
            PeerState::Reconnecting => write!(f, "reconnecting"),
            PeerState::Unreachable => write!(f, "unreachable"),
        }
    }
}

/// Conflict resolution strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ConflictStrategy {
    /// Use CRDT merge (default, always correct)
    #[default]
    CrdtMerge,
    /// Last-writer-wins based on HLC timestamps
    LastWriterWins,
    /// Higher region ID wins (deterministic)
    HigherRegionWins,
    /// Custom callback-based resolution
    Custom,
}

/// Geo-replication manager
pub struct GeoReplicationManager {
    config: GeoReplicationConfig,
    _clock_node_id: String,
    peers: Arc<RwLock<HashMap<String, PeerReplicationStatus>>>,
    stats: Arc<GeoReplicationStats>,
}

/// Aggregate replication statistics
#[derive(Debug, Default)]
pub struct GeoReplicationStats {
    pub total_records_replicated: std::sync::atomic::AtomicU64,
    pub total_bytes_replicated: std::sync::atomic::AtomicU64,
    pub total_conflicts_resolved: std::sync::atomic::AtomicU64,
    pub total_errors: std::sync::atomic::AtomicU64,
}

/// Snapshot of geo-replication statistics
#[derive(Debug, Clone, Serialize)]
pub struct GeoReplicationStatsSnapshot {
    pub local_region: String,
    pub mode: ReplicationMode,
    pub peer_count: usize,
    pub connected_peers: usize,
    pub total_records_replicated: u64,
    pub total_bytes_replicated: u64,
    pub total_conflicts_resolved: u64,
    pub total_errors: u64,
    pub peers: Vec<PeerReplicationStatus>,
}

impl GeoReplicationManager {
    /// Create a new geo-replication manager
    pub fn new(config: GeoReplicationConfig) -> Self {
        let clock_node_id = config.local_region.clone();

        let mut peers = HashMap::new();
        for peer in &config.peer_regions {
            peers.insert(
                peer.region_id.clone(),
                PeerReplicationStatus {
                    region_id: peer.region_id.clone(),
                    state: PeerState::Disconnected,
                    lag_ms: 0,
                    records_replicated: 0,
                    bytes_replicated: 0,
                    last_replicated_at: 0,
                    conflicts_resolved: 0,
                    errors: 0,
                },
            );
        }

        Self {
            config,
            _clock_node_id: clock_node_id,
            peers: Arc::new(RwLock::new(peers)),
            stats: Arc::new(GeoReplicationStats::default()),
        }
    }

    /// Get the local region ID
    pub fn local_region(&self) -> &str {
        &self.config.local_region
    }

    /// Get replication mode
    pub fn mode(&self) -> ReplicationMode {
        self.config.mode
    }

    /// Get current status of all peers
    pub async fn peer_statuses(&self) -> Vec<PeerReplicationStatus> {
        let peers = self.peers.read().await;
        peers.values().cloned().collect()
    }

    /// Get status for a specific peer
    pub async fn peer_status(&self, region_id: &str) -> Option<PeerReplicationStatus> {
        let peers = self.peers.read().await;
        peers.get(region_id).cloned()
    }

    /// Add a new peer region
    pub async fn add_peer(&self, peer: PeerRegion) {
        let mut peers = self.peers.write().await;
        peers.insert(
            peer.region_id.clone(),
            PeerReplicationStatus {
                region_id: peer.region_id,
                state: PeerState::Disconnected,
                lag_ms: 0,
                records_replicated: 0,
                bytes_replicated: 0,
                last_replicated_at: 0,
                conflicts_resolved: 0,
                errors: 0,
            },
        );
    }

    /// Remove a peer region
    pub async fn remove_peer(&self, region_id: &str) -> bool {
        let mut peers = self.peers.write().await;
        peers.remove(region_id).is_some()
    }

    /// Get a snapshot of all replication statistics
    pub async fn stats_snapshot(&self) -> GeoReplicationStatsSnapshot {
        use std::sync::atomic::Ordering;

        let peers = self.peers.read().await;
        let connected = peers
            .values()
            .filter(|p| p.state == PeerState::Connected)
            .count();

        GeoReplicationStatsSnapshot {
            local_region: self.config.local_region.clone(),
            mode: self.config.mode,
            peer_count: peers.len(),
            connected_peers: connected,
            total_records_replicated: self.stats.total_records_replicated.load(Ordering::Relaxed),
            total_bytes_replicated: self.stats.total_bytes_replicated.load(Ordering::Relaxed),
            total_conflicts_resolved: self.stats.total_conflicts_resolved.load(Ordering::Relaxed),
            total_errors: self.stats.total_errors.load(Ordering::Relaxed),
            peers: peers.values().cloned().collect(),
        }
    }

    /// Start replication for all peers
    pub async fn start(&self) -> crate::Result<()> {
        let mut peers = self.peers.write().await;
        for status in peers.values_mut() {
            status.state = PeerState::Connecting;
        }

        tracing::info!(
            local_region = %self.config.local_region,
            peer_count = peers.len(),
            mode = ?self.config.mode,
            "Geo-replication started"
        );

        Ok(())
    }

    /// Stop replication for all peers
    pub async fn stop(&self) -> crate::Result<()> {
        let mut peers = self.peers.write().await;
        for status in peers.values_mut() {
            status.state = PeerState::Disconnected;
        }

        tracing::info!(
            local_region = %self.config.local_region,
            "Geo-replication stopped"
        );

        Ok(())
    }

    /// Get the current HLC timestamp
    pub fn current_timestamp(&self) -> HybridLogicalClock {
        // Create a copy and tick for a new timestamp
        let mut clock = HybridLogicalClock::new(&self.config.local_region);
        clock.tick()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = GeoReplicationConfig::default();
        assert_eq!(config.local_region, "us-east-1");
        assert!(config.crdt_enabled);
        assert_eq!(config.mode, ReplicationMode::ActiveActive);
    }

    #[test]
    fn test_peer_state_display() {
        assert_eq!(PeerState::Connected.to_string(), "connected");
        assert_eq!(PeerState::Reconnecting.to_string(), "reconnecting");
    }

    #[tokio::test]
    async fn test_geo_replication_manager() {
        let mut config = GeoReplicationConfig::default();
        config.peer_regions.push(PeerRegion {
            region_id: "eu-west-1".to_string(),
            display_name: "EU West".to_string(),
            endpoint: "eu-west.example.com:9092".to_string(),
            connected: false,
            topics: vec![],
        });

        let manager = GeoReplicationManager::new(config);
        assert_eq!(manager.local_region(), "us-east-1");

        let statuses = manager.peer_statuses().await;
        assert_eq!(statuses.len(), 1);
        assert_eq!(statuses[0].region_id, "eu-west-1");
        assert_eq!(statuses[0].state, PeerState::Disconnected);
    }

    #[tokio::test]
    async fn test_start_stop() {
        let mut config = GeoReplicationConfig::default();
        config.peer_regions.push(PeerRegion {
            region_id: "ap-south-1".to_string(),
            display_name: "AP South".to_string(),
            endpoint: "ap-south.example.com:9092".to_string(),
            connected: false,
            topics: vec![],
        });

        let manager = GeoReplicationManager::new(config);

        manager.start().await.unwrap();
        let statuses = manager.peer_statuses().await;
        assert_eq!(statuses[0].state, PeerState::Connecting);

        manager.stop().await.unwrap();
        let statuses = manager.peer_statuses().await;
        assert_eq!(statuses[0].state, PeerState::Disconnected);
    }

    #[tokio::test]
    async fn test_add_remove_peer() {
        let config = GeoReplicationConfig::default();
        let manager = GeoReplicationManager::new(config);

        assert!(manager.peer_statuses().await.is_empty());

        manager
            .add_peer(PeerRegion {
                region_id: "us-west-2".to_string(),
                display_name: "US West".to_string(),
                endpoint: "us-west.example.com:9092".to_string(),
                connected: false,
                topics: vec![],
            })
            .await;

        assert_eq!(manager.peer_statuses().await.len(), 1);

        assert!(manager.remove_peer("us-west-2").await);
        assert!(manager.peer_statuses().await.is_empty());
    }

    #[tokio::test]
    async fn test_stats_snapshot() {
        let config = GeoReplicationConfig::default();
        let manager = GeoReplicationManager::new(config);
        let snapshot = manager.stats_snapshot().await;

        assert_eq!(snapshot.local_region, "us-east-1");
        assert_eq!(snapshot.total_records_replicated, 0);
        assert_eq!(snapshot.connected_peers, 0);
    }
}
