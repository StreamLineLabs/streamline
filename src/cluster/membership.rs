//! Node discovery and membership management
//!
//! This module handles node discovery, heartbeat tracking, and failure detection.

use crate::cluster::config::ClusterConfig;
use crate::cluster::node::{BrokerInfo, NodeId, NodeState, PeerInfo};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::interval;
use tracing::{debug, info, warn};

/// Peer discovery and membership manager
pub struct MembershipManager {
    /// Local node ID
    node_id: NodeId,

    /// Cluster configuration
    config: ClusterConfig,

    /// Known peers
    peers: Arc<RwLock<HashMap<NodeId, PeerInfo>>>,

    /// Shutdown signal
    shutdown: Arc<RwLock<bool>>,
}

impl MembershipManager {
    /// Create a new membership manager
    pub fn new(node_id: NodeId, config: ClusterConfig) -> Self {
        Self {
            node_id,
            config,
            peers: Arc::new(RwLock::new(HashMap::new())),
            shutdown: Arc::new(RwLock::new(false)),
        }
    }

    /// Get local node ID
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Get all known peers
    pub async fn peers(&self) -> HashMap<NodeId, PeerInfo> {
        self.peers.read().clone()
    }

    /// Get a specific peer
    pub async fn get_peer(&self, node_id: NodeId) -> Option<BrokerInfo> {
        self.peers.read().get(&node_id).map(|p| p.broker.clone())
    }

    /// Add or update a peer
    pub async fn add_peer(&self, broker: BrokerInfo) {
        let mut peers = self.peers.write();
        let node_id = broker.node_id;

        if let Some(peer) = peers.get_mut(&node_id) {
            // Update existing peer
            peer.broker = broker;
            peer.record_heartbeat();
            debug!(node_id, "Updated existing peer");
        } else {
            // Add new peer
            info!(node_id, "Adding new peer");
            peers.insert(node_id, PeerInfo::new(broker));
        }
    }

    /// Remove a peer
    pub async fn remove_peer(&self, node_id: NodeId) {
        let mut peers = self.peers.write();
        if peers.remove(&node_id).is_some() {
            info!(node_id, "Removed peer");
        }
    }

    /// Record a heartbeat from a peer
    pub async fn record_heartbeat(&self, node_id: NodeId) {
        let mut peers = self.peers.write();
        if let Some(peer) = peers.get_mut(&node_id) {
            peer.record_heartbeat();
            debug!(node_id, "Recorded heartbeat");
        }
    }

    /// Record a connection failure for a peer
    pub async fn record_failure(&self, node_id: NodeId) {
        let mut peers = self.peers.write();
        if let Some(peer) = peers.get_mut(&node_id) {
            peer.record_failure();
            warn!(
                node_id,
                failed_attempts = peer.failed_attempts,
                "Recorded connection failure"
            );
        }
    }

    /// Get all alive peers
    pub async fn alive_peers(&self) -> Vec<BrokerInfo> {
        let peers = self.peers.read();
        peers
            .values()
            .filter(|p| p.is_alive(self.config.session_timeout_ms))
            .map(|p| p.broker.clone())
            .collect()
    }

    /// Check if a peer is alive
    pub async fn is_peer_alive(&self, node_id: NodeId) -> bool {
        let peers = self.peers.read();
        peers
            .get(&node_id)
            .map(|p| p.is_alive(self.config.session_timeout_ms))
            .unwrap_or(false)
    }

    /// Start peer discovery from seed nodes
    pub async fn discover_peers(&self) -> Result<Vec<BrokerInfo>, std::io::Error> {
        let discovered = Vec::new();

        for seed_addr in &self.config.seed_nodes {
            debug!(%seed_addr, "Attempting to connect to seed node");

            match TcpStream::connect(seed_addr).await {
                Ok(_stream) => {
                    info!(%seed_addr, "Connected to seed node");
                    // In a real implementation, we would exchange broker info here
                    // For now, we just mark the connection as successful
                }
                Err(e) => {
                    warn!(
                        %seed_addr,
                        error = %e,
                        "Failed to connect to seed node"
                    );
                }
            }
        }

        Ok(discovered)
    }

    /// Start the heartbeat and failure detection task
    pub async fn start_failure_detection(&self) {
        let peers = self.peers.clone();
        let shutdown = self.shutdown.clone();
        let timeout_ms = self.config.session_timeout_ms;
        let interval_ms = self.config.heartbeat_interval_ms;

        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_millis(interval_ms));

            loop {
                ticker.tick().await;

                // Check shutdown
                if *shutdown.read() {
                    break;
                }

                // Check for dead peers
                let mut peers_guard = peers.write();
                for (node_id, peer) in peers_guard.iter_mut() {
                    if !peer.is_alive(timeout_ms) && peer.broker.state != NodeState::Dead {
                        warn!(
                            node_id = *node_id,
                            ms_since_heartbeat = peer.ms_since_heartbeat(),
                            "Peer marked as dead"
                        );
                        peer.broker.state = NodeState::Dead;
                    }
                }
            }
        });
    }

    /// Signal shutdown
    pub async fn shutdown(&self) {
        *self.shutdown.write() = true;
    }
}

/// Result of a peer discovery attempt
#[derive(Debug)]
pub struct DiscoveryResult {
    /// Discovered brokers
    pub brokers: Vec<BrokerInfo>,

    /// Failed seed nodes
    pub failed: Vec<SocketAddr>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> ClusterConfig {
        ClusterConfig {
            node_id: 1,
            advertised_addr: "127.0.0.1:9092".parse().unwrap(),
            inter_broker_addr: "127.0.0.1:9093".parse().unwrap(),
            seed_nodes: vec![],
            session_timeout_ms: 10000,
            heartbeat_interval_ms: 500,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_membership_manager_creation() {
        let config = test_config();
        let manager = MembershipManager::new(1, config);

        assert_eq!(manager.node_id(), 1);
        assert!(manager.peers().await.is_empty());
    }

    #[tokio::test]
    async fn test_add_peer() {
        let config = test_config();
        let manager = MembershipManager::new(1, config);

        let broker = BrokerInfo::new(
            2,
            "127.0.0.1:9094".parse().unwrap(),
            "127.0.0.1:9095".parse().unwrap(),
        );

        manager.add_peer(broker.clone()).await;

        let peers = manager.peers().await;
        assert_eq!(peers.len(), 1);
        assert!(peers.contains_key(&2));
    }

    #[tokio::test]
    async fn test_alive_peers() {
        let config = test_config();
        let manager = MembershipManager::new(1, config);

        let broker = BrokerInfo {
            node_id: 2,
            advertised_addr: "127.0.0.1:9094".parse().unwrap(),
            inter_broker_addr: "127.0.0.1:9095".parse().unwrap(),
            rack: None,
            datacenter: None,
            state: NodeState::Running,
        };

        manager.add_peer(broker).await;

        let alive = manager.alive_peers().await;
        assert_eq!(alive.len(), 1);
    }

    #[tokio::test]
    async fn test_record_heartbeat() {
        let config = test_config();
        let manager = MembershipManager::new(1, config);

        let broker = BrokerInfo::new(
            2,
            "127.0.0.1:9094".parse().unwrap(),
            "127.0.0.1:9095".parse().unwrap(),
        );

        manager.add_peer(broker).await;
        manager.record_heartbeat(2).await;

        assert!(manager.is_peer_alive(2).await);
    }

    #[tokio::test]
    async fn test_remove_peer() {
        let config = test_config();
        let manager = MembershipManager::new(1, config);

        let broker = BrokerInfo::new(
            2,
            "127.0.0.1:9094".parse().unwrap(),
            "127.0.0.1:9095".parse().unwrap(),
        );

        manager.add_peer(broker).await;
        assert_eq!(manager.peers().await.len(), 1);

        manager.remove_peer(2).await;
        assert!(manager.peers().await.is_empty());
    }
}
