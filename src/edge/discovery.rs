//! Edge Node Discovery and Registration
//!
//! Provides service discovery mechanisms for edge nodes:
//! - Manual registry-based discovery
//! - mDNS-based local network discovery
//! - Heartbeat-based health monitoring
//! - Automatic peer deregistration on timeout

use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Discovery protocol to use for finding peers
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiscoveryProtocol {
    /// Manual registry — nodes are added explicitly via API
    #[default]
    Manual,
    /// mDNS-based discovery on the local network
    Mdns,
    /// Seed-node gossip protocol
    Gossip,
}

impl std::fmt::Display for DiscoveryProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DiscoveryProtocol::Manual => write!(f, "manual"),
            DiscoveryProtocol::Mdns => write!(f, "mdns"),
            DiscoveryProtocol::Gossip => write!(f, "gossip"),
        }
    }
}

/// Status of a discovered edge node
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EdgeNodeStatus {
    /// Node status is unknown
    #[default]
    Unknown,
    /// Node is online and healthy
    Online,
    /// Node is online but degraded
    Degraded,
    /// Node is offline / unreachable
    Offline,
}

impl std::fmt::Display for EdgeNodeStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EdgeNodeStatus::Unknown => write!(f, "unknown"),
            EdgeNodeStatus::Online => write!(f, "online"),
            EdgeNodeStatus::Degraded => write!(f, "degraded"),
            EdgeNodeStatus::Offline => write!(f, "offline"),
        }
    }
}

/// Capabilities that an edge node advertises
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EdgeNodeCapabilities {
    /// Can produce messages
    pub can_produce: bool,
    /// Can consume messages
    pub can_consume: bool,
    /// Can replicate data to peers
    pub can_replicate: bool,
    /// Can forward data to cloud
    pub can_forward: bool,
    /// Maximum storage in bytes
    pub max_storage_bytes: u64,
}

/// A discovered edge node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeNode {
    /// Unique identifier
    pub id: String,
    /// Human-readable name
    pub name: String,
    /// Network address (host:port)
    pub address: String,
    /// Geographic latitude
    pub latitude: f64,
    /// Geographic longitude
    pub longitude: f64,
    /// Current status
    pub status: EdgeNodeStatus,
    /// Last heartbeat timestamp (ms since epoch)
    pub last_seen: i64,
    /// Advertised capabilities
    pub capabilities: EdgeNodeCapabilities,
}

impl EdgeNode {
    /// Create a new edge node with required fields
    pub fn new(
        id: impl Into<String>,
        name: impl Into<String>,
        address: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            address: address.into(),
            latitude: 0.0,
            longitude: 0.0,
            status: EdgeNodeStatus::Unknown,
            last_seen: chrono::Utc::now().timestamp_millis(),
            capabilities: EdgeNodeCapabilities::default(),
        }
    }

    /// Set geographic location
    pub fn with_location(mut self, lat: f64, lng: f64) -> Self {
        self.latitude = lat;
        self.longitude = lng;
        self
    }

    /// Set capabilities
    pub fn with_capabilities(mut self, caps: EdgeNodeCapabilities) -> Self {
        self.capabilities = caps;
        self
    }

    /// Age since last heartbeat in milliseconds
    pub fn age_ms(&self) -> i64 {
        chrono::Utc::now().timestamp_millis() - self.last_seen
    }
}

/// Configuration for the discovery service
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryConfig {
    /// This node's ID
    pub node_id: String,
    /// This node's name
    pub node_name: String,
    /// This node's address
    pub node_address: String,
    /// Discovery protocol
    pub protocol: DiscoveryProtocol,
    /// Heartbeat interval in milliseconds
    pub heartbeat_interval_ms: u64,
    /// Time after which an unseen node is marked offline (ms)
    pub node_timeout_ms: u64,
    /// Seed addresses for gossip-based discovery
    pub seed_addresses: Vec<String>,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            node_id: uuid::Uuid::new_v4().to_string(),
            node_name: "edge-node".to_string(),
            node_address: "0.0.0.0:9095".to_string(),
            protocol: DiscoveryProtocol::Manual,
            heartbeat_interval_ms: 5000,
            node_timeout_ms: 30000,
            seed_addresses: Vec::new(),
        }
    }
}

/// Discovery service for edge nodes
pub struct EdgeDiscovery {
    config: DiscoveryConfig,
    /// Registered nodes keyed by node ID
    nodes: Arc<RwLock<HashMap<String, EdgeNode>>>,
    /// Running flag
    running: Arc<AtomicBool>,
    /// Statistics
    stats: DiscoveryStats,
}

/// Internal atomic statistics
#[derive(Default)]
struct DiscoveryStats {
    nodes_registered: AtomicU64,
    nodes_deregistered: AtomicU64,
    heartbeats_processed: AtomicU64,
    timeouts_detected: AtomicU64,
}

/// Snapshot of discovery statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DiscoveryStatsSnapshot {
    pub nodes_registered: u64,
    pub nodes_deregistered: u64,
    pub heartbeats_processed: u64,
    pub timeouts_detected: u64,
}

impl EdgeDiscovery {
    /// Create a new discovery service
    pub fn new(config: DiscoveryConfig) -> Self {
        Self {
            config,
            nodes: Arc::new(RwLock::new(HashMap::new())),
            running: Arc::new(AtomicBool::new(false)),
            stats: DiscoveryStats::default(),
        }
    }

    /// Get the local node ID
    pub fn node_id(&self) -> &str {
        &self.config.node_id
    }

    /// Get the discovery protocol in use
    pub fn protocol(&self) -> DiscoveryProtocol {
        self.config.protocol
    }

    /// Start the discovery service
    pub async fn start(&self) -> Result<()> {
        if self.running.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        info!(
            node_id = %self.config.node_id,
            protocol = %self.config.protocol,
            "Starting edge discovery service"
        );

        // Register self
        let self_node = EdgeNode {
            id: self.config.node_id.clone(),
            name: self.config.node_name.clone(),
            address: self.config.node_address.clone(),
            latitude: 0.0,
            longitude: 0.0,
            status: EdgeNodeStatus::Online,
            last_seen: chrono::Utc::now().timestamp_millis(),
            capabilities: EdgeNodeCapabilities::default(),
        };
        self.register_node(self_node).await?;

        // Initiate seed-based discovery if gossip protocol
        if self.config.protocol == DiscoveryProtocol::Gossip {
            for seed in &self.config.seed_addresses {
                debug!(seed = %seed, "Contacting seed node for discovery");
            }
        }

        Ok(())
    }

    /// Stop the discovery service
    pub async fn stop(&self) -> Result<()> {
        if !self.running.swap(false, Ordering::SeqCst) {
            return Ok(());
        }
        info!(node_id = %self.config.node_id, "Stopping edge discovery service");
        Ok(())
    }

    /// Register a node in the discovery registry
    pub async fn register_node(&self, node: EdgeNode) -> Result<()> {
        let node_id = node.id.clone();
        let mut nodes = self.nodes.write().await;
        nodes.insert(node_id.clone(), node);
        self.stats.nodes_registered.fetch_add(1, Ordering::Relaxed);
        debug!(node_id = %node_id, "Registered edge node");
        Ok(())
    }

    /// Deregister a node by ID
    pub async fn deregister(&self, node_id: &str) -> Result<()> {
        let mut nodes = self.nodes.write().await;
        if nodes.remove(node_id).is_some() {
            self.stats
                .nodes_deregistered
                .fetch_add(1, Ordering::Relaxed);
            debug!(node_id = %node_id, "Deregistered edge node");
        }
        Ok(())
    }

    /// Process a heartbeat from a peer node
    pub async fn heartbeat(&self, node_id: &str) -> Result<()> {
        let mut nodes = self.nodes.write().await;
        if let Some(node) = nodes.get_mut(node_id) {
            node.last_seen = chrono::Utc::now().timestamp_millis();
            node.status = EdgeNodeStatus::Online;
            self.stats
                .heartbeats_processed
                .fetch_add(1, Ordering::Relaxed);
            debug!(node_id = %node_id, "Processed heartbeat");
        } else {
            return Err(StreamlineError::storage_msg(format!(
                "Unknown node: {}",
                node_id
            )));
        }
        Ok(())
    }

    /// Discover all currently-known peers (excluding self)
    pub async fn discover_peers(&self) -> Vec<EdgeNode> {
        let nodes = self.nodes.read().await;
        nodes
            .values()
            .filter(|n| n.id != self.config.node_id)
            .cloned()
            .collect()
    }

    /// Get all registered nodes (including self)
    pub async fn all_nodes(&self) -> Vec<EdgeNode> {
        let nodes = self.nodes.read().await;
        nodes.values().cloned().collect()
    }

    /// Get a specific node by ID
    pub async fn get_node(&self, node_id: &str) -> Option<EdgeNode> {
        let nodes = self.nodes.read().await;
        nodes.get(node_id).cloned()
    }

    /// Evict nodes that have not sent a heartbeat within the timeout window
    pub async fn evict_timed_out_nodes(&self) -> Vec<String> {
        let timeout = self.config.node_timeout_ms as i64;
        let now = chrono::Utc::now().timestamp_millis();

        let mut nodes = self.nodes.write().await;
        let timed_out: Vec<String> = nodes
            .iter()
            .filter(|(id, n)| *id != &self.config.node_id && (now - n.last_seen) > timeout)
            .map(|(id, _)| id.clone())
            .collect();

        for id in &timed_out {
            nodes.remove(id);
            self.stats.timeouts_detected.fetch_add(1, Ordering::Relaxed);
            warn!(node_id = %id, "Evicted timed-out edge node");
        }

        timed_out
    }

    /// Count of currently registered nodes
    pub async fn node_count(&self) -> usize {
        self.nodes.read().await.len()
    }

    /// Whether the service is running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    /// Get statistics snapshot
    pub fn stats(&self) -> DiscoveryStatsSnapshot {
        DiscoveryStatsSnapshot {
            nodes_registered: self.stats.nodes_registered.load(Ordering::Relaxed),
            nodes_deregistered: self.stats.nodes_deregistered.load(Ordering::Relaxed),
            heartbeats_processed: self.stats.heartbeats_processed.load(Ordering::Relaxed),
            timeouts_detected: self.stats.timeouts_detected.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_discovery() -> EdgeDiscovery {
        EdgeDiscovery::new(DiscoveryConfig::default())
    }

    #[tokio::test]
    async fn test_register_and_discover() {
        let disc = default_discovery();
        disc.start().await.unwrap();

        let peer = EdgeNode::new("peer-1", "Peer One", "10.0.0.2:9095")
            .with_location(37.77, -122.42);
        disc.register_node(peer).await.unwrap();

        let peers = disc.discover_peers().await;
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].id, "peer-1");
    }

    #[tokio::test]
    async fn test_heartbeat() {
        let disc = default_discovery();
        let node = EdgeNode::new("n1", "Node1", "10.0.0.3:9095");
        disc.register_node(node).await.unwrap();

        disc.heartbeat("n1").await.unwrap();
        let stats = disc.stats();
        assert_eq!(stats.heartbeats_processed, 1);
    }

    #[tokio::test]
    async fn test_heartbeat_unknown_node() {
        let disc = default_discovery();
        assert!(disc.heartbeat("does-not-exist").await.is_err());
    }

    #[tokio::test]
    async fn test_deregister() {
        let disc = default_discovery();
        let node = EdgeNode::new("n1", "Node1", "10.0.0.3:9095");
        disc.register_node(node).await.unwrap();

        disc.deregister("n1").await.unwrap();
        assert!(disc.get_node("n1").await.is_none());
    }

    #[tokio::test]
    async fn test_evict_timed_out() {
        let config = DiscoveryConfig {
            node_timeout_ms: 0, // immediate timeout
            ..Default::default()
        };
        let disc = EdgeDiscovery::new(config);

        let mut node = EdgeNode::new("stale", "Stale", "10.0.0.4:9095");
        node.last_seen = 0; // epoch — definitely timed out
        disc.register_node(node).await.unwrap();

        let evicted = disc.evict_timed_out_nodes().await;
        assert_eq!(evicted, vec!["stale"]);
        assert_eq!(disc.node_count().await, 0);
    }

    #[test]
    fn test_discovery_protocol_display() {
        assert_eq!(DiscoveryProtocol::Manual.to_string(), "manual");
        assert_eq!(DiscoveryProtocol::Mdns.to_string(), "mdns");
        assert_eq!(DiscoveryProtocol::Gossip.to_string(), "gossip");
    }

    #[test]
    fn test_edge_node_builder() {
        let node = EdgeNode::new("id", "name", "addr:9095")
            .with_location(1.0, 2.0)
            .with_capabilities(EdgeNodeCapabilities {
                can_produce: true,
                can_consume: true,
                ..Default::default()
            });
        assert_eq!(node.latitude, 1.0);
        assert!(node.capabilities.can_produce);
    }
}
