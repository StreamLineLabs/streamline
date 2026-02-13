//! Geo-Distributed Edge Mesh
//!
//! This module provides geographic distribution capabilities for edge nodes:
//! - Multi-region mesh topology with automatic peer discovery
//! - Geographic-aware routing for optimal latency
//! - Region failover and load balancing
//! - Cross-region data replication
//! - Network partition handling
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                     Cloud Control Plane                      │
//! │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐          │
//! │  │ Region: US  │  │ Region: EU  │  │ Region: APAC│          │
//! │  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘          │
//! └─────────┼────────────────┼────────────────┼─────────────────┘
//!           │                │                │
//!     ┌─────┴─────┐    ┌─────┴─────┐    ┌─────┴─────┐
//!     │Edge Mesh  │    │Edge Mesh  │    │Edge Mesh  │
//!     │  US-West  │◄──►│  EU-West  │◄──►│  APAC-East│
//!     └───────────┘    └───────────┘    └───────────┘
//! ```

use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

/// Geographic region identifier
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeoRegion {
    /// Region code (e.g., "us-west-1", "eu-central-1")
    pub code: String,
    /// Human-readable name
    pub name: String,
    /// Approximate latitude
    pub latitude: f64,
    /// Approximate longitude
    pub longitude: f64,
}

impl PartialEq for GeoRegion {
    fn eq(&self, other: &Self) -> bool {
        self.code == other.code
    }
}

impl Eq for GeoRegion {}

impl std::hash::Hash for GeoRegion {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.code.hash(state);
    }
}

impl GeoRegion {
    pub fn new(code: impl Into<String>, name: impl Into<String>, lat: f64, lon: f64) -> Self {
        Self {
            code: code.into(),
            name: name.into(),
            latitude: lat,
            longitude: lon,
        }
    }

    /// Calculate approximate distance to another region (in km)
    pub fn distance_to(&self, other: &GeoRegion) -> f64 {
        // Haversine formula for great-circle distance
        const R: f64 = 6371.0; // Earth's radius in km

        let lat1 = self.latitude.to_radians();
        let lat2 = other.latitude.to_radians();
        let dlat = (other.latitude - self.latitude).to_radians();
        let dlon = (other.longitude - self.longitude).to_radians();

        let a = (dlat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (dlon / 2.0).sin().powi(2);
        let c = 2.0 * a.sqrt().asin();

        R * c
    }

    /// Predefined regions
    pub fn us_west() -> Self {
        Self::new("us-west-1", "US West (N. California)", 37.7749, -122.4194)
    }

    pub fn us_east() -> Self {
        Self::new("us-east-1", "US East (N. Virginia)", 39.0438, -77.4874)
    }

    pub fn eu_west() -> Self {
        Self::new("eu-west-1", "EU (Ireland)", 53.3498, -6.2603)
    }

    pub fn eu_central() -> Self {
        Self::new("eu-central-1", "EU (Frankfurt)", 50.1109, 8.6821)
    }

    pub fn apac_northeast() -> Self {
        Self::new("ap-northeast-1", "Asia Pacific (Tokyo)", 35.6762, 139.6503)
    }

    pub fn apac_southeast() -> Self {
        Self::new(
            "ap-southeast-1",
            "Asia Pacific (Singapore)",
            1.3521,
            103.8198,
        )
    }
}

/// Node in the edge mesh
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeshNode {
    /// Unique node identifier
    pub id: String,
    /// Node's region
    pub region: GeoRegion,
    /// Network address
    pub address: String,
    /// Node capabilities
    pub capabilities: NodeCapabilities,
    /// Last seen timestamp (ms since epoch)
    pub last_seen: i64,
    /// Node status
    pub status: NodeStatus,
    /// Measured latency to this node (ms)
    pub latency_ms: Option<u32>,
    /// Throughput capacity (messages/sec)
    pub throughput_capacity: u64,
}

impl MeshNode {
    pub fn new(id: impl Into<String>, region: GeoRegion, address: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            region,
            address: address.into(),
            capabilities: NodeCapabilities::default(),
            last_seen: chrono::Utc::now().timestamp_millis(),
            status: NodeStatus::Unknown,
            latency_ms: None,
            throughput_capacity: 10000,
        }
    }

    pub fn is_healthy(&self) -> bool {
        matches!(self.status, NodeStatus::Healthy | NodeStatus::Degraded)
    }

    pub fn age_ms(&self) -> i64 {
        chrono::Utc::now().timestamp_millis() - self.last_seen
    }
}

/// Node capabilities
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NodeCapabilities {
    /// Can accept producer connections
    pub can_produce: bool,
    /// Can serve consumer requests
    pub can_consume: bool,
    /// Can forward to cloud
    pub can_forward: bool,
    /// Can replicate to peers
    pub can_replicate: bool,
    /// Maximum storage capacity (bytes)
    pub max_storage_bytes: u64,
    /// Supported compression types
    pub compression_types: Vec<String>,
}

impl NodeCapabilities {
    pub fn full() -> Self {
        Self {
            can_produce: true,
            can_consume: true,
            can_forward: true,
            can_replicate: true,
            max_storage_bytes: 10 * 1024 * 1024 * 1024, // 10GB
            compression_types: vec!["lz4".into(), "zstd".into(), "snappy".into()],
        }
    }

    pub fn consumer_only() -> Self {
        Self {
            can_consume: true,
            ..Default::default()
        }
    }
}

/// Node status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeStatus {
    Unknown,
    Healthy,
    Degraded,
    Unreachable,
    Draining,
    Offline,
}

impl std::fmt::Display for NodeStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeStatus::Unknown => write!(f, "unknown"),
            NodeStatus::Healthy => write!(f, "healthy"),
            NodeStatus::Degraded => write!(f, "degraded"),
            NodeStatus::Unreachable => write!(f, "unreachable"),
            NodeStatus::Draining => write!(f, "draining"),
            NodeStatus::Offline => write!(f, "offline"),
        }
    }
}

/// Mesh topology configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeshConfig {
    /// This node's ID
    pub node_id: String,
    /// This node's region
    pub region: GeoRegion,
    /// This node's address
    pub address: String,
    /// Seed nodes for discovery
    pub seed_nodes: Vec<String>,
    /// Heartbeat interval
    pub heartbeat_interval_ms: u64,
    /// Node timeout threshold
    pub node_timeout_ms: u64,
    /// Enable geographic routing
    pub geo_routing_enabled: bool,
    /// Prefer same-region nodes
    pub prefer_same_region: bool,
    /// Maximum cross-region latency before failover (ms)
    pub max_cross_region_latency_ms: u32,
    /// Replication factor for cross-region data
    pub cross_region_replication_factor: u8,
    /// Mesh topology type
    pub topology: MeshTopology,
    /// Sync policy for peer replication
    pub sync_policy: SyncPolicy,
}

impl Default for MeshConfig {
    fn default() -> Self {
        Self {
            node_id: uuid::Uuid::new_v4().to_string(),
            region: GeoRegion::us_west(),
            address: "0.0.0.0:9095".to_string(),
            seed_nodes: Vec::new(),
            heartbeat_interval_ms: 5000,
            node_timeout_ms: 30000,
            geo_routing_enabled: true,
            prefer_same_region: true,
            max_cross_region_latency_ms: 200,
            cross_region_replication_factor: 2,
            topology: MeshTopology::FullMesh,
            sync_policy: SyncPolicy::AllTopics,
        }
    }
}

/// Node map type alias
type NodeMap = HashMap<String, MeshNode>;
/// Region map type alias
type RegionMap = HashMap<String, Vec<String>>;

/// Edge mesh manager
pub struct EdgeMesh {
    config: MeshConfig,
    /// Known nodes in the mesh
    nodes: Arc<RwLock<NodeMap>>,
    /// Nodes organized by region
    nodes_by_region: Arc<RwLock<RegionMap>>,
    /// Peer connection states
    peer_connections: Arc<RwLock<HashMap<String, PeerConnection>>>,
    /// Running flag
    running: Arc<AtomicBool>,
    /// Statistics
    stats: MeshStats,
}

impl EdgeMesh {
    /// Create a new edge mesh
    pub fn new(config: MeshConfig) -> Self {
        Self {
            config,
            nodes: Arc::new(RwLock::new(HashMap::new())),
            nodes_by_region: Arc::new(RwLock::new(HashMap::new())),
            peer_connections: Arc::new(RwLock::new(HashMap::new())),
            running: Arc::new(AtomicBool::new(false)),
            stats: MeshStats::default(),
        }
    }

    /// Get this node's ID
    pub fn node_id(&self) -> &str {
        &self.config.node_id
    }

    /// Get this node's region
    pub fn region(&self) -> &GeoRegion {
        &self.config.region
    }

    /// Check if mesh is running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    /// Start the mesh
    pub async fn start(&self) -> Result<()> {
        if self.running.swap(true, Ordering::SeqCst) {
            return Ok(()); // Already running
        }

        info!(
            node_id = %self.config.node_id,
            region = %self.config.region.code,
            "Starting edge mesh"
        );

        // Register self
        let self_node = MeshNode::new(
            self.config.node_id.clone(),
            self.config.region.clone(),
            self.config.address.clone(),
        );
        self.register_node(self_node).await?;

        // Discover seed nodes
        for seed in &self.config.seed_nodes {
            debug!(seed = %seed, "Connecting to seed node");
            // In a real implementation, this would establish connections
        }

        Ok(())
    }

    /// Stop the mesh
    pub async fn stop(&self) -> Result<()> {
        if !self.running.swap(false, Ordering::SeqCst) {
            return Ok(()); // Already stopped
        }

        info!(node_id = %self.config.node_id, "Stopping edge mesh");
        Ok(())
    }

    /// Register a node in the mesh
    pub async fn register_node(&self, node: MeshNode) -> Result<()> {
        let node_id = node.id.clone();
        let region_code = node.region.code.clone();

        {
            let mut nodes: tokio::sync::RwLockWriteGuard<'_, NodeMap> = self.nodes.write().await;
            nodes.insert(node_id.clone(), node);
        }

        {
            let mut by_region: tokio::sync::RwLockWriteGuard<'_, RegionMap> =
                self.nodes_by_region.write().await;
            by_region
                .entry(region_code)
                .or_default()
                .push(node_id.clone());
        }

        self.stats.nodes_registered.fetch_add(1, Ordering::Relaxed);
        debug!(node_id = %node_id, "Registered node in mesh");
        Ok(())
    }

    /// Remove a node from the mesh
    pub async fn deregister_node(&self, node_id: &str) -> Result<()> {
        let region_code = {
            let mut nodes: tokio::sync::RwLockWriteGuard<'_, NodeMap> = self.nodes.write().await;
            nodes.remove(node_id).map(|n| n.region.code.clone())
        };

        if let Some(region) = region_code {
            let mut by_region: tokio::sync::RwLockWriteGuard<'_, RegionMap> =
                self.nodes_by_region.write().await;
            if let Some(region_nodes) = by_region.get_mut(&region) {
                region_nodes.retain(|id| id != node_id);
            }
            debug!(node_id = %node_id, "Deregistered node from mesh");
        }
        Ok(())
    }

    /// Update node status
    pub async fn update_node_status(&self, node_id: &str, status: NodeStatus) -> Result<()> {
        let mut nodes: tokio::sync::RwLockWriteGuard<'_, NodeMap> = self.nodes.write().await;
        if let Some(node) = nodes.get_mut(node_id) {
            node.status = status;
            node.last_seen = chrono::Utc::now().timestamp_millis();
        }
        Ok(())
    }

    /// Update node latency
    pub async fn update_node_latency(&self, node_id: &str, latency_ms: u32) -> Result<()> {
        let mut nodes: tokio::sync::RwLockWriteGuard<'_, NodeMap> = self.nodes.write().await;
        if let Some(node) = nodes.get_mut(node_id) {
            node.latency_ms = Some(latency_ms);
            node.last_seen = chrono::Utc::now().timestamp_millis();
        }
        Ok(())
    }

    /// Get all nodes
    pub async fn nodes(&self) -> Vec<MeshNode> {
        let nodes: tokio::sync::RwLockReadGuard<'_, NodeMap> = self.nodes.read().await;
        nodes.values().cloned().collect()
    }

    /// Get nodes in a specific region
    pub async fn nodes_in_region(&self, region_code: &str) -> Vec<MeshNode> {
        let nodes: tokio::sync::RwLockReadGuard<'_, NodeMap> = self.nodes.read().await;
        let by_region: tokio::sync::RwLockReadGuard<'_, RegionMap> =
            self.nodes_by_region.read().await;

        by_region
            .get(region_code)
            .map(|ids| ids.iter().filter_map(|id| nodes.get(id).cloned()).collect())
            .unwrap_or_default()
    }

    /// Get healthy nodes in a region
    pub async fn healthy_nodes_in_region(&self, region_code: &str) -> Vec<MeshNode> {
        self.nodes_in_region(region_code)
            .await
            .into_iter()
            .filter(|n| n.is_healthy())
            .collect()
    }

    /// Find the best node for routing based on geographic proximity and health
    pub async fn find_best_node(&self, prefer_region: Option<&str>) -> Option<MeshNode> {
        let nodes: tokio::sync::RwLockReadGuard<'_, NodeMap> = self.nodes.read().await;
        let healthy: Vec<&MeshNode> = nodes.values().filter(|n| n.is_healthy()).collect();

        if healthy.is_empty() {
            return None;
        }

        // If specific region preferred and we have nodes there
        if let Some(region) = prefer_region {
            let region_nodes: Vec<&&MeshNode> =
                healthy.iter().filter(|n| n.region.code == region).collect();
            if !region_nodes.is_empty() {
                // Return lowest latency in region
                return region_nodes
                    .into_iter()
                    .min_by_key(|n| n.latency_ms.unwrap_or(u32::MAX))
                    .map(|n| (*n).clone());
            }
        }

        // Otherwise, find closest by distance
        healthy
            .into_iter()
            .min_by(|a, b| {
                let dist_a = self.config.region.distance_to(&a.region);
                let dist_b = self.config.region.distance_to(&b.region);
                dist_a
                    .partial_cmp(&dist_b)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .cloned()
    }

    /// Find nodes for replication based on region diversity
    pub async fn find_replication_targets(&self, count: usize) -> Vec<MeshNode> {
        let nodes: tokio::sync::RwLockReadGuard<'_, NodeMap> = self.nodes.read().await;
        let healthy: Vec<MeshNode> = nodes
            .values()
            .filter(|n| n.is_healthy() && n.id != self.config.node_id)
            .cloned()
            .collect();

        if healthy.len() <= count {
            return healthy;
        }

        // Prefer nodes in different regions for redundancy
        let mut result: Vec<MeshNode> = Vec::new();
        let mut seen_regions: std::collections::HashSet<String> = std::collections::HashSet::new();

        // First pass: one node per region
        for node in &healthy {
            if !seen_regions.contains(&node.region.code) {
                result.push(node.clone());
                seen_regions.insert(node.region.code.clone());
                if result.len() >= count {
                    break;
                }
            }
        }

        // Second pass: fill remaining slots with lowest latency nodes
        if result.len() < count {
            let remaining: Vec<&MeshNode> = healthy
                .iter()
                .filter(|n| !result.iter().any(|r| r.id == n.id))
                .collect();

            for node in remaining {
                result.push(node.clone());
                if result.len() >= count {
                    break;
                }
            }
        }

        result
    }

    /// Get all known regions
    pub async fn regions(&self) -> Vec<GeoRegion> {
        let nodes: tokio::sync::RwLockReadGuard<'_, NodeMap> = self.nodes.read().await;
        let mut regions: HashMap<String, GeoRegion> = HashMap::new();
        for node in nodes.values() {
            regions.insert(node.region.code.clone(), node.region.clone());
        }
        regions.into_values().collect()
    }

    /// Get the configured mesh topology
    pub fn topology(&self) -> MeshTopology {
        self.config.topology
    }

    /// Get the configured sync policy
    pub fn sync_policy(&self) -> &SyncPolicy {
        &self.config.sync_policy
    }

    /// Join the mesh by connecting to seed peers and registering self.
    ///
    /// Establishes `PeerConnection` entries for every reachable seed node
    /// according to the configured `MeshTopology`.
    pub async fn join_mesh(&self) -> Result<()> {
        info!(
            node_id = %self.config.node_id,
            topology = %self.config.topology,
            seeds = self.config.seed_nodes.len(),
            "Joining edge mesh"
        );

        // Register ourselves first
        let self_node = MeshNode::new(
            self.config.node_id.clone(),
            self.config.region.clone(),
            self.config.address.clone(),
        );
        self.register_node(self_node).await?;

        // Create peer connections for each seed
        let mut conns = self.peer_connections.write().await;
        for seed in &self.config.seed_nodes {
            let peer_id = seed.clone();
            conns.insert(
                peer_id.clone(),
                PeerConnection {
                    peer_id: peer_id.clone(),
                    address: seed.clone(),
                    state: PeerConnectionState::Connecting,
                    last_sync: None,
                    rtt_ms: None,
                },
            );
            debug!(peer = %peer_id, "Created peer connection");
        }

        // Mark connections as connected (in production this would do real handshake)
        for conn in conns.values_mut() {
            conn.state = PeerConnectionState::Connected;
        }

        self.running.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Synchronise data with a specific peer.
    ///
    /// Respects the configured `SyncPolicy` to determine which topics
    /// are eligible for replication.
    pub async fn sync_with_peer(&self, peer_id: &str) -> Result<()> {
        // Verify peer exists
        {
            let conns = self.peer_connections.read().await;
            let conn = conns.get(peer_id).ok_or_else(|| {
                StreamlineError::storage_msg(format!("Unknown peer: {}", peer_id))
            })?;
            if conn.state != PeerConnectionState::Connected {
                return Err(StreamlineError::storage_msg(format!(
                    "Peer {} not connected (state: {:?})",
                    peer_id, conn.state
                )));
            }
        }

        debug!(peer = %peer_id, "Syncing with peer");

        // Mark syncing
        {
            let mut conns = self.peer_connections.write().await;
            if let Some(conn) = conns.get_mut(peer_id) {
                conn.state = PeerConnectionState::Syncing;
            }
        }

        // In production: exchange records filtered by SyncPolicy.
        // Stub — just update stats.
        self.stats.cross_region_syncs.fetch_add(1, Ordering::Relaxed);

        // Mark completed
        {
            let mut conns = self.peer_connections.write().await;
            if let Some(conn) = conns.get_mut(peer_id) {
                conn.state = PeerConnectionState::Connected;
                conn.last_sync = Some(chrono::Utc::now().timestamp_millis());
            }
        }

        Ok(())
    }

    /// Propagate an event (topic + payload) to all connected peers.
    ///
    /// The set of peers contacted depends on the `MeshTopology`:
    /// - `FullMesh`: all connected peers
    /// - `Star`: only the hub (first seed node)
    /// - `Ring`: left and right neighbours
    pub async fn propagate_event(&self, _topic: &str, _payload: &[u8]) -> Result<usize> {
        let conns = self.peer_connections.read().await;
        let connected: Vec<&PeerConnection> = conns
            .values()
            .filter(|c| c.state == PeerConnectionState::Connected)
            .collect();

        let targets: Vec<&PeerConnection> = match self.config.topology {
            MeshTopology::FullMesh => connected,
            MeshTopology::Star => connected.into_iter().take(1).collect(),
            MeshTopology::Ring => connected.into_iter().take(2).collect(),
        };

        let count = targets.len();
        for target in &targets {
            debug!(peer = %target.peer_id, "Propagating event to peer");
        }

        self.stats
            .messages_routed
            .fetch_add(count as u64, Ordering::Relaxed);

        Ok(count)
    }

    /// Get current peer connections
    pub async fn peer_connections(&self) -> Vec<PeerConnection> {
        self.peer_connections.read().await.values().cloned().collect()
    }

    /// Get mesh statistics
    pub fn stats(&self) -> MeshStatsSnapshot {
        MeshStatsSnapshot {
            nodes_registered: self.stats.nodes_registered.load(Ordering::Relaxed),
            heartbeats_sent: self.stats.heartbeats_sent.load(Ordering::Relaxed),
            heartbeats_received: self.stats.heartbeats_received.load(Ordering::Relaxed),
            messages_routed: self.stats.messages_routed.load(Ordering::Relaxed),
            cross_region_syncs: self.stats.cross_region_syncs.load(Ordering::Relaxed),
            failovers: self.stats.failovers.load(Ordering::Relaxed),
        }
    }
}

/// Mesh statistics (atomic counters)
#[derive(Default)]
struct MeshStats {
    nodes_registered: AtomicU64,
    heartbeats_sent: AtomicU64,
    heartbeats_received: AtomicU64,
    messages_routed: AtomicU64,
    cross_region_syncs: AtomicU64,
    failovers: AtomicU64,
}

/// Mesh statistics snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeshStatsSnapshot {
    pub nodes_registered: u64,
    pub heartbeats_sent: u64,
    pub heartbeats_received: u64,
    pub messages_routed: u64,
    pub cross_region_syncs: u64,
    pub failovers: u64,
}

/// Mesh topology type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MeshTopology {
    /// Hub-spoke: all edges sync through a central hub
    Star,
    /// Ring: each edge syncs with its two neighbours
    Ring,
    /// Every edge syncs directly with every other edge
    #[default]
    FullMesh,
}

impl std::fmt::Display for MeshTopology {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MeshTopology::Star => write!(f, "star"),
            MeshTopology::Ring => write!(f, "ring"),
            MeshTopology::FullMesh => write!(f, "full_mesh"),
        }
    }
}

/// Policy controlling which data is synced between mesh peers
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncPolicy {
    /// Replicate all topics to every peer
    AllTopics,
    /// Replicate only the listed topics
    SelectedTopics(Vec<String>),
    /// Replicate topics matching a prefix filter
    Filtered { prefix: String },
}

impl Default for SyncPolicy {
    fn default() -> Self {
        SyncPolicy::AllTopics
    }
}

/// Connection state for a single peer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerConnection {
    /// Peer node ID
    pub peer_id: String,
    /// Peer address
    pub address: String,
    /// Connection state
    pub state: PeerConnectionState,
    /// Last successful sync timestamp (ms since epoch)
    pub last_sync: Option<i64>,
    /// Measured round-trip latency (ms)
    pub rtt_ms: Option<u32>,
}

/// State of a peer connection
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PeerConnectionState {
    #[default]
    Disconnected,
    Connecting,
    Connected,
    Syncing,
    Failed,
}

/// Routing decision for geographic routing
#[derive(Debug, Clone, Serialize)]
pub struct RoutingDecision {
    /// Target node ID
    pub target_node_id: String,
    /// Target region
    pub target_region: String,
    /// Estimated latency (ms)
    pub estimated_latency_ms: Option<u32>,
    /// Is this a cross-region route
    pub is_cross_region: bool,
    /// Alternative targets (for failover)
    pub alternatives: Vec<String>,
}

/// Geographic router for message routing
pub struct GeoRouter {
    mesh: Arc<EdgeMesh>,
    config: GeoRouterConfig,
}

/// Geographic router configuration
#[derive(Debug, Clone)]
pub struct GeoRouterConfig {
    /// Prefer local region
    pub prefer_local: bool,
    /// Maximum acceptable latency (ms)
    pub max_latency_ms: u32,
    /// Enable cross-region routing
    pub cross_region_enabled: bool,
    /// Number of alternatives to include
    pub max_alternatives: usize,
}

impl Default for GeoRouterConfig {
    fn default() -> Self {
        Self {
            prefer_local: true,
            max_latency_ms: 100,
            cross_region_enabled: true,
            max_alternatives: 2,
        }
    }
}

impl GeoRouter {
    /// Create a new geographic router
    pub fn new(mesh: Arc<EdgeMesh>, config: GeoRouterConfig) -> Self {
        Self { mesh, config }
    }

    /// Route a message to the best node
    pub async fn route(&self, _topic: &str, _partition: Option<i32>) -> Result<RoutingDecision> {
        let local_region = self.mesh.region().code.clone();

        // First try local region
        if self.config.prefer_local {
            let local_nodes = self.mesh.healthy_nodes_in_region(&local_region).await;
            if !local_nodes.is_empty() {
                let best = local_nodes
                    .iter()
                    .min_by_key(|n| n.latency_ms.unwrap_or(u32::MAX))
                    .ok_or_else(|| StreamlineError::storage_msg("No local nodes available".into()))?;

                let alternatives: Vec<String> = local_nodes
                    .iter()
                    .filter(|n| n.id != best.id)
                    .take(self.config.max_alternatives)
                    .map(|n| n.id.clone())
                    .collect();

                return Ok(RoutingDecision {
                    target_node_id: best.id.clone(),
                    target_region: local_region,
                    estimated_latency_ms: best.latency_ms,
                    is_cross_region: false,
                    alternatives,
                });
            }
        }

        // Fallback to cross-region
        if self.config.cross_region_enabled {
            if let Some(best) = self.mesh.find_best_node(None).await {
                let is_cross_region = best.region.code != local_region;
                let targets = self
                    .mesh
                    .find_replication_targets(self.config.max_alternatives + 1)
                    .await;
                let alternatives: Vec<String> = targets
                    .iter()
                    .filter(|n| n.id != best.id)
                    .take(self.config.max_alternatives)
                    .map(|n| n.id.clone())
                    .collect();

                return Ok(RoutingDecision {
                    target_node_id: best.id.clone(),
                    target_region: best.region.code.clone(),
                    estimated_latency_ms: best.latency_ms,
                    is_cross_region,
                    alternatives,
                });
            }
        }

        Err(StreamlineError::storage_msg(
            "No healthy nodes available for routing".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_geo_region_distance() {
        let sf = GeoRegion::us_west();
        let tokyo = GeoRegion::apac_northeast();

        let distance = sf.distance_to(&tokyo);
        // SF to Tokyo is approximately 8,280 km
        assert!(distance > 8000.0 && distance < 9000.0);
    }

    #[test]
    fn test_mesh_node_creation() {
        let node = MeshNode::new("node-1", GeoRegion::us_west(), "127.0.0.1:9092");
        assert_eq!(node.id, "node-1");
        assert_eq!(node.region.code, "us-west-1");
        assert!(!node.is_healthy()); // Unknown status is not healthy
    }

    #[tokio::test]
    async fn test_edge_mesh_registration() {
        let config = MeshConfig::default();
        let mesh = EdgeMesh::new(config);

        let node = MeshNode {
            id: "test-node".to_string(),
            region: GeoRegion::eu_west(),
            address: "10.0.0.1:9092".to_string(),
            capabilities: NodeCapabilities::full(),
            last_seen: chrono::Utc::now().timestamp_millis(),
            status: NodeStatus::Healthy,
            latency_ms: Some(50),
            throughput_capacity: 10000,
        };

        mesh.register_node(node).await.unwrap();

        let nodes = mesh.nodes().await;
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].id, "test-node");
    }

    #[tokio::test]
    async fn test_find_replication_targets() {
        let config = MeshConfig {
            node_id: "local-node".to_string(),
            ..Default::default()
        };
        let mesh = EdgeMesh::new(config);

        // Add nodes in different regions
        for (id, region) in [
            ("node-us", GeoRegion::us_east()),
            ("node-eu", GeoRegion::eu_west()),
            ("node-apac", GeoRegion::apac_northeast()),
        ] {
            let mut node = MeshNode::new(id, region, format!("10.0.0.{}:9092", id.len()));
            node.status = NodeStatus::Healthy;
            mesh.register_node(node).await.unwrap();
        }

        let targets = mesh.find_replication_targets(2).await;
        assert_eq!(targets.len(), 2);

        // Should have nodes from different regions
        let regions: std::collections::HashSet<_> =
            targets.iter().map(|n| &n.region.code).collect();
        assert!(regions.len() >= 2);
    }

    #[test]
    fn test_mesh_config_default() {
        let config = MeshConfig::default();
        assert!(config.geo_routing_enabled);
        assert!(config.prefer_same_region);
        assert_eq!(config.cross_region_replication_factor, 2);
    }
}
