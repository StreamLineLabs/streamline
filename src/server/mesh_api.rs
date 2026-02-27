//! P2P Streaming Mesh API
//!
//! Manages peer-to-peer mesh networking for distributed streaming topology.
//!
//! ## Endpoints
//!
//! - `GET /api/v1/mesh/topology` - Get full mesh topology
//! - `GET /api/v1/mesh/peers` - List all peers
//! - `POST /api/v1/mesh/peers` - Add a peer
//! - `DELETE /api/v1/mesh/peers/:id` - Remove a peer
//! - `GET /api/v1/mesh/peers/:id` - Get a specific peer
//! - `GET /api/v1/mesh/stats` - Get mesh statistics

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, warn};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Discovery mechanism for finding mesh peers.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DiscoveryMethod {
    Mdns,
    Static { seeds: Vec<String> },
    Dns { domain: String },
    Gossip,
}

/// Consistency level for replicated reads/writes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ConsistencyLevel {
    Eventual,
    Session,
    Quorum,
    Strong,
}

/// Configuration for the mesh manager.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeshConfig {
    pub node_id: String,
    pub listen_addr: String,
    pub discovery: DiscoveryMethod,
    pub max_peers: usize,
    pub heartbeat_interval_ms: u64,
    pub gossip_interval_ms: u64,
    pub replication_factor: u32,
    pub consistency: ConsistencyLevel,
}

/// Current status of a peer in the mesh.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum PeerStatus {
    Active,
    Suspected,
    Down,
    Leaving,
}

/// Role a peer plays in the mesh.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum PeerRole {
    Full,
    Relay,
    Observer,
}

/// A single peer in the mesh.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeshPeer {
    pub id: String,
    pub addr: String,
    pub status: PeerStatus,
    pub role: PeerRole,
    pub topics: Vec<String>,
    pub last_heartbeat: u64,
    pub rtt_ms: f64,
    pub messages_synced: u64,
    pub bytes_synced: u64,
    pub joined_at: String,
}

/// A directed edge between two peers in the mesh topology.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeshEdge {
    pub from: String,
    pub to: String,
    pub rtt_ms: f64,
    pub bandwidth_mbps: f64,
}

/// A partition assignment across the mesh.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionAssignment {
    pub topic: String,
    pub partition: i32,
    pub leader: String,
    pub replicas: Vec<String>,
}

/// Full mesh topology snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeshTopology {
    pub local_node: String,
    pub peers: Vec<MeshPeer>,
    pub edges: Vec<MeshEdge>,
    pub partitions: Vec<PartitionAssignment>,
}

/// Atomic counters for mesh-level statistics.
pub struct MeshStats {
    pub peers_joined: AtomicU64,
    pub peers_removed: AtomicU64,
    pub heartbeats_received: AtomicU64,
    pub failures_detected: AtomicU64,
    pub messages_routed: AtomicU64,
    pub bytes_routed: AtomicU64,
}

/// Serialisable snapshot of [`MeshStats`].
#[derive(Debug, Serialize)]
pub struct MeshStatsSnapshot {
    pub peers_joined: u64,
    pub peers_removed: u64,
    pub heartbeats_received: u64,
    pub failures_detected: u64,
    pub messages_routed: u64,
    pub bytes_routed: u64,
}

/// Request body for adding a new peer.
#[derive(Debug, Deserialize)]
pub struct AddPeerRequest {
    pub id: String,
    pub addr: String,
    #[serde(default = "default_role")]
    pub role: PeerRole,
    #[serde(default)]
    pub topics: Vec<String>,
}

fn default_role() -> PeerRole {
    PeerRole::Full
}

// ---------------------------------------------------------------------------
// MeshManager
// ---------------------------------------------------------------------------

/// Manages the P2P streaming mesh: peers, topology, failure detection.
#[derive(Clone)]
pub struct MeshManager {
    peers: Arc<RwLock<HashMap<String, MeshPeer>>>,
    config: MeshConfig,
    stats: Arc<MeshStats>,
}

impl MeshStats {
    fn new() -> Self {
        Self {
            peers_joined: AtomicU64::new(0),
            peers_removed: AtomicU64::new(0),
            heartbeats_received: AtomicU64::new(0),
            failures_detected: AtomicU64::new(0),
            messages_routed: AtomicU64::new(0),
            bytes_routed: AtomicU64::new(0),
        }
    }

    fn snapshot(&self) -> MeshStatsSnapshot {
        MeshStatsSnapshot {
            peers_joined: self.peers_joined.load(Ordering::Relaxed),
            peers_removed: self.peers_removed.load(Ordering::Relaxed),
            heartbeats_received: self.heartbeats_received.load(Ordering::Relaxed),
            failures_detected: self.failures_detected.load(Ordering::Relaxed),
            messages_routed: self.messages_routed.load(Ordering::Relaxed),
            bytes_routed: self.bytes_routed.load(Ordering::Relaxed),
        }
    }
}

impl MeshManager {
    /// Create a new `MeshManager` with the given configuration.
    pub fn new(config: MeshConfig) -> Self {
        Self {
            peers: Arc::new(RwLock::new(HashMap::new())),
            config,
            stats: Arc::new(MeshStats::new()),
        }
    }

    /// Add a peer to the mesh. Returns `false` if `max_peers` would be exceeded.
    pub async fn add_peer(&self, peer: MeshPeer) -> bool {
        let mut peers = self.peers.write().await;
        if peers.len() >= self.config.max_peers && !peers.contains_key(&peer.id) {
            warn!(peer_id = %peer.id, "max_peers limit reached");
            return false;
        }
        debug!(peer_id = %peer.id, addr = %peer.addr, "peer added");
        peers.insert(peer.id.clone(), peer);
        self.stats.peers_joined.fetch_add(1, Ordering::Relaxed);
        true
    }

    /// Remove a peer by id. Returns the removed peer if it existed.
    pub async fn remove_peer(&self, id: &str) -> Option<MeshPeer> {
        let removed = self.peers.write().await.remove(id);
        if removed.is_some() {
            debug!(peer_id = %id, "peer removed");
            self.stats.peers_removed.fetch_add(1, Ordering::Relaxed);
        }
        removed
    }

    /// Get a clone of a single peer.
    pub async fn get_peer(&self, id: &str) -> Option<MeshPeer> {
        self.peers.read().await.get(id).cloned()
    }

    /// List all peers.
    pub async fn list_peers(&self) -> Vec<MeshPeer> {
        self.peers.read().await.values().cloned().collect()
    }

    /// Build a full topology snapshot.
    pub async fn get_topology(&self) -> MeshTopology {
        let peers = self.list_peers().await;
        let edges = self.build_edges(&peers);
        let partitions = self.get_partition_assignment(&peers);
        MeshTopology {
            local_node: self.config.node_id.clone(),
            peers,
            edges,
            partitions,
        }
    }

    /// Record a heartbeat for a given peer, updating its timestamp.
    pub async fn record_heartbeat(&self, peer_id: &str, timestamp: u64) -> bool {
        let mut peers = self.peers.write().await;
        if let Some(peer) = peers.get_mut(peer_id) {
            peer.last_heartbeat = timestamp;
            if peer.status == PeerStatus::Suspected {
                peer.status = PeerStatus::Active;
            }
            self.stats
                .heartbeats_received
                .fetch_add(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    /// Mark peers as `Suspected` or `Down` when heartbeats are overdue.
    pub async fn detect_failures(&self, now: u64) -> Vec<String> {
        let timeout = self.config.heartbeat_interval_ms * 3;
        let down_threshold = self.config.heartbeat_interval_ms * 6;
        let mut failed = Vec::new();
        let mut peers = self.peers.write().await;
        for peer in peers.values_mut() {
            if peer.status == PeerStatus::Leaving {
                continue;
            }
            let elapsed = now.saturating_sub(peer.last_heartbeat);
            if elapsed > down_threshold {
                if peer.status != PeerStatus::Down {
                    peer.status = PeerStatus::Down;
                    failed.push(peer.id.clone());
                    self.stats
                        .failures_detected
                        .fetch_add(1, Ordering::Relaxed);
                }
            } else if elapsed > timeout && peer.status == PeerStatus::Active {
                peer.status = PeerStatus::Suspected;
            }
        }
        failed
    }

    /// Derive partition assignments using simple round-robin across active peers.
    pub fn get_partition_assignment(&self, peers: &[MeshPeer]) -> Vec<PartitionAssignment> {
        let active: Vec<&MeshPeer> = peers
            .iter()
            .filter(|p| p.status == PeerStatus::Active)
            .collect();
        if active.is_empty() {
            return Vec::new();
        }

        let mut assignments = Vec::new();
        let mut all_topics: Vec<&str> = active
            .iter()
            .flat_map(|p| p.topics.iter().map(|t| t.as_str()))
            .collect();
        all_topics.sort_unstable();
        all_topics.dedup();

        let rf = self.config.replication_factor as usize;
        for topic in all_topics {
            let leader_idx = fxhash_str(topic) % active.len();
            let mut replicas: Vec<String> = Vec::with_capacity(rf);
            for i in 0..rf.min(active.len()) {
                replicas.push(active[(leader_idx + i) % active.len()].id.clone());
            }
            assignments.push(PartitionAssignment {
                topic: topic.to_string(),
                partition: 0,
                leader: active[leader_idx].id.clone(),
                replicas,
            });
        }
        assignments
    }

    /// Return a snapshot of mesh statistics.
    pub fn stats(&self) -> MeshStatsSnapshot {
        self.stats.snapshot()
    }

    // -- private helpers ----------------------------------------------------

    fn build_edges(&self, peers: &[MeshPeer]) -> Vec<MeshEdge> {
        let mut edges = Vec::new();
        for (i, a) in peers.iter().enumerate() {
            for b in peers.iter().skip(i + 1) {
                edges.push(MeshEdge {
                    from: a.id.clone(),
                    to: b.id.clone(),
                    rtt_ms: (a.rtt_ms + b.rtt_ms) / 2.0,
                    bandwidth_mbps: 1000.0,
                });
            }
        }
        edges
    }
}

/// Simple string hash for deterministic partition placement.
fn fxhash_str(s: &str) -> usize {
    let mut h: usize = 0;
    for b in s.bytes() {
        h = h.wrapping_mul(31).wrapping_add(b as usize);
    }
    h
}

// ---------------------------------------------------------------------------
// Axum routes
// ---------------------------------------------------------------------------

/// Build the mesh API router.
pub fn mesh_api_routes(manager: MeshManager) -> Router {
    Router::new()
        .route("/api/v1/mesh/topology", get(get_topology))
        .route("/api/v1/mesh/peers", get(list_peers).post(add_peer))
        .route(
            "/api/v1/mesh/peers/{id}",
            get(get_peer).delete(remove_peer),
        )
        .route("/api/v1/mesh/stats", get(get_stats))
        .with_state(manager)
}

async fn get_topology(State(mgr): State<MeshManager>) -> Json<MeshTopology> {
    Json(mgr.get_topology().await)
}

async fn list_peers(State(mgr): State<MeshManager>) -> Json<Vec<MeshPeer>> {
    Json(mgr.list_peers().await)
}

async fn add_peer(
    State(mgr): State<MeshManager>,
    Json(req): Json<AddPeerRequest>,
) -> Result<(StatusCode, Json<MeshPeer>), StatusCode> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    let peer = MeshPeer {
        id: req.id,
        addr: req.addr,
        status: PeerStatus::Active,
        role: req.role,
        topics: req.topics,
        last_heartbeat: now,
        rtt_ms: 0.0,
        messages_synced: 0,
        bytes_synced: 0,
        joined_at: chrono::Utc::now().to_rfc3339(),
    };

    if mgr.add_peer(peer.clone()).await {
        Ok((StatusCode::CREATED, Json(peer)))
    } else {
        Err(StatusCode::TOO_MANY_REQUESTS)
    }
}

async fn remove_peer(
    State(mgr): State<MeshManager>,
    Path(id): Path<String>,
) -> Result<Json<MeshPeer>, StatusCode> {
    mgr.remove_peer(&id)
        .await
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn get_peer(
    State(mgr): State<MeshManager>,
    Path(id): Path<String>,
) -> Result<Json<MeshPeer>, StatusCode> {
    mgr.get_peer(&id)
        .await
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn get_stats(State(mgr): State<MeshManager>) -> Json<MeshStatsSnapshot> {
    Json(mgr.stats())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    fn test_config() -> MeshConfig {
        MeshConfig {
            node_id: "node-0".into(),
            listen_addr: "127.0.0.1:9300".into(),
            discovery: DiscoveryMethod::Mdns,
            max_peers: 8,
            heartbeat_interval_ms: 1000,
            gossip_interval_ms: 500,
            replication_factor: 2,
            consistency: ConsistencyLevel::Eventual,
        }
    }

    fn make_peer(id: &str) -> MeshPeer {
        MeshPeer {
            id: id.into(),
            addr: format!("127.0.0.1:{}", 9300 + fxhash_str(id) % 100),
            status: PeerStatus::Active,
            role: PeerRole::Full,
            topics: vec!["events".into()],
            last_heartbeat: 1000,
            rtt_ms: 1.5,
            messages_synced: 0,
            bytes_synced: 0,
            joined_at: "2025-01-01T00:00:00Z".into(),
        }
    }

    #[tokio::test]
    async fn test_add_and_get_peer() {
        let mgr = MeshManager::new(test_config());
        let peer = make_peer("peer-1");
        assert!(mgr.add_peer(peer.clone()).await);
        let got = mgr.get_peer("peer-1").await.unwrap();
        assert_eq!(got.id, "peer-1");
        assert_eq!(got.addr, peer.addr);
    }

    #[tokio::test]
    async fn test_add_peer_respects_max() {
        let mut cfg = test_config();
        cfg.max_peers = 2;
        let mgr = MeshManager::new(cfg);
        assert!(mgr.add_peer(make_peer("a")).await);
        assert!(mgr.add_peer(make_peer("b")).await);
        assert!(!mgr.add_peer(make_peer("c")).await);
    }

    #[tokio::test]
    async fn test_add_peer_update_existing_ignores_limit() {
        let mut cfg = test_config();
        cfg.max_peers = 1;
        let mgr = MeshManager::new(cfg);
        assert!(mgr.add_peer(make_peer("a")).await);
        // Re-adding same id should succeed even at limit
        assert!(mgr.add_peer(make_peer("a")).await);
    }

    #[tokio::test]
    async fn test_remove_peer() {
        let mgr = MeshManager::new(test_config());
        mgr.add_peer(make_peer("peer-1")).await;
        let removed = mgr.remove_peer("peer-1").await;
        assert!(removed.is_some());
        assert!(mgr.get_peer("peer-1").await.is_none());
    }

    #[tokio::test]
    async fn test_remove_nonexistent_peer() {
        let mgr = MeshManager::new(test_config());
        assert!(mgr.remove_peer("ghost").await.is_none());
    }

    #[tokio::test]
    async fn test_list_peers() {
        let mgr = MeshManager::new(test_config());
        mgr.add_peer(make_peer("a")).await;
        mgr.add_peer(make_peer("b")).await;
        let peers = mgr.list_peers().await;
        assert_eq!(peers.len(), 2);
    }

    #[tokio::test]
    async fn test_record_heartbeat() {
        let mgr = MeshManager::new(test_config());
        mgr.add_peer(make_peer("peer-1")).await;
        assert!(mgr.record_heartbeat("peer-1", 5000).await);
        let peer = mgr.get_peer("peer-1").await.unwrap();
        assert_eq!(peer.last_heartbeat, 5000);
    }

    #[tokio::test]
    async fn test_record_heartbeat_unknown_peer() {
        let mgr = MeshManager::new(test_config());
        assert!(!mgr.record_heartbeat("ghost", 5000).await);
    }

    #[tokio::test]
    async fn test_heartbeat_recovers_suspected() {
        let mgr = MeshManager::new(test_config());
        let mut peer = make_peer("peer-1");
        peer.status = PeerStatus::Suspected;
        mgr.add_peer(peer).await;
        mgr.record_heartbeat("peer-1", 9999).await;
        let p = mgr.get_peer("peer-1").await.unwrap();
        assert_eq!(p.status, PeerStatus::Active);
    }

    #[tokio::test]
    async fn test_detect_failures_suspected() {
        let mgr = MeshManager::new(test_config());
        let mut peer = make_peer("peer-1");
        peer.last_heartbeat = 0;
        mgr.add_peer(peer).await;
        // heartbeat_interval_ms * 3 = 3000; now = 4000 → suspected
        mgr.detect_failures(4000).await;
        let p = mgr.get_peer("peer-1").await.unwrap();
        assert_eq!(p.status, PeerStatus::Suspected);
    }

    #[tokio::test]
    async fn test_detect_failures_down() {
        let mgr = MeshManager::new(test_config());
        let mut peer = make_peer("peer-1");
        peer.last_heartbeat = 0;
        mgr.add_peer(peer).await;
        // heartbeat_interval_ms * 6 = 6000; now = 7000 → down
        let failed = mgr.detect_failures(7000).await;
        assert_eq!(failed, vec!["peer-1"]);
        let p = mgr.get_peer("peer-1").await.unwrap();
        assert_eq!(p.status, PeerStatus::Down);
    }

    #[tokio::test]
    async fn test_detect_failures_skips_leaving() {
        let mgr = MeshManager::new(test_config());
        let mut peer = make_peer("peer-1");
        peer.last_heartbeat = 0;
        peer.status = PeerStatus::Leaving;
        mgr.add_peer(peer).await;
        let failed = mgr.detect_failures(999_999).await;
        assert!(failed.is_empty());
    }

    #[tokio::test]
    async fn test_topology_contains_edges() {
        let mgr = MeshManager::new(test_config());
        mgr.add_peer(make_peer("a")).await;
        mgr.add_peer(make_peer("b")).await;
        let topo = mgr.get_topology().await;
        assert_eq!(topo.local_node, "node-0");
        assert_eq!(topo.edges.len(), 1);
        assert_eq!(topo.peers.len(), 2);
    }

    #[tokio::test]
    async fn test_partition_assignment_round_robin() {
        let mgr = MeshManager::new(test_config());
        let mut p1 = make_peer("a");
        p1.topics = vec!["t1".into(), "t2".into()];
        let mut p2 = make_peer("b");
        p2.topics = vec!["t1".into()];
        let peers = vec![p1, p2];
        let assignments = mgr.get_partition_assignment(&peers);
        assert!(!assignments.is_empty());
        for a in &assignments {
            assert!(a.replicas.len() <= 2);
            assert!(!a.leader.is_empty());
        }
    }

    #[tokio::test]
    async fn test_partition_assignment_empty_when_no_active() {
        let mgr = MeshManager::new(test_config());
        let mut p = make_peer("a");
        p.status = PeerStatus::Down;
        let assignments = mgr.get_partition_assignment(&[p]);
        assert!(assignments.is_empty());
    }

    #[tokio::test]
    async fn test_stats_counters() {
        let mgr = MeshManager::new(test_config());
        mgr.add_peer(make_peer("a")).await;
        mgr.add_peer(make_peer("b")).await;
        mgr.remove_peer("b").await;
        let s = mgr.stats();
        assert_eq!(s.peers_joined, 2);
        assert_eq!(s.peers_removed, 1);
    }

    // -- HTTP handler tests -------------------------------------------------

    #[tokio::test]
    async fn test_http_list_peers_empty() {
        let app = mesh_api_routes(MeshManager::new(test_config()));
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/mesh/peers")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_http_get_peer_not_found() {
        let app = mesh_api_routes(MeshManager::new(test_config()));
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/mesh/peers/nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_http_get_stats() {
        let app = mesh_api_routes(MeshManager::new(test_config()));
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/mesh/stats")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_http_get_topology() {
        let app = mesh_api_routes(MeshManager::new(test_config()));
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/mesh/topology")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_discovery_serde_roundtrip() {
        let methods = vec![
            DiscoveryMethod::Mdns,
            DiscoveryMethod::Static {
                seeds: vec!["10.0.0.1:9300".into()],
            },
            DiscoveryMethod::Dns {
                domain: "mesh.local".into(),
            },
            DiscoveryMethod::Gossip,
        ];
        for m in methods {
            let json = serde_json::to_string(&m).unwrap();
            let back: DiscoveryMethod = serde_json::from_str(&json).unwrap();
            assert_eq!(m, back);
        }
    }
}
