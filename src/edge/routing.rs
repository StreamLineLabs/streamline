//! Mesh Routing Table for Cross-Instance Topic Forwarding
//!
//! Provides a distributed routing table that tracks which topics are available
//! on which mesh nodes, supporting multiple routing policies for produce/consume
//! request forwarding in the federated streaming mesh.
//!
//! # Routing Policies
//!
//! - **Nearest**: Route to the node with the lowest measured latency
//! - **RoundRobin**: Distribute requests evenly across healthy nodes
//! - **PrimaryBackup**: Use the highest-priority healthy route
//! - **HashBased**: Consistent hashing on message key for sticky routing
//! - **LocalFirst**: Prefer the local node, fall back to remote nodes

use crate::error::{Result, StreamlineError};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

/// A single route entry mapping a topic (optionally a partition) to a mesh node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeshRoute {
    /// Topic name
    pub topic: String,
    /// Optional partition (None means all partitions)
    pub partition: Option<i32>,
    /// Target node identifier
    pub target_node: String,
    /// Network address of the target node
    pub target_addr: String,
    /// Priority (lower value = higher priority)
    pub priority: u32,
    /// Whether the target node is considered healthy
    pub healthy: bool,
    /// Measured latency to the target node in milliseconds
    pub latency_ms: u64,
    /// Timestamp of last update (ms since epoch)
    pub last_updated: i64,
}

/// Routing policy for selecting among available routes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RoutingPolicy {
    /// Route to the node with the lowest latency
    Nearest,
    /// Distribute requests across healthy nodes in round-robin order
    RoundRobin,
    /// Use the highest-priority (lowest value) healthy route
    PrimaryBackup,
    /// Consistent hashing on message key
    HashBased { key_field: String },
    /// Prefer routes on the local node, fall back to remote
    LocalFirst,
}

/// The result of resolving a route for a topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteResolution {
    /// Topic that was resolved
    pub topic: String,
    /// The selected route
    pub selected_route: MeshRoute,
    /// Alternative routes (for failover)
    pub alternatives: Vec<MeshRoute>,
    /// The policy that was used for selection
    pub policy_used: RoutingPolicy,
    /// Time spent resolving the route in microseconds
    pub resolution_time_us: u64,
}

/// Aggregate statistics for the routing table.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RouteTableStats {
    /// Total number of routes in the table
    pub total_routes: usize,
    /// Number of routes marked healthy
    pub healthy_routes: usize,
    /// Number of routes marked unhealthy
    pub unhealthy_routes: usize,
    /// Distinct topics with at least one route
    pub topics_count: usize,
    /// Distinct target nodes across all routes
    pub nodes_count: usize,
    /// Average latency across all healthy routes (ms)
    pub avg_latency_ms: u64,
    /// Timestamp of last prune operation (ms since epoch)
    pub last_pruned_at: Option<i64>,
}

/// Consistent hash ring for deterministic key-based routing.
pub struct ConsistentHash {
    /// Sorted virtual-node positions on the hash ring
    ring: Vec<(u64, usize)>,
    /// Original node identifiers
    nodes: Vec<String>,
}

impl ConsistentHash {
    /// Create a new consistent hash ring.
    pub fn new(nodes: Vec<String>, virtual_nodes: usize) -> Self {
        let mut ring = Vec::with_capacity(nodes.len() * virtual_nodes);
        for (idx, node) in nodes.iter().enumerate() {
            for vn in 0..virtual_nodes {
                let key = format!("{}:{}", node, vn);
                let hash = Self::hash_bytes(key.as_bytes());
                ring.push((hash, idx));
            }
        }
        ring.sort_by_key(|&(h, _)| h);
        Self { ring, nodes }
    }

    /// Look up the node responsible for the given key.
    pub fn get_node<'a>(&'a self, key: &[u8]) -> &'a str {
        if self.ring.is_empty() {
            return "";
        }
        let hash = Self::hash_bytes(key);
        let pos = match self.ring.binary_search_by_key(&hash, |&(h, _)| h) {
            Ok(i) => i,
            Err(i) => {
                if i >= self.ring.len() {
                    0
                } else {
                    i
                }
            }
        };
        &self.nodes[self.ring[pos].1]
    }

    fn hash_bytes(data: &[u8]) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        data.hash(&mut hasher);
        hasher.finish()
    }
}

/// Distributed routing table for cross-instance topic forwarding.
///
/// Maintains a map of topic → routes, supports multiple routing policies,
/// and handles node health transitions and stale-route pruning.
pub struct MeshRouteTable {
    /// Identifier of the local node
    local_node_id: String,
    /// Default policy used when no override is supplied
    default_policy: RwLock<RoutingPolicy>,
    /// topic → list of routes
    routes: RwLock<HashMap<String, Vec<MeshRoute>>>,
    /// Per-topic round-robin counters
    rr_counters: RwLock<HashMap<String, usize>>,
    /// Timestamp of last prune (ms since epoch)
    last_pruned_at: RwLock<Option<i64>>,
}

impl MeshRouteTable {
    /// Create a new routing table for the given local node.
    pub fn new(local_node_id: String, default_policy: RoutingPolicy) -> Self {
        Self {
            local_node_id,
            default_policy: RwLock::new(default_policy),
            routes: RwLock::new(HashMap::new()),
            rr_counters: RwLock::new(HashMap::new()),
            last_pruned_at: RwLock::new(None),
        }
    }

    /// Insert a route into the table.
    pub fn add_route(&self, route: MeshRoute) -> Result<()> {
        let mut routes = self.routes.write();
        let entries = routes.entry(route.topic.clone()).or_default();

        // Replace existing route for same (topic, partition, node)
        if let Some(existing) = entries
            .iter_mut()
            .find(|r| r.target_node == route.target_node && r.partition == route.partition)
        {
            *existing = route;
        } else {
            entries.push(route);
        }
        Ok(())
    }

    /// Remove all routes for a topic/node pair.
    pub fn remove_route(&self, topic: &str, node_id: &str) -> Result<()> {
        let mut routes = self.routes.write();
        if let Some(entries) = routes.get_mut(topic) {
            entries.retain(|r| r.target_node != node_id);
            if entries.is_empty() {
                routes.remove(topic);
            }
        }
        Ok(())
    }

    /// Resolve the best route for a topic using the given (or default) policy.
    pub fn resolve(
        &self,
        topic: &str,
        key: Option<&[u8]>,
        policy: Option<&RoutingPolicy>,
    ) -> Result<RouteResolution> {
        let start = std::time::Instant::now();
        let routes = self.routes.read();
        let entries = routes.get(topic).ok_or_else(|| {
            StreamlineError::storage_msg(format!("no routes for topic '{}'", topic))
        })?;

        let healthy: Vec<MeshRoute> = entries.iter().filter(|r| r.healthy).cloned().collect();
        if healthy.is_empty() {
            return Err(StreamlineError::storage_msg(format!(
                "no healthy routes for topic '{}'",
                topic
            )));
        }

        let policy_guard = self.default_policy.read();
        let effective_policy = policy.unwrap_or(&*policy_guard);

        let selected = match effective_policy {
            RoutingPolicy::Nearest => healthy
                .iter()
                .min_by_key(|r| r.latency_ms)
                .cloned()
                .ok_or_else(|| StreamlineError::storage_msg(format!("no healthy routes for topic '{}'", topic)))?,
            RoutingPolicy::RoundRobin => {
                // Drop the routes read-lock is not needed for counter; we already cloned healthy.
                let mut counters = self.rr_counters.write();
                let counter = counters.entry(topic.to_string()).or_insert(0);
                let idx = *counter % healthy.len();
                *counter = counter.wrapping_add(1);
                healthy[idx].clone()
            }
            RoutingPolicy::PrimaryBackup => {
                healthy.iter().min_by_key(|r| r.priority).cloned()
                    .ok_or_else(|| StreamlineError::storage_msg(format!("no healthy routes for topic '{}'", topic)))?
            }
            RoutingPolicy::HashBased { .. } => {
                let node_ids: Vec<String> = healthy.iter().map(|r| r.target_node.clone()).collect();
                let ring = ConsistentHash::new(node_ids, 100);
                let effective_key = key.unwrap_or(b"");
                let target = ring.get_node(effective_key);
                healthy
                    .iter()
                    .find(|r| r.target_node == target)
                    .cloned()
                    .unwrap_or_else(|| healthy[0].clone())
            }
            RoutingPolicy::LocalFirst => {
                if let Some(local) = healthy.iter().find(|r| r.target_node == self.local_node_id) {
                    local.clone()
                } else {
                    healthy
                        .iter()
                        .min_by_key(|r| r.latency_ms)
                        .cloned()
                        .ok_or_else(|| StreamlineError::storage_msg(format!("no healthy routes for topic '{}'", topic)))?
                }
            }
        };

        let alternatives: Vec<MeshRoute> = healthy
            .into_iter()
            .filter(|r| r.target_node != selected.target_node || r.partition != selected.partition)
            .collect();

        let elapsed_us = start.elapsed().as_micros() as u64;

        Ok(RouteResolution {
            topic: topic.to_string(),
            selected_route: selected,
            alternatives,
            policy_used: effective_policy.clone(),
            resolution_time_us: elapsed_us,
        })
    }

    /// Mark all routes for a node as unhealthy.
    pub fn mark_unhealthy(&self, node_id: &str) {
        let mut routes = self.routes.write();
        for entries in routes.values_mut() {
            for route in entries.iter_mut() {
                if route.target_node == node_id {
                    route.healthy = false;
                }
            }
        }
    }

    /// Mark all routes for a node as healthy.
    pub fn mark_healthy(&self, node_id: &str) {
        let mut routes = self.routes.write();
        for entries in routes.values_mut() {
            for route in entries.iter_mut() {
                if route.target_node == node_id {
                    route.healthy = true;
                }
            }
        }
    }

    /// Update the measured latency for all routes targeting a node.
    pub fn update_latency(&self, node_id: &str, latency_ms: u64) {
        let mut routes = self.routes.write();
        let now = chrono::Utc::now().timestamp_millis();
        for entries in routes.values_mut() {
            for route in entries.iter_mut() {
                if route.target_node == node_id {
                    route.latency_ms = latency_ms;
                    route.last_updated = now;
                }
            }
        }
    }

    /// Return all routes for a given topic.
    pub fn get_routes_for_topic(&self, topic: &str) -> Vec<MeshRoute> {
        self.routes.read().get(topic).cloned().unwrap_or_default()
    }

    /// Return every route in the table.
    pub fn get_all_routes(&self) -> Vec<MeshRoute> {
        self.routes
            .read()
            .values()
            .flat_map(|v| v.iter().cloned())
            .collect()
    }

    /// Remove routes whose `last_updated` is older than `max_age_ms` from now.
    /// Returns the number of pruned routes.
    pub fn prune_stale(&self, max_age_ms: i64) -> usize {
        let now = chrono::Utc::now().timestamp_millis();
        let cutoff = now - max_age_ms;
        let mut routes = self.routes.write();
        let mut pruned = 0usize;

        routes.retain(|_topic, entries| {
            let before = entries.len();
            entries.retain(|r| r.last_updated >= cutoff);
            pruned += before - entries.len();
            !entries.is_empty()
        });

        *self.last_pruned_at.write() = Some(now);
        pruned
    }

    /// Compute aggregate statistics for the routing table.
    pub fn stats(&self) -> RouteTableStats {
        let routes = self.routes.read();
        let mut total = 0usize;
        let mut healthy = 0usize;
        let mut latency_sum = 0u64;
        let mut latency_count = 0u64;
        let mut node_set = std::collections::HashSet::new();

        for entries in routes.values() {
            for route in entries {
                total += 1;
                if route.healthy {
                    healthy += 1;
                    latency_sum += route.latency_ms;
                    latency_count += 1;
                }
                node_set.insert(route.target_node.clone());
            }
        }

        RouteTableStats {
            total_routes: total,
            healthy_routes: healthy,
            unhealthy_routes: total - healthy,
            topics_count: routes.len(),
            nodes_count: node_set.len(),
            avg_latency_ms: if latency_count > 0 {
                latency_sum / latency_count
            } else {
                0
            },
            last_pruned_at: *self.last_pruned_at.read(),
        }
    }

    /// Merge route announcements received from peers into this table.
    pub fn merge_from(&self, remote_table: &[(String, Vec<MeshRoute>)]) {
        let mut routes = self.routes.write();
        for (topic, incoming) in remote_table {
            let entries = routes.entry(topic.clone()).or_default();
            for new_route in incoming {
                if let Some(existing) = entries.iter_mut().find(|r| {
                    r.target_node == new_route.target_node && r.partition == new_route.partition
                }) {
                    if new_route.last_updated > existing.last_updated {
                        *existing = new_route.clone();
                    }
                } else {
                    entries.push(new_route.clone());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now_ms() -> i64 {
        chrono::Utc::now().timestamp_millis()
    }

    fn make_route(topic: &str, node: &str, priority: u32, latency: u64) -> MeshRoute {
        MeshRoute {
            topic: topic.to_string(),
            partition: None,
            target_node: node.to_string(),
            target_addr: format!("{}:9092", node),
            priority,
            healthy: true,
            latency_ms: latency,
            last_updated: now_ms(),
        }
    }

    #[test]
    fn test_add_and_resolve_nearest() {
        let table = MeshRouteTable::new("local".into(), RoutingPolicy::Nearest);
        table
            .add_route(make_route("events", "node-a", 1, 50))
            .unwrap();
        table
            .add_route(make_route("events", "node-b", 2, 10))
            .unwrap();

        let res = table.resolve("events", None, None).unwrap();
        assert_eq!(res.selected_route.target_node, "node-b");
        assert_eq!(res.alternatives.len(), 1);
    }

    #[test]
    fn test_primary_backup_policy() {
        let table = MeshRouteTable::new("local".into(), RoutingPolicy::PrimaryBackup);
        table
            .add_route(make_route("logs", "node-a", 5, 100))
            .unwrap();
        table
            .add_route(make_route("logs", "node-b", 1, 200))
            .unwrap();

        let res = table.resolve("logs", None, None).unwrap();
        // node-b has lower priority value → higher priority
        assert_eq!(res.selected_route.target_node, "node-b");
    }

    #[test]
    fn test_local_first_policy() {
        let table = MeshRouteTable::new("local-node".into(), RoutingPolicy::LocalFirst);
        table
            .add_route(make_route("metrics", "remote-1", 1, 5))
            .unwrap();
        table
            .add_route(make_route("metrics", "local-node", 2, 20))
            .unwrap();

        let res = table.resolve("metrics", None, None).unwrap();
        assert_eq!(res.selected_route.target_node, "local-node");
    }

    #[test]
    fn test_mark_unhealthy_and_prune() {
        let table = MeshRouteTable::new("local".into(), RoutingPolicy::Nearest);
        table
            .add_route(make_route("events", "node-a", 1, 10))
            .unwrap();
        table
            .add_route(make_route("events", "node-b", 2, 20))
            .unwrap();

        table.mark_unhealthy("node-a");
        let res = table.resolve("events", None, None).unwrap();
        assert_eq!(res.selected_route.target_node, "node-b");

        // Prune with 0 max-age removes all (they were just created so last_updated == now)
        // but since last_updated >= cutoff, nothing should be pruned when max_age is large
        let pruned = table.prune_stale(60_000);
        assert_eq!(pruned, 0);

        let stats = table.stats();
        assert_eq!(stats.total_routes, 2);
        assert_eq!(stats.unhealthy_routes, 1);
        assert!(stats.last_pruned_at.is_some());
    }

    #[test]
    fn test_round_robin_distribution() {
        let table = MeshRouteTable::new("local".into(), RoutingPolicy::RoundRobin);
        table
            .add_route(make_route("rr-topic", "node-a", 1, 10))
            .unwrap();
        table
            .add_route(make_route("rr-topic", "node-b", 1, 10))
            .unwrap();

        let r1 = table.resolve("rr-topic", None, None).unwrap();
        let r2 = table.resolve("rr-topic", None, None).unwrap();

        // Two consecutive resolves should pick different nodes
        assert_ne!(r1.selected_route.target_node, r2.selected_route.target_node,);
    }

    #[test]
    fn test_merge_from_peers() {
        let table = MeshRouteTable::new("local".into(), RoutingPolicy::Nearest);
        table
            .add_route(make_route("events", "node-a", 1, 50))
            .unwrap();

        let remote = vec![(
            "events".to_string(),
            vec![make_route("events", "node-c", 3, 30)],
        )];
        table.merge_from(&remote);

        let routes = table.get_routes_for_topic("events");
        assert_eq!(routes.len(), 2);
    }

    #[test]
    fn test_consistent_hash_stable() {
        let nodes = vec!["a".into(), "b".into(), "c".into()];
        let ring = ConsistentHash::new(nodes, 100);

        let n1 = ring.get_node(b"key-1");
        let n2 = ring.get_node(b"key-1");
        assert_eq!(n1, n2, "consistent hash must be deterministic");
    }

    #[test]
    fn test_remove_route() {
        let table = MeshRouteTable::new("local".into(), RoutingPolicy::Nearest);
        table.add_route(make_route("t", "node-a", 1, 10)).unwrap();
        table.add_route(make_route("t", "node-b", 2, 20)).unwrap();

        table.remove_route("t", "node-a").unwrap();
        let routes = table.get_routes_for_topic("t");
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].target_node, "node-b");
    }

    #[test]
    fn test_stats() {
        let table = MeshRouteTable::new("local".into(), RoutingPolicy::Nearest);
        table.add_route(make_route("a", "n1", 1, 10)).unwrap();
        table.add_route(make_route("a", "n2", 2, 30)).unwrap();
        table.add_route(make_route("b", "n1", 1, 20)).unwrap();

        let stats = table.stats();
        assert_eq!(stats.total_routes, 3);
        assert_eq!(stats.healthy_routes, 3);
        assert_eq!(stats.topics_count, 2);
        assert_eq!(stats.nodes_count, 2);
        assert_eq!(stats.avg_latency_ms, 20); // (10+30+20)/3
    }
}
