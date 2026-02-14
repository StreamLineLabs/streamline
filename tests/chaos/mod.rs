//! Chaos testing framework for Streamline clustering
//!
//! This module provides utilities for testing cluster resilience under
//! various failure conditions including:
//! - Network partitions
//! - Node failures
//! - Leader failures
//! - Recovery scenarios
//! - Storage faults (write failures, corruption)
//! - Network faults (latency, packet loss)
//!
//! This module requires the `clustering` feature to be enabled.
#![cfg(feature = "clustering")]

pub mod fault_injection;
pub use fault_injection::*;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};
use std::sync::Arc;
use std::time::Duration;

use streamline::{ClusterConfig, ClusterManager, InterBrokerTlsConfig, TopicManager};
use tempfile::TempDir;
use tokio::sync::RwLock;
use tokio::time::sleep;
use tracing::info;

/// Global port counter to ensure unique ports across test runs
static PORT_COUNTER: AtomicU16 = AtomicU16::new(0);

/// Allocate a unique port range for a test cluster
fn allocate_port_range(_num_nodes: usize) -> u16 {
    // Each cluster needs num_nodes * 2 ports (kafka + inter-broker per node)
    // We allocate in blocks of 100 to give breathing room
    let block_size = 100u16;
    let offset = PORT_COUNTER.fetch_add(block_size, Ordering::SeqCst);
    // Use PID-based offset to avoid conflicts between test sessions
    // Hash the PID to get a value in range 0-99 to add extra uniqueness
    let pid_offset = (std::process::id() % 100) as u16 * 100;
    // Start from port 40000 to avoid common ports and lower ephemeral range
    40000 + pid_offset + offset
}

/// Chaos cluster for testing failure scenarios
#[allow(dead_code)]
pub struct ChaosCluster {
    /// All nodes in the cluster
    nodes: Vec<ChaosNode>,
    /// Temporary directories for data storage
    temp_dirs: Vec<TempDir>,
    /// Network simulator for controlled failures
    network: Arc<NetworkSimulator>,
}

impl ChaosCluster {
    /// Create a new chaos cluster with the specified number of nodes
    pub async fn new(num_nodes: usize) -> Self {
        let mut nodes = Vec::with_capacity(num_nodes);
        let mut temp_dirs = Vec::with_capacity(num_nodes);
        let network = Arc::new(NetworkSimulator::new());

        // Allocate unique port range for this cluster to avoid conflicts in parallel tests
        let base_port = allocate_port_range(num_nodes);
        let base_kafka_port = base_port as usize;
        let base_inter_broker_port = (base_port + 50) as usize;

        // First, create all configs with seed nodes
        let seed_addrs: Vec<SocketAddr> = (1..=num_nodes)
            .map(|i| {
                format!("127.0.0.1:{}", base_inter_broker_port + i)
                    .parse()
                    .unwrap()
            })
            .collect();

        for i in 1..=num_nodes {
            let node_id = i as u64;
            let kafka_port = base_kafka_port + i;
            let inter_broker_port = base_inter_broker_port + i;

            let temp_dir = tempfile::tempdir().unwrap();
            let data_path = temp_dir.path().to_path_buf();

            // Create config with seed nodes (excluding self)
            let mut seed_nodes: Vec<SocketAddr> = seed_addrs
                .iter()
                .filter(|addr| addr.port() != inter_broker_port as u16)
                .cloned()
                .collect();

            // First node has no seeds (will bootstrap), others have seeds
            if i == 1 {
                seed_nodes.clear();
            }

            let config = ClusterConfig {
                node_id,
                advertised_addr: format!("127.0.0.1:{}", kafka_port).parse().unwrap(),
                inter_broker_addr: format!("127.0.0.1:{}", inter_broker_port).parse().unwrap(),
                seed_nodes,
                default_replication_factor: 3.min(num_nodes as i16),
                min_insync_replicas: (num_nodes as i16 / 2 + 1).min(2),
                heartbeat_interval_ms: 200,
                session_timeout_ms: 5000,
                // Use longer election timeouts to reduce leader churn during
                // membership changes and avoid openraft replication panics
                election_timeout_min_ms: 1000,
                election_timeout_max_ms: 2000,
                unclean_leader_election: false,
                inter_broker_tls: InterBrokerTlsConfig::default(),
                rack_id: None,
                rack_aware_assignment: true,
            };

            let node = ChaosNode::new(node_id, config, data_path, network.clone());
            nodes.push(node);
            temp_dirs.push(temp_dir);
        }

        Self {
            nodes,
            temp_dirs,
            network,
        }
    }

    /// Start all nodes in the cluster
    pub async fn start_all(&mut self) -> Result<(), String> {
        // Start the first node and bootstrap
        info!("Starting and bootstrapping node 1");
        self.nodes[0].start().await?;
        self.nodes[0].bootstrap().await?;

        // Wait for bootstrap to complete and leader election
        // Longer wait to ensure leader is stable before adding members
        sleep(Duration::from_secs(3)).await;

        // Start remaining nodes and have them join one at a time
        // The join handler now waits for learners to catch up before
        // promoting to voter, which prevents split-vote scenarios
        for i in 1..self.nodes.len() {
            info!("Starting node {} and joining cluster", i + 1);
            self.nodes[i].start().await?;
            // Give node time to start RPC listener
            sleep(Duration::from_millis(500)).await;
            // Join the existing cluster (join handler waits for catch-up)
            self.nodes[i].join().await?;
            // Longer stabilization period to allow replication to complete
            // and avoid openraft replication panics from rapid membership changes
            sleep(Duration::from_secs(2)).await;
        }

        // Wait for cluster to fully stabilize - leader should be established
        sleep(Duration::from_secs(3)).await;

        Ok(())
    }

    /// Stop all nodes in the cluster
    pub async fn stop_all(&mut self) {
        for node in &mut self.nodes {
            if node.is_running() {
                let _ = node.stop().await;
            }
        }
    }

    /// Get a reference to a specific node
    pub fn node(&self, node_id: u64) -> Option<&ChaosNode> {
        self.nodes.iter().find(|n| n.node_id == node_id)
    }

    /// Get a mutable reference to a specific node
    pub fn node_mut(&mut self, node_id: u64) -> Option<&mut ChaosNode> {
        self.nodes.iter_mut().find(|n| n.node_id == node_id)
    }

    /// Find the current leader node by consensus
    ///
    /// This queries all running nodes for what they believe the leader is.
    /// Returns the leader ID if a majority of running nodes agree, otherwise None.
    pub async fn find_leader(&self) -> Option<u64> {
        use std::collections::HashMap;

        let mut leader_votes: HashMap<u64, usize> = HashMap::new();
        let mut responding_nodes = 0;

        for node in &self.nodes {
            if node.is_running() {
                // Use timeout to avoid deadlocks on both the lock and leader_id call
                let lock_result =
                    tokio::time::timeout(Duration::from_millis(500), node.manager.read()).await;

                if let Ok(guard) = lock_result {
                    if let Some(ref manager) = *guard {
                        responding_nodes += 1;
                        // Also timeout the leader_id call to avoid potential hangs
                        if let Ok(Some(leader_id)) =
                            tokio::time::timeout(Duration::from_millis(500), manager.leader_id())
                                .await
                        {
                            *leader_votes.entry(leader_id).or_insert(0) += 1;
                        }
                    }
                }
            }
        }

        // Return the leader if there's majority consensus
        let majority = (responding_nodes / 2) + 1;
        for (leader_id, votes) in leader_votes {
            if votes >= majority {
                return Some(leader_id);
            }
        }

        None
    }

    /// Wait for a leader to be elected
    pub async fn wait_for_leader(&self, timeout: Duration) -> Result<u64, String> {
        let deadline = tokio::time::Instant::now() + timeout;

        while tokio::time::Instant::now() < deadline {
            if let Some(leader) = self.find_leader().await {
                return Ok(leader);
            }
            sleep(Duration::from_millis(100)).await;
        }

        Err("Timeout waiting for leader".to_string())
    }

    /// Wait for a specific number of brokers to register in the cluster
    pub async fn wait_for_brokers(&self, count: usize, timeout: Duration) -> Result<(), String> {
        let deadline = tokio::time::Instant::now() + timeout;

        while tokio::time::Instant::now() < deadline {
            if let Ok(metadata) = self.get_metadata().await {
                if metadata.brokers.len() >= count {
                    return Ok(());
                }
                info!(
                    "Waiting for brokers: {}/{} registered",
                    metadata.brokers.len(),
                    count
                );
            }
            sleep(Duration::from_millis(200)).await;
        }

        // Get final count for error message
        let current_count = self
            .get_metadata()
            .await
            .map(|m| m.brokers.len())
            .unwrap_or(0);
        Err(format!(
            "Timeout waiting for {} brokers (only {} registered)",
            count, current_count
        ))
    }

    /// Get cluster metadata from any available node
    pub async fn get_metadata(&self) -> Result<streamline::ClusterMetadata, String> {
        for node in &self.nodes {
            if node.is_running() {
                // Use timeout to avoid deadlocks on both lock and metadata call
                let lock_result =
                    tokio::time::timeout(Duration::from_millis(500), node.manager.read()).await;

                if let Ok(guard) = lock_result {
                    if let Some(ref manager) = *guard {
                        // Also timeout the metadata call
                        if let Ok(metadata) =
                            tokio::time::timeout(Duration::from_millis(500), manager.metadata())
                                .await
                        {
                            return Ok(metadata);
                        }
                    }
                }
            }
        }
        Err("No running nodes".to_string())
    }

    /// Partition a node from the cluster (simulate network isolation)
    #[allow(dead_code)]
    pub async fn partition_node(&self, node_id: u64) {
        self.network.isolate_node(node_id).await;
    }

    /// Heal a partition (restore network connectivity)
    #[allow(dead_code)]
    pub async fn heal_partition(&self, node_id: u64) {
        self.network.restore_node(node_id).await;
    }

    /// Get the number of running nodes
    pub fn running_count(&self) -> usize {
        self.nodes.iter().filter(|n| n.is_running()).count()
    }
}

/// A single node in the chaos cluster
#[allow(dead_code)]
pub struct ChaosNode {
    /// Node ID
    pub node_id: u64,
    /// Cluster configuration
    config: ClusterConfig,
    /// Data directory path
    data_path: PathBuf,
    /// Cluster manager (when running)
    manager: Arc<RwLock<Option<ClusterManager>>>,
    /// Topic manager (when running)
    topic_manager: Arc<RwLock<Option<Arc<TopicManager>>>>,
    /// Whether the node is running
    running: Arc<AtomicBool>,
    /// Network simulator reference
    network: Arc<NetworkSimulator>,
}

impl ChaosNode {
    /// Create a new chaos node
    pub fn new(
        node_id: u64,
        config: ClusterConfig,
        data_path: PathBuf,
        network: Arc<NetworkSimulator>,
    ) -> Self {
        Self {
            node_id,
            config,
            data_path,
            manager: Arc::new(RwLock::new(None)),
            topic_manager: Arc::new(RwLock::new(None)),
            running: Arc::new(AtomicBool::new(false)),
            network,
        }
    }

    /// Start the node
    pub async fn start(&mut self) -> Result<(), String> {
        if self.running.load(Ordering::SeqCst) {
            return Err("Node already running".to_string());
        }

        info!(node_id = self.node_id, "Starting chaos node");

        // Create cluster manager
        let manager = ClusterManager::new(self.config.clone(), self.data_path.clone())
            .await
            .map_err(|e| format!("Failed to create cluster manager: {}", e))?;

        // Start RPC listener
        manager
            .start_rpc_listener()
            .await
            .map_err(|e| format!("Failed to start RPC listener: {}", e))?;

        // Create topic manager
        let topic_path = self.data_path.join("topics");
        std::fs::create_dir_all(&topic_path).map_err(|e| e.to_string())?;
        let topic_manager = Arc::new(
            TopicManager::new(&topic_path)
                .map_err(|e| format!("Failed to create topic manager: {}", e))?,
        );

        // Wire up topic manager to cluster
        manager.set_topic_manager(topic_manager.clone()).await;

        // Store references
        *self.manager.write().await = Some(manager);
        *self.topic_manager.write().await = Some(topic_manager);
        self.running.store(true, Ordering::SeqCst);

        Ok(())
    }

    /// Bootstrap this node as the initial cluster leader
    pub async fn bootstrap(&self) -> Result<(), String> {
        let guard = self.manager.read().await;
        let manager = guard
            .as_ref()
            .ok_or_else(|| "Node not running".to_string())?;

        manager
            .bootstrap()
            .await
            .map_err(|e| format!("Bootstrap failed: {}", e))?;

        Ok(())
    }

    /// Join an existing cluster via seed nodes
    pub async fn join(&self) -> Result<(), String> {
        let guard = self.manager.read().await;
        let manager = guard
            .as_ref()
            .ok_or_else(|| "Node not running".to_string())?;

        // Get seed nodes from config
        let seed_nodes = &self.config.seed_nodes;
        if seed_nodes.is_empty() {
            return Err("No seed nodes configured".to_string());
        }

        manager
            .join_cluster(seed_nodes)
            .await
            .map_err(|e| format!("Join failed: {}", e))?;

        Ok(())
    }

    /// Stop the node
    pub async fn stop(&mut self) -> Result<(), String> {
        if !self.running.load(Ordering::SeqCst) {
            return Ok(());
        }

        info!(node_id = self.node_id, "Stopping chaos node");

        // Shutdown cluster manager
        if let Some(manager) = self.manager.write().await.take() {
            manager
                .shutdown()
                .await
                .map_err(|e| format!("Shutdown failed: {}", e))?;
        }

        // Clear topic manager
        *self.topic_manager.write().await = None;
        self.running.store(false, Ordering::SeqCst);

        Ok(())
    }

    /// Check if the node is running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Check if this node is the leader
    #[allow(dead_code)]
    pub async fn is_leader(&self) -> bool {
        if let Some(ref manager) = *self.manager.read().await {
            manager.is_leader().await
        } else {
            false
        }
    }

    /// Get what this node believes the leader ID to be
    #[allow(dead_code)]
    pub async fn get_leader_id(&self) -> Option<u64> {
        if let Some(ref manager) = *self.manager.read().await {
            manager.leader_id().await
        } else {
            None
        }
    }

    /// Create a topic on this node
    pub async fn create_topic(
        &self,
        name: &str,
        partitions: i32,
        replication_factor: i16,
    ) -> Result<(), String> {
        let guard = self.manager.read().await;
        let manager = guard
            .as_ref()
            .ok_or_else(|| "Node not running".to_string())?;

        manager
            .create_topic(name.to_string(), partitions, replication_factor)
            .await
            .map_err(|e| format!("Failed to create topic: {}", e))?;

        Ok(())
    }
}

/// Network simulator for controlled failure injection
pub struct NetworkSimulator {
    /// Isolated nodes (cannot communicate with others)
    isolated_nodes: RwLock<Vec<u64>>,
    /// Partitions (groups of nodes that can't communicate with each other)
    partitions: RwLock<Vec<Vec<u64>>>,
}

impl NetworkSimulator {
    /// Create a new network simulator
    pub fn new() -> Self {
        Self {
            isolated_nodes: RwLock::new(Vec::new()),
            partitions: RwLock::new(Vec::new()),
        }
    }

    /// Isolate a node from the cluster
    pub async fn isolate_node(&self, node_id: u64) {
        let mut isolated = self.isolated_nodes.write().await;
        if !isolated.contains(&node_id) {
            info!(node_id, "Isolating node from cluster");
            isolated.push(node_id);
        }
    }

    /// Restore a node's network connectivity
    pub async fn restore_node(&self, node_id: u64) {
        let mut isolated = self.isolated_nodes.write().await;
        isolated.retain(|&id| id != node_id);
        info!(node_id, "Restored node network connectivity");
    }

    /// Check if a node is isolated
    #[allow(dead_code)]
    pub async fn is_isolated(&self, node_id: u64) -> bool {
        self.isolated_nodes.read().await.contains(&node_id)
    }

    /// Create a network partition
    pub async fn create_partition(&self, group_a: Vec<u64>, group_b: Vec<u64>) {
        let mut partitions = self.partitions.write().await;
        info!(?group_a, ?group_b, "Creating network partition");
        partitions.push(group_a);
        partitions.push(group_b);
    }

    /// Heal all partitions
    pub async fn heal_all(&self) {
        *self.isolated_nodes.write().await = Vec::new();
        *self.partitions.write().await = Vec::new();
        info!("Healed all network partitions");
    }

    /// Check if two nodes can communicate
    pub async fn can_communicate(&self, node_a: u64, node_b: u64) -> bool {
        let isolated = self.isolated_nodes.read().await;
        if isolated.contains(&node_a) || isolated.contains(&node_b) {
            return false;
        }

        let partitions = self.partitions.read().await;
        for partition in partitions.iter() {
            if partition.contains(&node_a) && !partition.contains(&node_b) {
                return false;
            }
            if partition.contains(&node_b) && !partition.contains(&node_a) {
                return false;
            }
        }

        true
    }
}

impl Default for NetworkSimulator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_network_simulator_isolation() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let sim = NetworkSimulator::new();

            // Initially nodes can communicate
            assert!(sim.can_communicate(1, 2).await);

            // Isolate node 1
            sim.isolate_node(1).await;
            assert!(!sim.can_communicate(1, 2).await);
            assert!(sim.can_communicate(2, 3).await);

            // Restore node 1
            sim.restore_node(1).await;
            assert!(sim.can_communicate(1, 2).await);
        });
    }
}
