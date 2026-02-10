//! Partition leadership management
//!
//! This module handles partition leader election and tracking.
//! In a Kafka-like system, each partition has a leader broker that handles
//! all reads and writes for that partition.

use crate::cluster::node::NodeId;
use crate::cluster::ClusterManager;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Tracks the leadership state of all partitions on this node
pub struct LeadershipManager {
    /// Local node ID
    node_id: NodeId,

    /// Reference to cluster manager for metadata access
    cluster_manager: Arc<ClusterManager>,

    /// Cached partition leadership state
    /// Key: (topic, partition_id)
    leadership_cache: Arc<RwLock<HashMap<(String, i32), LeadershipState>>>,
}

/// Leadership state for a partition
#[derive(Debug, Clone)]
pub struct LeadershipState {
    /// Current leader node ID (None if unknown)
    pub leader_id: Option<NodeId>,

    /// Current leader epoch
    pub leader_epoch: i32,

    /// Whether this node is the leader
    pub is_leader: bool,

    /// List of replicas
    pub replicas: Vec<NodeId>,

    /// In-sync replicas
    pub isr: Vec<NodeId>,
}

impl LeadershipManager {
    /// Create a new leadership manager
    pub fn new(node_id: NodeId, cluster_manager: Arc<ClusterManager>) -> Self {
        Self {
            node_id,
            cluster_manager,
            leadership_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get the local node ID
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Check if this node is the leader for a partition
    pub async fn is_leader(&self, topic: &str, partition: i32) -> bool {
        // Check cache first
        {
            let cache = self.leadership_cache.read();
            if let Some(state) = cache.get(&(topic.to_string(), partition)) {
                return state.is_leader;
            }
        }

        // Refresh from cluster metadata
        self.refresh_leadership(topic, partition).await;

        let cache = self.leadership_cache.read();
        cache
            .get(&(topic.to_string(), partition))
            .map(|s| s.is_leader)
            .unwrap_or(false)
    }

    /// Get the leader ID for a partition
    pub async fn get_leader(&self, topic: &str, partition: i32) -> Option<NodeId> {
        // Check cache first
        {
            let cache = self.leadership_cache.read();
            if let Some(state) = cache.get(&(topic.to_string(), partition)) {
                return state.leader_id;
            }
        }

        // Refresh from cluster metadata
        self.refresh_leadership(topic, partition).await;

        let cache = self.leadership_cache.read();
        cache
            .get(&(topic.to_string(), partition))
            .and_then(|s| s.leader_id)
    }

    /// Get the leadership state for a partition
    pub async fn get_leadership_state(
        &self,
        topic: &str,
        partition: i32,
    ) -> Option<LeadershipState> {
        // Refresh from cluster metadata
        self.refresh_leadership(topic, partition).await;

        let cache = self.leadership_cache.read();
        cache.get(&(topic.to_string(), partition)).cloned()
    }

    /// Refresh leadership information from cluster metadata
    async fn refresh_leadership(&self, topic: &str, partition: i32) {
        let metadata = self.cluster_manager.metadata().await;

        if let Some(topic_assignment) = metadata.topics.get(topic) {
            if let Some(partition_assignment) = topic_assignment.partitions.get(&partition) {
                let state = LeadershipState {
                    leader_id: partition_assignment.leader,
                    leader_epoch: partition_assignment.leader_epoch,
                    is_leader: partition_assignment.leader == Some(self.node_id),
                    replicas: partition_assignment.replicas.clone(),
                    isr: partition_assignment.isr.clone(),
                };

                let mut cache = self.leadership_cache.write();
                cache.insert((topic.to_string(), partition), state);
            }
        }
    }

    /// Refresh all leadership information for a topic
    pub async fn refresh_topic_leadership(&self, topic: &str) {
        let metadata = self.cluster_manager.metadata().await;

        if let Some(topic_assignment) = metadata.topics.get(topic) {
            let mut cache = self.leadership_cache.write();

            for (&partition_id, partition_assignment) in &topic_assignment.partitions {
                let state = LeadershipState {
                    leader_id: partition_assignment.leader,
                    leader_epoch: partition_assignment.leader_epoch,
                    is_leader: partition_assignment.leader == Some(self.node_id),
                    replicas: partition_assignment.replicas.clone(),
                    isr: partition_assignment.isr.clone(),
                };

                cache.insert((topic.to_string(), partition_id), state);
            }
        }
    }

    /// Get all partitions where this node is the leader
    pub async fn get_leader_partitions(&self) -> Vec<(String, i32)> {
        let metadata = self.cluster_manager.metadata().await;
        let mut leader_partitions = Vec::new();

        for (topic_name, topic_assignment) in &metadata.topics {
            for (&partition_id, partition_assignment) in &topic_assignment.partitions {
                if partition_assignment.leader == Some(self.node_id) {
                    leader_partitions.push((topic_name.clone(), partition_id));
                }
            }
        }

        leader_partitions
    }

    /// Get all partitions where this node is a replica (leader or follower)
    pub async fn get_replica_partitions(&self) -> Vec<(String, i32)> {
        let metadata = self.cluster_manager.metadata().await;
        let mut replica_partitions = Vec::new();

        for (topic_name, topic_assignment) in &metadata.topics {
            for (&partition_id, partition_assignment) in &topic_assignment.partitions {
                if partition_assignment.replicas.contains(&self.node_id) {
                    replica_partitions.push((topic_name.clone(), partition_id));
                }
            }
        }

        replica_partitions
    }

    /// Elect a new leader for a partition from the ISR
    /// This should only be called by the Raft leader (controller)
    pub async fn elect_leader(
        &self,
        topic: &str,
        partition: i32,
        preferred_leader: Option<NodeId>,
    ) -> Result<NodeId, String> {
        let metadata = self.cluster_manager.metadata().await;

        let topic_assignment = metadata
            .topics
            .get(topic)
            .ok_or_else(|| format!("Topic {} not found", topic))?;

        let partition_assignment = topic_assignment
            .partitions
            .get(&partition)
            .ok_or_else(|| format!("Partition {} not found for topic {}", partition, topic))?;

        // Get alive brokers from metadata
        let alive_brokers: Vec<NodeId> =
            metadata.alive_brokers().iter().map(|b| b.node_id).collect();

        // If preferred leader is specified and is in ISR and alive, use it
        if let Some(preferred) = preferred_leader {
            if partition_assignment.isr.contains(&preferred) && alive_brokers.contains(&preferred) {
                info!(
                    topic,
                    partition,
                    new_leader = preferred,
                    "Elected preferred leader"
                );
                return Ok(preferred);
            }
        }

        // Select first ISR member that is alive
        for &replica in &partition_assignment.isr {
            if alive_brokers.contains(&replica) {
                info!(
                    topic,
                    partition,
                    new_leader = replica,
                    "Elected leader from ISR"
                );
                return Ok(replica);
            }
        }

        // If unclean leader election is allowed, select from all replicas
        // Note: This could lead to data loss
        if self.cluster_manager.config().unclean_leader_election {
            for &replica in &partition_assignment.replicas {
                if alive_brokers.contains(&replica) {
                    warn!(
                        topic,
                        partition,
                        new_leader = replica,
                        "Unclean leader election - potential data loss"
                    );
                    return Ok(replica);
                }
            }
        }

        Err(format!(
            "No eligible leader found for topic {} partition {}",
            topic, partition
        ))
    }

    /// Update leadership state after a leader change
    pub async fn update_leadership(
        &self,
        topic: &str,
        partition: i32,
        new_leader: NodeId,
        leader_epoch: i32,
    ) {
        let mut cache = self.leadership_cache.write();

        if let Some(state) = cache.get_mut(&(topic.to_string(), partition)) {
            state.leader_id = Some(new_leader);
            state.leader_epoch = leader_epoch;
            state.is_leader = new_leader == self.node_id;

            debug!(
                topic,
                partition,
                new_leader,
                leader_epoch,
                is_local = state.is_leader,
                "Leadership updated"
            );
        }
    }

    /// Invalidate cached leadership state for a partition
    pub async fn invalidate_cache(&self, topic: &str, partition: i32) {
        let mut cache = self.leadership_cache.write();
        cache.remove(&(topic.to_string(), partition));
    }

    /// Invalidate all cached leadership state for a topic
    pub async fn invalidate_topic_cache(&self, topic: &str) {
        let mut cache = self.leadership_cache.write();
        cache.retain(|(t, _), _| t != topic);
    }

    /// Clear all cached leadership state
    pub async fn clear_cache(&self) {
        let mut cache = self.leadership_cache.write();
        cache.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::ClusterConfig;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_leadership_manager_creation() {
        let dir = tempdir().unwrap();
        let config = ClusterConfig {
            node_id: 1,
            advertised_addr: "127.0.0.1:9092".parse().unwrap(),
            inter_broker_addr: "127.0.0.1:9093".parse().unwrap(),
            ..Default::default()
        };

        let cluster_manager = ClusterManager::new(config, dir.path().to_path_buf())
            .await
            .unwrap();

        let leadership = LeadershipManager::new(1, Arc::new(cluster_manager));
        assert_eq!(leadership.node_id(), 1);
    }
}
