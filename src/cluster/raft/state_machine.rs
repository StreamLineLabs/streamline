//! Cluster metadata for Raft state machine
//!
//! This module defines the cluster metadata that is managed by the Raft state machine.

use super::types::{PartitionAssignment, TopicAssignment};
use crate::cluster::node::{BrokerInfo, NodeId, NodeState};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, info};

/// Cluster metadata managed by the state machine
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ClusterMetadata {
    /// Cluster ID
    pub cluster_id: String,

    /// All known brokers in the cluster
    pub brokers: HashMap<NodeId, BrokerInfo>,

    /// Topic assignments (topic_name -> TopicAssignment)
    pub topics: HashMap<String, TopicAssignment>,

    /// Current controller/leader node ID
    pub controller_id: Option<NodeId>,

    /// Metadata version (incremented on each change)
    pub version: u64,
}

impl ClusterMetadata {
    /// Create new cluster metadata with a cluster ID
    pub fn new(cluster_id: String) -> Self {
        Self {
            cluster_id,
            brokers: HashMap::new(),
            topics: HashMap::new(),
            controller_id: None,
            version: 0,
        }
    }

    /// Register a broker
    pub fn register_broker(&mut self, broker: BrokerInfo) {
        info!(node_id = broker.node_id, "Registering broker");
        self.brokers.insert(broker.node_id, broker);
        self.version += 1;
    }

    /// Remove a broker
    pub fn remove_broker(&mut self, node_id: NodeId) {
        info!(node_id, "Removing broker");
        self.brokers.remove(&node_id);
        self.version += 1;
    }

    /// Update broker state
    pub fn update_broker_state(&mut self, node_id: NodeId, state: NodeState) {
        if let Some(broker) = self.brokers.get_mut(&node_id) {
            debug!(node_id, ?state, "Updating broker state");
            broker.state = state;
            self.version += 1;
        }
    }

    /// Create a topic
    pub fn create_topic(&mut self, assignment: TopicAssignment) {
        info!(topic = %assignment.name, "Creating topic");
        self.topics.insert(assignment.name.clone(), assignment);
        self.version += 1;
    }

    /// Delete a topic
    pub fn delete_topic(&mut self, topic: &str) {
        info!(topic, "Deleting topic");
        self.topics.remove(topic);
        self.version += 1;
    }

    /// Update the leader for a partition
    pub fn update_leader(
        &mut self,
        topic: &str,
        partition_id: i32,
        leader: NodeId,
        leader_epoch: i32,
    ) {
        if let Some(topic_assignment) = self.topics.get_mut(topic) {
            if let Some(partition) = topic_assignment.partitions.get_mut(&partition_id) {
                debug!(
                    topic,
                    partition_id, leader, leader_epoch, "Updating partition leader"
                );
                partition.leader = Some(leader);
                partition.leader_epoch = leader_epoch;
                self.version += 1;
            }
        }
    }

    /// Update the ISR for a partition
    pub fn update_isr(&mut self, topic: &str, partition_id: i32, isr: Vec<NodeId>) {
        if let Some(topic_assignment) = self.topics.get_mut(topic) {
            if let Some(partition) = topic_assignment.partitions.get_mut(&partition_id) {
                debug!(topic, partition_id, ?isr, "Updating partition ISR");
                partition.isr = isr;
                self.version += 1;
            }
        }
    }

    /// Get all alive brokers
    pub fn alive_brokers(&self) -> Vec<&BrokerInfo> {
        self.brokers.values().filter(|b| b.is_alive()).collect()
    }

    /// Get partition assignment for a topic
    pub fn get_partition(&self, topic: &str, partition_id: i32) -> Option<&PartitionAssignment> {
        self.topics
            .get(topic)
            .and_then(|t| t.partitions.get(&partition_id))
    }

    /// Check if a node is the leader for a partition
    pub fn is_partition_leader(&self, topic: &str, partition_id: i32, node_id: NodeId) -> bool {
        self.get_partition(topic, partition_id)
            .map(|p| p.leader == Some(node_id))
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_metadata_broker() {
        let mut metadata = ClusterMetadata::new("test-cluster".to_string());

        let broker = BrokerInfo::new(
            1,
            "127.0.0.1:9092".parse().unwrap(),
            "127.0.0.1:9093".parse().unwrap(),
        );

        metadata.register_broker(broker.clone());
        assert_eq!(metadata.brokers.len(), 1);
        assert_eq!(metadata.version, 1);

        metadata.remove_broker(1);
        assert_eq!(metadata.brokers.len(), 0);
        assert_eq!(metadata.version, 2);
    }

    #[test]
    fn test_cluster_metadata_topic() {
        let mut metadata = ClusterMetadata::new("test-cluster".to_string());

        let assignment = TopicAssignment::new("test-topic".to_string(), 3, 2);
        metadata.create_topic(assignment);

        assert!(metadata.topics.contains_key("test-topic"));
        assert_eq!(metadata.version, 1);

        metadata.delete_topic("test-topic");
        assert!(!metadata.topics.contains_key("test-topic"));
        assert_eq!(metadata.version, 2);
    }
}
