//! Raft type configuration for Streamline cluster
//!
//! This module defines the type configuration for openraft integration,
//! including the command types for cluster metadata operations.

use crate::cluster::node::{BrokerInfo, NodeId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Cursor;

// Use the openraft macro to declare types with proper defaults
openraft::declare_raft_types!(
    /// Type configuration for Streamline's Raft implementation
    pub StreamlineTypeConfig:
        D = ClusterCommand,
        R = ClusterResponse,
        NodeId = NodeId,
        Node = openraft::BasicNode,
        Entry = openraft::Entry<StreamlineTypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
);

/// Commands that can be applied to the cluster state machine via Raft
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ClusterCommand {
    /// Register a new broker in the cluster
    RegisterBroker(BrokerInfo),

    /// Remove a broker from the cluster
    RemoveBroker(NodeId),

    /// Create a new topic with partition assignments
    CreateTopic {
        name: String,
        assignment: TopicAssignment,
    },

    /// Delete a topic
    DeleteTopic(String),

    /// Update the leader for a partition
    UpdateLeader {
        topic: String,
        partition: i32,
        leader: NodeId,
        leader_epoch: i32,
    },

    /// Update the ISR (In-Sync Replicas) for a partition
    UpdateIsr {
        topic: String,
        partition: i32,
        isr: Vec<NodeId>,
    },

    /// Update broker state (e.g., mark as dead, running, etc.)
    UpdateBrokerState {
        node_id: NodeId,
        state: crate::cluster::node::NodeState,
    },
}

/// Response from applying a cluster command
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ClusterResponse {
    /// Command executed successfully
    Ok,

    /// Broker registered successfully
    BrokerRegistered { node_id: NodeId },

    /// Topic created successfully
    TopicCreated { name: String },

    /// Error occurred
    Error(String),
}

/// Assignment of partitions for a topic across brokers
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TopicAssignment {
    /// Topic name
    pub name: String,

    /// Number of partitions
    pub num_partitions: i32,

    /// Replication factor
    pub replication_factor: i16,

    /// Partition assignments: partition_id -> PartitionAssignment
    pub partitions: HashMap<i32, PartitionAssignment>,
}

impl TopicAssignment {
    /// Create a new topic assignment
    pub fn new(name: String, num_partitions: i32, replication_factor: i16) -> Self {
        Self {
            name,
            num_partitions,
            replication_factor,
            partitions: HashMap::new(),
        }
    }

    /// Add a partition assignment
    pub fn with_partition(mut self, partition: PartitionAssignment) -> Self {
        self.partitions.insert(partition.partition_id, partition);
        self
    }
}

/// Assignment information for a single partition
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartitionAssignment {
    /// Partition ID
    pub partition_id: i32,

    /// Current leader broker ID (None if no leader elected)
    pub leader: Option<NodeId>,

    /// List of replica broker IDs (includes leader)
    pub replicas: Vec<NodeId>,

    /// In-sync replica broker IDs
    pub isr: Vec<NodeId>,

    /// Leader epoch (incremented on leader change)
    pub leader_epoch: i32,
}

impl PartitionAssignment {
    /// Create a new partition assignment
    pub fn new(partition_id: i32, replicas: Vec<NodeId>) -> Self {
        Self {
            partition_id,
            leader: None,
            replicas: replicas.clone(),
            isr: replicas,
            leader_epoch: 0,
        }
    }

    /// Set the leader
    pub fn with_leader(mut self, leader: NodeId) -> Self {
        self.leader = Some(leader);
        self
    }

    /// Check if a node is in the ISR
    pub fn is_in_isr(&self, node_id: NodeId) -> bool {
        self.isr.contains(&node_id)
    }

    /// Check if a node is a replica
    pub fn is_replica(&self, node_id: NodeId) -> bool {
        self.replicas.contains(&node_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_assignment() {
        let assignment = PartitionAssignment::new(0, vec![1, 2, 3]).with_leader(1);

        assert_eq!(assignment.partition_id, 0);
        assert_eq!(assignment.leader, Some(1));
        assert_eq!(assignment.replicas, vec![1, 2, 3]);
        assert_eq!(assignment.isr, vec![1, 2, 3]);
        assert!(assignment.is_replica(1));
        assert!(assignment.is_in_isr(2));
        assert!(!assignment.is_replica(4));
    }

    #[test]
    fn test_topic_assignment() {
        let assignment = TopicAssignment::new("test-topic".to_string(), 3, 2)
            .with_partition(PartitionAssignment::new(0, vec![1, 2]).with_leader(1))
            .with_partition(PartitionAssignment::new(1, vec![2, 3]).with_leader(2));

        assert_eq!(assignment.name, "test-topic");
        assert_eq!(assignment.num_partitions, 3);
        assert_eq!(assignment.replication_factor, 2);
        assert_eq!(assignment.partitions.len(), 2);
    }

    #[test]
    fn test_cluster_command_serialization() {
        let cmd = ClusterCommand::UpdateLeader {
            topic: "test".to_string(),
            partition: 0,
            leader: 1,
            leader_epoch: 5,
        };

        let serialized = serde_json::to_string(&cmd).unwrap();
        let deserialized: ClusterCommand = serde_json::from_str(&serialized).unwrap();

        assert_eq!(cmd, deserialized);
    }
}
