//! Internal protocol for inter-broker communication
//!
//! This module defines the wire protocol for communication between brokers
//! for data replication and cluster coordination. These are custom API keys
//! that extend the Kafka protocol for internal use.
//!
//! ## API Keys
//!
//! - `REPLICA_FETCH` (10004): Followers fetch data from leaders
//! - `LEADER_AND_ISR` (10005): Controller pushes partition leadership changes
//! - `UPDATE_METADATA` (10006): Controller pushes cluster metadata updates
//! - `STOP_REPLICA` (10007): Controller tells broker to stop replicating a partition
//!
//! ## Wire Format
//!
//! All messages use length-prefixed JSON encoding over TCP:
//! ```text
//! [4 bytes: message length (big-endian)] [message JSON bytes]
//! ```

use crate::cluster::node::NodeId;
use crate::storage::Record;
use serde::{Deserialize, Serialize};

/// Internal API keys for inter-broker communication
pub mod api_keys {
    /// Follower fetches data from leader
    pub const REPLICA_FETCH: i16 = 10004;
    /// Controller pushes partition leadership/ISR changes
    pub const LEADER_AND_ISR: i16 = 10005;
    /// Controller pushes cluster metadata updates
    pub const UPDATE_METADATA: i16 = 10006;
    /// Controller tells broker to stop replicating a partition
    pub const STOP_REPLICA: i16 = 10007;
}

/// Wrapper for all internal protocol messages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum InternalMessage {
    /// Replica fetch request from follower to leader
    ReplicaFetchRequest(ReplicaFetchRequest),
    /// Replica fetch response from leader to follower
    ReplicaFetchResponse(ReplicaFetchResponse),
    /// Leader and ISR update from controller
    LeaderAndIsrRequest(LeaderAndIsrRequest),
    /// Response to leader and ISR update
    LeaderAndIsrResponse(LeaderAndIsrResponse),
    /// Metadata update from controller
    UpdateMetadataRequest(UpdateMetadataRequest),
    /// Response to metadata update
    UpdateMetadataResponse(UpdateMetadataResponse),
    /// Stop replica request from controller
    StopReplicaRequest(StopReplicaRequest),
    /// Response to stop replica
    StopReplicaResponse(StopReplicaResponse),
}

// ============================================================================
// Replica Fetch Protocol
// ============================================================================

/// Request from a follower to fetch data from a leader
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaFetchRequest {
    /// Replica ID of the follower making the request
    pub replica_id: NodeId,
    /// Maximum wait time in ms if not enough data
    pub max_wait_ms: i32,
    /// Minimum bytes to accumulate before returning
    pub min_bytes: i32,
    /// Maximum bytes to return
    pub max_bytes: i32,
    /// Topics and partitions to fetch
    pub topics: Vec<FetchTopic>,
}

/// Topic in a fetch request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchTopic {
    /// Topic name
    pub name: String,
    /// Partitions to fetch
    pub partitions: Vec<FetchPartition>,
}

/// Partition in a fetch request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchPartition {
    /// Partition ID
    pub partition: i32,
    /// Current leader epoch as known by the follower
    pub current_leader_epoch: i32,
    /// Offset to fetch from
    pub fetch_offset: i64,
    /// Log start offset of the follower (for log truncation detection)
    pub log_start_offset: i64,
    /// Maximum bytes for this partition
    pub partition_max_bytes: i32,
}

/// Response to a replica fetch request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaFetchResponse {
    /// Throttle time in ms
    pub throttle_time_ms: i32,
    /// Topics with fetched data
    pub topics: Vec<FetchTopicResponse>,
}

/// Topic in a fetch response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchTopicResponse {
    /// Topic name
    pub name: String,
    /// Partitions with fetched data
    pub partitions: Vec<FetchPartitionResponse>,
}

/// Partition in a fetch response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchPartitionResponse {
    /// Partition ID
    pub partition: i32,
    /// Error code (0 = success)
    pub error_code: i16,
    /// High watermark
    pub high_watermark: i64,
    /// Last stable offset (for transactions)
    pub last_stable_offset: i64,
    /// Log start offset
    pub log_start_offset: i64,
    /// Preferred read replica (for follower fetching optimization)
    pub preferred_read_replica: Option<NodeId>,
    /// Record batches
    pub records: Vec<RecordData>,
}

/// Serializable record data for wire transfer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordData {
    /// Record offset
    pub offset: i64,
    /// Record timestamp
    pub timestamp: i64,
    /// Record key (base64 encoded if binary)
    pub key: Option<Vec<u8>>,
    /// Record value
    pub value: Vec<u8>,
    /// Record headers
    pub headers: Vec<RecordHeader>,
}

/// Record header for wire transfer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordHeader {
    /// Header key
    pub key: String,
    /// Header value
    pub value: Vec<u8>,
}

impl From<&Record> for RecordData {
    fn from(record: &Record) -> Self {
        Self {
            offset: record.offset,
            timestamp: record.timestamp,
            key: record.key.as_ref().map(|k| k.to_vec()),
            value: record.value.to_vec(),
            headers: record
                .headers
                .iter()
                .map(|h| RecordHeader {
                    key: h.key.clone(),
                    value: h.value.to_vec(),
                })
                .collect(),
        }
    }
}

// ============================================================================
// Leader and ISR Protocol
// ============================================================================

/// Request from controller to update partition leadership and ISR
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaderAndIsrRequest {
    /// Controller ID
    pub controller_id: NodeId,
    /// Controller epoch
    pub controller_epoch: i32,
    /// Broker epoch (for fencing stale requests)
    pub broker_epoch: i64,
    /// Partition states to update
    pub partition_states: Vec<LeaderAndIsrPartitionState>,
    /// Live brokers in the cluster
    pub live_brokers: Vec<LiveBroker>,
}

/// Partition state in a LeaderAndIsr request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaderAndIsrPartitionState {
    /// Topic name
    pub topic: String,
    /// Partition ID
    pub partition: i32,
    /// Controller epoch
    pub controller_epoch: i32,
    /// Leader node ID
    pub leader: NodeId,
    /// Leader epoch
    pub leader_epoch: i32,
    /// ISR (In-Sync Replica) list
    pub isr: Vec<NodeId>,
    /// Partition epoch (ZK version equivalent)
    pub partition_epoch: i32,
    /// All replicas for this partition
    pub replicas: Vec<NodeId>,
    /// Replicas being added (for reassignment)
    pub adding_replicas: Vec<NodeId>,
    /// Replicas being removed (for reassignment)
    pub removing_replicas: Vec<NodeId>,
    /// Whether this is a new partition assignment
    pub is_new: bool,
}

/// Live broker information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveBroker {
    /// Broker node ID
    pub id: NodeId,
    /// Client-facing address
    pub host: String,
    /// Client-facing port
    pub port: i32,
    /// Inter-broker address
    pub inter_broker_host: String,
    /// Inter-broker port
    pub inter_broker_port: i32,
    /// Optional rack ID
    pub rack: Option<String>,
}

/// Response to a LeaderAndIsr request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaderAndIsrResponse {
    /// Error code (top-level)
    pub error_code: i16,
    /// Per-partition results
    pub partitions: Vec<LeaderAndIsrPartitionResponse>,
}

/// Per-partition result of LeaderAndIsr
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaderAndIsrPartitionResponse {
    /// Topic name
    pub topic: String,
    /// Partition ID
    pub partition: i32,
    /// Error code for this partition
    pub error_code: i16,
}

// ============================================================================
// Update Metadata Protocol
// ============================================================================

/// Request from controller to update cluster metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateMetadataRequest {
    /// Controller ID
    pub controller_id: NodeId,
    /// Controller epoch
    pub controller_epoch: i32,
    /// Broker epoch
    pub broker_epoch: i64,
    /// Partition states
    pub partition_states: Vec<UpdateMetadataPartitionState>,
    /// Live brokers
    pub live_brokers: Vec<LiveBroker>,
}

/// Partition state in UpdateMetadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateMetadataPartitionState {
    /// Topic name
    pub topic: String,
    /// Partition ID
    pub partition: i32,
    /// Controller epoch
    pub controller_epoch: i32,
    /// Leader node ID (-1 if no leader)
    pub leader: i32,
    /// Leader epoch
    pub leader_epoch: i32,
    /// ISR list
    pub isr: Vec<NodeId>,
    /// Partition epoch
    pub partition_epoch: i32,
    /// All replicas
    pub replicas: Vec<NodeId>,
    /// Offline replicas
    pub offline_replicas: Vec<NodeId>,
}

/// Response to UpdateMetadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateMetadataResponse {
    /// Error code
    pub error_code: i16,
}

// ============================================================================
// Stop Replica Protocol
// ============================================================================

/// Request from controller to stop replicating partitions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StopReplicaRequest {
    /// Controller ID
    pub controller_id: NodeId,
    /// Controller epoch
    pub controller_epoch: i32,
    /// Broker epoch
    pub broker_epoch: i64,
    /// Whether to delete the partition data
    pub delete_partitions: bool,
    /// Partitions to stop
    pub partitions: Vec<StopReplicaPartition>,
}

/// Partition to stop replicating
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StopReplicaPartition {
    /// Topic name
    pub topic: String,
    /// Partition ID
    pub partition: i32,
    /// Partition leader epoch (for fencing)
    pub leader_epoch: i32,
    /// Whether to delete the partition data
    pub delete_partition: bool,
}

/// Response to StopReplica
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StopReplicaResponse {
    /// Error code (top-level)
    pub error_code: i16,
    /// Per-partition results
    pub partitions: Vec<StopReplicaPartitionResponse>,
}

/// Per-partition result of StopReplica
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StopReplicaPartitionResponse {
    /// Topic name
    pub topic: String,
    /// Partition ID
    pub partition: i32,
    /// Error code for this partition
    pub error_code: i16,
}

// ============================================================================
// Error Codes
// ============================================================================

/// Error codes for internal protocol responses
pub mod error_codes {
    /// No error
    pub const NONE: i16 = 0;
    /// Unknown topic or partition
    pub const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
    /// Not leader or follower for partition
    pub const NOT_LEADER_OR_FOLLOWER: i16 = 6;
    /// Request not valid at this broker
    pub const BROKER_NOT_AVAILABLE: i16 = 8;
    /// Replica not available
    pub const REPLICA_NOT_AVAILABLE: i16 = 9;
    /// Stale controller epoch
    pub const STALE_CONTROLLER_EPOCH: i16 = 11;
    /// Offset out of range
    pub const OFFSET_OUT_OF_RANGE: i16 = 1;
    /// Stale broker epoch
    pub const STALE_BROKER_EPOCH: i16 = 77;
    /// Fenced leader epoch
    pub const FENCED_LEADER_EPOCH: i16 = 74;
    /// Unknown leader epoch
    pub const UNKNOWN_LEADER_EPOCH: i16 = 75;
}

// ============================================================================
// Builder helpers
// ============================================================================

impl ReplicaFetchRequest {
    /// Create a new replica fetch request
    pub fn new(replica_id: NodeId) -> Self {
        Self {
            replica_id,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 10 * 1024 * 1024, // 10MB
            topics: Vec::new(),
        }
    }

    /// Add a topic to fetch
    pub fn add_topic(mut self, name: String) -> Self {
        self.topics.push(FetchTopic {
            name,
            partitions: Vec::new(),
        });
        self
    }

    /// Add a partition to the last topic
    pub fn add_partition(mut self, partition: i32, fetch_offset: i64, leader_epoch: i32) -> Self {
        if let Some(topic) = self.topics.last_mut() {
            topic.partitions.push(FetchPartition {
                partition,
                current_leader_epoch: leader_epoch,
                fetch_offset,
                log_start_offset: 0,
                partition_max_bytes: 1024 * 1024, // 1MB
            });
        }
        self
    }
}

impl LeaderAndIsrRequest {
    /// Create a new LeaderAndIsr request
    pub fn new(controller_id: NodeId, controller_epoch: i32) -> Self {
        Self {
            controller_id,
            controller_epoch,
            broker_epoch: 0,
            partition_states: Vec::new(),
            live_brokers: Vec::new(),
        }
    }

    /// Add a partition state
    pub fn add_partition(
        mut self,
        topic: String,
        partition: i32,
        leader: NodeId,
        leader_epoch: i32,
        isr: Vec<NodeId>,
        replicas: Vec<NodeId>,
    ) -> Self {
        self.partition_states.push(LeaderAndIsrPartitionState {
            topic,
            partition,
            controller_epoch: self.controller_epoch,
            leader,
            leader_epoch,
            isr,
            partition_epoch: 0,
            replicas,
            adding_replicas: Vec::new(),
            removing_replicas: Vec::new(),
            is_new: false,
        });
        self
    }
}

impl ReplicaFetchResponse {
    /// Create an empty response
    pub fn empty() -> Self {
        Self {
            throttle_time_ms: 0,
            topics: Vec::new(),
        }
    }

    /// Create an error response for a single partition
    pub fn error(topic: String, partition: i32, error_code: i16) -> Self {
        Self {
            throttle_time_ms: 0,
            topics: vec![FetchTopicResponse {
                name: topic,
                partitions: vec![FetchPartitionResponse {
                    partition,
                    error_code,
                    high_watermark: -1,
                    last_stable_offset: -1,
                    log_start_offset: 0,
                    preferred_read_replica: None,
                    records: Vec::new(),
                }],
            }],
        }
    }
}

impl LeaderAndIsrResponse {
    /// Create a successful response
    pub fn success() -> Self {
        Self {
            error_code: error_codes::NONE,
            partitions: Vec::new(),
        }
    }

    /// Create an error response
    pub fn error(error_code: i16) -> Self {
        Self {
            error_code,
            partitions: Vec::new(),
        }
    }

    /// Add a partition result
    pub fn with_partition(mut self, topic: String, partition: i32, error_code: i16) -> Self {
        self.partitions.push(LeaderAndIsrPartitionResponse {
            topic,
            partition,
            error_code,
        });
        self
    }
}

impl UpdateMetadataResponse {
    /// Create a successful response
    pub fn success() -> Self {
        Self {
            error_code: error_codes::NONE,
        }
    }

    /// Create an error response
    pub fn error(error_code: i16) -> Self {
        Self { error_code }
    }
}

impl StopReplicaResponse {
    /// Create a successful response
    pub fn success() -> Self {
        Self {
            error_code: error_codes::NONE,
            partitions: Vec::new(),
        }
    }

    /// Add a partition result
    pub fn with_partition(mut self, topic: String, partition: i32, error_code: i16) -> Self {
        self.partitions.push(StopReplicaPartitionResponse {
            topic,
            partition,
            error_code,
        });
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replica_fetch_request_builder() {
        let request = ReplicaFetchRequest::new(1)
            .add_topic("test-topic".to_string())
            .add_partition(0, 100, 1)
            .add_partition(1, 200, 1);

        assert_eq!(request.replica_id, 1);
        assert_eq!(request.topics.len(), 1);
        assert_eq!(request.topics[0].partitions.len(), 2);
        assert_eq!(request.topics[0].partitions[0].fetch_offset, 100);
    }

    #[test]
    fn test_leader_and_isr_request_builder() {
        let request = LeaderAndIsrRequest::new(1, 5)
            .add_partition("topic1".to_string(), 0, 1, 1, vec![1, 2, 3], vec![1, 2, 3])
            .add_partition("topic1".to_string(), 1, 2, 1, vec![2, 1, 3], vec![1, 2, 3]);

        assert_eq!(request.controller_id, 1);
        assert_eq!(request.controller_epoch, 5);
        assert_eq!(request.partition_states.len(), 2);
        assert_eq!(request.partition_states[0].leader, 1);
        assert_eq!(request.partition_states[1].leader, 2);
    }

    #[test]
    fn test_internal_message_serialization() {
        let request = ReplicaFetchRequest::new(1)
            .add_topic("test".to_string())
            .add_partition(0, 0, 1);

        let msg = InternalMessage::ReplicaFetchRequest(request);
        let serialized = serde_json::to_vec(&msg).unwrap();
        let deserialized: InternalMessage = serde_json::from_slice(&serialized).unwrap();

        match deserialized {
            InternalMessage::ReplicaFetchRequest(req) => {
                assert_eq!(req.replica_id, 1);
                assert_eq!(req.topics[0].name, "test");
            }
            _ => panic!("Expected ReplicaFetchRequest"),
        }
    }

    #[test]
    fn test_replica_fetch_response_error() {
        let response = ReplicaFetchResponse::error(
            "topic".to_string(),
            0,
            error_codes::NOT_LEADER_OR_FOLLOWER,
        );
        assert_eq!(response.topics[0].partitions[0].error_code, 6);
    }

    #[test]
    fn test_leader_and_isr_response_builder() {
        let response = LeaderAndIsrResponse::success()
            .with_partition("topic".to_string(), 0, error_codes::NONE)
            .with_partition(
                "topic".to_string(),
                1,
                error_codes::UNKNOWN_TOPIC_OR_PARTITION,
            );

        assert_eq!(response.error_code, 0);
        assert_eq!(response.partitions.len(), 2);
        assert_eq!(response.partitions[1].error_code, 3);
    }
}
