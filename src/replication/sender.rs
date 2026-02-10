// Sender scaffolding for replication feature (not yet fully integrated)
#![allow(dead_code)]

//! Replica sender for leader replicas
//!
//! The ReplicaSender runs on leader brokers to respond to follower fetch requests.
//! It provides data from the leader's log along with the current high watermark.
//!
//! Key responsibilities:
//! - Serve fetch requests from followers
//! - Track follower progress (LEO)
//! - Provide data for ISR management decisions

use crate::cluster::node::NodeId;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tracing::{debug, info};

/// Type alias for follower states map
type FollowerStatesMap = HashMap<(String, i32), HashMap<NodeId, FollowerFetchState>>;

/// Configuration for the replica sender
#[derive(Debug, Clone)]
pub struct SenderConfig {
    /// Maximum bytes to send per partition
    pub max_bytes_per_partition: i32,

    /// Maximum bytes total per fetch response
    pub max_bytes_total: i32,

    /// Minimum bytes to wait for before responding
    pub min_bytes: i32,

    /// Maximum wait time when min_bytes not satisfied
    pub max_wait_ms: i64,
}

impl Default for SenderConfig {
    fn default() -> Self {
        Self {
            max_bytes_per_partition: 1024 * 1024, // 1MB
            max_bytes_total: 10 * 1024 * 1024,    // 10MB
            min_bytes: 1,
            max_wait_ms: 500,
        }
    }
}

/// Tracks a follower's fetch progress from the leader's perspective
#[derive(Debug, Clone)]
pub(crate) struct FollowerFetchState {
    /// Follower's node ID
    pub replica_id: NodeId,

    /// Last fetch offset requested by this follower
    pub fetch_offset: i64,

    /// Time of last fetch request
    pub last_fetch_time: Instant,

    /// Number of bytes sent to this follower
    pub bytes_sent: u64,

    /// Number of records sent to this follower
    pub records_sent: u64,
}

impl FollowerFetchState {
    /// Create new follower state
    pub fn new(replica_id: NodeId) -> Self {
        Self {
            replica_id,
            fetch_offset: 0,
            last_fetch_time: Instant::now(),
            bytes_sent: 0,
            records_sent: 0,
        }
    }

    /// Update state after serving a fetch
    pub fn record_fetch(&mut self, fetch_offset: i64, bytes: u64, records: u64) {
        self.fetch_offset = fetch_offset;
        self.last_fetch_time = Instant::now();
        self.bytes_sent += bytes;
        self.records_sent += records;
    }
}

/// Request from a follower to fetch data
#[derive(Debug, Clone)]
pub(crate) struct ReplicaFetchRequest {
    /// Follower's node ID
    pub replica_id: NodeId,

    /// Topic to fetch from
    pub topic: String,

    /// Partition to fetch from
    pub partition_id: i32,

    /// Offset to fetch from
    pub fetch_offset: i64,

    /// Maximum bytes to fetch
    pub max_bytes: i32,

    /// Current epoch as known by follower
    pub current_leader_epoch: i32,
}

/// Response to a follower fetch request
#[derive(Debug, Clone)]
pub(crate) struct ReplicaFetchResponse {
    /// Topic
    pub topic: String,

    /// Partition ID
    pub partition_id: i32,

    /// Error code (0 = success)
    pub error_code: i16,

    /// High watermark to report to follower
    pub high_watermark: i64,

    /// Last stable offset (for transactional reads, usually same as HWM)
    pub last_stable_offset: i64,

    /// Log start offset
    pub log_start_offset: i64,

    /// Record data (raw bytes)
    pub records: Vec<u8>,

    /// Number of records included
    pub record_count: usize,

    /// Last offset in the response
    pub last_offset: i64,
}

impl ReplicaFetchResponse {
    /// Create an error response
    pub fn error(topic: String, partition_id: i32, error_code: i16) -> Self {
        Self {
            topic,
            partition_id,
            error_code,
            high_watermark: -1,
            last_stable_offset: -1,
            log_start_offset: 0,
            records: Vec::new(),
            record_count: 0,
            last_offset: -1,
        }
    }

    /// Create a successful response
    pub fn success(
        topic: String,
        partition_id: i32,
        high_watermark: i64,
        log_start_offset: i64,
        records: Vec<u8>,
        record_count: usize,
        last_offset: i64,
    ) -> Self {
        Self {
            topic,
            partition_id,
            error_code: 0,
            high_watermark,
            last_stable_offset: high_watermark,
            log_start_offset,
            records,
            record_count,
            last_offset,
        }
    }
}

/// Error codes for replica fetch
pub mod error_codes {
    /// No error
    pub const NONE: i16 = 0;
    /// Not leader for partition
    pub const NOT_LEADER_OR_FOLLOWER: i16 = 6;
    /// Unknown topic or partition
    pub const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
    /// Offset out of range
    pub const OFFSET_OUT_OF_RANGE: i16 = 1;
    /// Fenced leader epoch
    pub const FENCED_LEADER_EPOCH: i16 = 74;
    /// Unknown leader epoch
    pub const UNKNOWN_LEADER_EPOCH: i16 = 75;
}

/// Manages replica sending for all leader partitions on this node
#[derive(Debug)]
pub struct ReplicaSender {
    /// Local node ID
    node_id: NodeId,

    /// Sender configuration
    config: SenderConfig,

    /// Follower states by (topic, partition) -> replica_id -> state
    follower_states: Arc<RwLock<FollowerStatesMap>>,
}

impl ReplicaSender {
    /// Create a new replica sender
    pub fn new(node_id: NodeId, config: SenderConfig) -> Self {
        Self {
            node_id,
            config,
            follower_states: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get the local node ID
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Get sender configuration
    pub fn config(&self) -> &SenderConfig {
        &self.config
    }

    /// Register a partition as being led by this node
    pub async fn register_partition(&self, topic: &str, partition_id: i32) {
        let mut states = self.follower_states.write().await;
        states.insert((topic.to_string(), partition_id), HashMap::new());

        info!(
            topic,
            partition_id,
            node = self.node_id,
            "Registered partition for replica sending"
        );
    }

    /// Unregister a partition (no longer leader)
    pub async fn unregister_partition(&self, topic: &str, partition_id: i32) {
        let mut states = self.follower_states.write().await;
        states.remove(&(topic.to_string(), partition_id));

        debug!(
            topic,
            partition_id,
            node = self.node_id,
            "Unregistered partition from replica sending"
        );
    }

    /// Check if we're tracking a partition
    pub async fn is_tracking(&self, topic: &str, partition_id: i32) -> bool {
        let states = self.follower_states.read().await;
        states.contains_key(&(topic.to_string(), partition_id))
    }

    /// Record a fetch request from a follower
    pub async fn record_fetch_request(
        &self,
        topic: &str,
        partition_id: i32,
        replica_id: NodeId,
        fetch_offset: i64,
        bytes_sent: u64,
        records_sent: u64,
    ) {
        let mut states = self.follower_states.write().await;

        if let Some(partition_states) = states.get_mut(&(topic.to_string(), partition_id)) {
            let follower = partition_states
                .entry(replica_id)
                .or_insert_with(|| FollowerFetchState::new(replica_id));
            follower.record_fetch(fetch_offset, bytes_sent, records_sent);

            debug!(
                topic,
                partition_id, replica_id, fetch_offset, "Recorded follower fetch"
            );
        }
    }

    /// Get the fetch offset for a follower
    pub async fn get_follower_fetch_offset(
        &self,
        topic: &str,
        partition_id: i32,
        replica_id: NodeId,
    ) -> Option<i64> {
        let states = self.follower_states.read().await;

        states
            .get(&(topic.to_string(), partition_id))
            .and_then(|partition_states| partition_states.get(&replica_id))
            .map(|state| state.fetch_offset)
    }

    /// Get all follower states for a partition
    pub(crate) async fn get_follower_states(
        &self,
        topic: &str,
        partition_id: i32,
    ) -> HashMap<NodeId, FollowerFetchState> {
        let states = self.follower_states.read().await;

        states
            .get(&(topic.to_string(), partition_id))
            .cloned()
            .unwrap_or_default()
    }

    /// Get list of partitions being tracked
    pub async fn list_partitions(&self) -> Vec<(String, i32)> {
        let states = self.follower_states.read().await;
        states.keys().cloned().collect()
    }

    /// Get statistics for a partition
    pub(crate) async fn get_partition_stats(
        &self,
        topic: &str,
        partition_id: i32,
    ) -> Option<PartitionSenderStats> {
        let states = self.follower_states.read().await;

        states
            .get(&(topic.to_string(), partition_id))
            .map(|follower_states| {
                let mut total_bytes = 0u64;
                let mut total_records = 0u64;
                let mut follower_count = 0usize;

                for state in follower_states.values() {
                    total_bytes += state.bytes_sent;
                    total_records += state.records_sent;
                    follower_count += 1;
                }

                PartitionSenderStats {
                    topic: topic.to_string(),
                    partition_id,
                    follower_count,
                    total_bytes_sent: total_bytes,
                    total_records_sent: total_records,
                }
            })
    }
}

/// Statistics for a partition's replica sending
#[derive(Debug, Clone)]
pub(crate) struct PartitionSenderStats {
    /// Topic name
    pub topic: String,

    /// Partition ID
    pub partition_id: i32,

    /// Number of followers
    pub follower_count: usize,

    /// Total bytes sent to all followers
    pub total_bytes_sent: u64,

    /// Total records sent to all followers
    pub total_records_sent: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sender_config_default() {
        let config = SenderConfig::default();
        assert_eq!(config.max_bytes_per_partition, 1024 * 1024);
        assert_eq!(config.max_bytes_total, 10 * 1024 * 1024);
    }

    #[test]
    fn test_follower_fetch_state() {
        let mut state = FollowerFetchState::new(2);
        assert_eq!(state.replica_id, 2);
        assert_eq!(state.fetch_offset, 0);
        assert_eq!(state.bytes_sent, 0);

        state.record_fetch(100, 1024, 10);
        assert_eq!(state.fetch_offset, 100);
        assert_eq!(state.bytes_sent, 1024);
        assert_eq!(state.records_sent, 10);

        state.record_fetch(200, 512, 5);
        assert_eq!(state.fetch_offset, 200);
        assert_eq!(state.bytes_sent, 1536);
        assert_eq!(state.records_sent, 15);
    }

    #[test]
    fn test_replica_fetch_response_error() {
        let response =
            ReplicaFetchResponse::error("test".to_string(), 0, error_codes::NOT_LEADER_OR_FOLLOWER);
        assert_eq!(response.error_code, 6);
        assert_eq!(response.high_watermark, -1);
        assert!(response.records.is_empty());
    }

    #[test]
    fn test_replica_fetch_response_success() {
        let records = vec![1, 2, 3, 4];
        let response = ReplicaFetchResponse::success("test".to_string(), 0, 100, 0, records, 1, 99);
        assert_eq!(response.error_code, 0);
        assert_eq!(response.high_watermark, 100);
        assert_eq!(response.records.len(), 4);
        assert_eq!(response.record_count, 1);
        assert_eq!(response.last_offset, 99);
    }

    #[tokio::test]
    async fn test_replica_sender_register_unregister() {
        let sender = ReplicaSender::new(1, SenderConfig::default());

        // Register partition
        sender.register_partition("topic1", 0).await;
        assert!(sender.is_tracking("topic1", 0).await);

        // Unregister partition
        sender.unregister_partition("topic1", 0).await;
        assert!(!sender.is_tracking("topic1", 0).await);
    }

    #[tokio::test]
    async fn test_replica_sender_record_fetch() {
        let sender = ReplicaSender::new(1, SenderConfig::default());

        sender.register_partition("topic1", 0).await;

        // Record fetch
        sender
            .record_fetch_request("topic1", 0, 2, 100, 1024, 10)
            .await;
        sender
            .record_fetch_request("topic1", 0, 3, 80, 512, 5)
            .await;

        // Check offsets
        let offset2 = sender.get_follower_fetch_offset("topic1", 0, 2).await;
        assert_eq!(offset2, Some(100));

        let offset3 = sender.get_follower_fetch_offset("topic1", 0, 3).await;
        assert_eq!(offset3, Some(80));

        // Check stats
        let stats = sender.get_partition_stats("topic1", 0).await.unwrap();
        assert_eq!(stats.follower_count, 2);
        assert_eq!(stats.total_bytes_sent, 1536);
        assert_eq!(stats.total_records_sent, 15);
    }

    #[tokio::test]
    async fn test_replica_sender_list_partitions() {
        let sender = ReplicaSender::new(1, SenderConfig::default());

        sender.register_partition("topic1", 0).await;
        sender.register_partition("topic1", 1).await;
        sender.register_partition("topic2", 0).await;

        let partitions = sender.list_partitions().await;
        assert_eq!(partitions.len(), 3);
    }

    #[tokio::test]
    async fn test_get_follower_states() {
        let sender = ReplicaSender::new(1, SenderConfig::default());

        sender.register_partition("topic1", 0).await;
        sender
            .record_fetch_request("topic1", 0, 2, 100, 1024, 10)
            .await;
        sender
            .record_fetch_request("topic1", 0, 3, 80, 512, 5)
            .await;

        let states = sender.get_follower_states("topic1", 0).await;
        assert_eq!(states.len(), 2);
        assert!(states.contains_key(&2));
        assert!(states.contains_key(&3));
    }
}
