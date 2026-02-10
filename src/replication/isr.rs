// ISR management scaffolding for replication feature (not yet fully integrated)
#![allow(dead_code)]

//! In-Sync Replica (ISR) management
//!
//! The ISR is the set of replicas that are "in sync" with the leader.
//! A replica is considered in-sync if:
//! - It has fetched all messages up to the leader's log end offset
//! - It has been sending fetch requests within the replica lag time threshold
//!
//! The ISR is critical for:
//! - Determining when acks=all is satisfied
//! - Selecting a new leader during failover
//! - Calculating the high watermark

use crate::cluster::node::NodeId;
use crate::metrics;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Type alias for partition replication states map
type PartitionStatesMap = HashMap<(String, i32), Arc<PartitionReplicationState>>;

/// Configuration for ISR management
#[derive(Debug, Clone)]
pub struct IsrConfig {
    /// Maximum time a replica can lag behind before being removed from ISR
    /// Default: 30 seconds
    pub replica_lag_time_max: Duration,

    /// Maximum number of messages a replica can lag behind
    /// If 0, only time-based lag is used
    /// Default: 0 (disabled)
    pub replica_lag_max_messages: i64,

    /// Minimum number of replicas that must acknowledge for acks=all
    /// Default: 1
    pub min_insync_replicas: i16,
}

impl Default for IsrConfig {
    fn default() -> Self {
        Self {
            replica_lag_time_max: Duration::from_secs(30),
            replica_lag_max_messages: 0,
            min_insync_replicas: 1,
        }
    }
}

/// State of a follower replica as tracked by the leader
#[derive(Debug, Clone)]
pub(crate) struct FollowerState {
    /// The replica's node ID
    pub replica_id: NodeId,

    /// Last known log end offset of this follower
    pub log_end_offset: i64,

    /// Time of last successful fetch request
    pub last_fetch_time: Instant,

    /// Time when this replica caught up to leader's LEO
    pub last_caught_up_time: Instant,

    /// Whether this replica is currently in the ISR
    pub in_isr: bool,
}

impl FollowerState {
    /// Create a new follower state
    pub fn new(replica_id: NodeId) -> Self {
        let now = Instant::now();
        Self {
            replica_id,
            log_end_offset: 0,
            last_fetch_time: now,
            last_caught_up_time: now,
            in_isr: true, // Start in ISR, will be shrunk if lagging
        }
    }

    /// Check if follower is lagging based on time
    pub fn is_lagging_by_time(&self, max_lag: Duration) -> bool {
        self.last_caught_up_time.elapsed() > max_lag
    }

    /// Check if follower is lagging based on message count
    pub fn is_lagging_by_messages(&self, leader_leo: i64, max_lag: i64) -> bool {
        if max_lag == 0 {
            return false; // Disabled
        }
        leader_leo - self.log_end_offset > max_lag
    }
}

/// Replication state for a single partition (tracked by the leader)
#[derive(Debug)]
pub(crate) struct PartitionReplicationState {
    /// Topic name
    pub topic: String,

    /// Partition ID
    pub partition_id: i32,

    /// Current leader epoch
    pub leader_epoch: i32,

    /// All replicas for this partition
    pub replicas: Vec<NodeId>,

    /// Current in-sync replicas
    pub isr: Arc<RwLock<Vec<NodeId>>>,

    /// State of each follower
    pub follower_states: Arc<RwLock<HashMap<NodeId, FollowerState>>>,

    /// ISR configuration
    pub config: IsrConfig,
}

impl PartitionReplicationState {
    /// Create a new partition replication state
    pub fn new(
        topic: String,
        partition_id: i32,
        leader_epoch: i32,
        replicas: Vec<NodeId>,
        initial_isr: Vec<NodeId>,
        config: IsrConfig,
    ) -> Self {
        Self {
            topic,
            partition_id,
            leader_epoch,
            replicas,
            isr: Arc::new(RwLock::new(initial_isr)),
            follower_states: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    /// Get current ISR
    pub async fn get_isr(&self) -> Vec<NodeId> {
        self.isr.read().await.clone()
    }

    /// Get follower state
    pub async fn get_follower_state(&self, replica_id: NodeId) -> Option<FollowerState> {
        self.follower_states.read().await.get(&replica_id).cloned()
    }

    /// Update follower's fetch state
    pub async fn update_follower_fetch(
        &self,
        replica_id: NodeId,
        fetch_offset: i64,
        leader_leo: i64,
    ) {
        let now = Instant::now();
        let mut states = self.follower_states.write().await;

        let state = states
            .entry(replica_id)
            .or_insert_with(|| FollowerState::new(replica_id));

        state.log_end_offset = fetch_offset;
        state.last_fetch_time = now;

        // Check if caught up
        if fetch_offset >= leader_leo {
            state.last_caught_up_time = now;
        }

        // Record replication metrics
        let lag_messages = leader_leo - fetch_offset;
        // Estimate lag in bytes (rough estimate: assume average message size of 1KB)
        let lag_bytes = lag_messages * 1024;
        metrics::record_replication_lag(
            &self.topic,
            self.partition_id,
            replica_id,
            lag_messages,
            lag_bytes,
        );

        // Record time since last successful replication (0 for just-fetched)
        metrics::record_last_replication_time(&self.topic, self.partition_id, replica_id, 0.0);

        debug!(
            topic = %self.topic,
            partition = self.partition_id,
            replica = replica_id,
            fetch_offset,
            leader_leo,
            "Updated follower fetch state"
        );
    }

    /// Check ISR health and shrink if necessary
    /// Returns list of removed replicas
    pub async fn maybe_shrink_isr(&self, leader_leo: i64) -> Vec<NodeId> {
        let states = self.follower_states.read().await;
        let mut isr = self.isr.write().await;
        let mut removed = Vec::new();

        isr.retain(|&replica_id| {
            if let Some(state) = states.get(&replica_id) {
                let lagging_by_time = state.is_lagging_by_time(self.config.replica_lag_time_max);
                let lagging_by_messages =
                    state.is_lagging_by_messages(leader_leo, self.config.replica_lag_max_messages);

                if lagging_by_time || lagging_by_messages {
                    warn!(
                        topic = %self.topic,
                        partition = self.partition_id,
                        replica = replica_id,
                        lagging_by_time,
                        lagging_by_messages,
                        follower_leo = state.log_end_offset,
                        leader_leo,
                        "Removing lagging replica from ISR"
                    );
                    removed.push(replica_id);
                    return false;
                }
            }
            true
        });

        // Update ISR size metric after shrinking
        if !removed.is_empty() {
            metrics::update_isr_size(&self.topic, self.partition_id, isr.len());
        }

        removed
    }

    /// Check if lagging replicas have caught up and expand ISR
    /// Returns list of added replicas
    pub async fn maybe_expand_isr(&self, leader_leo: i64) -> Vec<NodeId> {
        let states = self.follower_states.read().await;
        let mut isr = self.isr.write().await;
        let mut added = Vec::new();

        for &replica_id in &self.replicas {
            // Skip if already in ISR
            if isr.contains(&replica_id) {
                continue;
            }

            if let Some(state) = states.get(&replica_id) {
                // Replica has caught up if it's fetched to leader's LEO
                // and has been active recently
                let caught_up = state.log_end_offset >= leader_leo;
                let active = state.last_fetch_time.elapsed() < self.config.replica_lag_time_max;

                if caught_up && active {
                    info!(
                        topic = %self.topic,
                        partition = self.partition_id,
                        replica = replica_id,
                        "Adding caught-up replica to ISR"
                    );
                    isr.push(replica_id);
                    added.push(replica_id);
                }
            }
        }

        // Update ISR size metric after expanding
        if !added.is_empty() {
            metrics::update_isr_size(&self.topic, self.partition_id, isr.len());
        }

        added
    }

    /// Check if we have enough replicas in ISR for acks=all
    pub async fn has_enough_isr(&self) -> bool {
        let isr = self.isr.read().await;
        isr.len() >= self.config.min_insync_replicas as usize
    }

    /// Get the number of replicas in ISR
    pub async fn isr_count(&self) -> usize {
        self.isr.read().await.len()
    }
}

/// Manages ISR state for all partitions where this node is leader
#[derive(Debug)]
pub struct IsrManager {
    /// Local node ID
    node_id: NodeId,

    /// ISR state by (topic, partition)
    partitions: Arc<RwLock<PartitionStatesMap>>,

    /// Default configuration
    default_config: IsrConfig,
}

impl IsrManager {
    /// Create a new ISR manager
    pub fn new(node_id: NodeId, config: IsrConfig) -> Self {
        Self {
            node_id,
            partitions: Arc::new(RwLock::new(HashMap::new())),
            default_config: config,
        }
    }

    /// Get local node ID
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Register a partition for ISR tracking (when becoming leader)
    pub(crate) async fn register_partition(
        &self,
        topic: &str,
        partition_id: i32,
        leader_epoch: i32,
        replicas: Vec<NodeId>,
        initial_isr: Vec<NodeId>,
    ) -> Arc<PartitionReplicationState> {
        // Record initial ISR size metric
        metrics::update_isr_size(topic, partition_id, initial_isr.len());

        let state = Arc::new(PartitionReplicationState::new(
            topic.to_string(),
            partition_id,
            leader_epoch,
            replicas,
            initial_isr,
            self.default_config.clone(),
        ));

        let mut partitions = self.partitions.write().await;
        partitions.insert((topic.to_string(), partition_id), state.clone());

        info!(
            topic,
            partition_id,
            leader_epoch,
            node = self.node_id,
            "Registered partition for ISR tracking"
        );

        state
    }

    /// Unregister a partition (when no longer leader)
    pub async fn unregister_partition(&self, topic: &str, partition_id: i32) {
        let mut partitions = self.partitions.write().await;
        partitions.remove(&(topic.to_string(), partition_id));

        debug!(
            topic,
            partition_id,
            node = self.node_id,
            "Unregistered partition from ISR tracking"
        );
    }

    /// Get partition replication state
    pub(crate) async fn get_partition(
        &self,
        topic: &str,
        partition_id: i32,
    ) -> Option<Arc<PartitionReplicationState>> {
        let partitions = self.partitions.read().await;
        partitions.get(&(topic.to_string(), partition_id)).cloned()
    }

    /// Update follower fetch for a partition
    pub async fn update_follower_fetch(
        &self,
        topic: &str,
        partition_id: i32,
        replica_id: NodeId,
        fetch_offset: i64,
        leader_leo: i64,
    ) {
        if let Some(state) = self.get_partition(topic, partition_id).await {
            state
                .update_follower_fetch(replica_id, fetch_offset, leader_leo)
                .await;
        }
    }

    /// Check and update ISR for a partition
    /// Returns (removed, added) replica lists if ISR changed
    pub async fn check_and_update_isr(
        &self,
        topic: &str,
        partition_id: i32,
        leader_leo: i64,
    ) -> Option<(Vec<NodeId>, Vec<NodeId>)> {
        let state = self.get_partition(topic, partition_id).await?;

        let removed = state.maybe_shrink_isr(leader_leo).await;
        let added = state.maybe_expand_isr(leader_leo).await;

        if removed.is_empty() && added.is_empty() {
            None
        } else {
            Some((removed, added))
        }
    }

    /// Get all partitions being tracked
    pub async fn list_partitions(&self) -> Vec<(String, i32)> {
        let partitions = self.partitions.read().await;
        partitions.keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_follower_state_creation() {
        let state = FollowerState::new(2);
        assert_eq!(state.replica_id, 2);
        assert_eq!(state.log_end_offset, 0);
        assert!(state.in_isr);
    }

    #[test]
    fn test_follower_lagging_by_time() {
        let mut state = FollowerState::new(2);
        let max_lag = Duration::from_millis(100);

        // Initially not lagging
        assert!(!state.is_lagging_by_time(max_lag));

        // Simulate time passing
        state.last_caught_up_time = Instant::now() - Duration::from_millis(200);
        assert!(state.is_lagging_by_time(max_lag));
    }

    #[test]
    fn test_follower_lagging_by_messages() {
        let mut state = FollowerState::new(2);
        state.log_end_offset = 50;

        // Not lagging when disabled
        assert!(!state.is_lagging_by_messages(100, 0));

        // Check message lag
        assert!(!state.is_lagging_by_messages(100, 100)); // 50 messages behind, threshold 100
        assert!(state.is_lagging_by_messages(100, 40)); // 50 messages behind, threshold 40
    }

    #[tokio::test]
    async fn test_partition_replication_state() {
        let config = IsrConfig::default();
        let state = PartitionReplicationState::new(
            "test".to_string(),
            0,
            1,
            vec![1, 2, 3],
            vec![1, 2, 3],
            config,
        );

        assert_eq!(state.get_isr().await, vec![1, 2, 3]);
        assert!(state.has_enough_isr().await);
    }

    #[tokio::test]
    async fn test_update_follower_fetch() {
        let config = IsrConfig::default();
        let state = PartitionReplicationState::new(
            "test".to_string(),
            0,
            1,
            vec![1, 2, 3],
            vec![1, 2, 3],
            config,
        );

        state.update_follower_fetch(2, 100, 100).await;

        let follower = state.get_follower_state(2).await.unwrap();
        assert_eq!(follower.log_end_offset, 100);
    }

    #[tokio::test]
    async fn test_isr_shrink() {
        let config = IsrConfig {
            replica_lag_time_max: Duration::from_millis(10),
            ..Default::default()
        };
        let state = PartitionReplicationState::new(
            "test".to_string(),
            0,
            1,
            vec![1, 2, 3],
            vec![1, 2, 3],
            config,
        );

        // Simulate follower 2 fetching but being behind
        {
            let mut states = state.follower_states.write().await;
            let follower = FollowerState::new(2);
            states.insert(2, follower);
            // Make follower appear lagging
            states.get_mut(&2).unwrap().last_caught_up_time =
                Instant::now() - Duration::from_millis(100);
        }

        let removed = state.maybe_shrink_isr(100).await;
        assert_eq!(removed, vec![2]);
        assert!(!state.get_isr().await.contains(&2));
    }

    #[tokio::test]
    async fn test_isr_expand() {
        let config = IsrConfig::default();
        let state = PartitionReplicationState::new(
            "test".to_string(),
            0,
            1,
            vec![1, 2, 3],
            vec![1], // Only leader in ISR initially
            config,
        );

        // Simulate follower 2 catching up
        state.update_follower_fetch(2, 100, 100).await;

        let added = state.maybe_expand_isr(100).await;
        assert_eq!(added, vec![2]);
        assert!(state.get_isr().await.contains(&2));
    }

    #[tokio::test]
    async fn test_isr_manager() {
        let config = IsrConfig::default();
        let mgr = IsrManager::new(1, config);

        // Register partition
        let state = mgr
            .register_partition("test", 0, 1, vec![1, 2, 3], vec![1, 2, 3])
            .await;
        assert_eq!(state.get_isr().await, vec![1, 2, 3]);

        // Update follower
        mgr.update_follower_fetch("test", 0, 2, 100, 100).await;

        let partitions = mgr.list_partitions().await;
        assert_eq!(partitions.len(), 1);

        // Unregister
        mgr.unregister_partition("test", 0).await;
        assert!(mgr.get_partition("test", 0).await.is_none());
    }
}
