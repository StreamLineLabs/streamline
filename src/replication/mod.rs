//! Replication module for Streamline
//!
//! This module implements Kafka-like data replication for high availability:
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    ReplicationManager                        │
//! │  Coordinates replication for all partitions on this node    │
//! ├─────────────────────────────────────────────────────────────┤
//! │                                                             │
//! │  ┌──────────────────┐      ┌──────────────────┐            │
//! │  │  IsrManager      │      │  WatermarkManager│            │
//! │  │  (ISR tracking)  │      │  (HWM/LEO)       │            │
//! │  └────────┬─────────┘      └────────┬─────────┘            │
//! │           │                         │                       │
//! │  ┌────────┴─────────────────────────┴────────┐             │
//! │  │                                           │             │
//! │  │  Leader partitions:   ReplicaSender      │             │
//! │  │  (respond to follower fetches)           │             │
//! │  │                                           │             │
//! │  │  Follower partitions: ReplicaFetcher     │             │
//! │  │  (fetch from leaders)                    │             │
//! │  │                                           │             │
//! │  └───────────────────────────────────────────┘             │
//! │                                                             │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Key Concepts
//!
//! - **Log End Offset (LEO)**: Offset of next record to be written
//! - **High Watermark (HWM)**: Offset up to which data is committed (replicated to ISR)
//! - **In-Sync Replicas (ISR)**: Replicas that are caught up with the leader
//!
//! ## Replication Flow
//!
//! 1. Producer writes to leader with acks policy
//! 2. Leader appends to local log, LEO advances
//! 3. Followers fetch from leader
//! 4. Leader tracks follower LEOs
//! 5. HWM = min(LEO of all ISR members)
//! 6. Producer receives ack when acks policy satisfied

pub mod acks;
#[cfg(feature = "geo-replication")]
pub mod active_active;
pub mod failover;
pub mod fetcher;
pub mod geo;
pub mod isr;
pub mod multi_dc;
pub mod sender;
pub mod throttle;
pub mod wan_batch;
pub mod watermark;

// Public API types
pub use acks::AcksPolicy;
pub use isr::{IsrConfig, IsrManager};
pub use watermark::WatermarkManager;

// Public configuration types
pub use fetcher::{FetcherCommand, FetcherConfig, ReplicaFetcher};
pub use sender::{ReplicaSender, SenderConfig};

use crate::cluster::node::NodeId;
use crate::storage::StorageMode;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, info};

/// Configuration for the replication manager
#[derive(Debug, Clone, Default)]
pub struct ReplicationConfig {
    /// ISR configuration
    pub isr: IsrConfig,

    /// Fetcher configuration
    pub fetcher: FetcherConfig,

    /// Sender configuration
    pub sender: SenderConfig,
}

/// Role of this node for a partition
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaRole {
    /// This node is the leader for the partition
    Leader,
    /// This node is a follower for the partition
    Follower,
    /// This node is not a replica for the partition
    None,
}

/// Key for partition storage mode tracking
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PartitionKey {
    topic: String,
    partition: i32,
}

impl PartitionKey {
    fn new(topic: &str, partition: i32) -> Self {
        Self {
            topic: topic.to_string(),
            partition,
        }
    }
}

/// Diskless partition state tracking
#[derive(Debug, Clone)]
struct DisklessPartitionState {
    role: ReplicaRole,
    /// Leader ID for this partition (for future introspection)
    #[allow(dead_code)]
    leader_id: Option<NodeId>,
    /// Leader epoch for this partition (for future introspection)
    #[allow(dead_code)]
    leader_epoch: i32,
}

/// Manages all replication for this node
pub struct ReplicationManager {
    /// Local node ID
    node_id: NodeId,

    /// Replication configuration
    config: ReplicationConfig,

    /// ISR manager (for leader partitions)
    isr_manager: Arc<IsrManager>,

    /// Watermark manager (for all partitions)
    watermark_manager: Arc<WatermarkManager>,

    /// Replica fetcher (for follower partitions)
    replica_fetcher: Arc<ReplicaFetcher>,

    /// Replica sender (for leader partitions)
    replica_sender: Arc<ReplicaSender>,

    /// Command sender for the fetcher
    fetcher_command_tx: Option<mpsc::Sender<FetcherCommand>>,

    /// Storage mode for each partition (for replication decisions)
    partition_storage_modes: RwLock<HashMap<PartitionKey, StorageMode>>,

    /// State for diskless partitions (where ISR/fetcher don't track)
    diskless_partitions: RwLock<HashMap<PartitionKey, DisklessPartitionState>>,
}

impl std::fmt::Debug for ReplicationManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplicationManager")
            .field("node_id", &self.node_id)
            .field("config", &self.config)
            .finish()
    }
}

impl ReplicationManager {
    /// Create a new replication manager
    pub fn new(node_id: NodeId, config: ReplicationConfig) -> Self {
        let isr_manager = Arc::new(IsrManager::new(node_id, config.isr.clone()));
        let watermark_manager = Arc::new(WatermarkManager::new());
        let replica_fetcher = Arc::new(ReplicaFetcher::new(node_id, config.fetcher.clone()));
        let replica_sender = Arc::new(ReplicaSender::new(node_id, config.sender.clone()));

        Self {
            node_id,
            config,
            isr_manager,
            watermark_manager,
            replica_fetcher,
            replica_sender,
            fetcher_command_tx: None,
            partition_storage_modes: RwLock::new(HashMap::new()),
            diskless_partitions: RwLock::new(HashMap::new()),
        }
    }

    /// Get local node ID
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Get the configuration
    pub fn config(&self) -> &ReplicationConfig {
        &self.config
    }

    /// Get the ISR manager
    pub fn isr_manager(&self) -> Arc<IsrManager> {
        self.isr_manager.clone()
    }

    /// Get the watermark manager
    pub fn watermark_manager(&self) -> Arc<WatermarkManager> {
        self.watermark_manager.clone()
    }

    /// Get the replica fetcher
    pub fn replica_fetcher(&self) -> Arc<ReplicaFetcher> {
        self.replica_fetcher.clone()
    }

    /// Get the replica sender
    pub fn replica_sender(&self) -> Arc<ReplicaSender> {
        self.replica_sender.clone()
    }

    /// Get the fetcher command sender
    pub fn fetcher_command_tx(&self) -> Option<mpsc::Sender<FetcherCommand>> {
        self.fetcher_command_tx.clone()
    }

    /// Become leader for a partition
    pub async fn become_leader(
        &self,
        topic: &str,
        partition_id: i32,
        leader_epoch: i32,
        replicas: Vec<NodeId>,
        initial_isr: Vec<NodeId>,
        log_end_offset: i64,
    ) {
        info!(
            topic,
            partition_id,
            leader_epoch,
            node = self.node_id,
            ?replicas,
            ?initial_isr,
            log_end_offset,
            "Becoming leader for partition"
        );

        // Stop fetching if we were a follower
        self.replica_fetcher
            .remove_partition(topic, partition_id)
            .await;

        // Register for ISR tracking
        self.isr_manager
            .register_partition(
                topic,
                partition_id,
                leader_epoch,
                replicas,
                initial_isr.clone(),
            )
            .await;

        // Register for replica sending
        self.replica_sender
            .register_partition(topic, partition_id)
            .await;

        // Initialize watermarks
        let watermarks = self
            .watermark_manager
            .get_or_create(topic, partition_id)
            .await;
        watermarks.set_log_end_offset(log_end_offset);

        // If single-node ISR, HWM = LEO
        if initial_isr.len() == 1 && initial_isr[0] == self.node_id {
            watermarks.set_high_watermark(log_end_offset);
        }
    }

    /// Become follower for a partition
    pub async fn become_follower(
        &self,
        topic: &str,
        partition_id: i32,
        leader_id: NodeId,
        leader_epoch: i32,
        fetch_offset: i64,
    ) {
        info!(
            topic,
            partition_id,
            leader_id,
            leader_epoch,
            node = self.node_id,
            fetch_offset,
            "Becoming follower for partition"
        );

        // Stop sending if we were a leader
        self.isr_manager
            .unregister_partition(topic, partition_id)
            .await;
        self.replica_sender
            .unregister_partition(topic, partition_id)
            .await;

        // Start fetching from new leader
        self.replica_fetcher
            .add_partition(topic, partition_id, leader_id, leader_epoch, fetch_offset)
            .await;

        // Initialize watermarks
        let watermarks = self
            .watermark_manager
            .get_or_create(topic, partition_id)
            .await;
        watermarks.set_log_end_offset(fetch_offset);
    }

    /// Stop replicating a partition entirely
    pub async fn stop_partition(&self, topic: &str, partition_id: i32) {
        debug!(topic, partition_id, "Stopping replication for partition");

        self.isr_manager
            .unregister_partition(topic, partition_id)
            .await;
        self.replica_fetcher
            .remove_partition(topic, partition_id)
            .await;
        self.replica_sender
            .unregister_partition(topic, partition_id)
            .await;
        self.watermark_manager.remove(topic, partition_id).await;
    }

    /// Get the role of this node for a partition
    pub async fn get_role(&self, topic: &str, partition_id: i32) -> ReplicaRole {
        // Check if we're a leader (ISR manager tracks leader partitions)
        if self
            .isr_manager
            .get_partition(topic, partition_id)
            .await
            .is_some()
        {
            return ReplicaRole::Leader;
        }

        // Check if we're a follower (fetcher tracks follower partitions)
        if self
            .replica_fetcher
            .get_partition_state(topic, partition_id)
            .await
            .is_some()
        {
            return ReplicaRole::Follower;
        }

        // Check if this is a diskless partition (tracked separately)
        let key = PartitionKey::new(topic, partition_id);
        if let Some(state) = self.diskless_partitions.read().await.get(&key) {
            return state.role;
        }

        ReplicaRole::None
    }

    /// Update follower's fetch progress (called by leader when handling fetch request)
    pub async fn update_follower_progress(
        &self,
        topic: &str,
        partition_id: i32,
        replica_id: NodeId,
        fetch_offset: i64,
        leader_leo: i64,
    ) {
        // Update ISR manager
        self.isr_manager
            .update_follower_fetch(topic, partition_id, replica_id, fetch_offset, leader_leo)
            .await;

        // Update watermark manager
        if let Some(watermarks) = self.watermark_manager.get(topic, partition_id).await {
            watermarks
                .update_follower_leo(replica_id, fetch_offset)
                .await;
        }
    }

    /// Try to advance the high watermark for a partition
    /// Returns new HWM if it advanced
    pub async fn try_advance_hwm(&self, topic: &str, partition_id: i32) -> Option<i64> {
        // Get current ISR
        let isr_state = self.isr_manager.get_partition(topic, partition_id).await?;
        let isr = isr_state.get_isr().await;

        // Get watermarks
        let watermarks = self.watermark_manager.get(topic, partition_id).await?;

        // Calculate new HWM
        watermarks
            .calculate_high_watermark(&isr, self.node_id)
            .await
    }

    /// Check and update ISR, returns (removed, added) if ISR changed
    pub async fn check_and_update_isr(
        &self,
        topic: &str,
        partition_id: i32,
    ) -> Option<(Vec<NodeId>, Vec<NodeId>)> {
        // Get leader's LEO
        let watermarks = self.watermark_manager.get(topic, partition_id).await?;
        let leader_leo = watermarks.log_end_offset();

        // Check ISR
        self.isr_manager
            .check_and_update_isr(topic, partition_id, leader_leo)
            .await
    }

    /// Check if we can accept a write based on acks policy
    pub async fn can_accept_write(&self, topic: &str, partition_id: i32, acks: AcksPolicy) -> bool {
        match acks {
            AcksPolicy::None => true,
            AcksPolicy::Leader => {
                // Just need to be leader
                self.get_role(topic, partition_id).await == ReplicaRole::Leader
            }
            AcksPolicy::All => {
                // Need to be leader and have enough ISR
                if self.get_role(topic, partition_id).await != ReplicaRole::Leader {
                    return false;
                }

                if let Some(isr_state) = self.isr_manager.get_partition(topic, partition_id).await {
                    isr_state.has_enough_isr().await
                } else {
                    false
                }
            }
        }
    }

    /// Get the high watermark for a partition
    pub async fn get_high_watermark(&self, topic: &str, partition_id: i32) -> Option<i64> {
        let watermarks = self.watermark_manager.get(topic, partition_id).await?;
        Some(watermarks.high_watermark())
    }

    /// Get the log end offset for a partition
    pub async fn get_log_end_offset(&self, topic: &str, partition_id: i32) -> Option<i64> {
        let watermarks = self.watermark_manager.get(topic, partition_id).await?;
        Some(watermarks.log_end_offset())
    }

    /// Set the log end offset after appending records
    pub async fn set_log_end_offset(&self, topic: &str, partition_id: i32, offset: i64) {
        if let Some(watermarks) = self.watermark_manager.get(topic, partition_id).await {
            watermarks.set_log_end_offset(offset);
        }
    }

    /// Shutdown the replication manager
    pub async fn shutdown(&self) {
        info!(node = self.node_id, "Shutting down replication manager");

        // Send shutdown command to fetcher
        if let Some(tx) = &self.fetcher_command_tx {
            let _ = tx.send(FetcherCommand::Shutdown).await;
        }
    }

    // ============ Storage Mode Aware Methods ============

    /// Set the storage mode for a partition
    ///
    /// This determines replication behavior:
    /// - Local/Hybrid: Full replication with ISR tracking
    /// - Diskless: No replication (S3 handles durability)
    pub async fn set_storage_mode(&self, topic: &str, partition_id: i32, mode: StorageMode) {
        let key = PartitionKey::new(topic, partition_id);
        self.partition_storage_modes.write().await.insert(key, mode);
        debug!(
            topic,
            partition_id,
            storage_mode = %mode,
            "Set storage mode for partition"
        );
    }

    /// Get the storage mode for a partition
    pub async fn get_storage_mode(&self, topic: &str, partition_id: i32) -> StorageMode {
        let key = PartitionKey::new(topic, partition_id);
        self.partition_storage_modes
            .read()
            .await
            .get(&key)
            .copied()
            .unwrap_or_default()
    }

    /// Check if this partition requires replication
    pub async fn requires_replication(&self, topic: &str, partition_id: i32) -> bool {
        self.get_storage_mode(topic, partition_id)
            .await
            .requires_replication()
    }

    /// Check if this partition is diskless
    pub async fn is_diskless(&self, topic: &str, partition_id: i32) -> bool {
        self.get_storage_mode(topic, partition_id)
            .await
            .is_diskless()
    }

    /// Become leader for a partition with storage mode awareness
    ///
    /// For diskless partitions:
    /// - No ISR tracking (S3 is durable)
    /// - No replica sending needed
    /// - HWM = LEO immediately (writes go to S3)
    #[allow(clippy::too_many_arguments)]
    pub async fn become_leader_with_mode(
        &self,
        topic: &str,
        partition_id: i32,
        leader_epoch: i32,
        replicas: Vec<NodeId>,
        initial_isr: Vec<NodeId>,
        log_end_offset: i64,
        storage_mode: StorageMode,
    ) {
        // Track the storage mode
        self.set_storage_mode(topic, partition_id, storage_mode)
            .await;

        // Clean up any existing diskless tracking
        let key = PartitionKey::new(topic, partition_id);
        self.diskless_partitions.write().await.remove(&key);

        // Stop fetching if we were a follower
        self.replica_fetcher
            .remove_partition(topic, partition_id)
            .await;

        // Initialize watermarks
        let watermarks = self
            .watermark_manager
            .get_or_create(topic, partition_id)
            .await;
        watermarks.set_log_end_offset(log_end_offset);

        if storage_mode.requires_replication() {
            // Standard replication path
            info!(
                topic,
                partition_id,
                leader_epoch,
                node = self.node_id,
                ?replicas,
                ?initial_isr,
                log_end_offset,
                storage_mode = %storage_mode,
                "Becoming leader for partition (with replication)"
            );

            // Register for ISR tracking
            self.isr_manager
                .register_partition(
                    topic,
                    partition_id,
                    leader_epoch,
                    replicas,
                    initial_isr.clone(),
                )
                .await;

            // Register for replica sending
            self.replica_sender
                .register_partition(topic, partition_id)
                .await;

            // If single-node ISR, HWM = LEO
            if initial_isr.len() == 1 && initial_isr[0] == self.node_id {
                watermarks.set_high_watermark(log_end_offset);
            }
        } else {
            // Diskless path - no replication needed
            info!(
                topic,
                partition_id,
                leader_epoch,
                node = self.node_id,
                log_end_offset,
                storage_mode = %storage_mode,
                "Becoming leader for diskless partition (no replication)"
            );

            // Track this diskless partition as leader
            let key = PartitionKey::new(topic, partition_id);
            self.diskless_partitions.write().await.insert(
                key,
                DisklessPartitionState {
                    role: ReplicaRole::Leader,
                    leader_id: Some(self.node_id),
                    leader_epoch,
                },
            );

            // For diskless, HWM = LEO immediately (S3 handles durability)
            watermarks.set_high_watermark(log_end_offset);
        }
    }

    /// Become follower for a partition with storage mode awareness
    ///
    /// For diskless partitions:
    /// - No fetching from leader needed (read from S3)
    /// - Watermarks still tracked for consumer coordination
    pub async fn become_follower_with_mode(
        &self,
        topic: &str,
        partition_id: i32,
        leader_id: NodeId,
        leader_epoch: i32,
        fetch_offset: i64,
        storage_mode: StorageMode,
    ) {
        // Track the storage mode
        self.set_storage_mode(topic, partition_id, storage_mode)
            .await;

        // Clean up any existing diskless tracking
        let key = PartitionKey::new(topic, partition_id);
        self.diskless_partitions.write().await.remove(&key);

        // Stop sending if we were a leader
        self.isr_manager
            .unregister_partition(topic, partition_id)
            .await;
        self.replica_sender
            .unregister_partition(topic, partition_id)
            .await;

        // Initialize watermarks
        let watermarks = self
            .watermark_manager
            .get_or_create(topic, partition_id)
            .await;
        watermarks.set_log_end_offset(fetch_offset);

        if storage_mode.requires_replication() {
            // Standard replication path
            info!(
                topic,
                partition_id,
                leader_id,
                leader_epoch,
                node = self.node_id,
                fetch_offset,
                storage_mode = %storage_mode,
                "Becoming follower for partition (with replication)"
            );

            // Start fetching from new leader
            self.replica_fetcher
                .add_partition(topic, partition_id, leader_id, leader_epoch, fetch_offset)
                .await;
        } else {
            // Diskless path - no fetching needed
            info!(
                topic,
                partition_id,
                leader_id,
                leader_epoch,
                node = self.node_id,
                storage_mode = %storage_mode,
                "Becoming follower for diskless partition (no replication fetch)"
            );

            // Track this diskless partition as follower
            let key = PartitionKey::new(topic, partition_id);
            self.diskless_partitions.write().await.insert(
                key,
                DisklessPartitionState {
                    role: ReplicaRole::Follower,
                    leader_id: Some(leader_id),
                    leader_epoch,
                },
            );

            // For diskless, HWM = LEO immediately (reads from S3)
            watermarks.set_high_watermark(fetch_offset);
        }
    }

    /// Stop replicating a partition entirely with cleanup
    pub async fn stop_partition_with_mode(&self, topic: &str, partition_id: i32) {
        // Remove storage mode tracking
        let key = PartitionKey::new(topic, partition_id);
        self.partition_storage_modes.write().await.remove(&key);

        // Remove diskless partition tracking
        self.diskless_partitions.write().await.remove(&key);

        // Standard cleanup
        self.stop_partition(topic, partition_id).await;
    }

    /// Check if we can accept a write based on acks policy and storage mode
    ///
    /// For diskless partitions, acks=all immediately succeeds since S3 is durable.
    pub async fn can_accept_write_with_mode(
        &self,
        topic: &str,
        partition_id: i32,
        acks: AcksPolicy,
    ) -> bool {
        // For diskless, any acks policy can be satisfied immediately
        // since S3 provides durability
        if self.is_diskless(topic, partition_id).await {
            // Just need to check we're the leader for acks > 0
            return match acks {
                AcksPolicy::None => true,
                AcksPolicy::Leader | AcksPolicy::All => {
                    // Need to be leader, but no ISR check needed
                    self.get_role(topic, partition_id).await == ReplicaRole::Leader
                }
            };
        }

        // Standard replication check
        self.can_accept_write(topic, partition_id, acks).await
    }

    /// Advance HWM for a diskless partition
    ///
    /// For diskless topics, HWM = LEO immediately after write
    /// since S3 provides durability.
    pub async fn advance_hwm_diskless(&self, topic: &str, partition_id: i32) -> Option<i64> {
        if !self.is_diskless(topic, partition_id).await {
            return None;
        }

        let watermarks = self.watermark_manager.get(topic, partition_id).await?;
        let leo = watermarks.log_end_offset();
        watermarks.set_high_watermark(leo);
        Some(leo)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_replication_manager_creation() {
        let config = ReplicationConfig::default();
        let mgr = ReplicationManager::new(1, config);
        assert_eq!(mgr.node_id(), 1);
    }

    #[tokio::test]
    async fn test_become_leader() {
        let config = ReplicationConfig::default();
        let mgr = ReplicationManager::new(1, config);

        mgr.become_leader("test", 0, 1, vec![1, 2, 3], vec![1, 2, 3], 100)
            .await;

        assert_eq!(mgr.get_role("test", 0).await, ReplicaRole::Leader);
        assert!(mgr.replica_sender.is_tracking("test", 0).await);
        assert!(mgr.isr_manager.get_partition("test", 0).await.is_some());

        // Watermarks should be initialized
        let hwm = mgr.get_high_watermark("test", 0).await;
        let leo = mgr.get_log_end_offset("test", 0).await;
        assert!(hwm.is_some());
        assert_eq!(leo, Some(100));
    }

    #[tokio::test]
    async fn test_become_follower() {
        let config = ReplicationConfig::default();
        let mgr = ReplicationManager::new(1, config);

        mgr.become_follower("test", 0, 2, 1, 50).await;

        assert_eq!(mgr.get_role("test", 0).await, ReplicaRole::Follower);

        let state = mgr
            .replica_fetcher
            .get_partition_state("test", 0)
            .await
            .unwrap();
        assert_eq!(state.leader_id, Some(2));
        assert_eq!(state.fetch_offset, 50);
    }

    #[tokio::test]
    async fn test_leader_to_follower_transition() {
        let config = ReplicationConfig::default();
        let mgr = ReplicationManager::new(1, config);

        // Start as leader
        mgr.become_leader("test", 0, 1, vec![1, 2], vec![1, 2], 100)
            .await;
        assert_eq!(mgr.get_role("test", 0).await, ReplicaRole::Leader);

        // Become follower
        mgr.become_follower("test", 0, 2, 2, 100).await;
        assert_eq!(mgr.get_role("test", 0).await, ReplicaRole::Follower);

        // Should no longer be tracked as leader
        assert!(!mgr.replica_sender.is_tracking("test", 0).await);
        assert!(mgr.isr_manager.get_partition("test", 0).await.is_none());
    }

    #[tokio::test]
    async fn test_stop_partition() {
        let config = ReplicationConfig::default();
        let mgr = ReplicationManager::new(1, config);

        mgr.become_leader("test", 0, 1, vec![1], vec![1], 100).await;
        assert_eq!(mgr.get_role("test", 0).await, ReplicaRole::Leader);

        mgr.stop_partition("test", 0).await;
        assert_eq!(mgr.get_role("test", 0).await, ReplicaRole::None);
    }

    #[tokio::test]
    async fn test_can_accept_write() {
        let config = ReplicationConfig::default();
        let mgr = ReplicationManager::new(1, config);

        // Not a replica - can accept acks=0
        assert!(mgr.can_accept_write("test", 0, AcksPolicy::None).await);
        assert!(!mgr.can_accept_write("test", 0, AcksPolicy::Leader).await);
        assert!(!mgr.can_accept_write("test", 0, AcksPolicy::All).await);

        // As leader
        mgr.become_leader("test", 0, 1, vec![1, 2], vec![1, 2], 100)
            .await;
        assert!(mgr.can_accept_write("test", 0, AcksPolicy::None).await);
        assert!(mgr.can_accept_write("test", 0, AcksPolicy::Leader).await);
        assert!(mgr.can_accept_write("test", 0, AcksPolicy::All).await);

        // As follower
        mgr.become_follower("test", 0, 2, 2, 100).await;
        assert!(mgr.can_accept_write("test", 0, AcksPolicy::None).await);
        assert!(!mgr.can_accept_write("test", 0, AcksPolicy::Leader).await);
        assert!(!mgr.can_accept_write("test", 0, AcksPolicy::All).await);
    }

    #[tokio::test]
    async fn test_update_follower_progress_and_hwm() {
        let config = ReplicationConfig::default();
        let mgr = ReplicationManager::new(1, config);

        // Become leader with replicas 1, 2, 3
        mgr.become_leader("test", 0, 1, vec![1, 2, 3], vec![1, 2, 3], 100)
            .await;

        // Update follower progress
        mgr.update_follower_progress("test", 0, 2, 90, 100).await;
        mgr.update_follower_progress("test", 0, 3, 80, 100).await;

        // Try to advance HWM
        let new_hwm = mgr.try_advance_hwm("test", 0).await;
        // HWM should be min(100, 90, 80) = 80
        assert_eq!(new_hwm, Some(80));
    }

    #[tokio::test]
    async fn test_single_node_isr_hwm() {
        let config = ReplicationConfig::default();
        let mgr = ReplicationManager::new(1, config);

        // Single node ISR
        mgr.become_leader("test", 0, 1, vec![1], vec![1], 100).await;

        // HWM should equal LEO for single-node ISR
        let hwm = mgr.get_high_watermark("test", 0).await;
        assert_eq!(hwm, Some(100));
    }

    // ============ Storage Mode Aware Tests ============

    #[tokio::test]
    async fn test_diskless_leader_no_replication() {
        let config = ReplicationConfig::default();
        let mgr = ReplicationManager::new(1, config);

        // Become diskless leader
        mgr.become_leader_with_mode(
            "test",
            0,
            1,
            vec![1, 2, 3],
            vec![1, 2, 3],
            100,
            StorageMode::Diskless,
        )
        .await;

        assert_eq!(mgr.get_role("test", 0).await, ReplicaRole::Leader);

        // For diskless, HWM = LEO immediately (no ISR wait)
        let hwm = mgr.get_high_watermark("test", 0).await;
        assert_eq!(hwm, Some(100));

        // No ISR tracking for diskless
        assert!(mgr.isr_manager.get_partition("test", 0).await.is_none());

        // No replica sending for diskless
        assert!(!mgr.replica_sender.is_tracking("test", 0).await);
    }

    #[tokio::test]
    async fn test_diskless_follower_no_fetching() {
        let config = ReplicationConfig::default();
        let mgr = ReplicationManager::new(1, config);

        // Become diskless follower
        mgr.become_follower_with_mode("test", 0, 2, 1, 50, StorageMode::Diskless)
            .await;

        // Should still have watermarks (for consumer coordination)
        let leo = mgr.get_log_end_offset("test", 0).await;
        assert_eq!(leo, Some(50));

        // HWM = LEO for diskless
        let hwm = mgr.get_high_watermark("test", 0).await;
        assert_eq!(hwm, Some(50));

        // No replica fetching for diskless
        let state = mgr.replica_fetcher.get_partition_state("test", 0).await;
        assert!(state.is_none());
    }

    #[tokio::test]
    async fn test_local_mode_has_replication() {
        let config = ReplicationConfig::default();
        let mgr = ReplicationManager::new(1, config);

        // Become local leader
        mgr.become_leader_with_mode(
            "test",
            0,
            1,
            vec![1, 2, 3],
            vec![1, 2, 3],
            100,
            StorageMode::Local,
        )
        .await;

        // Local mode should have ISR tracking
        assert!(mgr.isr_manager.get_partition("test", 0).await.is_some());

        // Should have replica sending
        assert!(mgr.replica_sender.is_tracking("test", 0).await);

        // HWM should not immediately equal LEO (needs ISR ack)
        // With initial ISR of all replicas, needs follower progress
        assert!(mgr.requires_replication("test", 0).await);
    }

    #[tokio::test]
    async fn test_diskless_can_accept_write_acks_all() {
        let config = ReplicationConfig::default();
        let mgr = ReplicationManager::new(1, config);

        // Become diskless leader
        mgr.become_leader_with_mode("test", 0, 1, vec![1], vec![1], 100, StorageMode::Diskless)
            .await;

        // For diskless, acks=all should succeed immediately (no ISR needed)
        assert!(
            mgr.can_accept_write_with_mode("test", 0, AcksPolicy::All)
                .await
        );
        assert!(
            mgr.can_accept_write_with_mode("test", 0, AcksPolicy::Leader)
                .await
        );
        assert!(
            mgr.can_accept_write_with_mode("test", 0, AcksPolicy::None)
                .await
        );
    }

    #[tokio::test]
    async fn test_storage_mode_tracking() {
        let config = ReplicationConfig::default();
        let mgr = ReplicationManager::new(1, config);

        // Default should be Local
        assert_eq!(
            mgr.get_storage_mode("test", 0).await,
            StorageMode::default()
        );

        // Set diskless
        mgr.set_storage_mode("test", 0, StorageMode::Diskless).await;
        assert!(mgr.is_diskless("test", 0).await);
        assert!(!mgr.requires_replication("test", 0).await);

        // Set hybrid
        mgr.set_storage_mode("test", 0, StorageMode::Hybrid).await;
        assert!(!mgr.is_diskless("test", 0).await);
        assert!(mgr.requires_replication("test", 0).await);

        // Set local
        mgr.set_storage_mode("test", 0, StorageMode::Local).await;
        assert!(mgr.requires_replication("test", 0).await);
    }

    #[tokio::test]
    async fn test_advance_hwm_diskless() {
        let config = ReplicationConfig::default();
        let mgr = ReplicationManager::new(1, config);

        // Become diskless leader
        mgr.become_leader_with_mode("test", 0, 1, vec![1], vec![1], 100, StorageMode::Diskless)
            .await;

        // Simulate write advancing LEO
        mgr.set_log_end_offset("test", 0, 150).await;

        // Advance HWM for diskless
        let new_hwm = mgr.advance_hwm_diskless("test", 0).await;
        assert_eq!(new_hwm, Some(150));

        // Verify HWM was updated
        let hwm = mgr.get_high_watermark("test", 0).await;
        assert_eq!(hwm, Some(150));
    }

    #[tokio::test]
    async fn test_advance_hwm_diskless_only() {
        let config = ReplicationConfig::default();
        let mgr = ReplicationManager::new(1, config);

        // Become local leader
        mgr.become_leader_with_mode("test", 0, 1, vec![1], vec![1], 100, StorageMode::Local)
            .await;

        // advance_hwm_diskless should return None for non-diskless
        let result = mgr.advance_hwm_diskless("test", 0).await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_stop_partition_cleans_storage_mode() {
        let config = ReplicationConfig::default();
        let mgr = ReplicationManager::new(1, config);

        // Set storage mode
        mgr.set_storage_mode("test", 0, StorageMode::Diskless).await;
        assert!(mgr.is_diskless("test", 0).await);

        // Stop partition with mode cleanup
        mgr.stop_partition_with_mode("test", 0).await;

        // Should revert to default
        assert_eq!(
            mgr.get_storage_mode("test", 0).await,
            StorageMode::default()
        );
    }
}
