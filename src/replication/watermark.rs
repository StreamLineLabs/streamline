// Watermark management scaffolding for replication feature (not yet fully integrated)
#![allow(dead_code)]

//! High watermark management for partition replication
//!
//! The high watermark (HWM) is the offset up to which all in-sync replicas
//! have successfully replicated. Consumers can only read up to the HWM
//! to ensure they only see committed (durable) data.
//!
//! Key concepts:
//! - Log End Offset (LEO): The offset of the next record to be written
//! - High Watermark (HWM): The offset up to which data is committed
//! - HWM <= min(LEO of all ISR members)

use crate::cluster::node::NodeId;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Type alias for partition watermarks map
type PartitionWatermarksMap = HashMap<(String, i32), Arc<PartitionWatermarks>>;

/// Tracks watermarks for a single partition
#[derive(Debug)]
pub(crate) struct PartitionWatermarks {
    /// Topic name
    topic: String,

    /// Partition ID
    partition_id: i32,

    /// High watermark - offset up to which data is committed
    /// Only set by the leader based on ISR acknowledgments
    high_watermark: AtomicI64,

    /// Log end offset - offset of next record to be written
    log_end_offset: AtomicI64,

    /// Follower LEOs tracked by the leader
    /// Key: replica node ID, Value: that replica's LEO
    follower_leos: Arc<RwLock<HashMap<NodeId, i64>>>,
}

impl PartitionWatermarks {
    /// Create new watermark tracker for a partition
    pub fn new(topic: String, partition_id: i32) -> Self {
        Self {
            topic,
            partition_id,
            high_watermark: AtomicI64::new(0),
            log_end_offset: AtomicI64::new(0),
            follower_leos: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get the topic name
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Get the partition ID
    pub fn partition_id(&self) -> i32 {
        self.partition_id
    }

    /// Get current high watermark
    pub fn high_watermark(&self) -> i64 {
        self.high_watermark.load(Ordering::Acquire)
    }

    /// Get current log end offset
    pub fn log_end_offset(&self) -> i64 {
        self.log_end_offset.load(Ordering::Acquire)
    }

    /// Set the log end offset (after appending records)
    pub fn set_log_end_offset(&self, offset: i64) {
        self.log_end_offset.store(offset, Ordering::Release);
    }

    /// Update follower's LEO (called by leader when receiving fetch response)
    pub async fn update_follower_leo(&self, follower_id: NodeId, leo: i64) {
        let mut leos = self.follower_leos.write().await;
        leos.insert(follower_id, leo);
    }

    /// Remove a follower from tracking (when removed from ISR)
    pub async fn remove_follower(&self, follower_id: NodeId) {
        let mut leos = self.follower_leos.write().await;
        leos.remove(&follower_id);
    }

    /// Calculate new high watermark based on ISR LEOs
    /// Returns the new HWM if it advanced, None otherwise
    pub async fn calculate_high_watermark(
        &self,
        isr: &[NodeId],
        local_node_id: NodeId,
    ) -> Option<i64> {
        let leos = self.follower_leos.read().await;

        // Collect LEOs for all ISR members
        let mut isr_leos: Vec<i64> = Vec::with_capacity(isr.len());

        for &replica_id in isr {
            if replica_id == local_node_id {
                // Leader's own LEO
                isr_leos.push(self.log_end_offset());
            } else if let Some(&leo) = leos.get(&replica_id) {
                isr_leos.push(leo);
            } else {
                // If we don't have LEO for an ISR member, we can't advance HWM
                // This shouldn't happen in normal operation
                return None;
            }
        }

        if isr_leos.is_empty() {
            return None;
        }

        // HWM is the minimum LEO across all ISR members
        // Use match instead of unwrap for safety (even though we checked is_empty)
        let new_hwm = match isr_leos.iter().min() {
            Some(&hwm) => hwm,
            None => return None, // Should not happen after is_empty check, but be defensive
        };

        // Use compare-and-swap to ensure atomic, monotonic HWM advancement
        // This prevents race conditions where concurrent updates could cause
        // the HWM to regress if multiple threads calculate different values
        loop {
            let current_hwm = self.high_watermark.load(Ordering::Acquire);
            if new_hwm <= current_hwm {
                // HWM would not advance, no update needed
                return None;
            }

            // Try to atomically update HWM only if it hasn't changed
            match self.high_watermark.compare_exchange_weak(
                current_hwm,
                new_hwm,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Some(new_hwm),
                Err(_) => {
                    // Another thread updated HWM, retry with new value
                    continue;
                }
            }
        }
    }

    /// Set high watermark directly (used by followers receiving from leader)
    /// Uses compare-and-swap to ensure atomic, monotonic advancement
    pub fn set_high_watermark(&self, hwm: i64) {
        // Use CAS loop to ensure HWM only advances, never regresses
        loop {
            let current = self.high_watermark.load(Ordering::Acquire);
            if hwm <= current {
                // HWM would not advance, no update needed
                return;
            }

            match self.high_watermark.compare_exchange_weak(
                current,
                hwm,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(_) => continue, // Retry if another thread updated
            }
        }
    }

    /// Get all tracked follower LEOs
    pub async fn get_follower_leos(&self) -> HashMap<NodeId, i64> {
        self.follower_leos.read().await.clone()
    }

    /// Check if a given offset is committed (at or below HWM)
    pub fn is_committed(&self, offset: i64) -> bool {
        offset < self.high_watermark()
    }

    /// Get the lag between LEO and HWM (uncommitted records)
    pub fn uncommitted_count(&self) -> i64 {
        let leo = self.log_end_offset();
        let hwm = self.high_watermark();
        (leo - hwm).max(0)
    }
}

/// Manages watermarks for all partitions on this node
#[derive(Debug)]
pub struct WatermarkManager {
    /// Watermarks by (topic, partition)
    partitions: Arc<RwLock<PartitionWatermarksMap>>,
}

impl Default for WatermarkManager {
    fn default() -> Self {
        Self::new()
    }
}

impl WatermarkManager {
    /// Create a new watermark manager
    pub fn new() -> Self {
        Self {
            partitions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get or create watermark tracker for a partition
    pub(crate) async fn get_or_create(
        &self,
        topic: &str,
        partition_id: i32,
    ) -> Arc<PartitionWatermarks> {
        let key = (topic.to_string(), partition_id);

        // Check if exists first
        {
            let partitions = self.partitions.read().await;
            if let Some(watermarks) = partitions.get(&key) {
                return watermarks.clone();
            }
        }

        // Create new tracker
        let mut partitions = self.partitions.write().await;
        partitions
            .entry(key)
            .or_insert_with(|| Arc::new(PartitionWatermarks::new(topic.to_string(), partition_id)))
            .clone()
    }

    /// Get watermark tracker for a partition (if exists)
    pub(crate) async fn get(
        &self,
        topic: &str,
        partition_id: i32,
    ) -> Option<Arc<PartitionWatermarks>> {
        let partitions = self.partitions.read().await;
        partitions.get(&(topic.to_string(), partition_id)).cloned()
    }

    /// Remove watermark tracker for a partition
    pub async fn remove(&self, topic: &str, partition_id: i32) {
        let mut partitions = self.partitions.write().await;
        partitions.remove(&(topic.to_string(), partition_id));
    }

    /// Get all tracked partitions
    pub async fn list_partitions(&self) -> Vec<(String, i32)> {
        let partitions = self.partitions.read().await;
        partitions.keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_watermarks_creation() {
        let wm = PartitionWatermarks::new("test-topic".to_string(), 0);
        assert_eq!(wm.topic(), "test-topic");
        assert_eq!(wm.partition_id(), 0);
        assert_eq!(wm.high_watermark(), 0);
        assert_eq!(wm.log_end_offset(), 0);
    }

    #[test]
    fn test_set_log_end_offset() {
        let wm = PartitionWatermarks::new("test".to_string(), 0);
        wm.set_log_end_offset(100);
        assert_eq!(wm.log_end_offset(), 100);
    }

    #[test]
    fn test_set_high_watermark() {
        let wm = PartitionWatermarks::new("test".to_string(), 0);
        wm.set_high_watermark(50);
        assert_eq!(wm.high_watermark(), 50);

        // HWM should not go backwards
        wm.set_high_watermark(30);
        assert_eq!(wm.high_watermark(), 50);

        // But should advance
        wm.set_high_watermark(60);
        assert_eq!(wm.high_watermark(), 60);
    }

    #[tokio::test]
    async fn test_follower_leo_tracking() {
        let wm = PartitionWatermarks::new("test".to_string(), 0);

        wm.update_follower_leo(2, 100).await;
        wm.update_follower_leo(3, 80).await;

        let leos = wm.get_follower_leos().await;
        assert_eq!(leos.get(&2), Some(&100));
        assert_eq!(leos.get(&3), Some(&80));

        wm.remove_follower(2).await;
        let leos = wm.get_follower_leos().await;
        assert!(!leos.contains_key(&2));
        assert_eq!(leos.get(&3), Some(&80));
    }

    #[tokio::test]
    async fn test_calculate_high_watermark() {
        let wm = PartitionWatermarks::new("test".to_string(), 0);
        let local_node_id: NodeId = 1;

        // Set leader's LEO
        wm.set_log_end_offset(100);

        // Set follower LEOs
        wm.update_follower_leo(2, 90).await;
        wm.update_follower_leo(3, 80).await;

        // ISR includes leader (1) and both followers
        let isr = vec![1, 2, 3];
        let new_hwm = wm.calculate_high_watermark(&isr, local_node_id).await;

        // HWM should be min(100, 90, 80) = 80
        assert_eq!(new_hwm, Some(80));
        assert_eq!(wm.high_watermark(), 80);

        // Update follower 3's LEO
        wm.update_follower_leo(3, 95).await;
        let new_hwm = wm.calculate_high_watermark(&isr, local_node_id).await;

        // HWM should now be min(100, 90, 95) = 90
        assert_eq!(new_hwm, Some(90));
        assert_eq!(wm.high_watermark(), 90);
    }

    #[tokio::test]
    async fn test_calculate_hwm_single_node() {
        let wm = PartitionWatermarks::new("test".to_string(), 0);
        let local_node_id: NodeId = 1;

        wm.set_log_end_offset(100);

        // Single node ISR (leader only)
        let isr = vec![1];
        let new_hwm = wm.calculate_high_watermark(&isr, local_node_id).await;

        // HWM should equal LEO when leader is only ISR member
        assert_eq!(new_hwm, Some(100));
    }

    #[test]
    fn test_is_committed() {
        let wm = PartitionWatermarks::new("test".to_string(), 0);
        wm.set_high_watermark(50);

        assert!(wm.is_committed(0));
        assert!(wm.is_committed(49));
        assert!(!wm.is_committed(50)); // HWM offset itself is not committed
        assert!(!wm.is_committed(100));
    }

    #[test]
    fn test_uncommitted_count() {
        let wm = PartitionWatermarks::new("test".to_string(), 0);
        wm.set_log_end_offset(100);
        wm.set_high_watermark(80);

        assert_eq!(wm.uncommitted_count(), 20);
    }

    #[tokio::test]
    async fn test_watermark_manager() {
        let mgr = WatermarkManager::new();

        // Get or create
        let wm1 = mgr.get_or_create("topic1", 0).await;
        wm1.set_log_end_offset(100);

        // Get same partition again
        let wm1_again = mgr.get_or_create("topic1", 0).await;
        assert_eq!(wm1_again.log_end_offset(), 100);

        // Create different partition
        let wm2 = mgr.get_or_create("topic1", 1).await;
        wm2.set_log_end_offset(200);

        // List partitions
        let partitions = mgr.list_partitions().await;
        assert_eq!(partitions.len(), 2);

        // Remove partition
        mgr.remove("topic1", 0).await;
        assert!(mgr.get("topic1", 0).await.is_none());
        assert!(mgr.get("topic1", 1).await.is_some());
    }
}
