//! Edge sync checkpoint management
//!
//! Tracks synchronization progress for resumable syncing.

use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

/// Checkpoint for a single partition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionCheckpoint {
    /// Last synced offset
    pub last_offset: i64,
    /// Last sync timestamp
    pub last_sync_at: i64,
    /// Number of records synced at this checkpoint
    pub records_synced: u64,
    /// CRC32 of last synced record (for verification)
    pub last_crc: Option<u32>,
}

impl Default for PartitionCheckpoint {
    fn default() -> Self {
        Self {
            last_offset: -1,
            last_sync_at: 0,
            records_synced: 0,
            last_crc: None,
        }
    }
}

/// Topic-level checkpoint containing all partition checkpoints
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TopicCheckpoint {
    /// Partition checkpoints
    pub partitions: HashMap<i32, PartitionCheckpoint>,
    /// Topic metadata version (for schema evolution)
    pub metadata_version: u64,
    /// Last known partition count
    pub partition_count: i32,
}

/// Full sync checkpoint state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointState {
    /// Version of checkpoint format
    pub version: u16,
    /// Edge ID
    pub edge_id: String,
    /// Topic checkpoints
    pub topics: HashMap<String, TopicCheckpoint>,
    /// Global sync sequence number
    pub sync_sequence: u64,
    /// Last checkpoint timestamp
    pub checkpoint_at: i64,
    /// CRC32 of checkpoint data (for integrity)
    pub crc32: u32,
}

impl Default for CheckpointState {
    fn default() -> Self {
        Self {
            version: 1,
            edge_id: String::new(),
            topics: HashMap::new(),
            sync_sequence: 0,
            checkpoint_at: 0,
            crc32: 0,
        }
    }
}

/// Edge sync checkpoint manager
pub struct EdgeSyncCheckpoint {
    path: PathBuf,
    state: parking_lot::RwLock<CheckpointState>,
    dirty: std::sync::atomic::AtomicBool,
}

impl EdgeSyncCheckpoint {
    /// Create a new checkpoint manager
    pub fn new(path: PathBuf) -> Result<Self> {
        let state = if path.exists() {
            Self::load_from_file(&path)?
        } else {
            CheckpointState::default()
        };

        Ok(Self {
            path,
            state: parking_lot::RwLock::new(state),
            dirty: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Create an in-memory checkpoint (no persistence)
    pub fn in_memory() -> Self {
        Self {
            path: PathBuf::new(),
            state: parking_lot::RwLock::new(CheckpointState::default()),
            dirty: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Load checkpoint from file
    fn load_from_file(path: &PathBuf) -> Result<CheckpointState> {
        let content = std::fs::read_to_string(path).map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to read checkpoint: {}", e))
        })?;

        let state: CheckpointState = serde_json::from_str(&content).map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to parse checkpoint: {}", e))
        })?;

        // Verify CRC
        let computed_crc = Self::compute_crc(&state);
        if computed_crc != state.crc32 {
            return Err(StreamlineError::storage_msg(
                "Checkpoint CRC mismatch".to_string(),
            ));
        }

        Ok(state)
    }

    /// Save checkpoint to file
    pub fn save(&self) -> Result<()> {
        if !self.dirty.load(std::sync::atomic::Ordering::SeqCst) {
            return Ok(());
        }

        if self.path.as_os_str().is_empty() {
            // In-memory mode, nothing to save
            return Ok(());
        }

        let mut state = self.state.write();
        state.checkpoint_at = chrono::Utc::now().timestamp_millis();
        state.crc32 = Self::compute_crc(&state);

        let content = serde_json::to_string_pretty(&*state).map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to serialize checkpoint: {}", e))
        })?;

        // Write to temp file first, then rename for atomicity
        let temp_path = self.path.with_extension("tmp");
        std::fs::write(&temp_path, &content).map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to write checkpoint: {}", e))
        })?;

        std::fs::rename(&temp_path, &self.path).map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to rename checkpoint: {}", e))
        })?;

        self.dirty.store(false, std::sync::atomic::Ordering::SeqCst);

        Ok(())
    }

    /// Compute CRC32 for checkpoint state
    fn compute_crc(state: &CheckpointState) -> u32 {
        // Create a copy without crc32 for computation
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(state.version.to_le_bytes().as_ref());
        hasher.update(state.edge_id.as_bytes());
        hasher.update(state.sync_sequence.to_le_bytes().as_ref());
        hasher.update(state.checkpoint_at.to_le_bytes().as_ref());

        // Hash topic data - sort keys for deterministic order
        let mut topics: Vec<_> = state.topics.iter().collect();
        topics.sort_by_key(|(k, _)| *k);
        for (topic, tc) in topics {
            hasher.update(topic.as_bytes());
            let mut partitions: Vec<_> = tc.partitions.iter().collect();
            partitions.sort_by_key(|(k, _)| *k);
            for (partition, pc) in partitions {
                hasher.update(partition.to_le_bytes().as_ref());
                hasher.update(pc.last_offset.to_le_bytes().as_ref());
            }
        }

        hasher.finalize()
    }

    /// Get the last synced offset for a partition
    pub fn get_offset(&self, topic: &str, partition: i32) -> i64 {
        self.state
            .read()
            .topics
            .get(topic)
            .and_then(|t| t.partitions.get(&partition))
            .map(|p| p.last_offset)
            .unwrap_or(-1)
    }

    /// Update checkpoint for a partition
    pub fn update(&self, topic: &str, partition: i32, offset: i64) -> Result<()> {
        self.update_with_crc(topic, partition, offset, None)
    }

    /// Update checkpoint with CRC verification
    pub fn update_with_crc(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        crc: Option<u32>,
    ) -> Result<()> {
        let mut state = self.state.write();

        let topic_checkpoint = state.topics.entry(topic.to_string()).or_default();
        let partition_checkpoint = topic_checkpoint.partitions.entry(partition).or_default();

        if offset > partition_checkpoint.last_offset {
            partition_checkpoint.last_offset = offset;
            partition_checkpoint.last_sync_at = chrono::Utc::now().timestamp_millis();
            partition_checkpoint.records_synced += 1;
            partition_checkpoint.last_crc = crc;

            state.sync_sequence += 1;

            self.dirty.store(true, std::sync::atomic::Ordering::SeqCst);
        }

        Ok(())
    }

    /// Get checkpoint for a partition
    pub fn get_partition_checkpoint(
        &self,
        topic: &str,
        partition: i32,
    ) -> Option<PartitionCheckpoint> {
        self.state
            .read()
            .topics
            .get(topic)
            .and_then(|t| t.partitions.get(&partition).cloned())
    }

    /// Get checkpoint for a topic
    pub fn get_topic_checkpoint(&self, topic: &str) -> Option<TopicCheckpoint> {
        self.state.read().topics.get(topic).cloned()
    }

    /// Get all checkpoints
    pub fn get_all(&self) -> CheckpointState {
        self.state.read().clone()
    }

    /// Set edge ID
    pub fn set_edge_id(&self, edge_id: impl Into<String>) {
        self.state.write().edge_id = edge_id.into();
        self.dirty.store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Get edge ID
    pub fn edge_id(&self) -> String {
        self.state.read().edge_id.clone()
    }

    /// Get sync sequence number
    pub fn sync_sequence(&self) -> u64 {
        self.state.read().sync_sequence
    }

    /// Reset checkpoint for a topic
    pub fn reset_topic(&self, topic: &str) {
        self.state.write().topics.remove(topic);
        self.dirty.store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Reset checkpoint for a partition
    pub fn reset_partition(&self, topic: &str, partition: i32) {
        if let Some(topic_cp) = self.state.write().topics.get_mut(topic) {
            topic_cp.partitions.remove(&partition);
        }
        self.dirty.store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Reset all checkpoints
    pub fn reset_all(&self) {
        let mut state = self.state.write();
        state.topics.clear();
        state.sync_sequence = 0;
        self.dirty.store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Get sync progress for a topic as percentage
    pub fn progress(&self, topic: &str, total_offsets: &HashMap<i32, i64>) -> f32 {
        let state = self.state.read();

        if let Some(topic_cp) = state.topics.get(topic) {
            let mut synced = 0i64;
            let mut total = 0i64;

            for (partition, max_offset) in total_offsets {
                total += max_offset;
                if let Some(pc) = topic_cp.partitions.get(partition) {
                    synced += pc.last_offset.max(0);
                }
            }

            if total > 0 {
                (synced as f32 / total as f32) * 100.0
            } else {
                100.0
            }
        } else {
            0.0
        }
    }

    /// Check if checkpoint is dirty (has unsaved changes)
    pub fn is_dirty(&self) -> bool {
        self.dirty.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_checkpoint_in_memory() {
        let checkpoint = EdgeSyncCheckpoint::in_memory();

        // Initial state
        assert_eq!(checkpoint.get_offset("topic1", 0), -1);
        assert_eq!(checkpoint.sync_sequence(), 0);

        // Update
        checkpoint.update("topic1", 0, 100).unwrap();
        assert_eq!(checkpoint.get_offset("topic1", 0), 100);
        assert_eq!(checkpoint.sync_sequence(), 1);

        // Update same partition
        checkpoint.update("topic1", 0, 200).unwrap();
        assert_eq!(checkpoint.get_offset("topic1", 0), 200);
        assert_eq!(checkpoint.sync_sequence(), 2);
    }

    #[test]
    fn test_checkpoint_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let checkpoint_path = temp_dir.path().join("checkpoint.json");

        // Create and update
        {
            let checkpoint = EdgeSyncCheckpoint::new(checkpoint_path.clone()).unwrap();
            checkpoint.set_edge_id("edge-123");
            checkpoint.update("topic1", 0, 100).unwrap();
            checkpoint.update("topic1", 1, 50).unwrap();
            checkpoint.save().unwrap();
        }

        // Reload and verify
        {
            let checkpoint = EdgeSyncCheckpoint::new(checkpoint_path).unwrap();
            assert_eq!(checkpoint.edge_id(), "edge-123");
            assert_eq!(checkpoint.get_offset("topic1", 0), 100);
            assert_eq!(checkpoint.get_offset("topic1", 1), 50);
            assert_eq!(checkpoint.sync_sequence(), 2);
        }
    }

    #[test]
    fn test_checkpoint_reset() {
        let checkpoint = EdgeSyncCheckpoint::in_memory();

        checkpoint.update("topic1", 0, 100).unwrap();
        checkpoint.update("topic1", 1, 200).unwrap();
        checkpoint.update("topic2", 0, 50).unwrap();

        // Reset partition
        checkpoint.reset_partition("topic1", 0);
        assert_eq!(checkpoint.get_offset("topic1", 0), -1);
        assert_eq!(checkpoint.get_offset("topic1", 1), 200);

        // Reset topic
        checkpoint.reset_topic("topic1");
        assert_eq!(checkpoint.get_offset("topic1", 1), -1);
        assert_eq!(checkpoint.get_offset("topic2", 0), 50);

        // Reset all
        checkpoint.reset_all();
        assert_eq!(checkpoint.get_offset("topic2", 0), -1);
        assert_eq!(checkpoint.sync_sequence(), 0);
    }

    #[test]
    fn test_checkpoint_progress() {
        let checkpoint = EdgeSyncCheckpoint::in_memory();

        checkpoint.update("topic1", 0, 50).unwrap();
        checkpoint.update("topic1", 1, 75).unwrap();

        let mut total_offsets = HashMap::new();
        total_offsets.insert(0, 100);
        total_offsets.insert(1, 100);

        let progress = checkpoint.progress("topic1", &total_offsets);
        assert!((progress - 62.5).abs() < 0.1); // (50 + 75) / 200 * 100 = 62.5%
    }

    #[test]
    fn test_partition_checkpoint_details() {
        let checkpoint = EdgeSyncCheckpoint::in_memory();

        checkpoint
            .update_with_crc("topic1", 0, 100, Some(12345))
            .unwrap();

        let pc = checkpoint.get_partition_checkpoint("topic1", 0).unwrap();
        assert_eq!(pc.last_offset, 100);
        assert_eq!(pc.last_crc, Some(12345));
        assert_eq!(pc.records_synced, 1);
        assert!(pc.last_sync_at > 0);
    }

    #[test]
    fn test_checkpoint_no_regression() {
        let checkpoint = EdgeSyncCheckpoint::in_memory();

        checkpoint.update("topic1", 0, 100).unwrap();
        checkpoint.update("topic1", 0, 50).unwrap(); // Should not regress

        assert_eq!(checkpoint.get_offset("topic1", 0), 100);
        assert_eq!(checkpoint.sync_sequence(), 1); // Only 1 update counted
    }
}
