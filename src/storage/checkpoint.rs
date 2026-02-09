//! Checkpoint management for WAL recovery
//!
//! Checkpoints track which WAL entries have been successfully applied to segments,
//! enabling efficient crash recovery by only replaying uncommitted entries.
//!
//! ## Async Support
//!
//! This module provides both synchronous and asynchronous methods for checkpoint
//! operations. Use the `*_async` variants in async contexts to avoid blocking
//! the tokio runtime.

use crate::error::{Result, StreamlineError};
use crate::storage::async_io;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use tracing::{debug, info, warn};

/// Checkpoint state for a single partition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionCheckpoint {
    /// Last applied offset in the partition
    pub last_offset: i64,

    /// Last applied WAL sequence number for this partition
    pub last_wal_sequence: u64,

    /// Timestamp of last checkpoint
    pub timestamp: i64,
}

/// Global checkpoint state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Checkpoint {
    /// Last global WAL sequence that was checkpointed
    pub last_sequence: u64,

    /// Per-partition checkpoint state: (topic, partition) -> PartitionCheckpoint
    pub partitions: HashMap<String, HashMap<i32, PartitionCheckpoint>>,

    /// Timestamp of this checkpoint
    pub timestamp: i64,

    /// Version of checkpoint format
    pub version: u16,

    /// CRC32 checksum of checkpoint data (excluding this field)
    #[serde(default)]
    pub crc32: u32,
}

impl Default for Checkpoint {
    fn default() -> Self {
        Self {
            last_sequence: 0,
            partitions: HashMap::new(),
            timestamp: chrono::Utc::now().timestamp_millis(),
            version: 1,
            crc32: 0,
        }
    }
}

impl Checkpoint {
    /// Create a new empty checkpoint
    pub fn new() -> Self {
        Self::default()
    }

    /// Update checkpoint for a partition
    pub fn update_partition(
        &mut self,
        topic: &str,
        partition: i32,
        last_offset: i64,
        wal_sequence: u64,
    ) {
        let topic_map = self.partitions.entry(topic.to_string()).or_default();
        topic_map.insert(
            partition,
            PartitionCheckpoint {
                last_offset,
                last_wal_sequence: wal_sequence,
                timestamp: chrono::Utc::now().timestamp_millis(),
            },
        );

        // Update global sequence if this is newer
        if wal_sequence > self.last_sequence {
            self.last_sequence = wal_sequence;
        }

        self.timestamp = chrono::Utc::now().timestamp_millis();
    }

    /// Get checkpoint for a partition
    pub fn get_partition(&self, topic: &str, partition: i32) -> Option<&PartitionCheckpoint> {
        self.partitions.get(topic).and_then(|m| m.get(&partition))
    }

    /// Set the global last sequence (for checkpoint entries)
    pub fn set_last_sequence(&mut self, sequence: u64) {
        self.last_sequence = sequence;
        self.timestamp = chrono::Utc::now().timestamp_millis();
    }
}

/// Checkpoint manager for persisting and loading checkpoint state
pub struct CheckpointManager {
    /// Path to checkpoint file
    checkpoint_path: PathBuf,

    /// Current checkpoint state
    checkpoint: Checkpoint,
}

impl CheckpointManager {
    /// Create a new checkpoint manager
    pub fn new(data_dir: &Path) -> Result<Self> {
        let checkpoint_path = data_dir.join("wal").join("checkpoint.json");

        // Create wal directory if needed
        if let Some(parent) = checkpoint_path.parent() {
            fs::create_dir_all(parent)?;
        }

        // Load existing checkpoint or create new one
        let checkpoint = if checkpoint_path.exists() {
            Self::load_checkpoint(&checkpoint_path)?
        } else {
            Checkpoint::new()
        };

        info!(
            checkpoint_path = %checkpoint_path.display(),
            last_sequence = checkpoint.last_sequence,
            "Checkpoint manager initialized"
        );

        Ok(Self {
            checkpoint_path,
            checkpoint,
        })
    }

    /// Load checkpoint from disk
    fn load_checkpoint(path: &Path) -> Result<Checkpoint> {
        let data = fs::read_to_string(path)?;
        let checkpoint: Checkpoint = serde_json::from_str(&data).map_err(|e| {
            StreamlineError::CorruptedData(format!("Failed to parse checkpoint: {}", e))
        })?;

        // Verify CRC32 if present (version 1+ includes CRC)
        if checkpoint.version >= 1 && checkpoint.crc32 != 0 {
            let computed_crc = Self::compute_checkpoint_crc(&checkpoint);
            if computed_crc != checkpoint.crc32 {
                return Err(StreamlineError::CorruptedData(format!(
                    "Checkpoint CRC mismatch: expected {:#x}, computed {:#x}",
                    checkpoint.crc32, computed_crc
                )));
            }
            debug!(
                crc32 = checkpoint.crc32,
                "Checkpoint CRC validated successfully"
            );
        } else {
            // Old format without CRC - accept but warn
            if checkpoint.version < 1 {
                warn!("Loading checkpoint without CRC validation (old format)");
            }
        }

        debug!(
            last_sequence = checkpoint.last_sequence,
            partitions = checkpoint.partitions.len(),
            "Loaded checkpoint"
        );

        Ok(checkpoint)
    }

    /// Compute CRC32 checksum for checkpoint data (excluding the crc32 field itself)
    fn compute_checkpoint_crc(checkpoint: &Checkpoint) -> u32 {
        let mut hasher = crc32fast::Hasher::new();

        // Hash all fields except crc32
        hasher.update(&checkpoint.last_sequence.to_le_bytes());
        hasher.update(&checkpoint.timestamp.to_le_bytes());
        hasher.update(&checkpoint.version.to_le_bytes());

        // Hash partition data in deterministic order (sorted by topic, then partition)
        let mut topics: Vec<_> = checkpoint.partitions.keys().collect();
        topics.sort();

        for topic in topics {
            hasher.update(topic.as_bytes());
            if let Some(partitions) = checkpoint.partitions.get(topic) {
                let mut partition_ids: Vec<_> = partitions.keys().collect();
                partition_ids.sort();

                for &partition_id in partition_ids {
                    if let Some(p) = partitions.get(&partition_id) {
                        hasher.update(&partition_id.to_le_bytes());
                        hasher.update(&p.last_offset.to_le_bytes());
                        hasher.update(&p.last_wal_sequence.to_le_bytes());
                        hasher.update(&p.timestamp.to_le_bytes());
                    }
                }
            }
        }

        hasher.finalize()
    }

    /// Save checkpoint to disk
    pub fn save(&self) -> Result<()> {
        // Clone checkpoint and compute CRC
        let mut checkpoint_with_crc = self.checkpoint.clone();
        checkpoint_with_crc.crc32 = Self::compute_checkpoint_crc(&checkpoint_with_crc);

        let data = serde_json::to_string_pretty(&checkpoint_with_crc)?;

        // Write to temp file first, then rename for atomicity
        let temp_path = self.checkpoint_path.with_extension("json.tmp");

        // Write and sync in a block to ensure file is closed before rename
        {
            use std::io::Write;
            let mut file = fs::File::create(&temp_path)?;
            file.write_all(data.as_bytes())?;
            file.sync_all()?;
        }

        // Rename temp file to final location
        // On Unix, fs::rename is atomic and replaces the destination if it exists.
        // On Windows, fs::rename fails if the destination exists - we handle this below.
        #[cfg(unix)]
        fs::rename(&temp_path, &self.checkpoint_path)?;

        #[cfg(windows)]
        {
            // On Windows, try rename first, then remove + rename if it fails
            if let Err(_) = fs::rename(&temp_path, &self.checkpoint_path) {
                // Remove existing file and retry
                if self.checkpoint_path.exists() {
                    fs::remove_file(&self.checkpoint_path)?;
                }
                fs::rename(&temp_path, &self.checkpoint_path)?;
            }
        }

        // Sync parent directory to ensure rename is durable
        // This is CRITICAL for crash consistency - without this, the rename
        // could be lost on power failure
        if let Some(parent) = self.checkpoint_path.parent() {
            let dir = fs::File::open(parent).map_err(|e| {
                StreamlineError::storage_msg(format!(
                    "Failed to open checkpoint directory for sync: {}",
                    e
                ))
            })?;
            dir.sync_all().map_err(|e| {
                StreamlineError::storage_msg(format!(
                    "Failed to sync checkpoint directory - durability not guaranteed: {}",
                    e
                ))
            })?;
        }

        debug!(
            last_sequence = self.checkpoint.last_sequence,
            crc32 = checkpoint_with_crc.crc32,
            "Checkpoint saved"
        );

        Ok(())
    }

    /// Get current checkpoint state
    pub fn checkpoint(&self) -> &Checkpoint {
        &self.checkpoint
    }

    /// Get mutable checkpoint state
    pub fn checkpoint_mut(&mut self) -> &mut Checkpoint {
        &mut self.checkpoint
    }

    /// Update checkpoint for a partition and optionally save
    pub fn update_partition(
        &mut self,
        topic: &str,
        partition: i32,
        last_offset: i64,
        wal_sequence: u64,
        save: bool,
    ) -> Result<()> {
        self.checkpoint
            .update_partition(topic, partition, last_offset, wal_sequence);

        if save {
            self.save()?;
        }

        Ok(())
    }

    /// Record a checkpoint entry from WAL
    pub fn record_checkpoint(&mut self, sequence: u64, save: bool) -> Result<()> {
        self.checkpoint.set_last_sequence(sequence);

        if save {
            self.save()?;
        }

        Ok(())
    }

    /// Get the last checkpointed WAL sequence
    pub fn last_sequence(&self) -> u64 {
        self.checkpoint.last_sequence
    }

    // ==================== Async Methods ====================
    // These methods use spawn_blocking to avoid blocking the async runtime

    /// Create a new checkpoint manager asynchronously
    ///
    /// This is the async variant of `new()` that should be used in async contexts.
    pub async fn new_async(data_dir: PathBuf) -> Result<Self> {
        let checkpoint_path = data_dir.join("wal").join("checkpoint.json");

        // Create wal directory if needed
        if let Some(parent) = checkpoint_path.parent() {
            async_io::create_dir_all_async(parent.to_path_buf()).await?;
        }

        // Load existing checkpoint or create new one
        let checkpoint = if async_io::exists_async(checkpoint_path.clone()).await? {
            Self::load_checkpoint_async(checkpoint_path.clone()).await?
        } else {
            Checkpoint::new()
        };

        info!(
            checkpoint_path = %checkpoint_path.display(),
            last_sequence = checkpoint.last_sequence,
            "Checkpoint manager initialized (async)"
        );

        Ok(Self {
            checkpoint_path,
            checkpoint,
        })
    }

    /// Load checkpoint from disk asynchronously
    async fn load_checkpoint_async(path: PathBuf) -> Result<Checkpoint> {
        let data = async_io::read_to_string_async(path).await?;
        let checkpoint: Checkpoint = serde_json::from_str(&data).map_err(|e| {
            StreamlineError::CorruptedData(format!("Failed to parse checkpoint: {}", e))
        })?;

        // Verify CRC32 if present (version 1+ includes CRC)
        if checkpoint.version >= 1 && checkpoint.crc32 != 0 {
            let computed_crc = Self::compute_checkpoint_crc(&checkpoint);
            if computed_crc != checkpoint.crc32 {
                return Err(StreamlineError::CorruptedData(format!(
                    "Checkpoint CRC mismatch: expected {:#x}, computed {:#x}",
                    checkpoint.crc32, computed_crc
                )));
            }
            debug!(
                crc32 = checkpoint.crc32,
                "Checkpoint CRC validated successfully (async)"
            );
        } else {
            // Old format without CRC - accept but warn
            if checkpoint.version < 1 {
                warn!("Loading checkpoint without CRC validation (old format, async)");
            }
        }

        debug!(
            last_sequence = checkpoint.last_sequence,
            partitions = checkpoint.partitions.len(),
            "Loaded checkpoint (async)"
        );

        Ok(checkpoint)
    }

    /// Save checkpoint to disk asynchronously
    ///
    /// This is the async variant of `save()` that should be used in async contexts.
    pub async fn save_async(&self) -> Result<()> {
        // Clone checkpoint and compute CRC
        let mut checkpoint_with_crc = self.checkpoint.clone();
        checkpoint_with_crc.crc32 = Self::compute_checkpoint_crc(&checkpoint_with_crc);

        let data = serde_json::to_string_pretty(&checkpoint_with_crc)?;
        let checkpoint_path = self.checkpoint_path.clone();

        async_io::atomic_write_async(checkpoint_path, data.into_bytes()).await?;

        debug!(
            last_sequence = self.checkpoint.last_sequence,
            crc32 = checkpoint_with_crc.crc32,
            "Checkpoint saved (async)"
        );

        Ok(())
    }

    /// Update checkpoint for a partition and optionally save (async version)
    pub async fn update_partition_async(
        &mut self,
        topic: &str,
        partition: i32,
        last_offset: i64,
        wal_sequence: u64,
        save: bool,
    ) -> Result<()> {
        self.checkpoint
            .update_partition(topic, partition, last_offset, wal_sequence);

        if save {
            self.save_async().await?;
        }

        Ok(())
    }

    /// Record a checkpoint entry from WAL (async version)
    pub async fn record_checkpoint_async(&mut self, sequence: u64, save: bool) -> Result<()> {
        self.checkpoint.set_last_sequence(sequence);

        if save {
            self.save_async().await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_checkpoint_default() {
        let checkpoint = Checkpoint::new();
        assert_eq!(checkpoint.last_sequence, 0);
        assert!(checkpoint.partitions.is_empty());
    }

    #[test]
    fn test_checkpoint_update_partition() {
        let mut checkpoint = Checkpoint::new();

        checkpoint.update_partition("topic1", 0, 100, 50);
        checkpoint.update_partition("topic1", 1, 200, 75);
        checkpoint.update_partition("topic2", 0, 50, 100);

        assert_eq!(checkpoint.last_sequence, 100);

        let p1 = checkpoint.get_partition("topic1", 0).unwrap();
        assert_eq!(p1.last_offset, 100);
        assert_eq!(p1.last_wal_sequence, 50);

        let p2 = checkpoint.get_partition("topic1", 1).unwrap();
        assert_eq!(p2.last_offset, 200);
        assert_eq!(p2.last_wal_sequence, 75);

        let p3 = checkpoint.get_partition("topic2", 0).unwrap();
        assert_eq!(p3.last_offset, 50);
        assert_eq!(p3.last_wal_sequence, 100);

        assert!(checkpoint.get_partition("topic3", 0).is_none());
    }

    #[test]
    fn test_checkpoint_manager_persistence() {
        let dir = tempdir().unwrap();

        // Create and save checkpoint
        {
            let mut manager = CheckpointManager::new(dir.path()).unwrap();
            manager
                .update_partition("topic1", 0, 100, 50, true)
                .unwrap();
            manager
                .update_partition("topic1", 1, 200, 75, true)
                .unwrap();
        }

        // Load and verify
        {
            let manager = CheckpointManager::new(dir.path()).unwrap();
            let checkpoint = manager.checkpoint();

            assert_eq!(checkpoint.last_sequence, 75);

            let p1 = checkpoint.get_partition("topic1", 0).unwrap();
            assert_eq!(p1.last_offset, 100);

            let p2 = checkpoint.get_partition("topic1", 1).unwrap();
            assert_eq!(p2.last_offset, 200);
        }
    }

    #[test]
    fn test_checkpoint_serialization() {
        let mut checkpoint = Checkpoint::new();
        checkpoint.update_partition("test", 0, 100, 50);

        let json = serde_json::to_string(&checkpoint).unwrap();
        let parsed: Checkpoint = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.last_sequence, checkpoint.last_sequence);
        assert_eq!(parsed.get_partition("test", 0).unwrap().last_offset, 100);
    }

    // Async tests

    #[tokio::test]
    async fn test_checkpoint_manager_async() {
        let dir = tempdir().unwrap();

        // Create async checkpoint manager
        let mut manager = CheckpointManager::new_async(dir.path().to_path_buf())
            .await
            .unwrap();

        // Update partitions
        manager
            .update_partition_async("topic1", 0, 100, 50, true)
            .await
            .unwrap();
        manager
            .update_partition_async("topic1", 1, 200, 75, true)
            .await
            .unwrap();

        // Verify
        let checkpoint = manager.checkpoint();
        assert_eq!(checkpoint.last_sequence, 75);
        assert_eq!(
            checkpoint.get_partition("topic1", 0).unwrap().last_offset,
            100
        );
    }

    #[tokio::test]
    async fn test_checkpoint_manager_async_persistence() {
        let dir = tempdir().unwrap();

        // Create and save checkpoint
        {
            let mut manager = CheckpointManager::new_async(dir.path().to_path_buf())
                .await
                .unwrap();
            manager
                .update_partition_async("topic1", 0, 100, 50, true)
                .await
                .unwrap();
            manager
                .update_partition_async("topic1", 1, 200, 75, true)
                .await
                .unwrap();
        }

        // Load and verify
        {
            let manager = CheckpointManager::new_async(dir.path().to_path_buf())
                .await
                .unwrap();
            let checkpoint = manager.checkpoint();

            assert_eq!(checkpoint.last_sequence, 75);

            let p1 = checkpoint.get_partition("topic1", 0).unwrap();
            assert_eq!(p1.last_offset, 100);

            let p2 = checkpoint.get_partition("topic1", 1).unwrap();
            assert_eq!(p2.last_offset, 200);
        }
    }

    #[tokio::test]
    async fn test_checkpoint_save_async() {
        let dir = tempdir().unwrap();

        let mut manager = CheckpointManager::new_async(dir.path().to_path_buf())
            .await
            .unwrap();

        // Record checkpoint
        manager.record_checkpoint_async(100, true).await.unwrap();

        // Verify
        assert_eq!(manager.last_sequence(), 100);

        // Verify file exists
        let checkpoint_path = dir.path().join("wal").join("checkpoint.json");
        assert!(checkpoint_path.exists());
    }
}
