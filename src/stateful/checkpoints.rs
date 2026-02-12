//! Checkpointing
//!
//! Provides checkpointing for fault-tolerant stateful processing.

use super::{state::StateManager, windows::WindowManager, OperatorError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Checkpoint configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointConfig {
    /// Interval between checkpoints (ms)
    pub interval_ms: u64,
    /// Minimum pause between checkpoints (ms)
    pub min_pause_between_checkpoints_ms: u64,
    /// Checkpoint timeout (ms)
    pub timeout_ms: u64,
    /// Maximum concurrent checkpoints
    pub max_concurrent_checkpoints: usize,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            interval_ms: 60000,                     // 1 minute
            min_pause_between_checkpoints_ms: 1000, // 1 second
            timeout_ms: 300000,                     // 5 minutes
            max_concurrent_checkpoints: 1,
        }
    }
}

/// Checkpoint metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointMetadata {
    /// Checkpoint ID
    pub id: u64,
    /// Creation timestamp
    pub timestamp: i64,
    /// Duration to create (ms)
    pub duration_ms: u64,
    /// State size (bytes)
    pub state_size: usize,
    /// Operator states included
    pub operators: Vec<String>,
    /// Watermark at checkpoint
    pub watermark: i64,
    /// Is complete
    pub is_complete: bool,
}

/// Checkpoint data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Checkpoint {
    /// Metadata
    pub metadata: CheckpointMetadata,
    /// State snapshot
    pub state: Vec<u8>,
    /// Window state
    pub windows: HashMap<String, Vec<u8>>,
    /// Watermarks by partition
    pub watermarks: HashMap<u32, i64>,
    /// Custom data
    pub custom: HashMap<String, Vec<u8>>,
}

impl Checkpoint {
    /// Create a new checkpoint
    pub fn new(id: u64) -> Self {
        Self {
            metadata: CheckpointMetadata {
                id,
                timestamp: chrono::Utc::now().timestamp_millis(),
                duration_ms: 0,
                state_size: 0,
                operators: Vec::new(),
                watermark: 0,
                is_complete: false,
            },
            state: Vec::new(),
            windows: HashMap::new(),
            watermarks: HashMap::new(),
            custom: HashMap::new(),
        }
    }

    /// Serialize checkpoint
    pub fn serialize(&self) -> Result<Vec<u8>, OperatorError> {
        serde_json::to_vec(self).map_err(|e| OperatorError::SerializationError(e.to_string()))
    }

    /// Deserialize checkpoint
    pub fn deserialize(data: &[u8]) -> Result<Self, OperatorError> {
        serde_json::from_slice(data).map_err(|e| OperatorError::SerializationError(e.to_string()))
    }
}

/// Checkpoint store trait
pub trait CheckpointStore: Send + Sync {
    /// Save checkpoint
    fn save(&mut self, checkpoint: &Checkpoint) -> Result<(), OperatorError>;

    /// Load checkpoint by ID
    fn load(&self, id: u64) -> Result<Option<Checkpoint>, OperatorError>;

    /// Get latest checkpoint
    fn latest(&self) -> Result<Option<Checkpoint>, OperatorError>;

    /// List available checkpoints
    fn list(&self) -> Result<Vec<CheckpointMetadata>, OperatorError>;

    /// Delete checkpoint
    fn delete(&mut self, id: u64) -> Result<(), OperatorError>;

    /// Delete checkpoints older than timestamp
    fn delete_older_than(&mut self, timestamp: i64) -> Result<usize, OperatorError>;
}

/// In-memory checkpoint store
#[derive(Debug, Default)]
pub struct MemoryCheckpointStore {
    /// Checkpoints by ID
    checkpoints: HashMap<u64, Checkpoint>,
    /// Maximum checkpoints to retain
    max_retained: usize,
}

impl MemoryCheckpointStore {
    /// Create a new memory store
    pub fn new(max_retained: usize) -> Self {
        Self {
            checkpoints: HashMap::new(),
            max_retained,
        }
    }
}

impl CheckpointStore for MemoryCheckpointStore {
    fn save(&mut self, checkpoint: &Checkpoint) -> Result<(), OperatorError> {
        self.checkpoints
            .insert(checkpoint.metadata.id, checkpoint.clone());

        // Clean up old checkpoints
        if self.checkpoints.len() > self.max_retained {
            if let Some(oldest_id) = self
                .checkpoints
                .values()
                .min_by_key(|c| c.metadata.timestamp)
                .map(|c| c.metadata.id)
            {
                self.checkpoints.remove(&oldest_id);
            }
        }

        Ok(())
    }

    fn load(&self, id: u64) -> Result<Option<Checkpoint>, OperatorError> {
        Ok(self.checkpoints.get(&id).cloned())
    }

    fn latest(&self) -> Result<Option<Checkpoint>, OperatorError> {
        Ok(self
            .checkpoints
            .values()
            .max_by_key(|c| c.metadata.timestamp)
            .cloned())
    }

    fn list(&self) -> Result<Vec<CheckpointMetadata>, OperatorError> {
        Ok(self
            .checkpoints
            .values()
            .map(|c| c.metadata.clone())
            .collect())
    }

    fn delete(&mut self, id: u64) -> Result<(), OperatorError> {
        self.checkpoints.remove(&id);
        Ok(())
    }

    fn delete_older_than(&mut self, timestamp: i64) -> Result<usize, OperatorError> {
        let old_len = self.checkpoints.len();
        self.checkpoints
            .retain(|_, c| c.metadata.timestamp >= timestamp);
        Ok(old_len - self.checkpoints.len())
    }
}

/// Checkpoint manager
pub struct CheckpointManager {
    /// Configuration
    config: CheckpointConfig,
    /// Store
    store: Box<dyn CheckpointStore>,
    /// Next checkpoint ID
    next_id: u64,
    /// Last checkpoint timestamp
    last_checkpoint_ts: i64,
    /// In-progress checkpoint
    in_progress: Option<u64>,
    /// Statistics
    stats: CheckpointStats,
}

/// Checkpoint statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CheckpointStats {
    /// Total checkpoints created
    pub checkpoints_created: u64,
    /// Total checkpoints restored
    pub checkpoints_restored: u64,
    /// Failed checkpoints
    pub checkpoints_failed: u64,
    /// Average checkpoint duration (ms)
    pub avg_duration_ms: f64,
    /// Last checkpoint size
    pub last_size_bytes: usize,
}

impl CheckpointManager {
    /// Create a new checkpoint manager
    pub fn new(config: CheckpointConfig) -> Self {
        Self {
            config,
            store: Box::new(MemoryCheckpointStore::new(10)),
            next_id: 1,
            last_checkpoint_ts: chrono::Utc::now().timestamp_millis(),
            in_progress: None,
            stats: CheckpointStats::default(),
        }
    }

    /// Create a new checkpoint manager with custom store
    pub fn with_store(config: CheckpointConfig, store: Box<dyn CheckpointStore>) -> Self {
        Self {
            config,
            store,
            next_id: 1,
            last_checkpoint_ts: chrono::Utc::now().timestamp_millis(),
            in_progress: None,
            stats: CheckpointStats::default(),
        }
    }

    /// Check if checkpoint is due
    pub fn is_checkpoint_due(&self) -> bool {
        let now = chrono::Utc::now().timestamp_millis();
        now - self.last_checkpoint_ts >= self.config.interval_ms as i64
    }

    /// Create checkpoint
    pub fn create_checkpoint<T: Clone + Send + Sync + 'static>(
        &mut self,
        state_manager: &StateManager,
        _window_manager: &WindowManager<T>,
        watermarks: &HashMap<u32, i64>,
    ) -> Result<Checkpoint, OperatorError> {
        let start_time = chrono::Utc::now().timestamp_millis();

        // Create checkpoint
        let mut checkpoint = Checkpoint::new(self.next_id);
        self.next_id += 1;
        self.in_progress = Some(checkpoint.metadata.id);

        // Snapshot state
        checkpoint.state = state_manager.snapshot()?;
        checkpoint.metadata.state_size = checkpoint.state.len();

        // Save watermarks
        checkpoint.watermarks = watermarks.clone();
        checkpoint.metadata.watermark = *watermarks.values().min().unwrap_or(&0);

        // Calculate duration
        let end_time = chrono::Utc::now().timestamp_millis();
        checkpoint.metadata.duration_ms = (end_time - start_time) as u64;
        checkpoint.metadata.is_complete = true;

        // Update stats
        self.stats.checkpoints_created += 1;
        self.update_avg_duration(checkpoint.metadata.duration_ms as f64);
        self.stats.last_size_bytes = checkpoint.metadata.state_size;

        // Save checkpoint
        self.store.save(&checkpoint)?;
        self.last_checkpoint_ts = end_time;
        self.in_progress = None;

        Ok(checkpoint)
    }

    /// Restore from checkpoint
    pub fn restore_checkpoint<T: Clone + Send + Sync + 'static>(
        &mut self,
        checkpoint: &Checkpoint,
        state_manager: &mut StateManager,
        _window_manager: &mut WindowManager<T>,
        watermarks: &mut HashMap<u32, i64>,
    ) -> Result<(), OperatorError> {
        // Restore state
        state_manager.restore(&checkpoint.state)?;

        // Restore watermarks
        *watermarks = checkpoint.watermarks.clone();

        self.stats.checkpoints_restored += 1;
        Ok(())
    }

    /// Get latest checkpoint
    pub fn latest(&self) -> Result<Option<Checkpoint>, OperatorError> {
        self.store.latest()
    }

    /// Load checkpoint by ID
    pub fn load(&self, id: u64) -> Result<Option<Checkpoint>, OperatorError> {
        self.store.load(id)
    }

    /// List checkpoints
    pub fn list(&self) -> Result<Vec<CheckpointMetadata>, OperatorError> {
        self.store.list()
    }

    /// Delete old checkpoints
    pub fn cleanup(&mut self, retain_count: usize) -> Result<usize, OperatorError> {
        let checkpoints = self.store.list()?;
        if checkpoints.len() <= retain_count {
            return Ok(0);
        }

        let mut sorted: Vec<_> = checkpoints.into_iter().collect();
        sorted.sort_by_key(|c| c.timestamp);

        let to_delete = sorted.len() - retain_count;
        for metadata in sorted.into_iter().take(to_delete) {
            self.store.delete(metadata.id)?;
        }

        Ok(to_delete)
    }

    /// Get statistics
    pub fn stats(&self) -> &CheckpointStats {
        &self.stats
    }

    /// Update rolling average duration
    fn update_avg_duration(&mut self, duration: f64) {
        let n = self.stats.checkpoints_created as f64;
        self.stats.avg_duration_ms = ((self.stats.avg_duration_ms * (n - 1.0)) + duration) / n;
    }

    /// Check if checkpoint is in progress
    pub fn is_in_progress(&self) -> bool {
        self.in_progress.is_some()
    }

    /// Get configuration
    pub fn config(&self) -> &CheckpointConfig {
        &self.config
    }
}

/// Incremental checkpoint support
pub struct IncrementalCheckpoint {
    /// Base checkpoint ID
    pub base_id: u64,
    /// Delta data
    pub delta: Vec<u8>,
    /// Changed keys
    pub changed_keys: Vec<String>,
    /// Timestamp
    pub timestamp: i64,
}

impl IncrementalCheckpoint {
    /// Create new incremental checkpoint
    pub fn new(base_id: u64) -> Self {
        Self {
            base_id,
            delta: Vec::new(),
            changed_keys: Vec::new(),
            timestamp: chrono::Utc::now().timestamp_millis(),
        }
    }

    /// Add changed key
    pub fn add_change(&mut self, key: impl Into<String>) {
        self.changed_keys.push(key.into());
    }

    /// Set delta data
    pub fn set_delta(&mut self, delta: Vec<u8>) {
        self.delta = delta;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checkpoint_config_default() {
        let config = CheckpointConfig::default();
        assert_eq!(config.interval_ms, 60000);
        assert_eq!(config.max_concurrent_checkpoints, 1);
    }

    #[test]
    fn test_checkpoint_new() {
        let checkpoint = Checkpoint::new(1);
        assert_eq!(checkpoint.metadata.id, 1);
        assert!(!checkpoint.metadata.is_complete);
    }

    #[test]
    fn test_checkpoint_serialize_deserialize() {
        let mut checkpoint = Checkpoint::new(1);
        checkpoint.metadata.is_complete = true;
        checkpoint.state = vec![1, 2, 3];
        checkpoint.watermarks.insert(0, 1000);

        let serialized = checkpoint.serialize().unwrap();
        let deserialized = Checkpoint::deserialize(&serialized).unwrap();

        assert_eq!(deserialized.metadata.id, 1);
        assert_eq!(deserialized.state, vec![1, 2, 3]);
        assert_eq!(deserialized.watermarks.get(&0), Some(&1000));
    }

    #[test]
    fn test_memory_checkpoint_store() {
        let mut store = MemoryCheckpointStore::new(5);

        // Save checkpoint
        let mut cp = Checkpoint::new(1);
        cp.metadata.is_complete = true;
        store.save(&cp).unwrap();

        // Load checkpoint
        let loaded = store.load(1).unwrap();
        assert!(loaded.is_some());
        assert_eq!(loaded.unwrap().metadata.id, 1);

        // Latest checkpoint
        let latest = store.latest().unwrap();
        assert!(latest.is_some());
    }

    #[test]
    fn test_memory_checkpoint_store_max_retained() {
        let mut store = MemoryCheckpointStore::new(3);

        for i in 1..=5 {
            let mut cp = Checkpoint::new(i);
            cp.metadata.is_complete = true;
            cp.metadata.timestamp = i as i64 * 1000;
            store.save(&cp).unwrap();
        }

        let list = store.list().unwrap();
        assert_eq!(list.len(), 3);

        // Oldest should be removed
        assert!(store.load(1).unwrap().is_none());
        assert!(store.load(2).unwrap().is_none());
        assert!(store.load(3).unwrap().is_some());
    }

    #[test]
    fn test_checkpoint_manager() {
        let config = CheckpointConfig::default();
        let manager = CheckpointManager::new(config);

        assert!(!manager.is_checkpoint_due()); // Just created
        assert!(!manager.is_in_progress());
    }

    #[test]
    fn test_checkpoint_manager_cleanup() {
        let config = CheckpointConfig::default();
        let mut manager = CheckpointManager::new(config);

        // Create some checkpoints manually in store
        for i in 1..=5 {
            let mut cp = Checkpoint::new(i);
            cp.metadata.is_complete = true;
            cp.metadata.timestamp = i as i64 * 1000;
            manager.store.save(&cp).unwrap();
            manager.next_id = i + 1;
        }

        // Cleanup - retain only 2
        let deleted = manager.cleanup(2).unwrap();
        assert_eq!(deleted, 3);

        let remaining = manager.list().unwrap();
        assert_eq!(remaining.len(), 2);
    }

    #[test]
    fn test_checkpoint_stats() {
        let stats = CheckpointStats::default();
        assert_eq!(stats.checkpoints_created, 0);
        assert_eq!(stats.checkpoints_restored, 0);
    }

    #[test]
    fn test_incremental_checkpoint() {
        let mut incr = IncrementalCheckpoint::new(1);
        incr.add_change("key1");
        incr.add_change("key2");
        incr.set_delta(vec![1, 2, 3]);

        assert_eq!(incr.base_id, 1);
        assert_eq!(incr.changed_keys.len(), 2);
        assert_eq!(incr.delta, vec![1, 2, 3]);
    }

    #[test]
    fn test_checkpoint_store_delete_older_than() {
        let mut store = MemoryCheckpointStore::new(10);

        for i in 1..=5 {
            let mut cp = Checkpoint::new(i);
            cp.metadata.is_complete = true;
            cp.metadata.timestamp = i as i64 * 1000;
            store.save(&cp).unwrap();
        }

        // Delete checkpoints older than timestamp 3000
        let deleted = store.delete_older_than(3000).unwrap();
        assert_eq!(deleted, 2);

        let remaining = store.list().unwrap();
        assert_eq!(remaining.len(), 3);
    }
}
