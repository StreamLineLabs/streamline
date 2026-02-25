//! Exactly-Once Semantics (EOS) support for Streamline
//!
//! This module implements:
//! - Idempotent producer sequence tracking
//! - Duplicate detection via sequence number validation
//! - Producer session management with epoch fencing
//!
//! # Design
//!
//! Idempotent producers are identified by (ProducerId, ProducerEpoch).
//! Each topic-partition maintains a sequence window per producer.
//! Duplicate messages are detected by checking if the batch sequence
//! falls within an already-seen window.
//!
//! # Kafka Compatibility
//!
//! Implements the semantics of KIP-98 (Exactly Once Delivery):
//! - Sequence validation per (producer_id, topic, partition)
//! - Out-of-order detection (sequence gap)
//! - Duplicate detection (sequence already seen)
//! - Epoch fencing (old producer epochs rejected)

use crate::storage::{ProducerEpoch, ProducerId};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Instant;
use tracing::{debug, warn};

/// Maximum number of in-flight batches tracked per producer-partition
///
/// Increased from 5 to 20 to handle higher-throughput producers with
/// larger retry windows. This prevents sequence window overflow for
/// producers that batch aggressively.
const MAX_SEQUENCE_WINDOW: usize = 20;

/// Result of sequence validation
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SequenceValidation {
    /// Sequence is valid, proceed with append
    Valid,
    /// Duplicate detected, return success without writing
    Duplicate { existing_offset: i64 },
    /// Sequence gap detected (out of order)
    OutOfOrder { expected: i32, received: i32 },
    /// Producer epoch is stale (fenced)
    EpochFenced { current_epoch: ProducerEpoch },
}

/// Tracks sequence state for a single producer on a single partition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProducerSequenceState {
    /// Current producer epoch
    pub epoch: ProducerEpoch,
    /// Next expected sequence number
    pub next_sequence: i32,
    /// Recent batch entries for dedup: (first_sequence, last_sequence, offset)
    pub recent_batches: Vec<BatchEntry>,
    /// Last update time (not serialized)
    #[serde(skip)]
    pub last_updated: Option<Instant>,
}

/// Entry tracking a previously seen batch
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchEntry {
    pub first_sequence: i32,
    pub last_sequence: i32,
    pub base_offset: i64,
}

impl ProducerSequenceState {
    pub fn new(epoch: ProducerEpoch) -> Self {
        Self {
            epoch,
            next_sequence: 0,
            recent_batches: Vec::with_capacity(MAX_SEQUENCE_WINDOW),
            last_updated: Some(Instant::now()),
        }
    }

    /// Validate an incoming batch sequence
    pub fn validate_sequence(
        &self,
        epoch: ProducerEpoch,
        first_sequence: i32,
        last_sequence: i32,
    ) -> SequenceValidation {
        // Check epoch first
        if epoch < self.epoch {
            return SequenceValidation::EpochFenced {
                current_epoch: self.epoch,
            };
        }

        // New epoch resets sequence tracking
        if epoch > self.epoch {
            return SequenceValidation::Valid;
        }

        // Check for duplicate
        for batch in &self.recent_batches {
            if batch.first_sequence == first_sequence && batch.last_sequence == last_sequence {
                return SequenceValidation::Duplicate {
                    existing_offset: batch.base_offset,
                };
            }
        }

        // Check for sequence gap (allow sequence 0 as reset)
        if first_sequence != 0 && first_sequence != self.next_sequence {
            return SequenceValidation::OutOfOrder {
                expected: self.next_sequence,
                received: first_sequence,
            };
        }

        SequenceValidation::Valid
    }

    /// Record a successfully written batch
    pub fn record_batch(&mut self, first_sequence: i32, last_sequence: i32, base_offset: i64) {
        self.recent_batches.push(BatchEntry {
            first_sequence,
            last_sequence,
            base_offset,
        });

        // Keep window size bounded
        if self.recent_batches.len() > MAX_SEQUENCE_WINDOW {
            self.recent_batches.remove(0);
        }

        self.next_sequence = last_sequence.wrapping_add(1);
        self.last_updated = Some(Instant::now());
    }

    /// Update epoch (new producer session)
    pub fn update_epoch(&mut self, new_epoch: ProducerEpoch) {
        if new_epoch > self.epoch {
            self.epoch = new_epoch;
            self.next_sequence = 0;
            self.recent_batches.clear();
            self.last_updated = Some(Instant::now());
        }
    }
}

/// Partition-level key for sequence tracking
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct PartitionKey {
    topic: String,
    partition: i32,
}

/// EOS Manager: tracks idempotent producer sequences across all partitions
///
/// Thread-safe via DashMap sharding per partition key.
pub struct EosManager {
    /// (topic, partition) -> (producer_id -> sequence_state)
    sequences: DashMap<PartitionKey, HashMap<ProducerId, ProducerSequenceState>>,
    /// Whether EOS is enabled
    enabled: bool,
}

impl EosManager {
    /// Create a new EOS manager
    pub fn new(enabled: bool) -> Self {
        Self {
            sequences: DashMap::new(),
            enabled,
        }
    }

    /// Check if EOS is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Validate a produce request for idempotency
    pub fn validate_produce(
        &self,
        topic: &str,
        partition: i32,
        producer_id: ProducerId,
        producer_epoch: ProducerEpoch,
        first_sequence: i32,
        last_sequence: i32,
    ) -> SequenceValidation {
        if !self.enabled || producer_id < 0 {
            return SequenceValidation::Valid;
        }

        let key = PartitionKey {
            topic: topic.to_string(),
            partition,
        };

        let map = self.sequences.entry(key).or_default();
        match map.get(&producer_id) {
            Some(state) => state.validate_sequence(producer_epoch, first_sequence, last_sequence),
            None => SequenceValidation::Valid, // First batch from this producer
        }
    }

    /// Record a successful produce for sequence tracking
    pub fn record_produce(
        &self,
        topic: &str,
        partition: i32,
        producer_id: ProducerId,
        producer_epoch: ProducerEpoch,
        first_sequence: i32,
        last_sequence: i32,
        base_offset: i64,
    ) {
        if !self.enabled || producer_id < 0 {
            return;
        }

        let key = PartitionKey {
            topic: topic.to_string(),
            partition,
        };

        let mut map = self.sequences.entry(key).or_default();
        let state = map
            .entry(producer_id)
            .or_insert_with(|| ProducerSequenceState::new(producer_epoch));

        if producer_epoch > state.epoch {
            state.update_epoch(producer_epoch);
        }

        state.record_batch(first_sequence, last_sequence, base_offset);

        debug!(
            producer_id,
            producer_epoch,
            first_sequence,
            last_sequence,
            base_offset,
            "EOS: Recorded produce sequence"
        );
    }

    /// Fence a producer (bump epoch, invalidate old sessions)
    pub fn fence_producer(
        &self,
        topic: &str,
        partition: i32,
        producer_id: ProducerId,
        new_epoch: ProducerEpoch,
    ) {
        let key = PartitionKey {
            topic: topic.to_string(),
            partition,
        };

        if let Some(mut map) = self.sequences.get_mut(&key) {
            if let Some(state) = map.get_mut(&producer_id) {
                state.update_epoch(new_epoch);
                warn!(
                    producer_id,
                    new_epoch, "EOS: Producer fenced with new epoch"
                );
            }
        }
    }

    /// Get snapshot of sequence state for a producer on a partition
    pub fn get_sequence_state(
        &self,
        topic: &str,
        partition: i32,
        producer_id: ProducerId,
    ) -> Option<ProducerSequenceState> {
        let key = PartitionKey {
            topic: topic.to_string(),
            partition,
        };

        self.sequences
            .get(&key)
            .and_then(|map| map.get(&producer_id).cloned())
    }

    /// Clean up expired producer sequence state
    pub fn cleanup_expired(&self, max_age: std::time::Duration) {
        let mut removed = 0;
        for mut entry in self.sequences.iter_mut() {
            entry.value_mut().retain(|_, state| {
                let keep = state
                    .last_updated
                    .map(|t| t.elapsed() < max_age)
                    .unwrap_or(true);
                if !keep {
                    removed += 1;
                }
                keep
            });
        }
        if removed > 0 {
            debug!(removed, "EOS: Cleaned up expired producer sequences");
        }
        // Remove empty partition entries
        self.sequences.retain(|_, map| !map.is_empty());
    }

    /// Get total number of tracked producers
    pub fn tracked_producer_count(&self) -> usize {
        self.sequences.iter().map(|e| e.value().len()).sum()
    }

    /// Serialize all sequence state to a snapshot for crash recovery.
    ///
    /// Returns a map of (topic, partition) → (producer_id → state)
    /// that can be persisted to disk and restored with `restore_snapshot`.
    pub fn create_snapshot(&self) -> HashMap<(String, i32), HashMap<ProducerId, ProducerSequenceState>> {
        let mut snapshot = HashMap::new();
        for entry in self.sequences.iter() {
            let key = entry.key();
            snapshot.insert(
                (key.topic.clone(), key.partition),
                entry.value().clone(),
            );
        }
        snapshot
    }

    /// Restore sequence state from a previously saved snapshot.
    ///
    /// This is called during server startup to recover idempotent
    /// producer state from the last checkpoint, preventing false
    /// duplicate rejections after restart.
    pub fn restore_snapshot(
        &self,
        snapshot: HashMap<(String, i32), HashMap<ProducerId, ProducerSequenceState>>,
    ) {
        let mut count = 0;
        for ((topic, partition), producers) in snapshot {
            let key = PartitionKey { topic, partition };
            count += producers.len();
            self.sequences.insert(key, producers);
        }
        debug!(producers = count, "EOS: Restored sequence state from snapshot");
    }

    /// Save sequence state to a file for crash recovery
    pub fn save_to_file(&self, path: &std::path::Path) -> crate::error::Result<()> {
        let snapshot = self.create_snapshot();
        let serializable: HashMap<String, HashMap<String, ProducerSequenceState>> = snapshot
            .into_iter()
            .map(|((topic, partition), producers)| {
                let key = format!("{}:{}", topic, partition);
                let prods: HashMap<String, ProducerSequenceState> = producers
                    .into_iter()
                    .map(|(pid, state)| (pid.to_string(), state))
                    .collect();
                (key, prods)
            })
            .collect();

        let json = serde_json::to_string(&serializable).map_err(|e| {
            crate::error::StreamlineError::Storage(format!(
                "Failed to serialize EOS state: {}", e
            ))
        })?;

        let temp_path = path.with_extension("tmp");
        std::fs::write(&temp_path, &json).map_err(|e| {
            crate::error::StreamlineError::Storage(format!(
                "Failed to write EOS snapshot: {}", e
            ))
        })?;
        // Atomic rename for crash safety
        std::fs::rename(&temp_path, path).map_err(|e| {
            crate::error::StreamlineError::Storage(format!(
                "Failed to finalize EOS snapshot: {}", e
            ))
        })?;

        debug!(
            producers = self.tracked_producer_count(),
            "EOS: Saved sequence state to file"
        );
        Ok(())
    }

    /// Load sequence state from a previously saved file
    pub fn load_from_file(&self, path: &std::path::Path) -> crate::error::Result<()> {
        if !path.exists() {
            debug!("No EOS snapshot found, starting fresh");
            return Ok(());
        }

        let json = std::fs::read_to_string(path).map_err(|e| {
            crate::error::StreamlineError::Storage(format!(
                "Failed to read EOS snapshot: {}", e
            ))
        })?;

        let serializable: HashMap<String, HashMap<String, ProducerSequenceState>> =
            serde_json::from_str(&json).map_err(|e| {
                crate::error::StreamlineError::Storage(format!(
                    "Failed to parse EOS snapshot: {}", e
                ))
            })?;

        let mut count = 0;
        for (key_str, producers) in serializable {
            let parts: Vec<&str> = key_str.rsplitn(2, ':').collect();
            if parts.len() != 2 {
                warn!(key = %key_str, "EOS: Skipping malformed partition key in snapshot");
                continue;
            }
            let partition: i32 = match parts[0].parse() {
                Ok(p) => p,
                Err(_) => continue,
            };
            let topic = parts[1].to_string();

            let producer_map: HashMap<ProducerId, ProducerSequenceState> = producers
                .into_iter()
                .filter_map(|(pid_str, state)| {
                    pid_str.parse::<ProducerId>().ok().map(|pid| (pid, state))
                })
                .collect();
            count += producer_map.len();
            self.sequences.insert(
                PartitionKey { topic, partition },
                producer_map,
            );
        }

        debug!(producers = count, "EOS: Loaded sequence state from file");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sequence_validation_valid() {
        let state = ProducerSequenceState::new(0);
        let result = state.validate_sequence(0, 0, 0);
        assert_eq!(result, SequenceValidation::Valid);
    }

    #[test]
    fn test_sequence_validation_duplicate() {
        let mut state = ProducerSequenceState::new(0);
        state.record_batch(0, 4, 100);

        let result = state.validate_sequence(0, 0, 4);
        assert_eq!(
            result,
            SequenceValidation::Duplicate {
                existing_offset: 100
            }
        );
    }

    #[test]
    fn test_sequence_validation_out_of_order() {
        let mut state = ProducerSequenceState::new(0);
        state.record_batch(0, 4, 100);

        let result = state.validate_sequence(0, 10, 14);
        assert_eq!(
            result,
            SequenceValidation::OutOfOrder {
                expected: 5,
                received: 10
            }
        );
    }

    #[test]
    fn test_sequence_validation_epoch_fenced() {
        let state = ProducerSequenceState::new(5);
        let result = state.validate_sequence(3, 0, 0);
        assert_eq!(
            result,
            SequenceValidation::EpochFenced { current_epoch: 5 }
        );
    }

    #[test]
    fn test_sequence_new_epoch_resets() {
        let mut state = ProducerSequenceState::new(0);
        state.record_batch(0, 4, 100);

        // New epoch should be valid even with sequence 0
        let result = state.validate_sequence(1, 0, 0);
        assert_eq!(result, SequenceValidation::Valid);
    }

    #[test]
    fn test_sequence_window_bounded() {
        let mut state = ProducerSequenceState::new(0);
        for i in 0..10 {
            let base = i * 5;
            state.record_batch(base, base + 4, i as i64 * 100);
        }
        assert!(state.recent_batches.len() <= MAX_SEQUENCE_WINDOW);
    }

    #[test]
    fn test_eos_manager_validate_and_record() {
        let mgr = EosManager::new(true);

        // First produce should be valid
        let result = mgr.validate_produce("topic-1", 0, 1000, 0, 0, 4);
        assert_eq!(result, SequenceValidation::Valid);

        // Record it
        mgr.record_produce("topic-1", 0, 1000, 0, 0, 4, 0);

        // Duplicate should be detected
        let result = mgr.validate_produce("topic-1", 0, 1000, 0, 0, 4);
        assert_eq!(
            result,
            SequenceValidation::Duplicate {
                existing_offset: 0
            }
        );

        // Next sequence should be valid
        let result = mgr.validate_produce("topic-1", 0, 1000, 0, 5, 9);
        assert_eq!(result, SequenceValidation::Valid);
    }

    #[test]
    fn test_eos_manager_disabled() {
        let mgr = EosManager::new(false);
        // Should always return Valid when disabled
        let result = mgr.validate_produce("topic-1", 0, 1000, 0, 999, 999);
        assert_eq!(result, SequenceValidation::Valid);
    }

    #[test]
    fn test_eos_manager_fence_producer() {
        let mgr = EosManager::new(true);
        mgr.record_produce("topic-1", 0, 1000, 0, 0, 4, 0);

        // Fence with new epoch
        mgr.fence_producer("topic-1", 0, 1000, 1);

        // Old epoch should be fenced
        let result = mgr.validate_produce("topic-1", 0, 1000, 0, 5, 9);
        assert_eq!(
            result,
            SequenceValidation::EpochFenced { current_epoch: 1 }
        );
    }

    #[test]
    fn test_eos_manager_negative_producer_id_skipped() {
        let mgr = EosManager::new(true);
        let result = mgr.validate_produce("topic-1", 0, -1, 0, 0, 0);
        assert_eq!(result, SequenceValidation::Valid);
    }

    #[test]
    fn test_eos_manager_cleanup() {
        let mgr = EosManager::new(true);
        mgr.record_produce("topic-1", 0, 1000, 0, 0, 4, 0);

        assert_eq!(mgr.tracked_producer_count(), 1);

        // Cleanup with large duration should keep entries
        mgr.cleanup_expired(std::time::Duration::from_secs(3600));
        assert_eq!(mgr.tracked_producer_count(), 1);
    }

    #[test]
    fn test_eos_manager_multiple_partitions() {
        let mgr = EosManager::new(true);

        mgr.record_produce("topic-1", 0, 1000, 0, 0, 4, 0);
        mgr.record_produce("topic-1", 1, 1000, 0, 0, 4, 100);

        // Sequences are independent per partition
        let s0 = mgr.get_sequence_state("topic-1", 0, 1000).unwrap();
        let s1 = mgr.get_sequence_state("topic-1", 1, 1000).unwrap();
        assert_eq!(s0.next_sequence, 5);
        assert_eq!(s1.next_sequence, 5);
    }

    // ========== Snapshot Persistence Tests ==========

    #[test]
    fn test_create_and_restore_snapshot() {
        let mgr = EosManager::new(true);

        // Record some sequence state
        mgr.record_produce("topic-1", 0, 1000, 0, 0, 4, 0);
        mgr.record_produce("topic-1", 0, 1000, 0, 5, 9, 100);
        mgr.record_produce("topic-2", 0, 2000, 0, 0, 2, 200);

        // Create snapshot
        let snapshot = mgr.create_snapshot();
        assert_eq!(snapshot.len(), 2); // two partition keys

        // Restore into a new manager
        let mgr2 = EosManager::new(true);
        mgr2.restore_snapshot(snapshot);

        // Verify restored state
        let s1 = mgr2.get_sequence_state("topic-1", 0, 1000).unwrap();
        assert_eq!(s1.next_sequence, 10);
        assert_eq!(s1.epoch, 0);

        let s2 = mgr2.get_sequence_state("topic-2", 0, 2000).unwrap();
        assert_eq!(s2.next_sequence, 3);

        assert_eq!(mgr2.tracked_producer_count(), 2);
    }

    #[test]
    fn test_save_and_load_from_file() {
        let tmp = tempfile::TempDir::new().unwrap();
        let file_path = tmp.path().join("eos_snapshot.json");

        // Create and populate manager
        let mgr = EosManager::new(true);
        mgr.record_produce("orders", 0, 100, 0, 0, 9, 0);
        mgr.record_produce("orders", 1, 100, 0, 0, 4, 50);
        mgr.record_produce("events", 0, 200, 1, 0, 0, 0);

        // Save to file
        mgr.save_to_file(&file_path).unwrap();
        assert!(file_path.exists());

        // Load into a new manager
        let mgr2 = EosManager::new(true);
        mgr2.load_from_file(&file_path).unwrap();

        // Verify loaded state
        assert_eq!(mgr2.tracked_producer_count(), 3);

        let s = mgr2.get_sequence_state("orders", 0, 100).unwrap();
        assert_eq!(s.next_sequence, 10);

        let s = mgr2.get_sequence_state("orders", 1, 100).unwrap();
        assert_eq!(s.next_sequence, 5);

        let s = mgr2.get_sequence_state("events", 0, 200).unwrap();
        assert_eq!(s.next_sequence, 1);
        assert_eq!(s.epoch, 1);
    }

    #[test]
    fn test_load_from_nonexistent_file() {
        let tmp = tempfile::TempDir::new().unwrap();
        let file_path = tmp.path().join("nonexistent.json");

        let mgr = EosManager::new(true);
        // Should succeed gracefully — no file means fresh state
        mgr.load_from_file(&file_path).unwrap();
        assert_eq!(mgr.tracked_producer_count(), 0);
    }

    #[test]
    fn test_crash_recovery_preserves_dedup() {
        let tmp = tempfile::TempDir::new().unwrap();
        let file_path = tmp.path().join("eos_crash.json");

        // Simulate pre-crash state
        let mgr = EosManager::new(true);
        mgr.record_produce("topic", 0, 1000, 0, 0, 4, 0);
        mgr.record_produce("topic", 0, 1000, 0, 5, 9, 100);
        mgr.save_to_file(&file_path).unwrap();

        // Simulate crash and recovery
        let recovered = EosManager::new(true);
        recovered.load_from_file(&file_path).unwrap();

        // Replayed batch should be detected as duplicate
        let result = recovered.validate_produce("topic", 0, 1000, 0, 5, 9);
        assert_eq!(
            result,
            SequenceValidation::Duplicate {
                existing_offset: 100
            }
        );

        // Next batch should be valid
        let result = recovered.validate_produce("topic", 0, 1000, 0, 10, 14);
        assert_eq!(result, SequenceValidation::Valid);
    }

    #[test]
    fn test_snapshot_with_multiple_epochs() {
        let tmp = tempfile::TempDir::new().unwrap();
        let file_path = tmp.path().join("eos_epochs.json");

        let mgr = EosManager::new(true);

        // Epoch 0
        mgr.record_produce("t", 0, 1, 0, 0, 4, 0);
        // Fence to epoch 1
        mgr.fence_producer("t", 0, 1, 1);
        // Epoch 1
        mgr.record_produce("t", 0, 1, 1, 0, 2, 50);

        mgr.save_to_file(&file_path).unwrap();

        let mgr2 = EosManager::new(true);
        mgr2.load_from_file(&file_path).unwrap();

        // Old epoch should still be fenced
        let result = mgr2.validate_produce("t", 0, 1, 0, 5, 9);
        assert_eq!(result, SequenceValidation::EpochFenced { current_epoch: 1 });

        // Current epoch should work
        let result = mgr2.validate_produce("t", 0, 1, 1, 3, 5);
        assert_eq!(result, SequenceValidation::Valid);
    }

    #[test]
    fn test_atomic_file_write_no_corruption() {
        let tmp = tempfile::TempDir::new().unwrap();
        let file_path = tmp.path().join("eos_atomic.json");

        let mgr = EosManager::new(true);
        for i in 0..50 {
            mgr.record_produce(&format!("topic-{}", i % 10), i % 3, i as i64, 0, 0, 4, i as i64 * 100);
        }

        // Save multiple times (simulates periodic checkpointing)
        for _ in 0..5 {
            mgr.save_to_file(&file_path).unwrap();
        }

        // Verify the file is valid JSON
        let mgr2 = EosManager::new(true);
        mgr2.load_from_file(&file_path).unwrap();
        assert_eq!(mgr2.tracked_producer_count(), mgr.tracked_producer_count());
    }
}
