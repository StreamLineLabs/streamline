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
const MAX_SEQUENCE_WINDOW: usize = 5;

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
}
