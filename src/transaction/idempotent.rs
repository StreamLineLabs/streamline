//! Idempotent Producer Engine for Exactly-Once Semantics (EOS) GA
//!
//! This module provides the foundation for exactly-once semantics by
//! implementing idempotent producer management. An idempotent producer
//! prevents duplicate records even on retries by tracking producer IDs,
//! epochs, and per-partition sequence numbers.
//!
//! # Design
//!
//! - Producer IDs are allocated monotonically starting from 1000
//! - Each producer has an epoch that increments on session recovery
//! - Sequence numbers are tracked per (producer_id, topic, partition)
//! - Duplicate batches are detected and rejected without side effects
//! - Expired producers are cleaned up based on configurable TTL
//!
//! # Kafka Compatibility
//!
//! Implements the producer ID allocation and sequence tracking semantics
//! of KIP-98 (Exactly Once Delivery and Transactional Messaging).

use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use tracing::{debug, info, warn};

/// Base value for producer ID allocation
const PRODUCER_ID_BASE: i64 = 1000;

/// Configuration for idempotent producer management
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdempotentConfig {
    /// Maximum time a producer ID remains valid before expiry (ms).
    /// Default: 604800000 (7 days)
    pub max_producer_id_expiry_ms: u64,
    /// Maximum number of in-flight requests per producer.
    /// Default: 5
    pub max_in_flight_requests: u32,
    /// Expiry time for transactional IDs (ms).
    /// Default: 604800000 (7 days)
    pub transactional_id_expiry_ms: u64,
    /// Whether epoch fencing is enabled.
    /// Default: true
    pub enable_epoch_fencing: bool,
}

impl Default for IdempotentConfig {
    fn default() -> Self {
        Self {
            max_producer_id_expiry_ms: 604_800_000,
            max_in_flight_requests: 5,
            transactional_id_expiry_ms: 604_800_000,
            enable_epoch_fencing: true,
        }
    }
}

/// State for a single registered producer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProducerState {
    pub producer_id: i64,
    pub producer_epoch: i16,
    /// Last committed sequence number per (topic, partition)
    pub last_sequence: HashMap<(String, i32), i32>,
    /// Currently in-flight (uncommitted) batches
    pub in_flight_batches: Vec<InFlightBatch>,
    /// Timestamp when this producer was created (epoch ms)
    pub created_at: u64,
    /// Timestamp of the last update (epoch ms)
    pub last_updated_at: u64,
    /// Optional transactional ID for transactional producers
    pub transactional_id: Option<String>,
}

/// A batch that has been accepted but not yet fully committed
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InFlightBatch {
    pub first_sequence: i32,
    pub last_sequence: i32,
    pub topic: String,
    pub partition: i32,
    pub timestamp_ms: u64,
}

/// Result of deduplication checking for an incoming batch
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeduplicationResult {
    /// Batch is accepted; includes the next expected sequence number
    Accept { next_sequence: i32 },
    /// Batch is a duplicate of an already-committed batch
    Duplicate { existing_offset: i64 },
    /// Batch sequence number is out of order
    OutOfOrder { expected: i32, received: i32 },
    /// Producer epoch is stale; the current epoch is returned
    InvalidEpoch {
        current_epoch: i16,
        received_epoch: i16,
    },
}

/// Atomic counters for idempotent producer statistics
pub struct IdempotentStats {
    pub total_producers: AtomicU64,
    pub active_producers: AtomicU64,
    pub duplicates_rejected: AtomicU64,
    pub out_of_order_rejected: AtomicU64,
    pub epoch_fences: AtomicU64,
    pub sequences_committed: AtomicU64,
}

impl IdempotentStats {
    fn new() -> Self {
        Self {
            total_producers: AtomicU64::new(0),
            active_producers: AtomicU64::new(0),
            duplicates_rejected: AtomicU64::new(0),
            out_of_order_rejected: AtomicU64::new(0),
            epoch_fences: AtomicU64::new(0),
            sequences_committed: AtomicU64::new(0),
        }
    }
}

/// Point-in-time snapshot of idempotent producer statistics
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdempotentStatsSnapshot {
    pub total_producers: u64,
    pub active_producers: u64,
    pub duplicates_rejected: u64,
    pub out_of_order_rejected: u64,
    pub epoch_fences: u64,
    pub sequences_committed: u64,
}

/// Manages idempotent producers, providing producer ID allocation,
/// sequence validation, epoch fencing, and expiry.
pub struct IdempotentProducerManager {
    producers: Arc<RwLock<HashMap<i64, ProducerState>>>,
    config: IdempotentConfig,
    stats: Arc<IdempotentStats>,
    next_producer_id: AtomicI64,
}

impl IdempotentProducerManager {
    /// Create a new idempotent producer manager with the given configuration.
    pub fn new(config: IdempotentConfig) -> Self {
        info!("Initializing IdempotentProducerManager");
        Self {
            producers: Arc::new(RwLock::new(HashMap::new())),
            config,
            stats: Arc::new(IdempotentStats::new()),
            next_producer_id: AtomicI64::new(PRODUCER_ID_BASE),
        }
    }

    /// Allocate a new unique producer ID with initial epoch 0.
    ///
    /// Producer IDs are monotonically increasing starting from 1000.
    /// Returns `(producer_id, initial_epoch)`.
    pub fn allocate_producer_id(&self) -> (i64, i16) {
        let id = self.next_producer_id.fetch_add(1, Ordering::SeqCst);
        let epoch: i16 = 0;
        let now = current_time_ms();

        let state = ProducerState {
            producer_id: id,
            producer_epoch: epoch,
            last_sequence: HashMap::new(),
            in_flight_batches: Vec::new(),
            created_at: now,
            last_updated_at: now,
            transactional_id: None,
        };

        {
            let mut producers = self.producers.write().expect("producers lock poisoned");
            producers.insert(id, state);
        }

        self.stats.total_producers.fetch_add(1, Ordering::Relaxed);
        self.stats.active_producers.fetch_add(1, Ordering::Relaxed);

        debug!(producer_id = id, epoch, "Allocated new producer ID");
        (id, epoch)
    }

    /// Check whether an incoming batch sequence is valid for the given producer.
    ///
    /// Returns a [`DeduplicationResult`] indicating whether to accept, reject
    /// as duplicate, reject as out-of-order, or reject due to epoch fencing.
    pub fn check_sequence(
        &self,
        producer_id: i64,
        producer_epoch: i16,
        topic: &str,
        partition: i32,
        first_sequence: i32,
    ) -> DeduplicationResult {
        let producers = self.producers.read().expect("producers lock poisoned");

        let state = match producers.get(&producer_id) {
            Some(s) => s,
            None => {
                // Unknown producer — accept (first contact)
                return DeduplicationResult::Accept {
                    next_sequence: first_sequence + 1,
                };
            }
        };

        // Epoch fencing: reject stale epochs
        if self.config.enable_epoch_fencing && producer_epoch < state.producer_epoch {
            self.stats.epoch_fences.fetch_add(1, Ordering::Relaxed);
            warn!(
                producer_id,
                received_epoch = producer_epoch,
                current_epoch = state.producer_epoch,
                "Epoch fence: rejecting stale producer epoch"
            );
            return DeduplicationResult::InvalidEpoch {
                current_epoch: state.producer_epoch,
                received_epoch: producer_epoch,
            };
        }

        let key = (topic.to_string(), partition);

        // Check for duplicate against committed sequences
        if let Some(&last_seq) = state.last_sequence.get(&key) {
            if first_sequence <= last_seq {
                self.stats
                    .duplicates_rejected
                    .fetch_add(1, Ordering::Relaxed);
                debug!(
                    producer_id,
                    topic,
                    partition,
                    first_sequence,
                    last_committed = last_seq,
                    "Duplicate batch detected"
                );
                return DeduplicationResult::Duplicate {
                    existing_offset: last_seq as i64,
                };
            }

            // Out-of-order: sequence must be exactly last_seq + 1
            let expected = last_seq + 1;
            if first_sequence != expected {
                self.stats
                    .out_of_order_rejected
                    .fetch_add(1, Ordering::Relaxed);
                debug!(
                    producer_id,
                    topic,
                    partition,
                    expected,
                    received = first_sequence,
                    "Out-of-order batch detected"
                );
                return DeduplicationResult::OutOfOrder {
                    expected,
                    received: first_sequence,
                };
            }
        } else {
            // First batch for this (topic, partition) — sequence must be 0
            if first_sequence != 0 {
                self.stats
                    .out_of_order_rejected
                    .fetch_add(1, Ordering::Relaxed);
                return DeduplicationResult::OutOfOrder {
                    expected: 0,
                    received: first_sequence,
                };
            }
        }

        DeduplicationResult::Accept {
            next_sequence: first_sequence + 1,
        }
    }

    /// Commit a sequence number after a batch has been durably written.
    ///
    /// Updates the last committed sequence for the (producer, topic, partition)
    /// and prunes in-flight batches that are now committed.
    pub fn commit_sequence(
        &self,
        producer_id: i64,
        topic: &str,
        partition: i32,
        last_sequence: i32,
    ) -> Result<()> {
        let mut producers = self.producers.write().expect("producers lock poisoned");

        let state = producers.get_mut(&producer_id).ok_or_else(|| {
            StreamlineError::Protocol(format!(
                "Unknown producer ID {} during commit_sequence",
                producer_id
            ))
        })?;

        let key = (topic.to_string(), partition);
        state.last_sequence.insert(key.clone(), last_sequence);

        // Prune committed in-flight batches
        state.in_flight_batches.retain(|b| {
            !(b.topic == topic && b.partition == partition && b.last_sequence <= last_sequence)
        });

        state.last_updated_at = current_time_ms();

        self.stats
            .sequences_committed
            .fetch_add(1, Ordering::Relaxed);

        debug!(
            producer_id,
            topic,
            partition,
            last_sequence,
            "Committed sequence"
        );
        Ok(())
    }

    /// Bump the epoch for a producer, fencing any older sessions.
    ///
    /// The epoch wraps around at `i16::MAX`.
    /// Returns the new epoch value.
    pub fn bump_epoch(&self, producer_id: i64) -> Result<i16> {
        let mut producers = self.producers.write().expect("producers lock poisoned");

        let state = producers.get_mut(&producer_id).ok_or_else(|| {
            StreamlineError::Protocol(format!(
                "Unknown producer ID {} during bump_epoch",
                producer_id
            ))
        })?;

        let new_epoch = if state.producer_epoch == i16::MAX {
            0
        } else {
            state.producer_epoch + 1
        };

        let old_epoch = state.producer_epoch;
        state.producer_epoch = new_epoch;
        // Reset sequence tracking on epoch change
        state.last_sequence.clear();
        state.in_flight_batches.clear();
        state.last_updated_at = current_time_ms();

        self.stats.epoch_fences.fetch_add(1, Ordering::Relaxed);

        info!(
            producer_id,
            old_epoch,
            new_epoch,
            "Producer epoch bumped"
        );
        Ok(new_epoch)
    }

    /// Expire producers that have not been updated within the configured TTL.
    ///
    /// Returns the number of producers removed.
    pub fn expire_producers(&self) -> usize {
        let now = current_time_ms();
        let expiry_ms = self.config.max_producer_id_expiry_ms;
        let mut removed = 0;

        {
            let mut producers = self.producers.write().expect("producers lock poisoned");
            producers.retain(|id, state| {
                let age = now.saturating_sub(state.last_updated_at);
                if age > expiry_ms {
                    debug!(producer_id = id, age_ms = age, "Expiring producer");
                    removed += 1;
                    false
                } else {
                    true
                }
            });
        }

        if removed > 0 {
            self.stats
                .active_producers
                .fetch_sub(removed as u64, Ordering::Relaxed);
            info!(removed, "Expired idle producers");
        }

        removed
    }

    /// Get a snapshot of the state for a specific producer.
    pub fn get_producer_state(&self, producer_id: i64) -> Option<ProducerState> {
        let producers = self.producers.read().expect("producers lock poisoned");
        producers.get(&producer_id).cloned()
    }

    /// Get a point-in-time snapshot of statistics.
    pub fn stats(&self) -> IdempotentStatsSnapshot {
        IdempotentStatsSnapshot {
            total_producers: self.stats.total_producers.load(Ordering::Relaxed),
            active_producers: self.stats.active_producers.load(Ordering::Relaxed),
            duplicates_rejected: self.stats.duplicates_rejected.load(Ordering::Relaxed),
            out_of_order_rejected: self.stats.out_of_order_rejected.load(Ordering::Relaxed),
            epoch_fences: self.stats.epoch_fences.load(Ordering::Relaxed),
            sequences_committed: self.stats.sequences_committed.load(Ordering::Relaxed),
        }
    }
}

/// Returns the current wall-clock time in milliseconds since the Unix epoch.
fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    fn default_manager() -> IdempotentProducerManager {
        IdempotentProducerManager::new(IdempotentConfig::default())
    }

    // ==================== Allocation Tests ====================

    #[test]
    fn test_allocate_producer_id_starts_at_base() {
        let mgr = default_manager();
        let (id, epoch) = mgr.allocate_producer_id();
        assert_eq!(id, PRODUCER_ID_BASE);
        assert_eq!(epoch, 0);
    }

    #[test]
    fn test_allocate_producer_id_monotonic() {
        let mgr = default_manager();
        let (id1, _) = mgr.allocate_producer_id();
        let (id2, _) = mgr.allocate_producer_id();
        let (id3, _) = mgr.allocate_producer_id();
        assert_eq!(id2, id1 + 1);
        assert_eq!(id3, id2 + 1);
    }

    #[test]
    fn test_allocate_producer_id_creates_state() {
        let mgr = default_manager();
        let (id, _) = mgr.allocate_producer_id();
        let state = mgr.get_producer_state(id).unwrap();
        assert_eq!(state.producer_id, id);
        assert_eq!(state.producer_epoch, 0);
        assert!(state.last_sequence.is_empty());
        assert!(state.in_flight_batches.is_empty());
        assert!(state.transactional_id.is_none());
    }

    #[test]
    fn test_allocate_producer_id_updates_stats() {
        let mgr = default_manager();
        mgr.allocate_producer_id();
        mgr.allocate_producer_id();
        let s = mgr.stats();
        assert_eq!(s.total_producers, 2);
        assert_eq!(s.active_producers, 2);
    }

    // ==================== Sequence Checking Tests ====================

    #[test]
    fn test_check_sequence_first_batch_accepted() {
        let mgr = default_manager();
        let (id, epoch) = mgr.allocate_producer_id();
        let result = mgr.check_sequence(id, epoch, "topic", 0, 0);
        assert_eq!(result, DeduplicationResult::Accept { next_sequence: 1 });
    }

    #[test]
    fn test_check_sequence_unknown_producer_accepted() {
        let mgr = default_manager();
        let result = mgr.check_sequence(9999, 0, "topic", 0, 0);
        assert_eq!(result, DeduplicationResult::Accept { next_sequence: 1 });
    }

    #[test]
    fn test_check_sequence_sequential_accepted() {
        let mgr = default_manager();
        let (id, epoch) = mgr.allocate_producer_id();
        mgr.commit_sequence(id, "topic", 0, 0).unwrap();
        let result = mgr.check_sequence(id, epoch, "topic", 0, 1);
        assert_eq!(result, DeduplicationResult::Accept { next_sequence: 2 });
    }

    // ==================== Duplicate Detection Tests ====================

    #[test]
    fn test_check_sequence_duplicate_detected() {
        let mgr = default_manager();
        let (id, epoch) = mgr.allocate_producer_id();
        mgr.commit_sequence(id, "topic", 0, 5).unwrap();

        let result = mgr.check_sequence(id, epoch, "topic", 0, 3);
        assert!(matches!(result, DeduplicationResult::Duplicate { .. }));
    }

    #[test]
    fn test_check_sequence_exact_duplicate_detected() {
        let mgr = default_manager();
        let (id, epoch) = mgr.allocate_producer_id();
        mgr.commit_sequence(id, "topic", 0, 5).unwrap();

        // Sending the same sequence as already committed
        let result = mgr.check_sequence(id, epoch, "topic", 0, 5);
        assert_eq!(
            result,
            DeduplicationResult::Duplicate {
                existing_offset: 5
            }
        );
    }

    #[test]
    fn test_duplicate_increments_stat() {
        let mgr = default_manager();
        let (id, epoch) = mgr.allocate_producer_id();
        mgr.commit_sequence(id, "topic", 0, 5).unwrap();
        mgr.check_sequence(id, epoch, "topic", 0, 3);
        assert_eq!(mgr.stats().duplicates_rejected, 1);
    }

    // ==================== Out-of-Order Rejection Tests ====================

    #[test]
    fn test_check_sequence_out_of_order_rejected() {
        let mgr = default_manager();
        let (id, epoch) = mgr.allocate_producer_id();
        mgr.commit_sequence(id, "topic", 0, 2).unwrap();

        // Expected: 3, sending: 10
        let result = mgr.check_sequence(id, epoch, "topic", 0, 10);
        assert_eq!(
            result,
            DeduplicationResult::OutOfOrder {
                expected: 3,
                received: 10
            }
        );
    }

    #[test]
    fn test_check_sequence_first_batch_must_be_zero() {
        let mgr = default_manager();
        let (id, epoch) = mgr.allocate_producer_id();

        let result = mgr.check_sequence(id, epoch, "topic", 0, 5);
        assert_eq!(
            result,
            DeduplicationResult::OutOfOrder {
                expected: 0,
                received: 5
            }
        );
    }

    #[test]
    fn test_out_of_order_increments_stat() {
        let mgr = default_manager();
        let (id, epoch) = mgr.allocate_producer_id();
        mgr.commit_sequence(id, "topic", 0, 2).unwrap();
        mgr.check_sequence(id, epoch, "topic", 0, 10);
        assert_eq!(mgr.stats().out_of_order_rejected, 1);
    }

    // ==================== Epoch Fencing Tests ====================

    #[test]
    fn test_check_sequence_stale_epoch_fenced() {
        let mgr = default_manager();
        let (id, _) = mgr.allocate_producer_id();
        mgr.bump_epoch(id).unwrap();

        // Attempt with epoch 0 (stale)
        let result = mgr.check_sequence(id, 0, "topic", 0, 0);
        assert_eq!(
            result,
            DeduplicationResult::InvalidEpoch {
                current_epoch: 1,
                received_epoch: 0
            }
        );
    }

    #[test]
    fn test_bump_epoch_resets_sequences() {
        let mgr = default_manager();
        let (id, epoch) = mgr.allocate_producer_id();
        mgr.commit_sequence(id, "topic", 0, 5).unwrap();

        let new_epoch = mgr.bump_epoch(id).unwrap();
        assert_eq!(new_epoch, epoch + 1);

        let state = mgr.get_producer_state(id).unwrap();
        assert!(state.last_sequence.is_empty());
        assert!(state.in_flight_batches.is_empty());
    }

    #[test]
    fn test_bump_epoch_wraps_at_max() {
        let mgr = default_manager();
        let (id, _) = mgr.allocate_producer_id();

        // Set epoch to MAX
        {
            let mut producers = mgr.producers.write().unwrap();
            producers.get_mut(&id).unwrap().producer_epoch = i16::MAX;
        }

        let new_epoch = mgr.bump_epoch(id).unwrap();
        assert_eq!(new_epoch, 0);
    }

    #[test]
    fn test_epoch_fence_disabled_allows_stale() {
        let config = IdempotentConfig {
            enable_epoch_fencing: false,
            ..Default::default()
        };
        let mgr = IdempotentProducerManager::new(config);
        let (id, _) = mgr.allocate_producer_id();
        mgr.bump_epoch(id).unwrap();

        // Stale epoch should be accepted when fencing disabled
        let result = mgr.check_sequence(id, 0, "topic", 0, 0);
        assert_eq!(result, DeduplicationResult::Accept { next_sequence: 1 });
    }

    #[test]
    fn test_epoch_fence_increments_stat() {
        let mgr = default_manager();
        let (id, _) = mgr.allocate_producer_id();
        mgr.bump_epoch(id).unwrap();
        mgr.check_sequence(id, 0, "topic", 0, 0);
        assert_eq!(mgr.stats().epoch_fences, 2); // 1 from bump + 1 from check
    }

    // ==================== Commit Sequence Tests ====================

    #[test]
    fn test_commit_sequence_updates_state() {
        let mgr = default_manager();
        let (id, _) = mgr.allocate_producer_id();
        mgr.commit_sequence(id, "topic-a", 0, 4).unwrap();

        let state = mgr.get_producer_state(id).unwrap();
        assert_eq!(
            state.last_sequence.get(&("topic-a".to_string(), 0)),
            Some(&4)
        );
    }

    #[test]
    fn test_commit_sequence_unknown_producer_errors() {
        let mgr = default_manager();
        let result = mgr.commit_sequence(9999, "topic", 0, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_commit_sequence_prunes_in_flight() {
        let mgr = default_manager();
        let (id, _) = mgr.allocate_producer_id();

        // Add in-flight batches manually
        {
            let mut producers = mgr.producers.write().unwrap();
            let state = producers.get_mut(&id).unwrap();
            state.in_flight_batches.push(InFlightBatch {
                first_sequence: 0,
                last_sequence: 4,
                topic: "topic".to_string(),
                partition: 0,
                timestamp_ms: current_time_ms(),
            });
            state.in_flight_batches.push(InFlightBatch {
                first_sequence: 5,
                last_sequence: 9,
                topic: "topic".to_string(),
                partition: 0,
                timestamp_ms: current_time_ms(),
            });
        }

        mgr.commit_sequence(id, "topic", 0, 4).unwrap();

        let state = mgr.get_producer_state(id).unwrap();
        assert_eq!(state.in_flight_batches.len(), 1);
        assert_eq!(state.in_flight_batches[0].first_sequence, 5);
    }

    #[test]
    fn test_commit_sequence_increments_stat() {
        let mgr = default_manager();
        let (id, _) = mgr.allocate_producer_id();
        mgr.commit_sequence(id, "topic", 0, 0).unwrap();
        mgr.commit_sequence(id, "topic", 0, 1).unwrap();
        assert_eq!(mgr.stats().sequences_committed, 2);
    }

    // ==================== Expiry Tests ====================

    #[test]
    fn test_expire_producers_removes_stale() {
        let config = IdempotentConfig {
            max_producer_id_expiry_ms: 0, // expire immediately
            ..Default::default()
        };
        let mgr = IdempotentProducerManager::new(config);
        let (id, _) = mgr.allocate_producer_id();

        // Producer was just created with last_updated_at = now
        // With expiry_ms = 0, any age > 0 expires
        // Force the timestamp back
        {
            let mut producers = mgr.producers.write().unwrap();
            producers.get_mut(&id).unwrap().last_updated_at = 0;
        }

        let removed = mgr.expire_producers();
        assert_eq!(removed, 1);
        assert!(mgr.get_producer_state(id).is_none());
    }

    #[test]
    fn test_expire_producers_keeps_active() {
        let mgr = default_manager();
        mgr.allocate_producer_id();

        let removed = mgr.expire_producers();
        assert_eq!(removed, 0);
    }

    #[test]
    fn test_expire_producers_updates_stats() {
        let config = IdempotentConfig {
            max_producer_id_expiry_ms: 0,
            ..Default::default()
        };
        let mgr = IdempotentProducerManager::new(config);
        mgr.allocate_producer_id();
        mgr.allocate_producer_id();

        // Force timestamps back
        {
            let mut producers = mgr.producers.write().unwrap();
            for state in producers.values_mut() {
                state.last_updated_at = 0;
            }
        }

        mgr.expire_producers();
        assert_eq!(mgr.stats().active_producers, 0);
    }

    // ==================== Multi-Partition Tests ====================

    #[test]
    fn test_sequences_independent_per_partition() {
        let mgr = default_manager();
        let (id, epoch) = mgr.allocate_producer_id();

        mgr.commit_sequence(id, "topic", 0, 5).unwrap();
        mgr.commit_sequence(id, "topic", 1, 10).unwrap();

        // Partition 0: next expected is 6
        let r0 = mgr.check_sequence(id, epoch, "topic", 0, 6);
        assert_eq!(r0, DeduplicationResult::Accept { next_sequence: 7 });

        // Partition 1: next expected is 11
        let r1 = mgr.check_sequence(id, epoch, "topic", 1, 11);
        assert_eq!(r1, DeduplicationResult::Accept { next_sequence: 12 });
    }

    #[test]
    fn test_sequences_independent_per_topic() {
        let mgr = default_manager();
        let (id, epoch) = mgr.allocate_producer_id();

        mgr.commit_sequence(id, "orders", 0, 3).unwrap();
        mgr.commit_sequence(id, "events", 0, 7).unwrap();

        let r = mgr.check_sequence(id, epoch, "orders", 0, 4);
        assert_eq!(r, DeduplicationResult::Accept { next_sequence: 5 });

        let r = mgr.check_sequence(id, epoch, "events", 0, 8);
        assert_eq!(r, DeduplicationResult::Accept { next_sequence: 9 });
    }

    // ==================== Concurrent Access Tests ====================

    #[test]
    fn test_concurrent_allocate_ids_are_unique() {
        let mgr = Arc::new(default_manager());
        let mut handles = vec![];

        for _ in 0..10 {
            let mgr = Arc::clone(&mgr);
            handles.push(thread::spawn(move || mgr.allocate_producer_id().0));
        }

        let mut ids: Vec<i64> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), 10, "All producer IDs must be unique");
    }

    #[test]
    fn test_concurrent_check_and_commit() {
        let mgr = Arc::new(default_manager());
        let (id, epoch) = mgr.allocate_producer_id();
        let mut handles = vec![];

        for i in 0..5 {
            let mgr = Arc::clone(&mgr);
            handles.push(thread::spawn(move || {
                let topic = format!("topic-{}", i);
                let check = mgr.check_sequence(id, epoch, &topic, 0, 0);
                assert_eq!(
                    check,
                    DeduplicationResult::Accept { next_sequence: 1 }
                );
                mgr.commit_sequence(id, &topic, 0, 0).unwrap();
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let state = mgr.get_producer_state(id).unwrap();
        assert_eq!(state.last_sequence.len(), 5);
    }

    // ==================== Config Tests ====================

    #[test]
    fn test_default_config_values() {
        let config = IdempotentConfig::default();
        assert_eq!(config.max_producer_id_expiry_ms, 604_800_000);
        assert_eq!(config.max_in_flight_requests, 5);
        assert_eq!(config.transactional_id_expiry_ms, 604_800_000);
        assert!(config.enable_epoch_fencing);
    }

    // ==================== Stats Snapshot Tests ====================

    #[test]
    fn test_stats_snapshot_initial() {
        let mgr = default_manager();
        let s = mgr.stats();
        assert_eq!(
            s,
            IdempotentStatsSnapshot {
                total_producers: 0,
                active_producers: 0,
                duplicates_rejected: 0,
                out_of_order_rejected: 0,
                epoch_fences: 0,
                sequences_committed: 0,
            }
        );
    }

    // ==================== End-to-End Workflow Tests ====================

    #[test]
    fn test_full_produce_lifecycle() {
        let mgr = default_manager();
        let (id, epoch) = mgr.allocate_producer_id();

        // Batch 1: seq 0
        let r = mgr.check_sequence(id, epoch, "orders", 0, 0);
        assert_eq!(r, DeduplicationResult::Accept { next_sequence: 1 });
        mgr.commit_sequence(id, "orders", 0, 0).unwrap();

        // Batch 2: seq 1
        let r = mgr.check_sequence(id, epoch, "orders", 0, 1);
        assert_eq!(r, DeduplicationResult::Accept { next_sequence: 2 });
        mgr.commit_sequence(id, "orders", 0, 1).unwrap();

        // Retry batch 1 (duplicate)
        let r = mgr.check_sequence(id, epoch, "orders", 0, 0);
        assert!(matches!(r, DeduplicationResult::Duplicate { .. }));

        // Skip batch 3 (out of order)
        let r = mgr.check_sequence(id, epoch, "orders", 0, 5);
        assert!(matches!(r, DeduplicationResult::OutOfOrder { .. }));

        // Bump epoch and start over
        let new_epoch = mgr.bump_epoch(id).unwrap();
        let r = mgr.check_sequence(id, new_epoch, "orders", 0, 0);
        assert_eq!(r, DeduplicationResult::Accept { next_sequence: 1 });

        let s = mgr.stats();
        assert_eq!(s.total_producers, 1);
        assert_eq!(s.sequences_committed, 2);
        assert_eq!(s.duplicates_rejected, 1);
        assert_eq!(s.out_of_order_rejected, 1);
    }

    #[test]
    fn test_bump_epoch_unknown_producer_errors() {
        let mgr = default_manager();
        let result = mgr.bump_epoch(9999);
        assert!(result.is_err());
    }

    #[test]
    fn test_get_producer_state_nonexistent() {
        let mgr = default_manager();
        assert!(mgr.get_producer_state(42).is_none());
    }
}
