//! CRDT State Manager
//!
//! Manages CRDT state across topics and partitions, providing:
//! - Merged state queries
//! - Real-time state subscriptions
//! - Causal delivery ordering

use super::clock::{CausalTimestamp, VectorClock};
use super::record::CrdtRecord;
use super::types::CrdtValue;
use crate::error::{Result, StreamlineError};
use bytes::Bytes;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};

/// Configuration for CRDT state management
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrdtStateConfig {
    /// Maximum cached states per topic-partition
    pub max_cached_states: usize,
    /// Enable causal delivery ordering
    pub causal_delivery: bool,
    /// Maximum pending messages for causal ordering
    pub max_pending_causal: usize,
    /// State change broadcast buffer size
    pub broadcast_buffer_size: usize,
    /// Enable delta state optimization
    pub enable_delta: bool,
}

impl Default for CrdtStateConfig {
    fn default() -> Self {
        Self {
            max_cached_states: 10000,
            causal_delivery: false, // Default to false for simpler usage
            max_pending_causal: 1000,
            broadcast_buffer_size: 1024,
            enable_delta: true,
        }
    }
}

/// Key for state lookup (topic, partition, record key)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StateKey {
    pub topic: String,
    pub partition: i32,
    pub key: Bytes,
}

impl StateKey {
    /// Create a new state key
    pub fn new(topic: impl Into<String>, partition: i32, key: impl Into<Bytes>) -> Self {
        Self {
            topic: topic.into(),
            partition,
            key: key.into(),
        }
    }
}

/// Merged CRDT state
#[derive(Debug, Clone)]
pub struct MergedState {
    /// The merged CRDT value
    pub value: CrdtValue,
    /// Last update timestamp
    pub last_updated: CausalTimestamp,
    /// Merged vector clock (union of all contributing clocks)
    pub merged_clock: VectorClock,
    /// Number of records merged
    pub merge_count: u64,
    /// Last source offset
    pub last_offset: i64,
}

impl MergedState {
    /// Create initial state from a record
    pub fn from_record(record: &CrdtRecord, offset: i64) -> Self {
        Self {
            value: record.value.clone(),
            last_updated: record.metadata.timestamp.clone(),
            merged_clock: record.metadata.timestamp.vector_clock.clone(),
            merge_count: 1,
            last_offset: offset,
        }
    }

    /// Merge another record into this state
    pub fn merge(&mut self, record: &CrdtRecord, offset: i64) -> Result<()> {
        self.value.merge(&record.value)?;
        self.merged_clock
            .merge(&record.metadata.timestamp.vector_clock);

        // Update timestamp if newer
        if record.metadata.timestamp > self.last_updated {
            self.last_updated = record.metadata.timestamp.clone();
        }

        self.merge_count += 1;
        self.last_offset = self.last_offset.max(offset);
        Ok(())
    }
}

/// State change notification
#[derive(Debug, Clone)]
pub struct StateChange {
    /// The state key
    pub key: StateKey,
    /// Old value (if any)
    pub old_value: Option<CrdtValue>,
    /// New value
    pub new_value: CrdtValue,
    /// Change timestamp
    pub timestamp: CausalTimestamp,
    /// Source offset
    pub offset: i64,
}

/// Pending message for causal ordering
struct PendingMessage {
    record: CrdtRecord,
    offset: i64,
    dependencies: VectorClock,
}

/// Per-partition causal ordering state
struct CausalOrderState {
    /// Delivered vector clock
    delivered_clock: VectorClock,
    /// Pending messages waiting for dependencies
    pending: VecDeque<PendingMessage>,
}

impl CausalOrderState {
    fn new() -> Self {
        Self {
            delivered_clock: VectorClock::new(),
            pending: VecDeque::new(),
        }
    }
}

/// CRDT State Manager
///
/// Manages CRDT state across the system, providing merged state queries
/// and real-time subscriptions.
pub struct CrdtStateManager {
    /// Configuration
    config: CrdtStateConfig,
    /// Cached merged states
    states: DashMap<StateKey, MergedState>,
    /// State change broadcaster
    broadcaster: broadcast::Sender<StateChange>,
    /// Per-partition causal ordering
    causal_order: DashMap<(String, i32), RwLock<CausalOrderState>>,
    /// Statistics
    stats: CrdtStateStats,
}

/// Statistics for CRDT state operations
#[derive(Debug, Default)]
struct CrdtStateStats {
    /// Total states cached
    cached_states: AtomicU64,
    /// Total merges performed
    merges: AtomicU64,
    /// Cache hits
    cache_hits: AtomicU64,
    /// Cache misses
    cache_misses: AtomicU64,
    /// Causal ordering delays
    causal_delays: AtomicU64,
    /// State change notifications sent
    notifications_sent: AtomicU64,
}

/// Snapshot of state manager statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrdtStateStatsSnapshot {
    pub cached_states: u64,
    pub merges: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub causal_delays: u64,
    pub notifications_sent: u64,
}

impl CrdtStateManager {
    /// Create a new CRDT state manager
    pub fn new(config: CrdtStateConfig) -> Self {
        let (broadcaster, _) = broadcast::channel(config.broadcast_buffer_size);

        Self {
            config,
            states: DashMap::new(),
            broadcaster,
            causal_order: DashMap::new(),
            stats: CrdtStateStats::default(),
        }
    }

    /// Create with default configuration
    pub fn default_manager() -> Self {
        Self::new(CrdtStateConfig::default())
    }

    /// Process a CRDT record update
    pub async fn process_record(
        &self,
        topic: &str,
        partition: i32,
        record: &CrdtRecord,
        offset: i64,
    ) -> Result<()> {
        // Apply causal ordering if enabled
        if self.config.causal_delivery {
            self.apply_causal_ordering(topic, partition, record, offset)
                .await?;
        } else {
            self.apply_update(topic, partition, record, offset).await?;
        }

        Ok(())
    }

    /// Apply causal ordering to a record
    async fn apply_causal_ordering(
        &self,
        topic: &str,
        partition: i32,
        record: &CrdtRecord,
        offset: i64,
    ) -> Result<()> {
        let key = (topic.to_string(), partition);

        // Get or create causal order state
        let state = self
            .causal_order
            .entry(key.clone())
            .or_insert_with(|| RwLock::new(CausalOrderState::new()));

        let mut state = state.write().await;

        // Check if dependencies are satisfied
        let deps = &record.metadata.timestamp.vector_clock;

        if state.delivered_clock.satisfies(deps) {
            // Dependencies satisfied, deliver immediately
            drop(state);
            self.apply_update(topic, partition, record, offset).await?;

            // Try to deliver pending messages
            self.try_deliver_pending(topic, partition).await?;
        } else {
            // Add to pending queue
            if state.pending.len() >= self.config.max_pending_causal {
                return Err(StreamlineError::storage_msg(
                    "Causal ordering queue full".into(),
                ));
            }

            state.pending.push_back(PendingMessage {
                record: record.clone(),
                offset,
                dependencies: deps.clone(),
            });

            self.stats.causal_delays.fetch_add(1, Ordering::Relaxed);
        }

        Ok(())
    }

    /// Try to deliver pending messages after a delivery
    async fn try_deliver_pending(&self, topic: &str, partition: i32) -> Result<()> {
        let key = (topic.to_string(), partition);

        loop {
            let state = self.causal_order.get(&key);
            let state = match state {
                Some(s) => s,
                None => return Ok(()),
            };

            let mut state = state.write().await;

            // Find a deliverable message
            let deliverable = state
                .pending
                .iter()
                .position(|msg| state.delivered_clock.satisfies(&msg.dependencies));

            match deliverable {
                Some(idx) => {
                    let Some(msg) = state.pending.remove(idx) else {
                        return Ok(());
                    };
                    drop(state);

                    self.apply_update(topic, partition, &msg.record, msg.offset)
                        .await?;
                }
                None => return Ok(()),
            }
        }
    }

    /// Apply a CRDT update to state
    async fn apply_update(
        &self,
        topic: &str,
        partition: i32,
        record: &CrdtRecord,
        offset: i64,
    ) -> Result<()> {
        let key = StateKey::new(
            topic,
            partition,
            record
                .key
                .clone()
                .unwrap_or_else(|| Bytes::from_static(b"")),
        );

        // Get old value for notification
        let old_value = self.states.get(&key).map(|s| s.value.clone());

        // Update or insert state
        match self.states.get_mut(&key) {
            Some(mut state) => {
                state.merge(record, offset)?;
                self.stats.merges.fetch_add(1, Ordering::Relaxed);
            }
            None => {
                self.states
                    .insert(key.clone(), MergedState::from_record(record, offset));
                self.stats.cached_states.fetch_add(1, Ordering::Relaxed);
            }
        }

        // Update causal delivery clock
        if self.config.causal_delivery {
            let partition_key = (topic.to_string(), partition);
            if let Some(state) = self.causal_order.get(&partition_key) {
                let mut state = state.write().await;
                state
                    .delivered_clock
                    .merge(&record.metadata.timestamp.vector_clock);
            }
        }

        // Send notification
        let change = StateChange {
            key,
            old_value,
            new_value: record.value.clone(),
            timestamp: record.metadata.timestamp.clone(),
            offset,
        };

        let _ = self.broadcaster.send(change);
        self.stats
            .notifications_sent
            .fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Get merged state for a key
    pub fn get_merged_state(
        &self,
        topic: &str,
        partition: i32,
        key: &Bytes,
    ) -> Option<MergedState> {
        let state_key = StateKey::new(topic, partition, key.clone());

        match self.states.get(&state_key) {
            Some(state) => {
                self.stats.cache_hits.fetch_add(1, Ordering::Relaxed);
                Some(state.clone())
            }
            None => {
                self.stats.cache_misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Get all keys for a topic-partition
    pub fn keys(&self, topic: &str, partition: i32) -> Vec<Bytes> {
        self.states
            .iter()
            .filter(|entry| entry.key().topic == topic && entry.key().partition == partition)
            .map(|entry| entry.key().key.clone())
            .collect()
    }

    /// Subscribe to state changes
    pub fn subscribe(&self) -> broadcast::Receiver<StateChange> {
        self.broadcaster.subscribe()
    }

    /// Subscribe to state changes for a specific key
    pub fn subscribe_key(&self, topic: String, partition: i32, key: Bytes) -> KeySubscription {
        let rx = self.broadcaster.subscribe();
        KeySubscription {
            key: StateKey::new(topic, partition, key),
            rx,
        }
    }

    /// Get statistics snapshot
    pub fn stats(&self) -> CrdtStateStatsSnapshot {
        CrdtStateStatsSnapshot {
            cached_states: self.stats.cached_states.load(Ordering::Relaxed),
            merges: self.stats.merges.load(Ordering::Relaxed),
            cache_hits: self.stats.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.stats.cache_misses.load(Ordering::Relaxed),
            causal_delays: self.stats.causal_delays.load(Ordering::Relaxed),
            notifications_sent: self.stats.notifications_sent.load(Ordering::Relaxed),
        }
    }

    /// Clear all cached state
    pub fn clear(&self) {
        self.states.clear();
        self.causal_order.clear();
    }

    /// Remove state for a specific key
    pub fn remove(&self, topic: &str, partition: i32, key: &Bytes) {
        let state_key = StateKey::new(topic, partition, key.clone());
        self.states.remove(&state_key);
    }

    /// Get the number of cached states
    pub fn len(&self) -> usize {
        self.states.len()
    }

    /// Check if the cache is empty
    pub fn is_empty(&self) -> bool {
        self.states.is_empty()
    }
}

/// Subscription for a specific key
pub struct KeySubscription {
    key: StateKey,
    rx: broadcast::Receiver<StateChange>,
}

impl KeySubscription {
    /// Receive the next state change for this key
    pub async fn recv(&mut self) -> Result<StateChange> {
        loop {
            match self.rx.recv().await {
                Ok(change) if change.key == self.key => return Ok(change),
                Ok(_) => continue, // Different key, skip
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!("Subscription lagged by {} messages", n);
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => {
                    return Err(StreamlineError::storage_msg("Subscription closed".into()))
                }
            }
        }
    }

    /// Get the key this subscription is for
    pub fn key(&self) -> &StateKey {
        &self.key
    }
}

/// CRDT-aware consumer for causal delivery
pub struct CausalConsumer {
    /// Topic being consumed
    topic: String,
    /// Partition being consumed
    partition: i32,
    /// State manager
    state_manager: Arc<CrdtStateManager>,
    /// Local delivered clock
    delivered_clock: VectorClock,
}

impl CausalConsumer {
    /// Create a new causal consumer
    pub fn new(
        topic: impl Into<String>,
        partition: i32,
        state_manager: Arc<CrdtStateManager>,
    ) -> Self {
        Self {
            topic: topic.into(),
            partition,
            state_manager,
            delivered_clock: VectorClock::new(),
        }
    }

    /// Process a record with causal ordering
    pub async fn process(&mut self, record: &CrdtRecord, offset: i64) -> Result<()> {
        self.state_manager
            .process_record(&self.topic, self.partition, record, offset)
            .await
    }

    /// Get the current delivered clock
    pub fn delivered_clock(&self) -> &VectorClock {
        &self.delivered_clock
    }

    /// Get merged state for a key
    pub fn get_state(&self, key: &Bytes) -> Option<MergedState> {
        self.state_manager
            .get_merged_state(&self.topic, self.partition, key)
    }

    /// Subscribe to state changes
    pub fn subscribe(&self) -> broadcast::Receiver<StateChange> {
        self.state_manager.subscribe()
    }
}

/// Helper to create CRDT state from multiple partitions
pub struct MultiPartitionState {
    /// States per partition
    states: HashMap<i32, MergedState>,
}

impl MultiPartitionState {
    /// Create empty multi-partition state
    pub fn new() -> Self {
        Self {
            states: HashMap::new(),
        }
    }

    /// Add state from a partition
    pub fn add(&mut self, partition: i32, state: MergedState) {
        self.states.insert(partition, state);
    }

    /// Merge all partition states into one
    pub fn merge_all(&self) -> Result<Option<CrdtValue>> {
        let mut merged: Option<CrdtValue> = None;

        for state in self.states.values() {
            match &mut merged {
                Some(m) => m.merge(&state.value)?,
                None => merged = Some(state.value.clone()),
            }
        }

        Ok(merged)
    }

    /// Get the state for a specific partition
    pub fn get(&self, partition: i32) -> Option<&MergedState> {
        self.states.get(&partition)
    }

    /// Get all partitions with state
    pub fn partitions(&self) -> Vec<i32> {
        self.states.keys().copied().collect()
    }
}

impl Default for MultiPartitionState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::super::types::{CrdtOperation, GCounter};
    use super::*;

    #[tokio::test]
    async fn test_state_manager_basic() {
        let manager = CrdtStateManager::default_manager();

        let mut counter = GCounter::new("node1");
        counter.increment(42);

        let record = CrdtRecord::new(
            Some(Bytes::from("key1")),
            CrdtValue::GCounter(counter),
            "node1",
            CrdtOperation::State,
        );

        manager
            .process_record("topic", 0, &record, 1)
            .await
            .unwrap();

        let state = manager
            .get_merged_state("topic", 0, &Bytes::from("key1"))
            .unwrap();

        match state.value {
            CrdtValue::GCounter(c) => assert_eq!(c.value(), 42),
            _ => panic!("Expected GCounter"),
        }
    }

    #[tokio::test]
    async fn test_state_merge() {
        let manager = CrdtStateManager::default_manager();

        // First update
        let mut counter1 = GCounter::new("node1");
        counter1.increment(10);
        let record1 = CrdtRecord::new(
            Some(Bytes::from("key1")),
            CrdtValue::GCounter(counter1),
            "node1",
            CrdtOperation::State,
        );

        manager
            .process_record("topic", 0, &record1, 1)
            .await
            .unwrap();

        // Second update from different node
        let mut counter2 = GCounter::new("node2");
        counter2.increment(20);
        let record2 = CrdtRecord::new(
            Some(Bytes::from("key1")),
            CrdtValue::GCounter(counter2),
            "node2",
            CrdtOperation::State,
        );

        manager
            .process_record("topic", 0, &record2, 2)
            .await
            .unwrap();

        // Check merged value
        let state = manager
            .get_merged_state("topic", 0, &Bytes::from("key1"))
            .unwrap();

        match state.value {
            CrdtValue::GCounter(c) => assert_eq!(c.value(), 30),
            _ => panic!("Expected GCounter"),
        }

        assert_eq!(state.merge_count, 2);
    }

    #[tokio::test]
    async fn test_state_subscription() {
        let manager = Arc::new(CrdtStateManager::default_manager());
        let mut rx = manager.subscribe();

        // Process a record
        let mut counter = GCounter::new("node1");
        counter.increment(42);
        let record = CrdtRecord::new(
            Some(Bytes::from("key1")),
            CrdtValue::GCounter(counter),
            "node1",
            CrdtOperation::State,
        );

        manager
            .process_record("topic", 0, &record, 1)
            .await
            .unwrap();

        // Receive notification
        let change = rx.try_recv().unwrap();
        assert_eq!(change.key.topic, "topic");
        assert_eq!(change.key.partition, 0);
        assert_eq!(change.key.key, Bytes::from("key1"));
    }

    #[tokio::test]
    async fn test_multi_partition_merge() {
        let mut mp_state = MultiPartitionState::new();

        // Create states for two partitions
        let mut counter1 = GCounter::new("node1");
        counter1.increment(10);
        let record1 = CrdtRecord::new(
            Some(Bytes::from("key1")),
            CrdtValue::GCounter(counter1),
            "node1",
            CrdtOperation::State,
        );

        let mut counter2 = GCounter::new("node2");
        counter2.increment(20);
        let record2 = CrdtRecord::new(
            Some(Bytes::from("key1")),
            CrdtValue::GCounter(counter2),
            "node2",
            CrdtOperation::State,
        );

        mp_state.add(0, MergedState::from_record(&record1, 1));
        mp_state.add(1, MergedState::from_record(&record2, 2));

        // Merge all partitions
        let merged = mp_state.merge_all().unwrap().unwrap();
        match merged {
            CrdtValue::GCounter(c) => assert_eq!(c.value(), 30),
            _ => panic!("Expected GCounter"),
        }
    }

    #[test]
    fn test_stats() {
        let manager = CrdtStateManager::default_manager();
        let stats = manager.stats();

        assert_eq!(stats.cached_states, 0);
        assert_eq!(stats.merges, 0);
    }
}
