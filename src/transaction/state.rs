//! Transaction state structures
//!
//! Defines the state machine and data structures for tracking
//! transaction lifecycle and associated partitions/offsets.

use crate::storage::{ProducerEpoch, ProducerId};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::time::Instant;

/// Transaction state machine states
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionState {
    /// Transaction has been started but not yet prepared
    Ongoing,
    /// Transaction is being prepared for commit (two-phase commit)
    PrepareCommit,
    /// Transaction is being prepared for abort
    PrepareAbort,
    /// Transaction has been committed
    CompleteCommit,
    /// Transaction has been aborted
    CompleteAbort,
    /// Transaction is dead (expired or errored)
    Dead,
}

impl TransactionState {
    /// Check if this state allows adding partitions
    pub fn can_add_partitions(&self) -> bool {
        matches!(self, TransactionState::Ongoing)
    }

    /// Check if this state allows ending the transaction
    pub fn can_end_transaction(&self) -> bool {
        matches!(self, TransactionState::Ongoing)
    }

    /// Check if this is a terminal state
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            TransactionState::CompleteCommit
                | TransactionState::CompleteAbort
                | TransactionState::Dead
        )
    }

    /// Check if this is a preparing state
    pub fn is_preparing(&self) -> bool {
        matches!(
            self,
            TransactionState::PrepareCommit | TransactionState::PrepareAbort
        )
    }
}

impl std::fmt::Display for TransactionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransactionState::Ongoing => write!(f, "Ongoing"),
            TransactionState::PrepareCommit => write!(f, "PrepareCommit"),
            TransactionState::PrepareAbort => write!(f, "PrepareAbort"),
            TransactionState::CompleteCommit => write!(f, "CompleteCommit"),
            TransactionState::CompleteAbort => write!(f, "CompleteAbort"),
            TransactionState::Dead => write!(f, "Dead"),
        }
    }
}

/// A topic-partition participating in a transaction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionPartition {
    /// Topic name
    pub topic: String,
    /// Partition index
    pub partition: i32,
    /// Base offset of first record written in this partition for this transaction
    /// Used for READ_COMMITTED isolation level to compute last_stable_offset
    #[serde(default)]
    pub base_offset: Option<i64>,
}

impl PartialEq for TransactionPartition {
    fn eq(&self, other: &Self) -> bool {
        // Equality based only on topic and partition, not offset
        self.topic == other.topic && self.partition == other.partition
    }
}

impl Eq for TransactionPartition {}

impl std::hash::Hash for TransactionPartition {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Hash based only on topic and partition
        self.topic.hash(state);
        self.partition.hash(state);
    }
}

impl TransactionPartition {
    pub fn new(topic: impl Into<String>, partition: i32) -> Self {
        Self {
            topic: topic.into(),
            partition,
            base_offset: None,
        }
    }

    /// Create a new partition with a base offset
    pub fn with_base_offset(topic: impl Into<String>, partition: i32, base_offset: i64) -> Self {
        Self {
            topic: topic.into(),
            partition,
            base_offset: Some(base_offset),
        }
    }

    /// Set the base offset if not already set
    pub fn set_base_offset(&mut self, offset: i64) {
        if self.base_offset.is_none() {
            self.base_offset = Some(offset);
        }
    }
}

/// Consumer group offsets to commit as part of transaction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingTxnOffset {
    /// Consumer group ID
    pub group_id: String,
    /// Topic name
    pub topic: String,
    /// Partition index
    pub partition: i32,
    /// Offset to commit
    pub offset: i64,
    /// Optional metadata
    pub metadata: Option<String>,
}

/// Represents an active transaction
#[derive(Debug, Clone)]
pub struct Transaction {
    /// The transactional ID
    pub transactional_id: String,
    /// The producer ID associated with this transaction
    pub producer_id: ProducerId,
    /// The producer epoch
    pub producer_epoch: ProducerEpoch,
    /// Current transaction state
    pub state: TransactionState,
    /// Partitions participating in this transaction
    pub partitions: HashSet<TransactionPartition>,
    /// Consumer group IDs that have pending offset commits
    pub pending_offset_groups: HashSet<String>,
    /// Pending offset commits (group_id -> (topic, partition) -> offset)
    pub pending_offsets: HashMap<String, HashMap<(String, i32), PendingTxnOffset>>,
    /// Transaction timeout in milliseconds
    pub timeout_ms: i64,
    /// Timestamp when transaction started
    #[allow(dead_code)]
    started_at: Instant,
    /// Timestamp of last update
    pub last_update: Instant,
}

impl Transaction {
    /// Create a new transaction
    pub fn new(
        transactional_id: String,
        producer_id: ProducerId,
        producer_epoch: ProducerEpoch,
        timeout_ms: i64,
    ) -> Self {
        let now = Instant::now();
        Self {
            transactional_id,
            producer_id,
            producer_epoch,
            state: TransactionState::Ongoing,
            partitions: HashSet::new(),
            pending_offset_groups: HashSet::new(),
            pending_offsets: HashMap::new(),
            timeout_ms,
            started_at: now,
            last_update: now,
        }
    }

    /// Add a partition to the transaction
    pub fn add_partition(&mut self, topic: String, partition: i32) -> bool {
        if !self.state.can_add_partitions() {
            return false;
        }
        self.partitions
            .insert(TransactionPartition::new(topic, partition));
        self.last_update = Instant::now();
        true
    }

    /// Add a consumer group for offset commits
    pub fn add_offset_group(&mut self, group_id: String) -> bool {
        if !self.state.can_add_partitions() {
            return false;
        }
        self.pending_offset_groups.insert(group_id);
        self.last_update = Instant::now();
        true
    }

    /// Add a pending offset commit
    pub fn add_pending_offset(
        &mut self,
        group_id: String,
        topic: String,
        partition: i32,
        offset: i64,
        metadata: Option<String>,
    ) -> bool {
        if !self.state.can_add_partitions() {
            return false;
        }

        let pending = PendingTxnOffset {
            group_id: group_id.clone(),
            topic: topic.clone(),
            partition,
            offset,
            metadata,
        };

        self.pending_offsets
            .entry(group_id)
            .or_default()
            .insert((topic, partition), pending);
        self.last_update = Instant::now();
        true
    }

    /// Transition to prepare commit state
    pub fn prepare_commit(&mut self) -> bool {
        if self.state != TransactionState::Ongoing {
            return false;
        }
        self.state = TransactionState::PrepareCommit;
        self.last_update = Instant::now();
        true
    }

    /// Transition to prepare abort state
    pub fn prepare_abort(&mut self) -> bool {
        if self.state != TransactionState::Ongoing {
            return false;
        }
        self.state = TransactionState::PrepareAbort;
        self.last_update = Instant::now();
        true
    }

    /// Complete the commit
    pub fn complete_commit(&mut self) -> bool {
        if self.state != TransactionState::PrepareCommit {
            return false;
        }
        self.state = TransactionState::CompleteCommit;
        self.last_update = Instant::now();
        true
    }

    /// Complete the abort
    pub fn complete_abort(&mut self) -> bool {
        if self.state != TransactionState::PrepareAbort {
            return false;
        }
        self.state = TransactionState::CompleteAbort;
        self.last_update = Instant::now();
        true
    }

    /// Mark transaction as dead (timed out or error)
    pub fn mark_dead(&mut self) {
        self.state = TransactionState::Dead;
        self.last_update = Instant::now();
    }

    /// Check if transaction has timed out
    pub fn is_timed_out(&self) -> bool {
        self.last_update.elapsed().as_millis() as i64 > self.timeout_ms
    }

    /// Get all partitions in the transaction
    pub fn get_partitions(&self) -> Vec<TransactionPartition> {
        self.partitions.iter().cloned().collect()
    }

    /// Get all pending offsets for a group
    pub fn get_pending_offsets(&self, group_id: &str) -> Vec<&PendingTxnOffset> {
        self.pending_offsets
            .get(group_id)
            .map(|offsets| offsets.values().collect())
            .unwrap_or_default()
    }
}

/// Metadata about a transaction for persistence
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionMetadata {
    pub transactional_id: String,
    pub producer_id: ProducerId,
    pub producer_epoch: ProducerEpoch,
    pub state: TransactionState,
    pub partitions: Vec<TransactionPartition>,
    pub pending_offset_groups: Vec<String>,
    pub timeout_ms: i64,
    pub last_update_ms: i64,
}

impl From<&Transaction> for TransactionMetadata {
    fn from(txn: &Transaction) -> Self {
        TransactionMetadata {
            transactional_id: txn.transactional_id.clone(),
            producer_id: txn.producer_id,
            producer_epoch: txn.producer_epoch,
            state: txn.state,
            partitions: txn.partitions.iter().cloned().collect(),
            pending_offset_groups: txn.pending_offset_groups.iter().cloned().collect(),
            timeout_ms: txn.timeout_ms,
            last_update_ms: chrono::Utc::now().timestamp_millis(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_state_transitions() {
        let mut txn = Transaction::new("txn-1".to_string(), 1000, 0, 60000);

        // Initial state
        assert_eq!(txn.state, TransactionState::Ongoing);
        assert!(txn.state.can_add_partitions());
        assert!(txn.state.can_end_transaction());

        // Add partition
        assert!(txn.add_partition("topic-1".to_string(), 0));
        assert_eq!(txn.partitions.len(), 1);

        // Prepare commit
        assert!(txn.prepare_commit());
        assert_eq!(txn.state, TransactionState::PrepareCommit);
        assert!(!txn.state.can_add_partitions());

        // Complete commit
        assert!(txn.complete_commit());
        assert_eq!(txn.state, TransactionState::CompleteCommit);
        assert!(txn.state.is_terminal());
    }

    #[test]
    fn test_transaction_abort() {
        let mut txn = Transaction::new("txn-2".to_string(), 1001, 0, 60000);

        txn.add_partition("topic-1".to_string(), 0);
        txn.add_partition("topic-1".to_string(), 1);

        // Prepare abort
        assert!(txn.prepare_abort());
        assert_eq!(txn.state, TransactionState::PrepareAbort);

        // Complete abort
        assert!(txn.complete_abort());
        assert_eq!(txn.state, TransactionState::CompleteAbort);
        assert!(txn.state.is_terminal());
    }

    #[test]
    fn test_transaction_cannot_add_after_prepare() {
        let mut txn = Transaction::new("txn-3".to_string(), 1002, 0, 60000);

        txn.prepare_commit();
        assert!(!txn.add_partition("topic-1".to_string(), 0));
    }

    #[test]
    fn test_pending_offsets() {
        let mut txn = Transaction::new("txn-4".to_string(), 1003, 0, 60000);

        txn.add_offset_group("consumer-group-1".to_string());
        txn.add_pending_offset(
            "consumer-group-1".to_string(),
            "topic-1".to_string(),
            0,
            100,
            None,
        );

        let offsets = txn.get_pending_offsets("consumer-group-1");
        assert_eq!(offsets.len(), 1);
        assert_eq!(offsets[0].offset, 100);
    }

    #[test]
    fn test_transaction_partition_equality() {
        let p1 = TransactionPartition::new("topic", 0);
        let p2 = TransactionPartition::new("topic", 0);
        let p3 = TransactionPartition::new("topic", 1);

        assert_eq!(p1, p2);
        assert_ne!(p1, p3);
    }
}
