//! Transaction support for Kafka-compatible exactly-once semantics
//!
//! This module provides transaction coordination for producers that need
//! atomic writes across multiple topic-partitions. It implements the Kafka
//! transaction protocol including:
//!
//! - Transaction state management (Ongoing, Preparing, Committed, Aborted)
//! - AddPartitionsToTxn - register partitions in the transaction
//! - AddOffsetsToTxn - include consumer offsets in the transaction
//! - EndTxn - commit or abort the transaction
//! - TxnOffsetCommit - commit offsets as part of a transaction
//!
//! # Stability
//!
//! This module is **Beta**. The transaction API is functional but may have
//! changes in minor versions as we improve compatibility with Kafka clients.

mod coordinator;
pub mod eos;
pub mod log;
mod state;

pub use coordinator::{
    AbortedTransaction, TransactionCoordinator, TransactionTimeoutConfig, TransactionTimeoutStats,
};
pub use eos::{EosManager, SequenceValidation};
pub use log::TransactionLog;
pub use state::{Transaction, TransactionPartition, TransactionState};
