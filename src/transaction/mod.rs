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
//! - Idempotent producer sequence tracking with persistent snapshots
//! - Jepsen-style correctness verification
//!
//! # Stability
//!
//! This module is **Stable**. The transaction API implements Kafka-compatible
//! exactly-once semantics with persistent state recovery and crash safety.

mod coordinator;
pub mod eos;
pub mod jepsen;
pub mod log;
pub mod saga;
mod state;

pub use coordinator::{
    AbortedTransaction, TransactionCoordinator, TransactionTimeoutConfig, TransactionTimeoutStats,
};
pub use eos::{EosManager, SequenceValidation};
pub use jepsen::{EosChecker, TestResult as JepsenTestResult};
pub use log::TransactionLog;
pub use saga::{SagaBuilder, SagaDefinition, SagaExecution, SagaOrchestrator, SagaState};
pub use state::{Transaction, TransactionPartition, TransactionState};
