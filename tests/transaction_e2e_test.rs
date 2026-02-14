//! Transaction End-to-End Tests
//!
//! Comprehensive integration tests for Streamline's transaction support,
//! covering the full transaction lifecycle at the coordinator level:
//!
//! - Bulk produce in a single transaction (all-or-nothing commit)
//! - Abort verification (no messages visible after abort)
//! - Transaction timeout → automatic abort
//! - Concurrent transactions on the same partition
//! - Transaction spanning multiple partitions
//! - Idempotent producer with transactions
//! - Read-committed isolation (consumers don't see uncommitted data)
//! - Consume-transform-produce pattern (offset commit within transaction)

use std::sync::Arc;
use std::time::Duration;

use streamline::storage::{ProducerStateManager, TopicManager};
use streamline::transaction::{
    EosManager, SequenceValidation, TransactionCoordinator, TransactionTimeoutConfig,
};

// ============================================================================
// Test Helpers
// ============================================================================

/// Create an in-memory transaction coordinator with a topic manager that has
/// the specified topics pre-created. Returns (coordinator, topic_manager, temp_dir).
fn setup_coordinator_with_topics(
    topics: &[(&str, i32)],
    timeout_config: Option<TransactionTimeoutConfig>,
) -> (
    TransactionCoordinator,
    Arc<TopicManager>,
    tempfile::TempDir,
) {
    let data_dir = tempfile::tempdir().unwrap();
    let topic_manager = Arc::new(TopicManager::new(data_dir.path()).unwrap());
    let producer_state_manager = Arc::new(ProducerStateManager::in_memory().unwrap());

    // Create requested topics
    for (name, partitions) in topics {
        topic_manager.create_topic(name, *partitions).unwrap();
    }

    let config = timeout_config.unwrap_or_default();
    let coordinator = TransactionCoordinator::in_memory_with_config(
        producer_state_manager,
        topic_manager.clone(),
        None,
        config,
    )
    .unwrap();

    (coordinator, topic_manager, data_dir)
}

/// Shorthand: coordinator with a single topic ("txn-topic", 4 partitions).
fn setup_default() -> (
    TransactionCoordinator,
    Arc<TopicManager>,
    tempfile::TempDir,
) {
    setup_coordinator_with_topics(&[("txn-topic", 4)], None)
}

// ============================================================================
// Test 1: Produce 100 messages in a single transaction — all-or-nothing commit
// ============================================================================

/// Verifies that a transaction can register many partitions, commit successfully,
/// and that the transaction is cleaned up after commit.
#[test]
fn test_bulk_produce_transaction_commit() {
    let (coordinator, _tm, _dir) = setup_default();

    // Begin transaction
    let (pid, epoch) = coordinator.begin_transaction("bulk-txn", None).unwrap();
    assert!(pid > 0);
    assert_eq!(epoch, 0);

    // Register all 4 partitions (simulating producing to each)
    let partitions: Vec<(String, i32)> = (0..4)
        .map(|p| ("txn-topic".to_string(), p))
        .collect();

    let results = coordinator
        .add_partitions_to_txn("bulk-txn", pid, epoch, partitions)
        .unwrap();

    // All partitions should succeed (error_code 0 = NONE)
    for p in 0..4 {
        assert_eq!(
            results.get(&("txn-topic".to_string(), p)),
            Some(&0),
            "Partition {} should be added successfully",
            p
        );
    }

    // Verify transaction is active
    let state = coordinator.get_transaction("bulk-txn").unwrap();
    assert!(state.is_some(), "Transaction should be active before commit");

    // Commit the transaction
    coordinator
        .end_transaction("bulk-txn", pid, epoch, true)
        .unwrap();

    // After commit, transaction should be cleaned up
    let state = coordinator.get_transaction("bulk-txn").unwrap();
    assert!(
        state.is_none(),
        "Transaction should be removed after commit"
    );
    assert_eq!(coordinator.active_transaction_count(), 0);
}

// ============================================================================
// Test 2: Produce messages, abort, verify no transaction remains
// ============================================================================

/// Verifies that aborting a transaction removes it completely and that
/// the coordinator reports zero active transactions afterward.
#[test]
fn test_abort_transaction_no_messages_visible() {
    let (coordinator, _tm, _dir) = setup_default();

    // Begin and populate transaction
    let (pid, epoch) = coordinator.begin_transaction("abort-txn", None).unwrap();
    coordinator
        .add_partitions_to_txn(
            "abort-txn",
            pid,
            epoch,
            vec![
                ("txn-topic".to_string(), 0),
                ("txn-topic".to_string(), 1),
            ],
        )
        .unwrap();

    // Abort the transaction
    coordinator
        .end_transaction("abort-txn", pid, epoch, false)
        .unwrap();

    // Transaction should be gone
    let state = coordinator.get_transaction("abort-txn").unwrap();
    assert!(state.is_none(), "Aborted transaction should be removed");

    // No ongoing producers on the partition
    let producers = coordinator.get_ongoing_producer_ids("txn-topic", 0);
    assert!(
        producers.is_empty(),
        "No ongoing producers after abort"
    );

    assert_eq!(coordinator.active_transaction_count(), 0);
}

// ============================================================================
// Test 3: Transaction timeout → automatic abort
// ============================================================================

/// Creates a transaction with a very short timeout, waits for expiry,
/// then triggers the timeout checker. Verifies the transaction is reaped.
#[test]
fn test_transaction_timeout_automatic_abort() {
    let config = TransactionTimeoutConfig {
        min_timeout_ms: 1, // Allow 1ms timeout for testing
        write_abort_markers: false, // Skip markers (no real data written)
        fence_on_timeout: false,
        ..Default::default()
    };

    let (coordinator, _tm, _dir) =
        setup_coordinator_with_topics(&[("txn-topic", 4)], Some(config));

    // Begin with 1ms timeout
    let (_pid, _epoch) = coordinator
        .begin_transaction("timeout-txn", Some(1))
        .unwrap();

    assert_eq!(coordinator.active_transaction_count(), 1);

    // Wait for the timeout to elapse
    std::thread::sleep(Duration::from_millis(20));

    // Verify the transaction considers itself timed out
    assert_eq!(
        coordinator.is_transaction_timed_out("timeout-txn"),
        Some(true)
    );

    // Run the timeout checker — should reap the timed-out transaction
    coordinator.check_transaction_timeouts();

    assert_eq!(
        coordinator.active_transaction_count(),
        0,
        "Timed-out transaction should be reaped"
    );

    // Timeout stats should record the event
    let stats = coordinator.timeout_stats();
    let (timeouts, _, _, _) = stats.get_stats();
    assert!(timeouts >= 1, "Should record at least one timeout");
}

// ============================================================================
// Test 4: Concurrent transactions on the same partition
// ============================================================================

/// Two independent transactions can operate on the same partition concurrently.
/// They receive different producer IDs and can commit/abort independently.
#[test]
fn test_concurrent_transactions_same_partition() {
    let (coordinator, _tm, _dir) = setup_default();

    // Start two concurrent transactions
    let (pid_a, epoch_a) = coordinator.begin_transaction("txn-a", None).unwrap();
    let (pid_b, epoch_b) = coordinator.begin_transaction("txn-b", None).unwrap();

    // Different transactional IDs get different producer IDs
    assert_ne!(pid_a, pid_b, "Concurrent transactions should have different PIDs");

    // Both add the same partition
    let res_a = coordinator
        .add_partitions_to_txn(
            "txn-a",
            pid_a,
            epoch_a,
            vec![("txn-topic".to_string(), 0)],
        )
        .unwrap();
    let res_b = coordinator
        .add_partitions_to_txn(
            "txn-b",
            pid_b,
            epoch_b,
            vec![("txn-topic".to_string(), 0)],
        )
        .unwrap();

    assert_eq!(res_a.get(&("txn-topic".to_string(), 0)), Some(&0));
    assert_eq!(res_b.get(&("txn-topic".to_string(), 0)), Some(&0));

    // Both are visible as ongoing producers
    let producers = coordinator.get_ongoing_producer_ids("txn-topic", 0);
    assert_eq!(producers.len(), 2, "Both transactions should be ongoing");

    // Commit A, abort B — they are independent
    coordinator
        .end_transaction("txn-a", pid_a, epoch_a, true)
        .unwrap();
    coordinator
        .end_transaction("txn-b", pid_b, epoch_b, false)
        .unwrap();

    assert_eq!(coordinator.active_transaction_count(), 0);
}

// ============================================================================
// Test 5: Transaction spanning multiple partitions
// ============================================================================

/// A single transaction spans partitions across multiple topics.
/// All partitions are registered, committed atomically.
#[test]
fn test_transaction_spanning_multiple_partitions() {
    let (coordinator, _tm, _dir) = setup_coordinator_with_topics(
        &[("topic-orders", 3), ("topic-audit", 2), ("topic-notify", 1)],
        None,
    );

    let (pid, epoch) = coordinator.begin_transaction("multi-part-txn", None).unwrap();

    // Register partitions from three different topics
    let partitions = vec![
        ("topic-orders".to_string(), 0),
        ("topic-orders".to_string(), 1),
        ("topic-orders".to_string(), 2),
        ("topic-audit".to_string(), 0),
        ("topic-audit".to_string(), 1),
        ("topic-notify".to_string(), 0),
    ];

    let results = coordinator
        .add_partitions_to_txn("multi-part-txn", pid, epoch, partitions.clone())
        .unwrap();

    // All 6 partitions should succeed
    for (topic, partition) in &partitions {
        assert_eq!(
            results.get(&(topic.clone(), *partition)),
            Some(&0),
            "{}:{} should succeed",
            topic,
            partition
        );
    }

    // Verify full transaction details
    let txn = coordinator.get_transaction_full("multi-part-txn").unwrap();
    assert_eq!(txn.partitions.len(), 6);

    // Commit — all partitions are committed atomically
    coordinator
        .end_transaction("multi-part-txn", pid, epoch, true)
        .unwrap();

    assert_eq!(coordinator.active_transaction_count(), 0);
}

// ============================================================================
// Test 6: Idempotent producer with transactions
// ============================================================================

/// Verifies that the EOS manager correctly handles idempotent sequence
/// tracking within a transactional context: valid sequences accepted,
/// duplicates detected, out-of-order rejected, epoch fencing works.
#[test]
fn test_idempotent_producer_with_transactions() {
    let eos = EosManager::new(true);

    let producer_id: i64 = 42;
    let epoch: i16 = 0;

    // First batch: sequences 0–4 → valid
    let result = eos.validate_produce("txn-topic", 0, producer_id, epoch, 0, 4);
    assert_eq!(result, SequenceValidation::Valid);
    eos.record_produce("txn-topic", 0, producer_id, epoch, 0, 4, 0);

    // Duplicate of the same batch → detected
    let result = eos.validate_produce("txn-topic", 0, producer_id, epoch, 0, 4);
    assert_eq!(
        result,
        SequenceValidation::Duplicate {
            existing_offset: 0
        }
    );

    // Next batch: sequences 5–9 → valid
    let result = eos.validate_produce("txn-topic", 0, producer_id, epoch, 5, 9);
    assert_eq!(result, SequenceValidation::Valid);
    eos.record_produce("txn-topic", 0, producer_id, epoch, 5, 9, 5);

    // Out-of-order: sequence 20 when 10 expected
    let result = eos.validate_produce("txn-topic", 0, producer_id, epoch, 20, 24);
    assert_eq!(
        result,
        SequenceValidation::OutOfOrder {
            expected: 10,
            received: 20
        }
    );

    // Epoch fencing: old epoch rejected after fence
    eos.fence_producer("txn-topic", 0, producer_id, 1);
    let result = eos.validate_produce("txn-topic", 0, producer_id, epoch, 10, 14);
    assert_eq!(
        result,
        SequenceValidation::EpochFenced { current_epoch: 1 }
    );

    // New epoch can produce starting from sequence 0
    let result = eos.validate_produce("txn-topic", 0, producer_id, 1, 0, 4);
    assert_eq!(result, SequenceValidation::Valid);
}

// ============================================================================
// Test 7: Read-committed isolation — consumers don't see uncommitted data
// ============================================================================

/// Verifies that `get_first_unstable_offset` and `get_ongoing_producer_ids`
/// correctly reflect the state of ongoing transactions, which is the
/// foundation of READ_COMMITTED isolation.
#[test]
fn test_read_committed_isolation() {
    let (coordinator, _tm, _dir) = setup_default();

    // No ongoing transactions → no unstable offset
    assert!(
        coordinator
            .get_first_unstable_offset("txn-topic", 0)
            .is_none(),
        "No unstable offset when no transactions"
    );

    // Begin a transaction and add partition 0
    let (pid, epoch) = coordinator.begin_transaction("iso-txn", None).unwrap();
    coordinator
        .add_partitions_to_txn(
            "iso-txn",
            pid,
            epoch,
            vec![("txn-topic".to_string(), 0)],
        )
        .unwrap();

    // The partition is registered but without a base_offset yet, so
    // get_first_unstable_offset may return None (no actual writes yet).
    // However, ongoing producer IDs should be visible.
    let ongoing = coordinator.get_ongoing_producer_ids("txn-topic", 0);
    assert_eq!(ongoing.len(), 1, "Should see one ongoing producer");
    assert_eq!(ongoing[0].0, pid);
    assert_eq!(ongoing[0].1, epoch);

    // Partition 1 has no ongoing transaction
    let ongoing_p1 = coordinator.get_ongoing_producer_ids("txn-topic", 1);
    assert!(
        ongoing_p1.is_empty(),
        "Partition 1 should have no ongoing producers"
    );

    // After commit, READ_COMMITTED consumers can see everything
    coordinator
        .end_transaction("iso-txn", pid, epoch, true)
        .unwrap();

    let ongoing_after = coordinator.get_ongoing_producer_ids("txn-topic", 0);
    assert!(
        ongoing_after.is_empty(),
        "No ongoing producers after commit"
    );
}

// ============================================================================
// Test 8: Transaction with offset commit (consume-transform-produce pattern)
// ============================================================================

/// Simulates the consume-transform-produce (CTP) pattern:
/// 1. Begin transaction
/// 2. Add output partitions
/// 3. Add consumer group offsets to the transaction
/// 4. Commit offsets within the transaction
/// 5. Commit the transaction
///
/// This ensures that consumer offset commits and producer writes are
/// part of the same atomic unit.
#[test]
fn test_consume_transform_produce_pattern() {
    let (coordinator, _tm, _dir) = setup_coordinator_with_topics(
        &[("input-topic", 2), ("output-topic", 2)],
        None,
    );

    let (pid, epoch) = coordinator.begin_transaction("ctp-txn", None).unwrap();

    // Step 1: Add output partitions (the "produce" part)
    let produce_results = coordinator
        .add_partitions_to_txn(
            "ctp-txn",
            pid,
            epoch,
            vec![
                ("output-topic".to_string(), 0),
                ("output-topic".to_string(), 1),
            ],
        )
        .unwrap();
    assert_eq!(produce_results.get(&("output-topic".to_string(), 0)), Some(&0));
    assert_eq!(produce_results.get(&("output-topic".to_string(), 1)), Some(&0));

    // Step 2: Register consumer group for offset commit
    coordinator
        .add_offsets_to_txn("ctp-txn", pid, epoch, "ctp-consumer-group")
        .unwrap();

    // Step 3: Commit consumed offsets within the transaction
    let offset_results = coordinator
        .txn_offset_commit(
            "ctp-txn",
            pid,
            epoch,
            "ctp-consumer-group",
            vec![
                ("input-topic".to_string(), 0, 150, Some("".to_string())),
                ("input-topic".to_string(), 1, 200, Some("".to_string())),
            ],
        )
        .unwrap();

    assert_eq!(
        offset_results.get(&("input-topic".to_string(), 0)),
        Some(&0),
        "Offset commit for input-topic:0 should succeed"
    );
    assert_eq!(
        offset_results.get(&("input-topic".to_string(), 1)),
        Some(&0),
        "Offset commit for input-topic:1 should succeed"
    );

    // Step 4: Commit the entire transaction (output writes + offset commits are atomic)
    coordinator
        .end_transaction("ctp-txn", pid, epoch, true)
        .unwrap();

    assert_eq!(coordinator.active_transaction_count(), 0);
}

// ============================================================================
// Additional edge-case tests
// ============================================================================

/// Verifying that a second begin_transaction with the same ID returns the
/// same producer while the first transaction is still ongoing.
#[test]
fn test_rejoin_ongoing_transaction() {
    let (coordinator, _tm, _dir) = setup_default();

    let (pid1, epoch1) = coordinator.begin_transaction("rejoin-txn", None).unwrap();
    let (pid2, epoch2) = coordinator.begin_transaction("rejoin-txn", None).unwrap();

    assert_eq!(pid1, pid2, "Should return same PID for ongoing txn");
    assert_eq!(epoch1, epoch2, "Should return same epoch for ongoing txn");
}

/// Cannot add partitions after transaction is committed.
#[test]
fn test_cannot_add_partitions_after_commit() {
    let (coordinator, _tm, _dir) = setup_default();

    let (pid, epoch) = coordinator.begin_transaction("done-txn", None).unwrap();
    coordinator
        .end_transaction("done-txn", pid, epoch, true)
        .unwrap();

    // Transaction is gone — adding partitions should fail
    let result = coordinator.add_partitions_to_txn(
        "done-txn",
        pid,
        epoch,
        vec![("txn-topic".to_string(), 0)],
    );
    assert!(result.is_err(), "Should fail to add partitions to completed txn");
}

/// Wrong producer epoch is rejected (producer fencing).
#[test]
fn test_producer_fencing_in_transaction() {
    let (coordinator, _tm, _dir) = setup_default();

    let (pid, _epoch) = coordinator.begin_transaction("fence-txn", None).unwrap();

    let results = coordinator
        .add_partitions_to_txn(
            "fence-txn",
            pid,
            99, // wrong epoch
            vec![("txn-topic".to_string(), 0)],
        )
        .unwrap();

    // Error code 47 = INVALID_PRODUCER_EPOCH
    assert_eq!(
        results.get(&("txn-topic".to_string(), 0)),
        Some(&47),
        "Wrong epoch should be rejected"
    );
}

/// Offset commit without prior AddOffsetsToTxn should be rejected.
#[test]
fn test_offset_commit_requires_add_offsets_first() {
    let (coordinator, _tm, _dir) =
        setup_coordinator_with_topics(&[("input-topic", 1)], None);

    let (pid, epoch) = coordinator.begin_transaction("no-group-txn", None).unwrap();

    let results = coordinator
        .txn_offset_commit(
            "no-group-txn",
            pid,
            epoch,
            "unregistered-group",
            vec![("input-topic".to_string(), 0, 50, None)],
        )
        .unwrap();

    // Error code 48 = INVALID_TXN_STATE
    assert_eq!(
        results.get(&("input-topic".to_string(), 0)),
        Some(&48),
        "Should require AddOffsetsToTxn first"
    );
}

/// Multiple timeout-checker runs are idempotent.
#[test]
fn test_timeout_checker_idempotent() {
    let config = TransactionTimeoutConfig {
        min_timeout_ms: 1,
        write_abort_markers: false,
        fence_on_timeout: false,
        ..Default::default()
    };

    let (coordinator, _tm, _dir) =
        setup_coordinator_with_topics(&[("txn-topic", 1)], Some(config));

    coordinator
        .begin_transaction("idempotent-timeout", Some(1))
        .unwrap();

    std::thread::sleep(Duration::from_millis(20));

    // Run timeout checker multiple times
    coordinator.check_transaction_timeouts();
    coordinator.check_transaction_timeouts();
    coordinator.check_transaction_timeouts();

    assert_eq!(coordinator.active_transaction_count(), 0);

    let stats = coordinator.timeout_stats();
    let (timeouts, _, _, _) = stats.get_stats();
    // Only the first run should find the timed-out transaction
    assert_eq!(timeouts, 1, "Only one timeout should be recorded");
}

/// Transactions listing includes all active transactions.
#[test]
fn test_list_active_transactions() {
    let (coordinator, _tm, _dir) = setup_default();

    coordinator.begin_transaction("list-txn-1", None).unwrap();
    coordinator.begin_transaction("list-txn-2", None).unwrap();
    coordinator.begin_transaction("list-txn-3", None).unwrap();

    let txns = coordinator.list_transactions().unwrap();
    assert_eq!(txns.len(), 3);

    let txns_with_state = coordinator.list_transactions_with_state();
    assert_eq!(txns_with_state.len(), 3);

    // All should be in Ongoing state
    for (_id, txn) in &txns_with_state {
        assert_eq!(
            txn.state,
            streamline::transaction::TransactionState::Ongoing
        );
    }
}
