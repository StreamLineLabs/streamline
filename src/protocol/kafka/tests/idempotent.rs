use super::*;

#[test]
fn test_idempotent_init_producer_id_returns_unique_id() {
    // InitProducerId should return a unique producer ID for each non-transactional call
    use kafka_protocol::messages::InitProducerIdRequest;

    let handler = create_test_handler();

    let request1 = InitProducerIdRequest::default()
        .with_transactional_id(None)
        .with_transaction_timeout_ms(60000);

    let request2 = InitProducerIdRequest::default()
        .with_transactional_id(None)
        .with_transaction_timeout_ms(60000);

    let response1 = handler.handle_init_producer_id(request1).unwrap();
    let response2 = handler.handle_init_producer_id(request2).unwrap();

    assert!(
        response1.producer_id.0 > 0,
        "Producer ID should be positive"
    );
    assert!(
        response2.producer_id.0 > 0,
        "Producer ID should be positive"
    );
    assert_ne!(
        response1.producer_id.0, response2.producer_id.0,
        "Each InitProducerId call should return unique ID"
    );
}

#[test]
fn test_idempotent_init_producer_id_epoch_starts_at_zero() {
    // New producer should start with epoch 0
    use kafka_protocol::messages::InitProducerIdRequest;

    let handler = create_test_handler();

    let request = InitProducerIdRequest::default()
        .with_transactional_id(None)
        .with_transaction_timeout_ms(60000);

    let response = handler.handle_init_producer_id(request).unwrap();

    assert_eq!(response.producer_epoch, 0, "New producer epoch should be 0");
}

#[test]
fn test_idempotent_transactional_id_returns_same_pid() {
    // Same transactional ID should return the same producer ID with bumped epoch
    use kafka_protocol::messages::{InitProducerIdRequest, TransactionalId};

    let handler = create_test_handler();

    let txn_id = TransactionalId(StrBytes::from_static_str("test-txn-1"));

    let request1 = InitProducerIdRequest::default()
        .with_transactional_id(Some(txn_id.clone()))
        .with_transaction_timeout_ms(60000);

    let request2 = InitProducerIdRequest::default()
        .with_transactional_id(Some(txn_id))
        .with_transaction_timeout_ms(60000);

    let response1 = handler.handle_init_producer_id(request1).unwrap();
    let response2 = handler.handle_init_producer_id(request2).unwrap();

    assert_eq!(
        response1.producer_id, response2.producer_id,
        "Same transactional ID should return same producer ID"
    );
    assert_eq!(
        response1.producer_epoch, 0,
        "First call should have epoch 0"
    );
    assert_eq!(
        response2.producer_epoch, 1,
        "Second call should have epoch 1 (bumped)"
    );
}

#[test]
fn test_idempotent_different_transactional_ids_get_different_pids() {
    // Different transactional IDs should get different producer IDs
    use kafka_protocol::messages::{InitProducerIdRequest, TransactionalId};

    let handler = create_test_handler();

    let request1 = InitProducerIdRequest::default()
        .with_transactional_id(Some(TransactionalId(StrBytes::from_static_str(
            "txn-alpha",
        ))))
        .with_transaction_timeout_ms(60000);

    let request2 = InitProducerIdRequest::default()
        .with_transactional_id(Some(TransactionalId(StrBytes::from_static_str("txn-beta"))))
        .with_transaction_timeout_ms(60000);

    let response1 = handler.handle_init_producer_id(request1).unwrap();
    let response2 = handler.handle_init_producer_id(request2).unwrap();

    assert_ne!(
        response1.producer_id, response2.producer_id,
        "Different transactional IDs should get different producer IDs"
    );
}

#[test]
fn test_idempotent_producer_state_manager_validation() {
    // Test sequence validation through the producer state manager
    use crate::storage::producer_state::ProducerStateManager;
    use crate::storage::SequenceValidationResult;

    let manager = ProducerStateManager::in_memory().unwrap();
    let (pid, epoch) = manager.init_producer_id(None, 60000).unwrap();

    // First sequence (0) should be valid
    let result = manager
        .validate_sequence("test-topic", 0, pid, epoch, 0)
        .unwrap();
    assert!(
        matches!(result, SequenceValidationResult::Valid),
        "First sequence should be valid"
    );

    // Record the produce
    manager
        .record_produce("test-topic", 0, pid, epoch, 0, 1, 100)
        .unwrap();

    // Next sequence (1) should be valid
    let result = manager
        .validate_sequence("test-topic", 0, pid, epoch, 1)
        .unwrap();
    assert!(
        matches!(result, SequenceValidationResult::Valid),
        "Next sequential sequence should be valid"
    );
}

#[test]
fn test_idempotent_duplicate_sequence_detection() {
    // Duplicate sequence should return the existing offset
    use crate::storage::producer_state::ProducerStateManager;
    use crate::storage::SequenceValidationResult;

    let manager = ProducerStateManager::in_memory().unwrap();
    let (pid, epoch) = manager.init_producer_id(None, 60000).unwrap();

    // Record sequence 0 with offset 42
    manager
        .record_produce("test-topic", 0, pid, epoch, 0, 1, 42)
        .unwrap();

    // Same sequence should return duplicate with original offset
    let result = manager
        .validate_sequence("test-topic", 0, pid, epoch, 0)
        .unwrap();
    match result {
        SequenceValidationResult::Duplicate(offset) => {
            assert_eq!(offset, 42, "Duplicate should return original offset");
        }
        _ => panic!("Expected Duplicate result"),
    }
}

#[test]
fn test_idempotent_out_of_order_sequence_rejected() {
    // Out of order sequence should be rejected
    use crate::storage::producer_state::ProducerStateManager;
    use crate::storage::SequenceValidationResult;

    let manager = ProducerStateManager::in_memory().unwrap();
    let (pid, epoch) = manager.init_producer_id(None, 60000).unwrap();

    // Record sequence 0
    manager
        .record_produce("test-topic", 0, pid, epoch, 0, 1, 100)
        .unwrap();

    // Skip sequence 1, try sequence 10 (way out of order)
    let result = manager
        .validate_sequence("test-topic", 0, pid, epoch, 10)
        .unwrap();
    match result {
        SequenceValidationResult::OutOfOrder { expected, received } => {
            assert_eq!(expected, 1, "Expected sequence should be 1");
            assert_eq!(received, 10, "Received sequence should be 10");
        }
        _ => panic!("Expected OutOfOrder result"),
    }
}

#[test]
fn test_idempotent_old_epoch_rejected() {
    // Old producer epoch should be rejected (fencing)
    use crate::storage::producer_state::ProducerStateManager;
    use crate::storage::SequenceValidationResult;

    let manager = ProducerStateManager::in_memory().unwrap();
    let (pid, _) = manager.init_producer_id(None, 60000).unwrap();

    // Record with epoch 1
    manager
        .record_produce("test-topic", 0, pid, 1, 0, 1, 100)
        .unwrap();

    // Try to validate with older epoch 0
    let result = manager
        .validate_sequence("test-topic", 0, pid, 0, 1)
        .unwrap();
    match result {
        SequenceValidationResult::InvalidEpoch {
            current_epoch,
            received_epoch,
        } => {
            assert_eq!(current_epoch, 1, "Current epoch should be 1");
            assert_eq!(received_epoch, 0, "Received epoch should be 0");
        }
        _ => panic!("Expected InvalidEpoch result"),
    }
}

#[test]
fn test_idempotent_new_epoch_resets_sequence() {
    // New epoch should allow sequence to restart from 0
    use crate::storage::producer_state::ProducerStateManager;
    use crate::storage::SequenceValidationResult;

    let manager = ProducerStateManager::in_memory().unwrap();
    let (pid, _) = manager.init_producer_id(None, 60000).unwrap();

    // Record with epoch 0, sequence 5
    manager
        .record_produce("test-topic", 0, pid, 0, 5, 1, 100)
        .unwrap();

    // With new epoch 1, sequence 0 should be valid
    let result = manager
        .validate_sequence("test-topic", 0, pid, 1, 0)
        .unwrap();
    assert!(
        matches!(result, SequenceValidationResult::Valid),
        "New epoch should allow sequence to restart"
    );
}

#[test]
fn test_idempotent_batch_sequence_tracking() {
    // Batch with multiple records should track sequences correctly
    use crate::storage::producer_state::ProducerStateManager;
    use crate::storage::SequenceValidationResult;

    let manager = ProducerStateManager::in_memory().unwrap();
    let (pid, epoch) = manager.init_producer_id(None, 60000).unwrap();

    // Batch with base_sequence=0, count=5 (sequences 0,1,2,3,4)
    // After this, last_sequence should be 4, next expected should be 5
    manager
        .record_produce("test-topic", 0, pid, epoch, 0, 5, 100)
        .unwrap();

    // Next batch should start at sequence 5
    let result = manager
        .validate_sequence("test-topic", 0, pid, epoch, 5)
        .unwrap();
    assert!(
        matches!(result, SequenceValidationResult::Valid),
        "Sequence 5 should be valid after batch 0-4"
    );

    // Sequence 4 should be within retry window (valid)
    // Sequence 0 (base of first batch) should be duplicate
    let result = manager
        .validate_sequence("test-topic", 0, pid, epoch, 0)
        .unwrap();
    assert!(
        matches!(result, SequenceValidationResult::Duplicate(_)),
        "Base sequence of recorded batch should be duplicate"
    );
}

#[test]
fn test_idempotent_cross_partition_independence() {
    // Sequences are tracked independently per partition
    use crate::storage::producer_state::ProducerStateManager;
    use crate::storage::SequenceValidationResult;

    let manager = ProducerStateManager::in_memory().unwrap();
    let (pid, epoch) = manager.init_producer_id(None, 60000).unwrap();

    // Record on partition 0
    manager
        .record_produce("test-topic", 0, pid, epoch, 0, 3, 100)
        .unwrap();

    // Partition 1 should start fresh
    let result = manager
        .validate_sequence("test-topic", 1, pid, epoch, 0)
        .unwrap();
    assert!(
        matches!(result, SequenceValidationResult::Valid),
        "Partition 1 should accept sequence 0 independently"
    );

    // Partition 0 should expect sequence 3
    let result = manager
        .validate_sequence("test-topic", 0, pid, epoch, 3)
        .unwrap();
    assert!(
        matches!(result, SequenceValidationResult::Valid),
        "Partition 0 should expect sequence 3 after batch 0-2"
    );
}

#[test]
fn test_idempotent_cross_topic_independence() {
    // Sequences are tracked independently per topic
    use crate::storage::producer_state::ProducerStateManager;
    use crate::storage::SequenceValidationResult;

    let manager = ProducerStateManager::in_memory().unwrap();
    let (pid, epoch) = manager.init_producer_id(None, 60000).unwrap();

    // Record on topic-a
    manager
        .record_produce("topic-a", 0, pid, epoch, 0, 1, 100)
        .unwrap();

    // topic-b should start fresh for same producer/partition
    let result = manager
        .validate_sequence("topic-b", 0, pid, epoch, 0)
        .unwrap();
    assert!(
        matches!(result, SequenceValidationResult::Valid),
        "Different topic should accept sequence 0 independently"
    );
}

#[test]
fn test_idempotent_non_idempotent_producer_skipped() {
    // Producer ID -1 (NO_PRODUCER_ID) should skip sequence validation
    use crate::storage::producer_state::ProducerStateManager;
    use crate::storage::SequenceValidationResult;

    let manager = ProducerStateManager::in_memory().unwrap();

    // Any sequence should be valid for non-idempotent producer
    let result = manager
        .validate_sequence("test-topic", 0, -1, 0, 999)
        .unwrap();
    assert!(
        matches!(result, SequenceValidationResult::Valid),
        "Non-idempotent producer should always be valid"
    );
}

#[test]
fn test_idempotent_sequence_window_size() {
    // Duplicate detection should work within sliding window
    use crate::storage::producer_state::ProducerStateManager;
    use crate::storage::SequenceValidationResult;

    let manager = ProducerStateManager::in_memory().unwrap();
    let (pid, epoch) = manager.init_producer_id(None, 60000).unwrap();

    // Record sequences 0-4 (5 single-record batches)
    for seq in 0..5 {
        manager
            .record_produce("test-topic", 0, pid, epoch, seq, 1, seq as i64 + 100)
            .unwrap();
    }

    // All should be detectable as duplicates (within window of 5)
    for seq in 0..5 {
        let result = manager
            .validate_sequence("test-topic", 0, pid, epoch, seq)
            .unwrap();
        assert!(
            matches!(result, SequenceValidationResult::Duplicate(_)),
            "Sequence {} should be duplicate within window",
            seq
        );
    }
}

#[test]
fn test_idempotent_sequence_wrap_around_i32_max() {
    // Test behavior near i32::MAX (sequence number wraparound)
    use crate::storage::producer_state::ProducerStateManager;
    use crate::storage::SequenceValidationResult;

    let manager = ProducerStateManager::in_memory().unwrap();
    let (pid, epoch) = manager.init_producer_id(None, 60000).unwrap();

    // Record at near max
    let near_max = i32::MAX - 2;
    manager
        .record_produce("test-topic", 0, pid, epoch, near_max, 1, 100)
        .unwrap();

    // Next sequence should still work
    let result = manager
        .validate_sequence("test-topic", 0, pid, epoch, near_max + 1)
        .unwrap();
    assert!(
        matches!(result, SequenceValidationResult::Valid),
        "Sequence near i32::MAX should work"
    );
}

#[test]
fn test_idempotent_producer_metadata_stored() {
    // Producer metadata should be retrievable
    use crate::storage::producer_state::ProducerStateManager;

    let manager = ProducerStateManager::in_memory().unwrap();
    let (pid, _) = manager.init_producer_id(None, 60000).unwrap();

    let metadata = manager.get_producer_metadata(pid).unwrap();
    assert!(metadata.is_some(), "Producer metadata should exist");

    let meta = metadata.unwrap();
    assert_eq!(meta.producer_id, pid);
    assert_eq!(meta.producer_epoch, 0);
    assert!(meta.transactional_id.is_none());
}

#[test]
fn test_idempotent_transactional_producer_metadata() {
    // Transactional producer metadata should include transactional ID
    use crate::storage::producer_state::ProducerStateManager;

    let manager = ProducerStateManager::in_memory().unwrap();
    let (pid, _) = manager.init_producer_id(Some("my-txn-id"), 60000).unwrap();

    let metadata = manager.get_producer_metadata(pid).unwrap();
    assert!(metadata.is_some());

    let meta = metadata.unwrap();
    assert_eq!(meta.transactional_id, Some("my-txn-id".to_string()));
}
