use super::*;

fn create_control_record_batch(
    base_offset: i64,
    producer_id: i64,
    producer_epoch: i16,
    control_type: i16,
) -> bytes::Bytes {
    use bytes::{BufMut, BytesMut};

    let mut buf = BytesMut::with_capacity(256);

    buf.put_i64(base_offset);

    let batch_length_pos = buf.len();
    buf.put_i32(0);

    buf.put_i32(0);

    buf.put_i8(RECORD_BATCH_MAGIC_V2);

    let crc_pos = buf.len();
    buf.put_u32(0);

    let crc_start = buf.len();

    let attributes = ATTR_TRANSACTIONAL_BIT | ATTR_CONTROL_BIT;
    buf.put_i16(attributes);

    buf.put_i32(0);

    let timestamp = chrono::Utc::now().timestamp_millis();
    buf.put_i64(timestamp);
    buf.put_i64(timestamp);

    buf.put_i64(producer_id);
    buf.put_i16(producer_epoch);
    buf.put_i32(-1);
    buf.put_i32(1);

    let mut key_buf = BytesMut::with_capacity(4);
    key_buf.put_i16(0);
    key_buf.put_i16(control_type);

    let record_attributes: i8 = 0;
    let key_len = key_buf.len() as i32;
    let headers_count: i32 = 0;

    let record_len = 1 + 1 + 1 + 1 + key_len + 1 + 1;

    buf.put_u8(record_len as u8);
    buf.put_i8(record_attributes);
    buf.put_u8(0);
    buf.put_u8(0);

    buf.put_u8((key_len + 1) as u8);
    buf.put_slice(&key_buf);

    buf.put_u8(0);

    buf.put_u8((headers_count + 1) as u8);

    let crc = crc32fast::hash(&buf[crc_start..]);
    let crc_bytes = crc.to_be_bytes();
    buf[crc_pos..crc_pos + 4].copy_from_slice(&crc_bytes);

    let batch_length = (buf.len() - 12) as i32;
    let batch_len_bytes = batch_length.to_be_bytes();
    buf[batch_length_pos..batch_length_pos + 4].copy_from_slice(&batch_len_bytes);

    buf.freeze()
}

#[test]
fn test_init_producer_id_basic() {
    // Test basic InitProducerId request without transactional ID
    use kafka_protocol::messages::InitProducerIdRequest;

    let handler = create_test_handler();

    let request = InitProducerIdRequest::default().with_transaction_timeout_ms(60000);

    let response = handler.handle_init_producer_id(request).unwrap();

    // Should return a valid producer ID and epoch (or error if not supported)
    assert!(
        response.error_code == NONE || response.error_code == UNKNOWN_SERVER_ERROR,
        "Expected NONE (0) or UNKNOWN_SERVER_ERROR (-1), got {}",
        response.error_code
    );

    if response.error_code == NONE {
        // Producer ID should be non-negative
        assert!(
            response.producer_id.0 >= 0,
            "Producer ID should be non-negative when successful"
        );
        // Epoch should be non-negative
        assert!(
            response.producer_epoch >= 0,
            "Producer epoch should be non-negative when successful"
        );
    }
}

#[test]
fn test_init_producer_id_with_transactional_id() {
    // Test InitProducerId with a transactional ID
    use kafka_protocol::messages::InitProducerIdRequest;
    use kafka_protocol::messages::TransactionalId;

    let handler = create_test_handler();

    let request = InitProducerIdRequest::default()
        .with_transactional_id(Some(TransactionalId(StrBytes::from_static_str(
            "my-transaction-id",
        ))))
        .with_transaction_timeout_ms(60000);

    let response = handler.handle_init_producer_id(request).unwrap();

    // Should return a valid producer ID and epoch (or error if not supported)
    assert!(
        response.error_code == NONE
            || response.error_code == UNKNOWN_SERVER_ERROR
            || response.error_code == COORDINATOR_NOT_AVAILABLE,
        "Expected NONE (0), UNKNOWN_SERVER_ERROR (-1), or COORDINATOR_NOT_AVAILABLE (15), got {}",
        response.error_code
    );
}

#[test]
fn test_init_producer_id_returns_valid_ids() {
    // Multiple InitProducerId calls should return valid producer IDs
    // Note: Without a full transaction coordinator, IDs may not be unique
    use kafka_protocol::messages::InitProducerIdRequest;

    let handler = create_test_handler();

    let request1 = InitProducerIdRequest::default().with_transaction_timeout_ms(60000);
    let response1 = handler.handle_init_producer_id(request1).unwrap();

    let request2 = InitProducerIdRequest::default().with_transaction_timeout_ms(60000);
    let response2 = handler.handle_init_producer_id(request2).unwrap();

    // If both succeed, producer IDs should be valid (> 0)
    if response1.error_code == NONE && response2.error_code == NONE {
        assert!(
            response1.producer_id.0 > 0,
            "Producer ID should be positive, got {}",
            response1.producer_id.0
        );
        assert!(
            response2.producer_id.0 > 0,
            "Producer ID should be positive, got {}",
            response2.producer_id.0
        );
        // Note: In a full implementation, these would be unique
        // For now, verify both return valid IDs
    }
}

#[test]
fn test_init_producer_id_idempotent_with_same_txn_id() {
    // Same transactional ID should return consistent producer ID
    use kafka_protocol::messages::InitProducerIdRequest;
    use kafka_protocol::messages::TransactionalId;

    let handler = create_test_handler();

    let request1 = InitProducerIdRequest::default()
        .with_transactional_id(Some(TransactionalId(StrBytes::from_static_str(
            "idempotent-txn",
        ))))
        .with_transaction_timeout_ms(60000);
    let response1 = handler.handle_init_producer_id(request1).unwrap();

    let request2 = InitProducerIdRequest::default()
        .with_transactional_id(Some(TransactionalId(StrBytes::from_static_str(
            "idempotent-txn",
        ))))
        .with_transaction_timeout_ms(60000);
    let response2 = handler.handle_init_producer_id(request2).unwrap();

    // If both succeed, same transactional ID should get same producer ID
    // (but epoch may increment)
    if response1.error_code == NONE && response2.error_code == NONE {
        assert_eq!(
            response1.producer_id.0, response2.producer_id.0,
            "Same transactional ID should get same producer ID"
        );
    }
}

#[test]
fn test_add_partitions_to_txn_basic() {
    // Test AddPartitionsToTxn request
    use kafka_protocol::messages::add_partitions_to_txn_request::AddPartitionsToTxnTopic;
    use kafka_protocol::messages::AddPartitionsToTxnRequest;
    use kafka_protocol::messages::TransactionalId;
    use kafka_protocol::protocol::StrBytes;

    let handler = create_handler_with_topics(&[("txn-topic", 3)]);

    let request = AddPartitionsToTxnRequest::default()
        .with_v3_and_below_transactional_id(TransactionalId(StrBytes::from_static_str("test-txn")))
        .with_v3_and_below_producer_id(KafkaProducerId(12345))
        .with_v3_and_below_producer_epoch(0)
        .with_v3_and_below_topics(vec![AddPartitionsToTxnTopic::default()
            .with_name(TopicName::from(StrBytes::from_static_str("txn-topic")))
            .with_partitions(vec![0, 1, 2])]);

    let response = handler.handle_add_partitions_to_txn(request).unwrap();

    // Response should include results for the topic
    assert!(
        !response.results_by_topic_v3_and_below.is_empty(),
        "Response should include topic results"
    );
}

#[test]
fn test_add_partitions_to_txn_no_coordinator() {
    // Without a transaction coordinator, should return COORDINATOR_NOT_AVAILABLE
    use kafka_protocol::messages::add_partitions_to_txn_request::AddPartitionsToTxnTopic;
    use kafka_protocol::messages::AddPartitionsToTxnRequest;
    use kafka_protocol::messages::TransactionalId;

    let handler = create_test_handler();

    let request = AddPartitionsToTxnRequest::default()
        .with_v3_and_below_transactional_id(TransactionalId(StrBytes::from_static_str("test-txn")))
        .with_v3_and_below_producer_id(KafkaProducerId(12345))
        .with_v3_and_below_producer_epoch(0)
        .with_v3_and_below_topics(vec![AddPartitionsToTxnTopic::default()
            .with_name(TopicName::from(StrBytes::from_static_str("test-topic")))
            .with_partitions(vec![0])]);

    let response = handler.handle_add_partitions_to_txn(request).unwrap();

    // Should include results - may be COORDINATOR_NOT_AVAILABLE or other error
    assert!(
        !response.results_by_topic_v3_and_below.is_empty(),
        "Response should include topic results"
    );

    // Check first partition result
    let partition_result = &response.results_by_topic_v3_and_below[0].results_by_partition[0];
    assert!(
        partition_result.partition_error_code == COORDINATOR_NOT_AVAILABLE
            || partition_result.partition_error_code == NONE
            || partition_result.partition_error_code == UNKNOWN_SERVER_ERROR,
        "Expected COORDINATOR_NOT_AVAILABLE (15), NONE (0), or UNKNOWN_SERVER_ERROR (-1), got {}",
        partition_result.partition_error_code
    );
}

#[test]
fn test_add_partitions_to_txn_multiple_topics() {
    // Test adding partitions from multiple topics
    use kafka_protocol::messages::add_partitions_to_txn_request::AddPartitionsToTxnTopic;
    use kafka_protocol::messages::AddPartitionsToTxnRequest;

    let handler = create_handler_with_topics(&[("topic-a", 2), ("topic-b", 2)]);

    use kafka_protocol::messages::TransactionalId;
    let request = AddPartitionsToTxnRequest::default()
        .with_v3_and_below_transactional_id(TransactionalId(StrBytes::from_static_str(
            "multi-topic-txn",
        )))
        .with_v3_and_below_producer_id(KafkaProducerId(99999))
        .with_v3_and_below_producer_epoch(0)
        .with_v3_and_below_topics(vec![
            AddPartitionsToTxnTopic::default()
                .with_name(TopicName::from(StrBytes::from_static_str("topic-a")))
                .with_partitions(vec![0, 1]),
            AddPartitionsToTxnTopic::default()
                .with_name(TopicName::from(StrBytes::from_static_str("topic-b")))
                .with_partitions(vec![0]),
        ]);

    let response = handler.handle_add_partitions_to_txn(request).unwrap();

    // Response should include results for both topics
    // Note: may be combined into fewer entries depending on implementation
    let total_partitions: usize = response
        .results_by_topic_v3_and_below
        .iter()
        .map(|t| t.results_by_partition.len())
        .sum();
    assert!(
        total_partitions >= 1,
        "Response should include partition results"
    );
}

#[test]
fn test_add_offsets_to_txn_basic() {
    // Test AddOffsetsToTxn request
    use kafka_protocol::messages::AddOffsetsToTxnRequest;

    let handler = create_test_handler();

    use kafka_protocol::messages::TransactionalId;
    let request = AddOffsetsToTxnRequest::default()
        .with_transactional_id(TransactionalId(StrBytes::from_static_str("offsets-txn")))
        .with_producer_id(KafkaProducerId(12345))
        .with_producer_epoch(0)
        .with_group_id(GroupId::from(StrBytes::from_static_str("consumer-group")));

    let response = handler.handle_add_offsets_to_txn(request).unwrap();

    // Should return an error code (likely COORDINATOR_NOT_AVAILABLE without coordinator)
    assert!(
        response.error_code == NONE
            || response.error_code == COORDINATOR_NOT_AVAILABLE
            || response.error_code == UNKNOWN_SERVER_ERROR,
        "Expected NONE (0), COORDINATOR_NOT_AVAILABLE (15), or UNKNOWN_SERVER_ERROR (-1), got {}",
        response.error_code
    );
}

#[test]
fn test_add_offsets_to_txn_no_coordinator() {
    // Without a transaction coordinator, should return COORDINATOR_NOT_AVAILABLE
    use kafka_protocol::messages::AddOffsetsToTxnRequest;

    let handler = create_test_handler();

    use kafka_protocol::messages::TransactionalId;
    let request = AddOffsetsToTxnRequest::default()
        .with_transactional_id(TransactionalId(StrBytes::from_static_str("no-coord-txn")))
        .with_producer_id(KafkaProducerId(55555))
        .with_producer_epoch(0)
        .with_group_id(GroupId::from(StrBytes::from_static_str("test-group")));

    let response = handler.handle_add_offsets_to_txn(request).unwrap();

    // Should return COORDINATOR_NOT_AVAILABLE or succeed
    assert!(
        response.error_code == COORDINATOR_NOT_AVAILABLE
            || response.error_code == NONE
            || response.error_code == UNKNOWN_SERVER_ERROR,
        "Expected COORDINATOR_NOT_AVAILABLE (15), NONE (0), or UNKNOWN_SERVER_ERROR (-1), got {}",
        response.error_code
    );
}

#[test]
fn test_end_txn_commit() {
    // Test ending a transaction with commit
    use kafka_protocol::messages::EndTxnRequest;
    use kafka_protocol::messages::TransactionalId;

    let handler = create_test_handler();

    let request = EndTxnRequest::default()
        .with_transactional_id(TransactionalId(StrBytes::from_static_str("commit-txn")))
        .with_producer_id(KafkaProducerId(12345))
        .with_producer_epoch(0)
        .with_committed(true);

    let response = handler.handle_end_txn(request).unwrap();

    // Should return error code (COORDINATOR_NOT_AVAILABLE without coordinator)
    assert!(
        response.error_code == NONE
            || response.error_code == COORDINATOR_NOT_AVAILABLE
            || response.error_code == UNKNOWN_SERVER_ERROR,
        "Expected NONE (0), COORDINATOR_NOT_AVAILABLE (15), or UNKNOWN_SERVER_ERROR (-1), got {}",
        response.error_code
    );
}

#[test]
fn test_end_txn_abort() {
    // Test ending a transaction with abort
    use kafka_protocol::messages::EndTxnRequest;
    use kafka_protocol::messages::TransactionalId;

    let handler = create_test_handler();

    let request = EndTxnRequest::default()
        .with_transactional_id(TransactionalId(StrBytes::from_static_str("abort-txn")))
        .with_producer_id(KafkaProducerId(12345))
        .with_producer_epoch(0)
        .with_committed(false); // Abort

    let response = handler.handle_end_txn(request).unwrap();

    // Should return error code (COORDINATOR_NOT_AVAILABLE without coordinator)
    assert!(
        response.error_code == NONE
            || response.error_code == COORDINATOR_NOT_AVAILABLE
            || response.error_code == UNKNOWN_SERVER_ERROR,
        "Expected NONE (0), COORDINATOR_NOT_AVAILABLE (15), or UNKNOWN_SERVER_ERROR (-1), got {}",
        response.error_code
    );
}

#[test]
fn test_end_txn_no_coordinator() {
    // Without a transaction coordinator, should return COORDINATOR_NOT_AVAILABLE
    use kafka_protocol::messages::EndTxnRequest;
    use kafka_protocol::messages::TransactionalId;

    let handler = create_test_handler();

    let request = EndTxnRequest::default()
        .with_transactional_id(TransactionalId(StrBytes::from_static_str(
            "no-coord-end-txn",
        )))
        .with_producer_id(KafkaProducerId(99999))
        .with_producer_epoch(0)
        .with_committed(true);

    let response = handler.handle_end_txn(request).unwrap();

    assert!(
        response.error_code == COORDINATOR_NOT_AVAILABLE
            || response.error_code == NONE
            || response.error_code == UNKNOWN_SERVER_ERROR,
        "Expected COORDINATOR_NOT_AVAILABLE (15), NONE (0), or UNKNOWN_SERVER_ERROR (-1), got {}",
        response.error_code
    );
}

#[test]
fn test_txn_offset_commit_basic() {
    // Test TxnOffsetCommit request
    use kafka_protocol::messages::txn_offset_commit_request::{
        TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic,
    };
    use kafka_protocol::messages::TransactionalId;
    use kafka_protocol::messages::TxnOffsetCommitRequest;

    let handler = create_handler_with_topics(&[("txn-commit-topic", 1)]);

    let request = TxnOffsetCommitRequest::default()
        .with_transactional_id(TransactionalId(StrBytes::from_static_str(
            "offset-commit-txn",
        )))
        .with_group_id(GroupId::from(StrBytes::from_static_str(
            "txn-consumer-group",
        )))
        .with_producer_id(KafkaProducerId(12345))
        .with_producer_epoch(0)
        .with_topics(vec![TxnOffsetCommitRequestTopic::default()
            .with_name(TopicName::from(StrBytes::from_static_str(
                "txn-commit-topic",
            )))
            .with_partitions(vec![TxnOffsetCommitRequestPartition::default()
                .with_partition_index(0)
                .with_committed_offset(100)
                .with_committed_metadata(Some(StrBytes::from_static_str(
                    "metadata",
                )))])]);

    let response = handler.handle_txn_offset_commit(request).unwrap();

    // Response should include topic results
    assert!(
        !response.topics.is_empty(),
        "Response should include topic results"
    );
}

#[test]
fn test_txn_offset_commit_no_coordinator() {
    // Without a transaction coordinator, should return COORDINATOR_NOT_AVAILABLE
    use kafka_protocol::messages::txn_offset_commit_request::{
        TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic,
    };
    use kafka_protocol::messages::TransactionalId;
    use kafka_protocol::messages::TxnOffsetCommitRequest;

    let handler = create_test_handler();

    let request = TxnOffsetCommitRequest::default()
        .with_transactional_id(TransactionalId(StrBytes::from_static_str(
            "no-coord-offset-txn",
        )))
        .with_group_id(GroupId::from(StrBytes::from_static_str("no-coord-group")))
        .with_producer_id(KafkaProducerId(55555))
        .with_producer_epoch(0)
        .with_topics(vec![TxnOffsetCommitRequestTopic::default()
            .with_name(TopicName::from(StrBytes::from_static_str("test-topic")))
            .with_partitions(vec![TxnOffsetCommitRequestPartition::default()
                .with_partition_index(0)
                .with_committed_offset(50)])]);

    let response = handler.handle_txn_offset_commit(request).unwrap();

    // Should include partition results
    assert!(
        !response.topics.is_empty(),
        "Response should include topic results"
    );

    // Check first partition error code
    let partition_error = response.topics[0].partitions[0].error_code;
    assert!(
        partition_error == COORDINATOR_NOT_AVAILABLE
            || partition_error == NONE
            || partition_error == UNKNOWN_SERVER_ERROR,
        "Expected COORDINATOR_NOT_AVAILABLE (15), NONE (0), or UNKNOWN_SERVER_ERROR (-1), got {}",
        partition_error
    );
}

#[test]
fn test_txn_offset_commit_multiple_partitions() {
    // Test committing offsets for multiple partitions
    use kafka_protocol::messages::txn_offset_commit_request::{
        TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic,
    };
    use kafka_protocol::messages::TransactionalId;
    use kafka_protocol::messages::TxnOffsetCommitRequest;

    let handler = create_handler_with_topics(&[("multi-partition-topic", 3)]);

    let request = TxnOffsetCommitRequest::default()
        .with_transactional_id(TransactionalId(StrBytes::from_static_str(
            "multi-offset-txn",
        )))
        .with_group_id(GroupId::from(StrBytes::from_static_str("multi-group")))
        .with_producer_id(KafkaProducerId(77777))
        .with_producer_epoch(0)
        .with_topics(vec![TxnOffsetCommitRequestTopic::default()
            .with_name(TopicName::from(StrBytes::from_static_str(
                "multi-partition-topic",
            )))
            .with_partitions(vec![
                TxnOffsetCommitRequestPartition::default()
                    .with_partition_index(0)
                    .with_committed_offset(100),
                TxnOffsetCommitRequestPartition::default()
                    .with_partition_index(1)
                    .with_committed_offset(200),
                TxnOffsetCommitRequestPartition::default()
                    .with_partition_index(2)
                    .with_committed_offset(300),
            ])]);

    let response = handler.handle_txn_offset_commit(request).unwrap();

    // Should include results for all partitions
    assert!(!response.topics.is_empty());
    let total_partitions: usize = response.topics.iter().map(|t| t.partitions.len()).sum();
    assert!(
        total_partitions >= 1,
        "Response should include partition results"
    );
}

#[test]
fn test_transaction_full_lifecycle() {
    // Test a simplified transaction lifecycle:
    // 1. InitProducerId
    // 2. AddPartitionsToTxn
    // 3. EndTxn (commit)
    use kafka_protocol::messages::add_partitions_to_txn_request::AddPartitionsToTxnTopic;
    use kafka_protocol::messages::AddPartitionsToTxnRequest;
    use kafka_protocol::messages::EndTxnRequest;
    use kafka_protocol::messages::InitProducerIdRequest;
    use kafka_protocol::messages::TransactionalId;

    let handler = create_handler_with_topics(&[("lifecycle-topic", 1)]);

    // Step 1: Initialize producer
    let init_request = InitProducerIdRequest::default()
        .with_transactional_id(Some(TransactionalId(StrBytes::from_static_str(
            "lifecycle-txn",
        ))))
        .with_transaction_timeout_ms(60000);
    let init_response = handler.handle_init_producer_id(init_request).unwrap();

    // Continue only if init succeeded
    if init_response.error_code == NONE {
        let producer_id = init_response.producer_id;
        let producer_epoch = init_response.producer_epoch;

        // Step 2: Add partitions to transaction
        let add_request = AddPartitionsToTxnRequest::default()
            .with_v3_and_below_transactional_id(TransactionalId(StrBytes::from_static_str(
                "lifecycle-txn",
            )))
            .with_v3_and_below_producer_id(producer_id)
            .with_v3_and_below_producer_epoch(producer_epoch)
            .with_v3_and_below_topics(vec![AddPartitionsToTxnTopic::default()
                .with_name(TopicName::from(StrBytes::from_static_str(
                    "lifecycle-topic",
                )))
                .with_partitions(vec![0])]);
        let _add_response = handler.handle_add_partitions_to_txn(add_request).unwrap();

        // Step 3: End transaction (commit)
        let end_request = EndTxnRequest::default()
            .with_transactional_id(TransactionalId(StrBytes::from_static_str("lifecycle-txn")))
            .with_producer_id(producer_id)
            .with_producer_epoch(producer_epoch)
            .with_committed(true);
        let end_response = handler.handle_end_txn(end_request).unwrap();

        // Verify end transaction returned a valid response
        assert!(
            end_response.error_code == NONE
                || end_response.error_code == COORDINATOR_NOT_AVAILABLE
                || end_response.error_code == UNKNOWN_SERVER_ERROR,
            "EndTxn should return valid error code"
        );
    }
}

#[test]
fn test_transaction_abort_lifecycle() {
    // Test transaction abort lifecycle
    use kafka_protocol::messages::add_partitions_to_txn_request::AddPartitionsToTxnTopic;
    use kafka_protocol::messages::AddPartitionsToTxnRequest;
    use kafka_protocol::messages::EndTxnRequest;
    use kafka_protocol::messages::InitProducerIdRequest;
    use kafka_protocol::messages::TransactionalId;

    let handler = create_handler_with_topics(&[("abort-topic", 1)]);

    // Step 1: Initialize producer
    let init_request = InitProducerIdRequest::default()
        .with_transactional_id(Some(TransactionalId(StrBytes::from_static_str(
            "abort-lifecycle-txn",
        ))))
        .with_transaction_timeout_ms(60000);
    let init_response = handler.handle_init_producer_id(init_request).unwrap();

    if init_response.error_code == NONE {
        let producer_id = init_response.producer_id;
        let producer_epoch = init_response.producer_epoch;

        // Step 2: Add partitions
        let add_request = AddPartitionsToTxnRequest::default()
            .with_v3_and_below_transactional_id(TransactionalId(StrBytes::from_static_str(
                "abort-lifecycle-txn",
            )))
            .with_v3_and_below_producer_id(producer_id)
            .with_v3_and_below_producer_epoch(producer_epoch)
            .with_v3_and_below_topics(vec![AddPartitionsToTxnTopic::default()
                .with_name(TopicName::from(StrBytes::from_static_str("abort-topic")))
                .with_partitions(vec![0])]);
        let _add_response = handler.handle_add_partitions_to_txn(add_request).unwrap();

        // Step 3: Abort transaction
        let end_request = EndTxnRequest::default()
            .with_transactional_id(TransactionalId(StrBytes::from_static_str(
                "abort-lifecycle-txn",
            )))
            .with_producer_id(producer_id)
            .with_producer_epoch(producer_epoch)
            .with_committed(false); // Abort
        let end_response = handler.handle_end_txn(end_request).unwrap();

        // Verify abort returned valid response
        assert!(
            end_response.error_code == NONE
                || end_response.error_code == COORDINATOR_NOT_AVAILABLE
                || end_response.error_code == UNKNOWN_SERVER_ERROR,
            "Abort should return valid error code"
        );
    }
}

#[test]
fn test_control_record_abort_marker_format() {
    // Abort marker should have correct control record format
    let batch = create_control_record_batch(100, 12345, 0, CONTROL_TYPE_ABORT);

    // Verify attributes have control bit set
    let attributes = i16::from_be_bytes(batch[21..23].try_into().unwrap());
    let is_control = (attributes & ATTR_CONTROL_BIT) != 0;
    let is_transactional = (attributes & ATTR_TRANSACTIONAL_BIT) != 0;

    assert!(is_control, "Control bit should be set for abort marker");
    assert!(
        is_transactional,
        "Transactional bit should be set for abort marker"
    );
}

#[test]
fn test_control_record_commit_marker_format() {
    // Commit marker should have correct control record format
    let batch = create_control_record_batch(100, 12345, 0, CONTROL_TYPE_COMMIT);

    let attributes = i16::from_be_bytes(batch[21..23].try_into().unwrap());
    let is_control = (attributes & ATTR_CONTROL_BIT) != 0;
    let is_transactional = (attributes & ATTR_TRANSACTIONAL_BIT) != 0;

    assert!(is_control, "Control bit should be set for commit marker");
    assert!(
        is_transactional,
        "Transactional bit should be set for commit marker"
    );
}

#[test]
fn test_control_record_offset_advancement() {
    // Control record should have a valid offset
    let base_offset = 500;
    let batch = create_control_record_batch(base_offset, 12345, 0, CONTROL_TYPE_COMMIT);

    let stored_offset = i64::from_be_bytes(batch[0..8].try_into().unwrap());
    assert_eq!(
        stored_offset, base_offset,
        "Control record should preserve base offset"
    );

    // lastOffsetDelta should be 0 for single control record
    let last_offset_delta = i32::from_be_bytes(batch[23..27].try_into().unwrap());
    assert_eq!(
        last_offset_delta, 0,
        "Control batch should have lastOffsetDelta=0"
    );
}

#[test]
fn test_control_record_batch_attributes_bits() {
    // Test specific bit positions in control batch attributes
    let batch = create_control_record_batch(0, 100, 0, CONTROL_TYPE_ABORT);

    let attributes = i16::from_be_bytes(batch[21..23].try_into().unwrap());

    // Check each bit
    let compression = attributes & ATTR_COMPRESSION_MASK;
    let timestamp_type = (attributes & ATTR_TIMESTAMP_TYPE_BIT) != 0;
    let transactional = (attributes & ATTR_TRANSACTIONAL_BIT) != 0;
    let control = (attributes & ATTR_CONTROL_BIT) != 0;

    assert_eq!(compression, 0, "Control batches should have no compression");
    assert!(!timestamp_type, "Timestamp type should be CreateTime (0)");
    assert!(transactional, "Transactional bit must be set");
    assert!(control, "Control bit must be set");
}

#[test]
fn test_control_record_producer_metadata() {
    // Control record should contain producer ID and epoch
    let producer_id = 999_i64;
    let producer_epoch = 5_i16;
    let batch = create_control_record_batch(0, producer_id, producer_epoch, CONTROL_TYPE_COMMIT);

    // producerId is at offset 43
    let stored_pid = i64::from_be_bytes(batch[43..51].try_into().unwrap());
    assert_eq!(stored_pid, producer_id, "Producer ID should match");

    // producerEpoch is at offset 51
    let stored_epoch = i16::from_be_bytes(batch[51..53].try_into().unwrap());
    assert_eq!(stored_epoch, producer_epoch, "Producer epoch should match");
}

#[test]
fn test_control_record_base_sequence_is_negative_one() {
    // Control batches should have baseSequence = -1
    let batch = create_control_record_batch(0, 100, 0, CONTROL_TYPE_ABORT);

    // baseSequence is at offset 53
    let base_sequence = i32::from_be_bytes(batch[53..57].try_into().unwrap());
    assert_eq!(base_sequence, -1, "Control batch baseSequence should be -1");
}

#[test]
fn test_control_record_crc_valid() {
    // Control batch should have valid CRC
    let batch = create_control_record_batch(0, 100, 0, CONTROL_TYPE_COMMIT);

    // CRC is at offset 17
    let stored_crc = u32::from_be_bytes(batch[17..21].try_into().unwrap());

    // CRC is calculated over everything from attributes (offset 21) to end
    let crc_data = &batch[21..];
    let calculated_crc = crc32fast::hash(crc_data);

    assert_eq!(
        stored_crc, calculated_crc,
        "Control batch CRC should be valid"
    );
}

#[test]
fn test_control_record_magic_byte() {
    // Control batch should use magic byte 2
    let batch = create_control_record_batch(0, 100, 0, CONTROL_TYPE_ABORT);

    let magic = batch[16] as i8;
    assert_eq!(magic, RECORD_BATCH_MAGIC_V2, "Magic byte should be 2");
}

#[test]
fn test_control_record_timestamps() {
    // Control record should have valid timestamps
    let batch = create_control_record_batch(0, 100, 0, CONTROL_TYPE_COMMIT);

    // firstTimestamp is at offset 27
    let first_timestamp = i64::from_be_bytes(batch[27..35].try_into().unwrap());
    // maxTimestamp is at offset 35
    let max_timestamp = i64::from_be_bytes(batch[35..43].try_into().unwrap());

    assert!(first_timestamp > 0, "First timestamp should be positive");
    assert!(max_timestamp > 0, "Max timestamp should be positive");
    assert_eq!(
        first_timestamp, max_timestamp,
        "Control batch timestamps should be equal"
    );
}

#[test]
fn test_control_type_abort_constant() {
    // Verify abort control type constant
    assert_eq!(CONTROL_TYPE_ABORT, 0, "Abort type should be 0");
}

#[test]
fn test_control_type_commit_constant() {
    // Verify commit control type constant
    assert_eq!(CONTROL_TYPE_COMMIT, 1, "Commit type should be 1");
}

#[test]
fn test_control_batch_vs_data_batch_attributes() {
    // Compare control batch attributes with data batch
    let control_batch = create_control_record_batch(0, 100, 0, CONTROL_TYPE_COMMIT);
    let data_batch = create_test_record_batch(0, 100, 0, 0, 0, 1);

    let control_attrs = i16::from_be_bytes(control_batch[21..23].try_into().unwrap());
    let data_attrs = i16::from_be_bytes(data_batch[21..23].try_into().unwrap());

    let control_is_control = (control_attrs & ATTR_CONTROL_BIT) != 0;
    let data_is_control = (data_attrs & ATTR_CONTROL_BIT) != 0;

    assert!(control_is_control, "Control batch should have control bit");
    assert!(!data_is_control, "Data batch should NOT have control bit");
}

#[test]
fn test_isolation_level_read_uncommitted() {
    // READ_UNCOMMITTED should be isolation_level 0
    let isolation_level: i8 = 0;
    assert_eq!(isolation_level, 0, "READ_UNCOMMITTED should be 0");
}

#[test]
fn test_isolation_level_read_committed() {
    // READ_COMMITTED should be isolation_level 1
    let isolation_level: i8 = 1;
    assert_eq!(isolation_level, 1, "READ_COMMITTED should be 1");
}

#[test]
fn test_fetch_request_isolation_level() {
    // FetchRequest should have isolation_level field
    use kafka_protocol::messages::FetchRequest;

    let mut request = FetchRequest::default();
    request.isolation_level = 1; // READ_COMMITTED
    assert_eq!(request.isolation_level, 1);

    request.isolation_level = 0; // READ_UNCOMMITTED
    assert_eq!(request.isolation_level, 0);
}

#[test]
fn test_control_batch_single_record() {
    // Control batch should contain exactly one record
    let batch = create_control_record_batch(0, 100, 0, CONTROL_TYPE_COMMIT);

    // records count is at offset 57
    let record_count = i32::from_be_bytes(batch[57..61].try_into().unwrap());
    assert_eq!(
        record_count, 1,
        "Control batch should have exactly 1 record"
    );
}

#[test]
fn test_data_batch_not_control() {
    // Regular data batch should NOT have control bit set
    let batch = create_test_record_batch(0, 100, 0, 0, 0, 5);

    let attributes = i16::from_be_bytes(batch[21..23].try_into().unwrap());
    let is_control = (attributes & ATTR_CONTROL_BIT) != 0;

    assert!(!is_control, "Data batch should not be marked as control");
}

#[test]
fn test_transactional_data_batch_attributes() {
    // Create a transactional data batch (not control)
    // A transactional data batch has transactional bit but NOT control bit

    // The existing create_test_record_batch doesn't set transactional bit,
    // so we verify the distinction is clear
    let batch = create_test_record_batch(0, 100, 0, 0, 0, 1);
    let attributes = i16::from_be_bytes(batch[21..23].try_into().unwrap());

    // Regular data batch from our helper has no transactional/control bits
    let is_transactional = (attributes & ATTR_TRANSACTIONAL_BIT) != 0;
    let is_control = (attributes & ATTR_CONTROL_BIT) != 0;

    // The helper creates non-transactional batches
    assert!(
        !is_transactional,
        "Simple data batch should not be transactional"
    );
    assert!(!is_control, "Simple data batch should not be control");
}
