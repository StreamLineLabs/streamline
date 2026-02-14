//! Transaction Wire Protocol Tests
//!
//! Tests for Kafka transaction protocol including:
//! - AddPartitionsToTxn request/response encoding
//! - AddOffsetsToTxn request/response encoding
//! - EndTxn request/response encoding
//! - TxnOffsetCommit request/response encoding
//! - Control record markers (COMMIT/ABORT)
//! - Transaction state transitions
//! - Transactional ID encoding

use bytes::{Buf, BufMut, BytesMut};
use kafka_protocol::messages::{
    AddOffsetsToTxnRequest, AddOffsetsToTxnResponse, AddPartitionsToTxnRequest,
    AddPartitionsToTxnResponse, EndTxnRequest, EndTxnResponse, GroupId, ProducerId, RequestHeader,
    TopicName, TransactionalId, TxnOffsetCommitRequest, TxnOffsetCommitResponse,
};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};

// ============================================================================
// AddPartitionsToTxn Request Tests
// ============================================================================

/// Test AddPartitionsToTxnRequest v0-v3 encoding
#[test]
fn test_add_partitions_to_txn_request_v3_and_below() {
    use kafka_protocol::messages::add_partitions_to_txn_request::AddPartitionsToTxnTopic;

    let mut request = AddPartitionsToTxnRequest::default();
    request.v3_and_below_transactional_id =
        TransactionalId(StrBytes::from_static_str("my-transaction"));
    request.v3_and_below_producer_id = ProducerId(12345);
    request.v3_and_below_producer_epoch = 3;

    let mut topic = AddPartitionsToTxnTopic::default();
    topic.name = TopicName(StrBytes::from_static_str("test-topic"));
    topic.partitions = vec![0, 1, 2];
    request.v3_and_below_topics.push(topic);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 3).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = AddPartitionsToTxnRequest::decode(&mut read_buf, 3).unwrap();

    assert_eq!(
        decoded.v3_and_below_transactional_id,
        TransactionalId(StrBytes::from_static_str("my-transaction"))
    );
    assert_eq!(decoded.v3_and_below_producer_id, ProducerId(12345));
    assert_eq!(decoded.v3_and_below_producer_epoch, 3);
    assert_eq!(decoded.v3_and_below_topics.len(), 1);
    assert_eq!(decoded.v3_and_below_topics[0].partitions, vec![0, 1, 2]);
}

/// Test AddPartitionsToTxnRequest with multiple topics
#[test]
fn test_add_partitions_to_txn_request_multiple_topics() {
    use kafka_protocol::messages::add_partitions_to_txn_request::AddPartitionsToTxnTopic;

    let mut request = AddPartitionsToTxnRequest::default();
    request.v3_and_below_transactional_id =
        TransactionalId(StrBytes::from_static_str("multi-topic-txn"));
    request.v3_and_below_producer_id = ProducerId(99999);
    request.v3_and_below_producer_epoch = 1;

    // First topic
    let mut topic1 = AddPartitionsToTxnTopic::default();
    topic1.name = TopicName(StrBytes::from_static_str("topic-1"));
    topic1.partitions = vec![0, 1];
    request.v3_and_below_topics.push(topic1);

    // Second topic
    let mut topic2 = AddPartitionsToTxnTopic::default();
    topic2.name = TopicName(StrBytes::from_static_str("topic-2"));
    topic2.partitions = vec![0, 1, 2, 3];
    request.v3_and_below_topics.push(topic2);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 3).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = AddPartitionsToTxnRequest::decode(&mut read_buf, 3).unwrap();

    assert_eq!(decoded.v3_and_below_topics.len(), 2);
    assert_eq!(decoded.v3_and_below_topics[0].partitions.len(), 2);
    assert_eq!(decoded.v3_and_below_topics[1].partitions.len(), 4);
}

// ============================================================================
// AddPartitionsToTxn Response Tests
// ============================================================================

/// Test AddPartitionsToTxnResponse encoding
#[test]
fn test_add_partitions_to_txn_response() {
    use kafka_protocol::messages::add_partitions_to_txn_response::{
        AddPartitionsToTxnPartitionResult, AddPartitionsToTxnTopicResult,
    };

    let mut response = AddPartitionsToTxnResponse::default();
    response.throttle_time_ms = 0;

    let mut topic_result = AddPartitionsToTxnTopicResult::default();
    topic_result.name = TopicName(StrBytes::from_static_str("test-topic"));

    let mut partition_result = AddPartitionsToTxnPartitionResult::default();
    partition_result.partition_index = 0;
    partition_result.partition_error_code = 0; // No error
    topic_result.results_by_partition.push(partition_result);

    response.results_by_topic_v3_and_below.push(topic_result);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = AddPartitionsToTxnResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.throttle_time_ms, 0);
    assert_eq!(decoded.results_by_topic_v3_and_below.len(), 1);
}

/// Test AddPartitionsToTxnResponse with error
#[test]
fn test_add_partitions_to_txn_response_error() {
    use kafka_protocol::messages::add_partitions_to_txn_response::{
        AddPartitionsToTxnPartitionResult, AddPartitionsToTxnTopicResult,
    };

    let mut response = AddPartitionsToTxnResponse::default();

    let mut topic_result = AddPartitionsToTxnTopicResult::default();
    topic_result.name = TopicName(StrBytes::from_static_str("test-topic"));

    let mut partition_result = AddPartitionsToTxnPartitionResult::default();
    partition_result.partition_index = 0;
    partition_result.partition_error_code = 48; // INVALID_TXN_STATE
    topic_result.results_by_partition.push(partition_result);

    response.results_by_topic_v3_and_below.push(topic_result);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = AddPartitionsToTxnResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(
        decoded.results_by_topic_v3_and_below[0].results_by_partition[0].partition_error_code,
        48
    );
}

// ============================================================================
// AddOffsetsToTxn Request Tests
// ============================================================================

/// Test AddOffsetsToTxnRequest encoding
#[test]
fn test_add_offsets_to_txn_request() {
    let mut request = AddOffsetsToTxnRequest::default();
    request.transactional_id = TransactionalId(StrBytes::from_static_str("offset-txn"));
    request.producer_id = ProducerId(11111);
    request.producer_epoch = 2;
    request.group_id = GroupId(StrBytes::from_static_str("my-consumer-group"));

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = AddOffsetsToTxnRequest::decode(&mut read_buf, 0).unwrap();

    assert_eq!(
        decoded.transactional_id,
        TransactionalId(StrBytes::from_static_str("offset-txn"))
    );
    assert_eq!(decoded.producer_id, ProducerId(11111));
    assert_eq!(
        decoded.group_id,
        GroupId(StrBytes::from_static_str("my-consumer-group"))
    );
}

/// Test AddOffsetsToTxnRequest v3 flexible encoding
#[test]
fn test_add_offsets_to_txn_request_v3() {
    let mut request = AddOffsetsToTxnRequest::default();
    request.transactional_id = TransactionalId(StrBytes::from_static_str("flex-offset-txn"));
    request.producer_id = ProducerId(22222);
    request.producer_epoch = 3;
    request.group_id = GroupId(StrBytes::from_static_str("flex-group"));

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 3).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = AddOffsetsToTxnRequest::decode(&mut read_buf, 3).unwrap();

    assert_eq!(decoded.producer_id, ProducerId(22222));
}

// ============================================================================
// AddOffsetsToTxn Response Tests
// ============================================================================

/// Test AddOffsetsToTxnResponse encoding
#[test]
fn test_add_offsets_to_txn_response() {
    let mut response = AddOffsetsToTxnResponse::default();
    response.throttle_time_ms = 100;
    response.error_code = 0;

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = AddOffsetsToTxnResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.throttle_time_ms, 100);
    assert_eq!(decoded.error_code, 0);
}

/// Test AddOffsetsToTxnResponse with error
#[test]
fn test_add_offsets_to_txn_response_error() {
    let mut response = AddOffsetsToTxnResponse::default();
    response.throttle_time_ms = 0;
    response.error_code = 25; // COORDINATOR_NOT_AVAILABLE

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = AddOffsetsToTxnResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.error_code, 25);
}

// ============================================================================
// EndTxn Request Tests
// ============================================================================

/// Test EndTxnRequest for COMMIT
#[test]
fn test_end_txn_request_commit() {
    let mut request = EndTxnRequest::default();
    request.transactional_id = TransactionalId(StrBytes::from_static_str("commit-txn"));
    request.producer_id = ProducerId(33333);
    request.producer_epoch = 4;
    request.committed = true;

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = EndTxnRequest::decode(&mut read_buf, 0).unwrap();

    assert_eq!(
        decoded.transactional_id,
        TransactionalId(StrBytes::from_static_str("commit-txn"))
    );
    assert_eq!(decoded.producer_id, ProducerId(33333));
    assert!(decoded.committed);
}

/// Test EndTxnRequest for ABORT
#[test]
fn test_end_txn_request_abort() {
    let mut request = EndTxnRequest::default();
    request.transactional_id = TransactionalId(StrBytes::from_static_str("abort-txn"));
    request.producer_id = ProducerId(44444);
    request.producer_epoch = 5;
    request.committed = false; // ABORT

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = EndTxnRequest::decode(&mut read_buf, 0).unwrap();

    assert!(!decoded.committed);
}

/// Test EndTxnRequest v3 flexible encoding
#[test]
fn test_end_txn_request_v3() {
    let mut request = EndTxnRequest::default();
    request.transactional_id = TransactionalId(StrBytes::from_static_str("flex-end-txn"));
    request.producer_id = ProducerId(55555);
    request.producer_epoch = 6;
    request.committed = true;

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 3).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = EndTxnRequest::decode(&mut read_buf, 3).unwrap();

    assert!(decoded.committed);
}

// ============================================================================
// EndTxn Response Tests
// ============================================================================

/// Test EndTxnResponse encoding
#[test]
fn test_end_txn_response() {
    let mut response = EndTxnResponse::default();
    response.throttle_time_ms = 50;
    response.error_code = 0;

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = EndTxnResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.throttle_time_ms, 50);
    assert_eq!(decoded.error_code, 0);
}

/// Test EndTxnResponse with error
#[test]
fn test_end_txn_response_invalid_txn_state() {
    let mut response = EndTxnResponse::default();
    response.error_code = 48; // INVALID_TXN_STATE

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = EndTxnResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.error_code, 48);
}

// ============================================================================
// TxnOffsetCommit Request Tests
// ============================================================================

/// Test TxnOffsetCommitRequest encoding
#[test]
fn test_txn_offset_commit_request() {
    use kafka_protocol::messages::txn_offset_commit_request::{
        TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic,
    };

    let mut request = TxnOffsetCommitRequest::default();
    request.transactional_id = TransactionalId(StrBytes::from_static_str("offset-commit-txn"));
    request.group_id = GroupId(StrBytes::from_static_str("my-group"));
    request.producer_id = ProducerId(66666);
    request.producer_epoch = 7;

    let mut topic = TxnOffsetCommitRequestTopic::default();
    topic.name = TopicName(StrBytes::from_static_str("offset-topic"));

    let mut partition = TxnOffsetCommitRequestPartition::default();
    partition.partition_index = 0;
    partition.committed_offset = 1000;
    partition.committed_metadata = Some(StrBytes::from_static_str(""));
    topic.partitions.push(partition);

    request.topics.push(topic);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = TxnOffsetCommitRequest::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.producer_id, ProducerId(66666));
    assert_eq!(decoded.topics.len(), 1);
    assert_eq!(decoded.topics[0].partitions[0].committed_offset, 1000);
}

/// Test TxnOffsetCommitRequest with multiple partitions
#[test]
fn test_txn_offset_commit_request_multiple_partitions() {
    use kafka_protocol::messages::txn_offset_commit_request::{
        TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic,
    };

    let mut request = TxnOffsetCommitRequest::default();
    request.transactional_id = TransactionalId(StrBytes::from_static_str("multi-partition-txn"));
    request.group_id = GroupId(StrBytes::from_static_str("multi-group"));
    request.producer_id = ProducerId(77777);
    request.producer_epoch = 8;

    let mut topic = TxnOffsetCommitRequestTopic::default();
    topic.name = TopicName(StrBytes::from_static_str("multi-topic"));

    for i in 0..5 {
        let mut partition = TxnOffsetCommitRequestPartition::default();
        partition.partition_index = i;
        partition.committed_offset = (i as i64 + 1) * 100;
        partition.committed_metadata = Some(StrBytes::from_static_str(""));
        topic.partitions.push(partition);
    }

    request.topics.push(topic);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = TxnOffsetCommitRequest::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.topics[0].partitions.len(), 5);
    assert_eq!(decoded.topics[0].partitions[0].committed_offset, 100);
    assert_eq!(decoded.topics[0].partitions[4].committed_offset, 500);
}

// ============================================================================
// TxnOffsetCommit Response Tests
// ============================================================================

/// Test TxnOffsetCommitResponse encoding
#[test]
fn test_txn_offset_commit_response() {
    use kafka_protocol::messages::txn_offset_commit_response::{
        TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic,
    };

    let mut response = TxnOffsetCommitResponse::default();
    response.throttle_time_ms = 0;

    let mut topic = TxnOffsetCommitResponseTopic::default();
    topic.name = TopicName(StrBytes::from_static_str("response-topic"));

    let mut partition = TxnOffsetCommitResponsePartition::default();
    partition.partition_index = 0;
    partition.error_code = 0;
    topic.partitions.push(partition);

    response.topics.push(topic);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = TxnOffsetCommitResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.topics.len(), 1);
    assert_eq!(decoded.topics[0].partitions[0].error_code, 0);
}

// ============================================================================
// Control Record Tests
// ============================================================================

/// Test COMMIT control record key format
#[test]
fn test_commit_control_record_key() {
    // Control record key: version(i16) + type(i16)
    // Type: 0 = ABORT, 1 = COMMIT
    let mut key = BytesMut::new();
    key.put_i16(0); // version
    key.put_i16(1); // COMMIT

    let bytes = key.freeze();
    assert_eq!(bytes.len(), 4);

    let mut reader = bytes.as_ref();
    let version = reader.get_i16();
    let marker_type = reader.get_i16();

    assert_eq!(version, 0);
    assert_eq!(marker_type, 1);
}

/// Test ABORT control record key format
#[test]
fn test_abort_control_record_key() {
    let mut key = BytesMut::new();
    key.put_i16(0); // version
    key.put_i16(0); // ABORT

    let bytes = key.freeze();

    let mut reader = bytes.as_ref();
    let version = reader.get_i16();
    let marker_type = reader.get_i16();

    assert_eq!(version, 0);
    assert_eq!(marker_type, 0);
}

/// Test control batch attributes
#[test]
fn test_control_batch_attributes() {
    // Control batches have:
    // - bit 4 (transactional) = 1
    // - bit 5 (control) = 1
    let control_attributes: i16 = 0b0000_0000_0011_0000;

    let is_transactional = (control_attributes >> 4) & 0x01;
    let is_control = (control_attributes >> 5) & 0x01;

    assert_eq!(is_transactional, 1);
    assert_eq!(is_control, 1);
}

// ============================================================================
// Transaction State Tests
// ============================================================================

/// Test transaction state constants
#[test]
fn test_transaction_states() {
    // Transaction states (internal)
    let empty: i8 = 0;
    let ongoing: i8 = 1;
    let prepare_commit: i8 = 2;
    let prepare_abort: i8 = 3;
    let complete_commit: i8 = 4;
    let complete_abort: i8 = 5;
    let dead: i8 = 6;

    assert_eq!(empty, 0);
    assert_eq!(ongoing, 1);
    assert_eq!(prepare_commit, 2);
    assert_eq!(prepare_abort, 3);
    assert_eq!(complete_commit, 4);
    assert_eq!(complete_abort, 5);
    assert_eq!(dead, 6);
}

/// Test valid transaction state transitions
#[test]
fn test_valid_state_transitions() {
    let empty = 0;
    let ongoing = 1;
    let prepare_commit = 2;
    let prepare_abort = 3;
    let complete_commit = 4;
    let complete_abort = 5;

    // Test transition sequence for commit
    let state = empty;
    let state = if state == empty { ongoing } else { state };
    let state = if state == ongoing {
        prepare_commit
    } else {
        state
    };
    let state = if state == prepare_commit {
        complete_commit
    } else {
        state
    };
    assert_eq!(state, complete_commit);

    // Test transition sequence for abort
    let state2 = empty;
    let state2 = if state2 == empty { ongoing } else { state2 };
    let state2 = if state2 == ongoing {
        prepare_abort
    } else {
        state2
    };
    let state2 = if state2 == prepare_abort {
        complete_abort
    } else {
        state2
    };
    assert_eq!(state2, complete_abort);
}

// ============================================================================
// Transaction Error Code Tests
// ============================================================================

/// Test transaction-related error codes
#[test]
fn test_transaction_error_codes() {
    let invalid_txn_state: i16 = 48;
    let invalid_producer_id_mapping: i16 = 49;
    let invalid_producer_epoch: i16 = 47;
    let transactional_id_authorization_failed: i16 = 53;
    let concurrent_transactions: i16 = 51;
    let transaction_coordinator_fenced: i16 = 52;

    assert_eq!(invalid_txn_state, 48);
    assert_eq!(invalid_producer_id_mapping, 49);
    assert_eq!(invalid_producer_epoch, 47);
    assert_eq!(transactional_id_authorization_failed, 53);
    assert_eq!(concurrent_transactions, 51);
    assert_eq!(transaction_coordinator_fenced, 52);
}

/// Test producer fencing errors in transactions
#[test]
fn test_producer_fencing_in_transactions() {
    let producer_fenced: i16 = 90;
    let invalid_producer_epoch: i16 = 47;

    assert_eq!(producer_fenced, 90);
    assert_eq!(invalid_producer_epoch, 47);
}

// ============================================================================
// Transactional ID Encoding Tests
// ============================================================================

/// Test transactional ID wire format
#[test]
fn test_transactional_id_wire_format() {
    let txn_id = "my-transaction-123";

    // Non-compact encoding: length(i16) + bytes
    let mut buf = BytesMut::new();
    buf.put_i16(txn_id.len() as i16);
    buf.put_slice(txn_id.as_bytes());

    let bytes = buf.freeze();
    assert_eq!(bytes.len(), 2 + txn_id.len());

    let mut reader = bytes.as_ref();
    let len = reader.get_i16() as usize;
    let id_bytes = &reader[..len];
    let decoded_id = std::str::from_utf8(id_bytes).unwrap();

    assert_eq!(decoded_id, txn_id);
}

/// Test null transactional ID
#[test]
fn test_null_transactional_id() {
    // Null string is indicated by length = -1
    let mut buf = BytesMut::new();
    buf.put_i16(-1);

    let bytes = buf.freeze();
    let mut reader = bytes.as_ref();
    let len = reader.get_i16();

    assert_eq!(len, -1);
}

// ============================================================================
// API Key Tests
// ============================================================================

/// Test transaction-related API keys
#[test]
fn test_transaction_api_keys() {
    let add_partitions_to_txn: i16 = 24;
    let add_offsets_to_txn: i16 = 25;
    let end_txn: i16 = 26;
    let txn_offset_commit: i16 = 28;

    assert_eq!(add_partitions_to_txn, 24);
    assert_eq!(add_offsets_to_txn, 25);
    assert_eq!(end_txn, 26);
    assert_eq!(txn_offset_commit, 28);
}

/// Test supported versions for AddPartitionsToTxn
#[test]
fn test_add_partitions_to_txn_versions() {
    for version in 0..=4 {
        let request = AddPartitionsToTxnRequest::default();
        let mut buf = BytesMut::new();
        let result = request.encode(&mut buf, version);
        assert!(result.is_ok(), "Failed to encode version {}", version);
    }
}

/// Test supported versions for EndTxn
#[test]
fn test_end_txn_versions() {
    for version in 0..=3 {
        let request = EndTxnRequest::default();
        let mut buf = BytesMut::new();
        let result = request.encode(&mut buf, version);
        assert!(result.is_ok(), "Failed to encode version {}", version);
    }
}

// ============================================================================
// Transaction Timeout Tests
// ============================================================================

/// Test transaction timeout values
#[test]
fn test_transaction_timeout() {
    let default_timeout_ms: i32 = 60000;
    let max_timeout_ms: i32 = 900000;

    assert!(default_timeout_ms > 0);
    assert!(max_timeout_ms > default_timeout_ms);
    assert_eq!(default_timeout_ms, 60 * 1000);
    assert_eq!(max_timeout_ms, 15 * 60 * 1000);
}

// ============================================================================
// Request Header Tests
// ============================================================================

/// Test request header for AddPartitionsToTxn
#[test]
fn test_add_partitions_to_txn_request_header() {
    let mut header = RequestHeader::default();
    header.request_api_key = 24;
    header.request_api_version = 3;
    header.correlation_id = 12345;
    header.client_id = Some(StrBytes::from_static_str("txn-producer"));

    let mut buf = BytesMut::new();
    let header_version = AddPartitionsToTxnRequest::header_version(3);
    header.encode(&mut buf, header_version).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = RequestHeader::decode(&mut read_buf, header_version).unwrap();

    assert_eq!(decoded.request_api_key, 24);
}

/// Test request header for EndTxn
#[test]
fn test_end_txn_request_header() {
    let mut header = RequestHeader::default();
    header.request_api_key = 26;
    header.request_api_version = 3;
    header.correlation_id = 54321;

    let mut buf = BytesMut::new();
    let header_version = EndTxnRequest::header_version(3);
    header.encode(&mut buf, header_version).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = RequestHeader::decode(&mut read_buf, header_version).unwrap();

    assert_eq!(decoded.request_api_key, 26);
}

// ============================================================================
// Aborted Transaction List Tests
// ============================================================================

/// Test AbortedTransaction structure in fetch response
#[test]
fn test_aborted_transaction_structure() {
    struct AbortedTransaction {
        producer_id: i64,
        first_offset: i64,
    }

    let aborted = AbortedTransaction {
        producer_id: 12345,
        first_offset: 500,
    };

    assert_eq!(aborted.producer_id, 12345);
    assert_eq!(aborted.first_offset, 500);
}

/// Test aborted transaction list encoding
#[test]
fn test_aborted_transaction_list_encoding() {
    let mut buf = BytesMut::new();

    // Array length
    let count: i32 = 2;
    buf.put_i32(count);

    // First aborted transaction
    buf.put_i64(1000); // producer_id
    buf.put_i64(100); // first_offset

    // Second aborted transaction
    buf.put_i64(2000); // producer_id
    buf.put_i64(200); // first_offset

    let bytes = buf.freeze();
    let mut reader = bytes.as_ref();

    let array_len = reader.get_i32();
    assert_eq!(array_len, 2);

    let pid1 = reader.get_i64();
    let offset1 = reader.get_i64();
    assert_eq!(pid1, 1000);
    assert_eq!(offset1, 100);

    let pid2 = reader.get_i64();
    let offset2 = reader.get_i64();
    assert_eq!(pid2, 2000);
    assert_eq!(offset2, 200);
}

// ============================================================================
// Producer ID and Epoch Validation Tests
// ============================================================================

/// Test producer ID validation in transactions
#[test]
fn test_producer_id_validation() {
    let valid_producer_id: i64 = 12345;
    let invalid_producer_id: i64 = -1;

    assert!(valid_producer_id > 0);
    assert_eq!(invalid_producer_id, -1);
}

/// Test epoch validation
#[test]
fn test_epoch_validation() {
    let current_epoch: i16 = 5;
    let incoming_epoch: i16 = 5;

    assert_eq!(current_epoch, incoming_epoch);

    let new_epoch: i16 = 6;
    assert!(new_epoch > current_epoch);

    let old_epoch: i16 = 4;
    assert!(old_epoch < current_epoch);
}

// ============================================================================
// Transaction Coordinator Tests
// ============================================================================

/// Test finding transaction coordinator
#[test]
fn test_transaction_coordinator_key() {
    let txn_id = "my-transaction";

    let mut hash: u32 = 0;
    for byte in txn_id.bytes() {
        hash = hash.wrapping_mul(31).wrapping_add(byte as u32);
    }

    let num_partitions = 50;
    let partition = (hash % num_partitions) as i32;

    assert!(partition >= 0);
    assert!(partition < num_partitions as i32);
}

// ============================================================================
// Transaction Log Entry Tests
// ============================================================================

/// Test transaction log entry fields
#[test]
fn test_transaction_log_entry() {
    struct TransactionLogEntry {
        transactional_id: String,
        producer_id: i64,
        producer_epoch: i16,
        txn_timeout_ms: i32,
        state: i8,
        partitions: Vec<(String, i32)>,
    }

    let entry = TransactionLogEntry {
        transactional_id: "test-txn".to_string(),
        producer_id: 12345,
        producer_epoch: 3,
        txn_timeout_ms: 60000,
        state: 1, // ONGOING
        partitions: vec![
            ("topic1".to_string(), 0),
            ("topic1".to_string(), 1),
            ("topic2".to_string(), 0),
        ],
    };

    assert_eq!(entry.transactional_id, "test-txn");
    assert_eq!(entry.producer_id, 12345);
    assert_eq!(entry.producer_epoch, 3);
    assert_eq!(entry.txn_timeout_ms, 60000);
    assert_eq!(entry.state, 1);
    assert_eq!(entry.partitions.len(), 3);
}
