//! Protocol Fetch Features Tests - Phase 4
//!
//! Tests for Kafka protocol fetch-related features:
//! - Fetch sessions (v7+): Session ID, epoch, incremental fetch
//! - Isolation levels: READ_UNCOMMITTED vs READ_COMMITTED
//! - Max bytes and partition limits
//! - Leader epoch in fetch requests
//!
//! Source: https://kafka.apache.org/protocol.html

use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::*;
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};

// ============================================================================
// Isolation Level Constants
// ============================================================================

/// Read uncommitted data (including transactional records not yet committed)
const READ_UNCOMMITTED: i8 = 0;

/// Read only committed data (transactional records only after commit)
const READ_COMMITTED: i8 = 1;

// ============================================================================
// Fetch Session Constants
// ============================================================================

/// No session - full fetch request required
const INVALID_SESSION_ID: i32 = 0;

/// Initial session epoch for new sessions
const INITIAL_EPOCH: i32 = 0;

/// Epoch value indicating no session
const FINAL_EPOCH: i32 = -1;

// ============================================================================
// Phase 4: Isolation Level Tests
// ============================================================================

#[test]
fn test_isolation_level_constants() {
    // Per Kafka spec
    assert_eq!(READ_UNCOMMITTED, 0);
    assert_eq!(READ_COMMITTED, 1);
}

#[test]
fn test_fetch_request_isolation_level_v4() {
    use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};

    // isolation_level field added in v4
    let request = FetchRequest::default()
        .with_replica_id(BrokerId(-1))
        .with_max_wait_ms(500)
        .with_min_bytes(1)
        .with_max_bytes(1024 * 1024)
        .with_isolation_level(READ_UNCOMMITTED)
        .with_topics(vec![FetchTopic::default()
            .with_topic(TopicName::from(StrBytes::from_static_str("test-topic")))
            .with_partitions(vec![FetchPartition::default()
                .with_partition(0)
                .with_fetch_offset(0)
                .with_partition_max_bytes(1024 * 1024)])]);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 4).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = FetchRequest::decode(&mut bytes, 4).unwrap();

    assert_eq!(decoded.isolation_level, READ_UNCOMMITTED);
}

#[test]
fn test_fetch_request_read_committed() {
    use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};

    let request = FetchRequest::default()
        .with_replica_id(BrokerId(-1))
        .with_max_wait_ms(500)
        .with_min_bytes(1)
        .with_max_bytes(1024 * 1024)
        .with_isolation_level(READ_COMMITTED)
        .with_topics(vec![FetchTopic::default()
            .with_topic(TopicName::from(StrBytes::from_static_str("test-topic")))
            .with_partitions(vec![FetchPartition::default()
                .with_partition(0)
                .with_fetch_offset(0)
                .with_partition_max_bytes(1024 * 1024)])]);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 4).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = FetchRequest::decode(&mut bytes, 4).unwrap();

    assert_eq!(decoded.isolation_level, READ_COMMITTED);
}

#[test]
fn test_list_offsets_request_isolation_level() {
    use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};

    // ListOffsets v2+ supports isolation_level
    let request = ListOffsetsRequest::default()
        .with_replica_id(BrokerId(-1))
        .with_isolation_level(READ_COMMITTED)
        .with_topics(vec![ListOffsetsTopic::default()
            .with_name(TopicName::from(StrBytes::from_static_str("test-topic")))
            .with_partitions(vec![ListOffsetsPartition::default()
                .with_partition_index(0)
                .with_timestamp(-1)])]);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 4).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = ListOffsetsRequest::decode(&mut bytes, 4).unwrap();

    assert_eq!(decoded.isolation_level, READ_COMMITTED);
}

#[test]
fn test_isolation_level_affects_lso() {
    // When READ_COMMITTED, FetchResponse includes last_stable_offset (LSO)
    // LSO is the offset of the last committed transaction
    use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};

    let response = FetchResponse::default()
        .with_throttle_time_ms(0)
        .with_responses(vec![FetchableTopicResponse::default()
            .with_topic(TopicName::from(StrBytes::from_static_str("test-topic")))
            .with_partitions(vec![PartitionData::default()
                .with_partition_index(0)
                .with_error_code(0)
                .with_high_watermark(100)
                .with_last_stable_offset(95) // LSO < HW means uncommitted txn data
                .with_log_start_offset(0)])]);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 4).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = FetchResponse::decode(&mut bytes, 4).unwrap();

    let partition = &decoded.responses[0].partitions[0];
    assert_eq!(partition.high_watermark, 100);
    assert_eq!(partition.last_stable_offset, 95);
    // Offset 95-99 contain uncommitted transaction data
}

#[test]
fn test_fetch_response_aborted_transactions() {
    use kafka_protocol::messages::fetch_response::{
        AbortedTransaction, FetchableTopicResponse, PartitionData,
    };

    // v4+ includes aborted_transactions for READ_COMMITTED consumers
    let response = FetchResponse::default().with_responses(vec![FetchableTopicResponse::default()
        .with_topic(TopicName::from(StrBytes::from_static_str("test-topic")))
        .with_partitions(vec![PartitionData::default()
            .with_partition_index(0)
            .with_error_code(0)
            .with_high_watermark(100)
            .with_last_stable_offset(100)
            .with_aborted_transactions(Some(vec![
                AbortedTransaction::default()
                    .with_producer_id(ProducerId(1000))
                    .with_first_offset(50),
                AbortedTransaction::default()
                    .with_producer_id(ProducerId(1001))
                    .with_first_offset(75),
            ]))])]);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 4).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = FetchResponse::decode(&mut bytes, 4).unwrap();

    let aborted = decoded.responses[0].partitions[0]
        .aborted_transactions
        .as_ref()
        .unwrap();
    assert_eq!(aborted.len(), 2);
    assert_eq!(aborted[0].producer_id, 1000);
    assert_eq!(aborted[0].first_offset, 50);
    assert_eq!(aborted[1].producer_id, 1001);
    assert_eq!(aborted[1].first_offset, 75);
}

// ============================================================================
// Phase 4: Fetch Session Tests
// ============================================================================

#[test]
fn test_fetch_session_constants() {
    assert_eq!(INVALID_SESSION_ID, 0);
    assert_eq!(INITIAL_EPOCH, 0);
    assert_eq!(FINAL_EPOCH, -1);
}

#[test]
fn test_fetch_request_session_id_v7() {
    use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};

    // session_id and session_epoch added in v7
    let request = FetchRequest::default()
        .with_replica_id(BrokerId(-1))
        .with_max_wait_ms(500)
        .with_min_bytes(1)
        .with_max_bytes(1024 * 1024)
        .with_isolation_level(READ_UNCOMMITTED)
        .with_session_id(INVALID_SESSION_ID)
        .with_session_epoch(FINAL_EPOCH)
        .with_topics(vec![FetchTopic::default()
            .with_topic(TopicName::from(StrBytes::from_static_str("test-topic")))
            .with_partitions(vec![FetchPartition::default()
                .with_partition(0)
                .with_fetch_offset(0)
                .with_partition_max_bytes(1024 * 1024)])]);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 7).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = FetchRequest::decode(&mut bytes, 7).unwrap();

    assert_eq!(decoded.session_id, INVALID_SESSION_ID);
    assert_eq!(decoded.session_epoch, FINAL_EPOCH);
}

#[test]
fn test_fetch_response_session_id() {
    use kafka_protocol::messages::fetch_response::FetchableTopicResponse;

    // Response includes session_id for session management
    let response = FetchResponse::default()
        .with_throttle_time_ms(0)
        .with_session_id(12345) // Server-assigned session ID
        .with_responses(vec![FetchableTopicResponse::default()
            .with_topic(TopicName::from(StrBytes::from_static_str("test-topic")))]);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 7).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = FetchResponse::decode(&mut bytes, 7).unwrap();

    assert_eq!(decoded.session_id, 12345);
}

#[test]
fn test_fetch_session_creation() {
    use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
    use kafka_protocol::messages::fetch_response::FetchableTopicResponse;

    // Client creates session by sending session_id=0, epoch=-1
    let create_request = FetchRequest::default()
        .with_replica_id(BrokerId(-1))
        .with_max_wait_ms(500)
        .with_min_bytes(1)
        .with_max_bytes(1024 * 1024)
        .with_session_id(0) // Request new session
        .with_session_epoch(-1) // New session epoch
        .with_topics(vec![FetchTopic::default()
            .with_topic(TopicName::from(StrBytes::from_static_str("test-topic")))
            .with_partitions(vec![FetchPartition::default()
                .with_partition(0)
                .with_fetch_offset(0)
                .with_partition_max_bytes(1024 * 1024)])]);

    let mut buf = BytesMut::new();
    create_request.encode(&mut buf, 7).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded_request = FetchRequest::decode(&mut bytes, 7).unwrap();

    assert_eq!(decoded_request.session_id, 0);
    assert_eq!(decoded_request.session_epoch, -1);

    // Server responds with new session_id
    let create_response = FetchResponse::default()
        .with_session_id(99999) // New session assigned
        .with_responses(vec![FetchableTopicResponse::default()
            .with_topic(TopicName::from(StrBytes::from_static_str("test-topic")))]);

    let mut buf2 = BytesMut::new();
    create_response.encode(&mut buf2, 7).unwrap();

    let mut bytes2 = Bytes::from(buf2);
    let decoded_response = FetchResponse::decode(&mut bytes2, 7).unwrap();

    assert!(decoded_response.session_id > 0);
}

#[test]
fn test_fetch_session_incremental() {
    use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};

    // Incremental fetch uses existing session
    let incremental_request = FetchRequest::default()
        .with_replica_id(BrokerId(-1))
        .with_max_wait_ms(500)
        .with_min_bytes(1)
        .with_max_bytes(1024 * 1024)
        .with_session_id(99999) // Existing session
        .with_session_epoch(1) // Incremented epoch
        .with_topics(vec![
            // Only partitions with changes need to be included
            FetchTopic::default()
                .with_topic(TopicName::from(StrBytes::from_static_str("test-topic")))
                .with_partitions(vec![FetchPartition::default()
                    .with_partition(0)
                    .with_fetch_offset(100) // New offset
                    .with_partition_max_bytes(1024 * 1024)]),
        ]);

    let mut buf = BytesMut::new();
    incremental_request.encode(&mut buf, 7).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = FetchRequest::decode(&mut bytes, 7).unwrap();

    assert_eq!(decoded.session_id, 99999);
    assert_eq!(decoded.session_epoch, 1);
}

#[test]
fn test_fetch_session_close() {
    use kafka_protocol::messages::fetch_request::FetchTopic;

    // Close session by sending session_id with epoch=-1
    let close_request = FetchRequest::default()
        .with_replica_id(BrokerId(-1))
        .with_max_wait_ms(500)
        .with_min_bytes(1)
        .with_max_bytes(1024 * 1024)
        .with_session_id(99999) // Session to close
        .with_session_epoch(FINAL_EPOCH) // -1 indicates close
        .with_topics(vec![FetchTopic::default().with_topic(TopicName::from(
            StrBytes::from_static_str("test-topic"),
        ))]);

    let mut buf = BytesMut::new();
    close_request.encode(&mut buf, 7).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = FetchRequest::decode(&mut bytes, 7).unwrap();

    assert_eq!(decoded.session_id, 99999);
    assert_eq!(decoded.session_epoch, -1);
}

#[test]
fn test_fetch_request_forgotten_topics() {
    use kafka_protocol::messages::fetch_request::{FetchTopic, ForgottenTopic};

    // v7+ supports forgotten_topics_data to remove partitions from session
    let request = FetchRequest::default()
        .with_replica_id(BrokerId(-1))
        .with_max_wait_ms(500)
        .with_min_bytes(1)
        .with_max_bytes(1024 * 1024)
        .with_session_id(99999)
        .with_session_epoch(2)
        .with_topics(vec![FetchTopic::default().with_topic(TopicName::from(
            StrBytes::from_static_str("active-topic"),
        ))])
        .with_forgotten_topics_data(vec![ForgottenTopic::default()
            .with_topic(TopicName::from(StrBytes::from_static_str("removed-topic")))
            .with_partitions(vec![0, 1, 2])]);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 7).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = FetchRequest::decode(&mut bytes, 7).unwrap();

    assert_eq!(decoded.forgotten_topics_data.len(), 1);
    assert_eq!(decoded.forgotten_topics_data[0].partitions, vec![0, 1, 2]);
}

// ============================================================================
// Phase 4: Fetch Size and Limit Tests
// ============================================================================

#[test]
fn test_fetch_request_max_bytes() {
    use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};

    let request = FetchRequest::default()
        .with_replica_id(BrokerId(-1))
        .with_max_wait_ms(500)
        .with_min_bytes(1)
        .with_max_bytes(50 * 1024 * 1024) // 50MB
        .with_topics(vec![FetchTopic::default()
            .with_topic(TopicName::from(StrBytes::from_static_str("test-topic")))
            .with_partitions(vec![FetchPartition::default()
                .with_partition(0)
                .with_fetch_offset(0)
                .with_partition_max_bytes(10 * 1024 * 1024)])]);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 11).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = FetchRequest::decode(&mut bytes, 11).unwrap();

    assert_eq!(decoded.max_bytes, 50 * 1024 * 1024);
    assert_eq!(
        decoded.topics[0].partitions[0].partition_max_bytes,
        10 * 1024 * 1024
    );
}

#[test]
fn test_fetch_request_min_bytes() {
    use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};

    // min_bytes controls when broker responds
    let request = FetchRequest::default()
        .with_replica_id(BrokerId(-1))
        .with_max_wait_ms(5000)
        .with_min_bytes(1024) // Wait for at least 1KB
        .with_max_bytes(1024 * 1024)
        .with_topics(vec![FetchTopic::default()
            .with_topic(TopicName::from(StrBytes::from_static_str("test-topic")))
            .with_partitions(vec![FetchPartition::default()
                .with_partition(0)
                .with_fetch_offset(0)
                .with_partition_max_bytes(1024 * 1024)])]);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 11).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = FetchRequest::decode(&mut bytes, 11).unwrap();

    assert_eq!(decoded.min_bytes, 1024);
    assert_eq!(decoded.max_wait_ms, 5000);
}

#[test]
fn test_fetch_request_max_wait_ms() {
    use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};

    // max_wait_ms is the maximum time broker waits for min_bytes
    let request = FetchRequest::default()
        .with_replica_id(BrokerId(-1))
        .with_max_wait_ms(10000) // 10 seconds
        .with_min_bytes(100)
        .with_max_bytes(1024 * 1024)
        .with_topics(vec![FetchTopic::default()
            .with_topic(TopicName::from(StrBytes::from_static_str("test-topic")))
            .with_partitions(vec![FetchPartition::default()
                .with_partition(0)
                .with_fetch_offset(0)
                .with_partition_max_bytes(1024 * 1024)])]);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 11).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = FetchRequest::decode(&mut bytes, 11).unwrap();

    assert_eq!(decoded.max_wait_ms, 10000);
}

// ============================================================================
// Phase 4: Leader Epoch Tests
// ============================================================================

#[test]
fn test_fetch_request_current_leader_epoch_v9() {
    use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};

    // current_leader_epoch added in v9 for fencing
    let request = FetchRequest::default()
        .with_replica_id(BrokerId(-1))
        .with_max_wait_ms(500)
        .with_min_bytes(1)
        .with_max_bytes(1024 * 1024)
        .with_topics(vec![FetchTopic::default()
            .with_topic(TopicName::from(StrBytes::from_static_str("test-topic")))
            .with_partitions(vec![FetchPartition::default()
                .with_partition(0)
                .with_fetch_offset(0)
                .with_current_leader_epoch(5) // Leader epoch for fencing
                .with_partition_max_bytes(1024 * 1024)])]);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 9).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = FetchRequest::decode(&mut bytes, 9).unwrap();

    assert_eq!(decoded.topics[0].partitions[0].current_leader_epoch, 5);
}

#[test]
fn test_fetch_response_preferred_read_replica() {
    use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};

    // v11+ includes preferred_read_replica for follower fetching
    let response = FetchResponse::default()
        .with_throttle_time_ms(0)
        .with_responses(vec![FetchableTopicResponse::default()
            .with_topic(TopicName::from(StrBytes::from_static_str("test-topic")))
            .with_partitions(vec![PartitionData::default()
                .with_partition_index(0)
                .with_error_code(0)
                .with_high_watermark(100)
                .with_preferred_read_replica(BrokerId(2))])]);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 11).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = FetchResponse::decode(&mut bytes, 11).unwrap();

    assert_eq!(
        decoded.responses[0].partitions[0].preferred_read_replica,
        BrokerId(2)
    );
}

// ============================================================================
// Phase 4: Fetch Request Rack Awareness Tests
// ============================================================================

#[test]
fn test_fetch_request_rack_id() {
    use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};

    // rack_id added in v11 for rack-aware fetching
    let request = FetchRequest::default()
        .with_replica_id(BrokerId(-1))
        .with_max_wait_ms(500)
        .with_min_bytes(1)
        .with_max_bytes(1024 * 1024)
        .with_rack_id(StrBytes::from_static_str("rack-1"))
        .with_topics(vec![FetchTopic::default()
            .with_topic(TopicName::from(StrBytes::from_static_str("test-topic")))
            .with_partitions(vec![FetchPartition::default()
                .with_partition(0)
                .with_fetch_offset(0)
                .with_partition_max_bytes(1024 * 1024)])]);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 11).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = FetchRequest::decode(&mut bytes, 11).unwrap();

    assert_eq!(decoded.rack_id.as_str(), "rack-1");
}

// ============================================================================
// Phase 4: Fetch Response Record Tests
// ============================================================================

#[test]
fn test_fetch_response_records_field() {
    use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};

    // Test that records field can be set and encoded
    // Records are opaque bytes from the protocol perspective
    let record_bytes = Bytes::from_static(&[
        0x00, 0x00, 0x00, 0x00, // base offset (8 bytes)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, // batch length
        0x00, 0x00, 0x00, 0x00, // partition leader epoch
        0x02, // magic
        0x00, 0x00, 0x00, 0x00, // crc (placeholder)
        0x00, 0x00, // attributes
        0x00, 0x00, 0x00,
        0x00, // last offset delta
              // ... truncated for test simplicity
    ]);

    let response = FetchResponse::default()
        .with_throttle_time_ms(0)
        .with_responses(vec![FetchableTopicResponse::default()
            .with_topic(TopicName::from(StrBytes::from_static_str("test-topic")))
            .with_partitions(vec![PartitionData::default()
                .with_partition_index(0)
                .with_error_code(0)
                .with_high_watermark(2)
                .with_records(Some(record_bytes.clone()))])]);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 12).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = FetchResponse::decode(&mut bytes, 12).unwrap();

    assert!(decoded.responses[0].partitions[0].records.is_some());
}

#[test]
fn test_fetch_response_log_start_offset() {
    use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};

    // log_start_offset added in v5
    let response = FetchResponse::default().with_responses(vec![FetchableTopicResponse::default()
        .with_topic(TopicName::from(StrBytes::from_static_str("test-topic")))
        .with_partitions(vec![PartitionData::default()
            .with_partition_index(0)
            .with_error_code(0)
            .with_high_watermark(1000)
            .with_log_start_offset(100)])]);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 5).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = FetchResponse::decode(&mut bytes, 5).unwrap();

    assert_eq!(decoded.responses[0].partitions[0].log_start_offset, 100);
}

// ============================================================================
// Phase 4: Multiple Partitions and Topics Tests
// ============================================================================

#[test]
fn test_fetch_request_multiple_topics() {
    use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};

    let request = FetchRequest::default()
        .with_replica_id(BrokerId(-1))
        .with_max_wait_ms(500)
        .with_min_bytes(1)
        .with_max_bytes(10 * 1024 * 1024)
        .with_topics(vec![
            FetchTopic::default()
                .with_topic(TopicName::from(StrBytes::from_static_str("topic-a")))
                .with_partitions(vec![
                    FetchPartition::default()
                        .with_partition(0)
                        .with_fetch_offset(0)
                        .with_partition_max_bytes(1024 * 1024),
                    FetchPartition::default()
                        .with_partition(1)
                        .with_fetch_offset(100)
                        .with_partition_max_bytes(1024 * 1024),
                ]),
            FetchTopic::default()
                .with_topic(TopicName::from(StrBytes::from_static_str("topic-b")))
                .with_partitions(vec![FetchPartition::default()
                    .with_partition(0)
                    .with_fetch_offset(50)
                    .with_partition_max_bytes(2 * 1024 * 1024)]),
        ]);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 11).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = FetchRequest::decode(&mut bytes, 11).unwrap();

    assert_eq!(decoded.topics.len(), 2);
    assert_eq!(decoded.topics[0].partitions.len(), 2);
    assert_eq!(decoded.topics[1].partitions.len(), 1);
    assert_eq!(decoded.topics[0].partitions[1].fetch_offset, 100);
    assert_eq!(decoded.topics[1].partitions[0].fetch_offset, 50);
}

#[test]
fn test_fetch_response_multiple_partitions() {
    use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};

    let response = FetchResponse::default()
        .with_throttle_time_ms(0)
        .with_responses(vec![FetchableTopicResponse::default()
            .with_topic(TopicName::from(StrBytes::from_static_str("test-topic")))
            .with_partitions(vec![
                PartitionData::default()
                    .with_partition_index(0)
                    .with_error_code(0)
                    .with_high_watermark(100),
                PartitionData::default()
                    .with_partition_index(1)
                    .with_error_code(0)
                    .with_high_watermark(200),
                PartitionData::default()
                    .with_partition_index(2)
                    .with_error_code(3) // UNKNOWN_TOPIC_OR_PARTITION
                    .with_high_watermark(-1),
            ])]);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 12).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = FetchResponse::decode(&mut bytes, 12).unwrap();

    let partitions = &decoded.responses[0].partitions;
    assert_eq!(partitions.len(), 3);
    assert_eq!(partitions[0].high_watermark, 100);
    assert_eq!(partitions[1].high_watermark, 200);
    assert_eq!(partitions[2].error_code, 3);
}

// ============================================================================
// Phase 4: Version Compatibility Tests
// ============================================================================

#[test]
fn test_fetch_request_version_features() {
    use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};

    // Test that different versions have expected fields
    let base_request = FetchRequest::default()
        .with_replica_id(BrokerId(-1))
        .with_max_wait_ms(500)
        .with_min_bytes(1)
        .with_max_bytes(1024 * 1024)
        .with_topics(vec![FetchTopic::default()
            .with_topic(TopicName::from(StrBytes::from_static_str("test")))
            .with_partitions(vec![FetchPartition::default()
                .with_partition(0)
                .with_fetch_offset(0)
                .with_partition_max_bytes(1024 * 1024)])]);

    // Test encoding at different versions
    for version in [0, 4, 7, 11, 12] {
        let mut buf = BytesMut::new();
        base_request.encode(&mut buf, version).unwrap();

        let mut bytes = Bytes::from(buf);
        let decoded = FetchRequest::decode(&mut bytes, version).unwrap();

        assert_eq!(
            decoded.max_wait_ms, 500,
            "max_wait_ms should be preserved at v{}",
            version
        );
        assert_eq!(
            decoded.min_bytes, 1,
            "min_bytes should be preserved at v{}",
            version
        );
    }
}

#[test]
fn test_fetch_response_version_features() {
    use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};

    let base_response = FetchResponse::default()
        .with_throttle_time_ms(100)
        .with_responses(vec![FetchableTopicResponse::default()
            .with_topic(TopicName::from(StrBytes::from_static_str("test")))
            .with_partitions(vec![PartitionData::default()
                .with_partition_index(0)
                .with_error_code(0)
                .with_high_watermark(100)])]);

    // Test encoding at different versions
    for version in [0, 4, 7, 11, 12] {
        let mut buf = BytesMut::new();
        base_response.encode(&mut buf, version).unwrap();

        let mut bytes = Bytes::from(buf);
        let decoded = FetchResponse::decode(&mut bytes, version).unwrap();

        assert_eq!(
            decoded.responses[0].partitions[0].high_watermark, 100,
            "high_watermark should be preserved at v{}",
            version
        );
    }
}
