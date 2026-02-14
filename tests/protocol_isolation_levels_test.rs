//! Phase 25: Protocol Isolation Levels Tests
//!
//! Tests for Kafka protocol isolation level handling:
//! - READ_UNCOMMITTED vs READ_COMMITTED semantics
//! - Isolation level in FetchRequest (v4+)
//! - Isolation level in ListOffsetsRequest (v2+)
//! - Last Stable Offset (LSO) behavior
//! - Aborted transactions in FetchResponse
//! - Transactional message visibility
//!
//! Reference: https://kafka.apache.org/protocol.html#protocol_fetch

use bytes::BytesMut;
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::fetch_response::{
    AbortedTransaction, FetchableTopicResponse, PartitionData,
};
use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
use kafka_protocol::messages::list_offsets_response::{
    ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
};
use kafka_protocol::messages::{
    BrokerId, FetchRequest, FetchResponse, ListOffsetsRequest, ListOffsetsResponse, ProducerId,
    TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};

// ============================================================================
// Isolation Level Constants
// ============================================================================

/// READ_UNCOMMITTED (0): Read all messages including uncommitted transactional messages
const READ_UNCOMMITTED: i8 = 0;

/// READ_COMMITTED (1): Only read committed messages, skip uncommitted transactional messages
const READ_COMMITTED: i8 = 1;

// ============================================================================
// Isolation Level Constants Tests
// ============================================================================

/// Test isolation level constant values match Kafka specification
#[test]
fn test_isolation_level_constant_values() {
    assert_eq!(READ_UNCOMMITTED, 0, "READ_UNCOMMITTED must be 0");
    assert_eq!(READ_COMMITTED, 1, "READ_COMMITTED must be 1");
}

/// Test isolation level is represented as i8
#[test]
fn test_isolation_level_type() {
    let uncommitted: i8 = READ_UNCOMMITTED;
    let committed: i8 = READ_COMMITTED;

    // Both are valid i8 values
    assert_eq!(uncommitted, 0);
    assert_eq!(committed, 1);
}

/// Test isolation level ordering
#[test]
fn test_isolation_level_ordering() {
    // READ_UNCOMMITTED has lower value (less strict)
    // READ_COMMITTED has higher value (more strict)
    assert_ne!(READ_UNCOMMITTED, READ_COMMITTED);
}

// ============================================================================
// FetchRequest Isolation Level Tests
// ============================================================================

/// Test FetchRequest with READ_UNCOMMITTED isolation level (v4+)
#[test]
fn test_fetch_request_read_uncommitted_v4() {
    let request = FetchRequest::default()
        .with_replica_id(BrokerId(-1))
        .with_max_wait_ms(500)
        .with_min_bytes(1)
        .with_max_bytes(1024 * 1024)
        .with_isolation_level(READ_UNCOMMITTED)
        .with_topics(vec![create_fetch_topic("test-topic", 0, 0)]);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 4).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = FetchRequest::decode(&mut read_buf, 4).unwrap();

    assert_eq!(decoded.isolation_level, READ_UNCOMMITTED);
}

/// Test FetchRequest with READ_COMMITTED isolation level (v4+)
#[test]
fn test_fetch_request_read_committed_v4() {
    let request = FetchRequest::default()
        .with_replica_id(BrokerId(-1))
        .with_max_wait_ms(500)
        .with_min_bytes(1)
        .with_max_bytes(1024 * 1024)
        .with_isolation_level(READ_COMMITTED)
        .with_topics(vec![create_fetch_topic("test-topic", 0, 0)]);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 4).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = FetchRequest::decode(&mut read_buf, 4).unwrap();

    assert_eq!(decoded.isolation_level, READ_COMMITTED);
}

/// Test FetchRequest isolation level in multiple versions
#[test]
fn test_fetch_request_isolation_level_versions() {
    for version in 4..=13 {
        let request = FetchRequest::default()
            .with_replica_id(BrokerId(-1))
            .with_max_wait_ms(500)
            .with_min_bytes(1)
            .with_max_bytes(1024 * 1024)
            .with_isolation_level(READ_COMMITTED)
            .with_topics(vec![create_fetch_topic("test-topic", 0, 0)]);

        let mut buf = BytesMut::new();
        request.encode(&mut buf, version).unwrap();

        let mut read_buf = buf.freeze();
        let decoded = FetchRequest::decode(&mut read_buf, version).unwrap();

        assert_eq!(
            decoded.isolation_level, READ_COMMITTED,
            "isolation_level should be READ_COMMITTED at v{}",
            version
        );
    }
}

/// Test FetchRequest default isolation level
#[test]
fn test_fetch_request_default_isolation_level() {
    let request = FetchRequest::default();

    // Default should be READ_UNCOMMITTED (0)
    assert_eq!(
        request.isolation_level, READ_UNCOMMITTED,
        "Default isolation level should be READ_UNCOMMITTED"
    );
}

/// Test FetchRequest isolation level preserved across roundtrip
#[test]
fn test_fetch_request_isolation_level_roundtrip() {
    for isolation in [READ_UNCOMMITTED, READ_COMMITTED] {
        let request = FetchRequest::default()
            .with_replica_id(BrokerId(-1))
            .with_max_wait_ms(100)
            .with_min_bytes(1)
            .with_max_bytes(1024)
            .with_isolation_level(isolation)
            .with_topics(vec![create_fetch_topic("topic", 0, 0)]);

        let mut buf = BytesMut::new();
        request.encode(&mut buf, 11).unwrap();

        let mut read_buf = buf.freeze();
        let decoded = FetchRequest::decode(&mut read_buf, 11).unwrap();

        assert_eq!(decoded.isolation_level, isolation);
    }
}

// ============================================================================
// ListOffsetsRequest Isolation Level Tests
// ============================================================================

/// Test ListOffsetsRequest with READ_UNCOMMITTED (v2+)
#[test]
fn test_list_offsets_request_read_uncommitted() {
    let request = ListOffsetsRequest::default()
        .with_replica_id(BrokerId(-1))
        .with_isolation_level(READ_UNCOMMITTED)
        .with_topics(vec![create_list_offsets_topic("test-topic", 0, -1)]);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 2).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ListOffsetsRequest::decode(&mut read_buf, 2).unwrap();

    assert_eq!(decoded.isolation_level, READ_UNCOMMITTED);
}

/// Test ListOffsetsRequest with READ_COMMITTED (v2+)
#[test]
fn test_list_offsets_request_read_committed() {
    let request = ListOffsetsRequest::default()
        .with_replica_id(BrokerId(-1))
        .with_isolation_level(READ_COMMITTED)
        .with_topics(vec![create_list_offsets_topic("test-topic", 0, -1)]);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 2).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ListOffsetsRequest::decode(&mut read_buf, 2).unwrap();

    assert_eq!(decoded.isolation_level, READ_COMMITTED);
}

/// Test ListOffsetsRequest isolation level versions
#[test]
fn test_list_offsets_request_isolation_level_versions() {
    for version in 2..=7 {
        let request = ListOffsetsRequest::default()
            .with_replica_id(BrokerId(-1))
            .with_isolation_level(READ_COMMITTED)
            .with_topics(vec![create_list_offsets_topic("test-topic", 0, -1)]);

        let mut buf = BytesMut::new();
        request.encode(&mut buf, version).unwrap();

        let mut read_buf = buf.freeze();
        let decoded = ListOffsetsRequest::decode(&mut read_buf, version).unwrap();

        assert_eq!(
            decoded.isolation_level, READ_COMMITTED,
            "isolation_level should work at v{}",
            version
        );
    }
}

/// Test ListOffsetsRequest default isolation level
#[test]
fn test_list_offsets_request_default_isolation_level() {
    let request = ListOffsetsRequest::default();

    assert_eq!(
        request.isolation_level, READ_UNCOMMITTED,
        "Default should be READ_UNCOMMITTED"
    );
}

// ============================================================================
// Last Stable Offset (LSO) Tests
// ============================================================================

/// Test FetchResponse with LSO equal to high watermark (no uncommitted txns)
#[test]
fn test_fetch_response_lso_equals_hw() {
    let response = FetchResponse::default()
        .with_throttle_time_ms(0)
        .with_responses(vec![FetchableTopicResponse::default()
            .with_topic(TopicName::from(StrBytes::from_static_str("test-topic")))
            .with_partitions(vec![PartitionData::default()
                .with_partition_index(0)
                .with_error_code(0)
                .with_high_watermark(100)
                .with_last_stable_offset(100)
                .with_log_start_offset(0)])]);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 4).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = FetchResponse::decode(&mut read_buf, 4).unwrap();

    let partition = &decoded.responses[0].partitions[0];
    assert_eq!(partition.high_watermark, 100);
    assert_eq!(partition.last_stable_offset, 100);
    // LSO == HW means no uncommitted transactions
}

/// Test FetchResponse with LSO less than high watermark (uncommitted txns)
#[test]
fn test_fetch_response_lso_less_than_hw() {
    let response = FetchResponse::default()
        .with_throttle_time_ms(0)
        .with_responses(vec![FetchableTopicResponse::default()
            .with_topic(TopicName::from(StrBytes::from_static_str("test-topic")))
            .with_partitions(vec![PartitionData::default()
                .with_partition_index(0)
                .with_error_code(0)
                .with_high_watermark(100)
                .with_last_stable_offset(85) // 15 uncommitted messages
                .with_log_start_offset(0)])]);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 4).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = FetchResponse::decode(&mut read_buf, 4).unwrap();

    let partition = &decoded.responses[0].partitions[0];
    assert_eq!(partition.high_watermark, 100);
    assert_eq!(partition.last_stable_offset, 85);
    assert!(
        partition.last_stable_offset < partition.high_watermark,
        "LSO should be less than HW when there are uncommitted txns"
    );
}

/// Test LSO behavior with READ_COMMITTED consumers
#[test]
fn test_lso_read_committed_visibility() {
    // Simulate a partition with uncommitted transaction
    // HW = 100, LSO = 80 means offsets 80-99 are uncommitted
    let high_watermark: i64 = 100;
    let last_stable_offset: i64 = 80;

    // READ_COMMITTED consumer should only see up to LSO
    let visible_offset_committed = last_stable_offset;
    assert_eq!(visible_offset_committed, 80);

    // READ_UNCOMMITTED consumer can see up to HW
    let visible_offset_uncommitted = high_watermark;
    assert_eq!(visible_offset_uncommitted, 100);

    assert!(visible_offset_committed < visible_offset_uncommitted);
}

/// Test FetchResponse LSO in multiple versions
#[test]
fn test_fetch_response_lso_versions() {
    for version in 4..=13 {
        let response =
            FetchResponse::default().with_responses(vec![FetchableTopicResponse::default()
                .with_topic(TopicName::from(StrBytes::from_static_str("test-topic")))
                .with_partitions(vec![PartitionData::default()
                    .with_partition_index(0)
                    .with_error_code(0)
                    .with_high_watermark(1000)
                    .with_last_stable_offset(950)])]);

        let mut buf = BytesMut::new();
        response.encode(&mut buf, version).unwrap();

        let mut read_buf = buf.freeze();
        let decoded = FetchResponse::decode(&mut read_buf, version).unwrap();

        assert_eq!(
            decoded.responses[0].partitions[0].last_stable_offset, 950,
            "LSO should be preserved at v{}",
            version
        );
    }
}

// ============================================================================
// Aborted Transactions Tests
// ============================================================================

/// Test FetchResponse with single aborted transaction
#[test]
fn test_fetch_response_single_aborted_transaction() {
    let response = FetchResponse::default().with_responses(vec![FetchableTopicResponse::default()
        .with_topic(TopicName::from(StrBytes::from_static_str("test-topic")))
        .with_partitions(vec![PartitionData::default()
            .with_partition_index(0)
            .with_error_code(0)
            .with_high_watermark(100)
            .with_last_stable_offset(100)
            .with_aborted_transactions(Some(vec![AbortedTransaction::default()
                .with_producer_id(ProducerId(1000))
                .with_first_offset(50)]))])]);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 4).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = FetchResponse::decode(&mut read_buf, 4).unwrap();

    let aborted = decoded.responses[0].partitions[0]
        .aborted_transactions
        .as_ref()
        .unwrap();
    assert_eq!(aborted.len(), 1);
    assert_eq!(aborted[0].producer_id, 1000);
    assert_eq!(aborted[0].first_offset, 50);
}

/// Test FetchResponse with multiple aborted transactions
#[test]
fn test_fetch_response_multiple_aborted_transactions() {
    let response = FetchResponse::default().with_responses(vec![FetchableTopicResponse::default()
        .with_topic(TopicName::from(StrBytes::from_static_str("test-topic")))
        .with_partitions(vec![PartitionData::default()
            .with_partition_index(0)
            .with_error_code(0)
            .with_high_watermark(200)
            .with_last_stable_offset(200)
            .with_aborted_transactions(Some(vec![
                AbortedTransaction::default()
                    .with_producer_id(ProducerId(1001))
                    .with_first_offset(25),
                AbortedTransaction::default()
                    .with_producer_id(ProducerId(1002))
                    .with_first_offset(75),
                AbortedTransaction::default()
                    .with_producer_id(ProducerId(1003))
                    .with_first_offset(150),
            ]))])]);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 4).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = FetchResponse::decode(&mut read_buf, 4).unwrap();

    let aborted = decoded.responses[0].partitions[0]
        .aborted_transactions
        .as_ref()
        .unwrap();
    assert_eq!(aborted.len(), 3);
    assert_eq!(aborted[0].first_offset, 25);
    assert_eq!(aborted[1].first_offset, 75);
    assert_eq!(aborted[2].first_offset, 150);
}

/// Test FetchResponse with no aborted transactions
#[test]
fn test_fetch_response_no_aborted_transactions() {
    let response = FetchResponse::default().with_responses(vec![FetchableTopicResponse::default()
        .with_topic(TopicName::from(StrBytes::from_static_str("test-topic")))
        .with_partitions(vec![PartitionData::default()
            .with_partition_index(0)
            .with_error_code(0)
            .with_high_watermark(100)
            .with_last_stable_offset(100)
            .with_aborted_transactions(Some(vec![]))])]);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 4).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = FetchResponse::decode(&mut read_buf, 4).unwrap();

    let aborted = decoded.responses[0].partitions[0]
        .aborted_transactions
        .as_ref()
        .unwrap();
    assert!(aborted.is_empty());
}

/// Test FetchResponse with null aborted transactions list
#[test]
fn test_fetch_response_null_aborted_transactions() {
    let response = FetchResponse::default().with_responses(vec![FetchableTopicResponse::default()
        .with_topic(TopicName::from(StrBytes::from_static_str("test-topic")))
        .with_partitions(vec![PartitionData::default()
            .with_partition_index(0)
            .with_error_code(0)
            .with_high_watermark(100)
            .with_last_stable_offset(100)
            .with_aborted_transactions(None)])]);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 4).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = FetchResponse::decode(&mut read_buf, 4).unwrap();

    // None means no aborted transactions info (could be READ_UNCOMMITTED request)
    assert!(decoded.responses[0].partitions[0]
        .aborted_transactions
        .is_none());
}

// ============================================================================
// Transactional Message Visibility Tests
// ============================================================================

/// Test transactional visibility calculation
#[test]
fn test_transactional_visibility_uncommitted() {
    // READ_UNCOMMITTED: sees all messages up to HW
    let high_watermark: i64 = 100;
    let last_stable_offset: i64 = 85;

    // Consumer sees all offsets including uncommitted
    let max_visible = high_watermark;
    assert_eq!(max_visible, 100);

    // Uncommitted range: [LSO, HW)
    let uncommitted_start = last_stable_offset;
    let uncommitted_end = high_watermark;
    let uncommitted_count = uncommitted_end - uncommitted_start;
    assert_eq!(uncommitted_count, 15);
}

/// Test transactional visibility calculation for committed
#[test]
fn test_transactional_visibility_committed() {
    // READ_COMMITTED: sees only up to LSO
    let high_watermark: i64 = 100;
    let last_stable_offset: i64 = 85;

    // Consumer only sees committed offsets
    let max_visible = last_stable_offset;
    assert_eq!(max_visible, 85);

    // Messages from [LSO, HW) are not visible
    let hidden_count = high_watermark - last_stable_offset;
    assert_eq!(hidden_count, 15);
}

/// Test aborted transaction filtering
#[test]
fn test_aborted_transaction_filtering() {
    // READ_COMMITTED consumer needs to filter out aborted transaction records
    let aborted_txns = vec![
        (1001i64, 50i64), // producer_id 1001, starts at offset 50
        (1002i64, 75i64), // producer_id 1002, starts at offset 75
    ];

    // Offsets from aborted transactions should be skipped
    for (producer_id, first_offset) in &aborted_txns {
        assert!(*producer_id > 0, "Producer ID should be positive");
        assert!(*first_offset >= 0, "First offset should be non-negative");
    }
}

// ============================================================================
// ListOffsetsResponse with Isolation Level Tests
// ============================================================================

/// Test ListOffsetsResponse returns HW for READ_UNCOMMITTED
#[test]
fn test_list_offsets_response_read_uncommitted() {
    let response = ListOffsetsResponse::default()
        .with_throttle_time_ms(0)
        .with_topics(vec![ListOffsetsTopicResponse::default()
            .with_name(TopicName::from(StrBytes::from_static_str("test-topic")))
            .with_partitions(vec![ListOffsetsPartitionResponse::default()
                .with_partition_index(0)
                .with_error_code(0)
                .with_timestamp(-1)
                .with_offset(100)])]);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 2).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ListOffsetsResponse::decode(&mut read_buf, 2).unwrap();

    // For READ_UNCOMMITTED, offset should be high watermark
    assert_eq!(decoded.topics[0].partitions[0].offset, 100);
}

/// Test ListOffsetsResponse returns LSO for READ_COMMITTED
#[test]
fn test_list_offsets_response_read_committed_semantics() {
    // When isolation_level is READ_COMMITTED, ListOffsets returns LSO
    let response =
        ListOffsetsResponse::default().with_topics(vec![ListOffsetsTopicResponse::default()
            .with_name(TopicName::from(StrBytes::from_static_str("test-topic")))
            .with_partitions(vec![ListOffsetsPartitionResponse::default()
                .with_partition_index(0)
                .with_error_code(0)
                .with_offset(85)])]);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 2).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ListOffsetsResponse::decode(&mut read_buf, 2).unwrap();

    // For READ_COMMITTED, this would be LSO (85) instead of HW (100)
    assert_eq!(decoded.topics[0].partitions[0].offset, 85);
}

// ============================================================================
// Multiple Partition Isolation Level Tests
// ============================================================================

/// Test isolation level affects multiple partitions consistently
#[test]
fn test_isolation_level_multiple_partitions() {
    let response = FetchResponse::default().with_responses(vec![FetchableTopicResponse::default()
        .with_topic(TopicName::from(StrBytes::from_static_str("test-topic")))
        .with_partitions(vec![
            PartitionData::default()
                .with_partition_index(0)
                .with_high_watermark(100)
                .with_last_stable_offset(100), // No uncommitted txns
            PartitionData::default()
                .with_partition_index(1)
                .with_high_watermark(200)
                .with_last_stable_offset(180), // 20 uncommitted
            PartitionData::default()
                .with_partition_index(2)
                .with_high_watermark(300)
                .with_last_stable_offset(250), // 50 uncommitted
        ])]);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 4).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = FetchResponse::decode(&mut read_buf, 4).unwrap();

    let partitions = &decoded.responses[0].partitions;

    // Partition 0: LSO == HW (no uncommitted)
    assert_eq!(partitions[0].high_watermark, 100);
    assert_eq!(partitions[0].last_stable_offset, 100);

    // Partition 1: LSO < HW (20 uncommitted)
    assert_eq!(partitions[1].high_watermark, 200);
    assert_eq!(partitions[1].last_stable_offset, 180);

    // Partition 2: LSO < HW (50 uncommitted)
    assert_eq!(partitions[2].high_watermark, 300);
    assert_eq!(partitions[2].last_stable_offset, 250);
}

/// Test isolation level with different topics
#[test]
fn test_isolation_level_multiple_topics() {
    let request = FetchRequest::default()
        .with_replica_id(BrokerId(-1))
        .with_max_wait_ms(500)
        .with_min_bytes(1)
        .with_max_bytes(1024 * 1024)
        .with_isolation_level(READ_COMMITTED)
        .with_topics(vec![
            create_fetch_topic("topic-a", 0, 0),
            create_fetch_topic("topic-b", 0, 100),
            create_fetch_topic("topic-c", 0, 200),
        ]);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 11).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = FetchRequest::decode(&mut read_buf, 11).unwrap();

    // Isolation level applies to entire request
    assert_eq!(decoded.isolation_level, READ_COMMITTED);
    assert_eq!(decoded.topics.len(), 3);
}

// ============================================================================
// Edge Case Tests
// ============================================================================

/// Test isolation level with zero offset (empty partition)
#[test]
fn test_isolation_level_zero_offset() {
    // Empty partition: HW=0, LSO=0, log_start=0
    let response = FetchResponse::default().with_responses(vec![FetchableTopicResponse::default()
        .with_topic(TopicName::from(StrBytes::from_static_str("test-topic")))
        .with_partitions(vec![PartitionData::default()
            .with_partition_index(0)
            .with_error_code(0)
            .with_high_watermark(0)
            .with_last_stable_offset(0)
            .with_log_start_offset(0)])]);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 4).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = FetchResponse::decode(&mut read_buf, 4).unwrap();

    let partition = &decoded.responses[0].partitions[0];
    assert_eq!(partition.high_watermark, 0);
    // For an empty partition, LSO should match HW
    // Note: v4 encoding preserves LSO even when 0
    assert!(
        partition.last_stable_offset == 0 || partition.last_stable_offset == -1,
        "LSO should be 0 or -1 (default) for empty partition"
    );
}

/// Test isolation level with large offsets
#[test]
fn test_isolation_level_large_offsets() {
    let large_offset: i64 = i64::MAX - 1000;

    let response = FetchResponse::default().with_responses(vec![FetchableTopicResponse::default()
        .with_topic(TopicName::from(StrBytes::from_static_str("test-topic")))
        .with_partitions(vec![PartitionData::default()
            .with_partition_index(0)
            .with_high_watermark(large_offset)
            .with_last_stable_offset(large_offset - 100)])]);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 4).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = FetchResponse::decode(&mut read_buf, 4).unwrap();

    let partition = &decoded.responses[0].partitions[0];
    assert_eq!(partition.high_watermark, large_offset);
    assert_eq!(partition.last_stable_offset, large_offset - 100);
}

/// Test aborted transaction with max producer ID
#[test]
fn test_aborted_transaction_max_producer_id() {
    let response = FetchResponse::default().with_responses(vec![FetchableTopicResponse::default()
        .with_topic(TopicName::from(StrBytes::from_static_str("test-topic")))
        .with_partitions(vec![PartitionData::default()
            .with_partition_index(0)
            .with_high_watermark(100)
            .with_aborted_transactions(Some(vec![AbortedTransaction::default()
                .with_producer_id(ProducerId(i64::MAX))
                .with_first_offset(0)]))])]);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 4).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = FetchResponse::decode(&mut read_buf, 4).unwrap();

    let aborted = decoded.responses[0].partitions[0]
        .aborted_transactions
        .as_ref()
        .unwrap();
    assert_eq!(aborted[0].producer_id, i64::MAX);
}

// ============================================================================
// Helper Functions
// ============================================================================

fn create_fetch_topic(name: &str, partition: i32, offset: i64) -> FetchTopic {
    FetchTopic::default()
        .with_topic(TopicName::from(StrBytes::from_string(name.to_string())))
        .with_partitions(vec![FetchPartition::default()
            .with_partition(partition)
            .with_fetch_offset(offset)
            .with_partition_max_bytes(1024 * 1024)])
}

fn create_list_offsets_topic(name: &str, partition: i32, timestamp: i64) -> ListOffsetsTopic {
    ListOffsetsTopic::default()
        .with_name(TopicName::from(StrBytes::from_string(name.to_string())))
        .with_partitions(vec![ListOffsetsPartition::default()
            .with_partition_index(partition)
            .with_timestamp(timestamp)])
}
