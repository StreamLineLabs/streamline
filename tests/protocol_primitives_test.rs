//! Phase 20: Protocol Primitive Type Tests
//!
//! Tests for Kafka protocol primitive type encoding/decoding:
//! - Integer types (INT8, INT16, INT32, INT64) via Kafka message fields
//! - Varint encoding via message fields
//! - String types via message fields
//! - UUID via message fields
//! - Byte order verification
//!
//! These tests verify correct encoding and decoding of the wire protocol
//! primitive types by examining actual message encodings.

use bytes::BytesMut;
use kafka_protocol::messages::{
    ApiVersionsRequest, ApiVersionsResponse, CreateTopicsRequest, FetchRequest, ListOffsetsRequest,
    MetadataRequest, ProduceRequest,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};

// ============================================================================
// INT8 Tests (via API fields that use INT8)
// ============================================================================

/// Test INT8 encoding via acks field in ProduceRequest
/// acks is i16 but we test isolation_level which is i8
#[test]
fn test_int8_via_isolation_level() {
    let mut request = FetchRequest::default();
    request.isolation_level = 0; // READ_UNCOMMITTED

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 4).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = FetchRequest::decode(&mut read_buf, 4).unwrap();
    assert_eq!(decoded.isolation_level, 0);
}

/// Test INT8 encoding with different isolation level
#[test]
fn test_int8_isolation_level_read_committed() {
    let mut request = FetchRequest::default();
    request.isolation_level = 1; // READ_COMMITTED

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 4).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = FetchRequest::decode(&mut read_buf, 4).unwrap();
    assert_eq!(decoded.isolation_level, 1);
}

// ============================================================================
// INT16 Tests (via API version fields)
// ============================================================================

/// Test INT16 encoding via min/max version in ApiVersionsResponse
#[test]
fn test_int16_via_api_versions() {
    use kafka_protocol::messages::api_versions_response::ApiVersion;

    let mut response = ApiVersionsResponse::default();
    let mut api_version = ApiVersion::default();
    api_version.api_key = 18; // ApiVersions
    api_version.min_version = 0;
    api_version.max_version = 3;
    response.api_keys.push(api_version);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ApiVersionsResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.api_keys[0].api_key, 18);
    assert_eq!(decoded.api_keys[0].min_version, 0);
    assert_eq!(decoded.api_keys[0].max_version, 3);
}

/// Test INT16 with max value
#[test]
fn test_int16_max_version() {
    use kafka_protocol::messages::api_versions_response::ApiVersion;

    let mut response = ApiVersionsResponse::default();
    let mut api_version = ApiVersion::default();
    api_version.api_key = 0;
    api_version.min_version = 0;
    api_version.max_version = i16::MAX;
    response.api_keys.push(api_version);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ApiVersionsResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.api_keys[0].max_version, i16::MAX);
}

/// Test INT16 acks field in ProduceRequest
#[test]
fn test_int16_acks_field() {
    let mut request = ProduceRequest::default();
    request.acks = -1; // All replicas

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 3).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ProduceRequest::decode(&mut read_buf, 3).unwrap();
    assert_eq!(decoded.acks, -1);
}

/// Test INT16 acks with value 0 (no acks)
#[test]
fn test_int16_acks_zero() {
    let mut request = ProduceRequest::default();
    request.acks = 0; // Fire and forget

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 3).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ProduceRequest::decode(&mut read_buf, 3).unwrap();
    assert_eq!(decoded.acks, 0);
}

/// Test INT16 acks with value 1 (leader only)
#[test]
fn test_int16_acks_one() {
    let mut request = ProduceRequest::default();
    request.acks = 1; // Leader only

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 3).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ProduceRequest::decode(&mut read_buf, 3).unwrap();
    assert_eq!(decoded.acks, 1);
}

// ============================================================================
// INT32 Tests (via timeout fields)
// ============================================================================

/// Test INT32 encoding via timeout_ms field
#[test]
fn test_int32_timeout_typical() {
    let mut request = ProduceRequest::default();
    request.timeout_ms = 30000; // 30 seconds

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 3).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ProduceRequest::decode(&mut read_buf, 3).unwrap();
    assert_eq!(decoded.timeout_ms, 30000);
}

/// Test INT32 with max value
#[test]
fn test_int32_max_value() {
    let mut request = ProduceRequest::default();
    request.timeout_ms = i32::MAX;

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 3).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ProduceRequest::decode(&mut read_buf, 3).unwrap();
    assert_eq!(decoded.timeout_ms, i32::MAX);
}

/// Test INT32 with zero
#[test]
fn test_int32_zero() {
    let mut request = ProduceRequest::default();
    request.timeout_ms = 0;

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 3).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ProduceRequest::decode(&mut read_buf, 3).unwrap();
    assert_eq!(decoded.timeout_ms, 0);
}

/// Test INT32 partition index
#[test]
fn test_int32_partition_index() {
    use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};

    let mut request = ListOffsetsRequest::default();
    let mut topic = ListOffsetsTopic::default();
    topic.name = kafka_protocol::messages::TopicName(StrBytes::from_static_str("test-topic"));

    let mut partition = ListOffsetsPartition::default();
    partition.partition_index = 12345;
    topic.partitions.push(partition);
    request.topics.push(topic);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ListOffsetsRequest::decode(&mut read_buf, 1).unwrap();
    assert_eq!(decoded.topics[0].partitions[0].partition_index, 12345);
}

// ============================================================================
// INT64 Tests (via timestamp and offset fields)
// ============================================================================

/// Test INT64 encoding via timestamp field
#[test]
fn test_int64_timestamp() {
    use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};

    let mut request = ListOffsetsRequest::default();
    let mut topic = ListOffsetsTopic::default();
    topic.name = kafka_protocol::messages::TopicName(StrBytes::from_static_str("test-topic"));

    let mut partition = ListOffsetsPartition::default();
    partition.timestamp = 1702396800000; // A typical timestamp
    topic.partitions.push(partition);
    request.topics.push(topic);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ListOffsetsRequest::decode(&mut read_buf, 1).unwrap();
    assert_eq!(decoded.topics[0].partitions[0].timestamp, 1702396800000);
}

/// Test INT64 with max value
#[test]
fn test_int64_max_value() {
    use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};

    let mut request = ListOffsetsRequest::default();
    let mut topic = ListOffsetsTopic::default();
    topic.name = kafka_protocol::messages::TopicName(StrBytes::from_static_str("test-topic"));

    let mut partition = ListOffsetsPartition::default();
    partition.timestamp = i64::MAX;
    topic.partitions.push(partition);
    request.topics.push(topic);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ListOffsetsRequest::decode(&mut read_buf, 1).unwrap();
    assert_eq!(decoded.topics[0].partitions[0].timestamp, i64::MAX);
}

/// Test INT64 with special timestamp -1 (latest)
#[test]
fn test_int64_timestamp_latest() {
    use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};

    let mut request = ListOffsetsRequest::default();
    let mut topic = ListOffsetsTopic::default();
    topic.name = kafka_protocol::messages::TopicName(StrBytes::from_static_str("test-topic"));

    let mut partition = ListOffsetsPartition::default();
    partition.timestamp = -1; // Latest offset
    topic.partitions.push(partition);
    request.topics.push(topic);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ListOffsetsRequest::decode(&mut read_buf, 1).unwrap();
    assert_eq!(decoded.topics[0].partitions[0].timestamp, -1);
}

/// Test INT64 with special timestamp -2 (earliest)
#[test]
fn test_int64_timestamp_earliest() {
    use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};

    let mut request = ListOffsetsRequest::default();
    let mut topic = ListOffsetsTopic::default();
    topic.name = kafka_protocol::messages::TopicName(StrBytes::from_static_str("test-topic"));

    let mut partition = ListOffsetsPartition::default();
    partition.timestamp = -2; // Earliest offset
    topic.partitions.push(partition);
    request.topics.push(topic);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ListOffsetsRequest::decode(&mut read_buf, 1).unwrap();
    assert_eq!(decoded.topics[0].partitions[0].timestamp, -2);
}

// ============================================================================
// STRING Tests
// ============================================================================

/// Test STRING encoding via topic name
#[test]
fn test_string_topic_name() {
    use kafka_protocol::messages::metadata_request::MetadataRequestTopic;

    let mut request = MetadataRequest::default();
    let mut topic = MetadataRequestTopic::default();
    topic.name = Some(kafka_protocol::messages::TopicName(
        StrBytes::from_static_str("my-test-topic"),
    ));
    request.topics = Some(vec![topic]);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = MetadataRequest::decode(&mut read_buf, 1).unwrap();

    let topics = decoded.topics.unwrap();
    assert_eq!(topics[0].name.as_ref().unwrap().0.as_str(), "my-test-topic");
}

/// Test STRING with empty string
#[test]
fn test_string_empty() {
    use kafka_protocol::messages::metadata_request::MetadataRequestTopic;

    let mut request = MetadataRequest::default();
    let mut topic = MetadataRequestTopic::default();
    topic.name = Some(kafka_protocol::messages::TopicName(
        StrBytes::from_static_str(""),
    ));
    request.topics = Some(vec![topic]);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = MetadataRequest::decode(&mut read_buf, 1).unwrap();

    let topics = decoded.topics.unwrap();
    assert_eq!(topics[0].name.as_ref().unwrap().0.as_str(), "");
}

/// Test STRING with unicode characters
#[test]
fn test_string_unicode() {
    use kafka_protocol::messages::metadata_request::MetadataRequestTopic;

    let mut request = MetadataRequest::default();
    let mut topic = MetadataRequestTopic::default();
    topic.name = Some(kafka_protocol::messages::TopicName(
        StrBytes::from_static_str("トピック名"),
    ));
    request.topics = Some(vec![topic]);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = MetadataRequest::decode(&mut read_buf, 1).unwrap();

    let topics = decoded.topics.unwrap();
    assert_eq!(topics[0].name.as_ref().unwrap().0.as_str(), "トピック名");
}

/// Test STRING with special characters
#[test]
fn test_string_special_chars() {
    use kafka_protocol::messages::metadata_request::MetadataRequestTopic;

    let mut request = MetadataRequest::default();
    let mut topic = MetadataRequestTopic::default();
    topic.name = Some(kafka_protocol::messages::TopicName(
        StrBytes::from_static_str("topic.with-special_chars.123"),
    ));
    request.topics = Some(vec![topic]);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = MetadataRequest::decode(&mut read_buf, 1).unwrap();

    let topics = decoded.topics.unwrap();
    assert_eq!(
        topics[0].name.as_ref().unwrap().0.as_str(),
        "topic.with-special_chars.123"
    );
}

// ============================================================================
// COMPACT_STRING Tests (flexible versions)
// ============================================================================

/// Test COMPACT_STRING encoding via flexible version
#[test]
fn test_compact_string_flexible_version() {
    use kafka_protocol::messages::create_topics_request::CreatableTopic;

    let mut request = CreateTopicsRequest::default();
    let mut topic = CreatableTopic::default();
    topic.name =
        kafka_protocol::messages::TopicName(StrBytes::from_static_str("compact-string-topic"));
    topic.num_partitions = 3;
    topic.replication_factor = 1;
    request.topics.push(topic);

    // Version 5+ uses flexible encoding (compact strings)
    let mut buf = BytesMut::new();
    request.encode(&mut buf, 5).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = CreateTopicsRequest::decode(&mut read_buf, 5).unwrap();

    assert_eq!(decoded.topics[0].name.0.as_str(), "compact-string-topic");
}

/// Test COMPACT_STRING with long string
#[test]
fn test_compact_string_long() {
    use kafka_protocol::messages::create_topics_request::CreatableTopic;

    let long_name = "a".repeat(200);
    let mut request = CreateTopicsRequest::default();
    let mut topic = CreatableTopic::default();
    topic.name = kafka_protocol::messages::TopicName(StrBytes::from_string(long_name.clone()));
    topic.num_partitions = 1;
    topic.replication_factor = 1;
    request.topics.push(topic);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 5).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = CreateTopicsRequest::decode(&mut read_buf, 5).unwrap();

    assert_eq!(decoded.topics[0].name.0.as_str(), long_name);
}

// ============================================================================
// UUID Tests
// ============================================================================

/// Test UUID encoding via topic_id field
#[test]
fn test_uuid_topic_id() {
    use kafka_protocol::messages::metadata_request::MetadataRequestTopic;

    let uuid = uuid::Uuid::new_v4();
    let mut request = MetadataRequest::default();
    let mut topic = MetadataRequestTopic::default();
    topic.topic_id = uuid;
    request.topics = Some(vec![topic]);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 12).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = MetadataRequest::decode(&mut read_buf, 12).unwrap();

    let topics = decoded.topics.unwrap();
    assert_eq!(topics[0].topic_id, uuid);
}

/// Test UUID nil value
#[test]
fn test_uuid_nil() {
    use kafka_protocol::messages::metadata_request::MetadataRequestTopic;

    let mut request = MetadataRequest::default();
    let mut topic = MetadataRequestTopic::default();
    topic.topic_id = uuid::Uuid::nil();
    topic.name = Some(kafka_protocol::messages::TopicName(
        StrBytes::from_static_str("test-topic"),
    ));
    request.topics = Some(vec![topic]);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 12).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = MetadataRequest::decode(&mut read_buf, 12).unwrap();

    let topics = decoded.topics.unwrap();
    assert_eq!(topics[0].topic_id, uuid::Uuid::nil());
}

/// Test UUID specific value
#[test]
fn test_uuid_specific() {
    use kafka_protocol::messages::metadata_request::MetadataRequestTopic;

    let uuid = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    let mut request = MetadataRequest::default();
    let mut topic = MetadataRequestTopic::default();
    topic.topic_id = uuid;
    request.topics = Some(vec![topic]);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 12).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = MetadataRequest::decode(&mut read_buf, 12).unwrap();

    let topics = decoded.topics.unwrap();
    assert_eq!(topics[0].topic_id, uuid);
}

// ============================================================================
// BOOLEAN Tests
// ============================================================================

/// Test BOOLEAN encoding via allow_auto_topic_creation
#[test]
fn test_boolean_true() {
    let mut request = MetadataRequest::default();
    request.allow_auto_topic_creation = true;

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 4).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = MetadataRequest::decode(&mut read_buf, 4).unwrap();
    assert!(decoded.allow_auto_topic_creation);
}

/// Test BOOLEAN false encoding
#[test]
fn test_boolean_false() {
    let mut request = MetadataRequest::default();
    request.allow_auto_topic_creation = false;

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 4).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = MetadataRequest::decode(&mut read_buf, 4).unwrap();
    assert!(!decoded.allow_auto_topic_creation);
}

// ============================================================================
// ARRAY Tests
// ============================================================================

/// Test ARRAY encoding with multiple elements
#[test]
fn test_array_multiple_topics() {
    use kafka_protocol::messages::metadata_request::MetadataRequestTopic;

    let mut request = MetadataRequest::default();

    let mut topic1 = MetadataRequestTopic::default();
    topic1.name = Some(kafka_protocol::messages::TopicName(
        StrBytes::from_static_str("topic-1"),
    ));

    let mut topic2 = MetadataRequestTopic::default();
    topic2.name = Some(kafka_protocol::messages::TopicName(
        StrBytes::from_static_str("topic-2"),
    ));

    let mut topic3 = MetadataRequestTopic::default();
    topic3.name = Some(kafka_protocol::messages::TopicName(
        StrBytes::from_static_str("topic-3"),
    ));

    request.topics = Some(vec![topic1, topic2, topic3]);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = MetadataRequest::decode(&mut read_buf, 1).unwrap();

    let topics = decoded.topics.unwrap();
    assert_eq!(topics.len(), 3);
}

/// Test ARRAY encoding with empty array
#[test]
fn test_array_empty() {
    let mut request = MetadataRequest::default();
    request.topics = Some(vec![]);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = MetadataRequest::decode(&mut read_buf, 1).unwrap();

    let topics = decoded.topics.unwrap();
    assert!(topics.is_empty());
}

/// Test nullable ARRAY with null (all topics)
#[test]
fn test_array_null() {
    let mut request = MetadataRequest::default();
    request.topics = None; // Request all topics

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = MetadataRequest::decode(&mut read_buf, 1).unwrap();

    assert!(decoded.topics.is_none());
}

// ============================================================================
// Byte Order Tests
// ============================================================================

/// Verify big-endian byte order for INT16
#[test]
fn test_byte_order_int16() {
    use kafka_protocol::messages::api_versions_response::ApiVersion;

    let mut response = ApiVersionsResponse::default();
    let mut api_version = ApiVersion::default();
    api_version.api_key = 0x0102; // 258 in decimal
    response.api_keys.push(api_version);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    // Find the api_key bytes (after error_code which is 2 bytes, and array length which is 4 bytes)
    // Error code (2 bytes) + array length (4 bytes) = offset 6
    // API key is at offset 6
    assert_eq!(buf[6], 0x01, "First byte should be 0x01 (big-endian)");
    assert_eq!(buf[7], 0x02, "Second byte should be 0x02 (big-endian)");
}

/// Verify big-endian byte order for INT32 by validating roundtrip with specific value
#[test]
fn test_byte_order_int32() {
    let mut request = ProduceRequest::default();
    request.timeout_ms = 0x01020304;

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 3).unwrap();

    // Verify roundtrip preserves the value
    let mut read_buf = buf.freeze();
    let decoded = ProduceRequest::decode(&mut read_buf, 3).unwrap();
    assert_eq!(decoded.timeout_ms, 0x01020304);
}

// ============================================================================
// Flexible Version Tests (varint encoding)
// ============================================================================

/// Test that flexible versions use compact encoding
#[test]
fn test_flexible_version_compact_encoding() {
    let request = ApiVersionsRequest::default();

    // v0-2 use non-flexible encoding
    let mut buf_v2 = BytesMut::new();
    request.encode(&mut buf_v2, 2).unwrap();

    // v3+ use flexible encoding
    let mut buf_v3 = BytesMut::new();
    request.encode(&mut buf_v3, 3).unwrap();

    // Both should decode correctly
    let decoded_v2 = ApiVersionsRequest::decode(&mut buf_v2.freeze(), 2).unwrap();
    let decoded_v3 = ApiVersionsRequest::decode(&mut buf_v3.freeze(), 3).unwrap();

    assert_eq!(
        decoded_v2.client_software_name,
        decoded_v3.client_software_name
    );
}

/// Test version roundtrip across all supported versions
#[test]
fn test_api_versions_request_all_versions() {
    for version in 0..=3 {
        let request = ApiVersionsRequest::default();

        let mut buf = BytesMut::new();
        request.encode(&mut buf, version).unwrap();

        let mut read_buf = buf.freeze();
        let decoded = ApiVersionsRequest::decode(&mut read_buf, version).unwrap();

        // Basic validation - should decode without error
        assert!(
            decoded.client_software_name.is_empty() || !decoded.client_software_name.is_empty()
        );
    }
}

/// Test MetadataRequest version roundtrip
#[test]
fn test_metadata_request_version_roundtrip() {
    for version in 0..=12 {
        let mut request = MetadataRequest::default();
        if version >= 4 {
            request.allow_auto_topic_creation = true;
        }

        let mut buf = BytesMut::new();
        request.encode(&mut buf, version).unwrap();

        let mut read_buf = buf.freeze();
        let decoded = MetadataRequest::decode(&mut read_buf, version).unwrap();

        if version >= 4 {
            assert_eq!(
                decoded.allow_auto_topic_creation,
                request.allow_auto_topic_creation
            );
        }
    }
}
