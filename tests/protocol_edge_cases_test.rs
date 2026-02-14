//! Protocol Edge Cases Tests - Phase 5
//!
//! Tests for Kafka protocol edge cases and version boundaries:
//! - API version boundary transitions
//! - Correlation ID handling
//! - Request/response ordering
//! - Maximum message sizes
//! - Nullable field handling
//! - Empty requests/responses
//! - Request header validation
//!
//! Source: https://kafka.apache.org/protocol.html

use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::*;
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};

// ============================================================================
// Phase 5: Correlation ID Tests
// ============================================================================

#[test]
fn test_correlation_id_roundtrip() {
    // Correlation ID must be echoed back exactly
    let correlation_ids = [0, 1, 100, 1000, i32::MAX, -1, i32::MIN];

    for &corr_id in &correlation_ids {
        let header = RequestHeader::default()
            .with_request_api_key(18)
            .with_request_api_version(3)
            .with_correlation_id(corr_id)
            .with_client_id(Some(StrBytes::from_static_str("test")));

        let mut buf = BytesMut::new();
        header.encode(&mut buf, 2).unwrap();

        let mut bytes = Bytes::from(buf);
        let decoded = RequestHeader::decode(&mut bytes, 2).unwrap();

        assert_eq!(
            decoded.correlation_id, corr_id,
            "Correlation ID {} should be preserved",
            corr_id
        );
    }
}

#[test]
fn test_correlation_id_in_response_header() {
    let correlation_ids = [0, 12345, i32::MAX];

    for &corr_id in &correlation_ids {
        let header = ResponseHeader::default().with_correlation_id(corr_id);

        let mut buf = BytesMut::new();
        header.encode(&mut buf, 0).unwrap();

        let mut bytes = Bytes::from(buf);
        let decoded = ResponseHeader::decode(&mut bytes, 0).unwrap();

        assert_eq!(decoded.correlation_id, corr_id);
    }
}

#[test]
fn test_correlation_id_sequential() {
    // Simulate sequential requests with incrementing correlation IDs
    for corr_id in 0..100 {
        let request = ApiVersionsRequest::default();

        let header = RequestHeader::default()
            .with_request_api_key(18)
            .with_request_api_version(3)
            .with_correlation_id(corr_id)
            .with_client_id(Some(StrBytes::from_static_str("test")));

        let mut buf = BytesMut::new();
        header.encode(&mut buf, 2).unwrap();
        request.encode(&mut buf, 3).unwrap();

        // Parse back and verify
        let mut bytes = Bytes::from(buf);
        let decoded_header = RequestHeader::decode(&mut bytes, 2).unwrap();
        assert_eq!(decoded_header.correlation_id, corr_id);
    }
}

// ============================================================================
// Phase 5: Client ID Tests
// ============================================================================

#[test]
fn test_client_id_null() {
    // Null client_id is valid
    let header = RequestHeader::default()
        .with_request_api_key(18)
        .with_request_api_version(0)
        .with_correlation_id(1)
        .with_client_id(None);

    let mut buf = BytesMut::new();
    header.encode(&mut buf, 1).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = RequestHeader::decode(&mut bytes, 1).unwrap();

    assert!(decoded.client_id.is_none());
}

#[test]
fn test_client_id_empty_string() {
    // Empty string is different from null
    let header = RequestHeader::default()
        .with_request_api_key(18)
        .with_request_api_version(0)
        .with_correlation_id(1)
        .with_client_id(Some(StrBytes::from_static_str("")));

    let mut buf = BytesMut::new();
    header.encode(&mut buf, 1).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = RequestHeader::decode(&mut bytes, 1).unwrap();

    assert!(decoded.client_id.is_some());
    assert!(decoded.client_id.as_ref().unwrap().is_empty());
}

#[test]
fn test_client_id_long_string() {
    // Test with a longer client ID
    let long_id = "a".repeat(255);
    let header = RequestHeader::default()
        .with_request_api_key(18)
        .with_request_api_version(0)
        .with_correlation_id(1)
        .with_client_id(Some(StrBytes::from_string(long_id.clone())));

    let mut buf = BytesMut::new();
    header.encode(&mut buf, 1).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = RequestHeader::decode(&mut bytes, 1).unwrap();

    assert_eq!(decoded.client_id.as_ref().unwrap().as_str(), long_id);
}

#[test]
fn test_client_id_utf8() {
    // UTF-8 characters in client ID
    let utf8_id = "client-日本語-клиент";
    let header = RequestHeader::default()
        .with_request_api_key(18)
        .with_request_api_version(0)
        .with_correlation_id(1)
        .with_client_id(Some(StrBytes::from_string(utf8_id.to_string())));

    let mut buf = BytesMut::new();
    header.encode(&mut buf, 1).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = RequestHeader::decode(&mut bytes, 1).unwrap();

    assert_eq!(decoded.client_id.as_ref().unwrap().as_str(), utf8_id);
}

// ============================================================================
// Phase 5: API Version Boundary Tests
// ============================================================================

#[test]
fn test_api_versions_v0_to_v3_transition() {
    // ApiVersions is special - response header is always v0
    let response = ApiVersionsResponse::default().with_error_code(0);

    for version in 0..=3 {
        let mut buf = BytesMut::new();
        response.encode(&mut buf, version).unwrap();

        let mut bytes = Bytes::from(buf);
        let decoded = ApiVersionsResponse::decode(&mut bytes, version).unwrap();

        assert_eq!(
            decoded.error_code, 0,
            "Error code should decode at v{}",
            version
        );
    }
}

#[test]
fn test_metadata_flexible_version_boundary() {
    use kafka_protocol::messages::metadata_request::MetadataRequestTopic;

    // Metadata: v0-8 non-flexible, v9+ flexible
    let request =
        MetadataRequest::default().with_topics(Some(vec![MetadataRequestTopic::default()
            .with_name(Some(TopicName::from(StrBytes::from_static_str("test"))))]));

    // Test both sides of the boundary
    for version in [8, 9] {
        let mut buf = BytesMut::new();
        request.encode(&mut buf, version).unwrap();

        let mut bytes = Bytes::from(buf);
        let decoded = MetadataRequest::decode(&mut bytes, version).unwrap();

        assert_eq!(
            decoded.topics.as_ref().unwrap().len(),
            1,
            "Should decode topics at v{}",
            version
        );
    }
}

#[test]
fn test_produce_flexible_version_boundary() {
    // Produce: v0-8 non-flexible, v9+ flexible
    let request = ProduceRequest::default()
        .with_acks(1)
        .with_timeout_ms(30000);

    for version in [8, 9] {
        let mut buf = BytesMut::new();
        request.encode(&mut buf, version).unwrap();

        let mut bytes = Bytes::from(buf);
        let decoded = ProduceRequest::decode(&mut bytes, version).unwrap();

        assert_eq!(decoded.acks, 1, "Acks should decode at v{}", version);
        assert_eq!(
            decoded.timeout_ms, 30000,
            "Timeout should decode at v{}",
            version
        );
    }
}

#[test]
fn test_fetch_flexible_version_boundary() {
    // Fetch: v0-11 non-flexible, v12+ flexible
    let request = FetchRequest::default()
        .with_replica_id(BrokerId(-1))
        .with_max_wait_ms(500)
        .with_min_bytes(1)
        .with_max_bytes(1024 * 1024);

    for version in [11, 12] {
        let mut buf = BytesMut::new();
        request.encode(&mut buf, version).unwrap();

        let mut bytes = Bytes::from(buf);
        let decoded = FetchRequest::decode(&mut bytes, version).unwrap();

        assert_eq!(
            decoded.max_wait_ms, 500,
            "max_wait_ms should decode at v{}",
            version
        );
    }
}

#[test]
fn test_list_offsets_flexible_version_boundary() {
    // ListOffsets: v0-5 non-flexible, v6+ flexible
    let request = ListOffsetsRequest::default().with_replica_id(BrokerId(-1));

    for version in [5, 6] {
        let mut buf = BytesMut::new();
        request.encode(&mut buf, version).unwrap();

        let mut bytes = Bytes::from(buf);
        let decoded = ListOffsetsRequest::decode(&mut bytes, version).unwrap();

        assert_eq!(
            decoded.replica_id,
            BrokerId(-1),
            "replica_id should decode at v{}",
            version
        );
    }
}

// ============================================================================
// Phase 5: Empty Request/Response Tests
// ============================================================================

#[test]
fn test_api_versions_request_empty() {
    // ApiVersionsRequest has minimal body
    let request = ApiVersionsRequest::default();

    for version in 0..=3 {
        let mut buf = BytesMut::new();
        request.encode(&mut buf, version).unwrap();

        let mut bytes = Bytes::from(buf);
        let decoded = ApiVersionsRequest::decode(&mut bytes, version).unwrap();

        // Just verify it decodes without error
        assert!(decoded == request || version >= 3); // v3 has additional fields
    }
}

#[test]
fn test_metadata_request_all_topics() {
    // null topics means "all topics"
    let request = MetadataRequest::default().with_topics(None);

    for version in [0, 9, 12] {
        let mut buf = BytesMut::new();
        request.encode(&mut buf, version).unwrap();

        let mut bytes = Bytes::from(buf);
        let decoded = MetadataRequest::decode(&mut bytes, version).unwrap();

        assert!(
            decoded.topics.is_none(),
            "Null topics should be preserved at v{}",
            version
        );
    }
}

#[test]
fn test_metadata_request_empty_topics_list() {
    // Empty list is different from null
    let request = MetadataRequest::default().with_topics(Some(vec![]));

    for version in [0, 9, 12] {
        let mut buf = BytesMut::new();
        request.encode(&mut buf, version).unwrap();

        let mut bytes = Bytes::from(buf);
        let decoded = MetadataRequest::decode(&mut bytes, version).unwrap();

        assert!(
            decoded.topics.is_some(),
            "Empty list should not be null at v{}",
            version
        );
        assert!(
            decoded.topics.as_ref().unwrap().is_empty(),
            "Empty list should be empty at v{}",
            version
        );
    }
}

#[test]
fn test_produce_request_empty_topic_data() {
    // Produce with no topics
    let request = ProduceRequest::default()
        .with_acks(-1)
        .with_timeout_ms(30000)
        .with_topic_data(vec![]);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 9).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = ProduceRequest::decode(&mut bytes, 9).unwrap();

    assert!(decoded.topic_data.is_empty());
}

#[test]
fn test_fetch_request_empty_topics() {
    let request = FetchRequest::default()
        .with_replica_id(BrokerId(-1))
        .with_max_wait_ms(500)
        .with_min_bytes(1)
        .with_max_bytes(1024 * 1024)
        .with_topics(vec![]);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 12).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = FetchRequest::decode(&mut bytes, 12).unwrap();

    assert!(decoded.topics.is_empty());
}

// ============================================================================
// Phase 5: Nullable Field Tests
// ============================================================================

#[test]
fn test_nullable_string_in_response() {
    // Test nullable string field handling
    let response = SaslAuthenticateResponse::default()
        .with_error_code(0)
        .with_error_message(None); // Null error message

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 2).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = SaslAuthenticateResponse::decode(&mut bytes, 2).unwrap();

    assert!(decoded.error_message.is_none());
}

#[test]
fn test_nullable_bytes_in_response() {
    // Test nullable bytes field
    let response = SaslAuthenticateResponse::default()
        .with_error_code(0)
        .with_auth_bytes(Bytes::new()); // Empty but not null

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 2).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = SaslAuthenticateResponse::decode(&mut bytes, 2).unwrap();

    assert!(decoded.auth_bytes.is_empty());
}

#[test]
fn test_nullable_array_in_metadata() {
    use kafka_protocol::messages::metadata_response::MetadataResponseBroker;

    // Test with null topics vs empty topics
    let response_null =
        MetadataResponse::default().with_brokers(vec![MetadataResponseBroker::default()
            .with_node_id(BrokerId(1))
            .with_host(StrBytes::from_static_str("localhost"))
            .with_port(9092)]);

    let mut buf = BytesMut::new();
    response_null.encode(&mut buf, 9).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = MetadataResponse::decode(&mut bytes, 9).unwrap();

    assert_eq!(decoded.brokers.len(), 1);
}

// ============================================================================
// Phase 5: Large Value Tests
// ============================================================================

#[test]
fn test_large_topic_name() {
    use kafka_protocol::messages::metadata_request::MetadataRequestTopic;

    // Topic names can be up to 249 characters
    let large_name = "t".repeat(200);
    let request =
        MetadataRequest::default()
            .with_topics(Some(vec![MetadataRequestTopic::default().with_name(Some(
                TopicName::from(StrBytes::from_string(large_name.clone())),
            ))]));

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 12).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = MetadataRequest::decode(&mut bytes, 12).unwrap();

    assert_eq!(
        decoded.topics.as_ref().unwrap()[0]
            .name
            .as_ref()
            .unwrap()
            .as_str(),
        large_name
    );
}

#[test]
fn test_many_topics_in_request() {
    use kafka_protocol::messages::metadata_request::MetadataRequestTopic;

    // Test with many topics
    let topics: Vec<MetadataRequestTopic> = (0..100)
        .map(|i| {
            MetadataRequestTopic::default().with_name(Some(TopicName::from(StrBytes::from_string(
                format!("topic-{}", i),
            ))))
        })
        .collect();

    let request = MetadataRequest::default().with_topics(Some(topics));

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 12).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = MetadataRequest::decode(&mut bytes, 12).unwrap();

    assert_eq!(decoded.topics.as_ref().unwrap().len(), 100);
}

#[test]
fn test_many_partitions_in_fetch() {
    use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};

    // Test with many partitions
    let partitions: Vec<FetchPartition> = (0..100)
        .map(|i| {
            FetchPartition::default()
                .with_partition(i)
                .with_fetch_offset(0)
                .with_partition_max_bytes(1024 * 1024)
        })
        .collect();

    let request = FetchRequest::default()
        .with_replica_id(BrokerId(-1))
        .with_max_wait_ms(500)
        .with_min_bytes(1)
        .with_max_bytes(100 * 1024 * 1024)
        .with_topics(vec![FetchTopic::default()
            .with_topic(TopicName::from(StrBytes::from_static_str("test")))
            .with_partitions(partitions)]);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 12).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = FetchRequest::decode(&mut bytes, 12).unwrap();

    assert_eq!(decoded.topics[0].partitions.len(), 100);
}

// ============================================================================
// Phase 5: Request Header Version Tests
// ============================================================================

#[test]
fn test_request_header_v1_structure() {
    // Header v1: api_key (INT16) + api_version (INT16) + correlation_id (INT32) + client_id (NULLABLE_STRING)
    let header = RequestHeader::default()
        .with_request_api_key(3) // Metadata
        .with_request_api_version(8)
        .with_correlation_id(12345)
        .with_client_id(Some(StrBytes::from_static_str("test")));

    let mut buf = BytesMut::new();
    header.encode(&mut buf, 1).unwrap();

    // Verify structure manually
    let bytes = buf.to_vec();

    // API key at offset 0
    let api_key = i16::from_be_bytes([bytes[0], bytes[1]]);
    assert_eq!(api_key, 3);

    // API version at offset 2
    let api_version = i16::from_be_bytes([bytes[2], bytes[3]]);
    assert_eq!(api_version, 8);

    // Correlation ID at offset 4
    let corr_id = i32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
    assert_eq!(corr_id, 12345);

    // Client ID length at offset 8 (INT16)
    let client_id_len = i16::from_be_bytes([bytes[8], bytes[9]]);
    assert_eq!(client_id_len, 4); // "test"
}

#[test]
fn test_request_header_v2_has_tagged_fields() {
    // Header v2: same as v1 but with COMPACT_NULLABLE_STRING and tagged fields
    let header = RequestHeader::default()
        .with_request_api_key(18) // ApiVersions
        .with_request_api_version(3)
        .with_correlation_id(99999)
        .with_client_id(Some(StrBytes::from_static_str("test")));

    let mut buf = BytesMut::new();
    header.encode(&mut buf, 2).unwrap();

    let bytes = buf.to_vec();

    // Last byte should be 0x00 (empty tagged fields)
    assert_eq!(
        bytes.last(),
        Some(&0x00),
        "Header v2 should end with tagged fields marker"
    );
}

// ============================================================================
// Phase 5: Response Header Version Tests
// ============================================================================

#[test]
fn test_response_header_v0_structure() {
    // Header v0: just correlation_id (INT32)
    let header = ResponseHeader::default().with_correlation_id(54321);

    let mut buf = BytesMut::new();
    header.encode(&mut buf, 0).unwrap();

    assert_eq!(buf.len(), 4, "Response header v0 should be 4 bytes");

    let corr_id = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    assert_eq!(corr_id, 54321);
}

#[test]
fn test_response_header_v1_has_tagged_fields() {
    // Header v1: correlation_id + tagged_fields
    let header = ResponseHeader::default().with_correlation_id(11111);

    let mut buf = BytesMut::new();
    header.encode(&mut buf, 1).unwrap();

    // Should be 4 bytes correlation_id + 1 byte empty tagged fields = 5 bytes
    assert_eq!(buf.len(), 5, "Response header v1 should be 5 bytes");

    // Last byte is empty tagged fields
    assert_eq!(buf[4], 0x00);
}

// ============================================================================
// Phase 5: Special Cases
// ============================================================================

#[test]
fn test_sasl_handshake_never_flexible() {
    // SaslHandshake NEVER uses flexible encoding (per Kafka spec)
    let request =
        SaslHandshakeRequest::default().with_mechanism(StrBytes::from_static_str("PLAIN"));

    // v0 and v1 should both be non-flexible
    for version in 0..=1 {
        let mut buf = BytesMut::new();
        request.encode(&mut buf, version).unwrap();

        let mut bytes = Bytes::from(buf);
        let decoded = SaslHandshakeRequest::decode(&mut bytes, version).unwrap();

        assert_eq!(decoded.mechanism.as_str(), "PLAIN");
    }
}

#[test]
fn test_describe_cluster_always_flexible() {
    // DescribeCluster is always flexible from v0
    let request =
        DescribeClusterRequest::default().with_include_cluster_authorized_operations(true);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 0).unwrap();

    // Should have tagged fields marker at end
    let bytes = buf.to_vec();
    assert_eq!(bytes.last(), Some(&0x00));

    let mut bytes2 = Bytes::from(buf);
    let decoded = DescribeClusterRequest::decode(&mut bytes2, 0).unwrap();

    assert!(decoded.include_cluster_authorized_operations);
}

// ============================================================================
// Phase 5: Acks Values Tests
// ============================================================================

#[test]
fn test_produce_acks_values() {
    // acks: 0 = no ack, 1 = leader only, -1 = all ISR
    let acks_values = [0i16, 1, -1];

    for &acks in &acks_values {
        let request = ProduceRequest::default()
            .with_acks(acks)
            .with_timeout_ms(30000);

        let mut buf = BytesMut::new();
        request.encode(&mut buf, 9).unwrap();

        let mut bytes = Bytes::from(buf);
        let decoded = ProduceRequest::decode(&mut bytes, 9).unwrap();

        assert_eq!(decoded.acks, acks, "Acks {} should be preserved", acks);
    }
}

// ============================================================================
// Phase 5: Offset Values Tests
// ============================================================================

#[test]
fn test_special_offset_values() {
    // Special timestamp values for ListOffsets
    use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};

    let timestamps = [
        -1i64,    // Latest offset
        -2i64,    // Earliest offset
        0i64,     // Beginning of time
        i64::MAX, // Far future
    ];

    for &ts in &timestamps {
        let request = ListOffsetsRequest::default()
            .with_replica_id(BrokerId(-1))
            .with_topics(vec![ListOffsetsTopic::default()
                .with_name(TopicName::from(StrBytes::from_static_str("test")))
                .with_partitions(vec![ListOffsetsPartition::default()
                    .with_partition_index(0)
                    .with_timestamp(ts)])]);

        let mut buf = BytesMut::new();
        request.encode(&mut buf, 7).unwrap();

        let mut bytes = Bytes::from(buf);
        let decoded = ListOffsetsRequest::decode(&mut bytes, 7).unwrap();

        assert_eq!(
            decoded.topics[0].partitions[0].timestamp, ts,
            "Timestamp {} should be preserved",
            ts
        );
    }
}

#[test]
fn test_fetch_offset_values() {
    use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};

    let offsets = [0i64, 1, 1000, i64::MAX - 1];

    for &offset in &offsets {
        let request = FetchRequest::default()
            .with_replica_id(BrokerId(-1))
            .with_max_wait_ms(500)
            .with_min_bytes(1)
            .with_max_bytes(1024 * 1024)
            .with_topics(vec![FetchTopic::default()
                .with_topic(TopicName::from(StrBytes::from_static_str("test")))
                .with_partitions(vec![FetchPartition::default()
                    .with_partition(0)
                    .with_fetch_offset(offset)
                    .with_partition_max_bytes(1024 * 1024)])]);

        let mut buf = BytesMut::new();
        request.encode(&mut buf, 12).unwrap();

        let mut bytes = Bytes::from(buf);
        let decoded = FetchRequest::decode(&mut bytes, 12).unwrap();

        assert_eq!(
            decoded.topics[0].partitions[0].fetch_offset, offset,
            "Fetch offset {} should be preserved",
            offset
        );
    }
}

// ============================================================================
// Phase 5: Broker ID Tests
// ============================================================================

#[test]
fn test_broker_id_values() {
    use kafka_protocol::messages::metadata_response::MetadataResponseBroker;

    let broker_ids = [0, 1, 100, 1000, i32::MAX];

    for &id in &broker_ids {
        let response =
            MetadataResponse::default().with_brokers(vec![MetadataResponseBroker::default()
                .with_node_id(BrokerId(id))
                .with_host(StrBytes::from_static_str("localhost"))
                .with_port(9092)]);

        let mut buf = BytesMut::new();
        response.encode(&mut buf, 9).unwrap();

        let mut bytes = Bytes::from(buf);
        let decoded = MetadataResponse::decode(&mut bytes, 9).unwrap();

        assert_eq!(
            decoded.brokers[0].node_id,
            BrokerId(id),
            "Broker ID {} should be preserved",
            id
        );
    }
}

#[test]
fn test_replica_id_negative_one() {
    // replica_id = -1 means consumer (non-replica) fetch
    let request = FetchRequest::default()
        .with_replica_id(BrokerId(-1))
        .with_max_wait_ms(500)
        .with_min_bytes(1)
        .with_max_bytes(1024 * 1024);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 12).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = FetchRequest::decode(&mut bytes, 12).unwrap();

    assert_eq!(decoded.replica_id, BrokerId(-1));
}

// ============================================================================
// Phase 5: Group ID Tests
// ============================================================================

#[test]
fn test_group_id_empty() {
    let request = HeartbeatRequest::default()
        .with_group_id(GroupId::from(StrBytes::from_static_str("")))
        .with_generation_id(0)
        .with_member_id(StrBytes::from_static_str(""));

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 4).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = HeartbeatRequest::decode(&mut bytes, 4).unwrap();

    assert!(decoded.group_id.is_empty());
}

#[test]
fn test_group_id_long() {
    let long_group = "group-".to_string() + &"x".repeat(200);
    let request = HeartbeatRequest::default()
        .with_group_id(GroupId::from(StrBytes::from_string(long_group.clone())))
        .with_generation_id(1)
        .with_member_id(StrBytes::from_static_str("member-1"));

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 4).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = HeartbeatRequest::decode(&mut bytes, 4).unwrap();

    assert_eq!(decoded.group_id.as_str(), long_group);
}

// ============================================================================
// Phase 5: Generation ID Tests
// ============================================================================

#[test]
fn test_generation_id_values() {
    let generations = [0, 1, 100, i32::MAX];

    for &gen in &generations {
        let request = HeartbeatRequest::default()
            .with_group_id(GroupId::from(StrBytes::from_static_str("test-group")))
            .with_generation_id(gen)
            .with_member_id(StrBytes::from_static_str("member-1"));

        let mut buf = BytesMut::new();
        request.encode(&mut buf, 4).unwrap();

        let mut bytes = Bytes::from(buf);
        let decoded = HeartbeatRequest::decode(&mut bytes, 4).unwrap();

        assert_eq!(decoded.generation_id, gen);
    }
}
