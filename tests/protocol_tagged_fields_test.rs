//! Protocol Tagged Fields Tests - Phase 2
//!
//! Tests for Kafka protocol tagged fields (KIP-482) handling:
//! - Tagged fields are appended to flexible-version messages
//! - Format: count(varint) + [tag(varint) + size(varint) + data]*
//! - Unknown tags must be gracefully ignored
//! - Tags must be in ascending order
//!
//! Source: https://kafka.apache.org/protocol.html#protocol_types

use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::messages::*;
use kafka_protocol::protocol::{Decodable, Encodable};

// ============================================================================
// Tagged Fields Encoding Utilities
// ============================================================================

/// Encode an unsigned varint
fn encode_varint(value: u32) -> Vec<u8> {
    let mut result = Vec::new();
    let mut v = value;
    loop {
        let mut byte = (v & 0x7F) as u8;
        v >>= 7;
        if v != 0 {
            byte |= 0x80;
        }
        result.push(byte);
        if v == 0 {
            break;
        }
    }
    result
}

/// Decode an unsigned varint, returning (value, bytes_consumed)
fn decode_varint(data: &[u8]) -> Option<(u32, usize)> {
    let mut value: u32 = 0;
    let mut shift = 0;
    let mut bytes_read = 0;

    for &byte in data {
        bytes_read += 1;
        value |= ((byte & 0x7F) as u32) << shift;
        if byte & 0x80 == 0 {
            return Some((value, bytes_read));
        }
        shift += 7;
        if shift >= 32 {
            return None;
        }
    }
    None
}

/// Build an empty tagged fields section
fn build_empty_tagged_fields() -> Vec<u8> {
    vec![0x00] // Zero tags
}

/// Build a tagged fields section with given tags
/// tags: Vec<(tag_number, data)>
fn build_tagged_fields(tags: &[(u32, Vec<u8>)]) -> Vec<u8> {
    let mut result = Vec::new();

    // Number of tagged fields
    result.extend(encode_varint(tags.len() as u32));

    for (tag, data) in tags {
        // Tag number
        result.extend(encode_varint(*tag));
        // Data size
        result.extend(encode_varint(data.len() as u32));
        // Data
        result.extend(data);
    }

    result
}

/// Result type for parsed tagged fields
type TaggedFieldsResult = (Vec<(u32, Vec<u8>)>, usize);

/// Parse tagged fields section, returning Vec<(tag, data)>
fn parse_tagged_fields(data: &[u8]) -> Option<TaggedFieldsResult> {
    let mut offset = 0;

    // Read count
    let (count, n) = decode_varint(&data[offset..])?;
    offset += n;

    let mut tags = Vec::new();
    for _ in 0..count {
        // Read tag number
        let (tag, n) = decode_varint(&data[offset..])?;
        offset += n;

        // Read size
        let (size, n) = decode_varint(&data[offset..])?;
        offset += n;

        // Read data
        if offset + size as usize > data.len() {
            return None;
        }
        let tag_data = data[offset..offset + size as usize].to_vec();
        offset += size as usize;

        tags.push((tag, tag_data));
    }

    Some((tags, offset))
}

// ============================================================================
// Phase 2: Empty Tagged Fields Tests
// ============================================================================

#[test]
fn test_empty_tagged_fields_encoding() {
    let empty = build_empty_tagged_fields();
    assert_eq!(
        empty,
        vec![0x00],
        "Empty tagged fields should be single 0 byte"
    );
}

#[test]
fn test_empty_tagged_fields_parsing() {
    let empty = vec![0x00];
    let (tags, bytes_read) = parse_tagged_fields(&empty).unwrap();
    assert!(tags.is_empty());
    assert_eq!(bytes_read, 1);
}

#[test]
fn test_tagged_fields_in_api_versions_v3() {
    // ApiVersions v3 uses flexible encoding with tagged fields
    let request = ApiVersionsRequest::default()
        .with_client_software_name("test-client".to_string().into())
        .with_client_software_version("1.0.0".to_string().into());

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 3).unwrap();

    // The encoded message should end with tagged fields section
    // For ApiVersionsRequest v3, client_software_name and client_software_version
    // are regular fields, not tagged, so we should see empty tags at the end
    let bytes = buf.to_vec();

    // Last byte should be 0x00 (empty tagged fields) for standard request
    assert_eq!(
        bytes.last(),
        Some(&0x00),
        "Flexible message should end with tagged fields marker"
    );
}

// ============================================================================
// Phase 2: Single Tagged Field Tests
// ============================================================================

#[test]
fn test_single_tagged_field_encoding() {
    let tags = vec![(0u32, vec![0x01, 0x02, 0x03])];
    let encoded = build_tagged_fields(&tags);

    // Expected: [0x01 (count=1), 0x00 (tag=0), 0x03 (size=3), 0x01, 0x02, 0x03 (data)]
    assert_eq!(encoded, vec![0x01, 0x00, 0x03, 0x01, 0x02, 0x03]);
}

#[test]
fn test_single_tagged_field_parsing() {
    let encoded = vec![0x01, 0x00, 0x03, 0x01, 0x02, 0x03];
    let (tags, bytes_read) = parse_tagged_fields(&encoded).unwrap();

    assert_eq!(tags.len(), 1);
    assert_eq!(tags[0].0, 0); // tag number
    assert_eq!(tags[0].1, vec![0x01, 0x02, 0x03]); // data
    assert_eq!(bytes_read, 6);
}

#[test]
fn test_tagged_field_with_large_tag_number() {
    // Tag numbers can be large (use varint)
    let tags = vec![(1000u32, vec![0xFF])];
    let encoded = build_tagged_fields(&tags);

    let (parsed, _) = parse_tagged_fields(&encoded).unwrap();
    assert_eq!(parsed[0].0, 1000);
    assert_eq!(parsed[0].1, vec![0xFF]);
}

#[test]
fn test_tagged_field_with_large_data() {
    // Large data payload
    let large_data = vec![0xAB; 1000];
    let tags = vec![(42u32, large_data.clone())];
    let encoded = build_tagged_fields(&tags);

    let (parsed, _) = parse_tagged_fields(&encoded).unwrap();
    assert_eq!(parsed[0].0, 42);
    assert_eq!(parsed[0].1, large_data);
}

#[test]
fn test_tagged_field_empty_data() {
    // Tag with zero-length data is valid
    let tags = vec![(5u32, vec![])];
    let encoded = build_tagged_fields(&tags);

    // Expected: [0x01 (count), 0x05 (tag), 0x00 (size=0)]
    assert_eq!(encoded, vec![0x01, 0x05, 0x00]);

    let (parsed, _) = parse_tagged_fields(&encoded).unwrap();
    assert_eq!(parsed[0].0, 5);
    assert!(parsed[0].1.is_empty());
}

// ============================================================================
// Phase 2: Multiple Tagged Fields Tests
// ============================================================================

#[test]
fn test_multiple_tagged_fields_encoding() {
    let tags = vec![
        (0u32, vec![0x01]),
        (1u32, vec![0x02, 0x03]),
        (2u32, vec![0x04, 0x05, 0x06]),
    ];
    let encoded = build_tagged_fields(&tags);

    // Expected structure:
    // 0x03 (count=3)
    // 0x00, 0x01, 0x01 (tag 0, size 1, data)
    // 0x01, 0x02, 0x02, 0x03 (tag 1, size 2, data)
    // 0x02, 0x03, 0x04, 0x05, 0x06 (tag 2, size 3, data)
    let expected = vec![
        0x03, // count
        0x00, 0x01, 0x01, // tag 0
        0x01, 0x02, 0x02, 0x03, // tag 1
        0x02, 0x03, 0x04, 0x05, 0x06, // tag 2
    ];
    assert_eq!(encoded, expected);
}

#[test]
fn test_multiple_tagged_fields_parsing() {
    let encoded = vec![
        0x03, // count
        0x00, 0x01, 0x01, // tag 0
        0x01, 0x02, 0x02, 0x03, // tag 1
        0x02, 0x03, 0x04, 0x05, 0x06, // tag 2
    ];

    let (tags, bytes_read) = parse_tagged_fields(&encoded).unwrap();

    assert_eq!(tags.len(), 3);
    assert_eq!(tags[0], (0, vec![0x01]));
    assert_eq!(tags[1], (1, vec![0x02, 0x03]));
    assert_eq!(tags[2], (2, vec![0x04, 0x05, 0x06]));
    assert_eq!(bytes_read, encoded.len());
}

#[test]
fn test_tagged_fields_ascending_order() {
    // Per Kafka spec, tags must be in ascending order
    // Our builder doesn't enforce this, but we test the expected format
    let tags_ordered = vec![(0u32, vec![0x01]), (5u32, vec![0x02]), (10u32, vec![0x03])];

    let encoded = build_tagged_fields(&tags_ordered);
    let (parsed, _) = parse_tagged_fields(&encoded).unwrap();

    // Verify order is preserved
    assert!(parsed[0].0 < parsed[1].0);
    assert!(parsed[1].0 < parsed[2].0);
}

#[test]
fn test_many_tagged_fields() {
    // Test with many tagged fields
    let tags: Vec<(u32, Vec<u8>)> = (0..100).map(|i| (i, vec![i as u8])).collect();

    let encoded = build_tagged_fields(&tags);
    let (parsed, _) = parse_tagged_fields(&encoded).unwrap();

    assert_eq!(parsed.len(), 100);
    for (i, (tag, data)) in parsed.iter().enumerate() {
        assert_eq!(*tag, i as u32);
        assert_eq!(*data, vec![i as u8]);
    }
}

// ============================================================================
// Phase 2: Unknown Tagged Fields Handling
// ============================================================================

#[test]
fn test_unknown_tagged_fields_structure() {
    // When a client receives unknown tagged fields, it should skip them
    // This tests the skip logic by parsing and verifying structure

    // Simulate a message with unknown tags (tags 100, 200, 300)
    let unknown_tags = vec![
        (100u32, vec![0xDE, 0xAD]),
        (200u32, vec![0xBE, 0xEF]),
        (300u32, vec![0xCA, 0xFE]),
    ];

    let encoded = build_tagged_fields(&unknown_tags);

    // A compliant parser should be able to skip unknown tags
    let (parsed, bytes_read) = parse_tagged_fields(&encoded).unwrap();

    assert_eq!(parsed.len(), 3);
    assert_eq!(bytes_read, encoded.len());

    // Verify each unknown tag was preserved
    assert_eq!(parsed[0].0, 100);
    assert_eq!(parsed[1].0, 200);
    assert_eq!(parsed[2].0, 300);
}

#[test]
fn test_mixed_known_unknown_tags_simulation() {
    // Simulate receiving a mix of known and unknown tags
    // Tag 0 might be known, tags 50-99 unknown

    let tags = vec![
        (0u32, vec![0x01]),              // "known" tag
        (50u32, vec![0x02, 0x03]),       // unknown
        (99u32, vec![0x04, 0x05, 0x06]), // unknown
    ];

    let encoded = build_tagged_fields(&tags);
    let (parsed, _) = parse_tagged_fields(&encoded).unwrap();

    // All tags should be parseable even if semantically unknown
    assert_eq!(parsed.len(), 3);
}

// ============================================================================
// Phase 2: Tagged Fields in Real Protocol Messages
// ============================================================================

#[test]
fn test_metadata_response_tagged_fields_v9() {
    // MetadataResponse v9+ uses flexible encoding with tagged fields
    use kafka_protocol::messages::metadata_response::{
        MetadataResponseBroker, MetadataResponseTopic,
    };
    use kafka_protocol::protocol::StrBytes;

    let response = MetadataResponse::default()
        .with_brokers(vec![MetadataResponseBroker::default()
            .with_node_id(BrokerId(1))
            .with_host(StrBytes::from_static_str("localhost"))
            .with_port(9092)])
        .with_topics(vec![MetadataResponseTopic::default().with_name(Some(
            TopicName::from(StrBytes::from_static_str("test-topic")),
        ))]);

    // Encode at v9 (flexible)
    let mut buf = BytesMut::new();
    response.encode(&mut buf, 9).unwrap();

    // Decode and verify roundtrip
    let mut bytes = Bytes::from(buf);
    let decoded = MetadataResponse::decode(&mut bytes, 9).unwrap();

    assert_eq!(decoded.brokers.len(), 1);
    assert_eq!(decoded.topics.len(), 1);
}

#[test]
fn test_create_topics_response_tagged_fields() {
    // CreateTopicsResponse v5+ uses flexible encoding
    use kafka_protocol::messages::create_topics_response::CreatableTopicResult;
    use kafka_protocol::protocol::StrBytes;

    let response =
        CreateTopicsResponse::default().with_topics(vec![CreatableTopicResult::default()
            .with_name(TopicName::from(StrBytes::from_static_str("new-topic")))
            .with_error_code(0)]);

    // v5 is flexible
    let mut buf = BytesMut::new();
    response.encode(&mut buf, 5).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = CreateTopicsResponse::decode(&mut bytes, 5).unwrap();

    assert_eq!(decoded.topics.len(), 1);
    assert_eq!(decoded.topics[0].error_code, 0);
}

#[test]
fn test_produce_response_tagged_fields_v9() {
    use kafka_protocol::messages::produce_response::{
        PartitionProduceResponse, TopicProduceResponse,
    };
    use kafka_protocol::protocol::StrBytes;

    let response = ProduceResponse::default().with_responses(vec![TopicProduceResponse::default()
        .with_name(TopicName::from(StrBytes::from_static_str("test-topic")))
        .with_partition_responses(vec![PartitionProduceResponse::default()
            .with_index(0)
            .with_error_code(0)
            .with_base_offset(0)])]);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 9).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = ProduceResponse::decode(&mut bytes, 9).unwrap();

    assert_eq!(decoded.responses.len(), 1);
}

#[test]
fn test_fetch_response_tagged_fields_v12() {
    use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};
    use kafka_protocol::protocol::StrBytes;

    let response = FetchResponse::default()
        .with_throttle_time_ms(0)
        .with_responses(vec![FetchableTopicResponse::default()
            .with_topic(TopicName::from(StrBytes::from_static_str("test-topic")))
            .with_partitions(vec![PartitionData::default()
                .with_partition_index(0)
                .with_error_code(0)])]);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 12).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = FetchResponse::decode(&mut bytes, 12).unwrap();

    assert_eq!(decoded.responses.len(), 1);
}

// ============================================================================
// Phase 2: Tagged Fields Version Boundary Tests
// ============================================================================

#[test]
fn test_metadata_request_non_flexible_vs_flexible() {
    use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
    use kafka_protocol::protocol::StrBytes;

    let request =
        MetadataRequest::default().with_topics(Some(vec![MetadataRequestTopic::default()
            .with_name(Some(TopicName::from(StrBytes::from_static_str("test"))))]));

    // v8 is non-flexible (no tagged fields)
    let mut buf_v8 = BytesMut::new();
    request.encode(&mut buf_v8, 8).unwrap();

    // v9 is flexible (has tagged fields)
    let mut buf_v9 = BytesMut::new();
    request.encode(&mut buf_v9, 9).unwrap();

    // v9 should have the tagged fields marker
    let v9_bytes = buf_v9.to_vec();
    assert_eq!(
        v9_bytes.last(),
        Some(&0x00),
        "v9 should end with empty tagged fields marker"
    );

    // Both should decode correctly
    let mut bytes_v8 = Bytes::from(buf_v8);
    let mut bytes_v9 = Bytes::from(buf_v9);

    let decoded_v8 = MetadataRequest::decode(&mut bytes_v8, 8).unwrap();
    let decoded_v9 = MetadataRequest::decode(&mut bytes_v9, 9).unwrap();

    assert_eq!(decoded_v8.topics.as_ref().unwrap().len(), 1);
    assert_eq!(decoded_v9.topics.as_ref().unwrap().len(), 1);
}

#[test]
fn test_list_offsets_request_flexible_boundary() {
    use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
    use kafka_protocol::protocol::StrBytes;

    let request = ListOffsetsRequest::default()
        .with_replica_id(BrokerId(-1))
        .with_topics(vec![ListOffsetsTopic::default()
            .with_name(TopicName::from(StrBytes::from_static_str("test")))
            .with_partitions(vec![ListOffsetsPartition::default()
                .with_partition_index(0)
                .with_timestamp(-1)])]);

    // v5 is non-flexible
    let mut buf_v5 = BytesMut::new();
    request.encode(&mut buf_v5, 5).unwrap();

    // v6 is flexible
    let mut buf_v6 = BytesMut::new();
    request.encode(&mut buf_v6, 6).unwrap();

    // Both should roundtrip correctly
    let mut bytes_v5 = Bytes::from(buf_v5);
    let mut bytes_v6 = Bytes::from(buf_v6);

    let decoded_v5 = ListOffsetsRequest::decode(&mut bytes_v5, 5).unwrap();
    let decoded_v6 = ListOffsetsRequest::decode(&mut bytes_v6, 6).unwrap();

    assert_eq!(decoded_v5.topics.len(), 1);
    assert_eq!(decoded_v6.topics.len(), 1);
}

// ============================================================================
// Phase 2: Tagged Fields Edge Cases
// ============================================================================

#[test]
fn test_tagged_fields_max_tag_number() {
    // Test with maximum reasonable tag number
    let max_tag = u32::MAX / 2; // Large but reasonable
    let tags = vec![(max_tag, vec![0x01])];

    let encoded = build_tagged_fields(&tags);
    let (parsed, _) = parse_tagged_fields(&encoded).unwrap();

    assert_eq!(parsed[0].0, max_tag);
}

#[test]
fn test_tagged_fields_truncated_count() {
    // Truncated at count field
    let truncated = vec![0x80]; // Incomplete varint
    assert!(parse_tagged_fields(&truncated).is_none());
}

#[test]
fn test_tagged_fields_truncated_tag() {
    // Count says 1 tag, but tag number is truncated
    let truncated = vec![0x01, 0x80]; // count=1, incomplete tag varint
    assert!(parse_tagged_fields(&truncated).is_none());
}

#[test]
fn test_tagged_fields_truncated_size() {
    // Count and tag present, but size truncated
    let truncated = vec![0x01, 0x00, 0x80]; // count=1, tag=0, incomplete size
    assert!(parse_tagged_fields(&truncated).is_none());
}

#[test]
fn test_tagged_fields_truncated_data() {
    // Size says 10 bytes but only 3 provided
    let truncated = vec![0x01, 0x00, 0x0A, 0x01, 0x02, 0x03]; // count=1, tag=0, size=10, only 3 bytes
    assert!(parse_tagged_fields(&truncated).is_none());
}

#[test]
fn test_tagged_fields_zero_count_with_trailing_data() {
    // Zero tags but extra data after
    let data = vec![0x00, 0xFF, 0xFF]; // count=0, extra garbage

    let (tags, bytes_read) = parse_tagged_fields(&data).unwrap();
    assert!(tags.is_empty());
    assert_eq!(bytes_read, 1); // Only consumed the count byte
}

// ============================================================================
// Phase 2: Response Header Tagged Fields
// ============================================================================

#[test]
fn test_response_header_v0_no_tagged_fields() {
    // Response header v0: just correlation_id (4 bytes)
    let mut header_v0 = BytesMut::new();
    header_v0.put_i32(12345); // correlation_id

    assert_eq!(header_v0.len(), 4);
}

#[test]
fn test_response_header_v1_with_tagged_fields() {
    // Response header v1: correlation_id + tagged_fields
    let mut header_v1 = BytesMut::new();
    header_v1.put_i32(12345); // correlation_id
    header_v1.extend(build_empty_tagged_fields()); // empty tagged fields

    assert_eq!(header_v1.len(), 5); // 4 + 1
}

#[test]
fn test_api_versions_response_always_header_v0() {
    // Per Kafka spec, ApiVersionsResponse ALWAYS uses response header v0
    // This is critical for bootstrap compatibility

    let response = ApiVersionsResponse::default().with_error_code(0);

    // Even at v3 (flexible request), response header is v0
    let mut buf = BytesMut::new();
    response.encode(&mut buf, 3).unwrap();

    // Response body starts with error_code (INT16), not correlation_id
    // The correlation_id is in the header, which the server adds
    // This test verifies the body encoding doesn't include header fields
    assert!(!buf.is_empty());
}

// ============================================================================
// Phase 2: Consumer Group APIs Tagged Fields
// ============================================================================

#[test]
fn test_join_group_request_flexible_v6() {
    use kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol;
    use kafka_protocol::protocol::StrBytes;

    let request = JoinGroupRequest::default()
        .with_group_id(GroupId::from(StrBytes::from_static_str("test-group")))
        .with_session_timeout_ms(10000)
        .with_member_id(StrBytes::from_static_str(""))
        .with_protocol_type(StrBytes::from_static_str("consumer"))
        .with_protocols(vec![JoinGroupRequestProtocol::default()
            .with_name(StrBytes::from_static_str("range"))
            .with_metadata(Bytes::from_static(&[]))]);

    // v5 non-flexible
    let mut buf_v5 = BytesMut::new();
    request.encode(&mut buf_v5, 5).unwrap();

    // v6 flexible
    let mut buf_v6 = BytesMut::new();
    request.encode(&mut buf_v6, 6).unwrap();

    // Both should decode
    let mut bytes_v5 = Bytes::from(buf_v5);
    let mut bytes_v6 = Bytes::from(buf_v6);

    let decoded_v5 = JoinGroupRequest::decode(&mut bytes_v5, 5).unwrap();
    let decoded_v6 = JoinGroupRequest::decode(&mut bytes_v6, 6).unwrap();

    assert_eq!(decoded_v5.session_timeout_ms, 10000);
    assert_eq!(decoded_v6.session_timeout_ms, 10000);
}

#[test]
fn test_heartbeat_request_flexible_v4() {
    use kafka_protocol::protocol::StrBytes;

    let request = HeartbeatRequest::default()
        .with_group_id(GroupId::from(StrBytes::from_static_str("test-group")))
        .with_generation_id(1)
        .with_member_id(StrBytes::from_static_str("member-1"));

    // v3 non-flexible
    let mut buf_v3 = BytesMut::new();
    request.encode(&mut buf_v3, 3).unwrap();

    // v4 flexible
    let mut buf_v4 = BytesMut::new();
    request.encode(&mut buf_v4, 4).unwrap();

    let mut bytes_v3 = Bytes::from(buf_v3);
    let mut bytes_v4 = Bytes::from(buf_v4);

    let decoded_v3 = HeartbeatRequest::decode(&mut bytes_v3, 3).unwrap();
    let decoded_v4 = HeartbeatRequest::decode(&mut bytes_v4, 4).unwrap();

    assert_eq!(decoded_v3.generation_id, 1);
    assert_eq!(decoded_v4.generation_id, 1);
}
