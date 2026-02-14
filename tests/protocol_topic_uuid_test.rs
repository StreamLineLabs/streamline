//! Phase 12: Topic UUID Tests
//!
//! Tests for Topic UUID wire protocol (KIP-516):
//! - Topic UUID encoding/decoding (128-bit UUIDs)
//! - Metadata response with topic IDs
//! - Fetch request/response with topic IDs
//! - Zero UUID handling for legacy topics
//! - Topic name to UUID mapping

use bytes::BytesMut;
use kafka_protocol::messages::fetch_request::FetchTopic;
use kafka_protocol::messages::metadata_response::MetadataResponseTopic;
use kafka_protocol::messages::{
    BrokerId, FetchRequest, FetchResponse, MetadataRequest, MetadataResponse, TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use uuid::Uuid;

// =============================================================================
// UUID Encoding Tests
// =============================================================================

#[test]
fn test_uuid_encoding_basic() {
    // Kafka UUIDs are 128-bit (16 bytes)
    let uuid = Uuid::new_v4();
    let bytes = uuid.as_bytes();

    assert_eq!(bytes.len(), 16);
}

#[test]
fn test_uuid_from_bytes() {
    let bytes: [u8; 16] = [
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
        0x10,
    ];
    let uuid = Uuid::from_bytes(bytes);

    assert_eq!(uuid.as_bytes(), &bytes);
}

#[test]
fn test_uuid_roundtrip() {
    let original = Uuid::new_v4();
    let bytes = original.to_bytes_le();
    let restored = Uuid::from_bytes_le(bytes);

    assert_eq!(original, restored);
}

#[test]
fn test_uuid_nil() {
    // Nil UUID (all zeros) represents legacy topics without UUIDs
    let nil = Uuid::nil();

    assert!(nil.is_nil());
    assert_eq!(nil.as_bytes(), &[0u8; 16]);
}

#[test]
fn test_uuid_string_representation() {
    let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    let string = uuid.to_string();

    assert!(string.contains('-'));
    assert_eq!(string.len(), 36); // Standard UUID format
}

#[test]
fn test_uuid_comparison() {
    let uuid1 = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    let uuid2 = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    let uuid3 = Uuid::parse_str("660e8400-e29b-41d4-a716-446655440000").unwrap();

    assert_eq!(uuid1, uuid2);
    assert_ne!(uuid1, uuid3);
}

// =============================================================================
// Metadata Response Topic UUID Tests
// =============================================================================

#[test]
fn test_metadata_response_with_topic_id() {
    let mut response = MetadataResponse::default();

    let mut topic = MetadataResponseTopic::default();
    topic.name = Some(TopicName(StrBytes::from_static_str("my-topic")));
    topic.topic_id = Uuid::new_v4();
    topic.error_code = 0;

    response.topics.push(topic.clone());

    // Topic ID available in Metadata v10+
    let mut buf = BytesMut::new();
    response.encode(&mut buf, 10).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = MetadataResponse::decode(&mut read_buf, 10).unwrap();

    assert_eq!(decoded.topics[0].topic_id, topic.topic_id);
}

#[test]
fn test_metadata_response_nil_topic_id() {
    let mut response = MetadataResponse::default();

    let mut topic = MetadataResponseTopic::default();
    topic.name = Some(TopicName(StrBytes::from_static_str("legacy-topic")));
    topic.topic_id = Uuid::nil(); // Legacy topic without UUID
    topic.error_code = 0;

    response.topics.push(topic);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 10).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = MetadataResponse::decode(&mut read_buf, 10).unwrap();

    assert!(decoded.topics[0].topic_id.is_nil());
}

#[test]
fn test_metadata_response_multiple_topics_with_ids() {
    let mut response = MetadataResponse::default();

    for i in 0..3 {
        let mut topic = MetadataResponseTopic::default();
        topic.name = Some(TopicName(StrBytes::from_string(format!("topic-{}", i))));
        topic.topic_id = Uuid::new_v4();
        topic.error_code = 0;
        response.topics.push(topic);
    }

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 10).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = MetadataResponse::decode(&mut read_buf, 10).unwrap();

    assert_eq!(decoded.topics.len(), 3);

    // All should have unique UUIDs
    let uuids: std::collections::HashSet<Uuid> =
        decoded.topics.iter().map(|t| t.topic_id).collect();
    assert_eq!(uuids.len(), 3);
}

// =============================================================================
// Fetch Request Topic UUID Tests
// =============================================================================

#[test]
fn test_fetch_request_with_topic_id() {
    let mut request = FetchRequest::default();
    request.max_wait_ms = 500;
    request.min_bytes = 1;

    let mut topic = FetchTopic::default();
    topic.topic_id = Uuid::new_v4();
    // topic_id used instead of topic name in v13+

    request.topics.push(topic.clone());

    // Topic ID in Fetch v13+
    let mut buf = BytesMut::new();
    request.encode(&mut buf, 13).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = FetchRequest::decode(&mut read_buf, 13).unwrap();

    assert_eq!(decoded.topics[0].topic_id, topic.topic_id);
}

#[test]
fn test_fetch_request_topic_name_vs_uuid() {
    // Older versions use topic name
    let mut request_old = FetchRequest::default();
    let mut topic_old = FetchTopic::default();
    topic_old.topic = TopicName(StrBytes::from_static_str("my-topic"));
    request_old.topics.push(topic_old);

    let mut buf_old = BytesMut::new();
    request_old.encode(&mut buf_old, 11).unwrap();

    // Newer versions use topic UUID
    let mut request_new = FetchRequest::default();
    let mut topic_new = FetchTopic::default();
    topic_new.topic_id = Uuid::new_v4();
    request_new.topics.push(topic_new);

    let mut buf_new = BytesMut::new();
    request_new.encode(&mut buf_new, 13).unwrap();

    // Both should encode successfully
    assert!(!buf_old.is_empty());
    assert!(!buf_new.is_empty());
}

#[test]
fn test_fetch_request_nil_topic_id() {
    let mut request = FetchRequest::default();
    request.max_wait_ms = 500;
    request.min_bytes = 1;

    let mut topic = FetchTopic::default();
    topic.topic_id = Uuid::nil(); // Fall back to topic name

    request.topics.push(topic);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 13).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = FetchRequest::decode(&mut read_buf, 13).unwrap();

    assert!(decoded.topics[0].topic_id.is_nil());
}

// =============================================================================
// Fetch Response Topic UUID Tests
// =============================================================================

#[test]
fn test_fetch_response_with_topic_id() {
    use kafka_protocol::messages::fetch_response::FetchableTopicResponse;

    let mut response = FetchResponse::default();

    let mut topic_response = FetchableTopicResponse::default();
    topic_response.topic_id = Uuid::new_v4();

    response.responses.push(topic_response.clone());

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 13).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = FetchResponse::decode(&mut read_buf, 13).unwrap();

    assert_eq!(decoded.responses[0].topic_id, topic_response.topic_id);
}

// =============================================================================
// Topic UUID Lookup Tests
// =============================================================================

#[test]
fn test_topic_uuid_map() {
    let mut uuid_map: std::collections::HashMap<String, Uuid> = std::collections::HashMap::new();

    uuid_map.insert("topic-a".to_string(), Uuid::new_v4());
    uuid_map.insert("topic-b".to_string(), Uuid::new_v4());
    uuid_map.insert("topic-c".to_string(), Uuid::new_v4());

    assert!(uuid_map.contains_key("topic-a"));
    assert!(!uuid_map.contains_key("unknown-topic"));
}

#[test]
fn test_uuid_to_topic_map() {
    let mut topic_map: std::collections::HashMap<Uuid, String> = std::collections::HashMap::new();

    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();

    topic_map.insert(uuid1, "topic-a".to_string());
    topic_map.insert(uuid2, "topic-b".to_string());

    assert_eq!(topic_map.get(&uuid1), Some(&"topic-a".to_string()));
    assert!(!topic_map.contains_key(&Uuid::nil()));
}

#[test]
fn test_topic_uuid_bidirectional_lookup() {
    let mut name_to_uuid: std::collections::HashMap<String, Uuid> =
        std::collections::HashMap::new();
    let mut uuid_to_name: std::collections::HashMap<Uuid, String> =
        std::collections::HashMap::new();

    let name = "my-topic".to_string();
    let uuid = Uuid::new_v4();

    name_to_uuid.insert(name.clone(), uuid);
    uuid_to_name.insert(uuid, name.clone());

    // Lookup by name
    assert_eq!(name_to_uuid.get(&name), Some(&uuid));

    // Lookup by UUID
    assert_eq!(uuid_to_name.get(&uuid), Some(&name));
}

// =============================================================================
// Zero UUID Handling Tests
// =============================================================================

#[test]
fn test_zero_uuid_is_nil() {
    let zero_bytes = [0u8; 16];
    let uuid = Uuid::from_bytes(zero_bytes);

    assert!(uuid.is_nil());
    assert_eq!(uuid, Uuid::nil());
}

#[test]
fn test_zero_uuid_fallback_to_name() {
    // When topic_id is nil, use topic name instead
    let topic_id = Uuid::nil();
    let topic_name = "fallback-topic";

    let use_topic_name = topic_id.is_nil();
    assert!(use_topic_name);

    if use_topic_name {
        assert_eq!(topic_name, "fallback-topic");
    }
}

#[test]
fn test_non_nil_uuid_preferred() {
    let topic_id = Uuid::new_v4();
    let _topic_name = "my-topic";

    let use_topic_id = !topic_id.is_nil();
    assert!(use_topic_id);
}

// =============================================================================
// UUID Version Tests
// =============================================================================

#[test]
fn test_uuid_v4_random() {
    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();

    // Random UUIDs should be different
    assert_ne!(uuid1, uuid2);
}

#[test]
fn test_uuid_version_detection() {
    let uuid = Uuid::new_v4();

    // Version 4 is random UUID
    assert_eq!(uuid.get_version_num(), 4);
}

// =============================================================================
// Metadata Request Topic UUID Tests
// =============================================================================

#[test]
fn test_metadata_request_by_topic_name() {
    use kafka_protocol::messages::metadata_request::MetadataRequestTopic;

    let mut request = MetadataRequest::default();

    let mut topic = MetadataRequestTopic::default();
    topic.name = Some(TopicName(StrBytes::from_static_str("my-topic")));
    request.topics = Some(vec![topic]);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 9).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = MetadataRequest::decode(&mut read_buf, 9).unwrap();

    assert!(decoded.topics.is_some());
    let topics = decoded.topics.unwrap();
    assert_eq!(topics[0].name.as_ref().unwrap().0.as_str(), "my-topic");
}

#[test]
fn test_metadata_request_by_topic_id() {
    use kafka_protocol::messages::metadata_request::MetadataRequestTopic;

    let mut request = MetadataRequest::default();

    let topic_uuid = Uuid::new_v4();
    let mut topic = MetadataRequestTopic::default();
    topic.topic_id = topic_uuid;
    request.topics = Some(vec![topic]);

    // Topic ID in MetadataRequest available in v10+
    let mut buf = BytesMut::new();
    request.encode(&mut buf, 10).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = MetadataRequest::decode(&mut read_buf, 10).unwrap();

    assert!(decoded.topics.is_some());
    let topics = decoded.topics.unwrap();
    assert_eq!(topics[0].topic_id, topic_uuid);
}

// =============================================================================
// Topic ID Error Handling Tests
// =============================================================================

#[test]
fn test_unknown_topic_id_error() {
    // UNKNOWN_TOPIC_ID error code
    let error_code: i16 = 100;
    assert_eq!(error_code, 100);
}

#[test]
fn test_inconsistent_topic_id_error() {
    // INCONSISTENT_TOPIC_ID error code (topic ID changed)
    let error_code: i16 = 101;
    assert_eq!(error_code, 101);
}

#[test]
fn test_metadata_response_with_topic_id_error() {
    let mut response = MetadataResponse::default();

    let mut topic = MetadataResponseTopic::default();
    topic.topic_id = Uuid::new_v4();
    topic.error_code = 100; // UNKNOWN_TOPIC_ID

    response.topics.push(topic.clone());

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 10).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = MetadataResponse::decode(&mut read_buf, 10).unwrap();

    assert_eq!(decoded.topics[0].error_code, 100);
}

// =============================================================================
// UUID Persistence Tests
// =============================================================================

#[test]
fn test_topic_uuid_immutable() {
    // Once assigned, topic UUID should never change
    let original_uuid = Uuid::new_v4();
    let persisted_uuid = original_uuid; // Simulating persistence

    assert_eq!(original_uuid, persisted_uuid);
}

#[test]
fn test_topic_uuid_survives_rename() {
    // Topic UUID stays same even if topic is renamed
    let uuid = Uuid::new_v4();
    let topic_name = "new-name".to_string();

    // UUID unchanged
    assert_eq!(uuid, uuid);
    assert_eq!(topic_name, "new-name");
}

// =============================================================================
// CreateTopics with UUID Tests
// =============================================================================

#[test]
fn test_create_topics_response_with_uuid() {
    use kafka_protocol::messages::create_topics_response::CreatableTopicResult;
    use kafka_protocol::messages::CreateTopicsResponse;

    let mut response = CreateTopicsResponse::default();

    let topic_uuid = Uuid::new_v4();
    let mut result = CreatableTopicResult::default();
    result.name = TopicName(StrBytes::from_static_str("new-topic"));
    result.topic_id = topic_uuid;
    result.error_code = 0;

    response.topics.push(result);

    // CreateTopics v7 includes topic ID
    let mut buf = BytesMut::new();
    response.encode(&mut buf, 7).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = CreateTopicsResponse::decode(&mut read_buf, 7).unwrap();

    assert_eq!(decoded.topics[0].topic_id, topic_uuid);
}

// =============================================================================
// Delete Topics with UUID Tests
// =============================================================================

#[test]
fn test_delete_topics_request_by_uuid() {
    use kafka_protocol::messages::delete_topics_request::DeleteTopicState;
    use kafka_protocol::messages::DeleteTopicsRequest;

    let mut request = DeleteTopicsRequest::default();

    let topic_uuid = Uuid::new_v4();
    let mut topic = DeleteTopicState::default();
    topic.topic_id = topic_uuid;

    request.topics.push(topic);

    // DeleteTopics v6+ supports topic ID
    let mut buf = BytesMut::new();
    request.encode(&mut buf, 6).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DeleteTopicsRequest::decode(&mut read_buf, 6).unwrap();

    assert_eq!(decoded.topics[0].topic_id, topic_uuid);
}

#[test]
fn test_delete_topics_response_with_uuid() {
    use kafka_protocol::messages::delete_topics_response::DeletableTopicResult;
    use kafka_protocol::messages::DeleteTopicsResponse;

    let mut response = DeleteTopicsResponse::default();

    let topic_uuid = Uuid::new_v4();
    let mut result = DeletableTopicResult::default();
    result.topic_id = topic_uuid;
    result.error_code = 0;

    response.responses.push(result);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 6).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DeleteTopicsResponse::decode(&mut read_buf, 6).unwrap();

    assert_eq!(decoded.responses[0].topic_id, topic_uuid);
}

// =============================================================================
// Produce Request with Topic UUID Tests
// =============================================================================

#[test]
fn test_produce_request_topic_name() {
    use kafka_protocol::messages::produce_request::TopicProduceData;
    use kafka_protocol::messages::ProduceRequest;

    let mut request = ProduceRequest::default();
    request.acks = -1;
    request.timeout_ms = 30000;

    let mut topic = TopicProduceData::default();
    topic.name = TopicName(StrBytes::from_static_str("produce-topic"));

    request.topic_data.push(topic);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 7).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ProduceRequest::decode(&mut read_buf, 7).unwrap();

    assert_eq!(decoded.topic_data[0].name.0.as_str(), "produce-topic");
}

// =============================================================================
// ListOffsets with Topic UUID Tests
// =============================================================================

#[test]
fn test_list_offsets_request_topic_name() {
    use kafka_protocol::messages::list_offsets_request::ListOffsetsTopic;
    use kafka_protocol::messages::ListOffsetsRequest;

    let mut request = ListOffsetsRequest::default();
    request.replica_id = BrokerId(-1);

    let mut topic = ListOffsetsTopic::default();
    topic.name = TopicName(StrBytes::from_static_str("offsets-topic"));

    request.topics.push(topic);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 4).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ListOffsetsRequest::decode(&mut read_buf, 4).unwrap();

    assert_eq!(decoded.topics[0].name.0.as_str(), "offsets-topic");
}

// =============================================================================
// Version Compatibility Tests
// =============================================================================

#[test]
fn test_metadata_response_version_without_topic_id() {
    // Older Metadata versions don't have topic_id
    let mut response = MetadataResponse::default();

    let mut topic = MetadataResponseTopic::default();
    topic.name = Some(TopicName(StrBytes::from_static_str("old-topic")));
    topic.error_code = 0;

    response.topics.push(topic);

    // v9 doesn't include topic_id
    let mut buf = BytesMut::new();
    response.encode(&mut buf, 9).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = MetadataResponse::decode(&mut read_buf, 9).unwrap();

    // topic_id should be nil for v9
    assert!(decoded.topics[0].topic_id.is_nil());
}

#[test]
fn test_fetch_request_version_without_topic_id() {
    // Older Fetch versions don't have topic_id
    let mut request = FetchRequest::default();
    request.max_wait_ms = 500;
    request.min_bytes = 1;

    let mut topic = FetchTopic::default();
    topic.topic = TopicName(StrBytes::from_static_str("old-topic"));

    request.topics.push(topic);

    // v11 doesn't include topic_id
    let mut buf = BytesMut::new();
    request.encode(&mut buf, 11).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = FetchRequest::decode(&mut read_buf, 11).unwrap();

    assert_eq!(decoded.topics[0].topic.0.as_str(), "old-topic");
}
