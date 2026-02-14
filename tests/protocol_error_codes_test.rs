//! Phase 21: Protocol Error Code Tests
//!
//! Tests for Kafka protocol error code encoding/decoding:
//! - Common error codes across various response types
//! - Error code semantics (retriable vs non-retriable)
//! - Error propagation in nested response structures
//!
//! Error codes are defined in the Kafka protocol specification.
//! Reference: https://kafka.apache.org/protocol.html#protocol_error_codes

use bytes::BytesMut;
use kafka_protocol::messages::{
    ApiVersionsResponse, CreateTopicsResponse, DeleteTopicsResponse, FetchResponse,
    ListOffsetsResponse, MetadataResponse, ProduceResponse,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};

// ============================================================================
// Error Code Constants (from Kafka specification)
// ============================================================================

// No error
const NONE: i16 = 0;

// Retriable errors
const OFFSET_OUT_OF_RANGE: i16 = 1;
const CORRUPT_MESSAGE: i16 = 2;
const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
const LEADER_NOT_AVAILABLE: i16 = 5;
const NOT_LEADER_OR_FOLLOWER: i16 = 6;
const REQUEST_TIMED_OUT: i16 = 7;
const BROKER_NOT_AVAILABLE: i16 = 8;
const REPLICA_NOT_AVAILABLE: i16 = 9;
const STALE_CONTROLLER_EPOCH: i16 = 11;
const NOT_ENOUGH_REPLICAS: i16 = 19;
const NOT_ENOUGH_REPLICAS_AFTER_APPEND: i16 = 20;
const NOT_CONTROLLER: i16 = 41;
const KAFKA_STORAGE_ERROR: i16 = 56;

// Non-retriable errors
const INVALID_FETCH_SIZE: i16 = 4;
const MESSAGE_TOO_LARGE: i16 = 10;
const OFFSET_METADATA_TOO_LARGE: i16 = 12;
const NETWORK_EXCEPTION: i16 = 13;
const COORDINATOR_LOAD_IN_PROGRESS: i16 = 14;
const COORDINATOR_NOT_AVAILABLE: i16 = 15;
const NOT_COORDINATOR: i16 = 16;
const INVALID_TOPIC_EXCEPTION: i16 = 17;
const RECORD_LIST_TOO_LARGE: i16 = 18;
const INVALID_REQUIRED_ACKS: i16 = 21;
const ILLEGAL_GENERATION: i16 = 22;
const INCONSISTENT_GROUP_PROTOCOL: i16 = 23;
const INVALID_GROUP_ID: i16 = 24;
const UNKNOWN_MEMBER_ID: i16 = 25;
const INVALID_SESSION_TIMEOUT: i16 = 26;
const REBALANCE_IN_PROGRESS: i16 = 27;
const INVALID_COMMIT_OFFSET_SIZE: i16 = 28;
const TOPIC_AUTHORIZATION_FAILED: i16 = 29;
const GROUP_AUTHORIZATION_FAILED: i16 = 30;
const CLUSTER_AUTHORIZATION_FAILED: i16 = 31;
const INVALID_TIMESTAMP: i16 = 32;
const UNSUPPORTED_SASL_MECHANISM: i16 = 33;
const ILLEGAL_SASL_STATE: i16 = 34;
const UNSUPPORTED_VERSION: i16 = 35;
const TOPIC_ALREADY_EXISTS: i16 = 36;
const INVALID_PARTITIONS: i16 = 37;
const INVALID_REPLICATION_FACTOR: i16 = 38;
const INVALID_REPLICA_ASSIGNMENT: i16 = 39;
const INVALID_CONFIG: i16 = 40;
const SASL_AUTHENTICATION_FAILED: i16 = 58;
const UNKNOWN_PRODUCER_ID: i16 = 59;

// ============================================================================
// ApiVersionsResponse Error Tests
// ============================================================================

/// Test ApiVersionsResponse with no error
#[test]
fn test_api_versions_response_no_error() {
    let mut response = ApiVersionsResponse::default();
    response.error_code = NONE;

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ApiVersionsResponse::decode(&mut read_buf, 0).unwrap();
    assert_eq!(decoded.error_code, NONE);
}

/// Test ApiVersionsResponse with unsupported version error
#[test]
fn test_api_versions_response_unsupported_version() {
    let mut response = ApiVersionsResponse::default();
    response.error_code = UNSUPPORTED_VERSION;

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ApiVersionsResponse::decode(&mut read_buf, 0).unwrap();
    assert_eq!(decoded.error_code, UNSUPPORTED_VERSION);
}

// ============================================================================
// MetadataResponse Error Tests
// ============================================================================

/// Test MetadataResponse with topic error
#[test]
fn test_metadata_response_topic_error() {
    use kafka_protocol::messages::metadata_response::MetadataResponseTopic;

    let mut response = MetadataResponse::default();
    let mut topic = MetadataResponseTopic::default();
    topic.error_code = UNKNOWN_TOPIC_OR_PARTITION;
    topic.name = Some(kafka_protocol::messages::TopicName(
        StrBytes::from_static_str("unknown-topic"),
    ));
    response.topics.push(topic);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = MetadataResponse::decode(&mut read_buf, 1).unwrap();
    assert_eq!(decoded.topics[0].error_code, UNKNOWN_TOPIC_OR_PARTITION);
}

/// Test MetadataResponse with invalid topic error
#[test]
fn test_metadata_response_invalid_topic() {
    use kafka_protocol::messages::metadata_response::MetadataResponseTopic;

    let mut response = MetadataResponse::default();
    let mut topic = MetadataResponseTopic::default();
    topic.error_code = INVALID_TOPIC_EXCEPTION;
    topic.name = Some(kafka_protocol::messages::TopicName(
        StrBytes::from_static_str("invalid..topic"),
    ));
    response.topics.push(topic);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = MetadataResponse::decode(&mut read_buf, 1).unwrap();
    assert_eq!(decoded.topics[0].error_code, INVALID_TOPIC_EXCEPTION);
}

/// Test MetadataResponse with authorization error
#[test]
fn test_metadata_response_authorization_failed() {
    use kafka_protocol::messages::metadata_response::MetadataResponseTopic;

    let mut response = MetadataResponse::default();
    let mut topic = MetadataResponseTopic::default();
    topic.error_code = TOPIC_AUTHORIZATION_FAILED;
    topic.name = Some(kafka_protocol::messages::TopicName(
        StrBytes::from_static_str("protected-topic"),
    ));
    response.topics.push(topic);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = MetadataResponse::decode(&mut read_buf, 1).unwrap();
    assert_eq!(decoded.topics[0].error_code, TOPIC_AUTHORIZATION_FAILED);
}

/// Test MetadataResponse with leader not available error
#[test]
fn test_metadata_response_leader_not_available() {
    use kafka_protocol::messages::metadata_response::{
        MetadataResponsePartition, MetadataResponseTopic,
    };

    let mut response = MetadataResponse::default();
    let mut topic = MetadataResponseTopic::default();
    topic.error_code = NONE;
    topic.name = Some(kafka_protocol::messages::TopicName(
        StrBytes::from_static_str("my-topic"),
    ));

    let mut partition = MetadataResponsePartition::default();
    partition.partition_index = 0;
    partition.error_code = LEADER_NOT_AVAILABLE;
    partition.leader_id = kafka_protocol::messages::BrokerId(-1);
    topic.partitions.push(partition);
    response.topics.push(topic);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = MetadataResponse::decode(&mut read_buf, 1).unwrap();
    assert_eq!(
        decoded.topics[0].partitions[0].error_code,
        LEADER_NOT_AVAILABLE
    );
}

// ============================================================================
// ProduceResponse Error Tests
// ============================================================================

/// Test ProduceResponse with no error
#[test]
fn test_produce_response_no_error() {
    use kafka_protocol::messages::produce_response::{
        PartitionProduceResponse, TopicProduceResponse,
    };

    let mut response = ProduceResponse::default();
    let mut topic = TopicProduceResponse::default();
    topic.name = kafka_protocol::messages::TopicName(StrBytes::from_static_str("my-topic"));

    let mut partition = PartitionProduceResponse::default();
    partition.index = 0;
    partition.error_code = NONE;
    partition.base_offset = 100;
    topic.partition_responses.push(partition);
    response.responses.push(topic);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 3).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ProduceResponse::decode(&mut read_buf, 3).unwrap();
    assert_eq!(decoded.responses[0].partition_responses[0].error_code, NONE);
}

/// Test ProduceResponse with message too large error
#[test]
fn test_produce_response_message_too_large() {
    use kafka_protocol::messages::produce_response::{
        PartitionProduceResponse, TopicProduceResponse,
    };

    let mut response = ProduceResponse::default();
    let mut topic = TopicProduceResponse::default();
    topic.name = kafka_protocol::messages::TopicName(StrBytes::from_static_str("my-topic"));

    let mut partition = PartitionProduceResponse::default();
    partition.index = 0;
    partition.error_code = MESSAGE_TOO_LARGE;
    partition.base_offset = -1;
    topic.partition_responses.push(partition);
    response.responses.push(topic);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 3).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ProduceResponse::decode(&mut read_buf, 3).unwrap();
    assert_eq!(
        decoded.responses[0].partition_responses[0].error_code,
        MESSAGE_TOO_LARGE
    );
}

/// Test ProduceResponse with invalid required acks error
#[test]
fn test_produce_response_invalid_required_acks() {
    use kafka_protocol::messages::produce_response::{
        PartitionProduceResponse, TopicProduceResponse,
    };

    let mut response = ProduceResponse::default();
    let mut topic = TopicProduceResponse::default();
    topic.name = kafka_protocol::messages::TopicName(StrBytes::from_static_str("my-topic"));

    let mut partition = PartitionProduceResponse::default();
    partition.index = 0;
    partition.error_code = INVALID_REQUIRED_ACKS;
    topic.partition_responses.push(partition);
    response.responses.push(topic);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 3).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ProduceResponse::decode(&mut read_buf, 3).unwrap();
    assert_eq!(
        decoded.responses[0].partition_responses[0].error_code,
        INVALID_REQUIRED_ACKS
    );
}

/// Test ProduceResponse with not enough replicas error
#[test]
fn test_produce_response_not_enough_replicas() {
    use kafka_protocol::messages::produce_response::{
        PartitionProduceResponse, TopicProduceResponse,
    };

    let mut response = ProduceResponse::default();
    let mut topic = TopicProduceResponse::default();
    topic.name = kafka_protocol::messages::TopicName(StrBytes::from_static_str("my-topic"));

    let mut partition = PartitionProduceResponse::default();
    partition.index = 0;
    partition.error_code = NOT_ENOUGH_REPLICAS;
    topic.partition_responses.push(partition);
    response.responses.push(topic);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 3).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ProduceResponse::decode(&mut read_buf, 3).unwrap();
    assert_eq!(
        decoded.responses[0].partition_responses[0].error_code,
        NOT_ENOUGH_REPLICAS
    );
}

/// Test ProduceResponse with corrupt message error
#[test]
fn test_produce_response_corrupt_message() {
    use kafka_protocol::messages::produce_response::{
        PartitionProduceResponse, TopicProduceResponse,
    };

    let mut response = ProduceResponse::default();
    let mut topic = TopicProduceResponse::default();
    topic.name = kafka_protocol::messages::TopicName(StrBytes::from_static_str("my-topic"));

    let mut partition = PartitionProduceResponse::default();
    partition.index = 0;
    partition.error_code = CORRUPT_MESSAGE;
    topic.partition_responses.push(partition);
    response.responses.push(topic);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 3).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ProduceResponse::decode(&mut read_buf, 3).unwrap();
    assert_eq!(
        decoded.responses[0].partition_responses[0].error_code,
        CORRUPT_MESSAGE
    );
}

// ============================================================================
// FetchResponse Error Tests
// ============================================================================

/// Test FetchResponse with offset out of range error
#[test]
fn test_fetch_response_offset_out_of_range() {
    use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};

    let mut response = FetchResponse::default();
    let mut topic = FetchableTopicResponse::default();
    topic.topic = kafka_protocol::messages::TopicName(StrBytes::from_static_str("my-topic"));

    let mut partition = PartitionData::default();
    partition.partition_index = 0;
    partition.error_code = OFFSET_OUT_OF_RANGE;
    topic.partitions.push(partition);
    response.responses.push(topic);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 4).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = FetchResponse::decode(&mut read_buf, 4).unwrap();
    assert_eq!(
        decoded.responses[0].partitions[0].error_code,
        OFFSET_OUT_OF_RANGE
    );
}

/// Test FetchResponse with not leader or follower error
#[test]
fn test_fetch_response_not_leader_or_follower() {
    use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};

    let mut response = FetchResponse::default();
    let mut topic = FetchableTopicResponse::default();
    topic.topic = kafka_protocol::messages::TopicName(StrBytes::from_static_str("my-topic"));

    let mut partition = PartitionData::default();
    partition.partition_index = 0;
    partition.error_code = NOT_LEADER_OR_FOLLOWER;
    topic.partitions.push(partition);
    response.responses.push(topic);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 4).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = FetchResponse::decode(&mut read_buf, 4).unwrap();
    assert_eq!(
        decoded.responses[0].partitions[0].error_code,
        NOT_LEADER_OR_FOLLOWER
    );
}

/// Test FetchResponse with unknown topic or partition error
#[test]
fn test_fetch_response_unknown_topic_or_partition() {
    use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};

    let mut response = FetchResponse::default();
    let mut topic = FetchableTopicResponse::default();
    topic.topic =
        kafka_protocol::messages::TopicName(StrBytes::from_static_str("nonexistent-topic"));

    let mut partition = PartitionData::default();
    partition.partition_index = 0;
    partition.error_code = UNKNOWN_TOPIC_OR_PARTITION;
    topic.partitions.push(partition);
    response.responses.push(topic);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 4).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = FetchResponse::decode(&mut read_buf, 4).unwrap();
    assert_eq!(
        decoded.responses[0].partitions[0].error_code,
        UNKNOWN_TOPIC_OR_PARTITION
    );
}

// ============================================================================
// ListOffsetsResponse Error Tests
// ============================================================================

/// Test ListOffsetsResponse with no error
#[test]
fn test_list_offsets_response_no_error() {
    use kafka_protocol::messages::list_offsets_response::{
        ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
    };

    let mut response = ListOffsetsResponse::default();
    let mut topic = ListOffsetsTopicResponse::default();
    topic.name = kafka_protocol::messages::TopicName(StrBytes::from_static_str("my-topic"));

    let mut partition = ListOffsetsPartitionResponse::default();
    partition.partition_index = 0;
    partition.error_code = NONE;
    partition.offset = 12345;
    topic.partitions.push(partition);
    response.topics.push(topic);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ListOffsetsResponse::decode(&mut read_buf, 1).unwrap();
    assert_eq!(decoded.topics[0].partitions[0].error_code, NONE);
}

/// Test ListOffsetsResponse with unknown topic or partition error
#[test]
fn test_list_offsets_response_unknown_topic() {
    use kafka_protocol::messages::list_offsets_response::{
        ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
    };

    let mut response = ListOffsetsResponse::default();
    let mut topic = ListOffsetsTopicResponse::default();
    topic.name = kafka_protocol::messages::TopicName(StrBytes::from_static_str("unknown-topic"));

    let mut partition = ListOffsetsPartitionResponse::default();
    partition.partition_index = 0;
    partition.error_code = UNKNOWN_TOPIC_OR_PARTITION;
    topic.partitions.push(partition);
    response.topics.push(topic);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ListOffsetsResponse::decode(&mut read_buf, 1).unwrap();
    assert_eq!(
        decoded.topics[0].partitions[0].error_code,
        UNKNOWN_TOPIC_OR_PARTITION
    );
}

// ============================================================================
// CreateTopicsResponse Error Tests
// ============================================================================

/// Test CreateTopicsResponse with no error
#[test]
fn test_create_topics_response_no_error() {
    use kafka_protocol::messages::create_topics_response::CreatableTopicResult;

    let mut response = CreateTopicsResponse::default();
    let mut topic = CreatableTopicResult::default();
    topic.name = kafka_protocol::messages::TopicName(StrBytes::from_static_str("new-topic"));
    topic.error_code = NONE;
    response.topics.push(topic);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 2).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = CreateTopicsResponse::decode(&mut read_buf, 2).unwrap();
    assert_eq!(decoded.topics[0].error_code, NONE);
}

/// Test CreateTopicsResponse with topic already exists error
#[test]
fn test_create_topics_response_topic_already_exists() {
    use kafka_protocol::messages::create_topics_response::CreatableTopicResult;

    let mut response = CreateTopicsResponse::default();
    let mut topic = CreatableTopicResult::default();
    topic.name = kafka_protocol::messages::TopicName(StrBytes::from_static_str("existing-topic"));
    topic.error_code = TOPIC_ALREADY_EXISTS;
    response.topics.push(topic);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 2).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = CreateTopicsResponse::decode(&mut read_buf, 2).unwrap();
    assert_eq!(decoded.topics[0].error_code, TOPIC_ALREADY_EXISTS);
}

/// Test CreateTopicsResponse with invalid partitions error
#[test]
fn test_create_topics_response_invalid_partitions() {
    use kafka_protocol::messages::create_topics_response::CreatableTopicResult;

    let mut response = CreateTopicsResponse::default();
    let mut topic = CreatableTopicResult::default();
    topic.name = kafka_protocol::messages::TopicName(StrBytes::from_static_str("bad-topic"));
    topic.error_code = INVALID_PARTITIONS;
    response.topics.push(topic);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 2).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = CreateTopicsResponse::decode(&mut read_buf, 2).unwrap();
    assert_eq!(decoded.topics[0].error_code, INVALID_PARTITIONS);
}

/// Test CreateTopicsResponse with invalid replication factor error
#[test]
fn test_create_topics_response_invalid_replication_factor() {
    use kafka_protocol::messages::create_topics_response::CreatableTopicResult;

    let mut response = CreateTopicsResponse::default();
    let mut topic = CreatableTopicResult::default();
    topic.name = kafka_protocol::messages::TopicName(StrBytes::from_static_str("bad-topic"));
    topic.error_code = INVALID_REPLICATION_FACTOR;
    response.topics.push(topic);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 2).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = CreateTopicsResponse::decode(&mut read_buf, 2).unwrap();
    assert_eq!(decoded.topics[0].error_code, INVALID_REPLICATION_FACTOR);
}

/// Test CreateTopicsResponse with invalid config error
#[test]
fn test_create_topics_response_invalid_config() {
    use kafka_protocol::messages::create_topics_response::CreatableTopicResult;

    let mut response = CreateTopicsResponse::default();
    let mut topic = CreatableTopicResult::default();
    topic.name = kafka_protocol::messages::TopicName(StrBytes::from_static_str("bad-config-topic"));
    topic.error_code = INVALID_CONFIG;
    response.topics.push(topic);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 2).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = CreateTopicsResponse::decode(&mut read_buf, 2).unwrap();
    assert_eq!(decoded.topics[0].error_code, INVALID_CONFIG);
}

// ============================================================================
// DeleteTopicsResponse Error Tests
// ============================================================================

/// Test DeleteTopicsResponse with no error
#[test]
fn test_delete_topics_response_no_error() {
    use kafka_protocol::messages::delete_topics_response::DeletableTopicResult;

    let mut response = DeleteTopicsResponse::default();
    let mut topic = DeletableTopicResult::default();
    topic.name = Some(kafka_protocol::messages::TopicName(
        StrBytes::from_static_str("deleted-topic"),
    ));
    topic.error_code = NONE;
    response.responses.push(topic);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DeleteTopicsResponse::decode(&mut read_buf, 1).unwrap();
    assert_eq!(decoded.responses[0].error_code, NONE);
}

/// Test DeleteTopicsResponse with unknown topic or partition error
#[test]
fn test_delete_topics_response_unknown_topic() {
    use kafka_protocol::messages::delete_topics_response::DeletableTopicResult;

    let mut response = DeleteTopicsResponse::default();
    let mut topic = DeletableTopicResult::default();
    topic.name = Some(kafka_protocol::messages::TopicName(
        StrBytes::from_static_str("nonexistent-topic"),
    ));
    topic.error_code = UNKNOWN_TOPIC_OR_PARTITION;
    response.responses.push(topic);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DeleteTopicsResponse::decode(&mut read_buf, 1).unwrap();
    assert_eq!(decoded.responses[0].error_code, UNKNOWN_TOPIC_OR_PARTITION);
}

/// Test DeleteTopicsResponse with topic authorization failed error
#[test]
fn test_delete_topics_response_authorization_failed() {
    use kafka_protocol::messages::delete_topics_response::DeletableTopicResult;

    let mut response = DeleteTopicsResponse::default();
    let mut topic = DeletableTopicResult::default();
    topic.name = Some(kafka_protocol::messages::TopicName(
        StrBytes::from_static_str("protected-topic"),
    ));
    topic.error_code = TOPIC_AUTHORIZATION_FAILED;
    response.responses.push(topic);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DeleteTopicsResponse::decode(&mut read_buf, 1).unwrap();
    assert_eq!(decoded.responses[0].error_code, TOPIC_AUTHORIZATION_FAILED);
}

// ============================================================================
// Multiple Error Tests
// ============================================================================

/// Test response with multiple topics having different errors
#[test]
fn test_multiple_topics_different_errors() {
    use kafka_protocol::messages::metadata_response::MetadataResponseTopic;

    let mut response = MetadataResponse::default();

    let mut topic1 = MetadataResponseTopic::default();
    topic1.error_code = NONE;
    topic1.name = Some(kafka_protocol::messages::TopicName(
        StrBytes::from_static_str("valid-topic"),
    ));
    response.topics.push(topic1);

    let mut topic2 = MetadataResponseTopic::default();
    topic2.error_code = UNKNOWN_TOPIC_OR_PARTITION;
    topic2.name = Some(kafka_protocol::messages::TopicName(
        StrBytes::from_static_str("unknown-topic"),
    ));
    response.topics.push(topic2);

    let mut topic3 = MetadataResponseTopic::default();
    topic3.error_code = TOPIC_AUTHORIZATION_FAILED;
    topic3.name = Some(kafka_protocol::messages::TopicName(
        StrBytes::from_static_str("protected-topic"),
    ));
    response.topics.push(topic3);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = MetadataResponse::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.topics[0].error_code, NONE);
    assert_eq!(decoded.topics[1].error_code, UNKNOWN_TOPIC_OR_PARTITION);
    assert_eq!(decoded.topics[2].error_code, TOPIC_AUTHORIZATION_FAILED);
}

/// Test response with multiple partitions having different errors
#[test]
fn test_multiple_partitions_different_errors() {
    use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};

    let mut response = FetchResponse::default();
    let mut topic = FetchableTopicResponse::default();
    topic.topic = kafka_protocol::messages::TopicName(StrBytes::from_static_str("my-topic"));

    let mut partition0 = PartitionData::default();
    partition0.partition_index = 0;
    partition0.error_code = NONE;
    topic.partitions.push(partition0);

    let mut partition1 = PartitionData::default();
    partition1.partition_index = 1;
    partition1.error_code = OFFSET_OUT_OF_RANGE;
    topic.partitions.push(partition1);

    let mut partition2 = PartitionData::default();
    partition2.partition_index = 2;
    partition2.error_code = NOT_LEADER_OR_FOLLOWER;
    topic.partitions.push(partition2);

    response.responses.push(topic);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 4).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = FetchResponse::decode(&mut read_buf, 4).unwrap();

    assert_eq!(decoded.responses[0].partitions[0].error_code, NONE);
    assert_eq!(
        decoded.responses[0].partitions[1].error_code,
        OFFSET_OUT_OF_RANGE
    );
    assert_eq!(
        decoded.responses[0].partitions[2].error_code,
        NOT_LEADER_OR_FOLLOWER
    );
}

// ============================================================================
// Error Code Boundary Tests
// ============================================================================

/// Test all documented retriable error codes
#[test]
fn test_retriable_error_codes() {
    use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};

    let retriable_codes = vec![
        OFFSET_OUT_OF_RANGE,
        CORRUPT_MESSAGE,
        UNKNOWN_TOPIC_OR_PARTITION,
        LEADER_NOT_AVAILABLE,
        NOT_LEADER_OR_FOLLOWER,
        REQUEST_TIMED_OUT,
        BROKER_NOT_AVAILABLE,
        REPLICA_NOT_AVAILABLE,
        NOT_ENOUGH_REPLICAS,
        NOT_ENOUGH_REPLICAS_AFTER_APPEND,
        STALE_CONTROLLER_EPOCH,
        NOT_CONTROLLER,
        KAFKA_STORAGE_ERROR,
    ];

    for error_code in retriable_codes {
        let mut response = FetchResponse::default();
        let mut topic = FetchableTopicResponse::default();
        topic.topic = kafka_protocol::messages::TopicName(StrBytes::from_static_str("test-topic"));

        let mut partition = PartitionData::default();
        partition.partition_index = 0;
        partition.error_code = error_code;
        topic.partitions.push(partition);
        response.responses.push(topic);

        let mut buf = BytesMut::new();
        response.encode(&mut buf, 4).unwrap();

        let mut read_buf = buf.freeze();
        let decoded = FetchResponse::decode(&mut read_buf, 4).unwrap();
        assert_eq!(
            decoded.responses[0].partitions[0].error_code, error_code,
            "Failed for error code {}",
            error_code
        );
    }
}

/// Test error code with throttle time
#[test]
fn test_error_with_throttle_time() {
    use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};

    let mut response = FetchResponse::default();
    response.throttle_time_ms = 1000;

    let mut topic = FetchableTopicResponse::default();
    topic.topic = kafka_protocol::messages::TopicName(StrBytes::from_static_str("my-topic"));

    let mut partition = PartitionData::default();
    partition.partition_index = 0;
    partition.error_code = REQUEST_TIMED_OUT;
    topic.partitions.push(partition);
    response.responses.push(topic);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 4).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = FetchResponse::decode(&mut read_buf, 4).unwrap();

    assert_eq!(decoded.throttle_time_ms, 1000);
    assert_eq!(
        decoded.responses[0].partitions[0].error_code,
        REQUEST_TIMED_OUT
    );
}

/// Test negative error code values (should not exist but test encoding)
#[test]
fn test_negative_error_code() {
    let mut response = ApiVersionsResponse::default();
    response.error_code = -1; // Invalid but should still encode/decode

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ApiVersionsResponse::decode(&mut read_buf, 0).unwrap();
    assert_eq!(decoded.error_code, -1);
}

/// Test high error code value
#[test]
fn test_high_error_code() {
    let mut response = ApiVersionsResponse::default();
    response.error_code = 100; // Future error code

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ApiVersionsResponse::decode(&mut read_buf, 0).unwrap();
    assert_eq!(decoded.error_code, 100);
}

// Suppress warnings for unused constants that are kept for documentation purposes
#[allow(dead_code)]
const _UNUSED_ERRORS: &[i16] = &[
    INVALID_FETCH_SIZE,
    OFFSET_METADATA_TOO_LARGE,
    NETWORK_EXCEPTION,
    COORDINATOR_LOAD_IN_PROGRESS,
    COORDINATOR_NOT_AVAILABLE,
    NOT_COORDINATOR,
    RECORD_LIST_TOO_LARGE,
    ILLEGAL_GENERATION,
    INCONSISTENT_GROUP_PROTOCOL,
    INVALID_GROUP_ID,
    UNKNOWN_MEMBER_ID,
    INVALID_SESSION_TIMEOUT,
    REBALANCE_IN_PROGRESS,
    INVALID_COMMIT_OFFSET_SIZE,
    GROUP_AUTHORIZATION_FAILED,
    CLUSTER_AUTHORIZATION_FAILED,
    INVALID_TIMESTAMP,
    UNSUPPORTED_SASL_MECHANISM,
    ILLEGAL_SASL_STATE,
    INVALID_REPLICA_ASSIGNMENT,
    SASL_AUTHENTICATION_FAILED,
    UNKNOWN_PRODUCER_ID,
];
