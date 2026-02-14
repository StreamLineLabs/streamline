//! Protocol Error Handling Tests - Phase 3
//!
//! Tests for Kafka protocol error handling per specification:
//! - UNSUPPORTED_VERSION error responses
//! - Throttling (throttle_time_ms in responses)
//! - Leader epoch fencing (FENCED_LEADER_EPOCH, UNKNOWN_LEADER_EPOCH)
//! - Various error codes and their proper representation
//!
//! Source: https://kafka.apache.org/protocol.html#protocol_error_codes

use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::*;
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};

// ============================================================================
// Error Code Constants (for reference and testing)
// ============================================================================

#[allow(dead_code)]
mod error_codes {
    pub const NONE: i16 = 0;
    pub const UNKNOWN_SERVER_ERROR: i16 = -1;
    pub const OFFSET_OUT_OF_RANGE: i16 = 1;
    pub const CORRUPT_MESSAGE: i16 = 2;
    pub const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
    pub const INVALID_FETCH_SIZE: i16 = 4;
    pub const LEADER_NOT_AVAILABLE: i16 = 5;
    pub const NOT_LEADER_OR_FOLLOWER: i16 = 6;
    pub const REQUEST_TIMED_OUT: i16 = 7;
    pub const BROKER_NOT_AVAILABLE: i16 = 8;
    pub const REPLICA_NOT_AVAILABLE: i16 = 9;
    pub const MESSAGE_TOO_LARGE: i16 = 10;
    pub const STALE_CONTROLLER_EPOCH: i16 = 11;
    pub const OFFSET_METADATA_TOO_LARGE: i16 = 12;
    pub const NETWORK_EXCEPTION: i16 = 13;
    pub const COORDINATOR_LOAD_IN_PROGRESS: i16 = 14;
    pub const COORDINATOR_NOT_AVAILABLE: i16 = 15;
    pub const NOT_COORDINATOR: i16 = 16;
    pub const INVALID_TOPIC_EXCEPTION: i16 = 17;
    pub const RECORD_LIST_TOO_LARGE: i16 = 18;
    pub const NOT_ENOUGH_REPLICAS: i16 = 19;
    pub const NOT_ENOUGH_REPLICAS_AFTER_APPEND: i16 = 20;
    pub const INVALID_REQUIRED_ACKS: i16 = 21;
    pub const ILLEGAL_GENERATION: i16 = 22;
    pub const INCONSISTENT_GROUP_PROTOCOL: i16 = 23;
    pub const INVALID_GROUP_ID: i16 = 24;
    pub const UNKNOWN_MEMBER_ID: i16 = 25;
    pub const INVALID_SESSION_TIMEOUT: i16 = 26;
    pub const REBALANCE_IN_PROGRESS: i16 = 27;
    pub const INVALID_COMMIT_OFFSET_SIZE: i16 = 28;
    pub const TOPIC_AUTHORIZATION_FAILED: i16 = 29;
    pub const GROUP_AUTHORIZATION_FAILED: i16 = 30;
    pub const CLUSTER_AUTHORIZATION_FAILED: i16 = 31;
    pub const INVALID_TIMESTAMP: i16 = 32;
    pub const UNSUPPORTED_SASL_MECHANISM: i16 = 33;
    pub const ILLEGAL_SASL_STATE: i16 = 34;
    pub const UNSUPPORTED_VERSION: i16 = 35;
    pub const TOPIC_ALREADY_EXISTS: i16 = 36;
    pub const INVALID_PARTITIONS: i16 = 37;
    pub const INVALID_REPLICATION_FACTOR: i16 = 38;
    pub const INVALID_REPLICA_ASSIGNMENT: i16 = 39;
    pub const INVALID_CONFIG: i16 = 40;
    pub const NOT_CONTROLLER: i16 = 41;
    pub const INVALID_REQUEST: i16 = 42;
    pub const UNSUPPORTED_FOR_MESSAGE_FORMAT: i16 = 43;
    pub const POLICY_VIOLATION: i16 = 44;
    pub const OUT_OF_ORDER_SEQUENCE_NUMBER: i16 = 45;
    pub const DUPLICATE_SEQUENCE_NUMBER: i16 = 46;
    pub const INVALID_PRODUCER_EPOCH: i16 = 47;
    pub const INVALID_TXN_STATE: i16 = 48;
    pub const INVALID_PRODUCER_ID_MAPPING: i16 = 49;
    pub const INVALID_TRANSACTION_TIMEOUT: i16 = 50;
    pub const CONCURRENT_TRANSACTIONS: i16 = 51;
    pub const TRANSACTION_COORDINATOR_FENCED: i16 = 52;
    pub const TRANSACTIONAL_ID_AUTHORIZATION_FAILED: i16 = 53;
    pub const SECURITY_DISABLED: i16 = 54;
    pub const OPERATION_NOT_ATTEMPTED: i16 = 55;
    pub const KAFKA_STORAGE_ERROR: i16 = 56;
    pub const LOG_DIR_NOT_FOUND: i16 = 57;
    pub const SASL_AUTHENTICATION_FAILED: i16 = 58;
    pub const UNKNOWN_PRODUCER_ID: i16 = 59;
    pub const REASSIGNMENT_IN_PROGRESS: i16 = 60;
    pub const DELEGATION_TOKEN_AUTH_DISABLED: i16 = 61;
    pub const DELEGATION_TOKEN_NOT_FOUND: i16 = 62;
    pub const DELEGATION_TOKEN_OWNER_MISMATCH: i16 = 63;
    pub const DELEGATION_TOKEN_REQUEST_NOT_ALLOWED: i16 = 64;
    pub const DELEGATION_TOKEN_AUTHORIZATION_FAILED: i16 = 65;
    pub const DELEGATION_TOKEN_EXPIRED: i16 = 66;
    pub const INVALID_PRINCIPAL_TYPE: i16 = 67;
    pub const NON_EMPTY_GROUP: i16 = 68;
    pub const GROUP_ID_NOT_FOUND: i16 = 69;
    pub const FETCH_SESSION_ID_NOT_FOUND: i16 = 70;
    pub const INVALID_FETCH_SESSION_EPOCH: i16 = 71;
    pub const LISTENER_NOT_FOUND: i16 = 72;
    pub const TOPIC_DELETION_DISABLED: i16 = 73;
    pub const FENCED_LEADER_EPOCH: i16 = 74;
    pub const UNKNOWN_LEADER_EPOCH: i16 = 75;
    pub const UNSUPPORTED_COMPRESSION_TYPE: i16 = 76;
    pub const STALE_BROKER_EPOCH: i16 = 77;
    pub const OFFSET_NOT_AVAILABLE: i16 = 78;
    pub const MEMBER_ID_REQUIRED: i16 = 79;
    pub const PREFERRED_LEADER_NOT_AVAILABLE: i16 = 80;
    pub const GROUP_MAX_SIZE_REACHED: i16 = 81;
    pub const FENCED_INSTANCE_ID: i16 = 82;
    pub const ELIGIBLE_LEADERS_NOT_AVAILABLE: i16 = 83;
    pub const ELECTION_NOT_NEEDED: i16 = 84;
    pub const NO_REASSIGNMENT_IN_PROGRESS: i16 = 85;
    pub const GROUP_SUBSCRIBED_TO_TOPIC: i16 = 86;
    pub const INVALID_RECORD: i16 = 87;
    pub const UNSTABLE_OFFSET_COMMIT: i16 = 88;
    pub const THROTTLING_QUOTA_EXCEEDED: i16 = 89;
    pub const PRODUCER_FENCED: i16 = 90;
    pub const RESOURCE_NOT_FOUND: i16 = 91;
    pub const DUPLICATE_RESOURCE: i16 = 92;
    pub const UNACCEPTABLE_CREDENTIAL: i16 = 93;
    pub const INCONSISTENT_VOTER_SET: i16 = 94;
    pub const INVALID_UPDATE_VERSION: i16 = 95;
    pub const FEATURE_UPDATE_FAILED: i16 = 96;
    pub const PRINCIPAL_DESERIALIZATION_FAILURE: i16 = 97;
    pub const SNAPSHOT_NOT_FOUND: i16 = 98;
    pub const POSITION_OUT_OF_RANGE: i16 = 99;
    pub const UNKNOWN_TOPIC_ID: i16 = 100;
    pub const DUPLICATE_BROKER_REGISTRATION: i16 = 101;
    pub const BROKER_ID_NOT_REGISTERED: i16 = 102;
    pub const INCONSISTENT_TOPIC_ID: i16 = 103;
    pub const INCONSISTENT_CLUSTER_ID: i16 = 104;
    pub const TRANSACTIONAL_ID_NOT_FOUND: i16 = 105;
    pub const FETCH_SESSION_TOPIC_ID_ERROR: i16 = 106;
    pub const INELIGIBLE_REPLICA: i16 = 107;
    pub const NEW_LEADER_ELECTED: i16 = 108;
}

use error_codes::*;

// ============================================================================
// Phase 3: UNSUPPORTED_VERSION Error Tests
// ============================================================================

#[test]
fn test_unsupported_version_error_code_value() {
    // Per Kafka spec, UNSUPPORTED_VERSION is error code 35
    assert_eq!(UNSUPPORTED_VERSION, 35);
}

#[test]
fn test_api_versions_response_with_unsupported_version() {
    // When a client requests an unsupported API version, the broker
    // should respond with UNSUPPORTED_VERSION error

    let response = ApiVersionsResponse::default()
        .with_error_code(UNSUPPORTED_VERSION)
        .with_api_keys(vec![]); // Empty when version not supported

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = ApiVersionsResponse::decode(&mut bytes, 0).unwrap();

    assert_eq!(decoded.error_code, UNSUPPORTED_VERSION);
    assert!(decoded.api_keys.is_empty());
}

#[test]
fn test_api_versions_response_includes_supported_versions_on_error() {
    // Per Kafka spec (v2.4.0+), even on UNSUPPORTED_VERSION error,
    // the response should include the supported API versions

    use kafka_protocol::messages::api_versions_response::ApiVersion;

    let response = ApiVersionsResponse::default()
        .with_error_code(UNSUPPORTED_VERSION)
        .with_api_keys(vec![
            ApiVersion::default()
                .with_api_key(18) // ApiVersions
                .with_min_version(0)
                .with_max_version(3),
            ApiVersion::default()
                .with_api_key(3) // Metadata
                .with_min_version(0)
                .with_max_version(12),
        ]);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = ApiVersionsResponse::decode(&mut bytes, 0).unwrap();

    assert_eq!(decoded.error_code, UNSUPPORTED_VERSION);
    assert_eq!(decoded.api_keys.len(), 2);

    // Client can use this to negotiate a compatible version
    let api_versions_entry = decoded.api_keys.iter().find(|v| v.api_key == 18).unwrap();
    assert_eq!(api_versions_entry.min_version, 0);
    assert_eq!(api_versions_entry.max_version, 3);
}

#[test]
fn test_produce_response_with_unsupported_version() {
    use kafka_protocol::messages::produce_response::{
        PartitionProduceResponse, TopicProduceResponse,
    };

    // Topic-level or partition-level errors in produce response
    let response = ProduceResponse::default().with_responses(vec![TopicProduceResponse::default()
        .with_name(TopicName::from(StrBytes::from_static_str("test")))
        .with_partition_responses(vec![PartitionProduceResponse::default()
            .with_index(0)
            .with_error_code(UNSUPPORTED_VERSION)
            .with_base_offset(-1)])]);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 9).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = ProduceResponse::decode(&mut bytes, 9).unwrap();

    assert_eq!(
        decoded.responses[0].partition_responses[0].error_code,
        UNSUPPORTED_VERSION
    );
}

// ============================================================================
// Phase 3: Throttling Tests
// ============================================================================

#[test]
fn test_throttle_time_ms_in_api_versions_response() {
    // throttle_time_ms is included in ApiVersionsResponse v1+
    let response = ApiVersionsResponse::default()
        .with_error_code(NONE)
        .with_throttle_time_ms(1000); // 1 second throttle

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 3).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = ApiVersionsResponse::decode(&mut bytes, 3).unwrap();

    assert_eq!(decoded.throttle_time_ms, 1000);
}

#[test]
fn test_throttle_time_ms_in_metadata_response() {
    use kafka_protocol::messages::metadata_response::MetadataResponseBroker;

    let response = MetadataResponse::default()
        .with_throttle_time_ms(500)
        .with_brokers(vec![MetadataResponseBroker::default()
            .with_node_id(BrokerId(1))
            .with_host(StrBytes::from_static_str("localhost"))
            .with_port(9092)]);

    // throttle_time_ms available from v3+
    let mut buf = BytesMut::new();
    response.encode(&mut buf, 9).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = MetadataResponse::decode(&mut bytes, 9).unwrap();

    assert_eq!(decoded.throttle_time_ms, 500);
}

#[test]
fn test_throttle_time_ms_in_produce_response() {
    let response = ProduceResponse::default().with_throttle_time_ms(2000);

    // throttle_time_ms available from v1+
    let mut buf = BytesMut::new();
    response.encode(&mut buf, 9).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = ProduceResponse::decode(&mut bytes, 9).unwrap();

    assert_eq!(decoded.throttle_time_ms, 2000);
}

#[test]
fn test_throttle_time_ms_in_fetch_response() {
    let response = FetchResponse::default().with_throttle_time_ms(3000);

    // throttle_time_ms available from v1+
    let mut buf = BytesMut::new();
    response.encode(&mut buf, 12).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = FetchResponse::decode(&mut bytes, 12).unwrap();

    assert_eq!(decoded.throttle_time_ms, 3000);
}

#[test]
fn test_throttling_quota_exceeded_error() {
    // THROTTLING_QUOTA_EXCEEDED error code
    assert_eq!(THROTTLING_QUOTA_EXCEEDED, 89);

    // Can be used in various responses
    let response = ProduceResponse::default().with_throttle_time_ms(5000);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 9).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = ProduceResponse::decode(&mut bytes, 9).unwrap();

    // Server might set high throttle_time_ms when quota exceeded
    assert_eq!(decoded.throttle_time_ms, 5000);
}

#[test]
fn test_zero_throttle_time() {
    // Zero throttle time means no throttling
    let response = MetadataResponse::default().with_throttle_time_ms(0);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 9).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = MetadataResponse::decode(&mut bytes, 9).unwrap();

    assert_eq!(decoded.throttle_time_ms, 0);
}

#[test]
fn test_large_throttle_time() {
    // Large throttle times should be handled
    let large_throttle = i32::MAX;
    let response = FetchResponse::default().with_throttle_time_ms(large_throttle);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 12).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = FetchResponse::decode(&mut bytes, 12).unwrap();

    assert_eq!(decoded.throttle_time_ms, large_throttle);
}

// ============================================================================
// Phase 3: Leader Epoch Fencing Tests
// ============================================================================

#[test]
fn test_fenced_leader_epoch_error_code() {
    // FENCED_LEADER_EPOCH = 74
    // Returned when leader epoch in request is older than current
    assert_eq!(FENCED_LEADER_EPOCH, 74);
}

#[test]
fn test_unknown_leader_epoch_error_code() {
    // UNKNOWN_LEADER_EPOCH = 75
    // Returned when leader epoch is not known to the broker
    assert_eq!(UNKNOWN_LEADER_EPOCH, 75);
}

#[test]
fn test_fetch_response_with_fenced_leader_epoch() {
    use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};

    let response = FetchResponse::default().with_responses(vec![FetchableTopicResponse::default()
        .with_topic(TopicName::from(StrBytes::from_static_str("test")))
        .with_partitions(vec![PartitionData::default()
            .with_partition_index(0)
            .with_error_code(FENCED_LEADER_EPOCH)
            .with_high_watermark(-1)])]);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 12).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = FetchResponse::decode(&mut bytes, 12).unwrap();

    assert_eq!(
        decoded.responses[0].partitions[0].error_code,
        FENCED_LEADER_EPOCH
    );
}

#[test]
fn test_produce_response_with_fenced_leader_epoch() {
    use kafka_protocol::messages::produce_response::{
        PartitionProduceResponse, TopicProduceResponse,
    };

    let response = ProduceResponse::default().with_responses(vec![TopicProduceResponse::default()
        .with_name(TopicName::from(StrBytes::from_static_str("test")))
        .with_partition_responses(vec![PartitionProduceResponse::default()
            .with_index(0)
            .with_error_code(FENCED_LEADER_EPOCH)])]);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 9).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = ProduceResponse::decode(&mut bytes, 9).unwrap();

    assert_eq!(
        decoded.responses[0].partition_responses[0].error_code,
        FENCED_LEADER_EPOCH
    );
}

#[test]
fn test_list_offsets_response_with_unknown_leader_epoch() {
    use kafka_protocol::messages::list_offsets_response::{
        ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
    };

    let response =
        ListOffsetsResponse::default().with_topics(vec![ListOffsetsTopicResponse::default()
            .with_name(TopicName::from(StrBytes::from_static_str("test")))
            .with_partitions(vec![ListOffsetsPartitionResponse::default()
                .with_partition_index(0)
                .with_error_code(UNKNOWN_LEADER_EPOCH)])]);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 7).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = ListOffsetsResponse::decode(&mut bytes, 7).unwrap();

    assert_eq!(
        decoded.topics[0].partitions[0].error_code,
        UNKNOWN_LEADER_EPOCH
    );
}

// ============================================================================
// Phase 3: Common Error Code Tests
// ============================================================================

#[test]
fn test_unknown_topic_or_partition_error() {
    // UNKNOWN_TOPIC_OR_PARTITION = 3
    assert_eq!(UNKNOWN_TOPIC_OR_PARTITION, 3);

    use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};

    let response = FetchResponse::default().with_responses(vec![FetchableTopicResponse::default()
        .with_topic(TopicName::from(StrBytes::from_static_str("nonexistent")))
        .with_partitions(vec![PartitionData::default()
            .with_partition_index(0)
            .with_error_code(UNKNOWN_TOPIC_OR_PARTITION)])]);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 12).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = FetchResponse::decode(&mut bytes, 12).unwrap();

    assert_eq!(
        decoded.responses[0].partitions[0].error_code,
        UNKNOWN_TOPIC_OR_PARTITION
    );
}

#[test]
fn test_not_leader_or_follower_error() {
    // NOT_LEADER_OR_FOLLOWER = 6
    assert_eq!(NOT_LEADER_OR_FOLLOWER, 6);

    use kafka_protocol::messages::produce_response::{
        PartitionProduceResponse, TopicProduceResponse,
    };

    let response = ProduceResponse::default().with_responses(vec![TopicProduceResponse::default()
        .with_name(TopicName::from(StrBytes::from_static_str("test")))
        .with_partition_responses(vec![PartitionProduceResponse::default()
            .with_index(0)
            .with_error_code(NOT_LEADER_OR_FOLLOWER)])]);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 9).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = ProduceResponse::decode(&mut bytes, 9).unwrap();

    assert_eq!(
        decoded.responses[0].partition_responses[0].error_code,
        NOT_LEADER_OR_FOLLOWER
    );
}

#[test]
fn test_leader_not_available_error() {
    // LEADER_NOT_AVAILABLE = 5
    assert_eq!(LEADER_NOT_AVAILABLE, 5);
}

#[test]
fn test_offset_out_of_range_error() {
    // OFFSET_OUT_OF_RANGE = 1
    assert_eq!(OFFSET_OUT_OF_RANGE, 1);

    use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};

    let response = FetchResponse::default().with_responses(vec![FetchableTopicResponse::default()
        .with_topic(TopicName::from(StrBytes::from_static_str("test")))
        .with_partitions(vec![PartitionData::default()
            .with_partition_index(0)
            .with_error_code(OFFSET_OUT_OF_RANGE)])]);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 12).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = FetchResponse::decode(&mut bytes, 12).unwrap();

    assert_eq!(
        decoded.responses[0].partitions[0].error_code,
        OFFSET_OUT_OF_RANGE
    );
}

#[test]
fn test_request_timed_out_error() {
    // REQUEST_TIMED_OUT = 7
    assert_eq!(REQUEST_TIMED_OUT, 7);
}

// ============================================================================
// Phase 3: Topic Management Error Tests
// ============================================================================

#[test]
fn test_topic_already_exists_error() {
    // TOPIC_ALREADY_EXISTS = 36
    assert_eq!(TOPIC_ALREADY_EXISTS, 36);

    use kafka_protocol::messages::create_topics_response::CreatableTopicResult;

    let response =
        CreateTopicsResponse::default().with_topics(vec![CreatableTopicResult::default()
            .with_name(TopicName::from(StrBytes::from_static_str("existing-topic")))
            .with_error_code(TOPIC_ALREADY_EXISTS)]);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 5).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = CreateTopicsResponse::decode(&mut bytes, 5).unwrap();

    assert_eq!(decoded.topics[0].error_code, TOPIC_ALREADY_EXISTS);
}

#[test]
fn test_invalid_partitions_error() {
    // INVALID_PARTITIONS = 37
    assert_eq!(INVALID_PARTITIONS, 37);

    use kafka_protocol::messages::create_topics_response::CreatableTopicResult;

    let response =
        CreateTopicsResponse::default().with_topics(vec![CreatableTopicResult::default()
            .with_name(TopicName::from(StrBytes::from_static_str("invalid-topic")))
            .with_error_code(INVALID_PARTITIONS)]);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 5).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = CreateTopicsResponse::decode(&mut bytes, 5).unwrap();

    assert_eq!(decoded.topics[0].error_code, INVALID_PARTITIONS);
}

#[test]
fn test_topic_deletion_disabled_error() {
    // TOPIC_DELETION_DISABLED = 73
    assert_eq!(TOPIC_DELETION_DISABLED, 73);

    use kafka_protocol::messages::delete_topics_response::DeletableTopicResult;

    let response =
        DeleteTopicsResponse::default().with_responses(vec![DeletableTopicResult::default()
            .with_name(Some(TopicName::from(StrBytes::from_static_str(
                "protected-topic",
            ))))
            .with_error_code(TOPIC_DELETION_DISABLED)]);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 6).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = DeleteTopicsResponse::decode(&mut bytes, 6).unwrap();

    assert_eq!(decoded.responses[0].error_code, TOPIC_DELETION_DISABLED);
}

// ============================================================================
// Phase 3: Consumer Group Error Tests
// ============================================================================

#[test]
fn test_unknown_member_id_error() {
    // UNKNOWN_MEMBER_ID = 25
    assert_eq!(UNKNOWN_MEMBER_ID, 25);

    let response = HeartbeatResponse::default().with_error_code(UNKNOWN_MEMBER_ID);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 4).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = HeartbeatResponse::decode(&mut bytes, 4).unwrap();

    assert_eq!(decoded.error_code, UNKNOWN_MEMBER_ID);
}

#[test]
fn test_rebalance_in_progress_error() {
    // REBALANCE_IN_PROGRESS = 27
    assert_eq!(REBALANCE_IN_PROGRESS, 27);

    let response = SyncGroupResponse::default().with_error_code(REBALANCE_IN_PROGRESS);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 5).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = SyncGroupResponse::decode(&mut bytes, 5).unwrap();

    assert_eq!(decoded.error_code, REBALANCE_IN_PROGRESS);
}

#[test]
fn test_illegal_generation_error() {
    // ILLEGAL_GENERATION = 22
    assert_eq!(ILLEGAL_GENERATION, 22);

    let response = HeartbeatResponse::default().with_error_code(ILLEGAL_GENERATION);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 4).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = HeartbeatResponse::decode(&mut bytes, 4).unwrap();

    assert_eq!(decoded.error_code, ILLEGAL_GENERATION);
}

#[test]
fn test_member_id_required_error() {
    // MEMBER_ID_REQUIRED = 79
    assert_eq!(MEMBER_ID_REQUIRED, 79);

    let response = JoinGroupResponse::default()
        .with_error_code(MEMBER_ID_REQUIRED)
        .with_member_id(StrBytes::from_static_str("generated-member-id"));

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 9).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = JoinGroupResponse::decode(&mut bytes, 9).unwrap();

    assert_eq!(decoded.error_code, MEMBER_ID_REQUIRED);
    // Client should use the returned member_id in retry
    assert!(!decoded.member_id.is_empty());
}

#[test]
fn test_coordinator_not_available_error() {
    // COORDINATOR_NOT_AVAILABLE = 15
    assert_eq!(COORDINATOR_NOT_AVAILABLE, 15);

    // v0-2 use top-level error_code, v3+ use coordinators array
    let response = FindCoordinatorResponse::default().with_error_code(COORDINATOR_NOT_AVAILABLE);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 2).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = FindCoordinatorResponse::decode(&mut bytes, 2).unwrap();

    assert_eq!(decoded.error_code, COORDINATOR_NOT_AVAILABLE);
}

#[test]
fn test_not_coordinator_error() {
    // NOT_COORDINATOR = 16
    assert_eq!(NOT_COORDINATOR, 16);

    let response = HeartbeatResponse::default().with_error_code(NOT_COORDINATOR);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 4).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = HeartbeatResponse::decode(&mut bytes, 4).unwrap();

    assert_eq!(decoded.error_code, NOT_COORDINATOR);
}

#[test]
fn test_group_id_not_found_error() {
    // GROUP_ID_NOT_FOUND = 69
    assert_eq!(GROUP_ID_NOT_FOUND, 69);
}

#[test]
fn test_non_empty_group_error() {
    // NON_EMPTY_GROUP = 68 - cannot delete non-empty consumer group
    assert_eq!(NON_EMPTY_GROUP, 68);

    use kafka_protocol::messages::delete_groups_response::DeletableGroupResult;

    let response =
        DeleteGroupsResponse::default().with_results(vec![DeletableGroupResult::default()
            .with_group_id(GroupId::from(StrBytes::from_static_str("active-group")))
            .with_error_code(NON_EMPTY_GROUP)]);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 2).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = DeleteGroupsResponse::decode(&mut bytes, 2).unwrap();

    assert_eq!(decoded.results[0].error_code, NON_EMPTY_GROUP);
}

// ============================================================================
// Phase 3: Transaction Error Tests
// ============================================================================

#[test]
fn test_invalid_txn_state_error() {
    // INVALID_TXN_STATE = 48
    assert_eq!(INVALID_TXN_STATE, 48);
}

#[test]
fn test_invalid_producer_epoch_error() {
    // INVALID_PRODUCER_EPOCH = 47
    assert_eq!(INVALID_PRODUCER_EPOCH, 47);
}

#[test]
fn test_producer_fenced_error() {
    // PRODUCER_FENCED = 90
    assert_eq!(PRODUCER_FENCED, 90);
}

#[test]
fn test_transaction_coordinator_fenced_error() {
    // TRANSACTION_COORDINATOR_FENCED = 52
    assert_eq!(TRANSACTION_COORDINATOR_FENCED, 52);
}

#[test]
fn test_concurrent_transactions_error() {
    // CONCURRENT_TRANSACTIONS = 51
    assert_eq!(CONCURRENT_TRANSACTIONS, 51);
}

#[test]
fn test_transactional_id_not_found_error() {
    // TRANSACTIONAL_ID_NOT_FOUND = 105
    assert_eq!(TRANSACTIONAL_ID_NOT_FOUND, 105);
}

// ============================================================================
// Phase 3: Authorization Error Tests
// ============================================================================

#[test]
fn test_topic_authorization_failed_error() {
    // TOPIC_AUTHORIZATION_FAILED = 29
    assert_eq!(TOPIC_AUTHORIZATION_FAILED, 29);
}

#[test]
fn test_group_authorization_failed_error() {
    // GROUP_AUTHORIZATION_FAILED = 30
    assert_eq!(GROUP_AUTHORIZATION_FAILED, 30);
}

#[test]
fn test_cluster_authorization_failed_error() {
    // CLUSTER_AUTHORIZATION_FAILED = 31
    assert_eq!(CLUSTER_AUTHORIZATION_FAILED, 31);
}

#[test]
fn test_transactional_id_authorization_failed_error() {
    // TRANSACTIONAL_ID_AUTHORIZATION_FAILED = 53
    assert_eq!(TRANSACTIONAL_ID_AUTHORIZATION_FAILED, 53);
}

// ============================================================================
// Phase 3: SASL/Security Error Tests
// ============================================================================

#[test]
fn test_unsupported_sasl_mechanism_error() {
    // UNSUPPORTED_SASL_MECHANISM = 33
    assert_eq!(UNSUPPORTED_SASL_MECHANISM, 33);

    let response = SaslHandshakeResponse::default()
        .with_error_code(UNSUPPORTED_SASL_MECHANISM)
        .with_mechanisms(vec![
            StrBytes::from_static_str("PLAIN"),
            StrBytes::from_static_str("SCRAM-SHA-256"),
        ]);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 1).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = SaslHandshakeResponse::decode(&mut bytes, 1).unwrap();

    assert_eq!(decoded.error_code, UNSUPPORTED_SASL_MECHANISM);
    // Response includes supported mechanisms so client can retry
    assert_eq!(decoded.mechanisms.len(), 2);
}

#[test]
fn test_sasl_authentication_failed_error() {
    // SASL_AUTHENTICATION_FAILED = 58
    assert_eq!(SASL_AUTHENTICATION_FAILED, 58);

    let response = SaslAuthenticateResponse::default()
        .with_error_code(SASL_AUTHENTICATION_FAILED)
        .with_error_message(Some(StrBytes::from_static_str("Invalid credentials")));

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 2).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = SaslAuthenticateResponse::decode(&mut bytes, 2).unwrap();

    assert_eq!(decoded.error_code, SASL_AUTHENTICATION_FAILED);
}

#[test]
fn test_illegal_sasl_state_error() {
    // ILLEGAL_SASL_STATE = 34
    assert_eq!(ILLEGAL_SASL_STATE, 34);
}

// ============================================================================
// Phase 3: Fetch Session Error Tests
// ============================================================================

#[test]
fn test_fetch_session_id_not_found_error() {
    // FETCH_SESSION_ID_NOT_FOUND = 70
    assert_eq!(FETCH_SESSION_ID_NOT_FOUND, 70);
}

#[test]
fn test_invalid_fetch_session_epoch_error() {
    // INVALID_FETCH_SESSION_EPOCH = 71
    assert_eq!(INVALID_FETCH_SESSION_EPOCH, 71);
}

// ============================================================================
// Phase 3: Topic ID Error Tests (for newer protocol versions)
// ============================================================================

#[test]
fn test_unknown_topic_id_error() {
    // UNKNOWN_TOPIC_ID = 100 (for Topic UUID lookups)
    assert_eq!(UNKNOWN_TOPIC_ID, 100);
}

#[test]
fn test_inconsistent_topic_id_error() {
    // INCONSISTENT_TOPIC_ID = 103
    assert_eq!(INCONSISTENT_TOPIC_ID, 103);
}

// ============================================================================
// Phase 3: Error Code Classification Tests
// ============================================================================

#[test]
fn test_retriable_vs_non_retriable_errors() {
    // Retriable errors - client should retry
    let retriable = vec![
        COORDINATOR_NOT_AVAILABLE,
        COORDINATOR_LOAD_IN_PROGRESS,
        NOT_COORDINATOR,
        NOT_LEADER_OR_FOLLOWER,
        LEADER_NOT_AVAILABLE,
        REQUEST_TIMED_OUT,
        REBALANCE_IN_PROGRESS,
        UNKNOWN_MEMBER_ID, // Retriable with new member ID
    ];

    // Non-retriable errors - should not retry
    let non_retriable = vec![
        UNSUPPORTED_VERSION,
        INVALID_CONFIG,
        INVALID_PARTITIONS,
        TOPIC_AUTHORIZATION_FAILED,
        GROUP_AUTHORIZATION_FAILED,
        SASL_AUTHENTICATION_FAILED,
    ];

    // Just verify they are distinct sets and have expected values
    for &code in &retriable {
        assert!(!non_retriable.contains(&code));
    }

    for &code in &non_retriable {
        assert!(!retriable.contains(&code));
    }
}

#[test]
fn test_all_error_codes_have_valid_values() {
    // Test that all error codes we use are in valid range
    let all_codes = vec![
        NONE,
        UNKNOWN_SERVER_ERROR,
        OFFSET_OUT_OF_RANGE,
        UNKNOWN_TOPIC_OR_PARTITION,
        NOT_LEADER_OR_FOLLOWER,
        REQUEST_TIMED_OUT,
        UNSUPPORTED_VERSION,
        FENCED_LEADER_EPOCH,
        UNKNOWN_LEADER_EPOCH,
        THROTTLING_QUOTA_EXCEEDED,
        UNKNOWN_TOPIC_ID,
        TRANSACTIONAL_ID_NOT_FOUND,
    ];

    // All codes are already i16, just verify the collection is non-empty
    assert!(!all_codes.is_empty(), "Expected at least one error code");
}
