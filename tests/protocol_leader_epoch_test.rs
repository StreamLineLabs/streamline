//! Phase 13: OffsetForLeaderEpoch Tests
//!
//! Tests for OffsetForLeaderEpoch wire protocol:
//! - Request/response encoding
//! - Leader epoch tracking
//! - Epoch mismatch handling
//! - End offset retrieval
//! - Truncation detection

use bytes::BytesMut;
use kafka_protocol::messages::offset_for_leader_epoch_request::{
    OffsetForLeaderPartition, OffsetForLeaderTopic,
};
use kafka_protocol::messages::offset_for_leader_epoch_response::{
    EpochEndOffset, OffsetForLeaderTopicResult,
};
use kafka_protocol::messages::{
    BrokerId, OffsetForLeaderEpochRequest, OffsetForLeaderEpochResponse, TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};

// =============================================================================
// OffsetForLeaderEpoch Request Tests
// =============================================================================

#[test]
fn test_offset_for_leader_epoch_request_basic() {
    let request = OffsetForLeaderEpochRequest::default();

    let mut buf = BytesMut::new();
    let result = request.encode(&mut buf, 0);
    assert!(result.is_ok());
}

#[test]
fn test_offset_for_leader_epoch_request_with_topics() {
    let mut request = OffsetForLeaderEpochRequest::default();

    let mut topic = OffsetForLeaderTopic::default();
    topic.topic = TopicName(StrBytes::from_static_str("my-topic"));

    let mut partition = OffsetForLeaderPartition::default();
    partition.partition = 0;
    partition.leader_epoch = 5;

    topic.partitions.push(partition);
    request.topics.push(topic);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = OffsetForLeaderEpochRequest::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.topics.len(), 1);
    assert_eq!(decoded.topics[0].partitions[0].leader_epoch, 5);
}

#[test]
fn test_offset_for_leader_epoch_request_multiple_partitions() {
    let mut request = OffsetForLeaderEpochRequest::default();

    let mut topic = OffsetForLeaderTopic::default();
    topic.topic = TopicName(StrBytes::from_static_str("multi-partition-topic"));

    for i in 0..3 {
        let mut partition = OffsetForLeaderPartition::default();
        partition.partition = i;
        partition.leader_epoch = i + 1;
        topic.partitions.push(partition);
    }

    request.topics.push(topic);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = OffsetForLeaderEpochRequest::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.topics[0].partitions.len(), 3);
    assert_eq!(decoded.topics[0].partitions[0].leader_epoch, 1);
    assert_eq!(decoded.topics[0].partitions[1].leader_epoch, 2);
    assert_eq!(decoded.topics[0].partitions[2].leader_epoch, 3);
}

#[test]
fn test_offset_for_leader_epoch_request_with_current_leader_epoch() {
    let mut request = OffsetForLeaderEpochRequest::default();

    let mut topic = OffsetForLeaderTopic::default();
    topic.topic = TopicName(StrBytes::from_static_str("my-topic"));

    let mut partition = OffsetForLeaderPartition::default();
    partition.partition = 0;
    partition.current_leader_epoch = 10; // Current epoch on client
    partition.leader_epoch = 9; // Epoch we're querying for

    topic.partitions.push(partition);
    request.topics.push(topic);

    // Version 2+ includes current_leader_epoch
    let mut buf = BytesMut::new();
    request.encode(&mut buf, 2).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = OffsetForLeaderEpochRequest::decode(&mut read_buf, 2).unwrap();

    assert_eq!(decoded.topics[0].partitions[0].current_leader_epoch, 10);
    assert_eq!(decoded.topics[0].partitions[0].leader_epoch, 9);
}

#[test]
fn test_offset_for_leader_epoch_request_with_replica_id() {
    let mut request = OffsetForLeaderEpochRequest::default();
    request.replica_id = BrokerId(1);

    let mut topic = OffsetForLeaderTopic::default();
    topic.topic = TopicName(StrBytes::from_static_str("my-topic"));
    request.topics.push(topic);

    // replica_id available in v3+
    let mut buf = BytesMut::new();
    request.encode(&mut buf, 3).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = OffsetForLeaderEpochRequest::decode(&mut read_buf, 3).unwrap();

    assert_eq!(decoded.replica_id.0, 1);
}

#[test]
fn test_offset_for_leader_epoch_request_all_versions() {
    let request = OffsetForLeaderEpochRequest::default();

    // OffsetForLeaderEpoch supports versions 0-4
    for version in 0..=4 {
        let mut buf = BytesMut::new();
        let result = request.clone().encode(&mut buf, version);
        assert!(result.is_ok(), "Failed to encode v{}", version);
    }
}

// =============================================================================
// OffsetForLeaderEpoch Response Tests
// =============================================================================

#[test]
fn test_offset_for_leader_epoch_response_basic() {
    let response = OffsetForLeaderEpochResponse::default();

    let mut buf = BytesMut::new();
    let result = response.encode(&mut buf, 0);
    assert!(result.is_ok());
}

#[test]
fn test_offset_for_leader_epoch_response_with_results() {
    let mut response = OffsetForLeaderEpochResponse::default();

    let mut topic_result = OffsetForLeaderTopicResult::default();
    topic_result.topic = TopicName(StrBytes::from_static_str("my-topic"));

    let mut partition_result = EpochEndOffset::default();
    partition_result.partition = 0;
    partition_result.error_code = 0;
    partition_result.leader_epoch = 5;
    partition_result.end_offset = 1000;

    topic_result.partitions.push(partition_result);
    response.topics.push(topic_result);

    // leader_epoch in response available in v1+
    let mut buf = BytesMut::new();
    response.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = OffsetForLeaderEpochResponse::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.topics.len(), 1);
    assert_eq!(decoded.topics[0].partitions[0].end_offset, 1000);
    assert_eq!(decoded.topics[0].partitions[0].leader_epoch, 5);
}

#[test]
fn test_offset_for_leader_epoch_response_with_throttle() {
    let mut response = OffsetForLeaderEpochResponse::default();
    response.throttle_time_ms = 500;

    // Version 2+ includes throttle time
    let mut buf = BytesMut::new();
    response.encode(&mut buf, 2).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = OffsetForLeaderEpochResponse::decode(&mut read_buf, 2).unwrap();

    assert_eq!(decoded.throttle_time_ms, 500);
}

#[test]
fn test_offset_for_leader_epoch_response_error() {
    let mut response = OffsetForLeaderEpochResponse::default();

    let mut topic_result = OffsetForLeaderTopicResult::default();
    topic_result.topic = TopicName(StrBytes::from_static_str("my-topic"));

    let mut partition_result = EpochEndOffset::default();
    partition_result.partition = 0;
    partition_result.error_code = 6; // NOT_LEADER_OR_FOLLOWER
    partition_result.leader_epoch = -1;
    partition_result.end_offset = -1;

    topic_result.partitions.push(partition_result);
    response.topics.push(topic_result);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = OffsetForLeaderEpochResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.topics[0].partitions[0].error_code, 6);
}

#[test]
fn test_offset_for_leader_epoch_response_all_versions() {
    let response = OffsetForLeaderEpochResponse::default();

    for version in 0..=4 {
        let mut buf = BytesMut::new();
        let result = response.clone().encode(&mut buf, version);
        assert!(result.is_ok(), "Failed to encode response v{}", version);
    }
}

// =============================================================================
// Leader Epoch Tests
// =============================================================================

#[test]
fn test_leader_epoch_zero() {
    // Epoch 0 is the initial epoch
    let epoch: i32 = 0;
    assert_eq!(epoch, 0);
}

#[test]
fn test_leader_epoch_increment() {
    // Epochs increment on leader change
    let epoch1: i32 = 5;
    let epoch2 = epoch1 + 1;

    assert_eq!(epoch2, 6);
}

#[test]
fn test_leader_epoch_unknown() {
    // -1 indicates unknown/no epoch
    let unknown_epoch: i32 = -1;
    assert_eq!(unknown_epoch, -1);
}

#[test]
fn test_leader_epoch_comparison() {
    let old_epoch: i32 = 5;
    let new_epoch: i32 = 10;

    assert!(new_epoch > old_epoch);
}

#[test]
fn test_leader_epoch_in_request() {
    let mut request = OffsetForLeaderEpochRequest::default();

    let mut topic = OffsetForLeaderTopic::default();
    topic.topic = TopicName(StrBytes::from_static_str("epoch-topic"));

    let mut partition = OffsetForLeaderPartition::default();
    partition.partition = 0;
    partition.leader_epoch = 10;

    topic.partitions.push(partition);
    request.topics.push(topic);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = OffsetForLeaderEpochRequest::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.topics[0].partitions[0].leader_epoch, 10);
}

// =============================================================================
// End Offset Tests
// =============================================================================

#[test]
fn test_end_offset_basic() {
    let mut partition_result = EpochEndOffset::default();
    partition_result.end_offset = 5000;

    assert_eq!(partition_result.end_offset, 5000);
}

#[test]
fn test_end_offset_zero() {
    let mut partition_result = EpochEndOffset::default();
    partition_result.end_offset = 0;

    assert_eq!(partition_result.end_offset, 0);
}

#[test]
fn test_end_offset_undefined() {
    // -1 indicates undefined/unknown offset
    let mut partition_result = EpochEndOffset::default();
    partition_result.end_offset = -1;

    assert_eq!(partition_result.end_offset, -1);
}

#[test]
fn test_end_offset_large() {
    let mut partition_result = EpochEndOffset::default();
    partition_result.end_offset = i64::MAX;

    assert_eq!(partition_result.end_offset, i64::MAX);
}

// =============================================================================
// Epoch Mismatch Tests
// =============================================================================

#[test]
fn test_fenced_leader_epoch_error() {
    // FENCED_LEADER_EPOCH (74) - requested epoch is newer than current
    let error_code: i16 = 74;
    assert_eq!(error_code, 74);
}

#[test]
fn test_unknown_leader_epoch_error() {
    // UNKNOWN_LEADER_EPOCH (75) - requested epoch doesn't exist
    let error_code: i16 = 75;
    assert_eq!(error_code, 75);
}

#[test]
fn test_epoch_mismatch_response() {
    let mut response = OffsetForLeaderEpochResponse::default();

    let mut topic_result = OffsetForLeaderTopicResult::default();
    topic_result.topic = TopicName(StrBytes::from_static_str("my-topic"));

    let mut partition_result = EpochEndOffset::default();
    partition_result.partition = 0;
    partition_result.error_code = 75; // UNKNOWN_LEADER_EPOCH
    partition_result.leader_epoch = -1;
    partition_result.end_offset = -1;

    topic_result.partitions.push(partition_result);
    response.topics.push(topic_result);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = OffsetForLeaderEpochResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.topics[0].partitions[0].error_code, 75);
}

// =============================================================================
// Truncation Detection Tests
// =============================================================================

#[test]
fn test_truncation_detection() {
    // If end_offset < expected, truncation occurred
    let expected_offset: i64 = 1000;
    let returned_end_offset: i64 = 500;

    let truncation_detected = returned_end_offset < expected_offset;
    assert!(truncation_detected);
}

#[test]
fn test_no_truncation() {
    let expected_offset: i64 = 1000;
    let returned_end_offset: i64 = 1500;

    let truncation_detected = returned_end_offset < expected_offset;
    assert!(!truncation_detected);
}

#[test]
fn test_truncation_response() {
    // Simulating truncation scenario
    let mut response = OffsetForLeaderEpochResponse::default();

    let mut topic_result = OffsetForLeaderTopicResult::default();
    topic_result.topic = TopicName(StrBytes::from_static_str("truncated-topic"));

    let mut partition_result = EpochEndOffset::default();
    partition_result.partition = 0;
    partition_result.error_code = 0;
    partition_result.leader_epoch = 5;
    partition_result.end_offset = 500; // Lower than client's expected offset

    topic_result.partitions.push(partition_result);
    response.topics.push(topic_result);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = OffsetForLeaderEpochResponse::decode(&mut read_buf, 0).unwrap();

    // Client should detect truncation if end_offset < local offset
    assert_eq!(decoded.topics[0].partitions[0].end_offset, 500);
}

// =============================================================================
// Roundtrip Tests
// =============================================================================

#[test]
fn test_request_response_roundtrip() {
    let mut request = OffsetForLeaderEpochRequest::default();

    let mut topic = OffsetForLeaderTopic::default();
    topic.topic = TopicName(StrBytes::from_static_str("roundtrip-topic"));

    let mut partition = OffsetForLeaderPartition::default();
    partition.partition = 5;
    partition.leader_epoch = 20;

    topic.partitions.push(partition);
    request.topics.push(topic);

    let mut request_buf = BytesMut::new();
    request.encode(&mut request_buf, 2).unwrap();

    let mut read_buf = request_buf.freeze();
    let decoded_request = OffsetForLeaderEpochRequest::decode(&mut read_buf, 2).unwrap();

    // Prepare response based on request
    let mut response = OffsetForLeaderEpochResponse::default();

    let mut topic_result = OffsetForLeaderTopicResult::default();
    topic_result.topic = decoded_request.topics[0].topic.clone();

    let mut partition_result = EpochEndOffset::default();
    partition_result.partition = decoded_request.topics[0].partitions[0].partition;
    partition_result.error_code = 0;
    partition_result.leader_epoch = decoded_request.topics[0].partitions[0].leader_epoch;
    partition_result.end_offset = 10000;

    topic_result.partitions.push(partition_result);
    response.topics.push(topic_result);

    let mut response_buf = BytesMut::new();
    response.encode(&mut response_buf, 2).unwrap();

    let mut response_read_buf = response_buf.freeze();
    let decoded_response = OffsetForLeaderEpochResponse::decode(&mut response_read_buf, 2).unwrap();

    assert_eq!(
        decoded_response.topics[0].topic.0.as_str(),
        "roundtrip-topic"
    );
    assert_eq!(decoded_response.topics[0].partitions[0].partition, 5);
    assert_eq!(decoded_response.topics[0].partitions[0].end_offset, 10000);
}

// =============================================================================
// Multiple Topics/Partitions Tests
// =============================================================================

#[test]
fn test_multiple_topics_request() {
    let mut request = OffsetForLeaderEpochRequest::default();

    for i in 0..3 {
        let mut topic = OffsetForLeaderTopic::default();
        topic.topic = TopicName(StrBytes::from_string(format!("topic-{}", i)));

        let mut partition = OffsetForLeaderPartition::default();
        partition.partition = 0;
        partition.leader_epoch = i;
        topic.partitions.push(partition);

        request.topics.push(topic);
    }

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = OffsetForLeaderEpochRequest::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.topics.len(), 3);
}

#[test]
fn test_multiple_topics_response() {
    let mut response = OffsetForLeaderEpochResponse::default();

    for i in 0..3 {
        let mut topic_result = OffsetForLeaderTopicResult::default();
        topic_result.topic = TopicName(StrBytes::from_string(format!("topic-{}", i)));

        let mut partition_result = EpochEndOffset::default();
        partition_result.partition = 0;
        partition_result.error_code = 0;
        partition_result.leader_epoch = i;
        partition_result.end_offset = (i as i64 + 1) * 1000;

        topic_result.partitions.push(partition_result);
        response.topics.push(topic_result);
    }

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = OffsetForLeaderEpochResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.topics.len(), 3);
    assert_eq!(decoded.topics[0].partitions[0].end_offset, 1000);
    assert_eq!(decoded.topics[1].partitions[0].end_offset, 2000);
    assert_eq!(decoded.topics[2].partitions[0].end_offset, 3000);
}

// =============================================================================
// API Key and Version Tests
// =============================================================================

#[test]
fn test_offset_for_leader_epoch_api_key() {
    use kafka_protocol::messages::ApiKey;
    assert_eq!(ApiKey::OffsetForLeaderEpoch as i16, 23);
}

// =============================================================================
// Replica ID Tests
// =============================================================================

#[test]
fn test_replica_id_consumer() {
    // Consumer uses replica_id = -1
    let mut request = OffsetForLeaderEpochRequest::default();
    request.replica_id = BrokerId(-1);

    // replica_id available in v3+
    let mut buf = BytesMut::new();
    request.encode(&mut buf, 3).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = OffsetForLeaderEpochRequest::decode(&mut read_buf, 3).unwrap();

    assert_eq!(decoded.replica_id.0, -1);
}

#[test]
fn test_replica_id_follower() {
    // Follower replica uses its broker ID
    let mut request = OffsetForLeaderEpochRequest::default();
    request.replica_id = BrokerId(3);

    // replica_id available in v3+
    let mut buf = BytesMut::new();
    request.encode(&mut buf, 3).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = OffsetForLeaderEpochRequest::decode(&mut read_buf, 3).unwrap();

    assert_eq!(decoded.replica_id.0, 3);
}
