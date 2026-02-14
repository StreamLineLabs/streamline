//! Admin API Wire Protocol Tests
//!
//! Tests for Kafka admin API protocol including:
//! - DeleteTopics request/response encoding (v0-v6)
//! - CreatePartitions request/response encoding (v0-v3)
//! - DeleteRecords request/response encoding (v0-v2)
//! - DescribeCluster request/response encoding (v0)
//! - DeleteGroups request/response encoding (v0-v2)
//!
//! Based on Kafka Protocol Specification: https://kafka.apache.org/protocol.html

use bytes::BytesMut;
use kafka_protocol::messages::{
    BrokerId, CreatePartitionsRequest, CreatePartitionsResponse, DeleteGroupsRequest,
    DeleteGroupsResponse, DeleteRecordsRequest, DeleteRecordsResponse, DeleteTopicsRequest,
    DeleteTopicsResponse, DescribeClusterRequest, DescribeClusterResponse, GroupId, TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};

// ============================================================================
// DeleteTopics Request Tests (API Key 20, v0-v6)
// ============================================================================

/// Test DeleteTopicsRequest v0 basic encoding
#[test]
fn test_delete_topics_request_v0() {
    let mut request = DeleteTopicsRequest::default();
    request
        .topic_names
        .push(TopicName(StrBytes::from_static_str("topic-to-delete")));
    request.timeout_ms = 30000;

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DeleteTopicsRequest::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.topic_names.len(), 1);
    assert_eq!(
        decoded.topic_names[0],
        TopicName(StrBytes::from_static_str("topic-to-delete"))
    );
    assert_eq!(decoded.timeout_ms, 30000);
}

/// Test DeleteTopicsRequest with multiple topics
#[test]
fn test_delete_topics_request_multiple_topics() {
    let mut request = DeleteTopicsRequest::default();
    request
        .topic_names
        .push(TopicName(StrBytes::from_static_str("topic-1")));
    request
        .topic_names
        .push(TopicName(StrBytes::from_static_str("topic-2")));
    request
        .topic_names
        .push(TopicName(StrBytes::from_static_str("topic-3")));
    request.timeout_ms = 60000;

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DeleteTopicsRequest::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.topic_names.len(), 3);
    assert_eq!(decoded.timeout_ms, 60000);
}

/// Test DeleteTopicsRequest v4 (flexible version)
#[test]
fn test_delete_topics_request_v4_flexible() {
    let mut request = DeleteTopicsRequest::default();
    request
        .topic_names
        .push(TopicName(StrBytes::from_static_str("flexible-topic")));
    request.timeout_ms = 15000;

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 4).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DeleteTopicsRequest::decode(&mut read_buf, 4).unwrap();

    assert_eq!(decoded.topic_names.len(), 1);
    assert_eq!(decoded.timeout_ms, 15000);
}

/// Test DeleteTopicsRequest v6 with topics array (uses DeleteTopicState)
#[test]
fn test_delete_topics_request_v6() {
    use kafka_protocol::messages::delete_topics_request::DeleteTopicState;

    let mut request = DeleteTopicsRequest::default();
    let mut topic = DeleteTopicState::default();
    topic.name = Some(TopicName(StrBytes::from_static_str("v6-topic")));
    request.topics.push(topic);
    request.timeout_ms = 45000;

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 6).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DeleteTopicsRequest::decode(&mut read_buf, 6).unwrap();

    assert_eq!(decoded.topics.len(), 1);
    assert_eq!(decoded.timeout_ms, 45000);
}

/// Test DeleteTopicsRequest with empty topic list
#[test]
fn test_delete_topics_request_empty() {
    let mut request = DeleteTopicsRequest::default();
    request.timeout_ms = 10000;

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 3).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DeleteTopicsRequest::decode(&mut read_buf, 3).unwrap();

    assert_eq!(decoded.topic_names.len(), 0);
    assert_eq!(decoded.timeout_ms, 10000);
}

// ============================================================================
// DeleteTopics Response Tests
// ============================================================================

/// Test DeleteTopicsResponse v0 basic encoding
#[test]
fn test_delete_topics_response_v0() {
    use kafka_protocol::messages::delete_topics_response::DeletableTopicResult;

    let mut response = DeleteTopicsResponse::default();

    let mut result = DeletableTopicResult::default();
    result.name = Some(TopicName(StrBytes::from_static_str("deleted-topic")));
    result.error_code = 0; // No error
    response.responses.push(result);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DeleteTopicsResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.responses.len(), 1);
    assert_eq!(decoded.responses[0].error_code, 0);
}

/// Test DeleteTopicsResponse with error
#[test]
fn test_delete_topics_response_with_error() {
    use kafka_protocol::messages::delete_topics_response::DeletableTopicResult;

    let mut response = DeleteTopicsResponse::default();

    let mut result = DeletableTopicResult::default();
    result.name = Some(TopicName(StrBytes::from_static_str("nonexistent-topic")));
    result.error_code = 3; // UNKNOWN_TOPIC_OR_PARTITION
    response.responses.push(result);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DeleteTopicsResponse::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.responses[0].error_code, 3);
}

/// Test DeleteTopicsResponse v1 with throttle time
#[test]
fn test_delete_topics_response_v1_throttle() {
    use kafka_protocol::messages::delete_topics_response::DeletableTopicResult;

    let mut response = DeleteTopicsResponse::default();
    response.throttle_time_ms = 1000;

    let mut result = DeletableTopicResult::default();
    result.name = Some(TopicName(StrBytes::from_static_str("throttled-topic")));
    result.error_code = 0;
    response.responses.push(result);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DeleteTopicsResponse::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.throttle_time_ms, 1000);
}

/// Test DeleteTopicsResponse with multiple results
#[test]
fn test_delete_topics_response_multiple() {
    use kafka_protocol::messages::delete_topics_response::DeletableTopicResult;

    let mut response = DeleteTopicsResponse::default();
    response.throttle_time_ms = 0;

    for i in 0..5 {
        let mut result = DeletableTopicResult::default();
        result.name = Some(TopicName(StrBytes::from_string(format!("topic-{}", i))));
        result.error_code = if i % 2 == 0 { 0 } else { 3 };
        response.responses.push(result);
    }

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 4).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DeleteTopicsResponse::decode(&mut read_buf, 4).unwrap();

    assert_eq!(decoded.responses.len(), 5);
    assert_eq!(decoded.responses[0].error_code, 0);
    assert_eq!(decoded.responses[1].error_code, 3);
}

/// Test DeleteTopicsResponse v5 with error message
#[test]
fn test_delete_topics_response_v5_error_message() {
    use kafka_protocol::messages::delete_topics_response::DeletableTopicResult;

    let mut response = DeleteTopicsResponse::default();

    let mut result = DeletableTopicResult::default();
    result.name = Some(TopicName(StrBytes::from_static_str("error-topic")));
    result.error_code = 41; // INVALID_CONFIG
    result.error_message = Some(StrBytes::from_static_str("Invalid topic configuration"));
    response.responses.push(result);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 5).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DeleteTopicsResponse::decode(&mut read_buf, 5).unwrap();

    assert_eq!(decoded.responses[0].error_code, 41);
    assert!(decoded.responses[0].error_message.is_some());
}

// ============================================================================
// CreatePartitions Request Tests (API Key 37, v0-v3)
// ============================================================================

/// Test CreatePartitionsRequest v0 basic encoding
#[test]
fn test_create_partitions_request_v0() {
    use kafka_protocol::messages::create_partitions_request::CreatePartitionsTopic;

    let mut request = CreatePartitionsRequest::default();
    request.timeout_ms = 30000;
    request.validate_only = false;

    let mut topic = CreatePartitionsTopic::default();
    topic.name = TopicName(StrBytes::from_static_str("expand-topic"));
    topic.count = 10;
    request.topics.push(topic);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = CreatePartitionsRequest::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.topics.len(), 1);
    assert_eq!(decoded.topics[0].count, 10);
    assert!(!decoded.validate_only);
}

/// Test CreatePartitionsRequest with validate_only flag
#[test]
fn test_create_partitions_request_validate_only() {
    use kafka_protocol::messages::create_partitions_request::CreatePartitionsTopic;

    let mut request = CreatePartitionsRequest::default();
    request.timeout_ms = 15000;
    request.validate_only = true;

    let mut topic = CreatePartitionsTopic::default();
    topic.name = TopicName(StrBytes::from_static_str("validate-topic"));
    topic.count = 20;
    request.topics.push(topic);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = CreatePartitionsRequest::decode(&mut read_buf, 1).unwrap();

    assert!(decoded.validate_only);
    assert_eq!(decoded.topics[0].count, 20);
}

/// Test CreatePartitionsRequest with multiple topics
#[test]
fn test_create_partitions_request_multiple_topics() {
    use kafka_protocol::messages::create_partitions_request::CreatePartitionsTopic;

    let mut request = CreatePartitionsRequest::default();
    request.timeout_ms = 60000;

    for i in 1..=5 {
        let mut topic = CreatePartitionsTopic::default();
        topic.name = TopicName(StrBytes::from_string(format!("expand-topic-{}", i)));
        topic.count = i * 4;
        request.topics.push(topic);
    }

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 2).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = CreatePartitionsRequest::decode(&mut read_buf, 2).unwrap();

    assert_eq!(decoded.topics.len(), 5);
    assert_eq!(decoded.topics[0].count, 4);
    assert_eq!(decoded.topics[4].count, 20);
}

/// Test CreatePartitionsRequest v2 (flexible version)
#[test]
fn test_create_partitions_request_v2_flexible() {
    use kafka_protocol::messages::create_partitions_request::CreatePartitionsTopic;

    let mut request = CreatePartitionsRequest::default();
    request.timeout_ms = 30000;

    let mut topic = CreatePartitionsTopic::default();
    topic.name = TopicName(StrBytes::from_static_str("flexible-expand"));
    topic.count = 16;
    request.topics.push(topic);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 2).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = CreatePartitionsRequest::decode(&mut read_buf, 2).unwrap();

    assert_eq!(decoded.topics[0].count, 16);
}

/// Test CreatePartitionsRequest v3
#[test]
fn test_create_partitions_request_v3() {
    use kafka_protocol::messages::create_partitions_request::CreatePartitionsTopic;

    let mut request = CreatePartitionsRequest::default();
    request.timeout_ms = 45000;

    let mut topic = CreatePartitionsTopic::default();
    topic.name = TopicName(StrBytes::from_static_str("v3-expand"));
    topic.count = 32;
    request.topics.push(topic);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 3).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = CreatePartitionsRequest::decode(&mut read_buf, 3).unwrap();

    assert_eq!(decoded.topics[0].count, 32);
    assert_eq!(decoded.timeout_ms, 45000);
}

// ============================================================================
// CreatePartitions Response Tests
// ============================================================================

/// Test CreatePartitionsResponse v0 basic encoding
#[test]
fn test_create_partitions_response_v0() {
    use kafka_protocol::messages::create_partitions_response::CreatePartitionsTopicResult;

    let mut response = CreatePartitionsResponse::default();
    response.throttle_time_ms = 0;

    let mut result = CreatePartitionsTopicResult::default();
    result.name = TopicName(StrBytes::from_static_str("expanded-topic"));
    result.error_code = 0;
    response.results.push(result);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = CreatePartitionsResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.results.len(), 1);
    assert_eq!(decoded.results[0].error_code, 0);
}

/// Test CreatePartitionsResponse with error
#[test]
fn test_create_partitions_response_with_error() {
    use kafka_protocol::messages::create_partitions_response::CreatePartitionsTopicResult;

    let mut response = CreatePartitionsResponse::default();

    let mut result = CreatePartitionsTopicResult::default();
    result.name = TopicName(StrBytes::from_static_str("invalid-expand"));
    result.error_code = 37; // INVALID_PARTITIONS
    result.error_message = Some(StrBytes::from_static_str(
        "Partition count must be greater than current",
    ));
    response.results.push(result);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = CreatePartitionsResponse::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.results[0].error_code, 37);
    assert!(decoded.results[0].error_message.is_some());
}

/// Test CreatePartitionsResponse with throttle time
#[test]
fn test_create_partitions_response_throttle() {
    use kafka_protocol::messages::create_partitions_response::CreatePartitionsTopicResult;

    let mut response = CreatePartitionsResponse::default();
    response.throttle_time_ms = 5000;

    let mut result = CreatePartitionsTopicResult::default();
    result.name = TopicName(StrBytes::from_static_str("throttled-expand"));
    result.error_code = 0;
    response.results.push(result);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 2).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = CreatePartitionsResponse::decode(&mut read_buf, 2).unwrap();

    assert_eq!(decoded.throttle_time_ms, 5000);
}

// ============================================================================
// DeleteRecords Request Tests (API Key 21, v0-v2)
// ============================================================================

/// Test DeleteRecordsRequest v0 basic encoding
#[test]
fn test_delete_records_request_v0() {
    use kafka_protocol::messages::delete_records_request::{
        DeleteRecordsPartition, DeleteRecordsTopic,
    };

    let mut request = DeleteRecordsRequest::default();
    request.timeout_ms = 30000;

    let mut topic = DeleteRecordsTopic::default();
    topic.name = TopicName(StrBytes::from_static_str("delete-records-topic"));

    let mut partition = DeleteRecordsPartition::default();
    partition.partition_index = 0;
    partition.offset = 100;
    topic.partitions.push(partition);

    request.topics.push(topic);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DeleteRecordsRequest::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.topics.len(), 1);
    assert_eq!(decoded.topics[0].partitions[0].offset, 100);
}

/// Test DeleteRecordsRequest with multiple partitions
#[test]
fn test_delete_records_request_multiple_partitions() {
    use kafka_protocol::messages::delete_records_request::{
        DeleteRecordsPartition, DeleteRecordsTopic,
    };

    let mut request = DeleteRecordsRequest::default();
    request.timeout_ms = 60000;

    let mut topic = DeleteRecordsTopic::default();
    topic.name = TopicName(StrBytes::from_static_str("multi-partition-delete"));

    for i in 0..5 {
        let mut partition = DeleteRecordsPartition::default();
        partition.partition_index = i;
        partition.offset = (i + 1) as i64 * 1000;
        topic.partitions.push(partition);
    }

    request.topics.push(topic);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DeleteRecordsRequest::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.topics[0].partitions.len(), 5);
    assert_eq!(decoded.topics[0].partitions[0].offset, 1000);
    assert_eq!(decoded.topics[0].partitions[4].offset, 5000);
}

/// Test DeleteRecordsRequest v2 (flexible version)
#[test]
fn test_delete_records_request_v2_flexible() {
    use kafka_protocol::messages::delete_records_request::{
        DeleteRecordsPartition, DeleteRecordsTopic,
    };

    let mut request = DeleteRecordsRequest::default();
    request.timeout_ms = 30000;

    let mut topic = DeleteRecordsTopic::default();
    topic.name = TopicName(StrBytes::from_static_str("flexible-delete"));

    let mut partition = DeleteRecordsPartition::default();
    partition.partition_index = 0;
    partition.offset = 500;
    topic.partitions.push(partition);

    request.topics.push(topic);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 2).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DeleteRecordsRequest::decode(&mut read_buf, 2).unwrap();

    assert_eq!(decoded.topics[0].partitions[0].offset, 500);
}

/// Test DeleteRecordsRequest with high watermark offset
#[test]
fn test_delete_records_request_high_offset() {
    use kafka_protocol::messages::delete_records_request::{
        DeleteRecordsPartition, DeleteRecordsTopic,
    };

    let mut request = DeleteRecordsRequest::default();
    request.timeout_ms = 30000;

    let mut topic = DeleteRecordsTopic::default();
    topic.name = TopicName(StrBytes::from_static_str("high-offset-delete"));

    let mut partition = DeleteRecordsPartition::default();
    partition.partition_index = 0;
    partition.offset = i64::MAX / 2; // Large offset
    topic.partitions.push(partition);

    request.topics.push(topic);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 2).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DeleteRecordsRequest::decode(&mut read_buf, 2).unwrap();

    assert_eq!(decoded.topics[0].partitions[0].offset, i64::MAX / 2);
}

// ============================================================================
// DeleteRecords Response Tests
// ============================================================================

/// Test DeleteRecordsResponse v0 basic encoding
#[test]
fn test_delete_records_response_v0() {
    use kafka_protocol::messages::delete_records_response::{
        DeleteRecordsPartitionResult, DeleteRecordsTopicResult,
    };

    let mut response = DeleteRecordsResponse::default();
    response.throttle_time_ms = 0;

    let mut topic_result = DeleteRecordsTopicResult::default();
    topic_result.name = TopicName(StrBytes::from_static_str("deleted-records-topic"));

    let mut partition_result = DeleteRecordsPartitionResult::default();
    partition_result.partition_index = 0;
    partition_result.low_watermark = 100;
    partition_result.error_code = 0;
    topic_result.partitions.push(partition_result);

    response.topics.push(topic_result);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DeleteRecordsResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.topics.len(), 1);
    assert_eq!(decoded.topics[0].partitions[0].low_watermark, 100);
    assert_eq!(decoded.topics[0].partitions[0].error_code, 0);
}

/// Test DeleteRecordsResponse with error
#[test]
fn test_delete_records_response_with_error() {
    use kafka_protocol::messages::delete_records_response::{
        DeleteRecordsPartitionResult, DeleteRecordsTopicResult,
    };

    let mut response = DeleteRecordsResponse::default();

    let mut topic_result = DeleteRecordsTopicResult::default();
    topic_result.name = TopicName(StrBytes::from_static_str("failed-delete"));

    let mut partition_result = DeleteRecordsPartitionResult::default();
    partition_result.partition_index = 0;
    partition_result.low_watermark = -1;
    partition_result.error_code = 1; // OFFSET_OUT_OF_RANGE
    topic_result.partitions.push(partition_result);

    response.topics.push(topic_result);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DeleteRecordsResponse::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.topics[0].partitions[0].error_code, 1);
    assert_eq!(decoded.topics[0].partitions[0].low_watermark, -1);
}

/// Test DeleteRecordsResponse with throttle time
#[test]
fn test_delete_records_response_throttle() {
    use kafka_protocol::messages::delete_records_response::{
        DeleteRecordsPartitionResult, DeleteRecordsTopicResult,
    };

    let mut response = DeleteRecordsResponse::default();
    response.throttle_time_ms = 2000;

    let mut topic_result = DeleteRecordsTopicResult::default();
    topic_result.name = TopicName(StrBytes::from_static_str("throttled-delete"));

    let mut partition_result = DeleteRecordsPartitionResult::default();
    partition_result.partition_index = 0;
    partition_result.low_watermark = 50;
    partition_result.error_code = 0;
    topic_result.partitions.push(partition_result);

    response.topics.push(topic_result);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 2).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DeleteRecordsResponse::decode(&mut read_buf, 2).unwrap();

    assert_eq!(decoded.throttle_time_ms, 2000);
}

// ============================================================================
// DescribeCluster Request Tests (API Key 60, v0)
// ============================================================================

/// Test DescribeClusterRequest v0 basic encoding
#[test]
fn test_describe_cluster_request_v0() {
    let mut request = DescribeClusterRequest::default();
    request.include_cluster_authorized_operations = true;

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DescribeClusterRequest::decode(&mut read_buf, 0).unwrap();

    assert!(decoded.include_cluster_authorized_operations);
}

/// Test DescribeClusterRequest without authorized operations
#[test]
fn test_describe_cluster_request_no_auth_ops() {
    let mut request = DescribeClusterRequest::default();
    request.include_cluster_authorized_operations = false;

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DescribeClusterRequest::decode(&mut read_buf, 0).unwrap();

    assert!(!decoded.include_cluster_authorized_operations);
}

// ============================================================================
// DescribeCluster Response Tests
// ============================================================================

/// Test DescribeClusterResponse v0 basic encoding
#[test]
fn test_describe_cluster_response_v0() {
    use kafka_protocol::messages::describe_cluster_response::DescribeClusterBroker;

    let mut response = DescribeClusterResponse::default();
    response.throttle_time_ms = 0;
    response.error_code = 0;
    response.cluster_id = StrBytes::from_static_str("test-cluster-id");
    response.controller_id = BrokerId(1);

    let mut broker = DescribeClusterBroker::default();
    broker.broker_id = BrokerId(1);
    broker.host = StrBytes::from_static_str("localhost");
    broker.port = 9092;
    response.brokers.push(broker);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DescribeClusterResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.error_code, 0);
    assert_eq!(decoded.controller_id, BrokerId(1));
    assert_eq!(decoded.brokers.len(), 1);
}

/// Test DescribeClusterResponse with multiple brokers
#[test]
fn test_describe_cluster_response_multiple_brokers() {
    use kafka_protocol::messages::describe_cluster_response::DescribeClusterBroker;

    let mut response = DescribeClusterResponse::default();
    response.cluster_id = StrBytes::from_static_str("multi-broker-cluster");
    response.controller_id = BrokerId(1);

    for i in 1..=3 {
        let mut broker = DescribeClusterBroker::default();
        broker.broker_id = BrokerId(i);
        broker.host = StrBytes::from_string(format!("broker-{}.example.com", i));
        broker.port = 9092;
        response.brokers.push(broker);
    }

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DescribeClusterResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.brokers.len(), 3);
    assert_eq!(decoded.brokers[0].broker_id, BrokerId(1));
    assert_eq!(decoded.brokers[2].broker_id, BrokerId(3));
}

/// Test DescribeClusterResponse with error
#[test]
fn test_describe_cluster_response_with_error() {
    let mut response = DescribeClusterResponse::default();
    response.error_code = 31; // CLUSTER_AUTHORIZATION_FAILED
    response.error_message = Some(StrBytes::from_static_str(
        "Not authorized to describe cluster",
    ));

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DescribeClusterResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.error_code, 31);
    assert!(decoded.error_message.is_some());
}

/// Test DescribeClusterResponse with authorized operations
#[test]
fn test_describe_cluster_response_auth_operations() {
    use kafka_protocol::messages::describe_cluster_response::DescribeClusterBroker;

    let mut response = DescribeClusterResponse::default();
    response.cluster_id = StrBytes::from_static_str("auth-ops-cluster");
    response.controller_id = BrokerId(1);
    response.cluster_authorized_operations = 0x0F; // Some operations bitmask

    let mut broker = DescribeClusterBroker::default();
    broker.broker_id = BrokerId(1);
    broker.host = StrBytes::from_static_str("localhost");
    broker.port = 9092;
    response.brokers.push(broker);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DescribeClusterResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.cluster_authorized_operations, 0x0F);
}

// ============================================================================
// DeleteGroups Request Tests (API Key 42, v0-v2)
// ============================================================================

/// Test DeleteGroupsRequest v0 basic encoding
#[test]
fn test_delete_groups_request_v0() {
    let mut request = DeleteGroupsRequest::default();
    request
        .groups_names
        .push(GroupId(StrBytes::from_static_str("group-to-delete")));

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DeleteGroupsRequest::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.groups_names.len(), 1);
    assert_eq!(
        decoded.groups_names[0],
        GroupId(StrBytes::from_static_str("group-to-delete"))
    );
}

/// Test DeleteGroupsRequest with multiple groups
#[test]
fn test_delete_groups_request_multiple_groups() {
    let mut request = DeleteGroupsRequest::default();
    request
        .groups_names
        .push(GroupId(StrBytes::from_static_str("group-1")));
    request
        .groups_names
        .push(GroupId(StrBytes::from_static_str("group-2")));
    request
        .groups_names
        .push(GroupId(StrBytes::from_static_str("group-3")));

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DeleteGroupsRequest::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.groups_names.len(), 3);
}

/// Test DeleteGroupsRequest v2 (flexible version)
#[test]
fn test_delete_groups_request_v2_flexible() {
    let mut request = DeleteGroupsRequest::default();
    request
        .groups_names
        .push(GroupId(StrBytes::from_static_str("flexible-group")));

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 2).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DeleteGroupsRequest::decode(&mut read_buf, 2).unwrap();

    assert_eq!(decoded.groups_names.len(), 1);
}

/// Test DeleteGroupsRequest with empty groups list
#[test]
fn test_delete_groups_request_empty() {
    let request = DeleteGroupsRequest::default();

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DeleteGroupsRequest::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.groups_names.len(), 0);
}

// ============================================================================
// DeleteGroups Response Tests
// ============================================================================

/// Test DeleteGroupsResponse v0 basic encoding
#[test]
fn test_delete_groups_response_v0() {
    use kafka_protocol::messages::delete_groups_response::DeletableGroupResult;

    let mut response = DeleteGroupsResponse::default();
    response.throttle_time_ms = 0;

    let mut result = DeletableGroupResult::default();
    result.group_id = GroupId(StrBytes::from_static_str("deleted-group"));
    result.error_code = 0;
    response.results.push(result);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DeleteGroupsResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.results.len(), 1);
    assert_eq!(decoded.results[0].error_code, 0);
}

/// Test DeleteGroupsResponse with error
#[test]
fn test_delete_groups_response_with_error() {
    use kafka_protocol::messages::delete_groups_response::DeletableGroupResult;

    let mut response = DeleteGroupsResponse::default();

    let mut result = DeletableGroupResult::default();
    result.group_id = GroupId(StrBytes::from_static_str("active-group"));
    result.error_code = 68; // NON_EMPTY_GROUP
    response.results.push(result);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DeleteGroupsResponse::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.results[0].error_code, 68);
}

/// Test DeleteGroupsResponse with multiple results
#[test]
fn test_delete_groups_response_multiple() {
    use kafka_protocol::messages::delete_groups_response::DeletableGroupResult;

    let mut response = DeleteGroupsResponse::default();
    response.throttle_time_ms = 500;

    for i in 0..4 {
        let mut result = DeletableGroupResult::default();
        result.group_id = GroupId(StrBytes::from_string(format!("group-{}", i)));
        result.error_code = if i == 2 { 68 } else { 0 };
        response.results.push(result);
    }

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 2).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DeleteGroupsResponse::decode(&mut read_buf, 2).unwrap();

    assert_eq!(decoded.results.len(), 4);
    assert_eq!(decoded.results[2].error_code, 68);
    assert_eq!(decoded.throttle_time_ms, 500);
}

/// Test DeleteGroupsResponse with throttle time
#[test]
fn test_delete_groups_response_throttle() {
    use kafka_protocol::messages::delete_groups_response::DeletableGroupResult;

    let mut response = DeleteGroupsResponse::default();
    response.throttle_time_ms = 3000;

    let mut result = DeletableGroupResult::default();
    result.group_id = GroupId(StrBytes::from_static_str("throttled-group"));
    result.error_code = 0;
    response.results.push(result);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DeleteGroupsResponse::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.throttle_time_ms, 3000);
}

// ============================================================================
// Version Roundtrip Tests
// ============================================================================

/// Test DeleteTopics request roundtrip across all versions
#[test]
fn test_delete_topics_version_roundtrip() {
    use kafka_protocol::messages::delete_topics_request::DeleteTopicState;

    // v0-5 use topic_names
    for version in 0..=5 {
        let mut request = DeleteTopicsRequest::default();
        request
            .topic_names
            .push(TopicName(StrBytes::from_static_str("test-topic")));
        request.timeout_ms = 30000;

        let mut buf = BytesMut::new();
        request.encode(&mut buf, version).unwrap();

        let mut read_buf = buf.freeze();
        let decoded = DeleteTopicsRequest::decode(&mut read_buf, version).unwrap();

        assert_eq!(decoded.topic_names.len(), 1);
        assert_eq!(decoded.timeout_ms, 30000);
    }

    // v6 uses topics array with DeleteTopicState
    {
        let mut request = DeleteTopicsRequest::default();
        let mut topic = DeleteTopicState::default();
        topic.name = Some(TopicName(StrBytes::from_static_str("test-topic")));
        request.topics.push(topic);
        request.timeout_ms = 30000;

        let mut buf = BytesMut::new();
        request.encode(&mut buf, 6).unwrap();

        let mut read_buf = buf.freeze();
        let decoded = DeleteTopicsRequest::decode(&mut read_buf, 6).unwrap();

        assert_eq!(decoded.topics.len(), 1);
        assert_eq!(decoded.timeout_ms, 30000);
    }
}

/// Test CreatePartitions request roundtrip across all versions
#[test]
fn test_create_partitions_version_roundtrip() {
    use kafka_protocol::messages::create_partitions_request::CreatePartitionsTopic;

    for version in 0..=3 {
        let mut request = CreatePartitionsRequest::default();
        request.timeout_ms = 30000;

        let mut topic = CreatePartitionsTopic::default();
        topic.name = TopicName(StrBytes::from_static_str("test-topic"));
        topic.count = 10;
        request.topics.push(topic);

        let mut buf = BytesMut::new();
        request.encode(&mut buf, version).unwrap();

        let mut read_buf = buf.freeze();
        let decoded = CreatePartitionsRequest::decode(&mut read_buf, version).unwrap();

        assert_eq!(decoded.topics.len(), 1);
        assert_eq!(decoded.topics[0].count, 10);
    }
}

/// Test DeleteRecords request roundtrip across all versions
#[test]
fn test_delete_records_version_roundtrip() {
    use kafka_protocol::messages::delete_records_request::{
        DeleteRecordsPartition, DeleteRecordsTopic,
    };

    for version in 0..=2 {
        let mut request = DeleteRecordsRequest::default();
        request.timeout_ms = 30000;

        let mut topic = DeleteRecordsTopic::default();
        topic.name = TopicName(StrBytes::from_static_str("test-topic"));

        let mut partition = DeleteRecordsPartition::default();
        partition.partition_index = 0;
        partition.offset = 100;
        topic.partitions.push(partition);

        request.topics.push(topic);

        let mut buf = BytesMut::new();
        request.encode(&mut buf, version).unwrap();

        let mut read_buf = buf.freeze();
        let decoded = DeleteRecordsRequest::decode(&mut read_buf, version).unwrap();

        assert_eq!(decoded.topics.len(), 1);
        assert_eq!(decoded.topics[0].partitions[0].offset, 100);
    }
}

/// Test DeleteGroups request roundtrip across all versions
#[test]
fn test_delete_groups_version_roundtrip() {
    for version in 0..=2 {
        let mut request = DeleteGroupsRequest::default();
        request
            .groups_names
            .push(GroupId(StrBytes::from_static_str("test-group")));

        let mut buf = BytesMut::new();
        request.encode(&mut buf, version).unwrap();

        let mut read_buf = buf.freeze();
        let decoded = DeleteGroupsRequest::decode(&mut read_buf, version).unwrap();

        assert_eq!(decoded.groups_names.len(), 1);
    }
}
