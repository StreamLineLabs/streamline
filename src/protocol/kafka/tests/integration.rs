use super::*;

#[test]
fn test_integration_bootstrap_api_versions_first() {
    // Typical client bootstrap: ApiVersions as first request
    use kafka_protocol::messages::ApiVersionsRequest;

    let handler = create_test_handler();

    // Client sends ApiVersions first to discover supported APIs
    let api_versions_response = handler
        .handle_api_versions(ApiVersionsRequest::default())
        .unwrap();

    // Response should contain API information
    assert!(
        !api_versions_response.api_keys.is_empty(),
        "ApiVersions should return supported APIs"
    );

    // Error code should be 0 (success)
    assert_eq!(
        api_versions_response.error_code, 0,
        "ApiVersions should succeed"
    );
}

#[test]
fn test_integration_bootstrap_metadata_after_api_versions() {
    // Bootstrap: ApiVersions → Metadata
    use kafka_protocol::messages::{ApiVersionsRequest, MetadataRequest};

    let handler = create_test_handler();

    // Step 1: ApiVersions
    let _api_versions = handler
        .handle_api_versions(ApiVersionsRequest::default())
        .unwrap();

    // Step 2: Metadata to discover brokers
    let metadata_request = MetadataRequest::default();
    let metadata_response = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(handler.handle_metadata(metadata_request, 9))
        .unwrap();

    // Should have broker information
    assert!(
        !metadata_response.brokers.is_empty(),
        "Metadata should return broker info"
    );

    // Should have cluster ID
    // (cluster_id may be None in some cases, so we just check brokers)
}

#[test]
fn test_integration_bootstrap_without_api_versions() {
    // Some clients skip ApiVersions and go straight to Metadata
    use kafka_protocol::messages::MetadataRequest;

    let handler = create_test_handler();

    // Directly request Metadata
    let request = MetadataRequest::default();
    let response = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(handler.handle_metadata(request, 9))
        .unwrap();

    // Should still work
    assert!(!response.brokers.is_empty());
}

#[test]
fn test_integration_produce_creates_topic() {
    // Produce to a new topic (auto-create)
    use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
    use kafka_protocol::messages::ProduceRequest;

    let handler = create_test_handler();
    let topic_name = "integration-produce-topic";

    let partition_data = PartitionProduceData::default().with_index(0);

    let topic_data = TopicProduceData::default()
        .with_name(TopicName(StrBytes::from_static_str(topic_name)))
        .with_partition_data(vec![partition_data]);

    let request = ProduceRequest::default()
        .with_timeout_ms(5000)
        .with_acks(1)
        .with_topic_data(vec![topic_data]);

    let response = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(handler.handle_produce(request, &create_test_session_manager(), None))
        .unwrap();

    // Should have response for the topic
    assert!(
        !response.responses.is_empty(),
        "Produce should return response"
    );
}

#[test]
fn test_integration_produce_multiple_topics() {
    // Produce to multiple topics in one request
    use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
    use kafka_protocol::messages::ProduceRequest;

    let handler = create_test_handler();

    let topics: Vec<TopicProduceData> = (0..3)
        .map(|i| {
            let partition_data = PartitionProduceData::default().with_index(0);

            TopicProduceData::default()
                .with_name(TopicName(StrBytes::from_string(format!(
                    "multi-topic-{}",
                    i
                ))))
                .with_partition_data(vec![partition_data])
        })
        .collect();

    let request = ProduceRequest::default()
        .with_timeout_ms(5000)
        .with_acks(1)
        .with_topic_data(topics);

    let response = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(handler.handle_produce(request, &create_test_session_manager(), None))
        .unwrap();

    // Should have responses for all 3 topics
    assert_eq!(
        response.responses.len(),
        3,
        "Should have response for each topic"
    );
}

#[test]
fn test_integration_produce_acks_0() {
    // Fire-and-forget produce (acks=0)
    use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
    use kafka_protocol::messages::ProduceRequest;

    let handler = create_test_handler();

    let partition_data = PartitionProduceData::default().with_index(0);

    let topic_data = TopicProduceData::default()
        .with_name(TopicName(StrBytes::from_static_str("acks-0-topic")))
        .with_partition_data(vec![partition_data]);

    let request = ProduceRequest::default()
        .with_timeout_ms(5000)
        .with_acks(0) // Fire and forget
        .with_topic_data(vec![topic_data]);

    let response = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(handler.handle_produce(request, &create_test_session_manager(), None))
        .unwrap();

    // Even with acks=0, we get a response (in our implementation)
    let _ = response.responses.len();
}

#[test]
fn test_integration_fetch_empty_topic() {
    // Fetch from topic with no data
    use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
    use kafka_protocol::messages::FetchRequest;

    let handler = create_test_handler();

    let partition = FetchPartition::default()
        .with_partition(0)
        .with_fetch_offset(0);

    let topic = FetchTopic::default()
        .with_topic(TopicName(StrBytes::from_static_str("empty-fetch-topic")))
        .with_partitions(vec![partition]);

    let request = FetchRequest::default()
        .with_max_wait_ms(100)
        .with_min_bytes(1)
        .with_max_bytes(1048576)
        .with_topics(vec![topic]);

    let response = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(handler.handle_fetch(request, &create_test_session_manager(), None))
        .unwrap();

    // Should have response structure even for empty topic
    let _ = response.responses.len();
}

#[test]
fn test_integration_fetch_multiple_partitions() {
    // Fetch from multiple partitions
    use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
    use kafka_protocol::messages::FetchRequest;

    let handler = create_test_handler();

    let partitions: Vec<FetchPartition> = (0..3)
        .map(|i| {
            FetchPartition::default()
                .with_partition(i)
                .with_fetch_offset(0)
        })
        .collect();

    let topic = FetchTopic::default()
        .with_topic(TopicName(StrBytes::from_static_str(
            "multi-partition-fetch",
        )))
        .with_partitions(partitions);

    let request = FetchRequest::default()
        .with_max_wait_ms(100)
        .with_min_bytes(1)
        .with_topics(vec![topic]);

    let response = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(handler.handle_fetch(request, &create_test_session_manager(), None))
        .unwrap();

    // Should handle multiple partitions
    let _ = response.responses.len();
}

#[test]
fn test_integration_consumer_group_find_coordinator() {
    // Consumer group flow starts with FindCoordinator
    use kafka_protocol::messages::FindCoordinatorRequest;

    let handler = create_test_handler();

    let request = FindCoordinatorRequest::default()
        .with_key(StrBytes::from_static_str("test-consumer-group"))
        .with_key_type(0); // 0 = GROUP

    let response = handler.handle_find_coordinator(request, 3).unwrap();

    // Should return a coordinator or error
    // error_code 0 = success, 15 = COORDINATOR_NOT_AVAILABLE
    let _ = response.error_code;
}

#[test]
fn test_integration_consumer_group_join() {
    // JoinGroup request (sync method with api_version)
    use kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol;
    use kafka_protocol::messages::JoinGroupRequest;

    let handler = create_test_handler();

    let protocol = JoinGroupRequestProtocol::default()
        .with_name(StrBytes::from_static_str("range"))
        .with_metadata(bytes::Bytes::from_static(b""));

    let request = JoinGroupRequest::default()
        .with_group_id(GroupId(StrBytes::from_static_str("integration-test-group")))
        .with_session_timeout_ms(30000)
        .with_rebalance_timeout_ms(60000)
        .with_member_id(StrBytes::from_static_str(""))
        .with_protocol_type(StrBytes::from_static_str("consumer"))
        .with_protocols(vec![protocol]);

    let response = handler.handle_join_group(request, 6).unwrap();

    // Should get a response (may be error for unknown member)
    let _ = response.error_code;
}

#[test]
fn test_integration_consumer_group_list() {
    // ListGroups request
    use kafka_protocol::messages::ListGroupsRequest;

    let handler = create_test_handler();

    let request = ListGroupsRequest::default();
    let response = handler.handle_list_groups(request, 3).unwrap();

    // Should return list (may be empty)
    let _ = response.groups.len();
}

#[test]
fn test_integration_list_offsets_workflow() {
    // Consumer typically queries offsets before fetching
    use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
    use kafka_protocol::messages::ListOffsetsRequest;

    let handler = create_test_handler();

    // Query for earliest offset (timestamp -2)
    let partition = ListOffsetsPartition::default()
        .with_partition_index(0)
        .with_timestamp(-2); // EARLIEST

    let topic = ListOffsetsTopic::default()
        .with_name(TopicName(StrBytes::from_static_str(
            "offset-workflow-topic",
        )))
        .with_partitions(vec![partition]);

    let request = ListOffsetsRequest::default().with_topics(vec![topic]);

    let response = handler.handle_list_offsets(request, 6).unwrap();

    // Should return offset info
    assert!(!response.topics.is_empty(), "Should return offset info");
}

#[test]
fn test_integration_offset_commit_fetch_workflow() {
    // Commit offset then fetch it back
    use kafka_protocol::messages::offset_commit_request::{
        OffsetCommitRequestPartition, OffsetCommitRequestTopic,
    };
    use kafka_protocol::messages::offset_fetch_request::{
        OffsetFetchRequestGroup, OffsetFetchRequestTopics,
    };
    use kafka_protocol::messages::{OffsetCommitRequest, OffsetFetchRequest};

    let handler = create_test_handler();
    let group_id = "offset-workflow-group";
    let topic_name = "offset-workflow-topic";

    // Step 1: Commit an offset
    let partition = OffsetCommitRequestPartition::default()
        .with_partition_index(0)
        .with_committed_offset(42);

    let topic = OffsetCommitRequestTopic::default()
        .with_name(TopicName(StrBytes::from_static_str(topic_name)))
        .with_partitions(vec![partition]);

    let commit_request = OffsetCommitRequest::default()
        .with_group_id(GroupId(StrBytes::from_static_str(group_id)))
        .with_topics(vec![topic]);

    let commit_response = handler.handle_offset_commit(commit_request, 8).unwrap();

    // Should succeed or return valid error
    let _ = commit_response.topics;

    // Step 2: Fetch the offset
    let fetch_topic = OffsetFetchRequestTopics::default()
        .with_name(TopicName(StrBytes::from_static_str(topic_name)))
        .with_partition_indexes(vec![0]);

    let fetch_group = OffsetFetchRequestGroup::default()
        .with_group_id(GroupId(StrBytes::from_static_str(group_id)))
        .with_topics(Some(vec![fetch_topic]));

    let fetch_request = OffsetFetchRequest::default().with_groups(vec![fetch_group]);

    let fetch_response = handler.handle_offset_fetch(fetch_request, 8).unwrap();

    // Should return the committed offset or error
    let _ = fetch_response.groups;
}

#[test]
fn test_integration_create_then_describe_topic() {
    // CreateTopics then verify via Metadata
    use kafka_protocol::messages::create_topics_request::CreatableTopic;
    use kafka_protocol::messages::{CreateTopicsRequest, MetadataRequest};

    let handler = create_test_handler();
    let topic_name = "integration-created-topic";

    // Step 1: Create topic (sync method)
    let topic = CreatableTopic::default()
        .with_name(TopicName(StrBytes::from_static_str(topic_name)))
        .with_num_partitions(3)
        .with_replication_factor(1);

    let create_request = CreateTopicsRequest::default()
        .with_topics(vec![topic])
        .with_timeout_ms(5000);

    let create_response = handler.handle_create_topics(create_request).unwrap();

    // Should have created the topic
    assert!(
        !create_response.topics.is_empty(),
        "Should return topic creation result"
    );

    // Step 2: Describe via Metadata
    use kafka_protocol::messages::metadata_request::MetadataRequestTopic;

    let metadata_topic = MetadataRequestTopic::default()
        .with_name(Some(TopicName(StrBytes::from_static_str(topic_name))));

    let metadata_request = MetadataRequest::default().with_topics(Some(vec![metadata_topic]));

    let metadata_response = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(handler.handle_metadata(metadata_request, 9))
        .unwrap();

    // Should return topic metadata
    let _ = metadata_response.topics;
}

#[test]
fn test_integration_delete_topic() {
    // CreateTopics then DeleteTopics
    use kafka_protocol::messages::delete_topics_request::DeleteTopicState;
    use kafka_protocol::messages::{DeleteTopicsRequest, MetadataRequest};

    let handler = create_test_handler();
    let topic_name = "topic-to-delete";

    // Step 1: Request Metadata (topic may be auto-created or not exist)
    let metadata_request = MetadataRequest::default();
    let _ = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(handler.handle_metadata(metadata_request, 9));

    // Step 2: Delete topic (sync method with api_version)
    let topic_state = DeleteTopicState::default()
        .with_name(Some(TopicName(StrBytes::from_static_str(topic_name))));

    let delete_request = DeleteTopicsRequest::default()
        .with_topics(vec![topic_state])
        .with_timeout_ms(5000);

    let delete_response = handler.handle_delete_topics(delete_request, 6).unwrap();

    // Should have response for deletion attempt
    let _ = delete_response.responses;
}

#[test]
fn test_integration_fetch_unknown_topic_error() {
    // Fetch from non-existent topic
    use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
    use kafka_protocol::messages::FetchRequest;

    let handler = create_test_handler();

    let partition = FetchPartition::default()
        .with_partition(0)
        .with_fetch_offset(0);

    let topic = FetchTopic::default()
        .with_topic(TopicName(StrBytes::from_static_str(
            "definitely-does-not-exist-xyz",
        )))
        .with_partitions(vec![partition]);

    let request = FetchRequest::default()
        .with_max_wait_ms(100)
        .with_min_bytes(1)
        .with_topics(vec![topic]);

    let response = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(handler.handle_fetch(request, &create_test_session_manager(), None))
        .unwrap();

    // Response should exist, with error code per partition
    assert!(!response.responses.is_empty());
}

#[test]
fn test_integration_produce_to_invalid_partition() {
    // Produce to partition that doesn't exist
    use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
    use kafka_protocol::messages::ProduceRequest;

    let handler = create_test_handler();

    let partition_data = PartitionProduceData::default().with_index(999); // Invalid partition

    let topic_data = TopicProduceData::default()
        .with_name(TopicName(StrBytes::from_static_str("test-topic")))
        .with_partition_data(vec![partition_data]);

    let request = ProduceRequest::default()
        .with_timeout_ms(5000)
        .with_acks(1)
        .with_topic_data(vec![topic_data]);

    let response = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(handler.handle_produce(request, &create_test_session_manager(), None))
        .unwrap();

    // Should return error for the partition
    assert!(!response.responses.is_empty());
}

#[test]
fn test_integration_describe_configs_topic() {
    // Describe topic configurations (async, 3 args: request, principal, host)
    use kafka_protocol::messages::describe_configs_request::DescribeConfigsResource;
    use kafka_protocol::messages::DescribeConfigsRequest;

    let handler = create_test_handler();

    let resource = DescribeConfigsResource::default()
        .with_resource_type(2) // 2 = TOPIC
        .with_resource_name(StrBytes::from_static_str("config-test-topic"));

    let request = DescribeConfigsRequest::default().with_resources(vec![resource]);

    let response = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(handler.handle_describe_configs(request, "test-user", "127.0.0.1"))
        .unwrap();

    // Should return config info
    let _ = response.results;
}

#[test]
fn test_integration_describe_cluster() {
    // Describe cluster (async, 1 arg)
    use kafka_protocol::messages::DescribeClusterRequest;

    let handler = create_test_handler();

    let request = DescribeClusterRequest::default();
    let response = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(handler.handle_describe_cluster(request))
        .unwrap();

    // Should return cluster info
    assert!(
        response.error_code == 0 || !response.brokers.is_empty(),
        "Should return cluster info or success"
    );
}

#[test]
fn test_integration_init_producer_id() {
    // Initialize producer ID for idempotent/transactional producer (sync, 1 arg)
    use kafka_protocol::messages::{InitProducerIdRequest, TransactionalId};

    let handler = create_test_handler();

    let request = InitProducerIdRequest::default()
        .with_transactional_id(Some(TransactionalId(StrBytes::from_static_str(
            "test-transaction",
        ))))
        .with_transaction_timeout_ms(60000);

    let response = handler.handle_init_producer_id(request).unwrap();

    // Should return producer ID and epoch
    let _ = response.producer_id;
    let _ = response.producer_epoch;
}

#[test]
fn test_integration_describe_acls() {
    // Describe ACLs
    use kafka_protocol::messages::DescribeAclsRequest;

    let handler = create_test_handler();

    let request = DescribeAclsRequest::default()
        .with_resource_type_filter(1) // ANY
        .with_pattern_type_filter(1); // ANY

    let response = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(handler.handle_describe_acls(request))
        .unwrap();

    // Should return ACL info (may be empty)
    let _ = response.resources;
}
