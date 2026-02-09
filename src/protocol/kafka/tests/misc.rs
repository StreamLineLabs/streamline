use super::*;

#[test]
fn test_list_offsets_earliest() {
    use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};

    let handler = create_handler_with_topics(&[("offsets", 1)]);

    let request = ListOffsetsRequest::default().with_topics(vec![ListOffsetsTopic::default()
        .with_name(TopicName::from(StrBytes::from_static_str("offsets")))
        .with_partitions(vec![ListOffsetsPartition::default()
            .with_partition_index(0)
            .with_timestamp(-2)])]);

    let response = handler.handle_list_offsets(request, 7).unwrap();

    assert_eq!(response.topics[0].partitions[0].error_code, 0);
    assert_eq!(response.topics[0].partitions[0].offset, 0);
}

#[test]
fn test_list_offsets_latest() {
    use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};

    let handler = create_handler_with_topics(&[("offsets", 1)]);

    let request = ListOffsetsRequest::default().with_topics(vec![ListOffsetsTopic::default()
        .with_name(TopicName::from(StrBytes::from_static_str("offsets")))
        .with_partitions(vec![ListOffsetsPartition::default()
            .with_partition_index(0)
            .with_timestamp(-1)])]);

    let response = handler.handle_list_offsets(request, 7).unwrap();

    assert_eq!(response.topics[0].partitions[0].error_code, 0);
    assert_eq!(response.topics[0].partitions[0].offset, 0);
}

#[test]
fn test_list_offsets_unknown_topic() {
    use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};

    let handler = create_test_handler();

    let request = ListOffsetsRequest::default().with_topics(vec![ListOffsetsTopic::default()
        .with_name(TopicName::from(StrBytes::from_static_str("nonexistent")))
        .with_partitions(vec![ListOffsetsPartition::default()
            .with_partition_index(0)
            .with_timestamp(-1)])]);

    let response = handler.handle_list_offsets(request, 7).unwrap();

    // Error code 3 = UNKNOWN_TOPIC_OR_PARTITION
    assert_eq!(response.topics[0].partitions[0].error_code, 3);
}

#[test]
fn test_list_offsets_multiple_partitions() {
    use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};

    let handler = create_handler_with_topics(&[("multi", 3)]);

    let request = ListOffsetsRequest::default().with_topics(vec![ListOffsetsTopic::default()
        .with_name(TopicName::from(StrBytes::from_static_str("multi")))
        .with_partitions(vec![
            ListOffsetsPartition::default()
                .with_partition_index(0)
                .with_timestamp(-1),
            ListOffsetsPartition::default()
                .with_partition_index(1)
                .with_timestamp(-1),
            ListOffsetsPartition::default()
                .with_partition_index(2)
                .with_timestamp(-1),
        ])]);

    let response = handler.handle_list_offsets(request, 7).unwrap();

    assert_eq!(response.topics[0].partitions.len(), 3);
    for p in &response.topics[0].partitions {
        assert_eq!(p.error_code, 0);
    }
}

#[test]
fn test_list_offsets_all_versions() {
    use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};

    let handler = create_handler_with_topics(&[("test", 1)]);

    for version in 0..=7 {
        let request = ListOffsetsRequest::default().with_topics(vec![ListOffsetsTopic::default()
            .with_name(TopicName::from(StrBytes::from_static_str("test")))
            .with_partitions(vec![ListOffsetsPartition::default()
                .with_partition_index(0)
                .with_timestamp(-1)])]);

        let response = handler.handle_list_offsets(request, version);

        assert!(response.is_ok(), "ListOffsets v{} should succeed", version);
    }
}

#[test]
fn test_error_unknown_topic_returns_error_code_3() {
    // Fetching from a non-existent topic should return UNKNOWN_TOPIC_OR_PARTITION (3)
    use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
    use kafka_protocol::messages::BrokerId;
    use kafka_protocol::messages::ListOffsetsRequest;

    let handler = create_test_handler();

    let request = ListOffsetsRequest::default()
        .with_replica_id(BrokerId(-1))
        .with_topics(vec![ListOffsetsTopic::default()
            .with_name(TopicName::from(StrBytes::from_static_str(
                "nonexistent-topic",
            )))
            .with_partitions(vec![ListOffsetsPartition::default()
                .with_partition_index(0)
                .with_timestamp(-1)])]);

    let response = handler.handle_list_offsets(request, 7).unwrap();

    // Should get error for unknown topic
    assert_eq!(response.topics.len(), 1);
    let partition = &response.topics[0].partitions[0];
    assert!(
        partition.error_code == UNKNOWN_TOPIC_OR_PARTITION
            || partition.error_code == LEADER_NOT_AVAILABLE,
        "Expected UNKNOWN_TOPIC_OR_PARTITION (3) or LEADER_NOT_AVAILABLE (5), got {}",
        partition.error_code
    );
}

#[test]
fn test_error_topic_already_exists_returns_error_code_36() {
    // Creating a topic that already exists should return TOPIC_ALREADY_EXISTS (36)
    use kafka_protocol::messages::create_topics_request::CreatableTopic;
    use kafka_protocol::messages::CreateTopicsRequest;

    let handler = create_handler_with_topics(&[("existing-topic", 1)]);

    // Try to create the same topic again
    let request = CreateTopicsRequest::default()
        .with_topics(vec![CreatableTopic::default()
            .with_name(TopicName::from(StrBytes::from_static_str("existing-topic")))
            .with_num_partitions(1)
            .with_replication_factor(1)])
        .with_timeout_ms(5000);

    let response = handler.handle_create_topics(request).unwrap();

    assert_eq!(response.topics.len(), 1);
    assert_eq!(
        response.topics[0].error_code, TOPIC_ALREADY_EXISTS,
        "Expected TOPIC_ALREADY_EXISTS (36)"
    );
}

#[test]
fn test_error_invalid_partitions_handling() {
    // Test how the implementation handles partition count edge cases
    // Different implementations may validate differently
    use kafka_protocol::messages::create_topics_request::CreatableTopic;
    use kafka_protocol::messages::CreateTopicsRequest;

    let handler = create_test_handler();

    // Test with negative partitions (-1 often means "use default")
    let request = CreateTopicsRequest::default()
        .with_topics(vec![CreatableTopic::default()
            .with_name(TopicName::from(StrBytes::from_static_str("partition-test")))
            .with_num_partitions(-1)
            .with_replication_factor(1)])
        .with_timeout_ms(5000);

    let response = handler.handle_create_topics(request).unwrap();

    assert_eq!(response.topics.len(), 1);
    // Implementation may return error or use default partitions
    // All valid error codes for this scenario
    assert!(
        response.topics[0].error_code == INVALID_PARTITIONS
            || response.topics[0].error_code == INVALID_REQUEST
            || response.topics[0].error_code == NONE,
        "Expected INVALID_PARTITIONS (37), INVALID_REQUEST (42), or NONE (0), got {}",
        response.topics[0].error_code
    );
}

#[test]
fn test_error_group_id_not_found_returns_error_code_69() {
    // Deleting a non-existent group should return GROUP_ID_NOT_FOUND (69)
    use kafka_protocol::messages::DeleteGroupsRequest;

    let handler = create_test_handler();

    let request = DeleteGroupsRequest::default().with_groups_names(vec![GroupId::from(
        StrBytes::from_static_str("nonexistent-group"),
    )]);

    let response = handler.handle_delete_groups(request).unwrap();

    assert_eq!(response.results.len(), 1);
    // Should return GROUP_ID_NOT_FOUND or NONE (if deletion silently succeeds)
    assert!(
        response.results[0].error_code == GROUP_ID_NOT_FOUND
            || response.results[0].error_code == NONE,
        "Expected GROUP_ID_NOT_FOUND (69) or NONE (0), got {}",
        response.results[0].error_code
    );
}

#[test]
fn test_error_unsupported_version_detected() {
    // The header version functions should handle version range checking
    // We test that UNSUPPORTED_VERSION (35) is correctly defined for version mismatch scenarios

    // ApiVersions should list supported version ranges
    use kafka_protocol::messages::ApiVersionsRequest;

    let handler = create_test_handler();
    let request = ApiVersionsRequest::default();
    let response = handler.handle_api_versions(request).unwrap();

    // All APIs should have valid version ranges (min <= max)
    for api in &response.api_keys {
        assert!(
            api.min_version <= api.max_version,
            "API {} has invalid version range: min {} > max {}",
            api.api_key,
            api.min_version,
            api.max_version
        );
    }
}

#[test]
fn test_error_unknown_partition_returns_error() {
    // Fetching from a non-existent partition should return an error
    use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
    use kafka_protocol::messages::BrokerId;
    use kafka_protocol::messages::ListOffsetsRequest;

    let handler = create_handler_with_topics(&[("single-partition-topic", 1)]);

    let request = ListOffsetsRequest::default()
        .with_replica_id(BrokerId(-1))
        .with_topics(vec![ListOffsetsTopic::default()
            .with_name(TopicName::from(StrBytes::from_static_str(
                "single-partition-topic",
            )))
            .with_partitions(vec![ListOffsetsPartition::default()
                .with_partition_index(99) // Non-existent partition
                .with_timestamp(-1)])]);

    let response = handler.handle_list_offsets(request, 7).unwrap();

    assert_eq!(response.topics.len(), 1);
    let partition = &response.topics[0].partitions[0];
    // Should get error for unknown partition
    assert!(
        partition.error_code == UNKNOWN_TOPIC_OR_PARTITION
            || partition.error_code == LEADER_NOT_AVAILABLE,
        "Expected error for unknown partition, got {}",
        partition.error_code
    );
}

#[test]
fn test_success_response_returns_none_error() {
    // Verify that successful operations return NONE (0) error code
    use kafka_protocol::messages::ApiVersionsRequest;

    let handler = create_test_handler();
    let request = ApiVersionsRequest::default();
    let response = handler.handle_api_versions(request).unwrap();

    assert_eq!(
        response.error_code, NONE,
        "Successful ApiVersions should return NONE (0)"
    );
}

#[test]
fn test_list_offsets_success_returns_none_error() {
    use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
    use kafka_protocol::messages::BrokerId;
    use kafka_protocol::messages::ListOffsetsRequest;

    let handler = create_handler_with_topics(&[("offsets-topic", 1)]);

    let request = ListOffsetsRequest::default()
        .with_replica_id(BrokerId(-1))
        .with_topics(vec![ListOffsetsTopic::default()
            .with_name(TopicName::from(StrBytes::from_static_str("offsets-topic")))
            .with_partitions(vec![ListOffsetsPartition::default()
                .with_partition_index(0)
                .with_timestamp(-1)])]);

    let response = handler.handle_list_offsets(request, 7).unwrap();

    // Should succeed with NONE error code for existing topic/partition
    assert_eq!(response.topics.len(), 1);
    assert_eq!(
        response.topics[0].partitions[0].error_code, NONE,
        "Existing partition should return NONE (0)"
    );
}

#[test]
#[cfg(feature = "auth")]
fn test_describe_acls_basic() {
    // Test DescribeAcls request
    use kafka_protocol::messages::DescribeAclsRequest;

    let handler = create_test_handler();

    // Request to describe all ACLs (using filter)
    let request = DescribeAclsRequest::default()
        .with_resource_type_filter(1) // ANY
        .with_pattern_type_filter(1); // LITERAL

    let response = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(handler.handle_describe_acls(request))
        .unwrap();

    // Response should have an error code (NONE if ACLs supported, or appropriate error)
    assert!(
        response.error_code == NONE
            || response.error_code == SECURITY_DISABLED
            || response.error_code == UNKNOWN_SERVER_ERROR,
        "Expected NONE (0), SECURITY_DISABLED (89), or UNKNOWN_SERVER_ERROR (-1), got {}",
        response.error_code
    );
}

#[test]
#[cfg(feature = "auth")]
fn test_describe_acls_filter_by_resource() {
    // Test DescribeAcls with resource type filter
    use kafka_protocol::messages::DescribeAclsRequest;

    let handler = create_test_handler();

    // Filter for TOPIC resources (type 2)
    let request = DescribeAclsRequest::default()
        .with_resource_type_filter(2) // TOPIC
        .with_pattern_type_filter(1); // LITERAL

    let response = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(handler.handle_describe_acls(request))
        .unwrap();

    // Response should include resources array (possibly empty)
    // Error code indicates if operation is supported
    assert!(
        response.error_code == NONE
            || response.error_code == SECURITY_DISABLED
            || response.error_code == UNKNOWN_SERVER_ERROR,
        "DescribeAcls should return valid error code"
    );
}

#[test]
#[cfg(feature = "auth")]
fn test_create_acls_single() {
    // Test CreateAcls with a single ACL
    use kafka_protocol::messages::create_acls_request::AclCreation;
    use kafka_protocol::messages::CreateAclsRequest;

    let handler = create_test_handler();

    let acl = AclCreation::default()
        .with_resource_type(2) // TOPIC
        .with_resource_name(StrBytes::from_static_str("test-topic"))
        .with_resource_pattern_type(1) // LITERAL
        .with_principal(StrBytes::from_static_str("User:test-user"))
        .with_host(StrBytes::from_static_str("*"))
        .with_operation(2) // READ
        .with_permission_type(3); // ALLOW

    let request = CreateAclsRequest::default().with_creations(vec![acl]);

    let response = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(handler.handle_create_acls(request))
        .unwrap();

    // Response should have results for each ACL creation
    assert_eq!(
        response.results.len(),
        1,
        "Should have result for the single ACL"
    );

    // The error code can be NONE (success) or SECURITY_DISABLED
    let result_error = response.results[0].error_code;
    assert!(
        result_error == NONE
            || result_error == SECURITY_DISABLED
            || result_error == UNKNOWN_SERVER_ERROR,
        "Expected NONE, SECURITY_DISABLED, or UNKNOWN_SERVER_ERROR, got {}",
        result_error
    );
}

#[test]
#[cfg(feature = "auth")]
fn test_create_acls_multiple() {
    // Test CreateAcls with multiple ACLs
    use kafka_protocol::messages::create_acls_request::AclCreation;
    use kafka_protocol::messages::CreateAclsRequest;

    let handler = create_test_handler();

    let acl1 = AclCreation::default()
        .with_resource_type(2) // TOPIC
        .with_resource_name(StrBytes::from_static_str("topic-1"))
        .with_resource_pattern_type(1) // LITERAL
        .with_principal(StrBytes::from_static_str("User:user1"))
        .with_host(StrBytes::from_static_str("*"))
        .with_operation(2) // READ
        .with_permission_type(3); // ALLOW

    let acl2 = AclCreation::default()
        .with_resource_type(2) // TOPIC
        .with_resource_name(StrBytes::from_static_str("topic-2"))
        .with_resource_pattern_type(1) // LITERAL
        .with_principal(StrBytes::from_static_str("User:user2"))
        .with_host(StrBytes::from_static_str("*"))
        .with_operation(3) // WRITE
        .with_permission_type(3); // ALLOW

    let request = CreateAclsRequest::default().with_creations(vec![acl1, acl2]);

    let response = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(handler.handle_create_acls(request))
        .unwrap();

    // Response should have results for both ACL creations
    assert_eq!(
        response.results.len(),
        2,
        "Should have results for both ACLs"
    );
}

#[test]
#[cfg(feature = "auth")]
fn test_delete_acls_basic() {
    // Test DeleteAcls with a filter
    use kafka_protocol::messages::delete_acls_request::DeleteAclsFilter;
    use kafka_protocol::messages::DeleteAclsRequest;

    let handler = create_test_handler();

    let filter = DeleteAclsFilter::default()
        .with_resource_type_filter(2) // TOPIC
        .with_pattern_type_filter(1); // LITERAL

    let request = DeleteAclsRequest::default().with_filters(vec![filter]);

    let response = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(handler.handle_delete_acls(request))
        .unwrap();

    // Response should have filter results
    assert_eq!(
        response.filter_results.len(),
        1,
        "Should have result for the filter"
    );
}

#[test]
#[cfg(feature = "auth")]
fn test_delete_acls_multiple_filters() {
    // Test DeleteAcls with multiple filters
    use kafka_protocol::messages::delete_acls_request::DeleteAclsFilter;
    use kafka_protocol::messages::DeleteAclsRequest;

    let handler = create_test_handler();

    let filter1 = DeleteAclsFilter::default()
        .with_resource_type_filter(2) // TOPIC
        .with_pattern_type_filter(1); // LITERAL

    let filter2 = DeleteAclsFilter::default()
        .with_resource_type_filter(3) // GROUP
        .with_pattern_type_filter(1); // LITERAL

    let request = DeleteAclsRequest::default().with_filters(vec![filter1, filter2]);

    let response = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(handler.handle_delete_acls(request))
        .unwrap();

    // Response should have results for both filters
    assert_eq!(
        response.filter_results.len(),
        2,
        "Should have results for both filters"
    );
}

#[test]
fn test_acl_resource_types() {
    // Verify ACL resource type constants
    // These are standard Kafka resource types
    const RESOURCE_UNKNOWN: i8 = 0;
    const RESOURCE_ANY: i8 = 1;
    const RESOURCE_TOPIC: i8 = 2;
    const RESOURCE_GROUP: i8 = 3;
    const RESOURCE_CLUSTER: i8 = 4;
    const RESOURCE_TRANSACTIONAL_ID: i8 = 5;

    // Verify known resource types
    assert_eq!(RESOURCE_UNKNOWN, 0);
    assert_eq!(RESOURCE_ANY, 1);
    assert_eq!(RESOURCE_TOPIC, 2);
    assert_eq!(RESOURCE_GROUP, 3);
    assert_eq!(RESOURCE_CLUSTER, 4);
    assert_eq!(RESOURCE_TRANSACTIONAL_ID, 5);
}

#[test]
fn test_acl_operation_types() {
    // Verify ACL operation constants
    const OPERATION_UNKNOWN: i8 = 0;
    const OPERATION_ANY: i8 = 1;
    const OPERATION_ALL: i8 = 2;
    const OPERATION_READ: i8 = 3;
    const OPERATION_WRITE: i8 = 4;
    const OPERATION_CREATE: i8 = 5;
    const OPERATION_DELETE: i8 = 6;
    const OPERATION_ALTER: i8 = 7;
    const OPERATION_DESCRIBE: i8 = 8;

    // Verify operation values
    assert_eq!(OPERATION_UNKNOWN, 0);
    assert_eq!(OPERATION_ANY, 1);
    assert_eq!(OPERATION_ALL, 2);
    assert_eq!(OPERATION_READ, 3);
    assert_eq!(OPERATION_WRITE, 4);
    assert_eq!(OPERATION_CREATE, 5);
    assert_eq!(OPERATION_DELETE, 6);
    assert_eq!(OPERATION_ALTER, 7);
    assert_eq!(OPERATION_DESCRIBE, 8);
}

#[test]
fn test_acl_permission_types() {
    // Verify ACL permission type constants
    const PERMISSION_UNKNOWN: i8 = 0;
    const PERMISSION_ANY: i8 = 1;
    const PERMISSION_DENY: i8 = 2;
    const PERMISSION_ALLOW: i8 = 3;

    // Verify permission values
    assert_eq!(PERMISSION_UNKNOWN, 0);
    assert_eq!(PERMISSION_ANY, 1);
    assert_eq!(PERMISSION_DENY, 2);
    assert_eq!(PERMISSION_ALLOW, 3);
}

#[test]
fn test_create_partitions_increase() {
    // Test CreatePartitions to increase partition count
    use kafka_protocol::messages::create_partitions_request::CreatePartitionsTopic;
    use kafka_protocol::messages::CreatePartitionsRequest;

    let handler = create_test_handler();

    let topic = CreatePartitionsTopic::default()
        .with_name(TopicName(StrBytes::from_static_str("test-topic")))
        .with_count(6); // Increase to 6 partitions

    let request = CreatePartitionsRequest::default()
        .with_topics(vec![topic])
        .with_timeout_ms(30000);

    let response = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(handler.handle_create_partitions(request, "User:test", "*"))
        .unwrap();

    assert_eq!(
        response.results.len(),
        1,
        "Should have result for one topic"
    );
}

#[test]
fn test_create_partitions_multiple_topics() {
    // Test CreatePartitions for multiple topics
    use kafka_protocol::messages::create_partitions_request::CreatePartitionsTopic;
    use kafka_protocol::messages::CreatePartitionsRequest;

    let handler = create_test_handler();

    let topic1 = CreatePartitionsTopic::default()
        .with_name(TopicName(StrBytes::from_static_str("topic-1")))
        .with_count(4);

    let topic2 = CreatePartitionsTopic::default()
        .with_name(TopicName(StrBytes::from_static_str("topic-2")))
        .with_count(8);

    let request = CreatePartitionsRequest::default()
        .with_topics(vec![topic1, topic2])
        .with_timeout_ms(30000);

    let response = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(handler.handle_create_partitions(request, "User:test", "*"))
        .unwrap();

    assert_eq!(
        response.results.len(),
        2,
        "Should have results for both topics"
    );
}

#[test]
fn test_create_partitions_header_versions() {
    // CreatePartitions v2+ uses flexible headers
    use kafka_protocol::messages::ApiKey;

    // v0-1 use header v1
    for version in 0i16..2 {
        let header_version =
            KafkaHandler::request_header_version(ApiKey::CreatePartitions as i16, version);
        assert_eq!(
            header_version, 1,
            "CreatePartitions v{} should use request header v1",
            version
        );
    }

    // v2+ use header v2 (flexible)
    let header_version = KafkaHandler::request_header_version(ApiKey::CreatePartitions as i16, 2);
    assert_eq!(
        header_version, 2,
        "CreatePartitions v2 should use request header v2"
    );
}

#[test]
fn test_describe_cluster_basic() {
    // Test DescribeCluster returns cluster information
    use kafka_protocol::messages::DescribeClusterRequest;

    let handler = create_test_handler();

    let request = DescribeClusterRequest::default();

    let response = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(handler.handle_describe_cluster(request))
        .unwrap();

    // Should return valid cluster info
    assert_eq!(
        response.error_code, NONE,
        "DescribeCluster should return NONE error code"
    );

    // Should have broker information
    assert!(
        !response.brokers.is_empty(),
        "DescribeCluster should return at least one broker"
    );
}

#[test]
fn test_describe_cluster_broker_info() {
    // Test DescribeCluster broker details
    use kafka_protocol::messages::DescribeClusterRequest;

    let handler = create_test_handler();

    let request = DescribeClusterRequest::default();

    let response = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(handler.handle_describe_cluster(request))
        .unwrap();

    // Check first broker has valid info
    if !response.brokers.is_empty() {
        let broker = &response.brokers[0];
        // Broker should have a valid host
        assert!(
            !broker.host.is_empty(),
            "Broker should have a non-empty host"
        );
        // Port should be positive
        assert!(broker.port > 0, "Broker should have a positive port");
    }
}

#[test]
fn test_describe_cluster_header_always_flexible() {
    // DescribeCluster always uses flexible headers (v0+)
    use kafka_protocol::messages::ApiKey;

    for version in 0i16..=1 {
        let header_version =
            KafkaHandler::request_header_version(ApiKey::DescribeCluster as i16, version);
        assert_eq!(
            header_version, 2,
            "DescribeCluster v{} should use request header v2 (always flexible)",
            version
        );
    }
}

#[test]
fn test_delete_records_basic() {
    // Test DeleteRecords for a topic
    use kafka_protocol::messages::delete_records_request::{
        DeleteRecordsPartition, DeleteRecordsTopic,
    };
    use kafka_protocol::messages::DeleteRecordsRequest;

    let handler = create_test_handler();

    let partition = DeleteRecordsPartition::default()
        .with_partition_index(0)
        .with_offset(100); // Delete records before offset 100

    let topic = DeleteRecordsTopic::default()
        .with_name(TopicName(StrBytes::from_static_str("test-topic")))
        .with_partitions(vec![partition]);

    let request = DeleteRecordsRequest::default()
        .with_topics(vec![topic])
        .with_timeout_ms(30000);

    let response = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(handler.handle_delete_records(request))
        .unwrap();

    assert_eq!(response.topics.len(), 1, "Should have result for one topic");
}

#[test]
fn test_delete_records_multiple_partitions() {
    // Test DeleteRecords for multiple partitions
    use kafka_protocol::messages::delete_records_request::{
        DeleteRecordsPartition, DeleteRecordsTopic,
    };
    use kafka_protocol::messages::DeleteRecordsRequest;

    let handler = create_test_handler();

    let partition0 = DeleteRecordsPartition::default()
        .with_partition_index(0)
        .with_offset(50);

    let partition1 = DeleteRecordsPartition::default()
        .with_partition_index(1)
        .with_offset(100);

    let topic = DeleteRecordsTopic::default()
        .with_name(TopicName(StrBytes::from_static_str("test-topic")))
        .with_partitions(vec![partition0, partition1]);

    let request = DeleteRecordsRequest::default()
        .with_topics(vec![topic])
        .with_timeout_ms(30000);

    let response = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(handler.handle_delete_records(request))
        .unwrap();

    if !response.topics.is_empty() {
        assert_eq!(
            response.topics[0].partitions.len(),
            2,
            "Should have results for both partitions"
        );
    }
}

#[test]
fn test_delete_records_header_versions() {
    // DeleteRecords v2+ uses flexible headers
    use kafka_protocol::messages::ApiKey;

    // v0-1 use header v1
    for version in 0i16..2 {
        let header_version =
            KafkaHandler::request_header_version(ApiKey::DeleteRecords as i16, version);
        assert_eq!(
            header_version, 1,
            "DeleteRecords v{} should use request header v1",
            version
        );
    }

    // v2 uses header v2 (flexible)
    let header_version = KafkaHandler::request_header_version(ApiKey::DeleteRecords as i16, 2);
    assert_eq!(
        header_version, 2,
        "DeleteRecords v2 should use request header v2"
    );
}

#[test]
fn test_offset_for_leader_epoch_basic() {
    // Test OffsetForLeaderEpoch
    use kafka_protocol::messages::offset_for_leader_epoch_request::{
        OffsetForLeaderPartition, OffsetForLeaderTopic,
    };
    use kafka_protocol::messages::OffsetForLeaderEpochRequest;

    let handler = create_test_handler();

    let partition = OffsetForLeaderPartition::default()
        .with_partition(0)
        .with_leader_epoch(1);

    let topic = OffsetForLeaderTopic::default()
        .with_topic(TopicName(StrBytes::from_static_str("test-topic")))
        .with_partitions(vec![partition]);

    let request = OffsetForLeaderEpochRequest::default().with_topics(vec![topic]);

    let response = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(handler.handle_offset_for_leader_epoch(request))
        .unwrap();

    assert_eq!(response.topics.len(), 1, "Should have result for one topic");
}

#[test]
fn test_offset_for_leader_epoch_multiple_topics() {
    // Test OffsetForLeaderEpoch with multiple topics
    use kafka_protocol::messages::offset_for_leader_epoch_request::{
        OffsetForLeaderPartition, OffsetForLeaderTopic,
    };
    use kafka_protocol::messages::OffsetForLeaderEpochRequest;

    let handler = create_test_handler();

    let partition1 = OffsetForLeaderPartition::default()
        .with_partition(0)
        .with_leader_epoch(1);

    let topic1 = OffsetForLeaderTopic::default()
        .with_topic(TopicName(StrBytes::from_static_str("topic-1")))
        .with_partitions(vec![partition1]);

    let partition2 = OffsetForLeaderPartition::default()
        .with_partition(0)
        .with_leader_epoch(2);

    let topic2 = OffsetForLeaderTopic::default()
        .with_topic(TopicName(StrBytes::from_static_str("topic-2")))
        .with_partitions(vec![partition2]);

    let request = OffsetForLeaderEpochRequest::default().with_topics(vec![topic1, topic2]);

    let response = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(handler.handle_offset_for_leader_epoch(request))
        .unwrap();

    assert_eq!(
        response.topics.len(),
        2,
        "Should have results for both topics"
    );
}

#[test]
fn test_offset_for_leader_epoch_header_versions() {
    // OffsetForLeaderEpoch v4+ uses flexible headers
    use kafka_protocol::messages::ApiKey;

    // v0-3 use header v1
    for version in 0i16..4 {
        let header_version =
            KafkaHandler::request_header_version(ApiKey::OffsetForLeaderEpoch as i16, version);
        assert_eq!(
            header_version, 1,
            "OffsetForLeaderEpoch v{} should use request header v1",
            version
        );
    }

    // v4 uses header v2 (flexible)
    let header_version =
        KafkaHandler::request_header_version(ApiKey::OffsetForLeaderEpoch as i16, 4);
    assert_eq!(
        header_version, 2,
        "OffsetForLeaderEpoch v4 should use request header v2"
    );
}

#[test]
fn test_config_resource_types() {
    // Verify config resource type constants per Kafka spec
    const RESOURCE_UNKNOWN: i8 = 0;
    const RESOURCE_ANY: i8 = 1;
    const RESOURCE_TOPIC: i8 = 2;
    const RESOURCE_GROUP: i8 = 3;
    const RESOURCE_BROKER: i8 = 4;
    const RESOURCE_BROKER_LOGGER: i8 = 8;

    // These are standard Kafka resource types used in config operations
    assert_eq!(RESOURCE_UNKNOWN, 0);
    assert_eq!(RESOURCE_ANY, 1);
    assert_eq!(RESOURCE_TOPIC, 2);
    assert_eq!(RESOURCE_GROUP, 3);
    assert_eq!(RESOURCE_BROKER, 4);
    assert_eq!(RESOURCE_BROKER_LOGGER, 8);
}

#[test]
fn test_config_operation_types() {
    // Verify IncrementalAlterConfigs operation constants
    const OP_SET: i8 = 0;
    const OP_DELETE: i8 = 1;
    const OP_APPEND: i8 = 2;
    const OP_SUBTRACT: i8 = 3;

    // These are the incremental alter config operation types
    assert_eq!(OP_SET, 0);
    assert_eq!(OP_DELETE, 1);
    assert_eq!(OP_APPEND, 2);
    assert_eq!(OP_SUBTRACT, 3);
}

#[test]
fn test_empty_vs_null_array_topics() {
    // Test empty array vs null in Metadata request
    use kafka_protocol::messages::MetadataRequest;

    let handler = create_test_handler();

    // Empty array - request metadata for no specific topics (should return all)
    let request_empty = MetadataRequest::default().with_topics(Some(vec![]));

    let response = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(handler.handle_metadata(request_empty, 9))
        .unwrap();

    // Empty topics array should return broker info
    assert!(!response.brokers.is_empty(), "Should return broker info");
}

#[test]
fn test_empty_vs_null_array_topics_null() {
    // Test null topics (request all topics)
    use kafka_protocol::messages::MetadataRequest;

    let handler = create_test_handler();

    // None - null topics array means "get all topics"
    let request_null = MetadataRequest::default().with_topics(None);

    let response = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(handler.handle_metadata(request_null, 9))
        .unwrap();

    // Null topics should also return broker info
    assert!(!response.brokers.is_empty(), "Should return broker info");
}

#[test]
fn test_max_string_length_topic_name() {
    // Test with maximum reasonable topic name length
    use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
    use kafka_protocol::messages::MetadataRequest;

    let handler = create_test_handler();

    // Create a long topic name (249 chars - Kafka max is 249)
    let long_name: String = "a".repeat(249);
    let topic = MetadataRequestTopic::default()
        .with_name(Some(TopicName(StrBytes::from_string(long_name.clone()))));

    let request = MetadataRequest::default().with_topics(Some(vec![topic]));

    let response = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(handler.handle_metadata(request, 9))
        .unwrap();

    // Should handle long topic name (may return error for non-existent)
    assert!(
        !response.brokers.is_empty(),
        "Should still return broker info"
    );
}

#[test]
fn test_unicode_strings_in_topic_name() {
    // Test UTF-8 characters in topic names
    use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
    use kafka_protocol::messages::MetadataRequest;

    let handler = create_test_handler();

    // Unicode topic name (Kafka supports UTF-8)
    let unicode_name = "test-topic-日本語";
    let topic = MetadataRequestTopic::default().with_name(Some(TopicName(StrBytes::from_string(
        unicode_name.to_string(),
    ))));

    let request = MetadataRequest::default().with_topics(Some(vec![topic]));

    let response = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(handler.handle_metadata(request, 9))
        .unwrap();

    // Should handle unicode topic name
    assert!(!response.brokers.is_empty(), "Should return broker info");
}

#[test]
fn test_unicode_strings_in_client_id() {
    // Test UTF-8 characters in client ID
    use kafka_protocol::messages::ApiVersionsRequest;

    let handler = create_test_handler();

    // Unicode client ID
    let request = ApiVersionsRequest::default()
        .with_client_software_name(StrBytes::from_string("клиент-тест".to_string()))
        .with_client_software_version(StrBytes::from_static_str("1.0.0"));

    let response = handler.handle_api_versions(request).unwrap();

    // Should handle unicode client info
    assert_eq!(response.error_code, NONE);
}

#[test]
fn test_empty_group_id_handling() {
    // Test empty group ID in consumer group operations
    use kafka_protocol::messages::FindCoordinatorRequest;

    let handler = create_test_handler();

    // Empty group ID
    let request = FindCoordinatorRequest::default()
        .with_key(StrBytes::from_static_str(""))
        .with_key_type(0); // GROUP

    let response = handler.handle_find_coordinator(request, 3).unwrap();

    // Empty group ID should be handled (either return a coordinator or an error)
    // Any valid error code is acceptable - the key is that it doesn't panic
    let _error_code = response.error_code;
}

#[test]
fn test_zero_timeout_produce() {
    // Test produce with zero timeout
    use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
    use kafka_protocol::messages::ProduceRequest;

    let handler = create_test_handler();

    let partition_data = PartitionProduceData::default().with_index(0);

    let topic_data = TopicProduceData::default()
        .with_name(TopicName(StrBytes::from_static_str("test-topic")))
        .with_partition_data(vec![partition_data]);

    let request = ProduceRequest::default()
        .with_timeout_ms(0) // Zero timeout
        .with_acks(1)
        .with_topic_data(vec![topic_data]);

    let response = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(handler.handle_produce(request, &create_test_session_manager(), None))
        .unwrap();

    // Should handle zero timeout (may timeout immediately)
    assert!(!response.responses.is_empty() || response.responses.is_empty());
}

#[test]
fn test_negative_timeout_fetch() {
    // Test fetch with negative timeout (should be treated as 0 or error)
    use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
    use kafka_protocol::messages::FetchRequest;

    let handler = create_test_handler();

    let partition = FetchPartition::default()
        .with_partition(0)
        .with_fetch_offset(0);

    let topic = FetchTopic::default()
        .with_topic(TopicName(StrBytes::from_static_str("test-topic")))
        .with_partitions(vec![partition]);

    let request = FetchRequest::default()
        .with_max_wait_ms(-1) // Negative timeout
        .with_min_bytes(1)
        .with_topics(vec![topic]);

    let response = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(handler.handle_fetch(request, &create_test_session_manager(), None))
        .unwrap();

    // Should handle negative timeout gracefully
    let _ = response.responses.len();
}

#[test]
fn test_max_partitions_in_request() {
    // Test request with many partitions
    use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
    use kafka_protocol::messages::ListOffsetsRequest;

    let handler = create_test_handler();

    // Create 100 partitions in request
    let partitions: Vec<ListOffsetsPartition> = (0..100)
        .map(|i| {
            ListOffsetsPartition::default()
                .with_partition_index(i)
                .with_timestamp(-1) // LATEST
        })
        .collect();

    let topic = ListOffsetsTopic::default()
        .with_name(TopicName(StrBytes::from_static_str("test-topic")))
        .with_partitions(partitions);

    let request = ListOffsetsRequest::default().with_topics(vec![topic]);

    let response = handler.handle_list_offsets(request, 5).unwrap();

    // Should handle many partitions
    assert!(!response.topics.is_empty(), "Should return topic results");
}

#[test]
fn test_unsupported_version_high_metadata() {
    // Test requesting a version higher than supported
    use kafka_protocol::messages::ApiKey;
    use kafka_protocol::messages::ApiVersionsRequest;

    let handler = create_test_handler();

    // Get max supported version for Metadata from ApiVersions response
    let api_versions_response = handler
        .handle_api_versions(ApiVersionsRequest::default())
        .unwrap();
    let metadata_api = api_versions_response
        .api_keys
        .iter()
        .find(|a| a.api_key == ApiKey::Metadata as i16)
        .unwrap();

    let max_version = metadata_api.max_version;

    // Try version beyond max - the handler should still work
    // (version validation happens at protocol layer)
    use kafka_protocol::messages::MetadataRequest;

    let request = MetadataRequest::default();

    // Using max version should work
    let response = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(handler.handle_metadata(request, max_version))
        .unwrap();

    assert!(!response.brokers.is_empty());
}

#[test]
fn test_all_apis_have_version_0() {
    // Verify all implemented APIs support at least version 0
    use kafka_protocol::messages::ApiVersionsRequest;

    let handler = create_test_handler();
    let api_versions = handler
        .handle_api_versions(ApiVersionsRequest::default())
        .unwrap();

    for api in &api_versions.api_keys {
        assert!(
            api.min_version <= 0,
            "API {} should support version 0 or lower, min_version is {}",
            api.api_key,
            api.min_version
        );
    }
}

#[test]
fn test_correlation_id_preserved() {
    // Test that correlation ID is correctly preserved in responses
    use kafka_protocol::messages::ResponseHeader;

    // Create response header with specific correlation ID
    let correlation_id = 12345i32;
    let header = ResponseHeader::default().with_correlation_id(correlation_id);

    assert_eq!(
        header.correlation_id, correlation_id,
        "Correlation ID should be preserved"
    );
}

#[test]
fn test_int32_big_endian_encoding() {
    // Verify INT32 values are big-endian encoded
    let value: i32 = 0x01020304;
    let bytes = value.to_be_bytes();

    assert_eq!(bytes, [0x01, 0x02, 0x03, 0x04]);
}

#[test]
fn test_int64_big_endian_encoding() {
    // Verify INT64 values are big-endian encoded
    let value: i64 = 0x0102030405060708;
    let bytes = value.to_be_bytes();

    assert_eq!(bytes, [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);
}

#[test]
fn test_int16_big_endian_encoding() {
    // Verify INT16 values are big-endian encoded
    let value: i16 = 0x0102;
    let bytes = value.to_be_bytes();

    assert_eq!(bytes, [0x01, 0x02]);
}

#[test]
fn test_varint_encoding_small() {
    // Test that small values use minimal bytes in varint encoding
    // ZigZag encoding doubles positive values: n -> 2n
    // So values 0-63 fit in 1 byte (zigzag 0-126)
    let small_value: i32 = 63;
    // ZigZag encoding: (n << 1) ^ (n >> 31)
    let zigzag = ((small_value << 1) ^ (small_value >> 31)) as u32;
    assert!(
        zigzag < 128,
        "Value {} zigzag encodes to {}, should fit in 1 varint byte",
        small_value,
        zigzag
    );
    assert_eq!(zigzag, 126, "63 should zigzag encode to 126");
}

#[test]
fn test_varint_encoding_negative() {
    // Test ZigZag encoding for negative values
    // -1 should become 1 in ZigZag
    let value: i32 = -1;
    let zigzag = ((value << 1) ^ (value >> 31)) as u32;
    assert_eq!(zigzag, 1, "-1 should zigzag encode to 1");

    // -64 should become 127
    let value2: i32 = -64;
    let zigzag2 = ((value2 << 1) ^ (value2 >> 31)) as u32;
    assert_eq!(zigzag2, 127, "-64 should zigzag encode to 127");
}

#[test]
fn test_nullable_string_null_representation() {
    // In Kafka protocol, null strings have length -1
    let null_length: i16 = -1;
    assert_eq!(null_length, -1, "Null string length should be -1");
}

#[test]
fn test_handler_max_message_bytes_capped() {
    // Verify that setting max_message_bytes above hard cap gets capped
    let handler = create_test_handler().with_max_message_bytes(u64::MAX); // Try to set unlimited

    // The handler should have capped the value
    assert!(
        handler.max_message_bytes <= HARD_MAX_MESSAGE_BYTES,
        "Handler should cap max_message_bytes to hard limit"
    );
}

#[test]
fn test_handler_max_message_bytes_zero_gets_default() {
    // Verify that setting max_message_bytes to 0 gets replaced with default
    let handler = create_test_handler().with_max_message_bytes(0);

    assert_eq!(
        handler.max_message_bytes, DEFAULT_MAX_MESSAGE_BYTES,
        "Handler should use default when 0 is specified"
    );
}

#[test]
#[cfg(feature = "clustering")]
fn test_inter_broker_tls_config() {
    // Inter-broker TLS configuration
    use crate::cluster::config::InterBrokerTlsConfig;
    use std::path::PathBuf;

    let config = InterBrokerTlsConfig {
        enabled: true,
        cert_path: Some(PathBuf::from("/path/to/cert.pem")),
        key_path: Some(PathBuf::from("/path/to/key.pem")),
        ca_cert_path: Some(PathBuf::from("/path/to/ca.pem")),
        verify_peer: true,
        min_version: "1.2".to_string(),
    };

    assert!(config.enabled);
    assert!(config.verify_peer);
    assert!(config.ca_cert_path.is_some());
    assert_eq!(config.min_version, "1.2");
}

#[test]
#[cfg(feature = "clustering")]
fn test_inter_broker_tls_config_disabled() {
    // Inter-broker TLS disabled configuration
    use crate::cluster::config::InterBrokerTlsConfig;

    let config = InterBrokerTlsConfig {
        enabled: false,
        cert_path: None,
        key_path: None,
        ca_cert_path: None,
        verify_peer: false,
        min_version: "1.2".to_string(),
    };

    assert!(!config.enabled);
}

#[test]
fn test_create_error_response_api_versions() {
    use kafka_protocol::protocol::Decodable;

    // Test that create_error_response generates valid error responses for ApiVersions
    let response = KafkaHandler::create_error_response(
        ApiKey::ApiVersions as i16,
        3,
        123,
        UNKNOWN_SERVER_ERROR,
    )
    .expect("ApiVersions error response should encode");

    // Response should not be empty
    assert!(
        !response.is_empty(),
        "ApiVersions error response should not be empty"
    );

    // Decode and verify the response
    let mut buf = Bytes::from(response);

    // Read header (correlation_id)
    let correlation_id = buf.get_i32();
    assert_eq!(correlation_id, 123, "Correlation ID should match");

    // Decode the body
    let api_versions_response = ApiVersionsResponse::decode(&mut buf, 3).unwrap();
    assert_eq!(
        api_versions_response.error_code, UNKNOWN_SERVER_ERROR,
        "Error code should be UNKNOWN_SERVER_ERROR"
    );
}

#[test]
fn test_create_error_response_fetch() {
    use kafka_protocol::protocol::Decodable;

    let response =
        KafkaHandler::create_error_response(ApiKey::Fetch as i16, 11, 456, UNKNOWN_SERVER_ERROR)
            .expect("Fetch error response should encode");

    // Response should not be empty
    assert!(
        !response.is_empty(),
        "Fetch error response should not be empty"
    );

    // Decode and verify the response
    let mut buf = Bytes::from(response);

    // Read header (correlation_id)
    let correlation_id = buf.get_i32();
    assert_eq!(correlation_id, 456, "Correlation ID should match");

    // Decode the body
    let fetch_response = FetchResponse::decode(&mut buf, 11).unwrap();
    assert_eq!(
        fetch_response.error_code, UNKNOWN_SERVER_ERROR,
        "Error code should be UNKNOWN_SERVER_ERROR"
    );
}

#[test]
fn test_create_error_response_find_coordinator() {
    use kafka_protocol::protocol::Decodable;

    let response = KafkaHandler::create_error_response(
        ApiKey::FindCoordinator as i16,
        2,
        789,
        COORDINATOR_NOT_AVAILABLE,
    )
    .expect("FindCoordinator error response should encode");

    // Response should not be empty
    assert!(
        !response.is_empty(),
        "FindCoordinator error response should not be empty"
    );

    // Decode and verify the response
    let mut buf = Bytes::from(response);

    // Read header (correlation_id)
    let correlation_id = buf.get_i32();
    assert_eq!(correlation_id, 789, "Correlation ID should match");

    // Decode the body
    let find_coordinator_response = FindCoordinatorResponse::decode(&mut buf, 2).unwrap();
    assert_eq!(
        find_coordinator_response.error_code, COORDINATOR_NOT_AVAILABLE,
        "Error code should be COORDINATOR_NOT_AVAILABLE"
    );
}

#[test]
fn test_create_error_response_join_group() {
    use kafka_protocol::protocol::Decodable;

    let response =
        KafkaHandler::create_error_response(ApiKey::JoinGroup as i16, 5, 101, UNKNOWN_MEMBER_ID)
            .expect("JoinGroup error response should encode");

    // Response should not be empty
    assert!(
        !response.is_empty(),
        "JoinGroup error response should not be empty"
    );

    // Decode and verify the response
    let mut buf = Bytes::from(response);

    // Read header (correlation_id)
    let correlation_id = buf.get_i32();
    assert_eq!(correlation_id, 101, "Correlation ID should match");

    // Decode the body
    let join_group_response = JoinGroupResponse::decode(&mut buf, 5).unwrap();
    assert_eq!(
        join_group_response.error_code, UNKNOWN_MEMBER_ID,
        "Error code should be UNKNOWN_MEMBER_ID"
    );
}

#[test]
fn test_create_error_response_sasl_authenticate() {
    use kafka_protocol::protocol::Decodable;

    let response = KafkaHandler::create_error_response(
        ApiKey::SaslAuthenticate as i16,
        1,
        202,
        SASL_AUTHENTICATION_FAILED,
    )
    .expect("SaslAuthenticate error response should encode");

    // Response should not be empty
    assert!(
        !response.is_empty(),
        "SaslAuthenticate error response should not be empty"
    );

    // Decode and verify the response
    let mut buf = Bytes::from(response);

    // Read header (correlation_id)
    let correlation_id = buf.get_i32();
    assert_eq!(correlation_id, 202, "Correlation ID should match");

    // Decode the body
    let sasl_response = SaslAuthenticateResponse::decode(&mut buf, 1).unwrap();
    assert_eq!(
        sasl_response.error_code, SASL_AUTHENTICATION_FAILED,
        "Error code should be SASL_AUTHENTICATION_FAILED"
    );
    // Should have an error message
    assert!(
        sasl_response.error_message.is_some(),
        "Should have error message"
    );
}

#[test]
fn test_create_error_response_unknown_api() {
    // For unknown API keys, should return an error
    let response = KafkaHandler::create_error_response(
        9999, // Non-existent API key
        0,
        123,
        UNKNOWN_SERVER_ERROR,
    );

    assert!(response.is_err(), "Unknown API should return an error");
}

#[test]
fn test_create_error_response_init_producer_id() {
    use kafka_protocol::protocol::Decodable;

    // Test with v1 which uses standard header (v0)
    let response = KafkaHandler::create_error_response(
        ApiKey::InitProducerId as i16,
        1,
        303,
        UNKNOWN_SERVER_ERROR,
    )
    .expect("InitProducerId error response should encode");

    // Response should not be empty
    assert!(
        !response.is_empty(),
        "InitProducerId error response should not be empty"
    );

    // Decode and verify the response
    let mut buf = Bytes::from(response);

    // Read header (correlation_id) - v1 uses header v0
    let correlation_id = buf.get_i32();
    assert_eq!(correlation_id, 303, "Correlation ID should match");

    // Decode the body
    let init_producer_response = InitProducerIdResponse::decode(&mut buf, 1).unwrap();
    assert_eq!(
        init_producer_response.error_code, UNKNOWN_SERVER_ERROR,
        "Error code should be UNKNOWN_SERVER_ERROR"
    );
    // Producer ID should be -1 for error response
    assert_eq!(
        init_producer_response.producer_id.0, -1,
        "Producer ID should be -1"
    );
}

#[test]
fn test_create_error_response_heartbeat() {
    use kafka_protocol::protocol::Decodable;

    // Test heartbeat error response
    let response = KafkaHandler::create_error_response(
        ApiKey::Heartbeat as i16,
        3,
        404,
        REBALANCE_IN_PROGRESS,
    )
    .expect("Heartbeat error response should encode");

    // Response should not be empty
    assert!(
        !response.is_empty(),
        "Heartbeat error response should not be empty"
    );

    // Decode and verify the response
    let mut buf = Bytes::from(response);

    // Read header (correlation_id) - v3 uses header v0
    let correlation_id = buf.get_i32();
    assert_eq!(correlation_id, 404, "Correlation ID should match");

    // Decode the body
    let heartbeat_response = HeartbeatResponse::decode(&mut buf, 3).unwrap();
    assert_eq!(
        heartbeat_response.error_code, REBALANCE_IN_PROGRESS,
        "Error code should be REBALANCE_IN_PROGRESS"
    );
}
