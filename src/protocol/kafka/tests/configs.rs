use super::*;

#[test]
fn test_describe_configs_topic_resource() {
    // Test DescribeConfigs for topic resources
    use kafka_protocol::messages::describe_configs_request::DescribeConfigsResource;
    use kafka_protocol::messages::DescribeConfigsRequest;

    let handler = create_test_handler();

    // Resource type 2 = TOPIC
    let resource = DescribeConfigsResource::default()
        .with_resource_type(2)
        .with_resource_name(StrBytes::from_static_str("test-topic"));

    let request = DescribeConfigsRequest::default().with_resources(vec![resource]);

    let response = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(handler.handle_describe_configs(request, "User:test", "*"))
        .unwrap();

    // Should have one result
    assert_eq!(
        response.results.len(),
        1,
        "Should have result for one resource"
    );
}

#[test]
fn test_describe_configs_broker_resource() {
    // Test DescribeConfigs for broker resources
    use kafka_protocol::messages::describe_configs_request::DescribeConfigsResource;
    use kafka_protocol::messages::DescribeConfigsRequest;

    let handler = create_test_handler();

    // Resource type 4 = BROKER
    let resource = DescribeConfigsResource::default()
        .with_resource_type(4)
        .with_resource_name(StrBytes::from_static_str("0")); // Broker ID as string

    let request = DescribeConfigsRequest::default().with_resources(vec![resource]);

    let response = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(handler.handle_describe_configs(request, "User:test", "*"))
        .unwrap();

    // Should have one result
    assert_eq!(
        response.results.len(),
        1,
        "Should have result for broker resource"
    );
}

#[test]
fn test_describe_configs_multiple_resources() {
    // Test DescribeConfigs with multiple resources
    use kafka_protocol::messages::describe_configs_request::DescribeConfigsResource;
    use kafka_protocol::messages::DescribeConfigsRequest;

    let handler = create_test_handler();

    let resource1 = DescribeConfigsResource::default()
        .with_resource_type(2) // TOPIC
        .with_resource_name(StrBytes::from_static_str("topic-1"));

    let resource2 = DescribeConfigsResource::default()
        .with_resource_type(2) // TOPIC
        .with_resource_name(StrBytes::from_static_str("topic-2"));

    let request = DescribeConfigsRequest::default().with_resources(vec![resource1, resource2]);

    let response = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(handler.handle_describe_configs(request, "User:test", "*"))
        .unwrap();

    // Should have results for both resources
    assert_eq!(
        response.results.len(),
        2,
        "Should have results for both resources"
    );
}

#[test]
fn test_describe_configs_header_versions() {
    // DescribeConfigs v4+ uses flexible headers
    use kafka_protocol::messages::ApiKey;

    // v0-3 use header v1
    for version in 0i16..4 {
        let header_version =
            KafkaHandler::request_header_version(ApiKey::DescribeConfigs as i16, version);
        assert_eq!(
            header_version, 1,
            "DescribeConfigs v{} should use request header v1",
            version
        );
    }

    // v4+ use header v2 (flexible)
    let header_version = KafkaHandler::request_header_version(ApiKey::DescribeConfigs as i16, 4);
    assert_eq!(
        header_version, 2,
        "DescribeConfigs v4 should use request header v2"
    );
}

#[test]
fn test_describe_configs_returns_storage_mode() {
    // Test that DescribeConfigs returns storage.mode config
    use kafka_protocol::messages::create_topics_request::{CreatableTopic, CreateableTopicConfig};
    use kafka_protocol::messages::describe_configs_request::DescribeConfigsResource;
    use kafka_protocol::messages::DescribeConfigsRequest;

    let handler = create_test_handler();

    // First create a topic with diskless storage mode
    let create_request =
        CreateTopicsRequest::default().with_topics(vec![CreatableTopic::default()
            .with_name(TopicName::from(StrBytes::from_static_str(
                "describe-storage-mode-topic",
            )))
            .with_num_partitions(1)
            .with_replication_factor(1)
            .with_configs(vec![CreateableTopicConfig::default()
                .with_name(StrBytes::from_static_str("storage.mode"))
                .with_value(Some(StrBytes::from_static_str("diskless")))])]);

    let create_response = handler.handle_create_topics(create_request).unwrap();
    assert_eq!(create_response.topics[0].error_code, 0);

    // Now describe the topic configs
    let resource = DescribeConfigsResource::default()
        .with_resource_type(2) // TOPIC
        .with_resource_name(StrBytes::from_static_str("describe-storage-mode-topic"));

    let describe_request = DescribeConfigsRequest::default().with_resources(vec![resource]);

    let response = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(handler.handle_describe_configs(describe_request, "User:test", "*"))
        .unwrap();

    assert_eq!(response.results.len(), 1);
    assert_eq!(response.results[0].error_code, 0);

    // Find storage.mode config in the response
    let storage_mode_config = response.results[0]
        .configs
        .iter()
        .find(|c| c.name.as_str() == "storage.mode");

    assert!(
        storage_mode_config.is_some(),
        "storage.mode should be in DescribeConfigs response"
    );
    let storage_mode_config = storage_mode_config.unwrap();
    assert_eq!(
        storage_mode_config.value.as_ref().unwrap().as_str(),
        "diskless"
    );
    assert!(
        storage_mode_config.read_only,
        "storage.mode should be read-only"
    );
    assert!(
        !storage_mode_config.is_default,
        "diskless is not the default storage mode"
    );
}

#[test]
fn test_describe_configs_returns_all_topic_configs() {
    // Test that DescribeConfigs returns all topic configs
    use kafka_protocol::messages::describe_configs_request::DescribeConfigsResource;
    use kafka_protocol::messages::DescribeConfigsRequest;

    let handler = create_handler_with_topics(&[("all-configs-topic", 1)]);

    // Resource type 2 = TOPIC
    let resource = DescribeConfigsResource::default()
        .with_resource_type(2)
        .with_resource_name(StrBytes::from_static_str("all-configs-topic"));

    let request = DescribeConfigsRequest::default().with_resources(vec![resource]);

    let response = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(handler.handle_describe_configs(request, "User:test", "*"))
        .unwrap();

    assert_eq!(response.results.len(), 1);
    assert_eq!(response.results[0].error_code, 0);

    // Check that all expected configs are returned
    let config_names: Vec<&str> = response.results[0]
        .configs
        .iter()
        .map(|c| c.name.as_str())
        .collect();

    assert!(
        config_names.contains(&"retention.ms"),
        "Should include retention.ms"
    );
    assert!(
        config_names.contains(&"retention.bytes"),
        "Should include retention.bytes"
    );
    assert!(
        config_names.contains(&"segment.bytes"),
        "Should include segment.bytes"
    );
    assert!(
        config_names.contains(&"cleanup.policy"),
        "Should include cleanup.policy"
    );
    assert!(
        config_names.contains(&"storage.mode"),
        "Should include storage.mode"
    );
    assert!(
        config_names.contains(&"message.ttl.ms"),
        "Should include message.ttl.ms"
    );
    assert!(
        config_names.contains(&"min.cleanable.dirty.ratio"),
        "Should include min.cleanable.dirty.ratio"
    );
    assert!(
        config_names.contains(&"delete.retention.ms"),
        "Should include delete.retention.ms"
    );
    assert!(
        config_names.contains(&"min.compaction.lag.ms"),
        "Should include min.compaction.lag.ms"
    );
}

#[test]
fn test_alter_configs_topic() {
    // Test AlterConfigs for topic configuration
    use kafka_protocol::messages::alter_configs_request::{AlterConfigsResource, AlterableConfig};
    use kafka_protocol::messages::AlterConfigsRequest;

    let handler = create_test_handler();

    let config = AlterableConfig::default()
        .with_name(StrBytes::from_static_str("retention.ms"))
        .with_value(Some(StrBytes::from_static_str("86400000")));

    let resource = AlterConfigsResource::default()
        .with_resource_type(2) // TOPIC
        .with_resource_name(StrBytes::from_static_str("test-topic"))
        .with_configs(vec![config]);

    let request = AlterConfigsRequest::default().with_resources(vec![resource]);

    let response = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(handler.handle_alter_configs(request, "User:test", "*"))
        .unwrap();

    // Should have one result
    assert_eq!(
        response.responses.len(),
        1,
        "Should have result for one resource"
    );
}

#[test]
fn test_alter_configs_multiple() {
    // Test AlterConfigs with multiple configs
    use kafka_protocol::messages::alter_configs_request::{AlterConfigsResource, AlterableConfig};
    use kafka_protocol::messages::AlterConfigsRequest;

    let handler = create_test_handler();

    let config1 = AlterableConfig::default()
        .with_name(StrBytes::from_static_str("retention.ms"))
        .with_value(Some(StrBytes::from_static_str("86400000")));

    let config2 = AlterableConfig::default()
        .with_name(StrBytes::from_static_str("max.message.bytes"))
        .with_value(Some(StrBytes::from_static_str("1000000")));

    let resource = AlterConfigsResource::default()
        .with_resource_type(2) // TOPIC
        .with_resource_name(StrBytes::from_static_str("test-topic"))
        .with_configs(vec![config1, config2]);

    let request = AlterConfigsRequest::default().with_resources(vec![resource]);

    let response = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(handler.handle_alter_configs(request, "User:test", "*"))
        .unwrap();

    assert_eq!(
        response.responses.len(),
        1,
        "Should have result for the resource"
    );
}

#[test]
fn test_alter_configs_header_versions() {
    // AlterConfigs v2+ uses flexible headers
    use kafka_protocol::messages::ApiKey;

    // v0-1 use header v1
    for version in 0i16..2 {
        let header_version =
            KafkaHandler::request_header_version(ApiKey::AlterConfigs as i16, version);
        assert_eq!(
            header_version, 1,
            "AlterConfigs v{} should use request header v1",
            version
        );
    }

    // v2 uses header v2 (flexible)
    let header_version = KafkaHandler::request_header_version(ApiKey::AlterConfigs as i16, 2);
    assert_eq!(
        header_version, 2,
        "AlterConfigs v2 should use request header v2"
    );
}

#[test]
fn test_incremental_alter_configs_set() {
    // Test IncrementalAlterConfigs with SET operation
    use kafka_protocol::messages::incremental_alter_configs_request::{
        AlterConfigsResource, AlterableConfig,
    };
    use kafka_protocol::messages::IncrementalAlterConfigsRequest;

    let handler = create_test_handler();

    let config = AlterableConfig::default()
        .with_name(StrBytes::from_static_str("retention.ms"))
        .with_config_operation(0) // SET
        .with_value(Some(StrBytes::from_static_str("86400000")));

    let resource = AlterConfigsResource::default()
        .with_resource_type(2) // TOPIC
        .with_resource_name(StrBytes::from_static_str("test-topic"))
        .with_configs(vec![config]);

    let request = IncrementalAlterConfigsRequest::default().with_resources(vec![resource]);

    let response = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(handler.handle_incremental_alter_configs(request))
        .unwrap();

    assert_eq!(
        response.responses.len(),
        1,
        "Should have result for one resource"
    );
}

#[test]
fn test_incremental_alter_configs_delete() {
    // Test IncrementalAlterConfigs with DELETE operation
    use kafka_protocol::messages::incremental_alter_configs_request::{
        AlterConfigsResource, AlterableConfig,
    };
    use kafka_protocol::messages::IncrementalAlterConfigsRequest;

    let handler = create_test_handler();

    let config = AlterableConfig::default()
        .with_name(StrBytes::from_static_str("retention.ms"))
        .with_config_operation(1) // DELETE
        .with_value(None);

    let resource = AlterConfigsResource::default()
        .with_resource_type(2) // TOPIC
        .with_resource_name(StrBytes::from_static_str("test-topic"))
        .with_configs(vec![config]);

    let request = IncrementalAlterConfigsRequest::default().with_resources(vec![resource]);

    let response = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(handler.handle_incremental_alter_configs(request))
        .unwrap();

    assert_eq!(
        response.responses.len(),
        1,
        "Should have result for DELETE operation"
    );
}

#[test]
fn test_incremental_alter_configs_header_always_flexible() {
    // IncrementalAlterConfigs always uses flexible headers (v0+)
    use kafka_protocol::messages::ApiKey;

    for version in 0i16..=1 {
        let header_version =
            KafkaHandler::request_header_version(ApiKey::IncrementalAlterConfigs as i16, version);
        assert_eq!(
            header_version, 2,
            "IncrementalAlterConfigs v{} should use request header v2 (always flexible)",
            version
        );
    }
}
