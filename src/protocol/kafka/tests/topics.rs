use super::*;

#[test]
fn test_create_topics_success() {
    use kafka_protocol::messages::create_topics_request::CreatableTopic;

    let handler = create_test_handler();

    let request = CreateTopicsRequest::default().with_topics(vec![CreatableTopic::default()
        .with_name(TopicName::from(StrBytes::from_static_str("new-topic")))
        .with_num_partitions(3)
        .with_replication_factor(1)]);

    let response = handler.handle_create_topics(request).unwrap();

    assert_eq!(response.topics.len(), 1);
    assert_eq!(response.topics[0].name.as_str(), "new-topic");
    assert_eq!(response.topics[0].error_code, 0);
}

#[test]
fn test_create_topics_multiple() {
    use kafka_protocol::messages::create_topics_request::CreatableTopic;

    let handler = create_test_handler();

    let request = CreateTopicsRequest::default().with_topics(vec![
        CreatableTopic::default()
            .with_name(TopicName::from(StrBytes::from_static_str("topic-1")))
            .with_num_partitions(1)
            .with_replication_factor(1),
        CreatableTopic::default()
            .with_name(TopicName::from(StrBytes::from_static_str("topic-2")))
            .with_num_partitions(2)
            .with_replication_factor(1),
    ]);

    let response = handler.handle_create_topics(request).unwrap();

    assert_eq!(response.topics.len(), 2);
    for topic in &response.topics {
        assert_eq!(topic.error_code, 0, "{:?} should succeed", topic.name);
    }
}

#[test]
fn test_create_topics_already_exists() {
    use kafka_protocol::messages::create_topics_request::CreatableTopic;

    let handler = create_handler_with_topics(&[("existing", 1)]);

    let request = CreateTopicsRequest::default().with_topics(vec![CreatableTopic::default()
        .with_name(TopicName::from(StrBytes::from_static_str("existing")))
        .with_num_partitions(1)
        .with_replication_factor(1)]);

    let response = handler.handle_create_topics(request).unwrap();

    // Error code 36 = TOPIC_ALREADY_EXISTS
    assert_eq!(response.topics[0].error_code, 36);
}

#[test]
fn test_create_topics_with_many_partitions() {
    use kafka_protocol::messages::create_topics_request::CreatableTopic;

    let handler = create_test_handler();

    let request = CreateTopicsRequest::default().with_topics(vec![CreatableTopic::default()
        .with_name(TopicName::from(StrBytes::from_static_str(
            "many-partitions",
        )))
        .with_num_partitions(10)
        .with_replication_factor(1)]);

    let response = handler.handle_create_topics(request).unwrap();

    assert_eq!(response.topics[0].error_code, 0);
    assert_eq!(response.topics[0].num_partitions, 10);
}

#[test]
fn test_create_topics_with_storage_mode_local() {
    use kafka_protocol::messages::create_topics_request::{CreatableTopic, CreateableTopicConfig};

    let handler = create_test_handler();

    let request = CreateTopicsRequest::default().with_topics(vec![CreatableTopic::default()
        .with_name(TopicName::from(StrBytes::from_static_str(
            "storage-mode-local",
        )))
        .with_num_partitions(1)
        .with_replication_factor(1)
        .with_configs(vec![CreateableTopicConfig::default()
            .with_name(StrBytes::from_static_str("storage.mode"))
            .with_value(Some(StrBytes::from_static_str("local")))])]);

    let response = handler.handle_create_topics(request).unwrap();

    assert_eq!(response.topics[0].error_code, 0);

    // Verify the topic was created with local storage mode
    let metadata = handler
        .topic_manager
        .get_topic_metadata("storage-mode-local")
        .unwrap();
    assert_eq!(metadata.config.storage_mode, StorageMode::Local);
}

#[test]
fn test_create_topics_with_storage_mode_diskless() {
    use kafka_protocol::messages::create_topics_request::{CreatableTopic, CreateableTopicConfig};

    let handler = create_test_handler();

    let request = CreateTopicsRequest::default().with_topics(vec![CreatableTopic::default()
        .with_name(TopicName::from(StrBytes::from_static_str(
            "storage-mode-diskless",
        )))
        .with_num_partitions(1)
        .with_replication_factor(1)
        .with_configs(vec![CreateableTopicConfig::default()
            .with_name(StrBytes::from_static_str("storage.mode"))
            .with_value(Some(StrBytes::from_static_str("diskless")))])]);

    let response = handler.handle_create_topics(request).unwrap();

    assert_eq!(response.topics[0].error_code, 0);

    // Verify the topic was created with diskless storage mode
    let metadata = handler
        .topic_manager
        .get_topic_metadata("storage-mode-diskless")
        .unwrap();
    assert_eq!(metadata.config.storage_mode, StorageMode::Diskless);
}

#[test]
fn test_create_topics_with_storage_mode_hybrid() {
    use kafka_protocol::messages::create_topics_request::{CreatableTopic, CreateableTopicConfig};

    let handler = create_test_handler();

    let request = CreateTopicsRequest::default().with_topics(vec![CreatableTopic::default()
        .with_name(TopicName::from(StrBytes::from_static_str(
            "storage-mode-hybrid",
        )))
        .with_num_partitions(1)
        .with_replication_factor(1)
        .with_configs(vec![CreateableTopicConfig::default()
            .with_name(StrBytes::from_static_str("storage.mode"))
            .with_value(Some(StrBytes::from_static_str("hybrid")))])]);

    let response = handler.handle_create_topics(request).unwrap();

    assert_eq!(response.topics[0].error_code, 0);

    // Verify the topic was created with hybrid storage mode
    let metadata = handler
        .topic_manager
        .get_topic_metadata("storage-mode-hybrid")
        .unwrap();
    assert_eq!(metadata.config.storage_mode, StorageMode::Hybrid);
}

#[test]
fn test_create_topics_with_multiple_configs() {
    use kafka_protocol::messages::create_topics_request::{CreatableTopic, CreateableTopicConfig};

    let handler = create_test_handler();

    let request = CreateTopicsRequest::default().with_topics(vec![CreatableTopic::default()
        .with_name(TopicName::from(StrBytes::from_static_str(
            "multi-config-topic",
        )))
        .with_num_partitions(2)
        .with_replication_factor(1)
        .with_configs(vec![
            CreateableTopicConfig::default()
                .with_name(StrBytes::from_static_str("storage.mode"))
                .with_value(Some(StrBytes::from_static_str("diskless"))),
            CreateableTopicConfig::default()
                .with_name(StrBytes::from_static_str("retention.ms"))
                .with_value(Some(StrBytes::from_static_str("86400000"))),
            CreateableTopicConfig::default()
                .with_name(StrBytes::from_static_str("cleanup.policy"))
                .with_value(Some(StrBytes::from_static_str("compact"))),
        ])]);

    let response = handler.handle_create_topics(request).unwrap();

    assert_eq!(response.topics[0].error_code, 0);

    // Verify all configs were applied
    let metadata = handler
        .topic_manager
        .get_topic_metadata("multi-config-topic")
        .unwrap();
    assert_eq!(metadata.config.storage_mode, StorageMode::Diskless);
    assert_eq!(metadata.config.retention_ms, 86400000);
    assert_eq!(metadata.config.cleanup_policy, CleanupPolicy::Compact);
}

#[test]
fn test_create_topics_with_invalid_storage_mode_uses_default() {
    use kafka_protocol::messages::create_topics_request::{CreatableTopic, CreateableTopicConfig};

    let handler = create_test_handler();

    let request = CreateTopicsRequest::default().with_topics(vec![CreatableTopic::default()
        .with_name(TopicName::from(StrBytes::from_static_str(
            "invalid-storage-mode",
        )))
        .with_num_partitions(1)
        .with_replication_factor(1)
        .with_configs(vec![CreateableTopicConfig::default()
            .with_name(StrBytes::from_static_str("storage.mode"))
            .with_value(Some(StrBytes::from_static_str("invalid")))])]);

    let response = handler.handle_create_topics(request).unwrap();

    // Should succeed but use default (local) storage mode
    assert_eq!(response.topics[0].error_code, 0);

    let metadata = handler
        .topic_manager
        .get_topic_metadata("invalid-storage-mode")
        .unwrap();
    assert_eq!(metadata.config.storage_mode, StorageMode::Local);
}

#[test]
fn test_parse_topic_configs_all_fields() {
    use kafka_protocol::messages::create_topics_request::CreateableTopicConfig;

    let handler = create_test_handler();

    let configs = vec![
        CreateableTopicConfig::default()
            .with_name(StrBytes::from_static_str("storage.mode"))
            .with_value(Some(StrBytes::from_static_str("hybrid"))),
        CreateableTopicConfig::default()
            .with_name(StrBytes::from_static_str("retention.ms"))
            .with_value(Some(StrBytes::from_static_str("3600000"))),
        CreateableTopicConfig::default()
            .with_name(StrBytes::from_static_str("retention.bytes"))
            .with_value(Some(StrBytes::from_static_str("1073741824"))),
        CreateableTopicConfig::default()
            .with_name(StrBytes::from_static_str("segment.bytes"))
            .with_value(Some(StrBytes::from_static_str("536870912"))),
        CreateableTopicConfig::default()
            .with_name(StrBytes::from_static_str("cleanup.policy"))
            .with_value(Some(StrBytes::from_static_str("delete"))),
        CreateableTopicConfig::default()
            .with_name(StrBytes::from_static_str("message.ttl.ms"))
            .with_value(Some(StrBytes::from_static_str("7200000"))),
        CreateableTopicConfig::default()
            .with_name(StrBytes::from_static_str("min.cleanable.dirty.ratio"))
            .with_value(Some(StrBytes::from_static_str("0.75"))),
        CreateableTopicConfig::default()
            .with_name(StrBytes::from_static_str("delete.retention.ms"))
            .with_value(Some(StrBytes::from_static_str("172800000"))),
        CreateableTopicConfig::default()
            .with_name(StrBytes::from_static_str("min.compaction.lag.ms"))
            .with_value(Some(StrBytes::from_static_str("60000"))),
    ];

    let topic_config = handler.parse_topic_configs(&configs);

    assert_eq!(topic_config.storage_mode, StorageMode::Hybrid);
    assert_eq!(topic_config.retention_ms, 3600000);
    assert_eq!(topic_config.retention_bytes, 1073741824);
    assert_eq!(topic_config.segment_bytes, 536870912);
    assert_eq!(topic_config.cleanup_policy, CleanupPolicy::Delete);
    assert_eq!(topic_config.message_ttl_ms, 7200000);
    assert!((topic_config.min_cleanable_dirty_ratio - 0.75).abs() < 0.001);
    assert_eq!(topic_config.delete_retention_ms, 172800000);
    assert_eq!(topic_config.min_compaction_lag_ms, 60000);
}

#[test]
fn test_parse_topic_configs_empty() {
    let handler = create_test_handler();

    let configs = vec![];
    let topic_config = handler.parse_topic_configs(&configs);

    // Should return default config
    assert_eq!(topic_config.storage_mode, StorageMode::Local);
    assert_eq!(topic_config.retention_ms, -1);
    assert_eq!(topic_config.cleanup_policy, CleanupPolicy::Delete);
}

#[test]
fn test_parse_topic_configs_unknown_config_ignored() {
    use kafka_protocol::messages::create_topics_request::CreateableTopicConfig;

    let handler = create_test_handler();

    let configs = vec![
        CreateableTopicConfig::default()
            .with_name(StrBytes::from_static_str("unknown.config"))
            .with_value(Some(StrBytes::from_static_str("some-value"))),
        CreateableTopicConfig::default()
            .with_name(StrBytes::from_static_str("storage.mode"))
            .with_value(Some(StrBytes::from_static_str("diskless"))),
    ];

    let topic_config = handler.parse_topic_configs(&configs);

    // Unknown config should be ignored, storage.mode should be parsed
    assert_eq!(topic_config.storage_mode, StorageMode::Diskless);
}

#[test]
fn test_delete_topics_success() {
    let handler = create_handler_with_topics(&[("to-delete", 1)]);

    // Use api_version 4 (v0-5 use topic_names field)
    let request = DeleteTopicsRequest::default().with_topic_names(vec![TopicName::from(
        StrBytes::from_static_str("to-delete"),
    )]);

    let response = handler.handle_delete_topics(request, 4).unwrap();

    assert_eq!(response.responses.len(), 1);
    assert_eq!(response.responses[0].error_code, 0);
}

#[test]
fn test_delete_topics_nonexistent() {
    let handler = create_test_handler();

    // Use api_version 4 (v0-5 use topic_names field)
    let request = DeleteTopicsRequest::default().with_topic_names(vec![TopicName::from(
        StrBytes::from_static_str("nonexistent"),
    )]);

    let response = handler.handle_delete_topics(request, 4).unwrap();

    // Error code 3 = UNKNOWN_TOPIC_OR_PARTITION
    assert_eq!(response.responses[0].error_code, 3);
}

#[test]
fn test_delete_topics_multiple() {
    let handler = create_handler_with_topics(&[("del-1", 1), ("del-2", 1)]);

    // Use api_version 4 (v0-5 use topic_names field)
    let request = DeleteTopicsRequest::default().with_topic_names(vec![
        TopicName::from(StrBytes::from_static_str("del-1")),
        TopicName::from(StrBytes::from_static_str("del-2")),
    ]);

    let response = handler.handle_delete_topics(request, 4).unwrap();

    assert_eq!(response.responses.len(), 2);
    for r in &response.responses {
        assert_eq!(r.error_code, 0);
    }
}

#[test]
fn test_create_topics_success_returns_none_error() {
    // Create a new topic - should return NONE (0) error code
    use kafka_protocol::messages::create_topics_request::CreatableTopic;
    use kafka_protocol::messages::CreateTopicsRequest;

    let handler = create_test_handler();

    let request = CreateTopicsRequest::default()
        .with_topics(vec![CreatableTopic::default()
            .with_name(TopicName::from(StrBytes::from_static_str(
                "new-success-topic",
            )))
            .with_num_partitions(1)
            .with_replication_factor(1)])
        .with_timeout_ms(5000);

    let response = handler.handle_create_topics(request).unwrap();

    assert_eq!(response.topics.len(), 1);
    assert_eq!(
        response.topics[0].error_code, NONE,
        "Successful CreateTopics should return NONE (0)"
    );
}
