use super::*;

#[test]
fn test_kafka_handler_creation() {
    let handler = create_test_handler();
    assert_eq!(handler.node_id, 0);
    assert!(!handler.cluster_id.is_empty());
}

#[tokio::test]
async fn test_dispatch_request_api_versions_returns_response() {
    let handler = create_test_handler();
    let header = RequestHeader::default()
        .with_request_api_key(ApiKey::ApiVersions as i16)
        .with_request_api_version(0)
        .with_correlation_id(5);
    let result = handler
        .dispatch_request(
            ApiKey::ApiVersions,
            &header,
            Bytes::new(),
            &create_test_session_manager(),
            "user",
            "127.0.0.1",
        )
        .await
        .expect("dispatch should succeed");

    assert_eq!(result.effective_api_version, 0);
    assert!(result.cache_key.is_none());
    assert!(
        !result.response_body.is_empty(),
        "Expected encoded response body"
    );
}

#[test]
fn test_validate_client_id_valid() {
    // None is valid (client ID is optional)
    assert!(validate_client_id(&None).is_ok());

    // Empty string is valid
    assert!(validate_client_id(&Some(StrBytes::from_string(String::new()))).is_ok());

    // Normal client IDs
    assert!(validate_client_id(&Some(StrBytes::from_string("my-client".to_string()))).is_ok());
    assert!(validate_client_id(&Some(StrBytes::from_string("producer-1.2.3".to_string()))).is_ok());
    assert!(validate_client_id(&Some(StrBytes::from_string(
        "consumer_group_abc".to_string()
    )))
    .is_ok());

    // With spaces and special printable chars
    assert!(
        validate_client_id(&Some(StrBytes::from_string("my client (v1.0)".to_string()))).is_ok()
    );
}

#[test]
fn test_validate_client_id_too_long() {
    // 256 chars is the limit
    let max_len_id = "a".repeat(256);
    assert!(validate_client_id(&Some(StrBytes::from_string(max_len_id))).is_ok());

    // 257 chars should fail
    let too_long_id = "a".repeat(257);
    let result = validate_client_id(&Some(StrBytes::from_string(too_long_id)));
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("exceeds maximum length"));
}

#[test]
fn test_validate_client_id_invalid_chars() {
    // Null byte
    let with_null = StrBytes::from_static_str("client\0id");
    let result = validate_client_id(&Some(with_null));
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("invalid character"));

    // Control character (newline)
    let with_newline = StrBytes::from_static_str("client\nid");
    let result = validate_client_id(&Some(with_newline));
    assert!(result.is_err());

    // Tab character
    let with_tab = StrBytes::from_static_str("client\tid");
    let result = validate_client_id(&Some(with_tab));
    assert!(result.is_err());

    // DEL character (0x7F)
    let with_del = StrBytes::from_string("client\x7Fid".to_string());
    let result = validate_client_id(&Some(with_del));
    assert!(result.is_err());
}

#[test]
fn test_handle_api_versions() {
    let handler = create_test_handler();
    let request = ApiVersionsRequest::default();
    let response = handler.handle_api_versions(request).unwrap();

    assert_eq!(response.error_code, 0);
    assert!(!response.api_keys.is_empty());

    // Check that expected API keys are present
    let api_keys: Vec<i16> = response.api_keys.iter().map(|k| k.api_key).collect();
    assert!(api_keys.contains(&(ApiKey::ApiVersions as i16)));
    assert!(api_keys.contains(&(ApiKey::Metadata as i16)));
    assert!(api_keys.contains(&(ApiKey::Produce as i16)));
    assert!(api_keys.contains(&(ApiKey::Fetch as i16)));
    assert!(api_keys.contains(&(ApiKey::ListOffsets as i16)));
    assert!(api_keys.contains(&(ApiKey::CreateTopics as i16)));
}

#[tokio::test]
async fn test_handle_metadata_empty_topics() {
    let handler = create_test_handler();
    let request = MetadataRequest::default();
    let response = handler.handle_metadata(request, 12).await.unwrap();

    // Should have broker info
    assert!(!response.brokers.is_empty());
    assert!(response.cluster_id.is_some());

    // No topics exist yet
    assert!(response.topics.is_empty());
}

#[tokio::test]
async fn test_handle_metadata_with_topics() {
    let dir = tempdir().unwrap();
    let topic_manager = Arc::new(TopicManager::new(dir.path()).unwrap());
    topic_manager.create_topic("test-topic", 3).unwrap();

    let offset_dir = tempdir().unwrap();
    let group_coordinator =
        Arc::new(GroupCoordinator::new(offset_dir.path(), topic_manager.clone()).unwrap());
    let handler =
        KafkaHandler::new(topic_manager, group_coordinator).expect("Failed to create handler");
    let request = MetadataRequest::default();
    let response = handler.handle_metadata(request, 12).await.unwrap();

    assert_eq!(response.topics.len(), 1);
    let topic = &response.topics[0];
    assert_eq!(topic.name.as_ref().unwrap().as_str(), "test-topic");
    assert_eq!(topic.partitions.len(), 3);
}

#[test]
fn test_handle_create_topics() {
    use kafka_protocol::messages::create_topics_request::CreatableTopic;

    let handler = create_test_handler();

    let mut request = CreateTopicsRequest::default();
    request.topics = vec![CreatableTopic::default()
        .with_name(TopicName::from(StrBytes::from_static_str("new-topic")))
        .with_num_partitions(2)
        .with_replication_factor(1)];

    let response = handler.handle_create_topics(request).unwrap();

    assert_eq!(response.topics.len(), 1);
    assert_eq!(response.topics[0].error_code, 0);
    assert_eq!(response.topics[0].name.as_str(), "new-topic");
}

#[test]
fn test_handle_list_offsets_nonexistent_topic() {
    use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};

    let handler = create_test_handler();

    let mut request = ListOffsetsRequest::default();
    request.topics = vec![ListOffsetsTopic::default()
        .with_name(TopicName::from(StrBytes::from_static_str("nonexistent")))
        .with_partitions(vec![
            ListOffsetsPartition::default()
                .with_partition_index(0)
                .with_timestamp(-1), // Latest
        ])];

    let response = handler.handle_list_offsets(request, 7).unwrap();

    assert_eq!(response.topics.len(), 1);
    assert_eq!(response.topics[0].partitions.len(), 1);
    // Should return error code 3 (UNKNOWN_TOPIC_OR_PARTITION)
    assert_eq!(response.topics[0].partitions[0].error_code, 3);
}

#[test]
fn test_handle_list_offsets_existing_topic() {
    use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};

    let dir = tempdir().unwrap();
    let topic_manager = Arc::new(TopicManager::new(dir.path()).unwrap());
    topic_manager.create_topic("existing-topic", 1).unwrap();

    let offset_dir = tempdir().unwrap();
    let group_coordinator =
        Arc::new(GroupCoordinator::new(offset_dir.path(), topic_manager.clone()).unwrap());
    let handler =
        KafkaHandler::new(topic_manager, group_coordinator).expect("Failed to create handler");

    let mut request = ListOffsetsRequest::default();
    request.topics = vec![ListOffsetsTopic::default()
        .with_name(TopicName::from(StrBytes::from_static_str("existing-topic")))
        .with_partitions(vec![
            ListOffsetsPartition::default()
                .with_partition_index(0)
                .with_timestamp(-1), // Latest
        ])];

    let response = handler.handle_list_offsets(request, 7).unwrap();

    assert_eq!(response.topics.len(), 1);
    assert_eq!(response.topics[0].partitions.len(), 1);
    assert_eq!(response.topics[0].partitions[0].error_code, 0);
    assert_eq!(response.topics[0].partitions[0].offset, 0); // No records yet
}
