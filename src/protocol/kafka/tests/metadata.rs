use super::*;

#[tokio::test]
async fn test_metadata_empty_cluster_returns_broker_info() {
    let handler = create_test_handler();
    let request = MetadataRequest::default();
    let response = handler.handle_metadata(request, 12).await.unwrap();

    assert!(!response.brokers.is_empty(), "Should return broker info");
    assert!(response.cluster_id.is_some(), "Should return cluster_id");
    assert!(response.topics.is_empty(), "Empty cluster has no topics");
}

#[tokio::test]
async fn test_metadata_broker_info_valid() {
    let handler = create_test_handler();
    let request = MetadataRequest::default();
    let response = handler.handle_metadata(request, 12).await.unwrap();

    let broker = &response.brokers[0];
    assert!(broker.node_id.0 >= 0, "node_id >= 0");
    assert!(!broker.host.is_empty(), "host not empty");
    assert!(broker.port > 0, "port > 0");
}

#[tokio::test]
async fn test_metadata_with_topics() {
    let handler = create_handler_with_topics(&[("topic-a", 2), ("topic-b", 3)]);
    let request = MetadataRequest::default();
    let response = handler.handle_metadata(request, 12).await.unwrap();

    assert_eq!(response.topics.len(), 2, "Should return both topics");

    let names: Vec<&str> = response
        .topics
        .iter()
        .map(|t| t.name.as_ref().unwrap().as_str())
        .collect();

    assert!(names.contains(&"topic-a"));
    assert!(names.contains(&"topic-b"));
}

#[tokio::test]
async fn test_metadata_partition_count() {
    let handler = create_handler_with_topics(&[("partitioned", 5)]);
    let request = MetadataRequest::default();
    let response = handler.handle_metadata(request, 12).await.unwrap();

    assert_eq!(response.topics.len(), 1);
    assert_eq!(response.topics[0].partitions.len(), 5);

    for (i, p) in response.topics[0].partitions.iter().enumerate() {
        assert_eq!(p.partition_index, i as i32);
    }
}

#[tokio::test]
async fn test_metadata_specific_topic_request() {
    use kafka_protocol::messages::metadata_request::MetadataRequestTopic;

    let handler = create_handler_with_topics(&[("wanted", 1), ("not-wanted", 1)]);

    let request =
        MetadataRequest::default().with_topics(Some(vec![MetadataRequestTopic::default()
            .with_name(Some(TopicName::from(StrBytes::from_static_str("wanted"))))]));

    let response = handler.handle_metadata(request, 12).await.unwrap();

    assert_eq!(response.topics.len(), 1);
    assert_eq!(response.topics[0].name.as_ref().unwrap().as_str(), "wanted");
}

#[tokio::test]
async fn test_metadata_unknown_topic_returns_error() {
    use kafka_protocol::messages::metadata_request::MetadataRequestTopic;

    let handler = create_test_handler();

    let request = MetadataRequest::default()
        .with_topics(Some(vec![MetadataRequestTopic::default().with_name(Some(
            TopicName::from(StrBytes::from_static_str("nonexistent")),
        ))]))
        .with_allow_auto_topic_creation(false);

    let response = handler.handle_metadata(request, 12).await.unwrap();

    assert_eq!(response.topics.len(), 1);
    // Error code 3 = UNKNOWN_TOPIC_OR_PARTITION
    assert_eq!(response.topics[0].error_code, 3);
}

#[tokio::test]
async fn test_metadata_cluster_id_consistent() {
    let handler = create_test_handler();

    let response1 = handler
        .handle_metadata(MetadataRequest::default(), 12)
        .await
        .unwrap();
    let response2 = handler
        .handle_metadata(MetadataRequest::default(), 12)
        .await
        .unwrap();

    assert_eq!(
        response1.cluster_id, response2.cluster_id,
        "Cluster ID should be consistent"
    );
}

#[tokio::test]
async fn test_metadata_all_versions() {
    let handler = create_handler_with_topics(&[("test", 1)]);

    for version in 0..=12 {
        let request = MetadataRequest::default();
        let response = handler.handle_metadata(request, version).await;

        assert!(response.is_ok(), "Metadata v{} should succeed", version);
    }
}

#[test]
fn test_metadata_response_has_cluster_id() {
    // Verify Metadata response includes cluster ID
    use kafka_protocol::messages::MetadataRequest;

    let handler = create_test_handler();

    let request = MetadataRequest::default();

    let response = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(handler.handle_metadata(request, 9))
        .unwrap();

    // Cluster ID should be present and non-empty
    assert!(
        response.cluster_id.is_some(),
        "Cluster ID should be present"
    );
}

#[test]
fn test_metadata_response_has_controller() {
    // Verify Metadata response includes controller ID
    use kafka_protocol::messages::MetadataRequest;

    let handler = create_test_handler();

    let request = MetadataRequest::default();

    let response = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(handler.handle_metadata(request, 9))
        .unwrap();

    // Controller ID should be set (broker ID 0 or higher)
    assert!(
        response.controller_id.0 >= 0,
        "Controller ID should be valid"
    );
}
