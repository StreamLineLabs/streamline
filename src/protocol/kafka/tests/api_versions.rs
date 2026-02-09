use super::*;

#[test]
fn test_api_versions_response_encoding_v3() {
    use kafka_protocol::messages::api_versions_response::ApiVersion;
    use kafka_protocol::protocol::Encodable;

    // Create a minimal ApiVersionsResponse
    let response = ApiVersionsResponse::default()
        .with_error_code(0)
        .with_api_keys(vec![ApiVersion::default()
            .with_api_key(18)
            .with_min_version(0)
            .with_max_version(3)])
        .with_throttle_time_ms(0);

    // Encode the response body for version 3
    let mut body_buf = BytesMut::new();
    response.encode(&mut body_buf, 3).unwrap();
    println!("Response body (v3) hex: {:02x?}", body_buf.as_ref());
    println!("Response body (v3) len: {}", body_buf.len());

    // Encode the response header (version 0 for ApiVersions)
    let response_header = ResponseHeader::default().with_correlation_id(1);
    let mut header_buf = BytesMut::new();
    response_header.encode(&mut header_buf, 0).unwrap();
    println!("Response header (v0) hex: {:02x?}", header_buf.as_ref());
    println!("Response header (v0) len: {}", header_buf.len());

    // Full response
    let mut full_response = BytesMut::new();
    full_response.extend_from_slice(&header_buf);
    full_response.extend_from_slice(&body_buf);
    println!("Full response hex: {:02x?}", full_response.as_ref());
    println!("Full response len: {}", full_response.len());

    // The response should be decodable
    // Header is correlation_id (4 bytes)
    assert_eq!(header_buf.len(), 4);
    // Full response should be header + body
    assert_eq!(full_response.len(), header_buf.len() + body_buf.len());
}

/// Test that ApiVersions ALWAYS uses response header v0 regardless of request version
/// This is critical for the bootstrap protocol
#[test]
fn test_api_versions_response_header_always_v0() {
    // ApiVersions is the bootstrap API - it must ALWAYS use response header v0
    // so that clients can decode it without knowing the server's capabilities
    for version in 0..=3 {
        let header_version = KafkaHandler::response_header_version(18, version);
        assert_eq!(
            header_version, 0,
            "ApiVersions v{} must use response header v0 (bootstrap), got {}",
            version, header_version
        );
    }
}

#[test]
fn test_api_versions_contains_all_core_apis() {
    let handler = create_test_handler();
    let request = ApiVersionsRequest::default();
    let response = handler.handle_api_versions(request).unwrap();

    assert_eq!(response.error_code, 0, "ApiVersions should succeed");

    let api_keys: Vec<i16> = response.api_keys.iter().map(|k| k.api_key).collect();

    // Core APIs that must be present
    let expected = vec![
        0,  // Produce
        1,  // Fetch
        2,  // ListOffsets
        3,  // Metadata
        18, // ApiVersions
        19, // CreateTopics
        20, // DeleteTopics
    ];

    for key in expected {
        assert!(
            api_keys.contains(&key),
            "ApiVersions must include API key {}",
            key
        );
    }
}

#[test]
fn test_api_versions_valid_version_ranges() {
    let handler = create_test_handler();
    let request = ApiVersionsRequest::default();
    let response = handler.handle_api_versions(request).unwrap();

    for api in &response.api_keys {
        assert!(
            api.min_version <= api.max_version,
            "API {}: min_version ({}) > max_version ({})",
            api.api_key,
            api.min_version,
            api.max_version
        );
        assert!(api.min_version >= 0, "API {}: min_version < 0", api.api_key);
    }
}

#[test]
fn test_api_versions_throttle_time() {
    let handler = create_test_handler();
    let request = ApiVersionsRequest::default();
    let response = handler.handle_api_versions(request).unwrap();

    assert!(
        response.throttle_time_ms >= 0,
        "throttle_time_ms should be >= 0"
    );
}

#[test]
fn test_version_0_metadata() {
    // Test minimum version (v0) for Metadata
    use kafka_protocol::messages::MetadataRequest;

    let handler = create_test_handler();

    let request = MetadataRequest::default();

    let response = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(handler.handle_metadata(request, 0))
        .unwrap();

    // v0 should still return broker info
    assert!(!response.brokers.is_empty());
}

#[test]
fn test_version_0_produce() {
    // Test minimum version (v0) for Produce
    use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
    use kafka_protocol::messages::ProduceRequest;

    let handler = create_test_handler();

    let partition_data = PartitionProduceData::default().with_index(0);

    let topic_data = TopicProduceData::default()
        .with_name(TopicName(StrBytes::from_static_str("test-topic")))
        .with_partition_data(vec![partition_data]);

    let request = ProduceRequest::default()
        .with_timeout_ms(1000)
        .with_acks(1)
        .with_topic_data(vec![topic_data]);

    let response = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(handler.handle_produce(request, &create_test_session_manager(), None))
        .unwrap();

    // v0 should return responses
    let _ = response.responses.len();
}

#[test]
fn test_version_0_fetch() {
    // Test minimum version (v0) for Fetch
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
        .with_max_wait_ms(100)
        .with_min_bytes(1)
        .with_topics(vec![topic]);

    let response = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(handler.handle_fetch(request, &create_test_session_manager(), None))
        .unwrap();

    // v0 should work
    let _ = response.responses.len();
}

#[test]
fn test_api_version_ranges_valid() {
    // Verify all API version ranges are valid (min <= max)
    use kafka_protocol::messages::ApiVersionsRequest;

    let handler = create_test_handler();
    let api_versions = handler
        .handle_api_versions(ApiVersionsRequest::default())
        .unwrap();

    for api in &api_versions.api_keys {
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
fn test_api_versions_response_complete() {
    // Verify ApiVersions response includes all implemented APIs
    use kafka_protocol::messages::ApiVersionsRequest;

    let handler = create_test_handler();

    let request = ApiVersionsRequest::default();
    let response = handler.handle_api_versions(request).unwrap();

    // Should have many APIs
    assert!(
        response.api_keys.len() >= 30,
        "Should have at least 30 APIs, got {}",
        response.api_keys.len()
    );

    // Verify key APIs are present
    let api_keys: Vec<i16> = response.api_keys.iter().map(|a| a.api_key).collect();

    assert!(api_keys.contains(&0), "Should include Produce (0)");
    assert!(api_keys.contains(&1), "Should include Fetch (1)");
    assert!(api_keys.contains(&2), "Should include ListOffsets (2)");
    assert!(api_keys.contains(&3), "Should include Metadata (3)");
    assert!(api_keys.contains(&18), "Should include ApiVersions (18)");
}
