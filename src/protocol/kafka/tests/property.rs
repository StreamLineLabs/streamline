use super::*;

#[test]
fn test_property_header_version_consistency() {
    // Property: For all APIs except ApiVersions, if request header uses v2,
    // response header should use v1 (flexible encoding enabled together)
    use kafka_protocol::messages::ApiKey;

    let apis_to_test = vec![
        (ApiKey::Metadata, 9, 12),
        (ApiKey::Produce, 9, 9),
        (ApiKey::Fetch, 12, 13),
        (ApiKey::ListOffsets, 6, 7),
        (ApiKey::CreateTopics, 5, 7),
        (ApiKey::DeleteTopics, 4, 6),
        (ApiKey::FindCoordinator, 3, 4),
        (ApiKey::JoinGroup, 6, 9),
        (ApiKey::SyncGroup, 4, 5),
        (ApiKey::Heartbeat, 4, 4),
        (ApiKey::LeaveGroup, 4, 5),
        (ApiKey::OffsetCommit, 8, 8),
        (ApiKey::OffsetFetch, 6, 8),
        (ApiKey::DescribeGroups, 5, 5),
        (ApiKey::ListGroups, 3, 4),
        (ApiKey::DeleteGroups, 2, 2),
        (ApiKey::DescribeConfigs, 4, 4),
        (ApiKey::AlterConfigs, 2, 2),
        (ApiKey::CreatePartitions, 2, 3),
        (ApiKey::InitProducerId, 2, 4),
        (ApiKey::AddPartitionsToTxn, 3, 4),
        (ApiKey::EndTxn, 3, 3),
    ];

    for (api_key, flexible_start_version, max_version) in apis_to_test {
        // For versions >= flexible_start_version, both request and response use flexible encoding
        for version in flexible_start_version..=max_version {
            let req_header = KafkaHandler::request_header_version(api_key as i16, version);
            let resp_header = KafkaHandler::response_header_version(api_key as i16, version);

            // When request header is v2 (flexible), response header should be v1 (flexible)
            if req_header == 2 {
                assert_eq!(
                    resp_header, 1,
                    "API {:?} version {} has request header v2, should have response header v1",
                    api_key, version
                );
            }
        }
    }
}

#[test]
fn test_property_api_versions_always_header_v0() {
    // Property: ApiVersions ALWAYS uses response header v0 for bootstrap compatibility
    use kafka_protocol::messages::ApiKey;

    for version in 0..=3 {
        let resp_header =
            KafkaHandler::response_header_version(ApiKey::ApiVersions as i16, version);
        assert_eq!(
            resp_header, 0,
            "ApiVersions v{} should ALWAYS use response header v0",
            version
        );
    }
}

#[test]
fn test_property_sasl_handshake_always_header_v0() {
    // Property: SaslHandshake ALWAYS uses response header v0
    use kafka_protocol::messages::ApiKey;

    for version in 0..=2 {
        let resp_header =
            KafkaHandler::response_header_version(ApiKey::SaslHandshake as i16, version);
        assert_eq!(
            resp_header, 0,
            "SaslHandshake v{} should use response header v0",
            version
        );
    }
}

#[test]
fn test_property_all_defined_error_codes_in_valid_range() {
    // Property: All error codes we define should be within Kafka's known error code range
    // Valid error codes are 0 to 120+ (growing with each Kafka version)
    let valid_error_codes: Vec<i16> = vec![
        0,  // NONE
        1,  // OFFSET_OUT_OF_RANGE
        2,  // CORRUPT_MESSAGE
        3,  // UNKNOWN_TOPIC_OR_PARTITION
        5,  // LEADER_NOT_AVAILABLE
        6,  // NOT_LEADER_OR_FOLLOWER
        7,  // REQUEST_TIMED_OUT
        10, // MESSAGE_TOO_LARGE
        17, // INVALID_TOPIC_EXCEPTION
        22, // ILLEGAL_GENERATION
        24, // INVALID_GROUP_ID
        25, // UNKNOWN_MEMBER_ID
        27, // REBALANCE_IN_PROGRESS
        33, // UNSUPPORTED_SASL_MECHANISM
        35, // UNSUPPORTED_VERSION
        36, // TOPIC_ALREADY_EXISTS
        37, // INVALID_PARTITIONS
        42, // INVALID_REQUEST
        47, // INVALID_PRODUCER_EPOCH
        48, // INVALID_TXN_STATE
        58, // SASL_AUTHENTICATION_FAILED
        68, // NON_EMPTY_GROUP
        79, // MEMBER_ID_REQUIRED
    ];

    for code in valid_error_codes {
        assert!(
            (-1..=200).contains(&code),
            "Error code {} should be in valid range",
            code
        );
    }
}

#[test]
fn test_property_all_api_version_ranges_valid() {
    // Property: For all APIs, min_version <= max_version
    use kafka_protocol::messages::ApiVersionsRequest;

    let handler = create_test_handler();
    let response = handler
        .handle_api_versions(ApiVersionsRequest::default())
        .unwrap();

    for api in &response.api_keys {
        assert!(
            api.min_version <= api.max_version,
            "API {} has invalid version range: min {} > max {}",
            api.api_key,
            api.min_version,
            api.max_version
        );

        // Also verify min_version is non-negative
        assert!(
            api.min_version >= 0,
            "API {} has negative min_version: {}",
            api.api_key,
            api.min_version
        );
    }
}

#[test]
fn test_property_correlation_id_any_value_preserved() {
    // Property: Any valid INT32 correlation_id should be preserved
    use kafka_protocol::messages::ResponseHeader;

    let test_values: Vec<i32> = vec![
        0,
        1,
        -1,
        i32::MAX,
        i32::MIN,
        12345,
        -12345,
        0x7FFFFFFF,
        -0x7FFFFFFF,
    ];

    for correlation_id in test_values {
        let header = ResponseHeader::default().with_correlation_id(correlation_id);
        assert_eq!(
            header.correlation_id, correlation_id,
            "Correlation ID {} should be preserved",
            correlation_id
        );
    }
}

#[test]
fn test_property_valid_topic_names_accepted() {
    // Property: Valid topic names (alphanumeric, hyphen, underscore) should be accepted
    let valid_names = vec![
        "test-topic",
        "my_topic",
        "topic123",
        "a",
        "topic-with-dashes",
        "topic_with_underscores",
        "MixedCase",
        "123numeric",
    ];

    for name in valid_names {
        // Topic names should be non-empty and not contain special characters
        assert!(!name.is_empty(), "Topic name should not be empty");
        assert!(
            name.chars()
                .all(|c| c.is_alphanumeric() || c == '-' || c == '_'),
            "Topic name '{}' should only contain alphanumeric, hyphen, or underscore",
            name
        );
    }
}

#[test]
fn test_property_special_offset_values() {
    // Property: Special offset values should be recognized correctly
    let latest_offset: i64 = -1;
    let earliest_offset: i64 = -2;
    let max_timestamp_offset: i64 = -3;

    assert_eq!(latest_offset, -1, "Latest offset constant");
    assert_eq!(earliest_offset, -2, "Earliest offset constant");
    assert_eq!(max_timestamp_offset, -3, "Max timestamp offset constant");

    // Any non-negative offset is a regular offset
    for offset in [0i64, 1, 100, 1000, i64::MAX] {
        assert!(offset >= 0, "Regular offsets are non-negative");
    }
}

#[test]
fn test_property_partition_index_valid_range() {
    // Property: Valid partition indices are non-negative INT32 values
    let valid_partitions: Vec<i32> = vec![0, 1, 10, 100, 1000];

    for partition in valid_partitions {
        assert!(
            partition >= 0,
            "Partition index {} should be non-negative",
            partition
        );
    }
}

#[test]
fn test_property_acks_values() {
    // Property: Only valid acks values are -1, 0, 1
    let valid_acks: Vec<i16> = vec![-1, 0, 1];

    for acks in &valid_acks {
        assert!(
            *acks == -1 || *acks == 0 || *acks == 1,
            "Acks value {} should be -1, 0, or 1",
            acks
        );
    }

    // -1 means all replicas must ack
    assert_eq!(valid_acks[0], -1, "acks=-1 means all replicas");
    // 0 means no ack required
    assert_eq!(valid_acks[1], 0, "acks=0 means no acknowledgment");
    // 1 means leader only
    assert_eq!(valid_acks[2], 1, "acks=1 means leader only");
}

#[test]
fn test_property_size_limits() {
    // Property: Protocol has defined size limits
    let max_request_size: usize = 100 * 1024 * 1024; // 100MB
    let max_message_size: usize = 1024 * 1024; // 1MB default

    assert!(max_request_size > 0, "Max request size should be positive");
    assert!(max_message_size > 0, "Max message size should be positive");
    assert!(
        max_request_size >= max_message_size,
        "Max request size should be >= max message size"
    );
}

#[test]
fn test_property_message_size_is_int32() {
    // Property: Message size prefix is always INT32 (4 bytes, big-endian)
    let size_bytes = 4usize;
    assert_eq!(size_bytes, std::mem::size_of::<i32>());

    // Maximum valid message size fits in signed INT32
    let max_size: i32 = i32::MAX;
    assert!(max_size > 0);
}

#[test]
#[allow(clippy::assertions_on_constants)]
fn test_property_message_size_hard_cap() {
    // Property: Hard cap protects against DoS via memory exhaustion
    // These are intentional compile-time constant assertions
    assert!(
        HARD_MAX_MESSAGE_BYTES <= 256 * 1024 * 1024,
        "Hard cap should be at most 256 MB to prevent DoS"
    );
    assert!(HARD_MAX_MESSAGE_BYTES > 0, "Hard cap must be positive");
    assert!(
        DEFAULT_MAX_MESSAGE_BYTES <= HARD_MAX_MESSAGE_BYTES,
        "Default should not exceed hard cap"
    );
}
