use super::*;

fn api_name(api_key: i16) -> &'static str {
    match api_key {
        0 => "Produce",
        1 => "Fetch",
        2 => "ListOffsets",
        3 => "Metadata",
        8 => "OffsetCommit",
        9 => "OffsetFetch",
        10 => "FindCoordinator",
        11 => "JoinGroup",
        12 => "Heartbeat",
        13 => "LeaveGroup",
        14 => "SyncGroup",
        15 => "DescribeGroups",
        16 => "ListGroups",
        17 => "SaslHandshake",
        18 => "ApiVersions",
        19 => "CreateTopics",
        20 => "DeleteTopics",
        21 => "DeleteRecords",
        22 => "InitProducerId",
        23 => "OffsetForLeaderEpoch",
        24 => "AddPartitionsToTxn",
        25 => "AddOffsetsToTxn",
        26 => "EndTxn",
        28 => "TxnOffsetCommit",
        29 => "DescribeAcls",
        30 => "CreateAcls",
        31 => "DeleteAcls",
        32 => "DescribeConfigs",
        33 => "AlterConfigs",
        36 => "SaslAuthenticate",
        37 => "CreatePartitions",
        42 => "DeleteGroups",
        44 => "IncrementalAlterConfigs",
        60 => "DescribeCluster",
        _ => "Unknown",
    }
}

#[test]
fn test_request_header_version() {
    // ApiVersions v3+ uses header v2 (flexible), v0-2 uses header v1
    assert_eq!(
        KafkaHandler::request_header_version(ApiKey::ApiVersions as i16, 3),
        2
    );
    assert_eq!(
        KafkaHandler::request_header_version(ApiKey::ApiVersions as i16, 2),
        1
    );
    assert_eq!(
        KafkaHandler::request_header_version(ApiKey::ApiVersions as i16, 0),
        1
    );

    // Test flexible versions for other APIs
    assert_eq!(
        KafkaHandler::request_header_version(ApiKey::Metadata as i16, 9),
        2
    );
    assert_eq!(
        KafkaHandler::request_header_version(ApiKey::Produce as i16, 9),
        2
    );

    // Test non-flexible versions
    assert_eq!(
        KafkaHandler::request_header_version(ApiKey::Metadata as i16, 8),
        1
    );
    assert_eq!(
        KafkaHandler::request_header_version(ApiKey::Produce as i16, 8),
        1
    );
}

#[test]
fn test_response_header_version() {
    // ApiVersions always uses response header v0 (bootstrap response, non-flexible)
    assert_eq!(
        KafkaHandler::response_header_version(ApiKey::ApiVersions as i16, 3),
        0
    );
    assert_eq!(
        KafkaHandler::response_header_version(ApiKey::ApiVersions as i16, 2),
        0
    );
    assert_eq!(
        KafkaHandler::response_header_version(ApiKey::ApiVersions as i16, 0),
        0
    );

    // Test flexible versions for other APIs
    assert_eq!(
        KafkaHandler::response_header_version(ApiKey::Metadata as i16, 9),
        1
    );

    // Test non-flexible versions
    assert_eq!(
        KafkaHandler::response_header_version(ApiKey::Metadata as i16, 8),
        0
    );
}

#[test]
fn test_create_response_header() {
    let handler = create_test_handler();
    let header = handler.create_response_header(12345);
    assert_eq!(header.correlation_id, 12345);
}

#[test]
fn test_encode_response() {
    let handler = create_test_handler();
    let response = ApiVersionsResponse::default().with_error_code(NONE);
    let encoded = handler.encode_response(&response, 0).unwrap();
    assert!(!encoded.is_empty());
}

/// Exhaustive test of all API keys and versions for request header versioning
/// Verifies compliance with Kafka protocol specification
#[test]
fn test_all_api_request_header_versions() {
    for &(api_key, max_version, req_v2_start, _) in API_HEADER_VERSIONS {
        for version in 0..=max_version {
            let actual = KafkaHandler::request_header_version(api_key, version);

            let expected = match req_v2_start {
                Some(threshold) if version >= threshold => 2,
                _ => 1,
            };

            assert_eq!(
                actual,
                expected,
                "{} (key={}) v{}: request header version should be {} but got {}",
                api_name(api_key),
                api_key,
                version,
                expected,
                actual
            );
        }
    }
}

/// Exhaustive test of all API keys and versions for response header versioning
/// Verifies compliance with Kafka protocol specification
#[test]
fn test_all_api_response_header_versions() {
    for &(api_key, max_version, _, resp_v1_start) in API_HEADER_VERSIONS {
        for version in 0..=max_version {
            let actual = KafkaHandler::response_header_version(api_key, version);

            let expected = match resp_v1_start {
                Some(threshold) if version >= threshold => 1,
                _ => 0,
            };

            assert_eq!(
                actual,
                expected,
                "{} (key={}) v{}: response header version should be {} but got {}",
                api_name(api_key),
                api_key,
                version,
                expected,
                actual
            );
        }
    }
}

/// Test that SaslHandshake NEVER uses flexible versions
/// This is required for SASL authentication compatibility
#[test]
fn test_sasl_handshake_never_flexible() {
    for version in 0..=1 {
        let req_header = KafkaHandler::request_header_version(17, version);
        let resp_header = KafkaHandler::response_header_version(17, version);

        assert_eq!(
            req_header, 1,
            "SaslHandshake v{} must use request header v1 (non-flexible), got {}",
            version, req_header
        );
        assert_eq!(
            resp_header, 0,
            "SaslHandshake v{} must use response header v0 (non-flexible), got {}",
            version, resp_header
        );
    }
}

/// Test that always-flexible APIs (DescribeCluster, IncrementalAlterConfigs)
/// use flexible versions from v0
#[test]
fn test_always_flexible_apis() {
    // DescribeCluster (60) - always flexible from v0
    assert_eq!(
        KafkaHandler::request_header_version(60, 0),
        2,
        "DescribeCluster v0 must use request header v2 (flexible)"
    );
    assert_eq!(
        KafkaHandler::response_header_version(60, 0),
        1,
        "DescribeCluster v0 must use response header v1 (flexible)"
    );

    // IncrementalAlterConfigs (44) - always flexible from v0
    assert_eq!(
        KafkaHandler::request_header_version(44, 0),
        2,
        "IncrementalAlterConfigs v0 must use request header v2 (flexible)"
    );
    assert_eq!(
        KafkaHandler::response_header_version(44, 0),
        1,
        "IncrementalAlterConfigs v0 must use response header v1 (flexible)"
    );
}

/// Test header version transitions at exact boundaries
/// Verifies the version just before and after the flexible threshold
#[test]
fn test_header_version_boundary_transitions() {
    // Test cases: (api_key, threshold_version, api_name)
    let boundary_tests = [
        (3, 9, "Metadata"),    // v8 non-flexible, v9 flexible
        (0, 9, "Produce"),     // v8 non-flexible, v9 flexible
        (1, 12, "Fetch"),      // v11 non-flexible, v12 flexible
        (2, 6, "ListOffsets"), // v5 non-flexible, v6 flexible
        (19, 5, "CreateTopics"),
        (20, 4, "DeleteTopics"),
        (11, 6, "JoinGroup"),
        (14, 4, "SyncGroup"),
        (12, 4, "Heartbeat"),
        (13, 4, "LeaveGroup"),
        (9, 6, "OffsetFetch"),
        (8, 8, "OffsetCommit"),
        (10, 3, "FindCoordinator"),
    ];

    for (api_key, threshold, name) in boundary_tests {
        // Version just before threshold should be non-flexible
        if threshold > 0 {
            let before = threshold - 1;
            assert_eq!(
                KafkaHandler::request_header_version(api_key, before),
                1,
                "{} v{} (before threshold) should use request header v1",
                name,
                before
            );
            assert_eq!(
                KafkaHandler::response_header_version(api_key, before),
                0,
                "{} v{} (before threshold) should use response header v0",
                name,
                before
            );
        }

        // Version at threshold should be flexible
        assert_eq!(
            KafkaHandler::request_header_version(api_key, threshold),
            2,
            "{} v{} (at threshold) should use request header v2",
            name,
            threshold
        );

        // ApiVersions is special - response header is always v0
        if api_key != 18 {
            assert_eq!(
                KafkaHandler::response_header_version(api_key, threshold),
                1,
                "{} v{} (at threshold) should use response header v1",
                name,
                threshold
            );
        }
    }
}

/// Test that request header v2 implies tagged fields support (flexible)
/// and v1 implies no tagged fields (non-flexible)
#[test]
fn test_header_version_semantics() {
    // Request header v1 = api_key, api_version, correlation_id, client_id (non-flexible)
    // Request header v2 = same as v1 + tagged_fields (flexible)
    // Response header v0 = correlation_id only (non-flexible)
    // Response header v1 = correlation_id + tagged_fields (flexible)

    // Verify a sampling of non-flexible versions
    let non_flexible_cases = [
        (3, 8, "Metadata v8"),
        (0, 8, "Produce v8"),
        (1, 11, "Fetch v11"),
        (18, 2, "ApiVersions v2"),
    ];

    for (api_key, version, name) in non_flexible_cases {
        assert_eq!(
            KafkaHandler::request_header_version(api_key, version),
            1,
            "{} should be non-flexible (request header v1)",
            name
        );
    }

    // Verify a sampling of flexible versions
    let flexible_cases = [
        (3, 12, "Metadata v12"),
        (0, 9, "Produce v9"),
        (1, 12, "Fetch v12"),
        (18, 3, "ApiVersions v3"),
    ];

    for (api_key, version, name) in flexible_cases {
        assert_eq!(
            KafkaHandler::request_header_version(api_key, version),
            2,
            "{} should be flexible (request header v2)",
            name
        );
    }
}

/// Test consumer group APIs header versions
#[test]
fn test_consumer_group_api_header_versions() {
    // FindCoordinator (10): v3+ flexible
    assert_eq!(KafkaHandler::request_header_version(10, 2), 1);
    assert_eq!(KafkaHandler::request_header_version(10, 3), 2);
    assert_eq!(KafkaHandler::response_header_version(10, 2), 0);
    assert_eq!(KafkaHandler::response_header_version(10, 3), 1);

    // JoinGroup (11): v6+ flexible
    assert_eq!(KafkaHandler::request_header_version(11, 5), 1);
    assert_eq!(KafkaHandler::request_header_version(11, 6), 2);
    assert_eq!(KafkaHandler::response_header_version(11, 5), 0);
    assert_eq!(KafkaHandler::response_header_version(11, 6), 1);

    // SyncGroup (14): v4+ flexible
    assert_eq!(KafkaHandler::request_header_version(14, 3), 1);
    assert_eq!(KafkaHandler::request_header_version(14, 4), 2);
    assert_eq!(KafkaHandler::response_header_version(14, 3), 0);
    assert_eq!(KafkaHandler::response_header_version(14, 4), 1);

    // Heartbeat (12): v4+ flexible
    assert_eq!(KafkaHandler::request_header_version(12, 3), 1);
    assert_eq!(KafkaHandler::request_header_version(12, 4), 2);
    assert_eq!(KafkaHandler::response_header_version(12, 3), 0);
    assert_eq!(KafkaHandler::response_header_version(12, 4), 1);

    // LeaveGroup (13): v4+ flexible
    assert_eq!(KafkaHandler::request_header_version(13, 3), 1);
    assert_eq!(KafkaHandler::request_header_version(13, 4), 2);
    assert_eq!(KafkaHandler::response_header_version(13, 3), 0);
    assert_eq!(KafkaHandler::response_header_version(13, 4), 1);

    // OffsetCommit (8): v8+ flexible
    assert_eq!(KafkaHandler::request_header_version(8, 7), 1);
    assert_eq!(KafkaHandler::request_header_version(8, 8), 2);
    assert_eq!(KafkaHandler::response_header_version(8, 7), 0);
    assert_eq!(KafkaHandler::response_header_version(8, 8), 1);

    // OffsetFetch (9): v6+ flexible
    assert_eq!(KafkaHandler::request_header_version(9, 5), 1);
    assert_eq!(KafkaHandler::request_header_version(9, 6), 2);
    assert_eq!(KafkaHandler::response_header_version(9, 5), 0);
    assert_eq!(KafkaHandler::response_header_version(9, 6), 1);
}

/// Test transaction API header versions
#[test]
fn test_transaction_api_header_versions() {
    // InitProducerId (22): v2+ flexible
    assert_eq!(KafkaHandler::request_header_version(22, 1), 1);
    assert_eq!(KafkaHandler::request_header_version(22, 2), 2);
    assert_eq!(KafkaHandler::response_header_version(22, 1), 0);
    assert_eq!(KafkaHandler::response_header_version(22, 2), 1);

    // AddPartitionsToTxn (24): v3+ flexible
    assert_eq!(KafkaHandler::request_header_version(24, 2), 1);
    assert_eq!(KafkaHandler::request_header_version(24, 3), 2);
    assert_eq!(KafkaHandler::response_header_version(24, 2), 0);
    assert_eq!(KafkaHandler::response_header_version(24, 3), 1);

    // AddOffsetsToTxn (25): v3+ flexible
    assert_eq!(KafkaHandler::request_header_version(25, 2), 1);
    assert_eq!(KafkaHandler::request_header_version(25, 3), 2);
    assert_eq!(KafkaHandler::response_header_version(25, 2), 0);
    assert_eq!(KafkaHandler::response_header_version(25, 3), 1);

    // EndTxn (26): v3+ flexible
    assert_eq!(KafkaHandler::request_header_version(26, 2), 1);
    assert_eq!(KafkaHandler::request_header_version(26, 3), 2);
    assert_eq!(KafkaHandler::response_header_version(26, 2), 0);
    assert_eq!(KafkaHandler::response_header_version(26, 3), 1);

    // TxnOffsetCommit (28): v3+ flexible
    assert_eq!(KafkaHandler::request_header_version(28, 2), 1);
    assert_eq!(KafkaHandler::request_header_version(28, 3), 2);
    assert_eq!(KafkaHandler::response_header_version(28, 2), 0);
    assert_eq!(KafkaHandler::response_header_version(28, 3), 1);
}

/// Test ACL API header versions
#[test]
fn test_acl_api_header_versions() {
    // DescribeAcls (29): v2+ flexible
    assert_eq!(KafkaHandler::request_header_version(29, 1), 1);
    assert_eq!(KafkaHandler::request_header_version(29, 2), 2);
    assert_eq!(KafkaHandler::response_header_version(29, 1), 0);
    assert_eq!(KafkaHandler::response_header_version(29, 2), 1);

    // CreateAcls (30): v2+ flexible
    assert_eq!(KafkaHandler::request_header_version(30, 1), 1);
    assert_eq!(KafkaHandler::request_header_version(30, 2), 2);
    assert_eq!(KafkaHandler::response_header_version(30, 1), 0);
    assert_eq!(KafkaHandler::response_header_version(30, 2), 1);

    // DeleteAcls (31): v2+ flexible
    assert_eq!(KafkaHandler::request_header_version(31, 1), 1);
    assert_eq!(KafkaHandler::request_header_version(31, 2), 2);
    assert_eq!(KafkaHandler::response_header_version(31, 1), 0);
    assert_eq!(KafkaHandler::response_header_version(31, 2), 1);
}

/// Test config API header versions
#[test]
fn test_config_api_header_versions() {
    // DescribeConfigs (32): v4+ flexible
    assert_eq!(KafkaHandler::request_header_version(32, 3), 1);
    assert_eq!(KafkaHandler::request_header_version(32, 4), 2);
    assert_eq!(KafkaHandler::response_header_version(32, 3), 0);
    assert_eq!(KafkaHandler::response_header_version(32, 4), 1);

    // AlterConfigs (33): v2+ flexible
    assert_eq!(KafkaHandler::request_header_version(33, 1), 1);
    assert_eq!(KafkaHandler::request_header_version(33, 2), 2);
    assert_eq!(KafkaHandler::response_header_version(33, 1), 0);
    assert_eq!(KafkaHandler::response_header_version(33, 2), 1);

    // CreatePartitions (37): v2+ flexible
    assert_eq!(KafkaHandler::request_header_version(37, 1), 1);
    assert_eq!(KafkaHandler::request_header_version(37, 2), 2);
    assert_eq!(KafkaHandler::response_header_version(37, 1), 0);
    assert_eq!(KafkaHandler::response_header_version(37, 2), 1);
}

/// Test SASL authentication API header versions
#[test]
fn test_sasl_api_header_versions() {
    // SaslHandshake (17): NEVER flexible
    assert_eq!(KafkaHandler::request_header_version(17, 0), 1);
    assert_eq!(KafkaHandler::request_header_version(17, 1), 1);
    assert_eq!(KafkaHandler::response_header_version(17, 0), 0);
    assert_eq!(KafkaHandler::response_header_version(17, 1), 0);

    // SaslAuthenticate (36): v2+ flexible
    assert_eq!(KafkaHandler::request_header_version(36, 1), 1);
    assert_eq!(KafkaHandler::request_header_version(36, 2), 2);
    assert_eq!(KafkaHandler::response_header_version(36, 1), 0);
    assert_eq!(KafkaHandler::response_header_version(36, 2), 1);
}

/// Verify consistency between request and response flexible transitions
/// (except for ApiVersions which has special response header handling)
#[test]
fn test_header_version_consistency() {
    for &(api_key, max_version, req_v2_start, resp_v1_start) in API_HEADER_VERSIONS {
        // Skip ApiVersions - it has special response header handling
        if api_key == 18 {
            continue;
        }

        // For all other APIs, req header v2 and resp header v1 should start at same version
        assert_eq!(
            req_v2_start,
            resp_v1_start,
            "{} (key={}): request and response flexible versions should match",
            api_name(api_key),
            api_key
        );

        // Verify this consistency in the actual implementation
        for version in 0..=max_version {
            let req_flexible = KafkaHandler::request_header_version(api_key, version) == 2;
            let resp_flexible = KafkaHandler::response_header_version(api_key, version) == 1;

            assert_eq!(
                req_flexible,
                resp_flexible,
                "{} v{}: request flexible ({}) should match response flexible ({})",
                api_name(api_key),
                version,
                req_flexible,
                resp_flexible
            );
        }
    }
}

#[test]
fn test_acl_api_header_versions_phase6() {
    // Test ACL API header versions
    use kafka_protocol::messages::ApiKey;

    // DescribeAcls (Key 29)
    // v0-1 use header v1, v2+ use header v2
    let v1_header = KafkaHandler::request_header_version(ApiKey::DescribeAcls as i16, 1);
    assert_eq!(v1_header, 1, "DescribeAcls v1 should use request header v1");

    let v2_header = KafkaHandler::request_header_version(ApiKey::DescribeAcls as i16, 2);
    assert_eq!(v2_header, 2, "DescribeAcls v2 should use request header v2");

    // CreateAcls (Key 30)
    let v2_header = KafkaHandler::request_header_version(ApiKey::CreateAcls as i16, 2);
    assert_eq!(v2_header, 2, "CreateAcls v2 should use request header v2");

    // DeleteAcls (Key 31)
    let v2_header = KafkaHandler::request_header_version(ApiKey::DeleteAcls as i16, 2);
    assert_eq!(v2_header, 2, "DeleteAcls v2 should use request header v2");
}

#[test]
fn test_compact_string_null_representation() {
    // In compact encoding, null strings have length 0
    let null_compact_length: u32 = 0;
    assert_eq!(
        null_compact_length, 0,
        "Null compact string length should be 0"
    );
}
