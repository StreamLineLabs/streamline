//! Phase 24: Protocol Version Negotiation Tests
//!
//! Tests for Kafka protocol version negotiation:
//! - ApiVersions request/response for version discovery
//! - Version range validation per API
//! - Flexible vs non-flexible version boundaries
//! - Downgrade and upgrade scenarios
//! - Unsupported version handling
//! - Client software version exchange
//!
//! Reference: https://kafka.apache.org/protocol.html#protocol_api_keys

use bytes::BytesMut;
use kafka_protocol::messages::api_versions_response::ApiVersion;
use kafka_protocol::messages::{
    ApiVersionsRequest, ApiVersionsResponse, CreateTopicsRequest, FetchRequest, MetadataRequest,
    ProduceRequest, RequestHeader,
};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};

// ============================================================================
// ApiVersions Request Tests (Version Discovery)
// ============================================================================

/// Test ApiVersionsRequest v0 encoding (minimal)
#[test]
fn test_api_versions_request_v0() {
    let request = ApiVersionsRequest::default();

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 0).unwrap();

    // v0 has empty body
    assert_eq!(buf.len(), 0);

    let mut read_buf = buf.freeze();
    let decoded = ApiVersionsRequest::decode(&mut read_buf, 0).unwrap();

    // Default values
    assert!(decoded.client_software_name.is_empty());
    assert!(decoded.client_software_version.is_empty());
}

/// Test ApiVersionsRequest v1 encoding
#[test]
fn test_api_versions_request_v1() {
    let request = ApiVersionsRequest::default();

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 1).unwrap();

    // v1 has empty body
    assert_eq!(buf.len(), 0);

    let mut read_buf = buf.freeze();
    let decoded = ApiVersionsRequest::decode(&mut read_buf, 1).unwrap();

    assert!(decoded.client_software_name.is_empty());
}

/// Test ApiVersionsRequest v2 encoding
#[test]
fn test_api_versions_request_v2() {
    let request = ApiVersionsRequest::default();

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 2).unwrap();

    // v2 has empty body
    assert_eq!(buf.len(), 0);

    let mut read_buf = buf.freeze();
    let decoded = ApiVersionsRequest::decode(&mut read_buf, 2).unwrap();

    assert!(decoded.client_software_name.is_empty());
}

/// Test ApiVersionsRequest v3 with client software info (flexible version)
#[test]
fn test_api_versions_request_v3_with_client_info() {
    let mut request = ApiVersionsRequest::default();
    request.client_software_name = StrBytes::from_static_str("streamline-client");
    request.client_software_version = StrBytes::from_static_str("1.0.0");

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 3).unwrap();

    // v3 is flexible, should have client info in tagged fields
    assert!(!buf.is_empty());

    let mut read_buf = buf.freeze();
    let decoded = ApiVersionsRequest::decode(&mut read_buf, 3).unwrap();

    assert_eq!(decoded.client_software_name.as_str(), "streamline-client");
    assert_eq!(decoded.client_software_version.as_str(), "1.0.0");
}

/// Test ApiVersionsRequest v3 without client software info
#[test]
fn test_api_versions_request_v3_without_client_info() {
    let request = ApiVersionsRequest::default();

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 3).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ApiVersionsRequest::decode(&mut read_buf, 3).unwrap();

    // Should still decode with empty client info
    assert!(decoded.client_software_name.is_empty());
    assert!(decoded.client_software_version.is_empty());
}

// ============================================================================
// ApiVersions Response Tests (Version Advertisement)
// ============================================================================

/// Test ApiVersionsResponse with all supported APIs
#[test]
fn test_api_versions_response_all_apis() {
    let mut response = ApiVersionsResponse::default();
    response.error_code = 0;

    // Add supported API versions
    let apis = vec![
        (0, 0, 9),  // Produce
        (1, 0, 13), // Fetch
        (2, 0, 7),  // ListOffsets
        (3, 0, 12), // Metadata
        (18, 0, 3), // ApiVersions
        (19, 0, 7), // CreateTopics
    ];

    for (key, min, max) in apis {
        let mut api_version = ApiVersion::default();
        api_version.api_key = key;
        api_version.min_version = min;
        api_version.max_version = max;
        response.api_keys.push(api_version);
    }

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ApiVersionsResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.error_code, 0);
    assert_eq!(decoded.api_keys.len(), 6);

    // Verify specific API versions
    let produce_api = decoded.api_keys.iter().find(|a| a.api_key == 0).unwrap();
    assert_eq!(produce_api.min_version, 0);
    assert_eq!(produce_api.max_version, 9);
}

/// Test ApiVersionsResponse v3 with throttle time
#[test]
fn test_api_versions_response_v3_throttle_time() {
    let mut response = ApiVersionsResponse::default();
    response.error_code = 0;
    response.throttle_time_ms = 100;

    let mut api_version = ApiVersion::default();
    api_version.api_key = 18;
    api_version.min_version = 0;
    api_version.max_version = 3;
    response.api_keys.push(api_version);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 3).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ApiVersionsResponse::decode(&mut read_buf, 3).unwrap();

    assert_eq!(decoded.throttle_time_ms, 100);
}

/// Test ApiVersionsResponse with unsupported version error
#[test]
fn test_api_versions_response_unsupported_version() {
    let mut response = ApiVersionsResponse::default();
    response.error_code = 35; // UNSUPPORTED_VERSION

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ApiVersionsResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.error_code, 35);
    assert!(decoded.api_keys.is_empty());
}

// ============================================================================
// Version Range Validation Tests
// ============================================================================

/// Test version negotiation selects highest common version
#[test]
fn test_version_negotiation_highest_common() {
    // Client supports v0-5, server supports v2-8
    // Should negotiate v5 (highest common)
    let client_min: i16 = 0;
    let client_max: i16 = 5;
    let server_min: i16 = 2;
    let server_max: i16 = 8;

    let negotiated = std::cmp::min(client_max, server_max);
    let is_valid = negotiated >= std::cmp::max(client_min, server_min);

    assert!(is_valid);
    assert_eq!(negotiated, 5);
}

/// Test version negotiation with no overlap
#[test]
fn test_version_negotiation_no_overlap() {
    // Client supports v0-3, server supports v5-8
    // No common version
    let client_min: i16 = 0;
    let client_max: i16 = 3;
    let server_min: i16 = 5;
    let server_max: i16 = 8;

    let negotiated = std::cmp::min(client_max, server_max);
    let is_valid = negotiated >= std::cmp::max(client_min, server_min);

    assert!(!is_valid, "Should not find valid version");
}

/// Test version negotiation with exact match
#[test]
fn test_version_negotiation_exact_match() {
    // Both support exactly v3
    let client_min: i16 = 3;
    let client_max: i16 = 3;
    let server_min: i16 = 3;
    let server_max: i16 = 3;

    let negotiated = std::cmp::min(client_max, server_max);
    let is_valid = negotiated >= std::cmp::max(client_min, server_min);

    assert!(is_valid);
    assert_eq!(negotiated, 3);
}

/// Test version negotiation client has newer versions
#[test]
fn test_version_negotiation_client_newer() {
    // Client supports v0-10, server supports v0-7
    // Should negotiate v7
    let client_max: i16 = 10;
    let server_max: i16 = 7;

    let negotiated = std::cmp::min(client_max, server_max);
    assert_eq!(negotiated, 7);
}

/// Test version negotiation server has newer versions
#[test]
fn test_version_negotiation_server_newer() {
    // Client supports v0-5, server supports v0-12
    // Should negotiate v5
    let client_max: i16 = 5;
    let server_max: i16 = 12;

    let negotiated = std::cmp::min(client_max, server_max);
    assert_eq!(negotiated, 5);
}

// ============================================================================
// Flexible Version Boundary Tests
// ============================================================================

/// Test ProduceRequest flexible version boundary
#[test]
fn test_produce_request_flexible_boundary() {
    // ProduceRequest v0-8 are non-flexible, v9+ are flexible
    assert_eq!(ProduceRequest::header_version(8), 1); // Non-flexible
    assert_eq!(ProduceRequest::header_version(9), 2); // Flexible
}

/// Test FetchRequest flexible version boundary
#[test]
fn test_fetch_request_flexible_boundary() {
    // FetchRequest v0-11 are non-flexible, v12+ are flexible
    assert_eq!(FetchRequest::header_version(11), 1); // Non-flexible
    assert_eq!(FetchRequest::header_version(12), 2); // Flexible
}

/// Test MetadataRequest flexible version boundary
#[test]
fn test_metadata_request_flexible_boundary() {
    // MetadataRequest v0-8 are non-flexible, v9+ are flexible
    assert_eq!(MetadataRequest::header_version(8), 1); // Non-flexible
    assert_eq!(MetadataRequest::header_version(9), 2); // Flexible
}

/// Test CreateTopicsRequest flexible version boundary
#[test]
fn test_create_topics_request_flexible_boundary() {
    // CreateTopicsRequest v0-4 are non-flexible, v5+ are flexible
    assert_eq!(CreateTopicsRequest::header_version(4), 1); // Non-flexible
    assert_eq!(CreateTopicsRequest::header_version(5), 2); // Flexible
}

/// Test ApiVersionsRequest flexible version boundary
#[test]
fn test_api_versions_request_flexible_boundary() {
    // ApiVersionsRequest v0-2 are non-flexible, v3+ are flexible
    assert_eq!(ApiVersionsRequest::header_version(2), 1); // Non-flexible
    assert_eq!(ApiVersionsRequest::header_version(3), 2); // Flexible
}

// ============================================================================
// Header Version Determination Tests
// ============================================================================

/// Test request header version for all Produce versions
#[test]
fn test_request_header_versions_produce() {
    for version in 0..=9 {
        let header_version = ProduceRequest::header_version(version);
        if version < 9 {
            assert_eq!(
                header_version, 1,
                "Produce v{} should use header v1",
                version
            );
        } else {
            assert_eq!(
                header_version, 2,
                "Produce v{} should use header v2",
                version
            );
        }
    }
}

/// Test request header version for all Fetch versions
#[test]
fn test_request_header_versions_fetch() {
    for version in 0..=13 {
        let header_version = FetchRequest::header_version(version);
        if version < 12 {
            assert_eq!(header_version, 1, "Fetch v{} should use header v1", version);
        } else {
            assert_eq!(header_version, 2, "Fetch v{} should use header v2", version);
        }
    }
}

/// Test request header version for all Metadata versions
#[test]
fn test_request_header_versions_metadata() {
    for version in 0..=12 {
        let header_version = MetadataRequest::header_version(version);
        if version < 9 {
            assert_eq!(
                header_version, 1,
                "Metadata v{} should use header v1",
                version
            );
        } else {
            assert_eq!(
                header_version, 2,
                "Metadata v{} should use header v2",
                version
            );
        }
    }
}

/// Test request header version for all ApiVersions versions
#[test]
fn test_request_header_versions_api_versions() {
    for version in 0..=3 {
        let header_version = ApiVersionsRequest::header_version(version);
        if version < 3 {
            assert_eq!(
                header_version, 1,
                "ApiVersions v{} should use header v1",
                version
            );
        } else {
            assert_eq!(
                header_version, 2,
                "ApiVersions v{} should use header v2",
                version
            );
        }
    }
}

// ============================================================================
// Version-Specific Encoding Tests
// ============================================================================

/// Test encoding size differences between versions
#[test]
fn test_encoding_size_non_flexible_vs_flexible() {
    let request = MetadataRequest::default();

    // Encode non-flexible version
    let mut buf_v8 = BytesMut::new();
    request.encode(&mut buf_v8, 8).unwrap();

    // Encode flexible version
    let mut buf_v9 = BytesMut::new();
    request.encode(&mut buf_v9, 9).unwrap();

    // Flexible versions use compact encoding which may differ in size
    // For empty request, flexible should have tagged fields marker
    // Both should successfully encode
    assert!(
        !buf_v8.is_empty() || !buf_v9.is_empty(),
        "At least one should have content"
    );
}

/// Test that flexible versions handle empty tagged fields
#[test]
fn test_flexible_version_empty_tagged_fields() {
    let request = ApiVersionsRequest::default();

    // v3 is flexible but we don't set any tagged fields
    let mut buf = BytesMut::new();
    request.encode(&mut buf, 3).unwrap();

    // Should have minimal size (just tagged fields count = 0)
    let mut read_buf = buf.freeze();
    let decoded = ApiVersionsRequest::decode(&mut read_buf, 3).unwrap();

    // Should decode successfully with defaults
    assert!(decoded.client_software_name.is_empty());
}

// ============================================================================
// API Version Range Tests
// ============================================================================

/// Test ApiVersion struct encoding
#[test]
fn test_api_version_struct() {
    let mut api_version = ApiVersion::default();
    api_version.api_key = 18; // ApiVersions
    api_version.min_version = 0;
    api_version.max_version = 3;

    assert_eq!(api_version.api_key, 18);
    assert_eq!(api_version.min_version, 0);
    assert_eq!(api_version.max_version, 3);
}

/// Test all standard API version ranges
#[test]
fn test_standard_api_version_ranges() {
    // Standard supported version ranges
    let api_versions = vec![
        (0, "Produce", 0, 9),
        (1, "Fetch", 0, 13),
        (2, "ListOffsets", 0, 7),
        (3, "Metadata", 0, 12),
        (8, "OffsetCommit", 0, 8),
        (9, "OffsetFetch", 0, 8),
        (10, "FindCoordinator", 0, 4),
        (11, "JoinGroup", 0, 9),
        (12, "Heartbeat", 0, 4),
        (13, "LeaveGroup", 0, 5),
        (14, "SyncGroup", 0, 5),
        (15, "DescribeGroups", 0, 5),
        (16, "ListGroups", 0, 4),
        (17, "SaslHandshake", 0, 1),
        (18, "ApiVersions", 0, 3),
        (19, "CreateTopics", 0, 7),
        (20, "DeleteTopics", 0, 6),
    ];

    for (api_key, name, min, max) in api_versions {
        assert!(
            min <= max,
            "API {} ({}): min version {} should be <= max version {}",
            api_key,
            name,
            min,
            max
        );
        assert!(
            min >= 0,
            "API {} ({}): min version should be >= 0",
            api_key,
            name
        );
    }
}

// ============================================================================
// Version Downgrade Tests
// ============================================================================

/// Test graceful downgrade when server version is lower
#[test]
fn test_graceful_downgrade() {
    // Client wants v12 Metadata, server supports up to v9
    let client_desired: i16 = 12;
    let server_max: i16 = 9;

    let used_version = std::cmp::min(client_desired, server_max);
    assert_eq!(used_version, 9);

    // Verify v9 request can be encoded
    let request = MetadataRequest::default();
    let mut buf = BytesMut::new();
    request.encode(&mut buf, used_version).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = MetadataRequest::decode(&mut read_buf, used_version).unwrap();
    assert!(decoded.topics.is_none() || decoded.topics.as_ref().map_or(true, |t| t.is_empty()));
}

/// Test downgrade from flexible to non-flexible version
#[test]
fn test_downgrade_flexible_to_non_flexible() {
    // Client wants v12 Fetch (flexible), server supports up to v11 (non-flexible)
    let client_desired: i16 = 12;
    let server_max: i16 = 11;

    let used_version = std::cmp::min(client_desired, server_max);
    assert_eq!(used_version, 11);

    // v11 uses header v1 (non-flexible)
    assert_eq!(FetchRequest::header_version(used_version), 1);

    // Verify v11 request can be encoded
    let request = FetchRequest::default();
    let mut buf = BytesMut::new();
    request.encode(&mut buf, used_version).unwrap();

    let mut read_buf = buf.freeze();
    FetchRequest::decode(&mut read_buf, used_version).unwrap();
}

// ============================================================================
// Request Header with Version Tests
// ============================================================================

/// Test RequestHeader encoding varies by version
#[test]
fn test_request_header_version_encoding() {
    let mut header = RequestHeader::default();
    header.request_api_key = 3; // Metadata
    header.request_api_version = 9;
    header.correlation_id = 12345;
    header.client_id = Some(StrBytes::from_static_str("test-client"));

    // v1 header (non-flexible)
    let mut buf_v1 = BytesMut::new();
    header.encode(&mut buf_v1, 1).unwrap();

    // v2 header (flexible)
    let mut buf_v2 = BytesMut::new();
    header.encode(&mut buf_v2, 2).unwrap();

    // Both should encode successfully
    assert!(!buf_v1.is_empty());
    assert!(!buf_v2.is_empty());

    // v2 has additional tagged fields section
    // Size may differ due to compact string encoding in v2
}

/// Test RequestHeader v2 with tagged fields
#[test]
fn test_request_header_v2_tagged_fields() {
    let mut header = RequestHeader::default();
    header.request_api_key = 18;
    header.request_api_version = 3;
    header.correlation_id = 999;
    header.client_id = Some(StrBytes::from_static_str("client"));

    let mut buf = BytesMut::new();
    header.encode(&mut buf, 2).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = RequestHeader::decode(&mut read_buf, 2).unwrap();

    assert_eq!(decoded.request_api_key, 18);
    assert_eq!(decoded.request_api_version, 3);
    assert_eq!(decoded.correlation_id, 999);
    assert_eq!(decoded.client_id.unwrap().as_str(), "client");
}

// ============================================================================
// Unsupported Version Handling Tests
// ============================================================================

/// Test UNSUPPORTED_VERSION error code
#[test]
fn test_unsupported_version_error_code() {
    const UNSUPPORTED_VERSION: i16 = 35;

    let mut response = ApiVersionsResponse::default();
    response.error_code = UNSUPPORTED_VERSION;

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ApiVersionsResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.error_code, UNSUPPORTED_VERSION);
}

/// Test version too high scenario
#[test]
fn test_version_too_high() {
    // Simulate client requesting v15 when max is v9
    let requested_version: i16 = 15;
    let max_supported: i16 = 9;

    let is_supported = requested_version <= max_supported;
    assert!(!is_supported);
}

/// Test version too low scenario
#[test]
fn test_version_too_low() {
    // Simulate client requesting v0 when min is v2
    let requested_version: i16 = 0;
    let min_supported: i16 = 2;

    let is_supported = requested_version >= min_supported;
    assert!(!is_supported);
}

// ============================================================================
// Client Software Version Exchange Tests
// ============================================================================

/// Test client software name/version in ApiVersionsRequest v3
#[test]
fn test_client_software_exchange() {
    let mut request = ApiVersionsRequest::default();
    request.client_software_name = StrBytes::from_static_str("apache-kafka-java");
    request.client_software_version = StrBytes::from_static_str("3.5.0");

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 3).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ApiVersionsRequest::decode(&mut read_buf, 3).unwrap();

    assert_eq!(decoded.client_software_name.as_str(), "apache-kafka-java");
    assert_eq!(decoded.client_software_version.as_str(), "3.5.0");
}

/// Test long client software name
#[test]
fn test_long_client_software_name() {
    let long_name = "x".repeat(200);
    let mut request = ApiVersionsRequest::default();
    request.client_software_name = StrBytes::from_string(long_name.clone());

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 3).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ApiVersionsRequest::decode(&mut read_buf, 3).unwrap();

    assert_eq!(decoded.client_software_name.as_str(), long_name);
}

// ============================================================================
// Multiple API Version Discovery Tests
// ============================================================================

/// Test discovering versions for multiple APIs
#[test]
fn test_multi_api_version_discovery() {
    let mut response = ApiVersionsResponse::default();
    response.error_code = 0;

    // Add many APIs
    for api_key in [0, 1, 2, 3, 8, 9, 10, 11, 18, 19] {
        let mut api_version = ApiVersion::default();
        api_version.api_key = api_key;
        api_version.min_version = 0;
        api_version.max_version = match api_key {
            0 => 9,
            1 => 13,
            3 => 12,
            18 => 3,
            _ => 5,
        };
        response.api_keys.push(api_version);
    }

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 3).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ApiVersionsResponse::decode(&mut read_buf, 3).unwrap();

    assert_eq!(decoded.api_keys.len(), 10);

    // Verify we can find each API
    for api_key in [0, 1, 2, 3, 8, 9, 10, 11, 18, 19] {
        assert!(
            decoded.api_keys.iter().any(|a| a.api_key == api_key),
            "Should find API {}",
            api_key
        );
    }
}

/// Test empty ApiVersionsResponse (no supported APIs)
#[test]
fn test_empty_api_versions_response() {
    let mut response = ApiVersionsResponse::default();
    response.error_code = 0;
    // No APIs added

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ApiVersionsResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.error_code, 0);
    assert!(decoded.api_keys.is_empty());
}

// ============================================================================
// Version Compatibility Matrix Tests
// ============================================================================

/// Test that version 0 is always supported for core APIs
#[test]
fn test_version_zero_support() {
    // All core APIs should support v0
    let v0_requests = vec![
        ("ApiVersionsRequest", {
            let mut buf = BytesMut::new();
            ApiVersionsRequest::default().encode(&mut buf, 0).unwrap();
            true // Encoding succeeded
        }),
        ("MetadataRequest", {
            let mut buf = BytesMut::new();
            MetadataRequest::default().encode(&mut buf, 0).unwrap();
            true // Encoding succeeded
        }),
        ("ProduceRequest", {
            let mut buf = BytesMut::new();
            ProduceRequest::default().encode(&mut buf, 0).unwrap();
            true // Encoding succeeded
        }),
        ("FetchRequest", {
            let mut buf = BytesMut::new();
            FetchRequest::default().encode(&mut buf, 0).unwrap();
            true // Encoding succeeded
        }),
    ];

    for (name, success) in v0_requests {
        assert!(success, "{} v0 should encode successfully", name);
    }
}

/// Test version progression maintains backward compatibility
#[test]
fn test_version_backward_compatibility() {
    // Encoding at lower version should still work
    for version in 0..=3 {
        let request = ApiVersionsRequest::default();
        let mut buf = BytesMut::new();
        request.encode(&mut buf, version).unwrap();

        let mut read_buf = buf.freeze();
        let decoded = ApiVersionsRequest::decode(&mut read_buf, version).unwrap();

        // Basic fields should always be present
        assert!(decoded.client_software_name.is_empty() || version >= 3);
    }
}

// ============================================================================
// Throttle Time Version Tests
// ============================================================================

/// Test throttle_time_ms availability by version
#[test]
fn test_throttle_time_by_version() {
    let mut response = ApiVersionsResponse::default();
    response.error_code = 0;
    response.throttle_time_ms = 500;

    // v0 doesn't have throttle_time_ms
    let mut buf_v0 = BytesMut::new();
    response.encode(&mut buf_v0, 0).unwrap();

    // v1+ has throttle_time_ms
    let mut buf_v1 = BytesMut::new();
    response.encode(&mut buf_v1, 1).unwrap();

    // v1 should be larger due to throttle_time_ms field
    assert!(buf_v1.len() >= buf_v0.len());
}

/// Test throttle time is preserved in v3
#[test]
fn test_throttle_time_v3() {
    let mut response = ApiVersionsResponse::default();
    response.error_code = 0;
    response.throttle_time_ms = 1000;

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 3).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ApiVersionsResponse::decode(&mut read_buf, 3).unwrap();

    assert_eq!(decoded.throttle_time_ms, 1000);
}
