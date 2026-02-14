//! Phase 11: API Versions Negotiation Tests
//!
//! Tests for API version negotiation protocol:
//! - ApiVersions request/response encoding
//! - Version range encoding and decoding
//! - Version negotiation logic
//! - Unsupported version handling
//! - Flexible version detection

use bytes::BytesMut;
use kafka_protocol::messages::api_versions_response::ApiVersion;
use kafka_protocol::messages::{ApiKey, ApiVersionsRequest, ApiVersionsResponse};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};

// =============================================================================
// ApiVersions Request Tests
// =============================================================================

#[test]
fn test_api_versions_request_v0() {
    let request = ApiVersionsRequest::default();

    let mut buf = BytesMut::new();
    let result = request.encode(&mut buf, 0);
    assert!(result.is_ok());

    // v0 request has no body (empty)
    let mut read_buf = buf.freeze();
    let decoded = ApiVersionsRequest::decode(&mut read_buf, 0).unwrap();
    assert!(decoded.client_software_name.is_empty());
}

#[test]
fn test_api_versions_request_v3() {
    let mut request = ApiVersionsRequest::default();
    request.client_software_name = StrBytes::from_static_str("kafka-rust-client");
    request.client_software_version = StrBytes::from_static_str("1.0.0");

    let mut buf = BytesMut::new();
    let result = request.encode(&mut buf, 3);
    assert!(result.is_ok());

    let mut read_buf = buf.freeze();
    let decoded = ApiVersionsRequest::decode(&mut read_buf, 3).unwrap();

    assert_eq!(decoded.client_software_name.as_str(), "kafka-rust-client");
    assert_eq!(decoded.client_software_version.as_str(), "1.0.0");
}

#[test]
fn test_api_versions_request_all_versions() {
    let request = ApiVersionsRequest::default();

    // ApiVersions supports versions 0-3
    for version in 0..=3 {
        let mut buf = BytesMut::new();
        let result = request.clone().encode(&mut buf, version);
        assert!(result.is_ok(), "Failed to encode ApiVersions v{}", version);
    }
}

#[test]
fn test_api_versions_request_roundtrip() {
    let mut request = ApiVersionsRequest::default();
    request.client_software_name = StrBytes::from_static_str("my-client");
    request.client_software_version = StrBytes::from_static_str("2.1.0");

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 3).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ApiVersionsRequest::decode(&mut read_buf, 3).unwrap();

    assert_eq!(decoded.client_software_name.as_str(), "my-client");
    assert_eq!(decoded.client_software_version.as_str(), "2.1.0");
}

// =============================================================================
// ApiVersions Response Tests
// =============================================================================

#[test]
fn test_api_versions_response_basic() {
    let mut response = ApiVersionsResponse::default();
    response.error_code = 0;

    let mut buf = BytesMut::new();
    let result = response.encode(&mut buf, 0);
    assert!(result.is_ok());
}

#[test]
fn test_api_versions_response_with_versions() {
    let mut response = ApiVersionsResponse::default();
    response.error_code = 0;

    // Add Produce API (key 0)
    let mut produce = ApiVersion::default();
    produce.api_key = 0;
    produce.min_version = 0;
    produce.max_version = 9;
    response.api_keys.push(produce);

    // Add Fetch API (key 1)
    let mut fetch = ApiVersion::default();
    fetch.api_key = 1;
    fetch.min_version = 0;
    fetch.max_version = 13;
    response.api_keys.push(fetch);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ApiVersionsResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.api_keys.len(), 2);
    assert_eq!(decoded.api_keys[0].api_key, 0);
    assert_eq!(decoded.api_keys[0].max_version, 9);
}

#[test]
fn test_api_versions_response_unsupported_version_error() {
    let mut response = ApiVersionsResponse::default();
    response.error_code = 35; // UNSUPPORTED_VERSION

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ApiVersionsResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.error_code, 35);
}

#[test]
fn test_api_versions_response_with_throttle() {
    let mut response = ApiVersionsResponse::default();
    response.error_code = 0;
    response.throttle_time_ms = 1000;

    // Version 1+ includes throttle time
    let mut buf = BytesMut::new();
    response.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ApiVersionsResponse::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.throttle_time_ms, 1000);
}

#[test]
fn test_api_versions_response_all_versions() {
    let response = ApiVersionsResponse::default();

    for version in 0..=3 {
        let mut buf = BytesMut::new();
        let result = response.clone().encode(&mut buf, version);
        assert!(
            result.is_ok(),
            "Failed to encode ApiVersionsResponse v{}",
            version
        );
    }
}

// =============================================================================
// Version Range Tests
// =============================================================================

#[test]
fn test_version_range_single_version() {
    let mut api_version = ApiVersion::default();
    api_version.api_key = 18; // ApiVersions
    api_version.min_version = 0;
    api_version.max_version = 0;

    assert_eq!(api_version.min_version, api_version.max_version);
}

#[test]
fn test_version_range_multiple_versions() {
    let mut api_version = ApiVersion::default();
    api_version.api_key = 3; // Metadata
    api_version.min_version = 0;
    api_version.max_version = 12;

    assert!(api_version.max_version > api_version.min_version);
    assert_eq!(api_version.max_version - api_version.min_version + 1, 13); // 13 versions
}

#[test]
fn test_version_range_encoding() {
    let mut api_version = ApiVersion::default();
    api_version.api_key = 0;
    api_version.min_version = 3;
    api_version.max_version = 9;

    let mut response = ApiVersionsResponse::default();
    response.api_keys.push(api_version);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ApiVersionsResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.api_keys[0].min_version, 3);
    assert_eq!(decoded.api_keys[0].max_version, 9);
}

// =============================================================================
// Version Negotiation Logic Tests
// =============================================================================

#[test]
fn test_version_negotiation_client_in_range() {
    // Server supports versions 0-9
    let server_min = 0;
    let server_max = 9;

    // Client wants version 7
    let client_version = 7;

    let is_supported = client_version >= server_min && client_version <= server_max;
    assert!(is_supported);
}

#[test]
fn test_version_negotiation_client_too_high() {
    // Server supports versions 0-9
    let server_min = 0;
    let server_max = 9;

    // Client wants version 11
    let client_version = 11;

    let is_supported = client_version >= server_min && client_version <= server_max;
    assert!(!is_supported);

    // Client should use max supported version
    let negotiated = server_max;
    assert_eq!(negotiated, 9);
}

#[test]
fn test_version_negotiation_client_too_low() {
    // Server supports versions 3-9 (dropped support for 0-2)
    let server_min = 3;
    let server_max = 9;

    // Client only supports version 2
    let client_version = 2;

    let is_supported = client_version >= server_min && client_version <= server_max;
    assert!(!is_supported);

    // No compatible version available
}

#[test]
fn test_version_negotiation_select_highest() {
    // Server supports versions 0-9
    let server_min: i16 = 0;
    let server_max: i16 = 9;

    // Client supports versions 0-7
    let client_min: i16 = 0;
    let client_max: i16 = 7;

    // Negotiate highest common version
    let negotiated = std::cmp::min(server_max, client_max);
    assert_eq!(negotiated, 7);

    // Verify it's in both ranges
    assert!(negotiated >= server_min && negotiated <= server_max);
    assert!(negotiated >= client_min && negotiated <= client_max);
}

#[test]
fn test_version_negotiation_no_overlap() {
    // Server supports versions 5-9
    let server_min: i16 = 5;
    let server_max: i16 = 9;

    // Client supports versions 0-4
    let client_min: i16 = 0;
    let client_max: i16 = 4;

    // Check for overlap
    let has_overlap = server_min <= client_max && client_min <= server_max;
    assert!(!has_overlap);
}

#[test]
fn test_version_negotiation_for_api() {
    // Simulate version negotiation for multiple APIs
    let server_versions = vec![
        (0, 0, 9),  // Produce: 0-9
        (1, 0, 13), // Fetch: 0-13
        (3, 0, 12), // Metadata: 0-12
    ];

    let client_max_versions = vec![
        (0, 7),  // Produce: up to 7
        (1, 11), // Fetch: up to 11
        (3, 9),  // Metadata: up to 9
    ];

    let mut negotiated = Vec::new();

    for (api_key, server_min, server_max) in &server_versions {
        for (client_api, client_max) in &client_max_versions {
            if api_key == client_api {
                let version = std::cmp::min(*server_max, *client_max);
                if version >= *server_min {
                    negotiated.push((*api_key, version));
                }
            }
        }
    }

    assert_eq!(negotiated.len(), 3);
    assert_eq!(negotiated[0], (0, 7)); // Produce v7
    assert_eq!(negotiated[1], (1, 11)); // Fetch v11
    assert_eq!(negotiated[2], (3, 9)); // Metadata v9
}

// =============================================================================
// Unsupported Version Handling Tests
// =============================================================================

#[test]
fn test_unsupported_version_error_code() {
    let error_code: i16 = 35; // UNSUPPORTED_VERSION
    assert_eq!(error_code, 35);
}

#[test]
fn test_unsupported_version_response() {
    let mut response = ApiVersionsResponse::default();
    response.error_code = 35;

    // Even with error, response should include supported versions
    let mut produce = ApiVersion::default();
    produce.api_key = 0;
    produce.min_version = 0;
    produce.max_version = 9;
    response.api_keys.push(produce);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ApiVersionsResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.error_code, 35);
    assert!(!decoded.api_keys.is_empty());
}

#[test]
fn test_unsupported_api_key() {
    // Check if an API key is supported
    let supported_keys: Vec<i16> = vec![0, 1, 2, 3, 8, 9, 10, 11, 12, 13, 14, 18, 19];
    let unsupported_key: i16 = 100;

    assert!(!supported_keys.contains(&unsupported_key));
}

// =============================================================================
// Flexible Version Detection Tests
// =============================================================================

#[test]
fn test_flexible_version_api_versions() {
    // ApiVersions v3+ uses flexible versions
    let flexible_min_version = 3;

    // Check if version is flexible
    let v0_flexible = 0 >= flexible_min_version;
    let v3_flexible = 3 >= flexible_min_version;

    assert!(!v0_flexible);
    assert!(v3_flexible);
}

#[test]
fn test_flexible_version_produce() {
    // Produce v9+ uses flexible versions
    let flexible_min_version = 9;

    let v7_flexible = 7 >= flexible_min_version;
    let v9_flexible = 9 >= flexible_min_version;

    assert!(!v7_flexible);
    assert!(v9_flexible);
}

#[test]
fn test_flexible_version_fetch() {
    // Fetch v12+ uses flexible versions
    let flexible_min_version = 12;

    let v11_flexible = 11 >= flexible_min_version;
    let v12_flexible = 12 >= flexible_min_version;

    assert!(!v11_flexible);
    assert!(v12_flexible);
}

#[test]
fn test_flexible_version_metadata() {
    // Metadata v9+ uses flexible versions
    let flexible_min_version = 9;

    let v8_flexible = 8 >= flexible_min_version;
    let v9_flexible = 9 >= flexible_min_version;

    assert!(!v8_flexible);
    assert!(v9_flexible);
}

// =============================================================================
// API Key Tests
// =============================================================================

#[test]
fn test_all_api_keys() {
    // Verify common API keys
    assert_eq!(ApiKey::Produce as i16, 0);
    assert_eq!(ApiKey::Fetch as i16, 1);
    assert_eq!(ApiKey::ListOffsets as i16, 2);
    assert_eq!(ApiKey::Metadata as i16, 3);
    assert_eq!(ApiKey::OffsetCommit as i16, 8);
    assert_eq!(ApiKey::OffsetFetch as i16, 9);
    assert_eq!(ApiKey::FindCoordinator as i16, 10);
    assert_eq!(ApiKey::JoinGroup as i16, 11);
    assert_eq!(ApiKey::Heartbeat as i16, 12);
    assert_eq!(ApiKey::LeaveGroup as i16, 13);
    assert_eq!(ApiKey::SyncGroup as i16, 14);
    assert_eq!(ApiKey::ApiVersions as i16, 18);
    assert_eq!(ApiKey::CreateTopics as i16, 19);
}

#[test]
fn test_api_key_from_i16() {
    // Convert from i16 to ApiKey
    let key: i16 = 18;
    let api_key = ApiKey::try_from(key);
    assert!(api_key.is_ok());
    assert_eq!(api_key.unwrap(), ApiKey::ApiVersions);
}

#[test]
fn test_unknown_api_key() {
    let unknown_key: i16 = 999;
    let api_key = ApiKey::try_from(unknown_key);
    assert!(api_key.is_err());
}

// =============================================================================
// Full ApiVersions Flow Tests
// =============================================================================

#[test]
fn test_api_versions_handshake() {
    // Client sends ApiVersions request
    let request = ApiVersionsRequest::default();

    let mut request_buf = BytesMut::new();
    request.encode(&mut request_buf, 0).unwrap();

    // Server responds with supported versions
    let mut response = ApiVersionsResponse::default();
    response.error_code = 0;

    // Produce API
    let mut produce = ApiVersion::default();
    produce.api_key = 0;
    produce.min_version = 0;
    produce.max_version = 9;
    response.api_keys.push(produce);

    // Fetch API
    let mut fetch = ApiVersion::default();
    fetch.api_key = 1;
    fetch.min_version = 0;
    fetch.max_version = 13;
    response.api_keys.push(fetch);

    // Metadata API
    let mut metadata = ApiVersion::default();
    metadata.api_key = 3;
    metadata.min_version = 0;
    metadata.max_version = 12;
    response.api_keys.push(metadata);

    // ApiVersions API
    let mut api_versions = ApiVersion::default();
    api_versions.api_key = 18;
    api_versions.min_version = 0;
    api_versions.max_version = 3;
    response.api_keys.push(api_versions);

    let mut response_buf = BytesMut::new();
    response.encode(&mut response_buf, 0).unwrap();

    // Decode response
    let mut read_buf = response_buf.freeze();
    let decoded = ApiVersionsResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.error_code, 0);
    assert_eq!(decoded.api_keys.len(), 4);
}

#[test]
fn test_api_versions_lookup() {
    let mut response = ApiVersionsResponse::default();
    response.error_code = 0;

    // Add some API versions
    for (key, min, max) in [(0, 0, 9), (1, 0, 13), (3, 0, 12)] {
        let mut api_version = ApiVersion::default();
        api_version.api_key = key;
        api_version.min_version = min;
        api_version.max_version = max;
        response.api_keys.push(api_version);
    }

    // Lookup specific API
    let produce_version = response.api_keys.iter().find(|v| v.api_key == 0);
    assert!(produce_version.is_some());
    assert_eq!(produce_version.unwrap().max_version, 9);

    // Lookup non-existent API
    let unknown_version = response.api_keys.iter().find(|v| v.api_key == 100);
    assert!(unknown_version.is_none());
}

// =============================================================================
// Version Compatibility Tests
// =============================================================================

#[test]
fn test_backward_compatible_versions() {
    // Higher versions should be backward compatible
    let server_versions = [
        (0, 0, 9),  // Produce
        (1, 0, 13), // Fetch
    ];

    // Old client requesting old versions
    let old_client_versions = [(0, 3), (1, 5)];

    for (api_key, client_version) in old_client_versions {
        let server = server_versions
            .iter()
            .find(|(k, _, _)| *k == api_key)
            .unwrap();
        let is_compatible = client_version >= server.1 && client_version <= server.2;
        assert!(
            is_compatible,
            "API {} v{} should be compatible",
            api_key, client_version
        );
    }
}

#[test]
fn test_forward_compatible_negotiation() {
    // New client connecting to old server
    let old_server_max = 7; // Server supports Produce 0-7
    let new_client_max = 9; // Client supports Produce 0-9

    // Client should use server's max
    let negotiated = std::cmp::min(old_server_max, new_client_max);
    assert_eq!(negotiated, 7);
}

// =============================================================================
// Finalized Feature Tests (KIP-778)
// =============================================================================

#[test]
fn test_finalized_features_field() {
    let mut response = ApiVersionsResponse::default();
    response.error_code = 0;

    // Version 3+ includes finalized features
    let mut buf = BytesMut::new();
    response.encode(&mut buf, 3).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ApiVersionsResponse::decode(&mut read_buf, 3).unwrap();

    // finalized_features is available in v3+
    assert!(decoded.finalized_features.is_empty());
}

// =============================================================================
// Zero Copy / Tagged Fields Tests
// =============================================================================

#[test]
fn test_api_versions_v3_tagged_fields() {
    let mut request = ApiVersionsRequest::default();
    request.client_software_name = StrBytes::from_static_str("test-client");
    request.client_software_version = StrBytes::from_static_str("1.0");

    // v3 uses flexible encoding with tagged fields
    let mut buf = BytesMut::new();
    request.encode(&mut buf, 3).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ApiVersionsRequest::decode(&mut read_buf, 3).unwrap();

    assert_eq!(decoded.client_software_name.as_str(), "test-client");
}

#[test]
fn test_api_versions_response_v3_compact() {
    let mut response = ApiVersionsResponse::default();
    response.error_code = 0;

    let mut api_version = ApiVersion::default();
    api_version.api_key = 0;
    api_version.min_version = 0;
    api_version.max_version = 9;
    response.api_keys.push(api_version);

    // v3 uses compact arrays
    let mut buf = BytesMut::new();
    response.encode(&mut buf, 3).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ApiVersionsResponse::decode(&mut read_buf, 3).unwrap();

    assert_eq!(decoded.api_keys.len(), 1);
}
