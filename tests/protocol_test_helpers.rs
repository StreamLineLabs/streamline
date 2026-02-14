//! Test helpers for Kafka protocol testing
//!
//! This module provides utilities for:
//! - Building raw Kafka requests for any API version
//! - Parsing and validating responses
//! - Test server setup and management
//! - Assertion helpers for protocol compliance

#![allow(dead_code)]

use bytes::BytesMut;
use kafka_protocol::messages::*;
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::protocol::{Decodable, Encodable};

/// Build a size-prefixed Kafka request with proper header encoding
///
/// # Arguments
/// * `api_key` - The Kafka API key
/// * `api_version` - The API version to use
/// * `correlation_id` - The correlation ID for the request
/// * `client_id` - Optional client ID
/// * `request` - The request body to encode
///
/// # Returns
/// The complete request bytes including size prefix
pub fn build_request<T: Encodable>(
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    client_id: Option<&str>,
    request: &T,
) -> Vec<u8> {
    let header_version = request_header_version(api_key, api_version);

    // Build request header
    let mut header = RequestHeader::default()
        .with_request_api_key(api_key)
        .with_request_api_version(api_version)
        .with_correlation_id(correlation_id);

    if let Some(id) = client_id {
        header = header.with_client_id(Some(id.to_string().into()));
    }

    // Encode header
    let mut header_buf = BytesMut::new();
    header
        .encode(&mut header_buf, header_version)
        .expect("Failed to encode request header");

    // Encode body
    let mut body_buf = BytesMut::new();
    request
        .encode(&mut body_buf, api_version)
        .expect("Failed to encode request body");

    // Combine with size prefix
    let total_size = header_buf.len() + body_buf.len();
    let mut result = Vec::with_capacity(4 + total_size);
    result.extend_from_slice(&(total_size as u32).to_be_bytes());
    result.extend_from_slice(&header_buf);
    result.extend_from_slice(&body_buf);

    result
}

/// Build a raw ApiVersions request
pub fn build_api_versions_request(api_version: i16, correlation_id: i32) -> Vec<u8> {
    let request = ApiVersionsRequest::default();
    build_request(
        18,
        api_version,
        correlation_id,
        Some("test-client"),
        &request,
    )
}

/// Build a raw Metadata request
pub fn build_metadata_request(
    api_version: i16,
    correlation_id: i32,
    topics: Option<Vec<&str>>,
) -> Vec<u8> {
    use kafka_protocol::messages::metadata_request::MetadataRequestTopic;

    let topic_list = topics.map(|t| {
        t.into_iter()
            .map(|name| {
                MetadataRequestTopic::default().with_name(Some(TopicName::from(
                    StrBytes::from_string(name.to_string()),
                )))
            })
            .collect()
    });

    let request = MetadataRequest::default().with_topics(topic_list);
    build_request(
        3,
        api_version,
        correlation_id,
        Some("test-client"),
        &request,
    )
}

/// Parse a size-prefixed response, returning the response body
///
/// # Arguments
/// * `data` - Raw response bytes including size prefix
///
/// # Returns
/// The response body bytes (without size prefix)
pub fn parse_response(data: &[u8]) -> Option<&[u8]> {
    if data.len() < 4 {
        return None;
    }

    let size = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
    if data.len() < 4 + size {
        return None;
    }

    Some(&data[4..4 + size])
}

/// Extract correlation ID from a response
pub fn extract_correlation_id(response_body: &[u8]) -> Option<i32> {
    if response_body.len() < 4 {
        return None;
    }

    Some(i32::from_be_bytes([
        response_body[0],
        response_body[1],
        response_body[2],
        response_body[3],
    ]))
}

/// Parse an ApiVersionsResponse from raw response bytes
pub fn parse_api_versions_response(
    response_body: &[u8],
    api_version: i16,
) -> Result<ApiVersionsResponse, String> {
    let response_header_ver = response_header_version(18, api_version);

    let mut buf = bytes::Bytes::copy_from_slice(response_body);

    // Parse response header
    let _header = ResponseHeader::decode(&mut buf, response_header_ver)
        .map_err(|e| format!("Failed to decode response header: {}", e))?;

    // Parse response body
    ApiVersionsResponse::decode(&mut buf, api_version)
        .map_err(|e| format!("Failed to decode ApiVersionsResponse: {}", e))
}

/// Parse a MetadataResponse from raw response bytes
pub fn parse_metadata_response(
    response_body: &[u8],
    api_version: i16,
) -> Result<MetadataResponse, String> {
    let response_header_ver = response_header_version(3, api_version);

    let mut buf = bytes::Bytes::copy_from_slice(response_body);

    // Parse response header
    let _header = ResponseHeader::decode(&mut buf, response_header_ver)
        .map_err(|e| format!("Failed to decode response header: {}", e))?;

    // Parse response body
    MetadataResponse::decode(&mut buf, api_version)
        .map_err(|e| format!("Failed to decode MetadataResponse: {}", e))
}

/// Get the request header version for a given API key and version
///
/// This matches the Kafka protocol specification for header versioning.
pub fn request_header_version(api_key: i16, api_version: i16) -> i16 {
    match api_key {
        // ApiVersions
        18 if api_version >= 3 => 2,
        18 => 1,
        // Metadata
        3 if api_version >= 9 => 2,
        // Produce
        0 if api_version >= 9 => 2,
        // Fetch
        1 if api_version >= 12 => 2,
        // ListOffsets
        2 if api_version >= 6 => 2,
        // CreateTopics
        19 if api_version >= 5 => 2,
        // DeleteTopics
        20 if api_version >= 4 => 2,
        // SaslHandshake - never flexible
        17 => 1,
        // SaslAuthenticate
        36 if api_version >= 2 => 2,
        // FindCoordinator
        10 if api_version >= 3 => 2,
        // JoinGroup
        11 if api_version >= 6 => 2,
        // SyncGroup
        14 if api_version >= 4 => 2,
        // Heartbeat
        12 if api_version >= 4 => 2,
        // LeaveGroup
        13 if api_version >= 4 => 2,
        // OffsetFetch
        9 if api_version >= 6 => 2,
        // OffsetCommit
        8 if api_version >= 8 => 2,
        // DescribeGroups
        15 if api_version >= 5 => 2,
        // ListGroups
        16 if api_version >= 3 => 2,
        // DeleteGroups
        42 if api_version >= 2 => 2,
        // DescribeAcls
        29 if api_version >= 2 => 2,
        // CreateAcls
        30 if api_version >= 2 => 2,
        // DeleteAcls
        31 if api_version >= 2 => 2,
        // InitProducerId
        22 if api_version >= 2 => 2,
        // AddPartitionsToTxn
        24 if api_version >= 3 => 2,
        // AddOffsetsToTxn
        25 if api_version >= 3 => 2,
        // EndTxn
        26 if api_version >= 3 => 2,
        // TxnOffsetCommit
        28 if api_version >= 3 => 2,
        // DescribeConfigs
        32 if api_version >= 4 => 2,
        // AlterConfigs
        33 if api_version >= 2 => 2,
        // CreatePartitions
        37 if api_version >= 2 => 2,
        // DescribeCluster - always flexible
        60 => 2,
        // DeleteRecords
        21 if api_version >= 2 => 2,
        // IncrementalAlterConfigs - always flexible
        44 => 2,
        // OffsetForLeaderEpoch
        23 if api_version >= 4 => 2,
        _ => 1,
    }
}

/// Get the response header version for a given API key and version
///
/// This matches the Kafka protocol specification for header versioning.
pub fn response_header_version(api_key: i16, api_version: i16) -> i16 {
    match api_key {
        // ApiVersions - ALWAYS v0 (bootstrap response)
        18 => 0,
        // Metadata
        3 if api_version >= 9 => 1,
        // Produce
        0 if api_version >= 9 => 1,
        // Fetch
        1 if api_version >= 12 => 1,
        // ListOffsets
        2 if api_version >= 6 => 1,
        // CreateTopics
        19 if api_version >= 5 => 1,
        // DeleteTopics
        20 if api_version >= 4 => 1,
        // SaslHandshake - never flexible
        17 => 0,
        // SaslAuthenticate
        36 if api_version >= 2 => 1,
        // FindCoordinator
        10 if api_version >= 3 => 1,
        // JoinGroup
        11 if api_version >= 6 => 1,
        // SyncGroup
        14 if api_version >= 4 => 1,
        // Heartbeat
        12 if api_version >= 4 => 1,
        // LeaveGroup
        13 if api_version >= 4 => 1,
        // OffsetFetch
        9 if api_version >= 6 => 1,
        // OffsetCommit
        8 if api_version >= 8 => 1,
        // DescribeGroups
        15 if api_version >= 5 => 1,
        // ListGroups
        16 if api_version >= 3 => 1,
        // DeleteGroups
        42 if api_version >= 2 => 1,
        // DescribeAcls
        29 if api_version >= 2 => 1,
        // CreateAcls
        30 if api_version >= 2 => 1,
        // DeleteAcls
        31 if api_version >= 2 => 1,
        // InitProducerId
        22 if api_version >= 2 => 1,
        // AddPartitionsToTxn
        24 if api_version >= 3 => 1,
        // AddOffsetsToTxn
        25 if api_version >= 3 => 1,
        // EndTxn
        26 if api_version >= 3 => 1,
        // TxnOffsetCommit
        28 if api_version >= 3 => 1,
        // DescribeConfigs
        32 if api_version >= 4 => 1,
        // AlterConfigs
        33 if api_version >= 2 => 1,
        // CreatePartitions
        37 if api_version >= 2 => 1,
        // DescribeCluster - always flexible
        60 => 1,
        // DeleteRecords
        21 if api_version >= 2 => 1,
        // IncrementalAlterConfigs - always flexible
        44 => 1,
        // OffsetForLeaderEpoch
        23 if api_version >= 4 => 1,
        _ => 0,
    }
}

/// Expected header versions for all supported APIs
/// Returns (request_header_v2_starts_at, response_header_v1_starts_at)
/// where None means "never" for that API
pub fn expected_header_versions(api_key: i16) -> (Option<i16>, Option<i16>) {
    match api_key {
        // ApiVersions - request v2 at v3, response NEVER v1
        18 => (Some(3), None),
        // Metadata
        3 => (Some(9), Some(9)),
        // Produce
        0 => (Some(9), Some(9)),
        // Fetch
        1 => (Some(12), Some(12)),
        // ListOffsets
        2 => (Some(6), Some(6)),
        // CreateTopics
        19 => (Some(5), Some(5)),
        // DeleteTopics
        20 => (Some(4), Some(4)),
        // SaslHandshake - never flexible
        17 => (None, None),
        // SaslAuthenticate
        36 => (Some(2), Some(2)),
        // FindCoordinator
        10 => (Some(3), Some(3)),
        // JoinGroup
        11 => (Some(6), Some(6)),
        // SyncGroup
        14 => (Some(4), Some(4)),
        // Heartbeat
        12 => (Some(4), Some(4)),
        // LeaveGroup
        13 => (Some(4), Some(4)),
        // OffsetFetch
        9 => (Some(6), Some(6)),
        // OffsetCommit
        8 => (Some(8), Some(8)),
        // DescribeGroups
        15 => (Some(5), Some(5)),
        // ListGroups
        16 => (Some(3), Some(3)),
        // DeleteGroups
        42 => (Some(2), Some(2)),
        // DescribeAcls
        29 => (Some(2), Some(2)),
        // CreateAcls
        30 => (Some(2), Some(2)),
        // DeleteAcls
        31 => (Some(2), Some(2)),
        // InitProducerId
        22 => (Some(2), Some(2)),
        // AddPartitionsToTxn
        24 => (Some(3), Some(3)),
        // AddOffsetsToTxn
        25 => (Some(3), Some(3)),
        // EndTxn
        26 => (Some(3), Some(3)),
        // TxnOffsetCommit
        28 => (Some(3), Some(3)),
        // DescribeConfigs
        32 => (Some(4), Some(4)),
        // AlterConfigs
        33 => (Some(2), Some(2)),
        // CreatePartitions
        37 => (Some(2), Some(2)),
        // DescribeCluster - always flexible from v0
        60 => (Some(0), Some(0)),
        // DeleteRecords
        21 => (Some(2), Some(2)),
        // IncrementalAlterConfigs - always flexible from v0
        44 => (Some(0), Some(0)),
        // OffsetForLeaderEpoch
        23 => (Some(4), Some(4)),
        // Unknown API - default to non-flexible
        _ => (None, None),
    }
}

/// API key name mapping
pub fn api_key_name(api_key: i16) -> &'static str {
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

/// All supported API keys for testing
pub const SUPPORTED_API_KEYS: &[i16] = &[
    0,  // Produce
    1,  // Fetch
    2,  // ListOffsets
    3,  // Metadata
    8,  // OffsetCommit
    9,  // OffsetFetch
    10, // FindCoordinator
    11, // JoinGroup
    12, // Heartbeat
    13, // LeaveGroup
    14, // SyncGroup
    15, // DescribeGroups
    16, // ListGroups
    17, // SaslHandshake
    18, // ApiVersions
    19, // CreateTopics
    20, // DeleteTopics
    21, // DeleteRecords
    22, // InitProducerId
    23, // OffsetForLeaderEpoch
    24, // AddPartitionsToTxn
    25, // AddOffsetsToTxn
    26, // EndTxn
    28, // TxnOffsetCommit
    29, // DescribeAcls
    30, // CreateAcls
    31, // DeleteAcls
    32, // DescribeConfigs
    33, // AlterConfigs
    36, // SaslAuthenticate
    37, // CreatePartitions
    42, // DeleteGroups
    44, // IncrementalAlterConfigs
    60, // DescribeCluster
];

/// Get the maximum supported version for an API key
pub fn max_supported_version(api_key: i16) -> i16 {
    match api_key {
        0 => 9,  // Produce
        1 => 12, // Fetch (v13+ uses topic_id)
        2 => 7,  // ListOffsets
        3 => 12, // Metadata
        8 => 8,  // OffsetCommit
        9 => 8,  // OffsetFetch
        10 => 4, // FindCoordinator
        11 => 9, // JoinGroup
        12 => 4, // Heartbeat
        13 => 5, // LeaveGroup
        14 => 5, // SyncGroup
        15 => 5, // DescribeGroups
        16 => 4, // ListGroups
        17 => 1, // SaslHandshake
        18 => 3, // ApiVersions
        19 => 7, // CreateTopics
        20 => 6, // DeleteTopics
        21 => 2, // DeleteRecords
        22 => 4, // InitProducerId
        23 => 4, // OffsetForLeaderEpoch
        24 => 4, // AddPartitionsToTxn
        25 => 3, // AddOffsetsToTxn
        26 => 3, // EndTxn
        28 => 3, // TxnOffsetCommit
        29 => 3, // DescribeAcls
        30 => 3, // CreateAcls
        31 => 3, // DeleteAcls
        32 => 4, // DescribeConfigs
        33 => 2, // AlterConfigs
        36 => 2, // SaslAuthenticate
        37 => 3, // CreatePartitions
        42 => 2, // DeleteGroups
        44 => 1, // IncrementalAlterConfigs
        60 => 0, // DescribeCluster
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_header_version_api_versions() {
        // ApiVersions v0-2 use header v1, v3+ use header v2
        assert_eq!(request_header_version(18, 0), 1);
        assert_eq!(request_header_version(18, 1), 1);
        assert_eq!(request_header_version(18, 2), 1);
        assert_eq!(request_header_version(18, 3), 2);
    }

    #[test]
    fn test_response_header_version_api_versions() {
        // ApiVersions ALWAYS uses response header v0 (bootstrap)
        assert_eq!(response_header_version(18, 0), 0);
        assert_eq!(response_header_version(18, 1), 0);
        assert_eq!(response_header_version(18, 2), 0);
        assert_eq!(response_header_version(18, 3), 0);
    }

    #[test]
    fn test_request_header_version_metadata() {
        // Metadata v0-8 use header v1, v9+ use header v2
        assert_eq!(request_header_version(3, 0), 1);
        assert_eq!(request_header_version(3, 8), 1);
        assert_eq!(request_header_version(3, 9), 2);
        assert_eq!(request_header_version(3, 12), 2);
    }

    #[test]
    fn test_response_header_version_metadata() {
        // Metadata v0-8 use header v0, v9+ use header v1
        assert_eq!(response_header_version(3, 0), 0);
        assert_eq!(response_header_version(3, 8), 0);
        assert_eq!(response_header_version(3, 9), 1);
        assert_eq!(response_header_version(3, 12), 1);
    }

    #[test]
    fn test_sasl_handshake_never_flexible() {
        // SaslHandshake NEVER uses flexible versions
        assert_eq!(request_header_version(17, 0), 1);
        assert_eq!(request_header_version(17, 1), 1);
        assert_eq!(response_header_version(17, 0), 0);
        assert_eq!(response_header_version(17, 1), 0);
    }

    #[test]
    fn test_describe_cluster_always_flexible() {
        // DescribeCluster ALWAYS uses flexible versions from v0
        assert_eq!(request_header_version(60, 0), 2);
        assert_eq!(response_header_version(60, 0), 1);
    }

    #[test]
    fn test_build_api_versions_request() {
        let request = build_api_versions_request(3, 1234);

        // Should have size prefix (4 bytes) + header + body
        assert!(request.len() > 4);

        // First 4 bytes are size prefix
        let size = u32::from_be_bytes([request[0], request[1], request[2], request[3]]) as usize;
        assert_eq!(size, request.len() - 4);

        // Next 2 bytes are API key (18 for ApiVersions)
        let api_key = i16::from_be_bytes([request[4], request[5]]);
        assert_eq!(api_key, 18);

        // Next 2 bytes are API version
        let api_version = i16::from_be_bytes([request[6], request[7]]);
        assert_eq!(api_version, 3);

        // Next 4 bytes are correlation ID
        let correlation_id = i32::from_be_bytes([request[8], request[9], request[10], request[11]]);
        assert_eq!(correlation_id, 1234);
    }

    #[test]
    fn test_extract_correlation_id() {
        let response_body = [0x00, 0x00, 0x04, 0xD2, 0x00, 0x00]; // correlation_id = 1234
        assert_eq!(extract_correlation_id(&response_body), Some(1234));
    }

    #[test]
    fn test_all_api_keys_have_expected_versions() {
        for &api_key in SUPPORTED_API_KEYS {
            let (req_start, resp_start) = expected_header_versions(api_key);
            let max_version = max_supported_version(api_key);

            // Test all versions from 0 to max
            for version in 0..=max_version {
                let req_ver = request_header_version(api_key, version);
                let resp_ver = response_header_version(api_key, version);

                // Request header version should be 1 or 2
                assert!(
                    req_ver == 1 || req_ver == 2,
                    "API {} v{}: unexpected request header version {}",
                    api_key_name(api_key),
                    version,
                    req_ver
                );

                // Response header version should be 0 or 1
                assert!(
                    resp_ver == 0 || resp_ver == 1,
                    "API {} v{}: unexpected response header version {}",
                    api_key_name(api_key),
                    version,
                    resp_ver
                );

                // Verify flexible version thresholds
                if let Some(threshold) = req_start {
                    if version >= threshold {
                        assert_eq!(
                            req_ver,
                            2,
                            "API {} v{}: expected request header v2 (flexible)",
                            api_key_name(api_key),
                            version
                        );
                    } else {
                        assert_eq!(
                            req_ver,
                            1,
                            "API {} v{}: expected request header v1 (non-flexible)",
                            api_key_name(api_key),
                            version
                        );
                    }
                }

                if let Some(threshold) = resp_start {
                    if version >= threshold {
                        assert_eq!(
                            resp_ver,
                            1,
                            "API {} v{}: expected response header v1 (flexible)",
                            api_key_name(api_key),
                            version
                        );
                    } else {
                        assert_eq!(
                            resp_ver,
                            0,
                            "API {} v{}: expected response header v0 (non-flexible)",
                            api_key_name(api_key),
                            version
                        );
                    }
                }
            }
        }
    }
}
