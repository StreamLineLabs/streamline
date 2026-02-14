//! Phase 22: Protocol Header Format Tests
//!
//! Tests for Kafka protocol request/response header encoding/decoding:
//! - Request Header v1 and v2 formats
//! - Response Header v0 and v1 formats
//! - Header version determination based on API version
//! - Tagged fields in v2 headers
//!
//! Reference: https://kafka.apache.org/protocol.html#protocol_messages

use bytes::BytesMut;
use kafka_protocol::messages::{
    ApiVersionsRequest, ApiVersionsResponse, CreateTopicsRequest, FetchRequest, MetadataRequest,
    ProduceRequest, RequestHeader, ResponseHeader,
};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};

// ============================================================================
// Request Header v1 Tests
// ============================================================================

/// Test RequestHeader v1 basic encoding
#[test]
fn test_request_header_v1_basic() {
    let mut header = RequestHeader::default();
    header.request_api_key = 3; // Metadata
    header.request_api_version = 1;
    header.correlation_id = 12345;
    header.client_id = Some(StrBytes::from_static_str("test-client"));

    let mut buf = BytesMut::new();
    header.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = RequestHeader::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.request_api_key, 3);
    assert_eq!(decoded.request_api_version, 1);
    assert_eq!(decoded.correlation_id, 12345);
    assert_eq!(decoded.client_id.unwrap().as_str(), "test-client");
}

/// Test RequestHeader v1 with null client_id
#[test]
fn test_request_header_v1_null_client_id() {
    let mut header = RequestHeader::default();
    header.request_api_key = 0; // Produce
    header.request_api_version = 3;
    header.correlation_id = 1;
    header.client_id = None;

    let mut buf = BytesMut::new();
    header.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = RequestHeader::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.request_api_key, 0);
    assert!(decoded.client_id.is_none());
}

/// Test RequestHeader v1 with empty client_id
#[test]
fn test_request_header_v1_empty_client_id() {
    let mut header = RequestHeader::default();
    header.request_api_key = 1; // Fetch
    header.request_api_version = 4;
    header.correlation_id = 2;
    header.client_id = Some(StrBytes::from_static_str(""));

    let mut buf = BytesMut::new();
    header.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = RequestHeader::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.client_id.unwrap().as_str(), "");
}

/// Test RequestHeader v1 with long client_id
#[test]
fn test_request_header_v1_long_client_id() {
    let long_client_id = "a".repeat(200);
    let mut header = RequestHeader::default();
    header.request_api_key = 18; // ApiVersions
    header.request_api_version = 2;
    header.correlation_id = 100;
    header.client_id = Some(StrBytes::from_string(long_client_id.clone()));

    let mut buf = BytesMut::new();
    header.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = RequestHeader::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.client_id.unwrap().as_str(), long_client_id);
}

// ============================================================================
// Request Header v2 Tests (Flexible versions with tagged fields)
// ============================================================================

/// Test RequestHeader v2 basic encoding (flexible version)
#[test]
fn test_request_header_v2_basic() {
    let mut header = RequestHeader::default();
    header.request_api_key = 19; // CreateTopics
    header.request_api_version = 5; // Flexible version
    header.correlation_id = 54321;
    header.client_id = Some(StrBytes::from_static_str("flexible-client"));

    let mut buf = BytesMut::new();
    header.encode(&mut buf, 2).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = RequestHeader::decode(&mut read_buf, 2).unwrap();

    assert_eq!(decoded.request_api_key, 19);
    assert_eq!(decoded.request_api_version, 5);
    assert_eq!(decoded.correlation_id, 54321);
    assert_eq!(decoded.client_id.unwrap().as_str(), "flexible-client");
}

/// Test RequestHeader v2 with null client_id
#[test]
fn test_request_header_v2_null_client_id() {
    let mut header = RequestHeader::default();
    header.request_api_key = 18;
    header.request_api_version = 3; // Flexible version
    header.correlation_id = 999;
    header.client_id = None;

    let mut buf = BytesMut::new();
    header.encode(&mut buf, 2).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = RequestHeader::decode(&mut read_buf, 2).unwrap();

    assert!(decoded.client_id.is_none());
}

// ============================================================================
// Response Header v0 Tests
// ============================================================================

/// Test ResponseHeader v0 basic encoding
#[test]
fn test_response_header_v0_basic() {
    let mut header = ResponseHeader::default();
    header.correlation_id = 12345;

    let mut buf = BytesMut::new();
    header.encode(&mut buf, 0).unwrap();

    // v0 response header is just correlation_id (4 bytes)
    assert_eq!(buf.len(), 4);

    let mut read_buf = buf.freeze();
    let decoded = ResponseHeader::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.correlation_id, 12345);
}

/// Test ResponseHeader v0 with max correlation_id
#[test]
fn test_response_header_v0_max_correlation_id() {
    let mut header = ResponseHeader::default();
    header.correlation_id = i32::MAX;

    let mut buf = BytesMut::new();
    header.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ResponseHeader::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.correlation_id, i32::MAX);
}

/// Test ResponseHeader v0 with zero correlation_id
#[test]
fn test_response_header_v0_zero_correlation_id() {
    let mut header = ResponseHeader::default();
    header.correlation_id = 0;

    let mut buf = BytesMut::new();
    header.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ResponseHeader::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.correlation_id, 0);
}

// ============================================================================
// Response Header v1 Tests (with tagged fields)
// ============================================================================

/// Test ResponseHeader v1 basic encoding
#[test]
fn test_response_header_v1_basic() {
    let mut header = ResponseHeader::default();
    header.correlation_id = 67890;

    let mut buf = BytesMut::new();
    header.encode(&mut buf, 1).unwrap();

    // v1 has correlation_id (4 bytes) + tagged_fields (varint 0 for no tags)
    assert!(buf.len() >= 4);

    let mut read_buf = buf.freeze();
    let decoded = ResponseHeader::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.correlation_id, 67890);
}

// ============================================================================
// Header Version Determination Tests
// ============================================================================

/// Test that ProduceRequest uses correct header version
#[test]
fn test_produce_request_header_version() {
    // ProduceRequest v0-8 use header v1, v9+ use header v2
    assert_eq!(ProduceRequest::header_version(0), 1);
    assert_eq!(ProduceRequest::header_version(3), 1);
    assert_eq!(ProduceRequest::header_version(8), 1);
    assert_eq!(ProduceRequest::header_version(9), 2);
}

/// Test that FetchRequest uses correct header version
#[test]
fn test_fetch_request_header_version() {
    // FetchRequest v0-11 use header v1, v12+ use header v2
    assert_eq!(FetchRequest::header_version(0), 1);
    assert_eq!(FetchRequest::header_version(11), 1);
    assert_eq!(FetchRequest::header_version(12), 2);
}

/// Test that MetadataRequest uses correct header version
#[test]
fn test_metadata_request_header_version() {
    // MetadataRequest v0-8 use header v1, v9+ use header v2
    assert_eq!(MetadataRequest::header_version(0), 1);
    assert_eq!(MetadataRequest::header_version(8), 1);
    assert_eq!(MetadataRequest::header_version(9), 2);
}

/// Test that ApiVersionsRequest uses correct header version
#[test]
fn test_api_versions_request_header_version() {
    // ApiVersionsRequest v0-2 use header v1, v3+ use header v2
    assert_eq!(ApiVersionsRequest::header_version(0), 1);
    assert_eq!(ApiVersionsRequest::header_version(2), 1);
    assert_eq!(ApiVersionsRequest::header_version(3), 2);
}

/// Test that CreateTopicsRequest uses correct header version
#[test]
fn test_create_topics_request_header_version() {
    // CreateTopicsRequest v0-4 use header v1, v5+ use header v2
    assert_eq!(CreateTopicsRequest::header_version(0), 1);
    assert_eq!(CreateTopicsRequest::header_version(4), 1);
    assert_eq!(CreateTopicsRequest::header_version(5), 2);
}

/// Test that ApiVersionsResponse uses correct header version
#[test]
fn test_api_versions_response_header_version() {
    // ApiVersionsResponse uses header v0 for all versions
    // This is a special case in Kafka protocol
    assert_eq!(ApiVersionsResponse::header_version(0), 0);
    assert_eq!(ApiVersionsResponse::header_version(2), 0);
    assert_eq!(ApiVersionsResponse::header_version(3), 0);
}

// ============================================================================
// Header with Message Integration Tests
// ============================================================================

/// Test full request encoding with v1 header
#[test]
fn test_full_request_with_v1_header() {
    // Create a header
    let mut header = RequestHeader::default();
    header.request_api_key = 3; // Metadata
    header.request_api_version = 1;
    header.correlation_id = 100;
    header.client_id = Some(StrBytes::from_static_str("test-client"));

    // Create request body
    let request = MetadataRequest::default();

    // Encode header and body separately
    let mut header_buf = BytesMut::new();
    header.encode(&mut header_buf, 1).unwrap();

    let mut body_buf = BytesMut::new();
    request.encode(&mut body_buf, 1).unwrap();

    // Verify header can be decoded
    let mut read_buf = header_buf.freeze();
    let decoded_header = RequestHeader::decode(&mut read_buf, 1).unwrap();
    assert_eq!(decoded_header.correlation_id, 100);
}

/// Test full request encoding with v2 header (flexible)
#[test]
fn test_full_request_with_v2_header() {
    // Create a header for flexible version
    let mut header = RequestHeader::default();
    header.request_api_key = 18; // ApiVersions
    header.request_api_version = 3; // Flexible version
    header.correlation_id = 200;
    header.client_id = Some(StrBytes::from_static_str("flexible-test-client"));

    // Create request body
    let request = ApiVersionsRequest::default();

    // Encode header and body separately
    let mut header_buf = BytesMut::new();
    header.encode(&mut header_buf, 2).unwrap();

    let mut body_buf = BytesMut::new();
    request.encode(&mut body_buf, 3).unwrap();

    // Verify header can be decoded
    let mut read_buf = header_buf.freeze();
    let decoded_header = RequestHeader::decode(&mut read_buf, 2).unwrap();
    assert_eq!(decoded_header.correlation_id, 200);
}

// ============================================================================
// Correlation ID Matching Tests
// ============================================================================

/// Test that response correlation_id matches request
#[test]
fn test_correlation_id_matching() {
    let correlation_id = 42;

    // Create request header
    let mut req_header = RequestHeader::default();
    req_header.request_api_key = 3;
    req_header.request_api_version = 1;
    req_header.correlation_id = correlation_id;

    // Create response header with same correlation_id
    let mut resp_header = ResponseHeader::default();
    resp_header.correlation_id = correlation_id;

    // Encode both
    let mut req_buf = BytesMut::new();
    req_header.encode(&mut req_buf, 1).unwrap();

    let mut resp_buf = BytesMut::new();
    resp_header.encode(&mut resp_buf, 0).unwrap();

    // Decode and verify matching
    let mut req_read = req_buf.freeze();
    let decoded_req = RequestHeader::decode(&mut req_read, 1).unwrap();

    let mut resp_read = resp_buf.freeze();
    let decoded_resp = ResponseHeader::decode(&mut resp_read, 0).unwrap();

    assert_eq!(decoded_req.correlation_id, decoded_resp.correlation_id);
}

/// Test sequential correlation IDs
#[test]
fn test_sequential_correlation_ids() {
    for i in 0..100 {
        let mut header = ResponseHeader::default();
        header.correlation_id = i;

        let mut buf = BytesMut::new();
        header.encode(&mut buf, 0).unwrap();

        let mut read_buf = buf.freeze();
        let decoded = ResponseHeader::decode(&mut read_buf, 0).unwrap();

        assert_eq!(decoded.correlation_id, i);
    }
}

// ============================================================================
// Header Size Tests
// ============================================================================

/// Test RequestHeader v1 size calculation
#[test]
fn test_request_header_v1_size() {
    let mut header = RequestHeader::default();
    header.request_api_key = 0;
    header.request_api_version = 0;
    header.correlation_id = 0;
    header.client_id = Some(StrBytes::from_static_str("client"));

    let mut buf = BytesMut::new();
    header.encode(&mut buf, 1).unwrap();

    // v1: api_key (2) + api_version (2) + correlation_id (4) + client_id_length (2) + "client" (6) = 16
    assert_eq!(buf.len(), 16);
}

/// Test ResponseHeader v0 size
#[test]
fn test_response_header_v0_size() {
    let mut header = ResponseHeader::default();
    header.correlation_id = 0;

    let mut buf = BytesMut::new();
    header.encode(&mut buf, 0).unwrap();

    // v0: just correlation_id (4)
    assert_eq!(buf.len(), 4);
}

/// Test ResponseHeader v1 size (with empty tagged fields)
#[test]
fn test_response_header_v1_size() {
    let mut header = ResponseHeader::default();
    header.correlation_id = 0;

    let mut buf = BytesMut::new();
    header.encode(&mut buf, 1).unwrap();

    // v1: correlation_id (4) + tagged_fields_count (1, varint for 0) = 5
    assert_eq!(buf.len(), 5);
}
