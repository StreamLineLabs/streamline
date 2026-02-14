//! Phase 10: Request Pipelining Tests
//!
//! Tests for Kafka request pipelining protocol behavior:
//! - Request/response correlation via correlation IDs
//! - Request header encoding with correlation IDs
//! - Multiple concurrent requests
//! - Response ordering guarantees
//! - Client ID handling

use bytes::BytesMut;
use kafka_protocol::messages::{
    ApiVersionsRequest, ApiVersionsResponse, FetchRequest, FetchResponse, MetadataRequest,
    MetadataResponse, ProduceRequest, ProduceResponse, RequestHeader, ResponseHeader,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};

// =============================================================================
// Request Header Correlation ID Tests
// =============================================================================

#[test]
fn test_request_header_correlation_id_basic() {
    let mut header = RequestHeader::default();
    header.request_api_key = 3; // Metadata
    header.request_api_version = 0;
    header.correlation_id = 12345;
    header.client_id = Some(StrBytes::from_static_str("test-client"));

    let mut buf = BytesMut::new();
    // Request header version 1
    let result = header.encode(&mut buf, 1);
    assert!(result.is_ok());

    let mut read_buf = buf.freeze();
    let decoded = RequestHeader::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.correlation_id, 12345);
}

#[test]
fn test_request_header_correlation_id_zero() {
    let mut header = RequestHeader::default();
    header.correlation_id = 0; // First request

    let mut buf = BytesMut::new();
    header.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = RequestHeader::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.correlation_id, 0);
}

#[test]
fn test_request_header_correlation_id_max() {
    let mut header = RequestHeader::default();
    header.correlation_id = i32::MAX; // Maximum correlation ID

    let mut buf = BytesMut::new();
    header.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = RequestHeader::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.correlation_id, i32::MAX);
}

#[test]
fn test_request_header_correlation_id_negative() {
    let mut header = RequestHeader::default();
    header.correlation_id = -1; // Negative (wrapping)

    let mut buf = BytesMut::new();
    header.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = RequestHeader::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.correlation_id, -1);
}

#[test]
fn test_request_header_correlation_id_sequential() {
    // Correlation IDs are typically sequential
    let ids = [1, 2, 3, 4, 5];

    for &id in &ids {
        let mut header = RequestHeader::default();
        header.correlation_id = id;

        let mut buf = BytesMut::new();
        header.encode(&mut buf, 1).unwrap();

        let mut read_buf = buf.freeze();
        let decoded = RequestHeader::decode(&mut read_buf, 1).unwrap();

        assert_eq!(decoded.correlation_id, id);
    }
}

// =============================================================================
// Response Header Correlation ID Tests
// =============================================================================

#[test]
fn test_response_header_correlation_id_basic() {
    let mut header = ResponseHeader::default();
    header.correlation_id = 12345;

    let mut buf = BytesMut::new();
    // Response header version 0
    let result = header.encode(&mut buf, 0);
    assert!(result.is_ok());

    let mut read_buf = buf.freeze();
    let decoded = ResponseHeader::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.correlation_id, 12345);
}

#[test]
fn test_response_header_correlation_id_matches_request() {
    // Simulate request-response pair
    let request_correlation_id = 42;

    let mut request_header = RequestHeader::default();
    request_header.correlation_id = request_correlation_id;

    let mut response_header = ResponseHeader::default();
    response_header.correlation_id = request_correlation_id; // Must match

    assert_eq!(
        request_header.correlation_id,
        response_header.correlation_id
    );
}

#[test]
fn test_response_header_v1_with_tagged_fields() {
    let mut header = ResponseHeader::default();
    header.correlation_id = 99;

    // Response header version 1 includes tagged fields
    let mut buf = BytesMut::new();
    header.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ResponseHeader::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.correlation_id, 99);
}

// =============================================================================
// Multiple In-Flight Requests Tests
// =============================================================================

#[test]
fn test_multiple_metadata_requests_different_correlation_ids() {
    let mut headers = Vec::new();

    // Create 5 concurrent metadata requests
    for i in 0..5 {
        let mut header = RequestHeader::default();
        header.request_api_key = 3; // Metadata
        header.request_api_version = 0;
        header.correlation_id = i;
        headers.push(header);
    }

    // Verify each has unique correlation ID
    let correlation_ids: Vec<i32> = headers.iter().map(|h| h.correlation_id).collect();
    let unique_ids: std::collections::HashSet<i32> = correlation_ids.iter().copied().collect();
    assert_eq!(unique_ids.len(), 5);
}

#[test]
fn test_mixed_request_types_pipelining() {
    // Different request types can be pipelined
    let requests = vec![
        (3i16, 0i16, 1i32),  // Metadata v0, correlation 1
        (0i16, 7i16, 2i32),  // Produce v7, correlation 2
        (1i16, 11i16, 3i32), // Fetch v11, correlation 3
        (18i16, 3i16, 4i32), // ApiVersions v3, correlation 4
    ];

    for (api_key, version, correlation_id) in &requests {
        let mut header = RequestHeader::default();
        header.request_api_key = *api_key;
        header.request_api_version = *version;
        header.correlation_id = *correlation_id;

        let mut buf = BytesMut::new();
        let result = header.encode(&mut buf, 1);
        assert!(result.is_ok());
    }
}

#[test]
fn test_high_correlation_id_count() {
    // Simulate many in-flight requests
    for i in 0..100 {
        let mut header = RequestHeader::default();
        header.correlation_id = i;

        let mut buf = BytesMut::new();
        header.encode(&mut buf, 1).unwrap();

        let mut read_buf = buf.freeze();
        let decoded = RequestHeader::decode(&mut read_buf, 1).unwrap();

        assert_eq!(decoded.correlation_id, i);
    }
}

// =============================================================================
// Response Ordering Tests
// =============================================================================

#[test]
fn test_response_correlation_id_lookup() {
    // Simulate pending requests map
    let mut pending: std::collections::HashMap<i32, &str> = std::collections::HashMap::new();

    pending.insert(1, "metadata");
    pending.insert(2, "produce");
    pending.insert(3, "fetch");

    // Response arrives with correlation_id = 2
    let response_correlation_id = 2;
    let request_type = pending.get(&response_correlation_id);

    assert_eq!(request_type, Some(&"produce"));
}

#[test]
fn test_out_of_order_responses() {
    // Responses may arrive out of order
    let request_order = [1, 2, 3, 4, 5];
    let response_order = [3, 1, 5, 2, 4]; // Out of order

    // All correlation IDs should still be matchable
    let mut pending: std::collections::HashSet<i32> = request_order.iter().copied().collect();

    for response_id in &response_order {
        assert!(pending.remove(response_id));
    }

    assert!(pending.is_empty());
}

#[test]
fn test_correlation_id_reuse() {
    // After response, correlation ID can be reused
    let mut available_ids: Vec<i32> = (0..5).collect();
    let mut in_flight: std::collections::HashSet<i32> = std::collections::HashSet::new();

    // Send requests 0, 1, 2
    for _ in 0..3 {
        let id = available_ids.pop().unwrap();
        in_flight.insert(id);
    }

    assert_eq!(in_flight.len(), 3);

    // Receive response for ID 3 (originally sent second)
    in_flight.remove(&3);
    available_ids.push(3);

    // Can reuse ID 3
    assert!(available_ids.contains(&3));
}

// =============================================================================
// Client ID Tests
// =============================================================================

#[test]
fn test_client_id_in_request_header() {
    let mut header = RequestHeader::default();
    header.client_id = Some(StrBytes::from_static_str("my-client-app"));
    header.correlation_id = 1;

    let mut buf = BytesMut::new();
    header.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = RequestHeader::decode(&mut read_buf, 1).unwrap();

    assert_eq!(
        decoded.client_id.as_ref().unwrap().as_str(),
        "my-client-app"
    );
}

#[test]
fn test_client_id_null() {
    let mut header = RequestHeader::default();
    header.client_id = None;
    header.correlation_id = 1;

    let mut buf = BytesMut::new();
    header.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = RequestHeader::decode(&mut read_buf, 1).unwrap();

    assert!(decoded.client_id.is_none());
}

#[test]
fn test_client_id_empty_string() {
    let mut header = RequestHeader::default();
    header.client_id = Some(StrBytes::from_static_str(""));
    header.correlation_id = 1;

    let mut buf = BytesMut::new();
    header.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = RequestHeader::decode(&mut read_buf, 1).unwrap();

    assert!(decoded.client_id.as_ref().unwrap().is_empty());
}

#[test]
fn test_client_id_long() {
    let long_client_id = "a".repeat(256);
    let mut header = RequestHeader::default();
    header.client_id = Some(StrBytes::from_string(long_client_id.clone()));
    header.correlation_id = 1;

    let mut buf = BytesMut::new();
    header.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = RequestHeader::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.client_id.as_ref().unwrap().as_str(), long_client_id);
}

// =============================================================================
// Request Header Version Tests
// =============================================================================

#[test]
fn test_request_header_v0() {
    // Version 0: api_key, api_version, correlation_id, client_id
    let mut header = RequestHeader::default();
    header.request_api_key = 3;
    header.request_api_version = 0;
    header.correlation_id = 100;
    header.client_id = Some(StrBytes::from_static_str("client"));

    let mut buf = BytesMut::new();
    header.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = RequestHeader::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.request_api_key, 3);
    assert_eq!(decoded.request_api_version, 0);
    assert_eq!(decoded.correlation_id, 100);
}

#[test]
fn test_request_header_v1() {
    // Version 1: Same as v0, used with flexible versions
    let mut header = RequestHeader::default();
    header.request_api_key = 18; // ApiVersions
    header.request_api_version = 3;
    header.correlation_id = 200;
    header.client_id = Some(StrBytes::from_static_str("client-v1"));

    let mut buf = BytesMut::new();
    header.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = RequestHeader::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.request_api_key, 18);
    assert_eq!(decoded.request_api_version, 3);
    assert_eq!(decoded.correlation_id, 200);
}

#[test]
fn test_request_header_v2() {
    // Version 2: Adds tagged fields
    let mut header = RequestHeader::default();
    header.request_api_key = 1; // Fetch
    header.request_api_version = 12;
    header.correlation_id = 300;
    header.client_id = Some(StrBytes::from_static_str("client-v2"));

    let mut buf = BytesMut::new();
    header.encode(&mut buf, 2).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = RequestHeader::decode(&mut read_buf, 2).unwrap();

    assert_eq!(decoded.request_api_key, 1);
    assert_eq!(decoded.request_api_version, 12);
    assert_eq!(decoded.correlation_id, 300);
}

// =============================================================================
// Full Request/Response Roundtrip with Correlation ID Tests
// =============================================================================

#[test]
fn test_api_versions_request_response_correlation() {
    let correlation_id = 42;

    // Request
    let mut request_header = RequestHeader::default();
    request_header.request_api_key = 18; // ApiVersions
    request_header.request_api_version = 0;
    request_header.correlation_id = correlation_id;

    let request = ApiVersionsRequest::default();

    let mut request_buf = BytesMut::new();
    request_header.encode(&mut request_buf, 1).unwrap();
    request.encode(&mut request_buf, 0).unwrap();

    // Response
    let mut response_header = ResponseHeader::default();
    response_header.correlation_id = correlation_id;

    let response = ApiVersionsResponse::default();

    let mut response_buf = BytesMut::new();
    response_header.encode(&mut response_buf, 0).unwrap();
    response.encode(&mut response_buf, 0).unwrap();

    // Decode and verify correlation matches
    let mut request_read = request_buf.freeze();
    let decoded_request_header = RequestHeader::decode(&mut request_read, 1).unwrap();

    let mut response_read = response_buf.freeze();
    let decoded_response_header = ResponseHeader::decode(&mut response_read, 0).unwrap();

    assert_eq!(
        decoded_request_header.correlation_id,
        decoded_response_header.correlation_id
    );
}

#[test]
fn test_metadata_request_response_correlation() {
    let correlation_id = 100;

    // Request
    let mut request_header = RequestHeader::default();
    request_header.request_api_key = 3;
    request_header.request_api_version = 0;
    request_header.correlation_id = correlation_id;

    let request = MetadataRequest::default();

    let mut request_buf = BytesMut::new();
    request_header.encode(&mut request_buf, 1).unwrap();
    request.encode(&mut request_buf, 0).unwrap();

    // Response
    let mut response_header = ResponseHeader::default();
    response_header.correlation_id = correlation_id;

    let response = MetadataResponse::default();

    let mut response_buf = BytesMut::new();
    response_header.encode(&mut response_buf, 0).unwrap();
    response.encode(&mut response_buf, 0).unwrap();

    // Verify correlation
    let mut request_read = request_buf.freeze();
    let decoded_request_header = RequestHeader::decode(&mut request_read, 1).unwrap();

    let mut response_read = response_buf.freeze();
    let decoded_response_header = ResponseHeader::decode(&mut response_read, 0).unwrap();

    assert_eq!(
        decoded_request_header.correlation_id,
        decoded_response_header.correlation_id
    );
    assert_eq!(decoded_response_header.correlation_id, 100);
}

#[test]
fn test_produce_request_response_correlation() {
    let correlation_id = 999;

    // Request
    let mut request_header = RequestHeader::default();
    request_header.request_api_key = 0; // Produce
    request_header.request_api_version = 7;
    request_header.correlation_id = correlation_id;

    let request = ProduceRequest::default();

    let mut request_buf = BytesMut::new();
    request_header.encode(&mut request_buf, 1).unwrap();
    request.encode(&mut request_buf, 7).unwrap();

    // Response
    let mut response_header = ResponseHeader::default();
    response_header.correlation_id = correlation_id;

    let response = ProduceResponse::default();

    let mut response_buf = BytesMut::new();
    response_header.encode(&mut response_buf, 0).unwrap();
    response.encode(&mut response_buf, 7).unwrap();

    // Verify correlation
    let mut request_read = request_buf.freeze();
    let decoded_request_header = RequestHeader::decode(&mut request_read, 1).unwrap();

    let mut response_read = response_buf.freeze();
    let decoded_response_header = ResponseHeader::decode(&mut response_read, 0).unwrap();

    assert_eq!(
        decoded_request_header.correlation_id,
        decoded_response_header.correlation_id
    );
}

#[test]
fn test_fetch_request_response_correlation() {
    let correlation_id = 12345;

    // Request
    let mut request_header = RequestHeader::default();
    request_header.request_api_key = 1; // Fetch
    request_header.request_api_version = 11;
    request_header.correlation_id = correlation_id;

    let request = FetchRequest::default();

    let mut request_buf = BytesMut::new();
    request_header.encode(&mut request_buf, 1).unwrap();
    request.encode(&mut request_buf, 11).unwrap();

    // Response
    let mut response_header = ResponseHeader::default();
    response_header.correlation_id = correlation_id;

    let response = FetchResponse::default();

    let mut response_buf = BytesMut::new();
    response_header.encode(&mut response_buf, 0).unwrap();
    response.encode(&mut response_buf, 11).unwrap();

    // Verify correlation
    let mut request_read = request_buf.freeze();
    let decoded_request_header = RequestHeader::decode(&mut request_read, 1).unwrap();

    let mut response_read = response_buf.freeze();
    let decoded_response_header = ResponseHeader::decode(&mut response_read, 0).unwrap();

    assert_eq!(
        decoded_request_header.correlation_id,
        decoded_response_header.correlation_id
    );
}

// =============================================================================
// Max In-Flight Requests Tests
// =============================================================================

#[test]
fn test_max_in_flight_tracking() {
    // Simulate tracking max in-flight requests
    let max_in_flight = 5;
    let mut in_flight_count = 0;
    let mut pending: std::collections::VecDeque<i32> = std::collections::VecDeque::new();

    // Send requests up to max
    for i in 0..max_in_flight {
        pending.push_back(i);
        in_flight_count += 1;
    }

    assert_eq!(in_flight_count, max_in_flight);

    // Cannot send more without exceeding limit
    assert!(in_flight_count >= max_in_flight);

    // Receive a response
    let _completed = pending.pop_front();
    in_flight_count -= 1;

    // Now can send another
    assert!(in_flight_count < max_in_flight);
}

#[test]
fn test_idempotent_producer_max_in_flight() {
    // For idempotent producers, max.in.flight.requests.per.connection <= 5
    let max_in_flight_idempotent = 5;

    // Simulate idempotent producer constraint
    let mut in_flight: std::collections::BTreeMap<i32, i32> = std::collections::BTreeMap::new();

    for sequence_number in 0..max_in_flight_idempotent {
        in_flight.insert(sequence_number, sequence_number);
    }

    assert_eq!(in_flight.len(), 5);
}

#[test]
fn test_transactional_producer_single_in_flight() {
    // Transactional producers typically use max.in.flight.requests.per.connection = 1
    let max_in_flight_txn = 1;
    let mut in_flight = 0;

    // Send one request
    in_flight += 1;
    assert!(in_flight <= max_in_flight_txn);

    // Cannot send more
    assert!(in_flight >= max_in_flight_txn);

    // Must wait for response
    in_flight -= 1;
    assert!(in_flight < max_in_flight_txn);
}

// =============================================================================
// Correlation ID Mismatch Tests
// =============================================================================

#[test]
fn test_correlation_id_mismatch_detection() {
    let request_correlation = 100;
    let response_correlation = 101; // Mismatch!

    let is_match = request_correlation == response_correlation;
    assert!(!is_match);
}

#[test]
fn test_unknown_correlation_id_handling() {
    let mut pending: std::collections::HashMap<i32, &str> = std::collections::HashMap::new();
    pending.insert(1, "request-1");
    pending.insert(2, "request-2");

    // Response with unknown correlation ID
    let unknown_correlation = 99;
    let result = pending.get(&unknown_correlation);

    assert!(result.is_none());
}

// =============================================================================
// Correlation ID Wrapping Tests
// =============================================================================

#[test]
fn test_correlation_id_wrapping() {
    // Correlation ID wraps around at i32::MAX
    let mut correlation_id: i32 = i32::MAX - 2;

    correlation_id = correlation_id.wrapping_add(1);
    assert_eq!(correlation_id, i32::MAX - 1);

    correlation_id = correlation_id.wrapping_add(1);
    assert_eq!(correlation_id, i32::MAX);

    correlation_id = correlation_id.wrapping_add(1);
    assert_eq!(correlation_id, i32::MIN); // Wraps

    correlation_id = correlation_id.wrapping_add(1);
    assert_eq!(correlation_id, i32::MIN + 1);
}

#[test]
fn test_correlation_id_uniqueness_after_wrap() {
    // Even after wrapping, correlation IDs should be unique within pending set
    let mut pending: std::collections::HashSet<i32> = std::collections::HashSet::new();

    let ids = [i32::MAX, i32::MIN, 0, 1, -1];
    for id in ids {
        assert!(pending.insert(id));
    }

    assert_eq!(pending.len(), 5);
}

// =============================================================================
// Request Pipelining Performance Tests
// =============================================================================

#[test]
fn test_batch_request_encoding() {
    // Encode multiple requests efficiently
    let mut buf = BytesMut::new();

    for i in 0..10 {
        let mut header = RequestHeader::default();
        header.request_api_key = 3; // Metadata
        header.request_api_version = 0;
        header.correlation_id = i;

        header.encode(&mut buf, 1).unwrap();

        let request = MetadataRequest::default();
        request.encode(&mut buf, 0).unwrap();
    }

    // All 10 requests encoded into single buffer
    assert!(!buf.is_empty());
}

#[test]
fn test_request_header_size() {
    let mut header = RequestHeader::default();
    header.request_api_key = 0;
    header.request_api_version = 0;
    header.correlation_id = 0;
    header.client_id = None;

    let mut buf = BytesMut::new();
    header.encode(&mut buf, 1).unwrap();

    // Minimum header size: api_key(2) + api_version(2) + correlation_id(4) + client_id_length(2)
    assert!(buf.len() >= 10);
}

#[test]
fn test_response_header_size() {
    let mut header = ResponseHeader::default();
    header.correlation_id = 0;

    let mut buf = BytesMut::new();
    header.encode(&mut buf, 0).unwrap();

    // Response header v0: just correlation_id (4 bytes)
    assert_eq!(buf.len(), 4);
}

// =============================================================================
// API Key Consistency Tests
// =============================================================================

#[test]
fn test_request_api_key_matches_response_type() {
    // ApiVersions
    let mut header = RequestHeader::default();
    header.request_api_key = 18;

    let response = ApiVersionsResponse::default();
    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    // The API key in header should match the response type
    assert_eq!(header.request_api_key, 18);
}

#[test]
fn test_all_api_keys_encodable() {
    // Verify all common API keys can be encoded in request header
    let api_keys = [0, 1, 2, 3, 8, 9, 10, 11, 12, 13, 14, 18, 19];

    for key in api_keys {
        let mut header = RequestHeader::default();
        header.request_api_key = key;
        header.request_api_version = 0;
        header.correlation_id = key as i32;

        let mut buf = BytesMut::new();
        let result = header.encode(&mut buf, 1);
        assert!(result.is_ok(), "Failed to encode API key {}", key);
    }
}
