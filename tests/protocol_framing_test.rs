//! Protocol Framing Tests - Phase 2
//!
//! Tests the Kafka wire protocol framing format per specification:
//! ```
//! RequestOrResponse => Size (INT32 big-endian) + Message
//! ```
//!
//! Source: https://kafka.apache.org/protocol.html#protocol_common

use bytes::{BufMut, BytesMut};
use kafka_protocol::messages::{ApiVersionsRequest, RequestHeader};
use kafka_protocol::protocol::Encodable;

/// Build a properly framed Kafka request with size prefix
fn build_framed_request(api_key: i16, api_version: i16, correlation_id: i32) -> Vec<u8> {
    // Build header
    let mut header = RequestHeader::default()
        .with_request_api_key(api_key)
        .with_request_api_version(api_version)
        .with_correlation_id(correlation_id);

    // Determine header version (simplified for ApiVersions)
    let header_version = if api_key == 18 && api_version >= 3 {
        2
    } else {
        1
    };

    header = header.with_client_id(Some("test-client".to_string().into()));

    // Encode header
    let mut header_buf = BytesMut::new();
    header.encode(&mut header_buf, header_version).unwrap();

    // Encode body (ApiVersions request has minimal body)
    let request = ApiVersionsRequest::default();
    let mut body_buf = BytesMut::new();
    request.encode(&mut body_buf, api_version).unwrap();

    // Combine with size prefix
    let total_size = header_buf.len() + body_buf.len();
    let mut result = Vec::with_capacity(4 + total_size);
    result.extend_from_slice(&(total_size as u32).to_be_bytes());
    result.extend_from_slice(&header_buf);
    result.extend_from_slice(&body_buf);

    result
}

/// Extract size prefix from raw bytes
fn extract_size_prefix(data: &[u8]) -> Option<u32> {
    if data.len() < 4 {
        return None;
    }
    Some(u32::from_be_bytes([data[0], data[1], data[2], data[3]]))
}

/// Verify the relationship between size prefix and message body
fn verify_framing(data: &[u8]) -> Result<(), String> {
    if data.len() < 4 {
        return Err("Data too short for size prefix".to_string());
    }

    let size = extract_size_prefix(data).unwrap() as usize;
    let actual_body_len = data.len() - 4;

    if size != actual_body_len {
        return Err(format!(
            "Size mismatch: prefix says {} but actual body is {} bytes",
            size, actual_body_len
        ));
    }

    Ok(())
}

// ============================================================================
// Phase 2: Message Framing Tests
// Verifies Kafka wire protocol framing per specification
// ============================================================================

#[test]
fn test_size_prefix_accuracy() {
    // Build a properly framed request
    let request = build_framed_request(18, 3, 1234);

    // Verify size prefix matches body length
    assert!(verify_framing(&request).is_ok());

    // Manually verify the size
    let size_prefix = extract_size_prefix(&request).unwrap() as usize;
    let actual_body = &request[4..];
    assert_eq!(
        size_prefix,
        actual_body.len(),
        "Size prefix {} should match body length {}",
        size_prefix,
        actual_body.len()
    );
}

#[test]
fn test_size_prefix_is_int32_big_endian() {
    // Build a request and verify the size is encoded as INT32 big-endian
    let request = build_framed_request(18, 3, 1);

    // The size should be in the first 4 bytes, big-endian
    let size_bytes = &request[0..4];

    // Parse as big-endian
    let size_be = u32::from_be_bytes([size_bytes[0], size_bytes[1], size_bytes[2], size_bytes[3]]);

    // Parse as little-endian (should be different if size > 255)
    let _size_le = u32::from_le_bytes([size_bytes[0], size_bytes[1], size_bytes[2], size_bytes[3]]);

    // For a typical small message, size_be should be reasonable (< 1000)
    // and match the actual body length
    let actual_body_len = request.len() - 4;
    assert_eq!(size_be as usize, actual_body_len);

    // Verify this is indeed big-endian by checking the format
    // For a size of 100, big-endian would be [0, 0, 0, 100]
    // little-endian would be [100, 0, 0, 0]
    if actual_body_len > 0 && actual_body_len < 256 {
        assert_eq!(
            size_bytes[0], 0,
            "For small sizes, first byte should be 0 in big-endian"
        );
        assert_eq!(
            size_bytes[1], 0,
            "For small sizes, second byte should be 0 in big-endian"
        );
        assert_eq!(
            size_bytes[2], 0,
            "For small sizes, third byte should be 0 in big-endian"
        );
        assert_eq!(
            size_bytes[3], actual_body_len as u8,
            "For small sizes, fourth byte should be the size in big-endian"
        );
    }
}

#[test]
fn test_size_does_not_include_itself() {
    // Per Kafka spec: "The size of the upcoming request or response message in bytes.
    // This size does not include this 4-byte size field."

    let request = build_framed_request(18, 3, 1);
    let size_prefix = extract_size_prefix(&request).unwrap() as usize;

    // Size should be total length minus 4 (the size field itself)
    assert_eq!(
        size_prefix,
        request.len() - 4,
        "Size prefix should NOT include the 4-byte size field"
    );

    // NOT total length
    assert_ne!(
        size_prefix,
        request.len(),
        "Size prefix should not equal total length"
    );
}

#[test]
fn test_partial_size_read_detection() {
    // Simulate receiving incomplete size prefix (less than 4 bytes)
    let partial_data = vec![0x00, 0x00, 0x00]; // Only 3 bytes

    assert!(
        extract_size_prefix(&partial_data).is_none(),
        "Should detect incomplete size prefix"
    );

    // Empty data
    let empty_data: Vec<u8> = vec![];
    assert!(
        extract_size_prefix(&empty_data).is_none(),
        "Should detect empty data"
    );

    // 1 byte
    let one_byte = vec![0x00];
    assert!(
        extract_size_prefix(&one_byte).is_none(),
        "Should detect 1-byte partial"
    );

    // 2 bytes
    let two_bytes = vec![0x00, 0x00];
    assert!(
        extract_size_prefix(&two_bytes).is_none(),
        "Should detect 2-byte partial"
    );
}

#[test]
fn test_partial_body_read_detection() {
    // Build a complete request
    let request = build_framed_request(18, 3, 1);
    let size_prefix = extract_size_prefix(&request).unwrap() as usize;

    // Simulate receiving size + partial body
    let partial_body_len = size_prefix / 2; // Only half the body
    let partial_data: Vec<u8> = request[..4 + partial_body_len].to_vec();

    // Verify framing detects the incomplete body
    let result = verify_framing(&partial_data);
    assert!(
        result.is_err(),
        "Should detect incomplete body: {:?}",
        result
    );
}

#[test]
fn test_zero_size_message() {
    // A zero-size message is technically valid framing (though semantically invalid)
    let mut zero_size_msg = Vec::new();
    zero_size_msg.extend_from_slice(&0u32.to_be_bytes()); // Size = 0

    // Framing is valid (size matches body length of 0)
    assert!(
        verify_framing(&zero_size_msg).is_ok(),
        "Zero-size message has valid framing"
    );
}

#[test]
fn test_negative_size_interpretation() {
    // Size is INT32, so negative values are possible but invalid
    // -1 as INT32 = 0xFFFFFFFF = 4294967295 as unsigned
    let mut negative_size_msg = Vec::new();
    negative_size_msg.extend_from_slice(&(-1i32).to_be_bytes());

    let size = extract_size_prefix(&negative_size_msg).unwrap();

    // When interpreted as unsigned, -1 becomes max u32
    assert_eq!(size, u32::MAX, "Negative size should wrap to max u32");

    // This should be rejected as oversized
    // (Server should validate size < max_message_size before allocating)
}

#[test]
fn test_multiple_sequential_requests_framing() {
    // Build multiple requests back-to-back (pipelining scenario)
    let request1 = build_framed_request(18, 3, 1);
    let request2 = build_framed_request(18, 3, 2);
    let request3 = build_framed_request(3, 12, 3); // Metadata request

    // Concatenate them
    let mut pipelined = Vec::new();
    pipelined.extend_from_slice(&request1);
    pipelined.extend_from_slice(&request2);
    pipelined.extend_from_slice(&request3);

    // Parse each request from the pipelined stream
    let mut offset = 0;
    let mut parsed_count = 0;

    while offset < pipelined.len() {
        // Need at least 4 bytes for size
        assert!(
            pipelined.len() - offset >= 4,
            "Not enough bytes for size prefix at offset {}",
            offset
        );

        let size = u32::from_be_bytes([
            pipelined[offset],
            pipelined[offset + 1],
            pipelined[offset + 2],
            pipelined[offset + 3],
        ]) as usize;

        // Verify we have the full message
        assert!(
            pipelined.len() - offset >= 4 + size,
            "Incomplete message at offset {}: need {} more bytes",
            offset,
            (4 + size) - (pipelined.len() - offset)
        );

        // Extract this message
        let message = &pipelined[offset..offset + 4 + size];
        assert!(
            verify_framing(message).is_ok(),
            "Message {} at offset {} has invalid framing",
            parsed_count,
            offset
        );

        offset += 4 + size;
        parsed_count += 1;
    }

    assert_eq!(parsed_count, 3, "Should have parsed all 3 requests");
}

#[test]
fn test_correlation_id_in_framed_request() {
    // Verify correlation ID is preserved in the framed request
    let correlation_id = 12345i32;
    let request = build_framed_request(18, 3, correlation_id);

    // Skip size prefix (4 bytes), API key (2 bytes), API version (2 bytes)
    // Correlation ID should be at offset 8 (4 + 2 + 2)
    let corr_id_bytes = &request[8..12];
    let extracted_corr_id = i32::from_be_bytes([
        corr_id_bytes[0],
        corr_id_bytes[1],
        corr_id_bytes[2],
        corr_id_bytes[3],
    ]);

    assert_eq!(
        extracted_corr_id, correlation_id,
        "Correlation ID should be preserved in request"
    );
}

#[test]
fn test_api_key_in_framed_request() {
    // Verify API key is at the correct position (first 2 bytes after size)
    let request = build_framed_request(18, 3, 1); // ApiVersions

    // API key is at offset 4 (after size prefix)
    let api_key_bytes = &request[4..6];
    let api_key = i16::from_be_bytes([api_key_bytes[0], api_key_bytes[1]]);

    assert_eq!(api_key, 18, "API key should be 18 (ApiVersions)");
}

#[test]
fn test_api_version_in_framed_request() {
    // Verify API version is at the correct position (bytes 6-7)
    let request = build_framed_request(18, 3, 1);

    // API version is at offset 6 (after size prefix + API key)
    let api_version_bytes = &request[6..8];
    let api_version = i16::from_be_bytes([api_version_bytes[0], api_version_bytes[1]]);

    assert_eq!(api_version, 3, "API version should be 3");
}

#[test]
fn test_framing_with_different_api_versions() {
    // Test framing is consistent across different API versions
    for version in 0..=3 {
        let request = build_framed_request(18, version, 1);
        assert!(
            verify_framing(&request).is_ok(),
            "Framing should be valid for ApiVersions v{}",
            version
        );

        // All should have valid size prefix
        let size = extract_size_prefix(&request).unwrap() as usize;
        assert_eq!(
            size,
            request.len() - 4,
            "Size should match body for v{}",
            version
        );
    }
}

#[test]
fn test_max_message_size_boundary() {
    // Per Kafka spec, max message size is configurable (default 1MB)
    // Test that size prefix can represent large values correctly

    // 100MB in bytes (the hardcoded max in Streamline)
    let large_size: u32 = 100 * 1024 * 1024;

    let mut msg = Vec::new();
    msg.extend_from_slice(&large_size.to_be_bytes());

    let extracted = extract_size_prefix(&msg).unwrap();
    assert_eq!(
        extracted, large_size,
        "Should correctly parse 100MB size prefix"
    );

    // 1GB (beyond typical max)
    let huge_size: u32 = 1024 * 1024 * 1024;
    let mut huge_msg = Vec::new();
    huge_msg.extend_from_slice(&huge_size.to_be_bytes());

    let extracted_huge = extract_size_prefix(&huge_msg).unwrap();
    assert_eq!(
        extracted_huge, huge_size,
        "Should correctly parse 1GB size prefix"
    );
}

#[test]
fn test_response_framing_format() {
    // Response follows the same framing format as request
    // Size (INT32) + ResponseHeader + ResponseBody

    // Simulate a minimal response: correlation_id only
    let correlation_id: i32 = 9999;
    let error_code: i16 = 0;

    // Build a minimal ApiVersions response manually
    // Response header v0 (for ApiVersions): just correlation_id
    let mut response_body = BytesMut::new();
    response_body.put_i32(correlation_id); // correlation_id
    response_body.put_i16(error_code); // error_code
    response_body.put_i8(1); // api_keys array length (compact encoding: N+1)
                             // One API key entry (minimal)
    response_body.put_i16(18); // api_key
    response_body.put_i16(0); // min_version
    response_body.put_i16(3); // max_version
    response_body.put_i8(0); // tagged fields for this entry (compact)
    response_body.put_i32(0); // throttle_time_ms
    response_body.put_i8(0); // tagged fields (compact)

    // Frame it
    let mut framed_response = Vec::new();
    framed_response.extend_from_slice(&(response_body.len() as u32).to_be_bytes());
    framed_response.extend_from_slice(&response_body);

    // Verify framing
    assert!(
        verify_framing(&framed_response).is_ok(),
        "Response framing should be valid"
    );

    // Verify correlation ID can be extracted
    let extracted_corr = i32::from_be_bytes([
        framed_response[4],
        framed_response[5],
        framed_response[6],
        framed_response[7],
    ]);
    assert_eq!(
        extracted_corr, correlation_id,
        "Correlation ID should match"
    );
}

#[test]
fn test_fragmented_message_reassembly_simulation() {
    // Simulate receiving a message in fragments (as would happen with TCP)
    let complete_request = build_framed_request(18, 3, 42);

    // Fragment 1: partial size prefix
    let frag1 = &complete_request[0..2];
    assert!(
        extract_size_prefix(frag1).is_none(),
        "Partial size prefix should not parse"
    );

    // Fragment 1+2: complete size prefix
    let frag1_2 = &complete_request[0..4];
    let size = extract_size_prefix(frag1_2).unwrap() as usize;
    assert!(size > 0, "Size should be parseable with complete prefix");

    // Fragment 1+2+3: size + partial body
    let partial_body = &complete_request[0..4 + size / 2];
    assert!(
        verify_framing(partial_body).is_err(),
        "Partial body should fail framing check"
    );

    // Complete message
    assert!(
        verify_framing(&complete_request).is_ok(),
        "Complete message should pass framing check"
    );
}

// ============================================================================
// Edge Cases and Error Conditions
// ============================================================================

#[test]
fn test_size_overflow_protection() {
    // Test that extremely large size values are detected
    // This simulates a malicious or corrupted message

    // Max u32 value
    let max_size: u32 = u32::MAX;
    let mut msg = Vec::new();
    msg.extend_from_slice(&max_size.to_be_bytes());

    let extracted = extract_size_prefix(&msg).unwrap();
    assert_eq!(extracted, max_size);

    // Server should reject this as it would require ~4GB allocation
    // This is a framing-level concern that the server must handle
}

#[test]
fn test_minimum_valid_request_size() {
    // A valid Kafka request must have at least:
    // - API key (2 bytes)
    // - API version (2 bytes)
    // - Correlation ID (4 bytes)
    // = 8 bytes minimum for header v1

    let min_header_size = 8; // Without client_id

    // A request with size < 8 is definitely invalid
    let mut too_small = Vec::new();
    too_small.extend_from_slice(&(4u32).to_be_bytes()); // Size = 4
    too_small.extend_from_slice(&[0u8; 4]); // Only 4 bytes of "body"

    // While framing is technically valid (size matches body)...
    assert!(verify_framing(&too_small).is_ok());

    // ...the message is semantically invalid (can't contain a valid header)
    let body = &too_small[4..];
    assert!(
        body.len() < min_header_size,
        "Body too small for valid request header"
    );
}
