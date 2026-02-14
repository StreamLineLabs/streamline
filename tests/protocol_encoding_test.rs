//! Protocol Encoding Tests - Compact Encoding & Varint
//!
//! Tests for Kafka protocol encoding formats per specification:
//! - Varint encoding (unsigned and signed)
//! - Compact strings (COMPACT_STRING, COMPACT_NULLABLE_STRING)
//! - Compact arrays (COMPACT_ARRAY, COMPACT_NULLABLE_ARRAY)
//! - Compact bytes (COMPACT_BYTES, COMPACT_NULLABLE_BYTES)
//!
//! Source: https://kafka.apache.org/protocol.html#protocol_types

use bytes::{BufMut, Bytes, BytesMut};

// ============================================================================
// Varint Encoding Utilities
// ============================================================================

/// Encode an unsigned varint (used for compact string/array lengths)
/// Per Kafka spec: Variable-length encoding using 7 bits per byte
fn encode_unsigned_varint(value: u32) -> Vec<u8> {
    let mut result = Vec::new();
    let mut v = value;

    loop {
        let mut byte = (v & 0x7F) as u8;
        v >>= 7;
        if v != 0 {
            byte |= 0x80; // Set continuation bit
        }
        result.push(byte);
        if v == 0 {
            break;
        }
    }

    result
}

/// Decode an unsigned varint from bytes
fn decode_unsigned_varint(data: &[u8]) -> Option<(u32, usize)> {
    let mut value: u32 = 0;
    let mut shift = 0;
    let mut bytes_read = 0;

    for &byte in data {
        bytes_read += 1;
        value |= ((byte & 0x7F) as u32) << shift;

        if byte & 0x80 == 0 {
            return Some((value, bytes_read));
        }

        shift += 7;
        if shift >= 32 {
            return None; // Overflow
        }
    }

    None // Incomplete
}

/// Encode a signed varint using ZigZag encoding
/// Per Kafka spec: (value << 1) ^ (value >> 31) for 32-bit
fn encode_signed_varint(value: i32) -> Vec<u8> {
    let zigzag = ((value << 1) ^ (value >> 31)) as u32;
    encode_unsigned_varint(zigzag)
}

/// Decode a signed varint using ZigZag decoding
fn decode_signed_varint(data: &[u8]) -> Option<(i32, usize)> {
    let (zigzag, bytes_read) = decode_unsigned_varint(data)?;
    let value = ((zigzag >> 1) as i32) ^ (-((zigzag & 1) as i32));
    Some((value, bytes_read))
}

/// Encode a compact string (length as varint N+1, then UTF-8 bytes)
fn encode_compact_string(s: &str) -> Vec<u8> {
    let mut result = Vec::new();
    // Length is N+1 where N is the number of bytes
    result.extend(encode_unsigned_varint((s.len() + 1) as u32));
    result.extend(s.as_bytes());
    result
}

/// Encode a compact nullable string (0 for null, N+1 for N bytes)
fn encode_compact_nullable_string(s: Option<&str>) -> Vec<u8> {
    match s {
        None => encode_unsigned_varint(0), // Null represented as 0
        Some(s) => encode_compact_string(s),
    }
}

/// Encode a non-compact string (INT16 length prefix)
fn encode_string(s: &str) -> Vec<u8> {
    let mut result = Vec::new();
    result.extend((s.len() as i16).to_be_bytes());
    result.extend(s.as_bytes());
    result
}

/// Encode a non-compact nullable string (-1 for null)
fn encode_nullable_string(s: Option<&str>) -> Vec<u8> {
    match s {
        None => (-1i16).to_be_bytes().to_vec(),
        Some(s) => encode_string(s),
    }
}

/// Encode compact bytes (length as varint N+1, then raw bytes)
fn encode_compact_bytes(data: &[u8]) -> Vec<u8> {
    let mut result = Vec::new();
    result.extend(encode_unsigned_varint((data.len() + 1) as u32));
    result.extend(data);
    result
}

/// Encode compact nullable bytes (0 for null)
fn encode_compact_nullable_bytes(data: Option<&[u8]>) -> Vec<u8> {
    match data {
        None => encode_unsigned_varint(0),
        Some(d) => encode_compact_bytes(d),
    }
}

// ============================================================================
// Phase 1: Varint Encoding Tests
// ============================================================================

#[test]
fn test_unsigned_varint_single_byte_values() {
    // Values 0-127 should encode as single byte
    for value in 0..=127u32 {
        let encoded = encode_unsigned_varint(value);
        assert_eq!(
            encoded.len(),
            1,
            "Value {} should encode to 1 byte, got {}",
            value,
            encoded.len()
        );
        assert_eq!(
            encoded[0], value as u8,
            "Value {} should encode to 0x{:02X}, got 0x{:02X}",
            value, value, encoded[0]
        );

        // Decode and verify roundtrip
        let (decoded, bytes_read) = decode_unsigned_varint(&encoded).unwrap();
        assert_eq!(decoded, value);
        assert_eq!(bytes_read, 1);
    }
}

#[test]
fn test_unsigned_varint_two_byte_values() {
    // Values 128-16383 should encode as 2 bytes
    let test_cases: Vec<(u32, Vec<u8>)> = vec![
        (128, vec![0x80, 0x01]),
        (129, vec![0x81, 0x01]),
        (255, vec![0xFF, 0x01]),
        (256, vec![0x80, 0x02]),
        (300, vec![0xAC, 0x02]),
        (16383, vec![0xFF, 0x7F]),
    ];

    for (value, expected) in test_cases {
        let encoded = encode_unsigned_varint(value);
        assert_eq!(
            encoded, expected,
            "Value {} should encode to {:?}, got {:?}",
            value, expected, encoded
        );

        let (decoded, bytes_read) = decode_unsigned_varint(&encoded).unwrap();
        assert_eq!(decoded, value);
        assert_eq!(bytes_read, expected.len());
    }
}

#[test]
fn test_unsigned_varint_three_byte_values() {
    // Values 16384-2097151 require 3 bytes
    let test_cases: Vec<(u32, Vec<u8>)> = vec![
        (16384, vec![0x80, 0x80, 0x01]),
        (100000, vec![0xA0, 0x8D, 0x06]),
        (2097151, vec![0xFF, 0xFF, 0x7F]),
    ];

    for (value, expected) in test_cases {
        let encoded = encode_unsigned_varint(value);
        assert_eq!(
            encoded, expected,
            "Value {} should encode to {:?}, got {:?}",
            value, expected, encoded
        );

        let (decoded, _) = decode_unsigned_varint(&encoded).unwrap();
        assert_eq!(decoded, value);
    }
}

#[test]
fn test_unsigned_varint_max_values() {
    // Test boundary and max values
    let test_cases: Vec<u32> = vec![
        0,
        1,
        127,
        128,
        16383,
        16384,
        2097151,
        2097152,
        268435455, // Max 4-byte varint
        u32::MAX,
    ];

    for value in test_cases {
        let encoded = encode_unsigned_varint(value);
        let (decoded, _) = decode_unsigned_varint(&encoded).unwrap();
        assert_eq!(decoded, value, "Roundtrip failed for value {}", value);
    }
}

#[test]
fn test_unsigned_varint_encoding_length() {
    // Verify encoding lengths match Kafka spec
    assert_eq!(encode_unsigned_varint(0).len(), 1);
    assert_eq!(encode_unsigned_varint(127).len(), 1);
    assert_eq!(encode_unsigned_varint(128).len(), 2);
    assert_eq!(encode_unsigned_varint(16383).len(), 2);
    assert_eq!(encode_unsigned_varint(16384).len(), 3);
    assert_eq!(encode_unsigned_varint(2097151).len(), 3);
    assert_eq!(encode_unsigned_varint(2097152).len(), 4);
    assert_eq!(encode_unsigned_varint(268435455).len(), 4);
    assert_eq!(encode_unsigned_varint(268435456).len(), 5);
    assert_eq!(encode_unsigned_varint(u32::MAX).len(), 5);
}

#[test]
fn test_signed_varint_zigzag_encoding() {
    // Per Kafka spec, signed varints use ZigZag encoding:
    // 0 -> 0, -1 -> 1, 1 -> 2, -2 -> 3, 2 -> 4, etc.
    let test_cases: Vec<(i32, u32)> = vec![
        (0, 0),
        (-1, 1),
        (1, 2),
        (-2, 3),
        (2, 4),
        (i32::MAX, (i32::MAX as u32) * 2),
        (i32::MIN, u32::MAX),
    ];

    for (signed, expected_zigzag) in test_cases {
        let encoded = encode_signed_varint(signed);
        let unsigned_decoded = decode_unsigned_varint(&encoded).unwrap().0;
        assert_eq!(
            unsigned_decoded, expected_zigzag,
            "ZigZag of {} should be {}, got {}",
            signed, expected_zigzag, unsigned_decoded
        );

        // Verify roundtrip
        let (decoded, _) = decode_signed_varint(&encoded).unwrap();
        assert_eq!(decoded, signed);
    }
}

#[test]
fn test_signed_varint_negative_values() {
    // Small negative values should be efficient
    assert_eq!(
        encode_signed_varint(-1).len(),
        1,
        "-1 should encode to 1 byte"
    );
    assert_eq!(
        encode_signed_varint(-64).len(),
        1,
        "-64 should encode to 1 byte"
    );
    assert_eq!(
        encode_signed_varint(-65).len(),
        2,
        "-65 should encode to 2 bytes"
    );

    // Roundtrip tests
    for value in [-1, -64, -65, -8192, -8193, -1000000, i32::MIN] {
        let encoded = encode_signed_varint(value);
        let (decoded, _) = decode_signed_varint(&encoded).unwrap();
        assert_eq!(decoded, value, "Roundtrip failed for {}", value);
    }
}

#[test]
fn test_varint_incomplete_data() {
    // Multi-byte varint missing continuation
    let incomplete = vec![0x80]; // Has continuation bit but no more bytes
    assert!(
        decode_unsigned_varint(&incomplete).is_none(),
        "Should fail on incomplete varint"
    );

    // Three-byte varint with only two bytes
    let incomplete2 = vec![0x80, 0x80]; // Needs third byte
    assert!(
        decode_unsigned_varint(&incomplete2).is_none(),
        "Should fail on incomplete varint"
    );

    // Empty data
    assert!(decode_unsigned_varint(&[]).is_none());
}

// ============================================================================
// Phase 1: Compact String Tests
// ============================================================================

#[test]
fn test_compact_string_encoding() {
    // Empty string: length = 0+1 = 1
    let empty = encode_compact_string("");
    assert_eq!(empty, vec![0x01], "Empty string should encode as [0x01]");

    // "a": length = 1+1 = 2, then 'a'
    let single = encode_compact_string("a");
    assert_eq!(single, vec![0x02, b'a']);

    // "hello": length = 5+1 = 6, then "hello"
    let hello = encode_compact_string("hello");
    assert_eq!(hello, vec![0x06, b'h', b'e', b'l', b'l', b'o']);
}

#[test]
fn test_compact_string_vs_non_compact() {
    // Compare encoding sizes
    let short_str = "test";
    let compact = encode_compact_string(short_str);
    let non_compact = encode_string(short_str);

    // Compact: 1 byte length + 4 bytes = 5 bytes
    // Non-compact: 2 bytes length + 4 bytes = 6 bytes
    assert_eq!(compact.len(), 5);
    assert_eq!(non_compact.len(), 6);

    // For longer strings, the difference is even more pronounced
    let long_str = "a".repeat(127);
    let compact_long = encode_compact_string(&long_str);
    let non_compact_long = encode_string(&long_str);

    // Compact: 1 byte length (128 fits in 2 bytes as varint, but 127+1=128)
    // Actually 128 = 0x80 0x01, so 2 bytes
    assert_eq!(compact_long.len(), 2 + 127); // 2 byte varint + data
    assert_eq!(non_compact_long.len(), 2 + 127); // Same for this size
}

#[test]
fn test_compact_nullable_string_null() {
    // Null is encoded as varint 0
    let null_str = encode_compact_nullable_string(None);
    assert_eq!(null_str, vec![0x00], "Null string should encode as [0x00]");
}

#[test]
fn test_compact_nullable_string_empty() {
    // Empty string (not null): length = 0+1 = 1
    let empty = encode_compact_nullable_string(Some(""));
    assert_eq!(
        empty,
        vec![0x01],
        "Empty string should encode as [0x01], distinct from null"
    );
}

#[test]
fn test_compact_nullable_string_vs_non_compact_nullable() {
    // Null comparison
    let compact_null = encode_compact_nullable_string(None);
    let non_compact_null = encode_nullable_string(None);

    assert_eq!(compact_null, vec![0x00]); // 1 byte
    assert_eq!(non_compact_null, vec![0xFF, 0xFF]); // 2 bytes (-1 as INT16)

    // Non-null comparison
    let value = Some("test");
    let compact = encode_compact_nullable_string(value);
    let non_compact = encode_nullable_string(value);

    assert_eq!(compact.len(), 5); // 1 byte length + 4 bytes
    assert_eq!(non_compact.len(), 6); // 2 bytes length + 4 bytes
}

#[test]
fn test_compact_string_utf8() {
    // UTF-8 multi-byte characters
    let utf8_str = "\u{00E9}"; // 'é' - 2 bytes in UTF-8
    let encoded = encode_compact_string(utf8_str);
    // Length = 2 bytes + 1 = 3
    assert_eq!(encoded[0], 0x03);
    assert_eq!(&encoded[1..], utf8_str.as_bytes());

    // More complex UTF-8
    let emoji = "\u{1F600}"; // '' - 4 bytes in UTF-8
    let encoded_emoji = encode_compact_string(emoji);
    assert_eq!(encoded_emoji[0], 0x05); // 4+1 = 5
    assert_eq!(&encoded_emoji[1..], emoji.as_bytes());
}

#[test]
fn test_compact_string_large() {
    // Test string requiring multi-byte varint length
    let large = "x".repeat(1000);
    let encoded = encode_compact_string(&large);

    // Length = 1000+1 = 1001, which requires 2-byte varint
    let (len_value, len_bytes) = decode_unsigned_varint(&encoded).unwrap();
    assert_eq!(len_value, 1001);
    assert_eq!(len_bytes, 2);
    assert_eq!(encoded.len(), 2 + 1000);
}

// ============================================================================
// Phase 1: Compact Array Tests
// ============================================================================

/// Encode a compact array of i32 values
fn encode_compact_array_i32(values: &[i32]) -> Vec<u8> {
    let mut result = Vec::new();
    // Array length as N+1
    result.extend(encode_unsigned_varint((values.len() + 1) as u32));
    for &v in values {
        result.extend(v.to_be_bytes());
    }
    result
}

/// Encode a compact nullable array of i32 values
fn encode_compact_nullable_array_i32(values: Option<&[i32]>) -> Vec<u8> {
    match values {
        None => encode_unsigned_varint(0), // Null array
        Some(v) => encode_compact_array_i32(v),
    }
}

/// Encode a non-compact array of i32 values (INT32 length prefix)
fn encode_array_i32(values: &[i32]) -> Vec<u8> {
    let mut result = Vec::new();
    result.extend((values.len() as i32).to_be_bytes());
    for &v in values {
        result.extend(v.to_be_bytes());
    }
    result
}

#[test]
fn test_compact_array_encoding() {
    // Empty array: length = 0+1 = 1
    let empty: &[i32] = &[];
    let encoded_empty = encode_compact_array_i32(empty);
    assert_eq!(encoded_empty, vec![0x01]);

    // Single element: length = 1+1 = 2
    let single = &[42i32];
    let encoded_single = encode_compact_array_i32(single);
    assert_eq!(
        encoded_single,
        vec![0x02, 0x00, 0x00, 0x00, 0x2A] // length=2, then 42 as INT32
    );
}

#[test]
fn test_compact_array_vs_non_compact() {
    let values = &[1i32, 2, 3, 4, 5];

    let compact = encode_compact_array_i32(values);
    let non_compact = encode_array_i32(values);

    // Compact: 1 byte length (6) + 5*4 bytes = 21 bytes
    assert_eq!(compact.len(), 1 + 20);
    // Non-compact: 4 byte length + 5*4 bytes = 24 bytes
    assert_eq!(non_compact.len(), 4 + 20);
}

#[test]
fn test_compact_nullable_array_null() {
    let null_array: Option<&[i32]> = None;
    let encoded = encode_compact_nullable_array_i32(null_array);
    assert_eq!(encoded, vec![0x00], "Null array should encode as [0x00]");
}

#[test]
fn test_compact_nullable_array_empty() {
    let empty_array: Option<&[i32]> = Some(&[]);
    let encoded = encode_compact_nullable_array_i32(empty_array);
    assert_eq!(
        encoded,
        vec![0x01],
        "Empty array should encode as [0x01], distinct from null"
    );
}

#[test]
fn test_compact_array_large() {
    // Array with many elements requiring multi-byte varint
    let large: Vec<i32> = (0..200).collect();
    let encoded = encode_compact_array_i32(&large);

    // Length = 200+1 = 201, which requires 2-byte varint
    let (len_value, len_bytes) = decode_unsigned_varint(&encoded).unwrap();
    assert_eq!(len_value, 201);
    assert_eq!(len_bytes, 2);
    assert_eq!(encoded.len(), 2 + 200 * 4);
}

// ============================================================================
// Phase 1: Compact Bytes Tests
// ============================================================================

#[test]
fn test_compact_bytes_encoding() {
    // Empty bytes: length = 0+1 = 1
    let empty = encode_compact_bytes(&[]);
    assert_eq!(empty, vec![0x01]);

    // Some bytes
    let data = &[0xDE, 0xAD, 0xBE, 0xEF];
    let encoded = encode_compact_bytes(data);
    assert_eq!(encoded, vec![0x05, 0xDE, 0xAD, 0xBE, 0xEF]); // 4+1=5
}

#[test]
fn test_compact_nullable_bytes_null() {
    let null_bytes: Option<&[u8]> = None;
    let encoded = encode_compact_nullable_bytes(null_bytes);
    assert_eq!(encoded, vec![0x00]);
}

#[test]
fn test_compact_nullable_bytes_empty() {
    let empty_bytes: Option<&[u8]> = Some(&[]);
    let encoded = encode_compact_nullable_bytes(empty_bytes);
    assert_eq!(encoded, vec![0x01], "Empty bytes distinct from null");
}

#[test]
fn test_compact_bytes_large() {
    // Large byte array
    let large = vec![0xAB; 10000];
    let encoded = encode_compact_bytes(&large);

    let (len_value, len_bytes) = decode_unsigned_varint(&encoded).unwrap();
    assert_eq!(len_value, 10001); // 10000+1
    assert_eq!(len_bytes, 2); // 10001 fits in 2-byte varint
    assert_eq!(encoded.len(), 2 + 10000);
}

// ============================================================================
// Phase 1: Flexible vs Non-Flexible Version Encoding
// ============================================================================

#[test]
fn test_flexible_version_header_difference() {
    // Simulate request header v1 (non-flexible) vs v2 (flexible)

    // Header v1: api_key(2) + api_version(2) + correlation_id(4) + client_id(nullable_string)
    // client_id uses INT16 length prefix

    // Header v2: same fields but client_id uses COMPACT_NULLABLE_STRING
    // Plus tagged fields at the end (varint 0 for empty tags)

    let client_id = "test-client";

    // Header v1 layout
    let mut header_v1 = BytesMut::new();
    header_v1.put_i16(18); // api_key
    header_v1.put_i16(2); // api_version
    header_v1.put_i32(1234); // correlation_id
    header_v1.extend(encode_nullable_string(Some(client_id)));
    // Total: 2 + 2 + 4 + (2 + 11) = 21 bytes

    // Header v2 layout
    let mut header_v2 = BytesMut::new();
    header_v2.put_i16(18); // api_key
    header_v2.put_i16(3); // api_version
    header_v2.put_i32(5678); // correlation_id
    header_v2.extend(encode_compact_nullable_string(Some(client_id)));
    header_v2.put_u8(0); // Empty tagged fields
                         // Total: 2 + 2 + 4 + (1 + 11) + 1 = 21 bytes

    // Both have similar sizes for this case
    assert_eq!(header_v1.len(), 21);
    assert_eq!(header_v2.len(), 21);
}

#[test]
fn test_tagged_fields_empty() {
    // Empty tagged fields section is just a single byte: 0
    let empty_tags = encode_unsigned_varint(0);
    assert_eq!(empty_tags, vec![0x00]);
}

#[test]
fn test_tagged_field_encoding() {
    // Tagged field format: tag(varint) + size(varint) + data
    let tag = 0u32;
    let data = &[0x01, 0x02, 0x03];

    let mut encoded = Vec::new();
    // Number of tagged fields: 1
    encoded.extend(encode_unsigned_varint(1));
    // Tag number
    encoded.extend(encode_unsigned_varint(tag));
    // Data size
    encoded.extend(encode_unsigned_varint(data.len() as u32));
    // Data
    encoded.extend(data);

    // Should be: [0x01 (count), 0x00 (tag), 0x03 (size), 0x01, 0x02, 0x03 (data)]
    assert_eq!(encoded, vec![0x01, 0x00, 0x03, 0x01, 0x02, 0x03]);
}

// ============================================================================
// Phase 1: Roundtrip Tests Using kafka-protocol Crate
// ============================================================================

use kafka_protocol::messages::*;
use kafka_protocol::protocol::{Decodable, Encodable};

#[test]
fn test_api_versions_request_roundtrip_v0_non_flexible() {
    // v0 uses non-flexible encoding
    let request = ApiVersionsRequest::default();

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 0).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = ApiVersionsRequest::decode(&mut bytes, 0).unwrap();

    assert_eq!(request, decoded);
}

#[test]
fn test_api_versions_request_roundtrip_v3_flexible() {
    // v3 uses flexible encoding (compact strings, tagged fields)
    let request = ApiVersionsRequest::default()
        .with_client_software_name("test-client".to_string().into())
        .with_client_software_version("1.0.0".to_string().into());

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 3).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = ApiVersionsRequest::decode(&mut bytes, 3).unwrap();

    assert_eq!(request, decoded);
}

#[test]
fn test_metadata_request_roundtrip_v0_non_flexible() {
    use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
    use kafka_protocol::protocol::StrBytes;

    let request =
        MetadataRequest::default()
            .with_topics(Some(vec![MetadataRequestTopic::default().with_name(Some(
                TopicName::from(StrBytes::from_static_str("test-topic")),
            ))]));

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 0).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = MetadataRequest::decode(&mut bytes, 0).unwrap();

    assert_eq!(request, decoded);
}

#[test]
fn test_metadata_request_roundtrip_v9_flexible() {
    use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
    use kafka_protocol::protocol::StrBytes;

    let request = MetadataRequest::default()
        .with_topics(Some(vec![MetadataRequestTopic::default().with_name(Some(
            TopicName::from(StrBytes::from_static_str("test-topic")),
        ))]))
        .with_allow_auto_topic_creation(true);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 9).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = MetadataRequest::decode(&mut bytes, 9).unwrap();

    assert_eq!(request, decoded);
}

#[test]
fn test_produce_request_roundtrip_v0_non_flexible() {
    let request = ProduceRequest::default()
        .with_acks(1)
        .with_timeout_ms(30000);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 0).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = ProduceRequest::decode(&mut bytes, 0).unwrap();

    assert_eq!(request, decoded);
}

#[test]
fn test_produce_request_roundtrip_v9_flexible() {
    let request = ProduceRequest::default()
        .with_acks(-1)
        .with_timeout_ms(30000);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 9).unwrap();

    let mut bytes = Bytes::from(buf);
    let decoded = ProduceRequest::decode(&mut bytes, 9).unwrap();

    assert_eq!(request, decoded);
}

#[test]
fn test_fetch_request_roundtrip_across_versions() {
    // Test fetch request encoding across flexible version boundary
    for version in [0, 4, 11, 12] {
        let request = FetchRequest::default()
            .with_max_wait_ms(500)
            .with_min_bytes(1)
            .with_max_bytes(1024 * 1024);

        let mut buf = BytesMut::new();
        request.encode(&mut buf, version).unwrap();

        let mut bytes = Bytes::from(buf);
        let decoded = FetchRequest::decode(&mut bytes, version).unwrap();

        assert_eq!(
            request.max_wait_ms, decoded.max_wait_ms,
            "max_wait_ms mismatch at v{}",
            version
        );
        assert_eq!(
            request.min_bytes, decoded.min_bytes,
            "min_bytes mismatch at v{}",
            version
        );
    }
}

#[test]
fn test_encoding_size_difference_flexible_vs_non_flexible() {
    use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
    use kafka_protocol::protocol::StrBytes;

    // Create identical request content
    let request = MetadataRequest::default()
        .with_topics(Some(vec![MetadataRequestTopic::default().with_name(Some(
            TopicName::from(StrBytes::from_static_str("my-test-topic")),
        ))]))
        .with_allow_auto_topic_creation(true);

    // Encode at v8 (non-flexible) and v9 (flexible)
    let mut buf_v8 = BytesMut::new();
    request.encode(&mut buf_v8, 8).unwrap();

    let mut buf_v9 = BytesMut::new();
    request.encode(&mut buf_v9, 9).unwrap();

    // Both should decode successfully
    let mut bytes_v8 = Bytes::from(buf_v8.clone());
    let decoded_v8 = MetadataRequest::decode(&mut bytes_v8, 8).unwrap();

    let mut bytes_v9 = Bytes::from(buf_v9.clone());
    let decoded_v9 = MetadataRequest::decode(&mut bytes_v9, 9).unwrap();

    // Core fields should match
    assert_eq!(
        decoded_v8.allow_auto_topic_creation,
        decoded_v9.allow_auto_topic_creation
    );

    // Log size difference for visibility
    println!(
        "Metadata request size: v8={} bytes, v9={} bytes",
        buf_v8.len(),
        buf_v9.len()
    );
}

// ============================================================================
// Phase 1: Edge Cases
// ============================================================================

#[test]
fn test_varint_boundary_values() {
    // Test values at encoding boundaries
    let boundaries = [
        (0, 1),
        (127, 1),       // Max 1-byte
        (128, 2),       // Min 2-byte
        (16383, 2),     // Max 2-byte
        (16384, 3),     // Min 3-byte
        (2097151, 3),   // Max 3-byte
        (2097152, 4),   // Min 4-byte
        (268435455, 4), // Max 4-byte
        (268435456, 5), // Min 5-byte
    ];

    for (value, expected_len) in boundaries {
        let encoded = encode_unsigned_varint(value);
        assert_eq!(
            encoded.len(),
            expected_len,
            "Value {} should encode to {} bytes, got {}",
            value,
            expected_len,
            encoded.len()
        );
    }
}

#[test]
fn test_compact_string_all_nulls_vs_empty() {
    // Ensure we can distinguish between null and empty
    let null_encoded = encode_compact_nullable_string(None);
    let empty_encoded = encode_compact_nullable_string(Some(""));

    assert_ne!(
        null_encoded, empty_encoded,
        "Null and empty string must have different encodings"
    );
    assert_eq!(null_encoded, vec![0x00]);
    assert_eq!(empty_encoded, vec![0x01]);
}

#[test]
fn test_compact_array_nested_strings() {
    // Test compact array of compact strings (common pattern)
    let strings = &["one", "two", "three"];

    let mut encoded = Vec::new();
    // Array length: 3+1 = 4
    encoded.extend(encode_unsigned_varint(4));
    // Each string as compact string
    for s in strings {
        encoded.extend(encode_compact_string(s));
    }

    // Verify structure
    let (array_len, offset) = decode_unsigned_varint(&encoded).unwrap();
    assert_eq!(array_len, 4); // 3+1

    // First string should be at offset, length 3+1=4, "one"
    let (str_len, str_offset) = decode_unsigned_varint(&encoded[offset..]).unwrap();
    assert_eq!(str_len, 4); // 3+1
    assert_eq!(
        &encoded[offset + str_offset..offset + str_offset + 3],
        b"one"
    );
}
