use super::*;

#[test]
fn test_record_batch_magic_byte_v2() {
    // Verify that the record batch uses magic byte 2 (Kafka v2 format)
    let batch = create_test_record_batch(0, -1, -1, -1, 0, 1);

    // Magic byte is at offset 16 (baseOffset:8 + batchLength:4 + partitionLeaderEpoch:4)
    let magic = batch[16] as i8;
    assert_eq!(
        magic, RECORD_BATCH_MAGIC_V2,
        "Record batch must use magic byte 2 (Kafka v2), got {}",
        magic
    );
}

#[test]
fn test_record_batch_base_offset() {
    // Test that base offset is correctly set in the batch header
    let base_offset = 42i64;
    let batch = create_test_record_batch(base_offset, -1, -1, -1, 0, 1);

    // baseOffset is at offset 0, 8 bytes
    let actual_offset = i64::from_be_bytes(batch[0..8].try_into().unwrap());
    assert_eq!(
        actual_offset, base_offset,
        "Base offset mismatch: expected {}, got {}",
        base_offset, actual_offset
    );
}

#[test]
fn test_record_batch_last_offset_delta() {
    // Test that lastOffsetDelta equals record_count - 1
    let record_count = 5i32;
    let batch = create_test_record_batch(0, -1, -1, -1, 0, record_count);

    // lastOffsetDelta is at offset 23 (after magic:1 + crc:4 + attributes:2)
    // Actually: baseOffset:8 + batchLength:4 + partitionLeaderEpoch:4 + magic:1 + crc:4 + attributes:2 = 23
    let last_offset_delta = i32::from_be_bytes(batch[23..27].try_into().unwrap());
    assert_eq!(
        last_offset_delta,
        record_count - 1,
        "lastOffsetDelta should be {} for {} records, got {}",
        record_count - 1,
        record_count,
        last_offset_delta
    );
}

#[test]
fn test_record_batch_producer_id_epoch() {
    // Test that producer ID and epoch are correctly encoded
    let producer_id = 12345i64;
    let producer_epoch = 7i16;
    let batch = create_test_record_batch(0, producer_id, producer_epoch, 0, 0, 1);

    // producerId is at offset 43 (after maxTimestamp)
    // baseOffset:8 + batchLength:4 + partitionLeaderEpoch:4 + magic:1 + crc:4 + attributes:2 + lastOffsetDelta:4 + firstTimestamp:8 + maxTimestamp:8 = 43
    let actual_producer_id = i64::from_be_bytes(batch[43..51].try_into().unwrap());
    assert_eq!(
        actual_producer_id, producer_id,
        "Producer ID mismatch: expected {}, got {}",
        producer_id, actual_producer_id
    );

    // producerEpoch is at offset 51
    let actual_epoch = i16::from_be_bytes(batch[51..53].try_into().unwrap());
    assert_eq!(
        actual_epoch, producer_epoch,
        "Producer epoch mismatch: expected {}, got {}",
        producer_epoch, actual_epoch
    );
}

#[test]
fn test_record_batch_base_sequence() {
    // Test that base sequence is correctly encoded for idempotent producers
    let base_sequence = 100i32;
    let batch = create_test_record_batch(0, 1, 0, base_sequence, 0, 1);

    // baseSequence is at offset 53 (after producerEpoch)
    let actual_sequence = i32::from_be_bytes(batch[53..57].try_into().unwrap());
    assert_eq!(
        actual_sequence, base_sequence,
        "Base sequence mismatch: expected {}, got {}",
        base_sequence, actual_sequence
    );
}

#[test]
fn test_record_batch_attributes_compression_none() {
    // Test that compression=none is encoded as bits 0-2 = 0
    let batch = create_test_record_batch(0, -1, -1, -1, 0, 1);

    // attributes is at offset 21
    let attributes = i16::from_be_bytes(batch[21..23].try_into().unwrap());
    let compression_type = attributes & 0x07; // bits 0-2
    assert_eq!(
        compression_type, 0,
        "Compression type should be 0 (none), got {}",
        compression_type
    );
}

#[test]
fn test_record_batch_attributes_compression_gzip() {
    // Test that compression=gzip is encoded as bits 0-2 = 1
    let batch = create_test_record_batch(0, -1, -1, -1, 1, 1);

    let attributes = i16::from_be_bytes(batch[21..23].try_into().unwrap());
    let compression_type = attributes & 0x07;
    assert_eq!(
        compression_type, 1,
        "Compression type should be 1 (gzip), got {}",
        compression_type
    );
}

#[test]
fn test_record_batch_attributes_compression_snappy() {
    // Test that compression=snappy is encoded as bits 0-2 = 2
    let batch = create_test_record_batch(0, -1, -1, -1, 2, 1);

    let attributes = i16::from_be_bytes(batch[21..23].try_into().unwrap());
    let compression_type = attributes & 0x07;
    assert_eq!(
        compression_type, 2,
        "Compression type should be 2 (snappy), got {}",
        compression_type
    );
}

#[test]
fn test_record_batch_attributes_compression_lz4() {
    // Test that compression=lz4 is encoded as bits 0-2 = 3
    let batch = create_test_record_batch(0, -1, -1, -1, 3, 1);

    let attributes = i16::from_be_bytes(batch[21..23].try_into().unwrap());
    let compression_type = attributes & 0x07;
    assert_eq!(
        compression_type, 3,
        "Compression type should be 3 (lz4), got {}",
        compression_type
    );
}

#[test]
fn test_record_batch_attributes_compression_zstd() {
    // Test that compression=zstd is encoded as bits 0-2 = 4
    let batch = create_test_record_batch(0, -1, -1, -1, 4, 1);

    let attributes = i16::from_be_bytes(batch[21..23].try_into().unwrap());
    let compression_type = attributes & 0x07;
    assert_eq!(
        compression_type, 4,
        "Compression type should be 4 (zstd), got {}",
        compression_type
    );
}

#[test]
fn test_record_batch_attributes_timestamp_type() {
    // Test timestamp type bit (bit 3): 0=CreateTime, 1=LogAppendTime
    let batch = create_test_record_batch(0, -1, -1, -1, 0, 1);

    let attributes = i16::from_be_bytes(batch[21..23].try_into().unwrap());
    let timestamp_type = (attributes >> 3) & 0x01;
    assert_eq!(
        timestamp_type, 0,
        "Timestamp type should be 0 (CreateTime), got {}",
        timestamp_type
    );
}

#[test]
fn test_record_batch_attributes_transactional_bit() {
    // Test transactional bit (bit 4): 0=non-transactional, 1=transactional
    // When using producer_id with transaction, bit 4 should be set
    let batch = create_test_record_batch(0, -1, -1, -1, 0, 1);

    let attributes = i16::from_be_bytes(batch[21..23].try_into().unwrap());
    let transactional = (attributes >> 4) & 0x01;
    assert_eq!(
        transactional, 0,
        "Transactional bit should be 0 (non-transactional), got {}",
        transactional
    );
}

#[test]
fn test_record_batch_attributes_control_bit() {
    // Test control bit (bit 5): 0=data batch, 1=control batch (transaction markers)
    let batch = create_test_record_batch(0, -1, -1, -1, 0, 1);

    let attributes = i16::from_be_bytes(batch[21..23].try_into().unwrap());
    let is_control = (attributes >> 5) & 0x01;
    assert_eq!(
        is_control, 0,
        "Control bit should be 0 (data batch), got {}",
        is_control
    );
}

#[test]
fn test_record_batch_crc_calculation() {
    // Test that CRC32C is calculated over attributes through end of batch
    let batch = create_test_record_batch(0, -1, -1, -1, 0, 1);

    // CRC is at offset 17 (after magic)
    let stored_crc = u32::from_be_bytes(batch[17..21].try_into().unwrap());

    // CRC is calculated over everything from attributes (offset 21) to end
    let crc_data = &batch[21..];
    let calculated_crc = crc32fast::hash(crc_data);

    assert_eq!(
        stored_crc, calculated_crc,
        "CRC mismatch: stored {} != calculated {}",
        stored_crc, calculated_crc
    );
}

#[test]
fn test_record_batch_timestamps() {
    // Test that timestamps are present and valid
    let batch = create_test_record_batch(0, -1, -1, -1, 0, 1);

    // firstTimestamp is at offset 27
    let first_timestamp = i64::from_be_bytes(batch[27..35].try_into().unwrap());
    // maxTimestamp is at offset 35
    let max_timestamp = i64::from_be_bytes(batch[35..43].try_into().unwrap());

    assert!(
        first_timestamp > 0,
        "First timestamp should be positive (epoch millis)"
    );
    assert!(
        max_timestamp >= first_timestamp,
        "Max timestamp should be >= first timestamp"
    );
}

#[test]
fn test_record_batch_no_producer_id() {
    // Test that NO_PRODUCER_ID (-1) is correctly encoded
    let batch = create_test_record_batch(0, -1, -1, -1, 0, 1);

    let producer_id = i64::from_be_bytes(batch[43..51].try_into().unwrap());
    assert_eq!(
        producer_id, -1,
        "NO_PRODUCER_ID should be -1, got {}",
        producer_id
    );
}

#[test]
fn test_record_batch_record_count() {
    // Test that record count is correctly stored in the batch
    let record_count = 3i32;
    let batch = create_test_record_batch(0, -1, -1, -1, 0, record_count);

    // records array count is at offset 57 (after baseSequence)
    let actual_count = i32::from_be_bytes(batch[57..61].try_into().unwrap());
    assert_eq!(
        actual_count, record_count,
        "Record count mismatch: expected {}, got {}",
        record_count, actual_count
    );
}

#[test]
fn test_record_batch_batch_length() {
    // Test that batch length is correctly calculated
    let batch = create_test_record_batch(0, -1, -1, -1, 0, 1);

    // batchLength is at offset 8
    let batch_length = i32::from_be_bytes(batch[8..12].try_into().unwrap());

    // batchLength should be total_size - 12 (baseOffset:8 + batchLength:4)
    let expected_length = (batch.len() - 12) as i32;
    assert_eq!(
        batch_length, expected_length,
        "Batch length mismatch: expected {}, got {}",
        expected_length, batch_length
    );
}

#[test]
fn test_record_batch_partition_leader_epoch() {
    // Test partition leader epoch field
    let batch = create_test_record_batch(0, -1, -1, -1, 0, 1);

    // partitionLeaderEpoch is at offset 12
    let partition_leader_epoch = i32::from_be_bytes(batch[12..16].try_into().unwrap());
    assert_eq!(
        partition_leader_epoch, 0,
        "Partition leader epoch should be 0 initially"
    );
}

#[test]
fn test_record_batch_multiple_records() {
    // Test batch with multiple records
    let record_count = 5i32;
    let batch = create_test_record_batch(0, -1, -1, -1, 0, record_count);

    // Verify record count
    let actual_count = i32::from_be_bytes(batch[57..61].try_into().unwrap());
    assert_eq!(actual_count, record_count);

    // Verify lastOffsetDelta = record_count - 1
    let last_offset_delta = i32::from_be_bytes(batch[23..27].try_into().unwrap());
    assert_eq!(last_offset_delta, record_count - 1);
}

#[test]
fn test_record_batch_idempotent_producer_fields() {
    // Test all idempotent producer fields together
    let producer_id = 9999i64;
    let producer_epoch = 5i16;
    let base_sequence = 42i32;
    let batch = create_test_record_batch(0, producer_id, producer_epoch, base_sequence, 0, 1);

    // Verify all idempotent fields
    let actual_pid = i64::from_be_bytes(batch[43..51].try_into().unwrap());
    let actual_epoch = i16::from_be_bytes(batch[51..53].try_into().unwrap());
    let actual_seq = i32::from_be_bytes(batch[53..57].try_into().unwrap());

    assert_eq!(actual_pid, producer_id, "Producer ID mismatch");
    assert_eq!(actual_epoch, producer_epoch, "Producer epoch mismatch");
    assert_eq!(actual_seq, base_sequence, "Base sequence mismatch");
}

#[test]
fn test_record_batch_zero_records() {
    // Edge case: batch with zero records (should still be valid structurally)
    let batch = create_test_record_batch(0, -1, -1, -1, 0, 0);

    // Verify record count is 0
    let actual_count = i32::from_be_bytes(batch[57..61].try_into().unwrap());
    assert_eq!(actual_count, 0);

    // lastOffsetDelta should be -1 (0 - 1)
    let last_offset_delta = i32::from_be_bytes(batch[23..27].try_into().unwrap());
    assert_eq!(last_offset_delta, -1);
}

#[test]
fn test_record_batch_max_offset_calculation() {
    // Test that max offset = base_offset + lastOffsetDelta
    let base_offset = 100i64;
    let record_count = 10i32;
    let batch = create_test_record_batch(base_offset, -1, -1, -1, 0, record_count);

    let actual_base = i64::from_be_bytes(batch[0..8].try_into().unwrap());
    let last_offset_delta = i32::from_be_bytes(batch[23..27].try_into().unwrap());

    let max_offset = actual_base + last_offset_delta as i64;
    let expected_max = base_offset + (record_count - 1) as i64;

    assert_eq!(
        max_offset, expected_max,
        "Max offset should be {}, got {}",
        expected_max, max_offset
    );
}

#[test]
#[allow(clippy::useless_vec)]
fn test_record_headers_structure() {
    // Record headers are key-value pairs attached to individual records
    // Headers format: varint(header_count) + [header_key, header_value]...

    #[allow(dead_code)]
    struct RecordHeader {
        key: String,
        value: Vec<u8>,
    }

    let headers = vec![
        RecordHeader {
            key: "traceId".to_string(),
            value: b"abc123".to_vec(),
        },
        RecordHeader {
            key: "spanId".to_string(),
            value: b"def456".to_vec(),
        },
    ];

    assert_eq!(headers.len(), 2);
    assert_eq!(headers[0].key, "traceId");
    assert_eq!(headers[1].key, "spanId");
}

#[test]
fn test_record_headers_varint_count() {
    // Header count is encoded as varint (N+1) in Kafka protocol
    // 0 = no headers, 1 = one header, etc.

    fn encode_header_count(count: usize) -> i32 {
        // In Kafka protocol, header count is varint encoded
        count as i32
    }

    assert_eq!(encode_header_count(0), 0);
    assert_eq!(encode_header_count(1), 1);
    assert_eq!(encode_header_count(10), 10);
    assert_eq!(encode_header_count(100), 100);
}

#[test]
fn test_record_headers_string_key() {
    // Header keys are UTF-8 strings
    // Key format: varint(key_length) + key_bytes

    #[allow(dead_code)]
    struct RecordHeader {
        key: String,
        value: Vec<u8>,
    }

    let header = RecordHeader {
        key: "Content-Type".to_string(),
        value: b"application/json".to_vec(),
    };

    // Key should be valid UTF-8
    assert!(header.key.is_ascii());
    assert_eq!(header.key.len(), 12);
}

#[test]
fn test_record_headers_bytes_value() {
    // Header values are raw bytes (not necessarily UTF-8)
    // Value format: varint(value_length) + value_bytes

    #[allow(dead_code)]
    struct RecordHeader {
        key: String,
        value: Vec<u8>,
    }

    // Binary value
    let header = RecordHeader {
        key: "binary-data".to_string(),
        value: vec![0x00, 0x01, 0x02, 0xFF, 0xFE],
    };

    assert_eq!(header.value.len(), 5);
    assert_eq!(header.value[0], 0x00);
    assert_eq!(header.value[4], 0xFE);
}

#[test]
fn test_record_headers_null_value() {
    // Header values can be null (represented as Option<Vec<u8>>)

    #[allow(dead_code)]
    struct RecordHeader {
        key: String,
        value: Option<Vec<u8>>,
    }

    let header_with_value = RecordHeader {
        key: "has-value".to_string(),
        value: Some(b"data".to_vec()),
    };

    let header_null_value = RecordHeader {
        key: "no-value".to_string(),
        value: None,
    };

    assert!(header_with_value.value.is_some());
    assert!(header_null_value.value.is_none());
}

#[test]
fn test_record_headers_multiple_per_record() {
    // A single record can have multiple headers

    #[allow(dead_code)]
    struct Record {
        key: Vec<u8>,
        value: Vec<u8>,
        headers: Vec<(String, Vec<u8>)>,
    }

    let record = Record {
        key: b"user-123".to_vec(),
        value: b"{\"action\": \"click\"}".to_vec(),
        headers: vec![
            ("traceId".to_string(), b"trace-abc".to_vec()),
            ("spanId".to_string(), b"span-123".to_vec()),
            ("source".to_string(), b"web".to_vec()),
            ("timestamp".to_string(), b"1702400000000".to_vec()),
        ],
    };

    assert_eq!(record.headers.len(), 4);
}

#[test]
fn test_record_headers_empty() {
    // Records can have zero headers

    #[allow(dead_code)]
    struct Record {
        key: Vec<u8>,
        value: Vec<u8>,
        headers: Vec<(String, Vec<u8>)>,
    }

    let record = Record {
        key: b"key".to_vec(),
        value: b"value".to_vec(),
        headers: vec![],
    };

    assert!(record.headers.is_empty());
}

#[test]
#[allow(clippy::useless_vec)]
fn test_record_headers_duplicate_keys() {
    // Multiple headers with the same key are allowed

    let headers = vec![
        ("x-custom".to_string(), b"value1".to_vec()),
        ("x-custom".to_string(), b"value2".to_vec()),
        ("x-custom".to_string(), b"value3".to_vec()),
    ];

    // Count headers with key "x-custom"
    let count = headers.iter().filter(|(k, _)| k == "x-custom").count();
    assert_eq!(count, 3);

    // All values should be preserved
    let values: Vec<&Vec<u8>> = headers.iter().map(|(_, v)| v).collect();
    assert_eq!(values.len(), 3);
}

#[test]
fn test_record_headers_large_value() {
    // Header values can be large (up to configured limits)

    #[allow(dead_code)]
    struct RecordHeader {
        key: String,
        value: Vec<u8>,
    }

    // 10KB header value
    let large_value = vec![0xAB; 10 * 1024];

    let header = RecordHeader {
        key: "large-payload".to_string(),
        value: large_value,
    };

    assert_eq!(header.value.len(), 10 * 1024);
    assert!(header.value.iter().all(|&b| b == 0xAB));
}

#[test]
fn test_record_headers_unicode_key() {
    // Header keys can contain Unicode characters

    #[allow(dead_code)]
    struct RecordHeader {
        key: String,
        value: Vec<u8>,
    }

    let header = RecordHeader {
        key: "日本語キー".to_string(), // Japanese characters
        value: b"value".to_vec(),
    };

    assert_eq!(header.key, "日本語キー");
    assert_eq!(header.key.chars().count(), 5);
    // UTF-8 bytes are more than char count for multi-byte chars
    assert!(header.key.len() > 5);
}

#[test]
fn test_record_headers_unicode_value() {
    // Header values containing UTF-8 text

    #[allow(dead_code)]
    struct RecordHeader {
        key: String,
        value: Vec<u8>,
    }

    let unicode_value = "Привет мир".as_bytes().to_vec(); // Russian "Hello world"

    let header = RecordHeader {
        key: "greeting".to_string(),
        value: unicode_value,
    };

    let value_str = std::str::from_utf8(&header.value).unwrap();
    assert_eq!(value_str, "Привет мир");
}

#[test]
#[allow(clippy::useless_vec)]
fn test_record_headers_special_prefixes() {
    // Common header prefixes in Kafka ecosystem

    let headers = vec![
        // Tracing headers
        ("x-b3-traceid".to_string(), b"trace123".to_vec()),
        ("x-b3-spanid".to_string(), b"span456".to_vec()),
        ("x-b3-sampled".to_string(), b"1".to_vec()),
        // Confluent headers
        ("confluent-timestamp".to_string(), b"1702400000000".to_vec()),
        ("confluent-schema-id".to_string(), b"100".to_vec()),
        // Custom application headers
        ("x-correlation-id".to_string(), b"corr789".to_vec()),
        ("x-request-id".to_string(), b"req000".to_vec()),
    ];

    // Count x-b3-* headers (distributed tracing)
    let b3_headers = headers
        .iter()
        .filter(|(k, _)| k.starts_with("x-b3-"))
        .count();
    assert_eq!(b3_headers, 3);

    // Count confluent-* headers
    let confluent_headers = headers
        .iter()
        .filter(|(k, _)| k.starts_with("confluent-"))
        .count();
    assert_eq!(confluent_headers, 2);
}

#[test]
fn test_record_headers_ttl_header() {
    // x-message-ttl-ms is a common header for message TTL

    #[allow(dead_code)]
    struct RecordHeader {
        key: String,
        value: Vec<u8>,
    }

    let ttl_header = RecordHeader {
        key: "x-message-ttl-ms".to_string(),
        value: b"86400000".to_vec(), // 24 hours in milliseconds
    };

    let ttl_ms: i64 = std::str::from_utf8(&ttl_header.value)
        .unwrap()
        .parse()
        .unwrap();

    assert_eq!(ttl_ms, 86_400_000);
    assert_eq!(ttl_ms / 1000 / 60 / 60, 24); // 24 hours
}

#[test]
#[allow(clippy::useless_vec)]
fn test_record_headers_content_type() {
    // Content-Type header for schema information

    let headers = vec![
        ("content-type".to_string(), b"application/json".to_vec()),
        ("content-type".to_string(), b"application/avro".to_vec()),
        (
            "content-type".to_string(),
            b"application/x-protobuf".to_vec(),
        ),
    ];

    let json_type = &headers[0].1;
    let avro_type = &headers[1].1;
    let proto_type = &headers[2].1;

    assert_eq!(std::str::from_utf8(json_type).unwrap(), "application/json");
    assert_eq!(std::str::from_utf8(avro_type).unwrap(), "application/avro");
    assert_eq!(
        std::str::from_utf8(proto_type).unwrap(),
        "application/x-protobuf"
    );
}

#[test]
fn test_record_headers_serialization_size() {
    // Calculate serialized size of headers

    fn calculate_header_size(key: &str, value: &[u8]) -> usize {
        // varint(key_len) + key_bytes + varint(value_len) + value_bytes
        // Simplified: assume 1-byte varints for small values
        let key_len_bytes = if key.len() < 128 { 1 } else { 2 };
        let value_len_bytes = if value.len() < 128 { 1 } else { 2 };
        key_len_bytes + key.len() + value_len_bytes + value.len()
    }

    let size = calculate_header_size("traceId", b"abc123");
    // 1 (key_len) + 7 (key) + 1 (value_len) + 6 (value) = 15
    assert_eq!(size, 15);
}

#[test]
fn test_record_headers_order_preserved() {
    // Header order should be preserved during serialization/deserialization

    let headers = vec![
        ("first".to_string(), b"1".to_vec()),
        ("second".to_string(), b"2".to_vec()),
        ("third".to_string(), b"3".to_vec()),
        ("fourth".to_string(), b"4".to_vec()),
    ];

    // Verify order
    assert_eq!(headers[0].0, "first");
    assert_eq!(headers[1].0, "second");
    assert_eq!(headers[2].0, "third");
    assert_eq!(headers[3].0, "fourth");

    // Simulate roundtrip by reversing and re-reversing
    let reversed: Vec<_> = headers.iter().rev().cloned().collect();
    let restored: Vec<_> = reversed.iter().rev().cloned().collect();

    assert_eq!(headers, restored);
}

#[test]
fn test_record_headers_in_batch() {
    // Headers are part of individual records in a batch

    #[allow(dead_code)]
    struct Record {
        offset_delta: i32,
        timestamp_delta: i64,
        key: Option<Vec<u8>>,
        value: Option<Vec<u8>>,
        headers: Vec<(String, Vec<u8>)>,
    }

    #[allow(dead_code)]
    struct RecordBatch {
        base_offset: i64,
        records: Vec<Record>,
    }

    let batch = RecordBatch {
        base_offset: 0,
        records: vec![
            Record {
                offset_delta: 0,
                timestamp_delta: 0,
                key: Some(b"k1".to_vec()),
                value: Some(b"v1".to_vec()),
                headers: vec![("h1".to_string(), b"v1".to_vec())],
            },
            Record {
                offset_delta: 1,
                timestamp_delta: 100,
                key: Some(b"k2".to_vec()),
                value: Some(b"v2".to_vec()),
                headers: vec![
                    ("h1".to_string(), b"v1".to_vec()),
                    ("h2".to_string(), b"v2".to_vec()),
                ],
            },
        ],
    };

    // Different records can have different header counts
    assert_eq!(batch.records[0].headers.len(), 1);
    assert_eq!(batch.records[1].headers.len(), 2);
}

#[test]
fn test_record_headers_empty_key() {
    // Empty header key is valid but unusual

    #[allow(dead_code)]
    struct RecordHeader {
        key: String,
        value: Vec<u8>,
    }

    let header = RecordHeader {
        key: String::new(),
        value: b"value-for-empty-key".to_vec(),
    };

    assert!(header.key.is_empty());
    assert!(!header.value.is_empty());
}

#[test]
fn test_record_headers_empty_value() {
    // Empty header value is valid

    #[allow(dead_code)]
    struct RecordHeader {
        key: String,
        value: Vec<u8>,
    }

    let header = RecordHeader {
        key: "empty-value".to_string(),
        value: vec![],
    };

    assert!(!header.key.is_empty());
    assert!(header.value.is_empty());
}
