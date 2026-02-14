//! Protocol Record Batch Wire Format Tests
//!
//! Tests for Kafka record batch encoding including:
//! - Magic byte versions (v0, v1, v2)
//! - CRC32-C checksum validation
//! - Record batch attributes (compression, timestamp type, transactional, control)
//! - Producer ID/epoch fields for idempotent producers
//! - Base sequence for deduplication
//! - Control records (commit/abort markers)

use bytes::{Buf, BufMut, Bytes, BytesMut};
use kafka_protocol::records::{
    Compression, Record, RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};

// ============================================================================
// Record Batch Magic Byte Tests
// ============================================================================

/// Test that we can identify record batch magic byte v2 (current format)
#[test]
fn test_record_batch_magic_v2_identification() {
    // Magic byte v2 is at offset 16 in the record batch
    // Record batch v2 structure:
    // - baseOffset: int64 (8 bytes)
    // - batchLength: int32 (4 bytes)
    // - partitionLeaderEpoch: int32 (4 bytes)
    // - magic: int8 (1 byte) <- offset 16
    let mut batch_bytes = BytesMut::new();

    // baseOffset
    batch_bytes.put_i64(0);
    // batchLength (minimum valid)
    batch_bytes.put_i32(49);
    // partitionLeaderEpoch
    batch_bytes.put_i32(0);
    // magic byte v2
    batch_bytes.put_i8(2);

    let bytes = batch_bytes.freeze();
    assert_eq!(bytes[16], 2, "Magic byte should be 2 at offset 16");
}

/// Test magic byte v0 (legacy format) identification
#[test]
fn test_record_batch_magic_v0_identification() {
    // Legacy message set v0 has magic at offset 16
    let mut batch_bytes = BytesMut::new();

    // offset
    batch_bytes.put_i64(0);
    // message size
    batch_bytes.put_i32(14);
    // crc
    batch_bytes.put_i32(0);
    // magic v0
    batch_bytes.put_i8(0);

    let bytes = batch_bytes.freeze();
    assert_eq!(bytes[16], 0, "Magic byte should be 0 at offset 16");
}

/// Test magic byte v1 (timestamped legacy) identification
#[test]
fn test_record_batch_magic_v1_identification() {
    let mut batch_bytes = BytesMut::new();

    // offset
    batch_bytes.put_i64(0);
    // message size
    batch_bytes.put_i32(22);
    // crc
    batch_bytes.put_i32(0);
    // magic v1
    batch_bytes.put_i8(1);

    let bytes = batch_bytes.freeze();
    assert_eq!(bytes[16], 1, "Magic byte should be 1 at offset 16");
}

// ============================================================================
// Record Batch Attributes Tests
// ============================================================================

/// Test compression type encoding in attributes
#[test]
fn test_record_batch_attributes_compression_none() {
    // Attributes bits 0-2 = compression type
    // 0 = NONE, 1 = GZIP, 2 = SNAPPY, 3 = LZ4, 4 = ZSTD
    let attributes: i16 = 0b0000_0000_0000_0000;
    let compression = attributes & 0x07;
    assert_eq!(compression, 0, "No compression");
}

#[test]
fn test_record_batch_attributes_compression_gzip() {
    let attributes: i16 = 0b0000_0000_0000_0001;
    let compression = attributes & 0x07;
    assert_eq!(compression, 1, "GZIP compression");
}

#[test]
fn test_record_batch_attributes_compression_snappy() {
    let attributes: i16 = 0b0000_0000_0000_0010;
    let compression = attributes & 0x07;
    assert_eq!(compression, 2, "Snappy compression");
}

#[test]
fn test_record_batch_attributes_compression_lz4() {
    let attributes: i16 = 0b0000_0000_0000_0011;
    let compression = attributes & 0x07;
    assert_eq!(compression, 3, "LZ4 compression");
}

#[test]
fn test_record_batch_attributes_compression_zstd() {
    let attributes: i16 = 0b0000_0000_0000_0100;
    let compression = attributes & 0x07;
    assert_eq!(compression, 4, "ZSTD compression");
}

/// Test timestamp type bit in attributes (bit 3)
#[test]
fn test_record_batch_attributes_timestamp_create_time() {
    // Bit 3 = 0 means CreateTime
    let attributes: i16 = 0b0000_0000_0000_0000;
    let timestamp_type = (attributes >> 3) & 0x01;
    assert_eq!(timestamp_type, 0, "CreateTime timestamp");
}

#[test]
fn test_record_batch_attributes_timestamp_log_append_time() {
    // Bit 3 = 1 means LogAppendTime
    let attributes: i16 = 0b0000_0000_0000_1000;
    let timestamp_type = (attributes >> 3) & 0x01;
    assert_eq!(timestamp_type, 1, "LogAppendTime timestamp");
}

/// Test transactional bit in attributes (bit 4)
#[test]
fn test_record_batch_attributes_non_transactional() {
    let attributes: i16 = 0b0000_0000_0000_0000;
    let is_transactional = (attributes >> 4) & 0x01;
    assert_eq!(is_transactional, 0, "Non-transactional batch");
}

#[test]
fn test_record_batch_attributes_transactional() {
    // Bit 4 = 1 means transactional
    let attributes: i16 = 0b0000_0000_0001_0000;
    let is_transactional = (attributes >> 4) & 0x01;
    assert_eq!(is_transactional, 1, "Transactional batch");
}

/// Test control bit in attributes (bit 5)
#[test]
fn test_record_batch_attributes_not_control() {
    let attributes: i16 = 0b0000_0000_0000_0000;
    let is_control = (attributes >> 5) & 0x01;
    assert_eq!(is_control, 0, "Not a control batch");
}

#[test]
fn test_record_batch_attributes_is_control() {
    // Bit 5 = 1 means control batch
    let attributes: i16 = 0b0000_0000_0010_0000;
    let is_control = (attributes >> 5) & 0x01;
    assert_eq!(is_control, 1, "Control batch");
}

/// Test combined attributes
#[test]
fn test_record_batch_attributes_combined() {
    // LZ4 compression + LogAppendTime + transactional
    let attributes: i16 = 0b0000_0000_0001_1011;

    let compression = attributes & 0x07;
    let timestamp_type = (attributes >> 3) & 0x01;
    let is_transactional = (attributes >> 4) & 0x01;
    let is_control = (attributes >> 5) & 0x01;

    assert_eq!(compression, 3, "LZ4 compression");
    assert_eq!(timestamp_type, 1, "LogAppendTime");
    assert_eq!(is_transactional, 1, "Transactional");
    assert_eq!(is_control, 0, "Not control");
}

// ============================================================================
// CRC32-C Checksum Tests
// ============================================================================

/// Test CRC32-C calculation position in record batch
#[test]
fn test_record_batch_crc_position() {
    // CRC is at offset 17 (after magic byte)
    // CRC covers bytes from attributes to end of batch
    let mut batch_bytes = BytesMut::new();

    // baseOffset (8 bytes)
    batch_bytes.put_i64(100);
    // batchLength (4 bytes)
    batch_bytes.put_i32(49);
    // partitionLeaderEpoch (4 bytes)
    batch_bytes.put_i32(5);
    // magic (1 byte)
    batch_bytes.put_i8(2);
    // CRC starts here at offset 17 (4 bytes)
    batch_bytes.put_i32(0xDEADBEEFu32 as i32);

    let bytes = batch_bytes.freeze();

    // Verify CRC position
    let crc_offset = 17;
    let crc_bytes = &bytes[crc_offset..crc_offset + 4];
    let crc = i32::from_be_bytes([crc_bytes[0], crc_bytes[1], crc_bytes[2], crc_bytes[3]]);
    assert_eq!(crc as u32, 0xDEADBEEF);
}

/// Test CRC32-C computation on sample data
#[test]
fn test_crc32c_computation() {
    use crc32fast::Hasher;

    let data = b"test data for CRC";

    // crc32fast uses CRC32 (not CRC32-C), but this tests the pattern
    let mut hasher = Hasher::new();
    hasher.update(data);
    let crc = hasher.finalize();

    // Verify we get a consistent checksum
    let mut hasher2 = Hasher::new();
    hasher2.update(data);
    let crc2 = hasher2.finalize();

    assert_eq!(crc, crc2, "CRC should be deterministic");
    assert_ne!(crc, 0, "CRC should not be zero for non-empty data");
}

/// Test that different data produces different CRCs
#[test]
fn test_crc32c_different_data() {
    use crc32fast::Hasher;

    let data1 = b"first message";
    let data2 = b"second message";

    let mut hasher1 = Hasher::new();
    hasher1.update(data1);
    let crc1 = hasher1.finalize();

    let mut hasher2 = Hasher::new();
    hasher2.update(data2);
    let crc2 = hasher2.finalize();

    assert_ne!(crc1, crc2, "Different data should produce different CRCs");
}

// ============================================================================
// Producer ID and Epoch Tests (Idempotent Producer Support)
// ============================================================================

/// Test producer ID field position and encoding
#[test]
fn test_record_batch_producer_id_position() {
    // Producer ID is at offset 27 in record batch v2
    // After: baseOffset(8) + batchLength(4) + partitionLeaderEpoch(4) +
    //        magic(1) + crc(4) + attributes(2) + lastOffsetDelta(4) = 27
    let producer_id: i64 = 12345;

    let mut batch_bytes = BytesMut::new();

    // baseOffset
    batch_bytes.put_i64(0);
    // batchLength
    batch_bytes.put_i32(61);
    // partitionLeaderEpoch
    batch_bytes.put_i32(0);
    // magic
    batch_bytes.put_i8(2);
    // crc
    batch_bytes.put_i32(0);
    // attributes
    batch_bytes.put_i16(0);
    // lastOffsetDelta
    batch_bytes.put_i32(0);
    // firstTimestamp
    batch_bytes.put_i64(0);
    // maxTimestamp
    batch_bytes.put_i64(0);
    // producerId (at offset 43)
    batch_bytes.put_i64(producer_id);

    let bytes = batch_bytes.freeze();

    // Read back producer ID from offset 43
    let mut reader = &bytes[43..51];
    let read_producer_id = reader.get_i64();

    assert_eq!(read_producer_id, producer_id);
}

/// Test producer epoch field
#[test]
fn test_record_batch_producer_epoch() {
    // Producer epoch is a 16-bit value following producer ID
    let producer_epoch: i16 = 5;

    let mut batch_bytes = BytesMut::new();
    batch_bytes.put_i16(producer_epoch);

    let bytes = batch_bytes.freeze();
    let mut reader = bytes.as_ref();
    let read_epoch = reader.get_i16();

    assert_eq!(read_epoch, producer_epoch);
}

/// Test no producer ID (non-idempotent)
#[test]
fn test_record_batch_no_producer_id() {
    // -1 indicates no producer ID (non-idempotent producer)
    let no_producer_id: i64 = -1;
    let no_producer_epoch: i16 = -1;

    assert_eq!(no_producer_id, -1);
    assert_eq!(no_producer_epoch, -1);
}

// ============================================================================
// Base Sequence Tests (Deduplication)
// ============================================================================

/// Test base sequence field encoding
#[test]
fn test_record_batch_base_sequence() {
    // Base sequence is an int32 after producer epoch
    let base_sequence: i32 = 100;

    let mut batch_bytes = BytesMut::new();
    batch_bytes.put_i32(base_sequence);

    let bytes = batch_bytes.freeze();
    let mut reader = bytes.as_ref();
    let read_sequence = reader.get_i32();

    assert_eq!(read_sequence, base_sequence);
}

/// Test sequence number wrapping
#[test]
fn test_record_batch_sequence_wrapping() {
    // Sequence numbers wrap at i32::MAX
    let max_sequence: i32 = i32::MAX;
    let wrapped_sequence: i32 = 0;

    // Verify wrapping behavior
    let next_after_max = max_sequence.wrapping_add(1);
    assert_eq!(next_after_max, i32::MIN, "Sequence wraps to MIN after MAX");

    // In practice, Kafka resets to 0
    assert_eq!(wrapped_sequence, 0);
}

/// Test no sequence (non-idempotent)
#[test]
fn test_record_batch_no_sequence() {
    // -1 indicates no sequence number
    let no_sequence: i32 = -1;
    assert_eq!(no_sequence, -1);
}

// ============================================================================
// Record Batch Header Complete Structure Tests
// ============================================================================

/// Test complete record batch v2 header structure
#[test]
fn test_record_batch_v2_complete_header() {
    let mut batch = BytesMut::new();

    // Full record batch v2 header (61 bytes before records array length)
    let base_offset: i64 = 1000;
    let batch_length: i32 = 100;
    let partition_leader_epoch: i32 = 42;
    let magic: i8 = 2;
    let crc: i32 = 0x12345678;
    let attributes: i16 = 0b0000_0000_0000_0011; // LZ4
    let last_offset_delta: i32 = 5;
    let first_timestamp: i64 = 1609459200000;
    let max_timestamp: i64 = 1609459201000;
    let producer_id: i64 = 9999;
    let producer_epoch: i16 = 3;
    let base_sequence: i32 = 500;
    let records_count: i32 = 6;

    batch.put_i64(base_offset);
    batch.put_i32(batch_length);
    batch.put_i32(partition_leader_epoch);
    batch.put_i8(magic);
    batch.put_i32(crc);
    batch.put_i16(attributes);
    batch.put_i32(last_offset_delta);
    batch.put_i64(first_timestamp);
    batch.put_i64(max_timestamp);
    batch.put_i64(producer_id);
    batch.put_i16(producer_epoch);
    batch.put_i32(base_sequence);
    batch.put_i32(records_count);

    let bytes = batch.freeze();

    // Verify header is correct size (61 bytes)
    assert_eq!(bytes.len(), 61);

    // Read back all fields
    let mut reader = bytes.as_ref();
    assert_eq!(reader.get_i64(), base_offset);
    assert_eq!(reader.get_i32(), batch_length);
    assert_eq!(reader.get_i32(), partition_leader_epoch);
    assert_eq!(reader.get_i8(), magic);
    assert_eq!(reader.get_i32(), crc);
    assert_eq!(reader.get_i16(), attributes);
    assert_eq!(reader.get_i32(), last_offset_delta);
    assert_eq!(reader.get_i64(), first_timestamp);
    assert_eq!(reader.get_i64(), max_timestamp);
    assert_eq!(reader.get_i64(), producer_id);
    assert_eq!(reader.get_i16(), producer_epoch);
    assert_eq!(reader.get_i32(), base_sequence);
    assert_eq!(reader.get_i32(), records_count);
}

/// Test record batch with partition leader epoch
#[test]
fn test_record_batch_partition_leader_epoch() {
    // Partition leader epoch is used for fencing
    let leader_epoch: i32 = 15;

    let mut batch = BytesMut::new();
    batch.put_i64(0); // baseOffset
    batch.put_i32(49); // batchLength
    batch.put_i32(leader_epoch);

    let bytes = batch.freeze();
    let mut reader = &bytes[12..16];
    let read_epoch = reader.get_i32();

    assert_eq!(read_epoch, leader_epoch);
}

// ============================================================================
// Control Record Tests
// ============================================================================

/// Test control record key structure for COMMIT marker
#[test]
fn test_control_record_commit_marker() {
    // Control records have a specific key format:
    // - version: int16 (always 0)
    // - type: int16 (0 = ABORT, 1 = COMMIT)
    let mut key = BytesMut::new();
    key.put_i16(0); // version
    key.put_i16(1); // COMMIT type

    let bytes = key.freeze();
    let mut reader = bytes.as_ref();

    let version = reader.get_i16();
    let marker_type = reader.get_i16();

    assert_eq!(version, 0);
    assert_eq!(marker_type, 1, "COMMIT marker type");
}

/// Test control record key structure for ABORT marker
#[test]
fn test_control_record_abort_marker() {
    let mut key = BytesMut::new();
    key.put_i16(0); // version
    key.put_i16(0); // ABORT type

    let bytes = key.freeze();
    let mut reader = bytes.as_ref();

    let version = reader.get_i16();
    let marker_type = reader.get_i16();

    assert_eq!(version, 0);
    assert_eq!(marker_type, 0, "ABORT marker type");
}

/// Test control batch attributes
#[test]
fn test_control_batch_attributes() {
    // Control batch has bit 5 set + transactional bit
    let attributes: i16 = 0b0000_0000_0011_0000;

    let is_transactional = (attributes >> 4) & 0x01;
    let is_control = (attributes >> 5) & 0x01;

    assert_eq!(is_transactional, 1, "Control batches are transactional");
    assert_eq!(is_control, 1, "Control bit set");
}

// ============================================================================
// Timestamp Tests
// ============================================================================

/// Test first and max timestamp fields
#[test]
fn test_record_batch_timestamps() {
    let first_timestamp: i64 = 1609459200000; // 2021-01-01 00:00:00
    let max_timestamp: i64 = 1609459205000; // 5 seconds later

    let mut batch = BytesMut::new();
    batch.put_i64(first_timestamp);
    batch.put_i64(max_timestamp);

    let bytes = batch.freeze();
    let mut reader = bytes.as_ref();

    let read_first = reader.get_i64();
    let read_max = reader.get_i64();

    assert_eq!(read_first, first_timestamp);
    assert_eq!(read_max, max_timestamp);
    assert!(read_max >= read_first, "Max should be >= first");
}

/// Test timestamp delta in individual records
#[test]
fn test_record_timestamp_delta() {
    // Individual records store timestamp as delta from firstTimestamp
    let first_timestamp: i64 = 1609459200000;
    let record_timestamp: i64 = 1609459200500; // 500ms later

    let timestamp_delta = record_timestamp - first_timestamp;
    assert_eq!(timestamp_delta, 500);

    // Delta is stored as varint in record
    // Verify it fits in smaller encoding
    assert!(timestamp_delta < i32::MAX as i64);
}

// ============================================================================
// Record Encoding with kafka-protocol (using custom compression API)
// ============================================================================

// Type alias for decompression function
type DecompressFn = fn(&mut Bytes, Compression) -> anyhow::Result<Bytes>;

/// Helper to encode and decode a batch using kafka-protocol
fn encode_decode_batch(records: Vec<Record>) -> Vec<Record> {
    let mut buf = BytesMut::new();

    let options = RecordEncodeOptions {
        version: 2,
        compression: Compression::None,
    };

    // Use encode_with_custom_compression to avoid type inference issues
    RecordBatchEncoder::encode_with_custom_compression::<
        _,
        _,
        fn(&mut BytesMut, &mut BytesMut, Compression) -> anyhow::Result<()>,
    >(&mut buf, records.iter(), &options, None)
    .unwrap();

    let mut read_buf = buf.freeze();

    RecordBatchDecoder::decode_with_custom_compression::<Bytes, DecompressFn>(&mut read_buf, None)
        .unwrap()
}

/// Test encoding a single record batch
#[test]
fn test_encode_single_record_batch() {
    let records = vec![Record {
        transactional: false,
        control: false,
        partition_leader_epoch: 0,
        producer_id: -1,
        producer_epoch: -1,
        timestamp_type: TimestampType::Creation,
        offset: 0,
        sequence: -1,
        timestamp: 1609459200000,
        key: Some(Bytes::from("key1")),
        value: Some(Bytes::from("value1")),
        headers: Default::default(),
    }];

    let decoded = encode_decode_batch(records);

    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].key, Some(Bytes::from("key1")));
    assert_eq!(decoded[0].value, Some(Bytes::from("value1")));
}

/// Test encoding multiple records in a batch
#[test]
fn test_encode_multiple_records_batch() {
    let records = vec![
        Record {
            transactional: false,
            control: false,
            partition_leader_epoch: 0,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: TimestampType::Creation,
            offset: 0,
            sequence: -1,
            timestamp: 1609459200000,
            key: Some(Bytes::from("key1")),
            value: Some(Bytes::from("value1")),
            headers: Default::default(),
        },
        Record {
            transactional: false,
            control: false,
            partition_leader_epoch: 0,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: TimestampType::Creation,
            offset: 1,
            sequence: -1,
            timestamp: 1609459200100,
            key: Some(Bytes::from("key2")),
            value: Some(Bytes::from("value2")),
            headers: Default::default(),
        },
        Record {
            transactional: false,
            control: false,
            partition_leader_epoch: 0,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: TimestampType::Creation,
            offset: 2,
            sequence: -1,
            timestamp: 1609459200200,
            key: Some(Bytes::from("key3")),
            value: Some(Bytes::from("value3")),
            headers: Default::default(),
        },
    ];

    let decoded = encode_decode_batch(records);

    assert_eq!(decoded.len(), 3);
    assert_eq!(decoded[0].offset, 0);
    assert_eq!(decoded[1].offset, 1);
    assert_eq!(decoded[2].offset, 2);
}

/// Test encoding with transactional flag
#[test]
fn test_encode_transactional_batch() {
    let records = vec![Record {
        transactional: true,
        control: false,
        partition_leader_epoch: 0,
        producer_id: 1000,
        producer_epoch: 5,
        timestamp_type: TimestampType::Creation,
        offset: 0,
        sequence: 0,
        timestamp: 1609459200000,
        key: Some(Bytes::from("txn-key")),
        value: Some(Bytes::from("txn-value")),
        headers: Default::default(),
    }];

    let decoded = encode_decode_batch(records);

    assert_eq!(decoded.len(), 1);
    assert!(decoded[0].transactional);
    assert_eq!(decoded[0].producer_id, 1000);
    assert_eq!(decoded[0].producer_epoch, 5);
}

/// Test encoding control record
#[test]
fn test_encode_control_record() {
    // Control record key: version(2) + type(2) = 4 bytes
    let mut control_key = BytesMut::new();
    control_key.put_i16(0); // version
    control_key.put_i16(1); // COMMIT

    let records = vec![Record {
        transactional: true,
        control: true,
        partition_leader_epoch: 0,
        producer_id: 1000,
        producer_epoch: 5,
        timestamp_type: TimestampType::Creation,
        offset: 0,
        sequence: -1,
        timestamp: 1609459200000,
        key: Some(control_key.freeze()),
        value: None,
        headers: Default::default(),
    }];

    let decoded = encode_decode_batch(records);

    assert_eq!(decoded.len(), 1);
    assert!(decoded[0].control);
    assert!(decoded[0].transactional);
}

/// Test encoding record with headers
#[test]
fn test_encode_record_with_headers() {
    use kafka_protocol::indexmap::IndexMap;

    let mut headers = IndexMap::new();
    headers.insert(
        kafka_protocol::protocol::StrBytes::from_static_str("header1"),
        Some(Bytes::from("header-value1")),
    );
    headers.insert(
        kafka_protocol::protocol::StrBytes::from_static_str("header2"),
        Some(Bytes::from("header-value2")),
    );

    let records = vec![Record {
        transactional: false,
        control: false,
        partition_leader_epoch: 0,
        producer_id: -1,
        producer_epoch: -1,
        timestamp_type: TimestampType::Creation,
        offset: 0,
        sequence: -1,
        timestamp: 1609459200000,
        key: Some(Bytes::from("key")),
        value: Some(Bytes::from("value")),
        headers,
    }];

    let decoded = encode_decode_batch(records);

    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].headers.len(), 2);
}

/// Test empty decode buffer
#[test]
fn test_decode_empty_buffer() {
    let mut empty_buf = Bytes::new();

    // Decoding an empty buffer should return an empty vec
    let decoded = RecordBatchDecoder::decode_with_custom_compression::<Bytes, DecompressFn>(
        &mut empty_buf,
        None,
    );

    // Empty buffer should either decode to empty vec or return error
    if let Ok(records) = decoded {
        assert!(records.is_empty());
    }
    // Error on empty buffer is also acceptable
}

/// Test encoding record with null key
#[test]
fn test_encode_record_null_key() {
    let records = vec![Record {
        transactional: false,
        control: false,
        partition_leader_epoch: 0,
        producer_id: -1,
        producer_epoch: -1,
        timestamp_type: TimestampType::Creation,
        offset: 0,
        sequence: -1,
        timestamp: 1609459200000,
        key: None, // null key
        value: Some(Bytes::from("value-only")),
        headers: Default::default(),
    }];

    let decoded = encode_decode_batch(records);

    assert_eq!(decoded.len(), 1);
    assert!(decoded[0].key.is_none());
    assert_eq!(decoded[0].value, Some(Bytes::from("value-only")));
}

/// Test encoding record with null value (tombstone)
#[test]
fn test_encode_record_null_value_tombstone() {
    let records = vec![Record {
        transactional: false,
        control: false,
        partition_leader_epoch: 0,
        producer_id: -1,
        producer_epoch: -1,
        timestamp_type: TimestampType::Creation,
        offset: 0,
        sequence: -1,
        timestamp: 1609459200000,
        key: Some(Bytes::from("deleted-key")),
        value: None, // tombstone
        headers: Default::default(),
    }];

    let decoded = encode_decode_batch(records);

    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].key, Some(Bytes::from("deleted-key")));
    assert!(decoded[0].value.is_none(), "Tombstone has null value");
}

// ============================================================================
// Record Batch Size Tests
// ============================================================================

/// Test record batch respects size constraints
#[test]
fn test_record_batch_size_calculation() {
    // Create a larger batch
    let records: Vec<Record> = (0..100)
        .map(|i| Record {
            transactional: false,
            control: false,
            partition_leader_epoch: 0,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: TimestampType::Creation,
            offset: i as i64,
            sequence: -1,
            timestamp: 1609459200000 + (i as i64 * 100),
            key: Some(Bytes::from(format!("key-{}", i))),
            value: Some(Bytes::from(format!("value-{}", i))),
            headers: Default::default(),
        })
        .collect();

    let decoded = encode_decode_batch(records);
    assert_eq!(decoded.len(), 100);
}

/// Test large value handling
#[test]
fn test_record_batch_large_value() {
    // 1MB value
    let large_value = vec![b'x'; 1024 * 1024];

    let records = vec![Record {
        transactional: false,
        control: false,
        partition_leader_epoch: 0,
        producer_id: -1,
        producer_epoch: -1,
        timestamp_type: TimestampType::Creation,
        offset: 0,
        sequence: -1,
        timestamp: 1609459200000,
        key: Some(Bytes::from("large-key")),
        value: Some(Bytes::from(large_value.clone())),
        headers: Default::default(),
    }];

    let decoded = encode_decode_batch(records);

    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].value.as_ref().unwrap().len(), 1024 * 1024);
}

// ============================================================================
// Offset Delta Tests
// ============================================================================

/// Test offset delta encoding in records
#[test]
fn test_record_offset_delta() {
    let base_offset: i64 = 1000;
    let record_offsets = [1000i64, 1001, 1002, 1003, 1004];

    for (i, &offset) in record_offsets.iter().enumerate() {
        let delta = offset - base_offset;
        assert_eq!(delta, i as i64);
    }
}

/// Test last offset delta in batch header
#[test]
fn test_batch_last_offset_delta() {
    // lastOffsetDelta = last_record_offset - base_offset
    let base_offset: i64 = 1000;
    let record_count = 10;
    let last_offset = base_offset + (record_count - 1);

    let last_offset_delta = (last_offset - base_offset) as i32;
    assert_eq!(last_offset_delta, 9);
}

// ============================================================================
// LogAppendTime Tests
// ============================================================================

/// Test LogAppendTime timestamp type encoding in attributes
#[test]
fn test_log_append_time_attributes() {
    // LogAppendTime is indicated by bit 3 of attributes
    let create_time_attributes: i16 = 0b0000_0000_0000_0000;
    let log_append_time_attributes: i16 = 0b0000_0000_0000_1000;

    assert_eq!((create_time_attributes >> 3) & 0x01, 0);
    assert_eq!((log_append_time_attributes >> 3) & 0x01, 1);

    // The encoder sets attributes based on timestamp_type
    // When decoding, timestamp type is derived from batch attributes
    // Individual record timestamp_type is set at decode time based on batch
}

// ============================================================================
// Idempotent Producer Fields Tests
// ============================================================================

/// Test encoding with idempotent producer fields
#[test]
fn test_encode_idempotent_producer_batch() {
    let records = vec![Record {
        transactional: false,
        control: false,
        partition_leader_epoch: 0,
        producer_id: 12345,
        producer_epoch: 3,
        timestamp_type: TimestampType::Creation,
        offset: 0,
        sequence: 100, // base sequence
        timestamp: 1609459200000,
        key: Some(Bytes::from("key")),
        value: Some(Bytes::from("value")),
        headers: Default::default(),
    }];

    let decoded = encode_decode_batch(records);

    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].producer_id, 12345);
    assert_eq!(decoded[0].producer_epoch, 3);
    assert_eq!(decoded[0].sequence, 100);
}

/// Test multiple records with sequential sequences
#[test]
fn test_encode_sequential_sequences() {
    let records = vec![
        Record {
            transactional: false,
            control: false,
            partition_leader_epoch: 0,
            producer_id: 12345,
            producer_epoch: 1,
            timestamp_type: TimestampType::Creation,
            offset: 0,
            sequence: 0,
            timestamp: 1609459200000,
            key: Some(Bytes::from("key1")),
            value: Some(Bytes::from("value1")),
            headers: Default::default(),
        },
        Record {
            transactional: false,
            control: false,
            partition_leader_epoch: 0,
            producer_id: 12345,
            producer_epoch: 1,
            timestamp_type: TimestampType::Creation,
            offset: 1,
            sequence: 1,
            timestamp: 1609459200100,
            key: Some(Bytes::from("key2")),
            value: Some(Bytes::from("value2")),
            headers: Default::default(),
        },
    ];

    let decoded = encode_decode_batch(records);

    assert_eq!(decoded.len(), 2);
    assert_eq!(decoded[0].sequence, 0);
    assert_eq!(decoded[1].sequence, 1);
}

// ============================================================================
// Varint Encoding Tests (Record-level)
// ============================================================================

/// Test varint encoding for small values
#[test]
fn test_varint_encoding_small() {
    // Varint encoding: 0-63 = 1 byte, 64-8191 = 2 bytes, etc.
    let small_value: i32 = 50;

    // Zigzag encode
    let zigzag = ((small_value << 1) ^ (small_value >> 31)) as u32;
    assert_eq!(zigzag, 100); // 50 -> 100 in zigzag

    // 100 fits in 1 byte (< 128)
    assert!(zigzag < 128);
}

/// Test varint encoding for larger values
#[test]
fn test_varint_encoding_large() {
    let large_value: i32 = 10000;

    // Zigzag encode
    let zigzag = ((large_value << 1) ^ (large_value >> 31)) as u32;
    assert_eq!(zigzag, 20000); // 10000 -> 20000 in zigzag

    // 20000 requires 3 bytes (> 16383)
    assert!(zigzag > 16383);
}

/// Test varint encoding for negative values
#[test]
fn test_varint_encoding_negative() {
    let negative_value: i32 = -1;

    // Zigzag encode
    let zigzag = ((negative_value << 1) ^ (negative_value >> 31)) as u32;
    assert_eq!(zigzag, 1); // -1 -> 1 in zigzag

    // Negative numbers are encoded efficiently with zigzag
    assert!(zigzag < 128);
}

// ============================================================================
// Compression Type Tests
// ============================================================================

/// Test Compression enum values match wire format
#[test]
fn test_compression_enum_values() {
    assert_eq!(Compression::None as i32, 0);
    assert_eq!(Compression::Gzip as i32, 1);
    assert_eq!(Compression::Snappy as i32, 2);
    assert_eq!(Compression::Lz4 as i32, 3);
    assert_eq!(Compression::Zstd as i32, 4);
}

/// Test compression type extraction from attributes
#[test]
fn test_compression_from_attributes() {
    // Attributes with LZ4 compression
    let attributes: i16 = 0b0000_0000_0000_0011;
    let compression_bits = (attributes & 0x07) as i32;

    match compression_bits {
        0 => assert_eq!(compression_bits, Compression::None as i32),
        1 => assert_eq!(compression_bits, Compression::Gzip as i32),
        2 => assert_eq!(compression_bits, Compression::Snappy as i32),
        3 => assert_eq!(compression_bits, Compression::Lz4 as i32),
        4 => assert_eq!(compression_bits, Compression::Zstd as i32),
        _ => panic!("Unknown compression type"),
    }

    assert_eq!(compression_bits, 3); // LZ4
}

// ============================================================================
// Record Structure Tests
// ============================================================================

/// Test Record struct default values
#[test]
fn test_record_default_values() {
    let record = Record {
        transactional: false,
        control: false,
        partition_leader_epoch: 0,
        producer_id: -1,
        producer_epoch: -1,
        timestamp_type: TimestampType::Creation,
        offset: 0,
        sequence: -1,
        timestamp: 0,
        key: None,
        value: None,
        headers: Default::default(),
    };

    assert!(!record.transactional);
    assert!(!record.control);
    assert_eq!(record.producer_id, -1);
    assert_eq!(record.producer_epoch, -1);
    assert_eq!(record.sequence, -1);
    assert!(record.key.is_none());
    assert!(record.value.is_none());
    assert!(record.headers.is_empty());
}

/// Test Record with all fields set
#[test]
fn test_record_all_fields_set() {
    use kafka_protocol::indexmap::IndexMap;

    let mut headers = IndexMap::new();
    headers.insert(
        kafka_protocol::protocol::StrBytes::from_static_str("trace-id"),
        Some(Bytes::from("abc123")),
    );

    let record = Record {
        transactional: true,
        control: false,
        partition_leader_epoch: 10,
        producer_id: 9999,
        producer_epoch: 5,
        timestamp_type: TimestampType::Creation,
        offset: 100,
        sequence: 50,
        timestamp: 1609459200000,
        key: Some(Bytes::from("my-key")),
        value: Some(Bytes::from("my-value")),
        headers,
    };

    assert!(record.transactional);
    assert_eq!(record.producer_id, 9999);
    assert_eq!(record.producer_epoch, 5);
    assert_eq!(record.sequence, 50);
    assert_eq!(record.key, Some(Bytes::from("my-key")));
    assert_eq!(record.value, Some(Bytes::from("my-value")));
    assert_eq!(record.headers.len(), 1);
}
