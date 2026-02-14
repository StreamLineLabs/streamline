//! Idempotent Producer Wire Protocol Tests
//!
//! Tests for Kafka idempotent producer protocol including:
//! - InitProducerId request/response encoding
//! - Producer ID allocation
//! - Producer epoch management
//! - Sequence number validation
//! - Duplicate detection
//! - Producer fencing

use bytes::{Buf, BufMut, Bytes, BytesMut};
use kafka_protocol::messages::{
    InitProducerIdRequest, InitProducerIdResponse, ProducerId, RequestHeader, ResponseHeader,
    TransactionalId,
};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};
use kafka_protocol::records::{
    Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};

// ============================================================================
// InitProducerId Request Tests
// ============================================================================

/// Test InitProducerIdRequest v0 encoding
#[test]
fn test_init_producer_id_request_v0() {
    let mut request = InitProducerIdRequest::default();
    request.transactional_id = None; // Non-transactional
    request.transaction_timeout_ms = 60000;

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 0).unwrap();

    // Decode to verify round-trip
    let mut read_buf = buf.freeze();
    let decoded = InitProducerIdRequest::decode(&mut read_buf, 0).unwrap();

    assert!(decoded.transactional_id.is_none());
    assert_eq!(decoded.transaction_timeout_ms, 60000);
}

/// Test InitProducerIdRequest with transactional ID
#[test]
fn test_init_producer_id_request_transactional() {
    let mut request = InitProducerIdRequest::default();
    request.transactional_id = Some(TransactionalId(StrBytes::from_static_str("my-transaction")));
    request.transaction_timeout_ms = 30000;

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = InitProducerIdRequest::decode(&mut read_buf, 0).unwrap();

    assert_eq!(
        decoded.transactional_id,
        Some(TransactionalId(StrBytes::from_static_str("my-transaction")))
    );
    assert_eq!(decoded.transaction_timeout_ms, 30000);
}

/// Test InitProducerIdRequest v3 with producer ID/epoch for fencing
#[test]
fn test_init_producer_id_request_v3_fencing() {
    let mut request = InitProducerIdRequest::default();
    request.transactional_id = None;
    request.transaction_timeout_ms = 60000;
    request.producer_id = ProducerId(12345); // Existing producer ID
    request.producer_epoch = 5; // Current epoch

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 3).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = InitProducerIdRequest::decode(&mut read_buf, 3).unwrap();

    assert_eq!(decoded.producer_id, ProducerId(12345));
    assert_eq!(decoded.producer_epoch, 5);
}

/// Test InitProducerIdRequest v4 with flexible encoding
#[test]
fn test_init_producer_id_request_v4_flexible() {
    let mut request = InitProducerIdRequest::default();
    request.transactional_id = None;
    request.transaction_timeout_ms = 120000;
    request.producer_id = ProducerId(-1); // New producer
    request.producer_epoch = -1;

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 4).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = InitProducerIdRequest::decode(&mut read_buf, 4).unwrap();

    assert_eq!(decoded.producer_id, ProducerId(-1));
    assert_eq!(decoded.producer_epoch, -1);
    assert_eq!(decoded.transaction_timeout_ms, 120000);
}

// ============================================================================
// InitProducerId Response Tests
// ============================================================================

/// Test InitProducerIdResponse v0 encoding
#[test]
fn test_init_producer_id_response_v0() {
    let mut response = InitProducerIdResponse::default();
    response.error_code = 0; // No error
    response.producer_id = ProducerId(99999);
    response.producer_epoch = 0;

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = InitProducerIdResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.error_code, 0);
    assert_eq!(decoded.producer_id, ProducerId(99999));
    assert_eq!(decoded.producer_epoch, 0);
}

/// Test InitProducerIdResponse with throttle time
#[test]
fn test_init_producer_id_response_throttled() {
    let mut response = InitProducerIdResponse::default();
    response.throttle_time_ms = 1000;
    response.error_code = 0;
    response.producer_id = ProducerId(54321);
    response.producer_epoch = 1;

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = InitProducerIdResponse::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.throttle_time_ms, 1000);
    assert_eq!(decoded.producer_id, ProducerId(54321));
    assert_eq!(decoded.producer_epoch, 1);
}

/// Test InitProducerIdResponse error cases
#[test]
fn test_init_producer_id_response_errors() {
    // Test CLUSTER_AUTHORIZATION_FAILED (31)
    let mut response = InitProducerIdResponse::default();
    response.error_code = 31;
    response.producer_id = ProducerId(-1);
    response.producer_epoch = -1;

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = InitProducerIdResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.error_code, 31);
    assert_eq!(decoded.producer_id, ProducerId(-1));
    assert_eq!(decoded.producer_epoch, -1);
}

/// Test InitProducerIdResponse with bumped epoch
#[test]
fn test_init_producer_id_response_epoch_bump() {
    let mut response = InitProducerIdResponse::default();
    response.error_code = 0;
    response.producer_id = ProducerId(12345);
    response.producer_epoch = 6; // Bumped from 5

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 3).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = InitProducerIdResponse::decode(&mut read_buf, 3).unwrap();

    assert_eq!(decoded.producer_id, ProducerId(12345));
    assert_eq!(decoded.producer_epoch, 6);
}

// ============================================================================
// Producer ID Wire Format Tests
// ============================================================================

/// Test producer ID field encoding (8 bytes, big endian)
#[test]
fn test_producer_id_wire_format() {
    let producer_id: i64 = 0x123456789ABCDEF0;

    let mut buf = BytesMut::new();
    buf.put_i64(producer_id);

    let bytes = buf.freeze();
    assert_eq!(bytes.len(), 8);

    // Verify big-endian encoding
    assert_eq!(bytes[0], 0x12);
    assert_eq!(bytes[1], 0x34);
    assert_eq!(bytes[7], 0xF0);

    let mut reader = bytes.as_ref();
    let read_id = reader.get_i64();
    assert_eq!(read_id, producer_id);
}

/// Test producer epoch field encoding (2 bytes, big endian)
#[test]
fn test_producer_epoch_wire_format() {
    let producer_epoch: i16 = 0x1234;

    let mut buf = BytesMut::new();
    buf.put_i16(producer_epoch);

    let bytes = buf.freeze();
    assert_eq!(bytes.len(), 2);

    // Verify big-endian encoding
    assert_eq!(bytes[0], 0x12);
    assert_eq!(bytes[1], 0x34);

    let mut reader = bytes.as_ref();
    let read_epoch = reader.get_i16();
    assert_eq!(read_epoch, producer_epoch);
}

/// Test no producer ID sentinel value
#[test]
fn test_no_producer_id_sentinel() {
    // -1 indicates no producer ID
    let no_producer_id: i64 = -1;
    let no_producer_epoch: i16 = -1;

    let mut buf = BytesMut::new();
    buf.put_i64(no_producer_id);
    buf.put_i16(no_producer_epoch);

    let bytes = buf.freeze();

    let mut reader = bytes.as_ref();
    let read_id = reader.get_i64();
    let read_epoch = reader.get_i16();

    assert_eq!(read_id, -1);
    assert_eq!(read_epoch, -1);

    // -1 in two's complement
    assert_eq!(read_id as u64, u64::MAX);
    assert_eq!(read_epoch as u16, u16::MAX);
}

// ============================================================================
// Sequence Number Tests
// ============================================================================

/// Test base sequence field encoding (4 bytes)
#[test]
fn test_base_sequence_wire_format() {
    let base_sequence: i32 = 100;

    let mut buf = BytesMut::new();
    buf.put_i32(base_sequence);

    let bytes = buf.freeze();
    assert_eq!(bytes.len(), 4);

    let mut reader = bytes.as_ref();
    let read_seq = reader.get_i32();
    assert_eq!(read_seq, base_sequence);
}

/// Test sequence number range
#[test]
fn test_sequence_number_range() {
    // Sequence numbers are int32
    let min_seq: i32 = 0;
    let max_seq: i32 = i32::MAX;
    let no_seq: i32 = -1;

    assert_eq!(min_seq, 0);
    assert_eq!(max_seq, 2147483647);
    assert_eq!(no_seq, -1);
}

/// Test sequence wrapping behavior
#[test]
fn test_sequence_wrapping() {
    // When sequence reaches max, it should wrap
    let near_max: i32 = i32::MAX - 10;
    let wrapped = near_max.wrapping_add(20);

    // Should wrap around to negative
    assert!(wrapped < 0);

    // In practice, Kafka compares sequences with tolerance for wrapping
    let seq1: i32 = i32::MAX - 5;
    let seq2: i32 = seq1.wrapping_add(10);

    // seq2 is "after" seq1 even though numerically smaller
    let diff = seq2.wrapping_sub(seq1);
    assert_eq!(diff, 10);
}

/// Test consecutive sequence validation
#[test]
fn test_consecutive_sequence_validation() {
    // Valid: consecutive sequences
    let expected_seq: i32 = 100;
    let actual_seq: i32 = 100;

    assert_eq!(expected_seq, actual_seq);

    // Invalid: gap in sequences
    let expected_seq2: i32 = 101;
    let actual_seq2: i32 = 103;

    assert_ne!(expected_seq2, actual_seq2);
    assert_eq!(actual_seq2 - expected_seq2, 2); // Gap of 2
}

/// Test duplicate sequence detection
#[test]
fn test_duplicate_sequence_detection() {
    // Duplicate: sequence already seen
    let last_committed_seq: i32 = 100;
    let incoming_seq: i32 = 99; // Already processed

    let is_duplicate = incoming_seq <= last_committed_seq;
    assert!(is_duplicate);

    // Not duplicate: new sequence
    let new_seq: i32 = 101;
    let is_new = new_seq > last_committed_seq;
    assert!(is_new);
}

// ============================================================================
// Producer State Tests
// ============================================================================

/// Test producer state structure
#[test]
fn test_producer_state_fields() {
    // Producer state tracks:
    // - producer_id: i64
    // - producer_epoch: i16
    // - last_sequence: i32
    // - last_offset: i64
    // - timestamp: i64

    struct ProducerState {
        producer_id: i64,
        producer_epoch: i16,
        last_sequence: i32,
    }

    let state = ProducerState {
        producer_id: 12345,
        producer_epoch: 3,
        last_sequence: 50,
    };

    assert_eq!(state.producer_id, 12345);
    assert_eq!(state.producer_epoch, 3);
    assert_eq!(state.last_sequence, 50);
}

/// Test producer epoch fencing
#[test]
fn test_producer_epoch_fencing() {
    let stored_epoch: i16 = 5;

    // Same epoch: allowed
    let incoming_epoch1: i16 = 5;
    assert!(incoming_epoch1 >= stored_epoch);

    // Higher epoch: allowed (new producer instance)
    let incoming_epoch2: i16 = 6;
    assert!(incoming_epoch2 >= stored_epoch);

    // Lower epoch: fenced (old producer instance)
    let incoming_epoch3: i16 = 4;
    assert!(incoming_epoch3 < stored_epoch);
}

/// Test epoch overflow
#[test]
fn test_producer_epoch_overflow() {
    let max_epoch: i16 = i16::MAX;

    // When epoch reaches max, producer needs new ID
    let next_epoch = max_epoch.wrapping_add(1);
    assert_eq!(next_epoch, i16::MIN); // Wraps to negative

    // In practice, should get new producer ID before overflow
    assert!(max_epoch > 32000); // i16::MAX = 32767
}

// ============================================================================
// Idempotent Produce Request Tests
// ============================================================================

/// Helper to create idempotent produce data
fn create_idempotent_record_batch(
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    record_count: i32,
) -> BytesMut {
    let mut buf = BytesMut::new();

    let records: Vec<Record> = (0..record_count)
        .map(|i| Record {
            transactional: false,
            control: false,
            partition_leader_epoch: 0,
            producer_id,
            producer_epoch,
            timestamp_type: TimestampType::Creation,
            offset: i as i64,
            sequence: base_sequence + i,
            timestamp: 1609459200000 + (i as i64 * 100),
            key: Some(Bytes::from(format!("key-{}", i))),
            value: Some(Bytes::from(format!("value-{}", i))),
            headers: Default::default(),
        })
        .collect();

    let options = RecordEncodeOptions {
        version: 2,
        compression: Compression::None,
    };

    RecordBatchEncoder::encode_with_custom_compression::<
        _,
        _,
        fn(&mut BytesMut, &mut BytesMut, Compression) -> anyhow::Result<()>,
    >(&mut buf, records.iter(), &options, None)
    .unwrap();

    buf
}

/// Test idempotent produce batch encoding
#[test]
fn test_idempotent_produce_batch_encoding() {
    let batch = create_idempotent_record_batch(12345, 3, 100, 5);

    // Verify batch contains expected fields
    assert!(!batch.is_empty());

    // Parse the batch header to verify producer fields
    let bytes = batch.freeze();
    let mut reader = bytes.as_ref();

    // Skip to producer ID position (offset 43 in record batch)
    // baseOffset(8) + batchLength(4) + partitionLeaderEpoch(4) + magic(1) +
    // crc(4) + attributes(2) + lastOffsetDelta(4) + firstTimestamp(8) + maxTimestamp(8) = 43
    if bytes.len() > 51 {
        let _base_offset = reader.get_i64();
        let _batch_length = reader.get_i32();
        let _partition_leader_epoch = reader.get_i32();
        let _magic = reader.get_i8();
        let _crc = reader.get_i32();
        let _attributes = reader.get_i16();
        let _last_offset_delta = reader.get_i32();
        let _first_timestamp = reader.get_i64();
        let _max_timestamp = reader.get_i64();
        let producer_id = reader.get_i64();
        let producer_epoch = reader.get_i16();
        let base_sequence = reader.get_i32();

        assert_eq!(producer_id, 12345);
        assert_eq!(producer_epoch, 3);
        assert_eq!(base_sequence, 100);
    }
}

/// Test produce response with duplicate detection
#[test]
fn test_produce_response_duplicate() {
    use kafka_protocol::messages::produce_response::PartitionProduceResponse;

    let mut partition_response = PartitionProduceResponse::default();
    partition_response.index = 0;
    partition_response.error_code = 0; // No error for duplicate (already acked)
    partition_response.base_offset = 500; // Original offset

    // Response indicates the existing offset for duplicate
    assert_eq!(partition_response.base_offset, 500);
}

/// Test produce response with out of order sequence
#[test]
fn test_produce_response_out_of_order_sequence() {
    use kafka_protocol::messages::produce_response::PartitionProduceResponse;

    let mut partition_response = PartitionProduceResponse::default();
    partition_response.index = 0;
    partition_response.error_code = 45; // OUT_OF_ORDER_SEQUENCE_NUMBER
    partition_response.base_offset = -1;

    assert_eq!(partition_response.error_code, 45);
}

/// Test produce response with duplicate sequence
#[test]
fn test_produce_response_duplicate_sequence() {
    use kafka_protocol::messages::produce_response::PartitionProduceResponse;

    let mut partition_response = PartitionProduceResponse::default();
    partition_response.index = 0;
    partition_response.error_code = 46; // DUPLICATE_SEQUENCE_NUMBER
    partition_response.base_offset = 1000; // Offset of original record

    assert_eq!(partition_response.error_code, 46);
    assert_eq!(partition_response.base_offset, 1000);
}

// ============================================================================
// Producer Fencing Tests
// ============================================================================

/// Test PRODUCER_FENCED error
#[test]
fn test_producer_fenced_error() {
    use kafka_protocol::messages::produce_response::PartitionProduceResponse;

    let mut partition_response = PartitionProduceResponse::default();
    partition_response.index = 0;
    partition_response.error_code = 90; // PRODUCER_FENCED
    partition_response.base_offset = -1;

    assert_eq!(partition_response.error_code, 90);
}

/// Test INVALID_PRODUCER_EPOCH error
#[test]
fn test_invalid_producer_epoch_error() {
    use kafka_protocol::messages::produce_response::PartitionProduceResponse;

    let mut partition_response = PartitionProduceResponse::default();
    partition_response.index = 0;
    partition_response.error_code = 47; // INVALID_PRODUCER_EPOCH
    partition_response.base_offset = -1;

    assert_eq!(partition_response.error_code, 47);
}

/// Test UNKNOWN_PRODUCER_ID error
#[test]
fn test_unknown_producer_id_error() {
    use kafka_protocol::messages::produce_response::PartitionProduceResponse;

    let mut partition_response = PartitionProduceResponse::default();
    partition_response.index = 0;
    partition_response.error_code = 59; // UNKNOWN_PRODUCER_ID
    partition_response.base_offset = -1;

    assert_eq!(partition_response.error_code, 59);
}

// ============================================================================
// Transaction Timeout Tests
// ============================================================================

/// Test transaction timeout encoding
#[test]
fn test_transaction_timeout_encoding() {
    let timeout_ms: i32 = 60000; // 1 minute

    let mut buf = BytesMut::new();
    buf.put_i32(timeout_ms);

    let bytes = buf.freeze();
    let mut reader = bytes.as_ref();
    let read_timeout = reader.get_i32();

    assert_eq!(read_timeout, 60000);
}

/// Test various timeout values
#[test]
fn test_timeout_range() {
    // Minimum timeout (broker may reject if too low)
    let min_timeout: i32 = 1000; // 1 second
    assert!(min_timeout >= 0);

    // Default timeout
    let default_timeout: i32 = 60000; // 1 minute
    assert_eq!(default_timeout, 60000);

    // Maximum timeout (15 minutes is common max)
    let max_timeout: i32 = 900000;
    assert!(max_timeout > default_timeout);
}

// ============================================================================
// Request Header Tests for Idempotent Producer
// ============================================================================

/// Test request header for InitProducerId
#[test]
fn test_init_producer_id_request_header() {
    let mut header = RequestHeader::default();
    header.request_api_key = 22; // InitProducerId
    header.request_api_version = 4;
    header.correlation_id = 12345;
    header.client_id = Some(StrBytes::from_static_str("idempotent-producer"));

    let mut buf = BytesMut::new();
    let header_version = InitProducerIdRequest::header_version(4);
    header.encode(&mut buf, header_version).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = RequestHeader::decode(&mut read_buf, header_version).unwrap();

    assert_eq!(decoded.request_api_key, 22);
    assert_eq!(decoded.request_api_version, 4);
    assert_eq!(decoded.correlation_id, 12345);
}

/// Test response header for InitProducerId
#[test]
fn test_init_producer_id_response_header() {
    let mut header = ResponseHeader::default();
    header.correlation_id = 12345;

    let mut buf = BytesMut::new();
    let header_version = InitProducerIdResponse::header_version(4);
    header.encode(&mut buf, header_version).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ResponseHeader::decode(&mut read_buf, header_version).unwrap();

    assert_eq!(decoded.correlation_id, 12345);
}

// ============================================================================
// Multiple Batch Sequence Tests
// ============================================================================

/// Test multiple batches with sequential sequences
#[test]
fn test_multiple_batch_sequences() {
    // First batch: sequences 0-4
    let batch1 = create_idempotent_record_batch(12345, 1, 0, 5);
    assert!(!batch1.is_empty());

    // Second batch: sequences 5-9
    let batch2 = create_idempotent_record_batch(12345, 1, 5, 5);
    assert!(!batch2.is_empty());

    // Third batch: sequences 10-14
    let batch3 = create_idempotent_record_batch(12345, 1, 10, 5);
    assert!(!batch3.is_empty());
}

/// Test sequence gap detection
#[test]
fn test_sequence_gap_detection() {
    let expected_next_sequence: i32 = 100;
    let incoming_sequence: i32 = 105; // Gap of 5

    let gap = incoming_sequence - expected_next_sequence;
    assert_eq!(gap, 5);

    // This would result in OUT_OF_ORDER_SEQUENCE_NUMBER error
    assert!(gap > 0);
}

// ============================================================================
// API Key Tests
// ============================================================================

/// Test InitProducerId API key
#[test]
fn test_init_producer_id_api_key() {
    let api_key: i16 = 22;
    assert_eq!(api_key, 22);

    // Verify API key in request header
    let mut header = RequestHeader::default();
    header.request_api_key = api_key;

    assert_eq!(header.request_api_key, 22);
}

/// Test supported InitProducerId versions
#[test]
fn test_init_producer_id_versions() {
    // v0: Basic
    // v1: Added throttle_time_ms to response
    // v2: Support for flexible versions
    // v3: Added producer_id and producer_epoch to request for fencing
    // v4: Fully flexible

    for version in 0..=4 {
        let request = InitProducerIdRequest::default();
        let mut buf = BytesMut::new();

        // Should be able to encode all supported versions
        let result = request.encode(&mut buf, version);
        assert!(result.is_ok(), "Failed to encode version {}", version);
    }
}

// ============================================================================
// Idempotent Producer Configuration Tests
// ============================================================================

/// Test idempotent producer config values
#[test]
fn test_idempotent_producer_config() {
    // enable.idempotence = true
    let idempotence_enabled = true;
    assert!(idempotence_enabled);

    // acks = all (-1) required for idempotence
    let acks: i16 = -1;
    assert_eq!(acks, -1);

    // max.in.flight.requests.per.connection <= 5 for ordering
    let max_in_flight: u32 = 5;
    assert!(max_in_flight <= 5);
}

/// Test retries config for idempotent producer
#[test]
fn test_idempotent_retries_config() {
    // Idempotent producers can safely retry
    let retries: i32 = i32::MAX; // Effectively infinite
    assert!(retries > 0);

    // delivery.timeout.ms controls total time for retries
    let delivery_timeout_ms: i32 = 120000; // 2 minutes
    assert!(delivery_timeout_ms > 0);
}

// ============================================================================
// Record Key Tests
// ============================================================================

/// Test record batch with consistent producer fields
#[test]
fn test_batch_producer_field_consistency() {
    // All records in a batch must have same producer_id, epoch
    let producer_id: i64 = 12345;
    let producer_epoch: i16 = 3;
    let base_sequence: i32 = 100;

    let record1 = Record {
        transactional: false,
        control: false,
        partition_leader_epoch: 0,
        producer_id,
        producer_epoch,
        timestamp_type: TimestampType::Creation,
        offset: 0,
        sequence: base_sequence,
        timestamp: 1609459200000,
        key: Some(Bytes::from("key1")),
        value: Some(Bytes::from("value1")),
        headers: Default::default(),
    };

    let record2 = Record {
        transactional: false,
        control: false,
        partition_leader_epoch: 0,
        producer_id,    // Same
        producer_epoch, // Same
        timestamp_type: TimestampType::Creation,
        offset: 1,
        sequence: base_sequence + 1, // Incremented
        timestamp: 1609459200100,
        key: Some(Bytes::from("key2")),
        value: Some(Bytes::from("value2")),
        headers: Default::default(),
    };

    assert_eq!(record1.producer_id, record2.producer_id);
    assert_eq!(record1.producer_epoch, record2.producer_epoch);
    assert_eq!(record2.sequence - record1.sequence, 1);
}

// ============================================================================
// Error Code Constants Tests
// ============================================================================

/// Test idempotent producer error codes
#[test]
fn test_idempotent_error_codes() {
    // Error codes related to idempotent producers
    let out_of_order_sequence: i16 = 45;
    let duplicate_sequence: i16 = 46;
    let invalid_producer_epoch: i16 = 47;
    let unknown_producer_id: i16 = 59;
    let producer_fenced: i16 = 90;

    assert_eq!(out_of_order_sequence, 45);
    assert_eq!(duplicate_sequence, 46);
    assert_eq!(invalid_producer_epoch, 47);
    assert_eq!(unknown_producer_id, 59);
    assert_eq!(producer_fenced, 90);
}

/// Test transactional error codes
#[test]
fn test_transactional_error_codes() {
    let invalid_txn_state: i16 = 48;
    let invalid_producer_id_mapping: i16 = 49;
    let transactional_id_authorization_failed: i16 = 53;
    let concurrent_transactions: i16 = 51;

    assert_eq!(invalid_txn_state, 48);
    assert_eq!(invalid_producer_id_mapping, 49);
    assert_eq!(transactional_id_authorization_failed, 53);
    assert_eq!(concurrent_transactions, 51);
}

// ============================================================================
// Sequence Delta Tests
// ============================================================================

/// Test sequence delta in batch
#[test]
fn test_sequence_delta_in_batch() {
    // In a batch, sequence delta = sequence - base_sequence
    let base_sequence: i32 = 100;
    let record_sequences = [100, 101, 102, 103, 104];

    for (i, &seq) in record_sequences.iter().enumerate() {
        let delta = seq - base_sequence;
        assert_eq!(delta, i as i32);
    }
}

/// Test last sequence calculation
#[test]
fn test_last_sequence_calculation() {
    let base_sequence: i32 = 100;
    let record_count: i32 = 5;

    let last_sequence = base_sequence + record_count - 1;
    assert_eq!(last_sequence, 104);
}
