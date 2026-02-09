//! Control record creation for Kafka transaction markers
//!
//! Constants and functions for creating transaction commit/abort markers.

use bytes::{Bytes, BytesMut};

/// Control record type for abort marker (transaction aborted)
pub const CONTROL_TYPE_ABORT: i16 = 0;

/// Control record type for commit marker (transaction committed)
pub const CONTROL_TYPE_COMMIT: i16 = 1;

/// Kafka v2 record batch magic byte constant
pub(crate) const RECORD_BATCH_MAGIC_V2: i8 = 2;

/// Batch attributes bit positions
pub(crate) const ATTR_TRANSACTIONAL_BIT: i16 = 0x10; // bit 4
pub(crate) const ATTR_CONTROL_BIT: i16 = 0x20; // bit 5

/// Create a control batch (transaction marker) for commit or abort
///
/// Control records are written to partitions to mark transaction boundaries.
/// Consumers use these markers to implement read committed isolation level.
///
/// # Arguments
/// * `base_offset` - The base offset for this batch
/// * `producer_id` - The producer ID that owns this transaction
/// * `producer_epoch` - The producer epoch
/// * `control_type` - Either `CONTROL_TYPE_ABORT` (0) or `CONTROL_TYPE_COMMIT` (1)
///
/// # Returns
/// A serialized control record batch ready to be appended to a partition
pub fn create_control_record_batch(
    base_offset: i64,
    producer_id: i64,
    producer_epoch: i16,
    control_type: i16,
) -> Bytes {
    use bytes::BufMut;

    let mut buf = BytesMut::with_capacity(256);

    // baseOffset: int64
    buf.put_i64(base_offset);

    // batchLength placeholder
    let batch_length_pos = buf.len();
    buf.put_i32(0);

    // partitionLeaderEpoch: int32
    buf.put_i32(0);

    // magic: int8 (must be 2 for Kafka v2)
    buf.put_i8(RECORD_BATCH_MAGIC_V2);

    // CRC placeholder
    let crc_pos = buf.len();
    buf.put_u32(0);

    let crc_start = buf.len();

    // attributes: int16 (transactional=1, control=1)
    let attributes = ATTR_TRANSACTIONAL_BIT | ATTR_CONTROL_BIT;
    buf.put_i16(attributes);

    // lastOffsetDelta: int32 (0 for single control record)
    buf.put_i32(0);

    // firstTimestamp: int64
    let timestamp = chrono::Utc::now().timestamp_millis();
    buf.put_i64(timestamp);

    // maxTimestamp: int64
    buf.put_i64(timestamp);

    // producerId: int64
    buf.put_i64(producer_id);

    // producerEpoch: int16
    buf.put_i16(producer_epoch);

    // baseSequence: int32 (-1 for control batches)
    buf.put_i32(-1);

    // records array length: int32 (1 control record)
    buf.put_i32(1);

    // Control record format:
    // The key contains: version (i16) + type (i16)
    // The value is null (empty)

    // Build the control record key
    let mut key_buf = BytesMut::with_capacity(4);
    key_buf.put_i16(0); // version
    key_buf.put_i16(control_type); // ABORT(0) or COMMIT(1)

    // Record format (varint encoded):
    // length, attributes, timestampDelta, offsetDelta, keyLength, key, valueLength, value, headers

    let record_attributes: i8 = 0;
    let key_len = key_buf.len() as i32;
    let headers_count: i32 = 0;

    // Calculate record length (all varints)
    let record_len = 1 + 1 + 1 + 1 + key_len + 1 + 1; // simplified, actual uses varints

    // Write varint-encoded record
    buf.put_u8(record_len as u8); // length varint (simplified - single byte for small values)
    buf.put_i8(record_attributes);
    buf.put_u8(0); // timestampDelta varint = 0
    buf.put_u8(0); // offsetDelta varint = 0

    // keyLength as varint (positive = length + 1)
    buf.put_u8((key_len + 1) as u8);
    buf.put_slice(&key_buf);

    // valueLength as varint (0 = null, represented as 0 in compact encoding)
    buf.put_u8(0); // null value

    // headers count as varint
    buf.put_u8((headers_count + 1) as u8);

    // Calculate CRC32C
    let crc = crc32fast::hash(&buf[crc_start..]);
    let crc_bytes = crc.to_be_bytes();
    buf[crc_pos..crc_pos + 4].copy_from_slice(&crc_bytes);

    // Update batch length
    let batch_length = (buf.len() - 12) as i32; // exclude baseOffset and batchLength
    let batch_len_bytes = batch_length.to_be_bytes();
    buf[batch_length_pos..batch_length_pos + 4].copy_from_slice(&batch_len_bytes);

    buf.freeze()
}
