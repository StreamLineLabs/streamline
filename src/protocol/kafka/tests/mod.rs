use super::*;
use tempfile::tempdir;

pub(super) fn create_test_handler() -> KafkaHandler {
    let dir = tempdir().unwrap();
    let topic_manager = Arc::new(TopicManager::new(dir.path()).unwrap());
    let offset_dir = tempdir().unwrap();
    let group_coordinator =
        Arc::new(GroupCoordinator::new(offset_dir.path(), topic_manager.clone()).unwrap());
    KafkaHandler::new(topic_manager, group_coordinator).expect("Failed to create test handler")
}

/// Create a test SessionManager for quota tracking tests
pub(super) fn create_test_session_manager() -> SessionManager {
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
    SessionManager::with_peer_addr_and_tls(
        Some(SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::LOCALHOST,
            12345,
        ))),
        false,
    )
}

pub(super) fn create_handler_with_topics(topic_specs: &[(&str, i32)]) -> KafkaHandler {
    let dir = tempdir().unwrap();
    let topic_manager = Arc::new(TopicManager::new(dir.path()).unwrap());

    for (name, partitions) in topic_specs {
        topic_manager.create_topic(name, *partitions).unwrap();
    }

    let offset_dir = tempdir().unwrap();
    let group_coordinator =
        Arc::new(GroupCoordinator::new(offset_dir.path(), topic_manager.clone()).unwrap());
    KafkaHandler::new(topic_manager, group_coordinator)
        .expect("Failed to create handler with topics")
}

pub(super) fn create_test_record_batch(
    base_offset: i64,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    compression: i16,
    record_count: i32,
) -> bytes::Bytes {
    use bytes::{BufMut, BytesMut};

    let mut buf = BytesMut::with_capacity(256);

    // baseOffset: int64
    buf.put_i64(base_offset);

    // We'll calculate batchLength after building the content
    let batch_length_pos = buf.len();
    buf.put_i32(0); // placeholder

    // partitionLeaderEpoch: int32
    buf.put_i32(0);

    // magic: int8 (must be 2 for Kafka v2)
    buf.put_i8(RECORD_BATCH_MAGIC_V2);

    // CRC placeholder - we'll calculate after
    let crc_pos = buf.len();
    buf.put_u32(0); // placeholder

    // Everything after CRC is included in CRC calculation
    let crc_start = buf.len();

    // attributes: int16 (bits 0-2: compression, bit 3: timestamp type, bit 4: transactional, bit 5: control)
    buf.put_i16(compression);

    // lastOffsetDelta: int32
    buf.put_i32(record_count - 1);

    // firstTimestamp: int64
    let timestamp = chrono::Utc::now().timestamp_millis();
    buf.put_i64(timestamp);

    // maxTimestamp: int64
    buf.put_i64(timestamp);

    // producerId: int64
    buf.put_i64(producer_id);

    // producerEpoch: int16
    buf.put_i16(producer_epoch);

    // baseSequence: int32
    buf.put_i32(base_sequence);

    // records array length: int32
    buf.put_i32(record_count);

    // Add minimal records (varint encoded)
    for i in 0..record_count {
        let value = format!("test-{}", i);
        let value_bytes = value.as_bytes();

        let record_len = 1 + 1 + 1 + 1 + 1 + value_bytes.len() + 1;

        // length (varint)
        buf.put_u8(record_len as u8);

        // attributes: int8
        buf.put_i8(0);

        // timestampDelta: varlong (0 for simplicity)
        buf.put_u8(0);

        // offsetDelta: varint
        buf.put_u8(i as u8);

        // keyLength: varint (-1 = null, encoded as 0x01 in zigzag)
        buf.put_u8(0x01); // -1 in zigzag encoding

        // valueLength: varint
        buf.put_u8((value_bytes.len() * 2) as u8); // zigzag encoding

        // value bytes
        buf.put_slice(value_bytes);

        // headers count: varint (0)
        buf.put_u8(0);
    }

    // Calculate and set batchLength (everything after baseOffset and batchLength)
    let batch_length = (buf.len() - 8 - 4) as i32;
    buf[batch_length_pos..batch_length_pos + 4].copy_from_slice(&batch_length.to_be_bytes());

    // Calculate CRC32C over everything from attributes onward
    let crc_data = &buf[crc_start..];
    let crc = crc32fast::hash(crc_data);
    buf[crc_pos..crc_pos + 4].copy_from_slice(&crc.to_be_bytes());

    buf.freeze()
}

mod api_versions;
mod auth;
mod compaction;
mod configs;
mod consumer_groups;
mod error_codes;
mod handler;
mod headers;
mod idempotent;
mod integration;
mod metadata;
mod misc;
mod property;
mod quotas;
mod record_batch;
mod tls;
mod topics;
mod transactions;
