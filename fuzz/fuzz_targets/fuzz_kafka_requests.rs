#![no_main]
use libfuzzer_sys::fuzz_target;
use bytes::{Bytes, BytesMut, BufMut};
use kafka_protocol::messages::{
    ProduceRequest, FetchRequest, MetadataRequest, ApiVersionsRequest,
    CreateTopicsRequest, ListOffsetsRequest,
};
use kafka_protocol::protocol::Decodable;

/// Fuzz the full Kafka wire protocol decode path for multiple request types.
/// This targets the 393 unwrap() calls in protocol/kafka/ by feeding arbitrary
/// bytes into every major request decoder.
fuzz_target!(|data: &[u8]| {
    if data.len() < 2 {
        return;
    }

    // Use first byte to select which request type to fuzz
    let selector = data[0] % 6;
    let body = &data[1..];
    let mut buf = Bytes::copy_from_slice(body);

    match selector {
        0 => {
            // ProduceRequest — highest risk due to record batch parsing
            for version in [0, 3, 7, 9] {
                let mut b = buf.clone();
                let _ = ProduceRequest::decode(&mut b, version);
            }
        }
        1 => {
            // FetchRequest — complex partition/topic nesting
            for version in [0, 4, 11, 15] {
                let mut b = buf.clone();
                let _ = FetchRequest::decode(&mut b, version);
            }
        }
        2 => {
            // MetadataRequest — topic name parsing
            for version in [0, 4, 9, 12] {
                let mut b = buf.clone();
                let _ = MetadataRequest::decode(&mut b, version);
            }
        }
        3 => {
            // ApiVersionsRequest — should handle any garbage gracefully
            for version in [0, 1, 3] {
                let mut b = buf.clone();
                let _ = ApiVersionsRequest::decode(&mut b, version);
            }
        }
        4 => {
            // CreateTopicsRequest — topic config parsing
            for version in [0, 5, 7] {
                let mut b = buf.clone();
                let _ = CreateTopicsRequest::decode(&mut b, version);
            }
        }
        5 => {
            // ListOffsetsRequest — partition/timestamp parsing
            for version in [0, 4, 7] {
                let mut b = buf.clone();
                let _ = ListOffsetsRequest::decode(&mut b, version);
            }
        }
        _ => {}
    }
});
