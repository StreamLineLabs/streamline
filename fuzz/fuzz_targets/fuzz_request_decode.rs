#![no_main]
use libfuzzer_sys::fuzz_target;
use bytes::Bytes;
use kafka_protocol::messages::{
    ProduceRequest, FetchRequest, MetadataRequest, ApiVersionsRequest,
    CreateTopicsRequest, DeleteTopicsRequest,
};
use kafka_protocol::protocol::Decodable;

fuzz_target!(|data: &[u8]| {
    if data.len() < 2 {
        return;
    }

    // Use first byte to select which request type to fuzz
    let selector = data[0] % 6;
    let payload = &data[1..];
    let mut buf = Bytes::copy_from_slice(payload);

    match selector {
        0 => { let _ = ProduceRequest::decode(&mut buf, 9); }
        1 => { let _ = FetchRequest::decode(&mut buf, 13); }
        2 => { let _ = MetadataRequest::decode(&mut buf, 12); }
        3 => { let _ = ApiVersionsRequest::decode(&mut buf, 3); }
        4 => { let _ = CreateTopicsRequest::decode(&mut buf, 7); }
        5 => { let _ = DeleteTopicsRequest::decode(&mut buf, 6); }
        _ => unreachable!(),
    }
});
