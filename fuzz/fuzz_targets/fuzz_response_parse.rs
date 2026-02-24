#![no_main]
use libfuzzer_sys::fuzz_target;
use bytes::Bytes;
use kafka_protocol::messages::ResponseHeader;
use kafka_protocol::protocol::Decodable;

fuzz_target!(|data: &[u8]| {
    if data.len() < 4 {
        return;
    }

    // Try to decode as a ResponseHeader with header version 0
    let mut buf = Bytes::copy_from_slice(data);
    let _ = ResponseHeader::decode(&mut buf, 0);

    // Try to decode as a ResponseHeader with header version 1 (flexible)
    let mut buf = Bytes::copy_from_slice(data);
    let _ = ResponseHeader::decode(&mut buf, 1);
});
