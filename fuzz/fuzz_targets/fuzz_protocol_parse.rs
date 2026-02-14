#![no_main]
use libfuzzer_sys::fuzz_target;
use bytes::Bytes;
use kafka_protocol::messages::{ApiKey, RequestHeader};
use kafka_protocol::protocol::Decodable;

fuzz_target!(|data: &[u8]| {
    // Need at least 4 bytes for api_key (2) + api_version (2)
    if data.len() < 4 {
        return;
    }

    let api_key = i16::from_be_bytes([data[0], data[1]]);
    let _api_version = i16::from_be_bytes([data[2], data[3]]);

    // Try to parse the API key enum
    let _ = ApiKey::try_from(api_key);

    // Try to decode as a RequestHeader with header version 1 (legacy)
    let mut buf = Bytes::copy_from_slice(data);
    let _ = RequestHeader::decode(&mut buf, 1);

    // Try to decode as a RequestHeader with header version 2 (flexible)
    let mut buf = Bytes::copy_from_slice(data);
    let _ = RequestHeader::decode(&mut buf, 2);

    // Try header version 0 (oldest)
    let mut buf = Bytes::copy_from_slice(data);
    let _ = RequestHeader::decode(&mut buf, 0);
});
