#![no_main]
use libfuzzer_sys::fuzz_target;
use bytes::Bytes;
use kafka_protocol::records::{Compression, RecordBatchDecoder};

type DecompressFn = fn(&mut Bytes, Compression) -> anyhow::Result<Bytes>;

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }

    // Try to decode arbitrary bytes as a Kafka v2 record batch (no decompression)
    let mut buf = Bytes::copy_from_slice(data);
    let _ = RecordBatchDecoder::decode::<Bytes, DecompressFn>(&mut buf);
});
