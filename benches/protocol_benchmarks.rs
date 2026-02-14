//! Protocol layer benchmarks for Streamline
//!
//! Run with: cargo bench --bench protocol_benchmarks
//!
//! These benchmarks measure the performance of Kafka protocol operations
//! including request parsing, response serialization, and handler latency.

use bytes::BytesMut;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use kafka_protocol::messages::{
    ApiKey, BrokerId, FetchResponse, MetadataRequest, MetadataResponse, ProduceRequest,
    RequestHeader, TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};

/// Create a sample produce request for benchmarking
fn create_produce_request(num_records: usize, record_size: usize) -> ProduceRequest {
    use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};

    let mut request = ProduceRequest::default();
    request.acks = -1;
    request.timeout_ms = 30000;

    let mut topic_data = TopicProduceData::default();
    topic_data.name = TopicName(StrBytes::from_static_str("bench-topic"));

    let mut partition_data = PartitionProduceData::default();
    partition_data.index = 0;

    // Create records as raw bytes to simulate encoded record batch
    let record_value = vec![b'x'; record_size];
    let mut batch_buf = BytesMut::with_capacity(num_records * (record_size + 50));

    // Build a simple record batch manually
    // For benchmarking purposes, we'll create pre-encoded data
    for i in 0..num_records {
        // Simple record format: length + key + value
        let key = format!("key-{}", i);
        batch_buf.extend_from_slice(&(key.len() as u32).to_be_bytes());
        batch_buf.extend_from_slice(key.as_bytes());
        batch_buf.extend_from_slice(&(record_value.len() as u32).to_be_bytes());
        batch_buf.extend_from_slice(&record_value);
    }

    partition_data.records = Some(batch_buf.freeze());
    topic_data.partition_data = vec![partition_data];
    request.topic_data = vec![topic_data];

    request
}

/// Create a sample fetch response for benchmarking
fn create_fetch_response(num_records: usize, record_size: usize) -> FetchResponse {
    use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};

    let mut response = FetchResponse::default();

    let mut topic_response = FetchableTopicResponse::default();
    topic_response.topic = TopicName(StrBytes::from_static_str("bench-topic"));

    let mut partition = PartitionData::default();
    partition.partition_index = 0;
    partition.error_code = 0;
    partition.high_watermark = num_records as i64;

    // Create records as raw bytes
    let record_value = vec![b'x'; record_size];
    let mut batch_buf = BytesMut::with_capacity(num_records * (record_size + 50));

    for i in 0..num_records {
        let key = format!("key-{}", i);
        batch_buf.extend_from_slice(&(key.len() as u32).to_be_bytes());
        batch_buf.extend_from_slice(key.as_bytes());
        batch_buf.extend_from_slice(&(record_value.len() as u32).to_be_bytes());
        batch_buf.extend_from_slice(&record_value);
    }

    partition.records = Some(batch_buf.freeze());
    topic_response.partitions = vec![partition];
    response.responses = vec![topic_response];

    response
}

/// Benchmark produce request encoding
fn bench_produce_request_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("produce_request_encode");

    for (num_records, record_size) in [(1, 100), (10, 100), (100, 100), (100, 1000)] {
        let request = create_produce_request(num_records, record_size);
        let total_bytes = num_records * record_size;

        group.throughput(Throughput::Bytes(total_bytes as u64));
        group.bench_with_input(
            BenchmarkId::new("records_size", format!("{}x{}", num_records, record_size)),
            &request,
            |b, req| {
                b.iter(|| {
                    let mut buf = BytesMut::with_capacity(4096);
                    black_box(req.encode(&mut buf, 9)).unwrap();
                    black_box(buf)
                })
            },
        );
    }

    group.finish();
}

/// Benchmark produce request decoding
fn bench_produce_request_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("produce_request_decode");

    for (num_records, record_size) in [(1, 100), (10, 100), (100, 100), (100, 1000)] {
        let request = create_produce_request(num_records, record_size);
        let total_bytes = num_records * record_size;

        // Encode to bytes first
        let mut buf = BytesMut::with_capacity(4096);
        request.encode(&mut buf, 9).unwrap();
        let encoded = buf.freeze();

        group.throughput(Throughput::Bytes(total_bytes as u64));
        group.bench_with_input(
            BenchmarkId::new("records_size", format!("{}x{}", num_records, record_size)),
            &encoded,
            |b, bytes| {
                b.iter(|| {
                    let mut cursor = bytes.clone();
                    black_box(ProduceRequest::decode(&mut cursor, 9)).unwrap()
                })
            },
        );
    }

    group.finish();
}

/// Benchmark fetch response encoding
fn bench_fetch_response_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("fetch_response_encode");

    for (num_records, record_size) in [(1, 100), (10, 100), (100, 100), (1000, 100)] {
        let response = create_fetch_response(num_records, record_size);
        let total_bytes = num_records * record_size;

        group.throughput(Throughput::Bytes(total_bytes as u64));
        group.bench_with_input(
            BenchmarkId::new("records_size", format!("{}x{}", num_records, record_size)),
            &response,
            |b, resp| {
                b.iter(|| {
                    let mut buf = BytesMut::with_capacity(4096);
                    black_box(resp.encode(&mut buf, 13)).unwrap();
                    black_box(buf)
                })
            },
        );
    }

    group.finish();
}

/// Benchmark fetch response decoding
fn bench_fetch_response_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("fetch_response_decode");

    for (num_records, record_size) in [(1, 100), (10, 100), (100, 100), (1000, 100)] {
        let response = create_fetch_response(num_records, record_size);
        let total_bytes = num_records * record_size;

        // Encode to bytes first
        let mut buf = BytesMut::with_capacity(65536);
        response.encode(&mut buf, 13).unwrap();
        let encoded = buf.freeze();

        group.throughput(Throughput::Bytes(total_bytes as u64));
        group.bench_with_input(
            BenchmarkId::new("records_size", format!("{}x{}", num_records, record_size)),
            &encoded,
            |b, bytes| {
                b.iter(|| {
                    let mut cursor = bytes.clone();
                    black_box(FetchResponse::decode(&mut cursor, 13)).unwrap()
                })
            },
        );
    }

    group.finish();
}

/// Benchmark request header parsing
fn bench_request_header_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("request_header");

    // Create a typical request header
    let mut header = RequestHeader::default();
    header.request_api_key = ApiKey::Produce as i16;
    header.request_api_version = 9;
    header.correlation_id = 12345;
    header.client_id = Some(StrBytes::from_static_str("benchmark-client"));

    let mut buf = BytesMut::new();
    header.encode(&mut buf, 2).unwrap();
    let encoded = buf.freeze();

    group.bench_function("decode_v2", |b| {
        b.iter(|| {
            let mut cursor = encoded.clone();
            black_box(RequestHeader::decode(&mut cursor, 2)).unwrap()
        })
    });

    group.finish();
}

/// Benchmark metadata request/response
fn bench_metadata(c: &mut Criterion) {
    let mut group = c.benchmark_group("metadata");

    // Create metadata request
    let mut request = MetadataRequest::default();
    request.topics = Some(vec![]);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 12).unwrap();
    let request_encoded = buf.freeze();

    group.bench_function("request_decode", |b| {
        b.iter(|| {
            let mut cursor = request_encoded.clone();
            black_box(MetadataRequest::decode(&mut cursor, 12)).unwrap()
        })
    });

    // Create metadata response with multiple topics
    let mut response = MetadataResponse::default();
    response.cluster_id = Some(StrBytes::from_static_str("streamline-cluster"));

    // Add brokers
    use kafka_protocol::messages::metadata_response::MetadataResponseBroker;
    let mut broker = MetadataResponseBroker::default();
    broker.node_id = BrokerId(1);
    broker.host = StrBytes::from_static_str("localhost");
    broker.port = 9092;
    response.brokers = vec![broker];

    // Add topics
    use kafka_protocol::messages::metadata_response::{
        MetadataResponsePartition, MetadataResponseTopic,
    };
    for i in 0..10 {
        let mut topic = MetadataResponseTopic::default();
        topic.name = Some(TopicName(StrBytes::from_string(format!("topic-{}", i))));
        topic.error_code = 0;

        for p in 0..4 {
            let mut partition = MetadataResponsePartition::default();
            partition.partition_index = p;
            partition.leader_id = BrokerId(1);
            partition.replica_nodes = vec![BrokerId(1)];
            partition.isr_nodes = vec![BrokerId(1)];
            topic.partitions.push(partition);
        }
        response.topics.push(topic);
    }

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 12).unwrap();
    let response_encoded = buf.freeze();

    group.bench_function("response_encode", |b| {
        b.iter(|| {
            let mut buf = BytesMut::with_capacity(4096);
            black_box(response.encode(&mut buf, 12)).unwrap();
            black_box(buf)
        })
    });

    group.bench_function("response_decode", |b| {
        b.iter(|| {
            let mut cursor = response_encoded.clone();
            black_box(MetadataResponse::decode(&mut cursor, 12)).unwrap()
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_produce_request_encode,
    bench_produce_request_decode,
    bench_fetch_response_encode,
    bench_fetch_response_decode,
    bench_request_header_decode,
    bench_metadata,
);

criterion_main!(benches);
