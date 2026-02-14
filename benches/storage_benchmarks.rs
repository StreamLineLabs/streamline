//! Storage layer benchmarks for Streamline
//!
//! Run with: cargo bench
//!
//! These benchmarks measure the performance of core storage operations
//! including writes, reads, and topic management.

use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use streamline::TopicManager;
use tempfile::tempdir;

/// Benchmark single record writes
fn bench_single_write(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let manager = TopicManager::new(dir.path()).unwrap();
    manager.create_topic("bench-topic", 1).unwrap();

    let value = Bytes::from(vec![b'x'; 100]); // 100 byte payload

    c.bench_function("single_write_100b", |b| {
        b.iter(|| {
            manager
                .append("bench-topic", 0, None, black_box(value.clone()))
                .unwrap()
        })
    });
}

/// Benchmark writes with different payload sizes
fn bench_write_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_by_size");

    for size in [100, 1_000, 10_000, 100_000].iter() {
        let dir = tempdir().unwrap();
        let manager = TopicManager::new(dir.path()).unwrap();
        manager.create_topic("bench-topic", 1).unwrap();

        let value = Bytes::from(vec![b'x'; *size]);

        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            b.iter(|| {
                manager
                    .append("bench-topic", 0, None, black_box(value.clone()))
                    .unwrap()
            })
        });
    }

    group.finish();
}

/// Benchmark batch writes (sequential writes)
fn bench_batch_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_write");

    for batch_size in [10, 100, 1000].iter() {
        let dir = tempdir().unwrap();
        let manager = TopicManager::new(dir.path()).unwrap();
        manager.create_topic("bench-topic", 1).unwrap();

        let value = Bytes::from(vec![b'x'; 100]);

        group.throughput(Throughput::Elements(*batch_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            batch_size,
            |b, &size| {
                b.iter(|| {
                    for _ in 0..size {
                        manager
                            .append("bench-topic", 0, None, black_box(value.clone()))
                            .unwrap();
                    }
                })
            },
        );
    }

    group.finish();
}

/// Benchmark reads
fn bench_read(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let manager = TopicManager::new(dir.path()).unwrap();
    manager.create_topic("bench-topic", 1).unwrap();

    // Pre-populate with 10000 records
    let value = Bytes::from(vec![b'x'; 100]);
    for _ in 0..10_000 {
        manager
            .append("bench-topic", 0, None, value.clone())
            .unwrap();
    }

    let mut group = c.benchmark_group("read");

    // Benchmark reading different batch sizes
    for batch_size in [1, 10, 100, 1000].iter() {
        group.throughput(Throughput::Elements(*batch_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            batch_size,
            |b, &size| {
                b.iter(|| black_box(manager.read("bench-topic", 0, 0, size as usize).unwrap()))
            },
        );
    }

    group.finish();
}

/// Benchmark read from different offsets
fn bench_read_offsets(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let manager = TopicManager::new(dir.path()).unwrap();
    manager.create_topic("bench-topic", 1).unwrap();

    // Pre-populate with 10000 records
    let value = Bytes::from(vec![b'x'; 100]);
    for _ in 0..10_000 {
        manager
            .append("bench-topic", 0, None, value.clone())
            .unwrap();
    }

    let mut group = c.benchmark_group("read_from_offset");

    for offset in [0, 1000, 5000, 9000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(offset), offset, |b, &off| {
            b.iter(|| black_box(manager.read("bench-topic", 0, off, 100).unwrap()))
        });
    }

    group.finish();
}

/// Benchmark topic creation
fn bench_topic_create(c: &mut Criterion) {
    c.bench_function("topic_create", |b| {
        let dir = tempdir().unwrap();
        let manager = TopicManager::new(dir.path()).unwrap();
        let mut i = 0;

        b.iter(|| {
            manager
                .create_topic(&format!("topic-{}", i), black_box(1))
                .unwrap();
            i += 1;
        })
    });
}

/// Benchmark multi-partition writes
fn bench_multi_partition_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("multi_partition_write");

    for num_partitions in [1, 4, 8, 16].iter() {
        let dir = tempdir().unwrap();
        let manager = TopicManager::new(dir.path()).unwrap();
        manager
            .create_topic("bench-topic", *num_partitions)
            .unwrap();

        let value = Bytes::from(vec![b'x'; 100]);

        group.bench_with_input(
            BenchmarkId::from_parameter(num_partitions),
            num_partitions,
            |b, &partitions| {
                let mut partition = 0i32;
                b.iter(|| {
                    manager
                        .append("bench-topic", partition, None, black_box(value.clone()))
                        .unwrap();
                    partition = (partition + 1) % partitions;
                })
            },
        );
    }

    group.finish();
}

/// Benchmark offset queries
fn bench_offset_queries(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let manager = TopicManager::new(dir.path()).unwrap();
    manager.create_topic("bench-topic", 1).unwrap();

    // Pre-populate
    let value = Bytes::from(vec![b'x'; 100]);
    for _ in 0..1000 {
        manager
            .append("bench-topic", 0, None, value.clone())
            .unwrap();
    }

    let mut group = c.benchmark_group("offset_queries");

    group.bench_function("earliest_offset", |b| {
        b.iter(|| black_box(manager.earliest_offset("bench-topic", 0).unwrap()))
    });

    group.bench_function("latest_offset", |b| {
        b.iter(|| black_box(manager.latest_offset("bench-topic", 0).unwrap()))
    });

    group.bench_function("high_watermark", |b| {
        b.iter(|| black_box(manager.high_watermark("bench-topic", 0).unwrap()))
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_single_write,
    bench_write_sizes,
    bench_batch_write,
    bench_read,
    bench_read_offsets,
    bench_topic_create,
    bench_multi_partition_write,
    bench_offset_queries,
);

criterion_main!(benches);
