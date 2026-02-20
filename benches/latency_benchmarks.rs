//! Latency distribution benchmarks for Streamline
//!
//! Run with: cargo bench --bench latency_benchmarks
//!
//! Measures end-to-end produce→read latency at various percentiles (p50, p95, p99).
//! Unlike throughput benchmarks, these measure individual operation latency
//! to characterize tail latency behavior.

use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::time::Instant;
use streamline::TopicManager;
use tempfile::tempdir;

/// Collect latency samples and report percentiles
fn percentile(samples: &mut Vec<u64>, p: f64) -> u64 {
    samples.sort_unstable();
    let idx = ((p / 100.0) * samples.len() as f64).ceil() as usize;
    samples[idx.min(samples.len() - 1)]
}

/// Benchmark single-message produce latency distribution
fn bench_produce_latency(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let manager = TopicManager::new(dir.path()).unwrap();
    manager.create_topic("latency-topic", 1).unwrap();

    let payload = Bytes::from(vec![b'x'; 1024]); // 1 KB message

    c.bench_function("produce_latency_1kb", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let start = Instant::now();
                manager
                    .append("latency-topic", 0, None, black_box(payload.clone()))
                    .unwrap();
                total += start.elapsed();
            }
            total
        })
    });
}

/// Benchmark produce→read round-trip latency distribution
fn bench_produce_read_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("produce_read_latency");

    for size in [100, 1_024, 10_240] {
        let dir = tempdir().unwrap();
        let manager = TopicManager::new(dir.path()).unwrap();
        manager.create_topic("latency-rt", 1).unwrap();
        let payload = Bytes::from(vec![b'x'; size]);

        group.bench_function(format!("round_trip_{}b", size), |b| {
            b.iter_custom(|iters| {
                let mut total = std::time::Duration::ZERO;
                for _ in 0..iters {
                    let start = Instant::now();
                    let offset = manager
                        .append("latency-rt", 0, None, black_box(payload.clone()))
                        .unwrap();
                    let _records = manager
                        .read("latency-rt", 0, offset, 1)
                        .unwrap();
                    total += start.elapsed();
                }
                total
            })
        });
    }

    group.finish();
}

/// Measure raw latency percentiles and report them
/// This benchmark captures individual samples and reports p50/p95/p99
fn bench_latency_percentiles(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let manager = TopicManager::new(dir.path()).unwrap();
    manager.create_topic("pct-topic", 1).unwrap();

    let payload = Bytes::from(vec![b'x'; 1024]);
    let num_samples = 10_000;

    c.bench_function("latency_percentiles_1kb", |b| {
        b.iter(|| {
            let mut samples = Vec::with_capacity(num_samples);

            for _ in 0..num_samples {
                let start = Instant::now();
                let offset = manager
                    .append("pct-topic", 0, None, black_box(payload.clone()))
                    .unwrap();
                let _records = manager
                    .read("pct-topic", 0, offset, 1)
                    .unwrap();
                samples.push(start.elapsed().as_nanos() as u64);
            }

            let p50 = percentile(&mut samples, 50.0);
            let p95 = percentile(&mut samples, 95.0);
            let p99 = percentile(&mut samples, 99.0);
            let p999 = percentile(&mut samples, 99.9);

            black_box((p50, p95, p99, p999))
        })
    });
}

/// Benchmark latency under increasing concurrent write pressure
fn bench_latency_under_load(c: &mut Criterion) {
    let mut group = c.benchmark_group("latency_under_load");

    for partitions in [1, 4, 12] {
        let dir = tempdir().unwrap();
        let manager = TopicManager::new(dir.path()).unwrap();
        manager
            .create_topic_with_partitions("load-topic", partitions)
            .unwrap();

        let payload = Bytes::from(vec![b'x'; 1024]);

        // Pre-fill with data to simulate sustained load
        for i in 0..1000 {
            let partition = i % partitions;
            manager
                .append("load-topic", partition, None, payload.clone())
                .unwrap();
        }

        group.bench_function(format!("produce_{}p", partitions), |b| {
            b.iter_custom(|iters| {
                let mut total = std::time::Duration::ZERO;
                for i in 0..iters {
                    let partition = (i as i32) % partitions;
                    let start = Instant::now();
                    manager
                        .append("load-topic", partition, None, black_box(payload.clone()))
                        .unwrap();
                    total += start.elapsed();
                }
                total
            })
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_produce_latency,
    bench_produce_read_latency,
    bench_latency_percentiles,
    bench_latency_under_load,
);

criterion_main!(benches);
