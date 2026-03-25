//! M3 Edge Performance Benchmarks
//!
//! Measures:
//! - Local read latency (target: p99 < 5ms)
//! - Sync round-trip (simulated)
//!
//! Run: `cargo bench --bench m3_edge_perf --features edge`

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use streamline::edge::EdgeRuntime;

fn setup_runtime_with_data(topic: &str, record_count: usize) -> EdgeRuntime {
    let runtime = EdgeRuntime::in_memory().expect("failed to create edge runtime");
    runtime.create_topic(topic, 1).expect("failed to create topic");
    for i in 0..record_count {
        let key = format!("key-{}", i);
        let value = format!("value-{}-{}", i, "x".repeat(100));
        runtime.produce(topic, key.as_bytes(), value.as_bytes())
            .expect("failed to produce");
    }
    runtime
}

fn bench_local_read_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("edge_local_read");

    for count in [100, 1_000, 10_000].iter() {
        let runtime = setup_runtime_with_data("bench-reads", *count);
        let mid_offset = (*count as i64) / 2;

        group.bench_with_input(
            BenchmarkId::new("single_read", count),
            count,
            |b, _| {
                b.iter(|| {
                    let records = runtime
                        .consume(black_box("bench-reads"), 0, black_box(mid_offset), 1)
                        .expect("consume failed");
                    black_box(records);
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("batch_read_100", count),
            count,
            |b, _| {
                b.iter(|| {
                    let records = runtime
                        .consume(black_box("bench-reads"), 0, 0, black_box(100))
                        .expect("consume failed");
                    black_box(records);
                });
            },
        );
    }

    group.finish();
}

fn bench_local_write_latency(c: &mut Criterion) {
    let runtime = EdgeRuntime::in_memory().expect("failed to create edge runtime");
    runtime.create_topic("bench-writes", 1).expect("failed to create topic");

    let mut counter = 0u64;
    c.bench_function("edge_local_write", |b| {
        b.iter(|| {
            counter += 1;
            let key = format!("k-{}", counter);
            let value = format!("v-{}", counter);
            let offset = runtime
                .produce(black_box("bench-writes"), key.as_bytes(), value.as_bytes())
                .expect("produce failed");
            black_box(offset);
        });
    });
}

fn bench_simulated_sync_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("edge_sync_roundtrip");

    for batch_size in [10, 100, 1_000].iter() {
        group.bench_with_input(
            BenchmarkId::new("serialize_batch", batch_size),
            batch_size,
            |b, &size| {
                // Simulate serialization of a sync batch
                let records: Vec<(Vec<u8>, Vec<u8>)> = (0..size)
                    .map(|i| {
                        (
                            format!("key-{}", i).into_bytes(),
                            format!("value-{}-{}", i, "x".repeat(100)).into_bytes(),
                        )
                    })
                    .collect();

                b.iter(|| {
                    let mut total_bytes = 0usize;
                    for (k, v) in &records {
                        total_bytes += k.len() + v.len();
                    }
                    black_box(total_bytes);
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_local_read_latency,
    bench_local_write_latency,
    bench_simulated_sync_roundtrip,
);
criterion_main!(benches);
