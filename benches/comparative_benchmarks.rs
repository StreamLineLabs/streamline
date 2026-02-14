//! Comparative Benchmarks
//!
//! Run with: cargo bench --bench comparative_benchmarks
//!
//! This benchmark suite compares Streamline against Kafka and Redpanda
//! for various workloads.

mod comparative;

use comparative::{
    consumer_bench, e2e_latency, producer_bench, startup_bench, BenchmarkConfig, BenchmarkResult,
    BenchmarkSuite, BenchmarkTarget,
};
use criterion::{criterion_group, criterion_main, Criterion};
use std::time::Duration;

/// Run all comparative benchmarks against Streamline.
fn streamline_benchmarks(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("streamline");
    group.measurement_time(Duration::from_secs(30));
    group.sample_size(10);

    // Producer throughput
    group.bench_function("producer_1kb_6p", |b| {
        let config = producer_bench::ProducerBenchmarkPresets::medium(BenchmarkTarget::Streamline);
        b.to_async(&rt).iter(|| async {
            let short_config = BenchmarkConfig {
                duration_secs: 5,
                warmup_secs: 1,
                ..config.clone()
            };
            producer_bench::run_producer_benchmark(&short_config).await
        });
    });

    // Consumer throughput
    group.bench_function("consumer_1kb_6p", |b| {
        let config = consumer_bench::ConsumerBenchmarkPresets::medium(BenchmarkTarget::Streamline);
        b.to_async(&rt).iter(|| async {
            let short_config = BenchmarkConfig {
                duration_secs: 5,
                warmup_secs: 1,
                ..config.clone()
            };
            consumer_bench::run_consumer_benchmark(&short_config).await
        });
    });

    // E2E latency
    group.bench_function("e2e_latency_1kb", |b| {
        let config = e2e_latency::E2ELatencyPresets::quick(BenchmarkTarget::Streamline);
        b.to_async(&rt).iter(|| async {
            let short_config = BenchmarkConfig {
                duration_secs: 5,
                warmup_secs: 1,
                ..config.clone()
            };
            e2e_latency::run_e2e_latency_benchmark(&short_config).await
        });
    });

    group.finish();
}

/// Run startup time benchmarks.
fn startup_benchmarks(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("startup");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(5);

    group.bench_function("streamline_cold_start", |b| {
        b.to_async(&rt).iter(|| async {
            let config = startup_bench::StartupBenchmarkConfig {
                target: BenchmarkTarget::Streamline,
                num_runs: 1,
                ..Default::default()
            };
            startup_bench::run_startup_benchmark(&config).await
        });
    });

    group.finish();
}

criterion_group!(benches, streamline_benchmarks, startup_benchmarks);
criterion_main!(benches);

/// Run the full comparative benchmark suite (not via criterion).
///
/// Usage: cargo run --release --example run_comparative_benchmarks
#[allow(dead_code)]
async fn run_full_suite() -> BenchmarkSuite {
    let mut suite = BenchmarkSuite::new();

    let targets = [
        BenchmarkTarget::Streamline,
        // BenchmarkTarget::Kafka,    // Uncomment when Kafka is available
        // BenchmarkTarget::Redpanda, // Uncomment when Redpanda is available
    ];

    for target in targets {
        tracing::info!("Running benchmarks for {:?}", target);

        // Producer benchmark
        let config = producer_bench::ProducerBenchmarkPresets::medium(target);
        let result = producer_bench::run_producer_benchmark(&config).await;
        suite.add_result(result);

        // Consumer benchmark
        let config = consumer_bench::ConsumerBenchmarkPresets::medium(target);
        let result = consumer_bench::run_consumer_benchmark(&config).await;
        suite.add_result(result);

        // E2E latency benchmark
        let config = e2e_latency::E2ELatencyPresets::standard(target);
        let result = e2e_latency::run_e2e_latency_benchmark(&config).await;
        suite.add_result(result);

        // Startup benchmark
        let config = startup_bench::StartupBenchmarkConfig {
            target,
            num_runs: 3,
            ..Default::default()
        };
        let startup_result = startup_bench::run_startup_benchmark(&config).await;

        // Convert startup result to standard BenchmarkResult
        let mut result = BenchmarkResult::new(target, "Startup Time", BenchmarkConfig::default());
        result.latency_p50_ms = startup_result.port_ready_ms;
        result.latency_p99_ms = startup_result.first_produce_ms;
        suite.add_result(result);
    }

    suite
}
