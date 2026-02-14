// E2E latency benchmark scaffolding (not yet fully integrated)
#![allow(dead_code)]

//! End-to-End Latency Benchmark
//!
//! Measures the time from produce to consume (round-trip latency).

use super::{BenchmarkConfig, BenchmarkResult, BenchmarkTarget, LatencyTracker};
use std::time::{Duration, Instant};

/// Run the end-to-end latency benchmark.
///
/// This benchmark measures the round-trip time from when a message is
/// produced until it can be consumed. This is the most meaningful latency
/// metric for real-time streaming applications.
pub async fn run_e2e_latency_benchmark(config: &BenchmarkConfig) -> BenchmarkResult {
    let mut result = BenchmarkResult::new(config.target, "End-to-End Latency", config.clone());

    let message = super::generate_message(config.message_size);
    let mut latency_tracker = LatencyTracker::new();
    let mut total_messages: u64 = 0;
    let mut total_bytes: u64 = 0;

    // Warmup phase
    tracing::info!(
        "Starting E2E warmup phase ({} seconds) for {:?}",
        config.warmup_secs,
        config.target
    );

    let warmup_deadline = Instant::now() + Duration::from_secs(config.warmup_secs);
    while Instant::now() < warmup_deadline {
        let _ = produce_and_consume(&config.broker_addr, &message).await;
    }

    // Benchmark phase
    tracing::info!(
        "Starting E2E benchmark phase ({} seconds) for {:?}",
        config.duration_secs,
        config.target
    );

    let start = Instant::now();
    let deadline = start + Duration::from_secs(config.duration_secs);

    while Instant::now() < deadline {
        let round_trip_start = Instant::now();
        let bytes = produce_and_consume(&config.broker_addr, &message).await;
        let latency_ms = round_trip_start.elapsed().as_secs_f64() * 1000.0;

        latency_tracker.record(latency_ms);
        total_messages += 1;
        total_bytes += bytes as u64;
    }

    let duration = start.elapsed();
    result.calculate_throughput(total_messages, total_bytes, duration);
    result.calculate_latencies(latency_tracker.samples());

    tracing::info!(
        "E2E benchmark complete: p50={:.2}ms, p99={:.2}ms, p999={:.2}ms",
        result.latency_p50_ms,
        result.latency_p99_ms,
        result.latency_p999_ms
    );

    result
}

/// Produce a message and immediately consume it (placeholder implementation).
///
/// Returns the number of bytes transferred.
async fn produce_and_consume(_broker: &str, message: &[u8]) -> usize {
    // Simulate produce
    tokio::time::sleep(Duration::from_micros(50)).await;

    // Simulate consume
    tokio::time::sleep(Duration::from_micros(50)).await;

    message.len()
}

/// E2E latency benchmark configuration presets.
pub struct E2ELatencyPresets;

impl E2ELatencyPresets {
    /// Quick latency test with small messages.
    pub fn quick(target: BenchmarkTarget) -> BenchmarkConfig {
        BenchmarkConfig {
            message_size: 100,
            batch_size: 1, // Single messages for accurate latency
            num_partitions: 1,
            duration_secs: 60,
            warmup_secs: 10,
            num_producers: 1,
            num_consumers: 1,
            target,
            broker_addr: Self::default_broker(target),
        }
    }

    /// Standard latency test with typical message sizes.
    pub fn standard(target: BenchmarkTarget) -> BenchmarkConfig {
        BenchmarkConfig {
            message_size: 1024,
            batch_size: 1,
            num_partitions: 1,
            duration_secs: 300,
            warmup_secs: 30,
            num_producers: 1,
            num_consumers: 1,
            target,
            broker_addr: Self::default_broker(target),
        }
    }

    /// Extended latency test for percentile accuracy.
    pub fn extended(target: BenchmarkTarget) -> BenchmarkConfig {
        BenchmarkConfig {
            message_size: 1024,
            batch_size: 1,
            num_partitions: 1,
            duration_secs: 600,
            warmup_secs: 60,
            num_producers: 1,
            num_consumers: 1,
            target,
            broker_addr: Self::default_broker(target),
        }
    }

    fn default_broker(target: BenchmarkTarget) -> String {
        match target {
            BenchmarkTarget::Streamline => "localhost:9092".to_string(),
            BenchmarkTarget::Kafka => "localhost:9093".to_string(),
            BenchmarkTarget::Redpanda => "localhost:9094".to_string(),
        }
    }
}
