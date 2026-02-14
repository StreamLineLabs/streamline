// Comparative benchmark scaffolding (not yet fully integrated)
#![allow(dead_code)]

//! Comparative Benchmark Harness
//!
//! This module provides a common framework for running benchmarks against
//! Streamline, Kafka, and Redpanda to generate comparative performance data.

pub mod consumer_bench;
pub mod e2e_latency;
pub mod producer_bench;
pub mod startup_bench;

use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};

/// Configuration for a benchmark run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkConfig {
    /// Message size in bytes
    pub message_size: usize,

    /// Number of messages per batch
    pub batch_size: usize,

    /// Number of partitions for the test topic
    pub num_partitions: i32,

    /// Benchmark duration in seconds
    pub duration_secs: u64,

    /// Warmup duration in seconds (not counted in results)
    pub warmup_secs: u64,

    /// Number of producer threads/tasks
    pub num_producers: usize,

    /// Number of consumer threads/tasks
    pub num_consumers: usize,

    /// Target system (streamline, kafka, redpanda)
    pub target: BenchmarkTarget,

    /// Broker address
    pub broker_addr: String,
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        Self {
            message_size: 1024,
            batch_size: 100,
            num_partitions: 6,
            duration_secs: 60,
            warmup_secs: 10,
            num_producers: 1,
            num_consumers: 1,
            target: BenchmarkTarget::Streamline,
            broker_addr: "localhost:9092".to_string(),
        }
    }
}

/// Supported benchmark targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BenchmarkTarget {
    Streamline,
    Kafka,
    Redpanda,
}

impl std::fmt::Display for BenchmarkTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BenchmarkTarget::Streamline => write!(f, "streamline"),
            BenchmarkTarget::Kafka => write!(f, "kafka"),
            BenchmarkTarget::Redpanda => write!(f, "redpanda"),
        }
    }
}

/// Results from a benchmark run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkResult {
    /// Target system that was benchmarked
    pub target: BenchmarkTarget,

    /// Type of benchmark (producer, consumer, e2e, startup)
    pub benchmark_type: String,

    /// Messages per second throughput
    pub throughput_msgs_sec: f64,

    /// Megabytes per second throughput
    pub throughput_mb_sec: f64,

    /// 50th percentile latency in milliseconds
    pub latency_p50_ms: f64,

    /// 99th percentile latency in milliseconds
    pub latency_p99_ms: f64,

    /// 99.9th percentile latency in milliseconds
    pub latency_p999_ms: f64,

    /// Total messages processed
    pub total_messages: u64,

    /// Total bytes processed
    pub total_bytes: u64,

    /// Actual benchmark duration in seconds
    pub duration_secs: f64,

    /// Configuration used for this benchmark
    pub config: BenchmarkConfig,

    /// Timestamp when benchmark was run (ISO 8601)
    pub timestamp: String,
}

impl BenchmarkResult {
    /// Create a new benchmark result.
    pub fn new(target: BenchmarkTarget, benchmark_type: &str, config: BenchmarkConfig) -> Self {
        Self {
            target,
            benchmark_type: benchmark_type.to_string(),
            throughput_msgs_sec: 0.0,
            throughput_mb_sec: 0.0,
            latency_p50_ms: 0.0,
            latency_p99_ms: 0.0,
            latency_p999_ms: 0.0,
            total_messages: 0,
            total_bytes: 0,
            duration_secs: 0.0,
            config,
            timestamp: chrono::Utc::now().to_rfc3339(),
        }
    }

    /// Calculate throughput metrics from collected data.
    pub fn calculate_throughput(&mut self, messages: u64, bytes: u64, duration: Duration) {
        self.total_messages = messages;
        self.total_bytes = bytes;
        self.duration_secs = duration.as_secs_f64();

        if self.duration_secs > 0.0 {
            self.throughput_msgs_sec = messages as f64 / self.duration_secs;
            self.throughput_mb_sec = (bytes as f64 / 1_048_576.0) / self.duration_secs;
        }
    }

    /// Calculate latency percentiles from collected latencies.
    pub fn calculate_latencies(&mut self, mut latencies_ms: Vec<f64>) {
        if latencies_ms.is_empty() {
            return;
        }

        latencies_ms.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let len = latencies_ms.len();
        self.latency_p50_ms = latencies_ms[len * 50 / 100];
        self.latency_p99_ms = latencies_ms[len * 99 / 100];
        self.latency_p999_ms = latencies_ms[len.saturating_sub(1).min(len * 999 / 1000)];
    }
}

/// Latency tracker for collecting timing measurements.
pub struct LatencyTracker {
    samples: Vec<f64>,
    start_time: Option<Instant>,
}

impl LatencyTracker {
    /// Create a new latency tracker.
    pub fn new() -> Self {
        Self {
            samples: Vec::with_capacity(100_000),
            start_time: None,
        }
    }

    /// Start timing an operation.
    pub fn start(&mut self) {
        self.start_time = Some(Instant::now());
    }

    /// Stop timing and record the latency.
    pub fn stop(&mut self) {
        if let Some(start) = self.start_time.take() {
            let latency_ms = start.elapsed().as_secs_f64() * 1000.0;
            self.samples.push(latency_ms);
        }
    }

    /// Record a latency directly (in milliseconds).
    pub fn record(&mut self, latency_ms: f64) {
        self.samples.push(latency_ms);
    }

    /// Get all collected samples.
    pub fn samples(&self) -> Vec<f64> {
        self.samples.clone()
    }
}

impl Default for LatencyTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Generate a test message of the specified size.
pub fn generate_message(size: usize) -> Vec<u8> {
    let mut msg = vec![0u8; size];
    // Fill with predictable pattern for validation
    for (i, byte) in msg.iter_mut().enumerate() {
        *byte = (i % 256) as u8;
    }
    msg
}

/// Format a benchmark result as a markdown table row.
pub fn format_result_row(result: &BenchmarkResult) -> String {
    format!(
        "| {} | {:.0} | {:.2} | {:.2} ms | {:.2} ms |",
        result.target,
        result.throughput_msgs_sec,
        result.throughput_mb_sec,
        result.latency_p50_ms,
        result.latency_p99_ms
    )
}

/// Benchmark suite for running multiple benchmarks.
pub struct BenchmarkSuite {
    results: Vec<BenchmarkResult>,
}

impl BenchmarkSuite {
    /// Create a new benchmark suite.
    pub fn new() -> Self {
        Self {
            results: Vec::new(),
        }
    }

    /// Add a result to the suite.
    pub fn add_result(&mut self, result: BenchmarkResult) {
        self.results.push(result);
    }

    /// Get all results.
    pub fn results(&self) -> &[BenchmarkResult] {
        &self.results
    }

    /// Export results to JSON.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(&self.results)
    }

    /// Generate a markdown report.
    pub fn to_markdown(&self) -> String {
        let mut report = String::new();

        // Group results by benchmark type
        let mut grouped: std::collections::HashMap<String, Vec<&BenchmarkResult>> =
            std::collections::HashMap::new();

        for result in &self.results {
            grouped
                .entry(result.benchmark_type.clone())
                .or_default()
                .push(result);
        }

        for (benchmark_type, results) in grouped {
            report.push_str(&format!("### {}\n\n", benchmark_type));
            report.push_str("| System | msgs/sec | MB/sec | p50 Latency | p99 Latency |\n");
            report.push_str("|--------|----------|--------|-------------|-------------|\n");

            for result in results {
                report.push_str(&format_result_row(result));
                report.push('\n');
            }

            report.push('\n');
        }

        report
    }
}

impl Default for BenchmarkSuite {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::*;

    #[test]
    fn test_generate_message() {
        let msg = generate_message(100);
        assert_eq!(msg.len(), 100);
        assert_eq!(msg[0], 0);
        assert_eq!(msg[99], 99);
    }

    #[test]
    fn test_latency_tracker() {
        let mut tracker = LatencyTracker::new();
        tracker.record(1.0);
        tracker.record(2.0);
        tracker.record(3.0);
        assert_eq!(tracker.samples().len(), 3);
    }

    #[test]
    fn test_benchmark_result_throughput() {
        let config = BenchmarkConfig::default();
        let mut result = BenchmarkResult::new(BenchmarkTarget::Streamline, "producer", config);
        result.calculate_throughput(1000, 1_000_000, Duration::from_secs(1));
        assert_eq!(result.throughput_msgs_sec, 1000.0);
        assert!((result.throughput_mb_sec - 0.953674).abs() < 0.001);
    }
}
