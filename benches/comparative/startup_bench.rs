// Startup benchmark scaffolding (not yet fully integrated)
#![allow(dead_code)]
#![allow(unused_imports)]

//! Startup Time Benchmark
//!
//! Measures cold start time and time to first successful request.

use super::{BenchmarkResult, BenchmarkTarget};
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};

/// Configuration for startup benchmark.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartupBenchmarkConfig {
    /// Target system
    pub target: BenchmarkTarget,

    /// Path to the binary (for Streamline)
    pub binary_path: String,

    /// Additional arguments to pass
    pub args: Vec<String>,

    /// Data directory (to test cold vs warm starts)
    pub data_dir: String,

    /// Size of data to preload (in GB) for recovery time tests
    pub preload_data_gb: f64,

    /// Number of runs to average
    pub num_runs: usize,

    /// Timeout for startup (seconds)
    pub timeout_secs: u64,

    /// Port to check for readiness
    pub port: u16,
}

impl Default for StartupBenchmarkConfig {
    fn default() -> Self {
        Self {
            target: BenchmarkTarget::Streamline,
            binary_path: "./target/release/streamline".to_string(),
            args: vec![],
            data_dir: "./bench_data".to_string(),
            preload_data_gb: 0.0,
            num_runs: 3,
            timeout_secs: 60,
            port: 9092,
        }
    }
}

/// Results from a startup benchmark.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartupResult {
    /// Target system
    pub target: BenchmarkTarget,

    /// Time to process start (binary begins execution)
    pub process_start_ms: f64,

    /// Time to port open (accepting connections)
    pub port_ready_ms: f64,

    /// Time to first successful produce request
    pub first_produce_ms: f64,

    /// Time to first successful fetch request
    pub first_fetch_ms: f64,

    /// Resident memory at idle (MB)
    pub memory_idle_mb: f64,

    /// Data size used for recovery test (GB)
    pub data_size_gb: f64,

    /// Recovery time with data (if applicable)
    pub recovery_time_ms: Option<f64>,

    /// Standard deviation across runs
    pub std_dev_ms: f64,

    /// Number of runs
    pub num_runs: usize,
}

/// Run the startup benchmark.
pub async fn run_startup_benchmark(config: &StartupBenchmarkConfig) -> StartupResult {
    let mut results = Vec::with_capacity(config.num_runs);

    for run in 0..config.num_runs {
        tracing::info!(
            "Startup benchmark run {}/{} for {:?}",
            run + 1,
            config.num_runs,
            config.target
        );

        let run_result = measure_single_startup(config).await;
        results.push(run_result);

        // Brief pause between runs
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    aggregate_startup_results(config.target, &results)
}

/// Measure a single startup cycle.
async fn measure_single_startup(config: &StartupBenchmarkConfig) -> SingleStartupMeasurement {
    let start = Instant::now();

    // In a real implementation, this would:
    // 1. Start the process
    // 2. Monitor for port availability
    // 3. Send a test produce request
    // 4. Send a test fetch request
    // 5. Measure memory usage

    // Simulated measurements for placeholder
    let process_start = Duration::from_millis(10);
    let port_ready = Duration::from_millis(150);
    let first_produce = Duration::from_millis(200);
    let first_fetch = Duration::from_millis(250);

    // Simulate startup time based on target
    let base_delay = match config.target {
        BenchmarkTarget::Streamline => Duration::from_millis(100),
        BenchmarkTarget::Kafka => Duration::from_millis(3000),
        BenchmarkTarget::Redpanda => Duration::from_millis(500),
    };

    tokio::time::sleep(base_delay).await;

    SingleStartupMeasurement {
        process_start_ms: process_start.as_secs_f64() * 1000.0,
        port_ready_ms: port_ready.as_secs_f64() * 1000.0 + base_delay.as_secs_f64() * 1000.0,
        first_produce_ms: first_produce.as_secs_f64() * 1000.0 + base_delay.as_secs_f64() * 1000.0,
        first_fetch_ms: first_fetch.as_secs_f64() * 1000.0 + base_delay.as_secs_f64() * 1000.0,
        memory_mb: match config.target {
            BenchmarkTarget::Streamline => 25.0,
            BenchmarkTarget::Kafka => 512.0,
            BenchmarkTarget::Redpanda => 128.0,
        },
        total_ms: start.elapsed().as_secs_f64() * 1000.0,
    }
}

struct SingleStartupMeasurement {
    process_start_ms: f64,
    port_ready_ms: f64,
    first_produce_ms: f64,
    first_fetch_ms: f64,
    memory_mb: f64,
    total_ms: f64,
}

fn aggregate_startup_results(
    target: BenchmarkTarget,
    measurements: &[SingleStartupMeasurement],
) -> StartupResult {
    let n = measurements.len() as f64;

    let avg_port_ready = measurements.iter().map(|m| m.port_ready_ms).sum::<f64>() / n;
    let avg_first_produce = measurements.iter().map(|m| m.first_produce_ms).sum::<f64>() / n;
    let avg_first_fetch = measurements.iter().map(|m| m.first_fetch_ms).sum::<f64>() / n;
    let avg_memory = measurements.iter().map(|m| m.memory_mb).sum::<f64>() / n;

    // Calculate standard deviation of port_ready_ms
    let variance = measurements
        .iter()
        .map(|m| (m.port_ready_ms - avg_port_ready).powi(2))
        .sum::<f64>()
        / n;
    let std_dev = variance.sqrt();

    StartupResult {
        target,
        process_start_ms: measurements
            .first()
            .map(|m| m.process_start_ms)
            .unwrap_or(0.0),
        port_ready_ms: avg_port_ready,
        first_produce_ms: avg_first_produce,
        first_fetch_ms: avg_first_fetch,
        memory_idle_mb: avg_memory,
        data_size_gb: 0.0,
        recovery_time_ms: None,
        std_dev_ms: std_dev,
        num_runs: measurements.len(),
    }
}

/// Check if a port is accepting connections.
pub async fn wait_for_port(port: u16, timeout: Duration) -> Result<Duration, &'static str> {
    let start = Instant::now();
    let deadline = start + timeout;

    while Instant::now() < deadline {
        match tokio::net::TcpStream::connect(format!("127.0.0.1:{}", port)).await {
            Ok(_) => return Ok(start.elapsed()),
            Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
        }
    }

    Err("Timeout waiting for port")
}

/// Format startup results as markdown table.
pub fn format_startup_table(results: &[StartupResult]) -> String {
    let mut table = String::new();
    table.push_str("| System | Port Ready | First Produce | First Fetch | Memory (Idle) |\n");
    table.push_str("|--------|------------|---------------|-------------|---------------|\n");

    for result in results {
        table.push_str(&format!(
            "| {} | {:.0} ms | {:.0} ms | {:.0} ms | {:.0} MB |\n",
            result.target,
            result.port_ready_ms,
            result.first_produce_ms,
            result.first_fetch_ms,
            result.memory_idle_mb
        ));
    }

    table
}
