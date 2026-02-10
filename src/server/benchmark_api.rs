//! REST API for benchmarking Kafka operations
//!
//! This module provides endpoints for running produce/consume benchmarks.
//!
//! ## Endpoints
//!
//! - `POST /api/v1/benchmark/produce` - Start a produce benchmark
//! - `POST /api/v1/benchmark/consume` - Start a consume benchmark
//! - `GET /api/v1/benchmark/:id/status` - Get benchmark status
//! - `GET /api/v1/benchmark/:id/results` - Get benchmark results
//! - `GET /api/v1/benchmark` - List all benchmarks

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

/// Shared state for benchmark API
#[derive(Clone)]
pub(crate) struct BenchmarkApiState {
    /// Benchmark store
    pub store: Arc<BenchmarkStore>,
}

/// Store for benchmark configurations and results
pub struct BenchmarkStore {
    /// Active and completed benchmarks
    benchmarks: RwLock<HashMap<String, Benchmark>>,
}

impl BenchmarkStore {
    /// Create a new benchmark store
    pub fn new() -> Self {
        Self {
            benchmarks: RwLock::new(HashMap::new()),
        }
    }

    /// Create a new shared benchmark store
    pub fn new_shared() -> Arc<Self> {
        Arc::new(Self::new())
    }

    /// Start a new benchmark
    pub fn start_benchmark(
        &self,
        config: BenchmarkConfig,
        benchmark_type: BenchmarkType,
    ) -> Benchmark {
        let id = Uuid::new_v4().to_string();
        let benchmark = Benchmark {
            id: id.clone(),
            config,
            benchmark_type,
            status: BenchmarkStatus::Running,
            progress: BenchmarkProgress {
                messages_processed: 0,
                bytes_processed: 0,
                elapsed_ms: 0,
                percent_complete: 0.0,
            },
            results: None,
            started_at: chrono::Utc::now().timestamp_millis() as u64,
            completed_at: None,
        };

        self.benchmarks.write().insert(id, benchmark.clone());
        benchmark
    }

    /// Get a benchmark by ID
    pub fn get_benchmark(&self, id: &str) -> Option<Benchmark> {
        self.benchmarks.read().get(id).cloned()
    }

    /// Update benchmark progress
    pub fn update_progress(&self, id: &str, progress: BenchmarkProgress) {
        if let Some(benchmark) = self.benchmarks.write().get_mut(id) {
            benchmark.progress = progress;
        }
    }

    /// Complete a benchmark with results
    pub fn complete_benchmark(&self, id: &str, results: BenchmarkResults) {
        if let Some(benchmark) = self.benchmarks.write().get_mut(id) {
            benchmark.status = BenchmarkStatus::Completed;
            benchmark.results = Some(results);
            benchmark.completed_at = Some(chrono::Utc::now().timestamp_millis() as u64);
        }
    }

    /// Mark a benchmark as failed
    pub fn fail_benchmark(&self, id: &str, error: String) {
        if let Some(benchmark) = self.benchmarks.write().get_mut(id) {
            benchmark.status = BenchmarkStatus::Failed(error);
            benchmark.completed_at = Some(chrono::Utc::now().timestamp_millis() as u64);
        }
    }

    /// List all benchmarks
    pub fn list_benchmarks(&self) -> Vec<Benchmark> {
        self.benchmarks.read().values().cloned().collect()
    }
}

impl Default for BenchmarkStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Benchmark configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkConfig {
    /// Topic to benchmark
    pub topic: String,
    /// Message size in bytes
    #[serde(default = "default_message_size")]
    pub message_size: usize,
    /// Number of messages to send/receive
    #[serde(default = "default_message_count")]
    pub message_count: u64,
    /// Rate limit in messages per second (None = unlimited)
    #[serde(default)]
    pub rate_limit: Option<u64>,
    /// Number of partitions to use (produce only)
    #[serde(default)]
    pub num_partitions: Option<i32>,
    /// Consumer group (consume only)
    #[serde(default)]
    pub consumer_group: Option<String>,
}

fn default_message_size() -> usize {
    1024
}

fn default_message_count() -> u64 {
    10000
}

/// Benchmark type
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum BenchmarkType {
    Produce,
    Consume,
}

/// Benchmark status
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "state", content = "error")]
pub enum BenchmarkStatus {
    #[serde(rename = "pending")]
    Pending,
    #[serde(rename = "running")]
    Running,
    #[serde(rename = "completed")]
    Completed,
    #[serde(rename = "failed")]
    Failed(String),
}

/// Benchmark progress
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkProgress {
    /// Messages processed so far
    pub messages_processed: u64,
    /// Bytes processed so far
    pub bytes_processed: u64,
    /// Elapsed time in milliseconds
    pub elapsed_ms: u64,
    /// Percent complete (0-100)
    pub percent_complete: f64,
}

/// Benchmark results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkResults {
    /// Total duration in milliseconds
    pub duration_ms: u64,
    /// Total messages sent/received
    pub messages_total: u64,
    /// Total bytes sent/received
    pub bytes_total: u64,
    /// Throughput in messages per second
    pub throughput_msg_sec: f64,
    /// Throughput in MB per second
    pub throughput_mb_sec: f64,
    /// Latency p50 in milliseconds
    pub latency_p50_ms: f64,
    /// Latency p95 in milliseconds
    pub latency_p95_ms: f64,
    /// Latency p99 in milliseconds
    pub latency_p99_ms: f64,
    /// Average latency in milliseconds
    pub latency_avg_ms: f64,
    /// Number of errors
    pub errors: u64,
}

/// Benchmark data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Benchmark {
    /// Unique benchmark ID
    pub id: String,
    /// Benchmark configuration
    pub config: BenchmarkConfig,
    /// Benchmark type
    pub benchmark_type: BenchmarkType,
    /// Current status
    pub status: BenchmarkStatus,
    /// Progress information
    pub progress: BenchmarkProgress,
    /// Results (when completed)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub results: Option<BenchmarkResults>,
    /// Start timestamp (ms since epoch)
    pub started_at: u64,
    /// Completion timestamp (ms since epoch)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<u64>,
}

/// Response for list benchmarks
#[derive(Debug, Serialize)]
pub struct ListBenchmarksResponse {
    pub benchmarks: Vec<Benchmark>,
    pub total: usize,
}

/// Error response
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

/// Create the benchmark API router
pub(crate) fn create_benchmark_api_router(state: BenchmarkApiState) -> Router {
    Router::new()
        .route("/api/v1/benchmark", get(list_benchmarks))
        .route("/api/v1/benchmark/produce", post(start_produce_benchmark))
        .route("/api/v1/benchmark/consume", post(start_consume_benchmark))
        .route("/api/v1/benchmark/:id/status", get(get_benchmark_status))
        .route("/api/v1/benchmark/:id/results", get(get_benchmark_results))
        .with_state(state)
}

/// List all benchmarks
async fn list_benchmarks(State(state): State<BenchmarkApiState>) -> Json<ListBenchmarksResponse> {
    let benchmarks = state.store.list_benchmarks();
    let total = benchmarks.len();
    Json(ListBenchmarksResponse { benchmarks, total })
}

/// Start a produce benchmark
async fn start_produce_benchmark(
    State(state): State<BenchmarkApiState>,
    Json(config): Json<BenchmarkConfig>,
) -> (StatusCode, Json<Benchmark>) {
    let benchmark = state
        .store
        .start_benchmark(config.clone(), BenchmarkType::Produce);
    let id = benchmark.id.clone();
    let store = state.store.clone();

    // Run benchmark in background
    tokio::spawn(async move {
        run_produce_benchmark(store, id, config).await;
    });

    (StatusCode::ACCEPTED, Json(benchmark))
}

/// Start a consume benchmark
async fn start_consume_benchmark(
    State(state): State<BenchmarkApiState>,
    Json(config): Json<BenchmarkConfig>,
) -> (StatusCode, Json<Benchmark>) {
    let benchmark = state
        .store
        .start_benchmark(config.clone(), BenchmarkType::Consume);
    let id = benchmark.id.clone();
    let store = state.store.clone();

    // Run benchmark in background
    tokio::spawn(async move {
        run_consume_benchmark(store, id, config).await;
    });

    (StatusCode::ACCEPTED, Json(benchmark))
}

/// Get benchmark status
async fn get_benchmark_status(
    State(state): State<BenchmarkApiState>,
    Path(id): Path<String>,
) -> Result<Json<Benchmark>, (StatusCode, Json<ErrorResponse>)> {
    state.store.get_benchmark(&id).map(Json).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Benchmark '{}' not found", id),
            }),
        )
    })
}

/// Get benchmark results
async fn get_benchmark_results(
    State(state): State<BenchmarkApiState>,
    Path(id): Path<String>,
) -> Result<Json<BenchmarkResults>, (StatusCode, Json<ErrorResponse>)> {
    let benchmark = state.store.get_benchmark(&id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Benchmark '{}' not found", id),
            }),
        )
    })?;

    benchmark.results.map(Json).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: "Benchmark results not yet available".to_string(),
            }),
        )
    })
}

/// Run a produce benchmark
async fn run_produce_benchmark(store: Arc<BenchmarkStore>, id: String, config: BenchmarkConfig) {
    let start = Instant::now();
    let mut latencies: Vec<f64> = Vec::with_capacity(config.message_count as usize);
    let errors = 0u64;
    // message_data would be used in a real implementation
    let _message_data = vec![b'x'; config.message_size];

    // Calculate rate limiting delay
    let delay = config
        .rate_limit
        .map(|rate| Duration::from_secs_f64(1.0 / rate as f64));

    for i in 0..config.message_count {
        let op_start = Instant::now();

        // Simulate message production (in a real implementation, this would use the actual Kafka producer)
        // For now, we just simulate the latency
        tokio::time::sleep(Duration::from_micros(100)).await;

        let latency_ms = op_start.elapsed().as_secs_f64() * 1000.0;
        latencies.push(latency_ms);

        // Update progress periodically
        if i % 100 == 0 || i == config.message_count - 1 {
            let elapsed = start.elapsed();
            let progress = BenchmarkProgress {
                messages_processed: i + 1,
                bytes_processed: (i + 1) * config.message_size as u64,
                elapsed_ms: elapsed.as_millis() as u64,
                percent_complete: ((i + 1) as f64 / config.message_count as f64) * 100.0,
            };
            store.update_progress(&id, progress);
        }

        // Apply rate limiting
        if let Some(d) = delay {
            tokio::time::sleep(d).await;
        }
    }

    let duration = start.elapsed();

    // Calculate results
    let results = calculate_results(&latencies, duration, config.message_size, errors);
    store.complete_benchmark(&id, results);
}

/// Run a consume benchmark
async fn run_consume_benchmark(store: Arc<BenchmarkStore>, id: String, config: BenchmarkConfig) {
    let start = Instant::now();
    let mut latencies: Vec<f64> = Vec::with_capacity(config.message_count as usize);
    let errors = 0u64;

    // Calculate rate limiting delay
    let delay = config
        .rate_limit
        .map(|rate| Duration::from_secs_f64(1.0 / rate as f64));

    for i in 0..config.message_count {
        let op_start = Instant::now();

        // Simulate message consumption (in a real implementation, this would use the actual Kafka consumer)
        tokio::time::sleep(Duration::from_micros(50)).await;

        let latency_ms = op_start.elapsed().as_secs_f64() * 1000.0;
        latencies.push(latency_ms);

        // Update progress periodically
        if i % 100 == 0 || i == config.message_count - 1 {
            let elapsed = start.elapsed();
            let progress = BenchmarkProgress {
                messages_processed: i + 1,
                bytes_processed: (i + 1) * config.message_size as u64,
                elapsed_ms: elapsed.as_millis() as u64,
                percent_complete: ((i + 1) as f64 / config.message_count as f64) * 100.0,
            };
            store.update_progress(&id, progress);
        }

        // Apply rate limiting
        if let Some(d) = delay {
            tokio::time::sleep(d).await;
        }
    }

    let duration = start.elapsed();

    // Calculate results
    let results = calculate_results(&latencies, duration, config.message_size, errors);
    store.complete_benchmark(&id, results);
}

/// Calculate benchmark results from latencies
fn calculate_results(
    latencies: &[f64],
    duration: Duration,
    message_size: usize,
    errors: u64,
) -> BenchmarkResults {
    let duration_ms = duration.as_millis() as u64;
    let messages_total = latencies.len() as u64;
    let bytes_total = messages_total * message_size as u64;

    // Calculate throughput
    let duration_secs = duration.as_secs_f64();
    let throughput_msg_sec = if duration_secs > 0.0 {
        messages_total as f64 / duration_secs
    } else {
        0.0
    };
    let throughput_mb_sec = if duration_secs > 0.0 {
        (bytes_total as f64 / 1024.0 / 1024.0) / duration_secs
    } else {
        0.0
    };

    // Calculate latency percentiles
    let mut sorted_latencies = latencies.to_vec();
    sorted_latencies.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let latency_p50_ms = percentile(&sorted_latencies, 50.0);
    let latency_p95_ms = percentile(&sorted_latencies, 95.0);
    let latency_p99_ms = percentile(&sorted_latencies, 99.0);
    let latency_avg_ms = if !latencies.is_empty() {
        latencies.iter().sum::<f64>() / latencies.len() as f64
    } else {
        0.0
    };

    BenchmarkResults {
        duration_ms,
        messages_total,
        bytes_total,
        throughput_msg_sec,
        throughput_mb_sec,
        latency_p50_ms,
        latency_p95_ms,
        latency_p99_ms,
        latency_avg_ms,
        errors,
    }
}

/// Calculate percentile value
fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_state() -> BenchmarkApiState {
        BenchmarkApiState {
            store: BenchmarkStore::new_shared(),
        }
    }

    #[tokio::test]
    async fn test_list_benchmarks_empty() {
        let state = create_test_state();
        let response = list_benchmarks(State(state)).await;
        assert_eq!(response.0.total, 0);
        assert!(response.0.benchmarks.is_empty());
    }

    #[tokio::test]
    async fn test_start_produce_benchmark() {
        let state = create_test_state();

        let config = BenchmarkConfig {
            topic: "test-topic".to_string(),
            message_size: 100,
            message_count: 10,
            rate_limit: None,
            num_partitions: None,
            consumer_group: None,
        };

        let (status, benchmark) = start_produce_benchmark(State(state.clone()), Json(config)).await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(benchmark.0.benchmark_type, BenchmarkType::Produce);

        // Verify benchmark is stored
        let stored = state.store.get_benchmark(&benchmark.0.id);
        assert!(stored.is_some());
    }

    #[tokio::test]
    async fn test_start_consume_benchmark() {
        let state = create_test_state();

        let config = BenchmarkConfig {
            topic: "test-topic".to_string(),
            message_size: 100,
            message_count: 10,
            rate_limit: None,
            num_partitions: None,
            consumer_group: Some("test-group".to_string()),
        };

        let (status, benchmark) = start_consume_benchmark(State(state.clone()), Json(config)).await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(benchmark.0.benchmark_type, BenchmarkType::Consume);
    }

    #[tokio::test]
    async fn test_get_benchmark_not_found() {
        let state = create_test_state();
        let result = get_benchmark_status(State(state), Path("nonexistent".to_string())).await;
        assert!(result.is_err());
    }

    #[test]
    fn test_percentile_calculation() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
        // p50 at index 4.5 rounds to 5, which gives value 6.0 (0-indexed)
        assert_eq!(percentile(&values, 50.0), 6.0);
        // p90 at index 8.1 rounds to 8, which gives value 9.0
        assert_eq!(percentile(&values, 90.0), 9.0);
        // p99 at index 8.91 rounds to 9, which gives value 10.0
        assert_eq!(percentile(&values, 99.0), 10.0);
    }

    #[test]
    fn test_percentile_empty() {
        let values: Vec<f64> = vec![];
        assert_eq!(percentile(&values, 50.0), 0.0);
    }

    #[test]
    fn test_calculate_results() {
        let latencies = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let duration = Duration::from_secs(1);
        let results = calculate_results(&latencies, duration, 100, 0);

        assert_eq!(results.messages_total, 5);
        assert_eq!(results.bytes_total, 500);
        assert_eq!(results.throughput_msg_sec, 5.0);
        assert_eq!(results.errors, 0);
    }
}
