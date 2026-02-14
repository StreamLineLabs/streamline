//! Self-tuning module with ML-based optimization
//!
//! This module provides automatic tuning of system parameters based on
//! workload patterns and performance metrics. It uses machine learning
//! techniques to optimize configurations for best performance.
//!
//! Features:
//! - Workload detection and classification
//! - Parameter optimization using gradient descent and genetic algorithms
//! - Performance prediction models
//! - Adaptive configuration management
//! - Anomaly detection for workload changes

pub mod explain;
pub mod governor;
pub mod optimizer;
pub mod predictor;
pub mod workload;

pub use explain::{
    ExplainReport, ParameterRecommendation, RecommendationImpact, TuningExplainer,
    WorkloadExplanation,
};
pub use governor::{
    GovernorAction, PartitionRecommendation, PressureLevel, ResourceGovernor,
    ResourceGovernorConfig, ResourceSnapshot,
};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;
use tracing::{debug, info};

/// Auto-tuning configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoTuningConfig {
    /// Enable auto-tuning
    pub enabled: bool,
    /// Tuning interval in milliseconds
    pub tuning_interval_ms: u64,
    /// Minimum samples before tuning
    pub min_samples: usize,
    /// Learning rate for gradient descent
    pub learning_rate: f64,
    /// Exploration rate for optimization (0.0-1.0)
    pub exploration_rate: f64,
    /// Maximum parameter change per iteration (percentage)
    pub max_change_percent: f64,
    /// Enable workload detection
    pub enable_workload_detection: bool,
    /// Enable anomaly detection
    pub enable_anomaly_detection: bool,
    /// History size for metrics
    pub history_size: usize,
}

impl Default for AutoTuningConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            tuning_interval_ms: 30_000, // 30 seconds
            min_samples: 100,
            learning_rate: 0.01,
            exploration_rate: 0.1,
            max_change_percent: 10.0,
            enable_workload_detection: true,
            enable_anomaly_detection: true,
            history_size: 1000,
        }
    }
}

/// Tunable parameters
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TunableParameters {
    /// Batch size for writes
    pub batch_size: usize,
    /// Buffer pool size
    pub buffer_pool_size: usize,
    /// Number of I/O threads
    pub io_threads: usize,
    /// Flush interval in milliseconds
    pub flush_interval_ms: u64,
    /// Compression level (0-9)
    pub compression_level: u32,
    /// Cache size in bytes
    pub cache_size: usize,
    /// Max concurrent connections
    pub max_connections: usize,
    /// Request queue depth
    pub queue_depth: usize,
    /// Prefetch size for reads
    pub prefetch_size: usize,
}

impl Default for TunableParameters {
    fn default() -> Self {
        Self {
            batch_size: 16384,
            buffer_pool_size: 64 * 1024 * 1024, // 64MB
            io_threads: 4,
            flush_interval_ms: 1000,
            compression_level: 3,
            cache_size: 128 * 1024 * 1024, // 128MB
            max_connections: 1000,
            queue_depth: 128,
            prefetch_size: 4096,
        }
    }
}

impl TunableParameters {
    /// Convert to vector for optimization
    pub fn to_vec(&self) -> Vec<f64> {
        vec![
            self.batch_size as f64,
            self.buffer_pool_size as f64,
            self.io_threads as f64,
            self.flush_interval_ms as f64,
            self.compression_level as f64,
            self.cache_size as f64,
            self.max_connections as f64,
            self.queue_depth as f64,
            self.prefetch_size as f64,
        ]
    }

    /// Create from vector
    pub fn from_vec(v: &[f64]) -> Self {
        Self {
            batch_size: v.first().copied().unwrap_or(16384.0) as usize,
            buffer_pool_size: v.get(1).copied().unwrap_or(64.0 * 1024.0 * 1024.0) as usize,
            io_threads: v.get(2).copied().unwrap_or(4.0).max(1.0) as usize,
            flush_interval_ms: v.get(3).copied().unwrap_or(1000.0) as u64,
            compression_level: v.get(4).copied().unwrap_or(3.0).clamp(0.0, 9.0) as u32,
            cache_size: v.get(5).copied().unwrap_or(128.0 * 1024.0 * 1024.0) as usize,
            max_connections: v.get(6).copied().unwrap_or(1000.0) as usize,
            queue_depth: v.get(7).copied().unwrap_or(128.0).max(1.0) as usize,
            prefetch_size: v.get(8).copied().unwrap_or(4096.0) as usize,
        }
    }

    /// Get parameter bounds
    pub fn bounds() -> Vec<(f64, f64)> {
        vec![
            (1024.0, 1024.0 * 1024.0),                          // batch_size
            (16.0 * 1024.0 * 1024.0, 1024.0 * 1024.0 * 1024.0), // buffer_pool_size
            (1.0, 64.0),                                        // io_threads
            (100.0, 60000.0),                                   // flush_interval_ms
            (0.0, 9.0),                                         // compression_level
            (32.0 * 1024.0 * 1024.0, 2048.0 * 1024.0 * 1024.0), // cache_size
            (100.0, 100000.0),                                  // max_connections
            (1.0, 4096.0),                                      // queue_depth
            (512.0, 1024.0 * 1024.0),                           // prefetch_size
        ]
    }
}

/// Performance metrics for tuning
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PerformanceMetrics {
    /// Timestamp
    pub timestamp: i64,
    /// Throughput (records per second)
    pub throughput: f64,
    /// P99 latency in microseconds
    pub p99_latency_us: f64,
    /// Average latency in microseconds
    pub avg_latency_us: f64,
    /// CPU utilization (0-100)
    pub cpu_percent: f64,
    /// Memory utilization (0-100)
    pub memory_percent: f64,
    /// I/O wait percentage
    pub io_wait_percent: f64,
    /// Error rate (errors per second)
    pub error_rate: f64,
    /// Cache hit rate (0-1)
    pub cache_hit_rate: f64,
}

impl PerformanceMetrics {
    /// Calculate a composite score (higher is better)
    pub fn score(&self) -> f64 {
        // Weighted combination of metrics
        // Maximize throughput, minimize latency and errors
        let throughput_score = self.throughput.log10().max(0.0) * 10.0;
        let latency_penalty = (self.p99_latency_us / 1000.0).min(100.0); // Cap at 100ms
        let error_penalty = self.error_rate * 100.0;
        let resource_efficiency = 100.0 - self.cpu_percent.max(self.memory_percent);
        let cache_bonus = self.cache_hit_rate * 20.0;

        throughput_score + resource_efficiency + cache_bonus - latency_penalty - error_penalty
    }
}

/// Workload type classification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum WorkloadType {
    /// Write-heavy workload
    WriteHeavy,
    /// Read-heavy workload
    ReadHeavy,
    /// Balanced read/write
    Balanced,
    /// Latency-sensitive
    LatencySensitive,
    /// Throughput-oriented
    ThroughputOriented,
    /// Bursty workload
    Bursty,
    /// Unknown/mixed workload
    Unknown,
}

impl WorkloadType {
    /// Get recommended parameters for this workload type
    pub fn recommended_params(&self) -> TunableParameters {
        match self {
            WorkloadType::WriteHeavy => TunableParameters {
                batch_size: 65536,
                buffer_pool_size: 256 * 1024 * 1024,
                io_threads: 8,
                flush_interval_ms: 2000,
                compression_level: 1,
                cache_size: 64 * 1024 * 1024,
                max_connections: 2000,
                queue_depth: 256,
                prefetch_size: 4096,
            },
            WorkloadType::ReadHeavy => TunableParameters {
                batch_size: 8192,
                buffer_pool_size: 64 * 1024 * 1024,
                io_threads: 4,
                flush_interval_ms: 5000,
                compression_level: 3,
                cache_size: 512 * 1024 * 1024,
                max_connections: 5000,
                queue_depth: 64,
                prefetch_size: 65536,
            },
            WorkloadType::LatencySensitive => TunableParameters {
                batch_size: 4096,
                buffer_pool_size: 32 * 1024 * 1024,
                io_threads: 2,
                flush_interval_ms: 100,
                compression_level: 0,
                cache_size: 256 * 1024 * 1024,
                max_connections: 1000,
                queue_depth: 32,
                prefetch_size: 8192,
            },
            WorkloadType::ThroughputOriented => TunableParameters {
                batch_size: 131072,
                buffer_pool_size: 512 * 1024 * 1024,
                io_threads: 16,
                flush_interval_ms: 5000,
                compression_level: 6,
                cache_size: 256 * 1024 * 1024,
                max_connections: 10000,
                queue_depth: 512,
                prefetch_size: 131072,
            },
            WorkloadType::Bursty => TunableParameters {
                batch_size: 32768,
                buffer_pool_size: 128 * 1024 * 1024,
                io_threads: 8,
                flush_interval_ms: 500,
                compression_level: 3,
                cache_size: 128 * 1024 * 1024,
                max_connections: 2000,
                queue_depth: 256,
                prefetch_size: 16384,
            },
            _ => TunableParameters::default(),
        }
    }
}

/// Auto-tuning manager
pub struct AutoTuner {
    /// Configuration
    config: AutoTuningConfig,
    /// Current parameters
    current_params: RwLock<TunableParameters>,
    /// Performance history
    history: RwLock<Vec<(TunableParameters, PerformanceMetrics)>>,
    /// Detected workload type
    workload_type: RwLock<WorkloadType>,
    /// Is tuning active
    is_active: AtomicBool,
    /// Tuning iterations
    iterations: AtomicU64,
    /// Best score achieved
    best_score: RwLock<f64>,
    /// Best parameters
    best_params: RwLock<TunableParameters>,
    /// Start time
    start_time: Instant,
    /// Last tuning time
    last_tuning: RwLock<Instant>,
}

impl AutoTuner {
    /// Create a new auto-tuner
    pub fn new(config: AutoTuningConfig) -> Self {
        let default_params = TunableParameters::default();
        Self {
            config,
            current_params: RwLock::new(default_params.clone()),
            history: RwLock::new(Vec::new()),
            workload_type: RwLock::new(WorkloadType::Unknown),
            is_active: AtomicBool::new(false),
            iterations: AtomicU64::new(0),
            best_score: RwLock::new(f64::NEG_INFINITY),
            best_params: RwLock::new(default_params),
            start_time: Instant::now(),
            last_tuning: RwLock::new(Instant::now()),
        }
    }

    /// Start auto-tuning
    pub fn start(&self) {
        info!("Starting auto-tuning");
        self.is_active.store(true, Ordering::SeqCst);
    }

    /// Stop auto-tuning
    pub fn stop(&self) {
        info!("Stopping auto-tuning");
        self.is_active.store(false, Ordering::SeqCst);
    }

    /// Check if tuning is active
    pub fn is_active(&self) -> bool {
        self.is_active.load(Ordering::SeqCst)
    }

    /// Record performance metrics
    pub fn record_metrics(&self, metrics: PerformanceMetrics) {
        let params = self.current_params.read().clone();
        let mut history = self.history.write();
        history.push((params.clone(), metrics.clone()));

        // Trim history
        let history_len = history.len();
        if history_len > self.config.history_size {
            history.drain(0..history_len - self.config.history_size);
        }

        // Update best score
        let score = metrics.score();
        let mut best_score = self.best_score.write();
        if score > *best_score {
            *best_score = score;
            *self.best_params.write() = params;
            debug!("New best score: {:.2}", score);
        }

        // Detect workload type
        if self.config.enable_workload_detection {
            self.detect_workload(&metrics);
        }
    }

    /// Detect workload type from metrics
    fn detect_workload(&self, metrics: &PerformanceMetrics) {
        let history = self.history.read();
        if history.len() < 10 {
            return;
        }

        // Analyze recent metrics
        let recent: Vec<_> = history.iter().rev().take(10).collect();

        // Calculate variance in throughput (bursty detection)
        let throughputs: Vec<f64> = recent.iter().map(|(_, m)| m.throughput).collect();
        let mean_throughput: f64 = throughputs.iter().sum::<f64>() / throughputs.len() as f64;
        let variance: f64 = throughputs
            .iter()
            .map(|t| (t - mean_throughput).powi(2))
            .sum::<f64>()
            / throughputs.len() as f64;
        let cv = (variance.sqrt() / mean_throughput.max(1.0)).min(10.0); // Coefficient of variation

        // Analyze latency sensitivity
        let avg_latency: f64 =
            recent.iter().map(|(_, m)| m.avg_latency_us).sum::<f64>() / recent.len() as f64;

        // Analyze cache hit rate
        let avg_cache_hit: f64 =
            recent.iter().map(|(_, m)| m.cache_hit_rate).sum::<f64>() / recent.len() as f64;

        // Classify workload
        let workload = if cv > 0.5 {
            WorkloadType::Bursty
        } else if avg_latency < 1000.0 && metrics.error_rate < 0.001 {
            WorkloadType::LatencySensitive
        } else if mean_throughput > 100000.0 {
            WorkloadType::ThroughputOriented
        } else if avg_cache_hit > 0.8 {
            WorkloadType::ReadHeavy
        } else if avg_cache_hit < 0.3 {
            WorkloadType::WriteHeavy
        } else {
            WorkloadType::Balanced
        };

        let mut current_workload = self.workload_type.write();
        if *current_workload != workload {
            info!(
                "Workload type changed: {:?} -> {:?}",
                *current_workload, workload
            );
            *current_workload = workload;
        }
    }

    /// Perform a tuning iteration
    pub fn tune(&self) -> Option<TunableParameters> {
        if !self.is_active() || !self.config.enabled {
            return None;
        }

        let history = self.history.read();
        if history.len() < self.config.min_samples {
            debug!(
                "Not enough samples for tuning: {} < {}",
                history.len(),
                self.config.min_samples
            );
            return None;
        }

        let elapsed = self.last_tuning.read().elapsed();
        if elapsed.as_millis() < self.config.tuning_interval_ms as u128 {
            return None;
        }

        drop(history);

        *self.last_tuning.write() = Instant::now();
        self.iterations.fetch_add(1, Ordering::Relaxed);

        // Get current workload type
        let workload_type = *self.workload_type.read();

        // Decide whether to explore or exploit
        let explore = rand::random::<f64>() < self.config.exploration_rate;

        let new_params = if explore {
            // Exploration: try parameters based on workload type
            debug!("Exploring based on workload type: {:?}", workload_type);
            workload_type.recommended_params()
        } else {
            // Exploitation: gradient descent optimization
            debug!("Exploiting: gradient descent optimization");
            self.gradient_step()
        };

        // Apply change limits
        let bounded_params = self.apply_bounds(&new_params);

        *self.current_params.write() = bounded_params.clone();
        info!(
            "Tuning iteration {}: batch_size={}, cache_size={}MB",
            self.iterations.load(Ordering::Relaxed),
            bounded_params.batch_size,
            bounded_params.cache_size / (1024 * 1024)
        );

        Some(bounded_params)
    }

    /// Perform gradient descent step
    fn gradient_step(&self) -> TunableParameters {
        let history = self.history.read();
        if history.len() < 2 {
            return self.current_params.read().clone();
        }

        // Get recent history for gradient estimation
        let recent: Vec<_> = history.iter().rev().take(20).collect();

        // Estimate gradient by comparing parameter changes to score changes
        let current = self.current_params.read().to_vec();
        let bounds = TunableParameters::bounds();
        let mut gradient = vec![0.0; current.len()];

        for window in recent.windows(2) {
            let (params1, metrics1) = window[0];
            let (params2, metrics2) = window[1];

            let v1 = params1.to_vec();
            let v2 = params2.to_vec();
            let score_diff = metrics2.score() - metrics1.score();

            for i in 0..gradient.len() {
                let param_diff = v2[i] - v1[i];
                if param_diff.abs() > 1e-10 {
                    gradient[i] += score_diff / param_diff;
                }
            }
        }

        // Normalize gradient
        let grad_norm: f64 = gradient.iter().map(|g| g * g).sum::<f64>().sqrt();
        if grad_norm > 1e-10 {
            for g in &mut gradient {
                *g /= grad_norm;
            }
        }

        // Apply gradient step with learning rate
        let mut new_params = current.clone();
        for i in 0..new_params.len() {
            let step = gradient[i] * self.config.learning_rate * (bounds[i].1 - bounds[i].0);
            new_params[i] += step;
            new_params[i] = new_params[i].clamp(bounds[i].0, bounds[i].1);
        }

        TunableParameters::from_vec(&new_params)
    }

    /// Apply bounds and change limits
    fn apply_bounds(&self, params: &TunableParameters) -> TunableParameters {
        let current = self.current_params.read().to_vec();
        let new_vec = params.to_vec();
        let bounds = TunableParameters::bounds();
        let max_change = self.config.max_change_percent / 100.0;

        let bounded: Vec<f64> = new_vec
            .iter()
            .zip(current.iter())
            .zip(bounds.iter())
            .map(|((new, cur), (min, max))| {
                let max_delta = cur.abs() * max_change;
                let delta = (new - cur).clamp(-max_delta, max_delta);
                (cur + delta).clamp(*min, *max)
            })
            .collect();

        TunableParameters::from_vec(&bounded)
    }

    /// Get current parameters
    pub fn current_params(&self) -> TunableParameters {
        self.current_params.read().clone()
    }

    /// Get best parameters found
    pub fn best_params(&self) -> TunableParameters {
        self.best_params.read().clone()
    }

    /// Get best score
    pub fn best_score(&self) -> f64 {
        *self.best_score.read()
    }

    /// Get detected workload type
    pub fn workload_type(&self) -> WorkloadType {
        *self.workload_type.read()
    }

    /// Get tuning statistics
    pub fn stats(&self) -> AutoTuningStats {
        AutoTuningStats {
            is_active: self.is_active(),
            iterations: self.iterations.load(Ordering::Relaxed),
            history_size: self.history.read().len(),
            best_score: *self.best_score.read(),
            workload_type: *self.workload_type.read(),
            uptime_secs: self.start_time.elapsed().as_secs(),
        }
    }

    /// Apply workload-specific recommendations
    pub fn apply_workload_recommendations(&self) {
        let workload = *self.workload_type.read();
        let recommended = workload.recommended_params();
        *self.current_params.write() = recommended;
        info!("Applied recommendations for workload type: {:?}", workload);
    }
}

/// Auto-tuning statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoTuningStats {
    /// Is tuning active
    pub is_active: bool,
    /// Number of tuning iterations
    pub iterations: u64,
    /// History size
    pub history_size: usize,
    /// Best score achieved
    pub best_score: f64,
    /// Detected workload type
    pub workload_type: WorkloadType,
    /// Uptime in seconds
    pub uptime_secs: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tunable_parameters_default() {
        let params = TunableParameters::default();
        assert!(params.batch_size > 0);
        assert!(params.buffer_pool_size > 0);
    }

    #[test]
    fn test_tunable_parameters_vec_conversion() {
        let params = TunableParameters::default();
        let vec = params.to_vec();
        let restored = TunableParameters::from_vec(&vec);

        assert_eq!(params.batch_size, restored.batch_size);
        assert_eq!(params.io_threads, restored.io_threads);
    }

    #[test]
    fn test_performance_metrics_score() {
        let metrics = PerformanceMetrics {
            throughput: 100000.0,
            p99_latency_us: 1000.0,
            avg_latency_us: 500.0,
            cpu_percent: 50.0,
            memory_percent: 40.0,
            cache_hit_rate: 0.9,
            ..Default::default()
        };

        let score = metrics.score();
        assert!(score > 0.0);
    }

    #[test]
    fn test_workload_type_recommendations() {
        let write_heavy = WorkloadType::WriteHeavy.recommended_params();
        let read_heavy = WorkloadType::ReadHeavy.recommended_params();

        // Write-heavy should have larger batch size
        assert!(write_heavy.batch_size > read_heavy.batch_size);
        // Read-heavy should have larger cache
        assert!(read_heavy.cache_size > write_heavy.cache_size);
    }

    #[test]
    fn test_auto_tuner_creation() {
        let config = AutoTuningConfig::default();
        let tuner = AutoTuner::new(config);
        assert!(!tuner.is_active());
    }

    #[test]
    fn test_auto_tuner_start_stop() {
        let config = AutoTuningConfig::default();
        let tuner = AutoTuner::new(config);

        tuner.start();
        assert!(tuner.is_active());

        tuner.stop();
        assert!(!tuner.is_active());
    }

    #[test]
    fn test_auto_tuner_record_metrics() {
        let config = AutoTuningConfig {
            enable_workload_detection: false, // Disable to speed up test
            ..Default::default()
        };
        let tuner = AutoTuner::new(config);

        let metrics = PerformanceMetrics {
            timestamp: 1000,
            throughput: 50000.0,
            p99_latency_us: 2000.0,
            avg_latency_us: 1000.0,
            cpu_percent: 30.0,
            memory_percent: 25.0,
            cache_hit_rate: 0.75,
            ..Default::default()
        };

        tuner.record_metrics(metrics);
        assert_eq!(tuner.stats().history_size, 1);
    }

    #[test]
    fn test_auto_tuner_stats() {
        let config = AutoTuningConfig::default();
        let tuner = AutoTuner::new(config);

        let stats = tuner.stats();
        assert!(!stats.is_active);
        assert_eq!(stats.iterations, 0);
    }
}
