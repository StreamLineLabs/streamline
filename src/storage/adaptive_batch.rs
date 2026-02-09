//! Adaptive Batching for Streamline
//!
//! This module provides auto-tuning of batch sizes based on throughput and latency metrics.
//! The `AdaptiveBatcher` monitors P99 latency and adjusts batch sizes to meet target latency
//! while maximizing throughput.
//!
//! ## Example
//!
//! ```
//! use streamline::storage::adaptive_batch::{AdaptiveBatcher, AdaptiveBatchConfig};
//! use std::time::Duration;
//!
//! let config = AdaptiveBatchConfig::default();
//! let batcher = AdaptiveBatcher::new(config);
//!
//! // Record latency samples
//! batcher.record_latency(Duration::from_micros(150));
//!
//! // Get current optimal batch size
//! let batch_size = batcher.current_batch_size();
//! ```

use parking_lot::RwLock;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Configuration for adaptive batching
#[derive(Debug, Clone)]
pub struct AdaptiveBatchConfig {
    /// Target P99 latency in microseconds
    pub target_p99_latency_us: u64,
    /// Minimum batch size in bytes
    pub min_batch_size: usize,
    /// Maximum batch size in bytes
    pub max_batch_size: usize,
    /// Initial batch size in bytes
    pub initial_batch_size: usize,
    /// Number of samples to keep for latency calculation
    pub sample_window_size: usize,
    /// Adjustment interval - how often to adjust batch size
    pub adjustment_interval: Duration,
    /// Growth factor when latency is below target
    pub growth_factor: f64,
    /// Shrink factor when latency is above target
    pub shrink_factor: f64,
    /// Minimum samples before adjusting
    pub min_samples_for_adjustment: usize,
    /// Latency tolerance percentage (e.g., 0.1 = 10% tolerance)
    pub latency_tolerance: f64,
}

impl Default for AdaptiveBatchConfig {
    fn default() -> Self {
        Self {
            target_p99_latency_us: 10_000, // 10ms default target
            min_batch_size: 1024,          // 1KB minimum
            max_batch_size: 1024 * 1024,   // 1MB maximum
            initial_batch_size: 16 * 1024, // 16KB initial
            sample_window_size: 1000,
            adjustment_interval: Duration::from_secs(1),
            growth_factor: 1.25,
            shrink_factor: 0.75,
            min_samples_for_adjustment: 100,
            latency_tolerance: 0.1, // 10% tolerance
        }
    }
}

impl AdaptiveBatchConfig {
    /// Create a config optimized for low latency
    pub fn low_latency() -> Self {
        Self {
            target_p99_latency_us: 5_000, // 5ms target
            min_batch_size: 512,
            max_batch_size: 64 * 1024, // 64KB max
            initial_batch_size: 4 * 1024,
            sample_window_size: 500,
            adjustment_interval: Duration::from_millis(500),
            growth_factor: 1.1,
            shrink_factor: 0.8,
            min_samples_for_adjustment: 50,
            latency_tolerance: 0.05,
        }
    }

    /// Create a config optimized for high throughput
    pub fn high_throughput() -> Self {
        Self {
            target_p99_latency_us: 50_000, // 50ms target
            min_batch_size: 16 * 1024,
            max_batch_size: 16 * 1024 * 1024, // 16MB max
            initial_batch_size: 256 * 1024,
            sample_window_size: 2000,
            adjustment_interval: Duration::from_secs(2),
            growth_factor: 1.5,
            shrink_factor: 0.6,
            min_samples_for_adjustment: 200,
            latency_tolerance: 0.2,
        }
    }
}

/// Latency sample with timestamp
#[derive(Debug, Clone, Copy)]
struct LatencySample {
    latency_us: u64,
    /// Timestamp when sample was recorded (used for time-based windowing)
    #[allow(dead_code)]
    timestamp: Instant,
}

/// Statistics about adaptive batching
#[derive(Debug, Clone, Default)]
pub struct AdaptiveBatchStats {
    /// Current batch size
    pub current_batch_size: usize,
    /// Total samples collected
    pub total_samples: u64,
    /// Current P99 latency in microseconds
    pub current_p99_us: u64,
    /// Current P50 latency in microseconds
    pub current_p50_us: u64,
    /// Current P95 latency in microseconds
    pub current_p95_us: u64,
    /// Number of batch size increases
    pub size_increases: u64,
    /// Number of batch size decreases
    pub size_decreases: u64,
    /// Time since last adjustment
    pub time_since_last_adjustment_ms: u64,
}

/// Internal state for the batcher
struct BatcherState {
    /// Current batch size
    current_batch_size: usize,
    /// Latency samples
    samples: VecDeque<LatencySample>,
    /// Last adjustment time
    last_adjustment: Instant,
    /// Number of increases
    size_increases: u64,
    /// Number of decreases
    size_decreases: u64,
}

/// Adaptive batcher that auto-tunes batch sizes based on latency
pub struct AdaptiveBatcher {
    config: AdaptiveBatchConfig,
    state: RwLock<BatcherState>,
    total_samples: AtomicU64,
}

impl AdaptiveBatcher {
    /// Create a new adaptive batcher with the given configuration
    pub fn new(config: AdaptiveBatchConfig) -> Self {
        let initial_batch_size = config
            .initial_batch_size
            .clamp(config.min_batch_size, config.max_batch_size);

        Self {
            state: RwLock::new(BatcherState {
                current_batch_size: initial_batch_size,
                samples: VecDeque::with_capacity(config.sample_window_size),
                last_adjustment: Instant::now(),
                size_increases: 0,
                size_decreases: 0,
            }),
            config,
            total_samples: AtomicU64::new(0),
        }
    }

    /// Create a new adaptive batcher with default configuration
    pub fn with_defaults() -> Self {
        Self::new(AdaptiveBatchConfig::default())
    }

    /// Record a latency sample in microseconds
    pub fn record_latency_us(&self, latency_us: u64) {
        let sample = LatencySample {
            latency_us,
            timestamp: Instant::now(),
        };

        let mut state = self.state.write();

        // Add sample
        state.samples.push_back(sample);
        self.total_samples.fetch_add(1, Ordering::Relaxed);

        // Remove old samples if window is full
        while state.samples.len() > self.config.sample_window_size {
            state.samples.pop_front();
        }

        // Check if we should adjust
        if state.last_adjustment.elapsed() >= self.config.adjustment_interval
            && state.samples.len() >= self.config.min_samples_for_adjustment
        {
            self.adjust_batch_size(&mut state);
        }
    }

    /// Record a latency sample as a Duration
    pub fn record_latency(&self, latency: Duration) {
        self.record_latency_us(latency.as_micros() as u64);
    }

    /// Get the current optimal batch size
    pub fn current_batch_size(&self) -> usize {
        self.state.read().current_batch_size
    }

    /// Get the current configuration
    pub fn config(&self) -> &AdaptiveBatchConfig {
        &self.config
    }

    /// Get current statistics
    pub fn stats(&self) -> AdaptiveBatchStats {
        let state = self.state.read();
        let (p50, p95, p99) = self.calculate_percentiles(&state.samples);

        AdaptiveBatchStats {
            current_batch_size: state.current_batch_size,
            total_samples: self.total_samples.load(Ordering::Relaxed),
            current_p99_us: p99,
            current_p50_us: p50,
            current_p95_us: p95,
            size_increases: state.size_increases,
            size_decreases: state.size_decreases,
            time_since_last_adjustment_ms: state.last_adjustment.elapsed().as_millis() as u64,
        }
    }

    /// Manually set the batch size (useful for testing or overrides)
    pub fn set_batch_size(&self, size: usize) {
        let size = size.clamp(self.config.min_batch_size, self.config.max_batch_size);
        let mut state = self.state.write();
        state.current_batch_size = size;
    }

    /// Clear all samples and reset to initial batch size
    pub fn reset(&self) {
        let mut state = self.state.write();
        state.samples.clear();
        state.current_batch_size = self.config.initial_batch_size;
        state.last_adjustment = Instant::now();
        self.total_samples.store(0, Ordering::Relaxed);
    }

    /// Adjust batch size based on current latency samples
    fn adjust_batch_size(&self, state: &mut BatcherState) {
        let (_, _, p99) = self.calculate_percentiles(&state.samples);

        let target = self.config.target_p99_latency_us;
        let tolerance = self.config.latency_tolerance;
        let lower_bound = (target as f64 * (1.0 - tolerance)) as u64;
        let upper_bound = (target as f64 * (1.0 + tolerance)) as u64;

        let new_size = if p99 < lower_bound {
            // Latency is well below target, increase batch size
            let new = (state.current_batch_size as f64 * self.config.growth_factor) as usize;
            state.size_increases += 1;
            new.min(self.config.max_batch_size)
        } else if p99 > upper_bound {
            // Latency is above target, decrease batch size
            let new = (state.current_batch_size as f64 * self.config.shrink_factor) as usize;
            state.size_decreases += 1;
            new.max(self.config.min_batch_size)
        } else {
            // Within tolerance, keep current size
            state.current_batch_size
        };

        state.current_batch_size = new_size;
        state.last_adjustment = Instant::now();
    }

    /// Calculate P50, P95, P99 from samples
    fn calculate_percentiles(&self, samples: &VecDeque<LatencySample>) -> (u64, u64, u64) {
        if samples.is_empty() {
            return (0, 0, 0);
        }

        let mut latencies: Vec<u64> = samples.iter().map(|s| s.latency_us).collect();
        latencies.sort_unstable();

        let len = latencies.len();
        let p50_idx = (len as f64 * 0.50) as usize;
        let p95_idx = (len as f64 * 0.95) as usize;
        let p99_idx = (len as f64 * 0.99) as usize;

        let p50 = latencies.get(p50_idx).copied().unwrap_or(0);
        let p95 = latencies.get(p95_idx.min(len - 1)).copied().unwrap_or(0);
        let p99 = latencies.get(p99_idx.min(len - 1)).copied().unwrap_or(0);

        (p50, p95, p99)
    }
}

impl Default for AdaptiveBatcher {
    fn default() -> Self {
        Self::with_defaults()
    }
}

/// Builder for AdaptiveBatcher
#[derive(Debug, Clone)]
pub struct AdaptiveBatcherBuilder {
    config: AdaptiveBatchConfig,
}

impl AdaptiveBatcherBuilder {
    /// Create a new builder with default config
    pub fn new() -> Self {
        Self {
            config: AdaptiveBatchConfig::default(),
        }
    }

    /// Set target P99 latency in microseconds
    pub fn target_p99_latency_us(mut self, target: u64) -> Self {
        self.config.target_p99_latency_us = target;
        self
    }

    /// Set target P99 latency as Duration
    pub fn target_p99_latency(mut self, target: Duration) -> Self {
        self.config.target_p99_latency_us = target.as_micros() as u64;
        self
    }

    /// Set minimum batch size
    pub fn min_batch_size(mut self, size: usize) -> Self {
        self.config.min_batch_size = size;
        self
    }

    /// Set maximum batch size
    pub fn max_batch_size(mut self, size: usize) -> Self {
        self.config.max_batch_size = size;
        self
    }

    /// Set initial batch size
    pub fn initial_batch_size(mut self, size: usize) -> Self {
        self.config.initial_batch_size = size;
        self
    }

    /// Set sample window size
    pub fn sample_window_size(mut self, size: usize) -> Self {
        self.config.sample_window_size = size;
        self
    }

    /// Set adjustment interval
    pub fn adjustment_interval(mut self, interval: Duration) -> Self {
        self.config.adjustment_interval = interval;
        self
    }

    /// Set growth factor
    pub fn growth_factor(mut self, factor: f64) -> Self {
        self.config.growth_factor = factor;
        self
    }

    /// Set shrink factor
    pub fn shrink_factor(mut self, factor: f64) -> Self {
        self.config.shrink_factor = factor;
        self
    }

    /// Build the AdaptiveBatcher
    pub fn build(self) -> AdaptiveBatcher {
        AdaptiveBatcher::new(self.config)
    }
}

impl Default for AdaptiveBatcherBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Shared adaptive batcher that can be cloned
pub type SharedAdaptiveBatcher = Arc<AdaptiveBatcher>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = AdaptiveBatchConfig::default();
        assert_eq!(config.target_p99_latency_us, 10_000);
        assert_eq!(config.min_batch_size, 1024);
        assert_eq!(config.max_batch_size, 1024 * 1024);
        assert_eq!(config.initial_batch_size, 16 * 1024);
    }

    #[test]
    fn test_low_latency_config() {
        let config = AdaptiveBatchConfig::low_latency();
        assert_eq!(config.target_p99_latency_us, 5_000);
        assert!(config.max_batch_size < AdaptiveBatchConfig::default().max_batch_size);
    }

    #[test]
    fn test_high_throughput_config() {
        let config = AdaptiveBatchConfig::high_throughput();
        assert_eq!(config.target_p99_latency_us, 50_000);
        assert!(config.max_batch_size > AdaptiveBatchConfig::default().max_batch_size);
    }

    #[test]
    fn test_batcher_creation() {
        let batcher = AdaptiveBatcher::with_defaults();
        assert_eq!(batcher.current_batch_size(), 16 * 1024);
    }

    #[test]
    fn test_record_latency() {
        let batcher = AdaptiveBatcher::with_defaults();
        batcher.record_latency_us(100);
        batcher.record_latency(Duration::from_micros(200));

        let stats = batcher.stats();
        assert_eq!(stats.total_samples, 2);
    }

    #[test]
    fn test_batch_size_increase() {
        let config = AdaptiveBatchConfig {
            target_p99_latency_us: 10_000, // 10ms target
            min_batch_size: 1024,
            max_batch_size: 1024 * 1024,
            initial_batch_size: 16 * 1024,
            sample_window_size: 100,
            adjustment_interval: Duration::from_millis(1),
            growth_factor: 1.5,
            shrink_factor: 0.5,
            min_samples_for_adjustment: 10,
            latency_tolerance: 0.1,
        };

        let batcher = AdaptiveBatcher::new(config);
        let initial_size = batcher.current_batch_size();

        // Record very low latencies (below target)
        for _ in 0..20 {
            batcher.record_latency_us(1000); // 1ms, well below 10ms target
        }

        std::thread::sleep(Duration::from_millis(5));

        // Record more samples to trigger adjustment
        for _ in 0..20 {
            batcher.record_latency_us(1000);
        }

        let final_size = batcher.current_batch_size();
        assert!(
            final_size >= initial_size,
            "Batch size should increase when latency is low"
        );
    }

    #[test]
    fn test_batch_size_decrease() {
        let config = AdaptiveBatchConfig {
            target_p99_latency_us: 1000, // 1ms target
            min_batch_size: 1024,
            max_batch_size: 1024 * 1024,
            initial_batch_size: 16 * 1024,
            sample_window_size: 100,
            adjustment_interval: Duration::from_millis(1),
            growth_factor: 1.5,
            shrink_factor: 0.5,
            min_samples_for_adjustment: 10,
            latency_tolerance: 0.1,
        };

        let batcher = AdaptiveBatcher::new(config);
        let initial_size = batcher.current_batch_size();

        // Record high latencies (above target)
        for _ in 0..20 {
            batcher.record_latency_us(10_000); // 10ms, well above 1ms target
        }

        std::thread::sleep(Duration::from_millis(5));

        // Record more samples to trigger adjustment
        for _ in 0..20 {
            batcher.record_latency_us(10_000);
        }

        let final_size = batcher.current_batch_size();
        assert!(
            final_size <= initial_size,
            "Batch size should decrease when latency is high"
        );
    }

    #[test]
    fn test_batch_size_clamping() {
        let batcher = AdaptiveBatcher::with_defaults();

        batcher.set_batch_size(100); // Below minimum
        assert_eq!(batcher.current_batch_size(), 1024); // Should be clamped to min

        batcher.set_batch_size(100_000_000); // Above maximum
        assert_eq!(batcher.current_batch_size(), 1024 * 1024); // Should be clamped to max
    }

    #[test]
    fn test_reset() {
        let batcher = AdaptiveBatcher::with_defaults();
        batcher.record_latency_us(100);
        batcher.record_latency_us(200);
        batcher.set_batch_size(32 * 1024);

        batcher.reset();

        let stats = batcher.stats();
        assert_eq!(stats.total_samples, 0);
        assert_eq!(batcher.current_batch_size(), 16 * 1024); // Back to initial
    }

    #[test]
    fn test_builder() {
        let batcher = AdaptiveBatcherBuilder::new()
            .target_p99_latency(Duration::from_millis(5))
            .min_batch_size(2048)
            .max_batch_size(512 * 1024)
            .initial_batch_size(8192)
            .build();

        assert_eq!(batcher.config().target_p99_latency_us, 5000);
        assert_eq!(batcher.config().min_batch_size, 2048);
        assert_eq!(batcher.config().max_batch_size, 512 * 1024);
        assert_eq!(batcher.current_batch_size(), 8192);
    }

    #[test]
    fn test_percentile_calculation() {
        let config = AdaptiveBatchConfig {
            sample_window_size: 100,
            min_samples_for_adjustment: 10,
            ..Default::default()
        };
        let batcher = AdaptiveBatcher::new(config);

        // Add samples with known distribution
        for i in 1..=100 {
            batcher.record_latency_us(i * 100); // 100, 200, ..., 10000 us
        }

        let stats = batcher.stats();
        // P50 should be around 5000us
        assert!(stats.current_p50_us >= 4000 && stats.current_p50_us <= 6000);
        // P99 should be around 9900us
        assert!(stats.current_p99_us >= 9000 && stats.current_p99_us <= 10000);
    }

    #[test]
    fn test_stats() {
        let batcher = AdaptiveBatcher::with_defaults();
        let stats = batcher.stats();

        assert_eq!(stats.current_batch_size, 16 * 1024);
        assert_eq!(stats.total_samples, 0);
        assert_eq!(stats.size_increases, 0);
        assert_eq!(stats.size_decreases, 0);
    }
}
