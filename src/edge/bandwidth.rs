//! Bandwidth-aware sync for edge nodes (M3 P2).
//!
//! Adapts batch sizes and sync intervals based on observed network
//! throughput. When bandwidth is scarce the syncer sends smaller
//! batches with longer intervals; when bandwidth is plentiful it
//! increases batch sizes for efficiency.

use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::time::Instant;

/// Configuration for bandwidth-aware sync behaviour.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BandwidthConfig {
    /// Base interval between sync batches (milliseconds).
    #[serde(default = "default_batch_interval_ms")]
    pub batch_interval_ms: u64,
    /// Maximum bytes per sync batch.
    #[serde(default = "default_max_batch_bytes")]
    pub max_batch_bytes: usize,
    /// Base backoff duration when bandwidth is low (milliseconds).
    #[serde(default = "default_backoff_base_ms")]
    pub backoff_base_ms: u64,
    /// Topics that should be synced with higher priority.
    #[serde(default)]
    pub priority_topics: Vec<String>,
}

fn default_batch_interval_ms() -> u64 { 1_000 }
fn default_max_batch_bytes() -> usize { 1_048_576 } // 1 MiB
fn default_backoff_base_ms() -> u64 { 500 }

impl Default for BandwidthConfig {
    fn default() -> Self {
        Self {
            batch_interval_ms: default_batch_interval_ms(),
            max_batch_bytes: default_max_batch_bytes(),
            backoff_base_ms: default_backoff_base_ms(),
            priority_topics: Vec::new(),
        }
    }
}

/// Tracks recent throughput samples to estimate available bandwidth.
#[derive(Debug)]
pub struct BandwidthEstimator {
    /// Recent throughput samples (bytes/second).
    samples: VecDeque<f64>,
    /// Maximum number of samples to keep.
    window_size: usize,
    /// Last measurement time.
    last_sample_time: Option<Instant>,
}

impl BandwidthEstimator {
    /// Creates a new estimator with the given window size.
    pub fn new(window_size: usize) -> Self {
        Self {
            samples: VecDeque::with_capacity(window_size),
            window_size,
            last_sample_time: None,
        }
    }

    /// Records a transfer: `bytes_transferred` over `duration_ms`.
    pub fn record_transfer(&mut self, bytes_transferred: usize, duration_ms: u64) {
        if duration_ms == 0 {
            return;
        }
        let throughput = (bytes_transferred as f64) / (duration_ms as f64 / 1000.0);
        if self.samples.len() >= self.window_size {
            self.samples.pop_front();
        }
        self.samples.push_back(throughput);
        self.last_sample_time = Some(Instant::now());
    }

    /// Returns the estimated bandwidth in bytes/second (moving average).
    pub fn estimated_bandwidth(&self) -> f64 {
        if self.samples.is_empty() {
            return 0.0;
        }
        let sum: f64 = self.samples.iter().sum();
        sum / self.samples.len() as f64
    }

    /// Returns the number of recorded samples.
    pub fn sample_count(&self) -> usize {
        self.samples.len()
    }

    /// Returns true if enough samples exist for a reliable estimate.
    pub fn has_estimate(&self) -> bool {
        self.samples.len() >= 3
    }
}

impl Default for BandwidthEstimator {
    fn default() -> Self {
        Self::new(20)
    }
}

/// Calculates the optimal batch size based on config and estimated bandwidth.
///
/// When bandwidth is unknown or high, returns `config.max_batch_bytes`.
/// When bandwidth is low, reduces the batch size proportionally so that
/// each batch completes within the configured interval.
pub fn calculate_batch_size(config: &BandwidthConfig, estimator: &BandwidthEstimator) -> usize {
    if !estimator.has_estimate() {
        // Not enough data — use default max.
        return config.max_batch_bytes;
    }

    let bw = estimator.estimated_bandwidth();
    if bw <= 0.0 {
        // Zero bandwidth — use minimum batch (1 KiB).
        return 1024;
    }

    // Target: the batch should complete within batch_interval_ms.
    let interval_secs = config.batch_interval_ms as f64 / 1000.0;
    let target_bytes = (bw * interval_secs) as usize;

    // Clamp between 1 KiB and max_batch_bytes.
    target_bytes.clamp(1024, config.max_batch_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_values() {
        let cfg = BandwidthConfig::default();
        assert_eq!(cfg.batch_interval_ms, 1000);
        assert_eq!(cfg.max_batch_bytes, 1_048_576);
        assert_eq!(cfg.backoff_base_ms, 500);
        assert!(cfg.priority_topics.is_empty());
    }

    #[test]
    fn estimator_no_samples() {
        let est = BandwidthEstimator::default();
        assert_eq!(est.estimated_bandwidth(), 0.0);
        assert!(!est.has_estimate());
    }

    #[test]
    fn estimator_single_sample() {
        let mut est = BandwidthEstimator::default();
        est.record_transfer(1_000_000, 1000); // 1MB in 1s = 1MB/s
        assert!((est.estimated_bandwidth() - 1_000_000.0).abs() < 0.1);
        assert!(!est.has_estimate()); // Need >= 3 samples
    }

    #[test]
    fn estimator_moving_average() {
        let mut est = BandwidthEstimator::new(5);
        est.record_transfer(1_000, 1000); // 1 KB/s
        est.record_transfer(2_000, 1000); // 2 KB/s
        est.record_transfer(3_000, 1000); // 3 KB/s
        assert!(est.has_estimate());
        let avg = est.estimated_bandwidth();
        assert!((avg - 2000.0).abs() < 0.1);
    }

    #[test]
    fn estimator_window_eviction() {
        let mut est = BandwidthEstimator::new(2);
        est.record_transfer(1_000, 1000);
        est.record_transfer(2_000, 1000);
        est.record_transfer(4_000, 1000); // evicts first
        assert_eq!(est.sample_count(), 2);
        // Average of 2KB/s and 4KB/s = 3KB/s
        assert!((est.estimated_bandwidth() - 3000.0).abs() < 0.1);
    }

    #[test]
    fn estimator_ignores_zero_duration() {
        let mut est = BandwidthEstimator::default();
        est.record_transfer(1000, 0);
        assert_eq!(est.sample_count(), 0);
    }

    #[test]
    fn batch_size_no_estimate() {
        let cfg = BandwidthConfig::default();
        let est = BandwidthEstimator::default();
        assert_eq!(calculate_batch_size(&cfg, &est), cfg.max_batch_bytes);
    }

    #[test]
    fn batch_size_high_bandwidth() {
        let cfg = BandwidthConfig::default(); // max = 1 MiB, interval = 1s
        let mut est = BandwidthEstimator::new(5);
        // 10 MB/s — more than max_batch_bytes per interval
        for _ in 0..3 {
            est.record_transfer(10_000_000, 1000);
        }
        assert_eq!(calculate_batch_size(&cfg, &est), cfg.max_batch_bytes);
    }

    #[test]
    fn batch_size_low_bandwidth() {
        let cfg = BandwidthConfig {
            max_batch_bytes: 1_048_576,
            batch_interval_ms: 1000,
            ..Default::default()
        };
        let mut est = BandwidthEstimator::new(5);
        // 10 KB/s — very slow
        for _ in 0..3 {
            est.record_transfer(10_000, 1000);
        }
        let size = calculate_batch_size(&cfg, &est);
        assert_eq!(size, 10_000); // 10KB/s * 1s = 10KB
    }

    #[test]
    fn batch_size_minimum_clamp() {
        let cfg = BandwidthConfig::default();
        let mut est = BandwidthEstimator::new(5);
        // 100 bytes/s — extremely slow, should clamp to 1KB minimum
        for _ in 0..3 {
            est.record_transfer(100, 1000);
        }
        let size = calculate_batch_size(&cfg, &est);
        assert_eq!(size, 1024);
    }
}
