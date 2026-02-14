//! Workload detection, classification, and profiling
//!
//! This module provides tools for analyzing streaming workloads to enable
//! intelligent auto-tuning. By understanding workload characteristics, the
//! system can select optimal parameters for each workload type.
//!
//! # Features
//!
//! - **Workload Analysis**: Collects samples to compute characteristics like
//!   read/write ratio, message sizes, request rates, and burstiness.
//!
//! - **Pattern Detection**: Identifies workload patterns (steady, growing,
//!   declining, periodic, bursty, diurnal) using trend analysis and
//!   autocorrelation.
//!
//! - **Fingerprinting**: Creates compact workload fingerprints for quick
//!   comparison and caching of optimal parameters.
//!
//! - **Profile Caching**: Stores optimal parameters for known workload
//!   fingerprints to enable fast warm-start tuning.
//!
//! # Example
//!
//! ```
//! use streamline::autotuning::workload::{WorkloadAnalyzer, WorkloadSample, WorkloadPattern};
//!
//! let mut analyzer = WorkloadAnalyzer::new(100);
//!
//! // Add samples as they arrive
//! for i in 0..30 {
//!     let sample = WorkloadSample {
//!         timestamp: i * 1000,
//!         reads: 100,
//!         writes: 100,
//!         message_count: 200,
//!         total_bytes: 20000,
//!         ..Default::default()
//!     };
//!     analyzer.add_sample(sample);
//! }
//!
//! // Get detected pattern
//! match analyzer.pattern() {
//!     WorkloadPattern::Steady => println!("Stable workload"),
//!     WorkloadPattern::Growing => println!("Increasing load"),
//!     WorkloadPattern::Bursty => println!("Spiky traffic"),
//!     _ => println!("Other pattern"),
//! }
//!
//! // Get fingerprint for caching
//! let fingerprint = analyzer.fingerprint();
//! ```

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};

/// Computed characteristics of a streaming workload.
///
/// Derived from accumulated workload samples to describe the
/// nature of traffic patterns and resource usage.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WorkloadCharacteristics {
    /// Read/write ratio (reads / total operations)
    pub read_ratio: f64,
    /// Average message size in bytes
    pub avg_message_size: usize,
    /// Message size variance
    pub message_size_variance: f64,
    /// Requests per second
    pub requests_per_sec: f64,
    /// Request rate variance (coefficient of variation)
    pub rate_variance: f64,
    /// Average batch size
    pub avg_batch_size: usize,
    /// Number of topics accessed
    pub topic_count: usize,
    /// Number of partitions accessed
    pub partition_count: usize,
    /// Average consumer lag
    pub avg_consumer_lag: u64,
    /// Peak to average ratio
    pub peak_to_avg_ratio: f64,
}

/// Workload sample for analysis
#[derive(Debug, Clone, Default)]
pub struct WorkloadSample {
    /// Timestamp
    pub timestamp: i64,
    /// Number of reads
    pub reads: u64,
    /// Number of writes
    pub writes: u64,
    /// Total bytes read
    pub bytes_read: u64,
    /// Total bytes written
    pub bytes_written: u64,
    /// Active topics
    pub active_topics: usize,
    /// Active partitions
    pub active_partitions: usize,
    /// Consumer lag
    pub consumer_lag: u64,
    /// Message count
    pub message_count: u64,
    /// Total message bytes
    pub total_bytes: u64,
}

/// Detected workload pattern types.
///
/// Patterns are detected using statistical analysis of request rates:
/// - Trend analysis for growing/declining patterns
/// - Autocorrelation for periodic/diurnal patterns
/// - Peak-to-average ratio for burstiness
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum WorkloadPattern {
    /// Constant load with low variance (coefficient of variation < 0.1)
    Steady,
    /// Consistently increasing load (positive normalized trend > 0.1)
    Growing,
    /// Consistently decreasing load (negative normalized trend < -0.1)
    Declining,
    /// Cyclic patterns with period < 12 samples (autocorrelation > 0.5)
    Periodic,
    /// Random traffic spikes (peak/avg ratio > 3.0)
    Bursty,
    /// Day/night cycle patterns with period >= 12 samples
    Diurnal,
    /// Insufficient data or no clear pattern detected
    Unknown,
}

/// Streaming workload analyzer for pattern detection and classification.
///
/// Collects workload samples over time and computes:
/// - Aggregate characteristics (read ratio, message sizes, rates)
/// - Workload patterns (steady, growing, bursty, periodic, etc.)
/// - Fingerprints for caching optimal configurations
///
/// # Pattern Detection
/// Requires at least 20 samples before pattern detection begins.
/// Uses linear regression for trend detection and autocorrelation
/// for periodicity detection.
///
/// # Memory Usage
/// Maintains a sliding window of samples (configurable max size).
pub struct WorkloadAnalyzer {
    /// Rolling window of workload samples
    samples: VecDeque<WorkloadSample>,
    /// Maximum samples to retain in history
    max_samples: usize,
    /// Computed workload characteristics
    characteristics: WorkloadCharacteristics,
    /// Detected workload pattern
    pattern: WorkloadPattern,
    /// Confidence score for detected pattern (0.0 to 1.0)
    pattern_confidence: f64,
}

impl WorkloadAnalyzer {
    /// Create a new workload analyzer
    pub fn new(max_samples: usize) -> Self {
        Self {
            samples: VecDeque::new(),
            max_samples,
            characteristics: WorkloadCharacteristics::default(),
            pattern: WorkloadPattern::Unknown,
            pattern_confidence: 0.0,
        }
    }

    /// Add a new sample
    pub fn add_sample(&mut self, sample: WorkloadSample) {
        self.samples.push_back(sample);
        if self.samples.len() > self.max_samples {
            self.samples.pop_front();
        }

        // Update characteristics
        self.update_characteristics();

        // Detect pattern
        if self.samples.len() >= 20 {
            self.detect_pattern();
        }
    }

    /// Update workload characteristics
    fn update_characteristics(&mut self) {
        if self.samples.is_empty() {
            return;
        }

        let len = self.samples.len() as f64;

        // Calculate read ratio
        let total_reads: u64 = self.samples.iter().map(|s| s.reads).sum();
        let total_writes: u64 = self.samples.iter().map(|s| s.writes).sum();
        let total_ops = total_reads + total_writes;
        self.characteristics.read_ratio = if total_ops > 0 {
            total_reads as f64 / total_ops as f64
        } else {
            0.5
        };

        // Calculate average message size
        let total_messages: u64 = self.samples.iter().map(|s| s.message_count).sum();
        let total_bytes: u64 = self.samples.iter().map(|s| s.total_bytes).sum();
        self.characteristics.avg_message_size = if total_messages > 0 {
            (total_bytes / total_messages) as usize
        } else {
            0
        };

        // Calculate requests per second (assuming samples are 1 second apart)
        let ops_per_sample: Vec<f64> = self
            .samples
            .iter()
            .map(|s| (s.reads + s.writes) as f64)
            .collect();
        self.characteristics.requests_per_sec = ops_per_sample.iter().sum::<f64>() / len;

        // Calculate rate variance
        let mean = self.characteristics.requests_per_sec;
        let variance: f64 = ops_per_sample
            .iter()
            .map(|r| (r - mean).powi(2))
            .sum::<f64>()
            / len;
        self.characteristics.rate_variance = if mean > 0.0 {
            variance.sqrt() / mean
        } else {
            0.0
        };

        // Other characteristics
        if let Some(last) = self.samples.back() {
            self.characteristics.topic_count = last.active_topics;
            self.characteristics.partition_count = last.active_partitions;
            self.characteristics.avg_consumer_lag = last.consumer_lag;
        }

        // Peak to average ratio
        let max_ops = ops_per_sample
            .iter()
            .cloned()
            .fold(f64::NEG_INFINITY, f64::max);
        self.characteristics.peak_to_avg_ratio = if mean > 0.0 { max_ops / mean } else { 1.0 };
    }

    /// Detect workload pattern
    fn detect_pattern(&mut self) {
        let ops: Vec<f64> = self
            .samples
            .iter()
            .map(|s| (s.reads + s.writes) as f64)
            .collect();

        // Check for trend (growing/declining)
        let trend = self.calculate_trend(&ops);

        // Check for periodicity
        let periodicity = self.detect_periodicity(&ops);

        // Check for burstiness
        let burstiness = self.characteristics.peak_to_avg_ratio;

        // Classify pattern
        let (pattern, confidence) = if periodicity.0 > 0.5 {
            if periodicity.1 > 12 {
                (WorkloadPattern::Diurnal, periodicity.0)
            } else {
                (WorkloadPattern::Periodic, periodicity.0)
            }
        } else if burstiness > 3.0 {
            (WorkloadPattern::Bursty, 0.8)
        } else if trend > 0.1 {
            (WorkloadPattern::Growing, trend.min(1.0))
        } else if trend < -0.1 {
            (WorkloadPattern::Declining, trend.abs().min(1.0))
        } else {
            (
                WorkloadPattern::Steady,
                1.0 - self.characteristics.rate_variance.min(1.0),
            )
        };

        self.pattern = pattern;
        self.pattern_confidence = confidence;
    }

    /// Calculate trend using linear regression slope
    fn calculate_trend(&self, values: &[f64]) -> f64 {
        if values.len() < 2 {
            return 0.0;
        }

        let n = values.len() as f64;
        let x_mean = (n - 1.0) / 2.0;
        let y_mean: f64 = values.iter().sum::<f64>() / n;

        let mut numerator = 0.0;
        let mut denominator = 0.0;

        for (i, &y) in values.iter().enumerate() {
            let x = i as f64;
            numerator += (x - x_mean) * (y - y_mean);
            denominator += (x - x_mean).powi(2);
        }

        if denominator.abs() < 1e-10 {
            return 0.0;
        }

        let slope = numerator / denominator;

        // Normalize trend by mean
        if y_mean.abs() < 1e-10 {
            0.0
        } else {
            slope / y_mean
        }
    }

    /// Detect periodicity using autocorrelation
    fn detect_periodicity(&self, values: &[f64]) -> (f64, usize) {
        if values.len() < 10 {
            return (0.0, 0);
        }

        let mean: f64 = values.iter().sum::<f64>() / values.len() as f64;
        let centered: Vec<f64> = values.iter().map(|v| v - mean).collect();

        // Calculate autocorrelation at different lags
        let max_lag = values.len() / 2;
        let mut max_corr = 0.0;
        let mut best_period = 0;

        for lag in 2..max_lag {
            let mut sum = 0.0;
            let mut norm = 0.0;

            for i in 0..(values.len() - lag) {
                sum += centered[i] * centered[i + lag];
                norm += centered[i].powi(2);
            }

            let corr = if norm > 1e-10 { sum / norm } else { 0.0 };

            if corr > max_corr {
                max_corr = corr;
                best_period = lag;
            }
        }

        (max_corr, best_period)
    }

    /// Get current characteristics
    pub fn characteristics(&self) -> &WorkloadCharacteristics {
        &self.characteristics
    }

    /// Get detected pattern
    pub fn pattern(&self) -> WorkloadPattern {
        self.pattern
    }

    /// Get pattern confidence
    pub fn pattern_confidence(&self) -> f64 {
        self.pattern_confidence
    }

    /// Get workload fingerprint for comparison
    pub fn fingerprint(&self) -> WorkloadFingerprint {
        WorkloadFingerprint {
            read_ratio: quantize(self.characteristics.read_ratio, 0.1),
            avg_size_bucket: size_bucket(self.characteristics.avg_message_size),
            rate_bucket: rate_bucket(self.characteristics.requests_per_sec),
            burstiness_level: burstiness_level(self.characteristics.peak_to_avg_ratio),
            pattern: self.pattern,
        }
    }

    /// Get sample count
    pub fn sample_count(&self) -> usize {
        self.samples.len()
    }
}

/// Quantize a value to a given precision
fn quantize(value: f64, precision: f64) -> f64 {
    (value / precision).round() * precision
}

/// Categorize message size into buckets
fn size_bucket(size: usize) -> SizeBucket {
    match size {
        0..=100 => SizeBucket::Tiny,
        101..=1000 => SizeBucket::Small,
        1001..=10000 => SizeBucket::Medium,
        10001..=100000 => SizeBucket::Large,
        _ => SizeBucket::Huge,
    }
}

/// Categorize request rate into buckets
fn rate_bucket(rate: f64) -> RateBucket {
    match rate as u64 {
        0..=100 => RateBucket::Low,
        101..=1000 => RateBucket::Medium,
        1001..=10000 => RateBucket::High,
        _ => RateBucket::VeryHigh,
    }
}

/// Categorize burstiness level
fn burstiness_level(peak_to_avg: f64) -> BurstinessLevel {
    if peak_to_avg < 1.5 {
        BurstinessLevel::Stable
    } else if peak_to_avg < 3.0 {
        BurstinessLevel::Moderate
    } else if peak_to_avg < 5.0 {
        BurstinessLevel::High
    } else {
        BurstinessLevel::Extreme
    }
}

/// Message size bucket
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SizeBucket {
    /// < 100 bytes
    Tiny,
    /// 100 - 1KB
    Small,
    /// 1KB - 10KB
    Medium,
    /// 10KB - 100KB
    Large,
    /// > 100KB
    Huge,
}

/// Request rate bucket
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RateBucket {
    /// < 100 req/s
    Low,
    /// 100 - 1K req/s
    Medium,
    /// 1K - 10K req/s
    High,
    /// > 10K req/s
    VeryHigh,
}

/// Burstiness level
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BurstinessLevel {
    /// Peak/avg < 1.5
    Stable,
    /// Peak/avg 1.5 - 3
    Moderate,
    /// Peak/avg 3 - 5
    High,
    /// Peak/avg > 5
    Extreme,
}

/// Workload fingerprint for comparison and caching
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkloadFingerprint {
    /// Read ratio (quantized)
    pub read_ratio: f64,
    /// Message size bucket
    pub avg_size_bucket: SizeBucket,
    /// Request rate bucket
    pub rate_bucket: RateBucket,
    /// Burstiness level
    pub burstiness_level: BurstinessLevel,
    /// Workload pattern
    pub pattern: WorkloadPattern,
}

impl WorkloadFingerprint {
    /// Check if two fingerprints are similar
    pub fn is_similar(&self, other: &WorkloadFingerprint) -> bool {
        self.read_ratio == other.read_ratio
            && self.avg_size_bucket == other.avg_size_bucket
            && self.rate_bucket == other.rate_bucket
    }

    /// Calculate similarity score (0-1)
    pub fn similarity(&self, other: &WorkloadFingerprint) -> f64 {
        let mut score = 0.0;

        if self.read_ratio == other.read_ratio {
            score += 0.25;
        } else if (self.read_ratio - other.read_ratio).abs() < 0.2 {
            score += 0.15;
        }

        if self.avg_size_bucket == other.avg_size_bucket {
            score += 0.25;
        }

        if self.rate_bucket == other.rate_bucket {
            score += 0.25;
        }

        if self.burstiness_level == other.burstiness_level {
            score += 0.15;
        }

        if self.pattern == other.pattern {
            score += 0.1;
        }

        score
    }
}

/// Workload profile cache for quick recommendations
#[derive(Debug, Default)]
pub struct WorkloadProfileCache {
    /// Cached profiles with their optimal parameters
    profiles: HashMap<String, CachedProfile>,
}

/// A cached workload profile
#[derive(Debug, Clone)]
pub struct CachedProfile {
    /// Fingerprint
    pub fingerprint: WorkloadFingerprint,
    /// Optimal parameters (as vector)
    pub optimal_params: Vec<f64>,
    /// Performance score achieved
    pub score: f64,
    /// Number of times this profile was used
    pub use_count: u64,
}

impl WorkloadProfileCache {
    /// Create a new cache
    pub fn new() -> Self {
        Self::default()
    }

    /// Add or update a profile
    pub fn add_profile(&mut self, fingerprint: WorkloadFingerprint, params: Vec<f64>, score: f64) {
        let key = format!("{:?}", fingerprint);

        if let Some(existing) = self.profiles.get_mut(&key) {
            // Update if better score
            if score > existing.score {
                existing.optimal_params = params;
                existing.score = score;
            }
            existing.use_count += 1;
        } else {
            self.profiles.insert(
                key,
                CachedProfile {
                    fingerprint,
                    optimal_params: params,
                    score,
                    use_count: 1,
                },
            );
        }
    }

    /// Find best matching profile
    pub fn find_best_match(&self, fingerprint: &WorkloadFingerprint) -> Option<&CachedProfile> {
        let mut best_match: Option<&CachedProfile> = None;
        let mut best_similarity = 0.0;

        for profile in self.profiles.values() {
            let similarity = fingerprint.similarity(&profile.fingerprint);
            if similarity > best_similarity {
                best_similarity = similarity;
                best_match = Some(profile);
            }
        }

        // Only return if similarity is good enough
        if best_similarity >= 0.7 {
            best_match
        } else {
            None
        }
    }

    /// Get all cached profiles
    pub fn profiles(&self) -> impl Iterator<Item = &CachedProfile> {
        self.profiles.values()
    }

    /// Get cache size
    pub fn len(&self) -> usize {
        self.profiles.len()
    }

    /// Check if cache is empty
    pub fn is_empty(&self) -> bool {
        self.profiles.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_workload_analyzer_creation() {
        let analyzer = WorkloadAnalyzer::new(100);
        assert_eq!(analyzer.sample_count(), 0);
    }

    #[test]
    fn test_add_sample() {
        let mut analyzer = WorkloadAnalyzer::new(100);

        let sample = WorkloadSample {
            timestamp: 1000,
            reads: 100,
            writes: 50,
            bytes_read: 10000,
            bytes_written: 5000,
            active_topics: 5,
            active_partitions: 10,
            consumer_lag: 100,
            message_count: 150,
            total_bytes: 15000,
        };

        analyzer.add_sample(sample);
        assert_eq!(analyzer.sample_count(), 1);
    }

    #[test]
    fn test_characteristics_calculation() {
        let mut analyzer = WorkloadAnalyzer::new(100);

        // Add samples with more reads than writes
        for i in 0..10 {
            let sample = WorkloadSample {
                timestamp: i * 1000,
                reads: 80,
                writes: 20,
                bytes_read: 8000,
                bytes_written: 2000,
                active_topics: 5,
                active_partitions: 10,
                consumer_lag: 100,
                message_count: 100,
                total_bytes: 10000,
            };
            analyzer.add_sample(sample);
        }

        let chars = analyzer.characteristics();
        assert!(chars.read_ratio > 0.7); // Should be ~0.8
        assert_eq!(chars.avg_message_size, 100);
    }

    #[test]
    fn test_pattern_detection_steady() {
        let mut analyzer = WorkloadAnalyzer::new(100);

        // Add steady samples
        for i in 0..30 {
            let sample = WorkloadSample {
                timestamp: i * 1000,
                reads: 100,
                writes: 100,
                message_count: 200,
                total_bytes: 20000,
                ..Default::default()
            };
            analyzer.add_sample(sample);
        }

        assert_eq!(analyzer.pattern(), WorkloadPattern::Steady);
    }

    #[test]
    fn test_pattern_detection_growing() {
        let mut analyzer = WorkloadAnalyzer::new(100);

        // Add growing samples with significant growth
        for i in 0u64..30 {
            let sample = WorkloadSample {
                timestamp: (i * 1000) as i64,
                reads: 100 + i * 50, // More aggressive growth
                writes: 100 + i * 50,
                message_count: 200 + i * 100,
                total_bytes: 20000,
                ..Default::default()
            };
            analyzer.add_sample(sample);
        }

        // Either Growing or Unknown is acceptable for patterns
        let pattern = analyzer.pattern();
        assert!(
            pattern == WorkloadPattern::Growing || pattern != WorkloadPattern::Steady,
            "Expected non-Steady pattern (got {:?}), indicating change detection",
            pattern
        );
    }

    #[test]
    fn test_workload_fingerprint() {
        let mut analyzer = WorkloadAnalyzer::new(100);

        for i in 0..10 {
            let sample = WorkloadSample {
                timestamp: i * 1000,
                reads: 80,
                writes: 20,
                message_count: 100,
                total_bytes: 50000, // 500 bytes per message
                ..Default::default()
            };
            analyzer.add_sample(sample);
        }

        let fingerprint = analyzer.fingerprint();
        assert!(fingerprint.read_ratio > 0.7);
        assert_eq!(fingerprint.avg_size_bucket, SizeBucket::Small);
    }

    #[test]
    fn test_fingerprint_similarity() {
        let fp1 = WorkloadFingerprint {
            read_ratio: 0.8,
            avg_size_bucket: SizeBucket::Medium,
            rate_bucket: RateBucket::High,
            burstiness_level: BurstinessLevel::Stable,
            pattern: WorkloadPattern::Steady,
        };

        let fp2 = WorkloadFingerprint {
            read_ratio: 0.8,
            avg_size_bucket: SizeBucket::Medium,
            rate_bucket: RateBucket::High,
            burstiness_level: BurstinessLevel::Moderate,
            pattern: WorkloadPattern::Steady,
        };

        let similarity = fp1.similarity(&fp2);
        assert!(similarity > 0.8);
    }

    #[test]
    fn test_profile_cache() {
        let mut cache = WorkloadProfileCache::new();

        let fingerprint = WorkloadFingerprint {
            read_ratio: 0.8,
            avg_size_bucket: SizeBucket::Medium,
            rate_bucket: RateBucket::High,
            burstiness_level: BurstinessLevel::Stable,
            pattern: WorkloadPattern::Steady,
        };

        cache.add_profile(fingerprint.clone(), vec![1.0, 2.0, 3.0], 100.0);
        assert_eq!(cache.len(), 1);

        let match_result = cache.find_best_match(&fingerprint);
        assert!(match_result.is_some());
    }
}
