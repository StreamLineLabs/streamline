//! Partition skew analyzer — detects throughput imbalances.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Configuration for the skew analyzer.
#[derive(Debug, Clone)]
pub struct SkewAnalyzerConfig {
    /// Skew threshold: a partition with >N% of total throughput is "hot"
    pub hot_threshold_pct: f64,
    /// Cold threshold: a partition with <N% of expected share is "cold"
    pub cold_threshold_pct: f64,
    /// Minimum observation window in seconds
    pub observation_window_secs: u64,
    /// Number of samples to keep
    pub max_samples: usize,
}

impl Default for SkewAnalyzerConfig {
    fn default() -> Self {
        Self {
            hot_threshold_pct: 30.0,
            cold_threshold_pct: 5.0,
            observation_window_secs: 60,
            max_samples: 100,
        }
    }
}

/// Per-partition metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionMetrics {
    pub topic: String,
    pub partition: i32,
    /// Messages per second
    pub throughput_mps: f64,
    /// Bytes per second
    pub throughput_bps: f64,
    /// Consumer lag (total across all groups)
    pub consumer_lag: i64,
    /// Number of unique keys seen in sampling window
    pub unique_keys: u64,
    /// Estimated storage size in bytes
    pub size_bytes: u64,
}

/// Severity of partition skew.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SkewSeverity {
    None,
    Low,
    Medium,
    High,
    Critical,
}

/// Skew information for a single partition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionSkew {
    pub partition: i32,
    /// Throughput share as percentage
    pub throughput_share_pct: f64,
    /// Expected share (1/N partitions)
    pub expected_share_pct: f64,
    /// Deviation from expected
    pub deviation_pct: f64,
    /// Whether this partition is a hot spot
    pub is_hot: bool,
    /// Whether this partition is cold
    pub is_cold: bool,
}

/// Report on partition skew for a topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkewReport {
    pub topic: String,
    pub num_partitions: i32,
    pub severity: SkewSeverity,
    /// Skew coefficient (0 = perfectly balanced, 1 = fully skewed)
    pub skew_coefficient: f64,
    /// Per-partition analysis
    pub partitions: Vec<PartitionSkew>,
    /// Total throughput (msg/s)
    pub total_throughput_mps: f64,
    /// Recommended action
    pub recommendation: String,
}

/// Analyzes partition throughput distribution and detects skew.
pub struct SkewAnalyzer {
    config: SkewAnalyzerConfig,
    samples: HashMap<String, Vec<Vec<PartitionMetrics>>>,
}

impl SkewAnalyzer {
    /// Create a new skew analyzer.
    pub fn new(config: SkewAnalyzerConfig) -> Self {
        Self {
            config,
            samples: HashMap::new(),
        }
    }

    /// Record a metrics sample for a topic.
    pub fn record_sample(&mut self, topic: &str, metrics: Vec<PartitionMetrics>) {
        let samples = self.samples.entry(topic.to_string()).or_default();
        samples.push(metrics);
        if samples.len() > self.config.max_samples {
            samples.remove(0);
        }
    }

    /// Analyze skew for a topic based on recorded samples.
    pub fn analyze(&self, topic: &str) -> Option<SkewReport> {
        let samples = self.samples.get(topic)?;
        if samples.is_empty() {
            return None;
        }

        // Use the latest sample
        let latest = samples.last()?;
        let total_throughput: f64 = latest.iter().map(|m| m.throughput_mps).sum();

        if total_throughput == 0.0 {
            return Some(SkewReport {
                topic: topic.to_string(),
                num_partitions: latest.len() as i32,
                severity: SkewSeverity::None,
                skew_coefficient: 0.0,
                partitions: Vec::new(),
                total_throughput_mps: 0.0,
                recommendation: "No traffic detected".to_string(),
            });
        }

        let num_partitions = latest.len() as f64;
        let expected_share = 100.0 / num_partitions;

        let partition_skews: Vec<PartitionSkew> = latest
            .iter()
            .map(|m| {
                let share = (m.throughput_mps / total_throughput) * 100.0;
                let deviation = share - expected_share;
                PartitionSkew {
                    partition: m.partition,
                    throughput_share_pct: share,
                    expected_share_pct: expected_share,
                    deviation_pct: deviation,
                    is_hot: share > self.config.hot_threshold_pct,
                    is_cold: share < self.config.cold_threshold_pct,
                }
            })
            .collect();

        // Calculate skew coefficient using coefficient of variation
        let mean = total_throughput / num_partitions;
        let variance: f64 = latest
            .iter()
            .map(|m| (m.throughput_mps - mean).powi(2))
            .sum::<f64>()
            / num_partitions;
        let std_dev = variance.sqrt();
        let skew_coefficient = if mean > 0.0 { std_dev / mean } else { 0.0 };

        let severity = if skew_coefficient > 1.5 {
            SkewSeverity::Critical
        } else if skew_coefficient > 1.0 {
            SkewSeverity::High
        } else if skew_coefficient > 0.5 {
            SkewSeverity::Medium
        } else if skew_coefficient > 0.2 {
            SkewSeverity::Low
        } else {
            SkewSeverity::None
        };

        let hot_count = partition_skews.iter().filter(|p| p.is_hot).count();
        let recommendation = match &severity {
            SkewSeverity::None => "Partitions are well-balanced".to_string(),
            SkewSeverity::Low => "Minor imbalance — monitor but no action needed".to_string(),
            SkewSeverity::Medium => format!(
                "Moderate skew detected ({} hot partitions) — consider rebalancing",
                hot_count
            ),
            SkewSeverity::High | SkewSeverity::Critical => format!(
                "Severe skew ({} hot partitions, coefficient {:.2}) — rebalancing recommended",
                hot_count, skew_coefficient
            ),
        };

        Some(SkewReport {
            topic: topic.to_string(),
            num_partitions: latest.len() as i32,
            severity,
            skew_coefficient,
            partitions: partition_skews,
            total_throughput_mps: total_throughput,
            recommendation,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_metrics(topic: &str, throughputs: &[f64]) -> Vec<PartitionMetrics> {
        throughputs
            .iter()
            .enumerate()
            .map(|(i, &tp)| PartitionMetrics {
                topic: topic.to_string(),
                partition: i as i32,
                throughput_mps: tp,
                throughput_bps: tp * 100.0,
                consumer_lag: 0,
                unique_keys: 100,
                size_bytes: 0,
            })
            .collect()
    }

    #[test]
    fn test_balanced_partitions() {
        let mut analyzer = SkewAnalyzer::new(SkewAnalyzerConfig::default());
        analyzer.record_sample(
            "events",
            make_metrics("events", &[100.0, 100.0, 100.0, 100.0]),
        );

        let report = analyzer.analyze("events").unwrap();
        assert_eq!(report.severity, SkewSeverity::None);
        assert!(report.skew_coefficient < 0.1);
    }

    #[test]
    fn test_skewed_partitions() {
        let mut analyzer = SkewAnalyzer::new(SkewAnalyzerConfig::default());
        analyzer.record_sample("events", make_metrics("events", &[900.0, 50.0, 30.0, 20.0]));

        let report = analyzer.analyze("events").unwrap();
        assert!(report.severity == SkewSeverity::High || report.severity == SkewSeverity::Critical);
        assert!(report.partitions[0].is_hot);
    }

    #[test]
    fn test_no_traffic() {
        let mut analyzer = SkewAnalyzer::new(SkewAnalyzerConfig::default());
        analyzer.record_sample("empty", make_metrics("empty", &[0.0, 0.0, 0.0]));

        let report = analyzer.analyze("empty").unwrap();
        assert_eq!(report.severity, SkewSeverity::None);
    }
}
