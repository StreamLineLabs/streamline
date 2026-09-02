//! Partition skew analyzer — detects throughput imbalances using statistical
//! methods including Gini coefficient, standard deviation thresholds,
//! HyperLogLog-style cardinality estimation, and Count-Min sketch approximation.

use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

// ── Configuration ───────────────────────────────────────────────────────

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
    /// P99 latency threshold (ms) used by health scoring
    pub p99_latency_threshold_ms: f64,
    /// Target throughput per partition (msg/s) for health scoring (0 = auto)
    pub target_throughput_mps: f64,
}

impl Default for SkewAnalyzerConfig {
    fn default() -> Self {
        Self {
            hot_threshold_pct: 30.0,
            cold_threshold_pct: 5.0,
            observation_window_secs: 60,
            max_samples: 100,
            p99_latency_threshold_ms: 100.0,
            target_throughput_mps: 0.0,
        }
    }
}

// ── Core data types ─────────────────────────────────────────────────────

/// Per-partition metrics snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionMetrics {
    pub topic: String,
    pub partition: i32,
    /// Messages per second (`message_rate`)
    pub throughput_mps: f64,
    /// Bytes per second (`bytes_rate`)
    pub throughput_bps: f64,
    /// Consumer lag (total across all groups)
    pub consumer_lag: i64,
    /// Number of unique keys seen in sampling window (`key_count`)
    pub unique_keys: u64,
    /// Estimated storage size in bytes
    pub size_bytes: u64,
    /// Fraction of traffic from the single hottest key (0.0-1.0)
    pub hot_key_ratio: f64,
    /// P99 end-to-end latency in milliseconds
    pub p99_latency_ms: f64,
}

/// Severity of partition skew.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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

/// Legacy report on partition skew for a topic.
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

// ── New analysis types ──────────────────────────────────────────────────

/// Risk level for rebalance operations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RiskLevel {
    Low,
    Medium,
    High,
}

/// Distribution pattern of message keys.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DistributionType {
    /// Keys are roughly uniformly distributed
    Uniform,
    /// Zipf-like power-law distribution (few hot keys)
    Zipfian,
    /// Keys cluster around shared prefixes
    Clustered,
    /// Keys are monotonically increasing (timestamps, sequences)
    MonotonicKey,
}

/// A cluster of keys sharing a common prefix.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrefixCluster {
    pub prefix: String,
    pub key_count: u64,
    pub share_pct: f64,
}

/// Composite health score for a topic's partition layout (0.0-1.0).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthScore {
    /// Weighted overall score
    pub overall: f64,
    /// Balance across partitions (1.0 = perfectly even)
    pub balance_score: f64,
    /// Throughput relative to capacity (1.0 = all partitions active)
    pub throughput_score: f64,
    /// Latency health (1.0 = all partitions below threshold)
    pub latency_score: f64,
}

/// Rich analysis of partition throughput skew (Gini-based).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkewAnalysis {
    pub topic: String,
    /// Gini coefficient (0.0 = perfect equality, ~1.0 = max inequality)
    pub skew_coefficient: f64,
    /// Partition IDs with throughput >2 sigma above mean
    pub hot_partitions: Vec<i32>,
    /// Partition IDs with throughput <0.5x mean
    pub cold_partitions: Vec<i32>,
    /// Human-readable recommendations
    pub recommendations: Vec<String>,
    /// Composite health score
    pub health_score: HealthScore,
    /// Per-partition breakdown
    pub partition_details: Vec<PartitionSkew>,
    /// Total message throughput (msg/s)
    pub total_throughput_mps: f64,
    /// Severity classification
    pub severity: SkewSeverity,
}

/// Analysis of message key distribution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyDistributionAnalysis {
    /// Estimated unique key cardinality (HyperLogLog-style)
    pub estimated_cardinality: u64,
    /// Top-K hot keys by count (Count-Min sketch approximation)
    pub top_k_keys: Vec<(String, u64)>,
    /// Detected prefix clusters
    pub prefix_clusters: Vec<PrefixCluster>,
    /// Classified distribution pattern
    pub distribution_type: DistributionType,
}

/// Predicted benefit of rebalancing to a different partition count.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RebalanceBenefit {
    /// Expected Gini improvement (positive = better)
    pub estimated_skew_improvement: f64,
    /// Estimated bytes that must be relocated
    pub estimated_data_movement_bytes: u64,
    /// Risk assessment
    pub risk: RiskLevel,
    /// Human-readable recommendation
    pub recommendation: String,
}

// ── Algorithms ──────────────────────────────────────────────────────────

/// Calculate the Gini coefficient for a slice of non-negative values.
///
/// Returns 0.0 for perfect equality and approaches `(n-1)/n` for maximum
/// inequality. An empty or all-zero slice returns 0.0.
pub fn gini_coefficient(values: &[f64]) -> f64 {
    let n = values.len();
    if n < 2 {
        return 0.0;
    }
    let sum: f64 = values.iter().sum();
    if sum <= 0.0 {
        return 0.0;
    }

    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let nf = n as f64;
    let weighted_sum: f64 = sorted
        .iter()
        .enumerate()
        .map(|(i, &x)| (i as f64 + 1.0) * x)
        .sum();

    (2.0 * weighted_sum) / (nf * sum) - (nf + 1.0) / nf
}

// ── HyperLogLog cardinality estimation ──────────────────────────────────

const HLL_NUM_BUCKETS: usize = 64;
const HLL_BUCKET_BITS: u32 = 6; // log2(64)

fn hash_key_to_u64(key: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

/// Estimate cardinality using a simplified HyperLogLog algorithm.
fn estimate_cardinality_hll<'a>(keys: impl Iterator<Item = &'a String>) -> u64 {
    let mut registers = [0u8; HLL_NUM_BUCKETS];
    let mut seen_any = false;

    for key in keys {
        seen_any = true;
        let hash = hash_key_to_u64(key);
        let bucket = (hash >> (64 - HLL_BUCKET_BITS)) as usize;
        let remaining = hash << HLL_BUCKET_BITS;
        let leading_zeros = remaining.leading_zeros() as u8 + 1;
        if leading_zeros > registers[bucket] {
            registers[bucket] = leading_zeros;
        }
    }

    if !seen_any {
        return 0;
    }

    let alpha = 0.709; // bias correction for m = 64
    let m = HLL_NUM_BUCKETS as f64;
    let harmonic_sum: f64 = registers.iter().map(|&r| 2.0_f64.powi(-(r as i32))).sum();
    let raw_estimate = alpha * m * m / harmonic_sum;

    // Small-range correction
    if raw_estimate <= 2.5 * m {
        let zeros = registers.iter().filter(|&&r| r == 0).count() as f64;
        if zeros > 0.0 {
            return (m * (m / zeros).ln()) as u64;
        }
    }

    raw_estimate as u64
}

// ── Count-Min sketch for top-K estimation ───────────────────────────────

const CM_DEPTH: usize = 4;
const CM_WIDTH: usize = 256;
const CM_SEEDS: [u64; CM_DEPTH] = [
    0x9e3779b97f4a7c15,
    0x517cc1b727220a95,
    0x6c62272e07bb0142,
    0x62b821756295c58d,
];

fn cm_hash(key: &str, seed: u64) -> usize {
    let mut hasher = DefaultHasher::new();
    seed.hash(&mut hasher);
    key.hash(&mut hasher);
    (hasher.finish() as usize) % CM_WIDTH
}

fn cm_estimate(counters: &[[u64; CM_WIDTH]; CM_DEPTH], key: &str) -> u64 {
    (0..CM_DEPTH)
        .map(|d| counters[d][cm_hash(key, CM_SEEDS[d])])
        .min()
        .unwrap_or(0)
}

// ── Public analysis functions ───────────────────────────────────────────

/// Analyze throughput skew across partitions using Gini coefficient and
/// combined statistical/practical thresholds.
///
/// A partition is classified as **hot** when its throughput exceeds
/// `mean + 2σ` *or* its share exceeds `2× expected_share` (whichever
/// triggers first).  Partitions below `0.5× mean` are **cold**.
/// Monotonic-key patterns are detected via `hot_key_ratio`.
pub fn analyze_throughput_skew(metrics: &[PartitionMetrics]) -> SkewAnalysis {
    let topic = metrics.first().map(|m| m.topic.clone()).unwrap_or_default();

    if metrics.is_empty() {
        return SkewAnalysis {
            topic,
            skew_coefficient: 0.0,
            hot_partitions: Vec::new(),
            cold_partitions: Vec::new(),
            recommendations: vec!["No partitions to analyze".into()],
            health_score: HealthScore {
                overall: 1.0,
                balance_score: 1.0,
                throughput_score: 1.0,
                latency_score: 1.0,
            },
            partition_details: Vec::new(),
            total_throughput_mps: 0.0,
            severity: SkewSeverity::None,
        };
    }

    let rates: Vec<f64> = metrics.iter().map(|m| m.throughput_mps).collect();
    let total: f64 = rates.iter().sum();
    let n = rates.len() as f64;
    let mean = if n > 0.0 { total / n } else { 0.0 };

    let gini = gini_coefficient(&rates);

    // Standard deviation for hot/cold thresholds
    let variance: f64 = rates.iter().map(|&r| (r - mean).powi(2)).sum::<f64>() / n;
    let std_dev = variance.sqrt();

    let hot_threshold = mean + 2.0 * std_dev;
    let cold_threshold = mean * 0.5;
    let expected_share = if n > 0.0 { 100.0 / n } else { 0.0 };

    let mut hot_partitions = Vec::new();
    let mut cold_partitions = Vec::new();
    let mut partition_details = Vec::new();

    for m in metrics {
        let share = if total > 0.0 {
            (m.throughput_mps / total) * 100.0
        } else {
            expected_share
        };
        // Hot: statistical outlier (>2σ) OR practical outlier (>2× fair share)
        let is_hot =
            total > 0.0 && (m.throughput_mps > hot_threshold || share > expected_share * 2.0);
        let is_cold = total > 0.0 && m.throughput_mps < cold_threshold;

        if is_hot {
            hot_partitions.push(m.partition);
        }
        if is_cold {
            cold_partitions.push(m.partition);
        }

        partition_details.push(PartitionSkew {
            partition: m.partition,
            throughput_share_pct: share,
            expected_share_pct: expected_share,
            deviation_pct: share - expected_share,
            is_hot,
            is_cold,
        });
    }

    // Detect monotonic key patterns on hot partitions
    // Detect monotonic key patterns on hot partitions (use same combined criterion)
    let has_monotonic_keys = metrics.iter().any(|m| {
        let share = if total > 0.0 {
            (m.throughput_mps / total) * 100.0
        } else {
            0.0
        };
        m.hot_key_ratio > 0.8 && (m.throughput_mps > hot_threshold || share > expected_share * 2.0)
    });

    let mut recommendations = Vec::new();
    if !hot_partitions.is_empty() {
        recommendations.push(format!(
            "Hot partitions detected: {hot_partitions:?} -- consider redistributing keys"
        ));
    }
    if !cold_partitions.is_empty() {
        recommendations.push(format!(
            "Cold partitions: {cold_partitions:?} -- candidates for merging or removal"
        ));
    }
    if has_monotonic_keys {
        recommendations.push(
            "Monotonic key pattern detected -- use salted/hashed keys to avoid hotspotting".into(),
        );
    }
    if gini > 0.5 {
        recommendations.push(format!(
            "High Gini coefficient ({gini:.3}) -- rebalancing strongly recommended"
        ));
    }
    if recommendations.is_empty() {
        recommendations.push("Partitions are well-balanced".into());
    }

    let severity = severity_from_gini(gini);
    let health_score = calculate_health_score(metrics, gini);

    SkewAnalysis {
        topic,
        skew_coefficient: gini,
        hot_partitions,
        cold_partitions,
        recommendations,
        health_score,
        partition_details,
        total_throughput_mps: total,
        severity,
    }
}

/// Analyze the distribution of message keys.
///
/// Uses a HyperLogLog-style estimator for cardinality, a Count-Min sketch
/// for top-K hot-key detection, and prefix analysis for clustering.
pub fn analyze_key_distribution(keys: &HashMap<String, u64>) -> KeyDistributionAnalysis {
    if keys.is_empty() {
        return KeyDistributionAnalysis {
            estimated_cardinality: 0,
            top_k_keys: Vec::new(),
            prefix_clusters: Vec::new(),
            distribution_type: DistributionType::Uniform,
        };
    }

    let estimated_cardinality = estimate_cardinality_hll(keys.keys());
    let total_count: u64 = keys.values().sum();

    // Count-Min sketch for top-K
    let mut counters = [[0u64; CM_WIDTH]; CM_DEPTH];
    for (key, &count) in keys {
        for d in 0..CM_DEPTH {
            let idx = cm_hash(key, CM_SEEDS[d]);
            counters[d][idx] = counters[d][idx].saturating_add(count);
        }
    }

    let mut estimated: Vec<(String, u64)> = keys
        .keys()
        .map(|k| (k.clone(), cm_estimate(&counters, k)))
        .collect();
    estimated.sort_by(|a, b| b.1.cmp(&a.1));
    let top_k_keys: Vec<(String, u64)> = estimated.into_iter().take(10).collect();

    let prefix_clusters = detect_prefix_clusters(keys, total_count);
    let distribution_type = classify_distribution(keys, &top_k_keys, total_count, &prefix_clusters);

    KeyDistributionAnalysis {
        estimated_cardinality,
        top_k_keys,
        prefix_clusters,
        distribution_type,
    }
}

/// Predict the benefit of rebalancing from the current partition count to
/// `proposed_partitions`.
pub fn predict_rebalance_benefit(
    current: &SkewAnalysis,
    proposed_partitions: u32,
) -> RebalanceBenefit {
    let current_n = current.partition_details.len() as u32;

    if current_n == 0 || proposed_partitions == 0 {
        return RebalanceBenefit {
            estimated_skew_improvement: 0.0,
            estimated_data_movement_bytes: 0,
            risk: RiskLevel::Low,
            recommendation: "No change possible".into(),
        };
    }

    // Model: adding partitions reduces Gini roughly as sqrt(old_n / new_n).
    let estimated_new_gini = if proposed_partitions > current_n {
        current.skew_coefficient * (current_n as f64 / proposed_partitions as f64).sqrt()
    } else if proposed_partitions < current_n {
        // Fewer partitions: skew likely worsens
        (current.skew_coefficient * (current_n as f64 / proposed_partitions as f64).powf(0.3))
            .min(1.0)
    } else {
        current.skew_coefficient
    };

    let improvement = current.skew_coefficient - estimated_new_gini;

    // Rough data-movement estimate (1 hour of data at 100 bytes/msg)
    let total_bytes = (current.total_throughput_mps * 100.0 * 3600.0) as u64;
    let estimated_movement = if proposed_partitions > current_n {
        let frac = (proposed_partitions - current_n) as f64 / proposed_partitions as f64;
        (total_bytes as f64 * frac * 0.5) as u64
    } else if proposed_partitions < current_n {
        let frac = (current_n - proposed_partitions) as f64 / current_n as f64;
        (total_bytes as f64 * frac) as u64
    } else {
        0
    };

    let risk = if proposed_partitions < current_n {
        RiskLevel::High
    } else if proposed_partitions as f64 > current_n as f64 * 2.0 {
        RiskLevel::Medium
    } else {
        RiskLevel::Low
    };

    let recommendation = if improvement > 0.1 {
        format!(
            "Recommended: {} -> {} partitions, skew improvement ~{:.1}%",
            current_n,
            proposed_partitions,
            improvement * 100.0
        )
    } else if improvement > 0.0 {
        format!(
            "Marginal benefit ({:.1}% improvement) -- may not justify data movement",
            improvement * 100.0
        )
    } else {
        format!("Not recommended: {proposed_partitions} partitions would likely increase skew")
    };

    RebalanceBenefit {
        estimated_skew_improvement: improvement,
        estimated_data_movement_bytes: estimated_movement,
        risk,
        recommendation,
    }
}

/// Calculate a composite health score for a set of partition metrics.
pub fn calculate_health_score(metrics: &[PartitionMetrics], gini: f64) -> HealthScore {
    if metrics.is_empty() {
        return HealthScore {
            overall: 1.0,
            balance_score: 1.0,
            throughput_score: 1.0,
            latency_score: 1.0,
        };
    }

    // Balance: inverse of Gini
    let balance_score = (1.0 - gini).clamp(0.0, 1.0);

    // Throughput: fraction of partitions with non-zero traffic
    let active = metrics.iter().filter(|m| m.throughput_mps > 0.0).count() as f64;
    let throughput_score = (active / metrics.len() as f64).clamp(0.0, 1.0);

    // Latency: fraction of partitions below the default 100 ms threshold
    let latency_ok = metrics.iter().filter(|m| m.p99_latency_ms <= 100.0).count() as f64;
    let latency_score = (latency_ok / metrics.len() as f64).clamp(0.0, 1.0);

    let overall =
        (0.4 * balance_score + 0.3 * throughput_score + 0.3 * latency_score).clamp(0.0, 1.0);

    HealthScore {
        overall,
        balance_score,
        throughput_score,
        latency_score,
    }
}

// ── Internal helpers ────────────────────────────────────────────────────

fn severity_from_gini(gini: f64) -> SkewSeverity {
    if gini > 0.7 {
        SkewSeverity::Critical
    } else if gini > 0.5 {
        SkewSeverity::High
    } else if gini > 0.3 {
        SkewSeverity::Medium
    } else if gini > 0.15 {
        SkewSeverity::Low
    } else {
        SkewSeverity::None
    }
}

/// Detect clusters of keys sharing common prefixes.
fn detect_prefix_clusters(keys: &HashMap<String, u64>, total_count: u64) -> Vec<PrefixCluster> {
    if total_count == 0 {
        return Vec::new();
    }

    // prefix -> (key_count, msg_count)
    let mut prefix_counts: HashMap<String, (u64, u64)> = HashMap::new();

    for (key, &count) in keys {
        let max_len = key.len().min(8);
        for plen in 2..=max_len {
            if let Some(prefix) = key.get(..plen) {
                let entry = prefix_counts.entry(prefix.to_string()).or_insert((0, 0));
                entry.0 += 1;
                entry.1 += count;
            }
        }
    }

    let total_keys = keys.len() as f64;
    let mut clusters: Vec<PrefixCluster> = prefix_counts
        .into_iter()
        .filter(|(_prefix, (key_count, _))| {
            let share = *key_count as f64 / total_keys;
            *key_count > 1 && share > 0.2 && share < 0.95
        })
        .map(|(prefix, (key_count, _))| {
            let share_pct = (key_count as f64 / total_keys) * 100.0;
            PrefixCluster {
                prefix,
                key_count,
                share_pct,
            }
        })
        .collect();

    clusters.sort_by(|a, b| {
        b.share_pct
            .partial_cmp(&a.share_pct)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    clusters.truncate(10);
    clusters
}

/// Classify the key distribution pattern.
fn classify_distribution(
    keys: &HashMap<String, u64>,
    top_k: &[(String, u64)],
    total_count: u64,
    prefix_clusters: &[PrefixCluster],
) -> DistributionType {
    if total_count == 0 || keys.is_empty() {
        return DistributionType::Uniform;
    }

    // Monotonic: >=80% of keys parse as integers
    let numeric_count = keys.keys().filter(|k| k.parse::<i64>().is_ok()).count();
    if numeric_count as f64 / keys.len() as f64 > 0.8 {
        return DistributionType::MonotonicKey;
    }

    // Zipfian: top-1 key >20% of traffic, or top-3 >50%
    if let Some((_, top1)) = top_k.first() {
        if *top1 as f64 / total_count as f64 > 0.20 {
            return DistributionType::Zipfian;
        }
    }
    let top3: u64 = top_k.iter().take(3).map(|(_, c)| c).sum();
    if top3 as f64 / total_count as f64 > 0.50 {
        return DistributionType::Zipfian;
    }

    // Clustered: any prefix cluster >40% of keys
    if prefix_clusters.iter().any(|c| c.share_pct > 40.0) {
        return DistributionType::Clustered;
    }

    DistributionType::Uniform
}

// ── SkewAnalyzer (stateful, sample-based) ───────────────────────────────

/// Stateful analyzer that accumulates metrics samples over time.
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

    /// Analyze skew for a topic (legacy `SkewReport` format).
    pub fn analyze(&self, topic: &str) -> Option<SkewReport> {
        let samples = self.samples.get(topic)?;
        if samples.is_empty() {
            return None;
        }

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
            SkewSeverity::Low => "Minor imbalance -- monitor but no action needed".to_string(),
            SkewSeverity::Medium => format!(
                "Moderate skew detected ({hot_count} hot partitions) -- consider rebalancing"
            ),
            SkewSeverity::High | SkewSeverity::Critical => format!(
                "Severe skew ({hot_count} hot partitions, coefficient {skew_coefficient:.2}) -- rebalancing recommended"
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

    /// Rich analysis using Gini coefficient and sigma thresholds.
    pub fn analyze_deep(&self, topic: &str) -> Option<SkewAnalysis> {
        let samples = self.samples.get(topic)?;
        let latest = samples.last()?;
        if latest.is_empty() {
            return None;
        }
        Some(analyze_throughput_skew(latest))
    }

    /// Get the analyzer configuration.
    pub fn config(&self) -> &SkewAnalyzerConfig {
        &self.config
    }
}

// ── Tests ───────────────────────────────────────────────────────────────

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
                hot_key_ratio: 0.0,
                p99_latency_ms: 10.0,
            })
            .collect()
    }

    fn make_metrics_with_latency(
        topic: &str,
        throughputs: &[f64],
        latencies: &[f64],
    ) -> Vec<PartitionMetrics> {
        throughputs
            .iter()
            .zip(latencies.iter())
            .enumerate()
            .map(|(i, (&tp, &lat))| PartitionMetrics {
                topic: topic.to_string(),
                partition: i as i32,
                throughput_mps: tp,
                throughput_bps: tp * 100.0,
                consumer_lag: 0,
                unique_keys: 100,
                size_bytes: 0,
                hot_key_ratio: 0.0,
                p99_latency_ms: lat,
            })
            .collect()
    }

    // ── Gini coefficient ────────────────────────────────────────────────

    #[test]
    fn test_gini_uniform() {
        let values = vec![100.0, 100.0, 100.0, 100.0];
        assert!((gini_coefficient(&values)).abs() < 1e-10);
    }

    #[test]
    fn test_gini_extreme_skew() {
        // One partition gets everything: Gini = (n-1)/n = 0.75
        let values = vec![0.0, 0.0, 0.0, 400.0];
        let gini = gini_coefficient(&values);
        assert!((gini - 0.75).abs() < 1e-10, "got {gini}");
    }

    #[test]
    fn test_gini_moderate_skew() {
        let values = vec![10.0, 20.0, 30.0, 40.0];
        let gini = gini_coefficient(&values);
        assert!(gini > 0.1 && gini < 0.4, "got {gini}");
    }

    #[test]
    fn test_gini_empty_and_zeros() {
        assert_eq!(gini_coefficient(&[]), 0.0);
        assert_eq!(gini_coefficient(&[0.0, 0.0, 0.0]), 0.0);
        assert_eq!(gini_coefficient(&[42.0]), 0.0); // single element
    }

    #[test]
    fn test_gini_two_elements() {
        // [0, 100]: Gini = 0.5 for n=2
        let gini = gini_coefficient(&[0.0, 100.0]);
        assert!((gini - 0.5).abs() < 1e-10, "got {gini}");
    }

    // ── Throughput skew analysis ────────────────────────────────────────

    #[test]
    fn test_analyze_throughput_skew_uniform() {
        let metrics = make_metrics("events", &[100.0, 100.0, 100.0, 100.0]);
        let analysis = analyze_throughput_skew(&metrics);

        assert_eq!(analysis.severity, SkewSeverity::None);
        assert!(analysis.skew_coefficient < 0.01);
        assert!(analysis.hot_partitions.is_empty());
        assert!(analysis.cold_partitions.is_empty());
        assert!(analysis.health_score.overall > 0.9);
    }

    #[test]
    fn test_analyze_throughput_skew_highly_skewed() {
        let metrics = make_metrics("events", &[900.0, 30.0, 20.0, 10.0]);
        let analysis = analyze_throughput_skew(&metrics);

        assert!(
            analysis.severity == SkewSeverity::High || analysis.severity == SkewSeverity::Critical,
            "severity = {:?}",
            analysis.severity
        );
        assert!(!analysis.hot_partitions.is_empty());
        assert!(analysis.skew_coefficient > 0.5);
    }

    #[test]
    fn test_analyze_throughput_skew_empty() {
        let analysis = analyze_throughput_skew(&[]);
        assert_eq!(analysis.severity, SkewSeverity::None);
        assert_eq!(analysis.total_throughput_mps, 0.0);
    }

    #[test]
    fn test_analyze_throughput_skew_monotonic_key_detection() {
        let mut metrics = make_metrics("events", &[900.0, 30.0, 20.0, 10.0]);
        metrics[0].hot_key_ratio = 0.95; // monotonic key on the hot partition
        let analysis = analyze_throughput_skew(&metrics);

        assert!(analysis
            .recommendations
            .iter()
            .any(|r| r.contains("Monotonic key")));
    }

    // ── Key distribution analysis ───────────────────────────────────────

    #[test]
    fn test_key_distribution_uniform() {
        let keys: HashMap<String, u64> = (0..100).map(|i| (format!("key-{i:04}"), 10)).collect();
        let analysis = analyze_key_distribution(&keys);

        assert_eq!(analysis.distribution_type, DistributionType::Uniform);
        assert!(analysis.estimated_cardinality > 0);
    }

    #[test]
    fn test_key_distribution_zipfian() {
        let mut keys: HashMap<String, u64> = HashMap::new();
        keys.insert("hot-key".to_string(), 500);
        for i in 0..50 {
            keys.insert(format!("cold-{i}"), 5);
        }
        let analysis = analyze_key_distribution(&keys);
        assert_eq!(analysis.distribution_type, DistributionType::Zipfian);
        assert_eq!(analysis.top_k_keys[0].0, "hot-key");
    }

    #[test]
    fn test_key_distribution_monotonic() {
        let keys: HashMap<String, u64> = (1000..1100).map(|i| (i.to_string(), 10)).collect();
        let analysis = analyze_key_distribution(&keys);
        assert_eq!(analysis.distribution_type, DistributionType::MonotonicKey);
    }

    #[test]
    fn test_key_distribution_clustered() {
        let mut keys: HashMap<String, u64> = HashMap::new();
        // 60 keys starting with "user-" out of 100 total: >40% cluster
        for i in 0..60 {
            keys.insert(format!("user-{i}"), 10);
        }
        for i in 0..20 {
            keys.insert(format!("order-{i}"), 10);
        }
        for i in 0..20 {
            keys.insert(format!("z{i}-misc"), 10);
        }
        let analysis = analyze_key_distribution(&keys);
        assert_eq!(analysis.distribution_type, DistributionType::Clustered);
    }

    #[test]
    fn test_key_distribution_empty() {
        let analysis = analyze_key_distribution(&HashMap::new());
        assert_eq!(analysis.estimated_cardinality, 0);
        assert_eq!(analysis.distribution_type, DistributionType::Uniform);
    }

    // ── Rebalance benefit prediction ────────────────────────────────────

    #[test]
    fn test_predict_rebalance_benefit_increase() {
        let analysis =
            analyze_throughput_skew(&make_metrics("events", &[800.0, 100.0, 50.0, 50.0]));
        let benefit = predict_rebalance_benefit(&analysis, 8);

        assert!(benefit.estimated_skew_improvement > 0.0);
        assert_eq!(benefit.risk, RiskLevel::Low);
    }

    #[test]
    fn test_predict_rebalance_benefit_decrease() {
        let analysis =
            analyze_throughput_skew(&make_metrics("events", &[100.0, 100.0, 100.0, 100.0]));
        let benefit = predict_rebalance_benefit(&analysis, 2);

        assert_eq!(benefit.risk, RiskLevel::High);
    }

    #[test]
    fn test_predict_rebalance_benefit_no_change() {
        let analysis =
            analyze_throughput_skew(&make_metrics("events", &[100.0, 100.0, 100.0, 100.0]));
        let benefit = predict_rebalance_benefit(&analysis, 4);
        assert_eq!(benefit.estimated_data_movement_bytes, 0);
    }

    // ── Health score ────────────────────────────────────────────────────

    #[test]
    fn test_health_score_perfect() {
        let metrics = make_metrics("events", &[100.0, 100.0, 100.0, 100.0]);
        let score = calculate_health_score(&metrics, 0.0);

        assert!((score.balance_score - 1.0).abs() < 1e-6);
        assert!((score.throughput_score - 1.0).abs() < 1e-6);
        assert!((score.latency_score - 1.0).abs() < 1e-6);
        assert!(score.overall > 0.95);
    }

    #[test]
    fn test_health_score_degraded_latency() {
        let metrics = make_metrics_with_latency(
            "events",
            &[100.0, 100.0, 100.0, 100.0],
            &[10.0, 10.0, 200.0, 300.0],
        );
        let score = calculate_health_score(&metrics, 0.0);

        assert!((score.latency_score - 0.5).abs() < 1e-6);
        assert!(score.overall < 0.9);
    }

    #[test]
    fn test_health_score_empty() {
        let score = calculate_health_score(&[], 0.0);
        assert!((score.overall - 1.0).abs() < 1e-6);
    }

    // ── Legacy SkewAnalyzer ─────────────────────────────────────────────

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

    #[test]
    fn test_analyze_deep() {
        let mut analyzer = SkewAnalyzer::new(SkewAnalyzerConfig::default());
        analyzer.record_sample(
            "events",
            make_metrics("events", &[800.0, 100.0, 50.0, 50.0]),
        );

        let analysis = analyzer.analyze_deep("events").unwrap();
        assert!(analysis.skew_coefficient > 0.3);
        assert!(!analysis.hot_partitions.is_empty());
    }
}
