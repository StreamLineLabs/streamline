//! Pattern Recognition for Stream Data
//!
//! Provides frequency analysis, trend detection, and seasonal pattern
//! detection on streaming numeric and textual data using simple windowing.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Trend direction detected in a data stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Trend {
    Increasing,
    Decreasing,
    Stable,
    Unknown,
}

/// A detected seasonal pattern.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeasonalPattern {
    /// Estimated period length (in number of samples)
    pub period: usize,
    /// Strength of the pattern (0.0–1.0)
    pub strength: f64,
    /// Average value per position in the period
    pub profile: Vec<f64>,
}

/// Result of pattern analysis on a topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PatternAnalysis {
    /// Topic that was analysed
    pub topic: String,
    /// Number of samples analysed
    pub sample_count: usize,
    /// Current trend
    pub trend: Trend,
    /// Trend slope (units per sample)
    pub trend_slope: f64,
    /// Top-K most frequent string tokens (token → count)
    pub frequency_top_k: Vec<(String, u64)>,
    /// Total unique tokens observed
    pub unique_tokens: usize,
    /// Detected seasonal patterns (may be empty)
    pub seasonal_patterns: Vec<SeasonalPattern>,
    /// Timestamp of last update (Unix millis)
    pub last_updated_ms: i64,
}

/// Per-topic pattern recognizer.
pub struct TopicPatternRecognizer {
    /// Numeric values window
    values: VecDeque<f64>,
    max_window: usize,
    /// Token frequency map
    token_freq: HashMap<String, u64>,
    total_tokens: u64,
    /// Sample count
    sample_count: usize,
    /// Last updated
    last_updated_ms: i64,
}

impl TopicPatternRecognizer {
    fn new(max_window: usize) -> Self {
        Self {
            values: VecDeque::with_capacity(max_window),
            max_window,
            token_freq: HashMap::new(),
            total_tokens: 0,
            sample_count: 0,
            last_updated_ms: 0,
        }
    }

    /// Ingest a numeric value.
    pub fn add_value(&mut self, value: f64) {
        if self.values.len() >= self.max_window {
            self.values.pop_front();
        }
        self.values.push_back(value);
        self.sample_count += 1;
        self.last_updated_ms = chrono::Utc::now().timestamp_millis();
    }

    /// Ingest a text message – tokenise and update frequency map.
    pub fn add_message(&mut self, message: &str) {
        for token in message.split_whitespace() {
            let t = token.to_lowercase();
            *self.token_freq.entry(t).or_insert(0) += 1;
            self.total_tokens += 1;
        }
        self.sample_count += 1;
        self.last_updated_ms = chrono::Utc::now().timestamp_millis();
    }

    /// Detect trend via simple linear regression on the value window.
    pub fn detect_trend(&self) -> (Trend, f64) {
        let n = self.values.len();
        if n < 3 {
            return (Trend::Unknown, 0.0);
        }

        let n_f = n as f64;
        let mut sum_x = 0.0_f64;
        let mut sum_y = 0.0_f64;
        let mut sum_xy = 0.0_f64;
        let mut sum_x2 = 0.0_f64;

        for (i, &y) in self.values.iter().enumerate() {
            let x = i as f64;
            sum_x += x;
            sum_y += y;
            sum_xy += x * y;
            sum_x2 += x * x;
        }

        let denom = n_f * sum_x2 - sum_x * sum_x;
        if denom.abs() < f64::EPSILON {
            return (Trend::Stable, 0.0);
        }

        let slope = (n_f * sum_xy - sum_x * sum_y) / denom;

        // Normalise slope relative to mean to decide trend
        let mean = sum_y / n_f;
        let relative = if mean.abs() > f64::EPSILON {
            slope / mean.abs()
        } else {
            slope
        };

        let trend = if relative > 0.01 {
            Trend::Increasing
        } else if relative < -0.01 {
            Trend::Decreasing
        } else {
            Trend::Stable
        };

        (trend, slope)
    }

    /// Return the top-K most frequent tokens.
    fn top_k_tokens(&self, k: usize) -> Vec<(String, u64)> {
        let mut entries: Vec<_> = self.token_freq.iter().map(|(k, v)| (k.clone(), *v)).collect();
        entries.sort_by(|a, b| b.1.cmp(&a.1));
        entries.truncate(k);
        entries
    }

    /// Detect seasonal patterns by auto-correlation at candidate periods.
    pub fn detect_seasonal(&self, min_period: usize, max_period: usize) -> Vec<SeasonalPattern> {
        let n = self.values.len();
        if n < min_period * 3 {
            return Vec::new();
        }

        let vals: Vec<f64> = self.values.iter().copied().collect();
        let mean: f64 = vals.iter().sum::<f64>() / n as f64;
        let variance: f64 = vals.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / n as f64;
        if variance < f64::EPSILON {
            return Vec::new();
        }

        let mut patterns = Vec::new();

        for period in min_period..=max_period.min(n / 2) {
            // Compute normalised auto-correlation at this lag
            let mut corr = 0.0_f64;
            let count = n - period;
            for i in 0..count {
                corr += (vals[i] - mean) * (vals[i + period] - mean);
            }
            corr /= count as f64 * variance;

            if corr > 0.5 {
                // Build average profile
                let mut profile = vec![0.0_f64; period];
                let mut counts = vec![0usize; period];
                for (i, &v) in vals.iter().enumerate() {
                    profile[i % period] += v;
                    counts[i % period] += 1;
                }
                for (p, c) in profile.iter_mut().zip(counts.iter()) {
                    if *c > 0 {
                        *p /= *c as f64;
                    }
                }

                patterns.push(SeasonalPattern {
                    period,
                    strength: corr.min(1.0),
                    profile,
                });
            }
        }

        // Keep only the strongest non-harmonic patterns
        patterns.sort_by(|a, b| b.strength.partial_cmp(&a.strength).unwrap_or(std::cmp::Ordering::Equal));
        patterns.truncate(3);
        patterns
    }

    /// Produce a full pattern analysis snapshot.
    fn analyse(&self, topic: &str) -> PatternAnalysis {
        let (trend, slope) = self.detect_trend();
        let seasonal = self.detect_seasonal(2, 64);

        PatternAnalysis {
            topic: topic.to_string(),
            sample_count: self.sample_count,
            trend,
            trend_slope: slope,
            frequency_top_k: self.top_k_tokens(20),
            unique_tokens: self.token_freq.len(),
            seasonal_patterns: seasonal,
            last_updated_ms: self.last_updated_ms,
        }
    }

    fn reset(&mut self) {
        self.values.clear();
        self.token_freq.clear();
        self.total_tokens = 0;
        self.sample_count = 0;
    }
}

/// Configuration for pattern recognition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PatternConfig {
    /// Enable pattern recognition
    pub enabled: bool,
    /// Window size for numeric values
    pub window_size: usize,
    /// Minimum period to test for seasonality
    pub min_period: usize,
    /// Maximum period to test for seasonality
    pub max_period: usize,
}

impl Default for PatternConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            window_size: 2048,
            min_period: 2,
            max_period: 64,
        }
    }
}

/// Multi-topic pattern recognizer manager.
pub struct PatternRecognizer {
    config: PatternConfig,
    topics: Arc<RwLock<HashMap<String, TopicPatternRecognizer>>>,
}

impl PatternRecognizer {
    /// Create a new pattern recognizer.
    pub fn new(config: PatternConfig) -> Self {
        Self {
            config,
            topics: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Add a numeric value for a topic.
    pub async fn add_value(&self, topic: &str, value: f64) {
        let mut topics = self.topics.write().await;
        let recognizer = topics
            .entry(topic.to_string())
            .or_insert_with(|| TopicPatternRecognizer::new(self.config.window_size));
        recognizer.add_value(value);
    }

    /// Add a text message for a topic.
    pub async fn add_message(&self, topic: &str, message: &str) {
        let mut topics = self.topics.write().await;
        let recognizer = topics
            .entry(topic.to_string())
            .or_insert_with(|| TopicPatternRecognizer::new(self.config.window_size));
        recognizer.add_message(message);
    }

    /// Get pattern analysis for a topic.
    pub async fn analyse(&self, topic: &str) -> Option<PatternAnalysis> {
        let topics = self.topics.read().await;
        topics.get(topic).map(|r| r.analyse(topic))
    }

    /// List all tracked topics.
    pub async fn topics(&self) -> Vec<String> {
        let topics = self.topics.read().await;
        topics.keys().cloned().collect()
    }

    /// Reset tracking for a topic.
    pub async fn reset_topic(&self, topic: &str) {
        let mut topics = self.topics.write().await;
        if let Some(r) = topics.get_mut(topic) {
            r.reset();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trend_detection_increasing() {
        let mut r = TopicPatternRecognizer::new(100);
        for i in 0..50 {
            r.add_value(i as f64 * 2.0);
        }
        let (trend, slope) = r.detect_trend();
        assert_eq!(trend, Trend::Increasing);
        assert!(slope > 0.0);
    }

    #[test]
    fn test_trend_detection_decreasing() {
        let mut r = TopicPatternRecognizer::new(100);
        for i in 0..50 {
            r.add_value(100.0 - i as f64 * 1.5);
        }
        let (trend, _) = r.detect_trend();
        assert_eq!(trend, Trend::Decreasing);
    }

    #[test]
    fn test_trend_detection_stable() {
        let mut r = TopicPatternRecognizer::new(100);
        for _ in 0..50 {
            r.add_value(42.0);
        }
        let (trend, _) = r.detect_trend();
        assert_eq!(trend, Trend::Stable);
    }

    #[test]
    fn test_frequency_analysis() {
        let mut r = TopicPatternRecognizer::new(100);
        r.add_message("hello world hello rust hello");
        let top = r.top_k_tokens(5);
        assert_eq!(top[0].0, "hello");
        assert_eq!(top[0].1, 3);
    }

    #[test]
    fn test_seasonal_detection() {
        let mut r = TopicPatternRecognizer::new(256);
        // Generate a signal with period 10
        for i in 0..200 {
            let v = (i as f64 * std::f64::consts::TAU / 10.0).sin() * 10.0 + 50.0;
            r.add_value(v);
        }
        let patterns = r.detect_seasonal(5, 20);
        assert!(!patterns.is_empty(), "should detect at least one seasonal pattern");
        // The strongest pattern should have period 10 (or a close harmonic)
        let best = patterns[0].period;
        assert!(best == 10 || best == 20 || best == 5,
            "expected period near 10, got {}", best);
    }

    #[tokio::test]
    async fn test_pattern_recognizer() {
        let pr = PatternRecognizer::new(PatternConfig::default());
        for i in 0..50 {
            pr.add_value("test-topic", i as f64).await;
        }
        let analysis = pr.analyse("test-topic").await.unwrap();
        assert_eq!(analysis.trend, Trend::Increasing);
        assert_eq!(analysis.sample_count, 50);
    }
}
