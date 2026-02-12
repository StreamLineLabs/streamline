//! Anomaly Detection
//!
//! Real-time anomaly detection for stream data using various methods
//! including statistical analysis, embedding distance, and pattern matching.

use super::config::{AnomalyConfig, AnomalyMethod};
use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Anomaly detector for stream data
pub struct AnomalyDetector {
    /// Configuration
    config: AnomalyConfig,
    /// Detection engine
    engine: Arc<RwLock<DetectionEngine>>,
}

impl AnomalyDetector {
    /// Create a new anomaly detector
    pub fn new(config: AnomalyConfig) -> Result<Self> {
        let engine = DetectionEngine::new(&config);

        Ok(Self {
            config,
            engine: Arc::new(RwLock::new(engine)),
        })
    }

    /// Check if a message is anomalous
    pub async fn check(&self, message: &str) -> Result<AnomalyResult> {
        if !self.config.enabled {
            return Ok(AnomalyResult {
                is_anomaly: false,
                anomaly_type: None,
                score: 0.0,
                threshold: self.config.sensitivity,
                details: None,
            });
        }

        let mut engine = self.engine.write().await;
        engine.detect(message, &self.config)
    }

    /// Check a numeric value for anomalies
    pub async fn check_value(&self, value: f64) -> Result<AnomalyResult> {
        if !self.config.enabled {
            return Ok(AnomalyResult {
                is_anomaly: false,
                anomaly_type: None,
                score: 0.0,
                threshold: self.config.sensitivity,
                details: None,
            });
        }

        let mut engine = self.engine.write().await;
        engine.detect_numeric(value, &self.config)
    }

    /// Check a batch of values
    pub async fn check_batch(&self, values: &[f64]) -> Result<Vec<AnomalyResult>> {
        let mut results = Vec::with_capacity(values.len());

        for value in values {
            results.push(self.check_value(*value).await?);
        }

        Ok(results)
    }

    /// Train the detector with historical data
    pub async fn train(&self, data: &[f64]) -> Result<()> {
        let mut engine = self.engine.write().await;
        engine.train(data);
        Ok(())
    }

    /// Reset the detector
    pub async fn reset(&self) {
        let mut engine = self.engine.write().await;
        engine.reset();
    }

    /// Get detector statistics
    pub async fn stats(&self) -> DetectorStats {
        let engine = self.engine.read().await;
        engine.stats()
    }

    /// Update configuration
    pub async fn update_config(&self, config: AnomalyConfig) {
        let mut engine = self.engine.write().await;
        engine.update_config(&config);
    }

    /// Get recorded anomaly events.
    pub async fn events(&self) -> Vec<AnomalyEvent> {
        let engine = self.engine.read().await;
        engine.events().to_vec()
    }

    /// Check a numeric value using the IQR detector specifically.
    pub async fn check_iqr(&self, value: f64) -> Result<AnomalyResult> {
        let mut engine = self.engine.write().await;
        engine.samples_processed += 1;
        let result = engine.iqr.check(value);
        if result.is_anomaly {
            engine.anomalies_detected += 1;
            engine.record_event(&result, value, None);
        }
        Ok(result)
    }

    /// Check a numeric value using the moving average detector specifically.
    pub async fn check_moving_average(&self, value: f64) -> Result<AnomalyResult> {
        let mut engine = self.engine.write().await;
        engine.samples_processed += 1;
        let result = engine.moving_avg.check(value);
        if result.is_anomaly {
            engine.anomalies_detected += 1;
            engine.record_event(&result, value, None);
        }
        Ok(result)
    }

    /// Run all detection methods and return the most severe result.
    pub async fn check_all_methods(&self, value: f64) -> Result<AnomalyResult> {
        let mut engine = self.engine.write().await;
        engine.samples_processed += 1;

        let sensitivity = engine.statistical.sensitivity;
        let zscore_result = engine.statistical.check(value, sensitivity);
        let ma_result = engine.moving_avg.check(value);
        let iqr_result = engine.iqr.check(value);

        // Pick the result with the highest score
        let result = [zscore_result, ma_result, iqr_result]
            .into_iter()
            .max_by(|a, b| a.score.partial_cmp(&b.score).unwrap_or(std::cmp::Ordering::Equal))
            .unwrap_or_else(AnomalyResult::normal);

        if result.is_anomaly {
            engine.anomalies_detected += 1;
            engine.record_event(&result, value, None);
        }

        Ok(result)
    }

    /// Check a numeric value and record it against a specific topic.
    pub async fn check_value_for_topic(&self, topic: &str, value: f64) -> Result<AnomalyResult> {
        let result = self.check_value(value).await?;
        if result.is_anomaly {
            let mut engine = self.engine.write().await;
            // Re-record the event with the topic set
            let event = AnomalyEvent {
                id: uuid::Uuid::new_v4().to_string(),
                anomaly_type: result.anomaly_type.unwrap_or(AnomalyType::StatisticalOutlier),
                severity: result.score,
                timestamp_ms: chrono::Utc::now().timestamp_millis(),
                observed_value: value,
                expected_value: engine.statistical.mean,
                topic: Some(topic.to_string()),
                description: result
                    .details
                    .as_ref()
                    .map(|d| format!("Expected {}, got {}", d.expected, d.actual))
                    .unwrap_or_else(|| format!("Anomaly score: {:.3}", result.score)),
            };
            if engine.events.len() >= engine.max_events {
                engine.events.remove(0);
            }
            engine.events.push(event);
        }
        Ok(result)
    }

    /// Get anomaly events filtered by topic.
    pub async fn events_for_topic(&self, topic: &str) -> Vec<AnomalyEvent> {
        let engine = self.engine.read().await;
        engine
            .events()
            .iter()
            .filter(|e| e.topic.as_deref() == Some(topic))
            .cloned()
            .collect()
    }
}

/// Anomaly detection result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnomalyResult {
    /// Whether this is an anomaly
    pub is_anomaly: bool,
    /// Type of anomaly detected
    pub anomaly_type: Option<AnomalyType>,
    /// Anomaly score (0.0 - 1.0, higher = more anomalous)
    pub score: f32,
    /// Threshold used for detection
    pub threshold: f32,
    /// Additional details
    pub details: Option<AnomalyDetails>,
}

impl AnomalyResult {
    /// Create a non-anomaly result
    pub fn normal() -> Self {
        Self {
            is_anomaly: false,
            anomaly_type: None,
            score: 0.0,
            threshold: 0.0,
            details: None,
        }
    }

    /// Create an anomaly result
    pub fn anomaly(anomaly_type: AnomalyType, score: f32, threshold: f32) -> Self {
        Self {
            is_anomaly: true,
            anomaly_type: Some(anomaly_type),
            score,
            threshold,
            details: None,
        }
    }

    /// Add details
    pub fn with_details(mut self, details: AnomalyDetails) -> Self {
        self.details = Some(details);
        self
    }
}

/// Types of anomalies
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AnomalyType {
    /// Statistical outlier (z-score)
    StatisticalOutlier,
    /// Unusual pattern
    PatternAnomaly,
    /// Rate anomaly (too fast/slow)
    RateAnomaly,
    /// Content anomaly (unexpected content)
    ContentAnomaly,
    /// Volume anomaly (unusual message count)
    VolumeAnomaly,
    /// Structural anomaly (unexpected format)
    StructuralAnomaly,
    /// Semantic anomaly (embedding distance)
    SemanticAnomaly,
    /// Moving average deviation
    MovingAverageDeviation,
    /// IQR-based outlier
    IqrOutlier,
}

/// A detected anomaly event with severity, timestamp, and context.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnomalyEvent {
    /// Unique event ID
    pub id: String,
    /// Anomaly type
    pub anomaly_type: AnomalyType,
    /// Severity level (0.0–1.0, higher = more severe)
    pub severity: f32,
    /// When the anomaly was detected (Unix millis)
    pub timestamp_ms: i64,
    /// The observed value that triggered the anomaly
    pub observed_value: f64,
    /// The expected value or range
    pub expected_value: f64,
    /// Source topic (if applicable)
    pub topic: Option<String>,
    /// Human-readable description
    pub description: String,
}

/// Additional anomaly details
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnomalyDetails {
    /// Expected value/range
    pub expected: String,
    /// Actual value
    pub actual: String,
    /// Z-score (for statistical anomalies)
    pub z_score: Option<f64>,
    /// Distance from centroid (for embedding anomalies)
    pub distance: Option<f32>,
    /// Historical context
    pub context: Option<String>,
}

/// Detection engine
pub struct DetectionEngine {
    /// Method
    method: AnomalyMethod,
    /// Statistical detector
    statistical: StatisticalDetector,
    /// Moving average detector
    moving_avg: MovingAverageDetector,
    /// IQR detector
    iqr: IqrDetector,
    /// Pattern detector
    #[allow(dead_code)]
    pattern: PatternDetector,
    /// Embedding detector (if enabled)
    embedding: Option<EmbeddingDetector>,
    /// Total samples processed
    samples_processed: u64,
    /// Anomalies detected
    anomalies_detected: u64,
    /// Recorded anomaly events
    events: Vec<AnomalyEvent>,
    /// Maximum stored events
    max_events: usize,
}

impl DetectionEngine {
    /// Create a new detection engine
    fn new(config: &AnomalyConfig) -> Self {
        let embedding = if config.use_embeddings {
            Some(EmbeddingDetector::new(config.embedding_threshold))
        } else {
            None
        };

        Self {
            method: config.method.clone(),
            statistical: StatisticalDetector::new(config.window_size, config.sensitivity),
            moving_avg: MovingAverageDetector::new(config.window_size, config.sensitivity),
            iqr: IqrDetector::new(config.window_size, config.sensitivity),
            pattern: PatternDetector::new(config.window_size),
            embedding,
            samples_processed: 0,
            anomalies_detected: 0,
            events: Vec::new(),
            max_events: 1000,
        }
    }

    /// Detect anomalies in text
    fn detect(&mut self, message: &str, config: &AnomalyConfig) -> Result<AnomalyResult> {
        self.samples_processed += 1;

        // Check based on method
        let result = match self.method {
            AnomalyMethod::Statistical => {
                // Use message length as proxy for now
                let value = message.len() as f64;
                self.statistical.check(value, config.sensitivity)
            }
            AnomalyMethod::EmbeddingDistance => {
                if let Some(ref mut detector) = self.embedding {
                    detector.check(message)
                } else {
                    AnomalyResult::normal()
                }
            }
            AnomalyMethod::IsolationForest => {
                // Simplified isolation forest using statistical approach
                let value = message.len() as f64;
                self.statistical.check_isolation(value)
            }
            AnomalyMethod::Lof => {
                // Local outlier factor approximation
                let value = message.len() as f64;
                self.statistical.check_lof(value)
            }
            AnomalyMethod::Autoencoder => {
                // Autoencoder would need separate model
                // Using statistical as fallback
                let value = message.len() as f64;
                self.statistical.check(value, config.sensitivity)
            }
        };

        if result.is_anomaly {
            self.anomalies_detected += 1;
            self.record_event(&result, message.len() as f64, None);
        }

        Ok(result)
    }

    /// Detect anomalies in numeric values
    fn detect_numeric(&mut self, value: f64, config: &AnomalyConfig) -> Result<AnomalyResult> {
        self.samples_processed += 1;

        let result = match self.method {
            AnomalyMethod::Statistical => self.statistical.check(value, config.sensitivity),
            AnomalyMethod::IsolationForest => self.statistical.check_isolation(value),
            AnomalyMethod::Lof => self.statistical.check_lof(value),
            _ => {
                // Also feed the moving average detector
                let ma_result = self.moving_avg.check(value);
                if ma_result.is_anomaly {
                    ma_result
                } else {
                    self.statistical.check(value, config.sensitivity)
                }
            }
        };

        if result.is_anomaly {
            self.anomalies_detected += 1;
            self.record_event(&result, value, None);
        }

        Ok(result)
    }

    /// Record an anomaly event
    fn record_event(&mut self, result: &AnomalyResult, observed: f64, topic: Option<String>) {
        let event = AnomalyEvent {
            id: uuid::Uuid::new_v4().to_string(),
            anomaly_type: result.anomaly_type.unwrap_or(AnomalyType::StatisticalOutlier),
            severity: result.score,
            timestamp_ms: chrono::Utc::now().timestamp_millis(),
            observed_value: observed,
            expected_value: self.statistical.mean,
            topic,
            description: result
                .details
                .as_ref()
                .map(|d| format!("Expected {}, got {}", d.expected, d.actual))
                .unwrap_or_else(|| format!("Anomaly score: {:.3}", result.score)),
        };

        if self.events.len() >= self.max_events {
            self.events.remove(0);
        }
        self.events.push(event);
    }

    /// Get recorded anomaly events
    fn events(&self) -> &[AnomalyEvent] {
        &self.events
    }

    /// Train with historical data
    fn train(&mut self, data: &[f64]) {
        self.statistical.train(data);
        for &v in data {
            self.moving_avg.update(v);
        }
    }

    /// Reset the engine
    fn reset(&mut self) {
        self.statistical.reset();
        self.moving_avg.reset();
        self.iqr.reset();
        if let Some(ref mut embedding) = self.embedding {
            embedding.reset();
        }
        self.samples_processed = 0;
        self.anomalies_detected = 0;
        self.events.clear();
    }

    /// Update configuration
    fn update_config(&mut self, config: &AnomalyConfig) {
        self.method = config.method.clone();
        self.statistical.sensitivity = config.sensitivity;

        if config.use_embeddings && self.embedding.is_none() {
            self.embedding = Some(EmbeddingDetector::new(config.embedding_threshold));
        } else if !config.use_embeddings {
            self.embedding = None;
        }
    }

    /// Get statistics
    fn stats(&self) -> DetectorStats {
        DetectorStats {
            samples_processed: self.samples_processed,
            anomalies_detected: self.anomalies_detected,
            anomaly_rate: if self.samples_processed > 0 {
                self.anomalies_detected as f64 / self.samples_processed as f64
            } else {
                0.0
            },
            window_size: self.statistical.values.len(),
            mean: self.statistical.mean,
            std_dev: self.statistical.std_dev,
        }
    }
}

/// Statistical anomaly detector using z-scores
pub struct StatisticalDetector {
    /// Rolling window of values
    values: VecDeque<f64>,
    /// Maximum window size
    max_window: usize,
    /// Running mean
    mean: f64,
    /// Running variance
    variance: f64,
    /// Standard deviation
    std_dev: f64,
    /// Sensitivity threshold
    sensitivity: f32,
    /// Count of values
    count: u64,
}

impl StatisticalDetector {
    /// Create a new statistical detector
    fn new(window_size: usize, sensitivity: f32) -> Self {
        Self {
            values: VecDeque::with_capacity(window_size),
            max_window: window_size,
            mean: 0.0,
            variance: 0.0,
            std_dev: 0.0,
            sensitivity,
            count: 0,
        }
    }

    /// Update statistics with new value
    fn update(&mut self, value: f64) {
        self.count += 1;

        // Add to window
        if self.values.len() >= self.max_window {
            self.values.pop_front();
        }
        self.values.push_back(value);

        // Update running statistics (Welford's algorithm)
        if self.values.len() > 1 {
            let n = self.values.len() as f64;
            let old_mean = self.mean;
            self.mean = old_mean + (value - old_mean) / n;
            self.variance += (value - old_mean) * (value - self.mean);
            self.std_dev = (self.variance / (n - 1.0)).sqrt();
        } else {
            self.mean = value;
            self.variance = 0.0;
            self.std_dev = 0.0;
        }
    }

    /// Check if value is anomalous
    fn check(&mut self, value: f64, sensitivity: f32) -> AnomalyResult {
        // Need minimum samples before detection
        if self.values.len() < 10 {
            self.update(value);
            return AnomalyResult::normal();
        }

        // Calculate z-score
        let z_score = if self.std_dev > 0.0 {
            (value - self.mean) / self.std_dev
        } else {
            0.0
        };

        // Threshold based on sensitivity (higher sensitivity = lower threshold)
        let threshold = 3.0 - (sensitivity * 2.0);

        self.update(value);

        if z_score.abs() > threshold as f64 {
            AnomalyResult::anomaly(
                AnomalyType::StatisticalOutlier,
                z_score.abs() as f32 / 5.0, // Normalize to 0-1
                sensitivity,
            )
            .with_details(AnomalyDetails {
                expected: format!("{:.2} ± {:.2}", self.mean, self.std_dev * threshold as f64),
                actual: format!("{:.2}", value),
                z_score: Some(z_score),
                distance: None,
                context: Some(format!("Based on {} samples", self.values.len())),
            })
        } else {
            AnomalyResult::normal()
        }
    }

    /// Simplified isolation forest check
    fn check_isolation(&mut self, value: f64) -> AnomalyResult {
        if self.values.len() < 10 {
            self.update(value);
            return AnomalyResult::normal();
        }

        // Count how many values are between this value and the mean
        let isolation_score: f64 = self
            .values
            .iter()
            .filter(|&v| {
                let between_mean = (self.mean - value).signum() != (self.mean - v).signum();
                between_mean && v.abs() < value.abs()
            })
            .count() as f64
            / self.values.len() as f64;

        self.update(value);

        if isolation_score < 0.1 {
            AnomalyResult::anomaly(
                AnomalyType::PatternAnomaly,
                (1.0 - isolation_score) as f32,
                0.5,
            )
        } else {
            AnomalyResult::normal()
        }
    }

    /// Local outlier factor approximation
    fn check_lof(&mut self, value: f64) -> AnomalyResult {
        if self.values.len() < 10 {
            self.update(value);
            return AnomalyResult::normal();
        }

        // Calculate k-distance (distance to k-th nearest neighbor)
        let k = 5.min(self.values.len());
        let mut distances: Vec<f64> = self.values.iter().map(|v| (value - v).abs()).collect();
        distances.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let k_distance = distances.get(k - 1).copied().unwrap_or(0.0);

        // Simplified LOF: compare k-distance to average k-distance
        let avg_k_distance = distances.iter().take(k).sum::<f64>() / k as f64;
        let lof = if avg_k_distance > 0.0 {
            k_distance / avg_k_distance
        } else {
            1.0
        };

        self.update(value);

        if lof > 2.0 {
            AnomalyResult::anomaly(
                AnomalyType::PatternAnomaly,
                (lof / 5.0).min(1.0) as f32,
                0.5,
            )
        } else {
            AnomalyResult::normal()
        }
    }

    /// Train with historical data
    fn train(&mut self, data: &[f64]) {
        for value in data {
            self.update(*value);
        }
    }

    /// Reset the detector
    fn reset(&mut self) {
        self.values.clear();
        self.mean = 0.0;
        self.variance = 0.0;
        self.std_dev = 0.0;
        self.count = 0;
    }
}

/// Moving average anomaly detector.
///
/// Detects anomalies by comparing each value to an exponentially-weighted
/// moving average (EWMA) and flagging values that deviate beyond a
/// sensitivity-controlled threshold.
pub struct MovingAverageDetector {
    /// Rolling window of values
    values: VecDeque<f64>,
    /// Window size
    max_window: usize,
    /// Exponential moving average
    ema: f64,
    /// EMA of squared differences (for volatility)
    ema_var: f64,
    /// Smoothing factor (alpha)
    alpha: f64,
    /// Sensitivity (0.0–1.0)
    sensitivity: f32,
    /// Whether the EMA has been initialised
    initialized: bool,
}

impl MovingAverageDetector {
    fn new(window_size: usize, sensitivity: f32) -> Self {
        let alpha = 2.0 / (window_size as f64 + 1.0);
        Self {
            values: VecDeque::with_capacity(window_size),
            max_window: window_size,
            ema: 0.0,
            ema_var: 0.0,
            alpha,
            sensitivity,
            initialized: false,
        }
    }

    fn update(&mut self, value: f64) {
        if self.values.len() >= self.max_window {
            self.values.pop_front();
        }
        self.values.push_back(value);

        if !self.initialized {
            self.ema = value;
            self.initialized = true;
        } else {
            let diff = value - self.ema;
            self.ema += self.alpha * diff;
            self.ema_var = (1.0 - self.alpha) * (self.ema_var + self.alpha * diff * diff);
        }
    }

    /// Check a value against the moving average.
    fn check(&mut self, value: f64) -> AnomalyResult {
        if self.values.len() < 10 {
            self.update(value);
            return AnomalyResult::normal();
        }

        let std_dev = self.ema_var.sqrt();
        let deviation = (value - self.ema).abs();
        // Higher sensitivity → lower threshold
        let threshold_mult = 3.0 - (self.sensitivity as f64 * 2.0);
        let threshold = std_dev * threshold_mult;

        self.update(value);

        if threshold > 0.0 && deviation > threshold {
            let score = (deviation / (std_dev * 5.0)).min(1.0) as f32;
            AnomalyResult::anomaly(
                AnomalyType::MovingAverageDeviation,
                score,
                self.sensitivity,
            )
            .with_details(AnomalyDetails {
                expected: format!("{:.2} ± {:.2}", self.ema, threshold),
                actual: format!("{:.2}", value),
                z_score: Some(deviation / std_dev),
                distance: None,
                context: Some(format!("EMA window {} samples", self.values.len())),
            })
        } else {
            AnomalyResult::normal()
        }
    }

    fn reset(&mut self) {
        self.values.clear();
        self.ema = 0.0;
        self.ema_var = 0.0;
        self.initialized = false;
    }
}

/// IQR (Interquartile Range) anomaly detector.
///
/// Flags values that fall below Q1 - k*IQR or above Q3 + k*IQR where k is
/// controlled by sensitivity.
pub struct IqrDetector {
    /// Rolling window of values
    values: VecDeque<f64>,
    /// Max window size
    max_window: usize,
    /// Sensitivity (0.0–1.0)
    sensitivity: f32,
}

impl IqrDetector {
    fn new(window_size: usize, sensitivity: f32) -> Self {
        Self {
            values: VecDeque::with_capacity(window_size),
            max_window: window_size,
            sensitivity,
        }
    }

    fn update(&mut self, value: f64) {
        if self.values.len() >= self.max_window {
            self.values.pop_front();
        }
        self.values.push_back(value);
    }

    fn check(&mut self, value: f64) -> AnomalyResult {
        if self.values.len() < 20 {
            self.update(value);
            return AnomalyResult::normal();
        }

        let mut sorted: Vec<f64> = self.values.iter().copied().collect();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let n = sorted.len();
        let q1 = sorted[n / 4];
        let q3 = sorted[3 * n / 4];
        let iqr = q3 - q1;

        // Higher sensitivity → smaller multiplier → more sensitive
        let k = 1.5 + (1.0 - self.sensitivity as f64) * 1.5; // range 1.5–3.0

        let lower = q1 - k * iqr;
        let upper = q3 + k * iqr;

        self.update(value);

        if value < lower || value > upper {
            let deviation = if value < lower {
                (lower - value) / iqr.max(f64::EPSILON)
            } else {
                (value - upper) / iqr.max(f64::EPSILON)
            };
            let score = (deviation / 3.0).min(1.0) as f32;

            AnomalyResult::anomaly(AnomalyType::StatisticalOutlier, score, self.sensitivity)
                .with_details(AnomalyDetails {
                    expected: format!("[{:.2}, {:.2}] (IQR={:.2}, k={:.1})", lower, upper, iqr, k),
                    actual: format!("{:.2}", value),
                    z_score: None,
                    distance: None,
                    context: Some(format!(
                        "IQR method: Q1={:.2}, Q3={:.2}, {} samples",
                        q1, q3, n
                    )),
                })
        } else {
            AnomalyResult::normal()
        }
    }

    fn reset(&mut self) {
        self.values.clear();
    }
}

/// Pattern-based anomaly detector
pub struct PatternDetector {
    /// Recent patterns
    #[allow(dead_code)]
    patterns: VecDeque<String>,
    /// Maximum patterns to track
    #[allow(dead_code)]
    max_patterns: usize,
}

impl PatternDetector {
    /// Create a new pattern detector
    fn new(max_patterns: usize) -> Self {
        Self {
            patterns: VecDeque::with_capacity(max_patterns),
            max_patterns,
        }
    }
}

/// Embedding-based anomaly detector
pub struct EmbeddingDetector {
    /// Centroid of normal embeddings
    centroid: Option<Vec<f32>>,
    /// Average distance from centroid
    #[allow(dead_code)]
    avg_distance: f32,
    /// Threshold for anomaly
    #[allow(dead_code)]
    threshold: f32,
    /// Count of samples used
    #[allow(dead_code)]
    sample_count: usize,
}

impl EmbeddingDetector {
    /// Create a new embedding detector
    fn new(threshold: f32) -> Self {
        Self {
            centroid: None,
            avg_distance: 0.0,
            threshold,
            sample_count: 0,
        }
    }

    /// Check if text embedding is anomalous
    fn check(&mut self, _message: &str) -> AnomalyResult {
        // In production, would generate embedding and compare to centroid
        // For now, return normal
        if self.centroid.is_some() {
            // Would calculate distance here
            AnomalyResult::normal()
        } else {
            AnomalyResult::normal()
        }
    }

    /// Reset the detector
    fn reset(&mut self) {
        self.centroid = None;
        self.avg_distance = 0.0;
        self.sample_count = 0;
    }
}

/// Detector statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetectorStats {
    /// Total samples processed
    pub samples_processed: u64,
    /// Anomalies detected
    pub anomalies_detected: u64,
    /// Anomaly rate
    pub anomaly_rate: f64,
    /// Current window size
    pub window_size: usize,
    /// Current mean
    pub mean: f64,
    /// Current standard deviation
    pub std_dev: f64,
}

/// Detector configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetectorConfig {
    /// Detection method
    pub method: AnomalyMethod,
    /// Sensitivity (0.0 - 1.0)
    pub sensitivity: f32,
    /// Window size
    pub window_size: usize,
    /// Use embeddings
    pub use_embeddings: bool,
    /// Embedding threshold
    pub embedding_threshold: f32,
}

impl Default for DetectorConfig {
    fn default() -> Self {
        Self {
            method: AnomalyMethod::Statistical,
            sensitivity: 0.5,
            window_size: 1000,
            use_embeddings: false,
            embedding_threshold: 0.5,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_statistical_detector() {
        let mut detector = StatisticalDetector::new(100, 0.5);

        // Add normal values
        for i in 0..50 {
            let result = detector.check(100.0 + (i % 10) as f64, 0.5);
            assert!(!result.is_anomaly);
        }

        // Add anomalous value
        let result = detector.check(500.0, 0.5);
        assert!(result.is_anomaly);
        assert_eq!(result.anomaly_type, Some(AnomalyType::StatisticalOutlier));
    }

    #[test]
    fn test_anomaly_result() {
        let result = AnomalyResult::anomaly(AnomalyType::RateAnomaly, 0.9, 0.5);
        assert!(result.is_anomaly);
        assert_eq!(result.anomaly_type, Some(AnomalyType::RateAnomaly));

        let normal = AnomalyResult::normal();
        assert!(!normal.is_anomaly);
    }

    #[tokio::test]
    async fn test_anomaly_detector() {
        let config = AnomalyConfig::default();
        let detector = AnomalyDetector::new(config).unwrap();

        // Check normal message
        let _result = detector.check("This is a normal message").await.unwrap();
        // Result depends on history, so just check it doesn't error

        // Get stats
        let stats = detector.stats().await;
        assert!(stats.samples_processed > 0);
    }

    #[tokio::test]
    async fn test_detector_training() {
        let config = AnomalyConfig::default();
        let detector = AnomalyDetector::new(config).unwrap();

        // Train with data
        let data: Vec<f64> = (0..100).map(|i| 100.0 + (i % 10) as f64).collect();
        detector.train(&data).await.unwrap();

        // Check stats after training
        let stats = detector.stats().await;
        assert!(stats.mean > 0.0);
    }

    #[test]
    fn test_detector_config() {
        let config = DetectorConfig::default();
        assert_eq!(config.sensitivity, 0.5);
        assert_eq!(config.window_size, 1000);
    }
}
