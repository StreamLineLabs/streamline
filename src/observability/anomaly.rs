//! Anomaly Detection Engine for Streamline Observability
//!
//! Statistical models for detecting anomalies in streaming metrics:
//! - Consumer lag spikes and stuck consumers
//! - Throughput deviation from baseline
//! - Partition skew detection
//! - Error rate anomalies
//!
//! Uses exponentially weighted moving average (EWMA) with configurable sensitivity.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Configuration for the anomaly detection engine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnomalyDetectionConfig {
    /// Enable anomaly detection
    pub enabled: bool,
    /// EWMA smoothing factor (0.0 to 1.0, higher = more reactive)
    pub alpha: f64,
    /// Number of standard deviations for anomaly threshold
    pub sensitivity: f64,
    /// Minimum samples before detection activates (learning period)
    pub min_samples: usize,
    /// Detection interval in seconds
    pub interval_secs: u64,
    /// Webhook URL for anomaly notifications (optional)
    pub webhook_url: Option<String>,
}

impl Default for AnomalyDetectionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            alpha: 0.3,
            sensitivity: 3.0,
            min_samples: 30,
            interval_secs: 10,
            webhook_url: None,
        }
    }
}

/// Types of anomalies that can be detected
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum AnomalyType {
    /// Consumer lag exceeds baseline
    ConsumerLagSpike,
    /// Consumer has not made progress
    StuckConsumer,
    /// Throughput significantly below baseline
    ThroughputDrop,
    /// Throughput significantly above baseline (potential storm)
    ThroughputSpike,
    /// Uneven partition distribution
    PartitionSkew,
    /// Error rate above baseline
    ErrorRateAnomaly,
    /// Produce latency anomaly
    LatencyAnomaly,
}

impl std::fmt::Display for AnomalyType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ConsumerLagSpike => write!(f, "consumer_lag_spike"),
            Self::StuckConsumer => write!(f, "stuck_consumer"),
            Self::ThroughputDrop => write!(f, "throughput_drop"),
            Self::ThroughputSpike => write!(f, "throughput_spike"),
            Self::PartitionSkew => write!(f, "partition_skew"),
            Self::ErrorRateAnomaly => write!(f, "error_rate_anomaly"),
            Self::LatencyAnomaly => write!(f, "latency_anomaly"),
        }
    }
}

/// Severity of a detected anomaly
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum AnomalySeverity {
    Info,
    Warning,
    Critical,
}

/// A detected anomaly event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnomalyEvent {
    pub anomaly_type: AnomalyType,
    pub severity: AnomalySeverity,
    pub metric_name: String,
    pub current_value: f64,
    pub expected_value: f64,
    pub deviation_sigma: f64,
    pub description: String,
    pub timestamp: String,
    pub resource: String,
}

/// Exponentially Weighted Moving Average tracker for a single metric
#[derive(Debug, Clone)]
struct EwmaTracker {
    mean: f64,
    variance: f64,
    alpha: f64,
    sample_count: usize,
    last_value: f64,
    last_update: Instant,
}

impl EwmaTracker {
    fn new(alpha: f64) -> Self {
        Self {
            mean: 0.0,
            variance: 0.0,
            alpha,
            sample_count: 0,
            last_value: 0.0,
            last_update: Instant::now(),
        }
    }

    fn update(&mut self, value: f64) {
        if self.sample_count == 0 {
            self.mean = value;
            self.variance = 0.0;
        } else {
            let diff = value - self.mean;
            self.mean = self.alpha * value + (1.0 - self.alpha) * self.mean;
            self.variance = (1.0 - self.alpha) * (self.variance + self.alpha * diff * diff);
        }
        self.sample_count += 1;
        self.last_value = value;
        self.last_update = Instant::now();
    }

    fn std_dev(&self) -> f64 {
        self.variance.sqrt()
    }

    fn is_anomalous(&self, value: f64, sensitivity: f64, min_samples: usize) -> Option<f64> {
        if self.sample_count < min_samples {
            return None;
        }
        let std_dev = self.std_dev();
        if std_dev < f64::EPSILON {
            return None;
        }
        let deviation = (value - self.mean).abs() / std_dev;
        if deviation > sensitivity {
            Some(deviation)
        } else {
            None
        }
    }
}

/// The anomaly detection engine
pub struct AnomalyDetector {
    config: AnomalyDetectionConfig,
    trackers: Arc<RwLock<HashMap<String, EwmaTracker>>>,
    active_anomalies: Arc<RwLock<Vec<AnomalyEvent>>>,
    anomaly_history: Arc<RwLock<Vec<AnomalyEvent>>>,
}

impl AnomalyDetector {
    /// Create a new anomaly detector with the given configuration
    pub fn new(config: AnomalyDetectionConfig) -> Self {
        Self {
            config,
            trackers: Arc::new(RwLock::new(HashMap::new())),
            active_anomalies: Arc::new(RwLock::new(Vec::new())),
            anomaly_history: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Record a metric value and check for anomalies
    pub async fn record_metric(
        &self,
        metric_name: &str,
        value: f64,
        resource: &str,
    ) -> Option<AnomalyEvent> {
        if !self.config.enabled {
            return None;
        }

        let key = format!("{}:{}", metric_name, resource);
        let mut trackers = self.trackers.write().await;
        let tracker = trackers
            .entry(key.clone())
            .or_insert_with(|| EwmaTracker::new(self.config.alpha));

        let anomaly = tracker
            .is_anomalous(value, self.config.sensitivity, self.config.min_samples)
            .map(|deviation| {
                let (anomaly_type, severity) = classify_anomaly(metric_name, value, tracker.mean, deviation);
                AnomalyEvent {
                    anomaly_type,
                    severity,
                    metric_name: metric_name.to_string(),
                    current_value: value,
                    expected_value: tracker.mean,
                    deviation_sigma: deviation,
                    description: format!(
                        "{} on {}: current={:.2}, expected={:.2} ({}σ deviation)",
                        metric_name, resource, value, tracker.mean, deviation as i64
                    ),
                    timestamp: chrono::Utc::now().to_rfc3339(),
                    resource: resource.to_string(),
                }
            });

        tracker.update(value);

        if let Some(ref event) = anomaly {
            info!(
                anomaly_type = %event.anomaly_type,
                severity = ?event.severity,
                metric = %event.metric_name,
                resource = %event.resource,
                "Anomaly detected"
            );
            self.active_anomalies.write().await.push(event.clone());

            let mut history = self.anomaly_history.write().await;
            history.push(event.clone());
            // Keep history bounded
            if history.len() > 10000 {
                history.drain(..5000);
            }
        }

        anomaly
    }

    /// Check consumer lag for anomalies
    pub async fn check_consumer_lag(
        &self,
        group_id: &str,
        topic: &str,
        partition: i32,
        lag: i64,
    ) -> Option<AnomalyEvent> {
        let resource = format!("{}/{}/{}", group_id, topic, partition);
        self.record_metric("consumer_lag", lag as f64, &resource).await
    }

    /// Check throughput for anomalies
    pub async fn check_throughput(
        &self,
        topic: &str,
        msgs_per_sec: f64,
    ) -> Option<AnomalyEvent> {
        self.record_metric("throughput_msgs_sec", msgs_per_sec, topic).await
    }

    /// Check produce latency for anomalies
    pub async fn check_latency(
        &self,
        topic: &str,
        latency_ms: f64,
    ) -> Option<AnomalyEvent> {
        self.record_metric("produce_latency_ms", latency_ms, topic).await
    }

    /// Get all currently active anomalies
    pub async fn active_anomalies(&self) -> Vec<AnomalyEvent> {
        self.active_anomalies.read().await.clone()
    }

    /// Get anomaly history
    pub async fn anomaly_history(&self, limit: usize) -> Vec<AnomalyEvent> {
        let history = self.anomaly_history.read().await;
        history.iter().rev().take(limit).cloned().collect()
    }

    /// Clear resolved anomalies older than the given duration
    pub async fn clear_stale_anomalies(&self, max_age: Duration) {
        let cutoff = chrono::Utc::now() - chrono::Duration::from_std(max_age).unwrap_or_default();
        let cutoff_str = cutoff.to_rfc3339();

        let mut active = self.active_anomalies.write().await;
        active.retain(|a| a.timestamp > cutoff_str);
    }

    /// Get tracker statistics for a metric
    pub async fn tracker_stats(&self) -> HashMap<String, TrackerStats> {
        let trackers = self.trackers.read().await;
        trackers
            .iter()
            .map(|(key, tracker)| {
                (
                    key.clone(),
                    TrackerStats {
                        mean: tracker.mean,
                        std_dev: tracker.std_dev(),
                        sample_count: tracker.sample_count,
                        last_value: tracker.last_value,
                        seconds_since_update: tracker.last_update.elapsed().as_secs(),
                    },
                )
            })
            .collect()
    }
}

/// Statistics for a tracked metric
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackerStats {
    pub mean: f64,
    pub std_dev: f64,
    pub sample_count: usize,
    pub last_value: f64,
    pub seconds_since_update: u64,
}

fn classify_anomaly(
    metric_name: &str,
    value: f64,
    mean: f64,
    deviation: f64,
) -> (AnomalyType, AnomalySeverity) {
    let severity = if deviation > 5.0 {
        AnomalySeverity::Critical
    } else if deviation > 4.0 {
        AnomalySeverity::Warning
    } else {
        AnomalySeverity::Info
    };

    let anomaly_type = match metric_name {
        "consumer_lag" => {
            if value > mean * 10.0 {
                AnomalyType::ConsumerLagSpike
            } else {
                AnomalyType::ConsumerLagSpike
            }
        }
        "throughput_msgs_sec" => {
            if value < mean {
                AnomalyType::ThroughputDrop
            } else {
                AnomalyType::ThroughputSpike
            }
        }
        "produce_latency_ms" | "e2e_latency_ms" => AnomalyType::LatencyAnomaly,
        "error_rate" => AnomalyType::ErrorRateAnomaly,
        "partition_skew" => AnomalyType::PartitionSkew,
        _ => {
            if value > mean {
                AnomalyType::ThroughputSpike
            } else {
                AnomalyType::ThroughputDrop
            }
        }
    };

    (anomaly_type, severity)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ewma_tracker_basic() {
        let mut tracker = EwmaTracker::new(0.3);
        for i in 0..50 {
            tracker.update(100.0 + (i as f64 % 5.0));
        }
        assert!(tracker.mean > 99.0 && tracker.mean < 105.0);
        assert!(tracker.std_dev() > 0.0);
    }

    #[test]
    fn test_ewma_tracker_anomaly_detection() {
        let mut tracker = EwmaTracker::new(0.3);
        // Train with stable values
        for _ in 0..100 {
            tracker.update(100.0);
        }
        // Spike should be anomalous
        let result = tracker.is_anomalous(500.0, 3.0, 30);
        assert!(result.is_some());
    }

    #[test]
    fn test_ewma_tracker_too_few_samples() {
        let mut tracker = EwmaTracker::new(0.3);
        for _ in 0..5 {
            tracker.update(100.0);
        }
        // Not enough samples
        assert!(tracker.is_anomalous(500.0, 3.0, 30).is_none());
    }

    #[tokio::test]
    async fn test_anomaly_detector_records_metric() {
        let config = AnomalyDetectionConfig {
            min_samples: 5,
            sensitivity: 2.0,
            ..Default::default()
        };
        let detector = AnomalyDetector::new(config);

        // Train
        for _ in 0..10 {
            detector.record_metric("test", 100.0, "resource-1").await;
        }

        // Should detect anomaly
        let result = detector.record_metric("test", 1000.0, "resource-1").await;
        assert!(result.is_some());
    }

    #[test]
    fn test_classify_anomaly_lag() {
        let (atype, _) = classify_anomaly("consumer_lag", 10000.0, 100.0, 5.0);
        assert_eq!(atype, AnomalyType::ConsumerLagSpike);
    }

    #[test]
    fn test_classify_anomaly_throughput_drop() {
        let (atype, _) = classify_anomaly("throughput_msgs_sec", 10.0, 1000.0, 4.0);
        assert_eq!(atype, AnomalyType::ThroughputDrop);
    }
}
