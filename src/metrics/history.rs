//! Historical metrics storage for time-series visualization.
//!
//! This module provides a ring buffer for storing metrics snapshots
//! over time, enabling historical charts in the UI.

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Configuration for metrics history.
#[derive(Debug, Clone)]
pub struct MetricsHistoryConfig {
    /// Maximum number of data points to store.
    pub max_points: usize,
    /// Sampling interval in milliseconds.
    pub sample_interval_ms: u64,
}

impl Default for MetricsHistoryConfig {
    fn default() -> Self {
        Self {
            // 720 points at 5s intervals = 1 hour of history
            max_points: 720,
            sample_interval_ms: 5000,
        }
    }
}

/// A single metrics data point.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricPoint {
    /// Unix timestamp in milliseconds.
    pub timestamp: u64,
    /// Messages produced per second.
    pub messages_per_sec: f64,
    /// Bytes received per second.
    pub bytes_in_per_sec: f64,
    /// Bytes sent per second.
    pub bytes_out_per_sec: f64,
    /// Active connections count.
    pub connections: usize,
    /// Total topics count.
    pub topics: usize,
    /// Total partitions count.
    pub partitions: usize,
    /// Consumer group count.
    pub consumer_groups: usize,
    /// Total consumer lag.
    pub total_consumer_lag: i64,
}

impl Default for MetricPoint {
    fn default() -> Self {
        Self {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            messages_per_sec: 0.0,
            bytes_in_per_sec: 0.0,
            bytes_out_per_sec: 0.0,
            connections: 0,
            topics: 0,
            partitions: 0,
            consumer_groups: 0,
            total_consumer_lag: 0,
        }
    }
}

/// Historical metrics response for API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoricalMetrics {
    /// Data points.
    pub points: Vec<MetricPoint>,
    /// Start time of the data range (Unix ms).
    pub start_time: u64,
    /// End time of the data range (Unix ms).
    pub end_time: u64,
    /// Sample interval in milliseconds.
    pub interval_ms: u64,
}

/// Ring buffer for storing metrics history.
pub struct MetricsHistory {
    config: MetricsHistoryConfig,
    points: RwLock<VecDeque<MetricPoint>>,
    last_sample: RwLock<Option<Instant>>,
}

impl MetricsHistory {
    /// Create a new metrics history store.
    pub fn new(config: MetricsHistoryConfig) -> Self {
        Self {
            config,
            points: RwLock::new(VecDeque::with_capacity(720)),
            last_sample: RwLock::new(None),
        }
    }

    /// Create a new metrics history with default config wrapped in Arc.
    pub fn new_shared() -> Arc<Self> {
        Arc::new(Self::new(MetricsHistoryConfig::default()))
    }

    /// Check if enough time has passed for a new sample.
    pub fn should_sample(&self) -> bool {
        let last = self.last_sample.read();
        match *last {
            None => true,
            Some(instant) => {
                instant.elapsed() >= Duration::from_millis(self.config.sample_interval_ms)
            }
        }
    }

    /// Add a new data point.
    pub fn add_point(&self, point: MetricPoint) {
        let mut points = self.points.write();

        // Remove oldest points if at capacity
        while points.len() >= self.config.max_points {
            points.pop_front();
        }

        points.push_back(point);

        // Update last sample time
        *self.last_sample.write() = Some(Instant::now());
    }

    /// Get all historical data points.
    pub fn get_all(&self) -> Vec<MetricPoint> {
        self.points.read().iter().cloned().collect()
    }

    /// Get data points within a duration from now.
    pub fn get_since(&self, duration: Duration) -> Vec<MetricPoint> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let cutoff = now.saturating_sub(duration.as_millis() as u64);

        self.points
            .read()
            .iter()
            .filter(|p| p.timestamp >= cutoff)
            .cloned()
            .collect()
    }

    /// Get historical metrics for a specific duration.
    pub fn get_history(&self, duration: Duration) -> HistoricalMetrics {
        let points = self.get_since(duration);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let start_time = points.first().map(|p| p.timestamp).unwrap_or(now);
        let end_time = points.last().map(|p| p.timestamp).unwrap_or(now);

        HistoricalMetrics {
            points,
            start_time,
            end_time,
            interval_ms: self.config.sample_interval_ms,
        }
    }

    /// Get the number of stored points.
    pub fn len(&self) -> usize {
        self.points.read().len()
    }

    /// Check if the history is empty.
    pub fn is_empty(&self) -> bool {
        self.points.read().is_empty()
    }

    /// Clear all stored points.
    pub fn clear(&self) {
        self.points.write().clear();
        *self.last_sample.write() = None;
    }

    /// Get the sampling interval.
    pub fn sample_interval(&self) -> Duration {
        Duration::from_millis(self.config.sample_interval_ms)
    }
}

impl std::fmt::Debug for MetricsHistory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricsHistory")
            .field("config", &self.config)
            .field("points_count", &self.len())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_history_config_default() {
        let config = MetricsHistoryConfig::default();
        assert_eq!(config.max_points, 720);
        assert_eq!(config.sample_interval_ms, 5000);
    }

    #[test]
    fn test_metrics_history_add_point() {
        let history = MetricsHistory::new(MetricsHistoryConfig::default());

        let point = MetricPoint::default();
        history.add_point(point);

        assert_eq!(history.len(), 1);
    }

    #[test]
    fn test_metrics_history_ring_buffer() {
        let config = MetricsHistoryConfig {
            max_points: 3,
            sample_interval_ms: 1000,
        };
        let history = MetricsHistory::new(config);

        // Add 5 points, should only keep last 3
        for i in 0..5 {
            let point = MetricPoint {
                messages_per_sec: i as f64,
                ..Default::default()
            };
            history.add_point(point);
        }

        assert_eq!(history.len(), 3);

        let points = history.get_all();
        assert_eq!(points[0].messages_per_sec, 2.0);
        assert_eq!(points[1].messages_per_sec, 3.0);
        assert_eq!(points[2].messages_per_sec, 4.0);
    }

    #[test]
    fn test_metrics_history_get_since() {
        let history = MetricsHistory::new(MetricsHistoryConfig::default());

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        // Add a point from 30 minutes ago
        let old_point = MetricPoint {
            timestamp: now - 30 * 60 * 1000,
            ..Default::default()
        };
        history.add_point(old_point);

        // Add a point from now
        let new_point = MetricPoint::default();
        history.add_point(new_point);

        // Get points from last 15 minutes - should only get 1
        let recent = history.get_since(Duration::from_secs(15 * 60));
        assert_eq!(recent.len(), 1);

        // Get points from last hour - should get both
        let all = history.get_since(Duration::from_secs(60 * 60));
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn test_metrics_history_clear() {
        let history = MetricsHistory::new(MetricsHistoryConfig::default());

        history.add_point(MetricPoint::default());
        history.add_point(MetricPoint::default());

        assert_eq!(history.len(), 2);

        history.clear();

        assert!(history.is_empty());
    }

    #[test]
    fn test_metrics_history_debug() {
        let history = MetricsHistory::new(MetricsHistoryConfig::default());
        let debug_str = format!("{:?}", history);
        assert!(debug_str.contains("MetricsHistory"));
    }

    #[test]
    fn test_historical_metrics_response() {
        let history = MetricsHistory::new(MetricsHistoryConfig::default());

        history.add_point(MetricPoint::default());

        let response = history.get_history(Duration::from_secs(3600));
        assert_eq!(response.points.len(), 1);
        assert!(response.start_time > 0);
        assert!(response.end_time >= response.start_time);
    }
}
