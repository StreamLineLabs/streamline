//! Metrics collection utilities
//!
//! This module provides utilities for collecting and aggregating metrics
//! from various sources including system, application, and custom metrics.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Metric type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MetricType {
    /// Counter - monotonically increasing value
    Counter,
    /// Gauge - value that can go up or down
    Gauge,
    /// Histogram - distribution of values
    Histogram,
    /// Summary - similar to histogram with quantiles
    Summary,
}

/// A metric value
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetricValue {
    /// Counter value
    Counter(u64),
    /// Gauge value
    Gauge(f64),
    /// Histogram buckets
    Histogram(HistogramValue),
    /// Summary with quantiles
    Summary(SummaryValue),
}

/// Histogram value with buckets
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HistogramValue {
    /// Bucket boundaries and counts
    pub buckets: Vec<(f64, u64)>,
    /// Sum of all observations
    pub sum: f64,
    /// Count of observations
    pub count: u64,
}

/// Summary value with quantiles
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SummaryValue {
    /// Quantile values (e.g., 0.5, 0.9, 0.99)
    pub quantiles: Vec<(f64, f64)>,
    /// Sum of all observations
    pub sum: f64,
    /// Count of observations
    pub count: u64,
}

/// Metric definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricDefinition {
    /// Metric name
    pub name: String,
    /// Metric type
    pub metric_type: MetricType,
    /// Description
    pub description: String,
    /// Labels
    pub labels: Vec<String>,
    /// Unit (optional)
    pub unit: Option<String>,
}

/// A collected metric sample
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricSample {
    /// Metric name
    pub name: String,
    /// Metric value
    pub value: MetricValue,
    /// Labels
    pub labels: HashMap<String, String>,
    /// Timestamp (ms since epoch)
    pub timestamp: i64,
}

/// Metrics registry for managing metric definitions
#[derive(Debug, Default)]
pub struct MetricsRegistry {
    /// Registered metrics
    definitions: parking_lot::RwLock<HashMap<String, MetricDefinition>>,
    /// Counter values
    counters: parking_lot::RwLock<HashMap<String, Arc<AtomicU64>>>,
    /// Gauge values
    gauges: parking_lot::RwLock<HashMap<String, Arc<parking_lot::RwLock<f64>>>>,
}

impl MetricsRegistry {
    /// Create a new metrics registry
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a counter metric
    pub fn register_counter(&self, name: &str, description: &str) {
        let def = MetricDefinition {
            name: name.to_string(),
            metric_type: MetricType::Counter,
            description: description.to_string(),
            labels: Vec::new(),
            unit: None,
        };
        self.definitions.write().insert(name.to_string(), def);
        self.counters
            .write()
            .insert(name.to_string(), Arc::new(AtomicU64::new(0)));
    }

    /// Register a gauge metric
    pub fn register_gauge(&self, name: &str, description: &str) {
        let def = MetricDefinition {
            name: name.to_string(),
            metric_type: MetricType::Gauge,
            description: description.to_string(),
            labels: Vec::new(),
            unit: None,
        };
        self.definitions.write().insert(name.to_string(), def);
        self.gauges
            .write()
            .insert(name.to_string(), Arc::new(parking_lot::RwLock::new(0.0)));
    }

    /// Increment a counter
    pub fn increment_counter(&self, name: &str, value: u64) {
        if let Some(counter) = self.counters.read().get(name) {
            counter.fetch_add(value, Ordering::Relaxed);
        }
    }

    /// Set a gauge value
    pub fn set_gauge(&self, name: &str, value: f64) {
        if let Some(gauge) = self.gauges.read().get(name) {
            *gauge.write() = value;
        }
    }

    /// Get counter value
    pub fn get_counter(&self, name: &str) -> Option<u64> {
        self.counters
            .read()
            .get(name)
            .map(|c| c.load(Ordering::Relaxed))
    }

    /// Get gauge value
    pub fn get_gauge(&self, name: &str) -> Option<f64> {
        self.gauges.read().get(name).map(|g| *g.read())
    }

    /// Get all metric definitions
    pub fn get_definitions(&self) -> Vec<MetricDefinition> {
        self.definitions.read().values().cloned().collect()
    }

    /// Collect all current metric samples
    pub fn collect(&self) -> Vec<MetricSample> {
        let timestamp = chrono::Utc::now().timestamp_millis();
        let mut samples = Vec::new();

        // Collect counters
        for (name, counter) in self.counters.read().iter() {
            samples.push(MetricSample {
                name: name.clone(),
                value: MetricValue::Counter(counter.load(Ordering::Relaxed)),
                labels: HashMap::new(),
                timestamp,
            });
        }

        // Collect gauges
        for (name, gauge) in self.gauges.read().iter() {
            samples.push(MetricSample {
                name: name.clone(),
                value: MetricValue::Gauge(*gauge.read()),
                labels: HashMap::new(),
                timestamp,
            });
        }

        samples
    }
}

/// Rate calculator for computing rates from counters
pub struct RateCalculator {
    /// Previous values
    previous: parking_lot::RwLock<HashMap<String, (i64, u64)>>,
}

impl RateCalculator {
    /// Create a new rate calculator
    pub fn new() -> Self {
        Self {
            previous: parking_lot::RwLock::new(HashMap::new()),
        }
    }

    /// Calculate rate for a counter
    pub fn calculate(&self, name: &str, current_value: u64, timestamp: i64) -> f64 {
        let mut previous = self.previous.write();

        if let Some((prev_time, prev_value)) = previous.get(name) {
            let time_diff = (timestamp - prev_time) as f64 / 1000.0; // Convert to seconds
            let value_diff = current_value.saturating_sub(*prev_value);

            let rate = if time_diff > 0.0 {
                value_diff as f64 / time_diff
            } else {
                0.0
            };

            previous.insert(name.to_string(), (timestamp, current_value));
            rate
        } else {
            previous.insert(name.to_string(), (timestamp, current_value));
            0.0
        }
    }
}

impl Default for RateCalculator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_registry() {
        let registry = MetricsRegistry::new();

        registry.register_counter("requests_total", "Total requests");
        registry.register_gauge("active_connections", "Active connections");

        registry.increment_counter("requests_total", 10);
        registry.set_gauge("active_connections", 5.0);

        assert_eq!(registry.get_counter("requests_total"), Some(10));
        assert_eq!(registry.get_gauge("active_connections"), Some(5.0));
    }

    #[test]
    fn test_rate_calculator() {
        let calc = RateCalculator::new();

        // First sample - no rate yet
        let rate1 = calc.calculate("test", 100, 1000);
        assert_eq!(rate1, 0.0);

        // Second sample - 100 units in 1 second = 100/sec
        let rate2 = calc.calculate("test", 200, 2000);
        assert_eq!(rate2, 100.0);
    }
}
