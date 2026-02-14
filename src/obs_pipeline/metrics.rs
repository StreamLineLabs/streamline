//! Metrics Collection
//!
//! Provides metrics collection compatible with Prometheus/OpenTelemetry.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Metric type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetricType {
    /// Monotonically increasing counter
    Counter,
    /// Point-in-time value
    Gauge,
    /// Distribution of values
    Histogram,
    /// Precomputed quantiles
    Summary,
}

/// Metric label (key-value pair)
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MetricLabel {
    /// Label key
    pub key: String,
    /// Label value
    pub value: String,
}

impl MetricLabel {
    /// Create a new label
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

/// Labels as a set
pub type Labels = Vec<MetricLabel>;

/// Create labels from slice of tuples
pub fn labels_from_slice(slice: &[(&str, &str)]) -> Labels {
    slice
        .iter()
        .map(|(k, v)| MetricLabel::new(*k, *v))
        .collect()
}

/// Labels to string key for HashMap lookup
fn labels_key(labels: &Labels) -> String {
    let mut sorted: Vec<_> = labels.iter().collect();
    sorted.sort_by(|a, b| a.key.cmp(&b.key));
    sorted
        .iter()
        .map(|l| format!("{}={}", l.key, l.value))
        .collect::<Vec<_>>()
        .join(",")
}

/// Metric value
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetricValue {
    /// Counter value
    Counter(u64),
    /// Gauge value
    Gauge(f64),
    /// Histogram with buckets
    Histogram(HistogramValue),
    /// Summary with quantiles
    Summary(SummaryValue),
}

/// Counter metric
#[derive(Debug, Clone, Default)]
pub struct Counter {
    /// Values by label set
    values: HashMap<String, u64>,
}

impl Counter {
    /// Create a new counter
    pub fn new() -> Self {
        Self {
            values: HashMap::new(),
        }
    }

    /// Increment by value
    pub fn inc(&mut self, labels: &Labels, value: u64) {
        let key = labels_key(labels);
        *self.values.entry(key).or_default() += value;
    }

    /// Get value for labels
    pub fn value(&self, labels: &Labels) -> u64 {
        let key = labels_key(labels);
        self.values.get(&key).copied().unwrap_or(0)
    }

    /// Get all values
    pub fn all_values(&self) -> &HashMap<String, u64> {
        &self.values
    }
}

/// Gauge metric
#[derive(Debug, Clone, Default)]
pub struct Gauge {
    /// Values by label set
    values: HashMap<String, f64>,
}

impl Gauge {
    /// Create a new gauge
    pub fn new() -> Self {
        Self {
            values: HashMap::new(),
        }
    }

    /// Set value
    pub fn set(&mut self, labels: &Labels, value: f64) {
        let key = labels_key(labels);
        self.values.insert(key, value);
    }

    /// Increment by value
    pub fn inc(&mut self, labels: &Labels, value: f64) {
        let key = labels_key(labels);
        *self.values.entry(key).or_default() += value;
    }

    /// Decrement by value
    pub fn dec(&mut self, labels: &Labels, value: f64) {
        let key = labels_key(labels);
        *self.values.entry(key).or_default() -= value;
    }

    /// Get value for labels
    pub fn value(&self, labels: &Labels) -> f64 {
        let key = labels_key(labels);
        self.values.get(&key).copied().unwrap_or(0.0)
    }

    /// Get all values
    pub fn all_values(&self) -> &HashMap<String, f64> {
        &self.values
    }
}

/// Histogram bucket
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistogramBucket {
    /// Upper bound (inclusive)
    pub le: f64,
    /// Count of observations
    pub count: u64,
}

/// Histogram value
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistogramValue {
    /// Buckets
    pub buckets: Vec<HistogramBucket>,
    /// Sum of all observations
    pub sum: f64,
    /// Count of observations
    pub count: u64,
}

impl Default for HistogramValue {
    fn default() -> Self {
        Self {
            buckets: Vec::new(),
            sum: 0.0,
            count: 0,
        }
    }
}

/// Histogram metric
#[derive(Debug, Clone)]
pub struct Histogram {
    /// Bucket boundaries
    bucket_bounds: Vec<f64>,
    /// Values by label set
    values: HashMap<String, HistogramValue>,
}

impl Histogram {
    /// Create with default buckets
    pub fn new() -> Self {
        Self::with_buckets(vec![
            0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
        ])
    }

    /// Create with custom buckets
    pub fn with_buckets(bounds: Vec<f64>) -> Self {
        Self {
            bucket_bounds: bounds,
            values: HashMap::new(),
        }
    }

    /// Observe a value
    pub fn observe(&mut self, labels: &Labels, value: f64) {
        let key = labels_key(labels);
        let entry = self.values.entry(key).or_insert_with(|| {
            let buckets = self
                .bucket_bounds
                .iter()
                .map(|&le| HistogramBucket { le, count: 0 })
                .collect();
            HistogramValue {
                buckets,
                sum: 0.0,
                count: 0,
            }
        });

        entry.sum += value;
        entry.count += 1;

        // Update buckets
        for bucket in &mut entry.buckets {
            if value <= bucket.le {
                bucket.count += 1;
            }
        }
    }

    /// Get histogram value for labels
    pub fn value(&self, labels: &Labels) -> Option<&HistogramValue> {
        let key = labels_key(labels);
        self.values.get(&key)
    }

    /// Get all values
    pub fn all_values(&self) -> &HashMap<String, HistogramValue> {
        &self.values
    }
}

impl Default for Histogram {
    fn default() -> Self {
        Self::new()
    }
}

/// Summary quantile
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SummaryQuantile {
    /// Quantile (0.0 to 1.0)
    pub quantile: f64,
    /// Value at quantile
    pub value: f64,
}

/// Summary value
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SummaryValue {
    /// Quantiles
    pub quantiles: Vec<SummaryQuantile>,
    /// Sum of all observations
    pub sum: f64,
    /// Count of observations
    pub count: u64,
}

/// Metrics snapshot
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    /// Counter metrics
    pub counters: HashMap<String, HashMap<String, u64>>,
    /// Gauge metrics
    pub gauges: HashMap<String, HashMap<String, f64>>,
    /// Histogram metrics
    pub histograms: HashMap<String, HashMap<String, HistogramValue>>,
    /// Snapshot timestamp
    pub timestamp: i64,
}

impl MetricsSnapshot {
    /// Get counter value
    pub fn counter(&self, name: &str, labels: &str) -> Option<u64> {
        self.counters.get(name)?.get(labels).copied()
    }

    /// Get gauge value
    pub fn gauge(&self, name: &str, labels: &str) -> Option<f64> {
        self.gauges.get(name)?.get(labels).copied()
    }
}

/// Metrics collector
pub struct MetricsCollector {
    counters: HashMap<String, Counter>,
    gauges: HashMap<String, Gauge>,
    histograms: HashMap<String, Histogram>,
}

impl MetricsCollector {
    /// Create a new collector
    pub fn new() -> Self {
        Self {
            counters: HashMap::new(),
            gauges: HashMap::new(),
            histograms: HashMap::new(),
        }
    }

    /// Record a counter increment
    pub fn record_counter(&mut self, name: &str, value: u64, labels: &[(&str, &str)]) {
        let labels = labels_from_slice(labels);
        let counter = self.counters.entry(name.to_string()).or_default();
        counter.inc(&labels, value);
    }

    /// Record a gauge value
    pub fn record_gauge(&mut self, name: &str, value: f64, labels: &[(&str, &str)]) {
        let labels = labels_from_slice(labels);
        let gauge = self.gauges.entry(name.to_string()).or_default();
        gauge.set(&labels, value);
    }

    /// Record a histogram observation
    pub fn record_histogram(&mut self, name: &str, value: f64, labels: &[(&str, &str)]) {
        let labels = labels_from_slice(labels);
        let histogram = self.histograms.entry(name.to_string()).or_default();
        histogram.observe(&labels, value);
    }

    /// Get counter
    pub fn counter(&self, name: &str) -> Option<&Counter> {
        self.counters.get(name)
    }

    /// Get gauge
    pub fn gauge(&self, name: &str) -> Option<&Gauge> {
        self.gauges.get(name)
    }

    /// Get histogram
    pub fn histogram(&self, name: &str) -> Option<&Histogram> {
        self.histograms.get(name)
    }

    /// Get total metric count
    pub fn metric_count(&self) -> usize {
        self.counters.len() + self.gauges.len() + self.histograms.len()
    }

    /// Create a snapshot
    pub fn snapshot(&self) -> MetricsSnapshot {
        let mut snapshot = MetricsSnapshot {
            counters: HashMap::new(),
            gauges: HashMap::new(),
            histograms: HashMap::new(),
            timestamp: chrono::Utc::now().timestamp_millis(),
        };

        for (name, counter) in &self.counters {
            snapshot
                .counters
                .insert(name.clone(), counter.all_values().clone());
        }

        for (name, gauge) in &self.gauges {
            snapshot
                .gauges
                .insert(name.clone(), gauge.all_values().clone());
        }

        for (name, histogram) in &self.histograms {
            snapshot
                .histograms
                .insert(name.clone(), histogram.all_values().clone());
        }

        snapshot
    }

    /// Reset all metrics
    pub fn reset(&mut self) {
        self.counters.clear();
        self.gauges.clear();
        self.histograms.clear();
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_counter() {
        let mut counter = Counter::new();
        let labels = vec![MetricLabel::new("method", "GET")];

        counter.inc(&labels, 1);
        counter.inc(&labels, 2);

        assert_eq!(counter.value(&labels), 3);
    }

    #[test]
    fn test_gauge() {
        let mut gauge = Gauge::new();
        let labels = vec![MetricLabel::new("host", "server1")];

        gauge.set(&labels, 100.0);
        assert_eq!(gauge.value(&labels), 100.0);

        gauge.inc(&labels, 10.0);
        assert_eq!(gauge.value(&labels), 110.0);

        gauge.dec(&labels, 5.0);
        assert_eq!(gauge.value(&labels), 105.0);
    }

    #[test]
    fn test_histogram() {
        let mut histogram = Histogram::with_buckets(vec![1.0, 5.0, 10.0]);
        let labels = vec![];

        histogram.observe(&labels, 0.5);
        histogram.observe(&labels, 3.0);
        histogram.observe(&labels, 7.0);

        let value = histogram.value(&labels).unwrap();
        assert_eq!(value.count, 3);
        assert_eq!(value.sum, 10.5);

        // Check buckets
        assert_eq!(value.buckets[0].count, 1); // <= 1.0
        assert_eq!(value.buckets[1].count, 2); // <= 5.0
        assert_eq!(value.buckets[2].count, 3); // <= 10.0
    }

    #[test]
    fn test_metrics_collector() {
        let mut collector = MetricsCollector::new();

        collector.record_counter("requests", 1, &[("method", "GET")]);
        collector.record_gauge("temperature", 25.5, &[]);
        collector.record_histogram("latency", 0.1, &[]);

        assert_eq!(collector.metric_count(), 3);

        let snapshot = collector.snapshot();
        assert!(snapshot.counters.contains_key("requests"));
        assert!(snapshot.gauges.contains_key("temperature"));
        assert!(snapshot.histograms.contains_key("latency"));
    }

    #[test]
    fn test_labels_key() {
        let labels = vec![MetricLabel::new("b", "2"), MetricLabel::new("a", "1")];

        let key = labels_key(&labels);
        assert_eq!(key, "a=1,b=2"); // Should be sorted
    }
}
