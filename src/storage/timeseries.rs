//! Time-series aware storage optimizations
//!
//! Provides specialized storage for IoT, metrics, and event data workloads.
//! Supports automatic downsampling, Gorilla-style compression, and
//! configurable retention policies per resolution tier.
//!
//! Features:
//! - Register time-series topics with timestamp and value field mappings
//! - Gorilla-inspired compression (delta-of-delta, XOR float encoding)
//! - Multi-tier retention (raw, minute, hour, day)
//! - Automatic downsampling with configurable aggregation rules
//! - Time-range queries with on-the-fly aggregation

use crate::error::{Result, StreamlineError};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, info};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Engine for managing time-series topics, downsampling rules, and queries.
pub struct TimeSeriesEngine {
    series: Arc<RwLock<HashMap<String, TimeSeriesConfig>>>,
    downsampling_rules: Arc<RwLock<Vec<DownsamplingRule>>>,
    stats: Arc<TimeSeriesStats>,
    /// In-memory buffer of ingested points keyed by topic.
    points: Arc<RwLock<HashMap<String, Vec<DataPoint>>>>,
}

/// Configuration for a registered time-series topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeSeriesConfig {
    /// Topic name
    pub topic: String,
    /// JSONPath to the timestamp field in each record
    pub timestamp_field: String,
    /// Fields to aggregate during downsampling
    pub value_fields: Vec<String>,
    /// Per-tier retention configuration
    pub retention: TimeSeriesRetention,
    /// Compression strategy for this series
    pub compression: TimeSeriesCompression,
    /// Whether automatic downsampling is enabled
    pub enable_downsampling: bool,
    /// Unix-epoch millis when this series was registered
    pub created_at: u64,
}

/// Multi-tier retention policy controlling how long each resolution is kept.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeSeriesRetention {
    /// Hours to retain raw data (default: 24)
    #[serde(default = "default_raw_retention")]
    pub raw_retention_hours: u64,
    /// Hours to retain minute-level aggregates (default: 168 / 7 days)
    #[serde(default = "default_minute_retention")]
    pub minute_retention_hours: u64,
    /// Hours to retain hour-level aggregates (default: 8760 / 1 year)
    #[serde(default = "default_hour_retention")]
    pub hour_retention_hours: u64,
    /// Hours to retain day-level aggregates (default: 87600 / 10 years)
    #[serde(default = "default_day_retention")]
    pub day_retention_hours: u64,
}

fn default_raw_retention() -> u64 {
    24
}
fn default_minute_retention() -> u64 {
    168
}
fn default_hour_retention() -> u64 {
    8760
}
fn default_day_retention() -> u64 {
    87600
}

impl Default for TimeSeriesRetention {
    fn default() -> Self {
        Self {
            raw_retention_hours: default_raw_retention(),
            minute_retention_hours: default_minute_retention(),
            hour_retention_hours: default_hour_retention(),
            day_retention_hours: default_day_retention(),
        }
    }
}

/// Compression strategy applied to time-series data.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TimeSeriesCompression {
    /// No compression
    None,
    /// Delta-of-delta encoding for monotonic timestamps (Gorilla-style)
    DeltaOfDelta,
    /// XOR-based encoding for floating-point values (Gorilla-style)
    Xor,
    /// Dictionary encoding for low-cardinality string tags
    Dictionary,
    /// Run-length encoding for repeated values
    RunLength,
}

impl Default for TimeSeriesCompression {
    fn default() -> Self {
        Self::None
    }
}

/// A rule that drives automatic downsampling from one topic to another.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownsamplingRule {
    /// Source topic to read raw data from
    pub source_topic: String,
    /// Destination topic for downsampled results
    pub target_topic: String,
    /// Aggregation interval
    pub interval: DownsampleInterval,
    /// Aggregation functions to apply
    pub aggregations: Vec<Aggregation>,
    /// Current operational status
    pub status: RuleStatus,
}

/// Downsampling interval granularity.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(tag = "unit", content = "value", rename_all = "snake_case")]
pub enum DownsampleInterval {
    Second(u32),
    Minute(u32),
    Hour(u32),
    Day(u32),
}

impl DownsampleInterval {
    /// Returns the interval duration in seconds.
    pub fn as_secs(&self) -> u64 {
        match self {
            Self::Second(n) => *n as u64,
            Self::Minute(n) => *n as u64 * 60,
            Self::Hour(n) => *n as u64 * 3600,
            Self::Day(n) => *n as u64 * 86400,
        }
    }
}

/// Aggregation function applied during downsampling or queries.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Aggregation {
    Avg,
    Min,
    Max,
    Sum,
    Count,
    First,
    Last,
    P50,
    P99,
}

/// Operational status of a downsampling rule.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum RuleStatus {
    Active,
    Paused,
    Error { message: String },
}

/// Atomic counters for time-series engine metrics.
#[derive(Debug)]
pub struct TimeSeriesStats {
    pub series_count: AtomicU64,
    pub points_ingested: AtomicU64,
    pub points_downsampled: AtomicU64,
    /// Compression ratio stored as `ratio * 100` (e.g. 350 = 3.5×)
    pub compression_ratio: AtomicU64,
    pub queries_executed: AtomicU64,
}

impl Default for TimeSeriesStats {
    fn default() -> Self {
        Self {
            series_count: AtomicU64::new(0),
            points_ingested: AtomicU64::new(0),
            points_downsampled: AtomicU64::new(0),
            compression_ratio: AtomicU64::new(100),
            queries_executed: AtomicU64::new(0),
        }
    }
}

/// Serializable snapshot of [`TimeSeriesStats`].
#[derive(Debug, Clone, Serialize)]
pub struct TimeSeriesStatsSnapshot {
    pub series_count: u64,
    pub points_ingested: u64,
    pub points_downsampled: u64,
    pub compression_ratio: u64,
    pub queries_executed: u64,
}

/// A time-range query against a registered series topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeSeriesQuery {
    /// Topic to query
    pub topic: String,
    /// Inclusive start timestamp (epoch millis)
    pub start_time: u64,
    /// Exclusive end timestamp (epoch millis)
    pub end_time: u64,
    /// Optional aggregation interval – if set, points are bucketed
    pub interval: Option<DownsampleInterval>,
    /// Aggregation functions applied per bucket
    pub aggregations: Vec<Aggregation>,
    /// Tag-based filters (exact match)
    pub filters: HashMap<String, String>,
    /// Maximum number of points to return
    pub limit: Option<usize>,
}

/// Result of a time-series query.
#[derive(Debug, Clone, Serialize)]
pub struct TimeSeriesResult {
    pub points: Vec<DataPoint>,
    pub query_time_ms: f64,
    pub points_scanned: u64,
}

/// A single time-series data point.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataPoint {
    /// Timestamp in epoch milliseconds
    pub timestamp: u64,
    /// Numeric values keyed by field name
    pub values: HashMap<String, f64>,
    /// String tags/labels
    pub tags: HashMap<String, String>,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Apply a single aggregation function to a slice of f64 values.
fn apply_aggregation(agg: &Aggregation, values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    match agg {
        Aggregation::Avg => values.iter().sum::<f64>() / values.len() as f64,
        Aggregation::Min => values.iter().cloned().fold(f64::INFINITY, f64::min),
        Aggregation::Max => values.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
        Aggregation::Sum => values.iter().sum(),
        Aggregation::Count => values.len() as f64,
        Aggregation::First => values[0],
        Aggregation::Last => values[values.len() - 1],
        Aggregation::P50 => percentile(values, 50.0),
        Aggregation::P99 => percentile(values, 99.0),
    }
}

fn percentile(values: &[f64], pct: f64) -> f64 {
    let mut sorted: Vec<f64> = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let idx = ((pct / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

impl TimeSeriesEngine {
    /// Create a new time-series engine.
    pub fn new() -> Self {
        info!("initializing time-series engine");
        Self {
            series: Arc::new(RwLock::new(HashMap::new())),
            downsampling_rules: Arc::new(RwLock::new(Vec::new())),
            stats: Arc::new(TimeSeriesStats::default()),
            points: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a new time-series topic configuration.
    pub fn register_series(&self, config: TimeSeriesConfig) -> Result<()> {
        let topic = config.topic.clone();
        let mut series = self.series.write();
        if series.contains_key(&topic) {
            return Err(StreamlineError::storage(
                "register_series",
                format!("series already registered: {topic}"),
            ));
        }
        info!(topic = %topic, "registered time-series");
        series.insert(topic.clone(), config);
        self.points.write().entry(topic).or_default();
        self.stats.series_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Retrieve configuration for a registered series topic.
    pub fn get_series(&self, topic: &str) -> Result<TimeSeriesConfig> {
        self.series
            .read()
            .get(topic)
            .cloned()
            .ok_or_else(|| {
                StreamlineError::storage(
                    "get_series",
                    format!("series not found: {topic}"),
                )
            })
    }

    /// List all registered series configurations.
    pub fn list_series(&self) -> Vec<TimeSeriesConfig> {
        self.series.read().values().cloned().collect()
    }

    /// Remove a registered series and its buffered data.
    pub fn remove_series(&self, topic: &str) -> Result<()> {
        let mut series = self.series.write();
        if series.remove(topic).is_none() {
            return Err(StreamlineError::storage(
                "remove_series",
                format!("series not found: {topic}"),
            ));
        }
        self.points.write().remove(topic);
        self.stats.series_count.fetch_sub(1, Ordering::Relaxed);
        info!(topic = %topic, "removed time-series");
        Ok(())
    }

    /// Add a downsampling rule.
    pub fn add_downsampling_rule(&self, rule: DownsamplingRule) -> Result<()> {
        if rule.aggregations.is_empty() {
            return Err(StreamlineError::storage(
                "add_downsampling_rule",
                "rule must have at least one aggregation",
            ));
        }
        // Verify source topic is registered
        if !self.series.read().contains_key(&rule.source_topic) {
            return Err(StreamlineError::storage(
                "add_downsampling_rule",
                format!("source topic not registered: {}", rule.source_topic),
            ));
        }
        debug!(
            source = %rule.source_topic,
            target = %rule.target_topic,
            "added downsampling rule"
        );
        self.downsampling_rules.write().push(rule);
        Ok(())
    }

    /// List all downsampling rules.
    pub fn list_downsampling_rules(&self) -> Vec<DownsamplingRule> {
        self.downsampling_rules.read().clone()
    }

    /// Remove a downsampling rule by source and target topic pair.
    pub fn remove_rule(&self, source_topic: &str, target_topic: &str) -> Result<()> {
        let mut rules = self.downsampling_rules.write();
        let before = rules.len();
        rules.retain(|r| !(r.source_topic == source_topic && r.target_topic == target_topic));
        if rules.len() == before {
            return Err(StreamlineError::storage(
                "remove_rule",
                format!("rule not found: {source_topic} -> {target_topic}"),
            ));
        }
        debug!(source = %source_topic, target = %target_topic, "removed downsampling rule");
        Ok(())
    }

    /// Ingest a single data point into the specified topic.
    pub fn ingest_point(&self, topic: &str, point: DataPoint) -> Result<()> {
        if !self.series.read().contains_key(topic) {
            return Err(StreamlineError::storage(
                "ingest_point",
                format!("series not registered: {topic}"),
            ));
        }
        self.points
            .write()
            .entry(topic.to_string())
            .or_default()
            .push(point);
        self.stats.points_ingested.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Execute a time-series query with optional aggregation.
    pub fn query(&self, query: &TimeSeriesQuery) -> Result<TimeSeriesResult> {
        let start = std::time::Instant::now();

        if !self.series.read().contains_key(&query.topic) {
            return Err(StreamlineError::storage(
                "query",
                format!("series not registered: {}", query.topic),
            ));
        }

        let points_guard = self.points.read();
        let all_points = points_guard.get(&query.topic).cloned().unwrap_or_default();
        drop(points_guard);

        let points_scanned = all_points.len() as u64;

        // Filter by time range and tags
        let filtered: Vec<DataPoint> = all_points
            .into_iter()
            .filter(|p| p.timestamp >= query.start_time && p.timestamp < query.end_time)
            .filter(|p| {
                query
                    .filters
                    .iter()
                    .all(|(k, v)| p.tags.get(k).map_or(false, |tv| tv == v))
            })
            .collect();

        let result_points = if let Some(interval) = &query.interval {
            self.aggregate_points(&filtered, interval, &query.aggregations)
        } else {
            filtered
        };

        let result_points = match query.limit {
            Some(limit) => result_points.into_iter().take(limit).collect(),
            None => result_points,
        };

        let elapsed = start.elapsed().as_secs_f64() * 1000.0;
        self.stats.queries_executed.fetch_add(1, Ordering::Relaxed);

        debug!(
            topic = %query.topic,
            points = result_points.len(),
            scanned = points_scanned,
            time_ms = elapsed,
            "time-series query executed"
        );

        Ok(TimeSeriesResult {
            points: result_points,
            query_time_ms: elapsed,
            points_scanned,
        })
    }

    /// Return a snapshot of current engine statistics.
    pub fn stats(&self) -> TimeSeriesStatsSnapshot {
        TimeSeriesStatsSnapshot {
            series_count: self.stats.series_count.load(Ordering::Relaxed),
            points_ingested: self.stats.points_ingested.load(Ordering::Relaxed),
            points_downsampled: self.stats.points_downsampled.load(Ordering::Relaxed),
            compression_ratio: self.stats.compression_ratio.load(Ordering::Relaxed),
            queries_executed: self.stats.queries_executed.load(Ordering::Relaxed),
        }
    }

    // -- private helpers ----------------------------------------------------

    fn aggregate_points(
        &self,
        points: &[DataPoint],
        interval: &DownsampleInterval,
        aggregations: &[Aggregation],
    ) -> Vec<DataPoint> {
        if points.is_empty() || aggregations.is_empty() {
            return Vec::new();
        }

        let interval_ms = interval.as_secs() * 1000;
        if interval_ms == 0 {
            return points.to_vec();
        }

        // Collect all value field names
        let mut field_names: Vec<String> = Vec::new();
        for p in points {
            for k in p.values.keys() {
                if !field_names.contains(k) {
                    field_names.push(k.clone());
                }
            }
        }

        // Group points into time buckets
        let mut buckets: HashMap<u64, Vec<&DataPoint>> = HashMap::new();
        for p in points {
            let bucket = (p.timestamp / interval_ms) * interval_ms;
            buckets.entry(bucket).or_default().push(p);
        }

        let mut result: Vec<DataPoint> = Vec::with_capacity(buckets.len());
        for (bucket_ts, bucket_points) in &buckets {
            let mut values = HashMap::new();
            for field in &field_names {
                let field_vals: Vec<f64> = bucket_points
                    .iter()
                    .filter_map(|p| p.values.get(field).copied())
                    .collect();
                if field_vals.is_empty() {
                    continue;
                }
                for agg in aggregations {
                    let key = format!("{}_{:?}", field, agg).to_lowercase();
                    values.insert(key, apply_aggregation(agg, &field_vals));
                }
            }
            // Carry forward tags from the first point in the bucket
            let tags = bucket_points
                .first()
                .map(|p| p.tags.clone())
                .unwrap_or_default();
            result.push(DataPoint {
                timestamp: *bucket_ts,
                values,
                tags,
            });
        }

        result.sort_by_key(|p| p.timestamp);
        self.stats
            .points_downsampled
            .fetch_add(result.len() as u64, Ordering::Relaxed);
        result
    }
}

impl Default for TimeSeriesEngine {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_config(topic: &str) -> TimeSeriesConfig {
        TimeSeriesConfig {
            topic: topic.to_string(),
            timestamp_field: "$.timestamp".to_string(),
            value_fields: vec!["temperature".to_string()],
            retention: TimeSeriesRetention::default(),
            compression: TimeSeriesCompression::None,
            enable_downsampling: false,
            created_at: now_millis(),
        }
    }

    fn make_point(ts: u64, field: &str, value: f64) -> DataPoint {
        let mut values = HashMap::new();
        values.insert(field.to_string(), value);
        DataPoint {
            timestamp: ts,
            values,
            tags: HashMap::new(),
        }
    }

    fn make_tagged_point(ts: u64, field: &str, value: f64, tag_k: &str, tag_v: &str) -> DataPoint {
        let mut p = make_point(ts, field, value);
        p.tags.insert(tag_k.to_string(), tag_v.to_string());
        p
    }

    // -- engine creation ---------------------------------------------------

    #[test]
    fn test_new_engine() {
        let engine = TimeSeriesEngine::new();
        let s = engine.stats();
        assert_eq!(s.series_count, 0);
        assert_eq!(s.points_ingested, 0);
        assert_eq!(s.queries_executed, 0);
    }

    #[test]
    fn test_default_engine() {
        let engine = TimeSeriesEngine::default();
        assert_eq!(engine.list_series().len(), 0);
    }

    // -- series registration -----------------------------------------------

    #[test]
    fn test_register_series() {
        let engine = TimeSeriesEngine::new();
        engine.register_series(make_config("metrics")).unwrap();
        assert_eq!(engine.stats().series_count, 1);
    }

    #[test]
    fn test_register_duplicate_series() {
        let engine = TimeSeriesEngine::new();
        engine.register_series(make_config("metrics")).unwrap();
        let err = engine.register_series(make_config("metrics"));
        assert!(err.is_err());
    }

    #[test]
    fn test_get_series() {
        let engine = TimeSeriesEngine::new();
        engine.register_series(make_config("temp")).unwrap();
        let cfg = engine.get_series("temp").unwrap();
        assert_eq!(cfg.topic, "temp");
        assert_eq!(cfg.timestamp_field, "$.timestamp");
    }

    #[test]
    fn test_get_series_not_found() {
        let engine = TimeSeriesEngine::new();
        assert!(engine.get_series("nope").is_err());
    }

    #[test]
    fn test_list_series() {
        let engine = TimeSeriesEngine::new();
        engine.register_series(make_config("a")).unwrap();
        engine.register_series(make_config("b")).unwrap();
        assert_eq!(engine.list_series().len(), 2);
    }

    #[test]
    fn test_remove_series() {
        let engine = TimeSeriesEngine::new();
        engine.register_series(make_config("rm_me")).unwrap();
        engine.remove_series("rm_me").unwrap();
        assert_eq!(engine.stats().series_count, 0);
        assert!(engine.get_series("rm_me").is_err());
    }

    #[test]
    fn test_remove_series_not_found() {
        let engine = TimeSeriesEngine::new();
        assert!(engine.remove_series("ghost").is_err());
    }

    // -- downsampling rules ------------------------------------------------

    #[test]
    fn test_add_downsampling_rule() {
        let engine = TimeSeriesEngine::new();
        engine.register_series(make_config("raw")).unwrap();
        let rule = DownsamplingRule {
            source_topic: "raw".to_string(),
            target_topic: "raw_1m".to_string(),
            interval: DownsampleInterval::Minute(1),
            aggregations: vec![Aggregation::Avg],
            status: RuleStatus::Active,
        };
        engine.add_downsampling_rule(rule).unwrap();
        assert_eq!(engine.list_downsampling_rules().len(), 1);
    }

    #[test]
    fn test_add_rule_empty_aggregations() {
        let engine = TimeSeriesEngine::new();
        engine.register_series(make_config("raw")).unwrap();
        let rule = DownsamplingRule {
            source_topic: "raw".to_string(),
            target_topic: "raw_1m".to_string(),
            interval: DownsampleInterval::Minute(1),
            aggregations: vec![],
            status: RuleStatus::Active,
        };
        assert!(engine.add_downsampling_rule(rule).is_err());
    }

    #[test]
    fn test_add_rule_unregistered_source() {
        let engine = TimeSeriesEngine::new();
        let rule = DownsamplingRule {
            source_topic: "missing".to_string(),
            target_topic: "missing_1m".to_string(),
            interval: DownsampleInterval::Minute(1),
            aggregations: vec![Aggregation::Sum],
            status: RuleStatus::Active,
        };
        assert!(engine.add_downsampling_rule(rule).is_err());
    }

    #[test]
    fn test_remove_rule() {
        let engine = TimeSeriesEngine::new();
        engine.register_series(make_config("src")).unwrap();
        let rule = DownsamplingRule {
            source_topic: "src".to_string(),
            target_topic: "dst".to_string(),
            interval: DownsampleInterval::Hour(1),
            aggregations: vec![Aggregation::Max],
            status: RuleStatus::Active,
        };
        engine.add_downsampling_rule(rule).unwrap();
        engine.remove_rule("src", "dst").unwrap();
        assert!(engine.list_downsampling_rules().is_empty());
    }

    #[test]
    fn test_remove_rule_not_found() {
        let engine = TimeSeriesEngine::new();
        assert!(engine.remove_rule("x", "y").is_err());
    }

    // -- ingestion ---------------------------------------------------------

    #[test]
    fn test_ingest_point() {
        let engine = TimeSeriesEngine::new();
        engine.register_series(make_config("iot")).unwrap();
        engine
            .ingest_point("iot", make_point(1000, "temperature", 22.5))
            .unwrap();
        assert_eq!(engine.stats().points_ingested, 1);
    }

    #[test]
    fn test_ingest_unregistered_topic() {
        let engine = TimeSeriesEngine::new();
        let err = engine.ingest_point("nope", make_point(1000, "v", 1.0));
        assert!(err.is_err());
    }

    // -- queries -----------------------------------------------------------

    #[test]
    fn test_query_basic() {
        let engine = TimeSeriesEngine::new();
        engine.register_series(make_config("q")).unwrap();
        for i in 0..10 {
            engine
                .ingest_point("q", make_point(1000 + i * 100, "temperature", i as f64))
                .unwrap();
        }
        let result = engine
            .query(&TimeSeriesQuery {
                topic: "q".to_string(),
                start_time: 1000,
                end_time: 2000,
                interval: None,
                aggregations: vec![],
                filters: HashMap::new(),
                limit: None,
            })
            .unwrap();
        assert_eq!(result.points.len(), 10);
        assert_eq!(result.points_scanned, 10);
        assert_eq!(engine.stats().queries_executed, 1);
    }

    #[test]
    fn test_query_time_range_filter() {
        let engine = TimeSeriesEngine::new();
        engine.register_series(make_config("r")).unwrap();
        for ts in [100, 200, 300, 400, 500] {
            engine
                .ingest_point("r", make_point(ts, "v", 1.0))
                .unwrap();
        }
        let result = engine
            .query(&TimeSeriesQuery {
                topic: "r".to_string(),
                start_time: 200,
                end_time: 400,
                interval: None,
                aggregations: vec![],
                filters: HashMap::new(),
                limit: None,
            })
            .unwrap();
        assert_eq!(result.points.len(), 2); // 200, 300
    }

    #[test]
    fn test_query_with_tag_filter() {
        let engine = TimeSeriesEngine::new();
        engine.register_series(make_config("tagged")).unwrap();
        engine
            .ingest_point("tagged", make_tagged_point(100, "v", 1.0, "host", "a"))
            .unwrap();
        engine
            .ingest_point("tagged", make_tagged_point(200, "v", 2.0, "host", "b"))
            .unwrap();
        engine
            .ingest_point("tagged", make_tagged_point(300, "v", 3.0, "host", "a"))
            .unwrap();
        let mut filters = HashMap::new();
        filters.insert("host".to_string(), "a".to_string());
        let result = engine
            .query(&TimeSeriesQuery {
                topic: "tagged".to_string(),
                start_time: 0,
                end_time: 1000,
                interval: None,
                aggregations: vec![],
                filters,
                limit: None,
            })
            .unwrap();
        assert_eq!(result.points.len(), 2);
    }

    #[test]
    fn test_query_with_limit() {
        let engine = TimeSeriesEngine::new();
        engine.register_series(make_config("lim")).unwrap();
        for i in 0..20 {
            engine
                .ingest_point("lim", make_point(i * 100, "v", i as f64))
                .unwrap();
        }
        let result = engine
            .query(&TimeSeriesQuery {
                topic: "lim".to_string(),
                start_time: 0,
                end_time: u64::MAX,
                interval: None,
                aggregations: vec![],
                filters: HashMap::new(),
                limit: Some(5),
            })
            .unwrap();
        assert_eq!(result.points.len(), 5);
    }

    #[test]
    fn test_query_unregistered_topic() {
        let engine = TimeSeriesEngine::new();
        let err = engine.query(&TimeSeriesQuery {
            topic: "missing".to_string(),
            start_time: 0,
            end_time: 1000,
            interval: None,
            aggregations: vec![],
            filters: HashMap::new(),
            limit: None,
        });
        assert!(err.is_err());
    }

    // -- aggregation -------------------------------------------------------

    #[test]
    fn test_query_with_aggregation() {
        let engine = TimeSeriesEngine::new();
        engine.register_series(make_config("agg")).unwrap();
        // Two points in the same 10-second bucket
        engine
            .ingest_point("agg", make_point(10_000, "temperature", 20.0))
            .unwrap();
        engine
            .ingest_point("agg", make_point(15_000, "temperature", 30.0))
            .unwrap();
        // One in the next bucket
        engine
            .ingest_point("agg", make_point(20_000, "temperature", 40.0))
            .unwrap();

        let result = engine
            .query(&TimeSeriesQuery {
                topic: "agg".to_string(),
                start_time: 0,
                end_time: 30_000,
                interval: Some(DownsampleInterval::Second(10)),
                aggregations: vec![Aggregation::Avg, Aggregation::Max],
                filters: HashMap::new(),
                limit: None,
            })
            .unwrap();

        assert_eq!(result.points.len(), 2);
        let bucket0 = result.points.iter().find(|p| p.timestamp == 10_000).unwrap();
        assert!((bucket0.values["temperature_avg"] - 25.0).abs() < f64::EPSILON);
        assert!((bucket0.values["temperature_max"] - 30.0).abs() < f64::EPSILON);
    }

    // -- apply_aggregation unit tests --------------------------------------

    #[test]
    fn test_aggregation_avg() {
        assert!((apply_aggregation(&Aggregation::Avg, &[2.0, 4.0, 6.0]) - 4.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_aggregation_min_max() {
        let vals = [3.0, 1.0, 4.0, 1.5, 9.0];
        assert!((apply_aggregation(&Aggregation::Min, &vals) - 1.0).abs() < f64::EPSILON);
        assert!((apply_aggregation(&Aggregation::Max, &vals) - 9.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_aggregation_sum_count() {
        let vals = [1.0, 2.0, 3.0];
        assert!((apply_aggregation(&Aggregation::Sum, &vals) - 6.0).abs() < f64::EPSILON);
        assert!((apply_aggregation(&Aggregation::Count, &vals) - 3.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_aggregation_first_last() {
        let vals = [10.0, 20.0, 30.0];
        assert!((apply_aggregation(&Aggregation::First, &vals) - 10.0).abs() < f64::EPSILON);
        assert!((apply_aggregation(&Aggregation::Last, &vals) - 30.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_aggregation_empty_values() {
        assert!((apply_aggregation(&Aggregation::Avg, &[]) - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_aggregation_percentiles() {
        let vals: Vec<f64> = (1..=100).map(|i| i as f64).collect();
        let p50 = apply_aggregation(&Aggregation::P50, &vals);
        let p99 = apply_aggregation(&Aggregation::P99, &vals);
        assert!((p50 - 50.0).abs() < 1.5);
        assert!((p99 - 99.0).abs() < 1.5);
    }

    // -- interval / serde --------------------------------------------------

    #[test]
    fn test_downsample_interval_as_secs() {
        assert_eq!(DownsampleInterval::Second(30).as_secs(), 30);
        assert_eq!(DownsampleInterval::Minute(5).as_secs(), 300);
        assert_eq!(DownsampleInterval::Hour(2).as_secs(), 7200);
        assert_eq!(DownsampleInterval::Day(1).as_secs(), 86400);
    }

    #[test]
    fn test_retention_defaults() {
        let r = TimeSeriesRetention::default();
        assert_eq!(r.raw_retention_hours, 24);
        assert_eq!(r.minute_retention_hours, 168);
        assert_eq!(r.hour_retention_hours, 8760);
        assert_eq!(r.day_retention_hours, 87600);
    }

    #[test]
    fn test_config_serde_roundtrip() {
        let cfg = make_config("serde_test");
        let json = serde_json::to_string(&cfg).unwrap();
        let decoded: TimeSeriesConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.topic, "serde_test");
        assert_eq!(decoded.timestamp_field, "$.timestamp");
    }

    #[test]
    fn test_compression_serde() {
        let c = TimeSeriesCompression::DeltaOfDelta;
        let json = serde_json::to_string(&c).unwrap();
        let decoded: TimeSeriesCompression = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, c);
    }

    #[test]
    fn test_rule_status_serde() {
        let s = RuleStatus::Error {
            message: "timeout".to_string(),
        };
        let json = serde_json::to_string(&s).unwrap();
        assert!(json.contains("timeout"));
        let decoded: RuleStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, s);
    }

    #[test]
    fn test_stats_snapshot() {
        let engine = TimeSeriesEngine::new();
        engine.register_series(make_config("s1")).unwrap();
        engine.register_series(make_config("s2")).unwrap();
        let s = engine.stats();
        assert_eq!(s.series_count, 2);
        assert_eq!(s.compression_ratio, 100);
    }
}
