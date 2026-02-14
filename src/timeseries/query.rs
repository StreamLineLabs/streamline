//! Time-Series Query Engine
//!
//! Provides query capabilities for time-series data including:
//! - Time range queries
//! - Tag filtering
//! - Aggregations (sum, avg, min, max, count)
//! - Downsampling

use super::DataPoint;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Time range for queries
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct TimeRange {
    /// Start timestamp (inclusive)
    pub start: i64,
    /// End timestamp (exclusive)
    pub end: i64,
}

impl TimeRange {
    /// Create a new time range
    pub fn new(start: i64, end: i64) -> Self {
        Self { start, end }
    }

    /// Create a range for the last N milliseconds
    pub fn last(duration_ms: i64) -> Self {
        let now = chrono::Utc::now().timestamp_millis();
        Self {
            start: now - duration_ms,
            end: now,
        }
    }

    /// Create a range for the last N minutes
    pub fn last_minutes(minutes: i64) -> Self {
        Self::last(minutes * 60 * 1000)
    }

    /// Create a range for the last N hours
    pub fn last_hours(hours: i64) -> Self {
        Self::last(hours * 60 * 60 * 1000)
    }

    /// Create a range for the last N days
    pub fn last_days(days: i64) -> Self {
        Self::last(days * 24 * 60 * 60 * 1000)
    }

    /// Check if a timestamp is within the range
    pub fn contains(&self, timestamp: i64) -> bool {
        timestamp >= self.start && timestamp < self.end
    }

    /// Duration in milliseconds
    pub fn duration(&self) -> i64 {
        self.end - self.start
    }
}

/// Aggregation functions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Aggregation {
    /// Sum of values
    Sum,
    /// Average of values
    Avg,
    /// Minimum value
    Min,
    /// Maximum value
    Max,
    /// Count of values
    Count,
    /// First value
    First,
    /// Last value
    Last,
    /// Standard deviation
    StdDev,
    /// Percentile (requires additional parameter)
    Percentile(u8),
}

impl Aggregation {
    /// Apply aggregation to a slice of values
    pub fn apply(&self, values: &[f64]) -> Option<f64> {
        if values.is_empty() {
            return None;
        }

        match self {
            Aggregation::Sum => Some(values.iter().sum()),
            Aggregation::Avg => {
                let sum: f64 = values.iter().sum();
                Some(sum / values.len() as f64)
            }
            Aggregation::Min => values.iter().cloned().reduce(f64::min),
            Aggregation::Max => values.iter().cloned().reduce(f64::max),
            Aggregation::Count => Some(values.len() as f64),
            Aggregation::First => values.first().copied(),
            Aggregation::Last => values.last().copied(),
            Aggregation::StdDev => {
                let avg: f64 = values.iter().sum::<f64>() / values.len() as f64;
                let variance: f64 =
                    values.iter().map(|v| (v - avg).powi(2)).sum::<f64>() / values.len() as f64;
                Some(variance.sqrt())
            }
            Aggregation::Percentile(p) => {
                let mut sorted = values.to_vec();
                sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let idx = ((*p as f64 / 100.0) * (sorted.len() - 1) as f64) as usize;
                sorted.get(idx).copied()
            }
        }
    }
}

/// Query filter
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueryFilter {
    /// Match exact tag value
    TagEquals { key: String, value: String },
    /// Match tag with regex pattern
    TagMatches { key: String, pattern: String },
    /// Tag exists
    TagExists { key: String },
    /// Metric name equals
    MetricEquals(String),
    /// Metric name matches pattern
    MetricMatches(String),
    /// Value greater than
    ValueGt(f64),
    /// Value less than
    ValueLt(f64),
    /// Value between (inclusive)
    ValueBetween(f64, f64),
    /// AND combination of filters
    And(Vec<QueryFilter>),
    /// OR combination of filters
    Or(Vec<QueryFilter>),
    /// Negation
    Not(Box<QueryFilter>),
}

impl QueryFilter {
    /// Check if a data point matches the filter
    pub fn matches(&self, point: &DataPoint) -> bool {
        match self {
            QueryFilter::TagEquals { key, value } => point.tag(key) == Some(value.as_str()),
            QueryFilter::TagMatches { key, pattern } => {
                if let Some(tag_value) = point.tag(key) {
                    regex::Regex::new(pattern)
                        .map(|re| re.is_match(tag_value))
                        .unwrap_or(false)
                } else {
                    false
                }
            }
            QueryFilter::TagExists { key } => point.tags.contains_key(key),
            QueryFilter::MetricEquals(name) => point.metric == *name,
            QueryFilter::MetricMatches(pattern) => regex::Regex::new(pattern)
                .map(|re| re.is_match(&point.metric))
                .unwrap_or(false),
            QueryFilter::ValueGt(threshold) => point.value > *threshold,
            QueryFilter::ValueLt(threshold) => point.value < *threshold,
            QueryFilter::ValueBetween(low, high) => point.value >= *low && point.value <= *high,
            QueryFilter::And(filters) => filters.iter().all(|f| f.matches(point)),
            QueryFilter::Or(filters) => filters.iter().any(|f| f.matches(point)),
            QueryFilter::Not(filter) => !filter.matches(point),
        }
    }

    /// Create an equals filter
    pub fn tag_eq(key: &str, value: &str) -> Self {
        QueryFilter::TagEquals {
            key: key.to_string(),
            value: value.to_string(),
        }
    }

    /// Create a metric filter
    pub fn metric(name: &str) -> Self {
        QueryFilter::MetricEquals(name.to_string())
    }
}

/// Time-series query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeSeriesQuery {
    /// Metric name pattern
    pub metric: String,
    /// Time range
    pub time_range: TimeRange,
    /// Optional filters
    pub filters: Vec<QueryFilter>,
    /// Aggregation (optional)
    pub aggregation: Option<Aggregation>,
    /// Group by tags
    pub group_by: Vec<String>,
    /// Downsample interval in milliseconds (0 = no downsampling)
    pub downsample_ms: i64,
    /// Maximum number of results
    pub limit: Option<usize>,
    /// Offset for pagination
    pub offset: usize,
}

impl TimeSeriesQuery {
    /// Create a new query
    pub fn new(metric: &str, time_range: TimeRange) -> Self {
        Self {
            metric: metric.to_string(),
            time_range,
            filters: Vec::new(),
            aggregation: None,
            group_by: Vec::new(),
            downsample_ms: 0,
            limit: None,
            offset: 0,
        }
    }

    /// Add a filter
    pub fn filter(mut self, filter: QueryFilter) -> Self {
        self.filters.push(filter);
        self
    }

    /// Set aggregation
    pub fn aggregate(mut self, agg: Aggregation) -> Self {
        self.aggregation = Some(agg);
        self
    }

    /// Group by tags
    pub fn group_by_tag(mut self, tag: &str) -> Self {
        self.group_by.push(tag.to_string());
        self
    }

    /// Set downsampling interval
    pub fn downsample(mut self, interval_ms: i64) -> Self {
        self.downsample_ms = interval_ms;
        self
    }

    /// Set result limit
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Set offset
    pub fn offset(mut self, offset: usize) -> Self {
        self.offset = offset;
        self
    }
}

/// Query builder
pub struct QueryBuilder {
    query: TimeSeriesQuery,
}

impl QueryBuilder {
    /// Create a new builder
    pub fn new(metric: &str) -> Self {
        Self {
            query: TimeSeriesQuery::new(metric, TimeRange::last_hours(1)),
        }
    }

    /// Set time range
    pub fn time_range(mut self, range: TimeRange) -> Self {
        self.query.time_range = range;
        self
    }

    /// Query last N minutes
    pub fn last_minutes(mut self, minutes: i64) -> Self {
        self.query.time_range = TimeRange::last_minutes(minutes);
        self
    }

    /// Query last N hours
    pub fn last_hours(mut self, hours: i64) -> Self {
        self.query.time_range = TimeRange::last_hours(hours);
        self
    }

    /// Query last N days
    pub fn last_days(mut self, days: i64) -> Self {
        self.query.time_range = TimeRange::last_days(days);
        self
    }

    /// Add tag filter
    pub fn where_tag(mut self, key: &str, value: &str) -> Self {
        self.query.filters.push(QueryFilter::tag_eq(key, value));
        self
    }

    /// Add value filter
    pub fn where_value_gt(mut self, threshold: f64) -> Self {
        self.query.filters.push(QueryFilter::ValueGt(threshold));
        self
    }

    /// Set aggregation
    pub fn aggregate(mut self, agg: Aggregation) -> Self {
        self.query.aggregation = Some(agg);
        self
    }

    /// Sum aggregation
    pub fn sum(self) -> Self {
        self.aggregate(Aggregation::Sum)
    }

    /// Average aggregation
    pub fn avg(self) -> Self {
        self.aggregate(Aggregation::Avg)
    }

    /// Min aggregation
    pub fn min(self) -> Self {
        self.aggregate(Aggregation::Min)
    }

    /// Max aggregation
    pub fn max(self) -> Self {
        self.aggregate(Aggregation::Max)
    }

    /// Count aggregation
    pub fn count(self) -> Self {
        self.aggregate(Aggregation::Count)
    }

    /// Group by tag
    pub fn group_by(mut self, tag: &str) -> Self {
        self.query.group_by.push(tag.to_string());
        self
    }

    /// Downsample
    pub fn downsample(mut self, interval_ms: i64) -> Self {
        self.query.downsample_ms = interval_ms;
        self
    }

    /// Downsample by minutes
    pub fn downsample_minutes(self, minutes: i64) -> Self {
        self.downsample(minutes * 60 * 1000)
    }

    /// Set limit
    pub fn limit(mut self, limit: usize) -> Self {
        self.query.limit = Some(limit);
        self
    }

    /// Build the query
    pub fn build(self) -> TimeSeriesQuery {
        self.query
    }
}

/// Query result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    /// Query that produced this result
    pub query: TimeSeriesQuery,
    /// Result points
    pub points: Vec<DataPoint>,
    /// Aggregated value (if aggregation was applied)
    pub aggregated_value: Option<f64>,
    /// Grouped results (if group_by was used)
    pub groups: HashMap<String, Vec<DataPoint>>,
    /// Total matching points (before limit)
    pub total_count: usize,
    /// Query execution time in microseconds
    pub execution_time_us: u64,
}

impl QueryResult {
    /// Create a new result
    pub fn new(query: TimeSeriesQuery) -> Self {
        Self {
            query,
            points: Vec::new(),
            aggregated_value: None,
            groups: HashMap::new(),
            total_count: 0,
            execution_time_us: 0,
        }
    }

    /// Check if result is empty
    pub fn is_empty(&self) -> bool {
        self.points.is_empty() && self.groups.is_empty()
    }

    /// Get result count
    pub fn len(&self) -> usize {
        self.points.len()
    }
}

/// Execute a query against a set of data points
pub fn execute_query(query: &TimeSeriesQuery, data: &[DataPoint]) -> QueryResult {
    let start = std::time::Instant::now();
    let mut result = QueryResult::new(query.clone());

    // Filter points
    let mut filtered: Vec<DataPoint> = data
        .iter()
        .filter(|p| {
            // Time range filter
            if !query.time_range.contains(p.timestamp) {
                return false;
            }

            // Metric filter
            if !p.metric.contains(&query.metric) && query.metric != "*" {
                return false;
            }

            // Additional filters
            for filter in &query.filters {
                if !filter.matches(p) {
                    return false;
                }
            }

            true
        })
        .cloned()
        .collect();

    result.total_count = filtered.len();

    // Sort by timestamp
    filtered.sort_by_key(|p| p.timestamp);

    // Downsample if requested
    if query.downsample_ms > 0 {
        filtered = downsample_points(&filtered, query.downsample_ms, query.aggregation);
    }

    // Group by tags
    if !query.group_by.is_empty() {
        for point in &filtered {
            let group_key = query
                .group_by
                .iter()
                .filter_map(|tag| point.tag(tag))
                .collect::<Vec<_>>()
                .join("|");

            result
                .groups
                .entry(group_key)
                .or_default()
                .push(point.clone());
        }

        // Apply aggregation to each group
        if let Some(agg) = query.aggregation {
            for points in result.groups.values_mut() {
                let values: Vec<f64> = points.iter().map(|p| p.value).collect();
                if let Some(agg_value) = agg.apply(&values) {
                    // Replace points with single aggregated point
                    if let Some(first) = points.first() {
                        *points = vec![DataPoint::new(&first.metric, first.timestamp, agg_value)];
                    }
                }
            }
        }
    } else {
        // Apply aggregation to all points
        if let Some(agg) = query.aggregation {
            let values: Vec<f64> = filtered.iter().map(|p| p.value).collect();
            result.aggregated_value = agg.apply(&values);
        }

        // Apply offset and limit
        let offset = query.offset;
        let points = if offset > 0 {
            filtered.into_iter().skip(offset).collect()
        } else {
            filtered
        };

        result.points = if let Some(limit) = query.limit {
            points.into_iter().take(limit).collect()
        } else {
            points
        };
    }

    result.execution_time_us = start.elapsed().as_micros() as u64;
    result
}

/// Downsample points to a specified interval
fn downsample_points(
    points: &[DataPoint],
    interval_ms: i64,
    aggregation: Option<Aggregation>,
) -> Vec<DataPoint> {
    if points.is_empty() || interval_ms <= 0 {
        return points.to_vec();
    }

    let agg = aggregation.unwrap_or(Aggregation::Avg);
    let mut buckets: HashMap<i64, Vec<DataPoint>> = HashMap::new();

    // Group into buckets
    for point in points {
        let bucket = (point.timestamp / interval_ms) * interval_ms;
        buckets.entry(bucket).or_default().push(point.clone());
    }

    // Aggregate each bucket
    let mut result: Vec<DataPoint> = buckets
        .into_iter()
        .map(|(timestamp, bucket_points)| {
            let values: Vec<f64> = bucket_points.iter().map(|p| p.value).collect();
            let agg_value = agg.apply(&values).unwrap_or(0.0);

            let mut point = DataPoint::new(&bucket_points[0].metric, timestamp, agg_value);
            point.tags = bucket_points[0].tags.clone();
            point
        })
        .collect();

    result.sort_by_key(|p| p.timestamp);
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_data() -> Vec<DataPoint> {
        vec![
            DataPoint::new("cpu.usage", 1000, 45.5).with_tag("host", "server1"),
            DataPoint::new("cpu.usage", 2000, 50.0).with_tag("host", "server1"),
            DataPoint::new("cpu.usage", 3000, 55.5).with_tag("host", "server1"),
            DataPoint::new("cpu.usage", 1000, 30.0).with_tag("host", "server2"),
            DataPoint::new("cpu.usage", 2000, 35.0).with_tag("host", "server2"),
            DataPoint::new("memory.usage", 1000, 70.0).with_tag("host", "server1"),
        ]
    }

    #[test]
    fn test_time_range() {
        let range = TimeRange::new(1000, 3000);

        assert!(range.contains(1000));
        assert!(range.contains(2500));
        assert!(!range.contains(3000));
        assert!(!range.contains(500));
    }

    #[test]
    fn test_aggregation_functions() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];

        assert_eq!(Aggregation::Sum.apply(&values), Some(15.0));
        assert_eq!(Aggregation::Avg.apply(&values), Some(3.0));
        assert_eq!(Aggregation::Min.apply(&values), Some(1.0));
        assert_eq!(Aggregation::Max.apply(&values), Some(5.0));
        assert_eq!(Aggregation::Count.apply(&values), Some(5.0));
        assert_eq!(Aggregation::First.apply(&values), Some(1.0));
        assert_eq!(Aggregation::Last.apply(&values), Some(5.0));
    }

    #[test]
    fn test_query_filter() {
        let point = DataPoint::new("cpu.usage", 1000, 50.0)
            .with_tag("host", "server1")
            .with_tag("region", "us-west");

        assert!(QueryFilter::tag_eq("host", "server1").matches(&point));
        assert!(!QueryFilter::tag_eq("host", "server2").matches(&point));
        assert!(QueryFilter::metric("cpu.usage").matches(&point));
        assert!(QueryFilter::ValueGt(40.0).matches(&point));
        assert!(!QueryFilter::ValueGt(60.0).matches(&point));
    }

    #[test]
    fn test_query_builder() {
        let query = QueryBuilder::new("cpu.usage")
            .last_hours(1)
            .where_tag("host", "server1")
            .avg()
            .group_by("host")
            .downsample_minutes(5)
            .limit(100)
            .build();

        assert_eq!(query.metric, "cpu.usage");
        assert_eq!(query.aggregation, Some(Aggregation::Avg));
        assert_eq!(query.group_by, vec!["host"]);
        assert_eq!(query.downsample_ms, 5 * 60 * 1000);
        assert_eq!(query.limit, Some(100));
    }

    #[test]
    fn test_execute_query() {
        let data = create_test_data();

        let query = TimeSeriesQuery::new("cpu.usage", TimeRange::new(0, 5000));
        let result = execute_query(&query, &data);

        assert_eq!(result.points.len(), 5); // All cpu.usage points
    }

    #[test]
    fn test_execute_query_with_aggregation() {
        let data = create_test_data();

        let query =
            TimeSeriesQuery::new("cpu.usage", TimeRange::new(0, 5000)).aggregate(Aggregation::Avg);
        let result = execute_query(&query, &data);

        assert!(result.aggregated_value.is_some());
    }

    #[test]
    fn test_execute_query_with_filter() {
        let data = create_test_data();

        let query = TimeSeriesQuery::new("cpu.usage", TimeRange::new(0, 5000))
            .filter(QueryFilter::tag_eq("host", "server1"));
        let result = execute_query(&query, &data);

        assert_eq!(result.points.len(), 3);
    }

    #[test]
    fn test_downsample() {
        let data = vec![
            DataPoint::new("test", 1000, 10.0),
            DataPoint::new("test", 1500, 20.0),
            DataPoint::new("test", 2000, 30.0),
            DataPoint::new("test", 2500, 40.0),
        ];

        let downsampled = downsample_points(&data, 2000, Some(Aggregation::Avg));

        assert_eq!(downsampled.len(), 2);
    }
}
