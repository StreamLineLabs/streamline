//! Feature Transformations
//!
//! Provides transformation functions for feature engineering including:
//! - Windowed aggregations (tumbling, sliding, session)
//! - Mathematical operations (log, sqrt, normalize)
//! - String operations (hash, tokenize)
//! - Time-based features (hour_of_day, day_of_week, is_weekend)

use crate::featurestore::feature::FeatureValue;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Transformation engine for applying feature transformations
pub struct TransformationEngine {
    // Registered UDFs could go here in a full implementation
}

impl TransformationEngine {
    /// Create a new transformation engine
    pub fn new() -> Self {
        Self {}
    }

    /// Apply a windowed aggregation over a set of values
    pub fn aggregate(
        &self,
        values: &[f64],
        aggregation: WindowedAggregation,
    ) -> f64 {
        match aggregation {
            WindowedAggregation::Sum => values.iter().sum(),
            WindowedAggregation::Avg => {
                if values.is_empty() {
                    0.0
                } else {
                    values.iter().sum::<f64>() / values.len() as f64
                }
            }
            WindowedAggregation::Count => values.len() as f64,
            WindowedAggregation::Min => values
                .iter()
                .cloned()
                .fold(f64::INFINITY, f64::min),
            WindowedAggregation::Max => values
                .iter()
                .cloned()
                .fold(f64::NEG_INFINITY, f64::max),
            WindowedAggregation::StdDev => {
                if values.len() < 2 {
                    return 0.0;
                }
                let mean = values.iter().sum::<f64>() / values.len() as f64;
                let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>()
                    / (values.len() - 1) as f64;
                variance.sqrt()
            }
            WindowedAggregation::Variance => {
                if values.len() < 2 {
                    return 0.0;
                }
                let mean = values.iter().sum::<f64>() / values.len() as f64;
                values.iter().map(|v| (v - mean).powi(2)).sum::<f64>()
                    / (values.len() - 1) as f64
            }
            WindowedAggregation::First => values.first().copied().unwrap_or(0.0),
            WindowedAggregation::Last => values.last().copied().unwrap_or(0.0),
        }
    }

    /// Apply tumbling window aggregation
    ///
    /// Groups timestamped values into non-overlapping windows of `window_size_ms`
    /// and applies the aggregation to each window.
    pub fn tumbling_window_aggregate(
        &self,
        timestamped_values: &[(i64, f64)],
        window_size_ms: i64,
        aggregation: WindowedAggregation,
    ) -> Vec<WindowResult> {
        if timestamped_values.is_empty() || window_size_ms <= 0 {
            return Vec::new();
        }

        let min_ts = timestamped_values.iter().map(|(ts, _)| *ts).min().unwrap_or(0);
        let max_ts = timestamped_values.iter().map(|(ts, _)| *ts).max().unwrap_or(0);

        let mut results = Vec::new();
        let mut window_start = min_ts - (min_ts % window_size_ms);

        while window_start <= max_ts {
            let window_end = window_start + window_size_ms;

            let window_values: Vec<f64> = timestamped_values
                .iter()
                .filter(|(ts, _)| *ts >= window_start && *ts < window_end)
                .map(|(_, v)| *v)
                .collect();

            if !window_values.is_empty() {
                results.push(WindowResult {
                    window_start,
                    window_end,
                    value: self.aggregate(&window_values, aggregation),
                    count: window_values.len(),
                });
            }

            window_start = window_end;
        }

        results
    }

    /// Apply sliding window aggregation
    ///
    /// Groups timestamped values into overlapping windows of `window_size_ms`
    /// with a slide interval of `slide_ms`.
    pub fn sliding_window_aggregate(
        &self,
        timestamped_values: &[(i64, f64)],
        window_size_ms: i64,
        slide_ms: i64,
        aggregation: WindowedAggregation,
    ) -> Vec<WindowResult> {
        if timestamped_values.is_empty() || window_size_ms <= 0 || slide_ms <= 0 {
            return Vec::new();
        }

        let min_ts = timestamped_values.iter().map(|(ts, _)| *ts).min().unwrap_or(0);
        let max_ts = timestamped_values.iter().map(|(ts, _)| *ts).max().unwrap_or(0);

        let mut results = Vec::new();
        let mut window_start = min_ts - (min_ts % slide_ms);

        while window_start <= max_ts {
            let window_end = window_start + window_size_ms;

            let window_values: Vec<f64> = timestamped_values
                .iter()
                .filter(|(ts, _)| *ts >= window_start && *ts < window_end)
                .map(|(_, v)| *v)
                .collect();

            if !window_values.is_empty() {
                results.push(WindowResult {
                    window_start,
                    window_end,
                    value: self.aggregate(&window_values, aggregation),
                    count: window_values.len(),
                });
            }

            window_start += slide_ms;
        }

        results
    }

    /// Apply session window aggregation
    ///
    /// Groups values by activity sessions. A new session starts when there is
    /// a gap larger than `gap_ms` between consecutive events.
    pub fn session_window_aggregate(
        &self,
        timestamped_values: &[(i64, f64)],
        gap_ms: i64,
        aggregation: WindowedAggregation,
    ) -> Vec<WindowResult> {
        if timestamped_values.is_empty() || gap_ms <= 0 {
            return Vec::new();
        }

        let mut sorted: Vec<(i64, f64)> = timestamped_values.to_vec();
        sorted.sort_by_key(|(ts, _)| *ts);

        let mut results = Vec::new();
        let mut session_start = sorted[0].0;
        let mut session_values = vec![sorted[0].1];
        let mut last_ts = sorted[0].0;

        for &(ts, value) in &sorted[1..] {
            if ts - last_ts > gap_ms {
                // New session
                results.push(WindowResult {
                    window_start: session_start,
                    window_end: last_ts,
                    value: self.aggregate(&session_values, aggregation),
                    count: session_values.len(),
                });
                session_start = ts;
                session_values = vec![value];
            } else {
                session_values.push(value);
            }
            last_ts = ts;
        }

        // Close the last session
        results.push(WindowResult {
            window_start: session_start,
            window_end: last_ts,
            value: self.aggregate(&session_values, aggregation),
            count: session_values.len(),
        });

        results
    }

    // ── Mathematical Operations ──

    /// Natural logarithm (ln)
    pub fn log(value: f64) -> FeatureValue {
        if value > 0.0 {
            FeatureValue::Float64(value.ln())
        } else {
            FeatureValue::Null
        }
    }

    /// Log base 10
    pub fn log10(value: f64) -> FeatureValue {
        if value > 0.0 {
            FeatureValue::Float64(value.log10())
        } else {
            FeatureValue::Null
        }
    }

    /// Square root
    pub fn sqrt(value: f64) -> FeatureValue {
        if value >= 0.0 {
            FeatureValue::Float64(value.sqrt())
        } else {
            FeatureValue::Null
        }
    }

    /// Z-score normalization: (value - mean) / std_dev
    pub fn normalize(value: f64, mean: f64, std_dev: f64) -> FeatureValue {
        if std_dev == 0.0 {
            FeatureValue::Float64(0.0)
        } else {
            FeatureValue::Float64((value - mean) / std_dev)
        }
    }

    /// Min-max normalization: (value - min) / (max - min)
    pub fn min_max_normalize(value: f64, min: f64, max: f64) -> FeatureValue {
        if (max - min).abs() < f64::EPSILON {
            FeatureValue::Float64(0.0)
        } else {
            FeatureValue::Float64((value - min) / (max - min))
        }
    }

    /// Absolute value
    pub fn abs(value: f64) -> FeatureValue {
        FeatureValue::Float64(value.abs())
    }

    /// Power function
    pub fn pow(value: f64, exponent: f64) -> FeatureValue {
        FeatureValue::Float64(value.powf(exponent))
    }

    // ── String Operations ──

    /// Simple hash of a string (for feature hashing / one-hot alternatives)
    pub fn hash_string(value: &str) -> FeatureValue {
        let hash = value
            .bytes()
            .fold(0u64, |acc, b| acc.wrapping_mul(31).wrapping_add(b as u64));
        FeatureValue::Int64(hash as i64)
    }

    /// Tokenize a string into word count
    pub fn tokenize_count(value: &str) -> FeatureValue {
        FeatureValue::Int64(value.split_whitespace().count() as i64)
    }

    /// String length
    pub fn string_length(value: &str) -> FeatureValue {
        FeatureValue::Int64(value.len() as i64)
    }

    // ── Time-Based Features ──

    /// Extract hour of day (0-23) from a timestamp (millis)
    pub fn hour_of_day(timestamp_ms: i64) -> FeatureValue {
        use chrono::{TimeZone, Timelike, Utc};
        let dt = Utc.timestamp_millis_opt(timestamp_ms);
        match dt.single() {
            Some(dt) => FeatureValue::Int64(dt.hour() as i64),
            None => FeatureValue::Null,
        }
    }

    /// Extract day of week (0=Monday, 6=Sunday) from a timestamp (millis)
    pub fn day_of_week(timestamp_ms: i64) -> FeatureValue {
        use chrono::{Datelike, TimeZone, Utc};
        let dt = Utc.timestamp_millis_opt(timestamp_ms);
        match dt.single() {
            Some(dt) => FeatureValue::Int64(dt.weekday().num_days_from_monday() as i64),
            None => FeatureValue::Null,
        }
    }

    /// Check if timestamp falls on a weekend
    pub fn is_weekend(timestamp_ms: i64) -> FeatureValue {
        use chrono::{Datelike, TimeZone, Utc};
        let dt = Utc.timestamp_millis_opt(timestamp_ms);
        match dt.single() {
            Some(dt) => {
                let dow = dt.weekday().num_days_from_monday();
                FeatureValue::Bool(dow >= 5) // Saturday=5, Sunday=6
            }
            None => FeatureValue::Null,
        }
    }

    /// Extract month (1-12)
    pub fn month(timestamp_ms: i64) -> FeatureValue {
        use chrono::{Datelike, TimeZone, Utc};
        let dt = Utc.timestamp_millis_opt(timestamp_ms);
        match dt.single() {
            Some(dt) => FeatureValue::Int64(dt.month() as i64),
            None => FeatureValue::Null,
        }
    }

    /// Check if timestamp falls within business hours (9-17 UTC)
    pub fn is_business_hours(timestamp_ms: i64) -> FeatureValue {
        use chrono::{TimeZone, Timelike, Utc};
        let dt = Utc.timestamp_millis_opt(timestamp_ms);
        match dt.single() {
            Some(dt) => {
                let hour = dt.hour();
                FeatureValue::Bool((9..17).contains(&hour))
            }
            None => FeatureValue::Null,
        }
    }
}

/// Windowed aggregation type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WindowedAggregation {
    /// Sum of values
    Sum,
    /// Average
    Avg,
    /// Count
    Count,
    /// Minimum
    Min,
    /// Maximum
    Max,
    /// Standard deviation (sample)
    StdDev,
    /// Variance (sample)
    Variance,
    /// First value in window
    First,
    /// Last value in window
    Last,
}

/// Result of a windowed aggregation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowResult {
    /// Window start timestamp (millis)
    pub window_start: i64,
    /// Window end timestamp (millis)
    pub window_end: i64,
    /// Aggregated value
    pub value: f64,
    /// Number of elements in the window
    pub count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn engine() -> TransformationEngine {
        TransformationEngine::new()
    }

    // ── Aggregation Tests ──

    #[test]
    fn test_sum() {
        let e = engine();
        assert_eq!(e.aggregate(&[1.0, 2.0, 3.0], WindowedAggregation::Sum), 6.0);
    }

    #[test]
    fn test_avg() {
        let e = engine();
        assert_eq!(e.aggregate(&[2.0, 4.0, 6.0], WindowedAggregation::Avg), 4.0);
    }

    #[test]
    fn test_avg_empty() {
        let e = engine();
        assert_eq!(e.aggregate(&[], WindowedAggregation::Avg), 0.0);
    }

    #[test]
    fn test_count() {
        let e = engine();
        assert_eq!(
            e.aggregate(&[1.0, 2.0, 3.0, 4.0], WindowedAggregation::Count),
            4.0
        );
    }

    #[test]
    fn test_min() {
        let e = engine();
        assert_eq!(
            e.aggregate(&[3.0, 1.0, 4.0, 1.5], WindowedAggregation::Min),
            1.0
        );
    }

    #[test]
    fn test_max() {
        let e = engine();
        assert_eq!(
            e.aggregate(&[3.0, 1.0, 4.0, 1.5], WindowedAggregation::Max),
            4.0
        );
    }

    #[test]
    fn test_first_last() {
        let e = engine();
        assert_eq!(
            e.aggregate(&[10.0, 20.0, 30.0], WindowedAggregation::First),
            10.0
        );
        assert_eq!(
            e.aggregate(&[10.0, 20.0, 30.0], WindowedAggregation::Last),
            30.0
        );
    }

    #[test]
    fn test_std_dev() {
        let e = engine();
        let values = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
        let result = e.aggregate(&values, WindowedAggregation::StdDev);
        // Expected sample std dev ~ 2.138
        assert!((result - 2.138).abs() < 0.01);
    }

    // ── Tumbling Window Tests ──

    #[test]
    fn test_tumbling_window() {
        let e = engine();
        let values = vec![
            (1000, 1.0),
            (1500, 2.0),
            (2000, 3.0),
            (2500, 4.0),
            (3000, 5.0),
        ];

        let results = e.tumbling_window_aggregate(&values, 1000, WindowedAggregation::Sum);

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].value, 3.0); // 1000-1999: 1+2
        assert_eq!(results[1].value, 7.0); // 2000-2999: 3+4
        assert_eq!(results[2].value, 5.0); // 3000-3999: 5
    }

    #[test]
    fn test_tumbling_window_avg() {
        let e = engine();
        let values = vec![(0, 10.0), (500, 20.0), (1000, 30.0), (1500, 40.0)];

        let results = e.tumbling_window_aggregate(&values, 1000, WindowedAggregation::Avg);

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].value, 15.0); // avg(10, 20)
        assert_eq!(results[1].value, 35.0); // avg(30, 40)
    }

    // ── Sliding Window Tests ──

    #[test]
    fn test_sliding_window() {
        let e = engine();
        let values = vec![
            (0, 1.0),
            (500, 2.0),
            (1000, 3.0),
            (1500, 4.0),
            (2000, 5.0),
        ];

        let results =
            e.sliding_window_aggregate(&values, 1000, 500, WindowedAggregation::Sum);

        // Windows: [0,1000)=[1,2], [500,1500)=[2,3], [1000,2000)=[3,4], [1500,2500)=[4,5], [2000,3000)=[5]
        assert!(results.len() >= 3);
    }

    // ── Session Window Tests ──

    #[test]
    fn test_session_window() {
        let e = engine();
        // Two sessions: (100, 200, 300) and (1000, 1100)
        let values = vec![
            (100, 1.0),
            (200, 2.0),
            (300, 3.0),
            (1000, 4.0),
            (1100, 5.0),
        ];

        let results =
            e.session_window_aggregate(&values, 500, WindowedAggregation::Sum);

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].value, 6.0); // 1+2+3
        assert_eq!(results[0].count, 3);
        assert_eq!(results[1].value, 9.0); // 4+5
        assert_eq!(results[1].count, 2);
    }

    // ── Mathematical Operation Tests ──

    #[test]
    fn test_log() {
        match TransformationEngine::log(std::f64::consts::E) {
            FeatureValue::Float64(v) => assert!((v - 1.0).abs() < 0.001),
            _ => panic!("Expected Float64"),
        }
        assert_eq!(TransformationEngine::log(-1.0), FeatureValue::Null);
    }

    #[test]
    fn test_sqrt() {
        match TransformationEngine::sqrt(9.0) {
            FeatureValue::Float64(v) => assert!((v - 3.0).abs() < 0.001),
            _ => panic!("Expected Float64"),
        }
        assert_eq!(TransformationEngine::sqrt(-1.0), FeatureValue::Null);
    }

    #[test]
    fn test_normalize() {
        match TransformationEngine::normalize(10.0, 5.0, 2.5) {
            FeatureValue::Float64(v) => assert!((v - 2.0).abs() < 0.001),
            _ => panic!("Expected Float64"),
        }
    }

    #[test]
    fn test_min_max_normalize() {
        match TransformationEngine::min_max_normalize(5.0, 0.0, 10.0) {
            FeatureValue::Float64(v) => assert!((v - 0.5).abs() < 0.001),
            _ => panic!("Expected Float64"),
        }
    }

    // ── String Operation Tests ──

    #[test]
    fn test_hash_string() {
        let h1 = TransformationEngine::hash_string("hello");
        let h2 = TransformationEngine::hash_string("world");
        let h3 = TransformationEngine::hash_string("hello");

        assert_ne!(h1, h2);
        assert_eq!(h1, h3); // Same input produces same hash
    }

    #[test]
    fn test_tokenize_count() {
        assert_eq!(
            TransformationEngine::tokenize_count("hello world foo"),
            FeatureValue::Int64(3)
        );
        assert_eq!(
            TransformationEngine::tokenize_count(""),
            FeatureValue::Int64(0)
        );
    }

    #[test]
    fn test_string_length() {
        assert_eq!(
            TransformationEngine::string_length("hello"),
            FeatureValue::Int64(5)
        );
    }

    // ── Time-Based Feature Tests ──

    #[test]
    fn test_hour_of_day() {
        // 2024-01-15 14:30:00 UTC -> hour = 14
        let ts = 1705325400000i64;
        match TransformationEngine::hour_of_day(ts) {
            FeatureValue::Int64(h) => assert_eq!(h, 14),
            other => panic!("Expected Int64, got {:?}", other),
        }
    }

    #[test]
    fn test_day_of_week() {
        // 2024-01-15 is Monday -> 0
        let ts = 1705325400000i64;
        match TransformationEngine::day_of_week(ts) {
            FeatureValue::Int64(d) => assert_eq!(d, 0),
            other => panic!("Expected Int64, got {:?}", other),
        }
    }

    #[test]
    fn test_is_weekend() {
        // Monday
        let monday_ts = 1705325400000i64;
        assert_eq!(
            TransformationEngine::is_weekend(monday_ts),
            FeatureValue::Bool(false)
        );

        // Saturday (2024-01-20)
        let saturday_ts = 1705757400000i64;
        assert_eq!(
            TransformationEngine::is_weekend(saturday_ts),
            FeatureValue::Bool(true)
        );
    }

    #[test]
    fn test_is_business_hours() {
        // 14:30 UTC -> business hours
        let ts = 1705325400000i64;
        assert_eq!(
            TransformationEngine::is_business_hours(ts),
            FeatureValue::Bool(true)
        );
    }

    #[test]
    fn test_empty_window_inputs() {
        let e = engine();
        assert!(e
            .tumbling_window_aggregate(&[], 1000, WindowedAggregation::Sum)
            .is_empty());
        assert!(e
            .sliding_window_aggregate(&[], 1000, 500, WindowedAggregation::Sum)
            .is_empty());
        assert!(e
            .session_window_aggregate(&[], 500, WindowedAggregation::Sum)
            .is_empty());
    }
}
