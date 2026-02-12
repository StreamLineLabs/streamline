//! Aggregation Operations
//!
//! Provides aggregation functions for stream processing.

use super::OperatorError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Aggregation function type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum AggregationFunction {
    /// Count elements
    #[default]
    Count,
    /// Sum values
    Sum,
    /// Average values
    Avg,
    /// Minimum value
    Min,
    /// Maximum value
    Max,
    /// First value
    First,
    /// Last value
    Last,
    /// Distinct count
    CountDistinct,
    /// Standard deviation
    StdDev,
    /// Variance
    Variance,
    /// Percentile (stores p-value in state)
    Percentile,
    /// Custom (user-defined)
    Custom,
}

/// Aggregation trait
pub trait Aggregation<T, R>: Send + Sync {
    /// Initialize aggregation state
    fn init(&self) -> AggregationState;

    /// Accumulate a value
    fn accumulate(&self, state: &mut AggregationState, value: T) -> Result<(), OperatorError>;

    /// Get current result
    fn result(&self, state: &AggregationState) -> Result<R, OperatorError>;

    /// Merge two states (for parallel aggregation)
    fn merge(
        &self,
        state1: &AggregationState,
        state2: &AggregationState,
    ) -> Result<AggregationState, OperatorError>;

    /// Reset state
    fn reset(&self, state: &mut AggregationState);
}

/// Aggregation state
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AggregationState {
    /// Count of values
    pub count: u64,
    /// Sum of values
    pub sum: f64,
    /// Minimum value
    pub min: Option<f64>,
    /// Maximum value
    pub max: Option<f64>,
    /// First value
    pub first: Option<f64>,
    /// Last value
    pub last: Option<f64>,
    /// Sum of squares (for variance/stddev)
    pub sum_squares: f64,
    /// Distinct values (for count distinct)
    pub distinct: Option<Vec<String>>,
    /// Sorted values (for percentile)
    pub sorted_values: Option<Vec<f64>>,
    /// Custom state (JSON)
    pub custom: Option<String>,
}

impl AggregationState {
    /// Create a new aggregation state
    pub fn new() -> Self {
        Self::default()
    }

    /// Get mean value
    pub fn mean(&self) -> Option<f64> {
        if self.count > 0 {
            Some(self.sum / self.count as f64)
        } else {
            None
        }
    }

    /// Get variance
    pub fn variance(&self) -> Option<f64> {
        if self.count > 1 {
            let mean = self.sum / self.count as f64;
            Some((self.sum_squares / self.count as f64) - (mean * mean))
        } else {
            None
        }
    }

    /// Get standard deviation
    pub fn std_dev(&self) -> Option<f64> {
        self.variance().map(|v| v.sqrt())
    }

    /// Get percentile (requires sorted_values)
    pub fn percentile(&self, p: f64) -> Option<f64> {
        self.sorted_values.as_ref().and_then(|values| {
            if values.is_empty() || !(0.0..=100.0).contains(&p) {
                return None;
            }
            let index = ((p / 100.0) * (values.len() - 1) as f64).round() as usize;
            values.get(index).copied()
        })
    }
}

/// Aggregation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregationResult {
    /// Result value
    pub value: f64,
    /// Count of inputs
    pub count: u64,
    /// Aggregation function used
    pub function: AggregationFunction,
    /// Timestamp
    pub timestamp: i64,
    /// Key (for keyed aggregations)
    pub key: Option<String>,
}

impl AggregationResult {
    /// Create a new result
    pub fn new(value: f64, count: u64, function: AggregationFunction) -> Self {
        Self {
            value,
            count,
            function,
            timestamp: chrono::Utc::now().timestamp_millis(),
            key: None,
        }
    }

    /// Set key
    pub fn with_key(mut self, key: impl Into<String>) -> Self {
        self.key = Some(key.into());
        self
    }
}

/// Count aggregation
#[derive(Debug, Clone, Default)]
pub struct CountAggregation;

impl<T> Aggregation<T, u64> for CountAggregation {
    fn init(&self) -> AggregationState {
        AggregationState::new()
    }

    fn accumulate(&self, state: &mut AggregationState, _value: T) -> Result<(), OperatorError> {
        state.count += 1;
        Ok(())
    }

    fn result(&self, state: &AggregationState) -> Result<u64, OperatorError> {
        Ok(state.count)
    }

    fn merge(
        &self,
        state1: &AggregationState,
        state2: &AggregationState,
    ) -> Result<AggregationState, OperatorError> {
        Ok(AggregationState {
            count: state1.count + state2.count,
            ..Default::default()
        })
    }

    fn reset(&self, state: &mut AggregationState) {
        state.count = 0;
    }
}

/// Sum aggregation
#[derive(Debug, Clone, Default)]
pub struct SumAggregation;

impl Aggregation<f64, f64> for SumAggregation {
    fn init(&self) -> AggregationState {
        AggregationState::new()
    }

    fn accumulate(&self, state: &mut AggregationState, value: f64) -> Result<(), OperatorError> {
        state.sum += value;
        state.count += 1;
        Ok(())
    }

    fn result(&self, state: &AggregationState) -> Result<f64, OperatorError> {
        Ok(state.sum)
    }

    fn merge(
        &self,
        state1: &AggregationState,
        state2: &AggregationState,
    ) -> Result<AggregationState, OperatorError> {
        Ok(AggregationState {
            sum: state1.sum + state2.sum,
            count: state1.count + state2.count,
            ..Default::default()
        })
    }

    fn reset(&self, state: &mut AggregationState) {
        state.sum = 0.0;
        state.count = 0;
    }
}

/// Average aggregation
#[derive(Debug, Clone, Default)]
pub struct AvgAggregation;

impl Aggregation<f64, f64> for AvgAggregation {
    fn init(&self) -> AggregationState {
        AggregationState::new()
    }

    fn accumulate(&self, state: &mut AggregationState, value: f64) -> Result<(), OperatorError> {
        state.sum += value;
        state.count += 1;
        Ok(())
    }

    fn result(&self, state: &AggregationState) -> Result<f64, OperatorError> {
        state
            .mean()
            .ok_or_else(|| OperatorError::AggregationError("No values to average".to_string()))
    }

    fn merge(
        &self,
        state1: &AggregationState,
        state2: &AggregationState,
    ) -> Result<AggregationState, OperatorError> {
        Ok(AggregationState {
            sum: state1.sum + state2.sum,
            count: state1.count + state2.count,
            ..Default::default()
        })
    }

    fn reset(&self, state: &mut AggregationState) {
        state.sum = 0.0;
        state.count = 0;
    }
}

/// Min aggregation
#[derive(Debug, Clone, Default)]
pub struct MinAggregation;

impl Aggregation<f64, f64> for MinAggregation {
    fn init(&self) -> AggregationState {
        AggregationState::new()
    }

    fn accumulate(&self, state: &mut AggregationState, value: f64) -> Result<(), OperatorError> {
        state.min = Some(state.min.map_or(value, |m| m.min(value)));
        state.count += 1;
        Ok(())
    }

    fn result(&self, state: &AggregationState) -> Result<f64, OperatorError> {
        state
            .min
            .ok_or_else(|| OperatorError::AggregationError("No values for min".to_string()))
    }

    fn merge(
        &self,
        state1: &AggregationState,
        state2: &AggregationState,
    ) -> Result<AggregationState, OperatorError> {
        let min = match (state1.min, state2.min) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };
        Ok(AggregationState {
            min,
            count: state1.count + state2.count,
            ..Default::default()
        })
    }

    fn reset(&self, state: &mut AggregationState) {
        state.min = None;
        state.count = 0;
    }
}

/// Max aggregation
#[derive(Debug, Clone, Default)]
pub struct MaxAggregation;

impl Aggregation<f64, f64> for MaxAggregation {
    fn init(&self) -> AggregationState {
        AggregationState::new()
    }

    fn accumulate(&self, state: &mut AggregationState, value: f64) -> Result<(), OperatorError> {
        state.max = Some(state.max.map_or(value, |m| m.max(value)));
        state.count += 1;
        Ok(())
    }

    fn result(&self, state: &AggregationState) -> Result<f64, OperatorError> {
        state
            .max
            .ok_or_else(|| OperatorError::AggregationError("No values for max".to_string()))
    }

    fn merge(
        &self,
        state1: &AggregationState,
        state2: &AggregationState,
    ) -> Result<AggregationState, OperatorError> {
        let max = match (state1.max, state2.max) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };
        Ok(AggregationState {
            max,
            count: state1.count + state2.count,
            ..Default::default()
        })
    }

    fn reset(&self, state: &mut AggregationState) {
        state.max = None;
        state.count = 0;
    }
}

/// Count distinct aggregation
#[derive(Debug, Clone, Default)]
pub struct CountDistinctAggregation;

impl Aggregation<String, u64> for CountDistinctAggregation {
    fn init(&self) -> AggregationState {
        AggregationState {
            distinct: Some(Vec::new()),
            ..Default::default()
        }
    }

    fn accumulate(&self, state: &mut AggregationState, value: String) -> Result<(), OperatorError> {
        if let Some(ref mut distinct) = state.distinct {
            if !distinct.contains(&value) {
                distinct.push(value);
            }
        }
        state.count += 1;
        Ok(())
    }

    fn result(&self, state: &AggregationState) -> Result<u64, OperatorError> {
        Ok(state.distinct.as_ref().map(|d| d.len() as u64).unwrap_or(0))
    }

    fn merge(
        &self,
        state1: &AggregationState,
        state2: &AggregationState,
    ) -> Result<AggregationState, OperatorError> {
        let mut distinct = state1.distinct.clone().unwrap_or_default();
        if let Some(ref d2) = state2.distinct {
            for v in d2 {
                if !distinct.contains(v) {
                    distinct.push(v.clone());
                }
            }
        }
        Ok(AggregationState {
            distinct: Some(distinct),
            count: state1.count + state2.count,
            ..Default::default()
        })
    }

    fn reset(&self, state: &mut AggregationState) {
        state.distinct = Some(Vec::new());
        state.count = 0;
    }
}

/// Standard deviation aggregation
#[derive(Debug, Clone, Default)]
pub struct StdDevAggregation;

impl Aggregation<f64, f64> for StdDevAggregation {
    fn init(&self) -> AggregationState {
        AggregationState::new()
    }

    fn accumulate(&self, state: &mut AggregationState, value: f64) -> Result<(), OperatorError> {
        state.sum += value;
        state.sum_squares += value * value;
        state.count += 1;
        Ok(())
    }

    fn result(&self, state: &AggregationState) -> Result<f64, OperatorError> {
        state.std_dev().ok_or_else(|| {
            OperatorError::AggregationError("Not enough values for stddev".to_string())
        })
    }

    fn merge(
        &self,
        state1: &AggregationState,
        state2: &AggregationState,
    ) -> Result<AggregationState, OperatorError> {
        Ok(AggregationState {
            sum: state1.sum + state2.sum,
            sum_squares: state1.sum_squares + state2.sum_squares,
            count: state1.count + state2.count,
            ..Default::default()
        })
    }

    fn reset(&self, state: &mut AggregationState) {
        state.sum = 0.0;
        state.sum_squares = 0.0;
        state.count = 0;
    }
}

/// Multi-aggregation (run multiple aggregations at once)
pub struct MultiAggregation {
    /// Aggregation states by name
    states: HashMap<String, AggregationState>,
    /// Functions by name
    functions: HashMap<String, AggregationFunction>,
}

impl MultiAggregation {
    /// Create a new multi-aggregation
    pub fn new() -> Self {
        Self {
            states: HashMap::new(),
            functions: HashMap::new(),
        }
    }

    /// Add aggregation
    pub fn add(&mut self, name: impl Into<String>, function: AggregationFunction) {
        let name = name.into();
        self.functions.insert(name.clone(), function);
        self.states.insert(name, AggregationState::new());
    }

    /// Accumulate a value
    pub fn accumulate(&mut self, value: f64) -> Result<(), OperatorError> {
        for (name, function) in &self.functions {
            if let Some(state) = self.states.get_mut(name) {
                match function {
                    AggregationFunction::Count => {
                        state.count += 1;
                    }
                    AggregationFunction::Sum => {
                        state.sum += value;
                        state.count += 1;
                    }
                    AggregationFunction::Avg => {
                        state.sum += value;
                        state.count += 1;
                    }
                    AggregationFunction::Min => {
                        state.min = Some(state.min.map_or(value, |m| m.min(value)));
                        state.count += 1;
                    }
                    AggregationFunction::Max => {
                        state.max = Some(state.max.map_or(value, |m| m.max(value)));
                        state.count += 1;
                    }
                    AggregationFunction::First => {
                        if state.first.is_none() {
                            state.first = Some(value);
                        }
                        state.count += 1;
                    }
                    AggregationFunction::Last => {
                        state.last = Some(value);
                        state.count += 1;
                    }
                    AggregationFunction::StdDev | AggregationFunction::Variance => {
                        state.sum += value;
                        state.sum_squares += value * value;
                        state.count += 1;
                    }
                    AggregationFunction::Percentile => {
                        if state.sorted_values.is_none() {
                            state.sorted_values = Some(Vec::new());
                        }
                        if let Some(ref mut values) = state.sorted_values {
                            values.push(value);
                            values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                        }
                        state.count += 1;
                    }
                    _ => {
                        state.count += 1;
                    }
                }
            }
        }
        Ok(())
    }

    /// Get result for a specific aggregation
    pub fn result(&self, name: &str) -> Option<f64> {
        let function = self.functions.get(name)?;
        let state = self.states.get(name)?;

        match function {
            AggregationFunction::Count => Some(state.count as f64),
            AggregationFunction::Sum => Some(state.sum),
            AggregationFunction::Avg => state.mean(),
            AggregationFunction::Min => state.min,
            AggregationFunction::Max => state.max,
            AggregationFunction::First => state.first,
            AggregationFunction::Last => state.last,
            AggregationFunction::StdDev => state.std_dev(),
            AggregationFunction::Variance => state.variance(),
            AggregationFunction::Percentile => state.percentile(95.0), // Default to p95
            _ => None,
        }
    }

    /// Get all results
    pub fn results(&self) -> HashMap<String, f64> {
        let mut results = HashMap::new();
        for name in self.functions.keys() {
            if let Some(value) = self.result(name) {
                results.insert(name.clone(), value);
            }
        }
        results
    }

    /// Reset all states
    pub fn reset(&mut self) {
        for state in self.states.values_mut() {
            *state = AggregationState::new();
        }
    }
}

impl Default for MultiAggregation {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count_aggregation() {
        let agg = CountAggregation;
        let mut state = <CountAggregation as Aggregation<i32, u64>>::init(&agg);

        <CountAggregation as Aggregation<i32, u64>>::accumulate(&agg, &mut state, 1).unwrap();
        <CountAggregation as Aggregation<i32, u64>>::accumulate(&agg, &mut state, 2).unwrap();
        <CountAggregation as Aggregation<i32, u64>>::accumulate(&agg, &mut state, 3).unwrap();

        assert_eq!(
            <CountAggregation as Aggregation<i32, u64>>::result(&agg, &state).unwrap(),
            3
        );
    }

    #[test]
    fn test_sum_aggregation() {
        let agg = SumAggregation;
        let mut state = agg.init();

        agg.accumulate(&mut state, 1.0).unwrap();
        agg.accumulate(&mut state, 2.0).unwrap();
        agg.accumulate(&mut state, 3.0).unwrap();

        assert_eq!(agg.result(&state).unwrap(), 6.0);
    }

    #[test]
    fn test_avg_aggregation() {
        let agg = AvgAggregation;
        let mut state = agg.init();

        agg.accumulate(&mut state, 2.0).unwrap();
        agg.accumulate(&mut state, 4.0).unwrap();
        agg.accumulate(&mut state, 6.0).unwrap();

        assert_eq!(agg.result(&state).unwrap(), 4.0);
    }

    #[test]
    fn test_min_aggregation() {
        let agg = MinAggregation;
        let mut state = agg.init();

        agg.accumulate(&mut state, 5.0).unwrap();
        agg.accumulate(&mut state, 2.0).unwrap();
        agg.accumulate(&mut state, 8.0).unwrap();

        assert_eq!(agg.result(&state).unwrap(), 2.0);
    }

    #[test]
    fn test_max_aggregation() {
        let agg = MaxAggregation;
        let mut state = agg.init();

        agg.accumulate(&mut state, 5.0).unwrap();
        agg.accumulate(&mut state, 2.0).unwrap();
        agg.accumulate(&mut state, 8.0).unwrap();

        assert_eq!(agg.result(&state).unwrap(), 8.0);
    }

    #[test]
    fn test_count_distinct_aggregation() {
        let agg = CountDistinctAggregation;
        let mut state = agg.init();

        agg.accumulate(&mut state, "a".to_string()).unwrap();
        agg.accumulate(&mut state, "b".to_string()).unwrap();
        agg.accumulate(&mut state, "a".to_string()).unwrap();
        agg.accumulate(&mut state, "c".to_string()).unwrap();

        assert_eq!(agg.result(&state).unwrap(), 3);
    }

    #[test]
    fn test_aggregation_state() {
        let mut state = AggregationState::new();
        state.sum = 15.0;
        state.count = 3;
        state.sum_squares = 77.0; // 1 + 25 + 49

        assert_eq!(state.mean(), Some(5.0));
        assert!(state.variance().is_some());
        assert!(state.std_dev().is_some());
    }

    #[test]
    fn test_aggregation_state_percentile() {
        let mut state = AggregationState::new();
        state.sorted_values = Some(vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]);

        assert_eq!(state.percentile(0.0), Some(1.0));
        // 50th percentile: 50/100 * (10-1) = 4.5, rounded = 5 -> index 5 = 6.0
        assert_eq!(state.percentile(50.0), Some(6.0));
        assert_eq!(state.percentile(100.0), Some(10.0));
    }

    #[test]
    fn test_multi_aggregation() {
        let mut multi = MultiAggregation::new();
        multi.add("count", AggregationFunction::Count);
        multi.add("sum", AggregationFunction::Sum);
        multi.add("avg", AggregationFunction::Avg);
        multi.add("min", AggregationFunction::Min);
        multi.add("max", AggregationFunction::Max);

        multi.accumulate(1.0).unwrap();
        multi.accumulate(2.0).unwrap();
        multi.accumulate(3.0).unwrap();

        let results = multi.results();
        assert_eq!(results.get("count"), Some(&3.0));
        assert_eq!(results.get("sum"), Some(&6.0));
        assert_eq!(results.get("avg"), Some(&2.0));
        assert_eq!(results.get("min"), Some(&1.0));
        assert_eq!(results.get("max"), Some(&3.0));
    }

    #[test]
    fn test_aggregation_merge() {
        let agg = SumAggregation;
        let mut state1 = agg.init();
        let mut state2 = agg.init();

        agg.accumulate(&mut state1, 1.0).unwrap();
        agg.accumulate(&mut state1, 2.0).unwrap();
        agg.accumulate(&mut state2, 3.0).unwrap();
        agg.accumulate(&mut state2, 4.0).unwrap();

        let merged = agg.merge(&state1, &state2).unwrap();
        assert_eq!(agg.result(&merged).unwrap(), 10.0);
    }

    #[test]
    fn test_aggregation_reset() {
        let agg = CountAggregation;
        let mut state = <CountAggregation as Aggregation<i32, u64>>::init(&agg);

        <CountAggregation as Aggregation<i32, u64>>::accumulate(&agg, &mut state, 1).unwrap();
        <CountAggregation as Aggregation<i32, u64>>::accumulate(&agg, &mut state, 2).unwrap();
        assert_eq!(
            <CountAggregation as Aggregation<i32, u64>>::result(&agg, &state).unwrap(),
            2
        );

        <CountAggregation as Aggregation<i32, u64>>::reset(&agg, &mut state);
        assert_eq!(
            <CountAggregation as Aggregation<i32, u64>>::result(&agg, &state).unwrap(),
            0
        );
    }

    #[test]
    fn test_aggregation_function_default() {
        let func = AggregationFunction::default();
        assert_eq!(func, AggregationFunction::Count);
    }

    #[test]
    fn test_aggregation_result() {
        let result = AggregationResult::new(42.0, 10, AggregationFunction::Sum).with_key("test");

        assert_eq!(result.value, 42.0);
        assert_eq!(result.count, 10);
        assert_eq!(result.function, AggregationFunction::Sum);
        assert_eq!(result.key, Some("test".to_string()));
    }
}
