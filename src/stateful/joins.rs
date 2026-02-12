//! Stream Joins
//!
//! Provides join operations for combining streams.

use super::{OperatorError, StreamElement};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};

/// Join type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum JoinType {
    /// Inner join (both sides must match)
    #[default]
    Inner,
    /// Left join (all left, matching right)
    Left,
    /// Right join (all right, matching left)
    Right,
    /// Full outer join (all from both)
    FullOuter,
    /// Left semi join (left rows with matches)
    LeftSemi,
    /// Left anti join (left rows without matches)
    LeftAnti,
}

/// Join condition
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub enum JoinCondition {
    /// Key equality
    #[default]
    KeyEqual,
    /// Field equality
    FieldEqual {
        left_field: String,
        right_field: String,
    },
    /// Custom predicate (stored as expression string)
    Custom { expression: String },
    /// Time-based join window
    Temporal { window_ms: i64 },
}

/// Join result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinResult<L, R> {
    /// Left value (None for right-only in outer join)
    pub left: Option<L>,
    /// Right value (None for left-only in outer join)
    pub right: Option<R>,
    /// Join key
    pub key: String,
    /// Timestamp
    pub timestamp: i64,
    /// Is late join
    pub is_late: bool,
}

impl<L, R> JoinResult<L, R> {
    /// Create inner join result
    pub fn inner(left: L, right: R, key: impl Into<String>, timestamp: i64) -> Self {
        Self {
            left: Some(left),
            right: Some(right),
            key: key.into(),
            timestamp,
            is_late: false,
        }
    }

    /// Create left-only result
    pub fn left_only(left: L, key: impl Into<String>, timestamp: i64) -> Self {
        Self {
            left: Some(left),
            right: None,
            key: key.into(),
            timestamp,
            is_late: false,
        }
    }

    /// Create right-only result
    pub fn right_only(right: R, key: impl Into<String>, timestamp: i64) -> Self {
        Self {
            left: None,
            right: Some(right),
            key: key.into(),
            timestamp,
            is_late: false,
        }
    }
}

/// Stream join operator
pub struct StreamJoin<L: Clone, R: Clone> {
    /// Join type
    join_type: JoinType,
    /// Join condition
    condition: JoinCondition,
    /// Left side buffer
    left_buffer: HashMap<String, VecDeque<BufferedElement<L>>>,
    /// Right side buffer
    right_buffer: HashMap<String, VecDeque<BufferedElement<R>>>,
    /// Retention time (ms) for buffered elements
    retention_ms: i64,
    /// Current watermark
    watermark: i64,
    /// Statistics
    stats: JoinStats,
}

/// Buffered element
#[derive(Debug, Clone)]
struct BufferedElement<T> {
    /// Value
    value: T,
    /// Timestamp
    timestamp: i64,
    /// Has been matched
    matched: bool,
}

/// Join statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct JoinStats {
    /// Left elements received
    pub left_received: u64,
    /// Right elements received
    pub right_received: u64,
    /// Matches produced
    pub matches: u64,
    /// Left buffer size
    pub left_buffer_size: usize,
    /// Right buffer size
    pub right_buffer_size: usize,
    /// Evicted elements
    pub evicted: u64,
}

impl<L: Clone, R: Clone> StreamJoin<L, R> {
    /// Create a new stream join
    pub fn new(join_type: JoinType, condition: JoinCondition, retention_ms: i64) -> Self {
        Self {
            join_type,
            condition,
            left_buffer: HashMap::new(),
            right_buffer: HashMap::new(),
            retention_ms,
            watermark: 0,
            stats: JoinStats::default(),
        }
    }

    /// Create inner join
    pub fn inner(retention_ms: i64) -> Self {
        Self::new(JoinType::Inner, JoinCondition::KeyEqual, retention_ms)
    }

    /// Create left join
    pub fn left(retention_ms: i64) -> Self {
        Self::new(JoinType::Left, JoinCondition::KeyEqual, retention_ms)
    }

    /// Create right join
    pub fn right(retention_ms: i64) -> Self {
        Self::new(JoinType::Right, JoinCondition::KeyEqual, retention_ms)
    }

    /// Create full outer join
    pub fn full_outer(retention_ms: i64) -> Self {
        Self::new(JoinType::FullOuter, JoinCondition::KeyEqual, retention_ms)
    }

    /// Add left element
    pub fn add_left(
        &mut self,
        element: StreamElement<L>,
    ) -> Result<Vec<JoinResult<L, R>>, OperatorError> {
        let key = element.key.clone().unwrap_or_default();
        self.stats.left_received += 1;

        let buffered = BufferedElement {
            value: element.value.clone(),
            timestamp: element.timestamp,
            matched: false,
        };

        // Find matches in right buffer
        let mut results = Vec::new();
        if let Some(right_elements) = self.right_buffer.get_mut(&key) {
            for right_elem in right_elements.iter_mut() {
                if Self::check_condition(&self.condition, element.timestamp, right_elem.timestamp) {
                    results.push(JoinResult::inner(
                        element.value.clone(),
                        right_elem.value.clone(),
                        &key,
                        element.timestamp.max(right_elem.timestamp),
                    ));
                    right_elem.matched = true;
                    self.stats.matches += 1;
                }
            }
        }

        // Add to buffer
        self.left_buffer.entry(key).or_default().push_back(buffered);

        self.update_buffer_stats();
        Ok(results)
    }

    /// Add right element
    pub fn add_right(
        &mut self,
        element: StreamElement<R>,
    ) -> Result<Vec<JoinResult<L, R>>, OperatorError> {
        let key = element.key.clone().unwrap_or_default();
        self.stats.right_received += 1;

        let buffered = BufferedElement {
            value: element.value.clone(),
            timestamp: element.timestamp,
            matched: false,
        };

        // Find matches in left buffer
        let mut results = Vec::new();
        if let Some(left_elements) = self.left_buffer.get_mut(&key) {
            for left_elem in left_elements.iter_mut() {
                if Self::check_condition(&self.condition, left_elem.timestamp, element.timestamp) {
                    results.push(JoinResult::inner(
                        left_elem.value.clone(),
                        element.value.clone(),
                        &key,
                        element.timestamp.max(left_elem.timestamp),
                    ));
                    left_elem.matched = true;
                    self.stats.matches += 1;
                }
            }
        }

        // Add to buffer
        self.right_buffer
            .entry(key)
            .or_default()
            .push_back(buffered);

        self.update_buffer_stats();
        Ok(results)
    }

    /// Check if elements match based on condition (using cloned condition to avoid borrow conflicts)
    fn check_condition(condition: &JoinCondition, left_ts: i64, right_ts: i64) -> bool {
        match condition {
            JoinCondition::KeyEqual => true, // Key already matched
            JoinCondition::Temporal { window_ms } => (left_ts - right_ts).abs() <= *window_ms,
            JoinCondition::FieldEqual { .. } => true, // Would need field extraction
            JoinCondition::Custom { .. } => true,     // Would need expression evaluation
        }
    }

    /// Advance watermark and emit unmatched elements for outer joins
    pub fn advance_watermark(
        &mut self,
        watermark: i64,
    ) -> Result<Vec<JoinResult<L, R>>, OperatorError> {
        self.watermark = watermark;
        let mut results = Vec::new();

        let eviction_threshold = watermark - self.retention_ms;

        // Process expired elements
        for (key, left_elements) in &mut self.left_buffer {
            while let Some(front) = left_elements.front() {
                if front.timestamp > eviction_threshold {
                    break;
                }

                let Some(elem) = left_elements.pop_front() else {
                    break;
                };
                self.stats.evicted += 1;

                // Emit unmatched for left/full outer
                if !elem.matched
                    && matches!(
                        self.join_type,
                        JoinType::Left | JoinType::FullOuter | JoinType::LeftAnti
                    )
                {
                    if self.join_type == JoinType::LeftAnti {
                        // Anti join - emit because there was no match
                        results.push(JoinResult::left_only(
                            elem.value,
                            key.clone(),
                            elem.timestamp,
                        ));
                    } else {
                        results.push(JoinResult::left_only(
                            elem.value,
                            key.clone(),
                            elem.timestamp,
                        ));
                    }
                }
            }
        }

        for (key, right_elements) in &mut self.right_buffer {
            while let Some(front) = right_elements.front() {
                if front.timestamp > eviction_threshold {
                    break;
                }

                let Some(elem) = right_elements.pop_front() else {
                    break;
                };
                self.stats.evicted += 1;

                // Emit unmatched for right/full outer
                if !elem.matched && matches!(self.join_type, JoinType::Right | JoinType::FullOuter)
                {
                    results.push(JoinResult::right_only(
                        elem.value,
                        key.clone(),
                        elem.timestamp,
                    ));
                }
            }
        }

        // Clean up empty keys
        self.left_buffer.retain(|_, v| !v.is_empty());
        self.right_buffer.retain(|_, v| !v.is_empty());

        self.update_buffer_stats();
        Ok(results)
    }

    /// Update buffer size stats
    fn update_buffer_stats(&mut self) {
        self.stats.left_buffer_size = self.left_buffer.values().map(|v| v.len()).sum();
        self.stats.right_buffer_size = self.right_buffer.values().map(|v| v.len()).sum();
    }

    /// Get statistics
    pub fn stats(&self) -> &JoinStats {
        &self.stats
    }

    /// Clear buffers
    pub fn clear(&mut self) {
        self.left_buffer.clear();
        self.right_buffer.clear();
        self.update_buffer_stats();
    }
}

/// Temporal join (time-windowed join)
pub struct TemporalJoin<L: Clone, R: Clone> {
    /// Inner stream join
    inner: StreamJoin<L, R>,
    /// Time window (ms)
    window_ms: i64,
}

impl<L: Clone, R: Clone> TemporalJoin<L, R> {
    /// Create a new temporal join
    pub fn new(join_type: JoinType, window_ms: i64) -> Self {
        let condition = JoinCondition::Temporal { window_ms };
        Self {
            inner: StreamJoin::new(join_type, condition, window_ms * 2),
            window_ms,
        }
    }

    /// Add left element
    pub fn add_left(
        &mut self,
        element: StreamElement<L>,
    ) -> Result<Vec<JoinResult<L, R>>, OperatorError> {
        self.inner.add_left(element)
    }

    /// Add right element
    pub fn add_right(
        &mut self,
        element: StreamElement<R>,
    ) -> Result<Vec<JoinResult<L, R>>, OperatorError> {
        self.inner.add_right(element)
    }

    /// Advance watermark
    pub fn advance_watermark(
        &mut self,
        watermark: i64,
    ) -> Result<Vec<JoinResult<L, R>>, OperatorError> {
        self.inner.advance_watermark(watermark)
    }

    /// Get statistics
    pub fn stats(&self) -> &JoinStats {
        self.inner.stats()
    }

    /// Get window size
    pub fn window_ms(&self) -> i64 {
        self.window_ms
    }
}

/// Interval join (elements within time intervals)
pub struct IntervalJoin<L: Clone, R: Clone> {
    /// Inner stream join
    inner: StreamJoin<L, R>,
    /// Lower bound offset (left.time - right.time >= lower_bound)
    lower_bound_ms: i64,
    /// Upper bound offset (left.time - right.time <= upper_bound)
    upper_bound_ms: i64,
}

impl<L: Clone, R: Clone> IntervalJoin<L, R> {
    /// Create a new interval join
    pub fn new(join_type: JoinType, lower_bound_ms: i64, upper_bound_ms: i64) -> Self {
        let retention = (upper_bound_ms - lower_bound_ms).abs() * 2;
        Self {
            inner: StreamJoin::new(join_type, JoinCondition::KeyEqual, retention),
            lower_bound_ms,
            upper_bound_ms,
        }
    }

    /// Check if elements are within interval (static to avoid borrow conflicts)
    fn check_interval(
        lower_bound_ms: i64,
        upper_bound_ms: i64,
        left_ts: i64,
        right_ts: i64,
    ) -> bool {
        let diff = left_ts - right_ts;
        diff >= lower_bound_ms && diff <= upper_bound_ms
    }

    /// Add left element
    pub fn add_left(
        &mut self,
        element: StreamElement<L>,
    ) -> Result<Vec<JoinResult<L, R>>, OperatorError> {
        let key = element.key.clone().unwrap_or_default();
        let lower = self.lower_bound_ms;
        let upper = self.upper_bound_ms;

        let buffered = BufferedElement {
            value: element.value.clone(),
            timestamp: element.timestamp,
            matched: false,
        };

        // Find matches in right buffer within interval
        let mut results = Vec::new();
        if let Some(right_elements) = self.inner.right_buffer.get_mut(&key) {
            for right_elem in right_elements.iter_mut() {
                if Self::check_interval(lower, upper, element.timestamp, right_elem.timestamp) {
                    results.push(JoinResult::inner(
                        element.value.clone(),
                        right_elem.value.clone(),
                        &key,
                        element.timestamp.max(right_elem.timestamp),
                    ));
                    right_elem.matched = true;
                }
            }
        }

        self.inner
            .left_buffer
            .entry(key)
            .or_default()
            .push_back(buffered);

        Ok(results)
    }

    /// Add right element
    pub fn add_right(
        &mut self,
        element: StreamElement<R>,
    ) -> Result<Vec<JoinResult<L, R>>, OperatorError> {
        let key = element.key.clone().unwrap_or_default();
        let lower = self.lower_bound_ms;
        let upper = self.upper_bound_ms;

        let buffered = BufferedElement {
            value: element.value.clone(),
            timestamp: element.timestamp,
            matched: false,
        };

        // Find matches in left buffer within interval
        let mut results = Vec::new();
        if let Some(left_elements) = self.inner.left_buffer.get_mut(&key) {
            for left_elem in left_elements.iter_mut() {
                if Self::check_interval(lower, upper, left_elem.timestamp, element.timestamp) {
                    results.push(JoinResult::inner(
                        left_elem.value.clone(),
                        element.value.clone(),
                        &key,
                        element.timestamp.max(left_elem.timestamp),
                    ));
                    left_elem.matched = true;
                }
            }
        }

        self.inner
            .right_buffer
            .entry(key)
            .or_default()
            .push_back(buffered);

        Ok(results)
    }

    /// Advance watermark
    pub fn advance_watermark(
        &mut self,
        watermark: i64,
    ) -> Result<Vec<JoinResult<L, R>>, OperatorError> {
        self.inner.advance_watermark(watermark)
    }

    /// Get statistics
    pub fn stats(&self) -> &JoinStats {
        self.inner.stats()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_join_type_default() {
        let jt = JoinType::default();
        assert_eq!(jt, JoinType::Inner);
    }

    #[test]
    fn test_join_condition_default() {
        let jc = JoinCondition::default();
        assert!(matches!(jc, JoinCondition::KeyEqual));
    }

    #[test]
    fn test_inner_join() {
        let mut join: StreamJoin<i32, i32> = StreamJoin::inner(10000);

        // Add left element
        let left = StreamElement::new(1, 1000).with_key("k1");
        let results = join.add_left(left).unwrap();
        assert!(results.is_empty()); // No match yet

        // Add matching right element
        let right = StreamElement::new(10, 2000).with_key("k1");
        let results = join.add_right(right).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].left, Some(1));
        assert_eq!(results[0].right, Some(10));
    }

    #[test]
    fn test_left_join() {
        let mut join: StreamJoin<i32, i32> = StreamJoin::left(1000);

        // Add left element with no match
        let left = StreamElement::new(1, 1000).with_key("k1");
        join.add_left(left).unwrap();

        // Advance watermark past retention
        let results = join.advance_watermark(3000).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].left, Some(1));
        assert!(results[0].right.is_none());
    }

    #[test]
    fn test_temporal_join() {
        let mut join: TemporalJoin<i32, i32> = TemporalJoin::new(JoinType::Inner, 500);

        // Add elements within window
        join.add_left(StreamElement::new(1, 1000).with_key("k1"))
            .unwrap();
        let results = join
            .add_right(StreamElement::new(10, 1200).with_key("k1"))
            .unwrap();
        assert_eq!(results.len(), 1);

        // Add element outside window
        let results = join
            .add_right(StreamElement::new(20, 2000).with_key("k1"))
            .unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_interval_join() {
        let mut join: IntervalJoin<i32, i32> = IntervalJoin::new(JoinType::Inner, -500, 500);

        // Add elements within interval
        join.add_left(StreamElement::new(1, 1000).with_key("k1"))
            .unwrap();
        let results = join
            .add_right(StreamElement::new(10, 1200).with_key("k1"))
            .unwrap();
        assert_eq!(results.len(), 1);

        // Add element outside interval
        let results = join
            .add_right(StreamElement::new(20, 2000).with_key("k1"))
            .unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_join_result() {
        let result: JoinResult<i32, i32> = JoinResult::inner(1, 2, "key", 1000);
        assert_eq!(result.left, Some(1));
        assert_eq!(result.right, Some(2));
        assert_eq!(result.key, "key");
        assert!(!result.is_late);

        let left_only: JoinResult<i32, i32> = JoinResult::left_only(1, "key", 1000);
        assert_eq!(left_only.left, Some(1));
        assert!(left_only.right.is_none());
    }

    #[test]
    fn test_join_stats() {
        let mut join: StreamJoin<i32, i32> = StreamJoin::inner(10000);

        join.add_left(StreamElement::new(1, 1000).with_key("k1"))
            .unwrap();
        join.add_right(StreamElement::new(10, 2000).with_key("k1"))
            .unwrap();

        let stats = join.stats();
        assert_eq!(stats.left_received, 1);
        assert_eq!(stats.right_received, 1);
        assert_eq!(stats.matches, 1);
    }

    #[test]
    fn test_join_multiple_matches() {
        let mut join: StreamJoin<i32, i32> = StreamJoin::inner(10000);

        // Add multiple left elements
        join.add_left(StreamElement::new(1, 1000).with_key("k1"))
            .unwrap();
        join.add_left(StreamElement::new(2, 1500).with_key("k1"))
            .unwrap();

        // Add right element - should match both
        let results = join
            .add_right(StreamElement::new(10, 2000).with_key("k1"))
            .unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_join_clear() {
        let mut join: StreamJoin<i32, i32> = StreamJoin::inner(10000);

        join.add_left(StreamElement::new(1, 1000).with_key("k1"))
            .unwrap();
        join.add_right(StreamElement::new(10, 2000).with_key("k1"))
            .unwrap();

        join.clear();
        let stats = join.stats();
        assert_eq!(stats.left_buffer_size, 0);
        assert_eq!(stats.right_buffer_size, 0);
    }
}
