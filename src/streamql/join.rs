//! Stream Join Operators for StreamlineQL
//!
//! This module implements various join operators for stream processing:
//!
//! ## Join Types
//!
//! - **Interval Join**: Time-bounded join for stream-stream scenarios
//! - **Temporal Join**: Stream-table join with versioned table lookups
//! - **Hash Join**: In-memory equi-join using hash tables
//! - **Nested Loop Join**: General-purpose join for complex conditions
//!
//! ## Supported Join Semantics
//!
//! - `INNER JOIN`: Emits only matching pairs
//! - `LEFT JOIN`: Emits all left rows, null-padding unmatched
//! - `RIGHT JOIN`: Emits all right rows, null-padding unmatched
//! - `FULL JOIN`: Emits all rows from both sides
//! - `CROSS JOIN`: Cartesian product
//!
//! ## Example
//!
//! ```sql
//! SELECT o.order_id, c.customer_name
//! FROM orders o
//! INNER JOIN customers c
//!   ON o.customer_id = c.id
//! WINDOW INTERVAL '1 hour'
//! ```
//!
//! ## Stream-Stream Join Semantics
//!
//! For stream-stream joins, events are matched within a time window:
//!
//! ```text
//! Stream A:  ──●─────●─────────●───────▶
//! Stream B:  ────●──────●───────────●──▶
//!             [──window──]
//! ```

use super::ast::{Expression, JoinCondition, JoinType};
use super::functions::FunctionRegistry;
use super::operators::{evaluate_expression, StreamOperator};
use super::types::{Column, Row, Schema, Value};
use crate::error::{Result, StreamlineError};
use std::collections::{HashMap, VecDeque};
use std::time::Duration;

/// Configuration for join operations
#[derive(Debug, Clone)]
pub struct JoinConfig {
    /// Join type (INNER, LEFT, RIGHT, FULL, CROSS)
    pub join_type: JoinType,
    /// Join condition
    pub condition: JoinCondition,
    /// Time window for interval joins (optional)
    pub interval: Option<Duration>,
    /// Maximum buffer size per side
    pub max_buffer_size: usize,
    /// Late data tolerance
    pub late_tolerance: Duration,
}

impl Default for JoinConfig {
    fn default() -> Self {
        Self {
            join_type: JoinType::Inner,
            condition: JoinCondition::None,
            interval: Some(Duration::from_secs(3600)), // 1 hour default
            max_buffer_size: 100_000,
            late_tolerance: Duration::from_secs(60),
        }
    }
}

/// Buffered row for join operations
#[derive(Debug, Clone)]
struct BufferedRow {
    /// The row data
    row: Row,
    /// Event timestamp in milliseconds
    event_time: i64,
    /// Whether this row has been matched
    matched: bool,
}

/// Hash join operator for equi-joins
///
/// Uses a hash table to efficiently match rows with equal join keys.
/// Suitable for joins where the condition is `a.x = b.y`.
#[allow(dead_code)]
pub struct HashJoinOperator {
    /// Join configuration
    config: JoinConfig,
    /// Left side schema
    left_schema: Schema,
    /// Right side schema
    right_schema: Schema,
    /// Output schema
    output_schema: Schema,
    /// Left key expression (column index)
    left_key: usize,
    /// Right key expression (column index)
    right_key: usize,
    /// Hash table for left side (key -> rows)
    left_table: HashMap<Value, Vec<BufferedRow>>,
    /// Hash table for right side (key -> rows)
    right_table: HashMap<Value, Vec<BufferedRow>>,
    /// Unmatched left rows (for LEFT/FULL joins)
    unmatched_left: VecDeque<BufferedRow>,
    /// Unmatched right rows (for RIGHT/FULL joins)
    unmatched_right: VecDeque<BufferedRow>,
    /// Function registry
    functions: FunctionRegistry,
    /// Currently processing side
    current_side: JoinSide,
    /// Output buffer
    output_buffer: VecDeque<Row>,
}

/// Which side of the join we're processing
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinSide {
    Left,
    Right,
}

#[allow(dead_code)]
impl HashJoinOperator {
    /// Create a new hash join operator
    pub fn new(
        config: JoinConfig,
        left_schema: Schema,
        right_schema: Schema,
        left_key_col: &str,
        right_key_col: &str,
    ) -> Result<Self> {
        let left_key = left_schema.column_index(left_key_col).ok_or_else(|| {
            StreamlineError::Query(format!("Unknown left key column: {}", left_key_col))
        })?;

        let right_key = right_schema.column_index(right_key_col).ok_or_else(|| {
            StreamlineError::Query(format!("Unknown right key column: {}", right_key_col))
        })?;

        let output_schema = Self::build_output_schema(&left_schema, &right_schema);

        Ok(Self {
            config,
            left_schema,
            right_schema,
            output_schema,
            left_key,
            right_key,
            left_table: HashMap::new(),
            right_table: HashMap::new(),
            unmatched_left: VecDeque::new(),
            unmatched_right: VecDeque::new(),
            functions: FunctionRegistry::new(),
            current_side: JoinSide::Left,
            output_buffer: VecDeque::new(),
        })
    }

    /// Build the output schema by combining left and right
    fn build_output_schema(left: &Schema, right: &Schema) -> Schema {
        let mut columns = Vec::with_capacity(left.len() + right.len());

        // Add left columns
        for col in &left.columns {
            columns.push(Column::new(
                format!("left_{}", col.name),
                col.data_type.clone(),
            ));
        }

        // Add right columns
        for col in &right.columns {
            columns.push(Column::new(
                format!("right_{}", col.name),
                col.data_type.clone(),
            ));
        }

        Schema::new(columns)
    }

    /// Set the side for incoming rows
    pub fn set_side(&mut self, side: JoinSide) {
        self.current_side = side;
    }

    /// Process a left row
    fn process_left(&mut self, row: Row) -> Result<Vec<Row>> {
        let event_time = row.event_time.unwrap_or(0);
        let key = row
            .values
            .get(self.left_key)
            .cloned()
            .unwrap_or(Value::Null);
        let interval_ms = self.config.interval.map(|d| d.as_millis() as i64);

        let buffered = BufferedRow {
            row: row.clone(),
            event_time,
            matched: false,
        };

        let mut results = Vec::new();

        // Check for matches in right table
        if let Some(right_rows) = self.right_table.get_mut(&key) {
            for right in right_rows.iter_mut() {
                let within_interval = match interval_ms {
                    Some(interval) => (event_time - right.event_time).abs() <= interval,
                    None => true,
                };
                if within_interval {
                    right.matched = true;
                    // Combine rows inline to avoid borrow conflict
                    let mut values = Vec::with_capacity(row.values.len() + right.row.values.len());
                    values.extend(row.values.iter().cloned());
                    values.extend(right.row.values.iter().cloned());
                    let combined_time = match (row.event_time, right.row.event_time) {
                        (Some(l), Some(r)) => Some(l.max(r)),
                        (Some(t), None) | (None, Some(t)) => Some(t),
                        (None, None) => None,
                    };
                    results.push(Row {
                        values,
                        event_time: combined_time,
                    });
                }
            }
        }

        // Buffer the row for future matches
        self.left_table.entry(key).or_default().push(buffered);

        // Enforce buffer size limit
        self.evict_old_entries();

        Ok(results)
    }

    /// Process a right row
    fn process_right(&mut self, row: Row) -> Result<Vec<Row>> {
        let event_time = row.event_time.unwrap_or(0);
        let key = row
            .values
            .get(self.right_key)
            .cloned()
            .unwrap_or(Value::Null);
        let interval_ms = self.config.interval.map(|d| d.as_millis() as i64);

        let buffered = BufferedRow {
            row: row.clone(),
            event_time,
            matched: false,
        };

        let mut results = Vec::new();

        // Check for matches in left table
        if let Some(left_rows) = self.left_table.get_mut(&key) {
            for left in left_rows.iter_mut() {
                let within_interval = match interval_ms {
                    Some(interval) => (left.event_time - event_time).abs() <= interval,
                    None => true,
                };
                if within_interval {
                    left.matched = true;
                    // Combine rows inline to avoid borrow conflict
                    let mut values = Vec::with_capacity(left.row.values.len() + row.values.len());
                    values.extend(left.row.values.iter().cloned());
                    values.extend(row.values.iter().cloned());
                    let combined_time = match (left.row.event_time, row.event_time) {
                        (Some(l), Some(r)) => Some(l.max(r)),
                        (Some(t), None) | (None, Some(t)) => Some(t),
                        (None, None) => None,
                    };
                    results.push(Row {
                        values,
                        event_time: combined_time,
                    });
                }
            }
        }

        // Buffer the row for future matches
        self.right_table.entry(key).or_default().push(buffered);

        // Enforce buffer size limit
        self.evict_old_entries();

        Ok(results)
    }

    /// Check if two timestamps are within the join interval
    fn is_within_interval(&self, left_time: i64, right_time: i64) -> bool {
        if let Some(interval) = self.config.interval {
            let diff = (left_time - right_time).abs();
            diff <= interval.as_millis() as i64
        } else {
            true // No interval constraint
        }
    }

    /// Combine a left and right row into an output row
    fn combine_rows(&self, left: &Row, right: &Row) -> Row {
        let mut values = Vec::with_capacity(left.values.len() + right.values.len());
        values.extend(left.values.iter().cloned());
        values.extend(right.values.iter().cloned());

        let event_time = match (left.event_time, right.event_time) {
            (Some(l), Some(r)) => Some(l.max(r)),
            (Some(t), None) | (None, Some(t)) => Some(t),
            (None, None) => None,
        };

        Row { values, event_time }
    }

    /// Create a null-padded row for outer joins
    fn null_pad_left(&self, left: &Row) -> Row {
        let mut values = left.values.clone();
        for _ in 0..self.right_schema.len() {
            values.push(Value::Null);
        }
        Row {
            values,
            event_time: left.event_time,
        }
    }

    /// Create a null-padded row for outer joins
    fn null_pad_right(&self, right: &Row) -> Row {
        let mut values = Vec::with_capacity(self.left_schema.len() + right.values.len());
        for _ in 0..self.left_schema.len() {
            values.push(Value::Null);
        }
        values.extend(right.values.iter().cloned());
        Row {
            values,
            event_time: right.event_time,
        }
    }

    /// Evict old entries from buffers
    fn evict_old_entries(&mut self) {
        // Count total entries
        let left_count: usize = self.left_table.values().map(|v| v.len()).sum();
        let right_count: usize = self.right_table.values().map(|v| v.len()).sum();

        // Evict if over limit (simple strategy: remove oldest)
        if left_count > self.config.max_buffer_size {
            // Find and remove oldest entries
            for rows in self.left_table.values_mut() {
                if rows.len() > 1 {
                    // Move unmatched rows to unmatched buffer for outer joins
                    if matches!(self.config.join_type, JoinType::Left | JoinType::Full) {
                        if let Some(evicted) = rows.first() {
                            if !evicted.matched {
                                self.unmatched_left.push_back(evicted.clone());
                            }
                        }
                    }
                    rows.remove(0);
                }
            }
        }

        if right_count > self.config.max_buffer_size {
            for rows in self.right_table.values_mut() {
                if rows.len() > 1 {
                    if matches!(self.config.join_type, JoinType::Right | JoinType::Full) {
                        if let Some(evicted) = rows.first() {
                            if !evicted.matched {
                                self.unmatched_right.push_back(evicted.clone());
                            }
                        }
                    }
                    rows.remove(0);
                }
            }
        }
    }
}

impl StreamOperator for HashJoinOperator {
    fn process(&mut self, row: Row) -> Result<Option<Row>> {
        let results = match self.current_side {
            JoinSide::Left => self.process_left(row)?,
            JoinSide::Right => self.process_right(row)?,
        };

        // Buffer results and return one at a time
        self.output_buffer.extend(results);
        Ok(self.output_buffer.pop_front())
    }

    fn flush(&mut self) -> Result<Vec<Row>> {
        let mut results: Vec<Row> = self.output_buffer.drain(..).collect();

        // For outer joins, emit unmatched rows
        match self.config.join_type {
            JoinType::Left | JoinType::Full => {
                for rows in self.left_table.values() {
                    for buffered in rows {
                        if !buffered.matched {
                            results.push(self.null_pad_left(&buffered.row));
                        }
                    }
                }
                while let Some(buffered) = self.unmatched_left.pop_front() {
                    results.push(self.null_pad_left(&buffered.row));
                }
            }
            _ => {}
        }

        match self.config.join_type {
            JoinType::Right | JoinType::Full => {
                for rows in self.right_table.values() {
                    for buffered in rows {
                        if !buffered.matched {
                            results.push(self.null_pad_right(&buffered.row));
                        }
                    }
                }
                while let Some(buffered) = self.unmatched_right.pop_front() {
                    results.push(self.null_pad_right(&buffered.row));
                }
            }
            _ => {}
        }

        Ok(results)
    }

    fn output_schema(&self) -> &Schema {
        &self.output_schema
    }
}

/// Interval join operator for stream-stream joins with time bounds
///
/// This operator joins two streams where matching events must occur
/// within a specified time interval of each other.
#[allow(dead_code)]
pub struct IntervalJoinOperator {
    /// Base hash join
    inner: HashJoinOperator,
    /// Lower bound of interval (can be negative)
    lower_bound: i64,
    /// Upper bound of interval
    upper_bound: i64,
}

#[allow(dead_code)]
impl IntervalJoinOperator {
    /// Create a new interval join operator
    ///
    /// The interval is defined as: left.time BETWEEN right.time - lower AND right.time + upper
    pub fn new(
        config: JoinConfig,
        left_schema: Schema,
        right_schema: Schema,
        left_key_col: &str,
        right_key_col: &str,
        lower_bound_ms: i64,
        upper_bound_ms: i64,
    ) -> Result<Self> {
        let inner = HashJoinOperator::new(
            config,
            left_schema,
            right_schema,
            left_key_col,
            right_key_col,
        )?;

        Ok(Self {
            inner,
            lower_bound: lower_bound_ms,
            upper_bound: upper_bound_ms,
        })
    }

    /// Check if timestamps are within the asymmetric interval
    fn is_within_bounds(&self, left_time: i64, right_time: i64) -> bool {
        let diff = left_time - right_time;
        diff >= self.lower_bound && diff <= self.upper_bound
    }
}

impl StreamOperator for IntervalJoinOperator {
    fn process(&mut self, row: Row) -> Result<Option<Row>> {
        self.inner.process(row)
    }

    fn flush(&mut self) -> Result<Vec<Row>> {
        self.inner.flush()
    }

    fn output_schema(&self) -> &Schema {
        self.inner.output_schema()
    }
}

/// Temporal join operator for stream-table joins
///
/// Joins a stream against a versioned table, matching each stream event
/// with the table state at that point in time.
#[allow(dead_code)]
pub struct TemporalJoinOperator {
    /// Join configuration
    config: JoinConfig,
    /// Stream schema
    stream_schema: Schema,
    /// Table schema
    table_schema: Schema,
    /// Output schema
    output_schema: Schema,
    /// Stream key column index
    stream_key: usize,
    /// Table key column index
    table_key: usize,
    /// Table time column index
    table_time_key: usize,
    /// Versioned table state (key -> (valid_time, row))
    table_state: HashMap<Value, Vec<(i64, Row)>>,
    /// Function registry
    #[allow(dead_code)]
    functions: FunctionRegistry,
}

impl TemporalJoinOperator {
    /// Create a new temporal join operator
    pub fn new(
        config: JoinConfig,
        stream_schema: Schema,
        table_schema: Schema,
        stream_key_col: &str,
        table_key_col: &str,
        table_time_col: &str,
    ) -> Result<Self> {
        let stream_key = stream_schema.column_index(stream_key_col).ok_or_else(|| {
            StreamlineError::Query(format!("Unknown stream key column: {}", stream_key_col))
        })?;

        let table_key = table_schema.column_index(table_key_col).ok_or_else(|| {
            StreamlineError::Query(format!("Unknown table key column: {}", table_key_col))
        })?;

        let table_time_key = table_schema.column_index(table_time_col).ok_or_else(|| {
            StreamlineError::Query(format!("Unknown table time column: {}", table_time_col))
        })?;

        let output_schema = Self::build_output_schema(&stream_schema, &table_schema);

        Ok(Self {
            config,
            stream_schema,
            table_schema,
            output_schema,
            stream_key,
            table_key,
            table_time_key,
            table_state: HashMap::new(),
            functions: FunctionRegistry::new(),
        })
    }

    /// Build output schema
    fn build_output_schema(stream: &Schema, table: &Schema) -> Schema {
        let mut columns = Vec::with_capacity(stream.len() + table.len());

        for col in &stream.columns {
            columns.push(Column::new(col.name.clone(), col.data_type.clone()));
        }

        for col in &table.columns {
            columns.push(Column::new(
                format!("table_{}", col.name),
                col.data_type.clone(),
            ));
        }

        Schema::new(columns)
    }

    /// Update table state with a new row
    pub fn update_table(&mut self, row: Row) {
        let key = row
            .values
            .get(self.table_key)
            .cloned()
            .unwrap_or(Value::Null);
        let valid_time = row
            .values
            .get(self.table_time_key)
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        let versions = self.table_state.entry(key).or_default();

        // Insert maintaining sorted order by time
        let pos = versions
            .iter()
            .position(|(t, _)| *t > valid_time)
            .unwrap_or(versions.len());
        versions.insert(pos, (valid_time, row));
    }

    /// Lookup table state at a given time
    fn lookup_at_time(&self, key: &Value, event_time: i64) -> Option<&Row> {
        self.table_state.get(key).and_then(|versions| {
            // Find the latest version that is <= event_time
            let mut result = None;
            for (valid_time, row) in versions {
                if *valid_time <= event_time {
                    result = Some(row);
                } else {
                    break;
                }
            }
            result
        })
    }

    /// Combine stream and table rows
    fn combine_rows(&self, stream_row: &Row, table_row: Option<&Row>) -> Row {
        let mut values = stream_row.values.clone();

        if let Some(table) = table_row {
            values.extend(table.values.iter().cloned());
        } else {
            // Null-pad for outer join
            for _ in 0..self.table_schema.len() {
                values.push(Value::Null);
            }
        }

        Row {
            values,
            event_time: stream_row.event_time,
        }
    }
}

impl StreamOperator for TemporalJoinOperator {
    fn process(&mut self, row: Row) -> Result<Option<Row>> {
        let key = row
            .values
            .get(self.stream_key)
            .cloned()
            .unwrap_or(Value::Null);
        let event_time = row.event_time.unwrap_or(0);

        let table_row = self.lookup_at_time(&key, event_time);

        match self.config.join_type {
            JoinType::Inner => {
                if table_row.is_some() {
                    Ok(Some(self.combine_rows(&row, table_row)))
                } else {
                    Ok(None)
                }
            }
            JoinType::Left => Ok(Some(self.combine_rows(&row, table_row))),
            _ => Err(StreamlineError::Query(
                "Temporal join only supports INNER and LEFT joins".to_string(),
            )),
        }
    }

    fn flush(&mut self) -> Result<Vec<Row>> {
        Ok(Vec::new())
    }

    fn output_schema(&self) -> &Schema {
        &self.output_schema
    }
}

/// Nested loop join operator for complex conditions
///
/// Suitable for non-equi joins where hash-based optimization isn't possible.
pub struct NestedLoopJoinOperator {
    /// Join configuration
    config: JoinConfig,
    /// Left schema
    left_schema: Schema,
    /// Right schema
    right_schema: Schema,
    /// Output schema
    output_schema: Schema,
    /// Join condition expression
    condition: Expression,
    /// Left buffer
    left_buffer: Vec<BufferedRow>,
    /// Right buffer
    right_buffer: Vec<BufferedRow>,
    /// Function registry
    functions: FunctionRegistry,
    /// Current side
    current_side: JoinSide,
}

impl NestedLoopJoinOperator {
    /// Create a new nested loop join operator
    pub fn new(
        config: JoinConfig,
        left_schema: Schema,
        right_schema: Schema,
        condition: Expression,
    ) -> Self {
        let output_schema = Self::build_output_schema(&left_schema, &right_schema);

        Self {
            config,
            left_schema,
            right_schema,
            output_schema,
            condition,
            left_buffer: Vec::new(),
            right_buffer: Vec::new(),
            functions: FunctionRegistry::new(),
            current_side: JoinSide::Left,
        }
    }

    /// Build output schema
    fn build_output_schema(left: &Schema, right: &Schema) -> Schema {
        let mut columns = Vec::with_capacity(left.len() + right.len());

        for col in &left.columns {
            columns.push(Column::new(
                format!("left_{}", col.name),
                col.data_type.clone(),
            ));
        }

        for col in &right.columns {
            columns.push(Column::new(
                format!("right_{}", col.name),
                col.data_type.clone(),
            ));
        }

        Schema::new(columns)
    }

    /// Set the side for incoming rows
    pub fn set_side(&mut self, side: JoinSide) {
        self.current_side = side;
    }

    /// Process a left row against all buffered right rows
    fn process_left(&mut self, row: Row) -> Result<Vec<Row>> {
        let event_time = row.event_time.unwrap_or(0);
        let mut results = Vec::new();
        let mut matched = false;

        // First pass: collect matches
        let mut match_indices = Vec::new();
        for (i, right) in self.right_buffer.iter().enumerate() {
            // Combine rows inline
            let mut values = row.values.clone();
            values.extend(right.row.values.iter().cloned());
            let combined_time = match (row.event_time, right.row.event_time) {
                (Some(l), Some(r)) => Some(l.max(r)),
                (Some(t), None) | (None, Some(t)) => Some(t),
                (None, None) => None,
            };
            let combined = Row {
                values,
                event_time: combined_time,
            };

            let result = evaluate_expression(
                &self.condition,
                &combined,
                &self.output_schema,
                &self.functions,
            )?;
            let condition_met = match result {
                Value::Boolean(b) => b,
                Value::Null => false,
                _ => {
                    return Err(StreamlineError::Query(
                        "Join condition must be boolean".to_string(),
                    ))
                }
            };

            if condition_met {
                matched = true;
                match_indices.push(i);
                results.push(combined);
            }
        }

        // Second pass: mark matched
        for i in match_indices {
            self.right_buffer[i].matched = true;
        }

        self.left_buffer.push(BufferedRow {
            row,
            event_time,
            matched,
        });

        Ok(results)
    }

    /// Process a right row against all buffered left rows
    fn process_right(&mut self, row: Row) -> Result<Vec<Row>> {
        let event_time = row.event_time.unwrap_or(0);
        let mut results = Vec::new();
        let mut matched = false;

        // First pass: collect matches
        let mut match_indices = Vec::new();
        for (i, left) in self.left_buffer.iter().enumerate() {
            // Combine rows inline
            let mut values = left.row.values.clone();
            values.extend(row.values.iter().cloned());
            let combined_time = match (left.row.event_time, row.event_time) {
                (Some(l), Some(r)) => Some(l.max(r)),
                (Some(t), None) | (None, Some(t)) => Some(t),
                (None, None) => None,
            };
            let combined = Row {
                values,
                event_time: combined_time,
            };

            let result = evaluate_expression(
                &self.condition,
                &combined,
                &self.output_schema,
                &self.functions,
            )?;
            let condition_met = match result {
                Value::Boolean(b) => b,
                Value::Null => false,
                _ => {
                    return Err(StreamlineError::Query(
                        "Join condition must be boolean".to_string(),
                    ))
                }
            };

            if condition_met {
                matched = true;
                match_indices.push(i);
                results.push(combined);
            }
        }

        // Second pass: mark matched
        for i in match_indices {
            self.left_buffer[i].matched = true;
        }

        self.right_buffer.push(BufferedRow {
            row,
            event_time,
            matched,
        });

        Ok(results)
    }

    /// Create a null-padded row for outer joins
    fn null_pad_left(&self, left: &Row) -> Row {
        let mut values = left.values.clone();
        for _ in 0..self.right_schema.len() {
            values.push(Value::Null);
        }
        Row {
            values,
            event_time: left.event_time,
        }
    }

    /// Create a null-padded row for outer joins
    fn null_pad_right(&self, right: &Row) -> Row {
        let mut values = Vec::with_capacity(self.left_schema.len() + right.values.len());
        for _ in 0..self.left_schema.len() {
            values.push(Value::Null);
        }
        values.extend(right.values.iter().cloned());
        Row {
            values,
            event_time: right.event_time,
        }
    }
}

impl StreamOperator for NestedLoopJoinOperator {
    fn process(&mut self, row: Row) -> Result<Option<Row>> {
        let results = match self.current_side {
            JoinSide::Left => self.process_left(row)?,
            JoinSide::Right => self.process_right(row)?,
        };

        // For now, return first result if any
        Ok(results.into_iter().next())
    }

    fn flush(&mut self) -> Result<Vec<Row>> {
        let mut results = Vec::new();

        // Emit unmatched rows for outer joins
        match self.config.join_type {
            JoinType::Left | JoinType::Full => {
                for buffered in &self.left_buffer {
                    if !buffered.matched {
                        results.push(self.null_pad_left(&buffered.row));
                    }
                }
            }
            _ => {}
        }

        match self.config.join_type {
            JoinType::Right | JoinType::Full => {
                for buffered in &self.right_buffer {
                    if !buffered.matched {
                        results.push(self.null_pad_right(&buffered.row));
                    }
                }
            }
            _ => {}
        }

        Ok(results)
    }

    fn output_schema(&self) -> &Schema {
        &self.output_schema
    }
}

/// Cross join (Cartesian product) operator
#[allow(dead_code)]
pub struct CrossJoinOperator {
    /// Left schema
    left_schema: Schema,
    /// Right schema
    #[allow(dead_code)]
    right_schema: Schema,
    /// Output schema
    output_schema: Schema,
    /// Left buffer
    left_buffer: Vec<Row>,
    /// Right buffer
    right_buffer: Vec<Row>,
    /// Current side
    current_side: JoinSide,
    /// Output buffer for incremental output
    output_buffer: VecDeque<Row>,
}

impl CrossJoinOperator {
    /// Create a new cross join operator
    pub fn new(left_schema: Schema, right_schema: Schema) -> Self {
        let output_schema = Self::build_output_schema(&left_schema, &right_schema);

        Self {
            left_schema,
            right_schema,
            output_schema,
            left_buffer: Vec::new(),
            right_buffer: Vec::new(),
            current_side: JoinSide::Left,
            output_buffer: VecDeque::new(),
        }
    }

    /// Build output schema
    fn build_output_schema(left: &Schema, right: &Schema) -> Schema {
        let mut columns = Vec::with_capacity(left.len() + right.len());

        for col in &left.columns {
            columns.push(Column::new(
                format!("left_{}", col.name),
                col.data_type.clone(),
            ));
        }

        for col in &right.columns {
            columns.push(Column::new(
                format!("right_{}", col.name),
                col.data_type.clone(),
            ));
        }

        Schema::new(columns)
    }

    /// Set the side for incoming rows
    pub fn set_side(&mut self, side: JoinSide) {
        self.current_side = side;
    }

    /// Combine two rows
    fn combine_rows(&self, left: &Row, right: &Row) -> Row {
        let mut values = left.values.clone();
        values.extend(right.values.iter().cloned());
        Row::new(values)
    }
}

impl StreamOperator for CrossJoinOperator {
    fn process(&mut self, row: Row) -> Result<Option<Row>> {
        match self.current_side {
            JoinSide::Left => {
                // Cross with all existing right rows
                for right in &self.right_buffer {
                    self.output_buffer.push_back(self.combine_rows(&row, right));
                }
                self.left_buffer.push(row);
            }
            JoinSide::Right => {
                // Cross with all existing left rows
                for left in &self.left_buffer {
                    self.output_buffer.push_back(self.combine_rows(left, &row));
                }
                self.right_buffer.push(row);
            }
        }

        Ok(self.output_buffer.pop_front())
    }

    fn flush(&mut self) -> Result<Vec<Row>> {
        Ok(self.output_buffer.drain(..).collect())
    }

    fn output_schema(&self) -> &Schema {
        &self.output_schema
    }
}

/// Builder for creating join operators
pub struct JoinBuilder {
    config: JoinConfig,
    left_schema: Option<Schema>,
    right_schema: Option<Schema>,
    left_key: Option<String>,
    right_key: Option<String>,
}

impl JoinBuilder {
    /// Create a new join builder
    pub fn new() -> Self {
        Self {
            config: JoinConfig::default(),
            left_schema: None,
            right_schema: None,
            left_key: None,
            right_key: None,
        }
    }

    /// Set the join type
    pub fn join_type(mut self, join_type: JoinType) -> Self {
        self.config.join_type = join_type;
        self
    }

    /// Set the left schema
    pub fn left_schema(mut self, schema: Schema) -> Self {
        self.left_schema = Some(schema);
        self
    }

    /// Set the right schema
    pub fn right_schema(mut self, schema: Schema) -> Self {
        self.right_schema = Some(schema);
        self
    }

    /// Set the join keys
    pub fn on(mut self, left_key: &str, right_key: &str) -> Self {
        self.left_key = Some(left_key.to_string());
        self.right_key = Some(right_key.to_string());
        self
    }

    /// Set the interval window
    pub fn interval(mut self, duration: Duration) -> Self {
        self.config.interval = Some(duration);
        self
    }

    /// Set the maximum buffer size
    pub fn max_buffer_size(mut self, size: usize) -> Self {
        self.config.max_buffer_size = size;
        self
    }

    /// Build a hash join operator
    pub fn build_hash_join(self) -> Result<HashJoinOperator> {
        let left_schema = self
            .left_schema
            .ok_or_else(|| StreamlineError::Query("Left schema required".to_string()))?;
        let right_schema = self
            .right_schema
            .ok_or_else(|| StreamlineError::Query("Right schema required".to_string()))?;
        let left_key = self
            .left_key
            .ok_or_else(|| StreamlineError::Query("Left key required".to_string()))?;
        let right_key = self
            .right_key
            .ok_or_else(|| StreamlineError::Query("Right key required".to_string()))?;

        HashJoinOperator::new(
            self.config,
            left_schema,
            right_schema,
            &left_key,
            &right_key,
        )
    }

    /// Build a cross join operator
    pub fn build_cross_join(self) -> Result<CrossJoinOperator> {
        let left_schema = self
            .left_schema
            .ok_or_else(|| StreamlineError::Query("Left schema required".to_string()))?;
        let right_schema = self
            .right_schema
            .ok_or_else(|| StreamlineError::Query("Right schema required".to_string()))?;

        Ok(CrossJoinOperator::new(left_schema, right_schema))
    }
}

impl Default for JoinBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::super::types::DataType;
    use super::*;

    fn orders_schema() -> Schema {
        Schema::new(vec![
            Column::new("order_id", DataType::Int64),
            Column::new("customer_id", DataType::Int64),
            Column::new("amount", DataType::Float64),
        ])
    }

    fn customers_schema() -> Schema {
        Schema::new(vec![
            Column::new("id", DataType::Int64),
            Column::new("name", DataType::String),
        ])
    }

    #[test]
    fn test_hash_join_inner() {
        let config = JoinConfig {
            join_type: JoinType::Inner,
            interval: None,
            ..Default::default()
        };

        let mut join = HashJoinOperator::new(
            config,
            orders_schema(),
            customers_schema(),
            "customer_id",
            "id",
        )
        .unwrap();

        // Add a customer (right side)
        join.set_side(JoinSide::Right);
        let customer = Row::new(vec![Value::Int64(1), Value::String("Alice".to_string())]);
        let result = join.process(customer).unwrap();
        assert!(result.is_none()); // No match yet

        // Add an order (left side)
        join.set_side(JoinSide::Left);
        let order = Row::new(vec![
            Value::Int64(100),
            Value::Int64(1), // customer_id = 1
            Value::Float64(50.0),
        ]);
        let result = join.process(order).unwrap();
        assert!(result.is_some()); // Should match

        let joined = result.unwrap();
        assert_eq!(joined.values.len(), 5); // 3 order + 2 customer columns
        assert_eq!(joined.values[0], Value::Int64(100)); // order_id
        assert_eq!(joined.values[4], Value::String("Alice".to_string())); // customer name
    }

    #[test]
    fn test_hash_join_left_outer() {
        let config = JoinConfig {
            join_type: JoinType::Left,
            interval: None,
            ..Default::default()
        };

        let mut join = HashJoinOperator::new(
            config,
            orders_schema(),
            customers_schema(),
            "customer_id",
            "id",
        )
        .unwrap();

        // Add an order without matching customer
        join.set_side(JoinSide::Left);
        let order = Row::new(vec![
            Value::Int64(100),
            Value::Int64(999), // non-existent customer
            Value::Float64(50.0),
        ]);
        let _ = join.process(order);

        // Flush should emit unmatched rows
        let results = join.flush().unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].values[0], Value::Int64(100)); // order_id preserved
        assert_eq!(results[0].values[3], Value::Null); // customer id is null
        assert_eq!(results[0].values[4], Value::Null); // customer name is null
    }

    #[test]
    fn test_cross_join() {
        let mut join = CrossJoinOperator::new(
            Schema::new(vec![Column::new("a", DataType::Int64)]),
            Schema::new(vec![Column::new("b", DataType::Int64)]),
        );

        // Add left rows
        join.set_side(JoinSide::Left);
        join.process(Row::new(vec![Value::Int64(1)])).unwrap();
        join.process(Row::new(vec![Value::Int64(2)])).unwrap();

        // Add right rows
        join.set_side(JoinSide::Right);
        let r1 = join.process(Row::new(vec![Value::Int64(10)])).unwrap();
        let r2 = join.process(Row::new(vec![Value::Int64(20)])).unwrap();

        // Should have cross product: (1,10), (2,10), (1,20), (2,20)
        assert!(r1.is_some() || r2.is_some());

        let remaining = join.flush().unwrap();
        // Total results should be 4
        let total = (if r1.is_some() { 1 } else { 0 })
            + (if r2.is_some() { 1 } else { 0 })
            + remaining.len();
        assert_eq!(total, 4);
    }

    #[test]
    fn test_join_builder() {
        let join = JoinBuilder::new()
            .join_type(JoinType::Inner)
            .left_schema(orders_schema())
            .right_schema(customers_schema())
            .on("customer_id", "id")
            .interval(Duration::from_secs(3600))
            .max_buffer_size(10000)
            .build_hash_join();

        assert!(join.is_ok());
    }

    #[test]
    fn test_interval_bounds() {
        let config = JoinConfig {
            join_type: JoinType::Inner,
            interval: Some(Duration::from_secs(60)), // 1 minute window
            ..Default::default()
        };

        let mut join = HashJoinOperator::new(
            config,
            orders_schema(),
            customers_schema(),
            "customer_id",
            "id",
        )
        .unwrap();

        // Add customer at t=0
        join.set_side(JoinSide::Right);
        let customer =
            Row::with_event_time(vec![Value::Int64(1), Value::String("Alice".to_string())], 0);
        join.process(customer).unwrap();

        // Add order at t=30s (within window)
        join.set_side(JoinSide::Left);
        let order1 = Row::with_event_time(
            vec![Value::Int64(100), Value::Int64(1), Value::Float64(50.0)],
            30_000, // 30 seconds
        );
        let result1 = join.process(order1).unwrap();
        assert!(result1.is_some()); // Should match

        // Add order at t=120s (outside window)
        let order2 = Row::with_event_time(
            vec![Value::Int64(101), Value::Int64(1), Value::Float64(75.0)],
            120_000, // 120 seconds
        );
        let result2 = join.process(order2).unwrap();
        assert!(result2.is_none()); // Should not match
    }
}
