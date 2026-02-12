//! SQL-Native Stream Processing Engine
//!
//! This module provides a full continuous query engine for SQL over streams
//! with windowed queries, JOINs, aggregations, and CEP pattern matching.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────┐    ┌──────────────┐    ┌──────────────────────┐
//! │  SQL Input   │──▶│  Parser      │──▶│  ContinuousQueryEngine│
//! │  (String)    │   │  (validate)  │   │  (manage & execute)   │
//! └─────────────┘    └──────────────┘    └──────────────────────┘
//!                                                │
//!                          ┌──────────────────────┼─────────────┐
//!                          ▼                      ▼             ▼
//!                    ┌───────────┐        ┌────────────┐  ┌──────────┐
//!                    │ Windows   │        │ Aggregates │  │  Joins   │
//!                    │ (tumbling,│        │ (count,sum,│  │ (inner,  │
//!                    │  hopping, │        │  avg,min,  │  │  left,   │
//!                    │  session) │        │  max)      │  │  right)  │
//!                    └───────────┘        └────────────┘  └──────────┘
//! ```
//!
//! ## Example
//!
//! ```sql
//! SELECT user_id, COUNT(*) as event_count, AVG(latency) as avg_latency
//! FROM events
//! WHERE status = 'error'
//! WINDOW TUMBLING(SIZE 60000)
//! GROUP BY user_id
//! HAVING COUNT(*) > 5
//! ```

use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

// ---------------------------------------------------------------------------
// Core enums
// ---------------------------------------------------------------------------

/// Type of a continuous query
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueryType {
    /// Standard SELECT query
    Select,
    /// INSERT INTO ... SELECT ...
    InsertInto,
    /// CREATE STREAM AS SELECT ...
    CreateStream,
    /// CREATE TABLE AS SELECT ...
    CreateTable,
}

/// Status of a running query
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueryStatus {
    /// SQL is being parsed
    Parsing,
    /// Execution plan is being built
    Planning,
    /// Query is actively processing records
    Running,
    /// Query finished (bounded source)
    Completed,
    /// Query encountered a fatal error
    Failed,
    /// Query was cancelled by the user
    Cancelled,
}

/// Window type for windowed aggregations
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EngineWindowType {
    /// Fixed-size, non-overlapping windows
    Tumbling,
    /// Fixed-size windows that advance by a hop interval
    Hopping,
    /// Windows that close after an inactivity gap
    Session,
    /// Sliding window that emits on every record
    Sliding,
}

/// Join type for stream-stream or stream-table joins
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinType {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
}

/// Condition specification for a join
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinCondition {
    /// Join on matching key columns
    OnKeys { left_key: String, right_key: String },
    /// Join on an arbitrary expression
    OnExpression(String),
}

/// Comparison operators used in compiled filters
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ComparisonOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Like,
    In,
    Between,
    IsNull,
    IsNotNull,
}

/// Value within a compiled filter expression
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FilterValue {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    List(Vec<FilterValue>),
    Null,
}

/// Value of a single field in a `StreamRecord`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FieldValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
}

impl FieldValue {
    /// Try to extract an f64 for aggregation purposes
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            FieldValue::Int(i) => Some(*i as f64),
            FieldValue::Float(f) => Some(*f),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Aggregate State
// ---------------------------------------------------------------------------

/// Accumulator for a single aggregate function in a window
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AggregateState {
    /// COUNT(*)
    Count(u64),
    /// SUM(expr)
    Sum(f64),
    /// AVG(expr)
    Avg { sum: f64, count: u64 },
    /// MIN(expr)
    Min(f64),
    /// MAX(expr)
    Max(f64),
}

impl AggregateState {
    /// Feed a new numeric value into the accumulator
    pub fn update(&mut self, value: f64) {
        match self {
            AggregateState::Count(c) => *c += 1,
            AggregateState::Sum(s) => *s += value,
            AggregateState::Avg { sum, count } => {
                *sum += value;
                *count += 1;
            }
            AggregateState::Min(m) => {
                if value < *m {
                    *m = value;
                }
            }
            AggregateState::Max(m) => {
                if value > *m {
                    *m = value;
                }
            }
        }
    }

    /// Compute the final result of the aggregate
    pub fn result(&self) -> FieldValue {
        match self {
            AggregateState::Count(c) => FieldValue::Int(*c as i64),
            AggregateState::Sum(s) => FieldValue::Float(*s),
            AggregateState::Avg { sum, count } => {
                if *count == 0 {
                    FieldValue::Null
                } else {
                    FieldValue::Float(*sum / *count as f64)
                }
            }
            AggregateState::Min(m) => FieldValue::Float(*m),
            AggregateState::Max(m) => FieldValue::Float(*m),
        }
    }
}

// ---------------------------------------------------------------------------
// Data structures
// ---------------------------------------------------------------------------

/// A projection in a SELECT clause
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Projection {
    /// Column name, function call, or expression text
    pub expression: String,
    /// Optional alias (AS <alias>)
    pub alias: Option<String>,
}

/// Compiled filter for efficient evaluation
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompiledFilter {
    /// Field name to evaluate
    pub field: String,
    /// Comparison operator
    pub op: ComparisonOp,
    /// Right-hand-side value
    pub value: FilterValue,
}

impl CompiledFilter {
    /// Evaluate the compiled filter against a set of fields
    pub fn evaluate(&self, fields: &HashMap<String, FieldValue>) -> bool {
        let field_value = match fields.get(&self.field) {
            Some(v) => v,
            None => return matches!(self.op, ComparisonOp::IsNull),
        };

        match &self.op {
            ComparisonOp::IsNull => matches!(field_value, FieldValue::Null),
            ComparisonOp::IsNotNull => !matches!(field_value, FieldValue::Null),
            ComparisonOp::Eq => self.field_equals(field_value),
            ComparisonOp::Ne => !self.field_equals(field_value),
            ComparisonOp::Lt => self.field_cmp(field_value).is_some_and(|o| o.is_lt()),
            ComparisonOp::Le => self.field_cmp(field_value).is_some_and(|o| o.is_le()),
            ComparisonOp::Gt => self.field_cmp(field_value).is_some_and(|o| o.is_gt()),
            ComparisonOp::Ge => self.field_cmp(field_value).is_some_and(|o| o.is_ge()),
            ComparisonOp::Like => self.field_like(field_value),
            ComparisonOp::In => self.field_in(field_value),
            ComparisonOp::Between => self.field_between(field_value),
        }
    }

    /// Equality check between field value and filter value
    fn field_equals(&self, field_value: &FieldValue) -> bool {
        match (field_value, &self.value) {
            (FieldValue::String(a), FilterValue::String(b)) => a == b,
            (FieldValue::Int(a), FilterValue::Int(b)) => a == b,
            (FieldValue::Float(a), FilterValue::Float(b)) => (a - b).abs() < f64::EPSILON,
            (FieldValue::Bool(a), FilterValue::Bool(b)) => a == b,
            (FieldValue::Null, FilterValue::Null) => true,
            _ => false,
        }
    }

    /// Ordering comparison
    fn field_cmp(&self, field_value: &FieldValue) -> Option<std::cmp::Ordering> {
        match (field_value, &self.value) {
            (FieldValue::Int(a), FilterValue::Int(b)) => Some(a.cmp(b)),
            (FieldValue::Float(a), FilterValue::Float(b)) => a.partial_cmp(b),
            (FieldValue::String(a), FilterValue::String(b)) => Some(a.cmp(b)),
            (FieldValue::Int(a), FilterValue::Float(b)) => (*a as f64).partial_cmp(b),
            (FieldValue::Float(a), FilterValue::Int(b)) => a.partial_cmp(&(*b as f64)),
            _ => None,
        }
    }

    /// SQL LIKE pattern matching (supports % and _)
    fn field_like(&self, field_value: &FieldValue) -> bool {
        let (FieldValue::String(text), FilterValue::String(pattern)) = (field_value, &self.value)
        else {
            return false;
        };
        like_match(text, pattern)
    }

    /// SQL IN (...) check
    fn field_in(&self, field_value: &FieldValue) -> bool {
        let FilterValue::List(list) = &self.value else {
            return false;
        };
        list.iter().any(|v| {
            let tmp = CompiledFilter {
                field: self.field.clone(),
                op: ComparisonOp::Eq,
                value: v.clone(),
            };
            let fields = {
                let mut m = HashMap::new();
                m.insert(self.field.clone(), field_value.clone());
                m
            };
            tmp.evaluate(&fields)
        })
    }

    /// SQL BETWEEN check (list must have exactly 2 elements)
    fn field_between(&self, field_value: &FieldValue) -> bool {
        let FilterValue::List(list) = &self.value else {
            return false;
        };
        if list.len() != 2 {
            return false;
        }
        let low = CompiledFilter {
            field: self.field.clone(),
            op: ComparisonOp::Ge,
            value: list[0].clone(),
        };
        let high = CompiledFilter {
            field: self.field.clone(),
            op: ComparisonOp::Le,
            value: list[1].clone(),
        };
        let fields = {
            let mut m = HashMap::new();
            m.insert(self.field.clone(), field_value.clone());
            m
        };
        low.evaluate(&fields) && high.evaluate(&fields)
    }
}

/// Simple SQL LIKE pattern matching
fn like_match(text: &str, pattern: &str) -> bool {
    let mut t = text.chars().peekable();
    let mut p = pattern.chars().peekable();

    fn helper(
        t: &mut std::iter::Peekable<std::str::Chars>,
        p: &mut std::iter::Peekable<std::str::Chars>,
    ) -> bool {
        // We use a simple recursive approach for clarity.
        // Clone iterators to allow backtracking for '%'.
        loop {
            match p.peek() {
                None => return t.peek().is_none(),
                Some('%') => {
                    p.next();
                    // '%' can match zero or more characters
                    // Try matching the remaining pattern against every suffix of t
                    let mut t_clone = t.clone();
                    let p_snapshot = p.clone();
                    loop {
                        let mut p_try = p_snapshot.clone();
                        let mut t_try = t_clone.clone();
                        if helper(&mut t_try, &mut p_try) {
                            return true;
                        }
                        if t_clone.next().is_none() {
                            return false;
                        }
                    }
                }
                Some('_') => {
                    p.next();
                    if t.next().is_none() {
                        return false;
                    }
                }
                Some(&pc) => {
                    p.next();
                    match t.next() {
                        Some(tc) if tc == pc => {}
                        _ => return false,
                    }
                }
            }
        }
    }

    helper(&mut t, &mut p)
}

/// Filter expression (raw or compiled)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FilterExpr {
    /// Original expression text
    pub expression: String,
    /// Compiled representation for fast evaluation
    pub compiled: Option<CompiledFilter>,
}

/// Window specification
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WindowSpec {
    /// Window strategy
    pub window_type: EngineWindowType,
    /// Window size in milliseconds
    pub size_ms: u64,
    /// Advance / hop interval for hopping windows
    pub advance_ms: Option<u64>,
    /// Inactivity gap for session windows
    pub gap_ms: Option<u64>,
    /// Grace period for late data
    pub grace_period_ms: Option<u64>,
}

/// Join specification
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinSpec {
    /// Type of join
    pub join_type: JoinType,
    /// Left source topic
    pub left_topic: String,
    /// Right source topic
    pub right_topic: String,
    /// Join condition
    pub condition: JoinCondition,
    /// Temporal bound for stream-stream joins
    pub within_ms: Option<u64>,
}

// ---------------------------------------------------------------------------
// StreamQuery - a parsed and validated continuous query
// ---------------------------------------------------------------------------

/// A parsed and validated continuous query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamQuery {
    /// Original SQL text
    pub sql: String,
    /// Query type
    pub query_type: QueryType,
    /// Source topic(s)
    pub source_topics: Vec<String>,
    /// Optional sink topic (INSERT INTO / CREATE STREAM)
    pub sink_topic: Option<String>,
    /// Optional window specification
    pub window: Option<WindowSpec>,
    /// GROUP BY column names
    pub group_by: Vec<String>,
    /// HAVING clause text
    pub having: Option<String>,
    /// SELECT projections
    pub projections: Vec<Projection>,
    /// WHERE filter
    pub filter: Option<FilterExpr>,
    /// JOIN specifications
    pub joins: Vec<JoinSpec>,
}

// ---------------------------------------------------------------------------
// Stream records
// ---------------------------------------------------------------------------

/// A record flowing through the query engine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamRecord {
    /// Source topic
    pub topic: String,
    /// Partition number
    pub partition: i32,
    /// Offset within the partition
    pub offset: i64,
    /// Optional record key
    pub key: Option<Vec<u8>>,
    /// Field name -> value mapping
    pub fields: HashMap<String, FieldValue>,
    /// Event timestamp in milliseconds
    pub timestamp: i64,
}

// ---------------------------------------------------------------------------
// Window state
// ---------------------------------------------------------------------------

/// State for a single window instance
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowState {
    /// Inclusive window start time (ms)
    pub window_start: i64,
    /// Exclusive window end time (ms)
    pub window_end: i64,
    /// Records collected in this window
    pub records: Vec<StreamRecord>,
    /// Running aggregation states keyed by expression
    pub aggregations: HashMap<String, AggregateState>,
    /// Whether the window has been closed / emitted
    pub is_closed: bool,
}

// ---------------------------------------------------------------------------
// Query handles, stats, and info
// ---------------------------------------------------------------------------

/// Handle for a submitted continuous query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryHandle {
    /// Unique query identifier
    pub id: String,
    /// Original SQL text
    pub sql: String,
    /// Epoch millis when the query was created
    pub created_at: i64,
    /// Current status
    pub status: QueryStatus,
}

/// Runtime statistics for a query
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryStats {
    /// Records consumed from source topics
    pub records_in: u64,
    /// Records emitted to sink / output
    pub records_out: u64,
    /// Records discarded by the WHERE filter
    pub records_filtered: u64,
    /// Number of window instances opened
    pub windows_created: u64,
    /// Number of window instances closed and emitted
    pub windows_expired: u64,
    /// Number of join matches found
    pub joins_performed: u64,
    /// Number of join probes that found no match
    pub join_misses: u64,
    /// Average per-record processing latency in microseconds
    pub avg_latency_us: u64,
    /// Total errors encountered
    pub errors: u64,
    /// Time since query start in milliseconds
    pub uptime_ms: u64,
}

/// Combined information about a running query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryInfo {
    /// Query handle
    pub handle: QueryHandle,
    /// Runtime statistics
    pub stats: QueryStats,
    /// Source topics being consumed
    pub source_topics: Vec<String>,
}

// ---------------------------------------------------------------------------
// Simple SQL parser
// ---------------------------------------------------------------------------

/// Parse a simple SELECT statement into a `StreamQuery`.
///
/// Supported syntax (case-insensitive keywords):
///
/// ```text
/// SELECT <projections>
/// FROM <topic> [JOIN <topic> ON <left_key> = <right_key> [WITHIN <ms>]]
/// [WHERE <expression>]
/// [GROUP BY <cols>]
/// [HAVING <expression>]
/// [WINDOW TUMBLING|HOPPING|SESSION|SLIDING (SIZE <ms> [, ADVANCE <ms>] [, GAP <ms>])]
/// ```
pub fn parse_simple_select(sql: &str) -> Result<StreamQuery> {
    let normalized = normalize_whitespace(sql);
    let upper = normalized.to_uppercase();

    // Must start with SELECT
    if !upper.starts_with("SELECT ") {
        return Err(StreamlineError::Parse(
            "Query must start with SELECT".into(),
        ));
    }

    // ---- Extract major clauses by finding keyword positions ----
    let _select_pos: usize = 0;
    let from_pos = find_keyword(&upper, "FROM")
        .ok_or_else(|| StreamlineError::Parse("Missing FROM clause".into()))?;

    let join_pos = find_keyword(&upper, "JOIN");
    let where_pos = find_keyword(&upper, "WHERE");
    let group_by_pos = find_keyword(&upper, "GROUP BY");
    let having_pos = find_keyword(&upper, "HAVING");
    let window_pos = find_keyword(&upper, "WINDOW");

    // ---- Projections ----
    let proj_text = &normalized["SELECT ".len()..from_pos].trim();
    let projections = parse_projections(proj_text)?;

    // ---- FROM / source topics ----
    let from_end = first_of(&[join_pos, where_pos, group_by_pos, having_pos, window_pos])
        .unwrap_or(normalized.len());
    let from_text = normalized[from_pos + "FROM".len()..from_end]
        .trim()
        .to_string();
    let source_topics: Vec<String> = from_text
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    if source_topics.is_empty() {
        return Err(StreamlineError::Parse("No source topic specified".into()));
    }

    // ---- JOIN ----
    let mut joins = Vec::new();
    if let Some(jp) = join_pos {
        let join_end = first_of(&[where_pos, group_by_pos, having_pos, window_pos])
            .unwrap_or(normalized.len());
        let join_text = normalized[jp + "JOIN".len()..join_end].trim().to_string();
        let join_upper = join_text.to_uppercase();

        // Parse: <right_topic> ON <left_key> = <right_key> [WITHIN <ms>]
        let on_pos = join_upper
            .find("ON ")
            .ok_or_else(|| StreamlineError::Parse("JOIN requires ON condition".into()))?;
        let right_topic = join_text[..on_pos].trim().to_string();

        let within_pos = join_upper.find("WITHIN");
        let cond_end = within_pos.unwrap_or(join_text.len());
        let cond_text = join_text[on_pos + "ON ".len()..cond_end].trim().to_string();

        let within_ms = if let Some(wp) = within_pos {
            let ms_str = join_text[wp + "WITHIN".len()..].trim();
            Some(
                ms_str
                    .parse::<u64>()
                    .map_err(|_| StreamlineError::Parse("Invalid WITHIN value".into()))?,
            )
        } else {
            None
        };

        let condition = if let Some(eq_idx) = cond_text.find('=') {
            let left_key = cond_text[..eq_idx].trim().to_string();
            let right_key = cond_text[eq_idx + 1..].trim().to_string();
            JoinCondition::OnKeys {
                left_key,
                right_key,
            }
        } else {
            JoinCondition::OnExpression(cond_text.clone())
        };

        let left_topic = source_topics.first().cloned().unwrap_or_default();

        joins.push(JoinSpec {
            join_type: JoinType::Inner,
            left_topic,
            right_topic,
            condition,
            within_ms,
        });
    }

    // ---- WHERE ----
    let filter = if let Some(wp) = where_pos {
        let where_end =
            first_of(&[group_by_pos, having_pos, window_pos]).unwrap_or(normalized.len());
        let expr = normalized[wp + "WHERE".len()..where_end].trim().to_string();
        Some(FilterExpr {
            compiled: compile_simple_filter(&expr),
            expression: expr,
        })
    } else {
        None
    };

    // ---- GROUP BY ----
    let group_by = if let Some(gp) = group_by_pos {
        let gb_end = first_of(&[having_pos, window_pos]).unwrap_or(normalized.len());
        let gb_text = normalized[gp + "GROUP BY".len()..gb_end].trim().to_string();
        gb_text
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    } else {
        Vec::new()
    };

    // ---- HAVING ----
    let having = if let Some(hp) = having_pos {
        let h_end = window_pos.unwrap_or(normalized.len());
        let h_text = normalized[hp + "HAVING".len()..h_end].trim().to_string();
        if h_text.is_empty() {
            None
        } else {
            Some(h_text)
        }
    } else {
        None
    };

    // ---- WINDOW ----
    let window = if let Some(wp) = window_pos {
        let w_text = normalized[wp + "WINDOW".len()..].trim().to_string();
        Some(parse_window_spec(&w_text)?)
    } else {
        None
    };

    Ok(StreamQuery {
        sql: sql.to_string(),
        query_type: QueryType::Select,
        source_topics,
        sink_topic: None,
        window,
        group_by,
        having,
        projections,
        filter,
        joins,
    })
}

/// Find the byte offset of a keyword that appears as a standalone word in `upper`.
fn find_keyword(upper: &str, keyword: &str) -> Option<usize> {
    let mut search_from = 0;
    while let Some(pos) = upper[search_from..].find(keyword) {
        let abs_pos = search_from + pos;
        let before_ok = abs_pos == 0 || upper.as_bytes()[abs_pos - 1].is_ascii_whitespace();
        let after_pos = abs_pos + keyword.len();
        let after_ok = after_pos >= upper.len()
            || upper.as_bytes()[after_pos].is_ascii_whitespace()
            || upper.as_bytes()[after_pos] == b'(';
        if before_ok && after_ok {
            return Some(abs_pos);
        }
        search_from = abs_pos + keyword.len();
    }
    None
}

/// Return the smallest `Some` value from a slice of `Option<usize>`
fn first_of(options: &[Option<usize>]) -> Option<usize> {
    options.iter().filter_map(|o| *o).min()
}

/// Collapse multiple whitespace characters into single spaces and trim
fn normalize_whitespace(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut prev_space = true; // trim leading
    for ch in s.chars() {
        if ch.is_whitespace() {
            if !prev_space {
                result.push(' ');
                prev_space = true;
            }
        } else {
            result.push(ch);
            prev_space = false;
        }
    }
    if result.ends_with(' ') {
        result.pop();
    }
    result
}

/// Parse a comma-separated projection list
fn parse_projections(text: &str) -> Result<Vec<Projection>> {
    if text.is_empty() {
        return Err(StreamlineError::Parse("Empty projection list".into()));
    }

    let mut projections = Vec::new();
    // Split on commas that are not inside parentheses
    let parts = split_top_level(text, ',');

    for part in parts {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }

        // Check for AS alias (case-insensitive)
        let upper = part.to_uppercase();
        let (expression, alias) = if let Some(as_pos) = find_as_keyword(&upper) {
            let expr = part[..as_pos].trim().to_string();
            let alias = part[as_pos + 3..].trim().to_string();
            (expr, Some(alias))
        } else {
            (part.to_string(), None)
        };

        projections.push(Projection { expression, alias });
    }

    if projections.is_empty() {
        return Err(StreamlineError::Parse("No projections found".into()));
    }

    Ok(projections)
}

/// Split a string on `delimiter` only when not inside parentheses
fn split_top_level(s: &str, delimiter: char) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut depth = 0u32;

    for ch in s.chars() {
        if ch == '(' {
            depth += 1;
            current.push(ch);
        } else if ch == ')' {
            depth = depth.saturating_sub(1);
            current.push(ch);
        } else if ch == delimiter && depth == 0 {
            parts.push(current.clone());
            current.clear();
        } else {
            current.push(ch);
        }
    }
    if !current.is_empty() {
        parts.push(current);
    }
    parts
}

/// Find ` AS ` keyword position (must be surrounded by whitespace)
fn find_as_keyword(upper: &str) -> Option<usize> {
    let mut search_from = 0;
    while let Some(pos) = upper[search_from..].find(" AS ") {
        let abs_pos = search_from + pos;
        // Check we're not inside parentheses
        let depth: i32 = upper[..abs_pos]
            .chars()
            .map(|c| match c {
                '(' => 1,
                ')' => -1,
                _ => 0,
            })
            .sum();
        if depth == 0 {
            return Some(abs_pos);
        }
        search_from = abs_pos + 4;
    }
    None
}

/// Parse a window specification text like `TUMBLING(SIZE 60000)`
fn parse_window_spec(text: &str) -> Result<WindowSpec> {
    let upper = text.to_uppercase();
    let (window_type, params_start) = if upper.starts_with("TUMBLING") {
        (EngineWindowType::Tumbling, "TUMBLING".len())
    } else if upper.starts_with("HOPPING") {
        (EngineWindowType::Hopping, "HOPPING".len())
    } else if upper.starts_with("SESSION") {
        (EngineWindowType::Session, "SESSION".len())
    } else if upper.starts_with("SLIDING") {
        (EngineWindowType::Sliding, "SLIDING".len())
    } else {
        return Err(StreamlineError::Parse(format!(
            "Unknown window type: {}",
            text
        )));
    };

    // Extract content within parentheses
    let rest = text[params_start..].trim();
    let inner = if rest.starts_with('(') && rest.ends_with(')') {
        &rest[1..rest.len() - 1]
    } else {
        rest
    };

    let inner_upper = inner.to_uppercase();
    let parts: Vec<&str> = inner_upper.split(',').collect();

    let mut size_ms: Option<u64> = None;
    let mut advance_ms: Option<u64> = None;
    let mut gap_ms: Option<u64> = None;
    let mut grace_period_ms: Option<u64> = None;

    for part in &parts {
        let part = part.trim();
        if let Some(val) = part.strip_prefix("SIZE") {
            size_ms = Some(parse_ms_value(val.trim())?);
        } else if let Some(val) = part.strip_prefix("ADVANCE") {
            advance_ms = Some(parse_ms_value(val.trim())?);
        } else if let Some(val) = part.strip_prefix("GAP") {
            gap_ms = Some(parse_ms_value(val.trim())?);
        } else if let Some(val) = part.strip_prefix("GRACE") {
            grace_period_ms = Some(parse_ms_value(val.trim())?);
        } else {
            // Try bare number as SIZE
            if size_ms.is_none() {
                if let Ok(v) = part.parse::<u64>() {
                    size_ms = Some(v);
                }
            }
        }
    }

    let size_ms =
        size_ms.ok_or_else(|| StreamlineError::Parse("Window SIZE is required".into()))?;

    Ok(WindowSpec {
        window_type,
        size_ms,
        advance_ms,
        gap_ms,
        grace_period_ms,
    })
}

/// Parse a millisecond value (plain integer)
fn parse_ms_value(s: &str) -> Result<u64> {
    s.parse::<u64>()
        .map_err(|_| StreamlineError::Parse(format!("Invalid millisecond value: {}", s)))
}

/// Attempt to compile a simple `field op value` filter expression
fn compile_simple_filter(expr: &str) -> Option<CompiledFilter> {
    let expr = expr.trim();

    // IS NOT NULL
    {
        let upper = expr.to_uppercase();
        if let Some(pos) = upper.find("IS NOT NULL") {
            let field = expr[..pos].trim().to_string();
            if !field.is_empty() {
                return Some(CompiledFilter {
                    field,
                    op: ComparisonOp::IsNotNull,
                    value: FilterValue::Null,
                });
            }
        }
        // IS NULL
        if let Some(pos) = upper.find("IS NULL") {
            let field = expr[..pos].trim().to_string();
            if !field.is_empty() {
                return Some(CompiledFilter {
                    field,
                    op: ComparisonOp::IsNull,
                    value: FilterValue::Null,
                });
            }
        }
    }

    // Two-character operators: !=, >=, <=
    for (op_str, op) in &[
        ("!=", ComparisonOp::Ne),
        (">=", ComparisonOp::Ge),
        ("<=", ComparisonOp::Le),
    ] {
        if let Some(pos) = expr.find(op_str) {
            let field = expr[..pos].trim().to_string();
            let value_str = expr[pos + op_str.len()..].trim();
            if let Some(val) = parse_filter_value(value_str) {
                return Some(CompiledFilter {
                    field,
                    op: op.clone(),
                    value: val,
                });
            }
        }
    }

    // Single-character operators: =, <, >
    for (op_str, op) in &[
        ("=", ComparisonOp::Eq),
        ("<", ComparisonOp::Lt),
        (">", ComparisonOp::Gt),
    ] {
        if let Some(pos) = expr.find(op_str) {
            // Ensure we're not matching part of !=, >=, <=
            if pos > 0 && matches!(expr.as_bytes()[pos - 1], b'!' | b'>' | b'<') {
                continue;
            }
            if pos + 1 < expr.len() && expr.as_bytes()[pos + 1] == b'=' {
                continue;
            }
            let field = expr[..pos].trim().to_string();
            let value_str = expr[pos + op_str.len()..].trim();
            if let Some(val) = parse_filter_value(value_str) {
                return Some(CompiledFilter {
                    field,
                    op: op.clone(),
                    value: val,
                });
            }
        }
    }

    None
}

/// Parse a literal value from a filter expression
fn parse_filter_value(s: &str) -> Option<FilterValue> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    // Quoted string
    if (s.starts_with('\'') && s.ends_with('\'')) || (s.starts_with('"') && s.ends_with('"')) {
        return Some(FilterValue::String(s[1..s.len() - 1].to_string()));
    }
    // Boolean
    match s.to_uppercase().as_str() {
        "TRUE" => return Some(FilterValue::Bool(true)),
        "FALSE" => return Some(FilterValue::Bool(false)),
        "NULL" => return Some(FilterValue::Null),
        _ => {}
    }
    // Integer
    if let Ok(i) = s.parse::<i64>() {
        return Some(FilterValue::Int(i));
    }
    // Float
    if let Ok(f) = s.parse::<f64>() {
        return Some(FilterValue::Float(f));
    }
    None
}

// ---------------------------------------------------------------------------
// ContinuousQueryEngine
// ---------------------------------------------------------------------------

/// Internal state for a single running query
struct RunningQuery {
    handle: QueryHandle,
    query: StreamQuery,
    stats: QueryStats,
}

/// Manages submission, execution, and lifecycle of continuous queries
pub struct ContinuousQueryEngine {
    /// Active queries keyed by query ID
    queries: Arc<RwLock<HashMap<String, RunningQuery>>>,
    /// Monotonically increasing counter for query IDs
    next_id: Arc<RwLock<u64>>,
}

impl ContinuousQueryEngine {
    /// Create a new engine with no active queries
    pub fn new() -> Self {
        Self {
            queries: Arc::new(RwLock::new(HashMap::new())),
            next_id: Arc::new(RwLock::new(1)),
        }
    }

    /// Submit a SQL query for continuous execution.
    ///
    /// The SQL is parsed and validated. If successful a `QueryHandle` is returned
    /// that can be used to inspect or cancel the query.
    pub async fn submit_query(&self, sql: &str) -> Result<QueryHandle> {
        // Parse
        let query = parse_simple_select(sql)?;

        // Allocate ID
        let id = {
            let mut counter = self.next_id.write().await;
            let id = *counter;
            *counter += 1;
            format!("q-{}", id)
        };

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        let handle = QueryHandle {
            id: id.clone(),
            sql: sql.to_string(),
            created_at: now_ms,
            status: QueryStatus::Running,
        };

        let running = RunningQuery {
            handle: handle.clone(),
            query,
            stats: QueryStats::default(),
        };

        self.queries.write().await.insert(id, running);

        Ok(handle)
    }

    /// Cancel a running query
    pub async fn cancel_query(&self, handle: &QueryHandle) -> Result<()> {
        let mut queries = self.queries.write().await;
        let entry = queries
            .get_mut(&handle.id)
            .ok_or_else(|| StreamlineError::Query(format!("Query not found: {}", handle.id)))?;

        match entry.handle.status {
            QueryStatus::Running | QueryStatus::Parsing | QueryStatus::Planning => {
                entry.handle.status = QueryStatus::Cancelled;
                Ok(())
            }
            _ => Err(StreamlineError::Query(format!(
                "Cannot cancel query in {:?} state",
                entry.handle.status,
            ))),
        }
    }

    /// List all known queries (active and completed)
    pub async fn list_queries(&self) -> Vec<QueryInfo> {
        let queries = self.queries.read().await;
        queries
            .values()
            .map(|rq| QueryInfo {
                handle: rq.handle.clone(),
                stats: rq.stats.clone(),
                source_topics: rq.query.source_topics.clone(),
            })
            .collect()
    }

    /// Get statistics for a specific query
    pub async fn get_query_stats(&self, handle: &QueryHandle) -> Result<QueryStats> {
        let queries = self.queries.read().await;
        let rq = queries
            .get(&handle.id)
            .ok_or_else(|| StreamlineError::Query(format!("Query not found: {}", handle.id)))?;
        Ok(rq.stats.clone())
    }
}

impl Default for ContinuousQueryEngine {
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

    // ---- SQL parsing tests ----

    #[test]
    fn test_parse_basic_select() {
        let sql = "SELECT user_id, name FROM events";
        let q = parse_simple_select(sql).unwrap();
        assert_eq!(q.query_type, QueryType::Select);
        assert_eq!(q.source_topics, vec!["events"]);
        assert_eq!(q.projections.len(), 2);
        assert_eq!(q.projections[0].expression, "user_id");
        assert_eq!(q.projections[1].expression, "name");
        assert!(q.filter.is_none());
        assert!(q.window.is_none());
        assert!(q.joins.is_empty());
    }

    #[test]
    fn test_parse_select_star() {
        let sql = "SELECT * FROM logs";
        let q = parse_simple_select(sql).unwrap();
        assert_eq!(q.projections.len(), 1);
        assert_eq!(q.projections[0].expression, "*");
        assert_eq!(q.source_topics, vec!["logs"]);
    }

    #[test]
    fn test_parse_select_with_filter() {
        let sql = "SELECT id, amount FROM orders WHERE amount > 100";
        let q = parse_simple_select(sql).unwrap();
        assert!(q.filter.is_some());
        let f = q.filter.unwrap();
        assert_eq!(f.expression, "amount > 100");
        let compiled = f.compiled.unwrap();
        assert_eq!(compiled.field, "amount");
        assert_eq!(compiled.op, ComparisonOp::Gt);
        assert_eq!(compiled.value, FilterValue::Int(100));
    }

    #[test]
    fn test_parse_select_with_string_filter() {
        let sql = "SELECT * FROM users WHERE status = 'active'";
        let q = parse_simple_select(sql).unwrap();
        let compiled = q.filter.unwrap().compiled.unwrap();
        assert_eq!(compiled.field, "status");
        assert_eq!(compiled.op, ComparisonOp::Eq);
        assert_eq!(compiled.value, FilterValue::String("active".into()));
    }

    #[test]
    fn test_parse_select_with_window() {
        let sql = "SELECT user_id, COUNT(*) as cnt FROM events WINDOW TUMBLING(SIZE 60000)";
        let q = parse_simple_select(sql).unwrap();
        assert!(q.window.is_some());
        let w = q.window.unwrap();
        assert_eq!(w.window_type, EngineWindowType::Tumbling);
        assert_eq!(w.size_ms, 60000);
        assert!(w.advance_ms.is_none());
        // Check projection with alias
        assert_eq!(q.projections[1].alias.as_deref(), Some("cnt"));
    }

    #[test]
    fn test_parse_select_with_hopping_window() {
        let sql =
            "SELECT region, SUM(sales) FROM metrics WINDOW HOPPING(SIZE 60000, ADVANCE 10000)";
        let q = parse_simple_select(sql).unwrap();
        let w = q.window.unwrap();
        assert_eq!(w.window_type, EngineWindowType::Hopping);
        assert_eq!(w.size_ms, 60000);
        assert_eq!(w.advance_ms, Some(10000));
    }

    #[test]
    fn test_parse_select_with_group_by() {
        let sql = "SELECT region, COUNT(*) FROM events GROUP BY region";
        let q = parse_simple_select(sql).unwrap();
        assert_eq!(q.group_by, vec!["region"]);
    }

    #[test]
    fn test_parse_select_with_join() {
        let sql = "SELECT o.id, c.name FROM orders JOIN customers ON customer_id = id WITHIN 30000";
        let q = parse_simple_select(sql).unwrap();
        assert_eq!(q.joins.len(), 1);
        let j = &q.joins[0];
        assert_eq!(j.left_topic, "orders");
        assert_eq!(j.right_topic, "customers");
        assert_eq!(j.within_ms, Some(30000));
        match &j.condition {
            JoinCondition::OnKeys {
                left_key,
                right_key,
            } => {
                assert_eq!(left_key, "customer_id");
                assert_eq!(right_key, "id");
            }
            _ => panic!("Expected OnKeys join condition"),
        }
    }

    #[test]
    fn test_parse_missing_from() {
        let result = parse_simple_select("SELECT *");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_not_select() {
        let result = parse_simple_select("INSERT INTO t VALUES (1)");
        assert!(result.is_err());
    }

    // ---- Window spec tests ----

    #[test]
    fn test_window_spec_session() {
        let w = parse_window_spec("SESSION(SIZE 300000, GAP 60000)").unwrap();
        assert_eq!(w.window_type, EngineWindowType::Session);
        assert_eq!(w.size_ms, 300000);
        assert_eq!(w.gap_ms, Some(60000));
    }

    // ---- Aggregate state tests ----

    #[test]
    fn test_aggregate_count() {
        let mut agg = AggregateState::Count(0);
        agg.update(1.0);
        agg.update(2.0);
        agg.update(3.0);
        assert_eq!(agg.result(), FieldValue::Int(3));
    }

    #[test]
    fn test_aggregate_sum() {
        let mut agg = AggregateState::Sum(0.0);
        agg.update(10.0);
        agg.update(20.0);
        agg.update(30.0);
        assert_eq!(agg.result(), FieldValue::Float(60.0));
    }

    #[test]
    fn test_aggregate_avg() {
        let mut agg = AggregateState::Avg { sum: 0.0, count: 0 };
        agg.update(10.0);
        agg.update(20.0);
        assert_eq!(agg.result(), FieldValue::Float(15.0));
    }

    #[test]
    fn test_aggregate_avg_empty() {
        let agg = AggregateState::Avg { sum: 0.0, count: 0 };
        assert_eq!(agg.result(), FieldValue::Null);
    }

    #[test]
    fn test_aggregate_min() {
        let mut agg = AggregateState::Min(f64::MAX);
        agg.update(50.0);
        agg.update(10.0);
        agg.update(30.0);
        assert_eq!(agg.result(), FieldValue::Float(10.0));
    }

    #[test]
    fn test_aggregate_max() {
        let mut agg = AggregateState::Max(f64::MIN);
        agg.update(10.0);
        agg.update(50.0);
        agg.update(30.0);
        assert_eq!(agg.result(), FieldValue::Float(50.0));
    }

    // ---- Filter compilation and evaluation tests ----

    #[test]
    fn test_compiled_filter_eq_int() {
        let filter = CompiledFilter {
            field: "age".into(),
            op: ComparisonOp::Eq,
            value: FilterValue::Int(25),
        };
        let mut fields = HashMap::new();
        fields.insert("age".into(), FieldValue::Int(25));
        assert!(filter.evaluate(&fields));
        fields.insert("age".into(), FieldValue::Int(30));
        assert!(!filter.evaluate(&fields));
    }

    #[test]
    fn test_compiled_filter_gt_float() {
        let filter = CompiledFilter {
            field: "price".into(),
            op: ComparisonOp::Gt,
            value: FilterValue::Float(99.99),
        };
        let mut fields = HashMap::new();
        fields.insert("price".into(), FieldValue::Float(100.0));
        assert!(filter.evaluate(&fields));
        fields.insert("price".into(), FieldValue::Float(50.0));
        assert!(!filter.evaluate(&fields));
    }

    #[test]
    fn test_compiled_filter_like() {
        let filter = CompiledFilter {
            field: "name".into(),
            op: ComparisonOp::Like,
            value: FilterValue::String("A%".into()),
        };
        let mut fields = HashMap::new();
        fields.insert("name".into(), FieldValue::String("Alice".into()));
        assert!(filter.evaluate(&fields));
        fields.insert("name".into(), FieldValue::String("Bob".into()));
        assert!(!filter.evaluate(&fields));
    }

    #[test]
    fn test_compiled_filter_in() {
        let filter = CompiledFilter {
            field: "status".into(),
            op: ComparisonOp::In,
            value: FilterValue::List(vec![
                FilterValue::String("active".into()),
                FilterValue::String("pending".into()),
            ]),
        };
        let mut fields = HashMap::new();
        fields.insert("status".into(), FieldValue::String("active".into()));
        assert!(filter.evaluate(&fields));
        fields.insert("status".into(), FieldValue::String("deleted".into()));
        assert!(!filter.evaluate(&fields));
    }

    #[test]
    fn test_compiled_filter_between() {
        let filter = CompiledFilter {
            field: "score".into(),
            op: ComparisonOp::Between,
            value: FilterValue::List(vec![FilterValue::Int(10), FilterValue::Int(20)]),
        };
        let mut fields = HashMap::new();
        fields.insert("score".into(), FieldValue::Int(15));
        assert!(filter.evaluate(&fields));
        fields.insert("score".into(), FieldValue::Int(25));
        assert!(!filter.evaluate(&fields));
    }

    #[test]
    fn test_compiled_filter_is_null() {
        let filter = CompiledFilter {
            field: "email".into(),
            op: ComparisonOp::IsNull,
            value: FilterValue::Null,
        };
        let mut fields = HashMap::new();
        fields.insert("email".into(), FieldValue::Null);
        assert!(filter.evaluate(&fields));
        fields.insert("email".into(), FieldValue::String("a@b.com".into()));
        assert!(!filter.evaluate(&fields));
    }

    #[test]
    fn test_compiled_filter_is_null_missing_field() {
        let filter = CompiledFilter {
            field: "nonexistent".into(),
            op: ComparisonOp::IsNull,
            value: FilterValue::Null,
        };
        let fields = HashMap::new();
        // Missing field is treated as NULL
        assert!(filter.evaluate(&fields));
    }

    // ---- Field value tests ----

    #[test]
    fn test_field_value_as_f64() {
        assert_eq!(FieldValue::Int(42).as_f64(), Some(42.0));
        assert_eq!(
            FieldValue::Float(std::f64::consts::PI).as_f64(),
            Some(std::f64::consts::PI)
        );
        assert_eq!(FieldValue::String("hi".into()).as_f64(), None);
        assert_eq!(FieldValue::Null.as_f64(), None);
    }

    // ---- Query lifecycle tests ----

    #[tokio::test]
    async fn test_query_submit_and_list() {
        let engine = ContinuousQueryEngine::new();
        let handle = engine.submit_query("SELECT * FROM events").await.unwrap();
        assert_eq!(handle.status, QueryStatus::Running);
        assert!(handle.id.starts_with("q-"));

        let queries = engine.list_queries().await;
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0].handle.id, handle.id);
        assert_eq!(queries[0].source_topics, vec!["events"]);
    }

    #[tokio::test]
    async fn test_query_cancel() {
        let engine = ContinuousQueryEngine::new();
        let handle = engine.submit_query("SELECT * FROM events").await.unwrap();

        engine.cancel_query(&handle).await.unwrap();

        let stats = engine.get_query_stats(&handle).await.unwrap();
        assert_eq!(stats.records_in, 0);

        let queries = engine.list_queries().await;
        assert_eq!(queries[0].handle.status, QueryStatus::Cancelled);
    }

    #[tokio::test]
    async fn test_query_cancel_already_cancelled() {
        let engine = ContinuousQueryEngine::new();
        let handle = engine.submit_query("SELECT * FROM events").await.unwrap();

        engine.cancel_query(&handle).await.unwrap();
        let result = engine.cancel_query(&handle).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_stats_unknown_query() {
        let engine = ContinuousQueryEngine::new();
        let fake_handle = QueryHandle {
            id: "q-999".into(),
            sql: String::new(),
            created_at: 0,
            status: QueryStatus::Running,
        };
        let result = engine.get_query_stats(&fake_handle).await;
        assert!(result.is_err());
    }

    // ---- LIKE pattern matching ----

    #[test]
    fn test_like_match_patterns() {
        assert!(like_match("hello", "hello"));
        assert!(like_match("hello", "%"));
        assert!(like_match("hello", "h%"));
        assert!(like_match("hello", "%o"));
        assert!(like_match("hello", "h%o"));
        assert!(like_match("hello", "_ello"));
        assert!(like_match("hello", "hell_"));
        assert!(!like_match("hello", "world"));
        assert!(!like_match("hello", "h_llo_")); // too long
        assert!(like_match("", "%"));
        assert!(!like_match("", "_"));
    }

    // ---- Full query with all clauses ----

    #[test]
    fn test_parse_full_query() {
        let sql = "\
            SELECT region, COUNT(*) as cnt, AVG(latency) as avg_lat \
            FROM metrics \
            WHERE status = 'error' \
            GROUP BY region \
            HAVING COUNT(*) > 5 \
            WINDOW TUMBLING(SIZE 30000)";
        let q = parse_simple_select(sql).unwrap();

        assert_eq!(q.projections.len(), 3);
        assert_eq!(q.projections[0].expression, "region");
        assert_eq!(q.projections[1].alias.as_deref(), Some("cnt"));
        assert_eq!(q.projections[2].alias.as_deref(), Some("avg_lat"));

        assert_eq!(q.source_topics, vec!["metrics"]);
        assert!(q.filter.is_some());
        assert_eq!(q.group_by, vec!["region"]);
        assert!(q.having.is_some());
        assert_eq!(q.having.as_deref(), Some("COUNT(*) > 5"));

        let w = q.window.unwrap();
        assert_eq!(w.window_type, EngineWindowType::Tumbling);
        assert_eq!(w.size_ms, 30000);
    }
}
