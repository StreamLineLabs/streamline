//! Breakpoint engine for the streaming debugger.
//!
//! Provides a rich condition-based breakpoint system with composite conditions,
//! multiple actions, hit counting, rate limiting, and a centralized manager.

use crate::storage::Record;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

static BREAKPOINT_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

fn next_breakpoint_id() -> u64 {
    BREAKPOINT_ID_COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// Logical operator for combining conditions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum LogicalOp {
    And,
    Or,
}

/// A composable condition that determines whether a breakpoint fires.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BreakpointCondition {
    /// Match records whose key matches a regex pattern.
    OnKey { pattern: String },
    /// Match records whose value matches a regex pattern.
    OnValue { pattern: String },
    /// Match records that have a header with the given key (and optional value pattern).
    OnHeader { key: String, value: Option<String> },
    /// Match records in a specific partition.
    OnPartition { id: i32 },
    /// Match records whose offset falls within an inclusive range.
    OnOffset { start: i64, end: i64 },
    /// Match records whose timestamp falls within an inclusive range (ms since epoch).
    OnTimestamp { start_ms: i64, end_ms: i64 },
    /// Match records whose value is valid JSON and the given path equals the expected value.
    JsonField { path: String, value: String },
    /// Match records whose total size is within a threshold.
    SizeThreshold {
        min_bytes: Option<usize>,
        max_bytes: Option<usize>,
    },
    /// Combine multiple conditions with a logical operator.
    Composite {
        conditions: Vec<BreakpointCondition>,
        op: LogicalOp,
    },
}

/// Evaluate a [`BreakpointCondition`] against a record and its partition context.
pub fn evaluate_condition(
    condition: &BreakpointCondition,
    record: &Record,
    partition: i32,
) -> bool {
    match condition {
        BreakpointCondition::OnKey { pattern } => {
            let Some(key) = record.key.as_ref() else {
                return false;
            };
            let Ok(key_str) = std::str::from_utf8(key) else {
                return false;
            };
            Regex::new(pattern)
                .map(|re| re.is_match(key_str))
                .unwrap_or(false)
        }
        BreakpointCondition::OnValue { pattern } => {
            let Ok(val_str) = std::str::from_utf8(&record.value) else {
                return false;
            };
            Regex::new(pattern)
                .map(|re| re.is_match(val_str))
                .unwrap_or(false)
        }
        BreakpointCondition::OnHeader { key, value } => record.headers.iter().any(|h| {
            if h.key != *key {
                return false;
            }
            match value {
                Some(expected) => {
                    let Ok(hv) = std::str::from_utf8(&h.value) else {
                        return false;
                    };
                    Regex::new(expected)
                        .map(|re| re.is_match(hv))
                        .unwrap_or(false)
                }
                None => true,
            }
        }),
        BreakpointCondition::OnPartition { id } => partition == *id,
        BreakpointCondition::OnOffset { start, end } => {
            record.offset >= *start && record.offset <= *end
        }
        BreakpointCondition::OnTimestamp { start_ms, end_ms } => {
            record.timestamp >= *start_ms && record.timestamp <= *end_ms
        }
        BreakpointCondition::JsonField {
            path,
            value: expected,
        } => {
            let Ok(json) = serde_json::from_slice::<serde_json::Value>(&record.value) else {
                return false;
            };
            let mut current = &json;
            for part in path.split('.') {
                match current.get(part) {
                    Some(next) => current = next,
                    None => return false,
                }
            }
            current.to_string().trim_matches('"') == expected
        }
        BreakpointCondition::SizeThreshold {
            min_bytes,
            max_bytes,
        } => {
            let size = record.value.len() + record.key.as_ref().map(|k| k.len()).unwrap_or(0);
            let above_min = min_bytes.map(|m| size >= m).unwrap_or(true);
            let below_max = max_bytes.map(|m| size <= m).unwrap_or(true);
            above_min && below_max
        }
        BreakpointCondition::Composite { conditions, op } => match op {
            LogicalOp::And => conditions
                .iter()
                .all(|c| evaluate_condition(c, record, partition)),
            LogicalOp::Or => conditions
                .iter()
                .any(|c| evaluate_condition(c, record, partition)),
        },
    }
}

/// Action to take when a breakpoint fires.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BreakpointAction {
    /// Pause execution at this message (default).
    #[default]
    Pause,
    /// Log the match and continue.
    Log,
    /// Count occurrences without stopping.
    Count,
    /// Capture a snapshot of the record and surrounding context.
    Snapshot,
    /// Emit an alert with the given message.
    Alert(String),
}

/// A breakpoint that triggers when a condition is met on a stream event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Breakpoint {
    pub id: u64,
    pub name: String,
    pub enabled: bool,
    pub breakpoint_type: BreakpointType,
    #[serde(default)]
    pub condition: Option<BreakpointCondition>,
    pub action: BreakpointAction,
    #[serde(default)]
    pub hit_count: u64,
}

/// Legacy breakpoint types (retained for backward compatibility).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BreakpointType {
    Pattern(PatternBreakpoint),
    Offset {
        offset: i64,
    },
    JsonField {
        path: String,
        value: String,
    },
    KeyMatch {
        pattern: String,
    },
    Header {
        name: String,
        value: Option<String>,
    },
    SizeThreshold {
        min_bytes: Option<usize>,
        max_bytes: Option<usize>,
    },
    TimeRange {
        start_ms: Option<i64>,
        end_ms: Option<i64>,
    },
    ConditionOnly,
}

/// Regex-based pattern breakpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PatternBreakpoint {
    pub pattern: String,
    #[serde(default)]
    pub case_insensitive: bool,
    #[serde(default)]
    pub search_keys: bool,
}

/// Result of evaluating a breakpoint against a message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BreakpointMatch {
    pub breakpoint_id: u64,
    pub breakpoint_name: String,
    pub matched_content: String,
    pub offset: i64,
    pub partition: i32,
    pub action: BreakpointAction,
    pub hit_count: u64,
}

impl Breakpoint {
    /// Create a pattern breakpoint (legacy convenience).
    pub fn pattern(pattern: impl Into<String>) -> Self {
        let id = next_breakpoint_id();
        Self {
            id,
            name: format!("pattern-{id}"),
            enabled: true,
            breakpoint_type: BreakpointType::Pattern(PatternBreakpoint {
                pattern: pattern.into(),
                case_insensitive: false,
                search_keys: false,
            }),
            condition: None,
            action: BreakpointAction::Pause,
            hit_count: 0,
        }
    }

    /// Create an offset breakpoint (legacy convenience).
    pub fn at_offset(offset: i64) -> Self {
        let id = next_breakpoint_id();
        Self {
            id,
            name: format!("offset-{offset}"),
            enabled: true,
            breakpoint_type: BreakpointType::Offset { offset },
            condition: None,
            action: BreakpointAction::Pause,
            hit_count: 0,
        }
    }

    /// Create a JSON field breakpoint (legacy convenience).
    pub fn json_field(path: impl Into<String>, value: impl Into<String>) -> Self {
        let id = next_breakpoint_id();
        Self {
            id,
            name: format!("json-{id}"),
            enabled: true,
            breakpoint_type: BreakpointType::JsonField {
                path: path.into(),
                value: value.into(),
            },
            condition: None,
            action: BreakpointAction::Pause,
            hit_count: 0,
        }
    }

    /// Create a breakpoint from a rich [`BreakpointCondition`].
    pub fn from_condition(name: impl Into<String>, condition: BreakpointCondition) -> Self {
        let id = next_breakpoint_id();
        Self {
            id,
            name: name.into(),
            enabled: true,
            breakpoint_type: BreakpointType::ConditionOnly,
            condition: Some(condition),
            action: BreakpointAction::Pause,
            hit_count: 0,
        }
    }

    /// Set the action for this breakpoint (builder-style).
    pub fn with_action(mut self, action: BreakpointAction) -> Self {
        self.action = action;
        self
    }

    /// Check if this breakpoint matches using the legacy interface.
    pub fn matches(&self, offset: i64, key: Option<&[u8]>, value: Option<&[u8]>) -> bool {
        if !self.enabled {
            return false;
        }
        match &self.breakpoint_type {
            BreakpointType::Pattern(p) => {
                let regex = if p.case_insensitive {
                    Regex::new(&format!("(?i){}", p.pattern)).ok()
                } else {
                    Regex::new(&p.pattern).ok()
                };
                if let Some(re) = regex {
                    if let Some(v) = value {
                        if let Ok(s) = std::str::from_utf8(v) {
                            if re.is_match(s) {
                                return true;
                            }
                        }
                    }
                    if p.search_keys {
                        if let Some(k) = key {
                            if let Ok(s) = std::str::from_utf8(k) {
                                if re.is_match(s) {
                                    return true;
                                }
                            }
                        }
                    }
                }
                false
            }
            BreakpointType::Offset { offset: target } => offset == *target,
            BreakpointType::JsonField {
                path,
                value: expected,
            } => {
                if let Some(v) = value {
                    if let Ok(json) = serde_json::from_slice::<serde_json::Value>(v) {
                        let mut current = &json;
                        for part in path.split('.') {
                            match current.get(part) {
                                Some(next) => current = next,
                                None => return false,
                            }
                        }
                        return current.to_string().trim_matches('"') == expected;
                    }
                }
                false
            }
            BreakpointType::KeyMatch { pattern } => {
                if let Some(k) = key {
                    if let Ok(s) = std::str::from_utf8(k) {
                        return Regex::new(pattern)
                            .map(|re| re.is_match(s))
                            .unwrap_or(false);
                    }
                }
                false
            }
            BreakpointType::Header { .. } => false,
            BreakpointType::SizeThreshold {
                min_bytes,
                max_bytes,
            } => {
                let size = value.map(|v| v.len()).unwrap_or(0);
                let above_min = min_bytes.map(|m| size >= m).unwrap_or(true);
                let below_max = max_bytes.map(|m| size <= m).unwrap_or(true);
                above_min && below_max
            }
            BreakpointType::TimeRange { .. } => false,
            BreakpointType::ConditionOnly => false,
        }
    }

    /// Evaluate this breakpoint against a full [`Record`] with partition context.
    pub fn evaluate(&self, record: &Record, partition: i32) -> bool {
        if !self.enabled {
            return false;
        }
        if let Some(cond) = &self.condition {
            return evaluate_condition(cond, record, partition);
        }
        let key_bytes: Option<&[u8]> = record.key.as_deref();
        let value_bytes: &[u8] = &record.value;
        self.matches(record.offset, key_bytes, Some(value_bytes))
    }
}

#[derive(Debug)]
struct RateLimiterState {
    window_start: Instant,
    hits_in_window: u64,
}

/// Centralized manager for breakpoints with enable/disable/remove,
/// hit counting, and optional rate limiting.
pub struct BreakpointManager {
    breakpoints: Vec<Breakpoint>,
    max_hits_per_second: u64,
    rate_state: HashMap<u64, RateLimiterState>,
}

impl std::fmt::Debug for BreakpointManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BreakpointManager")
            .field("breakpoints", &self.breakpoints)
            .field("max_hits_per_second", &self.max_hits_per_second)
            .finish()
    }
}

impl BreakpointManager {
    pub fn new() -> Self {
        Self {
            breakpoints: Vec::new(),
            max_hits_per_second: 0,
            rate_state: HashMap::new(),
        }
    }

    pub fn with_rate_limit(max_hits_per_second: u64) -> Self {
        Self {
            breakpoints: Vec::new(),
            max_hits_per_second,
            rate_state: HashMap::new(),
        }
    }

    pub fn add(&mut self, breakpoint: Breakpoint) -> u64 {
        let id = breakpoint.id;
        self.breakpoints.push(breakpoint);
        id
    }

    pub fn remove(&mut self, id: u64) -> bool {
        let before = self.breakpoints.len();
        self.breakpoints.retain(|bp| bp.id != id);
        self.rate_state.remove(&id);
        self.breakpoints.len() < before
    }

    pub fn enable(&mut self, id: u64) -> bool {
        if let Some(bp) = self.breakpoints.iter_mut().find(|bp| bp.id == id) {
            bp.enabled = true;
            return true;
        }
        false
    }

    pub fn disable(&mut self, id: u64) -> bool {
        if let Some(bp) = self.breakpoints.iter_mut().find(|bp| bp.id == id) {
            bp.enabled = false;
            return true;
        }
        false
    }

    pub fn get(&self, id: u64) -> Option<&Breakpoint> {
        self.breakpoints.iter().find(|bp| bp.id == id)
    }

    pub fn list(&self) -> &[Breakpoint] {
        &self.breakpoints
    }

    pub fn reset_counters(&mut self) {
        for bp in &mut self.breakpoints {
            bp.hit_count = 0;
        }
        self.rate_state.clear();
    }

    /// Evaluate all enabled breakpoints against a record.
    pub fn evaluate_all(&mut self, record: &Record, partition: i32) -> Vec<BreakpointMatch> {
        let now = Instant::now();
        let mut matches = Vec::new();
        for bp in &mut self.breakpoints {
            if !bp.evaluate(record, partition) {
                continue;
            }
            if self.max_hits_per_second > 0 {
                let state = self.rate_state.entry(bp.id).or_insert(RateLimiterState {
                    window_start: now,
                    hits_in_window: 0,
                });
                let elapsed = now.duration_since(state.window_start);
                if elapsed.as_secs() >= 1 {
                    state.window_start = now;
                    state.hits_in_window = 0;
                }
                if state.hits_in_window >= self.max_hits_per_second {
                    continue;
                }
                state.hits_in_window += 1;
            }
            bp.hit_count += 1;
            let content = std::str::from_utf8(&record.value)
                .map(|s| s.to_string())
                .unwrap_or_else(|_| format!("<binary {} bytes>", record.value.len()));
            matches.push(BreakpointMatch {
                breakpoint_id: bp.id,
                breakpoint_name: bp.name.clone(),
                matched_content: content,
                offset: record.offset,
                partition,
                action: bp.action.clone(),
                hit_count: bp.hit_count,
            });
        }
        matches
    }
}

impl Default for BreakpointManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::record::Header;
    use bytes::Bytes;

    fn make_record(offset: i64, key: Option<&str>, value: &[u8]) -> Record {
        Record {
            offset,
            timestamp: 1000,
            key: key.map(|k| Bytes::from(k.to_owned())),
            value: Bytes::from(value.to_vec()),
            headers: Vec::new(),
            crc: None,
        }
    }

    fn make_record_with_headers(offset: i64, value: &[u8], headers: Vec<(&str, &[u8])>) -> Record {
        Record {
            offset,
            timestamp: 1500,
            key: None,
            value: Bytes::from(value.to_vec()),
            headers: headers
                .into_iter()
                .map(|(k, v)| Header {
                    key: k.to_string(),
                    value: Bytes::from(v.to_vec()),
                })
                .collect(),
            crc: None,
        }
    }

    #[test]
    fn test_pattern_breakpoint() {
        let bp = Breakpoint::pattern("error");
        assert!(bp.matches(0, None, Some(b"an error occurred")));
        assert!(!bp.matches(0, None, Some(b"all good")));
    }

    #[test]
    fn test_offset_breakpoint() {
        let bp = Breakpoint::at_offset(42);
        assert!(bp.matches(42, None, None));
        assert!(!bp.matches(41, None, None));
    }

    #[test]
    fn test_json_field_breakpoint() {
        let bp = Breakpoint::json_field("status", "error");
        assert!(bp.matches(0, None, Some(br#"{"status":"error","msg":"fail"}"#)));
        assert!(!bp.matches(0, None, Some(br#"{"status":"ok"}"#)));
    }

    #[test]
    fn test_disabled_breakpoint() {
        let mut bp = Breakpoint::pattern("error");
        bp.enabled = false;
        assert!(!bp.matches(0, None, Some(b"an error occurred")));
    }

    #[test]
    fn test_size_threshold_breakpoint() {
        let bp = Breakpoint {
            id: 1,
            name: "size".to_string(),
            enabled: true,
            breakpoint_type: BreakpointType::SizeThreshold {
                min_bytes: Some(10),
                max_bytes: None,
            },
            condition: None,
            action: BreakpointAction::Pause,
            hit_count: 0,
        };
        assert!(!bp.matches(0, None, Some(b"short")));
        assert!(bp.matches(0, None, Some(b"this is a longer message")));
    }

    #[test]
    fn test_condition_on_key() {
        let cond = BreakpointCondition::OnKey {
            pattern: "^user-".to_string(),
        };
        let record = make_record(0, Some("user-123"), b"hello");
        assert!(evaluate_condition(&cond, &record, 0));
        let record2 = make_record(0, Some("order-456"), b"hello");
        assert!(!evaluate_condition(&cond, &record2, 0));
    }

    #[test]
    fn test_condition_on_value() {
        let cond = BreakpointCondition::OnValue {
            pattern: "error".to_string(),
        };
        let record = make_record(0, None, b"an error occurred");
        assert!(evaluate_condition(&cond, &record, 0));
        let record2 = make_record(0, None, b"all good");
        assert!(!evaluate_condition(&cond, &record2, 0));
    }

    #[test]
    fn test_condition_on_header() {
        let cond = BreakpointCondition::OnHeader {
            key: "content-type".to_string(),
            value: Some("json".to_string()),
        };
        let record =
            make_record_with_headers(0, b"data", vec![("content-type", b"application/json")]);
        assert!(evaluate_condition(&cond, &record, 0));
        let record2 = make_record_with_headers(0, b"data", vec![("content-type", b"text/plain")]);
        assert!(!evaluate_condition(&cond, &record2, 0));
    }

    #[test]
    fn test_condition_on_partition() {
        let cond = BreakpointCondition::OnPartition { id: 3 };
        let record = make_record(0, None, b"data");
        assert!(evaluate_condition(&cond, &record, 3));
        assert!(!evaluate_condition(&cond, &record, 2));
    }

    #[test]
    fn test_condition_on_offset_range() {
        let cond = BreakpointCondition::OnOffset { start: 10, end: 20 };
        assert!(evaluate_condition(
            &cond,
            &make_record(15, None, b"data"),
            0
        ));
        assert!(!evaluate_condition(
            &cond,
            &make_record(25, None, b"data"),
            0
        ));
        assert!(evaluate_condition(
            &cond,
            &make_record(10, None, b"data"),
            0
        ));
        assert!(evaluate_condition(
            &cond,
            &make_record(20, None, b"data"),
            0
        ));
    }

    #[test]
    fn test_condition_on_timestamp_range() {
        let cond = BreakpointCondition::OnTimestamp {
            start_ms: 1000,
            end_ms: 2000,
        };
        let record = make_record(0, None, b"data");
        assert!(evaluate_condition(&cond, &record, 0));
        let mut r_late = make_record(0, None, b"data");
        r_late.timestamp = 3000;
        assert!(!evaluate_condition(&cond, &r_late, 0));
    }

    #[test]
    fn test_composite_and() {
        let cond = BreakpointCondition::Composite {
            conditions: vec![
                BreakpointCondition::OnPartition { id: 0 },
                BreakpointCondition::OnValue {
                    pattern: "error".to_string(),
                },
            ],
            op: LogicalOp::And,
        };
        let record = make_record(0, None, b"an error occurred");
        assert!(evaluate_condition(&cond, &record, 0));
        assert!(!evaluate_condition(&cond, &record, 1));
    }

    #[test]
    fn test_composite_or() {
        let cond = BreakpointCondition::Composite {
            conditions: vec![
                BreakpointCondition::OnPartition { id: 0 },
                BreakpointCondition::OnPartition { id: 1 },
            ],
            op: LogicalOp::Or,
        };
        let record = make_record(0, None, b"data");
        assert!(evaluate_condition(&cond, &record, 0));
        assert!(evaluate_condition(&cond, &record, 1));
        assert!(!evaluate_condition(&cond, &record, 2));
    }

    #[test]
    fn test_breakpoint_from_condition() {
        let bp = Breakpoint::from_condition(
            "error-on-p0",
            BreakpointCondition::Composite {
                conditions: vec![
                    BreakpointCondition::OnPartition { id: 0 },
                    BreakpointCondition::OnValue {
                        pattern: "error".to_string(),
                    },
                ],
                op: LogicalOp::And,
            },
        );
        let record = make_record(0, None, b"an error occurred");
        assert!(bp.evaluate(&record, 0));
        assert!(!bp.evaluate(&record, 1));
    }

    #[test]
    fn test_breakpoint_with_action() {
        let bp = Breakpoint::pattern("test").with_action(BreakpointAction::Log);
        assert!(matches!(bp.action, BreakpointAction::Log));
    }

    #[test]
    fn test_manager_add_remove() {
        let mut mgr = BreakpointManager::new();
        let id = mgr.add(Breakpoint::pattern("test"));
        assert_eq!(mgr.list().len(), 1);
        assert!(mgr.remove(id));
        assert!(mgr.list().is_empty());
        assert!(!mgr.remove(id));
    }

    #[test]
    fn test_manager_enable_disable() {
        let mut mgr = BreakpointManager::new();
        let id = mgr.add(Breakpoint::pattern("test"));
        assert!(mgr.disable(id));
        assert!(!mgr.get(id).expect("bp exists").enabled);
        assert!(mgr.enable(id));
        assert!(mgr.get(id).expect("bp exists").enabled);
    }

    #[test]
    fn test_manager_evaluate_all() {
        let mut mgr = BreakpointManager::new();
        mgr.add(Breakpoint::from_condition(
            "errors",
            BreakpointCondition::OnValue {
                pattern: "error".to_string(),
            },
        ));
        mgr.add(Breakpoint::from_condition(
            "partition-0",
            BreakpointCondition::OnPartition { id: 0 },
        ));
        let record = make_record(0, None, b"an error occurred");
        let hits = mgr.evaluate_all(&record, 0);
        assert_eq!(hits.len(), 2);
        let record2 = make_record(0, None, b"all good");
        let hits2 = mgr.evaluate_all(&record2, 0);
        assert_eq!(hits2.len(), 1);
    }

    #[test]
    fn test_manager_hit_counting() {
        let mut mgr = BreakpointManager::new();
        let id = mgr.add(Breakpoint::from_condition(
            "counter",
            BreakpointCondition::OnValue {
                pattern: "hit".to_string(),
            },
        ));
        let record = make_record(0, None, b"hit me");
        mgr.evaluate_all(&record, 0);
        mgr.evaluate_all(&record, 0);
        mgr.evaluate_all(&record, 0);
        assert_eq!(mgr.get(id).expect("bp exists").hit_count, 3);
    }

    #[test]
    fn test_manager_rate_limiting() {
        let mut mgr = BreakpointManager::with_rate_limit(2);
        mgr.add(Breakpoint::from_condition(
            "limited",
            BreakpointCondition::OnValue {
                pattern: "hit".to_string(),
            },
        ));
        let record = make_record(0, None, b"hit me");
        let h1 = mgr.evaluate_all(&record, 0);
        let h2 = mgr.evaluate_all(&record, 0);
        let h3 = mgr.evaluate_all(&record, 0);
        assert_eq!(h1.len(), 1);
        assert_eq!(h2.len(), 1);
        assert_eq!(h3.len(), 0);
    }

    #[test]
    fn test_manager_reset_counters() {
        let mut mgr = BreakpointManager::new();
        let id = mgr.add(Breakpoint::from_condition(
            "counter",
            BreakpointCondition::OnValue {
                pattern: "hit".to_string(),
            },
        ));
        let record = make_record(0, None, b"hit me");
        mgr.evaluate_all(&record, 0);
        assert_eq!(mgr.get(id).expect("bp exists").hit_count, 1);
        mgr.reset_counters();
        assert_eq!(mgr.get(id).expect("bp exists").hit_count, 0);
    }

    #[test]
    fn test_nested_composite() {
        let cond = BreakpointCondition::Composite {
            conditions: vec![
                BreakpointCondition::OnPartition { id: 0 },
                BreakpointCondition::Composite {
                    conditions: vec![
                        BreakpointCondition::OnValue {
                            pattern: "error".to_string(),
                        },
                        BreakpointCondition::OnValue {
                            pattern: "warning".to_string(),
                        },
                    ],
                    op: LogicalOp::Or,
                },
            ],
            op: LogicalOp::And,
        };
        let error_rec = make_record(0, None, b"an error occurred");
        assert!(evaluate_condition(&cond, &error_rec, 0));
        assert!(!evaluate_condition(&cond, &error_rec, 1));
        let warn_rec = make_record(0, None, b"a warning appeared");
        assert!(evaluate_condition(&cond, &warn_rec, 0));
        let info_rec = make_record(0, None, b"info message");
        assert!(!evaluate_condition(&cond, &info_rec, 0));
    }
}
