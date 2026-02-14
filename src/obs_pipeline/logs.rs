//! Log Aggregation
//!
//! Provides log collection and aggregation compatible with OpenTelemetry Logs.

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};

/// Log severity level
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
pub enum LogLevel {
    /// Trace level (most verbose)
    Trace = 1,
    /// Debug level
    Debug = 5,
    /// Info level
    #[default]
    Info = 9,
    /// Warning level
    Warn = 13,
    /// Error level
    Error = 17,
    /// Fatal level
    Fatal = 21,
}

impl LogLevel {
    /// Parse from string (convenience method)
    pub fn parse(s: &str) -> Option<Self> {
        s.parse().ok()
    }

    /// Convert to string
    pub fn as_str(&self) -> &'static str {
        match self {
            LogLevel::Trace => "TRACE",
            LogLevel::Debug => "DEBUG",
            LogLevel::Info => "INFO",
            LogLevel::Warn => "WARN",
            LogLevel::Error => "ERROR",
            LogLevel::Fatal => "FATAL",
        }
    }
}

impl std::str::FromStr for LogLevel {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "trace" => Ok(LogLevel::Trace),
            "debug" => Ok(LogLevel::Debug),
            "info" => Ok(LogLevel::Info),
            "warn" | "warning" => Ok(LogLevel::Warn),
            "error" => Ok(LogLevel::Error),
            "fatal" => Ok(LogLevel::Fatal),
            _ => Err(()),
        }
    }
}

/// Log source information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogSource {
    /// Service name
    pub service: String,
    /// Component/module name
    pub component: Option<String>,
    /// File name
    pub file: Option<String>,
    /// Line number
    pub line: Option<u32>,
    /// Function name
    pub function: Option<String>,
}

impl LogSource {
    /// Create a new log source
    pub fn new(service: impl Into<String>) -> Self {
        Self {
            service: service.into(),
            component: None,
            file: None,
            line: None,
            function: None,
        }
    }

    /// Set component
    pub fn with_component(mut self, component: impl Into<String>) -> Self {
        self.component = Some(component.into());
        self
    }

    /// Set file location
    pub fn with_location(mut self, file: &str, line: u32) -> Self {
        self.file = Some(file.to_string());
        self.line = Some(line);
        self
    }
}

/// Log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    /// Timestamp (Unix ms)
    pub timestamp: i64,
    /// Observed timestamp (when log was collected)
    pub observed_timestamp: i64,
    /// Severity level
    pub level: LogLevel,
    /// Log message body
    pub body: String,
    /// Attributes/fields
    pub attributes: HashMap<String, String>,
    /// Trace context (if available)
    pub trace_id: Option<String>,
    /// Span ID (if available)
    pub span_id: Option<String>,
    /// Resource/source information
    pub source: Option<LogSource>,
}

impl LogEntry {
    /// Create a new log entry
    pub fn new(level: LogLevel, body: impl Into<String>) -> Self {
        let now = chrono::Utc::now().timestamp_millis();
        Self {
            timestamp: now,
            observed_timestamp: now,
            level,
            body: body.into(),
            attributes: HashMap::new(),
            trace_id: None,
            span_id: None,
            source: None,
        }
    }

    /// Set timestamp
    pub fn with_timestamp(mut self, timestamp: i64) -> Self {
        self.timestamp = timestamp;
        self
    }

    /// Add attributes
    pub fn with_attributes(mut self, attributes: HashMap<String, String>) -> Self {
        self.attributes = attributes;
        self
    }

    /// Add single attribute
    pub fn with_attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.attributes.insert(key.into(), value.into());
        self
    }

    /// Set trace context
    pub fn with_trace_context(mut self, trace_id: &str, span_id: &str) -> Self {
        self.trace_id = Some(trace_id.to_string());
        self.span_id = Some(span_id.to_string());
        self
    }

    /// Set source
    pub fn with_source(mut self, source: LogSource) -> Self {
        self.source = Some(source);
        self
    }

    /// Check if entry matches a level filter
    pub fn matches_level(&self, min_level: LogLevel) -> bool {
        self.level >= min_level
    }
}

/// Log query for filtering
#[derive(Debug, Clone, Default)]
pub struct LogQuery {
    /// Minimum severity level
    pub min_level: Option<LogLevel>,
    /// Maximum severity level
    pub max_level: Option<LogLevel>,
    /// Time range start
    pub start_time: Option<i64>,
    /// Time range end
    pub end_time: Option<i64>,
    /// Text search in body
    pub body_contains: Option<String>,
    /// Attribute filters
    pub attribute_filters: HashMap<String, String>,
    /// Trace ID filter
    pub trace_id: Option<String>,
    /// Service filter
    pub service: Option<String>,
    /// Maximum results
    pub limit: Option<usize>,
}

impl LogQuery {
    /// Create a new query
    pub fn new() -> Self {
        Self::default()
    }

    /// Filter by minimum level
    pub fn min_level(mut self, level: LogLevel) -> Self {
        self.min_level = Some(level);
        self
    }

    /// Filter by time range
    pub fn time_range(mut self, start: i64, end: i64) -> Self {
        self.start_time = Some(start);
        self.end_time = Some(end);
        self
    }

    /// Search body text
    pub fn body_contains(mut self, text: impl Into<String>) -> Self {
        self.body_contains = Some(text.into());
        self
    }

    /// Filter by attribute
    pub fn attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.attribute_filters.insert(key.into(), value.into());
        self
    }

    /// Filter by trace ID
    pub fn trace_id(mut self, trace_id: impl Into<String>) -> Self {
        self.trace_id = Some(trace_id.into());
        self
    }

    /// Set result limit
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Check if entry matches query
    pub fn matches(&self, entry: &LogEntry) -> bool {
        // Level filter
        if let Some(min) = &self.min_level {
            if entry.level < *min {
                return false;
            }
        }
        if let Some(max) = &self.max_level {
            if entry.level > *max {
                return false;
            }
        }

        // Time range
        if let Some(start) = self.start_time {
            if entry.timestamp < start {
                return false;
            }
        }
        if let Some(end) = self.end_time {
            if entry.timestamp > end {
                return false;
            }
        }

        // Body search
        if let Some(search) = &self.body_contains {
            if !entry.body.to_lowercase().contains(&search.to_lowercase()) {
                return false;
            }
        }

        // Attribute filters
        for (key, value) in &self.attribute_filters {
            if entry.attributes.get(key) != Some(value) {
                return false;
            }
        }

        // Trace ID
        if let Some(trace_id) = &self.trace_id {
            if entry.trace_id.as_ref() != Some(trace_id) {
                return false;
            }
        }

        // Service
        if let Some(service) = &self.service {
            if let Some(source) = &entry.source {
                if &source.service != service {
                    return false;
                }
            } else {
                return false;
            }
        }

        true
    }
}

/// Log aggregator
pub struct LogAggregator {
    /// Log entries (ring buffer)
    entries: VecDeque<LogEntry>,
    /// Maximum entries to keep
    max_entries: usize,
    /// Retention in days
    retention_days: u32,
    /// Minimum level to store
    min_level: LogLevel,
}

impl LogAggregator {
    /// Create a new aggregator
    pub fn new(retention_days: u32) -> Self {
        Self {
            entries: VecDeque::new(),
            max_entries: 100_000,
            retention_days,
            min_level: LogLevel::Debug,
        }
    }

    /// Set minimum level
    pub fn with_min_level(mut self, level: LogLevel) -> Self {
        self.min_level = level;
        self
    }

    /// Set max entries
    pub fn with_max_entries(mut self, max: usize) -> Self {
        self.max_entries = max;
        self
    }

    /// Add a log entry
    pub fn add(&mut self, entry: LogEntry) {
        if entry.level < self.min_level {
            return;
        }

        self.entries.push_back(entry);

        // Enforce max entries
        while self.entries.len() > self.max_entries {
            self.entries.pop_front();
        }
    }

    /// Query logs
    pub fn query(&self, query: &LogQuery) -> Vec<LogEntry> {
        let mut results: Vec<LogEntry> = self
            .entries
            .iter()
            .filter(|e| query.matches(e))
            .cloned()
            .collect();

        // Apply limit
        if let Some(limit) = query.limit {
            results.truncate(limit);
        }

        results
    }

    /// Get recent logs
    pub fn recent(&self, count: usize) -> Vec<LogEntry> {
        self.entries.iter().rev().take(count).cloned().collect()
    }

    /// Get logs by level
    pub fn by_level(&self, level: LogLevel) -> Vec<LogEntry> {
        self.entries
            .iter()
            .filter(|e| e.level == level)
            .cloned()
            .collect()
    }

    /// Get error logs
    pub fn errors(&self) -> Vec<LogEntry> {
        self.entries
            .iter()
            .filter(|e| e.level >= LogLevel::Error)
            .cloned()
            .collect()
    }

    /// Get logs for a trace
    pub fn for_trace(&self, trace_id: &str) -> Vec<LogEntry> {
        self.entries
            .iter()
            .filter(|e| e.trace_id.as_deref() == Some(trace_id))
            .cloned()
            .collect()
    }

    /// Count entries
    pub fn count(&self) -> usize {
        self.entries.len()
    }

    /// Count by level
    pub fn count_by_level(&self) -> HashMap<LogLevel, usize> {
        let mut counts = HashMap::new();
        for entry in &self.entries {
            *counts.entry(entry.level).or_default() += 1;
        }
        counts
    }

    /// Clear expired entries
    pub fn cleanup(&mut self) {
        let cutoff = chrono::Utc::now().timestamp_millis()
            - (self.retention_days as i64 * 24 * 60 * 60 * 1000);

        self.entries.retain(|e| e.timestamp > cutoff);
    }

    /// Clear all entries
    pub fn clear(&mut self) {
        self.entries.clear();
    }
}

impl Default for LogAggregator {
    fn default() -> Self {
        Self::new(7)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_level_ordering() {
        assert!(LogLevel::Error > LogLevel::Warn);
        assert!(LogLevel::Warn > LogLevel::Info);
        assert!(LogLevel::Info > LogLevel::Debug);
        assert!(LogLevel::Debug > LogLevel::Trace);
    }

    #[test]
    fn test_log_level_from_str() {
        assert_eq!(LogLevel::parse("info"), Some(LogLevel::Info));
        assert_eq!(LogLevel::parse("INFO"), Some(LogLevel::Info));
        assert_eq!(LogLevel::parse("warn"), Some(LogLevel::Warn));
        assert_eq!(LogLevel::parse("warning"), Some(LogLevel::Warn));
        assert_eq!(LogLevel::parse("invalid"), None);
    }

    #[test]
    fn test_log_entry_creation() {
        let entry = LogEntry::new(LogLevel::Info, "Test message").with_attribute("user_id", "123");

        assert_eq!(entry.level, LogLevel::Info);
        assert_eq!(entry.body, "Test message");
        assert_eq!(entry.attributes.get("user_id"), Some(&"123".to_string()));
    }

    #[test]
    fn test_log_entry_with_trace_context() {
        let entry = LogEntry::new(LogLevel::Info, "Test").with_trace_context("abc123", "def456");

        assert_eq!(entry.trace_id, Some("abc123".to_string()));
        assert_eq!(entry.span_id, Some("def456".to_string()));
    }

    #[test]
    fn test_log_query_level_filter() {
        let entry_info = LogEntry::new(LogLevel::Info, "Info message");
        let entry_error = LogEntry::new(LogLevel::Error, "Error message");

        let query = LogQuery::new().min_level(LogLevel::Warn);

        assert!(!query.matches(&entry_info));
        assert!(query.matches(&entry_error));
    }

    #[test]
    fn test_log_query_body_search() {
        let entry = LogEntry::new(LogLevel::Info, "User login successful");

        let query_match = LogQuery::new().body_contains("login");
        let query_no_match = LogQuery::new().body_contains("logout");

        assert!(query_match.matches(&entry));
        assert!(!query_no_match.matches(&entry));
    }

    #[test]
    fn test_log_query_attribute_filter() {
        let entry = LogEntry::new(LogLevel::Info, "Test").with_attribute("user_id", "123");

        let query_match = LogQuery::new().attribute("user_id", "123");
        let query_no_match = LogQuery::new().attribute("user_id", "456");

        assert!(query_match.matches(&entry));
        assert!(!query_no_match.matches(&entry));
    }

    #[test]
    fn test_log_aggregator_add() {
        let mut aggregator = LogAggregator::new(7);

        aggregator.add(LogEntry::new(LogLevel::Info, "Test 1"));
        aggregator.add(LogEntry::new(LogLevel::Error, "Test 2"));

        assert_eq!(aggregator.count(), 2);
    }

    #[test]
    fn test_log_aggregator_min_level() {
        let mut aggregator = LogAggregator::new(7).with_min_level(LogLevel::Warn);

        aggregator.add(LogEntry::new(LogLevel::Info, "Should not be stored"));
        aggregator.add(LogEntry::new(LogLevel::Error, "Should be stored"));

        assert_eq!(aggregator.count(), 1);
    }

    #[test]
    fn test_log_aggregator_query() {
        let mut aggregator = LogAggregator::new(7);

        aggregator.add(LogEntry::new(LogLevel::Info, "Info message"));
        aggregator.add(LogEntry::new(LogLevel::Error, "Error message"));
        aggregator.add(LogEntry::new(LogLevel::Error, "Another error"));

        let query = LogQuery::new().min_level(LogLevel::Error);
        let results = aggregator.query(&query);

        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_log_aggregator_recent() {
        let mut aggregator = LogAggregator::new(7);

        for i in 0..10 {
            aggregator.add(LogEntry::new(LogLevel::Info, format!("Message {}", i)));
        }

        let recent = aggregator.recent(3);
        assert_eq!(recent.len(), 3);
        assert_eq!(recent[0].body, "Message 9");
    }

    #[test]
    fn test_log_aggregator_max_entries() {
        let mut aggregator = LogAggregator::new(7).with_max_entries(5);

        for i in 0..10 {
            aggregator.add(LogEntry::new(LogLevel::Info, format!("Message {}", i)));
        }

        assert_eq!(aggregator.count(), 5);
    }

    #[test]
    fn test_log_aggregator_count_by_level() {
        let mut aggregator = LogAggregator::new(7);

        aggregator.add(LogEntry::new(LogLevel::Info, "Info 1"));
        aggregator.add(LogEntry::new(LogLevel::Info, "Info 2"));
        aggregator.add(LogEntry::new(LogLevel::Error, "Error 1"));

        let counts = aggregator.count_by_level();
        assert_eq!(counts.get(&LogLevel::Info), Some(&2));
        assert_eq!(counts.get(&LogLevel::Error), Some(&1));
    }
}
