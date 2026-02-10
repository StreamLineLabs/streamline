//! In-memory ring buffer for server logs
//!
//! This module provides a thread-safe ring buffer for storing recent log entries.
//! It enables the Log Viewer UI to display real-time server logs without disk I/O.

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Log level for filtering
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl LogLevel {
    /// Parse from string (case-insensitive)
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "TRACE" => Some(LogLevel::Trace),
            "DEBUG" => Some(LogLevel::Debug),
            "INFO" => Some(LogLevel::Info),
            "WARN" | "WARNING" => Some(LogLevel::Warn),
            "ERROR" => Some(LogLevel::Error),
            _ => None,
        }
    }

    /// Check if this level should be displayed for a minimum level filter
    pub fn matches_filter(&self, min_level: LogLevel) -> bool {
        (*self as u8) >= (min_level as u8)
    }
}

impl std::fmt::Display for LogLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogLevel::Trace => write!(f, "TRACE"),
            LogLevel::Debug => write!(f, "DEBUG"),
            LogLevel::Info => write!(f, "INFO"),
            LogLevel::Warn => write!(f, "WARN"),
            LogLevel::Error => write!(f, "ERROR"),
        }
    }
}

/// A single log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    /// Unique ID for this entry
    pub id: u64,
    /// Unix timestamp in milliseconds
    pub timestamp: u64,
    /// Log level
    pub level: LogLevel,
    /// Target/module name
    pub target: String,
    /// Log message
    pub message: String,
    /// Additional structured fields (key-value pairs)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<serde_json::Value>,
}

impl LogEntry {
    /// Create a new log entry with auto-generated timestamp and ID
    pub fn new(level: LogLevel, target: &str, message: &str) -> Self {
        Self {
            id: 0, // Will be set by buffer
            timestamp: current_timestamp_ms(),
            level,
            target: target.to_string(),
            message: message.to_string(),
            fields: None,
        }
    }

    /// Create log entry with additional fields
    pub fn with_fields(
        level: LogLevel,
        target: &str,
        message: &str,
        fields: serde_json::Value,
    ) -> Self {
        Self {
            id: 0,
            timestamp: current_timestamp_ms(),
            level,
            target: target.to_string(),
            message: message.to_string(),
            fields: Some(fields),
        }
    }
}

/// Configuration for the log buffer
#[derive(Debug, Clone)]
pub struct LogBufferConfig {
    /// Maximum number of entries to store
    pub max_entries: usize,
    /// Minimum log level to capture
    pub min_level: LogLevel,
}

impl Default for LogBufferConfig {
    fn default() -> Self {
        Self {
            max_entries: 10_000,
            min_level: LogLevel::Info,
        }
    }
}

/// Thread-safe ring buffer for log entries
pub struct LogBuffer {
    entries: RwLock<VecDeque<LogEntry>>,
    max_entries: usize,
    min_level: LogLevel,
    next_id: RwLock<u64>,
}

impl LogBuffer {
    /// Create a new log buffer with default config
    pub fn new() -> Self {
        Self::with_config(LogBufferConfig::default())
    }

    /// Create a new log buffer with custom config
    pub fn with_config(config: LogBufferConfig) -> Self {
        Self {
            entries: RwLock::new(VecDeque::with_capacity(config.max_entries)),
            max_entries: config.max_entries,
            min_level: config.min_level,
            next_id: RwLock::new(1),
        }
    }

    /// Create as shared Arc
    pub fn new_shared() -> Arc<Self> {
        Arc::new(Self::new())
    }

    /// Create with config as shared Arc
    pub fn with_config_shared(config: LogBufferConfig) -> Arc<Self> {
        Arc::new(Self::with_config(config))
    }

    /// Add a log entry to the buffer
    pub fn push(&self, mut entry: LogEntry) {
        // Check level filter
        if !entry.level.matches_filter(self.min_level) {
            return;
        }

        // Assign ID
        {
            let mut id = self.next_id.write();
            entry.id = *id;
            *id += 1;
        }

        // Add to buffer
        let mut entries = self.entries.write();
        if entries.len() >= self.max_entries {
            entries.pop_front();
        }
        entries.push_back(entry);
    }

    /// Log a message at the specified level
    pub fn log(&self, level: LogLevel, target: &str, message: &str) {
        self.push(LogEntry::new(level, target, message));
    }

    /// Log with structured fields
    pub fn log_with_fields(
        &self,
        level: LogLevel,
        target: &str,
        message: &str,
        fields: serde_json::Value,
    ) {
        self.push(LogEntry::with_fields(level, target, message, fields));
    }

    /// Get recent log entries
    ///
    /// Returns entries filtered by level and limited by count.
    /// If `after_id` is provided, only returns entries with ID > after_id.
    pub fn get_entries(
        &self,
        min_level: Option<LogLevel>,
        limit: usize,
        after_id: Option<u64>,
    ) -> Vec<LogEntry> {
        let entries = self.entries.read();
        let min_level = min_level.unwrap_or(LogLevel::Trace);

        entries
            .iter()
            .filter(|e| e.level.matches_filter(min_level))
            .filter(|e| after_id.map_or(true, |id| e.id > id))
            .rev() // Most recent first
            .take(limit)
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
            .rev() // Restore chronological order
            .collect()
    }

    /// Get entries matching a search pattern
    pub fn search(
        &self,
        pattern: &str,
        min_level: Option<LogLevel>,
        limit: usize,
    ) -> Vec<LogEntry> {
        let entries = self.entries.read();
        let min_level = min_level.unwrap_or(LogLevel::Trace);
        let pattern_lower = pattern.to_lowercase();

        entries
            .iter()
            .filter(|e| e.level.matches_filter(min_level))
            .filter(|e| {
                e.message.to_lowercase().contains(&pattern_lower)
                    || e.target.to_lowercase().contains(&pattern_lower)
            })
            .rev()
            .take(limit)
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect()
    }

    /// Get current entry count
    pub fn len(&self) -> usize {
        self.entries.read().len()
    }

    /// Check if buffer is empty
    pub fn is_empty(&self) -> bool {
        self.entries.read().is_empty()
    }

    /// Clear all entries
    pub fn clear(&self) {
        self.entries.write().clear();
    }

    /// Get the latest entry ID (for SSE polling)
    pub fn latest_id(&self) -> Option<u64> {
        self.entries.read().back().map(|e| e.id)
    }
}

impl Default for LogBuffer {
    fn default() -> Self {
        Self::new()
    }
}

/// Get current timestamp in milliseconds since Unix epoch
fn current_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_level_filter() {
        assert!(LogLevel::Error.matches_filter(LogLevel::Info));
        assert!(LogLevel::Info.matches_filter(LogLevel::Info));
        assert!(!LogLevel::Debug.matches_filter(LogLevel::Info));
    }

    #[test]
    fn test_log_buffer_push() {
        let buffer = LogBuffer::new();
        buffer.log(LogLevel::Info, "test", "Hello");
        buffer.log(LogLevel::Warn, "test", "World");

        let entries = buffer.get_entries(None, 10, None);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].message, "Hello");
        assert_eq!(entries[1].message, "World");
    }

    #[test]
    fn test_log_buffer_ring() {
        let config = LogBufferConfig {
            max_entries: 3,
            min_level: LogLevel::Trace,
        };
        let buffer = LogBuffer::with_config(config);

        for i in 0..5 {
            buffer.log(LogLevel::Info, "test", &format!("Message {}", i));
        }

        let entries = buffer.get_entries(None, 10, None);
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].message, "Message 2");
        assert_eq!(entries[1].message, "Message 3");
        assert_eq!(entries[2].message, "Message 4");
    }

    #[test]
    fn test_log_buffer_level_filter() {
        let config = LogBufferConfig {
            max_entries: 100,
            min_level: LogLevel::Info,
        };
        let buffer = LogBuffer::with_config(config);

        buffer.log(LogLevel::Debug, "test", "Debug message");
        buffer.log(LogLevel::Info, "test", "Info message");
        buffer.log(LogLevel::Error, "test", "Error message");

        let entries = buffer.get_entries(None, 10, None);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].message, "Info message");
    }

    #[test]
    fn test_log_buffer_search() {
        let buffer = LogBuffer::new();
        buffer.log(LogLevel::Info, "kafka", "Connection established");
        buffer.log(LogLevel::Info, "http", "Request received");
        buffer.log(LogLevel::Error, "kafka", "Connection error");

        let entries = buffer.search("kafka", None, 10);
        assert_eq!(entries.len(), 2);

        let entries = buffer.search("error", None, 10);
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn test_log_buffer_after_id() {
        let buffer = LogBuffer::new();
        buffer.log(LogLevel::Info, "test", "First");
        buffer.log(LogLevel::Info, "test", "Second");
        buffer.log(LogLevel::Info, "test", "Third");

        let entries = buffer.get_entries(None, 10, Some(1));
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].message, "Second");
    }
}
