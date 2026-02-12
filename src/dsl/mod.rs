//! Stream Processing DSL Module
//!
//! Provides a declarative domain-specific language for defining stream
//! processing pipelines with:
//! - Fluent API for building pipelines
//! - SQL-like syntax parsing
//! - Filter, map, aggregate operations
//! - Windowing support (tumbling, sliding, session)
//! - Join operations between streams
//!
//! # Overview
//!
//! The DSL module enables users to define complex stream transformations
//! using either a fluent Rust API or a SQL-like text syntax.
//!
//! # Example - Fluent API
//!
//! ```ignore
//! use streamline::dsl::{Pipeline, WindowType};
//!
//! let pipeline = Pipeline::from_topic("events")
//!     .filter(|r| r.get("type") == Some("click"))
//!     .map(|r| r.select(&["user_id", "timestamp", "page"]))
//!     .window(WindowType::Tumbling { duration_ms: 60000 })
//!     .aggregate(|w| w.count().group_by("user_id"))
//!     .to_topic("click_counts");
//! ```
//!
//! # Example - SQL-like Syntax
//!
//! ```ignore
//! use streamline::dsl::DslParser;
//!
//! let query = r#"
//!     SELECT user_id, COUNT(*) as click_count
//!     FROM STREAM 'events'
//!     WHERE type = 'click'
//!     WINDOW TUMBLING 1 MINUTE
//!     GROUP BY user_id
//!     EMIT TO 'click_counts'
//! "#;
//!
//! let pipeline = DslParser::parse(query)?;
//! ```

pub mod flow_designer;
pub mod operators;
pub mod parser;
pub mod pipeline;
pub mod window;

// Re-export main types
pub use pipeline::{Pipeline, PipelineBuilder};
pub use window::WindowType;

use crate::error::{Result, StreamlineError};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A record in the DSL processing pipeline
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DslRecord {
    /// Record key
    pub key: Option<Bytes>,
    /// Record value as structured data
    pub value: serde_json::Value,
    /// Record timestamp
    pub timestamp: i64,
    /// Record headers
    pub headers: HashMap<String, String>,
    /// Source topic
    pub source_topic: String,
    /// Source partition
    pub source_partition: i32,
    /// Source offset
    pub source_offset: i64,
}

impl DslRecord {
    /// Create a new DSL record
    pub fn new(value: serde_json::Value, source_topic: &str) -> Self {
        Self {
            key: None,
            value,
            timestamp: chrono::Utc::now().timestamp_millis(),
            headers: HashMap::new(),
            source_topic: source_topic.to_string(),
            source_partition: 0,
            source_offset: 0,
        }
    }

    /// Create from raw bytes
    pub fn from_bytes(
        key: Option<Bytes>,
        value: Bytes,
        source_topic: &str,
        partition: i32,
        offset: i64,
        timestamp: i64,
    ) -> Result<Self> {
        let value_json: serde_json::Value = serde_json::from_slice(&value).unwrap_or(
            serde_json::Value::String(String::from_utf8_lossy(&value).to_string()),
        );

        Ok(Self {
            key,
            value: value_json,
            timestamp,
            headers: HashMap::new(),
            source_topic: source_topic.to_string(),
            source_partition: partition,
            source_offset: offset,
        })
    }

    /// Get a field from the record value
    pub fn get(&self, field: &str) -> Option<&serde_json::Value> {
        match &self.value {
            serde_json::Value::Object(map) => map.get(field),
            _ => None,
        }
    }

    /// Get a string field
    pub fn get_str(&self, field: &str) -> Option<&str> {
        self.get(field).and_then(|v| v.as_str())
    }

    /// Get an integer field
    pub fn get_i64(&self, field: &str) -> Option<i64> {
        self.get(field).and_then(|v| v.as_i64())
    }

    /// Get a float field
    pub fn get_f64(&self, field: &str) -> Option<f64> {
        self.get(field).and_then(|v| v.as_f64())
    }

    /// Get a boolean field
    pub fn get_bool(&self, field: &str) -> Option<bool> {
        self.get(field).and_then(|v| v.as_bool())
    }

    /// Set a field in the record value
    pub fn set(&mut self, field: &str, value: serde_json::Value) {
        if let serde_json::Value::Object(ref mut map) = self.value {
            map.insert(field.to_string(), value);
        }
    }

    /// Select specific fields
    pub fn select(&self, fields: &[&str]) -> Self {
        let mut new_value = serde_json::Map::new();
        if let serde_json::Value::Object(map) = &self.value {
            for field in fields {
                if let Some(v) = map.get(*field) {
                    new_value.insert(field.to_string(), v.clone());
                }
            }
        }
        Self {
            key: self.key.clone(),
            value: serde_json::Value::Object(new_value),
            timestamp: self.timestamp,
            headers: self.headers.clone(),
            source_topic: self.source_topic.clone(),
            source_partition: self.source_partition,
            source_offset: self.source_offset,
        }
    }

    /// Convert to bytes for output
    pub fn to_bytes(&self) -> Result<Bytes> {
        let json = serde_json::to_vec(&self.value).map_err(StreamlineError::Serialization)?;
        Ok(Bytes::from(json))
    }
}

/// DSL execution context
#[derive(Debug, Clone)]
pub struct DslContext {
    /// Variables available in expressions
    pub variables: HashMap<String, serde_json::Value>,
    /// Current window start time (if in window)
    pub window_start: Option<i64>,
    /// Current window end time (if in window)
    pub window_end: Option<i64>,
    /// Processing time
    pub processing_time: i64,
}

impl Default for DslContext {
    fn default() -> Self {
        Self {
            variables: HashMap::new(),
            window_start: None,
            window_end: None,
            processing_time: chrono::Utc::now().timestamp_millis(),
        }
    }
}

impl DslContext {
    /// Create a new context
    pub fn new() -> Self {
        Self::default()
    }

    /// Set a variable
    pub fn set_var(&mut self, name: &str, value: serde_json::Value) {
        self.variables.insert(name.to_string(), value);
    }

    /// Get a variable
    pub fn get_var(&self, name: &str) -> Option<&serde_json::Value> {
        self.variables.get(name)
    }

    /// Create a windowed context
    pub fn with_window(start: i64, end: i64) -> Self {
        Self {
            variables: HashMap::new(),
            window_start: Some(start),
            window_end: Some(end),
            processing_time: chrono::Utc::now().timestamp_millis(),
        }
    }
}

/// Result type for DSL operations
pub type DslResult<T> = std::result::Result<T, DslError>;

/// DSL-specific errors
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DslError {
    /// Parse error
    Parse(String),
    /// Invalid expression
    InvalidExpression(String),
    /// Type mismatch
    TypeMismatch { expected: String, found: String },
    /// Field not found
    FieldNotFound(String),
    /// Invalid operator
    InvalidOperator(String),
    /// Window error
    WindowError(String),
    /// Execution error
    ExecutionError(String),
}

impl std::fmt::Display for DslError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DslError::Parse(msg) => write!(f, "Parse error: {}", msg),
            DslError::InvalidExpression(msg) => write!(f, "Invalid expression: {}", msg),
            DslError::TypeMismatch { expected, found } => {
                write!(f, "Type mismatch: expected {}, found {}", expected, found)
            }
            DslError::FieldNotFound(field) => write!(f, "Field not found: {}", field),
            DslError::InvalidOperator(op) => write!(f, "Invalid operator: {}", op),
            DslError::WindowError(msg) => write!(f, "Window error: {}", msg),
            DslError::ExecutionError(msg) => write!(f, "Execution error: {}", msg),
        }
    }
}

impl std::error::Error for DslError {}

impl From<DslError> for StreamlineError {
    fn from(err: DslError) -> Self {
        StreamlineError::Config(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dsl_record_get_fields() {
        let value = serde_json::json!({
            "user_id": "user123",
            "count": 42,
            "score": 99.5,
            "active": true
        });
        let record = DslRecord::new(value, "test-topic");

        assert_eq!(record.get_str("user_id"), Some("user123"));
        assert_eq!(record.get_i64("count"), Some(42));
        assert_eq!(record.get_f64("score"), Some(99.5));
        assert_eq!(record.get_bool("active"), Some(true));
        assert_eq!(record.get_str("missing"), None);
    }

    #[test]
    fn test_dsl_record_select() {
        let value = serde_json::json!({
            "user_id": "user123",
            "count": 42,
            "extra": "data"
        });
        let record = DslRecord::new(value, "test-topic");
        let selected = record.select(&["user_id", "count"]);

        assert_eq!(selected.get_str("user_id"), Some("user123"));
        assert_eq!(selected.get_i64("count"), Some(42));
        assert_eq!(selected.get_str("extra"), None);
    }

    #[test]
    fn test_dsl_record_set() {
        let value = serde_json::json!({
            "user_id": "user123"
        });
        let mut record = DslRecord::new(value, "test-topic");
        record.set("count", serde_json::json!(100));

        assert_eq!(record.get_i64("count"), Some(100));
    }

    #[test]
    fn test_dsl_context() {
        let mut ctx = DslContext::new();
        ctx.set_var("threshold", serde_json::json!(100));

        assert_eq!(ctx.get_var("threshold"), Some(&serde_json::json!(100)));
        assert_eq!(ctx.get_var("missing"), None);
    }

    #[test]
    fn test_dsl_context_window() {
        let ctx = DslContext::with_window(1000, 2000);

        assert_eq!(ctx.window_start, Some(1000));
        assert_eq!(ctx.window_end, Some(2000));
    }
}
