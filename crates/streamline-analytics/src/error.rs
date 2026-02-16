//! Error types for the Streamline analytics engine.
//!
//! This module defines domain-specific errors for the embedded DuckDB analytics
//! engine, covering SQL execution failures, configuration issues, topic resolution
//! errors, and query timeouts.
//!
//! # Stability
//!
//! **Stable** -- Breaking changes only in major versions.

/// Errors from the analytics engine.
#[derive(Debug, thiserror::Error)]
pub enum AnalyticsError {
    /// A general analytics engine error (e.g. DuckDB internal failure).
    #[error("Analytics error: {0}")]
    Analytics(String),

    /// An analytics configuration error (e.g. invalid engine settings).
    #[error("Configuration error: {0}")]
    Config(String),

    /// The SQL statement could not be parsed or prepared by DuckDB.
    ///
    /// The inner string contains the DuckDB diagnostic message.
    #[error("Invalid SQL: {0}")]
    InvalidSql(String),

    /// A topic referenced in the query does not exist.
    #[error("Topic not found: {0}")]
    TopicNotFound(String),

    /// The query exceeded the configured timeout.
    #[error("Query timed out after {timeout_ms}ms")]
    QueryTimeout {
        /// The timeout that was exceeded, in milliseconds.
        timeout_ms: u64,
    },

    /// A DuckDB operation failed (wrapper around the duckdb crate error).
    #[error("DuckDB error: {0}")]
    DuckDb(String),
}

impl AnalyticsError {
    /// Create an `InvalidSql` error with a user-friendly hint.
    ///
    /// # Examples
    ///
    /// ```
    /// use streamline_analytics::error::AnalyticsError;
    ///
    /// let err = AnalyticsError::invalid_sql(
    ///     "SELECT * FORM events",
    ///     "syntax error near 'FORM'",
    /// );
    /// assert!(err.to_string().contains("syntax error"));
    /// ```
    pub fn invalid_sql(sql: &str, detail: impl Into<String>) -> Self {
        let detail = detail.into();
        // Truncate very long queries in the error message
        let sql_preview = if sql.len() > 120 {
            format!("{}...", &sql[..120])
        } else {
            sql.to_string()
        };
        Self::InvalidSql(format!("{} (query: {})", detail, sql_preview))
    }

    /// Create a `TopicNotFound` error for a specific topic name.
    pub fn topic_not_found(topic: impl Into<String>) -> Self {
        Self::TopicNotFound(topic.into())
    }

    /// Create a `QueryTimeout` error.
    pub fn query_timeout(timeout_ms: u64) -> Self {
        Self::QueryTimeout { timeout_ms }
    }

    /// Wrap a raw DuckDB error string.
    pub fn duckdb(detail: impl Into<String>) -> Self {
        Self::DuckDb(detail.into())
    }

    /// Returns `true` if the caller could reasonably retry this operation
    /// (e.g. transient DuckDB lock contention).
    pub fn is_retriable(&self) -> bool {
        matches!(self, AnalyticsError::QueryTimeout { .. })
    }
}

impl From<duckdb::Error> for AnalyticsError {
    fn from(e: duckdb::Error) -> Self {
        let msg = e.to_string();
        // Heuristic: DuckDB syntax / parse errors
        if msg.contains("Parser Error") || msg.contains("Binder Error") {
            AnalyticsError::InvalidSql(msg)
        } else {
            AnalyticsError::DuckDb(msg)
        }
    }
}

/// A specialised `Result` type for analytics operations.
pub type Result<T> = std::result::Result<T, AnalyticsError>;
