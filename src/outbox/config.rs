//! Outbox Configuration Types

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Database type for outbox
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum DatabaseType {
    /// PostgreSQL
    #[default]
    PostgreSQL,
    /// MySQL
    MySQL,
    /// SQLite
    SQLite,
    /// SQL Server
    SqlServer,
}

impl DatabaseType {
    /// Get the SQL syntax for this database type
    pub fn sql_dialect(&self) -> &'static str {
        match self {
            DatabaseType::PostgreSQL => "postgresql",
            DatabaseType::MySQL => "mysql",
            DatabaseType::SQLite => "sqlite",
            DatabaseType::SqlServer => "tsql",
        }
    }
}

/// Main outbox configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutboxConfig {
    /// Enable outbox pattern
    pub enabled: bool,
    /// Default table configuration
    pub table: OutboxTableConfig,
    /// Default publisher configuration
    pub publisher: PublisherConfig,
    /// Retry configuration
    pub retry: RetryConfig,
    /// Deduplication configuration
    pub deduplication: DeduplicationConfig,
    /// Cleanup configuration
    pub cleanup: CleanupConfig,
}

impl Default for OutboxConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            table: OutboxTableConfig::default(),
            publisher: PublisherConfig::default(),
            retry: RetryConfig::default(),
            deduplication: DeduplicationConfig::default(),
            cleanup: CleanupConfig::default(),
        }
    }
}

/// Outbox table configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutboxTableConfig {
    /// Table name
    pub table_name: String,
    /// Schema/namespace
    pub schema: Option<String>,
    /// ID column name
    pub id_column: String,
    /// Aggregate type column name
    pub aggregate_type_column: String,
    /// Aggregate ID column name
    pub aggregate_id_column: String,
    /// Event type column name
    pub event_type_column: String,
    /// Payload column name
    pub payload_column: String,
    /// Created timestamp column name
    pub created_at_column: String,
    /// Published timestamp column name
    pub published_at_column: String,
    /// State column name
    pub state_column: String,
    /// Retry count column name
    pub retry_count_column: String,
    /// Topic column name (optional, for routing)
    pub topic_column: Option<String>,
    /// Partition key column name (optional)
    pub partition_key_column: Option<String>,
    /// Headers column name (optional, JSON)
    pub headers_column: Option<String>,
}

impl Default for OutboxTableConfig {
    fn default() -> Self {
        Self {
            table_name: "outbox".to_string(),
            schema: None,
            id_column: "id".to_string(),
            aggregate_type_column: "aggregate_type".to_string(),
            aggregate_id_column: "aggregate_id".to_string(),
            event_type_column: "event_type".to_string(),
            payload_column: "payload".to_string(),
            created_at_column: "created_at".to_string(),
            published_at_column: "published_at".to_string(),
            state_column: "state".to_string(),
            retry_count_column: "retry_count".to_string(),
            topic_column: Some("topic".to_string()),
            partition_key_column: Some("partition_key".to_string()),
            headers_column: Some("headers".to_string()),
        }
    }
}

impl OutboxTableConfig {
    /// Get fully qualified table name
    pub fn qualified_table_name(&self) -> String {
        match &self.schema {
            Some(schema) => format!("{}.{}", schema, self.table_name),
            None => self.table_name.clone(),
        }
    }
}

/// Publisher configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublisherConfig {
    /// Database type
    pub database_type: DatabaseType,
    /// Database connection string
    pub connection_string: String,
    /// Default target topic
    pub default_topic: String,
    /// Polling interval (milliseconds)
    pub poll_interval_ms: u64,
    /// Batch size for polling
    pub batch_size: usize,
    /// Lock timeout for claimed messages (seconds)
    pub lock_timeout_seconds: u64,
    /// Use transaction for publish
    pub transactional: bool,
    /// Table configuration override
    pub table: Option<OutboxTableConfig>,
}

impl Default for PublisherConfig {
    fn default() -> Self {
        Self {
            database_type: DatabaseType::PostgreSQL,
            connection_string: String::new(),
            default_topic: "events".to_string(),
            poll_interval_ms: 100,
            batch_size: 100,
            lock_timeout_seconds: 60,
            transactional: true,
            table: None,
        }
    }
}

/// Retry configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    /// Enable retries
    pub enabled: bool,
    /// Maximum retry attempts
    pub max_attempts: u32,
    /// Initial backoff (milliseconds)
    pub initial_backoff_ms: u64,
    /// Maximum backoff (milliseconds)
    pub max_backoff_ms: u64,
    /// Backoff multiplier
    pub backoff_multiplier: f64,
    /// Move to DLQ after max attempts
    pub dlq_on_exhaust: bool,
    /// DLQ topic name
    pub dlq_topic: Option<String>,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_attempts: 5,
            initial_backoff_ms: 1000,
            max_backoff_ms: 60_000,
            backoff_multiplier: 2.0,
            dlq_on_exhaust: true,
            dlq_topic: Some("outbox-dlq".to_string()),
        }
    }
}

impl RetryConfig {
    /// Calculate backoff for a given attempt
    pub fn calculate_backoff(&self, attempt: u32) -> Duration {
        if attempt == 0 {
            return Duration::from_millis(self.initial_backoff_ms);
        }

        let backoff_ms =
            self.initial_backoff_ms as f64 * self.backoff_multiplier.powi(attempt as i32);
        let capped_ms = backoff_ms.min(self.max_backoff_ms as f64) as u64;

        Duration::from_millis(capped_ms)
    }
}

/// Deduplication configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeduplicationConfig {
    /// Enable deduplication
    pub enabled: bool,
    /// Deduplication window (seconds)
    pub window_seconds: u64,
    /// Maximum cache size
    pub max_cache_size: usize,
    /// Cache eviction interval (seconds)
    pub eviction_interval_seconds: u64,
}

impl Default for DeduplicationConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            window_seconds: 3600, // 1 hour
            max_cache_size: 100_000,
            eviction_interval_seconds: 60,
        }
    }
}

/// Cleanup configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CleanupConfig {
    /// Enable automatic cleanup
    pub enabled: bool,
    /// Retention period for published messages (hours)
    pub retention_hours: u64,
    /// Cleanup interval (minutes)
    pub cleanup_interval_minutes: u64,
    /// Batch size for cleanup
    pub batch_size: usize,
}

impl Default for CleanupConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            retention_hours: 24,
            cleanup_interval_minutes: 60,
            batch_size: 1000,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = OutboxConfig::default();
        assert!(config.enabled);
        assert_eq!(config.table.table_name, "outbox");
    }

    #[test]
    fn test_qualified_table_name() {
        let mut table_config = OutboxTableConfig::default();
        assert_eq!(table_config.qualified_table_name(), "outbox");

        table_config.schema = Some("myschema".to_string());
        assert_eq!(table_config.qualified_table_name(), "myschema.outbox");
    }

    #[test]
    fn test_retry_backoff() {
        let config = RetryConfig::default();

        let backoff0 = config.calculate_backoff(0);
        assert_eq!(backoff0, Duration::from_millis(1000));

        let backoff1 = config.calculate_backoff(1);
        assert_eq!(backoff1, Duration::from_millis(2000));

        let backoff2 = config.calculate_backoff(2);
        assert_eq!(backoff2, Duration::from_millis(4000));

        // Should cap at max
        let backoff10 = config.calculate_backoff(10);
        assert_eq!(backoff10, Duration::from_millis(60_000));
    }
}
