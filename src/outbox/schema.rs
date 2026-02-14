//! Outbox Schema and Table Management

use super::config::{DatabaseType, OutboxTableConfig};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// Outbox entry state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum OutboxState {
    /// Pending publication
    #[default]
    Pending,
    /// Claimed by a publisher (in-progress)
    Claimed,
    /// Successfully published
    Published,
    /// Failed (will retry)
    Failed,
    /// Permanently failed (moved to DLQ or abandoned)
    Dead,
}

impl OutboxState {
    /// Convert to string for database storage
    pub fn as_str(&self) -> &'static str {
        match self {
            OutboxState::Pending => "PENDING",
            OutboxState::Claimed => "CLAIMED",
            OutboxState::Published => "PUBLISHED",
            OutboxState::Failed => "FAILED",
            OutboxState::Dead => "DEAD",
        }
    }

    /// Parse from string
    pub fn parse_from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "PENDING" => Some(OutboxState::Pending),
            "CLAIMED" => Some(OutboxState::Claimed),
            "PUBLISHED" => Some(OutboxState::Published),
            "FAILED" => Some(OutboxState::Failed),
            "DEAD" => Some(OutboxState::Dead),
            _ => None,
        }
    }
}

/// Outbox entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutboxEntry {
    /// Unique ID (UUID)
    pub id: String,
    /// Aggregate type (e.g., "Order", "User")
    pub aggregate_type: String,
    /// Aggregate ID (e.g., order_id, user_id)
    pub aggregate_id: String,
    /// Event type (e.g., "OrderCreated", "UserUpdated")
    pub event_type: String,
    /// Payload (typically JSON)
    pub payload: Vec<u8>,
    /// Target topic (optional override)
    pub topic: Option<String>,
    /// Partition key (optional)
    pub partition_key: Option<String>,
    /// Headers (optional)
    pub headers: HashMap<String, String>,
    /// Current state
    pub state: OutboxState,
    /// Retry count
    pub retry_count: u32,
    /// Created timestamp (milliseconds)
    pub created_at: i64,
    /// Published timestamp (milliseconds, if published)
    pub published_at: Option<i64>,
    /// Last error message
    pub last_error: Option<String>,
}

impl OutboxEntry {
    /// Create a new outbox entry
    pub fn new(
        aggregate_type: impl Into<String>,
        aggregate_id: impl Into<String>,
        event_type: impl Into<String>,
        payload: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            aggregate_type: aggregate_type.into(),
            aggregate_id: aggregate_id.into(),
            event_type: event_type.into(),
            payload: payload.into(),
            topic: None,
            partition_key: None,
            headers: HashMap::new(),
            state: OutboxState::Pending,
            retry_count: 0,
            created_at: chrono::Utc::now().timestamp_millis(),
            published_at: None,
            last_error: None,
        }
    }

    /// Set target topic
    pub fn with_topic(mut self, topic: impl Into<String>) -> Self {
        self.topic = Some(topic.into());
        self
    }

    /// Set partition key
    pub fn with_partition_key(mut self, key: impl Into<String>) -> Self {
        self.partition_key = Some(key.into());
        self
    }

    /// Add header
    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }

    /// Mark as claimed
    pub fn claim(&mut self) {
        self.state = OutboxState::Claimed;
    }

    /// Mark as published
    pub fn mark_published(&mut self) {
        self.state = OutboxState::Published;
        self.published_at = Some(chrono::Utc::now().timestamp_millis());
    }

    /// Mark as failed
    pub fn mark_failed(&mut self, error: impl Into<String>) {
        self.state = OutboxState::Failed;
        self.retry_count += 1;
        self.last_error = Some(error.into());
    }

    /// Mark as dead (no more retries)
    pub fn mark_dead(&mut self, error: impl Into<String>) {
        self.state = OutboxState::Dead;
        self.last_error = Some(error.into());
    }

    /// Get payload as string (if UTF-8)
    pub fn payload_str(&self) -> Option<&str> {
        std::str::from_utf8(&self.payload).ok()
    }

    /// Check if entry can be retried
    pub fn can_retry(&self, max_attempts: u32) -> bool {
        self.state == OutboxState::Failed && self.retry_count < max_attempts
    }

    /// Generate a deduplication key
    pub fn dedup_key(&self) -> String {
        format!("{}:{}:{}", self.aggregate_type, self.aggregate_id, self.id)
    }
}

/// Outbox schema definition
#[derive(Debug, Clone)]
pub struct OutboxSchema {
    /// Table configuration
    config: OutboxTableConfig,
    /// Database type
    db_type: DatabaseType,
}

impl OutboxSchema {
    /// Create a new outbox schema
    pub fn new(config: OutboxTableConfig, db_type: DatabaseType) -> Self {
        Self { config, db_type }
    }

    /// Generate CREATE TABLE statement
    pub fn create_table_sql(&self) -> String {
        match self.db_type {
            DatabaseType::PostgreSQL => self.create_table_postgres(),
            DatabaseType::MySQL => self.create_table_mysql(),
            DatabaseType::SQLite => self.create_table_sqlite(),
            DatabaseType::SqlServer => self.create_table_sqlserver(),
        }
    }

    /// Generate DROP TABLE statement
    pub fn drop_table_sql(&self) -> String {
        format!(
            "DROP TABLE IF EXISTS {}",
            self.config.qualified_table_name()
        )
    }

    /// Generate SELECT for pending entries
    pub fn select_pending_sql(&self, batch_size: usize) -> String {
        let table = self.config.qualified_table_name();

        match self.db_type {
            DatabaseType::PostgreSQL => format!(
                r#"SELECT * FROM {} 
                   WHERE {} = 'PENDING' OR ({} = 'FAILED' AND {} < 5)
                   ORDER BY {} ASC
                   LIMIT {}
                   FOR UPDATE SKIP LOCKED"#,
                table,
                self.config.state_column,
                self.config.state_column,
                self.config.retry_count_column,
                self.config.created_at_column,
                batch_size
            ),
            DatabaseType::MySQL => format!(
                r#"SELECT * FROM {} 
                   WHERE {} = 'PENDING' OR ({} = 'FAILED' AND {} < 5)
                   ORDER BY {} ASC
                   LIMIT {}
                   FOR UPDATE SKIP LOCKED"#,
                table,
                self.config.state_column,
                self.config.state_column,
                self.config.retry_count_column,
                self.config.created_at_column,
                batch_size
            ),
            DatabaseType::SQLite => format!(
                r#"SELECT * FROM {} 
                   WHERE {} = 'PENDING' OR ({} = 'FAILED' AND {} < 5)
                   ORDER BY {} ASC
                   LIMIT {}"#,
                table,
                self.config.state_column,
                self.config.state_column,
                self.config.retry_count_column,
                self.config.created_at_column,
                batch_size
            ),
            DatabaseType::SqlServer => format!(
                r#"SELECT TOP({}) * FROM {} WITH (UPDLOCK, READPAST)
                   WHERE {} = 'PENDING' OR ({} = 'FAILED' AND {} < 5)
                   ORDER BY {} ASC"#,
                batch_size,
                table,
                self.config.state_column,
                self.config.state_column,
                self.config.retry_count_column,
                self.config.created_at_column
            ),
        }
    }

    /// Generate UPDATE for marking entry as published
    pub fn update_published_sql(&self) -> String {
        let table = self.config.qualified_table_name();

        format!(
            r#"UPDATE {} SET {} = 'PUBLISHED', {} = $1 WHERE {} = $2"#,
            table, self.config.state_column, self.config.published_at_column, self.config.id_column
        )
    }

    /// Generate UPDATE for marking entry as failed
    pub fn update_failed_sql(&self) -> String {
        let table = self.config.qualified_table_name();

        format!(
            r#"UPDATE {} SET {} = 'FAILED', {} = {} + 1 WHERE {} = $1"#,
            table,
            self.config.state_column,
            self.config.retry_count_column,
            self.config.retry_count_column,
            self.config.id_column
        )
    }

    /// Generate DELETE for cleanup
    pub fn cleanup_sql(&self, retention_hours: u64) -> String {
        let table = self.config.qualified_table_name();

        match self.db_type {
            DatabaseType::PostgreSQL => format!(
                r#"DELETE FROM {} 
                   WHERE {} = 'PUBLISHED' 
                   AND {} < NOW() - INTERVAL '{} hours'"#,
                table, self.config.state_column, self.config.published_at_column, retention_hours
            ),
            DatabaseType::MySQL => format!(
                r#"DELETE FROM {} 
                   WHERE {} = 'PUBLISHED' 
                   AND {} < DATE_SUB(NOW(), INTERVAL {} HOUR)"#,
                table, self.config.state_column, self.config.published_at_column, retention_hours
            ),
            _ => format!(
                r#"DELETE FROM {} 
                   WHERE {} = 'PUBLISHED'"#,
                table, self.config.state_column
            ),
        }
    }

    fn create_table_postgres(&self) -> String {
        let table = self.config.qualified_table_name();
        let mut sql = format!(
            r#"CREATE TABLE IF NOT EXISTS {} (
    {} UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    {} VARCHAR(255) NOT NULL,
    {} VARCHAR(255) NOT NULL,
    {} VARCHAR(255) NOT NULL,
    {} JSONB NOT NULL,
    {} TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    {} TIMESTAMP WITH TIME ZONE,
    {} VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    {} INTEGER NOT NULL DEFAULT 0"#,
            table,
            self.config.id_column,
            self.config.aggregate_type_column,
            self.config.aggregate_id_column,
            self.config.event_type_column,
            self.config.payload_column,
            self.config.created_at_column,
            self.config.published_at_column,
            self.config.state_column,
            self.config.retry_count_column
        );

        if let Some(topic_col) = &self.config.topic_column {
            sql.push_str(&format!(",\n    {} VARCHAR(255)", topic_col));
        }
        if let Some(pk_col) = &self.config.partition_key_column {
            sql.push_str(&format!(",\n    {} VARCHAR(255)", pk_col));
        }
        if let Some(headers_col) = &self.config.headers_column {
            sql.push_str(&format!(",\n    {} JSONB", headers_col));
        }

        sql.push_str("\n);\n");

        // Add indexes
        sql.push_str(&format!(
            "CREATE INDEX IF NOT EXISTS idx_{}_state_created ON {} ({}, {});\n",
            self.config.table_name, table, self.config.state_column, self.config.created_at_column
        ));
        sql.push_str(&format!(
            "CREATE INDEX IF NOT EXISTS idx_{}_aggregate ON {} ({}, {});\n",
            self.config.table_name,
            table,
            self.config.aggregate_type_column,
            self.config.aggregate_id_column
        ));

        sql
    }

    fn create_table_mysql(&self) -> String {
        let table = self.config.qualified_table_name();
        let mut sql = format!(
            r#"CREATE TABLE IF NOT EXISTS {} (
    {} CHAR(36) PRIMARY KEY,
    {} VARCHAR(255) NOT NULL,
    {} VARCHAR(255) NOT NULL,
    {} VARCHAR(255) NOT NULL,
    {} JSON NOT NULL,
    {} TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    {} TIMESTAMP NULL,
    {} VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    {} INT NOT NULL DEFAULT 0"#,
            table,
            self.config.id_column,
            self.config.aggregate_type_column,
            self.config.aggregate_id_column,
            self.config.event_type_column,
            self.config.payload_column,
            self.config.created_at_column,
            self.config.published_at_column,
            self.config.state_column,
            self.config.retry_count_column
        );

        if let Some(topic_col) = &self.config.topic_column {
            sql.push_str(&format!(",\n    {} VARCHAR(255)", topic_col));
        }
        if let Some(pk_col) = &self.config.partition_key_column {
            sql.push_str(&format!(",\n    {} VARCHAR(255)", pk_col));
        }
        if let Some(headers_col) = &self.config.headers_column {
            sql.push_str(&format!(",\n    {} JSON", headers_col));
        }

        sql.push_str(&format!(
            ",\n    INDEX idx_state_created ({}, {})",
            self.config.state_column, self.config.created_at_column
        ));
        sql.push_str(&format!(
            ",\n    INDEX idx_aggregate ({}, {})",
            self.config.aggregate_type_column, self.config.aggregate_id_column
        ));

        sql.push_str("\n);\n");

        sql
    }

    fn create_table_sqlite(&self) -> String {
        let table = self.config.qualified_table_name();
        let mut sql = format!(
            r#"CREATE TABLE IF NOT EXISTS {} (
    {} TEXT PRIMARY KEY,
    {} TEXT NOT NULL,
    {} TEXT NOT NULL,
    {} TEXT NOT NULL,
    {} TEXT NOT NULL,
    {} TEXT DEFAULT (datetime('now')),
    {} TEXT,
    {} TEXT NOT NULL DEFAULT 'PENDING',
    {} INTEGER NOT NULL DEFAULT 0"#,
            table,
            self.config.id_column,
            self.config.aggregate_type_column,
            self.config.aggregate_id_column,
            self.config.event_type_column,
            self.config.payload_column,
            self.config.created_at_column,
            self.config.published_at_column,
            self.config.state_column,
            self.config.retry_count_column
        );

        if let Some(topic_col) = &self.config.topic_column {
            sql.push_str(&format!(",\n    {} TEXT", topic_col));
        }
        if let Some(pk_col) = &self.config.partition_key_column {
            sql.push_str(&format!(",\n    {} TEXT", pk_col));
        }
        if let Some(headers_col) = &self.config.headers_column {
            sql.push_str(&format!(",\n    {} TEXT", headers_col));
        }

        sql.push_str("\n);\n");

        sql.push_str(&format!(
            "CREATE INDEX IF NOT EXISTS idx_{}_state_created ON {} ({}, {});\n",
            self.config.table_name, table, self.config.state_column, self.config.created_at_column
        ));

        sql
    }

    fn create_table_sqlserver(&self) -> String {
        let table = self.config.qualified_table_name();
        let mut sql = format!(
            r#"IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{}' AND xtype='U')
CREATE TABLE {} (
    {} UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
    {} NVARCHAR(255) NOT NULL,
    {} NVARCHAR(255) NOT NULL,
    {} NVARCHAR(255) NOT NULL,
    {} NVARCHAR(MAX) NOT NULL,
    {} DATETIME2 DEFAULT GETUTCDATE(),
    {} DATETIME2,
    {} NVARCHAR(20) NOT NULL DEFAULT 'PENDING',
    {} INT NOT NULL DEFAULT 0"#,
            self.config.table_name,
            table,
            self.config.id_column,
            self.config.aggregate_type_column,
            self.config.aggregate_id_column,
            self.config.event_type_column,
            self.config.payload_column,
            self.config.created_at_column,
            self.config.published_at_column,
            self.config.state_column,
            self.config.retry_count_column
        );

        if let Some(topic_col) = &self.config.topic_column {
            sql.push_str(&format!(",\n    {} NVARCHAR(255)", topic_col));
        }
        if let Some(pk_col) = &self.config.partition_key_column {
            sql.push_str(&format!(",\n    {} NVARCHAR(255)", pk_col));
        }
        if let Some(headers_col) = &self.config.headers_column {
            sql.push_str(&format!(",\n    {} NVARCHAR(MAX)", headers_col));
        }

        sql.push_str("\n);\n");

        sql
    }
}

/// Outbox table manager
pub struct OutboxTableManager {
    /// Schema definition
    schema: OutboxSchema,
}

impl OutboxTableManager {
    /// Create a new table manager
    pub fn new(config: OutboxTableConfig, db_type: DatabaseType) -> Self {
        Self {
            schema: OutboxSchema::new(config, db_type),
        }
    }

    /// Get the schema
    pub fn schema(&self) -> &OutboxSchema {
        &self.schema
    }

    /// Get CREATE TABLE SQL
    pub fn create_table_sql(&self) -> String {
        self.schema.create_table_sql()
    }

    /// Get DROP TABLE SQL
    pub fn drop_table_sql(&self) -> String {
        self.schema.drop_table_sql()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_outbox_entry_creation() {
        let entry = OutboxEntry::new("Order", "order-123", "OrderCreated", b"{}".to_vec())
            .with_topic("orders")
            .with_partition_key("order-123");

        assert_eq!(entry.aggregate_type, "Order");
        assert_eq!(entry.aggregate_id, "order-123");
        assert_eq!(entry.event_type, "OrderCreated");
        assert_eq!(entry.topic, Some("orders".to_string()));
        assert_eq!(entry.state, OutboxState::Pending);
    }

    #[test]
    fn test_outbox_state_transitions() {
        let mut entry = OutboxEntry::new("Order", "1", "Created", vec![]);

        entry.claim();
        assert_eq!(entry.state, OutboxState::Claimed);

        entry.mark_published();
        assert_eq!(entry.state, OutboxState::Published);
        assert!(entry.published_at.is_some());
    }

    #[test]
    fn test_create_table_postgres() {
        let config = OutboxTableConfig::default();
        let schema = OutboxSchema::new(config, DatabaseType::PostgreSQL);
        let sql = schema.create_table_sql();

        assert!(sql.contains("CREATE TABLE IF NOT EXISTS outbox"));
        assert!(sql.contains("UUID PRIMARY KEY"));
        assert!(sql.contains("JSONB"));
    }

    #[test]
    fn test_create_table_mysql() {
        let config = OutboxTableConfig::default();
        let schema = OutboxSchema::new(config, DatabaseType::MySQL);
        let sql = schema.create_table_sql();

        assert!(sql.contains("CREATE TABLE IF NOT EXISTS outbox"));
        assert!(sql.contains("CHAR(36) PRIMARY KEY"));
        assert!(sql.contains("JSON"));
    }

    #[test]
    fn test_select_pending_sql() {
        let config = OutboxTableConfig::default();
        let schema = OutboxSchema::new(config, DatabaseType::PostgreSQL);
        let sql = schema.select_pending_sql(100);

        assert!(sql.contains("SELECT * FROM outbox"));
        assert!(sql.contains("FOR UPDATE SKIP LOCKED"));
        assert!(sql.contains("LIMIT 100"));
    }
}
