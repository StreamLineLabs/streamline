//! Change Data Capture (CDC) module
//!
//! This module provides CDC capabilities for capturing database changes
//! and streaming them to Streamline topics.
//!
//! Supported databases:
//! - PostgreSQL (logical replication with pgoutput or wal2json)
//! - MySQL (binlog replication with row-based format)
//! - MongoDB (change streams)
//! - SQL Server (polling-based CDC)
//! - Oracle (LogMiner)
//! - CockroachDB (changefeed)
//! - TiDB (TiCDC)
//! - Cassandra (CDC)
//! - DynamoDB Streams
//! - Snowflake Streams
//! - BigQuery Change Data Capture
//!
//! Features:
//! - Universal CDC Hub for multi-source coordination
//! - Schema evolution tracking with compatibility checking
//! - Automatic schema migration between sources
//! - Debezium-compatible event format
//! - Initial snapshots with consistent offsets
//! - At-least-once delivery with offset tracking
//! - Cross-database joins and transformations
//! - Dead letter queue for failed events

pub mod change_event;
pub mod config;
pub mod debezium;
pub mod heartbeat;
pub mod native_cdc;
pub mod schema;
pub mod schema_history;
pub mod snapshot;
pub mod universal;

#[cfg(feature = "mongodb-cdc")]
pub mod mongodb;
#[cfg(feature = "mysql-cdc")]
pub mod mysql;
#[cfg(feature = "postgres-cdc")]
pub mod postgres;
#[cfg(feature = "sqlserver-cdc")]
pub mod sqlserver;

// Re-exports
pub use config::{CdcConfig, OutputFormat, SchemaCompatibility, SchemaEvolutionConfig};
pub use schema::{
    ColumnSchema, CompatibilityResult, SchemaEvolutionStats, SchemaEvolutionTracker, SchemaHistory,
    SchemaRegistry, SchemaVersion, TableSchema,
};

// New Debezium-replacement re-exports
pub use change_event::{ChangeEvent, Operation, SourceInfo, TransactionInfo};
pub use debezium::{
    columns_to_debezium_fields, map_db_type_to_debezium, DebeziumEnvelope, DebeziumField,
    DebeziumSchema,
};
pub use heartbeat::{CdcHeartbeat, HeartbeatConfig, HeartbeatMessage};
pub use schema_history::{DdlType, SchemaChange, SchemaHistoryStore};
pub use snapshot::{SnapshotManager, SnapshotProgress, SnapshotState, SnapshotStrategy};

#[cfg(feature = "postgres-cdc")]
pub use config::{PostgresCdcConfig, PostgresOutputPlugin, PostgresSslMode};
#[cfg(feature = "postgres-cdc")]
pub use postgres::PostgresCdcSource;

#[cfg(feature = "mysql-cdc")]
pub use config::MySqlCdcConfig;
#[cfg(feature = "mysql-cdc")]
pub use mysql::{BinlogPosition, MySqlCdcSource};

#[cfg(feature = "mongodb-cdc")]
pub use config::{MongoDbCdcConfig, MongoFullDocumentMode};
#[cfg(feature = "mongodb-cdc")]
pub use mongodb::{MongoDbCdcSource, ResumeToken};

#[cfg(feature = "sqlserver-cdc")]
pub use config::SqlServerCdcConfig;
#[cfg(feature = "sqlserver-cdc")]
pub use sqlserver::{LsnPosition, SqlServerCdcSource};

use crate::error::Result;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

/// CDC operation type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CdcOperation {
    /// Insert operation
    Insert,
    /// Update operation
    Update,
    /// Delete operation
    Delete,
    /// Snapshot (initial load)
    Snapshot,
    /// Transaction begin
    Begin,
    /// Transaction commit
    Commit,
    /// DDL change
    Ddl,
}

impl std::fmt::Display for CdcOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CdcOperation::Insert => write!(f, "INSERT"),
            CdcOperation::Update => write!(f, "UPDATE"),
            CdcOperation::Delete => write!(f, "DELETE"),
            CdcOperation::Snapshot => write!(f, "SNAPSHOT"),
            CdcOperation::Begin => write!(f, "BEGIN"),
            CdcOperation::Commit => write!(f, "COMMIT"),
            CdcOperation::Ddl => write!(f, "DDL"),
        }
    }
}

/// A single column value from a CDC event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcColumnValue {
    /// Column name
    pub name: String,
    /// Column data type
    pub data_type: String,
    /// Column value (serialized as JSON)
    pub value: Option<serde_json::Value>,
}

/// A CDC event representing a database change
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcEvent {
    /// Database identifier
    pub database: String,
    /// Schema name
    pub schema: String,
    /// Table name
    pub table: String,
    /// Operation type
    pub operation: CdcOperation,
    /// LSN or position in the WAL
    pub position: String,
    /// Transaction ID (if available)
    pub transaction_id: Option<String>,
    /// Timestamp of the change
    pub timestamp: DateTime<Utc>,
    /// Column values before the change (for UPDATE/DELETE)
    pub before: Option<Vec<CdcColumnValue>>,
    /// Column values after the change (for INSERT/UPDATE)
    pub after: Option<Vec<CdcColumnValue>>,
    /// Primary key columns
    pub primary_key: Vec<String>,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
}

impl CdcEvent {
    /// Create a new CDC event
    pub fn new(
        database: String,
        schema: String,
        table: String,
        operation: CdcOperation,
        position: String,
    ) -> Self {
        Self {
            database,
            schema,
            table,
            operation,
            position,
            transaction_id: None,
            timestamp: Utc::now(),
            before: None,
            after: None,
            primary_key: Vec::new(),
            metadata: HashMap::new(),
        }
    }

    /// Convert to JSON bytes
    pub fn to_json_bytes(&self) -> Result<Bytes> {
        let json = serde_json::to_vec(self)?;
        Ok(Bytes::from(json))
    }

    /// Generate a record key based on primary key values
    pub fn record_key(&self) -> Option<Bytes> {
        // Use primary key values if available
        if self.primary_key.is_empty() {
            return None;
        }

        // Get values for primary key columns
        let values = self.after.as_ref().or(self.before.as_ref())?;
        let pk_values: Vec<String> = self
            .primary_key
            .iter()
            .filter_map(|pk| {
                values
                    .iter()
                    .find(|col| &col.name == pk)
                    .and_then(|col| col.value.as_ref())
                    .map(|v| v.to_string())
            })
            .collect();

        if pk_values.is_empty() {
            None
        } else {
            Some(Bytes::from(pk_values.join(":")))
        }
    }

    /// Get the target topic name
    pub fn topic_name(&self, prefix: Option<&str>) -> String {
        match prefix {
            Some(p) => format!("{}.{}.{}", p, self.schema, self.table),
            None => format!("{}.{}", self.schema, self.table),
        }
    }
}

/// CDC source status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CdcSourceStatus {
    /// Source is running
    Running,
    /// Source is paused
    Paused,
    /// Source is stopped
    Stopped,
    /// Source has an error
    Error,
    /// Source is initializing
    Initializing,
    /// Source is performing initial snapshot
    Snapshotting,
}

impl std::fmt::Display for CdcSourceStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CdcSourceStatus::Running => write!(f, "RUNNING"),
            CdcSourceStatus::Paused => write!(f, "PAUSED"),
            CdcSourceStatus::Stopped => write!(f, "STOPPED"),
            CdcSourceStatus::Error => write!(f, "ERROR"),
            CdcSourceStatus::Initializing => write!(f, "INITIALIZING"),
            CdcSourceStatus::Snapshotting => write!(f, "SNAPSHOTTING"),
        }
    }
}

/// CDC source metrics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CdcSourceMetrics {
    /// Total events processed
    pub events_processed: u64,
    /// Events by operation type
    pub events_by_operation: HashMap<String, u64>,
    /// Total bytes processed
    pub bytes_processed: u64,
    /// Current lag in milliseconds
    pub lag_ms: u64,
    /// Last event timestamp
    pub last_event_time: Option<DateTime<Utc>>,
    /// Last committed position
    pub last_position: Option<String>,
    /// Error count
    pub error_count: u64,
    /// Last error message
    pub last_error: Option<String>,
}

impl CdcSourceMetrics {
    /// Record an event
    pub fn record_event(&mut self, event: &CdcEvent) {
        self.events_processed += 1;
        let op_key = event.operation.to_string();
        *self.events_by_operation.entry(op_key).or_insert(0) += 1;
        self.last_event_time = Some(event.timestamp);
        self.last_position = Some(event.position.clone());

        // Calculate lag
        let now = Utc::now();
        let lag = now.signed_duration_since(event.timestamp);
        self.lag_ms = lag.num_milliseconds().max(0) as u64;
    }

    /// Record an error
    pub fn record_error(&mut self, error: &str) {
        self.error_count += 1;
        self.last_error = Some(error.to_string());
    }
}

/// CDC source information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcSourceInfo {
    /// Source name
    pub name: String,
    /// Source type (e.g., "postgres")
    pub source_type: String,
    /// Database name
    pub database: String,
    /// Tables being captured
    pub tables: Vec<String>,
    /// Current status
    pub status: CdcSourceStatus,
    /// Source metrics
    pub metrics: CdcSourceMetrics,
    /// Created timestamp
    pub created_at: DateTime<Utc>,
}

/// Trait for CDC sources
#[async_trait::async_trait]
pub trait CdcSource: Send + Sync {
    /// Get the source name
    fn name(&self) -> &str;

    /// Get source type
    fn source_type(&self) -> &str;

    /// Get source information
    fn info(&self) -> CdcSourceInfo;

    /// Start the CDC source
    async fn start(&self) -> Result<mpsc::Receiver<CdcEvent>>;

    /// Stop the CDC source
    async fn stop(&self) -> Result<()>;

    /// Pause the CDC source
    async fn pause(&self) -> Result<()>;

    /// Resume the CDC source
    async fn resume(&self) -> Result<()>;

    /// Get current status
    fn status(&self) -> CdcSourceStatus;

    /// Get current metrics
    fn metrics(&self) -> CdcSourceMetrics;

    /// Commit a position (for at-least-once delivery)
    async fn commit(&self, position: &str) -> Result<()>;
}

/// CDC Manager for managing multiple CDC sources
pub struct CdcManager {
    /// Active CDC sources
    sources: dashmap::DashMap<String, Arc<dyn CdcSource>>,
    /// Schema history store
    schema_history: Arc<schema_history::SchemaHistoryStore>,
    /// Snapshot managers per source
    snapshot_managers: dashmap::DashMap<String, Arc<snapshot::SnapshotManager>>,
    /// Heartbeat emitters per source
    heartbeats: dashmap::DashMap<String, Arc<heartbeat::CdcHeartbeat>>,
}

impl CdcManager {
    /// Create a new CDC manager
    pub fn new() -> Self {
        Self {
            sources: dashmap::DashMap::new(),
            schema_history: Arc::new(schema_history::SchemaHistoryStore::new()),
            snapshot_managers: dashmap::DashMap::new(),
            heartbeats: dashmap::DashMap::new(),
        }
    }

    /// Add a CDC source
    pub fn add_source(&self, source: Arc<dyn CdcSource>) -> Result<()> {
        let name = source.name().to_string();
        if self.sources.contains_key(&name) {
            return Err(crate::error::StreamlineError::Config(format!(
                "CDC source '{}' already exists",
                name
            )));
        }
        self.sources.insert(name, source);
        Ok(())
    }

    /// Remove a CDC source
    pub fn remove_source(&self, name: &str) -> Option<Arc<dyn CdcSource>> {
        self.sources.remove(name).map(|(_, v)| v)
    }

    /// Get a CDC source
    pub fn get_source(&self, name: &str) -> Option<Arc<dyn CdcSource>> {
        self.sources.get(name).map(|r| Arc::clone(&*r))
    }

    /// List all CDC sources
    pub fn list_sources(&self) -> Vec<CdcSourceInfo> {
        self.sources.iter().map(|r| r.value().info()).collect()
    }

    /// Start a specific source
    pub async fn start_source(&self, name: &str) -> Result<mpsc::Receiver<CdcEvent>> {
        let source = self.get_source(name).ok_or_else(|| {
            crate::error::StreamlineError::Config(format!("CDC source '{}' not found", name))
        })?;
        source.start().await
    }

    /// Stop a specific source
    pub async fn stop_source(&self, name: &str) -> Result<()> {
        let source = self.get_source(name).ok_or_else(|| {
            crate::error::StreamlineError::Config(format!("CDC source '{}' not found", name))
        })?;
        // Stop heartbeat if running
        if let Some(hb) = self.heartbeats.get(name) {
            hb.stop();
        }
        source.stop().await
    }

    /// Stop all sources
    pub async fn stop_all(&self) -> Result<()> {
        for entry in self.heartbeats.iter() {
            entry.value().stop();
        }
        for entry in self.sources.iter() {
            entry.value().stop().await?;
        }
        Ok(())
    }

    /// Get the shared schema history store
    pub fn schema_history(&self) -> &Arc<schema_history::SchemaHistoryStore> {
        &self.schema_history
    }

    /// Register a snapshot manager for a source
    pub fn register_snapshot_manager(
        &self,
        source_name: &str,
        strategy: snapshot::SnapshotStrategy,
    ) -> Arc<snapshot::SnapshotManager> {
        let mgr = Arc::new(snapshot::SnapshotManager::new(strategy));
        self.snapshot_managers
            .insert(source_name.to_string(), Arc::clone(&mgr));
        mgr
    }

    /// Get snapshot manager for a source
    pub fn get_snapshot_manager(
        &self,
        source_name: &str,
    ) -> Option<Arc<snapshot::SnapshotManager>> {
        self.snapshot_managers
            .get(source_name)
            .map(|r| Arc::clone(&*r))
    }

    /// Register and start a heartbeat for a source
    pub fn register_heartbeat(
        &self,
        source_name: &str,
        config: heartbeat::HeartbeatConfig,
    ) -> Arc<heartbeat::CdcHeartbeat> {
        let hb = heartbeat::CdcHeartbeat::new(source_name, config);
        self.heartbeats
            .insert(source_name.to_string(), Arc::clone(&hb));
        hb
    }

    /// Get heartbeat for a source
    pub fn get_heartbeat(&self, source_name: &str) -> Option<Arc<heartbeat::CdcHeartbeat>> {
        self.heartbeats.get(source_name).map(|r| Arc::clone(&*r))
    }

    /// Get status for a source
    pub fn get_status(&self, name: &str) -> Option<CdcSourceInfo> {
        self.sources.get(name).map(|r| r.value().info())
    }

    /// Check health of all running CDC sources.
    /// Returns a map of source name → health status description.
    pub fn check_health(&self) -> HashMap<String, CdcHealthStatus> {
        self.sources
            .iter()
            .map(|entry| {
                let name = entry.key().clone();
                let source = entry.value();
                let metrics = source.metrics();
                let status = source.status();

                let health = match status {
                    CdcSourceStatus::Running => {
                        if metrics.error_count > 0 {
                            CdcHealthStatus::Degraded {
                                error_count: metrics.error_count,
                                last_error: metrics.last_error.clone(),
                            }
                        } else {
                            CdcHealthStatus::Healthy
                        }
                    }
                    CdcSourceStatus::Error => CdcHealthStatus::Unhealthy {
                        reason: metrics
                            .last_error
                            .clone()
                            .unwrap_or_else(|| "unknown error".to_string()),
                    },
                    CdcSourceStatus::Stopped => CdcHealthStatus::Stopped,
                    _ => CdcHealthStatus::Healthy,
                };

                (name, health)
            })
            .collect()
    }

    /// Get replication lag for a specific source in milliseconds.
    /// Returns `None` if the source is not found or not running.
    pub fn replication_lag_ms(&self, name: &str) -> Option<u64> {
        let source = self.sources.get(name)?;
        let metrics = source.value().metrics();
        if source.value().status() == CdcSourceStatus::Running {
            Some(metrics.lag_ms)
        } else {
            None
        }
    }

    /// Get replication lag for all running sources.
    pub fn all_replication_lags(&self) -> HashMap<String, u64> {
        self.sources
            .iter()
            .filter(|entry| entry.value().status() == CdcSourceStatus::Running)
            .map(|entry| (entry.key().clone(), entry.value().metrics().lag_ms))
            .collect()
    }
}

/// Health status for a CDC source.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum CdcHealthStatus {
    /// Source is running with no errors.
    Healthy,
    /// Source is running but has encountered errors.
    Degraded {
        error_count: u64,
        last_error: Option<String>,
    },
    /// Source is not functioning.
    Unhealthy { reason: String },
    /// Source is intentionally stopped.
    Stopped,
}

impl Default for CdcManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Retry configuration for CDC source connections.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcRetryConfig {
    /// Maximum number of retry attempts before giving up
    pub max_retries: u32,
    /// Initial backoff duration in milliseconds
    pub initial_backoff_ms: u64,
    /// Maximum backoff duration in milliseconds
    pub max_backoff_ms: u64,
    /// Backoff multiplier (e.g., 2.0 for exponential backoff)
    pub backoff_multiplier: f64,
}

impl Default for CdcRetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 10,
            initial_backoff_ms: 1000,
            max_backoff_ms: 60_000,
            backoff_multiplier: 2.0,
        }
    }
}

impl CdcRetryConfig {
    /// Calculate the backoff duration for a given attempt number.
    pub fn backoff_for_attempt(&self, attempt: u32) -> std::time::Duration {
        let backoff_ms = (self.initial_backoff_ms as f64
            * self.backoff_multiplier.powi(attempt as i32)) as u64;
        let clamped = backoff_ms.min(self.max_backoff_ms);
        std::time::Duration::from_millis(clamped)
    }

    /// Returns true if retries are exhausted.
    pub fn is_exhausted(&self, attempt: u32) -> bool {
        attempt >= self.max_retries
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cdc_event_creation() {
        let event = CdcEvent::new(
            "testdb".to_string(),
            "public".to_string(),
            "users".to_string(),
            CdcOperation::Insert,
            "0/123456".to_string(),
        );

        assert_eq!(event.database, "testdb");
        assert_eq!(event.schema, "public");
        assert_eq!(event.table, "users");
        assert_eq!(event.operation, CdcOperation::Insert);
    }

    #[test]
    fn test_cdc_event_topic_name() {
        let event = CdcEvent::new(
            "testdb".to_string(),
            "public".to_string(),
            "users".to_string(),
            CdcOperation::Insert,
            "0/123456".to_string(),
        );

        assert_eq!(event.topic_name(None), "public.users");
        assert_eq!(event.topic_name(Some("cdc")), "cdc.public.users");
    }

    #[test]
    fn test_cdc_operation_display() {
        assert_eq!(CdcOperation::Insert.to_string(), "INSERT");
        assert_eq!(CdcOperation::Update.to_string(), "UPDATE");
        assert_eq!(CdcOperation::Delete.to_string(), "DELETE");
    }

    #[test]
    fn test_cdc_metrics() {
        let mut metrics = CdcSourceMetrics::default();

        let event = CdcEvent::new(
            "testdb".to_string(),
            "public".to_string(),
            "users".to_string(),
            CdcOperation::Insert,
            "0/123456".to_string(),
        );

        metrics.record_event(&event);
        assert_eq!(metrics.events_processed, 1);
        assert_eq!(metrics.events_by_operation.get("INSERT"), Some(&1));
    }

    #[test]
    fn test_cdc_manager() {
        let manager = CdcManager::new();
        assert!(manager.list_sources().is_empty());
    }

    #[test]
    fn test_cdc_manager_health_check_empty() {
        let manager = CdcManager::new();
        let health = manager.check_health();
        assert!(health.is_empty());
    }

    #[test]
    fn test_cdc_manager_replication_lag_missing() {
        let manager = CdcManager::new();
        assert!(manager.replication_lag_ms("nonexistent").is_none());
    }

    #[test]
    fn test_cdc_manager_all_replication_lags_empty() {
        let manager = CdcManager::new();
        assert!(manager.all_replication_lags().is_empty());
    }
}
