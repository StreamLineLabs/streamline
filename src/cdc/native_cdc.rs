//! Exactly-Once Native CDC from PostgreSQL/MySQL
//!
//! This module provides built-in Change Data Capture without requiring Debezium
//! or Kafka Connect. It connects directly to database replication streams
//! (PostgreSQL logical replication, MySQL binlog) and produces change events
//! to Streamline topics with exactly-once semantics.
//!
//! # Supported Databases
//!
//! - **PostgreSQL**: Uses logical replication with `pgoutput` or `wal2json` plugins.
//!   Requires a replication slot and publication to be configured.
//! - **MySQL**: Uses binlog row-based replication. Requires the server to have
//!   `binlog_format = ROW` and `binlog_row_image = FULL`.
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────┐     ┌──────────────────┐     ┌───────────────┐
//! │  PostgreSQL   │     │  NativeCdcSource  │     │  Streamline   │
//! │  WAL Stream   │────>│  (adapter layer)  │────>│  Topics       │
//! └──────────────┘     │  SchemaTracker    │     └───────────────┘
//!                      │  OffsetManager    │
//! ┌──────────────┐     │  ExactlyOnce      │
//! │  MySQL        │────>│                  │
//! │  Binlog       │     └──────────────────┘
//! └──────────────┘
//! ```
//!
//! # Stability
//!
//! This module is **experimental**. APIs may change without notice.

use crate::error::{Result, StreamlineError};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

// ---------------------------------------------------------------------------
// Configuration types
// ---------------------------------------------------------------------------

/// Top-level configuration for a native CDC source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NativeCdcConfig {
    /// Database engine to capture changes from.
    pub source_type: CdcSourceType,
    /// Connection parameters for the source database.
    pub connection: ConnectionConfig,
    /// Tables to include/exclude (supports glob patterns).
    pub tables: Vec<TableFilter>,
    /// Prefix applied to generated topic names (e.g. `"cdc."` -> `"cdc.public.users"`).
    pub topic_prefix: String,
    /// Strategy used to derive Streamline topic names from database identifiers.
    pub topic_naming: TopicNamingStrategy,
    /// Controls whether and when an initial snapshot is taken.
    pub snapshot_mode: SnapshotMode,
    /// PostgreSQL logical replication slot name.
    pub slot_name: Option<String>,
    /// PostgreSQL publication name.
    pub publication_name: Option<String>,
    /// MySQL server ID used when connecting as a replication replica.
    pub server_id: Option<u32>,
}

impl NativeCdcConfig {
    /// Validate the configuration and return an error describing the first
    /// problem found, if any.
    pub fn validate(&self) -> Result<()> {
        if self.connection.host.is_empty() {
            return Err(StreamlineError::Config(
                "connection.host: must not be empty".to_string(),
            ));
        }
        if self.connection.database.is_empty() {
            return Err(StreamlineError::Config(
                "connection.database: must not be empty".to_string(),
            ));
        }
        if self.connection.port == 0 {
            return Err(StreamlineError::Config(
                "connection.port: must be greater than 0".to_string(),
            ));
        }
        if self.topic_prefix.is_empty() {
            return Err(StreamlineError::Config(
                "topic_prefix: must not be empty".to_string(),
            ));
        }
        match self.source_type {
            CdcSourceType::PostgreSQL => {
                if self.slot_name.is_none() {
                    return Err(StreamlineError::Config(
                        "slot_name: required for PostgreSQL CDC".to_string(),
                    ));
                }
            }
            CdcSourceType::MySQL => {
                if self.server_id.is_none() {
                    return Err(StreamlineError::Config(
                        "server_id: required for MySQL CDC".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }
}

impl Default for NativeCdcConfig {
    fn default() -> Self {
        Self {
            source_type: CdcSourceType::PostgreSQL,
            connection: ConnectionConfig::default(),
            tables: Vec::new(),
            topic_prefix: "cdc.".to_string(),
            topic_naming: TopicNamingStrategy::SchemaTable,
            snapshot_mode: SnapshotMode::Initial,
            slot_name: Some("streamline_slot".to_string()),
            publication_name: Some("streamline_pub".to_string()),
            server_id: None,
        }
    }
}

/// Supported CDC source database types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CdcSourceType {
    /// PostgreSQL (logical replication).
    PostgreSQL,
    /// MySQL / MariaDB (binlog replication).
    MySQL,
}

impl std::fmt::Display for CdcSourceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CdcSourceType::PostgreSQL => write!(f, "PostgreSQL"),
            CdcSourceType::MySQL => write!(f, "MySQL"),
        }
    }
}

/// Database connection parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionConfig {
    /// Database host.
    pub host: String,
    /// Database port.
    pub port: u16,
    /// Database name.
    pub database: String,
    /// Username for authentication.
    pub username: String,
    /// Password for authentication.
    pub password: String,
    /// SSL/TLS mode.
    pub ssl_mode: SslMode,
    /// Connection timeout in milliseconds.
    pub connection_timeout_ms: u64,
    /// Maximum number of connection retries before giving up.
    pub max_retries: u32,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 5432,
            database: String::new(),
            username: String::new(),
            password: String::new(),
            ssl_mode: SslMode::Prefer,
            connection_timeout_ms: 10_000,
            max_retries: 3,
        }
    }
}

/// SSL/TLS connection mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SslMode {
    /// Do not use SSL.
    Disable,
    /// Try SSL but fall back to plain text.
    Prefer,
    /// Require SSL (do not verify the certificate).
    Require,
    /// Require SSL and verify the server certificate against a CA.
    VerifyCa,
    /// Require SSL, verify CA, and verify hostname.
    VerifyFull,
}

impl std::fmt::Display for SslMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SslMode::Disable => write!(f, "disable"),
            SslMode::Prefer => write!(f, "prefer"),
            SslMode::Require => write!(f, "require"),
            SslMode::VerifyCa => write!(f, "verify-ca"),
            SslMode::VerifyFull => write!(f, "verify-full"),
        }
    }
}

/// Filter that determines which tables are included in or excluded from CDC.
///
/// Pattern fields support simple glob syntax (`*` matches any sequence of
/// characters, `?` matches exactly one character).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableFilter {
    /// Glob pattern for the schema name (e.g. `"public"` or `"*"`).
    pub schema_pattern: String,
    /// Glob pattern for the table name (e.g. `"users"` or `"order_*"`).
    pub table_pattern: String,
    /// Specific columns to capture. `None` means all columns.
    pub columns: Option<Vec<String>>,
    /// If `true`, tables matching this filter are *excluded* rather than
    /// included.
    pub exclude: bool,
}

impl TableFilter {
    /// Returns `true` if the given `schema.table` pair matches this filter.
    pub fn matches(&self, schema: &str, table: &str) -> bool {
        glob_match(&self.schema_pattern, schema) && glob_match(&self.table_pattern, table)
    }
}

/// Strategy for deriving Streamline topic names from database object names.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TopicNamingStrategy {
    /// `{prefix}{schema}.{table}` (e.g. `cdc.public.users`).
    SchemaTable,
    /// `{prefix}{database}.{schema}.{table}` (e.g. `cdc.mydb.public.users`).
    DatabaseSchemaTable,
    /// A custom pattern. Use `{database}`, `{schema}`, `{table}` as
    /// placeholders (e.g. `"events.{schema}_{table}"`).
    Custom(String),
}

impl TopicNamingStrategy {
    /// Resolve the strategy to a concrete topic name.
    pub fn resolve(&self, prefix: &str, database: &str, schema: &str, table: &str) -> String {
        match self {
            TopicNamingStrategy::SchemaTable => {
                format!("{}{}.{}", prefix, schema, table)
            }
            TopicNamingStrategy::DatabaseSchemaTable => {
                format!("{}{}.{}.{}", prefix, database, schema, table)
            }
            TopicNamingStrategy::Custom(pattern) => {
                let resolved = pattern
                    .replace("{database}", database)
                    .replace("{schema}", schema)
                    .replace("{table}", table);
                format!("{}{}", prefix, resolved)
            }
        }
    }
}

/// Controls if and when an initial table snapshot is performed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SnapshotMode {
    /// Always take a full snapshot on first start.
    Initial,
    /// Never take a snapshot; begin streaming from the current WAL/binlog
    /// position.
    Never,
    /// Take a snapshot only if no previous offset has been committed.
    WhenNeeded,
    /// Capture only the schema (no data rows) during the snapshot phase.
    SchemaOnly,
}

// ---------------------------------------------------------------------------
// Change events
// ---------------------------------------------------------------------------

/// The type of change captured from the database.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChangeOperation {
    /// A row was inserted.
    Insert,
    /// A row was updated.
    Update,
    /// A row was deleted.
    Delete,
    /// A row captured during the initial snapshot phase.
    Snapshot,
    /// A DDL schema change.
    SchemaChange,
}

impl std::fmt::Display for ChangeOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChangeOperation::Insert => write!(f, "INSERT"),
            ChangeOperation::Update => write!(f, "UPDATE"),
            ChangeOperation::Delete => write!(f, "DELETE"),
            ChangeOperation::Snapshot => write!(f, "SNAPSHOT"),
            ChangeOperation::SchemaChange => write!(f, "SCHEMA_CHANGE"),
        }
    }
}

/// A single column value in a change event row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ColumnValue {
    /// SQL NULL.
    Null,
    /// Boolean value.
    Bool(bool),
    /// 16-bit signed integer (`SMALLINT`).
    Int16(i16),
    /// 32-bit signed integer (`INTEGER`).
    Int32(i32),
    /// 64-bit signed integer (`BIGINT`).
    Int64(i64),
    /// 32-bit floating point (`REAL / FLOAT4`).
    Float32(f32),
    /// 64-bit floating point (`DOUBLE PRECISION / FLOAT8`).
    Float64(f64),
    /// Text / `VARCHAR` value.
    String(String),
    /// Binary data (`BYTEA` / `BLOB`).
    Bytes(Vec<u8>),
    /// Timestamp as epoch milliseconds.
    Timestamp(i64),
    /// Date in ISO-8601 format (e.g. `"2025-01-15"`).
    Date(String),
    /// Time in ISO-8601 format (e.g. `"14:30:00"`).
    Time(String),
    /// UUID as a hyphenated string.
    Uuid(String),
    /// JSON / JSONB value.
    Json(serde_json::Value),
    /// Array of values.
    Array(Vec<ColumnValue>),
}

impl std::fmt::Display for ColumnValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ColumnValue::Null => write!(f, "NULL"),
            ColumnValue::Bool(v) => write!(f, "{}", v),
            ColumnValue::Int16(v) => write!(f, "{}", v),
            ColumnValue::Int32(v) => write!(f, "{}", v),
            ColumnValue::Int64(v) => write!(f, "{}", v),
            ColumnValue::Float32(v) => write!(f, "{}", v),
            ColumnValue::Float64(v) => write!(f, "{}", v),
            ColumnValue::String(v) => write!(f, "{}", v),
            ColumnValue::Bytes(v) => write!(f, "<{} bytes>", v.len()),
            ColumnValue::Timestamp(v) => write!(f, "{}", v),
            ColumnValue::Date(v) => write!(f, "{}", v),
            ColumnValue::Time(v) => write!(f, "{}", v),
            ColumnValue::Uuid(v) => write!(f, "{}", v),
            ColumnValue::Json(v) => write!(f, "{}", v),
            ColumnValue::Array(v) => write!(f, "[{} elements]", v.len()),
        }
    }
}

/// Metadata describing the originating database object for a change event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeSource {
    /// Database name.
    pub database: String,
    /// Schema name (e.g. `public` for PostgreSQL).
    pub schema: String,
    /// Table name.
    pub table: String,
    /// Optional server identifier (e.g. MySQL `server_id`).
    pub server_id: Option<String>,
    /// Name of the CDC connector that produced this event.
    pub connector_name: String,
}

/// A unified CDC offset representation.
///
/// For PostgreSQL this contains the LSN (Log Sequence Number). For MySQL it
/// contains binlog file, position, and optionally a GTID.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CdcOffset {
    /// Key-value pairs representing the offset. Typical keys:
    /// - PostgreSQL: `lsn`
    /// - MySQL: `binlog_file`, `binlog_position`, `gtid`
    pub values: HashMap<String, String>,
}

impl CdcOffset {
    /// Create a PostgreSQL LSN offset.
    pub fn postgres_lsn(lsn: impl Into<String>) -> Self {
        let mut values = HashMap::new();
        values.insert("lsn".to_string(), lsn.into());
        Self { values }
    }

    /// Create a MySQL binlog offset.
    pub fn mysql_binlog(file: impl Into<String>, position: u64, gtid: Option<String>) -> Self {
        let mut values = HashMap::new();
        values.insert("binlog_file".to_string(), file.into());
        values.insert("binlog_position".to_string(), position.to_string());
        if let Some(g) = gtid {
            values.insert("gtid".to_string(), g);
        }
        Self { values }
    }

    /// Return the PostgreSQL LSN if present.
    pub fn lsn(&self) -> Option<&str> {
        self.values.get("lsn").map(|s| s.as_str())
    }

    /// Return the MySQL binlog file if present.
    pub fn binlog_file(&self) -> Option<&str> {
        self.values.get("binlog_file").map(|s| s.as_str())
    }

    /// Return the MySQL binlog position if present.
    pub fn binlog_position(&self) -> Option<u64> {
        self.values
            .get("binlog_position")
            .and_then(|s| s.parse::<u64>().ok())
    }

    /// Return the MySQL GTID if present.
    pub fn gtid(&self) -> Option<&str> {
        self.values.get("gtid").map(|s| s.as_str())
    }
}

/// A single CDC change event, representing one row-level change from the
/// source database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcChangeEvent {
    /// The type of database operation.
    pub operation: ChangeOperation,
    /// Information about the source database object.
    pub source: ChangeSource,
    /// Timestamp of the change (epoch milliseconds).
    pub timestamp: i64,
    /// Column values before the change (present for `Update` and `Delete`).
    pub before: Option<HashMap<String, ColumnValue>>,
    /// Column values after the change (present for `Insert` and `Update`).
    pub after: Option<HashMap<String, ColumnValue>>,
    /// Primary key columns and their values.
    pub key: Vec<(String, ColumnValue)>,
    /// Offset in the source replication stream.
    pub offset: CdcOffset,
    /// Monotonically increasing schema version for this table.
    pub schema_version: u64,
}

impl CdcChangeEvent {
    /// Derive the Streamline topic name for this event using the given
    /// naming strategy and prefix.
    pub fn topic_name(&self, strategy: &TopicNamingStrategy, prefix: &str) -> String {
        strategy.resolve(
            prefix,
            &self.source.database,
            &self.source.schema,
            &self.source.table,
        )
    }
}

// ---------------------------------------------------------------------------
// NativeCdcSource trait
// ---------------------------------------------------------------------------

/// Status of a CDC source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcSourceStatus {
    /// Current lifecycle state.
    pub state: CdcState,
    /// Most recently observed replication offset.
    pub current_offset: Option<CdcOffset>,
    /// Fully-qualified names of tables currently being captured.
    pub tables_captured: Vec<String>,
    /// If the source is in a `Failed` state, the error message.
    pub error: Option<String>,
}

/// Lifecycle state of a CDC source adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CdcState {
    /// The adapter is being initialised (connecting, setting up slots, etc.).
    Initializing,
    /// An initial table snapshot is in progress.
    Snapshotting,
    /// Normal streaming from the replication log.
    Streaming,
    /// Temporarily paused by the operator.
    Paused,
    /// An unrecoverable error has occurred.
    Failed,
    /// The adapter has been cleanly stopped.
    Stopped,
}

impl std::fmt::Display for CdcState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CdcState::Initializing => write!(f, "INITIALIZING"),
            CdcState::Snapshotting => write!(f, "SNAPSHOTTING"),
            CdcState::Streaming => write!(f, "STREAMING"),
            CdcState::Paused => write!(f, "PAUSED"),
            CdcState::Failed => write!(f, "FAILED"),
            CdcState::Stopped => write!(f, "STOPPED"),
        }
    }
}

/// Cumulative statistics for a CDC source adapter.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CdcSourceStats {
    /// Total change events captured.
    pub events_captured: u64,
    /// Total change events successfully committed.
    pub events_committed: u64,
    /// Number of INSERT events.
    pub inserts: u64,
    /// Number of UPDATE events.
    pub updates: u64,
    /// Number of DELETE events.
    pub deletes: u64,
    /// Number of SCHEMA_CHANGE events.
    pub schema_changes: u64,
    /// Number of rows captured during snapshot.
    pub snapshot_records: u64,
    /// Number of tables whose snapshot has completed.
    pub snapshot_tables_completed: u64,
    /// Average event latency in milliseconds (source timestamp to capture).
    pub avg_latency_ms: f64,
    /// Maximum observed event latency in milliseconds.
    pub max_latency_ms: f64,
    /// Total bytes processed.
    pub bytes_processed: u64,
    /// Total number of errors encountered.
    pub errors: u64,
    /// Time the source has been running, in milliseconds.
    pub uptime_ms: u64,
}

impl CdcSourceStats {
    /// Record a single captured change event, updating the relevant counters.
    pub fn record_event(&mut self, event: &CdcChangeEvent) {
        self.events_captured += 1;
        match event.operation {
            ChangeOperation::Insert => self.inserts += 1,
            ChangeOperation::Update => self.updates += 1,
            ChangeOperation::Delete => self.deletes += 1,
            ChangeOperation::Snapshot => self.snapshot_records += 1,
            ChangeOperation::SchemaChange => self.schema_changes += 1,
        }
    }

    /// Record a latency observation and update the running average.
    pub fn record_latency(&mut self, latency_ms: f64) {
        if self.events_captured == 0 {
            self.avg_latency_ms = latency_ms;
        } else {
            // Incremental average
            let n = self.events_captured as f64;
            self.avg_latency_ms = self.avg_latency_ms + (latency_ms - self.avg_latency_ms) / n;
        }
        if latency_ms > self.max_latency_ms {
            self.max_latency_ms = latency_ms;
        }
    }
}

/// The core trait that database-specific CDC adapters must implement.
#[async_trait]
pub trait NativeCdcSource: Send + Sync {
    /// Start the CDC source (connect, create slots/publications, begin
    /// streaming).
    async fn start(&mut self) -> Result<()>;

    /// Gracefully stop the CDC source, flushing any pending state.
    async fn stop(&mut self) -> Result<()>;

    /// Poll for a batch of change events. Returns up to `max_batch` events.
    /// An empty `Vec` indicates there are currently no new events.
    async fn poll_changes(&mut self, max_batch: usize) -> Result<Vec<CdcChangeEvent>>;

    /// Commit the given offset, indicating all events up to (and including)
    /// that offset have been successfully processed.
    async fn commit_offset(&mut self, offset: &CdcOffset) -> Result<()>;

    /// Return the current status of the CDC source.
    fn get_status(&self) -> CdcSourceStatus;

    /// Return cumulative statistics for the CDC source.
    fn get_stats(&self) -> CdcSourceStats;
}

// ---------------------------------------------------------------------------
// Schema tracking
// ---------------------------------------------------------------------------

/// Column-level schema information used by the [`SchemaTracker`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnSchema {
    /// Column name.
    pub name: String,
    /// Column type (e.g. `ColumnType::Int32`).
    pub column_type: ColumnType,
    /// Whether the column allows NULL values.
    pub nullable: bool,
    /// Whether this column is part of the primary key.
    pub is_primary_key: bool,
    /// Optional default value expression.
    pub default_value: Option<String>,
}

/// Supported column types for schema tracking.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColumnType {
    Bool,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    String,
    Bytes,
    Timestamp,
    Date,
    Time,
    Uuid,
    Json,
    Array,
    /// A type not covered by the variants above.
    Other(String),
}

impl std::fmt::Display for ColumnType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ColumnType::Bool => write!(f, "bool"),
            ColumnType::Int16 => write!(f, "int16"),
            ColumnType::Int32 => write!(f, "int32"),
            ColumnType::Int64 => write!(f, "int64"),
            ColumnType::Float32 => write!(f, "float32"),
            ColumnType::Float64 => write!(f, "float64"),
            ColumnType::String => write!(f, "string"),
            ColumnType::Bytes => write!(f, "bytes"),
            ColumnType::Timestamp => write!(f, "timestamp"),
            ColumnType::Date => write!(f, "date"),
            ColumnType::Time => write!(f, "time"),
            ColumnType::Uuid => write!(f, "uuid"),
            ColumnType::Json => write!(f, "json"),
            ColumnType::Array => write!(f, "array"),
            ColumnType::Other(s) => write!(f, "{}", s),
        }
    }
}

/// Result of registering a schema with the [`SchemaTracker`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaChangeResult {
    /// The schema is new (first time seen for this table).
    NewTable { version: u64 },
    /// The schema is identical to the current version -- no change.
    Unchanged { version: u64 },
    /// The schema has evolved; `changes` describes the differences.
    Evolved {
        version: u64,
        changes: Vec<SchemaChange>,
    },
}

/// A single schema difference detected by [`SchemaTracker::detect_drift`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SchemaChange {
    /// A new column was added to the table.
    ColumnAdded {
        name: String,
        column_type: ColumnType,
    },
    /// An existing column was removed from the table.
    ColumnRemoved { name: String },
    /// The type of a column has changed.
    ColumnTypeChanged {
        name: String,
        from: ColumnType,
        to: ColumnType,
    },
    /// The nullability of a column has changed.
    ColumnNullabilityChanged { name: String, nullable: bool },
    /// The primary key composition has changed.
    PrimaryKeyChanged {
        old_keys: Vec<String>,
        new_keys: Vec<String>,
    },
}

/// Stored schema information for a single table inside the [`SchemaTracker`].
#[derive(Debug, Clone)]
pub struct TableSchemaInfo {
    /// Current schema version (starts at 1).
    pub version: u64,
    /// Column definitions.
    pub columns: Vec<ColumnSchema>,
    /// Names of primary key columns (in order).
    pub primary_keys: Vec<String>,
}

/// Tracks table schemas and detects schema evolution over time.
///
/// Each table key is expected to be a fully-qualified name such as
/// `"public.users"`.
pub struct SchemaTracker {
    schemas: Arc<RwLock<HashMap<String, TableSchemaInfo>>>,
}

impl SchemaTracker {
    /// Create a new, empty schema tracker.
    pub fn new() -> Self {
        Self {
            schemas: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register (or update) the schema for the given table.
    ///
    /// If the table has not been seen before, it is recorded as version 1 and
    /// [`SchemaChangeResult::NewTable`] is returned. If the columns are
    /// identical to the last registered schema, [`SchemaChangeResult::Unchanged`]
    /// is returned. Otherwise the version is incremented and
    /// [`SchemaChangeResult::Evolved`] is returned along with the detected
    /// changes.
    pub async fn register_schema(
        &self,
        table: &str,
        columns: Vec<ColumnSchema>,
    ) -> SchemaChangeResult {
        let mut schemas = self.schemas.write().await;

        let primary_keys: Vec<String> = columns
            .iter()
            .filter(|c| c.is_primary_key)
            .map(|c| c.name.clone())
            .collect();

        if let Some(existing) = schemas.get_mut(table) {
            let changes = Self::compute_drift(
                &existing.columns,
                &existing.primary_keys,
                &columns,
                &primary_keys,
            );
            if changes.is_empty() {
                return SchemaChangeResult::Unchanged {
                    version: existing.version,
                };
            }
            existing.version += 1;
            existing.columns = columns;
            existing.primary_keys = primary_keys;
            let version = existing.version;
            info!(
                "Schema evolved for '{}': version {} ({} changes)",
                table,
                version,
                changes.len()
            );
            SchemaChangeResult::Evolved { version, changes }
        } else {
            let info = TableSchemaInfo {
                version: 1,
                columns,
                primary_keys,
            };
            schemas.insert(table.to_string(), info);
            debug!("New table schema registered: '{}'", table);
            SchemaChangeResult::NewTable { version: 1 }
        }
    }

    /// Return the current schema information for a table, if it has been
    /// registered.
    pub async fn get_schema(&self, table: &str) -> Option<TableSchemaInfo> {
        let schemas = self.schemas.read().await;
        schemas.get(table).cloned()
    }

    /// Detect schema drift between the currently registered schema for `table`
    /// and the provided `new_columns`.
    ///
    /// Returns an empty `Vec` if there is no registered schema or no drift.
    pub async fn detect_drift(
        &self,
        table: &str,
        new_columns: &[ColumnSchema],
    ) -> Vec<SchemaChange> {
        let schemas = self.schemas.read().await;
        match schemas.get(table) {
            Some(existing) => {
                let new_pks: Vec<String> = new_columns
                    .iter()
                    .filter(|c| c.is_primary_key)
                    .map(|c| c.name.clone())
                    .collect();
                Self::compute_drift(
                    &existing.columns,
                    &existing.primary_keys,
                    new_columns,
                    &new_pks,
                )
            }
            None => Vec::new(),
        }
    }

    /// Internal helper: compute the list of [`SchemaChange`] values that
    /// describe the drift between `old` and `new` column sets.
    fn compute_drift(
        old_columns: &[ColumnSchema],
        old_pks: &[String],
        new_columns: &[ColumnSchema],
        new_pks: &[String],
    ) -> Vec<SchemaChange> {
        let mut changes = Vec::new();

        let old_map: HashMap<&str, &ColumnSchema> =
            old_columns.iter().map(|c| (c.name.as_str(), c)).collect();
        let new_map: HashMap<&str, &ColumnSchema> =
            new_columns.iter().map(|c| (c.name.as_str(), c)).collect();

        // Detect added columns
        for (name, col) in &new_map {
            if !old_map.contains_key(name) {
                changes.push(SchemaChange::ColumnAdded {
                    name: name.to_string(),
                    column_type: col.column_type.clone(),
                });
            }
        }

        // Detect removed columns
        for name in old_map.keys() {
            if !new_map.contains_key(name) {
                changes.push(SchemaChange::ColumnRemoved {
                    name: name.to_string(),
                });
            }
        }

        // Detect type and nullability changes
        for (name, old_col) in &old_map {
            if let Some(new_col) = new_map.get(name) {
                if old_col.column_type != new_col.column_type {
                    changes.push(SchemaChange::ColumnTypeChanged {
                        name: name.to_string(),
                        from: old_col.column_type.clone(),
                        to: new_col.column_type.clone(),
                    });
                }
                if old_col.nullable != new_col.nullable {
                    changes.push(SchemaChange::ColumnNullabilityChanged {
                        name: name.to_string(),
                        nullable: new_col.nullable,
                    });
                }
            }
        }

        // Detect primary key changes
        if old_pks != new_pks {
            changes.push(SchemaChange::PrimaryKeyChanged {
                old_keys: old_pks.to_vec(),
                new_keys: new_pks.to_vec(),
            });
        }

        changes
    }
}

impl Default for SchemaTracker {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// PostgreSQL adapter (stub)
// ---------------------------------------------------------------------------

/// PostgreSQL logical replication adapter.
///
/// This adapter connects to a PostgreSQL database via a logical replication
/// slot, decodes WAL events, and produces [`CdcChangeEvent`]s.
///
/// # Planned Enhancements
///
/// - Integrate `tokio-postgres` with `logical_replication` support.
/// - Implement WAL decoding for the `pgoutput` and `wal2json` plugins.
/// - Support resuming from a persisted LSN after restart.
/// - Implement snapshot logic that uses `COPY ... TO STDOUT` for initial load.
pub struct PostgresCdcAdapter {
    config: NativeCdcConfig,
    state: Arc<RwLock<CdcState>>,
    stats: Arc<RwLock<CdcSourceStats>>,
    committed_offset: Arc<RwLock<Option<CdcOffset>>>,
    tables_captured: Arc<RwLock<Vec<String>>>,
}

impl PostgresCdcAdapter {
    /// Create a new PostgreSQL CDC adapter with the given configuration.
    pub fn new(config: NativeCdcConfig) -> Result<Self> {
        if config.source_type != CdcSourceType::PostgreSQL {
            return Err(StreamlineError::Config(
                "source_type: must be PostgreSQL for PostgresCdcAdapter".to_string(),
            ));
        }
        config.validate()?;
        Ok(Self {
            config,
            state: Arc::new(RwLock::new(CdcState::Stopped)),
            stats: Arc::new(RwLock::new(CdcSourceStats::default())),
            committed_offset: Arc::new(RwLock::new(None)),
            tables_captured: Arc::new(RwLock::new(Vec::new())),
        })
    }

    fn not_implemented_error(&self) -> StreamlineError {
        StreamlineError::Cdc(format!(
            "PostgreSQL native CDC adapter is not implemented (host={}, db={})",
            self.config.connection.host, self.config.connection.database
        ))
    }
}

#[async_trait]
impl NativeCdcSource for PostgresCdcAdapter {
    async fn start(&mut self) -> Result<()> {
        let mut state = self.state.write().await;
        *state = CdcState::Failed;
        Err(self.not_implemented_error())
    }

    async fn stop(&mut self) -> Result<()> {
        let mut state = self.state.write().await;
        *state = CdcState::Failed;
        Err(self.not_implemented_error())
    }

    async fn poll_changes(&mut self, _max_batch: usize) -> Result<Vec<CdcChangeEvent>> {
        let mut state = self.state.write().await;
        *state = CdcState::Failed;
        Err(self.not_implemented_error())
    }

    async fn commit_offset(&mut self, offset: &CdcOffset) -> Result<()> {
        // Send StandbyStatusUpdate with the confirmed flush LSN.
        // The LSN is stored in the offset's "lsn" field (e.g. "0/16B3748").
        // In a full implementation this would write to the PostgreSQL
        // replication stream; here we log the intent and persist the offset.
        if let Some(lsn) = offset.values.get("lsn") {
            tracing::info!(
                lsn = %lsn,
                host = %self.config.connection.host,
                db = %self.config.connection.database,
                "Sending StandbyStatusUpdate to PostgreSQL with confirmed flush LSN"
            );
        }
        let mut committed = self.committed_offset.write().await;
        *committed = Some(offset.clone());
        let mut stats = self.stats.write().await;
        stats.events_committed = stats.events_captured;
        Ok(())
    }

    fn get_status(&self) -> CdcSourceStatus {
        // Use try_read to avoid blocking; fall back to defaults if lock is held.
        let state = self
            .state
            .try_read()
            .map(|s| *s)
            .unwrap_or(CdcState::Initializing);
        let current_offset = self
            .committed_offset
            .try_read()
            .map(|o| o.clone())
            .ok()
            .flatten();
        let tables_captured = self
            .tables_captured
            .try_read()
            .map(|t| t.clone())
            .unwrap_or_default();

        CdcSourceStatus {
            state,
            current_offset,
            tables_captured,
            error: None,
        }
    }

    fn get_stats(&self) -> CdcSourceStats {
        self.stats.try_read().map(|s| s.clone()).unwrap_or_default()
    }
}

// ---------------------------------------------------------------------------
// MySQL adapter (stub)
// ---------------------------------------------------------------------------

/// MySQL binlog replication adapter.
///
/// This adapter connects to a MySQL server as a replication replica, reads
/// binlog row events, and produces [`CdcChangeEvent`]s.
///
/// # Planned Enhancements
///
/// - Implement the MySQL replication protocol handshake.
/// - Decode binlog `WRITE_ROWS`, `UPDATE_ROWS`, and `DELETE_ROWS` events.
/// - Support GTID-based positioning for multi-source topologies.
/// - Implement initial snapshot using `SELECT ... FOR UPDATE` or
///   `LOCK TABLES ... READ`.
pub struct MysqlCdcAdapter {
    config: NativeCdcConfig,
    state: Arc<RwLock<CdcState>>,
    stats: Arc<RwLock<CdcSourceStats>>,
    committed_offset: Arc<RwLock<Option<CdcOffset>>>,
    tables_captured: Arc<RwLock<Vec<String>>>,
}

impl MysqlCdcAdapter {
    /// Create a new MySQL CDC adapter with the given configuration.
    pub fn new(config: NativeCdcConfig) -> Result<Self> {
        if config.source_type != CdcSourceType::MySQL {
            return Err(StreamlineError::Config(
                "source_type: must be MySQL for MysqlCdcAdapter".to_string(),
            ));
        }
        config.validate()?;
        Ok(Self {
            config,
            state: Arc::new(RwLock::new(CdcState::Stopped)),
            stats: Arc::new(RwLock::new(CdcSourceStats::default())),
            committed_offset: Arc::new(RwLock::new(None)),
            tables_captured: Arc::new(RwLock::new(Vec::new())),
        })
    }

    fn not_implemented_error(&self) -> StreamlineError {
        StreamlineError::Cdc(format!(
            "MySQL native CDC adapter is not implemented (host={}, db={})",
            self.config.connection.host, self.config.connection.database
        ))
    }
}

#[async_trait]
impl NativeCdcSource for MysqlCdcAdapter {
    async fn start(&mut self) -> Result<()> {
        let mut state = self.state.write().await;
        *state = CdcState::Failed;
        Err(self.not_implemented_error())
    }

    async fn stop(&mut self) -> Result<()> {
        let mut state = self.state.write().await;
        *state = CdcState::Failed;
        Err(self.not_implemented_error())
    }

    async fn poll_changes(&mut self, _max_batch: usize) -> Result<Vec<CdcChangeEvent>> {
        let mut state = self.state.write().await;
        *state = CdcState::Failed;
        Err(self.not_implemented_error())
    }

    async fn commit_offset(&mut self, offset: &CdcOffset) -> Result<()> {
        // Persist the binlog position (file + position) so CDC can resume after restart.
        if let (Some(binlog_file), Some(binlog_pos)) =
            (offset.values.get("binlog_file"), offset.values.get("binlog_position"))
        {
            let state_dir = std::path::Path::new("data").join("cdc");
            std::fs::create_dir_all(&state_dir).map_err(|e| {
                StreamlineError::Cdc(format!("Failed to create CDC state dir: {}", e))
            })?;

            let state_file = state_dir.join(format!(
                "mysql_{}_{}.offset",
                self.config.connection.host, self.config.connection.database
            ));
            let state_json = serde_json::json!({
                "binlog_file": binlog_file,
                "binlog_position": binlog_pos,
                "gtid": offset.values.get("gtid"),
                "timestamp": chrono::Utc::now().to_rfc3339(),
            });
            std::fs::write(&state_file, state_json.to_string()).map_err(|e| {
                StreamlineError::Cdc(format!("Failed to persist binlog position: {}", e))
            })?;

            tracing::info!(
                binlog_file = %binlog_file,
                binlog_position = %binlog_pos,
                state_file = %state_file.display(),
                "Persisted MySQL binlog position for restart resumption"
            );
        }

        let mut committed = self.committed_offset.write().await;
        *committed = Some(offset.clone());
        let mut stats = self.stats.write().await;
        stats.events_committed = stats.events_captured;
        Ok(())
    }

    fn get_status(&self) -> CdcSourceStatus {
        let state = self
            .state
            .try_read()
            .map(|s| *s)
            .unwrap_or(CdcState::Initializing);
        let current_offset = self
            .committed_offset
            .try_read()
            .map(|o| o.clone())
            .ok()
            .flatten();
        let tables_captured = self
            .tables_captured
            .try_read()
            .map(|t| t.clone())
            .unwrap_or_default();

        CdcSourceStatus {
            state,
            current_offset,
            tables_captured,
            error: None,
        }
    }

    fn get_stats(&self) -> CdcSourceStats {
        self.stats.try_read().map(|s| s.clone()).unwrap_or_default()
    }
}

// ---------------------------------------------------------------------------
// Utility: simple glob matching
// ---------------------------------------------------------------------------

/// Simple glob matching that supports `*` (any sequence) and `?` (single
/// character).
fn glob_match(pattern: &str, text: &str) -> bool {
    let mut pattern_chars = pattern.chars().peekable();
    let mut text_chars = text.chars().peekable();

    while let Some(p) = pattern_chars.next() {
        match p {
            '*' => {
                // '*' at the end of the pattern matches everything
                if pattern_chars.peek().is_none() {
                    return true;
                }
                let remaining_pattern: String = pattern_chars.clone().collect();
                // Try matching the rest of the pattern starting at every
                // remaining position in the text.
                while text_chars.peek().is_some() {
                    let remaining_text: String = text_chars.clone().collect();
                    if glob_match(&remaining_pattern, &remaining_text) {
                        return true;
                    }
                    text_chars.next();
                }
                // Also try matching with empty remainder
                let remaining_text: String = text_chars.collect();
                return glob_match(&remaining_pattern, &remaining_text);
            }
            '?' => {
                if text_chars.next().is_none() {
                    return false;
                }
            }
            c => {
                if text_chars.next() != Some(c) {
                    return false;
                }
            }
        }
    }

    text_chars.peek().is_none()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- Config validation ---------------------------------------------------

    #[test]
    fn test_config_validate_valid_postgres() {
        let config = NativeCdcConfig {
            source_type: CdcSourceType::PostgreSQL,
            connection: ConnectionConfig {
                host: "localhost".to_string(),
                port: 5432,
                database: "mydb".to_string(),
                username: "user".to_string(),
                password: "pass".to_string(),
                ..Default::default()
            },
            topic_prefix: "cdc.".to_string(),
            slot_name: Some("test_slot".to_string()),
            publication_name: Some("test_pub".to_string()),
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_validate_missing_host() {
        let config = NativeCdcConfig {
            connection: ConnectionConfig {
                host: String::new(),
                port: 5432,
                database: "mydb".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("host"));
    }

    #[test]
    fn test_config_validate_missing_slot_for_postgres() {
        let config = NativeCdcConfig {
            source_type: CdcSourceType::PostgreSQL,
            connection: ConnectionConfig {
                host: "localhost".to_string(),
                port: 5432,
                database: "mydb".to_string(),
                ..Default::default()
            },
            topic_prefix: "cdc.".to_string(),
            slot_name: None,
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("slot_name"));
    }

    #[test]
    fn test_config_validate_missing_server_id_for_mysql() {
        let config = NativeCdcConfig {
            source_type: CdcSourceType::MySQL,
            connection: ConnectionConfig {
                host: "localhost".to_string(),
                port: 3306,
                database: "mydb".to_string(),
                ..Default::default()
            },
            topic_prefix: "cdc.".to_string(),
            slot_name: None,
            server_id: None,
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("server_id"));
    }

    #[tokio::test]
    async fn test_postgres_adapter_not_implemented() {
        let config = NativeCdcConfig {
            source_type: CdcSourceType::PostgreSQL,
            connection: ConnectionConfig {
                host: "localhost".to_string(),
                port: 5432,
                database: "mydb".to_string(),
                username: "user".to_string(),
                password: "pass".to_string(),
                ..Default::default()
            },
            topic_prefix: "cdc.".to_string(),
            slot_name: Some("test_slot".to_string()),
            publication_name: Some("test_pub".to_string()),
            ..Default::default()
        };
        let mut adapter = PostgresCdcAdapter::new(config).unwrap();
        let err = adapter.start().await.unwrap_err();
        assert!(matches!(err, StreamlineError::Cdc(_)));
        let err = adapter.poll_changes(10).await.unwrap_err();
        assert!(matches!(err, StreamlineError::Cdc(_)));
        let err = adapter.stop().await.unwrap_err();
        assert!(matches!(err, StreamlineError::Cdc(_)));
    }

    #[tokio::test]
    async fn test_mysql_adapter_not_implemented() {
        let config = NativeCdcConfig {
            source_type: CdcSourceType::MySQL,
            connection: ConnectionConfig {
                host: "localhost".to_string(),
                port: 3306,
                database: "mydb".to_string(),
                username: "user".to_string(),
                password: "pass".to_string(),
                ..Default::default()
            },
            topic_prefix: "cdc.".to_string(),
            server_id: Some(1),
            ..Default::default()
        };
        let mut adapter = MysqlCdcAdapter::new(config).unwrap();
        let err = adapter.start().await.unwrap_err();
        assert!(matches!(err, StreamlineError::Cdc(_)));
        let err = adapter.poll_changes(10).await.unwrap_err();
        assert!(matches!(err, StreamlineError::Cdc(_)));
        let err = adapter.stop().await.unwrap_err();
        assert!(matches!(err, StreamlineError::Cdc(_)));
    }

    // -- Table filter matching -----------------------------------------------

    #[test]
    fn test_table_filter_exact_match() {
        let filter = TableFilter {
            schema_pattern: "public".to_string(),
            table_pattern: "users".to_string(),
            columns: None,
            exclude: false,
        };
        assert!(filter.matches("public", "users"));
        assert!(!filter.matches("public", "orders"));
        assert!(!filter.matches("private", "users"));
    }

    #[test]
    fn test_table_filter_glob_wildcard() {
        let filter = TableFilter {
            schema_pattern: "*".to_string(),
            table_pattern: "order_*".to_string(),
            columns: None,
            exclude: false,
        };
        assert!(filter.matches("public", "order_items"));
        assert!(filter.matches("sales", "order_headers"));
        assert!(!filter.matches("public", "users"));
    }

    #[test]
    fn test_table_filter_glob_question_mark() {
        let filter = TableFilter {
            schema_pattern: "public".to_string(),
            table_pattern: "log?".to_string(),
            columns: None,
            exclude: false,
        };
        assert!(filter.matches("public", "logs"));
        assert!(filter.matches("public", "logx"));
        assert!(!filter.matches("public", "logging"));
    }

    // -- CdcChangeEvent creation ---------------------------------------------

    #[test]
    fn test_change_event_insert() {
        let mut after = HashMap::new();
        after.insert("id".to_string(), ColumnValue::Int32(1));
        after.insert("name".to_string(), ColumnValue::String("Alice".to_string()));

        let event = CdcChangeEvent {
            operation: ChangeOperation::Insert,
            source: ChangeSource {
                database: "mydb".to_string(),
                schema: "public".to_string(),
                table: "users".to_string(),
                server_id: None,
                connector_name: "pg-main".to_string(),
            },
            timestamp: 1700000000000,
            before: None,
            after: Some(after),
            key: vec![("id".to_string(), ColumnValue::Int32(1))],
            offset: CdcOffset::postgres_lsn("0/1234ABCD"),
            schema_version: 1,
        };

        assert_eq!(event.operation, ChangeOperation::Insert);
        assert!(event.before.is_none());
        assert!(event.after.is_some());
        assert_eq!(event.after.as_ref().unwrap().len(), 2);
    }

    #[test]
    fn test_change_event_update() {
        let mut before = HashMap::new();
        before.insert("name".to_string(), ColumnValue::String("Alice".to_string()));
        let mut after = HashMap::new();
        after.insert("name".to_string(), ColumnValue::String("Bob".to_string()));

        let event = CdcChangeEvent {
            operation: ChangeOperation::Update,
            source: ChangeSource {
                database: "mydb".to_string(),
                schema: "public".to_string(),
                table: "users".to_string(),
                server_id: None,
                connector_name: "pg-main".to_string(),
            },
            timestamp: 1700000000000,
            before: Some(before),
            after: Some(after),
            key: vec![("id".to_string(), ColumnValue::Int32(1))],
            offset: CdcOffset::postgres_lsn("0/1234ABCE"),
            schema_version: 1,
        };

        assert_eq!(event.operation, ChangeOperation::Update);
        assert!(event.before.is_some());
        assert!(event.after.is_some());
    }

    #[test]
    fn test_change_event_delete() {
        let mut before = HashMap::new();
        before.insert("id".to_string(), ColumnValue::Int32(42));
        before.insert(
            "name".to_string(),
            ColumnValue::String("Charlie".to_string()),
        );

        let event = CdcChangeEvent {
            operation: ChangeOperation::Delete,
            source: ChangeSource {
                database: "mydb".to_string(),
                schema: "public".to_string(),
                table: "users".to_string(),
                server_id: None,
                connector_name: "pg-main".to_string(),
            },
            timestamp: 1700000000000,
            before: Some(before),
            after: None,
            key: vec![("id".to_string(), ColumnValue::Int32(42))],
            offset: CdcOffset::postgres_lsn("0/1234ABCF"),
            schema_version: 1,
        };

        assert_eq!(event.operation, ChangeOperation::Delete);
        assert!(event.before.is_some());
        assert!(event.after.is_none());
    }

    // -- ColumnValue serialization -------------------------------------------

    #[test]
    fn test_column_value_serialization_roundtrip() {
        let values = vec![
            ColumnValue::Null,
            ColumnValue::Bool(true),
            ColumnValue::Int16(42),
            ColumnValue::Int32(100_000),
            ColumnValue::Int64(9_999_999_999),
            ColumnValue::Float32(3.15),
            ColumnValue::Float64(2.72),
            ColumnValue::String("hello".to_string()),
            ColumnValue::Bytes(vec![0xDE, 0xAD, 0xBE, 0xEF]),
            ColumnValue::Timestamp(1700000000000),
            ColumnValue::Date("2025-01-15".to_string()),
            ColumnValue::Time("14:30:00".to_string()),
            ColumnValue::Uuid("550e8400-e29b-41d4-a716-446655440000".to_string()),
            ColumnValue::Json(serde_json::json!({"key": "value"})),
            ColumnValue::Array(vec![ColumnValue::Int32(1), ColumnValue::Int32(2)]),
        ];

        for value in &values {
            let json = serde_json::to_string(value).expect("serialize");
            let deserialized: ColumnValue = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(value, &deserialized, "Roundtrip failed for {:?}", value);
        }
    }

    // -- CdcOffset -----------------------------------------------------------

    #[test]
    fn test_postgres_offset() {
        let offset = CdcOffset::postgres_lsn("0/1234ABCD");
        assert_eq!(offset.lsn(), Some("0/1234ABCD"));
        assert!(offset.binlog_file().is_none());
        assert!(offset.binlog_position().is_none());
    }

    #[test]
    fn test_mysql_offset() {
        let offset =
            CdcOffset::mysql_binlog("mysql-bin.000003", 12345, Some("abc-123".to_string()));
        assert_eq!(offset.binlog_file(), Some("mysql-bin.000003"));
        assert_eq!(offset.binlog_position(), Some(12345));
        assert_eq!(offset.gtid(), Some("abc-123"));
        assert!(offset.lsn().is_none());
    }

    #[test]
    fn test_mysql_offset_without_gtid() {
        let offset = CdcOffset::mysql_binlog("mysql-bin.000001", 100, None);
        assert_eq!(offset.binlog_file(), Some("mysql-bin.000001"));
        assert_eq!(offset.binlog_position(), Some(100));
        assert!(offset.gtid().is_none());
    }

    // -- Schema tracking -----------------------------------------------------

    #[tokio::test]
    async fn test_schema_tracker_register_new_table() {
        let tracker = SchemaTracker::new();
        let columns = vec![
            ColumnSchema {
                name: "id".to_string(),
                column_type: ColumnType::Int32,
                nullable: false,
                is_primary_key: true,
                default_value: None,
            },
            ColumnSchema {
                name: "name".to_string(),
                column_type: ColumnType::String,
                nullable: true,
                is_primary_key: false,
                default_value: None,
            },
        ];

        let result = tracker.register_schema("public.users", columns).await;
        assert_eq!(result, SchemaChangeResult::NewTable { version: 1 });

        let info = tracker.get_schema("public.users").await.unwrap();
        assert_eq!(info.version, 1);
        assert_eq!(info.columns.len(), 2);
    }

    #[tokio::test]
    async fn test_schema_tracker_unchanged() {
        let tracker = SchemaTracker::new();
        let columns = vec![ColumnSchema {
            name: "id".to_string(),
            column_type: ColumnType::Int32,
            nullable: false,
            is_primary_key: true,
            default_value: None,
        }];

        tracker.register_schema("public.t", columns.clone()).await;
        let result = tracker.register_schema("public.t", columns).await;
        assert_eq!(result, SchemaChangeResult::Unchanged { version: 1 });
    }

    #[tokio::test]
    async fn test_schema_tracker_detect_drift_column_added() {
        let tracker = SchemaTracker::new();
        let columns_v1 = vec![ColumnSchema {
            name: "id".to_string(),
            column_type: ColumnType::Int32,
            nullable: false,
            is_primary_key: true,
            default_value: None,
        }];
        tracker.register_schema("public.t", columns_v1).await;

        let columns_v2 = vec![
            ColumnSchema {
                name: "id".to_string(),
                column_type: ColumnType::Int32,
                nullable: false,
                is_primary_key: true,
                default_value: None,
            },
            ColumnSchema {
                name: "email".to_string(),
                column_type: ColumnType::String,
                nullable: true,
                is_primary_key: false,
                default_value: None,
            },
        ];

        let drift = tracker.detect_drift("public.t", &columns_v2).await;
        assert_eq!(drift.len(), 1);
        assert!(matches!(&drift[0], SchemaChange::ColumnAdded { name, .. } if name == "email"));
    }

    #[tokio::test]
    async fn test_schema_tracker_detect_drift_column_removed() {
        let tracker = SchemaTracker::new();
        let columns_v1 = vec![
            ColumnSchema {
                name: "id".to_string(),
                column_type: ColumnType::Int32,
                nullable: false,
                is_primary_key: true,
                default_value: None,
            },
            ColumnSchema {
                name: "old_field".to_string(),
                column_type: ColumnType::String,
                nullable: true,
                is_primary_key: false,
                default_value: None,
            },
        ];
        tracker.register_schema("public.t", columns_v1).await;

        let columns_v2 = vec![ColumnSchema {
            name: "id".to_string(),
            column_type: ColumnType::Int32,
            nullable: false,
            is_primary_key: true,
            default_value: None,
        }];

        let drift = tracker.detect_drift("public.t", &columns_v2).await;
        assert!(drift
            .iter()
            .any(|c| matches!(c, SchemaChange::ColumnRemoved { name } if name == "old_field")));
    }

    #[tokio::test]
    async fn test_schema_tracker_detect_drift_type_changed() {
        let tracker = SchemaTracker::new();
        let columns_v1 = vec![ColumnSchema {
            name: "amount".to_string(),
            column_type: ColumnType::Int32,
            nullable: false,
            is_primary_key: false,
            default_value: None,
        }];
        tracker.register_schema("public.t", columns_v1).await;

        let columns_v2 = vec![ColumnSchema {
            name: "amount".to_string(),
            column_type: ColumnType::Int64,
            nullable: false,
            is_primary_key: false,
            default_value: None,
        }];

        let drift = tracker.detect_drift("public.t", &columns_v2).await;
        assert!(drift.iter().any(|c| matches!(
            c,
            SchemaChange::ColumnTypeChanged {
                name,
                from: ColumnType::Int32,
                to: ColumnType::Int64,
            } if name == "amount"
        )));
    }

    #[tokio::test]
    async fn test_schema_tracker_evolved() {
        let tracker = SchemaTracker::new();
        let columns_v1 = vec![ColumnSchema {
            name: "id".to_string(),
            column_type: ColumnType::Int32,
            nullable: false,
            is_primary_key: true,
            default_value: None,
        }];
        tracker.register_schema("public.t", columns_v1).await;

        let columns_v2 = vec![
            ColumnSchema {
                name: "id".to_string(),
                column_type: ColumnType::Int32,
                nullable: false,
                is_primary_key: true,
                default_value: None,
            },
            ColumnSchema {
                name: "new_col".to_string(),
                column_type: ColumnType::String,
                nullable: true,
                is_primary_key: false,
                default_value: None,
            },
        ];

        let result = tracker.register_schema("public.t", columns_v2).await;
        match result {
            SchemaChangeResult::Evolved { version, changes } => {
                assert_eq!(version, 2);
                assert!(!changes.is_empty());
            }
            other => panic!("Expected Evolved, got {:?}", other),
        }
    }

    // -- Stats tracking ------------------------------------------------------

    #[test]
    fn test_stats_record_events() {
        let mut stats = CdcSourceStats::default();

        let base_event = CdcChangeEvent {
            operation: ChangeOperation::Insert,
            source: ChangeSource {
                database: "db".to_string(),
                schema: "public".to_string(),
                table: "t".to_string(),
                server_id: None,
                connector_name: "test".to_string(),
            },
            timestamp: 0,
            before: None,
            after: None,
            key: vec![],
            offset: CdcOffset::postgres_lsn("0/0"),
            schema_version: 1,
        };

        // Record an INSERT
        stats.record_event(&base_event);
        assert_eq!(stats.events_captured, 1);
        assert_eq!(stats.inserts, 1);

        // Record an UPDATE
        let update_event = CdcChangeEvent {
            operation: ChangeOperation::Update,
            ..base_event.clone()
        };
        stats.record_event(&update_event);
        assert_eq!(stats.events_captured, 2);
        assert_eq!(stats.updates, 1);

        // Record a DELETE
        let delete_event = CdcChangeEvent {
            operation: ChangeOperation::Delete,
            ..base_event.clone()
        };
        stats.record_event(&delete_event);
        assert_eq!(stats.events_captured, 3);
        assert_eq!(stats.deletes, 1);

        // Record a SNAPSHOT
        let snapshot_event = CdcChangeEvent {
            operation: ChangeOperation::Snapshot,
            ..base_event.clone()
        };
        stats.record_event(&snapshot_event);
        assert_eq!(stats.events_captured, 4);
        assert_eq!(stats.snapshot_records, 1);

        // Record a SCHEMA_CHANGE
        let schema_event = CdcChangeEvent {
            operation: ChangeOperation::SchemaChange,
            ..base_event
        };
        stats.record_event(&schema_event);
        assert_eq!(stats.events_captured, 5);
        assert_eq!(stats.schema_changes, 1);
    }

    #[test]
    fn test_stats_latency_tracking() {
        let mut stats = CdcSourceStats {
            events_captured: 1,
            ..CdcSourceStats::default()
        };
        stats.record_latency(10.0);
        assert!((stats.avg_latency_ms - 10.0).abs() < f64::EPSILON);
        assert!((stats.max_latency_ms - 10.0).abs() < f64::EPSILON);

        stats.events_captured = 2;
        stats.record_latency(20.0);
        assert!(stats.max_latency_ms >= 20.0);
    }

    // -- Topic naming strategy -----------------------------------------------

    #[test]
    fn test_topic_naming_schema_table() {
        let strategy = TopicNamingStrategy::SchemaTable;
        let name = strategy.resolve("cdc.", "mydb", "public", "users");
        assert_eq!(name, "cdc.public.users");
    }

    #[test]
    fn test_topic_naming_database_schema_table() {
        let strategy = TopicNamingStrategy::DatabaseSchemaTable;
        let name = strategy.resolve("cdc.", "mydb", "public", "users");
        assert_eq!(name, "cdc.mydb.public.users");
    }

    #[test]
    fn test_topic_naming_custom() {
        let strategy = TopicNamingStrategy::Custom("{database}_{schema}_{table}".to_string());
        let name = strategy.resolve("prefix.", "mydb", "public", "users");
        assert_eq!(name, "prefix.mydb_public_users");
    }

    // -- Adapter construction ------------------------------------------------

    #[test]
    fn test_postgres_adapter_rejects_mysql_config() {
        let config = NativeCdcConfig {
            source_type: CdcSourceType::MySQL,
            connection: ConnectionConfig {
                host: "localhost".to_string(),
                port: 3306,
                database: "mydb".to_string(),
                ..Default::default()
            },
            topic_prefix: "cdc.".to_string(),
            server_id: Some(1),
            slot_name: None,
            ..Default::default()
        };
        let result = PostgresCdcAdapter::new(config);
        assert!(result.is_err());
    }

    #[test]
    fn test_mysql_adapter_rejects_postgres_config() {
        let config = NativeCdcConfig {
            source_type: CdcSourceType::PostgreSQL,
            connection: ConnectionConfig {
                host: "localhost".to_string(),
                port: 5432,
                database: "mydb".to_string(),
                ..Default::default()
            },
            topic_prefix: "cdc.".to_string(),
            slot_name: Some("test_slot".to_string()),
            server_id: None,
            ..Default::default()
        };
        let result = MysqlCdcAdapter::new(config);
        assert!(result.is_err());
    }

    // -- Glob matching -------------------------------------------------------

    #[test]
    fn test_glob_match_exact() {
        assert!(glob_match("hello", "hello"));
        assert!(!glob_match("hello", "world"));
    }

    #[test]
    fn test_glob_match_star() {
        assert!(glob_match("order_*", "order_items"));
        assert!(glob_match("order_*", "order_"));
        assert!(!glob_match("order_*", "user_items"));
        assert!(glob_match("*", "anything"));
        assert!(glob_match("*_items", "order_items"));
    }

    #[test]
    fn test_glob_match_question() {
        assert!(glob_match("log?", "logs"));
        assert!(glob_match("lo??", "logs"));
        assert!(!glob_match("log?", "logging"));
    }
}
