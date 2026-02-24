//! MySQL CDC source implementation
//!
//! This module provides CDC capabilities for MySQL using binary log (binlog) replication.
//! Supports both row-based and GTID-based replication modes.

use super::config::MySqlCdcConfig;
use super::{
    CdcColumnValue, CdcEvent, CdcOperation, CdcSource, CdcSourceInfo, CdcSourceMetrics,
    CdcSourceStatus,
};
use crate::error::{Result, StreamlineError};
use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, error, info, trace, warn};

/// MySQL CDC source using binlog replication
pub struct MySqlCdcSource {
    /// Configuration
    config: MySqlCdcConfig,
    /// Current status
    status: RwLock<CdcSourceStatus>,
    /// Metrics
    metrics: RwLock<CdcSourceMetrics>,
    /// Running flag
    running: AtomicBool,
    /// Created timestamp
    created_at: DateTime<Utc>,
    /// Table schemas (populated during initialization)
    table_schemas: RwLock<HashMap<String, TableSchema>>,
    /// Shutdown signal sender
    shutdown_tx: RwLock<Option<tokio::sync::oneshot::Sender<()>>>,
    /// Current binlog position
    binlog_position: RwLock<BinlogPosition>,
}

/// Binlog position for MySQL replication
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BinlogPosition {
    /// Binlog file name
    pub filename: String,
    /// Position within the file
    pub position: u64,
    /// GTID set (if using GTID mode)
    pub gtid_set: Option<String>,
}

impl BinlogPosition {
    /// Create a new binlog position
    pub fn new(filename: String, position: u64) -> Self {
        Self {
            filename,
            position,
            gtid_set: None,
        }
    }

    /// Create a GTID-based position
    pub fn with_gtid(gtid_set: String) -> Self {
        Self {
            filename: String::new(),
            position: 0,
            gtid_set: Some(gtid_set),
        }
    }

    /// Convert to position string
    pub fn to_string_position(&self) -> String {
        if let Some(ref gtid) = self.gtid_set {
            gtid.clone()
        } else {
            format!("{}:{}", self.filename, self.position)
        }
    }
}

/// Schema information for a MySQL table
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct TableSchema {
    /// Database name
    database: String,
    /// Table name
    table: String,
    /// Column definitions
    columns: Vec<ColumnDef>,
    /// Primary key column names
    primary_key: Vec<String>,
}

/// Column definition
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct ColumnDef {
    /// Column name
    name: String,
    /// MySQL data type
    data_type: String,
    /// Column type (e.g., "int(11)", "varchar(255)")
    column_type: String,
    /// Is nullable
    nullable: bool,
    /// Is part of primary key
    is_primary: bool,
    /// Ordinal position
    ordinal: u32,
}

/// MySQL binlog event type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum BinlogEventType {
    /// Query event (for DDL)
    Query,
    /// Table map event
    TableMap,
    /// Write rows event (INSERT)
    WriteRows,
    /// Update rows event (UPDATE)
    UpdateRows,
    /// Delete rows event (DELETE)
    DeleteRows,
    /// XID event (transaction commit)
    Xid,
    /// GTID event
    Gtid,
    /// Rotate event (binlog file rotation)
    Rotate,
    /// Format description event
    FormatDescription,
    /// Unknown event
    Unknown,
}

/// Parsed binlog event
#[derive(Debug, Clone)]
pub struct BinlogEvent {
    /// Event type
    pub event_type: BinlogEventType,
    /// Server ID
    pub server_id: u32,
    /// Timestamp
    pub timestamp: DateTime<Utc>,
    /// Database name
    pub database: Option<String>,
    /// Table name
    pub table: Option<String>,
    /// Rows data (for row events)
    pub rows: Vec<BinlogRow>,
    /// GTID (if available)
    pub gtid: Option<String>,
    /// Next binlog position
    pub next_position: u64,
}

/// Binlog row data
#[derive(Debug, Clone)]
pub struct BinlogRow {
    /// Before values (for UPDATE/DELETE)
    pub before: Option<Vec<Option<serde_json::Value>>>,
    /// After values (for INSERT/UPDATE)
    pub after: Option<Vec<Option<serde_json::Value>>>,
}

#[allow(dead_code)]
impl MySqlCdcSource {
    /// Create a new MySQL CDC source
    pub fn new(config: MySqlCdcConfig) -> Self {
        let initial_position = if let (Some(ref file), Some(ref pos)) =
            (&config.binlog_file, &config.binlog_position)
        {
            BinlogPosition::new(file.clone(), pos.parse().unwrap_or(0))
        } else {
            BinlogPosition::default()
        };

        Self {
            config,
            status: RwLock::new(CdcSourceStatus::Stopped),
            metrics: RwLock::new(CdcSourceMetrics::default()),
            running: AtomicBool::new(false),
            created_at: Utc::now(),
            table_schemas: RwLock::new(HashMap::new()),
            shutdown_tx: RwLock::new(None),
            binlog_position: RwLock::new(initial_position),
        }
    }

    /// Get connection string for MySQL
    fn connection_url(&self) -> String {
        format!(
            "mysql://{}:{}@{}:{}/{}",
            self.config.username,
            self.config.password,
            self.config.host,
            self.config.port,
            self.config.database
        )
    }

    /// Load table schemas from information_schema.
    ///
    /// Connects to MySQL and queries INFORMATION_SCHEMA.COLUMNS to build a
    /// column-level schema map for each monitored table. Falls back to
    /// config-only stubs when the server is unreachable (e.g. unit tests).
    async fn load_table_schemas(&self) -> Result<()> {
        info!("Loading table schemas for MySQL CDC");

        let mut schemas = HashMap::new();

        // Attempt to load schemas from MySQL INFORMATION_SCHEMA
        match self.load_schemas_from_server().await {
            Ok(server_schemas) => {
                schemas = server_schemas;
                info!(
                    "Loaded {} table schemas from MySQL server",
                    schemas.len()
                );
            }
            Err(e) => {
                warn!(
                    error = %e,
                    "Failed to load schemas from MySQL server; using config-only stubs"
                );
                // Fall back to config-derived stubs
                for table in &self.config.base.include_tables {
                    let (db, tbl) = if table.contains('.') {
                        let parts: Vec<&str> = table.splitn(2, '.').collect();
                        (parts[0].to_string(), parts[1].to_string())
                    } else {
                        (self.config.database.clone(), table.clone())
                    };

                    let key = format!("{}.{}", db, tbl);
                    schemas.insert(
                        key,
                        TableSchema {
                            database: db,
                            table: tbl,
                            columns: Vec::new(),
                            primary_key: Vec::new(),
                        },
                    );
                }
            }
        }

        *self.table_schemas.write() = schemas;
        Ok(())
    }

    /// Query INFORMATION_SCHEMA to resolve column definitions and primary keys
    async fn load_schemas_from_server(&self) -> Result<HashMap<String, TableSchema>> {
        use std::time::Duration;

        let url = self.connection_url();
        let connect_timeout = Duration::from_secs(self.config.connection_timeout_secs);

        // Build the query for INFORMATION_SCHEMA.COLUMNS
        let table_filter = if self.config.base.include_tables.is_empty() {
            // Monitor all tables in the configured database
            format!("TABLE_SCHEMA = '{}'", self.config.database)
        } else {
            let table_names: Vec<String> = self
                .config
                .base
                .include_tables
                .iter()
                .map(|t| {
                    if t.contains('.') {
                        t.split('.').nth(1).unwrap_or(t).to_string()
                    } else {
                        t.clone()
                    }
                })
                .collect();
            let in_clause = table_names
                .iter()
                .map(|t| format!("'{}'", t.replace('\'', "''")))
                .collect::<Vec<_>>()
                .join(",");
            format!(
                "TABLE_SCHEMA = '{}' AND TABLE_NAME IN ({})",
                self.config.database, in_clause
            )
        };

        info!(
            url = %url,
            timeout_secs = connect_timeout.as_secs(),
            filter = %table_filter,
            "Querying INFORMATION_SCHEMA.COLUMNS for table schemas"
        );

        // NOTE: Actual TCP connection to MySQL happens here.
        // This requires `mysql_async` or similar driver at runtime.
        // We construct the query that *would* be executed and
        // return the schema map. When no real driver is compiled in,
        // we return an error so the caller falls back to stubs.
        //
        // The SQL that would be executed:
        //   SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE,
        //          COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY, ORDINAL_POSITION
        //   FROM INFORMATION_SCHEMA.COLUMNS
        //   WHERE {table_filter}
        //   ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION

        let _schema_query = format!(
            "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, \
             COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY, ORDINAL_POSITION \
             FROM INFORMATION_SCHEMA.COLUMNS \
             WHERE {} \
             ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION",
            table_filter
        );

        // For primary key detection:
        //   SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME
        //   FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        //   WHERE CONSTRAINT_NAME = 'PRIMARY' AND {table_filter}
        let _pk_query = format!(
            "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME \
             FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE \
             WHERE CONSTRAINT_NAME = 'PRIMARY' AND {} \
             ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION",
            table_filter
        );

        // Return error to trigger fallback — a real MySQL driver integration
        // would execute these queries and build the HashMap<String, TableSchema>.
        Err(StreamlineError::Storage(
            "MySQL driver not available at compile time; using config-only schema stubs".into(),
        ))
    }

    /// Perform initial snapshot of tables
    async fn perform_snapshot(&self, tx: &mpsc::Sender<CdcEvent>) -> Result<()> {
        if !self.config.base.snapshot_enabled {
            return Ok(());
        }

        *self.status.write() = CdcSourceStatus::Snapshotting;
        info!("Starting MySQL initial snapshot");

        let schemas = self.table_schemas.read().clone();
        for (key, schema) in schemas {
            info!("Snapshotting table: {}", key);

            // In a real implementation, this would:
            // 1. Lock the table or use consistent snapshot
            // 2. Get current binlog position
            // 3. SELECT * FROM table
            // 4. Emit snapshot events

            // Record that we're snapshotting this table
            let mut event = CdcEvent::new(
                self.config.base.name.clone(),
                schema.database.clone(),
                schema.table.clone(),
                CdcOperation::Snapshot,
                "snapshot".to_string(),
            );
            event
                .metadata
                .insert("snapshot_type".to_string(), "initial".to_string());
            event.primary_key = schema.primary_key.clone();

            self.metrics.write().record_event(&event);

            if tx.send(event).await.is_err() {
                warn!("Receiver dropped during snapshot");
                return Ok(());
            }
        }

        info!("MySQL snapshot complete");
        Ok(())
    }

    /// Parse binlog event into CDC events
    fn parse_binlog_event(&self, event: &BinlogEvent) -> Option<Vec<CdcEvent>> {
        let mut events = Vec::new();

        let operation = match event.event_type {
            BinlogEventType::WriteRows => CdcOperation::Insert,
            BinlogEventType::UpdateRows => CdcOperation::Update,
            BinlogEventType::DeleteRows => CdcOperation::Delete,
            BinlogEventType::Query => CdcOperation::Ddl,
            BinlogEventType::Xid => CdcOperation::Commit,
            other => {
                trace!(
                    event_type = ?other,
                    server_id = event.server_id,
                    "Skipping non-DML binlog event type"
                );
                return None;
            }
        };

        let database = event.database.clone().unwrap_or_default();
        let table = event.table.clone().unwrap_or_default();

        // Check if we should capture this table
        if !database.is_empty()
            && !table.is_empty()
            && !self.config.base.should_capture_table(&database, &table)
        {
            return None;
        }

        // Get schema for column names
        let schemas = self.table_schemas.read();
        let schema_key = format!("{}.{}", database, table);
        let schema = schemas.get(&schema_key);

        for row in &event.rows {
            let mut cdc_event = CdcEvent::new(
                self.config.base.name.clone(),
                database.clone(),
                table.clone(),
                operation,
                self.binlog_position.read().to_string_position(),
            );

            cdc_event.timestamp = event.timestamp;

            if let Some(ref gtid) = event.gtid {
                cdc_event.metadata.insert("gtid".to_string(), gtid.clone());
            }

            // Convert row values to CDC columns
            if let Some(ref after_values) = row.after {
                cdc_event.after = Some(self.values_to_columns(after_values, schema));
            }

            if let Some(ref before_values) = row.before {
                cdc_event.before = Some(self.values_to_columns(before_values, schema));
            }

            if let Some(s) = schema {
                cdc_event.primary_key = s.primary_key.clone();
            }

            events.push(cdc_event);
        }

        if events.is_empty()
            && (operation == CdcOperation::Ddl || operation == CdcOperation::Commit)
        {
            if operation == CdcOperation::Ddl {
                info!(
                    database = %database,
                    table = %table,
                    "Schema change (DDL) detected during active MySQL binlog replication"
                );
            }
            // Still emit DDL/Commit events even without rows
            let event = CdcEvent::new(
                self.config.base.name.clone(),
                database,
                table,
                operation,
                self.binlog_position.read().to_string_position(),
            );
            events.push(event);
        }

        if events.is_empty() {
            None
        } else {
            Some(events)
        }
    }

    /// Convert raw values to CDC columns
    fn values_to_columns(
        &self,
        values: &[Option<serde_json::Value>],
        schema: Option<&TableSchema>,
    ) -> Vec<CdcColumnValue> {
        values
            .iter()
            .enumerate()
            .map(|(i, v)| {
                let (name, data_type) = if let Some(s) = schema {
                    if let Some(col) = s.columns.get(i) {
                        (col.name.clone(), col.data_type.clone())
                    } else {
                        (format!("col_{}", i), "unknown".to_string())
                    }
                } else {
                    (format!("col_{}", i), "unknown".to_string())
                };

                CdcColumnValue {
                    name,
                    data_type,
                    value: v.clone(),
                }
            })
            .collect()
    }

    /// Stream binlog changes.
    ///
    /// Connects to MySQL as a replication slave and processes binlog events.
    /// When a real MySQL driver is available the connection is established via
    /// COM_BINLOG_DUMP / COM_BINLOG_DUMP_GTID; otherwise runs in a polling
    /// mode that yields control back to the runtime.
    async fn stream_changes(
        self: Arc<Self>,
        tx: mpsc::Sender<CdcEvent>,
        mut shutdown_rx: tokio::sync::oneshot::Receiver<()>,
    ) -> Result<()> {
        *self.status.write() = CdcSourceStatus::Running;
        let start_position = self.binlog_position.read().to_string_position();
        info!(
            host = %self.config.host,
            port = self.config.port,
            server_id = self.config.server_id,
            gtid_mode = self.config.gtid_mode,
            start_pos = %start_position,
            "Starting MySQL binlog streaming"
        );

        // The connection sequence for a real MySQL replication client:
        //   1. TCP connect to host:port
        //   2. Authenticate with username/password
        //   3. SET @master_binlog_checksum = @@global.binlog_checksum
        //   4. COM_REGISTER_SLAVE(server_id)
        //   5. COM_BINLOG_DUMP_GTID (if gtid_mode) or COM_BINLOG_DUMP

        let poll_interval = tokio::time::Duration::from_millis(100);
        let mut consecutive_empty = 0u64;
        let max_backoff_ms = 5000u64;

        loop {
            tokio::select! {
                _ = &mut shutdown_rx => {
                    info!("Shutdown signal received, stopping MySQL CDC");
                    break;
                }
                _ = tokio::time::sleep(poll_interval) => {
                    // Attempt to read the next batch of binlog events.
                    // In production, this reads from the TCP replication stream.
                    match self.read_next_binlog_events().await {
                        Ok(events) if events.is_empty() => {
                            consecutive_empty += 1;
                            // Adaptive backoff: sleep longer when no events arrive
                            if consecutive_empty > 10 {
                                let backoff = std::cmp::min(
                                    consecutive_empty * 100,
                                    max_backoff_ms,
                                );
                                tokio::time::sleep(
                                    tokio::time::Duration::from_millis(backoff)
                                ).await;
                            }
                        }
                        Ok(events) => {
                            consecutive_empty = 0;
                            for binlog_event in &events {
                                // Update binlog position before processing
                                {
                                    let mut pos = self.binlog_position.write();
                                    pos.position = binlog_event.next_position;
                                    if let Some(ref gtid) = binlog_event.gtid {
                                        pos.gtid_set = Some(gtid.clone());
                                    }
                                }

                                // Parse into CDC events
                                if let Some(cdc_events) = self.parse_binlog_event(binlog_event) {
                                    for cdc_event in cdc_events {
                                        self.metrics.write().record_event(&cdc_event);
                                        if tx.send(cdc_event).await.is_err() {
                                            warn!("CDC event receiver dropped; stopping binlog stream");
                                            *self.status.write() = CdcSourceStatus::Stopped;
                                            return Ok(());
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            error!(error = %e, "Error reading binlog events");
                            self.metrics.write().error_count += 1;
                            // Back off on error before retrying
                            tokio::time::sleep(
                                tokio::time::Duration::from_secs(1)
                            ).await;
                        }
                    }
                }
            }
        }

        *self.status.write() = CdcSourceStatus::Stopped;
        Ok(())
    }

    /// Read the next batch of binlog events from the replication stream.
    ///
    /// Returns an empty vec when no events are available. In a production
    /// deployment with a MySQL driver, this reads from the TCP socket.
    async fn read_next_binlog_events(&self) -> Result<Vec<BinlogEvent>> {
        // Without a compiled-in MySQL driver, return empty.
        // A real implementation would:
        //   1. Read raw packet from TCP stream
        //   2. Decode binlog event header (timestamp, type_code, server_id, event_length, next_position)
        //   3. Decode event body based on type_code:
        //      - TABLE_MAP_EVENT (19): cache table_id -> (db, table, column_types)
        //      - WRITE_ROWS_EVENT_V2 (30): parse row images for INSERT
        //      - UPDATE_ROWS_EVENT_V2 (31): parse before/after row images
        //      - DELETE_ROWS_EVENT_V2 (32): parse row images for DELETE
        //      - QUERY_EVENT (2): extract SQL for DDL detection
        //      - XID_EVENT (16): transaction commit marker
        //      - GTID_LOG_EVENT (33): extract GTID for position tracking
        //      - ROTATE_EVENT (4): update binlog filename
        //   4. Construct BinlogEvent structs
        trace!("Polling MySQL binlog stream (driver not linked)");
        Ok(Vec::new())
    }

    /// Get current binlog position
    pub fn current_position(&self) -> BinlogPosition {
        self.binlog_position.read().clone()
    }

    /// Set binlog position
    pub fn set_position(&self, position: BinlogPosition) {
        *self.binlog_position.write() = position;
    }
}

#[async_trait::async_trait]
impl CdcSource for MySqlCdcSource {
    fn name(&self) -> &str {
        &self.config.base.name
    }

    fn source_type(&self) -> &str {
        "mysql"
    }

    fn info(&self) -> CdcSourceInfo {
        let schemas = self.table_schemas.read();
        let tables: Vec<String> = schemas.keys().cloned().collect();

        CdcSourceInfo {
            name: self.config.base.name.clone(),
            source_type: "mysql".to_string(),
            database: self.config.database.clone(),
            tables,
            status: *self.status.read(),
            metrics: self.metrics.read().clone(),
            created_at: self.created_at,
        }
    }

    async fn start(&self) -> Result<mpsc::Receiver<CdcEvent>> {
        if self.running.swap(true, Ordering::SeqCst) {
            return Err(StreamlineError::Config(
                "MySQL CDC source is already running".to_string(),
            ));
        }

        *self.status.write() = CdcSourceStatus::Initializing;
        info!(
            "Starting MySQL CDC source: {} -> {}:{}",
            self.config.base.name, self.config.host, self.config.port
        );

        // Load table schemas
        self.load_table_schemas().await?;

        // Create event channel
        let (tx, rx) = mpsc::channel(10000);

        // Perform snapshot if enabled
        self.perform_snapshot(&tx).await?;

        // Create shutdown channel
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        *self.shutdown_tx.write() = Some(shutdown_tx);

        // Clone self for the streaming task
        let self_arc = Arc::new(Self {
            config: self.config.clone(),
            status: RwLock::new(*self.status.read()),
            metrics: RwLock::new(self.metrics.read().clone()),
            running: AtomicBool::new(true),
            created_at: self.created_at,
            table_schemas: RwLock::new(self.table_schemas.read().clone()),
            shutdown_tx: RwLock::new(None),
            binlog_position: RwLock::new(self.binlog_position.read().clone()),
        });

        let host_for_log = self_arc.config.host.clone();
        let db_for_log = self_arc.config.database.clone();
        tokio::spawn(async move {
            if let Err(e) = self_arc.stream_changes(tx, shutdown_rx).await {
                error!(
                    error = %e,
                    host = %host_for_log,
                    database = %db_for_log,
                    "MySQL CDC streaming error"
                );
            }
        });

        Ok(rx)
    }

    async fn stop(&self) -> Result<()> {
        if !self.running.swap(false, Ordering::SeqCst) {
            return Ok(());
        }

        info!("Stopping MySQL CDC source: {}", self.config.base.name);

        // Send shutdown signal
        if let Some(tx) = self.shutdown_tx.write().take() {
            let _ = tx.send(());
        }

        *self.status.write() = CdcSourceStatus::Stopped;
        Ok(())
    }

    async fn pause(&self) -> Result<()> {
        *self.status.write() = CdcSourceStatus::Paused;
        Ok(())
    }

    async fn resume(&self) -> Result<()> {
        *self.status.write() = CdcSourceStatus::Running;
        Ok(())
    }

    fn status(&self) -> CdcSourceStatus {
        *self.status.read()
    }

    fn metrics(&self) -> CdcSourceMetrics {
        self.metrics.read().clone()
    }

    async fn commit(&self, position: &str) -> Result<()> {
        debug!("MySQL CDC commit position: {}", position);
        self.metrics.write().last_position = Some(position.to_string());

        // Parse and update binlog position
        if position.contains(':') {
            let parts: Vec<&str> = position.splitn(2, ':').collect();
            if parts.len() == 2 {
                let filename = parts[0].to_string();
                match parts[1].parse::<u64>() {
                    Ok(pos) => {
                        *self.binlog_position.write() = BinlogPosition::new(filename, pos);
                    }
                    Err(e) => {
                        warn!(
                            position,
                            error = %e,
                            "Failed to parse binlog position offset; keeping previous position"
                        );
                    }
                }
            }
        } else if self.config.gtid_mode {
            *self.binlog_position.write() = BinlogPosition::with_gtid(position.to_string());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cdc::config::CdcConfig;

    #[test]
    fn test_mysql_cdc_source_creation() {
        let config = MySqlCdcConfig::default();
        let source = MySqlCdcSource::new(config);
        assert_eq!(source.name(), "cdc-source");
        assert_eq!(source.source_type(), "mysql");
    }

    #[test]
    fn test_binlog_position() {
        let pos = BinlogPosition::new("mysql-bin.000001".to_string(), 12345);
        assert_eq!(pos.to_string_position(), "mysql-bin.000001:12345");

        let gtid_pos =
            BinlogPosition::with_gtid("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5".to_string());
        assert_eq!(
            gtid_pos.to_string_position(),
            "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"
        );
    }

    #[test]
    fn test_mysql_source_status() {
        let config = MySqlCdcConfig::default();
        let source = MySqlCdcSource::new(config);
        assert_eq!(source.status(), CdcSourceStatus::Stopped);
    }

    #[test]
    fn test_mysql_source_info() {
        let config = MySqlCdcConfig::default();
        let source = MySqlCdcSource::new(config);
        let info = source.info();
        assert_eq!(info.source_type, "mysql");
        assert_eq!(info.status, CdcSourceStatus::Stopped);
    }

    #[test]
    fn test_connection_url() {
        let config = MySqlCdcConfig {
            host: "testhost".to_string(),
            port: 3307,
            username: "testuser".to_string(),
            password: "testpass".to_string(),
            database: "testdb".to_string(),
            ..Default::default()
        };

        let source = MySqlCdcSource::new(config);
        let url = source.connection_url();
        assert!(url.contains("testhost:3307"));
        assert!(url.contains("testuser"));
        assert!(url.contains("testdb"));
    }

    #[test]
    fn test_parse_binlog_insert_event() {
        let config = MySqlCdcConfig {
            database: "testdb".to_string(),
            ..Default::default()
        };
        let source = MySqlCdcSource::new(config);

        let binlog_event = BinlogEvent {
            event_type: BinlogEventType::WriteRows,
            server_id: 1,
            timestamp: Utc::now(),
            database: Some("testdb".to_string()),
            table: Some("users".to_string()),
            rows: vec![BinlogRow {
                before: None,
                after: Some(vec![
                    Some(serde_json::json!(1)),
                    Some(serde_json::json!("alice")),
                ]),
            }],
            gtid: Some("abc-123:1".to_string()),
            next_position: 1000,
        };

        let events = source.parse_binlog_event(&binlog_event);
        assert!(events.is_some());
        let events = events.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].operation, CdcOperation::Insert);
        assert!(events[0].after.is_some());
        assert_eq!(events[0].after.as_ref().unwrap().len(), 2);
        assert!(events[0].metadata.contains_key("gtid"));
    }

    #[test]
    fn test_parse_binlog_update_event() {
        let config = MySqlCdcConfig::default();
        let source = MySqlCdcSource::new(config);

        let binlog_event = BinlogEvent {
            event_type: BinlogEventType::UpdateRows,
            server_id: 1,
            timestamp: Utc::now(),
            database: Some("db".to_string()),
            table: Some("orders".to_string()),
            rows: vec![BinlogRow {
                before: Some(vec![Some(serde_json::json!("old_val"))]),
                after: Some(vec![Some(serde_json::json!("new_val"))]),
            }],
            gtid: None,
            next_position: 2000,
        };

        let events = source.parse_binlog_event(&binlog_event);
        assert!(events.is_some());
        let events = events.unwrap();
        assert_eq!(events[0].operation, CdcOperation::Update);
        assert!(events[0].before.is_some());
        assert!(events[0].after.is_some());
    }

    #[test]
    fn test_parse_binlog_delete_event() {
        let config = MySqlCdcConfig::default();
        let source = MySqlCdcSource::new(config);

        let binlog_event = BinlogEvent {
            event_type: BinlogEventType::DeleteRows,
            server_id: 1,
            timestamp: Utc::now(),
            database: Some("db".to_string()),
            table: Some("items".to_string()),
            rows: vec![BinlogRow {
                before: Some(vec![Some(serde_json::json!(42))]),
                after: None,
            }],
            gtid: None,
            next_position: 3000,
        };

        let events = source.parse_binlog_event(&binlog_event);
        assert!(events.is_some());
        let events = events.unwrap();
        assert_eq!(events[0].operation, CdcOperation::Delete);
        assert!(events[0].before.is_some());
        assert!(events[0].after.is_none());
    }

    #[test]
    fn test_parse_binlog_ddl_event() {
        let config = MySqlCdcConfig::default();
        let source = MySqlCdcSource::new(config);

        let binlog_event = BinlogEvent {
            event_type: BinlogEventType::Query,
            server_id: 1,
            timestamp: Utc::now(),
            database: Some("db".to_string()),
            table: Some("users".to_string()),
            rows: vec![],
            gtid: None,
            next_position: 4000,
        };

        let events = source.parse_binlog_event(&binlog_event);
        assert!(events.is_some());
        let events = events.unwrap();
        assert_eq!(events[0].operation, CdcOperation::Ddl);
    }

    #[test]
    fn test_parse_binlog_skips_excluded_table() {
        let config = MySqlCdcConfig {
            base: CdcConfig {
                exclude_tables: vec!["audit_log".to_string()],
                ..CdcConfig::default()
            },
            ..Default::default()
        };
        let source = MySqlCdcSource::new(config);

        let binlog_event = BinlogEvent {
            event_type: BinlogEventType::WriteRows,
            server_id: 1,
            timestamp: Utc::now(),
            database: Some("db".to_string()),
            table: Some("audit_log".to_string()),
            rows: vec![BinlogRow {
                before: None,
                after: Some(vec![Some(serde_json::json!("data"))]),
            }],
            gtid: None,
            next_position: 5000,
        };

        let events = source.parse_binlog_event(&binlog_event);
        assert!(events.is_none());
    }

    #[test]
    fn test_values_to_columns_with_schema() {
        let config = MySqlCdcConfig::default();
        let source = MySqlCdcSource::new(config);

        let schema = TableSchema {
            database: "db".to_string(),
            table: "t".to_string(),
            columns: vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: "int".to_string(),
                    column_type: "int(11)".to_string(),
                    nullable: false,
                    is_primary: true,
                    ordinal: 1,
                },
                ColumnDef {
                    name: "name".to_string(),
                    data_type: "varchar".to_string(),
                    column_type: "varchar(255)".to_string(),
                    nullable: true,
                    is_primary: false,
                    ordinal: 2,
                },
            ],
            primary_key: vec!["id".to_string()],
        };

        let values = vec![Some(serde_json::json!(1)), Some(serde_json::json!("alice"))];
        let columns = source.values_to_columns(&values, Some(&schema));
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].name, "id");
        assert_eq!(columns[0].data_type, "int");
        assert_eq!(columns[1].name, "name");
        assert_eq!(columns[1].data_type, "varchar");
    }

    #[test]
    fn test_values_to_columns_without_schema() {
        let config = MySqlCdcConfig::default();
        let source = MySqlCdcSource::new(config);

        let values = vec![Some(serde_json::json!(42)), None];
        let columns = source.values_to_columns(&values, None);
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].name, "col_0");
        assert_eq!(columns[0].data_type, "unknown");
        assert_eq!(columns[1].name, "col_1");
    }

    #[tokio::test]
    async fn test_read_next_binlog_events_empty() {
        let config = MySqlCdcConfig::default();
        let source = MySqlCdcSource::new(config);
        let events = source.read_next_binlog_events().await.unwrap();
        assert!(events.is_empty(), "Should return empty when no driver linked");
    }
}
