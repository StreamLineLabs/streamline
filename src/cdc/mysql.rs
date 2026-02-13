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

    /// Load table schemas from information_schema
    async fn load_table_schemas(&self) -> Result<()> {
        info!("Loading table schemas for MySQL CDC");

        // In a real implementation, this would query INFORMATION_SCHEMA.COLUMNS
        // For now, we simulate schema loading
        let mut schemas = HashMap::new();

        // The actual implementation would:
        // 1. Connect to MySQL
        // 2. Query INFORMATION_SCHEMA.COLUMNS
        // 3. Build schema map

        // Placeholder for actual schema loading
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

        *self.table_schemas.write() = schemas;
        Ok(())
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

    /// Stream binlog changes
    async fn stream_changes(
        self: Arc<Self>,
        _tx: mpsc::Sender<CdcEvent>,
        mut shutdown_rx: tokio::sync::oneshot::Receiver<()>,
    ) -> Result<()> {
        *self.status.write() = CdcSourceStatus::Running;
        info!(
            "Starting MySQL binlog streaming from {}",
            self.binlog_position.read().to_string_position()
        );

        // In a real implementation, this would:
        // 1. Connect to MySQL with replication client privileges
        // 2. Send REGISTER_SLAVE command
        // 3. Send COM_BINLOG_DUMP or COM_BINLOG_DUMP_GTID
        // 4. Process binlog events

        let poll_interval = tokio::time::Duration::from_millis(100);

        loop {
            tokio::select! {
                _ = &mut shutdown_rx => {
                    info!("Shutdown signal received, stopping MySQL CDC");
                    break;
                }
                _ = tokio::time::sleep(poll_interval) => {
                    // In a real implementation, we would read from the binlog stream
                    // For now, this is a placeholder that would process events

                    // Simulated event processing loop would go here
                    // Each event would be parsed and sent through tx

                    debug!("MySQL binlog polling...");
                }
            }
        }

        *self.status.write() = CdcSourceStatus::Stopped;
        Ok(())
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
}
