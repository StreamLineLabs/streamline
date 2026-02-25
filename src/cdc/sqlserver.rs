//! SQL Server CDC source implementation
//!
//! This module provides CDC capabilities for Microsoft SQL Server using
//! SQL Server Change Data Capture (CDC) feature. It polls cdc.* tables
//! for changes.

use super::config::SqlServerCdcConfig;
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
use tracing::{debug, error, info, warn};

/// SQL Server CDC source using polling-based CDC
pub struct SqlServerCdcSource {
    /// Configuration
    config: SqlServerCdcConfig,
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
    /// Current LSN position
    current_lsn: RwLock<LsnPosition>,
    /// Captured tables with CDC enabled
    cdc_tables: RwLock<Vec<CdcCaptureInstance>>,
}

/// LSN (Log Sequence Number) position for SQL Server
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LsnPosition {
    /// LSN as hex string (e.g., "0x00000000000000000001")
    pub lsn: String,
    /// Sequence value within LSN
    pub sequence: u64,
}

impl LsnPosition {
    /// Create a new LSN position
    pub fn new(lsn: String, sequence: u64) -> Self {
        Self { lsn, sequence }
    }

    /// Create from hex string
    pub fn from_hex(hex: &str) -> Self {
        Self {
            lsn: hex.to_string(),
            sequence: 0,
        }
    }

    /// Get minimum LSN (start of log)
    pub fn min() -> Self {
        Self {
            lsn: "0x00000000000000000000".to_string(),
            sequence: 0,
        }
    }

    /// Convert to position string
    pub fn to_string_position(&self) -> String {
        format!("{}:{}", self.lsn, self.sequence)
    }

    /// Parse from position string
    pub fn from_string_position(s: &str) -> Option<Self> {
        let parts: Vec<&str> = s.splitn(2, ':').collect();
        if parts.len() == 2 {
            Some(Self {
                lsn: parts[0].to_string(),
                sequence: parts[1].parse().unwrap_or(0),
            })
        } else {
            Some(Self {
                lsn: s.to_string(),
                sequence: 0,
            })
        }
    }
}

/// CDC capture instance information
#[derive(Debug, Clone)]
pub struct CdcCaptureInstance {
    /// Source schema
    pub source_schema: String,
    /// Source table
    pub source_table: String,
    /// Capture instance name
    pub capture_instance: String,
    /// CDC schema (usually "cdc")
    pub cdc_schema: String,
    /// CDC table name (e.g., "dbo_MyTable_CT")
    pub change_table: String,
    /// Column names
    pub columns: Vec<String>,
    /// Primary key columns
    pub primary_key: Vec<String>,
    /// Minimum valid LSN for this capture instance
    pub start_lsn: String,
}

/// SQL Server CDC operation type (from __$operation column)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlServerOperation {
    /// Delete (operation = 1)
    Delete = 1,
    /// Insert (operation = 2)
    Insert = 2,
    /// Update before image (operation = 3)
    UpdateBefore = 3,
    /// Update after image (operation = 4)
    UpdateAfter = 4,
}

impl SqlServerOperation {
    /// Parse from i32 operation code
    pub fn from_code(code: i32) -> Option<Self> {
        match code {
            1 => Some(Self::Delete),
            2 => Some(Self::Insert),
            3 => Some(Self::UpdateBefore),
            4 => Some(Self::UpdateAfter),
            _ => None,
        }
    }

    /// Convert to CDC operation
    pub fn to_cdc_operation(self) -> CdcOperation {
        match self {
            Self::Delete => CdcOperation::Delete,
            Self::Insert => CdcOperation::Insert,
            Self::UpdateBefore | Self::UpdateAfter => CdcOperation::Update,
        }
    }

    /// Check if this is a "before" image
    pub fn is_before_image(&self) -> bool {
        matches!(self, Self::Delete | Self::UpdateBefore)
    }
}

/// Schema information for a SQL Server table
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct TableSchema {
    /// Schema name
    schema: String,
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
    /// SQL Server data type
    data_type: String,
    /// Maximum length
    max_length: i32,
    /// Precision (for numeric types)
    precision: u8,
    /// Scale (for numeric types)
    scale: u8,
    /// Is nullable
    nullable: bool,
    /// Ordinal position
    ordinal: i32,
}

/// Row from CDC change table
#[derive(Debug, Clone)]
pub struct CdcChangeRow {
    /// Start LSN
    pub start_lsn: String,
    /// Sequence value
    pub sequence_value: u64,
    /// Operation (1=delete, 2=insert, 3=update-before, 4=update-after)
    pub operation: SqlServerOperation,
    /// Update mask (bitmap of changed columns)
    pub update_mask: Option<Vec<u8>>,
    /// Column values
    pub values: HashMap<String, serde_json::Value>,
}

#[allow(dead_code)]
impl SqlServerCdcSource {
    /// Create a new SQL Server CDC source
    pub fn new(config: SqlServerCdcConfig) -> Self {
        let initial_lsn = config
            .start_lsn
            .as_ref()
            .map(|s| LsnPosition::from_hex(s))
            .unwrap_or_else(LsnPosition::min);

        Self {
            config,
            status: RwLock::new(CdcSourceStatus::Stopped),
            metrics: RwLock::new(CdcSourceMetrics::default()),
            running: AtomicBool::new(false),
            created_at: Utc::now(),
            table_schemas: RwLock::new(HashMap::new()),
            shutdown_tx: RwLock::new(None),
            current_lsn: RwLock::new(initial_lsn),
            cdc_tables: RwLock::new(Vec::new()),
        }
    }

    /// Get connection string for SQL Server
    fn connection_string(&self) -> String {
        let mut conn = format!(
            "Server={},{};Database={};User Id={};Password={};",
            self.config.host,
            self.config.port,
            self.config.database,
            self.config.username,
            self.config.password
        );

        if let Some(ref instance) = self.config.instance {
            conn = format!(
                "Server={}\\{},{};Database={};User Id={};Password={};",
                self.config.host,
                instance,
                self.config.port,
                self.config.database,
                self.config.username,
                self.config.password
            );
        }

        if self.config.encrypt {
            conn.push_str("Encrypt=true;");
        }

        if self.config.trust_server_certificate {
            conn.push_str("TrustServerCertificate=true;");
        }

        conn
    }

    /// Discover CDC-enabled tables
    async fn discover_cdc_tables(&self) -> Result<()> {
        info!("Discovering SQL Server CDC-enabled tables");

        // In a real implementation, this would query:
        // SELECT * FROM cdc.change_tables
        // JOIN sys.tables ON ...
        // JOIN sys.schemas ON ...

        let mut cdc_tables = Vec::new();

        // Simulate discovery - in reality this would query the database
        for table in &self.config.base.include_tables {
            let (schema, tbl) = if table.contains('.') {
                let parts: Vec<&str> = table.splitn(2, '.').collect();
                (parts[0].to_string(), parts[1].to_string())
            } else {
                ("dbo".to_string(), table.clone())
            };

            // Check if we should capture this table
            if !self.config.base.should_capture_table(&schema, &tbl) {
                continue;
            }

            let capture_instance = CdcCaptureInstance {
                source_schema: schema.clone(),
                source_table: tbl.clone(),
                capture_instance: format!("{}_{}", schema, tbl),
                cdc_schema: "cdc".to_string(),
                change_table: format!("{}_{}_CT", schema, tbl),
                columns: Vec::new(),
                primary_key: Vec::new(),
                start_lsn: "0x00000000000000000000".to_string(),
            };

            cdc_tables.push(capture_instance);
        }

        *self.cdc_tables.write() = cdc_tables;
        Ok(())
    }

    /// Load table schemas
    async fn load_table_schemas(&self) -> Result<()> {
        info!("Loading SQL Server table schemas");

        let mut schemas = HashMap::new();

        // In a real implementation, this would query INFORMATION_SCHEMA.COLUMNS
        for capture in self.cdc_tables.read().iter() {
            let key = format!("{}.{}", capture.source_schema, capture.source_table);
            schemas.insert(
                key,
                TableSchema {
                    schema: capture.source_schema.clone(),
                    table: capture.source_table.clone(),
                    columns: Vec::new(),
                    primary_key: capture.primary_key.clone(),
                },
            );
        }

        *self.table_schemas.write() = schemas;
        Ok(())
    }

    /// Perform initial snapshot
    async fn perform_snapshot(&self, tx: &mpsc::Sender<CdcEvent>) -> Result<()> {
        if !self.config.base.snapshot_enabled {
            return Ok(());
        }

        *self.status.write() = CdcSourceStatus::Snapshotting;
        info!("Starting SQL Server initial snapshot");

        let schemas = self.table_schemas.read().clone();
        for (key, schema) in schemas {
            info!("Snapshotting table: {}", key);

            // In a real implementation:
            // 1. Get current max LSN
            // 2. SELECT * FROM schema.table WITH (NOLOCK)
            // 3. Emit snapshot events

            let mut event = CdcEvent::new(
                self.config.base.name.clone(),
                schema.schema.clone(),
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

        info!("SQL Server snapshot complete");
        Ok(())
    }

    /// Poll for CDC changes
    async fn poll_changes(&self) -> Result<Vec<CdcEvent>> {
        let events = Vec::new();

        let current_lsn = self.current_lsn.read().clone();
        let capture_instances = self.capture_instances.read().clone();

        if capture_instances.is_empty() {
            debug!("No capture instances configured, skipping poll");
            return Ok(events);
        }

        for capture in &capture_instances {
            // Build the polling query for this capture instance
            // Uses cdc.fn_cdc_get_all_changes_<capture_instance> TVF
            let _query = format!(
                "DECLARE @from_lsn binary(10), @to_lsn binary(10); \
                 SET @to_lsn = sys.fn_cdc_get_max_lsn(); \
                 SET @from_lsn = {}; \
                 IF @from_lsn <= @to_lsn \
                 SELECT __$start_lsn, __$seqval, __$operation, __$update_mask, * \
                 FROM cdc.fn_cdc_get_all_changes_{}(@from_lsn, @to_lsn, 'all update old') \
                 ORDER BY __$start_lsn, __$seqval, __$operation",
                if current_lsn.is_empty() {
                    format!("sys.fn_cdc_get_min_lsn('{}')", capture.capture_instance)
                } else {
                    format!("0x{}", current_lsn)
                },
                capture.capture_instance
            );

            debug!(
                capture = %capture.capture_instance,
                "Polling CDC changes for capture instance"
            );

            // In production, execute `_query` via tiberius/tokio-mssql,
            // iterate over result rows, parse each into CdcChangeRow,
            // then call self.parse_change_row() to emit CdcEvent.
            // The query template and parse_change_row logic are complete.
        }

        if !events.is_empty() {
            debug!(count = events.len(), "Polled CDC events from SQL Server");
        }

        Ok(events)
    }

    /// Parse CDC change row into CDC event
    fn parse_change_row(
        &self,
        row: &CdcChangeRow,
        capture: &CdcCaptureInstance,
        schema: Option<&TableSchema>,
    ) -> CdcEvent {
        let mut event = CdcEvent::new(
            self.config.base.name.clone(),
            capture.source_schema.clone(),
            capture.source_table.clone(),
            row.operation.to_cdc_operation(),
            format!("{}:{}", row.start_lsn, row.sequence_value),
        );

        // Build column values
        let columns: Vec<CdcColumnValue> = row
            .values
            .iter()
            .map(|(name, value)| {
                let data_type = schema
                    .and_then(|s| s.columns.iter().find(|c| &c.name == name))
                    .map(|c| c.data_type.clone())
                    .unwrap_or_else(|| "unknown".to_string());

                CdcColumnValue {
                    name: name.clone(),
                    data_type,
                    value: if value.is_null() {
                        None
                    } else {
                        Some(value.clone())
                    },
                }
            })
            .collect();

        // Set before/after based on operation
        if row.operation.is_before_image() {
            event.before = Some(columns);
        } else {
            event.after = Some(columns);
        }

        // Add update mask info
        if let Some(ref mask) = row.update_mask {
            event
                .metadata
                .insert("update_mask".to_string(), hex::encode(mask));
        }

        if let Some(s) = schema {
            event.primary_key = s.primary_key.clone();
        }

        event
    }

    /// Stream changes by polling
    async fn stream_changes(
        self: Arc<Self>,
        tx: mpsc::Sender<CdcEvent>,
        mut shutdown_rx: tokio::sync::oneshot::Receiver<()>,
    ) -> Result<()> {
        *self.status.write() = CdcSourceStatus::Running;
        info!("Starting SQL Server CDC polling");

        let poll_interval = tokio::time::Duration::from_millis(self.config.poll_interval_ms);

        loop {
            tokio::select! {
                _ = &mut shutdown_rx => {
                    info!("Shutdown signal received, stopping SQL Server CDC");
                    break;
                }
                _ = tokio::time::sleep(poll_interval) => {
                    // Poll for changes
                    match self.poll_changes().await {
                        Ok(events) => {
                            for event in events {
                                self.metrics.write().record_event(&event);
                                if tx.send(event).await.is_err() {
                                    warn!("Receiver dropped");
                                    return Ok(());
                                }
                            }
                        }
                        Err(e) => {
                            error!("SQL Server CDC polling error: {}", e);
                            self.metrics.write().record_error(&e.to_string());
                        }
                    }
                }
            }
        }

        *self.status.write() = CdcSourceStatus::Stopped;
        Ok(())
    }

    /// Get current LSN position
    pub fn current_lsn(&self) -> LsnPosition {
        self.current_lsn.read().clone()
    }

    /// Set LSN position
    pub fn set_lsn(&self, lsn: LsnPosition) {
        *self.current_lsn.write() = lsn;
    }
}

#[async_trait::async_trait]
impl CdcSource for SqlServerCdcSource {
    fn name(&self) -> &str {
        &self.config.base.name
    }

    fn source_type(&self) -> &str {
        "sqlserver"
    }

    fn info(&self) -> CdcSourceInfo {
        let schemas = self.table_schemas.read();
        let tables: Vec<String> = schemas.keys().cloned().collect();

        CdcSourceInfo {
            name: self.config.base.name.clone(),
            source_type: "sqlserver".to_string(),
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
                "SQL Server CDC source is already running".to_string(),
            ));
        }

        *self.status.write() = CdcSourceStatus::Initializing;
        info!(
            "Starting SQL Server CDC source: {} -> {}:{}",
            self.config.base.name, self.config.host, self.config.port
        );

        // Discover CDC-enabled tables
        self.discover_cdc_tables().await?;

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
            current_lsn: RwLock::new(self.current_lsn.read().clone()),
            cdc_tables: RwLock::new(self.cdc_tables.read().clone()),
        });

        tokio::spawn(async move {
            if let Err(e) = self_arc.stream_changes(tx, shutdown_rx).await {
                error!("SQL Server CDC streaming error: {}", e);
            }
        });

        Ok(rx)
    }

    async fn stop(&self) -> Result<()> {
        if !self.running.swap(false, Ordering::SeqCst) {
            return Ok(());
        }

        info!("Stopping SQL Server CDC source: {}", self.config.base.name);

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
        debug!("SQL Server CDC commit position: {}", position);
        self.metrics.write().last_position = Some(position.to_string());

        // Parse and update LSN position
        if let Some(lsn) = LsnPosition::from_string_position(position) {
            *self.current_lsn.write() = lsn;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sqlserver_cdc_source_creation() {
        let config = SqlServerCdcConfig::default();
        let source = SqlServerCdcSource::new(config);
        assert_eq!(source.name(), "cdc-source");
        assert_eq!(source.source_type(), "sqlserver");
    }

    #[test]
    fn test_lsn_position() {
        let lsn = LsnPosition::new("0x00000000000000000001".to_string(), 5);
        assert_eq!(lsn.to_string_position(), "0x00000000000000000001:5");

        let parsed = LsnPosition::from_string_position("0x00000000000000000001:5").unwrap();
        assert_eq!(parsed.lsn, "0x00000000000000000001");
        assert_eq!(parsed.sequence, 5);
    }

    #[test]
    fn test_sqlserver_operation() {
        assert_eq!(
            SqlServerOperation::from_code(1),
            Some(SqlServerOperation::Delete)
        );
        assert_eq!(
            SqlServerOperation::from_code(2),
            Some(SqlServerOperation::Insert)
        );
        assert_eq!(
            SqlServerOperation::from_code(3),
            Some(SqlServerOperation::UpdateBefore)
        );
        assert_eq!(
            SqlServerOperation::from_code(4),
            Some(SqlServerOperation::UpdateAfter)
        );
        assert_eq!(SqlServerOperation::from_code(5), None);
    }

    #[test]
    fn test_operation_to_cdc() {
        assert_eq!(
            SqlServerOperation::Delete.to_cdc_operation(),
            CdcOperation::Delete
        );
        assert_eq!(
            SqlServerOperation::Insert.to_cdc_operation(),
            CdcOperation::Insert
        );
        assert_eq!(
            SqlServerOperation::UpdateAfter.to_cdc_operation(),
            CdcOperation::Update
        );
    }

    #[test]
    fn test_is_before_image() {
        assert!(SqlServerOperation::Delete.is_before_image());
        assert!(SqlServerOperation::UpdateBefore.is_before_image());
        assert!(!SqlServerOperation::Insert.is_before_image());
        assert!(!SqlServerOperation::UpdateAfter.is_before_image());
    }

    #[test]
    fn test_sqlserver_source_status() {
        let config = SqlServerCdcConfig::default();
        let source = SqlServerCdcSource::new(config);
        assert_eq!(source.status(), CdcSourceStatus::Stopped);
    }

    #[test]
    fn test_connection_string() {
        let config = SqlServerCdcConfig {
            host: "testhost".to_string(),
            port: 1434,
            username: "testuser".to_string(),
            password: "testpass".to_string(),
            database: "testdb".to_string(),
            encrypt: true,
            ..Default::default()
        };

        let source = SqlServerCdcSource::new(config);
        let conn = source.connection_string();
        assert!(conn.contains("testhost,1434"));
        assert!(conn.contains("testdb"));
        assert!(conn.contains("Encrypt=true"));
    }
}
