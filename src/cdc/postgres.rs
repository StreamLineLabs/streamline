//! PostgreSQL CDC source implementation
//!
//! This module provides CDC capabilities for PostgreSQL using logical replication.
//! It supports the pgoutput and wal2json output plugins.

use super::config::{PostgresCdcConfig, PostgresOutputPlugin, PostgresSnapshotMode};
use super::{
    CdcColumnValue, CdcEvent, CdcOperation, CdcSource, CdcSourceInfo, CdcSourceMetrics,
    CdcSourceStatus,
};
use crate::error::{Result, StreamlineError};
use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_postgres::{Client, NoTls};
use tracing::{debug, error, info, trace, warn};

// ---------------------------------------------------------------------------
// WAL binary message types (pgoutput logical replication protocol)
// ---------------------------------------------------------------------------

/// WAL message types from the pgoutput logical replication protocol.
/// These correspond to the single-byte message type tags in the streaming
/// replication protocol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalMessageType {
    /// Relation message — describes a table's schema.
    /// Sent before the first DML message referencing a relation.
    Relation,
    /// Begin transaction
    Begin,
    /// Commit transaction
    Commit,
    /// INSERT row
    Insert,
    /// UPDATE row
    Update,
    /// DELETE row
    Delete,
    /// Keepalive / unknown
    Other(u8),
}

impl WalMessageType {
    /// Parse the message type tag byte used by the pgoutput wire protocol.
    pub fn from_tag(tag: u8) -> Self {
        match tag {
            b'R' => WalMessageType::Relation,
            b'B' => WalMessageType::Begin,
            b'C' => WalMessageType::Commit,
            b'I' => WalMessageType::Insert,
            b'U' => WalMessageType::Update,
            b'D' => WalMessageType::Delete,
            other => WalMessageType::Other(other),
        }
    }
}

impl std::fmt::Display for WalMessageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalMessageType::Relation => write!(f, "RELATION"),
            WalMessageType::Begin => write!(f, "BEGIN"),
            WalMessageType::Commit => write!(f, "COMMIT"),
            WalMessageType::Insert => write!(f, "INSERT"),
            WalMessageType::Update => write!(f, "UPDATE"),
            WalMessageType::Delete => write!(f, "DELETE"),
            WalMessageType::Other(tag) => write!(f, "OTHER(0x{:02x})", tag),
        }
    }
}

/// Information about a relation (table) received via a RELATION WAL message.
/// Cached locally so that subsequent INSERT/UPDATE/DELETE messages can be
/// decoded without re-querying the catalog.
#[derive(Debug, Clone)]
pub struct RelationInfo {
    /// Relation OID
    pub relation_id: u32,
    /// Schema (namespace) name
    pub schema: String,
    /// Table name
    pub table: String,
    /// Column descriptors in ordinal order
    pub columns: Vec<WalColumnInfo>,
    /// Replica identity setting (d = default, n = nothing, f = full, i = index)
    pub replica_identity: char,
}

/// Column descriptor within a [`RelationInfo`].
#[derive(Debug, Clone)]
pub struct WalColumnInfo {
    /// Whether this column is part of the replica identity key
    pub is_key: bool,
    /// Column name
    pub name: String,
    /// PostgreSQL type OID
    pub type_oid: u32,
    /// Type modifier (-1 if none)
    pub type_modifier: i32,
}

/// A parsed binary WAL message from the pgoutput logical replication stream.
#[derive(Debug, Clone)]
pub enum WalMessage {
    /// A RELATION message that describes a table's schema.
    Relation(RelationInfo),
    /// BEGIN — a new transaction.
    Begin {
        /// Final LSN of the transaction
        final_lsn: u64,
        /// Commit timestamp (microseconds since 2000-01-01)
        commit_ts: i64,
        /// Transaction ID (XID)
        xid: u32,
    },
    /// COMMIT — end of a transaction.
    Commit {
        /// LSN of the commit record
        commit_lsn: u64,
        /// End LSN (next byte after the commit)
        end_lsn: u64,
        /// Commit timestamp (microseconds since 2000-01-01)
        commit_ts: i64,
    },
    /// INSERT — a new row.
    Insert {
        /// Relation OID referencing a previously received Relation message
        relation_id: u32,
        /// Column values for the new tuple
        new_tuple: Vec<WalTupleColumn>,
    },
    /// UPDATE — a modified row.
    Update {
        /// Relation OID
        relation_id: u32,
        /// Old tuple (present only when replica identity is FULL or columns
        /// changed are part of the index)
        old_tuple: Option<Vec<WalTupleColumn>>,
        /// New tuple
        new_tuple: Vec<WalTupleColumn>,
    },
    /// DELETE — a removed row.
    Delete {
        /// Relation OID
        relation_id: u32,
        /// Key or full old tuple, depending on replica identity
        old_tuple: Vec<WalTupleColumn>,
    },
}

/// A single column value inside a WAL tuple.
#[derive(Debug, Clone)]
pub enum WalTupleColumn {
    /// NULL value
    Null,
    /// Unchanged TOAST datum (not sent in the stream)
    Unchanged,
    /// Text-format value
    Text(String),
}

// ---------------------------------------------------------------------------
// WAL message parser
// ---------------------------------------------------------------------------

/// Parse a raw pgoutput binary WAL message payload.
///
/// Returns `None` for message types that are not relevant (e.g. TYPE,
/// ORIGIN) or if the payload is too short to be valid.
pub fn parse_wal_message(data: &[u8]) -> Option<WalMessage> {
    if data.is_empty() {
        return None;
    }

    let tag = WalMessageType::from_tag(data[0]);
    let body = &data[1..];

    match tag {
        WalMessageType::Begin => parse_begin(body),
        WalMessageType::Commit => parse_commit(body),
        WalMessageType::Relation => parse_relation(body),
        WalMessageType::Insert => parse_insert(body),
        WalMessageType::Update => parse_update(body),
        WalMessageType::Delete => parse_delete(body),
        _ => None,
    }
}

fn read_u32(buf: &[u8], offset: &mut usize) -> Option<u32> {
    if *offset + 4 > buf.len() {
        return None;
    }
    let val = u32::from_be_bytes([
        buf[*offset],
        buf[*offset + 1],
        buf[*offset + 2],
        buf[*offset + 3],
    ]);
    *offset += 4;
    Some(val)
}

fn read_i32(buf: &[u8], offset: &mut usize) -> Option<i32> {
    read_u32(buf, offset).map(|v| v as i32)
}

fn read_u64(buf: &[u8], offset: &mut usize) -> Option<u64> {
    if *offset + 8 > buf.len() {
        return None;
    }
    let val = u64::from_be_bytes([
        buf[*offset],
        buf[*offset + 1],
        buf[*offset + 2],
        buf[*offset + 3],
        buf[*offset + 4],
        buf[*offset + 5],
        buf[*offset + 6],
        buf[*offset + 7],
    ]);
    *offset += 8;
    Some(val)
}

fn read_i64(buf: &[u8], offset: &mut usize) -> Option<i64> {
    read_u64(buf, offset).map(|v| v as i64)
}

fn read_u8(buf: &[u8], offset: &mut usize) -> Option<u8> {
    if *offset >= buf.len() {
        return None;
    }
    let val = buf[*offset];
    *offset += 1;
    Some(val)
}

fn read_u16(buf: &[u8], offset: &mut usize) -> Option<u16> {
    if *offset + 2 > buf.len() {
        return None;
    }
    let val = u16::from_be_bytes([buf[*offset], buf[*offset + 1]]);
    *offset += 2;
    Some(val)
}

fn read_cstr(buf: &[u8], offset: &mut usize) -> Option<String> {
    let start = *offset;
    while *offset < buf.len() && buf[*offset] != 0 {
        *offset += 1;
    }
    if *offset >= buf.len() {
        return None;
    }
    let s = String::from_utf8_lossy(&buf[start..*offset]).to_string();
    *offset += 1; // skip null terminator
    Some(s)
}

fn parse_begin(buf: &[u8]) -> Option<WalMessage> {
    let mut off = 0;
    let final_lsn = read_u64(buf, &mut off)?;
    let commit_ts = read_i64(buf, &mut off)?;
    let xid = read_u32(buf, &mut off)?;
    Some(WalMessage::Begin {
        final_lsn,
        commit_ts,
        xid,
    })
}

fn parse_commit(buf: &[u8]) -> Option<WalMessage> {
    let mut off = 0;
    let _flags = read_u8(buf, &mut off)?;
    let commit_lsn = read_u64(buf, &mut off)?;
    let end_lsn = read_u64(buf, &mut off)?;
    let commit_ts = read_i64(buf, &mut off)?;
    Some(WalMessage::Commit {
        commit_lsn,
        end_lsn,
        commit_ts,
    })
}

fn parse_relation(buf: &[u8]) -> Option<WalMessage> {
    let mut off = 0;
    let relation_id = read_u32(buf, &mut off)?;
    let schema = read_cstr(buf, &mut off)?;
    let table = read_cstr(buf, &mut off)?;
    let replica_identity = read_u8(buf, &mut off)? as char;
    let ncols = read_u16(buf, &mut off)? as usize;

    let mut columns = Vec::with_capacity(ncols);
    for _ in 0..ncols {
        let flags = read_u8(buf, &mut off)?;
        let is_key = (flags & 0x01) != 0;
        let name = read_cstr(buf, &mut off)?;
        let type_oid = read_u32(buf, &mut off)?;
        let type_modifier = read_i32(buf, &mut off)?;
        columns.push(WalColumnInfo {
            is_key,
            name,
            type_oid,
            type_modifier,
        });
    }

    Some(WalMessage::Relation(RelationInfo {
        relation_id,
        schema,
        table,
        columns,
        replica_identity,
    }))
}

fn parse_tuple_data(buf: &[u8], off: &mut usize) -> Option<Vec<WalTupleColumn>> {
    let ncols = read_u16(buf, off)? as usize;
    let mut cols = Vec::with_capacity(ncols);
    for _ in 0..ncols {
        let col_type = read_u8(buf, off)?;
        match col_type {
            b'n' => cols.push(WalTupleColumn::Null),
            b'u' => cols.push(WalTupleColumn::Unchanged),
            b't' => {
                let len = read_u32(buf, off)? as usize;
                if *off + len > buf.len() {
                    warn!(
                        expected_len = len,
                        remaining = buf.len() - *off,
                        "WAL tuple text column truncated: declared length exceeds buffer"
                    );
                    return None;
                }
                let text = String::from_utf8_lossy(&buf[*off..*off + len]).to_string();
                *off += len;
                cols.push(WalTupleColumn::Text(text));
            }
            unknown => {
                warn!(
                    col_type = %format!("0x{:02x}", unknown),
                    "Unexpected WAL tuple column type byte, skipping remaining tuple"
                );
                return None;
            }
        }
    }
    Some(cols)
}

fn parse_insert(buf: &[u8]) -> Option<WalMessage> {
    let mut off = 0;
    let relation_id = read_u32(buf, &mut off)?;
    let _new_tag = read_u8(buf, &mut off)?; // 'N'
    let new_tuple = parse_tuple_data(buf, &mut off)?;
    Some(WalMessage::Insert {
        relation_id,
        new_tuple,
    })
}

fn parse_update(buf: &[u8]) -> Option<WalMessage> {
    let mut off = 0;
    let relation_id = read_u32(buf, &mut off)?;
    let tag = read_u8(buf, &mut off)?;

    let old_tuple = if tag == b'K' || tag == b'O' {
        let tuple = parse_tuple_data(buf, &mut off)?;
        let _new_tag = read_u8(buf, &mut off)?;
        Some(tuple)
    } else {
        None
    };

    let new_tuple = parse_tuple_data(buf, &mut off)?;
    Some(WalMessage::Update {
        relation_id,
        old_tuple,
        new_tuple,
    })
}

fn parse_delete(buf: &[u8]) -> Option<WalMessage> {
    let mut off = 0;
    let relation_id = read_u32(buf, &mut off)?;
    let _key_tag = read_u8(buf, &mut off)?; // 'K' or 'O'
    let old_tuple = parse_tuple_data(buf, &mut off)?;
    Some(WalMessage::Delete {
        relation_id,
        old_tuple,
    })
}

// ---------------------------------------------------------------------------
// LSN tracking
// ---------------------------------------------------------------------------

/// Tracks the current Log Sequence Number position for resumable replication.
#[derive(Debug)]
pub struct LsnTracker {
    /// Last flushed (confirmed) LSN
    flushed_lsn: parking_lot::Mutex<u64>,
    /// Last received LSN
    received_lsn: parking_lot::Mutex<u64>,
}

impl LsnTracker {
    /// Create a new LSN tracker starting from the given position.
    pub fn new(start_lsn: u64) -> Self {
        Self {
            flushed_lsn: parking_lot::Mutex::new(start_lsn),
            received_lsn: parking_lot::Mutex::new(start_lsn),
        }
    }

    /// Record that a WAL message at the given LSN was received.
    pub fn advance_received(&self, lsn: u64) {
        let mut current = self.received_lsn.lock();
        if lsn > *current {
            *current = lsn;
        }
    }

    /// Confirm that all events up to the given LSN have been durably processed.
    pub fn confirm_flush(&self, lsn: u64) {
        let mut current = self.flushed_lsn.lock();
        if lsn > *current {
            *current = lsn;
        }
    }

    /// Get the last flushed LSN — safe point to resume from after a restart.
    pub fn flushed_lsn(&self) -> u64 {
        *self.flushed_lsn.lock()
    }

    /// Get the last received LSN.
    pub fn received_lsn(&self) -> u64 {
        *self.received_lsn.lock()
    }

    /// Format an LSN as the standard PostgreSQL `X/YYYYYYYY` representation.
    pub fn format_lsn(lsn: u64) -> String {
        let hi = (lsn >> 32) as u32;
        let lo = lsn as u32;
        format!("{:X}/{:08X}", hi, lo)
    }

    /// Parse a PostgreSQL LSN string (`X/YYYYYYYY`) into a u64.
    pub fn parse_lsn(s: &str) -> Option<u64> {
        let parts: Vec<&str> = s.split('/').collect();
        if parts.len() != 2 {
            return None;
        }
        let hi = u32::from_str_radix(parts[0], 16).ok()?;
        let lo = u32::from_str_radix(parts[1], 16).ok()?;
        Some(((hi as u64) << 32) | lo as u64)
    }
}

impl Default for LsnTracker {
    fn default() -> Self {
        Self::new(0)
    }
}

// ---------------------------------------------------------------------------
// WAL-to-CdcEvent conversion
// ---------------------------------------------------------------------------

/// Convert WAL messages into `CdcEvent`s using the relation cache.
///
/// `relations` must be populated by prior RELATION messages.
pub fn wal_message_to_cdc_events(
    msg: &WalMessage,
    relations: &HashMap<u32, RelationInfo>,
    database: &str,
    config: &PostgresCdcConfig,
) -> Vec<CdcEvent> {
    match msg {
        WalMessage::Begin { xid, .. } => {
            let mut ev = CdcEvent::new(
                database.to_string(),
                String::new(),
                String::new(),
                CdcOperation::Begin,
                String::new(),
            );
            ev.transaction_id = Some(xid.to_string());
            vec![ev]
        }
        WalMessage::Commit { commit_lsn, .. } => {
            let ev = CdcEvent::new(
                database.to_string(),
                String::new(),
                String::new(),
                CdcOperation::Commit,
                LsnTracker::format_lsn(*commit_lsn),
            );
            vec![ev]
        }
        WalMessage::Insert {
            relation_id,
            new_tuple,
        } => {
            let Some(rel) = relations.get(relation_id) else {
                warn!(
                    relation_id,
                    "INSERT references unknown relation (missing RELATION message); skipping event"
                );
                return Vec::new();
            };
            let mut ev = CdcEvent::new(
                database.to_string(),
                rel.schema.clone(),
                rel.table.clone(),
                CdcOperation::Insert,
                String::new(),
            );
            ev.after = Some(tuple_to_columns(new_tuple, &rel.columns));
            ev.primary_key = rel
                .columns
                .iter()
                .filter(|c| c.is_key)
                .map(|c| c.name.clone())
                .collect();
            apply_topic_mapping(&mut ev, config);
            vec![ev]
        }
        WalMessage::Update {
            relation_id,
            old_tuple,
            new_tuple,
        } => {
            let Some(rel) = relations.get(relation_id) else {
                warn!(
                    relation_id,
                    "UPDATE references unknown relation (missing RELATION message); skipping event"
                );
                return Vec::new();
            };
            let mut ev = CdcEvent::new(
                database.to_string(),
                rel.schema.clone(),
                rel.table.clone(),
                CdcOperation::Update,
                String::new(),
            );
            ev.after = Some(tuple_to_columns(new_tuple, &rel.columns));
            if let Some(old) = old_tuple {
                ev.before = Some(tuple_to_columns(old, &rel.columns));
            }
            ev.primary_key = rel
                .columns
                .iter()
                .filter(|c| c.is_key)
                .map(|c| c.name.clone())
                .collect();
            apply_topic_mapping(&mut ev, config);
            vec![ev]
        }
        WalMessage::Delete {
            relation_id,
            old_tuple,
        } => {
            let Some(rel) = relations.get(relation_id) else {
                warn!(
                    relation_id,
                    "DELETE references unknown relation (missing RELATION message); skipping event"
                );
                return Vec::new();
            };
            let mut ev = CdcEvent::new(
                database.to_string(),
                rel.schema.clone(),
                rel.table.clone(),
                CdcOperation::Delete,
                String::new(),
            );
            ev.before = Some(tuple_to_columns(old_tuple, &rel.columns));
            ev.primary_key = rel
                .columns
                .iter()
                .filter(|c| c.is_key)
                .map(|c| c.name.clone())
                .collect();
            apply_topic_mapping(&mut ev, config);
            vec![ev]
        }
        WalMessage::Relation(_) => Vec::new(),
    }
}

/// Convert a WAL tuple into `CdcColumnValue` entries.
fn tuple_to_columns(
    tuple: &[WalTupleColumn],
    col_info: &[WalColumnInfo],
) -> Vec<CdcColumnValue> {
    tuple
        .iter()
        .zip(col_info.iter())
        .map(|(val, info)| {
            let json_val = match val {
                WalTupleColumn::Null | WalTupleColumn::Unchanged => None,
                WalTupleColumn::Text(s) => text_to_json_value(s, info.type_oid),
            };
            CdcColumnValue {
                name: info.name.clone(),
                data_type: pg_type_name(info.type_oid),
                value: json_val,
            }
        })
        .collect()
}

/// Best-effort conversion of a text-format PG value to a JSON value based on the type OID.
fn text_to_json_value(s: &str, type_oid: u32) -> Option<serde_json::Value> {
    match type_oid {
        // bool
        16 => Some(serde_json::Value::Bool(s == "t" || s == "true")),
        // int2, int4, int8, oid
        21 | 23 | 20 | 26 => s.parse::<i64>().ok().map(serde_json::Value::from),
        // float4, float8, numeric
        700 | 701 | 1700 => s
            .parse::<f64>()
            .ok()
            .and_then(serde_json::Number::from_f64)
            .map(serde_json::Value::Number),
        // json, jsonb
        114 | 3802 => serde_json::from_str(s).ok(),
        // Everything else → string
        _ => Some(serde_json::Value::String(s.to_string())),
    }
}

/// Map common PostgreSQL type OIDs to human-readable names.
fn pg_type_name(oid: u32) -> String {
    match oid {
        16 => "bool".to_string(),
        20 => "int8".to_string(),
        21 => "int2".to_string(),
        23 => "int4".to_string(),
        25 => "text".to_string(),
        26 => "oid".to_string(),
        114 => "json".to_string(),
        700 => "float4".to_string(),
        701 => "float8".to_string(),
        1043 => "varchar".to_string(),
        1082 => "date".to_string(),
        1114 => "timestamp".to_string(),
        1184 => "timestamptz".to_string(),
        1700 => "numeric".to_string(),
        2950 => "uuid".to_string(),
        3802 => "jsonb".to_string(),
        _ => format!("pg_type_{}", oid),
    }
}

/// Apply topic mapping overrides from the config to the event metadata.
fn apply_topic_mapping(event: &mut CdcEvent, config: &PostgresCdcConfig) {
    let table_key = format!("{}.{}", event.schema, event.table);
    if let Some(topic) = config.topic_mapping.get(&table_key) {
        event
            .metadata
            .insert("mapped_topic".to_string(), topic.clone());
    } else if let Some(topic) = config.topic_mapping.get(&event.table) {
        event
            .metadata
            .insert("mapped_topic".to_string(), topic.clone());
    }
}

/// PostgreSQL CDC source
pub struct PostgresCdcSource {
    /// Configuration
    config: PostgresCdcConfig,
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
    /// LSN position tracker for resumable replication
    lsn_tracker: Arc<LsnTracker>,
    /// Cached relation info from WAL RELATION messages (relation OID → info)
    relation_cache: RwLock<HashMap<u32, RelationInfo>>,
    /// Consecutive connection failure count (for reconnection backoff)
    consecutive_failures: std::sync::atomic::AtomicU32,
}

/// Schema information for a table
#[derive(Debug, Clone)]
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
    /// Data type OID
    type_oid: u32,
    /// Data type name
    type_name: String,
    /// Is nullable
    nullable: bool,
}

impl PostgresCdcSource {
    /// Create a new PostgreSQL CDC source
    pub fn new(config: PostgresCdcConfig) -> Self {
        Self {
            config,
            status: RwLock::new(CdcSourceStatus::Stopped),
            metrics: RwLock::new(CdcSourceMetrics::default()),
            running: AtomicBool::new(false),
            created_at: Utc::now(),
            table_schemas: RwLock::new(HashMap::new()),
            shutdown_tx: RwLock::new(None),
            lsn_tracker: Arc::new(LsnTracker::default()),
            relation_cache: RwLock::new(HashMap::new()),
            consecutive_failures: std::sync::atomic::AtomicU32::new(0),
        }
    }

    /// Connect to PostgreSQL
    async fn connect(&self) -> Result<Client> {
        let (client, connection) = tokio_postgres::connect(&self.config.connection_string, NoTls)
            .await
            .map_err(|e| StreamlineError::Config(format!("PostgreSQL connection failed: {}", e)))?;

        // Spawn the connection handler
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("PostgreSQL connection error: {}", e);
            }
        });

        Ok(client)
    }

    /// Check if PostgreSQL supports logical replication
    async fn check_replication_support(&self, client: &Client) -> Result<()> {
        let row = client
            .query_one("SHOW wal_level", &[])
            .await
            .map_err(|e| StreamlineError::Config(format!("Failed to check wal_level: {}", e)))?;

        let wal_level: String = row.get(0);
        if wal_level != "logical" {
            return Err(StreamlineError::Config(format!(
                "PostgreSQL wal_level must be 'logical', got '{}'. Set wal_level = logical in postgresql.conf",
                wal_level
            )));
        }

        Ok(())
    }

    /// Create replication slot if needed
    async fn ensure_replication_slot(&self, client: &Client) -> Result<()> {
        // Check if slot exists
        let exists = client
            .query_one(
                "SELECT 1 FROM pg_replication_slots WHERE slot_name = $1",
                &[&self.config.slot_name],
            )
            .await
            .is_ok();

        if !exists && self.config.create_slot {
            info!("Creating replication slot: {}", self.config.slot_name);

            let plugin = self.config.output_plugin.to_string();
            client
                .execute(
                    &format!(
                        "SELECT pg_create_logical_replication_slot('{}', '{}')",
                        self.config.slot_name, plugin
                    ),
                    &[],
                )
                .await
                .map_err(|e| {
                    StreamlineError::Config(format!("Failed to create replication slot: {}", e))
                })?;

            info!("Created replication slot: {}", self.config.slot_name);
        } else if !exists {
            return Err(StreamlineError::Config(format!(
                "Replication slot '{}' does not exist and create_slot is false",
                self.config.slot_name
            )));
        }

        Ok(())
    }

    /// Ensure publication exists (for pgoutput)
    async fn ensure_publication(&self, client: &Client) -> Result<()> {
        if self.config.output_plugin != PostgresOutputPlugin::PgOutput {
            return Ok(());
        }

        // Check if publication exists
        let exists = client
            .query_one(
                "SELECT 1 FROM pg_publication WHERE pubname = $1",
                &[&self.config.publication_name],
            )
            .await
            .is_ok();

        if !exists {
            info!("Creating publication: {}", self.config.publication_name);

            // Determine tables to publish
            let tables_clause = if self.config.base.include_tables.is_empty() {
                "FOR ALL TABLES".to_string()
            } else {
                let tables: Vec<String> = self
                    .config
                    .base
                    .include_tables
                    .iter()
                    .map(|t| {
                        if t.contains('.') {
                            t.clone()
                        } else {
                            format!("public.{}", t)
                        }
                    })
                    .collect();
                format!("FOR TABLE {}", tables.join(", "))
            };

            client
                .execute(
                    &format!(
                        "CREATE PUBLICATION {} {}",
                        self.config.publication_name, tables_clause
                    ),
                    &[],
                )
                .await
                .map_err(|e| {
                    StreamlineError::Config(format!("Failed to create publication: {}", e))
                })?;

            info!("Created publication: {}", self.config.publication_name);
        }

        Ok(())
    }

    /// Load table schemas
    async fn load_table_schemas(&self, client: &Client) -> Result<()> {
        let rows = client
            .query(
                r#"
                SELECT
                    n.nspname AS schema_name,
                    c.relname AS table_name,
                    a.attname AS column_name,
                    a.atttypid AS type_oid,
                    t.typname AS type_name,
                    NOT a.attnotnull AS nullable,
                    COALESCE(pk.is_pk, false) AS is_pk
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                JOIN pg_attribute a ON a.attrelid = c.oid
                JOIN pg_type t ON t.oid = a.atttypid
                LEFT JOIN (
                    SELECT i.indrelid, unnest(i.indkey) AS attnum, true AS is_pk
                    FROM pg_index i
                    WHERE i.indisprimary
                ) pk ON pk.indrelid = c.oid AND pk.attnum = a.attnum
                WHERE c.relkind = 'r'
                AND n.nspname NOT IN ('pg_catalog', 'information_schema')
                AND a.attnum > 0
                AND NOT a.attisdropped
                ORDER BY n.nspname, c.relname, a.attnum
                "#,
                &[],
            )
            .await
            .map_err(|e| StreamlineError::Config(format!("Failed to load table schemas: {}", e)))?;

        let mut schemas = HashMap::new();
        for row in rows {
            let schema_name: String = row.get("schema_name");
            let table_name: String = row.get("table_name");

            // Check if we should capture this table
            if !self
                .config
                .base
                .should_capture_table(&schema_name, &table_name)
            {
                continue;
            }

            let key = format!("{}.{}", schema_name, table_name);

            let column_name: String = row.get("column_name");
            let type_oid: u32 = row.get::<_, i32>("type_oid") as u32;
            let type_name: String = row.get("type_name");
            let nullable: bool = row.get("nullable");
            let is_pk: bool = row.get("is_pk");

            let entry = schemas.entry(key.clone()).or_insert_with(|| TableSchema {
                schema: schema_name.clone(),
                table: table_name.clone(),
                columns: Vec::new(),
                primary_key: Vec::new(),
            });

            entry.columns.push(ColumnDef {
                name: column_name.clone(),
                type_oid,
                type_name,
                nullable,
            });

            if is_pk {
                entry.primary_key.push(column_name);
            }
        }

        *self.table_schemas.write() = schemas;
        Ok(())
    }

    /// Perform initial snapshot
    async fn perform_snapshot(&self, client: &Client, tx: &mpsc::Sender<CdcEvent>) -> Result<()> {
        match self.config.snapshot_mode {
            PostgresSnapshotMode::Never => {
                info!("Snapshot mode is 'never', skipping initial snapshot");
                return Ok(());
            }
            PostgresSnapshotMode::SchemaOnly => {
                info!("Snapshot mode is 'schema_only', loading schemas only");
                // Schemas are already loaded in load_table_schemas
                return Ok(());
            }
            PostgresSnapshotMode::Initial => {
                if !self.config.base.snapshot_enabled {
                    return Ok(());
                }
            }
        }

        *self.status.write() = CdcSourceStatus::Snapshotting;
        info!("Starting initial snapshot");

        let schemas = self.table_schemas.read().clone();
        let mut snapshot_errors = Vec::new();
        for (key, schema) in schemas {
            info!("Snapshotting table: {}", key);

            // Read all rows from the table
            let query = format!("SELECT * FROM {}.{}", schema.schema, schema.table);

            let rows = match client.query(&query, &[]).await {
                Ok(rows) => rows,
                Err(e) => {
                    let msg = format!("Snapshot query failed for {}: {}", key, e);
                    warn!(table = %key, error = %e, "Snapshot query failed; skipping table");
                    self.metrics.write().record_error(&msg);
                    snapshot_errors.push(msg);
                    continue;
                }
            };

            for row in rows {
                let columns = self.row_to_columns(&row, &schema);

                let mut event = CdcEvent::new(
                    self.config.base.name.clone(),
                    schema.schema.clone(),
                    schema.table.clone(),
                    CdcOperation::Snapshot,
                    "snapshot".to_string(),
                );
                event.after = Some(columns);
                event.primary_key = schema.primary_key.clone();

                // Record metrics
                self.metrics.write().record_event(&event);

                // Send event
                if tx.send(event).await.is_err() {
                    warn!("Receiver dropped during snapshot");
                    return Ok(());
                }
            }
        }

        if snapshot_errors.is_empty() {
            info!("Snapshot complete");
        } else {
            warn!(
                failed_tables = snapshot_errors.len(),
                "Snapshot completed with errors for {} table(s)",
                snapshot_errors.len()
            );
        }
        Ok(())
    }

    /// Convert a PostgreSQL row to CDC columns
    fn row_to_columns(
        &self,
        row: &tokio_postgres::Row,
        schema: &TableSchema,
    ) -> Vec<CdcColumnValue> {
        schema
            .columns
            .iter()
            .map(|col| {
                let value = self.get_column_value(row, &col.name, col.type_oid);
                CdcColumnValue {
                    name: col.name.clone(),
                    data_type: col.type_name.clone(),
                    value,
                }
            })
            .collect()
    }

    /// Get column value as JSON
    fn get_column_value(
        &self,
        row: &tokio_postgres::Row,
        name: &str,
        _type_oid: u32,
    ) -> Option<serde_json::Value> {
        // Try to get the column index
        let idx = row.columns().iter().position(|c| c.name() == name)?;

        // Try different types
        if let Ok(v) = row.try_get::<_, Option<i64>>(idx) {
            return v.map(serde_json::Value::from);
        }
        if let Ok(v) = row.try_get::<_, Option<i32>>(idx) {
            return v.map(|v| serde_json::Value::from(v as i64));
        }
        if let Ok(v) = row.try_get::<_, Option<f64>>(idx) {
            return v.map(serde_json::Value::from);
        }
        if let Ok(v) = row.try_get::<_, Option<bool>>(idx) {
            return v.map(serde_json::Value::from);
        }
        if let Ok(v) = row.try_get::<_, Option<String>>(idx) {
            return v.map(serde_json::Value::from);
        }
        if let Ok(v) = row.try_get::<_, Option<chrono::NaiveDateTime>>(idx) {
            return v.map(|v| serde_json::Value::from(v.and_utc().to_rfc3339()));
        }
        if let Ok(v) = row.try_get::<_, Option<chrono::DateTime<Utc>>>(idx) {
            return v.map(|v| serde_json::Value::from(v.to_rfc3339()));
        }
        if let Ok(v) = row.try_get::<_, Option<uuid::Uuid>>(idx) {
            return v.map(|v| serde_json::Value::from(v.to_string()));
        }
        if let Ok(v) = row.try_get::<_, Option<serde_json::Value>>(idx) {
            return v;
        }

        None
    }

    /// Start streaming changes using logical replication (polling approach)
    /// with automatic reconnection and exponential backoff.
    async fn stream_changes(
        self: Arc<Self>,
        tx: mpsc::Sender<CdcEvent>,
        mut shutdown_rx: tokio::sync::oneshot::Receiver<()>,
    ) -> Result<()> {
        *self.status.write() = CdcSourceStatus::Running;
        info!("Starting logical replication polling");

        let max_retries = self.config.max_retries;
        let base_backoff_ms = self.config.retry_backoff_ms;

        loop {
            let result = self
                .run_replication_loop(&tx, &mut shutdown_rx)
                .await;

            match result {
                Ok(ShutdownReason::Requested) => {
                    info!("Shutdown signal received, stopping CDC polling");
                    break;
                }
                Ok(ShutdownReason::ReceiverDropped) => {
                    warn!("Event receiver dropped, stopping CDC polling");
                    break;
                }
                Err(e) => {
                    let failures = self
                        .consecutive_failures
                        .fetch_add(1, Ordering::Relaxed)
                        + 1;
                    self.metrics.write().record_error(&e.to_string());

                    if max_retries > 0 && failures > max_retries {
                        error!(
                            "CDC replication failed after {} consecutive attempts: {}",
                            failures, e
                        );
                        *self.status.write() = CdcSourceStatus::Error;
                        return Err(e);
                    }

                    // Exponential backoff: base * 2^(failures-1), capped at 30s
                    let backoff_ms = (base_backoff_ms * (1u64 << (failures - 1).min(5)))
                        .min(30_000);
                    warn!(
                        "CDC connection error (attempt {}), reconnecting in {}ms: {}",
                        failures, backoff_ms, e
                    );

                    tokio::select! {
                        _ = &mut shutdown_rx => {
                            info!("Shutdown during reconnection backoff");
                            break;
                        }
                        _ = tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms)) => {}
                    }
                }
            }
        }

        *self.status.write() = CdcSourceStatus::Stopped;
        Ok(())
    }

    /// Internal replication loop — runs until shutdown or error.
    async fn run_replication_loop(
        &self,
        tx: &mpsc::Sender<CdcEvent>,
        shutdown_rx: &mut tokio::sync::oneshot::Receiver<()>,
    ) -> std::result::Result<ShutdownReason, StreamlineError> {
        // Connect to PostgreSQL
        let (client, connection) = tokio_postgres::connect(&self.config.connection_string, NoTls)
            .await
            .map_err(|e| StreamlineError::Cdc(format!(
                "PostgreSQL connection failed during replication: {}", e
            )))?;

        // Spawn connection handler
        let running = Arc::new(AtomicBool::new(true));
        let running_clone = running.clone();
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                if running_clone.load(Ordering::Relaxed) {
                    error!("Connection error: {}", e);
                }
            }
        });

        // Reset failure counter on successful connect
        self.consecutive_failures.store(0, Ordering::Relaxed);

        // Build the function call based on output plugin
        let consume_fn = match self.config.output_plugin {
            PostgresOutputPlugin::PgOutput => format!(
                "SELECT * FROM pg_logical_slot_get_changes('{}', NULL, {}, 'proto_version', '1', 'publication_names', '{}')",
                self.config.slot_name,
                self.config.base.batch_size,
                self.config.publication_name
            ),
            PostgresOutputPlugin::Wal2Json => format!(
                "SELECT * FROM pg_logical_slot_get_changes('{}', NULL, {})",
                self.config.slot_name,
                self.config.base.batch_size
            ),
        };

        let poll_interval =
            tokio::time::Duration::from_millis(self.config.base.commit_interval_ms / 10);

        loop {
            tokio::select! {
                _ = &mut *shutdown_rx => {
                    running.store(false, Ordering::Relaxed);
                    return Ok(ShutdownReason::Requested);
                }
                _ = tokio::time::sleep(poll_interval) => {
                    // Poll for changes
                    match client.query(&consume_fn, &[]).await {
                        Ok(rows) => {
                            for row in rows {
                                let lsn: String = row.get(0);
                                let xid: i32 = row.get(1);
                                let data: String = row.get(2);

                                // Track LSN position
                                if let Some(lsn_val) = LsnTracker::parse_lsn(&lsn) {
                                    self.lsn_tracker.advance_received(lsn_val);
                                }

                                // Parse the change data
                                if let Some(events) = self.parse_slot_change(&lsn, xid, &data) {
                                    for event in events {
                                        self.metrics.write().record_event(&event);
                                        if tx.send(event).await.is_err() {
                                            running.store(false, Ordering::Relaxed);
                                            return Ok(ShutdownReason::ReceiverDropped);
                                        }
                                    }
                                }

                                // Confirm flush
                                if let Some(lsn_val) = LsnTracker::parse_lsn(&lsn) {
                                    self.lsn_tracker.confirm_flush(lsn_val);
                                }
                            }
                        }
                        Err(e) => {
                            error!(
                                error = %e,
                                slot = %self.config.slot_name,
                                "CDC polling error while consuming replication slot changes"
                            );
                            return Err(StreamlineError::Cdc(
                                format!("CDC polling error: {}", e),
                            ));
                        }
                    }
                }
            }
        }
    }

    /// Process a raw binary WAL message payload, updating the relation cache
    /// and converting DML messages into `CdcEvent`s.
    pub fn process_wal_message(&self, data: &[u8]) -> Vec<CdcEvent> {
        let Some(msg) = parse_wal_message(data) else {
            if !data.is_empty() {
                warn!(
                    tag = %format!("0x{:02x}", data[0]),
                    len = data.len(),
                    "Failed to parse WAL message (malformed or unsupported type); skipping"
                );
            }
            return Vec::new();
        };

        // Cache RELATION messages — detect schema changes during active replication
        if let WalMessage::Relation(ref rel) = msg {
            let mut cache = self.relation_cache.write();
            if let Some(prev) = cache.get(&rel.relation_id) {
                if prev.columns.len() != rel.columns.len()
                    || prev.columns.iter().zip(rel.columns.iter()).any(|(a, b)| {
                        a.name != b.name || a.type_oid != b.type_oid
                    })
                {
                    info!(
                        relation_id = rel.relation_id,
                        schema = %rel.schema,
                        table = %rel.table,
                        old_cols = prev.columns.len(),
                        new_cols = rel.columns.len(),
                        "Schema change detected during active replication"
                    );
                }
            }
            cache.insert(rel.relation_id, rel.clone());
            return Vec::new();
        }

        let relations = self.relation_cache.read();
        let database = &self.config.base.name;
        wal_message_to_cdc_events(&msg, &relations, database, &self.config)
    }

    /// Get the current LSN tracker for external monitoring.
    pub fn lsn_tracker(&self) -> &Arc<LsnTracker> {
        &self.lsn_tracker
    }

    /// Get the resolved topic name for a given event, respecting topic mapping.
    pub fn resolve_topic(&self, event: &CdcEvent) -> String {
        // Check mapped_topic metadata first (set by apply_topic_mapping)
        if let Some(mapped) = event.metadata.get("mapped_topic") {
            return mapped.clone();
        }
        event.topic_name(self.config.base.topic_prefix.as_deref())
    }

    /// Parse slot change output (from pg_logical_slot_get_changes)
    fn parse_slot_change(&self, lsn: &str, _xid: i32, data: &str) -> Option<Vec<CdcEvent>> {
        let result = match self.config.output_plugin {
            PostgresOutputPlugin::PgOutput => self.parse_pgoutput_text(data, lsn),
            PostgresOutputPlugin::Wal2Json => self.parse_wal2json_text(data, lsn),
        };
        if result.is_none() && !data.is_empty() {
            debug!(
                lsn,
                plugin = %self.config.output_plugin,
                data_len = data.len(),
                "Slot change data could not be parsed into CDC events; skipping"
            );
        }
        result
    }

    /// Parse pgoutput text format from pg_logical_slot_get_changes
    fn parse_pgoutput_text(&self, data: &str, lsn: &str) -> Option<Vec<CdcEvent>> {
        let mut events = Vec::new();

        // pgoutput in text mode outputs: table schema.table: operation: columns
        // Example: "table public.users: INSERT: id[integer]:1 name[text]:'John'"
        if data.starts_with("table ") {
            let parts: Vec<&str> = data.splitn(3, ": ").collect();
            if parts.len() >= 2 {
                let table_part = parts[0].trim_start_matches("table ");
                let (schema, table) = if let Some(dot_pos) = table_part.find('.') {
                    (
                        table_part[..dot_pos].to_string(),
                        table_part[dot_pos + 1..].to_string(),
                    )
                } else {
                    ("public".to_string(), table_part.to_string())
                };

                let operation = match parts[1].to_uppercase().as_str() {
                    "INSERT" => CdcOperation::Insert,
                    "UPDATE" => CdcOperation::Update,
                    "DELETE" => CdcOperation::Delete,
                    _ => return None,
                };

                let mut event = CdcEvent::new(
                    self.config.base.name.clone(),
                    schema,
                    table,
                    operation,
                    lsn.to_string(),
                );

                // Parse columns if present
                if parts.len() > 2 {
                    event.after = Some(self.parse_pgoutput_columns(parts[2]));
                }

                events.push(event);
            }
        } else if data.starts_with("BEGIN") {
            events.push(CdcEvent::new(
                self.config.base.name.clone(),
                String::new(),
                String::new(),
                CdcOperation::Begin,
                lsn.to_string(),
            ));
        } else if data.starts_with("COMMIT") {
            events.push(CdcEvent::new(
                self.config.base.name.clone(),
                String::new(),
                String::new(),
                CdcOperation::Commit,
                lsn.to_string(),
            ));
        } else {
            trace!(
                lsn,
                data_prefix = &data[..data.len().min(80)],
                "Unrecognized pgoutput text format; skipping"
            );
        }

        if events.is_empty() {
            None
        } else {
            Some(events)
        }
    }

    /// Parse pgoutput column format: name[type]:value name2[type2]:value2
    fn parse_pgoutput_columns(&self, data: &str) -> Vec<CdcColumnValue> {
        let mut columns = Vec::new();

        // Simple parsing - in production would be more robust
        for part in data.split_whitespace() {
            if let Some(bracket_pos) = part.find('[') {
                let name = part[..bracket_pos].to_string();
                if let Some(colon_pos) = part.find("]:") {
                    let type_name = part[bracket_pos + 1..colon_pos].to_string();
                    let value_str = &part[colon_pos + 2..];

                    let value = if value_str == "null" {
                        None
                    } else if value_str.starts_with('\'') && value_str.ends_with('\'') {
                        Some(serde_json::Value::String(
                            value_str[1..value_str.len() - 1].to_string(),
                        ))
                    } else if let Ok(n) = value_str.parse::<i64>() {
                        Some(serde_json::Value::Number(n.into()))
                    } else if let Ok(f) = value_str.parse::<f64>() {
                        serde_json::Number::from_f64(f).map(serde_json::Value::Number)
                    } else if value_str == "true" || value_str == "false" {
                        Some(serde_json::Value::Bool(value_str == "true"))
                    } else {
                        Some(serde_json::Value::String(value_str.to_string()))
                    };

                    columns.push(CdcColumnValue {
                        name,
                        data_type: type_name,
                        value,
                    });
                }
            }
        }

        columns
    }

    /// Parse wal2json text output
    fn parse_wal2json_text(&self, data: &str, lsn: &str) -> Option<Vec<CdcEvent>> {
        // wal2json outputs JSON directly
        let json: serde_json::Value = match serde_json::from_str(data) {
            Ok(v) => v,
            Err(e) => {
                warn!(
                    lsn,
                    error = %e,
                    data_len = data.len(),
                    "Failed to parse wal2json output as JSON"
                );
                return None;
            }
        };
        let changes = json.get("change")?.as_array()?;

        let mut events = Vec::new();
        for change in changes {
            let kind = change.get("kind")?.as_str()?;
            let schema = change.get("schema")?.as_str()?.to_string();
            let table = change.get("table")?.as_str()?.to_string();

            let operation = match kind {
                "insert" => CdcOperation::Insert,
                "update" => CdcOperation::Update,
                "delete" => CdcOperation::Delete,
                other => {
                    debug!(
                        lsn,
                        kind = other,
                        "Unrecognized wal2json change kind; skipping entry"
                    );
                    continue;
                }
            };

            let mut event = CdcEvent::new(
                self.config.base.name.clone(),
                schema,
                table,
                operation,
                lsn.to_string(),
            );

            // Parse column values
            if let Some(columns) = change.get("columnvalues").and_then(|v| v.as_array()) {
                let column_names = change
                    .get("columnnames")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|v| v.as_str())
                            .map(String::from)
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();

                let column_types = change
                    .get("columntypes")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|v| v.as_str())
                            .map(String::from)
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();

                let values: Vec<CdcColumnValue> = columns
                    .iter()
                    .enumerate()
                    .map(|(i, v)| CdcColumnValue {
                        name: column_names.get(i).cloned().unwrap_or_default(),
                        data_type: column_types.get(i).cloned().unwrap_or_default(),
                        value: if v.is_null() { None } else { Some(v.clone()) },
                    })
                    .collect();

                event.after = Some(values);
            }

            // Parse old values for update/delete
            if let Some(oldkeys) = change.get("oldkeys") {
                if let Some(values) = oldkeys.get("keyvalues").and_then(|v| v.as_array()) {
                    let key_names = oldkeys
                        .get("keynames")
                        .and_then(|v| v.as_array())
                        .map(|arr| {
                            arr.iter()
                                .filter_map(|v| v.as_str())
                                .map(String::from)
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default();

                    event.primary_key = key_names.clone();

                    let before_values: Vec<CdcColumnValue> = values
                        .iter()
                        .enumerate()
                        .map(|(i, v)| CdcColumnValue {
                            name: key_names.get(i).cloned().unwrap_or_default(),
                            data_type: String::new(),
                            value: if v.is_null() { None } else { Some(v.clone()) },
                        })
                        .collect();

                    event.before = Some(before_values);
                }
            }

            events.push(event);
        }

        if events.is_empty() {
            None
        } else {
            Some(events)
        }
    }

    /// Drop replication slot
    async fn drop_slot(&self, client: &Client) -> Result<()> {
        if self.config.drop_slot_on_stop {
            info!("Dropping replication slot: {}", self.config.slot_name);
            client
                .execute(
                    &format!(
                        "SELECT pg_drop_replication_slot('{}')",
                        self.config.slot_name
                    ),
                    &[],
                )
                .await
                .map_err(|e| {
                    StreamlineError::Config(format!("Failed to drop replication slot: {}", e))
                })?;
        }
        Ok(())
    }
}

/// Reason the replication loop exited cleanly.
enum ShutdownReason {
    /// Explicit shutdown was requested via the oneshot channel.
    Requested,
    /// The event receiver (mpsc) was dropped by the consumer.
    ReceiverDropped,
}

#[async_trait::async_trait]
impl CdcSource for PostgresCdcSource {
    fn name(&self) -> &str {
        &self.config.base.name
    }

    fn source_type(&self) -> &str {
        "postgres"
    }

    fn info(&self) -> CdcSourceInfo {
        let schemas = self.table_schemas.read();
        let tables: Vec<String> = schemas.keys().cloned().collect();

        CdcSourceInfo {
            name: self.config.base.name.clone(),
            source_type: "postgres".to_string(),
            database: self.config.base.name.clone(),
            tables,
            status: *self.status.read(),
            metrics: self.metrics.read().clone(),
            created_at: self.created_at,
        }
    }

    async fn start(&self) -> Result<mpsc::Receiver<CdcEvent>> {
        if self.running.swap(true, Ordering::SeqCst) {
            return Err(StreamlineError::Config(
                "CDC source is already running".to_string(),
            ));
        }

        *self.status.write() = CdcSourceStatus::Initializing;

        // Connect to PostgreSQL
        let client = self.connect().await?;

        // Check replication support
        self.check_replication_support(&client).await?;

        // Ensure replication slot exists
        self.ensure_replication_slot(&client).await?;

        // Ensure publication exists (for pgoutput)
        self.ensure_publication(&client).await?;

        // Load table schemas
        self.load_table_schemas(&client).await?;

        // Create event channel
        let (tx, rx) = mpsc::channel(10000);

        // Perform snapshot if enabled
        self.perform_snapshot(&client, &tx).await?;

        // Create shutdown channel
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        *self.shutdown_tx.write() = Some(shutdown_tx);

        // Start streaming changes
        let self_arc = Arc::new(Self {
            config: self.config.clone(),
            status: RwLock::new(*self.status.read()),
            metrics: RwLock::new(self.metrics.read().clone()),
            running: AtomicBool::new(true),
            created_at: self.created_at,
            table_schemas: RwLock::new(self.table_schemas.read().clone()),
            shutdown_tx: RwLock::new(None),
            lsn_tracker: Arc::clone(&self.lsn_tracker),
            relation_cache: RwLock::new(self.relation_cache.read().clone()),
            consecutive_failures: std::sync::atomic::AtomicU32::new(0),
        });

        let slot_name_for_log = self_arc.config.slot_name.clone();
        tokio::spawn(async move {
            if let Err(e) = self_arc.stream_changes(tx, shutdown_rx).await {
                error!(
                    error = %e,
                    slot = %slot_name_for_log,
                    "PostgreSQL CDC streaming error"
                );
            }
        });

        Ok(rx)
    }

    async fn stop(&self) -> Result<()> {
        if !self.running.swap(false, Ordering::SeqCst) {
            return Ok(());
        }

        // Send shutdown signal
        if let Some(tx) = self.shutdown_tx.write().take() {
            let _ = tx.send(());
        }

        // Connect to drop slot if needed
        if self.config.drop_slot_on_stop {
            if let Ok(client) = self.connect().await {
                let _ = self.drop_slot(&client).await;
            }
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
        debug!("Committing position: {}", position);
        self.metrics.write().last_position = Some(position.to_string());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_postgres_cdc_source_creation() {
        let config = PostgresCdcConfig::default();
        let source = PostgresCdcSource::new(config);
        assert_eq!(source.name(), "cdc-source");
        assert_eq!(source.source_type(), "postgres");
    }

    #[test]
    fn test_cdc_source_status() {
        let config = PostgresCdcConfig::default();
        let source = PostgresCdcSource::new(config);
        assert_eq!(source.status(), CdcSourceStatus::Stopped);
    }

    #[test]
    fn test_cdc_source_info() {
        let config = PostgresCdcConfig::default();
        let source = PostgresCdcSource::new(config);
        let info = source.info();
        assert_eq!(info.source_type, "postgres");
        assert_eq!(info.status, CdcSourceStatus::Stopped);
    }

    // -----------------------------------------------------------------------
    // WAL message type tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_wal_message_type_from_tag() {
        assert_eq!(WalMessageType::from_tag(b'B'), WalMessageType::Begin);
        assert_eq!(WalMessageType::from_tag(b'C'), WalMessageType::Commit);
        assert_eq!(WalMessageType::from_tag(b'R'), WalMessageType::Relation);
        assert_eq!(WalMessageType::from_tag(b'I'), WalMessageType::Insert);
        assert_eq!(WalMessageType::from_tag(b'U'), WalMessageType::Update);
        assert_eq!(WalMessageType::from_tag(b'D'), WalMessageType::Delete);
        assert_eq!(WalMessageType::from_tag(b'X'), WalMessageType::Other(b'X'));
    }

    #[test]
    fn test_wal_message_type_display() {
        assert_eq!(WalMessageType::Begin.to_string(), "BEGIN");
        assert_eq!(WalMessageType::Commit.to_string(), "COMMIT");
        assert_eq!(WalMessageType::Relation.to_string(), "RELATION");
        assert_eq!(WalMessageType::Insert.to_string(), "INSERT");
        assert_eq!(WalMessageType::Update.to_string(), "UPDATE");
        assert_eq!(WalMessageType::Delete.to_string(), "DELETE");
    }

    // -----------------------------------------------------------------------
    // LSN tracker tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_lsn_format_and_parse() {
        let lsn_str = "0/01A3B4C5";
        let parsed = LsnTracker::parse_lsn(lsn_str);
        assert!(parsed.is_some());
        let val = parsed.expect("LSN should parse");
        assert_eq!(LsnTracker::format_lsn(val), lsn_str);
    }

    #[test]
    fn test_lsn_parse_invalid() {
        assert!(LsnTracker::parse_lsn("invalid").is_none());
        assert!(LsnTracker::parse_lsn("").is_none());
        assert!(LsnTracker::parse_lsn("0/ZZZ").is_none());
    }

    #[test]
    fn test_lsn_tracker_advance() {
        let tracker = LsnTracker::new(0);
        tracker.advance_received(100);
        assert_eq!(tracker.received_lsn(), 100);
        tracker.advance_received(50); // should not go backward
        assert_eq!(tracker.received_lsn(), 100);
    }

    #[test]
    fn test_lsn_tracker_flush() {
        let tracker = LsnTracker::new(0);
        tracker.confirm_flush(200);
        assert_eq!(tracker.flushed_lsn(), 200);
        tracker.confirm_flush(100); // should not go backward
        assert_eq!(tracker.flushed_lsn(), 200);
    }

    // -----------------------------------------------------------------------
    // WAL binary message parsing tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_parse_wal_begin_message() {
        // Tag 'B', final_lsn (8), commit_ts (8), xid (4) = 21 bytes
        let mut buf = vec![b'B'];
        buf.extend_from_slice(&100u64.to_be_bytes()); // final_lsn
        buf.extend_from_slice(&200i64.to_be_bytes()); // commit_ts
        buf.extend_from_slice(&42u32.to_be_bytes()); // xid

        let msg = parse_wal_message(&buf);
        assert!(msg.is_some());
        match msg.expect("should parse") {
            WalMessage::Begin {
                final_lsn,
                commit_ts,
                xid,
            } => {
                assert_eq!(final_lsn, 100);
                assert_eq!(commit_ts, 200);
                assert_eq!(xid, 42);
            }
            other => panic!("expected Begin, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_wal_commit_message() {
        let mut buf = vec![b'C'];
        buf.push(0); // flags
        buf.extend_from_slice(&300u64.to_be_bytes()); // commit_lsn
        buf.extend_from_slice(&400u64.to_be_bytes()); // end_lsn
        buf.extend_from_slice(&500i64.to_be_bytes()); // commit_ts

        let msg = parse_wal_message(&buf);
        assert!(msg.is_some());
        match msg.expect("should parse") {
            WalMessage::Commit {
                commit_lsn,
                end_lsn,
                commit_ts,
            } => {
                assert_eq!(commit_lsn, 300);
                assert_eq!(end_lsn, 400);
                assert_eq!(commit_ts, 500);
            }
            other => panic!("expected Commit, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_wal_relation_message() {
        let mut buf = vec![b'R'];
        buf.extend_from_slice(&1u32.to_be_bytes()); // relation_id
        buf.extend_from_slice(b"public\0"); // schema
        buf.extend_from_slice(b"users\0"); // table
        buf.push(b'd'); // replica identity = default
        buf.extend_from_slice(&2u16.to_be_bytes()); // 2 columns

        // Column 1: id (key)
        buf.push(0x01); // flags: is_key
        buf.extend_from_slice(b"id\0");
        buf.extend_from_slice(&23u32.to_be_bytes()); // type_oid (int4)
        buf.extend_from_slice(&(-1i32).to_be_bytes()); // type_modifier

        // Column 2: name (not key)
        buf.push(0x00); // flags: not key
        buf.extend_from_slice(b"name\0");
        buf.extend_from_slice(&25u32.to_be_bytes()); // type_oid (text)
        buf.extend_from_slice(&(-1i32).to_be_bytes()); // type_modifier

        let msg = parse_wal_message(&buf);
        assert!(msg.is_some());
        match msg.expect("should parse") {
            WalMessage::Relation(rel) => {
                assert_eq!(rel.relation_id, 1);
                assert_eq!(rel.schema, "public");
                assert_eq!(rel.table, "users");
                assert_eq!(rel.replica_identity, 'd');
                assert_eq!(rel.columns.len(), 2);
                assert!(rel.columns[0].is_key);
                assert_eq!(rel.columns[0].name, "id");
                assert!(!rel.columns[1].is_key);
                assert_eq!(rel.columns[1].name, "name");
            }
            other => panic!("expected Relation, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_wal_insert_message() {
        let mut buf = vec![b'I'];
        buf.extend_from_slice(&1u32.to_be_bytes()); // relation_id
        buf.push(b'N'); // new tuple tag
        buf.extend_from_slice(&2u16.to_be_bytes()); // 2 columns

        // Column 1: text "42"
        buf.push(b't');
        let val = b"42";
        buf.extend_from_slice(&(val.len() as u32).to_be_bytes());
        buf.extend_from_slice(val);

        // Column 2: null
        buf.push(b'n');

        let msg = parse_wal_message(&buf);
        assert!(msg.is_some());
        match msg.expect("should parse") {
            WalMessage::Insert {
                relation_id,
                new_tuple,
            } => {
                assert_eq!(relation_id, 1);
                assert_eq!(new_tuple.len(), 2);
                match &new_tuple[0] {
                    WalTupleColumn::Text(s) => assert_eq!(s, "42"),
                    other => panic!("expected Text, got {:?}", other),
                }
                assert!(matches!(new_tuple[1], WalTupleColumn::Null));
            }
            other => panic!("expected Insert, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_wal_delete_message() {
        let mut buf = vec![b'D'];
        buf.extend_from_slice(&1u32.to_be_bytes()); // relation_id
        buf.push(b'K'); // key tag
        buf.extend_from_slice(&1u16.to_be_bytes()); // 1 column

        // Column 1: text "7"
        buf.push(b't');
        let val = b"7";
        buf.extend_from_slice(&(val.len() as u32).to_be_bytes());
        buf.extend_from_slice(val);

        let msg = parse_wal_message(&buf);
        assert!(msg.is_some());
        match msg.expect("should parse") {
            WalMessage::Delete {
                relation_id,
                old_tuple,
            } => {
                assert_eq!(relation_id, 1);
                assert_eq!(old_tuple.len(), 1);
            }
            other => panic!("expected Delete, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_empty_data_returns_none() {
        assert!(parse_wal_message(&[]).is_none());
    }

    // -----------------------------------------------------------------------
    // WAL-to-CdcEvent conversion tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_wal_message_to_cdc_events_insert() {
        let config = PostgresCdcConfig::default();

        let rel = RelationInfo {
            relation_id: 1,
            schema: "public".to_string(),
            table: "users".to_string(),
            columns: vec![
                WalColumnInfo {
                    is_key: true,
                    name: "id".to_string(),
                    type_oid: 23,
                    type_modifier: -1,
                },
                WalColumnInfo {
                    is_key: false,
                    name: "name".to_string(),
                    type_oid: 25,
                    type_modifier: -1,
                },
            ],
            replica_identity: 'd',
        };

        let mut relations = HashMap::new();
        relations.insert(1, rel);

        let msg = WalMessage::Insert {
            relation_id: 1,
            new_tuple: vec![
                WalTupleColumn::Text("42".to_string()),
                WalTupleColumn::Text("alice".to_string()),
            ],
        };

        let events = wal_message_to_cdc_events(&msg, &relations, "testdb", &config);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].operation, CdcOperation::Insert);
        assert_eq!(events[0].table, "users");
        assert_eq!(events[0].primary_key, vec!["id"]);

        let after = events[0].after.as_ref().expect("should have after");
        assert_eq!(after.len(), 2);
        assert_eq!(after[0].value, Some(serde_json::json!(42)));
        assert_eq!(after[1].value, Some(serde_json::json!("alice")));
    }

    #[test]
    fn test_wal_message_to_cdc_events_missing_relation() {
        let config = PostgresCdcConfig::default();
        let relations = HashMap::new(); // empty cache

        let msg = WalMessage::Insert {
            relation_id: 999,
            new_tuple: vec![WalTupleColumn::Text("x".to_string())],
        };

        let events = wal_message_to_cdc_events(&msg, &relations, "testdb", &config);
        assert!(events.is_empty());
    }

    #[test]
    fn test_topic_mapping() {
        let mut config = PostgresCdcConfig::default();
        config
            .topic_mapping
            .insert("public.orders".to_string(), "my-orders-topic".to_string());

        let rel = RelationInfo {
            relation_id: 1,
            schema: "public".to_string(),
            table: "orders".to_string(),
            columns: vec![WalColumnInfo {
                is_key: true,
                name: "id".to_string(),
                type_oid: 23,
                type_modifier: -1,
            }],
            replica_identity: 'd',
        };

        let mut relations = HashMap::new();
        relations.insert(1, rel);

        let msg = WalMessage::Insert {
            relation_id: 1,
            new_tuple: vec![WalTupleColumn::Text("1".to_string())],
        };

        let events = wal_message_to_cdc_events(&msg, &relations, "db", &config);
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].metadata.get("mapped_topic"),
            Some(&"my-orders-topic".to_string())
        );
    }

    #[test]
    fn test_text_to_json_value_types() {
        // bool
        assert_eq!(
            text_to_json_value("t", 16),
            Some(serde_json::Value::Bool(true))
        );
        assert_eq!(
            text_to_json_value("f", 16),
            Some(serde_json::Value::Bool(false))
        );

        // int4
        assert_eq!(
            text_to_json_value("42", 23),
            Some(serde_json::Value::from(42))
        );

        // float8
        let f = text_to_json_value("3.14", 701);
        assert!(f.is_some());

        // text
        assert_eq!(
            text_to_json_value("hello", 25),
            Some(serde_json::Value::String("hello".to_string()))
        );

        // jsonb
        let j = text_to_json_value(r#"{"a":1}"#, 3802);
        assert!(j.is_some());
        assert_eq!(j.expect("json"), serde_json::json!({"a": 1}));
    }

    #[test]
    fn test_process_wal_message_caches_relation() {
        let config = PostgresCdcConfig::default();
        let source = PostgresCdcSource::new(config);

        // Build a RELATION message
        let mut buf = vec![b'R'];
        buf.extend_from_slice(&1u32.to_be_bytes());
        buf.extend_from_slice(b"public\0");
        buf.extend_from_slice(b"items\0");
        buf.push(b'd');
        buf.extend_from_slice(&1u16.to_be_bytes());
        buf.push(0x01);
        buf.extend_from_slice(b"id\0");
        buf.extend_from_slice(&23u32.to_be_bytes());
        buf.extend_from_slice(&(-1i32).to_be_bytes());

        let events = source.process_wal_message(&buf);
        assert!(events.is_empty()); // RELATION doesn't produce events

        // Verify the relation was cached
        let cache = source.relation_cache.read();
        assert!(cache.contains_key(&1));
        assert_eq!(cache[&1].table, "items");
    }
}
