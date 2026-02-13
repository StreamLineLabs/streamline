//! Debezium-compatible change event format
//!
//! Provides a `ChangeEvent` struct that matches Debezium's event envelope,
//! enabling drop-in compatibility with existing Debezium consumers.

use super::{CdcEvent, CdcOperation};
use chrono::Utc;
use serde::{Deserialize, Serialize};

/// Debezium-compatible operation codes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Operation {
    /// Read (snapshot)
    #[serde(rename = "r")]
    Read,
    /// Create (insert)
    #[serde(rename = "c")]
    Create,
    /// Update
    #[serde(rename = "u")]
    Update,
    /// Delete
    #[serde(rename = "d")]
    Delete,
}

impl std::fmt::Display for Operation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Operation::Read => write!(f, "r"),
            Operation::Create => write!(f, "c"),
            Operation::Update => write!(f, "u"),
            Operation::Delete => write!(f, "d"),
        }
    }
}

impl From<CdcOperation> for Operation {
    fn from(op: CdcOperation) -> Self {
        match op {
            CdcOperation::Insert => Operation::Create,
            CdcOperation::Update => Operation::Update,
            CdcOperation::Delete => Operation::Delete,
            CdcOperation::Snapshot => Operation::Read,
            _ => Operation::Read,
        }
    }
}

/// Source metadata describing the origin database
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceInfo {
    /// Connector version
    pub version: String,
    /// Connector name (e.g. "streamline")
    pub connector: String,
    /// Logical name of the database server
    pub name: String,
    /// Timestamp in milliseconds when the change was made in the source database
    pub ts_ms: i64,
    /// Whether this is part of a snapshot
    #[serde(default)]
    pub snapshot: Option<String>,
    /// Source database type (e.g. "postgres", "mysql")
    pub db: String,
    /// Database schema (e.g. "public")
    #[serde(default)]
    pub schema: Option<String>,
    /// Table name
    pub table: String,
    /// LSN or binlog position
    #[serde(default)]
    pub lsn: Option<String>,
    /// Transaction ID
    #[serde(default, rename = "txId")]
    pub tx_id: Option<String>,
}

/// Transaction metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionInfo {
    /// Transaction identifier
    pub id: String,
    /// Total order of the event within the transaction
    pub total_order: u64,
    /// Number of events in the collection the transaction belongs to
    pub data_collection_order: u64,
}

/// Debezium-compatible change event envelope
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeEvent {
    /// Row state before the change (for updates and deletes)
    #[serde(default)]
    pub before: Option<serde_json::Value>,
    /// Row state after the change (for inserts and updates)
    #[serde(default)]
    pub after: Option<serde_json::Value>,
    /// Source database metadata
    pub source: SourceInfo,
    /// Operation type: c (create), u (update), d (delete), r (read/snapshot)
    pub op: Operation,
    /// Timestamp in milliseconds at which the connector processed the event
    pub ts_ms: i64,
    /// Transaction metadata (optional)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transaction: Option<TransactionInfo>,
}

impl ChangeEvent {
    /// Create a new ChangeEvent from a CdcEvent
    pub fn from_cdc_event(event: &CdcEvent, server_name: &str) -> Self {
        let before = event.before.as_ref().map(|cols| {
            let map: serde_json::Map<String, serde_json::Value> = cols
                .iter()
                .map(|c| {
                    (
                        c.name.clone(),
                        c.value.clone().unwrap_or(serde_json::Value::Null),
                    )
                })
                .collect();
            serde_json::Value::Object(map)
        });

        let after = event.after.as_ref().map(|cols| {
            let map: serde_json::Map<String, serde_json::Value> = cols
                .iter()
                .map(|c| {
                    (
                        c.name.clone(),
                        c.value.clone().unwrap_or(serde_json::Value::Null),
                    )
                })
                .collect();
            serde_json::Value::Object(map)
        });

        let is_snapshot = event.operation == CdcOperation::Snapshot;

        ChangeEvent {
            before,
            after,
            source: SourceInfo {
                version: env!("CARGO_PKG_VERSION").to_string(),
                connector: "streamline".to_string(),
                name: server_name.to_string(),
                ts_ms: event.timestamp.timestamp_millis(),
                snapshot: if is_snapshot {
                    Some("true".to_string())
                } else {
                    None
                },
                db: event.database.clone(),
                schema: Some(event.schema.clone()),
                table: event.table.clone(),
                lsn: Some(event.position.clone()),
                tx_id: event.transaction_id.clone(),
            },
            op: Operation::from(event.operation),
            ts_ms: Utc::now().timestamp_millis(),
            transaction: event.transaction_id.as_ref().map(|id| TransactionInfo {
                id: id.clone(),
                total_order: 1,
                data_collection_order: 1,
            }),
        }
    }

    /// Serialize to Debezium-compatible JSON bytes
    pub fn to_json_bytes(&self) -> crate::error::Result<bytes::Bytes> {
        let json = serde_json::to_vec(self)?;
        Ok(bytes::Bytes::from(json))
    }

    /// Create the key payload (primary key as JSON object)
    pub fn key_from_cdc_event(
        event: &CdcEvent,
        server_name: &str,
    ) -> Option<serde_json::Value> {
        if event.primary_key.is_empty() {
            return None;
        }

        let values = event.after.as_ref().or(event.before.as_ref())?;
        let mut key_map = serde_json::Map::new();

        for pk_col in &event.primary_key {
            if let Some(col) = values.iter().find(|c| &c.name == pk_col) {
                key_map.insert(
                    pk_col.clone(),
                    col.value.clone().unwrap_or(serde_json::Value::Null),
                );
            }
        }

        if key_map.is_empty() {
            None
        } else {
            let mut envelope = serde_json::Map::new();
            envelope.insert("payload".to_string(), serde_json::Value::Object(key_map));
            envelope.insert(
                "schema".to_string(),
                serde_json::json!({
                    "type": "struct",
                    "name": format!("{}.{}.{}.Key", server_name, event.schema, event.table),
                    "optional": false,
                }),
            );
            Some(serde_json::Value::Object(envelope))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cdc::{CdcColumnValue, CdcEvent, CdcOperation};

    fn make_insert_event() -> CdcEvent {
        let mut event = CdcEvent::new(
            "testdb".to_string(),
            "public".to_string(),
            "users".to_string(),
            CdcOperation::Insert,
            "0/AABBCC".to_string(),
        );
        event.after = Some(vec![
            CdcColumnValue {
                name: "id".to_string(),
                data_type: "int4".to_string(),
                value: Some(serde_json::json!(1)),
            },
            CdcColumnValue {
                name: "name".to_string(),
                data_type: "varchar".to_string(),
                value: Some(serde_json::json!("alice")),
            },
        ]);
        event.primary_key = vec!["id".to_string()];
        event.transaction_id = Some("tx-100".to_string());
        event
    }

    #[test]
    fn test_from_cdc_event_insert() {
        let cdc = make_insert_event();
        let ce = ChangeEvent::from_cdc_event(&cdc, "myserver");

        assert_eq!(ce.op, Operation::Create);
        assert!(ce.before.is_none());
        assert!(ce.after.is_some());
        assert_eq!(ce.source.db, "testdb");
        assert_eq!(ce.source.table, "users");
        assert_eq!(ce.source.connector, "streamline");
        assert!(ce.transaction.is_some());
    }

    #[test]
    fn test_operation_from_cdc_operation() {
        assert_eq!(Operation::from(CdcOperation::Insert), Operation::Create);
        assert_eq!(Operation::from(CdcOperation::Update), Operation::Update);
        assert_eq!(Operation::from(CdcOperation::Delete), Operation::Delete);
        assert_eq!(Operation::from(CdcOperation::Snapshot), Operation::Read);
    }

    #[test]
    fn test_change_event_json_roundtrip() {
        let cdc = make_insert_event();
        let ce = ChangeEvent::from_cdc_event(&cdc, "myserver");
        let bytes = ce.to_json_bytes().unwrap();
        let parsed: ChangeEvent = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed.op, Operation::Create);
        assert_eq!(parsed.source.table, "users");
    }

    #[test]
    fn test_key_from_cdc_event() {
        let cdc = make_insert_event();
        let key = ChangeEvent::key_from_cdc_event(&cdc, "myserver");
        assert!(key.is_some());
        let key = key.unwrap();
        assert!(key.get("payload").is_some());
    }

    #[test]
    fn test_operation_display() {
        assert_eq!(Operation::Create.to_string(), "c");
        assert_eq!(Operation::Update.to_string(), "u");
        assert_eq!(Operation::Delete.to_string(), "d");
        assert_eq!(Operation::Read.to_string(), "r");
    }
}
