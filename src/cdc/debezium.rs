//! Debezium-compatible envelope format
//!
//! Wraps CDC events in the standard Debezium JSON envelope so that existing
//! Debezium consumers (Kafka Connect sinks, Flink CDC, etc.) can consume
//! Streamline CDC output without modification.
//!
//! The envelope follows the Debezium convention:
//! ```json
//! {
//!   "schema": { "type": "struct", "fields": [...], "optional": false, "name": "..." },
//!   "payload": { "before": ..., "after": ..., "source": ..., "op": "c", "ts_ms": ... }
//! }
//! ```

use super::change_event::{ChangeEvent, Operation, SourceInfo};
use super::schema::{ColumnSchema, TableSchema};
use super::{CdcEvent, CdcOperation};
use crate::error::Result;
use chrono::Utc;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Debezium schema types
// ---------------------------------------------------------------------------

/// A single field inside a Debezium schema definition.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DebeziumField {
    /// Debezium type name (e.g. `"int32"`, `"string"`, `"struct"`)
    #[serde(rename = "type")]
    pub field_type: String,
    /// Whether the field is optional
    pub optional: bool,
    /// Field name
    pub field: String,
    /// Logical Debezium name (e.g. `"io.debezium.time.Timestamp"`)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Version of the logical type
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<i32>,
    /// Nested fields (only for struct types)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<DebeziumField>>,
}

/// Top-level Debezium schema block that accompanies a payload.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DebeziumSchema {
    /// Always `"struct"` at the envelope level
    #[serde(rename = "type")]
    pub schema_type: String,
    /// Envelope fields (`before`, `after`, `source`, `op`, `ts_ms`, `transaction`)
    pub fields: Vec<DebeziumField>,
    /// Always `false` at the envelope level
    pub optional: bool,
    /// Fully-qualified envelope name
    /// e.g. `"streamline.public.users.Envelope"`
    pub name: String,
    /// Schema version
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<i32>,
}

// ---------------------------------------------------------------------------
// Debezium envelope
// ---------------------------------------------------------------------------

/// Full Debezium-compatible message envelope (`schema` + `payload`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DebeziumEnvelope {
    /// Schema describing the payload structure
    pub schema: DebeziumSchema,
    /// The actual change event payload
    pub payload: ChangeEvent,
}

impl DebeziumEnvelope {
    /// Build a `DebeziumEnvelope` from a raw `CdcEvent`.
    ///
    /// * `event` – The internal CDC event.
    /// * `server_name` – Logical server name emitted in the source block.
    /// * `table_schema` – Optional column metadata used to generate the
    ///   Debezium schema block. When `None`, a minimal schema is produced.
    pub fn from_cdc_event(
        event: &CdcEvent,
        server_name: &str,
        table_schema: Option<&TableSchema>,
    ) -> Self {
        let payload = ChangeEvent::from_cdc_event(event, server_name);
        let schema = build_envelope_schema(event, server_name, table_schema);
        Self { schema, payload }
    }

    /// Serialize the envelope to Debezium-compatible JSON bytes.
    pub fn to_json_bytes(&self) -> Result<bytes::Bytes> {
        let json = serde_json::to_vec(self)?;
        Ok(bytes::Bytes::from(json))
    }

    /// Build the key envelope (schema + payload) for the record key.
    pub fn key_envelope(event: &CdcEvent, server_name: &str) -> Option<serde_json::Value> {
        ChangeEvent::key_from_cdc_event(event, server_name)
    }
}

// ---------------------------------------------------------------------------
// Schema generation helpers
// ---------------------------------------------------------------------------

/// Build the top-level Debezium envelope schema.
fn build_envelope_schema(
    event: &CdcEvent,
    server_name: &str,
    table_schema: Option<&TableSchema>,
) -> DebeziumSchema {
    let envelope_name = format!("{}.{}.{}.Envelope", server_name, event.schema, event.table);
    let value_schema_name = format!("{}.{}.{}.Value", server_name, event.schema, event.table);

    let value_fields = match table_schema {
        Some(ts) => columns_to_debezium_fields(&ts.columns),
        None => infer_fields_from_event(event),
    };

    let value_schema = DebeziumField {
        field_type: "struct".to_string(),
        optional: true,
        field: String::new(), // placeholder, overridden per-use below
        name: Some(value_schema_name),
        version: Some(1),
        fields: Some(value_fields),
    };

    let before_field = DebeziumField {
        field: "before".to_string(),
        ..value_schema.clone()
    };
    let after_field = DebeziumField {
        field: "after".to_string(),
        ..value_schema
    };

    let source_field = build_source_schema_field();
    let op_field = DebeziumField {
        field_type: "string".to_string(),
        optional: false,
        field: "op".to_string(),
        name: None,
        version: None,
        fields: None,
    };
    let ts_ms_field = DebeziumField {
        field_type: "int64".to_string(),
        optional: false,
        field: "ts_ms".to_string(),
        name: None,
        version: None,
        fields: None,
    };
    let transaction_field = build_transaction_schema_field();

    DebeziumSchema {
        schema_type: "struct".to_string(),
        fields: vec![
            before_field,
            after_field,
            source_field,
            op_field,
            ts_ms_field,
            transaction_field,
        ],
        optional: false,
        name: envelope_name,
        version: Some(1),
    }
}

/// Build the `source` struct schema field (matches Debezium's source info block).
fn build_source_schema_field() -> DebeziumField {
    let source_fields = vec![
        simple_field("version", "string", false),
        simple_field("connector", "string", false),
        simple_field("name", "string", false),
        simple_field("ts_ms", "int64", false),
        simple_field("snapshot", "string", true),
        simple_field("db", "string", false),
        simple_field("schema", "string", true),
        simple_field("table", "string", false),
        simple_field("lsn", "string", true),
        simple_field("txId", "string", true),
    ];

    DebeziumField {
        field_type: "struct".to_string(),
        optional: false,
        field: "source".to_string(),
        name: Some("io.debezium.connector.streamline.Source".to_string()),
        version: Some(1),
        fields: Some(source_fields),
    }
}

/// Build the `transaction` struct schema field.
fn build_transaction_schema_field() -> DebeziumField {
    let tx_fields = vec![
        simple_field("id", "string", false),
        simple_field("total_order", "int64", false),
        simple_field("data_collection_order", "int64", false),
    ];

    DebeziumField {
        field_type: "struct".to_string(),
        optional: true,
        field: "transaction".to_string(),
        name: Some("event.block".to_string()),
        version: Some(1),
        fields: Some(tx_fields),
    }
}

/// Convenience helper for a simple scalar field.
fn simple_field(name: &str, field_type: &str, optional: bool) -> DebeziumField {
    DebeziumField {
        field_type: field_type.to_string(),
        optional,
        field: name.to_string(),
        name: None,
        version: None,
        fields: None,
    }
}

// ---------------------------------------------------------------------------
// Column → Debezium field mapping
// ---------------------------------------------------------------------------

/// Convert a slice of `ColumnSchema` into Debezium struct fields.
pub fn columns_to_debezium_fields(columns: &[ColumnSchema]) -> Vec<DebeziumField> {
    columns.iter().map(column_to_debezium_field).collect()
}

/// Map a single `ColumnSchema` to a `DebeziumField`, resolving the
/// database-native type to the closest Debezium type.
fn column_to_debezium_field(col: &ColumnSchema) -> DebeziumField {
    let (dbz_type, logical_name) = map_db_type_to_debezium(&col.data_type);

    DebeziumField {
        field_type: dbz_type,
        optional: col.nullable,
        field: col.name.clone(),
        name: logical_name,
        version: None,
        fields: None,
    }
}

/// Map a PostgreSQL / MySQL column type string to the Debezium schema type
/// and an optional logical type name.
///
/// Returns `(debezium_type, optional_logical_name)`.
pub fn map_db_type_to_debezium(db_type: &str) -> (String, Option<String>) {
    let lower = db_type.to_lowercase();

    // Check full type string first for special cases like tinyint(1)
    let trimmed = lower.trim();
    if trimmed == "tinyint(1)" {
        return ("boolean".into(), None);
    }

    let base = lower.split('(').next().unwrap_or(&lower).trim();

    match base {
        // -- Boolean ----------------------------------------------------------
        "bool" | "boolean" => ("boolean".into(), None),

        // -- Integer types ----------------------------------------------------
        "int2" | "smallint" | "smallserial" => ("int16".into(), None),
        "int" | "int4" | "integer" | "mediumint" | "serial" => ("int32".into(), None),
        "int8" | "bigint" | "bigserial" => ("int64".into(), None),
        "tinyint" => ("int16".into(), None),

        // -- Floating point ---------------------------------------------------
        "float4" | "real" | "float" => ("float".into(), None),
        "float8" | "double precision" | "double" => ("double".into(), None),

        // -- Exact numeric ----------------------------------------------------
        "numeric" | "decimal" | "money" => (
            "bytes".into(),
            Some("org.apache.kafka.connect.data.Decimal".into()),
        ),

        // -- String types -----------------------------------------------------
        "text" | "varchar" | "character varying" | "char" | "character" | "bpchar" | "citext"
        | "name" | "tinytext" | "mediumtext" | "longtext" | "enum" | "set" => {
            ("string".into(), None)
        }

        // -- Binary -----------------------------------------------------------
        "bytea" | "blob" | "tinyblob" | "mediumblob" | "longblob" | "binary" | "varbinary"
        | "bit" | "varbit" => ("bytes".into(), None),

        // -- Date / Time ------------------------------------------------------
        "date" => ("int32".into(), Some("io.debezium.time.Date".into())),
        "time" | "time without time zone" => {
            ("int64".into(), Some("io.debezium.time.MicroTime".into()))
        }
        "timetz" | "time with time zone" => (
            "string".into(),
            Some("io.debezium.time.ZonedTime".into()),
        ),
        "timestamp" | "timestamp without time zone" | "datetime" => (
            "int64".into(),
            Some("io.debezium.time.MicroTimestamp".into()),
        ),
        "timestamptz" | "timestamp with time zone" => (
            "string".into(),
            Some("io.debezium.time.ZonedTimestamp".into()),
        ),
        "interval" => (
            "int64".into(),
            Some("io.debezium.time.MicroDuration".into()),
        ),
        "year" => ("int32".into(), None),

        // -- UUID -------------------------------------------------------------
        "uuid" => (
            "string".into(),
            Some("io.debezium.data.Uuid".into()),
        ),

        // -- JSON / JSONB -----------------------------------------------------
        "json" | "jsonb" => (
            "string".into(),
            Some("io.debezium.data.Json".into()),
        ),

        // -- XML --------------------------------------------------------------
        "xml" => (
            "string".into(),
            Some("io.debezium.data.Xml".into()),
        ),

        // -- Arrays (PostgreSQL) ----------------------------------------------
        _ if base.starts_with('_') || lower.contains("[]") => ("string".into(), None),

        // -- Geometry / PostGIS -----------------------------------------------
        "geometry" | "geography" | "point" | "line" | "polygon" | "path" | "circle" | "box"
        | "lseg" => ("string".into(), None),

        // -- Network types (PostgreSQL) ---------------------------------------
        "inet" | "cidr" | "macaddr" | "macaddr8" => ("string".into(), None),

        // -- PostgreSQL-specific ----------------------------------------------
        "oid" => ("int64".into(), None),
        "hstore" => ("string".into(), None),
        "tsquery" | "tsvector" => ("string".into(), None),
        "pg_lsn" => ("string".into(), None),

        // -- Fallback ---------------------------------------------------------
        _ => ("string".into(), None),
    }
}

/// When no `TableSchema` is available, infer minimal Debezium fields from the
/// column values present in the CDC event itself.
fn infer_fields_from_event(event: &CdcEvent) -> Vec<DebeziumField> {
    let cols = event
        .after
        .as_ref()
        .or(event.before.as_ref());

    match cols {
        Some(columns) => columns
            .iter()
            .map(|c| {
                let (dbz_type, logical_name) = map_db_type_to_debezium(&c.data_type);
                DebeziumField {
                    field_type: dbz_type,
                    optional: c.value.is_none(),
                    field: c.name.clone(),
                    name: logical_name,
                    version: None,
                    fields: None,
                }
            })
            .collect(),
        None => Vec::new(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cdc::{CdcColumnValue, CdcEvent, CdcOperation};

    fn sample_event() -> CdcEvent {
        let mut event = CdcEvent::new(
            "testdb".into(),
            "public".into(),
            "users".into(),
            CdcOperation::Insert,
            "0/AABB00".into(),
        );
        event.after = Some(vec![
            CdcColumnValue {
                name: "id".into(),
                data_type: "int4".into(),
                value: Some(serde_json::json!(42)),
            },
            CdcColumnValue {
                name: "name".into(),
                data_type: "varchar".into(),
                value: Some(serde_json::json!("alice")),
            },
            CdcColumnValue {
                name: "email".into(),
                data_type: "text".into(),
                value: Some(serde_json::json!("alice@example.com")),
            },
            CdcColumnValue {
                name: "created_at".into(),
                data_type: "timestamptz".into(),
                value: Some(serde_json::json!("2024-01-15T10:30:00Z")),
            },
        ]);
        event.primary_key = vec!["id".into()];
        event.transaction_id = Some("tx-500".into());
        event
    }

    fn sample_table_schema() -> TableSchema {
        TableSchema {
            database: "testdb".into(),
            schema: "public".into(),
            table: "users".into(),
            columns: vec![
                ColumnSchema {
                    name: "id".into(),
                    data_type: "int4".into(),
                    nullable: false,
                    default_value: None,
                    position: 0,
                },
                ColumnSchema {
                    name: "name".into(),
                    data_type: "varchar".into(),
                    nullable: false,
                    default_value: None,
                    position: 1,
                },
                ColumnSchema {
                    name: "email".into(),
                    data_type: "text".into(),
                    nullable: true,
                    default_value: None,
                    position: 2,
                },
                ColumnSchema {
                    name: "created_at".into(),
                    data_type: "timestamptz".into(),
                    nullable: false,
                    default_value: None,
                    position: 3,
                },
            ],
            primary_key: vec!["id".into()],
            created_at: Utc::now(),
        }
    }

    #[test]
    fn test_envelope_from_cdc_event_with_schema() {
        let event = sample_event();
        let ts = sample_table_schema();
        let envelope = DebeziumEnvelope::from_cdc_event(&event, "myserver", Some(&ts));

        assert_eq!(
            envelope.schema.name,
            "myserver.public.users.Envelope"
        );
        assert_eq!(envelope.schema.schema_type, "struct");
        assert!(!envelope.schema.optional);
        assert_eq!(envelope.schema.fields.len(), 6); // before, after, source, op, ts_ms, transaction

        // Payload checks
        assert_eq!(envelope.payload.op, Operation::Create);
        assert!(envelope.payload.before.is_none());
        assert!(envelope.payload.after.is_some());
        assert_eq!(envelope.payload.source.db, "testdb");
        assert_eq!(envelope.payload.source.connector, "streamline");
    }

    #[test]
    fn test_envelope_from_cdc_event_without_schema() {
        let event = sample_event();
        let envelope = DebeziumEnvelope::from_cdc_event(&event, "myserver", None);

        // Should still produce a schema by inferring from event columns
        let before_field = &envelope.schema.fields[0];
        assert_eq!(before_field.field, "before");
        let fields = before_field.fields.as_ref().unwrap();
        assert_eq!(fields.len(), 4);
    }

    #[test]
    fn test_envelope_json_roundtrip() {
        let event = sample_event();
        let ts = sample_table_schema();
        let envelope = DebeziumEnvelope::from_cdc_event(&event, "srv", Some(&ts));

        let bytes = envelope.to_json_bytes().unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

        assert!(parsed.get("schema").is_some());
        assert!(parsed.get("payload").is_some());

        let payload = parsed.get("payload").unwrap();
        assert_eq!(payload.get("op").unwrap().as_str().unwrap(), "c");
        assert!(payload.get("source").is_some());
        assert!(payload.get("ts_ms").is_some());
    }

    #[test]
    fn test_debezium_schema_field_types() {
        let ts = sample_table_schema();
        let fields = columns_to_debezium_fields(&ts.columns);

        assert_eq!(fields[0].field, "id");
        assert_eq!(fields[0].field_type, "int32");
        assert!(!fields[0].optional);

        assert_eq!(fields[1].field, "name");
        assert_eq!(fields[1].field_type, "string");
        assert!(!fields[1].optional);

        assert_eq!(fields[2].field, "email");
        assert_eq!(fields[2].field_type, "string");
        assert!(fields[2].optional);

        assert_eq!(fields[3].field, "created_at");
        assert_eq!(fields[3].field_type, "string");
        assert_eq!(
            fields[3].name.as_deref(),
            Some("io.debezium.time.ZonedTimestamp")
        );
    }

    #[test]
    fn test_map_postgres_types() {
        assert_eq!(map_db_type_to_debezium("bool").0, "boolean");
        assert_eq!(map_db_type_to_debezium("int2").0, "int16");
        assert_eq!(map_db_type_to_debezium("int4").0, "int32");
        assert_eq!(map_db_type_to_debezium("int8").0, "int64");
        assert_eq!(map_db_type_to_debezium("float4").0, "float");
        assert_eq!(map_db_type_to_debezium("float8").0, "double");
        assert_eq!(map_db_type_to_debezium("numeric").0, "bytes");
        assert_eq!(map_db_type_to_debezium("text").0, "string");
        assert_eq!(map_db_type_to_debezium("bytea").0, "bytes");
        assert_eq!(map_db_type_to_debezium("uuid").0, "string");
        assert_eq!(
            map_db_type_to_debezium("uuid").1.as_deref(),
            Some("io.debezium.data.Uuid")
        );
        assert_eq!(map_db_type_to_debezium("jsonb").0, "string");
        assert_eq!(
            map_db_type_to_debezium("jsonb").1.as_deref(),
            Some("io.debezium.data.Json")
        );
        assert_eq!(map_db_type_to_debezium("date").0, "int32");
        assert_eq!(
            map_db_type_to_debezium("date").1.as_deref(),
            Some("io.debezium.time.Date")
        );
    }

    #[test]
    fn test_map_mysql_types() {
        assert_eq!(map_db_type_to_debezium("tinyint(1)").0, "boolean");
        assert_eq!(map_db_type_to_debezium("tinyint").0, "int16");
        assert_eq!(map_db_type_to_debezium("mediumint").0, "int32");
        assert_eq!(map_db_type_to_debezium("bigint").0, "int64");
        assert_eq!(map_db_type_to_debezium("double").0, "double");
        assert_eq!(map_db_type_to_debezium("datetime").0, "int64");
        assert_eq!(map_db_type_to_debezium("enum").0, "string");
        assert_eq!(map_db_type_to_debezium("blob").0, "bytes");
        assert_eq!(map_db_type_to_debezium("year").0, "int32");
    }

    #[test]
    fn test_envelope_source_metadata() {
        let mut event = sample_event();
        event.operation = CdcOperation::Snapshot;
        let envelope = DebeziumEnvelope::from_cdc_event(&event, "srv", None);

        let src = &envelope.payload.source;
        assert_eq!(src.connector, "streamline");
        assert_eq!(src.db, "testdb");
        assert_eq!(src.schema.as_deref(), Some("public"));
        assert_eq!(src.table, "users");
        assert_eq!(src.lsn.as_deref(), Some("0/AABB00"));
        assert_eq!(src.tx_id.as_deref(), Some("tx-500"));
        assert_eq!(src.snapshot.as_deref(), Some("true"));
        assert_eq!(envelope.payload.op, Operation::Read);
    }

    #[test]
    fn test_envelope_delete_event() {
        let mut event = CdcEvent::new(
            "testdb".into(),
            "public".into(),
            "users".into(),
            CdcOperation::Delete,
            "0/CC00".into(),
        );
        event.before = Some(vec![CdcColumnValue {
            name: "id".into(),
            data_type: "int4".into(),
            value: Some(serde_json::json!(99)),
        }]);
        event.primary_key = vec!["id".into()];

        let envelope = DebeziumEnvelope::from_cdc_event(&event, "srv", None);
        assert_eq!(envelope.payload.op, Operation::Delete);
        assert!(envelope.payload.before.is_some());
        assert!(envelope.payload.after.is_none());
    }

    #[test]
    fn test_envelope_update_event() {
        let mut event = CdcEvent::new(
            "testdb".into(),
            "public".into(),
            "users".into(),
            CdcOperation::Update,
            "0/DD00".into(),
        );
        event.before = Some(vec![CdcColumnValue {
            name: "name".into(),
            data_type: "varchar".into(),
            value: Some(serde_json::json!("old_name")),
        }]);
        event.after = Some(vec![CdcColumnValue {
            name: "name".into(),
            data_type: "varchar".into(),
            value: Some(serde_json::json!("new_name")),
        }]);

        let envelope = DebeziumEnvelope::from_cdc_event(&event, "srv", None);
        assert_eq!(envelope.payload.op, Operation::Update);
        assert!(envelope.payload.before.is_some());
        assert!(envelope.payload.after.is_some());
    }

    #[test]
    fn test_key_envelope() {
        let event = sample_event();
        let key = DebeziumEnvelope::key_envelope(&event, "myserver");
        assert!(key.is_some());
        let key = key.unwrap();
        assert!(key.get("schema").is_some());
        assert!(key.get("payload").is_some());
    }

    #[test]
    fn test_fallback_type_mapping() {
        // Unknown types should fall back to string
        assert_eq!(map_db_type_to_debezium("custom_type").0, "string");
        assert!(map_db_type_to_debezium("custom_type").1.is_none());
    }

    #[test]
    fn test_array_type_mapping() {
        // PostgreSQL array types
        assert_eq!(map_db_type_to_debezium("_int4").0, "string");
        assert_eq!(map_db_type_to_debezium("integer[]").0, "string");
    }
}
