//! Schema evolution history tracking
//!
//! Records DDL changes over time so consumers can correlate each change event
//! with the exact table schema that was in effect when the change occurred.

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::info;

/// The kind of DDL change
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DdlType {
    CreateTable,
    AlterTable,
    DropTable,
    RenameTable,
    AddColumn,
    DropColumn,
    AlterColumn,
    AddIndex,
    DropIndex,
}

impl std::fmt::Display for DdlType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DdlType::CreateTable => write!(f, "CREATE_TABLE"),
            DdlType::AlterTable => write!(f, "ALTER_TABLE"),
            DdlType::DropTable => write!(f, "DROP_TABLE"),
            DdlType::RenameTable => write!(f, "RENAME_TABLE"),
            DdlType::AddColumn => write!(f, "ADD_COLUMN"),
            DdlType::DropColumn => write!(f, "DROP_COLUMN"),
            DdlType::AlterColumn => write!(f, "ALTER_COLUMN"),
            DdlType::AddIndex => write!(f, "ADD_INDEX"),
            DdlType::DropIndex => write!(f, "DROP_INDEX"),
        }
    }
}

/// A single recorded schema change
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaChange {
    /// Unique id
    pub id: u64,
    /// The DDL type
    pub ddl_type: DdlType,
    /// Fully-qualified table (database.schema.table)
    pub table: String,
    /// The DDL statement that was executed
    pub ddl: String,
    /// WAL position / binlog offset when the DDL was observed
    pub position: String,
    /// Database-provided timestamp of the change
    pub source_ts: DateTime<Utc>,
    /// When Streamline recorded it
    pub recorded_at: DateTime<Utc>,
}

/// Column definition stored in a versioned schema
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub default_value: Option<String>,
    pub ordinal: u32,
}

/// A versioned snapshot of a table's schema
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaVersionRecord {
    /// Monotonically increasing version number for this table
    pub version: u32,
    /// The table this schema describes
    pub table: String,
    /// Column definitions at this version
    pub columns: Vec<ColumnDef>,
    /// Primary key columns
    pub primary_key: Vec<String>,
    /// The schema change that produced this version (None for initial capture)
    pub change_id: Option<u64>,
    /// When this version was recorded
    pub recorded_at: DateTime<Utc>,
}

/// Persistent store for schema DDL history
pub struct SchemaHistoryStore {
    /// Per-table ordered list of schema changes
    changes: RwLock<Vec<SchemaChange>>,
    /// Per-table current + historical schema versions
    versions: RwLock<HashMap<String, Vec<SchemaVersionRecord>>>,
    /// Auto-incrementing change id
    next_id: RwLock<u64>,
}

impl SchemaHistoryStore {
    /// Create an empty history store
    pub fn new() -> Self {
        Self {
            changes: RwLock::new(Vec::new()),
            versions: RwLock::new(HashMap::new()),
            next_id: RwLock::new(1),
        }
    }

    /// Record a DDL change and, optionally, the resulting schema version
    pub fn record_change(
        &self,
        ddl_type: DdlType,
        table: &str,
        ddl: &str,
        position: &str,
        source_ts: DateTime<Utc>,
        new_columns: Option<(Vec<ColumnDef>, Vec<String>)>,
    ) -> SchemaChange {
        let id = {
            let mut next = self.next_id.write();
            let id = *next;
            *next += 1;
            id
        };

        let change = SchemaChange {
            id,
            ddl_type,
            table: table.to_string(),
            ddl: ddl.to_string(),
            position: position.to_string(),
            source_ts,
            recorded_at: Utc::now(),
        };

        self.changes.write().push(change.clone());

        // If new column definitions were provided, store a new schema version
        if let Some((columns, primary_key)) = new_columns {
            let mut versions = self.versions.write();
            let table_versions = versions.entry(table.to_string()).or_default();
            let version_num = table_versions.len() as u32 + 1;
            table_versions.push(SchemaVersionRecord {
                version: version_num,
                table: table.to_string(),
                columns,
                primary_key,
                change_id: Some(id),
                recorded_at: Utc::now(),
            });
        }

        info!(id, table, ddl_type = %change.ddl_type, "Schema change recorded");
        change
    }

    /// Get the schema that was in effect at a given position/timestamp for a table.
    /// Returns the latest version whose recorded_at <= ts.
    pub fn get_schema_at(
        &self,
        table: &str,
        ts: DateTime<Utc>,
    ) -> Option<SchemaVersionRecord> {
        let versions = self.versions.read();
        versions.get(table).and_then(|v| {
            v.iter()
                .rev()
                .find(|sv| sv.recorded_at <= ts)
                .cloned()
        })
    }

    /// Get the current (latest) schema for a table
    pub fn get_current_schema(&self, table: &str) -> Option<SchemaVersionRecord> {
        let versions = self.versions.read();
        versions.get(table).and_then(|v| v.last().cloned())
    }

    /// List all changes, optionally filtered to a single table
    pub fn list_changes(&self, table: Option<&str>) -> Vec<SchemaChange> {
        let changes = self.changes.read();
        match table {
            Some(t) => changes.iter().filter(|c| c.table == t).cloned().collect(),
            None => changes.clone(),
        }
    }

    /// List all schema versions for a table
    pub fn list_versions(&self, table: &str) -> Vec<SchemaVersionRecord> {
        let versions = self.versions.read();
        versions.get(table).cloned().unwrap_or_default()
    }

    /// Total number of recorded changes
    pub fn change_count(&self) -> usize {
        self.changes.read().len()
    }
}

impl Default for SchemaHistoryStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_and_list_changes() {
        let store = SchemaHistoryStore::new();
        store.record_change(
            DdlType::CreateTable,
            "public.users",
            "CREATE TABLE users (id INT PRIMARY KEY)",
            "0/AAA",
            Utc::now(),
            Some((
                vec![ColumnDef {
                    name: "id".to_string(),
                    data_type: "int".to_string(),
                    nullable: false,
                    default_value: None,
                    ordinal: 0,
                }],
                vec!["id".to_string()],
            )),
        );

        store.record_change(
            DdlType::AddColumn,
            "public.users",
            "ALTER TABLE users ADD COLUMN name VARCHAR(255)",
            "0/BBB",
            Utc::now(),
            Some((
                vec![
                    ColumnDef {
                        name: "id".to_string(),
                        data_type: "int".to_string(),
                        nullable: false,
                        default_value: None,
                        ordinal: 0,
                    },
                    ColumnDef {
                        name: "name".to_string(),
                        data_type: "varchar".to_string(),
                        nullable: true,
                        default_value: None,
                        ordinal: 1,
                    },
                ],
                vec!["id".to_string()],
            )),
        );

        let all = store.list_changes(None);
        assert_eq!(all.len(), 2);

        let filtered = store.list_changes(Some("public.users"));
        assert_eq!(filtered.len(), 2);

        let empty = store.list_changes(Some("public.orders"));
        assert!(empty.is_empty());
    }

    #[test]
    fn test_get_current_schema() {
        let store = SchemaHistoryStore::new();
        store.record_change(
            DdlType::CreateTable,
            "public.users",
            "CREATE TABLE users (id INT)",
            "0/100",
            Utc::now(),
            Some((
                vec![ColumnDef {
                    name: "id".to_string(),
                    data_type: "int".to_string(),
                    nullable: false,
                    default_value: None,
                    ordinal: 0,
                }],
                vec!["id".to_string()],
            )),
        );

        let current = store.get_current_schema("public.users").unwrap();
        assert_eq!(current.version, 1);
        assert_eq!(current.columns.len(), 1);
    }

    #[test]
    fn test_schema_versions() {
        let store = SchemaHistoryStore::new();

        store.record_change(
            DdlType::CreateTable,
            "t",
            "CREATE TABLE t (a INT)",
            "0/1",
            Utc::now(),
            Some((
                vec![ColumnDef {
                    name: "a".to_string(),
                    data_type: "int".to_string(),
                    nullable: false,
                    default_value: None,
                    ordinal: 0,
                }],
                vec!["a".to_string()],
            )),
        );
        store.record_change(
            DdlType::AddColumn,
            "t",
            "ALTER TABLE t ADD b TEXT",
            "0/2",
            Utc::now(),
            Some((
                vec![
                    ColumnDef {
                        name: "a".to_string(),
                        data_type: "int".to_string(),
                        nullable: false,
                        default_value: None,
                        ordinal: 0,
                    },
                    ColumnDef {
                        name: "b".to_string(),
                        data_type: "text".to_string(),
                        nullable: true,
                        default_value: None,
                        ordinal: 1,
                    },
                ],
                vec!["a".to_string()],
            )),
        );

        let versions = store.list_versions("t");
        assert_eq!(versions.len(), 2);
        assert_eq!(versions[0].columns.len(), 1);
        assert_eq!(versions[1].columns.len(), 2);
    }

    #[test]
    fn test_ddl_type_display() {
        assert_eq!(DdlType::CreateTable.to_string(), "CREATE_TABLE");
        assert_eq!(DdlType::AlterColumn.to_string(), "ALTER_COLUMN");
    }

    #[test]
    fn test_change_count() {
        let store = SchemaHistoryStore::new();
        assert_eq!(store.change_count(), 0);
        store.record_change(
            DdlType::CreateTable,
            "t",
            "CREATE TABLE t (id INT)",
            "0/1",
            Utc::now(),
            None,
        );
        assert_eq!(store.change_count(), 1);
    }
}
