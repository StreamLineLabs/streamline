//! SQLite query engine implementation
//!
//! The `SQLiteQueryEngine` creates an in-memory SQLite database and populates
//! regular tables from Streamline topic data on demand. Queries are executed
//! using standard SQLite SQL, giving users access to `json_extract`, `datetime`,
//! aggregations, joins, and every other SQLite built-in.

use crate::storage::TopicManager;
use base64::Engine;
use parking_lot::Mutex;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, info, warn};

use super::virtual_table::TopicTableMapper;

/// Maximum number of rows returned by a single query when no LIMIT is present.
const DEFAULT_MAX_ROWS: usize = 10_000;

/// Default query timeout in milliseconds.
const DEFAULT_TIMEOUT_MS: u64 = 30_000;

/// Result of a SQL query execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    /// Column names in result order.
    pub columns: Vec<String>,
    /// Row data -- each row is a vector of nullable string values.
    pub rows: Vec<QueryResultRow>,
    /// Total number of rows returned.
    pub row_count: usize,
    /// Wall-clock execution time in milliseconds.
    pub execution_ms: u64,
}

/// A single result row represented as a vector of nullable string cells.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResultRow {
    pub values: Vec<Option<String>>,
}

/// Information about a table available for querying.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableInfo {
    /// Table name (matches the Streamline topic name).
    pub name: String,
    /// Column definitions.
    pub columns: Vec<ColumnInfo>,
    /// Number of rows currently synced into the table.
    pub row_count: usize,
}

/// Column metadata for a virtual table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
}

/// The SQLite query engine.
///
/// Internally holds an in-memory SQLite connection and lazily syncs topic data
/// into regular tables. All access is serialized through a `Mutex` because
/// `rusqlite::Connection` is not `Sync`.
pub struct SQLiteQueryEngine {
    /// The SQLite connection (in-memory).
    conn: Mutex<Connection>,
    /// Reference to the Streamline topic manager for reading data.
    topic_manager: Arc<TopicManager>,
    /// Tracks which topics have been synced and at what offset per partition.
    sync_state: Mutex<HashMap<String, HashMap<i32, i64>>>,
    /// Configurable maximum rows per query result (for memory safety).
    max_rows: usize,
}

impl SQLiteQueryEngine {
    /// Create a new SQLite query engine backed by the given topic manager.
    pub fn new(topic_manager: Arc<TopicManager>) -> crate::Result<Self> {
        Self::with_max_rows(topic_manager, DEFAULT_MAX_ROWS)
    }

    /// Create a new SQLite query engine with a custom row limit.
    pub fn with_max_rows(
        topic_manager: Arc<TopicManager>,
        max_rows: usize,
    ) -> crate::Result<Self> {
        let conn = Connection::open_in_memory().map_err(|e| {
            crate::error::StreamlineError::Storage(format!(
                "Failed to open in-memory SQLite database: {}",
                e
            ))
        })?;

        // Enable WAL mode for better concurrent read performance (even in-memory).
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")
            .ok(); // Best-effort; not critical for in-memory

        // Enable JSON1 extension (bundled with rusqlite's bundled feature).
        // json_extract, json_array, json_object are available by default.

        info!("SQLite query engine initialized (in-memory)");

        Ok(Self {
            conn: Mutex::new(conn),
            topic_manager,
            sync_state: Mutex::new(HashMap::new()),
            max_rows,
        })
    }

    /// Execute a SQL query and return the results.
    ///
    /// Before execution, any topics referenced in the query that have not yet
    /// been synced will be synced automatically. The query is subject to a
    /// configurable timeout.
    pub fn execute_query(&self, sql: &str, timeout_ms: Option<u64>) -> crate::Result<QueryResult> {
        let start = Instant::now();
        let timeout = timeout_ms.unwrap_or(DEFAULT_TIMEOUT_MS);

        // Auto-sync referenced topics before querying.
        self.auto_sync_topics(sql)?;

        let conn = self.conn.lock();

        // Set a busy timeout for the query duration.
        conn.busy_timeout(std::time::Duration::from_millis(timeout))
            .ok(); // best-effort

        self.run_query(&conn, sql, start, timeout)
    }

    /// Sync the latest data from a specific topic into the SQLite table.
    ///
    /// If the table does not exist yet it will be created. Only new records
    /// (since the last sync) are inserted.
    pub fn sync_topic(&self, topic: &str) -> crate::Result<usize> {
        let mapper = TopicTableMapper::new(&self.topic_manager);
        let conn = self.conn.lock();
        let mut sync_state = self.sync_state.lock();

        mapper.sync_topic_to_table(&conn, topic, &mut sync_state)
    }

    /// List all tables (topics) that are available for querying.
    ///
    /// This includes both already-synced tables and topics that *could* be
    /// synced on demand.
    pub fn list_tables(&self) -> crate::Result<Vec<TableInfo>> {
        let topics = self.topic_manager.list_topics()?;
        let conn = self.conn.lock();
        let sync_state = self.sync_state.lock();

        let mut tables = Vec::with_capacity(topics.len());
        for meta in &topics {
            let row_count = if sync_state.contains_key(&meta.name) {
                // Table exists -- get actual row count.
                let sql = format!(
                    "SELECT COUNT(*) FROM \"{}\"",
                    meta.name.replace('"', "\"\"")
                );
                conn.query_row(&sql, [], |row| row.get::<_, i64>(0))
                    .unwrap_or(0) as usize
            } else {
                0
            };

            tables.push(TableInfo {
                name: meta.name.clone(),
                columns: vec![
                    ColumnInfo {
                        name: "offset".to_string(),
                        data_type: "INTEGER".to_string(),
                    },
                    ColumnInfo {
                        name: "partition".to_string(),
                        data_type: "INTEGER".to_string(),
                    },
                    ColumnInfo {
                        name: "timestamp".to_string(),
                        data_type: "INTEGER".to_string(),
                    },
                    ColumnInfo {
                        name: "key".to_string(),
                        data_type: "TEXT".to_string(),
                    },
                    ColumnInfo {
                        name: "value".to_string(),
                        data_type: "TEXT".to_string(),
                    },
                    ColumnInfo {
                        name: "headers".to_string(),
                        data_type: "JSON".to_string(),
                    },
                ],
                row_count,
            });
        }

        Ok(tables)
    }

    // ── Private helpers ─────────────────────────────────────────────────

    /// Execute the actual query against the connection and collect results.
    fn run_query(
        &self,
        conn: &Connection,
        sql: &str,
        start: Instant,
        timeout_ms: u64,
    ) -> crate::Result<QueryResult> {
        let mut stmt = conn.prepare(sql).map_err(|e| {
            crate::error::StreamlineError::Storage(format!("SQLite prepare error: {}", e))
        })?;

        let column_count = stmt.column_count();
        let columns: Vec<String> = (0..column_count)
            .map(|i| stmt.column_name(i).unwrap_or("?").to_string())
            .collect();

        let rows_iter = stmt
            .query_map([], |row| {
                let mut values = Vec::with_capacity(column_count);
                for i in 0..column_count {
                    let val: Option<String> = match row.get_ref(i) {
                        Ok(rusqlite::types::ValueRef::Null) => None,
                        Ok(rusqlite::types::ValueRef::Integer(n)) => Some(n.to_string()),
                        Ok(rusqlite::types::ValueRef::Real(f)) => Some(f.to_string()),
                        Ok(rusqlite::types::ValueRef::Text(s)) => {
                            Some(String::from_utf8_lossy(s).to_string())
                        }
                        Ok(rusqlite::types::ValueRef::Blob(b)) => {
                            Some(base64::engine::general_purpose::STANDARD.encode(b))
                        }
                        Err(_) => None,
                    };
                    values.push(val);
                }
                Ok(QueryResultRow { values })
            })
            .map_err(|e| {
                if e.to_string().contains("interrupted") {
                    crate::error::StreamlineError::Storage("Query timed out".to_string())
                } else {
                    crate::error::StreamlineError::Storage(format!("SQLite query error: {}", e))
                }
            })?;

        let deadline = start + std::time::Duration::from_millis(timeout_ms);
        let max_rows = self.max_rows;
        let mut rows = Vec::new();
        for row_result in rows_iter {
            if rows.len() >= max_rows {
                debug!(max_rows = max_rows, "Query result truncated at max rows");
                break;
            }
            if Instant::now() > deadline {
                return Err(crate::error::StreamlineError::Storage(
                    "Query timed out".to_string(),
                ));
            }
            match row_result {
                Ok(row) => rows.push(row),
                Err(e) => {
                    if e.to_string().contains("interrupted") {
                        return Err(crate::error::StreamlineError::Storage(
                            "Query timed out".to_string(),
                        ));
                    }
                    warn!(error = %e, "Error reading SQLite row, skipping");
                }
            }
        }

        let row_count = rows.len();
        let execution_ms = start.elapsed().as_millis() as u64;

        Ok(QueryResult {
            columns,
            rows,
            row_count,
            execution_ms,
        })
    }

    /// Attempt to detect topic names referenced in the SQL and sync them.
    ///
    /// This is a best-effort heuristic: we check each known topic name to see
    /// if it appears in the query text (case-insensitive). For topics that are
    /// not yet synced we create the table and load data.
    fn auto_sync_topics(&self, sql: &str) -> crate::Result<()> {
        let topics = self.topic_manager.list_topics()?;
        let sql_lower = sql.to_lowercase();

        for meta in &topics {
            let topic_lower = meta.name.to_lowercase();
            // Check for unquoted or double-quoted references.
            if sql_lower.contains(&topic_lower) {
                let count = self.sync_topic(&meta.name)?;
                if count > 0 {
                    debug!(topic = %meta.name, new_records = count, "Auto-synced topic");
                }
            }
        }

        Ok(())
    }
}

// -- Unit tests --

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    /// Helper: create an in-memory TopicManager, add a topic, produce some records.
    fn setup_test_engine() -> (SQLiteQueryEngine, Arc<TopicManager>) {
        let tm = Arc::new(TopicManager::in_memory().unwrap());
        tm.create_topic("events", 1).unwrap();

        // Produce some JSON records.
        for i in 0..5 {
            let value = format!(
                r#"{{"id":{},"status":"ok","amount":{}}}"#,
                i,
                (i + 1) * 100
            );
            tm.append("events", 0, None, Bytes::from(value)).unwrap();
        }

        let engine = SQLiteQueryEngine::new(tm.clone()).unwrap();
        (engine, tm)
    }

    #[test]
    fn test_basic_select() {
        let (engine, _tm) = setup_test_engine();
        let result = engine.execute_query("SELECT * FROM events", None).unwrap();
        assert_eq!(result.row_count, 5);
        assert!(result.columns.contains(&"key".to_string()));
        assert!(result.columns.contains(&"value".to_string()));
        assert!(result.columns.contains(&"offset".to_string()));
    }

    #[test]
    fn test_where_clause() {
        let (engine, _tm) = setup_test_engine();
        let result = engine
            .execute_query("SELECT * FROM events WHERE \"offset\" >= 3", None)
            .unwrap();
        assert_eq!(result.row_count, 2);
    }

    #[test]
    fn test_count_aggregation() {
        let (engine, _tm) = setup_test_engine();
        let result = engine
            .execute_query("SELECT COUNT(*) as cnt FROM events", None)
            .unwrap();
        assert_eq!(result.row_count, 1);
        let count_val = result.rows[0].values[0].as_deref().unwrap();
        assert_eq!(count_val, "5");
    }

    #[test]
    fn test_sum_aggregation() {
        let (engine, _tm) = setup_test_engine();
        let result = engine
            .execute_query(
                "SELECT SUM(json_extract(value, '$.amount')) as total FROM events",
                None,
            )
            .unwrap();
        assert_eq!(result.row_count, 1);
        // amounts: 100 + 200 + 300 + 400 + 500 = 1500
        let total = result.rows[0].values[0].as_deref().unwrap();
        assert_eq!(total, "1500");
    }

    #[test]
    fn test_avg_aggregation() {
        let (engine, _tm) = setup_test_engine();
        let result = engine
            .execute_query(
                "SELECT AVG(json_extract(value, '$.amount')) as avg_amount FROM events",
                None,
            )
            .unwrap();
        assert_eq!(result.row_count, 1);
        let avg = result.rows[0].values[0]
            .as_deref()
            .unwrap()
            .parse::<f64>()
            .unwrap();
        assert!((avg - 300.0).abs() < 0.01);
    }

    #[test]
    fn test_json_extract() {
        let (engine, _tm) = setup_test_engine();
        let result = engine
            .execute_query(
                "SELECT json_extract(value, '$.status') as status FROM events LIMIT 1",
                None,
            )
            .unwrap();
        assert_eq!(result.row_count, 1);
        assert_eq!(result.rows[0].values[0].as_deref().unwrap(), "ok");
    }

    #[test]
    fn test_json_extract_where() {
        let (engine, _tm) = setup_test_engine();
        let result = engine
            .execute_query(
                "SELECT json_extract(value, '$.id') as id FROM events WHERE json_extract(value, '$.amount') > 300",
                None,
            )
            .unwrap();
        // amounts > 300 are 400 (id=3) and 500 (id=4)
        assert_eq!(result.row_count, 2);
    }

    #[test]
    fn test_query_timeout() {
        let (engine, _tm) = setup_test_engine();
        // A trivial query should not time out even with a 1ms timeout.
        // We cannot reliably test actual timeout without a long-running query,
        // so just verify the parameter is accepted.
        let result = engine.execute_query("SELECT 1", Some(1000));
        assert!(result.is_ok());
    }

    #[test]
    fn test_invalid_sql() {
        let (engine, _tm) = setup_test_engine();
        let result = engine.execute_query("NOT VALID SQL AT ALL", None);
        assert!(result.is_err());
    }

    #[test]
    fn test_list_tables() {
        let (engine, _tm) = setup_test_engine();

        // Before any query, tables are listed but row_count is 0.
        let tables = engine.list_tables().unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].name, "events");
        assert_eq!(tables[0].row_count, 0); // not synced yet

        // After a query (which triggers auto-sync), row_count should update.
        engine.execute_query("SELECT * FROM events", None).unwrap();
        let tables = engine.list_tables().unwrap();
        assert_eq!(tables[0].row_count, 5);
    }

    #[test]
    fn test_sync_topic_incremental() {
        let (engine, tm) = setup_test_engine();

        // First sync.
        let count = engine.sync_topic("events").unwrap();
        assert_eq!(count, 5);

        // No new records -- sync returns 0.
        let count = engine.sync_topic("events").unwrap();
        assert_eq!(count, 0);

        // Produce more records and sync again.
        tm.append("events", 0, None, Bytes::from(r#"{"id":99}"#))
            .unwrap();
        let count = engine.sync_topic("events").unwrap();
        assert_eq!(count, 1);

        // Verify total.
        let result = engine
            .execute_query("SELECT COUNT(*) FROM events", None)
            .unwrap();
        assert_eq!(result.rows[0].values[0].as_deref().unwrap(), "6");
    }

    #[test]
    fn test_multiple_topics() {
        let tm = Arc::new(TopicManager::in_memory().unwrap());
        tm.create_topic("orders", 1).unwrap();
        tm.create_topic("users", 1).unwrap();

        tm.append("orders", 0, None, Bytes::from(r#"{"total":42}"#))
            .unwrap();
        tm.append("users", 0, None, Bytes::from(r#"{"name":"alice"}"#))
            .unwrap();

        let engine = SQLiteQueryEngine::new(tm).unwrap();
        let tables = engine.list_tables().unwrap();
        assert_eq!(tables.len(), 2);

        let result = engine
            .execute_query("SELECT COUNT(*) FROM orders", None)
            .unwrap();
        assert_eq!(result.rows[0].values[0].as_deref().unwrap(), "1");

        let result = engine
            .execute_query("SELECT json_extract(value, '$.name') FROM users", None)
            .unwrap();
        assert_eq!(result.rows[0].values[0].as_deref().unwrap(), "alice");
    }

    #[test]
    fn test_type_mapping_columns() {
        let (engine, _tm) = setup_test_engine();
        let result = engine.execute_query("SELECT * FROM events LIMIT 1", None).unwrap();
        // Verify all expected columns are present.
        assert!(result.columns.contains(&"key".to_string()));
        assert!(result.columns.contains(&"value".to_string()));
        assert!(result.columns.contains(&"offset".to_string()));
        assert!(result.columns.contains(&"timestamp".to_string()));
        assert!(result.columns.contains(&"partition".to_string()));
        assert!(result.columns.contains(&"headers".to_string()));
    }

    #[test]
    fn test_timestamp_is_integer() {
        let (engine, _tm) = setup_test_engine();
        let result = engine
            .execute_query(
                "SELECT typeof(timestamp) as ts_type FROM events LIMIT 1",
                None,
            )
            .unwrap();
        assert_eq!(result.row_count, 1);
        let ts_type = result.rows[0].values[0].as_deref().unwrap();
        assert_eq!(ts_type, "integer");
    }

    #[test]
    fn test_offset_is_integer() {
        let (engine, _tm) = setup_test_engine();
        let result = engine
            .execute_query(
                "SELECT typeof(\"offset\") as off_type FROM events LIMIT 1",
                None,
            )
            .unwrap();
        assert_eq!(result.rows[0].values[0].as_deref().unwrap(), "integer");
    }

    #[test]
    fn test_partition_is_integer() {
        let (engine, _tm) = setup_test_engine();
        let result = engine
            .execute_query(
                "SELECT typeof(partition) as p_type FROM events LIMIT 1",
                None,
            )
            .unwrap();
        assert_eq!(result.rows[0].values[0].as_deref().unwrap(), "integer");
    }

    #[test]
    fn test_configurable_max_rows() {
        let tm = Arc::new(TopicManager::in_memory().unwrap());
        tm.create_topic("big_topic", 1).unwrap();

        // Insert 20 records.
        for i in 0..20 {
            let value = format!(r#"{{"idx":{}}}"#, i);
            tm.append("big_topic", 0, None, Bytes::from(value))
                .unwrap();
        }

        // Engine with max_rows = 5.
        let engine = SQLiteQueryEngine::with_max_rows(tm, 5).unwrap();
        let result = engine
            .execute_query("SELECT * FROM big_topic", None)
            .unwrap();
        // Should be truncated to 5 rows.
        assert_eq!(result.row_count, 5);
    }

    #[test]
    fn test_min_max_aggregation() {
        let (engine, _tm) = setup_test_engine();
        let result = engine
            .execute_query(
                "SELECT MIN(json_extract(value, '$.amount')) as mn, MAX(json_extract(value, '$.amount')) as mx FROM events",
                None,
            )
            .unwrap();
        assert_eq!(result.row_count, 1);
        assert_eq!(result.rows[0].values[0].as_deref().unwrap(), "100");
        assert_eq!(result.rows[0].values[1].as_deref().unwrap(), "500");
    }

    #[test]
    fn test_group_by() {
        let (engine, _tm) = setup_test_engine();
        let result = engine
            .execute_query(
                "SELECT partition, COUNT(*) as cnt FROM events GROUP BY partition",
                None,
            )
            .unwrap();
        assert_eq!(result.row_count, 1); // all in partition 0
        assert_eq!(result.rows[0].values[1].as_deref().unwrap(), "5");
    }

    #[test]
    fn test_order_by_limit() {
        let (engine, _tm) = setup_test_engine();
        let result = engine
            .execute_query(
                "SELECT \"offset\" FROM events ORDER BY \"offset\" DESC LIMIT 2",
                None,
            )
            .unwrap();
        assert_eq!(result.row_count, 2);
        assert_eq!(result.rows[0].values[0].as_deref().unwrap(), "4");
        assert_eq!(result.rows[1].values[0].as_deref().unwrap(), "3");
    }

    #[test]
    fn test_headers_json_column() {
        let (engine, _tm) = setup_test_engine();
        let result = engine
            .execute_query("SELECT headers FROM events LIMIT 1", None)
            .unwrap();
        assert_eq!(result.row_count, 1);
        // Records were inserted without headers, so should be "{}".
        assert_eq!(result.rows[0].values[0].as_deref().unwrap(), "{}");
    }
}
