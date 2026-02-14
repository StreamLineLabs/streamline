//! Integration tests for the SQLite query interface
//!
//! These tests exercise the `SQLiteQueryEngine` end-to-end, verifying:
//! - Basic SELECT queries
//! - WHERE clause filtering
//! - Aggregations (COUNT, SUM, AVG)
//! - JSON extraction from message values
//! - Query timeout handling
//! - Error handling for invalid SQL
//! - Table listing
//!
//! All tests use an in-memory TopicManager so no disk I/O is required.

#![cfg(feature = "sqlite-queries")]

use bytes::Bytes;
use std::sync::Arc;
use streamline::sqlite::{QueryResult, SQLiteQueryEngine};
use streamline::storage::TopicManager;

// ── Test helpers ────────────────────────────────────────────────────────

/// Create a TopicManager + SQLiteQueryEngine with a single topic populated
/// with JSON records.
fn create_test_env() -> (SQLiteQueryEngine, Arc<TopicManager>) {
    let tm = Arc::new(TopicManager::in_memory().unwrap());
    tm.create_topic("events", 2).unwrap();

    // Produce records across 2 partitions.
    for i in 0..10 {
        let partition = i % 2;
        let key = format!("key-{}", i);
        let value = format!(
            r#"{{"id":{},"user":"user-{}","amount":{},"status":"{}"}}"#,
            i,
            i % 3,
            (i + 1) * 10,
            if i % 2 == 0 { "completed" } else { "pending" }
        );
        tm.append(
            "events",
            partition,
            Some(Bytes::from(key)),
            Bytes::from(value),
        )
        .unwrap();
    }

    let engine = SQLiteQueryEngine::new(tm.clone()).unwrap();
    (engine, tm)
}

/// Helper to extract a single scalar value from a query result.
fn scalar(result: &QueryResult) -> &str {
    result.rows[0].values[0].as_deref().unwrap()
}

// ── Tests ───────────────────────────────────────────────────────────────

#[test]
fn test_basic_select_all() {
    let (engine, _) = create_test_env();
    let result = engine.execute_query("SELECT * FROM events", None).unwrap();

    assert_eq!(result.row_count, 10);
    assert!(result.columns.contains(&"offset".to_string()));
    assert!(result.columns.contains(&"partition".to_string()));
    assert!(result.columns.contains(&"timestamp".to_string()));
    assert!(result.columns.contains(&"key".to_string()));
    assert!(result.columns.contains(&"value".to_string()));
    assert!(result.columns.contains(&"headers".to_string()));
    assert!(result.execution_ms < 5000); // sanity: should be fast
}

#[test]
fn test_select_with_limit() {
    let (engine, _) = create_test_env();
    let result = engine
        .execute_query("SELECT * FROM events LIMIT 3", None)
        .unwrap();

    assert_eq!(result.row_count, 3);
}

#[test]
fn test_select_with_offset() {
    let (engine, _) = create_test_env();
    let result = engine
        .execute_query("SELECT * FROM events LIMIT 5 OFFSET 7", None)
        .unwrap();

    assert_eq!(result.row_count, 3); // only 3 records left after offset 7
}

#[test]
fn test_where_clause_offset_filter() {
    let (engine, _) = create_test_env();
    let result = engine
        .execute_query(
            "SELECT COUNT(*) as cnt FROM events WHERE \"offset\" >= 3",
            None,
        )
        .unwrap();

    // Partition 0 has offsets 0..4 (5 records), partition 1 has offsets 0..4
    // offset >= 3 means offsets 3,4 in each partition = 4 records
    let cnt: i64 = scalar(&result).parse().unwrap();
    assert_eq!(cnt, 4);
}

#[test]
fn test_where_clause_partition_filter() {
    let (engine, _) = create_test_env();
    let result = engine
        .execute_query(
            "SELECT COUNT(*) as cnt FROM events WHERE partition = 0",
            None,
        )
        .unwrap();

    assert_eq!(scalar(&result), "5");
}

#[test]
fn test_where_clause_key_filter() {
    let (engine, _) = create_test_env();
    let result = engine
        .execute_query("SELECT key FROM events WHERE key = 'key-5'", None)
        .unwrap();

    assert_eq!(result.row_count, 1);
    assert_eq!(result.rows[0].values[0].as_deref().unwrap(), "key-5");
}

#[test]
fn test_count_aggregation() {
    let (engine, _) = create_test_env();
    let result = engine
        .execute_query("SELECT COUNT(*) as total FROM events", None)
        .unwrap();

    assert_eq!(result.row_count, 1);
    assert_eq!(scalar(&result), "10");
}

#[test]
fn test_sum_aggregation() {
    let (engine, _) = create_test_env();
    let result = engine
        .execute_query(
            "SELECT SUM(json_extract(value, '$.amount')) as total FROM events",
            None,
        )
        .unwrap();

    // amounts: 10+20+30+40+50+60+70+80+90+100 = 550
    assert_eq!(scalar(&result), "550");
}

#[test]
fn test_avg_aggregation() {
    let (engine, _) = create_test_env();
    let result = engine
        .execute_query(
            "SELECT AVG(json_extract(value, '$.amount')) as avg_amount FROM events",
            None,
        )
        .unwrap();

    let avg: f64 = scalar(&result).parse().unwrap();
    assert!((avg - 55.0).abs() < 0.01);
}

#[test]
fn test_group_by_partition() {
    let (engine, _) = create_test_env();
    let result = engine
        .execute_query(
            "SELECT partition, COUNT(*) as cnt FROM events GROUP BY partition ORDER BY partition",
            None,
        )
        .unwrap();

    assert_eq!(result.row_count, 2);
    // Each partition gets 5 records.
    assert_eq!(result.rows[0].values[1].as_deref().unwrap(), "5");
    assert_eq!(result.rows[1].values[1].as_deref().unwrap(), "5");
}

#[test]
fn test_json_extract_field() {
    let (engine, _) = create_test_env();
    let result = engine
        .execute_query(
            "SELECT json_extract(value, '$.user') as user FROM events WHERE key = 'key-0'",
            None,
        )
        .unwrap();

    assert_eq!(result.row_count, 1);
    assert_eq!(result.rows[0].values[0].as_deref().unwrap(), "user-0");
}

#[test]
fn test_json_extract_where_clause() {
    let (engine, _) = create_test_env();
    let result = engine
        .execute_query(
            "SELECT json_extract(value, '$.id') as id FROM events WHERE json_extract(value, '$.status') = 'completed' ORDER BY id",
            None,
        )
        .unwrap();

    // Even-indexed records (0,2,4,6,8) have status=completed
    assert_eq!(result.row_count, 5);
    assert_eq!(result.rows[0].values[0].as_deref().unwrap(), "0");
    assert_eq!(result.rows[4].values[0].as_deref().unwrap(), "8");
}

#[test]
fn test_json_extract_nested_aggregation() {
    let (engine, _) = create_test_env();
    let result = engine
        .execute_query(
            "SELECT json_extract(value, '$.status') as status, COUNT(*) as cnt \
             FROM events GROUP BY status ORDER BY status",
            None,
        )
        .unwrap();

    assert_eq!(result.row_count, 2);
    // "completed" = 5, "pending" = 5
    let statuses: Vec<&str> = result
        .rows
        .iter()
        .map(|r| r.values[0].as_deref().unwrap())
        .collect();
    assert!(statuses.contains(&"completed"));
    assert!(statuses.contains(&"pending"));
}

#[test]
fn test_query_timeout_accepted() {
    let (engine, _) = create_test_env();
    // A fast query with an explicit timeout should succeed.
    let result = engine.execute_query("SELECT 1 as one", Some(5000));
    assert!(result.is_ok());
    assert_eq!(scalar(&result.unwrap()), "1");
}

#[test]
fn test_invalid_sql_syntax() {
    let (engine, _) = create_test_env();
    let result = engine.execute_query("THIS IS NOT SQL", None);
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("SQLite") || err_msg.contains("error"),
        "Error should mention SQLite: {}",
        err_msg
    );
}

#[test]
fn test_invalid_table_name() {
    let (engine, _) = create_test_env();
    let result = engine.execute_query("SELECT * FROM nonexistent_topic", None);
    assert!(result.is_err());
}

#[test]
fn test_invalid_column() {
    let (engine, _) = create_test_env();
    // Auto-sync events first so the table exists.
    engine.sync_topic("events").unwrap();
    let result = engine.execute_query("SELECT nonexistent_col FROM events", None);
    assert!(result.is_err());
}

#[test]
fn test_list_tables() {
    let (engine, _) = create_test_env();
    let tables = engine.list_tables().unwrap();

    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].name, "events");
    assert_eq!(tables[0].columns.len(), 6);

    // Verify column names.
    let col_names: Vec<&str> = tables[0].columns.iter().map(|c| c.name.as_str()).collect();
    assert!(col_names.contains(&"offset"));
    assert!(col_names.contains(&"partition"));
    assert!(col_names.contains(&"timestamp"));
    assert!(col_names.contains(&"key"));
    assert!(col_names.contains(&"value"));
    assert!(col_names.contains(&"headers"));
}

#[test]
fn test_list_tables_row_count_after_sync() {
    let (engine, _) = create_test_env();

    // Before sync, row count is 0.
    let tables = engine.list_tables().unwrap();
    assert_eq!(tables[0].row_count, 0);

    // Trigger sync via a query.
    engine.execute_query("SELECT * FROM events", None).unwrap();

    // After sync, row count is 10.
    let tables = engine.list_tables().unwrap();
    assert_eq!(tables[0].row_count, 10);
}

#[test]
fn test_sync_topic_incremental() {
    let (engine, tm) = create_test_env();

    // First sync loads all 10 records.
    let synced = engine.sync_topic("events").unwrap();
    assert_eq!(synced, 10);

    // Second sync with no new data returns 0.
    let synced = engine.sync_topic("events").unwrap();
    assert_eq!(synced, 0);

    // Produce a new record and sync again.
    tm.append(
        "events",
        0,
        Some(Bytes::from("key-new")),
        Bytes::from(r#"{"id":999}"#),
    )
    .unwrap();

    let synced = engine.sync_topic("events").unwrap();
    assert_eq!(synced, 1);

    // Verify total.
    let result = engine
        .execute_query("SELECT COUNT(*) FROM events", None)
        .unwrap();
    assert_eq!(scalar(&result), "11");
}

#[test]
fn test_multiple_topics() {
    let tm = Arc::new(TopicManager::in_memory().unwrap());
    tm.create_topic("orders", 1).unwrap();
    tm.create_topic("users", 1).unwrap();

    tm.append(
        "orders",
        0,
        None,
        Bytes::from(r#"{"total":100}"#),
    )
    .unwrap();
    tm.append(
        "orders",
        0,
        None,
        Bytes::from(r#"{"total":200}"#),
    )
    .unwrap();
    tm.append(
        "users",
        0,
        None,
        Bytes::from(r#"{"name":"alice"}"#),
    )
    .unwrap();

    let engine = SQLiteQueryEngine::new(tm).unwrap();

    // Query orders.
    let result = engine
        .execute_query(
            "SELECT SUM(json_extract(value, '$.total')) as total FROM orders",
            None,
        )
        .unwrap();
    assert_eq!(scalar(&result), "300");

    // Query users.
    let result = engine
        .execute_query(
            "SELECT json_extract(value, '$.name') as name FROM users",
            None,
        )
        .unwrap();
    assert_eq!(result.rows[0].values[0].as_deref().unwrap(), "alice");

    // List all tables.
    let tables = engine.list_tables().unwrap();
    assert_eq!(tables.len(), 2);
    let names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
    assert!(names.contains(&"orders"));
    assert!(names.contains(&"users"));
}

#[test]
fn test_cross_topic_join() {
    let tm = Arc::new(TopicManager::in_memory().unwrap());
    tm.create_topic("orders", 1).unwrap();
    tm.create_topic("customers", 1).unwrap();

    tm.append(
        "orders",
        0,
        None,
        Bytes::from(r#"{"customer_id":1,"total":100}"#),
    )
    .unwrap();
    tm.append(
        "orders",
        0,
        None,
        Bytes::from(r#"{"customer_id":2,"total":200}"#),
    )
    .unwrap();
    tm.append(
        "customers",
        0,
        None,
        Bytes::from(r#"{"id":1,"name":"alice"}"#),
    )
    .unwrap();
    tm.append(
        "customers",
        0,
        None,
        Bytes::from(r#"{"id":2,"name":"bob"}"#),
    )
    .unwrap();

    let engine = SQLiteQueryEngine::new(tm).unwrap();

    let result = engine
        .execute_query(
            r#"SELECT
                json_extract(c.value, '$.name') as customer_name,
                json_extract(o.value, '$.total') as order_total
            FROM orders o
            JOIN customers c
                ON json_extract(o.value, '$.customer_id') = json_extract(c.value, '$.id')
            ORDER BY customer_name"#,
            None,
        )
        .unwrap();

    assert_eq!(result.row_count, 2);
    assert_eq!(result.rows[0].values[0].as_deref().unwrap(), "alice");
    assert_eq!(result.rows[0].values[1].as_deref().unwrap(), "100");
    assert_eq!(result.rows[1].values[0].as_deref().unwrap(), "bob");
    assert_eq!(result.rows[1].values[1].as_deref().unwrap(), "200");
}

#[test]
fn test_distinct_keys() {
    let (engine, _) = create_test_env();
    let result = engine
        .execute_query("SELECT COUNT(DISTINCT key) as unique_keys FROM events", None)
        .unwrap();

    assert_eq!(scalar(&result), "10");
}

#[test]
fn test_order_by() {
    let (engine, _) = create_test_env();
    let result = engine
        .execute_query(
            "SELECT json_extract(value, '$.amount') as amount FROM events ORDER BY amount DESC LIMIT 3",
            None,
        )
        .unwrap();

    assert_eq!(result.row_count, 3);
    let amounts: Vec<i64> = result
        .rows
        .iter()
        .map(|r| r.values[0].as_deref().unwrap().parse().unwrap())
        .collect();
    assert!(amounts[0] >= amounts[1]);
    assert!(amounts[1] >= amounts[2]);
}

#[test]
fn test_empty_topic() {
    let tm = Arc::new(TopicManager::in_memory().unwrap());
    tm.create_topic("empty", 1).unwrap();

    let engine = SQLiteQueryEngine::new(tm).unwrap();
    let result = engine
        .execute_query("SELECT COUNT(*) FROM empty", None)
        .unwrap();
    assert_eq!(scalar(&result), "0");
}
