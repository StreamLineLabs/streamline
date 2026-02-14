//! Comprehensive integration tests for the analytics module.
//!
//! These tests exercise the DuckDB analytics engine through the
//! `streamline_analytics` workspace crate, covering:
//!
//! - Basic SQL queries on stream data
//! - Aggregation queries (COUNT, SUM, AVG, MIN, MAX)
//! - Window functions (ROW_NUMBER, RANK, LAG)
//! - Error handling (invalid SQL, non-existent topics)
//! - Concurrent query execution
//! - Query timeout behaviour
//! - Large result set handling
//! - Pagination (offset + limit)
//! - Cache operations
//! - Materialized view lifecycle

#[cfg(feature = "analytics")]
mod analytics_tests {
    use std::sync::Arc;
    use streamline_analytics::duckdb::{DuckDBEngine, QueryOptions};
    use streamline_analytics::error::AnalyticsError;
    use streamline_analytics::topic_source::{AnalyticsRecord, TopicDataSource};

    // ── Test data sources ────────────────────────────────────────────────

    /// Empty topic source for minimal tests.
    struct EmptySource;

    impl TopicDataSource for EmptySource {
        fn num_partitions(&self, _topic: &str) -> streamline_analytics::error::Result<i32> {
            Ok(1)
        }
        fn earliest_offset(
            &self,
            _topic: &str,
            _partition: i32,
        ) -> streamline_analytics::error::Result<i64> {
            Ok(0)
        }
        fn latest_offset(
            &self,
            _topic: &str,
            _partition: i32,
        ) -> streamline_analytics::error::Result<i64> {
            Ok(0)
        }
        fn read_records(
            &self,
            _topic: &str,
            _partition: i32,
            _offset: i64,
            _max: usize,
        ) -> streamline_analytics::error::Result<Vec<AnalyticsRecord>> {
            Ok(vec![])
        }
    }

    /// Populated source with realistic event data across two topics.
    struct TestSource;

    impl TopicDataSource for TestSource {
        fn num_partitions(&self, topic: &str) -> streamline_analytics::error::Result<i32> {
            match topic {
                "events" => Ok(2),
                "metrics" => Ok(1),
                _ => Err(AnalyticsError::topic_not_found(topic)),
            }
        }

        fn earliest_offset(
            &self,
            _topic: &str,
            _partition: i32,
        ) -> streamline_analytics::error::Result<i64> {
            Ok(0)
        }

        fn latest_offset(
            &self,
            topic: &str,
            partition: i32,
        ) -> streamline_analytics::error::Result<i64> {
            match (topic, partition) {
                ("events", 0) => Ok(5),
                ("events", 1) => Ok(3),
                ("metrics", 0) => Ok(4),
                _ => Ok(0),
            }
        }

        fn read_records(
            &self,
            topic: &str,
            partition: i32,
            _offset: i64,
            _max: usize,
        ) -> streamline_analytics::error::Result<Vec<AnalyticsRecord>> {
            match (topic, partition) {
                ("events", 0) => Ok(vec![
                    record(0, 1700000000000, Some("u1"), r#"{"user_id":"u1","action":"click","amount":10}"#),
                    record(1, 1700000001000, Some("u2"), r#"{"user_id":"u2","action":"view","amount":20}"#),
                    record(2, 1700000002000, Some("u1"), r#"{"user_id":"u1","action":"purchase","amount":100}"#),
                    record(3, 1700000003000, Some("u3"), r#"{"user_id":"u3","action":"click","amount":5}"#),
                    record(4, 1700000004000, Some("u2"), r#"{"user_id":"u2","action":"purchase","amount":50}"#),
                ]),
                ("events", 1) => Ok(vec![
                    record(0, 1700000005000, Some("u4"), r#"{"user_id":"u4","action":"view","amount":15}"#),
                    record(1, 1700000006000, Some("u1"), r#"{"user_id":"u1","action":"view","amount":8}"#),
                    record(2, 1700000007000, Some("u5"), r#"{"user_id":"u5","action":"click","amount":12}"#),
                ]),
                ("metrics", 0) => Ok(vec![
                    record(0, 1700000000000, None, r#"{"host":"h1","cpu":45.5,"mem":1024}"#),
                    record(1, 1700000001000, None, r#"{"host":"h2","cpu":78.2,"mem":2048}"#),
                    record(2, 1700000002000, None, r#"{"host":"h1","cpu":52.0,"mem":1536}"#),
                    record(3, 1700000003000, None, r#"{"host":"h2","cpu":30.1,"mem":4096}"#),
                ]),
                _ => Ok(vec![]),
            }
        }
    }

    fn record(offset: i64, ts: i64, key: Option<&str>, value: &str) -> AnalyticsRecord {
        AnalyticsRecord {
            offset,
            timestamp: ts,
            key: key.map(|k| k.as_bytes().to_vec()),
            value: value.as_bytes().to_vec(),
            headers: vec![],
        }
    }

    fn no_cache_opts() -> QueryOptions {
        QueryOptions {
            enable_cache: false,
            ..Default::default()
        }
    }

    // ── Engine creation ──────────────────────────────────────────────────

    #[tokio::test]
    async fn test_engine_creation_succeeds() {
        let src: Arc<dyn TopicDataSource> = Arc::new(EmptySource);
        assert!(DuckDBEngine::new(src).is_ok());
    }

    // ── Basic SQL queries ────────────────────────────────────────────────

    #[tokio::test]
    async fn test_select_constant() {
        let engine = DuckDBEngine::new(Arc::new(EmptySource) as Arc<dyn TopicDataSource>).unwrap();

        let result = engine
            .execute_query("SELECT 1 AS val, 'hello' AS txt", no_cache_opts())
            .await
            .unwrap();

        assert_eq!(result.row_count, 1);
        assert_eq!(result.columns, vec!["val", "txt"]);
        assert_eq!(result.rows[0].values[0], serde_json::json!(1));
        assert_eq!(result.rows[0].values[1], serde_json::json!("hello"));
    }

    #[tokio::test]
    async fn test_select_all_from_topic() {
        let engine = DuckDBEngine::new(Arc::new(TestSource) as Arc<dyn TopicDataSource>).unwrap();

        let result = engine
            .execute_query(
                "SELECT * FROM streamline_topic_events ORDER BY \"offset\", \"partition\"",
                no_cache_opts(),
            )
            .await
            .unwrap();

        // 5 records from partition 0 + 3 from partition 1
        assert_eq!(result.row_count, 8);
        assert!(!result.from_cache);
    }

    #[tokio::test]
    async fn test_select_with_where_clause() {
        let engine = DuckDBEngine::new(Arc::new(TestSource) as Arc<dyn TopicDataSource>).unwrap();

        let result = engine
            .execute_query(
                "SELECT * FROM streamline_topic_events WHERE key = 'u1'",
                no_cache_opts(),
            )
            .await
            .unwrap();

        // u1 appears 3 times: offsets 0,2 on p0 and offset 1 on p1
        assert_eq!(result.row_count, 3);
    }

    // ── Aggregation queries ──────────────────────────────────────────────

    #[tokio::test]
    async fn test_count() {
        let engine = DuckDBEngine::new(Arc::new(TestSource) as Arc<dyn TopicDataSource>).unwrap();

        let result = engine
            .execute_query(
                "SELECT COUNT(*) AS cnt FROM streamline_topic_events",
                no_cache_opts(),
            )
            .await
            .unwrap();

        assert_eq!(result.rows[0].values[0], serde_json::json!(8));
    }

    #[tokio::test]
    async fn test_sum() {
        let engine = DuckDBEngine::new(Arc::new(TestSource) as Arc<dyn TopicDataSource>).unwrap();

        let result = engine
            .execute_query(
                "SELECT SUM(CAST(value->>'amount' AS INTEGER)) AS total FROM streamline_topic_events",
                no_cache_opts(),
            )
            .await
            .unwrap();

        // 10+20+100+5+50+15+8+12 = 220
        assert_eq!(result.rows[0].values[0], serde_json::json!(220));
    }

    #[tokio::test]
    async fn test_avg() {
        let engine = DuckDBEngine::new(Arc::new(TestSource) as Arc<dyn TopicDataSource>).unwrap();

        let result = engine
            .execute_query(
                "SELECT AVG(CAST(value->>'amount' AS DOUBLE)) AS avg_amt FROM streamline_topic_events",
                no_cache_opts(),
            )
            .await
            .unwrap();

        let avg = result.rows[0].values[0].as_f64().unwrap();
        assert!((avg - 27.5).abs() < 0.01); // 220 / 8 = 27.5
    }

    #[tokio::test]
    async fn test_min_max() {
        let engine = DuckDBEngine::new(Arc::new(TestSource) as Arc<dyn TopicDataSource>).unwrap();

        let result = engine
            .execute_query(
                "SELECT MIN(CAST(value->>'amount' AS INTEGER)) AS min_amt, \
                        MAX(CAST(value->>'amount' AS INTEGER)) AS max_amt \
                 FROM streamline_topic_events",
                no_cache_opts(),
            )
            .await
            .unwrap();

        assert_eq!(result.rows[0].values[0], serde_json::json!(5));
        assert_eq!(result.rows[0].values[1], serde_json::json!(100));
    }

    #[tokio::test]
    async fn test_group_by() {
        let engine = DuckDBEngine::new(Arc::new(TestSource) as Arc<dyn TopicDataSource>).unwrap();

        let result = engine
            .execute_query(
                "SELECT value->>'action' AS action, COUNT(*) AS cnt \
                 FROM streamline_topic_events \
                 GROUP BY value->>'action' \
                 ORDER BY cnt DESC",
                no_cache_opts(),
            )
            .await
            .unwrap();

        // click: 3, view: 3, purchase: 2
        assert_eq!(result.row_count, 3);
    }

    // ── Window functions ─────────────────────────────────────────────────

    #[tokio::test]
    async fn test_row_number() {
        let engine = DuckDBEngine::new(Arc::new(TestSource) as Arc<dyn TopicDataSource>).unwrap();

        let result = engine
            .execute_query(
                "SELECT \"offset\", \"partition\", ROW_NUMBER() OVER (ORDER BY \"partition\", \"offset\") AS rn \
                 FROM streamline_topic_events",
                no_cache_opts(),
            )
            .await
            .unwrap();

        assert_eq!(result.row_count, 8);
        // Check monotonically increasing row numbers
        for (i, row) in result.rows.iter().enumerate() {
            assert_eq!(row.values[2], serde_json::json!((i + 1) as i64));
        }
    }

    #[tokio::test]
    async fn test_rank_window() {
        let engine = DuckDBEngine::new(Arc::new(TestSource) as Arc<dyn TopicDataSource>).unwrap();

        let result = engine
            .execute_query(
                "SELECT key, RANK() OVER (ORDER BY key) AS rnk \
                 FROM streamline_topic_events \
                 WHERE key IS NOT NULL",
                no_cache_opts(),
            )
            .await
            .unwrap();

        assert!(result.row_count > 0);
    }

    // ── Error handling ───────────────────────────────────────────────────

    #[tokio::test]
    async fn test_invalid_sql_error() {
        let engine = DuckDBEngine::new(Arc::new(EmptySource) as Arc<dyn TopicDataSource>).unwrap();

        let result = engine
            .execute_query("SELCT * FORM nothing", no_cache_opts())
            .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, AnalyticsError::InvalidSql(_)),
            "Expected InvalidSql, got: {:?}",
            err
        );
    }

    #[tokio::test]
    async fn test_nonexistent_table_error() {
        let engine = DuckDBEngine::new(Arc::new(EmptySource) as Arc<dyn TopicDataSource>).unwrap();

        let result = engine
            .execute_query("SELECT * FROM does_not_exist", no_cache_opts())
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_nonexistent_topic_returns_error() {
        let engine = DuckDBEngine::new(Arc::new(TestSource) as Arc<dyn TopicDataSource>).unwrap();

        // Loading a topic that TestSource does not know about
        let result = engine
            .execute_query(
                "SELECT * FROM streamline_topic('nonexistent')",
                no_cache_opts(),
            )
            .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, AnalyticsError::TopicNotFound(_)),
            "Expected TopicNotFound, got: {:?}",
            err
        );
    }

    // ── Concurrent query execution ───────────────────────────────────────

    #[tokio::test]
    async fn test_concurrent_queries() {
        let engine =
            Arc::new(DuckDBEngine::new(Arc::new(EmptySource) as Arc<dyn TopicDataSource>).unwrap());

        let mut handles = Vec::new();
        for i in 0..10 {
            let eng = engine.clone();
            handles.push(tokio::spawn(async move {
                eng.execute_query(&format!("SELECT {} AS val", i), no_cache_opts())
                    .await
            }));
        }

        for handle in handles {
            let result = handle.await.unwrap();
            assert!(result.is_ok());
        }
    }

    #[tokio::test]
    async fn test_concurrent_queries_on_populated_data() {
        let engine =
            Arc::new(DuckDBEngine::new(Arc::new(TestSource) as Arc<dyn TopicDataSource>).unwrap());

        let mut handles = Vec::new();
        for _ in 0..5 {
            let eng = engine.clone();
            handles.push(tokio::spawn(async move {
                eng.execute_query(
                    "SELECT COUNT(*) FROM streamline_topic_events",
                    no_cache_opts(),
                )
                .await
            }));
        }

        for handle in handles {
            let result = handle.await.unwrap().unwrap();
            assert_eq!(result.rows[0].values[0], serde_json::json!(8));
        }
    }

    // ── Query timeout ────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_fast_query_does_not_timeout() {
        let engine = DuckDBEngine::new(Arc::new(EmptySource) as Arc<dyn TopicDataSource>).unwrap();

        let result = engine
            .execute_query(
                "SELECT 42",
                QueryOptions {
                    timeout_ms: Some(5_000),
                    enable_cache: false,
                    ..Default::default()
                },
            )
            .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_no_timeout_when_not_configured() {
        let engine = DuckDBEngine::new(Arc::new(EmptySource) as Arc<dyn TopicDataSource>).unwrap();

        let result = engine
            .execute_query(
                "SELECT 1",
                QueryOptions {
                    timeout_ms: None,
                    enable_cache: false,
                    ..Default::default()
                },
            )
            .await;

        assert!(result.is_ok());
    }

    // ── Large result set handling ────────────────────────────────────────

    #[tokio::test]
    async fn test_max_rows_truncates_results() {
        let engine = DuckDBEngine::new(Arc::new(TestSource) as Arc<dyn TopicDataSource>).unwrap();

        let result = engine
            .execute_query(
                "SELECT * FROM streamline_topic_events",
                QueryOptions {
                    max_rows: Some(3),
                    enable_cache: false,
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        assert_eq!(result.row_count, 3);
        assert!(result.has_more);
        assert_eq!(result.total_rows, 8);
    }

    #[tokio::test]
    async fn test_unlimited_rows() {
        let engine = DuckDBEngine::new(Arc::new(TestSource) as Arc<dyn TopicDataSource>).unwrap();

        let result = engine
            .execute_query(
                "SELECT * FROM streamline_topic_events",
                QueryOptions {
                    max_rows: None,
                    enable_cache: false,
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        assert_eq!(result.row_count, 8);
        assert!(!result.has_more);
    }

    // ── Pagination ───────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_pagination_offset_and_limit() {
        let engine = DuckDBEngine::new(Arc::new(TestSource) as Arc<dyn TopicDataSource>).unwrap();

        // Page 1: rows 0-2
        let page1 = engine
            .execute_query(
                "SELECT * FROM streamline_topic_events ORDER BY \"partition\", \"offset\"",
                QueryOptions {
                    max_rows: Some(3),
                    offset: 0,
                    enable_cache: false,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(page1.row_count, 3);
        assert!(page1.has_more);

        // Page 2: rows 3-5
        let page2 = engine
            .execute_query(
                "SELECT * FROM streamline_topic_events ORDER BY \"partition\", \"offset\"",
                QueryOptions {
                    max_rows: Some(3),
                    offset: 3,
                    enable_cache: false,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(page2.row_count, 3);
        assert!(page2.has_more);

        // Page 3: rows 6-7 (only 2 left)
        let page3 = engine
            .execute_query(
                "SELECT * FROM streamline_topic_events ORDER BY \"partition\", \"offset\"",
                QueryOptions {
                    max_rows: Some(3),
                    offset: 6,
                    enable_cache: false,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(page3.row_count, 2);
        assert!(!page3.has_more);
    }

    #[tokio::test]
    async fn test_offset_beyond_results_returns_empty() {
        let engine = DuckDBEngine::new(Arc::new(TestSource) as Arc<dyn TopicDataSource>).unwrap();

        let result = engine
            .execute_query(
                "SELECT * FROM streamline_topic_events",
                QueryOptions {
                    max_rows: Some(10),
                    offset: 100,
                    enable_cache: false,
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        assert_eq!(result.row_count, 0);
        assert!(!result.has_more);
    }

    // ── Cache operations ─────────────────────────────────────────────────

    #[tokio::test]
    async fn test_cache_hit_on_second_call() {
        let engine = DuckDBEngine::new(Arc::new(EmptySource) as Arc<dyn TopicDataSource>).unwrap();

        let sql = "SELECT 99 AS answer";
        let opts = QueryOptions {
            enable_cache: true,
            cache_ttl_seconds: 60,
            ..Default::default()
        };

        let r1 = engine.execute_query(sql, opts.clone()).await.unwrap();
        assert!(!r1.from_cache);

        let r2 = engine.execute_query(sql, opts).await.unwrap();
        assert!(r2.from_cache);
    }

    #[tokio::test]
    async fn test_clear_cache_invalidates_entries() {
        let engine = DuckDBEngine::new(Arc::new(EmptySource) as Arc<dyn TopicDataSource>).unwrap();

        engine
            .execute_query("SELECT 1", QueryOptions::default())
            .await
            .unwrap();

        assert_eq!(engine.cache_stats().total_entries, 1);

        engine.clear_cache();
        assert_eq!(engine.cache_stats().total_entries, 0);

        // Next query should not be from cache
        let result = engine
            .execute_query("SELECT 1", QueryOptions::default())
            .await
            .unwrap();
        assert!(!result.from_cache);
    }

    #[tokio::test]
    async fn test_cache_disabled() {
        let engine = DuckDBEngine::new(Arc::new(EmptySource) as Arc<dyn TopicDataSource>).unwrap();

        let sql = "SELECT 1";
        let opts = QueryOptions {
            enable_cache: false,
            ..Default::default()
        };

        engine.execute_query(sql, opts.clone()).await.unwrap();
        engine.execute_query(sql, opts).await.unwrap();

        // Nothing should be cached
        assert_eq!(engine.cache_stats().total_entries, 0);
    }

    // ── Materialized views ───────────────────────────────────────────────

    #[tokio::test]
    async fn test_materialized_view_create_list_drop() {
        let engine = DuckDBEngine::new(Arc::new(EmptySource) as Arc<dyn TopicDataSource>).unwrap();

        engine
            .create_materialized_view("v1", "SELECT 42 AS answer", 300)
            .unwrap();
        assert_eq!(engine.list_materialized_views().len(), 1);

        engine
            .create_materialized_view("v2", "SELECT 'hello' AS greeting", 600)
            .unwrap();
        assert_eq!(engine.list_materialized_views().len(), 2);

        engine.drop_materialized_view("v1").await.unwrap();
        assert_eq!(engine.list_materialized_views().len(), 1);

        engine.drop_materialized_view("v2").await.unwrap();
        assert_eq!(engine.list_materialized_views().len(), 0);
    }

    #[tokio::test]
    async fn test_materialized_view_refresh() {
        let engine = DuckDBEngine::new(Arc::new(EmptySource) as Arc<dyn TopicDataSource>).unwrap();

        engine
            .create_materialized_view("test_view", "SELECT 1 AS val", 60)
            .unwrap();

        // Refresh should succeed
        engine.refresh_materialized_view("test_view").await.unwrap();
    }

    #[tokio::test]
    async fn test_drop_nonexistent_view_returns_error() {
        let engine = DuckDBEngine::new(Arc::new(EmptySource) as Arc<dyn TopicDataSource>).unwrap();

        let result = engine.drop_materialized_view("no_such_view").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_refresh_nonexistent_view_returns_error() {
        let engine = DuckDBEngine::new(Arc::new(EmptySource) as Arc<dyn TopicDataSource>).unwrap();

        let result = engine.refresh_materialized_view("no_such_view").await;
        assert!(result.is_err());
    }

    // ── Cross-topic queries ──────────────────────────────────────────────

    #[tokio::test]
    async fn test_query_multiple_topics() {
        let engine = DuckDBEngine::new(Arc::new(TestSource) as Arc<dyn TopicDataSource>).unwrap();

        // Load both topics by referencing them
        let _ = engine
            .execute_query(
                "SELECT * FROM streamline_topic('events') LIMIT 1",
                no_cache_opts(),
            )
            .await;
        let _ = engine
            .execute_query(
                "SELECT * FROM streamline_topic('metrics') LIMIT 1",
                no_cache_opts(),
            )
            .await;

        // Now query both tables
        let events = engine
            .execute_query(
                "SELECT COUNT(*) AS cnt FROM streamline_topic_events",
                no_cache_opts(),
            )
            .await
            .unwrap();
        let metrics = engine
            .execute_query(
                "SELECT COUNT(*) AS cnt FROM streamline_topic_metrics",
                no_cache_opts(),
            )
            .await
            .unwrap();

        assert_eq!(events.rows[0].values[0], serde_json::json!(8));
        assert_eq!(metrics.rows[0].values[0], serde_json::json!(4));
    }

    // ── QueryResult metadata ─────────────────────────────────────────────

    #[tokio::test]
    async fn test_query_result_metadata() {
        let engine = DuckDBEngine::new(Arc::new(TestSource) as Arc<dyn TopicDataSource>).unwrap();

        let result = engine
            .execute_query(
                "SELECT * FROM streamline_topic_events",
                no_cache_opts(),
            )
            .await
            .unwrap();

        assert_eq!(result.row_count, 8);
        assert_eq!(result.total_rows, 8);
        assert!(!result.from_cache);
        assert!(!result.has_more);
        assert!(result.execution_time_ms < 10_000); // should be fast
        assert!(result.columns.contains(&"offset".to_string()));
        assert!(result.columns.contains(&"partition".to_string()));
        assert!(result.columns.contains(&"key".to_string()));
        assert!(result.columns.contains(&"value".to_string()));
    }

    // ── Error type properties ────────────────────────────────────────────

    #[test]
    fn test_analytics_error_is_retriable() {
        let timeout_err = AnalyticsError::query_timeout(5000);
        assert!(timeout_err.is_retriable());

        let sql_err = AnalyticsError::invalid_sql("SELECT", "syntax error");
        assert!(!sql_err.is_retriable());

        let topic_err = AnalyticsError::topic_not_found("nope");
        assert!(!topic_err.is_retriable());
    }

    #[test]
    fn test_analytics_error_display() {
        let err = AnalyticsError::query_timeout(3000);
        assert_eq!(err.to_string(), "Query timed out after 3000ms");

        let err = AnalyticsError::topic_not_found("orders");
        assert_eq!(err.to_string(), "Topic not found: orders");
    }
}
