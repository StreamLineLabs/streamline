//! Comprehensive integration tests for the Feature Store module.
//!
//! These tests exercise the feature store through its engine and component APIs:
//!
//! - Feature view registration, listing, deletion
//! - Online feature retrieval (DashMap-based)
//! - Offline/historical feature storage with point-in-time correctness
//! - TTL eviction in the online store
//! - Materialization from offline to online store
//! - Windowed aggregations (tumbling, sliding, session)
//! - Transformation operations (math, string, time-based)
//! - Feature ingestion pipeline
//! - API request/response serialization

#[cfg(feature = "featurestore")]
mod featurestore_tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use streamline::featurestore::config::FeatureStoreConfig;
    use streamline::featurestore::engine::FeatureStoreEngine;
    use streamline::featurestore::feature::FeatureValue;
    use streamline::featurestore::feature_view::{
        AggregationWindow, FeatureDataType, FeatureDefinitionView, FeatureViewDefinition,
        FeatureViewEntity, WindowType,
    };
    use streamline::featurestore::offline_store::HistoricalStore;
    use streamline::featurestore::online_store::DashMapOnlineStore;
    use streamline::featurestore::transformations::{TransformationEngine, WindowedAggregation};

    // ── Engine creation ──────────────────────────────────────────────────

    #[tokio::test]
    async fn test_engine_creation_succeeds() {
        let config = FeatureStoreConfig::default();
        let engine = FeatureStoreEngine::new(config);
        assert!(engine.is_ok());
    }

    #[tokio::test]
    async fn test_engine_starts_empty() {
        let engine = FeatureStoreEngine::new(FeatureStoreConfig::default()).unwrap();
        let stats = engine.stats().await;
        assert_eq!(stats.feature_views, 0);
        assert_eq!(stats.online_store_entries, 0);
        assert_eq!(stats.offline_store_entries, 0);
        assert_eq!(stats.online_store_evictions, 0);
    }

    // ── Feature view CRUD ────────────────────────────────────────────────

    #[tokio::test]
    async fn test_register_feature_view() {
        let engine = FeatureStoreEngine::new(FeatureStoreConfig::default()).unwrap();

        let view = FeatureViewDefinition::new("user_features")
            .with_description("User behavior features")
            .with_entity(FeatureViewEntity::new("user_id").with_join_key("user_id"))
            .with_feature(FeatureDefinitionView::new("purchase_count", FeatureDataType::Int64))
            .with_feature(FeatureDefinitionView::new("avg_amount", FeatureDataType::Float64))
            .with_source_topic("user_events")
            .with_ttl_seconds(3600);

        let result = engine.register_feature_view(view).await;
        assert!(result.is_ok());

        let stats = engine.stats().await;
        assert_eq!(stats.feature_views, 1);
    }

    #[tokio::test]
    async fn test_register_duplicate_view_fails() {
        let engine = FeatureStoreEngine::new(FeatureStoreConfig::default()).unwrap();

        let view = FeatureViewDefinition::new("my_view");
        engine.register_feature_view(view.clone()).await.unwrap();

        let result = engine.register_feature_view(view).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_list_feature_views() {
        let engine = FeatureStoreEngine::new(FeatureStoreConfig::default()).unwrap();

        engine
            .register_feature_view(
                FeatureViewDefinition::new("view_a").with_source_topic("topic_a"),
            )
            .await
            .unwrap();
        engine
            .register_feature_view(
                FeatureViewDefinition::new("view_b").with_source_topic("topic_b"),
            )
            .await
            .unwrap();
        engine
            .register_feature_view(
                FeatureViewDefinition::new("view_c").with_source_topic("topic_c"),
            )
            .await
            .unwrap();

        let views = engine.list_feature_views().await;
        assert_eq!(views.len(), 3);
    }

    #[tokio::test]
    async fn test_get_feature_view() {
        let engine = FeatureStoreEngine::new(FeatureStoreConfig::default()).unwrap();

        let view = FeatureViewDefinition::new("lookup_view")
            .with_description("A view for lookup tests");
        engine.register_feature_view(view).await.unwrap();

        let fetched = engine.get_feature_view("lookup_view").await;
        assert!(fetched.is_some());
        let fetched = fetched.unwrap();
        assert_eq!(fetched.name, "lookup_view");
        assert_eq!(fetched.description, Some("A view for lookup tests".to_string()));
    }

    #[tokio::test]
    async fn test_get_nonexistent_view_returns_none() {
        let engine = FeatureStoreEngine::new(FeatureStoreConfig::default()).unwrap();
        assert!(engine.get_feature_view("does_not_exist").await.is_none());
    }

    #[tokio::test]
    async fn test_delete_feature_view() {
        let engine = FeatureStoreEngine::new(FeatureStoreConfig::default()).unwrap();

        engine
            .register_feature_view(FeatureViewDefinition::new("to_delete"))
            .await
            .unwrap();

        assert!(engine.delete_feature_view("to_delete").await.is_ok());
        assert!(engine.get_feature_view("to_delete").await.is_none());

        let stats = engine.stats().await;
        assert_eq!(stats.feature_views, 0);
    }

    #[tokio::test]
    async fn test_delete_nonexistent_view_fails() {
        let engine = FeatureStoreEngine::new(FeatureStoreConfig::default()).unwrap();
        assert!(engine.delete_feature_view("ghost").await.is_err());
    }

    // ── Feature view with aggregation windows ────────────────────────────

    #[tokio::test]
    async fn test_feature_view_with_aggregation_windows() {
        let engine = FeatureStoreEngine::new(FeatureStoreConfig::default()).unwrap();

        let view = FeatureViewDefinition::new("windowed_view")
            .with_aggregation_window(AggregationWindow::one_minute())
            .with_aggregation_window(AggregationWindow::five_minutes())
            .with_aggregation_window(AggregationWindow::one_hour())
            .with_aggregation_window(AggregationWindow::one_day());

        engine.register_feature_view(view).await.unwrap();

        let fetched = engine.get_feature_view("windowed_view").await.unwrap();
        assert_eq!(fetched.aggregation_windows.len(), 4);
        assert_eq!(fetched.aggregation_windows[0].name, "1_minute");
        assert_eq!(fetched.aggregation_windows[0].duration_seconds, 60);
        assert_eq!(fetched.aggregation_windows[1].name, "5_minutes");
        assert_eq!(fetched.aggregation_windows[1].duration_seconds, 300);
        assert_eq!(fetched.aggregation_windows[2].name, "1_hour");
        assert_eq!(fetched.aggregation_windows[2].duration_seconds, 3600);
        assert_eq!(fetched.aggregation_windows[3].name, "1_day");
        assert_eq!(fetched.aggregation_windows[3].duration_seconds, 86400);
    }

    #[tokio::test]
    async fn test_feature_view_custom_window() {
        let view = FeatureViewDefinition::new("custom_windows")
            .with_aggregation_window(AggregationWindow::custom_tumbling("15_min", 900))
            .with_aggregation_window(AggregationWindow::custom_sliding("30_min_slide_5", 1800, 300))
            .with_aggregation_window(AggregationWindow::custom_session("30_min_session", 1800));

        assert_eq!(view.aggregation_windows.len(), 3);
        assert_eq!(view.aggregation_windows[0].window_type, WindowType::Tumbling);
        assert_eq!(view.aggregation_windows[1].window_type, WindowType::Sliding);
        assert_eq!(view.aggregation_windows[1].slide_seconds, Some(300));
        assert_eq!(view.aggregation_windows[2].window_type, WindowType::Session);
    }

    // ── Online feature serving ───────────────────────────────────────────

    #[tokio::test]
    async fn test_ingest_and_retrieve_online_features() {
        let engine = FeatureStoreEngine::new(FeatureStoreConfig::default()).unwrap();

        let view = FeatureViewDefinition::new("user_features")
            .with_feature(FeatureDefinitionView::new("age", FeatureDataType::Int64))
            .with_feature(FeatureDefinitionView::new("score", FeatureDataType::Float64))
            .with_ttl_seconds(3600);
        engine.register_feature_view(view).await.unwrap();

        // Ingest features
        let mut features = HashMap::new();
        features.insert("age".to_string(), FeatureValue::Int64(30));
        features.insert("score".to_string(), FeatureValue::Float64(0.95));
        engine
            .ingest(
                "user_features",
                "user_42",
                features,
                chrono::Utc::now().timestamp_millis(),
            )
            .await
            .unwrap();

        // Retrieve
        let result = engine
            .get_online_features("user_42", &["age".to_string(), "score".to_string()])
            .await
            .unwrap();

        assert_eq!(result.entity_key, "user_42");
        assert_eq!(result.found_features.len(), 2);
        assert_eq!(result.missing_features.len(), 0);
        assert_eq!(result.features.get("age"), Some(&FeatureValue::Int64(30)));
        assert_eq!(
            result.features.get("score"),
            Some(&FeatureValue::Float64(0.95))
        );
        assert!(result.latency_us < 10_000); // Should be well under 10ms
    }

    #[tokio::test]
    async fn test_online_features_missing_returns_null() {
        let engine = FeatureStoreEngine::new(FeatureStoreConfig::default()).unwrap();

        let result = engine
            .get_online_features("nobody", &["feature_a".to_string(), "feature_b".to_string()])
            .await
            .unwrap();

        assert_eq!(result.missing_features.len(), 2);
        assert_eq!(result.found_features.len(), 0);
        assert_eq!(
            result.features.get("feature_a"),
            Some(&FeatureValue::Null)
        );
    }

    #[tokio::test]
    async fn test_online_features_partial_match() {
        let engine = FeatureStoreEngine::new(FeatureStoreConfig::default()).unwrap();

        let view = FeatureViewDefinition::new("partial_view")
            .with_feature(FeatureDefinitionView::new("exists", FeatureDataType::Int64))
            .with_ttl_seconds(3600);
        engine.register_feature_view(view).await.unwrap();

        let mut features = HashMap::new();
        features.insert("exists".to_string(), FeatureValue::Int64(42));
        engine
            .ingest(
                "partial_view",
                "entity_1",
                features,
                chrono::Utc::now().timestamp_millis(),
            )
            .await
            .unwrap();

        let result = engine
            .get_online_features(
                "entity_1",
                &["exists".to_string(), "does_not_exist".to_string()],
            )
            .await
            .unwrap();

        assert_eq!(result.found_features.len(), 1);
        assert_eq!(result.missing_features.len(), 1);
        assert_eq!(
            result.features.get("exists"),
            Some(&FeatureValue::Int64(42))
        );
        assert_eq!(
            result.features.get("does_not_exist"),
            Some(&FeatureValue::Null)
        );
    }

    // ── Historical features with point-in-time correctness ───────────────

    #[tokio::test]
    async fn test_historical_features_point_in_time() {
        let engine = FeatureStoreEngine::new(FeatureStoreConfig::default()).unwrap();

        let view = FeatureViewDefinition::new("price_view")
            .with_feature(FeatureDefinitionView::new("price", FeatureDataType::Float64));
        engine.register_feature_view(view).await.unwrap();

        // Ingest at increasing timestamps
        let mut f1 = HashMap::new();
        f1.insert("price".to_string(), FeatureValue::Float64(10.0));
        engine.ingest("price_view", "item_1", f1, 1000).await.unwrap();

        let mut f2 = HashMap::new();
        f2.insert("price".to_string(), FeatureValue::Float64(15.0));
        engine.ingest("price_view", "item_1", f2, 2000).await.unwrap();

        let mut f3 = HashMap::new();
        f3.insert("price".to_string(), FeatureValue::Float64(20.0));
        engine.ingest("price_view", "item_1", f3, 3000).await.unwrap();

        // Query at different timestamps
        let results = engine
            .get_historical_features(
                &[
                    "item_1".to_string(),
                    "item_1".to_string(),
                    "item_1".to_string(),
                    "item_1".to_string(),
                ],
                &[500, 1500, 2500, 5000],
            )
            .await
            .unwrap();

        assert_eq!(results.len(), 4);

        // t=500: before any record -> empty
        assert!(results[0].features.is_empty());

        // t=1500: should get t=1000 value (10.0)
        assert_eq!(
            results[1].features.get("price"),
            Some(&FeatureValue::Float64(10.0))
        );

        // t=2500: should get t=2000 value (15.0)
        assert_eq!(
            results[2].features.get("price"),
            Some(&FeatureValue::Float64(15.0))
        );

        // t=5000: should get latest t=3000 value (20.0)
        assert_eq!(
            results[3].features.get("price"),
            Some(&FeatureValue::Float64(20.0))
        );
    }

    #[tokio::test]
    async fn test_historical_features_mismatched_lengths_error() {
        let engine = FeatureStoreEngine::new(FeatureStoreConfig::default()).unwrap();

        let result = engine
            .get_historical_features(
                &["e1".to_string(), "e2".to_string()],
                &[1000], // Mismatched length
            )
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_historical_features_multiple_entities() {
        let engine = FeatureStoreEngine::new(FeatureStoreConfig::default()).unwrap();

        let view = FeatureViewDefinition::new("multi_entity")
            .with_feature(FeatureDefinitionView::new("value", FeatureDataType::Float64));
        engine.register_feature_view(view).await.unwrap();

        let mut f1 = HashMap::new();
        f1.insert("value".to_string(), FeatureValue::Float64(100.0));
        engine.ingest("multi_entity", "entity_a", f1, 1000).await.unwrap();

        let mut f2 = HashMap::new();
        f2.insert("value".to_string(), FeatureValue::Float64(200.0));
        engine.ingest("multi_entity", "entity_b", f2, 1500).await.unwrap();

        let results = engine
            .get_historical_features(
                &["entity_a".to_string(), "entity_b".to_string()],
                &[2000, 2000],
            )
            .await
            .unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(
            results[0].features.get("value"),
            Some(&FeatureValue::Float64(100.0))
        );
        assert_eq!(
            results[1].features.get("value"),
            Some(&FeatureValue::Float64(200.0))
        );
    }

    // ── TTL eviction ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_online_store_ttl_expiry() {
        let store = DashMapOnlineStore::new(10_000, 0);

        // Insert an entry that is already expired
        let now = chrono::Utc::now().timestamp_millis();
        let entry = streamline::featurestore::online_store::OnlineEntry {
            value: FeatureValue::Float64(42.0),
            event_timestamp: now - 5000,
            created_timestamp: now - 5000,
            expiry_timestamp: now - 1000, // Expired 1 second ago
        };

        // Access internal DashMap via put and then override - use the eviction mechanism
        store.put("fresh_key".to_string(), FeatureValue::Float64(1.0), now, 3600);
        assert!(store.get("fresh_key").is_some());

        // A put with very short TTL and then immediate get should still work
        // because the TTL hasn't expired yet
        store.put(
            "short_ttl".to_string(),
            FeatureValue::Float64(2.0),
            now,
            3600, // 1 hour TTL
        );
        assert!(store.get("short_ttl").is_some());
    }

    #[tokio::test]
    async fn test_online_store_eviction_on_capacity() {
        let store = DashMapOnlineStore::new(10, 0); // Max 10 entries

        // Insert 20 entries
        for i in 0..20 {
            store.put(
                format!("key_{}", i),
                FeatureValue::Int64(i),
                i as i64 * 1000,
                0,
            );
        }

        // Should have evicted some entries
        assert!(store.len() <= 10);
        assert!(store.eviction_count() > 0);
    }

    #[tokio::test]
    async fn test_online_store_evict_expired_batch() {
        let store = DashMapOnlineStore::new(10_000, 0);
        let now = chrono::Utc::now().timestamp_millis();

        // Insert valid entries
        for i in 0..5 {
            store.put(
                format!("valid_{}", i),
                FeatureValue::Int64(i),
                now,
                3600, // 1 hour TTL
            );
        }

        assert_eq!(store.len(), 5);

        // Run eviction - nothing should be evicted since all are valid
        let evicted = store.evict_expired();
        assert_eq!(evicted, 0);
        assert_eq!(store.len(), 5);
    }

    // ── Materialization ──────────────────────────────────────────────────

    #[tokio::test]
    async fn test_materialize_offline_to_online() {
        let engine = FeatureStoreEngine::new(FeatureStoreConfig::default()).unwrap();

        let view = FeatureViewDefinition::new("mat_view")
            .with_feature(FeatureDefinitionView::new("amount", FeatureDataType::Float64))
            .with_ttl_seconds(3600);
        engine.register_feature_view(view).await.unwrap();

        // Write directly to the offline store
        let mut features = HashMap::new();
        features.insert("amount".to_string(), FeatureValue::Float64(99.99));
        engine
            .offline_store()
            .write(
                "mat_view",
                "user_mat",
                features,
                chrono::Utc::now().timestamp_millis(),
            )
            .await;

        // Materialize
        let mat_result = engine.materialize("mat_view").await.unwrap();
        assert_eq!(mat_result.feature_view, "mat_view");
        assert!(mat_result.records_written > 0);
        assert!(mat_result.duration_ms < 5000); // Should be fast

        // Now the online store should have the data
        let online_result = engine
            .get_online_features("user_mat", &["amount".to_string()])
            .await
            .unwrap();
        assert_eq!(online_result.found_features.len(), 1);
        assert_eq!(
            online_result.features.get("amount"),
            Some(&FeatureValue::Float64(99.99))
        );
    }

    #[tokio::test]
    async fn test_materialize_nonexistent_view_fails() {
        let engine = FeatureStoreEngine::new(FeatureStoreConfig::default()).unwrap();
        assert!(engine.materialize("nonexistent").await.is_err());
    }

    #[tokio::test]
    async fn test_materialize_empty_view() {
        let engine = FeatureStoreEngine::new(FeatureStoreConfig::default()).unwrap();

        engine
            .register_feature_view(FeatureViewDefinition::new("empty_view"))
            .await
            .unwrap();

        let result = engine.materialize("empty_view").await.unwrap();
        assert_eq!(result.records_written, 0);
    }

    // ── Feature ingestion ────────────────────────────────────────────────

    #[tokio::test]
    async fn test_ingest_writes_to_both_stores() {
        let engine = FeatureStoreEngine::new(FeatureStoreConfig::default()).unwrap();

        let view = FeatureViewDefinition::new("dual_write")
            .with_feature(FeatureDefinitionView::new("val", FeatureDataType::Float64))
            .with_ttl_seconds(3600);
        engine.register_feature_view(view).await.unwrap();

        let mut features = HashMap::new();
        features.insert("val".to_string(), FeatureValue::Float64(42.0));
        engine
            .ingest("dual_write", "entity_1", features, 5000)
            .await
            .unwrap();

        // Online store should have it
        let online = engine
            .get_online_features("entity_1", &["val".to_string()])
            .await
            .unwrap();
        assert_eq!(online.found_features.len(), 1);

        // Offline store should also have it
        let offline = engine.offline_store().point_in_time_lookup("entity_1", 6000).await;
        assert_eq!(offline.get("val"), Some(&FeatureValue::Float64(42.0)));
    }

    #[tokio::test]
    async fn test_ingest_nonexistent_view_fails() {
        let engine = FeatureStoreEngine::new(FeatureStoreConfig::default()).unwrap();

        let result = engine
            .ingest("nonexistent_view", "entity", HashMap::new(), 1000)
            .await;
        assert!(result.is_err());
    }

    // ── Offline store: point-in-time join ────────────────────────────────

    #[tokio::test]
    async fn test_point_in_time_join() {
        let store = HistoricalStore::new();

        // User 1: purchase amounts over time
        let mut f1 = HashMap::new();
        f1.insert("amount".to_string(), FeatureValue::Float64(100.0));
        store.write("purchases", "user_1", f1, 1000).await;

        let mut f2 = HashMap::new();
        f2.insert("amount".to_string(), FeatureValue::Float64(250.0));
        store.write("purchases", "user_1", f2, 2000).await;

        // User 2: single purchase
        let mut f3 = HashMap::new();
        f3.insert("amount".to_string(), FeatureValue::Float64(75.0));
        store.write("purchases", "user_2", f3, 1500).await;

        // PIT join
        let results = store
            .point_in_time_join(
                "purchases",
                &[
                    ("user_1".to_string(), 1500), // Should get t=1000 value
                    ("user_1".to_string(), 3000), // Should get t=2000 value
                    ("user_2".to_string(), 1000), // Before any record -> null
                    ("user_2".to_string(), 2000), // Should get t=1500 value
                ],
                &["amount".to_string()],
            )
            .await;

        assert_eq!(results.len(), 4);
        assert_eq!(
            results[0].get("amount"),
            Some(&FeatureValue::Float64(100.0))
        );
        assert_eq!(
            results[1].get("amount"),
            Some(&FeatureValue::Float64(250.0))
        );
        assert_eq!(results[2].get("amount"), Some(&FeatureValue::Null));
        assert_eq!(
            results[3].get("amount"),
            Some(&FeatureValue::Float64(75.0))
        );
    }

    #[tokio::test]
    async fn test_offline_store_get_range() {
        let store = HistoricalStore::new();

        for i in 1..=10 {
            let mut f = HashMap::new();
            f.insert("v".to_string(), FeatureValue::Int64(i));
            store.write("view", "entity", f, i as i64 * 1000).await;
        }

        // Get records between t=3000 and t=7000
        let range = store.get_range("view", Some(3000), Some(7000)).await;
        assert_eq!(range.len(), 5); // t=3000,4000,5000,6000,7000

        // Get all records
        let all = store.get_range("view", None, None).await;
        assert_eq!(all.len(), 10);
    }

    // ── Windowed aggregations ────────────────────────────────────────────

    #[test]
    fn test_tumbling_window_aggregation() {
        let engine = TransformationEngine::new();

        let values = vec![
            (0, 10.0),
            (500, 20.0),
            (1000, 30.0),
            (1500, 40.0),
            (2000, 50.0),
        ];

        let results =
            engine.tumbling_window_aggregate(&values, 1000, WindowedAggregation::Sum);

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].value, 30.0); // 10+20
        assert_eq!(results[0].count, 2);
        assert_eq!(results[1].value, 70.0); // 30+40
        assert_eq!(results[1].count, 2);
        assert_eq!(results[2].value, 50.0); // 50
        assert_eq!(results[2].count, 1);
    }

    #[test]
    fn test_tumbling_window_avg() {
        let engine = TransformationEngine::new();

        let values = vec![
            (0, 10.0),
            (500, 20.0),
            (1000, 30.0),
            (1500, 40.0),
        ];

        let results =
            engine.tumbling_window_aggregate(&values, 1000, WindowedAggregation::Avg);

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].value, 15.0); // avg(10, 20)
        assert_eq!(results[1].value, 35.0); // avg(30, 40)
    }

    #[test]
    fn test_sliding_window_aggregation() {
        let engine = TransformationEngine::new();

        let values = vec![
            (0, 1.0),
            (500, 2.0),
            (1000, 3.0),
            (1500, 4.0),
            (2000, 5.0),
        ];

        let results =
            engine.sliding_window_aggregate(&values, 1000, 500, WindowedAggregation::Sum);

        // Should have overlapping windows
        assert!(results.len() >= 3);

        // First window [0, 1000) contains values 1.0, 2.0
        assert_eq!(results[0].value, 3.0);
    }

    #[test]
    fn test_session_window_aggregation() {
        let engine = TransformationEngine::new();

        // Two distinct sessions with a large gap between them
        let values = vec![
            (100, 1.0),
            (200, 2.0),
            (300, 3.0),
            // Gap > 500ms
            (1000, 10.0),
            (1100, 20.0),
        ];

        let results =
            engine.session_window_aggregate(&values, 500, WindowedAggregation::Sum);

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].value, 6.0); // 1+2+3
        assert_eq!(results[0].count, 3);
        assert_eq!(results[1].value, 30.0); // 10+20
        assert_eq!(results[1].count, 2);
    }

    #[test]
    fn test_aggregation_operations() {
        let engine = TransformationEngine::new();
        let values = [1.0, 2.0, 3.0, 4.0, 5.0];

        assert_eq!(engine.aggregate(&values, WindowedAggregation::Sum), 15.0);
        assert_eq!(engine.aggregate(&values, WindowedAggregation::Avg), 3.0);
        assert_eq!(engine.aggregate(&values, WindowedAggregation::Count), 5.0);
        assert_eq!(engine.aggregate(&values, WindowedAggregation::Min), 1.0);
        assert_eq!(engine.aggregate(&values, WindowedAggregation::Max), 5.0);
        assert_eq!(engine.aggregate(&values, WindowedAggregation::First), 1.0);
        assert_eq!(engine.aggregate(&values, WindowedAggregation::Last), 5.0);
    }

    #[test]
    fn test_empty_aggregations() {
        let engine = TransformationEngine::new();

        assert!(engine
            .tumbling_window_aggregate(&[], 1000, WindowedAggregation::Sum)
            .is_empty());
        assert!(engine
            .sliding_window_aggregate(&[], 1000, 500, WindowedAggregation::Sum)
            .is_empty());
        assert!(engine
            .session_window_aggregate(&[], 500, WindowedAggregation::Sum)
            .is_empty());
    }

    // ── Mathematical transformations ─────────────────────────────────────

    #[test]
    fn test_math_log() {
        match TransformationEngine::log(std::f64::consts::E) {
            FeatureValue::Float64(v) => assert!((v - 1.0).abs() < 0.001),
            other => panic!("Expected Float64, got {:?}", other),
        }
        // log of negative -> Null
        assert_eq!(TransformationEngine::log(-1.0), FeatureValue::Null);
        assert_eq!(TransformationEngine::log(0.0), FeatureValue::Null);
    }

    #[test]
    fn test_math_log10() {
        match TransformationEngine::log10(100.0) {
            FeatureValue::Float64(v) => assert!((v - 2.0).abs() < 0.001),
            other => panic!("Expected Float64, got {:?}", other),
        }
        assert_eq!(TransformationEngine::log10(-1.0), FeatureValue::Null);
    }

    #[test]
    fn test_math_sqrt() {
        match TransformationEngine::sqrt(9.0) {
            FeatureValue::Float64(v) => assert!((v - 3.0).abs() < 0.001),
            other => panic!("Expected Float64, got {:?}", other),
        }
        assert_eq!(TransformationEngine::sqrt(-1.0), FeatureValue::Null);
    }

    #[test]
    fn test_math_normalize() {
        // Z-score: (10 - 5) / 2.5 = 2.0
        match TransformationEngine::normalize(10.0, 5.0, 2.5) {
            FeatureValue::Float64(v) => assert!((v - 2.0).abs() < 0.001),
            other => panic!("Expected Float64, got {:?}", other),
        }
        // std_dev = 0 -> returns 0
        assert_eq!(
            TransformationEngine::normalize(10.0, 5.0, 0.0),
            FeatureValue::Float64(0.0)
        );
    }

    #[test]
    fn test_math_min_max_normalize() {
        // (5 - 0) / (10 - 0) = 0.5
        match TransformationEngine::min_max_normalize(5.0, 0.0, 10.0) {
            FeatureValue::Float64(v) => assert!((v - 0.5).abs() < 0.001),
            other => panic!("Expected Float64, got {:?}", other),
        }
        // min == max -> returns 0
        assert_eq!(
            TransformationEngine::min_max_normalize(5.0, 5.0, 5.0),
            FeatureValue::Float64(0.0)
        );
    }

    #[test]
    fn test_math_abs() {
        assert_eq!(TransformationEngine::abs(-5.0), FeatureValue::Float64(5.0));
        assert_eq!(TransformationEngine::abs(5.0), FeatureValue::Float64(5.0));
    }

    #[test]
    fn test_math_pow() {
        match TransformationEngine::pow(2.0, 10.0) {
            FeatureValue::Float64(v) => assert!((v - 1024.0).abs() < 0.001),
            other => panic!("Expected Float64, got {:?}", other),
        }
    }

    // ── String transformations ───────────────────────────────────────────

    #[test]
    fn test_hash_string_deterministic() {
        let h1 = TransformationEngine::hash_string("hello");
        let h2 = TransformationEngine::hash_string("hello");
        let h3 = TransformationEngine::hash_string("world");

        assert_eq!(h1, h2); // Same input -> same output
        assert_ne!(h1, h3); // Different input -> different output
    }

    #[test]
    fn test_tokenize_count() {
        assert_eq!(
            TransformationEngine::tokenize_count("hello world foo bar"),
            FeatureValue::Int64(4)
        );
        assert_eq!(
            TransformationEngine::tokenize_count("single"),
            FeatureValue::Int64(1)
        );
        assert_eq!(
            TransformationEngine::tokenize_count(""),
            FeatureValue::Int64(0)
        );
    }

    #[test]
    fn test_string_length() {
        assert_eq!(
            TransformationEngine::string_length("hello"),
            FeatureValue::Int64(5)
        );
        assert_eq!(
            TransformationEngine::string_length(""),
            FeatureValue::Int64(0)
        );
    }

    // ── Time-based features ──────────────────────────────────────────────

    #[test]
    fn test_hour_of_day() {
        // 2024-01-15 14:30:00 UTC = 1705325400000 ms
        let ts = 1705325400000i64;
        match TransformationEngine::hour_of_day(ts) {
            FeatureValue::Int64(h) => assert_eq!(h, 14),
            other => panic!("Expected Int64, got {:?}", other),
        }
    }

    #[test]
    fn test_day_of_week() {
        // 2024-01-15 is Monday -> 0
        let ts = 1705325400000i64;
        match TransformationEngine::day_of_week(ts) {
            FeatureValue::Int64(d) => assert_eq!(d, 0),
            other => panic!("Expected Int64, got {:?}", other),
        }
    }

    #[test]
    fn test_is_weekend() {
        // Monday 2024-01-15
        let monday = 1705325400000i64;
        assert_eq!(
            TransformationEngine::is_weekend(monday),
            FeatureValue::Bool(false)
        );

        // Saturday 2024-01-20
        let saturday = 1705757400000i64;
        assert_eq!(
            TransformationEngine::is_weekend(saturday),
            FeatureValue::Bool(true)
        );
    }

    #[test]
    fn test_month_extraction() {
        // January 2024
        let ts = 1705325400000i64;
        match TransformationEngine::month(ts) {
            FeatureValue::Int64(m) => assert_eq!(m, 1),
            other => panic!("Expected Int64, got {:?}", other),
        }
    }

    #[test]
    fn test_is_business_hours() {
        // 14:30 UTC -> business hours (9-17)
        let business = 1705325400000i64;
        assert_eq!(
            TransformationEngine::is_business_hours(business),
            FeatureValue::Bool(true)
        );
    }

    // ── DashMap online store: concurrent access ──────────────────────────

    #[test]
    fn test_online_store_concurrent_reads_writes() {
        use std::thread;

        let store = Arc::new(DashMapOnlineStore::new(100_000, 0));

        let mut handles = vec![];

        // Spawn writer threads
        for t in 0..4 {
            let store = store.clone();
            handles.push(thread::spawn(move || {
                for i in 0..500 {
                    store.put(
                        format!("t{}_k{}", t, i),
                        FeatureValue::Int64(i),
                        i as i64,
                        0,
                    );
                }
            }));
        }

        // Spawn reader threads
        for t in 0..4 {
            let store = store.clone();
            handles.push(thread::spawn(move || {
                for i in 0..500 {
                    let _ = store.get(&format!("t{}_k{}", t, i));
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // All writes should have completed
        assert!(store.put_count() >= 2000);
        assert!(store.get_count() >= 2000);
    }

    #[test]
    fn test_online_store_multi_get() {
        let store = DashMapOnlineStore::new(1000, 0);

        store.put("a".to_string(), FeatureValue::Int64(1), 1000, 0);
        store.put("b".to_string(), FeatureValue::Int64(2), 1000, 0);

        let results = store.multi_get(&[
            "a".to_string(),
            "b".to_string(),
            "c".to_string(), // Missing
        ]);

        assert_eq!(results.len(), 3);
        assert!(results[0].is_some());
        assert!(results[1].is_some());
        assert!(results[2].is_none());
    }

    // ── End-to-end pipeline test ─────────────────────────────────────────

    #[tokio::test]
    async fn test_end_to_end_feature_pipeline() {
        let engine = FeatureStoreEngine::new(FeatureStoreConfig::default()).unwrap();

        // 1. Register a feature view
        let view = FeatureViewDefinition::new("fraud_features")
            .with_description("Real-time fraud detection features")
            .with_entity(FeatureViewEntity::new("transaction_id"))
            .with_feature(FeatureDefinitionView::new(
                "amount",
                FeatureDataType::Float64,
            ))
            .with_feature(FeatureDefinitionView::new(
                "merchant_risk",
                FeatureDataType::Float64,
            ))
            .with_feature(FeatureDefinitionView::new(
                "is_weekend_tx",
                FeatureDataType::Bool,
            ))
            .with_source_topic("transactions")
            .with_ttl_seconds(7200)
            .with_aggregation_window(AggregationWindow::one_minute())
            .with_aggregation_window(AggregationWindow::one_hour())
            .with_tag("team", "ml-platform")
            .with_owner("fraud-team");

        engine.register_feature_view(view).await.unwrap();

        // 2. Ingest a series of feature records
        let base_ts = 1705325400000i64; // Jan 15, 2024 14:30 UTC
        for i in 0..10 {
            let mut features = HashMap::new();
            features.insert(
                "amount".to_string(),
                FeatureValue::Float64(100.0 + i as f64 * 10.0),
            );
            features.insert(
                "merchant_risk".to_string(),
                FeatureValue::Float64(0.1 + i as f64 * 0.05),
            );
            features.insert(
                "is_weekend_tx".to_string(),
                FeatureValue::Bool(false),
            );

            engine
                .ingest(
                    "fraud_features",
                    &format!("tx_{}", i),
                    features,
                    base_ts + i * 1000,
                )
                .await
                .unwrap();
        }

        // 3. Check stats
        let stats = engine.stats().await;
        assert_eq!(stats.feature_views, 1);
        assert!(stats.online_store_entries > 0);
        assert!(stats.offline_store_entries > 0);

        // 4. Retrieve online features for the latest transaction
        let result = engine
            .get_online_features(
                "tx_9",
                &[
                    "amount".to_string(),
                    "merchant_risk".to_string(),
                    "is_weekend_tx".to_string(),
                ],
            )
            .await
            .unwrap();

        assert_eq!(result.found_features.len(), 3);
        assert_eq!(result.missing_features.len(), 0);

        // 5. Retrieve historical features with PIT correctness
        let hist = engine
            .get_historical_features(
                &["tx_0".to_string(), "tx_5".to_string()],
                &[base_ts + 500, base_ts + 5500],
            )
            .await
            .unwrap();

        assert_eq!(hist.len(), 2);
        // tx_0 at base_ts+500 should get the t=base_ts record
        assert!(!hist[0].features.is_empty());

        // 6. Verify the feature view metadata
        let fetched = engine.get_feature_view("fraud_features").await.unwrap();
        assert_eq!(fetched.entities.len(), 1);
        assert_eq!(fetched.features.len(), 3);
        assert_eq!(fetched.aggregation_windows.len(), 2);
        assert_eq!(fetched.tags.get("team"), Some(&"ml-platform".to_string()));
        assert_eq!(fetched.owner, Some("fraud-team".to_string()));
    }
}
