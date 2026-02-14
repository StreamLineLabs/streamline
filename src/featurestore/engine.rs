//! Feature Store Engine
//!
//! Central engine for managing feature computation pipelines, including
//! registration, online/offline retrieval, and materialization.

use super::config::FeatureStoreConfig;
use super::entity::EntityKey;
use super::feature::FeatureValue;
use super::feature_view::{AggregationWindow, FeatureViewDefinition};
use super::offline_store::HistoricalStore;
use super::online_store::DashMapOnlineStore;
use super::transformations::TransformationEngine;
use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

/// Feature Store Engine that manages feature computation pipelines
pub struct FeatureStoreEngine {
    /// Configuration
    config: FeatureStoreConfig,
    /// Registered feature views
    views: Arc<RwLock<HashMap<String, FeatureViewDefinition>>>,
    /// DashMap-based online store for low-latency lookups
    online_store: Arc<DashMapOnlineStore>,
    /// Historical/offline store
    offline_store: Arc<HistoricalStore>,
    /// Transformation engine for feature computation
    transformation_engine: Arc<TransformationEngine>,
}

impl FeatureStoreEngine {
    /// Create a new feature store engine
    pub fn new(config: FeatureStoreConfig) -> Result<Self> {
        let online_store = Arc::new(DashMapOnlineStore::new(
            config.online.max_entries,
            config.online.ttl_seconds,
        ));
        let offline_store = Arc::new(HistoricalStore::new());
        let transformation_engine = Arc::new(TransformationEngine::new());

        Ok(Self {
            config,
            views: Arc::new(RwLock::new(HashMap::new())),
            online_store,
            offline_store,
            transformation_engine,
        })
    }

    /// Register a feature view definition
    pub async fn register_feature_view(&self, view: FeatureViewDefinition) -> Result<()> {
        let mut views = self.views.write().await;
        if views.contains_key(&view.name) {
            return Err(StreamlineError::Config(format!(
                "Feature view already exists: {}",
                view.name
            )));
        }
        info!(
            name = %view.name,
            features = view.features.len(),
            entities = view.entities.len(),
            "Registered feature view"
        );
        views.insert(view.name.clone(), view);
        Ok(())
    }

    /// Update an existing feature view
    pub async fn update_feature_view(&self, view: FeatureViewDefinition) -> Result<()> {
        let mut views = self.views.write().await;
        if !views.contains_key(&view.name) {
            return Err(StreamlineError::Config(format!(
                "Feature view not found: {}",
                view.name
            )));
        }
        views.insert(view.name.clone(), view);
        Ok(())
    }

    /// Delete a feature view
    pub async fn delete_feature_view(&self, name: &str) -> Result<()> {
        let mut views = self.views.write().await;
        if views.remove(name).is_none() {
            return Err(StreamlineError::Config(format!(
                "Feature view not found: {}",
                name
            )));
        }
        Ok(())
    }

    /// Get a feature view by name
    pub async fn get_feature_view(&self, name: &str) -> Option<FeatureViewDefinition> {
        let views = self.views.read().await;
        views.get(name).cloned()
    }

    /// List all feature views
    pub async fn list_feature_views(&self) -> Vec<FeatureViewDefinition> {
        let views = self.views.read().await;
        views.values().cloned().collect()
    }

    /// Point-in-time lookup for online features
    ///
    /// Retrieves the latest feature values for a given entity key,
    /// optionally filtered to specific feature names.
    pub async fn get_online_features(
        &self,
        entity_key: &str,
        feature_names: &[String],
    ) -> Result<OnlineFeatureResult> {
        let start = std::time::Instant::now();

        let mut values = HashMap::new();
        let mut found = Vec::new();
        let mut missing = Vec::new();

        for name in feature_names {
            let lookup_key = format!("{}:{}", entity_key, name);
            if let Some(entry) = self.online_store.get(&lookup_key) {
                values.insert(name.clone(), entry.value);
                found.push(name.clone());
            } else {
                values.insert(name.clone(), FeatureValue::Null);
                missing.push(name.clone());
            }
        }

        Ok(OnlineFeatureResult {
            entity_key: entity_key.to_string(),
            features: values,
            found_features: found,
            missing_features: missing,
            latency_us: start.elapsed().as_micros() as u64,
        })
    }

    /// Historical feature retrieval with point-in-time correctness
    ///
    /// For each (entity_key, timestamp) pair, returns the latest feature values
    /// that were valid at or before the given timestamp.
    pub async fn get_historical_features(
        &self,
        entity_keys: &[String],
        timestamps: &[i64],
    ) -> Result<Vec<HistoricalFeatureResult>> {
        if entity_keys.len() != timestamps.len() {
            return Err(StreamlineError::Config(
                "entity_keys and timestamps must have the same length".to_string(),
            ));
        }

        let mut results = Vec::with_capacity(entity_keys.len());

        for (entity_key, timestamp) in entity_keys.iter().zip(timestamps.iter()) {
            let features = self
                .offline_store
                .point_in_time_lookup(entity_key, *timestamp)
                .await;

            results.push(HistoricalFeatureResult {
                entity_key: entity_key.clone(),
                event_timestamp: *timestamp,
                features,
            });
        }

        Ok(results)
    }

    /// Materialize features from the offline store into the online store
    ///
    /// Reads historical data for the given feature view and writes the latest
    /// values into the online store for fast serving.
    pub async fn materialize(&self, feature_view: &str) -> Result<MaterializationResult> {
        let start = std::time::Instant::now();

        let view = {
            let views = self.views.read().await;
            views.get(feature_view).cloned().ok_or_else(|| {
                StreamlineError::Config(format!("Feature view not found: {}", feature_view))
            })?
        };

        // Get all records from the offline store for this view
        let records = self.offline_store.get_all_for_view(feature_view).await;

        let mut records_written = 0u64;
        let ttl_seconds = view.ttl_seconds.unwrap_or(self.config.online.ttl_seconds);

        for (entity_key, features) in &records {
            for (feature_name, value) in features {
                let key = format!("{}:{}", entity_key, feature_name);
                self.online_store.put(
                    key,
                    value.clone(),
                    chrono::Utc::now().timestamp_millis(),
                    ttl_seconds,
                );
                records_written += 1;
            }
        }

        Ok(MaterializationResult {
            feature_view: feature_view.to_string(),
            records_written,
            duration_ms: start.elapsed().as_millis() as u64,
        })
    }

    /// Ingest a feature record into both online and offline stores
    pub async fn ingest(
        &self,
        feature_view: &str,
        entity_key: &str,
        features: HashMap<String, FeatureValue>,
        event_timestamp: i64,
    ) -> Result<()> {
        let view = {
            let views = self.views.read().await;
            views.get(feature_view).cloned().ok_or_else(|| {
                StreamlineError::Config(format!("Feature view not found: {}", feature_view))
            })?
        };

        let ttl_seconds = view.ttl_seconds.unwrap_or(self.config.online.ttl_seconds);

        // Write to online store
        for (name, value) in &features {
            let key = format!("{}:{}", entity_key, name);
            self.online_store
                .put(key, value.clone(), event_timestamp, ttl_seconds);
        }

        // Write to offline store
        self.offline_store
            .write(feature_view, entity_key, features, event_timestamp)
            .await;

        Ok(())
    }

    /// Get the online store reference
    pub fn online_store(&self) -> &Arc<DashMapOnlineStore> {
        &self.online_store
    }

    /// Get the offline store reference
    pub fn offline_store(&self) -> &Arc<HistoricalStore> {
        &self.offline_store
    }

    /// Get the transformation engine reference
    pub fn transformation_engine(&self) -> &Arc<TransformationEngine> {
        &self.transformation_engine
    }

    /// Get engine statistics
    pub async fn stats(&self) -> EngineStats {
        let views = self.views.read().await;
        EngineStats {
            feature_views: views.len(),
            online_store_entries: self.online_store.len(),
            offline_store_entries: self.offline_store.len().await,
            online_store_evictions: self.online_store.eviction_count(),
        }
    }
}

/// Result of an online feature lookup
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OnlineFeatureResult {
    /// Entity key
    pub entity_key: String,
    /// Feature name -> value map
    pub features: HashMap<String, FeatureValue>,
    /// Features that were found
    pub found_features: Vec<String>,
    /// Features that were missing
    pub missing_features: Vec<String>,
    /// Latency in microseconds
    pub latency_us: u64,
}

/// Result of a historical feature lookup
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoricalFeatureResult {
    /// Entity key
    pub entity_key: String,
    /// Point-in-time timestamp
    pub event_timestamp: i64,
    /// Feature name -> value map
    pub features: HashMap<String, FeatureValue>,
}

/// Materialization result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializationResult {
    /// Feature view name
    pub feature_view: String,
    /// Number of records written to online store
    pub records_written: u64,
    /// Duration in milliseconds
    pub duration_ms: u64,
}

/// Engine statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineStats {
    /// Number of registered feature views
    pub feature_views: usize,
    /// Number of entries in the online store
    pub online_store_entries: usize,
    /// Number of entries in the offline store
    pub offline_store_entries: usize,
    /// Number of evictions from the online store
    pub online_store_evictions: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::featurestore::feature_view::{
        AggregationType, FeatureDefinitionView, FeatureViewDefinition, FeatureViewEntity,
    };

    fn test_config() -> FeatureStoreConfig {
        FeatureStoreConfig::default()
    }

    fn test_view() -> FeatureViewDefinition {
        FeatureViewDefinition::new("user_features")
            .with_description("User feature view for ML models")
            .with_entity(FeatureViewEntity::new("user_id"))
            .with_feature(FeatureDefinitionView::new(
                "avg_purchase_amount",
                super::super::feature_view::FeatureDataType::Float64,
            ))
            .with_feature(FeatureDefinitionView::new(
                "login_count_24h",
                super::super::feature_view::FeatureDataType::Int64,
            ))
            .with_source_topic("user_events")
            .with_ttl_seconds(3600)
    }

    #[tokio::test]
    async fn test_engine_creation() {
        let engine = FeatureStoreEngine::new(test_config());
        assert!(engine.is_ok());
    }

    #[tokio::test]
    async fn test_register_feature_view() {
        let engine = FeatureStoreEngine::new(test_config()).unwrap();
        let view = test_view();

        let result = engine.register_feature_view(view).await;
        assert!(result.is_ok());

        let stats = engine.stats().await;
        assert_eq!(stats.feature_views, 1);
    }

    #[tokio::test]
    async fn test_duplicate_feature_view() {
        let engine = FeatureStoreEngine::new(test_config()).unwrap();
        let view = test_view();

        engine.register_feature_view(view.clone()).await.unwrap();
        let result = engine.register_feature_view(view).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_online_features() {
        let engine = FeatureStoreEngine::new(test_config()).unwrap();
        let view = test_view();
        engine.register_feature_view(view).await.unwrap();

        // Ingest some features
        let mut features = HashMap::new();
        features.insert(
            "avg_purchase_amount".to_string(),
            FeatureValue::Float64(42.50),
        );
        features.insert("login_count_24h".to_string(), FeatureValue::Int64(5));

        engine
            .ingest(
                "user_features",
                "user_123",
                features,
                chrono::Utc::now().timestamp_millis(),
            )
            .await
            .unwrap();

        // Retrieve online features
        let result = engine
            .get_online_features(
                "user_123",
                &[
                    "avg_purchase_amount".to_string(),
                    "login_count_24h".to_string(),
                ],
            )
            .await
            .unwrap();

        assert_eq!(result.found_features.len(), 2);
        assert_eq!(result.missing_features.len(), 0);
        assert_eq!(
            result.features.get("avg_purchase_amount"),
            Some(&FeatureValue::Float64(42.50))
        );
    }

    #[tokio::test]
    async fn test_get_online_features_missing() {
        let engine = FeatureStoreEngine::new(test_config()).unwrap();

        let result = engine
            .get_online_features("nonexistent", &["some_feature".to_string()])
            .await
            .unwrap();

        assert_eq!(result.missing_features.len(), 1);
        assert_eq!(
            result.features.get("some_feature"),
            Some(&FeatureValue::Null)
        );
    }

    #[tokio::test]
    async fn test_get_historical_features() {
        let engine = FeatureStoreEngine::new(test_config()).unwrap();
        let view = test_view();
        engine.register_feature_view(view).await.unwrap();

        // Ingest features at different timestamps
        let mut features_t1 = HashMap::new();
        features_t1.insert(
            "avg_purchase_amount".to_string(),
            FeatureValue::Float64(30.0),
        );
        engine
            .ingest("user_features", "user_123", features_t1, 1000)
            .await
            .unwrap();

        let mut features_t2 = HashMap::new();
        features_t2.insert(
            "avg_purchase_amount".to_string(),
            FeatureValue::Float64(50.0),
        );
        engine
            .ingest("user_features", "user_123", features_t2, 2000)
            .await
            .unwrap();

        // Query at timestamp 1500 should return t1 values
        let results = engine
            .get_historical_features(&["user_123".to_string()], &[1500])
            .await
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].features.get("avg_purchase_amount"),
            Some(&FeatureValue::Float64(30.0))
        );
    }

    #[tokio::test]
    async fn test_materialize() {
        let engine = FeatureStoreEngine::new(test_config()).unwrap();
        let view = test_view();
        engine.register_feature_view(view).await.unwrap();

        // Write to offline store directly
        let mut features = HashMap::new();
        features.insert(
            "avg_purchase_amount".to_string(),
            FeatureValue::Float64(99.0),
        );
        engine
            .offline_store()
            .write(
                "user_features",
                "user_456",
                features,
                chrono::Utc::now().timestamp_millis(),
            )
            .await;

        // Materialize
        let result = engine.materialize("user_features").await.unwrap();
        assert!(result.records_written > 0);
        assert!(result.duration_ms < 1000);

        // Verify online store has the data
        let online = engine
            .get_online_features("user_456", &["avg_purchase_amount".to_string()])
            .await
            .unwrap();
        assert_eq!(online.found_features.len(), 1);
    }

    #[tokio::test]
    async fn test_list_feature_views() {
        let engine = FeatureStoreEngine::new(test_config()).unwrap();

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

        let views = engine.list_feature_views().await;
        assert_eq!(views.len(), 2);
    }

    #[tokio::test]
    async fn test_delete_feature_view() {
        let engine = FeatureStoreEngine::new(test_config()).unwrap();
        engine
            .register_feature_view(FeatureViewDefinition::new("to_delete"))
            .await
            .unwrap();

        assert!(engine.delete_feature_view("to_delete").await.is_ok());
        assert!(engine.delete_feature_view("to_delete").await.is_err());
    }
}
