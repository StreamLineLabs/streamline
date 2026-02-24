//! Feature Store Materialization
//!
//! Provides a feature store for AI/ML pipelines that materializes and serves
//! feature values with point-in-time correctness, TTL-based expiration, and
//! support for windowed aggregation results.

use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

/// Configuration for the feature store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureStoreConfig {
    /// Logical storage path (for future persistent backends).
    pub storage_path: String,
    /// Time-to-live for feature values.
    pub ttl: Duration,
    /// Maximum number of features stored per entity.
    pub max_features_per_entity: usize,
}

impl Default for FeatureStoreConfig {
    fn default() -> Self {
        Self {
            storage_path: "/tmp/streamline/feature_store".to_string(),
            ttl: Duration::from_secs(86400), // 24 hours
            max_features_per_entity: 1024,
        }
    }
}

/// Supported feature value types.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FeatureValue {
    Float64(f64),
    Int64(i64),
    String(String),
    Vector(Vec<f64>),
    Bool(bool),
}

impl std::fmt::Display for FeatureValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FeatureValue::Float64(v) => write!(f, "{v}"),
            FeatureValue::Int64(v) => write!(f, "{v}"),
            FeatureValue::String(v) => write!(f, "{v}"),
            FeatureValue::Vector(v) => write!(f, "{v:?}"),
            FeatureValue::Bool(v) => write!(f, "{v}"),
        }
    }
}

/// Statistics about the feature store.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FeatureStoreStats {
    pub total_entities: usize,
    pub total_features: usize,
    pub total_values: usize,
}

type FeatureTimeline = BTreeMap<u64, FeatureValue>;
type EntityFeatures = HashMap<String, FeatureTimeline>;

/// In-memory feature store with point-in-time lookup and TTL expiration.
pub struct FeatureStore {
    config: FeatureStoreConfig,
    /// entity_id -> (feature_name -> sorted timeline)
    data: Arc<RwLock<HashMap<String, EntityFeatures>>>,
}

impl FeatureStore {
    /// Create a new feature store.
    pub fn new(config: FeatureStoreConfig) -> Result<Self> {
        if config.max_features_per_entity == 0 {
            return Err(StreamlineError::Config(
                "max_features_per_entity must be > 0".into(),
            ));
        }
        Ok(Self {
            config,
            data: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Store a feature value at the given timestamp.
    pub async fn put_feature(
        &self,
        entity_id: &str,
        feature_name: &str,
        value: FeatureValue,
        timestamp: u64,
    ) -> Result<()> {
        let mut data = self.data.write().await;
        let entity = data.entry(entity_id.to_string()).or_default();

        if !entity.contains_key(feature_name)
            && entity.len() >= self.config.max_features_per_entity
        {
            return Err(StreamlineError::Config(format!(
                "entity '{entity_id}' has reached the maximum of {} features",
                self.config.max_features_per_entity
            )));
        }

        let timeline = entity.entry(feature_name.to_string()).or_default();
        timeline.insert(timestamp, value);
        Ok(())
    }

    /// Get the latest value for a feature.
    pub async fn get_feature(
        &self,
        entity_id: &str,
        feature_name: &str,
    ) -> Result<Option<FeatureValue>> {
        let data = self.data.read().await;
        Ok(data
            .get(entity_id)
            .and_then(|e| e.get(feature_name))
            .and_then(|tl| tl.iter().next_back())
            .map(|(_, v)| v.clone()))
    }

    /// Get all latest feature values for an entity.
    pub async fn get_features(
        &self,
        entity_id: &str,
    ) -> Result<HashMap<String, FeatureValue>> {
        let data = self.data.read().await;
        let mut result = HashMap::new();
        if let Some(entity) = data.get(entity_id) {
            for (name, timeline) in entity {
                if let Some((_, v)) = timeline.iter().next_back() {
                    result.insert(name.clone(), v.clone());
                }
            }
        }
        Ok(result)
    }

    /// Get the feature value at or before the given timestamp (point-in-time).
    pub async fn get_feature_at(
        &self,
        entity_id: &str,
        feature_name: &str,
        timestamp: u64,
    ) -> Result<Option<FeatureValue>> {
        let data = self.data.read().await;
        Ok(data
            .get(entity_id)
            .and_then(|e| e.get(feature_name))
            .and_then(|tl| tl.range(..=timestamp).next_back())
            .map(|(_, v)| v.clone()))
    }

    /// Materialize aggregated features from windowed computation results.
    ///
    /// Each entry in `window_values` is a `(timestamp, value)` pair produced
    /// by a window aggregation. They are bulk-inserted into the timeline.
    pub async fn materialize_from_window(
        &self,
        entity_id: &str,
        feature_name: &str,
        window_values: Vec<(u64, FeatureValue)>,
    ) -> Result<()> {
        let mut data = self.data.write().await;
        let entity = data.entry(entity_id.to_string()).or_default();

        if !entity.contains_key(feature_name)
            && entity.len() >= self.config.max_features_per_entity
        {
            return Err(StreamlineError::Config(format!(
                "entity '{entity_id}' has reached the maximum of {} features",
                self.config.max_features_per_entity
            )));
        }

        let timeline = entity.entry(feature_name.to_string()).or_default();
        for (ts, val) in window_values {
            timeline.insert(ts, val);
        }
        Ok(())
    }

    /// Remove feature values whose timestamps are older than the configured TTL
    /// relative to the supplied `now` epoch (milliseconds).
    pub async fn expire_stale_features(&self) -> Result<usize> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let cutoff = now.saturating_sub(self.config.ttl.as_millis() as u64);

        let mut data = self.data.write().await;
        let mut expired = 0usize;

        for entity in data.values_mut() {
            for timeline in entity.values_mut() {
                let before = timeline.len();
                // Remove all entries strictly before the cutoff.
                *timeline = timeline.split_off(&cutoff);
                expired += before - timeline.len();
            }
            // Prune empty timelines.
            entity.retain(|_, tl| !tl.is_empty());
        }
        // Prune empty entities.
        data.retain(|_, e| !e.is_empty());

        Ok(expired)
    }

    /// Return statistics about the store.
    pub async fn stats(&self) -> FeatureStoreStats {
        let data = self.data.read().await;
        let total_entities = data.len();
        let mut total_features = 0usize;
        let mut total_values = 0usize;
        for entity in data.values() {
            total_features += entity.len();
            for timeline in entity.values() {
                total_values += timeline.len();
            }
        }
        FeatureStoreStats {
            total_entities,
            total_features,
            total_values,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_store() -> FeatureStore {
        FeatureStore::new(FeatureStoreConfig::default()).unwrap()
    }

    #[tokio::test]
    async fn test_put_and_get_feature() {
        let store = default_store();
        store
            .put_feature("user:1", "age", FeatureValue::Int64(30), 1000)
            .await
            .unwrap();

        let val = store.get_feature("user:1", "age").await.unwrap();
        assert_eq!(val, Some(FeatureValue::Int64(30)));
    }

    #[tokio::test]
    async fn test_get_feature_missing() {
        let store = default_store();
        let val = store.get_feature("user:1", "age").await.unwrap();
        assert_eq!(val, None);
    }

    #[tokio::test]
    async fn test_get_features() {
        let store = default_store();
        store
            .put_feature("user:1", "age", FeatureValue::Int64(25), 100)
            .await
            .unwrap();
        store
            .put_feature("user:1", "name", FeatureValue::String("Alice".into()), 100)
            .await
            .unwrap();

        let features = store.get_features("user:1").await.unwrap();
        assert_eq!(features.len(), 2);
        assert_eq!(features.get("age"), Some(&FeatureValue::Int64(25)));
    }

    #[tokio::test]
    async fn test_get_latest_value() {
        let store = default_store();
        store
            .put_feature("user:1", "score", FeatureValue::Float64(1.0), 100)
            .await
            .unwrap();
        store
            .put_feature("user:1", "score", FeatureValue::Float64(2.0), 200)
            .await
            .unwrap();

        let val = store.get_feature("user:1", "score").await.unwrap();
        assert_eq!(val, Some(FeatureValue::Float64(2.0)));
    }

    #[tokio::test]
    async fn test_get_feature_at_point_in_time() {
        let store = default_store();
        store
            .put_feature("user:1", "score", FeatureValue::Float64(1.0), 100)
            .await
            .unwrap();
        store
            .put_feature("user:1", "score", FeatureValue::Float64(2.0), 200)
            .await
            .unwrap();
        store
            .put_feature("user:1", "score", FeatureValue::Float64(3.0), 300)
            .await
            .unwrap();

        let val = store.get_feature_at("user:1", "score", 150).await.unwrap();
        assert_eq!(val, Some(FeatureValue::Float64(1.0)));

        let val = store.get_feature_at("user:1", "score", 200).await.unwrap();
        assert_eq!(val, Some(FeatureValue::Float64(2.0)));

        let val = store.get_feature_at("user:1", "score", 50).await.unwrap();
        assert_eq!(val, None);
    }

    #[tokio::test]
    async fn test_materialize_from_window() {
        let store = default_store();
        let window = vec![
            (100, FeatureValue::Float64(10.0)),
            (200, FeatureValue::Float64(20.0)),
            (300, FeatureValue::Float64(30.0)),
        ];
        store
            .materialize_from_window("user:1", "avg_spend", window)
            .await
            .unwrap();

        let val = store.get_feature("user:1", "avg_spend").await.unwrap();
        assert_eq!(val, Some(FeatureValue::Float64(30.0)));

        let val = store
            .get_feature_at("user:1", "avg_spend", 150)
            .await
            .unwrap();
        assert_eq!(val, Some(FeatureValue::Float64(10.0)));
    }

    #[tokio::test]
    async fn test_stats() {
        let store = default_store();
        store
            .put_feature("user:1", "a", FeatureValue::Bool(true), 1)
            .await
            .unwrap();
        store
            .put_feature("user:1", "b", FeatureValue::Int64(42), 1)
            .await
            .unwrap();
        store
            .put_feature("user:2", "a", FeatureValue::Float64(3.14), 1)
            .await
            .unwrap();

        let stats = store.stats().await;
        assert_eq!(stats.total_entities, 2);
        assert_eq!(stats.total_features, 3);
        assert_eq!(stats.total_values, 3);
    }

    #[tokio::test]
    async fn test_expire_stale_features() {
        let config = FeatureStoreConfig {
            ttl: Duration::from_secs(1),
            ..Default::default()
        };
        let store = FeatureStore::new(config).unwrap();

        // Insert a value with a very old timestamp (epoch 0).
        store
            .put_feature("user:1", "old", FeatureValue::Int64(1), 0)
            .await
            .unwrap();

        let expired = store.expire_stale_features().await.unwrap();
        assert!(expired >= 1);

        let val = store.get_feature("user:1", "old").await.unwrap();
        assert_eq!(val, None);
    }

    #[tokio::test]
    async fn test_max_features_per_entity() {
        let config = FeatureStoreConfig {
            max_features_per_entity: 2,
            ..Default::default()
        };
        let store = FeatureStore::new(config).unwrap();

        store
            .put_feature("e1", "f1", FeatureValue::Bool(true), 1)
            .await
            .unwrap();
        store
            .put_feature("e1", "f2", FeatureValue::Bool(true), 1)
            .await
            .unwrap();

        let res = store
            .put_feature("e1", "f3", FeatureValue::Bool(true), 1)
            .await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_feature_value_display() {
        assert_eq!(FeatureValue::Float64(1.5).to_string(), "1.5");
        assert_eq!(FeatureValue::Int64(42).to_string(), "42");
        assert_eq!(FeatureValue::Bool(true).to_string(), "true");
        assert_eq!(FeatureValue::String("hi".into()).to_string(), "hi");
    }

    #[tokio::test]
    async fn test_vector_feature() {
        let store = default_store();
        let vec_val = FeatureValue::Vector(vec![1.0, 2.0, 3.0]);
        store
            .put_feature("user:1", "embedding", vec_val.clone(), 100)
            .await
            .unwrap();

        let val = store.get_feature("user:1", "embedding").await.unwrap();
        assert_eq!(val, Some(vec_val));
    }

    #[test]
    fn test_invalid_config() {
        let config = FeatureStoreConfig {
            max_features_per_entity: 0,
            ..Default::default()
        };
        assert!(FeatureStore::new(config).is_err());
    }
}
