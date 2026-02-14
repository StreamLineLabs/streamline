//! Online and Offline Feature Stores
//!
//! Storage backends for feature data.

use super::config::{OfflineStoreConfig, OnlineStoreConfig};
use super::entity::EntityKey;
use super::feature::{FeatureValue, FeatureVector};
use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Online feature store for low-latency retrieval
pub struct OnlineStore {
    config: OnlineStoreConfig,
    data: Arc<RwLock<HashMap<String, HashMap<String, OnlineFeatureRecord>>>>,
    size: Arc<AtomicUsize>,
}

impl OnlineStore {
    /// Create a new online store
    pub fn new(config: OnlineStoreConfig) -> Result<Self> {
        Ok(Self {
            config,
            data: Arc::new(RwLock::new(HashMap::new())),
            size: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Get features for an entity
    pub async fn get(
        &self,
        feature_view: &str,
        entity_key: &EntityKey,
    ) -> Option<OnlineFeatureRecord> {
        let data = self.data.read().await;
        data.get(feature_view)
            .and_then(|view_data| view_data.get(&entity_key.to_key_string()).cloned())
    }

    /// Get features for multiple entities
    pub async fn multi_get(
        &self,
        feature_view: &str,
        entity_keys: &[EntityKey],
    ) -> Vec<Option<OnlineFeatureRecord>> {
        let data = self.data.read().await;
        entity_keys
            .iter()
            .map(|key| {
                data.get(feature_view)
                    .and_then(|view_data| view_data.get(&key.to_key_string()).cloned())
            })
            .collect()
    }

    /// Put features for an entity
    pub async fn put(&self, feature_view: &str, record: FeatureRecord) -> Result<()> {
        let mut data = self.data.write().await;
        let view_data = data.entry(feature_view.to_string()).or_default();

        let key = record.entity_key.to_key_string();
        let is_new = !view_data.contains_key(&key);

        let online_record = OnlineFeatureRecord {
            entity_key: record.entity_key,
            features: record.features,
            event_timestamp: record.event_timestamp,
            created_timestamp: chrono::Utc::now().timestamp_millis(),
        };

        view_data.insert(key, online_record);

        if is_new {
            self.size.fetch_add(1, Ordering::Relaxed);
        }

        // Check if we need to evict
        if self.size.load(Ordering::Relaxed) > self.config.max_entries {
            self.evict_oldest(&mut data).await;
        }

        Ok(())
    }

    /// Delete features for an entity
    pub async fn delete(&self, feature_view: &str, entity_key: &EntityKey) -> Result<()> {
        let mut data = self.data.write().await;
        if let Some(view_data) = data.get_mut(feature_view) {
            if view_data.remove(&entity_key.to_key_string()).is_some() {
                self.size.fetch_sub(1, Ordering::Relaxed);
            }
        }
        Ok(())
    }

    /// Clear all data for a feature view
    pub async fn clear_view(&self, feature_view: &str) -> Result<()> {
        let mut data = self.data.write().await;
        if let Some(view_data) = data.remove(feature_view) {
            self.size.fetch_sub(view_data.len(), Ordering::Relaxed);
        }
        Ok(())
    }

    /// Get store size
    pub async fn size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    /// Evict oldest entries
    async fn evict_oldest(&self, data: &mut HashMap<String, HashMap<String, OnlineFeatureRecord>>) {
        let target_size = self.config.max_entries * 9 / 10; // Evict to 90% capacity

        // Find oldest entries across all views
        let mut entries: Vec<(String, String, i64)> = Vec::new();
        for (view, view_data) in data.iter() {
            for (key, record) in view_data.iter() {
                entries.push((view.clone(), key.clone(), record.event_timestamp));
            }
        }

        entries.sort_by_key(|(_, _, ts)| *ts);

        let to_evict = self
            .size
            .load(Ordering::Relaxed)
            .saturating_sub(target_size);
        for (view, key, _) in entries.into_iter().take(to_evict) {
            if let Some(view_data) = data.get_mut(&view) {
                view_data.remove(&key);
                self.size.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }
}

/// Online feature record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OnlineFeatureRecord {
    /// Entity key
    pub entity_key: EntityKey,
    /// Feature values
    pub features: HashMap<String, FeatureValue>,
    /// Event timestamp
    pub event_timestamp: i64,
    /// Created timestamp
    pub created_timestamp: i64,
}

/// Offline feature store for historical data
pub struct OfflineStore {
    #[allow(dead_code)]
    config: OfflineStoreConfig,
    data: Arc<RwLock<HashMap<String, Vec<FeatureRecord>>>>,
}

impl OfflineStore {
    /// Create a new offline store
    pub fn new(config: OfflineStoreConfig) -> Result<Self> {
        Ok(Self {
            config,
            data: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Write a feature record
    pub async fn write(&self, feature_view: &str, record: FeatureRecord) -> Result<()> {
        let mut data = self.data.write().await;
        let records = data.entry(feature_view.to_string()).or_default();
        records.push(record);
        Ok(())
    }

    /// Write multiple feature records
    pub async fn write_batch(&self, feature_view: &str, records: Vec<FeatureRecord>) -> Result<()> {
        let mut data = self.data.write().await;
        let view_records = data.entry(feature_view.to_string()).or_default();
        view_records.extend(records);
        Ok(())
    }

    /// Get records in a time range
    pub async fn get_range(
        &self,
        feature_view: &str,
        start_time: Option<i64>,
        end_time: Option<i64>,
    ) -> Result<Vec<FeatureRecord>> {
        let data = self.data.read().await;
        let records = data.get(feature_view).cloned().unwrap_or_default();

        Ok(records
            .into_iter()
            .filter(|r| {
                if let Some(start) = start_time {
                    if r.event_timestamp < start {
                        return false;
                    }
                }
                if let Some(end) = end_time {
                    if r.event_timestamp > end {
                        return false;
                    }
                }
                true
            })
            .collect())
    }

    /// Point-in-time join
    pub async fn point_in_time_join(
        &self,
        feature_view: &str,
        entity_timestamps: &[(EntityKey, i64)],
        features: &[String],
    ) -> Result<Vec<FeatureVector>> {
        let data = self.data.read().await;
        let records = data.get(feature_view).cloned().unwrap_or_default();

        let mut results = Vec::new();

        for (entity_key, timestamp) in entity_timestamps {
            // Find the latest record before or at the timestamp
            let matching_record = records
                .iter()
                .filter(|r| r.entity_key == *entity_key && r.event_timestamp <= *timestamp)
                .max_by_key(|r| r.event_timestamp);

            if let Some(record) = matching_record {
                let names: Vec<String> = features.to_vec();
                let values: Vec<FeatureValue> = features
                    .iter()
                    .map(|f| {
                        record
                            .features
                            .get(f)
                            .cloned()
                            .unwrap_or(FeatureValue::Null)
                    })
                    .collect();

                results.push(FeatureVector::new(names, values, record.event_timestamp));
            } else {
                // No matching record, return null values
                let names: Vec<String> = features.to_vec();
                let values: Vec<FeatureValue> =
                    features.iter().map(|_| FeatureValue::Null).collect();
                results.push(FeatureVector::new(names, values, *timestamp));
            }
        }

        Ok(results)
    }

    /// Get store size
    pub async fn size(&self) -> usize {
        let data = self.data.read().await;
        data.values().map(|v| v.len()).sum()
    }

    /// Clear all data for a feature view
    pub async fn clear_view(&self, feature_view: &str) -> Result<()> {
        let mut data = self.data.write().await;
        data.remove(feature_view);
        Ok(())
    }
}

/// Feature record (for both online and offline storage)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureRecord {
    /// Entity key
    pub entity_key: EntityKey,
    /// Feature values
    pub features: HashMap<String, FeatureValue>,
    /// Event timestamp (when the features were valid)
    pub event_timestamp: i64,
    /// Created timestamp (when the record was created)
    pub created_timestamp: Option<i64>,
}

impl FeatureRecord {
    /// Create a new feature record
    pub fn new(entity_key: EntityKey, event_timestamp: i64) -> Self {
        Self {
            entity_key,
            features: HashMap::new(),
            event_timestamp,
            created_timestamp: Some(chrono::Utc::now().timestamp_millis()),
        }
    }

    /// Add a feature value
    pub fn with_feature(mut self, name: impl Into<String>, value: FeatureValue) -> Self {
        self.features.insert(name.into(), value);
        self
    }

    /// Add multiple feature values
    pub fn with_features(mut self, features: HashMap<String, FeatureValue>) -> Self {
        self.features.extend(features);
        self
    }

    /// Get a feature value
    pub fn get(&self, name: &str) -> Option<&FeatureValue> {
        self.features.get(name)
    }

    /// Convert to FeatureVector
    pub fn to_vector(&self, feature_names: &[String]) -> FeatureVector {
        let values: Vec<FeatureValue> = feature_names
            .iter()
            .map(|n| self.features.get(n).cloned().unwrap_or(FeatureValue::Null))
            .collect();

        FeatureVector::new(feature_names.to_vec(), values, self.event_timestamp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::featurestore::entity::EntityValue;

    #[tokio::test]
    async fn test_online_store() {
        let config = OnlineStoreConfig::default();
        let store = OnlineStore::new(config).unwrap();

        let entity_key =
            EntityKey::new("user").with_key("user_id", EntityValue::String("123".into()));

        let record = FeatureRecord::new(entity_key.clone(), chrono::Utc::now().timestamp_millis())
            .with_feature("age", FeatureValue::Int32(25))
            .with_feature("score", FeatureValue::Float64(0.95));

        store.put("user_features", record).await.unwrap();

        let retrieved = store.get("user_features", &entity_key).await;
        assert!(retrieved.is_some());

        let online_record = retrieved.unwrap();
        assert_eq!(
            online_record.features.get("age"),
            Some(&FeatureValue::Int32(25))
        );
    }

    #[tokio::test]
    async fn test_offline_store_point_in_time() {
        let config = OfflineStoreConfig::default();
        let store = OfflineStore::new(config).unwrap();

        let entity_key =
            EntityKey::new("user").with_key("user_id", EntityValue::String("123".into()));

        // Write records at different times
        let t1 = 1000i64;
        let t2 = 2000i64;
        let t3 = 3000i64;

        store
            .write(
                "user_features",
                FeatureRecord::new(entity_key.clone(), t1)
                    .with_feature("score", FeatureValue::Float64(0.5)),
            )
            .await
            .unwrap();

        store
            .write(
                "user_features",
                FeatureRecord::new(entity_key.clone(), t2)
                    .with_feature("score", FeatureValue::Float64(0.7)),
            )
            .await
            .unwrap();

        store
            .write(
                "user_features",
                FeatureRecord::new(entity_key.clone(), t3)
                    .with_feature("score", FeatureValue::Float64(0.9)),
            )
            .await
            .unwrap();

        // Point-in-time join at t2 + 500 should return the t2 record
        let results = store
            .point_in_time_join(
                "user_features",
                &[(entity_key, 2500)],
                &["score".to_string()],
            )
            .await
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get_f64("score"), Some(0.7));
    }
}
