//! Offline/Historical Feature Store
//!
//! Provides historical feature storage with point-in-time join logic
//! for training dataset generation and time-travel queries.

use crate::featurestore::feature::FeatureValue;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// A single historical feature record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoricalRecord {
    /// Entity key (e.g., "user_123")
    pub entity_key: String,
    /// Feature values
    pub features: HashMap<String, FeatureValue>,
    /// Event timestamp (when the features were valid)
    pub event_timestamp: i64,
    /// Ingestion timestamp (when the record was written)
    pub ingestion_timestamp: i64,
}

/// Historical feature store for offline/training data
///
/// Stores versioned feature records organized by feature view,
/// supporting point-in-time joins for ML training dataset generation.
pub struct HistoricalStore {
    /// Data indexed by feature_view -> list of records (sorted by event_timestamp)
    data: Arc<RwLock<HashMap<String, Vec<HistoricalRecord>>>>,
}

impl HistoricalStore {
    /// Create a new historical store
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Write a feature record to the offline store
    pub async fn write(
        &self,
        feature_view: &str,
        entity_key: &str,
        features: HashMap<String, FeatureValue>,
        event_timestamp: i64,
    ) {
        let mut data = self.data.write().await;
        let records = data.entry(feature_view.to_string()).or_default();

        let record = HistoricalRecord {
            entity_key: entity_key.to_string(),
            features,
            event_timestamp,
            ingestion_timestamp: chrono::Utc::now().timestamp_millis(),
        };

        // Insert in sorted order by event_timestamp
        let pos = records
            .binary_search_by_key(&event_timestamp, |r| r.event_timestamp)
            .unwrap_or_else(|p| p);
        records.insert(pos, record);
    }

    /// Write a batch of records
    pub async fn write_batch(&self, feature_view: &str, records: Vec<HistoricalRecord>) {
        let mut data = self.data.write().await;
        let view_records = data.entry(feature_view.to_string()).or_default();
        view_records.extend(records);
        view_records.sort_by_key(|r| r.event_timestamp);
    }

    /// Point-in-time lookup for a single entity
    ///
    /// Returns the latest feature values for the entity that were valid
    /// at or before the given timestamp.
    pub async fn point_in_time_lookup(
        &self,
        entity_key: &str,
        timestamp: i64,
    ) -> HashMap<String, FeatureValue> {
        let data = self.data.read().await;
        let mut result = HashMap::new();

        // Search across all feature views
        for records in data.values() {
            let matching = records
                .iter()
                .filter(|r| r.entity_key == entity_key && r.event_timestamp <= timestamp)
                .max_by_key(|r| r.event_timestamp);

            if let Some(record) = matching {
                result.extend(record.features.clone());
            }
        }

        result
    }

    /// Point-in-time lookup within a specific feature view
    pub async fn point_in_time_lookup_view(
        &self,
        feature_view: &str,
        entity_key: &str,
        timestamp: i64,
    ) -> HashMap<String, FeatureValue> {
        let data = self.data.read().await;
        let records = match data.get(feature_view) {
            Some(r) => r,
            None => return HashMap::new(),
        };

        records
            .iter()
            .filter(|r| r.entity_key == entity_key && r.event_timestamp <= timestamp)
            .max_by_key(|r| r.event_timestamp)
            .map(|r| r.features.clone())
            .unwrap_or_default()
    }

    /// Point-in-time join for multiple entities and timestamps
    ///
    /// For each (entity_key, timestamp) pair, finds the latest features
    /// that were valid at or before that timestamp.
    pub async fn point_in_time_join(
        &self,
        feature_view: &str,
        entity_timestamps: &[(String, i64)],
        feature_names: &[String],
    ) -> Vec<HashMap<String, FeatureValue>> {
        let data = self.data.read().await;
        let records = data.get(feature_view).cloned().unwrap_or_default();

        entity_timestamps
            .iter()
            .map(|(entity_key, timestamp)| {
                let matching = records
                    .iter()
                    .filter(|r| r.entity_key == *entity_key && r.event_timestamp <= *timestamp)
                    .max_by_key(|r| r.event_timestamp);

                match matching {
                    Some(record) => feature_names
                        .iter()
                        .map(|name| {
                            let value = record
                                .features
                                .get(name)
                                .cloned()
                                .unwrap_or(FeatureValue::Null);
                            (name.clone(), value)
                        })
                        .collect(),
                    None => feature_names
                        .iter()
                        .map(|name| (name.clone(), FeatureValue::Null))
                        .collect(),
                }
            })
            .collect()
    }

    /// Get records in a time range for a feature view
    pub async fn get_range(
        &self,
        feature_view: &str,
        start_time: Option<i64>,
        end_time: Option<i64>,
    ) -> Vec<HistoricalRecord> {
        let data = self.data.read().await;
        let records = match data.get(feature_view) {
            Some(r) => r,
            None => return Vec::new(),
        };

        records
            .iter()
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
            .cloned()
            .collect()
    }

    /// Get all records for a feature view, grouped by entity key
    pub async fn get_all_for_view(
        &self,
        feature_view: &str,
    ) -> Vec<(String, HashMap<String, FeatureValue>)> {
        let data = self.data.read().await;
        let records = match data.get(feature_view) {
            Some(r) => r,
            None => return Vec::new(),
        };

        // Group by entity_key, keeping only the latest record per entity
        let mut latest: HashMap<String, &HistoricalRecord> = HashMap::new();
        for record in records {
            let entry = latest.entry(record.entity_key.clone()).or_insert(record);
            if record.event_timestamp > entry.event_timestamp {
                *entry = record;
            }
        }

        latest
            .into_iter()
            .map(|(key, record)| (key, record.features.clone()))
            .collect()
    }

    /// Total number of records across all views
    pub async fn len(&self) -> usize {
        let data = self.data.read().await;
        data.values().map(|v| v.len()).sum()
    }

    /// Check if the store is empty
    pub async fn is_empty(&self) -> bool {
        self.len().await == 0
    }

    /// Clear all data for a feature view
    pub async fn clear_view(&self, feature_view: &str) {
        let mut data = self.data.write().await;
        data.remove(feature_view);
    }

    /// Clear all data
    pub async fn clear(&self) {
        let mut data = self.data.write().await;
        data.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_write_and_read() {
        let store = HistoricalStore::new();

        let mut features = HashMap::new();
        features.insert("score".to_string(), FeatureValue::Float64(0.95));

        store
            .write("user_features", "user_123", features, 1000)
            .await;

        let result = store.point_in_time_lookup("user_123", 1500).await;
        assert_eq!(result.get("score"), Some(&FeatureValue::Float64(0.95)));
    }

    #[tokio::test]
    async fn test_point_in_time_correctness() {
        let store = HistoricalStore::new();

        // Write records at different timestamps
        let mut f1 = HashMap::new();
        f1.insert("score".to_string(), FeatureValue::Float64(0.5));
        store.write("view", "entity_1", f1, 1000).await;

        let mut f2 = HashMap::new();
        f2.insert("score".to_string(), FeatureValue::Float64(0.7));
        store.write("view", "entity_1", f2, 2000).await;

        let mut f3 = HashMap::new();
        f3.insert("score".to_string(), FeatureValue::Float64(0.9));
        store.write("view", "entity_1", f3, 3000).await;

        // At t=1500, should get the t=1000 value
        let result = store
            .point_in_time_lookup_view("view", "entity_1", 1500)
            .await;
        assert_eq!(result.get("score"), Some(&FeatureValue::Float64(0.5)));

        // At t=2500, should get the t=2000 value
        let result = store
            .point_in_time_lookup_view("view", "entity_1", 2500)
            .await;
        assert_eq!(result.get("score"), Some(&FeatureValue::Float64(0.7)));

        // At t=5000, should get the latest (t=3000) value
        let result = store
            .point_in_time_lookup_view("view", "entity_1", 5000)
            .await;
        assert_eq!(result.get("score"), Some(&FeatureValue::Float64(0.9)));

        // At t=500, before any record, should get empty
        let result = store
            .point_in_time_lookup_view("view", "entity_1", 500)
            .await;
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_point_in_time_join() {
        let store = HistoricalStore::new();

        let mut f1 = HashMap::new();
        f1.insert("amount".to_string(), FeatureValue::Float64(100.0));
        store.write("purchases", "user_1", f1, 1000).await;

        let mut f2 = HashMap::new();
        f2.insert("amount".to_string(), FeatureValue::Float64(200.0));
        store.write("purchases", "user_1", f2, 2000).await;

        let mut f3 = HashMap::new();
        f3.insert("amount".to_string(), FeatureValue::Float64(50.0));
        store.write("purchases", "user_2", f3, 1500).await;

        let results = store
            .point_in_time_join(
                "purchases",
                &[
                    ("user_1".to_string(), 1500),
                    ("user_1".to_string(), 2500),
                    ("user_2".to_string(), 1000),
                    ("user_2".to_string(), 2000),
                ],
                &["amount".to_string()],
            )
            .await;

        assert_eq!(results.len(), 4);
        // user_1 at 1500 -> t=1000 value
        assert_eq!(
            results[0].get("amount"),
            Some(&FeatureValue::Float64(100.0))
        );
        // user_1 at 2500 -> t=2000 value
        assert_eq!(
            results[1].get("amount"),
            Some(&FeatureValue::Float64(200.0))
        );
        // user_2 at 1000 -> no record yet
        assert_eq!(results[2].get("amount"), Some(&FeatureValue::Null));
        // user_2 at 2000 -> t=1500 value
        assert_eq!(
            results[3].get("amount"),
            Some(&FeatureValue::Float64(50.0))
        );
    }

    #[tokio::test]
    async fn test_get_range() {
        let store = HistoricalStore::new();

        for i in 1..=5 {
            let mut f = HashMap::new();
            f.insert("v".to_string(), FeatureValue::Int64(i));
            store
                .write("view", "entity", f, i as i64 * 1000)
                .await;
        }

        let range = store.get_range("view", Some(2000), Some(4000)).await;
        assert_eq!(range.len(), 3); // t=2000, 3000, 4000
    }

    #[tokio::test]
    async fn test_get_all_for_view() {
        let store = HistoricalStore::new();

        let mut f1 = HashMap::new();
        f1.insert("v".to_string(), FeatureValue::Int64(1));
        store.write("view", "e1", f1, 1000).await;

        let mut f2 = HashMap::new();
        f2.insert("v".to_string(), FeatureValue::Int64(2));
        store.write("view", "e1", f2, 2000).await;

        let mut f3 = HashMap::new();
        f3.insert("v".to_string(), FeatureValue::Int64(3));
        store.write("view", "e2", f3, 1500).await;

        let all = store.get_all_for_view("view").await;
        assert_eq!(all.len(), 2); // Two entities

        // e1 should have the latest value (t=2000)
        let e1 = all.iter().find(|(k, _)| k == "e1").unwrap();
        assert_eq!(e1.1.get("v"), Some(&FeatureValue::Int64(2)));
    }

    #[tokio::test]
    async fn test_len_and_clear() {
        let store = HistoricalStore::new();

        let mut f = HashMap::new();
        f.insert("v".to_string(), FeatureValue::Int64(1));
        store.write("view1", "e1", f.clone(), 1000).await;
        store.write("view1", "e2", f.clone(), 2000).await;
        store.write("view2", "e1", f, 3000).await;

        assert_eq!(store.len().await, 3);
        assert!(!store.is_empty().await);

        store.clear_view("view1").await;
        assert_eq!(store.len().await, 1);

        store.clear().await;
        assert!(store.is_empty().await);
    }
}
