//! Feature Serving Layer
//!
//! Low-latency feature retrieval for online inference.

use super::entity::EntityKey;
use super::feature::{FeatureValue, FeatureVector};
use super::registry::FeatureRegistry;
use super::store::{FeatureRecord, OfflineStore, OnlineStore};
use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// Feature server for online and offline serving
pub struct FeatureServer {
    registry: Arc<FeatureRegistry>,
    online_store: Arc<OnlineStore>,
    offline_store: Arc<OfflineStore>,
    stats: Arc<ServingStatsInner>,
}

impl FeatureServer {
    /// Create a new feature server
    pub fn new(
        registry: Arc<FeatureRegistry>,
        online_store: Arc<OnlineStore>,
        offline_store: Arc<OfflineStore>,
    ) -> Result<Self> {
        Ok(Self {
            registry,
            online_store,
            offline_store,
            stats: Arc::new(ServingStatsInner::default()),
        })
    }

    /// Get online features for entities
    pub async fn get_online_features(
        &self,
        feature_view: &str,
        entity_keys: &[EntityKey],
        features: Option<&[String]>,
    ) -> Result<OnlineFeatures> {
        let start = Instant::now();
        self.stats.requests.fetch_add(1, Ordering::Relaxed);

        // Get the feature view definition
        let view = self.registry.get_view(feature_view).await.ok_or_else(|| {
            StreamlineError::Config(format!("Feature view not found: {}", feature_view))
        })?;

        // Determine which features to retrieve
        let feature_names: Vec<String> = features
            .map(|f| f.to_vec())
            .unwrap_or_else(|| view.features.clone());

        // Retrieve from online store
        let records = self.online_store.multi_get(feature_view, entity_keys).await;

        // Build response
        let mut entity_rows = Vec::new();
        let mut found_count = 0;
        let mut missing_count = 0;

        for (i, record) in records.into_iter().enumerate() {
            let entity_key = entity_keys[i].clone();

            if let Some(r) = record {
                found_count += 1;
                let values: Vec<FeatureValue> = feature_names
                    .iter()
                    .map(|f| r.features.get(f).cloned().unwrap_or(FeatureValue::Null))
                    .collect();

                entity_rows.push(EntityRow {
                    entity_key,
                    values: FeatureVector::new(feature_names.clone(), values, r.event_timestamp),
                    status: RetrievalStatus::Found,
                });
            } else {
                missing_count += 1;
                let null_values: Vec<FeatureValue> =
                    feature_names.iter().map(|_| FeatureValue::Null).collect();

                entity_rows.push(EntityRow {
                    entity_key,
                    values: FeatureVector::new(feature_names.clone(), null_values, 0),
                    status: RetrievalStatus::NotFound,
                });
            }
        }

        let latency_us = start.elapsed().as_micros() as u64;
        self.stats
            .total_latency_us
            .fetch_add(latency_us, Ordering::Relaxed);

        Ok(OnlineFeatures {
            feature_view: feature_view.to_string(),
            feature_names,
            rows: entity_rows,
            metadata: ResponseMetadata {
                found_count,
                missing_count,
                latency_us,
            },
        })
    }

    /// Get historical features with point-in-time correctness
    pub async fn get_historical_features(
        &self,
        request: PointInTimeJoinRequest,
    ) -> Result<Vec<FeatureRecord>> {
        let start = Instant::now();
        self.stats.pit_requests.fetch_add(1, Ordering::Relaxed);

        // Get the feature view definition
        let view = self
            .registry
            .get_view(&request.feature_view)
            .await
            .ok_or_else(|| {
                StreamlineError::Config(format!("Feature view not found: {}", request.feature_view))
            })?;

        // Determine which features to retrieve
        let feature_names: Vec<String> = request
            .features
            .clone()
            .unwrap_or_else(|| view.features.clone());

        // Perform point-in-time join
        let entity_timestamps: Vec<(EntityKey, i64)> = request
            .entity_rows
            .into_iter()
            .map(|r| (r.entity_key, r.event_timestamp))
            .collect();

        let vectors = self
            .offline_store
            .point_in_time_join(&request.feature_view, &entity_timestamps, &feature_names)
            .await?;

        // Convert to FeatureRecords
        let records: Vec<FeatureRecord> = entity_timestamps
            .into_iter()
            .zip(vectors.into_iter())
            .map(|((entity_key, _), vector)| {
                let mut features = HashMap::new();
                for (name, value) in vector.names.into_iter().zip(vector.values.into_iter()) {
                    features.insert(name, value);
                }
                FeatureRecord {
                    entity_key,
                    features,
                    event_timestamp: vector.event_timestamp,
                    created_timestamp: Some(vector.created_timestamp),
                }
            })
            .collect();

        let latency_us = start.elapsed().as_micros() as u64;
        self.stats
            .total_pit_latency_us
            .fetch_add(latency_us, Ordering::Relaxed);

        Ok(records)
    }

    /// Get serving statistics
    pub fn stats(&self) -> ServingStats {
        let requests = self.stats.requests.load(Ordering::Relaxed);
        let total_latency = self.stats.total_latency_us.load(Ordering::Relaxed);
        let pit_requests = self.stats.pit_requests.load(Ordering::Relaxed);
        let total_pit_latency = self.stats.total_pit_latency_us.load(Ordering::Relaxed);

        ServingStats {
            online_requests: requests,
            online_avg_latency_us: if requests > 0 {
                total_latency / requests
            } else {
                0
            },
            pit_requests,
            pit_avg_latency_us: if pit_requests > 0 {
                total_pit_latency / pit_requests
            } else {
                0
            },
        }
    }
}

/// Internal stats tracking
#[derive(Default)]
struct ServingStatsInner {
    requests: AtomicU64,
    total_latency_us: AtomicU64,
    pit_requests: AtomicU64,
    total_pit_latency_us: AtomicU64,
}

/// Online features response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OnlineFeatures {
    /// Feature view name
    pub feature_view: String,
    /// Feature names
    pub feature_names: Vec<String>,
    /// Entity rows with feature values
    pub rows: Vec<EntityRow>,
    /// Response metadata
    pub metadata: ResponseMetadata,
}

impl OnlineFeatures {
    /// Get features as a map for a specific entity
    pub fn get_entity(&self, entity_key: &EntityKey) -> Option<&EntityRow> {
        self.rows.iter().find(|r| r.entity_key == *entity_key)
    }

    /// Get all values for a specific feature
    pub fn get_feature_column(&self, feature_name: &str) -> Vec<&FeatureValue> {
        let idx = self.feature_names.iter().position(|n| n == feature_name);
        match idx {
            Some(i) => self.rows.iter().map(|r| &r.values.values[i]).collect(),
            None => Vec::new(),
        }
    }

    /// Convert to DataFrame-like structure
    pub fn to_dict(&self) -> HashMap<String, Vec<FeatureValue>> {
        let mut result: HashMap<String, Vec<FeatureValue>> = HashMap::new();

        for name in &self.feature_names {
            result.insert(name.clone(), Vec::new());
        }

        for row in &self.rows {
            for (name, value) in row.values.names.iter().zip(row.values.values.iter()) {
                if let Some(col) = result.get_mut(name) {
                    col.push(value.clone());
                }
            }
        }

        result
    }
}

/// Entity row in response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityRow {
    /// Entity key
    pub entity_key: EntityKey,
    /// Feature values
    pub values: FeatureVector,
    /// Retrieval status
    pub status: RetrievalStatus,
}

/// Retrieval status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RetrievalStatus {
    /// Found in store
    Found,
    /// Not found
    NotFound,
    /// Expired (past TTL)
    Expired,
    /// Error during retrieval
    Error,
}

/// Response metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseMetadata {
    /// Number of entities found
    pub found_count: usize,
    /// Number of entities not found
    pub missing_count: usize,
    /// Latency in microseconds
    pub latency_us: u64,
}

/// Point-in-time join request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PointInTimeJoinRequest {
    /// Feature view name
    pub feature_view: String,
    /// Entity rows with timestamps
    pub entity_rows: Vec<EntityTimestamp>,
    /// Features to retrieve (optional, defaults to all)
    pub features: Option<Vec<String>>,
    /// Full feature names (include feature view prefix)
    pub full_feature_names: bool,
}

impl PointInTimeJoinRequest {
    /// Create a new point-in-time join request
    pub fn new(feature_view: impl Into<String>) -> Self {
        Self {
            feature_view: feature_view.into(),
            entity_rows: Vec::new(),
            features: None,
            full_feature_names: false,
        }
    }

    /// Add an entity row
    pub fn with_entity(mut self, entity_key: EntityKey, event_timestamp: i64) -> Self {
        self.entity_rows.push(EntityTimestamp {
            entity_key,
            event_timestamp,
        });
        self
    }

    /// Set features to retrieve
    pub fn with_features(mut self, features: Vec<String>) -> Self {
        self.features = Some(features);
        self
    }

    /// Use full feature names
    pub fn with_full_feature_names(mut self) -> Self {
        self.full_feature_names = true;
        self
    }
}

/// Entity with timestamp for point-in-time join
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityTimestamp {
    /// Entity key
    pub entity_key: EntityKey,
    /// Event timestamp
    pub event_timestamp: i64,
}

/// Serving statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServingStats {
    /// Online feature requests
    pub online_requests: u64,
    /// Average online latency in microseconds
    pub online_avg_latency_us: u64,
    /// Point-in-time join requests
    pub pit_requests: u64,
    /// Average PIT join latency in microseconds
    pub pit_avg_latency_us: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::featurestore::config::{OfflineStoreConfig, OnlineStoreConfig, RegistryConfig};
    use crate::featurestore::entity::EntityValue;
    use crate::featurestore::registry::FeatureView;

    #[tokio::test]
    async fn test_feature_server_online() {
        let registry = Arc::new(FeatureRegistry::new(RegistryConfig::default()).unwrap());
        let online_store = Arc::new(OnlineStore::new(OnlineStoreConfig::default()).unwrap());
        let offline_store = Arc::new(OfflineStore::new(OfflineStoreConfig::default()).unwrap());

        // Register a feature view
        let view = FeatureView::new("user_features")
            .with_entity("user")
            .with_feature("age")
            .with_feature("score");

        registry.register_view(view).await.unwrap();

        // Add data to online store
        let entity_key =
            EntityKey::new("user").with_key("user_id", EntityValue::String("123".into()));

        let record = FeatureRecord::new(entity_key.clone(), chrono::Utc::now().timestamp_millis())
            .with_feature("age", FeatureValue::Int32(25))
            .with_feature("score", FeatureValue::Float64(0.95));

        online_store.put("user_features", record).await.unwrap();

        // Create server and retrieve
        let server = FeatureServer::new(registry, online_store, offline_store).unwrap();

        let result = server
            .get_online_features("user_features", &[entity_key], None)
            .await
            .unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.metadata.found_count, 1);
        assert_eq!(result.rows[0].status, RetrievalStatus::Found);
    }
}
