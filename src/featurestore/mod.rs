//! Real-Time ML Feature Store
//!
//! Provides a feature store built on Streamline's streaming infrastructure for
//! real-time ML feature serving with point-in-time correctness.
//!
//! # Features
//!
//! - **Feature Registry**: Centralized feature definitions with versioning
//! - **Online Serving**: Low-latency feature retrieval (<10ms p99)
//! - **Point-in-Time Joins**: Historical feature retrieval for training
//! - **Feature Pipelines**: Streaming feature computation
//! - **Feast Integration**: Compatible with Feast API
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │                      FEATURE STORE                                      │
//! ├─────────────────────────────────────────────────────────────────────────┤
//! │  ┌──────────────┐    ┌──────────────┐    ┌──────────────────────────┐  │
//! │  │   Feature    │    │   Online     │    │  Historical              │  │
//! │  │   Registry   │    │   Store      │    │  Store (Offline)         │  │
//! │  └──────┬───────┘    └──────┬───────┘    └──────────┬───────────────┘  │
//! │         │                   │                       │                   │
//! │         └───────────────────┼───────────────────────┘                   │
//! │                             │                                           │
//! │  ┌──────────────────────────┴──────────────────────────────────────┐   │
//! │  │                    Feature Serving Layer                        │   │
//! │  │  ┌────────────┐  ┌────────────┐  ┌────────────────────────────┐│   │
//! │  │  │ Online Get │  │ PIT Join   │  │ Batch Retrieval            ││   │
//! │  │  │ (<10ms)    │  │            │  │                            ││   │
//! │  │  └────────────┘  └────────────┘  └────────────────────────────┘│   │
//! │  └─────────────────────────────────────────────────────────────────┘   │
//! │                             ▲                                           │
//! │  ┌──────────────────────────┴──────────────────────────────────────┐   │
//! │  │                   Feature Pipelines                             │   │
//! │  │  ┌────────────┐  ┌────────────┐  ┌────────────┐                │   │
//! │  │  │ Streaming  │  │ Batch      │  │ On-Demand  │                │   │
//! │  │  │ Transforms │  │ Transforms │  │ Transforms │                │   │
//! │  │  └────────────┘  └────────────┘  └────────────┘                │   │
//! │  └─────────────────────────────────────────────────────────────────┘   │
//! └─────────────────────────────────────────────────────────────────────────┘
//! ```

pub mod config;
pub mod engine;
pub mod entity;
pub mod feature;
pub mod feature_view;
pub mod offline_store;
pub mod online_store;
pub mod pipeline;
pub mod registry;
pub mod serving;
pub mod store;
pub mod transformations;

pub use config::FeatureStoreConfig;
pub use engine::FeatureStoreEngine;
pub use entity::EntityKey;
pub use feature_view::{AggregationWindow, FeatureViewDefinition};
pub use offline_store::HistoricalStore;
pub use online_store::DashMapOnlineStore;
pub use pipeline::{FeaturePipeline, PipelineConfig as FeaturePipelineConfig};
pub use registry::{FeatureGroup, FeatureRegistry, FeatureView};
pub use serving::{FeatureServer, OnlineFeatures, PointInTimeJoinRequest};
pub use store::{FeatureRecord, OfflineStore, OnlineStore};
pub use transformations::TransformationEngine;

use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Feature Store Manager
pub struct FeatureStoreManager {
    #[allow(dead_code)]
    config: FeatureStoreConfig,
    registry: Arc<FeatureRegistry>,
    online_store: Arc<OnlineStore>,
    offline_store: Arc<OfflineStore>,
    server: Arc<FeatureServer>,
    pipelines: Arc<RwLock<HashMap<String, FeaturePipeline>>>,
}

impl FeatureStoreManager {
    /// Create a new feature store manager
    pub fn new(config: FeatureStoreConfig) -> Result<Self> {
        let registry = Arc::new(FeatureRegistry::new(config.registry.clone())?);
        let online_store = Arc::new(OnlineStore::new(config.online.clone())?);
        let offline_store = Arc::new(OfflineStore::new(config.offline.clone())?);
        let server = Arc::new(FeatureServer::new(
            registry.clone(),
            online_store.clone(),
            offline_store.clone(),
        )?);

        Ok(Self {
            config,
            registry,
            online_store,
            offline_store,
            server,
            pipelines: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Register a new feature group
    pub async fn register_feature_group(&self, group: FeatureGroup) -> Result<()> {
        self.registry.register_group(group).await
    }

    /// Register a feature view
    pub async fn register_feature_view(&self, view: FeatureView) -> Result<()> {
        self.registry.register_view(view).await
    }

    /// Get online features for an entity
    pub async fn get_online_features(
        &self,
        feature_view: &str,
        entity_keys: &[EntityKey],
        features: Option<&[String]>,
    ) -> Result<OnlineFeatures> {
        self.server
            .get_online_features(feature_view, entity_keys, features)
            .await
    }

    /// Get historical features with point-in-time correctness
    pub async fn get_historical_features(
        &self,
        request: PointInTimeJoinRequest,
    ) -> Result<Vec<FeatureRecord>> {
        self.server.get_historical_features(request).await
    }

    /// Materialize features from offline to online store
    pub async fn materialize(
        &self,
        feature_view: &str,
        start_time: Option<i64>,
        end_time: Option<i64>,
    ) -> Result<MaterializeResult> {
        let start = std::time::Instant::now();
        let view = self.registry.get_view(feature_view).await.ok_or_else(|| {
            StreamlineError::Config(format!("Feature view not found: {}", feature_view))
        })?;

        // Get features from offline store
        let records = self
            .offline_store
            .get_range(&view.name, start_time, end_time)
            .await?;

        // Write to online store
        let mut count = 0;
        for record in records {
            self.online_store.put(&view.name, record).await?;
            count += 1;
        }

        Ok(MaterializeResult {
            feature_view: feature_view.to_string(),
            records_written: count,
            duration_ms: start.elapsed().as_millis() as u64,
        })
    }

    /// Create a feature pipeline
    pub async fn create_pipeline(&self, config: FeaturePipelineConfig) -> Result<String> {
        let pipeline = FeaturePipeline::new(config.clone())?;
        let id = pipeline.id.clone();

        let mut pipelines = self.pipelines.write().await;
        pipelines.insert(id.clone(), pipeline);

        Ok(id)
    }

    /// Start a feature pipeline
    pub async fn start_pipeline(&self, pipeline_id: &str) -> Result<()> {
        let pipelines = self.pipelines.read().await;
        let pipeline = pipelines.get(pipeline_id).ok_or_else(|| {
            StreamlineError::Config(format!("Pipeline not found: {}", pipeline_id))
        })?;
        pipeline.start().await
    }

    /// Stop a feature pipeline
    pub async fn stop_pipeline(&self, pipeline_id: &str) -> Result<()> {
        let pipelines = self.pipelines.read().await;
        let pipeline = pipelines.get(pipeline_id).ok_or_else(|| {
            StreamlineError::Config(format!("Pipeline not found: {}", pipeline_id))
        })?;
        pipeline.stop().await
    }

    /// Get feature store statistics
    pub async fn stats(&self) -> FeatureStoreStats {
        FeatureStoreStats {
            online_store_size: self.online_store.size().await,
            offline_store_size: self.offline_store.size().await,
            feature_groups: self.registry.group_count().await,
            feature_views: self.registry.view_count().await,
            active_pipelines: self.pipelines.read().await.len(),
        }
    }

    /// Get the feature registry
    pub fn registry(&self) -> Arc<FeatureRegistry> {
        self.registry.clone()
    }

    /// Get the online store
    pub fn online_store(&self) -> Arc<OnlineStore> {
        self.online_store.clone()
    }

    /// Get the offline store
    pub fn offline_store(&self) -> Arc<OfflineStore> {
        self.offline_store.clone()
    }
}

/// Materialization result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializeResult {
    /// Feature view name
    pub feature_view: String,
    /// Records written
    pub records_written: usize,
    /// Duration in milliseconds
    pub duration_ms: u64,
}

/// Feature store statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureStoreStats {
    /// Online store size (records)
    pub online_store_size: usize,
    /// Offline store size (records)
    pub offline_store_size: usize,
    /// Number of feature groups
    pub feature_groups: usize,
    /// Number of feature views
    pub feature_views: usize,
    /// Active pipelines
    pub active_pipelines: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::featurestore::entity::{Entity, EntityKey, EntityValue, EntityValueType};
    use crate::featurestore::feature::{Feature, FeatureType};

    #[tokio::test]
    async fn test_feature_store_creation() {
        let config = FeatureStoreConfig::default();
        let store = FeatureStoreManager::new(config);
        assert!(store.is_ok());
    }

    #[tokio::test]
    async fn test_feature_store_stats() {
        let config = FeatureStoreConfig::default();
        let store = FeatureStoreManager::new(config).unwrap();
        let stats = store.stats().await;
        assert_eq!(stats.online_store_size, 0);
        assert_eq!(stats.offline_store_size, 0);
        assert_eq!(stats.feature_groups, 0);
        assert_eq!(stats.feature_views, 0);
        assert_eq!(stats.active_pipelines, 0);
    }

    #[tokio::test]
    async fn test_register_entity() {
        let config = FeatureStoreConfig::default();
        let store = FeatureStoreManager::new(config).unwrap();

        let entity = Entity::new("user", vec!["user_id".to_string()])
            .with_value_type(EntityValueType::Int64)
            .with_description("User entity");

        let result = store.registry().register_entity(entity).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_register_feature_group() {
        let config = FeatureStoreConfig::default();
        let store = FeatureStoreManager::new(config).unwrap();

        let entity = Entity::new("user", vec!["user_id".to_string()])
            .with_value_type(EntityValueType::Int64);
        store.registry().register_entity(entity).await.unwrap();

        let group = FeatureGroup::new("user_features")
            .with_entity("user")
            .with_feature(Feature::new("age", FeatureType::Int64).with_description("User age"));

        let result = store.register_feature_group(group).await;
        assert!(result.is_ok());

        let stats = store.stats().await;
        assert_eq!(stats.feature_groups, 1);
    }

    #[tokio::test]
    async fn test_get_online_features() {
        let config = FeatureStoreConfig::default();
        let store = FeatureStoreManager::new(config).unwrap();

        // Try to get features for non-existent view
        let entity_key = EntityKey::new("user").with_key("user_id", EntityValue::Int64(123));
        let result = store
            .get_online_features("nonexistent_view", &[entity_key], None)
            .await;
        assert!(result.is_err());
    }
}
