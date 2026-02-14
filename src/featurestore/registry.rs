//! Feature Registry
//!
//! Central repository for feature metadata including feature groups and views.

use super::config::RegistryConfig;
use super::entity::Entity;
use super::feature::{Feature, FeatureSchema};
use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Feature registry
pub struct FeatureRegistry {
    config: RegistryConfig,
    entities: Arc<RwLock<HashMap<String, Entity>>>,
    groups: Arc<RwLock<HashMap<String, FeatureGroup>>>,
    views: Arc<RwLock<HashMap<String, FeatureView>>>,
}

impl FeatureRegistry {
    /// Create a new feature registry
    pub fn new(config: RegistryConfig) -> Result<Self> {
        Ok(Self {
            config,
            entities: Arc::new(RwLock::new(HashMap::new())),
            groups: Arc::new(RwLock::new(HashMap::new())),
            views: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Register an entity
    pub async fn register_entity(&self, entity: Entity) -> Result<()> {
        let mut entities = self.entities.write().await;
        if entities.contains_key(&entity.name) {
            return Err(StreamlineError::Config(format!(
                "Entity already exists: {}",
                entity.name
            )));
        }
        entities.insert(entity.name.clone(), entity);
        Ok(())
    }

    /// Get an entity
    pub async fn get_entity(&self, name: &str) -> Option<Entity> {
        let entities = self.entities.read().await;
        entities.get(name).cloned()
    }

    /// List entities
    pub async fn list_entities(&self) -> Vec<Entity> {
        let entities = self.entities.read().await;
        entities.values().cloned().collect()
    }

    /// Register a feature group
    pub async fn register_group(&self, group: FeatureGroup) -> Result<()> {
        let mut groups = self.groups.write().await;
        if groups.contains_key(&group.name) {
            return Err(StreamlineError::Config(format!(
                "Feature group already exists: {}",
                group.name
            )));
        }
        groups.insert(group.name.clone(), group);
        Ok(())
    }

    /// Get a feature group
    pub async fn get_group(&self, name: &str) -> Option<FeatureGroup> {
        let groups = self.groups.read().await;
        groups.get(name).cloned()
    }

    /// List feature groups
    pub async fn list_groups(&self) -> Vec<FeatureGroup> {
        let groups = self.groups.read().await;
        groups.values().cloned().collect()
    }

    /// Get group count
    pub async fn group_count(&self) -> usize {
        let groups = self.groups.read().await;
        groups.len()
    }

    /// Register a feature view
    pub async fn register_view(&self, view: FeatureView) -> Result<()> {
        let mut views = self.views.write().await;
        if views.contains_key(&view.name) {
            return Err(StreamlineError::Config(format!(
                "Feature view already exists: {}",
                view.name
            )));
        }
        views.insert(view.name.clone(), view);
        Ok(())
    }

    /// Update a feature view
    pub async fn update_view(&self, view: FeatureView) -> Result<()> {
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

    /// Get a feature view
    pub async fn get_view(&self, name: &str) -> Option<FeatureView> {
        let views = self.views.read().await;
        views.get(name).cloned()
    }

    /// List feature views
    pub async fn list_views(&self) -> Vec<FeatureView> {
        let views = self.views.read().await;
        views.values().cloned().collect()
    }

    /// Get view count
    pub async fn view_count(&self) -> usize {
        let views = self.views.read().await;
        views.len()
    }

    /// Delete a feature view
    pub async fn delete_view(&self, name: &str) -> Result<()> {
        let mut views = self.views.write().await;
        if views.remove(name).is_none() {
            return Err(StreamlineError::Config(format!(
                "Feature view not found: {}",
                name
            )));
        }
        Ok(())
    }

    /// Search views by tags
    pub async fn search_views_by_tag(&self, key: &str, value: &str) -> Vec<FeatureView> {
        let views = self.views.read().await;
        views
            .values()
            .filter(|v| v.tags.get(key).is_some_and(|v| v == value))
            .cloned()
            .collect()
    }

    /// Get configuration
    pub fn config(&self) -> &RegistryConfig {
        &self.config
    }
}

/// Feature group - a collection of related features from the same source
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureGroup {
    /// Group name
    pub name: String,
    /// Description
    pub description: Option<String>,
    /// Entity types this group relates to
    pub entities: Vec<String>,
    /// Features in this group
    pub features: Vec<Feature>,
    /// Source configuration
    pub source: FeatureSource,
    /// Tags
    pub tags: HashMap<String, String>,
    /// Owner
    pub owner: Option<String>,
    /// Created timestamp
    pub created_at: i64,
    /// Updated timestamp
    pub updated_at: i64,
}

impl FeatureGroup {
    /// Create a new feature group
    pub fn new(name: impl Into<String>) -> Self {
        let now = chrono::Utc::now().timestamp_millis();
        Self {
            name: name.into(),
            description: None,
            entities: Vec::new(),
            features: Vec::new(),
            source: FeatureSource::default(),
            tags: HashMap::new(),
            owner: None,
            created_at: now,
            updated_at: now,
        }
    }

    /// Set description
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Add an entity
    pub fn with_entity(mut self, entity: impl Into<String>) -> Self {
        self.entities.push(entity.into());
        self
    }

    /// Add a feature
    pub fn with_feature(mut self, feature: Feature) -> Self {
        self.features.push(feature);
        self
    }

    /// Set source
    pub fn with_source(mut self, source: FeatureSource) -> Self {
        self.source = source;
        self
    }

    /// Add a tag
    pub fn with_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags.insert(key.into(), value.into());
        self
    }

    /// Set owner
    pub fn with_owner(mut self, owner: impl Into<String>) -> Self {
        self.owner = Some(owner.into());
        self
    }

    /// Get feature names
    pub fn feature_names(&self) -> Vec<&str> {
        self.features.iter().map(|f| f.name.as_str()).collect()
    }

    /// Get feature by name
    pub fn get_feature(&self, name: &str) -> Option<&Feature> {
        self.features.iter().find(|f| f.name == name)
    }
}

/// Feature source configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureSource {
    /// Source type
    pub source_type: FeatureSourceType,
    /// Topic name (for Kafka/Streamline sources)
    pub topic: Option<String>,
    /// Table name (for batch sources)
    pub table: Option<String>,
    /// Event timestamp column
    pub event_timestamp_column: String,
    /// Created timestamp column
    pub created_timestamp_column: Option<String>,
    /// Field mapping (source field -> feature name)
    pub field_mapping: HashMap<String, String>,
}

impl Default for FeatureSource {
    fn default() -> Self {
        Self {
            source_type: FeatureSourceType::Streamline,
            topic: None,
            table: None,
            event_timestamp_column: "event_timestamp".to_string(),
            created_timestamp_column: None,
            field_mapping: HashMap::new(),
        }
    }
}

impl FeatureSource {
    /// Create a Streamline source
    pub fn streamline(topic: impl Into<String>) -> Self {
        Self {
            source_type: FeatureSourceType::Streamline,
            topic: Some(topic.into()),
            ..Default::default()
        }
    }

    /// Create a Kafka source
    pub fn kafka(topic: impl Into<String>) -> Self {
        Self {
            source_type: FeatureSourceType::Kafka,
            topic: Some(topic.into()),
            ..Default::default()
        }
    }

    /// Create a batch source
    pub fn batch(table: impl Into<String>) -> Self {
        Self {
            source_type: FeatureSourceType::Batch,
            table: Some(table.into()),
            ..Default::default()
        }
    }

    /// Set event timestamp column
    pub fn with_event_timestamp(mut self, column: impl Into<String>) -> Self {
        self.event_timestamp_column = column.into();
        self
    }

    /// Add field mapping
    pub fn with_field_mapping(
        mut self,
        source_field: impl Into<String>,
        feature_name: impl Into<String>,
    ) -> Self {
        self.field_mapping
            .insert(source_field.into(), feature_name.into());
        self
    }
}

/// Feature source type
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum FeatureSourceType {
    /// Streamline topic
    #[default]
    Streamline,
    /// Kafka topic
    Kafka,
    /// Batch table (BigQuery, Snowflake, etc.)
    Batch,
    /// Push-based (real-time writes)
    Push,
    /// Request source (on-demand computation)
    RequestSource,
}

/// Feature view - a selection of features for serving
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureView {
    /// View name
    pub name: String,
    /// Description
    pub description: Option<String>,
    /// Source feature groups
    pub source_groups: Vec<String>,
    /// Entity types
    pub entities: Vec<String>,
    /// Features (from source groups)
    pub features: Vec<String>,
    /// Feature schema
    pub schema: FeatureSchema,
    /// Configuration
    pub config: FeatureViewConfig,
    /// Tags
    pub tags: HashMap<String, String>,
    /// Owner
    pub owner: Option<String>,
    /// Created timestamp
    pub created_at: i64,
    /// Updated timestamp
    pub updated_at: i64,
}

impl FeatureView {
    /// Create a new feature view
    pub fn new(name: impl Into<String>) -> Self {
        let now = chrono::Utc::now().timestamp_millis();
        Self {
            name: name.into(),
            description: None,
            source_groups: Vec::new(),
            entities: Vec::new(),
            features: Vec::new(),
            schema: FeatureSchema::new(),
            config: FeatureViewConfig::default(),
            tags: HashMap::new(),
            owner: None,
            created_at: now,
            updated_at: now,
        }
    }

    /// Set description
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Add a source group
    pub fn with_source_group(mut self, group: impl Into<String>) -> Self {
        self.source_groups.push(group.into());
        self
    }

    /// Add an entity
    pub fn with_entity(mut self, entity: impl Into<String>) -> Self {
        self.entities.push(entity.into());
        self
    }

    /// Add a feature
    pub fn with_feature(mut self, feature: impl Into<String>) -> Self {
        self.features.push(feature.into());
        self
    }

    /// Set schema
    pub fn with_schema(mut self, schema: FeatureSchema) -> Self {
        self.schema = schema;
        self
    }

    /// Set configuration
    pub fn with_config(mut self, config: FeatureViewConfig) -> Self {
        self.config = config;
        self
    }

    /// Add a tag
    pub fn with_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags.insert(key.into(), value.into());
        self
    }

    /// Set owner
    pub fn with_owner(mut self, owner: impl Into<String>) -> Self {
        self.owner = Some(owner.into());
        self
    }
}

/// Feature view configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureViewConfig {
    /// Enable online serving
    pub online: bool,
    /// Enable offline serving
    pub offline: bool,
    /// TTL for features in seconds
    pub ttl_seconds: Option<u64>,
    /// Materialization schedule (cron expression)
    pub materialization_schedule: Option<String>,
    /// Batch source for historical data
    pub batch_source: Option<String>,
    /// Stream source for online data
    pub stream_source: Option<String>,
}

impl Default for FeatureViewConfig {
    fn default() -> Self {
        Self {
            online: true,
            offline: true,
            ttl_seconds: None,
            materialization_schedule: None,
            batch_source: None,
            stream_source: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::featurestore::feature::FeatureType;

    #[tokio::test]
    async fn test_registry_creation() {
        let config = RegistryConfig::default();
        let registry = FeatureRegistry::new(config);
        assert!(registry.is_ok());
    }

    #[tokio::test]
    async fn test_register_feature_group() {
        let config = RegistryConfig::default();
        let registry = FeatureRegistry::new(config).unwrap();

        let group = FeatureGroup::new("user_features")
            .with_description("User-related features")
            .with_entity("user")
            .with_feature(Feature::new("age", FeatureType::Int32))
            .with_feature(Feature::new("tenure", FeatureType::Float64));

        let result = registry.register_group(group).await;
        assert!(result.is_ok());

        let retrieved = registry.get_group("user_features").await;
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().features.len(), 2);
    }

    #[tokio::test]
    async fn test_register_feature_view() {
        let config = RegistryConfig::default();
        let registry = FeatureRegistry::new(config).unwrap();

        let view = FeatureView::new("user_view")
            .with_description("User feature view")
            .with_source_group("user_features")
            .with_entity("user")
            .with_feature("age")
            .with_feature("tenure");

        let result = registry.register_view(view).await;
        assert!(result.is_ok());

        let retrieved = registry.get_view("user_view").await;
        assert!(retrieved.is_some());
    }

    #[tokio::test]
    async fn test_duplicate_registration() {
        let config = RegistryConfig::default();
        let registry = FeatureRegistry::new(config).unwrap();

        let group = FeatureGroup::new("test_group");
        registry.register_group(group.clone()).await.unwrap();

        let result = registry.register_group(group).await;
        assert!(result.is_err());
    }
}
