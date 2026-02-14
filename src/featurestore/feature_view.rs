//! Feature View Definitions
//!
//! Feature views define how features are computed, what entities they belong to,
//! their data types, aggregation windows, and TTL configuration.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Feature view definition describing a set of features for serving
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureViewDefinition {
    /// Unique name for this feature view
    pub name: String,
    /// Human-readable description
    pub description: Option<String>,
    /// Entities (join keys) this view is keyed on
    pub entities: Vec<FeatureViewEntity>,
    /// Feature definitions in this view
    pub features: Vec<FeatureDefinitionView>,
    /// Source topic to read raw events from
    pub source_topic: Option<String>,
    /// Transformation logic (SQL expression or DSL)
    pub transformation: Option<String>,
    /// TTL for online features in seconds (0 = no expiry)
    pub ttl_seconds: Option<u64>,
    /// Aggregation windows (e.g., 1min, 5min, 1hour, 1day)
    pub aggregation_windows: Vec<AggregationWindow>,
    /// Tags for organization
    pub tags: HashMap<String, String>,
    /// Owner of this feature view
    pub owner: Option<String>,
    /// Created timestamp (millis)
    pub created_at: i64,
    /// Updated timestamp (millis)
    pub updated_at: i64,
}

impl FeatureViewDefinition {
    /// Create a new feature view definition
    pub fn new(name: impl Into<String>) -> Self {
        let now = chrono::Utc::now().timestamp_millis();
        Self {
            name: name.into(),
            description: None,
            entities: Vec::new(),
            features: Vec::new(),
            source_topic: None,
            transformation: None,
            ttl_seconds: None,
            aggregation_windows: Vec::new(),
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
    pub fn with_entity(mut self, entity: FeatureViewEntity) -> Self {
        self.entities.push(entity);
        self
    }

    /// Add a feature
    pub fn with_feature(mut self, feature: FeatureDefinitionView) -> Self {
        self.features.push(feature);
        self
    }

    /// Set source topic
    pub fn with_source_topic(mut self, topic: impl Into<String>) -> Self {
        self.source_topic = Some(topic.into());
        self
    }

    /// Set transformation logic
    pub fn with_transformation(mut self, transformation: impl Into<String>) -> Self {
        self.transformation = Some(transformation.into());
        self
    }

    /// Set TTL in seconds
    pub fn with_ttl_seconds(mut self, ttl: u64) -> Self {
        self.ttl_seconds = Some(ttl);
        self
    }

    /// Add an aggregation window
    pub fn with_aggregation_window(mut self, window: AggregationWindow) -> Self {
        self.aggregation_windows.push(window);
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

    /// Get entity join keys
    pub fn join_keys(&self) -> Vec<&str> {
        self.entities.iter().map(|e| e.join_key.as_str()).collect()
    }
}

/// Entity definition within a feature view
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureViewEntity {
    /// Entity name (also used as the join key)
    pub name: String,
    /// Join key column name
    pub join_key: String,
    /// Description
    pub description: Option<String>,
}

impl FeatureViewEntity {
    /// Create a new entity where name is also the join key
    pub fn new(name: impl Into<String>) -> Self {
        let n = name.into();
        Self {
            join_key: n.clone(),
            name: n,
            description: None,
        }
    }

    /// Create with a separate join key
    pub fn with_join_key(mut self, join_key: impl Into<String>) -> Self {
        self.join_key = join_key.into();
        self
    }

    /// Set description
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }
}

/// Feature definition within a feature view
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureDefinitionView {
    /// Feature name
    pub name: String,
    /// Data type
    pub dtype: FeatureDataType,
    /// Description
    pub description: Option<String>,
    /// Aggregation type (for windowed features)
    pub aggregation: Option<AggregationType>,
    /// Tags
    pub tags: HashMap<String, String>,
}

impl FeatureDefinitionView {
    /// Create a new feature definition
    pub fn new(name: impl Into<String>, dtype: FeatureDataType) -> Self {
        Self {
            name: name.into(),
            dtype,
            description: None,
            aggregation: None,
            tags: HashMap::new(),
        }
    }

    /// Set description
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Set aggregation type
    pub fn with_aggregation(mut self, aggregation: AggregationType) -> Self {
        self.aggregation = Some(aggregation);
        self
    }

    /// Add a tag
    pub fn with_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags.insert(key.into(), value.into());
        self
    }
}

/// Feature data types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FeatureDataType {
    /// Boolean
    Bool,
    /// 64-bit integer
    Int64,
    /// 64-bit floating point
    Float64,
    /// UTF-8 string
    String,
    /// Timestamp (milliseconds since epoch)
    Timestamp,
}

impl std::fmt::Display for FeatureDataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FeatureDataType::Bool => write!(f, "bool"),
            FeatureDataType::Int64 => write!(f, "int64"),
            FeatureDataType::Float64 => write!(f, "float64"),
            FeatureDataType::String => write!(f, "string"),
            FeatureDataType::Timestamp => write!(f, "timestamp"),
        }
    }
}

/// Aggregation type for windowed features
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggregationType {
    /// Sum of values
    Sum,
    /// Average of values
    Avg,
    /// Count of values
    Count,
    /// Minimum value
    Min,
    /// Maximum value
    Max,
    /// Last value
    Last,
    /// First value
    First,
}

impl std::fmt::Display for AggregationType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AggregationType::Sum => write!(f, "sum"),
            AggregationType::Avg => write!(f, "avg"),
            AggregationType::Count => write!(f, "count"),
            AggregationType::Min => write!(f, "min"),
            AggregationType::Max => write!(f, "max"),
            AggregationType::Last => write!(f, "last"),
            AggregationType::First => write!(f, "first"),
        }
    }
}

/// Aggregation window definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregationWindow {
    /// Window name (e.g., "1min", "5min", "1hour", "1day")
    pub name: String,
    /// Window duration in seconds
    pub duration_seconds: u64,
    /// Window type
    pub window_type: WindowType,
    /// Slide interval in seconds (for sliding windows)
    pub slide_seconds: Option<u64>,
}

impl AggregationWindow {
    /// Create a 1-minute tumbling window
    pub fn one_minute() -> Self {
        Self {
            name: "1min".to_string(),
            duration_seconds: 60,
            window_type: WindowType::Tumbling,
            slide_seconds: None,
        }
    }

    /// Create a 5-minute tumbling window
    pub fn five_minutes() -> Self {
        Self {
            name: "5min".to_string(),
            duration_seconds: 300,
            window_type: WindowType::Tumbling,
            slide_seconds: None,
        }
    }

    /// Create a 1-hour tumbling window
    pub fn one_hour() -> Self {
        Self {
            name: "1hour".to_string(),
            duration_seconds: 3600,
            window_type: WindowType::Tumbling,
            slide_seconds: None,
        }
    }

    /// Create a 1-day tumbling window
    pub fn one_day() -> Self {
        Self {
            name: "1day".to_string(),
            duration_seconds: 86400,
            window_type: WindowType::Tumbling,
            slide_seconds: None,
        }
    }

    /// Create a custom tumbling window
    pub fn tumbling(name: impl Into<String>, duration_seconds: u64) -> Self {
        Self {
            name: name.into(),
            duration_seconds,
            window_type: WindowType::Tumbling,
            slide_seconds: None,
        }
    }

    /// Create a custom sliding window
    pub fn sliding(
        name: impl Into<String>,
        duration_seconds: u64,
        slide_seconds: u64,
    ) -> Self {
        Self {
            name: name.into(),
            duration_seconds,
            window_type: WindowType::Sliding,
            slide_seconds: Some(slide_seconds),
        }
    }

    /// Create a session window
    pub fn session(name: impl Into<String>, gap_seconds: u64) -> Self {
        Self {
            name: name.into(),
            duration_seconds: gap_seconds,
            window_type: WindowType::Session,
            slide_seconds: None,
        }
    }
}

/// Window type for aggregations
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WindowType {
    /// Tumbling (non-overlapping, fixed-size)
    Tumbling,
    /// Sliding (overlapping, fixed-size with slide interval)
    Sliding,
    /// Session (activity-based, gap-triggered)
    Session,
}

impl std::fmt::Display for WindowType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WindowType::Tumbling => write!(f, "tumbling"),
            WindowType::Sliding => write!(f, "sliding"),
            WindowType::Session => write!(f, "session"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_feature_view_creation() {
        let view = FeatureViewDefinition::new("user_features")
            .with_description("User feature set")
            .with_entity(FeatureViewEntity::new("user_id"))
            .with_feature(FeatureDefinitionView::new(
                "purchase_count",
                FeatureDataType::Int64,
            ))
            .with_source_topic("purchases")
            .with_ttl_seconds(3600)
            .with_tag("team", "ml");

        assert_eq!(view.name, "user_features");
        assert_eq!(view.entities.len(), 1);
        assert_eq!(view.features.len(), 1);
        assert_eq!(view.source_topic, Some("purchases".to_string()));
        assert_eq!(view.ttl_seconds, Some(3600));
        assert_eq!(view.tags.get("team"), Some(&"ml".to_string()));
    }

    #[test]
    fn test_feature_definition_with_aggregation() {
        let feature = FeatureDefinitionView::new("avg_amount", FeatureDataType::Float64)
            .with_aggregation(AggregationType::Avg)
            .with_description("Average purchase amount");

        assert_eq!(feature.name, "avg_amount");
        assert_eq!(feature.dtype, FeatureDataType::Float64);
        assert_eq!(feature.aggregation, Some(AggregationType::Avg));
    }

    #[test]
    fn test_aggregation_windows() {
        let one_min = AggregationWindow::one_minute();
        assert_eq!(one_min.duration_seconds, 60);
        assert_eq!(one_min.window_type, WindowType::Tumbling);

        let sliding = AggregationWindow::sliding("10min_slide_1min", 600, 60);
        assert_eq!(sliding.duration_seconds, 600);
        assert_eq!(sliding.slide_seconds, Some(60));
        assert_eq!(sliding.window_type, WindowType::Sliding);

        let session = AggregationWindow::session("session_30min", 1800);
        assert_eq!(session.window_type, WindowType::Session);
    }

    #[test]
    fn test_feature_view_entity() {
        let entity = FeatureViewEntity::new("user_id")
            .with_join_key("uid")
            .with_description("User identifier");

        assert_eq!(entity.name, "user_id");
        assert_eq!(entity.join_key, "uid");
        assert_eq!(entity.description, Some("User identifier".to_string()));
    }

    #[test]
    fn test_feature_names() {
        let view = FeatureViewDefinition::new("test")
            .with_feature(FeatureDefinitionView::new("f1", FeatureDataType::Int64))
            .with_feature(FeatureDefinitionView::new("f2", FeatureDataType::Float64));

        assert_eq!(view.feature_names(), vec!["f1", "f2"]);
    }

    #[test]
    fn test_join_keys() {
        let view = FeatureViewDefinition::new("test")
            .with_entity(FeatureViewEntity::new("user_id"))
            .with_entity(FeatureViewEntity::new("session_id"));

        assert_eq!(view.join_keys(), vec!["user_id", "session_id"]);
    }

    #[test]
    fn test_data_type_display() {
        assert_eq!(format!("{}", FeatureDataType::Float64), "float64");
        assert_eq!(format!("{}", FeatureDataType::Int64), "int64");
        assert_eq!(format!("{}", FeatureDataType::String), "string");
    }

    #[test]
    fn test_aggregation_type_display() {
        assert_eq!(format!("{}", AggregationType::Sum), "sum");
        assert_eq!(format!("{}", AggregationType::Avg), "avg");
        assert_eq!(format!("{}", AggregationType::Count), "count");
    }
}
