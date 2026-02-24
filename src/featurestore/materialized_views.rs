//! Materialized Views for Real-Time Feature Computation
//!
//! Provides real-time stream-to-table materialization that automatically
//! computes and updates aggregated features as events arrive.
//!
//! # Architecture
//!
//! ```text
//! Streamline Topic → Materialization Engine → Feature Table (in-memory/DuckDB)
//!     (events)          (windowed aggs)          (point-in-time queries)
//! ```
//!
//! # Example
//!
//! ```yaml
//! # Materialized view definition
//! name: user_purchase_features
//! source_topic: purchases
//! entity_key: user_id
//! features:
//!   - name: total_purchases_1h
//!     aggregation: count
//!     window: tumbling(1h)
//!   - name: avg_order_value_24h
//!     aggregation: avg
//!     field: amount
//!     window: sliding(24h, 1h)
//!   - name: last_purchase_category
//!     aggregation: last_value
//!     field: category
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Configuration for a materialized view
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializedViewConfig {
    /// View name
    pub name: String,
    /// Source Streamline topic
    pub source_topic: String,
    /// JSON path to the entity key in messages
    pub entity_key: String,
    /// Feature definitions with aggregation windows
    pub features: Vec<MaterializedFeature>,
    /// Maximum entities to track (LRU eviction)
    #[serde(default = "default_max_entities")]
    pub max_entities: usize,
    /// TTL for entity entries
    #[serde(default)]
    pub entity_ttl_secs: Option<u64>,
    /// Watermark delay for late events
    #[serde(default = "default_watermark_delay")]
    pub watermark_delay_secs: u64,
}

fn default_max_entities() -> usize {
    1_000_000
}

fn default_watermark_delay() -> u64 {
    5
}

/// A feature definition within a materialized view
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializedFeature {
    /// Feature name
    pub name: String,
    /// Aggregation function
    pub aggregation: AggregationFunc,
    /// JSON path to the field to aggregate (optional for count)
    pub field: Option<String>,
    /// Time window for the aggregation
    pub window: Option<WindowSpec>,
}

/// Aggregation functions
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AggregationFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    LastValue,
    FirstValue,
    CountDistinct,
}

impl std::fmt::Display for AggregationFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Count => write!(f, "count"),
            Self::Sum => write!(f, "sum"),
            Self::Avg => write!(f, "avg"),
            Self::Min => write!(f, "min"),
            Self::Max => write!(f, "max"),
            Self::LastValue => write!(f, "last_value"),
            Self::FirstValue => write!(f, "first_value"),
            Self::CountDistinct => write!(f, "count_distinct"),
        }
    }
}

/// Window specification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowSpec {
    /// Window type
    #[serde(rename = "type")]
    pub window_type: WindowType,
    /// Window size in seconds
    pub size_secs: u64,
    /// Slide interval in seconds (for sliding windows)
    pub slide_secs: Option<u64>,
}

/// Window types
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WindowType {
    Tumbling,
    Sliding,
    Session,
    /// No window — lifetime aggregation
    Global,
}

/// Internal state for a single entity's aggregated features
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityFeatureState {
    pub entity_key: String,
    pub features: HashMap<String, FeatureAccumulator>,
    pub last_updated: i64,
    pub event_count: u64,
}

/// Accumulator for computing running aggregations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureAccumulator {
    pub count: u64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
    pub last_value: Option<f64>,
    pub first_value: Option<f64>,
    pub distinct_values: Vec<String>,
}

impl Default for FeatureAccumulator {
    fn default() -> Self {
        Self {
            count: 0,
            sum: 0.0,
            min: f64::MAX,
            max: f64::MIN,
            last_value: None,
            first_value: None,
            distinct_values: Vec::new(),
        }
    }
}

impl FeatureAccumulator {
    /// Update the accumulator with a new value
    pub fn update(&mut self, value: f64) {
        self.count += 1;
        self.sum += value;
        if value < self.min {
            self.min = value;
        }
        if value > self.max {
            self.max = value;
        }
        self.last_value = Some(value);
        if self.first_value.is_none() {
            self.first_value = Some(value);
        }
    }

    /// Update with a string value (for count_distinct)
    pub fn update_distinct(&mut self, value: &str) {
        if !self.distinct_values.contains(&value.to_string()) {
            self.distinct_values.push(value.to_string());
        }
        self.count += 1;
    }

    /// Compute the aggregated value
    pub fn compute(&self, func: AggregationFunc) -> Option<f64> {
        match func {
            AggregationFunc::Count => Some(self.count as f64),
            AggregationFunc::Sum => Some(self.sum),
            AggregationFunc::Avg => {
                if self.count > 0 {
                    Some(self.sum / self.count as f64)
                } else {
                    None
                }
            }
            AggregationFunc::Min => {
                if self.count > 0 {
                    Some(self.min)
                } else {
                    None
                }
            }
            AggregationFunc::Max => {
                if self.count > 0 {
                    Some(self.max)
                } else {
                    None
                }
            }
            AggregationFunc::LastValue => self.last_value,
            AggregationFunc::FirstValue => self.first_value,
            AggregationFunc::CountDistinct => Some(self.distinct_values.len() as f64),
        }
    }
}

/// The materialized view engine manages multiple views
pub struct MaterializedViewEngine {
    views: Arc<RwLock<HashMap<String, MaterializedView>>>,
}

/// A single materialized view instance
pub struct MaterializedView {
    pub config: MaterializedViewConfig,
    pub entities: Arc<RwLock<HashMap<String, EntityFeatureState>>>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub events_processed: std::sync::atomic::AtomicU64,
}

impl MaterializedViewEngine {
    /// Create a new engine
    pub fn new() -> Self {
        Self {
            views: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a new materialized view
    pub async fn register_view(
        &self,
        config: MaterializedViewConfig,
    ) -> Result<(), String> {
        let name = config.name.clone();
        let view = MaterializedView {
            config,
            entities: Arc::new(RwLock::new(HashMap::new())),
            created_at: chrono::Utc::now(),
            events_processed: std::sync::atomic::AtomicU64::new(0),
        };
        self.views.write().await.insert(name.clone(), view);
        info!(view = %name, "Materialized view registered");
        Ok(())
    }

    /// Process an event for all matching views
    pub async fn process_event(
        &self,
        topic: &str,
        key: Option<&str>,
        value: &serde_json::Value,
        timestamp: i64,
    ) {
        let views = self.views.read().await;
        for (_, view) in views.iter() {
            if view.config.source_topic == topic {
                // Extract entity key from the event
                let entity_key = if let Some(k) = key {
                    k.to_string()
                } else {
                    extract_json_field(value, &view.config.entity_key)
                        .unwrap_or_default()
                };

                if entity_key.is_empty() {
                    continue;
                }

                let mut entities = view.entities.write().await;

                // Evict if at capacity
                if entities.len() >= view.config.max_entities
                    && !entities.contains_key(&entity_key)
                {
                    // Simple LRU: remove oldest entry
                    if let Some(oldest_key) = entities
                        .iter()
                        .min_by_key(|(_, state)| state.last_updated)
                        .map(|(k, _)| k.clone())
                    {
                        entities.remove(&oldest_key);
                    }
                }

                let state = entities
                    .entry(entity_key.clone())
                    .or_insert_with(|| EntityFeatureState {
                        entity_key: entity_key.clone(),
                        features: HashMap::new(),
                        last_updated: timestamp,
                        event_count: 0,
                    });

                state.last_updated = timestamp;
                state.event_count += 1;

                // Update each feature accumulator
                for feature_def in &view.config.features {
                    let acc = state
                        .features
                        .entry(feature_def.name.clone())
                        .or_insert_with(FeatureAccumulator::default);

                    if feature_def.aggregation == AggregationFunc::Count {
                        acc.update(1.0);
                    } else if let Some(ref field) = feature_def.field {
                        if let Some(field_val) = extract_json_field(value, field) {
                            if let Ok(num) = field_val.parse::<f64>() {
                                acc.update(num);
                            } else if feature_def.aggregation == AggregationFunc::CountDistinct
                            {
                                acc.update_distinct(&field_val);
                            }
                        }
                    }
                }

                view.events_processed
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }

    /// Get features for an entity from a specific view
    pub async fn get_features(
        &self,
        view_name: &str,
        entity_key: &str,
    ) -> Result<HashMap<String, Option<f64>>, String> {
        let views = self.views.read().await;
        let view = views
            .get(view_name)
            .ok_or_else(|| format!("View '{}' not found", view_name))?;

        let entities = view.entities.read().await;
        let state = entities
            .get(entity_key)
            .ok_or_else(|| format!("Entity '{}' not found in view '{}'", entity_key, view_name))?;

        let mut result = HashMap::new();
        for feature_def in &view.config.features {
            let value = state
                .features
                .get(&feature_def.name)
                .and_then(|acc| acc.compute(feature_def.aggregation));
            result.insert(feature_def.name.clone(), value);
        }

        Ok(result)
    }

    /// List all registered views
    pub async fn list_views(&self) -> Vec<String> {
        self.views.read().await.keys().cloned().collect()
    }

    /// Get view statistics
    pub async fn view_stats(&self, view_name: &str) -> Result<ViewStats, String> {
        let views = self.views.read().await;
        let view = views
            .get(view_name)
            .ok_or_else(|| format!("View '{}' not found", view_name))?;

        let entity_count = view.entities.read().await.len();
        Ok(ViewStats {
            name: view_name.to_string(),
            source_topic: view.config.source_topic.clone(),
            entity_count,
            events_processed: view
                .events_processed
                .load(std::sync::atomic::Ordering::Relaxed),
            feature_count: view.config.features.len(),
            created_at: view.created_at.to_rfc3339(),
        })
    }
}

impl Default for MaterializedViewEngine {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics for a materialized view
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewStats {
    pub name: String,
    pub source_topic: String,
    pub entity_count: usize,
    pub events_processed: u64,
    pub feature_count: usize,
    pub created_at: String,
}

/// Extract a field value from a JSON object by dot-notation path
fn extract_json_field(value: &serde_json::Value, path: &str) -> Option<String> {
    let parts: Vec<&str> = path.split('.').collect();
    let mut current = value;
    for part in parts {
        current = current.get(part)?;
    }
    match current {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::Bool(b) => Some(b.to_string()),
        _ => Some(current.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> MaterializedViewConfig {
        MaterializedViewConfig {
            name: "test_view".to_string(),
            source_topic: "purchases".to_string(),
            entity_key: "user_id".to_string(),
            features: vec![
                MaterializedFeature {
                    name: "total_purchases".to_string(),
                    aggregation: AggregationFunc::Count,
                    field: None,
                    window: None,
                },
                MaterializedFeature {
                    name: "total_amount".to_string(),
                    aggregation: AggregationFunc::Sum,
                    field: Some("amount".to_string()),
                    window: None,
                },
                MaterializedFeature {
                    name: "avg_amount".to_string(),
                    aggregation: AggregationFunc::Avg,
                    field: Some("amount".to_string()),
                    window: None,
                },
            ],
            max_entities: 100,
            entity_ttl_secs: None,
            watermark_delay_secs: 0,
        }
    }

    #[test]
    fn test_accumulator_basic() {
        let mut acc = FeatureAccumulator::default();
        acc.update(10.0);
        acc.update(20.0);
        acc.update(30.0);

        assert_eq!(acc.compute(AggregationFunc::Count), Some(3.0));
        assert_eq!(acc.compute(AggregationFunc::Sum), Some(60.0));
        assert_eq!(acc.compute(AggregationFunc::Avg), Some(20.0));
        assert_eq!(acc.compute(AggregationFunc::Min), Some(10.0));
        assert_eq!(acc.compute(AggregationFunc::Max), Some(30.0));
        assert_eq!(acc.compute(AggregationFunc::LastValue), Some(30.0));
        assert_eq!(acc.compute(AggregationFunc::FirstValue), Some(10.0));
    }

    #[test]
    fn test_extract_json_field() {
        let json = serde_json::json!({
            "user_id": "u123",
            "amount": 42.5,
            "nested": { "field": "value" }
        });
        assert_eq!(extract_json_field(&json, "user_id"), Some("u123".to_string()));
        assert_eq!(extract_json_field(&json, "amount"), Some("42.5".to_string()));
        assert_eq!(extract_json_field(&json, "nested.field"), Some("value".to_string()));
        assert_eq!(extract_json_field(&json, "missing"), None);
    }

    #[tokio::test]
    async fn test_materialized_view_engine() {
        let engine = MaterializedViewEngine::new();
        engine.register_view(test_config()).await.unwrap();

        // Process events
        for i in 0..5 {
            let event = serde_json::json!({
                "user_id": "user-1",
                "amount": 10.0 * (i + 1) as f64,
                "category": "electronics"
            });
            engine
                .process_event("purchases", Some("user-1"), &event, i as i64)
                .await;
        }

        // Query features
        let features = engine.get_features("test_view", "user-1").await.unwrap();
        assert_eq!(features["total_purchases"], Some(5.0));
        assert_eq!(features["total_amount"], Some(150.0)); // 10+20+30+40+50
        assert_eq!(features["avg_amount"], Some(30.0));
    }

    #[tokio::test]
    async fn test_view_stats() {
        let engine = MaterializedViewEngine::new();
        engine.register_view(test_config()).await.unwrap();

        let stats = engine.view_stats("test_view").await.unwrap();
        assert_eq!(stats.name, "test_view");
        assert_eq!(stats.feature_count, 3);
    }
}
