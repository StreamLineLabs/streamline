//! WebAssembly (WASM) message transformation framework
//!
//! This module provides the ability to transform messages in-flight using
//! WebAssembly modules. This enables:
//!
//! - Message filtering
//! - Format transformation (e.g., JSON to Avro)
//! - Field extraction and routing
//! - Data masking and redaction
//! - Custom business logic
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                   TransformationEngine                          │
//! │                                                                 │
//! │  ┌──────────────────┐    ┌──────────────────────────────────┐  │
//! │  │ WasmModuleLoader │───▶│ TransformationRegistry            │  │
//! │  │ (.wasm files)    │    │ (topic → transformation mapping)  │  │
//! │  └──────────────────┘    └──────────────────────────────────┘  │
//! │                                                                 │
//! │  ┌──────────────────────────────────────────────────────────┐  │
//! │  │                 WasmRuntime (wasmtime)                    │  │
//! │  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐       │  │
//! │  │  │ Instance 1  │  │ Instance 2  │  │ Instance N  │       │  │
//! │  │  │ (filter)    │  │ (transform) │  │ (route)     │       │  │
//! │  │  └─────────────┘  └─────────────┘  └─────────────┘       │  │
//! │  └──────────────────────────────────────────────────────────┘  │
//! │                                                                 │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Transformation Types
//!
//! - **Filter**: Drop messages matching a predicate
//! - **Transform**: Modify message content
//! - **Route**: Change destination topic based on content
//! - **Enrich**: Add fields from external sources
//! - **Aggregate**: Combine multiple messages

pub mod error;

use error::Result;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

pub mod api;
pub mod builtins;
pub mod catalog;
pub mod engine;
pub mod faas;
pub mod function_registry;
pub mod pipeline;
pub mod runtime;

// Re-export engine types

// Re-export FaaS types

/// Transformation configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformationConfig {
    /// Unique identifier for this transformation
    pub id: String,

    /// Human-readable name
    pub name: String,

    /// Description of what this transformation does
    pub description: Option<String>,

    /// Path to the WASM module
    pub wasm_path: PathBuf,

    /// Entry point function name
    pub entry_point: String,

    /// Topics this transformation applies to (empty = all topics)
    pub topics: Vec<String>,

    /// Transformation type
    pub transform_type: TransformationType,

    /// Whether the transformation is enabled
    pub enabled: bool,

    /// Maximum execution time in milliseconds
    pub timeout_ms: u64,

    /// Maximum memory in bytes
    pub max_memory_bytes: usize,

    /// Additional configuration passed to the WASM module
    pub config: HashMap<String, String>,
}

impl Default for TransformationConfig {
    fn default() -> Self {
        Self {
            id: String::new(),
            name: String::new(),
            description: None,
            wasm_path: PathBuf::new(),
            entry_point: "transform".to_string(),
            topics: Vec::new(),
            transform_type: TransformationType::Transform,
            enabled: true,
            timeout_ms: 1000,
            max_memory_bytes: 64 * 1024 * 1024, // 64MB
            config: HashMap::new(),
        }
    }
}

/// Types of transformations
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum TransformationType {
    /// Filter messages based on a predicate
    Filter,

    /// Transform message content
    #[default]
    Transform,

    /// Route messages to different topics
    Route,

    /// Enrich messages with additional data
    Enrich,

    /// Aggregate multiple messages
    Aggregate,
}

impl std::fmt::Display for TransformationType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransformationType::Filter => write!(f, "filter"),
            TransformationType::Transform => write!(f, "transform"),
            TransformationType::Route => write!(f, "route"),
            TransformationType::Enrich => write!(f, "enrich"),
            TransformationType::Aggregate => write!(f, "aggregate"),
        }
    }
}

/// Message passed to transformation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformInput {
    /// Source topic
    pub topic: String,

    /// Partition
    pub partition: i32,

    /// Message key (if present)
    pub key: Option<Vec<u8>>,

    /// Message value
    pub value: Vec<u8>,

    /// Message headers
    pub headers: HashMap<String, Vec<u8>>,

    /// Timestamp in milliseconds
    pub timestamp_ms: i64,
}

/// Result of a transformation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TransformOutput {
    /// Pass through unchanged
    Pass(TransformInput),

    /// Drop the message
    Drop,

    /// Transform to new message(s)
    Transformed(Vec<TransformResult>),

    /// Route to different topic(s)
    Route(Vec<RouteResult>),

    /// Error during transformation
    Error(String),
}

/// A transformed message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformResult {
    /// New key (None = keep original)
    pub key: Option<Vec<u8>>,

    /// New value
    pub value: Vec<u8>,

    /// New headers (merged with original)
    pub headers: HashMap<String, Vec<u8>>,
}

/// Routing result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteResult {
    /// Target topic
    pub topic: String,

    /// Target partition (None = auto-assign)
    pub partition: Option<i32>,

    /// Message key
    pub key: Option<Vec<u8>>,

    /// Message value
    pub value: Vec<u8>,

    /// Message headers
    pub headers: HashMap<String, Vec<u8>>,
}

/// Statistics for a transformation
#[derive(Debug, Default, Clone, Serialize)]
pub struct TransformationStats {
    /// Total messages processed
    pub messages_processed: u64,

    /// Messages passed through
    pub messages_passed: u64,

    /// Messages dropped
    pub messages_dropped: u64,

    /// Messages transformed
    pub messages_transformed: u64,

    /// Messages routed
    pub messages_routed: u64,

    /// Errors encountered
    pub errors: u64,

    /// Total processing time in microseconds
    pub total_time_us: u64,

    /// Average processing time in microseconds
    pub avg_time_us: f64,
}

/// Registry of available transformations
pub struct TransformationRegistry {
    /// Transformations by ID
    transformations: RwLock<HashMap<String, TransformationConfig>>,

    /// Topic to transformation mapping
    topic_mapping: RwLock<HashMap<String, Vec<String>>>,

    /// Statistics per transformation
    stats: RwLock<HashMap<String, TransformationStats>>,
}

impl TransformationRegistry {
    /// Create a new transformation registry
    pub fn new() -> Self {
        Self {
            transformations: RwLock::new(HashMap::new()),
            topic_mapping: RwLock::new(HashMap::new()),
            stats: RwLock::new(HashMap::new()),
        }
    }

    /// Register a transformation
    pub async fn register(&self, config: TransformationConfig) -> Result<()> {
        let id = config.id.clone();
        let topics = config.topics.clone();

        // Add to transformations map
        let mut transformations = self.transformations.write();
        transformations.insert(id.clone(), config);

        // Update topic mapping
        let mut topic_mapping = self.topic_mapping.write();
        for topic in topics {
            topic_mapping.entry(topic).or_default().push(id.clone());
        }

        // Initialize stats
        let mut stats = self.stats.write();
        stats.insert(id, TransformationStats::default());

        Ok(())
    }

    /// Unregister a transformation
    pub async fn unregister(&self, id: &str) -> Result<()> {
        let mut transformations = self.transformations.write();
        if let Some(config) = transformations.remove(id) {
            // Remove from topic mapping
            let mut topic_mapping = self.topic_mapping.write();
            for topic in &config.topics {
                if let Some(ids) = topic_mapping.get_mut(topic) {
                    ids.retain(|i| i != id);
                }
            }

            // Remove stats
            let mut stats = self.stats.write();
            stats.remove(id);
        }
        Ok(())
    }

    /// Get transformations for a topic
    pub async fn get_for_topic(&self, topic: &str) -> Vec<TransformationConfig> {
        let topic_mapping = self.topic_mapping.read();
        let transformations = self.transformations.read();

        // Get specific topic transformations
        let mut configs = Vec::new();
        if let Some(ids) = topic_mapping.get(topic) {
            for id in ids {
                if let Some(config) = transformations.get(id) {
                    if config.enabled {
                        configs.push(config.clone());
                    }
                }
            }
        }

        // Add global transformations (empty topics list)
        for config in transformations.values() {
            if config.enabled && config.topics.is_empty() {
                configs.push(config.clone());
            }
        }

        configs
    }

    /// Get statistics for a transformation
    pub async fn get_stats(&self, id: &str) -> Option<TransformationStats> {
        let stats = self.stats.read();
        stats.get(id).cloned()
    }

    /// Update statistics
    #[allow(clippy::too_many_arguments)]
    pub async fn update_stats(
        &self,
        id: &str,
        processed: u64,
        passed: u64,
        dropped: u64,
        transformed: u64,
        routed: u64,
        errors: u64,
        time_us: u64,
    ) {
        let mut stats = self.stats.write();
        if let Some(s) = stats.get_mut(id) {
            s.messages_processed += processed;
            s.messages_passed += passed;
            s.messages_dropped += dropped;
            s.messages_transformed += transformed;
            s.messages_routed += routed;
            s.errors += errors;
            s.total_time_us += time_us;
            if s.messages_processed > 0 {
                s.avg_time_us = s.total_time_us as f64 / s.messages_processed as f64;
            }
        }
    }

    /// List all transformations
    pub async fn list(&self) -> Vec<TransformationConfig> {
        let transformations = self.transformations.read();
        transformations.values().cloned().collect()
    }
}

impl Default for TransformationRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_registry_register() {
        let registry = TransformationRegistry::new();

        let config = TransformationConfig {
            id: "test-transform".to_string(),
            name: "Test Transformation".to_string(),
            topics: vec!["topic-a".to_string(), "topic-b".to_string()],
            ..Default::default()
        };

        registry.register(config).await.unwrap();

        let configs = registry.get_for_topic("topic-a").await;
        assert_eq!(configs.len(), 1);
        assert_eq!(configs[0].id, "test-transform");
    }

    #[tokio::test]
    async fn test_registry_global_transformation() {
        let registry = TransformationRegistry::new();

        // Global transformation (empty topics)
        let config = TransformationConfig {
            id: "global-transform".to_string(),
            name: "Global Transformation".to_string(),
            topics: vec![], // Empty = applies to all topics
            ..Default::default()
        };

        registry.register(config).await.unwrap();

        // Should apply to any topic
        let configs = registry.get_for_topic("any-topic").await;
        assert_eq!(configs.len(), 1);
        assert_eq!(configs[0].id, "global-transform");
    }

    #[test]
    fn test_transformation_type_display() {
        assert_eq!(TransformationType::Filter.to_string(), "filter");
        assert_eq!(TransformationType::Transform.to_string(), "transform");
        assert_eq!(TransformationType::Route.to_string(), "route");
    }
}
