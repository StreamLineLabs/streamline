//! Marketplace Web Registry — REST API for the WASM transform marketplace
//!
//! Provides endpoints for searching, browsing, and managing transforms
//! in the Streamline WASM transform marketplace.
//!
//! ## Endpoints
//!
//! - `GET /api/v1/marketplace/transforms` — Search and list transforms
//! - `GET /api/v1/marketplace/transforms/:name` — Get transform details
//! - `GET /api/v1/marketplace/categories` — List transform categories
//! - `GET /api/v1/marketplace/stats` — Marketplace statistics
//! - `POST /api/v1/marketplace/pipelines` — Create a visual pipeline
//! - `GET /api/v1/marketplace/pipelines` — List saved pipelines

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A published transform in the marketplace
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketplaceTransform {
    pub name: String,
    pub version: String,
    pub description: String,
    pub author: String,
    pub category: TransformCategory,
    pub downloads: u64,
    pub rating: f32,
    pub tags: Vec<String>,
    pub wasm_size_bytes: u64,
    pub published_at: String,
    pub source_url: Option<String>,
    pub readme: Option<String>,
}

/// Transform categories for browsing
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransformCategory {
    Filter,
    Enrichment,
    Routing,
    Serialization,
    Security,
    Analytics,
    Validation,
    Custom,
}

impl std::fmt::Display for TransformCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransformCategory::Filter => write!(f, "Filter"),
            TransformCategory::Enrichment => write!(f, "Enrichment"),
            TransformCategory::Routing => write!(f, "Routing"),
            TransformCategory::Serialization => write!(f, "Serialization"),
            TransformCategory::Security => write!(f, "Security"),
            TransformCategory::Analytics => write!(f, "Analytics"),
            TransformCategory::Validation => write!(f, "Validation"),
            TransformCategory::Custom => write!(f, "Custom"),
        }
    }
}

/// Search parameters for marketplace
#[derive(Debug, Deserialize)]
pub struct SearchParams {
    pub query: Option<String>,
    pub category: Option<TransformCategory>,
    pub sort: Option<SortField>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub enum SortField {
    #[serde(rename = "downloads")]
    Downloads,
    #[serde(rename = "rating")]
    Rating,
    #[serde(rename = "recent")]
    Recent,
    #[serde(rename = "name")]
    Name,
}

/// Visual pipeline definition for the pipeline builder
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VisualPipeline {
    pub name: String,
    pub description: Option<String>,
    pub steps: Vec<PipelineStep>,
    pub source_topic: String,
    pub sink_topic: Option<String>,
    pub created_at: String,
}

/// A step in a visual pipeline
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineStep {
    pub id: String,
    pub transform_name: String,
    pub config: HashMap<String, serde_json::Value>,
    pub position: PipelinePosition,
    pub connections: Vec<String>,
}

/// Position of a step in the visual pipeline builder
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelinePosition {
    pub x: f64,
    pub y: f64,
}

/// Marketplace statistics
#[derive(Debug, Serialize)]
pub struct MarketplaceStats {
    pub total_transforms: usize,
    pub total_downloads: u64,
    pub total_authors: usize,
    pub categories: HashMap<String, usize>,
    pub top_transforms: Vec<String>,
}

/// Get the built-in transforms shipped with Streamline
pub fn builtin_transforms() -> Vec<MarketplaceTransform> {
    vec![
        MarketplaceTransform {
            name: "json-filter".into(),
            version: "0.2.0".into(),
            description: "Filter messages by JSON field values using JSONPath expressions".into(),
            author: "StreamlineLabs".into(),
            category: TransformCategory::Filter,
            downloads: 0,
            rating: 5.0,
            tags: vec!["json".into(), "filter".into(), "jsonpath".into()],
            wasm_size_bytes: 45_000,
            published_at: "2026-01-23".into(),
            source_url: Some("https://github.com/streamlinelabs/streamline-marketplace/tree/main/transforms/json-filter".into()),
            readme: None,
        },
        MarketplaceTransform {
            name: "timestamp-enricher".into(),
            version: "0.2.0".into(),
            description: "Add processing timestamps and formatted dates to messages".into(),
            author: "StreamlineLabs".into(),
            category: TransformCategory::Enrichment,
            downloads: 0,
            rating: 5.0,
            tags: vec!["timestamp".into(), "enrichment".into(), "datetime".into()],
            wasm_size_bytes: 32_000,
            published_at: "2026-01-23".into(),
            source_url: Some("https://github.com/streamlinelabs/streamline-marketplace/tree/main/transforms/timestamp-enricher".into()),
            readme: None,
        },
        MarketplaceTransform {
            name: "pii-redactor".into(),
            version: "0.2.0".into(),
            description: "Redact personally identifiable information (email, SSN, phone) from messages".into(),
            author: "StreamlineLabs".into(),
            category: TransformCategory::Security,
            downloads: 0,
            rating: 5.0,
            tags: vec!["pii".into(), "redaction".into(), "privacy".into(), "gdpr".into()],
            wasm_size_bytes: 52_000,
            published_at: "2026-01-23".into(),
            source_url: Some("https://github.com/streamlinelabs/streamline-marketplace/tree/main/transforms/pii-redactor".into()),
            readme: None,
        },
        MarketplaceTransform {
            name: "schema-validator".into(),
            version: "0.2.0".into(),
            description: "Validate messages against JSON Schema, routing invalid messages to DLQ".into(),
            author: "StreamlineLabs".into(),
            category: TransformCategory::Validation,
            downloads: 0,
            rating: 5.0,
            tags: vec!["schema".into(), "validation".into(), "json-schema".into()],
            wasm_size_bytes: 48_000,
            published_at: "2026-01-23".into(),
            source_url: Some("https://github.com/streamlinelabs/streamline-marketplace/tree/main/transforms/schema-validator".into()),
            readme: None,
        },
        MarketplaceTransform {
            name: "field-router".into(),
            version: "0.2.0".into(),
            description: "Route messages to different topics based on field values".into(),
            author: "StreamlineLabs".into(),
            category: TransformCategory::Routing,
            downloads: 0,
            rating: 5.0,
            tags: vec!["routing".into(), "field-based".into(), "topic-routing".into()],
            wasm_size_bytes: 38_000,
            published_at: "2026-01-23".into(),
            source_url: Some("https://github.com/streamlinelabs/streamline-marketplace/tree/main/transforms/field-router".into()),
            readme: None,
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builtin_transforms() {
        let transforms = builtin_transforms();
        assert_eq!(transforms.len(), 5, "Must have 5 built-in transforms");

        let names: Vec<&str> = transforms.iter().map(|t| t.name.as_str()).collect();
        assert!(names.contains(&"json-filter"));
        assert!(names.contains(&"pii-redactor"));
        assert!(names.contains(&"field-router"));
    }

    #[test]
    fn test_visual_pipeline_serialization() {
        let pipeline = VisualPipeline {
            name: "test-pipeline".into(),
            description: Some("Test".into()),
            steps: vec![PipelineStep {
                id: "step-1".into(),
                transform_name: "json-filter".into(),
                config: HashMap::new(),
                position: PipelinePosition { x: 100.0, y: 50.0 },
                connections: vec!["step-2".into()],
            }],
            source_topic: "input".into(),
            sink_topic: Some("output".into()),
            created_at: "2026-01-01".into(),
        };
        let json = serde_json::to_string(&pipeline).unwrap();
        assert!(json.contains("test-pipeline"));
    }

    #[test]
    fn test_transform_categories() {
        assert_eq!(TransformCategory::Filter.to_string(), "Filter");
        assert_eq!(TransformCategory::Security.to_string(), "Security");
    }
}
