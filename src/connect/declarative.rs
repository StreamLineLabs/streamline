//! Declarative Connector Configuration
//!
//! Enables defining connector pipelines in YAML/JSON files for GitOps-friendly
//! deployment. Connectors are specified declaratively and applied via the CLI
//! or REST API.
//!
//! # Example YAML
//!
//! ```yaml
//! apiVersion: streamline.io/v1
//! kind: ConnectorPipeline
//! metadata:
//!   name: postgres-to-events
//! spec:
//!   source:
//!     type: postgres-cdc
//!     config:
//!       connection_url: postgresql://user:pass@localhost:5432/mydb
//!       publication: streamline_pub
//!       slot_name: streamline_slot
//!       tables:
//!         - public.orders
//!         - public.customers
//!   transforms:
//!     - type: field-filter
//!       config:
//!         include: [id, name, email, created_at]
//!     - type: pii-redactor
//!       config:
//!         fields: [email, phone]
//!   sink:
//!     type: streamline-topic
//!     config:
//!       topic_prefix: cdc
//!       partitions: 6
//!   error_handling:
//!     dead_letter_topic: dlq-postgres
//!     max_retries: 3
//!     retry_backoff_ms: 1000
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use tracing::{debug, info, warn};

/// Top-level connector pipeline manifest
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorManifest {
    #[serde(rename = "apiVersion", default = "default_api_version")]
    pub api_version: String,
    pub kind: String,
    pub metadata: ConnectorMetadata,
    pub spec: ConnectorPipelineSpec,
}

fn default_api_version() -> String {
    "streamline.io/v1".to_string()
}

/// Connector pipeline metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorMetadata {
    pub name: String,
    #[serde(default)]
    pub namespace: String,
    #[serde(default)]
    pub labels: HashMap<String, String>,
    #[serde(default)]
    pub annotations: HashMap<String, String>,
}

/// Pipeline specification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorPipelineSpec {
    /// Source connector configuration
    pub source: ConnectorEndpoint,
    /// Optional transform chain
    #[serde(default)]
    pub transforms: Vec<TransformStep>,
    /// Sink connector configuration
    pub sink: ConnectorEndpoint,
    /// Error handling configuration
    #[serde(default)]
    pub error_handling: ErrorHandlingConfig,
    /// Pipeline parallelism (number of tasks)
    #[serde(default = "default_parallelism")]
    pub parallelism: usize,
}

fn default_parallelism() -> usize {
    1
}

/// A connector endpoint (source or sink)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorEndpoint {
    /// Connector type (e.g., "postgres-cdc", "http-webhook", "streamline-topic", "s3")
    #[serde(rename = "type")]
    pub connector_type: String,
    /// Connector-specific configuration
    #[serde(default)]
    pub config: HashMap<String, serde_json::Value>,
}

/// A transform step in the pipeline
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformStep {
    /// Transform type (e.g., "field-filter", "pii-redactor", "json-to-avro")
    #[serde(rename = "type")]
    pub transform_type: String,
    /// Transform-specific configuration
    #[serde(default)]
    pub config: HashMap<String, serde_json::Value>,
}

/// Error handling configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorHandlingConfig {
    /// Dead letter topic for failed messages
    #[serde(default = "default_dlq_topic")]
    pub dead_letter_topic: String,
    /// Maximum retry attempts
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    /// Initial retry backoff in milliseconds
    #[serde(default = "default_retry_backoff")]
    pub retry_backoff_ms: u64,
    /// Maximum retry backoff in milliseconds
    #[serde(default = "default_max_backoff")]
    pub max_retry_backoff_ms: u64,
    /// Whether to stop the pipeline on error (vs. skip and continue)
    #[serde(default)]
    pub fail_on_error: bool,
}

fn default_dlq_topic() -> String {
    "dlq".to_string()
}
fn default_max_retries() -> u32 {
    3
}
fn default_retry_backoff() -> u64 {
    1000
}
fn default_max_backoff() -> u64 {
    30000
}

impl Default for ErrorHandlingConfig {
    fn default() -> Self {
        Self {
            dead_letter_topic: default_dlq_topic(),
            max_retries: default_max_retries(),
            retry_backoff_ms: default_retry_backoff(),
            max_retry_backoff_ms: default_max_backoff(),
            fail_on_error: false,
        }
    }
}

/// Known connector types with their validation rules
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorTypeInfo {
    pub name: String,
    pub description: String,
    pub direction: ConnectorDirection,
    pub required_config: Vec<String>,
    pub optional_config: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ConnectorDirection {
    Source,
    Sink,
    Both,
}

/// Registry of known connector types
pub fn known_connector_types() -> Vec<ConnectorTypeInfo> {
    vec![
        ConnectorTypeInfo {
            name: "postgres-cdc".to_string(),
            description: "PostgreSQL Change Data Capture via logical replication".to_string(),
            direction: ConnectorDirection::Source,
            required_config: vec!["connection_url".to_string()],
            optional_config: vec![
                "publication".to_string(),
                "slot_name".to_string(),
                "tables".to_string(),
            ],
        },
        ConnectorTypeInfo {
            name: "mysql-cdc".to_string(),
            description: "MySQL Change Data Capture via binlog".to_string(),
            direction: ConnectorDirection::Source,
            required_config: vec!["connection_url".to_string()],
            optional_config: vec!["server_id".to_string(), "tables".to_string()],
        },
        ConnectorTypeInfo {
            name: "http-webhook".to_string(),
            description: "HTTP webhook source/sink".to_string(),
            direction: ConnectorDirection::Both,
            required_config: vec!["url".to_string()],
            optional_config: vec![
                "method".to_string(),
                "headers".to_string(),
                "interval_ms".to_string(),
            ],
        },
        ConnectorTypeInfo {
            name: "s3".to_string(),
            description: "Amazon S3 / compatible object storage sink".to_string(),
            direction: ConnectorDirection::Sink,
            required_config: vec!["bucket".to_string(), "region".to_string()],
            optional_config: vec![
                "prefix".to_string(),
                "format".to_string(),
                "batch_size".to_string(),
            ],
        },
        ConnectorTypeInfo {
            name: "streamline-topic".to_string(),
            description: "Streamline topic source/sink (for internal routing)".to_string(),
            direction: ConnectorDirection::Both,
            required_config: vec![],
            optional_config: vec![
                "topic_prefix".to_string(),
                "partitions".to_string(),
                "topic".to_string(),
            ],
        },
        ConnectorTypeInfo {
            name: "file".to_string(),
            description: "File source (reads lines from files) or file sink".to_string(),
            direction: ConnectorDirection::Both,
            required_config: vec!["path".to_string()],
            optional_config: vec!["format".to_string(), "delimiter".to_string()],
        },
        ConnectorTypeInfo {
            name: "console".to_string(),
            description: "Console sink (prints to stdout, useful for debugging)".to_string(),
            direction: ConnectorDirection::Sink,
            required_config: vec![],
            optional_config: vec!["format".to_string()],
        },
    ]
}

/// Known transform types
pub fn known_transform_types() -> Vec<&'static str> {
    vec![
        "field-filter",
        "field-rename",
        "field-router",
        "pii-redactor",
        "json-filter",
        "json-to-avro",
        "csv-to-json",
        "schema-validator",
        "timestamp-enricher",
        "deduplicator",
        "rate-limiter",
    ]
}

/// Validation result for a connector manifest
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationResult {
    pub valid: bool,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
}

/// Validate a connector manifest
pub fn validate_manifest(manifest: &ConnectorManifest) -> ValidationResult {
    let mut errors = Vec::new();
    let mut warnings = Vec::new();

    // Validate API version
    if !manifest.api_version.starts_with("streamline") {
        warnings.push(format!(
            "Unexpected apiVersion '{}', expected 'streamline.io/v1'",
            manifest.api_version
        ));
    }

    // Validate kind
    if manifest.kind != "ConnectorPipeline" {
        errors.push(format!(
            "Invalid kind '{}', expected 'ConnectorPipeline'",
            manifest.kind
        ));
    }

    // Validate metadata
    if manifest.metadata.name.is_empty() {
        errors.push("metadata.name is required".to_string());
    }

    // Validate source connector
    let known_types = known_connector_types();
    if let Some(type_info) = known_types
        .iter()
        .find(|t| t.name == manifest.spec.source.connector_type)
    {
        if type_info.direction == ConnectorDirection::Sink {
            errors.push(format!(
                "Connector type '{}' is a sink, not a source",
                manifest.spec.source.connector_type
            ));
        }
        for required in &type_info.required_config {
            if !manifest.spec.source.config.contains_key(required) {
                errors.push(format!(
                    "Source connector '{}' requires config key '{}'",
                    manifest.spec.source.connector_type, required
                ));
            }
        }
    } else {
        warnings.push(format!(
            "Unknown source connector type '{}' — will attempt dynamic loading",
            manifest.spec.source.connector_type
        ));
    }

    // Validate sink connector
    if let Some(type_info) = known_types
        .iter()
        .find(|t| t.name == manifest.spec.sink.connector_type)
    {
        if type_info.direction == ConnectorDirection::Source {
            errors.push(format!(
                "Connector type '{}' is a source, not a sink",
                manifest.spec.sink.connector_type
            ));
        }
        for required in &type_info.required_config {
            if !manifest.spec.sink.config.contains_key(required) {
                errors.push(format!(
                    "Sink connector '{}' requires config key '{}'",
                    manifest.spec.sink.connector_type, required
                ));
            }
        }
    } else {
        warnings.push(format!(
            "Unknown sink connector type '{}' — will attempt dynamic loading",
            manifest.spec.sink.connector_type
        ));
    }

    // Validate transforms
    let known_transforms = known_transform_types();
    for (i, transform) in manifest.spec.transforms.iter().enumerate() {
        if !known_transforms.contains(&transform.transform_type.as_str()) {
            warnings.push(format!(
                "Transform #{} has unknown type '{}' — will attempt WASM loading",
                i + 1,
                transform.transform_type
            ));
        }
    }

    // Validate parallelism
    if manifest.spec.parallelism == 0 {
        errors.push("spec.parallelism must be >= 1".to_string());
    }

    ValidationResult {
        valid: errors.is_empty(),
        errors,
        warnings,
    }
}

/// Load a connector manifest from a YAML file
pub fn load_manifest_from_file(path: &Path) -> Result<ConnectorManifest, String> {
    let content =
        std::fs::read_to_string(path).map_err(|e| format!("Failed to read {}: {}", path.display(), e))?;
    load_manifest_from_str(&content)
}

/// Parse a connector manifest from a YAML or JSON string
pub fn load_manifest_from_str(content: &str) -> Result<ConnectorManifest, String> {
    serde_yaml::from_str(content)
        .or_else(|_| serde_json::from_str(content).map_err(|e| e.to_string()))
        .map_err(|e| format!("Failed to parse connector manifest: {}", e))
}

/// Generate a scaffold YAML manifest for a given connector type
pub fn scaffold_manifest(source_type: &str, sink_type: &str, name: &str) -> String {
    format!(
        r#"apiVersion: streamline.io/v1
kind: ConnectorPipeline
metadata:
  name: {name}
spec:
  source:
    type: {source_type}
    config:
      # Add {source_type}-specific configuration here
      {}
  transforms: []
  sink:
    type: {sink_type}
    config:
      # Add {sink_type}-specific configuration here
      {}
  error_handling:
    dead_letter_topic: dlq-{name}
    max_retries: 3
    retry_backoff_ms: 1000
  parallelism: 1
"#,
        known_connector_types()
            .iter()
            .find(|t| t.name == source_type)
            .map(|t| t
                .required_config
                .iter()
                .map(|k| format!("# {}: <required>", k))
                .collect::<Vec<_>>()
                .join("\n      "))
            .unwrap_or_default(),
        known_connector_types()
            .iter()
            .find(|t| t.name == sink_type)
            .map(|t| t
                .required_config
                .iter()
                .map(|k| format!("# {}: <required>", k))
                .collect::<Vec<_>>()
                .join("\n      "))
            .unwrap_or_default(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_manifest() -> ConnectorManifest {
        let yaml = r#"
apiVersion: streamline.io/v1
kind: ConnectorPipeline
metadata:
  name: test-pipeline
spec:
  source:
    type: postgres-cdc
    config:
      connection_url: postgresql://localhost:5432/test
  transforms:
    - type: field-filter
      config:
        include: ["id", "name"]
  sink:
    type: streamline-topic
    config:
      topic_prefix: cdc
"#;
        load_manifest_from_str(yaml).unwrap()
    }

    #[test]
    fn test_parse_manifest() {
        let manifest = sample_manifest();
        assert_eq!(manifest.metadata.name, "test-pipeline");
        assert_eq!(manifest.spec.source.connector_type, "postgres-cdc");
        assert_eq!(manifest.spec.sink.connector_type, "streamline-topic");
        assert_eq!(manifest.spec.transforms.len(), 1);
    }

    #[test]
    fn test_validate_manifest_valid() {
        let manifest = sample_manifest();
        let result = validate_manifest(&manifest);
        assert!(result.valid, "Errors: {:?}", result.errors);
    }

    #[test]
    fn test_validate_manifest_missing_required() {
        let yaml = r#"
apiVersion: streamline.io/v1
kind: ConnectorPipeline
metadata:
  name: broken
spec:
  source:
    type: postgres-cdc
    config: {}
  transforms: []
  sink:
    type: s3
    config: {}
"#;
        let manifest = load_manifest_from_str(yaml).unwrap();
        let result = validate_manifest(&manifest);
        assert!(!result.valid);
        assert!(result.errors.iter().any(|e| e.contains("connection_url")));
        assert!(result.errors.iter().any(|e| e.contains("bucket")));
    }

    #[test]
    fn test_validate_wrong_direction() {
        let yaml = r#"
apiVersion: streamline.io/v1
kind: ConnectorPipeline
metadata:
  name: wrong-dir
spec:
  source:
    type: console
    config: {}
  transforms: []
  sink:
    type: streamline-topic
    config: {}
"#;
        let manifest = load_manifest_from_str(yaml).unwrap();
        let result = validate_manifest(&manifest);
        assert!(!result.valid);
        assert!(result.errors.iter().any(|e| e.contains("is a sink")));
    }

    #[test]
    fn test_scaffold_manifest() {
        let yaml = scaffold_manifest("postgres-cdc", "s3", "my-pipeline");
        assert!(yaml.contains("postgres-cdc"));
        assert!(yaml.contains("s3"));
        assert!(yaml.contains("my-pipeline"));
    }

    #[test]
    fn test_known_connector_types() {
        let types = known_connector_types();
        assert!(types.len() >= 5);
        assert!(types.iter().any(|t| t.name == "postgres-cdc"));
        assert!(types.iter().any(|t| t.name == "s3"));
    }
}
