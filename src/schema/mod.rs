//! Schema Registry
//!
//! This module provides Confluent-compatible Schema Registry functionality,
//! supporting Avro, Protobuf, and JSON Schema formats.
//!
//! ## Features
//!
//! - Confluent Schema Registry API compatibility
//! - Schema storage in `_schemas` internal topic
//! - Compatibility checking (BACKWARD, FORWARD, FULL, NONE)
//! - Schema caching with TTL
//! - Optional schema validation on produce
//!
//! ## Supported Schema Types
//!
//! - **Avro**: Full compatibility checking using `apache-avro`
//! - **Protobuf**: Schema parsing and validation using `prost`
//! - **JSON Schema**: Draft-07 validation using `jsonschema`
//!
//! ## API Endpoints
//!
//! - `GET /subjects` - List all subjects
//! - `GET /subjects/{subject}/versions` - List versions for a subject
//! - `GET /subjects/{subject}/versions/{version}` - Get schema by version
//! - `GET /subjects/{subject}/versions/{version}/schema` - Get raw schema string
//! - `GET /subjects/{subject}/versions/{version}/referencedby` - Get referencing schema IDs
//! - `POST /subjects/{subject}/versions` - Register a new schema
//! - `POST /compatibility/subjects/{subject}/versions/{version}` - Check compatibility
//! - `DELETE /subjects/{subject}` - Delete a subject
//! - `DELETE /subjects/{subject}/versions/{version}` - Delete a version
//! - `GET /config` - Get global compatibility level
//! - `PUT /config` - Set global compatibility level
//! - `GET /config/{subject}` - Get subject compatibility level
//! - `PUT /config/{subject}` - Set subject compatibility level
//! - `DELETE /config/{subject}` - Delete subject config (reset to global)
//! - `GET /schemas/ids/{id}` - Get schema by ID
//! - `GET /schemas/ids/{id}/schema` - Get raw schema string by ID
//! - `GET /schemas/ids/{id}/subjects` - Get subjects for schema ID
//! - `GET /schemas/ids/{id}/versions` - Get subject-version pairs for schema ID
//! - `GET /schemas/types` - List supported schema types

pub mod avro;
pub mod compatibility;
pub mod inference;
pub mod json_schema;
pub mod protobuf;
pub mod store;

use serde::{Deserialize, Serialize};
use std::fmt;

/// Schema type enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "UPPERCASE")]
pub enum SchemaType {
    /// Apache Avro schema
    #[default]
    Avro,
    /// Protocol Buffers schema
    Protobuf,
    /// JSON Schema
    Json,
}

impl fmt::Display for SchemaType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SchemaType::Avro => write!(f, "AVRO"),
            SchemaType::Protobuf => write!(f, "PROTOBUF"),
            SchemaType::Json => write!(f, "JSON"),
        }
    }
}

impl std::str::FromStr for SchemaType {
    type Err = SchemaError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "AVRO" => Ok(SchemaType::Avro),
            "PROTOBUF" => Ok(SchemaType::Protobuf),
            "JSON" => Ok(SchemaType::Json),
            _ => Err(SchemaError::InvalidSchemaType(s.to_string())),
        }
    }
}

/// Compatibility level for schema evolution
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "UPPERCASE")]
pub enum CompatibilityLevel {
    /// No compatibility checking
    None,
    /// New schema can read data written by old schema
    #[default]
    Backward,
    /// Old schema can read data written by new schema
    Forward,
    /// Both backward and forward compatible
    Full,
    /// Backward compatible with all previous versions
    BackwardTransitive,
    /// Forward compatible with all previous versions
    ForwardTransitive,
    /// Full compatible with all previous versions
    FullTransitive,
}

impl fmt::Display for CompatibilityLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompatibilityLevel::None => write!(f, "NONE"),
            CompatibilityLevel::Backward => write!(f, "BACKWARD"),
            CompatibilityLevel::Forward => write!(f, "FORWARD"),
            CompatibilityLevel::Full => write!(f, "FULL"),
            CompatibilityLevel::BackwardTransitive => write!(f, "BACKWARD_TRANSITIVE"),
            CompatibilityLevel::ForwardTransitive => write!(f, "FORWARD_TRANSITIVE"),
            CompatibilityLevel::FullTransitive => write!(f, "FULL_TRANSITIVE"),
        }
    }
}

impl std::str::FromStr for CompatibilityLevel {
    type Err = SchemaError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "NONE" => Ok(CompatibilityLevel::None),
            "BACKWARD" => Ok(CompatibilityLevel::Backward),
            "FORWARD" => Ok(CompatibilityLevel::Forward),
            "FULL" => Ok(CompatibilityLevel::Full),
            "BACKWARD_TRANSITIVE" => Ok(CompatibilityLevel::BackwardTransitive),
            "FORWARD_TRANSITIVE" => Ok(CompatibilityLevel::ForwardTransitive),
            "FULL_TRANSITIVE" => Ok(CompatibilityLevel::FullTransitive),
            _ => Err(SchemaError::InvalidCompatibilityLevel(s.to_string())),
        }
    }
}

/// A registered schema with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisteredSchema {
    /// Subject name (usually topic-key or topic-value)
    pub subject: String,
    /// Schema version within the subject
    pub version: i32,
    /// Globally unique schema ID
    pub id: i32,
    /// Schema type
    #[serde(rename = "schemaType", default)]
    pub schema_type: SchemaType,
    /// The schema definition as a string
    pub schema: String,
    /// Schema references (for complex schemas)
    #[serde(default)]
    pub references: Vec<SchemaReference>,
}

/// Reference to another schema
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaReference {
    /// Reference name used in the schema
    pub name: String,
    /// Subject containing the referenced schema
    pub subject: String,
    /// Version of the referenced schema
    pub version: i32,
}

/// Schema registration request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaRegistrationRequest {
    /// The schema definition
    pub schema: String,
    /// Schema type (defaults to AVRO)
    #[serde(rename = "schemaType", default)]
    pub schema_type: SchemaType,
    /// Schema references
    #[serde(default)]
    pub references: Vec<SchemaReference>,
}

/// Schema registration response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaRegistrationResponse {
    /// Assigned schema ID
    pub id: i32,
}

/// Schema lookup response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaLookupResponse {
    /// Subject name
    pub subject: String,
    /// Schema ID
    pub id: i32,
    /// Schema version
    pub version: i32,
    /// Schema definition
    pub schema: String,
}

/// Compatibility check request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompatibilityCheckRequest {
    /// Schema to check
    pub schema: String,
    /// Schema type
    #[serde(rename = "schemaType", default)]
    pub schema_type: SchemaType,
    /// Schema references
    #[serde(default)]
    pub references: Vec<SchemaReference>,
}

/// Compatibility check response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompatibilityCheckResponse {
    /// Whether the schema is compatible
    pub is_compatible: bool,
    /// Messages explaining incompatibilities
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub messages: Vec<String>,
}

/// Config response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigResponse {
    /// Compatibility level
    #[serde(rename = "compatibilityLevel")]
    pub compatibility_level: CompatibilityLevel,
}

/// Config update request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigUpdateRequest {
    /// New compatibility level
    pub compatibility: CompatibilityLevel,
}

/// Subject version response
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct SubjectVersionResponse {
    /// Subject name
    pub subject: String,
    /// Version number
    pub version: i32,
}

/// Schema error types
#[derive(Debug, Clone, thiserror::Error)]
pub enum SchemaError {
    /// Schema not found
    #[error("Schema not found: {0}")]
    SchemaNotFound(String),

    /// Subject not found
    #[error("Subject not found: {0}")]
    SubjectNotFound(String),

    /// Version not found
    #[error("Version {version} not found for subject {subject}")]
    VersionNotFound { subject: String, version: i32 },

    /// Invalid schema
    #[error("Invalid schema: {0}")]
    InvalidSchema(String),

    /// Invalid schema type
    #[error("Invalid schema type: {0}")]
    InvalidSchemaType(String),

    /// Invalid compatibility level
    #[error("Invalid compatibility level: {0}")]
    InvalidCompatibilityLevel(String),

    /// Incompatible schema
    #[error("Schema is incompatible with previous version: {0}")]
    IncompatibleSchema(String),

    /// Storage error
    #[error("Storage error: {0}")]
    StorageError(String),

    /// Schema already exists
    #[error("Schema already registered with ID {0}")]
    SchemaAlreadyExists(i32),

    /// Serialization error
    #[error("Serialization error: {0}")]
    SerializationError(String),
}

/// Schema registry configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaRegistryConfig {
    /// Whether schema registry is enabled
    #[serde(default)]
    pub enabled: bool,

    /// Default compatibility level for new subjects
    #[serde(default)]
    pub default_compatibility: CompatibilityLevel,

    /// Cache TTL in seconds
    #[serde(default = "default_cache_ttl")]
    pub cache_ttl_seconds: u64,

    /// Maximum cache size (number of schemas)
    #[serde(default = "default_cache_size")]
    pub max_cache_size: usize,

    /// Whether to validate schemas on produce
    #[serde(default)]
    pub validate_on_produce: bool,

    /// Internal topic name for schema storage
    #[serde(default = "default_schema_topic")]
    pub schema_topic: String,
}

fn default_cache_ttl() -> u64 {
    300 // 5 minutes
}

fn default_cache_size() -> usize {
    1000
}

fn default_schema_topic() -> String {
    "_schemas".to_string()
}

impl Default for SchemaRegistryConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            default_compatibility: CompatibilityLevel::default(),
            cache_ttl_seconds: default_cache_ttl(),
            max_cache_size: default_cache_size(),
            validate_on_produce: false,
            schema_topic: default_schema_topic(),
        }
    }
}

// Re-export main types
pub use compatibility::CompatibilityChecker;
pub use store::{SchemaCache, SchemaRegistry, SchemaStore, SubjectVersionPair};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_type_display() {
        assert_eq!(SchemaType::Avro.to_string(), "AVRO");
        assert_eq!(SchemaType::Protobuf.to_string(), "PROTOBUF");
        assert_eq!(SchemaType::Json.to_string(), "JSON");
    }

    #[test]
    fn test_schema_type_parse() {
        assert_eq!("AVRO".parse::<SchemaType>().unwrap(), SchemaType::Avro);
        assert_eq!("avro".parse::<SchemaType>().unwrap(), SchemaType::Avro);
        assert_eq!(
            "PROTOBUF".parse::<SchemaType>().unwrap(),
            SchemaType::Protobuf
        );
        assert_eq!("JSON".parse::<SchemaType>().unwrap(), SchemaType::Json);
        assert!("INVALID".parse::<SchemaType>().is_err());
    }

    #[test]
    fn test_compatibility_level_display() {
        assert_eq!(CompatibilityLevel::None.to_string(), "NONE");
        assert_eq!(CompatibilityLevel::Backward.to_string(), "BACKWARD");
        assert_eq!(CompatibilityLevel::Forward.to_string(), "FORWARD");
        assert_eq!(CompatibilityLevel::Full.to_string(), "FULL");
        assert_eq!(
            CompatibilityLevel::BackwardTransitive.to_string(),
            "BACKWARD_TRANSITIVE"
        );
    }

    #[test]
    fn test_compatibility_level_parse() {
        assert_eq!(
            "BACKWARD".parse::<CompatibilityLevel>().unwrap(),
            CompatibilityLevel::Backward
        );
        assert_eq!(
            "backward".parse::<CompatibilityLevel>().unwrap(),
            CompatibilityLevel::Backward
        );
        assert_eq!(
            "FULL_TRANSITIVE".parse::<CompatibilityLevel>().unwrap(),
            CompatibilityLevel::FullTransitive
        );
        assert!("INVALID".parse::<CompatibilityLevel>().is_err());
    }

    #[test]
    fn test_schema_registry_config_default() {
        let config = SchemaRegistryConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.default_compatibility, CompatibilityLevel::Backward);
        assert_eq!(config.cache_ttl_seconds, 300);
        assert_eq!(config.max_cache_size, 1000);
        assert!(!config.validate_on_produce);
        assert_eq!(config.schema_topic, "_schemas");
    }

    #[test]
    fn test_registered_schema_serialization() {
        let schema = RegisteredSchema {
            subject: "test-value".to_string(),
            version: 1,
            id: 1,
            schema_type: SchemaType::Avro,
            schema: r#"{"type":"string"}"#.to_string(),
            references: vec![],
        };

        let json = serde_json::to_string(&schema).unwrap();
        let parsed: RegisteredSchema = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.subject, schema.subject);
        assert_eq!(parsed.version, schema.version);
        assert_eq!(parsed.id, schema.id);
        assert_eq!(parsed.schema_type, schema.schema_type);
    }
}
