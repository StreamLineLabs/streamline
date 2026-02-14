//! Contract definitions for stream topics.

use serde::{Deserialize, Serialize};

/// A stream contract defining expected schema and invariants for a topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamContract {
    /// Contract name
    pub name: String,
    /// Target topic
    pub topic: String,
    /// Contract specification
    pub spec: ContractSpec,
    /// Optional description
    #[serde(default)]
    pub description: String,
    /// Contract version
    #[serde(default = "default_version")]
    pub version: String,
}

fn default_version() -> String {
    "1.0.0".to_string()
}

/// Contract specification with schema and invariants.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContractSpec {
    /// Expected schema fields
    #[serde(default)]
    pub fields: Vec<ContractField>,
    /// Invariants that must hold
    #[serde(default)]
    pub invariants: Vec<ContractInvariant>,
    /// Mock data specification for testing
    pub mock_data: Option<MockDataSpec>,
    /// Maximum allowed message size in bytes
    pub max_message_size: Option<usize>,
    /// Required headers
    #[serde(default)]
    pub required_headers: Vec<String>,
}

/// A field definition in the contract schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContractField {
    /// Field name (JSONPath for nested fields)
    pub name: String,
    /// Expected type
    pub field_type: FieldType,
    /// Whether this field is required
    #[serde(default = "default_true")]
    pub required: bool,
    /// Optional description
    #[serde(default)]
    pub description: String,
    /// Optional regex pattern the value must match
    pub pattern: Option<String>,
    /// Minimum value (for numeric types)
    pub min: Option<f64>,
    /// Maximum value (for numeric types)
    pub max: Option<f64>,
}

fn default_true() -> bool {
    true
}

/// Supported field types in a contract.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum FieldType {
    String,
    Integer,
    Float,
    Boolean,
    Array,
    Object,
    Null,
    Any,
}

impl std::fmt::Display for FieldType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::String => write!(f, "string"),
            Self::Integer => write!(f, "integer"),
            Self::Float => write!(f, "float"),
            Self::Boolean => write!(f, "boolean"),
            Self::Array => write!(f, "array"),
            Self::Object => write!(f, "object"),
            Self::Null => write!(f, "null"),
            Self::Any => write!(f, "any"),
        }
    }
}

/// Invariants that must hold across a stream of messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContractInvariant {
    /// Messages must be ordered by a field
    Ordering(OrderingInvariant),
    /// A combination of fields must be unique
    Uniqueness(UniquenessInvariant),
    /// Messages per second must not exceed a threshold
    RateLimit { max_per_second: f64 },
    /// All messages must match a JSON Schema
    JsonSchema { schema: String },
}

/// Ordering invariant definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderingInvariant {
    /// Field to check ordering on
    pub field: String,
    /// Ordering direction
    #[serde(default)]
    pub direction: OrderingDirection,
    /// Scope: per-key or global
    #[serde(default)]
    pub scope: OrderingScope,
}

/// Direction for ordering invariants.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OrderingDirection {
    #[default]
    Ascending,
    Descending,
}

/// Scope for ordering invariants.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderingScope {
    #[default]
    Global,
    PerKey,
}

/// Uniqueness invariant definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UniquenessInvariant {
    /// Fields that form the unique key
    pub fields: Vec<String>,
}

/// Mock data generation specification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MockDataSpec {
    /// Number of messages to generate
    #[serde(default = "default_count")]
    pub count: u64,
    /// Message template with variable substitution
    pub template: Option<String>,
    /// Key template
    pub key_template: Option<String>,
    /// Target partition (None = round-robin)
    pub partition: Option<i32>,
}

fn default_count() -> u64 {
    100
}

impl StreamContract {
    /// Create a new contract with basic settings.
    pub fn new(name: impl Into<String>, topic: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            topic: topic.into(),
            spec: ContractSpec {
                fields: Vec::new(),
                invariants: Vec::new(),
                mock_data: None,
                max_message_size: None,
                required_headers: Vec::new(),
            },
            description: String::new(),
            version: "1.0.0".to_string(),
        }
    }

    /// Add a required field to the contract.
    pub fn with_field(mut self, name: impl Into<String>, field_type: FieldType) -> Self {
        self.spec.fields.push(ContractField {
            name: name.into(),
            field_type,
            required: true,
            description: String::new(),
            pattern: None,
            min: None,
            max: None,
        });
        self
    }

    /// Add an invariant to the contract.
    pub fn with_invariant(mut self, invariant: ContractInvariant) -> Self {
        self.spec.invariants.push(invariant);
        self
    }

    /// Set mock data configuration.
    pub fn with_mock_data(mut self, count: u64, template: impl Into<String>) -> Self {
        self.spec.mock_data = Some(MockDataSpec {
            count,
            template: Some(template.into()),
            key_template: None,
            partition: None,
        });
        self
    }

    /// Load a contract from a YAML string.
    pub fn from_yaml(yaml: &str) -> std::result::Result<Self, String> {
        serde_yaml::from_str(yaml).map_err(|e| format!("Failed to parse contract YAML: {}", e))
    }

    /// Serialize the contract to YAML.
    pub fn to_yaml(&self) -> std::result::Result<String, String> {
        serde_yaml::to_string(self).map_err(|e| format!("Failed to serialize contract: {}", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_contract_builder() {
        let contract = StreamContract::new("test-contract", "events")
            .with_field("user_id", FieldType::String)
            .with_field("count", FieldType::Integer)
            .with_mock_data(50, r#"{"user_id":"{{uuid}}","count":{{random:1:100}}}"#);

        assert_eq!(contract.name, "test-contract");
        assert_eq!(contract.topic, "events");
        assert_eq!(contract.spec.fields.len(), 2);
        assert!(contract.spec.mock_data.is_some());
        assert_eq!(contract.spec.mock_data.unwrap().count, 50);
    }

    #[test]
    fn test_field_type_display() {
        assert_eq!(FieldType::String.to_string(), "string");
        assert_eq!(FieldType::Integer.to_string(), "integer");
        assert_eq!(FieldType::Any.to_string(), "any");
    }

    #[test]
    fn test_contract_yaml_roundtrip() {
        let contract = StreamContract::new("test", "topic").with_field("id", FieldType::String);
        let yaml = contract.to_yaml().unwrap();
        let parsed = StreamContract::from_yaml(&yaml).unwrap();
        assert_eq!(parsed.name, "test");
        assert_eq!(parsed.spec.fields.len(), 1);
    }
}
