//! Feature definitions and values
//!
//! Features are individual measurable properties used for ML models.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

/// Feature definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Feature {
    /// Feature name
    pub name: String,
    /// Feature data type
    pub dtype: FeatureType,
    /// Description
    pub description: Option<String>,
    /// Tags
    pub tags: HashMap<String, String>,
    /// Transformation (SQL or Python expression)
    pub transformation: Option<String>,
}

impl Feature {
    /// Create a new feature
    pub fn new(name: impl Into<String>, dtype: FeatureType) -> Self {
        Self {
            name: name.into(),
            dtype,
            description: None,
            tags: HashMap::new(),
            transformation: None,
        }
    }

    /// Set description
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Add a tag
    pub fn with_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags.insert(key.into(), value.into());
        self
    }

    /// Set transformation
    pub fn with_transformation(mut self, transformation: impl Into<String>) -> Self {
        self.transformation = Some(transformation.into());
        self
    }
}

/// Feature data type
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum FeatureType {
    /// Boolean
    Bool,
    /// 32-bit integer
    Int32,
    /// 64-bit integer
    Int64,
    /// 32-bit float
    Float32,
    /// 64-bit float
    #[default]
    Float64,
    /// String
    String,
    /// Bytes
    Bytes,
    /// Array of integers
    Int32Array,
    /// Array of floats
    Float32Array,
    /// Array of doubles
    Float64Array,
    /// JSON object
    Json,
    /// Timestamp (milliseconds since epoch)
    Timestamp,
    /// Embedding vector
    Embedding,
}

impl fmt::Display for FeatureType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FeatureType::Bool => write!(f, "bool"),
            FeatureType::Int32 => write!(f, "int32"),
            FeatureType::Int64 => write!(f, "int64"),
            FeatureType::Float32 => write!(f, "float32"),
            FeatureType::Float64 => write!(f, "float64"),
            FeatureType::String => write!(f, "string"),
            FeatureType::Bytes => write!(f, "bytes"),
            FeatureType::Int32Array => write!(f, "int32[]"),
            FeatureType::Float32Array => write!(f, "float32[]"),
            FeatureType::Float64Array => write!(f, "float64[]"),
            FeatureType::Json => write!(f, "json"),
            FeatureType::Timestamp => write!(f, "timestamp"),
            FeatureType::Embedding => write!(f, "embedding"),
        }
    }
}

/// Feature definition with schema information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureDefinition {
    /// Feature name
    pub name: String,
    /// Feature type
    pub dtype: FeatureType,
    /// Is nullable
    pub nullable: bool,
    /// Default value
    pub default_value: Option<FeatureValue>,
    /// Validation rules
    pub validation: Option<FeatureValidation>,
}

impl FeatureDefinition {
    /// Create a new feature definition
    pub fn new(name: impl Into<String>, dtype: FeatureType) -> Self {
        Self {
            name: name.into(),
            dtype,
            nullable: true,
            default_value: None,
            validation: None,
        }
    }

    /// Set nullable
    pub fn required(mut self) -> Self {
        self.nullable = false;
        self
    }

    /// Set default value
    pub fn with_default(mut self, value: FeatureValue) -> Self {
        self.default_value = Some(value);
        self
    }

    /// Set validation rules
    pub fn with_validation(mut self, validation: FeatureValidation) -> Self {
        self.validation = Some(validation);
        self
    }
}

/// Feature validation rules
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureValidation {
    /// Minimum value (for numeric types)
    pub min_value: Option<f64>,
    /// Maximum value (for numeric types)
    pub max_value: Option<f64>,
    /// Allowed values (enum-like)
    pub allowed_values: Option<Vec<String>>,
    /// Regex pattern (for strings)
    pub pattern: Option<String>,
    /// Maximum length (for strings/arrays)
    pub max_length: Option<usize>,
}

impl FeatureValidation {
    /// Create new validation rules
    pub fn new() -> Self {
        Self {
            min_value: None,
            max_value: None,
            allowed_values: None,
            pattern: None,
            max_length: None,
        }
    }

    /// Set value range
    pub fn with_range(mut self, min: f64, max: f64) -> Self {
        self.min_value = Some(min);
        self.max_value = Some(max);
        self
    }

    /// Set allowed values
    pub fn with_allowed_values(mut self, values: Vec<String>) -> Self {
        self.allowed_values = Some(values);
        self
    }

    /// Validate a feature value
    pub fn validate(&self, value: &FeatureValue) -> bool {
        match value {
            FeatureValue::Float32(f) => {
                let v = *f as f64;
                if let Some(min) = self.min_value {
                    if v < min {
                        return false;
                    }
                }
                if let Some(max) = self.max_value {
                    if v > max {
                        return false;
                    }
                }
                true
            }
            FeatureValue::Float64(v) => {
                if let Some(min) = self.min_value {
                    if *v < min {
                        return false;
                    }
                }
                if let Some(max) = self.max_value {
                    if *v > max {
                        return false;
                    }
                }
                true
            }
            FeatureValue::String(s) => {
                if let Some(allowed) = &self.allowed_values {
                    if !allowed.contains(s) {
                        return false;
                    }
                }
                if let Some(max_len) = self.max_length {
                    if s.len() > max_len {
                        return false;
                    }
                }
                true
            }
            _ => true,
        }
    }
}

impl Default for FeatureValidation {
    fn default() -> Self {
        Self::new()
    }
}

/// Feature schema (collection of feature definitions)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureSchema {
    /// Features in the schema
    pub features: Vec<FeatureDefinition>,
}

impl FeatureSchema {
    /// Create a new feature schema
    pub fn new() -> Self {
        Self {
            features: Vec::new(),
        }
    }

    /// Add a feature
    pub fn with_feature(mut self, feature: FeatureDefinition) -> Self {
        self.features.push(feature);
        self
    }

    /// Get feature by name
    pub fn get_feature(&self, name: &str) -> Option<&FeatureDefinition> {
        self.features.iter().find(|f| f.name == name)
    }

    /// Get feature names
    pub fn feature_names(&self) -> Vec<&str> {
        self.features.iter().map(|f| f.name.as_str()).collect()
    }
}

impl Default for FeatureSchema {
    fn default() -> Self {
        Self::new()
    }
}

/// Feature value
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub enum FeatureValue {
    /// Null value
    #[default]
    Null,
    /// Boolean
    Bool(bool),
    /// 32-bit integer
    Int32(i32),
    /// 64-bit integer
    Int64(i64),
    /// 32-bit float
    Float32(f32),
    /// 64-bit float
    Float64(f64),
    /// String
    String(String),
    /// Bytes
    Bytes(Vec<u8>),
    /// Array of integers
    Int32Array(Vec<i32>),
    /// Array of floats
    Float32Array(Vec<f32>),
    /// Array of doubles
    Float64Array(Vec<f64>),
    /// JSON value
    Json(serde_json::Value),
    /// Timestamp (milliseconds since epoch)
    Timestamp(i64),
    /// Embedding vector
    Embedding(Vec<f32>),
}

impl FeatureValue {
    /// Check if value is null
    pub fn is_null(&self) -> bool {
        matches!(self, FeatureValue::Null)
    }

    /// Get as f64
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            FeatureValue::Float32(f) => Some(*f as f64),
            FeatureValue::Float64(f) => Some(*f),
            FeatureValue::Int32(i) => Some(*i as f64),
            FeatureValue::Int64(i) => Some(*i as f64),
            _ => None,
        }
    }

    /// Get as string
    pub fn as_str(&self) -> Option<&str> {
        match self {
            FeatureValue::String(s) => Some(s),
            _ => None,
        }
    }

    /// Check if value matches the expected type
    pub fn matches_type(&self, dtype: FeatureType) -> bool {
        matches!(
            (self, dtype),
            (FeatureValue::Null, _)
                | (FeatureValue::Bool(_), FeatureType::Bool)
                | (FeatureValue::Int32(_), FeatureType::Int32)
                | (FeatureValue::Int64(_), FeatureType::Int64)
                | (FeatureValue::Float32(_), FeatureType::Float32)
                | (FeatureValue::Float64(_), FeatureType::Float64)
                | (FeatureValue::String(_), FeatureType::String)
                | (FeatureValue::Bytes(_), FeatureType::Bytes)
                | (FeatureValue::Int32Array(_), FeatureType::Int32Array)
                | (FeatureValue::Float32Array(_), FeatureType::Float32Array)
                | (FeatureValue::Float64Array(_), FeatureType::Float64Array)
                | (FeatureValue::Json(_), FeatureType::Json)
                | (FeatureValue::Timestamp(_), FeatureType::Timestamp)
                | (FeatureValue::Embedding(_), FeatureType::Embedding)
        )
    }
}

impl fmt::Display for FeatureValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FeatureValue::Null => write!(f, "null"),
            FeatureValue::Bool(b) => write!(f, "{}", b),
            FeatureValue::Int32(i) => write!(f, "{}", i),
            FeatureValue::Int64(i) => write!(f, "{}", i),
            FeatureValue::Float32(v) => write!(f, "{}", v),
            FeatureValue::Float64(v) => write!(f, "{}", v),
            FeatureValue::String(s) => write!(f, "{}", s),
            FeatureValue::Bytes(b) => write!(f, "[{} bytes]", b.len()),
            FeatureValue::Int32Array(arr) => write!(f, "[{} int32s]", arr.len()),
            FeatureValue::Float32Array(arr) => write!(f, "[{} float32s]", arr.len()),
            FeatureValue::Float64Array(arr) => write!(f, "[{} float64s]", arr.len()),
            FeatureValue::Json(_) => write!(f, "[json]"),
            FeatureValue::Timestamp(ts) => write!(f, "{}", ts),
            FeatureValue::Embedding(e) => write!(f, "[{}-dim embedding]", e.len()),
        }
    }
}

/// Feature vector (a row of feature values)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureVector {
    /// Feature names
    pub names: Vec<String>,
    /// Feature values
    pub values: Vec<FeatureValue>,
    /// Event timestamp
    pub event_timestamp: i64,
    /// Created timestamp
    pub created_timestamp: i64,
}

impl FeatureVector {
    /// Create a new feature vector
    pub fn new(names: Vec<String>, values: Vec<FeatureValue>, event_timestamp: i64) -> Self {
        Self {
            names,
            values,
            event_timestamp,
            created_timestamp: chrono::Utc::now().timestamp_millis(),
        }
    }

    /// Get a value by name
    pub fn get(&self, name: &str) -> Option<&FeatureValue> {
        self.names
            .iter()
            .position(|n| n == name)
            .map(|i| &self.values[i])
    }

    /// Get value as f64
    pub fn get_f64(&self, name: &str) -> Option<f64> {
        self.get(name).and_then(|v| v.as_f64())
    }

    /// Get value as string
    pub fn get_str(&self, name: &str) -> Option<&str> {
        self.get(name).and_then(|v| v.as_str())
    }

    /// Convert to HashMap
    pub fn to_map(&self) -> HashMap<String, FeatureValue> {
        self.names
            .iter()
            .zip(self.values.iter())
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Number of features
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_feature_creation() {
        let feature = Feature::new("user_age", FeatureType::Int32)
            .with_description("User's age in years")
            .with_tag("category", "demographics");

        assert_eq!(feature.name, "user_age");
        assert_eq!(feature.dtype, FeatureType::Int32);
        assert_eq!(feature.description, Some("User's age in years".to_string()));
    }

    #[test]
    fn test_feature_value_type_match() {
        assert!(FeatureValue::Float64(1.0).matches_type(FeatureType::Float64));
        assert!(FeatureValue::String("test".into()).matches_type(FeatureType::String));
        assert!(FeatureValue::Null.matches_type(FeatureType::Float64));
        assert!(!FeatureValue::Float64(1.0).matches_type(FeatureType::String));
    }

    #[test]
    fn test_feature_validation() {
        let validation = FeatureValidation::new().with_range(0.0, 100.0);

        assert!(validation.validate(&FeatureValue::Float64(50.0)));
        assert!(!validation.validate(&FeatureValue::Float64(150.0)));
        assert!(!validation.validate(&FeatureValue::Float64(-10.0)));
    }

    #[test]
    fn test_feature_vector() {
        let vec = FeatureVector::new(
            vec!["age".into(), "score".into()],
            vec![FeatureValue::Int32(25), FeatureValue::Float64(0.95)],
            chrono::Utc::now().timestamp_millis(),
        );

        assert_eq!(vec.len(), 2);
        assert_eq!(vec.get_f64("score"), Some(0.95));
    }
}
