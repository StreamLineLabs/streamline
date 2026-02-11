//! Schema Auto-Inference
//!
//! This module provides automatic schema inference from JSON messages.
//! It can detect JSON structure and generate Avro, JSON Schema, or Protobuf schemas.
//!
//! ## Features
//!
//! - Automatic type detection from JSON values
//! - Array element type unification
//! - Nullable field detection
//! - Schema drift detection
//! - Migration suggestions
//!
//! ## Example
//!
//! ```rust,ignore
//! use streamline::schema::inference::SchemaInferrer;
//!
//! let inferrer = SchemaInferrer::new();
//!
//! // Infer from a single message
//! let schema = inferrer.infer_from_json(r#"{"name": "Alice", "age": 30}"#)?;
//!
//! // Infer from multiple samples for better accuracy
//! let schema = inferrer.infer_from_samples(&[
//!     r#"{"name": "Alice", "age": 30}"#,
//!     r#"{"name": "Bob", "age": null}"#,
//! ])?;
//! ```

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

/// Schema inference configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceConfig {
    /// Maximum samples to analyze for inference
    #[serde(default = "default_max_samples")]
    pub max_samples: usize,

    /// Minimum samples before inferring nullability
    #[serde(default = "default_min_samples_for_nullability")]
    pub min_samples_for_nullability: usize,

    /// Treat missing fields as nullable
    #[serde(default = "default_missing_as_nullable")]
    pub missing_as_nullable: bool,

    /// Infer enums from string fields with limited values
    #[serde(default = "default_infer_enums")]
    pub infer_enums: bool,

    /// Maximum unique values to consider as enum
    #[serde(default = "default_max_enum_values")]
    pub max_enum_values: usize,

    /// Generate documentation from field names
    #[serde(default = "default_generate_docs")]
    pub generate_docs: bool,
}

fn default_max_samples() -> usize {
    1000
}

fn default_min_samples_for_nullability() -> usize {
    10
}

fn default_missing_as_nullable() -> bool {
    true
}

fn default_infer_enums() -> bool {
    true
}

fn default_max_enum_values() -> usize {
    20
}

fn default_generate_docs() -> bool {
    false
}

impl Default for InferenceConfig {
    fn default() -> Self {
        Self {
            max_samples: default_max_samples(),
            min_samples_for_nullability: default_min_samples_for_nullability(),
            missing_as_nullable: default_missing_as_nullable(),
            infer_enums: default_infer_enums(),
            max_enum_values: default_max_enum_values(),
            generate_docs: default_generate_docs(),
        }
    }
}

/// Inferred field type
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InferredType {
    /// Null value
    Null,
    /// Boolean value
    Boolean,
    /// Integer (fits in i64)
    Integer,
    /// Floating point number
    Float,
    /// String value
    String,
    /// Enum with specific values
    Enum { values: Vec<String> },
    /// Array of elements
    Array { items: Box<InferredType> },
    /// Object/Record with fields
    Object {
        fields: BTreeMap<String, InferredField>,
    },
    /// Union of multiple types (for nullable or variant fields)
    Union { types: Vec<InferredType> },
    /// Unknown (no samples)
    Unknown,
}

impl InferredType {
    /// Check if this type is nullable
    pub fn is_nullable(&self) -> bool {
        match self {
            InferredType::Null => true,
            InferredType::Union { types } => types.contains(&InferredType::Null),
            _ => false,
        }
    }

    /// Make this type nullable
    pub fn make_nullable(self) -> InferredType {
        if self.is_nullable() {
            return self;
        }

        match self {
            InferredType::Null => InferredType::Null,
            InferredType::Union { mut types } => {
                if !types.contains(&InferredType::Null) {
                    types.insert(0, InferredType::Null);
                }
                InferredType::Union { types }
            }
            other => InferredType::Union {
                types: vec![InferredType::Null, other],
            },
        }
    }

    /// Convert to Avro schema JSON
    pub fn to_avro_schema(&self, name: &str) -> Value {
        match self {
            InferredType::Null => Value::String("null".to_string()),
            InferredType::Boolean => Value::String("boolean".to_string()),
            InferredType::Integer => Value::String("long".to_string()),
            InferredType::Float => Value::String("double".to_string()),
            InferredType::String => Value::String("string".to_string()),
            InferredType::Enum { values } => {
                serde_json::json!({
                    "type": "enum",
                    "name": format!("{}Enum", to_pascal_case(name)),
                    "symbols": values
                })
            }
            InferredType::Array { items } => {
                serde_json::json!({
                    "type": "array",
                    "items": items.to_avro_schema("item")
                })
            }
            InferredType::Object { fields } => {
                let avro_fields: Vec<Value> = fields
                    .iter()
                    .map(|(field_name, field)| {
                        let mut field_schema = serde_json::json!({
                            "name": field_name,
                            "type": field.field_type.to_avro_schema(field_name)
                        });
                        if let Some(doc) = &field.doc {
                            field_schema["doc"] = Value::String(doc.clone());
                        }
                        if let Some(default) = &field.default {
                            field_schema["default"] = default.clone();
                        }
                        field_schema
                    })
                    .collect();

                serde_json::json!({
                    "type": "record",
                    "name": to_pascal_case(name),
                    "fields": avro_fields
                })
            }
            InferredType::Union { types } => {
                let avro_types: Vec<Value> = types.iter().map(|t| t.to_avro_schema(name)).collect();
                Value::Array(avro_types)
            }
            InferredType::Unknown => Value::String("null".to_string()),
        }
    }

    /// Convert to JSON Schema
    pub fn to_json_schema(&self) -> Value {
        match self {
            InferredType::Null => serde_json::json!({"type": "null"}),
            InferredType::Boolean => serde_json::json!({"type": "boolean"}),
            InferredType::Integer => serde_json::json!({"type": "integer"}),
            InferredType::Float => serde_json::json!({"type": "number"}),
            InferredType::String => serde_json::json!({"type": "string"}),
            InferredType::Enum { values } => {
                serde_json::json!({
                    "type": "string",
                    "enum": values
                })
            }
            InferredType::Array { items } => {
                serde_json::json!({
                    "type": "array",
                    "items": items.to_json_schema()
                })
            }
            InferredType::Object { fields } => {
                let properties: serde_json::Map<String, Value> = fields
                    .iter()
                    .map(|(name, field)| {
                        let mut schema = field.field_type.to_json_schema();
                        if let Some(doc) = &field.doc {
                            if let Value::Object(ref mut obj) = schema {
                                obj.insert("description".to_string(), Value::String(doc.clone()));
                            }
                        }
                        (name.clone(), schema)
                    })
                    .collect();

                let required: Vec<String> = fields
                    .iter()
                    .filter(|(_, field)| !field.field_type.is_nullable())
                    .map(|(name, _)| name.clone())
                    .collect();

                serde_json::json!({
                    "type": "object",
                    "properties": properties,
                    "required": required
                })
            }
            InferredType::Union { types } => {
                // JSON Schema uses anyOf for unions
                let any_of: Vec<Value> = types.iter().map(|t| t.to_json_schema()).collect();
                serde_json::json!({"anyOf": any_of})
            }
            InferredType::Unknown => serde_json::json!({}),
        }
    }
}

/// Inferred field information
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct InferredField {
    /// Field type
    pub field_type: InferredType,
    /// Documentation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
    /// Default value
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<Value>,
    /// Sample count
    pub sample_count: usize,
    /// Null count
    pub null_count: usize,
}

/// Schema inferrer for automatic schema detection
pub struct SchemaInferrer {
    config: InferenceConfig,
}

impl SchemaInferrer {
    /// Create a new schema inferrer with default configuration
    pub fn new() -> Self {
        Self {
            config: InferenceConfig::default(),
        }
    }

    /// Create a new schema inferrer with custom configuration
    pub fn with_config(config: InferenceConfig) -> Self {
        Self { config }
    }

    /// Infer schema from a single JSON string
    pub fn infer_from_json(&self, json: &str) -> Result<InferredType, InferenceError> {
        let value: Value =
            serde_json::from_str(json).map_err(|e| InferenceError::ParseError(e.to_string()))?;
        Ok(self.infer_type(&value))
    }

    /// Infer schema from a JSON value
    pub fn infer_from_value(&self, value: &Value) -> InferredType {
        self.infer_type(value)
    }

    /// Infer schema from multiple JSON samples (more accurate)
    pub fn infer_from_samples(&self, samples: &[&str]) -> Result<InferredType, InferenceError> {
        if samples.is_empty() {
            return Ok(InferredType::Unknown);
        }

        let values: Vec<Value> = samples
            .iter()
            .take(self.config.max_samples)
            .map(|s| serde_json::from_str(s))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| InferenceError::ParseError(e.to_string()))?;

        self.infer_from_values(&values)
    }

    /// Infer schema from multiple JSON values
    pub fn infer_from_values(&self, values: &[Value]) -> Result<InferredType, InferenceError> {
        if values.is_empty() {
            return Ok(InferredType::Unknown);
        }

        let mut merged = self.infer_type(&values[0]);
        for value in values.iter().skip(1) {
            let inferred = self.infer_type(value);
            merged = self.merge_types(merged, inferred);
        }

        Ok(merged)
    }

    /// Infer type from a single JSON value
    fn infer_type(&self, value: &Value) -> InferredType {
        match value {
            Value::Null => InferredType::Null,
            Value::Bool(_) => InferredType::Boolean,
            Value::Number(n) => {
                if n.is_i64() || n.is_u64() {
                    InferredType::Integer
                } else {
                    InferredType::Float
                }
            }
            Value::String(_) => InferredType::String,
            Value::Array(arr) => {
                if arr.is_empty() {
                    InferredType::Array {
                        items: Box::new(InferredType::Unknown),
                    }
                } else {
                    let mut item_type = self.infer_type(&arr[0]);
                    for item in arr.iter().skip(1) {
                        item_type = self.merge_types(item_type, self.infer_type(item));
                    }
                    InferredType::Array {
                        items: Box::new(item_type),
                    }
                }
            }
            Value::Object(obj) => {
                let fields: BTreeMap<String, InferredField> = obj
                    .iter()
                    .map(|(key, val)| {
                        let field_type = self.infer_type(val);
                        let is_null = matches!(field_type, InferredType::Null);
                        (
                            key.clone(),
                            InferredField {
                                field_type,
                                doc: if self.config.generate_docs {
                                    Some(generate_doc_from_name(key))
                                } else {
                                    None
                                },
                                default: None,
                                sample_count: 1,
                                null_count: if is_null { 1 } else { 0 },
                            },
                        )
                    })
                    .collect();

                InferredType::Object { fields }
            }
        }
    }

    /// Merge two inferred types into a unified type
    fn merge_types(&self, t1: InferredType, t2: InferredType) -> InferredType {
        if t1 == t2 {
            return t1;
        }

        match (&t1, &t2) {
            // Null + anything = nullable anything
            (InferredType::Null, other) | (other, InferredType::Null) => {
                other.clone().make_nullable()
            }

            // Integer + Float = Float
            (InferredType::Integer, InferredType::Float)
            | (InferredType::Float, InferredType::Integer) => InferredType::Float,

            // Merge arrays
            (InferredType::Array { items: items1 }, InferredType::Array { items: items2 }) => {
                InferredType::Array {
                    items: Box::new(
                        self.merge_types(items1.as_ref().clone(), items2.as_ref().clone()),
                    ),
                }
            }

            // Merge objects
            (
                InferredType::Object { fields: fields1 },
                InferredType::Object { fields: fields2 },
            ) => {
                let mut merged_fields = fields1.clone();

                for (key, field2) in fields2 {
                    if let Some(field1) = merged_fields.get_mut(key) {
                        // Field exists in both - merge types
                        field1.field_type =
                            self.merge_types(field1.field_type.clone(), field2.field_type.clone());
                        field1.sample_count += field2.sample_count;
                        field1.null_count += field2.null_count;
                    } else {
                        // Field only in second - add as nullable if configured
                        let mut new_field = field2.clone();
                        if self.config.missing_as_nullable {
                            new_field.field_type = new_field.field_type.make_nullable();
                        }
                        merged_fields.insert(key.clone(), new_field);
                    }
                }

                // Mark fields only in first as nullable
                if self.config.missing_as_nullable {
                    for (key, field) in merged_fields.iter_mut() {
                        if !fields2.contains_key(key) && !field.field_type.is_nullable() {
                            field.field_type = field.field_type.clone().make_nullable();
                        }
                    }
                }

                InferredType::Object {
                    fields: merged_fields,
                }
            }

            // Merge unions
            (InferredType::Union { types: types1 }, InferredType::Union { types: types2 }) => {
                let mut merged: Vec<InferredType> = types1.clone();
                for t in types2 {
                    if !merged.contains(t) {
                        merged.push(t.clone());
                    }
                }
                InferredType::Union { types: merged }
            }

            // Union + type = add to union
            (InferredType::Union { types }, other) | (other, InferredType::Union { types }) => {
                let mut merged = types.clone();
                if !merged.contains(other) {
                    merged.push(other.clone());
                }
                InferredType::Union { types: merged }
            }

            // Incompatible types -> union
            _ => InferredType::Union {
                types: vec![t1, t2],
            },
        }
    }

    /// Detect schema drift between two schemas
    pub fn detect_drift(&self, old: &InferredType, new: &InferredType) -> Vec<SchemaDrift> {
        let mut drifts = Vec::new();
        self.detect_drift_recursive(old, new, "", &mut drifts);
        drifts
    }

    fn detect_drift_recursive(
        &self,
        old: &InferredType,
        new: &InferredType,
        path: &str,
        drifts: &mut Vec<SchemaDrift>,
    ) {
        match (old, new) {
            (
                InferredType::Object { fields: old_fields },
                InferredType::Object { fields: new_fields },
            ) => {
                // Check for removed fields
                for key in old_fields.keys() {
                    if !new_fields.contains_key(key) {
                        let field_path = if path.is_empty() {
                            key.clone()
                        } else {
                            format!("{}.{}", path, key)
                        };
                        drifts.push(SchemaDrift::FieldRemoved {
                            path: field_path,
                            field_type: old_fields[key].field_type.clone(),
                        });
                    }
                }

                // Check for added fields
                for key in new_fields.keys() {
                    if !old_fields.contains_key(key) {
                        let field_path = if path.is_empty() {
                            key.clone()
                        } else {
                            format!("{}.{}", path, key)
                        };
                        drifts.push(SchemaDrift::FieldAdded {
                            path: field_path,
                            field_type: new_fields[key].field_type.clone(),
                        });
                    }
                }

                // Check for type changes
                for (key, old_field) in old_fields {
                    if let Some(new_field) = new_fields.get(key) {
                        let field_path = if path.is_empty() {
                            key.clone()
                        } else {
                            format!("{}.{}", path, key)
                        };

                        if old_field.field_type != new_field.field_type {
                            // Check if it's just a nullability change
                            let old_nullable = old_field.field_type.is_nullable();
                            let new_nullable = new_field.field_type.is_nullable();

                            if old_nullable != new_nullable {
                                drifts.push(SchemaDrift::NullabilityChanged {
                                    path: field_path.clone(),
                                    was_nullable: old_nullable,
                                    is_nullable: new_nullable,
                                });
                            } else {
                                drifts.push(SchemaDrift::TypeChanged {
                                    path: field_path.clone(),
                                    old_type: old_field.field_type.clone(),
                                    new_type: new_field.field_type.clone(),
                                });
                            }
                        }

                        // Recurse for nested objects
                        self.detect_drift_recursive(
                            &old_field.field_type,
                            &new_field.field_type,
                            &field_path,
                            drifts,
                        );
                    }
                }
            }
            (
                InferredType::Array { items: old_items },
                InferredType::Array { items: new_items },
            ) => {
                let array_path = if path.is_empty() {
                    "[]".to_string()
                } else {
                    format!("{}[]", path)
                };
                self.detect_drift_recursive(old_items, new_items, &array_path, drifts);
            }
            _ => {
                // Type change at this level
                if old != new && !path.is_empty() {
                    drifts.push(SchemaDrift::TypeChanged {
                        path: path.to_string(),
                        old_type: old.clone(),
                        new_type: new.clone(),
                    });
                }
            }
        }
    }

    /// Generate migration suggestions for schema drift
    pub fn suggest_migrations(&self, drifts: &[SchemaDrift]) -> Vec<MigrationSuggestion> {
        drifts
            .iter()
            .map(|drift| match drift {
                SchemaDrift::FieldAdded { path, field_type } => MigrationSuggestion {
                    drift: drift.clone(),
                    action: MigrationAction::AddFieldWithDefault,
                    description: format!(
                        "Add field '{}' of type {:?} with a default value",
                        path, field_type
                    ),
                    breaking: false,
                },
                SchemaDrift::FieldRemoved { path, .. } => MigrationSuggestion {
                    drift: drift.clone(),
                    action: MigrationAction::MakeOptional,
                    description: format!(
                        "Make field '{}' optional before removing, or use schema compatibility BACKWARD",
                        path
                    ),
                    breaking: true,
                },
                SchemaDrift::TypeChanged { path, old_type, new_type } => MigrationSuggestion {
                    drift: drift.clone(),
                    action: MigrationAction::TypeCoercion,
                    description: format!(
                        "Type change at '{}' from {:?} to {:?}. Consider using a union type or separate versioned schemas.",
                        path, old_type, new_type
                    ),
                    breaking: true,
                },
                SchemaDrift::NullabilityChanged {
                    path,
                    was_nullable: _,
                    is_nullable,
                } => {
                    if *is_nullable {
                        MigrationSuggestion {
                            drift: drift.clone(),
                            action: MigrationAction::MakeOptional,
                            description: format!(
                                "Field '{}' became nullable. This is backward compatible.",
                                path
                            ),
                            breaking: false,
                        }
                    } else {
                        MigrationSuggestion {
                            drift: drift.clone(),
                            action: MigrationAction::AddFieldWithDefault,
                            description: format!(
                                "Field '{}' became required. Add a default value for backward compatibility.",
                                path
                            ),
                            breaking: true,
                        }
                    }
                }
            })
            .collect()
    }
}

impl Default for SchemaInferrer {
    fn default() -> Self {
        Self::new()
    }
}

/// Schema drift detection result
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SchemaDrift {
    /// A field was added
    FieldAdded {
        path: String,
        field_type: InferredType,
    },
    /// A field was removed
    FieldRemoved {
        path: String,
        field_type: InferredType,
    },
    /// A field's type changed
    TypeChanged {
        path: String,
        old_type: InferredType,
        new_type: InferredType,
    },
    /// A field's nullability changed
    NullabilityChanged {
        path: String,
        was_nullable: bool,
        is_nullable: bool,
    },
}

/// Suggested migration action
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationSuggestion {
    /// The drift this suggestion addresses
    pub drift: SchemaDrift,
    /// Suggested action
    pub action: MigrationAction,
    /// Human-readable description
    pub description: String,
    /// Whether this change is breaking
    pub breaking: bool,
}

/// Migration action types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MigrationAction {
    /// Add a field with a default value
    AddFieldWithDefault,
    /// Make a field optional/nullable
    MakeOptional,
    /// Use type coercion or union
    TypeCoercion,
    /// Create a new schema version
    NewVersion,
}

/// Schema inference error
#[derive(Debug, Clone, thiserror::Error)]
pub enum InferenceError {
    #[error("Failed to parse JSON: {0}")]
    ParseError(String),

    #[error("Empty input")]
    EmptyInput,

    #[error("Type conflict: {0}")]
    TypeConflict(String),
}

/// Convert string to PascalCase for Avro record names
fn to_pascal_case(s: &str) -> String {
    s.split(|c: char| !c.is_alphanumeric())
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                None => String::new(),
                Some(first) => first.to_uppercase().chain(chars).collect(),
            }
        })
        .collect()
}

/// Generate documentation from field name
fn generate_doc_from_name(name: &str) -> String {
    // Convert snake_case or camelCase to sentence
    let words: Vec<String> = name
        .split(['_', '-'])
        .flat_map(|part| {
            // Split camelCase
            let mut words = Vec::new();
            let mut current = String::new();
            for c in part.chars() {
                if c.is_uppercase() && !current.is_empty() {
                    words.push(current);
                    current = c.to_lowercase().to_string();
                } else {
                    current.push(c.to_ascii_lowercase());
                }
            }
            if !current.is_empty() {
                words.push(current);
            }
            words
        })
        .collect();

    if words.is_empty() {
        return name.to_string();
    }

    let mut sentence = words.join(" ");
    if let Some(first) = sentence.get_mut(0..1) {
        first.make_ascii_uppercase();
    }
    sentence
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_infer_simple_types() {
        let inferrer = SchemaInferrer::new();

        assert_eq!(
            inferrer.infer_from_json("null").unwrap(),
            InferredType::Null
        );
        assert_eq!(
            inferrer.infer_from_json("true").unwrap(),
            InferredType::Boolean
        );
        assert_eq!(
            inferrer.infer_from_json("42").unwrap(),
            InferredType::Integer
        );
        assert_eq!(
            inferrer.infer_from_json("3.14").unwrap(),
            InferredType::Float
        );
        assert_eq!(
            inferrer.infer_from_json(r#""hello""#).unwrap(),
            InferredType::String
        );
    }

    #[test]
    fn test_infer_object() {
        let inferrer = SchemaInferrer::new();

        let schema = inferrer
            .infer_from_json(r#"{"name": "Alice", "age": 30}"#)
            .unwrap();

        match schema {
            InferredType::Object { ref fields } => {
                assert_eq!(fields.len(), 2);
                assert!(fields.contains_key("name"));
                assert!(fields.contains_key("age"));
                assert_eq!(fields["name"].field_type, InferredType::String);
                assert_eq!(fields["age"].field_type, InferredType::Integer);
            }
            other => assert!(false, "Expected Object type, got {:?}", other),
        }
    }

    #[test]
    fn test_infer_array() {
        let inferrer = SchemaInferrer::new();

        let schema = inferrer.infer_from_json(r#"[1, 2, 3]"#).unwrap();

        match schema {
            InferredType::Array { ref items } => {
                assert_eq!(**items, InferredType::Integer);
            }
            other => assert!(false, "Expected Array type, got {:?}", other),
        }
    }

    #[test]
    fn test_merge_nullable() {
        let inferrer = SchemaInferrer::new();

        let samples = &[r#"{"name": "Alice"}"#, r#"{"name": null}"#];

        let schema = inferrer.infer_from_samples(samples).unwrap();

        match schema {
            InferredType::Object { ref fields } => {
                assert!(fields["name"].field_type.is_nullable());
            }
            other => assert!(false, "Expected Object type, got {:?}", other),
        }
    }

    #[test]
    fn test_merge_missing_field() {
        let inferrer = SchemaInferrer::new();

        let samples = &[r#"{"name": "Alice", "age": 30}"#, r#"{"name": "Bob"}"#];

        let schema = inferrer.infer_from_samples(samples).unwrap();

        match schema {
            InferredType::Object { ref fields } => {
                assert!(fields["age"].field_type.is_nullable());
            }
            other => assert!(false, "Expected Object type, got {:?}", other),
        }
    }

    #[test]
    fn test_to_avro_schema() {
        let inferrer = SchemaInferrer::new();

        let schema = inferrer
            .infer_from_json(r#"{"name": "Alice", "age": 30}"#)
            .unwrap();

        let avro = schema.to_avro_schema("Person");
        let avro_obj = avro.as_object().unwrap();

        assert_eq!(avro_obj["type"], "record");
        assert_eq!(avro_obj["name"], "Person");
        assert!(avro_obj["fields"].is_array());
    }

    #[test]
    fn test_to_json_schema() {
        let inferrer = SchemaInferrer::new();

        let schema = inferrer
            .infer_from_json(r#"{"name": "Alice", "age": 30}"#)
            .unwrap();

        let json_schema = schema.to_json_schema();
        let obj = json_schema.as_object().unwrap();

        assert_eq!(obj["type"], "object");
        assert!(obj["properties"].is_object());
        assert!(obj["required"].is_array());
    }

    #[test]
    fn test_detect_drift() {
        let inferrer = SchemaInferrer::new();

        let old_schema = inferrer
            .infer_from_json(r#"{"name": "Alice", "age": 30}"#)
            .unwrap();

        let new_schema = inferrer
            .infer_from_json(r#"{"name": "Bob", "email": "bob@example.com"}"#)
            .unwrap();

        let drifts = inferrer.detect_drift(&old_schema, &new_schema);

        // Should detect: age removed, email added
        assert!(drifts
            .iter()
            .any(|d| matches!(d, SchemaDrift::FieldRemoved { path, .. } if path == "age")));
        assert!(drifts
            .iter()
            .any(|d| matches!(d, SchemaDrift::FieldAdded { path, .. } if path == "email")));
    }

    #[test]
    fn test_to_pascal_case() {
        assert_eq!(to_pascal_case("my_field"), "MyField");
        assert_eq!(to_pascal_case("user-id"), "UserId");
        assert_eq!(to_pascal_case("simple"), "Simple");
    }
}
