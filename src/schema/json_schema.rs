//! JSON Schema validation and compatibility checking
//!
//! This module provides JSON Schema parsing, validation, and compatibility
//! checking using the `jsonschema` crate.

use super::{CompatibilityLevel, SchemaError};
use jsonschema::Validator;
use serde_json::Value;
use std::collections::HashSet;

/// JSON Schema validator
#[derive(Debug, Default)]
pub struct JsonSchemaValidator;

impl JsonSchemaValidator {
    /// Create a new JSON Schema validator
    pub fn new() -> Self {
        Self
    }

    /// Parse and compile a JSON Schema
    pub fn parse(&self, schema_str: &str) -> Result<Value, SchemaError> {
        serde_json::from_str(schema_str)
            .map_err(|e| SchemaError::InvalidSchema(format!("Invalid JSON: {}", e)))
    }

    /// Validate that a schema string is valid JSON Schema
    pub fn validate(&self, schema_str: &str) -> Result<(), SchemaError> {
        let schema_value = self.parse(schema_str)?;

        // Try to compile the schema
        Validator::new(&schema_value)
            .map_err(|e| SchemaError::InvalidSchema(format!("Invalid JSON Schema: {}", e)))?;

        Ok(())
    }

    /// Validate data against a schema
    pub fn validate_data(&self, schema_str: &str, data: &[u8]) -> Result<(), SchemaError> {
        let schema_value = self.parse(schema_str)?;
        let compiled = Validator::new(&schema_value)
            .map_err(|e| SchemaError::InvalidSchema(format!("Invalid JSON Schema: {}", e)))?;

        let data_value: Value = serde_json::from_slice(data)
            .map_err(|e| SchemaError::InvalidSchema(format!("Invalid JSON data: {}", e)))?;

        let result = compiled.validate(&data_value);
        if let Err(error) = result {
            return Err(SchemaError::InvalidSchema(format!(
                "Data validation failed: {}",
                error
            )));
        }

        Ok(())
    }

    /// Check compatibility between schemas
    pub fn check_compatibility(
        &self,
        new_schema_str: &str,
        existing_schema_str: &str,
        level: CompatibilityLevel,
    ) -> Result<Vec<String>, SchemaError> {
        let new_schema = self.parse(new_schema_str)?;
        let existing_schema = self.parse(existing_schema_str)?;

        match level {
            CompatibilityLevel::None => Ok(vec![]),
            CompatibilityLevel::Backward | CompatibilityLevel::BackwardTransitive => {
                self.check_backward_compatible(&new_schema, &existing_schema)
            }
            CompatibilityLevel::Forward | CompatibilityLevel::ForwardTransitive => {
                self.check_forward_compatible(&new_schema, &existing_schema)
            }
            CompatibilityLevel::Full | CompatibilityLevel::FullTransitive => {
                let mut messages = vec![];
                messages.extend(self.check_backward_compatible(&new_schema, &existing_schema)?);
                messages.extend(self.check_forward_compatible(&new_schema, &existing_schema)?);
                Ok(messages)
            }
        }
    }

    /// Check backward compatibility (new schema can read old data)
    fn check_backward_compatible(
        &self,
        new_schema: &Value,
        existing_schema: &Value,
    ) -> Result<Vec<String>, SchemaError> {
        let mut messages = vec![];

        // Check type compatibility
        let new_type = self.get_schema_type(new_schema);
        let existing_type = self.get_schema_type(existing_schema);

        if !self.types_compatible(&new_type, &existing_type) {
            messages.push(format!(
                "Type changed from {:?} to {:?}",
                existing_type, new_type
            ));
        }

        // For object schemas, check properties
        if new_type.contains("object") && existing_type.contains("object") {
            self.check_object_backward_compatibility(new_schema, existing_schema, &mut messages)?;
        }

        // For array schemas, check items
        if new_type.contains("array") && existing_type.contains("array") {
            self.check_array_backward_compatibility(new_schema, existing_schema, &mut messages)?;
        }

        // Check enum values
        if let (Some(new_enum), Some(existing_enum)) =
            (new_schema.get("enum"), existing_schema.get("enum"))
        {
            self.check_enum_backward_compatibility(new_enum, existing_enum, &mut messages);
        }

        Ok(messages)
    }

    /// Check forward compatibility (old schema can read new data)
    fn check_forward_compatible(
        &self,
        new_schema: &Value,
        existing_schema: &Value,
    ) -> Result<Vec<String>, SchemaError> {
        let mut messages = vec![];

        // Check type compatibility
        let new_type = self.get_schema_type(new_schema);
        let existing_type = self.get_schema_type(existing_schema);

        if !self.types_compatible(&existing_type, &new_type) {
            messages.push(format!(
                "Type changed from {:?} to {:?} (not forward compatible)",
                existing_type, new_type
            ));
        }

        // For object schemas, check for new required properties
        if new_type.contains("object") && existing_type.contains("object") {
            self.check_object_forward_compatibility(new_schema, existing_schema, &mut messages)?;
        }

        Ok(messages)
    }

    fn check_object_backward_compatibility(
        &self,
        new_schema: &Value,
        existing_schema: &Value,
        messages: &mut Vec<String>,
    ) -> Result<(), SchemaError> {
        let new_properties = new_schema
            .get("properties")
            .and_then(|p| p.as_object())
            .cloned()
            .unwrap_or_default();

        let existing_properties = existing_schema
            .get("properties")
            .and_then(|p| p.as_object())
            .cloned()
            .unwrap_or_default();

        let new_required: HashSet<String> = new_schema
            .get("required")
            .and_then(|r| r.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str())
                    .map(String::from)
                    .collect()
            })
            .unwrap_or_default();

        // Check for removed properties that were in existing schema
        for (prop_name, _) in &existing_properties {
            if !new_properties.contains_key(prop_name) {
                messages.push(format!("Property '{}' was removed", prop_name));
            }
        }

        // Check for new required properties (not backward compatible)
        for req in &new_required {
            if !existing_properties.contains_key(req) {
                messages.push(format!(
                    "New required property '{}' added without default",
                    req
                ));
            }
        }

        // Check property type compatibility
        for (prop_name, new_prop_schema) in &new_properties {
            if let Some(existing_prop_schema) = existing_properties.get(prop_name) {
                let new_type = self.get_schema_type(new_prop_schema);
                let existing_type = self.get_schema_type(existing_prop_schema);

                if !self.types_compatible(&new_type, &existing_type) {
                    messages.push(format!(
                        "Property '{}' type changed from {:?} to {:?}",
                        prop_name, existing_type, new_type
                    ));
                }
            }
        }

        Ok(())
    }

    fn check_object_forward_compatibility(
        &self,
        new_schema: &Value,
        existing_schema: &Value,
        messages: &mut Vec<String>,
    ) -> Result<(), SchemaError> {
        let new_properties = new_schema
            .get("properties")
            .and_then(|p| p.as_object())
            .cloned()
            .unwrap_or_default();

        let existing_required: HashSet<String> = existing_schema
            .get("required")
            .and_then(|r| r.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str())
                    .map(String::from)
                    .collect()
            })
            .unwrap_or_default();

        let existing_properties = existing_schema
            .get("properties")
            .and_then(|p| p.as_object())
            .cloned()
            .unwrap_or_default();

        // Check for removed required properties
        for req in &existing_required {
            if !new_properties.contains_key(req) {
                messages.push(format!(
                    "Required property '{}' was removed (not forward compatible)",
                    req
                ));
            }
        }

        // Check for added properties that might break strict validation
        let allows_additional = new_schema
            .get("additionalProperties")
            .map(|v| v.as_bool().unwrap_or(true))
            .unwrap_or(true);

        if !allows_additional {
            for prop_name in new_properties.keys() {
                if !existing_properties.contains_key(prop_name) {
                    messages.push(format!(
                        "New property '{}' added but additionalProperties is false",
                        prop_name
                    ));
                }
            }
        }

        Ok(())
    }

    fn check_array_backward_compatibility(
        &self,
        new_schema: &Value,
        existing_schema: &Value,
        messages: &mut Vec<String>,
    ) -> Result<(), SchemaError> {
        let new_items = new_schema.get("items");
        let existing_items = existing_schema.get("items");

        if let (Some(new_items), Some(existing_items)) = (new_items, existing_items) {
            let new_type = self.get_schema_type(new_items);
            let existing_type = self.get_schema_type(existing_items);

            if !self.types_compatible(&new_type, &existing_type) {
                messages.push(format!(
                    "Array items type changed from {:?} to {:?}",
                    existing_type, new_type
                ));
            }
        }

        // Check minItems/maxItems constraints
        let new_min = new_schema.get("minItems").and_then(|v| v.as_u64());
        let existing_min = existing_schema.get("minItems").and_then(|v| v.as_u64());

        if let (Some(new_min), Some(existing_min)) = (new_min, existing_min) {
            if new_min > existing_min {
                messages.push(format!(
                    "minItems increased from {} to {} (not backward compatible)",
                    existing_min, new_min
                ));
            }
        }

        let new_max = new_schema.get("maxItems").and_then(|v| v.as_u64());
        let existing_max = existing_schema.get("maxItems").and_then(|v| v.as_u64());

        if let (Some(new_max), Some(existing_max)) = (new_max, existing_max) {
            if new_max < existing_max {
                messages.push(format!(
                    "maxItems decreased from {} to {} (not backward compatible)",
                    existing_max, new_max
                ));
            }
        }

        Ok(())
    }

    fn check_enum_backward_compatibility(
        &self,
        new_enum: &Value,
        existing_enum: &Value,
        messages: &mut Vec<String>,
    ) {
        let new_values: HashSet<String> = new_enum
            .as_array()
            .map(|arr| arr.iter().map(|v| v.to_string()).collect())
            .unwrap_or_default();

        let existing_values: HashSet<String> = existing_enum
            .as_array()
            .map(|arr| arr.iter().map(|v| v.to_string()).collect())
            .unwrap_or_default();

        // Check for removed enum values
        for value in &existing_values {
            if !new_values.contains(value) {
                messages.push(format!("Enum value {} was removed", value));
            }
        }
    }

    /// Get the type(s) from a schema
    fn get_schema_type(&self, schema: &Value) -> HashSet<String> {
        let mut types = HashSet::new();

        if let Some(t) = schema.get("type") {
            match t {
                Value::String(s) => {
                    types.insert(s.clone());
                }
                Value::Array(arr) => {
                    for v in arr {
                        if let Value::String(s) = v {
                            types.insert(s.clone());
                        }
                    }
                }
                _ => {}
            }
        }

        // If no type specified, infer from keywords
        if types.is_empty() {
            if schema.get("properties").is_some() {
                types.insert("object".to_string());
            }
            if schema.get("items").is_some() {
                types.insert("array".to_string());
            }
            if schema.get("enum").is_some() {
                // Enum can be any type
            }
        }

        types
    }

    /// Check if types are compatible
    fn types_compatible(
        &self,
        reader_types: &HashSet<String>,
        writer_types: &HashSet<String>,
    ) -> bool {
        // If either is empty, consider compatible
        if reader_types.is_empty() || writer_types.is_empty() {
            return true;
        }

        // Writer types should be subset of reader types for backward compatibility
        for writer_type in writer_types {
            if !reader_types.contains(writer_type) {
                // Check for numeric promotions
                let compatible = matches!(
                    (
                        reader_types.iter().next().map(|s| s.as_str()),
                        writer_type.as_str()
                    ),
                    (Some("number"), "integer") | (Some("string"), "string")
                );
                if !compatible {
                    return false;
                }
            }
        }

        true
    }

    /// Get the canonical form of a schema (normalized)
    pub fn canonical_form(&self, schema_str: &str) -> Result<String, SchemaError> {
        let schema = self.parse(schema_str)?;
        serde_json::to_string(&schema).map_err(|e| SchemaError::SerializationError(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_schema() {
        let validator = JsonSchemaValidator::new();

        let schema = r#"{"type": "string"}"#;
        assert!(validator.validate(schema).is_ok());
    }

    #[test]
    fn test_parse_object_schema() {
        let validator = JsonSchemaValidator::new();

        let schema = r#"{
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "integer"}
            },
            "required": ["name"]
        }"#;

        assert!(validator.validate(schema).is_ok());
    }

    #[test]
    fn test_validate_data() {
        let validator = JsonSchemaValidator::new();

        let schema = r#"{
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "integer"}
            },
            "required": ["name"]
        }"#;

        // Valid data
        let valid_data = br#"{"name": "John", "age": 30}"#;
        assert!(validator.validate_data(schema, valid_data).is_ok());

        // Invalid data (missing required field)
        let invalid_data = br#"{"age": 30}"#;
        assert!(validator.validate_data(schema, invalid_data).is_err());

        // Invalid data (wrong type)
        let invalid_data = br#"{"name": 123}"#;
        assert!(validator.validate_data(schema, invalid_data).is_err());
    }

    #[test]
    fn test_backward_compatible_add_optional_property() {
        let validator = JsonSchemaValidator::new();

        let old_schema = r#"{
            "type": "object",
            "properties": {
                "name": {"type": "string"}
            }
        }"#;

        let new_schema = r#"{
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "email": {"type": "string"}
            }
        }"#;

        let result =
            validator.check_compatibility(new_schema, old_schema, CompatibilityLevel::Backward);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_backward_incompatible_add_required_property() {
        let validator = JsonSchemaValidator::new();

        let old_schema = r#"{
            "type": "object",
            "properties": {
                "name": {"type": "string"}
            }
        }"#;

        let new_schema = r#"{
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "email": {"type": "string"}
            },
            "required": ["name", "email"]
        }"#;

        let result =
            validator.check_compatibility(new_schema, old_schema, CompatibilityLevel::Backward);
        assert!(result.is_ok());
        let messages = result.unwrap();
        assert!(!messages.is_empty());
    }

    #[test]
    fn test_backward_incompatible_remove_property() {
        let validator = JsonSchemaValidator::new();

        let old_schema = r#"{
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "email": {"type": "string"}
            }
        }"#;

        let new_schema = r#"{
            "type": "object",
            "properties": {
                "name": {"type": "string"}
            }
        }"#;

        let result =
            validator.check_compatibility(new_schema, old_schema, CompatibilityLevel::Backward);
        assert!(result.is_ok());
        let messages = result.unwrap();
        assert!(!messages.is_empty());
    }

    #[test]
    fn test_backward_incompatible_change_type() {
        let validator = JsonSchemaValidator::new();

        let old_schema = r#"{
            "type": "object",
            "properties": {
                "age": {"type": "integer"}
            }
        }"#;

        let new_schema = r#"{
            "type": "object",
            "properties": {
                "age": {"type": "string"}
            }
        }"#;

        let result =
            validator.check_compatibility(new_schema, old_schema, CompatibilityLevel::Backward);
        assert!(result.is_ok());
        let messages = result.unwrap();
        assert!(!messages.is_empty());
    }

    #[test]
    fn test_enum_compatibility() {
        let validator = JsonSchemaValidator::new();

        let old_schema = r#"{"enum": ["A", "B"]}"#;
        let new_schema = r#"{"enum": ["A", "B", "C"]}"#;

        // Adding enum values is backward compatible
        let result =
            validator.check_compatibility(new_schema, old_schema, CompatibilityLevel::Backward);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_enum_remove_value() {
        let validator = JsonSchemaValidator::new();

        let old_schema = r#"{"enum": ["A", "B", "C"]}"#;
        let new_schema = r#"{"enum": ["A", "B"]}"#;

        // Removing enum values is NOT backward compatible
        let result =
            validator.check_compatibility(new_schema, old_schema, CompatibilityLevel::Backward);
        assert!(result.is_ok());
        let messages = result.unwrap();
        assert!(!messages.is_empty());
    }

    #[test]
    fn test_array_compatibility() {
        let validator = JsonSchemaValidator::new();

        let old_schema = r#"{
            "type": "array",
            "items": {"type": "integer"}
        }"#;

        let new_schema = r#"{
            "type": "array",
            "items": {"type": "number"}
        }"#;

        // integer -> number is backward compatible
        let result =
            validator.check_compatibility(new_schema, old_schema, CompatibilityLevel::Backward);
        assert!(result.is_ok());
    }

    #[test]
    fn test_canonical_form() {
        let validator = JsonSchemaValidator::new();

        let schema = r#"{ "type" : "string" }"#;
        let canonical = validator.canonical_form(schema);
        assert!(canonical.is_ok());
    }
}
