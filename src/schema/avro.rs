//! Avro schema validation and compatibility checking
//!
//! This module provides Avro schema parsing, validation, and compatibility
//! checking using the `apache-avro` crate.

use super::{CompatibilityLevel, SchemaError};
use apache_avro::Schema as AvroSchema;
use std::collections::HashSet;

/// Avro schema validator
#[derive(Debug, Default)]
pub struct AvroValidator;

impl AvroValidator {
    /// Create a new Avro validator
    pub fn new() -> Self {
        Self
    }

    /// Parse and validate an Avro schema string
    pub fn parse(&self, schema_str: &str) -> Result<AvroSchema, SchemaError> {
        AvroSchema::parse_str(schema_str)
            .map_err(|e| SchemaError::InvalidSchema(format!("Invalid Avro schema: {}", e)))
    }

    /// Validate that a schema string is valid Avro
    pub fn validate(&self, schema_str: &str) -> Result<(), SchemaError> {
        self.parse(schema_str)?;
        Ok(())
    }

    /// Check if a new schema is compatible with an existing schema
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
        reader: &AvroSchema,
        writer: &AvroSchema,
    ) -> Result<Vec<String>, SchemaError> {
        let mut messages = vec![];

        // Check type compatibility
        if !Self::types_compatible(reader, writer, true) {
            messages.push(format!(
                "Type mismatch: reader expects {:?}, writer has {:?}",
                self.schema_type_name(reader),
                self.schema_type_name(writer)
            ));
        }

        // For records, check field compatibility
        if let (AvroSchema::Record(reader_rec), AvroSchema::Record(writer_rec)) = (reader, writer) {
            let writer_field_names: HashSet<_> =
                writer_rec.fields.iter().map(|f| &f.name).collect();

            for reader_field in &reader_rec.fields {
                if !writer_field_names.contains(&reader_field.name) {
                    // New field in reader - must have default
                    if reader_field.default.is_none() {
                        messages.push(format!(
                            "New field '{}' in reader has no default value",
                            reader_field.name
                        ));
                    }
                }
            }
        }

        Ok(messages)
    }

    /// Check forward compatibility (old schema can read new data)
    fn check_forward_compatible(
        &self,
        writer: &AvroSchema,
        reader: &AvroSchema,
    ) -> Result<Vec<String>, SchemaError> {
        let mut messages = vec![];

        // Check type compatibility
        if !Self::types_compatible(reader, writer, false) {
            messages.push(format!(
                "Type mismatch: reader expects {:?}, writer has {:?}",
                self.schema_type_name(reader),
                self.schema_type_name(writer)
            ));
        }

        // For records, check field compatibility
        if let (AvroSchema::Record(reader_rec), AvroSchema::Record(writer_rec)) = (reader, writer) {
            let reader_field_names: HashSet<_> =
                reader_rec.fields.iter().map(|f| &f.name).collect();

            for writer_field in &writer_rec.fields {
                if !reader_field_names.contains(&writer_field.name) {
                    // New field in writer - reader must have default or field must be optional
                    let field_in_reader = reader_rec
                        .fields
                        .iter()
                        .find(|f| f.name == writer_field.name);

                    if field_in_reader.is_none() {
                        // Field doesn't exist in reader, which is fine for forward compat
                        // as long as reader can ignore it
                    }
                }
            }
        }

        Ok(messages)
    }

    /// Check if two types are compatible
    fn types_compatible(reader: &AvroSchema, writer: &AvroSchema, backward: bool) -> bool {
        use AvroSchema::*;

        match (reader, writer) {
            // Same types
            (Null, Null) => true,
            (Boolean, Boolean) => true,
            (Int, Int) => true,
            (Long, Long) => true,
            (Float, Float) => true,
            (Double, Double) => true,
            (Bytes, Bytes) => true,
            (String, String) => true,

            // Numeric promotions (backward: reader can be wider)
            (Long, Int) if backward => true,
            (Float, Int) if backward => true,
            (Float, Long) if backward => true,
            (Double, Int) if backward => true,
            (Double, Long) if backward => true,
            (Double, Float) if backward => true,

            // Forward: writer can be wider
            (Int, Long) if !backward => true,
            (Int, Float) if !backward => true,
            (Long, Float) if !backward => true,
            (Int, Double) if !backward => true,
            (Long, Double) if !backward => true,
            (Float, Double) if !backward => true,

            // Bytes and String are interchangeable
            (String, Bytes) | (Bytes, String) => true,

            // Arrays
            (Array(r), Array(w)) => Self::types_compatible(&r.items, &w.items, backward),

            // Maps
            (Map(r), Map(w)) => Self::types_compatible(&r.types, &w.types, backward),

            // Unions - reader must accept all writer types
            (Union(r_union), Union(w_union)) => w_union.variants().iter().all(|w_variant| {
                r_union
                    .variants()
                    .iter()
                    .any(|r_variant| Self::types_compatible(r_variant, w_variant, backward))
            }),

            // Union with non-union
            (Union(r_union), w) => r_union
                .variants()
                .iter()
                .any(|r| Self::types_compatible(r, w, backward)),

            (r, Union(w_union)) => w_union
                .variants()
                .iter()
                .all(|w| Self::types_compatible(r, w, backward)),

            // Records
            (Record(r), Record(w)) => r.name == w.name,

            // Enums
            (Enum(r), Enum(w)) => {
                // Reader's symbols must be superset of writer's symbols for backward compat
                if backward {
                    w.symbols.iter().all(|s| r.symbols.contains(s))
                } else {
                    r.symbols.iter().all(|s| w.symbols.contains(s))
                }
            }

            // Fixed
            (Fixed(r), Fixed(w)) => r.name == w.name && r.size == w.size,

            _ => false,
        }
    }

    /// Get a human-readable type name for a schema
    fn schema_type_name(&self, schema: &AvroSchema) -> String {
        match schema {
            AvroSchema::Null => "null".to_string(),
            AvroSchema::Boolean => "boolean".to_string(),
            AvroSchema::Int => "int".to_string(),
            AvroSchema::Long => "long".to_string(),
            AvroSchema::Float => "float".to_string(),
            AvroSchema::Double => "double".to_string(),
            AvroSchema::Bytes => "bytes".to_string(),
            AvroSchema::String => "string".to_string(),
            AvroSchema::Array(_) => "array".to_string(),
            AvroSchema::Map(_) => "map".to_string(),
            AvroSchema::Union(_) => "union".to_string(),
            AvroSchema::Record(r) => format!("record<{}>", r.name.fullname(None)),
            AvroSchema::Enum(e) => format!("enum<{}>", e.name.fullname(None)),
            AvroSchema::Fixed(f) => format!("fixed<{}>", f.name.fullname(None)),
            _ => "unknown".to_string(),
        }
    }

    /// Validate data against a schema
    pub fn validate_data(&self, schema_str: &str, data: &[u8]) -> Result<(), SchemaError> {
        let schema = self.parse(schema_str)?;

        // Try to deserialize the data using the schema
        let reader = apache_avro::Reader::with_schema(&schema, data)
            .map_err(|e| SchemaError::InvalidSchema(format!("Failed to read Avro data: {}", e)))?;

        // Iterate through records to validate
        for value in reader {
            value.map_err(|e| {
                SchemaError::InvalidSchema(format!("Data doesn't match schema: {}", e))
            })?;
        }

        Ok(())
    }

    /// Get the canonical form of a schema (for comparison)
    pub fn canonical_form(&self, schema_str: &str) -> Result<String, SchemaError> {
        let schema = self.parse(schema_str)?;
        Ok(schema.canonical_form())
    }

    /// Get the fingerprint of a schema
    #[allow(dead_code)]
    pub fn fingerprint(&self, schema_str: &str) -> Result<Vec<u8>, SchemaError> {
        let schema = self.parse(schema_str)?;
        let rabin = schema.fingerprint::<apache_avro::rabin::Rabin>();
        Ok(rabin.bytes.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_primitive_schema() {
        let validator = AvroValidator::new();

        // Valid schemas
        assert!(validator.validate(r#""string""#).is_ok());
        assert!(validator.validate(r#""int""#).is_ok());
        assert!(validator.validate(r#""long""#).is_ok());
        assert!(validator.validate(r#""boolean""#).is_ok());
        assert!(validator.validate(r#""null""#).is_ok());
    }

    #[test]
    fn test_parse_record_schema() {
        let validator = AvroValidator::new();

        let schema = r#"{
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "age", "type": "int"}
            ]
        }"#;

        assert!(validator.validate(schema).is_ok());
    }

    #[test]
    fn test_parse_invalid_schema() {
        let validator = AvroValidator::new();

        assert!(validator.validate(r#""invalid_type""#).is_err());
        assert!(validator.validate(r#"{"type": "record"}"#).is_err()); // Missing name and fields
        assert!(validator.validate("not json").is_err());
    }

    #[test]
    fn test_backward_compatible_add_optional_field() {
        let validator = AvroValidator::new();

        let old_schema = r#"{
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "name", "type": "string"}
            ]
        }"#;

        let new_schema = r#"{
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "email", "type": "string", "default": ""}
            ]
        }"#;

        let result =
            validator.check_compatibility(new_schema, old_schema, CompatibilityLevel::Backward);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_backward_incompatible_add_required_field() {
        let validator = AvroValidator::new();

        let old_schema = r#"{
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "name", "type": "string"}
            ]
        }"#;

        let new_schema = r#"{
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "email", "type": "string"}
            ]
        }"#;

        let result =
            validator.check_compatibility(new_schema, old_schema, CompatibilityLevel::Backward);
        assert!(result.is_ok());
        let messages = result.unwrap();
        assert!(!messages.is_empty());
    }

    #[test]
    fn test_numeric_promotion_compatible() {
        let validator = AvroValidator::new();

        // Int to Long is backward compatible
        let result =
            validator.check_compatibility(r#""long""#, r#""int""#, CompatibilityLevel::Backward);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_canonical_form() {
        let validator = AvroValidator::new();

        let schema = r#"{
            "type": "record",
            "name": "User",
            "namespace": "com.example",
            "fields": [
                {"name": "name", "type": "string"}
            ]
        }"#;

        let canonical = validator.canonical_form(schema);
        assert!(canonical.is_ok());

        // Canonical form should be consistent
        let canonical2 = validator.canonical_form(schema);
        assert_eq!(canonical.unwrap(), canonical2.unwrap());
    }

    #[test]
    fn test_fingerprint() {
        let validator = AvroValidator::new();

        let schema = r#""string""#;
        let fingerprint = validator.fingerprint(schema);
        assert!(fingerprint.is_ok());
        assert!(!fingerprint.unwrap().is_empty());
    }

    #[test]
    fn test_enum_compatibility() {
        let validator = AvroValidator::new();

        let old_schema = r#"{
            "type": "enum",
            "name": "Status",
            "symbols": ["PENDING", "ACTIVE"]
        }"#;

        let new_schema = r#"{
            "type": "enum",
            "name": "Status",
            "symbols": ["PENDING", "ACTIVE", "DELETED"]
        }"#;

        // Adding symbols is backward compatible
        let result =
            validator.check_compatibility(new_schema, old_schema, CompatibilityLevel::Backward);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_array_compatibility() {
        let validator = AvroValidator::new();

        let old_schema = r#"{"type": "array", "items": "int"}"#;
        let new_schema = r#"{"type": "array", "items": "long"}"#;

        // Array<int> to Array<long> is backward compatible
        let result =
            validator.check_compatibility(new_schema, old_schema, CompatibilityLevel::Backward);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_union_compatibility() {
        let validator = AvroValidator::new();

        let old_schema = r#"["null", "string"]"#;
        let new_schema = r#"["null", "string", "int"]"#;

        // Adding type to union is backward compatible
        let result =
            validator.check_compatibility(new_schema, old_schema, CompatibilityLevel::Backward);
        assert!(result.is_ok());
    }
}
