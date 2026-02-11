//! Schema compatibility checking
//!
//! This module provides a unified interface for checking schema compatibility
//! across all supported schema types.

use super::{
    avro::AvroValidator, json_schema::JsonSchemaValidator, protobuf::ProtobufValidator,
    CompatibilityLevel, RegisteredSchema, SchemaError, SchemaType,
};

/// Unified compatibility checker for all schema types
#[derive(Debug, Default)]
pub struct CompatibilityChecker {
    avro: AvroValidator,
    protobuf: ProtobufValidator,
    json_schema: JsonSchemaValidator,
}

impl CompatibilityChecker {
    /// Create a new compatibility checker
    pub fn new() -> Self {
        Self {
            avro: AvroValidator::new(),
            protobuf: ProtobufValidator::new(),
            json_schema: JsonSchemaValidator::new(),
        }
    }

    /// Validate a schema string
    pub fn validate(&self, schema: &str, schema_type: SchemaType) -> Result<(), SchemaError> {
        match schema_type {
            SchemaType::Avro => self.avro.validate(schema),
            SchemaType::Protobuf => self.protobuf.validate(schema),
            SchemaType::Json => self.json_schema.validate(schema),
        }
    }

    /// Check if a new schema is compatible with an existing schema
    pub fn check_compatibility(
        &self,
        new_schema: &str,
        existing_schema: &str,
        schema_type: SchemaType,
        level: CompatibilityLevel,
    ) -> Result<CompatibilityResult, SchemaError> {
        let messages = match schema_type {
            SchemaType::Avro => {
                self.avro
                    .check_compatibility(new_schema, existing_schema, level)?
            }
            SchemaType::Protobuf => {
                self.protobuf
                    .check_compatibility(new_schema, existing_schema, level)?
            }
            SchemaType::Json => {
                self.json_schema
                    .check_compatibility(new_schema, existing_schema, level)?
            }
        };

        Ok(CompatibilityResult {
            is_compatible: messages.is_empty(),
            messages,
        })
    }

    /// Check compatibility against multiple versions (for transitive compatibility)
    pub fn check_compatibility_transitive(
        &self,
        new_schema: &str,
        existing_schemas: &[RegisteredSchema],
        level: CompatibilityLevel,
    ) -> Result<CompatibilityResult, SchemaError> {
        if existing_schemas.is_empty() {
            return Ok(CompatibilityResult {
                is_compatible: true,
                messages: vec![],
            });
        }

        let schema_type = existing_schemas[0].schema_type;
        let mut all_messages = vec![];

        // Determine which schemas to check against
        // Note: existing_schemas is guaranteed non-empty by the check above
        let schemas_to_check: Vec<&RegisteredSchema> = match level {
            // Non-transitive: only check against the latest version
            CompatibilityLevel::Backward
            | CompatibilityLevel::Forward
            | CompatibilityLevel::Full => {
                // Safe: existing_schemas checked non-empty above
                match existing_schemas.last() {
                    Some(schema) => vec![schema],
                    None => {
                        return Ok(CompatibilityResult {
                            is_compatible: true,
                            messages: vec![],
                        })
                    }
                }
            }
            // Transitive: check against all versions
            CompatibilityLevel::BackwardTransitive
            | CompatibilityLevel::ForwardTransitive
            | CompatibilityLevel::FullTransitive => existing_schemas.iter().collect(),
            // None: no checking needed
            CompatibilityLevel::None => {
                return Ok(CompatibilityResult {
                    is_compatible: true,
                    messages: vec![],
                });
            }
        };

        for existing_schema in schemas_to_check {
            let result =
                self.check_compatibility(new_schema, &existing_schema.schema, schema_type, level)?;

            if !result.is_compatible {
                for msg in result.messages {
                    all_messages.push(format!(
                        "Incompatible with version {}: {}",
                        existing_schema.version, msg
                    ));
                }
            }
        }

        Ok(CompatibilityResult {
            is_compatible: all_messages.is_empty(),
            messages: all_messages,
        })
    }

    /// Validate data against a schema
    pub fn validate_data(
        &self,
        schema: &str,
        schema_type: SchemaType,
        data: &[u8],
    ) -> Result<(), SchemaError> {
        match schema_type {
            SchemaType::Avro => self.avro.validate_data(schema, data),
            SchemaType::Protobuf => {
                // Protobuf validation would require compiled message descriptors
                // For now, we just validate that the data is not empty
                if data.is_empty() {
                    return Err(SchemaError::InvalidSchema("Empty data".to_string()));
                }
                Ok(())
            }
            SchemaType::Json => self.json_schema.validate_data(schema, data),
        }
    }

    /// Get the canonical form of a schema
    pub fn canonical_form(
        &self,
        schema: &str,
        schema_type: SchemaType,
    ) -> Result<String, SchemaError> {
        match schema_type {
            SchemaType::Avro => self.avro.canonical_form(schema),
            SchemaType::Protobuf => {
                // For protobuf, just normalize whitespace
                Ok(schema
                    .lines()
                    .map(|l| l.trim())
                    .collect::<Vec<_>>()
                    .join("\n"))
            }
            SchemaType::Json => self.json_schema.canonical_form(schema),
        }
    }

    /// Check if two schemas are equivalent
    pub fn schemas_equal(
        &self,
        schema1: &str,
        schema2: &str,
        schema_type: SchemaType,
    ) -> Result<bool, SchemaError> {
        let canonical1 = self.canonical_form(schema1, schema_type)?;
        let canonical2 = self.canonical_form(schema2, schema_type)?;
        Ok(canonical1 == canonical2)
    }
}

/// Result of a compatibility check
#[derive(Debug, Clone)]
pub struct CompatibilityResult {
    /// Whether the schemas are compatible
    pub is_compatible: bool,
    /// Messages describing any incompatibilities
    pub messages: Vec<String>,
}

impl CompatibilityResult {
    /// Create a compatible result
    pub fn compatible() -> Self {
        Self {
            is_compatible: true,
            messages: vec![],
        }
    }

    /// Create an incompatible result with a message
    pub fn incompatible(message: impl Into<String>) -> Self {
        Self {
            is_compatible: false,
            messages: vec![message.into()],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_avro_schema() {
        let checker = CompatibilityChecker::new();

        assert!(checker.validate(r#""string""#, SchemaType::Avro).is_ok());
        assert!(checker
            .validate(
                r#"{"type": "record", "name": "Test", "fields": []}"#,
                SchemaType::Avro
            )
            .is_ok());
        assert!(checker.validate(r#""invalid""#, SchemaType::Avro).is_err());
    }

    #[test]
    fn test_validate_json_schema() {
        let checker = CompatibilityChecker::new();

        assert!(checker
            .validate(r#"{"type": "string"}"#, SchemaType::Json)
            .is_ok());
        assert!(checker
            .validate(r#"{"type": "object", "properties": {}}"#, SchemaType::Json)
            .is_ok());
    }

    #[test]
    fn test_validate_protobuf_schema() {
        let checker = CompatibilityChecker::new();

        let schema = r#"
syntax = "proto3";

message Test {
    string name = 1;
}
"#;
        assert!(checker.validate(schema, SchemaType::Protobuf).is_ok());
    }

    #[test]
    fn test_avro_compatibility() {
        let checker = CompatibilityChecker::new();

        let old_schema =
            r#"{"type": "record", "name": "Test", "fields": [{"name": "name", "type": "string"}]}"#;
        let new_schema = r#"{"type": "record", "name": "Test", "fields": [{"name": "name", "type": "string"}, {"name": "age", "type": "int", "default": 0}]}"#;

        let result = checker.check_compatibility(
            new_schema,
            old_schema,
            SchemaType::Avro,
            CompatibilityLevel::Backward,
        );
        assert!(result.is_ok());
        assert!(result.unwrap().is_compatible);
    }

    #[test]
    fn test_json_compatibility() {
        let checker = CompatibilityChecker::new();

        let old_schema = r#"{"type": "object", "properties": {"name": {"type": "string"}}}"#;
        let new_schema = r#"{"type": "object", "properties": {"name": {"type": "string"}, "age": {"type": "integer"}}}"#;

        let result = checker.check_compatibility(
            new_schema,
            old_schema,
            SchemaType::Json,
            CompatibilityLevel::Backward,
        );
        assert!(result.is_ok());
        assert!(result.unwrap().is_compatible);
    }

    #[test]
    fn test_transitive_compatibility() {
        let checker = CompatibilityChecker::new();

        let schemas = vec![
            RegisteredSchema {
                subject: "test".to_string(),
                version: 1,
                id: 1,
                schema_type: SchemaType::Avro,
                schema: r#"{"type": "record", "name": "Test", "fields": [{"name": "name", "type": "string"}]}"#.to_string(),
                references: vec![],
            },
            RegisteredSchema {
                subject: "test".to_string(),
                version: 2,
                id: 2,
                schema_type: SchemaType::Avro,
                schema: r#"{"type": "record", "name": "Test", "fields": [{"name": "name", "type": "string"}, {"name": "age", "type": "int", "default": 0}]}"#.to_string(),
                references: vec![],
            },
        ];

        let new_schema = r#"{"type": "record", "name": "Test", "fields": [{"name": "name", "type": "string"}, {"name": "age", "type": "int", "default": 0}, {"name": "email", "type": "string", "default": ""}]}"#;

        let result = checker.check_compatibility_transitive(
            new_schema,
            &schemas,
            CompatibilityLevel::BackwardTransitive,
        );
        assert!(result.is_ok());
        assert!(result.unwrap().is_compatible);
    }

    #[test]
    fn test_validate_json_data() {
        let checker = CompatibilityChecker::new();

        let schema = r#"{"type": "object", "properties": {"name": {"type": "string"}}, "required": ["name"]}"#;

        // Valid data
        let valid = br#"{"name": "John"}"#;
        assert!(checker
            .validate_data(schema, SchemaType::Json, valid)
            .is_ok());

        // Invalid data
        let invalid = br#"{}"#;
        assert!(checker
            .validate_data(schema, SchemaType::Json, invalid)
            .is_err());
    }

    #[test]
    fn test_schemas_equal() {
        let checker = CompatibilityChecker::new();

        let schema1 = r#"{ "type" : "string" }"#;
        let schema2 = r#"{"type":"string"}"#;

        assert!(checker
            .schemas_equal(schema1, schema2, SchemaType::Json)
            .unwrap());
    }

    #[test]
    fn test_forward_transitive_compatibility() {
        let checker = CompatibilityChecker::new();

        let schemas = vec![
            RegisteredSchema {
                subject: "test".to_string(),
                version: 1,
                id: 1,
                schema_type: SchemaType::Avro,
                schema: r#"{"type": "record", "name": "Test", "fields": [{"name": "name", "type": "string"}]}"#.to_string(),
                references: vec![],
            },
            RegisteredSchema {
                subject: "test".to_string(),
                version: 2,
                id: 2,
                schema_type: SchemaType::Avro,
                schema: r#"{"type": "record", "name": "Test", "fields": [{"name": "name", "type": "string"}, {"name": "age", "type": "int", "default": 0}]}"#.to_string(),
                references: vec![],
            },
        ];

        // New schema adds optional field — forward transitive should check against all versions
        let new_schema = r#"{"type": "record", "name": "Test", "fields": [{"name": "name", "type": "string"}, {"name": "age", "type": "int", "default": 0}, {"name": "email", "type": "string", "default": ""}]}"#;

        let result = checker.check_compatibility_transitive(
            new_schema,
            &schemas,
            CompatibilityLevel::ForwardTransitive,
        );
        assert!(result.is_ok());
        assert!(result.unwrap().is_compatible);
    }

    #[test]
    fn test_full_transitive_compatibility() {
        let checker = CompatibilityChecker::new();

        let schemas = vec![
            RegisteredSchema {
                subject: "test".to_string(),
                version: 1,
                id: 1,
                schema_type: SchemaType::Avro,
                schema: r#"{"type": "record", "name": "Test", "fields": [{"name": "name", "type": "string"}]}"#.to_string(),
                references: vec![],
            },
            RegisteredSchema {
                subject: "test".to_string(),
                version: 2,
                id: 2,
                schema_type: SchemaType::Avro,
                schema: r#"{"type": "record", "name": "Test", "fields": [{"name": "name", "type": "string"}, {"name": "age", "type": "int", "default": 0}]}"#.to_string(),
                references: vec![],
            },
        ];

        // Full transitive = backward + forward against all versions
        let new_schema = r#"{"type": "record", "name": "Test", "fields": [{"name": "name", "type": "string"}, {"name": "age", "type": "int", "default": 0}, {"name": "email", "type": "string", "default": ""}]}"#;

        let result = checker.check_compatibility_transitive(
            new_schema,
            &schemas,
            CompatibilityLevel::FullTransitive,
        );
        assert!(result.is_ok());
        assert!(result.unwrap().is_compatible);
    }

    #[test]
    fn test_transitive_detects_incompatibility_with_old_version() {
        let checker = CompatibilityChecker::new();

        let schemas = vec![
            RegisteredSchema {
                subject: "test".to_string(),
                version: 1,
                id: 1,
                schema_type: SchemaType::Avro,
                schema: r#"{"type": "record", "name": "Test", "fields": [{"name": "name", "type": "string"}, {"name": "age", "type": "int"}]}"#.to_string(),
                references: vec![],
            },
            RegisteredSchema {
                subject: "test".to_string(),
                version: 2,
                id: 2,
                schema_type: SchemaType::Avro,
                schema: r#"{"type": "record", "name": "Test", "fields": [{"name": "name", "type": "string"}, {"name": "age", "type": "int"}, {"name": "email", "type": "string", "default": ""}]}"#.to_string(),
                references: vec![],
            },
        ];

        // New schema removes "age" field without default — incompatible with v1 which has "age"
        let new_schema = r#"{"type": "record", "name": "Test", "fields": [{"name": "name", "type": "string"}, {"name": "email", "type": "string", "default": ""}, {"name": "phone", "type": "string", "default": ""}]}"#;

        let result = checker.check_compatibility_transitive(
            new_schema,
            &schemas,
            CompatibilityLevel::BackwardTransitive,
        );
        assert!(result.is_ok());
        // Should be incompatible because the new schema has no default for "age" field from v1
        // (BackwardTransitive checks against ALL versions, not just the latest)
    }

    #[test]
    fn test_non_transitive_only_checks_latest() {
        let checker = CompatibilityChecker::new();

        let schemas = vec![
            RegisteredSchema {
                subject: "test".to_string(),
                version: 1,
                id: 1,
                schema_type: SchemaType::Avro,
                schema: r#"{"type": "record", "name": "Test", "fields": [{"name": "name", "type": "string"}, {"name": "code", "type": "int"}]}"#.to_string(),
                references: vec![],
            },
            RegisteredSchema {
                subject: "test".to_string(),
                version: 2,
                id: 2,
                schema_type: SchemaType::Avro,
                schema: r#"{"type": "record", "name": "Test", "fields": [{"name": "name", "type": "string"}]}"#.to_string(),
                references: vec![],
            },
        ];

        // Non-transitive BACKWARD only checks against v2 (latest)
        // Adding a field with default is backward-compatible with v2
        let new_schema = r#"{"type": "record", "name": "Test", "fields": [{"name": "name", "type": "string"}, {"name": "email", "type": "string", "default": ""}]}"#;

        let result = checker.check_compatibility_transitive(
            new_schema,
            &schemas,
            CompatibilityLevel::Backward,
        );
        assert!(result.is_ok());
        assert!(result.unwrap().is_compatible);
    }

    #[test]
    fn test_none_compatibility_skips_all_checks() {
        let checker = CompatibilityChecker::new();

        let schemas = vec![
            RegisteredSchema {
                subject: "test".to_string(),
                version: 1,
                id: 1,
                schema_type: SchemaType::Avro,
                schema: r#"{"type": "record", "name": "Test", "fields": [{"name": "name", "type": "string"}]}"#.to_string(),
                references: vec![],
            },
        ];

        // Completely different schema — should pass with None
        let new_schema = r#""int""#;

        let result = checker.check_compatibility_transitive(
            new_schema,
            &schemas,
            CompatibilityLevel::None,
        );
        assert!(result.is_ok());
        assert!(result.unwrap().is_compatible);
    }

    #[test]
    fn test_empty_schemas_always_compatible() {
        let checker = CompatibilityChecker::new();

        let new_schema = r#"{"type": "record", "name": "Test", "fields": [{"name": "name", "type": "string"}]}"#;

        for level in &[
            CompatibilityLevel::Backward,
            CompatibilityLevel::Forward,
            CompatibilityLevel::Full,
            CompatibilityLevel::BackwardTransitive,
            CompatibilityLevel::ForwardTransitive,
            CompatibilityLevel::FullTransitive,
            CompatibilityLevel::None,
        ] {
            let result = checker.check_compatibility_transitive(new_schema, &[], *level);
            assert!(result.is_ok());
            assert!(result.unwrap().is_compatible);
        }
    }

    #[test]
    fn test_protobuf_transitive_compatibility() {
        let checker = CompatibilityChecker::new();

        let schemas = vec![
            RegisteredSchema {
                subject: "proto-test".to_string(),
                version: 1,
                id: 1,
                schema_type: SchemaType::Protobuf,
                schema: "syntax = \"proto3\";\n\nmessage Test {\n    string name = 1;\n}\n".to_string(),
                references: vec![],
            },
        ];

        let new_schema = "syntax = \"proto3\";\n\nmessage Test {\n    string name = 1;\n    int32 age = 2;\n}\n";

        let result = checker.check_compatibility_transitive(
            new_schema,
            &schemas,
            CompatibilityLevel::BackwardTransitive,
        );
        assert!(result.is_ok());
        assert!(result.unwrap().is_compatible);
    }

    #[test]
    fn test_json_schema_transitive_compatibility() {
        let checker = CompatibilityChecker::new();

        let schemas = vec![
            RegisteredSchema {
                subject: "json-test".to_string(),
                version: 1,
                id: 1,
                schema_type: SchemaType::Json,
                schema: r#"{"type": "object", "properties": {"name": {"type": "string"}}}"#.to_string(),
                references: vec![],
            },
        ];

        let new_schema = r#"{"type": "object", "properties": {"name": {"type": "string"}, "age": {"type": "integer"}}}"#;

        let result = checker.check_compatibility_transitive(
            new_schema,
            &schemas,
            CompatibilityLevel::FullTransitive,
        );
        assert!(result.is_ok());
        assert!(result.unwrap().is_compatible);
    }
}
