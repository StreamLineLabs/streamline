//! Protocol Buffers schema validation and compatibility checking
//!
//! This module provides Protobuf schema parsing, validation, and basic
//! compatibility checking.

use super::{CompatibilityLevel, SchemaError, SchemaReference};
use std::collections::{HashMap, HashSet};

/// Well-known Protobuf imports that don't require external resolution
const WELL_KNOWN_IMPORTS: &[&str] = &[
    "google/protobuf/any.proto",
    "google/protobuf/api.proto",
    "google/protobuf/descriptor.proto",
    "google/protobuf/duration.proto",
    "google/protobuf/empty.proto",
    "google/protobuf/field_mask.proto",
    "google/protobuf/source_context.proto",
    "google/protobuf/struct.proto",
    "google/protobuf/timestamp.proto",
    "google/protobuf/type.proto",
    "google/protobuf/wrappers.proto",
];

/// Well-known types and the import that provides them
const WELL_KNOWN_TYPES: &[(&str, &str)] = &[
    ("google.protobuf.Any", "google/protobuf/any.proto"),
    ("google.protobuf.Duration", "google/protobuf/duration.proto"),
    ("google.protobuf.Empty", "google/protobuf/empty.proto"),
    ("google.protobuf.FieldMask", "google/protobuf/field_mask.proto"),
    ("google.protobuf.Struct", "google/protobuf/struct.proto"),
    ("google.protobuf.Timestamp", "google/protobuf/timestamp.proto"),
    ("google.protobuf.Value", "google/protobuf/struct.proto"),
    ("google.protobuf.ListValue", "google/protobuf/struct.proto"),
    ("google.protobuf.BoolValue", "google/protobuf/wrappers.proto"),
    ("google.protobuf.BytesValue", "google/protobuf/wrappers.proto"),
    ("google.protobuf.DoubleValue", "google/protobuf/wrappers.proto"),
    ("google.protobuf.FloatValue", "google/protobuf/wrappers.proto"),
    ("google.protobuf.Int32Value", "google/protobuf/wrappers.proto"),
    ("google.protobuf.Int64Value", "google/protobuf/wrappers.proto"),
    ("google.protobuf.StringValue", "google/protobuf/wrappers.proto"),
    ("google.protobuf.UInt32Value", "google/protobuf/wrappers.proto"),
    ("google.protobuf.UInt64Value", "google/protobuf/wrappers.proto"),
];

/// Protobuf schema validator
#[derive(Debug, Default)]
pub struct ProtobufValidator;

/// Parsed Protobuf schema representation
#[derive(Debug, Clone)]
pub struct ProtobufSchema {
    /// Schema syntax (proto2 or proto3)
    pub syntax: String,
    /// Package name
    pub package: Option<String>,
    /// Message definitions
    pub messages: Vec<MessageDefinition>,
    /// Enum definitions
    pub enums: Vec<EnumDefinition>,
    /// Import statements
    pub imports: Vec<String>,
}

/// A message definition
#[derive(Debug, Clone)]
pub struct MessageDefinition {
    /// Message name
    pub name: String,
    /// Message fields
    pub fields: Vec<FieldDefinition>,
    /// Nested messages
    pub nested_messages: Vec<MessageDefinition>,
    /// Nested enums
    pub nested_enums: Vec<EnumDefinition>,
    /// Oneof groups
    pub oneofs: Vec<OneofDefinition>,
    /// Reserved field numbers
    pub reserved_numbers: HashSet<i32>,
    /// Reserved field names
    pub reserved_names: HashSet<String>,
}

/// A field definition
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct FieldDefinition {
    /// Field name
    pub name: String,
    /// Field number
    pub number: i32,
    /// Field type
    pub field_type: String,
    /// Whether the field is repeated
    pub repeated: bool,
    /// Whether the field is optional (proto2)
    pub optional: bool,
    /// Whether the field is required (proto2)
    pub required: bool,
    /// Oneof group name if part of a oneof
    pub oneof: Option<String>,
    /// Default value (proto2)
    pub default: Option<String>,
}

/// An enum definition
#[derive(Debug, Clone)]
pub struct EnumDefinition {
    /// Enum name
    pub name: String,
    /// Enum values
    pub values: Vec<EnumValue>,
    /// Whether allow_alias is enabled
    pub allow_alias: bool,
}

/// An enum value
#[derive(Debug, Clone)]
pub struct EnumValue {
    /// Value name
    pub name: String,
    /// Value number
    pub number: i32,
}

/// A oneof definition
#[derive(Debug, Clone)]
pub struct OneofDefinition {
    /// Oneof name
    pub name: String,
    /// Fields in this oneof
    pub field_names: Vec<String>,
}

impl ProtobufValidator {
    /// Create a new Protobuf validator
    pub fn new() -> Self {
        Self
    }

    /// Parse a Protobuf schema string
    pub fn parse(&self, schema_str: &str) -> Result<ProtobufSchema, SchemaError> {
        let mut schema = ProtobufSchema {
            syntax: "proto3".to_string(),
            package: None,
            messages: vec![],
            enums: vec![],
            imports: vec![],
        };

        let lines: Vec<&str> = schema_str.lines().collect();
        let mut i = 0;

        while i < lines.len() {
            let line = lines[i].trim();

            // Skip empty lines and comments
            if line.is_empty() || line.starts_with("//") {
                i += 1;
                continue;
            }

            // Parse syntax
            if line.starts_with("syntax") {
                schema.syntax = self.parse_syntax(line)?;
            }
            // Parse package
            else if line.starts_with("package") {
                schema.package = Some(self.parse_package(line)?);
            }
            // Parse import
            else if line.starts_with("import") {
                schema.imports.push(self.parse_import(line)?);
            }
            // Parse message
            else if line.starts_with("message") {
                let (msg, consumed) = self.parse_message(&lines[i..])?;
                schema.messages.push(msg);
                i += consumed;
                continue;
            }
            // Parse enum
            else if line.starts_with("enum") {
                let (enum_def, consumed) = self.parse_enum(&lines[i..])?;
                schema.enums.push(enum_def);
                i += consumed;
                continue;
            }
            // Parse option (ignore for now)
            else if line.starts_with("option") {
                // Skip options
            }

            i += 1;
        }

        Ok(schema)
    }

    fn parse_syntax(&self, line: &str) -> Result<String, SchemaError> {
        // syntax = "proto3";
        let parts: Vec<&str> = line.split('=').collect();
        if parts.len() != 2 {
            return Err(SchemaError::InvalidSchema(
                "Invalid syntax declaration".to_string(),
            ));
        }

        let syntax = parts[1]
            .trim()
            .trim_matches(|c| c == '"' || c == '\'' || c == ';')
            .to_string();

        if syntax != "proto2" && syntax != "proto3" {
            return Err(SchemaError::InvalidSchema(format!(
                "Unknown syntax: {}",
                syntax
            )));
        }

        Ok(syntax)
    }

    fn parse_package(&self, line: &str) -> Result<String, SchemaError> {
        // package com.example;
        let package = line
            .trim_start_matches("package")
            .trim()
            .trim_end_matches(';')
            .to_string();

        if package.is_empty() {
            return Err(SchemaError::InvalidSchema("Empty package name".to_string()));
        }

        Ok(package)
    }

    fn parse_import(&self, line: &str) -> Result<String, SchemaError> {
        // import "other.proto";
        let import = line
            .trim_start_matches("import")
            .trim()
            .trim_matches(|c| c == '"' || c == '\'' || c == ';')
            .to_string();

        Ok(import)
    }

    fn parse_message(&self, lines: &[&str]) -> Result<(MessageDefinition, usize), SchemaError> {
        let first_line = lines[0].trim();

        // Extract message name
        let name = first_line
            .trim_start_matches("message")
            .trim()
            .trim_end_matches('{')
            .trim()
            .to_string();

        if name.is_empty() {
            return Err(SchemaError::InvalidSchema("Empty message name".to_string()));
        }

        let mut msg = MessageDefinition {
            name,
            fields: vec![],
            nested_messages: vec![],
            nested_enums: vec![],
            oneofs: vec![],
            reserved_numbers: HashSet::new(),
            reserved_names: HashSet::new(),
        };

        let mut brace_count = if first_line.contains('{') { 1 } else { 0 };
        let mut i = 1;
        let mut current_oneof: Option<String> = None;

        while i < lines.len() && brace_count > 0 {
            let line = lines[i].trim();

            // Count braces
            brace_count += line.chars().filter(|&c| c == '{').count();
            brace_count -= line.chars().filter(|&c| c == '}').count();

            if brace_count == 0 {
                i += 1;
                break;
            }

            // Skip empty lines and comments
            if line.is_empty() || line.starts_with("//") {
                i += 1;
                continue;
            }

            // Parse nested message
            if line.starts_with("message") {
                let (nested_msg, consumed) = self.parse_message(&lines[i..])?;
                msg.nested_messages.push(nested_msg);
                i += consumed;
                continue;
            }

            // Parse nested enum
            if line.starts_with("enum") {
                let (nested_enum, consumed) = self.parse_enum(&lines[i..])?;
                msg.nested_enums.push(nested_enum);
                i += consumed;
                continue;
            }

            // Parse oneof
            if line.starts_with("oneof") {
                let oneof_name = line
                    .trim_start_matches("oneof")
                    .trim()
                    .trim_end_matches('{')
                    .trim()
                    .to_string();
                current_oneof = Some(oneof_name.clone());
                msg.oneofs.push(OneofDefinition {
                    name: oneof_name,
                    field_names: vec![],
                });
            }

            // Parse reserved
            if line.starts_with("reserved") {
                self.parse_reserved(line, &mut msg)?;
                i += 1;
                continue;
            }

            // Parse field
            if let Some(field) = self.parse_field(line, &current_oneof)? {
                // Add to oneof if we're in one
                if let Some(ref oneof_name) = current_oneof {
                    if let Some(oneof) = msg.oneofs.iter_mut().find(|o| &o.name == oneof_name) {
                        oneof.field_names.push(field.name.clone());
                    }
                }
                msg.fields.push(field);
            }

            // End of oneof
            if line == "}" && current_oneof.is_some() {
                current_oneof = None;
            }

            i += 1;
        }

        Ok((msg, i))
    }

    fn parse_enum(&self, lines: &[&str]) -> Result<(EnumDefinition, usize), SchemaError> {
        let first_line = lines[0].trim();

        // Extract enum name
        let name = first_line
            .trim_start_matches("enum")
            .trim()
            .trim_end_matches('{')
            .trim()
            .to_string();

        if name.is_empty() {
            return Err(SchemaError::InvalidSchema("Empty enum name".to_string()));
        }

        let mut enum_def = EnumDefinition {
            name,
            values: vec![],
            allow_alias: false,
        };

        let mut brace_count = if first_line.contains('{') { 1 } else { 0 };
        let mut i = 1;

        while i < lines.len() && brace_count > 0 {
            let line = lines[i].trim();

            // Count braces
            brace_count += line.chars().filter(|&c| c == '{').count();
            brace_count -= line.chars().filter(|&c| c == '}').count();

            if brace_count == 0 {
                i += 1;
                break;
            }

            // Skip empty lines and comments
            if line.is_empty() || line.starts_with("//") {
                i += 1;
                continue;
            }

            // Check for allow_alias option
            if line.contains("allow_alias") && line.contains("true") {
                enum_def.allow_alias = true;
                i += 1;
                continue;
            }

            // Parse enum value
            if let Some(value) = self.parse_enum_value(line)? {
                enum_def.values.push(value);
            }

            i += 1;
        }

        Ok((enum_def, i))
    }

    fn parse_field(
        &self,
        line: &str,
        current_oneof: &Option<String>,
    ) -> Result<Option<FieldDefinition>, SchemaError> {
        let line = line.trim_end_matches(';');

        // Skip options and other non-field lines
        if line.starts_with("option")
            || line.starts_with("reserved")
            || line.starts_with("}")
            || line.starts_with("{")
            || line.is_empty()
        {
            return Ok(None);
        }

        let mut parts: Vec<&str> = line.split_whitespace().collect();

        if parts.is_empty() {
            return Ok(None);
        }

        let mut repeated = false;
        let mut optional = false;
        let mut required = false;

        // Check for modifiers
        if parts[0] == "repeated" {
            repeated = true;
            parts.remove(0);
        } else if parts[0] == "optional" {
            optional = true;
            parts.remove(0);
        } else if parts[0] == "required" {
            required = true;
            parts.remove(0);
        }

        if parts.len() < 3 {
            return Ok(None);
        }

        let field_type = parts[0].to_string();
        let name = parts[1].to_string();

        // Parse field number from "= N" part
        let number_str = parts.get(3).or(parts.get(2)).unwrap_or(&"0");
        let number = number_str
            .trim_matches(|c: char| !c.is_ascii_digit() && c != '-')
            .parse::<i32>()
            .unwrap_or(0);

        if number == 0 {
            return Ok(None);
        }

        Ok(Some(FieldDefinition {
            name,
            number,
            field_type,
            repeated,
            optional,
            required,
            oneof: current_oneof.clone(),
            default: None,
        }))
    }

    fn parse_enum_value(&self, line: &str) -> Result<Option<EnumValue>, SchemaError> {
        let line = line.trim_end_matches(';');

        // Skip options
        if line.starts_with("option") || line.is_empty() {
            return Ok(None);
        }

        let parts: Vec<&str> = line.split('=').collect();
        if parts.len() != 2 {
            return Ok(None);
        }

        let name = parts[0].trim().to_string();
        let number_str = parts[1].trim();
        let number = number_str
            .trim_matches(|c: char| !c.is_ascii_digit() && c != '-')
            .parse::<i32>()
            .unwrap_or(0);

        Ok(Some(EnumValue { name, number }))
    }

    fn parse_reserved(&self, line: &str, msg: &mut MessageDefinition) -> Result<(), SchemaError> {
        let content = line
            .trim_start_matches("reserved")
            .trim()
            .trim_end_matches(';');

        for part in content.split(',') {
            let part = part.trim();

            // Check if it's a name (quoted string)
            if part.starts_with('"') || part.starts_with('\'') {
                let name = part.trim_matches(|c| c == '"' || c == '\'');
                msg.reserved_names.insert(name.to_string());
            }
            // Check if it's a range
            else if part.contains(" to ") {
                let range_parts: Vec<&str> = part.split(" to ").collect();
                if range_parts.len() == 2 {
                    if let (Ok(start), Ok(end)) = (
                        range_parts[0].trim().parse::<i32>(),
                        range_parts[1].trim().parse::<i32>(),
                    ) {
                        for num in start..=end {
                            msg.reserved_numbers.insert(num);
                        }
                    }
                }
            }
            // Single number
            else if let Ok(num) = part.parse::<i32>() {
                msg.reserved_numbers.insert(num);
            }
        }

        Ok(())
    }

    /// Validate that a schema string is valid Protobuf
    pub fn validate(&self, schema_str: &str) -> Result<(), SchemaError> {
        let schema = self.parse(schema_str)?;

        // Validate messages
        for msg in &schema.messages {
            self.validate_message(msg)?;
        }

        // Validate enums
        for enum_def in &schema.enums {
            self.validate_enum(enum_def)?;
        }

        // Validate type references within the schema
        self.validate_type_references(&schema)?;

        Ok(())
    }

    /// Validate a schema along with referenced schemas
    pub fn validate_with_references(
        &self,
        schema_str: &str,
        references: &[(&str, &str)],
    ) -> Result<(), SchemaError> {
        let schema = self.parse(schema_str)?;

        // Parse all referenced schemas
        let mut ref_schemas: HashMap<String, ProtobufSchema> = HashMap::new();
        for (name, ref_str) in references {
            let ref_schema = self.parse(ref_str)?;
            ref_schemas.insert(name.to_string(), ref_schema);
        }

        // Validate messages
        for msg in &schema.messages {
            self.validate_message(msg)?;
        }

        // Validate enums
        for enum_def in &schema.enums {
            self.validate_enum(enum_def)?;
        }

        // Validate type references, considering referenced schemas
        self.validate_type_references_with_deps(&schema, &ref_schemas)?;

        Ok(())
    }

    /// Resolve imports and return unresolved ones
    pub fn resolve_imports(
        &self,
        schema: &ProtobufSchema,
        available_refs: &[SchemaReference],
    ) -> Result<Vec<String>, SchemaError> {
        let mut unresolved = Vec::new();

        for import in &schema.imports {
            // Well-known imports are always resolved
            if self.is_well_known_import(import) {
                continue;
            }

            // Check if a reference provides this import
            let resolved = available_refs.iter().any(|r| r.name == *import);
            if !resolved {
                unresolved.push(import.clone());
            }
        }

        Ok(unresolved)
    }

    /// Check if an import is a well-known Protobuf import
    fn is_well_known_import(&self, import: &str) -> bool {
        WELL_KNOWN_IMPORTS.contains(&import)
    }

    /// Check if a type name is a well-known Protobuf type
    fn is_well_known_type(&self, type_name: &str) -> bool {
        WELL_KNOWN_TYPES.iter().any(|(t, _)| *t == type_name)
    }

    /// Check if a type is a built-in scalar type
    fn is_scalar_type(&self, type_name: &str) -> bool {
        matches!(
            type_name,
            "double"
                | "float"
                | "int32"
                | "int64"
                | "uint32"
                | "uint64"
                | "sint32"
                | "sint64"
                | "fixed32"
                | "fixed64"
                | "sfixed32"
                | "sfixed64"
                | "bool"
                | "string"
                | "bytes"
        )
    }

    /// Check if a type is a map type (e.g., map<string, int32>)
    fn is_map_type(&self, type_name: &str) -> bool {
        type_name.starts_with("map<")
    }

    /// Collect all defined type names in a schema (messages + enums, including nested)
    fn collect_defined_types(
        &self,
        schema: &ProtobufSchema,
    ) -> HashSet<String> {
        let mut types = HashSet::new();
        let prefix = schema.package.as_deref().unwrap_or("");

        for msg in &schema.messages {
            self.collect_message_types(msg, prefix, &mut types);
        }
        for enum_def in &schema.enums {
            let full_name = if prefix.is_empty() {
                enum_def.name.clone()
            } else {
                format!("{}.{}", prefix, enum_def.name)
            };
            types.insert(full_name);
            types.insert(enum_def.name.clone());
        }

        types
    }

    fn collect_message_types(
        &self,
        msg: &MessageDefinition,
        prefix: &str,
        types: &mut HashSet<String>,
    ) {
        let full_name = if prefix.is_empty() {
            msg.name.clone()
        } else {
            format!("{}.{}", prefix, msg.name)
        };
        types.insert(full_name.clone());
        types.insert(msg.name.clone());

        for nested in &msg.nested_messages {
            self.collect_message_types(nested, &full_name, types);
        }
        for nested_enum in &msg.nested_enums {
            let enum_full = format!("{}.{}", full_name, nested_enum.name);
            types.insert(enum_full);
            types.insert(nested_enum.name.clone());
        }
    }

    /// Validate that all field type references can be resolved within the schema
    fn validate_type_references(&self, schema: &ProtobufSchema) -> Result<(), SchemaError> {
        let defined_types = self.collect_defined_types(schema);

        for msg in &schema.messages {
            self.validate_field_type_refs(msg, &defined_types, &schema.imports)?;
        }

        Ok(())
    }

    /// Validate type references considering dependent schemas
    fn validate_type_references_with_deps(
        &self,
        schema: &ProtobufSchema,
        ref_schemas: &HashMap<String, ProtobufSchema>,
    ) -> Result<(), SchemaError> {
        let mut all_types = self.collect_defined_types(schema);

        // Add types from referenced schemas
        for ref_schema in ref_schemas.values() {
            all_types.extend(self.collect_defined_types(ref_schema));
        }

        for msg in &schema.messages {
            self.validate_field_type_refs(msg, &all_types, &schema.imports)?;
        }

        Ok(())
    }

    fn validate_field_type_refs(
        &self,
        msg: &MessageDefinition,
        defined_types: &HashSet<String>,
        imports: &[String],
    ) -> Result<(), SchemaError> {
        for field in &msg.fields {
            let ft = &field.field_type;

            // Skip scalar types and maps
            if self.is_scalar_type(ft) || self.is_map_type(ft) {
                continue;
            }

            // Skip well-known types
            if self.is_well_known_type(ft) {
                continue;
            }

            // Check if the type is defined locally
            if defined_types.contains(ft) {
                continue;
            }

            // Strip leading dot for fully-qualified names
            let stripped = ft.strip_prefix('.').unwrap_or(ft);
            if defined_types.contains(stripped) {
                continue;
            }

            // If there are imports, the type might come from an imported file
            // We allow unresolved types when imports are present (they may provide the type)
            let has_relevant_import = !imports.is_empty()
                && !imports.iter().all(|i| self.is_well_known_import(i));
            if has_relevant_import {
                continue;
            }

            return Err(SchemaError::InvalidSchema(format!(
                "Unresolved type reference '{}' in message '{}'. \
                 If this type is defined in another .proto file, add it as a schema reference.",
                ft, msg.name
            )));
        }

        // Recurse into nested messages
        for nested in &msg.nested_messages {
            self.validate_field_type_refs(nested, defined_types, imports)?;
        }

        Ok(())
    }

    fn validate_message(&self, msg: &MessageDefinition) -> Result<(), SchemaError> {
        let mut field_numbers: HashMap<i32, &str> = HashMap::new();
        let mut field_names: HashSet<&str> = HashSet::new();

        for field in &msg.fields {
            // Check for duplicate field numbers
            if let Some(existing) = field_numbers.get(&field.number) {
                return Err(SchemaError::InvalidSchema(format!(
                    "Duplicate field number {} for fields '{}' and '{}' in message '{}'",
                    field.number, existing, field.name, msg.name
                )));
            }
            field_numbers.insert(field.number, &field.name);

            // Check for duplicate field names
            if !field_names.insert(&field.name) {
                return Err(SchemaError::InvalidSchema(format!(
                    "Duplicate field name '{}' in message '{}'",
                    field.name, msg.name
                )));
            }

            // Check for reserved numbers
            if msg.reserved_numbers.contains(&field.number) {
                return Err(SchemaError::InvalidSchema(format!(
                    "Field number {} is reserved in message '{}'",
                    field.number, msg.name
                )));
            }

            // Check for reserved names
            if msg.reserved_names.contains(&field.name) {
                return Err(SchemaError::InvalidSchema(format!(
                    "Field name '{}' is reserved in message '{}'",
                    field.name, msg.name
                )));
            }

            // Validate field number range (1 to 536870911, excluding 19000-19999)
            if field.number < 1
                || field.number > 536870911
                || (field.number >= 19000 && field.number <= 19999)
            {
                return Err(SchemaError::InvalidSchema(format!(
                    "Invalid field number {} in message '{}' (must be 1-536870911, excluding 19000-19999)",
                    field.number, msg.name
                )));
            }
        }

        // Validate nested messages
        for nested in &msg.nested_messages {
            self.validate_message(nested)?;
        }

        // Validate nested enums
        for nested in &msg.nested_enums {
            self.validate_enum(nested)?;
        }

        Ok(())
    }

    fn validate_enum(&self, enum_def: &EnumDefinition) -> Result<(), SchemaError> {
        let mut value_numbers: HashMap<i32, &str> = HashMap::new();

        // In proto3, first enum value must be 0
        // We'll be lenient here since we support both proto2 and proto3

        for value in &enum_def.values {
            // Check for duplicate numbers (unless allow_alias)
            if !enum_def.allow_alias {
                if let Some(existing) = value_numbers.get(&value.number) {
                    return Err(SchemaError::InvalidSchema(format!(
                        "Duplicate enum value {} for '{}' and '{}' in enum '{}' (use allow_alias = true to allow)",
                        value.number, existing, value.name, enum_def.name
                    )));
                }
            }
            value_numbers.insert(value.number, &value.name);
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

    fn check_backward_compatible(
        &self,
        new_schema: &ProtobufSchema,
        existing_schema: &ProtobufSchema,
    ) -> Result<Vec<String>, SchemaError> {
        let mut messages = vec![];

        // Check messages
        for existing_msg in &existing_schema.messages {
            let new_msg = new_schema
                .messages
                .iter()
                .find(|m| m.name == existing_msg.name);

            match new_msg {
                None => {
                    // Message removed - not backward compatible
                    messages.push(format!("Message '{}' was removed", existing_msg.name));
                }
                Some(new_msg) => {
                    // Check fields
                    for existing_field in &existing_msg.fields {
                        let new_field = new_msg
                            .fields
                            .iter()
                            .find(|f| f.number == existing_field.number);

                        match new_field {
                            None => {
                                // Field removed - check if reserved
                                if !new_msg.reserved_numbers.contains(&existing_field.number) {
                                    messages.push(format!(
                                        "Field '{}' (number {}) removed from message '{}' without being reserved",
                                        existing_field.name, existing_field.number, existing_msg.name
                                    ));
                                }
                            }
                            Some(new_field) => {
                                // Check type compatibility
                                if !self.types_compatible(
                                    &new_field.field_type,
                                    &existing_field.field_type,
                                ) {
                                    messages.push(format!(
                                        "Field '{}' in message '{}' changed type from '{}' to '{}'",
                                        existing_field.name,
                                        existing_msg.name,
                                        existing_field.field_type,
                                        new_field.field_type
                                    ));
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(messages)
    }

    fn check_forward_compatible(
        &self,
        new_schema: &ProtobufSchema,
        existing_schema: &ProtobufSchema,
    ) -> Result<Vec<String>, SchemaError> {
        let mut messages = vec![];

        // Check messages
        for new_msg in &new_schema.messages {
            let existing_msg = existing_schema
                .messages
                .iter()
                .find(|m| m.name == new_msg.name);

            if let Some(existing_msg) = existing_msg {
                // Check for new required fields
                for new_field in &new_msg.fields {
                    let existing_field = existing_msg
                        .fields
                        .iter()
                        .find(|f| f.number == new_field.number);

                    if existing_field.is_none() && new_field.required {
                        messages.push(format!(
                            "New required field '{}' added to message '{}'",
                            new_field.name, new_msg.name
                        ));
                    }
                }
            }
        }

        Ok(messages)
    }

    fn types_compatible(&self, new_type: &str, existing_type: &str) -> bool {
        if new_type == existing_type {
            return true;
        }

        // Wire-compatible types (can be read interchangeably)
        let compatible_groups: Vec<Vec<&str>> = vec![
            vec!["int32", "uint32", "int64", "uint64", "bool"],
            vec!["sint32", "sint64"],
            vec!["fixed32", "sfixed32"],
            vec!["fixed64", "sfixed64"],
            vec!["string", "bytes"],
        ];

        for group in &compatible_groups {
            if group.contains(&new_type) && group.contains(&existing_type) {
                return true;
            }
        }

        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_proto() {
        let validator = ProtobufValidator::new();

        let schema = r#"
syntax = "proto3";

package example;

message Person {
    string name = 1;
    int32 age = 2;
    repeated string emails = 3;
}
"#;

        let result = validator.parse(schema);
        assert!(result.is_ok());

        let parsed = result.unwrap();
        assert_eq!(parsed.syntax, "proto3");
        assert_eq!(parsed.package, Some("example".to_string()));
        assert_eq!(parsed.messages.len(), 1);
        assert_eq!(parsed.messages[0].name, "Person");
        assert_eq!(parsed.messages[0].fields.len(), 3);
    }

    #[test]
    fn test_parse_enum() {
        let validator = ProtobufValidator::new();

        let schema = r#"
syntax = "proto3";

enum Status {
    UNKNOWN = 0;
    ACTIVE = 1;
    INACTIVE = 2;
}
"#;

        let result = validator.parse(schema);
        assert!(result.is_ok());

        let parsed = result.unwrap();
        assert_eq!(parsed.enums.len(), 1);
        assert_eq!(parsed.enums[0].name, "Status");
        assert_eq!(parsed.enums[0].values.len(), 3);
    }

    #[test]
    fn test_validate_duplicate_field_number() {
        let validator = ProtobufValidator::new();

        let schema = r#"
syntax = "proto3";

message Invalid {
    string name = 1;
    int32 age = 1;
}
"#;

        let result = validator.validate(schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_reserved_field() {
        let validator = ProtobufValidator::new();

        let schema = r#"
syntax = "proto3";

message Invalid {
    reserved 2;
    string name = 1;
    int32 age = 2;
}
"#;

        let result = validator.validate(schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_backward_compatible_add_field() {
        let validator = ProtobufValidator::new();

        let old_schema = r#"
syntax = "proto3";

message Person {
    string name = 1;
}
"#;

        let new_schema = r#"
syntax = "proto3";

message Person {
    string name = 1;
    int32 age = 2;
}
"#;

        let result =
            validator.check_compatibility(new_schema, old_schema, CompatibilityLevel::Backward);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_backward_incompatible_remove_field() {
        let validator = ProtobufValidator::new();

        let old_schema = r#"
syntax = "proto3";

message Person {
    string name = 1;
    int32 age = 2;
}
"#;

        let new_schema = r#"
syntax = "proto3";

message Person {
    string name = 1;
}
"#;

        let result =
            validator.check_compatibility(new_schema, old_schema, CompatibilityLevel::Backward);
        assert!(result.is_ok());
        let messages = result.unwrap();
        assert!(!messages.is_empty());
    }

    #[test]
    fn test_backward_compatible_remove_with_reserved() {
        let validator = ProtobufValidator::new();

        let old_schema = r#"
syntax = "proto3";

message Person {
    string name = 1;
    int32 age = 2;
}
"#;

        let new_schema = r#"
syntax = "proto3";

message Person {
    reserved 2;
    string name = 1;
}
"#;

        let result =
            validator.check_compatibility(new_schema, old_schema, CompatibilityLevel::Backward);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_parse_imports() {
        let validator = ProtobufValidator::new();

        let schema = r#"
syntax = "proto3";

import "google/protobuf/timestamp.proto";
import "other.proto";

package example;

message Event {
    string name = 1;
    int64 id = 2;
}
"#;

        let result = validator.parse(schema);
        assert!(result.is_ok());

        let parsed = result.unwrap();
        assert_eq!(parsed.imports.len(), 2);
        assert_eq!(parsed.imports[0], "google/protobuf/timestamp.proto");
        assert_eq!(parsed.imports[1], "other.proto");
    }

    #[test]
    fn test_well_known_import_resolution() {
        let validator = ProtobufValidator::new();

        let schema = r#"
syntax = "proto3";

import "google/protobuf/timestamp.proto";

message Event {
    string name = 1;
}
"#;

        let parsed = validator.parse(schema).unwrap();
        let unresolved = validator.resolve_imports(&parsed, &[]).unwrap();
        // Well-known imports should be auto-resolved
        assert!(unresolved.is_empty());
    }

    #[test]
    fn test_unresolved_import() {
        let validator = ProtobufValidator::new();

        let schema = r#"
syntax = "proto3";

import "custom/types.proto";

message Event {
    string name = 1;
}
"#;

        let parsed = validator.parse(schema).unwrap();
        let unresolved = validator.resolve_imports(&parsed, &[]).unwrap();
        assert_eq!(unresolved.len(), 1);
        assert_eq!(unresolved[0], "custom/types.proto");
    }

    #[test]
    fn test_import_resolved_by_reference() {
        let validator = ProtobufValidator::new();

        let schema = r#"
syntax = "proto3";

import "custom/types.proto";

message Event {
    string name = 1;
}
"#;

        let parsed = validator.parse(schema).unwrap();
        let refs = vec![super::SchemaReference {
            name: "custom/types.proto".to_string(),
            subject: "custom-types-value".to_string(),
            version: 1,
        }];
        let unresolved = validator.resolve_imports(&parsed, &refs).unwrap();
        assert!(unresolved.is_empty());
    }

    #[test]
    fn test_validate_with_references() {
        let validator = ProtobufValidator::new();

        let main_schema = r#"
syntax = "proto3";

import "address.proto";

message Person {
    string name = 1;
    Address home = 2;
}
"#;

        let ref_schema = r#"
syntax = "proto3";

message Address {
    string street = 1;
    string city = 2;
}
"#;

        let result = validator.validate_with_references(
            main_schema,
            &[("address.proto", ref_schema)],
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_type_reference_local_enum() {
        let validator = ProtobufValidator::new();

        let schema = r#"
syntax = "proto3";

enum Status {
    UNKNOWN = 0;
    ACTIVE = 1;
}

message Person {
    string name = 1;
    Status status = 2;
}
"#;

        // Should validate fine since Status is defined locally
        assert!(validator.validate(schema).is_ok());
    }

    #[test]
    fn test_type_reference_nested_message() {
        let validator = ProtobufValidator::new();

        let schema = r#"
syntax = "proto3";

message Outer {
    string name = 1;
    Inner nested = 2;

    message Inner {
        int32 value = 1;
    }
}
"#;

        assert!(validator.validate(schema).is_ok());
    }

    #[test]
    fn test_full_compatibility() {
        let validator = ProtobufValidator::new();

        let old_schema = r#"
syntax = "proto3";

message Person {
    string name = 1;
}
"#;

        let new_schema = r#"
syntax = "proto3";

message Person {
    string name = 1;
    int32 age = 2;
}
"#;

        // Full compatibility = backward + forward
        let result =
            validator.check_compatibility(new_schema, old_schema, CompatibilityLevel::Full);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_forward_incompatible_new_required_field() {
        let validator = ProtobufValidator::new();

        let old_schema = r#"
syntax = "proto2";

message Person {
    required string name = 1;
}
"#;

        let new_schema = r#"
syntax = "proto2";

message Person {
    required string name = 1;
    required int32 age = 2;
}
"#;

        let result =
            validator.check_compatibility(new_schema, old_schema, CompatibilityLevel::Forward);
        assert!(result.is_ok());
        let messages = result.unwrap();
        assert!(!messages.is_empty());
    }

    #[test]
    fn test_none_compatibility() {
        let validator = ProtobufValidator::new();

        let old_schema = r#"
syntax = "proto3";

message Person {
    string name = 1;
}
"#;

        let new_schema = r#"
syntax = "proto3";

message Completely {
    int32 different = 1;
}
"#;

        let result =
            validator.check_compatibility(new_schema, old_schema, CompatibilityLevel::None);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }
}
