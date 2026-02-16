//! Built-in WASM-compatible functions
//!
//! Native Rust implementations of common stream processing functions.
//! These match the WASM function interface but execute as optimized
//! native code, providing zero-overhead transforms for common patterns.

use crate::error::{Result, WasmError};
use crate::{TransformInput, TransformOutput, TransformResult};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::debug;

/// Trait for built-in functions that can process records
pub trait BuiltinFunction: Send + Sync {
    /// Name of the built-in function
    fn name(&self) -> &str;

    /// Description of what this function does
    fn description(&self) -> &str;

    /// Execute the function on an input record
    fn execute(&self, input: &TransformInput) -> Result<TransformOutput>;
}

// ─── json_filter ────────────────────────────────────────────────────────────

/// Filter JSON records by field value
///
/// Configuration:
/// - `field`: JSON field path (dot-separated, e.g. "user.status")
/// - `value`: Expected value (exact match)
/// - `operator`: Comparison operator: "eq", "neq", "contains", "exists" (default: "eq")
#[derive(Debug, Clone)]
pub struct JsonFilter {
    pub field: String,
    pub value: String,
    pub operator: FilterOperator,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterOperator {
    Eq,
    Neq,
    Contains,
    Exists,
}

impl JsonFilter {
    pub fn new(field: &str, value: &str, operator: FilterOperator) -> Self {
        Self {
            field: field.to_string(),
            value: value.to_string(),
            operator,
        }
    }

    pub fn from_config(config: &HashMap<String, String>) -> Result<Self> {
        let field = config
            .get("field")
            .ok_or_else(|| WasmError::Configuration("json_filter requires 'field'".into()))?
            .clone();
        let value = config.get("value").cloned().unwrap_or_default();
        let operator = match config.get("operator").map(|s| s.as_str()) {
            Some("neq") => FilterOperator::Neq,
            Some("contains") => FilterOperator::Contains,
            Some("exists") => FilterOperator::Exists,
            _ => FilterOperator::Eq,
        };
        Ok(Self {
            field,
            value,
            operator,
        })
    }
}

impl BuiltinFunction for JsonFilter {
    fn name(&self) -> &str {
        "json_filter"
    }

    fn description(&self) -> &str {
        "Filter JSON records by field value"
    }

    fn execute(&self, input: &TransformInput) -> Result<TransformOutput> {
        let json: serde_json::Value = serde_json::from_slice(&input.value)
            .map_err(|e| WasmError::Wasm(format!("Invalid JSON input: {}", e)))?;

        let field_value = resolve_json_path(&json, &self.field);

        let matches = match self.operator {
            FilterOperator::Eq => field_value
                .map(|v| value_to_string(v) == self.value)
                .unwrap_or(false),
            FilterOperator::Neq => field_value
                .map(|v| value_to_string(v) != self.value)
                .unwrap_or(true),
            FilterOperator::Contains => field_value
                .map(|v| value_to_string(v).contains(&self.value))
                .unwrap_or(false),
            FilterOperator::Exists => field_value.is_some(),
        };

        if matches {
            debug!(field = %self.field, "json_filter: record passed");
            Ok(TransformOutput::Pass(input.clone()))
        } else {
            debug!(field = %self.field, "json_filter: record dropped");
            Ok(TransformOutput::Drop)
        }
    }
}

// ─── json_transform ─────────────────────────────────────────────────────────

/// Transform JSON fields: rename, add, or remove fields
///
/// Configuration:
/// - `rename`: Comma-separated list of "old_name:new_name" pairs
/// - `add`: Comma-separated list of "field:value" pairs
/// - `remove`: Comma-separated list of field names to remove
#[derive(Debug, Clone)]
pub struct JsonTransform {
    pub renames: Vec<(String, String)>,
    pub additions: Vec<(String, String)>,
    pub removals: Vec<String>,
}

impl JsonTransform {
    pub fn from_config(config: &HashMap<String, String>) -> Result<Self> {
        let renames = config
            .get("rename")
            .map(|s| {
                s.split(',')
                    .filter_map(|pair| {
                        let parts: Vec<&str> = pair.trim().splitn(2, ':').collect();
                        if parts.len() == 2 {
                            Some((parts[0].to_string(), parts[1].to_string()))
                        } else {
                            None
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();

        let additions = config
            .get("add")
            .map(|s| {
                s.split(',')
                    .filter_map(|pair| {
                        let parts: Vec<&str> = pair.trim().splitn(2, ':').collect();
                        if parts.len() == 2 {
                            Some((parts[0].to_string(), parts[1].to_string()))
                        } else {
                            None
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();

        let removals = config
            .get("remove")
            .map(|s| s.split(',').map(|f| f.trim().to_string()).collect())
            .unwrap_or_default();

        Ok(Self {
            renames,
            additions,
            removals,
        })
    }
}

impl BuiltinFunction for JsonTransform {
    fn name(&self) -> &str {
        "json_transform"
    }

    fn description(&self) -> &str {
        "Transform JSON fields (rename, add, remove)"
    }

    fn execute(&self, input: &TransformInput) -> Result<TransformOutput> {
        let mut json: serde_json::Value = serde_json::from_slice(&input.value)
            .map_err(|e| WasmError::Wasm(format!("Invalid JSON input: {}", e)))?;

        if let Some(obj) = json.as_object_mut() {
            // Remove fields
            for field in &self.removals {
                obj.remove(field);
            }

            // Rename fields
            for (old_name, new_name) in &self.renames {
                if let Some(value) = obj.remove(old_name) {
                    obj.insert(new_name.clone(), value);
                }
            }

            // Add fields
            for (field, value) in &self.additions {
                obj.insert(
                    field.clone(),
                    serde_json::Value::String(value.clone()),
                );
            }
        }

        let output_bytes = serde_json::to_vec(&json)
            .map_err(|e| WasmError::Wasm(format!("Failed to serialize JSON: {}", e)))?;

        Ok(TransformOutput::Transformed(vec![TransformResult {
            key: input.key.clone(),
            value: output_bytes,
            headers: input.headers.clone(),
        }]))
    }
}

// ─── json_flatten ───────────────────────────────────────────────────────────

/// Flatten nested JSON into a single-level object with dot-separated keys
///
/// Configuration:
/// - `separator`: Separator for nested keys (default: ".")
/// - `max_depth`: Maximum nesting depth to flatten (default: 10)
#[derive(Debug, Clone)]
pub struct JsonFlatten {
    pub separator: String,
    pub max_depth: usize,
}

impl JsonFlatten {
    pub fn from_config(config: &HashMap<String, String>) -> Result<Self> {
        let separator = config
            .get("separator")
            .cloned()
            .unwrap_or_else(|| ".".to_string());
        let max_depth = config
            .get("max_depth")
            .and_then(|s| s.parse().ok())
            .unwrap_or(10);
        Ok(Self {
            separator,
            max_depth,
        })
    }
}

impl BuiltinFunction for JsonFlatten {
    fn name(&self) -> &str {
        "json_flatten"
    }

    fn description(&self) -> &str {
        "Flatten nested JSON into single-level object"
    }

    fn execute(&self, input: &TransformInput) -> Result<TransformOutput> {
        let json: serde_json::Value = serde_json::from_slice(&input.value)
            .map_err(|e| WasmError::Wasm(format!("Invalid JSON input: {}", e)))?;

        let mut flat = serde_json::Map::new();
        flatten_value(&json, "", &self.separator, self.max_depth, 0, &mut flat);

        let output_bytes = serde_json::to_vec(&serde_json::Value::Object(flat))
            .map_err(|e| WasmError::Wasm(format!("Failed to serialize JSON: {}", e)))?;

        Ok(TransformOutput::Transformed(vec![TransformResult {
            key: input.key.clone(),
            value: output_bytes,
            headers: input.headers.clone(),
        }]))
    }
}

// ─── timestamp_extract ──────────────────────────────────────────────────────

/// Extract a timestamp from a JSON field and set it as the record timestamp
///
/// Configuration:
/// - `field`: JSON field path containing the timestamp
/// - `format`: Timestamp format: "epoch_ms", "epoch_s", "iso8601" (default: "epoch_ms")
#[derive(Debug, Clone)]
pub struct TimestampExtract {
    pub field: String,
    pub format: TimestampFormat,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimestampFormat {
    EpochMs,
    EpochS,
    Iso8601,
}

impl TimestampExtract {
    pub fn from_config(config: &HashMap<String, String>) -> Result<Self> {
        let field = config
            .get("field")
            .ok_or_else(|| {
                WasmError::Configuration("timestamp_extract requires 'field'".into())
            })?
            .clone();
        let format = match config.get("format").map(|s| s.as_str()) {
            Some("epoch_s") => TimestampFormat::EpochS,
            Some("iso8601") => TimestampFormat::Iso8601,
            _ => TimestampFormat::EpochMs,
        };
        Ok(Self { field, format })
    }
}

impl BuiltinFunction for TimestampExtract {
    fn name(&self) -> &str {
        "timestamp_extract"
    }

    fn description(&self) -> &str {
        "Extract timestamp from record field"
    }

    fn execute(&self, input: &TransformInput) -> Result<TransformOutput> {
        let json: serde_json::Value = serde_json::from_slice(&input.value)
            .map_err(|e| WasmError::Wasm(format!("Invalid JSON input: {}", e)))?;

        let field_value = resolve_json_path(&json, &self.field)
            .ok_or_else(|| WasmError::Wasm(format!("Field '{}' not found", self.field)))?;

        let timestamp_ms = match self.format {
            TimestampFormat::EpochMs => field_value
                .as_i64()
                .ok_or_else(|| WasmError::Wasm("Expected numeric timestamp".into()))?,
            TimestampFormat::EpochS => {
                let secs = field_value
                    .as_i64()
                    .ok_or_else(|| WasmError::Wasm("Expected numeric timestamp".into()))?;
                secs * 1000
            }
            TimestampFormat::Iso8601 => {
                let s = field_value
                    .as_str()
                    .ok_or_else(|| WasmError::Wasm("Expected string timestamp".into()))?;
                chrono::DateTime::parse_from_rfc3339(s)
                    .map_err(|e| WasmError::Wasm(format!("Invalid ISO 8601 timestamp: {}", e)))?
                    .timestamp_millis()
            }
        };

        // Add extracted timestamp as a header
        let mut headers = input.headers.clone();
        headers.insert(
            "x-extracted-timestamp".to_string(),
            timestamp_ms.to_string().into_bytes(),
        );

        Ok(TransformOutput::Transformed(vec![TransformResult {
            key: input.key.clone(),
            value: input.value.clone(),
            headers,
        }]))
    }
}

// ─── field_mask ─────────────────────────────────────────────────────────────

/// Mask PII fields (email, phone, etc.) in JSON records
///
/// Configuration:
/// - `fields`: Comma-separated list of field names to mask
/// - `strategy`: Masking strategy: "redact", "hash", "partial" (default: "redact")
#[derive(Debug, Clone)]
pub struct FieldMask {
    pub fields: Vec<String>,
    pub strategy: MaskStrategy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaskStrategy {
    /// Replace with "***REDACTED***"
    Redact,
    /// Replace with a deterministic hash
    Hash,
    /// Partially mask (e.g., "j***@example.com")
    Partial,
}

impl FieldMask {
    pub fn from_config(config: &HashMap<String, String>) -> Result<Self> {
        let fields = config
            .get("fields")
            .ok_or_else(|| WasmError::Configuration("field_mask requires 'fields'".into()))?
            .split(',')
            .map(|f| f.trim().to_string())
            .collect();
        let strategy = match config.get("strategy").map(|s| s.as_str()) {
            Some("hash") => MaskStrategy::Hash,
            Some("partial") => MaskStrategy::Partial,
            _ => MaskStrategy::Redact,
        };
        Ok(Self { fields, strategy })
    }
}

impl BuiltinFunction for FieldMask {
    fn name(&self) -> &str {
        "field_mask"
    }

    fn description(&self) -> &str {
        "Mask PII fields (email, phone, etc.)"
    }

    fn execute(&self, input: &TransformInput) -> Result<TransformOutput> {
        let mut json: serde_json::Value = serde_json::from_slice(&input.value)
            .map_err(|e| WasmError::Wasm(format!("Invalid JSON input: {}", e)))?;

        if let Some(obj) = json.as_object_mut() {
            for field in &self.fields {
                if let Some(value) = obj.get(field) {
                    let masked = mask_value(value, &self.strategy);
                    obj.insert(field.clone(), serde_json::Value::String(masked));
                }
            }
        }

        let output_bytes = serde_json::to_vec(&json)
            .map_err(|e| WasmError::Wasm(format!("Failed to serialize JSON: {}", e)))?;

        Ok(TransformOutput::Transformed(vec![TransformResult {
            key: input.key.clone(),
            value: output_bytes,
            headers: input.headers.clone(),
        }]))
    }
}

// ─── Builtin registry ───────────────────────────────────────────────────────

/// Information about a built-in function
#[derive(Debug, Clone, Serialize)]
pub struct BuiltinInfo {
    pub name: String,
    pub description: String,
    pub config_keys: Vec<String>,
}

/// List all available built-in functions
pub fn list_builtins() -> Vec<BuiltinInfo> {
    vec![
        BuiltinInfo {
            name: "json_filter".to_string(),
            description: "Filter JSON records by field value".to_string(),
            config_keys: vec![
                "field".to_string(),
                "value".to_string(),
                "operator".to_string(),
            ],
        },
        BuiltinInfo {
            name: "json_transform".to_string(),
            description: "Transform JSON fields (rename, add, remove)".to_string(),
            config_keys: vec![
                "rename".to_string(),
                "add".to_string(),
                "remove".to_string(),
            ],
        },
        BuiltinInfo {
            name: "json_flatten".to_string(),
            description: "Flatten nested JSON into single-level object".to_string(),
            config_keys: vec!["separator".to_string(), "max_depth".to_string()],
        },
        BuiltinInfo {
            name: "timestamp_extract".to_string(),
            description: "Extract timestamp from record field".to_string(),
            config_keys: vec!["field".to_string(), "format".to_string()],
        },
        BuiltinInfo {
            name: "field_mask".to_string(),
            description: "Mask PII fields (email, phone, etc.)".to_string(),
            config_keys: vec!["fields".to_string(), "strategy".to_string()],
        },
    ]
}

/// Create a built-in function from name and config
pub fn create_builtin(
    name: &str,
    config: &HashMap<String, String>,
) -> Result<Box<dyn BuiltinFunction>> {
    match name {
        "json_filter" => Ok(Box::new(JsonFilter::from_config(config)?)),
        "json_transform" => Ok(Box::new(JsonTransform::from_config(config)?)),
        "json_flatten" => Ok(Box::new(JsonFlatten::from_config(config)?)),
        "timestamp_extract" => Ok(Box::new(TimestampExtract::from_config(config)?)),
        "field_mask" => Ok(Box::new(FieldMask::from_config(config)?)),
        _ => Err(WasmError::Validation(format!(
            "Unknown built-in function: '{}'",
            name
        ))),
    }
}

// ─── Helper functions ───────────────────────────────────────────────────────

/// Resolve a dot-separated path into a JSON value
fn resolve_json_path<'a>(json: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    let mut current = json;
    for key in path.split('.') {
        current = current.get(key)?;
    }
    Some(current)
}

/// Convert a JSON value to a string for comparison
fn value_to_string(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Null => "null".to_string(),
        other => other.to_string(),
    }
}

/// Recursively flatten a JSON value
fn flatten_value(
    value: &serde_json::Value,
    prefix: &str,
    separator: &str,
    max_depth: usize,
    current_depth: usize,
    output: &mut serde_json::Map<String, serde_json::Value>,
) {
    match value {
        serde_json::Value::Object(map) if current_depth < max_depth => {
            for (key, val) in map {
                let new_prefix = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{}{}{}", prefix, separator, key)
                };
                flatten_value(val, &new_prefix, separator, max_depth, current_depth + 1, output);
            }
        }
        _ => {
            output.insert(prefix.to_string(), value.clone());
        }
    }
}

/// Mask a JSON value based on strategy
fn mask_value(value: &serde_json::Value, strategy: &MaskStrategy) -> String {
    let original = value_to_string(value);
    match strategy {
        MaskStrategy::Redact => "***REDACTED***".to_string(),
        MaskStrategy::Hash => {
            // Simple deterministic hash (not cryptographic)
            let hash: u64 = original.bytes().fold(0u64, |acc, b| {
                acc.wrapping_mul(31).wrapping_add(b as u64)
            });
            format!("masked_{:016x}", hash)
        }
        MaskStrategy::Partial => {
            if original.contains('@') {
                // Email: show first char + domain
                let parts: Vec<&str> = original.splitn(2, '@').collect();
                if parts.len() == 2 && !parts[0].is_empty() {
                    format!("{}***@{}", &parts[0][..1], parts[1])
                } else {
                    "***REDACTED***".to_string()
                }
            } else if original.len() > 4 {
                // Show last 4 characters
                format!("***{}", &original[original.len() - 4..])
            } else {
                "***REDACTED***".to_string()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_json_input(json: &str) -> TransformInput {
        TransformInput {
            topic: "test-topic".to_string(),
            partition: 0,
            key: None,
            value: json.as_bytes().to_vec(),
            headers: HashMap::new(),
            timestamp_ms: 0,
        }
    }

    // ── json_filter tests ───────────────────────────────────────────────

    #[test]
    fn test_json_filter_eq_match() {
        let filter = JsonFilter::new("status", "active", FilterOperator::Eq);
        let input = make_json_input(r#"{"status": "active", "name": "test"}"#);
        let output = filter.execute(&input).unwrap();
        assert!(matches!(output, TransformOutput::Pass(_)));
    }

    #[test]
    fn test_json_filter_eq_no_match() {
        let filter = JsonFilter::new("status", "active", FilterOperator::Eq);
        let input = make_json_input(r#"{"status": "inactive"}"#);
        let output = filter.execute(&input).unwrap();
        assert!(matches!(output, TransformOutput::Drop));
    }

    #[test]
    fn test_json_filter_contains() {
        let filter = JsonFilter::new("email", "example.com", FilterOperator::Contains);
        let input = make_json_input(r#"{"email": "user@example.com"}"#);
        let output = filter.execute(&input).unwrap();
        assert!(matches!(output, TransformOutput::Pass(_)));
    }

    #[test]
    fn test_json_filter_exists() {
        let filter = JsonFilter::new("name", "", FilterOperator::Exists);
        let input = make_json_input(r#"{"name": "Alice"}"#);
        let output = filter.execute(&input).unwrap();
        assert!(matches!(output, TransformOutput::Pass(_)));
    }

    #[test]
    fn test_json_filter_nested_path() {
        let filter = JsonFilter::new("user.status", "active", FilterOperator::Eq);
        let input = make_json_input(r#"{"user": {"status": "active"}}"#);
        let output = filter.execute(&input).unwrap();
        assert!(matches!(output, TransformOutput::Pass(_)));
    }

    // ── json_transform tests ────────────────────────────────────────────

    #[test]
    fn test_json_transform_rename() {
        let mut config = HashMap::new();
        config.insert("rename".to_string(), "old_name:new_name".to_string());
        let transform = JsonTransform::from_config(&config).unwrap();

        let input = make_json_input(r#"{"old_name": "value"}"#);
        let output = transform.execute(&input).unwrap();

        if let TransformOutput::Transformed(results) = output {
            let json: serde_json::Value = serde_json::from_slice(&results[0].value).unwrap();
            assert!(json.get("new_name").is_some());
            assert!(json.get("old_name").is_none());
        } else {
            panic!("Expected Transformed output");
        }
    }

    #[test]
    fn test_json_transform_add_and_remove() {
        let mut config = HashMap::new();
        config.insert("add".to_string(), "env:production".to_string());
        config.insert("remove".to_string(), "debug".to_string());
        let transform = JsonTransform::from_config(&config).unwrap();

        let input = make_json_input(r#"{"debug": true, "name": "test"}"#);
        let output = transform.execute(&input).unwrap();

        if let TransformOutput::Transformed(results) = output {
            let json: serde_json::Value = serde_json::from_slice(&results[0].value).unwrap();
            assert_eq!(json["env"], "production");
            assert!(json.get("debug").is_none());
        } else {
            panic!("Expected Transformed output");
        }
    }

    // ── json_flatten tests ──────────────────────────────────────────────

    #[test]
    fn test_json_flatten() {
        let flatten = JsonFlatten {
            separator: ".".to_string(),
            max_depth: 10,
        };
        let input = make_json_input(r#"{"user": {"name": "Alice", "address": {"city": "NYC"}}}"#);
        let output = flatten.execute(&input).unwrap();

        if let TransformOutput::Transformed(results) = output {
            let json: serde_json::Value = serde_json::from_slice(&results[0].value).unwrap();
            assert_eq!(json["user.name"], "Alice");
            assert_eq!(json["user.address.city"], "NYC");
        } else {
            panic!("Expected Transformed output");
        }
    }

    // ── timestamp_extract tests ─────────────────────────────────────────

    #[test]
    fn test_timestamp_extract_epoch_ms() {
        let extract = TimestampExtract {
            field: "ts".to_string(),
            format: TimestampFormat::EpochMs,
        };
        let input = make_json_input(r#"{"ts": 1700000000000, "data": "test"}"#);
        let output = extract.execute(&input).unwrap();

        if let TransformOutput::Transformed(results) = output {
            assert!(results[0].headers.contains_key("x-extracted-timestamp"));
        } else {
            panic!("Expected Transformed output");
        }
    }

    #[test]
    fn test_timestamp_extract_iso8601() {
        let extract = TimestampExtract {
            field: "created_at".to_string(),
            format: TimestampFormat::Iso8601,
        };
        let input =
            make_json_input(r#"{"created_at": "2024-01-15T12:00:00Z", "data": "test"}"#);
        let output = extract.execute(&input).unwrap();
        assert!(matches!(output, TransformOutput::Transformed(_)));
    }

    // ── field_mask tests ────────────────────────────────────────────────

    #[test]
    fn test_field_mask_redact() {
        let mask = FieldMask {
            fields: vec!["email".to_string(), "phone".to_string()],
            strategy: MaskStrategy::Redact,
        };
        let input =
            make_json_input(r#"{"email": "alice@example.com", "phone": "555-1234", "name": "Alice"}"#);
        let output = mask.execute(&input).unwrap();

        if let TransformOutput::Transformed(results) = output {
            let json: serde_json::Value = serde_json::from_slice(&results[0].value).unwrap();
            assert_eq!(json["email"], "***REDACTED***");
            assert_eq!(json["phone"], "***REDACTED***");
            assert_eq!(json["name"], "Alice");
        } else {
            panic!("Expected Transformed output");
        }
    }

    #[test]
    fn test_field_mask_partial_email() {
        let mask = FieldMask {
            fields: vec!["email".to_string()],
            strategy: MaskStrategy::Partial,
        };
        let input = make_json_input(r#"{"email": "alice@example.com"}"#);
        let output = mask.execute(&input).unwrap();

        if let TransformOutput::Transformed(results) = output {
            let json: serde_json::Value = serde_json::from_slice(&results[0].value).unwrap();
            assert_eq!(json["email"], "a***@example.com");
        } else {
            panic!("Expected Transformed output");
        }
    }

    #[test]
    fn test_field_mask_hash() {
        let mask = FieldMask {
            fields: vec!["ssn".to_string()],
            strategy: MaskStrategy::Hash,
        };
        let input = make_json_input(r#"{"ssn": "123-45-6789"}"#);
        let output = mask.execute(&input).unwrap();

        if let TransformOutput::Transformed(results) = output {
            let json: serde_json::Value = serde_json::from_slice(&results[0].value).unwrap();
            let masked = json["ssn"].as_str().unwrap();
            assert!(masked.starts_with("masked_"));
        } else {
            panic!("Expected Transformed output");
        }
    }

    // ── create_builtin tests ────────────────────────────────────────────

    #[test]
    fn test_create_builtin_known() {
        let mut config = HashMap::new();
        config.insert("field".to_string(), "status".to_string());
        config.insert("value".to_string(), "active".to_string());

        let func = create_builtin("json_filter", &config).unwrap();
        assert_eq!(func.name(), "json_filter");
    }

    #[test]
    fn test_create_builtin_unknown() {
        assert!(create_builtin("unknown_func", &HashMap::new()).is_err());
    }

    #[test]
    fn test_list_builtins() {
        let builtins = list_builtins();
        assert_eq!(builtins.len(), 5);
        assert!(builtins.iter().any(|b| b.name == "json_filter"));
        assert!(builtins.iter().any(|b| b.name == "field_mask"));
    }

    // ── helper tests ────────────────────────────────────────────────────

    #[test]
    fn test_resolve_json_path() {
        let json: serde_json::Value =
            serde_json::from_str(r#"{"a": {"b": {"c": 42}}}"#).unwrap();
        let val = resolve_json_path(&json, "a.b.c").unwrap();
        assert_eq!(val, &serde_json::json!(42));
    }

    #[test]
    fn test_mask_value_redact() {
        let v = serde_json::json!("secret");
        assert_eq!(mask_value(&v, &MaskStrategy::Redact), "***REDACTED***");
    }

    #[test]
    fn test_mask_value_partial_phone() {
        let v = serde_json::json!("555-123-4567");
        let masked = mask_value(&v, &MaskStrategy::Partial);
        assert_eq!(masked, "***4567");
    }
}
