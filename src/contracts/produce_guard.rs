//! Producer append validation hook (M4 P1).
//!
//! When a topic has an active contract, this module is called from the
//! produce path; invalid records are rejected with a structured
//! `ContractError` carrying actionable diagnostics.
//!
//! Stability tier: **Experimental**. Not yet wired into
//! `src/server/produce.rs` — wire when M4 P1 lands.

use serde_json::Value;

#[derive(Debug, Clone)]
pub struct ContractRejection {
    pub topic: String,
    pub partition: i32,
    pub schema_id: Option<u32>,
    pub field_path: String,
    pub expected: String,
    pub actual: String,
}

impl std::fmt::Display for ContractRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "contract violation on {}/p{}: field `{}` expected {}, got {}{}",
            self.topic,
            self.partition,
            self.field_path,
            self.expected,
            self.actual,
            self.schema_id
                .map(|id| format!(" (schema_id={id})"))
                .unwrap_or_default()
        )
    }
}

impl std::error::Error for ContractRejection {}

/// Validates a record against an active contract. Returns `Ok(())` on success
/// or `Err(ContractRejection)` with all info needed for an actionable error
/// message returned to the producer.
///
/// The current implementation supports JSON-encoded payloads and walks
/// `$.foo.bar`-style paths against `Contract::assertions`. Empty contracts
/// (zero assertions) accept everything.
pub fn validate_record(
    topic: &str,
    partition: i32,
    value: &[u8],
    contract: &Contract,
) -> Result<(), ContractRejection> {
    if contract.assertions.is_empty() {
        return Ok(());
    }

    let parsed: Value = serde_json::from_slice(value).map_err(|e| ContractRejection {
        topic: topic.to_string(),
        partition,
        schema_id: contract.schema_id,
        field_path: "$".to_string(),
        expected: "valid JSON object".to_string(),
        actual: format!("parse error: {e}"),
    })?;

    for assertion in &contract.assertions {
        let resolved = resolve_path(&parsed, &assertion.path);
        match (resolved, &assertion.expected) {
            (Some(v), expected) if value_matches(v, expected) => continue,
            (Some(v), expected) => {
                return Err(ContractRejection {
                    topic: topic.to_string(),
                    partition,
                    schema_id: contract.schema_id,
                    field_path: assertion.path.clone(),
                    expected: expected.as_str().to_string(),
                    actual: type_name(v).to_string(),
                });
            }
            (None, expected) => {
                return Err(ContractRejection {
                    topic: topic.to_string(),
                    partition,
                    schema_id: contract.schema_id,
                    field_path: assertion.path.clone(),
                    expected: expected.as_str().to_string(),
                    actual: "missing".to_string(),
                });
            }
        }
    }
    Ok(())
}

fn resolve_path<'a>(root: &'a Value, path: &str) -> Option<&'a Value> {
    let trimmed = path
        .strip_prefix("$.")
        .or_else(|| path.strip_prefix('$'))
        .unwrap_or(path);
    if trimmed.is_empty() {
        return Some(root);
    }
    let mut cur = root;
    for segment in trimmed.split('.') {
        if segment.is_empty() {
            continue;
        }
        cur = cur.get(segment)?;
    }
    Some(cur)
}

fn value_matches(v: &Value, expected: &ExpectedType) -> bool {
    match expected {
        ExpectedType::Number => v.is_number(),
        ExpectedType::String => v.is_string(),
        ExpectedType::Bool => v.is_boolean(),
        ExpectedType::Object => v.is_object(),
        ExpectedType::Array => v.is_array(),
    }
}

fn type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Type a contract assertion expects at a given JSON path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpectedType {
    Number,
    String,
    Bool,
    Object,
    Array,
}

impl ExpectedType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ExpectedType::Number => "number",
            ExpectedType::String => "string",
            ExpectedType::Bool => "bool",
            ExpectedType::Object => "object",
            ExpectedType::Array => "array",
        }
    }
}

/// A single field-level assertion in a contract.
#[derive(Debug, Clone)]
pub struct FieldAssertion {
    /// JSONPath-lite. `$.foo.bar` or `foo.bar`.
    pub path: String,
    pub expected: ExpectedType,
}

/// Compiled contract — populated by `m4-p0-dsl` work.
#[derive(Debug, Clone, Default)]
pub struct Contract {
    pub topic: String,
    pub version: u32,
    pub schema_id: Option<u32>,
    pub assertions: Vec<FieldAssertion>,
}

impl Contract {
    pub fn new(topic: impl Into<String>, version: u32) -> Self {
        Self {
            topic: topic.into(),
            version,
            schema_id: None,
            assertions: Vec::new(),
        }
    }

    pub fn require(mut self, path: impl Into<String>, expected: ExpectedType) -> Self {
        self.assertions.push(FieldAssertion {
            path: path.into(),
            expected,
        });
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejection_display_is_actionable() {
        let r = ContractRejection {
            topic: "orders".into(),
            partition: 0,
            schema_id: Some(7),
            field_path: "$.amount".into(),
            expected: "number".into(),
            actual: "string".into(),
        };
        let msg = r.to_string();
        assert!(msg.contains("orders"));
        assert!(msg.contains("$.amount"));
        assert!(msg.contains("schema_id=7"));
    }

    #[test]
    fn empty_contract_accepts_anything() {
        let c = Contract::new("t", 1);
        assert!(validate_record("t", 0, b"{}", &c).is_ok());
        assert!(validate_record("t", 0, b"not json", &c).is_ok());
    }

    #[test]
    fn missing_required_field_is_rejected() {
        let c = Contract::new("orders", 1).require("$.amount", ExpectedType::Number);
        let err = validate_record("orders", 0, br#"{"id":1}"#, &c).unwrap_err();
        assert_eq!(err.field_path, "$.amount");
        assert_eq!(err.actual, "missing");
    }

    #[test]
    fn wrong_type_is_rejected_with_actual_type() {
        let c = Contract::new("orders", 1).require("$.amount", ExpectedType::Number);
        let err = validate_record("orders", 0, br#"{"amount":"twenty"}"#, &c).unwrap_err();
        assert_eq!(err.actual, "string");
        assert_eq!(err.expected, "number");
    }

    #[test]
    fn nested_path_resolves() {
        let c = Contract::new("events", 1).require("$.user.id", ExpectedType::String);
        assert!(validate_record("events", 0, br#"{"user":{"id":"u1"}}"#, &c).is_ok());
        assert!(validate_record("events", 0, br#"{"user":{}}"#, &c).is_err());
    }

    #[test]
    fn invalid_json_is_rejected_at_root() {
        let c = Contract::new("t", 1).require("$.x", ExpectedType::Number);
        let err = validate_record("t", 0, b"{not-json", &c).unwrap_err();
        assert_eq!(err.field_path, "$");
        assert!(err.actual.starts_with("parse error"));
    }

    #[test]
    fn multiple_assertions_all_must_pass() {
        let c = Contract::new("orders", 1)
            .require("$.amount", ExpectedType::Number)
            .require("$.currency", ExpectedType::String);
        assert!(validate_record("orders", 0, br#"{"amount":10,"currency":"USD"}"#, &c).is_ok());
        let err = validate_record("orders", 0, br#"{"amount":10,"currency":42}"#, &c).unwrap_err();
        assert_eq!(err.field_path, "$.currency");
    }
}
