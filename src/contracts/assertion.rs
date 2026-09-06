//! Assertion engine for validating stream messages against contracts.

use super::definition::{ContractField, ContractInvariant, FieldType, StreamContract};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Result of evaluating assertions against a message stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssertionResult {
    /// Name of the assertion
    pub name: String,
    /// Whether the assertion passed
    pub passed: bool,
    /// Human-readable message
    pub message: String,
    /// Number of records checked
    pub records_checked: u64,
    /// Number of failures
    pub failures: u64,
    /// Detailed failure info (first N failures)
    pub failure_details: Vec<String>,
    /// `true` when the assertion could not be evaluated because the invariant
    /// is not supported by this engine (or not supported in this evaluation
    /// mode). Unsupported assertions are always reported as **failures** so a
    /// contract can never appear to pass on a check that was never run.
    #[serde(default)]
    pub unsupported: bool,
}

impl AssertionResult {
    /// Build a failing result for an invariant that this engine cannot evaluate.
    ///
    /// This deliberately reports `passed == false`: silently returning success
    /// for an unimplemented check would make contract enforcement dishonest.
    fn unsupported(name: impl Into<String>, reason: impl Into<String>, records: u64) -> Self {
        let reason = reason.into();
        Self {
            name: name.into(),
            passed: false,
            message: format!("Unsupported invariant: {reason}"),
            records_checked: records,
            failures: 1,
            failure_details: vec![reason],
            unsupported: true,
        }
    }
}

/// Types of assertions that can be evaluated.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AssertionType {
    /// Field exists and has the correct type
    FieldPresence,
    /// Field value matches a pattern
    FieldPattern,
    /// Message size is within bounds
    MessageSize,
    /// Required headers are present
    HeaderPresence,
    /// Ordering invariant holds
    Ordering,
    /// Uniqueness invariant holds
    Uniqueness,
}

/// Assertion on a specific field.
#[derive(Debug, Clone)]
pub struct FieldAssertion {
    pub field: ContractField,
    pub assertion_type: AssertionType,
}

/// A single assertion to validate.
#[derive(Debug, Clone)]
pub struct Assertion {
    pub name: String,
    pub assertion_type: AssertionType,
    pub description: String,
}

/// Engine for validating messages against stream contracts.
pub struct AssertionEngine {
    contract: StreamContract,
    max_failure_details: usize,
}

impl AssertionEngine {
    /// Create a new assertion engine for the given contract.
    pub fn new(contract: StreamContract) -> Self {
        Self {
            contract,
            max_failure_details: 10,
        }
    }

    /// Set the maximum number of failure details to retain.
    pub fn with_max_failures(mut self, max: usize) -> Self {
        self.max_failure_details = max;
        self
    }

    /// Validate a batch of JSON messages against the contract.
    pub fn validate_messages(&self, messages: &[serde_json::Value]) -> Vec<AssertionResult> {
        let mut results = Vec::new();

        // Check field presence and types
        for field in &self.contract.spec.fields {
            results.push(self.check_field(field, messages));
        }

        // Check invariants
        for invariant in &self.contract.spec.invariants {
            results.push(self.check_invariant(invariant, messages));
        }

        // Check max message size
        if let Some(max_size) = self.contract.spec.max_message_size {
            results.push(self.check_message_size(max_size, messages));
        }

        results
    }

    fn check_field(
        &self,
        field: &ContractField,
        messages: &[serde_json::Value],
    ) -> AssertionResult {
        let mut failures = Vec::new();
        let mut failure_count = 0u64;

        for (i, msg) in messages.iter().enumerate() {
            let value = Self::extract_field(msg, &field.name);

            if field.required && value.is_none() {
                failure_count += 1;
                if failures.len() < self.max_failure_details {
                    failures.push(format!(
                        "Message {}: missing required field '{}'",
                        i, field.name
                    ));
                }
                continue;
            }

            if let Some(val) = value {
                if !self.type_matches(val, &field.field_type) {
                    failure_count += 1;
                    if failures.len() < self.max_failure_details {
                        failures.push(format!(
                            "Message {}: field '{}' expected type {}, got {:?}",
                            i, field.name, field.field_type, val
                        ));
                    }
                }

                // Check pattern if specified
                if let Some(ref pattern) = field.pattern {
                    if let Some(s) = val.as_str() {
                        if let Ok(re) = regex::Regex::new(pattern) {
                            if !re.is_match(s) {
                                failure_count += 1;
                                if failures.len() < self.max_failure_details {
                                    failures.push(format!(
                                        "Message {}: field '{}' value '{}' doesn't match pattern '{}'",
                                        i, field.name, s, pattern
                                    ));
                                }
                            }
                        }
                    }
                }
            }
        }

        AssertionResult {
            name: format!("field:{}", field.name),
            passed: failure_count == 0,
            message: if failure_count == 0 {
                format!(
                    "Field '{}' valid in all {} messages",
                    field.name,
                    messages.len()
                )
            } else {
                format!(
                    "Field '{}' failed in {}/{} messages",
                    field.name,
                    failure_count,
                    messages.len()
                )
            },
            records_checked: messages.len() as u64,
            failures: failure_count,
            failure_details: failures,
            unsupported: false,
        }
    }

    fn check_invariant(
        &self,
        invariant: &ContractInvariant,
        messages: &[serde_json::Value],
    ) -> AssertionResult {
        match invariant {
            ContractInvariant::Ordering(ordering) => {
                let mut failures = Vec::new();
                let mut failure_count = 0u64;

                for window in messages.windows(2) {
                    let a = Self::extract_field(&window[0], &ordering.field);
                    let b = Self::extract_field(&window[1], &ordering.field);

                    if let (Some(a_val), Some(b_val)) = (a, b) {
                        let ordered = match &ordering.direction {
                            super::definition::OrderingDirection::Ascending => {
                                Self::compare_json_values(a_val, b_val) <= 0
                            }
                            super::definition::OrderingDirection::Descending => {
                                Self::compare_json_values(a_val, b_val) >= 0
                            }
                        };
                        if !ordered {
                            failure_count += 1;
                            if failures.len() < self.max_failure_details {
                                failures.push(format!(
                                    "Ordering violation on field '{}': {:?} -> {:?}",
                                    ordering.field, a_val, b_val
                                ));
                            }
                        }
                    }
                }

                AssertionResult {
                    name: format!("invariant:ordering:{}", ordering.field),
                    passed: failure_count == 0,
                    message: if failure_count == 0 {
                        format!("Ordering on '{}' holds", ordering.field)
                    } else {
                        format!(
                            "{} ordering violations on '{}'",
                            failure_count, ordering.field
                        )
                    },
                    records_checked: messages.len() as u64,
                    failures: failure_count,
                    failure_details: failures,
                    unsupported: false,
                }
            }
            ContractInvariant::Uniqueness(uniqueness) => {
                let mut seen: HashSet<String> = HashSet::new();
                let mut failures = Vec::new();
                let mut failure_count = 0u64;

                for (i, msg) in messages.iter().enumerate() {
                    let key: String = uniqueness
                        .fields
                        .iter()
                        .map(|f| {
                            Self::extract_field(msg, f)
                                .map(|v| v.to_string())
                                .unwrap_or_default()
                        })
                        .collect::<Vec<_>>()
                        .join("|");

                    if !seen.insert(key.clone()) {
                        failure_count += 1;
                        if failures.len() < self.max_failure_details {
                            failures.push(format!("Message {i}: duplicate key '{key}'"));
                        }
                    }
                }

                AssertionResult {
                    name: format!("invariant:uniqueness:{}", uniqueness.fields.join("+")),
                    passed: failure_count == 0,
                    message: if failure_count == 0 {
                        "Uniqueness constraint holds".to_string()
                    } else {
                        format!("{failure_count} uniqueness violations")
                    },
                    records_checked: messages.len() as u64,
                    failures: failure_count,
                    failure_details: failures,
                    unsupported: false,
                }
            }
            ContractInvariant::RateLimit { max_per_second } => AssertionResult::unsupported(
                "invariant:rate_limit",
                format!(
                    "rate limit (max {max_per_second}/s) cannot be evaluated in batch mode; \
                     messages carry no arrival timestamps here. Remove the invariant or run \
                     it through the streaming validator."
                ),
                messages.len() as u64,
            ),
            ContractInvariant::JsonSchema { schema: _ } => AssertionResult::unsupported(
                "invariant:json_schema",
                "JSON Schema validation is not implemented by the assertion engine",
                messages.len() as u64,
            ),
        }
    }

    fn check_message_size(
        &self,
        max_size: usize,
        messages: &[serde_json::Value],
    ) -> AssertionResult {
        let mut failures = Vec::new();
        let mut failure_count = 0u64;

        for (i, msg) in messages.iter().enumerate() {
            let size = msg.to_string().len();
            if size > max_size {
                failure_count += 1;
                if failures.len() < self.max_failure_details {
                    failures.push(format!("Message {i}: size {size} exceeds max {max_size}"));
                }
            }
        }

        AssertionResult {
            name: "message_size".to_string(),
            passed: failure_count == 0,
            message: if failure_count == 0 {
                format!("All messages within {max_size} byte limit")
            } else {
                format!("{failure_count} messages exceed {max_size} byte limit")
            },
            records_checked: messages.len() as u64,
            failures: failure_count,
            failure_details: failures,
            unsupported: false,
        }
    }

    fn extract_field<'a>(
        value: &'a serde_json::Value,
        path: &str,
    ) -> Option<&'a serde_json::Value> {
        let parts: Vec<&str> = path.split('.').collect();
        let mut current = value;
        for part in parts {
            current = current.get(part)?;
        }
        Some(current)
    }

    fn type_matches(&self, value: &serde_json::Value, expected: &FieldType) -> bool {
        match expected {
            FieldType::String => value.is_string(),
            FieldType::Integer => value.is_i64() || value.is_u64(),
            FieldType::Float => value.is_f64(),
            FieldType::Boolean => value.is_boolean(),
            FieldType::Array => value.is_array(),
            FieldType::Object => value.is_object(),
            FieldType::Null => value.is_null(),
            FieldType::Any => true,
        }
    }

    fn compare_json_values(a: &serde_json::Value, b: &serde_json::Value) -> i32 {
        match (a.as_f64(), b.as_f64()) {
            (Some(a_num), Some(b_num)) => a_num.partial_cmp(&b_num).map(|o| o as i32).unwrap_or(0),
            _ => {
                let a_str = a.to_string();
                let b_str = b.to_string();
                a_str.cmp(&b_str) as i32
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::definition::*;
    use serde_json::json;

    #[test]
    fn test_field_presence_check() {
        let contract = StreamContract::new("test", "events")
            .with_field("user_id", FieldType::String)
            .with_field("count", FieldType::Integer);

        let engine = AssertionEngine::new(contract);
        let messages = vec![
            json!({"user_id": "abc", "count": 5}),
            json!({"user_id": "def", "count": 10}),
        ];

        let results = engine.validate_messages(&messages);
        assert!(results.iter().all(|r| r.passed));
    }

    #[test]
    fn test_field_type_mismatch() {
        let contract =
            StreamContract::new("test", "events").with_field("count", FieldType::Integer);

        let engine = AssertionEngine::new(contract);
        let messages = vec![json!({"count": "not-a-number"})];

        let results = engine.validate_messages(&messages);
        assert!(!results[0].passed);
    }

    #[test]
    fn test_uniqueness_invariant() {
        let contract = StreamContract::new("test", "events").with_invariant(
            ContractInvariant::Uniqueness(UniquenessInvariant {
                fields: vec!["id".to_string()],
            }),
        );

        let engine = AssertionEngine::new(contract);
        let messages = vec![
            json!({"id": "a"}),
            json!({"id": "b"}),
            json!({"id": "a"}), // duplicate
        ];

        let results = engine.validate_messages(&messages);
        let uniqueness = results
            .iter()
            .find(|r| r.name.contains("uniqueness"))
            .unwrap();
        assert!(!uniqueness.passed);
        assert_eq!(uniqueness.failures, 1);
    }

    /// Regression: an invariant the engine cannot evaluate must NOT be reported
    /// as passing. Previously `RateLimit` returned `passed: true` with a
    /// "skipped" message, which silently green-lit unenforced contracts.
    #[test]
    fn test_rate_limit_invariant_reports_unsupported_failure() {
        let contract =
            StreamContract::new("test", "events").with_invariant(ContractInvariant::RateLimit {
                max_per_second: 100.0,
            });

        let engine = AssertionEngine::new(contract);
        let results = engine.validate_messages(&[json!({"id": "a"})]);

        let rate = results
            .iter()
            .find(|r| r.name == "invariant:rate_limit")
            .expect("rate limit assertion must be reported");
        assert!(!rate.passed, "unsupported invariant must not pass");
        assert!(rate.unsupported, "must be flagged unsupported");
        assert_eq!(rate.failures, 1);
        assert!(rate.message.starts_with("Unsupported invariant:"));
    }

    /// Regression: JSON Schema invariants are not implemented; they must fail
    /// closed rather than report success.
    #[test]
    fn test_json_schema_invariant_reports_unsupported_failure() {
        let contract =
            StreamContract::new("test", "events").with_invariant(ContractInvariant::JsonSchema {
                schema: "{\"type\":\"object\"}".to_string(),
            });

        let engine = AssertionEngine::new(contract);
        let results = engine.validate_messages(&[json!({"id": "a"})]);

        let schema = results
            .iter()
            .find(|r| r.name == "invariant:json_schema")
            .expect("json schema assertion must be reported");
        assert!(!schema.passed, "unsupported invariant must not pass");
        assert!(schema.unsupported);
        assert_eq!(schema.failures, 1);
    }

    /// Supported invariants must never be flagged unsupported.
    #[test]
    fn test_supported_invariants_are_not_flagged_unsupported() {
        let contract = StreamContract::new("test", "events")
            .with_field("id", FieldType::String)
            .with_invariant(ContractInvariant::Uniqueness(UniquenessInvariant {
                fields: vec!["id".to_string()],
            }));

        let engine = AssertionEngine::new(contract);
        let results = engine.validate_messages(&[json!({"id": "a"}), json!({"id": "b"})]);

        assert!(results.iter().all(|r| !r.unsupported));
        assert!(results.iter().all(|r| r.passed));
    }

    /// Older persisted reports have no `unsupported` field; it must default to
    /// `false` so deserialization stays backward compatible.
    #[test]
    fn test_assertion_result_deserializes_without_unsupported_field() {
        let legacy = r#"{
            "name": "field:id",
            "passed": true,
            "message": "ok",
            "records_checked": 1,
            "failures": 0,
            "failure_details": []
        }"#;
        let parsed: AssertionResult = serde_json::from_str(legacy).unwrap();
        assert!(!parsed.unsupported);
    }
}
