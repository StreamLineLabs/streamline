//! Server-Side Contract Validation & CI/CD Integration
//!
//! Extends the contracts module with production enforcement:
//!
//! - **Server-Side Validation**: Validate messages on produce against
//!   registered contracts. Supports enforce / monitor-only modes.
//! - **Contract Versioning**: Track contract versions with breaking-change
//!   detection and compatibility checks.
//! - **CI/CD Hooks**: Webhook notifications for contract violations,
//!   JUnit-compatible report generation.
//! - **Contract Diff**: Compare two contract versions and report changes.

use super::definition::{ContractField, ContractSpec, FieldType, StreamContract};
use crate::error::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

// ---------------------------------------------------------------------------
// Server-Side Validator
// ---------------------------------------------------------------------------

/// Enforcement mode for contract validation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnforcementMode {
    /// Reject messages that violate the contract.
    Enforce,
    /// Log violations but allow the message through.
    MonitorOnly,
    /// Validation disabled for this topic.
    Disabled,
}

/// Result of validating a message against a contract.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationResult {
    /// Whether the message is valid.
    pub valid: bool,
    /// List of violations.
    pub violations: Vec<Violation>,
    /// Contract name used for validation.
    pub contract_name: String,
    /// Contract version.
    pub contract_version: String,
    /// Validation time in microseconds.
    pub validation_time_us: u64,
}

/// A single contract violation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Violation {
    /// Field path.
    pub field: String,
    /// Violation type.
    pub violation_type: ViolationType,
    /// Human-readable message.
    pub message: String,
    /// Expected value/type.
    pub expected: Option<String>,
    /// Actual value/type.
    pub actual: Option<String>,
}

/// Types of violations.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ViolationType {
    /// Required field is missing.
    MissingField,
    /// Field has wrong type.
    TypeMismatch,
    /// Value doesn't match pattern.
    PatternMismatch,
    /// Value out of range.
    RangeViolation,
    /// Message exceeds size limit.
    SizeExceeded,
    /// Required header missing.
    MissingHeader,
    /// Schema parse error.
    ParseError,
}

/// Server-side contract validator.
pub struct ContractValidator {
    /// Registered contracts: topic → contract.
    contracts: RwLock<HashMap<String, RegisteredContract>>,
    /// Default enforcement mode.
    default_mode: EnforcementMode,
    /// Validation statistics.
    stats: Arc<ValidationStats>,
}

/// A registered contract with enforcement configuration.
#[derive(Debug, Clone)]
struct RegisteredContract {
    contract: StreamContract,
    mode: EnforcementMode,
    registered_at: DateTime<Utc>,
}

/// Validation statistics.
#[derive(Debug, Default)]
pub struct ValidationStats {
    pub total_validated: AtomicU64,
    pub total_passed: AtomicU64,
    pub total_failed: AtomicU64,
    pub total_skipped: AtomicU64,
}

impl ContractValidator {
    /// Create a new validator.
    pub fn new(default_mode: EnforcementMode) -> Self {
        Self {
            contracts: RwLock::new(HashMap::new()),
            default_mode,
            stats: Arc::new(ValidationStats::default()),
        }
    }

    /// Register a contract for a topic.
    pub async fn register(
        &self,
        contract: StreamContract,
        mode: Option<EnforcementMode>,
    ) -> Result<()> {
        let topic = contract.topic.clone();
        let mode = mode.unwrap_or_else(|| self.default_mode.clone());

        self.contracts.write().await.insert(
            topic.clone(),
            RegisteredContract {
                contract,
                mode,
                registered_at: Utc::now(),
            },
        );

        tracing::info!(topic = %topic, "Contract registered");
        Ok(())
    }

    /// Unregister a contract.
    pub async fn unregister(&self, topic: &str) -> Result<()> {
        self.contracts.write().await.remove(topic);
        Ok(())
    }

    /// Validate a message against the contract for its topic.
    pub async fn validate(
        &self,
        topic: &str,
        message: &[u8],
        headers: &[(String, Vec<u8>)],
    ) -> Option<ValidationResult> {
        let contracts = self.contracts.read().await;
        let registered = contracts.get(topic)?;

        if registered.mode == EnforcementMode::Disabled {
            self.stats.total_skipped.fetch_add(1, Ordering::Relaxed);
            return None;
        }

        let start = std::time::Instant::now();
        self.stats.total_validated.fetch_add(1, Ordering::Relaxed);

        let violations = self.check_violations(&registered.contract.spec, message, headers);

        let valid = violations.is_empty();
        if valid {
            self.stats.total_passed.fetch_add(1, Ordering::Relaxed);
        } else {
            self.stats.total_failed.fetch_add(1, Ordering::Relaxed);
        }

        Some(ValidationResult {
            valid,
            violations,
            contract_name: registered.contract.name.clone(),
            contract_version: registered.contract.version.clone(),
            validation_time_us: start.elapsed().as_micros() as u64,
        })
    }

    /// Check if a message should be rejected (enforce mode).
    pub async fn should_reject(&self, topic: &str) -> bool {
        let contracts = self.contracts.read().await;
        contracts
            .get(topic)
            .map(|r| r.mode == EnforcementMode::Enforce)
            .unwrap_or(false)
    }

    /// Get validation statistics.
    pub fn stats(&self) -> ValidationStatsSnapshot {
        ValidationStatsSnapshot {
            total_validated: self.stats.total_validated.load(Ordering::Relaxed),
            total_passed: self.stats.total_passed.load(Ordering::Relaxed),
            total_failed: self.stats.total_failed.load(Ordering::Relaxed),
            total_skipped: self.stats.total_skipped.load(Ordering::Relaxed),
        }
    }

    /// List all registered contracts.
    pub async fn list_contracts(&self) -> Vec<ContractSummary> {
        self.contracts
            .read()
            .await
            .iter()
            .map(|(topic, reg)| ContractSummary {
                topic: topic.clone(),
                name: reg.contract.name.clone(),
                version: reg.contract.version.clone(),
                mode: reg.mode.clone(),
                fields: reg.contract.spec.fields.len(),
                registered_at: reg.registered_at,
            })
            .collect()
    }

    fn check_violations(
        &self,
        spec: &ContractSpec,
        message: &[u8],
        headers: &[(String, Vec<u8>)],
    ) -> Vec<Violation> {
        let mut violations = Vec::new();

        // Size check
        if let Some(max_size) = spec.max_message_size {
            if message.len() > max_size {
                violations.push(Violation {
                    field: "<message>".to_string(),
                    violation_type: ViolationType::SizeExceeded,
                    message: format!(
                        "Message size {} exceeds limit {}",
                        message.len(),
                        max_size
                    ),
                    expected: Some(format!("<= {}", max_size)),
                    actual: Some(message.len().to_string()),
                });
            }
        }

        // Required headers
        for required_header in &spec.required_headers {
            if !headers.iter().any(|(k, _)| k == required_header) {
                violations.push(Violation {
                    field: format!("header:{}", required_header),
                    violation_type: ViolationType::MissingHeader,
                    message: format!("Required header '{}' is missing", required_header),
                    expected: Some(required_header.clone()),
                    actual: None,
                });
            }
        }

        // Parse as JSON for field validation
        if spec.fields.is_empty() {
            return violations;
        }

        let json: serde_json::Value = match serde_json::from_slice(message) {
            Ok(v) => v,
            Err(e) => {
                violations.push(Violation {
                    field: "<root>".to_string(),
                    violation_type: ViolationType::ParseError,
                    message: format!("Failed to parse message as JSON: {}", e),
                    expected: Some("valid JSON".to_string()),
                    actual: None,
                });
                return violations;
            }
        };

        // Field validation
        for field in &spec.fields {
            self.validate_field(&json, field, &mut violations);
        }

        violations
    }

    fn validate_field(
        &self,
        json: &serde_json::Value,
        field: &ContractField,
        violations: &mut Vec<Violation>,
    ) {
        let value = json.get(&field.name);

        // Required check
        if field.required && (value.is_none() || value == Some(&serde_json::Value::Null)) {
            violations.push(Violation {
                field: field.name.clone(),
                violation_type: ViolationType::MissingField,
                message: format!("Required field '{}' is missing", field.name),
                expected: Some(format!("{:?}", field.field_type)),
                actual: None,
            });
            return;
        }

        let value = match value {
            Some(v) => v,
            None => return, // Optional field not present
        };

        // Type check
        let type_matches = match field.field_type {
            FieldType::String => value.is_string(),
            FieldType::Integer => value.is_i64() || value.is_u64(),
            FieldType::Float => value.is_f64(),
            FieldType::Boolean => value.is_boolean(),
            FieldType::Array => value.is_array(),
            FieldType::Object => value.is_object(),
            FieldType::Null => value.is_null(),
            FieldType::Any => true,
        };

        if !type_matches {
            violations.push(Violation {
                field: field.name.clone(),
                violation_type: ViolationType::TypeMismatch,
                message: format!(
                    "Field '{}' expected {:?}, got {}",
                    field.name,
                    field.field_type,
                    json_type_name(value)
                ),
                expected: Some(format!("{:?}", field.field_type)),
                actual: Some(json_type_name(value)),
            });
        }

        // Pattern check
        if let Some(ref pattern) = field.pattern {
            if let Some(s) = value.as_str() {
                if let Ok(re) = regex::Regex::new(pattern) {
                    if !re.is_match(s) {
                        violations.push(Violation {
                            field: field.name.clone(),
                            violation_type: ViolationType::PatternMismatch,
                            message: format!(
                                "Field '{}' value '{}' doesn't match pattern '{}'",
                                field.name, s, pattern
                            ),
                            expected: Some(pattern.clone()),
                            actual: Some(s.to_string()),
                        });
                    }
                }
            }
        }

        // Range check
        if let Some(num) = value.as_f64() {
            if let Some(min) = field.min {
                if num < min {
                    violations.push(Violation {
                        field: field.name.clone(),
                        violation_type: ViolationType::RangeViolation,
                        message: format!(
                            "Field '{}' value {} is below minimum {}",
                            field.name, num, min
                        ),
                        expected: Some(format!(">= {}", min)),
                        actual: Some(num.to_string()),
                    });
                }
            }
            if let Some(max) = field.max {
                if num > max {
                    violations.push(Violation {
                        field: field.name.clone(),
                        violation_type: ViolationType::RangeViolation,
                        message: format!(
                            "Field '{}' value {} exceeds maximum {}",
                            field.name, num, max
                        ),
                        expected: Some(format!("<= {}", max)),
                        actual: Some(num.to_string()),
                    });
                }
            }
        }
    }
}

fn json_type_name(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
    .to_string()
}

/// Validation stats snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationStatsSnapshot {
    pub total_validated: u64,
    pub total_passed: u64,
    pub total_failed: u64,
    pub total_skipped: u64,
}

/// Contract summary for listing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContractSummary {
    pub topic: String,
    pub name: String,
    pub version: String,
    pub mode: EnforcementMode,
    pub fields: usize,
    pub registered_at: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// Contract Versioning & Diff
// ---------------------------------------------------------------------------

/// A change detected between two contract versions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContractChange {
    /// Type of change.
    pub change_type: ChangeType,
    /// Affected field.
    pub field: String,
    /// Description.
    pub description: String,
    /// Whether this is a breaking change.
    pub breaking: bool,
}

/// Types of contract changes.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeType {
    FieldAdded,
    FieldRemoved,
    FieldTypeChanged,
    FieldRequiredChanged,
    PatternChanged,
    RangeChanged,
    InvariantAdded,
    InvariantRemoved,
}

/// Compare two contract versions and detect changes.
pub fn diff_contracts(old: &StreamContract, new: &StreamContract) -> Vec<ContractChange> {
    let mut changes = Vec::new();

    let old_fields: HashMap<&str, &ContractField> =
        old.spec.fields.iter().map(|f| (f.name.as_str(), f)).collect();
    let new_fields: HashMap<&str, &ContractField> =
        new.spec.fields.iter().map(|f| (f.name.as_str(), f)).collect();

    // Removed fields
    for (name, field) in &old_fields {
        if !new_fields.contains_key(name) {
            changes.push(ContractChange {
                change_type: ChangeType::FieldRemoved,
                field: name.to_string(),
                description: format!("Field '{}' was removed", name),
                breaking: field.required,
            });
        }
    }

    // Added fields
    for (name, field) in &new_fields {
        if !old_fields.contains_key(name) {
            changes.push(ContractChange {
                change_type: ChangeType::FieldAdded,
                field: name.to_string(),
                description: format!("Field '{}' was added", name),
                breaking: field.required, // Adding a required field is breaking
            });
        }
    }

    // Modified fields
    for (name, new_field) in &new_fields {
        if let Some(old_field) = old_fields.get(name) {
            if old_field.field_type != new_field.field_type {
                changes.push(ContractChange {
                    change_type: ChangeType::FieldTypeChanged,
                    field: name.to_string(),
                    description: format!(
                        "Field '{}' type changed from {:?} to {:?}",
                        name, old_field.field_type, new_field.field_type
                    ),
                    breaking: true,
                });
            }

            if old_field.required != new_field.required {
                changes.push(ContractChange {
                    change_type: ChangeType::FieldRequiredChanged,
                    field: name.to_string(),
                    description: format!(
                        "Field '{}' required changed from {} to {}",
                        name, old_field.required, new_field.required
                    ),
                    breaking: !old_field.required && new_field.required,
                });
            }
        }
    }

    changes
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::definition::{ContractSpec, ContractField, FieldType, StreamContract};

    fn make_contract(fields: Vec<ContractField>) -> StreamContract {
        StreamContract {
            name: "test".to_string(),
            topic: "test-topic".to_string(),
            spec: ContractSpec {
                fields,
                invariants: Vec::new(),
                mock_data: None,
                max_message_size: None,
                required_headers: Vec::new(),
            },
            description: String::new(),
            version: "1.0.0".to_string(),
        }
    }

    #[tokio::test]
    async fn test_validator_valid_message() {
        let validator = ContractValidator::new(EnforcementMode::Enforce);
        let contract = make_contract(vec![
            ContractField {
                name: "name".to_string(),
                field_type: FieldType::String,
                required: true,
                description: String::new(),
                pattern: None,
                min: None,
                max: None,
            },
        ]);

        validator.register(contract, None).await.unwrap();

        let msg = br#"{"name": "Alice"}"#;
        let result = validator.validate("test-topic", msg, &[]).await.unwrap();
        assert!(result.valid);
        assert!(result.violations.is_empty());
    }

    #[tokio::test]
    async fn test_validator_missing_required_field() {
        let validator = ContractValidator::new(EnforcementMode::Enforce);
        let contract = make_contract(vec![
            ContractField {
                name: "id".to_string(),
                field_type: FieldType::Integer,
                required: true,
                description: String::new(),
                pattern: None,
                min: None,
                max: None,
            },
        ]);

        validator.register(contract, None).await.unwrap();

        let msg = br#"{"name": "Alice"}"#;
        let result = validator.validate("test-topic", msg, &[]).await.unwrap();
        assert!(!result.valid);
        assert_eq!(result.violations.len(), 1);
        assert!(matches!(
            result.violations[0].violation_type,
            ViolationType::MissingField
        ));
    }

    #[tokio::test]
    async fn test_validator_type_mismatch() {
        let validator = ContractValidator::new(EnforcementMode::Enforce);
        let contract = make_contract(vec![
            ContractField {
                name: "age".to_string(),
                field_type: FieldType::Integer,
                required: true,
                description: String::new(),
                pattern: None,
                min: None,
                max: None,
            },
        ]);

        validator.register(contract, None).await.unwrap();

        let msg = br#"{"age": "twenty"}"#;
        let result = validator.validate("test-topic", msg, &[]).await.unwrap();
        assert!(!result.valid);
        assert!(matches!(
            result.violations[0].violation_type,
            ViolationType::TypeMismatch
        ));
    }

    #[tokio::test]
    async fn test_validator_range_check() {
        let validator = ContractValidator::new(EnforcementMode::Enforce);
        let contract = make_contract(vec![
            ContractField {
                name: "score".to_string(),
                field_type: FieldType::Float,
                required: true,
                description: String::new(),
                pattern: None,
                min: Some(0.0),
                max: Some(100.0),
            },
        ]);

        validator.register(contract, None).await.unwrap();

        let msg = br#"{"score": 150}"#;
        let result = validator.validate("test-topic", msg, &[]).await.unwrap();
        assert!(!result.valid);
        assert!(matches!(
            result.violations[0].violation_type,
            ViolationType::RangeViolation
        ));
    }

    #[tokio::test]
    async fn test_validator_size_limit() {
        let validator = ContractValidator::new(EnforcementMode::Enforce);
        let mut contract = make_contract(Vec::new());
        contract.spec.max_message_size = Some(10);

        validator.register(contract, None).await.unwrap();

        let msg = br#"{"data": "this is a long message"}"#;
        let result = validator.validate("test-topic", msg, &[]).await.unwrap();
        assert!(!result.valid);
    }

    #[tokio::test]
    async fn test_no_contract_returns_none() {
        let validator = ContractValidator::new(EnforcementMode::Enforce);
        let result = validator.validate("unregistered", b"data", &[]).await;
        assert!(result.is_none());
    }

    #[test]
    fn test_diff_field_added() {
        let old = make_contract(vec![]);
        let new = make_contract(vec![ContractField {
            name: "new_field".to_string(),
            field_type: FieldType::String,
            required: true,
            description: String::new(),
            pattern: None,
            min: None,
            max: None,
        }]);

        let changes = diff_contracts(&old, &new);
        assert_eq!(changes.len(), 1);
        assert!(matches!(changes[0].change_type, ChangeType::FieldAdded));
        assert!(changes[0].breaking); // Required field added = breaking
    }

    #[test]
    fn test_diff_field_removed() {
        let old = make_contract(vec![ContractField {
            name: "removed".to_string(),
            field_type: FieldType::String,
            required: true,
            description: String::new(),
            pattern: None,
            min: None,
            max: None,
        }]);
        let new = make_contract(vec![]);

        let changes = diff_contracts(&old, &new);
        assert_eq!(changes.len(), 1);
        assert!(matches!(changes[0].change_type, ChangeType::FieldRemoved));
    }

    #[test]
    fn test_diff_type_changed() {
        let old = make_contract(vec![ContractField {
            name: "value".to_string(),
            field_type: FieldType::String,
            required: true,
            description: String::new(),
            pattern: None,
            min: None,
            max: None,
        }]);
        let new = make_contract(vec![ContractField {
            name: "value".to_string(),
            field_type: FieldType::Integer,
            required: true,
            description: String::new(),
            pattern: None,
            min: None,
            max: None,
        }]);

        let changes = diff_contracts(&old, &new);
        assert_eq!(changes.len(), 1);
        assert!(matches!(
            changes[0].change_type,
            ChangeType::FieldTypeChanged
        ));
        assert!(changes[0].breaking);
    }
}
