//! Data Contracts & Governance Engine
//!
//! Provides governance policies and data contracts on top of the schema registry.
//! Contracts define quality rules and SLAs for schema subjects, while policies
//! enforce organizational standards on schema evolution.

use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use super::{CompatibilityLevel, SchemaError, SchemaType};

// ---------------------------------------------------------------------------
// Data Contract types
// ---------------------------------------------------------------------------

/// Severity level for quality rule violations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    /// Hard failure — must not proceed.
    Error,
    /// Soft failure — logged but non-blocking.
    Warning,
}

/// The kind of quality check applied to a field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuleType {
    /// Field must not be null / missing.
    NotNull,
    /// Numeric field must fall within a range (expression: `"0..100"`).
    Range,
    /// String field must match a regex pattern.
    Regex,
    /// Arbitrary custom expression evaluated externally.
    Custom,
}

/// A single quality rule within a data contract.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QualityRule {
    /// Human-readable name for the rule.
    pub rule_name: String,
    /// Kind of check.
    pub rule_type: RuleType,
    /// Dot-path to the field (e.g. `"user.email"`).
    pub field: String,
    /// Type-specific expression (regex pattern, range bounds, etc.).
    pub expression: String,
    /// How severe a violation is.
    pub severity: Severity,
}

/// Captures a single quality-rule violation when evaluating data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContractViolation {
    /// Name of the rule that was violated.
    pub rule_name: String,
    /// Field that failed validation.
    pub field: String,
    /// The actual value found (stringified for uniformity).
    pub actual_value: String,
    /// Description of the constraint that was expected.
    pub expected_constraint: String,
    /// Severity inherited from the rule.
    pub severity: Severity,
}

/// A data contract binding quality expectations to a schema subject.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataContract {
    /// Unique contract name.
    pub name: String,
    /// Owning team or user.
    pub owner: String,
    /// Human-readable description.
    pub description: String,
    /// Schema subject this contract governs.
    pub schema_subject: String,
    /// Quality rules that data must satisfy.
    #[serde(default)]
    pub quality_rules: Vec<QualityRule>,
    /// SLA description (e.g. `"99.9% availability"`).
    #[serde(default)]
    pub sla: Option<String>,
    /// Free-form tags for discovery.
    #[serde(default)]
    pub tags: Vec<String>,
    /// Arbitrary key-value metadata.
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

// ---------------------------------------------------------------------------
// Governance Policy types
// ---------------------------------------------------------------------------

/// Naming convention required for schema fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NamingConvention {
    SnakeCase,
    CamelCase,
    PascalCase,
}

/// A single governance rule enforced by a policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyRule {
    /// Schema must include a `doc` / description field.
    RequireDocumentation,
    /// Schema must declare an owner in its metadata.
    RequireOwner,
    /// Maximum number of top-level fields allowed.
    MaxFieldCount(usize),
    /// All field names must follow the given convention.
    RequiredFieldNaming(NamingConvention),
    /// Breaking changes are forbidden (enforced via compatibility).
    ForbidBreakingChanges,
    /// Subject must use at least this compatibility level.
    RequireCompatibilityLevel(CompatibilityLevel),
    /// Changes require explicit review approval before registration.
    RequireReviewApproval,
}

/// Scope to which a governance policy applies.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyScope {
    /// Applies to every subject in the registry.
    Global,
    /// Applies to a single subject.
    Subject(String),
    /// Applies to all subjects owned by a team.
    Team(String),
}

/// A governance policy composed of one or more rules.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GovernancePolicy {
    /// Unique policy name.
    pub policy_name: String,
    /// Where this policy applies.
    pub scope: PolicyScope,
    /// Rules that make up the policy.
    pub rules: Vec<PolicyRule>,
}

/// A violation produced when a schema fails a governance policy rule.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyViolation {
    /// Name of the policy that was violated.
    pub policy_name: String,
    /// The specific rule that failed.
    pub rule: PolicyRule,
    /// Human-readable explanation.
    pub message: String,
}

// ---------------------------------------------------------------------------
// Governance Engine
// ---------------------------------------------------------------------------

/// Central engine for managing data contracts and governance policies.
#[derive(Debug, Clone)]
pub struct GovernanceEngine {
    contracts: Arc<RwLock<HashMap<String, DataContract>>>,
    policies: Arc<RwLock<HashMap<String, GovernancePolicy>>>,
}

impl Default for GovernanceEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl GovernanceEngine {
    /// Create a new, empty governance engine.
    pub fn new() -> Self {
        Self {
            contracts: Arc::new(RwLock::new(HashMap::new())),
            policies: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    // -- Contract management ------------------------------------------------

    /// Register (or replace) a data contract.
    pub async fn register_contract(&self, contract: DataContract) -> Result<(), SchemaError> {
        info!(name = %contract.name, subject = %contract.schema_subject, "Registering data contract");
        let mut contracts = self.contracts.write().await;
        contracts.insert(contract.name.clone(), contract);
        Ok(())
    }

    /// Retrieve a contract by name.
    pub async fn get_contract(&self, name: &str) -> Result<DataContract, SchemaError> {
        let contracts = self.contracts.read().await;
        contracts
            .get(name)
            .cloned()
            .ok_or_else(|| SchemaError::SchemaNotFound(format!("Contract not found: {name}")))
    }

    /// List all registered contracts.
    pub async fn list_contracts(&self) -> Vec<DataContract> {
        let contracts = self.contracts.read().await;
        contracts.values().cloned().collect()
    }

    /// Remove a contract by name. Returns an error if it does not exist.
    pub async fn remove_contract(&self, name: &str) -> Result<(), SchemaError> {
        let mut contracts = self.contracts.write().await;
        if contracts.remove(name).is_none() {
            return Err(SchemaError::SchemaNotFound(format!(
                "Contract not found: {name}"
            )));
        }
        info!(name, "Removed data contract");
        Ok(())
    }

    // -- Policy management --------------------------------------------------

    /// Add (or replace) a governance policy.
    pub async fn add_policy(&self, policy: GovernancePolicy) -> Result<(), SchemaError> {
        info!(name = %policy.policy_name, "Adding governance policy");
        let mut policies = self.policies.write().await;
        policies.insert(policy.policy_name.clone(), policy);
        Ok(())
    }

    /// Retrieve a policy by name.
    pub async fn get_policy(&self, name: &str) -> Result<GovernancePolicy, SchemaError> {
        let policies = self.policies.read().await;
        policies
            .get(name)
            .cloned()
            .ok_or_else(|| SchemaError::SchemaNotFound(format!("Policy not found: {name}")))
    }

    /// List all registered policies.
    pub async fn list_policies(&self) -> Vec<GovernancePolicy> {
        let policies = self.policies.read().await;
        policies.values().cloned().collect()
    }

    /// Remove a policy by name. Returns an error if it does not exist.
    pub async fn remove_policy(&self, name: &str) -> Result<(), SchemaError> {
        let mut policies = self.policies.write().await;
        if policies.remove(name).is_none() {
            return Err(SchemaError::SchemaNotFound(format!(
                "Policy not found: {name}"
            )));
        }
        info!(name, "Removed governance policy");
        Ok(())
    }

    // -- Evaluation ---------------------------------------------------------

    /// Validate `data` against the quality rules in `contract`.
    ///
    /// Returns a (possibly empty) list of [`ContractViolation`]s.
    pub fn evaluate_contract(
        data: &serde_json::Value,
        contract: &DataContract,
    ) -> Vec<ContractViolation> {
        let mut violations = Vec::new();

        for rule in &contract.quality_rules {
            let field_value = resolve_field(data, &rule.field);

            match &rule.rule_type {
                RuleType::NotNull => {
                    if field_value.is_none() || field_value == Some(&serde_json::Value::Null) {
                        violations.push(ContractViolation {
                            rule_name: rule.rule_name.clone(),
                            field: rule.field.clone(),
                            actual_value: "null".to_string(),
                            expected_constraint: "field must not be null".to_string(),
                            severity: rule.severity,
                        });
                    }
                }
                RuleType::Range => {
                    if let Some(val) = field_value.and_then(|v| v.as_f64()) {
                        if let Some((min, max)) = parse_range(&rule.expression) {
                            if val < min || val > max {
                                violations.push(ContractViolation {
                                    rule_name: rule.rule_name.clone(),
                                    field: rule.field.clone(),
                                    actual_value: val.to_string(),
                                    expected_constraint: format!("value in range {}", rule.expression),
                                    severity: rule.severity,
                                });
                            }
                        } else {
                            warn!(
                                rule = %rule.rule_name,
                                expression = %rule.expression,
                                "Invalid range expression"
                            );
                        }
                    } else {
                        violations.push(ContractViolation {
                            rule_name: rule.rule_name.clone(),
                            field: rule.field.clone(),
                            actual_value: field_value
                                .map(|v| v.to_string())
                                .unwrap_or_else(|| "missing".to_string()),
                            expected_constraint: format!("numeric value in range {}", rule.expression),
                            severity: rule.severity,
                        });
                    }
                }
                RuleType::Regex => {
                    if let Some(val) = field_value.and_then(|v| v.as_str()) {
                        match regex::Regex::new(&rule.expression) {
                            Ok(re) => {
                                if !re.is_match(val) {
                                    violations.push(ContractViolation {
                                        rule_name: rule.rule_name.clone(),
                                        field: rule.field.clone(),
                                        actual_value: val.to_string(),
                                        expected_constraint: format!(
                                            "must match regex: {}",
                                            rule.expression
                                        ),
                                        severity: rule.severity,
                                    });
                                }
                            }
                            Err(e) => {
                                warn!(
                                    rule = %rule.rule_name,
                                    error = %e,
                                    "Invalid regex in quality rule"
                                );
                            }
                        }
                    } else {
                        violations.push(ContractViolation {
                            rule_name: rule.rule_name.clone(),
                            field: rule.field.clone(),
                            actual_value: field_value
                                .map(|v| v.to_string())
                                .unwrap_or_else(|| "missing".to_string()),
                            expected_constraint: format!("string matching regex: {}", rule.expression),
                            severity: rule.severity,
                        });
                    }
                }
                RuleType::Custom => {
                    debug!(
                        rule = %rule.rule_name,
                        "Custom rule evaluation is a no-op in built-in engine"
                    );
                }
            }
        }

        violations
    }

    /// Validate a raw schema string against the rules in `policy`.
    ///
    /// Returns a (possibly empty) list of [`PolicyViolation`]s.
    pub fn evaluate_policy(
        schema: &str,
        schema_type: SchemaType,
        policy: &GovernancePolicy,
    ) -> Vec<PolicyViolation> {
        let mut violations = Vec::new();

        // Best-effort parse of the schema for field-level checks.
        let parsed: Option<serde_json::Value> = serde_json::from_str(schema).ok();

        for rule in &policy.rules {
            match rule {
                PolicyRule::RequireDocumentation => {
                    if !schema_has_documentation(schema, schema_type, parsed.as_ref()) {
                        violations.push(PolicyViolation {
                            policy_name: policy.policy_name.clone(),
                            rule: rule.clone(),
                            message: "Schema must include documentation".to_string(),
                        });
                    }
                }
                PolicyRule::RequireOwner => {
                    if !schema_has_owner(schema, parsed.as_ref()) {
                        violations.push(PolicyViolation {
                            policy_name: policy.policy_name.clone(),
                            rule: rule.clone(),
                            message: "Schema must declare an owner".to_string(),
                        });
                    }
                }
                PolicyRule::MaxFieldCount(max) => {
                    let count = count_top_level_fields(schema, schema_type, parsed.as_ref());
                    if count > *max {
                        violations.push(PolicyViolation {
                            policy_name: policy.policy_name.clone(),
                            rule: rule.clone(),
                            message: format!(
                                "Schema has {count} top-level fields, maximum allowed is {max}"
                            ),
                        });
                    }
                }
                PolicyRule::RequiredFieldNaming(convention) => {
                    let bad_fields =
                        fields_violating_naming(schema, schema_type, parsed.as_ref(), *convention);
                    if !bad_fields.is_empty() {
                        violations.push(PolicyViolation {
                            policy_name: policy.policy_name.clone(),
                            rule: rule.clone(),
                            message: format!(
                                "Fields violate {convention:?} naming: {}",
                                bad_fields.join(", ")
                            ),
                        });
                    }
                }
                PolicyRule::ForbidBreakingChanges => {
                    // Enforcement relies on the compatibility checker at registration
                    // time; here we simply note the policy exists.
                    debug!(policy = %policy.policy_name, "ForbidBreakingChanges evaluated at registration time");
                }
                PolicyRule::RequireCompatibilityLevel(_) => {
                    // Enforced at registration time by comparing subject config.
                    debug!(policy = %policy.policy_name, "RequireCompatibilityLevel evaluated at registration time");
                }
                PolicyRule::RequireReviewApproval => {
                    // Cannot be evaluated purely against schema text; requires
                    // workflow integration.
                    debug!(policy = %policy.policy_name, "RequireReviewApproval requires workflow integration");
                }
            }
        }

        violations
    }
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Resolve a dot-separated field path in a JSON value.
fn resolve_field<'a>(value: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    let mut current = value;
    for segment in path.split('.') {
        current = current.get(segment)?;
    }
    Some(current)
}

/// Parse a range expression like `"0..100"` into `(min, max)`.
fn parse_range(expr: &str) -> Option<(f64, f64)> {
    let parts: Vec<&str> = expr.split("..").collect();
    if parts.len() == 2 {
        let min = parts[0].trim().parse::<f64>().ok()?;
        let max = parts[1].trim().parse::<f64>().ok()?;
        Some((min, max))
    } else {
        None
    }
}

/// Heuristic: does the schema contain documentation?
fn schema_has_documentation(
    raw: &str,
    schema_type: SchemaType,
    parsed: Option<&serde_json::Value>,
) -> bool {
    match schema_type {
        SchemaType::Avro | SchemaType::Json => parsed
            .and_then(|v| v.get("doc").or_else(|| v.get("description")))
            .and_then(|v| v.as_str())
            .map(|s| !s.is_empty())
            .unwrap_or(false),
        SchemaType::Protobuf => {
            // Look for leading comment blocks in protobuf source.
            raw.contains("//") || raw.contains("/*")
        }
    }
}

/// Heuristic: does the schema declare an owner?
fn schema_has_owner(raw: &str, parsed: Option<&serde_json::Value>) -> bool {
    if let Some(v) = parsed {
        if v.get("owner").is_some() {
            return true;
        }
        // Check inside metadata/annotations map.
        if let Some(meta) = v.get("metadata").or_else(|| v.get("annotations")) {
            if meta.get("owner").is_some() {
                return true;
            }
        }
    }
    // Fallback: simple text search (covers protobuf comments).
    raw.contains("owner")
}

/// Count top-level fields in the schema.
fn count_top_level_fields(
    _raw: &str,
    schema_type: SchemaType,
    parsed: Option<&serde_json::Value>,
) -> usize {
    match (schema_type, parsed) {
        (SchemaType::Avro, Some(v)) => v
            .get("fields")
            .and_then(|f| f.as_array())
            .map(|a| a.len())
            .unwrap_or(0),
        (SchemaType::Json, Some(v)) => v
            .get("properties")
            .and_then(|p| p.as_object())
            .map(|o| o.len())
            .unwrap_or(0),
        _ => 0, // Protobuf field counting would require a full parser.
    }
}

/// Return field names that violate the given naming convention.
fn fields_violating_naming(
    _raw: &str,
    schema_type: SchemaType,
    parsed: Option<&serde_json::Value>,
    convention: NamingConvention,
) -> Vec<String> {
    let field_names: Vec<String> = match (schema_type, parsed) {
        (SchemaType::Avro, Some(v)) => v
            .get("fields")
            .and_then(|f| f.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|f| f.get("name").and_then(|n| n.as_str()).map(String::from))
                    .collect()
            })
            .unwrap_or_default(),
        (SchemaType::Json, Some(v)) => v
            .get("properties")
            .and_then(|p| p.as_object())
            .map(|o| o.keys().cloned().collect())
            .unwrap_or_default(),
        _ => Vec::new(),
    };

    field_names
        .into_iter()
        .filter(|name| !matches_convention(name, convention))
        .collect()
}

/// Check whether `name` follows `convention`.
fn matches_convention(name: &str, convention: NamingConvention) -> bool {
    match convention {
        NamingConvention::SnakeCase => {
            // snake_case: lowercase, digits, underscores; no leading/trailing underscore.
            !name.is_empty()
                && name
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
                && !name.starts_with('_')
                && !name.ends_with('_')
        }
        NamingConvention::CamelCase => {
            // camelCase: starts lowercase, no underscores.
            !name.is_empty()
                && name.starts_with(|c: char| c.is_ascii_lowercase())
                && !name.contains('_')
        }
        NamingConvention::PascalCase => {
            // PascalCase: starts uppercase, no underscores.
            !name.is_empty()
                && name.starts_with(|c: char| c.is_ascii_uppercase())
                && !name.contains('_')
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // -- Helper builders ----------------------------------------------------

    fn sample_contract() -> DataContract {
        DataContract {
            name: "user-events-contract".to_string(),
            owner: "platform-team".to_string(),
            description: "Contract for user event streams".to_string(),
            schema_subject: "user-events-value".to_string(),
            quality_rules: vec![
                QualityRule {
                    rule_name: "user_id_required".to_string(),
                    rule_type: RuleType::NotNull,
                    field: "user_id".to_string(),
                    expression: String::new(),
                    severity: Severity::Error,
                },
                QualityRule {
                    rule_name: "age_range".to_string(),
                    rule_type: RuleType::Range,
                    field: "age".to_string(),
                    expression: "0..150".to_string(),
                    severity: Severity::Warning,
                },
                QualityRule {
                    rule_name: "email_format".to_string(),
                    rule_type: RuleType::Regex,
                    field: "email".to_string(),
                    expression: r"^[^@]+@[^@]+\.[^@]+$".to_string(),
                    severity: Severity::Error,
                },
            ],
            sla: Some("99.9% availability".to_string()),
            tags: vec!["user".to_string(), "events".to_string()],
            metadata: HashMap::from([("version".to_string(), "1".to_string())]),
        }
    }

    fn sample_policy() -> GovernancePolicy {
        GovernancePolicy {
            policy_name: "strict-schema-policy".to_string(),
            scope: PolicyScope::Global,
            rules: vec![
                PolicyRule::RequireDocumentation,
                PolicyRule::RequireOwner,
                PolicyRule::MaxFieldCount(10),
                PolicyRule::RequiredFieldNaming(NamingConvention::SnakeCase),
            ],
        }
    }

    // -- Contract CRUD ------------------------------------------------------

    #[tokio::test]
    async fn test_register_and_get_contract() {
        let engine = GovernanceEngine::new();
        let contract = sample_contract();

        engine.register_contract(contract.clone()).await.unwrap();
        let fetched = engine.get_contract("user-events-contract").await.unwrap();

        assert_eq!(fetched.name, "user-events-contract");
        assert_eq!(fetched.owner, "platform-team");
        assert_eq!(fetched.quality_rules.len(), 3);
    }

    #[tokio::test]
    async fn test_get_contract_not_found() {
        let engine = GovernanceEngine::new();
        assert!(engine.get_contract("nonexistent").await.is_err());
    }

    #[tokio::test]
    async fn test_list_contracts() {
        let engine = GovernanceEngine::new();
        engine.register_contract(sample_contract()).await.unwrap();

        let list = engine.list_contracts().await;
        assert_eq!(list.len(), 1);
    }

    #[tokio::test]
    async fn test_remove_contract() {
        let engine = GovernanceEngine::new();
        engine.register_contract(sample_contract()).await.unwrap();
        engine.remove_contract("user-events-contract").await.unwrap();

        assert!(engine.get_contract("user-events-contract").await.is_err());
    }

    #[tokio::test]
    async fn test_remove_contract_not_found() {
        let engine = GovernanceEngine::new();
        assert!(engine.remove_contract("nope").await.is_err());
    }

    // -- Policy CRUD --------------------------------------------------------

    #[tokio::test]
    async fn test_add_and_get_policy() {
        let engine = GovernanceEngine::new();
        engine.add_policy(sample_policy()).await.unwrap();

        let fetched = engine.get_policy("strict-schema-policy").await.unwrap();
        assert_eq!(fetched.rules.len(), 4);
    }

    #[tokio::test]
    async fn test_get_policy_not_found() {
        let engine = GovernanceEngine::new();
        assert!(engine.get_policy("nonexistent").await.is_err());
    }

    #[tokio::test]
    async fn test_list_policies() {
        let engine = GovernanceEngine::new();
        engine.add_policy(sample_policy()).await.unwrap();

        let list = engine.list_policies().await;
        assert_eq!(list.len(), 1);
    }

    #[tokio::test]
    async fn test_remove_policy() {
        let engine = GovernanceEngine::new();
        engine.add_policy(sample_policy()).await.unwrap();
        engine.remove_policy("strict-schema-policy").await.unwrap();

        assert!(engine.get_policy("strict-schema-policy").await.is_err());
    }

    #[tokio::test]
    async fn test_remove_policy_not_found() {
        let engine = GovernanceEngine::new();
        assert!(engine.remove_policy("nope").await.is_err());
    }

    // -- Contract evaluation ------------------------------------------------

    #[test]
    fn test_evaluate_contract_all_pass() {
        let contract = sample_contract();
        let data = json!({
            "user_id": "u-123",
            "age": 30,
            "email": "alice@example.com"
        });

        let violations = GovernanceEngine::evaluate_contract(&data, &contract);
        assert!(violations.is_empty(), "Expected no violations: {violations:?}");
    }

    #[test]
    fn test_evaluate_contract_not_null_violation() {
        let contract = sample_contract();
        let data = json!({
            "age": 25,
            "email": "bob@example.com"
        });

        let violations = GovernanceEngine::evaluate_contract(&data, &contract);
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].rule_name, "user_id_required");
        assert_eq!(violations[0].severity, Severity::Error);
    }

    #[test]
    fn test_evaluate_contract_null_field_violation() {
        let contract = sample_contract();
        let data = json!({
            "user_id": null,
            "age": 25,
            "email": "bob@example.com"
        });

        let violations = GovernanceEngine::evaluate_contract(&data, &contract);
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].rule_name, "user_id_required");
    }

    #[test]
    fn test_evaluate_contract_range_violation() {
        let contract = sample_contract();
        let data = json!({
            "user_id": "u-1",
            "age": 200,
            "email": "a@b.com"
        });

        let violations = GovernanceEngine::evaluate_contract(&data, &contract);
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].rule_name, "age_range");
        assert_eq!(violations[0].severity, Severity::Warning);
    }

    #[test]
    fn test_evaluate_contract_regex_violation() {
        let contract = sample_contract();
        let data = json!({
            "user_id": "u-1",
            "age": 25,
            "email": "not-an-email"
        });

        let violations = GovernanceEngine::evaluate_contract(&data, &contract);
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].rule_name, "email_format");
    }

    #[test]
    fn test_evaluate_contract_multiple_violations() {
        let contract = sample_contract();
        let data = json!({
            "age": -5,
            "email": "bad"
        });

        let violations = GovernanceEngine::evaluate_contract(&data, &contract);
        assert_eq!(violations.len(), 3); // not-null, range, regex
    }

    #[test]
    fn test_evaluate_contract_nested_field() {
        let contract = DataContract {
            name: "nested-contract".to_string(),
            owner: "team".to_string(),
            description: "tests nested paths".to_string(),
            schema_subject: "test-value".to_string(),
            quality_rules: vec![QualityRule {
                rule_name: "nested_required".to_string(),
                rule_type: RuleType::NotNull,
                field: "user.name".to_string(),
                expression: String::new(),
                severity: Severity::Error,
            }],
            sla: None,
            tags: vec![],
            metadata: HashMap::new(),
        };

        let data = json!({ "user": { "name": "Alice" } });
        assert!(GovernanceEngine::evaluate_contract(&data, &contract).is_empty());

        let data_missing = json!({ "user": {} });
        assert_eq!(
            GovernanceEngine::evaluate_contract(&data_missing, &contract).len(),
            1
        );
    }

    #[test]
    fn test_evaluate_contract_custom_rule_noop() {
        let contract = DataContract {
            name: "custom".to_string(),
            owner: "t".to_string(),
            description: String::new(),
            schema_subject: "s".to_string(),
            quality_rules: vec![QualityRule {
                rule_name: "custom_check".to_string(),
                rule_type: RuleType::Custom,
                field: "x".to_string(),
                expression: "external://validate".to_string(),
                severity: Severity::Warning,
            }],
            sla: None,
            tags: vec![],
            metadata: HashMap::new(),
        };

        let data = json!({"x": 1});
        assert!(GovernanceEngine::evaluate_contract(&data, &contract).is_empty());
    }

    // -- Policy evaluation --------------------------------------------------

    #[test]
    fn test_evaluate_policy_avro_passes() {
        let policy = sample_policy();
        let schema = r#"{
            "type": "record",
            "name": "User",
            "doc": "A user record",
            "owner": "platform-team",
            "fields": [
                {"name": "user_id", "type": "string"},
                {"name": "email", "type": "string"}
            ]
        }"#;

        let violations = GovernanceEngine::evaluate_policy(schema, SchemaType::Avro, &policy);
        assert!(violations.is_empty(), "Expected no violations: {violations:?}");
    }

    #[test]
    fn test_evaluate_policy_missing_documentation() {
        let policy = GovernancePolicy {
            policy_name: "doc-required".to_string(),
            scope: PolicyScope::Global,
            rules: vec![PolicyRule::RequireDocumentation],
        };
        let schema = r#"{"type":"record","name":"X","fields":[]}"#;

        let violations = GovernanceEngine::evaluate_policy(schema, SchemaType::Avro, &policy);
        assert_eq!(violations.len(), 1);
        assert!(violations[0].message.contains("documentation"));
    }

    #[test]
    fn test_evaluate_policy_missing_owner() {
        let policy = GovernancePolicy {
            policy_name: "owner-required".to_string(),
            scope: PolicyScope::Global,
            rules: vec![PolicyRule::RequireOwner],
        };
        let schema = r#"{"type":"record","name":"X","fields":[]}"#;

        let violations = GovernanceEngine::evaluate_policy(schema, SchemaType::Avro, &policy);
        assert_eq!(violations.len(), 1);
        assert!(violations[0].message.contains("owner"));
    }

    #[test]
    fn test_evaluate_policy_max_field_count() {
        let policy = GovernancePolicy {
            policy_name: "max-fields".to_string(),
            scope: PolicyScope::Global,
            rules: vec![PolicyRule::MaxFieldCount(2)],
        };
        let schema = r#"{
            "type": "record",
            "name": "Wide",
            "fields": [
                {"name": "a", "type": "string"},
                {"name": "b", "type": "string"},
                {"name": "c", "type": "string"}
            ]
        }"#;

        let violations = GovernanceEngine::evaluate_policy(schema, SchemaType::Avro, &policy);
        assert_eq!(violations.len(), 1);
        assert!(violations[0].message.contains("3"));
    }

    #[test]
    fn test_evaluate_policy_naming_violation() {
        let policy = GovernancePolicy {
            policy_name: "naming".to_string(),
            scope: PolicyScope::Global,
            rules: vec![PolicyRule::RequiredFieldNaming(NamingConvention::SnakeCase)],
        };
        let schema = r#"{
            "type": "record",
            "name": "T",
            "fields": [
                {"name": "good_name", "type": "string"},
                {"name": "badName", "type": "string"}
            ]
        }"#;

        let violations = GovernanceEngine::evaluate_policy(schema, SchemaType::Avro, &policy);
        assert_eq!(violations.len(), 1);
        assert!(violations[0].message.contains("badName"));
    }

    #[test]
    fn test_evaluate_policy_json_schema() {
        let policy = GovernancePolicy {
            policy_name: "json-policy".to_string(),
            scope: PolicyScope::Subject("orders-value".to_string()),
            rules: vec![
                PolicyRule::RequireDocumentation,
                PolicyRule::MaxFieldCount(2),
            ],
        };
        let schema = r#"{
            "type": "object",
            "description": "Order schema",
            "properties": {
                "order_id": {"type": "string"},
                "amount": {"type": "number"},
                "currency": {"type": "string"}
            }
        }"#;

        let violations = GovernanceEngine::evaluate_policy(schema, SchemaType::Json, &policy);
        // Documentation present (via "description"), but 3 fields > max 2
        assert_eq!(violations.len(), 1);
        assert!(violations[0].message.contains("3"));
    }

    #[test]
    fn test_evaluate_policy_protobuf_documentation() {
        let policy = GovernancePolicy {
            policy_name: "proto-doc".to_string(),
            scope: PolicyScope::Global,
            rules: vec![PolicyRule::RequireDocumentation],
        };

        // With a comment — should pass.
        let proto_with_doc = "// User message\nmessage User { string name = 1; }";
        assert!(
            GovernanceEngine::evaluate_policy(proto_with_doc, SchemaType::Protobuf, &policy)
                .is_empty()
        );

        // Without a comment — should fail.
        let proto_no_doc = "message User { string name = 1; }";
        assert_eq!(
            GovernanceEngine::evaluate_policy(proto_no_doc, SchemaType::Protobuf, &policy).len(),
            1
        );
    }

    // -- Naming convention helpers ------------------------------------------

    #[test]
    fn test_matches_snake_case() {
        assert!(matches_convention("user_id", NamingConvention::SnakeCase));
        assert!(matches_convention("a", NamingConvention::SnakeCase));
        assert!(!matches_convention("userId", NamingConvention::SnakeCase));
        assert!(!matches_convention("UserId", NamingConvention::SnakeCase));
        assert!(!matches_convention("_leading", NamingConvention::SnakeCase));
        assert!(!matches_convention("trailing_", NamingConvention::SnakeCase));
        assert!(!matches_convention("", NamingConvention::SnakeCase));
    }

    #[test]
    fn test_matches_camel_case() {
        assert!(matches_convention("userId", NamingConvention::CamelCase));
        assert!(matches_convention("a", NamingConvention::CamelCase));
        assert!(!matches_convention("UserId", NamingConvention::CamelCase));
        assert!(!matches_convention("user_id", NamingConvention::CamelCase));
    }

    #[test]
    fn test_matches_pascal_case() {
        assert!(matches_convention("UserId", NamingConvention::PascalCase));
        assert!(!matches_convention("userId", NamingConvention::PascalCase));
        assert!(!matches_convention("user_id", NamingConvention::PascalCase));
    }

    // -- Serialization roundtrip --------------------------------------------

    #[test]
    fn test_contract_serde_roundtrip() {
        let contract = sample_contract();
        let json = serde_json::to_string_pretty(&contract).unwrap();
        let parsed: DataContract = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.name, contract.name);
        assert_eq!(parsed.quality_rules.len(), contract.quality_rules.len());
        assert_eq!(parsed.tags, contract.tags);
    }

    #[test]
    fn test_policy_serde_roundtrip() {
        let policy = sample_policy();
        let json = serde_json::to_string_pretty(&policy).unwrap();
        let parsed: GovernancePolicy = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.policy_name, policy.policy_name);
        assert_eq!(parsed.rules.len(), policy.rules.len());
    }

    #[test]
    fn test_parse_range() {
        assert_eq!(parse_range("0..100"), Some((0.0, 100.0)));
        assert_eq!(parse_range("-10..10"), Some((-10.0, 10.0)));
        assert_eq!(parse_range("0.5..1.5"), Some((0.5, 1.5)));
        assert_eq!(parse_range("bad"), None);
        assert_eq!(parse_range(".."), None);
    }

    #[test]
    fn test_resolve_field_nested() {
        let data = json!({"a": {"b": {"c": 42}}});
        assert_eq!(resolve_field(&data, "a.b.c"), Some(&json!(42)));
        assert_eq!(resolve_field(&data, "a.b"), Some(&json!({"c": 42})));
        assert!(resolve_field(&data, "a.x").is_none());
    }

    #[test]
    fn test_default_governance_engine() {
        let engine = GovernanceEngine::default();
        // Just ensure Default impl works without panic.
        assert!(std::ptr::eq(
            Arc::as_ptr(&engine.contracts),
            Arc::as_ptr(&engine.contracts)
        ));
    }
}
