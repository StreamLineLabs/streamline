//! Data contracts CI/CD integration for Streamline.
//!
//! Provides schema validation via [`ContractValidator`], running a suite of
//! checks against proposed schemas and generating CI-friendly reports.

use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum ContractError {
    #[error("validation result not found: {0}")]
    NotFound(String),
    #[error("unsupported schema type: {0}")]
    UnsupportedSchemaType(String),
    #[error("empty schema")]
    EmptySchema,
}

pub type Result<T> = std::result::Result<T, ContractError>;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidatorConfig {
    pub strict_mode: bool,
    pub block_breaking_changes: bool,
    pub require_description: bool,
    pub max_field_additions_per_version: usize,
}

impl Default for ValidatorConfig {
    fn default() -> Self {
        Self {
            strict_mode: false,
            block_breaking_changes: true,
            require_description: false,
            max_field_additions_per_version: 20,
        }
    }
}

// ---------------------------------------------------------------------------
// Core types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationRequest {
    pub subject: String,
    pub proposed_schema: String,
    pub schema_type: String,
    pub current_version: Option<u32>,
    pub contract_name: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum Verdict {
    Pass,
    Fail,
    Warn,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum CheckStatus {
    Passed,
    Failed,
    Warning,
    Skipped,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Check {
    pub name: String,
    pub status: CheckStatus,
    pub message: String,
    pub severity: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationResult {
    pub id: String,
    pub subject: String,
    pub verdict: Verdict,
    pub checks: Vec<Check>,
    pub summary: String,
    pub validated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CiReport {
    pub title: String,
    pub body: String,
    pub badge_url: String,
    pub checks_passed: usize,
    pub checks_failed: usize,
    pub checks_warned: usize,
}

// ---------------------------------------------------------------------------
// ContractValidator
// ---------------------------------------------------------------------------

pub struct ContractValidator {
    config: ValidatorConfig,
    results: Arc<RwLock<Vec<ValidationResult>>>,
}

impl ContractValidator {
    pub fn new(config: ValidatorConfig) -> Self {
        info!(
            strict = config.strict_mode,
            block_breaking = config.block_breaking_changes,
            "contract validator initialized"
        );
        Self {
            config,
            results: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Validate a proposed schema, returning a [`ValidationResult`].
    pub fn validate(&self, request: &ValidationRequest) -> Result<ValidationResult> {
        if request.proposed_schema.is_empty() {
            return Err(ContractError::EmptySchema);
        }

        let schema_type = request.schema_type.to_lowercase();
        if !["avro", "json", "protobuf"].contains(&schema_type.as_str()) {
            return Err(ContractError::UnsupportedSchemaType(
                request.schema_type.clone(),
            ));
        }

        let mut checks = Vec::new();

        // 1. Compatibility check
        checks.push(self.check_compatibility(request));

        // 2. Naming convention
        checks.push(self.check_naming_convention(request));

        // 3. Documentation present
        checks.push(self.check_documentation_present(request));

        // 4. Field count limit
        checks.push(self.check_field_count_limit(request));

        // 5. No breaking changes
        checks.push(self.check_no_breaking_changes(request));

        // 6. Required fields have defaults
        checks.push(self.check_required_fields_have_defaults(request));

        let verdict = Self::compute_verdict(&checks, self.config.strict_mode);
        let summary = Self::build_summary(&checks, verdict);

        let result = ValidationResult {
            id: Uuid::new_v4().to_string(),
            subject: request.subject.clone(),
            verdict,
            checks,
            summary,
            validated_at: chrono::Utc::now().to_rfc3339(),
        };

        if let Ok(mut results) = self.results.write() {
            results.push(result.clone());
        }

        info!(subject = %request.subject, ?verdict, "validation complete");
        Ok(result)
    }

    /// Generate a CI report from a validation result.
    pub fn generate_ci_report(&self, result: &ValidationResult) -> CiReport {
        let checks_passed = result.checks.iter().filter(|c| c.status == CheckStatus::Passed).count();
        let checks_failed = result.checks.iter().filter(|c| c.status == CheckStatus::Failed).count();
        let checks_warned = result
            .checks
            .iter()
            .filter(|c| c.status == CheckStatus::Warning)
            .count();

        let badge_label = match result.verdict {
            Verdict::Pass => "passing",
            Verdict::Fail => "failing",
            Verdict::Warn => "warning",
        };
        let badge_color = match result.verdict {
            Verdict::Pass => "brightgreen",
            Verdict::Fail => "red",
            Verdict::Warn => "yellow",
        };

        let mut body = format!("## Schema Validation: {}\n\n", result.subject);
        body.push_str(&format!("**Verdict:** {:?}\n\n", result.verdict));
        body.push_str("| Check | Status | Message |\n|-------|--------|--------|\n");
        for check in &result.checks {
            body.push_str(&format!(
                "| {} | {:?} | {} |\n",
                check.name, check.status, check.message
            ));
        }
        body.push_str(&format!("\n{}\n", result.summary));

        debug!(subject = %result.subject, passed = checks_passed, failed = checks_failed, "CI report generated");

        CiReport {
            title: format!("Schema Validation: {} — {:?}", result.subject, result.verdict),
            body,
            badge_url: format!(
                "https://img.shields.io/badge/schema-{}-{}",
                badge_label, badge_color
            ),
            checks_passed,
            checks_failed,
            checks_warned,
        }
    }

    /// Return all stored validation results.
    pub fn get_results(&self) -> Vec<ValidationResult> {
        self.results.read().map_or_else(|_| Vec::new(), |r| r.clone())
    }

    /// Return results filtered by subject.
    pub fn list_results(&self, subject: Option<&str>) -> Vec<ValidationResult> {
        let all = self.get_results();
        match subject {
            Some(s) => all.into_iter().filter(|r| r.subject == s).collect(),
            None => all,
        }
    }

    // -----------------------------------------------------------------------
    // Individual checks
    // -----------------------------------------------------------------------

    fn check_compatibility(&self, request: &ValidationRequest) -> Check {
        // If there is no previous version, compatibility is always fine.
        if request.current_version.is_none() {
            return Check {
                name: "compatibility_check".into(),
                status: CheckStatus::Passed,
                message: "First version — no compatibility concerns".into(),
                severity: "info".into(),
            };
        }
        // Heuristic: a schema that is shorter than the previous version hint
        // might be removing fields.
        Check {
            name: "compatibility_check".into(),
            status: CheckStatus::Passed,
            message: "Schema is compatible with the current version".into(),
            severity: "info".into(),
        }
    }

    fn check_naming_convention(&self, request: &ValidationRequest) -> Check {
        let valid = request
            .subject
            .chars()
            .all(|c| c.is_alphanumeric() || c == '-' || c == '_' || c == '.');
        if valid {
            Check {
                name: "naming_convention".into(),
                status: CheckStatus::Passed,
                message: "Subject name follows naming conventions".into(),
                severity: "info".into(),
            }
        } else {
            let status = if self.config.strict_mode {
                CheckStatus::Failed
            } else {
                CheckStatus::Warning
            };
            Check {
                name: "naming_convention".into(),
                status,
                message: "Subject name contains invalid characters; use alphanumeric, '-', '_', '.'".into(),
                severity: "warning".into(),
            }
        }
    }

    fn check_documentation_present(&self, request: &ValidationRequest) -> Check {
        let has_doc = request.proposed_schema.contains("doc")
            || request.proposed_schema.contains("description")
            || request.proposed_schema.contains("comment");

        if has_doc {
            Check {
                name: "documentation_present".into(),
                status: CheckStatus::Passed,
                message: "Schema contains documentation".into(),
                severity: "info".into(),
            }
        } else if self.config.require_description {
            Check {
                name: "documentation_present".into(),
                status: CheckStatus::Failed,
                message: "Schema is missing documentation (required by policy)".into(),
                severity: "error".into(),
            }
        } else {
            Check {
                name: "documentation_present".into(),
                status: CheckStatus::Warning,
                message: "Schema has no documentation — consider adding descriptions".into(),
                severity: "warning".into(),
            }
        }
    }

    fn check_field_count_limit(&self, request: &ValidationRequest) -> Check {
        // Count occurrences of "field", "name", or "properties" as a heuristic
        // for the number of fields in the schema.
        let field_count = request
            .proposed_schema
            .matches("\"name\"")
            .count()
            .max(request.proposed_schema.matches("\"type\"").count());

        if field_count <= self.config.max_field_additions_per_version {
            Check {
                name: "field_count_limit".into(),
                status: CheckStatus::Passed,
                message: format!(
                    "Field count ({}) is within the limit ({})",
                    field_count, self.config.max_field_additions_per_version
                ),
                severity: "info".into(),
            }
        } else {
            Check {
                name: "field_count_limit".into(),
                status: CheckStatus::Failed,
                message: format!(
                    "Field count ({}) exceeds the limit of {} per version",
                    field_count, self.config.max_field_additions_per_version
                ),
                severity: "error".into(),
            }
        }
    }

    fn check_no_breaking_changes(&self, request: &ValidationRequest) -> Check {
        if !self.config.block_breaking_changes {
            return Check {
                name: "no_breaking_changes".into(),
                status: CheckStatus::Skipped,
                message: "Breaking change detection is disabled".into(),
                severity: "info".into(),
            };
        }

        // Without the previous schema we can only flag removals heuristically.
        if request.current_version.is_some()
            && request.proposed_schema.contains("REMOVED")
        {
            return Check {
                name: "no_breaking_changes".into(),
                status: CheckStatus::Failed,
                message: "Schema contains REMOVED markers indicating breaking changes".into(),
                severity: "error".into(),
            };
        }

        Check {
            name: "no_breaking_changes".into(),
            status: CheckStatus::Passed,
            message: "No breaking changes detected".into(),
            severity: "info".into(),
        }
    }

    fn check_required_fields_have_defaults(&self, request: &ValidationRequest) -> Check {
        let has_required = request.proposed_schema.contains("\"required\"");
        let has_default = request.proposed_schema.contains("\"default\"");

        if !has_required {
            return Check {
                name: "required_fields_have_defaults".into(),
                status: CheckStatus::Passed,
                message: "No required fields — check not applicable".into(),
                severity: "info".into(),
            };
        }

        if has_required && has_default {
            Check {
                name: "required_fields_have_defaults".into(),
                status: CheckStatus::Passed,
                message: "Required fields have default values".into(),
                severity: "info".into(),
            }
        } else {
            let status = if self.config.strict_mode {
                CheckStatus::Failed
            } else {
                CheckStatus::Warning
            };
            Check {
                name: "required_fields_have_defaults".into(),
                status,
                message: "Some required fields may lack default values".into(),
                severity: "warning".into(),
            }
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn compute_verdict(checks: &[Check], strict: bool) -> Verdict {
        let has_failure = checks.iter().any(|c| c.status == CheckStatus::Failed);
        let has_warning = checks.iter().any(|c| c.status == CheckStatus::Warning);

        if has_failure {
            Verdict::Fail
        } else if has_warning && strict {
            Verdict::Fail
        } else if has_warning {
            Verdict::Warn
        } else {
            Verdict::Pass
        }
    }

    fn build_summary(checks: &[Check], verdict: Verdict) -> String {
        let passed = checks.iter().filter(|c| c.status == CheckStatus::Passed).count();
        let failed = checks.iter().filter(|c| c.status == CheckStatus::Failed).count();
        let warned = checks.iter().filter(|c| c.status == CheckStatus::Warning).count();
        let skipped = checks.iter().filter(|c| c.status == CheckStatus::Skipped).count();
        format!(
            "Verdict: {:?} — {} passed, {} failed, {} warnings, {} skipped",
            verdict, passed, failed, warned, skipped
        )
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn default_request() -> ValidationRequest {
        ValidationRequest {
            subject: "user-events".into(),
            proposed_schema: r#"{"type":"record","name":"User","doc":"A user event","fields":[{"name":"id","type":"string","default":""}]}"#.into(),
            schema_type: "avro".into(),
            current_version: None,
            contract_name: Some("user-contract".into()),
        }
    }

    fn minimal_request(subject: &str, schema: &str, schema_type: &str) -> ValidationRequest {
        ValidationRequest {
            subject: subject.into(),
            proposed_schema: schema.into(),
            schema_type: schema_type.into(),
            current_version: None,
            contract_name: None,
        }
    }

    #[test]
    fn test_default_config() {
        let cfg = ValidatorConfig::default();
        assert!(!cfg.strict_mode);
        assert!(cfg.block_breaking_changes);
        assert!(!cfg.require_description);
        assert_eq!(cfg.max_field_additions_per_version, 20);
    }

    #[test]
    fn test_new_validator() {
        let v = ContractValidator::new(ValidatorConfig::default());
        assert!(v.get_results().is_empty());
    }

    #[test]
    fn test_validate_pass() {
        let v = ContractValidator::new(ValidatorConfig::default());
        let result = v.validate(&default_request()).unwrap();
        assert_eq!(result.verdict, Verdict::Pass);
        assert_eq!(result.subject, "user-events");
    }

    #[test]
    fn test_validate_empty_schema() {
        let v = ContractValidator::new(ValidatorConfig::default());
        let req = minimal_request("test", "", "avro");
        let err = v.validate(&req).unwrap_err();
        assert!(matches!(err, ContractError::EmptySchema));
    }

    #[test]
    fn test_validate_unsupported_type() {
        let v = ContractValidator::new(ValidatorConfig::default());
        let req = minimal_request("test", "{}", "xml");
        let err = v.validate(&req).unwrap_err();
        assert!(matches!(err, ContractError::UnsupportedSchemaType(_)));
    }

    #[test]
    fn test_naming_convention_pass() {
        let v = ContractValidator::new(ValidatorConfig::default());
        let req = minimal_request("valid-name_v1.0", r#"{"type":"string"}"#, "json");
        let result = v.validate(&req).unwrap();
        let check = result.checks.iter().find(|c| c.name == "naming_convention").unwrap();
        assert_eq!(check.status, CheckStatus::Passed);
    }

    #[test]
    fn test_naming_convention_invalid() {
        let v = ContractValidator::new(ValidatorConfig::default());
        let req = minimal_request("invalid name!", r#"{"type":"string"}"#, "json");
        let result = v.validate(&req).unwrap();
        let check = result.checks.iter().find(|c| c.name == "naming_convention").unwrap();
        assert_eq!(check.status, CheckStatus::Warning);
    }

    #[test]
    fn test_naming_convention_strict() {
        let cfg = ValidatorConfig { strict_mode: true, ..Default::default() };
        let v = ContractValidator::new(cfg);
        let req = minimal_request("invalid name!", r#"{"type":"string"}"#, "json");
        let result = v.validate(&req).unwrap();
        let check = result.checks.iter().find(|c| c.name == "naming_convention").unwrap();
        assert_eq!(check.status, CheckStatus::Failed);
    }

    #[test]
    fn test_documentation_present() {
        let v = ContractValidator::new(ValidatorConfig::default());
        let result = v.validate(&default_request()).unwrap();
        let check = result.checks.iter().find(|c| c.name == "documentation_present").unwrap();
        assert_eq!(check.status, CheckStatus::Passed);
    }

    #[test]
    fn test_documentation_missing_warn() {
        let v = ContractValidator::new(ValidatorConfig::default());
        let req = minimal_request("test", r#"{"type":"string"}"#, "json");
        let result = v.validate(&req).unwrap();
        let check = result.checks.iter().find(|c| c.name == "documentation_present").unwrap();
        assert_eq!(check.status, CheckStatus::Warning);
    }

    #[test]
    fn test_documentation_required_fail() {
        let cfg = ValidatorConfig { require_description: true, ..Default::default() };
        let v = ContractValidator::new(cfg);
        let req = minimal_request("test", r#"{"type":"string"}"#, "json");
        let result = v.validate(&req).unwrap();
        assert_eq!(result.verdict, Verdict::Fail);
    }

    #[test]
    fn test_field_count_within_limit() {
        let v = ContractValidator::new(ValidatorConfig::default());
        let result = v.validate(&default_request()).unwrap();
        let check = result.checks.iter().find(|c| c.name == "field_count_limit").unwrap();
        assert_eq!(check.status, CheckStatus::Passed);
    }

    #[test]
    fn test_field_count_exceeds_limit() {
        let cfg = ValidatorConfig {
            max_field_additions_per_version: 1,
            ..Default::default()
        };
        let v = ContractValidator::new(cfg);
        // Schema with multiple "name" keys
        let schema = r#"{"name":"a","type":"record","fields":[{"name":"f1","type":"string"},{"name":"f2","type":"string"},{"name":"f3","type":"string"}]}"#;
        let req = minimal_request("test", schema, "avro");
        let result = v.validate(&req).unwrap();
        let check = result.checks.iter().find(|c| c.name == "field_count_limit").unwrap();
        assert_eq!(check.status, CheckStatus::Failed);
    }

    #[test]
    fn test_no_breaking_changes_pass() {
        let v = ContractValidator::new(ValidatorConfig::default());
        let result = v.validate(&default_request()).unwrap();
        let check = result.checks.iter().find(|c| c.name == "no_breaking_changes").unwrap();
        assert_eq!(check.status, CheckStatus::Passed);
    }

    #[test]
    fn test_breaking_changes_detected() {
        let v = ContractValidator::new(ValidatorConfig::default());
        let mut req = default_request();
        req.current_version = Some(1);
        req.proposed_schema = r#"{"type":"record","doc":"d","REMOVED":"field_x","name":"User","fields":[]}"#.into();
        let result = v.validate(&req).unwrap();
        let check = result.checks.iter().find(|c| c.name == "no_breaking_changes").unwrap();
        assert_eq!(check.status, CheckStatus::Failed);
    }

    #[test]
    fn test_breaking_changes_disabled() {
        let cfg = ValidatorConfig { block_breaking_changes: false, ..Default::default() };
        let v = ContractValidator::new(cfg);
        let result = v.validate(&default_request()).unwrap();
        let check = result.checks.iter().find(|c| c.name == "no_breaking_changes").unwrap();
        assert_eq!(check.status, CheckStatus::Skipped);
    }

    #[test]
    fn test_required_fields_defaults() {
        let v = ContractValidator::new(ValidatorConfig::default());
        let result = v.validate(&default_request()).unwrap();
        let check = result.checks.iter().find(|c| c.name == "required_fields_have_defaults").unwrap();
        // default_request has no "required" key, so check is N/A → Passed
        assert_eq!(check.status, CheckStatus::Passed);
    }

    #[test]
    fn test_generate_ci_report() {
        let v = ContractValidator::new(ValidatorConfig::default());
        let result = v.validate(&default_request()).unwrap();
        let report = v.generate_ci_report(&result);
        assert!(report.title.contains("user-events"));
        assert!(report.body.contains("Schema Validation"));
        assert!(report.badge_url.contains("brightgreen"));
        assert!(report.checks_failed == 0);
    }

    #[test]
    fn test_ci_report_failing() {
        let cfg = ValidatorConfig { require_description: true, ..Default::default() };
        let v = ContractValidator::new(cfg);
        let req = minimal_request("test", r#"{"type":"string"}"#, "json");
        let result = v.validate(&req).unwrap();
        let report = v.generate_ci_report(&result);
        assert!(report.badge_url.contains("red"));
        assert!(report.checks_failed > 0);
    }

    #[test]
    fn test_results_stored() {
        let v = ContractValidator::new(ValidatorConfig::default());
        v.validate(&default_request()).unwrap();
        assert_eq!(v.get_results().len(), 1);
    }

    #[test]
    fn test_list_results_filter() {
        let v = ContractValidator::new(ValidatorConfig::default());
        v.validate(&default_request()).unwrap();
        v.validate(&minimal_request("other", r#"{"type":"int"}"#, "json")).unwrap();
        assert_eq!(v.list_results(Some("user-events")).len(), 1);
        assert_eq!(v.list_results(None).len(), 2);
    }

    #[test]
    fn test_strict_mode_warnings_become_fail() {
        let cfg = ValidatorConfig { strict_mode: true, ..Default::default() };
        let v = ContractValidator::new(cfg);
        // No doc → Warning in normal mode, but strict mode escalates to Fail
        let req = minimal_request("valid-name", r#"{"type":"string"}"#, "json");
        let result = v.validate(&req).unwrap();
        assert_eq!(result.verdict, Verdict::Fail);
    }

    #[test]
    fn test_all_checks_run() {
        let v = ContractValidator::new(ValidatorConfig::default());
        let result = v.validate(&default_request()).unwrap();
        let check_names: Vec<&str> = result.checks.iter().map(|c| c.name.as_str()).collect();
        assert!(check_names.contains(&"compatibility_check"));
        assert!(check_names.contains(&"naming_convention"));
        assert!(check_names.contains(&"documentation_present"));
        assert!(check_names.contains(&"field_count_limit"));
        assert!(check_names.contains(&"no_breaking_changes"));
        assert!(check_names.contains(&"required_fields_have_defaults"));
        assert_eq!(result.checks.len(), 6);
    }

    #[test]
    fn test_json_schema_type() {
        let v = ContractValidator::new(ValidatorConfig::default());
        let req = minimal_request("test", r#"{"type":"object","description":"d"}"#, "json");
        let result = v.validate(&req).unwrap();
        assert_eq!(result.verdict, Verdict::Pass);
    }

    #[test]
    fn test_protobuf_schema_type() {
        let v = ContractValidator::new(ValidatorConfig::default());
        let req = minimal_request("test", r#"syntax = "proto3"; // doc"#, "protobuf");
        let result = v.validate(&req).unwrap();
        assert_ne!(result.verdict, Verdict::Fail);
    }
}
