//! Policy validation for Streamline

use crate::policy::types::{AclPolicy, PolicyDocument, QuotaPolicy, SchemaPolicy, TopicPolicy};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Policy validation error
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyValidationError {
    /// Error code for programmatic handling
    pub code: String,
    /// Human-readable error message
    pub message: String,
    /// Path to the problematic element (e.g., "topics[0].name")
    pub path: Option<String>,
    /// Severity level
    pub severity: ValidationSeverity,
}

impl PolicyValidationError {
    /// Create a new error
    pub fn error(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            path: None,
            severity: ValidationSeverity::Error,
        }
    }

    /// Create a warning
    pub fn warning(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            path: None,
            severity: ValidationSeverity::Warning,
        }
    }

    /// Set the path
    pub fn with_path(mut self, path: impl Into<String>) -> Self {
        self.path = Some(path.into());
        self
    }
}

impl std::fmt::Display for PolicyValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(ref path) = self.path {
            write!(
                f,
                "[{}] {} at {}: {}",
                self.severity, self.code, path, self.message
            )
        } else {
            write!(f, "[{}] {}: {}", self.severity, self.code, self.message)
        }
    }
}

/// Validation severity level
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ValidationSeverity {
    /// Critical error that prevents policy application
    Error,
    /// Warning that should be addressed but doesn't block
    Warning,
    /// Informational notice
    Info,
}

impl std::fmt::Display for ValidationSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValidationSeverity::Error => write!(f, "ERROR"),
            ValidationSeverity::Warning => write!(f, "WARN"),
            ValidationSeverity::Info => write!(f, "INFO"),
        }
    }
}

/// Result of policy validation
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ValidationResult {
    /// Whether the policy is valid (no errors)
    pub valid: bool,
    /// List of validation errors/warnings
    pub errors: Vec<PolicyValidationError>,
}

impl ValidationResult {
    /// Create a successful validation result
    pub fn success() -> Self {
        Self {
            valid: true,
            errors: Vec::new(),
        }
    }

    /// Create a failed validation result with an error
    pub fn failure(error: PolicyValidationError) -> Self {
        Self {
            valid: false,
            errors: vec![error],
        }
    }

    /// Add an error
    pub fn add_error(&mut self, error: PolicyValidationError) {
        if error.severity == ValidationSeverity::Error {
            self.valid = false;
        }
        self.errors.push(error);
    }

    /// Check if there are any errors (not warnings)
    pub fn has_errors(&self) -> bool {
        self.errors
            .iter()
            .any(|e| e.severity == ValidationSeverity::Error)
    }

    /// Check if there are any warnings
    pub fn has_warnings(&self) -> bool {
        self.errors
            .iter()
            .any(|e| e.severity == ValidationSeverity::Warning)
    }

    /// Get only errors
    pub fn errors_only(&self) -> Vec<&PolicyValidationError> {
        self.errors
            .iter()
            .filter(|e| e.severity == ValidationSeverity::Error)
            .collect()
    }

    /// Get only warnings
    pub fn warnings_only(&self) -> Vec<&PolicyValidationError> {
        self.errors
            .iter()
            .filter(|e| e.severity == ValidationSeverity::Warning)
            .collect()
    }

    /// Merge another result into this one
    pub fn merge(&mut self, other: ValidationResult) {
        if !other.valid {
            self.valid = false;
        }
        self.errors.extend(other.errors);
    }
}

/// Policy validator
pub struct PolicyValidator {
    /// Maximum allowed partitions
    max_partitions: i32,
    /// Maximum allowed replication factor
    max_replication_factor: i16,
    /// Reserved topic name patterns
    reserved_patterns: Vec<String>,
    /// Valid ACL operations
    valid_operations: HashSet<String>,
    /// Valid resource types
    valid_resource_types: HashSet<String>,
    /// Valid schema types
    valid_schema_types: HashSet<String>,
    /// Valid compatibility levels
    valid_compatibility_levels: HashSet<String>,
}

impl PolicyValidator {
    /// Create a new validator with default settings
    pub fn new() -> Self {
        Self {
            max_partitions: 1000,
            max_replication_factor: 10,
            reserved_patterns: vec![
                "__consumer_offsets".to_string(),
                "__transaction_state".to_string(),
                "_schemas".to_string(),
            ],
            valid_operations: [
                "read",
                "write",
                "create",
                "delete",
                "alter",
                "describe",
                "clusteraction",
                "describeconfigs",
                "alterconfigs",
                "idempotentwrite",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            valid_resource_types: [
                "topic",
                "group",
                "cluster",
                "transactional_id",
                "delegation_token",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            valid_schema_types: ["avro", "protobuf", "json", "jsonschema"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            valid_compatibility_levels: [
                "none",
                "backward",
                "backward_transitive",
                "forward",
                "forward_transitive",
                "full",
                "full_transitive",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
        }
    }

    /// Set maximum partitions
    pub fn with_max_partitions(mut self, max: i32) -> Self {
        self.max_partitions = max;
        self
    }

    /// Set maximum replication factor
    pub fn with_max_replication(mut self, max: i16) -> Self {
        self.max_replication_factor = max;
        self
    }

    /// Add a reserved pattern
    pub fn with_reserved_pattern(mut self, pattern: impl Into<String>) -> Self {
        self.reserved_patterns.push(pattern.into());
        self
    }

    /// Validate a complete policy document
    pub fn validate(&self, doc: &PolicyDocument) -> ValidationResult {
        let mut result = ValidationResult::success();

        // Validate version
        if doc.version.is_empty() {
            result.add_error(
                PolicyValidationError::warning("MISSING_VERSION", "Policy version not specified")
                    .with_path("version"),
            );
        }

        // Check for duplicate topic names
        let mut seen_topics = HashSet::new();
        for (i, topic) in doc.topics.iter().enumerate() {
            if !seen_topics.insert(&topic.name) {
                result.add_error(
                    PolicyValidationError::error(
                        "DUPLICATE_TOPIC",
                        format!("Duplicate topic name: {}", topic.name),
                    )
                    .with_path(format!("topics[{}].name", i)),
                );
            }
            result.merge(self.validate_topic(topic, i));
        }

        // Validate ACLs
        for (i, acl) in doc.acls.iter().enumerate() {
            result.merge(self.validate_acl(acl, i));
        }

        // Check for duplicate quota entities
        let mut seen_quotas = HashSet::new();
        for (i, quota) in doc.quotas.iter().enumerate() {
            let key = format!("{}:{}", quota.entity_type, quota.entity);
            if !seen_quotas.insert(key.clone()) {
                result.add_error(
                    PolicyValidationError::warning(
                        "DUPLICATE_QUOTA",
                        format!("Duplicate quota entity: {}", key),
                    )
                    .with_path(format!("quotas[{}]", i)),
                );
            }
            result.merge(self.validate_quota(quota, i));
        }

        // Validate schemas
        for (i, schema) in doc.schemas.iter().enumerate() {
            result.merge(self.validate_schema(schema, i));
        }

        // Validate retention policy references
        for (i, topic) in doc.topics.iter().enumerate() {
            if let Some(ref policy_ref) = topic.retention_policy_ref {
                if !doc.retention_policies.contains_key(policy_ref) {
                    result.add_error(
                        PolicyValidationError::error(
                            "INVALID_RETENTION_REF",
                            format!("Retention policy '{}' not found", policy_ref),
                        )
                        .with_path(format!("topics[{}].retention_policy_ref", i)),
                    );
                }
            }
        }

        result
    }

    /// Validate a topic policy
    pub fn validate_topic(&self, topic: &TopicPolicy, index: usize) -> ValidationResult {
        let mut result = ValidationResult::success();
        let path_prefix = format!("topics[{}]", index);

        // Validate name
        if topic.name.is_empty() {
            result.add_error(
                PolicyValidationError::error("EMPTY_TOPIC_NAME", "Topic name cannot be empty")
                    .with_path(format!("{}.name", path_prefix)),
            );
        } else {
            // Check for reserved patterns
            for pattern in &self.reserved_patterns {
                if topic.name.starts_with(pattern) {
                    result.add_error(
                        PolicyValidationError::error(
                            "RESERVED_TOPIC_NAME",
                            format!("Topic name '{}' is reserved", topic.name),
                        )
                        .with_path(format!("{}.name", path_prefix)),
                    );
                }
            }

            // Check for invalid characters
            if topic.name.contains(' ') || topic.name.contains('\t') {
                result.add_error(
                    PolicyValidationError::error(
                        "INVALID_TOPIC_NAME",
                        "Topic name cannot contain whitespace",
                    )
                    .with_path(format!("{}.name", path_prefix)),
                );
            }
        }

        // Validate partitions
        if topic.partitions < 1 {
            result.add_error(
                PolicyValidationError::error(
                    "INVALID_PARTITIONS",
                    format!("Partitions must be at least 1, got {}", topic.partitions),
                )
                .with_path(format!("{}.partitions", path_prefix)),
            );
        } else if topic.partitions > self.max_partitions {
            result.add_error(
                PolicyValidationError::warning(
                    "HIGH_PARTITION_COUNT",
                    format!(
                        "Partition count {} exceeds recommended maximum {}",
                        topic.partitions, self.max_partitions
                    ),
                )
                .with_path(format!("{}.partitions", path_prefix)),
            );
        }

        // Validate replication factor
        if topic.replication_factor < 1 {
            result.add_error(
                PolicyValidationError::error(
                    "INVALID_REPLICATION",
                    format!(
                        "Replication factor must be at least 1, got {}",
                        topic.replication_factor
                    ),
                )
                .with_path(format!("{}.replication_factor", path_prefix)),
            );
        } else if topic.replication_factor > self.max_replication_factor {
            result.add_error(
                PolicyValidationError::error(
                    "REPLICATION_TOO_HIGH",
                    format!(
                        "Replication factor {} exceeds maximum {}",
                        topic.replication_factor, self.max_replication_factor
                    ),
                )
                .with_path(format!("{}.replication_factor", path_prefix)),
            );
        }

        // Validate retention
        if topic.retention.ms < -1 {
            result.add_error(
                PolicyValidationError::error(
                    "INVALID_RETENTION_MS",
                    "Retention ms must be -1 (unlimited) or positive",
                )
                .with_path(format!("{}.retention.ms", path_prefix)),
            );
        }

        if topic.retention.bytes < -1 {
            result.add_error(
                PolicyValidationError::error(
                    "INVALID_RETENTION_BYTES",
                    "Retention bytes must be -1 (unlimited) or positive",
                )
                .with_path(format!("{}.retention.bytes", path_prefix)),
            );
        }

        // Validate cleanup policy
        let valid_cleanup = ["delete", "compact", "delete,compact", "compact,delete"];
        if !valid_cleanup.contains(&topic.retention.cleanup_policy.as_str()) {
            result.add_error(
                PolicyValidationError::error(
                    "INVALID_CLEANUP_POLICY",
                    format!("Invalid cleanup policy: {}", topic.retention.cleanup_policy),
                )
                .with_path(format!("{}.retention.cleanup_policy", path_prefix)),
            );
        }

        result
    }

    /// Validate an ACL policy
    pub fn validate_acl(&self, acl: &AclPolicy, index: usize) -> ValidationResult {
        let mut result = ValidationResult::success();
        let path_prefix = format!("acls[{}]", index);

        // Validate principal format
        if !acl.principal.contains(':') {
            result.add_error(
                PolicyValidationError::error(
                    "INVALID_PRINCIPAL",
                    format!("Principal '{}' must be in format Type:Name", acl.principal),
                )
                .with_path(format!("{}.principal", path_prefix)),
            );
        }

        // Validate resource type
        if !self
            .valid_resource_types
            .contains(&acl.resource_type.to_lowercase())
        {
            result.add_error(
                PolicyValidationError::error(
                    "INVALID_RESOURCE_TYPE",
                    format!("Invalid resource type: {}", acl.resource_type),
                )
                .with_path(format!("{}.resource_type", path_prefix)),
            );
        }

        // Validate operations
        for (i, op) in acl.operations.iter().enumerate() {
            if !self.valid_operations.contains(&op.to_lowercase()) {
                result.add_error(
                    PolicyValidationError::error(
                        "INVALID_OPERATION",
                        format!("Invalid operation: {}", op),
                    )
                    .with_path(format!("{}.operations[{}]", path_prefix, i)),
                );
            }
        }

        // Validate permission
        if acl.permission != "allow" && acl.permission != "deny" {
            result.add_error(
                PolicyValidationError::error(
                    "INVALID_PERMISSION",
                    format!(
                        "Permission must be 'allow' or 'deny', got '{}'",
                        acl.permission
                    ),
                )
                .with_path(format!("{}.permission", path_prefix)),
            );
        }

        // Validate pattern type
        if acl.pattern_type != "literal" && acl.pattern_type != "prefixed" {
            result.add_error(
                PolicyValidationError::error(
                    "INVALID_PATTERN_TYPE",
                    format!(
                        "Pattern type must be 'literal' or 'prefixed', got '{}'",
                        acl.pattern_type
                    ),
                )
                .with_path(format!("{}.pattern_type", path_prefix)),
            );
        }

        result
    }

    /// Validate a quota policy
    pub fn validate_quota(&self, quota: &QuotaPolicy, index: usize) -> ValidationResult {
        let mut result = ValidationResult::success();
        let path_prefix = format!("quotas[{}]", index);

        // Validate entity type
        let valid_entity_types = ["user", "client-id", "ip"];
        if !valid_entity_types.contains(&quota.entity_type.as_str()) {
            result.add_error(
                PolicyValidationError::error(
                    "INVALID_ENTITY_TYPE",
                    format!("Invalid entity type: {}", quota.entity_type),
                )
                .with_path(format!("{}.entity_type", path_prefix)),
            );
        }

        // Validate rates
        if let Some(rate) = quota.producer_byte_rate {
            if rate < 0 {
                result.add_error(
                    PolicyValidationError::error(
                        "INVALID_RATE",
                        "Producer byte rate cannot be negative",
                    )
                    .with_path(format!("{}.producer_byte_rate", path_prefix)),
                );
            }
        }

        if let Some(rate) = quota.consumer_byte_rate {
            if rate < 0 {
                result.add_error(
                    PolicyValidationError::error(
                        "INVALID_RATE",
                        "Consumer byte rate cannot be negative",
                    )
                    .with_path(format!("{}.consumer_byte_rate", path_prefix)),
                );
            }
        }

        if let Some(pct) = quota.request_percentage {
            if !(0.0..=100.0).contains(&pct) {
                result.add_error(
                    PolicyValidationError::error(
                        "INVALID_PERCENTAGE",
                        format!("Request percentage must be 0-100, got {}", pct),
                    )
                    .with_path(format!("{}.request_percentage", path_prefix)),
                );
            }
        }

        result
    }

    /// Validate a schema policy
    pub fn validate_schema(&self, schema: &SchemaPolicy, index: usize) -> ValidationResult {
        let mut result = ValidationResult::success();
        let path_prefix = format!("schemas[{}]", index);

        // Validate subject
        if schema.subject.is_empty() {
            result.add_error(
                PolicyValidationError::error("EMPTY_SUBJECT", "Schema subject cannot be empty")
                    .with_path(format!("{}.subject", path_prefix)),
            );
        }

        // Validate schema type
        if !self
            .valid_schema_types
            .contains(&schema.schema_type.to_lowercase())
        {
            result.add_error(
                PolicyValidationError::error(
                    "INVALID_SCHEMA_TYPE",
                    format!("Invalid schema type: {}", schema.schema_type),
                )
                .with_path(format!("{}.schema_type", path_prefix)),
            );
        }

        // Validate compatibility level
        if !self
            .valid_compatibility_levels
            .contains(&schema.compatibility.to_lowercase())
        {
            result.add_error(
                PolicyValidationError::error(
                    "INVALID_COMPATIBILITY",
                    format!("Invalid compatibility level: {}", schema.compatibility),
                )
                .with_path(format!("{}.compatibility", path_prefix)),
            );
        }

        // Warn if neither schema nor schema_file is provided
        if schema.schema.is_none() && schema.schema_file.is_none() {
            result.add_error(
                PolicyValidationError::warning(
                    "MISSING_SCHEMA",
                    "Neither 'schema' nor 'schema_file' specified",
                )
                .with_path(path_prefix.clone()),
            );
        }

        // Warn if both are provided
        if schema.schema.is_some() && schema.schema_file.is_some() {
            result.add_error(
                PolicyValidationError::warning(
                    "DUAL_SCHEMA",
                    "Both 'schema' and 'schema_file' specified; 'schema' takes precedence",
                )
                .with_path(path_prefix),
            );
        }

        result
    }
}

impl Default for PolicyValidator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::types::*;
    use std::collections::HashMap;

    #[test]
    fn test_validation_result() {
        let mut result = ValidationResult::success();
        assert!(result.valid);
        assert!(result.errors.is_empty());

        result.add_error(PolicyValidationError::warning("TEST", "test warning"));
        assert!(result.valid); // Warnings don't invalidate

        result.add_error(PolicyValidationError::error("TEST", "test error"));
        assert!(!result.valid);
    }

    #[test]
    fn test_validate_valid_document() {
        let mut doc = PolicyDocument::default();
        doc.add_topic(TopicPolicy::new("events").with_partitions(3));
        doc.add_acl(AclPolicy::new("User:alice", "topic", "events").with_operations(vec!["read"]));

        let validator = PolicyValidator::new();
        let result = validator.validate(&doc);

        assert!(result.valid);
    }

    #[test]
    fn test_validate_duplicate_topics() {
        let mut doc = PolicyDocument::default();
        doc.add_topic(TopicPolicy::new("events"));
        doc.add_topic(TopicPolicy::new("events")); // duplicate

        let validator = PolicyValidator::new();
        let result = validator.validate(&doc);

        assert!(!result.valid);
        assert!(result.errors.iter().any(|e| e.code == "DUPLICATE_TOPIC"));
    }

    #[test]
    fn test_validate_invalid_partitions() {
        let topic = TopicPolicy::new("events").with_partitions(0);

        let validator = PolicyValidator::new();
        let result = validator.validate_topic(&topic, 0);

        assert!(!result.valid);
        assert!(result.errors.iter().any(|e| e.code == "INVALID_PARTITIONS"));
    }

    #[test]
    fn test_validate_reserved_topic() {
        let topic = TopicPolicy::new("__consumer_offsets_custom");

        let validator = PolicyValidator::new();
        let result = validator.validate_topic(&topic, 0);

        assert!(!result.valid);
        assert!(result
            .errors
            .iter()
            .any(|e| e.code == "RESERVED_TOPIC_NAME"));
    }

    #[test]
    fn test_validate_invalid_acl_principal() {
        let acl = AclPolicy {
            principal: "alice".to_string(), // missing Type:
            resource_type: "topic".to_string(),
            resource_name: "events".to_string(),
            pattern_type: "literal".to_string(),
            operations: vec!["read".to_string()],
            permission: "allow".to_string(),
            host: "*".to_string(),
            description: None,
            labels: HashMap::new(),
        };

        let validator = PolicyValidator::new();
        let result = validator.validate_acl(&acl, 0);

        assert!(!result.valid);
        assert!(result.errors.iter().any(|e| e.code == "INVALID_PRINCIPAL"));
    }

    #[test]
    fn test_validate_invalid_operation() {
        let acl = AclPolicy::new("User:alice", "topic", "events")
            .with_operations(vec!["read", "invalid_op"]);

        let validator = PolicyValidator::new();
        let result = validator.validate_acl(&acl, 0);

        assert!(!result.valid);
        assert!(result.errors.iter().any(|e| e.code == "INVALID_OPERATION"));
    }

    #[test]
    fn test_validate_quota() {
        let quota = QuotaPolicy::for_user("alice").with_producer_rate(10_000_000);

        let validator = PolicyValidator::new();
        let result = validator.validate_quota(&quota, 0);

        assert!(result.valid);
    }

    #[test]
    fn test_validate_invalid_quota_rate() {
        let mut quota = QuotaPolicy::for_user("alice");
        quota.producer_byte_rate = Some(-100); // invalid

        let validator = PolicyValidator::new();
        let result = validator.validate_quota(&quota, 0);

        assert!(!result.valid);
        assert!(result.errors.iter().any(|e| e.code == "INVALID_RATE"));
    }

    #[test]
    fn test_validate_schema() {
        let schema = SchemaPolicy::new("events-value", "avro")
            .with_compatibility("backward")
            .with_file("schemas/events.avsc");

        let validator = PolicyValidator::new();
        let result = validator.validate_schema(&schema, 0);

        assert!(result.valid);
    }

    #[test]
    fn test_validate_invalid_schema_type() {
        let schema = SchemaPolicy::new("events-value", "xml"); // invalid type

        let validator = PolicyValidator::new();
        let result = validator.validate_schema(&schema, 0);

        assert!(!result.valid);
        assert!(result
            .errors
            .iter()
            .any(|e| e.code == "INVALID_SCHEMA_TYPE"));
    }
}
