//! Policy Engine for applying and enforcing policies

use crate::error::{Result, StreamlineError};
use crate::policy::audit::{PolicyAuditEntry, PolicyAuditLog, PolicyAuditTrail, PolicyChangeType};
use crate::policy::loader::{PolicyLoader, PolicySource};
use crate::policy::types::{AclPolicy, PolicyDocument, QuotaPolicy, TopicPolicy};
use crate::policy::validator::{PolicyValidator, ValidationResult};
use crate::storage::TopicManager;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info};

/// Statistics about the policy engine
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PolicyEngineStat {
    /// Number of policies loaded
    pub policies_loaded: usize,
    /// Number of topics managed
    pub topics_managed: usize,
    /// Number of ACLs managed
    pub acls_managed: usize,
    /// Number of quotas managed
    pub quotas_managed: usize,
    /// Number of schemas managed
    pub schemas_managed: usize,
    /// Last policy apply timestamp
    pub last_apply_timestamp: Option<i64>,
    /// Last validation result
    pub last_validation_valid: bool,
    /// Number of validation errors
    pub validation_errors: usize,
    /// Number of validation warnings
    pub validation_warnings: usize,
}

/// Policy Engine for managing and applying policies
pub struct PolicyEngine {
    /// Current policy document
    current_policy: Arc<RwLock<PolicyDocument>>,
    /// Policy validator
    validator: PolicyValidator,
    /// Policy loader
    loader: PolicyLoader,
    /// Audit trail
    audit_trail: Arc<RwLock<PolicyAuditTrail>>,
    /// Topic manager (optional, for applying topic policies)
    topic_manager: Option<Arc<TopicManager>>,
    /// Dry run mode (validate only, don't apply)
    dry_run: bool,
    /// Statistics
    stats: Arc<RwLock<PolicyEngineStat>>,
}

impl PolicyEngine {
    /// Create a new policy engine
    pub fn new() -> Self {
        Self {
            current_policy: Arc::new(RwLock::new(PolicyDocument::default())),
            validator: PolicyValidator::new(),
            loader: PolicyLoader::new(),
            audit_trail: Arc::new(RwLock::new(PolicyAuditTrail::new())),
            topic_manager: None,
            dry_run: false,
            stats: Arc::new(RwLock::new(PolicyEngineStat::default())),
        }
    }

    /// Set the topic manager for applying topic policies
    pub fn with_topic_manager(mut self, tm: Arc<TopicManager>) -> Self {
        self.topic_manager = Some(tm);
        self
    }

    /// Set the policy loader
    pub fn with_loader(mut self, loader: PolicyLoader) -> Self {
        self.loader = loader;
        self
    }

    /// Set the validator
    pub fn with_validator(mut self, validator: PolicyValidator) -> Self {
        self.validator = validator;
        self
    }

    /// Enable dry run mode
    pub fn with_dry_run(mut self, dry_run: bool) -> Self {
        self.dry_run = dry_run;
        self
    }

    /// Load and validate policies from a source
    pub async fn load(&self, source: PolicySource) -> Result<ValidationResult> {
        info!("Loading policies");

        let doc = self.loader.load(source)?;
        let validation = self.validator.validate(&doc);

        // Update stats
        {
            let mut stats = self.stats.write().await;
            stats.last_validation_valid = validation.valid;
            stats.validation_errors = validation.errors_only().len();
            stats.validation_warnings = validation.warnings_only().len();
        }

        if validation.valid {
            let mut current = self.current_policy.write().await;
            *current = doc;

            let mut stats = self.stats.write().await;
            stats.policies_loaded += 1;
        }

        Ok(validation)
    }

    /// Apply the current policy to the system
    pub async fn apply(&self) -> Result<PolicyApplyResult> {
        let policy = self.current_policy.read().await.clone();
        let mut result = PolicyApplyResult::default();

        info!(
            topics = policy.topics.len(),
            acls = policy.acls.len(),
            quotas = policy.quotas.len(),
            "Applying policy"
        );

        if self.dry_run {
            info!("Dry run mode - not applying changes");
            result.dry_run = true;
            result.changes = self.compute_changes(&policy).await?;
            return Ok(result);
        }

        // Apply topic policies
        for topic_policy in &policy.topics {
            match self.apply_topic_policy(topic_policy).await {
                Ok(change) => {
                    if let Some(c) = change {
                        result.changes.push(c);
                    }
                }
                Err(e) => {
                    error!(topic = %topic_policy.name, error = %e, "Failed to apply topic policy");
                    result
                        .errors
                        .push(format!("Topic {}: {}", topic_policy.name, e));
                }
            }
        }

        // Apply ACL policies
        for acl_policy in &policy.acls {
            match self.apply_acl_policy(acl_policy).await {
                Ok(change) => {
                    if let Some(c) = change {
                        result.changes.push(c);
                    }
                }
                Err(e) => {
                    error!(
                        principal = %acl_policy.principal,
                        error = %e,
                        "Failed to apply ACL policy"
                    );
                    result
                        .errors
                        .push(format!("ACL {}: {}", acl_policy.principal, e));
                }
            }
        }

        // Apply quota policies
        for quota_policy in &policy.quotas {
            match self.apply_quota_policy(quota_policy).await {
                Ok(change) => {
                    if let Some(c) = change {
                        result.changes.push(c);
                    }
                }
                Err(e) => {
                    error!(
                        entity = %quota_policy.entity,
                        error = %e,
                        "Failed to apply quota policy"
                    );
                    result
                        .errors
                        .push(format!("Quota {}: {}", quota_policy.entity, e));
                }
            }
        }

        // Update stats
        {
            let mut stats = self.stats.write().await;
            stats.topics_managed = policy.topics.len();
            stats.acls_managed = policy.acls.len();
            stats.quotas_managed = policy.quotas.len();
            stats.schemas_managed = policy.schemas.len();
            stats.last_apply_timestamp = Some(chrono::Utc::now().timestamp_millis());
        }

        result.success = result.errors.is_empty();
        Ok(result)
    }

    /// Apply a single topic policy
    async fn apply_topic_policy(&self, policy: &TopicPolicy) -> Result<Option<PolicyChange>> {
        let Some(ref tm) = self.topic_manager else {
            debug!("No topic manager configured, skipping topic policy");
            return Ok(None);
        };

        // Check if topic exists
        let exists = tm.get_topic_metadata(&policy.name).is_ok();

        if exists {
            // Topic exists - check if config needs updating
            debug!(topic = %policy.name, "Topic already exists");

            // Log audit entry
            let mut trail = self.audit_trail.write().await;
            trail.add(
                PolicyAuditEntry::new(PolicyChangeType::Update, "topic", &policy.name)
                    .with_detail("Checked existing topic"),
            );

            Ok(Some(PolicyChange {
                resource_type: "topic".to_string(),
                resource_name: policy.name.clone(),
                change_type: "no_change".to_string(),
                details: "Topic already exists".to_string(),
            }))
        } else if policy.auto_create {
            // Auto-create the topic
            info!(topic = %policy.name, partitions = policy.partitions, "Creating topic");
            tm.create_topic(&policy.name, policy.partitions)?;

            // Log audit entry
            let mut trail = self.audit_trail.write().await;
            trail.add(
                PolicyAuditEntry::new(PolicyChangeType::Create, "topic", &policy.name)
                    .with_detail(format!("Created with {} partitions", policy.partitions)),
            );

            Ok(Some(PolicyChange {
                resource_type: "topic".to_string(),
                resource_name: policy.name.clone(),
                change_type: "create".to_string(),
                details: format!("Created with {} partitions", policy.partitions),
            }))
        } else {
            debug!(topic = %policy.name, "Topic does not exist and auto_create is false");
            Ok(None)
        }
    }

    /// Apply a single ACL policy
    async fn apply_acl_policy(&self, policy: &AclPolicy) -> Result<Option<PolicyChange>> {
        // ACL application would integrate with auth module
        // For now, log the intent
        debug!(
            principal = %policy.principal,
            resource = %policy.resource_name,
            "Would apply ACL policy"
        );

        let mut trail = self.audit_trail.write().await;
        trail.add(
            PolicyAuditEntry::new(
                PolicyChangeType::Create,
                "acl",
                format!(
                    "{}:{}:{}",
                    policy.principal, policy.resource_type, policy.resource_name
                ),
            )
            .with_detail(format!("Operations: {:?}", policy.operations)),
        );

        Ok(Some(PolicyChange {
            resource_type: "acl".to_string(),
            resource_name: format!("{}:{}", policy.principal, policy.resource_name),
            change_type: "apply".to_string(),
            details: format!(
                "Operations: {:?}, Permission: {}",
                policy.operations, policy.permission
            ),
        }))
    }

    /// Apply a single quota policy
    async fn apply_quota_policy(&self, policy: &QuotaPolicy) -> Result<Option<PolicyChange>> {
        // Quota application would integrate with quota manager
        debug!(
            entity = %policy.entity,
            "Would apply quota policy"
        );

        let mut trail = self.audit_trail.write().await;
        trail.add(
            PolicyAuditEntry::new(PolicyChangeType::Create, "quota", &policy.entity).with_detail(
                format!(
                    "Producer: {:?}, Consumer: {:?}",
                    policy.producer_byte_rate, policy.consumer_byte_rate
                ),
            ),
        );

        Ok(Some(PolicyChange {
            resource_type: "quota".to_string(),
            resource_name: policy.entity.clone(),
            change_type: "apply".to_string(),
            details: format!(
                "Producer rate: {:?}, Consumer rate: {:?}",
                policy.producer_byte_rate, policy.consumer_byte_rate
            ),
        }))
    }

    /// Compute what changes would be made without applying them
    async fn compute_changes(&self, policy: &PolicyDocument) -> Result<Vec<PolicyChange>> {
        let mut changes = Vec::new();

        for topic in &policy.topics {
            if let Some(ref tm) = self.topic_manager {
                let exists = tm.get_topic_metadata(&topic.name).is_ok();
                if !exists && topic.auto_create {
                    changes.push(PolicyChange {
                        resource_type: "topic".to_string(),
                        resource_name: topic.name.clone(),
                        change_type: "create".to_string(),
                        details: format!("Would create with {} partitions", topic.partitions),
                    });
                }
            }
        }

        for acl in &policy.acls {
            changes.push(PolicyChange {
                resource_type: "acl".to_string(),
                resource_name: format!("{}:{}", acl.principal, acl.resource_name),
                change_type: "apply".to_string(),
                details: format!("Would set operations: {:?}", acl.operations),
            });
        }

        for quota in &policy.quotas {
            changes.push(PolicyChange {
                resource_type: "quota".to_string(),
                resource_name: quota.entity.clone(),
                change_type: "apply".to_string(),
                details: format!(
                    "Would set producer: {:?}, consumer: {:?}",
                    quota.producer_byte_rate, quota.consumer_byte_rate
                ),
            });
        }

        Ok(changes)
    }

    /// Get the current policy document
    pub async fn current_policy(&self) -> PolicyDocument {
        self.current_policy.read().await.clone()
    }

    /// Get the audit trail
    pub async fn audit_trail(&self) -> PolicyAuditLog {
        self.audit_trail.read().await.to_log()
    }

    /// Get engine statistics
    pub async fn stats(&self) -> PolicyEngineStat {
        self.stats.read().await.clone()
    }

    /// Clear the audit trail
    pub async fn clear_audit_trail(&self) {
        let mut trail = self.audit_trail.write().await;
        trail.clear();
    }

    /// Export the current policy to YAML
    pub async fn export_yaml(&self) -> Result<String> {
        let policy = self.current_policy.read().await;
        serde_yaml::to_string(&*policy)
            .map_err(|e| StreamlineError::Config(format!("Failed to export policy: {}", e)))
    }

    /// Export the current policy to JSON
    pub async fn export_json(&self) -> Result<String> {
        let policy = self.current_policy.read().await;
        serde_json::to_string_pretty(&*policy)
            .map_err(|e| StreamlineError::Config(format!("Failed to export policy: {}", e)))
    }
}

impl Default for PolicyEngine {
    fn default() -> Self {
        Self::new()
    }
}

/// Result of applying policies
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PolicyApplyResult {
    /// Whether the apply was successful
    pub success: bool,
    /// Whether this was a dry run
    pub dry_run: bool,
    /// Changes made (or would be made in dry run)
    pub changes: Vec<PolicyChange>,
    /// Errors encountered
    pub errors: Vec<String>,
}

/// A single policy change
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyChange {
    /// Type of resource (topic, acl, quota, schema)
    pub resource_type: String,
    /// Name of the resource
    pub resource_name: String,
    /// Type of change (create, update, delete)
    pub change_type: String,
    /// Details about the change
    pub details: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_policy_engine_creation() {
        let engine = PolicyEngine::new();
        let policy = engine.current_policy().await;
        assert!(policy.topics.is_empty());
    }

    #[tokio::test]
    async fn test_load_inline_policy() {
        let engine = PolicyEngine::new();

        let yaml = r#"
version: "1.0"
metadata:
  name: test
topics:
  - name: events
    partitions: 3
"#;

        let result = engine
            .load(PolicySource::Inline(yaml.to_string()))
            .await
            .unwrap();
        assert!(result.valid);

        let policy = engine.current_policy().await;
        assert_eq!(policy.topics.len(), 1);
        assert_eq!(policy.topics[0].name, "events");
    }

    #[tokio::test]
    async fn test_dry_run_apply() {
        let engine = PolicyEngine::new().with_dry_run(true);

        let yaml = r#"
topics:
  - name: events
    partitions: 3
    auto_create: true
acls:
  - principal: "User:alice"
    resource_type: topic
    resource_name: events
    operations: [read]
"#;

        engine
            .load(PolicySource::Inline(yaml.to_string()))
            .await
            .unwrap();
        let result = engine.apply().await.unwrap();

        assert!(result.dry_run);
        assert!(!result.changes.is_empty());
    }

    #[tokio::test]
    async fn test_export_yaml() {
        let engine = PolicyEngine::new();

        let yaml = r#"
metadata:
  name: export-test
topics:
  - name: events
    partitions: 6
"#;

        engine
            .load(PolicySource::Inline(yaml.to_string()))
            .await
            .unwrap();
        let exported = engine.export_yaml().await.unwrap();

        assert!(exported.contains("events"));
        assert!(exported.contains("export-test"));
    }

    #[tokio::test]
    async fn test_stats() {
        let engine = PolicyEngine::new();

        let yaml = r#"
topics:
  - name: events
    partitions: 3
"#;

        let result = engine
            .load(PolicySource::Inline(yaml.to_string()))
            .await
            .unwrap();
        assert!(result.valid);

        let stats = engine.stats().await;
        assert_eq!(stats.policies_loaded, 1);
        assert!(stats.last_validation_valid);
    }
}
