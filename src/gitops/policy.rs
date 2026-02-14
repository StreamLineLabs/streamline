//! OPA Policy Integration for GitOps
//!
//! Provides policy-as-code support using Open Policy Agent (OPA) for:
//! - Admission control for manifest changes
//! - Compliance validation
//! - Cost estimation rules
//! - Security policy enforcement

use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Policy decision result
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PolicyDecision {
    /// Policy allows the action
    Allow,
    /// Policy denies the action
    Deny,
    /// Policy warns but allows
    Warn,
}

/// Policy violation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyViolation {
    /// Policy ID
    pub policy_id: String,
    /// Policy name
    pub policy_name: String,
    /// Violation message
    pub message: String,
    /// Severity level
    pub severity: PolicySeverity,
    /// Resource path that violated the policy
    pub resource_path: String,
    /// Suggested fix
    pub suggestion: Option<String>,
}

/// Policy severity
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PolicySeverity {
    /// Critical - blocks deployment
    Critical,
    /// High - blocks deployment
    High,
    /// Medium - warning
    Medium,
    /// Low - informational
    Low,
}

impl PolicySeverity {
    /// Check if this severity blocks deployment
    pub fn is_blocking(&self) -> bool {
        matches!(self, PolicySeverity::Critical | PolicySeverity::High)
    }
}

/// Policy evaluation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyEvaluationResult {
    /// Overall decision
    pub decision: PolicyDecision,
    /// List of violations
    pub violations: Vec<PolicyViolation>,
    /// Evaluation duration in milliseconds
    pub duration_ms: u64,
    /// Policies evaluated
    pub policies_evaluated: usize,
}

/// OPA policy rule
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpaPolicy {
    /// Policy ID
    pub id: String,
    /// Policy name
    pub name: String,
    /// Policy description
    pub description: String,
    /// Rego code for the policy
    pub rego: String,
    /// Severity
    pub severity: PolicySeverity,
    /// Is the policy enabled
    pub enabled: bool,
    /// Target resource types (empty = all)
    pub target_resources: Vec<String>,
}

impl Default for OpaPolicy {
    fn default() -> Self {
        Self {
            id: String::new(),
            name: String::new(),
            description: String::new(),
            rego: String::new(),
            severity: PolicySeverity::Medium,
            enabled: true,
            target_resources: Vec::new(),
        }
    }
}

/// Built-in policies
pub struct BuiltinPolicies;

impl BuiltinPolicies {
    /// Get all built-in policies
    pub fn all() -> Vec<OpaPolicy> {
        vec![
            Self::require_replication(),
            Self::max_partitions(),
            Self::require_retention(),
            Self::naming_convention(),
            Self::resource_limits(),
            Self::security_require_tls(),
            Self::cost_estimation(),
        ]
    }

    /// Policy: Require replication factor >= 2 for production
    pub fn require_replication() -> OpaPolicy {
        OpaPolicy {
            id: "streamline.topic.replication".to_string(),
            name: "Require Replication".to_string(),
            description: "Topics must have replication factor >= 2 in production".to_string(),
            rego: r#"
package streamline.topic.replication

default allow = false

allow {
    input.spec.replicationFactor >= 2
}

allow {
    input.metadata.labels["environment"] == "development"
}

violation[msg] {
    input.spec.replicationFactor < 2
    input.metadata.labels["environment"] != "development"
    msg := sprintf("Topic '%s' has replication factor %d, must be >= 2 in production", 
                   [input.metadata.name, input.spec.replicationFactor])
}
"#
            .to_string(),
            severity: PolicySeverity::High,
            enabled: true,
            target_resources: vec!["StreamlineTopic".to_string()],
        }
    }

    /// Policy: Maximum partitions limit
    pub fn max_partitions() -> OpaPolicy {
        OpaPolicy {
            id: "streamline.topic.max-partitions".to_string(),
            name: "Maximum Partitions".to_string(),
            description: "Topics cannot exceed maximum partition limit".to_string(),
            rego: r#"
package streamline.topic.max_partitions

max_partitions := 100

default allow = true

deny[msg] {
    input.spec.partitions > max_partitions
    msg := sprintf("Topic '%s' has %d partitions, maximum allowed is %d",
                   [input.metadata.name, input.spec.partitions, max_partitions])
}
"#
            .to_string(),
            severity: PolicySeverity::Medium,
            enabled: true,
            target_resources: vec!["StreamlineTopic".to_string()],
        }
    }

    /// Policy: Require retention configuration
    pub fn require_retention() -> OpaPolicy {
        OpaPolicy {
            id: "streamline.topic.retention".to_string(),
            name: "Require Retention Policy".to_string(),
            description: "Topics must have retention.ms configured".to_string(),
            rego: r#"
package streamline.topic.retention

default allow = true

warn[msg] {
    not input.spec.config["retention.ms"]
    msg := sprintf("Topic '%s' has no retention.ms configured", [input.metadata.name])
}
"#
            .to_string(),
            severity: PolicySeverity::Low,
            enabled: true,
            target_resources: vec!["StreamlineTopic".to_string()],
        }
    }

    /// Policy: Naming convention enforcement
    pub fn naming_convention() -> OpaPolicy {
        OpaPolicy {
            id: "streamline.naming.convention".to_string(),
            name: "Naming Convention".to_string(),
            description: "Resources must follow naming convention".to_string(),
            rego: r#"
package streamline.naming.convention

import future.keywords.in

# Valid characters: lowercase letters, numbers, hyphens
valid_name(name) {
    regex.match("^[a-z][a-z0-9-]*[a-z0-9]$", name)
}

valid_name(name) {
    count(name) == 1
    regex.match("^[a-z]$", name)
}

default allow = false

allow {
    valid_name(input.metadata.name)
}

violation[msg] {
    not valid_name(input.metadata.name)
    msg := sprintf("Resource name '%s' violates naming convention (must be lowercase, alphanumeric with hyphens)",
                   [input.metadata.name])
}
"#.to_string(),
            severity: PolicySeverity::Medium,
            enabled: true,
            target_resources: vec![],
        }
    }

    /// Policy: Resource limits
    pub fn resource_limits() -> OpaPolicy {
        OpaPolicy {
            id: "streamline.cluster.resources".to_string(),
            name: "Resource Limits".to_string(),
            description: "Clusters must have resource limits defined".to_string(),
            rego: r#"
package streamline.cluster.resources

default allow = true

deny[msg] {
    input.kind == "StreamlineCluster"
    not input.spec.resources
    msg := sprintf("Cluster '%s' must have resource limits defined", [input.metadata.name])
}

deny[msg] {
    input.kind == "StreamlineCluster"
    not input.spec.resources.memory
    msg := sprintf("Cluster '%s' must have memory limit defined", [input.metadata.name])
}
"#
            .to_string(),
            severity: PolicySeverity::High,
            enabled: true,
            target_resources: vec!["StreamlineCluster".to_string()],
        }
    }

    /// Policy: Security - require TLS
    pub fn security_require_tls() -> OpaPolicy {
        OpaPolicy {
            id: "streamline.security.tls".to_string(),
            name: "Require TLS".to_string(),
            description: "Production clusters must have TLS enabled".to_string(),
            rego: r#"
package streamline.security.tls

default allow = false

allow {
    input.spec.tls.enabled == true
}

allow {
    input.metadata.labels["environment"] == "development"
}

allow {
    input.metadata.labels["environment"] == "test"
}

violation[msg] {
    input.kind == "StreamlineCluster"
    not input.spec.tls.enabled
    env := input.metadata.labels["environment"]
    env != "development"
    env != "test"
    msg := sprintf("Cluster '%s' must have TLS enabled in production", [input.metadata.name])
}
"#
            .to_string(),
            severity: PolicySeverity::Critical,
            enabled: true,
            target_resources: vec!["StreamlineCluster".to_string()],
        }
    }

    /// Policy: Cost estimation
    pub fn cost_estimation() -> OpaPolicy {
        OpaPolicy {
            id: "streamline.cost.estimation".to_string(),
            name: "Cost Estimation".to_string(),
            description: "Warn when estimated monthly cost exceeds threshold".to_string(),
            rego: r#"
package streamline.cost.estimation

# Cost per partition per month (in cents)
cost_per_partition := 10

# Cost per replica per month (in cents)
cost_per_replica := 100

# Monthly cost threshold for warning (in cents)
cost_threshold := 10000

estimate_topic_cost(topic) = cost {
    partitions := topic.spec.partitions
    replicas := topic.spec.replicationFactor
    cost := (partitions * cost_per_partition) + (replicas * cost_per_replica)
}

default allow = true

warn[msg] {
    cost := estimate_topic_cost(input)
    cost > cost_threshold
    msg := sprintf("Topic '%s' estimated monthly cost $%.2f exceeds threshold $%.2f",
                   [input.metadata.name, cost / 100, cost_threshold / 100])
}
"#
            .to_string(),
            severity: PolicySeverity::Low,
            enabled: true,
            target_resources: vec!["StreamlineTopic".to_string()],
        }
    }
}

/// Policy engine configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyEngineConfig {
    /// Enable policy evaluation
    pub enabled: bool,
    /// Fail on critical/high violations
    pub enforce: bool,
    /// Built-in policies to enable
    pub builtin_policies: Vec<String>,
    /// Custom policies directory
    pub policy_dir: Option<String>,
    /// OPA server URL (optional, for external OPA)
    pub opa_url: Option<String>,
}

impl Default for PolicyEngineConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            enforce: true,
            builtin_policies: vec![
                "streamline.topic.replication".to_string(),
                "streamline.naming.convention".to_string(),
            ],
            policy_dir: None,
            opa_url: None,
        }
    }
}

/// Policy engine for evaluating OPA policies
pub struct PolicyEngine {
    /// Configuration
    config: PolicyEngineConfig,
    /// Loaded policies
    policies: Arc<RwLock<HashMap<String, OpaPolicy>>>,
}

impl PolicyEngine {
    /// Create a new policy engine
    pub fn new(config: PolicyEngineConfig) -> Self {
        let engine = Self {
            config: config.clone(),
            policies: Arc::new(RwLock::new(HashMap::new())),
        };

        // Load built-in policies synchronously during construction
        let builtin = BuiltinPolicies::all();
        let mut policies = HashMap::new();
        for policy in builtin {
            if config.builtin_policies.contains(&policy.id) || config.builtin_policies.is_empty() {
                policies.insert(policy.id.clone(), policy);
            }
        }

        // Replace the policies
        let policies_lock = engine.policies.clone();
        std::thread::spawn(move || {
            if let Ok(rt) = tokio::runtime::Runtime::new() {
                rt.block_on(async {
                    let mut lock = policies_lock.write().await;
                    *lock = policies;
                });
            }
        })
        .join()
        .ok();

        engine
    }

    /// Load built-in policies
    pub async fn load_builtin_policies(&self) -> Result<()> {
        let builtin = BuiltinPolicies::all();
        let mut policies = self.policies.write().await;

        for policy in builtin {
            if self.config.builtin_policies.contains(&policy.id)
                || self.config.builtin_policies.is_empty()
            {
                policies.insert(policy.id.clone(), policy);
            }
        }

        Ok(())
    }

    /// Add a custom policy
    pub async fn add_policy(&self, policy: OpaPolicy) -> Result<()> {
        let mut policies = self.policies.write().await;
        policies.insert(policy.id.clone(), policy);
        Ok(())
    }

    /// Remove a policy
    pub async fn remove_policy(&self, policy_id: &str) -> Result<()> {
        let mut policies = self.policies.write().await;
        policies.remove(policy_id);
        Ok(())
    }

    /// List all policies
    pub async fn list_policies(&self) -> Vec<OpaPolicy> {
        let policies = self.policies.read().await;
        policies.values().cloned().collect()
    }

    /// Evaluate policies against a resource
    pub async fn evaluate<T: Serialize>(&self, resource: &T) -> Result<PolicyEvaluationResult> {
        if !self.config.enabled {
            return Ok(PolicyEvaluationResult {
                decision: PolicyDecision::Allow,
                violations: Vec::new(),
                duration_ms: 0,
                policies_evaluated: 0,
            });
        }

        let start = std::time::Instant::now();
        let policies = self.policies.read().await;
        let mut violations = Vec::new();

        let resource_json = serde_json::to_value(resource)
            .map_err(|e| StreamlineError::Config(format!("Failed to serialize resource: {}", e)))?;

        let resource_kind = resource_json
            .get("kind")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        for policy in policies.values() {
            if !policy.enabled {
                continue;
            }

            // Check if policy applies to this resource type
            if !policy.target_resources.is_empty()
                && !policy.target_resources.contains(&resource_kind.to_string())
            {
                continue;
            }

            // Evaluate the policy (simplified - real implementation would use OPA)
            if let Some(violation) = self.evaluate_policy(policy, &resource_json).await? {
                violations.push(violation);
            }
        }

        let decision = if violations.iter().any(|v| v.severity.is_blocking()) {
            PolicyDecision::Deny
        } else if !violations.is_empty() {
            PolicyDecision::Warn
        } else {
            PolicyDecision::Allow
        };

        Ok(PolicyEvaluationResult {
            decision,
            violations,
            duration_ms: start.elapsed().as_millis() as u64,
            policies_evaluated: policies.len(),
        })
    }

    /// Evaluate a single policy against a resource (simplified implementation)
    async fn evaluate_policy(
        &self,
        policy: &OpaPolicy,
        resource: &serde_json::Value,
    ) -> Result<Option<PolicyViolation>> {
        // This is a simplified evaluation - in production, we would:
        // 1. Use actual OPA/Rego evaluation (via opa-wasm or external OPA server)
        // 2. Parse and evaluate the Rego code properly
        //
        // For now, we do basic checks based on policy ID

        let metadata = resource.get("metadata").unwrap_or(&serde_json::Value::Null);
        let spec = resource.get("spec").unwrap_or(&serde_json::Value::Null);
        let name = metadata
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        match policy.id.as_str() {
            "streamline.topic.replication" => {
                let replication = spec
                    .get("replicationFactor")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(1);
                let env = metadata
                    .get("labels")
                    .and_then(|l| l.get("environment"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("production");

                if replication < 2 && env != "development" {
                    return Ok(Some(PolicyViolation {
                        policy_id: policy.id.clone(),
                        policy_name: policy.name.clone(),
                        message: format!(
                            "Topic '{}' has replication factor {}, must be >= 2 in production",
                            name, replication
                        ),
                        severity: policy.severity,
                        resource_path: "spec.replicationFactor".to_string(),
                        suggestion: Some("Set replicationFactor to at least 2".to_string()),
                    }));
                }
            }

            "streamline.topic.max-partitions" => {
                let partitions = spec.get("partitions").and_then(|v| v.as_i64()).unwrap_or(1);

                if partitions > 100 {
                    return Ok(Some(PolicyViolation {
                        policy_id: policy.id.clone(),
                        policy_name: policy.name.clone(),
                        message: format!(
                            "Topic '{}' has {} partitions, maximum allowed is 100",
                            name, partitions
                        ),
                        severity: policy.severity,
                        resource_path: "spec.partitions".to_string(),
                        suggestion: Some("Reduce partition count to 100 or less".to_string()),
                    }));
                }
            }

            "streamline.naming.convention" => {
                let name_pattern = regex::Regex::new(r"^[a-z][a-z0-9-]*[a-z0-9]$|^[a-z]$")
                    .map_err(|e| StreamlineError::Config(format!("Invalid regex: {}", e)))?;
                if !name_pattern.is_match(name) {
                    return Ok(Some(PolicyViolation {
                        policy_id: policy.id.clone(),
                        policy_name: policy.name.clone(),
                        message: format!("Resource name '{}' violates naming convention", name),
                        severity: policy.severity,
                        resource_path: "metadata.name".to_string(),
                        suggestion: Some(
                            "Use lowercase alphanumeric characters and hyphens".to_string(),
                        ),
                    }));
                }
            }

            "streamline.security.tls" => {
                let tls_enabled = spec
                    .get("tls")
                    .and_then(|t| t.get("enabled"))
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                let env = metadata
                    .get("labels")
                    .and_then(|l| l.get("environment"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("production");

                if !tls_enabled && env != "development" && env != "test" {
                    return Ok(Some(PolicyViolation {
                        policy_id: policy.id.clone(),
                        policy_name: policy.name.clone(),
                        message: format!("Cluster '{}' must have TLS enabled in production", name),
                        severity: policy.severity,
                        resource_path: "spec.tls.enabled".to_string(),
                        suggestion: Some("Set spec.tls.enabled to true".to_string()),
                    }));
                }
            }

            _ => {}
        }

        Ok(None)
    }

    /// Check if policy enforcement is enabled
    pub fn is_enforcing(&self) -> bool {
        self.config.enforce
    }
}

impl Default for PolicyEngine {
    fn default() -> Self {
        Self::new(PolicyEngineConfig::default())
    }
}

/// Argo CD webhook handler for GitOps synchronization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArgoCdSyncEvent {
    /// Application name
    pub app_name: String,
    /// Sync phase
    pub phase: ArgoCdSyncPhase,
    /// Sync status
    pub status: ArgoCdSyncStatus,
    /// Revision
    pub revision: String,
    /// Sync started at
    pub started_at: Option<String>,
    /// Sync finished at
    pub finished_at: Option<String>,
}

/// Argo CD sync phase
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArgoCdSyncPhase {
    /// Pre-sync hook
    PreSync,
    /// Sync phase
    Sync,
    /// Post-sync hook
    PostSync,
    /// Sync wave
    SyncWave,
}

/// Argo CD sync status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArgoCdSyncStatus {
    /// Unknown
    Unknown,
    /// Running
    Running,
    /// Succeeded
    Succeeded,
    /// Failed
    Failed,
}

/// Argo CD integration handler
pub struct ArgoCdHandler {
    /// Policy engine for validation
    policy_engine: Arc<PolicyEngine>,
}

impl ArgoCdHandler {
    /// Create a new Argo CD handler
    pub fn new(policy_engine: Arc<PolicyEngine>) -> Self {
        Self { policy_engine }
    }

    /// Handle pre-sync hook - validate resources before sync
    pub async fn handle_pre_sync<T: Serialize>(
        &self,
        resources: &[T],
    ) -> Result<PolicyEvaluationResult> {
        let mut all_violations = Vec::new();
        let start = std::time::Instant::now();
        let mut policies_evaluated = 0;

        for resource in resources {
            let result = self.policy_engine.evaluate(resource).await?;
            all_violations.extend(result.violations);
            policies_evaluated = result.policies_evaluated;
        }

        let decision = if all_violations.iter().any(|v| v.severity.is_blocking()) {
            PolicyDecision::Deny
        } else if !all_violations.is_empty() {
            PolicyDecision::Warn
        } else {
            PolicyDecision::Allow
        };

        Ok(PolicyEvaluationResult {
            decision,
            violations: all_violations,
            duration_ms: start.elapsed().as_millis() as u64,
            policies_evaluated,
        })
    }

    /// Handle sync event
    pub async fn handle_sync_event(&self, event: &ArgoCdSyncEvent) -> Result<()> {
        tracing::info!(
            app = %event.app_name,
            phase = ?event.phase,
            status = ?event.status,
            revision = %event.revision,
            "Argo CD sync event received"
        );

        match event.phase {
            ArgoCdSyncPhase::PreSync => {
                tracing::info!(app = %event.app_name, "Running pre-sync validation");
            }
            ArgoCdSyncPhase::Sync => {
                tracing::info!(app = %event.app_name, "Sync in progress");
            }
            ArgoCdSyncPhase::PostSync => {
                if event.status == ArgoCdSyncStatus::Succeeded {
                    tracing::info!(app = %event.app_name, "Post-sync completed successfully");
                } else {
                    tracing::warn!(app = %event.app_name, "Post-sync failed");
                }
            }
            ArgoCdSyncPhase::SyncWave => {
                tracing::debug!(app = %event.app_name, "Sync wave event");
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_policy_engine_creation() {
        let engine = PolicyEngine::default();
        let policies = engine.list_policies().await;
        assert!(!policies.is_empty());
    }

    #[tokio::test]
    async fn test_naming_convention_policy() {
        let config = PolicyEngineConfig {
            enabled: true,
            enforce: true,
            builtin_policies: vec!["streamline.naming.convention".to_string()],
            ..Default::default()
        };
        let engine = PolicyEngine::new(config);
        engine.load_builtin_policies().await.unwrap();

        // Valid name
        let valid_resource = serde_json::json!({
            "kind": "StreamlineTopic",
            "metadata": { "name": "valid-topic-name" },
            "spec": {}
        });
        let result = engine.evaluate(&valid_resource).await.unwrap();
        assert_eq!(result.decision, PolicyDecision::Allow);

        // Invalid name (uppercase)
        let invalid_resource = serde_json::json!({
            "kind": "StreamlineTopic",
            "metadata": { "name": "Invalid_Topic_Name" },
            "spec": {}
        });
        let result = engine.evaluate(&invalid_resource).await.unwrap();
        assert!(!result.violations.is_empty());
    }

    #[tokio::test]
    async fn test_replication_policy() {
        let config = PolicyEngineConfig {
            enabled: true,
            enforce: true,
            builtin_policies: vec!["streamline.topic.replication".to_string()],
            ..Default::default()
        };
        let engine = PolicyEngine::new(config);
        engine.load_builtin_policies().await.unwrap();

        // Production with low replication
        let resource = serde_json::json!({
            "kind": "StreamlineTopic",
            "metadata": {
                "name": "events",
                "labels": { "environment": "production" }
            },
            "spec": {
                "partitions": 3,
                "replicationFactor": 1
            }
        });
        let result = engine.evaluate(&resource).await.unwrap();
        assert_eq!(result.decision, PolicyDecision::Deny);

        // Development with low replication (allowed)
        let dev_resource = serde_json::json!({
            "kind": "StreamlineTopic",
            "metadata": {
                "name": "events",
                "labels": { "environment": "development" }
            },
            "spec": {
                "partitions": 3,
                "replicationFactor": 1
            }
        });
        let result = engine.evaluate(&dev_resource).await.unwrap();
        assert_eq!(result.decision, PolicyDecision::Allow);
    }

    #[test]
    fn test_builtin_policies() {
        let policies = BuiltinPolicies::all();
        assert!(!policies.is_empty());

        // Check that all policies have valid IDs
        for policy in &policies {
            assert!(!policy.id.is_empty());
            assert!(!policy.name.is_empty());
            assert!(!policy.rego.is_empty());
        }
    }
}
