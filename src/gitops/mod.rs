//! GitOps Configuration Module
//!
//! Provides declarative infrastructure-as-code support for Streamline:
//! - YAML/JSON configuration for topics, ACLs, schemas, sinks
//! - Drift detection and reconciliation
//! - Kubernetes CRD integration
//! - Terraform provider support
//! - OPA policy-as-code integration
//! - Argo CD webhook support
//!
//! # Example
//!
//! ```yaml
//! apiVersion: streamline.io/v1
//! kind: StreamlineConfig
//! metadata:
//!   name: production-config
//! spec:
//!   topics:
//!     - name: events
//!       partitions: 6
//!       replication_factor: 3
//!       config:
//!         retention.ms: 604800000
//!         cleanup.policy: delete
//!     - name: users
//!       partitions: 3
//!       config:
//!         cleanup.policy: compact
//!   acls:
//!     - principal: User:app-producer
//!       resource_type: topic
//!       resource_name: events
//!       operation: write
//!       permission: allow
//! ```

pub mod config;
pub mod crd;
pub mod planner;
pub mod policy;
pub mod reconciler;

pub use config::{
    AclSpec, ConfigManifest, GitOpsConfig, ManifestKind, ManifestMetadata, SchemaSpec, SinkSpec,
    StreamlineConfigSpec, TopicSpec,
};
pub use crd::{
    CrdGenerator, CustomResourceDefinition, StreamlineCluster, StreamlineSink, StreamlineTopic,
};
pub use planner::{
    AclInfo, ChangeAction, ChangeImpact, ExecutionPlan, FieldDiff, GitOpsPlanner, PlanFormatter,
    PlanSummary, PlannedChange, ResourceType, TopicInfo,
};
pub use policy::{
    ArgoCdHandler, ArgoCdSyncEvent, ArgoCdSyncPhase, ArgoCdSyncStatus, BuiltinPolicies, OpaPolicy,
    PolicyDecision, PolicyEngine, PolicyEngineConfig, PolicyEvaluationResult, PolicySeverity,
    PolicyViolation,
};
pub use reconciler::{
    DriftDetector, DriftResult, DriftType, ReconcileResult, ReconcileStatus, Reconciler,
};

use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;

/// GitOps manager - coordinates all GitOps operations
pub struct GitOpsManager {
    /// Configuration
    config: GitOpsConfig,
    /// Loaded manifests
    manifests: Arc<RwLock<HashMap<String, ConfigManifest>>>,
    /// Reconciler
    reconciler: Arc<Reconciler>,
    /// Drift detector
    drift_detector: Arc<DriftDetector>,
}

impl GitOpsManager {
    /// Create a new GitOps manager
    pub fn new(config: GitOpsConfig) -> Self {
        Self {
            config: config.clone(),
            manifests: Arc::new(RwLock::new(HashMap::new())),
            reconciler: Arc::new(Reconciler::new(config.reconcile.clone())),
            drift_detector: Arc::new(DriftDetector::new()),
        }
    }

    /// Load a manifest from YAML string
    pub async fn load_yaml(&self, yaml: &str) -> Result<ConfigManifest> {
        let manifest: ConfigManifest = serde_yaml::from_str(yaml).map_err(|e| {
            StreamlineError::Config(format!("Failed to parse YAML manifest: {}", e))
        })?;

        self.register_manifest(manifest.clone()).await?;
        Ok(manifest)
    }

    /// Load a manifest from JSON string
    pub async fn load_json(&self, json: &str) -> Result<ConfigManifest> {
        let manifest: ConfigManifest = serde_json::from_str(json).map_err(|e| {
            StreamlineError::Config(format!("Failed to parse JSON manifest: {}", e))
        })?;

        self.register_manifest(manifest.clone()).await?;
        Ok(manifest)
    }

    /// Load manifests from a directory
    pub async fn load_directory(&self, path: &Path) -> Result<Vec<ConfigManifest>> {
        let mut manifests = Vec::new();

        if !path.exists() || !path.is_dir() {
            return Err(StreamlineError::Config(format!(
                "Directory does not exist: {:?}",
                path
            )));
        }

        for entry in std::fs::read_dir(path)
            .map_err(|e| StreamlineError::Config(format!("Failed to read directory: {}", e)))?
        {
            let entry = entry
                .map_err(|e| StreamlineError::Config(format!("Failed to read entry: {}", e)))?;
            let file_path = entry.path();

            if file_path.is_file() {
                let extension = file_path.extension().and_then(|e| e.to_str());
                match extension {
                    Some("yaml") | Some("yml") => {
                        let content = std::fs::read_to_string(&file_path).map_err(|e| {
                            StreamlineError::Config(format!("Failed to read file: {}", e))
                        })?;
                        let manifest = self.load_yaml(&content).await?;
                        manifests.push(manifest);
                    }
                    Some("json") => {
                        let content = std::fs::read_to_string(&file_path).map_err(|e| {
                            StreamlineError::Config(format!("Failed to read file: {}", e))
                        })?;
                        let manifest = self.load_json(&content).await?;
                        manifests.push(manifest);
                    }
                    _ => continue,
                }
            }
        }

        Ok(manifests)
    }

    /// Register a manifest
    async fn register_manifest(&self, manifest: ConfigManifest) -> Result<()> {
        let key = format!("{}/{}", manifest.metadata.namespace, manifest.metadata.name);
        let mut manifests = self.manifests.write().await;
        manifests.insert(key, manifest);
        Ok(())
    }

    /// Get a manifest by name
    pub async fn get_manifest(&self, namespace: &str, name: &str) -> Option<ConfigManifest> {
        let key = format!("{}/{}", namespace, name);
        let manifests = self.manifests.read().await;
        manifests.get(&key).cloned()
    }

    /// List all manifests
    pub async fn list_manifests(&self) -> Vec<ConfigManifest> {
        let manifests = self.manifests.read().await;
        manifests.values().cloned().collect()
    }

    /// Detect drift between manifests and current state
    pub async fn detect_drift(&self) -> Result<Vec<DriftResult>> {
        let manifests = self.manifests.read().await;
        let mut drifts = Vec::new();

        for manifest in manifests.values() {
            let drift = self.drift_detector.detect(manifest).await?;
            if !drift.is_empty() {
                drifts.extend(drift);
            }
        }

        Ok(drifts)
    }

    /// Reconcile manifests with current state
    pub async fn reconcile(&self) -> Result<Vec<ReconcileResult>> {
        if !self.config.reconcile.enabled {
            return Err(StreamlineError::Config("Reconciliation is disabled".into()));
        }

        let manifests = self.manifests.read().await;
        let mut results = Vec::new();

        for manifest in manifests.values() {
            let result = self.reconciler.reconcile(manifest).await?;
            results.push(result);
        }

        Ok(results)
    }

    /// Apply a manifest immediately
    pub async fn apply(&self, manifest: &ConfigManifest) -> Result<ReconcileResult> {
        self.reconciler.reconcile(manifest).await
    }

    /// Export current state to YAML
    pub async fn export_yaml(&self) -> Result<String> {
        let manifests = self.list_manifests().await;
        let yaml = serde_yaml::to_string(&manifests)
            .map_err(|e| StreamlineError::Config(format!("Failed to export to YAML: {}", e)))?;
        Ok(yaml)
    }

    /// Export current state to JSON
    pub async fn export_json(&self) -> Result<String> {
        let manifests = self.list_manifests().await;
        let json = serde_json::to_string_pretty(&manifests)
            .map_err(|e| StreamlineError::Config(format!("Failed to export to JSON: {}", e)))?;
        Ok(json)
    }

    /// Validate a manifest without applying
    pub fn validate(&self, manifest: &ConfigManifest) -> Result<ValidationResult> {
        let mut errors = Vec::new();
        let mut warnings = Vec::new();

        // Validate metadata
        if manifest.metadata.name.is_empty() {
            errors.push("Manifest name is required".to_string());
        }

        // Validate based on kind
        if let Some(spec) = &manifest.spec {
            // Validate topics
            for topic in &spec.topics {
                if topic.name.is_empty() {
                    errors.push("Topic name is required".to_string());
                }
                if topic.partitions == 0 {
                    errors.push(format!(
                        "Topic '{}' must have at least 1 partition",
                        topic.name
                    ));
                }
                if topic.partitions > 1000 {
                    warnings.push(format!(
                        "Topic '{}' has {} partitions, which may impact performance",
                        topic.name, topic.partitions
                    ));
                }
            }

            // Validate ACLs
            for acl in &spec.acls {
                if acl.principal.is_empty() {
                    errors.push("ACL principal is required".to_string());
                }
                if acl.resource_name.is_empty() && acl.resource_pattern != Some("*".to_string()) {
                    errors.push("ACL resource_name is required (or use pattern '*')".to_string());
                }
            }
        }

        Ok(ValidationResult {
            valid: errors.is_empty(),
            errors,
            warnings,
        })
    }

    /// Get configuration
    pub fn config(&self) -> &GitOpsConfig {
        &self.config
    }
}

/// Validation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationResult {
    /// Is the manifest valid
    pub valid: bool,
    /// Validation errors
    pub errors: Vec<String>,
    /// Validation warnings
    pub warnings: Vec<String>,
}

impl Default for GitOpsManager {
    fn default() -> Self {
        Self::new(GitOpsConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_YAML: &str = r#"
apiVersion: streamline.io/v1
kind: StreamlineConfig
metadata:
  name: test-config
  namespace: default
spec:
  topics:
    - name: events
      partitions: 3
      replication_factor: 1
      config:
        retention.ms: "604800000"
  acls:
    - principal: User:app
      resource_type: topic
      resource_name: events
      operation: write
      permission: allow
"#;

    #[tokio::test]
    async fn test_load_yaml() {
        let manager = GitOpsManager::default();
        let manifest = manager.load_yaml(SAMPLE_YAML).await.unwrap();

        assert_eq!(manifest.metadata.name, "test-config");
        assert_eq!(manifest.spec.as_ref().unwrap().topics.len(), 1);
        assert_eq!(manifest.spec.as_ref().unwrap().topics[0].name, "events");
    }

    #[test]
    fn test_validate_manifest() {
        let manager = GitOpsManager::default();

        let manifest: ConfigManifest = serde_yaml::from_str(SAMPLE_YAML).unwrap();
        let result = manager.validate(&manifest).unwrap();

        assert!(result.valid);
        assert!(result.errors.is_empty());
    }

    #[test]
    fn test_validate_invalid_manifest() {
        let manager = GitOpsManager::default();

        let invalid_yaml = r#"
apiVersion: streamline.io/v1
kind: StreamlineConfig
metadata:
  name: ""
  namespace: default
spec:
  topics:
    - name: ""
      partitions: 0
"#;

        let manifest: ConfigManifest = serde_yaml::from_str(invalid_yaml).unwrap();
        let result = manager.validate(&manifest).unwrap();

        assert!(!result.valid);
        assert!(!result.errors.is_empty());
    }
}
