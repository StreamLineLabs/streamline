//! GitOps Reconciliation Engine
//!
//! Detects drift between declared state (manifests) and actual state,
//! then reconciles them using a controller-based approach.

use super::config::{AclSpec, ConfigManifest, ReconcileConfig, SinkSpec, TopicSpec};
use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Drift type
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DriftType {
    /// Resource exists in manifest but not in cluster
    Missing,
    /// Resource exists in cluster but not in manifest
    Extra,
    /// Resource exists but configuration differs
    Modified,
}

/// Drift result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriftResult {
    /// Resource type
    pub resource_type: String,
    /// Resource name
    pub resource_name: String,
    /// Drift type
    pub drift_type: DriftType,
    /// Details about the drift
    pub details: String,
    /// Expected value (from manifest)
    pub expected: Option<String>,
    /// Actual value (from cluster)
    pub actual: Option<String>,
}

/// Drift detector
pub struct DriftDetector {
    /// Cache of last known state
    state_cache: Arc<RwLock<StateCache>>,
}

#[derive(Default)]
struct StateCache {
    topics: std::collections::HashMap<String, TopicState>,
    acls: std::collections::HashMap<String, AclState>,
    sinks: std::collections::HashMap<String, SinkState>,
}

#[derive(Clone)]
struct TopicState {
    partitions: u32,
    replication_factor: u16,
    config: std::collections::HashMap<String, String>,
}

#[derive(Clone)]
#[allow(dead_code)]
struct AclState {
    principal: String,
    resource_type: String,
    resource_name: String,
    operation: String,
    permission: String,
}

#[derive(Clone)]
#[allow(dead_code)]
struct SinkState {
    sink_type: String,
    topics: Vec<String>,
    running: bool,
}

impl DriftDetector {
    /// Create a new drift detector
    pub fn new() -> Self {
        Self {
            state_cache: Arc::new(RwLock::new(StateCache::default())),
        }
    }

    /// Detect drift between manifest and current state
    pub async fn detect(&self, manifest: &ConfigManifest) -> Result<Vec<DriftResult>> {
        let mut drifts = Vec::new();

        if let Some(spec) = &manifest.spec {
            // Check topics
            for topic in &spec.topics {
                let topic_drift = self.detect_topic_drift(topic).await?;
                drifts.extend(topic_drift);
            }

            // Check ACLs
            for acl in &spec.acls {
                let acl_drift = self.detect_acl_drift(acl).await?;
                drifts.extend(acl_drift);
            }

            // Check sinks
            for sink in &spec.sinks {
                let sink_drift = self.detect_sink_drift(sink).await?;
                drifts.extend(sink_drift);
            }
        }

        Ok(drifts)
    }

    /// Update the state cache with current cluster state
    pub async fn refresh_state(&self) -> Result<()> {
        // In production, this would query the actual cluster state
        // For now, we just clear the cache
        let mut cache = self.state_cache.write().await;
        *cache = StateCache::default();
        Ok(())
    }

    async fn detect_topic_drift(&self, topic: &TopicSpec) -> Result<Vec<DriftResult>> {
        let mut drifts = Vec::new();
        let cache = self.state_cache.read().await;

        if let Some(current) = cache.topics.get(&topic.name) {
            // Check partitions
            if current.partitions != topic.partitions {
                drifts.push(DriftResult {
                    resource_type: "Topic".to_string(),
                    resource_name: topic.name.clone(),
                    drift_type: DriftType::Modified,
                    details: "Partition count differs".to_string(),
                    expected: Some(topic.partitions.to_string()),
                    actual: Some(current.partitions.to_string()),
                });
            }

            // Check replication factor
            if current.replication_factor != topic.replication_factor {
                drifts.push(DriftResult {
                    resource_type: "Topic".to_string(),
                    resource_name: topic.name.clone(),
                    drift_type: DriftType::Modified,
                    details: "Replication factor differs".to_string(),
                    expected: Some(topic.replication_factor.to_string()),
                    actual: Some(current.replication_factor.to_string()),
                });
            }

            // Check config differences
            for (key, expected_val) in &topic.config {
                if let Some(actual_val) = current.config.get(key) {
                    if actual_val != expected_val {
                        drifts.push(DriftResult {
                            resource_type: "Topic".to_string(),
                            resource_name: topic.name.clone(),
                            drift_type: DriftType::Modified,
                            details: format!("Config '{}' differs", key),
                            expected: Some(expected_val.clone()),
                            actual: Some(actual_val.clone()),
                        });
                    }
                } else {
                    drifts.push(DriftResult {
                        resource_type: "Topic".to_string(),
                        resource_name: topic.name.clone(),
                        drift_type: DriftType::Modified,
                        details: format!("Config '{}' missing", key),
                        expected: Some(expected_val.clone()),
                        actual: None,
                    });
                }
            }
        } else {
            // Topic doesn't exist
            drifts.push(DriftResult {
                resource_type: "Topic".to_string(),
                resource_name: topic.name.clone(),
                drift_type: DriftType::Missing,
                details: "Topic does not exist".to_string(),
                expected: Some(format!(
                    "partitions={}, replication={}",
                    topic.partitions, topic.replication_factor
                )),
                actual: None,
            });
        }

        Ok(drifts)
    }

    async fn detect_acl_drift(&self, acl: &AclSpec) -> Result<Vec<DriftResult>> {
        let mut drifts = Vec::new();
        let cache = self.state_cache.read().await;

        let acl_key = format!(
            "{}:{}:{}:{}",
            acl.principal, acl.resource_type, acl.resource_name, acl.operation
        );

        if !cache.acls.contains_key(&acl_key) {
            drifts.push(DriftResult {
                resource_type: "ACL".to_string(),
                resource_name: acl_key,
                drift_type: DriftType::Missing,
                details: "ACL does not exist".to_string(),
                expected: Some(format!(
                    "{} {} on {}:{}",
                    acl.permission, acl.operation, acl.resource_type, acl.resource_name
                )),
                actual: None,
            });
        }

        Ok(drifts)
    }

    async fn detect_sink_drift(&self, sink: &SinkSpec) -> Result<Vec<DriftResult>> {
        let mut drifts = Vec::new();
        let cache = self.state_cache.read().await;

        if let Some(current) = cache.sinks.get(&sink.name) {
            // Check sink type
            if current.sink_type != sink.sink_type {
                drifts.push(DriftResult {
                    resource_type: "Sink".to_string(),
                    resource_name: sink.name.clone(),
                    drift_type: DriftType::Modified,
                    details: "Sink type differs".to_string(),
                    expected: Some(sink.sink_type.clone()),
                    actual: Some(current.sink_type.clone()),
                });
            }

            // Check topics
            if current.topics != sink.topics {
                drifts.push(DriftResult {
                    resource_type: "Sink".to_string(),
                    resource_name: sink.name.clone(),
                    drift_type: DriftType::Modified,
                    details: "Sink topics differ".to_string(),
                    expected: Some(sink.topics.join(", ")),
                    actual: Some(current.topics.join(", ")),
                });
            }
        } else if sink.enabled {
            drifts.push(DriftResult {
                resource_type: "Sink".to_string(),
                resource_name: sink.name.clone(),
                drift_type: DriftType::Missing,
                details: "Sink does not exist".to_string(),
                expected: Some(format!("{} -> {}", sink.topics.join(", "), sink.sink_type)),
                actual: None,
            });
        }

        Ok(drifts)
    }
}

impl Default for DriftDetector {
    fn default() -> Self {
        Self::new()
    }
}

/// Reconcile status
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReconcileStatus {
    /// Reconciliation succeeded
    Success,
    /// Reconciliation failed
    Failed,
    /// Reconciliation skipped (dry run)
    Skipped,
    /// Reconciliation pending
    Pending,
    /// No changes needed
    NoOp,
}

/// Reconcile result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReconcileResult {
    /// Manifest name
    pub manifest_name: String,
    /// Overall status
    pub status: ReconcileStatus,
    /// Resources created
    pub created: Vec<String>,
    /// Resources updated
    pub updated: Vec<String>,
    /// Resources deleted
    pub deleted: Vec<String>,
    /// Errors encountered
    pub errors: Vec<String>,
    /// Duration in milliseconds
    pub duration_ms: u64,
}

/// Reconciler - applies manifests to cluster
pub struct Reconciler {
    /// Configuration
    config: ReconcileConfig,
    /// Drift detector
    drift_detector: Arc<DriftDetector>,
}

impl Reconciler {
    /// Create a new reconciler
    pub fn new(config: ReconcileConfig) -> Self {
        Self {
            config,
            drift_detector: Arc::new(DriftDetector::new()),
        }
    }

    /// Reconcile a manifest
    pub async fn reconcile(&self, manifest: &ConfigManifest) -> Result<ReconcileResult> {
        let start = std::time::Instant::now();
        let manifest_name = format!("{}/{}", manifest.metadata.namespace, manifest.metadata.name);

        let mut result = ReconcileResult {
            manifest_name: manifest_name.clone(),
            status: ReconcileStatus::Pending,
            created: Vec::new(),
            updated: Vec::new(),
            deleted: Vec::new(),
            errors: Vec::new(),
            duration_ms: 0,
        };

        // Detect drift
        let drifts = self.drift_detector.detect(manifest).await?;

        if drifts.is_empty() {
            result.status = ReconcileStatus::NoOp;
            result.duration_ms = start.elapsed().as_millis() as u64;
            return Ok(result);
        }

        // If dry run, skip actual reconciliation
        if self.config.dry_run {
            result.status = ReconcileStatus::Skipped;
            for drift in &drifts {
                match drift.drift_type {
                    DriftType::Missing => {
                        result.created.push(format!(
                            "{}: {} (would create)",
                            drift.resource_type, drift.resource_name
                        ));
                    }
                    DriftType::Modified => {
                        result.updated.push(format!(
                            "{}: {} (would update)",
                            drift.resource_type, drift.resource_name
                        ));
                    }
                    DriftType::Extra => {
                        result.deleted.push(format!(
                            "{}: {} (would delete)",
                            drift.resource_type, drift.resource_name
                        ));
                    }
                }
            }
            result.duration_ms = start.elapsed().as_millis() as u64;
            return Ok(result);
        }

        // Apply changes
        for drift in &drifts {
            match self.apply_drift(drift, manifest).await {
                Ok(applied) => {
                    if applied {
                        match drift.drift_type {
                            DriftType::Missing => {
                                result.created.push(format!(
                                    "{}: {}",
                                    drift.resource_type, drift.resource_name
                                ));
                            }
                            DriftType::Modified => {
                                result.updated.push(format!(
                                    "{}: {}",
                                    drift.resource_type, drift.resource_name
                                ));
                            }
                            DriftType::Extra => {
                                result.deleted.push(format!(
                                    "{}: {}",
                                    drift.resource_type, drift.resource_name
                                ));
                            }
                        }
                    }
                }
                Err(e) => {
                    result.errors.push(format!(
                        "{}: {} - {}",
                        drift.resource_type, drift.resource_name, e
                    ));
                }
            }
        }

        // Set final status
        result.status = if result.errors.is_empty() {
            ReconcileStatus::Success
        } else {
            ReconcileStatus::Failed
        };

        result.duration_ms = start.elapsed().as_millis() as u64;
        Ok(result)
    }

    /// Reconcile with retries
    pub async fn reconcile_with_retry(&self, manifest: &ConfigManifest) -> Result<ReconcileResult> {
        let mut last_result = None;

        for attempt in 0..=self.config.max_retries {
            let result = self.reconcile(manifest).await?;

            if result.status == ReconcileStatus::Success
                || result.status == ReconcileStatus::NoOp
                || result.status == ReconcileStatus::Skipped
            {
                return Ok(result);
            }

            last_result = Some(result);

            if attempt < self.config.max_retries && self.config.retry_on_failure {
                tokio::time::sleep(std::time::Duration::from_secs(
                    self.config.retry_delay_seconds,
                ))
                .await;
            }
        }

        last_result
            .ok_or_else(|| StreamlineError::Config("Reconciliation failed after retries".into()))
    }

    async fn apply_drift(&self, drift: &DriftResult, manifest: &ConfigManifest) -> Result<bool> {
        // In a real implementation, this would call the appropriate APIs
        // to create/update/delete resources based on the drift type
        match drift.resource_type.as_str() {
            "Topic" => self.apply_topic_drift(drift, manifest).await,
            "ACL" => self.apply_acl_drift(drift, manifest).await,
            "Sink" => self.apply_sink_drift(drift, manifest).await,
            _ => Err(StreamlineError::Config(format!(
                "Unknown resource type: {}",
                drift.resource_type
            ))),
        }
    }

    async fn apply_topic_drift(
        &self,
        drift: &DriftResult,
        manifest: &ConfigManifest,
    ) -> Result<bool> {
        if let Some(spec) = &manifest.spec {
            if let Some(_topic) = spec.topics.iter().find(|t| t.name == drift.resource_name) {
                match drift.drift_type {
                    DriftType::Missing => {
                        // Would call topic_manager.create_topic() here
                        tracing::info!(
                            topic = %drift.resource_name,
                            "Creating topic from GitOps manifest"
                        );
                        Ok(true)
                    }
                    DriftType::Modified => {
                        // Would call topic_manager.alter_topic() here
                        tracing::info!(
                            topic = %drift.resource_name,
                            change = %drift.details,
                            "Updating topic from GitOps manifest"
                        );
                        Ok(true)
                    }
                    DriftType::Extra => {
                        // Topics are not auto-deleted by default
                        tracing::warn!(
                            topic = %drift.resource_name,
                            "Extra topic found but not deleting (requires explicit action)"
                        );
                        Ok(false)
                    }
                }
            } else {
                Ok(false)
            }
        } else {
            Ok(false)
        }
    }

    async fn apply_acl_drift(
        &self,
        drift: &DriftResult,
        manifest: &ConfigManifest,
    ) -> Result<bool> {
        if manifest.spec.is_some() {
            match drift.drift_type {
                DriftType::Missing => {
                    // Would call acl_store.add_acl() here
                    tracing::info!(
                        acl = %drift.resource_name,
                        "Creating ACL from GitOps manifest"
                    );
                    Ok(true)
                }
                DriftType::Modified | DriftType::Extra => {
                    // ACLs are typically replaced, not modified
                    tracing::info!(
                        acl = %drift.resource_name,
                        "ACL would be modified/removed"
                    );
                    Ok(true)
                }
            }
        } else {
            Ok(false)
        }
    }

    async fn apply_sink_drift(
        &self,
        drift: &DriftResult,
        manifest: &ConfigManifest,
    ) -> Result<bool> {
        if let Some(spec) = &manifest.spec {
            if let Some(_sink) = spec.sinks.iter().find(|s| s.name == drift.resource_name) {
                match drift.drift_type {
                    DriftType::Missing => {
                        // Would call sink_manager.create_sink() here
                        tracing::info!(
                            sink = %drift.resource_name,
                            "Creating sink from GitOps manifest"
                        );
                        Ok(true)
                    }
                    DriftType::Modified => {
                        // Would call sink_manager.update_sink() here
                        tracing::info!(
                            sink = %drift.resource_name,
                            "Updating sink from GitOps manifest"
                        );
                        Ok(true)
                    }
                    DriftType::Extra => {
                        tracing::warn!(
                            sink = %drift.resource_name,
                            "Extra sink found but not removing"
                        );
                        Ok(false)
                    }
                }
            } else {
                Ok(false)
            }
        } else {
            Ok(false)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gitops::config::{ManifestKind, ManifestMetadata, StreamlineConfigSpec};

    fn create_test_manifest() -> ConfigManifest {
        ConfigManifest {
            api_version: "streamline.io/v1".to_string(),
            kind: ManifestKind::StreamlineConfig,
            metadata: ManifestMetadata {
                name: "test".to_string(),
                namespace: "default".to_string(),
                ..Default::default()
            },
            spec: Some(StreamlineConfigSpec {
                topics: vec![TopicSpec {
                    name: "events".to_string(),
                    partitions: 3,
                    replication_factor: 1,
                    ..Default::default()
                }],
                ..Default::default()
            }),
        }
    }

    #[tokio::test]
    async fn test_drift_detection() {
        let detector = DriftDetector::new();
        let manifest = create_test_manifest();

        let drifts = detector.detect(&manifest).await.unwrap();

        // Since state cache is empty, all resources should show as missing
        assert!(!drifts.is_empty());
        assert!(drifts.iter().any(|d| d.drift_type == DriftType::Missing));
    }

    #[tokio::test]
    async fn test_reconcile_dry_run() {
        let config = ReconcileConfig {
            dry_run: true,
            ..Default::default()
        };
        let reconciler = Reconciler::new(config);
        let manifest = create_test_manifest();

        let result = reconciler.reconcile(&manifest).await.unwrap();

        assert_eq!(result.status, ReconcileStatus::Skipped);
        assert!(result.created.iter().any(|s| s.contains("would create")));
    }
}
