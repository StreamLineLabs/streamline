//! GitOps Configuration Engine
//!
//! Declarative topic, ACL, schema, and quota management via Git-synced YAML
//! configurations.  The engine watches a local directory for resource manifests,
//! computes a diff against the current cluster state, and applies (or previews)
//! the required changes.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Top-level configuration for the GitOps engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GitOpsConfig {
    /// Path to the directory containing YAML resource definitions.
    #[serde(default = "default_config_dir")]
    pub config_dir: String,

    /// How often (in seconds) the engine re-syncs with the config directory.
    #[serde(default = "default_sync_interval")]
    pub sync_interval_secs: u64,

    /// When `true`, the engine computes changes but never applies them.
    #[serde(default)]
    pub dry_run: bool,

    /// Automatically create topics declared in config that do not yet exist.
    #[serde(default = "default_true")]
    pub auto_create_topics: bool,

    /// Automatically delete topics that are *not* declared in config.
    /// Defaults to `false` for safety.
    #[serde(default)]
    pub auto_delete_topics: bool,

    /// Whether schema resources are synced.
    #[serde(default = "default_true")]
    pub enable_schema_sync: bool,
}

fn default_config_dir() -> String {
    "./gitops".to_string()
}
fn default_sync_interval() -> u64 {
    30
}
fn default_true() -> bool {
    true
}

impl Default for GitOpsConfig {
    fn default() -> Self {
        Self {
            config_dir: default_config_dir(),
            sync_interval_secs: default_sync_interval(),
            dry_run: false,
            auto_create_topics: true,
            auto_delete_topics: false,
            enable_schema_sync: true,
        }
    }
}

// ---------------------------------------------------------------------------
// State & status
// ---------------------------------------------------------------------------

/// Current status of the GitOps engine.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum GitOpsStatus {
    Idle,
    Syncing,
    Error(String),
    Disabled,
}

/// Mutable runtime state of the engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GitOpsState {
    pub last_sync: Option<u64>,
    pub applied_resources: Vec<AppliedResource>,
    pub pending_changes: Vec<ResourceChange>,
    pub sync_errors: Vec<SyncError>,
    pub status: GitOpsStatus,
}

impl Default for GitOpsState {
    fn default() -> Self {
        Self {
            last_sync: None,
            applied_resources: Vec::new(),
            pending_changes: Vec::new(),
            sync_errors: Vec::new(),
            status: GitOpsStatus::Idle,
        }
    }
}

// ---------------------------------------------------------------------------
// Resource types
// ---------------------------------------------------------------------------

/// The kind of Streamline resource managed by GitOps.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ResourceKind {
    Topic,
    Acl,
    Schema,
    ConsumerGroup,
    Quota,
}

/// Reconciliation status of a single resource.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ResourceStatus {
    InSync,
    Drifted,
    Error(String),
    PendingDelete,
}

/// A resource that has been applied by the engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppliedResource {
    pub kind: ResourceKind,
    pub name: String,
    /// SHA / version hash of the configuration that was applied.
    pub version: String,
    pub applied_at: u64,
    pub status: ResourceStatus,
}

/// A pending change detected during a sync / plan.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResourceChange {
    pub kind: ResourceKind,
    pub name: String,
    pub action: ChangeAction,
    /// Optional human-readable diff.
    pub diff: Option<String>,
}

/// What the engine intends to do with a resource.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ChangeAction {
    Create,
    Update,
    Delete,
    NoChange,
}

/// An error encountered while syncing a single resource.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncError {
    pub resource: String,
    pub error: String,
    pub timestamp: u64,
}

// ---------------------------------------------------------------------------
// YAML resource definitions
// ---------------------------------------------------------------------------

/// A topic resource definition as found in a YAML manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicResource {
    pub api_version: String,
    pub kind: String,
    pub metadata: ResourceMetadata,
    pub spec: TopicSpec,
}

/// Metadata common to all resource definitions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceMetadata {
    pub name: String,
    #[serde(default)]
    pub labels: HashMap<String, String>,
    #[serde(default)]
    pub annotations: HashMap<String, String>,
}

/// The desired-state specification for a topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicSpec {
    pub partitions: u32,
    pub replication_factor: u32,
    pub retention_ms: Option<u64>,
    #[serde(default)]
    pub config: HashMap<String, String>,
}

// ---------------------------------------------------------------------------
// Engine implementation
// ---------------------------------------------------------------------------

/// The core GitOps engine.
pub struct GitOpsEngine {
    state: Arc<RwLock<GitOpsState>>,
    config: GitOpsConfig,
}

impl GitOpsEngine {
    /// Create a new engine with the given configuration.
    pub fn new(config: GitOpsConfig) -> Self {
        info!(config_dir = %config.config_dir, "GitOps engine created");
        Self {
            state: Arc::new(RwLock::new(GitOpsState::default())),
            config,
        }
    }

    /// Compute the set of changes that *would* be applied (dry-run).
    pub async fn plan(&self) -> Vec<ResourceChange> {
        debug!("computing gitops plan");
        let state = self.state.read().await;
        state.pending_changes.clone()
    }

    /// Apply all pending changes and return the resulting applied resources.
    pub async fn apply(&self) -> Result<Vec<AppliedResource>, String> {
        if self.config.dry_run {
            info!("dry-run mode — skipping apply");
            return Ok(Vec::new());
        }

        let mut state = self.state.write().await;
        state.status = GitOpsStatus::Syncing;
        let now = now_epoch();

        let mut applied = Vec::new();
        let mut errors = Vec::new();

        let changes: Vec<ResourceChange> = state.pending_changes.drain(..).collect();
        for change in changes {
            match &change.action {
                ChangeAction::NoChange => continue,
                ChangeAction::Create | ChangeAction::Update => {
                    let resource = AppliedResource {
                        kind: change.kind.clone(),
                        name: change.name.clone(),
                        version: format!("v{}", now),
                        applied_at: now,
                        status: ResourceStatus::InSync,
                    };
                    info!(name = %resource.name, action = ?change.action, "applied resource");
                    applied.push(resource);
                }
                ChangeAction::Delete => {
                    if !self.config.auto_delete_topics
                        && change.kind == ResourceKind::Topic
                    {
                        warn!(name = %change.name, "auto-delete disabled — skipping topic deletion");
                        errors.push(SyncError {
                            resource: change.name.clone(),
                            error: "auto_delete_topics is disabled".to_string(),
                            timestamp: now,
                        });
                        continue;
                    }
                    let resource = AppliedResource {
                        kind: change.kind.clone(),
                        name: change.name.clone(),
                        version: "deleted".to_string(),
                        applied_at: now,
                        status: ResourceStatus::PendingDelete,
                    };
                    info!(name = %resource.name, "deleted resource");
                    applied.push(resource);
                }
            }
        }

        state.applied_resources.extend(applied.clone());
        state.sync_errors.extend(errors);
        state.last_sync = Some(now);
        state.status = GitOpsStatus::Idle;

        Ok(applied)
    }

    /// Return a snapshot of the current engine state.
    pub async fn get_state(&self) -> GitOpsState {
        self.state.read().await.clone()
    }

    /// Parse a YAML string into a [`TopicResource`].
    pub fn parse_resource(yaml: &str) -> Result<TopicResource, String> {
        serde_yaml::from_str(yaml).map_err(|e| format!("YAML parse error: {e}"))
    }

    /// Compute the diff between a current (optional) and desired topic resource.
    pub fn diff(current: Option<&TopicResource>, desired: &TopicResource) -> ResourceChange {
        match current {
            None => ResourceChange {
                kind: ResourceKind::Topic,
                name: desired.metadata.name.clone(),
                action: ChangeAction::Create,
                diff: Some("new resource".to_string()),
            },
            Some(cur) => {
                let mut diffs = Vec::new();

                if cur.spec.partitions != desired.spec.partitions {
                    diffs.push(format!(
                        "partitions: {} -> {}",
                        cur.spec.partitions, desired.spec.partitions
                    ));
                }
                if cur.spec.replication_factor != desired.spec.replication_factor {
                    diffs.push(format!(
                        "replication_factor: {} -> {}",
                        cur.spec.replication_factor, desired.spec.replication_factor
                    ));
                }
                if cur.spec.retention_ms != desired.spec.retention_ms {
                    diffs.push(format!(
                        "retention_ms: {:?} -> {:?}",
                        cur.spec.retention_ms, desired.spec.retention_ms
                    ));
                }
                if cur.spec.config != desired.spec.config {
                    diffs.push("config changed".to_string());
                }

                if diffs.is_empty() {
                    ResourceChange {
                        kind: ResourceKind::Topic,
                        name: desired.metadata.name.clone(),
                        action: ChangeAction::NoChange,
                        diff: None,
                    }
                } else {
                    ResourceChange {
                        kind: ResourceKind::Topic,
                        name: desired.metadata.name.clone(),
                        action: ChangeAction::Update,
                        diff: Some(diffs.join("; ")),
                    }
                }
            }
        }
    }

    /// Return all previously applied resources.
    pub async fn list_applied(&self) -> Vec<AppliedResource> {
        self.state.read().await.applied_resources.clone()
    }

    /// Full reconciliation: inject pending changes, then apply them.
    pub async fn reconcile(
        &self,
        desired: Vec<ResourceChange>,
    ) -> Result<Vec<AppliedResource>, String> {
        {
            let mut state = self.state.write().await;
            state.pending_changes = desired;
        }
        self.apply().await
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn now_epoch() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn default_engine() -> GitOpsEngine {
        GitOpsEngine::new(GitOpsConfig::default())
    }

    fn sample_topic_yaml() -> &'static str {
        r#"
api_version: "streamline.io/v1"
kind: "Topic"
metadata:
  name: "orders"
  labels:
    team: "platform"
  annotations:
    description: "Order events"
spec:
  partitions: 6
  replication_factor: 3
  retention_ms: 604800000
  config:
    compression.type: "zstd"
"#
    }

    fn make_topic(name: &str, partitions: u32, rf: u32) -> TopicResource {
        TopicResource {
            api_version: "streamline.io/v1".to_string(),
            kind: "Topic".to_string(),
            metadata: ResourceMetadata {
                name: name.to_string(),
                labels: HashMap::new(),
                annotations: HashMap::new(),
            },
            spec: TopicSpec {
                partitions,
                replication_factor: rf,
                retention_ms: None,
                config: HashMap::new(),
            },
        }
    }

    // 1
    #[test]
    fn test_default_config() {
        let cfg = GitOpsConfig::default();
        assert_eq!(cfg.config_dir, "./gitops");
        assert_eq!(cfg.sync_interval_secs, 30);
        assert!(!cfg.dry_run);
        assert!(cfg.auto_create_topics);
        assert!(!cfg.auto_delete_topics);
        assert!(cfg.enable_schema_sync);
    }

    // 2
    #[test]
    fn test_default_state() {
        let state = GitOpsState::default();
        assert_eq!(state.status, GitOpsStatus::Idle);
        assert!(state.last_sync.is_none());
        assert!(state.applied_resources.is_empty());
        assert!(state.pending_changes.is_empty());
        assert!(state.sync_errors.is_empty());
    }

    // 3
    #[test]
    fn test_parse_resource_valid() {
        let res = GitOpsEngine::parse_resource(sample_topic_yaml()).unwrap();
        assert_eq!(res.metadata.name, "orders");
        assert_eq!(res.spec.partitions, 6);
        assert_eq!(res.spec.replication_factor, 3);
        assert_eq!(res.spec.retention_ms, Some(604_800_000));
        assert_eq!(
            res.spec.config.get("compression.type"),
            Some(&"zstd".to_string())
        );
    }

    // 4
    #[test]
    fn test_parse_resource_invalid_yaml() {
        let result = GitOpsEngine::parse_resource("::not yaml::");
        assert!(result.is_err());
    }

    // 5
    #[test]
    fn test_parse_resource_labels_and_annotations() {
        let res = GitOpsEngine::parse_resource(sample_topic_yaml()).unwrap();
        assert_eq!(res.metadata.labels.get("team"), Some(&"platform".to_string()));
        assert_eq!(
            res.metadata.annotations.get("description"),
            Some(&"Order events".to_string())
        );
    }

    // 6
    #[test]
    fn test_diff_create() {
        let desired = make_topic("new-topic", 3, 1);
        let change = GitOpsEngine::diff(None, &desired);
        assert_eq!(change.action, ChangeAction::Create);
        assert_eq!(change.name, "new-topic");
        assert!(change.diff.is_some());
    }

    // 7
    #[test]
    fn test_diff_no_change() {
        let a = make_topic("same", 3, 1);
        let b = make_topic("same", 3, 1);
        let change = GitOpsEngine::diff(Some(&a), &b);
        assert_eq!(change.action, ChangeAction::NoChange);
        assert!(change.diff.is_none());
    }

    // 8
    #[test]
    fn test_diff_partition_change() {
        let current = make_topic("t", 3, 1);
        let desired = make_topic("t", 6, 1);
        let change = GitOpsEngine::diff(Some(&current), &desired);
        assert_eq!(change.action, ChangeAction::Update);
        assert!(change.diff.as_ref().unwrap().contains("partitions"));
    }

    // 9
    #[test]
    fn test_diff_replication_factor_change() {
        let current = make_topic("t", 3, 1);
        let desired = make_topic("t", 3, 3);
        let change = GitOpsEngine::diff(Some(&current), &desired);
        assert_eq!(change.action, ChangeAction::Update);
        assert!(change.diff.as_ref().unwrap().contains("replication_factor"));
    }

    // 10
    #[test]
    fn test_diff_config_change() {
        let current = make_topic("t", 3, 1);
        let mut desired = make_topic("t", 3, 1);
        desired
            .spec
            .config
            .insert("cleanup.policy".to_string(), "compact".to_string());
        let change = GitOpsEngine::diff(Some(&current), &desired);
        assert_eq!(change.action, ChangeAction::Update);
        assert!(change.diff.as_ref().unwrap().contains("config changed"));
    }

    // 11
    #[tokio::test]
    async fn test_plan_empty_on_new_engine() {
        let engine = default_engine();
        let plan = engine.plan().await;
        assert!(plan.is_empty());
    }

    // 12
    #[tokio::test]
    async fn test_apply_empty() {
        let engine = default_engine();
        let applied = engine.apply().await.unwrap();
        assert!(applied.is_empty());
    }

    // 13
    #[tokio::test]
    async fn test_apply_creates_resources() {
        let engine = default_engine();
        let changes = vec![ResourceChange {
            kind: ResourceKind::Topic,
            name: "orders".to_string(),
            action: ChangeAction::Create,
            diff: Some("new resource".to_string()),
        }];
        let applied = engine.reconcile(changes).await.unwrap();
        assert_eq!(applied.len(), 1);
        assert_eq!(applied[0].name, "orders");
        assert_eq!(applied[0].status, ResourceStatus::InSync);
    }

    // 14
    #[tokio::test]
    async fn test_apply_dry_run_skips() {
        let mut cfg = GitOpsConfig::default();
        cfg.dry_run = true;
        let engine = GitOpsEngine::new(cfg);
        {
            let mut state = engine.state.write().await;
            state.pending_changes.push(ResourceChange {
                kind: ResourceKind::Topic,
                name: "x".to_string(),
                action: ChangeAction::Create,
                diff: None,
            });
        }
        let applied = engine.apply().await.unwrap();
        assert!(applied.is_empty());
    }

    // 15
    #[tokio::test]
    async fn test_apply_delete_blocked_by_default() {
        let engine = default_engine();
        let changes = vec![ResourceChange {
            kind: ResourceKind::Topic,
            name: "old-topic".to_string(),
            action: ChangeAction::Delete,
            diff: None,
        }];
        let applied = engine.reconcile(changes).await.unwrap();
        // Deletion is blocked (auto_delete_topics = false)
        assert!(applied.is_empty());
        let state = engine.get_state().await;
        assert_eq!(state.sync_errors.len(), 1);
    }

    // 16
    #[tokio::test]
    async fn test_apply_delete_allowed() {
        let mut cfg = GitOpsConfig::default();
        cfg.auto_delete_topics = true;
        let engine = GitOpsEngine::new(cfg);
        let changes = vec![ResourceChange {
            kind: ResourceKind::Topic,
            name: "old-topic".to_string(),
            action: ChangeAction::Delete,
            diff: None,
        }];
        let applied = engine.reconcile(changes).await.unwrap();
        assert_eq!(applied.len(), 1);
        assert_eq!(applied[0].status, ResourceStatus::PendingDelete);
    }

    // 17
    #[tokio::test]
    async fn test_list_applied_after_reconcile() {
        let engine = default_engine();
        let changes = vec![
            ResourceChange {
                kind: ResourceKind::Topic,
                name: "a".to_string(),
                action: ChangeAction::Create,
                diff: None,
            },
            ResourceChange {
                kind: ResourceKind::Schema,
                name: "b".to_string(),
                action: ChangeAction::Update,
                diff: None,
            },
        ];
        engine.reconcile(changes).await.unwrap();
        let list = engine.list_applied().await;
        assert_eq!(list.len(), 2);
    }

    // 18
    #[tokio::test]
    async fn test_get_state_reflects_sync_timestamp() {
        let engine = default_engine();
        let changes = vec![ResourceChange {
            kind: ResourceKind::Acl,
            name: "acl1".to_string(),
            action: ChangeAction::Create,
            diff: None,
        }];
        engine.reconcile(changes).await.unwrap();
        let state = engine.get_state().await;
        assert!(state.last_sync.is_some());
        assert_eq!(state.status, GitOpsStatus::Idle);
    }

    // 19
    #[test]
    fn test_resource_kind_serialization() {
        let json = serde_json::to_string(&ResourceKind::ConsumerGroup).unwrap();
        assert_eq!(json, "\"ConsumerGroup\"");
        let back: ResourceKind = serde_json::from_str(&json).unwrap();
        assert_eq!(back, ResourceKind::ConsumerGroup);
    }

    // 20
    #[test]
    fn test_config_serde_roundtrip() {
        let cfg = GitOpsConfig::default();
        let json = serde_json::to_string(&cfg).unwrap();
        let back: GitOpsConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(back.config_dir, cfg.config_dir);
        assert_eq!(back.sync_interval_secs, cfg.sync_interval_secs);
    }

    // 21
    #[tokio::test]
    async fn test_no_change_action_skipped() {
        let engine = default_engine();
        let changes = vec![ResourceChange {
            kind: ResourceKind::Topic,
            name: "unchanged".to_string(),
            action: ChangeAction::NoChange,
            diff: None,
        }];
        let applied = engine.reconcile(changes).await.unwrap();
        assert!(applied.is_empty());
    }

    // 22
    #[test]
    fn test_diff_retention_change() {
        let mut current = make_topic("t", 3, 1);
        current.spec.retention_ms = Some(100_000);
        let mut desired = make_topic("t", 3, 1);
        desired.spec.retention_ms = Some(200_000);
        let change = GitOpsEngine::diff(Some(&current), &desired);
        assert_eq!(change.action, ChangeAction::Update);
        assert!(change.diff.as_ref().unwrap().contains("retention_ms"));
    }
}
