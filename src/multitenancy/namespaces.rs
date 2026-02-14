//! Multi-Tenant Namespace Isolation
//!
//! Provides hierarchical namespaces with per-tenant resource quotas,
//! authentication scoping, and usage metering. Namespaces form a tree
//! structure (e.g., "org/team/project") that maps naturally to topic
//! name prefixes, enabling clean multi-tenant isolation within a single
//! Streamline cluster.
//!
//! # Hierarchy
//!
//! Namespaces are hierarchical paths separated by `/`:
//!
//! ```text
//! acme                         (org-level)
//! acme/platform                (team-level)
//! acme/platform/payments       (project-level)
//! ```
//!
//! Each namespace has its own quotas, usage metering, and access grants.
//! A parent namespace's owner implicitly has `ManageNamespace` access
//! to all descendant namespaces.
//!
//! # Topic Resolution
//!
//! Topics within a namespace are prefixed with the namespace path:
//!
//! ```text
//! Namespace: "acme/platform/payments"
//! Topic:     "orders"
//! Resolved:  "acme.platform.payments.orders"
//! ```

use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use tokio::sync::RwLock;

// ---------------------------------------------------------------------------
// NamespaceConfig
// ---------------------------------------------------------------------------

/// Configuration for the namespace isolation subsystem.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamespaceConfig {
    /// Maximum number of namespaces across all tenants.
    pub max_namespaces: usize,
    /// Default quotas applied when none are explicitly provided.
    pub default_quotas: NamespaceQuotas,
    /// Whether to enable per-namespace usage metering.
    pub enable_metering: bool,
    /// Separator used when converting a namespace path into a topic prefix.
    /// For example, with separator `"."`, the path `"org/team"` becomes
    /// the topic prefix `"org.team."`.
    pub separator: String,
}

impl Default for NamespaceConfig {
    fn default() -> Self {
        Self {
            max_namespaces: 1000,
            default_quotas: NamespaceQuotas::default(),
            enable_metering: true,
            separator: "/".to_string(),
        }
    }
}

// ---------------------------------------------------------------------------
// NamespacePath
// ---------------------------------------------------------------------------

/// A validated hierarchical namespace path.
///
/// Paths consist of one or more segments separated by `/`. Each segment
/// must be non-empty and contain only alphanumeric characters, hyphens,
/// or underscores.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NamespacePath {
    /// The individual path segments (e.g., `["org", "team", "project"]`).
    pub segments: Vec<String>,
}

impl NamespacePath {
    /// Parse and validate a path string.
    ///
    /// # Errors
    ///
    /// Returns an error if the path is empty, contains empty segments, or
    /// any segment contains characters other than `[a-zA-Z0-9_-]`.
    pub fn new(path: &str) -> Result<Self> {
        let trimmed = path.trim_matches('/');
        if trimmed.is_empty() {
            return Err(StreamlineError::Validation(
                "namespace path must not be empty".into(),
            ));
        }

        let segments: Vec<String> = trimmed.split('/').map(|s| s.to_string()).collect();

        for segment in &segments {
            if segment.is_empty() {
                return Err(StreamlineError::Validation(
                    "namespace path must not contain empty segments".into(),
                ));
            }
            if !segment
                .chars()
                .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
            {
                return Err(StreamlineError::Validation(format!(
                    "namespace segment '{}' contains invalid characters; \
                     only alphanumeric, hyphens, and underscores are allowed",
                    segment
                )));
            }
        }

        Ok(Self { segments })
    }

    /// Return the parent path, or `None` if this is a root-level namespace.
    pub fn parent(&self) -> Option<NamespacePath> {
        if self.segments.len() <= 1 {
            return None;
        }
        Some(NamespacePath {
            segments: self.segments[..self.segments.len() - 1].to_vec(),
        })
    }

    /// Returns `true` if `self` is an ancestor of `other`.
    ///
    /// A path is an ancestor of another if it is a strict prefix of the
    /// other path's segments.
    pub fn is_ancestor_of(&self, other: &NamespacePath) -> bool {
        if self.segments.len() >= other.segments.len() {
            return false;
        }
        other.segments[..self.segments.len()] == self.segments[..]
    }

    /// The depth of this namespace (number of segments).
    pub fn depth(&self) -> usize {
        self.segments.len()
    }

    /// Build the topic prefix for this namespace using the given separator.
    ///
    /// For example, with separator `"."`, the path `"org/team/project"`
    /// produces the prefix `"org.team.project."`.
    pub fn topic_prefix(&self, separator: &str) -> String {
        let mut prefix = self.segments.join(separator);
        prefix.push_str(separator);
        prefix
    }
}

impl fmt::Display for NamespacePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.segments.join("/"))
    }
}

// ---------------------------------------------------------------------------
// NamespaceQuotas
// ---------------------------------------------------------------------------

/// Per-namespace resource quotas.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamespaceQuotas {
    /// Maximum number of topics in this namespace.
    pub max_topics: u32,
    /// Maximum total number of partitions across all topics.
    pub max_partitions: u32,
    /// Maximum storage in bytes.
    pub max_storage_bytes: u64,
    /// Maximum produce throughput in bytes per second.
    pub max_produce_rate_bytes_per_sec: u64,
    /// Maximum consume throughput in bytes per second.
    pub max_consume_rate_bytes_per_sec: u64,
    /// Maximum number of concurrent connections.
    pub max_connections: u32,
    /// Maximum number of consumer groups.
    pub max_consumer_groups: u32,
}

impl Default for NamespaceQuotas {
    fn default() -> Self {
        Self {
            max_topics: 100,
            max_partitions: 1000,
            max_storage_bytes: 10 * 1024 * 1024 * 1024, // 10 GB
            max_produce_rate_bytes_per_sec: 100 * 1024 * 1024, // 100 MB/s
            max_consume_rate_bytes_per_sec: 200 * 1024 * 1024, // 200 MB/s
            max_connections: 100,
            max_consumer_groups: 50,
        }
    }
}

// ---------------------------------------------------------------------------
// NamespaceStatus
// ---------------------------------------------------------------------------

/// The lifecycle status of a namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NamespaceStatus {
    /// Namespace is active and accepting operations.
    Active,
    /// Namespace is suspended (e.g., quota exceeded, admin action).
    Suspended,
    /// Namespace is scheduled for deletion.
    PendingDeletion,
}

// ---------------------------------------------------------------------------
// Namespace
// ---------------------------------------------------------------------------

/// A single namespace within the hierarchy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Namespace {
    /// Unique identifier (UUID).
    pub id: String,
    /// Hierarchical path.
    pub path: NamespacePath,
    /// Human-readable name.
    pub display_name: String,
    /// Optional longer description.
    pub description: Option<String>,
    /// Principal who owns this namespace.
    pub owner: String,
    /// Resource quotas.
    pub quotas: NamespaceQuotas,
    /// Creation timestamp (epoch milliseconds).
    pub created_at: i64,
    /// Last modification timestamp (epoch milliseconds).
    pub updated_at: i64,
    /// Current lifecycle status.
    pub status: NamespaceStatus,
    /// Arbitrary key-value labels.
    pub metadata: HashMap<String, String>,
}

// ---------------------------------------------------------------------------
// NamespaceOperation
// ---------------------------------------------------------------------------

/// Operations that can be performed within a namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum NamespaceOperation {
    /// Create a new topic.
    CreateTopic,
    /// Delete a topic.
    DeleteTopic,
    /// Produce records.
    Produce,
    /// Consume records.
    Consume,
    /// Create a consumer group.
    CreateConsumerGroup,
    /// Modify the namespace's quotas.
    ModifyQuotas,
    /// Manage (create/delete/suspend) child namespaces.
    ManageNamespace,
}

// ---------------------------------------------------------------------------
// NamespaceUsage / QuotaUtilization
// ---------------------------------------------------------------------------

/// Snapshot of resource usage for a single namespace.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamespaceUsage {
    /// Path of the namespace this usage pertains to.
    pub namespace_path: String,
    /// Number of topics.
    pub topics_count: u32,
    /// Number of partitions.
    pub partitions_count: u32,
    /// Bytes of storage consumed.
    pub storage_bytes: u64,
    /// Current produce throughput (bytes/s).
    pub produce_rate_bytes_per_sec: f64,
    /// Current consume throughput (bytes/s).
    pub consume_rate_bytes_per_sec: f64,
    /// Active connections.
    pub connections: u32,
    /// Active consumer groups.
    pub consumer_groups: u32,
    /// Computed quota utilization percentages.
    pub quota_utilization: QuotaUtilization,
}

/// Percentage-based utilization relative to namespace quotas.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuotaUtilization {
    /// Topics used / max_topics * 100.
    pub topics_percent: f64,
    /// Partitions used / max_partitions * 100.
    pub partitions_percent: f64,
    /// Storage used / max_storage_bytes * 100.
    pub storage_percent: f64,
    /// Produce rate / max_produce_rate * 100.
    pub produce_rate_percent: f64,
    /// Consume rate / max_consume_rate * 100.
    pub consume_rate_percent: f64,
    /// Connections / max_connections * 100.
    pub connections_percent: f64,
}

impl QuotaUtilization {
    /// Returns `true` if any utilization metric exceeds the given threshold
    /// (expressed as a percentage, e.g. `80.0` for 80%).
    pub fn is_near_limit(&self, threshold: f64) -> bool {
        self.topics_percent > threshold
            || self.partitions_percent > threshold
            || self.storage_percent > threshold
            || self.produce_rate_percent > threshold
            || self.consume_rate_percent > threshold
            || self.connections_percent > threshold
    }

    /// Returns `true` if any utilization metric exceeds 100%.
    pub fn is_over_limit(&self) -> bool {
        self.is_near_limit(100.0)
    }
}

// ---------------------------------------------------------------------------
// UsageMeter (internal counters)
// ---------------------------------------------------------------------------

/// Internal per-namespace usage counters.
#[derive(Debug, Clone, Default)]
struct NamespaceCounters {
    topics_count: u32,
    partitions_count: u32,
    storage_bytes: u64,
    produce_bytes_window: u64,
    consume_bytes_window: u64,
    connections: u32,
    consumer_groups: u32,
}

/// Tracks live usage per namespace for quota enforcement and metering.
pub struct UsageMeter {
    counters: HashMap<String, NamespaceCounters>,
}

impl UsageMeter {
    /// Create a new, empty usage meter.
    pub fn new() -> Self {
        Self {
            counters: HashMap::new(),
        }
    }

    /// Record bytes produced in the given namespace.
    pub fn record_produce(&mut self, path: &str, bytes: u64) {
        let c = self.counters.entry(path.to_string()).or_default();
        c.produce_bytes_window = c.produce_bytes_window.saturating_add(bytes);
    }

    /// Record bytes consumed in the given namespace.
    pub fn record_consume(&mut self, path: &str, bytes: u64) {
        let c = self.counters.entry(path.to_string()).or_default();
        c.consume_bytes_window = c.consume_bytes_window.saturating_add(bytes);
    }

    /// Adjust the connection count for the given namespace.
    ///
    /// Pass a positive `delta` when a connection opens and a negative
    /// `delta` when it closes.
    pub fn record_connection(&mut self, path: &str, delta: i32) {
        let c = self.counters.entry(path.to_string()).or_default();
        if delta >= 0 {
            c.connections = c.connections.saturating_add(delta as u32);
        } else {
            c.connections = c.connections.saturating_sub(delta.unsigned_abs());
        }
    }

    /// Build a [`NamespaceUsage`] snapshot for the given namespace and its
    /// configured quotas.
    pub fn get_usage(&self, path: &str, quotas: &NamespaceQuotas) -> NamespaceUsage {
        let c = self.counters.get(path).cloned().unwrap_or_default();

        let utilization = QuotaUtilization {
            topics_percent: pct(c.topics_count as f64, quotas.max_topics as f64),
            partitions_percent: pct(c.partitions_count as f64, quotas.max_partitions as f64),
            storage_percent: pct(c.storage_bytes as f64, quotas.max_storage_bytes as f64),
            produce_rate_percent: pct(
                c.produce_bytes_window as f64,
                quotas.max_produce_rate_bytes_per_sec as f64,
            ),
            consume_rate_percent: pct(
                c.consume_bytes_window as f64,
                quotas.max_consume_rate_bytes_per_sec as f64,
            ),
            connections_percent: pct(c.connections as f64, quotas.max_connections as f64),
        };

        NamespaceUsage {
            namespace_path: path.to_string(),
            topics_count: c.topics_count,
            partitions_count: c.partitions_count,
            storage_bytes: c.storage_bytes,
            produce_rate_bytes_per_sec: c.produce_bytes_window as f64,
            consume_rate_bytes_per_sec: c.consume_bytes_window as f64,
            connections: c.connections,
            consumer_groups: c.consumer_groups,
            quota_utilization: utilization,
        }
    }

    /// Reset produce and consume rate accumulators. This should be called
    /// periodically (e.g., every second) so the byte-window counters
    /// represent a recent rate rather than a running total.
    pub fn reset_rates(&mut self) {
        for c in self.counters.values_mut() {
            c.produce_bytes_window = 0;
            c.consume_bytes_window = 0;
        }
    }

    /// Set the topic count for a namespace directly (used by the manager
    /// when topics are created or deleted).
    #[allow(dead_code)]
    pub(crate) fn set_topics(&mut self, path: &str, count: u32) {
        self.counters
            .entry(path.to_string())
            .or_default()
            .topics_count = count;
    }

    /// Set the partition count for a namespace directly.
    #[allow(dead_code)]
    pub(crate) fn set_partitions(&mut self, path: &str, count: u32) {
        self.counters
            .entry(path.to_string())
            .or_default()
            .partitions_count = count;
    }

    /// Set the storage bytes for a namespace directly.
    #[allow(dead_code)]
    pub(crate) fn set_storage(&mut self, path: &str, bytes: u64) {
        self.counters
            .entry(path.to_string())
            .or_default()
            .storage_bytes = bytes;
    }

    /// Set the consumer group count for a namespace directly.
    #[allow(dead_code)]
    pub(crate) fn set_consumer_groups(&mut self, path: &str, count: u32) {
        self.counters
            .entry(path.to_string())
            .or_default()
            .consumer_groups = count;
    }

    /// Remove all counters for a namespace.
    pub(crate) fn remove(&mut self, path: &str) {
        self.counters.remove(path);
    }
}

impl Default for UsageMeter {
    fn default() -> Self {
        Self::new()
    }
}

/// Compute percentage, clamping to 0.0 when the maximum is zero.
fn pct(current: f64, max: f64) -> f64 {
    if max <= 0.0 {
        0.0
    } else {
        (current / max) * 100.0
    }
}

// ---------------------------------------------------------------------------
// NamespaceAuthz / AccessGrant
// ---------------------------------------------------------------------------

/// A recorded grant of operations to a principal within a namespace.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessGrant {
    /// The principal (user or service) that received the grant.
    pub principal: String,
    /// The namespace path the grant applies to.
    pub namespace_path: String,
    /// The set of operations permitted.
    pub operations: Vec<NamespaceOperation>,
    /// Epoch milliseconds when the grant was created.
    pub granted_at: i64,
    /// The principal who issued the grant.
    pub granted_by: String,
}

/// Authorization layer for namespace-scoped operations.
///
/// Grants are keyed by `(principal, namespace_path)`.
pub struct NamespaceAuthz {
    /// `principal -> namespace_path -> AccessGrant`
    grants: HashMap<String, HashMap<String, AccessGrant>>,
}

impl NamespaceAuthz {
    /// Create a new, empty authorization store.
    pub fn new() -> Self {
        Self {
            grants: HashMap::new(),
        }
    }

    /// Check whether `principal` is permitted to perform `operation` within
    /// the given namespace.
    ///
    /// Returns `Ok(true)` if access is granted, `Ok(false)` if not. This
    /// method does **not** return an error for denied access -- the caller
    /// can decide whether denial is an error.
    pub fn check_access(
        &self,
        principal: &str,
        namespace: &NamespacePath,
        operation: NamespaceOperation,
    ) -> Result<bool> {
        let ns_str = namespace.to_string();

        if let Some(ns_grants) = self.grants.get(principal) {
            if let Some(grant) = ns_grants.get(&ns_str) {
                if grant.operations.contains(&operation) {
                    return Ok(true);
                }
            }

            // Walk up the hierarchy: a ManageNamespace grant on an ancestor
            // implies all operations on descendants.
            let mut ancestor = namespace.parent();
            while let Some(ref anc) = ancestor {
                let anc_str = anc.to_string();
                if let Some(grant) = ns_grants.get(&anc_str) {
                    if grant
                        .operations
                        .contains(&NamespaceOperation::ManageNamespace)
                    {
                        return Ok(true);
                    }
                }
                ancestor = anc.parent();
            }
        }

        Ok(false)
    }

    /// Grant a set of operations to `principal` on `namespace`.
    pub fn grant_access(
        &mut self,
        principal: &str,
        namespace: &NamespacePath,
        operations: Vec<NamespaceOperation>,
        granted_by: &str,
    ) {
        let ns_str = namespace.to_string();
        let grant = AccessGrant {
            principal: principal.to_string(),
            namespace_path: ns_str.clone(),
            operations,
            granted_at: chrono::Utc::now().timestamp_millis(),
            granted_by: granted_by.to_string(),
        };

        self.grants
            .entry(principal.to_string())
            .or_default()
            .insert(ns_str, grant);
    }

    /// Revoke all grants for `principal` on `namespace`.
    pub fn revoke_access(&mut self, principal: &str, namespace: &NamespacePath) {
        let ns_str = namespace.to_string();
        if let Some(ns_grants) = self.grants.get_mut(principal) {
            ns_grants.remove(&ns_str);
        }
    }

    /// List all grants that apply to the given namespace.
    pub fn list_grants(&self, namespace: &NamespacePath) -> Vec<AccessGrant> {
        let ns_str = namespace.to_string();
        let mut result = Vec::new();
        for ns_grants in self.grants.values() {
            if let Some(grant) = ns_grants.get(&ns_str) {
                result.push(grant.clone());
            }
        }
        result
    }
}

impl Default for NamespaceAuthz {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// HierarchicalNamespaceManager
// ---------------------------------------------------------------------------

/// Manages all namespaces, providing CRUD operations, quota validation,
/// usage metering, and topic name resolution.
pub struct HierarchicalNamespaceManager {
    config: NamespaceConfig,
    namespaces: Arc<RwLock<HashMap<String, Namespace>>>,
    meter: Arc<RwLock<UsageMeter>>,
    authz: Arc<RwLock<NamespaceAuthz>>,
}

impl HierarchicalNamespaceManager {
    /// Create a new manager with the given configuration.
    pub fn new(config: NamespaceConfig) -> Self {
        Self {
            config,
            namespaces: Arc::new(RwLock::new(HashMap::new())),
            meter: Arc::new(RwLock::new(UsageMeter::new())),
            authz: Arc::new(RwLock::new(NamespaceAuthz::new())),
        }
    }

    /// Create a namespace at the given path.
    ///
    /// # Errors
    ///
    /// - Path validation failure.
    /// - Maximum namespace count exceeded.
    /// - Namespace already exists at the given path.
    pub async fn create_namespace(
        &self,
        path: &str,
        display_name: &str,
        owner: &str,
        quotas: Option<NamespaceQuotas>,
    ) -> Result<Namespace> {
        let ns_path = NamespacePath::new(path)?;

        let mut map = self.namespaces.write().await;

        if map.len() >= self.config.max_namespaces {
            return Err(StreamlineError::ResourceExhausted(format!(
                "maximum namespace count ({}) reached",
                self.config.max_namespaces,
            )));
        }

        let key = ns_path.to_string();
        if map.contains_key(&key) {
            return Err(StreamlineError::Tenant(format!(
                "namespace '{}' already exists",
                key,
            )));
        }

        let now = chrono::Utc::now().timestamp_millis();
        let ns = Namespace {
            id: uuid_v4(),
            path: ns_path,
            display_name: display_name.to_string(),
            description: None,
            owner: owner.to_string(),
            quotas: quotas.unwrap_or_else(|| self.config.default_quotas.clone()),
            created_at: now,
            updated_at: now,
            status: NamespaceStatus::Active,
            metadata: HashMap::new(),
        };

        map.insert(key, ns.clone());

        Ok(ns)
    }

    /// Delete the namespace at the given path.
    ///
    /// # Errors
    ///
    /// Returns an error if the namespace does not exist.
    pub async fn delete_namespace(&self, path: &str) -> Result<()> {
        let ns_path = NamespacePath::new(path)?;
        let key = ns_path.to_string();

        let mut map = self.namespaces.write().await;
        if map.remove(&key).is_none() {
            return Err(StreamlineError::Tenant(format!(
                "namespace '{}' not found",
                key,
            )));
        }

        // Clean up metering counters.
        let mut meter = self.meter.write().await;
        meter.remove(&key);

        Ok(())
    }

    /// Retrieve a namespace by path.
    pub async fn get_namespace(&self, path: &str) -> Result<Namespace> {
        let ns_path = NamespacePath::new(path)?;
        let key = ns_path.to_string();

        let map = self.namespaces.read().await;
        map.get(&key)
            .cloned()
            .ok_or_else(|| StreamlineError::Tenant(format!("namespace '{}' not found", key)))
    }

    /// List namespaces that are direct children of `parent`. If `parent`
    /// is `None`, returns all root-level namespaces.
    pub async fn list_namespaces(&self, parent: Option<&NamespacePath>) -> Vec<Namespace> {
        let map = self.namespaces.read().await;
        map.values()
            .filter(|ns| match parent {
                Some(p) => {
                    ns.path.segments.len() == p.segments.len() + 1 && p.is_ancestor_of(&ns.path)
                }
                None => ns.path.segments.len() == 1,
            })
            .cloned()
            .collect()
    }

    /// Update the quotas for an existing namespace.
    pub async fn update_quotas(&self, path: &str, quotas: NamespaceQuotas) -> Result<()> {
        let ns_path = NamespacePath::new(path)?;
        let key = ns_path.to_string();

        let mut map = self.namespaces.write().await;
        let ns = map
            .get_mut(&key)
            .ok_or_else(|| StreamlineError::Tenant(format!("namespace '{}' not found", key)))?;

        ns.quotas = quotas;
        ns.updated_at = chrono::Utc::now().timestamp_millis();
        Ok(())
    }

    /// Suspend a namespace, preventing further operations.
    pub async fn suspend_namespace(&self, path: &str) -> Result<()> {
        let ns_path = NamespacePath::new(path)?;
        let key = ns_path.to_string();

        let mut map = self.namespaces.write().await;
        let ns = map
            .get_mut(&key)
            .ok_or_else(|| StreamlineError::Tenant(format!("namespace '{}' not found", key)))?;

        ns.status = NamespaceStatus::Suspended;
        ns.updated_at = chrono::Utc::now().timestamp_millis();
        Ok(())
    }

    /// Re-activate a previously suspended namespace.
    pub async fn activate_namespace(&self, path: &str) -> Result<()> {
        let ns_path = NamespacePath::new(path)?;
        let key = ns_path.to_string();

        let mut map = self.namespaces.write().await;
        let ns = map
            .get_mut(&key)
            .ok_or_else(|| StreamlineError::Tenant(format!("namespace '{}' not found", key)))?;

        ns.status = NamespaceStatus::Active;
        ns.updated_at = chrono::Utc::now().timestamp_millis();
        Ok(())
    }

    /// Resolve a topic name within a namespace to its fully-qualified form.
    ///
    /// The fully-qualified name is built by joining the namespace path
    /// segments with the configured separator and appending the topic name.
    pub fn resolve_topic(&self, namespace_path: &NamespacePath, topic_name: &str) -> String {
        let prefix = namespace_path.topic_prefix(&self.config.separator);
        format!("{}{}", prefix, topic_name)
    }

    /// Validate that a given operation is permitted within the namespace.
    ///
    /// This checks:
    /// 1. The namespace exists and is active.
    /// 2. For quota-relevant operations (`CreateTopic`, `Produce`, etc.),
    ///    the current usage does not exceed quotas.
    pub async fn validate_operation(
        &self,
        path: &str,
        operation: NamespaceOperation,
    ) -> Result<()> {
        let ns_path = NamespacePath::new(path)?;
        let key = ns_path.to_string();

        let map = self.namespaces.read().await;
        let ns = map
            .get(&key)
            .ok_or_else(|| StreamlineError::Tenant(format!("namespace '{}' not found", key)))?;

        if ns.status != NamespaceStatus::Active {
            return Err(StreamlineError::Tenant(format!(
                "namespace '{}' is not active (status: {:?})",
                key, ns.status,
            )));
        }

        // For operations that consume quota, check usage.
        let meter = self.meter.read().await;
        let usage = meter.get_usage(&key, &ns.quotas);

        match operation {
            NamespaceOperation::CreateTopic => {
                if usage.topics_count >= ns.quotas.max_topics {
                    return Err(StreamlineError::ResourceExhausted(format!(
                        "namespace '{}' has reached the maximum topic count ({})",
                        key, ns.quotas.max_topics,
                    )));
                }
            }
            NamespaceOperation::Produce => {
                if usage.produce_rate_bytes_per_sec
                    >= ns.quotas.max_produce_rate_bytes_per_sec as f64
                {
                    return Err(StreamlineError::ResourceExhausted(format!(
                        "namespace '{}' has exceeded the produce rate quota",
                        key,
                    )));
                }
            }
            NamespaceOperation::Consume => {
                if usage.consume_rate_bytes_per_sec
                    >= ns.quotas.max_consume_rate_bytes_per_sec as f64
                {
                    return Err(StreamlineError::ResourceExhausted(format!(
                        "namespace '{}' has exceeded the consume rate quota",
                        key,
                    )));
                }
            }
            NamespaceOperation::CreateConsumerGroup => {
                if usage.consumer_groups >= ns.quotas.max_consumer_groups {
                    return Err(StreamlineError::ResourceExhausted(format!(
                        "namespace '{}' has reached the maximum consumer group count ({})",
                        key, ns.quotas.max_consumer_groups,
                    )));
                }
            }
            // These operations are not quota-gated:
            NamespaceOperation::DeleteTopic
            | NamespaceOperation::ModifyQuotas
            | NamespaceOperation::ManageNamespace => {}
        }

        Ok(())
    }

    /// Retrieve current usage and quota utilization for a namespace.
    pub async fn get_usage(&self, path: &str) -> Result<NamespaceUsage> {
        let ns_path = NamespacePath::new(path)?;
        let key = ns_path.to_string();

        let map = self.namespaces.read().await;
        let ns = map
            .get(&key)
            .ok_or_else(|| StreamlineError::Tenant(format!("namespace '{}' not found", key)))?;

        let meter = self.meter.read().await;
        Ok(meter.get_usage(&key, &ns.quotas))
    }

    /// Obtain a reference to the shared usage meter (for external recording).
    pub fn meter(&self) -> &Arc<RwLock<UsageMeter>> {
        &self.meter
    }

    /// Obtain a reference to the shared authorization store.
    pub fn authz(&self) -> &Arc<RwLock<NamespaceAuthz>> {
        &self.authz
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Generate a UUID-v4 string. Uses a simple random approach that does not
/// require the `uuid` crate.
fn uuid_v4() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};

    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();

    // Simple pseudo-random UUID for identifiers. In production, the `uuid`
    // crate with `v4` would be preferred.
    format!(
        "{:08x}-{:04x}-4{:03x}-{:04x}-{:012x}",
        (seed & 0xFFFF_FFFF) as u32,
        ((seed >> 32) & 0xFFFF) as u16,
        ((seed >> 48) & 0x0FFF) as u16,
        (((seed >> 60) & 0x3F) | 0x80) as u16,
        (seed >> 64) as u64 & 0xFFFF_FFFF_FFFF,
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- NamespacePath parsing --

    #[test]
    fn test_path_parse_valid() {
        let p = NamespacePath::new("org/team/project").unwrap();
        assert_eq!(p.segments, vec!["org", "team", "project"]);
        assert_eq!(p.to_string(), "org/team/project");
    }

    #[test]
    fn test_path_parse_single_segment() {
        let p = NamespacePath::new("root").unwrap();
        assert_eq!(p.segments, vec!["root"]);
    }

    #[test]
    fn test_path_parse_trims_slashes() {
        let p = NamespacePath::new("/org/team/").unwrap();
        assert_eq!(p.segments, vec!["org", "team"]);
    }

    #[test]
    fn test_path_parse_empty_error() {
        let err = NamespacePath::new("").unwrap_err();
        assert!(err.to_string().contains("must not be empty"));
    }

    #[test]
    fn test_path_parse_empty_segment_error() {
        let err = NamespacePath::new("org//team").unwrap_err();
        assert!(err.to_string().contains("empty segments"));
    }

    #[test]
    fn test_path_parse_invalid_chars_error() {
        let err = NamespacePath::new("org/te@m").unwrap_err();
        assert!(err.to_string().contains("invalid characters"));
    }

    // -- NamespacePath hierarchy --

    #[test]
    fn test_path_parent() {
        let p = NamespacePath::new("org/team/project").unwrap();
        let parent = p.parent().unwrap();
        assert_eq!(parent.to_string(), "org/team");

        let grandparent = parent.parent().unwrap();
        assert_eq!(grandparent.to_string(), "org");

        assert!(grandparent.parent().is_none());
    }

    #[test]
    fn test_path_is_ancestor_of() {
        let ancestor = NamespacePath::new("org").unwrap();
        let child = NamespacePath::new("org/team").unwrap();
        let grandchild = NamespacePath::new("org/team/project").unwrap();
        let unrelated = NamespacePath::new("other/team").unwrap();

        assert!(ancestor.is_ancestor_of(&child));
        assert!(ancestor.is_ancestor_of(&grandchild));
        assert!(child.is_ancestor_of(&grandchild));
        assert!(!child.is_ancestor_of(&ancestor));
        assert!(!ancestor.is_ancestor_of(&ancestor)); // not strict prefix
        assert!(!ancestor.is_ancestor_of(&unrelated));
    }

    #[test]
    fn test_path_depth() {
        assert_eq!(NamespacePath::new("a").unwrap().depth(), 1);
        assert_eq!(NamespacePath::new("a/b").unwrap().depth(), 2);
        assert_eq!(NamespacePath::new("a/b/c").unwrap().depth(), 3);
    }

    // -- Topic name resolution --

    #[test]
    fn test_topic_prefix_default_separator() {
        let p = NamespacePath::new("org/team/project").unwrap();
        // Using "/" as the separator (the default).
        assert_eq!(p.topic_prefix("/"), "org/team/project/");
    }

    #[test]
    fn test_topic_prefix_dot_separator() {
        let p = NamespacePath::new("org/team").unwrap();
        assert_eq!(p.topic_prefix("."), "org.team.");
    }

    #[test]
    fn test_resolve_topic() {
        let config = NamespaceConfig {
            separator: ".".to_string(),
            ..Default::default()
        };
        let mgr = HierarchicalNamespaceManager::new(config);
        let path = NamespacePath::new("acme/platform").unwrap();
        let resolved = mgr.resolve_topic(&path, "orders");
        assert_eq!(resolved, "acme.platform.orders");
    }

    // -- Quota utilization --

    #[test]
    fn test_quota_utilization_under_limit() {
        let util = QuotaUtilization {
            topics_percent: 50.0,
            partitions_percent: 30.0,
            storage_percent: 20.0,
            produce_rate_percent: 10.0,
            consume_rate_percent: 5.0,
            connections_percent: 40.0,
        };
        assert!(!util.is_near_limit(80.0));
        assert!(!util.is_over_limit());
    }

    #[test]
    fn test_quota_utilization_near_limit() {
        let util = QuotaUtilization {
            topics_percent: 85.0,
            partitions_percent: 30.0,
            storage_percent: 20.0,
            produce_rate_percent: 10.0,
            consume_rate_percent: 5.0,
            connections_percent: 40.0,
        };
        assert!(util.is_near_limit(80.0));
        assert!(!util.is_over_limit());
    }

    #[test]
    fn test_quota_utilization_over_limit() {
        let util = QuotaUtilization {
            topics_percent: 105.0,
            partitions_percent: 30.0,
            storage_percent: 20.0,
            produce_rate_percent: 10.0,
            consume_rate_percent: 5.0,
            connections_percent: 40.0,
        };
        assert!(util.is_near_limit(80.0));
        assert!(util.is_over_limit());
    }

    // -- Namespace CRUD --

    #[tokio::test]
    async fn test_create_and_get_namespace() {
        let mgr = HierarchicalNamespaceManager::new(NamespaceConfig::default());

        let ns = mgr
            .create_namespace("acme/platform", "Acme Platform", "admin", None)
            .await
            .unwrap();

        assert_eq!(ns.display_name, "Acme Platform");
        assert_eq!(ns.owner, "admin");
        assert_eq!(ns.status, NamespaceStatus::Active);

        let retrieved = mgr.get_namespace("acme/platform").await.unwrap();
        assert_eq!(retrieved.id, ns.id);
    }

    #[tokio::test]
    async fn test_create_duplicate_namespace_error() {
        let mgr = HierarchicalNamespaceManager::new(NamespaceConfig::default());

        mgr.create_namespace("acme", "Acme", "admin", None)
            .await
            .unwrap();

        let err = mgr
            .create_namespace("acme", "Acme Dup", "admin", None)
            .await
            .unwrap_err();

        assert!(err.to_string().contains("already exists"));
    }

    #[tokio::test]
    async fn test_delete_namespace() {
        let mgr = HierarchicalNamespaceManager::new(NamespaceConfig::default());

        mgr.create_namespace("acme", "Acme", "admin", None)
            .await
            .unwrap();

        mgr.delete_namespace("acme").await.unwrap();

        let err = mgr.get_namespace("acme").await.unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[tokio::test]
    async fn test_delete_nonexistent_namespace_error() {
        let mgr = HierarchicalNamespaceManager::new(NamespaceConfig::default());

        let err = mgr.delete_namespace("nope").await.unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[tokio::test]
    async fn test_suspend_and_activate_namespace() {
        let mgr = HierarchicalNamespaceManager::new(NamespaceConfig::default());

        mgr.create_namespace("acme", "Acme", "admin", None)
            .await
            .unwrap();

        mgr.suspend_namespace("acme").await.unwrap();
        let ns = mgr.get_namespace("acme").await.unwrap();
        assert_eq!(ns.status, NamespaceStatus::Suspended);

        mgr.activate_namespace("acme").await.unwrap();
        let ns = mgr.get_namespace("acme").await.unwrap();
        assert_eq!(ns.status, NamespaceStatus::Active);
    }

    #[tokio::test]
    async fn test_list_root_namespaces() {
        let mgr = HierarchicalNamespaceManager::new(NamespaceConfig::default());

        mgr.create_namespace("acme", "Acme", "admin", None)
            .await
            .unwrap();
        mgr.create_namespace("acme/platform", "Platform", "admin", None)
            .await
            .unwrap();
        mgr.create_namespace("globex", "Globex", "admin", None)
            .await
            .unwrap();

        let roots = mgr.list_namespaces(None).await;
        assert_eq!(roots.len(), 2);
    }

    #[tokio::test]
    async fn test_list_child_namespaces() {
        let mgr = HierarchicalNamespaceManager::new(NamespaceConfig::default());

        mgr.create_namespace("acme", "Acme", "admin", None)
            .await
            .unwrap();
        mgr.create_namespace("acme/platform", "Platform", "admin", None)
            .await
            .unwrap();
        mgr.create_namespace("acme/infra", "Infra", "admin", None)
            .await
            .unwrap();
        mgr.create_namespace("acme/platform/payments", "Payments", "admin", None)
            .await
            .unwrap();

        let parent = NamespacePath::new("acme").unwrap();
        let children = mgr.list_namespaces(Some(&parent)).await;
        // Direct children of "acme" are "acme/platform" and "acme/infra",
        // but NOT "acme/platform/payments".
        assert_eq!(children.len(), 2);
    }

    // -- Usage tracking --

    #[tokio::test]
    async fn test_usage_tracking() {
        let mgr = HierarchicalNamespaceManager::new(NamespaceConfig::default());

        mgr.create_namespace("acme", "Acme", "admin", None)
            .await
            .unwrap();

        {
            let mut meter = mgr.meter().write().await;
            meter.set_topics("acme", 10);
            meter.set_partitions("acme", 30);
            meter.record_produce("acme", 500);
            meter.record_consume("acme", 1000);
            meter.record_connection("acme", 5);
        }

        let usage = mgr.get_usage("acme").await.unwrap();
        assert_eq!(usage.topics_count, 10);
        assert_eq!(usage.partitions_count, 30);
        assert_eq!(usage.connections, 5);
        assert!(usage.produce_rate_bytes_per_sec > 0.0);
        assert!(usage.consume_rate_bytes_per_sec > 0.0);
    }

    #[test]
    fn test_usage_meter_reset_rates() {
        let mut meter = UsageMeter::new();
        meter.record_produce("ns", 1000);
        meter.record_consume("ns", 2000);

        let quotas = NamespaceQuotas::default();
        let usage = meter.get_usage("ns", &quotas);
        assert!(usage.produce_rate_bytes_per_sec > 0.0);

        meter.reset_rates();
        let usage = meter.get_usage("ns", &quotas);
        assert_eq!(usage.produce_rate_bytes_per_sec, 0.0);
        assert_eq!(usage.consume_rate_bytes_per_sec, 0.0);
    }

    // -- Validate operation (quota enforcement) --

    #[tokio::test]
    async fn test_validate_operation_suspended_namespace() {
        let mgr = HierarchicalNamespaceManager::new(NamespaceConfig::default());

        mgr.create_namespace("acme", "Acme", "admin", None)
            .await
            .unwrap();
        mgr.suspend_namespace("acme").await.unwrap();

        let err = mgr
            .validate_operation("acme", NamespaceOperation::Produce)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("not active"));
    }

    #[tokio::test]
    async fn test_validate_operation_topic_quota_exceeded() {
        let small_quotas = NamespaceQuotas {
            max_topics: 2,
            ..Default::default()
        };
        let mgr = HierarchicalNamespaceManager::new(NamespaceConfig::default());

        mgr.create_namespace("acme", "Acme", "admin", Some(small_quotas))
            .await
            .unwrap();

        {
            let mut meter = mgr.meter().write().await;
            meter.set_topics("acme", 2);
        }

        let err = mgr
            .validate_operation("acme", NamespaceOperation::CreateTopic)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("maximum topic count"));
    }

    // -- Authorization --

    #[test]
    fn test_authz_grant_and_check() {
        let mut authz = NamespaceAuthz::new();
        let ns = NamespacePath::new("acme/platform").unwrap();

        authz.grant_access(
            "alice",
            &ns,
            vec![NamespaceOperation::Produce, NamespaceOperation::Consume],
            "admin",
        );

        assert!(authz
            .check_access("alice", &ns, NamespaceOperation::Produce)
            .unwrap());
        assert!(authz
            .check_access("alice", &ns, NamespaceOperation::Consume)
            .unwrap());
        assert!(!authz
            .check_access("alice", &ns, NamespaceOperation::CreateTopic)
            .unwrap());
        assert!(!authz
            .check_access("bob", &ns, NamespaceOperation::Produce)
            .unwrap());
    }

    #[test]
    fn test_authz_revoke() {
        let mut authz = NamespaceAuthz::new();
        let ns = NamespacePath::new("acme").unwrap();

        authz.grant_access("alice", &ns, vec![NamespaceOperation::Produce], "admin");
        assert!(authz
            .check_access("alice", &ns, NamespaceOperation::Produce)
            .unwrap());

        authz.revoke_access("alice", &ns);
        assert!(!authz
            .check_access("alice", &ns, NamespaceOperation::Produce)
            .unwrap());
    }

    #[test]
    fn test_authz_ancestor_manage_implies_child_access() {
        let mut authz = NamespaceAuthz::new();
        let parent = NamespacePath::new("acme").unwrap();
        let child = NamespacePath::new("acme/platform").unwrap();

        authz.grant_access(
            "admin_user",
            &parent,
            vec![NamespaceOperation::ManageNamespace],
            "root",
        );

        // ManageNamespace on an ancestor implies all operations on descendants.
        assert!(authz
            .check_access("admin_user", &child, NamespaceOperation::Produce)
            .unwrap());
        assert!(authz
            .check_access("admin_user", &child, NamespaceOperation::CreateTopic)
            .unwrap());
    }

    #[test]
    fn test_authz_list_grants() {
        let mut authz = NamespaceAuthz::new();
        let ns = NamespacePath::new("acme").unwrap();

        authz.grant_access("alice", &ns, vec![NamespaceOperation::Produce], "admin");
        authz.grant_access("bob", &ns, vec![NamespaceOperation::Consume], "admin");

        let grants = authz.list_grants(&ns);
        assert_eq!(grants.len(), 2);
    }

    // -- Namespace defaults --

    #[test]
    fn test_default_quotas() {
        let q = NamespaceQuotas::default();
        assert_eq!(q.max_topics, 100);
        assert_eq!(q.max_partitions, 1000);
        assert_eq!(q.max_storage_bytes, 10 * 1024 * 1024 * 1024);
        assert_eq!(q.max_produce_rate_bytes_per_sec, 100 * 1024 * 1024);
        assert_eq!(q.max_consume_rate_bytes_per_sec, 200 * 1024 * 1024);
        assert_eq!(q.max_connections, 100);
        assert_eq!(q.max_consumer_groups, 50);
    }

    #[test]
    fn test_default_config() {
        let c = NamespaceConfig::default();
        assert_eq!(c.max_namespaces, 1000);
        assert!(c.enable_metering);
        assert_eq!(c.separator, "/");
    }

    // -- Connection tracking --

    #[test]
    fn test_usage_meter_connection_tracking() {
        let mut meter = UsageMeter::new();
        meter.record_connection("ns", 3);
        meter.record_connection("ns", 2);
        let quotas = NamespaceQuotas::default();
        let usage = meter.get_usage("ns", &quotas);
        assert_eq!(usage.connections, 5);

        meter.record_connection("ns", -2);
        let usage = meter.get_usage("ns", &quotas);
        assert_eq!(usage.connections, 3);
    }

    // -- Max namespace limit --

    #[tokio::test]
    async fn test_max_namespaces_limit() {
        let config = NamespaceConfig {
            max_namespaces: 2,
            ..Default::default()
        };
        let mgr = HierarchicalNamespaceManager::new(config);

        mgr.create_namespace("ns1", "NS1", "admin", None)
            .await
            .unwrap();
        mgr.create_namespace("ns2", "NS2", "admin", None)
            .await
            .unwrap();

        let err = mgr
            .create_namespace("ns3", "NS3", "admin", None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("maximum namespace count"));
    }

    // -- Update quotas --

    #[tokio::test]
    async fn test_update_quotas() {
        let mgr = HierarchicalNamespaceManager::new(NamespaceConfig::default());

        mgr.create_namespace("acme", "Acme", "admin", None)
            .await
            .unwrap();

        let new_quotas = NamespaceQuotas {
            max_topics: 500,
            ..Default::default()
        };
        mgr.update_quotas("acme", new_quotas).await.unwrap();

        let ns = mgr.get_namespace("acme").await.unwrap();
        assert_eq!(ns.quotas.max_topics, 500);
    }
}
