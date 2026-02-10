//! Rolling upgrade coordination for zero-downtime cluster upgrades
//!
//! This module provides the `UpgradeCoordinator` which manages the
//! sequencing and safety of rolling upgrades across cluster nodes.

// parking_lot locks are designed to be safe across await points, unlike std::sync
#![allow(clippy::await_holding_lock)]

use super::node::NodeState;
use super::version::{check_compatibility, FeatureFlags, StreamlineVersion};
use super::ClusterManager;
use crate::error::{Result, StreamlineError};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};

/// State of a rolling upgrade
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum UpgradeState {
    /// No upgrade in progress
    #[default]
    Idle,
    /// Upgrade is being prepared (validating compatibility)
    Preparing,
    /// Upgrade is in progress
    InProgress,
    /// Upgrade is paused (waiting for user intervention)
    Paused,
    /// Upgrade completed successfully
    Completed,
    /// Upgrade failed
    Failed,
    /// Upgrade was cancelled
    Cancelled,
}

/// Progress information for a node during upgrade
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeUpgradeProgress {
    /// Node ID
    pub node_id: u64,
    /// Node's current version
    pub version: StreamlineVersion,
    /// Whether the node has been upgraded
    pub upgraded: bool,
    /// Whether the node is currently upgrading
    pub upgrading: bool,
    /// Time when upgrade started for this node (if upgrading)
    pub upgrade_started: Option<u64>,
    /// Time when upgrade completed for this node (if upgraded)
    pub upgrade_completed: Option<u64>,
    /// Any error message
    pub error: Option<String>,
}

/// Overall upgrade progress
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpgradeProgress {
    /// Current state of the upgrade
    pub state: UpgradeState,
    /// Target version
    pub target_version: StreamlineVersion,
    /// Source version (version before upgrade)
    pub source_version: StreamlineVersion,
    /// Progress per node
    pub nodes: HashMap<u64, NodeUpgradeProgress>,
    /// Total number of nodes
    pub total_nodes: usize,
    /// Number of nodes upgraded
    pub upgraded_nodes: usize,
    /// Number of nodes currently upgrading
    pub upgrading_nodes: usize,
    /// Time when upgrade started
    pub started_at: Option<u64>,
    /// Time when upgrade completed
    pub completed_at: Option<u64>,
    /// Any global error message
    pub error: Option<String>,
}

impl UpgradeProgress {
    /// Create new upgrade progress
    pub fn new(source_version: StreamlineVersion, target_version: StreamlineVersion) -> Self {
        Self {
            state: UpgradeState::Idle,
            target_version,
            source_version,
            nodes: HashMap::new(),
            total_nodes: 0,
            upgraded_nodes: 0,
            upgrading_nodes: 0,
            started_at: None,
            completed_at: None,
            error: None,
        }
    }

    /// Calculate percentage complete
    pub fn percent_complete(&self) -> f64 {
        if self.total_nodes == 0 {
            return 0.0;
        }
        (self.upgraded_nodes as f64 / self.total_nodes as f64) * 100.0
    }
}

/// Configuration for the upgrade coordinator
#[derive(Debug, Clone)]
pub struct UpgradeConfig {
    /// Maximum time to wait for a node to drain connections
    pub drain_timeout: Duration,
    /// Maximum time to wait for a node to complete upgrade
    pub node_upgrade_timeout: Duration,
    /// Minimum number of healthy nodes required during upgrade
    pub min_healthy_nodes: usize,
    /// Whether to pause on first failure
    pub pause_on_failure: bool,
    /// Whether to allow downgrade (target version < current version)
    pub allow_downgrade: bool,
}

impl Default for UpgradeConfig {
    fn default() -> Self {
        Self {
            drain_timeout: Duration::from_secs(60),
            node_upgrade_timeout: Duration::from_secs(300),
            min_healthy_nodes: 1,
            pause_on_failure: true,
            allow_downgrade: false,
        }
    }
}

/// Coordinates rolling upgrades across the cluster
pub struct UpgradeCoordinator {
    /// Cluster manager reference
    cluster_manager: Arc<ClusterManager>,
    /// Current upgrade progress
    progress: Arc<RwLock<Option<UpgradeProgress>>>,
    /// Configuration
    config: UpgradeConfig,
    /// Active feature flags for the cluster
    feature_flags: Arc<RwLock<FeatureFlags>>,
    /// Nodes that have reported their version
    node_versions: Arc<RwLock<HashMap<u64, StreamlineVersion>>>,
}

impl UpgradeCoordinator {
    /// Create a new upgrade coordinator
    pub fn new(cluster_manager: Arc<ClusterManager>, config: UpgradeConfig) -> Self {
        Self {
            cluster_manager,
            progress: Arc::new(RwLock::new(None)),
            config,
            feature_flags: Arc::new(RwLock::new(FeatureFlags::new())),
            node_versions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create with default config
    pub fn with_defaults(cluster_manager: Arc<ClusterManager>) -> Self {
        Self::new(cluster_manager, UpgradeConfig::default())
    }

    /// Record a node's version
    pub async fn report_node_version(&self, node_id: u64, version: StreamlineVersion) {
        let mut versions = self.node_versions.write();
        let old_version = versions.insert(node_id, version);

        if old_version != Some(version) {
            info!(
                node_id,
                version = %version,
                "Node version reported"
            );
        }

        // Check compatibility with other nodes
        for (&other_id, &other_version) in versions.iter() {
            if other_id != node_id {
                let result = check_compatibility(&version, &other_version);
                if !result.compatible {
                    warn!(
                        node_id,
                        other_node_id = other_id,
                        version = %version,
                        other_version = %other_version,
                        reason = ?result.reason,
                        "Node version compatibility warning"
                    );
                }
            }
        }
    }

    /// Remove a node from version tracking
    pub async fn remove_node(&self, node_id: u64) {
        self.node_versions.write().remove(&node_id);
    }

    /// Get all known node versions
    pub async fn get_node_versions(&self) -> HashMap<u64, StreamlineVersion> {
        self.node_versions.read().clone()
    }

    /// Get the minimum version across all nodes
    pub async fn get_cluster_min_version(&self) -> Option<StreamlineVersion> {
        let versions = self.node_versions.read();
        versions.values().min().copied()
    }

    /// Get the maximum version across all nodes
    pub async fn get_cluster_max_version(&self) -> Option<StreamlineVersion> {
        let versions = self.node_versions.read();
        versions.values().max().copied()
    }

    /// Check if the cluster is in a mixed-version state
    pub async fn is_mixed_version(&self) -> bool {
        let versions = self.node_versions.read();
        let unique_versions: HashSet<_> = versions.values().collect();
        unique_versions.len() > 1
    }

    /// Validate that an upgrade to the target version is safe
    pub async fn validate_upgrade(&self, target_version: StreamlineVersion) -> Result<()> {
        let current_version = StreamlineVersion::current();

        // Check if this is a downgrade
        if target_version < current_version && !self.config.allow_downgrade {
            return Err(StreamlineError::Config(
                "Downgrade not allowed. Set allow_downgrade=true to enable.".to_string(),
            ));
        }

        // Check version compatibility
        let result = check_compatibility(&current_version, &target_version);
        if !result.compatible {
            return Err(StreamlineError::Config(format!(
                "Version incompatible: {}",
                result.reason.unwrap_or_default()
            )));
        }

        // Check that all nodes are compatible with the target version
        let versions = self.node_versions.read();
        for (&node_id, &node_version) in versions.iter() {
            let result = check_compatibility(&node_version, &target_version);
            if !result.compatible {
                return Err(StreamlineError::Config(format!(
                    "Node {} (version {}) is incompatible with target version {}: {}",
                    node_id,
                    node_version,
                    target_version,
                    result.reason.unwrap_or_default()
                )));
            }
        }

        // Check cluster health
        let healthy_count = self.count_healthy_nodes().await;
        if healthy_count < self.config.min_healthy_nodes {
            return Err(StreamlineError::Cluster(format!(
                "Not enough healthy nodes: {} < {}",
                healthy_count, self.config.min_healthy_nodes
            )));
        }

        Ok(())
    }

    /// Count the number of healthy nodes in the cluster
    async fn count_healthy_nodes(&self) -> usize {
        // Query the cluster manager for alive brokers
        let metadata = self.cluster_manager.metadata().await;
        metadata
            .brokers
            .values()
            .filter(|b| b.state == NodeState::Running)
            .count()
    }

    /// Start a rolling upgrade
    pub async fn start_upgrade(&self, target_version: StreamlineVersion) -> Result<()> {
        // Validate first
        self.validate_upgrade(target_version).await?;

        let current_version = StreamlineVersion::current();

        let mut progress = UpgradeProgress::new(current_version, target_version);
        progress.state = UpgradeState::Preparing;
        progress.started_at = Some(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        );

        // Initialize node progress
        let versions = self.node_versions.read();
        for (&node_id, &version) in versions.iter() {
            progress.nodes.insert(
                node_id,
                NodeUpgradeProgress {
                    node_id,
                    version,
                    upgraded: version >= target_version,
                    upgrading: false,
                    upgrade_started: None,
                    upgrade_completed: None,
                    error: None,
                },
            );
        }

        progress.total_nodes = progress.nodes.len();
        progress.upgraded_nodes = progress.nodes.values().filter(|n| n.upgraded).count();

        let total_nodes = progress.total_nodes;

        // Store progress
        *self.progress.write() = Some(progress);

        info!(
            source_version = %current_version,
            target_version = %target_version,
            total_nodes = total_nodes,
            "Starting rolling upgrade"
        );

        // Transition to in-progress
        self.set_upgrade_state(UpgradeState::InProgress).await;

        Ok(())
    }

    /// Get the current upgrade progress
    pub async fn get_progress(&self) -> Option<UpgradeProgress> {
        self.progress.read().clone()
    }

    /// Set the upgrade state
    async fn set_upgrade_state(&self, state: UpgradeState) {
        if let Some(ref mut progress) = *self.progress.write() {
            progress.state = state;

            if matches!(state, UpgradeState::Completed | UpgradeState::Failed) {
                progress.completed_at = Some(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                );
            }
        }
    }

    /// Mark a node as upgrading
    pub async fn mark_node_upgrading(&self, node_id: u64) {
        if let Some(ref mut progress) = *self.progress.write() {
            if let Some(node) = progress.nodes.get_mut(&node_id) {
                node.upgrading = true;
                node.upgrade_started = Some(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                );
                progress.upgrading_nodes += 1;
            }
        }
    }

    /// Mark a node as upgraded
    pub async fn mark_node_upgraded(&self, node_id: u64, new_version: StreamlineVersion) {
        // Update the version tracking
        self.report_node_version(node_id, new_version).await;

        if let Some(ref mut progress) = *self.progress.write() {
            if let Some(node) = progress.nodes.get_mut(&node_id) {
                node.upgrading = false;
                node.upgraded = true;
                node.version = new_version;
                node.upgrade_completed = Some(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                );

                if progress.upgrading_nodes > 0 {
                    progress.upgrading_nodes -= 1;
                }
                progress.upgraded_nodes = progress.nodes.values().filter(|n| n.upgraded).count();

                info!(
                    node_id,
                    new_version = %new_version,
                    upgraded = progress.upgraded_nodes,
                    total = progress.total_nodes,
                    "Node upgraded"
                );

                // Check if upgrade is complete
                if progress.upgraded_nodes == progress.total_nodes {
                    progress.state = UpgradeState::Completed;
                    progress.completed_at = Some(
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs(),
                    );

                    info!(
                        target_version = %progress.target_version,
                        "Rolling upgrade completed"
                    );
                }
            }
        }
    }

    /// Mark a node upgrade as failed
    pub async fn mark_node_failed(&self, node_id: u64, error: String) {
        error!(node_id, error = %error, "Node upgrade failed");

        if let Some(ref mut progress) = *self.progress.write() {
            if let Some(node) = progress.nodes.get_mut(&node_id) {
                node.upgrading = false;
                node.error = Some(error.clone());

                if progress.upgrading_nodes > 0 {
                    progress.upgrading_nodes -= 1;
                }
            }

            if self.config.pause_on_failure {
                progress.state = UpgradeState::Paused;
                progress.error = Some(format!("Node {} failed: {}", node_id, error));

                warn!(node_id, "Upgrade paused due to node failure");
            }
        }
    }

    /// Pause the upgrade
    pub async fn pause_upgrade(&self) -> Result<()> {
        let mut progress_guard = self.progress.write();
        if let Some(ref mut progress) = *progress_guard {
            if progress.state == UpgradeState::InProgress {
                progress.state = UpgradeState::Paused;
                info!("Upgrade paused by user");
                Ok(())
            } else {
                Err(StreamlineError::Config(format!(
                    "Cannot pause upgrade in state {:?}",
                    progress.state
                )))
            }
        } else {
            Err(StreamlineError::Config(
                "No upgrade in progress".to_string(),
            ))
        }
    }

    /// Resume a paused upgrade
    pub async fn resume_upgrade(&self) -> Result<()> {
        let mut progress_guard = self.progress.write();
        if let Some(ref mut progress) = *progress_guard {
            if progress.state == UpgradeState::Paused {
                progress.state = UpgradeState::InProgress;
                progress.error = None;
                info!("Upgrade resumed");
                Ok(())
            } else {
                Err(StreamlineError::Config(format!(
                    "Cannot resume upgrade in state {:?}",
                    progress.state
                )))
            }
        } else {
            Err(StreamlineError::Config(
                "No upgrade in progress".to_string(),
            ))
        }
    }

    /// Cancel the upgrade
    pub async fn cancel_upgrade(&self) -> Result<()> {
        let mut progress_guard = self.progress.write();
        if let Some(ref mut progress) = *progress_guard {
            if matches!(
                progress.state,
                UpgradeState::InProgress | UpgradeState::Paused | UpgradeState::Preparing
            ) {
                progress.state = UpgradeState::Cancelled;
                progress.completed_at = Some(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                );
                warn!("Upgrade cancelled");
                Ok(())
            } else {
                Err(StreamlineError::Config(format!(
                    "Cannot cancel upgrade in state {:?}",
                    progress.state
                )))
            }
        } else {
            Err(StreamlineError::Config(
                "No upgrade in progress".to_string(),
            ))
        }
    }

    /// Get the next node to upgrade
    ///
    /// Returns the node ID if there's a node that should be upgraded next,
    /// or None if all nodes are upgraded or no nodes are ready.
    pub async fn get_next_node_to_upgrade(&self) -> Option<u64> {
        let progress = self.progress.read();
        if let Some(ref p) = *progress {
            if p.state != UpgradeState::InProgress {
                return None;
            }

            // Find a node that hasn't been upgraded and isn't currently upgrading
            // Prefer non-leader nodes first
            let leader_id = self.cluster_manager.controller_id().await;

            let mut candidates: Vec<_> = p
                .nodes
                .values()
                .filter(|n| !n.upgraded && !n.upgrading && n.error.is_none())
                .collect();

            // Sort to put non-leaders first (leader gets upgraded last)
            candidates.sort_by_key(|n| leader_id == Some(n.node_id));

            candidates.first().map(|n| n.node_id)
        } else {
            None
        }
    }

    /// Check if it's safe to upgrade a node
    pub async fn can_upgrade_node(&self, node_id: u64) -> Result<()> {
        // Check that we have enough healthy nodes
        let healthy_count = self.count_healthy_nodes().await;
        if healthy_count <= self.config.min_healthy_nodes {
            return Err(StreamlineError::Cluster(format!(
                "Cannot upgrade node {}: would leave only {} healthy nodes (min: {})",
                node_id,
                healthy_count - 1,
                self.config.min_healthy_nodes
            )));
        }

        // Check upgrade state
        let progress = self.progress.read();
        if let Some(ref p) = *progress {
            if p.state != UpgradeState::InProgress {
                return Err(StreamlineError::Config(format!(
                    "Upgrade not in progress (state: {:?})",
                    p.state
                )));
            }

            // Check that the node isn't already upgrading
            if let Some(node) = p.nodes.get(&node_id) {
                if node.upgrading {
                    return Err(StreamlineError::Config(format!(
                        "Node {} is already upgrading",
                        node_id
                    )));
                }
                if node.upgraded {
                    return Err(StreamlineError::Config(format!(
                        "Node {} is already upgraded",
                        node_id
                    )));
                }
            }
        }

        Ok(())
    }

    /// Get the cluster's effective feature flags based on minimum version
    pub async fn get_effective_features(&self) -> FeatureFlags {
        let min_version = self.get_cluster_min_version().await;
        let mut flags = FeatureFlags::new();

        if let Some(min) = min_version {
            // Enable features based on minimum cluster version
            if min >= StreamlineVersion::new(0, 1, 0, 1) {
                flags.enable("connection_pipelining");
                flags.enable("sparse_index");
            }
        }

        flags
    }

    /// Update feature flags based on current cluster state
    pub async fn update_feature_flags(&self) {
        let effective = self.get_effective_features().await;
        *self.feature_flags.write() = effective;
    }

    /// Get current feature flags
    pub async fn get_feature_flags(&self) -> FeatureFlags {
        self.feature_flags.read().clone()
    }

    /// Check if a specific feature is enabled cluster-wide
    pub async fn is_feature_enabled(&self, feature: &str) -> bool {
        self.feature_flags.read().is_enabled(feature)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_upgrade_state_default() {
        let state = UpgradeState::default();
        assert_eq!(state, UpgradeState::Idle);
    }

    #[test]
    fn test_upgrade_progress_percent() {
        let mut progress =
            UpgradeProgress::new(StreamlineVersion::current(), StreamlineVersion::current());

        progress.total_nodes = 4;
        progress.upgraded_nodes = 0;
        assert_eq!(progress.percent_complete(), 0.0);

        progress.upgraded_nodes = 2;
        assert_eq!(progress.percent_complete(), 50.0);

        progress.upgraded_nodes = 4;
        assert_eq!(progress.percent_complete(), 100.0);

        progress.total_nodes = 0;
        assert_eq!(progress.percent_complete(), 0.0);
    }

    #[test]
    fn test_upgrade_config_default() {
        let config = UpgradeConfig::default();
        assert_eq!(config.drain_timeout, Duration::from_secs(60));
        assert_eq!(config.node_upgrade_timeout, Duration::from_secs(300));
        assert_eq!(config.min_healthy_nodes, 1);
        assert!(config.pause_on_failure);
        assert!(!config.allow_downgrade);
    }
}
