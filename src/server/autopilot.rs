//! Auto-Pilot Mode Engine
//!
//! Automatically manages topics, partitions, compaction, scaling, and healing
//! based on configurable thresholds and heuristics.

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// The auto-pilot engine that evaluates cluster state and takes corrective actions.
pub struct AutoPilot {
    config: AutoPilotConfig,
    actions: Arc<RwLock<Vec<AutoPilotAction>>>,
    stats: Arc<AutoPilotStats>,
}

/// Configuration knobs for the auto-pilot engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoPilotConfig {
    pub enabled: bool,
    pub auto_create_topics: bool,
    pub auto_tune_partitions: bool,
    pub auto_compact: bool,
    pub auto_scale: bool,
    pub auto_heal: bool,
    pub min_partitions: u32,
    pub max_partitions: u32,
    pub compaction_threshold_pct: f64,
    pub scale_up_threshold_pct: f64,
    pub scale_down_threshold_pct: f64,
    pub heal_timeout_secs: u64,
    pub action_cooldown_secs: u64,
}

/// A recorded auto-pilot action.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoPilotAction {
    pub id: String,
    pub action_type: ActionType,
    pub target: String,
    pub reason: String,
    pub status: ActionStatus,
    pub created_at: String,
    pub completed_at: Option<String>,
    pub reverted: bool,
}

/// The kind of action the auto-pilot performed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ActionType {
    TopicCreated { partitions: u32, retention_ms: u64 },
    PartitionsAdded { from: u32, to: u32 },
    Compacted { freed_bytes: u64 },
    ScaledUp { from: i32, to: i32 },
    ScaledDown { from: i32, to: i32 },
    Healed { issue: String, remedy: String },
    ConfigTuned { key: String, old: String, new: String },
}

/// Status of an auto-pilot action.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ActionStatus {
    Pending,
    Executing,
    Completed,
    Failed(String),
    Reverted,
}

/// A point-in-time snapshot of auto-pilot statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoPilotSnapshot {
    pub total_actions: u64,
    pub successful: u64,
    pub failed: u64,
    pub reverted: u64,
    pub topics_auto_created: u64,
    pub partitions_added: u64,
    pub compactions: u64,
    pub scale_events: u64,
    pub heals: u64,
}

/// Atomic counters backing the snapshot.
struct AutoPilotStats {
    total_actions: AtomicU64,
    successful: AtomicU64,
    failed: AtomicU64,
    reverted: AtomicU64,
    topics_auto_created: AtomicU64,
    partitions_added: AtomicU64,
    compactions: AtomicU64,
    scale_events: AtomicU64,
    heals: AtomicU64,
}

// ---------------------------------------------------------------------------
// Default implementations
// ---------------------------------------------------------------------------

impl Default for AutoPilotConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            auto_create_topics: true,
            auto_tune_partitions: true,
            auto_compact: true,
            auto_scale: true,
            auto_heal: true,
            min_partitions: 1,
            max_partitions: 128,
            compaction_threshold_pct: 50.0,
            scale_up_threshold_pct: 80.0,
            scale_down_threshold_pct: 20.0,
            heal_timeout_secs: 30,
            action_cooldown_secs: 60,
        }
    }
}

impl Default for AutoPilotStats {
    fn default() -> Self {
        Self {
            total_actions: AtomicU64::new(0),
            successful: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            reverted: AtomicU64::new(0),
            topics_auto_created: AtomicU64::new(0),
            partitions_added: AtomicU64::new(0),
            compactions: AtomicU64::new(0),
            scale_events: AtomicU64::new(0),
            heals: AtomicU64::new(0),
        }
    }
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

impl AutoPilot {
    /// Create a new auto-pilot engine with the given configuration.
    pub fn new(config: AutoPilotConfig) -> Self {
        info!(
            enabled = config.enabled,
            auto_create = config.auto_create_topics,
            auto_scale = config.auto_scale,
            "Initializing auto-pilot engine"
        );
        Self {
            config,
            actions: Arc::new(RwLock::new(Vec::new())),
            stats: Arc::new(AutoPilotStats::default()),
        }
    }

    /// Determine whether a topic should be auto-created.
    pub fn should_create_topic(&self, name: &str) -> bool {
        if !self.config.enabled || !self.config.auto_create_topics {
            return false;
        }
        // Reject internal / reserved prefixes
        if name.starts_with("__") || name.is_empty() {
            debug!(topic = %name, "Skipping auto-create for reserved topic");
            return false;
        }
        true
    }

    /// Build the action for auto-creating a topic.
    pub fn auto_create_topic(&self, name: &str) -> ActionType {
        let partitions = self.config.min_partitions.max(1);
        let retention_ms = 7 * 24 * 60 * 60 * 1000; // 7 days default
        info!(topic = %name, partitions, "Auto-creating topic");
        ActionType::TopicCreated {
            partitions,
            retention_ms,
        }
    }

    /// Evaluate whether a topic needs more (or fewer) partitions.
    pub fn evaluate_partitions(
        &self,
        topic: &str,
        throughput_bytes_sec: f64,
        partition_count: u32,
    ) -> Option<ActionType> {
        if !self.config.enabled || !self.config.auto_tune_partitions {
            return None;
        }

        // Heuristic: ~10 MB/s per partition is ideal
        const BYTES_PER_PARTITION: f64 = 10_000_000.0;
        let desired = ((throughput_bytes_sec / BYTES_PER_PARTITION).ceil() as u32)
            .max(self.config.min_partitions)
            .min(self.config.max_partitions);

        if desired > partition_count {
            info!(topic = %topic, from = partition_count, to = desired, "Partition increase recommended");
            Some(ActionType::PartitionsAdded {
                from: partition_count,
                to: desired,
            })
        } else {
            None
        }
    }

    /// Evaluate whether a topic should be compacted.
    pub fn evaluate_compaction(
        &self,
        topic: &str,
        size_bytes: u64,
        compactable_pct: f64,
    ) -> Option<ActionType> {
        if !self.config.enabled || !self.config.auto_compact {
            return None;
        }
        if compactable_pct >= self.config.compaction_threshold_pct {
            let freed = (size_bytes as f64 * compactable_pct / 100.0) as u64;
            info!(topic = %topic, freed_bytes = freed, "Compaction recommended");
            Some(ActionType::Compacted { freed_bytes: freed })
        } else {
            None
        }
    }

    /// Evaluate whether the cluster should scale up or down.
    pub fn evaluate_scaling(
        &self,
        cpu_pct: f64,
        mem_pct: f64,
        _connections: u64,
    ) -> Option<ActionType> {
        if !self.config.enabled || !self.config.auto_scale {
            return None;
        }
        let peak = cpu_pct.max(mem_pct);
        if peak >= self.config.scale_up_threshold_pct {
            info!(cpu = cpu_pct, mem = mem_pct, "Scale-up recommended");
            Some(ActionType::ScaledUp { from: 1, to: 2 })
        } else if peak <= self.config.scale_down_threshold_pct {
            info!(cpu = cpu_pct, mem = mem_pct, "Scale-down recommended");
            Some(ActionType::ScaledDown { from: 2, to: 1 })
        } else {
            None
        }
    }

    /// Evaluate cluster health and suggest remediation.
    pub fn evaluate_health(
        &self,
        error_count: u64,
        timeout_count: u64,
    ) -> Option<ActionType> {
        if !self.config.enabled || !self.config.auto_heal {
            return None;
        }
        let threshold = 10u64;
        if error_count > threshold || timeout_count > threshold {
            let issue = format!("errors={error_count}, timeouts={timeout_count}");
            let remedy = if timeout_count > error_count {
                "increase heal_timeout_secs".to_string()
            } else {
                "restart unhealthy partitions".to_string()
            };
            warn!(issue = %issue, remedy = %remedy, "Health issue detected");
            Some(ActionType::Healed { issue, remedy })
        } else {
            None
        }
    }

    /// Execute (record) an auto-pilot action.
    pub async fn execute_action(&self, action_type: ActionType, target: &str, reason: &str) -> Result<String, String> {
        let now = timestamp_now();
        let action = AutoPilotAction {
            id: Uuid::new_v4().to_string(),
            action_type: action_type.clone(),
            target: target.into(),
            reason: reason.into(),
            status: ActionStatus::Completed,
            created_at: now.clone(),
            completed_at: Some(now),
            reverted: false,
        };

        let id = action.id.clone();
        info!(id = %id, target = %target, "Executing auto-pilot action");

        self.actions.write().await.push(action);
        self.stats.total_actions.fetch_add(1, Ordering::Relaxed);
        self.stats.successful.fetch_add(1, Ordering::Relaxed);

        // Update category counters
        match action_type {
            ActionType::TopicCreated { .. } => {
                self.stats.topics_auto_created.fetch_add(1, Ordering::Relaxed);
            }
            ActionType::PartitionsAdded { .. } => {
                self.stats.partitions_added.fetch_add(1, Ordering::Relaxed);
            }
            ActionType::Compacted { .. } => {
                self.stats.compactions.fetch_add(1, Ordering::Relaxed);
            }
            ActionType::ScaledUp { .. } | ActionType::ScaledDown { .. } => {
                self.stats.scale_events.fetch_add(1, Ordering::Relaxed);
            }
            ActionType::Healed { .. } => {
                self.stats.heals.fetch_add(1, Ordering::Relaxed);
            }
            ActionType::ConfigTuned { .. } => {}
        }

        Ok(id)
    }

    /// Revert a previously executed action by its id.
    pub async fn revert_action(&self, id: &str) -> Result<(), String> {
        let mut actions = self.actions.write().await;
        let action = actions
            .iter_mut()
            .find(|a| a.id == id)
            .ok_or_else(|| "action not found".to_string())?;

        if action.reverted {
            return Err("action already reverted".into());
        }
        if action.status == ActionStatus::Pending || action.status == ActionStatus::Executing {
            return Err("cannot revert an in-progress action".into());
        }

        action.reverted = true;
        action.status = ActionStatus::Reverted;
        info!(id = %id, "Auto-pilot action reverted");
        self.stats.reverted.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Return all recorded actions.
    pub async fn get_actions(&self) -> Vec<AutoPilotAction> {
        self.actions.read().await.clone()
    }

    /// Return a point-in-time snapshot of auto-pilot stats.
    pub fn snapshot(&self) -> AutoPilotSnapshot {
        AutoPilotSnapshot {
            total_actions: self.stats.total_actions.load(Ordering::Relaxed),
            successful: self.stats.successful.load(Ordering::Relaxed),
            failed: self.stats.failed.load(Ordering::Relaxed),
            reverted: self.stats.reverted.load(Ordering::Relaxed),
            topics_auto_created: self.stats.topics_auto_created.load(Ordering::Relaxed),
            partitions_added: self.stats.partitions_added.load(Ordering::Relaxed),
            compactions: self.stats.compactions.load(Ordering::Relaxed),
            scale_events: self.stats.scale_events.load(Ordering::Relaxed),
            heals: self.stats.heals.load(Ordering::Relaxed),
        }
    }
}

fn timestamp_now() -> String {
    let d = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    format!("{}", d.as_secs())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn default_autopilot() -> AutoPilot {
        AutoPilot::new(AutoPilotConfig::default())
    }

    #[test]
    fn test_new_autopilot() {
        let ap = default_autopilot();
        let snap = ap.snapshot();
        assert_eq!(snap.total_actions, 0);
        assert_eq!(snap.successful, 0);
    }

    #[test]
    fn test_should_create_topic_enabled() {
        let ap = default_autopilot();
        assert!(ap.should_create_topic("orders"));
    }

    #[test]
    fn test_should_create_topic_disabled() {
        let cfg = AutoPilotConfig { auto_create_topics: false, ..Default::default() };
        let ap = AutoPilot::new(cfg);
        assert!(!ap.should_create_topic("orders"));
    }

    #[test]
    fn test_should_create_topic_reserved() {
        let ap = default_autopilot();
        assert!(!ap.should_create_topic("__consumer_offsets"));
        assert!(!ap.should_create_topic(""));
    }

    #[test]
    fn test_auto_create_topic() {
        let ap = default_autopilot();
        match ap.auto_create_topic("events") {
            ActionType::TopicCreated { partitions, retention_ms } => {
                assert!(partitions >= 1);
                assert!(retention_ms > 0);
            }
            _ => panic!("expected TopicCreated"),
        }
    }

    #[test]
    fn test_evaluate_partitions_increase() {
        let ap = default_autopilot();
        let result = ap.evaluate_partitions("t", 50_000_000.0, 2);
        assert!(result.is_some());
        if let Some(ActionType::PartitionsAdded { from, to }) = result {
            assert_eq!(from, 2);
            assert!(to > 2);
        }
    }

    #[test]
    fn test_evaluate_partitions_no_change() {
        let ap = default_autopilot();
        let result = ap.evaluate_partitions("t", 5_000_000.0, 4);
        assert!(result.is_none());
    }

    #[test]
    fn test_evaluate_partitions_disabled() {
        let cfg = AutoPilotConfig { auto_tune_partitions: false, ..Default::default() };
        let ap = AutoPilot::new(cfg);
        assert!(ap.evaluate_partitions("t", 100_000_000.0, 1).is_none());
    }

    #[test]
    fn test_evaluate_compaction_triggered() {
        let ap = default_autopilot();
        let result = ap.evaluate_compaction("t", 1_000_000, 60.0);
        assert!(result.is_some());
        if let Some(ActionType::Compacted { freed_bytes }) = result {
            assert!(freed_bytes > 0);
        }
    }

    #[test]
    fn test_evaluate_compaction_below_threshold() {
        let ap = default_autopilot();
        assert!(ap.evaluate_compaction("t", 1_000_000, 10.0).is_none());
    }

    #[test]
    fn test_evaluate_scaling_up() {
        let ap = default_autopilot();
        let result = ap.evaluate_scaling(90.0, 70.0, 100);
        assert!(matches!(result, Some(ActionType::ScaledUp { .. })));
    }

    #[test]
    fn test_evaluate_scaling_down() {
        let ap = default_autopilot();
        let result = ap.evaluate_scaling(10.0, 15.0, 10);
        assert!(matches!(result, Some(ActionType::ScaledDown { .. })));
    }

    #[test]
    fn test_evaluate_scaling_stable() {
        let ap = default_autopilot();
        assert!(ap.evaluate_scaling(50.0, 50.0, 50).is_none());
    }

    #[test]
    fn test_evaluate_health_errors() {
        let ap = default_autopilot();
        let result = ap.evaluate_health(20, 5);
        assert!(result.is_some());
        if let Some(ActionType::Healed { remedy, .. }) = result {
            assert!(remedy.contains("restart"));
        }
    }

    #[test]
    fn test_evaluate_health_timeouts() {
        let ap = default_autopilot();
        let result = ap.evaluate_health(5, 20);
        assert!(result.is_some());
        if let Some(ActionType::Healed { remedy, .. }) = result {
            assert!(remedy.contains("timeout"));
        }
    }

    #[test]
    fn test_evaluate_health_ok() {
        let ap = default_autopilot();
        assert!(ap.evaluate_health(2, 3).is_none());
    }

    #[tokio::test]
    async fn test_execute_action() {
        let ap = default_autopilot();
        let action = ActionType::TopicCreated { partitions: 3, retention_ms: 86400000 };
        let id = ap.execute_action(action, "orders", "first produce").await.unwrap();
        assert!(!id.is_empty());
        assert_eq!(ap.snapshot().total_actions, 1);
        assert_eq!(ap.snapshot().topics_auto_created, 1);
    }

    #[tokio::test]
    async fn test_execute_compaction_action() {
        let ap = default_autopilot();
        let action = ActionType::Compacted { freed_bytes: 1024 };
        ap.execute_action(action, "logs", "high compactable %").await.unwrap();
        assert_eq!(ap.snapshot().compactions, 1);
    }

    #[tokio::test]
    async fn test_execute_scale_action() {
        let ap = default_autopilot();
        let action = ActionType::ScaledUp { from: 1, to: 2 };
        ap.execute_action(action, "cluster", "cpu high").await.unwrap();
        assert_eq!(ap.snapshot().scale_events, 1);
    }

    #[tokio::test]
    async fn test_revert_action() {
        let ap = default_autopilot();
        let action = ActionType::TopicCreated { partitions: 1, retention_ms: 1000 };
        let id = ap.execute_action(action, "t", "test").await.unwrap();
        ap.revert_action(&id).await.unwrap();
        assert_eq!(ap.snapshot().reverted, 1);
    }

    #[tokio::test]
    async fn test_revert_action_not_found() {
        let ap = default_autopilot();
        assert!(ap.revert_action("missing").await.is_err());
    }

    #[tokio::test]
    async fn test_revert_action_already_reverted() {
        let ap = default_autopilot();
        let action = ActionType::Compacted { freed_bytes: 0 };
        let id = ap.execute_action(action, "t", "r").await.unwrap();
        ap.revert_action(&id).await.unwrap();
        assert!(ap.revert_action(&id).await.is_err());
    }

    #[tokio::test]
    async fn test_get_actions() {
        let ap = default_autopilot();
        let a1 = ActionType::TopicCreated { partitions: 1, retention_ms: 1000 };
        let a2 = ActionType::Compacted { freed_bytes: 100 };
        ap.execute_action(a1, "t1", "r1").await.unwrap();
        ap.execute_action(a2, "t2", "r2").await.unwrap();

        let actions = ap.get_actions().await;
        assert_eq!(actions.len(), 2);
    }

    #[tokio::test]
    async fn test_snapshot_after_multiple_actions() {
        let ap = default_autopilot();
        ap.execute_action(ActionType::TopicCreated { partitions: 1, retention_ms: 1000 }, "a", "r").await.unwrap();
        ap.execute_action(ActionType::Healed { issue: "x".into(), remedy: "y".into() }, "b", "r").await.unwrap();
        ap.execute_action(ActionType::ScaledUp { from: 1, to: 3 }, "c", "r").await.unwrap();

        let snap = ap.snapshot();
        assert_eq!(snap.total_actions, 3);
        assert_eq!(snap.successful, 3);
        assert_eq!(snap.topics_auto_created, 1);
        assert_eq!(snap.heals, 1);
        assert_eq!(snap.scale_events, 1);
    }

    #[test]
    fn test_evaluate_partitions_respects_max() {
        let cfg = AutoPilotConfig { max_partitions: 4, ..Default::default() };
        let ap = AutoPilot::new(cfg);
        let result = ap.evaluate_partitions("t", 500_000_000.0, 2);
        if let Some(ActionType::PartitionsAdded { to, .. }) = result {
            assert!(to <= 4);
        }
    }

    #[test]
    fn test_disabled_autopilot() {
        let cfg = AutoPilotConfig { enabled: false, ..Default::default() };
        let ap = AutoPilot::new(cfg);
        assert!(!ap.should_create_topic("x"));
        assert!(ap.evaluate_partitions("t", 100_000_000.0, 1).is_none());
        assert!(ap.evaluate_compaction("t", 1_000_000, 99.0).is_none());
        assert!(ap.evaluate_scaling(99.0, 99.0, 1000).is_none());
        assert!(ap.evaluate_health(100, 100).is_none());
    }
}
