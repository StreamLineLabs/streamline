//! Cross-Region Coordination & Health Monitoring
//!
//! Provides the operational layer for managing multi-region active-active
//! deployments. Includes region health monitoring via heartbeats, replication
//! lag tracking, automatic failover orchestration, and split-brain detection
//! with fencing tokens.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │                      REGION COORDINATOR                                │
//! ├─────────────────────────────────────────────────────────────────────────┤
//! │                                                                         │
//! │   us-east-1               eu-west-1               ap-south-1           │
//! │  ┌──────────────┐       ┌──────────────┐       ┌──────────────┐       │
//! │  │  Heartbeat   │──────►│  Heartbeat   │──────►│  Heartbeat   │       │
//! │  │  Monitor     │◄──────│  Monitor     │◄──────│  Monitor     │       │
//! │  └──────┬───────┘       └──────┬───────┘       └──────┬───────┘       │
//! │         │                      │                      │               │
//! │  ┌──────┴──────────────────────┴──────────────────────┴──────────┐    │
//! │  │                  Failure Detector                              │    │
//! │  │  • Heartbeat timeout detection                                │    │
//! │  │  • Replication lag monitoring                                  │    │
//! │  │  • Split-brain detection with fencing tokens                  │    │
//! │  └──────┬────────────────────────────────────────────────────────┘    │
//! │         │                                                             │
//! │  ┌──────┴────────────────────────────────────────────────────────┐    │
//! │  │                 Failover Orchestrator                          │    │
//! │  │  • Automatic promotion / demotion decisions                   │    │
//! │  │  • Fencing of unreachable regions                             │    │
//! │  └───────────────────────────────────────────────────────────────┘    │
//! └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Feature Gate
//!
//! This module requires the `geo-replication` feature flag:
//! ```toml
//! [dependencies]
//! streamline = { version = "0.1", features = ["geo-replication"] }
//! ```

use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the region coordinator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegionCoordinatorConfig {
    /// Interval between heartbeats in milliseconds.
    pub heartbeat_interval_ms: u64,

    /// Time after which a region is considered unreachable if no heartbeat is
    /// received, in milliseconds.
    pub heartbeat_timeout_ms: u64,

    /// Maximum tolerable replication lag in milliseconds before a region is
    /// marked as degraded.
    pub max_replication_lag_ms: u64,

    /// Whether to automatically promote/demote/fence regions on failure.
    pub enable_auto_failover: bool,

    /// Minimum number of healthy regions required for the cluster to operate.
    pub min_healthy_regions: usize,
}

impl Default for RegionCoordinatorConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval_ms: 5000,
            heartbeat_timeout_ms: 15000,
            max_replication_lag_ms: 30000,
            enable_auto_failover: false,
            min_healthy_regions: 1,
        }
    }
}

// ---------------------------------------------------------------------------
// Health & status types
// ---------------------------------------------------------------------------

/// Current operational status of a region.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RegionStatus {
    /// Region is reachable and replication lag is within bounds.
    Healthy,
    /// Region is reachable but replication lag exceeds the configured maximum.
    Degraded,
    /// Region has not sent a heartbeat within the timeout window.
    Unreachable,
    /// Region has been explicitly fenced to prevent split-brain.
    Fenced,
}

impl std::fmt::Display for RegionStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Healthy => write!(f, "Healthy"),
            Self::Degraded => write!(f, "Degraded"),
            Self::Unreachable => write!(f, "Unreachable"),
            Self::Fenced => write!(f, "Fenced"),
        }
    }
}

/// Health state for a single region tracked by the coordinator.
#[derive(Debug, Clone)]
pub struct RegionHealth {
    /// Unique identifier for the region (e.g. `"us-east-1"`).
    pub region_id: String,
    /// Current status.
    pub status: RegionStatus,
    /// When the last heartbeat was received.
    pub last_heartbeat: Instant,
    /// Current replication lag observed from this region, in milliseconds.
    pub replication_lag_ms: u64,
    /// Number of records pending replication to/from this region.
    pub pending_records: u64,
    /// Monotonically increasing fencing token used for split-brain prevention.
    pub fencing_token: u64,
}

/// Serializable snapshot of [`RegionHealth`] (Instant is not serializable).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegionHealthSnapshot {
    pub region_id: String,
    pub status: RegionStatus,
    /// Milliseconds since the last heartbeat was received.
    pub last_heartbeat_age_ms: u64,
    pub replication_lag_ms: u64,
    pub pending_records: u64,
    pub fencing_token: u64,
}

impl RegionHealth {
    /// Create a snapshot suitable for serialization.
    pub fn snapshot(&self) -> RegionHealthSnapshot {
        RegionHealthSnapshot {
            region_id: self.region_id.clone(),
            status: self.status,
            last_heartbeat_age_ms: self.last_heartbeat.elapsed().as_millis() as u64,
            replication_lag_ms: self.replication_lag_ms,
            pending_records: self.pending_records,
            fencing_token: self.fencing_token,
        }
    }
}

// ---------------------------------------------------------------------------
// Failover types
// ---------------------------------------------------------------------------

/// Action the coordinator recommends for a region.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FailoverAction {
    /// Promote a region to accept writes.
    Promote,
    /// Demote a region from accepting writes.
    Demote,
    /// Fence a region to prevent split-brain.
    Fence,
    /// No action required.
    NoAction,
}

/// A decision produced by the failure detector about a specific region.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailoverDecision {
    /// Recommended action.
    pub action: FailoverAction,
    /// Region this decision applies to.
    pub target_region: String,
    /// Human-readable reason for the decision.
    pub reason: String,
    /// Fencing token at the time of the decision.
    pub fencing_token: u64,
}

// ---------------------------------------------------------------------------
// Replication summary
// ---------------------------------------------------------------------------

/// Aggregate statistics across all tracked regions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationSummary {
    /// Total number of tracked regions (including local).
    pub total_regions: usize,
    /// Number of healthy regions.
    pub healthy_regions: usize,
    /// Number of degraded regions.
    pub degraded_regions: usize,
    /// Number of unreachable regions.
    pub unreachable_regions: usize,
    /// Number of fenced regions.
    pub fenced_regions: usize,
    /// Maximum replication lag across all regions, in milliseconds.
    pub max_replication_lag_ms: u64,
    /// Total pending records across all regions.
    pub total_pending_records: u64,
}

// ---------------------------------------------------------------------------
// RegionCoordinator
// ---------------------------------------------------------------------------

/// Cross-region coordinator responsible for health monitoring, failure
/// detection, and failover orchestration.
pub struct RegionCoordinator {
    /// Identifier of the local region.
    local_region: String,
    /// Per-region health state.
    regions: Arc<RwLock<HashMap<String, RegionHealth>>>,
    /// Coordinator configuration.
    config: RegionCoordinatorConfig,
    /// Global monotonically increasing fencing token counter.
    next_fencing_token: AtomicU64,
}

impl RegionCoordinator {
    /// Create a new region coordinator for `local_region`.
    pub fn new(local_region: impl Into<String>, config: RegionCoordinatorConfig) -> Self {
        let local_region = local_region.into();
        let coordinator = Self {
            local_region: local_region.clone(),
            regions: Arc::new(RwLock::new(HashMap::new())),
            config,
            next_fencing_token: AtomicU64::new(1),
        };

        // Eagerly register the local region as healthy.
        let local_health = RegionHealth {
            region_id: local_region,
            status: RegionStatus::Healthy,
            last_heartbeat: Instant::now(),
            replication_lag_ms: 0,
            pending_records: 0,
            fencing_token: 0,
        };

        // Use try_write because we just created the lock — it will never
        // contend here.
        if let Ok(mut regions) = coordinator.regions.try_write() {
            regions.insert(local_health.region_id.clone(), local_health);
        }

        info!(
            local_region = %coordinator.local_region,
            "Region coordinator initialised"
        );
        coordinator
    }

    // -----------------------------------------------------------------
    // Heartbeat handling
    // -----------------------------------------------------------------

    /// Record a heartbeat from `region_id`, updating its health state.
    ///
    /// If the region was previously unreachable it transitions back to healthy
    /// (or degraded, depending on the lag).
    pub async fn record_heartbeat(
        &self,
        region_id: impl Into<String>,
        replication_lag_ms: u64,
        pending_records: u64,
    ) {
        let region_id = region_id.into();
        let mut regions = self.regions.write().await;

        let status = if replication_lag_ms > self.config.max_replication_lag_ms {
            RegionStatus::Degraded
        } else {
            RegionStatus::Healthy
        };

        let entry = regions
            .entry(region_id.clone())
            .or_insert_with(|| RegionHealth {
                region_id: region_id.clone(),
                status: RegionStatus::Healthy,
                last_heartbeat: Instant::now(),
                replication_lag_ms: 0,
                pending_records: 0,
                fencing_token: 0,
            });

        // Fenced regions stay fenced — they must be explicitly unfenced.
        if entry.status != RegionStatus::Fenced {
            entry.status = status;
        }
        entry.last_heartbeat = Instant::now();
        entry.replication_lag_ms = replication_lag_ms;
        entry.pending_records = pending_records;

        debug!(
            region = %region_id,
            lag_ms = replication_lag_ms,
            pending = pending_records,
            status = %entry.status,
            "Heartbeat recorded"
        );
    }

    // -----------------------------------------------------------------
    // Health queries
    // -----------------------------------------------------------------

    /// Return a snapshot of health state for every tracked region.
    pub async fn check_region_health(&self) -> Vec<RegionHealth> {
        let regions = self.regions.read().await;
        regions.values().cloned().collect()
    }

    /// Build an aggregate [`ReplicationSummary`] across all regions.
    pub async fn get_replication_summary(&self) -> ReplicationSummary {
        let regions = self.regions.read().await;
        let mut summary = ReplicationSummary {
            total_regions: regions.len(),
            healthy_regions: 0,
            degraded_regions: 0,
            unreachable_regions: 0,
            fenced_regions: 0,
            max_replication_lag_ms: 0,
            total_pending_records: 0,
        };

        for health in regions.values() {
            match health.status {
                RegionStatus::Healthy => summary.healthy_regions += 1,
                RegionStatus::Degraded => summary.degraded_regions += 1,
                RegionStatus::Unreachable => summary.unreachable_regions += 1,
                RegionStatus::Fenced => summary.fenced_regions += 1,
            }
            if health.replication_lag_ms > summary.max_replication_lag_ms {
                summary.max_replication_lag_ms = health.replication_lag_ms;
            }
            summary.total_pending_records += health.pending_records;
        }

        summary
    }

    // -----------------------------------------------------------------
    // Failure detection
    // -----------------------------------------------------------------

    /// Evaluate all tracked regions and return failover decisions for any that
    /// require action (heartbeat timeout, excessive lag, etc.).
    ///
    /// If `enable_auto_failover` is `false` the decisions are still produced
    /// but are advisory — callers can choose whether to act on them.
    pub async fn detect_failures(&self) -> Vec<FailoverDecision> {
        let mut decisions = Vec::new();
        let mut regions = self.regions.write().await;

        let timeout = std::time::Duration::from_millis(self.config.heartbeat_timeout_ms);
        let max_lag = self.config.max_replication_lag_ms;

        for health in regions.values_mut() {
            // Skip local region and already-fenced regions.
            if health.region_id == self.local_region || health.status == RegionStatus::Fenced {
                continue;
            }

            let elapsed = health.last_heartbeat.elapsed();

            if elapsed > timeout {
                // Region missed heartbeat window.
                let previous_status = health.status;
                health.status = RegionStatus::Unreachable;

                let action = if self.config.enable_auto_failover {
                    FailoverAction::Fence
                } else {
                    FailoverAction::NoAction
                };

                let token = self.next_fencing_token.load(Ordering::SeqCst);

                warn!(
                    region = %health.region_id,
                    elapsed_ms = elapsed.as_millis() as u64,
                    previous = %previous_status,
                    "Region heartbeat timeout"
                );

                decisions.push(FailoverDecision {
                    action,
                    target_region: health.region_id.clone(),
                    reason: format!(
                        "Heartbeat timeout: {}ms elapsed (threshold: {}ms)",
                        elapsed.as_millis(),
                        self.config.heartbeat_timeout_ms,
                    ),
                    fencing_token: token,
                });
            } else if health.replication_lag_ms > max_lag
                && health.status != RegionStatus::Degraded
            {
                // Transition to degraded if lag exceeds threshold.
                health.status = RegionStatus::Degraded;

                decisions.push(FailoverDecision {
                    action: FailoverAction::Demote,
                    target_region: health.region_id.clone(),
                    reason: format!(
                        "Replication lag {}ms exceeds maximum {}ms",
                        health.replication_lag_ms, max_lag,
                    ),
                    fencing_token: health.fencing_token,
                });
            }
        }

        decisions
    }

    // -----------------------------------------------------------------
    // Fencing
    // -----------------------------------------------------------------

    /// Fence a region, preventing it from participating in replication.
    ///
    /// Returns the new monotonically increasing fencing token. Any region
    /// holding a token lower than this value must stop accepting writes.
    pub async fn fence_region(&self, region_id: &str) -> Result<u64> {
        let mut regions = self.regions.write().await;
        let health = regions.get_mut(region_id).ok_or_else(|| {
            StreamlineError::Replication(format!("Unknown region: {}", region_id))
        })?;

        let token = self.next_fencing_token.fetch_add(1, Ordering::SeqCst);
        health.status = RegionStatus::Fenced;
        health.fencing_token = token;

        warn!(
            region = %region_id,
            fencing_token = token,
            "Region fenced"
        );

        Ok(token)
    }

    /// Remove the fence from a region, allowing it to rejoin the cluster.
    ///
    /// The region transitions to `Healthy` and its heartbeat timer is reset.
    pub async fn unfence_region(&self, region_id: &str) -> Result<()> {
        let mut regions = self.regions.write().await;
        let health = regions.get_mut(region_id).ok_or_else(|| {
            StreamlineError::Replication(format!("Unknown region: {}", region_id))
        })?;

        if health.status != RegionStatus::Fenced {
            return Err(StreamlineError::Replication(format!(
                "Region {} is not fenced (current status: {})",
                region_id, health.status,
            )));
        }

        health.status = RegionStatus::Healthy;
        health.last_heartbeat = Instant::now();

        info!(
            region = %region_id,
            fencing_token = health.fencing_token,
            "Region unfenced"
        );

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{self, Duration};

    fn default_config() -> RegionCoordinatorConfig {
        RegionCoordinatorConfig::default()
    }

    fn fast_timeout_config() -> RegionCoordinatorConfig {
        RegionCoordinatorConfig {
            heartbeat_interval_ms: 50,
            heartbeat_timeout_ms: 100,
            max_replication_lag_ms: 500,
            enable_auto_failover: true,
            min_healthy_regions: 1,
        }
    }

    // --- Construction ---

    #[tokio::test]
    async fn test_new_registers_local_region() {
        let coord = RegionCoordinator::new("us-east-1", default_config());
        let health = coord.check_region_health().await;
        assert_eq!(health.len(), 1);
        assert_eq!(health[0].region_id, "us-east-1");
        assert_eq!(health[0].status, RegionStatus::Healthy);
    }

    #[tokio::test]
    async fn test_default_config_values() {
        let cfg = RegionCoordinatorConfig::default();
        assert_eq!(cfg.heartbeat_interval_ms, 5000);
        assert_eq!(cfg.heartbeat_timeout_ms, 15000);
        assert_eq!(cfg.max_replication_lag_ms, 30000);
        assert!(!cfg.enable_auto_failover);
        assert_eq!(cfg.min_healthy_regions, 1);
    }

    // --- Heartbeat ---

    #[tokio::test]
    async fn test_record_heartbeat_creates_region() {
        let coord = RegionCoordinator::new("us-east-1", default_config());
        coord.record_heartbeat("eu-west-1", 100, 5).await;

        let health = coord.check_region_health().await;
        assert_eq!(health.len(), 2);

        let eu = health.iter().find(|h| h.region_id == "eu-west-1").unwrap();
        assert_eq!(eu.status, RegionStatus::Healthy);
        assert_eq!(eu.replication_lag_ms, 100);
        assert_eq!(eu.pending_records, 5);
    }

    #[tokio::test]
    async fn test_heartbeat_updates_existing_region() {
        let coord = RegionCoordinator::new("us-east-1", default_config());
        coord.record_heartbeat("eu-west-1", 100, 5).await;
        coord.record_heartbeat("eu-west-1", 200, 10).await;

        let health = coord.check_region_health().await;
        let eu = health.iter().find(|h| h.region_id == "eu-west-1").unwrap();
        assert_eq!(eu.replication_lag_ms, 200);
        assert_eq!(eu.pending_records, 10);
    }

    #[tokio::test]
    async fn test_high_lag_marks_degraded() {
        let config = RegionCoordinatorConfig {
            max_replication_lag_ms: 500,
            ..default_config()
        };
        let coord = RegionCoordinator::new("us-east-1", config);
        coord.record_heartbeat("eu-west-1", 1000, 0).await;

        let health = coord.check_region_health().await;
        let eu = health.iter().find(|h| h.region_id == "eu-west-1").unwrap();
        assert_eq!(eu.status, RegionStatus::Degraded);
    }

    #[tokio::test]
    async fn test_heartbeat_does_not_unfence() {
        let coord = RegionCoordinator::new("us-east-1", default_config());
        coord.record_heartbeat("eu-west-1", 0, 0).await;
        coord.fence_region("eu-west-1").await.unwrap();

        // Heartbeat should not change fenced status.
        coord.record_heartbeat("eu-west-1", 0, 0).await;
        let health = coord.check_region_health().await;
        let eu = health.iter().find(|h| h.region_id == "eu-west-1").unwrap();
        assert_eq!(eu.status, RegionStatus::Fenced);
    }

    // --- Failure detection ---

    #[tokio::test]
    async fn test_detect_failures_timeout() {
        let coord = RegionCoordinator::new("us-east-1", fast_timeout_config());
        coord.record_heartbeat("eu-west-1", 0, 0).await;

        // Wait for heartbeat to expire.
        time::sleep(Duration::from_millis(150)).await;

        let decisions = coord.detect_failures().await;
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].target_region, "eu-west-1");
        assert_eq!(decisions[0].action, FailoverAction::Fence);
        assert!(decisions[0].reason.contains("Heartbeat timeout"));
    }

    #[tokio::test]
    async fn test_detect_failures_no_action_without_auto_failover() {
        let config = RegionCoordinatorConfig {
            heartbeat_timeout_ms: 100,
            enable_auto_failover: false,
            ..default_config()
        };
        let coord = RegionCoordinator::new("us-east-1", config);
        coord.record_heartbeat("eu-west-1", 0, 0).await;

        time::sleep(Duration::from_millis(150)).await;

        let decisions = coord.detect_failures().await;
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].action, FailoverAction::NoAction);
    }

    #[tokio::test]
    async fn test_detect_failures_skips_local_region() {
        let coord = RegionCoordinator::new("us-east-1", fast_timeout_config());

        time::sleep(Duration::from_millis(150)).await;

        let decisions = coord.detect_failures().await;
        assert!(decisions.is_empty(), "local region should not be evaluated");
    }

    #[tokio::test]
    async fn test_detect_failures_skips_fenced_region() {
        let coord = RegionCoordinator::new("us-east-1", fast_timeout_config());
        coord.record_heartbeat("eu-west-1", 0, 0).await;
        coord.fence_region("eu-west-1").await.unwrap();

        time::sleep(Duration::from_millis(150)).await;

        let decisions = coord.detect_failures().await;
        assert!(decisions.is_empty(), "fenced regions should be skipped");
    }

    #[tokio::test]
    async fn test_detect_failures_lag_demote() {
        let config = RegionCoordinatorConfig {
            max_replication_lag_ms: 100,
            heartbeat_timeout_ms: 60_000,
            ..default_config()
        };
        let coord = RegionCoordinator::new("us-east-1", config);
        // First heartbeat is healthy (lag within bounds).
        coord.record_heartbeat("eu-west-1", 50, 0).await;

        // Manually set lag above threshold without going through heartbeat
        // (heartbeat itself would already mark degraded, so we test the
        // detect_failures path by setting lag after creation).
        {
            let mut regions = coord.regions.write().await;
            let h = regions.get_mut("eu-west-1").unwrap();
            h.replication_lag_ms = 200;
            h.status = RegionStatus::Healthy; // force back to healthy
        }

        let decisions = coord.detect_failures().await;
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].action, FailoverAction::Demote);
    }

    // --- Fencing ---

    #[tokio::test]
    async fn test_fence_region() {
        let coord = RegionCoordinator::new("us-east-1", default_config());
        coord.record_heartbeat("eu-west-1", 0, 0).await;

        let token = coord.fence_region("eu-west-1").await.unwrap();
        assert!(token > 0);

        let health = coord.check_region_health().await;
        let eu = health.iter().find(|h| h.region_id == "eu-west-1").unwrap();
        assert_eq!(eu.status, RegionStatus::Fenced);
        assert_eq!(eu.fencing_token, token);
    }

    #[tokio::test]
    async fn test_fence_unknown_region_errors() {
        let coord = RegionCoordinator::new("us-east-1", default_config());
        let err = coord.fence_region("unknown").await.unwrap_err();
        assert!(err.to_string().contains("Unknown region"));
    }

    #[tokio::test]
    async fn test_fencing_token_monotonically_increases() {
        let coord = RegionCoordinator::new("us-east-1", default_config());
        coord.record_heartbeat("eu-west-1", 0, 0).await;
        coord.record_heartbeat("ap-south-1", 0, 0).await;

        let t1 = coord.fence_region("eu-west-1").await.unwrap();
        // Unfence so we can fence again.
        coord.unfence_region("eu-west-1").await.unwrap();
        let t2 = coord.fence_region("eu-west-1").await.unwrap();
        let t3 = coord.fence_region("ap-south-1").await.unwrap();

        assert!(t2 > t1, "tokens must increase: t2={t2} > t1={t1}");
        assert!(t3 > t2, "tokens must increase: t3={t3} > t2={t2}");
    }

    // --- Unfencing ---

    #[tokio::test]
    async fn test_unfence_region() {
        let coord = RegionCoordinator::new("us-east-1", default_config());
        coord.record_heartbeat("eu-west-1", 0, 0).await;
        coord.fence_region("eu-west-1").await.unwrap();

        coord.unfence_region("eu-west-1").await.unwrap();

        let health = coord.check_region_health().await;
        let eu = health.iter().find(|h| h.region_id == "eu-west-1").unwrap();
        assert_eq!(eu.status, RegionStatus::Healthy);
    }

    #[tokio::test]
    async fn test_unfence_non_fenced_errors() {
        let coord = RegionCoordinator::new("us-east-1", default_config());
        coord.record_heartbeat("eu-west-1", 0, 0).await;

        let err = coord.unfence_region("eu-west-1").await.unwrap_err();
        assert!(err.to_string().contains("not fenced"));
    }

    #[tokio::test]
    async fn test_unfence_unknown_region_errors() {
        let coord = RegionCoordinator::new("us-east-1", default_config());
        let err = coord.unfence_region("unknown").await.unwrap_err();
        assert!(err.to_string().contains("Unknown region"));
    }

    // --- Replication summary ---

    #[tokio::test]
    async fn test_replication_summary() {
        let config = RegionCoordinatorConfig {
            max_replication_lag_ms: 500,
            ..default_config()
        };
        let coord = RegionCoordinator::new("us-east-1", config);
        coord.record_heartbeat("eu-west-1", 100, 10).await;
        coord.record_heartbeat("ap-south-1", 1000, 20).await; // degraded
        coord.record_heartbeat("sa-east-1", 50, 5).await;
        coord.fence_region("sa-east-1").await.unwrap();

        let summary = coord.get_replication_summary().await;
        assert_eq!(summary.total_regions, 4); // local + 3
        assert_eq!(summary.healthy_regions, 2); // local + eu
        assert_eq!(summary.degraded_regions, 1); // ap
        assert_eq!(summary.fenced_regions, 1); // sa
        assert_eq!(summary.unreachable_regions, 0);
        assert_eq!(summary.max_replication_lag_ms, 1000);
        assert_eq!(summary.total_pending_records, 35); // 10 + 20 + 5
    }

    #[tokio::test]
    async fn test_replication_summary_empty() {
        let coord = RegionCoordinator::new("us-east-1", default_config());
        let summary = coord.get_replication_summary().await;
        assert_eq!(summary.total_regions, 1);
        assert_eq!(summary.healthy_regions, 1);
        assert_eq!(summary.max_replication_lag_ms, 0);
    }

    // --- Snapshot ---

    #[tokio::test]
    async fn test_region_health_snapshot() {
        let coord = RegionCoordinator::new("us-east-1", default_config());
        coord.record_heartbeat("eu-west-1", 42, 7).await;

        let health = coord.check_region_health().await;
        let eu = health.iter().find(|h| h.region_id == "eu-west-1").unwrap();
        let snap = eu.snapshot();

        assert_eq!(snap.region_id, "eu-west-1");
        assert_eq!(snap.status, RegionStatus::Healthy);
        assert_eq!(snap.replication_lag_ms, 42);
        assert_eq!(snap.pending_records, 7);
        // Heartbeat age should be very small since we just recorded it.
        assert!(snap.last_heartbeat_age_ms < 1000);
    }

    // --- Display ---

    #[test]
    fn test_region_status_display() {
        assert_eq!(RegionStatus::Healthy.to_string(), "Healthy");
        assert_eq!(RegionStatus::Degraded.to_string(), "Degraded");
        assert_eq!(RegionStatus::Unreachable.to_string(), "Unreachable");
        assert_eq!(RegionStatus::Fenced.to_string(), "Fenced");
    }

    // --- Multi-region coordination scenario ---

    #[tokio::test]
    async fn test_multi_region_lifecycle() {
        let config = RegionCoordinatorConfig {
            heartbeat_timeout_ms: 100,
            max_replication_lag_ms: 500,
            enable_auto_failover: true,
            ..default_config()
        };
        let coord = RegionCoordinator::new("us-east-1", config);

        // Phase 1: all regions healthy.
        coord.record_heartbeat("eu-west-1", 50, 2).await;
        coord.record_heartbeat("ap-south-1", 80, 3).await;

        let summary = coord.get_replication_summary().await;
        assert_eq!(summary.healthy_regions, 3);

        // Phase 2: eu-west-1 goes silent.
        time::sleep(Duration::from_millis(150)).await;

        // Keep ap-south-1 alive.
        coord.record_heartbeat("ap-south-1", 80, 3).await;

        let decisions = coord.detect_failures().await;
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].target_region, "eu-west-1");
        assert_eq!(decisions[0].action, FailoverAction::Fence);

        // Phase 3: fence eu-west-1.
        let token = coord.fence_region("eu-west-1").await.unwrap();
        assert!(token > 0);

        let summary = coord.get_replication_summary().await;
        assert_eq!(summary.fenced_regions, 1);
        assert_eq!(summary.healthy_regions, 2);

        // Phase 4: eu-west-1 comes back — unfence.
        coord.unfence_region("eu-west-1").await.unwrap();
        coord.record_heartbeat("eu-west-1", 10, 0).await;

        let summary = coord.get_replication_summary().await;
        assert_eq!(summary.healthy_regions, 3);
        assert_eq!(summary.fenced_regions, 0);
    }
}
