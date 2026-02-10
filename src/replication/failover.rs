//! Region-Aware Failover and Consumer Routing
//!
//! Provides:
//! - Automatic failover detection and promotion
//! - Region-aware consumer routing (prefer local region)
//! - Split-brain detection and resolution
//! - Cross-region health monitoring

use crate::error::{Result, StreamlineError};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{info, warn};

/// Failover configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailoverConfig {
    /// Health check interval
    pub health_check_interval: Duration,
    /// Number of consecutive failures before failover
    pub failure_threshold: u32,
    /// Fencing token epoch for split-brain prevention
    pub fencing_epoch: u64,
    /// Grace period before promoting standby
    pub failover_grace_period: Duration,
    /// Enable automatic failover
    pub auto_failover: bool,
    /// Maximum allowed replication lag before considering region unhealthy
    pub max_replication_lag_ms: u64,
}

impl Default for FailoverConfig {
    fn default() -> Self {
        Self {
            health_check_interval: Duration::from_secs(5),
            failure_threshold: 3,
            fencing_epoch: 0,
            failover_grace_period: Duration::from_secs(30),
            auto_failover: true,
            max_replication_lag_ms: 5000,
        }
    }
}

/// Region health status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RegionHealth {
    Healthy,
    Degraded,
    Unreachable,
    Fenced,
}

impl std::fmt::Display for RegionHealth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Healthy => write!(f, "healthy"),
            Self::Degraded => write!(f, "degraded"),
            Self::Unreachable => write!(f, "unreachable"),
            Self::Fenced => write!(f, "fenced"),
        }
    }
}

/// Region role in the replication topology
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RegionRole {
    /// Active region accepting writes
    Active,
    /// Standby region (read replicas only)
    Standby,
    /// Region being promoted to active
    Promoting,
    /// Region being demoted
    Demoting,
}

impl std::fmt::Display for RegionRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Active => write!(f, "active"),
            Self::Standby => write!(f, "standby"),
            Self::Promoting => write!(f, "promoting"),
            Self::Demoting => write!(f, "demoting"),
        }
    }
}

/// Region state for failover management
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegionFailoverState {
    pub region_id: String,
    pub role: RegionRole,
    pub health: RegionHealth,
    pub consecutive_failures: u32,
    pub replication_lag_ms: u64,
    pub last_heartbeat: DateTime<Utc>,
    pub fencing_epoch: u64,
    pub endpoint: String,
}

/// Consumer routing preference
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub enum RoutingPreference {
    /// Always prefer local region
    #[default]
    LocalFirst,
    /// Route to lowest-lag region
    LowestLag,
    /// Round-robin across healthy regions
    RoundRobin,
    /// Explicit region affinity
    RegionAffinity { preferred_region: String },
}

// Using #[derive(Default)] instead (LocalFirst is the desired default via #[default] attribute)

impl std::fmt::Display for RoutingPreference {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LocalFirst => write!(f, "local-first"),
            Self::LowestLag => write!(f, "lowest-lag"),
            Self::RoundRobin => write!(f, "round-robin"),
            Self::RegionAffinity { preferred_region } => {
                write!(f, "affinity({})", preferred_region)
            }
        }
    }
}

/// Failover event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailoverEvent {
    pub id: String,
    pub from_region: String,
    pub to_region: String,
    pub reason: FailoverReason,
    pub initiated_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub success: bool,
    pub fencing_epoch: u64,
}

/// Reason for failover
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FailoverReason {
    RegionUnreachable,
    HighReplicationLag,
    ManualFailover,
    SplitBrainResolution,
}

impl std::fmt::Display for FailoverReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RegionUnreachable => write!(f, "region-unreachable"),
            Self::HighReplicationLag => write!(f, "high-replication-lag"),
            Self::ManualFailover => write!(f, "manual"),
            Self::SplitBrainResolution => write!(f, "split-brain-resolution"),
        }
    }
}

/// Failover orchestrator
pub struct FailoverOrchestrator {
    config: FailoverConfig,
    local_region: String,
    regions: Arc<RwLock<HashMap<String, RegionFailoverState>>>,
    events: Arc<RwLock<Vec<FailoverEvent>>>,
    routing: Arc<RwLock<RoutingPreference>>,
}

impl FailoverOrchestrator {
    pub fn new(local_region: &str, config: FailoverConfig) -> Self {
        Self {
            config,
            local_region: local_region.to_string(),
            regions: Arc::new(RwLock::new(HashMap::new())),
            events: Arc::new(RwLock::new(Vec::new())),
            routing: Arc::new(RwLock::new(RoutingPreference::LocalFirst)),
        }
    }

    /// Register a region
    pub async fn register_region(
        &self,
        region_id: &str,
        endpoint: &str,
        role: RegionRole,
    ) -> Result<()> {
        let state = RegionFailoverState {
            region_id: region_id.to_string(),
            role,
            health: RegionHealth::Healthy,
            consecutive_failures: 0,
            replication_lag_ms: 0,
            last_heartbeat: Utc::now(),
            fencing_epoch: self.config.fencing_epoch,
            endpoint: endpoint.to_string(),
        };

        self.regions
            .write()
            .await
            .insert(region_id.to_string(), state);
        info!("Registered region: {} ({})", region_id, role);
        Ok(())
    }

    /// Record a heartbeat from a region
    pub async fn record_heartbeat(&self, region_id: &str, replication_lag_ms: u64) -> Result<()> {
        let mut regions = self.regions.write().await;
        let region = regions
            .get_mut(region_id)
            .ok_or_else(|| StreamlineError::Internal(format!("Region not found: {}", region_id)))?;

        region.last_heartbeat = Utc::now();
        region.replication_lag_ms = replication_lag_ms;
        region.consecutive_failures = 0;

        if replication_lag_ms > self.config.max_replication_lag_ms {
            region.health = RegionHealth::Degraded;
        } else {
            region.health = RegionHealth::Healthy;
        }

        Ok(())
    }

    /// Record a failure for a region
    pub async fn record_failure(&self, region_id: &str) -> Result<Option<FailoverEvent>> {
        let mut regions = self.regions.write().await;

        // First pass: update failure count and check if failover is needed
        let (should_failover, was_active) = {
            let region = regions.get_mut(region_id).ok_or_else(|| {
                StreamlineError::Internal(format!("Region not found: {}", region_id))
            })?;

            region.consecutive_failures += 1;

            if region.consecutive_failures >= self.config.failure_threshold {
                region.health = RegionHealth::Unreachable;
                let is_active = region.role == RegionRole::Active;
                (true, is_active)
            } else {
                region.health = RegionHealth::Degraded;
                (false, false)
            }
        };

        if !should_failover || !self.config.auto_failover || !was_active {
            return Ok(None);
        }

        // Find best standby to promote (no mutable borrow held)
        let standby = regions
            .values()
            .filter(|r| r.role == RegionRole::Standby && r.health == RegionHealth::Healthy)
            .min_by_key(|r| r.replication_lag_ms)
            .map(|r| r.region_id.clone());

        if let Some(promote_region) = standby {
            let epoch = self.config.fencing_epoch + 1;
            let event = FailoverEvent {
                id: uuid::Uuid::new_v4().to_string(),
                from_region: region_id.to_string(),
                to_region: promote_region.clone(),
                reason: FailoverReason::RegionUnreachable,
                initiated_at: Utc::now(),
                completed_at: None,
                success: false,
                fencing_epoch: epoch,
            };

            // Fence old active
            if let Some(old_active) = regions.get_mut(region_id) {
                old_active.health = RegionHealth::Fenced;
                old_active.role = RegionRole::Demoting;
            }

            // Promote standby
            if let Some(new_active) = regions.get_mut(&promote_region) {
                new_active.role = RegionRole::Active;
                new_active.fencing_epoch = epoch;
            }

            warn!(
                "Failover initiated: {} → {} (epoch {})",
                region_id, promote_region, epoch
            );

            return Ok(Some(event));
        }

        Ok(None)
    }

    /// Detect split-brain scenario
    pub async fn detect_split_brain(&self) -> bool {
        let regions = self.regions.read().await;
        let active_count = regions
            .values()
            .filter(|r| r.role == RegionRole::Active)
            .count();
        active_count > 1
    }

    /// Resolve split-brain by fencing all but the highest-epoch active region
    pub async fn resolve_split_brain(&self) -> Result<Option<FailoverEvent>> {
        let mut regions = self.regions.write().await;
        let active_regions: Vec<_> = regions
            .values()
            .filter(|r| r.role == RegionRole::Active)
            .cloned()
            .collect();

        if active_regions.len() <= 1 {
            return Ok(None);
        }

        // Keep the one with the highest fencing epoch
        let winner = active_regions
            .iter()
            .max_by_key(|r| r.fencing_epoch)
            .ok_or_else(|| StreamlineError::Internal("No active regions found".into()))?
            .region_id
            .clone();

        for region in regions.values_mut() {
            if region.role == RegionRole::Active && region.region_id != winner {
                region.role = RegionRole::Standby;
                region.health = RegionHealth::Fenced;
                warn!("Split-brain: fencing region {}", region.region_id);
            }
        }

        let event = FailoverEvent {
            id: uuid::Uuid::new_v4().to_string(),
            from_region: "multiple".to_string(),
            to_region: winner,
            reason: FailoverReason::SplitBrainResolution,
            initiated_at: Utc::now(),
            completed_at: Some(Utc::now()),
            success: true,
            fencing_epoch: active_regions
                .iter()
                .map(|r| r.fencing_epoch)
                .max()
                .unwrap_or(0),
        };

        Ok(Some(event))
    }

    /// Route a consumer to the best region
    pub async fn route_consumer(&self, consumer_region: &str) -> Result<String> {
        let regions = self.regions.read().await;
        let routing = self.routing.read().await;

        let healthy_regions: Vec<_> = regions
            .values()
            .filter(|r| matches!(r.health, RegionHealth::Healthy | RegionHealth::Degraded))
            .collect();

        if healthy_regions.is_empty() {
            return Err(StreamlineError::Internal("No healthy regions".into()));
        }

        let target = match &*routing {
            RoutingPreference::LocalFirst => healthy_regions
                .iter()
                .find(|r| r.region_id == consumer_region)
                .or_else(|| healthy_regions.first())
                .map(|r| r.endpoint.clone()),
            RoutingPreference::LowestLag => healthy_regions
                .iter()
                .min_by_key(|r| r.replication_lag_ms)
                .map(|r| r.endpoint.clone()),
            RoutingPreference::RoundRobin => healthy_regions.first().map(|r| r.endpoint.clone()),
            RoutingPreference::RegionAffinity { preferred_region } => healthy_regions
                .iter()
                .find(|r| r.region_id == *preferred_region)
                .or_else(|| healthy_regions.first())
                .map(|r| r.endpoint.clone()),
        };

        target.ok_or_else(|| StreamlineError::Internal("No suitable region".into()))
    }

    /// Set routing preference
    pub async fn set_routing(&self, preference: RoutingPreference) {
        *self.routing.write().await = preference;
    }

    /// Get all region states
    pub async fn region_states(&self) -> Vec<RegionFailoverState> {
        self.regions.read().await.values().cloned().collect()
    }

    /// Get failover event history
    pub async fn failover_history(&self) -> Vec<FailoverEvent> {
        self.events.read().await.clone()
    }

    /// Get the local region ID
    pub fn local_region(&self) -> &str {
        &self.local_region
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_register_and_list_regions() {
        let orch = FailoverOrchestrator::new("us-east-1", FailoverConfig::default());
        orch.register_region(
            "us-east-1",
            "east.streamline.cloud:9092",
            RegionRole::Active,
        )
        .await
        .unwrap();
        orch.register_region(
            "us-west-2",
            "west.streamline.cloud:9092",
            RegionRole::Standby,
        )
        .await
        .unwrap();

        let regions = orch.region_states().await;
        assert_eq!(regions.len(), 2);
    }

    #[tokio::test]
    async fn test_heartbeat_updates_health() {
        let orch = FailoverOrchestrator::new("us-east-1", FailoverConfig::default());
        orch.register_region("us-east-1", "east:9092", RegionRole::Active)
            .await
            .unwrap();

        orch.record_heartbeat("us-east-1", 100).await.unwrap();
        let regions = orch.region_states().await;
        assert_eq!(regions[0].health, RegionHealth::Healthy);

        orch.record_heartbeat("us-east-1", 10000).await.unwrap();
        let regions = orch.region_states().await;
        assert_eq!(regions[0].health, RegionHealth::Degraded);
    }

    #[tokio::test]
    async fn test_failover_on_failures() {
        let config = FailoverConfig {
            failure_threshold: 2,
            auto_failover: true,
            ..Default::default()
        };
        let orch = FailoverOrchestrator::new("us-east-1", config);

        orch.register_region("us-east-1", "east:9092", RegionRole::Active)
            .await
            .unwrap();
        orch.register_region("us-west-2", "west:9092", RegionRole::Standby)
            .await
            .unwrap();

        // First failure - no failover yet
        let event = orch.record_failure("us-east-1").await.unwrap();
        assert!(event.is_none());

        // Second failure - triggers failover
        let event = orch.record_failure("us-east-1").await.unwrap();
        assert!(event.is_some());

        let regions = orch.region_states().await;
        let west = regions.iter().find(|r| r.region_id == "us-west-2").unwrap();
        assert_eq!(west.role, RegionRole::Active);
    }

    #[tokio::test]
    async fn test_split_brain_detection() {
        let orch = FailoverOrchestrator::new("us-east-1", FailoverConfig::default());
        orch.register_region("us-east-1", "east:9092", RegionRole::Active)
            .await
            .unwrap();
        orch.register_region("us-west-2", "west:9092", RegionRole::Active)
            .await
            .unwrap();

        assert!(orch.detect_split_brain().await);

        let event = orch.resolve_split_brain().await.unwrap();
        assert!(event.is_some());
        assert!(!orch.detect_split_brain().await);
    }

    #[tokio::test]
    async fn test_consumer_routing_local_first() {
        let orch = FailoverOrchestrator::new("us-east-1", FailoverConfig::default());
        orch.register_region("us-east-1", "east:9092", RegionRole::Active)
            .await
            .unwrap();
        orch.register_region("us-west-2", "west:9092", RegionRole::Standby)
            .await
            .unwrap();

        let endpoint = orch.route_consumer("us-east-1").await.unwrap();
        assert_eq!(endpoint, "east:9092");
    }

    #[tokio::test]
    async fn test_consumer_routing_lowest_lag() {
        let orch = FailoverOrchestrator::new("us-east-1", FailoverConfig::default());
        orch.register_region("us-east-1", "east:9092", RegionRole::Active)
            .await
            .unwrap();
        orch.register_region("us-west-2", "west:9092", RegionRole::Standby)
            .await
            .unwrap();

        orch.record_heartbeat("us-east-1", 500).await.unwrap();
        orch.record_heartbeat("us-west-2", 100).await.unwrap();

        orch.set_routing(RoutingPreference::LowestLag).await;
        let endpoint = orch.route_consumer("us-east-1").await.unwrap();
        assert_eq!(endpoint, "west:9092");
    }

    #[test]
    fn test_region_health_display() {
        assert_eq!(RegionHealth::Healthy.to_string(), "healthy");
        assert_eq!(RegionHealth::Fenced.to_string(), "fenced");
    }

    #[test]
    fn test_region_role_display() {
        assert_eq!(RegionRole::Active.to_string(), "active");
        assert_eq!(RegionRole::Standby.to_string(), "standby");
    }

    #[test]
    fn test_routing_preference_display() {
        assert_eq!(RoutingPreference::LocalFirst.to_string(), "local-first");
        assert_eq!(RoutingPreference::LowestLag.to_string(), "lowest-lag");
    }

    #[test]
    fn test_failover_reason_display() {
        assert_eq!(FailoverReason::ManualFailover.to_string(), "manual");
        assert_eq!(
            FailoverReason::RegionUnreachable.to_string(),
            "region-unreachable"
        );
    }
}
