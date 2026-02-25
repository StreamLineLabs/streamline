//! FaaS Auto-Scaler, Cold-Start Pool & Invocation Metering
//!
//! Provides production-grade function lifecycle management:
//!
//! - **Auto-Scaling**: Adjusts function instance count based on queue depth,
//!   invocation rate, and latency targets.
//! - **Cold-Start Pool**: Pre-warms WASM instances to eliminate cold-start
//!   latency for latency-sensitive functions.
//! - **Invocation Metering**: Per-function cost tracking for usage-based billing
//!   in `streamline-cloud`.
//! - **Function Versioning**: Deploy multiple versions with traffic splitting.

use crate::error::{Result, StreamlineError};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

// ---------------------------------------------------------------------------
// Auto-Scaler
// ---------------------------------------------------------------------------

/// Auto-scaling policy for a function.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScalingPolicy {
    /// Minimum instances (0 = scale-to-zero allowed).
    pub min_instances: u32,
    /// Maximum instances.
    pub max_instances: u32,
    /// Target invocations per second per instance.
    pub target_rps_per_instance: f64,
    /// Target p99 latency in milliseconds.
    pub target_p99_latency_ms: u64,
    /// Scale-up cooldown in seconds.
    pub scale_up_cooldown_secs: u64,
    /// Scale-down cooldown in seconds.
    pub scale_down_cooldown_secs: u64,
    /// Queue depth threshold that triggers immediate scale-up.
    pub queue_depth_threshold: u64,
}

impl Default for ScalingPolicy {
    fn default() -> Self {
        Self {
            min_instances: 0,
            max_instances: 100,
            target_rps_per_instance: 50.0,
            target_p99_latency_ms: 100,
            scale_up_cooldown_secs: 30,
            scale_down_cooldown_secs: 120,
            queue_depth_threshold: 1000,
        }
    }
}

/// Scaling decision.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScalingDecision {
    /// Function name.
    pub function_name: String,
    /// Current instance count.
    pub current_instances: u32,
    /// Desired instance count.
    pub desired_instances: u32,
    /// Reason for the decision.
    pub reason: ScalingReason,
    /// Timestamp.
    pub timestamp: DateTime<Utc>,
}

/// Reason for scaling.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScalingReason {
    /// RPS exceeds target.
    HighLoad { current_rps: f64, target_rps: f64 },
    /// Queue depth exceeds threshold.
    QueuePressure { depth: u64, threshold: u64 },
    /// Latency exceeds target.
    HighLatency { p99_ms: u64, target_ms: u64 },
    /// Low utilization — scale down.
    LowUtilization { utilization_pct: f64 },
    /// Scale to zero (idle).
    IdleScaleDown,
    /// Minimum instances floor.
    MinimumFloor,
}

/// Metrics snapshot for scaling decisions.
#[derive(Debug, Clone, Default)]
pub struct FunctionMetricsSnapshot {
    pub current_rps: f64,
    pub p99_latency_ms: u64,
    pub queue_depth: u64,
    pub active_instances: u32,
    pub error_rate: f64,
}

/// The auto-scaler evaluates function metrics and produces scaling decisions.
pub struct AutoScaler {
    policies: RwLock<HashMap<String, ScalingPolicy>>,
    last_scale_up: RwLock<HashMap<String, DateTime<Utc>>>,
    last_scale_down: RwLock<HashMap<String, DateTime<Utc>>>,
    decisions: RwLock<Vec<ScalingDecision>>,
}

impl AutoScaler {
    pub fn new() -> Self {
        Self {
            policies: RwLock::new(HashMap::new()),
            last_scale_up: RwLock::new(HashMap::new()),
            last_scale_down: RwLock::new(HashMap::new()),
            decisions: RwLock::new(Vec::new()),
        }
    }

    /// Register a scaling policy for a function.
    pub async fn set_policy(&self, function_name: &str, policy: ScalingPolicy) {
        self.policies
            .write()
            .await
            .insert(function_name.to_string(), policy);
    }

    /// Remove a scaling policy.
    pub async fn remove_policy(&self, function_name: &str) {
        self.policies.write().await.remove(function_name);
    }

    /// Evaluate metrics and produce a scaling decision.
    pub async fn evaluate(
        &self,
        function_name: &str,
        metrics: &FunctionMetricsSnapshot,
    ) -> Option<ScalingDecision> {
        let policies = self.policies.read().await;
        let policy = policies.get(function_name)?;

        let current = metrics.active_instances;
        let desired = self.compute_desired(policy, metrics);

        if desired == current {
            return None;
        }

        // Check cooldowns
        let now = Utc::now();
        if desired > current {
            if let Some(last) = self.last_scale_up.read().await.get(function_name) {
                let elapsed = now.signed_duration_since(*last).num_seconds();
                if elapsed < policy.scale_up_cooldown_secs as i64 {
                    return None;
                }
            }
        } else {
            if let Some(last) = self.last_scale_down.read().await.get(function_name) {
                let elapsed = now.signed_duration_since(*last).num_seconds();
                if elapsed < policy.scale_down_cooldown_secs as i64 {
                    return None;
                }
            }
        }

        let reason = self.determine_reason(policy, metrics, current, desired);

        let decision = ScalingDecision {
            function_name: function_name.to_string(),
            current_instances: current,
            desired_instances: desired,
            reason,
            timestamp: now,
        };

        // Record cooldown
        if desired > current {
            self.last_scale_up
                .write()
                .await
                .insert(function_name.to_string(), now);
        } else {
            self.last_scale_down
                .write()
                .await
                .insert(function_name.to_string(), now);
        }

        // Store decision history
        let mut decisions = self.decisions.write().await;
        decisions.push(decision.clone());
        if decisions.len() > 500 {
            decisions.drain(0..100);
        }

        Some(decision)
    }

    /// Get recent scaling decisions.
    pub async fn recent_decisions(&self, limit: usize) -> Vec<ScalingDecision> {
        self.decisions
            .read()
            .await
            .iter()
            .rev()
            .take(limit)
            .cloned()
            .collect()
    }

    fn compute_desired(&self, policy: &ScalingPolicy, metrics: &FunctionMetricsSnapshot) -> u32 {
        let mut desired = metrics.active_instances;

        // Queue pressure — immediate scale-up
        if metrics.queue_depth > policy.queue_depth_threshold {
            let ratio = metrics.queue_depth as f64 / policy.queue_depth_threshold as f64;
            desired = (desired as f64 * ratio).ceil() as u32;
        }

        // RPS-based scaling
        if policy.target_rps_per_instance > 0.0 {
            let needed =
                (metrics.current_rps / policy.target_rps_per_instance).ceil() as u32;
            desired = desired.max(needed);
        }

        // Latency-based scaling
        if metrics.p99_latency_ms > policy.target_p99_latency_ms && metrics.active_instances > 0 {
            let ratio = metrics.p99_latency_ms as f64 / policy.target_p99_latency_ms as f64;
            desired = (metrics.active_instances as f64 * ratio).ceil() as u32;
        }

        // Scale-down: low utilization
        if metrics.current_rps < policy.target_rps_per_instance * 0.3
            && metrics.active_instances > policy.min_instances
        {
            desired = desired.min(
                (metrics.current_rps / policy.target_rps_per_instance).ceil().max(1.0) as u32,
            );
        }

        // Scale-to-zero
        if metrics.current_rps == 0.0
            && metrics.queue_depth == 0
            && policy.min_instances == 0
        {
            desired = 0;
        }

        // Clamp to policy bounds
        desired.clamp(policy.min_instances, policy.max_instances)
    }

    fn determine_reason(
        &self,
        policy: &ScalingPolicy,
        metrics: &FunctionMetricsSnapshot,
        current: u32,
        desired: u32,
    ) -> ScalingReason {
        if desired == 0 {
            return ScalingReason::IdleScaleDown;
        }
        if desired == policy.min_instances && desired > current {
            return ScalingReason::MinimumFloor;
        }
        if metrics.queue_depth > policy.queue_depth_threshold {
            return ScalingReason::QueuePressure {
                depth: metrics.queue_depth,
                threshold: policy.queue_depth_threshold,
            };
        }
        if metrics.p99_latency_ms > policy.target_p99_latency_ms {
            return ScalingReason::HighLatency {
                p99_ms: metrics.p99_latency_ms,
                target_ms: policy.target_p99_latency_ms,
            };
        }
        if desired > current {
            ScalingReason::HighLoad {
                current_rps: metrics.current_rps,
                target_rps: policy.target_rps_per_instance * current as f64,
            }
        } else {
            ScalingReason::LowUtilization {
                utilization_pct: if current > 0 {
                    metrics.current_rps / (policy.target_rps_per_instance * current as f64) * 100.0
                } else {
                    0.0
                },
            }
        }
    }
}

impl Default for AutoScaler {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Cold-Start Pool
// ---------------------------------------------------------------------------

/// Pre-warmed instance slot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WarmInstance {
    /// Function name this instance is warmed for.
    pub function_name: String,
    /// Instance ID.
    pub instance_id: String,
    /// When the instance was warmed.
    pub warmed_at: DateTime<Utc>,
    /// Whether currently assigned to a live invocation.
    pub assigned: bool,
}

/// Configuration for the cold-start pool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColdStartPoolConfig {
    /// Minimum pre-warmed instances per function.
    pub min_warm_per_function: u32,
    /// Maximum total warm instances across all functions.
    pub max_total_warm: u32,
    /// Warm instance TTL before recycling (seconds).
    pub warm_ttl_secs: u64,
}

impl Default for ColdStartPoolConfig {
    fn default() -> Self {
        Self {
            min_warm_per_function: 1,
            max_total_warm: 50,
            warm_ttl_secs: 300,
        }
    }
}

/// Cold-start pool for pre-warming WASM instances.
pub struct ColdStartPool {
    config: ColdStartPoolConfig,
    pool: RwLock<Vec<WarmInstance>>,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl ColdStartPool {
    pub fn new(config: ColdStartPoolConfig) -> Self {
        Self {
            config,
            pool: RwLock::new(Vec::new()),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Acquire a pre-warmed instance for a function. Returns None on miss.
    pub async fn acquire(&self, function_name: &str) -> Option<WarmInstance> {
        let mut pool = self.pool.write().await;
        let idx = pool.iter().position(|i| {
            i.function_name == function_name && !i.assigned
        })?;

        let mut instance = pool[idx].clone();
        instance.assigned = true;
        pool[idx] = instance.clone();
        self.hits.fetch_add(1, Ordering::Relaxed);
        Some(instance)
    }

    /// Return an instance to the pool after invocation completes.
    pub async fn release(&self, instance_id: &str) {
        let mut pool = self.pool.write().await;
        if let Some(instance) = pool.iter_mut().find(|i| i.instance_id == instance_id) {
            instance.assigned = false;
            instance.warmed_at = Utc::now(); // Refresh TTL
        }
    }

    /// Pre-warm instances for a function.
    pub async fn warm(&self, function_name: &str, count: u32) {
        let mut pool = self.pool.write().await;

        let current = pool
            .iter()
            .filter(|i| i.function_name == function_name && !i.assigned)
            .count() as u32;

        let to_add = count.saturating_sub(current);
        let total = pool.len() as u32;

        for i in 0..to_add {
            if total + i >= self.config.max_total_warm {
                break;
            }
            pool.push(WarmInstance {
                function_name: function_name.to_string(),
                instance_id: format!("{}-warm-{}", function_name, uuid_v4()),
                warmed_at: Utc::now(),
                assigned: false,
            });
        }
    }

    /// Evict expired warm instances.
    pub async fn evict_expired(&self) -> usize {
        let mut pool = self.pool.write().await;
        let now = Utc::now();
        let ttl = chrono::Duration::seconds(self.config.warm_ttl_secs as i64);
        let before = pool.len();
        pool.retain(|i| {
            i.assigned || now.signed_duration_since(i.warmed_at) < ttl
        });
        before - pool.len()
    }

    /// Get pool statistics.
    pub async fn stats(&self) -> ColdStartPoolStats {
        let pool = self.pool.read().await;
        ColdStartPoolStats {
            total_warm: pool.len(),
            assigned: pool.iter().filter(|i| i.assigned).count(),
            available: pool.iter().filter(|i| !i.assigned).count(),
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
        }
    }
}

/// Cold-start pool statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColdStartPoolStats {
    pub total_warm: usize,
    pub assigned: usize,
    pub available: usize,
    pub hits: u64,
    pub misses: u64,
}

// ---------------------------------------------------------------------------
// Invocation Metering
// ---------------------------------------------------------------------------

/// Per-invocation metering record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvocationRecord {
    /// Function name.
    pub function_name: String,
    /// Function version.
    pub version: String,
    /// Invocation ID.
    pub invocation_id: String,
    /// Duration in milliseconds.
    pub duration_ms: u64,
    /// Memory used in bytes.
    pub memory_bytes: u64,
    /// CPU time in milliseconds.
    pub cpu_time_ms: u64,
    /// Input size in bytes.
    pub input_bytes: u64,
    /// Output size in bytes.
    pub output_bytes: u64,
    /// Whether the invocation succeeded.
    pub success: bool,
    /// Timestamp.
    pub timestamp: DateTime<Utc>,
    /// Computed cost (usage units).
    pub cost_units: f64,
}

/// Metering engine for tracking function invocation costs.
pub struct InvocationMeter {
    /// Cost per GB-second of execution.
    pub cost_per_gb_sec: f64,
    /// Cost per invocation.
    pub cost_per_invocation: f64,
    /// Records (bounded).
    records: RwLock<Vec<InvocationRecord>>,
    /// Per-function aggregates.
    aggregates: RwLock<HashMap<String, FunctionUsageAggregate>>,
}

/// Aggregate usage for one function.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FunctionUsageAggregate {
    pub total_invocations: u64,
    pub total_duration_ms: u64,
    pub total_memory_gb_sec: f64,
    pub total_cost_units: f64,
    pub error_count: u64,
}

impl InvocationMeter {
    pub fn new(cost_per_gb_sec: f64, cost_per_invocation: f64) -> Self {
        Self {
            cost_per_gb_sec,
            cost_per_invocation,
            records: RwLock::new(Vec::new()),
            aggregates: RwLock::new(HashMap::new()),
        }
    }

    /// Record a completed invocation.
    pub async fn record(&self, mut record: InvocationRecord) {
        // Compute cost
        let gb_sec =
            (record.memory_bytes as f64 / (1024.0 * 1024.0 * 1024.0))
                * (record.duration_ms as f64 / 1000.0);
        record.cost_units = gb_sec * self.cost_per_gb_sec + self.cost_per_invocation;

        // Update aggregates
        let mut agg = self.aggregates.write().await;
        let entry = agg.entry(record.function_name.clone()).or_default();
        entry.total_invocations += 1;
        entry.total_duration_ms += record.duration_ms;
        entry.total_memory_gb_sec += gb_sec;
        entry.total_cost_units += record.cost_units;
        if !record.success {
            entry.error_count += 1;
        }

        // Store record (bounded)
        let mut records = self.records.write().await;
        records.push(record);
        if records.len() > 10_000 {
            records.drain(0..5_000);
        }
    }

    /// Get usage for a function.
    pub async fn usage(&self, function_name: &str) -> Option<FunctionUsageAggregate> {
        self.aggregates.read().await.get(function_name).cloned()
    }

    /// Get all function usage aggregates.
    pub async fn all_usage(&self) -> HashMap<String, FunctionUsageAggregate> {
        self.aggregates.read().await.clone()
    }
}

// ---------------------------------------------------------------------------
// Function Versioning
// ---------------------------------------------------------------------------

/// A versioned function deployment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionVersion {
    /// Version identifier (semver or sequential).
    pub version: String,
    /// Traffic weight (0-100). All versions must sum to 100.
    pub traffic_weight: u32,
    /// When this version was deployed.
    pub deployed_at: DateTime<Utc>,
    /// Whether this is the "stable" version.
    pub stable: bool,
    /// Canary health: error rate for this version.
    pub error_rate: f64,
}

/// Manages multiple versions of a function with traffic splitting.
pub struct VersionRouter {
    versions: RwLock<HashMap<String, Vec<FunctionVersion>>>,
}

impl VersionRouter {
    pub fn new() -> Self {
        Self {
            versions: RwLock::new(HashMap::new()),
        }
    }

    /// Register a new version for a function.
    pub async fn add_version(
        &self,
        function_name: &str,
        version: FunctionVersion,
    ) -> Result<()> {
        let mut map = self.versions.write().await;
        let versions = map.entry(function_name.to_string()).or_default();

        if versions.iter().any(|v| v.version == version.version) {
            return Err(StreamlineError::Config(format!(
                "Version {} already exists for function {}",
                version.version, function_name
            )));
        }

        versions.push(version);
        Ok(())
    }

    /// Route a request to a version based on traffic weights.
    pub async fn route(&self, function_name: &str) -> Option<String> {
        let map = self.versions.read().await;
        let versions = map.get(function_name)?;

        if versions.is_empty() {
            return None;
        }
        if versions.len() == 1 {
            return Some(versions[0].version.clone());
        }

        // Weighted random selection
        let total_weight: u32 = versions.iter().map(|v| v.traffic_weight).sum();
        if total_weight == 0 {
            return Some(versions[0].version.clone());
        }

        let mut rng_val = (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos()
            % total_weight) as u32;

        for v in versions {
            if rng_val < v.traffic_weight {
                return Some(v.version.clone());
            }
            rng_val -= v.traffic_weight;
        }

        Some(versions.last()?.version.clone())
    }

    /// List versions for a function.
    pub async fn list_versions(&self, function_name: &str) -> Vec<FunctionVersion> {
        self.versions
            .read()
            .await
            .get(function_name)
            .cloned()
            .unwrap_or_default()
    }

    /// Promote a version to 100% traffic (rollback all others to 0).
    pub async fn promote(&self, function_name: &str, version: &str) -> Result<()> {
        let mut map = self.versions.write().await;
        let versions = map.get_mut(function_name).ok_or_else(|| {
            StreamlineError::Config(format!("Function {} not found", function_name))
        })?;

        let found = versions.iter().any(|v| v.version == version);
        if !found {
            return Err(StreamlineError::Config(format!(
                "Version {} not found",
                version
            )));
        }

        for v in versions.iter_mut() {
            if v.version == version {
                v.traffic_weight = 100;
                v.stable = true;
            } else {
                v.traffic_weight = 0;
                v.stable = false;
            }
        }

        Ok(())
    }
}

impl Default for VersionRouter {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn uuid_v4() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{:032x}", t)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_autoscaler_high_load() {
        let scaler = AutoScaler::new();
        scaler
            .set_policy("fn1", ScalingPolicy {
                target_rps_per_instance: 50.0,
                ..Default::default()
            })
            .await;

        let metrics = FunctionMetricsSnapshot {
            current_rps: 200.0,
            active_instances: 2,
            ..Default::default()
        };

        let decision = scaler.evaluate("fn1", &metrics).await;
        assert!(decision.is_some());
        let d = decision.unwrap();
        assert!(d.desired_instances > 2);
    }

    #[tokio::test]
    async fn test_autoscaler_scale_to_zero() {
        let scaler = AutoScaler::new();
        scaler
            .set_policy("fn2", ScalingPolicy {
                min_instances: 0,
                ..Default::default()
            })
            .await;

        let metrics = FunctionMetricsSnapshot {
            current_rps: 0.0,
            queue_depth: 0,
            active_instances: 1,
            ..Default::default()
        };

        let decision = scaler.evaluate("fn2", &metrics).await;
        assert!(decision.is_some());
        assert_eq!(decision.unwrap().desired_instances, 0);
    }

    #[tokio::test]
    async fn test_cold_start_pool() {
        let pool = ColdStartPool::new(ColdStartPoolConfig::default());

        pool.warm("fn1", 3).await;
        let stats = pool.stats().await;
        assert_eq!(stats.available, 3);

        let instance = pool.acquire("fn1").await;
        assert!(instance.is_some());
        let stats = pool.stats().await;
        assert_eq!(stats.available, 2);
        assert_eq!(stats.assigned, 1);
    }

    #[tokio::test]
    async fn test_invocation_meter() {
        let meter = InvocationMeter::new(0.0000166667, 0.0000002);

        let record = InvocationRecord {
            function_name: "fn1".to_string(),
            version: "v1".to_string(),
            invocation_id: "inv-1".to_string(),
            duration_ms: 100,
            memory_bytes: 128 * 1024 * 1024,
            cpu_time_ms: 50,
            input_bytes: 1024,
            output_bytes: 512,
            success: true,
            timestamp: Utc::now(),
            cost_units: 0.0,
        };

        meter.record(record).await;
        let usage = meter.usage("fn1").await.unwrap();
        assert_eq!(usage.total_invocations, 1);
        assert!(usage.total_cost_units > 0.0);
    }

    #[tokio::test]
    async fn test_version_router() {
        let router = VersionRouter::new();

        router
            .add_version("fn1", FunctionVersion {
                version: "v1".to_string(),
                traffic_weight: 90,
                deployed_at: Utc::now(),
                stable: true,
                error_rate: 0.0,
            })
            .await
            .unwrap();

        router
            .add_version("fn1", FunctionVersion {
                version: "v2".to_string(),
                traffic_weight: 10,
                deployed_at: Utc::now(),
                stable: false,
                error_rate: 0.0,
            })
            .await
            .unwrap();

        let versions = router.list_versions("fn1").await;
        assert_eq!(versions.len(), 2);

        // Route should return a version
        let v = router.route("fn1").await;
        assert!(v.is_some());

        // Promote v2
        router.promote("fn1", "v2").await.unwrap();
        let versions = router.list_versions("fn1").await;
        let v2 = versions.iter().find(|v| v.version == "v2").unwrap();
        assert_eq!(v2.traffic_weight, 100);
        assert!(v2.stable);
    }

    #[tokio::test]
    async fn test_duplicate_version() {
        let router = VersionRouter::new();
        router
            .add_version("fn1", FunctionVersion {
                version: "v1".to_string(),
                traffic_weight: 100,
                deployed_at: Utc::now(),
                stable: true,
                error_rate: 0.0,
            })
            .await
            .unwrap();

        let result = router
            .add_version("fn1", FunctionVersion {
                version: "v1".to_string(),
                traffic_weight: 0,
                deployed_at: Utc::now(),
                stable: false,
                error_rate: 0.0,
            })
            .await;
        assert!(result.is_err());
    }
}
