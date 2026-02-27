//! Built-in chaos testing / fault injection framework for resilience testing.
//!
//! Provides a [`ChaosEngine`] that manages [`ChaosExperiment`]s, each injecting
//! a specific [`FaultType`] against a [`ChaosTarget`]. The engine enforces
//! safety controls: it refuses to run when disabled, and in safe mode only
//! low/medium intensity faults are permitted.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};
use tracing::{info, warn};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Errors returned by [`ChaosEngine`] operations.
#[derive(Debug, thiserror::Error)]
pub enum ChaosError {
    #[error("chaos engine is disabled")]
    Disabled,
    #[error("experiment not found: {0}")]
    NotFound(String),
    #[error("experiment already exists: {0}")]
    AlreadyExists(String),
    #[error("fault type not allowed: {0}")]
    FaultNotAllowed(String),
    #[error("intensity {0:?} is not allowed in safe mode")]
    IntensityNotAllowed(FaultIntensity),
    #[error("max concurrent experiments reached ({0})")]
    MaxConcurrentReached(usize),
    #[error("invalid state transition from {from:?} to {to:?}")]
    InvalidTransition {
        from: ExperimentStatus,
        to: ExperimentStatus,
    },
}

pub type Result<T> = std::result::Result<T, ChaosError>;

// ---------------------------------------------------------------------------
// Core types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum FaultType {
    LatencyInjection { min_ms: u64, max_ms: u64 },
    PacketLoss { percentage: f64 },
    ConnectionDrop { percentage: f64 },
    DiskSlowdown { factor: f64 },
    CpuPressure { percentage: f64 },
    MemoryPressure { target_mb: u64 },
    PartitionUnavailable,
    LeaderElection,
    ClockSkew { skew_ms: i64 },
    NetworkPartition { isolated_nodes: Vec<String> },
}

impl std::fmt::Display for FaultType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LatencyInjection { .. } => write!(f, "LatencyInjection"),
            Self::PacketLoss { .. } => write!(f, "PacketLoss"),
            Self::ConnectionDrop { .. } => write!(f, "ConnectionDrop"),
            Self::DiskSlowdown { .. } => write!(f, "DiskSlowdown"),
            Self::CpuPressure { .. } => write!(f, "CpuPressure"),
            Self::MemoryPressure { .. } => write!(f, "MemoryPressure"),
            Self::PartitionUnavailable => write!(f, "PartitionUnavailable"),
            Self::LeaderElection => write!(f, "LeaderElection"),
            Self::ClockSkew { .. } => write!(f, "ClockSkew"),
            Self::NetworkPartition { .. } => write!(f, "NetworkPartition"),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum FaultIntensity {
    Low,
    Medium,
    High,
    Extreme,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ChaosTarget {
    AllBrokers,
    Broker(String),
    Topic(String),
    Partition { topic: String, partition: i32 },
    ConsumerGroup(String),
    Network,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ChaosSchedule {
    Immediate,
    Delayed { delay_secs: u64 },
    Recurring { interval_secs: u64, count: Option<u32> },
    RandomWindow { window_secs: u64 },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ExperimentStatus {
    Pending,
    Running,
    Completed,
    Failed(String),
    Cancelled,
}

// ---------------------------------------------------------------------------
// Specs & results
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FaultSpec {
    pub fault_type: FaultType,
    pub intensity: FaultIntensity,
    pub duration_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChaosMetricSnapshot {
    pub throughput_msg_sec: f64,
    pub latency_p99_ms: f64,
    pub error_rate: f64,
    pub active_connections: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExperimentResult {
    pub phase: String,
    pub success: bool,
    pub message: String,
    pub metrics_before: ChaosMetricSnapshot,
    pub metrics_after: ChaosMetricSnapshot,
    pub timestamp: String,
}

// ---------------------------------------------------------------------------
// Experiment
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChaosExperiment {
    pub id: String,
    pub name: String,
    pub description: String,
    pub fault: FaultSpec,
    pub target: ChaosTarget,
    pub schedule: ChaosSchedule,
    pub status: ExperimentStatus,
    pub results: Vec<ExperimentResult>,
    pub created_at: String,
    pub started_at: Option<String>,
    pub ended_at: Option<String>,
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct ChaosStats {
    pub experiments_run: AtomicU64,
    pub experiments_succeeded: AtomicU64,
    pub experiments_failed: AtomicU64,
    pub faults_injected: AtomicU64,
    pub total_fault_duration_secs: AtomicU64,
}

/// Immutable snapshot of [`ChaosStats`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChaosStatsSnapshot {
    pub experiments_run: u64,
    pub experiments_succeeded: u64,
    pub experiments_failed: u64,
    pub faults_injected: u64,
    pub total_fault_duration_secs: u64,
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChaosConfig {
    pub enabled: bool,
    pub max_concurrent_experiments: usize,
    pub require_confirmation: bool,
    pub safe_mode: bool,
    pub allowed_fault_types: Vec<FaultType>,
}

impl Default for ChaosConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_concurrent_experiments: 3,
            require_confirmation: true,
            safe_mode: true,
            allowed_fault_types: Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------

pub struct ChaosEngine {
    experiments: Arc<RwLock<HashMap<String, ChaosExperiment>>>,
    config: ChaosConfig,
    stats: Arc<ChaosStats>,
}

impl ChaosEngine {
    pub fn new(config: ChaosConfig) -> Self {
        info!(enabled = config.enabled, safe_mode = config.safe_mode, "chaos engine initialised");
        Self {
            experiments: Arc::new(RwLock::new(HashMap::new())),
            config,
            stats: Arc::new(ChaosStats::default()),
        }
    }

    /// Create a new experiment and return its id.
    pub fn create_experiment(
        &self,
        name: &str,
        desc: &str,
        fault: FaultSpec,
        target: ChaosTarget,
        schedule: ChaosSchedule,
    ) -> Result<String> {
        if !self.config.enabled {
            return Err(ChaosError::Disabled);
        }

        if !self.is_fault_allowed(&fault.fault_type) {
            return Err(ChaosError::FaultNotAllowed(fault.fault_type.to_string()));
        }

        if self.config.safe_mode
            && matches!(fault.intensity, FaultIntensity::High | FaultIntensity::Extreme)
        {
            return Err(ChaosError::IntensityNotAllowed(fault.intensity));
        }

        let id = Uuid::new_v4().to_string();
        let now = chrono::Utc::now().to_rfc3339();

        let experiment = ChaosExperiment {
            id: id.clone(),
            name: name.to_string(),
            description: desc.to_string(),
            fault,
            target,
            schedule,
            status: ExperimentStatus::Pending,
            results: Vec::new(),
            created_at: now,
            started_at: None,
            ended_at: None,
        };

        let mut experiments = self.experiments.write().unwrap();
        experiments.insert(id.clone(), experiment);
        info!(id = %id, name, "chaos experiment created");
        Ok(id)
    }

    /// Transition an experiment to [`ExperimentStatus::Running`].
    pub fn start_experiment(&self, id: &str) -> Result<()> {
        if !self.config.enabled {
            return Err(ChaosError::Disabled);
        }

        let mut experiments = self.experiments.write().unwrap();

        // Check concurrent limit (count currently running experiments).
        let running = experiments
            .values()
            .filter(|e| e.status == ExperimentStatus::Running)
            .count();
        if running >= self.config.max_concurrent_experiments {
            return Err(ChaosError::MaxConcurrentReached(
                self.config.max_concurrent_experiments,
            ));
        }

        let exp = experiments
            .get_mut(id)
            .ok_or_else(|| ChaosError::NotFound(id.to_string()))?;

        if exp.status != ExperimentStatus::Pending {
            return Err(ChaosError::InvalidTransition {
                from: exp.status.clone(),
                to: ExperimentStatus::Running,
            });
        }

        exp.status = ExperimentStatus::Running;
        exp.started_at = Some(chrono::Utc::now().to_rfc3339());

        self.stats.experiments_run.fetch_add(1, Ordering::Relaxed);
        self.stats.faults_injected.fetch_add(1, Ordering::Relaxed);
        self.stats
            .total_fault_duration_secs
            .fetch_add(exp.fault.duration_secs, Ordering::Relaxed);

        info!(id, "chaos experiment started");
        Ok(())
    }

    /// Stop a running experiment (marks it as [`ExperimentStatus::Completed`]).
    pub fn stop_experiment(&self, id: &str) -> Result<()> {
        let mut experiments = self.experiments.write().unwrap();
        let exp = experiments
            .get_mut(id)
            .ok_or_else(|| ChaosError::NotFound(id.to_string()))?;

        if exp.status != ExperimentStatus::Running {
            return Err(ChaosError::InvalidTransition {
                from: exp.status.clone(),
                to: ExperimentStatus::Completed,
            });
        }

        exp.status = ExperimentStatus::Completed;
        exp.ended_at = Some(chrono::Utc::now().to_rfc3339());
        self.stats
            .experiments_succeeded
            .fetch_add(1, Ordering::Relaxed);

        info!(id, "chaos experiment stopped");
        Ok(())
    }

    pub fn get_experiment(&self, id: &str) -> Option<ChaosExperiment> {
        self.experiments.read().unwrap().get(id).cloned()
    }

    pub fn list_experiments(&self) -> Vec<ChaosExperiment> {
        self.experiments.read().unwrap().values().cloned().collect()
    }

    pub fn delete_experiment(&self, id: &str) -> Result<()> {
        let mut experiments = self.experiments.write().unwrap();
        let exp = experiments
            .get(id)
            .ok_or_else(|| ChaosError::NotFound(id.to_string()))?;

        if exp.status == ExperimentStatus::Running {
            warn!(id, "refusing to delete a running experiment");
            return Err(ChaosError::InvalidTransition {
                from: ExperimentStatus::Running,
                to: ExperimentStatus::Cancelled,
            });
        }

        experiments.remove(id);
        info!(id, "chaos experiment deleted");
        Ok(())
    }

    pub fn record_result(&self, id: &str, result: ExperimentResult) -> Result<()> {
        let mut experiments = self.experiments.write().unwrap();
        let exp = experiments
            .get_mut(id)
            .ok_or_else(|| ChaosError::NotFound(id.to_string()))?;

        if !result.success {
            self.stats
                .experiments_failed
                .fetch_add(1, Ordering::Relaxed);
        }

        exp.results.push(result);
        Ok(())
    }

    /// Returns `true` when the given fault type is explicitly listed in
    /// [`ChaosConfig::allowed_fault_types`], or when the allow-list is empty
    /// (meaning all types are permitted).
    pub fn is_fault_allowed(&self, fault_type: &FaultType) -> bool {
        if self.config.allowed_fault_types.is_empty() {
            return true;
        }
        self.config
            .allowed_fault_types
            .iter()
            .any(|allowed| std::mem::discriminant(allowed) == std::mem::discriminant(fault_type))
    }

    pub fn stats(&self) -> ChaosStatsSnapshot {
        ChaosStatsSnapshot {
            experiments_run: self.stats.experiments_run.load(Ordering::Relaxed),
            experiments_succeeded: self.stats.experiments_succeeded.load(Ordering::Relaxed),
            experiments_failed: self.stats.experiments_failed.load(Ordering::Relaxed),
            faults_injected: self.stats.faults_injected.load(Ordering::Relaxed),
            total_fault_duration_secs: self
                .stats
                .total_fault_duration_secs
                .load(Ordering::Relaxed),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn enabled_config() -> ChaosConfig {
        ChaosConfig {
            enabled: true,
            max_concurrent_experiments: 3,
            require_confirmation: false,
            safe_mode: false,
            allowed_fault_types: Vec::new(),
        }
    }

    fn safe_config() -> ChaosConfig {
        ChaosConfig {
            enabled: true,
            safe_mode: true,
            ..enabled_config()
        }
    }

    fn sample_fault(intensity: FaultIntensity) -> FaultSpec {
        FaultSpec {
            fault_type: FaultType::LatencyInjection {
                min_ms: 50,
                max_ms: 200,
            },
            intensity,
            duration_secs: 30,
        }
    }

    fn sample_result(success: bool) -> ExperimentResult {
        let snap = ChaosMetricSnapshot {
            throughput_msg_sec: 1000.0,
            latency_p99_ms: 5.0,
            error_rate: 0.0,
            active_connections: 10,
        };
        ExperimentResult {
            phase: "steady-state".into(),
            success,
            message: "ok".into(),
            metrics_before: snap.clone(),
            metrics_after: snap,
            timestamp: chrono::Utc::now().to_rfc3339(),
        }
    }

    // 1. Disabled engine refuses to create experiments
    #[test]
    fn disabled_engine_refuses_create() {
        let engine = ChaosEngine::new(ChaosConfig::default());
        let res = engine.create_experiment(
            "test",
            "desc",
            sample_fault(FaultIntensity::Low),
            ChaosTarget::AllBrokers,
            ChaosSchedule::Immediate,
        );
        assert!(matches!(res, Err(ChaosError::Disabled)));
    }

    // 2. Disabled engine refuses to start experiments
    #[test]
    fn disabled_engine_refuses_start() {
        let engine = ChaosEngine::new(ChaosConfig::default());
        assert!(matches!(
            engine.start_experiment("any"),
            Err(ChaosError::Disabled)
        ));
    }

    // 3. Create experiment returns unique id
    #[test]
    fn create_experiment_returns_id() {
        let engine = ChaosEngine::new(enabled_config());
        let id = engine
            .create_experiment(
                "latency",
                "inject latency",
                sample_fault(FaultIntensity::Low),
                ChaosTarget::AllBrokers,
                ChaosSchedule::Immediate,
            )
            .unwrap();
        assert!(!id.is_empty());
    }

    // 4. Created experiment is retrievable
    #[test]
    fn get_experiment_after_create() {
        let engine = ChaosEngine::new(enabled_config());
        let id = engine
            .create_experiment(
                "latency",
                "inject latency",
                sample_fault(FaultIntensity::Low),
                ChaosTarget::Topic("test".into()),
                ChaosSchedule::Immediate,
            )
            .unwrap();
        let exp = engine.get_experiment(&id).unwrap();
        assert_eq!(exp.name, "latency");
        assert_eq!(exp.status, ExperimentStatus::Pending);
    }

    // 5. List experiments returns all
    #[test]
    fn list_experiments() {
        let engine = ChaosEngine::new(enabled_config());
        engine
            .create_experiment(
                "a",
                "",
                sample_fault(FaultIntensity::Low),
                ChaosTarget::AllBrokers,
                ChaosSchedule::Immediate,
            )
            .unwrap();
        engine
            .create_experiment(
                "b",
                "",
                sample_fault(FaultIntensity::Low),
                ChaosTarget::Network,
                ChaosSchedule::Immediate,
            )
            .unwrap();
        assert_eq!(engine.list_experiments().len(), 2);
    }

    // 6. Start experiment transitions to Running
    #[test]
    fn start_experiment_transitions() {
        let engine = ChaosEngine::new(enabled_config());
        let id = engine
            .create_experiment(
                "e",
                "",
                sample_fault(FaultIntensity::Low),
                ChaosTarget::AllBrokers,
                ChaosSchedule::Immediate,
            )
            .unwrap();
        engine.start_experiment(&id).unwrap();
        let exp = engine.get_experiment(&id).unwrap();
        assert_eq!(exp.status, ExperimentStatus::Running);
        assert!(exp.started_at.is_some());
    }

    // 7. Stop experiment transitions to Completed
    #[test]
    fn stop_experiment_transitions() {
        let engine = ChaosEngine::new(enabled_config());
        let id = engine
            .create_experiment(
                "e",
                "",
                sample_fault(FaultIntensity::Low),
                ChaosTarget::AllBrokers,
                ChaosSchedule::Immediate,
            )
            .unwrap();
        engine.start_experiment(&id).unwrap();
        engine.stop_experiment(&id).unwrap();
        let exp = engine.get_experiment(&id).unwrap();
        assert_eq!(exp.status, ExperimentStatus::Completed);
        assert!(exp.ended_at.is_some());
    }

    // 8. Cannot start a non-pending experiment
    #[test]
    fn start_completed_experiment_fails() {
        let engine = ChaosEngine::new(enabled_config());
        let id = engine
            .create_experiment(
                "e",
                "",
                sample_fault(FaultIntensity::Low),
                ChaosTarget::AllBrokers,
                ChaosSchedule::Immediate,
            )
            .unwrap();
        engine.start_experiment(&id).unwrap();
        engine.stop_experiment(&id).unwrap();
        assert!(matches!(
            engine.start_experiment(&id),
            Err(ChaosError::InvalidTransition { .. })
        ));
    }

    // 9. Cannot stop a non-running experiment
    #[test]
    fn stop_pending_experiment_fails() {
        let engine = ChaosEngine::new(enabled_config());
        let id = engine
            .create_experiment(
                "e",
                "",
                sample_fault(FaultIntensity::Low),
                ChaosTarget::AllBrokers,
                ChaosSchedule::Immediate,
            )
            .unwrap();
        assert!(matches!(
            engine.stop_experiment(&id),
            Err(ChaosError::InvalidTransition { .. })
        ));
    }

    // 10. Delete pending experiment works
    #[test]
    fn delete_pending_experiment() {
        let engine = ChaosEngine::new(enabled_config());
        let id = engine
            .create_experiment(
                "e",
                "",
                sample_fault(FaultIntensity::Low),
                ChaosTarget::AllBrokers,
                ChaosSchedule::Immediate,
            )
            .unwrap();
        engine.delete_experiment(&id).unwrap();
        assert!(engine.get_experiment(&id).is_none());
    }

    // 11. Cannot delete running experiment
    #[test]
    fn delete_running_experiment_fails() {
        let engine = ChaosEngine::new(enabled_config());
        let id = engine
            .create_experiment(
                "e",
                "",
                sample_fault(FaultIntensity::Low),
                ChaosTarget::AllBrokers,
                ChaosSchedule::Immediate,
            )
            .unwrap();
        engine.start_experiment(&id).unwrap();
        assert!(matches!(
            engine.delete_experiment(&id),
            Err(ChaosError::InvalidTransition { .. })
        ));
    }

    // 12. Start non-existent experiment fails
    #[test]
    fn start_nonexistent_experiment() {
        let engine = ChaosEngine::new(enabled_config());
        assert!(matches!(
            engine.start_experiment("nope"),
            Err(ChaosError::NotFound(_))
        ));
    }

    // 13. Safe mode rejects high intensity
    #[test]
    fn safe_mode_rejects_high_intensity() {
        let engine = ChaosEngine::new(safe_config());
        let res = engine.create_experiment(
            "e",
            "",
            sample_fault(FaultIntensity::High),
            ChaosTarget::AllBrokers,
            ChaosSchedule::Immediate,
        );
        assert!(matches!(res, Err(ChaosError::IntensityNotAllowed(_))));
    }

    // 14. Safe mode rejects extreme intensity
    #[test]
    fn safe_mode_rejects_extreme_intensity() {
        let engine = ChaosEngine::new(safe_config());
        let res = engine.create_experiment(
            "e",
            "",
            sample_fault(FaultIntensity::Extreme),
            ChaosTarget::AllBrokers,
            ChaosSchedule::Immediate,
        );
        assert!(matches!(res, Err(ChaosError::IntensityNotAllowed(_))));
    }

    // 15. Safe mode allows low intensity
    #[test]
    fn safe_mode_allows_low_intensity() {
        let engine = ChaosEngine::new(safe_config());
        assert!(engine
            .create_experiment(
                "e",
                "",
                sample_fault(FaultIntensity::Low),
                ChaosTarget::AllBrokers,
                ChaosSchedule::Immediate,
            )
            .is_ok());
    }

    // 16. Safe mode allows medium intensity
    #[test]
    fn safe_mode_allows_medium_intensity() {
        let engine = ChaosEngine::new(safe_config());
        assert!(engine
            .create_experiment(
                "e",
                "",
                sample_fault(FaultIntensity::Medium),
                ChaosTarget::AllBrokers,
                ChaosSchedule::Immediate,
            )
            .is_ok());
    }

    // 17. Allowed fault types restriction
    #[test]
    fn fault_type_restriction() {
        let config = ChaosConfig {
            allowed_fault_types: vec![FaultType::LeaderElection],
            ..enabled_config()
        };
        let engine = ChaosEngine::new(config);
        let res = engine.create_experiment(
            "e",
            "",
            sample_fault(FaultIntensity::Low), // LatencyInjection – not allowed
            ChaosTarget::AllBrokers,
            ChaosSchedule::Immediate,
        );
        assert!(matches!(res, Err(ChaosError::FaultNotAllowed(_))));
    }

    // 18. Allowed fault types permit matching discriminant
    #[test]
    fn fault_type_allowed_when_matching() {
        let config = ChaosConfig {
            allowed_fault_types: vec![FaultType::LatencyInjection {
                min_ms: 0,
                max_ms: 0,
            }],
            ..enabled_config()
        };
        let engine = ChaosEngine::new(config);
        assert!(engine
            .create_experiment(
                "e",
                "",
                sample_fault(FaultIntensity::Low),
                ChaosTarget::AllBrokers,
                ChaosSchedule::Immediate,
            )
            .is_ok());
    }

    // 19. is_fault_allowed with empty allow list
    #[test]
    fn is_fault_allowed_empty_list() {
        let engine = ChaosEngine::new(enabled_config());
        assert!(engine.is_fault_allowed(&FaultType::LeaderElection));
        assert!(engine.is_fault_allowed(&FaultType::PartitionUnavailable));
    }

    // 20. Max concurrent experiments enforced
    #[test]
    fn max_concurrent_experiments() {
        let config = ChaosConfig {
            max_concurrent_experiments: 1,
            ..enabled_config()
        };
        let engine = ChaosEngine::new(config);
        let id1 = engine
            .create_experiment(
                "a",
                "",
                sample_fault(FaultIntensity::Low),
                ChaosTarget::AllBrokers,
                ChaosSchedule::Immediate,
            )
            .unwrap();
        let id2 = engine
            .create_experiment(
                "b",
                "",
                sample_fault(FaultIntensity::Low),
                ChaosTarget::AllBrokers,
                ChaosSchedule::Immediate,
            )
            .unwrap();
        engine.start_experiment(&id1).unwrap();
        assert!(matches!(
            engine.start_experiment(&id2),
            Err(ChaosError::MaxConcurrentReached(1))
        ));
    }

    // 21. Stats after lifecycle
    #[test]
    fn stats_after_lifecycle() {
        let engine = ChaosEngine::new(enabled_config());
        let id = engine
            .create_experiment(
                "e",
                "",
                sample_fault(FaultIntensity::Low),
                ChaosTarget::AllBrokers,
                ChaosSchedule::Immediate,
            )
            .unwrap();
        engine.start_experiment(&id).unwrap();
        engine.stop_experiment(&id).unwrap();
        let s = engine.stats();
        assert_eq!(s.experiments_run, 1);
        assert_eq!(s.experiments_succeeded, 1);
        assert_eq!(s.faults_injected, 1);
        assert_eq!(s.total_fault_duration_secs, 30);
    }

    // 22. Record result success
    #[test]
    fn record_result_success() {
        let engine = ChaosEngine::new(enabled_config());
        let id = engine
            .create_experiment(
                "e",
                "",
                sample_fault(FaultIntensity::Low),
                ChaosTarget::AllBrokers,
                ChaosSchedule::Immediate,
            )
            .unwrap();
        engine.record_result(&id, sample_result(true)).unwrap();
        let exp = engine.get_experiment(&id).unwrap();
        assert_eq!(exp.results.len(), 1);
        assert!(exp.results[0].success);
    }

    // 23. Record result failure increments stats
    #[test]
    fn record_result_failure_increments_stats() {
        let engine = ChaosEngine::new(enabled_config());
        let id = engine
            .create_experiment(
                "e",
                "",
                sample_fault(FaultIntensity::Low),
                ChaosTarget::AllBrokers,
                ChaosSchedule::Immediate,
            )
            .unwrap();
        engine.record_result(&id, sample_result(false)).unwrap();
        assert_eq!(engine.stats().experiments_failed, 1);
    }

    // 24. Record result for non-existent experiment
    #[test]
    fn record_result_nonexistent() {
        let engine = ChaosEngine::new(enabled_config());
        assert!(matches!(
            engine.record_result("nope", sample_result(true)),
            Err(ChaosError::NotFound(_))
        ));
    }

    // 25. Default config values
    #[test]
    fn default_config() {
        let cfg = ChaosConfig::default();
        assert!(!cfg.enabled);
        assert_eq!(cfg.max_concurrent_experiments, 3);
        assert!(cfg.require_confirmation);
        assert!(cfg.safe_mode);
        assert!(cfg.allowed_fault_types.is_empty());
    }

    // 26. Initial stats are zero
    #[test]
    fn initial_stats_zero() {
        let engine = ChaosEngine::new(enabled_config());
        let s = engine.stats();
        assert_eq!(
            s,
            ChaosStatsSnapshot {
                experiments_run: 0,
                experiments_succeeded: 0,
                experiments_failed: 0,
                faults_injected: 0,
                total_fault_duration_secs: 0,
            }
        );
    }

    // 27. Various targets stored correctly
    #[test]
    fn various_targets() {
        let engine = ChaosEngine::new(enabled_config());
        let targets = vec![
            ChaosTarget::AllBrokers,
            ChaosTarget::Broker("b1".into()),
            ChaosTarget::Topic("t1".into()),
            ChaosTarget::Partition {
                topic: "t".into(),
                partition: 0,
            },
            ChaosTarget::ConsumerGroup("cg".into()),
            ChaosTarget::Network,
        ];
        for (i, target) in targets.into_iter().enumerate() {
            let expected = target.clone();
            let id = engine
                .create_experiment(
                    &format!("e{i}"),
                    "",
                    sample_fault(FaultIntensity::Low),
                    target,
                    ChaosSchedule::Immediate,
                )
                .unwrap();
            assert_eq!(engine.get_experiment(&id).unwrap().target, expected);
        }
    }

    // 28. Various schedules stored correctly
    #[test]
    fn various_schedules() {
        let engine = ChaosEngine::new(enabled_config());
        let schedules = vec![
            ChaosSchedule::Immediate,
            ChaosSchedule::Delayed { delay_secs: 10 },
            ChaosSchedule::Recurring {
                interval_secs: 60,
                count: Some(5),
            },
            ChaosSchedule::RandomWindow { window_secs: 300 },
        ];
        for (i, sched) in schedules.into_iter().enumerate() {
            let expected = sched.clone();
            let id = engine
                .create_experiment(
                    &format!("s{i}"),
                    "",
                    sample_fault(FaultIntensity::Low),
                    ChaosTarget::AllBrokers,
                    sched,
                )
                .unwrap();
            assert_eq!(engine.get_experiment(&id).unwrap().schedule, expected);
        }
    }

    // 29. Delete completed experiment succeeds
    #[test]
    fn delete_completed_experiment() {
        let engine = ChaosEngine::new(enabled_config());
        let id = engine
            .create_experiment(
                "e",
                "",
                sample_fault(FaultIntensity::Low),
                ChaosTarget::AllBrokers,
                ChaosSchedule::Immediate,
            )
            .unwrap();
        engine.start_experiment(&id).unwrap();
        engine.stop_experiment(&id).unwrap();
        engine.delete_experiment(&id).unwrap();
        assert!(engine.get_experiment(&id).is_none());
    }

    // 30. Multiple results accumulate
    #[test]
    fn multiple_results_accumulate() {
        let engine = ChaosEngine::new(enabled_config());
        let id = engine
            .create_experiment(
                "e",
                "",
                sample_fault(FaultIntensity::Low),
                ChaosTarget::AllBrokers,
                ChaosSchedule::Immediate,
            )
            .unwrap();
        engine.record_result(&id, sample_result(true)).unwrap();
        engine.record_result(&id, sample_result(true)).unwrap();
        engine.record_result(&id, sample_result(false)).unwrap();
        let exp = engine.get_experiment(&id).unwrap();
        assert_eq!(exp.results.len(), 3);
        assert_eq!(engine.stats().experiments_failed, 1);
    }
}
