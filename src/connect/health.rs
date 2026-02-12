//! Health monitoring and task rebalancing for the Kafka Connect runtime
//!
//! Provides:
//! - Periodic health checks for connectors and tasks
//! - Health history tracking and status aggregation
//! - Automatic failure detection and restart triggering
//! - Task rebalancing across available workers

use super::{ConnectorState, TaskState};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tracing::{debug, info, warn};

/// Default number of health checks to retain per connector
const DEFAULT_HISTORY_SIZE: usize = 100;

/// Health status for a connector or task
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum HealthStatus {
    /// All checks passing
    Healthy,
    /// Some tasks degraded but connector operational
    Degraded,
    /// Connector or task has failed
    Failed,
    /// Status not yet determined
    Unknown,
}

impl std::fmt::Display for HealthStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HealthStatus::Healthy => write!(f, "HEALTHY"),
            HealthStatus::Degraded => write!(f, "DEGRADED"),
            HealthStatus::Failed => write!(f, "FAILED"),
            HealthStatus::Unknown => write!(f, "UNKNOWN"),
        }
    }
}

/// Result of a single health check
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheck {
    /// Name of the connector checked
    pub connector_name: String,
    /// Time of the check (milliseconds since monitor creation)
    pub check_time_ms: u64,
    /// Determined health status
    pub status: HealthStatus,
    /// Latency of the check in milliseconds
    pub latency_ms: u64,
    /// Error message if unhealthy
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Health report for a single connector
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorHealthReport {
    /// Connector name
    pub connector_name: String,
    /// Current health status
    pub status: HealthStatus,
    /// Uptime in seconds (since last successful start)
    pub uptime_seconds: u64,
    /// Number of restarts triggered by the monitor
    pub restart_count: u64,
    /// Last failure message
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_failure: Option<String>,
    /// Per-task health status
    pub tasks: Vec<TaskHealthStatus>,
}

/// Health status for a single task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskHealthStatus {
    /// Task ID
    pub task_id: i32,
    /// Current health status
    pub status: HealthStatus,
    /// Error message if unhealthy
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Cluster-wide health summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterHealthSummary {
    /// Overall cluster status
    pub status: HealthStatus,
    /// Total connectors monitored
    pub total_connectors: usize,
    /// Healthy connectors
    pub healthy_count: usize,
    /// Degraded connectors
    pub degraded_count: usize,
    /// Failed connectors
    pub failed_count: usize,
    /// Total health checks performed
    pub total_checks: u64,
}

/// Internal per-connector tracking state
struct ConnectorHealthState {
    history: Vec<HealthCheck>,
    history_max: usize,
    restart_count: AtomicU64,
    started_at: Instant,
    last_failure: Option<String>,
}

impl ConnectorHealthState {
    fn new(history_max: usize) -> Self {
        Self {
            history: Vec::with_capacity(history_max),
            history_max,
            restart_count: AtomicU64::new(0),
            started_at: Instant::now(),
            last_failure: None,
        }
    }

    fn push_check(&mut self, check: HealthCheck) {
        if self.history.len() >= self.history_max {
            self.history.remove(0);
        }
        if check.status == HealthStatus::Failed {
            self.last_failure = check.error.clone();
        }
        self.history.push(check);
    }

    fn latest_status(&self) -> HealthStatus {
        self.history
            .last()
            .map(|c| c.status)
            .unwrap_or(HealthStatus::Unknown)
    }
}

/// Monitors connector and task health via periodic checks
pub struct ConnectorHealthMonitor {
    /// Health check interval in milliseconds
    check_interval_ms: u64,
    /// Per-connector health state
    states: RwLock<HashMap<String, ConnectorHealthState>>,
    /// Maximum history entries per connector
    history_size: usize,
    /// Monotonic reference for check timestamps
    created_at: Instant,
    /// Total checks performed
    total_checks: AtomicU64,
}

impl ConnectorHealthMonitor {
    /// Create a new health monitor with the given check interval
    pub fn new(check_interval_ms: u64) -> Self {
        info!(check_interval_ms, "Creating connector health monitor");
        Self {
            check_interval_ms,
            states: RwLock::new(HashMap::new()),
            history_size: DEFAULT_HISTORY_SIZE,
            created_at: Instant::now(),
            total_checks: AtomicU64::new(0),
        }
    }

    /// Return the configured check interval in milliseconds
    pub fn check_interval_ms(&self) -> u64 {
        self.check_interval_ms
    }

    /// Perform a health check for a connector given its state and task states.
    ///
    /// Returns the resulting `HealthCheck` and records it in history.
    pub fn check_connector(
        &self,
        name: &str,
        connector_state: &ConnectorState,
        task_states: &[(i32, TaskState)],
    ) -> HealthCheck {
        let start = Instant::now();

        let (status, error) = Self::evaluate_health(connector_state, task_states);

        let latency_ms = start.elapsed().as_millis() as u64;
        let check_time_ms = self.created_at.elapsed().as_millis() as u64;

        let check = HealthCheck {
            connector_name: name.to_string(),
            check_time_ms,
            status,
            latency_ms,
            error: error.clone(),
        };

        // Record the check
        let mut states = self.states.write();
        let state = states
            .entry(name.to_string())
            .or_insert_with(|| ConnectorHealthState::new(self.history_size));
        state.push_check(check.clone());
        self.total_checks.fetch_add(1, Ordering::Relaxed);

        match status {
            HealthStatus::Healthy => {
                debug!(connector = name, "Health check passed");
            }
            HealthStatus::Degraded => {
                warn!(connector = name, "Connector degraded");
            }
            HealthStatus::Failed => {
                warn!(connector = name, error = ?error, "Connector failed");
                state.restart_count.fetch_add(1, Ordering::Relaxed);
            }
            HealthStatus::Unknown => {
                debug!(connector = name, "Connector status unknown");
            }
        }

        check
    }

    /// Evaluate health from connector and task states
    fn evaluate_health(
        connector_state: &ConnectorState,
        task_states: &[(i32, TaskState)],
    ) -> (HealthStatus, Option<String>) {
        match connector_state {
            ConnectorState::Failed => (
                HealthStatus::Failed,
                Some("Connector is in FAILED state".to_string()),
            ),
            ConnectorState::Unassigned => (
                HealthStatus::Unknown,
                Some("Connector is unassigned".to_string()),
            ),
            ConnectorState::Paused => (HealthStatus::Healthy, None),
            ConnectorState::Restarting => (
                HealthStatus::Degraded,
                Some("Connector is restarting".to_string()),
            ),
            ConnectorState::Running => {
                let failed: Vec<i32> = task_states
                    .iter()
                    .filter(|(_, s)| *s == TaskState::Failed)
                    .map(|(id, _)| *id)
                    .collect();

                if failed.is_empty() {
                    (HealthStatus::Healthy, None)
                } else if failed.len() == task_states.len() {
                    (
                        HealthStatus::Failed,
                        Some(format!("All {} tasks failed", failed.len())),
                    )
                } else {
                    (
                        HealthStatus::Degraded,
                        Some(format!("Tasks failed: {:?}", failed)),
                    )
                }
            }
        }
    }

    /// Get a health report for a specific connector
    pub fn get_health_report(
        &self,
        name: &str,
        task_states: &[(i32, TaskState)],
    ) -> Option<ConnectorHealthReport> {
        let states = self.states.read();
        let state = states.get(name)?;

        let tasks = task_states
            .iter()
            .map(|(id, ts)| {
                let (status, error) = match ts {
                    TaskState::Running => (HealthStatus::Healthy, None),
                    TaskState::Paused => (HealthStatus::Healthy, None),
                    TaskState::Unassigned => {
                        (HealthStatus::Unknown, Some("Task unassigned".to_string()))
                    }
                    TaskState::Failed => (HealthStatus::Failed, Some("Task failed".to_string())),
                };
                TaskHealthStatus {
                    task_id: *id,
                    status,
                    error,
                }
            })
            .collect();

        Some(ConnectorHealthReport {
            connector_name: name.to_string(),
            status: state.latest_status(),
            uptime_seconds: state.started_at.elapsed().as_secs(),
            restart_count: state.restart_count.load(Ordering::Relaxed),
            last_failure: state.last_failure.clone(),
            tasks,
        })
    }

    /// Get a cluster-wide health summary across all monitored connectors
    pub fn get_cluster_health(&self) -> ClusterHealthSummary {
        let states = self.states.read();

        let mut healthy = 0usize;
        let mut degraded = 0usize;
        let mut failed = 0usize;

        for state in states.values() {
            match state.latest_status() {
                HealthStatus::Healthy => healthy += 1,
                HealthStatus::Degraded => degraded += 1,
                HealthStatus::Failed => failed += 1,
                HealthStatus::Unknown => {}
            }
        }

        let overall = if failed > 0 {
            HealthStatus::Failed
        } else if degraded > 0 {
            HealthStatus::Degraded
        } else if healthy > 0 {
            HealthStatus::Healthy
        } else {
            HealthStatus::Unknown
        };

        ClusterHealthSummary {
            status: overall,
            total_connectors: states.len(),
            healthy_count: healthy,
            degraded_count: degraded,
            failed_count: failed,
            total_checks: self.total_checks.load(Ordering::Relaxed),
        }
    }

    /// Record a connector restart (resets the started_at timer)
    pub fn record_restart(&self, name: &str) {
        let mut states = self.states.write();
        if let Some(state) = states.get_mut(name) {
            state.started_at = Instant::now();
            info!(connector = name, "Recorded connector restart");
        }
    }

    /// Remove tracking state for a connector
    pub fn remove_connector(&self, name: &str) {
        let mut states = self.states.write();
        states.remove(name);
        debug!(connector = name, "Removed connector from health monitor");
    }
}

/// Rebalancing strategy for distributing tasks across workers
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RebalanceStrategy {
    /// Assign tasks to workers in round-robin order
    RoundRobin,
    /// Assign tasks to the worker with the fewest current assignments
    LeastLoaded,
}

impl std::fmt::Display for RebalanceStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RebalanceStrategy::RoundRobin => write!(f, "round-robin"),
            RebalanceStrategy::LeastLoaded => write!(f, "least-loaded"),
        }
    }
}

/// Distributes connector tasks across available workers
pub struct TaskRebalancer {
    strategy: RebalanceStrategy,
    /// connector_name -> task_count
    connectors: RwLock<HashMap<String, usize>>,
    /// Current assignment: (connector_name, task_id) -> worker_id
    assignments: RwLock<HashMap<(String, i32), String>>,
}

impl TaskRebalancer {
    /// Create a new rebalancer with the given strategy
    pub fn new(strategy: RebalanceStrategy) -> Self {
        info!(%strategy, "Creating task rebalancer");
        Self {
            strategy,
            connectors: RwLock::new(HashMap::new()),
            assignments: RwLock::new(HashMap::new()),
        }
    }

    /// Register a connector with its task count
    pub fn add_connector(&self, name: &str, task_count: usize) {
        let mut connectors = self.connectors.write();
        connectors.insert(name.to_string(), task_count);
        info!(
            connector = name,
            task_count, "Added connector to rebalancer"
        );
    }

    /// Remove a connector and its assignments
    pub fn remove_connector(&self, name: &str) {
        {
            let mut connectors = self.connectors.write();
            connectors.remove(name);
        }
        {
            let mut assignments = self.assignments.write();
            assignments.retain(|(c, _), _| c != name);
        }
        info!(connector = name, "Removed connector from rebalancer");
    }

    /// Rebalance all tasks across the given workers.
    ///
    /// `connectors` maps connector name to task count.
    /// `workers` is the list of available worker IDs.
    ///
    /// Returns the new assignment map: (connector_name, task_id) -> worker_id.
    pub fn rebalance(
        &self,
        connectors: &HashMap<String, usize>,
        workers: &[String],
    ) -> HashMap<(String, i32), String> {
        if workers.is_empty() {
            warn!("No workers available for rebalancing");
            return HashMap::new();
        }

        let new_assignments = match self.strategy {
            RebalanceStrategy::RoundRobin => Self::rebalance_round_robin(connectors, workers),
            RebalanceStrategy::LeastLoaded => Self::rebalance_least_loaded(connectors, workers),
        };

        // Update internal state
        {
            let mut stored = self.connectors.write();
            for (name, count) in connectors {
                stored.insert(name.clone(), *count);
            }
        }
        {
            let mut stored = self.assignments.write();
            *stored = new_assignments.clone();
        }

        info!(
            strategy = %self.strategy,
            tasks = new_assignments.len(),
            workers = workers.len(),
            "Rebalanced tasks"
        );

        new_assignments
    }

    fn rebalance_round_robin(
        connectors: &HashMap<String, usize>,
        workers: &[String],
    ) -> HashMap<(String, i32), String> {
        let mut result = HashMap::new();
        let mut worker_idx = 0usize;

        // Sort connector names for deterministic assignment
        let mut names: Vec<&String> = connectors.keys().collect();
        names.sort();

        for name in names {
            let count = connectors[name];
            for task_id in 0..count {
                let worker = &workers[worker_idx % workers.len()];
                result.insert((name.clone(), task_id as i32), worker.clone());
                worker_idx += 1;
            }
        }

        result
    }

    fn rebalance_least_loaded(
        connectors: &HashMap<String, usize>,
        workers: &[String],
    ) -> HashMap<(String, i32), String> {
        let mut result = HashMap::new();
        let mut load: HashMap<usize, usize> = (0..workers.len()).map(|i| (i, 0usize)).collect();

        // Sort connector names for deterministic assignment
        let mut names: Vec<&String> = connectors.keys().collect();
        names.sort();

        for name in names {
            let count = connectors[name];
            for task_id in 0..count {
                // Pick the worker index with the smallest current load
                let Some(&idx) = load.iter().min_by_key(|(_, &c)| c).map(|(i, _)| i) else {
                    continue;
                };

                if let Some(count) = load.get_mut(&idx) {
                    *count += 1;
                }
                result.insert((name.clone(), task_id as i32), workers[idx].clone());
            }
        }

        result
    }

    /// Get the current task assignment map
    pub fn get_assignments(&self) -> HashMap<(String, i32), String> {
        self.assignments.read().clone()
    }

    /// Get the current rebalancing strategy
    pub fn strategy(&self) -> RebalanceStrategy {
        self.strategy
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_check_healthy_connector() {
        let monitor = ConnectorHealthMonitor::new(5000);

        let tasks = vec![(0, TaskState::Running), (1, TaskState::Running)];
        let check = monitor.check_connector("my-source", &ConnectorState::Running, &tasks);

        assert_eq!(check.status, HealthStatus::Healthy);
        assert!(check.error.is_none());
        assert_eq!(check.connector_name, "my-source");
    }

    #[test]
    fn test_health_check_failed_connector() {
        let monitor = ConnectorHealthMonitor::new(5000);

        let check = monitor.check_connector("bad-sink", &ConnectorState::Failed, &[]);

        assert_eq!(check.status, HealthStatus::Failed);
        assert!(check.error.is_some());

        let summary = monitor.get_cluster_health();
        assert_eq!(summary.failed_count, 1);
        assert_eq!(summary.status, HealthStatus::Failed);
    }

    #[test]
    fn test_health_check_degraded_tasks() {
        let monitor = ConnectorHealthMonitor::new(5000);

        let tasks = vec![
            (0, TaskState::Running),
            (1, TaskState::Failed),
            (2, TaskState::Running),
        ];
        let check = monitor.check_connector("partial", &ConnectorState::Running, &tasks);

        assert_eq!(check.status, HealthStatus::Degraded);
        assert!(check.error.unwrap().contains("Tasks failed"));

        let report = monitor.get_health_report("partial", &tasks).unwrap();
        assert_eq!(report.status, HealthStatus::Degraded);
        assert_eq!(report.tasks.len(), 3);
        assert_eq!(report.tasks[1].status, HealthStatus::Failed);
    }

    #[test]
    fn test_cluster_health_summary() {
        let monitor = ConnectorHealthMonitor::new(1000);

        monitor.check_connector("ok-1", &ConnectorState::Running, &[(0, TaskState::Running)]);
        monitor.check_connector("ok-2", &ConnectorState::Running, &[(0, TaskState::Running)]);
        monitor.check_connector("bad", &ConnectorState::Failed, &[]);

        let summary = monitor.get_cluster_health();
        assert_eq!(summary.total_connectors, 3);
        assert_eq!(summary.healthy_count, 2);
        assert_eq!(summary.failed_count, 1);
        assert_eq!(summary.total_checks, 3);
        assert_eq!(summary.status, HealthStatus::Failed);
    }

    #[test]
    fn test_rebalancer_round_robin() {
        let rebalancer = TaskRebalancer::new(RebalanceStrategy::RoundRobin);

        let mut connectors = HashMap::new();
        connectors.insert("source-a".to_string(), 2);
        connectors.insert("sink-b".to_string(), 2);

        let workers = vec!["worker-1".to_string(), "worker-2".to_string()];
        let assignments = rebalancer.rebalance(&connectors, &workers);

        assert_eq!(assignments.len(), 4);
        // Every task should be assigned to some worker
        for worker in assignments.values() {
            assert!(workers.contains(worker));
        }
    }

    #[test]
    fn test_rebalancer_least_loaded() {
        let rebalancer = TaskRebalancer::new(RebalanceStrategy::LeastLoaded);

        let mut connectors = HashMap::new();
        connectors.insert("conn-a".to_string(), 3);

        let workers = vec!["w1".to_string(), "w2".to_string(), "w3".to_string()];
        let assignments = rebalancer.rebalance(&connectors, &workers);

        assert_eq!(assignments.len(), 3);
        // With 3 tasks and 3 workers, each worker should get exactly 1
        let mut counts: HashMap<&str, usize> = HashMap::new();
        for worker in assignments.values() {
            *counts.entry(worker.as_str()).or_default() += 1;
        }
        for &c in counts.values() {
            assert_eq!(c, 1);
        }
    }

    #[test]
    fn test_rebalancer_add_remove_connector() {
        let rebalancer = TaskRebalancer::new(RebalanceStrategy::RoundRobin);

        rebalancer.add_connector("c1", 2);
        rebalancer.add_connector("c2", 1);

        let mut connectors = HashMap::new();
        connectors.insert("c1".to_string(), 2);
        connectors.insert("c2".to_string(), 1);
        let workers = vec!["w1".to_string()];
        let assignments = rebalancer.rebalance(&connectors, &workers);
        assert_eq!(assignments.len(), 3);

        rebalancer.remove_connector("c2");
        let kept = rebalancer.get_assignments();
        assert!(!kept.contains_key(&("c2".to_string(), 0)));
    }

    #[test]
    fn test_rebalancer_empty_workers() {
        let rebalancer = TaskRebalancer::new(RebalanceStrategy::RoundRobin);
        let mut connectors = HashMap::new();
        connectors.insert("c1".to_string(), 2);

        let assignments = rebalancer.rebalance(&connectors, &[]);
        assert!(assignments.is_empty());
    }

    #[test]
    fn test_health_status_display() {
        assert_eq!(format!("{}", HealthStatus::Healthy), "HEALTHY");
        assert_eq!(format!("{}", HealthStatus::Degraded), "DEGRADED");
        assert_eq!(format!("{}", HealthStatus::Failed), "FAILED");
        assert_eq!(format!("{}", HealthStatus::Unknown), "UNKNOWN");
    }
}
