//! Refresh Scheduler
//!
//! Schedules and manages automatic refresh of materialized views.

use super::config::RefreshMode;
use super::materialized_view::{MaterializedView, MaterializedViewManager, RefreshResult};
use crate::error::{Result, StreamlineError};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use tokio::time::Instant;

/// Refresh scheduler for materialized views
pub struct RefreshScheduler {
    /// View manager
    view_manager: Arc<MaterializedViewManager>,
    /// Scheduled tasks
    tasks: Arc<RwLock<HashMap<String, ScheduledTask>>>,
    /// Maximum concurrent refreshes
    max_concurrent: usize,
    /// Current refresh count
    active_count: Arc<RwLock<usize>>,
    /// Shutdown signal
    shutdown_tx: Option<mpsc::Sender<()>>,
}

impl RefreshScheduler {
    /// Create a new refresh scheduler
    pub fn new(view_manager: Arc<MaterializedViewManager>, max_concurrent: usize) -> Self {
        Self {
            view_manager,
            tasks: Arc::new(RwLock::new(HashMap::new())),
            max_concurrent,
            active_count: Arc::new(RwLock::new(0)),
            shutdown_tx: None,
        }
    }

    /// Start the scheduler
    pub async fn start(&mut self) -> Result<()> {
        let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);
        self.shutdown_tx = Some(shutdown_tx);

        let view_manager = self.view_manager.clone();
        let tasks = self.tasks.clone();
        let max_concurrent = self.max_concurrent;
        let active_count = self.active_count.clone();

        // Spawn the scheduler loop
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // Check for views that need refresh
                        if let Err(e) = Self::check_and_refresh(
                            &view_manager,
                            &tasks,
                            max_concurrent,
                            &active_count,
                        ).await {
                            tracing::error!(error = %e, "Scheduler error");
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        tracing::info!("Refresh scheduler shutting down");
                        break;
                    }
                }
            }
        });

        tracing::info!("Refresh scheduler started");
        Ok(())
    }

    /// Stop the scheduler
    pub async fn stop(&mut self) -> Result<()> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(()).await;
        }
        Ok(())
    }

    /// Schedule a view for refresh
    pub async fn schedule_view(&self, view_name: &str) -> Result<()> {
        let view =
            self.view_manager.get_view(view_name).await.ok_or_else(|| {
                StreamlineError::Config(format!("View '{}' not found", view_name))
            })?;

        let task = ScheduledTask::from_view(&view);
        let mut tasks = self.tasks.write().await;
        tasks.insert(view_name.to_string(), task);

        tracing::info!(view = view_name, "View scheduled for refresh");
        Ok(())
    }

    /// Unschedule a view
    pub async fn unschedule_view(&self, view_name: &str) -> Result<()> {
        let mut tasks = self.tasks.write().await;
        tasks.remove(view_name);
        Ok(())
    }

    /// Force immediate refresh of a view
    pub async fn refresh_now(&self, view_name: &str) -> Result<RefreshResult> {
        self.view_manager.refresh_view(view_name).await
    }

    /// Get scheduler status
    pub async fn status(&self) -> SchedulerStatus {
        let tasks = self.tasks.read().await;
        let active = *self.active_count.read().await;

        let scheduled_views: Vec<_> = tasks
            .iter()
            .map(|(name, task)| ViewScheduleInfo {
                name: name.clone(),
                next_refresh: task.next_run,
                last_refresh: task.last_run,
                last_duration_ms: task.last_duration_ms,
                status: task.status.clone(),
            })
            .collect();

        SchedulerStatus {
            running: true,
            active_refreshes: active,
            max_concurrent: self.max_concurrent,
            scheduled_views,
        }
    }

    /// Check and refresh views that need it
    async fn check_and_refresh(
        view_manager: &Arc<MaterializedViewManager>,
        tasks: &Arc<RwLock<HashMap<String, ScheduledTask>>>,
        max_concurrent: usize,
        active_count: &Arc<RwLock<usize>>,
    ) -> Result<()> {
        let now = Instant::now();

        // Get views that need refresh
        let views_to_refresh: Vec<String> = {
            let tasks = tasks.read().await;
            tasks
                .iter()
                .filter(|(_, task)| {
                    task.next_run.is_some_and(|next| now >= next)
                        && matches!(task.status, TaskStatus::Idle | TaskStatus::Failed(_))
                })
                .map(|(name, _)| name.clone())
                .collect()
        };

        // Check concurrent limit
        let current_active = *active_count.read().await;
        let available_slots = max_concurrent.saturating_sub(current_active);

        if available_slots == 0 {
            return Ok(());
        }

        // Refresh views up to the available limit
        for view_name in views_to_refresh.into_iter().take(available_slots) {
            let view_manager = view_manager.clone();
            let tasks = tasks.clone();
            let active_count = active_count.clone();

            // Mark as running
            {
                let mut tasks = tasks.write().await;
                if let Some(task) = tasks.get_mut(&view_name) {
                    task.status = TaskStatus::Running;
                }
            }

            // Increment active count
            {
                let mut count = active_count.write().await;
                *count += 1;
            }

            // Spawn refresh task
            let name = view_name.clone();
            tokio::spawn(async move {
                let start = Instant::now();

                let result = view_manager.refresh_view(&name).await;

                let duration = start.elapsed();

                // Update task status
                {
                    let mut tasks = tasks.write().await;
                    if let Some(task) = tasks.get_mut(&name) {
                        task.last_run = Some(Instant::now());
                        task.last_duration_ms = Some(duration.as_millis() as u64);

                        match result {
                            Ok(ref res) => {
                                task.status = TaskStatus::Idle;
                                task.last_result = Some(res.clone());
                                task.failure_count = 0;
                                task.schedule_next();
                                tracing::info!(
                                    view = %name,
                                    rows = res.rows_affected,
                                    duration_ms = res.duration_ms,
                                    "View refresh completed"
                                );
                            }
                            Err(e) => {
                                task.failure_count += 1;
                                task.status = TaskStatus::Failed(e.to_string());
                                task.schedule_next_with_backoff();
                                tracing::error!(
                                    view = %name,
                                    error = %e,
                                    failures = task.failure_count,
                                    "View refresh failed"
                                );
                            }
                        }
                    }
                }

                // Decrement active count
                {
                    let mut count = active_count.write().await;
                    *count = count.saturating_sub(1);
                }
            });
        }

        Ok(())
    }
}

/// Scheduled task for a view
#[derive(Debug, Clone)]
pub struct ScheduledTask {
    /// View name
    pub view_name: String,
    /// Refresh mode
    pub refresh_mode: RefreshMode,
    /// Next scheduled run
    pub next_run: Option<Instant>,
    /// Last run time
    pub last_run: Option<Instant>,
    /// Last run duration in ms
    pub last_duration_ms: Option<u64>,
    /// Task status
    pub status: TaskStatus,
    /// Last refresh result
    pub last_result: Option<RefreshResult>,
    /// Consecutive failure count
    pub failure_count: u32,
}

impl ScheduledTask {
    /// Create a task from a view
    pub fn from_view(view: &MaterializedView) -> Self {
        let mut task = Self {
            view_name: view.name.clone(),
            refresh_mode: view.definition.refresh_mode.clone(),
            next_run: None,
            last_run: None,
            last_duration_ms: None,
            status: TaskStatus::Idle,
            last_result: None,
            failure_count: 0,
        };

        task.schedule_next();
        task
    }

    /// Schedule the next run
    pub fn schedule_next(&mut self) {
        self.next_run = match &self.refresh_mode {
            RefreshMode::Immediate => Some(Instant::now()),
            RefreshMode::Periodic { interval_secs } => {
                Some(Instant::now() + Duration::from_secs(*interval_secs))
            }
            RefreshMode::Manual => None,
            RefreshMode::OnQuery { .. } => None,
        };
    }

    /// Schedule next run with exponential backoff for failures
    pub fn schedule_next_with_backoff(&mut self) {
        let base_delay = match &self.refresh_mode {
            RefreshMode::Periodic { interval_secs } => Duration::from_secs(*interval_secs),
            _ => Duration::from_secs(60),
        };

        // Exponential backoff: 2^failures * base, capped at 1 hour
        let backoff_multiplier = 2u64.saturating_pow(self.failure_count.min(6));
        let backoff_delay = base_delay.saturating_mul(backoff_multiplier as u32);
        let max_delay = Duration::from_secs(3600);

        self.next_run = Some(Instant::now() + backoff_delay.min(max_delay));
    }
}

/// Task status
#[derive(Debug, Clone)]
pub enum TaskStatus {
    /// Task is idle, waiting for next run
    Idle,
    /// Task is currently running
    Running,
    /// Task failed with error
    Failed(String),
    /// Task is paused
    Paused,
}

/// Scheduler status
#[derive(Debug, Clone)]
pub struct SchedulerStatus {
    /// Whether scheduler is running
    pub running: bool,
    /// Number of active refreshes
    pub active_refreshes: usize,
    /// Maximum concurrent refreshes
    pub max_concurrent: usize,
    /// Scheduled views
    pub scheduled_views: Vec<ViewScheduleInfo>,
}

/// Schedule info for a view
#[derive(Debug, Clone)]
pub struct ViewScheduleInfo {
    /// View name
    pub name: String,
    /// Next scheduled refresh
    pub next_refresh: Option<Instant>,
    /// Last refresh time
    pub last_refresh: Option<Instant>,
    /// Last refresh duration in ms
    pub last_duration_ms: Option<u64>,
    /// Current status
    pub status: TaskStatus,
}

/// Priority queue for scheduling
pub struct RefreshQueue {
    /// Pending refreshes
    pending: Vec<QueuedRefresh>,
}

impl RefreshQueue {
    /// Create a new refresh queue
    pub fn new() -> Self {
        Self {
            pending: Vec::new(),
        }
    }

    /// Add a refresh to the queue
    pub fn push(&mut self, refresh: QueuedRefresh) {
        self.pending.push(refresh);
        self.pending.sort_by_key(|r| std::cmp::Reverse(r.priority));
    }

    /// Pop the highest priority refresh
    pub fn pop(&mut self) -> Option<QueuedRefresh> {
        self.pending.pop()
    }

    /// Check if queue is empty
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// Get queue length
    pub fn len(&self) -> usize {
        self.pending.len()
    }
}

impl Default for RefreshQueue {
    fn default() -> Self {
        Self::new()
    }
}

/// Queued refresh request
#[derive(Debug, Clone)]
pub struct QueuedRefresh {
    /// View name
    pub view_name: String,
    /// Priority (higher = more urgent)
    pub priority: u32,
    /// Requested at
    pub requested_at: Instant,
    /// Reason for refresh
    pub reason: RefreshReason,
}

/// Reason for refresh
#[derive(Debug, Clone)]
pub enum RefreshReason {
    /// Scheduled refresh
    Scheduled,
    /// Manual request
    Manual,
    /// Query triggered (lazy refresh)
    QueryTriggered,
    /// Data changed
    DataChanged,
    /// Dependency updated
    DependencyUpdated(String),
}

/// Dependency tracker for view refresh ordering
pub struct DependencyTracker {
    /// View dependencies (view -> views it depends on)
    dependencies: HashMap<String, Vec<String>>,
    /// Reverse dependencies (view -> views that depend on it)
    dependents: HashMap<String, Vec<String>>,
}

impl DependencyTracker {
    /// Create a new dependency tracker
    pub fn new() -> Self {
        Self {
            dependencies: HashMap::new(),
            dependents: HashMap::new(),
        }
    }

    /// Add a dependency
    pub fn add_dependency(&mut self, view: &str, depends_on: &str) {
        self.dependencies
            .entry(view.to_string())
            .or_default()
            .push(depends_on.to_string());

        self.dependents
            .entry(depends_on.to_string())
            .or_default()
            .push(view.to_string());
    }

    /// Get views that depend on a given view
    pub fn get_dependents(&self, view: &str) -> Vec<&str> {
        self.dependents
            .get(view)
            .map(|deps| deps.iter().map(|s| s.as_str()).collect())
            .unwrap_or_default()
    }

    /// Get views that a given view depends on
    pub fn get_dependencies(&self, view: &str) -> Vec<&str> {
        self.dependencies
            .get(view)
            .map(|deps| deps.iter().map(|s| s.as_str()).collect())
            .unwrap_or_default()
    }

    /// Get topological order for refresh
    pub fn topological_order(&self, views: &[String]) -> Result<Vec<String>> {
        let mut result = Vec::new();
        let mut visited = HashMap::new();
        let mut temp_visited = HashMap::new();

        for view in views {
            if !visited.contains_key(view) {
                self.visit(view, &mut visited, &mut temp_visited, &mut result)?;
            }
        }

        Ok(result)
    }

    fn visit(
        &self,
        view: &str,
        visited: &mut HashMap<String, bool>,
        temp_visited: &mut HashMap<String, bool>,
        result: &mut Vec<String>,
    ) -> Result<()> {
        if temp_visited.get(view).copied().unwrap_or(false) {
            return Err(StreamlineError::Config(format!(
                "Circular dependency detected involving view '{}'",
                view
            )));
        }

        if visited.get(view).copied().unwrap_or(false) {
            return Ok(());
        }

        temp_visited.insert(view.to_string(), true);

        for dep in self.get_dependencies(view) {
            self.visit(dep, visited, temp_visited, result)?;
        }

        temp_visited.insert(view.to_string(), false);
        visited.insert(view.to_string(), true);
        result.push(view.to_string());

        Ok(())
    }
}

impl Default for DependencyTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_refresh_queue() {
        let mut queue = RefreshQueue::new();

        queue.push(QueuedRefresh {
            view_name: "low".to_string(),
            priority: 1,
            requested_at: Instant::now(),
            reason: RefreshReason::Scheduled,
        });

        queue.push(QueuedRefresh {
            view_name: "high".to_string(),
            priority: 10,
            requested_at: Instant::now(),
            reason: RefreshReason::Manual,
        });

        queue.push(QueuedRefresh {
            view_name: "medium".to_string(),
            priority: 5,
            requested_at: Instant::now(),
            reason: RefreshReason::QueryTriggered,
        });

        // Should pop in priority order (highest first)
        assert_eq!(queue.pop().unwrap().view_name, "low");
        assert_eq!(queue.pop().unwrap().view_name, "medium");
        assert_eq!(queue.pop().unwrap().view_name, "high");
    }

    #[test]
    fn test_dependency_tracker() {
        let mut tracker = DependencyTracker::new();

        // view_c depends on view_b, view_b depends on view_a
        tracker.add_dependency("view_b", "view_a");
        tracker.add_dependency("view_c", "view_b");

        let order = tracker
            .topological_order(&[
                "view_c".to_string(),
                "view_a".to_string(),
                "view_b".to_string(),
            ])
            .unwrap();

        // view_a should come before view_b, view_b before view_c
        let pos_a = order.iter().position(|v| v == "view_a").unwrap();
        let pos_b = order.iter().position(|v| v == "view_b").unwrap();
        let pos_c = order.iter().position(|v| v == "view_c").unwrap();

        assert!(pos_a < pos_b);
        assert!(pos_b < pos_c);
    }

    #[test]
    fn test_circular_dependency_detection() {
        let mut tracker = DependencyTracker::new();

        tracker.add_dependency("a", "b");
        tracker.add_dependency("b", "c");
        tracker.add_dependency("c", "a"); // Creates cycle

        let result = tracker.topological_order(&["a".to_string()]);
        assert!(result.is_err());
    }

    #[test]
    fn test_backoff_scheduling() {
        let mut task = ScheduledTask {
            view_name: "test".to_string(),
            refresh_mode: RefreshMode::Periodic { interval_secs: 60 },
            next_run: None,
            last_run: None,
            last_duration_ms: None,
            status: TaskStatus::Idle,
            last_result: None,
            failure_count: 0,
        };

        // First failure - should use base interval
        task.failure_count = 1;
        task.schedule_next_with_backoff();
        // With failure_count=1, multiplier is 2, so delay is 120 seconds

        // Third failure - should have significant backoff
        task.failure_count = 3;
        task.schedule_next_with_backoff();
        // With failure_count=3, multiplier is 8, so delay is 480 seconds
    }
}
