//! Initial snapshot support for CDC
//!
//! Provides `SnapshotManager` for orchestrating consistent initial table
//! snapshots with multiple strategies: full initial load, on-demand,
//! schema-only, or skip entirely.

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::info;

/// Strategy for when/how to perform an initial snapshot
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotStrategy {
    /// Full snapshot on first run
    Initial,
    /// Snapshot only when no stored offset exists
    WhenNeeded,
    /// Skip snapshot, start from current WAL/binlog position
    Never,
    /// Capture table schemas but do not snapshot row data
    SchemaOnly,
}

impl Default for SnapshotStrategy {
    fn default() -> Self {
        Self::Initial
    }
}

impl std::fmt::Display for SnapshotStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SnapshotStrategy::Initial => write!(f, "initial"),
            SnapshotStrategy::WhenNeeded => write!(f, "when_needed"),
            SnapshotStrategy::Never => write!(f, "never"),
            SnapshotStrategy::SchemaOnly => write!(f, "schema_only"),
        }
    }
}

/// Current state of a snapshot
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotState {
    /// Not started
    Pending,
    /// Currently running
    Running,
    /// Completed successfully
    Completed,
    /// Failed
    Failed,
    /// Skipped (strategy was Never)
    Skipped,
}

impl Default for SnapshotState {
    fn default() -> Self {
        Self::Pending
    }
}

/// Progress tracking for a single table snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotProgress {
    /// Fully-qualified table name
    pub table: String,
    /// Current state
    pub state: SnapshotState,
    /// Rows captured so far
    pub rows_captured: u64,
    /// Estimated total rows (if available)
    pub total_rows_estimate: Option<u64>,
    /// When the snapshot started
    pub started_at: Option<DateTime<Utc>>,
    /// When the snapshot completed
    pub completed_at: Option<DateTime<Utc>>,
    /// Error message if failed
    pub error: Option<String>,
}

impl SnapshotProgress {
    /// Create a new pending progress entry
    pub fn new(table: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            state: SnapshotState::Pending,
            rows_captured: 0,
            total_rows_estimate: None,
            started_at: None,
            completed_at: None,
            error: None,
        }
    }

    /// Percentage complete (0–100), if an estimate is available
    pub fn percent_complete(&self) -> Option<f64> {
        self.total_rows_estimate.map(|total| {
            if total == 0 {
                100.0
            } else {
                (self.rows_captured as f64 / total as f64 * 100.0).min(100.0)
            }
        })
    }
}

/// Orchestrates initial table snapshots for a CDC connector
pub struct SnapshotManager {
    /// Chosen strategy
    strategy: SnapshotStrategy,
    /// Per-table progress
    progress: RwLock<HashMap<String, SnapshotProgress>>,
    /// Overall state
    state: RwLock<SnapshotState>,
}

impl SnapshotManager {
    /// Create a new SnapshotManager with the given strategy
    pub fn new(strategy: SnapshotStrategy) -> Self {
        Self {
            strategy,
            progress: RwLock::new(HashMap::new()),
            state: RwLock::new(SnapshotState::Pending),
        }
    }

    /// Get the configured strategy
    pub fn strategy(&self) -> SnapshotStrategy {
        self.strategy
    }

    /// Whether a snapshot should run, given whether a stored offset exists
    pub fn should_snapshot(&self, has_stored_offset: bool) -> bool {
        match self.strategy {
            SnapshotStrategy::Initial => true,
            SnapshotStrategy::WhenNeeded => !has_stored_offset,
            SnapshotStrategy::Never => false,
            SnapshotStrategy::SchemaOnly => true,
        }
    }

    /// Begin the overall snapshot (call before iterating tables)
    pub fn start_snapshot(&self, tables: &[String]) {
        let mut progress = self.progress.write();
        progress.clear();
        for table in tables {
            progress.insert(table.clone(), SnapshotProgress::new(table));
        }
        *self.state.write() = SnapshotState::Running;
        info!(
            strategy = %self.strategy,
            tables = tables.len(),
            "Snapshot started"
        );
    }

    /// Mark a table snapshot as started
    pub fn start_table(&self, table: &str) {
        if let Some(p) = self.progress.write().get_mut(table) {
            p.state = SnapshotState::Running;
            p.started_at = Some(Utc::now());
        }
    }

    /// Record rows captured for a table
    pub fn record_rows(&self, table: &str, count: u64) {
        if let Some(p) = self.progress.write().get_mut(table) {
            p.rows_captured += count;
        }
    }

    /// Mark a table snapshot as completed
    pub fn complete_table(&self, table: &str) {
        if let Some(p) = self.progress.write().get_mut(table) {
            p.state = SnapshotState::Completed;
            p.completed_at = Some(Utc::now());
        }
    }

    /// Mark a table snapshot as failed
    pub fn fail_table(&self, table: &str, error: &str) {
        if let Some(p) = self.progress.write().get_mut(table) {
            p.state = SnapshotState::Failed;
            p.error = Some(error.to_string());
            p.completed_at = Some(Utc::now());
        }
    }

    /// Complete the overall snapshot
    pub fn complete_snapshot(&self) {
        let progress = self.progress.read();
        let any_failed = progress.values().any(|p| p.state == SnapshotState::Failed);
        *self.state.write() = if any_failed {
            SnapshotState::Failed
        } else {
            SnapshotState::Completed
        };
        info!(state = ?*self.state.read(), "Snapshot finished");
    }

    /// Get progress for a specific table
    pub fn get_progress(&self, table: &str) -> Option<SnapshotProgress> {
        self.progress.read().get(table).cloned()
    }

    /// Get progress for all tables
    pub fn all_progress(&self) -> Vec<SnapshotProgress> {
        self.progress.read().values().cloned().collect()
    }

    /// Get the overall snapshot state
    pub fn state(&self) -> SnapshotState {
        *self.state.read()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_snapshot_initial() {
        let mgr = SnapshotManager::new(SnapshotStrategy::Initial);
        assert!(mgr.should_snapshot(false));
        assert!(mgr.should_snapshot(true));
    }

    #[test]
    fn test_should_snapshot_when_needed() {
        let mgr = SnapshotManager::new(SnapshotStrategy::WhenNeeded);
        assert!(mgr.should_snapshot(false));
        assert!(!mgr.should_snapshot(true));
    }

    #[test]
    fn test_should_snapshot_never() {
        let mgr = SnapshotManager::new(SnapshotStrategy::Never);
        assert!(!mgr.should_snapshot(false));
        assert!(!mgr.should_snapshot(true));
    }

    #[test]
    fn test_snapshot_lifecycle() {
        let mgr = SnapshotManager::new(SnapshotStrategy::Initial);
        let tables = vec!["public.users".to_string(), "public.orders".to_string()];

        mgr.start_snapshot(&tables);
        assert_eq!(mgr.state(), SnapshotState::Running);

        mgr.start_table("public.users");
        mgr.record_rows("public.users", 500);
        mgr.complete_table("public.users");

        mgr.start_table("public.orders");
        mgr.record_rows("public.orders", 200);
        mgr.complete_table("public.orders");

        mgr.complete_snapshot();
        assert_eq!(mgr.state(), SnapshotState::Completed);

        let p = mgr.get_progress("public.users").unwrap();
        assert_eq!(p.rows_captured, 500);
        assert_eq!(p.state, SnapshotState::Completed);
    }

    #[test]
    fn test_snapshot_failure() {
        let mgr = SnapshotManager::new(SnapshotStrategy::Initial);
        mgr.start_snapshot(&["t1".to_string()]);
        mgr.start_table("t1");
        mgr.fail_table("t1", "connection lost");
        mgr.complete_snapshot();
        assert_eq!(mgr.state(), SnapshotState::Failed);
        assert_eq!(
            mgr.get_progress("t1").unwrap().error.as_deref(),
            Some("connection lost")
        );
    }

    #[test]
    fn test_percent_complete() {
        let mut p = SnapshotProgress::new("t1");
        assert!(p.percent_complete().is_none());

        p.total_rows_estimate = Some(1000);
        p.rows_captured = 250;
        assert!((p.percent_complete().unwrap() - 25.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_strategy_display() {
        assert_eq!(SnapshotStrategy::Initial.to_string(), "initial");
        assert_eq!(SnapshotStrategy::WhenNeeded.to_string(), "when_needed");
        assert_eq!(SnapshotStrategy::Never.to_string(), "never");
        assert_eq!(SnapshotStrategy::SchemaOnly.to_string(), "schema_only");
    }
}
