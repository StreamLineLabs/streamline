//! Branch lineage: every branch creation is a lineage event (M5 P3).
//!
//! Lineage events form an audit trail that tracks when, why, and by whom
//! each branch was created. This enables governance, compliance, and
//! data-mesh lineage graphs.

use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::Mutex;

/// A lineage event emitted when a branch is created or discarded.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BranchLineageEvent {
    /// Name of the branch that was created.
    pub branch_name: String,
    /// The base topic the branch was forked from.
    pub base_topic: String,
    /// Per-partition base offsets at branch creation.
    pub base_offsets: Vec<i64>,
    /// User or service that created the branch.
    pub created_by: String,
    /// When the branch was created (epoch milliseconds).
    pub created_at: u64,
}

impl BranchLineageEvent {
    /// Creates a lineage event from branch metadata.
    pub fn from_meta(meta: &super::metadata::BranchMeta) -> Self {
        Self {
            branch_name: meta.id.0.clone(),
            base_topic: meta.base_topic.clone(),
            base_offsets: meta.base_offsets.clone(),
            created_by: meta.created_by.clone(),
            created_at: meta.created_at_ms,
        }
    }
}

/// In-memory lineage log for branch events.
///
/// In a production deployment this would write to a system topic
/// (`__branch_lineage`) or an external lineage service (e.g., OpenLineage).
/// This in-memory implementation is suitable for testing and single-node
/// deployments.
#[derive(Debug)]
pub struct LineageLog {
    events: Mutex<VecDeque<BranchLineageEvent>>,
    capacity: usize,
}

impl LineageLog {
    /// Creates a new lineage log with the given capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            events: Mutex::new(VecDeque::with_capacity(capacity)),
            capacity,
        }
    }

    /// Records a branch creation event.
    pub fn record_branch_creation(&self, event: &BranchLineageEvent) {
        let mut events = self.events.lock().unwrap_or_else(|e| e.into_inner());
        if events.len() >= self.capacity {
            events.pop_front();
        }
        events.push_back(event.clone());
    }

    /// Returns all recorded lineage events (oldest first).
    pub fn events(&self) -> Vec<BranchLineageEvent> {
        self.events
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .cloned()
            .collect()
    }

    /// Returns the number of recorded events.
    pub fn len(&self) -> usize {
        self.events.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    /// Returns true if no events have been recorded.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for LineageLog {
    fn default() -> Self {
        Self::new(10_000)
    }
}

/// Convenience function: record a branch creation event.
///
/// Wraps `LineageLog::record_branch_creation` for callers that have
/// a reference to the global log.
pub fn record_branch_creation(log: &LineageLog, event: &BranchLineageEvent) {
    log.record_branch_creation(event);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_event() -> BranchLineageEvent {
        BranchLineageEvent {
            branch_name: "orders:experiment-a".into(),
            base_topic: "orders".into(),
            base_offsets: vec![10, 20],
            created_by: "alice".into(),
            created_at: 1700000000000,
        }
    }

    #[test]
    fn record_and_retrieve() {
        let log = LineageLog::default();
        let evt = sample_event();
        log.record_branch_creation(&evt);

        assert_eq!(log.len(), 1);
        let events = log.events();
        assert_eq!(events[0].branch_name, "orders:experiment-a");
        assert_eq!(events[0].base_topic, "orders");
    }

    #[test]
    fn capacity_eviction() {
        let log = LineageLog::new(2);
        let mut e1 = sample_event();
        e1.branch_name = "a".into();
        let mut e2 = sample_event();
        e2.branch_name = "b".into();
        let mut e3 = sample_event();
        e3.branch_name = "c".into();

        log.record_branch_creation(&e1);
        log.record_branch_creation(&e2);
        log.record_branch_creation(&e3);

        assert_eq!(log.len(), 2);
        let events = log.events();
        assert_eq!(events[0].branch_name, "b");
        assert_eq!(events[1].branch_name, "c");
    }

    #[test]
    fn from_meta() {
        let meta =
            crate::branches::metadata::BranchMeta::new("orders", "exp-a", vec![5, 10], "bob");
        let evt = BranchLineageEvent::from_meta(&meta);
        assert_eq!(evt.branch_name, "orders:exp-a");
        assert_eq!(evt.base_topic, "orders");
        assert_eq!(evt.created_by, "bob");
    }

    #[test]
    fn convenience_function() {
        let log = LineageLog::default();
        record_branch_creation(&log, &sample_event());
        assert_eq!(log.len(), 1);
    }

    #[test]
    fn empty_log() {
        let log = LineageLog::default();
        assert!(log.is_empty());
        assert_eq!(log.len(), 0);
        assert!(log.events().is_empty());
    }
}
