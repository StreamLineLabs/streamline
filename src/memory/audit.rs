//! SOC2-compliant audit logging for memory operations (M1 P3).

use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

fn read_or_recover<T>(m: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    m.read().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn write_or_recover<T>(m: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    m.write().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Classification of memory operations for audit purposes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemoryOperation {
    Recall,
    Remember,
    Share,
    Forget,
    Delete,
    Decay,
}

impl std::fmt::Display for MemoryOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MemoryOperation::Recall => write!(f, "recall"),
            MemoryOperation::Remember => write!(f, "remember"),
            MemoryOperation::Share => write!(f, "share"),
            MemoryOperation::Forget => write!(f, "forget"),
            MemoryOperation::Delete => write!(f, "delete"),
            MemoryOperation::Decay => write!(f, "decay"),
        }
    }
}

/// A single auditable event within the memory subsystem.
#[derive(Debug, Clone)]
pub struct MemoryAuditEvent {
    /// Unix epoch seconds when the event occurred.
    pub timestamp: i64,
    /// The agent that triggered the operation.
    pub agent_id: String,
    /// Which operation was performed.
    pub operation: MemoryOperation,
    /// Structured details (topic, key, result counts, etc.).
    pub details: serde_json::Value,
    /// Tenant the agent belongs to.
    pub tenant_id: String,
}

/// Append-only audit log protected by a `RwLock` for concurrent access.
#[derive(Debug, Default)]
pub struct MemoryAuditLog {
    events: RwLock<Vec<MemoryAuditEvent>>,
}

impl MemoryAuditLog {
    /// Creates an empty audit log.
    pub fn new() -> Self {
        Self::default()
    }
}

/// Append an event to the audit log.
pub fn log_event(audit: &MemoryAuditLog, event: MemoryAuditEvent) {
    write_or_recover(&audit.events).push(event);
}

/// Retrieve events from the log, optionally filtered to those at or after
/// `since` (unix epoch seconds). Returns events in chronological order.
pub fn get_events(audit: &MemoryAuditLog, since: Option<i64>) -> Vec<MemoryAuditEvent> {
    let events = read_or_recover(&audit.events);
    match since {
        Some(ts) => events.iter().filter(|e| e.timestamp >= ts).cloned().collect(),
        None => events.clone(),
    }
}

fn now_epoch_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

/// Convenience constructor for a `MemoryAuditEvent` with the current timestamp.
pub fn create_event(
    agent_id: &str,
    operation: MemoryOperation,
    details: serde_json::Value,
    tenant_id: &str,
) -> MemoryAuditEvent {
    MemoryAuditEvent {
        timestamp: now_epoch_secs(),
        agent_id: agent_id.to_string(),
        operation,
        details,
        tenant_id: tenant_id.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_event(ts: i64, op: MemoryOperation) -> MemoryAuditEvent {
        MemoryAuditEvent {
            timestamp: ts,
            agent_id: "agent-1".into(),
            operation: op,
            details: serde_json::json!({"topic": "__mem.acme.agent-1.episodic"}),
            tenant_id: "acme".into(),
        }
    }

    #[test]
    fn log_and_retrieve_events() {
        let log = MemoryAuditLog::new();
        log_event(&log, sample_event(100, MemoryOperation::Recall));
        log_event(&log, sample_event(200, MemoryOperation::Remember));

        let all = get_events(&log, None);
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].operation, MemoryOperation::Recall);
        assert_eq!(all[1].operation, MemoryOperation::Remember);
    }

    #[test]
    fn filter_events_by_since() {
        let log = MemoryAuditLog::new();
        log_event(&log, sample_event(100, MemoryOperation::Recall));
        log_event(&log, sample_event(200, MemoryOperation::Remember));
        log_event(&log, sample_event(300, MemoryOperation::Share));

        let filtered = get_events(&log, Some(200));
        assert_eq!(filtered.len(), 2);
        assert_eq!(filtered[0].timestamp, 200);
        assert_eq!(filtered[1].timestamp, 300);
    }

    #[test]
    fn empty_log_returns_empty() {
        let log = MemoryAuditLog::new();
        assert!(get_events(&log, None).is_empty());
        assert!(get_events(&log, Some(0)).is_empty());
    }

    #[test]
    fn operation_display() {
        assert_eq!(MemoryOperation::Recall.to_string(), "recall");
        assert_eq!(MemoryOperation::Delete.to_string(), "delete");
        assert_eq!(MemoryOperation::Decay.to_string(), "decay");
    }

    #[test]
    fn create_event_sets_timestamp() {
        let e = create_event("a1", MemoryOperation::Forget, serde_json::json!({}), "t1");
        assert!(e.timestamp > 0);
        assert_eq!(e.agent_id, "a1");
        assert_eq!(e.tenant_id, "t1");
    }

    #[test]
    fn append_only_ordering_preserved() {
        let log = MemoryAuditLog::new();
        for i in 0..10 {
            log_event(&log, sample_event(i, MemoryOperation::Remember));
        }
        let all = get_events(&log, None);
        for (idx, e) in all.iter().enumerate() {
            assert_eq!(e.timestamp, idx as i64);
        }
    }
}
