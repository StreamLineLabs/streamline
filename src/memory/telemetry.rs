//! Telemetry and metrics for agent memory (M1 P2).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, OnceLock};

fn lock_or_recover<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

use super::Tier;

/// Global memory-subsystem counters.
pub struct MemoryMetrics {
    pub recall_total: AtomicU64,
    pub recall_hits: AtomicU64,
    pub remember_total: AtomicU64,
    pub decay_total: AtomicU64,
    pub acl_denied: AtomicU64,
    agents: Mutex<HashMap<String, AgentMetrics>>,
}

/// Per-agent breakdown of memory operations.
#[derive(Debug, Default)]
pub struct AgentMetrics {
    pub recalls: u64,
    pub remembers: u64,
    pub acl_denials: u64,
}

impl MemoryMetrics {
    fn new() -> Self {
        Self {
            recall_total: AtomicU64::new(0),
            recall_hits: AtomicU64::new(0),
            remember_total: AtomicU64::new(0),
            decay_total: AtomicU64::new(0),
            acl_denied: AtomicU64::new(0),
            agents: Mutex::new(HashMap::new()),
        }
    }

    /// Get per-agent metrics (recalls, remembers) for a specific agent.
    pub fn agent_stats(&self, agent_id: &str) -> (u64, u64) {
        lock_or_recover(&self.agents)
            .get(agent_id)
            .map(|a| (a.recalls, a.remembers))
            .unwrap_or((0, 0))
    }
}

/// Returns the singleton `MemoryMetrics` instance.
pub fn get() -> &'static MemoryMetrics {
    static INSTANCE: OnceLock<MemoryMetrics> = OnceLock::new();
    INSTANCE.get_or_init(MemoryMetrics::new)
}

/// Record a recall operation for `agent_id` that returned `hit_count` results.
pub fn record_recall(agent_id: &str, hit_count: usize) {
    let m = get();
    m.recall_total.fetch_add(1, Ordering::Relaxed);
    m.recall_hits
        .fetch_add(hit_count as u64, Ordering::Relaxed);
    lock_or_recover(&m.agents)
        .entry(agent_id.to_string())
        .or_default()
        .recalls += 1;
}

/// Record a remember (write) for the given agent and tier.
pub fn record_remember(agent_id: &str, _tier: Tier) {
    let m = get();
    m.remember_total.fetch_add(1, Ordering::Relaxed);
    lock_or_recover(&m.agents)
        .entry(agent_id.to_string())
        .or_default()
        .remembers += 1;
}

/// Record an ACL denial for the given agent.
pub fn record_acl_denied(agent_id: &str) {
    let m = get();
    m.acl_denied.fetch_add(1, Ordering::Relaxed);
    lock_or_recover(&m.agents)
        .entry(agent_id.to_string())
        .or_default()
        .acl_denials += 1;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_recall_increments() {
        let before = get().recall_total.load(Ordering::Relaxed);
        record_recall("agent-1", 3);
        let after = get().recall_total.load(Ordering::Relaxed);
        assert!(after > before);
    }

    #[test]
    fn recall_hits_accumulated() {
        let before = get().recall_hits.load(Ordering::Relaxed);
        record_recall("agent-2", 5);
        record_recall("agent-2", 2);
        let after = get().recall_hits.load(Ordering::Relaxed);
        assert!(after >= before + 7);
    }

    #[test]
    fn record_remember_increments() {
        let before = get().remember_total.load(Ordering::Relaxed);
        record_remember("agent-3", Tier::Episodic);
        let after = get().remember_total.load(Ordering::Relaxed);
        assert_eq!(after, before + 1);
    }

    #[test]
    fn record_acl_denied_increments() {
        let before = get().acl_denied.load(Ordering::Relaxed);
        record_acl_denied("agent-4");
        let after = get().acl_denied.load(Ordering::Relaxed);
        assert_eq!(after, before + 1);
    }

    #[test]
    fn per_agent_metrics_tracked() {
        record_recall("tracked-agent", 1);
        record_remember("tracked-agent", Tier::Semantic);
        record_acl_denied("tracked-agent");

        let agents = get().agents.lock().unwrap();
        let a = agents.get("tracked-agent").unwrap();
        assert!(a.recalls >= 1);
        assert!(a.remembers >= 1);
        assert!(a.acl_denials >= 1);
    }
}
