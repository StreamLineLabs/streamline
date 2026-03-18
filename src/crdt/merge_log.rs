//! Merge observability for CRDT operations (M3 P2).
//!
//! Records every merge decision for auditability.

use std::collections::VecDeque;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

fn read_or_recover<T>(m: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    m.read().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn write_or_recover<T>(m: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    m.write().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Source node information for a value that participated in a merge.
#[derive(Debug, Clone)]
pub struct NodeSource {
    pub node_id: String,
    pub timestamp_ms: i64,
}

/// A recorded merge decision.
#[derive(Debug, Clone)]
pub struct MergeEvent {
    /// Topic where the merge occurred.
    pub topic: String,
    /// Record key that was merged.
    pub key: String,
    /// CRDT type involved (e.g. "lww_register", "g_counter").
    pub crdt_type: String,
    /// String representation of the value that was chosen.
    pub chosen_value: String,
    /// String representations of values that were discarded.
    pub discarded_values: Vec<String>,
    /// Unix epoch milliseconds when the merge was recorded.
    pub merge_timestamp: i64,
    /// The nodes whose values participated in the merge.
    pub sources: Vec<NodeSource>,
}

/// Default ring buffer capacity for the merge log.
const DEFAULT_CAPACITY: usize = 4096;

/// A bounded ring-buffer of `MergeEvent`s for observability.
///
/// When the buffer is full, the oldest event is evicted.
#[derive(Debug)]
pub struct MergeLog {
    events: RwLock<VecDeque<MergeEvent>>,
    capacity: usize,
}

impl MergeLog {
    /// Creates a new merge log with the given ring-buffer capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            events: RwLock::new(VecDeque::with_capacity(capacity)),
            capacity,
        }
    }

    /// Returns the configured capacity of the ring buffer.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns the current number of events stored.
    pub fn len(&self) -> usize {
        read_or_recover(&self.events).len()
    }

    /// Returns `true` when the log contains no events.
    pub fn is_empty(&self) -> bool {
        read_or_recover(&self.events).is_empty()
    }
}

impl Default for MergeLog {
    fn default() -> Self {
        Self::new(DEFAULT_CAPACITY)
    }
}

/// Record a merge event, evicting the oldest if the buffer is at capacity.
pub fn record_merge(log: &MergeLog, event: MergeEvent) {
    let mut events = write_or_recover(&log.events);
    if events.len() >= log.capacity {
        events.pop_front();
    }
    events.push_back(event);
}

/// Return the most recent `limit` merge events (newest last).
pub fn get_recent(log: &MergeLog, limit: usize) -> Vec<MergeEvent> {
    let events = read_or_recover(&log.events);
    let skip = events.len().saturating_sub(limit);
    events.iter().skip(skip).cloned().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn evt(topic: &str, key: &str, ts: i64) -> MergeEvent {
        MergeEvent {
            topic: topic.into(),
            key: key.into(),
            crdt_type: "lww_register".into(),
            chosen_value: "winner".into(),
            discarded_values: vec!["loser".into()],
            merge_timestamp: ts,
            sources: vec![
                NodeSource {
                    node_id: "n1".into(),
                    timestamp_ms: ts,
                },
                NodeSource {
                    node_id: "n2".into(),
                    timestamp_ms: ts - 10,
                },
            ],
        }
    }

    #[test]
    fn record_and_retrieve() {
        let log = MergeLog::default();
        record_merge(&log, evt("t1", "k1", 100));
        record_merge(&log, evt("t1", "k2", 200));

        let recent = get_recent(&log, 10);
        assert_eq!(recent.len(), 2);
        assert_eq!(recent[0].key, "k1");
        assert_eq!(recent[1].key, "k2");
    }

    #[test]
    fn limit_caps_returned_events() {
        let log = MergeLog::default();
        for i in 0..10 {
            record_merge(&log, evt("t1", &format!("k{}", i), i));
        }
        let recent = get_recent(&log, 3);
        assert_eq!(recent.len(), 3);
        assert_eq!(recent[0].key, "k7");
        assert_eq!(recent[1].key, "k8");
        assert_eq!(recent[2].key, "k9");
    }

    #[test]
    fn ring_buffer_evicts_oldest() {
        let log = MergeLog::new(3);
        record_merge(&log, evt("t", "a", 1));
        record_merge(&log, evt("t", "b", 2));
        record_merge(&log, evt("t", "c", 3));
        record_merge(&log, evt("t", "d", 4));

        assert_eq!(log.len(), 3);
        let all = get_recent(&log, 10);
        assert_eq!(all[0].key, "b");
        assert_eq!(all[2].key, "d");
    }

    #[test]
    fn empty_log() {
        let log = MergeLog::default();
        assert!(log.is_empty());
        assert_eq!(get_recent(&log, 10).len(), 0);
    }

    #[test]
    fn default_capacity() {
        let log = MergeLog::default();
        assert_eq!(log.capacity(), 4096);
    }

    #[test]
    fn merge_event_sources_preserved() {
        let log = MergeLog::default();
        let e = evt("t1", "k1", 500);
        record_merge(&log, e);

        let recent = get_recent(&log, 1);
        assert_eq!(recent[0].sources.len(), 2);
        assert_eq!(recent[0].sources[0].node_id, "n1");
    }
}
