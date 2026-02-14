//! Consumer state diffing for the streaming debugger.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Difference in a single partition offset.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OffsetDiff {
    pub topic: String,
    pub partition: i32,
    pub offset_before: i64,
    pub offset_after: i64,
    pub lag_before: i64,
    pub lag_after: i64,
}

impl OffsetDiff {
    /// Returns the delta in offsets (positive = forward progress).
    pub fn offset_delta(&self) -> i64 {
        self.offset_after - self.offset_before
    }

    /// Returns the delta in lag (negative = catching up).
    pub fn lag_delta(&self) -> i64 {
        self.lag_after - self.lag_before
    }
}

/// Diff for a single consumer group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupStateDiff {
    /// Consumer group ID
    pub group_id: String,
    /// State before (e.g., "Stable", "Rebalancing")
    pub state_before: String,
    /// State after
    pub state_after: String,
    /// Member count before
    pub members_before: usize,
    /// Member count after
    pub members_after: usize,
    /// Per-partition offset diffs
    pub offset_diffs: Vec<OffsetDiff>,
}

impl GroupStateDiff {
    /// Total messages consumed across all partitions.
    pub fn total_consumed(&self) -> i64 {
        self.offset_diffs
            .iter()
            .map(|d| d.offset_delta())
            .filter(|d| *d > 0)
            .sum()
    }

    /// Whether the group state changed.
    pub fn state_changed(&self) -> bool {
        self.state_before != self.state_after
    }

    /// Whether group membership changed.
    pub fn membership_changed(&self) -> bool {
        self.members_before != self.members_after
    }
}

/// Snapshot of consumer state at a point in time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerStateSnapshot {
    /// Timestamp of the snapshot
    pub timestamp_ms: i64,
    /// Group states: group_id -> (state, member_count, offsets)
    pub groups: HashMap<String, GroupSnapshot>,
}

/// Snapshot of a single group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupSnapshot {
    pub state: String,
    pub member_count: usize,
    /// topic:partition -> (committed_offset, high_watermark)
    pub offsets: HashMap<String, (i64, i64)>,
}

/// Diff between two consumer state snapshots.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerStateDiff {
    /// Timestamp of the before snapshot
    pub before_timestamp_ms: i64,
    /// Timestamp of the after snapshot
    pub after_timestamp_ms: i64,
    /// Per-group diffs
    pub group_diffs: Vec<GroupStateDiff>,
    /// Groups added in the after snapshot
    pub groups_added: Vec<String>,
    /// Groups removed in the after snapshot
    pub groups_removed: Vec<String>,
}

impl ConsumerStateDiff {
    /// Summary of the diff.
    pub fn summary(&self) -> String {
        let total_consumed: i64 = self.group_diffs.iter().map(|g| g.total_consumed()).sum();
        let state_changes = self
            .group_diffs
            .iter()
            .filter(|g| g.state_changed())
            .count();
        format!(
            "{} groups diffed, {} messages consumed, {} state changes, {} added, {} removed",
            self.group_diffs.len(),
            total_consumed,
            state_changes,
            self.groups_added.len(),
            self.groups_removed.len(),
        )
    }
}

/// Engine for computing state diffs between snapshots.
pub struct StateDiffEngine;

impl StateDiffEngine {
    /// Compute the diff between two consumer state snapshots.
    pub fn diff(
        before: &ConsumerStateSnapshot,
        after: &ConsumerStateSnapshot,
    ) -> ConsumerStateDiff {
        let mut group_diffs = Vec::new();
        let mut groups_added = Vec::new();
        let mut groups_removed = Vec::new();

        // Find diffs for groups present in both
        for (group_id, after_snap) in &after.groups {
            if let Some(before_snap) = before.groups.get(group_id) {
                let mut offset_diffs = Vec::new();

                // Compare offsets across all partitions
                let all_keys: std::collections::HashSet<&String> = before_snap
                    .offsets
                    .keys()
                    .chain(after_snap.offsets.keys())
                    .collect();

                for key in all_keys {
                    let (topic, partition) = Self::parse_topic_partition(key);
                    let (before_offset, before_hw) =
                        before_snap.offsets.get(key).copied().unwrap_or((0, 0));
                    let (after_offset, after_hw) =
                        after_snap.offsets.get(key).copied().unwrap_or((0, 0));

                    offset_diffs.push(OffsetDiff {
                        topic,
                        partition,
                        offset_before: before_offset,
                        offset_after: after_offset,
                        lag_before: before_hw - before_offset,
                        lag_after: after_hw - after_offset,
                    });
                }

                group_diffs.push(GroupStateDiff {
                    group_id: group_id.clone(),
                    state_before: before_snap.state.clone(),
                    state_after: after_snap.state.clone(),
                    members_before: before_snap.member_count,
                    members_after: after_snap.member_count,
                    offset_diffs,
                });
            } else {
                groups_added.push(group_id.clone());
            }
        }

        for group_id in before.groups.keys() {
            if !after.groups.contains_key(group_id) {
                groups_removed.push(group_id.clone());
            }
        }

        ConsumerStateDiff {
            before_timestamp_ms: before.timestamp_ms,
            after_timestamp_ms: after.timestamp_ms,
            group_diffs,
            groups_added,
            groups_removed,
        }
    }

    fn parse_topic_partition(key: &str) -> (String, i32) {
        if let Some(idx) = key.rfind(':') {
            let topic = key[..idx].to_string();
            let partition = key[idx + 1..].parse().unwrap_or(0);
            (topic, partition)
        } else {
            (key.to_string(), 0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_state_diff_basic() {
        let before = ConsumerStateSnapshot {
            timestamp_ms: 1000,
            groups: {
                let mut m = HashMap::new();
                m.insert(
                    "group-1".to_string(),
                    GroupSnapshot {
                        state: "Stable".to_string(),
                        member_count: 2,
                        offsets: {
                            let mut o = HashMap::new();
                            o.insert("events:0".to_string(), (100, 200));
                            o.insert("events:1".to_string(), (50, 150));
                            o
                        },
                    },
                );
                m
            },
        };

        let after = ConsumerStateSnapshot {
            timestamp_ms: 2000,
            groups: {
                let mut m = HashMap::new();
                m.insert(
                    "group-1".to_string(),
                    GroupSnapshot {
                        state: "Stable".to_string(),
                        member_count: 3,
                        offsets: {
                            let mut o = HashMap::new();
                            o.insert("events:0".to_string(), (150, 250));
                            o.insert("events:1".to_string(), (100, 200));
                            o
                        },
                    },
                );
                m
            },
        };

        let diff = StateDiffEngine::diff(&before, &after);
        assert_eq!(diff.group_diffs.len(), 1);

        let g = &diff.group_diffs[0];
        assert_eq!(g.total_consumed(), 100); // 50 + 50
        assert!(g.membership_changed());
        assert!(!g.state_changed());
    }

    #[test]
    fn test_state_diff_groups_added_removed() {
        let before = ConsumerStateSnapshot {
            timestamp_ms: 1000,
            groups: {
                let mut m = HashMap::new();
                m.insert(
                    "old-group".to_string(),
                    GroupSnapshot {
                        state: "Stable".to_string(),
                        member_count: 1,
                        offsets: HashMap::new(),
                    },
                );
                m
            },
        };

        let after = ConsumerStateSnapshot {
            timestamp_ms: 2000,
            groups: {
                let mut m = HashMap::new();
                m.insert(
                    "new-group".to_string(),
                    GroupSnapshot {
                        state: "Stable".to_string(),
                        member_count: 1,
                        offsets: HashMap::new(),
                    },
                );
                m
            },
        };

        let diff = StateDiffEngine::diff(&before, &after);
        assert_eq!(diff.groups_added, vec!["new-group"]);
        assert_eq!(diff.groups_removed, vec!["old-group"]);
    }
}
