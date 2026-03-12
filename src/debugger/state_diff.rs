//! Consumer state diffing and point-in-time snapshots for the streaming debugger.

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

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
    pub fn offset_delta(&self) -> i64 { self.offset_after - self.offset_before }
    pub fn lag_delta(&self) -> i64 { self.lag_after - self.lag_before }
    pub fn is_regression(&self) -> bool { self.offset_after < self.offset_before }
}

/// Change type for a collection member.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MemberChangeType { Added, Removed, Unchanged }

/// A change in group membership.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemberChange {
    pub member_id: String,
    pub change_type: MemberChangeType,
}

/// A partition that was reassigned between group members.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionReassignment {
    pub topic: String,
    pub partition: i32,
    pub previous_owner: Option<String>,
    pub new_owner: Option<String>,
}

/// Diff for a single consumer group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupStateDiff {
    pub group_id: String,
    pub state_before: String,
    pub state_after: String,
    pub members_before: usize,
    pub members_after: usize,
    pub member_changes: Vec<MemberChange>,
    pub offset_diffs: Vec<OffsetDiff>,
    pub partition_reassignments: Vec<PartitionReassignment>,
}

impl GroupStateDiff {
    pub fn total_consumed(&self) -> i64 {
        self.offset_diffs.iter().map(|d| d.offset_delta()).filter(|d| *d > 0).sum()
    }
    pub fn state_changed(&self) -> bool { self.state_before != self.state_after }
    pub fn membership_changed(&self) -> bool { self.members_before != self.members_after }
    pub fn regression_count(&self) -> usize { self.offset_diffs.iter().filter(|d| d.is_regression()).count() }
    pub fn lag_changes(&self) -> Vec<(String, i32, i64)> {
        self.offset_diffs.iter().map(|d| (d.topic.clone(), d.partition, d.lag_delta())).collect()
    }
}

/// Snapshot of consumer state at a point in time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerStateSnapshot {
    pub timestamp_ms: i64,
    pub groups: HashMap<String, GroupSnapshot>,
}

/// Snapshot of a single group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupSnapshot {
    pub state: String,
    pub member_count: usize,
    #[serde(default)]
    pub members: Vec<String>,
    pub offsets: HashMap<String, (i64, i64)>,
    #[serde(default)]
    pub assignments: HashMap<String, String>,
}

/// Diff between two consumer state snapshots.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerStateDiff {
    pub before_timestamp_ms: i64,
    pub after_timestamp_ms: i64,
    pub group_diffs: Vec<GroupStateDiff>,
    pub groups_added: Vec<String>,
    pub groups_removed: Vec<String>,
}

impl ConsumerStateDiff {
    pub fn summary(&self) -> String {
        let total_consumed: i64 = self.group_diffs.iter().map(|g| g.total_consumed()).sum();
        let state_changes = self.group_diffs.iter().filter(|g| g.state_changed()).count();
        let regressions: usize = self.group_diffs.iter().map(|g| g.regression_count()).sum();
        format!(
            "{} groups diffed, {} messages consumed, {} state changes, {} regressions, {} added, {} removed",
            self.group_diffs.len(), total_consumed, state_changes, regressions,
            self.groups_added.len(), self.groups_removed.len(),
        )
    }
}

/// Snapshot of topic configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicConfigSnapshot {
    pub topic: String,
    pub partition_count: i32,
    pub replication_factor: i32,
    pub configs: HashMap<String, String>,
}

/// A single config value change.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigChange {
    pub key: String,
    pub value_before: Option<String>,
    pub value_after: Option<String>,
    pub change_type: ConfigChangeType,
}

/// Type of configuration change.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ConfigChangeType { Added, Removed, Modified }

/// Diff between two topic configurations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigDiff {
    pub topic: String,
    pub partition_count_before: i32,
    pub partition_count_after: i32,
    pub replication_factor_before: i32,
    pub replication_factor_after: i32,
    pub config_changes: Vec<ConfigChange>,
}

impl ConfigDiff {
    pub fn partitions_changed(&self) -> bool { self.partition_count_before != self.partition_count_after }
    pub fn replication_changed(&self) -> bool { self.replication_factor_before != self.replication_factor_after }
    pub fn has_config_changes(&self) -> bool { !self.config_changes.is_empty() }
}

/// A point-in-time snapshot of the entire debug state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DebugSnapshot {
    pub timestamp_ms: i64,
    pub label: String,
    pub consumer_state: ConsumerStateSnapshot,
    pub topic_configs: HashMap<String, TopicConfigSnapshot>,
    pub high_watermarks: HashMap<String, HashMap<i32, i64>>,
}

/// Diff between two full debug snapshots.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotDiff {
    pub before_label: String,
    pub after_label: String,
    pub elapsed_ms: i64,
    pub consumer_diff: ConsumerStateDiff,
    pub config_diffs: Vec<ConfigDiff>,
    pub watermark_changes: HashMap<String, HashMap<i32, (i64, i64)>>,
    pub topics_added: Vec<String>,
    pub topics_removed: Vec<String>,
}

impl SnapshotDiff {
    pub fn summary(&self) -> String {
        let watermark_advances: usize = self.watermark_changes.values()
            .flat_map(|m| m.values()).filter(|(before, after)| after > before).count();
        format!(
            "Snapshot diff ({} -> {}, {}ms): {}, {} config changes, {} watermark advances, {} topics added, {} topics removed",
            self.before_label, self.after_label, self.elapsed_ms, self.consumer_diff.summary(),
            self.config_diffs.iter().map(|d| d.config_changes.len()).sum::<usize>(),
            watermark_advances, self.topics_added.len(), self.topics_removed.len(),
        )
    }
}

/// Engine for computing state diffs between snapshots.
pub struct StateDiffEngine;

impl StateDiffEngine {
    pub fn diff(before: &ConsumerStateSnapshot, after: &ConsumerStateSnapshot) -> ConsumerStateDiff {
        let mut group_diffs = Vec::new();
        let mut groups_added = Vec::new();
        let mut groups_removed = Vec::new();
        for (group_id, after_snap) in &after.groups {
            if let Some(before_snap) = before.groups.get(group_id) {
                let offset_diffs = Self::compute_offset_diffs(before_snap, after_snap);
                let member_changes = Self::compute_member_changes(before_snap, after_snap);
                let partition_reassignments = Self::compute_partition_reassignments(before_snap, after_snap);
                group_diffs.push(GroupStateDiff {
                    group_id: group_id.clone(),
                    state_before: before_snap.state.clone(), state_after: after_snap.state.clone(),
                    members_before: before_snap.member_count, members_after: after_snap.member_count,
                    member_changes, offset_diffs, partition_reassignments,
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
            before_timestamp_ms: before.timestamp_ms, after_timestamp_ms: after.timestamp_ms,
            group_diffs, groups_added, groups_removed,
        }
    }

    fn compute_offset_diffs(before: &GroupSnapshot, after: &GroupSnapshot) -> Vec<OffsetDiff> {
        let all_keys: HashSet<&String> = before.offsets.keys().chain(after.offsets.keys()).collect();
        let mut diffs = Vec::new();
        for key in all_keys {
            let (topic, partition) = Self::parse_topic_partition(key);
            let (before_offset, before_hw) = before.offsets.get(key).copied().unwrap_or((0, 0));
            let (after_offset, after_hw) = after.offsets.get(key).copied().unwrap_or((0, 0));
            diffs.push(OffsetDiff {
                topic, partition, offset_before: before_offset, offset_after: after_offset,
                lag_before: before_hw - before_offset, lag_after: after_hw - after_offset,
            });
        }
        diffs
    }

    fn compute_member_changes(before: &GroupSnapshot, after: &GroupSnapshot) -> Vec<MemberChange> {
        let before_set: HashSet<&String> = before.members.iter().collect();
        let after_set: HashSet<&String> = after.members.iter().collect();
        let mut changes = Vec::new();
        for m in &after.members {
            if !before_set.contains(m) {
                changes.push(MemberChange { member_id: m.clone(), change_type: MemberChangeType::Added });
            }
        }
        for m in &before.members {
            if !after_set.contains(m) {
                changes.push(MemberChange { member_id: m.clone(), change_type: MemberChangeType::Removed });
            }
        }
        changes
    }

    fn compute_partition_reassignments(before: &GroupSnapshot, after: &GroupSnapshot) -> Vec<PartitionReassignment> {
        let all_keys: HashSet<&String> = before.assignments.keys().chain(after.assignments.keys()).collect();
        let mut reassignments = Vec::new();
        for key in all_keys {
            let prev = before.assignments.get(key);
            let next = after.assignments.get(key);
            if prev != next {
                let (topic, partition) = Self::parse_topic_partition(key);
                reassignments.push(PartitionReassignment {
                    topic, partition, previous_owner: prev.cloned(), new_owner: next.cloned(),
                });
            }
        }
        reassignments
    }

    pub fn diff_consumer_groups(before: &GroupSnapshot, after: &GroupSnapshot, group_id: &str) -> GroupStateDiff {
        let offset_diffs = Self::compute_offset_diffs(before, after);
        let member_changes = Self::compute_member_changes(before, after);
        let partition_reassignments = Self::compute_partition_reassignments(before, after);
        GroupStateDiff {
            group_id: group_id.to_string(),
            state_before: before.state.clone(), state_after: after.state.clone(),
            members_before: before.member_count, members_after: after.member_count,
            member_changes, offset_diffs, partition_reassignments,
        }
    }

    pub fn diff_topic_configs(before: &TopicConfigSnapshot, after: &TopicConfigSnapshot) -> ConfigDiff {
        let all_keys: HashSet<&String> = before.configs.keys().chain(after.configs.keys()).collect();
        let mut changes = Vec::new();
        for key in all_keys {
            match (before.configs.get(key), after.configs.get(key)) {
                (Some(vb), Some(va)) if vb != va => changes.push(ConfigChange {
                    key: key.to_string(), value_before: Some(vb.clone()),
                    value_after: Some(va.clone()), change_type: ConfigChangeType::Modified,
                }),
                (None, Some(va)) => changes.push(ConfigChange {
                    key: key.to_string(), value_before: None,
                    value_after: Some(va.clone()), change_type: ConfigChangeType::Added,
                }),
                (Some(vb), None) => changes.push(ConfigChange {
                    key: key.to_string(), value_before: Some(vb.clone()),
                    value_after: None, change_type: ConfigChangeType::Removed,
                }),
                _ => {}
            }
        }
        ConfigDiff {
            topic: before.topic.clone(),
            partition_count_before: before.partition_count, partition_count_after: after.partition_count,
            replication_factor_before: before.replication_factor, replication_factor_after: after.replication_factor,
            config_changes: changes,
        }
    }

    pub fn create_snapshot(
        label: impl Into<String>, timestamp_ms: i64, consumer_state: ConsumerStateSnapshot,
        topic_configs: HashMap<String, TopicConfigSnapshot>,
        high_watermarks: HashMap<String, HashMap<i32, i64>>,
    ) -> DebugSnapshot {
        DebugSnapshot { timestamp_ms, label: label.into(), consumer_state, topic_configs, high_watermarks }
    }

    pub fn compare_snapshots(before: &DebugSnapshot, after: &DebugSnapshot) -> SnapshotDiff {
        let consumer_diff = Self::diff(&before.consumer_state, &after.consumer_state);
        let before_topics: HashSet<&String> = before.topic_configs.keys().collect();
        let after_topics: HashSet<&String> = after.topic_configs.keys().collect();
        let topics_added: Vec<String> = after_topics.difference(&before_topics).map(|t| (*t).clone()).collect();
        let topics_removed: Vec<String> = before_topics.difference(&after_topics).map(|t| (*t).clone()).collect();
        let mut config_diffs = Vec::new();
        for topic in before_topics.intersection(&after_topics) {
            if let (Some(bc), Some(ac)) = (before.topic_configs.get(*topic), after.topic_configs.get(*topic)) {
                let diff = Self::diff_topic_configs(bc, ac);
                if diff.has_config_changes() || diff.partitions_changed() || diff.replication_changed() {
                    config_diffs.push(diff);
                }
            }
        }
        let all_hw_topics: HashSet<&String> = before.high_watermarks.keys().chain(after.high_watermarks.keys()).collect();
        let mut watermark_changes: HashMap<String, HashMap<i32, (i64, i64)>> = HashMap::new();
        for topic in all_hw_topics {
            let before_hw = before.high_watermarks.get(topic);
            let after_hw = after.high_watermarks.get(topic);
            let all_partitions: HashSet<i32> = before_hw.map(|m| m.keys().copied().collect::<HashSet<_>>()).unwrap_or_default()
                .union(&after_hw.map(|m| m.keys().copied().collect::<HashSet<_>>()).unwrap_or_default()).copied().collect();
            let mut partition_changes = HashMap::new();
            for p in all_partitions {
                let bw = before_hw.and_then(|m| m.get(&p)).copied().unwrap_or(0);
                let aw = after_hw.and_then(|m| m.get(&p)).copied().unwrap_or(0);
                if bw != aw { partition_changes.insert(p, (bw, aw)); }
            }
            if !partition_changes.is_empty() {
                watermark_changes.insert(topic.clone(), partition_changes);
            }
        }
        SnapshotDiff {
            before_label: before.label.clone(), after_label: after.label.clone(),
            elapsed_ms: after.timestamp_ms - before.timestamp_ms,
            consumer_diff, config_diffs, watermark_changes, topics_added, topics_removed,
        }
    }

    fn parse_topic_partition(key: &str) -> (String, i32) {
        if let Some(idx) = key.rfind(':') {
            let topic = key[..idx].to_string();
            let partition = key[idx + 1..].parse().unwrap_or(0);
            (topic, partition)
        } else { (key.to_string(), 0) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_group_snapshot(state: &str, members: Vec<&str>, offsets: Vec<(&str, i64, i64)>) -> GroupSnapshot {
        GroupSnapshot {
            state: state.to_string(), member_count: members.len(),
            members: members.iter().map(|s| s.to_string()).collect(),
            offsets: offsets.into_iter().map(|(k, o, hw)| (k.to_string(), (o, hw))).collect(),
            assignments: HashMap::new(),
        }
    }

    #[test]
    fn test_state_diff_basic() {
        let before = ConsumerStateSnapshot { timestamp_ms: 1000, groups: {
            let mut m = HashMap::new();
            m.insert("group-1".to_string(), make_group_snapshot("Stable", vec!["m1", "m2"],
                vec![("events:0", 100, 200), ("events:1", 50, 150)]));
            m
        }};
        let after = ConsumerStateSnapshot { timestamp_ms: 2000, groups: {
            let mut m = HashMap::new();
            m.insert("group-1".to_string(), make_group_snapshot("Stable", vec!["m1", "m2", "m3"],
                vec![("events:0", 150, 250), ("events:1", 100, 200)]));
            m
        }};
        let diff = StateDiffEngine::diff(&before, &after);
        assert_eq!(diff.group_diffs.len(), 1);
        let g = &diff.group_diffs[0];
        assert_eq!(g.total_consumed(), 100);
        assert!(g.membership_changed());
    }

    #[test]
    fn test_state_diff_groups_added_removed() {
        let before = ConsumerStateSnapshot { timestamp_ms: 1000, groups: {
            let mut m = HashMap::new();
            m.insert("old-group".to_string(), make_group_snapshot("Stable", vec!["m1"], vec![]));
            m
        }};
        let after = ConsumerStateSnapshot { timestamp_ms: 2000, groups: {
            let mut m = HashMap::new();
            m.insert("new-group".to_string(), make_group_snapshot("Stable", vec!["m1"], vec![]));
            m
        }};
        let diff = StateDiffEngine::diff(&before, &after);
        assert_eq!(diff.groups_added, vec!["new-group"]);
        assert_eq!(diff.groups_removed, vec!["old-group"]);
    }

    #[test]
    fn test_member_changes() {
        let before = make_group_snapshot("Stable", vec!["m1", "m2"], vec![]);
        let after = make_group_snapshot("Stable", vec!["m2", "m3"], vec![]);
        let diff = StateDiffEngine::diff_consumer_groups(&before, &after, "test-group");
        assert_eq!(diff.member_changes.len(), 2);
        assert!(diff.member_changes.iter().any(|c| c.member_id == "m3" && c.change_type == MemberChangeType::Added));
        assert!(diff.member_changes.iter().any(|c| c.member_id == "m1" && c.change_type == MemberChangeType::Removed));
    }

    #[test]
    fn test_offset_regression() {
        let before = make_group_snapshot("Stable", vec![], vec![("t:0", 100, 200)]);
        let after = make_group_snapshot("Stable", vec![], vec![("t:0", 80, 200)]);
        let diff = StateDiffEngine::diff_consumer_groups(&before, &after, "g1");
        assert_eq!(diff.regression_count(), 1);
        assert_eq!(diff.offset_diffs[0].offset_delta(), -20);
    }

    #[test]
    fn test_lag_changes() {
        let before = make_group_snapshot("Stable", vec![], vec![("t:0", 100, 200)]);
        let after = make_group_snapshot("Stable", vec![], vec![("t:0", 150, 300)]);
        let diff = StateDiffEngine::diff_consumer_groups(&before, &after, "g1");
        let lag = diff.lag_changes();
        assert_eq!(lag.len(), 1);
        assert_eq!(lag[0].2, 50);
    }

    #[test]
    fn test_partition_reassignments() {
        let mut before = make_group_snapshot("Stable", vec!["m1", "m2"], vec![]);
        before.assignments.insert("t:0".to_string(), "m1".to_string());
        before.assignments.insert("t:1".to_string(), "m2".to_string());
        let mut after = make_group_snapshot("Stable", vec!["m1", "m2"], vec![]);
        after.assignments.insert("t:0".to_string(), "m2".to_string());
        after.assignments.insert("t:1".to_string(), "m1".to_string());
        let diff = StateDiffEngine::diff_consumer_groups(&before, &after, "g1");
        assert_eq!(diff.partition_reassignments.len(), 2);
    }

    #[test]
    fn test_diff_topic_configs() {
        let before = TopicConfigSnapshot {
            topic: "events".to_string(), partition_count: 3, replication_factor: 1,
            configs: {
                let mut m = HashMap::new();
                m.insert("retention.ms".to_string(), "86400000".to_string());
                m.insert("max.message.bytes".to_string(), "1048576".to_string());
                m.insert("old.config".to_string(), "value".to_string());
                m
            },
        };
        let after = TopicConfigSnapshot {
            topic: "events".to_string(), partition_count: 6, replication_factor: 3,
            configs: {
                let mut m = HashMap::new();
                m.insert("retention.ms".to_string(), "172800000".to_string());
                m.insert("max.message.bytes".to_string(), "1048576".to_string());
                m.insert("new.config".to_string(), "new-value".to_string());
                m
            },
        };
        let diff = StateDiffEngine::diff_topic_configs(&before, &after);
        assert!(diff.partitions_changed());
        assert!(diff.replication_changed());
        assert!(diff.has_config_changes());
    }

    #[test]
    fn test_create_and_compare_snapshots() {
        let before = StateDiffEngine::create_snapshot("t1", 1000,
            ConsumerStateSnapshot { timestamp_ms: 1000, groups: {
                let mut m = HashMap::new();
                m.insert("g1".to_string(), make_group_snapshot("Stable", vec!["m1"], vec![("t:0", 10, 20)]));
                m
            }},
            {
                let mut m = HashMap::new();
                m.insert("events".to_string(), TopicConfigSnapshot {
                    topic: "events".to_string(), partition_count: 3, replication_factor: 1, configs: HashMap::new(),
                });
                m
            },
            {
                let mut m = HashMap::new();
                let mut p = HashMap::new();
                p.insert(0, 100i64);
                m.insert("events".to_string(), p);
                m
            },
        );
        let after = StateDiffEngine::create_snapshot("t2", 2000,
            ConsumerStateSnapshot { timestamp_ms: 2000, groups: {
                let mut m = HashMap::new();
                m.insert("g1".to_string(), make_group_snapshot("Stable", vec!["m1"], vec![("t:0", 18, 30)]));
                m
            }},
            {
                let mut m = HashMap::new();
                m.insert("events".to_string(), TopicConfigSnapshot {
                    topic: "events".to_string(), partition_count: 6, replication_factor: 1, configs: HashMap::new(),
                });
                m
            },
            {
                let mut m = HashMap::new();
                let mut p = HashMap::new();
                p.insert(0, 200i64);
                m.insert("events".to_string(), p);
                m
            },
        );
        let diff = StateDiffEngine::compare_snapshots(&before, &after);
        assert_eq!(diff.elapsed_ms, 1000);
        assert_eq!(diff.consumer_diff.group_diffs.len(), 1);
        assert_eq!(diff.config_diffs.len(), 1);
        assert!(diff.watermark_changes.contains_key("events"));
    }

    #[test]
    fn test_snapshot_diff_topics_added_removed() {
        let before = StateDiffEngine::create_snapshot("t1", 1000,
            ConsumerStateSnapshot { timestamp_ms: 1000, groups: HashMap::new() },
            {
                let mut m = HashMap::new();
                m.insert("old-topic".to_string(), TopicConfigSnapshot {
                    topic: "old-topic".to_string(), partition_count: 1, replication_factor: 1, configs: HashMap::new(),
                });
                m
            },
            HashMap::new(),
        );
        let after = StateDiffEngine::create_snapshot("t2", 2000,
            ConsumerStateSnapshot { timestamp_ms: 2000, groups: HashMap::new() },
            {
                let mut m = HashMap::new();
                m.insert("new-topic".to_string(), TopicConfigSnapshot {
                    topic: "new-topic".to_string(), partition_count: 1, replication_factor: 1, configs: HashMap::new(),
                });
                m
            },
            HashMap::new(),
        );
        let diff = StateDiffEngine::compare_snapshots(&before, &after);
        assert!(diff.topics_added.contains(&"new-topic".to_string()));
        assert!(diff.topics_removed.contains(&"old-topic".to_string()));
    }
}
