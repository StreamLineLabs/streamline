//! KIP-848 Assignment Types
//!
//! Defines target and current assignment structures for the new consumer protocol.

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// Topic-partitions organized by topic
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TopicPartitions {
    /// Map from topic name to partition indices
    partitions: HashMap<String, HashSet<i32>>,
}

impl TopicPartitions {
    /// Create empty topic partitions
    pub fn empty() -> Self {
        Self::default()
    }

    /// Create from a list of (topic, partition) pairs
    pub fn from_partitions(partitions: Vec<(String, i32)>) -> Self {
        let mut tp = Self::empty();
        for (topic, partition) in partitions {
            tp.add((topic, partition));
        }
        tp
    }

    /// Add a partition
    pub fn add(&mut self, tp: (String, i32)) {
        self.partitions.entry(tp.0).or_default().insert(tp.1);
    }

    /// Remove a partition
    pub fn remove(&mut self, tp: &(String, i32)) {
        if let Some(parts) = self.partitions.get_mut(&tp.0) {
            parts.remove(&tp.1);
            if parts.is_empty() {
                self.partitions.remove(&tp.0);
            }
        }
    }

    /// Check if contains a partition
    pub fn contains(&self, tp: &(String, i32)) -> bool {
        self.partitions
            .get(&tp.0)
            .map(|parts| parts.contains(&tp.1))
            .unwrap_or(false)
    }

    /// Get all topics
    pub fn topics(&self) -> impl Iterator<Item = &String> {
        self.partitions.keys()
    }

    /// Get partitions for a topic
    pub fn partitions_for_topic(&self, topic: &str) -> Option<&HashSet<i32>> {
        self.partitions.get(topic)
    }

    /// Convert to a flat set of (topic, partition) pairs
    pub fn as_set(&self) -> HashSet<(String, i32)> {
        let mut set = HashSet::new();
        for (topic, parts) in &self.partitions {
            for part in parts {
                set.insert((topic.clone(), *part));
            }
        }
        set
    }

    /// Convert to a flat list of (topic, partition) pairs
    pub fn as_list(&self) -> Vec<(String, i32)> {
        let mut list = Vec::new();
        for (topic, parts) in &self.partitions {
            for part in parts {
                list.push((topic.clone(), *part));
            }
        }
        list
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.partitions.is_empty()
    }

    /// Get total partition count
    pub fn len(&self) -> usize {
        self.partitions.values().map(|s| s.len()).sum()
    }
}

/// Target assignment computed by the server
/// This is the desired state the member should converge to
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TargetAssignment {
    /// Assignment epoch - incremented when assignment changes
    pub epoch: i32,

    /// Assigned topic partitions
    pub partitions: TopicPartitions,
}

impl TargetAssignment {
    /// Create empty target assignment
    pub fn empty() -> Self {
        Self::default()
    }

    /// Create from a list of partitions
    pub fn from_partitions(partitions: Vec<(String, i32)>) -> Self {
        Self {
            epoch: 1,
            partitions: TopicPartitions::from_partitions(partitions),
        }
    }

    /// Create with specific epoch
    pub fn with_epoch(epoch: i32, partitions: Vec<(String, i32)>) -> Self {
        Self {
            epoch,
            partitions: TopicPartitions::from_partitions(partitions),
        }
    }

    /// Convert to set
    pub fn as_set(&self) -> HashSet<(String, i32)> {
        self.partitions.as_set()
    }

    /// Get partition count
    pub fn partition_count(&self) -> usize {
        self.partitions.len()
    }
}

/// Current assignment owned by a member
/// This is the actual state the member reports
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CurrentAssignment {
    /// Assigned topic partitions
    pub partitions: TopicPartitions,
}

impl CurrentAssignment {
    /// Create empty current assignment
    pub fn empty() -> Self {
        Self::default()
    }

    /// Create from a list of partitions
    pub fn from_partitions(partitions: Vec<(String, i32)>) -> Self {
        Self {
            partitions: TopicPartitions::from_partitions(partitions),
        }
    }

    /// Add a partition
    pub fn add(&mut self, tp: (String, i32)) {
        self.partitions.add(tp);
    }

    /// Remove a partition
    pub fn remove(&mut self, tp: &(String, i32)) {
        self.partitions.remove(tp);
    }

    /// Convert to set
    pub fn as_set(&self) -> HashSet<(String, i32)> {
        self.partitions.as_set()
    }

    /// Get partition count
    pub fn partition_count(&self) -> usize {
        self.partitions.len()
    }
}

/// Delta between two assignments for incremental reconciliation
#[derive(Debug, Clone)]
pub struct AssignmentDelta {
    /// Partitions to revoke (in current but not target)
    pub to_revoke: Vec<(String, i32)>,

    /// Partitions to assign (in target but not current)
    pub to_assign: Vec<(String, i32)>,
}

impl AssignmentDelta {
    /// Compute delta between current and target assignments
    pub fn compute(current: &CurrentAssignment, target: &TargetAssignment) -> Self {
        let current_set = current.as_set();
        let target_set = target.as_set();

        let to_revoke: Vec<_> = current_set.difference(&target_set).cloned().collect();
        let to_assign: Vec<_> = target_set.difference(&current_set).cloned().collect();

        Self {
            to_revoke,
            to_assign,
        }
    }

    /// Check if there are any changes
    pub fn is_empty(&self) -> bool {
        self.to_revoke.is_empty() && self.to_assign.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_partitions() {
        let mut tp = TopicPartitions::empty();
        tp.add(("topic-1".to_string(), 0));
        tp.add(("topic-1".to_string(), 1));
        tp.add(("topic-2".to_string(), 0));

        assert_eq!(tp.len(), 3);
        assert!(tp.contains(&("topic-1".to_string(), 0)));
        assert!(!tp.contains(&("topic-1".to_string(), 2)));

        tp.remove(&("topic-1".to_string(), 0));
        assert_eq!(tp.len(), 2);
        assert!(!tp.contains(&("topic-1".to_string(), 0)));
    }

    #[test]
    fn test_assignment_delta() {
        let current = CurrentAssignment::from_partitions(vec![
            ("topic-1".to_string(), 0),
            ("topic-1".to_string(), 1),
        ]);

        let target = TargetAssignment::from_partitions(vec![
            ("topic-1".to_string(), 0),
            ("topic-1".to_string(), 2),
        ]);

        let delta = AssignmentDelta::compute(&current, &target);

        assert_eq!(delta.to_revoke.len(), 1);
        assert!(delta.to_revoke.contains(&("topic-1".to_string(), 1)));

        assert_eq!(delta.to_assign.len(), 1);
        assert!(delta.to_assign.contains(&("topic-1".to_string(), 2)));
    }
}
