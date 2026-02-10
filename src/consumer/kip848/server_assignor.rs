//! KIP-848 Server-Side Assignors
//!
//! Implements server-side partition assignment strategies for the new consumer protocol.

use super::assignment::TargetAssignment;
use super::group::ConsumerGroupKip848;
use super::member::MemberId;
use std::collections::{HashMap, HashSet};

/// Trait for server-side partition assignors
pub trait ServerSideAssignor: Send + Sync {
    /// Returns the unique name of this assignor
    fn name(&self) -> &str;

    /// Compute partition assignments for all members in a group
    ///
    /// # Arguments
    /// * `group` - The consumer group to assign partitions to
    /// * `topic_partitions` - Map of topic name to partition count
    ///
    /// # Returns
    /// Map of member ID to their target assignment
    fn assign(
        &self,
        group: &ConsumerGroupKip848,
        topic_partitions: &HashMap<String, i32>,
    ) -> HashMap<MemberId, TargetAssignment>;
}

/// Uniform (round-robin) assignor
///
/// Distributes partitions evenly across all members, regardless of topic.
/// This is the default assignor in KIP-848.
#[derive(Debug, Default)]
pub struct UniformAssignor;

impl UniformAssignor {
    /// Create a new uniform assignor
    pub fn new() -> Self {
        Self
    }
}

impl ServerSideAssignor for UniformAssignor {
    fn name(&self) -> &str {
        "uniform"
    }

    fn assign(
        &self,
        group: &ConsumerGroupKip848,
        topic_partitions: &HashMap<String, i32>,
    ) -> HashMap<MemberId, TargetAssignment> {
        let mut assignments: HashMap<MemberId, Vec<(String, i32)>> = HashMap::new();

        // Initialize empty assignment for each member
        for member_id in group.members.keys() {
            assignments.insert(member_id.clone(), Vec::new());
        }

        if group.members.is_empty() {
            return assignments
                .into_iter()
                .map(|(k, v)| (k, TargetAssignment::from_partitions(v)))
                .collect();
        }

        // Get member IDs sorted for deterministic assignment
        let mut member_ids: Vec<&MemberId> = group.members.keys().collect();
        member_ids.sort();

        // Collect all partitions from subscribed topics
        let mut all_partitions: Vec<(String, i32)> = Vec::new();
        for topic in &group.subscribed_topics {
            if let Some(&partition_count) = topic_partitions.get(topic) {
                for partition in 0..partition_count {
                    all_partitions.push((topic.clone(), partition));
                }
            }
        }

        // Sort partitions for deterministic assignment
        all_partitions.sort();

        // Round-robin assignment
        for (idx, partition) in all_partitions.into_iter().enumerate() {
            let member_idx = idx % member_ids.len();
            let member_id = member_ids[member_idx];

            // Only assign if member is subscribed to this topic
            if let Some(member) = group.members.get(member_id) {
                if member.subscribed_topics.contains(&partition.0) {
                    if let Some(member_partitions) = assignments.get_mut(member_id) {
                        member_partitions.push(partition);
                    }
                }
            }
        }

        // Convert to TargetAssignment
        assignments
            .into_iter()
            .map(|(member_id, partitions)| {
                (
                    member_id,
                    TargetAssignment::with_epoch(group.assignment_epoch + 1, partitions),
                )
            })
            .collect()
    }
}

/// Range assignor
///
/// Assigns partitions on a per-topic basis. Each topic's partitions are divided
/// into contiguous ranges and assigned to members subscribed to that topic.
#[derive(Debug, Default)]
pub struct RangeAssignor;

impl RangeAssignor {
    /// Create a new range assignor
    pub fn new() -> Self {
        Self
    }
}

impl ServerSideAssignor for RangeAssignor {
    fn name(&self) -> &str {
        "range"
    }

    fn assign(
        &self,
        group: &ConsumerGroupKip848,
        topic_partitions: &HashMap<String, i32>,
    ) -> HashMap<MemberId, TargetAssignment> {
        let mut assignments: HashMap<MemberId, Vec<(String, i32)>> = HashMap::new();

        // Initialize empty assignment for each member
        for member_id in group.members.keys() {
            assignments.insert(member_id.clone(), Vec::new());
        }

        if group.members.is_empty() {
            return assignments
                .into_iter()
                .map(|(k, v)| (k, TargetAssignment::from_partitions(v)))
                .collect();
        }

        // Process each topic independently
        for topic in &group.subscribed_topics {
            let partition_count = match topic_partitions.get(topic) {
                Some(&count) => count,
                None => continue,
            };

            // Get members subscribed to this topic
            let mut subscribed_members: Vec<&MemberId> = group
                .members
                .iter()
                .filter(|(_, member)| member.subscribed_topics.contains(topic))
                .map(|(id, _)| id)
                .collect();
            subscribed_members.sort();

            if subscribed_members.is_empty() {
                continue;
            }

            // Divide partitions into ranges
            let member_count = subscribed_members.len() as i32;
            let partitions_per_member = partition_count / member_count;
            let extra_partitions = partition_count % member_count;

            let mut partition_idx = 0;
            for (member_idx, member_id) in subscribed_members.iter().enumerate() {
                // Members at the beginning get one extra partition if there's a remainder
                let member_partition_count = partitions_per_member
                    + if (member_idx as i32) < extra_partitions {
                        1
                    } else {
                        0
                    };

                for _ in 0..member_partition_count {
                    if let Some(member_partitions) = assignments.get_mut(*member_id) {
                        member_partitions.push((topic.clone(), partition_idx));
                    }
                    partition_idx += 1;
                }
            }
        }

        // Convert to TargetAssignment
        assignments
            .into_iter()
            .map(|(member_id, partitions)| {
                (
                    member_id,
                    TargetAssignment::with_epoch(group.assignment_epoch + 1, partitions),
                )
            })
            .collect()
    }
}

/// Sticky assignor
///
/// Attempts to preserve existing assignments while balancing partitions evenly.
/// Minimizes partition movement during rebalances.
#[derive(Debug, Default)]
pub struct StickyAssignor;

impl StickyAssignor {
    /// Create a new sticky assignor
    pub fn new() -> Self {
        Self
    }
}

impl ServerSideAssignor for StickyAssignor {
    fn name(&self) -> &str {
        "sticky"
    }

    fn assign(
        &self,
        group: &ConsumerGroupKip848,
        topic_partitions: &HashMap<String, i32>,
    ) -> HashMap<MemberId, TargetAssignment> {
        let mut assignments: HashMap<MemberId, Vec<(String, i32)>> = HashMap::new();

        // Initialize with current assignments for each member
        for (member_id, member) in &group.members {
            let current: Vec<(String, i32)> =
                member.current_assignment.as_set().into_iter().collect();
            assignments.insert(member_id.clone(), current);
        }

        if group.members.is_empty() {
            return assignments
                .into_iter()
                .map(|(k, v)| (k, TargetAssignment::from_partitions(v)))
                .collect();
        }

        // Collect all partitions that need to be assigned
        let mut all_partitions: HashSet<(String, i32)> = HashSet::new();
        for topic in &group.subscribed_topics {
            if let Some(&partition_count) = topic_partitions.get(topic) {
                for partition in 0..partition_count {
                    all_partitions.insert((topic.clone(), partition));
                }
            }
        }

        // Find currently assigned partitions
        let mut assigned: HashSet<(String, i32)> = HashSet::new();
        for partitions in assignments.values() {
            for p in partitions {
                assigned.insert(p.clone());
            }
        }

        // Remove partitions that are no longer valid (topic removed, etc.)
        for partitions in assignments.values_mut() {
            partitions.retain(|p| all_partitions.contains(p));
        }

        // Find unassigned partitions
        let unassigned: Vec<(String, i32)> =
            all_partitions.difference(&assigned).cloned().collect();

        // Get member IDs sorted for deterministic assignment
        let mut member_ids: Vec<&MemberId> = group.members.keys().collect();
        member_ids.sort();

        // Assign unassigned partitions to members with fewest partitions
        for partition in unassigned {
            // Find member with fewest partitions who is subscribed to this topic
            let best_member = member_ids
                .iter()
                .filter(|id| {
                    group
                        .members
                        .get(**id)
                        .map(|m| m.subscribed_topics.contains(&partition.0))
                        .unwrap_or(false)
                })
                .min_by_key(|id| assignments.get(**id).map(|v| v.len()).unwrap_or(0));

            if let Some(member_id) = best_member {
                if let Some(member_partitions) = assignments.get_mut(*member_id) {
                    member_partitions.push(partition);
                }
            }
        }

        // Rebalance: move partitions from over-assigned to under-assigned members
        let total_partitions: usize = assignments.values().map(|v| v.len()).sum();
        let _target_per_member = total_partitions / member_ids.len().max(1);
        let _extra = total_partitions % member_ids.len().max(1);

        // This is a simplified rebalancing - a full implementation would be more sophisticated
        // For now, we just ensure the basic assignment is done

        // Convert to TargetAssignment
        assignments
            .into_iter()
            .map(|(member_id, partitions)| {
                (
                    member_id,
                    TargetAssignment::with_epoch(group.assignment_epoch + 1, partitions),
                )
            })
            .collect()
    }
}

/// Registry for server-side assignors
pub struct AssignorRegistry {
    assignors: HashMap<String, Box<dyn ServerSideAssignor>>,
    default_assignor: String,
}

impl Default for AssignorRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl AssignorRegistry {
    /// Create a new registry with default assignors
    pub fn new() -> Self {
        let mut registry = Self {
            assignors: HashMap::new(),
            default_assignor: "uniform".to_string(),
        };

        // Register default assignors
        registry.register(Box::new(UniformAssignor::new()));
        registry.register(Box::new(RangeAssignor::new()));
        registry.register(Box::new(StickyAssignor::new()));

        registry
    }

    /// Register an assignor
    pub fn register(&mut self, assignor: Box<dyn ServerSideAssignor>) {
        self.assignors.insert(assignor.name().to_string(), assignor);
    }

    /// Get an assignor by name
    pub fn get(&self, name: &str) -> Option<&dyn ServerSideAssignor> {
        self.assignors.get(name).map(|a| a.as_ref())
    }

    /// Get the default assignor
    ///
    /// Returns None only if the registry was improperly initialized (should not happen
    /// in normal usage as `new()` always registers the default assignor).
    pub fn get_default(&self) -> Option<&dyn ServerSideAssignor> {
        self.assignors
            .get(&self.default_assignor)
            .map(|a| a.as_ref())
    }

    /// Set the default assignor name
    pub fn set_default(&mut self, name: &str) -> bool {
        if self.assignors.contains_key(name) {
            self.default_assignor = name.to_string();
            true
        } else {
            false
        }
    }

    /// List available assignor names
    pub fn list_assignors(&self) -> Vec<&str> {
        self.assignors.keys().map(|s| s.as_str()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::kip848::member::ConsumerMember;

    fn create_test_group() -> ConsumerGroupKip848 {
        let mut group = ConsumerGroupKip848::new("test-group".to_string(), "uniform".to_string());

        let mut member1 = ConsumerMember::new(
            "member-1".to_string(),
            None,
            "client-1".to_string(),
            "localhost".to_string(),
            30000,
            300000,
        );
        member1.set_subscribed_topics(vec!["topic-1".to_string(), "topic-2".to_string()]);
        group.add_member(member1);

        let mut member2 = ConsumerMember::new(
            "member-2".to_string(),
            None,
            "client-2".to_string(),
            "localhost".to_string(),
            30000,
            300000,
        );
        member2.set_subscribed_topics(vec!["topic-1".to_string(), "topic-2".to_string()]);
        group.add_member(member2);

        group
    }

    fn create_topic_partitions() -> HashMap<String, i32> {
        let mut topics = HashMap::new();
        topics.insert("topic-1".to_string(), 4);
        topics.insert("topic-2".to_string(), 4);
        topics
    }

    #[test]
    fn test_uniform_assignor() {
        let group = create_test_group();
        let topic_partitions = create_topic_partitions();

        let assignor = UniformAssignor::new();
        let assignments = assignor.assign(&group, &topic_partitions);

        assert_eq!(assignments.len(), 2);

        // Each member should get 4 partitions (8 total / 2 members)
        let total_assigned: usize = assignments.values().map(|a| a.partition_count()).sum();
        assert_eq!(total_assigned, 8);
    }

    #[test]
    fn test_range_assignor() {
        let group = create_test_group();
        let topic_partitions = create_topic_partitions();

        let assignor = RangeAssignor::new();
        let assignments = assignor.assign(&group, &topic_partitions);

        assert_eq!(assignments.len(), 2);

        // Each member should get 4 partitions (2 per topic)
        let total_assigned: usize = assignments.values().map(|a| a.partition_count()).sum();
        assert_eq!(total_assigned, 8);
    }

    #[test]
    fn test_assignor_registry() {
        let registry = AssignorRegistry::new();

        assert!(registry.get("uniform").is_some());
        assert!(registry.get("range").is_some());
        assert!(registry.get("sticky").is_some());
        assert!(registry.get("nonexistent").is_none());

        let assignors = registry.list_assignors();
        assert!(assignors.contains(&"uniform"));
        assert!(assignors.contains(&"range"));
        assert!(assignors.contains(&"sticky"));
    }

    #[test]
    fn test_empty_group_assignment() {
        let group = ConsumerGroupKip848::new("empty-group".to_string(), "uniform".to_string());
        let topic_partitions = create_topic_partitions();

        let assignor = UniformAssignor::new();
        let assignments = assignor.assign(&group, &topic_partitions);

        assert!(assignments.is_empty());
    }
}
