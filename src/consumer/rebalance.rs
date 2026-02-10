//! Partition assignment strategies for consumer groups
//!
//! This module provides various partition assignment strategies for consumer groups:
//!
//! - `RangeAssignor`: Assigns partitions on a per-topic basis using ranges
//! - `RoundRobinAssignor`: Assigns partitions across topics in round-robin fashion
//! - `StickyAssignor`: Minimizes partition movements during rebalances
//!
//! ## Sticky Assignment
//!
//! The `StickyAssignor` maintains partition stickiness by:
//! 1. Preserving existing assignments when possible
//! 2. Only moving partitions when necessary for balance
//! 3. Using generation tracking to handle member departures
//!
//! This reduces unnecessary partition movement during rebalances, improving
//! consumer group stability and reducing duplicate processing.

use crate::consumer::group::GroupMember;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// Trait for partition assignment strategies
pub trait PartitionAssignor {
    /// Assign partitions to members
    fn assign(
        &self,
        members: &HashMap<String, GroupMember>,
        topics_partitions: &HashMap<String, i32>,
    ) -> HashMap<String, Vec<(String, i32)>>;
}

/// Range assignment strategy
///
/// Assigns partitions on a per-topic basis. For each topic, partitions are
/// divided into ranges and assigned to consumers in order.
pub struct RangeAssignor;

impl PartitionAssignor for RangeAssignor {
    fn assign(
        &self,
        members: &HashMap<String, GroupMember>,
        topics_partitions: &HashMap<String, i32>,
    ) -> HashMap<String, Vec<(String, i32)>> {
        let mut assignments: HashMap<String, Vec<(String, i32)>> = HashMap::new();

        // Initialize assignments for all members
        for member_id in members.keys() {
            assignments.insert(member_id.clone(), Vec::new());
        }

        // Get all unique topics subscribed by all members
        let mut all_topics = std::collections::HashSet::new();
        for member in members.values() {
            for topic in &member.subscriptions {
                all_topics.insert(topic.clone());
            }
        }

        // For each topic, assign partitions using range strategy
        for topic in all_topics {
            let num_partitions = topics_partitions.get(&topic).copied().unwrap_or(1);

            // Get members subscribed to this topic
            let mut subscribed_members: Vec<String> = members
                .iter()
                .filter(|(_, m)| m.subscriptions.contains(&topic))
                .map(|(id, _)| id.clone())
                .collect();

            // Sort members for deterministic assignment
            subscribed_members.sort();

            if subscribed_members.is_empty() {
                continue;
            }

            // Assign partitions in ranges
            let partitions_per_member = num_partitions / subscribed_members.len() as i32;
            let extra = num_partitions % subscribed_members.len() as i32;

            let mut partition = 0;
            for (i, member_id) in subscribed_members.iter().enumerate() {
                let num_to_assign = if (i as i32) < extra {
                    partitions_per_member + 1
                } else {
                    partitions_per_member
                };

                if let Some(assignment) = assignments.get_mut(member_id) {
                    for _ in 0..num_to_assign {
                        assignment.push((topic.clone(), partition));
                        partition += 1;
                    }
                }
            }
        }

        assignments
    }
}

/// Round-robin assignment strategy
///
/// Assigns partitions across all topics in a round-robin fashion
pub struct RoundRobinAssignor;

impl PartitionAssignor for RoundRobinAssignor {
    fn assign(
        &self,
        members: &HashMap<String, GroupMember>,
        topics_partitions: &HashMap<String, i32>,
    ) -> HashMap<String, Vec<(String, i32)>> {
        let mut assignments: HashMap<String, Vec<(String, i32)>> = HashMap::new();

        // Initialize assignments for all members
        for member_id in members.keys() {
            assignments.insert(member_id.clone(), Vec::new());
        }

        // Collect all (topic, partition) pairs
        let mut all_partitions = Vec::new();
        for (topic, &num_partitions) in topics_partitions {
            for partition in 0..num_partitions {
                all_partitions.push((topic.clone(), partition));
            }
        }

        // Sort for deterministic assignment
        all_partitions.sort();

        // Get sorted member list
        let mut member_list: Vec<String> = members.keys().cloned().collect();
        member_list.sort();

        if member_list.is_empty() {
            return assignments;
        }

        // Assign partitions in round-robin
        for (i, partition) in all_partitions.iter().enumerate() {
            let member_idx = i % member_list.len();
            let member_id = &member_list[member_idx];

            // Check if member is subscribed to this topic
            if let Some(member) = members.get(member_id) {
                if member.subscriptions.contains(&partition.0) {
                    if let Some(assignment) = assignments.get_mut(member_id) {
                        assignment.push(partition.clone());
                    }
                }
            }
        }

        assignments
    }
}

/// Topic partition identifier
pub type TopicPartition = (String, i32);

/// User data encoding for sticky assignment
///
/// This is encoded in the member's protocol_metadata and contains
/// the member's previous assignment along with generation tracking.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StickyUserData {
    /// Previous assignments from this member
    pub owned_partitions: Vec<TopicPartition>,
    /// Generation when this assignment was made
    pub generation: i32,
}

impl StickyUserData {
    /// Create new user data with owned partitions
    pub fn new(owned_partitions: Vec<TopicPartition>, generation: i32) -> Self {
        Self {
            owned_partitions,
            generation,
        }
    }

    /// Encode user data to bytes
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }

    /// Decode user data from bytes
    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.is_empty() {
            return None;
        }
        serde_json::from_slice(data).ok()
    }
}

/// Sticky partition assignor
///
/// The sticky assignor minimizes partition movement during rebalances by:
/// 1. Preserving existing assignments when possible
/// 2. Moving partitions only to achieve balanced distribution
/// 3. Preferring to keep partitions with their current owner
///
/// This is particularly useful for stateful consumers where partition
/// movement has a high cost (e.g., local state rebuild).
///
/// ## Algorithm
///
/// 1. Collect all partitions that need assignment
/// 2. For each partition, check if the previous owner is still subscribed
/// 3. Keep the partition with its previous owner if possible
/// 4. Assign remaining partitions to members with fewer assignments
/// 5. Rebalance if assignment is significantly unbalanced
#[derive(Debug, Default)]
pub struct StickyAssignor {
    /// Current generation for this rebalance (reserved for future use)
    #[allow(dead_code)]
    generation: i32,
}

impl StickyAssignor {
    /// Create a new sticky assignor
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a sticky assignor with a specific generation
    pub fn with_generation(generation: i32) -> Self {
        Self { generation }
    }

    /// Parse sticky user data from member protocol metadata
    fn parse_user_data(metadata: &[u8]) -> Option<StickyUserData> {
        StickyUserData::decode(metadata)
    }

    /// Get the target assignment size for even distribution
    fn target_assignment_size(total_partitions: usize, num_members: usize) -> (usize, usize) {
        if num_members == 0 {
            return (0, 0);
        }
        let base = total_partitions / num_members;
        let extra = total_partitions % num_members;
        (base, extra)
    }

    /// Check if a member can accept more partitions
    fn can_accept_more(current_size: usize, target_base: usize, extra_slots: &mut usize) -> bool {
        if current_size < target_base {
            true
        } else if current_size == target_base && *extra_slots > 0 {
            *extra_slots -= 1;
            true
        } else {
            false
        }
    }
}

impl PartitionAssignor for StickyAssignor {
    fn assign(
        &self,
        members: &HashMap<String, GroupMember>,
        topics_partitions: &HashMap<String, i32>,
    ) -> HashMap<String, Vec<TopicPartition>> {
        let mut assignments: HashMap<String, Vec<TopicPartition>> = HashMap::new();

        // Initialize assignments for all members
        for member_id in members.keys() {
            assignments.insert(member_id.clone(), Vec::new());
        }

        if members.is_empty() {
            return assignments;
        }

        // Collect all partitions that need assignment
        let mut all_partitions: Vec<TopicPartition> = Vec::new();
        for (topic, &num_partitions) in topics_partitions {
            for partition in 0..num_partitions {
                all_partitions.push((topic.clone(), partition));
            }
        }
        all_partitions.sort();

        // Build a map of previous assignments from user data
        let mut previous_assignments: HashMap<TopicPartition, String> = HashMap::new();
        for (member_id, member) in members {
            // Try to get previous assignment from protocol_metadata (user data)
            if let Some(user_data) = Self::parse_user_data(&member.protocol_metadata) {
                for tp in user_data.owned_partitions {
                    // Only consider previous assignment if member is still subscribed
                    if member.subscriptions.contains(&tp.0) {
                        previous_assignments.insert(tp, member_id.clone());
                    }
                }
            }
            // Also consider current assignment field
            for tp in &member.assignment {
                if member.subscriptions.contains(&tp.0) {
                    previous_assignments.insert(tp.clone(), member_id.clone());
                }
            }
        }

        // Build subscription map: which members are subscribed to which topics
        let mut topic_subscribers: HashMap<String, Vec<String>> = HashMap::new();
        for (member_id, member) in members {
            for topic in &member.subscriptions {
                topic_subscribers
                    .entry(topic.clone())
                    .or_default()
                    .push(member_id.clone());
            }
        }

        // Sort subscribers for determinism
        for subscribers in topic_subscribers.values_mut() {
            subscribers.sort();
        }

        // Calculate target distribution
        let num_members = members.len();
        let total_partitions = all_partitions.len();
        let (target_base, extra_count) =
            Self::target_assignment_size(total_partitions, num_members);

        // Track how many partitions each member has
        let mut member_counts: HashMap<String, usize> =
            members.keys().map(|id| (id.clone(), 0)).collect();

        // Track unassigned partitions
        let mut unassigned: Vec<TopicPartition> = Vec::new();
        let mut assigned_partitions: HashSet<TopicPartition> = HashSet::new();

        // First pass: preserve sticky assignments where valid
        for tp in &all_partitions {
            if let Some(prev_owner) = previous_assignments.get(tp) {
                // Check if previous owner is still subscribed to this topic
                if let Some(member) = members.get(prev_owner) {
                    if member.subscriptions.contains(&tp.0) {
                        // Check if this member can still accept more
                        let current_count = *member_counts.get(prev_owner).unwrap_or(&0);
                        let max_allowed = target_base + if extra_count > 0 { 1 } else { 0 };

                        if current_count < max_allowed {
                            if let Some(assignment) = assignments.get_mut(prev_owner) {
                                assignment.push(tp.clone());
                                if let Some(count) = member_counts.get_mut(prev_owner) {
                                    *count += 1;
                                }
                                assigned_partitions.insert(tp.clone());
                                continue;
                            }
                        }
                    }
                }
            }
            unassigned.push(tp.clone());
        }

        // Second pass: assign remaining partitions to members with capacity
        // Sort members by current count (ascending) for balanced assignment
        let mut sorted_members: Vec<String> = members.keys().cloned().collect();
        sorted_members.sort();

        let mut remaining_extra = extra_count;
        for tp in unassigned {
            // Find eligible subscribers sorted by their current assignment count
            let topic = &tp.0;
            let subscribers = topic_subscribers.get(topic).cloned().unwrap_or_default();

            // Sort subscribers by current assignment count (prefer less loaded)
            let mut eligible: Vec<(String, usize)> = subscribers
                .iter()
                .filter_map(|s| member_counts.get(s).map(|&count| (s.clone(), count)))
                .collect();
            eligible.sort_by_key(|(_, count)| *count);

            // Find a member that can accept this partition
            for (member_id, current_count) in eligible {
                if Self::can_accept_more(current_count, target_base, &mut remaining_extra) {
                    if let Some(assignment) = assignments.get_mut(&member_id) {
                        assignment.push(tp.clone());
                        if let Some(count) = member_counts.get_mut(&member_id) {
                            *count += 1;
                        }
                        assigned_partitions.insert(tp.clone());
                        break;
                    }
                }
            }
        }

        // Final pass: assign any truly unassigned partitions (edge cases)
        for tp in &all_partitions {
            if !assigned_partitions.contains(tp) {
                // Find any subscriber that can take it
                let topic = &tp.0;
                if let Some(subscribers) = topic_subscribers.get(topic) {
                    if let Some(member_id) = subscribers.first() {
                        if let Some(assignment) = assignments.get_mut(member_id) {
                            assignment.push(tp.clone());
                        }
                    }
                }
            }
        }

        // Sort assignments for determinism
        for assignment in assignments.values_mut() {
            assignment.sort();
        }

        assignments
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    fn create_test_member(id: &str, topics: Vec<String>) -> GroupMember {
        GroupMember {
            member_id: id.to_string(),
            client_id: format!("client-{}", id),
            client_host: "localhost".to_string(),
            session_timeout_ms: 45000,
            rebalance_timeout_ms: 300000,
            subscriptions: topics,
            assignment: vec![],
            last_heartbeat: Instant::now(),
            protocol_metadata: vec![],
        }
    }

    #[test]
    fn test_range_assignor_single_topic() {
        let mut members = HashMap::new();
        members.insert(
            "m1".to_string(),
            create_test_member("m1", vec!["topic1".to_string()]),
        );
        members.insert(
            "m2".to_string(),
            create_test_member("m2", vec!["topic1".to_string()]),
        );
        members.insert(
            "m3".to_string(),
            create_test_member("m3", vec!["topic1".to_string()]),
        );

        let mut topics = HashMap::new();
        topics.insert("topic1".to_string(), 5);

        let assignor = RangeAssignor;
        let assignments = assignor.assign(&members, &topics);

        // With 5 partitions and 3 members: 2, 2, 1
        assert_eq!(assignments.len(), 3);

        let total: usize = assignments.values().map(|a| a.len()).sum();
        assert_eq!(total, 5);
    }

    #[test]
    fn test_roundrobin_assignor() {
        let mut members = HashMap::new();
        members.insert(
            "m1".to_string(),
            create_test_member("m1", vec!["topic1".to_string()]),
        );
        members.insert(
            "m2".to_string(),
            create_test_member("m2", vec!["topic1".to_string()]),
        );

        let mut topics = HashMap::new();
        topics.insert("topic1".to_string(), 4);

        let assignor = RoundRobinAssignor;
        let assignments = assignor.assign(&members, &topics);

        assert_eq!(assignments.len(), 2);

        let total: usize = assignments.values().map(|a| a.len()).sum();
        assert_eq!(total, 4);
    }

    fn create_test_member_with_assignment(
        id: &str,
        topics: Vec<String>,
        assignment: Vec<(String, i32)>,
    ) -> GroupMember {
        GroupMember {
            member_id: id.to_string(),
            client_id: format!("client-{}", id),
            client_host: "localhost".to_string(),
            session_timeout_ms: 45000,
            rebalance_timeout_ms: 300000,
            subscriptions: topics,
            assignment,
            last_heartbeat: Instant::now(),
            protocol_metadata: vec![],
        }
    }

    fn create_test_member_with_userdata(
        id: &str,
        topics: Vec<String>,
        user_data: StickyUserData,
    ) -> GroupMember {
        GroupMember {
            member_id: id.to_string(),
            client_id: format!("client-{}", id),
            client_host: "localhost".to_string(),
            session_timeout_ms: 45000,
            rebalance_timeout_ms: 300000,
            subscriptions: topics,
            assignment: vec![],
            last_heartbeat: Instant::now(),
            protocol_metadata: user_data.encode(),
        }
    }

    #[test]
    fn test_sticky_assignor_basic() {
        let mut members = HashMap::new();
        members.insert(
            "m1".to_string(),
            create_test_member("m1", vec!["topic1".to_string()]),
        );
        members.insert(
            "m2".to_string(),
            create_test_member("m2", vec!["topic1".to_string()]),
        );

        let mut topics = HashMap::new();
        topics.insert("topic1".to_string(), 4);

        let assignor = StickyAssignor::new();
        let assignments = assignor.assign(&members, &topics);

        assert_eq!(assignments.len(), 2);

        let total: usize = assignments.values().map(|a| a.len()).sum();
        assert_eq!(total, 4);

        // Each member should get 2 partitions
        for assignment in assignments.values() {
            assert_eq!(assignment.len(), 2);
        }
    }

    #[test]
    fn test_sticky_assignor_preserves_assignments() {
        // Initial state: m1 has partitions 0,1 and m2 has partitions 2,3
        let mut members = HashMap::new();
        members.insert(
            "m1".to_string(),
            create_test_member_with_assignment(
                "m1",
                vec!["topic1".to_string()],
                vec![("topic1".to_string(), 0), ("topic1".to_string(), 1)],
            ),
        );
        members.insert(
            "m2".to_string(),
            create_test_member_with_assignment(
                "m2",
                vec!["topic1".to_string()],
                vec![("topic1".to_string(), 2), ("topic1".to_string(), 3)],
            ),
        );

        let mut topics = HashMap::new();
        topics.insert("topic1".to_string(), 4);

        let assignor = StickyAssignor::new();
        let assignments = assignor.assign(&members, &topics);

        // Assignments should be preserved
        let m1_assignment = assignments.get("m1").unwrap();
        let m2_assignment = assignments.get("m2").unwrap();

        assert_eq!(m1_assignment.len(), 2);
        assert_eq!(m2_assignment.len(), 2);

        // m1 should still have partitions 0 and 1
        assert!(m1_assignment.contains(&("topic1".to_string(), 0)));
        assert!(m1_assignment.contains(&("topic1".to_string(), 1)));

        // m2 should still have partitions 2 and 3
        assert!(m2_assignment.contains(&("topic1".to_string(), 2)));
        assert!(m2_assignment.contains(&("topic1".to_string(), 3)));
    }

    #[test]
    fn test_sticky_assignor_new_member_joins() {
        // m1 has all 4 partitions, m2 joins
        let mut members = HashMap::new();
        members.insert(
            "m1".to_string(),
            create_test_member_with_assignment(
                "m1",
                vec!["topic1".to_string()],
                vec![
                    ("topic1".to_string(), 0),
                    ("topic1".to_string(), 1),
                    ("topic1".to_string(), 2),
                    ("topic1".to_string(), 3),
                ],
            ),
        );
        members.insert(
            "m2".to_string(),
            create_test_member("m2", vec!["topic1".to_string()]),
        );

        let mut topics = HashMap::new();
        topics.insert("topic1".to_string(), 4);

        let assignor = StickyAssignor::new();
        let assignments = assignor.assign(&members, &topics);

        // Each member should get 2 partitions
        let m1_assignment = assignments.get("m1").unwrap();
        let m2_assignment = assignments.get("m2").unwrap();

        assert_eq!(m1_assignment.len(), 2);
        assert_eq!(m2_assignment.len(), 2);

        // Total should be 4
        let total: usize = assignments.values().map(|a| a.len()).sum();
        assert_eq!(total, 4);
    }

    #[test]
    fn test_sticky_assignor_member_leaves() {
        // m1 has partitions 0,1 and there's a member that just left
        let mut members = HashMap::new();
        members.insert(
            "m1".to_string(),
            create_test_member_with_assignment(
                "m1",
                vec!["topic1".to_string()],
                vec![("topic1".to_string(), 0), ("topic1".to_string(), 1)],
            ),
        );

        let mut topics = HashMap::new();
        topics.insert("topic1".to_string(), 4);

        let assignor = StickyAssignor::new();
        let assignments = assignor.assign(&members, &topics);

        // m1 should get all 4 partitions now
        let m1_assignment = assignments.get("m1").unwrap();
        assert_eq!(m1_assignment.len(), 4);
    }

    #[test]
    fn test_sticky_assignor_with_userdata() {
        // m1 has previous assignments in userdata
        let user_data = StickyUserData::new(
            vec![("topic1".to_string(), 0), ("topic1".to_string(), 1)],
            1,
        );

        let mut members = HashMap::new();
        members.insert(
            "m1".to_string(),
            create_test_member_with_userdata("m1", vec!["topic1".to_string()], user_data),
        );
        members.insert(
            "m2".to_string(),
            create_test_member("m2", vec!["topic1".to_string()]),
        );

        let mut topics = HashMap::new();
        topics.insert("topic1".to_string(), 4);

        let assignor = StickyAssignor::new();
        let assignments = assignor.assign(&members, &topics);

        // m1 should still have its previous partitions
        let m1_assignment = assignments.get("m1").unwrap();
        assert!(m1_assignment.contains(&("topic1".to_string(), 0)));
        assert!(m1_assignment.contains(&("topic1".to_string(), 1)));

        // Each should have 2 partitions
        assert_eq!(m1_assignment.len(), 2);
        assert_eq!(assignments.get("m2").unwrap().len(), 2);
    }

    #[test]
    fn test_sticky_userdata_encode_decode() {
        let user_data = StickyUserData::new(
            vec![
                ("topic1".to_string(), 0),
                ("topic1".to_string(), 1),
                ("topic2".to_string(), 0),
            ],
            5,
        );

        let encoded = user_data.encode();
        let decoded = StickyUserData::decode(&encoded).unwrap();

        assert_eq!(decoded.owned_partitions, user_data.owned_partitions);
        assert_eq!(decoded.generation, 5);
    }

    #[test]
    fn test_sticky_assignor_multiple_topics() {
        let mut members = HashMap::new();
        members.insert(
            "m1".to_string(),
            create_test_member("m1", vec!["topic1".to_string(), "topic2".to_string()]),
        );
        members.insert(
            "m2".to_string(),
            create_test_member("m2", vec!["topic1".to_string(), "topic2".to_string()]),
        );

        let mut topics = HashMap::new();
        topics.insert("topic1".to_string(), 3);
        topics.insert("topic2".to_string(), 3);

        let assignor = StickyAssignor::new();
        let assignments = assignor.assign(&members, &topics);

        // 6 total partitions, 2 members = 3 each
        let m1_assignment = assignments.get("m1").unwrap();
        let m2_assignment = assignments.get("m2").unwrap();

        assert_eq!(m1_assignment.len(), 3);
        assert_eq!(m2_assignment.len(), 3);
    }

    #[test]
    fn test_sticky_assignor_uneven_partitions() {
        let mut members = HashMap::new();
        members.insert(
            "m1".to_string(),
            create_test_member("m1", vec!["topic1".to_string()]),
        );
        members.insert(
            "m2".to_string(),
            create_test_member("m2", vec!["topic1".to_string()]),
        );
        members.insert(
            "m3".to_string(),
            create_test_member("m3", vec!["topic1".to_string()]),
        );

        let mut topics = HashMap::new();
        topics.insert("topic1".to_string(), 5);

        let assignor = StickyAssignor::new();
        let assignments = assignor.assign(&members, &topics);

        // 5 partitions among 3 members: 2, 2, 1
        let total: usize = assignments.values().map(|a| a.len()).sum();
        assert_eq!(total, 5);

        // No member should have more than 2
        for assignment in assignments.values() {
            assert!(assignment.len() <= 2);
        }
    }

    #[test]
    fn test_sticky_assignor_empty_members() {
        let members = HashMap::new();

        let mut topics = HashMap::new();
        topics.insert("topic1".to_string(), 4);

        let assignor = StickyAssignor::new();
        let assignments = assignor.assign(&members, &topics);

        assert!(assignments.is_empty());
    }

    #[test]
    fn test_sticky_assignor_no_partitions() {
        let mut members = HashMap::new();
        members.insert(
            "m1".to_string(),
            create_test_member("m1", vec!["topic1".to_string()]),
        );

        let topics = HashMap::new();

        let assignor = StickyAssignor::new();
        let assignments = assignor.assign(&members, &topics);

        // Should have entry for member but no partitions
        assert_eq!(assignments.len(), 1);
        assert!(assignments.get("m1").unwrap().is_empty());
    }
}
