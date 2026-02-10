//! KIP-848 Consumer Group Types
//!
//! Defines the consumer group state for the new protocol.

use super::assignment::TargetAssignment;
use super::member::{ConsumerMember, MemberId, MemberState};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Instant;

/// Group epoch - incremented on each rebalance
pub type GroupEpoch = i32;

/// Group states in KIP-848 protocol
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum GroupStateKip848 {
    /// Group has no members
    #[default]
    Empty,

    /// Group has members and a stable assignment
    Stable,

    /// Group is computing or distributing a new assignment
    Reconciling,

    /// Group is being removed
    Dead,
}

/// A consumer group using the KIP-848 protocol
#[derive(Debug, Serialize, Deserialize)]
pub struct ConsumerGroupKip848 {
    /// Group ID
    pub group_id: String,

    /// Current group epoch
    pub group_epoch: GroupEpoch,

    /// Current assignment epoch (target assignment version)
    pub assignment_epoch: i32,

    /// Current group state
    pub state: GroupStateKip848,

    /// Members in this group
    pub members: HashMap<MemberId, ConsumerMember>,

    /// Current target assignments for all members
    /// Key: member_id, Value: their target assignment
    pub target_assignments: HashMap<MemberId, TargetAssignment>,

    /// Preferred assignor for this group
    pub assignor: String,

    /// Subscribed topics (union of all member subscriptions)
    pub subscribed_topics: Vec<String>,

    /// Last state change timestamp
    #[serde(skip, default = "Instant::now")]
    pub last_state_change: Instant,
}

impl ConsumerGroupKip848 {
    /// Create a new empty group
    pub fn new(group_id: String, assignor: String) -> Self {
        Self {
            group_id,
            group_epoch: 0,
            assignment_epoch: 0,
            state: GroupStateKip848::Empty,
            members: HashMap::new(),
            target_assignments: HashMap::new(),
            assignor,
            subscribed_topics: Vec::new(),
            last_state_change: Instant::now(),
        }
    }

    /// Add a member to the group
    pub fn add_member(&mut self, member: ConsumerMember) {
        let member_id = member.member_id.clone();
        self.members.insert(member_id, member);
        self.update_subscribed_topics();
        self.transition_to_reconciling();
    }

    /// Remove a member from the group
    pub fn remove_member(&mut self, member_id: &str) -> Option<ConsumerMember> {
        let member = self.members.remove(member_id);
        self.target_assignments.remove(member_id);

        if member.is_some() {
            self.update_subscribed_topics();
            if self.members.is_empty() {
                self.transition_to_empty();
            } else {
                self.transition_to_reconciling();
            }
        }

        member
    }

    /// Get a member by ID
    pub fn get_member(&self, member_id: &str) -> Option<&ConsumerMember> {
        self.members.get(member_id)
    }

    /// Get a mutable reference to a member by ID
    pub fn get_member_mut(&mut self, member_id: &str) -> Option<&mut ConsumerMember> {
        self.members.get_mut(member_id)
    }

    /// Check if a member exists
    pub fn has_member(&self, member_id: &str) -> bool {
        self.members.contains_key(member_id)
    }

    /// Get member count
    pub fn member_count(&self) -> usize {
        self.members.len()
    }

    /// Bump group epoch
    pub fn bump_epoch(&mut self) {
        self.group_epoch += 1;
    }

    /// Bump assignment epoch
    pub fn bump_assignment_epoch(&mut self) {
        self.assignment_epoch += 1;
    }

    /// Update the union of subscribed topics from all members
    fn update_subscribed_topics(&mut self) {
        let mut all_topics: Vec<String> = self
            .members
            .values()
            .flat_map(|m| m.subscribed_topics.iter().cloned())
            .collect();
        all_topics.sort();
        all_topics.dedup();
        self.subscribed_topics = all_topics;
    }

    /// Transition to Empty state
    pub fn transition_to_empty(&mut self) {
        self.state = GroupStateKip848::Empty;
        self.last_state_change = Instant::now();
        self.target_assignments.clear();
    }

    /// Transition to Stable state
    pub fn transition_to_stable(&mut self) {
        self.state = GroupStateKip848::Stable;
        self.last_state_change = Instant::now();
    }

    /// Transition to Reconciling state
    pub fn transition_to_reconciling(&mut self) {
        if self.state != GroupStateKip848::Reconciling {
            self.state = GroupStateKip848::Reconciling;
            self.last_state_change = Instant::now();
            self.bump_epoch();
        }
    }

    /// Transition to Dead state
    pub fn transition_to_dead(&mut self) {
        self.state = GroupStateKip848::Dead;
        self.last_state_change = Instant::now();
    }

    /// Set target assignments for all members
    pub fn set_target_assignments(&mut self, assignments: HashMap<MemberId, TargetAssignment>) {
        self.target_assignments = assignments;
        self.bump_assignment_epoch();

        // Update each member's target assignment
        for (member_id, target) in &self.target_assignments {
            if let Some(member) = self.members.get_mut(member_id) {
                member.transition_to_reconciling(target.clone());
            }
        }
    }

    /// Check if all members have completed reconciliation
    pub fn is_reconciliation_complete(&self) -> bool {
        self.members
            .values()
            .all(|m| m.state == MemberState::Stable || m.is_reconciliation_complete())
    }

    /// Get expired members based on session timeout
    pub fn get_expired_members(&self) -> Vec<MemberId> {
        self.members
            .iter()
            .filter(|(_, m)| m.is_session_expired())
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Get all member IDs
    pub fn member_ids(&self) -> Vec<MemberId> {
        self.members.keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_member(id: &str) -> ConsumerMember {
        ConsumerMember::new(
            id.to_string(),
            None,
            "client".to_string(),
            "localhost".to_string(),
            30000,
            300000,
        )
    }

    #[test]
    fn test_group_lifecycle() {
        let mut group = ConsumerGroupKip848::new("test-group".to_string(), "uniform".to_string());
        assert_eq!(group.state, GroupStateKip848::Empty);
        assert_eq!(group.group_epoch, 0);

        // Add member triggers reconciling
        let mut member = create_test_member("member-1");
        member.set_subscribed_topics(vec!["topic-1".to_string()]);
        group.add_member(member);

        assert_eq!(group.state, GroupStateKip848::Reconciling);
        assert_eq!(group.group_epoch, 1);
        assert_eq!(group.member_count(), 1);
        assert!(group.subscribed_topics.contains(&"topic-1".to_string()));

        // Remove member
        group.remove_member("member-1");
        assert_eq!(group.state, GroupStateKip848::Empty);
        assert_eq!(group.member_count(), 0);
    }

    #[test]
    fn test_subscribed_topics_union() {
        let mut group = ConsumerGroupKip848::new("test-group".to_string(), "uniform".to_string());

        let mut member1 = create_test_member("member-1");
        member1.set_subscribed_topics(vec!["topic-1".to_string(), "topic-2".to_string()]);
        group.add_member(member1);

        let mut member2 = create_test_member("member-2");
        member2.set_subscribed_topics(vec!["topic-2".to_string(), "topic-3".to_string()]);
        group.add_member(member2);

        assert_eq!(group.subscribed_topics.len(), 3);
        assert!(group.subscribed_topics.contains(&"topic-1".to_string()));
        assert!(group.subscribed_topics.contains(&"topic-2".to_string()));
        assert!(group.subscribed_topics.contains(&"topic-3".to_string()));
    }
}
