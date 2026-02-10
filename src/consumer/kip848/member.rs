//! KIP-848 Consumer Member Types
//!
//! Defines the member state machine and related types for the new consumer protocol.

use super::assignment::{CurrentAssignment, TargetAssignment};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::time::Instant;

/// Member epoch - incremented on each state change
pub type MemberEpoch = i32;

/// Instance ID for static membership
pub type InstanceId = String;

/// Member ID - unique identifier for a member within a group
pub type MemberId = String;

/// Member states in KIP-848 protocol
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum MemberState {
    /// Member is joining the group (initial state)
    /// Awaiting assignment from the server
    #[default]
    Joining,

    /// Member has a stable assignment
    /// Normal operating state - member is consuming from assigned partitions
    Stable,

    /// Member is reconciling its assignment
    /// Target assignment differs from current assignment
    /// Member must revoke and/or assign partitions
    Reconciling,

    /// Member is leaving the group
    /// Triggered by explicit leave request or timeout
    Leaving,

    /// Member has been fenced due to stale epoch
    /// Must rejoin with a new member ID
    Fenced,
}

/// A consumer member in a KIP-848 group
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerMember {
    /// Unique member ID assigned by the server
    pub member_id: MemberId,

    /// Optional instance ID for static membership
    pub instance_id: Option<InstanceId>,

    /// Current member epoch
    pub member_epoch: MemberEpoch,

    /// Current member state
    pub state: MemberState,

    /// Client ID from the consumer configuration
    pub client_id: String,

    /// Client host address
    pub client_host: String,

    /// Topics the member is subscribed to
    pub subscribed_topics: HashSet<String>,

    /// Server-assigned target assignment
    #[serde(skip)]
    pub target_assignment: TargetAssignment,

    /// Current assignment owned by the member
    #[serde(skip)]
    pub current_assignment: CurrentAssignment,

    /// Partitions pending revocation (target doesn't include these)
    #[serde(skip)]
    pub pending_revocation: HashSet<(String, i32)>,

    /// Partitions pending assignment (target includes but not yet owned)
    #[serde(skip)]
    pub pending_assignment: HashSet<(String, i32)>,

    /// Last heartbeat timestamp
    #[serde(skip, default = "Instant::now")]
    pub last_heartbeat: Instant,

    /// Session timeout in milliseconds
    pub session_timeout_ms: i32,

    /// Rebalance timeout in milliseconds
    pub rebalance_timeout_ms: i32,

    /// Custom assignor name preference (if any)
    pub assignor: Option<String>,
}

impl ConsumerMember {
    /// Create a new member in Joining state
    pub fn new(
        member_id: MemberId,
        instance_id: Option<InstanceId>,
        client_id: String,
        client_host: String,
        session_timeout_ms: i32,
        rebalance_timeout_ms: i32,
    ) -> Self {
        Self {
            member_id,
            instance_id,
            member_epoch: 0,
            state: MemberState::Joining,
            client_id,
            client_host,
            subscribed_topics: HashSet::new(),
            target_assignment: TargetAssignment::empty(),
            current_assignment: CurrentAssignment::empty(),
            pending_revocation: HashSet::new(),
            pending_assignment: HashSet::new(),
            last_heartbeat: Instant::now(),
            session_timeout_ms,
            rebalance_timeout_ms,
            assignor: None,
        }
    }

    /// Update subscribed topics
    pub fn set_subscribed_topics(&mut self, topics: impl IntoIterator<Item = String>) {
        self.subscribed_topics = topics.into_iter().collect();
    }

    /// Check if session has timed out
    pub fn is_session_expired(&self) -> bool {
        self.last_heartbeat.elapsed().as_millis() > self.session_timeout_ms as u128
    }

    /// Update heartbeat timestamp
    pub fn touch(&mut self) {
        self.last_heartbeat = Instant::now();
    }

    /// Bump member epoch
    pub fn bump_epoch(&mut self) {
        self.member_epoch += 1;
    }

    /// Transition to Stable state
    pub fn transition_to_stable(&mut self) {
        self.state = MemberState::Stable;
        self.bump_epoch();
        // Clear pending operations - member is now stable
        self.pending_revocation.clear();
        self.pending_assignment.clear();
    }

    /// Transition to Reconciling state
    pub fn transition_to_reconciling(&mut self, target: TargetAssignment) {
        self.state = MemberState::Reconciling;
        self.target_assignment = target;
        // Calculate pending operations
        self.compute_pending_operations();
    }

    /// Transition to Leaving state
    pub fn transition_to_leaving(&mut self) {
        self.state = MemberState::Leaving;
    }

    /// Transition to Fenced state
    pub fn transition_to_fenced(&mut self) {
        self.state = MemberState::Fenced;
    }

    /// Compute pending revocations and assignments from current and target
    fn compute_pending_operations(&mut self) {
        let current_set = self.current_assignment.as_set();
        let target_set = self.target_assignment.as_set();

        // Pending revocation: partitions in current but not in target
        self.pending_revocation = current_set.difference(&target_set).cloned().collect();

        // Pending assignment: partitions in target but not in current
        self.pending_assignment = target_set.difference(&current_set).cloned().collect();
    }

    /// Process revocation acknowledgment from client
    pub fn acknowledge_revocations(&mut self, revoked: &[(String, i32)]) {
        for tp in revoked {
            self.pending_revocation.remove(tp);
            self.current_assignment.remove(tp);
        }
    }

    /// Process assignment acknowledgment from client
    pub fn acknowledge_assignments(&mut self, assigned: &[(String, i32)]) {
        for tp in assigned {
            self.pending_assignment.remove(tp);
            self.current_assignment.add(tp.clone());
        }
    }

    /// Check if reconciliation is complete
    pub fn is_reconciliation_complete(&self) -> bool {
        self.pending_revocation.is_empty() && self.pending_assignment.is_empty()
    }

    /// Get list of partitions to revoke
    pub fn get_pending_revocations(&self) -> Vec<(String, i32)> {
        self.pending_revocation.iter().cloned().collect()
    }

    /// Get list of partitions to assign
    pub fn get_pending_assignments(&self) -> Vec<(String, i32)> {
        self.pending_assignment.iter().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_member_state_transitions() {
        let mut member = ConsumerMember::new(
            "member-1".to_string(),
            None,
            "client-1".to_string(),
            "localhost:9092".to_string(),
            30000,
            300000,
        );

        assert_eq!(member.state, MemberState::Joining);
        assert_eq!(member.member_epoch, 0);

        member.transition_to_stable();
        assert_eq!(member.state, MemberState::Stable);
        assert_eq!(member.member_epoch, 1);

        let target = TargetAssignment::from_partitions(vec![
            ("topic-1".to_string(), 0),
            ("topic-1".to_string(), 1),
        ]);
        member.transition_to_reconciling(target);
        assert_eq!(member.state, MemberState::Reconciling);

        member.transition_to_fenced();
        assert_eq!(member.state, MemberState::Fenced);
    }

    #[test]
    fn test_pending_operations() {
        let mut member = ConsumerMember::new(
            "member-1".to_string(),
            None,
            "client-1".to_string(),
            "localhost:9092".to_string(),
            30000,
            300000,
        );

        // Set current assignment
        member.current_assignment.add(("topic-1".to_string(), 0));
        member.current_assignment.add(("topic-1".to_string(), 1));

        // Set target assignment (removes partition 1, adds partition 2)
        let target = TargetAssignment::from_partitions(vec![
            ("topic-1".to_string(), 0),
            ("topic-1".to_string(), 2),
        ]);

        member.transition_to_reconciling(target);

        assert_eq!(member.pending_revocation.len(), 1);
        assert!(member
            .pending_revocation
            .contains(&("topic-1".to_string(), 1)));

        assert_eq!(member.pending_assignment.len(), 1);
        assert!(member
            .pending_assignment
            .contains(&("topic-1".to_string(), 2)));
    }
}
