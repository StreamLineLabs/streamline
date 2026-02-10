//! KIP-848 Reconciliation Engine
//!
//! Manages the incremental rebalancing process for consumer groups.

use super::assignment::AssignmentDelta;
use super::errors::Kip848Error;
use super::group::{ConsumerGroupKip848, GroupStateKip848};
use super::heartbeat::{
    topic_partitions_to_assignments, AssignmentInfo, HeartbeatRequest, HeartbeatResponse,
    TopicAssignment,
};
use super::member::{MemberId, MemberState};
use super::server_assignor::AssignorRegistry;
use parking_lot::RwLock;
use std::collections::HashMap;
use tracing::{debug, info, warn};
use uuid::Uuid;

/// Configuration for the reconciliation engine
#[derive(Debug, Clone)]
pub struct ReconciliationConfig {
    /// Default heartbeat interval in milliseconds
    pub heartbeat_interval_ms: i32,

    /// Session timeout in milliseconds
    pub session_timeout_ms: i32,

    /// Rebalance timeout in milliseconds
    pub rebalance_timeout_ms: i32,

    /// Maximum group size
    pub max_group_size: usize,
}

impl Default for ReconciliationConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval_ms: 3000,
            session_timeout_ms: 45000,
            rebalance_timeout_ms: 300000,
            max_group_size: 500,
        }
    }
}

/// Result of processing a heartbeat
#[derive(Debug)]
pub enum HeartbeatResult {
    /// Member should continue with stable assignment
    Stable,

    /// Member has new assignment to reconcile
    Reconcile {
        /// Partitions to revoke
        to_revoke: Vec<TopicAssignment>,
        /// Partitions to assign
        to_assign: Vec<TopicAssignment>,
    },

    /// Member should wait (no change)
    Wait,
}

/// The reconciliation engine manages the state of KIP-848 consumer groups
pub struct ReconciliationEngine {
    /// Consumer groups managed by this engine
    groups: RwLock<HashMap<String, ConsumerGroupKip848>>,

    /// Assignor registry
    assignor_registry: AssignorRegistry,

    /// Topic partition counts (topic name -> partition count)
    /// In production, this would query TopicManager
    topic_partitions: RwLock<HashMap<String, i32>>,

    /// Configuration
    config: ReconciliationConfig,
}

impl ReconciliationEngine {
    /// Create a new reconciliation engine
    pub fn new(config: ReconciliationConfig) -> Self {
        Self {
            groups: RwLock::new(HashMap::new()),
            assignor_registry: AssignorRegistry::new(),
            topic_partitions: RwLock::new(HashMap::new()),
            config,
        }
    }

    /// Create with default configuration
    pub fn new_default() -> Self {
        Self::new(ReconciliationConfig::default())
    }

    /// Update topic partition counts
    pub fn update_topic_partitions(&self, topic: &str, partition_count: i32) {
        let mut topics = self.topic_partitions.write();
        topics.insert(topic.to_string(), partition_count);
    }

    /// Remove a topic from tracking
    pub fn remove_topic(&self, topic: &str) {
        let mut topics = self.topic_partitions.write();
        topics.remove(topic);
    }

    /// Process a heartbeat request
    pub fn process_heartbeat(&self, request: HeartbeatRequest) -> HeartbeatResponse {
        if request.is_join() {
            self.handle_join(request)
        } else if request.is_leave() {
            self.handle_leave(request)
        } else {
            self.handle_heartbeat(request)
        }
    }

    /// Handle initial join request
    fn handle_join(&self, request: HeartbeatRequest) -> HeartbeatResponse {
        let mut groups = self.groups.write();

        // Get or create group
        let group = groups.entry(request.group_id.clone()).or_insert_with(|| {
            let assignor = request
                .server_assignor
                .clone()
                .unwrap_or_else(|| "uniform".to_string());
            info!(group_id = %request.group_id, "Creating new consumer group");
            ConsumerGroupKip848::new(request.group_id.clone(), assignor)
        });

        // Check group size limit
        if group.member_count() >= self.config.max_group_size {
            return HeartbeatResponse::error(Kip848Error::GroupMaxSizeReached);
        }

        // Generate member ID
        let member_id = format!("{}-{}", request.group_id, Uuid::new_v4());

        // Create member
        let mut member = super::member::ConsumerMember::new(
            member_id.clone(),
            request.instance_id.clone(),
            format!("client-{}", member_id),
            "unknown".to_string(),
            self.config.session_timeout_ms,
            request.rebalance_timeout_ms,
        );
        member.set_subscribed_topics(request.subscribed_topic_names.clone());

        if let Some(ref assignor) = request.server_assignor {
            member.assignor = Some(assignor.clone());
        }

        // Add member to group
        group.add_member(member);

        info!(
            group_id = %request.group_id,
            member_id = %member_id,
            "Member joined group"
        );

        // Trigger rebalance
        self.trigger_rebalance_for_group(group);

        HeartbeatResponse::join_success(member_id, 1, self.config.heartbeat_interval_ms)
    }

    /// Handle leave request
    fn handle_leave(&self, request: HeartbeatRequest) -> HeartbeatResponse {
        let mut groups = self.groups.write();

        let group = match groups.get_mut(&request.group_id) {
            Some(g) => g,
            None => {
                return HeartbeatResponse::error(Kip848Error::CoordinatorNotAvailable);
            }
        };

        // Remove member
        if group.remove_member(&request.member_id).is_some() {
            info!(
                group_id = %request.group_id,
                member_id = %request.member_id,
                "Member left group"
            );

            // Trigger rebalance if group still has members
            if !group.members.is_empty() {
                self.trigger_rebalance_for_group(group);
            }
        }

        HeartbeatResponse::success(request.member_epoch, self.config.heartbeat_interval_ms)
    }

    /// Handle regular heartbeat
    fn handle_heartbeat(&self, request: HeartbeatRequest) -> HeartbeatResponse {
        let mut groups = self.groups.write();

        let group = match groups.get_mut(&request.group_id) {
            Some(g) => g,
            None => {
                return HeartbeatResponse::error(Kip848Error::CoordinatorNotAvailable);
            }
        };

        // First check if member exists and validate epoch
        {
            let member = match group.get_member(&request.member_id) {
                Some(m) => m,
                None => {
                    return HeartbeatResponse::error(Kip848Error::UnknownMemberId);
                }
            };

            // Check epoch
            if request.member_epoch < member.member_epoch {
                return HeartbeatResponse::error(Kip848Error::StaleMemberEpoch);
            }

            if request.member_epoch > member.member_epoch {
                warn!(
                    group_id = %request.group_id,
                    member_id = %request.member_id,
                    expected = member.member_epoch,
                    received = request.member_epoch,
                    "Member epoch mismatch"
                );
            }
        }

        // Get target assignment if any (clone to avoid borrow issues)
        let target_assignment = group.target_assignments.get(&request.member_id).cloned();

        // Now get mutable reference and update member
        // Safety: We already verified the member exists above, but check again for robustness
        let member = match group.get_member_mut(&request.member_id) {
            Some(m) => m,
            None => {
                return HeartbeatResponse::error(Kip848Error::UnknownMemberId);
            }
        };

        // Update heartbeat timestamp
        member.touch();

        // Update current assignment from request
        let reported_partitions = request.as_topic_partitions();
        member.current_assignment =
            super::assignment::CurrentAssignment::from_partitions(reported_partitions.as_list());

        // Determine response based on member state
        let member_state = member.state;
        let member_epoch = member.member_epoch;

        match member_state {
            MemberState::Stable => {
                // Check if there's a new target assignment pending
                if let Some(ref target) = target_assignment {
                    let delta = AssignmentDelta::compute(&member.current_assignment, target);
                    if !delta.is_empty() {
                        // Member needs to reconcile
                        member.transition_to_reconciling(target.clone());
                        return self.create_reconcile_response(member);
                    }
                }

                // No changes needed
                HeartbeatResponse::success(member_epoch, self.config.heartbeat_interval_ms)
            }

            MemberState::Reconciling => {
                // Check if reconciliation is complete
                if member.is_reconciliation_complete() {
                    member.transition_to_stable();
                    let final_epoch = member.member_epoch;
                    debug!(
                        group_id = %request.group_id,
                        member_id = %request.member_id,
                        "Member reconciliation complete"
                    );

                    // Check if all members are stable
                    let all_stable = group
                        .members
                        .values()
                        .all(|m| m.state == MemberState::Stable);
                    if all_stable && group.state == GroupStateKip848::Reconciling {
                        group.transition_to_stable();
                        info!(group_id = %request.group_id, "Group reconciliation complete");
                    }

                    HeartbeatResponse::success(final_epoch, self.config.heartbeat_interval_ms)
                } else {
                    // Continue reconciliation
                    self.create_reconcile_response(member)
                }
            }

            MemberState::Joining => {
                // Member is still joining, wait for assignment
                HeartbeatResponse::success(member_epoch, self.config.heartbeat_interval_ms)
            }

            MemberState::Leaving => {
                // Member is leaving, just acknowledge
                HeartbeatResponse::success(member_epoch, self.config.heartbeat_interval_ms)
            }

            MemberState::Fenced => HeartbeatResponse::error(Kip848Error::FencedMemberEpoch),
        }
    }

    /// Create a response with reconciliation instructions
    fn create_reconcile_response(
        &self,
        member: &super::member::ConsumerMember,
    ) -> HeartbeatResponse {
        let to_revoke = member.get_pending_revocations();
        let to_assign = member.get_pending_assignments();

        // Convert to TopicAssignment format
        let mut revoke_map: HashMap<String, Vec<i32>> = HashMap::new();
        for (topic, partition) in to_revoke {
            revoke_map.entry(topic).or_default().push(partition);
        }

        let mut assign_map: HashMap<String, Vec<i32>> = HashMap::new();
        for (topic, partition) in to_assign {
            assign_map.entry(topic).or_default().push(partition);
        }

        // Build assignment info
        let assignment = AssignmentInfo {
            error_code: 0,
            topic_partitions: topic_partitions_to_assignments(&member.target_assignment.partitions),
        };

        HeartbeatResponse::success_with_assignment(
            member.member_epoch,
            self.config.heartbeat_interval_ms,
            assignment,
        )
    }

    /// Trigger a rebalance for a group
    fn trigger_rebalance_for_group(&self, group: &mut ConsumerGroupKip848) {
        let topics = self.topic_partitions.read();

        // Get assignor - try requested assignor, then default, then first available
        let assignor = self
            .assignor_registry
            .get(&group.assignor)
            .or_else(|| self.assignor_registry.get_default());

        let Some(assignor) = assignor else {
            warn!(
                group_id = %group.group_id,
                "No assignor available - registry is empty, skipping rebalance"
            );
            return;
        };

        // Compute new assignments
        let assignments = assignor.assign(group, &topics);

        // Apply assignments
        group.set_target_assignments(assignments);

        info!(
            group_id = %group.group_id,
            epoch = group.group_epoch,
            assignment_epoch = group.assignment_epoch,
            member_count = group.member_count(),
            "Triggered rebalance"
        );
    }

    /// Trigger rebalance for a specific group by ID
    pub fn trigger_rebalance(&self, group_id: &str) -> bool {
        let mut groups = self.groups.write();
        if let Some(group) = groups.get_mut(group_id) {
            self.trigger_rebalance_for_group(group);
            true
        } else {
            false
        }
    }

    /// Get group info
    pub fn get_group(&self, group_id: &str) -> Option<GroupInfo> {
        let groups = self.groups.read();
        groups.get(group_id).map(|g| GroupInfo {
            group_id: g.group_id.clone(),
            state: g.state,
            group_epoch: g.group_epoch,
            assignment_epoch: g.assignment_epoch,
            member_count: g.member_count(),
            assignor: g.assignor.clone(),
        })
    }

    /// List all groups
    pub fn list_groups(&self) -> Vec<String> {
        let groups = self.groups.read();
        groups.keys().cloned().collect()
    }

    /// Get expired members across all groups
    pub fn get_expired_members(&self) -> Vec<(String, Vec<MemberId>)> {
        let groups = self.groups.read();
        groups
            .iter()
            .map(|(group_id, group)| (group_id.clone(), group.get_expired_members()))
            .filter(|(_, members)| !members.is_empty())
            .collect()
    }

    /// Remove expired members and trigger rebalance if needed
    pub fn cleanup_expired_members(&self) {
        let expired = self.get_expired_members();

        let mut groups = self.groups.write();

        for (group_id, member_ids) in expired {
            if let Some(group) = groups.get_mut(&group_id) {
                for member_id in member_ids {
                    if group.remove_member(&member_id).is_some() {
                        info!(
                            group_id = %group_id,
                            member_id = %member_id,
                            "Removed expired member"
                        );
                    }
                }

                // Trigger rebalance if group still has members
                if !group.members.is_empty() {
                    self.trigger_rebalance_for_group(group);
                }
            }
        }
    }
}

/// Summary information about a group
#[derive(Debug, Clone)]
pub struct GroupInfo {
    /// Group ID
    pub group_id: String,
    /// Current state
    pub state: GroupStateKip848,
    /// Group epoch
    pub group_epoch: i32,
    /// Assignment epoch
    pub assignment_epoch: i32,
    /// Number of members
    pub member_count: usize,
    /// Assignor name
    pub assignor: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_engine() -> ReconciliationEngine {
        let engine = ReconciliationEngine::new_default();
        engine.update_topic_partitions("topic-1", 4);
        engine.update_topic_partitions("topic-2", 4);
        engine
    }

    #[test]
    fn test_join_creates_group() {
        let engine = create_engine();

        let request = HeartbeatRequest::join("test-group", vec!["topic-1".to_string()], 300000);

        let response = engine.process_heartbeat(request);

        assert!(!response.is_error());
        assert!(response.member_id.is_some());
        assert_eq!(response.member_epoch, 1);

        let group_info = engine.get_group("test-group");
        assert!(group_info.is_some());
        assert_eq!(group_info.unwrap().member_count, 1);
    }

    #[test]
    fn test_multiple_members_join() {
        let engine = create_engine();

        // First member joins
        let request1 = HeartbeatRequest::join("test-group", vec!["topic-1".to_string()], 300000);
        let response1 = engine.process_heartbeat(request1);
        assert!(!response1.is_error());

        // Second member joins
        let request2 = HeartbeatRequest::join("test-group", vec!["topic-1".to_string()], 300000);
        let response2 = engine.process_heartbeat(request2);
        assert!(!response2.is_error());

        // Check group has 2 members
        let group_info = engine.get_group("test-group").unwrap();
        assert_eq!(group_info.member_count, 2);
        assert_eq!(group_info.state, GroupStateKip848::Reconciling);
    }

    #[test]
    fn test_member_leave() {
        let engine = create_engine();

        // Join
        let join_request =
            HeartbeatRequest::join("test-group", vec!["topic-1".to_string()], 300000);
        let join_response = engine.process_heartbeat(join_request);
        let member_id = join_response.member_id.unwrap();

        // Leave
        let mut leave_request = HeartbeatRequest::heartbeat(
            "test-group",
            &member_id,
            join_response.member_epoch,
            vec![],
        );
        leave_request.member_epoch = -1; // Leave signal

        let leave_response = engine.process_heartbeat(leave_request);
        assert!(!leave_response.is_error());

        // Group should be empty now
        let group_info = engine.get_group("test-group").unwrap();
        assert_eq!(group_info.member_count, 0);
        assert_eq!(group_info.state, GroupStateKip848::Empty);
    }

    #[test]
    fn test_unknown_member() {
        let engine = create_engine();

        // Create group with one member
        let join_request =
            HeartbeatRequest::join("test-group", vec!["topic-1".to_string()], 300000);
        engine.process_heartbeat(join_request);

        // Heartbeat from unknown member
        let request = HeartbeatRequest::heartbeat("test-group", "unknown-member", 1, vec![]);

        let response = engine.process_heartbeat(request);
        assert!(response.is_error());
        assert_eq!(response.get_error(), Some(Kip848Error::UnknownMemberId));
    }

    #[test]
    fn test_trigger_rebalance() {
        let engine = create_engine();

        // Create group
        let request = HeartbeatRequest::join("test-group", vec!["topic-1".to_string()], 300000);
        engine.process_heartbeat(request);

        // Trigger rebalance
        assert!(engine.trigger_rebalance("test-group"));
        assert!(!engine.trigger_rebalance("nonexistent-group"));
    }

    #[test]
    fn test_list_groups() {
        let engine = create_engine();

        // Create two groups
        engine.process_heartbeat(HeartbeatRequest::join(
            "group-1",
            vec!["topic-1".to_string()],
            300000,
        ));
        engine.process_heartbeat(HeartbeatRequest::join(
            "group-2",
            vec!["topic-1".to_string()],
            300000,
        ));

        let groups = engine.list_groups();
        assert_eq!(groups.len(), 2);
        assert!(groups.contains(&"group-1".to_string()));
        assert!(groups.contains(&"group-2".to_string()));
    }
}
