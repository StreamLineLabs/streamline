//! KIP-848 Consumer Group Heartbeat
//!
//! Implements the ConsumerGroupHeartbeat request/response handling for the new protocol.

use super::assignment::TopicPartitions;
use super::errors::Kip848Error;
use super::member::MemberEpoch;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A topic partition assignment for heartbeat messages
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TopicAssignment {
    /// Topic name
    pub topic_name: String,
    /// Assigned partition indices
    pub partitions: Vec<i32>,
}

/// Consumer Group Heartbeat Request (API Key 68)
///
/// Sent by consumers to maintain group membership and receive assignment updates.
#[derive(Debug, Clone, Default)]
pub struct HeartbeatRequest {
    /// Consumer group ID
    pub group_id: String,

    /// Member ID (empty string for initial join)
    pub member_id: String,

    /// Current member epoch (0 for initial join)
    pub member_epoch: MemberEpoch,

    /// Instance ID for static membership (optional)
    pub instance_id: Option<String>,

    /// Rack ID for rack-aware assignment (optional)
    pub rack_id: Option<String>,

    /// Rebalance timeout in milliseconds
    pub rebalance_timeout_ms: i32,

    /// Topics the consumer wants to subscribe to
    pub subscribed_topic_names: Vec<String>,

    /// Regex pattern for topic subscription (optional)
    pub subscribed_topic_regex: Option<String>,

    /// Server-side assignor to use (optional, uses group default if not specified)
    pub server_assignor: Option<String>,

    /// Topic partitions currently owned by this member
    pub topic_partitions: Vec<TopicAssignment>,
}

impl HeartbeatRequest {
    /// Create a new join request (initial heartbeat)
    pub fn join(
        group_id: impl Into<String>,
        subscribed_topics: Vec<String>,
        rebalance_timeout_ms: i32,
    ) -> Self {
        Self {
            group_id: group_id.into(),
            member_id: String::new(),
            member_epoch: 0,
            subscribed_topic_names: subscribed_topics,
            rebalance_timeout_ms,
            ..Default::default()
        }
    }

    /// Create a heartbeat request for an existing member
    pub fn heartbeat(
        group_id: impl Into<String>,
        member_id: impl Into<String>,
        member_epoch: MemberEpoch,
        current_partitions: Vec<TopicAssignment>,
    ) -> Self {
        Self {
            group_id: group_id.into(),
            member_id: member_id.into(),
            member_epoch,
            topic_partitions: current_partitions,
            ..Default::default()
        }
    }

    /// Check if this is an initial join request
    pub fn is_join(&self) -> bool {
        self.member_id.is_empty() && self.member_epoch == 0
    }

    /// Check if this is a leave request
    pub fn is_leave(&self) -> bool {
        self.member_epoch == -1
    }

    /// Convert topic partitions to TopicPartitions type
    pub fn as_topic_partitions(&self) -> TopicPartitions {
        let mut tp = TopicPartitions::empty();
        for assignment in &self.topic_partitions {
            for partition in &assignment.partitions {
                tp.add((assignment.topic_name.clone(), *partition));
            }
        }
        tp
    }
}

/// Assignment information in heartbeat response
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AssignmentInfo {
    /// Error code for this assignment (0 = no error)
    pub error_code: i16,

    /// Topic partitions to assign
    pub topic_partitions: Vec<TopicAssignment>,
}

/// Consumer Group Heartbeat Response (API Key 68)
///
/// Response to heartbeat request with assignment updates.
#[derive(Debug, Clone, Default)]
pub struct HeartbeatResponse {
    /// Throttle time in milliseconds
    pub throttle_time_ms: i32,

    /// Top-level error code
    pub error_code: i16,

    /// Error message (if error_code != 0)
    pub error_message: Option<String>,

    /// Assigned member ID (assigned by server on join)
    pub member_id: Option<String>,

    /// Member epoch after this heartbeat
    pub member_epoch: MemberEpoch,

    /// Time in milliseconds until next heartbeat is required
    pub heartbeat_interval_ms: i32,

    /// Current assignment for this member
    pub assignment: Option<AssignmentInfo>,
}

impl HeartbeatResponse {
    /// Create an error response
    pub fn error(error: Kip848Error) -> Self {
        Self {
            error_code: error.code(),
            error_message: Some(error.to_string()),
            heartbeat_interval_ms: 5000, // Default interval even on error
            ..Default::default()
        }
    }

    /// Create a successful join response
    pub fn join_success(
        member_id: impl Into<String>,
        member_epoch: MemberEpoch,
        heartbeat_interval_ms: i32,
    ) -> Self {
        Self {
            error_code: 0,
            member_id: Some(member_id.into()),
            member_epoch,
            heartbeat_interval_ms,
            ..Default::default()
        }
    }

    /// Create a successful heartbeat response with assignment
    pub fn success_with_assignment(
        member_epoch: MemberEpoch,
        heartbeat_interval_ms: i32,
        assignment: AssignmentInfo,
    ) -> Self {
        Self {
            error_code: 0,
            member_epoch,
            heartbeat_interval_ms,
            assignment: Some(assignment),
            ..Default::default()
        }
    }

    /// Create a successful heartbeat response without assignment change
    pub fn success(member_epoch: MemberEpoch, heartbeat_interval_ms: i32) -> Self {
        Self {
            error_code: 0,
            member_epoch,
            heartbeat_interval_ms,
            ..Default::default()
        }
    }

    /// Check if response indicates an error
    pub fn is_error(&self) -> bool {
        self.error_code != 0
    }

    /// Get the Kip848Error if this is an error response
    pub fn get_error(&self) -> Option<Kip848Error> {
        if self.error_code == 0 {
            None
        } else {
            Kip848Error::from_code(self.error_code)
        }
    }
}

/// Convert TopicPartitions to Vec<TopicAssignment>
pub fn topic_partitions_to_assignments(tp: &TopicPartitions) -> Vec<TopicAssignment> {
    let mut result: HashMap<String, Vec<i32>> = HashMap::new();

    for (topic, partition) in tp.as_list() {
        result.entry(topic).or_default().push(partition);
    }

    result
        .into_iter()
        .map(|(topic_name, mut partitions)| {
            partitions.sort();
            TopicAssignment {
                topic_name,
                partitions,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_join_request() {
        let request = HeartbeatRequest::join(
            "test-group",
            vec!["topic-1".to_string(), "topic-2".to_string()],
            300000,
        );

        assert!(request.is_join());
        assert!(!request.is_leave());
        assert_eq!(request.group_id, "test-group");
        assert_eq!(request.subscribed_topic_names.len(), 2);
    }

    #[test]
    fn test_heartbeat_request() {
        let partitions = vec![TopicAssignment {
            topic_name: "topic-1".to_string(),
            partitions: vec![0, 1, 2],
        }];

        let request = HeartbeatRequest::heartbeat("test-group", "member-1", 5, partitions);

        assert!(!request.is_join());
        assert!(!request.is_leave());
        assert_eq!(request.member_id, "member-1");
        assert_eq!(request.member_epoch, 5);
    }

    #[test]
    fn test_leave_request() {
        let mut request = HeartbeatRequest::heartbeat("test-group", "member-1", 5, vec![]);
        request.member_epoch = -1;

        assert!(request.is_leave());
    }

    #[test]
    fn test_error_response() {
        let response = HeartbeatResponse::error(Kip848Error::FencedMemberEpoch);

        assert!(response.is_error());
        assert_eq!(response.error_code, 110);
        assert_eq!(response.get_error(), Some(Kip848Error::FencedMemberEpoch));
    }

    #[test]
    fn test_success_response() {
        let response = HeartbeatResponse::success(5, 3000);

        assert!(!response.is_error());
        assert_eq!(response.member_epoch, 5);
        assert_eq!(response.heartbeat_interval_ms, 3000);
    }

    #[test]
    fn test_topic_partitions_to_assignments() {
        let mut tp = TopicPartitions::empty();
        tp.add(("topic-1".to_string(), 0));
        tp.add(("topic-1".to_string(), 1));
        tp.add(("topic-2".to_string(), 0));

        let assignments = topic_partitions_to_assignments(&tp);

        assert_eq!(assignments.len(), 2);

        let topic1 = assignments
            .iter()
            .find(|a| a.topic_name == "topic-1")
            .unwrap();
        assert_eq!(topic1.partitions, vec![0, 1]);

        let topic2 = assignments
            .iter()
            .find(|a| a.topic_name == "topic-2")
            .unwrap();
        assert_eq!(topic2.partitions, vec![0]);
    }
}
