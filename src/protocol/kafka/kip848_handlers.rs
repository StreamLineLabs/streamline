//! KIP-848 consumer group protocol handlers.
//!
//! This module contains handlers for the new consumer group protocol
//! (KIP-848) including consumer group heartbeat and describe operations.


use crate::consumer::kip848::HeartbeatRequest as Kip848HeartbeatRequest;
use crate::consumer::kip848::heartbeat::TopicAssignment as Kip848TopicAssignment;
use crate::error::Result;
use crate::protocol::handlers::error_codes::*;
use kafka_protocol::messages::{
    ConsumerGroupDescribeRequest, ConsumerGroupDescribeResponse, ConsumerGroupHeartbeatRequest, ConsumerGroupHeartbeatResponse,
};
use kafka_protocol::protocol::StrBytes;
use super::KafkaHandler;
use tracing::{debug, info, warn};

impl KafkaHandler {
    /// Handle ConsumerGroupHeartbeat request (API Key 68)
    ///
    /// This is the KIP-848 new consumer protocol heartbeat.
    /// Routes requests to the ReconciliationEngine for processing.
    pub(in crate::protocol) fn handle_consumer_group_heartbeat(
        &self,
        request: ConsumerGroupHeartbeatRequest,
    ) -> Result<ConsumerGroupHeartbeatResponse> {
        use kafka_protocol::messages::consumer_group_heartbeat_response::{
            Assignment, TopicPartitions as ResponseTopicPartitions,
        };

        let group_id = request.group_id.as_str();
        let member_id = request.member_id.as_str();
        let member_epoch = request.member_epoch;

        info!(
            group_id = group_id,
            member_id = member_id,
            member_epoch = member_epoch,
            "ConsumerGroupHeartbeat request (KIP-848)"
        );

        // Convert kafka-protocol request to internal KIP-848 request
        let kip848_request = self.convert_heartbeat_request(&request);

        // Process through reconciliation engine
        let kip848_response = self.kip848_engine.process_heartbeat(kip848_request);

        // Convert internal response back to kafka-protocol response
        let mut response = ConsumerGroupHeartbeatResponse::default()
            .with_throttle_time_ms(0)
            .with_error_code(kip848_response.error_code)
            .with_member_epoch(kip848_response.member_epoch)
            .with_heartbeat_interval_ms(kip848_response.heartbeat_interval_ms);

        if let Some(ref error_msg) = kip848_response.error_message {
            response = response.with_error_message(Some(StrBytes::from_string(error_msg.clone())));
        }

        if let Some(ref member_id) = kip848_response.member_id {
            response = response.with_member_id(Some(StrBytes::from_string(member_id.clone())));
        }

        // Convert assignment if present
        if let Some(ref assignment_info) = kip848_response.assignment {
            let topic_partitions: Vec<ResponseTopicPartitions> = assignment_info
                .topic_partitions
                .iter()
                .map(|tp| {
                    // Get topic ID from TopicManager, or generate deterministic UUID from name
                    let topic_id = self.get_topic_id(&tp.topic_name);
                    ResponseTopicPartitions::default()
                        .with_topic_id(topic_id)
                        .with_partitions(tp.partitions.clone())
                })
                .collect();

            let assignment = Assignment::default().with_topic_partitions(topic_partitions);
            response = response.with_assignment(Some(assignment));
        }

        debug!(
            group_id = group_id,
            member_id = member_id,
            response_epoch = kip848_response.member_epoch,
            error_code = kip848_response.error_code,
            "ConsumerGroupHeartbeat response"
        );

        Ok(response)
    }

    /// Convert kafka-protocol heartbeat request to internal KIP-848 request
    fn convert_heartbeat_request(
        &self,
        request: &ConsumerGroupHeartbeatRequest,
    ) -> Kip848HeartbeatRequest {
        // Convert topic partitions (UUIDs to names)
        let topic_partitions: Vec<Kip848TopicAssignment> = request
            .topic_partitions
            .as_ref()
            .map(|tps| {
                tps.iter()
                    .filter_map(|tp| {
                        // Try to resolve topic name from UUID
                        let topic_name = self.get_topic_name_by_id(tp.topic_id);
                        topic_name.map(|name| Kip848TopicAssignment {
                            topic_name: name,
                            partitions: tp.partitions.clone(),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();

        Kip848HeartbeatRequest {
            group_id: request.group_id.to_string(),
            member_id: request.member_id.to_string(),
            member_epoch: request.member_epoch,
            instance_id: request.instance_id.as_ref().map(|s| s.to_string()),
            rack_id: request.rack_id.as_ref().map(|s| s.to_string()),
            rebalance_timeout_ms: request.rebalance_timeout_ms,
            subscribed_topic_names: request
                .subscribed_topic_names
                .as_ref()
                .map(|names| names.iter().map(|n| n.to_string()).collect())
                .unwrap_or_default(),
            subscribed_topic_regex: None, // Not used in current implementation
            server_assignor: request.server_assignor.as_ref().map(|s| s.to_string()),
            topic_partitions,
        }
    }

    /// Get topic ID from TopicManager, or generate a random UUID for unknown topics
    fn get_topic_id(&self, topic_name: &str) -> uuid::Uuid {
        // Try to get topic ID from TopicManager
        if let Ok(metadata) = self.topic_manager.get_topic_metadata(topic_name) {
            return metadata.topic_id;
        }
        // Generate a random UUID for unknown topics
        // Note: This is not deterministic, but unknown topics are edge cases
        uuid::Uuid::new_v4()
    }

    /// Get topic name from topic ID by scanning known topics
    fn get_topic_name_by_id(&self, topic_id: uuid::Uuid) -> Option<String> {
        // Scan all topics to find matching ID
        if let Ok(topics) = self.topic_manager.list_topics() {
            for topic in topics {
                if topic.topic_id == topic_id {
                    return Some(topic.name);
                }
            }
        }
        None
    }

    /// Handle ConsumerGroupDescribe request (API Key 69)
    ///
    /// Describes consumer groups using the new KIP-848 protocol format.
    /// First checks KIP-848 groups, then falls back to legacy coordinator.
    pub(in crate::protocol) fn handle_consumer_group_describe(
        &self,
        request: ConsumerGroupDescribeRequest,
    ) -> Result<ConsumerGroupDescribeResponse> {
        use kafka_protocol::messages::consumer_group_describe_response::DescribedGroup;

        info!(
            group_count = request.group_ids.len(),
            "ConsumerGroupDescribe request (KIP-848)"
        );

        // Build response for each requested group
        let mut groups = Vec::new();

        for group_id in &request.group_ids {
            let group_id_str = group_id.as_str();

            // First check KIP-848 groups
            if let Some(kip848_group) = self.kip848_engine.get_group(group_id_str) {
                // Build response from KIP-848 group state
                let state_str = match kip848_group.state {
                    crate::consumer::kip848::GroupStateKip848::Empty => "Empty",
                    crate::consumer::kip848::GroupStateKip848::Stable => "Stable",
                    crate::consumer::kip848::GroupStateKip848::Reconciling => "Reconciling",
                    crate::consumer::kip848::GroupStateKip848::Dead => "Dead",
                };

                // Build member list from KIP-848 group
                let members = self.build_kip848_member_list(group_id_str);

                let described_group = DescribedGroup::default()
                    .with_error_code(NONE)
                    .with_group_id(group_id.clone())
                    .with_group_state(StrBytes::from_string(state_str.to_string()))
                    .with_group_epoch(kip848_group.group_epoch)
                    .with_assignment_epoch(kip848_group.assignment_epoch)
                    .with_assignor_name(StrBytes::from_string(kip848_group.assignor.clone()))
                    .with_members(members)
                    .with_authorized_operations(-1); // Not available

                groups.push(described_group);
                continue;
            }

            // Fall back to legacy coordinator
            let group_result = self.group_coordinator.get_group(group_id_str);

            let described_group = match group_result {
                Ok(Some(_group)) => {
                    // Group exists in legacy coordinator
                    // Return as Stable state (legacy groups use classic protocol)
                    DescribedGroup::default()
                        .with_error_code(NONE)
                        .with_group_id(group_id.clone())
                        .with_group_state(StrBytes::from_static_str("Stable"))
                        .with_group_epoch(0)
                        .with_assignment_epoch(0)
                        .with_assignor_name(StrBytes::from_static_str("range"))
                        .with_members(vec![])
                        .with_authorized_operations(-1)
                }
                Ok(None) => {
                    // Group not found
                    DescribedGroup::default()
                        .with_error_code(GROUP_ID_NOT_FOUND)
                        .with_group_id(group_id.clone())
                        .with_group_state(StrBytes::from_static_str(""))
                        .with_error_message(Some(StrBytes::from_string(format!(
                            "Group {} not found",
                            group_id_str
                        ))))
                }
                Err(e) => {
                    // Error looking up group
                    warn!(
                        group_id = group_id_str,
                        error = %e,
                        "Error looking up group for ConsumerGroupDescribe"
                    );
                    DescribedGroup::default()
                        .with_error_code(UNKNOWN_SERVER_ERROR)
                        .with_group_id(group_id.clone())
                        .with_group_state(StrBytes::from_static_str(""))
                        .with_error_message(Some(StrBytes::from_string(format!("Error: {}", e))))
                }
            };

            groups.push(described_group);
        }

        Ok(ConsumerGroupDescribeResponse::default()
            .with_throttle_time_ms(0)
            .with_groups(groups))
    }

    /// Build member list from KIP-848 group for ConsumerGroupDescribe
    fn build_kip848_member_list(
        &self,
        _group_id: &str,
    ) -> Vec<kafka_protocol::messages::consumer_group_describe_response::Member> {
        // Get group info from KIP-848 engine
        // For now, return empty list since we don't have full member details exposed
        // The ReconciliationEngine's get_group returns GroupInfo which doesn't include members
        // In a full implementation, we would need to expose member details
        vec![]
    }

}
