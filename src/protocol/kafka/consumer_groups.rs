//! Consumer group handlers for the Kafka protocol.
//!
//! This module contains handlers for consumer group coordination including
//! group joining, syncing, heartbeats, offset management, and group administration.


use bytes::Bytes;
use crate::error::Result;
use crate::protocol::handlers::error_codes::*;
use kafka_protocol::messages::GroupId;
use kafka_protocol::messages::TopicName;
use kafka_protocol::messages::{
    DeleteGroupsRequest, DeleteGroupsResponse, DescribeGroupsRequest, DescribeGroupsResponse, FindCoordinatorRequest, FindCoordinatorResponse, HeartbeatRequest, HeartbeatResponse, JoinGroupRequest, JoinGroupResponse, LeaveGroupRequest, LeaveGroupResponse, ListGroupsRequest, ListGroupsResponse, OffsetCommitRequest, OffsetCommitResponse, OffsetFetchRequest, OffsetFetchResponse, SyncGroupRequest, SyncGroupResponse,
};
use kafka_protocol::protocol::StrBytes;
use super::KafkaHandler;
use tracing::{debug, error, info, warn};

impl KafkaHandler {
    /// Handle FindCoordinator request
    pub(in crate::protocol) fn handle_find_coordinator(
        &self,
        request: FindCoordinatorRequest,
        api_version: i16,
    ) -> Result<FindCoordinatorResponse> {
        use kafka_protocol::messages::find_coordinator_response::Coordinator;
        use kafka_protocol::messages::BrokerId;

        debug!(
            api_version = api_version,
            coordinator_keys = ?request.coordinator_keys,
            key = ?request.key,
            key_type = request.key_type,
            "FindCoordinator request received"
        );

        // Since we're single-node, we are the coordinator for all groups
        if api_version >= 4 {
            // Version 4+ uses the coordinators array
            let keys: Vec<StrBytes> = request.coordinator_keys.clone();

            let host = StrBytes::from_string(self.advertised_addr.ip().to_string());
            let port = self.advertised_addr.port() as i32;

            let coordinators: Vec<Coordinator> = keys
                .into_iter()
                .map(|key| {
                    Coordinator::default()
                        .with_key(key)
                        .with_node_id(BrokerId::from(self.node_id))
                        .with_host(host.clone())
                        .with_port(port)
                        .with_error_code(NONE)
                        .with_error_message(None)
                })
                .collect();

            debug!(
                coordinators_count = coordinators.len(),
                node_id = self.node_id,
                "FindCoordinator v4+ response"
            );
            Ok(FindCoordinatorResponse::default()
                .with_throttle_time_ms(0)
                .with_coordinators(coordinators))
        } else if api_version >= 1 {
            // Version 1-3 uses top-level fields with throttle_time_ms
            Ok(FindCoordinatorResponse::default()
                .with_throttle_time_ms(0)
                .with_error_code(NONE)
                .with_node_id(BrokerId::from(self.node_id))
                .with_host(StrBytes::from_string(self.advertised_addr.ip().to_string()))
                .with_port(self.advertised_addr.port() as i32))
        } else {
            // Version 0 uses top-level fields without throttle_time_ms
            Ok(FindCoordinatorResponse::default()
                .with_error_code(NONE)
                .with_node_id(BrokerId::from(self.node_id))
                .with_host(StrBytes::from_string(self.advertised_addr.ip().to_string()))
                .with_port(self.advertised_addr.port() as i32))
        }
    }

    /// Handle JoinGroup request
    pub(in crate::protocol) fn handle_join_group(
        &self,
        request: JoinGroupRequest,
        api_version: i16,
    ) -> Result<JoinGroupResponse> {
        use kafka_protocol::messages::join_group_response::JoinGroupResponseMember;

        let group_id = request.group_id.as_str();
        let member_id = if request.member_id.is_empty() {
            None
        } else {
            Some(request.member_id.as_str())
        };
        // protocol_type is a StrBytes, not an Option
        let protocol_type = if request.protocol_type.is_empty() {
            "consumer"
        } else {
            request.protocol_type.as_str()
        };

        // Extract subscriptions and protocol metadata from protocols
        let (subscriptions, protocol_metadata, protocol_name) =
            Self::parse_join_group_protocols(&request.protocols);

        let result = self.group_coordinator.join_group(
            group_id,
            member_id,
            "kafka-client", // client_id - ideally from header, use placeholder
            "localhost",
            request.session_timeout_ms,
            request.rebalance_timeout_ms,
            protocol_type,  // protocol_type (e.g., "consumer")
            &protocol_name, // protocol_name (e.g., "range", "roundrobin")
            subscriptions,
            protocol_metadata.clone(),
        )?;

        let (assigned_member_id, generation_id, leader_id, _members) = result;

        // Create member list for leader - includes metadata for partition assignment
        let member_list: Vec<JoinGroupResponseMember> = if assigned_member_id == leader_id {
            // Leader gets full member info with metadata
            self.group_coordinator
                .get_group(group_id)?
                .map(|group| {
                    group
                        .members
                        .iter()
                        .map(|(mid, member)| {
                            JoinGroupResponseMember::default()
                                .with_member_id(StrBytes::from_string(mid.clone()))
                                .with_metadata(Bytes::from(member.protocol_metadata.clone()))
                        })
                        .collect()
                })
                .unwrap_or_default()
        } else {
            // Non-leaders get empty member list
            vec![]
        };

        let mut response = JoinGroupResponse::default()
            .with_error_code(NONE)
            .with_generation_id(generation_id)
            .with_protocol_name(Some(StrBytes::from_string(protocol_name.clone())))
            .with_member_id(StrBytes::from_string(assigned_member_id))
            .with_leader(StrBytes::from_string(leader_id))
            .with_members(member_list);

        // v2+: throttle_time_ms
        if api_version >= 2 {
            response = response.with_throttle_time_ms(0);
        }

        // v7+: protocol_type
        if api_version >= 7 {
            response =
                response.with_protocol_type(Some(StrBytes::from_string(protocol_type.to_string())));
        }

        Ok(response)
    }

    /// Parse JoinGroup protocols to extract subscriptions and protocol metadata
    fn parse_join_group_protocols(
        protocols: &[kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol],
    ) -> (Vec<String>, Vec<u8>, String) {
        // Default to range assignor
        let mut protocol_name = "range".to_string();
        let mut subscriptions = Vec::new();
        let mut protocol_metadata = Vec::new();

        // Find the first supported protocol (prefer range, then roundrobin)
        for protocol in protocols {
            let name = protocol.name.as_str();
            if name == "range" || name == "roundrobin" {
                protocol_name = name.to_string();
                protocol_metadata = protocol.metadata.to_vec();

                // Parse subscription from metadata
                // Kafka consumer protocol subscription format:
                // version (2 bytes) + topic count (4 bytes) + [topic name length (2 bytes) + topic name]* + user data length (4 bytes) + user data
                if protocol.metadata.len() >= 6 {
                    let metadata = &protocol.metadata[..];
                    let mut offset = 2; // Skip version

                    // Read topic count (4 bytes big-endian, but Kafka uses compact array with varint in newer versions)
                    // For simplicity, try reading as 2-byte count first (older protocol)
                    if offset + 2 <= metadata.len() {
                        let topic_count =
                            u16::from_be_bytes([metadata[offset], metadata[offset + 1]]) as usize;
                        offset += 2;

                        for _ in 0..topic_count {
                            if offset + 2 > metadata.len() {
                                break;
                            }
                            let topic_len =
                                u16::from_be_bytes([metadata[offset], metadata[offset + 1]])
                                    as usize;
                            offset += 2;

                            if offset + topic_len > metadata.len() {
                                break;
                            }
                            if let Ok(topic) =
                                std::str::from_utf8(&metadata[offset..offset + topic_len])
                            {
                                subscriptions.push(topic.to_string());
                            }
                            offset += topic_len;
                        }
                    }
                }
                break;
            }
        }

        (subscriptions, protocol_metadata, protocol_name)
    }

    /// Handle Heartbeat request
    pub(in crate::protocol) fn handle_heartbeat(
        &self,
        request: HeartbeatRequest,
        api_version: i16,
    ) -> Result<HeartbeatResponse> {
        let group_id = request.group_id.as_str();
        let member_id = request.member_id.as_str();
        let generation_id = request.generation_id;

        let error_code = match self
            .group_coordinator
            .heartbeat(group_id, member_id, generation_id)
        {
            Ok(_) => NONE,
            Err(_) => REBALANCE_IN_PROGRESS,
        };

        let mut response = HeartbeatResponse::default().with_error_code(error_code);

        // v1+: throttle_time_ms
        if api_version >= 1 {
            response = response.with_throttle_time_ms(0);
        }

        Ok(response)
    }

    /// Handle LeaveGroup request
    pub(in crate::protocol) fn handle_leave_group(
        &self,
        request: LeaveGroupRequest,
        api_version: i16,
    ) -> Result<LeaveGroupResponse> {
        use kafka_protocol::messages::leave_group_response::MemberResponse;

        let group_id = request.group_id.as_str();
        let mut member_responses = Vec::new();

        let mut response = if api_version >= 3 {
            // Version 3+ uses members array
            for member in &request.members {
                let member_id = member.member_id.as_str();
                let _ = self.group_coordinator.leave_group(group_id, member_id);
                member_responses.push(
                    MemberResponse::default()
                        .with_member_id(member.member_id.clone())
                        .with_error_code(NONE),
                );
            }
            LeaveGroupResponse::default()
                .with_error_code(NONE)
                .with_members(member_responses)
        } else {
            // Version 0-2 uses member_id field
            if !request.member_id.is_empty() {
                let _ = self
                    .group_coordinator
                    .leave_group(group_id, request.member_id.as_str());
            }
            LeaveGroupResponse::default().with_error_code(NONE)
        };

        // v1+: throttle_time_ms
        if api_version >= 1 {
            response = response.with_throttle_time_ms(0);
        }

        Ok(response)
    }

    /// Handle SyncGroup request
    pub(in crate::protocol) fn handle_sync_group(
        &self,
        request: SyncGroupRequest,
        api_version: i16,
    ) -> Result<SyncGroupResponse> {
        let group_id = request.group_id.as_str();
        let member_id = request.member_id.as_str();
        let generation_id = request.generation_id;

        // Get protocol info for v5+ response fields
        let (protocol_type, protocol_name) = self
            .group_coordinator
            .get_group(group_id)?
            .map(|g| (g.protocol_type.clone(), g.protocol_name.clone()))
            .unwrap_or_else(|| ("consumer".to_string(), "range".to_string()));

        // Parse assignments from leader's request
        let mut assignments = std::collections::HashMap::new();
        for assignment in &request.assignments {
            let mid = assignment.member_id.as_str().to_string();
            // Parse assignment bytes to extract topic partitions
            let parsed_assignment = Self::parse_member_assignment(&assignment.assignment);
            assignments.insert(mid, parsed_assignment);
        }

        let result =
            self.group_coordinator
                .sync_group(group_id, member_id, generation_id, assignments)?;

        // Encode this member's assignment
        let assignment_bytes = Self::encode_member_assignment(&result);

        let mut response = SyncGroupResponse::default()
            .with_error_code(NONE)
            .with_assignment(Bytes::from(assignment_bytes));

        // v1+: throttle_time_ms
        if api_version >= 1 {
            response = response.with_throttle_time_ms(0);
        }

        // v5+: protocol_type and protocol_name
        if api_version >= 5 {
            response = response
                .with_protocol_type(Some(StrBytes::from_string(protocol_type)))
                .with_protocol_name(Some(StrBytes::from_string(protocol_name)));
        }

        Ok(response)
    }

    /// Parse member assignment bytes from Kafka protocol
    /// Format: version (2 bytes) + topic count (4 bytes) + [topic name len (2) + topic name + partition count (4) + partition ids (4 each)]* + user data len (4) + user data
    ///
    /// SECURITY: All count fields are validated to prevent integer overflow attacks.
    /// Negative or excessively large counts are rejected to prevent DoS via memory exhaustion
    /// or CPU spinning on massive iterations.
    fn parse_member_assignment(data: &[u8]) -> Vec<(String, i32)> {
        // Maximum reasonable limits to prevent DoS attacks
        const MAX_TOPICS: usize = 10_000;
        const MAX_PARTITIONS_PER_TOPIC: usize = 100_000;

        let mut result = Vec::new();
        if data.len() < 6 {
            return result;
        }

        let mut offset = 2; // Skip version

        // Read topic count
        if offset + 4 > data.len() {
            return result;
        }
        let topic_count_i32 = i32::from_be_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]);
        offset += 4;

        // SECURITY: Validate topic_count is non-negative and within reasonable bounds
        if topic_count_i32 < 0 {
            warn!(
                topic_count = topic_count_i32,
                "Invalid negative topic count in member assignment"
            );
            return result;
        }
        let topic_count = topic_count_i32 as usize;
        if topic_count > MAX_TOPICS {
            warn!(
                topic_count,
                max = MAX_TOPICS,
                "Topic count exceeds maximum in member assignment"
            );
            return result;
        }

        for _ in 0..topic_count {
            // Read topic name length
            if offset + 2 > data.len() {
                break;
            }
            let topic_len = u16::from_be_bytes([data[offset], data[offset + 1]]) as usize;
            offset += 2;

            // Read topic name
            if offset + topic_len > data.len() {
                break;
            }
            let topic = match std::str::from_utf8(&data[offset..offset + topic_len]) {
                Ok(t) => t.to_string(),
                Err(_) => break,
            };
            offset += topic_len;

            // Read partition count
            if offset + 4 > data.len() {
                break;
            }
            let partition_count_i32 = i32::from_be_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]);
            offset += 4;

            // SECURITY: Validate partition_count is non-negative and within reasonable bounds
            if partition_count_i32 < 0 {
                warn!(
                    topic = %topic,
                    partition_count = partition_count_i32,
                    "Invalid negative partition count in member assignment"
                );
                break;
            }
            let partition_count = partition_count_i32 as usize;
            if partition_count > MAX_PARTITIONS_PER_TOPIC {
                warn!(
                    topic = %topic,
                    partition_count,
                    max = MAX_PARTITIONS_PER_TOPIC,
                    "Partition count exceeds maximum in member assignment"
                );
                break;
            }

            // Read partition IDs
            for _ in 0..partition_count {
                if offset + 4 > data.len() {
                    break;
                }
                let partition_id = i32::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                ]);
                offset += 4;
                result.push((topic.clone(), partition_id));
            }
        }

        result
    }

    /// Encode member assignment to Kafka protocol format
    fn encode_member_assignment(assignment: &[(String, i32)]) -> Vec<u8> {
        let mut buf = Vec::new();

        // Version (2 bytes)
        buf.extend_from_slice(&0i16.to_be_bytes());

        // Group by topic
        let mut topic_partitions: std::collections::HashMap<&str, Vec<i32>> =
            std::collections::HashMap::new();
        for (topic, partition) in assignment {
            topic_partitions
                .entry(topic.as_str())
                .or_default()
                .push(*partition);
        }

        // Topic count (4 bytes)
        buf.extend_from_slice(&(topic_partitions.len() as i32).to_be_bytes());

        // Write each topic
        for (topic, partitions) in &topic_partitions {
            // Topic name length (2 bytes)
            buf.extend_from_slice(&(topic.len() as u16).to_be_bytes());
            // Topic name
            buf.extend_from_slice(topic.as_bytes());
            // Partition count (4 bytes)
            buf.extend_from_slice(&(partitions.len() as i32).to_be_bytes());
            // Partition IDs
            for partition in partitions {
                buf.extend_from_slice(&partition.to_be_bytes());
            }
        }

        // User data length (4 bytes) - empty
        buf.extend_from_slice(&0i32.to_be_bytes());

        buf
    }

    /// Handle OffsetCommit request
    pub(in crate::protocol) fn handle_offset_commit(
        &self,
        request: OffsetCommitRequest,
        api_version: i16,
    ) -> Result<OffsetCommitResponse> {
        use kafka_protocol::messages::offset_commit_response::{
            OffsetCommitResponsePartition, OffsetCommitResponseTopic,
        };

        let group_id = request.group_id.as_str();
        let mut topic_responses = Vec::new();

        // Track lag for metrics aggregation
        let mut total_lag: i64 = 0;
        let mut max_lag: i64 = 0;

        for topic in request.topics.iter() {
            let topic_name = topic.name.as_str();
            let mut partition_responses = Vec::new();

            for partition in topic.partitions.iter() {
                let partition_index = partition.partition_index;
                let committed_offset = partition.committed_offset;
                let metadata = partition
                    .committed_metadata
                    .as_ref()
                    .map(|s| s.as_str())
                    .unwrap_or("");

                let error_code = match self.group_coordinator.commit_offset(
                    group_id,
                    topic_name,
                    partition_index,
                    committed_offset,
                    metadata.to_string(),
                ) {
                    Ok(_) => {
                        // Record partition-level lag metric on successful commit
                        let log_end_offset = self
                            .topic_manager
                            .latest_offset(topic_name, partition_index)
                            .unwrap_or(committed_offset);
                        let lag = (log_end_offset - committed_offset).max(0);

                        // Streamline metrics (Prometheus format)
                        crate::metrics::record_consumer_group_lag(
                            group_id,
                            topic_name,
                            partition_index,
                            lag,
                        );

                        // JMX-compatible metrics for monitoring tool compatibility
                        #[cfg(feature = "metrics")]
                        crate::metrics::jmx::record_consumer_lag(
                            group_id,
                            topic_name,
                            partition_index,
                            lag,
                        );

                        // Track for total/max calculations
                        total_lag += lag;
                        if lag > max_lag {
                            max_lag = lag;
                        }

                        NONE
                    }
                    Err(_) => OFFSET_OUT_OF_RANGE,
                };

                let partition_response = OffsetCommitResponsePartition::default()
                    .with_partition_index(partition_index)
                    .with_error_code(error_code);

                partition_responses.push(partition_response);
            }

            let topic_response = OffsetCommitResponseTopic::default()
                .with_name(TopicName::from(StrBytes::from_string(
                    topic_name.to_string(),
                )))
                .with_partitions(partition_responses);

            topic_responses.push(topic_response);
        }

        // Record aggregate lag metrics for the group
        if total_lag > 0 || max_lag > 0 {
            crate::metrics::record_consumer_group_total_lag(group_id, total_lag);
            crate::metrics::record_consumer_group_max_lag(group_id, max_lag);
            // JMX-compatible max lag metric
            #[cfg(feature = "metrics")]
            crate::metrics::jmx::record_consumer_lag_max(group_id, max_lag);
        }

        let mut response = OffsetCommitResponse::default().with_topics(topic_responses);

        // v3+: throttle_time_ms
        if api_version >= 3 {
            response = response.with_throttle_time_ms(0);
        }

        Ok(response)
    }

    /// Handle OffsetFetch request
    pub(in crate::protocol) fn handle_offset_fetch(
        &self,
        request: OffsetFetchRequest,
        api_version: i16,
    ) -> Result<OffsetFetchResponse> {
        // v8+ uses groups array instead of group_id + topics
        if api_version >= 8 {
            use kafka_protocol::messages::offset_fetch_response::{
                OffsetFetchResponseGroup, OffsetFetchResponsePartitions, OffsetFetchResponseTopics,
            };

            let mut group_responses: Vec<OffsetFetchResponseGroup> = Vec::new();

            // v8+ request has `groups` array
            for group in &request.groups {
                let group_id = group.group_id.as_str();
                let mut topic_responses_v8: Vec<OffsetFetchResponseTopics> = Vec::new();

                if let Some(topics) = group.topics.as_ref() {
                    for topic in topics {
                        let topic_name = topic.name.as_str();
                        let mut partition_responses: Vec<OffsetFetchResponsePartitions> =
                            Vec::new();

                        for partition in topic.partition_indexes.iter() {
                            let committed_offset = self
                                .group_coordinator
                                .fetch_offset(group_id, topic_name, *partition);

                            let (offset, metadata, error_code) = match committed_offset {
                                Ok(Some(co)) => (co.offset, co.metadata, NONE),
                                Ok(None) => (-1, String::new(), NONE),
                                Err(_) => (-1, String::new(), OFFSET_OUT_OF_RANGE),
                            };

                            let partition_response = OffsetFetchResponsePartitions::default()
                                .with_partition_index(*partition)
                                .with_committed_offset(offset)
                                .with_committed_leader_epoch(-1)
                                .with_metadata(Some(StrBytes::from_string(metadata)))
                                .with_error_code(error_code);

                            partition_responses.push(partition_response);
                        }

                        let topic_response = OffsetFetchResponseTopics::default()
                            .with_name(TopicName::from(StrBytes::from_string(
                                topic_name.to_string(),
                            )))
                            .with_partitions(partition_responses);

                        topic_responses_v8.push(topic_response);
                    }
                }

                let group_response = OffsetFetchResponseGroup::default()
                    .with_group_id(GroupId::from(StrBytes::from_string(group_id.to_string())))
                    .with_topics(topic_responses_v8)
                    .with_error_code(NONE);

                group_responses.push(group_response);
            }

            let response = OffsetFetchResponse::default()
                .with_throttle_time_ms(0)
                .with_groups(group_responses);

            Ok(response)
        } else {
            let group_id = request.group_id.as_str();
            // v0-7: Use top-level topics array
            use kafka_protocol::messages::offset_fetch_response::{
                OffsetFetchResponsePartition, OffsetFetchResponseTopic,
            };

            let mut topic_responses = Vec::new();

            if let Some(topics) = request.topics.as_ref() {
                for topic in topics {
                    let topic_name = topic.name.as_str();
                    let mut partition_responses = Vec::new();

                    for partition in topic.partition_indexes.iter() {
                        let committed_offset = self
                            .group_coordinator
                            .fetch_offset(group_id, topic_name, *partition);

                        let (offset, metadata, error_code) = match committed_offset {
                            Ok(Some(co)) => (co.offset, co.metadata, NONE),
                            Ok(None) => (-1, String::new(), NONE),
                            Err(_) => (-1, String::new(), OFFSET_OUT_OF_RANGE),
                        };

                        let mut partition_response = OffsetFetchResponsePartition::default()
                            .with_partition_index(*partition)
                            .with_committed_offset(offset)
                            .with_metadata(Some(StrBytes::from_string(metadata)))
                            .with_error_code(error_code);

                        // v5-7: committed_leader_epoch
                        if api_version >= 5 {
                            partition_response = partition_response.with_committed_leader_epoch(-1);
                        }

                        partition_responses.push(partition_response);
                    }

                    let topic_response = OffsetFetchResponseTopic::default()
                        .with_name(TopicName::from(StrBytes::from_string(
                            topic_name.to_string(),
                        )))
                        .with_partitions(partition_responses);

                    topic_responses.push(topic_response);
                }
            }

            let mut response = OffsetFetchResponse::default().with_topics(topic_responses);

            // v2-7: top-level error_code
            if api_version >= 2 {
                response = response.with_error_code(NONE);
            }

            // v3+: throttle_time_ms
            if api_version >= 3 {
                response = response.with_throttle_time_ms(0);
            }

            Ok(response)
        }
    }

    /// Handle DescribeGroups request
    pub(in crate::protocol) fn handle_describe_groups(
        &self,
        request: DescribeGroupsRequest,
        api_version: i16,
    ) -> Result<DescribeGroupsResponse> {
        use kafka_protocol::messages::describe_groups_response::{
            DescribedGroup, DescribedGroupMember,
        };

        let mut groups = Vec::new();

        for group_id in request.groups.iter() {
            let group_id_str = group_id.as_str();

            let described_group = match self.group_coordinator.get_group(group_id_str)? {
                Some(group) => {
                    let members: Vec<DescribedGroupMember> = group
                        .members
                        .iter()
                        .map(|(member_id, member)| {
                            DescribedGroupMember::default()
                                .with_member_id(StrBytes::from_string(member_id.clone()))
                                .with_client_id(StrBytes::from_string(member.client_id.clone()))
                                .with_client_host(StrBytes::from_string(member.client_host.clone()))
                        })
                        .collect();

                    let state = match group.state {
                        crate::consumer::GroupState::Empty => "Empty",
                        crate::consumer::GroupState::PreparingRebalance => "PreparingRebalance",
                        crate::consumer::GroupState::CompletingRebalance => "CompletingRebalance",
                        crate::consumer::GroupState::Stable => "Stable",
                        crate::consumer::GroupState::Dead => "Dead",
                    };

                    let mut described = DescribedGroup::default()
                        .with_error_code(NONE)
                        .with_group_id(kafka_protocol::messages::GroupId::from(
                            StrBytes::from_string(group.group_id.clone()),
                        ))
                        .with_group_state(StrBytes::from_static_str(state))
                        .with_protocol_type(StrBytes::from_string(group.protocol_type.clone()))
                        .with_protocol_data(StrBytes::from_string(group.protocol_name.clone()))
                        .with_members(members);

                    // v3+: authorized_operations (-2147483648 = NOT_SET)
                    if api_version >= 3 {
                        described = described.with_authorized_operations(-2147483648);
                    }

                    described
                }
                None => {
                    let mut described = DescribedGroup::default()
                        .with_error_code(COORDINATOR_NOT_AVAILABLE)
                        .with_group_id(kafka_protocol::messages::GroupId::from(
                            StrBytes::from_string(group_id_str.to_string()),
                        ));

                    // v3+: authorized_operations
                    if api_version >= 3 {
                        described = described.with_authorized_operations(-2147483648);
                    }

                    described
                }
            };

            groups.push(described_group);
        }

        let mut response = DescribeGroupsResponse::default().with_groups(groups);

        // v1+: throttle_time_ms
        if api_version >= 1 {
            response = response.with_throttle_time_ms(0);
        }

        Ok(response)
    }

    /// Handle ListGroups request
    pub(in crate::protocol) fn handle_list_groups(
        &self,
        _request: ListGroupsRequest,
        api_version: i16,
    ) -> Result<ListGroupsResponse> {
        use kafka_protocol::messages::list_groups_response::ListedGroup;

        let group_ids = self.group_coordinator.list_groups()?;

        let groups: Vec<ListedGroup> = group_ids
            .iter()
            .map(|group_id| {
                let mut listed = ListedGroup::default()
                    .with_group_id(kafka_protocol::messages::GroupId::from(
                        StrBytes::from_string(group_id.clone()),
                    ))
                    .with_protocol_type(StrBytes::from_static_str("consumer"));

                // v4+: group_state
                if api_version >= 4 {
                    // Try to get actual state from coordinator
                    let state = self
                        .group_coordinator
                        .get_group(group_id)
                        .ok()
                        .flatten()
                        .map(|g| match g.state {
                            crate::consumer::GroupState::Empty => "Empty",
                            crate::consumer::GroupState::PreparingRebalance => "PreparingRebalance",
                            crate::consumer::GroupState::CompletingRebalance => {
                                "CompletingRebalance"
                            }
                            crate::consumer::GroupState::Stable => "Stable",
                            crate::consumer::GroupState::Dead => "Dead",
                        })
                        .unwrap_or("Empty");
                    listed = listed.with_group_state(StrBytes::from_static_str(state));
                }

                // v5+: group_type
                if api_version >= 5 {
                    listed = listed.with_group_type(StrBytes::from_static_str("consumer"));
                }

                listed
            })
            .collect();

        let mut response = ListGroupsResponse::default()
            .with_error_code(NONE)
            .with_groups(groups);

        // v1+: throttle_time_ms
        if api_version >= 1 {
            response = response.with_throttle_time_ms(0);
        }

        Ok(response)
    }

    /// Handle DeleteGroups request
    pub(in crate::protocol) fn handle_delete_groups(
        &self,
        request: DeleteGroupsRequest,
    ) -> Result<DeleteGroupsResponse> {
        use kafka_protocol::messages::delete_groups_response::DeletableGroupResult;

        let mut results = Vec::new();

        for group_id in request.groups_names.iter() {
            let group_id_str = group_id.as_str();

            let error_code = match self.group_coordinator.delete_group(group_id_str) {
                Ok(_) => {
                    info!(group_id = %group_id_str, "Consumer group deleted");
                    NONE
                }
                Err(e) => {
                    let error_msg = e.to_string();
                    if error_msg.contains("non-empty") {
                        warn!(group_id = %group_id_str, "Cannot delete non-empty group");
                        NON_EMPTY_GROUP
                    } else if error_msg.contains("not found") {
                        warn!(group_id = %group_id_str, "Group not found");
                        GROUP_ID_NOT_FOUND
                    } else {
                        error!(group_id = %group_id_str, error = %e, "Failed to delete group");
                        NOT_COORDINATOR
                    }
                }
            };

            let result = DeletableGroupResult::default()
                .with_group_id(kafka_protocol::messages::GroupId::from(
                    StrBytes::from_string(group_id_str.to_string()),
                ))
                .with_error_code(error_code);

            results.push(result);
        }

        let response = DeleteGroupsResponse::default().with_results(results);

        Ok(response)
    }

}
