//! Core API handlers for the Kafka protocol.
//!
//! This module contains handlers for fundamental protocol operations
//! including API version negotiation, metadata, and response encoding utilities.


use bytes::BytesMut;
use crate::error::{Result, StreamlineError};
use crate::protocol::handlers::error_codes::*;
use kafka_protocol::messages::ApiKey;
use kafka_protocol::messages::ProducerId as KafkaProducerId;
use kafka_protocol::messages::ResponseHeader;
use kafka_protocol::messages::TopicName;
use kafka_protocol::messages::{
    ApiVersionsRequest, ApiVersionsResponse,
    AlterConfigsResponse, ControlledShutdownRequest, ControlledShutdownResponse, CreateTopicsResponse, DeleteGroupsResponse, DeleteTopicsResponse, DescribeConfigsResponse, DescribeGroupsResponse, EndTxnResponse, FetchResponse, FindCoordinatorResponse, HeartbeatResponse, InitProducerIdResponse, JoinGroupResponse, LeaveGroupResponse, ListGroupsResponse, ListOffsetsResponse, MetadataRequest, MetadataResponse, OffsetCommitResponse, OffsetFetchResponse, ProduceResponse, SaslAuthenticateResponse, SaslHandshakeResponse, SyncGroupResponse,
};
use kafka_protocol::protocol::Encodable;
use kafka_protocol::protocol::StrBytes;
use super::KafkaHandler;
use super::constants::HeaderVersionRule;
use super::constants::REQUEST_HEADER_FLEXIBLE_VERSIONS;
use super::constants::RESPONSE_HEADER_FLEXIBLE_VERSIONS;
use super::constants::build_api_versions;
#[allow(unused_imports)]
use tracing::{debug, info, warn};
use uuid::Uuid;

impl KafkaHandler {
    /// Get API key name for metrics
    pub(super) fn api_key_name(api_key: i16) -> &'static str {
        match ApiKey::try_from(api_key) {
            Ok(ApiKey::Produce) => "produce",
            Ok(ApiKey::Fetch) => "fetch",
            Ok(ApiKey::Metadata) => "metadata",
            Ok(ApiKey::ApiVersions) => "api_versions",
            Ok(ApiKey::ListOffsets) => "list_offsets",
            Ok(ApiKey::CreateTopics) => "create_topics",
            Ok(ApiKey::SaslHandshake) => "sasl_handshake",
            Ok(ApiKey::SaslAuthenticate) => "sasl_authenticate",
            Ok(ApiKey::DescribeAcls) => "describe_acls",
            Ok(ApiKey::CreateAcls) => "create_acls",
            Ok(ApiKey::DeleteAcls) => "delete_acls",
            _ => "unknown",
        }
    }

    /// Handle ApiVersions request
    ///
    /// Returns the list of supported API versions from SUPPORTED_API_VERSIONS const.
    pub(in crate::protocol) fn handle_api_versions(
        &self,
        _request: ApiVersionsRequest,
    ) -> Result<ApiVersionsResponse> {
        // Build API version list from declarative const array
        let api_keys = build_api_versions();

        // Note: throttle_time_ms is required for v1+ but always 0 for us
        let response = ApiVersionsResponse::default()
            .with_error_code(NONE)
            .with_api_keys(api_keys)
            .with_throttle_time_ms(0);

        Ok(response)
    }

    /// Handle ControlledShutdown request
    ///
    /// This API is used for graceful broker shutdown. In a single-node deployment,
    /// there are no partition leadership transfers needed, so we return an empty
    /// list of remaining partitions.
    ///
    /// In a cluster deployment (future), this would:
    /// 1. Mark the broker as shutting down
    /// 2. Transfer partition leadership to other brokers
    /// 3. Return any partitions that still need leadership transfer
    pub(in crate::protocol) fn handle_controlled_shutdown(
        &self,
        request: ControlledShutdownRequest,
    ) -> Result<ControlledShutdownResponse> {
        // Convert BrokerId to i32 for logging and comparison
        let broker_id: i32 = request.broker_id.into();

        info!(
            broker_id = broker_id,
            broker_epoch = request.broker_epoch,
            "Received ControlledShutdown request"
        );

        // Validate broker ID matches our node ID
        // For single-node deployments, we accept any broker_id that matches ours
        if broker_id != self.node_id {
            warn!(
                requested_broker_id = broker_id,
                our_node_id = self.node_id,
                "ControlledShutdown request for different broker"
            );
            // Return STALE_BROKER_EPOCH error (error code 77)
            // This indicates the broker ID doesn't match
            return Ok(ControlledShutdownResponse::default()
                .with_error_code(77) // STALE_BROKER_EPOCH
                .with_remaining_partitions(vec![]));
        }

        // In single-node mode, there are no partitions that need leadership transfer
        // because this broker is the only one. Return success with empty remaining partitions.
        //
        // In cluster mode, this would:
        // 1. Notify the cluster manager of impending shutdown
        // 2. Transfer leadership of all partitions led by this broker
        // 3. Return partitions still awaiting leadership transfer

        #[cfg(feature = "clustering")]
        if let Some(ref _cluster_manager) = self.cluster_manager {
            // Future: Implement cluster-aware controlled shutdown
            // 1. cluster_manager.initiate_shutdown(request.broker_id)?
            // 2. Get remaining partitions that need leadership transfer
            // For now, just return success
            debug!("Cluster mode: ControlledShutdown acknowledged");
        }

        info!(
            broker_id = broker_id,
            "ControlledShutdown completed successfully - no partitions remaining"
        );

        Ok(ControlledShutdownResponse::default()
            .with_error_code(0) // NONE - success
            .with_remaining_partitions(vec![]))
    }

    /// Handle Metadata request
    pub(in crate::protocol) async fn handle_metadata(
        &self,
        request: MetadataRequest,
        api_version: i16,
    ) -> Result<MetadataResponse> {
        use kafka_protocol::messages::metadata_response::{
            MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
        };
        use kafka_protocol::messages::BrokerId;

        // Build broker info based on cluster mode
        #[cfg(feature = "clustering")]
        let (brokers, controller_id, cluster_metadata) =
            if let Some(ref cluster_manager) = self.cluster_manager {
                // Cluster mode: get brokers from cluster metadata
                let metadata = cluster_manager.metadata().await;
                let brokers: Vec<MetadataResponseBroker> = metadata
                    .alive_brokers()
                    .iter()
                    .map(|b| {
                        let mut broker = MetadataResponseBroker::default()
                            .with_node_id(BrokerId::from(b.node_id as i32))
                            .with_host(StrBytes::from_string(b.advertised_addr.ip().to_string()))
                            .with_port(b.advertised_addr.port() as i32);
                        // Include rack info if available
                        if let Some(ref rack) = b.rack {
                            broker = broker.with_rack(Some(StrBytes::from_string(rack.clone())));
                        }
                        broker
                    })
                    .collect();

                // Get controller ID (Raft leader)
                let controller_id = cluster_manager
                    .leader_id()
                    .await
                    .map(|id| BrokerId::from(id as i32))
                    .unwrap_or(BrokerId::from(-1));

                (brokers, controller_id, Some(metadata))
            } else {
                // Single-node mode: just report this node using the configured advertised address
                let broker = MetadataResponseBroker::default()
                    .with_node_id(BrokerId::from(self.node_id))
                    .with_host(StrBytes::from_string(self.advertised_addr.ip().to_string()))
                    .with_port(self.advertised_addr.port() as i32);

                (
                    vec![broker],
                    BrokerId::from(self.node_id),
                    None::<crate::cluster::ClusterMetadata>,
                )
            };

        // Single-node mode without clustering feature
        #[cfg(not(feature = "clustering"))]
        let (brokers, controller_id, cluster_metadata): (
            Vec<MetadataResponseBroker>,
            BrokerId,
            Option<()>,
        ) = {
            let broker = MetadataResponseBroker::default()
                .with_node_id(BrokerId::from(self.node_id))
                .with_host(StrBytes::from_string(self.advertised_addr.ip().to_string()))
                .with_port(self.advertised_addr.port() as i32);

            (vec![broker], BrokerId::from(self.node_id), None)
        };

        // Get requested topics or all topics
        // Track topics that exist and topics that don't
        let (existing_topics, missing_topic_names): (
            Vec<crate::storage::TopicMetadata>,
            Vec<String>,
        ) = if let Some(ref requested_topics) = request.topics {
            if requested_topics.is_empty() {
                (self.topic_manager.list_topics()?, vec![])
            } else {
                let mut existing = Vec::new();
                let mut missing = Vec::new();

                for topic in requested_topics.iter() {
                    if let Some(ref name) = topic.name {
                        let topic_name = name.as_str();
                        match self.topic_manager.get_topic_metadata(topic_name) {
                            Ok(metadata) => existing.push(metadata),
                            Err(_) => {
                                // Topic doesn't exist - try to auto-create if allowed
                                if request.allow_auto_topic_creation {
                                    // Auto-create with default settings (1 partition)
                                    match self.topic_manager.create_topic(topic_name, 1) {
                                        Ok(_) => {
                                            info!(topic = %topic_name, "Auto-created topic via metadata request");
                                            // Now get the metadata for the newly created topic
                                            if let Ok(metadata) =
                                                self.topic_manager.get_topic_metadata(topic_name)
                                            {
                                                existing.push(metadata);
                                            }
                                        }
                                        Err(e) => {
                                            warn!(topic = %topic_name, error = %e, "Failed to auto-create topic");
                                            missing.push(topic_name.to_string());
                                        }
                                    }
                                } else {
                                    missing.push(topic_name.to_string());
                                }
                            }
                        }
                    }
                }
                (existing, missing)
            }
        } else {
            (self.topic_manager.list_topics()?, vec![])
        };

        let topics = existing_topics;

        // Helper to generate a deterministic topic ID from topic name
        // Uses a simple hash-based approach for v10+ compatibility
        let generate_topic_id = |topic_name: &str| -> Uuid {
            use std::hash::{Hash, Hasher};
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            topic_name.hash(&mut hasher);
            let hash1 = hasher.finish();
            // Hash again for second 8 bytes
            hash1.hash(&mut hasher);
            let hash2 = hasher.finish();
            // Combine into UUID bytes
            let mut bytes = [0u8; 16];
            bytes[0..8].copy_from_slice(&hash1.to_be_bytes());
            bytes[8..16].copy_from_slice(&hash2.to_be_bytes());
            // Set version (4) and variant (RFC 4122)
            bytes[6] = (bytes[6] & 0x0f) | 0x40; // version 4
            bytes[8] = (bytes[8] & 0x3f) | 0x80; // variant RFC 4122
            Uuid::from_bytes(bytes)
        };

        // Build topic responses with partition info from cluster metadata
        let topic_responses: Vec<MetadataResponseTopic> = topics
            .iter()
            .map(|t| {
                let partitions: Vec<MetadataResponsePartition> = (0..t.num_partitions)
                    .map(|p| {
                        // Get partition assignment from cluster metadata if available
                        #[cfg(feature = "clustering")]
                        if let Some(ref metadata) = cluster_metadata {
                            if let Some(topic_assignment) = metadata.topics.get(&t.name) {
                                if let Some(partition_assignment) =
                                    topic_assignment.partitions.get(&p)
                                {
                                    let leader_id = partition_assignment
                                        .leader
                                        .map(|id| BrokerId::from(id as i32))
                                        .unwrap_or(BrokerId::from(-1));

                                    let replica_nodes: Vec<BrokerId> = partition_assignment
                                        .replicas
                                        .iter()
                                        .map(|&id| BrokerId::from(id as i32))
                                        .collect();

                                    let isr_nodes: Vec<BrokerId> = partition_assignment
                                        .isr
                                        .iter()
                                        .map(|&id| BrokerId::from(id as i32))
                                        .collect();

                                    return MetadataResponsePartition::default()
                                        .with_error_code(NONE)
                                        .with_partition_index(p)
                                        .with_leader_id(leader_id)
                                        .with_leader_epoch(partition_assignment.leader_epoch)
                                        .with_replica_nodes(replica_nodes)
                                        .with_isr_nodes(isr_nodes)
                                        .with_offline_replicas(vec![]);
                                }
                            }
                        }

                        // Suppress unused variable warning when clustering is disabled
                        #[cfg(not(feature = "clustering"))]
                        let _ = &cluster_metadata;

                        // Fall back to single-node defaults
                        MetadataResponsePartition::default()
                            .with_error_code(NONE)
                            .with_partition_index(p)
                            .with_leader_id(BrokerId::from(self.node_id))
                            .with_leader_epoch(0)
                            .with_replica_nodes(vec![BrokerId::from(self.node_id)])
                            .with_isr_nodes(vec![BrokerId::from(self.node_id)])
                            .with_offline_replicas(vec![])
                    })
                    .collect();

                let mut topic_response = MetadataResponseTopic::default()
                    .with_error_code(NONE)
                    .with_name(Some(TopicName::from(StrBytes::from_string(t.name.clone()))))
                    .with_is_internal(false)
                    .with_partitions(partitions);

                // v10+ requires topic_id (UUID)
                if api_version >= 10 {
                    topic_response = topic_response.with_topic_id(generate_topic_id(&t.name));
                }

                topic_response
            })
            .collect();

        // Add missing topics with UNKNOWN_TOPIC_OR_PARTITION error
        let mut all_topic_responses = topic_responses;
        for missing_topic in missing_topic_names {
            let mut missing_response = MetadataResponseTopic::default()
                .with_error_code(UNKNOWN_TOPIC_OR_PARTITION)
                .with_name(Some(TopicName::from(StrBytes::from_string(
                    missing_topic.clone(),
                ))))
                .with_is_internal(false)
                .with_partitions(vec![]);

            // v10+ requires topic_id (UUID)
            if api_version >= 10 {
                missing_response =
                    missing_response.with_topic_id(generate_topic_id(&missing_topic));
            }

            all_topic_responses.push(missing_response);
        }

        let mut response = MetadataResponse::default()
            .with_brokers(brokers)
            .with_cluster_id(Some(StrBytes::from_string(self.cluster_id.clone())))
            .with_controller_id(controller_id)
            .with_topics(all_topic_responses);

        // v3+ requires throttle_time_ms
        if api_version >= 3 {
            response = response.with_throttle_time_ms(0);
        }

        Ok(response)
    }

    pub(super) fn is_flexible_header(api_key: ApiKey, api_version: i16, rules: &[HeaderVersionRule]) -> bool {
        rules
            .iter()
            .any(|rule| rule.api_key == api_key && api_version >= rule.min_version)
    }

    /// Get the header version for a request
    pub(super) fn request_header_version(api_key: i16, api_version: i16) -> i16 {
        // Flexible versions (2) for newer API versions
        // ApiVersions v3+ uses flexible header (v2) per kafka-protocol crate
        match ApiKey::try_from(api_key) {
            Ok(key)
                if Self::is_flexible_header(key, api_version, REQUEST_HEADER_FLEXIBLE_VERSIONS) =>
            {
                2
            }
            Ok(_) => 1,
            Err(_) => 1,
        }
    }

    /// Get the header version for a response
    pub(super) fn response_header_version(api_key: i16, api_version: i16) -> i16 {
        // Flexible versions (1) for newer API versions
        // Note: ApiVersions always uses response header version 0, even for API v3+
        // because it's the bootstrap request and uses non-flexible encoding
        match ApiKey::try_from(api_key) {
            Ok(ApiKey::ApiVersions) => 0,
            Ok(key)
                if Self::is_flexible_header(
                    key,
                    api_version,
                    RESPONSE_HEADER_FLEXIBLE_VERSIONS,
                ) =>
            {
                1
            }
            Ok(_) => 0,
            Err(_) => 0,
        }
    }

    /// Create an error response for a given API key when request processing fails.
    ///
    /// This function generates a properly formatted Kafka error response that clients
    /// can understand, rather than returning an empty response which would violate
    /// the protocol.
    ///
    /// # Arguments
    /// * `api_key` - The API key from the request
    /// * `api_version` - The API version from the request
    /// * `correlation_id` - The correlation ID from the request
    /// * `error_code` - The Kafka error code to include in the response
    ///
    /// # Returns
    /// The encoded response bytes, or an error if encoding fails
    pub(super) fn create_error_response(
        api_key: i16,
        api_version: i16,
        correlation_id: i32,
        error_code: i16,
    ) -> Result<Vec<u8>> {
        let response_header = ResponseHeader::default().with_correlation_id(correlation_id);
        let response_header_version = Self::response_header_version(api_key, api_version);

        // Create appropriate error response based on API type
        let response_body = match ApiKey::try_from(api_key) {
            Ok(ApiKey::ApiVersions) => {
                let response = ApiVersionsResponse::default()
                    .with_error_code(error_code)
                    .with_api_keys(vec![])
                    .with_throttle_time_ms(0);
                Self::encode_response_static(&response, api_version)?
            }
            Ok(ApiKey::Metadata) => {
                let response = MetadataResponse::default()
                    .with_brokers(vec![])
                    .with_topics(vec![])
                    .with_throttle_time_ms(0);
                // Note: MetadataResponse doesn't have a top-level error_code
                Self::encode_response_static(&response, api_version)?
            }
            Ok(ApiKey::Produce) => {
                let response = ProduceResponse::default().with_throttle_time_ms(0);
                // Note: ProduceResponse has per-topic/partition errors, not top-level
                Self::encode_response_static(&response, api_version)?
            }
            Ok(ApiKey::Fetch) => {
                let response = FetchResponse::default()
                    .with_error_code(error_code)
                    .with_throttle_time_ms(0);
                Self::encode_response_static(&response, api_version)?
            }
            Ok(ApiKey::ListOffsets) => {
                let response = ListOffsetsResponse::default().with_throttle_time_ms(0);
                Self::encode_response_static(&response, api_version)?
            }
            Ok(ApiKey::CreateTopics) => {
                let response = CreateTopicsResponse::default().with_throttle_time_ms(0);
                Self::encode_response_static(&response, api_version)?
            }
            Ok(ApiKey::DeleteTopics) => {
                let response = DeleteTopicsResponse::default().with_throttle_time_ms(0);
                Self::encode_response_static(&response, api_version)?
            }
            Ok(ApiKey::FindCoordinator) => {
                use kafka_protocol::messages::BrokerId;
                let response = FindCoordinatorResponse::default()
                    .with_error_code(error_code)
                    .with_node_id(BrokerId::from(-1))
                    .with_throttle_time_ms(0);
                Self::encode_response_static(&response, api_version)?
            }
            Ok(ApiKey::JoinGroup) => {
                let response = JoinGroupResponse::default()
                    .with_error_code(error_code)
                    .with_throttle_time_ms(0);
                Self::encode_response_static(&response, api_version)?
            }
            Ok(ApiKey::SyncGroup) => {
                let response = SyncGroupResponse::default()
                    .with_error_code(error_code)
                    .with_throttle_time_ms(0);
                Self::encode_response_static(&response, api_version)?
            }
            Ok(ApiKey::Heartbeat) => {
                let response = HeartbeatResponse::default()
                    .with_error_code(error_code)
                    .with_throttle_time_ms(0);
                Self::encode_response_static(&response, api_version)?
            }
            Ok(ApiKey::LeaveGroup) => {
                let response = LeaveGroupResponse::default()
                    .with_error_code(error_code)
                    .with_throttle_time_ms(0);
                Self::encode_response_static(&response, api_version)?
            }
            Ok(ApiKey::OffsetFetch) => {
                let response = OffsetFetchResponse::default()
                    .with_error_code(error_code)
                    .with_throttle_time_ms(0);
                Self::encode_response_static(&response, api_version)?
            }
            Ok(ApiKey::OffsetCommit) => {
                let response = OffsetCommitResponse::default().with_throttle_time_ms(0);
                Self::encode_response_static(&response, api_version)?
            }
            Ok(ApiKey::DescribeGroups) => {
                let response = DescribeGroupsResponse::default().with_throttle_time_ms(0);
                Self::encode_response_static(&response, api_version)?
            }
            Ok(ApiKey::ListGroups) => {
                let response = ListGroupsResponse::default()
                    .with_error_code(error_code)
                    .with_throttle_time_ms(0);
                Self::encode_response_static(&response, api_version)?
            }
            Ok(ApiKey::DeleteGroups) => {
                let response = DeleteGroupsResponse::default().with_throttle_time_ms(0);
                Self::encode_response_static(&response, api_version)?
            }
            Ok(ApiKey::SaslHandshake) => {
                let response = SaslHandshakeResponse::default()
                    .with_error_code(error_code)
                    .with_mechanisms(vec![]);
                Self::encode_response_static(&response, api_version)?
            }
            Ok(ApiKey::SaslAuthenticate) => {
                let response = SaslAuthenticateResponse::default()
                    .with_error_code(error_code)
                    .with_error_message(Some(StrBytes::from_static_str(
                        "Request processing failed",
                    )));
                Self::encode_response_static(&response, api_version)?
            }
            Ok(ApiKey::InitProducerId) => {
                let response = InitProducerIdResponse::default()
                    .with_error_code(error_code)
                    .with_producer_id(KafkaProducerId(-1))
                    .with_producer_epoch(-1)
                    .with_throttle_time_ms(0);
                Self::encode_response_static(&response, api_version)?
            }
            Ok(ApiKey::EndTxn) => {
                let response = EndTxnResponse::default()
                    .with_error_code(error_code)
                    .with_throttle_time_ms(0);
                Self::encode_response_static(&response, api_version)?
            }
            Ok(ApiKey::DescribeConfigs) => {
                let response = DescribeConfigsResponse::default().with_throttle_time_ms(0);
                Self::encode_response_static(&response, api_version)?
            }
            Ok(ApiKey::AlterConfigs) => {
                let response = AlterConfigsResponse::default().with_throttle_time_ms(0);
                Self::encode_response_static(&response, api_version)?
            }
            _ => {
                return Err(StreamlineError::protocol_msg(format!(
                    "Unknown API key: {}",
                    api_key
                )));
            }
        };

        // Encode header + body
        let mut response_buf = BytesMut::new();
        response_header
            .encode(&mut response_buf, response_header_version)
            .map_err(|e| {
                StreamlineError::protocol_msg(format!("Failed to encode response header: {}", e))
            })?;
        response_buf.extend_from_slice(&response_body);

        Ok(response_buf.to_vec())
    }

    /// Static version of encode_response for use in create_error_response
    pub(super) fn encode_response_static<T: Encodable>(response: &T, version: i16) -> Result<Vec<u8>> {
        let mut buf = BytesMut::new();
        response.encode(&mut buf, version).map_err(|e| {
            StreamlineError::protocol_msg(format!("Failed to encode response: {}", e))
        })?;
        Ok(buf.to_vec())
    }

}
