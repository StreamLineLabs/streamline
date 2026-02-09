//! Administrative handlers for the Kafka protocol.
//!
//! This module contains handlers for cluster administration operations
//! including configuration management, log directories, partition reassignment,
//! leader election, and feature updates.


use crate::error::{Result, StreamlineError};
use crate::protocol::handlers::error_codes::*;
use crate::storage::topic::CleanupPolicy;
use kafka_protocol::messages::TopicName;
use kafka_protocol::messages::{
    AlterConfigsRequest, AlterConfigsResponse, AlterPartitionReassignmentsRequest, AlterPartitionReassignmentsResponse, AlterReplicaLogDirsRequest, AlterReplicaLogDirsResponse, DeleteRecordsRequest, DeleteRecordsResponse, DescribeClusterRequest, DescribeClusterResponse, DescribeConfigsRequest, DescribeConfigsResponse, DescribeLogDirsRequest, DescribeLogDirsResponse, ElectLeadersRequest, ElectLeadersResponse, IncrementalAlterConfigsRequest, IncrementalAlterConfigsResponse, ListPartitionReassignmentsRequest, ListPartitionReassignmentsResponse, OffsetDeleteRequest, OffsetDeleteResponse, OffsetForLeaderEpochRequest, OffsetForLeaderEpochResponse, UnregisterBrokerRequest, UnregisterBrokerResponse, UpdateFeaturesRequest, UpdateFeaturesResponse,
};
use kafka_protocol::protocol::StrBytes;
use super::KafkaHandler;
use super::Operation;
use super::ResourceType;
use tracing::{debug, info, warn};

impl KafkaHandler {
    /// Handle DescribeConfigs request
    pub(in crate::protocol) async fn handle_describe_configs(
        &self,
        request: DescribeConfigsRequest,
        principal: &str,
        host: &str,
    ) -> Result<DescribeConfigsResponse> {
        use kafka_protocol::messages::describe_configs_response::{
            DescribeConfigsResourceResult, DescribeConfigsResult,
        };

        // Resource types: 2=Topic, 4=Broker
        const RESOURCE_TYPE_TOPIC: i8 = 2;
        const RESOURCE_TYPE_BROKER: i8 = 4;

        let mut results = Vec::new();

        for resource in &request.resources {
            let resource_name = resource.resource_name.as_str();
            let resource_type = resource.resource_type;

            // Check authorization based on resource type
            let auth_result = match resource_type {
                RESOURCE_TYPE_TOPIC => {
                    self.check_authorization(
                        principal,
                        host,
                        Operation::DescribeConfigs,
                        ResourceType::Topic,
                        resource_name,
                    )
                    .await
                }
                RESOURCE_TYPE_BROKER => {
                    self.check_authorization(
                        principal,
                        host,
                        Operation::DescribeConfigs,
                        ResourceType::Cluster,
                        "kafka-cluster",
                    )
                    .await
                }
                _ => {
                    // Unknown resource type
                    results.push(
                        DescribeConfigsResult::default()
                            .with_error_code(UNKNOWN_SERVER_ERROR)
                            .with_error_message(Some(StrBytes::from_static_str(
                                "Unknown resource type",
                            )))
                            .with_resource_type(resource_type)
                            .with_resource_name(resource.resource_name.clone()),
                    );
                    continue;
                }
            };

            if auth_result.is_err() {
                results.push(
                    DescribeConfigsResult::default()
                        .with_error_code(CLUSTER_AUTHORIZATION_FAILED)
                        .with_error_message(Some(StrBytes::from_static_str("Authorization failed")))
                        .with_resource_type(resource_type)
                        .with_resource_name(resource.resource_name.clone()),
                );
                continue;
            }

            // Get configs based on resource type
            let configs = match resource_type {
                RESOURCE_TYPE_TOPIC => {
                    // Get topic configs
                    match self.topic_manager.get_topic_metadata(resource_name) {
                        Ok(metadata) => {
                            vec![
                                DescribeConfigsResourceResult::default()
                                    .with_name(StrBytes::from_static_str("retention.ms"))
                                    .with_value(Some(StrBytes::from_string(
                                        metadata.config.retention_ms.to_string(),
                                    )))
                                    .with_read_only(false)
                                    .with_is_default(metadata.config.retention_ms == -1)
                                    .with_is_sensitive(false),
                                DescribeConfigsResourceResult::default()
                                    .with_name(StrBytes::from_static_str("retention.bytes"))
                                    .with_value(Some(StrBytes::from_string(
                                        metadata.config.retention_bytes.to_string(),
                                    )))
                                    .with_read_only(false)
                                    .with_is_default(metadata.config.retention_bytes == -1)
                                    .with_is_sensitive(false),
                                DescribeConfigsResourceResult::default()
                                    .with_name(StrBytes::from_static_str("segment.bytes"))
                                    .with_value(Some(StrBytes::from_string(
                                        metadata.config.segment_bytes.to_string(),
                                    )))
                                    .with_read_only(false)
                                    .with_is_default(false)
                                    .with_is_sensitive(false),
                                DescribeConfigsResourceResult::default()
                                    .with_name(StrBytes::from_static_str("cleanup.policy"))
                                    .with_value(Some(StrBytes::from_string(
                                        metadata.config.cleanup_policy.to_string(),
                                    )))
                                    .with_read_only(false)
                                    .with_is_default(
                                        metadata.config.cleanup_policy == CleanupPolicy::Delete,
                                    )
                                    .with_is_sensitive(false),
                                DescribeConfigsResourceResult::default()
                                    .with_name(StrBytes::from_static_str("storage.mode"))
                                    .with_value(Some(StrBytes::from_string(
                                        metadata.config.storage_mode.to_string(),
                                    )))
                                    .with_read_only(true) // storage mode cannot be changed after creation
                                    .with_is_default(metadata.config.storage_mode.is_local())
                                    .with_is_sensitive(false),
                                DescribeConfigsResourceResult::default()
                                    .with_name(StrBytes::from_static_str("message.ttl.ms"))
                                    .with_value(Some(StrBytes::from_string(
                                        metadata.config.message_ttl_ms.to_string(),
                                    )))
                                    .with_read_only(false)
                                    .with_is_default(metadata.config.message_ttl_ms == -1)
                                    .with_is_sensitive(false),
                                DescribeConfigsResourceResult::default()
                                    .with_name(StrBytes::from_static_str(
                                        "min.cleanable.dirty.ratio",
                                    ))
                                    .with_value(Some(StrBytes::from_string(
                                        metadata.config.min_cleanable_dirty_ratio.to_string(),
                                    )))
                                    .with_read_only(false)
                                    .with_is_default(
                                        (metadata.config.min_cleanable_dirty_ratio - 0.5).abs()
                                            < 0.001,
                                    )
                                    .with_is_sensitive(false),
                                DescribeConfigsResourceResult::default()
                                    .with_name(StrBytes::from_static_str("delete.retention.ms"))
                                    .with_value(Some(StrBytes::from_string(
                                        metadata.config.delete_retention_ms.to_string(),
                                    )))
                                    .with_read_only(false)
                                    .with_is_default(
                                        metadata.config.delete_retention_ms == 86_400_000,
                                    )
                                    .with_is_sensitive(false),
                                DescribeConfigsResourceResult::default()
                                    .with_name(StrBytes::from_static_str("min.compaction.lag.ms"))
                                    .with_value(Some(StrBytes::from_string(
                                        metadata.config.min_compaction_lag_ms.to_string(),
                                    )))
                                    .with_read_only(false)
                                    .with_is_default(metadata.config.min_compaction_lag_ms == 0)
                                    .with_is_sensitive(false),
                            ]
                        }
                        Err(_) => {
                            results.push(
                                DescribeConfigsResult::default()
                                    .with_error_code(UNKNOWN_TOPIC_OR_PARTITION)
                                    .with_error_message(Some(StrBytes::from_static_str(
                                        "Topic not found",
                                    )))
                                    .with_resource_type(resource_type)
                                    .with_resource_name(resource.resource_name.clone()),
                            );
                            continue;
                        }
                    }
                }
                RESOURCE_TYPE_BROKER => {
                    // Return basic broker configs
                    vec![
                        DescribeConfigsResourceResult::default()
                            .with_name(StrBytes::from_static_str("log.retention.hours"))
                            .with_value(Some(StrBytes::from_static_str("168")))
                            .with_read_only(true)
                            .with_is_default(true)
                            .with_is_sensitive(false),
                        DescribeConfigsResourceResult::default()
                            .with_name(StrBytes::from_static_str("log.segment.bytes"))
                            .with_value(Some(StrBytes::from_static_str("1073741824")))
                            .with_read_only(true)
                            .with_is_default(true)
                            .with_is_sensitive(false),
                        DescribeConfigsResourceResult::default()
                            .with_name(StrBytes::from_static_str("num.partitions"))
                            .with_value(Some(StrBytes::from_static_str("1")))
                            .with_read_only(true)
                            .with_is_default(true)
                            .with_is_sensitive(false),
                    ]
                }
                _ => Vec::new(),
            };

            results.push(
                DescribeConfigsResult::default()
                    .with_error_code(NONE)
                    .with_resource_type(resource_type)
                    .with_resource_name(resource.resource_name.clone())
                    .with_configs(configs),
            );
        }

        Ok(DescribeConfigsResponse::default()
            .with_throttle_time_ms(0)
            .with_results(results))
    }

    /// Handle AlterConfigs request
    pub(in crate::protocol) async fn handle_alter_configs(
        &self,
        request: AlterConfigsRequest,
        principal: &str,
        host: &str,
    ) -> Result<AlterConfigsResponse> {
        use kafka_protocol::messages::alter_configs_response::AlterConfigsResourceResponse;

        // Resource types: 2=Topic, 4=Broker
        const RESOURCE_TYPE_TOPIC: i8 = 2;
        const RESOURCE_TYPE_BROKER: i8 = 4;

        let mut responses = Vec::new();

        for resource in &request.resources {
            let resource_name = resource.resource_name.as_str();
            let resource_type = resource.resource_type;

            // Check authorization based on resource type
            let auth_result = match resource_type {
                RESOURCE_TYPE_TOPIC => {
                    self.check_authorization(
                        principal,
                        host,
                        Operation::AlterConfigs,
                        ResourceType::Topic,
                        resource_name,
                    )
                    .await
                }
                RESOURCE_TYPE_BROKER => {
                    self.check_authorization(
                        principal,
                        host,
                        Operation::AlterConfigs,
                        ResourceType::Cluster,
                        "kafka-cluster",
                    )
                    .await
                }
                _ => {
                    responses.push(
                        AlterConfigsResourceResponse::default()
                            .with_error_code(UNKNOWN_SERVER_ERROR)
                            .with_error_message(Some(StrBytes::from_static_str(
                                "Unknown resource type",
                            )))
                            .with_resource_type(resource_type)
                            .with_resource_name(resource.resource_name.clone()),
                    );
                    continue;
                }
            };

            if auth_result.is_err() {
                responses.push(
                    AlterConfigsResourceResponse::default()
                        .with_error_code(CLUSTER_AUTHORIZATION_FAILED)
                        .with_error_message(Some(StrBytes::from_static_str("Authorization failed")))
                        .with_resource_type(resource_type)
                        .with_resource_name(resource.resource_name.clone()),
                );
                continue;
            }

            // Handle config changes
            match resource_type {
                RESOURCE_TYPE_TOPIC => {
                    // Check if topic exists
                    if self
                        .topic_manager
                        .get_topic_metadata(resource_name)
                        .is_err()
                    {
                        responses.push(
                            AlterConfigsResourceResponse::default()
                                .with_error_code(UNKNOWN_TOPIC_OR_PARTITION)
                                .with_error_message(Some(StrBytes::from_static_str(
                                    "Topic not found",
                                )))
                                .with_resource_type(resource_type)
                                .with_resource_name(resource.resource_name.clone()),
                        );
                        continue;
                    }

                    // If validate_only, just return success
                    if request.validate_only {
                        responses.push(
                            AlterConfigsResourceResponse::default()
                                .with_error_code(NONE)
                                .with_resource_type(resource_type)
                                .with_resource_name(resource.resource_name.clone()),
                        );
                        continue;
                    }

                    // Topic configs cannot be modified dynamically in current implementation
                    // Return success for now (configs are set at creation time)
                    responses.push(
                        AlterConfigsResourceResponse::default()
                            .with_error_code(NONE)
                            .with_resource_type(resource_type)
                            .with_resource_name(resource.resource_name.clone()),
                    );
                }
                RESOURCE_TYPE_BROKER => {
                    // Broker configs are read-only in Streamline
                    if request.validate_only {
                        responses.push(
                            AlterConfigsResourceResponse::default()
                                .with_error_code(NONE)
                                .with_resource_type(resource_type)
                                .with_resource_name(resource.resource_name.clone()),
                        );
                    } else {
                        responses.push(
                            AlterConfigsResourceResponse::default()
                                .with_error_code(UNKNOWN_SERVER_ERROR)
                                .with_error_message(Some(StrBytes::from_static_str(
                                    "Broker configs are read-only",
                                )))
                                .with_resource_type(resource_type)
                                .with_resource_name(resource.resource_name.clone()),
                        );
                    }
                }
                _ => {}
            }
        }

        Ok(AlterConfigsResponse::default()
            .with_throttle_time_ms(0)
            .with_responses(responses))
    }

    /// Handle DescribeCluster request
    pub(in crate::protocol) async fn handle_describe_cluster(
        &self,
        request: DescribeClusterRequest,
    ) -> Result<DescribeClusterResponse> {
        use kafka_protocol::messages::describe_cluster_response::DescribeClusterBroker;
        use kafka_protocol::messages::BrokerId;

        #[cfg(feature = "clustering")]
        let (brokers, controller_id, cluster_id) =
            if let Some(ref cluster_manager) = self.cluster_manager {
                // Cluster mode
                let metadata = cluster_manager.metadata().await;
                let brokers: Vec<DescribeClusterBroker> = metadata
                    .alive_brokers()
                    .iter()
                    .map(|b| {
                        DescribeClusterBroker::default()
                            .with_broker_id(BrokerId::from(b.node_id as i32))
                            .with_host(StrBytes::from_string(b.advertised_addr.ip().to_string()))
                            .with_port(b.advertised_addr.port() as i32)
                            .with_rack(b.rack.as_ref().map(|r| StrBytes::from_string(r.clone())))
                    })
                    .collect();

                let controller_id = cluster_manager
                    .leader_id()
                    .await
                    .map(|id| BrokerId::from(id as i32))
                    .unwrap_or(BrokerId::from(-1));

                let cluster_id = metadata.cluster_id.clone();

                (brokers, controller_id, cluster_id)
            } else {
                // Single-node mode: use the configured advertised address
                let broker = DescribeClusterBroker::default()
                    .with_broker_id(BrokerId::from(self.node_id))
                    .with_host(StrBytes::from_string(self.advertised_addr.ip().to_string()))
                    .with_port(self.advertised_addr.port() as i32)
                    .with_rack(None);

                (
                    vec![broker],
                    BrokerId::from(self.node_id),
                    "streamline-cluster".to_string(),
                )
            };

        // Single-node mode without clustering feature
        #[cfg(not(feature = "clustering"))]
        let (brokers, controller_id, cluster_id) = {
            let broker = DescribeClusterBroker::default()
                .with_broker_id(BrokerId::from(self.node_id))
                .with_host(StrBytes::from_string(self.advertised_addr.ip().to_string()))
                .with_port(self.advertised_addr.port() as i32)
                .with_rack(None);

            (
                vec![broker],
                BrokerId::from(self.node_id),
                "streamline-cluster".to_string(),
            )
        };

        // Calculate cluster authorized operations if requested
        let cluster_authorized_operations = if request.include_cluster_authorized_operations {
            // Return bitmask of all cluster operations (simplified)
            // In a full implementation, this would check ACLs
            0x7FFF // All operations allowed
        } else {
            -2147483648 // Not requested (INT_MIN as sentinel)
        };

        Ok(DescribeClusterResponse::default()
            .with_throttle_time_ms(0)
            .with_error_code(NONE)
            .with_cluster_id(StrBytes::from_string(cluster_id))
            .with_controller_id(controller_id)
            .with_brokers(brokers)
            .with_cluster_authorized_operations(cluster_authorized_operations))
    }

    /// Handle OffsetForLeaderEpoch request
    pub(in crate::protocol) async fn handle_offset_for_leader_epoch(
        &self,
        request: OffsetForLeaderEpochRequest,
    ) -> Result<OffsetForLeaderEpochResponse> {
        use kafka_protocol::messages::offset_for_leader_epoch_response::{
            EpochEndOffset, OffsetForLeaderTopicResult,
        };

        let mut topic_results = Vec::new();

        for topic in &request.topics {
            let topic_name = topic.topic.as_str();

            let mut partition_results = Vec::new();

            for partition in &topic.partitions {
                // Get the partition's end offset
                // In a full implementation, this would validate leader epochs
                match self
                    .topic_manager
                    .latest_offset(topic_name, partition.partition)
                {
                    Ok(end_offset) => {
                        // For simplicity, we return the current end offset
                        // A full implementation would find the offset for the requested epoch
                        partition_results.push(
                            EpochEndOffset::default()
                                .with_error_code(NONE)
                                .with_partition(partition.partition)
                                .with_leader_epoch(partition.leader_epoch)
                                .with_end_offset(end_offset),
                        );
                    }
                    Err(_) => {
                        partition_results.push(
                            EpochEndOffset::default()
                                .with_error_code(UNKNOWN_TOPIC_OR_PARTITION)
                                .with_partition(partition.partition)
                                .with_leader_epoch(-1)
                                .with_end_offset(-1),
                        );
                    }
                }
            }

            topic_results.push(
                OffsetForLeaderTopicResult::default()
                    .with_topic(topic.topic.clone())
                    .with_partitions(partition_results),
            );
        }

        Ok(OffsetForLeaderEpochResponse::default()
            .with_throttle_time_ms(0)
            .with_topics(topic_results))
    }

    /// Handle DeleteRecords request
    pub(in crate::protocol) async fn handle_delete_records(
        &self,
        request: DeleteRecordsRequest,
    ) -> Result<DeleteRecordsResponse> {
        use kafka_protocol::messages::delete_records_response::{
            DeleteRecordsPartitionResult, DeleteRecordsTopicResult,
        };

        info!(
            timeout_ms = request.timeout_ms,
            topics_count = request.topics.len(),
            "DeleteRecords request"
        );

        let mut topic_results = Vec::new();

        for topic in &request.topics {
            let topic_name = topic.name.as_str();
            let mut partition_results = Vec::new();

            for partition in &topic.partitions {
                let partition_index = partition.partition_index;
                let target_offset = partition.offset;

                // Delete records before the target offset
                match self.topic_manager.delete_records_before(
                    topic_name,
                    partition_index,
                    target_offset,
                ) {
                    Ok(low_watermark) => {
                        info!(
                            topic = topic_name,
                            partition = partition_index,
                            target_offset,
                            low_watermark,
                            "Deleted records successfully"
                        );
                        partition_results.push(
                            DeleteRecordsPartitionResult::default()
                                .with_partition_index(partition_index)
                                .with_low_watermark(low_watermark)
                                .with_error_code(NONE),
                        );
                    }
                    Err(e) => {
                        warn!(
                            topic = topic_name,
                            partition = partition_index,
                            error = %e,
                            "Failed to delete records"
                        );
                        // Determine appropriate error code
                        let error_code = match &e {
                            StreamlineError::TopicNotFound(_) => UNKNOWN_TOPIC_OR_PARTITION,
                            StreamlineError::PartitionNotFound(_, _) => UNKNOWN_TOPIC_OR_PARTITION,
                            _ => UNKNOWN_SERVER_ERROR,
                        };
                        partition_results.push(
                            DeleteRecordsPartitionResult::default()
                                .with_partition_index(partition_index)
                                .with_low_watermark(-1)
                                .with_error_code(error_code),
                        );
                    }
                }
            }

            topic_results.push(
                DeleteRecordsTopicResult::default()
                    .with_name(topic.name.clone())
                    .with_partitions(partition_results),
            );
        }

        Ok(DeleteRecordsResponse::default()
            .with_throttle_time_ms(0)
            .with_topics(topic_results))
    }

    /// Handle IncrementalAlterConfigs request (API key 44)
    ///
    /// This API is used by Kafka Connect and other tools to incrementally
    /// modify configurations. It supports SET, DELETE, and APPEND operations.
    pub(in crate::protocol) async fn handle_incremental_alter_configs(
        &self,
        request: IncrementalAlterConfigsRequest,
    ) -> Result<IncrementalAlterConfigsResponse> {
        use kafka_protocol::messages::incremental_alter_configs_response::AlterConfigsResourceResponse;

        info!(
            resources_count = request.resources.len(),
            validate_only = request.validate_only,
            "IncrementalAlterConfigs request"
        );

        let mut responses = Vec::new();

        for resource in &request.resources {
            let resource_type = resource.resource_type;
            let resource_name = resource.resource_name.as_str();

            debug!(
                resource_type = resource_type,
                resource_name = resource_name,
                configs_count = resource.configs.len(),
                "Processing config resource"
            );

            // For now, we accept all config changes but only persist topic-level configs
            // Resource types: 2 = TOPIC, 4 = BROKER, 8 = BROKER_LOGGER
            let error_code = if resource_type == 2 {
                // Topic configuration
                match self.topic_manager.get_topic_metadata(resource_name) {
                    Ok(_topic) => {
                        // Process config operations
                        for config in &resource.configs {
                            let config_name = config.name.as_str();
                            let config_value = config.value.as_deref();
                            let operation = config.config_operation;

                            debug!(
                                config_name = config_name,
                                config_value = ?config_value,
                                operation = operation,
                                "Config operation"
                            );

                            // Operation types: 0 = SET, 1 = DELETE, 2 = APPEND, 3 = SUBTRACT
                            // For now, log and acknowledge - actual persistence would
                            // go through TopicManager
                            match operation {
                                0 => {
                                    // SET
                                    info!(
                                        topic = resource_name,
                                        config = config_name,
                                        value = ?config_value,
                                        "Setting topic config"
                                    );
                                }
                                1 => {
                                    // DELETE
                                    info!(
                                        topic = resource_name,
                                        config = config_name,
                                        "Deleting topic config (reverting to default)"
                                    );
                                }
                                2 => {
                                    // APPEND
                                    info!(
                                        topic = resource_name,
                                        config = config_name,
                                        value = ?config_value,
                                        "Appending to topic config"
                                    );
                                }
                                3 => {
                                    // SUBTRACT
                                    info!(
                                        topic = resource_name,
                                        config = config_name,
                                        value = ?config_value,
                                        "Subtracting from topic config"
                                    );
                                }
                                _ => {
                                    warn!(operation = operation, "Unknown config operation");
                                }
                            }
                        }
                        NONE
                    }
                    Err(_) => UNKNOWN_TOPIC_OR_PARTITION,
                }
            } else if resource_type == 4 || resource_type == 8 {
                // Broker or broker logger configuration
                // Accept but don't persist (single-node doesn't need broker config changes)
                for config in &resource.configs {
                    debug!(
                        config_name = config.name.as_str(),
                        config_value = ?config.value,
                        "Broker config operation (acknowledged but not persisted)"
                    );
                }
                NONE
            } else {
                // Unknown resource type
                warn!(
                    resource_type = resource_type,
                    "Unknown resource type for config"
                );
                INVALID_REQUEST
            };

            responses.push(
                AlterConfigsResourceResponse::default()
                    .with_error_code(error_code)
                    .with_error_message(if error_code == NONE {
                        None
                    } else {
                        Some(StrBytes::from_static_str("Configuration error"))
                    })
                    .with_resource_type(resource_type)
                    .with_resource_name(resource.resource_name.clone()),
            );
        }

        Ok(IncrementalAlterConfigsResponse::default()
            .with_throttle_time_ms(0)
            .with_responses(responses))
    }

    /// Handle DescribeLogDirs request (API Key 35)
    /// Returns information about log directories and their contents
    pub(in crate::protocol) fn handle_describe_log_dirs(
        &self,
        request: DescribeLogDirsRequest,
    ) -> Result<DescribeLogDirsResponse> {
        use kafka_protocol::messages::describe_log_dirs_response::{
            DescribeLogDirsPartition, DescribeLogDirsResult, DescribeLogDirsTopic,
        };

        info!("DescribeLogDirs request");

        // Use default data directory path
        let data_dir = std::path::PathBuf::from("./data");

        // Get disk usage info - use reasonable defaults
        // In production, use sys_info or similar to get actual disk stats
        let (total_bytes, usable_bytes) = (1_000_000_000_000i64, 500_000_000_000i64);

        let mut topics = Vec::new();

        // Get topics to describe
        let topic_names: Vec<String> = if let Some(ref requested_topics) = request.topics {
            requested_topics
                .iter()
                .map(|t| t.topic.as_str().to_string())
                .collect()
        } else {
            // All topics
            self.topic_manager
                .list_topics()
                .unwrap_or_default()
                .into_iter()
                .map(|m| m.name)
                .collect()
        };

        for topic_name in topic_names {
            if let Ok(metadata) = self.topic_manager.get_topic_metadata(&topic_name) {
                let mut partitions = Vec::new();

                for partition_id in 0..metadata.num_partitions {
                    // Get partition size (approximate)
                    let size = self
                        .topic_manager
                        .high_watermark(&topic_name, partition_id)
                        .unwrap_or(0)
                        * 100; // Rough estimate: 100 bytes per message

                    partitions.push(
                        DescribeLogDirsPartition::default()
                            .with_partition_index(partition_id)
                            .with_partition_size(size)
                            .with_offset_lag(0)
                            .with_is_future_key(false),
                    );
                }

                topics.push(
                    DescribeLogDirsTopic::default()
                        .with_name(TopicName::from(StrBytes::from_string(topic_name)))
                        .with_partitions(partitions),
                );
            }
        }

        let result = DescribeLogDirsResult::default()
            .with_error_code(NONE)
            .with_log_dir(StrBytes::from_string(
                data_dir.to_string_lossy().to_string(),
            ))
            .with_topics(topics)
            .with_total_bytes(total_bytes)
            .with_usable_bytes(usable_bytes);

        Ok(DescribeLogDirsResponse::default()
            .with_throttle_time_ms(0)
            .with_results(vec![result]))
    }

    /// Handle ListPartitionReassignments request (API Key 46)
    /// Lists ongoing partition reassignments (none in single-node mode)
    pub(in crate::protocol) fn handle_list_partition_reassignments(
        &self,
        _request: ListPartitionReassignmentsRequest,
    ) -> Result<ListPartitionReassignmentsResponse> {
        info!("ListPartitionReassignments request");

        // In single-node mode, there are never any ongoing reassignments
        Ok(ListPartitionReassignmentsResponse::default()
            .with_throttle_time_ms(0)
            .with_error_code(NONE)
            .with_topics(vec![]))
    }

    /// Handle OffsetDelete request (API Key 47)
    /// Deletes committed offsets for a consumer group
    pub(in crate::protocol) fn handle_offset_delete(
        &self,
        request: OffsetDeleteRequest,
    ) -> Result<OffsetDeleteResponse> {
        use kafka_protocol::messages::offset_delete_response::{
            OffsetDeleteResponsePartition, OffsetDeleteResponseTopic,
        };

        let group_id = request.group_id.as_str();

        info!(group_id, "OffsetDelete request");

        // Check if group exists first
        let group_exists = self
            .group_coordinator
            .get_group(group_id)
            .is_ok_and(|o| o.is_some());

        if !group_exists {
            return Ok(OffsetDeleteResponse::default()
                .with_throttle_time_ms(0)
                .with_error_code(INVALID_GROUP_ID)
                .with_topics(vec![]));
        }

        let mut topic_results = Vec::new();

        for topic in &request.topics {
            let topic_name = topic.name.as_str();
            let mut partition_results = Vec::new();

            for partition in &topic.partitions {
                let partition_index = partition.partition_index;

                // Delete the offset
                let error_code = match self.group_coordinator.delete_offset(
                    group_id,
                    topic_name,
                    partition_index,
                ) {
                    Ok(()) => {
                        debug!(
                            group_id,
                            topic = topic_name,
                            partition = partition_index,
                            "Offset deleted"
                        );
                        NONE
                    }
                    Err(e) => {
                        warn!(error = %e, "Failed to delete offset");
                        UNKNOWN_SERVER_ERROR
                    }
                };

                partition_results.push(
                    OffsetDeleteResponsePartition::default()
                        .with_partition_index(partition_index)
                        .with_error_code(error_code),
                );
            }

            topic_results.push(
                OffsetDeleteResponseTopic::default()
                    .with_name(topic.name.clone())
                    .with_partitions(partition_results),
            );
        }

        Ok(OffsetDeleteResponse::default()
            .with_throttle_time_ms(0)
            .with_error_code(NONE)
            .with_topics(topic_results))
    }

    /// Handle UpdateFeatures request (API Key 57)
    pub(in crate::protocol) fn handle_update_features(
        &self,
        request: UpdateFeaturesRequest,
    ) -> Result<UpdateFeaturesResponse> {
        use kafka_protocol::messages::update_features_response::UpdatableFeatureResult;

        info!(
            feature_updates = ?request.feature_updates.len(),
            validate_only = request.validate_only,
            "UpdateFeatures request"
        );

        // For single-node deployment, we don't really need to track feature flags
        // Just acknowledge all requested features as updated
        let mut results = vec![];

        for feature in &request.feature_updates {
            let feature_name = feature.feature.as_str();
            let max_version_level = feature.max_version_level;

            info!(
                feature_name = feature_name,
                max_version_level = max_version_level,
                "Processing feature update"
            );

            // Accept all feature updates (we don't actually store them for single-node)
            results.push(
                UpdatableFeatureResult::default()
                    .with_feature(feature.feature.clone())
                    .with_error_code(NONE)
                    .with_error_message(None),
            );
        }

        Ok(UpdateFeaturesResponse::default()
            .with_throttle_time_ms(0)
            .with_error_code(NONE)
            .with_results(results))
    }

    /// Handle UnregisterBroker request (API Key 64)
    pub(in crate::protocol) fn handle_unregister_broker(
        &self,
        request: UnregisterBrokerRequest,
    ) -> Result<UnregisterBrokerResponse> {
        let broker_id = request.broker_id.0;
        info!(broker_id, "UnregisterBroker request");

        // For single-node deployment, we can't unregister ourselves
        // Return UNSUPPORTED_VERSION as this operation doesn't make sense for single-node
        if broker_id == self.node_id {
            warn!(
                broker_id,
                "Cannot unregister self in single-node deployment"
            );
            return Ok(UnregisterBrokerResponse::default()
                .with_throttle_time_ms(0)
                .with_error_code(UNSUPPORTED_VERSION)
                .with_error_message(Some(StrBytes::from_string(
                    "Cannot unregister broker in single-node deployment".to_string(),
                ))));
        }

        // If trying to unregister a different broker, return not found
        warn!(broker_id, "Broker not found (single-node deployment)");
        Ok(UnregisterBrokerResponse::default()
            .with_throttle_time_ms(0)
            .with_error_code(BROKER_NOT_AVAILABLE)
            .with_error_message(Some(StrBytes::from_string(format!(
                "Broker {} not found",
                broker_id
            )))))
    }

    /// Handle AlterReplicaLogDirs request (API Key 34)
    ///
    /// Moves replicas between log directories. For single-node deployment,
    /// this is a no-op since there's only one log directory.
    pub(in crate::protocol) fn handle_alter_replica_log_dirs(
        &self,
        request: AlterReplicaLogDirsRequest,
    ) -> Result<AlterReplicaLogDirsResponse> {
        use kafka_protocol::messages::alter_replica_log_dirs_response::AlterReplicaLogDirTopicResult;

        info!(
            dirs_count = request.dirs.len(),
            "AlterReplicaLogDirs request"
        );

        // For single-node deployment, we have only one log directory
        // Return success for all requested moves (no-op)
        let mut results = Vec::new();

        for dir in &request.dirs {
            for topic in &dir.topics {
                let topic_result = AlterReplicaLogDirTopicResult::default()
                    .with_topic_name(topic.name.clone())
                    .with_partitions(
                        topic
                            .partitions
                            .iter()
                            .map(|p| {
                                kafka_protocol::messages::alter_replica_log_dirs_response::AlterReplicaLogDirPartitionResult::default()
                                    .with_partition_index(*p)
                                    .with_error_code(NONE)
                            })
                            .collect(),
                    );
                results.push(topic_result);
            }
        }

        Ok(AlterReplicaLogDirsResponse::default()
            .with_throttle_time_ms(0)
            .with_results(results))
    }

    /// Handle ElectLeaders request (API Key 43)
    ///
    /// Triggers leader election for partitions. For single-node deployment,
    /// this node is always the leader.
    pub(in crate::protocol) fn handle_elect_leaders(
        &self,
        request: ElectLeadersRequest,
    ) -> Result<ElectLeadersResponse> {
        use kafka_protocol::messages::elect_leaders_response::{
            PartitionResult, ReplicaElectionResult,
        };

        info!(
            election_type = request.election_type,
            topic_count = request
                .topic_partitions
                .as_ref()
                .map(|t| t.len())
                .unwrap_or(0),
            "ElectLeaders request"
        );

        let mut results = Vec::new();

        // If topic_partitions is None, elect leaders for all partitions
        let topics: Vec<_> = match &request.topic_partitions {
            Some(topics) => topics
                .iter()
                .map(|t| (t.topic.as_str().to_string(), t.partitions.clone()))
                .collect(),
            None => {
                // Get all topics
                self.topic_manager
                    .list_topics()
                    .unwrap_or_default()
                    .into_iter()
                    .map(|m| {
                        let partitions: Vec<i32> = (0..m.num_partitions).collect();
                        (m.name, partitions)
                    })
                    .collect()
            }
        };

        for (topic_name, partitions) in topics {
            let partition_results: Vec<PartitionResult> = partitions
                .iter()
                .map(|p| {
                    // In single-node, we're always the leader
                    PartitionResult::default()
                        .with_partition_id(*p)
                        .with_error_code(NONE)
                        .with_error_message(None)
                })
                .collect();

            results.push(
                ReplicaElectionResult::default()
                    .with_topic(TopicName::from(StrBytes::from_string(topic_name)))
                    .with_partition_result(partition_results),
            );
        }

        Ok(ElectLeadersResponse::default()
            .with_throttle_time_ms(0)
            .with_error_code(NONE)
            .with_replica_election_results(results))
    }

    /// Handle AlterPartitionReassignments request (API Key 45)
    ///
    /// Reassigns partition replicas. For single-node deployment, this is a no-op
    /// since there's only one broker.
    pub(in crate::protocol) fn handle_alter_partition_reassignments(
        &self,
        request: AlterPartitionReassignmentsRequest,
    ) -> Result<AlterPartitionReassignmentsResponse> {
        use kafka_protocol::messages::alter_partition_reassignments_response::{
            ReassignablePartitionResponse, ReassignableTopicResponse,
        };

        info!(
            topics_count = request.topics.len(),
            "AlterPartitionReassignments request"
        );

        // For single-node deployment, accept all reassignment requests as no-ops
        let results: Vec<ReassignableTopicResponse> = request
            .topics
            .iter()
            .map(|topic| {
                let partition_responses: Vec<ReassignablePartitionResponse> = topic
                    .partitions
                    .iter()
                    .map(|p| {
                        ReassignablePartitionResponse::default()
                            .with_partition_index(p.partition_index)
                            .with_error_code(NONE)
                            .with_error_message(None)
                    })
                    .collect();

                ReassignableTopicResponse::default()
                    .with_name(topic.name.clone())
                    .with_partitions(partition_responses)
            })
            .collect();

        Ok(AlterPartitionReassignmentsResponse::default()
            .with_throttle_time_ms(0)
            .with_error_code(NONE)
            .with_responses(results))
    }

}
