//! Topic management handlers for the Kafka protocol.
//!
//! This module contains handlers for creating, deleting, and managing
//! topics and partitions.


use crate::error::Result;
use crate::protocol::handlers::error_codes::*;
use crate::storage::storage_mode::StorageMode;
use crate::storage::topic::{TopicConfig, CleanupPolicy};
use kafka_protocol::messages::TopicName;
use kafka_protocol::messages::{
    CreatePartitionsRequest, CreatePartitionsResponse, CreateTopicsRequest, CreateTopicsResponse, DeleteTopicsRequest, DeleteTopicsResponse,
};
use kafka_protocol::protocol::StrBytes;
use super::KafkaHandler;
use super::Operation;
use super::ResourceType;
use tracing::{info, warn};

impl KafkaHandler {
    /// Handle CreateTopics request
    pub(in crate::protocol) fn handle_create_topics(
        &self,
        request: CreateTopicsRequest,
    ) -> Result<CreateTopicsResponse> {
        use kafka_protocol::messages::create_topics_response::CreatableTopicResult;

        let mut topic_results = Vec::new();

        for topic in request.topics.iter() {
            let topic_name = topic.name.as_str().to_string();
            let num_partitions = topic.num_partitions.max(1);

            // Parse topic configs to extract storage.mode and other settings
            let topic_config = self.parse_topic_configs(&topic.configs);

            let error_code = match self.topic_manager.create_topic_with_config(
                &topic_name,
                num_partitions,
                topic_config,
            ) {
                Ok(_) => {
                    // Notify KIP-848 engine about the new topic for partition assignment
                    self.kip848_engine
                        .update_topic_partitions(&topic_name, num_partitions);
                    NONE
                }
                Err(_) => TOPIC_ALREADY_EXISTS,
            };

            let topic_result = CreatableTopicResult::default()
                .with_name(TopicName::from(StrBytes::from_string(topic_name)))
                .with_error_code(error_code)
                .with_num_partitions(num_partitions)
                .with_replication_factor(1);

            topic_results.push(topic_result);
        }

        let response = CreateTopicsResponse::default().with_topics(topic_results);

        Ok(response)
    }

    /// Parse topic configs from CreateTopicsRequest into TopicConfig
    pub(crate) fn parse_topic_configs(
        &self,
        configs: &[kafka_protocol::messages::create_topics_request::CreateableTopicConfig],
    ) -> TopicConfig {
        let mut topic_config = TopicConfig::default();

        for config in configs {
            let name = config.name.as_str();
            let value = config.value.as_ref().map(|v| v.as_str());

            match name {
                "storage.mode" => {
                    if let Some(v) = value {
                        if let Ok(mode) = v.parse::<StorageMode>() {
                            topic_config.storage_mode = mode;
                        }
                    }
                }
                "retention.ms" => {
                    if let Some(v) = value {
                        if let Ok(ms) = v.parse::<i64>() {
                            topic_config.retention_ms = ms;
                        }
                    }
                }
                "retention.bytes" => {
                    if let Some(v) = value {
                        if let Ok(bytes) = v.parse::<i64>() {
                            topic_config.retention_bytes = bytes;
                        }
                    }
                }
                "segment.bytes" => {
                    if let Some(v) = value {
                        if let Ok(bytes) = v.parse::<u64>() {
                            topic_config.segment_bytes = bytes;
                        }
                    }
                }
                "cleanup.policy" => {
                    if let Some(v) = value {
                        if let Ok(policy) = v.parse::<CleanupPolicy>() {
                            topic_config.cleanup_policy = policy;
                        }
                    }
                }
                "message.ttl.ms" | "message.timestamp.difference.max.ms" => {
                    if let Some(v) = value {
                        if let Ok(ms) = v.parse::<i64>() {
                            topic_config.message_ttl_ms = ms;
                        }
                    }
                }
                "min.cleanable.dirty.ratio" => {
                    if let Some(v) = value {
                        if let Ok(ratio) = v.parse::<f64>() {
                            topic_config.min_cleanable_dirty_ratio = ratio;
                        }
                    }
                }
                "delete.retention.ms" => {
                    if let Some(v) = value {
                        if let Ok(ms) = v.parse::<i64>() {
                            topic_config.delete_retention_ms = ms;
                        }
                    }
                }
                "min.compaction.lag.ms" => {
                    if let Some(v) = value {
                        if let Ok(ms) = v.parse::<i64>() {
                            topic_config.min_compaction_lag_ms = ms;
                        }
                    }
                }
                _ => {
                    // Unknown config, ignore
                }
            }
        }

        topic_config
    }

    /// Handle DeleteTopics request
    pub(in crate::protocol) fn handle_delete_topics(
        &self,
        request: DeleteTopicsRequest,
        api_version: i16,
    ) -> Result<DeleteTopicsResponse> {
        use kafka_protocol::messages::delete_topics_response::DeletableTopicResult;

        let mut responses = Vec::new();

        // For versions 0-5, topic names are in topic_names field
        // For version 6+, topics are in topics field with optional name or topic_id
        let topic_names: Vec<String> = if api_version >= 6 {
            request
                .topics
                .iter()
                .filter_map(|t| t.name.as_ref().map(|n| n.as_str().to_string()))
                .collect()
        } else {
            request
                .topic_names
                .iter()
                .map(|n| n.as_str().to_string())
                .collect()
        };

        for topic_name in topic_names {
            let (error_code, error_message) = match self.topic_manager.delete_topic(&topic_name) {
                Ok(_) => {
                    info!(topic = %topic_name, "Topic deleted via Kafka protocol");
                    // Notify KIP-848 engine to remove topic from partition tracking
                    self.kip848_engine.remove_topic(&topic_name);
                    (NONE, None)
                }
                Err(crate::error::StreamlineError::TopicNotFound(_)) => (
                    UNKNOWN_TOPIC_OR_PARTITION,
                    Some(StrBytes::from_string(format!(
                        "Topic '{}' does not exist",
                        topic_name
                    ))),
                ),
                Err(e) => (
                    UNKNOWN_SERVER_ERROR,
                    Some(StrBytes::from_string(format!(
                        "Failed to delete topic: {}",
                        e
                    ))),
                ),
            };

            let result = DeletableTopicResult::default()
                .with_name(Some(TopicName::from(StrBytes::from_string(
                    topic_name.clone(),
                ))))
                .with_error_code(error_code)
                .with_error_message(error_message);

            responses.push(result);
        }

        let response = DeleteTopicsResponse::default()
            .with_throttle_time_ms(0)
            .with_responses(responses);

        Ok(response)
    }

    /// Handle CreatePartitions request
    pub(in crate::protocol) async fn handle_create_partitions(
        &self,
        request: CreatePartitionsRequest,
        principal: &str,
        host: &str,
    ) -> Result<CreatePartitionsResponse> {
        use kafka_protocol::messages::create_partitions_response::CreatePartitionsTopicResult;

        let mut results = Vec::new();

        for topic in &request.topics {
            let topic_name = topic.name.as_str();

            // Check authorization
            if self
                .check_authorization(
                    principal,
                    host,
                    Operation::Alter,
                    ResourceType::Topic,
                    topic_name,
                )
                .await
                .is_err()
            {
                results.push(
                    CreatePartitionsTopicResult::default()
                        .with_name(topic.name.clone())
                        .with_error_code(TOPIC_AUTHORIZATION_FAILED)
                        .with_error_message(Some(StrBytes::from_static_str(
                            "Authorization failed",
                        ))),
                );
                continue;
            }

            // Check if topic exists
            let metadata = match self.topic_manager.get_topic_metadata(topic_name) {
                Ok(m) => m,
                Err(_) => {
                    results.push(
                        CreatePartitionsTopicResult::default()
                            .with_name(topic.name.clone())
                            .with_error_code(UNKNOWN_TOPIC_OR_PARTITION)
                            .with_error_message(Some(StrBytes::from_static_str("Topic not found"))),
                    );
                    continue;
                }
            };

            let current_count = metadata.num_partitions;
            let requested_count = topic.count;

            // Validate: can only increase partition count
            if requested_count <= current_count {
                results.push(
                    CreatePartitionsTopicResult::default()
                        .with_name(topic.name.clone())
                        .with_error_code(INVALID_PARTITIONS)
                        .with_error_message(Some(StrBytes::from_string(format!(
                            "Partition count must be greater than current count ({})",
                            current_count
                        )))),
                );
                continue;
            }

            // If validate_only, just return success
            if request.validate_only {
                results.push(
                    CreatePartitionsTopicResult::default()
                        .with_name(topic.name.clone())
                        .with_error_code(NONE),
                );
                continue;
            }

            // Actually add partitions
            match self
                .topic_manager
                .add_partitions(topic_name, requested_count)
            {
                Ok(new_partition_ids) => {
                    info!(
                        topic = topic_name,
                        current_count,
                        requested_count,
                        new_partitions = ?new_partition_ids,
                        "Partitions added successfully"
                    );
                    results.push(
                        CreatePartitionsTopicResult::default()
                            .with_name(topic.name.clone())
                            .with_error_code(NONE),
                    );
                }
                Err(e) => {
                    warn!(
                        topic = topic_name,
                        current_count,
                        requested_count,
                        error = %e,
                        "Failed to add partitions"
                    );
                    results.push(
                        CreatePartitionsTopicResult::default()
                            .with_name(topic.name.clone())
                            .with_error_code(INVALID_PARTITIONS)
                            .with_error_message(Some(StrBytes::from_string(format!(
                                "Failed to add partitions: {}",
                                e
                            )))),
                    );
                }
            }
        }

        Ok(CreatePartitionsResponse::default()
            .with_throttle_time_ms(0)
            .with_results(results))
    }

}
