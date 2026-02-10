//! HTTP REST API for producing and consuming messages
//!
//! This module provides a simple HTTP/REST API as an alternative to the Kafka
//! protocol. This enables browser clients, simple scripts, and applications
//! without Kafka client libraries to produce and consume messages.
//!
//! ## Endpoints
//!
//! - `GET /api/v1/topics` - List all topics
//! - `POST /api/v1/topics` - Create a topic
//! - `GET /api/v1/topics/{topic}` - Get topic metadata
//! - `DELETE /api/v1/topics/{topic}` - Delete a topic
//! - `POST /api/v1/topics/{topic}/messages` - Produce messages
//! - `GET /api/v1/topics/{topic}/partitions/{partition}/messages` - Consume messages

use crate::error::{Result, StreamlineError};
use crate::storage::record::Header;
use crate::storage::TopicManager;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, get, post},
    Json, Router,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{debug, error, info};

/// Shared state for the REST API
#[derive(Clone)]
pub(crate) struct ApiState {
    /// Topic manager for storage operations
    pub topic_manager: Arc<TopicManager>,
}

/// Topic creation request
#[derive(Debug, Deserialize)]
pub struct CreateTopicRequest {
    /// Topic name
    pub name: String,
    /// Number of partitions (default: 1)
    #[serde(default = "default_partitions")]
    pub partitions: i32,
}

fn default_partitions() -> i32 {
    1
}

/// Topic information for list response (matches UI client TopicInfo)
#[derive(Debug, Serialize)]
pub struct TopicInfo {
    pub name: String,
    pub partition_count: usize,
    pub replication_factor: usize,
    pub is_internal: bool,
    pub total_messages: u64,
    pub total_bytes: u64,
}

/// Detailed topic information (matches UI client TopicDetails)
#[derive(Debug, Serialize)]
pub struct TopicDetails {
    pub name: String,
    pub partition_count: usize,
    pub replication_factor: usize,
    pub is_internal: bool,
    pub partitions: Vec<PartitionInfo>,
    pub config: std::collections::HashMap<String, String>,
    pub total_messages: u64,
    pub total_bytes: u64,
    pub messages_per_second: f64,
    pub bytes_per_second: f64,
}

/// Partition information (matches UI client PartitionInfo)
#[derive(Debug, Serialize)]
pub struct PartitionInfo {
    pub partition_id: i32,
    pub leader: Option<u64>,
    pub replicas: Vec<u64>,
    pub isr: Vec<u64>,
    pub start_offset: i64,
    pub end_offset: i64,
    pub size_bytes: u64,
}

/// Produce request - messages to publish
#[derive(Debug, Deserialize)]
pub struct ProduceRequest {
    /// Records to produce
    pub records: Vec<ProduceRecord>,
}

/// Single record to produce
#[derive(Debug, Deserialize)]
pub struct ProduceRecord {
    /// Optional key (for partitioning)
    pub key: Option<String>,
    /// Message value (JSON or string)
    pub value: serde_json::Value,
    /// Optional partition (overrides key-based partitioning)
    pub partition: Option<i32>,
    /// Optional headers
    #[serde(default)]
    pub headers: std::collections::HashMap<String, String>,
}

/// Produce response
#[derive(Debug, Serialize)]
pub struct ProduceResponse {
    /// Results for each record
    pub offsets: Vec<ProduceOffsetResult>,
}

/// Result for a single produced record
#[derive(Debug, Serialize)]
pub struct ProduceOffsetResult {
    /// Partition the record was written to
    pub partition: i32,
    /// Offset assigned to the record
    pub offset: i64,
}

fn compute_topic_totals(topic_manager: &TopicManager, topic: &str) -> Result<(u64, u64)> {
    let partitions = topic_manager.all_partition_info(topic)?;
    let mut total_messages = 0u64;
    let mut total_bytes = 0u64;

    for partition in partitions {
        let messages = if partition.latest_offset > partition.earliest_offset {
            (partition.latest_offset - partition.earliest_offset) as u64
        } else {
            0
        };
        total_messages += messages;
        total_bytes += partition.size;
    }

    Ok((total_messages, total_bytes))
}

fn build_partition_details(
    topic_manager: &TopicManager,
    topic: &str,
) -> Result<(Vec<PartitionInfo>, u64, u64)> {
    let partition_infos = topic_manager.all_partition_info(topic)?;
    let mut partitions = Vec::with_capacity(partition_infos.len());
    let mut total_messages = 0u64;
    let mut total_bytes = 0u64;

    for info in partition_infos {
        let messages = if info.latest_offset > info.earliest_offset {
            (info.latest_offset - info.earliest_offset) as u64
        } else {
            0
        };
        total_messages += messages;
        total_bytes += info.size;

        partitions.push(PartitionInfo {
            partition_id: info.id,
            leader: Some(1),
            replicas: vec![1],
            isr: vec![1],
            start_offset: info.earliest_offset,
            end_offset: info.latest_offset,
            size_bytes: info.size,
        });
    }

    Ok((partitions, total_messages, total_bytes))
}

/// Query parameters for consuming messages
#[derive(Debug, Deserialize)]
pub struct ConsumeQuery {
    /// Starting offset (default: 0)
    #[serde(default)]
    pub offset: i64,
    /// Maximum number of messages to return (default: 100)
    #[serde(default = "default_limit")]
    pub limit: usize,
}

fn default_limit() -> usize {
    100
}

/// Consume response
#[derive(Debug, Serialize)]
pub struct ConsumeResponse {
    /// Topic name
    pub topic: String,
    /// Partition ID
    pub partition: i32,
    /// Records returned
    pub records: Vec<ConsumeRecord>,
    /// Next offset to fetch from
    pub next_offset: i64,
}

/// Single consumed record
#[derive(Debug, Serialize)]
pub struct ConsumeRecord {
    /// Record offset
    pub offset: i64,
    /// Record timestamp (milliseconds)
    pub timestamp: i64,
    /// Record key (if present)
    pub key: Option<String>,
    /// Record value
    pub value: serde_json::Value,
    /// Record headers
    pub headers: std::collections::HashMap<String, String>,
}

/// Error response with actionable hints
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    /// Error code (e.g., "TOPIC_NOT_FOUND", "INVALID_REQUEST")
    pub error: String,
    /// Human-readable error message
    pub message: String,
    /// Actionable hint for resolving the error (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
    /// Documentation URL for more information (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub docs_url: Option<String>,
}

impl ErrorResponse {
    fn new(error: &str, message: impl Into<String>) -> Self {
        Self {
            error: error.to_string(),
            message: message.into(),
            hint: None,
            docs_url: None,
        }
    }

    /// Create an error response with a hint for resolution
    #[allow(dead_code)]
    fn with_hint(error: &str, message: impl Into<String>, hint: impl Into<String>) -> Self {
        Self {
            error: error.to_string(),
            message: message.into(),
            hint: Some(hint.into()),
            docs_url: None,
        }
    }

    fn from_streamline_error(err: &StreamlineError) -> Self {
        use crate::error::ErrorHint;

        let (error_code, status_hint, docs) = match err {
            StreamlineError::TopicNotFound(name) => (
                "TOPIC_NOT_FOUND",
                Some(format!(
                    "Available topics can be listed at GET /api/v1/topics. Create with POST /api/v1/topics {{\"name\": \"{}\", \"partitions\": 1}}",
                    name
                )),
                Some("https://docs.streamline.dev/api/topics"),
            ),
            StreamlineError::TopicAlreadyExists(_) => (
                "TOPIC_ALREADY_EXISTS",
                Some("Use a different name or DELETE the existing topic first".into()),
                Some("https://docs.streamline.dev/api/topics"),
            ),
            StreamlineError::PartitionNotFound(topic, partition) => (
                "PARTITION_NOT_FOUND",
                Some(format!(
                    "Topic '{}' has fewer than {} partitions. Check topic details at GET /api/v1/topics/{}",
                    topic, partition + 1, topic
                )),
                None,
            ),
            StreamlineError::InvalidOffset(_) => (
                "INVALID_OFFSET",
                Some("Use offset=0 or query the topic to find valid offset range".into()),
                None,
            ),
            StreamlineError::MessageTooLarge(size, max) => (
                "MESSAGE_TOO_LARGE",
                Some(format!(
                    "Message size {} exceeds limit {}. Split into smaller messages.",
                    size, max
                )),
                None,
            ),
            StreamlineError::InvalidTopicName(name) => (
                "INVALID_TOPIC_NAME",
                Some(format!(
                    "Topic name '{}' is invalid. Names must be 1-255 characters, alphanumeric with . _ - allowed",
                    name
                )),
                None,
            ),
            StreamlineError::AuthenticationFailed(_) => (
                "AUTHENTICATION_FAILED",
                Some("Check your API credentials or authentication headers".into()),
                Some("https://docs.streamline.dev/security/authentication"),
            ),
            StreamlineError::AuthorizationFailed(_) => (
                "AUTHORIZATION_FAILED",
                Some("You don't have permission for this operation. Contact your administrator.".into()),
                Some("https://docs.streamline.dev/security/authorization"),
            ),
            _ => {
                // Use the ErrorHint trait for other errors
                let hint_text = err.hint();
                ("INTERNAL_ERROR", hint_text, None)
            }
        };

        Self {
            error: error_code.to_string(),
            message: err.to_string(),
            hint: status_hint,
            docs_url: docs.map(String::from),
        }
    }
}

/// Query parameters for topic comparison
#[derive(Debug, Deserialize)]
pub struct CompareTopicsQuery {
    /// Comma-separated list of topic names to compare (2 topics required)
    pub topics: String,
}

/// Topic comparison response
#[derive(Debug, Serialize)]
pub struct TopicComparison {
    /// Details for each topic being compared
    pub topics: Vec<TopicDetails>,
    /// Configuration differences between topics
    pub config_diff: Vec<ConfigDifference>,
    /// Metrics comparison
    pub metrics_comparison: MetricsComparison,
}

/// Configuration difference between topics
#[derive(Debug, Serialize)]
pub struct ConfigDifference {
    /// Configuration key
    pub key: String,
    /// Value for each topic (in same order as topics array)
    pub values: Vec<Option<String>>,
    /// Whether all values are the same
    pub is_same: bool,
}

/// Metrics comparison between topics
#[derive(Debug, Serialize)]
pub struct MetricsComparison {
    /// Topic with more messages
    pub messages_leader: Option<String>,
    /// Topic with higher throughput
    pub throughput_leader: Option<String>,
    /// Topic with more partitions
    pub partitions_leader: Option<String>,
}

/// Create the REST API router
pub(crate) fn create_api_router(state: ApiState) -> Router {
    Router::new()
        .route("/api/v1/topics", get(list_topics))
        .route("/api/v1/topics", post(create_topic))
        .route("/api/v1/topics/compare", get(compare_topics))
        .route("/api/v1/topics/:topic", get(get_topic))
        .route("/api/v1/topics/:topic", delete(delete_topic))
        .route("/api/v1/topics/:topic/messages", post(produce_messages))
        .route(
            "/api/v1/topics/:topic/partitions/:partition/messages",
            get(consume_messages),
        )
        .with_state(state)
}

/// List all topics
async fn list_topics(State(state): State<ApiState>) -> Response {
    match state.topic_manager.list_topics() {
        Ok(topics) => {
            let mut topic_infos = Vec::with_capacity(topics.len());
            for metadata in topics {
                let (total_messages, total_bytes) = match compute_topic_totals(
                    &state.topic_manager,
                    &metadata.name,
                ) {
                    Ok(totals) => totals,
                    Err(e) => {
                        error!(error = %e, topic = %metadata.name, "Failed to compute topic totals");
                        let error = ErrorResponse::new("INTERNAL_ERROR", e.to_string());
                        return (StatusCode::INTERNAL_SERVER_ERROR, Json(error)).into_response();
                    }
                };

                topic_infos.push(TopicInfo {
                    name: metadata.name,
                    partition_count: metadata.num_partitions as usize,
                    replication_factor: metadata.replication_factor as usize,
                    is_internal: false, // Streamline doesn't have internal topics
                    total_messages,
                    total_bytes,
                });
            }
            (StatusCode::OK, Json(topic_infos)).into_response()
        }
        Err(e) => {
            error!(error = %e, "Failed to list topics");
            let error = ErrorResponse::new("INTERNAL_ERROR", e.to_string());
            (StatusCode::INTERNAL_SERVER_ERROR, Json(error)).into_response()
        }
    }
}

/// Create a new topic
async fn create_topic(
    State(state): State<ApiState>,
    Json(request): Json<CreateTopicRequest>,
) -> Response {
    let partitions = request.partitions.max(1);

    match state.topic_manager.create_topic(&request.name, partitions) {
        Ok(_) => {
            info!(topic = %request.name, partitions = partitions, "Topic created via REST API");
            let response = TopicDetails {
                name: request.name.clone(),
                partition_count: partitions as usize,
                replication_factor: 1,
                is_internal: false,
                partitions: (0..partitions)
                    .map(|p| PartitionInfo {
                        partition_id: p,
                        leader: Some(1),
                        replicas: vec![1],
                        isr: vec![1],
                        start_offset: 0,
                        end_offset: 0,
                        size_bytes: 0,
                    })
                    .collect(),
                config: std::collections::HashMap::new(),
                total_messages: 0,
                total_bytes: 0,
                messages_per_second: 0.0,
                bytes_per_second: 0.0,
            };
            (StatusCode::CREATED, Json(response)).into_response()
        }
        Err(e @ StreamlineError::TopicAlreadyExists(_)) => {
            let error = ErrorResponse::from_streamline_error(&e);
            (StatusCode::CONFLICT, Json(error)).into_response()
        }
        Err(e) => {
            error!(error = %e, "Failed to create topic");
            let error = ErrorResponse::from_streamline_error(&e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(error)).into_response()
        }
    }
}

/// Get topic metadata
async fn get_topic(State(state): State<ApiState>, Path(topic): Path<String>) -> Response {
    match state.topic_manager.get_topic_metadata(&topic) {
        Ok(metadata) => {
            let (partitions, total_messages, total_bytes) =
                match build_partition_details(&state.topic_manager, &topic) {
                    Ok(details) => details,
                    Err(e) => {
                        error!(error = %e, topic = %topic, "Failed to build partition details");
                        let error = ErrorResponse::from_streamline_error(&e);
                        return (StatusCode::INTERNAL_SERVER_ERROR, Json(error)).into_response();
                    }
                };

            // Expose topic configuration from TopicMetadata
            let mut config_map = std::collections::HashMap::new();
            config_map.insert(
                "retention.ms".to_string(),
                metadata.config.retention_ms.to_string(),
            );
            config_map.insert(
                "retention.bytes".to_string(),
                metadata.config.retention_bytes.to_string(),
            );
            config_map.insert(
                "segment.bytes".to_string(),
                metadata.config.segment_bytes.to_string(),
            );
            config_map.insert(
                "cleanup.policy".to_string(),
                format!("{:?}", metadata.config.cleanup_policy).to_lowercase(),
            );
            config_map.insert(
                "message.ttl.ms".to_string(),
                metadata.config.message_ttl_ms.to_string(),
            );

            // Basic rate tracking: compute approximate rates from partition stats.
            // A production implementation would use a sliding-window counter updated
            // on each produce call; here we report 0.0 as baseline until the rate
            // tracker is integrated into the produce path.
            let messages_per_second = 0.0_f64;
            let bytes_per_second = 0.0_f64;

            let response = TopicDetails {
                name: topic,
                partition_count: metadata.num_partitions as usize,
                replication_factor: metadata.replication_factor as usize,
                is_internal: false,
                partitions,
                config: config_map,
                total_messages,
                total_bytes,
                messages_per_second,
                bytes_per_second,
            };
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e @ StreamlineError::TopicNotFound(_)) => {
            let error = ErrorResponse::from_streamline_error(&e);
            (StatusCode::NOT_FOUND, Json(error)).into_response()
        }
        Err(e) => {
            error!(error = %e, "Failed to get topic metadata");
            let error = ErrorResponse::from_streamline_error(&e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(error)).into_response()
        }
    }
}

/// Delete a topic
async fn delete_topic(State(state): State<ApiState>, Path(topic): Path<String>) -> Response {
    match state.topic_manager.delete_topic(&topic) {
        Ok(_) => {
            info!(topic = %topic, "Topic deleted via REST API");
            (StatusCode::NO_CONTENT, "").into_response()
        }
        Err(e @ StreamlineError::TopicNotFound(_)) => {
            let error = ErrorResponse::from_streamline_error(&e);
            (StatusCode::NOT_FOUND, Json(error)).into_response()
        }
        Err(e) => {
            error!(error = %e, "Failed to delete topic");
            let error = ErrorResponse::from_streamline_error(&e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(error)).into_response()
        }
    }
}

/// Produce messages to a topic
async fn produce_messages(
    State(state): State<ApiState>,
    Path(topic): Path<String>,
    Json(request): Json<ProduceRequest>,
) -> Response {
    // Get or create the topic and get its partition count
    if let Err(e) = state.topic_manager.get_or_create_topic(&topic, 1) {
        error!(error = %e, "Failed to get/create topic for produce");
        let error = ErrorResponse::from_streamline_error(&e);
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(error)).into_response();
    }

    // Get topic metadata to determine partition count
    let num_partitions = match state.topic_manager.get_topic_metadata(&topic) {
        Ok(m) => m.num_partitions,
        Err(e) => {
            error!(error = %e, "Failed to get topic metadata");
            let error = ErrorResponse::from_streamline_error(&e);
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(error)).into_response();
        }
    };

    let mut offsets = Vec::new();

    for record in request.records {
        // Determine partition
        let partition = if let Some(p) = record.partition {
            p % num_partitions
        } else if let Some(ref key) = record.key {
            // Simple hash-based partitioning
            let hash = key.bytes().fold(0u32, |acc, b| acc.wrapping_add(b as u32));
            (hash % num_partitions as u32) as i32
        } else {
            // Round-robin (simple: just use 0 for now)
            0
        };

        // Convert value to bytes
        let value_bytes = match serde_json::to_vec(&record.value) {
            Ok(v) => Bytes::from(v),
            Err(e) => {
                let error = ErrorResponse::new("INVALID_VALUE", e.to_string());
                return (StatusCode::BAD_REQUEST, Json(error)).into_response();
            }
        };

        // Convert key to bytes
        let key_bytes = record.key.map(|k| Bytes::from(k.into_bytes()));

        // Convert headers
        let headers: Vec<Header> = record
            .headers
            .into_iter()
            .map(|(k, v)| Header {
                key: k,
                value: Bytes::from(v.into_bytes()),
            })
            .collect();

        // Append to partition using TopicManager with headers
        match state.topic_manager.append_with_headers(
            &topic,
            partition,
            key_bytes,
            value_bytes,
            headers,
        ) {
            Ok(offset) => {
                debug!(topic = %topic, partition = partition, offset = offset, "Record produced via REST API");
                offsets.push(ProduceOffsetResult { partition, offset });
            }
            Err(e) => {
                error!(error = %e, "Failed to produce record");
                let error = ErrorResponse::from_streamline_error(&e);
                return (StatusCode::INTERNAL_SERVER_ERROR, Json(error)).into_response();
            }
        }
    }

    let response = ProduceResponse { offsets };
    (StatusCode::OK, Json(response)).into_response()
}

/// Consume messages from a topic partition
async fn consume_messages(
    State(state): State<ApiState>,
    Path((topic, partition)): Path<(String, i32)>,
    Query(query): Query<ConsumeQuery>,
) -> Response {
    // Read records from partition using TopicManager
    match state
        .topic_manager
        .read(&topic, partition, query.offset, query.limit)
    {
        Ok(records) => {
            let next_offset = records.last().map(|r| r.offset + 1).unwrap_or(query.offset);

            let consume_records: Vec<ConsumeRecord> = records
                .into_iter()
                .map(|r| {
                    // Try to parse value as JSON, fall back to string
                    let value = match serde_json::from_slice(&r.value) {
                        Ok(v) => v,
                        Err(_) => {
                            // Fall back to string representation
                            serde_json::Value::String(String::from_utf8_lossy(&r.value).to_string())
                        }
                    };

                    let key = r.key.map(|k| String::from_utf8_lossy(&k).to_string());

                    let headers: std::collections::HashMap<String, String> = r
                        .headers
                        .into_iter()
                        .map(|h| (h.key, String::from_utf8_lossy(&h.value).to_string()))
                        .collect();

                    ConsumeRecord {
                        offset: r.offset,
                        timestamp: r.timestamp,
                        key,
                        value,
                        headers,
                    }
                })
                .collect();

            let response = ConsumeResponse {
                topic: topic.clone(),
                partition,
                records: consume_records,
                next_offset,
            };
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e @ StreamlineError::TopicNotFound(_)) => {
            let error = ErrorResponse::from_streamline_error(&e);
            (StatusCode::NOT_FOUND, Json(error)).into_response()
        }
        Err(e @ StreamlineError::PartitionNotFound(_, _)) => {
            let error = ErrorResponse::from_streamline_error(&e);
            (StatusCode::NOT_FOUND, Json(error)).into_response()
        }
        Err(e) => {
            error!(error = %e, "Failed to consume records");
            let error = ErrorResponse::from_streamline_error(&e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(error)).into_response()
        }
    }
}

/// Compare two topics
async fn compare_topics(
    State(state): State<ApiState>,
    Query(query): Query<CompareTopicsQuery>,
) -> Response {
    // Parse comma-separated topic names
    let topic_names: Vec<&str> = query.topics.split(',').map(|s| s.trim()).collect();

    if topic_names.len() != 2 {
        let error = ErrorResponse::new(
            "INVALID_REQUEST",
            "Exactly 2 topics must be provided for comparison",
        );
        return (StatusCode::BAD_REQUEST, Json(error)).into_response();
    }

    let mut topic_details = Vec::new();

    // Fetch details for each topic
    for topic_name in &topic_names {
        match state.topic_manager.get_topic_metadata(topic_name) {
            Ok(metadata) => {
                let (partitions, total_messages, total_bytes) =
                    match build_partition_details(&state.topic_manager, topic_name) {
                        Ok(details) => details,
                        Err(e) => {
                            error!(
                                error = %e,
                                topic = %topic_name,
                                "Failed to build partition details for comparison"
                            );
                            let error = ErrorResponse::new("INTERNAL_ERROR", e.to_string());
                            return (StatusCode::INTERNAL_SERVER_ERROR, Json(error))
                                .into_response();
                        }
                    };

                topic_details.push(TopicDetails {
                    name: topic_name.to_string(),
                    partition_count: metadata.num_partitions as usize,
                    replication_factor: metadata.replication_factor as usize,
                    is_internal: false,
                    partitions,
                    config: std::collections::HashMap::new(),
                    total_messages,
                    total_bytes,
                    messages_per_second: 0.0,
                    bytes_per_second: 0.0,
                });
            }
            Err(StreamlineError::TopicNotFound(_)) => {
                let error = ErrorResponse::new(
                    "TOPIC_NOT_FOUND",
                    format!("Topic '{}' does not exist", topic_name),
                );
                return (StatusCode::NOT_FOUND, Json(error)).into_response();
            }
            Err(e) => {
                error!(error = %e, "Failed to get topic metadata for comparison");
                let error = ErrorResponse::new("INTERNAL_ERROR", e.to_string());
                return (StatusCode::INTERNAL_SERVER_ERROR, Json(error)).into_response();
            }
        }
    }

    // Build config differences
    let mut all_keys: std::collections::HashSet<String> = std::collections::HashSet::new();
    for td in &topic_details {
        all_keys.extend(td.config.keys().cloned());
    }

    // Standard Kafka config keys to always show in comparison
    let standard_keys = [
        "retention.ms",
        "retention.bytes",
        "cleanup.policy",
        "compression.type",
        "max.message.bytes",
        "min.insync.replicas",
        "segment.bytes",
        "segment.ms",
    ];

    for key in standard_keys {
        all_keys.insert(key.to_string());
    }

    let config_diff: Vec<ConfigDifference> = all_keys
        .into_iter()
        .map(|key| {
            let values: Vec<Option<String>> = topic_details
                .iter()
                .map(|td| td.config.get(&key).cloned())
                .collect();
            let is_same = values.windows(2).all(|w| w[0] == w[1]);
            ConfigDifference {
                key,
                values,
                is_same,
            }
        })
        .collect();

    // Determine leaders in metrics
    let messages_leader = if topic_details.len() == 2 {
        if topic_details[0].total_messages > topic_details[1].total_messages {
            Some(topic_details[0].name.clone())
        } else if topic_details[1].total_messages > topic_details[0].total_messages {
            Some(topic_details[1].name.clone())
        } else {
            None
        }
    } else {
        None
    };

    let throughput_leader = if topic_details.len() == 2 {
        if topic_details[0].messages_per_second > topic_details[1].messages_per_second {
            Some(topic_details[0].name.clone())
        } else if topic_details[1].messages_per_second > topic_details[0].messages_per_second {
            Some(topic_details[1].name.clone())
        } else {
            None
        }
    } else {
        None
    };

    let partitions_leader = if topic_details.len() == 2 {
        if topic_details[0].partition_count > topic_details[1].partition_count {
            Some(topic_details[0].name.clone())
        } else if topic_details[1].partition_count > topic_details[0].partition_count {
            Some(topic_details[1].name.clone())
        } else {
            None
        }
    } else {
        None
    };

    let comparison = TopicComparison {
        topics: topic_details,
        config_diff,
        metrics_comparison: MetricsComparison {
            messages_leader,
            throughput_leader,
            partitions_leader,
        },
    };

    (StatusCode::OK, Json(comparison)).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tempfile::TempDir;
    use tower::util::ServiceExt;

    fn create_test_state() -> (ApiState, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());
        let state = ApiState { topic_manager };
        (state, temp_dir)
    }

    #[tokio::test]
    async fn test_list_topics_empty() {
        let (state, _temp_dir) = create_test_state();
        let app = create_api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/topics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_create_topic() {
        let (state, _temp_dir) = create_test_state();
        let app = create_api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/topics")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"name": "test-topic", "partitions": 3}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn test_produce_and_consume() {
        let (state, _temp_dir) = create_test_state();
        let _app = create_api_router(state.clone());

        // Create topic first
        let _ = state.topic_manager.create_topic("test-topic", 1);

        // Produce a message
        let app_clone = create_api_router(state.clone());
        let response = app_clone
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/topics/test-topic/messages")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"records": [{"key": "key1", "value": {"msg": "hello"}}]}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        // Consume the message
        let app_clone = create_api_router(state);
        let response = app_clone
            .oneshot(
                Request::builder()
                    .uri("/api/v1/topics/test-topic/partitions/0/messages?offset=0&limit=10")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_nonexistent_topic() {
        let (state, _temp_dir) = create_test_state();
        let app = create_api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/topics/nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_delete_topic() {
        let (state, _temp_dir) = create_test_state();

        // Create topic first
        let _ = state.topic_manager.create_topic("to-delete", 1);

        let app = create_api_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/v1/topics/to-delete")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    #[test]
    fn test_build_partition_details_bytes() {
        let (state, _temp_dir) = create_test_state();
        let topic_manager = &state.topic_manager;

        topic_manager.create_topic("bytes-topic", 1).unwrap();
        topic_manager
            .append(
                "bytes-topic",
                0,
                Some(Bytes::from_static(b"key")),
                Bytes::from_static(b"value"),
            )
            .unwrap();

        let (partitions, total_messages, total_bytes) =
            build_partition_details(topic_manager, "bytes-topic").unwrap();

        assert_eq!(partitions.len(), 1);
        assert_eq!(total_messages, 1);
        assert!(total_bytes > 0);
        assert!(partitions[0].size_bytes > 0);
    }

    #[test]
    fn test_compute_topic_totals_missing_topic() {
        let (state, _temp_dir) = create_test_state();
        let err = compute_topic_totals(&state.topic_manager, "missing-topic").unwrap_err();
        assert!(matches!(err, StreamlineError::TopicNotFound(_)));
    }
}
