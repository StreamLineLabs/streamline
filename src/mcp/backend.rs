//! MCP Backend trait and implementations
//!
//! Defines the `McpBackend` trait for abstracting Streamline operations
//! that MCP tools and resources can invoke. Enables dependency injection
//! and testability via mock backends.
//!
//! ## Stability: Experimental

use crate::error::StreamlineError;
use crate::storage::TopicManager;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

// ─── MCP Error Types ────────────────────────────────────────────────

/// MCP-specific error with JSON-RPC error code mapping.
#[derive(Debug, Clone)]
pub struct McpError {
    pub code: McpErrorCode,
    pub message: String,
    pub data: Option<serde_json::Value>,
}

/// JSON-RPC and MCP error codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpErrorCode {
    /// JSON-RPC: Parse error (-32700)
    ParseError,
    /// JSON-RPC: Invalid request (-32600)
    InvalidRequest,
    /// JSON-RPC: Method not found (-32601)
    MethodNotFound,
    /// JSON-RPC: Invalid params (-32602)
    InvalidParams,
    /// JSON-RPC: Internal error (-32603)
    InternalError,
    /// MCP: Resource not found (-32001)
    ResourceNotFound,
    /// MCP: Topic not found (-32002)
    TopicNotFound,
    /// MCP: Partition not found (-32003)
    PartitionNotFound,
    /// MCP: Topic already exists (-32004)
    TopicAlreadyExists,
    /// MCP: Feature not available (-32005)
    NotSupported,
}

impl McpErrorCode {
    pub fn json_rpc_code(self) -> i32 {
        match self {
            Self::ParseError => -32700,
            Self::InvalidRequest => -32600,
            Self::MethodNotFound => -32601,
            Self::InvalidParams => -32602,
            Self::InternalError => -32603,
            Self::ResourceNotFound => -32001,
            Self::TopicNotFound => -32002,
            Self::PartitionNotFound => -32003,
            Self::TopicAlreadyExists => -32004,
            Self::NotSupported => -32005,
        }
    }
}

impl std::fmt::Display for McpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for McpError {}

impl McpError {
    pub fn parse_error(msg: impl Into<String>) -> Self {
        Self {
            code: McpErrorCode::ParseError,
            message: msg.into(),
            data: None,
        }
    }

    pub fn invalid_request(msg: impl Into<String>) -> Self {
        Self {
            code: McpErrorCode::InvalidRequest,
            message: msg.into(),
            data: None,
        }
    }

    pub fn method_not_found(msg: impl Into<String>) -> Self {
        Self {
            code: McpErrorCode::MethodNotFound,
            message: msg.into(),
            data: None,
        }
    }

    pub fn invalid_params(msg: impl Into<String>) -> Self {
        Self {
            code: McpErrorCode::InvalidParams,
            message: msg.into(),
            data: None,
        }
    }

    pub fn internal(msg: impl Into<String>) -> Self {
        Self {
            code: McpErrorCode::InternalError,
            message: msg.into(),
            data: None,
        }
    }

    pub fn topic_not_found(name: impl Into<String>) -> Self {
        let name = name.into();
        Self {
            code: McpErrorCode::TopicNotFound,
            message: format!("Topic not found: {}", name),
            data: None,
        }
    }

    pub fn partition_not_found(topic: &str, partition: i32) -> Self {
        Self {
            code: McpErrorCode::PartitionNotFound,
            message: format!(
                "Partition {} not found in topic '{}'",
                partition, topic
            ),
            data: None,
        }
    }

    pub fn topic_already_exists(name: impl Into<String>) -> Self {
        let name = name.into();
        Self {
            code: McpErrorCode::TopicAlreadyExists,
            message: format!("Topic already exists: {}", name),
            data: None,
        }
    }

    pub fn not_supported(feature: impl Into<String>) -> Self {
        Self {
            code: McpErrorCode::NotSupported,
            message: format!("Feature not available: {}", feature.into()),
            data: None,
        }
    }

    pub fn resource_not_found(uri: impl Into<String>) -> Self {
        Self {
            code: McpErrorCode::ResourceNotFound,
            message: format!("Resource not found: {}", uri.into()),
            data: None,
        }
    }
}

impl From<StreamlineError> for McpError {
    fn from(e: StreamlineError) -> Self {
        match &e {
            StreamlineError::TopicNotFound(name) => McpError::topic_not_found(name.clone()),
            StreamlineError::PartitionNotFound(topic, partition) => {
                McpError::partition_not_found(topic, *partition)
            }
            StreamlineError::TopicAlreadyExists(name) => {
                McpError::topic_already_exists(name.clone())
            }
            StreamlineError::Validation(msg) => McpError::invalid_params(msg.clone()),
            StreamlineError::InvalidTopicName(name) => {
                McpError::invalid_params(format!("Invalid topic name: {}", name))
            }
            StreamlineError::InvalidPartitionCount(msg) => McpError::invalid_params(msg.clone()),
            _ => McpError::internal(e.to_string()),
        }
    }
}

impl From<McpError> for StreamlineError {
    fn from(e: McpError) -> Self {
        StreamlineError::Mcp(e.message)
    }
}

/// Result type for MCP operations.
pub type McpResult<T> = std::result::Result<T, McpError>;

// ─── Backend Data Types ─────────────────────────────────────────────

/// Summary info for a topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicInfo {
    pub name: String,
    pub partitions: i32,
    pub replication_factor: i16,
    pub retention_ms: i64,
    pub total_messages: i64,
    pub created_at: i64,
}

/// Detailed topic information with partition-level data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicDetail {
    pub name: String,
    pub partitions: Vec<PartitionDetail>,
    pub replication_factor: i16,
    pub config: TopicConfigInfo,
    pub created_at: i64,
    pub total_messages: i64,
}

/// Per-partition detail.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionDetail {
    pub id: i32,
    pub start_offset: i64,
    pub end_offset: i64,
    pub message_count: i64,
    pub high_watermark: i64,
}

/// Topic configuration exposed to MCP clients.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicConfigInfo {
    pub retention_ms: i64,
    pub retention_bytes: i64,
    pub segment_bytes: u64,
    pub cleanup_policy: String,
    pub message_ttl_ms: i64,
}

/// Result of producing a message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProduceResult {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

/// A single message with metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageInfo {
    pub offset: i64,
    pub timestamp: i64,
    pub key: Option<String>,
    pub value: String,
    pub headers: Vec<(String, String)>,
}

/// Consumer group information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroupInfo {
    pub group_id: String,
    pub state: String,
    pub members: Vec<GroupMemberInfo>,
    pub total_lag: i64,
}

/// Consumer group member info.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupMemberInfo {
    pub member_id: String,
    pub client_id: String,
    pub assignments: Vec<PartitionAssignment>,
}

/// Partition assignment for a consumer group member.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionAssignment {
    pub topic: String,
    pub partition: i32,
    pub current_offset: i64,
    pub end_offset: i64,
    pub lag: i64,
}

/// Query execution result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub row_count: usize,
    pub execution_time_ms: u64,
}

/// Server metrics snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    pub messages_in_per_sec: f64,
    pub messages_out_per_sec: f64,
    pub bytes_in_per_sec: f64,
    pub bytes_out_per_sec: f64,
    pub total_topics: u32,
    pub total_partitions: u32,
    pub total_messages: u64,
    pub storage_bytes: u64,
    pub active_connections: u32,
    pub uptime_seconds: u64,
}

impl Default for MetricsSnapshot {
    fn default() -> Self {
        Self {
            messages_in_per_sec: 0.0,
            messages_out_per_sec: 0.0,
            bytes_in_per_sec: 0.0,
            bytes_out_per_sec: 0.0,
            total_topics: 0,
            total_partitions: 0,
            total_messages: 0,
            storage_bytes: 0,
            active_connections: 0,
            uptime_seconds: 0,
        }
    }
}

/// Server configuration (secrets redacted).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfigInfo {
    pub version: String,
    pub kafka_port: u16,
    pub http_port: u16,
    pub storage_mode: String,
    pub features: Vec<String>,
    pub limits: ServerLimits,
}

/// Server limits.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerLimits {
    pub max_message_size: usize,
    pub max_partitions_per_topic: i32,
    pub max_topics: u32,
}

// ─── McpBackend Trait ───────────────────────────────────────────────

/// Trait for Streamline operations that MCP tools can invoke.
///
/// Implementations provide the actual business logic for tool execution.
/// This abstraction allows dependency injection and testability.
#[async_trait::async_trait]
pub trait McpBackend: Send + Sync {
    /// List all topics with summary info.
    async fn list_topics(&self) -> McpResult<Vec<TopicInfo>>;

    /// Get detailed topic information.
    async fn describe_topic(&self, name: &str) -> McpResult<TopicDetail>;

    /// Produce a message to a topic.
    async fn produce(
        &self,
        topic: &str,
        key: Option<&str>,
        value: &str,
        partition: Option<i32>,
    ) -> McpResult<ProduceResult>;

    /// Consume messages from a topic partition.
    async fn consume(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        count: u32,
    ) -> McpResult<Vec<MessageInfo>>;

    /// List consumer groups.
    async fn list_consumer_groups(&self) -> McpResult<Vec<ConsumerGroupInfo>>;

    /// Execute a SQL/StreamQL query.
    async fn query(&self, sql: &str) -> McpResult<QueryResult>;

    /// Get server metrics.
    async fn get_metrics(&self) -> McpResult<MetricsSnapshot>;

    /// Create a new topic.
    async fn create_topic(
        &self,
        name: &str,
        partitions: u32,
        retention_ms: Option<i64>,
    ) -> McpResult<()>;

    /// Delete a topic.
    async fn delete_topic(&self, name: &str) -> McpResult<()>;

    /// Get server configuration (secrets redacted).
    async fn get_server_config(&self) -> McpResult<ServerConfigInfo>;

    /// Search messages in a topic by regex pattern.
    async fn search_messages(
        &self,
        topic: &str,
        partition: i32,
        pattern: &str,
        max_results: usize,
    ) -> McpResult<Vec<MessageInfo>>;
}

// ─── TopicManagerBackend ────────────────────────────────────────────

/// Backend implementation backed by `TopicManager`.
pub struct TopicManagerBackend {
    topic_manager: Arc<TopicManager>,
}

impl TopicManagerBackend {
    pub fn new(topic_manager: Arc<TopicManager>) -> Self {
        Self { topic_manager }
    }
}

#[async_trait::async_trait]
impl McpBackend for TopicManagerBackend {
    async fn list_topics(&self) -> McpResult<Vec<TopicInfo>> {
        let topics = self.topic_manager.list_topics().map_err(McpError::from)?;
        let mut result = Vec::with_capacity(topics.len());
        for meta in &topics {
            let mut total_messages: i64 = 0;
            for i in 0..meta.num_partitions {
                let start = self
                    .topic_manager
                    .earliest_offset(&meta.name, i)
                    .unwrap_or(0);
                let end = self
                    .topic_manager
                    .latest_offset(&meta.name, i)
                    .unwrap_or(0);
                total_messages += end - start;
            }
            result.push(TopicInfo {
                name: meta.name.clone(),
                partitions: meta.num_partitions,
                replication_factor: meta.replication_factor,
                retention_ms: meta.config.retention_ms,
                total_messages,
                created_at: meta.created_at,
            });
        }
        Ok(result)
    }

    async fn describe_topic(&self, name: &str) -> McpResult<TopicDetail> {
        let meta = self
            .topic_manager
            .get_topic_metadata(name)
            .map_err(McpError::from)?;
        let mut partitions = Vec::with_capacity(meta.num_partitions as usize);
        let mut total_messages: i64 = 0;
        for i in 0..meta.num_partitions {
            let start = self
                .topic_manager
                .earliest_offset(name, i)
                .unwrap_or(0);
            let end = self.topic_manager.latest_offset(name, i).unwrap_or(0);
            let hw = self
                .topic_manager
                .high_watermark(name, i)
                .unwrap_or(end);
            let count = end - start;
            total_messages += count;
            partitions.push(PartitionDetail {
                id: i,
                start_offset: start,
                end_offset: end,
                message_count: count,
                high_watermark: hw,
            });
        }
        let config = TopicConfigInfo {
            retention_ms: meta.config.retention_ms,
            retention_bytes: meta.config.retention_bytes,
            segment_bytes: meta.config.segment_bytes,
            cleanup_policy: format!("{:?}", meta.config.cleanup_policy),
            message_ttl_ms: meta.config.message_ttl_ms,
        };
        Ok(TopicDetail {
            name: meta.name.clone(),
            partitions,
            replication_factor: meta.replication_factor,
            config,
            created_at: meta.created_at,
            total_messages,
        })
    }

    async fn produce(
        &self,
        topic: &str,
        key: Option<&str>,
        value: &str,
        partition: Option<i32>,
    ) -> McpResult<ProduceResult> {
        let partition = partition.unwrap_or(0);
        let key_bytes = key.map(|k| Bytes::from(k.to_string()));
        let value_bytes = Bytes::from(value.to_string());
        let offset = self
            .topic_manager
            .append(topic, partition, key_bytes, value_bytes)
            .map_err(McpError::from)?;
        Ok(ProduceResult {
            topic: topic.to_string(),
            partition,
            offset,
        })
    }

    async fn consume(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        count: u32,
    ) -> McpResult<Vec<MessageInfo>> {
        let meta = self
            .topic_manager
            .get_topic_metadata(topic)
            .map_err(McpError::from)?;
        if partition >= meta.num_partitions {
            return Err(McpError::partition_not_found(topic, partition));
        }
        let end_offset = self
            .topic_manager
            .latest_offset(topic, partition)
            .map_err(McpError::from)?;
        let start = if offset < 0 {
            (end_offset - count as i64).max(0)
        } else {
            offset
        };
        let records = self
            .topic_manager
            .read(topic, partition, start, count as usize)
            .map_err(McpError::from)?;
        Ok(records
            .iter()
            .map(|r| MessageInfo {
                offset: r.offset,
                timestamp: r.timestamp,
                key: r.key
                    .as_ref()
                    .map(|k| String::from_utf8_lossy(k).to_string()),
                value: String::from_utf8_lossy(&r.value).to_string(),
                headers: r
                    .headers
                    .iter()
                    .map(|h| {
                        (
                            h.key.clone(),
                            String::from_utf8_lossy(&h.value).to_string(),
                        )
                    })
                    .collect(),
            })
            .collect())
    }

    async fn list_consumer_groups(&self) -> McpResult<Vec<ConsumerGroupInfo>> {
        // TopicManager doesn't manage consumer groups directly;
        // the group coordinator is a separate component.
        Ok(Vec::new())
    }

    async fn query(&self, _sql: &str) -> McpResult<QueryResult> {
        Err(McpError::not_supported(
            "SQL queries require the analytics engine feature",
        ))
    }

    async fn get_metrics(&self) -> McpResult<MetricsSnapshot> {
        let stats = self
            .topic_manager
            .get_all_topic_stats()
            .map_err(McpError::from)?;
        let total_topics = stats.len() as u32;
        let mut total_partitions: u32 = 0;
        let mut total_messages: u64 = 0;
        for s in &stats {
            total_partitions += s.num_partitions as u32;
            total_messages += s.total_messages;
        }
        Ok(MetricsSnapshot {
            messages_in_per_sec: 0.0,
            messages_out_per_sec: 0.0,
            bytes_in_per_sec: 0.0,
            bytes_out_per_sec: 0.0,
            total_topics,
            total_partitions,
            total_messages,
            storage_bytes: 0,
            active_connections: 0,
            uptime_seconds: 0,
        })
    }

    async fn create_topic(
        &self,
        name: &str,
        partitions: u32,
        retention_ms: Option<i64>,
    ) -> McpResult<()> {
        if let Some(retention) = retention_ms {
            let mut config = crate::storage::TopicConfig::default();
            config.retention_ms = retention;
            self.topic_manager
                .create_topic_with_config(name, partitions as i32, config)
                .map_err(McpError::from)?;
        } else {
            self.topic_manager
                .create_topic(name, partitions as i32)
                .map_err(McpError::from)?;
        }
        Ok(())
    }

    async fn delete_topic(&self, name: &str) -> McpResult<()> {
        self.topic_manager
            .delete_topic(name)
            .map_err(McpError::from)?;
        Ok(())
    }

    async fn get_server_config(&self) -> McpResult<ServerConfigInfo> {
        Ok(ServerConfigInfo {
            version: env!("CARGO_PKG_VERSION").to_string(),
            kafka_port: 9092,
            http_port: 9094,
            storage_mode: "persistent".to_string(),
            features: vec![
                "kafka-protocol".to_string(),
                "mcp".to_string(),
                "http-api".to_string(),
            ],
            limits: ServerLimits {
                max_message_size: 1_048_576,
                max_partitions_per_topic: 1024,
                max_topics: 10_000,
            },
        })
    }

    async fn search_messages(
        &self,
        topic: &str,
        partition: i32,
        pattern: &str,
        max_results: usize,
    ) -> McpResult<Vec<MessageInfo>> {
        let re = regex::Regex::new(pattern)
            .map_err(|e| McpError::invalid_params(format!("Invalid regex: {}", e)))?;

        let end_offset = self
            .topic_manager
            .latest_offset(topic, partition)
            .map_err(McpError::from)?;
        let scan_start = (end_offset - 1000).max(0);
        let records = self
            .topic_manager
            .read(topic, partition, scan_start, 1000)
            .map_err(McpError::from)?;

        let mut matches = Vec::new();
        for record in &records {
            if matches.len() >= max_results {
                break;
            }
            let text = String::from_utf8_lossy(&record.value);
            if re.is_match(&text) {
                matches.push(MessageInfo {
                    offset: record.offset,
                    timestamp: record.timestamp,
                    key: record
                        .key
                        .as_ref()
                        .map(|k| String::from_utf8_lossy(k).to_string()),
                    value: text.to_string(),
                    headers: record
                        .headers
                        .iter()
                        .map(|h| {
                            (
                                h.key.clone(),
                                String::from_utf8_lossy(&h.value).to_string(),
                            )
                        })
                        .collect(),
                });
            }
        }
        Ok(matches)
    }
}

// ─── MockMcpBackend (test only) ─────────────────────────────────────

#[cfg(test)]
pub use self::mock::MockMcpBackend;

#[cfg(test)]
mod mock {
    use super::*;
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::sync::Mutex;

    /// Mock backend for testing MCP tools and resources.
    pub struct MockMcpBackend {
        pub topics: Mutex<Vec<TopicInfo>>,
        pub messages: Mutex<HashMap<String, Vec<MessageInfo>>>,
        pub consumer_groups: Mutex<Vec<ConsumerGroupInfo>>,
        pub metrics: Mutex<MetricsSnapshot>,
        pub produced_messages: Mutex<Vec<ProduceResult>>,
        pub deleted_topics: Mutex<Vec<String>>,
        pub created_topics: Mutex<Vec<(String, u32, Option<i64>)>>,
        pub next_offset: AtomicI64,
        pub fail_method: Mutex<Option<String>>,
    }

    impl MockMcpBackend {
        pub fn new() -> Self {
            Self {
                topics: Mutex::new(Vec::new()),
                messages: Mutex::new(HashMap::new()),
                consumer_groups: Mutex::new(Vec::new()),
                metrics: Mutex::new(MetricsSnapshot::default()),
                produced_messages: Mutex::new(Vec::new()),
                deleted_topics: Mutex::new(Vec::new()),
                created_topics: Mutex::new(Vec::new()),
                next_offset: AtomicI64::new(0),
                fail_method: Mutex::new(None),
            }
        }

        /// Add a topic to the mock.
        pub fn add_topic(&self, name: &str, partitions: i32) {
            self.topics.lock().unwrap().push(TopicInfo {
                name: name.to_string(),
                partitions,
                replication_factor: 1,
                retention_ms: 604_800_000,
                total_messages: 0,
                created_at: 1700000000000,
            });
        }

        /// Add messages for a topic.
        pub fn add_messages(&self, topic: &str, msgs: Vec<MessageInfo>) {
            let count = msgs.len() as i64;
            self.messages
                .lock()
                .unwrap()
                .insert(topic.to_string(), msgs);
            // Update topic total_messages
            let mut topics = self.topics.lock().unwrap();
            if let Some(t) = topics.iter_mut().find(|t| t.name == topic) {
                t.total_messages = count;
            }
        }

        /// Set a method to fail on next call.
        pub fn set_fail_method(&self, method: Option<&str>) {
            *self.fail_method.lock().unwrap() = method.map(|s| s.to_string());
        }

        fn check_fail(&self, method: &str) -> McpResult<()> {
            let fail = self.fail_method.lock().unwrap();
            if fail.as_deref() == Some(method) {
                Err(McpError::internal(format!(
                    "Mock error: {} failed",
                    method
                )))
            } else {
                Ok(())
            }
        }
    }

    #[async_trait::async_trait]
    impl McpBackend for MockMcpBackend {
        async fn list_topics(&self) -> McpResult<Vec<TopicInfo>> {
            self.check_fail("list_topics")?;
            Ok(self.topics.lock().unwrap().clone())
        }

        async fn describe_topic(&self, name: &str) -> McpResult<TopicDetail> {
            self.check_fail("describe_topic")?;
            let topics = self.topics.lock().unwrap();
            let topic = topics
                .iter()
                .find(|t| t.name == name)
                .ok_or_else(|| McpError::topic_not_found(name))?;

            let messages = self.messages.lock().unwrap();
            let msg_count = messages
                .get(name)
                .map(|m| m.len() as i64)
                .unwrap_or(0);

            let mut partitions = Vec::new();
            for i in 0..topic.partitions {
                let count = if i == 0 { msg_count } else { 0 };
                partitions.push(PartitionDetail {
                    id: i,
                    start_offset: 0,
                    end_offset: count,
                    message_count: count,
                    high_watermark: count,
                });
            }

            Ok(TopicDetail {
                name: name.to_string(),
                partitions,
                replication_factor: topic.replication_factor,
                config: TopicConfigInfo {
                    retention_ms: topic.retention_ms,
                    retention_bytes: -1,
                    segment_bytes: 104_857_600,
                    cleanup_policy: "Delete".to_string(),
                    message_ttl_ms: -1,
                },
                created_at: topic.created_at,
                total_messages: msg_count,
            })
        }

        async fn produce(
            &self,
            topic: &str,
            _key: Option<&str>,
            _value: &str,
            partition: Option<i32>,
        ) -> McpResult<ProduceResult> {
            self.check_fail("produce")?;
            let topics = self.topics.lock().unwrap();
            if !topics.iter().any(|t| t.name == topic) {
                return Err(McpError::topic_not_found(topic));
            }
            drop(topics);

            let offset = self.next_offset.fetch_add(1, Ordering::SeqCst);
            let result = ProduceResult {
                topic: topic.to_string(),
                partition: partition.unwrap_or(0),
                offset,
            };
            self.produced_messages.lock().unwrap().push(result.clone());
            Ok(result)
        }

        async fn consume(
            &self,
            topic: &str,
            _partition: i32,
            offset: i64,
            count: u32,
        ) -> McpResult<Vec<MessageInfo>> {
            self.check_fail("consume")?;
            let messages = self.messages.lock().unwrap();
            let topic_msgs = messages
                .get(topic)
                .ok_or_else(|| McpError::topic_not_found(topic))?;
            let start = offset.max(0) as usize;
            let end = (start + count as usize).min(topic_msgs.len());
            Ok(topic_msgs[start..end].to_vec())
        }

        async fn list_consumer_groups(&self) -> McpResult<Vec<ConsumerGroupInfo>> {
            self.check_fail("list_consumer_groups")?;
            Ok(self.consumer_groups.lock().unwrap().clone())
        }

        async fn query(&self, sql: &str) -> McpResult<QueryResult> {
            self.check_fail("query")?;
            Ok(QueryResult {
                columns: vec!["result".to_string()],
                rows: vec![vec![serde_json::json!(sql)]],
                row_count: 1,
                execution_time_ms: 1,
            })
        }

        async fn get_metrics(&self) -> McpResult<MetricsSnapshot> {
            self.check_fail("get_metrics")?;
            Ok(self.metrics.lock().unwrap().clone())
        }

        async fn create_topic(
            &self,
            name: &str,
            partitions: u32,
            retention_ms: Option<i64>,
        ) -> McpResult<()> {
            self.check_fail("create_topic")?;
            let topics = self.topics.lock().unwrap();
            if topics.iter().any(|t| t.name == name) {
                return Err(McpError::topic_already_exists(name));
            }
            drop(topics);
            self.created_topics
                .lock()
                .unwrap()
                .push((name.to_string(), partitions, retention_ms));
            self.add_topic(name, partitions as i32);
            Ok(())
        }

        async fn delete_topic(&self, name: &str) -> McpResult<()> {
            self.check_fail("delete_topic")?;
            let mut topics = self.topics.lock().unwrap();
            let before = topics.len();
            topics.retain(|t| t.name != name);
            if topics.len() == before {
                return Err(McpError::topic_not_found(name));
            }
            drop(topics);
            self.deleted_topics
                .lock()
                .unwrap()
                .push(name.to_string());
            Ok(())
        }

        async fn get_server_config(&self) -> McpResult<ServerConfigInfo> {
            self.check_fail("get_server_config")?;
            Ok(ServerConfigInfo {
                version: "0.1.0-test".to_string(),
                kafka_port: 9092,
                http_port: 9094,
                storage_mode: "in-memory".to_string(),
                features: vec!["kafka-protocol".to_string(), "mcp".to_string()],
                limits: ServerLimits {
                    max_message_size: 1_048_576,
                    max_partitions_per_topic: 1024,
                    max_topics: 10_000,
                },
            })
        }

        async fn search_messages(
            &self,
            topic: &str,
            _partition: i32,
            pattern: &str,
            max_results: usize,
        ) -> McpResult<Vec<MessageInfo>> {
            self.check_fail("search_messages")?;
            let re = regex::Regex::new(pattern)
                .map_err(|e| McpError::invalid_params(format!("Invalid regex: {}", e)))?;

            let messages = self.messages.lock().unwrap();
            let topic_msgs = messages
                .get(topic)
                .ok_or_else(|| McpError::topic_not_found(topic))?;

            Ok(topic_msgs
                .iter()
                .filter(|m| re.is_match(&m.value))
                .take(max_results)
                .cloned()
                .collect())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mcp_error_codes() {
        assert_eq!(McpErrorCode::ParseError.json_rpc_code(), -32700);
        assert_eq!(McpErrorCode::InvalidRequest.json_rpc_code(), -32600);
        assert_eq!(McpErrorCode::MethodNotFound.json_rpc_code(), -32601);
        assert_eq!(McpErrorCode::InvalidParams.json_rpc_code(), -32602);
        assert_eq!(McpErrorCode::InternalError.json_rpc_code(), -32603);
        assert_eq!(McpErrorCode::TopicNotFound.json_rpc_code(), -32002);
        assert_eq!(McpErrorCode::PartitionNotFound.json_rpc_code(), -32003);
        assert_eq!(McpErrorCode::TopicAlreadyExists.json_rpc_code(), -32004);
        assert_eq!(McpErrorCode::NotSupported.json_rpc_code(), -32005);
    }

    #[test]
    fn test_mcp_error_constructors() {
        let e = McpError::invalid_params("bad input");
        assert_eq!(e.code, McpErrorCode::InvalidParams);
        assert_eq!(e.message, "bad input");
        assert!(e.data.is_none());

        let e = McpError::topic_not_found("events");
        assert_eq!(e.code, McpErrorCode::TopicNotFound);
        assert!(e.message.contains("events"));

        let e = McpError::partition_not_found("events", 5);
        assert_eq!(e.code, McpErrorCode::PartitionNotFound);
        assert!(e.message.contains("5"));
        assert!(e.message.contains("events"));
    }

    #[test]
    fn test_mcp_error_display() {
        let e = McpError::internal("something broke");
        assert_eq!(format!("{}", e), "something broke");
    }

    #[test]
    fn test_streamline_error_conversion() {
        let se = StreamlineError::TopicNotFound("my-topic".to_string());
        let me: McpError = se.into();
        assert_eq!(me.code, McpErrorCode::TopicNotFound);
        assert!(me.message.contains("my-topic"));

        let se = StreamlineError::Validation("bad field".to_string());
        let me: McpError = se.into();
        assert_eq!(me.code, McpErrorCode::InvalidParams);

        let se = StreamlineError::TopicAlreadyExists("dup".to_string());
        let me: McpError = se.into();
        assert_eq!(me.code, McpErrorCode::TopicAlreadyExists);
    }

    #[test]
    fn test_mcp_error_to_streamline() {
        let me = McpError::internal("oops");
        let se: StreamlineError = me.into();
        match se {
            StreamlineError::Mcp(msg) => assert_eq!(msg, "oops"),
            _ => panic!("expected Mcp variant"),
        }
    }

    #[test]
    fn test_data_type_serialization() {
        let topic = TopicInfo {
            name: "test".to_string(),
            partitions: 3,
            replication_factor: 1,
            retention_ms: 604_800_000,
            total_messages: 42,
            created_at: 1700000000000,
        };
        let json = serde_json::to_value(&topic).unwrap();
        assert_eq!(json["name"], "test");
        assert_eq!(json["partitions"], 3);
        assert_eq!(json["total_messages"], 42);

        let msg = MessageInfo {
            offset: 10,
            timestamp: 1700000000000,
            key: Some("k1".to_string()),
            value: "hello".to_string(),
            headers: vec![("h1".to_string(), "v1".to_string())],
        };
        let json = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["offset"], 10);
        assert_eq!(json["key"], "k1");
    }

    #[test]
    fn test_metrics_snapshot_default() {
        let m = MetricsSnapshot::default();
        assert_eq!(m.total_topics, 0);
        assert_eq!(m.total_messages, 0);
        assert_eq!(m.messages_in_per_sec, 0.0);
    }

    #[tokio::test]
    async fn test_mock_backend_list_topics() {
        let mock = MockMcpBackend::new();
        mock.add_topic("events", 3);
        mock.add_topic("logs", 1);

        let topics = mock.list_topics().await.unwrap();
        assert_eq!(topics.len(), 2);
        assert_eq!(topics[0].name, "events");
        assert_eq!(topics[0].partitions, 3);
        assert_eq!(topics[1].name, "logs");
    }

    #[tokio::test]
    async fn test_mock_backend_describe_topic() {
        let mock = MockMcpBackend::new();
        mock.add_topic("events", 2);
        mock.add_messages(
            "events",
            vec![
                MessageInfo {
                    offset: 0,
                    timestamp: 1000,
                    key: None,
                    value: "msg1".to_string(),
                    headers: vec![],
                },
                MessageInfo {
                    offset: 1,
                    timestamp: 2000,
                    key: None,
                    value: "msg2".to_string(),
                    headers: vec![],
                },
            ],
        );

        let detail = mock.describe_topic("events").await.unwrap();
        assert_eq!(detail.name, "events");
        assert_eq!(detail.total_messages, 2);
        assert_eq!(detail.partitions.len(), 2);
        assert_eq!(detail.partitions[0].message_count, 2);
    }

    #[tokio::test]
    async fn test_mock_backend_describe_not_found() {
        let mock = MockMcpBackend::new();
        let err = mock.describe_topic("nope").await.unwrap_err();
        assert_eq!(err.code, McpErrorCode::TopicNotFound);
    }

    #[tokio::test]
    async fn test_mock_backend_produce() {
        let mock = MockMcpBackend::new();
        mock.add_topic("events", 1);

        let r = mock.produce("events", Some("k"), "v", None).await.unwrap();
        assert_eq!(r.topic, "events");
        assert_eq!(r.partition, 0);
        assert_eq!(r.offset, 0);

        let r2 = mock.produce("events", None, "v2", Some(1)).await.unwrap();
        assert_eq!(r2.offset, 1);
        assert_eq!(r2.partition, 1);

        assert_eq!(mock.produced_messages.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn test_mock_backend_produce_no_topic() {
        let mock = MockMcpBackend::new();
        let err = mock.produce("nope", None, "v", None).await.unwrap_err();
        assert_eq!(err.code, McpErrorCode::TopicNotFound);
    }

    #[tokio::test]
    async fn test_mock_backend_consume() {
        let mock = MockMcpBackend::new();
        mock.add_topic("events", 1);
        mock.add_messages(
            "events",
            (0..5)
                .map(|i| MessageInfo {
                    offset: i,
                    timestamp: 1000 + i,
                    key: None,
                    value: format!("msg{}", i),
                    headers: vec![],
                })
                .collect(),
        );

        let msgs = mock.consume("events", 0, 1, 3).await.unwrap();
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[0].offset, 1);
        assert_eq!(msgs[2].offset, 3);
    }

    #[tokio::test]
    async fn test_mock_backend_create_delete_topic() {
        let mock = MockMcpBackend::new();

        mock.create_topic("new-topic", 4, Some(3600000))
            .await
            .unwrap();
        assert_eq!(mock.topics.lock().unwrap().len(), 1);
        assert_eq!(mock.created_topics.lock().unwrap().len(), 1);

        // Duplicate should fail
        let err = mock
            .create_topic("new-topic", 1, None)
            .await
            .unwrap_err();
        assert_eq!(err.code, McpErrorCode::TopicAlreadyExists);

        mock.delete_topic("new-topic").await.unwrap();
        assert_eq!(mock.topics.lock().unwrap().len(), 0);
        assert_eq!(mock.deleted_topics.lock().unwrap().len(), 1);

        // Delete non-existent should fail
        let err = mock.delete_topic("new-topic").await.unwrap_err();
        assert_eq!(err.code, McpErrorCode::TopicNotFound);
    }

    #[tokio::test]
    async fn test_mock_backend_fail_method() {
        let mock = MockMcpBackend::new();
        mock.add_topic("events", 1);
        mock.set_fail_method(Some("list_topics"));

        let err = mock.list_topics().await.unwrap_err();
        assert_eq!(err.code, McpErrorCode::InternalError);
        assert!(err.message.contains("list_topics"));

        mock.set_fail_method(None);
        let topics = mock.list_topics().await.unwrap();
        assert_eq!(topics.len(), 1);
    }

    #[tokio::test]
    async fn test_mock_backend_search_messages() {
        let mock = MockMcpBackend::new();
        mock.add_topic("logs", 1);
        mock.add_messages(
            "logs",
            vec![
                MessageInfo {
                    offset: 0,
                    timestamp: 1000,
                    key: None,
                    value: "INFO: started".to_string(),
                    headers: vec![],
                },
                MessageInfo {
                    offset: 1,
                    timestamp: 2000,
                    key: None,
                    value: "ERROR: failed to connect".to_string(),
                    headers: vec![],
                },
                MessageInfo {
                    offset: 2,
                    timestamp: 3000,
                    key: None,
                    value: "ERROR: timeout".to_string(),
                    headers: vec![],
                },
            ],
        );

        let matches = mock.search_messages("logs", 0, "ERROR", 10).await.unwrap();
        assert_eq!(matches.len(), 2);
        assert_eq!(matches[0].offset, 1);
    }

    #[tokio::test]
    async fn test_mock_backend_search_invalid_regex() {
        let mock = MockMcpBackend::new();
        mock.add_topic("logs", 1);
        mock.add_messages("logs", vec![]);

        let err = mock
            .search_messages("logs", 0, "[invalid", 10)
            .await
            .unwrap_err();
        assert_eq!(err.code, McpErrorCode::InvalidParams);
    }

    #[tokio::test]
    async fn test_mock_backend_get_metrics() {
        let mock = MockMcpBackend::new();
        let m = mock.get_metrics().await.unwrap();
        assert_eq!(m.total_topics, 0);
    }

    #[tokio::test]
    async fn test_mock_backend_get_server_config() {
        let mock = MockMcpBackend::new();
        let cfg = mock.get_server_config().await.unwrap();
        assert_eq!(cfg.kafka_port, 9092);
        assert!(cfg.features.contains(&"mcp".to_string()));
    }

    #[tokio::test]
    async fn test_mock_backend_query() {
        let mock = MockMcpBackend::new();
        let result = mock.query("SELECT 1").await.unwrap();
        assert_eq!(result.row_count, 1);
    }
}
