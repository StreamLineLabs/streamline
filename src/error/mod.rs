//! Error types for Streamline
//!
//! This module defines the main error types used throughout Streamline and provides
//! mapping to Kafka protocol error codes for proper client compatibility.

use thiserror::Error;

mod kafka_error_code;
mod domain;
mod hints;

pub use kafka_error_code::KafkaErrorCode;
pub use domain::{StorageError, ProtocolError, ConfigError, ServerError, ClusterError};
pub use hints::{ErrorContext, ErrorHint};

/// Result type alias for Streamline operations
pub type Result<T> = std::result::Result<T, StreamlineError>;

/// Main error type for Streamline
#[derive(Error, Debug)]
pub enum StreamlineError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Storage error: {0}")]
    StorageDomain(#[from] StorageError),

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Protocol error: {0}")]
    ProtocolDomain(#[from] ProtocolError),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Configuration error: {0}")]
    ConfigDomain(#[from] ConfigError),

    #[error("Topic not found: {0}")]
    TopicNotFound(String),

    #[error("Partition not found: topic={0}, partition={1}")]
    PartitionNotFound(String, i32),

    #[error("Invalid offset: {0}")]
    InvalidOffset(i64),

    #[error("Corrupted data: {0}")]
    CorruptedData(String),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Server error: {0}")]
    Server(String),

    #[error("Server error: {0}")]
    ServerDomain(#[from] ServerError),

    #[error("Authentication failed: {0}")]
    AuthenticationFailed(String),

    #[error("Authorization failed: {0}")]
    AuthorizationFailed(String),

    #[error("Invalid credentials")]
    InvalidCredentials,

    #[error("Cluster error: {0}")]
    Cluster(String),

    #[error("Cluster error: {0}")]
    ClusterDomain(#[from] ClusterError),

    #[error("Not leader for partition: topic={0}, partition={1}")]
    NotLeader(String, i32),

    #[error("Replication error: {0}")]
    Replication(String),

    #[error("Topic already exists: {0}")]
    TopicAlreadyExists(String),

    #[error("Invalid topic name: {0}")]
    InvalidTopicName(String),

    #[error("Invalid partition count: {0}")]
    InvalidPartitionCount(String),

    #[error("Invalid client ID: {0}")]
    InvalidClientId(String),

    #[error("Message too large: size={0}, max={1}")]
    MessageTooLarge(usize, usize),

    #[error("Rate limit exceeded")]
    RateLimitExceeded,

    #[error("Connection limit exceeded")]
    ConnectionLimitExceeded,

    #[error("Request timeout")]
    RequestTimeout,

    #[error("Server is shutting down")]
    ShuttingDown,

    #[error("Analytics error: {0}")]
    Analytics(String),

    #[error("Sink error: {0}")]
    Sink(String),

    #[error("Query error: {0}")]
    Query(String),

    #[error("Parse error: {0}")]
    Parse(String),

    #[error("Timeout: {0}")]
    Timeout(String),

    #[error("WASM error: {0}")]
    Wasm(String),

    #[error("Invalid data: {0}")]
    InvalidData(String),

    #[error("AI/ML error: {0}")]
    AI(String),

    #[error("Resource exhausted: {0}")]
    ResourceExhausted(String),

    #[error("CRDT error: {0}")]
    Crdt(String),

    #[error("Network error: {0}")]
    Network(String),

    #[error("Tenant error: {0}")]
    Tenant(String),

    #[error("Validation error: {0}")]
    Validation(String),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("MCP error: {0}")]
    Mcp(String),

    #[error("CDC error: {0}")]
    Cdc(String),

    #[error("Connector error: {0}")]
    Connector(String),

    #[error("Pipeline error: {0}")]
    Pipeline(String),

    #[error("Namespace error: {0}")]
    Namespace(String),

    #[error("Replication conflict: {0}")]
    ReplicationConflict(String),

    #[error("Quota exceeded: {0}")]
    QuotaExceeded(String),

    #[error("Contract violation: {0}")]
    ContractViolation(String),

    #[error("Debugger error: {0}")]
    Debugger(String),

    #[error("Marketplace error: {0}")]
    Marketplace(String),

    #[error("Gateway error: {0}")]
    Gateway(String),

    #[error("Rebalance error: {0}")]
    Rebalance(String),

    #[error("Playground error: {0}")]
    Playground(String),

    #[error("Lineage error: {0}")]
    Lineage(String),

    #[error("FFI error: {0}")]
    Ffi(String),
}

impl StreamlineError {
    // ========== Error Context Builders ==========
    //
    // These builders provide a more ergonomic way to create errors with
    // contextual information, reducing boilerplate format!() calls.

    /// Create a storage error from a message string (routes through StorageDomain)
    pub fn storage_msg(msg: String) -> Self {
        StreamlineError::StorageDomain(StorageError::Message(msg))
    }

    /// Create a storage error with operation context
    ///
    /// # Example
    /// ```ignore
    /// StreamlineError::storage("append", "disk full")
    /// // produces: "Storage error: append: disk full"
    /// ```
    pub fn storage(operation: &str, detail: impl Into<String>) -> Self {
        StreamlineError::StorageDomain(StorageError::operation(operation, detail))
    }

    /// Create a storage error for a specific topic/partition
    pub fn storage_partition(
        topic: &str,
        partition: i32,
        operation: &str,
        detail: impl Into<String>,
    ) -> Self {
        StreamlineError::StorageDomain(StorageError::partition(topic, partition, operation, detail))
    }

    /// Create a protocol error from a message string (routes through ProtocolDomain)
    pub fn protocol_msg(msg: String) -> Self {
        StreamlineError::ProtocolDomain(ProtocolError::Message(msg))
    }

    /// Create a protocol error with context
    ///
    /// # Example
    /// ```ignore
    /// StreamlineError::protocol("parse", "invalid header magic")
    /// // produces: "Protocol error: parse: invalid header magic"
    /// ```
    pub fn protocol(operation: &str, detail: impl Into<String>) -> Self {
        StreamlineError::ProtocolDomain(ProtocolError::operation(operation, detail))
    }

    /// Create a protocol error for unexpected message types
    pub fn protocol_unexpected(expected: &str, got: &str) -> Self {
        StreamlineError::ProtocolDomain(ProtocolError::unexpected(expected, got))
    }

    /// Create a protocol error for invalid field values
    pub fn protocol_invalid_field(field: &str, reason: impl Into<String>) -> Self {
        StreamlineError::ProtocolDomain(ProtocolError::invalid_field(field, reason))
    }

    /// Create a server error with context
    pub fn server(operation: &str, detail: impl Into<String>) -> Self {
        StreamlineError::ServerDomain(ServerError::operation(operation, detail))
    }

    /// Create a server error for bind failures
    pub fn server_bind_failed(address: impl Into<String>, reason: impl Into<String>) -> Self {
        StreamlineError::ServerDomain(ServerError::bind_failed(address, reason))
    }

    /// Create a server error for connection failures
    pub fn server_connection(detail: impl Into<String>) -> Self {
        StreamlineError::ServerDomain(ServerError::connection(detail))
    }

    /// Create a server error for shutdown failures
    pub fn server_shutdown(detail: impl Into<String>) -> Self {
        StreamlineError::ServerDomain(ServerError::shutdown(detail))
    }

    /// Create a server error for task failures
    pub fn server_task_failed(task: impl Into<String>, detail: impl Into<String>) -> Self {
        StreamlineError::ServerDomain(ServerError::task_failed(task, detail))
    }

    /// Create a configuration error with context
    pub fn config(setting: &str, reason: impl Into<String>) -> Self {
        StreamlineError::ConfigDomain(ConfigError::invalid_setting(setting, reason))
    }

    /// Create a cluster error with context
    pub fn cluster(operation: &str, detail: impl Into<String>) -> Self {
        StreamlineError::ClusterDomain(ClusterError::operation(operation, detail))
    }

    /// Create a cluster error for node not found
    pub fn cluster_node_not_found(node_id: impl Into<String>) -> Self {
        StreamlineError::ClusterDomain(ClusterError::node_not_found(node_id))
    }

    /// Create a cluster error for leader election failures
    pub fn cluster_leader_election_failed(detail: impl Into<String>) -> Self {
        StreamlineError::ClusterDomain(ClusterError::leader_election_failed(detail))
    }

    /// Create a cluster error for Raft failures
    pub fn cluster_raft(detail: impl Into<String>) -> Self {
        StreamlineError::ClusterDomain(ClusterError::raft(detail))
    }

    /// Create a cluster error for bootstrap failures
    pub fn cluster_bootstrap_failed(detail: impl Into<String>) -> Self {
        StreamlineError::ClusterDomain(ClusterError::bootstrap_failed(detail))
    }

    /// Create a replication error with context
    pub fn replication(operation: &str, detail: impl Into<String>) -> Self {
        StreamlineError::Replication(format!("{}: {}", operation, detail.into()))
    }

    /// Create a replication error for a specific topic/partition
    pub fn replication_partition(
        topic: &str,
        partition: i32,
        operation: &str,
        detail: impl Into<String>,
    ) -> Self {
        StreamlineError::Replication(format!(
            "{}/{}: {}: {}",
            topic,
            partition,
            operation,
            detail.into()
        ))
    }

    /// Create an authentication error with context
    pub fn auth_failed(reason: impl Into<String>) -> Self {
        StreamlineError::AuthenticationFailed(reason.into())
    }

    /// Create an authorization error for a specific resource
    pub fn authz_failed(principal: &str, operation: &str, resource: &str) -> Self {
        StreamlineError::AuthorizationFailed(format!(
            "{} is not authorized to {} on {}",
            principal, operation, resource
        ))
    }

    /// Create a corrupted data error with context
    pub fn corrupted(location: &str, detail: impl Into<String>) -> Self {
        StreamlineError::CorruptedData(format!("{}: {}", location, detail.into()))
    }

    // ========== Kafka Error Code Conversion ==========

    /// Convert this error to the corresponding Kafka protocol error code
    pub fn kafka_error_code(&self) -> KafkaErrorCode {
        match self {
            StreamlineError::Io(_) => KafkaErrorCode::KafkaStorageError,
            StreamlineError::Storage(_) | StreamlineError::StorageDomain(_) => {
                KafkaErrorCode::KafkaStorageError
            }
            StreamlineError::Protocol(_) | StreamlineError::ProtocolDomain(_) => {
                KafkaErrorCode::InvalidRequest
            }
            StreamlineError::Config(_) | StreamlineError::ConfigDomain(_) => {
                KafkaErrorCode::InvalidConfig
            }
            StreamlineError::TopicNotFound(_) => KafkaErrorCode::UnknownTopicOrPartition,
            StreamlineError::PartitionNotFound(_, _) => KafkaErrorCode::UnknownTopicOrPartition,
            StreamlineError::InvalidOffset(_) => KafkaErrorCode::OffsetOutOfRange,
            StreamlineError::CorruptedData(_) => KafkaErrorCode::CorruptMessage,
            StreamlineError::Serialization(_) => KafkaErrorCode::InvalidRequest,
            StreamlineError::Server(_) | StreamlineError::ServerDomain(_) => {
                KafkaErrorCode::UnknownServerError
            }
            StreamlineError::AuthenticationFailed(_) => KafkaErrorCode::SaslAuthenticationFailed,
            StreamlineError::AuthorizationFailed(_) => KafkaErrorCode::TopicAuthorizationFailed,
            StreamlineError::InvalidCredentials => KafkaErrorCode::SaslAuthenticationFailed,
            StreamlineError::Cluster(_) | StreamlineError::ClusterDomain(_) => {
                KafkaErrorCode::BrokerNotAvailable
            }
            StreamlineError::NotLeader(_, _) => KafkaErrorCode::NotLeaderOrFollower,
            StreamlineError::Replication(_) => KafkaErrorCode::ReplicaNotAvailable,
            StreamlineError::TopicAlreadyExists(_) => KafkaErrorCode::TopicAlreadyExists,
            StreamlineError::InvalidTopicName(_) => KafkaErrorCode::InvalidTopicException,
            StreamlineError::InvalidPartitionCount(_) => KafkaErrorCode::InvalidPartitions,
            StreamlineError::InvalidClientId(_) => KafkaErrorCode::InvalidRequest,
            StreamlineError::MessageTooLarge(_, _) => KafkaErrorCode::MessageTooLarge,
            StreamlineError::RateLimitExceeded => KafkaErrorCode::ThrottlingQuotaExceeded,
            StreamlineError::ConnectionLimitExceeded => KafkaErrorCode::BrokerNotAvailable,
            StreamlineError::RequestTimeout => KafkaErrorCode::RequestTimedOut,
            StreamlineError::ShuttingDown => KafkaErrorCode::BrokerNotAvailable,
            StreamlineError::Analytics(_) => KafkaErrorCode::UnknownServerError,
            StreamlineError::Sink(_) => KafkaErrorCode::UnknownServerError,
            StreamlineError::Query(_) => KafkaErrorCode::UnknownServerError,
            StreamlineError::Parse(_) => KafkaErrorCode::InvalidRequest,
            StreamlineError::Timeout(_) => KafkaErrorCode::RequestTimedOut,
            StreamlineError::Wasm(_) => KafkaErrorCode::UnknownServerError,

            StreamlineError::InvalidData(_) => KafkaErrorCode::InvalidRequest,
            StreamlineError::AI(_) => KafkaErrorCode::UnknownServerError,
            StreamlineError::ResourceExhausted(_) => KafkaErrorCode::ThrottlingQuotaExceeded,
            StreamlineError::Crdt(_) => KafkaErrorCode::UnknownServerError,
            StreamlineError::Network(_) => KafkaErrorCode::NetworkException,
            StreamlineError::Tenant(_) => KafkaErrorCode::UnknownServerError,
            StreamlineError::Validation(_) => KafkaErrorCode::InvalidRequest,
            StreamlineError::Internal(_) => KafkaErrorCode::UnknownServerError,
            StreamlineError::Mcp(_) => KafkaErrorCode::UnknownServerError,
            StreamlineError::Cdc(_) => KafkaErrorCode::UnknownServerError,
            StreamlineError::Connector(_) => KafkaErrorCode::UnknownServerError,
            StreamlineError::Pipeline(_) => KafkaErrorCode::UnknownServerError,
            StreamlineError::Namespace(_) => KafkaErrorCode::UnknownServerError,
            StreamlineError::ReplicationConflict(_) => KafkaErrorCode::UnknownServerError,
            StreamlineError::QuotaExceeded(_) => KafkaErrorCode::ThrottlingQuotaExceeded,
            StreamlineError::ContractViolation(_) => KafkaErrorCode::UnknownServerError,
            StreamlineError::Debugger(_) => KafkaErrorCode::UnknownServerError,
            StreamlineError::Marketplace(_) => KafkaErrorCode::UnknownServerError,
            StreamlineError::Gateway(_) => KafkaErrorCode::NetworkException,
            StreamlineError::Rebalance(_) => KafkaErrorCode::RebalanceInProgress,
            StreamlineError::Playground(_) => KafkaErrorCode::UnknownServerError,
            StreamlineError::Lineage(_) => KafkaErrorCode::UnknownServerError,
            StreamlineError::Ffi(_) => KafkaErrorCode::UnknownServerError,
        }
    }

    /// Returns true if this error is retriable by Kafka clients
    pub fn is_retriable(&self) -> bool {
        self.kafka_error_code().is_retriable()
    }
}

impl From<&StreamlineError> for i16 {
    fn from(err: &StreamlineError) -> i16 {
        err.kafka_error_code().as_i16()
    }
}

impl From<streamline_wasm::error::WasmError> for StreamlineError {
    fn from(e: streamline_wasm::error::WasmError) -> Self {
        match e {
            streamline_wasm::error::WasmError::Configuration(msg) => StreamlineError::Config(msg),
            streamline_wasm::error::WasmError::Timeout(msg) => StreamlineError::Timeout(msg),
            streamline_wasm::error::WasmError::Wasm(msg) => StreamlineError::Wasm(msg),
            streamline_wasm::error::WasmError::Validation(msg) => StreamlineError::Validation(msg),
            streamline_wasm::error::WasmError::Internal(msg) => StreamlineError::Internal(msg),
            streamline_wasm::error::WasmError::ResourceExhausted(msg) => {
                StreamlineError::ResourceExhausted(msg)
            }
            streamline_wasm::error::WasmError::Storage(msg) => StreamlineError::Storage(msg),
        }
    }
}

#[cfg(feature = "analytics")]
impl From<streamline_analytics::error::AnalyticsError> for StreamlineError {
    fn from(e: streamline_analytics::error::AnalyticsError) -> Self {
        match e {
            streamline_analytics::error::AnalyticsError::Analytics(msg) => {
                StreamlineError::Analytics(msg)
            }
            streamline_analytics::error::AnalyticsError::Config(msg) => {
                StreamlineError::Config(msg)
            }
            streamline_analytics::error::AnalyticsError::InvalidSql(msg) => {
                StreamlineError::Query(msg)
            }
            streamline_analytics::error::AnalyticsError::TopicNotFound(topic) => {
                StreamlineError::TopicNotFound(topic)
            }
            streamline_analytics::error::AnalyticsError::QueryTimeout { timeout_ms } => {
                StreamlineError::Timeout(format!("Query timed out after {}ms", timeout_ms))
            }
            streamline_analytics::error::AnalyticsError::DuckDb(msg) => {
                StreamlineError::Analytics(msg)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    #[test]
    fn test_io_error_display() {
        let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
        let err: StreamlineError = io_err.into();
        assert!(err.to_string().contains("IO error"));
    }

    #[test]
    fn test_storage_error_display() {
        let err = StreamlineError::Storage("disk full".to_string());
        assert_eq!(err.to_string(), "Storage error: disk full");
    }

    #[test]
    fn test_protocol_error_display() {
        let err = StreamlineError::Protocol("invalid message".to_string());
        assert_eq!(err.to_string(), "Protocol error: invalid message");
    }

    #[test]
    fn test_config_error_display() {
        let err = StreamlineError::Config("invalid address".to_string());
        assert_eq!(err.to_string(), "Configuration error: invalid address");
    }

    #[test]
    fn test_topic_not_found_display() {
        let err = StreamlineError::TopicNotFound("my-topic".to_string());
        assert_eq!(err.to_string(), "Topic not found: my-topic");
    }

    #[test]
    fn test_partition_not_found_display() {
        let err = StreamlineError::PartitionNotFound("my-topic".to_string(), 5);
        assert_eq!(
            err.to_string(),
            "Partition not found: topic=my-topic, partition=5"
        );
    }

    #[test]
    fn test_invalid_offset_display() {
        let err = StreamlineError::InvalidOffset(-100);
        assert_eq!(err.to_string(), "Invalid offset: -100");
    }

    #[test]
    fn test_corrupted_data_display() {
        let err = StreamlineError::CorruptedData("checksum mismatch".to_string());
        assert_eq!(err.to_string(), "Corrupted data: checksum mismatch");
    }

    #[test]
    fn test_server_error_display() {
        let err = StreamlineError::Server("connection refused".to_string());
        assert_eq!(err.to_string(), "Server error: connection refused");
    }

    #[test]
    fn test_error_is_debug() {
        let err = StreamlineError::Storage("test".to_string());
        let debug_str = format!("{:?}", err);
        assert!(debug_str.contains("Storage"));
    }

    #[test]
    fn test_kafka_error_code_mapping() {
        // Test all error types map to correct Kafka codes
        let cases = vec![
            (
                StreamlineError::TopicNotFound("t".into()),
                KafkaErrorCode::UnknownTopicOrPartition,
            ),
            (
                StreamlineError::PartitionNotFound("t".into(), 0),
                KafkaErrorCode::UnknownTopicOrPartition,
            ),
            (
                StreamlineError::InvalidOffset(0),
                KafkaErrorCode::OffsetOutOfRange,
            ),
            (
                StreamlineError::CorruptedData("c".into()),
                KafkaErrorCode::CorruptMessage,
            ),
            (
                StreamlineError::NotLeader("t".into(), 0),
                KafkaErrorCode::NotLeaderOrFollower,
            ),
            (
                StreamlineError::AuthenticationFailed("a".into()),
                KafkaErrorCode::SaslAuthenticationFailed,
            ),
            (
                StreamlineError::AuthorizationFailed("a".into()),
                KafkaErrorCode::TopicAuthorizationFailed,
            ),
            (
                StreamlineError::TopicAlreadyExists("t".into()),
                KafkaErrorCode::TopicAlreadyExists,
            ),
            (
                StreamlineError::MessageTooLarge(100, 50),
                KafkaErrorCode::MessageTooLarge,
            ),
            (
                StreamlineError::RateLimitExceeded,
                KafkaErrorCode::ThrottlingQuotaExceeded,
            ),
            (
                StreamlineError::RequestTimeout,
                KafkaErrorCode::RequestTimedOut,
            ),
        ];

        for (err, expected_code) in cases {
            assert_eq!(
                err.kafka_error_code(),
                expected_code,
                "Error {:?} should map to {:?}",
                err,
                expected_code
            );
        }
    }

    #[test]
    fn test_kafka_error_code_as_i16() {
        assert_eq!(KafkaErrorCode::None.as_i16(), 0);
        assert_eq!(KafkaErrorCode::UnknownServerError.as_i16(), -1);
        assert_eq!(KafkaErrorCode::UnknownTopicOrPartition.as_i16(), 3);
        assert_eq!(KafkaErrorCode::NotLeaderOrFollower.as_i16(), 6);
        assert_eq!(KafkaErrorCode::TopicAlreadyExists.as_i16(), 36);
        assert_eq!(KafkaErrorCode::SaslAuthenticationFailed.as_i16(), 58);
    }

    #[test]
    fn test_is_retriable() {
        // Retriable errors
        assert!(KafkaErrorCode::NotLeaderOrFollower.is_retriable());
        assert!(KafkaErrorCode::RequestTimedOut.is_retriable());
        assert!(KafkaErrorCode::ThrottlingQuotaExceeded.is_retriable());
        assert!(KafkaErrorCode::LeaderNotAvailable.is_retriable());
        assert!(KafkaErrorCode::ReplicaNotAvailable.is_retriable());

        // Non-retriable errors
        assert!(!KafkaErrorCode::TopicAlreadyExists.is_retriable());
        assert!(!KafkaErrorCode::InvalidRequest.is_retriable());
        assert!(!KafkaErrorCode::SaslAuthenticationFailed.is_retriable());
        assert!(!KafkaErrorCode::TopicAuthorizationFailed.is_retriable());
    }

    #[test]
    fn test_from_streamline_error_to_i16() {
        let err = StreamlineError::TopicNotFound("test".into());
        let code: i16 = (&err).into();
        assert_eq!(code, 3); // UnknownTopicOrPartition

        let err = StreamlineError::InvalidOffset(0);
        let code: i16 = (&err).into();
        assert_eq!(code, 1); // OffsetOutOfRange
    }

    #[test]
    fn test_streamline_error_is_retriable() {
        // Retriable errors
        assert!(StreamlineError::NotLeader("t".into(), 0).is_retriable());
        assert!(StreamlineError::RequestTimeout.is_retriable());
        assert!(StreamlineError::RateLimitExceeded.is_retriable());

        // Non-retriable errors
        assert!(!StreamlineError::TopicAlreadyExists("t".into()).is_retriable());
        assert!(!StreamlineError::AuthenticationFailed("a".into()).is_retriable());
        assert!(!StreamlineError::AuthorizationFailed("a".into()).is_retriable());
    }

    #[test]
    fn test_new_error_types_display() {
        let err = StreamlineError::TopicAlreadyExists("my-topic".to_string());
        assert_eq!(err.to_string(), "Topic already exists: my-topic");

        let err = StreamlineError::MessageTooLarge(1000, 500);
        assert_eq!(err.to_string(), "Message too large: size=1000, max=500");

        let err = StreamlineError::RateLimitExceeded;
        assert_eq!(err.to_string(), "Rate limit exceeded");

        let err = StreamlineError::ConnectionLimitExceeded;
        assert_eq!(err.to_string(), "Connection limit exceeded");

        let err = StreamlineError::RequestTimeout;
        assert_eq!(err.to_string(), "Request timeout");
    }

    // ========== Error Context Builder Tests ==========

    #[test]
    fn test_storage_builder() {
        let err = StreamlineError::storage("append", "disk full");
        assert_eq!(err.to_string(), "Storage error: append: disk full");
    }

    #[test]
    fn test_storage_partition_builder() {
        let err = StreamlineError::storage_partition("my-topic", 3, "write", "segment closed");
        assert_eq!(
            err.to_string(),
            "Storage error: my-topic/3: write: segment closed"
        );
    }

    #[test]
    fn test_protocol_builder() {
        let err = StreamlineError::protocol("parse", "invalid header magic");
        assert_eq!(
            err.to_string(),
            "Protocol error: parse: invalid header magic"
        );
    }

    #[test]
    fn test_protocol_unexpected_builder() {
        let err = StreamlineError::protocol_unexpected("ProduceRequest", "FetchRequest");
        assert_eq!(
            err.to_string(),
            "Protocol error: expected ProduceRequest, got FetchRequest"
        );
    }

    #[test]
    fn test_protocol_invalid_field_builder() {
        let err = StreamlineError::protocol_invalid_field("partition_id", "must be non-negative");
        assert_eq!(
            err.to_string(),
            "Protocol error: invalid partition_id: must be non-negative"
        );
    }

    #[test]
    fn test_server_builder() {
        let err = StreamlineError::server("bind", "address already in use");
        assert_eq!(
            err.to_string(),
            "Server error: bind: address already in use"
        );
    }

    #[test]
    fn test_config_builder() {
        let err = StreamlineError::config("listen_addr", "invalid port number");
        assert_eq!(
            err.to_string(),
            "Configuration error: listen_addr: invalid port number"
        );
    }

    #[test]
    fn test_cluster_builder() {
        let err = StreamlineError::cluster("join", "no seed nodes available");
        assert_eq!(
            err.to_string(),
            "Cluster error: join: no seed nodes available"
        );
    }

    #[test]
    fn test_replication_builder() {
        let err = StreamlineError::replication("sync", "follower timeout");
        assert_eq!(err.to_string(), "Replication error: sync: follower timeout");
    }

    #[test]
    fn test_replication_partition_builder() {
        let err =
            StreamlineError::replication_partition("events", 2, "fetch", "leader not available");
        assert_eq!(
            err.to_string(),
            "Replication error: events/2: fetch: leader not available"
        );
    }

    #[test]
    fn test_auth_failed_builder() {
        let err = StreamlineError::auth_failed("invalid username or password");
        assert_eq!(
            err.to_string(),
            "Authentication failed: invalid username or password"
        );
    }

    #[test]
    fn test_authz_failed_builder() {
        let err = StreamlineError::authz_failed("User:alice", "write", "topic:orders");
        assert_eq!(
            err.to_string(),
            "Authorization failed: User:alice is not authorized to write on topic:orders"
        );
    }

    #[test]
    fn test_corrupted_builder() {
        let err = StreamlineError::corrupted("segment-0.log", "CRC mismatch at offset 1024");
        assert_eq!(
            err.to_string(),
            "Corrupted data: segment-0.log: CRC mismatch at offset 1024"
        );
    }

    // ========== ErrorContext Tests ==========

    #[test]
    fn test_error_context_builder() {
        let ctx = ErrorContext::new()
            .with_topics(vec!["orders".into(), "events".into()])
            .with_partition_count(6)
            .with_groups(vec!["my-group".into()])
            .with_docs_url("https://docs.example.com");

        assert_eq!(
            ctx.available_topics,
            Some(vec!["orders".into(), "events".into()])
        );
        assert_eq!(ctx.partition_count, Some(6));
        assert_eq!(ctx.available_groups, Some(vec!["my-group".into()]));
        assert_eq!(ctx.docs_base_url, Some("https://docs.example.com".into()));
    }

    #[test]
    fn test_error_context_default() {
        let ctx = ErrorContext::default();
        assert!(ctx.available_topics.is_none());
        assert!(ctx.partition_count.is_none());
        assert!(ctx.available_groups.is_none());
    }

    // ========== ErrorHint Tests ==========

    #[test]
    fn test_hint_topic_not_found() {
        let err = StreamlineError::TopicNotFound("my-topic".into());
        let hint = err.hint().unwrap();
        assert!(hint.contains("streamline-cli topics list"));
        assert!(hint.contains("streamline-cli topics create my-topic"));
    }

    #[test]
    fn test_hint_partition_not_found() {
        let err = StreamlineError::PartitionNotFound("orders".into(), 5);
        let hint = err.hint().unwrap();
        assert!(hint.contains("fewer than 6 partitions"));
        assert!(hint.contains("streamline-cli topics describe orders"));
    }

    #[test]
    fn test_hint_topic_already_exists() {
        let err = StreamlineError::TopicAlreadyExists("events".into());
        let hint = err.hint().unwrap();
        assert!(hint.contains("already exists"));
        assert!(hint.contains("streamline-cli topics delete events"));
    }

    #[test]
    fn test_hint_message_too_large() {
        let err = StreamlineError::MessageTooLarge(2_000_000, 1_000_000);
        let hint = err.hint().unwrap();
        assert!(hint.contains("2000000"));
        assert!(hint.contains("1000000"));
        assert!(hint.contains("max.message.bytes"));
    }

    #[test]
    fn test_hint_with_context_topic_not_found() {
        let err = StreamlineError::TopicNotFound("events".into());
        let ctx =
            ErrorContext::new().with_topics(vec!["orders".into(), "users".into(), "logs".into()]);

        let hint = err.hint_with_context(&ctx).unwrap();
        assert!(hint.contains("Topic 'events' not found"));
        assert!(hint.contains("orders"));
        assert!(hint.contains("users"));
        assert!(hint.contains("logs"));
    }

    #[test]
    fn test_hint_with_context_topic_not_found_empty() {
        let err = StreamlineError::TopicNotFound("events".into());
        let ctx = ErrorContext::new().with_topics(vec![]);

        let hint = err.hint_with_context(&ctx).unwrap();
        assert!(hint.contains("No topics exist yet"));
    }

    #[test]
    fn test_hint_with_context_partition_not_found() {
        let err = StreamlineError::PartitionNotFound("orders".into(), 10);
        let ctx = ErrorContext::new().with_partition_count(6);

        let hint = err.hint_with_context(&ctx).unwrap();
        assert!(hint.contains("Partition 10 does not exist"));
        assert!(hint.contains("6 partitions"));
        assert!(hint.contains("Valid range: 0-5"));
    }

    #[test]
    fn test_suggest_fix_topic_not_found() {
        let err = StreamlineError::TopicNotFound("my-events".into());
        let fix = err.suggest_fix().unwrap();
        assert!(fix.contains("streamline-cli topics create my-events"));
    }

    #[test]
    fn test_suggest_fix_with_context_similar_topics() {
        let err = StreamlineError::TopicNotFound("event".into());
        let ctx = ErrorContext::new().with_topics(vec!["events".into(), "orders".into()]);

        let fix = err.suggest_fix_with_context(&ctx).unwrap();
        assert!(fix.contains("Did you mean"));
        assert!(fix.contains("events"));
    }

    #[test]
    fn test_docs_url() {
        let err = StreamlineError::TopicNotFound("test".into());
        let url = err.docs_url().unwrap();
        assert!(url.contains("streamline.dev/docs"));
        assert!(url.contains("topics"));

        let err = StreamlineError::AuthenticationFailed("test".into());
        let url = err.docs_url().unwrap();
        assert!(url.contains("authentication"));

        let err = StreamlineError::Cluster("test".into());
        let url = err.docs_url().unwrap();
        assert!(url.contains("clustering"));
    }

    #[test]
    fn test_with_hint() {
        let err = StreamlineError::TopicNotFound("orders".into());
        let output = err.with_hint();
        assert!(output.contains("Topic not found: orders"));
        assert!(output.contains("hint:"));
    }

    #[test]
    fn test_format_with_context() {
        let err = StreamlineError::TopicNotFound("events".into());
        let ctx = ErrorContext::new().with_topics(vec!["orders".into(), "users".into()]);

        let output = err.format_with_context(&ctx);
        assert!(output.contains("Error: Topic not found: events"));
        assert!(output.contains("Hint:"));
        assert!(output.contains("orders"));
        assert!(output.contains("Fix:"));
        assert!(output.contains("Docs:"));
    }

    // ========== Levenshtein Distance Tests ==========

    #[test]
    fn test_levenshtein_distance_identical() {
        assert_eq!(levenshtein_distance("hello", "hello"), 0);
    }

    #[test]
    fn test_levenshtein_distance_empty() {
        assert_eq!(levenshtein_distance("", "hello"), 5);
        assert_eq!(levenshtein_distance("hello", ""), 5);
        assert_eq!(levenshtein_distance("", ""), 0);
    }

    #[test]
    fn test_levenshtein_distance_single_char() {
        assert_eq!(levenshtein_distance("cat", "hat"), 1);
        assert_eq!(levenshtein_distance("cat", "cats"), 1);
        assert_eq!(levenshtein_distance("cats", "cat"), 1);
    }

    #[test]
    fn test_levenshtein_distance_similar() {
        assert_eq!(levenshtein_distance("events", "event"), 1);
        assert_eq!(levenshtein_distance("orders", "order"), 1);
        assert!(levenshtein_distance("my-topic", "mytopic") <= 2);
    }

    // ========== ServerError Domain Tests ==========

    #[test]
    fn test_server_error_bind_failed() {
        let err = StreamlineError::server_bind_failed("0.0.0.0:9092", "address already in use");
        assert_eq!(
            err.to_string(),
            "Server error: bind failed on 0.0.0.0:9092: address already in use"
        );
    }

    #[test]
    fn test_server_error_connection() {
        let err = StreamlineError::server_connection("TLS handshake failed");
        assert_eq!(
            err.to_string(),
            "Server error: connection error: TLS handshake failed"
        );
    }

    #[test]
    fn test_server_error_shutdown() {
        let err = StreamlineError::server_shutdown("pending writes not flushed");
        assert_eq!(
            err.to_string(),
            "Server error: shutdown: pending writes not flushed"
        );
    }

    #[test]
    fn test_server_error_task_failed() {
        let err = StreamlineError::server_task_failed("writer", "task panicked");
        assert_eq!(err.to_string(), "Server error: writer: task panicked");
    }

    #[test]
    fn test_server_domain_from() {
        let err: StreamlineError = ServerError::connection("reset").into();
        assert_eq!(err.to_string(), "Server error: connection error: reset");
        assert_eq!(err.kafka_error_code(), KafkaErrorCode::UnknownServerError);
    }

    // ========== ClusterError Domain Tests ==========

    #[test]
    fn test_cluster_error_node_not_found() {
        let err = StreamlineError::cluster_node_not_found("node-3");
        assert_eq!(err.to_string(), "Cluster error: node not found: node-3");
    }

    #[test]
    fn test_cluster_error_leader_election_failed() {
        let err = StreamlineError::cluster_leader_election_failed("no quorum");
        assert_eq!(
            err.to_string(),
            "Cluster error: leader election failed: no quorum"
        );
    }

    #[test]
    fn test_cluster_error_raft() {
        let err = StreamlineError::cluster_raft("log compaction failed");
        assert_eq!(
            err.to_string(),
            "Cluster error: raft error: log compaction failed"
        );
    }

    #[test]
    fn test_cluster_error_bootstrap_failed() {
        let err = StreamlineError::cluster_bootstrap_failed("no seed nodes reachable");
        assert_eq!(
            err.to_string(),
            "Cluster error: bootstrap failed: no seed nodes reachable"
        );
    }

    #[test]
    fn test_cluster_domain_from() {
        let err: StreamlineError = ClusterError::not_ready("joining").into();
        assert_eq!(err.to_string(), "Cluster error: not ready: joining");
        assert_eq!(err.kafka_error_code(), KafkaErrorCode::BrokerNotAvailable);
    }

    #[test]
    fn test_cluster_domain_kafka_error_code() {
        let err = StreamlineError::ClusterDomain(ClusterError::raft("test"));
        assert_eq!(err.kafka_error_code(), KafkaErrorCode::BrokerNotAvailable);
    }

    #[test]
    fn test_server_domain_kafka_error_code() {
        let err = StreamlineError::ServerDomain(ServerError::bind_failed("addr", "in use"));
        assert_eq!(err.kafka_error_code(), KafkaErrorCode::UnknownServerError);
    }

    #[test]
    fn test_improved_kafka_mappings() {
        let err = StreamlineError::Rebalance("test".into());
        assert_eq!(err.kafka_error_code(), KafkaErrorCode::RebalanceInProgress);

        let err = StreamlineError::Gateway("test".into());
        assert_eq!(err.kafka_error_code(), KafkaErrorCode::NetworkException);
    }
}
