//! Kafka protocol handler for Streamline
//!
//! This module implements the Kafka wire protocol to provide
//! compatibility with existing Kafka clients.
//!
//! The protocol implementation is organized into submodules:
//! - `handlers` - Contains error codes and handler implementations
//! - `constants` - Protocol constants and version tables
//! - `auth_stubs` - Stub types for lite builds without auth feature

mod constants;
#[cfg(test)]
mod tests;
mod authorization;
mod connection;
mod control_records;

// Re-export constants used within this module
use constants::ProcessedResponse;
// Re-export SUPPORTED_API_VERSIONS for use by handlers/registry.rs tests
#[allow(unused_imports)]
pub(crate) use constants::SUPPORTED_API_VERSIONS;
pub use control_records::{create_control_record_batch, CONTROL_TYPE_ABORT, CONTROL_TYPE_COMMIT};
#[allow(unused_imports)]
pub(crate) use control_records::{ATTR_CONTROL_BIT, ATTR_TRANSACTIONAL_BIT, RECORD_BATCH_MAGIC_V2};

// Re-exports for test access
#[cfg(test)]
pub(crate) use kafka_protocol::messages::{
    ApiVersionsRequest, ApiVersionsResponse, CreateTopicsRequest, DeleteTopicsRequest,
    FetchResponse, FindCoordinatorResponse, GroupId, HeartbeatResponse, InitProducerIdResponse,
    JoinGroupResponse, ListOffsetsRequest, MetadataRequest, SaslAuthenticateResponse, TopicName,
    ProducerId as KafkaProducerId,
};
#[cfg(test)]
pub(crate) use super::handlers::error_codes::{
    NONE, UNKNOWN_SERVER_ERROR, OFFSET_OUT_OF_RANGE, CORRUPT_MESSAGE,
    UNKNOWN_TOPIC_OR_PARTITION, INVALID_FETCH_SIZE, LEADER_NOT_AVAILABLE,
    NOT_LEADER_OR_FOLLOWER, REQUEST_TIMED_OUT, BROKER_NOT_AVAILABLE,
    REPLICA_NOT_AVAILABLE, MESSAGE_TOO_LARGE, STALE_CONTROLLER_EPOCH,
    OFFSET_METADATA_TOO_LARGE, NETWORK_EXCEPTION, COORDINATOR_LOAD_IN_PROGRESS,
    COORDINATOR_NOT_AVAILABLE, NOT_COORDINATOR, INVALID_TOPIC_EXCEPTION,
    RECORD_LIST_TOO_LARGE, NOT_ENOUGH_REPLICAS, NOT_ENOUGH_REPLICAS_AFTER_APPEND,
    INVALID_REQUIRED_ACKS, ILLEGAL_GENERATION, INCONSISTENT_GROUP_PROTOCOL,
    INVALID_GROUP_ID, UNKNOWN_MEMBER_ID, INVALID_SESSION_TIMEOUT,
    REBALANCE_IN_PROGRESS, INVALID_COMMIT_OFFSET_SIZE, TOPIC_AUTHORIZATION_FAILED,
    GROUP_AUTHORIZATION_FAILED, CLUSTER_AUTHORIZATION_FAILED, INVALID_TIMESTAMP,
    UNSUPPORTED_SASL_MECHANISM, ILLEGAL_SASL_STATE, UNSUPPORTED_VERSION,
    TOPIC_ALREADY_EXISTS, INVALID_PARTITIONS, INVALID_REPLICATION_FACTOR,
    INVALID_REPLICA_ASSIGNMENT, INVALID_CONFIG, NOT_CONTROLLER, INVALID_REQUEST,
    UNSUPPORTED_FOR_MESSAGE_FORMAT, POLICY_VIOLATION, OUT_OF_ORDER_SEQUENCE_NUMBER,
    DUPLICATE_SEQUENCE_NUMBER, INVALID_PRODUCER_EPOCH, INVALID_TXN_STATE,
    INVALID_PRODUCER_ID_MAPPING, INVALID_TRANSACTION_TIMEOUT, CONCURRENT_TRANSACTIONS,
    TRANSACTION_COORDINATOR_FENCED, TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
    SECURITY_DISABLED, OPERATION_NOT_ATTEMPTED, KAFKA_STORAGE_ERROR, LOG_DIR_NOT_FOUND,
    SASL_AUTHENTICATION_FAILED, UNKNOWN_PRODUCER_ID, REASSIGNMENT_IN_PROGRESS,
    DELEGATION_TOKEN_AUTH_DISABLED, DELEGATION_TOKEN_NOT_FOUND,
    DELEGATION_TOKEN_OWNER_MISMATCH, DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
    DELEGATION_TOKEN_AUTHORIZATION_FAILED, DELEGATION_TOKEN_EXPIRED,
    INVALID_PRINCIPAL_TYPE, NON_EMPTY_GROUP, GROUP_ID_NOT_FOUND,
    FETCH_SESSION_ID_NOT_FOUND, INVALID_FETCH_SESSION_EPOCH, LISTENER_NOT_FOUND,
    TOPIC_DELETION_DISABLED, FENCED_LEADER_EPOCH, UNKNOWN_LEADER_EPOCH,
    UNSUPPORTED_COMPRESSION_TYPE, STALE_BROKER_EPOCH, OFFSET_NOT_AVAILABLE,
    MEMBER_ID_REQUIRED, PREFERRED_LEADER_NOT_AVAILABLE, GROUP_MAX_SIZE_REACHED,
    FENCED_INSTANCE_ID, THROTTLING_QUOTA_EXCEEDED, PRODUCER_FENCED,
    TRANSACTIONAL_ID_NOT_FOUND,
};
#[cfg(test)]
pub(crate) use crate::storage::storage_mode::StorageMode;
#[cfg(test)]
pub(crate) use crate::storage::topic::CleanupPolicy;

// Test-only constants for protocol attribute bit masks
#[cfg(test)]
pub(crate) const ATTR_COMPRESSION_MASK: i16 = 0x07;
#[cfg(test)]
pub(crate) const ATTR_TIMESTAMP_TYPE_BIT: i16 = 0x08;

// Test-only: API header version table
// Format: (api_key, max_version, req_header_v2_start, resp_header_v1_start)
#[cfg(test)]
#[rustfmt::skip]
pub(crate) const API_HEADER_VERSIONS: &[(i16, i16, Option<i16>, Option<i16>)] = &[
    (0, 9, Some(9), Some(9)),       // Produce
    (1, 15, Some(12), Some(12)),    // Fetch
    (2, 7, Some(6), Some(6)),       // ListOffsets
    (3, 12, Some(9), Some(9)),      // Metadata
    (7, 3, Some(3), Some(3)),       // ControlledShutdown
    (18, 3, Some(3), None),         // ApiVersions (response always v0)
    (19, 7, Some(5), Some(5)),      // CreateTopics
    (20, 6, Some(4), Some(4)),      // DeleteTopics
    (37, 3, Some(2), Some(2)),      // CreatePartitions
    (32, 4, Some(4), Some(4)),      // DescribeConfigs
    (33, 2, Some(2), Some(2)),      // AlterConfigs
    (44, 1, Some(0), Some(0)),      // IncrementalAlterConfigs
    (8, 8, Some(8), Some(8)),       // OffsetCommit
    (9, 8, Some(6), Some(6)),       // OffsetFetch
    (10, 4, Some(3), Some(3)),      // FindCoordinator
    (11, 9, Some(6), Some(6)),      // JoinGroup
    (12, 4, Some(4), Some(4)),      // Heartbeat
    (13, 5, Some(4), Some(4)),      // LeaveGroup
    (14, 5, Some(4), Some(4)),      // SyncGroup
    (15, 5, Some(5), Some(5)),      // DescribeGroups
    (16, 4, Some(3), Some(3)),      // ListGroups
    (42, 2, Some(2), Some(2)),      // DeleteGroups
    (17, 2, None, None),            // SaslHandshake
    (36, 2, Some(2), Some(2)),      // SaslAuthenticate
    (29, 3, Some(2), Some(2)),      // DescribeAcls
    (30, 3, Some(2), Some(2)),      // CreateAcls
    (31, 3, Some(2), Some(2)),      // DeleteAcls
    (22, 5, Some(2), Some(2)),      // InitProducerId
    (24, 4, Some(3), Some(3)),      // AddPartitionsToTxn
    (25, 3, Some(3), Some(3)),      // AddOffsetsToTxn
    (26, 3, Some(3), Some(3)),      // EndTxn
    (28, 3, Some(3), Some(3)),      // TxnOffsetCommit
    (27, 1, None, None),            // WriteTxnMarkers
    (60, 1, Some(0), Some(0)),      // DescribeCluster
    (23, 4, Some(4), Some(4)),      // OffsetForLeaderEpoch
    (21, 2, Some(2), Some(2)),      // DeleteRecords
    (35, 4, None, None),            // DescribeLogDirs
    (34, 2, None, None),            // AlterReplicaLogDirs
    (46, 0, None, None),            // ListPartitionReassignments
    (45, 0, None, None),            // AlterPartitionReassignments
    (43, 2, None, None),            // ElectLeaders
    (47, 0, None, None),            // OffsetDelete
    (61, 0, None, None),            // DescribeProducers
    (65, 0, None, None),            // DescribeTransactions
    (66, 0, None, None),            // ListTransactions
    (57, 1, None, None),            // UpdateFeatures
    (64, 0, None, None),            // UnregisterBroker
    (68, 0, None, None),            // ConsumerGroupHeartbeat
    (69, 1, None, None),            // ConsumerGroupDescribe
    (38, 3, None, None),            // CreateDelegationToken
    (39, 2, None, None),            // RenewDelegationToken
    (40, 2, None, None),            // ExpireDelegationToken
    (41, 3, None, None),            // DescribeDelegationToken
    (48, 1, None, None),            // DescribeClientQuotas
    (49, 1, None, None),            // AlterClientQuotas
    (50, 0, None, None),            // DescribeUserScramCredentials
    (51, 0, None, None),            // AlterUserScramCredentials
    (55, 1, None, None),            // DescribeQuorum
];

use super::handlers::DispatchResult;

#[cfg(feature = "auth")]
pub(crate) use crate::auth::{
    Acl, AclFilter, AuthSession, Authorizer, AuthorizerConfig, DelegationTokenManager, Operation,
    PatternType, Permission, ResourceType, SaslMechanism, SaslPlainAuthenticator,
    ScramAuthenticator, SessionManager, UserStore,
};
#[cfg(feature = "clustering")]
use crate::cluster::ClusterManager;
#[cfg(feature = "auth")]
use crate::config::AclConfig;
#[cfg(feature = "auth")]
use crate::config::AuthConfig;

// Stub types when auth is disabled - provides no-op implementations
#[cfg(not(feature = "auth"))]
#[allow(dead_code)]
mod auth_stubs;
mod core_api;
mod data_plane;
mod topic_mgmt;
mod consumer_groups;
mod auth_handlers;
mod transaction_handlers;
mod admin_handlers;
mod kip848_handlers;
mod security_handlers;

use crate::consumer::kip848::ReconciliationEngine;
use crate::consumer::GroupCoordinator;
use crate::error::{Result, StreamlineError};
use crate::metrics;
#[cfg(feature = "clustering")]
use crate::replication::ReplicationManager;
use crate::runtime::ShardedRuntime;
use crate::server::{QuotaManager, ResourceLimiter};
use crate::storage::{BufferPool, ProducerStateManager, TopicManager, ZeroCopyConfig};
use crate::transaction::TransactionCoordinator;
#[cfg(not(feature = "auth"))]
pub(crate) use auth_stubs::{Operation, ResourceType, SaslMechanism, SessionManager, UserStore};
#[cfg(test)]
use bytes::Buf;
use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::{
    ApiKey, RequestHeader, ResponseHeader,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, warn};
#[cfg(feature = "auth")]
use tracing::info;

use super::pipeline::PipelineConfig;
use super::response_cache::ResponseCache;

/// Kafka protocol handler
pub struct KafkaHandler {
    /// Topic manager for storage operations
    topic_manager: Arc<TopicManager>,

    /// Group coordinator for consumer groups
    group_coordinator: Arc<GroupCoordinator>,

    /// Producer state manager for idempotent producers
    producer_state_manager: Arc<ProducerStateManager>,

    /// Transaction coordinator for transaction management
    transaction_coordinator: Option<Arc<TransactionCoordinator>>,

    /// Node ID (cluster ID for metadata responses)
    node_id: i32,

    /// Cluster ID
    cluster_id: String,

    /// Advertised address for clients to connect (used in single-node mode)
    /// In cluster mode, this is overridden by ClusterConfig.advertised_addr
    advertised_addr: std::net::SocketAddr,

    /// Authentication configuration (requires auth feature)
    #[cfg(feature = "auth")]
    auth_config: Arc<AuthConfig>,

    /// User store for authentication (requires auth feature)
    #[cfg(feature = "auth")]
    user_store: Arc<Option<UserStore>>,

    /// Authorizer for ACL checks (requires auth feature)
    #[cfg(feature = "auth")]
    authorizer: Arc<Authorizer>,

    /// Optional cluster manager (None = single-node mode, requires clustering feature)
    #[cfg(feature = "clustering")]
    cluster_manager: Option<Arc<ClusterManager>>,

    /// Optional replication manager for multi-node replication (requires clustering feature)
    #[cfg(feature = "clustering")]
    replication_manager: Option<Arc<ReplicationManager>>,

    /// Optional quota manager for client quotas and throttling
    quota_manager: Option<Arc<QuotaManager>>,

    /// Optional delegation token manager (requires auth feature)
    #[cfg(feature = "auth")]
    delegation_token_manager: Option<Arc<DelegationTokenManager>>,

    /// Pipeline configuration for connection pipelining
    pipeline_config: PipelineConfig,

    /// Maximum message/request size in bytes
    max_message_bytes: u64,

    /// Enable automatic topic creation on produce requests
    auto_create_topics: bool,

    /// Buffer pool for reusable byte buffers
    buffer_pool: Arc<BufferPool>,

    /// Zero-copy configuration
    zerocopy_config: ZeroCopyConfig,

    /// Response cache for sendfile optimization (optional)
    response_cache: Option<Arc<ResponseCache>>,

    /// Optional resource limiter for request size validation
    resource_limiter: Option<Arc<ResourceLimiter>>,

    /// KIP-848 reconciliation engine for new consumer protocol
    kip848_engine: Arc<ReconciliationEngine>,

    /// Optional sharded runtime for thread-per-core execution
    /// When set, partition work is routed to the shard owning the partition
    sharded_runtime: Option<Arc<ShardedRuntime>>,
}

/// A cloneable handle to the Kafka handler for use in spawned tasks
///
/// This handle contains all the shared state needed for processing requests.
/// It's created by `KafkaHandler::clone_for_task()` and used in the pipelined
/// request processing tasks. Uses Deref to delegate method calls to the inner
/// KafkaHandler.
pub struct KafkaHandlerHandle {
    inner: KafkaHandler,
}

impl std::ops::Deref for KafkaHandlerHandle {
    type Target = KafkaHandler;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// Default advertised address for single-node mode (localhost)
const DEFAULT_ADVERTISED_ADDR: std::net::SocketAddr = std::net::SocketAddr::new(
    std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
    9092,
);

/// Default maximum message/request size (1 MB - matches config::DEFAULT_MAX_MESSAGE_BYTES)
/// This is a conservative default suitable for most use cases.
const DEFAULT_MAX_MESSAGE_BYTES: u64 = 1024 * 1024;

/// Hard cap on maximum message size to prevent DoS via memory exhaustion.
/// Even if configured higher, allocations will be rejected above this limit.
/// 256 MB is chosen as a reasonable upper bound that allows large batches
/// while preventing catastrophic memory allocation attacks.
const HARD_MAX_MESSAGE_BYTES: u64 = 256 * 1024 * 1024;

/// Maximum length for client IDs (reasonable limit to prevent abuse)
const MAX_CLIENT_ID_LENGTH: usize = 256;

/// Validate a Kafka client ID.
///
/// A valid client ID:
/// - Is empty/None (allowed - client ID is optional in Kafka protocol)
/// - Contains only printable ASCII characters (0x20-0x7E)
/// - Is at most 256 characters long
///
/// Invalid client IDs may indicate:
/// - Malicious clients attempting injection attacks
/// - Misconfigured clients with binary garbage
/// - Clients with encoding issues
///
/// Returns Ok(()) if valid, or an error describing the validation failure.
fn validate_client_id(client_id: &Option<StrBytes>) -> Result<()> {
    let Some(client_id) = client_id else {
        return Ok(()); // None is valid - client ID is optional
    };

    // Empty string is also valid
    if client_id.is_empty() {
        return Ok(());
    }

    // Check length
    if client_id.len() > MAX_CLIENT_ID_LENGTH {
        return Err(StreamlineError::InvalidClientId(format!(
            "Client ID exceeds maximum length of {} characters (got {})",
            MAX_CLIENT_ID_LENGTH,
            client_id.len()
        )));
    }

    // Check that all characters are printable ASCII (0x20-0x7E)
    // This prevents control characters, null bytes, and non-ASCII that could
    // cause issues in logging, metrics, or downstream systems
    for (i, byte) in client_id.as_bytes().iter().enumerate() {
        if !matches!(byte, 0x20..=0x7E) {
            return Err(StreamlineError::InvalidClientId(format!(
                "Client ID contains invalid character at position {}: byte 0x{:02X}. \
                 Only printable ASCII characters (space through tilde) are allowed",
                i, byte
            )));
        }
    }

    Ok(())
}

impl KafkaHandler {
    /// Create a new Kafka protocol handler
    ///
    /// # Errors
    /// Returns an error if the producer state manager cannot be created.
    pub fn new(
        topic_manager: Arc<TopicManager>,
        group_coordinator: Arc<GroupCoordinator>,
    ) -> Result<Self> {
        // Create in-memory producer state manager for simple constructor
        let producer_state_manager = Arc::new(ProducerStateManager::in_memory()?);

        let zerocopy_config = ZeroCopyConfig::default();
        let buffer_pool = Arc::new(BufferPool::new(
            zerocopy_config.buffer_pool_size,
            zerocopy_config.buffer_size,
        ));

        Ok(Self {
            topic_manager,
            group_coordinator,
            producer_state_manager,
            transaction_coordinator: None,
            node_id: 0,
            cluster_id: uuid::Uuid::new_v4().to_string(),
            advertised_addr: DEFAULT_ADVERTISED_ADDR,
            #[cfg(feature = "auth")]
            auth_config: Arc::new(AuthConfig::default()),
            #[cfg(feature = "auth")]
            user_store: Arc::new(None),
            #[cfg(feature = "auth")]
            authorizer: Arc::new(Authorizer::allow_all()),
            #[cfg(feature = "clustering")]
            cluster_manager: None,
            #[cfg(feature = "clustering")]
            replication_manager: None,
            quota_manager: None,
            #[cfg(feature = "auth")]
            delegation_token_manager: None,
            pipeline_config: PipelineConfig::default(),
            max_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
            auto_create_topics: true, // Default to true for backward compatibility
            buffer_pool,
            zerocopy_config,
            response_cache: None,
            resource_limiter: None,
            kip848_engine: Arc::new(ReconciliationEngine::new_default()),
            sharded_runtime: None,
        })
    }

    /// Create a new Kafka protocol handler with producer state manager
    pub fn new_with_producer_state(
        topic_manager: Arc<TopicManager>,
        group_coordinator: Arc<GroupCoordinator>,
        producer_state_manager: Arc<ProducerStateManager>,
    ) -> Self {
        let zerocopy_config = ZeroCopyConfig::default();
        let buffer_pool = Arc::new(BufferPool::new(
            zerocopy_config.buffer_pool_size,
            zerocopy_config.buffer_size,
        ));

        Self {
            topic_manager,
            group_coordinator,
            producer_state_manager,
            transaction_coordinator: None,
            node_id: 0,
            cluster_id: uuid::Uuid::new_v4().to_string(),
            advertised_addr: DEFAULT_ADVERTISED_ADDR,
            #[cfg(feature = "auth")]
            auth_config: Arc::new(AuthConfig::default()),
            #[cfg(feature = "auth")]
            user_store: Arc::new(None),
            #[cfg(feature = "auth")]
            authorizer: Arc::new(Authorizer::allow_all()),
            #[cfg(feature = "clustering")]
            cluster_manager: None,
            #[cfg(feature = "clustering")]
            replication_manager: None,
            quota_manager: None,
            #[cfg(feature = "auth")]
            delegation_token_manager: None,
            pipeline_config: PipelineConfig::default(),
            max_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
            auto_create_topics: true,
            buffer_pool,
            zerocopy_config,
            response_cache: None,
            resource_limiter: None,
            kip848_engine: Arc::new(ReconciliationEngine::new_default()),
            sharded_runtime: None,
        }
    }

    /// Create a new Kafka protocol handler with authentication (requires auth feature)
    ///
    /// # Errors
    /// Returns an error if the producer state manager cannot be created.
    #[cfg(feature = "auth")]
    pub fn new_with_auth(
        topic_manager: Arc<TopicManager>,
        group_coordinator: Arc<GroupCoordinator>,
        auth_config: AuthConfig,
        user_store: Option<UserStore>,
    ) -> Result<Self> {
        let producer_state_manager = Arc::new(ProducerStateManager::in_memory()?);

        let zerocopy_config = ZeroCopyConfig::default();
        let buffer_pool = Arc::new(BufferPool::new(
            zerocopy_config.buffer_pool_size,
            zerocopy_config.buffer_size,
        ));

        Ok(Self {
            topic_manager,
            group_coordinator,
            producer_state_manager,
            transaction_coordinator: None,
            node_id: 0,
            cluster_id: uuid::Uuid::new_v4().to_string(),
            advertised_addr: DEFAULT_ADVERTISED_ADDR,
            auth_config: Arc::new(auth_config),
            user_store: Arc::new(user_store),
            authorizer: Arc::new(Authorizer::allow_all()),
            #[cfg(feature = "clustering")]
            cluster_manager: None,
            #[cfg(feature = "clustering")]
            replication_manager: None,
            quota_manager: None,
            delegation_token_manager: None,
            pipeline_config: PipelineConfig::default(),
            max_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
            auto_create_topics: true,
            buffer_pool,
            zerocopy_config,
            response_cache: None,
            resource_limiter: None,
            kip848_engine: Arc::new(ReconciliationEngine::new_default()),
            sharded_runtime: None,
        })
    }

    /// Create a new Kafka protocol handler with authentication and authorization (requires auth feature)
    #[cfg(feature = "auth")]
    pub fn new_with_auth_and_acl(
        topic_manager: Arc<TopicManager>,
        group_coordinator: Arc<GroupCoordinator>,
        auth_config: AuthConfig,
        user_store: Option<UserStore>,
        acl_config: AclConfig,
    ) -> Result<Self> {
        let producer_state_manager = Arc::new(ProducerStateManager::in_memory()?);

        // Create authorizer configuration
        let authorizer_config = AuthorizerConfig {
            enabled: acl_config.enabled,
            default_permission: Permission::Deny,
            super_users: acl_config.super_users.into_iter().collect(),
            allow_if_no_acls: acl_config.allow_if_no_acls,
            rbac_enabled: false, // RBAC can be enabled via config when needed
        };

        // Load ACLs from file if provided
        let authorizer = if let Some(ref acl_file) = acl_config.acl_file {
            if acl_file.exists() {
                info!(file = %acl_file.display(), "Loading ACLs from file");
                Authorizer::from_file(acl_file, authorizer_config)?
            } else {
                info!(file = %acl_file.display(), "ACL file not found, starting with empty ACLs");
                Authorizer::new(authorizer_config)
            }
        } else {
            Authorizer::new(authorizer_config)
        };

        let zerocopy_config = ZeroCopyConfig::default();
        let buffer_pool = Arc::new(BufferPool::new(
            zerocopy_config.buffer_pool_size,
            zerocopy_config.buffer_size,
        ));

        Ok(Self {
            topic_manager,
            group_coordinator,
            producer_state_manager,
            transaction_coordinator: None,
            kip848_engine: Arc::new(ReconciliationEngine::new_default()),
            node_id: 0,
            cluster_id: uuid::Uuid::new_v4().to_string(),
            advertised_addr: DEFAULT_ADVERTISED_ADDR,
            auth_config: Arc::new(auth_config),
            user_store: Arc::new(user_store),
            authorizer: Arc::new(authorizer),
            #[cfg(feature = "clustering")]
            cluster_manager: None,
            #[cfg(feature = "clustering")]
            replication_manager: None,
            quota_manager: None,
            delegation_token_manager: None,
            pipeline_config: PipelineConfig::default(),
            max_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
            auto_create_topics: true,
            buffer_pool,
            zerocopy_config,
            response_cache: None,
            resource_limiter: None,
            sharded_runtime: None,
        })
    }

    /// Set the advertised address for clients to connect
    /// This is used in single-node mode; in cluster mode, ClusterConfig.advertised_addr is used
    pub fn with_advertised_addr(mut self, addr: std::net::SocketAddr) -> Self {
        self.advertised_addr = addr;
        self
    }

    /// Set the cluster manager for multi-broker mode (requires clustering feature)
    #[cfg(feature = "clustering")]
    pub fn with_cluster_manager(mut self, cluster_manager: Arc<ClusterManager>) -> Self {
        self.node_id = cluster_manager.node_id() as i32;
        self.cluster_manager = Some(cluster_manager);
        self
    }

    /// Set the replication manager for multi-node replication (requires clustering feature)
    #[cfg(feature = "clustering")]
    pub fn with_replication_manager(
        mut self,
        replication_manager: Arc<ReplicationManager>,
    ) -> Self {
        self.replication_manager = Some(replication_manager);
        self
    }

    /// Set the transaction coordinator for transaction support
    pub fn with_transaction_coordinator(
        mut self,
        transaction_coordinator: Arc<TransactionCoordinator>,
    ) -> Self {
        self.transaction_coordinator = Some(transaction_coordinator);
        self
    }

    /// Set the quota manager for client quotas and throttling
    pub fn with_quota_manager(mut self, quota_manager: Arc<QuotaManager>) -> Self {
        self.quota_manager = Some(quota_manager);
        self
    }

    /// Set the delegation token manager for delegation token authentication (requires auth feature)
    #[cfg(feature = "auth")]
    pub fn with_delegation_token_manager(
        mut self,
        delegation_token_manager: Arc<DelegationTokenManager>,
    ) -> Self {
        self.delegation_token_manager = Some(delegation_token_manager);
        self
    }

    /// Set the resource limiter for request size validation
    ///
    /// When configured, this limiter validates incoming request sizes against
    /// configurable limits before allocating memory buffers. This provides an
    /// additional layer of protection beyond the hard-coded maximum.
    pub fn with_resource_limiter(mut self, limiter: Arc<ResourceLimiter>) -> Self {
        self.resource_limiter = Some(limiter);
        self
    }

    /// Set the pipeline configuration for connection pipelining
    pub fn with_pipeline_config(mut self, config: PipelineConfig) -> Self {
        self.pipeline_config = config;
        self
    }

    /// Set the maximum message/request size in bytes.
    /// Values exceeding HARD_MAX_MESSAGE_BYTES (256 MB) will be capped to prevent DoS.
    pub fn with_max_message_bytes(mut self, max_bytes: u64) -> Self {
        if max_bytes > HARD_MAX_MESSAGE_BYTES {
            warn!(
                requested = max_bytes,
                capped_to = HARD_MAX_MESSAGE_BYTES,
                "Requested max_message_bytes exceeds hard cap, capping to prevent DoS"
            );
            self.max_message_bytes = HARD_MAX_MESSAGE_BYTES;
        } else if max_bytes == 0 {
            warn!(
                "max_message_bytes cannot be 0, using default {}",
                DEFAULT_MAX_MESSAGE_BYTES
            );
            self.max_message_bytes = DEFAULT_MAX_MESSAGE_BYTES;
        } else {
            self.max_message_bytes = max_bytes;
        }
        self
    }

    /// Enable or disable automatic topic creation on produce requests
    ///
    /// When enabled (default), topics are automatically created when a producer
    /// sends messages to a non-existent topic. When disabled, produces to
    /// non-existent topics will fail with UNKNOWN_TOPIC_OR_PARTITION.
    ///
    /// This corresponds to Kafka's `auto.create.topics.enable` configuration.
    pub fn with_auto_create_topics(mut self, enabled: bool) -> Self {
        self.auto_create_topics = enabled;
        self
    }

    /// Set the zero-copy configuration for optimized I/O
    ///
    /// This configures buffer pooling, memory mapping, sendfile, and response
    /// caching for reduced memory copies during data transfer.
    pub fn with_zerocopy_config(mut self, config: ZeroCopyConfig) -> Self {
        // Rebuild buffer pool with new config
        self.buffer_pool = Arc::new(BufferPool::new(config.buffer_pool_size, config.buffer_size));
        self.zerocopy_config = config;
        self
    }

    /// Set the response cache for sendfile optimization
    ///
    /// The response cache stores fetch responses on disk so they can be served
    /// using zero-copy sendfile() on repeated requests. This is particularly
    /// beneficial for consumer groups where multiple consumers read the same data.
    ///
    /// The cache respects the zerocopy_config.enable_response_cache setting -
    /// if caching is disabled in the config, responses won't be stored.
    pub fn with_response_cache(mut self, cache: Arc<ResponseCache>) -> Self {
        self.response_cache = Some(cache);
        self
    }

    /// Set the sharded runtime for thread-per-core partition routing
    ///
    /// When set, Produce and Fetch requests will route partition work to the
    /// shard that owns the partition (`partition_id % shard_count`). This
    /// achieves true thread-per-core execution with CPU affinity and cache
    /// locality, similar to Redpanda's architecture.
    ///
    /// When not set, requests are processed directly by the Tokio runtime.
    pub fn with_sharded_runtime(mut self, runtime: Arc<ShardedRuntime>) -> Self {
        self.sharded_runtime = Some(runtime);
        self
    }


    /// Create a cloneable handle for use in spawned tasks
    fn clone_for_task(&self) -> KafkaHandlerHandle {
        KafkaHandlerHandle {
            inner: KafkaHandler {
                topic_manager: self.topic_manager.clone(),
                group_coordinator: self.group_coordinator.clone(),
                producer_state_manager: self.producer_state_manager.clone(),
                transaction_coordinator: self.transaction_coordinator.clone(),
                kip848_engine: self.kip848_engine.clone(),
                node_id: self.node_id,
                cluster_id: self.cluster_id.clone(),
                advertised_addr: self.advertised_addr,
                #[cfg(feature = "auth")]
                auth_config: self.auth_config.clone(),
                #[cfg(feature = "auth")]
                user_store: self.user_store.clone(),
                #[cfg(feature = "auth")]
                authorizer: self.authorizer.clone(),
                #[cfg(feature = "clustering")]
                cluster_manager: self.cluster_manager.clone(),
                #[cfg(feature = "clustering")]
                replication_manager: self.replication_manager.clone(),
                quota_manager: self.quota_manager.clone(),
                #[cfg(feature = "auth")]
                delegation_token_manager: self.delegation_token_manager.clone(),
                pipeline_config: PipelineConfig::default(), // Not used in task processing
                max_message_bytes: self.max_message_bytes,
                auto_create_topics: self.auto_create_topics,
                buffer_pool: self.buffer_pool.clone(),
                zerocopy_config: self.zerocopy_config.clone(),
                response_cache: self.response_cache.clone(),
                resource_limiter: self.resource_limiter.clone(),
                sharded_runtime: self.sharded_runtime.clone(),
            },
        }
    }

    /// Process a Kafka message and return the response with optional cache key
    #[tracing::instrument(level = "debug", skip(self, data, session_manager), fields(msg_size = data.len()))]
    async fn process_message(
        &self,
        data: &[u8],
        session_manager: &SessionManager,
    ) -> Result<ProcessedResponse> {
        let start_time = Instant::now();
        let request_size = data.len();

        let result = self
            .process_message_inner(data.to_vec(), session_manager)
            .await;

        // Extract API key for metrics
        let api_key_name = if data.len() >= 2 {
            let api_key = i16::from_be_bytes([data[0], data[1]]);
            Self::api_key_name(api_key)
        } else {
            "unknown"
        };

        let (response, has_error) = match result {
            Ok(resp) => (resp, false),
            Err(e) => {
                metrics::record_request(api_key_name, start_time.elapsed(), request_size, 0, true);
                return Err(e);
            }
        };

        // Record metrics
        metrics::record_request(
            api_key_name,
            start_time.elapsed(),
            request_size,
            response.data.len(),
            has_error,
        );

        Ok(response)
    }


    /// Process a Kafka message and return the response (inner implementation)
    #[tracing::instrument(level = "debug", skip(self, data, session_manager))]
    async fn process_message_inner(
        &self,
        data: Vec<u8>,
        session_manager: &SessionManager,
    ) -> Result<ProcessedResponse> {
        // Parse request header (peek at API key and version without copying)
        if data.len() < 4 {
            return Err(StreamlineError::protocol_msg(
                "Message too short".to_string(),
            ));
        }

        // Defense-in-depth: validate message size even though it's checked at the network layer
        // This protects against bugs in callers or direct invocations that bypass network checks
        if data.len() as u64 > self.max_message_bytes {
            return Err(StreamlineError::protocol_msg(format!(
                "Message size {} exceeds maximum allowed size {}",
                data.len(),
                self.max_message_bytes
            )));
        }

        // Read API key and version directly from slice (zero-copy peek)
        let api_key = i16::from_be_bytes([data[0], data[1]]);
        let api_version = i16::from_be_bytes([data[2], data[3]]);

        debug!(
            api_key = api_key,
            api_version = api_version,
            "Processing request"
        );

        // Take ownership instead of copying (zero-copy optimization)
        let mut buf = Bytes::from(data);

        // Determine header version based on API key and version
        let header_version = Self::request_header_version(api_key, api_version);

        // Parse request header
        let header = RequestHeader::decode(&mut buf, header_version).map_err(|e| {
            StreamlineError::protocol_msg(format!("Failed to decode header: {}", e))
        })?;

        // Validate client ID BEFORE logging to prevent log injection attacks
        // Invalid client IDs with control characters (newlines, etc.) could corrupt logs
        validate_client_id(&header.client_id)?;

        debug!(
            api_key = header.request_api_key,
            api_version = header.request_api_version,
            correlation_id = header.correlation_id,
            client_id = ?header.client_id,
            "Request header"
        );

        // Route to appropriate handler
        let api_key_enum = ApiKey::try_from(header.request_api_key).map_err(|_| {
            StreamlineError::protocol_msg(format!("Unknown API key: {}", header.request_api_key))
        })?;

        // Check authentication for requests that require it
        // Allow ApiVersions, SaslHandshake, and SaslAuthenticate without authentication
        #[cfg(feature = "auth")]
        {
            let requires_auth = !matches!(
                api_key_enum,
                ApiKey::ApiVersions | ApiKey::SaslHandshake | ApiKey::SaslAuthenticate
            );

            if requires_auth && self.auth_config.enabled && !self.auth_config.allow_anonymous {
                // Check if user is authenticated
                if !session_manager.is_authenticated().await {
                    warn!(api_key = ?api_key_enum, "Unauthenticated request rejected");

                    // Return authentication error
                    // For Kafka protocol, we need to return proper error responses
                    // Different APIs have different error response structures
                    // For now, return a protocol error
                    return Err(StreamlineError::AuthenticationFailed(
                        "Authentication required".to_string(),
                    ));
                }
            }
        }

        // Get principal and host for authorization checks
        let principal = self.get_principal(session_manager).await;
        let host = session_manager.peer_addr_string();

        let dispatch_result = self
            .dispatch_request(
                api_key_enum,
                &header,
                buf,
                session_manager,
                principal.as_str(),
                host.as_str(),
            )
            .await?;
        let DispatchResult {
            response_header,
            response_body,
            effective_api_version,
            cache_key: fetch_cache_key,
        } = dispatch_result;

        // Encode response
        // Use effective_api_version which may differ from request version for ApiVersions
        let response_header_version =
            Self::response_header_version(header.request_api_key, effective_api_version);
        let mut response_buf = BytesMut::new();
        response_header
            .encode(&mut response_buf, response_header_version)
            .map_err(|e| {
                StreamlineError::protocol_msg(format!("Failed to encode response header: {}", e))
            })?;
        let header_len = response_buf.len();
        response_buf.extend_from_slice(&response_body);

        debug!(
            api_key = header.request_api_key,
            api_version = effective_api_version,
            response_header_version = response_header_version,
            correlation_id = header.correlation_id,
            header_len = header_len,
            full_response_len = response_buf.len(),
            full_response_hex = ?hex::encode(&response_buf[..response_buf.len().min(150)]),
            "Full response encoded"
        );

        Ok(ProcessedResponse {
            data: response_buf.to_vec(),
            cache_key: fetch_cache_key,
        })
    }

    #[tracing::instrument(skip(self, buf, session_manager), fields(api_key = ?api_key_enum, correlation_id = header.correlation_id, principal, host))]
    async fn dispatch_request(
        &self,
        api_key_enum: ApiKey,
        header: &RequestHeader,
        buf: Bytes,
        session_manager: &SessionManager,
        principal: &str,
        host: &str,
    ) -> Result<DispatchResult> {
        super::handlers::dispatch_request(
            self,
            api_key_enum,
            header,
            buf,
            session_manager,
            principal,
            host,
        )
        .await
    }












































    /// Encode a response message
    pub(super) fn encode_response<T: Encodable>(
        &self,
        response: &T,
        version: i16,
    ) -> Result<Vec<u8>> {
        let mut buf = BytesMut::new();
        response.encode(&mut buf, version).map_err(|e| {
            StreamlineError::protocol_msg(format!("Failed to encode response: {}", e))
        })?;
        Ok(buf.to_vec())
    }

    /// Create a response header with the given correlation ID
    pub(super) fn create_response_header(&self, correlation_id: i32) -> ResponseHeader {
        ResponseHeader::default().with_correlation_id(correlation_id)
    }
















































}
