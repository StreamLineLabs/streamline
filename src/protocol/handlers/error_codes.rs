//! Kafka protocol error codes
//!
//! See: <https://kafka.apache.org/protocol#protocol_error_codes>

/// No error - operation succeeded
pub const NONE: i16 = 0;
/// Unknown server error
pub const UNKNOWN_SERVER_ERROR: i16 = -1;
/// Offset out of range
pub const OFFSET_OUT_OF_RANGE: i16 = 1;
/// Corrupt message - CRC check failed
pub const CORRUPT_MESSAGE: i16 = 2;
/// Unknown topic or partition
pub const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
/// Invalid fetch size
pub const INVALID_FETCH_SIZE: i16 = 4;
/// Leader not available - partition leader election in progress
pub const LEADER_NOT_AVAILABLE: i16 = 5;
/// Not leader for partition
pub const NOT_LEADER_OR_FOLLOWER: i16 = 6;
/// Request timed out
pub const REQUEST_TIMED_OUT: i16 = 7;
/// Broker not available
pub const BROKER_NOT_AVAILABLE: i16 = 8;
/// Replica not available
pub const REPLICA_NOT_AVAILABLE: i16 = 9;
/// Message too large for configured maximum
pub const MESSAGE_TOO_LARGE: i16 = 10;
/// Stale controller epoch
pub const STALE_CONTROLLER_EPOCH: i16 = 11;
/// Offset metadata too large
pub const OFFSET_METADATA_TOO_LARGE: i16 = 12;
/// Network exception during request
pub const NETWORK_EXCEPTION: i16 = 13;
/// Coordinator loading in progress
pub const COORDINATOR_LOAD_IN_PROGRESS: i16 = 14;
/// Group coordinator not available
pub const COORDINATOR_NOT_AVAILABLE: i16 = 15;
/// Not coordinator for group
pub const NOT_COORDINATOR: i16 = 16;
/// Invalid topic exception
pub const INVALID_TOPIC_EXCEPTION: i16 = 17;
/// Record batch too large
pub const RECORD_LIST_TOO_LARGE: i16 = 18;
/// Not enough replicas available
pub const NOT_ENOUGH_REPLICAS: i16 = 19;
/// Not enough in-sync replicas
pub const NOT_ENOUGH_REPLICAS_AFTER_APPEND: i16 = 20;
/// Invalid required acks
pub const INVALID_REQUIRED_ACKS: i16 = 21;
/// Illegal generation
pub const ILLEGAL_GENERATION: i16 = 22;
/// Inconsistent group protocol
pub const INCONSISTENT_GROUP_PROTOCOL: i16 = 23;
/// Invalid group id
pub const INVALID_GROUP_ID: i16 = 24;
/// Unknown member id
pub const UNKNOWN_MEMBER_ID: i16 = 25;
/// Invalid session timeout
pub const INVALID_SESSION_TIMEOUT: i16 = 26;
/// Rebalance in progress
pub const REBALANCE_IN_PROGRESS: i16 = 27;
/// Invalid commit offset size
pub const INVALID_COMMIT_OFFSET_SIZE: i16 = 28;
/// Topic authorization failed
pub const TOPIC_AUTHORIZATION_FAILED: i16 = 29;
/// Group authorization failed
pub const GROUP_AUTHORIZATION_FAILED: i16 = 30;
/// Cluster authorization failed
pub const CLUSTER_AUTHORIZATION_FAILED: i16 = 31;
/// Invalid timestamp
pub const INVALID_TIMESTAMP: i16 = 32;
/// Unsupported SASL mechanism
pub const UNSUPPORTED_SASL_MECHANISM: i16 = 33;
/// Illegal SASL state
pub const ILLEGAL_SASL_STATE: i16 = 34;
/// Unsupported version
pub const UNSUPPORTED_VERSION: i16 = 35;
/// Topic already exists
pub const TOPIC_ALREADY_EXISTS: i16 = 36;
/// Invalid partitions
pub const INVALID_PARTITIONS: i16 = 37;
/// Invalid replication factor
pub const INVALID_REPLICATION_FACTOR: i16 = 38;
/// Invalid replica assignment
pub const INVALID_REPLICA_ASSIGNMENT: i16 = 39;
/// Invalid config
pub const INVALID_CONFIG: i16 = 40;
/// Not controller
pub const NOT_CONTROLLER: i16 = 41;
/// Invalid request
pub const INVALID_REQUEST: i16 = 42;
/// Unsupported for message format
pub const UNSUPPORTED_FOR_MESSAGE_FORMAT: i16 = 43;
/// Policy violation
pub const POLICY_VIOLATION: i16 = 44;
/// Out of order sequence number
pub const OUT_OF_ORDER_SEQUENCE_NUMBER: i16 = 45;
/// Duplicate sequence number
pub const DUPLICATE_SEQUENCE_NUMBER: i16 = 46;
/// Invalid producer epoch
pub const INVALID_PRODUCER_EPOCH: i16 = 47;
/// Invalid transaction state
pub const INVALID_TXN_STATE: i16 = 48;
/// Invalid producer ID mapping
pub const INVALID_PRODUCER_ID_MAPPING: i16 = 49;
/// Invalid transaction timeout
pub const INVALID_TRANSACTION_TIMEOUT: i16 = 50;
/// Concurrent transactions
pub const CONCURRENT_TRANSACTIONS: i16 = 51;
/// Transaction coordinator fenced
pub const TRANSACTION_COORDINATOR_FENCED: i16 = 52;
/// Transactional ID authorization failed
pub const TRANSACTIONAL_ID_AUTHORIZATION_FAILED: i16 = 53;
/// Security disabled
pub const SECURITY_DISABLED: i16 = 54;
/// Operation not attempted
pub const OPERATION_NOT_ATTEMPTED: i16 = 55;
/// Kafka storage error
pub const KAFKA_STORAGE_ERROR: i16 = 56;
/// Log directory not found
pub const LOG_DIR_NOT_FOUND: i16 = 57;
/// SASL authentication failed
pub const SASL_AUTHENTICATION_FAILED: i16 = 58;
/// Unknown producer ID
pub const UNKNOWN_PRODUCER_ID: i16 = 59;
/// Reassignment in progress
pub const REASSIGNMENT_IN_PROGRESS: i16 = 60;
/// Delegation token not found
pub const DELEGATION_TOKEN_AUTH_DISABLED: i16 = 61;
/// Delegation token not found
pub const DELEGATION_TOKEN_NOT_FOUND: i16 = 62;
/// Delegation token owner mismatch
pub const DELEGATION_TOKEN_OWNER_MISMATCH: i16 = 63;
/// Delegation token request not allowed
pub const DELEGATION_TOKEN_REQUEST_NOT_ALLOWED: i16 = 64;
/// Delegation token authorization failed
pub const DELEGATION_TOKEN_AUTHORIZATION_FAILED: i16 = 65;
/// Delegation token expired
pub const DELEGATION_TOKEN_EXPIRED: i16 = 66;
/// Invalid principal type
pub const INVALID_PRINCIPAL_TYPE: i16 = 67;
/// Non-empty group cannot be deleted
pub const NON_EMPTY_GROUP: i16 = 68;
/// Group ID not found
pub const GROUP_ID_NOT_FOUND: i16 = 69;
/// Fetch session ID not found
pub const FETCH_SESSION_ID_NOT_FOUND: i16 = 70;
/// Invalid fetch session epoch
pub const INVALID_FETCH_SESSION_EPOCH: i16 = 71;
/// Listener not found
pub const LISTENER_NOT_FOUND: i16 = 72;
/// Topic deletion disabled
pub const TOPIC_DELETION_DISABLED: i16 = 73;
/// Fenced leader epoch
pub const FENCED_LEADER_EPOCH: i16 = 74;
/// Unknown leader epoch
pub const UNKNOWN_LEADER_EPOCH: i16 = 75;
/// Unsupported compression type
pub const UNSUPPORTED_COMPRESSION_TYPE: i16 = 76;
/// Stale broker epoch
pub const STALE_BROKER_EPOCH: i16 = 77;
/// Offset not available
pub const OFFSET_NOT_AVAILABLE: i16 = 78;
/// Member ID required
pub const MEMBER_ID_REQUIRED: i16 = 79;
/// Preferred leader not available
pub const PREFERRED_LEADER_NOT_AVAILABLE: i16 = 80;
/// Group max size reached
pub const GROUP_MAX_SIZE_REACHED: i16 = 81;
/// Fenced instance ID
pub const FENCED_INSTANCE_ID: i16 = 82;
/// Throttling quota exceeded
pub const THROTTLING_QUOTA_EXCEEDED: i16 = 89;
/// Producer fenced
pub const PRODUCER_FENCED: i16 = 90;
/// Resource not found
pub const RESOURCE_NOT_FOUND: i16 = 91;
/// Transactional ID not found
pub const TRANSACTIONAL_ID_NOT_FOUND: i16 = 105;
