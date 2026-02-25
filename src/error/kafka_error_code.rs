//! Kafka protocol error codes for Streamline

/// Kafka protocol error codes
/// See: <https://kafka.apache.org/protocol.html#protocol_error_codes>
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i16)]
pub enum KafkaErrorCode {
    /// No error
    None = 0,
    /// Unknown server error
    UnknownServerError = -1,
    /// The requested offset is outside the range of offsets
    OffsetOutOfRange = 1,
    /// Message failed its CRC check or is otherwise corrupt
    CorruptMessage = 2,
    /// Unknown topic or partition
    UnknownTopicOrPartition = 3,
    /// Invalid message size
    InvalidMessageSize = 4,
    /// Not leader for partition
    LeaderNotAvailable = 5,
    /// Not leader for partition
    NotLeaderOrFollower = 6,
    /// Request timed out
    RequestTimedOut = 7,
    /// Broker not available
    BrokerNotAvailable = 8,
    /// Replica not available
    ReplicaNotAvailable = 9,
    /// Message too large
    MessageTooLarge = 10,
    /// Stale controller epoch
    StaleControllerEpoch = 11,
    /// Offset metadata string too large
    OffsetMetadataTooLarge = 12,
    /// Broker is shutting down
    NetworkException = 13,
    /// Coordinator load in progress
    CoordinatorLoadInProgress = 14,
    /// Coordinator not available
    CoordinatorNotAvailable = 15,
    /// Not coordinator for group
    NotCoordinator = 16,
    /// Invalid topic
    InvalidTopicException = 17,
    /// Batch larger than max configured size
    RecordListTooLarge = 18,
    /// Not enough in-sync replicas
    NotEnoughReplicas = 19,
    /// Not enough in-sync replicas after append
    NotEnoughReplicasAfterAppend = 20,
    /// Invalid required acks
    InvalidRequiredAcks = 21,
    /// Illegal generation
    IllegalGeneration = 22,
    /// Inconsistent group protocol
    InconsistentGroupProtocol = 23,
    /// Invalid group id
    InvalidGroupId = 24,
    /// Unknown member
    UnknownMemberId = 25,
    /// Invalid session timeout
    InvalidSessionTimeout = 26,
    /// Rebalance in progress
    RebalanceInProgress = 27,
    /// Invalid commit offset size
    InvalidCommitOffsetSize = 28,
    /// Topic authorization failed
    TopicAuthorizationFailed = 29,
    /// Group authorization failed
    GroupAuthorizationFailed = 30,
    /// Cluster authorization failed
    ClusterAuthorizationFailed = 31,
    /// Invalid timestamp
    InvalidTimestamp = 32,
    /// Unsupported SASL mechanism
    UnsupportedSaslMechanism = 33,
    /// Illegal SASL state
    IllegalSaslState = 34,
    /// Unsupported version
    UnsupportedVersion = 35,
    /// Topic already exists
    TopicAlreadyExists = 36,
    /// Invalid partitions
    InvalidPartitions = 37,
    /// Invalid replication factor
    InvalidReplicationFactor = 38,
    /// Invalid replica assignment
    InvalidReplicaAssignment = 39,
    /// Invalid config
    InvalidConfig = 40,
    /// Not controller
    NotController = 41,
    /// Invalid request
    InvalidRequest = 42,
    /// Unsupported for message format
    UnsupportedForMessageFormat = 43,
    /// Policy violation
    PolicyViolation = 44,
    /// Out of order sequence number
    OutOfOrderSequenceNumber = 45,
    /// Duplicate sequence number
    DuplicateSequenceNumber = 46,
    /// Invalid producer epoch
    InvalidProducerEpoch = 47,
    /// Invalid txn state
    InvalidTxnState = 48,
    /// Invalid producer id mapping
    InvalidProducerIdMapping = 49,
    /// Invalid transaction timeout
    InvalidTransactionTimeout = 50,
    /// Concurrent transactions
    ConcurrentTransactions = 51,
    /// Transaction coordinator fenced
    TransactionCoordinatorFenced = 52,
    /// Transactional id authorization failed
    TransactionalIdAuthorizationFailed = 53,
    /// Security disabled
    SecurityDisabled = 54,
    /// Operation not attempted
    OperationNotAttempted = 55,
    /// Kafka storage error
    KafkaStorageError = 56,
    /// Log dir not found
    LogDirNotFound = 57,
    /// SASL authentication failed
    SaslAuthenticationFailed = 58,
    /// Unknown producer id
    UnknownProducerId = 59,
    /// Reassignment in progress
    ReassignmentInProgress = 60,
    /// Delegation token auth disabled
    DelegationTokenAuthDisabled = 61,
    /// Delegation token not found
    DelegationTokenNotFound = 62,
    /// Delegation token owner mismatch
    DelegationTokenOwnerMismatch = 63,
    /// Delegation token request not allowed
    DelegationTokenRequestNotAllowed = 64,
    /// Delegation token authorization failed
    DelegationTokenAuthorizationFailed = 65,
    /// Delegation token expired
    DelegationTokenExpired = 66,
    /// Invalid principal type
    InvalidPrincipalType = 67,
    /// Non empty group
    NonEmptyGroup = 68,
    /// Group id not found
    GroupIdNotFound = 69,
    /// Fetch session id not found
    FetchSessionIdNotFound = 70,
    /// Invalid fetch session epoch
    InvalidFetchSessionEpoch = 71,
    /// Listener not found
    ListenerNotFound = 72,
    /// Topic deletion disabled
    TopicDeletionDisabled = 73,
    /// Fenced leader epoch
    FencedLeaderEpoch = 74,
    /// Unknown leader epoch
    UnknownLeaderEpoch = 75,
    /// Unsupported compression type
    UnsupportedCompressionType = 76,
    /// Stale broker epoch
    StaleBrokerEpoch = 77,
    /// Offset not available
    OffsetNotAvailable = 78,
    /// Member id required
    MemberIdRequired = 79,
    /// Preferred leader not available
    PreferredLeaderNotAvailable = 80,
    /// Group max size reached
    GroupMaxSizeReached = 81,
    /// Fenced instance id
    FencedInstanceId = 82,
    /// Eligible leaders not available
    EligibleLeadersNotAvailable = 83,
    /// Election not needed
    ElectionNotNeeded = 84,
    /// No reassignment in progress
    NoReassignmentInProgress = 85,
    /// Group subscribed to topic
    GroupSubscribedToTopic = 86,
    /// Invalid record
    InvalidRecord = 87,
    /// Unstable offset commit
    UnstableOffsetCommit = 88,
    /// Throttling quota exceeded
    ThrottlingQuotaExceeded = 89,
    /// Producer fenced
    ProducerFenced = 90,
    /// Resource not found
    ResourceNotFound = 91,
    /// Duplicate resource
    DuplicateResource = 92,
    /// Unacceptable credential
    UnacceptableCredential = 93,
    /// Inconsistent voter set
    InconsistentVoterSet = 94,
    /// Invalid update version
    InvalidUpdateVersion = 95,
    /// Feature update failed
    FeatureUpdateFailed = 96,
    /// Principal deserialization failure
    PrincipalDeserializationFailure = 97,
    /// Snapshot not found
    SnapshotNotFound = 98,
    /// Position out of range
    PositionOutOfRange = 99,
    /// Unknown topic id
    UnknownTopicId = 100,
    /// Duplicate broker registration
    DuplicateBrokerRegistration = 101,
    /// Broker id not registered
    BrokerIdNotRegistered = 102,
    /// Inconsistent topic id
    InconsistentTopicId = 103,
    /// Inconsistent cluster id
    InconsistentClusterId = 104,
    /// Transactional id not found
    TransactionalIdNotFound = 105,
    /// Fetch session topic id error
    FetchSessionTopicIdError = 106,
}

impl KafkaErrorCode {
    /// Returns true if this error is retriable
    pub fn is_retriable(&self) -> bool {
        matches!(
            self,
            KafkaErrorCode::CorruptMessage
                | KafkaErrorCode::UnknownTopicOrPartition
                | KafkaErrorCode::LeaderNotAvailable
                | KafkaErrorCode::NotLeaderOrFollower
                | KafkaErrorCode::RequestTimedOut
                | KafkaErrorCode::ReplicaNotAvailable
                | KafkaErrorCode::NetworkException
                | KafkaErrorCode::CoordinatorLoadInProgress
                | KafkaErrorCode::CoordinatorNotAvailable
                | KafkaErrorCode::NotCoordinator
                | KafkaErrorCode::NotEnoughReplicas
                | KafkaErrorCode::NotEnoughReplicasAfterAppend
                | KafkaErrorCode::KafkaStorageError
                | KafkaErrorCode::FetchSessionIdNotFound
                | KafkaErrorCode::OffsetNotAvailable
                | KafkaErrorCode::PreferredLeaderNotAvailable
                | KafkaErrorCode::UnstableOffsetCommit
                | KafkaErrorCode::ThrottlingQuotaExceeded
        )
    }

    /// Convert to the i16 error code used in Kafka protocol
    pub fn as_i16(&self) -> i16 {
        *self as i16
    }
}
