//! Protocol constants and version tables for Kafka wire protocol

use kafka_protocol::messages::ApiKey;

use crate::protocol::response_cache::CacheKey;

#[derive(Clone, Copy)]
pub(super) struct HeaderVersionRule {
    pub(super) api_key: ApiKey,
    pub(super) min_version: i16,
}

pub(super) const REQUEST_HEADER_FLEXIBLE_VERSIONS: &[HeaderVersionRule] = &[
    HeaderVersionRule {
        api_key: ApiKey::ApiVersions,
        min_version: 3,
    },
    HeaderVersionRule {
        api_key: ApiKey::ControlledShutdown,
        min_version: 3,
    },
    HeaderVersionRule {
        api_key: ApiKey::Metadata,
        min_version: 9,
    },
    HeaderVersionRule {
        api_key: ApiKey::Produce,
        min_version: 9,
    },
    HeaderVersionRule {
        api_key: ApiKey::Fetch,
        min_version: 12,
    },
    HeaderVersionRule {
        api_key: ApiKey::ListOffsets,
        min_version: 6,
    },
    HeaderVersionRule {
        api_key: ApiKey::CreateTopics,
        min_version: 5,
    },
    HeaderVersionRule {
        api_key: ApiKey::DeleteTopics,
        min_version: 4,
    },
    HeaderVersionRule {
        api_key: ApiKey::SaslAuthenticate,
        min_version: 2,
    },
    HeaderVersionRule {
        api_key: ApiKey::FindCoordinator,
        min_version: 3,
    },
    HeaderVersionRule {
        api_key: ApiKey::JoinGroup,
        min_version: 6,
    },
    HeaderVersionRule {
        api_key: ApiKey::SyncGroup,
        min_version: 4,
    },
    HeaderVersionRule {
        api_key: ApiKey::Heartbeat,
        min_version: 4,
    },
    HeaderVersionRule {
        api_key: ApiKey::LeaveGroup,
        min_version: 4,
    },
    HeaderVersionRule {
        api_key: ApiKey::OffsetFetch,
        min_version: 6,
    },
    HeaderVersionRule {
        api_key: ApiKey::OffsetCommit,
        min_version: 8,
    },
    HeaderVersionRule {
        api_key: ApiKey::DescribeGroups,
        min_version: 5,
    },
    HeaderVersionRule {
        api_key: ApiKey::ListGroups,
        min_version: 3,
    },
    HeaderVersionRule {
        api_key: ApiKey::DeleteGroups,
        min_version: 2,
    },
    HeaderVersionRule {
        api_key: ApiKey::DescribeAcls,
        min_version: 2,
    },
    HeaderVersionRule {
        api_key: ApiKey::CreateAcls,
        min_version: 2,
    },
    HeaderVersionRule {
        api_key: ApiKey::DeleteAcls,
        min_version: 2,
    },
    HeaderVersionRule {
        api_key: ApiKey::InitProducerId,
        min_version: 2,
    },
    HeaderVersionRule {
        api_key: ApiKey::AddPartitionsToTxn,
        min_version: 3,
    },
    HeaderVersionRule {
        api_key: ApiKey::AddOffsetsToTxn,
        min_version: 3,
    },
    HeaderVersionRule {
        api_key: ApiKey::EndTxn,
        min_version: 3,
    },
    HeaderVersionRule {
        api_key: ApiKey::TxnOffsetCommit,
        min_version: 3,
    },
    HeaderVersionRule {
        api_key: ApiKey::DescribeConfigs,
        min_version: 4,
    },
    HeaderVersionRule {
        api_key: ApiKey::AlterConfigs,
        min_version: 2,
    },
    HeaderVersionRule {
        api_key: ApiKey::CreatePartitions,
        min_version: 2,
    },
    HeaderVersionRule {
        api_key: ApiKey::DescribeCluster,
        min_version: 0,
    },
    HeaderVersionRule {
        api_key: ApiKey::DeleteRecords,
        min_version: 2,
    },
    HeaderVersionRule {
        api_key: ApiKey::IncrementalAlterConfigs,
        min_version: 0,
    },
    HeaderVersionRule {
        api_key: ApiKey::OffsetForLeaderEpoch,
        min_version: 4,
    },
];

pub(super) const RESPONSE_HEADER_FLEXIBLE_VERSIONS: &[HeaderVersionRule] = &[
    HeaderVersionRule {
        api_key: ApiKey::ControlledShutdown,
        min_version: 3,
    },
    HeaderVersionRule {
        api_key: ApiKey::Metadata,
        min_version: 9,
    },
    HeaderVersionRule {
        api_key: ApiKey::Produce,
        min_version: 9,
    },
    HeaderVersionRule {
        api_key: ApiKey::Fetch,
        min_version: 12,
    },
    HeaderVersionRule {
        api_key: ApiKey::ListOffsets,
        min_version: 6,
    },
    HeaderVersionRule {
        api_key: ApiKey::CreateTopics,
        min_version: 5,
    },
    HeaderVersionRule {
        api_key: ApiKey::DeleteTopics,
        min_version: 4,
    },
    HeaderVersionRule {
        api_key: ApiKey::SaslAuthenticate,
        min_version: 2,
    },
    HeaderVersionRule {
        api_key: ApiKey::FindCoordinator,
        min_version: 3,
    },
    HeaderVersionRule {
        api_key: ApiKey::JoinGroup,
        min_version: 6,
    },
    HeaderVersionRule {
        api_key: ApiKey::SyncGroup,
        min_version: 4,
    },
    HeaderVersionRule {
        api_key: ApiKey::Heartbeat,
        min_version: 4,
    },
    HeaderVersionRule {
        api_key: ApiKey::LeaveGroup,
        min_version: 4,
    },
    HeaderVersionRule {
        api_key: ApiKey::OffsetFetch,
        min_version: 6,
    },
    HeaderVersionRule {
        api_key: ApiKey::OffsetCommit,
        min_version: 8,
    },
    HeaderVersionRule {
        api_key: ApiKey::DescribeGroups,
        min_version: 5,
    },
    HeaderVersionRule {
        api_key: ApiKey::ListGroups,
        min_version: 3,
    },
    HeaderVersionRule {
        api_key: ApiKey::DeleteGroups,
        min_version: 2,
    },
    HeaderVersionRule {
        api_key: ApiKey::DescribeAcls,
        min_version: 2,
    },
    HeaderVersionRule {
        api_key: ApiKey::CreateAcls,
        min_version: 2,
    },
    HeaderVersionRule {
        api_key: ApiKey::DeleteAcls,
        min_version: 2,
    },
    HeaderVersionRule {
        api_key: ApiKey::InitProducerId,
        min_version: 2,
    },
    HeaderVersionRule {
        api_key: ApiKey::AddPartitionsToTxn,
        min_version: 3,
    },
    HeaderVersionRule {
        api_key: ApiKey::AddOffsetsToTxn,
        min_version: 3,
    },
    HeaderVersionRule {
        api_key: ApiKey::EndTxn,
        min_version: 3,
    },
    HeaderVersionRule {
        api_key: ApiKey::TxnOffsetCommit,
        min_version: 3,
    },
    HeaderVersionRule {
        api_key: ApiKey::DescribeConfigs,
        min_version: 4,
    },
    HeaderVersionRule {
        api_key: ApiKey::AlterConfigs,
        min_version: 2,
    },
    HeaderVersionRule {
        api_key: ApiKey::CreatePartitions,
        min_version: 2,
    },
    HeaderVersionRule {
        api_key: ApiKey::DescribeCluster,
        min_version: 0,
    },
    HeaderVersionRule {
        api_key: ApiKey::DeleteRecords,
        min_version: 2,
    },
    HeaderVersionRule {
        api_key: ApiKey::IncrementalAlterConfigs,
        min_version: 0,
    },
    HeaderVersionRule {
        api_key: ApiKey::OffsetForLeaderEpoch,
        min_version: 4,
    },
];

// ============================================================================
// Processed Response with optional cache key for sendfile optimization
// ============================================================================

/// Response from message processing, includes optional cache key for zero-copy optimization.
///
/// When `cache_key` is Some, the response can be cached and sent via sendfile
/// on subsequent requests with the same parameters.
pub(super) struct ProcessedResponse {
    /// Serialized response data
    pub(super) data: Vec<u8>,
    /// Optional cache key for fetch responses (enables sendfile optimization)
    pub(super) cache_key: Option<CacheKey>,
}

// ============================================================================
// Supported API Versions
// ============================================================================
// Format: (api_key, min_version, max_version)
// This declarative array makes it easy to audit and update supported versions.
#[rustfmt::skip]
pub(crate) const SUPPORTED_API_VERSIONS: &[(i16, i16, i16)] = &[
    // Core protocol
    (0,  0, 9),   // Produce (v9: flexible versions, improved acks)
    (1,  0, 15),  // Fetch (v13+ uses topic_id, v15: rack-aware fetch)
    (2,  0, 7),   // ListOffsets
    (3,  0, 12),  // Metadata
    (7,  0, 3),   // ControlledShutdown
    (18, 0, 3),   // ApiVersions

    // Topic management
    (19, 0, 7),   // CreateTopics
    (20, 0, 6),   // DeleteTopics
    (37, 0, 3),   // CreatePartitions
    (32, 0, 4),   // DescribeConfigs
    (33, 0, 2),   // AlterConfigs
    (44, 0, 1),   // IncrementalAlterConfigs

    // Consumer groups (classic protocol)
    (8,  0, 8),   // OffsetCommit
    (9,  0, 8),   // OffsetFetch
    (10, 0, 4),   // FindCoordinator
    (11, 0, 9),   // JoinGroup
    (12, 0, 4),   // Heartbeat
    (13, 0, 5),   // LeaveGroup
    (14, 0, 5),   // SyncGroup
    (15, 0, 5),   // DescribeGroups
    (16, 0, 4),   // ListGroups
    (42, 0, 2),   // DeleteGroups

    // SASL authentication
    (17, 0, 2),   // SaslHandshake
    (36, 0, 2),   // SaslAuthenticate

    // ACL management
    (29, 0, 3),   // DescribeAcls
    (30, 0, 3),   // CreateAcls
    (31, 0, 3),   // DeleteAcls

    // Transactions
    (22, 0, 5),   // InitProducerId (v5: transaction improvements)
    (24, 0, 4),   // AddPartitionsToTxn
    (25, 0, 3),   // AddOffsetsToTxn
    (26, 0, 3),   // EndTxn
    (28, 0, 3),   // TxnOffsetCommit
    (27, 0, 1),   // WriteTxnMarkers

    // Cluster management
    (60, 0, 1),   // DescribeCluster
    (23, 0, 4),   // OffsetForLeaderEpoch
    (21, 0, 2),   // DeleteRecords
    (35, 0, 4),   // DescribeLogDirs
    (34, 0, 2),   // AlterReplicaLogDirs
    (46, 0, 0),   // ListPartitionReassignments
    (45, 0, 0),   // AlterPartitionReassignments
    (43, 0, 2),   // ElectLeaders

    // Offset management
    (47, 0, 0),   // OffsetDelete

    // Producer/transaction introspection
    (61, 0, 0),   // DescribeProducers
    (65, 0, 0),   // DescribeTransactions
    (66, 0, 0),   // ListTransactions

    // Feature management
    (57, 0, 1),   // UpdateFeatures

    // Broker management
    (64, 0, 0),   // UnregisterBroker

    // KIP-848 Consumer Groups (next-gen)
    (68, 0, 0),   // ConsumerGroupHeartbeat
    (69, 0, 1),   // ConsumerGroupDescribe (v1: KIP-848 updates)

    // Delegation tokens
    (38, 0, 3),   // CreateDelegationToken
    (39, 0, 2),   // RenewDelegationToken
    (40, 0, 2),   // ExpireDelegationToken
    (41, 0, 3),   // DescribeDelegationToken

    // Client quotas
    (48, 0, 1),   // DescribeClientQuotas
    (49, 0, 1),   // AlterClientQuotas

    // SCRAM credentials
    (50, 0, 0),   // DescribeUserScramCredentials
    (51, 0, 0),   // AlterUserScramCredentials

    // Quorum info
    (55, 0, 1),   // DescribeQuorum
];

/// Build API version list from the const array.
/// This is more maintainable than manually creating each ApiVersion object.
pub(super) fn build_api_versions(
) -> Vec<kafka_protocol::messages::api_versions_response::ApiVersion> {
    use kafka_protocol::messages::api_versions_response::ApiVersion;

    SUPPORTED_API_VERSIONS
        .iter()
        .map(|&(api_key, min_version, max_version)| {
            ApiVersion::default()
                .with_api_key(api_key)
                .with_min_version(min_version)
                .with_max_version(max_version)
        })
        .collect()
}
