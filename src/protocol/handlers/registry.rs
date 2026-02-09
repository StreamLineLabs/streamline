use bytes::Bytes;
use kafka_protocol::messages::{ApiKey, RequestHeader, ResponseHeader};

use crate::error::{ProtocolError, Result, StreamlineError};
use crate::protocol::kafka::{KafkaHandler, SessionManager};
use crate::protocol::response_cache::CacheKey;
use tracing::warn;

use super::{admin, configs, core, data_plane, groups, security, topics, transactions};

pub(crate) struct DispatchResult {
    pub(crate) response_header: ResponseHeader,
    pub(crate) response_body: Vec<u8>,
    pub(crate) effective_api_version: i16,
    pub(crate) cache_key: Option<CacheKey>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum HandlerGroup {
    Core,
    DataPlane,
    Topics,
    Groups,
    Configs,
    Security,
    Transactions,
    Admin,
}

pub(crate) struct HandlerRegistration {
    pub(crate) api_key: ApiKey,
    pub(crate) group: HandlerGroup,
}

pub(crate) const HANDLER_REGISTRY: &[HandlerRegistration] = &[
    HandlerRegistration {
        api_key: ApiKey::ApiVersions,
        group: HandlerGroup::Core,
    },
    HandlerRegistration {
        api_key: ApiKey::Metadata,
        group: HandlerGroup::Core,
    },
    HandlerRegistration {
        api_key: ApiKey::ControlledShutdown,
        group: HandlerGroup::Core,
    },
    HandlerRegistration {
        api_key: ApiKey::Produce,
        group: HandlerGroup::DataPlane,
    },
    HandlerRegistration {
        api_key: ApiKey::Fetch,
        group: HandlerGroup::DataPlane,
    },
    HandlerRegistration {
        api_key: ApiKey::ListOffsets,
        group: HandlerGroup::DataPlane,
    },
    HandlerRegistration {
        api_key: ApiKey::CreateTopics,
        group: HandlerGroup::Topics,
    },
    HandlerRegistration {
        api_key: ApiKey::DeleteTopics,
        group: HandlerGroup::Topics,
    },
    HandlerRegistration {
        api_key: ApiKey::CreatePartitions,
        group: HandlerGroup::Topics,
    },
    HandlerRegistration {
        api_key: ApiKey::FindCoordinator,
        group: HandlerGroup::Groups,
    },
    HandlerRegistration {
        api_key: ApiKey::JoinGroup,
        group: HandlerGroup::Groups,
    },
    HandlerRegistration {
        api_key: ApiKey::Heartbeat,
        group: HandlerGroup::Groups,
    },
    HandlerRegistration {
        api_key: ApiKey::LeaveGroup,
        group: HandlerGroup::Groups,
    },
    HandlerRegistration {
        api_key: ApiKey::SyncGroup,
        group: HandlerGroup::Groups,
    },
    HandlerRegistration {
        api_key: ApiKey::OffsetCommit,
        group: HandlerGroup::Groups,
    },
    HandlerRegistration {
        api_key: ApiKey::OffsetFetch,
        group: HandlerGroup::Groups,
    },
    HandlerRegistration {
        api_key: ApiKey::DescribeGroups,
        group: HandlerGroup::Groups,
    },
    HandlerRegistration {
        api_key: ApiKey::ListGroups,
        group: HandlerGroup::Groups,
    },
    HandlerRegistration {
        api_key: ApiKey::DeleteGroups,
        group: HandlerGroup::Groups,
    },
    HandlerRegistration {
        api_key: ApiKey::OffsetDelete,
        group: HandlerGroup::Groups,
    },
    HandlerRegistration {
        api_key: ApiKey::ConsumerGroupHeartbeat,
        group: HandlerGroup::Groups,
    },
    HandlerRegistration {
        api_key: ApiKey::ConsumerGroupDescribe,
        group: HandlerGroup::Groups,
    },
    HandlerRegistration {
        api_key: ApiKey::DescribeConfigs,
        group: HandlerGroup::Configs,
    },
    HandlerRegistration {
        api_key: ApiKey::AlterConfigs,
        group: HandlerGroup::Configs,
    },
    HandlerRegistration {
        api_key: ApiKey::IncrementalAlterConfigs,
        group: HandlerGroup::Configs,
    },
    HandlerRegistration {
        api_key: ApiKey::SaslHandshake,
        group: HandlerGroup::Security,
    },
    HandlerRegistration {
        api_key: ApiKey::SaslAuthenticate,
        group: HandlerGroup::Security,
    },
    HandlerRegistration {
        api_key: ApiKey::DescribeAcls,
        group: HandlerGroup::Security,
    },
    HandlerRegistration {
        api_key: ApiKey::CreateAcls,
        group: HandlerGroup::Security,
    },
    HandlerRegistration {
        api_key: ApiKey::DeleteAcls,
        group: HandlerGroup::Security,
    },
    HandlerRegistration {
        api_key: ApiKey::CreateDelegationToken,
        group: HandlerGroup::Security,
    },
    HandlerRegistration {
        api_key: ApiKey::RenewDelegationToken,
        group: HandlerGroup::Security,
    },
    HandlerRegistration {
        api_key: ApiKey::ExpireDelegationToken,
        group: HandlerGroup::Security,
    },
    HandlerRegistration {
        api_key: ApiKey::DescribeDelegationToken,
        group: HandlerGroup::Security,
    },
    HandlerRegistration {
        api_key: ApiKey::DescribeUserScramCredentials,
        group: HandlerGroup::Security,
    },
    HandlerRegistration {
        api_key: ApiKey::AlterUserScramCredentials,
        group: HandlerGroup::Security,
    },
    HandlerRegistration {
        api_key: ApiKey::InitProducerId,
        group: HandlerGroup::Transactions,
    },
    HandlerRegistration {
        api_key: ApiKey::AddPartitionsToTxn,
        group: HandlerGroup::Transactions,
    },
    HandlerRegistration {
        api_key: ApiKey::AddOffsetsToTxn,
        group: HandlerGroup::Transactions,
    },
    HandlerRegistration {
        api_key: ApiKey::EndTxn,
        group: HandlerGroup::Transactions,
    },
    HandlerRegistration {
        api_key: ApiKey::TxnOffsetCommit,
        group: HandlerGroup::Transactions,
    },
    HandlerRegistration {
        api_key: ApiKey::WriteTxnMarkers,
        group: HandlerGroup::Transactions,
    },
    HandlerRegistration {
        api_key: ApiKey::DescribeProducers,
        group: HandlerGroup::Transactions,
    },
    HandlerRegistration {
        api_key: ApiKey::DescribeTransactions,
        group: HandlerGroup::Transactions,
    },
    HandlerRegistration {
        api_key: ApiKey::ListTransactions,
        group: HandlerGroup::Transactions,
    },
    HandlerRegistration {
        api_key: ApiKey::DescribeCluster,
        group: HandlerGroup::Admin,
    },
    HandlerRegistration {
        api_key: ApiKey::OffsetForLeaderEpoch,
        group: HandlerGroup::Admin,
    },
    HandlerRegistration {
        api_key: ApiKey::DeleteRecords,
        group: HandlerGroup::Admin,
    },
    HandlerRegistration {
        api_key: ApiKey::DescribeLogDirs,
        group: HandlerGroup::Admin,
    },
    HandlerRegistration {
        api_key: ApiKey::AlterReplicaLogDirs,
        group: HandlerGroup::Admin,
    },
    HandlerRegistration {
        api_key: ApiKey::ListPartitionReassignments,
        group: HandlerGroup::Admin,
    },
    HandlerRegistration {
        api_key: ApiKey::AlterPartitionReassignments,
        group: HandlerGroup::Admin,
    },
    HandlerRegistration {
        api_key: ApiKey::ElectLeaders,
        group: HandlerGroup::Admin,
    },
    HandlerRegistration {
        api_key: ApiKey::DescribeClientQuotas,
        group: HandlerGroup::Admin,
    },
    HandlerRegistration {
        api_key: ApiKey::AlterClientQuotas,
        group: HandlerGroup::Admin,
    },
    HandlerRegistration {
        api_key: ApiKey::UpdateFeatures,
        group: HandlerGroup::Admin,
    },
    HandlerRegistration {
        api_key: ApiKey::UnregisterBroker,
        group: HandlerGroup::Admin,
    },
    HandlerRegistration {
        api_key: ApiKey::DescribeQuorum,
        group: HandlerGroup::Admin,
    },
];

#[tracing::instrument(skip(handler, buf, session_manager), fields(api_key = ?api_key, correlation_id = header.correlation_id, principal, host))]
pub(crate) async fn dispatch_request(
    handler: &KafkaHandler,
    api_key: ApiKey,
    header: &RequestHeader,
    buf: Bytes,
    session_manager: &SessionManager,
    principal: &str,
    host: &str,
) -> Result<DispatchResult> {
    let registration = match HANDLER_REGISTRY
        .iter()
        .find(|entry| entry.api_key == api_key)
    {
        Some(entry) => entry,
        None => {
            warn!(api_key = ?api_key, "Unsupported API");
            return Err(StreamlineError::ProtocolDomain(
                ProtocolError::unsupported_api_key(format!("{:?}", api_key)),
            ));
        }
    };

    match registration.group {
        HandlerGroup::Core => {
            core::dispatch(
                handler,
                api_key,
                header,
                buf,
                session_manager,
                principal,
                host,
            )
            .await
        }
        HandlerGroup::DataPlane => {
            data_plane::dispatch(
                handler,
                api_key,
                header,
                buf,
                session_manager,
                principal,
                host,
            )
            .await
        }
        HandlerGroup::Topics => {
            topics::dispatch(
                handler,
                api_key,
                header,
                buf,
                session_manager,
                principal,
                host,
            )
            .await
        }
        HandlerGroup::Groups => {
            groups::dispatch(
                handler,
                api_key,
                header,
                buf,
                session_manager,
                principal,
                host,
            )
            .await
        }
        HandlerGroup::Configs => {
            configs::dispatch(
                handler,
                api_key,
                header,
                buf,
                session_manager,
                principal,
                host,
            )
            .await
        }
        HandlerGroup::Security => {
            security::dispatch(
                handler,
                api_key,
                header,
                buf,
                session_manager,
                principal,
                host,
            )
            .await
        }
        HandlerGroup::Transactions => {
            transactions::dispatch(
                handler,
                api_key,
                header,
                buf,
                session_manager,
                principal,
                host,
            )
            .await
        }
        HandlerGroup::Admin => {
            admin::dispatch(
                handler,
                api_key,
                header,
                buf,
                session_manager,
                principal,
                host,
            )
            .await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::kafka::SUPPORTED_API_VERSIONS;
    use std::collections::HashSet;

    #[test]
    fn registry_covers_supported_api_versions() {
        let supported: HashSet<i16> = SUPPORTED_API_VERSIONS
            .iter()
            .map(|(api_key, _, _)| *api_key)
            .collect();
        let registered: HashSet<i16> = HANDLER_REGISTRY
            .iter()
            .map(|entry| entry.api_key as i16)
            .collect();

        for api_key in supported {
            assert!(
                registered.contains(&api_key),
                "Missing handler registration for api key {}",
                api_key
            );
        }
    }

    #[test]
    fn registry_has_no_duplicates() {
        let mut seen = HashSet::new();
        for entry in HANDLER_REGISTRY {
            assert!(
                seen.insert(entry.api_key as i16),
                "Duplicate handler registration for {:?}",
                entry.api_key
            );
        }
    }
}
