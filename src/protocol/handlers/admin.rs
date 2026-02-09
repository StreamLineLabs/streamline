use bytes::Bytes;
use kafka_protocol::messages::{
    AlterClientQuotasRequest, AlterPartitionReassignmentsRequest, AlterReplicaLogDirsRequest,
    ApiKey, DeleteRecordsRequest, DescribeClientQuotasRequest, DescribeClusterRequest,
    DescribeLogDirsRequest, DescribeQuorumRequest, ElectLeadersRequest,
    ListPartitionReassignmentsRequest, OffsetForLeaderEpochRequest, RequestHeader,
    UnregisterBrokerRequest, UpdateFeaturesRequest,
};
use kafka_protocol::protocol::Decodable;

use crate::error::{Result, StreamlineError};
use crate::protocol::kafka::{KafkaHandler, Operation, ResourceType, SessionManager};

use super::registry::DispatchResult;

#[tracing::instrument(level = "info", skip(handler, buf, _session_manager), fields(api_key = ?api_key, correlation_id = header.correlation_id))]
pub(crate) async fn dispatch(
    handler: &KafkaHandler,
    api_key: ApiKey,
    header: &RequestHeader,
    mut buf: Bytes,
    _session_manager: &SessionManager,
    principal: &str,
    host: &str,
) -> Result<DispatchResult> {
    match api_key {
        ApiKey::DescribeCluster => {
            let request = DescribeClusterRequest::decode(&mut buf, header.request_api_version)
                .map_err(|e| {
                    StreamlineError::protocol("decode", format!("Failed to decode request: {}", e))
                })?;

            handler
                .check_authorization(
                    principal,
                    host,
                    Operation::Describe,
                    ResourceType::Cluster,
                    "kafka-cluster",
                )
                .await?;

            let response = handler.handle_describe_cluster(request).await?;
            let response_body = handler.encode_response(&response, header.request_api_version)?;
            Ok(DispatchResult {
                response_header: handler.create_response_header(header.correlation_id),
                response_body,
                effective_api_version: header.request_api_version,
                cache_key: None,
            })
        }
        ApiKey::OffsetForLeaderEpoch => {
            let request = OffsetForLeaderEpochRequest::decode(&mut buf, header.request_api_version)
                .map_err(|e| {
                    StreamlineError::protocol("decode", format!("Failed to decode request: {}", e))
                })?;

            let response = handler.handle_offset_for_leader_epoch(request).await?;
            let response_body = handler.encode_response(&response, header.request_api_version)?;
            Ok(DispatchResult {
                response_header: handler.create_response_header(header.correlation_id),
                response_body,
                effective_api_version: header.request_api_version,
                cache_key: None,
            })
        }
        ApiKey::DeleteRecords => {
            let request = DeleteRecordsRequest::decode(&mut buf, header.request_api_version)
                .map_err(|e| {
                    StreamlineError::protocol("decode", format!("Failed to decode request: {}", e))
                })?;

            for topic in &request.topics {
                handler
                    .check_authorization(
                        principal,
                        host,
                        Operation::Delete,
                        ResourceType::Topic,
                        topic.name.as_str(),
                    )
                    .await?;
            }

            let response = handler.handle_delete_records(request).await?;
            let response_body = handler.encode_response(&response, header.request_api_version)?;
            Ok(DispatchResult {
                response_header: handler.create_response_header(header.correlation_id),
                response_body,
                effective_api_version: header.request_api_version,
                cache_key: None,
            })
        }
        ApiKey::DescribeLogDirs => {
            let request = DescribeLogDirsRequest::decode(&mut buf, header.request_api_version)
                .map_err(|e| {
                    StreamlineError::protocol("decode", format!("Failed to decode request: {}", e))
                })?;

            let response = handler.handle_describe_log_dirs(request)?;
            let response_body = handler.encode_response(&response, header.request_api_version)?;
            Ok(DispatchResult {
                response_header: handler.create_response_header(header.correlation_id),
                response_body,
                effective_api_version: header.request_api_version,
                cache_key: None,
            })
        }
        ApiKey::AlterReplicaLogDirs => {
            let request = AlterReplicaLogDirsRequest::decode(&mut buf, header.request_api_version)
                .map_err(|e| {
                    StreamlineError::protocol("decode", format!("Failed to decode request: {}", e))
                })?;

            let response = handler.handle_alter_replica_log_dirs(request)?;
            let response_body = handler.encode_response(&response, header.request_api_version)?;
            Ok(DispatchResult {
                response_header: handler.create_response_header(header.correlation_id),
                response_body,
                effective_api_version: header.request_api_version,
                cache_key: None,
            })
        }
        ApiKey::ListPartitionReassignments => {
            let request =
                ListPartitionReassignmentsRequest::decode(&mut buf, header.request_api_version)
                    .map_err(|e| {
                        StreamlineError::protocol(
                            "decode",
                            format!("Failed to decode request: {}", e),
                        )
                    })?;

            let response = handler.handle_list_partition_reassignments(request)?;
            let response_body = handler.encode_response(&response, header.request_api_version)?;
            Ok(DispatchResult {
                response_header: handler.create_response_header(header.correlation_id),
                response_body,
                effective_api_version: header.request_api_version,
                cache_key: None,
            })
        }
        ApiKey::AlterPartitionReassignments => {
            let request =
                AlterPartitionReassignmentsRequest::decode(&mut buf, header.request_api_version)
                    .map_err(|e| {
                        StreamlineError::protocol(
                            "decode",
                            format!("Failed to decode request: {}", e),
                        )
                    })?;

            let response = handler.handle_alter_partition_reassignments(request)?;
            let response_body = handler.encode_response(&response, header.request_api_version)?;
            Ok(DispatchResult {
                response_header: handler.create_response_header(header.correlation_id),
                response_body,
                effective_api_version: header.request_api_version,
                cache_key: None,
            })
        }
        ApiKey::ElectLeaders => {
            let request = ElectLeadersRequest::decode(&mut buf, header.request_api_version)
                .map_err(|e| {
                    StreamlineError::protocol("decode", format!("Failed to decode request: {}", e))
                })?;

            let response = handler.handle_elect_leaders(request)?;
            let response_body = handler.encode_response(&response, header.request_api_version)?;
            Ok(DispatchResult {
                response_header: handler.create_response_header(header.correlation_id),
                response_body,
                effective_api_version: header.request_api_version,
                cache_key: None,
            })
        }
        ApiKey::DescribeClientQuotas => {
            let request = DescribeClientQuotasRequest::decode(&mut buf, header.request_api_version)
                .map_err(|e| {
                    StreamlineError::protocol("decode", format!("Failed to decode request: {}", e))
                })?;

            let response = handler.handle_describe_client_quotas(request)?;
            let response_body = handler.encode_response(&response, header.request_api_version)?;
            Ok(DispatchResult {
                response_header: handler.create_response_header(header.correlation_id),
                response_body,
                effective_api_version: header.request_api_version,
                cache_key: None,
            })
        }
        ApiKey::AlterClientQuotas => {
            let request = AlterClientQuotasRequest::decode(&mut buf, header.request_api_version)
                .map_err(|e| {
                    StreamlineError::protocol("decode", format!("Failed to decode request: {}", e))
                })?;

            let response = handler.handle_alter_client_quotas(request)?;
            let response_body = handler.encode_response(&response, header.request_api_version)?;
            Ok(DispatchResult {
                response_header: handler.create_response_header(header.correlation_id),
                response_body,
                effective_api_version: header.request_api_version,
                cache_key: None,
            })
        }
        ApiKey::UpdateFeatures => {
            let request = UpdateFeaturesRequest::decode(&mut buf, header.request_api_version)
                .map_err(|e| {
                    StreamlineError::protocol("decode", format!("Failed to decode request: {}", e))
                })?;

            let response = handler.handle_update_features(request)?;
            let response_body = handler.encode_response(&response, header.request_api_version)?;
            Ok(DispatchResult {
                response_header: handler.create_response_header(header.correlation_id),
                response_body,
                effective_api_version: header.request_api_version,
                cache_key: None,
            })
        }
        ApiKey::UnregisterBroker => {
            let request = UnregisterBrokerRequest::decode(&mut buf, header.request_api_version)
                .map_err(|e| {
                    StreamlineError::protocol("decode", format!("Failed to decode request: {}", e))
                })?;

            let response = handler.handle_unregister_broker(request)?;
            let response_body = handler.encode_response(&response, header.request_api_version)?;
            Ok(DispatchResult {
                response_header: handler.create_response_header(header.correlation_id),
                response_body,
                effective_api_version: header.request_api_version,
                cache_key: None,
            })
        }
        ApiKey::DescribeQuorum => {
            let request = DescribeQuorumRequest::decode(&mut buf, header.request_api_version)
                .map_err(|e| {
                    StreamlineError::protocol("decode", format!("Failed to decode request: {}", e))
                })?;

            let response = handler.handle_describe_quorum(request)?;
            let response_body = handler.encode_response(&response, header.request_api_version)?;
            Ok(DispatchResult {
                response_header: handler.create_response_header(header.correlation_id),
                response_body,
                effective_api_version: header.request_api_version,
                cache_key: None,
            })
        }
        _ => Err(StreamlineError::protocol(
            "unsupported API key",
            format!("{:?}", api_key),
        )),
    }
}
