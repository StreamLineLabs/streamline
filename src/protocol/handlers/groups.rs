use bytes::Bytes;
use kafka_protocol::messages::{
    ApiKey, ConsumerGroupDescribeRequest, ConsumerGroupHeartbeatRequest, DeleteGroupsRequest,
    DescribeGroupsRequest, FindCoordinatorRequest, HeartbeatRequest, JoinGroupRequest,
    LeaveGroupRequest, ListGroupsRequest, OffsetCommitRequest, OffsetDeleteRequest,
    OffsetFetchRequest, RequestHeader, SyncGroupRequest,
};
use kafka_protocol::protocol::Decodable;

use crate::error::{Result, StreamlineError};
use crate::protocol::kafka::{KafkaHandler, Operation, ResourceType, SessionManager};

use super::registry::DispatchResult;

#[tracing::instrument(level = "debug", skip(handler, buf, _session_manager), fields(api_key = ?api_key, correlation_id = header.correlation_id))]
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
        ApiKey::FindCoordinator => {
            let request = FindCoordinatorRequest::decode(&mut buf, header.request_api_version)
                .map_err(|e| {
                    StreamlineError::protocol("decode", format!("Failed to decode request: {}", e))
                })?;
            let response = handler.handle_find_coordinator(request, header.request_api_version)?;
            let response_body = handler.encode_response(&response, header.request_api_version)?;
            Ok(DispatchResult {
                response_header: handler.create_response_header(header.correlation_id),
                response_body,
                effective_api_version: header.request_api_version,
                cache_key: None,
            })
        }
        ApiKey::JoinGroup => {
            let request =
                JoinGroupRequest::decode(&mut buf, header.request_api_version).map_err(|e| {
                    StreamlineError::protocol("decode", format!("Failed to decode request: {}", e))
                })?;

            handler
                .check_group_authorization(
                    principal,
                    host,
                    request.group_id.as_str(),
                    Operation::Read,
                )
                .await?;

            let response = handler.handle_join_group(request, header.request_api_version)?;
            let response_body = handler.encode_response(&response, header.request_api_version)?;
            Ok(DispatchResult {
                response_header: handler.create_response_header(header.correlation_id),
                response_body,
                effective_api_version: header.request_api_version,
                cache_key: None,
            })
        }
        ApiKey::Heartbeat => {
            let request =
                HeartbeatRequest::decode(&mut buf, header.request_api_version).map_err(|e| {
                    StreamlineError::protocol("decode", format!("Failed to decode request: {}", e))
                })?;

            handler
                .check_group_authorization(
                    principal,
                    host,
                    request.group_id.as_str(),
                    Operation::Read,
                )
                .await?;

            let response = handler.handle_heartbeat(request, header.request_api_version)?;
            let response_body = handler.encode_response(&response, header.request_api_version)?;
            Ok(DispatchResult {
                response_header: handler.create_response_header(header.correlation_id),
                response_body,
                effective_api_version: header.request_api_version,
                cache_key: None,
            })
        }
        ApiKey::LeaveGroup => {
            let request =
                LeaveGroupRequest::decode(&mut buf, header.request_api_version).map_err(|e| {
                    StreamlineError::protocol("decode", format!("Failed to decode request: {}", e))
                })?;

            handler
                .check_group_authorization(
                    principal,
                    host,
                    request.group_id.as_str(),
                    Operation::Read,
                )
                .await?;

            let response = handler.handle_leave_group(request, header.request_api_version)?;
            let response_body = handler.encode_response(&response, header.request_api_version)?;
            Ok(DispatchResult {
                response_header: handler.create_response_header(header.correlation_id),
                response_body,
                effective_api_version: header.request_api_version,
                cache_key: None,
            })
        }
        ApiKey::SyncGroup => {
            let request =
                SyncGroupRequest::decode(&mut buf, header.request_api_version).map_err(|e| {
                    StreamlineError::protocol("decode", format!("Failed to decode request: {}", e))
                })?;

            handler
                .check_group_authorization(
                    principal,
                    host,
                    request.group_id.as_str(),
                    Operation::Read,
                )
                .await?;

            let response = handler.handle_sync_group(request, header.request_api_version)?;
            let response_body = handler.encode_response(&response, header.request_api_version)?;
            Ok(DispatchResult {
                response_header: handler.create_response_header(header.correlation_id),
                response_body,
                effective_api_version: header.request_api_version,
                cache_key: None,
            })
        }
        ApiKey::OffsetCommit => {
            let request = OffsetCommitRequest::decode(&mut buf, header.request_api_version)
                .map_err(|e| {
                    StreamlineError::protocol("decode", format!("Failed to decode request: {}", e))
                })?;

            handler
                .check_group_authorization(
                    principal,
                    host,
                    request.group_id.as_str(),
                    Operation::Read,
                )
                .await?;

            let response = handler.handle_offset_commit(request, header.request_api_version)?;
            let response_body = handler.encode_response(&response, header.request_api_version)?;
            Ok(DispatchResult {
                response_header: handler.create_response_header(header.correlation_id),
                response_body,
                effective_api_version: header.request_api_version,
                cache_key: None,
            })
        }
        ApiKey::OffsetFetch => {
            let request = OffsetFetchRequest::decode(&mut buf, header.request_api_version)
                .map_err(|e| {
                    StreamlineError::protocol("decode", format!("Failed to decode request: {}", e))
                })?;

            handler
                .check_group_authorization(
                    principal,
                    host,
                    request.group_id.as_str(),
                    Operation::Describe,
                )
                .await?;

            let response = handler.handle_offset_fetch(request, header.request_api_version)?;
            let response_body = handler.encode_response(&response, header.request_api_version)?;
            Ok(DispatchResult {
                response_header: handler.create_response_header(header.correlation_id),
                response_body,
                effective_api_version: header.request_api_version,
                cache_key: None,
            })
        }
        ApiKey::DescribeGroups => {
            let request = DescribeGroupsRequest::decode(&mut buf, header.request_api_version)
                .map_err(|e| {
                    StreamlineError::protocol("decode", format!("Failed to decode request: {}", e))
                })?;

            for group_id in request.groups.iter() {
                handler
                    .check_group_authorization(
                        principal,
                        host,
                        group_id.as_str(),
                        Operation::Describe,
                    )
                    .await?;
            }

            let response = handler.handle_describe_groups(request, header.request_api_version)?;
            let response_body = handler.encode_response(&response, header.request_api_version)?;
            Ok(DispatchResult {
                response_header: handler.create_response_header(header.correlation_id),
                response_body,
                effective_api_version: header.request_api_version,
                cache_key: None,
            })
        }
        ApiKey::ListGroups => {
            let request =
                ListGroupsRequest::decode(&mut buf, header.request_api_version).map_err(|e| {
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

            let response = handler.handle_list_groups(request, header.request_api_version)?;
            let response_body = handler.encode_response(&response, header.request_api_version)?;
            Ok(DispatchResult {
                response_header: handler.create_response_header(header.correlation_id),
                response_body,
                effective_api_version: header.request_api_version,
                cache_key: None,
            })
        }
        ApiKey::DeleteGroups => {
            let request = DeleteGroupsRequest::decode(&mut buf, header.request_api_version)
                .map_err(|e| {
                    StreamlineError::protocol("decode", format!("Failed to decode request: {}", e))
                })?;

            for group_id in request.groups_names.iter() {
                handler
                    .check_group_authorization(
                        principal,
                        host,
                        group_id.as_str(),
                        Operation::Delete,
                    )
                    .await?;
            }

            let response = handler.handle_delete_groups(request)?;
            let response_body = handler.encode_response(&response, header.request_api_version)?;
            Ok(DispatchResult {
                response_header: handler.create_response_header(header.correlation_id),
                response_body,
                effective_api_version: header.request_api_version,
                cache_key: None,
            })
        }
        ApiKey::OffsetDelete => {
            let request = OffsetDeleteRequest::decode(&mut buf, header.request_api_version)
                .map_err(|e| {
                    StreamlineError::protocol("decode", format!("Failed to decode request: {}", e))
                })?;

            let response = handler.handle_offset_delete(request)?;
            let response_body = handler.encode_response(&response, header.request_api_version)?;
            Ok(DispatchResult {
                response_header: handler.create_response_header(header.correlation_id),
                response_body,
                effective_api_version: header.request_api_version,
                cache_key: None,
            })
        }
        ApiKey::ConsumerGroupHeartbeat => {
            let request =
                ConsumerGroupHeartbeatRequest::decode(&mut buf, header.request_api_version)
                    .map_err(|e| {
                        StreamlineError::protocol(
                            "decode",
                            format!("Failed to decode request: {}", e),
                        )
                    })?;

            let response = handler.handle_consumer_group_heartbeat(request)?;
            let response_body = handler.encode_response(&response, header.request_api_version)?;
            Ok(DispatchResult {
                response_header: handler.create_response_header(header.correlation_id),
                response_body,
                effective_api_version: header.request_api_version,
                cache_key: None,
            })
        }
        ApiKey::ConsumerGroupDescribe => {
            let request =
                ConsumerGroupDescribeRequest::decode(&mut buf, header.request_api_version)
                    .map_err(|e| {
                        StreamlineError::protocol(
                            "decode",
                            format!("Failed to decode request: {}", e),
                        )
                    })?;

            let response = handler.handle_consumer_group_describe(request)?;
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
