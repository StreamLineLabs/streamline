use bytes::Bytes;
use kafka_protocol::messages::{
    AddOffsetsToTxnRequest, AddPartitionsToTxnRequest, ApiKey, DescribeProducersRequest,
    DescribeTransactionsRequest, EndTxnRequest, InitProducerIdRequest, ListTransactionsRequest,
    RequestHeader, TxnOffsetCommitRequest, WriteTxnMarkersRequest,
};
use kafka_protocol::protocol::Decodable;

use crate::error::{Result, StreamlineError};
use crate::protocol::kafka::{KafkaHandler, Operation, ResourceType, SessionManager};

use super::registry::DispatchResult;

#[tracing::instrument(skip(handler, buf, _session_manager), fields(api_key = ?api_key, correlation_id = header.correlation_id))]
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
        ApiKey::InitProducerId => {
            let request = InitProducerIdRequest::decode(&mut buf, header.request_api_version)
                .map_err(|e| {
                    StreamlineError::protocol("decode", format!("Failed to decode request: {}", e))
                })?;

            handler
                .check_authorization(
                    principal,
                    host,
                    Operation::IdempotentWrite,
                    ResourceType::Cluster,
                    "kafka-cluster",
                )
                .await?;

            let response = handler.handle_init_producer_id(request)?;
            let response_body = handler.encode_response(&response, header.request_api_version)?;
            Ok(DispatchResult {
                response_header: handler.create_response_header(header.correlation_id),
                response_body,
                effective_api_version: header.request_api_version,
                cache_key: None,
            })
        }
        ApiKey::AddPartitionsToTxn => {
            let request = AddPartitionsToTxnRequest::decode(&mut buf, header.request_api_version)
                .map_err(|e| {
                StreamlineError::protocol("decode", format!("Failed to decode request: {}", e))
            })?;

            handler
                .check_authorization(
                    principal,
                    host,
                    Operation::Write,
                    ResourceType::TransactionalId,
                    request.v3_and_below_transactional_id.as_str(),
                )
                .await?;

            let response = handler.handle_add_partitions_to_txn(request)?;
            let response_body = handler.encode_response(&response, header.request_api_version)?;
            Ok(DispatchResult {
                response_header: handler.create_response_header(header.correlation_id),
                response_body,
                effective_api_version: header.request_api_version,
                cache_key: None,
            })
        }
        ApiKey::AddOffsetsToTxn => {
            let request = AddOffsetsToTxnRequest::decode(&mut buf, header.request_api_version)
                .map_err(|e| {
                    StreamlineError::protocol("decode", format!("Failed to decode request: {}", e))
                })?;

            handler
                .check_authorization(
                    principal,
                    host,
                    Operation::Write,
                    ResourceType::TransactionalId,
                    request.transactional_id.as_str(),
                )
                .await?;

            let response = handler.handle_add_offsets_to_txn(request)?;
            let response_body = handler.encode_response(&response, header.request_api_version)?;
            Ok(DispatchResult {
                response_header: handler.create_response_header(header.correlation_id),
                response_body,
                effective_api_version: header.request_api_version,
                cache_key: None,
            })
        }
        ApiKey::EndTxn => {
            let request =
                EndTxnRequest::decode(&mut buf, header.request_api_version).map_err(|e| {
                    StreamlineError::protocol("decode", format!("Failed to decode request: {}", e))
                })?;

            handler
                .check_authorization(
                    principal,
                    host,
                    Operation::Write,
                    ResourceType::TransactionalId,
                    request.transactional_id.as_str(),
                )
                .await?;

            let response = handler.handle_end_txn(request)?;
            let response_body = handler.encode_response(&response, header.request_api_version)?;
            Ok(DispatchResult {
                response_header: handler.create_response_header(header.correlation_id),
                response_body,
                effective_api_version: header.request_api_version,
                cache_key: None,
            })
        }
        ApiKey::TxnOffsetCommit => {
            let request = TxnOffsetCommitRequest::decode(&mut buf, header.request_api_version)
                .map_err(|e| {
                    StreamlineError::protocol("decode", format!("Failed to decode request: {}", e))
                })?;

            handler
                .check_authorization(
                    principal,
                    host,
                    Operation::Write,
                    ResourceType::TransactionalId,
                    request.transactional_id.as_str(),
                )
                .await?;

            let response = handler.handle_txn_offset_commit(request)?;
            let response_body = handler.encode_response(&response, header.request_api_version)?;
            Ok(DispatchResult {
                response_header: handler.create_response_header(header.correlation_id),
                response_body,
                effective_api_version: header.request_api_version,
                cache_key: None,
            })
        }
        ApiKey::WriteTxnMarkers => {
            let request = WriteTxnMarkersRequest::decode(&mut buf, header.request_api_version)
                .map_err(|e| {
                    StreamlineError::protocol("decode", format!("Failed to decode request: {}", e))
                })?;

            let response = handler.handle_write_txn_markers(request)?;
            let response_body = handler.encode_response(&response, header.request_api_version)?;
            Ok(DispatchResult {
                response_header: handler.create_response_header(header.correlation_id),
                response_body,
                effective_api_version: header.request_api_version,
                cache_key: None,
            })
        }
        ApiKey::DescribeProducers => {
            let request = DescribeProducersRequest::decode(&mut buf, header.request_api_version)
                .map_err(|e| {
                    StreamlineError::protocol("decode", format!("Failed to decode request: {}", e))
                })?;

            let response = handler.handle_describe_producers(request)?;
            let response_body = handler.encode_response(&response, header.request_api_version)?;
            Ok(DispatchResult {
                response_header: handler.create_response_header(header.correlation_id),
                response_body,
                effective_api_version: header.request_api_version,
                cache_key: None,
            })
        }
        ApiKey::DescribeTransactions => {
            let request = DescribeTransactionsRequest::decode(&mut buf, header.request_api_version)
                .map_err(|e| {
                    StreamlineError::protocol("decode", format!("Failed to decode request: {}", e))
                })?;

            let response = handler.handle_describe_transactions(request)?;
            let response_body = handler.encode_response(&response, header.request_api_version)?;
            Ok(DispatchResult {
                response_header: handler.create_response_header(header.correlation_id),
                response_body,
                effective_api_version: header.request_api_version,
                cache_key: None,
            })
        }
        ApiKey::ListTransactions => {
            let request = ListTransactionsRequest::decode(&mut buf, header.request_api_version)
                .map_err(|e| {
                    StreamlineError::protocol("decode", format!("Failed to decode request: {}", e))
                })?;

            let response = handler.handle_list_transactions(request)?;
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
