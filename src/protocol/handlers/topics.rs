use bytes::Bytes;
use kafka_protocol::messages::{
    ApiKey, CreatePartitionsRequest, CreateTopicsRequest, DeleteTopicsRequest, RequestHeader,
};
use kafka_protocol::protocol::Decodable;

use crate::error::{Result, StreamlineError};
use crate::protocol::kafka::{KafkaHandler, SessionManager};

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
        ApiKey::CreateTopics => {
            let request = CreateTopicsRequest::decode(&mut buf, header.request_api_version)
                .map_err(|e| {
                    StreamlineError::protocol("decode", format!("Failed to decode request: {}", e))
                })?;

            handler
                .check_create_topic_authorization(principal, host)
                .await?;

            let response = handler.handle_create_topics(request)?;
            let response_body = handler.encode_response(&response, header.request_api_version)?;
            Ok(DispatchResult {
                response_header: handler.create_response_header(header.correlation_id),
                response_body,
                effective_api_version: header.request_api_version,
                cache_key: None,
            })
        }
        ApiKey::DeleteTopics => {
            let request = DeleteTopicsRequest::decode(&mut buf, header.request_api_version)
                .map_err(|e| {
                    StreamlineError::protocol("decode", format!("Failed to decode request: {}", e))
                })?;

            handler
                .check_delete_topic_authorization(principal, host)
                .await?;

            let response = handler.handle_delete_topics(request, header.request_api_version)?;
            let response_body = handler.encode_response(&response, header.request_api_version)?;
            Ok(DispatchResult {
                response_header: handler.create_response_header(header.correlation_id),
                response_body,
                effective_api_version: header.request_api_version,
                cache_key: None,
            })
        }
        ApiKey::CreatePartitions => {
            let request = CreatePartitionsRequest::decode(&mut buf, header.request_api_version)
                .map_err(|e| {
                    StreamlineError::protocol("decode", format!("Failed to decode request: {}", e))
                })?;

            let response = handler
                .handle_create_partitions(request, principal, host)
                .await?;
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
