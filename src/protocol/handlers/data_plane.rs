use bytes::Bytes;
use kafka_protocol::messages::{
    ApiKey, FetchRequest, ListOffsetsRequest, ProduceRequest, RequestHeader,
};
use kafka_protocol::protocol::Decodable;
use tracing::debug;

use crate::error::{Result, StreamlineError};
use crate::protocol::kafka::{KafkaHandler, SessionManager};
use crate::protocol::CacheKey;

use super::registry::DispatchResult;

#[tracing::instrument(level = "debug", skip(handler, buf, session_manager), fields(api_key = ?api_key, correlation_id = header.correlation_id))]
pub(crate) async fn dispatch(
    handler: &KafkaHandler,
    api_key: ApiKey,
    header: &RequestHeader,
    mut buf: Bytes,
    session_manager: &SessionManager,
    principal: &str,
    host: &str,
) -> Result<DispatchResult> {
    match api_key {
        ApiKey::Produce => {
            let request =
                ProduceRequest::decode(&mut buf, header.request_api_version).map_err(|e| {
                    StreamlineError::protocol("decode", format!("Failed to decode request: {}", e))
                })?;

            let topics: Vec<&str> = request.topic_data.iter().map(|t| t.name.as_str()).collect();
            handler
                .check_produce_authorization(principal, host, &topics)
                .await?;

            let client_id_str = header.client_id.as_ref().map(|s| s.as_str());
            let response = handler
                .handle_produce(request, session_manager, client_id_str)
                .await?;
            let response_body = handler.encode_response(&response, header.request_api_version)?;
            Ok(DispatchResult {
                response_header: handler.create_response_header(header.correlation_id),
                response_body,
                effective_api_version: header.request_api_version,
                cache_key: None,
            })
        }
        ApiKey::Fetch => {
            let request =
                FetchRequest::decode(&mut buf, header.request_api_version).map_err(|e| {
                    StreamlineError::protocol("decode", format!("Failed to decode request: {}", e))
                })?;

            let topics: Vec<&str> = request.topics.iter().map(|t| t.topic.as_str()).collect();
            handler
                .check_fetch_authorization(principal, host, &topics)
                .await?;

            let mut cache_key = None;
            if let Some(first_topic) = request.topics.first() {
                if let Some(first_partition) = first_topic.partitions.first() {
                    cache_key = Some(CacheKey::new(
                        first_topic.topic.as_str().to_string(),
                        first_partition.partition,
                        first_partition.fetch_offset,
                        first_partition.partition_max_bytes,
                    ));
                }
            }

            let client_id_str = header.client_id.as_ref().map(|s| s.as_str());
            let response = handler
                .handle_fetch(request, session_manager, client_id_str)
                .await?;
            let response_body = handler.encode_response(&response, header.request_api_version)?;
            Ok(DispatchResult {
                response_header: handler.create_response_header(header.correlation_id),
                response_body,
                effective_api_version: header.request_api_version,
                cache_key,
            })
        }
        ApiKey::ListOffsets => {
            let request = ListOffsetsRequest::decode(&mut buf, header.request_api_version)
                .map_err(|e| {
                    StreamlineError::protocol("decode", format!("Failed to decode request: {}", e))
                })?;
            let response = handler.handle_list_offsets(request, header.request_api_version)?;
            let response_body = handler.encode_response(&response, header.request_api_version)?;
            debug!(
                api_version = header.request_api_version,
                body_len = response_body.len(),
                body_hex = ?hex::encode(&response_body[..response_body.len().min(100)]),
                "ListOffsets response encoded"
            );
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
