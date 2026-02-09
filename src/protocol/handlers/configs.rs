use bytes::Bytes;
use kafka_protocol::messages::{
    AlterConfigsRequest, ApiKey, DescribeConfigsRequest, IncrementalAlterConfigsRequest,
    RequestHeader,
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
        ApiKey::DescribeConfigs => {
            let request = DescribeConfigsRequest::decode(&mut buf, header.request_api_version)
                .map_err(|e| {
                    StreamlineError::protocol("decode", format!("Failed to decode request: {}", e))
                })?;

            let response = handler
                .handle_describe_configs(request, principal, host)
                .await?;
            let response_body = handler.encode_response(&response, header.request_api_version)?;
            Ok(DispatchResult {
                response_header: handler.create_response_header(header.correlation_id),
                response_body,
                effective_api_version: header.request_api_version,
                cache_key: None,
            })
        }
        ApiKey::AlterConfigs => {
            let request = AlterConfigsRequest::decode(&mut buf, header.request_api_version)
                .map_err(|e| {
                    StreamlineError::protocol("decode", format!("Failed to decode request: {}", e))
                })?;

            let response = handler
                .handle_alter_configs(request, principal, host)
                .await?;
            let response_body = handler.encode_response(&response, header.request_api_version)?;
            Ok(DispatchResult {
                response_header: handler.create_response_header(header.correlation_id),
                response_body,
                effective_api_version: header.request_api_version,
                cache_key: None,
            })
        }
        ApiKey::IncrementalAlterConfigs => {
            let request =
                IncrementalAlterConfigsRequest::decode(&mut buf, header.request_api_version)
                    .map_err(|e| {
                        StreamlineError::protocol(
                            "decode",
                            format!("Failed to decode request: {}", e),
                        )
                    })?;

            handler
                .check_authorization(
                    principal,
                    host,
                    Operation::AlterConfigs,
                    ResourceType::Cluster,
                    "kafka-cluster",
                )
                .await?;

            let response = handler.handle_incremental_alter_configs(request).await?;
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
