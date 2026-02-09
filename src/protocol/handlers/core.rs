use bytes::Bytes;
use kafka_protocol::messages::{
    ApiKey, ApiVersionsRequest, ControlledShutdownRequest, MetadataRequest, RequestHeader,
};
use kafka_protocol::protocol::Decodable;
use tracing::warn;

use crate::error::{Result, StreamlineError};
use crate::protocol::kafka::{KafkaHandler, SessionManager};

use super::registry::DispatchResult;

#[tracing::instrument(skip(handler, buf, _session_manager, _principal, _host), fields(api_key = ?api_key, correlation_id = header.correlation_id))]
pub(crate) async fn dispatch(
    handler: &KafkaHandler,
    api_key: ApiKey,
    header: &RequestHeader,
    mut buf: Bytes,
    _session_manager: &SessionManager,
    _principal: &str,
    _host: &str,
) -> Result<DispatchResult> {
    match api_key {
        ApiKey::ApiVersions => {
            const API_VERSIONS_MAX_SUPPORTED: i16 = 3;
            let response_version = header.request_api_version.min(API_VERSIONS_MAX_SUPPORTED);

            let (request_result, final_version) = match ApiVersionsRequest::decode(
                &mut buf.clone(),
                response_version,
            ) {
                Ok(req) => (Ok(req), response_version),
                Err(e) => {
                    warn!(
                        requested_version = header.request_api_version,
                        capped_version = response_version,
                        error = %e,
                        "ApiVersions decode failed, falling back to v0 - may indicate protocol mismatch or malformed request"
                    );
                    (ApiVersionsRequest::decode(&mut buf, 0), 0)
                }
            };

            let request = request_result.map_err(|e| {
                StreamlineError::protocol("decode", format!("Failed to decode request: {}", e))
            })?;
            let response = handler.handle_api_versions(request)?;
            let response_body = handler.encode_response(&response, final_version)?;
            Ok(DispatchResult {
                response_header: handler.create_response_header(header.correlation_id),
                response_body,
                effective_api_version: final_version,
                cache_key: None,
            })
        }
        ApiKey::Metadata => {
            let request =
                MetadataRequest::decode(&mut buf, header.request_api_version).map_err(|e| {
                    StreamlineError::protocol("decode", format!("Failed to decode request: {}", e))
                })?;
            let response = handler
                .handle_metadata(request, header.request_api_version)
                .await?;
            let response_body = handler.encode_response(&response, header.request_api_version)?;
            Ok(DispatchResult {
                response_header: handler.create_response_header(header.correlation_id),
                response_body,
                effective_api_version: header.request_api_version,
                cache_key: None,
            })
        }
        ApiKey::ControlledShutdown => {
            let request = ControlledShutdownRequest::decode(&mut buf, header.request_api_version)
                .map_err(|e| {
                StreamlineError::protocol("decode", format!("Failed to decode request: {}", e))
            })?;
            let response = handler.handle_controlled_shutdown(request)?;
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
