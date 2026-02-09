use bytes::Bytes;
use kafka_protocol::messages::{
    AlterUserScramCredentialsRequest, ApiKey, CreateAclsRequest, CreateDelegationTokenRequest,
    DeleteAclsRequest, DescribeAclsRequest, DescribeDelegationTokenRequest,
    DescribeUserScramCredentialsRequest, ExpireDelegationTokenRequest, RenewDelegationTokenRequest,
    RequestHeader, SaslAuthenticateRequest, SaslHandshakeRequest,
};
use kafka_protocol::protocol::Decodable;

use crate::error::{Result, StreamlineError};
use crate::protocol::kafka::{KafkaHandler, Operation, ResourceType, SessionManager};

use super::registry::DispatchResult;

#[tracing::instrument(level = "info", skip(handler, buf, session_manager), fields(api_key = ?api_key, correlation_id = header.correlation_id))]
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
        ApiKey::SaslHandshake => {
            let request = SaslHandshakeRequest::decode(&mut buf, header.request_api_version)
                .map_err(|e| {
                    StreamlineError::protocol("decode", format!("Failed to decode request: {}", e))
                })?;
            let response = handler
                .handle_sasl_handshake(request, session_manager)
                .await?;
            let response_body = handler.encode_response(&response, header.request_api_version)?;
            Ok(DispatchResult {
                response_header: handler.create_response_header(header.correlation_id),
                response_body,
                effective_api_version: header.request_api_version,
                cache_key: None,
            })
        }
        ApiKey::SaslAuthenticate => {
            let request = SaslAuthenticateRequest::decode(&mut buf, header.request_api_version)
                .map_err(|e| {
                    StreamlineError::protocol("decode", format!("Failed to decode request: {}", e))
                })?;
            let response = handler
                .handle_sasl_authenticate(request, session_manager)
                .await?;
            let response_body = handler.encode_response(&response, header.request_api_version)?;
            Ok(DispatchResult {
                response_header: handler.create_response_header(header.correlation_id),
                response_body,
                effective_api_version: header.request_api_version,
                cache_key: None,
            })
        }
        ApiKey::DescribeAcls => {
            let request = DescribeAclsRequest::decode(&mut buf, header.request_api_version)
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

            let response = handler.handle_describe_acls(request).await?;
            let response_body = handler.encode_response(&response, header.request_api_version)?;
            Ok(DispatchResult {
                response_header: handler.create_response_header(header.correlation_id),
                response_body,
                effective_api_version: header.request_api_version,
                cache_key: None,
            })
        }
        ApiKey::CreateAcls => {
            let request =
                CreateAclsRequest::decode(&mut buf, header.request_api_version).map_err(|e| {
                    StreamlineError::protocol("decode", format!("Failed to decode request: {}", e))
                })?;

            handler
                .check_authorization(
                    principal,
                    host,
                    Operation::Alter,
                    ResourceType::Cluster,
                    "kafka-cluster",
                )
                .await?;

            let response = handler.handle_create_acls(request).await?;
            let response_body = handler.encode_response(&response, header.request_api_version)?;
            Ok(DispatchResult {
                response_header: handler.create_response_header(header.correlation_id),
                response_body,
                effective_api_version: header.request_api_version,
                cache_key: None,
            })
        }
        ApiKey::DeleteAcls => {
            let request =
                DeleteAclsRequest::decode(&mut buf, header.request_api_version).map_err(|e| {
                    StreamlineError::protocol("decode", format!("Failed to decode request: {}", e))
                })?;

            handler
                .check_authorization(
                    principal,
                    host,
                    Operation::Alter,
                    ResourceType::Cluster,
                    "kafka-cluster",
                )
                .await?;

            let response = handler.handle_delete_acls(request).await?;
            let response_body = handler.encode_response(&response, header.request_api_version)?;
            Ok(DispatchResult {
                response_header: handler.create_response_header(header.correlation_id),
                response_body,
                effective_api_version: header.request_api_version,
                cache_key: None,
            })
        }
        ApiKey::CreateDelegationToken => {
            let request =
                CreateDelegationTokenRequest::decode(&mut buf, header.request_api_version)
                    .map_err(|e| {
                        StreamlineError::protocol(
                            "decode",
                            format!("Failed to decode request: {}", e),
                        )
                    })?;

            #[cfg(feature = "auth")]
            let response = handler.handle_create_delegation_token(request, principal)?;
            #[cfg(not(feature = "auth"))]
            let response = handler.handle_create_delegation_token(request)?;
            let response_body = handler.encode_response(&response, header.request_api_version)?;
            Ok(DispatchResult {
                response_header: handler.create_response_header(header.correlation_id),
                response_body,
                effective_api_version: header.request_api_version,
                cache_key: None,
            })
        }
        ApiKey::RenewDelegationToken => {
            let request = RenewDelegationTokenRequest::decode(&mut buf, header.request_api_version)
                .map_err(|e| {
                    StreamlineError::protocol("decode", format!("Failed to decode request: {}", e))
                })?;

            #[cfg(feature = "auth")]
            let response = handler.handle_renew_delegation_token(request, principal)?;
            #[cfg(not(feature = "auth"))]
            let response = handler.handle_renew_delegation_token(request)?;
            let response_body = handler.encode_response(&response, header.request_api_version)?;
            Ok(DispatchResult {
                response_header: handler.create_response_header(header.correlation_id),
                response_body,
                effective_api_version: header.request_api_version,
                cache_key: None,
            })
        }
        ApiKey::ExpireDelegationToken => {
            let request =
                ExpireDelegationTokenRequest::decode(&mut buf, header.request_api_version)
                    .map_err(|e| {
                        StreamlineError::protocol(
                            "decode",
                            format!("Failed to decode request: {}", e),
                        )
                    })?;

            #[cfg(feature = "auth")]
            let response = handler.handle_expire_delegation_token(request, principal)?;
            #[cfg(not(feature = "auth"))]
            let response = handler.handle_expire_delegation_token(request)?;
            let response_body = handler.encode_response(&response, header.request_api_version)?;
            Ok(DispatchResult {
                response_header: handler.create_response_header(header.correlation_id),
                response_body,
                effective_api_version: header.request_api_version,
                cache_key: None,
            })
        }
        ApiKey::DescribeDelegationToken => {
            let request =
                DescribeDelegationTokenRequest::decode(&mut buf, header.request_api_version)
                    .map_err(|e| {
                        StreamlineError::protocol(
                            "decode",
                            format!("Failed to decode request: {}", e),
                        )
                    })?;

            let response = handler.handle_describe_delegation_token(request)?;
            let response_body = handler.encode_response(&response, header.request_api_version)?;
            Ok(DispatchResult {
                response_header: handler.create_response_header(header.correlation_id),
                response_body,
                effective_api_version: header.request_api_version,
                cache_key: None,
            })
        }
        ApiKey::DescribeUserScramCredentials => {
            let request =
                DescribeUserScramCredentialsRequest::decode(&mut buf, header.request_api_version)
                    .map_err(|e| {
                    StreamlineError::protocol("decode", format!("Failed to decode request: {}", e))
                })?;

            let response = handler.handle_describe_user_scram_credentials(request)?;
            let response_body = handler.encode_response(&response, header.request_api_version)?;
            Ok(DispatchResult {
                response_header: handler.create_response_header(header.correlation_id),
                response_body,
                effective_api_version: header.request_api_version,
                cache_key: None,
            })
        }
        ApiKey::AlterUserScramCredentials => {
            let request =
                AlterUserScramCredentialsRequest::decode(&mut buf, header.request_api_version)
                    .map_err(|e| {
                        StreamlineError::protocol(
                            "decode",
                            format!("Failed to decode request: {}", e),
                        )
                    })?;

            let response = handler.handle_alter_user_scram_credentials(request)?;
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
