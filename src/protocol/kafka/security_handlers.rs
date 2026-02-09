//! Security and delegation token handlers for the Kafka protocol.
//!
//! This module contains handlers for delegation token management,
//! client quotas, SCRAM credentials, and quorum description.


use bytes::Bytes;
use crate::error::Result;
use crate::protocol::handlers::error_codes::*;
use kafka_protocol::messages::TopicName;
use kafka_protocol::messages::{
    AlterClientQuotasRequest, AlterClientQuotasResponse, AlterUserScramCredentialsRequest, AlterUserScramCredentialsResponse, CreateDelegationTokenRequest, CreateDelegationTokenResponse, DescribeClientQuotasRequest, DescribeClientQuotasResponse, DescribeDelegationTokenRequest, DescribeDelegationTokenResponse, DescribeQuorumRequest, DescribeQuorumResponse, DescribeUserScramCredentialsRequest, DescribeUserScramCredentialsResponse, ExpireDelegationTokenRequest, ExpireDelegationTokenResponse, RenewDelegationTokenRequest, RenewDelegationTokenResponse,
};
use kafka_protocol::protocol::StrBytes;
use super::KafkaHandler;
#[allow(unused_imports)]
use tracing::{debug, info, warn};

impl KafkaHandler {
    /// Handle CreateDelegationToken request (API Key 38)
    ///
    /// Creates delegation tokens for authentication.
    #[cfg(feature = "auth")]
    pub(in crate::protocol) fn handle_create_delegation_token(
        &self,
        request: CreateDelegationTokenRequest,
        principal: &str,
    ) -> Result<CreateDelegationTokenResponse> {
        info!(
            owner_principal = ?request.owner_principal_name,
            max_lifetime_ms = request.max_lifetime_ms,
            "CreateDelegationToken request"
        );

        // Check if delegation token manager is available
        let Some(ref token_manager) = self.delegation_token_manager else {
            return Ok(CreateDelegationTokenResponse::default()
                .with_error_code(DELEGATION_TOKEN_AUTH_DISABLED)
                .with_throttle_time_ms(0)
                .with_issue_timestamp_ms(0)
                .with_expiry_timestamp_ms(0)
                .with_max_timestamp_ms(0)
                .with_token_id(StrBytes::from_static_str(""))
                .with_hmac(Bytes::new()));
        };

        // Use the principal as requester
        let requester = principal.to_string();

        // Owner defaults to requester if not specified
        let owner = request
            .owner_principal_name
            .as_ref()
            .map(|s| s.to_string())
            .unwrap_or_else(|| requester.clone());

        // Extract renewers from request
        let renewers: Vec<String> = request
            .renewers
            .iter()
            .map(|r| r.principal_name.to_string())
            .collect();

        // Max lifetime from request (None = use default)
        let max_lifetime_ms = if request.max_lifetime_ms > 0 {
            Some(request.max_lifetime_ms as u64)
        } else {
            None
        };

        // Create the token
        match token_manager.create_token_sync(&owner, &requester, renewers, max_lifetime_ms) {
            Ok(token) => {
                // Get HMAC bytes for response
                let hmac_bytes = token_manager
                    .get_token_hmac_bytes(&token.token_id)
                    .unwrap_or_default();

                info!(
                    token_id = %token.token_id,
                    owner = %token.owner,
                    expiry_ms = token.expiry_timestamp_ms,
                    "Delegation token created"
                );

                Ok(CreateDelegationTokenResponse::default()
                    .with_error_code(NONE)
                    .with_throttle_time_ms(0)
                    .with_issue_timestamp_ms(token.issue_timestamp_ms as i64)
                    .with_expiry_timestamp_ms(token.expiry_timestamp_ms as i64)
                    .with_max_timestamp_ms(token.max_timestamp_ms as i64)
                    .with_token_id(StrBytes::from_string(token.token_id))
                    .with_hmac(Bytes::from(hmac_bytes)))
            }
            Err(e) => {
                warn!(error = %e, "Failed to create delegation token");
                Ok(CreateDelegationTokenResponse::default()
                    .with_error_code(DELEGATION_TOKEN_REQUEST_NOT_ALLOWED)
                    .with_throttle_time_ms(0)
                    .with_issue_timestamp_ms(0)
                    .with_expiry_timestamp_ms(0)
                    .with_max_timestamp_ms(0)
                    .with_token_id(StrBytes::from_static_str(""))
                    .with_hmac(Bytes::new()))
            }
        }
    }

    /// Handle CreateDelegationToken request (API Key 38) - non-auth fallback
    #[cfg(not(feature = "auth"))]
    pub(in crate::protocol) fn handle_create_delegation_token(
        &self,
        request: CreateDelegationTokenRequest,
    ) -> Result<CreateDelegationTokenResponse> {
        info!(
            owner_principal = ?request.owner_principal_name,
            max_lifetime_ms = request.max_lifetime_ms,
            "CreateDelegationToken request (auth disabled)"
        );

        Ok(CreateDelegationTokenResponse::default()
            .with_error_code(DELEGATION_TOKEN_AUTH_DISABLED)
            .with_throttle_time_ms(0)
            .with_issue_timestamp_ms(0)
            .with_expiry_timestamp_ms(0)
            .with_max_timestamp_ms(0)
            .with_token_id(StrBytes::from_static_str(""))
            .with_hmac(Bytes::new()))
    }

    /// Handle RenewDelegationToken request (API Key 39)
    ///
    /// Renews a delegation token to extend its expiry time.
    #[cfg(feature = "auth")]
    pub(in crate::protocol) fn handle_renew_delegation_token(
        &self,
        request: RenewDelegationTokenRequest,
        principal: &str,
    ) -> Result<RenewDelegationTokenResponse> {
        info!("RenewDelegationToken request");

        // Check if delegation token manager is available
        let Some(ref token_manager) = self.delegation_token_manager else {
            return Ok(RenewDelegationTokenResponse::default()
                .with_error_code(DELEGATION_TOKEN_AUTH_DISABLED)
                .with_throttle_time_ms(0)
                .with_expiry_timestamp_ms(0));
        };

        // Decode token ID from HMAC bytes
        // The HMAC is used to look up the token in a real implementation
        // For simplicity, we'll try to find a token by matching the HMAC
        let hmac_bytes = request.hmac.to_vec();
        let token_id = if let Some(id) = token_manager.find_token_by_hmac(&hmac_bytes) {
            id
        } else {
            return Ok(RenewDelegationTokenResponse::default()
                .with_error_code(DELEGATION_TOKEN_NOT_FOUND)
                .with_throttle_time_ms(0)
                .with_expiry_timestamp_ms(0));
        };

        // Renew period from request (None = use default)
        let renew_period_ms = if request.renew_period_ms > 0 {
            Some(request.renew_period_ms as u64)
        } else {
            None
        };

        // Renew the token
        match token_manager.renew_token_sync(&token_id, principal, renew_period_ms) {
            Ok(token) => {
                info!(
                    token_id = %token.token_id,
                    new_expiry_ms = token.expiry_timestamp_ms,
                    "Delegation token renewed"
                );

                Ok(RenewDelegationTokenResponse::default()
                    .with_error_code(NONE)
                    .with_throttle_time_ms(0)
                    .with_expiry_timestamp_ms(token.expiry_timestamp_ms as i64))
            }
            Err(e) => {
                warn!(error = %e, "Failed to renew delegation token");
                // Determine appropriate error code
                let error_code = if e.to_string().contains("not found") {
                    DELEGATION_TOKEN_NOT_FOUND
                } else if e.to_string().contains("expired") {
                    DELEGATION_TOKEN_EXPIRED
                } else if e.to_string().contains("not authorized") {
                    DELEGATION_TOKEN_OWNER_MISMATCH
                } else {
                    DELEGATION_TOKEN_REQUEST_NOT_ALLOWED
                };

                Ok(RenewDelegationTokenResponse::default()
                    .with_error_code(error_code)
                    .with_throttle_time_ms(0)
                    .with_expiry_timestamp_ms(0))
            }
        }
    }

    /// Handle RenewDelegationToken request (API Key 39) - non-auth fallback
    #[cfg(not(feature = "auth"))]
    pub(in crate::protocol) fn handle_renew_delegation_token(
        &self,
        _request: RenewDelegationTokenRequest,
    ) -> Result<RenewDelegationTokenResponse> {
        info!("RenewDelegationToken request (auth disabled)");

        Ok(RenewDelegationTokenResponse::default()
            .with_error_code(DELEGATION_TOKEN_AUTH_DISABLED)
            .with_throttle_time_ms(0)
            .with_expiry_timestamp_ms(0))
    }

    /// Handle ExpireDelegationToken request (API Key 40)
    ///
    /// Expires (invalidates) a delegation token immediately.
    #[cfg(feature = "auth")]
    pub(in crate::protocol) fn handle_expire_delegation_token(
        &self,
        request: ExpireDelegationTokenRequest,
        principal: &str,
    ) -> Result<ExpireDelegationTokenResponse> {
        info!("ExpireDelegationToken request");

        // Check if delegation token manager is available
        let Some(ref token_manager) = self.delegation_token_manager else {
            return Ok(ExpireDelegationTokenResponse::default()
                .with_error_code(DELEGATION_TOKEN_AUTH_DISABLED)
                .with_throttle_time_ms(0)
                .with_expiry_timestamp_ms(0));
        };

        // Find token by HMAC
        let hmac_bytes = request.hmac.to_vec();
        let token_id = if let Some(id) = token_manager.find_token_by_hmac(&hmac_bytes) {
            id
        } else {
            return Ok(ExpireDelegationTokenResponse::default()
                .with_error_code(DELEGATION_TOKEN_NOT_FOUND)
                .with_throttle_time_ms(0)
                .with_expiry_timestamp_ms(0));
        };

        // Expire the token
        match token_manager.expire_token_sync(&token_id, principal) {
            Ok(expiry_ms) => {
                info!(
                    token_id = %token_id,
                    expiry_ms = expiry_ms,
                    "Delegation token expired"
                );

                Ok(ExpireDelegationTokenResponse::default()
                    .with_error_code(NONE)
                    .with_throttle_time_ms(0)
                    .with_expiry_timestamp_ms(expiry_ms as i64))
            }
            Err(e) => {
                warn!(error = %e, "Failed to expire delegation token");
                let error_code = if e.to_string().contains("not found") {
                    DELEGATION_TOKEN_NOT_FOUND
                } else if e.to_string().contains("not authorized") {
                    DELEGATION_TOKEN_OWNER_MISMATCH
                } else {
                    DELEGATION_TOKEN_REQUEST_NOT_ALLOWED
                };

                Ok(ExpireDelegationTokenResponse::default()
                    .with_error_code(error_code)
                    .with_throttle_time_ms(0)
                    .with_expiry_timestamp_ms(0))
            }
        }
    }

    /// Handle ExpireDelegationToken request (API Key 40) - non-auth fallback
    #[cfg(not(feature = "auth"))]
    pub(in crate::protocol) fn handle_expire_delegation_token(
        &self,
        _request: ExpireDelegationTokenRequest,
    ) -> Result<ExpireDelegationTokenResponse> {
        info!("ExpireDelegationToken request (auth disabled)");

        Ok(ExpireDelegationTokenResponse::default()
            .with_error_code(DELEGATION_TOKEN_AUTH_DISABLED)
            .with_throttle_time_ms(0)
            .with_expiry_timestamp_ms(0))
    }

    /// Handle DescribeDelegationToken request (API Key 41)
    ///
    /// Lists and describes delegation tokens.
    #[cfg(feature = "auth")]
    pub(in crate::protocol) fn handle_describe_delegation_token(
        &self,
        request: DescribeDelegationTokenRequest,
    ) -> Result<DescribeDelegationTokenResponse> {
        use kafka_protocol::messages::describe_delegation_token_response::DescribedDelegationToken;
        use kafka_protocol::messages::describe_delegation_token_response::DescribedDelegationTokenRenewer;

        info!("DescribeDelegationToken request");

        // Check if delegation token manager is available
        let Some(ref token_manager) = self.delegation_token_manager else {
            return Ok(DescribeDelegationTokenResponse::default()
                .with_error_code(DELEGATION_TOKEN_AUTH_DISABLED)
                .with_throttle_time_ms(0)
                .with_tokens(vec![]));
        };

        // Get owner filter from request
        let owner_filter: Option<String> = request
            .owners
            .as_ref()
            .and_then(|owners| owners.first())
            .map(|o| o.principal_name.to_string());

        // List tokens (optionally filtered by owner)
        let tokens = token_manager.list_tokens_sync(owner_filter.as_deref());

        // Convert to response format
        let described_tokens: Vec<DescribedDelegationToken> = tokens
            .into_iter()
            .map(|token| {
                let renewers: Vec<DescribedDelegationTokenRenewer> = token
                    .renewers
                    .into_iter()
                    .map(|r| {
                        DescribedDelegationTokenRenewer::default()
                            .with_principal_type(StrBytes::from_static_str("User"))
                            .with_principal_name(StrBytes::from_string(r))
                    })
                    .collect();

                // Get HMAC for this token
                let hmac_bytes = token_manager
                    .get_token_hmac_bytes(&token.token_id)
                    .unwrap_or_default();

                DescribedDelegationToken::default()
                    .with_principal_type(StrBytes::from_static_str("User"))
                    .with_principal_name(StrBytes::from_string(token.owner))
                    .with_token_requester_principal_type(StrBytes::from_static_str("User"))
                    .with_token_requester_principal_name(StrBytes::from_string(token.requester))
                    .with_issue_timestamp(token.issue_timestamp_ms as i64)
                    .with_expiry_timestamp(token.expiry_timestamp_ms as i64)
                    .with_max_timestamp(token.max_timestamp_ms as i64)
                    .with_token_id(StrBytes::from_string(token.token_id))
                    .with_hmac(Bytes::from(hmac_bytes))
                    .with_renewers(renewers)
            })
            .collect();

        info!(
            token_count = described_tokens.len(),
            "Describing delegation tokens"
        );

        Ok(DescribeDelegationTokenResponse::default()
            .with_error_code(NONE)
            .with_throttle_time_ms(0)
            .with_tokens(described_tokens))
    }

    /// Handle DescribeDelegationToken request (API Key 41) - non-auth fallback
    #[cfg(not(feature = "auth"))]
    pub(in crate::protocol) fn handle_describe_delegation_token(
        &self,
        _request: DescribeDelegationTokenRequest,
    ) -> Result<DescribeDelegationTokenResponse> {
        info!("DescribeDelegationToken request (auth disabled)");

        Ok(DescribeDelegationTokenResponse::default()
            .with_error_code(NONE)
            .with_throttle_time_ms(0)
            .with_tokens(vec![]))
    }

    /// Handle DescribeClientQuotas request (API Key 48)
    ///
    /// Describes client quota configurations. Returns quotas matching the filter
    /// from the QuotaManager.
    pub(in crate::protocol) fn handle_describe_client_quotas(
        &self,
        request: DescribeClientQuotasRequest,
    ) -> Result<DescribeClientQuotasResponse> {
        use kafka_protocol::messages::describe_client_quotas_response::{
            EntityData, EntryData, ValueData,
        };

        info!(
            components_count = request.components.len(),
            strict = request.strict,
            "DescribeClientQuotas request"
        );

        // Get quota manager or return empty
        let quota_manager = match &self.quota_manager {
            Some(qm) => qm,
            None => {
                debug!("No quota manager configured, returning empty quota list");
                return Ok(DescribeClientQuotasResponse::default()
                    .with_throttle_time_ms(0)
                    .with_error_code(NONE)
                    .with_entries(Some(vec![])));
            }
        };

        // Parse filter components to extract client-id filter
        let client_id_filter: Option<String> = request
            .components
            .iter()
            .find(|c| c.entity_type.as_str() == "client-id")
            .and_then(|c| c._match.as_ref().map(|m| m.to_string()));

        let mut entries = Vec::new();

        // If filtering by specific client-id
        if let Some(ref client_id) = client_id_filter {
            if let Some(config) = quota_manager.get_client_quota(client_id) {
                let entity = vec![EntityData::default()
                    .with_entity_type(StrBytes::from_static_str("client-id"))
                    .with_entity_name(Some(StrBytes::from_string(client_id.clone())))];

                let mut values = Vec::new();
                if config.producer_byte_rate > 0 {
                    values.push(
                        ValueData::default()
                            .with_key(StrBytes::from_static_str("producer_byte_rate"))
                            .with_value(config.producer_byte_rate as f64),
                    );
                }
                if config.consumer_byte_rate > 0 {
                    values.push(
                        ValueData::default()
                            .with_key(StrBytes::from_static_str("consumer_byte_rate"))
                            .with_value(config.consumer_byte_rate as f64),
                    );
                }
                if config.request_percentage > 0 {
                    values.push(
                        ValueData::default()
                            .with_key(StrBytes::from_static_str("request_percentage"))
                            .with_value(config.request_percentage as f64),
                    );
                }

                if !values.is_empty() {
                    entries.push(EntryData::default().with_entity(entity).with_values(values));
                }
            }
        } else if request.components.is_empty() || !request.strict {
            // List all client quotas
            for (client_id, config) in quota_manager.list_client_quotas() {
                let entity = vec![EntityData::default()
                    .with_entity_type(StrBytes::from_static_str("client-id"))
                    .with_entity_name(Some(StrBytes::from_string(client_id)))];

                let mut values = Vec::new();
                if config.producer_byte_rate > 0 {
                    values.push(
                        ValueData::default()
                            .with_key(StrBytes::from_static_str("producer_byte_rate"))
                            .with_value(config.producer_byte_rate as f64),
                    );
                }
                if config.consumer_byte_rate > 0 {
                    values.push(
                        ValueData::default()
                            .with_key(StrBytes::from_static_str("consumer_byte_rate"))
                            .with_value(config.consumer_byte_rate as f64),
                    );
                }
                if config.request_percentage > 0 {
                    values.push(
                        ValueData::default()
                            .with_key(StrBytes::from_static_str("request_percentage"))
                            .with_value(config.request_percentage as f64),
                    );
                }

                if !values.is_empty() {
                    entries.push(EntryData::default().with_entity(entity).with_values(values));
                }
            }
        }

        debug!(
            entries_count = entries.len(),
            "DescribeClientQuotas returning entries"
        );

        Ok(DescribeClientQuotasResponse::default()
            .with_throttle_time_ms(0)
            .with_error_code(NONE)
            .with_entries(Some(entries)))
    }

    /// Handle AlterClientQuotas request (API Key 49)
    ///
    /// Alters client quota configurations. Stores quotas in the QuotaManager
    /// for enforcement on produce/fetch operations.
    pub(in crate::protocol) fn handle_alter_client_quotas(
        &self,
        request: AlterClientQuotasRequest,
    ) -> Result<AlterClientQuotasResponse> {
        use kafka_protocol::messages::alter_client_quotas_response::{EntityData, EntryData};

        info!(
            entries_count = request.entries.len(),
            validate_only = request.validate_only,
            "AlterClientQuotas request"
        );

        // Get quota manager
        let quota_manager = match &self.quota_manager {
            Some(qm) => qm,
            None => {
                // No quota manager - return success but quotas won't be stored
                debug!("No quota manager configured, quota alterations will not be stored");
                let results: Vec<EntryData> = request
                    .entries
                    .iter()
                    .map(|entry| {
                        let entity: Vec<EntityData> = entry
                            .entity
                            .iter()
                            .map(|e| {
                                EntityData::default()
                                    .with_entity_type(e.entity_type.clone())
                                    .with_entity_name(e.entity_name.clone())
                            })
                            .collect();
                        EntryData::default()
                            .with_error_code(NONE)
                            .with_error_message(None)
                            .with_entity(entity)
                    })
                    .collect();

                return Ok(AlterClientQuotasResponse::default()
                    .with_throttle_time_ms(0)
                    .with_entries(results));
            }
        };

        let results: Vec<EntryData> = request
            .entries
            .iter()
            .map(|entry| {
                // Find client-id entity
                let client_id = entry
                    .entity
                    .iter()
                    .find(|e| e.entity_type.as_str() == "client-id")
                    .and_then(|e| e.entity_name.as_ref().map(|n| n.to_string()));

                // Build response entity
                let entity: Vec<EntityData> = entry
                    .entity
                    .iter()
                    .map(|e| {
                        EntityData::default()
                            .with_entity_type(e.entity_type.clone())
                            .with_entity_name(e.entity_name.clone())
                    })
                    .collect();

                // Apply quota operations if not validate_only
                if !request.validate_only {
                    if let Some(ref cid) = client_id {
                        for op in &entry.ops {
                            let key = op.key.as_str();
                            match op.value {
                                value if value.is_nan() || op.remove => {
                                    // Remove the quota (set to unlimited)
                                    debug!(
                                        client_id = cid,
                                        quota_type = key,
                                        "Removing client quota"
                                    );
                                    quota_manager.remove_quota_value(cid, key);
                                }
                                value => {
                                    // Set the quota value
                                    debug!(
                                        client_id = cid,
                                        quota_type = key,
                                        value = value,
                                        "Setting client quota"
                                    );
                                    quota_manager.set_quota_value(cid, key, Some(value));
                                }
                            }
                        }
                    }
                }

                EntryData::default()
                    .with_error_code(NONE)
                    .with_error_message(None)
                    .with_entity(entity)
            })
            .collect();

        info!(
            entries_altered = results.len(),
            validate_only = request.validate_only,
            "AlterClientQuotas completed"
        );

        Ok(AlterClientQuotasResponse::default()
            .with_throttle_time_ms(0)
            .with_entries(results))
    }

    /// Handle DescribeUserScramCredentials request (API Key 50)
    ///
    /// Describes SCRAM credentials for users.
    pub(in crate::protocol) fn handle_describe_user_scram_credentials(
        &self,
        request: DescribeUserScramCredentialsRequest,
    ) -> Result<DescribeUserScramCredentialsResponse> {
        use kafka_protocol::messages::describe_user_scram_credentials_response::DescribeUserScramCredentialsResult;

        info!(
            users_count = request.users.as_ref().map(|u| u.len()).unwrap_or(0),
            "DescribeUserScramCredentials request"
        );

        // If users is None or empty, describe all users
        // Otherwise, describe specific users
        let results: Vec<DescribeUserScramCredentialsResult> = match &request.users {
            Some(users) if !users.is_empty() => {
                users
                    .iter()
                    .map(|u| {
                        // User not found (SCRAM credentials not stored)
                        DescribeUserScramCredentialsResult::default()
                            .with_user(u.name.clone())
                            .with_error_code(RESOURCE_NOT_FOUND)
                            .with_error_message(Some(StrBytes::from_string(format!(
                                "User {} not found",
                                u.name.as_str()
                            ))))
                            .with_credential_infos(vec![])
                    })
                    .collect()
            }
            _ => {
                // Return empty list - no users configured
                vec![]
            }
        };

        Ok(DescribeUserScramCredentialsResponse::default()
            .with_throttle_time_ms(0)
            .with_error_code(NONE)
            .with_results(results))
    }

    /// Handle AlterUserScramCredentials request (API Key 51)
    ///
    /// Alters SCRAM credentials for users. Currently returns success but
    /// credentials are not persisted.
    pub(in crate::protocol) fn handle_alter_user_scram_credentials(
        &self,
        request: AlterUserScramCredentialsRequest,
    ) -> Result<AlterUserScramCredentialsResponse> {
        use kafka_protocol::messages::alter_user_scram_credentials_response::AlterUserScramCredentialsResult;

        info!(
            deletions_count = request.deletions.len(),
            upsertions_count = request.upsertions.len(),
            "AlterUserScramCredentials request"
        );

        let mut results = Vec::new();

        // Process deletions
        for deletion in &request.deletions {
            results.push(
                AlterUserScramCredentialsResult::default()
                    .with_user(deletion.name.clone())
                    .with_error_code(NONE)
                    .with_error_message(None),
            );
        }

        // Process upsertions
        for upsertion in &request.upsertions {
            results.push(
                AlterUserScramCredentialsResult::default()
                    .with_user(upsertion.name.clone())
                    .with_error_code(NONE)
                    .with_error_message(None),
            );
        }

        Ok(AlterUserScramCredentialsResponse::default()
            .with_throttle_time_ms(0)
            .with_results(results))
    }

    /// Handle DescribeQuorum request (API Key 55)
    ///
    /// Describes the Raft quorum state. For single-node deployment, returns
    /// this node as the only voter and leader.
    pub(in crate::protocol) fn handle_describe_quorum(
        &self,
        _request: DescribeQuorumRequest,
    ) -> Result<DescribeQuorumResponse> {
        use kafka_protocol::messages::describe_quorum_response::{ReplicaState, TopicData};
        use kafka_protocol::messages::BrokerId;

        info!("DescribeQuorum request");

        // For single-node, we are the only voter and the leader
        let leader_state = ReplicaState::default()
            .with_replica_id(BrokerId(self.node_id))
            .with_log_end_offset(-1) // Unknown
            .with_last_fetch_timestamp(-1)
            .with_last_caught_up_timestamp(-1);

        let topic_data = TopicData::default()
            .with_topic_name(TopicName::from(StrBytes::from_static_str(
                "__cluster_metadata",
            )))
            .with_partitions(vec![
                kafka_protocol::messages::describe_quorum_response::PartitionData::default()
                    .with_partition_index(0)
                    .with_error_code(NONE)
                    .with_leader_id(BrokerId(self.node_id))
                    .with_leader_epoch(0)
                    .with_high_watermark(0)
                    .with_current_voters(vec![leader_state.clone()])
                    .with_observers(vec![]),
            ]);

        Ok(DescribeQuorumResponse::default()
            .with_error_code(NONE)
            .with_topics(vec![topic_data]))
    }

}
