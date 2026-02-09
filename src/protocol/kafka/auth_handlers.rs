//! Authentication and authorization handlers for the Kafka protocol.
//!
//! This module contains SASL authentication handlers (PLAIN, OAUTHBEARER, SCRAM)
//! and ACL management handlers. Both full auth implementations and lite/stub
//! versions for builds without the auth feature are included.


#[cfg(feature = "auth")]
use super::AuthSession;
#[cfg(feature = "auth")]
use super::PatternType;
#[cfg(feature = "auth")]
use super::Permission;
#[cfg(feature = "auth")]
use super::SaslPlainAuthenticator;
#[cfg(feature = "auth")]
use super::ScramAuthenticator;
#[cfg(feature = "auth")]
use super::{Acl, AclFilter};
#[allow(unused_imports)]
use bytes::Bytes;
use crate::error::Result;
use crate::protocol::handlers::error_codes::*;
use kafka_protocol::messages::{
    CreateAclsRequest, CreateAclsResponse, DeleteAclsRequest, DeleteAclsResponse, DescribeAclsRequest, DescribeAclsResponse, SaslAuthenticateRequest, SaslAuthenticateResponse, SaslHandshakeRequest, SaslHandshakeResponse,
};
use kafka_protocol::protocol::StrBytes;
use super::KafkaHandler;
#[allow(unused_imports)]
use super::{Operation, ResourceType};
use super::UserStore;
use super::SaslMechanism;
use super::SessionManager;
#[cfg(feature = "auth")]
use tracing::{debug, error, info, warn};

impl KafkaHandler {
    /// Get the principal from the session for authorization checks (requires auth feature)
    #[cfg(feature = "auth")]
    pub(super) async fn get_principal(&self, session_manager: &SessionManager) -> String {
        if let Some(session) = session_manager.get_session().await {
            format!("User:{}", session.user.username)
        } else {
            // Anonymous user
            "User:ANONYMOUS".to_string()
        }
    }

    // ============================================================
    // Stub authorization methods when auth is disabled (lite builds)
    // These are no-ops that always allow access
    // ============================================================

    /// Get the principal - returns ANONYMOUS when auth is disabled
    #[cfg(not(feature = "auth"))]
    pub(super) async fn get_principal(&self, _session_manager: &SessionManager) -> String {
        "User:ANONYMOUS".to_string()
    }

    /// Handle SASL handshake request
    #[cfg(feature = "auth")]
    pub(in crate::protocol) async fn handle_sasl_handshake(
        &self,
        request: SaslHandshakeRequest,
        session_manager: &SessionManager,
    ) -> Result<SaslHandshakeResponse> {
        debug!(mechanism = ?request.mechanism, "SASL handshake request");

        // If auth is not enabled, return error
        if !self.auth_config.enabled {
            let response = SaslHandshakeResponse::default()
                .with_error_code(UNSUPPORTED_SASL_MECHANISM)
                .with_mechanisms(vec![]);
            return Ok(response);
        }

        // Check if requested mechanism is supported
        let requested_mechanism = request.mechanism.as_str();
        let is_supported = self
            .auth_config
            .sasl_mechanisms
            .iter()
            .any(|m| m == requested_mechanism);

        if is_supported {
            // Parse and store the negotiated mechanism
            if let Some(mechanism) = SaslMechanism::from_name(requested_mechanism) {
                session_manager
                    .update_sasl_session(|s| s.set_mechanism(mechanism))
                    .await;
            }

            // Return all supported mechanisms
            let mechanisms: Vec<StrBytes> = self
                .auth_config
                .sasl_mechanisms
                .iter()
                .map(|m| StrBytes::from_string(m.clone()))
                .collect();

            let response = SaslHandshakeResponse::default()
                .with_error_code(NONE)
                .with_mechanisms(mechanisms);
            Ok(response)
        } else {
            // Mechanism not supported
            let response = SaslHandshakeResponse::default()
                .with_error_code(UNSUPPORTED_SASL_MECHANISM)
                .with_mechanisms(vec![]);
            Ok(response)
        }
    }

    /// Handle SASL authenticate request
    #[cfg(feature = "auth")]
    pub(in crate::protocol) async fn handle_sasl_authenticate(
        &self,
        request: SaslAuthenticateRequest,
        session_manager: &SessionManager,
    ) -> Result<SaslAuthenticateResponse> {
        debug!("SASL authenticate request");

        // If auth is not enabled, allow the request
        if !self.auth_config.enabled {
            let response = SaslAuthenticateResponse::default()
                .with_error_code(NONE)
                .with_auth_bytes(Bytes::new());
            return Ok(response);
        }

        // Get user store
        let user_store = match self.user_store.as_ref() {
            Some(store) => store,
            None => {
                error!("Authentication enabled but no user store configured");
                let response = SaslAuthenticateResponse::default()
                    .with_error_code(SASL_AUTHENTICATION_FAILED)
                    .with_error_message(Some(StrBytes::from_static_str(
                        "Authentication not configured",
                    )));
                return Ok(response);
            }
        };

        // Get the negotiated mechanism from SASL session
        let sasl_session = session_manager.get_sasl_session().await;
        let mechanism = sasl_session.mechanism.unwrap_or(SaslMechanism::Plain);

        let auth_bytes = request.auth_bytes.as_ref();

        match mechanism {
            SaslMechanism::Plain => {
                self.handle_plain_authenticate(auth_bytes, user_store, session_manager)
                    .await
            }
            SaslMechanism::ScramSha256 | SaslMechanism::ScramSha512 => {
                self.handle_scram_authenticate(mechanism, auth_bytes, user_store, session_manager)
                    .await
            }
            SaslMechanism::OAuthBearer => {
                self.handle_oauthbearer_authenticate(auth_bytes, session_manager)
                    .await
            }
        }
    }

    /// Handle PLAIN authentication (single-step)
    ///
    /// SECURITY: SASL/PLAIN sends credentials in plaintext and MUST only be used
    /// over TLS connections. Using it without TLS exposes credentials to
    /// network eavesdropping attacks.
    #[cfg(feature = "auth")]
    pub(in crate::protocol) async fn handle_plain_authenticate(
        &self,
        auth_bytes: &[u8],
        user_store: &UserStore,
        session_manager: &SessionManager,
    ) -> Result<SaslAuthenticateResponse> {
        // SECURITY: Reject SASL/PLAIN over non-TLS connections
        // SASL/PLAIN sends credentials in plaintext, making it vulnerable
        // to network sniffing if not encrypted by TLS.
        if !session_manager.is_tls() {
            warn!("SASL/PLAIN authentication rejected: connection is not TLS-encrypted");
            let response = SaslAuthenticateResponse::default()
                .with_error_code(SASL_AUTHENTICATION_FAILED)
                .with_error_message(Some(StrBytes::from_static_str(
                    "SASL/PLAIN requires TLS encryption",
                )));
            return Ok(response);
        }

        match SaslPlainAuthenticator::authenticate(auth_bytes, user_store) {
            Ok(username) => {
                // Get user and create session
                if let Some(user) = user_store.get_user(&username) {
                    let session = AuthSession::new(user.clone());
                    session_manager.set_session(session).await;

                    info!(username = %username, "User authenticated successfully via PLAIN");

                    let response = SaslAuthenticateResponse::default()
                        .with_error_code(NONE)
                        .with_auth_bytes(Bytes::new());
                    Ok(response)
                } else {
                    warn!(username = %username, "User authenticated but not found in store");
                    let response = SaslAuthenticateResponse::default()
                        .with_error_code(SASL_AUTHENTICATION_FAILED)
                        .with_error_message(Some(StrBytes::from_static_str(
                            "Authentication failed",
                        )));
                    Ok(response)
                }
            }
            Err(e) => {
                warn!(error = %e, "PLAIN authentication failed");
                let response = SaslAuthenticateResponse::default()
                    .with_error_code(SASL_AUTHENTICATION_FAILED)
                    .with_error_message(Some(StrBytes::from_static_str("Invalid credentials")));
                Ok(response)
            }
        }
    }

    /// Handle OAUTHBEARER authentication
    #[cfg(feature = "auth")]
    pub(in crate::protocol) async fn handle_oauthbearer_authenticate(
        &self,
        auth_bytes: &[u8],
        session_manager: &SessionManager,
    ) -> Result<SaslAuthenticateResponse> {
        use crate::auth::{OAuthProvider, User};

        // Get OAuth provider from session manager's config
        let oauth_config = {
            let sasl_session = session_manager.get_sasl_session().await;
            sasl_session.oauth_config.clone()
        };

        // Check if OAuth is enabled
        let oauth_config = match oauth_config {
            Some(config) if config.enabled => config,
            _ => {
                warn!("OAUTHBEARER authentication attempted but OAuth is not enabled");
                let response = SaslAuthenticateResponse::default()
                    .with_error_code(SASL_AUTHENTICATION_FAILED)
                    .with_error_message(Some(StrBytes::from_static_str(
                        "OAuth authentication not enabled",
                    )));
                return Ok(response);
            }
        };

        // Parse the OAUTHBEARER data to extract the token
        let token = match OAuthProvider::parse_oauthbearer_data(auth_bytes) {
            Ok(t) => t,
            Err(e) => {
                warn!(error = %e, "Failed to parse OAUTHBEARER data");
                let response = SaslAuthenticateResponse::default()
                    .with_error_code(SASL_AUTHENTICATION_FAILED)
                    .with_error_message(Some(StrBytes::from_static_str(
                        "Invalid OAUTHBEARER format",
                    )));
                return Ok(response);
            }
        };

        // Create OAuth provider and validate token
        let provider = OAuthProvider::new(oauth_config);
        match provider.validate_token(&token).await {
            Ok(principal) => {
                info!(principal = %principal, "User authenticated successfully via OAUTHBEARER");

                // Create a user with the principal as the username
                let user = User::oauth_user(principal.clone());
                let session = AuthSession::new(user);
                session_manager.set_session(session).await;

                let response = SaslAuthenticateResponse::default()
                    .with_error_code(NONE)
                    .with_auth_bytes(Bytes::new());
                Ok(response)
            }
            Err(e) => {
                warn!(error = %e, "OAUTHBEARER token validation failed");
                let response = SaslAuthenticateResponse::default()
                    .with_error_code(SASL_AUTHENTICATION_FAILED)
                    .with_error_message(Some(StrBytes::from_static_str("Token validation failed")));
                Ok(response)
            }
        }
    }

    /// Handle SCRAM authentication (multi-step)
    #[cfg(feature = "auth")]
    pub(in crate::protocol) async fn handle_scram_authenticate(
        &self,
        mechanism: SaslMechanism,
        auth_bytes: &[u8],
        user_store: &UserStore,
        session_manager: &SessionManager,
    ) -> Result<SaslAuthenticateResponse> {
        let sasl_session = session_manager.get_sasl_session().await;

        // Check if we're in the middle of a SCRAM handshake
        if sasl_session.is_scram_in_progress() {
            // This is the client-final message
            self.handle_scram_client_final(mechanism, auth_bytes, user_store, session_manager)
                .await
        } else {
            // This is the client-first message
            self.handle_scram_client_first(mechanism, auth_bytes, user_store, session_manager)
                .await
        }
    }

    /// Handle SCRAM client-first message
    #[cfg(feature = "auth")]
    pub(in crate::protocol) async fn handle_scram_client_first(
        &self,
        mechanism: SaslMechanism,
        auth_bytes: &[u8],
        user_store: &UserStore,
        session_manager: &SessionManager,
    ) -> Result<SaslAuthenticateResponse> {
        let authenticator = match ScramAuthenticator::new(mechanism) {
            Ok(auth) => auth,
            Err(e) => {
                error!(error = %e, "Failed to create SCRAM authenticator");
                let response = SaslAuthenticateResponse::default()
                    .with_error_code(SASL_AUTHENTICATION_FAILED)
                    .with_error_message(Some(StrBytes::from_static_str("Authentication failed")));
                return Ok(response);
            }
        };

        match authenticator.process_client_first(auth_bytes, user_store) {
            Ok((server_first, state)) => {
                debug!(username = %state.username, "SCRAM client-first processed");

                // Store state for the next message
                session_manager
                    .update_sasl_session(|s| s.set_scram_state(state))
                    .await;

                // Return server-first message
                let response = SaslAuthenticateResponse::default()
                    .with_error_code(NONE)
                    .with_auth_bytes(Bytes::from(server_first));
                Ok(response)
            }
            Err(e) => {
                warn!(error = %e, "SCRAM client-first processing failed");
                let response = SaslAuthenticateResponse::default()
                    .with_error_code(SASL_AUTHENTICATION_FAILED)
                    .with_error_message(Some(StrBytes::from_static_str("Invalid credentials")));
                Ok(response)
            }
        }
    }

    /// Handle SCRAM client-final message
    #[cfg(feature = "auth")]
    pub(in crate::protocol) async fn handle_scram_client_final(
        &self,
        mechanism: SaslMechanism,
        auth_bytes: &[u8],
        user_store: &UserStore,
        session_manager: &SessionManager,
    ) -> Result<SaslAuthenticateResponse> {
        // Take the SCRAM state (consumes it)
        let mut sasl_session = session_manager.get_sasl_session().await;
        let state = match sasl_session.take_scram_state() {
            Some(s) => s,
            None => {
                error!("SCRAM state not found");
                let response = SaslAuthenticateResponse::default()
                    .with_error_code(SASL_AUTHENTICATION_FAILED)
                    .with_error_message(Some(StrBytes::from_static_str(
                        "Authentication state error",
                    )));
                return Ok(response);
            }
        };

        // Update session to clear the state
        session_manager
            .update_sasl_session(|s| {
                s.scram_state = None;
            })
            .await;

        let authenticator = match ScramAuthenticator::new(mechanism) {
            Ok(auth) => auth,
            Err(e) => {
                error!(error = %e, "Failed to create SCRAM authenticator");
                let response = SaslAuthenticateResponse::default()
                    .with_error_code(SASL_AUTHENTICATION_FAILED)
                    .with_error_message(Some(StrBytes::from_static_str("Authentication failed")));
                return Ok(response);
            }
        };

        match authenticator.process_client_final(auth_bytes, &state, user_store) {
            Ok((server_final, username)) => {
                // Get user and create session
                if let Some(user) = user_store.get_user(&username) {
                    let session = AuthSession::new(user.clone());
                    session_manager.set_session(session).await;

                    info!(username = %username, mechanism = ?mechanism, "User authenticated successfully via SCRAM");

                    // Return server-final message
                    let response = SaslAuthenticateResponse::default()
                        .with_error_code(NONE)
                        .with_auth_bytes(Bytes::from(server_final));
                    Ok(response)
                } else {
                    warn!(username = %username, "User authenticated but not found in store");
                    let response = SaslAuthenticateResponse::default()
                        .with_error_code(SASL_AUTHENTICATION_FAILED)
                        .with_error_message(Some(StrBytes::from_static_str(
                            "Authentication failed",
                        )));
                    Ok(response)
                }
            }
            Err(e) => {
                warn!(error = %e, "SCRAM client-final processing failed");
                let response = SaslAuthenticateResponse::default()
                    .with_error_code(SASL_AUTHENTICATION_FAILED)
                    .with_error_message(Some(StrBytes::from_static_str("Invalid credentials")));
                Ok(response)
            }
        }
    }

    // ============================================================
    // Stub SASL handlers when auth is disabled (lite builds)
    // These return error responses indicating SASL is not available
    // ============================================================

    /// Handle PLAIN authentication - returns error when auth is disabled
    #[cfg(not(feature = "auth"))]
    #[allow(dead_code)]
    pub(in crate::protocol) async fn handle_plain_authenticate(
        &self,
        _auth_bytes: &[u8],
        _user_store: &UserStore,
        _session_manager: &SessionManager,
    ) -> Result<SaslAuthenticateResponse> {
        // SASL not available in lite builds
        Ok(SaslAuthenticateResponse::default()
            .with_error_code(SASL_AUTHENTICATION_FAILED)
            .with_error_message(Some(StrBytes::from_static_str(
                "SASL authentication not available: auth feature not enabled",
            ))))
    }

    /// Handle OAUTHBEARER authentication - returns error when auth is disabled
    #[cfg(not(feature = "auth"))]
    #[allow(dead_code)]
    pub(in crate::protocol) async fn handle_oauthbearer_authenticate(
        &self,
        _auth_bytes: &[u8],
        _session_manager: &SessionManager,
    ) -> Result<SaslAuthenticateResponse> {
        // SASL not available in lite builds
        Ok(SaslAuthenticateResponse::default()
            .with_error_code(SASL_AUTHENTICATION_FAILED)
            .with_error_message(Some(StrBytes::from_static_str(
                "SASL authentication not available: auth feature not enabled",
            ))))
    }

    /// Handle SCRAM authentication - returns error when auth is disabled
    #[cfg(not(feature = "auth"))]
    #[allow(dead_code)]
    pub(in crate::protocol) async fn handle_scram_authenticate(
        &self,
        _mechanism: SaslMechanism,
        _auth_bytes: &[u8],
        _user_store: &UserStore,
        _session_manager: &SessionManager,
    ) -> Result<SaslAuthenticateResponse> {
        // SASL not available in lite builds
        Ok(SaslAuthenticateResponse::default()
            .with_error_code(SASL_AUTHENTICATION_FAILED)
            .with_error_message(Some(StrBytes::from_static_str(
                "SASL authentication not available: auth feature not enabled",
            ))))
    }

    /// Handle SASL handshake request - returns error when auth is disabled
    #[cfg(not(feature = "auth"))]
    pub(in crate::protocol) async fn handle_sasl_handshake(
        &self,
        _request: SaslHandshakeRequest,
        _session_manager: &SessionManager,
    ) -> Result<SaslHandshakeResponse> {
        // SASL not available in lite builds
        Ok(SaslHandshakeResponse::default()
            .with_error_code(UNSUPPORTED_SASL_MECHANISM)
            .with_mechanisms(vec![]))
    }

    /// Handle SASL authenticate request - returns error when auth is disabled
    #[cfg(not(feature = "auth"))]
    pub(in crate::protocol) async fn handle_sasl_authenticate(
        &self,
        _request: SaslAuthenticateRequest,
        _session_manager: &SessionManager,
    ) -> Result<SaslAuthenticateResponse> {
        // SASL not available in lite builds
        Ok(SaslAuthenticateResponse::default()
            .with_error_code(SASL_AUTHENTICATION_FAILED)
            .with_error_message(Some(StrBytes::from_static_str(
                "SASL authentication not available: auth feature not enabled",
            ))))
    }

    /// Handle DescribeAcls request
    #[cfg(feature = "auth")]
    pub(in crate::protocol) async fn handle_describe_acls(
        &self,
        request: DescribeAclsRequest,
    ) -> Result<DescribeAclsResponse> {
        use kafka_protocol::messages::describe_acls_response::{
            AclDescription, DescribeAclsResource,
        };

        debug!("DescribeAcls request received");

        // Build filter from request
        let filter = self.build_acl_filter_from_describe_request(&request);

        // Get matching ACLs from authorizer
        let acl_store = self.authorizer.acl_store();
        let acl_store_guard = acl_store.read().await;
        let matching_acls = acl_store_guard.find_acls(&filter);

        // Group ACLs by resource
        let mut resources_map: std::collections::HashMap<(i8, String, i8), Vec<AclDescription>> =
            std::collections::HashMap::new();

        for acl in matching_acls {
            let key = (
                acl.resource_type.to_code(),
                acl.resource_name.clone(),
                acl.pattern_type.to_code(),
            );

            let acl_desc = AclDescription::default()
                .with_principal(StrBytes::from_string(acl.principal.clone()))
                .with_host(StrBytes::from_string(acl.host.clone()))
                .with_operation(acl.operation.to_code())
                .with_permission_type(acl.permission.to_code());

            resources_map.entry(key).or_default().push(acl_desc);
        }

        // Build response resources
        let resources: Vec<DescribeAclsResource> = resources_map
            .into_iter()
            .map(|((resource_type, resource_name, pattern_type), acls)| {
                DescribeAclsResource::default()
                    .with_resource_type(resource_type)
                    .with_resource_name(StrBytes::from_string(resource_name))
                    .with_pattern_type(pattern_type)
                    .with_acls(acls)
            })
            .collect();

        info!(num_resources = resources.len(), "DescribeAcls completed");

        let response = DescribeAclsResponse::default()
            .with_error_code(NONE)
            .with_resources(resources);

        Ok(response)
    }

    /// Build AclFilter from DescribeAclsRequest
    #[cfg(feature = "auth")]
    fn build_acl_filter_from_describe_request(&self, request: &DescribeAclsRequest) -> AclFilter {
        let mut filter = AclFilter::new();

        // Resource type filter (0 = ANY)
        if request.resource_type_filter != 0 && request.resource_type_filter != 1 {
            if let Some(rt) = ResourceType::from_code(request.resource_type_filter) {
                filter = filter.with_resource_type(rt);
            }
        }

        // Resource name filter
        if let Some(ref name) = request.resource_name_filter {
            filter = filter.with_resource_name(name.as_str());
        }

        // Pattern type filter (0 = ANY, 1 = MATCH)
        if request.pattern_type_filter > 1 {
            if let Some(pt) = PatternType::from_code(request.pattern_type_filter) {
                filter = filter.with_pattern_type(pt);
            }
        }

        // Principal filter
        if let Some(ref principal) = request.principal_filter {
            filter = filter.with_principal(principal.as_str());
        }

        // Host filter
        if let Some(ref host) = request.host_filter {
            filter = filter.with_host(host.as_str());
        }

        // Operation filter (0 = UNKNOWN, 1 = ANY, 2 = ALL)
        if request.operation > 2 {
            if let Some(op) = Operation::from_code(request.operation) {
                filter = filter.with_operation(op);
            }
        }

        // Permission type filter (0 = UNKNOWN, 1 = ANY)
        if request.permission_type > 1 {
            if let Some(perm) = Permission::from_code(request.permission_type) {
                filter = filter.with_permission(perm);
            }
        }

        filter
    }

    /// Handle CreateAcls request
    #[cfg(feature = "auth")]
    pub(in crate::protocol) async fn handle_create_acls(
        &self,
        request: CreateAclsRequest,
    ) -> Result<CreateAclsResponse> {
        debug!("CreateAcls request received");

        let mut results = Vec::new();

        for creation in request.creations.iter() {
            let result = self.create_single_acl(creation).await;
            results.push(result);
        }

        info!(
            num_created = results.iter().filter(|r| r.error_code == NONE).count(),
            "CreateAcls completed"
        );

        let response = CreateAclsResponse::default().with_results(results);

        Ok(response)
    }

    /// Create a single ACL from the request
    #[cfg(feature = "auth")]
    async fn create_single_acl(
        &self,
        creation: &kafka_protocol::messages::create_acls_request::AclCreation,
    ) -> kafka_protocol::messages::create_acls_response::AclCreationResult {
        use kafka_protocol::messages::create_acls_response::AclCreationResult;

        // Parse resource type
        let resource_type = match ResourceType::from_code(creation.resource_type) {
            Some(rt) => rt,
            None => {
                return AclCreationResult::default()
                    .with_error_code(UNKNOWN_SERVER_ERROR)
                    .with_error_message(Some(StrBytes::from_static_str("Invalid resource type")));
            }
        };

        // Parse pattern type (default to LITERAL for version 0)
        let pattern_type =
            PatternType::from_code(creation.resource_pattern_type).unwrap_or(PatternType::Literal);

        // Parse operation
        let operation = match Operation::from_code(creation.operation) {
            Some(op) => op,
            None => {
                return AclCreationResult::default()
                    .with_error_code(UNKNOWN_SERVER_ERROR)
                    .with_error_message(Some(StrBytes::from_static_str("Invalid operation")));
            }
        };

        // Parse permission
        let permission = match Permission::from_code(creation.permission_type) {
            Some(perm) => perm,
            None => {
                return AclCreationResult::default()
                    .with_error_code(UNKNOWN_SERVER_ERROR)
                    .with_error_message(Some(StrBytes::from_static_str(
                        "Invalid permission type",
                    )));
            }
        };

        // Create the ACL
        let acl = Acl::new(
            resource_type,
            creation.resource_name.as_str(),
            pattern_type,
            creation.principal.as_str(),
            creation.host.as_str(),
            operation,
            permission,
        );

        // Add to store
        let acl_store = self.authorizer.acl_store();
        let mut acl_store_guard = acl_store.write().await;
        acl_store_guard.add_acl(acl);

        info!(
            resource_type = ?resource_type,
            resource_name = creation.resource_name.as_str(),
            principal = creation.principal.as_str(),
            operation = ?operation,
            permission = ?permission,
            "ACL created"
        );

        AclCreationResult::default().with_error_code(NONE)
    }

    /// Handle DeleteAcls request
    #[cfg(feature = "auth")]
    pub(in crate::protocol) async fn handle_delete_acls(
        &self,
        request: DeleteAclsRequest,
    ) -> Result<DeleteAclsResponse> {
        use kafka_protocol::messages::delete_acls_response::{
            DeleteAclsFilterResult, DeleteAclsMatchingAcl,
        };

        debug!("DeleteAcls request received");

        let mut filter_results = Vec::new();

        for filter_req in request.filters.iter() {
            let filter = self.build_acl_filter_from_delete_filter(filter_req);

            // Get matching ACLs before deletion
            let acl_store = self.authorizer.acl_store();
            let matching_acls: Vec<Acl>;
            {
                let acl_store_guard = acl_store.read().await;
                matching_acls = acl_store_guard
                    .find_acls(&filter)
                    .into_iter()
                    .cloned()
                    .collect();
            }

            // Build matching ACL descriptions for the response
            let matching_acls_response: Vec<DeleteAclsMatchingAcl> = matching_acls
                .iter()
                .map(|acl| {
                    DeleteAclsMatchingAcl::default()
                        .with_error_code(NONE)
                        .with_resource_type(acl.resource_type.to_code())
                        .with_resource_name(StrBytes::from_string(acl.resource_name.clone()))
                        .with_pattern_type(acl.pattern_type.to_code())
                        .with_principal(StrBytes::from_string(acl.principal.clone()))
                        .with_host(StrBytes::from_string(acl.host.clone()))
                        .with_operation(acl.operation.to_code())
                        .with_permission_type(acl.permission.to_code())
                })
                .collect();

            // Remove matching ACLs
            {
                let mut acl_store_guard = acl_store.write().await;
                let removed = acl_store_guard.remove_acls(&filter);
                info!(num_removed = removed, "ACLs deleted");
            }

            let filter_result = DeleteAclsFilterResult::default()
                .with_error_code(NONE)
                .with_matching_acls(matching_acls_response);

            filter_results.push(filter_result);
        }

        let response = DeleteAclsResponse::default().with_filter_results(filter_results);

        Ok(response)
    }

    /// Build AclFilter from DeleteAclsFilter
    #[cfg(feature = "auth")]
    fn build_acl_filter_from_delete_filter(
        &self,
        filter_req: &kafka_protocol::messages::delete_acls_request::DeleteAclsFilter,
    ) -> AclFilter {
        let mut filter = AclFilter::new();

        // Resource type filter (0 = ANY)
        if filter_req.resource_type_filter != 0 && filter_req.resource_type_filter != 1 {
            if let Some(rt) = ResourceType::from_code(filter_req.resource_type_filter) {
                filter = filter.with_resource_type(rt);
            }
        }

        // Resource name filter
        if let Some(ref name) = filter_req.resource_name_filter {
            filter = filter.with_resource_name(name.as_str());
        }

        // Pattern type filter (0 = ANY, 1 = MATCH)
        if filter_req.pattern_type_filter > 1 {
            if let Some(pt) = PatternType::from_code(filter_req.pattern_type_filter) {
                filter = filter.with_pattern_type(pt);
            }
        }

        // Principal filter
        if let Some(ref principal) = filter_req.principal_filter {
            filter = filter.with_principal(principal.as_str());
        }

        // Host filter
        if let Some(ref host) = filter_req.host_filter {
            filter = filter.with_host(host.as_str());
        }

        // Operation filter (0 = UNKNOWN, 1 = ANY, 2 = ALL)
        if filter_req.operation > 2 {
            if let Some(op) = Operation::from_code(filter_req.operation) {
                filter = filter.with_operation(op);
            }
        }

        // Permission type filter (0 = UNKNOWN, 1 = ANY)
        if filter_req.permission_type > 1 {
            if let Some(perm) = Permission::from_code(filter_req.permission_type) {
                filter = filter.with_permission(perm);
            }
        }

        filter
    }

    // ============================================================
    // Stub ACL handlers when auth is disabled (lite builds)
    // These return error responses indicating ACLs are not available
    // ============================================================

    /// Handle DescribeAcls request - returns error when auth is disabled
    #[cfg(not(feature = "auth"))]
    pub(in crate::protocol) async fn handle_describe_acls(
        &self,
        _request: DescribeAclsRequest,
    ) -> Result<DescribeAclsResponse> {
        // ACLs not available in lite builds
        Ok(DescribeAclsResponse::default()
            .with_error_code(CLUSTER_AUTHORIZATION_FAILED)
            .with_error_message(Some(StrBytes::from_static_str(
                "ACLs not available: auth feature not enabled",
            ))))
    }

    /// Handle CreateAcls request - returns error when auth is disabled
    #[cfg(not(feature = "auth"))]
    pub(in crate::protocol) async fn handle_create_acls(
        &self,
        _request: CreateAclsRequest,
    ) -> Result<CreateAclsResponse> {
        // ACLs not available in lite builds
        Ok(CreateAclsResponse::default()
            .with_throttle_time_ms(0)
            .with_results(vec![]))
    }

    /// Handle DeleteAcls request - returns error when auth is disabled
    #[cfg(not(feature = "auth"))]
    pub(in crate::protocol) async fn handle_delete_acls(
        &self,
        _request: DeleteAclsRequest,
    ) -> Result<DeleteAclsResponse> {
        // ACLs not available in lite builds
        Ok(DeleteAclsResponse::default()
            .with_throttle_time_ms(0)
            .with_filter_results(vec![]))
    }

}
