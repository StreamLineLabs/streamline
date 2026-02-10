//! SASL authentication mechanisms

use crate::auth::session::{ChannelBindingType, ScramState};
use crate::auth::users::ScramCredentials;
use crate::auth::UserStore;
use crate::error::{Result, StreamlineError};
use base64::{
    engine::general_purpose::{STANDARD as BASE64, URL_SAFE_NO_PAD},
    Engine,
};
use hmac::{digest::KeyInit, Hmac, Mac};
use rand::Rng;
use sha2::{Digest, Sha256, Sha512};
use tracing::{debug, warn};
use zeroize::Zeroizing;

/// SASL mechanism types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SaslMechanism {
    /// SASL/PLAIN (username/password)
    Plain,
    /// SASL/SCRAM-SHA-256
    ScramSha256,
    /// SASL/SCRAM-SHA-512
    ScramSha512,
    /// SASL/OAUTHBEARER (OAuth 2.0 / OIDC)
    OAuthBearer,
}

impl SaslMechanism {
    /// Get the mechanism name
    pub fn name(&self) -> &'static str {
        match self {
            SaslMechanism::Plain => "PLAIN",
            SaslMechanism::ScramSha256 => "SCRAM-SHA-256",
            SaslMechanism::ScramSha512 => "SCRAM-SHA-512",
            SaslMechanism::OAuthBearer => "OAUTHBEARER",
        }
    }

    /// Parse mechanism from name
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "PLAIN" => Some(SaslMechanism::Plain),
            "SCRAM-SHA-256" => Some(SaslMechanism::ScramSha256),
            "SCRAM-SHA-512" => Some(SaslMechanism::ScramSha512),
            "OAUTHBEARER" => Some(SaslMechanism::OAuthBearer),
            _ => None,
        }
    }

    /// Check if this mechanism requires OAuth provider
    pub fn requires_oauth(&self) -> bool {
        matches!(self, SaslMechanism::OAuthBearer)
    }
}

/// SASL/PLAIN authenticator
pub struct SaslPlainAuthenticator;

impl SaslPlainAuthenticator {
    /// Authenticate using SASL/PLAIN mechanism
    ///
    /// SASL/PLAIN format: \[authzid\]\\x00username\\x00password
    /// We ignore authzid (authorization identity) for now
    ///
    /// SECURITY: The password is wrapped in `Zeroizing` to ensure it is
    /// securely erased from memory after authentication completes (success or failure).
    /// This prevents passwords from lingering in memory where they could be exposed
    /// via memory dumps, core files, or memory inspection attacks.
    pub fn authenticate(auth_bytes: &[u8], user_store: &UserStore) -> Result<String> {
        // Parse SASL/PLAIN message
        let parts: Vec<&[u8]> = auth_bytes.split(|&b| b == 0).collect();

        if parts.len() != 3 {
            return Err(StreamlineError::AuthenticationFailed(
                "Invalid SASL/PLAIN format".to_string(),
            ));
        }

        let username = std::str::from_utf8(parts[1]).map_err(|_| {
            StreamlineError::AuthenticationFailed("Invalid username encoding".to_string())
        })?;

        // SECURITY: Copy password into a Zeroizing wrapper that will securely
        // clear the memory when dropped (whether authentication succeeds or fails)
        let password: Zeroizing<String> = Zeroizing::new(
            std::str::from_utf8(parts[2])
                .map_err(|_| {
                    StreamlineError::AuthenticationFailed("Invalid password encoding".to_string())
                })?
                .to_string(),
        );

        // Authenticate against user store
        // When this returns (success or error), `password` goes out of scope
        // and the Zeroizing wrapper ensures the memory is securely erased
        user_store.authenticate(username, &password)?;

        Ok(username.to_string())
    }

    /// Encode credentials for SASL/PLAIN (useful for testing)
    #[cfg(test)]
    pub fn encode_credentials(username: &str, password: &str) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.push(0); // authzid (empty)
        bytes.extend_from_slice(username.as_bytes());
        bytes.push(0);
        bytes.extend_from_slice(password.as_bytes());
        bytes
    }
}

/// SCRAM authenticator for SHA-256 and SHA-512 mechanisms
///
/// Implements RFC 5802 SCRAM authentication protocol.
/// This is a multi-step authentication that requires tracking state across messages.
pub struct ScramAuthenticator {
    mechanism: SaslMechanism,
}

impl ScramAuthenticator {
    /// Create a new SCRAM authenticator for the given mechanism
    pub fn new(mechanism: SaslMechanism) -> Result<Self> {
        match mechanism {
            SaslMechanism::ScramSha256 | SaslMechanism::ScramSha512 => Ok(Self { mechanism }),
            _ => Err(StreamlineError::AuthenticationFailed(
                "Invalid mechanism for SCRAM authenticator".to_string(),
            )),
        }
    }

    /// Process the client-first message and generate server-first response
    ///
    /// Client-first message format: `n,,n=username,r=client-nonce`
    /// Server-first response format: `r=combined-nonce,s=salt,i=iterations`
    ///
    /// # Arguments
    /// * `auth_bytes` - The client-first message bytes
    /// * `user_store` - User store for credential lookup
    /// * `server_channel_binding` - Optional channel binding data from the TLS connection
    ///   (SHA-256 hash of server certificate for tls-server-end-point)
    ///
    /// Returns: (server_first_message, ScramState)
    pub fn process_client_first(
        &self,
        auth_bytes: &[u8],
        user_store: &UserStore,
    ) -> Result<(String, ScramState)> {
        self.process_client_first_with_channel_binding(auth_bytes, user_store, None)
    }

    /// Process the client-first message with optional channel binding support
    ///
    /// Client-first message format: `n,,n=username,r=client-nonce` or `p=tls-server-end-point,,n=username,r=client-nonce`
    /// Server-first response format: `r=combined-nonce,s=salt,i=iterations`
    ///
    /// # Arguments
    /// * `auth_bytes` - The client-first message bytes
    /// * `user_store` - User store for credential lookup
    /// * `server_channel_binding` - Optional channel binding data from the TLS connection
    ///   (SHA-256 hash of server certificate for tls-server-end-point)
    ///
    /// Returns: (server_first_message, ScramState)
    pub fn process_client_first_with_channel_binding(
        &self,
        auth_bytes: &[u8],
        user_store: &UserStore,
        server_channel_binding: Option<&[u8]>,
    ) -> Result<(String, ScramState)> {
        let client_first = std::str::from_utf8(auth_bytes).map_err(|_| {
            StreamlineError::AuthenticationFailed("Invalid UTF-8 in client-first message".into())
        })?;

        // Parse client-first message
        // Format: gs2-header,client-first-message-bare
        // gs2-header: n,, (no channel binding) or y,, (channel binding) or p=tls-unique,,
        // client-first-message-bare: n=username,r=client-nonce

        let parts: Vec<&str> = client_first.splitn(3, ',').collect();
        if parts.len() < 3 {
            return Err(StreamlineError::AuthenticationFailed(
                "Invalid client-first message format".into(),
            ));
        }

        // Extract gs2-cbind-flag (first part)
        // RFC 5802: "n" = no channel binding, "y" = supports but not required, "p=..." = required
        let gs2_flag = parts[0];
        let channel_binding_type =
            ChannelBindingType::from_gs2_flag(gs2_flag).ok_or_else(|| {
                StreamlineError::AuthenticationFailed(
                    "Invalid GS2 channel binding flag - expected 'n', 'y', or 'p=...'".into(),
                )
            })?;

        // Validate channel binding requirements
        match channel_binding_type {
            ChannelBindingType::TlsServerEndPoint => {
                // Client requires tls-server-end-point channel binding
                if server_channel_binding.is_none() {
                    return Err(StreamlineError::AuthenticationFailed(
                        "Channel binding (tls-server-end-point) is required by client but \
                         server does not have TLS channel binding data available. \
                         Ensure TLS is enabled and configured correctly."
                            .into(),
                    ));
                }
                debug!("SCRAM channel binding (tls-server-end-point) negotiated");
            }
            ChannelBindingType::TlsUnique | ChannelBindingType::TlsExporter => {
                // These channel binding types are not yet supported
                return Err(StreamlineError::AuthenticationFailed(format!(
                    "Channel binding type '{}' is not supported. Use tls-server-end-point instead.",
                    gs2_flag
                )));
            }
            ChannelBindingType::Supported => {
                // Client supports channel binding but doesn't require it
                if server_channel_binding.is_none() {
                    warn!(
                        "SCRAM client supports channel binding (y flag) but server cannot verify it - \
                         consider enabling TLS channel binding for better security"
                    );
                } else {
                    debug!(
                        "SCRAM client supports channel binding but did not require it - \
                         proceeding without channel binding verification"
                    );
                }
            }
            ChannelBindingType::None => {
                // Client doesn't want channel binding - this is fine
            }
        }

        // parts[1] is authzid (usually empty)
        // parts[2] is client-first-message-bare
        let client_first_bare = parts[2];

        // Parse client-first-message-bare: n=username,r=client-nonce
        let (username, client_nonce) = Self::parse_client_first_bare(client_first_bare)?;

        // Look up user to get SCRAM credentials
        let user = user_store
            .get_user(&username)
            .ok_or(StreamlineError::InvalidCredentials)?;

        // Get SCRAM credentials for the mechanism
        let creds = self.get_scram_credentials(user)?;

        // Generate server nonce (append to client nonce)
        let server_nonce_part = Self::generate_nonce();
        let combined_nonce = format!("{}{}", client_nonce, server_nonce_part);

        // Build server-first message
        let server_first = format!(
            "r={},s={},i={}",
            combined_nonce, creds.salt, creds.iterations
        );

        // Create state for next step
        let state = ScramState {
            username,
            client_nonce,
            server_nonce: combined_nonce,
            salt: creds.salt.clone(),
            iterations: creds.iterations,
            auth_message: String::new(), // Will be completed in process_client_final
            client_first_bare: client_first_bare.to_string(),
            server_first: server_first.clone(),
            channel_binding_type,
            channel_binding_data: server_channel_binding.map(|d| d.to_vec()),
        };

        Ok((server_first, state))
    }

    /// Process the client-final message and generate server-final response
    ///
    /// Client-final message format: `c=channel-binding,r=combined-nonce,p=client-proof`
    /// Server-final response format: `v=server-signature`
    ///
    /// Returns: (server_final_message, username)
    pub fn process_client_final(
        &self,
        auth_bytes: &[u8],
        state: &ScramState,
        user_store: &UserStore,
    ) -> Result<(String, String)> {
        let client_final = std::str::from_utf8(auth_bytes).map_err(|_| {
            StreamlineError::AuthenticationFailed("Invalid UTF-8 in client-final message".into())
        })?;

        // Parse client-final message: c=base64(gs2-header + channel-binding-data),r=nonce,p=proof
        let (channel_binding_b64, nonce, client_proof) = Self::parse_client_final(client_final)?;

        // Verify nonce matches
        if nonce != state.server_nonce {
            return Err(StreamlineError::AuthenticationFailed(
                "Nonce mismatch".into(),
            ));
        }

        // Verify channel binding data if required
        self.verify_channel_binding(&channel_binding_b64, state)?;

        // Get user and credentials
        let user = user_store
            .get_user(&state.username)
            .ok_or(StreamlineError::InvalidCredentials)?;

        let creds = self.get_scram_credentials(user)?;

        // Build client-final-message-without-proof
        let client_final_without_proof = format!("c={},r={}", channel_binding_b64, nonce);

        // Build auth message: client-first-bare,server-first,client-final-without-proof
        let auth_message = format!(
            "{},{},{}",
            state.client_first_bare, state.server_first, client_final_without_proof
        );

        // Verify client proof
        let stored_key = creds.stored_key_bytes()?;
        let server_key = creds.server_key_bytes()?;

        // Decode client proof from base64
        let client_proof_bytes = BASE64
            .decode(&client_proof)
            .map_err(|_| StreamlineError::AuthenticationFailed("Invalid client proof".into()))?;

        // Verify using the proper method: recover ClientKey and check H(ClientKey) == StoredKey
        let is_valid =
            match self.mechanism {
                SaslMechanism::ScramSha256 => Self::verify_client_proof_internal::<
                    Sha256,
                    Hmac<Sha256>,
                >(
                    &stored_key, &auth_message, &client_proof_bytes
                )?,
                SaslMechanism::ScramSha512 => Self::verify_client_proof_internal::<
                    Sha512,
                    Hmac<Sha512>,
                >(
                    &stored_key, &auth_message, &client_proof_bytes
                )?,
                _ => unreachable!(),
            };

        if !is_valid {
            return Err(StreamlineError::InvalidCredentials);
        }

        // Compute server signature
        let server_signature = match self.mechanism {
            SaslMechanism::ScramSha256 => {
                self.compute_server_signature::<Hmac<Sha256>>(&server_key, &auth_message)?
            }
            SaslMechanism::ScramSha512 => {
                self.compute_server_signature::<Hmac<Sha512>>(&server_key, &auth_message)?
            }
            _ => unreachable!(),
        };

        // Build server-final message
        let server_final = format!("v={}", BASE64.encode(&server_signature));

        Ok((server_final, state.username.clone()))
    }

    /// Verify channel binding data from client-final message
    ///
    /// The c= value in client-final is base64(gs2-header + channel-binding-data)
    /// - For "n,," (no binding): c=base64("n,,") = "biws"
    /// - For "p=tls-server-end-point,,": c=base64("p=tls-server-end-point,," + cert_hash)
    fn verify_channel_binding(&self, channel_binding_b64: &str, state: &ScramState) -> Result<()> {
        // Decode the channel binding from base64
        let channel_binding_bytes = BASE64.decode(channel_binding_b64).map_err(|_| {
            StreamlineError::AuthenticationFailed("Invalid channel binding encoding".into())
        })?;

        match state.channel_binding_type {
            ChannelBindingType::None => {
                // Expected: "n,," (no channel binding)
                let expected = b"n,,";
                if channel_binding_bytes != expected {
                    return Err(StreamlineError::AuthenticationFailed(
                        "Channel binding mismatch: expected 'n,,' for no channel binding".into(),
                    ));
                }
            }
            ChannelBindingType::Supported => {
                // Client supports but doesn't require - expected: "y,,"
                // We don't verify channel binding data in this case
                let expected = b"y,,";
                if channel_binding_bytes != expected {
                    return Err(StreamlineError::AuthenticationFailed(
                        "Channel binding mismatch: expected 'y,,' for supported-but-not-required"
                            .into(),
                    ));
                }
            }
            ChannelBindingType::TlsServerEndPoint => {
                // Expected: "p=tls-server-end-point,," + channel_binding_data
                let header = b"p=tls-server-end-point,,";

                // Verify the header prefix
                if !channel_binding_bytes.starts_with(header) {
                    return Err(StreamlineError::AuthenticationFailed(
                        "Channel binding mismatch: invalid tls-server-end-point header".into(),
                    ));
                }

                // Extract client's channel binding data
                let client_cb_data = &channel_binding_bytes[header.len()..];

                // Compare with server's channel binding data
                let server_cb_data = state.channel_binding_data.as_ref().ok_or_else(|| {
                    StreamlineError::AuthenticationFailed(
                        "Server channel binding data not available".into(),
                    )
                })?;

                // Use constant-time comparison to prevent timing attacks
                if !Self::constant_time_eq(client_cb_data, server_cb_data) {
                    warn!(
                        "SCRAM channel binding verification failed - \
                         this could indicate a MITM attack or TLS session mismatch"
                    );
                    return Err(StreamlineError::AuthenticationFailed(
                        "Channel binding verification failed".into(),
                    ));
                }

                debug!("SCRAM channel binding (tls-server-end-point) verified successfully");
            }
            ChannelBindingType::TlsUnique | ChannelBindingType::TlsExporter => {
                // These should have been rejected in process_client_first
                return Err(StreamlineError::AuthenticationFailed(
                    "Unsupported channel binding type".into(),
                ));
            }
        }

        Ok(())
    }

    /// Parse client-first-message-bare: n=username,r=client-nonce
    fn parse_client_first_bare(msg: &str) -> Result<(String, String)> {
        let mut username = None;
        let mut nonce = None;

        for part in msg.split(',') {
            if let Some(rest) = part.strip_prefix("n=") {
                // Decode username (may have escaping)
                username = Some(Self::decode_scram_name(rest)?);
            } else if let Some(rest) = part.strip_prefix("r=") {
                nonce = Some(rest.to_string());
            }
        }

        let username = username.ok_or_else(|| {
            StreamlineError::AuthenticationFailed("Missing username in client-first".into())
        })?;
        let nonce = nonce.ok_or_else(|| {
            StreamlineError::AuthenticationFailed("Missing nonce in client-first".into())
        })?;

        Ok((username, nonce))
    }

    /// Parse client-final message: c=channel-binding,r=nonce,p=proof
    fn parse_client_final(msg: &str) -> Result<(String, String, String)> {
        let mut channel_binding = None;
        let mut nonce = None;
        let mut proof = None;

        for part in msg.split(',') {
            if let Some(rest) = part.strip_prefix("c=") {
                channel_binding = Some(rest.to_string());
            } else if let Some(rest) = part.strip_prefix("r=") {
                nonce = Some(rest.to_string());
            } else if let Some(rest) = part.strip_prefix("p=") {
                proof = Some(rest.to_string());
            }
        }

        let channel_binding = channel_binding.ok_or_else(|| {
            StreamlineError::AuthenticationFailed("Missing channel binding in client-final".into())
        })?;
        let nonce = nonce.ok_or_else(|| {
            StreamlineError::AuthenticationFailed("Missing nonce in client-final".into())
        })?;
        let proof = proof.ok_or_else(|| {
            StreamlineError::AuthenticationFailed("Missing proof in client-final".into())
        })?;

        Ok((channel_binding, nonce, proof))
    }

    /// Decode SCRAM name (handle =2C and =3D escapes)
    fn decode_scram_name(name: &str) -> Result<String> {
        Ok(name.replace("=2C", ",").replace("=3D", "="))
    }

    /// Get SCRAM credentials for the current mechanism
    fn get_scram_credentials<'a>(
        &self,
        user: &'a crate::auth::User,
    ) -> Result<&'a ScramCredentials> {
        match self.mechanism {
            SaslMechanism::ScramSha256 => user.scram_sha256.as_ref().ok_or_else(|| {
                StreamlineError::AuthenticationFailed(
                    "User does not have SCRAM-SHA-256 credentials".into(),
                )
            }),
            SaslMechanism::ScramSha512 => user.scram_sha512.as_ref().ok_or_else(|| {
                StreamlineError::AuthenticationFailed(
                    "User does not have SCRAM-SHA-512 credentials".into(),
                )
            }),
            _ => Err(StreamlineError::AuthenticationFailed(
                "Invalid SCRAM mechanism".into(),
            )),
        }
    }

    /// Generate a random nonce (24 bytes, URL-safe base64 encoded without padding)
    ///
    /// Uses URL_SAFE_NO_PAD to ensure nonce contains only chars that are safe
    /// in the SCRAM protocol (no '/', '+', or '=' which could conflict with
    /// SCRAM message delimiters).
    fn generate_nonce() -> String {
        let mut nonce_bytes = [0u8; 24];
        rand::thread_rng().fill(&mut nonce_bytes);
        URL_SAFE_NO_PAD.encode(nonce_bytes)
    }

    /// Verify client proof by recovering ClientKey and checking hash
    ///
    /// The verification is:
    /// 1. Compute ClientSignature = HMAC(StoredKey, AuthMessage)
    /// 2. Recover ClientKey = ClientProof XOR ClientSignature
    /// 3. Check H(ClientKey) == StoredKey
    fn verify_client_proof_internal<D, H>(
        stored_key: &[u8],
        auth_message: &str,
        client_proof: &[u8],
    ) -> Result<bool>
    where
        D: Digest,
        H: Mac + KeyInit,
    {
        // Compute ClientSignature = HMAC(StoredKey, AuthMessage)
        let client_signature = Self::hmac::<H>(stored_key, auth_message.as_bytes())?;

        // Recover ClientKey = ClientProof XOR ClientSignature
        if client_proof.len() != client_signature.len() {
            return Ok(false);
        }

        let client_key: Vec<u8> = client_proof
            .iter()
            .zip(client_signature.iter())
            .map(|(a, b)| a ^ b)
            .collect();

        // Verify H(ClientKey) == StoredKey
        let mut hasher = D::new();
        hasher.update(&client_key);
        let computed_stored_key = hasher.finalize();

        Ok(Self::constant_time_eq(&computed_stored_key, stored_key))
    }

    /// Compute server signature: HMAC(ServerKey, AuthMessage)
    fn compute_server_signature<H>(&self, server_key: &[u8], auth_message: &str) -> Result<Vec<u8>>
    where
        H: Mac + KeyInit,
    {
        Self::hmac::<H>(server_key, auth_message.as_bytes())
    }

    /// Compute HMAC
    fn hmac<H: Mac + KeyInit>(key: &[u8], data: &[u8]) -> Result<Vec<u8>> {
        let mut mac = <H as KeyInit>::new_from_slice(key)
            .map_err(|e| StreamlineError::AuthenticationFailed(e.to_string()))?;
        mac.update(data);
        Ok(mac.finalize().into_bytes().to_vec())
    }

    /// Constant-time comparison to prevent timing attacks
    fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
        if a.len() != b.len() {
            return false;
        }
        a.iter().zip(b.iter()).fold(0, |acc, (x, y)| acc | (x ^ y)) == 0
    }
}

/// Result of SCRAM client-first processing
pub struct ScramClientFirstResult {
    /// Server-first message to send to client
    pub server_first: String,
    /// State to store for next step
    pub state: ScramState,
}

/// Result of SCRAM client-final processing
pub struct ScramClientFinalResult {
    /// Server-final message to send to client
    pub server_final: String,
    /// Authenticated username
    pub username: String,
}

/// Helper functions for SCRAM that can work with password directly (for testing)
#[cfg(test)]
pub mod scram_test_utils {
    use super::*;

    /// Compute client proof from password using SHA-256 (client-side computation for testing)
    pub fn compute_client_proof_sha256(
        password: &str,
        salt: &[u8],
        iterations: u32,
        auth_message: &str,
    ) -> Vec<u8> {
        // SaltedPassword = PBKDF2(password, salt, iterations)
        let mut salted_password = [0u8; 32];
        pbkdf2::pbkdf2_hmac::<Sha256>(password.as_bytes(), salt, iterations, &mut salted_password);

        // ClientKey = HMAC(SaltedPassword, "Client Key")
        let client_key = hmac_sha256(&salted_password, b"Client Key");

        // StoredKey = SHA256(ClientKey)
        let stored_key = Sha256::digest(&client_key);

        // ClientSignature = HMAC(StoredKey, AuthMessage)
        let client_signature = hmac_sha256(&stored_key, auth_message.as_bytes());

        // ClientProof = ClientKey XOR ClientSignature
        client_key
            .iter()
            .zip(client_signature.iter())
            .map(|(a, b)| a ^ b)
            .collect()
    }

    /// Compute client proof from password using SHA-512 (client-side computation for testing)
    pub fn compute_client_proof_sha512(
        password: &str,
        salt: &[u8],
        iterations: u32,
        auth_message: &str,
    ) -> Vec<u8> {
        // SaltedPassword = PBKDF2(password, salt, iterations)
        let mut salted_password = [0u8; 64];
        pbkdf2::pbkdf2_hmac::<Sha512>(password.as_bytes(), salt, iterations, &mut salted_password);

        // ClientKey = HMAC(SaltedPassword, "Client Key")
        let client_key = hmac_sha512(&salted_password, b"Client Key");

        // StoredKey = SHA512(ClientKey)
        let stored_key = Sha512::digest(&client_key);

        // ClientSignature = HMAC(StoredKey, AuthMessage)
        let client_signature = hmac_sha512(&stored_key, auth_message.as_bytes());

        // ClientProof = ClientKey XOR ClientSignature
        client_key
            .iter()
            .zip(client_signature.iter())
            .map(|(a, b)| a ^ b)
            .collect()
    }

    fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
        // HMAC-SHA256 accepts any key length, so this should never fail
        let mut mac = <Hmac<Sha256> as KeyInit>::new_from_slice(key)
            .expect("HMAC-SHA256 should accept any key length");
        mac.update(data);
        mac.finalize().into_bytes().to_vec()
    }

    fn hmac_sha512(key: &[u8], data: &[u8]) -> Vec<u8> {
        // HMAC-SHA512 accepts any key length, so this should never fail
        let mut mac = <Hmac<Sha512> as KeyInit>::new_from_slice(key)
            .expect("HMAC-SHA512 should accept any key length");
        mac.update(data);
        mac.finalize().into_bytes().to_vec()
    }

    /// Encode client-first message
    pub fn encode_client_first(username: &str, client_nonce: &str) -> Vec<u8> {
        format!("n,,n={},r={}", username, client_nonce).into_bytes()
    }

    /// Encode client-final message
    pub fn encode_client_final(nonce: &str, proof: &[u8]) -> Vec<u8> {
        // c= is base64("n,,") for no channel binding
        let channel_binding = BASE64.encode("n,,");
        format!(
            "c={},r={},p={}",
            channel_binding,
            nonce,
            BASE64.encode(proof)
        )
        .into_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::User;

    #[test]
    fn test_sasl_mechanism_names() {
        assert_eq!(SaslMechanism::Plain.name(), "PLAIN");
        assert_eq!(SaslMechanism::ScramSha256.name(), "SCRAM-SHA-256");
        assert_eq!(SaslMechanism::ScramSha512.name(), "SCRAM-SHA-512");
        assert_eq!(SaslMechanism::OAuthBearer.name(), "OAUTHBEARER");
    }

    #[test]
    fn test_sasl_mechanism_from_name() {
        assert_eq!(
            SaslMechanism::from_name("PLAIN"),
            Some(SaslMechanism::Plain)
        );
        assert_eq!(
            SaslMechanism::from_name("SCRAM-SHA-256"),
            Some(SaslMechanism::ScramSha256)
        );
        assert_eq!(
            SaslMechanism::from_name("SCRAM-SHA-512"),
            Some(SaslMechanism::ScramSha512)
        );
        assert_eq!(
            SaslMechanism::from_name("OAUTHBEARER"),
            Some(SaslMechanism::OAuthBearer)
        );
        assert_eq!(SaslMechanism::from_name("UNKNOWN"), None);
    }

    #[test]
    fn test_sasl_mechanism_requires_oauth() {
        assert!(!SaslMechanism::Plain.requires_oauth());
        assert!(!SaslMechanism::ScramSha256.requires_oauth());
        assert!(!SaslMechanism::ScramSha512.requires_oauth());
        assert!(SaslMechanism::OAuthBearer.requires_oauth());
    }

    #[test]
    fn test_sasl_plain_authenticate_success() {
        let mut store = UserStore::new();
        let user = User::new(
            "testuser".to_string(),
            "password123",
            vec!["admin".to_string()],
        )
        .unwrap();
        store.add_user(user);

        let auth_bytes = SaslPlainAuthenticator::encode_credentials("testuser", "password123");
        let result = SaslPlainAuthenticator::authenticate(&auth_bytes, &store);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "testuser");
    }

    #[test]
    fn test_sasl_plain_authenticate_wrong_password() {
        let mut store = UserStore::new();
        let user = User::new(
            "testuser".to_string(),
            "password123",
            vec!["admin".to_string()],
        )
        .unwrap();
        store.add_user(user);

        let auth_bytes = SaslPlainAuthenticator::encode_credentials("testuser", "wrongpassword");
        let result = SaslPlainAuthenticator::authenticate(&auth_bytes, &store);

        assert!(result.is_err());
    }

    #[test]
    fn test_sasl_plain_authenticate_unknown_user() {
        let store = UserStore::new();

        let auth_bytes = SaslPlainAuthenticator::encode_credentials("unknown", "password123");
        let result = SaslPlainAuthenticator::authenticate(&auth_bytes, &store);

        assert!(result.is_err());
    }

    #[test]
    fn test_sasl_plain_invalid_format() {
        let store = UserStore::new();

        // Invalid format: only 2 parts
        let auth_bytes = b"\x00username";
        let result = SaslPlainAuthenticator::authenticate(auth_bytes, &store);

        assert!(result.is_err());
    }
}
