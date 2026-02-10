//! Delegation Token Management for Streamline
//!
//! Delegation tokens provide a way to create short-lived, revocable credentials
//! for use in dynamic environments like containers, where sharing long-lived
//! credentials is impractical.
//!
//! ## Features
//!
//! - **Token Creation**: Generate time-limited tokens for authenticated principals
//! - **Token Renewal**: Extend token lifetime before expiration
//! - **Token Expiration**: Automatic cleanup of expired tokens
//! - **HMAC-based Authentication**: Secure token verification using HMAC-SHA-256
//!
//! ## Security Model
//!
//! Delegation tokens inherit the permissions of the principal that created them.
//! Token IDs are base64-encoded random values, and HMAC values are computed
//! using a shared secret key.

use crate::error::{Result, StreamlineError};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use hmac::{Hmac, Mac};
use parking_lot::RwLock;
use rand::Rng;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug, info, warn};

/// Default token max lifetime in milliseconds (7 days)
pub const DEFAULT_TOKEN_MAX_LIFETIME_MS: u64 = 7 * 24 * 60 * 60 * 1000;

/// Default token renewal period in milliseconds (1 day)
pub const DEFAULT_TOKEN_RENEW_PERIOD_MS: u64 = 24 * 60 * 60 * 1000;

/// Minimum token lifetime in milliseconds (1 minute)
pub const MIN_TOKEN_LIFETIME_MS: u64 = 60 * 1000;

/// Token ID length in bytes
const TOKEN_ID_BYTES: usize = 16;

/// HMAC key length in bytes
const HMAC_KEY_BYTES: usize = 32;

/// Delegation token configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DelegationTokenConfig {
    /// Enable delegation token support
    pub enabled: bool,

    /// Maximum token lifetime in milliseconds
    pub max_lifetime_ms: u64,

    /// Secret key for HMAC generation (base64 encoded)
    /// If not provided, a random key is generated on startup
    pub secret_key: Option<String>,

    /// Enable automatic expiration cleanup
    pub cleanup_enabled: bool,

    /// Cleanup interval in milliseconds
    pub cleanup_interval_ms: u64,
}

impl Default for DelegationTokenConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_lifetime_ms: DEFAULT_TOKEN_MAX_LIFETIME_MS,
            secret_key: None,
            cleanup_enabled: true,
            cleanup_interval_ms: 60 * 60 * 1000, // 1 hour
        }
    }
}

impl DelegationTokenConfig {
    /// Create a new enabled configuration
    pub fn enabled() -> Self {
        Self {
            enabled: true,
            ..Default::default()
        }
    }

    /// Set the maximum token lifetime
    pub fn with_max_lifetime_ms(mut self, ms: u64) -> Self {
        self.max_lifetime_ms = ms;
        self
    }

    /// Set the secret key
    pub fn with_secret_key(mut self, key: String) -> Self {
        self.secret_key = Some(key);
        self
    }
}

/// A delegation token
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DelegationToken {
    /// Unique token ID (base64 encoded)
    pub token_id: String,

    /// Principal (user) who owns this token
    pub owner: String,

    /// Token requester (may be different from owner for impersonation)
    pub requester: String,

    /// List of principals who can renew/describe this token
    pub renewers: Vec<String>,

    /// When the token was created (ms since epoch)
    pub issue_timestamp_ms: u64,

    /// When the token will expire (ms since epoch)
    pub expiry_timestamp_ms: u64,

    /// Maximum lifetime (ms since epoch) - token cannot be renewed past this
    pub max_timestamp_ms: u64,

    /// HMAC value for authentication (base64 encoded)
    pub hmac: String,
}

impl DelegationToken {
    /// Check if the token is expired
    pub fn is_expired(&self) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        now >= self.expiry_timestamp_ms
    }

    /// Check if the token can be renewed
    pub fn can_renew(&self) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        now < self.max_timestamp_ms && !self.is_expired()
    }

    /// Check if a principal can renew this token
    pub fn can_principal_renew(&self, principal: &str) -> bool {
        principal == self.owner || self.renewers.contains(&principal.to_string())
    }

    /// Get remaining lifetime in milliseconds
    pub fn remaining_lifetime_ms(&self) -> u64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        self.expiry_timestamp_ms.saturating_sub(now)
    }
}

/// Token information returned to clients (excludes sensitive HMAC)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenInfo {
    /// Token ID
    pub token_id: String,

    /// Token owner
    pub owner: String,

    /// Token requester
    pub requester: String,

    /// Principals who can renew
    pub renewers: Vec<String>,

    /// Issue timestamp
    pub issue_timestamp_ms: u64,

    /// Expiry timestamp
    pub expiry_timestamp_ms: u64,

    /// Maximum timestamp
    pub max_timestamp_ms: u64,
}

impl From<&DelegationToken> for TokenInfo {
    fn from(token: &DelegationToken) -> Self {
        Self {
            token_id: token.token_id.clone(),
            owner: token.owner.clone(),
            requester: token.requester.clone(),
            renewers: token.renewers.clone(),
            issue_timestamp_ms: token.issue_timestamp_ms,
            expiry_timestamp_ms: token.expiry_timestamp_ms,
            max_timestamp_ms: token.max_timestamp_ms,
        }
    }
}

/// Token authentication credentials (token ID + HMAC)
#[derive(Debug, Clone)]
pub struct TokenCredentials {
    /// Token ID
    pub token_id: String,

    /// HMAC value
    pub hmac: Vec<u8>,
}

impl TokenCredentials {
    /// Parse credentials from SASL/OAUTHBEARER-style format
    /// Format: base64(token_id:hmac)
    pub fn from_sasl_bytes(auth_bytes: &[u8]) -> Result<Self> {
        let decoded = BASE64
            .decode(auth_bytes)
            .map_err(|_| StreamlineError::AuthenticationFailed("Invalid token encoding".into()))?;

        let decoded_str = String::from_utf8(decoded)
            .map_err(|_| StreamlineError::AuthenticationFailed("Invalid token UTF-8".into()))?;

        let parts: Vec<&str> = decoded_str.splitn(2, ':').collect();
        if parts.len() != 2 {
            return Err(StreamlineError::AuthenticationFailed(
                "Invalid token format".into(),
            ));
        }

        let hmac = BASE64
            .decode(parts[1])
            .map_err(|_| StreamlineError::AuthenticationFailed("Invalid HMAC encoding".into()))?;

        Ok(Self {
            token_id: parts[0].to_string(),
            hmac,
        })
    }

    /// Encode credentials for transmission
    pub fn to_sasl_bytes(&self) -> Vec<u8> {
        let hmac_b64 = BASE64.encode(&self.hmac);
        let combined = format!("{}:{}", self.token_id, hmac_b64);
        BASE64.encode(combined).into_bytes()
    }
}

/// Delegation token manager
#[derive(Debug)]
pub struct DelegationTokenManager {
    /// Configuration
    config: DelegationTokenConfig,

    /// Secret key for HMAC generation
    secret_key: Vec<u8>,

    /// Active tokens indexed by token ID
    tokens: Arc<RwLock<HashMap<String, DelegationToken>>>,

    /// Tokens indexed by owner for efficient lookup
    tokens_by_owner: Arc<RwLock<HashMap<String, Vec<String>>>>,
}

impl DelegationTokenManager {
    /// Create a new delegation token manager
    pub fn new(config: DelegationTokenConfig) -> Self {
        let secret_key = if let Some(ref key_b64) = config.secret_key {
            BASE64.decode(key_b64).unwrap_or_else(|_| {
                warn!(
                    "Invalid base64-encoded secret key provided, generating random key - \
                     tokens will not be portable across restarts"
                );
                Self::generate_random_key()
            })
        } else {
            // Warn instead of info because random keys mean tokens won't survive restarts
            // In production, a stable secret key should be configured
            warn!(
                "No delegation token secret key configured, generating random key - \
                 tokens will not be portable across restarts. \
                 Set 'delegation_token_secret_key' in config for production deployments"
            );
            Self::generate_random_key()
        };

        Self {
            config,
            secret_key,
            tokens: Arc::new(RwLock::new(HashMap::new())),
            tokens_by_owner: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Check if delegation tokens are enabled
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Create a new delegation token
    pub async fn create_token(
        &self,
        owner: &str,
        requester: &str,
        renewers: Vec<String>,
        max_lifetime_ms: Option<u64>,
    ) -> Result<DelegationToken> {
        if !self.config.enabled {
            return Err(StreamlineError::protocol_msg(
                "Delegation tokens are disabled".into(),
            ));
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| StreamlineError::protocol_msg(format!("Time error: {}", e)))?
            .as_millis() as u64;

        // Calculate timestamps
        let max_lifetime = max_lifetime_ms
            .unwrap_or(self.config.max_lifetime_ms)
            .min(self.config.max_lifetime_ms);

        let max_timestamp = now + max_lifetime;
        let expiry_timestamp = now + DEFAULT_TOKEN_RENEW_PERIOD_MS.min(max_lifetime);

        // Generate token ID
        let token_id = Self::generate_token_id();

        // Compute HMAC
        let hmac = self.compute_hmac(&token_id, owner, now, expiry_timestamp)?;
        let hmac_b64 = BASE64.encode(&hmac);

        let token = DelegationToken {
            token_id: token_id.clone(),
            owner: owner.to_string(),
            requester: requester.to_string(),
            renewers,
            issue_timestamp_ms: now,
            expiry_timestamp_ms: expiry_timestamp,
            max_timestamp_ms: max_timestamp,
            hmac: hmac_b64,
        };

        // Store token
        {
            let mut tokens = self.tokens.write();
            tokens.insert(token_id.clone(), token.clone());
        }

        {
            let mut by_owner = self.tokens_by_owner.write();
            by_owner
                .entry(owner.to_string())
                .or_default()
                .push(token_id.clone());
        }

        info!(
            token_id = %token_id,
            owner = %owner,
            expiry_ms = expiry_timestamp,
            "Created delegation token"
        );

        Ok(token)
    }

    /// Renew a delegation token
    pub async fn renew_token(
        &self,
        token_id: &str,
        principal: &str,
        renew_period_ms: Option<u64>,
    ) -> Result<DelegationToken> {
        if !self.config.enabled {
            return Err(StreamlineError::protocol_msg(
                "Delegation tokens are disabled".into(),
            ));
        }

        let mut tokens = self.tokens.write();
        let token = tokens
            .get_mut(token_id)
            .ok_or(StreamlineError::protocol_msg("Token not found".into()))?;

        // Check authorization
        if !token.can_principal_renew(principal) {
            return Err(StreamlineError::protocol_msg(
                "Not authorized to renew this token".into(),
            ));
        }

        // Check if token can be renewed
        if !token.can_renew() {
            return Err(StreamlineError::protocol_msg(
                "Token cannot be renewed (expired or past max lifetime)".into(),
            ));
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| StreamlineError::protocol_msg(format!("Time error: {}", e)))?
            .as_millis() as u64;

        // Calculate new expiry
        let renew_period = renew_period_ms.unwrap_or(DEFAULT_TOKEN_RENEW_PERIOD_MS);
        let new_expiry = (now + renew_period).min(token.max_timestamp_ms);

        // Recompute HMAC with new expiry
        let hmac = self.compute_hmac(
            &token.token_id,
            &token.owner,
            token.issue_timestamp_ms,
            new_expiry,
        )?;
        let hmac_b64 = BASE64.encode(&hmac);

        token.expiry_timestamp_ms = new_expiry;
        token.hmac = hmac_b64;

        debug!(
            token_id = %token_id,
            new_expiry_ms = new_expiry,
            "Renewed delegation token"
        );

        Ok(token.clone())
    }

    /// Expire (revoke) a delegation token
    pub async fn expire_token(&self, token_id: &str, principal: &str) -> Result<()> {
        if !self.config.enabled {
            return Err(StreamlineError::protocol_msg(
                "Delegation tokens are disabled".into(),
            ));
        }

        let mut tokens = self.tokens.write();
        let token = tokens
            .get(token_id)
            .ok_or(StreamlineError::protocol_msg("Token not found".into()))?;

        // Check authorization (only owner can expire)
        if token.owner != principal {
            return Err(StreamlineError::protocol_msg(
                "Not authorized to expire this token".into(),
            ));
        }

        let owner = token.owner.clone();
        tokens.remove(token_id);

        // Remove from owner index
        {
            let mut by_owner = self.tokens_by_owner.write();
            if let Some(owner_tokens) = by_owner.get_mut(&owner) {
                owner_tokens.retain(|id| id != token_id);
            }
        }

        info!(token_id = %token_id, "Expired delegation token");

        Ok(())
    }

    /// Get token by ID
    pub async fn get_token(&self, token_id: &str) -> Option<DelegationToken> {
        let tokens = self.tokens.read();
        tokens.get(token_id).cloned()
    }

    /// Get token info by ID (without sensitive HMAC)
    pub async fn get_token_info(&self, token_id: &str) -> Option<TokenInfo> {
        let tokens = self.tokens.read();
        tokens.get(token_id).map(TokenInfo::from)
    }

    /// List tokens for an owner
    pub async fn list_tokens(&self, owner: Option<&str>) -> Vec<TokenInfo> {
        let tokens = self.tokens.read();

        match owner {
            Some(o) => tokens
                .values()
                .filter(|t| t.owner == o)
                .map(TokenInfo::from)
                .collect(),
            None => tokens.values().map(TokenInfo::from).collect(),
        }
    }

    /// Authenticate using token credentials
    pub async fn authenticate(&self, credentials: &TokenCredentials) -> Result<String> {
        if !self.config.enabled {
            return Err(StreamlineError::protocol_msg(
                "Delegation tokens are disabled".into(),
            ));
        }

        let tokens = self.tokens.read();
        let token = tokens
            .get(&credentials.token_id)
            .ok_or_else(|| StreamlineError::AuthenticationFailed("Token not found".into()))?;

        // Check if token is expired
        if token.is_expired() {
            return Err(StreamlineError::AuthenticationFailed(
                "Token expired".into(),
            ));
        }

        // Verify HMAC
        let expected_hmac = self.compute_hmac(
            &token.token_id,
            &token.owner,
            token.issue_timestamp_ms,
            token.expiry_timestamp_ms,
        )?;

        if !Self::constant_time_eq(&credentials.hmac, &expected_hmac) {
            return Err(StreamlineError::AuthenticationFailed(
                "Invalid token HMAC".into(),
            ));
        }

        debug!(
            token_id = %credentials.token_id,
            owner = %token.owner,
            "Authenticated via delegation token"
        );

        Ok(token.owner.clone())
    }

    /// Clean up expired tokens
    pub async fn cleanup_expired(&self) -> usize {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let mut tokens = self.tokens.write();
        let mut by_owner = self.tokens_by_owner.write();

        let expired: Vec<_> = tokens
            .iter()
            .filter(|(_, t)| t.expiry_timestamp_ms <= now)
            .map(|(id, t)| (id.clone(), t.owner.clone()))
            .collect();

        let count = expired.len();

        for (token_id, owner) in expired {
            tokens.remove(&token_id);
            if let Some(owner_tokens) = by_owner.get_mut(&owner) {
                owner_tokens.retain(|id| id != &token_id);
            }
        }

        if count > 0 {
            debug!(count, "Cleaned up expired delegation tokens");
        }

        count
    }

    /// Get statistics about delegation tokens
    pub async fn stats(&self) -> DelegationTokenStats {
        self.stats_sync()
    }

    /// Get statistics about delegation tokens (sync version)
    pub fn stats_sync(&self) -> DelegationTokenStats {
        let tokens = self.tokens.read();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let total = tokens.len();
        let expired = tokens
            .values()
            .filter(|t| t.expiry_timestamp_ms <= now)
            .count();
        let active = total - expired;

        DelegationTokenStats {
            total,
            active,
            expired,
        }
    }

    // ========== Sync Method Variants for Protocol Handlers ==========

    /// Create a new delegation token (sync version)
    pub fn create_token_sync(
        &self,
        owner: &str,
        requester: &str,
        renewers: Vec<String>,
        max_lifetime_ms: Option<u64>,
    ) -> Result<DelegationToken> {
        if !self.config.enabled {
            return Err(StreamlineError::protocol_msg(
                "Delegation tokens are disabled".into(),
            ));
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| StreamlineError::protocol_msg(format!("Time error: {}", e)))?
            .as_millis() as u64;

        let max_lifetime = max_lifetime_ms
            .unwrap_or(self.config.max_lifetime_ms)
            .min(self.config.max_lifetime_ms);

        let max_timestamp = now + max_lifetime;
        let expiry_timestamp = now + DEFAULT_TOKEN_RENEW_PERIOD_MS.min(max_lifetime);

        let token_id = Self::generate_token_id();
        let hmac = self.compute_hmac(&token_id, owner, now, expiry_timestamp)?;
        let hmac_b64 = BASE64.encode(&hmac);

        let token = DelegationToken {
            token_id: token_id.clone(),
            owner: owner.to_string(),
            requester: requester.to_string(),
            renewers,
            issue_timestamp_ms: now,
            expiry_timestamp_ms: expiry_timestamp,
            max_timestamp_ms: max_timestamp,
            hmac: hmac_b64,
        };

        {
            let mut tokens = self.tokens.write();
            tokens.insert(token_id.clone(), token.clone());
        }

        {
            let mut by_owner = self.tokens_by_owner.write();
            by_owner
                .entry(owner.to_string())
                .or_default()
                .push(token_id.clone());
        }

        info!(
            token_id = %token_id,
            owner = %owner,
            expiry_ms = expiry_timestamp,
            "Created delegation token"
        );

        Ok(token)
    }

    /// Renew a delegation token (sync version)
    pub fn renew_token_sync(
        &self,
        token_id: &str,
        principal: &str,
        renew_period_ms: Option<u64>,
    ) -> Result<DelegationToken> {
        if !self.config.enabled {
            return Err(StreamlineError::protocol_msg(
                "Delegation tokens are disabled".into(),
            ));
        }

        let mut tokens = self.tokens.write();
        let token = tokens
            .get_mut(token_id)
            .ok_or(StreamlineError::protocol_msg("Token not found".into()))?;

        if !token.can_principal_renew(principal) {
            return Err(StreamlineError::protocol_msg(
                "Not authorized to renew this token".into(),
            ));
        }

        if !token.can_renew() {
            return Err(StreamlineError::protocol_msg(
                "Token cannot be renewed (expired or past max lifetime)".into(),
            ));
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| StreamlineError::protocol_msg(format!("Time error: {}", e)))?
            .as_millis() as u64;

        let renew_period = renew_period_ms.unwrap_or(DEFAULT_TOKEN_RENEW_PERIOD_MS);
        let new_expiry = (now + renew_period).min(token.max_timestamp_ms);

        let hmac = self.compute_hmac(
            &token.token_id,
            &token.owner,
            token.issue_timestamp_ms,
            new_expiry,
        )?;
        let hmac_b64 = BASE64.encode(&hmac);

        token.expiry_timestamp_ms = new_expiry;
        token.hmac = hmac_b64;

        debug!(
            token_id = %token_id,
            new_expiry_ms = new_expiry,
            "Renewed delegation token"
        );

        Ok(token.clone())
    }

    /// Expire (revoke) a delegation token (sync version)
    pub fn expire_token_sync(&self, token_id: &str, principal: &str) -> Result<u64> {
        if !self.config.enabled {
            return Err(StreamlineError::protocol_msg(
                "Delegation tokens are disabled".into(),
            ));
        }

        let mut tokens = self.tokens.write();
        let token = tokens
            .get(token_id)
            .ok_or(StreamlineError::protocol_msg("Token not found".into()))?;

        if token.owner != principal {
            return Err(StreamlineError::protocol_msg(
                "Not authorized to expire this token".into(),
            ));
        }

        let owner = token.owner.clone();
        let expiry = token.expiry_timestamp_ms;
        tokens.remove(token_id);

        {
            let mut by_owner = self.tokens_by_owner.write();
            if let Some(owner_tokens) = by_owner.get_mut(&owner) {
                owner_tokens.retain(|id| id != token_id);
            }
        }

        info!(token_id = %token_id, "Expired delegation token");

        Ok(expiry)
    }

    /// List tokens (sync version)
    pub fn list_tokens_sync(&self, owner: Option<&str>) -> Vec<TokenInfo> {
        let tokens = self.tokens.read();

        match owner {
            Some(o) => tokens
                .values()
                .filter(|t| t.owner == o)
                .map(TokenInfo::from)
                .collect(),
            None => tokens.values().map(TokenInfo::from).collect(),
        }
    }

    /// Get token info by ID (sync version)
    pub fn get_token_info_sync(&self, token_id: &str) -> Option<TokenInfo> {
        let tokens = self.tokens.read();
        tokens.get(token_id).map(TokenInfo::from)
    }

    /// Get the raw HMAC bytes for a token (for protocol responses)
    pub fn get_token_hmac_bytes(&self, token_id: &str) -> Option<Vec<u8>> {
        let tokens = self.tokens.read();
        tokens
            .get(token_id)
            .and_then(|t| BASE64.decode(&t.hmac).ok())
    }

    /// Find a token by its HMAC bytes (for renew/expire operations)
    ///
    /// Returns the token ID if found, None otherwise.
    pub fn find_token_by_hmac(&self, hmac_bytes: &[u8]) -> Option<String> {
        let tokens = self.tokens.read();
        for (token_id, token) in tokens.iter() {
            if let Ok(stored_hmac) = BASE64.decode(&token.hmac) {
                if stored_hmac == hmac_bytes {
                    return Some(token_id.clone());
                }
            }
        }
        None
    }

    /// Generate a random token ID
    fn generate_token_id() -> String {
        let mut bytes = [0u8; TOKEN_ID_BYTES];
        rand::thread_rng().fill(&mut bytes);
        BASE64.encode(bytes)
    }

    /// Generate a random secret key
    fn generate_random_key() -> Vec<u8> {
        let mut key = vec![0u8; HMAC_KEY_BYTES];
        rand::thread_rng().fill(&mut key[..]);
        key
    }

    /// Compute HMAC for token authentication
    fn compute_hmac(
        &self,
        token_id: &str,
        owner: &str,
        issue_timestamp: u64,
        expiry_timestamp: u64,
    ) -> Result<Vec<u8>> {
        let message = format!(
            "{}:{}:{}:{}",
            token_id, owner, issue_timestamp, expiry_timestamp
        );

        let mut mac = Hmac::<Sha256>::new_from_slice(&self.secret_key)
            .map_err(|e| StreamlineError::protocol_msg(format!("HMAC error: {}", e)))?;
        mac.update(message.as_bytes());

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

impl Clone for DelegationTokenManager {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            secret_key: self.secret_key.clone(),
            tokens: Arc::clone(&self.tokens),
            tokens_by_owner: Arc::clone(&self.tokens_by_owner),
        }
    }
}

/// Statistics about delegation tokens
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DelegationTokenStats {
    /// Total number of tokens
    pub total: usize,

    /// Number of active (non-expired) tokens
    pub active: usize,

    /// Number of expired tokens
    pub expired: usize,
}

/// Spawn a background task to periodically clean up expired tokens
pub fn spawn_cleanup_task(
    manager: DelegationTokenManager,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval_timer = tokio::time::interval(interval);
        loop {
            interval_timer.tick().await;
            manager.cleanup_expired().await;
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_delegation_token_manager_disabled() {
        let config = DelegationTokenConfig::default();
        let manager = DelegationTokenManager::new(config);

        assert!(!manager.is_enabled());

        let result = manager.create_token("user1", "user1", vec![], None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_create_token() {
        let config = DelegationTokenConfig::enabled();
        let manager = DelegationTokenManager::new(config);

        let token = manager
            .create_token("owner", "requester", vec!["renewer1".to_string()], None)
            .await
            .unwrap();

        assert_eq!(token.owner, "owner");
        assert_eq!(token.requester, "requester");
        assert_eq!(token.renewers, vec!["renewer1"]);
        assert!(!token.is_expired());
        assert!(token.can_renew());
    }

    #[tokio::test]
    async fn test_authenticate_token() {
        let config = DelegationTokenConfig::enabled();
        let manager = DelegationTokenManager::new(config);

        let token = manager
            .create_token("testuser", "testuser", vec![], None)
            .await
            .unwrap();

        // Decode the HMAC from the token
        let hmac = BASE64.decode(&token.hmac).unwrap();

        let credentials = TokenCredentials {
            token_id: token.token_id.clone(),
            hmac,
        };

        let authenticated_user = manager.authenticate(&credentials).await.unwrap();
        assert_eq!(authenticated_user, "testuser");
    }

    #[tokio::test]
    async fn test_authenticate_invalid_hmac() {
        let config = DelegationTokenConfig::enabled();
        let manager = DelegationTokenManager::new(config);

        let token = manager
            .create_token("testuser", "testuser", vec![], None)
            .await
            .unwrap();

        let credentials = TokenCredentials {
            token_id: token.token_id,
            hmac: vec![0u8; 32], // Invalid HMAC
        };

        let result = manager.authenticate(&credentials).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_renew_token() {
        let config = DelegationTokenConfig::enabled();
        let manager = DelegationTokenManager::new(config);

        let token = manager
            .create_token("owner", "owner", vec!["renewer".to_string()], None)
            .await
            .unwrap();

        // Token should not be expired and should be renewable
        assert!(!token.is_expired());
        assert!(token.can_renew());

        // Owner can renew
        let renewed = manager
            .renew_token(&token.token_id, "owner", None)
            .await
            .unwrap();
        // Renewed token should have valid expiry
        assert!(!renewed.is_expired());
        assert!(renewed.expiry_timestamp_ms > 0);
        assert!(renewed.expiry_timestamp_ms <= renewed.max_timestamp_ms);

        // Renewer can also renew
        let renewed2 = manager
            .renew_token(&token.token_id, "renewer", None)
            .await
            .unwrap();
        assert!(!renewed2.is_expired());
        assert!(renewed2.expiry_timestamp_ms > 0);
    }

    #[tokio::test]
    async fn test_renew_unauthorized() {
        let config = DelegationTokenConfig::enabled();
        let manager = DelegationTokenManager::new(config);

        let token = manager
            .create_token("owner", "owner", vec![], None)
            .await
            .unwrap();

        // Random user cannot renew
        let result = manager
            .renew_token(&token.token_id, "randomuser", None)
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_expire_token() {
        let config = DelegationTokenConfig::enabled();
        let manager = DelegationTokenManager::new(config);

        let token = manager
            .create_token("owner", "owner", vec![], None)
            .await
            .unwrap();

        // Owner can expire
        manager
            .expire_token(&token.token_id, "owner")
            .await
            .unwrap();

        // Token should be gone
        assert!(manager.get_token(&token.token_id).await.is_none());
    }

    #[tokio::test]
    async fn test_expire_unauthorized() {
        let config = DelegationTokenConfig::enabled();
        let manager = DelegationTokenManager::new(config);

        let token = manager
            .create_token("owner", "owner", vec![], None)
            .await
            .unwrap();

        // Random user cannot expire
        let result = manager.expire_token(&token.token_id, "randomuser").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_list_tokens() {
        let config = DelegationTokenConfig::enabled();
        let manager = DelegationTokenManager::new(config);

        manager
            .create_token("user1", "user1", vec![], None)
            .await
            .unwrap();
        manager
            .create_token("user1", "user1", vec![], None)
            .await
            .unwrap();
        manager
            .create_token("user2", "user2", vec![], None)
            .await
            .unwrap();

        let all = manager.list_tokens(None).await;
        assert_eq!(all.len(), 3);

        let user1_tokens = manager.list_tokens(Some("user1")).await;
        assert_eq!(user1_tokens.len(), 2);

        let user2_tokens = manager.list_tokens(Some("user2")).await;
        assert_eq!(user2_tokens.len(), 1);
    }

    #[tokio::test]
    async fn test_token_credentials_encoding() {
        let credentials = TokenCredentials {
            token_id: "test-token-id".to_string(),
            hmac: vec![1, 2, 3, 4, 5, 6, 7, 8],
        };

        let encoded = credentials.to_sasl_bytes();
        let decoded = TokenCredentials::from_sasl_bytes(&encoded).unwrap();

        assert_eq!(decoded.token_id, "test-token-id");
        assert_eq!(decoded.hmac, vec![1, 2, 3, 4, 5, 6, 7, 8]);
    }

    #[tokio::test]
    async fn test_stats() {
        let config = DelegationTokenConfig::enabled();
        let manager = DelegationTokenManager::new(config);

        let stats = manager.stats().await;
        assert_eq!(stats.total, 0);
        assert_eq!(stats.active, 0);

        manager
            .create_token("user", "user", vec![], None)
            .await
            .unwrap();

        let stats = manager.stats().await;
        assert_eq!(stats.total, 1);
        assert_eq!(stats.active, 1);
    }

    #[tokio::test]
    async fn test_config_with_secret_key() {
        let key = BASE64.encode(vec![1u8; 32]);
        let config = DelegationTokenConfig::enabled().with_secret_key(key.clone());

        assert!(config.enabled);
        assert_eq!(config.secret_key, Some(key));
    }

    #[tokio::test]
    async fn test_token_can_principal_renew() {
        let token = DelegationToken {
            token_id: "test".to_string(),
            owner: "owner".to_string(),
            requester: "requester".to_string(),
            renewers: vec!["renewer1".to_string(), "renewer2".to_string()],
            issue_timestamp_ms: 0,
            expiry_timestamp_ms: u64::MAX,
            max_timestamp_ms: u64::MAX,
            hmac: String::new(),
        };

        assert!(token.can_principal_renew("owner"));
        assert!(token.can_principal_renew("renewer1"));
        assert!(token.can_principal_renew("renewer2"));
        assert!(!token.can_principal_renew("random"));
    }

    #[tokio::test]
    async fn test_cleanup_expired() {
        let config = DelegationTokenConfig::enabled();
        let manager = DelegationTokenManager::new(config);

        // Create a token with very short lifetime (already expired)
        {
            let mut tokens = manager.tokens.write();
            tokens.insert(
                "expired-token".to_string(),
                DelegationToken {
                    token_id: "expired-token".to_string(),
                    owner: "user".to_string(),
                    requester: "user".to_string(),
                    renewers: vec![],
                    issue_timestamp_ms: 0,
                    expiry_timestamp_ms: 1, // Already expired
                    max_timestamp_ms: 1,
                    hmac: String::new(),
                },
            );
        }

        let stats = manager.stats().await;
        assert_eq!(stats.expired, 1);

        let cleaned = manager.cleanup_expired().await;
        assert_eq!(cleaned, 1);

        let stats = manager.stats().await;
        assert_eq!(stats.total, 0);
    }
}
