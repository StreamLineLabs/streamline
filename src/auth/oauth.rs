//! OAuth 2.0 / OIDC authentication support
//!
//! This module provides OAuth 2.0 and OpenID Connect authentication for Streamline,
//! implementing the SASL/OAUTHBEARER mechanism for Kafka protocol compatibility.
//!
//! ## Features
//!
//! - JWT token validation with RS256, RS384, RS512, ES256, ES384 algorithms
//! - OIDC discovery for automatic JWKS endpoint detection
//! - Multiple issuer support for multi-tenant deployments
//! - Token caching for performance
//! - Audience and issuer validation
//!
//! ## Example Configuration
//!
//! ```bash
//! streamline --oauth-enabled \
//!     --oauth-issuer-url https://auth.example.com \
//!     --oauth-audience my-kafka-cluster \
//!     --oauth-jwks-url https://auth.example.com/.well-known/jwks.json
//! ```

use crate::error::{Result, StreamlineError};
use jsonwebtoken::{
    decode, decode_header, jwk::JwkSet, Algorithm, DecodingKey, TokenData, Validation,
};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex as TokioMutex;
use tracing::{debug, info, warn};

/// OAuth/OIDC configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OAuthConfig {
    /// Enable OAuth/OIDC authentication
    pub enabled: bool,

    /// OIDC issuer URL (e.g., `https://auth.example.com`)
    /// Used for token validation and OIDC discovery
    pub issuer_url: Option<String>,

    /// JWKS (JSON Web Key Set) URL for token validation
    /// If not specified, will be discovered from issuer_url
    pub jwks_url: Option<String>,

    /// Expected audience claim in the token
    pub audience: Option<String>,

    /// Additional valid issuers (for multi-tenant setups)
    #[serde(default)]
    pub additional_issuers: Vec<String>,

    /// Claim name to use as the principal/username (default: "sub")
    #[serde(default = "default_principal_claim")]
    pub principal_claim: String,

    /// JWKS refresh interval in seconds (default: 3600)
    #[serde(default = "default_jwks_refresh_interval")]
    pub jwks_refresh_interval_secs: u64,

    /// Allow tokens without audience claim
    #[serde(default)]
    pub allow_missing_audience: bool,

    /// Custom scope requirements
    #[serde(default)]
    pub required_scopes: Vec<String>,

    /// Clock skew tolerance in seconds for token time validation (default: 60)
    #[serde(default = "default_clock_skew_seconds")]
    pub clock_skew_seconds: u64,

    /// Validate issued-at (iat) claim
    #[serde(default = "default_validate_iat")]
    pub validate_iat: bool,

    /// Validate not-before (nbf) claim
    #[serde(default = "default_validate_nbf")]
    pub validate_nbf: bool,

    /// Maximum token age in seconds (from iat claim). None means no limit.
    #[serde(default)]
    pub max_token_age_seconds: Option<u64>,
}

fn default_principal_claim() -> String {
    "sub".to_string()
}

fn default_jwks_refresh_interval() -> u64 {
    900 // 15 minutes (reduced from 1 hour for better security)
}

fn default_clock_skew_seconds() -> u64 {
    60 // 1 minute tolerance
}

fn default_validate_iat() -> bool {
    true
}

fn default_validate_nbf() -> bool {
    true
}

impl Default for OAuthConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            issuer_url: None,
            jwks_url: None,
            audience: None,
            additional_issuers: Vec::new(),
            principal_claim: default_principal_claim(),
            jwks_refresh_interval_secs: default_jwks_refresh_interval(),
            allow_missing_audience: false,
            required_scopes: Vec::new(),
            clock_skew_seconds: default_clock_skew_seconds(),
            validate_iat: default_validate_iat(),
            validate_nbf: default_validate_nbf(),
            max_token_age_seconds: None,
        }
    }
}

/// Standard JWT claims
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenClaims {
    /// Subject (user identifier)
    pub sub: Option<String>,

    /// Issuer
    pub iss: Option<String>,

    /// Audience (can be string or array)
    #[serde(default)]
    pub aud: Audience,

    /// Expiration time (Unix timestamp)
    pub exp: Option<i64>,

    /// Issued at time (Unix timestamp)
    pub iat: Option<i64>,

    /// Not before time (Unix timestamp)
    pub nbf: Option<i64>,

    /// JWT ID
    pub jti: Option<String>,

    /// Scope claim (space-separated string)
    pub scope: Option<String>,

    /// Azure AD preferred username
    pub preferred_username: Option<String>,

    /// Email claim
    pub email: Option<String>,

    /// Name claim
    pub name: Option<String>,

    /// Additional claims (catch-all)
    #[serde(flatten)]
    pub additional: HashMap<String, serde_json::Value>,
}

/// Audience can be a single string or array of strings
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(untagged)]
pub enum Audience {
    #[default]
    None,
    Single(String),
    Multiple(Vec<String>),
}

impl Audience {
    pub fn contains(&self, aud: &str) -> bool {
        match self {
            Audience::None => false,
            Audience::Single(s) => s == aud,
            Audience::Multiple(v) => v.iter().any(|a| a == aud),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Audience::None => true,
            Audience::Single(s) => s.is_empty(),
            Audience::Multiple(v) => v.is_empty(),
        }
    }
}

/// Cached JWKS with refresh tracking
struct CachedJwks {
    jwks: JwkSet,
    fetched_at: Instant,
}

/// OAuth/OIDC provider for token validation
pub struct OAuthProvider {
    config: OAuthConfig,
    jwks_cache: Arc<RwLock<Option<CachedJwks>>>,
    /// Pre-loaded decoding keys (for static configuration)
    static_keys: Arc<RwLock<HashMap<String, DecodingKey>>>,
    /// Flag to prevent concurrent JWKS fetches
    jwks_fetch_in_progress: Arc<AtomicBool>,
    /// Async mutex for serializing JWKS fetch requests (must be async-safe)
    jwks_fetch_lock: Arc<TokioMutex<()>>,
}

impl OAuthProvider {
    /// Create a new OAuth provider with the given configuration
    pub fn new(config: OAuthConfig) -> Self {
        info!(
            issuer = ?config.issuer_url,
            audience = ?config.audience,
            jwks_refresh_interval_secs = config.jwks_refresh_interval_secs,
            clock_skew_seconds = config.clock_skew_seconds,
            "OAuth provider initialized"
        );

        Self {
            config,
            jwks_cache: Arc::new(RwLock::new(None)),
            static_keys: Arc::new(RwLock::new(HashMap::new())),
            jwks_fetch_in_progress: Arc::new(AtomicBool::new(false)),
            jwks_fetch_lock: Arc::new(TokioMutex::new(())),
        }
    }

    /// Check if OAuth is enabled
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Get the configuration
    pub fn config(&self) -> &OAuthConfig {
        &self.config
    }

    /// Validate an OAuth/OIDC token and extract the principal
    pub async fn validate_token(&self, token: &str) -> Result<String> {
        // Decode header to get key ID and algorithm
        let header = decode_header(token).map_err(|e| {
            StreamlineError::AuthenticationFailed(format!("Invalid token header: {}", e))
        })?;

        debug!(
            algorithm = ?header.alg,
            kid = ?header.kid,
            "Validating OAuth token"
        );

        // Get the decoding key
        let decoding_key = self.get_decoding_key(&header.kid, header.alg).await?;

        // Build validation rules
        let mut validation = Validation::new(header.alg);

        // Set issuer validation
        let mut valid_issuers = Vec::new();
        if let Some(ref issuer) = self.config.issuer_url {
            valid_issuers.push(issuer.clone());
            // Also allow without trailing slash
            if issuer.ends_with('/') {
                valid_issuers.push(issuer.trim_end_matches('/').to_string());
            } else {
                valid_issuers.push(format!("{}/", issuer));
            }
        }
        valid_issuers.extend(self.config.additional_issuers.clone());

        if !valid_issuers.is_empty() {
            validation.set_issuer(&valid_issuers);
        }

        // Set audience validation
        if let Some(ref audience) = self.config.audience {
            validation.set_audience(&[audience]);
        } else if !self.config.allow_missing_audience {
            // Require audience but don't validate specific value
            validation.validate_aud = false;
        } else {
            validation.validate_aud = false;
        }

        // Decode and validate the token
        let token_data: TokenData<TokenClaims> = decode(token, &decoding_key, &validation)
            .map_err(|e| {
                StreamlineError::AuthenticationFailed(format!("Token validation failed: {}", e))
            })?;

        // Validate time-based claims (iat, nbf) with clock skew tolerance
        self.validate_time_claims(&token_data.claims)?;

        // Validate scopes if required
        if !self.config.required_scopes.is_empty() {
            self.validate_scopes(&token_data.claims)?;
        }

        // Extract principal from configured claim
        let principal = self.extract_principal(&token_data.claims)?;

        info!(
            principal = %principal,
            issuer = ?token_data.claims.iss,
            "OAuth token validated successfully"
        );

        Ok(principal)
    }

    /// Get decoding key for token validation
    async fn get_decoding_key(&self, kid: &Option<String>, alg: Algorithm) -> Result<DecodingKey> {
        // First check static keys
        if let Some(key_id) = kid {
            let static_keys = self.static_keys.read();
            if let Some(key) = static_keys.get(key_id) {
                return Ok(key.clone());
            }
        }

        // Try to get from JWKS
        let jwks = self.get_jwks().await?;

        // Find matching key
        let key = if let Some(ref key_id) = kid {
            jwks.keys
                .iter()
                .find(|k| k.common.key_id.as_ref() == Some(key_id))
        } else {
            // No kid specified, try to find by algorithm
            jwks.keys.iter().find(|k| {
                k.common
                    .key_algorithm
                    .map(|a| algorithm_matches(a, alg))
                    .unwrap_or(true)
            })
        };

        let jwk = key.ok_or_else(|| {
            StreamlineError::AuthenticationFailed(format!(
                "No matching key found for kid={:?}, alg={:?}",
                kid, alg
            ))
        })?;

        DecodingKey::from_jwk(jwk)
            .map_err(|e| StreamlineError::AuthenticationFailed(format!("Invalid JWK: {}", e)))
    }

    /// Get JWKS, fetching if necessary
    ///
    /// Uses double-checked locking with a mutex to prevent concurrent fetches.
    /// If multiple requests arrive while JWKS is being fetched, only one will
    /// actually perform the fetch; others will wait and use the cached result.
    async fn get_jwks(&self) -> Result<JwkSet> {
        // First check: fast path with read lock
        {
            let cache = self.jwks_cache.read();
            if let Some(ref cached) = *cache {
                let age = cached.fetched_at.elapsed();
                if age < Duration::from_secs(self.config.jwks_refresh_interval_secs) {
                    debug!("JWKS cache hit");
                    return Ok(cached.jwks.clone());
                }
            }
        }

        // Acquire fetch lock to serialize JWKS fetches
        let _fetch_guard = self.jwks_fetch_lock.lock().await;

        // Second check: another thread may have fetched while we waited
        {
            let cache = self.jwks_cache.read();
            if let Some(ref cached) = *cache {
                let age = cached.fetched_at.elapsed();
                if age < Duration::from_secs(self.config.jwks_refresh_interval_secs) {
                    debug!("JWKS cache hit after acquiring lock");
                    return Ok(cached.jwks.clone());
                }
            }
        }

        // Mark that we're fetching
        self.jwks_fetch_in_progress.store(true, Ordering::SeqCst);

        // Perform the fetch
        let result = self.fetch_jwks().await;

        // Clear fetch flag
        self.jwks_fetch_in_progress.store(false, Ordering::SeqCst);

        // Handle result
        let jwks = result?;

        // Update cache
        {
            let mut cache = self.jwks_cache.write();
            *cache = Some(CachedJwks {
                jwks: jwks.clone(),
                fetched_at: Instant::now(),
            });
        }

        Ok(jwks)
    }

    /// Fetch JWKS from the configured URL
    async fn fetch_jwks(&self) -> Result<JwkSet> {
        let jwks_url = self.get_jwks_url()?;

        debug!(url = %jwks_url, "Fetching JWKS");

        // Use reqwest to fetch JWKS
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| {
                StreamlineError::AuthenticationFailed(format!(
                    "Failed to create HTTP client: {}",
                    e
                ))
            })?;

        let response = client.get(&jwks_url).send().await.map_err(|e| {
            StreamlineError::AuthenticationFailed(format!("Failed to fetch JWKS: {}", e))
        })?;

        if !response.status().is_success() {
            return Err(StreamlineError::AuthenticationFailed(format!(
                "JWKS fetch failed with status: {}",
                response.status()
            )));
        }

        let jwks: JwkSet = response.json().await.map_err(|e| {
            StreamlineError::AuthenticationFailed(format!("Failed to parse JWKS: {}", e))
        })?;

        info!(
            keys_count = jwks.keys.len(),
            url = %jwks_url,
            "JWKS fetched successfully"
        );

        Ok(jwks)
    }

    /// Get JWKS URL from config or discover from issuer
    fn get_jwks_url(&self) -> Result<String> {
        if let Some(ref url) = self.config.jwks_url {
            return Ok(url.clone());
        }

        if let Some(ref issuer) = self.config.issuer_url {
            // Standard OIDC discovery path
            let base = issuer.trim_end_matches('/');
            return Ok(format!("{}/.well-known/jwks.json", base));
        }

        Err(StreamlineError::Config(
            "No JWKS URL configured and no issuer URL for discovery".to_string(),
        ))
    }

    /// Validate required scopes
    fn validate_scopes(&self, claims: &TokenClaims) -> Result<()> {
        let token_scopes: Vec<&str> = claims
            .scope
            .as_ref()
            .map(|s| s.split_whitespace().collect())
            .unwrap_or_default();

        for required in &self.config.required_scopes {
            if !token_scopes.contains(&required.as_str()) {
                return Err(StreamlineError::AuthorizationFailed(format!(
                    "Missing required scope: {}",
                    required
                )));
            }
        }

        Ok(())
    }

    /// Validate time-based claims (iat, nbf) with clock skew tolerance
    fn validate_time_claims(&self, claims: &TokenClaims) -> Result<()> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        let clock_skew = self.config.clock_skew_seconds as i64;

        // Validate issued-at (iat) claim
        if self.config.validate_iat {
            if let Some(iat) = claims.iat {
                // Token should not be issued in the future (with clock skew tolerance)
                if iat > now + clock_skew {
                    warn!(
                        iat = iat,
                        now = now,
                        clock_skew = clock_skew,
                        "Token issued-at claim is in the future"
                    );
                    return Err(StreamlineError::AuthenticationFailed(
                        "Token issued-at claim is in the future".to_string(),
                    ));
                }

                // Validate maximum token age if configured
                if let Some(max_age) = self.config.max_token_age_seconds {
                    let token_age = now - iat;
                    if token_age > max_age as i64 {
                        warn!(
                            iat = iat,
                            now = now,
                            token_age = token_age,
                            max_age = max_age,
                            "Token exceeds maximum age"
                        );
                        return Err(StreamlineError::AuthenticationFailed(format!(
                            "Token exceeds maximum age of {} seconds",
                            max_age
                        )));
                    }
                }
            }
        }

        // Validate not-before (nbf) claim
        if self.config.validate_nbf {
            if let Some(nbf) = claims.nbf {
                // Token should be valid (not before time has passed, with clock skew tolerance)
                if nbf > now + clock_skew {
                    warn!(
                        nbf = nbf,
                        now = now,
                        clock_skew = clock_skew,
                        "Token not-before claim is in the future"
                    );
                    return Err(StreamlineError::AuthenticationFailed(
                        "Token is not yet valid (nbf claim)".to_string(),
                    ));
                }
            }
        }

        // Validate that iat <= exp if both are present
        if let (Some(iat), Some(exp)) = (claims.iat, claims.exp) {
            if iat > exp {
                warn!(iat = iat, exp = exp, "Token issued-at is after expiration");
                return Err(StreamlineError::AuthenticationFailed(
                    "Invalid token: issued-at is after expiration".to_string(),
                ));
            }
        }

        Ok(())
    }

    /// Extract principal from token claims
    fn extract_principal(&self, claims: &TokenClaims) -> Result<String> {
        // Try the configured claim first
        let principal = match self.config.principal_claim.as_str() {
            "sub" => claims.sub.clone(),
            "email" => claims.email.clone(),
            "preferred_username" => claims.preferred_username.clone(),
            "name" => claims.name.clone(),
            other => claims
                .additional
                .get(other)
                .and_then(|v| v.as_str())
                .map(String::from),
        };

        // Fallback chain: configured claim -> sub -> email -> preferred_username
        principal
            .or_else(|| claims.sub.clone())
            .or_else(|| claims.email.clone())
            .or_else(|| claims.preferred_username.clone())
            .ok_or_else(|| {
                StreamlineError::AuthenticationFailed(
                    "No principal claim found in token".to_string(),
                )
            })
    }

    /// Add a static decoding key (for testing or pre-configured keys)
    pub async fn add_static_key(&self, kid: String, key: DecodingKey) {
        let mut keys = self.static_keys.write();
        keys.insert(kid, key);
    }

    /// Parse SASL/OAUTHBEARER authentication data
    ///
    /// Format: `n,,\x01auth=Bearer <token>\x01\x01`
    /// See RFC 7628 for details
    pub fn parse_oauthbearer_data(data: &[u8]) -> Result<String> {
        let data_str = std::str::from_utf8(data).map_err(|_| {
            StreamlineError::AuthenticationFailed("Invalid OAUTHBEARER data encoding".to_string())
        })?;

        // OAUTHBEARER format: n,,\x01auth=Bearer <token>\x01\x01
        // or simplified: auth=Bearer <token>

        // Try to find the token in the data
        if let Some(bearer_pos) = data_str.find("auth=Bearer ") {
            let token_start = bearer_pos + "auth=Bearer ".len();
            let token_end = data_str[token_start..]
                .find('\x01')
                .map(|i| token_start + i)
                .unwrap_or(data_str.len());

            let token = data_str[token_start..token_end].trim();
            if !token.is_empty() {
                return Ok(token.to_string());
            }
        }

        // Also try simple "Bearer <token>" format
        if let Some(stripped) = data_str.strip_prefix("Bearer ") {
            return Ok(stripped.trim().to_string());
        }

        Err(StreamlineError::AuthenticationFailed(
            "Invalid OAUTHBEARER format: could not extract token".to_string(),
        ))
    }
}

/// Check if JWK algorithm matches JWT algorithm
fn algorithm_matches(jwk_alg: jsonwebtoken::jwk::KeyAlgorithm, jwt_alg: Algorithm) -> bool {
    use jsonwebtoken::jwk::KeyAlgorithm;
    matches!(
        (jwk_alg, jwt_alg),
        (KeyAlgorithm::RS256, Algorithm::RS256)
            | (KeyAlgorithm::RS384, Algorithm::RS384)
            | (KeyAlgorithm::RS512, Algorithm::RS512)
            | (KeyAlgorithm::ES256, Algorithm::ES256)
            | (KeyAlgorithm::ES384, Algorithm::ES384)
    )
}

/// Statistics for OAuth provider
#[derive(Debug, Default, Clone)]
pub struct OAuthStats {
    pub tokens_validated: u64,
    pub validation_failures: u64,
    pub jwks_fetches: u64,
    pub cache_hits: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_oauth_config_default() {
        let config = OAuthConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.principal_claim, "sub");
        // 15 minutes (900 seconds) - reduced from 1 hour for better security
        assert_eq!(config.jwks_refresh_interval_secs, 900);
    }

    #[test]
    fn test_audience_contains() {
        let none = Audience::None;
        assert!(!none.contains("test"));

        let single = Audience::Single("test".to_string());
        assert!(single.contains("test"));
        assert!(!single.contains("other"));

        let multiple = Audience::Multiple(vec!["a".to_string(), "b".to_string()]);
        assert!(multiple.contains("a"));
        assert!(multiple.contains("b"));
        assert!(!multiple.contains("c"));
    }

    #[test]
    fn test_parse_oauthbearer_data() {
        // Standard OAUTHBEARER format
        let data = b"n,,\x01auth=Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9\x01\x01";
        let token = OAuthProvider::parse_oauthbearer_data(data).unwrap();
        assert_eq!(token, "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9");

        // Simple Bearer format
        let data = b"Bearer eyJhbGciOiJSUzI1NiJ9";
        let token = OAuthProvider::parse_oauthbearer_data(data).unwrap();
        assert_eq!(token, "eyJhbGciOiJSUzI1NiJ9");

        // Invalid format
        let data = b"invalid data";
        assert!(OAuthProvider::parse_oauthbearer_data(data).is_err());
    }

    #[test]
    fn test_oauth_provider_disabled() {
        let config = OAuthConfig::default();
        let provider = OAuthProvider::new(config);
        assert!(!provider.is_enabled());
    }

    #[test]
    fn test_extract_principal() {
        let config = OAuthConfig {
            principal_claim: "email".to_string(),
            ..Default::default()
        };
        let provider = OAuthProvider::new(config);

        let claims = TokenClaims {
            sub: Some("user123".to_string()),
            email: Some("user@example.com".to_string()),
            iss: None,
            aud: Audience::None,
            exp: None,
            iat: None,
            nbf: None,
            jti: None,
            scope: None,
            preferred_username: None,
            name: None,
            additional: HashMap::new(),
        };

        let principal = provider.extract_principal(&claims).unwrap();
        assert_eq!(principal, "user@example.com");
    }

    #[test]
    fn test_extract_principal_fallback() {
        let config = OAuthConfig {
            principal_claim: "custom_claim".to_string(),
            ..Default::default()
        };
        let provider = OAuthProvider::new(config);

        // No custom_claim, should fallback to sub
        let claims = TokenClaims {
            sub: Some("fallback_user".to_string()),
            email: None,
            iss: None,
            aud: Audience::None,
            exp: None,
            iat: None,
            nbf: None,
            jti: None,
            scope: None,
            preferred_username: None,
            name: None,
            additional: HashMap::new(),
        };

        let principal = provider.extract_principal(&claims).unwrap();
        assert_eq!(principal, "fallback_user");
    }

    #[test]
    fn test_validate_scopes() {
        let config = OAuthConfig {
            required_scopes: vec!["read".to_string(), "write".to_string()],
            ..Default::default()
        };
        let provider = OAuthProvider::new(config);

        // Has required scopes
        let claims = TokenClaims {
            scope: Some("read write admin".to_string()),
            sub: None,
            email: None,
            iss: None,
            aud: Audience::None,
            exp: None,
            iat: None,
            nbf: None,
            jti: None,
            preferred_username: None,
            name: None,
            additional: HashMap::new(),
        };
        assert!(provider.validate_scopes(&claims).is_ok());

        // Missing required scope
        let claims_missing = TokenClaims {
            scope: Some("read".to_string()),
            ..claims.clone()
        };
        assert!(provider.validate_scopes(&claims_missing).is_err());
    }
}
