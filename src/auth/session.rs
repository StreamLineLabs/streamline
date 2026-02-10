//! Session management for authenticated connections

use crate::auth::oauth::OAuthConfig;
use crate::auth::sasl::SaslMechanism;
use crate::auth::User;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::debug;
use uuid::Uuid;

/// Default session timeout duration (30 minutes)
pub const DEFAULT_SESSION_TIMEOUT: Duration = Duration::from_secs(30 * 60);

/// Minimum allowed session timeout (1 minute)
pub const MIN_SESSION_TIMEOUT: Duration = Duration::from_secs(60);

/// Maximum allowed session timeout (24 hours)
pub const MAX_SESSION_TIMEOUT: Duration = Duration::from_secs(24 * 60 * 60);

/// Authenticated session
#[derive(Debug, Clone)]
pub struct AuthSession {
    /// Session ID
    pub session_id: String,

    /// Authenticated user
    pub user: User,

    /// When the session was created
    created_at: Instant,

    /// Session timeout duration
    timeout: Duration,

    /// Last activity timestamp (updated on each request)
    last_activity: Instant,
}

impl AuthSession {
    /// Create a new authenticated session with default timeout
    pub fn new(user: User) -> Self {
        let now = Instant::now();
        Self {
            session_id: Uuid::new_v4().to_string(),
            user,
            created_at: now,
            timeout: DEFAULT_SESSION_TIMEOUT,
            last_activity: now,
        }
    }

    /// Create a new authenticated session with custom timeout
    pub fn with_timeout(user: User, timeout: Duration) -> Self {
        // Clamp timeout to valid range
        let timeout = timeout.clamp(MIN_SESSION_TIMEOUT, MAX_SESSION_TIMEOUT);
        let now = Instant::now();
        Self {
            session_id: Uuid::new_v4().to_string(),
            user,
            created_at: now,
            timeout,
            last_activity: now,
        }
    }

    /// Check if user has permission
    pub fn has_permission(&self, permission: &str) -> bool {
        self.user.has_permission(permission)
    }

    /// Check if the session has expired (either absolute timeout or inactivity)
    pub fn is_expired(&self) -> bool {
        let now = Instant::now();

        // Check absolute session timeout (from creation)
        if now.duration_since(self.created_at) > self.timeout {
            debug!(
                session_id = %self.session_id,
                user = %self.user.username,
                "Session expired (absolute timeout)"
            );
            return true;
        }

        // Check inactivity timeout (half of session timeout)
        let inactivity_timeout = self.timeout / 2;
        if now.duration_since(self.last_activity) > inactivity_timeout {
            debug!(
                session_id = %self.session_id,
                user = %self.user.username,
                "Session expired (inactivity)"
            );
            return true;
        }

        false
    }

    /// Update last activity timestamp (call on each authenticated request)
    pub fn touch(&mut self) {
        self.last_activity = Instant::now();
    }

    /// Get the session age
    pub fn age(&self) -> Duration {
        self.created_at.elapsed()
    }

    /// Get time since last activity
    pub fn idle_time(&self) -> Duration {
        self.last_activity.elapsed()
    }

    /// Get remaining time until session expires
    pub fn remaining_time(&self) -> Duration {
        let age = self.created_at.elapsed();
        if age >= self.timeout {
            Duration::ZERO
        } else {
            self.timeout - age
        }
    }
}

/// Channel binding type for SCRAM authentication (RFC 5929)
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChannelBindingType {
    /// No channel binding ("n" flag)
    None,
    /// Client supports channel binding but not required ("y" flag)
    Supported,
    /// tls-server-end-point channel binding (hash of server certificate)
    TlsServerEndPoint,
    /// tls-unique channel binding (TLS Finished message - not commonly supported)
    TlsUnique,
    /// tls-exporter channel binding (RFC 9266 - TLS 1.3)
    TlsExporter,
}

impl ChannelBindingType {
    /// Get the GS2 flag for this channel binding type
    pub fn gs2_flag(&self) -> &'static str {
        match self {
            ChannelBindingType::None => "n",
            ChannelBindingType::Supported => "y",
            ChannelBindingType::TlsServerEndPoint => "p=tls-server-end-point",
            ChannelBindingType::TlsUnique => "p=tls-unique",
            ChannelBindingType::TlsExporter => "p=tls-exporter",
        }
    }

    /// Parse from GS2 flag string
    pub fn from_gs2_flag(flag: &str) -> Option<Self> {
        match flag {
            "n" => Some(ChannelBindingType::None),
            "y" => Some(ChannelBindingType::Supported),
            "p=tls-server-end-point" => Some(ChannelBindingType::TlsServerEndPoint),
            "p=tls-unique" => Some(ChannelBindingType::TlsUnique),
            "p=tls-exporter" => Some(ChannelBindingType::TlsExporter),
            _ => None,
        }
    }

    /// Check if this type requires channel binding data
    pub fn requires_binding_data(&self) -> bool {
        matches!(
            self,
            ChannelBindingType::TlsServerEndPoint
                | ChannelBindingType::TlsUnique
                | ChannelBindingType::TlsExporter
        )
    }
}

/// SCRAM authentication state for multi-step handshake
#[derive(Debug, Clone)]
pub struct ScramState {
    /// The username being authenticated
    pub username: String,

    /// Client nonce from client-first message
    pub client_nonce: String,

    /// Combined server nonce (client_nonce + server_nonce)
    pub server_nonce: String,

    /// Salt for this user (base64)
    pub salt: String,

    /// Iteration count for PBKDF2
    pub iterations: u32,

    /// The auth message for signature verification
    /// auth_message = client-first-message-bare + "," + server-first-message + "," + client-final-message-without-proof
    pub auth_message: String,

    /// The client-first-message-bare (needed for auth_message construction)
    pub client_first_bare: String,

    /// The server-first-message (needed for auth_message construction)
    pub server_first: String,

    /// Channel binding type requested by client
    pub channel_binding_type: ChannelBindingType,

    /// Server's channel binding data (e.g., hash of server certificate)
    /// This is compared against what the client sends
    pub channel_binding_data: Option<Vec<u8>>,
}

/// SASL session state for tracking mechanism negotiation and handshake progress
#[derive(Debug, Clone)]
pub struct SaslSession {
    /// The negotiated SASL mechanism (set during SaslHandshake)
    pub mechanism: Option<SaslMechanism>,

    /// SCRAM handshake state (for multi-step SCRAM authentication)
    pub scram_state: Option<ScramState>,

    /// OAuth configuration for OAUTHBEARER authentication
    pub oauth_config: Option<OAuthConfig>,
}

impl SaslSession {
    /// Create a new SASL session
    pub fn new() -> Self {
        Self {
            mechanism: None,
            scram_state: None,
            oauth_config: None,
        }
    }

    /// Create a new SASL session with OAuth configuration
    pub fn with_oauth_config(oauth_config: OAuthConfig) -> Self {
        Self {
            mechanism: None,
            scram_state: None,
            oauth_config: Some(oauth_config),
        }
    }

    /// Set the negotiated mechanism
    pub fn set_mechanism(&mut self, mechanism: SaslMechanism) {
        self.mechanism = Some(mechanism);
    }

    /// Get the negotiated mechanism
    pub fn get_mechanism(&self) -> Option<SaslMechanism> {
        self.mechanism
    }

    /// Set the SCRAM state
    pub fn set_scram_state(&mut self, state: ScramState) {
        self.scram_state = Some(state);
    }

    /// Take the SCRAM state (consumes it)
    pub fn take_scram_state(&mut self) -> Option<ScramState> {
        self.scram_state.take()
    }

    /// Check if we're in the middle of a SCRAM handshake
    pub fn is_scram_in_progress(&self) -> bool {
        self.scram_state.is_some()
    }

    /// Clear the SASL session
    pub fn clear(&mut self) {
        self.mechanism = None;
        self.scram_state = None;
    }
}

impl Default for SaslSession {
    fn default() -> Self {
        Self::new()
    }
}

/// Session manager for tracking authenticated connections
#[derive(Debug, Clone)]
pub struct SessionManager {
    /// Current authenticated session (one per connection)
    session: Arc<RwLock<Option<AuthSession>>>,

    /// SASL session state (for tracking mechanism and SCRAM handshake)
    sasl_session: Arc<RwLock<SaslSession>>,

    /// Peer address of the connection (for ACL host-based checks)
    peer_addr: Option<std::net::SocketAddr>,

    /// Whether the connection is over TLS (important for SASL/PLAIN security)
    is_tls: bool,

    /// Channel binding data for SCRAM authentication (tls-server-end-point)
    /// This is the SHA-256 hash of the server's certificate
    channel_binding_data: Option<Vec<u8>>,
}

impl SessionManager {
    /// Create a new session manager
    pub fn new() -> Self {
        Self {
            session: Arc::new(RwLock::new(None)),
            sasl_session: Arc::new(RwLock::new(SaslSession::new())),
            peer_addr: None,
            is_tls: false,
            channel_binding_data: None,
        }
    }

    /// Create a new session manager with peer address
    pub fn with_peer_addr(peer_addr: Option<std::net::SocketAddr>) -> Self {
        Self {
            session: Arc::new(RwLock::new(None)),
            sasl_session: Arc::new(RwLock::new(SaslSession::new())),
            peer_addr,
            is_tls: false,
            channel_binding_data: None,
        }
    }

    /// Create a new session manager with peer address and TLS status
    pub fn with_peer_addr_and_tls(peer_addr: Option<std::net::SocketAddr>, is_tls: bool) -> Self {
        Self {
            session: Arc::new(RwLock::new(None)),
            sasl_session: Arc::new(RwLock::new(SaslSession::new())),
            peer_addr,
            is_tls,
            channel_binding_data: None,
        }
    }

    /// Create a new session manager with peer address, TLS status, and channel binding data
    ///
    /// The channel binding data is typically the SHA-256 hash of the server certificate
    /// for tls-server-end-point binding (RFC 5929).
    pub fn with_channel_binding(
        peer_addr: Option<std::net::SocketAddr>,
        is_tls: bool,
        channel_binding_data: Option<Vec<u8>>,
    ) -> Self {
        Self {
            session: Arc::new(RwLock::new(None)),
            sasl_session: Arc::new(RwLock::new(SaslSession::new())),
            peer_addr,
            is_tls,
            channel_binding_data,
        }
    }

    /// Check if the connection is over TLS
    pub fn is_tls(&self) -> bool {
        self.is_tls
    }

    /// Get channel binding data for SCRAM authentication
    pub fn channel_binding_data(&self) -> Option<&[u8]> {
        self.channel_binding_data.as_deref()
    }

    /// Check if channel binding is available
    pub fn supports_channel_binding(&self) -> bool {
        self.is_tls && self.channel_binding_data.is_some()
    }

    /// Get the peer address as a string for ACL checks
    ///
    /// Returns the IP address of the peer, or "*" if not available.
    /// This is used for host-based ACL rules.
    pub fn peer_addr_string(&self) -> String {
        self.peer_addr
            .map(|addr| addr.ip().to_string())
            .unwrap_or_else(|| "*".to_string())
    }

    /// Get the raw peer address
    pub fn peer_addr(&self) -> Option<std::net::SocketAddr> {
        self.peer_addr
    }

    /// Set the authenticated session
    pub async fn set_session(&self, session: AuthSession) {
        let mut s = self.session.write().await;
        *s = Some(session);
    }

    /// Get the current session (returns None if expired)
    pub async fn get_session(&self) -> Option<AuthSession> {
        let session = self.session.read().await.clone();
        match &session {
            Some(s) if s.is_expired() => {
                // Session is expired, clear it
                drop(session);
                self.clear_session().await;
                None
            }
            _ => session,
        }
    }

    /// Get the current session without checking expiration (for internal use)
    pub async fn get_session_unchecked(&self) -> Option<AuthSession> {
        self.session.read().await.clone()
    }

    /// Check if authenticated (also checks session expiration)
    pub async fn is_authenticated(&self) -> bool {
        self.get_session().await.is_some()
    }

    /// Update the session's last activity timestamp
    pub async fn touch_session(&self) {
        let mut s = self.session.write().await;
        if let Some(ref mut session) = *s {
            session.touch();
        }
    }

    /// Validate session and update activity. Returns None if session is expired or missing.
    pub async fn validate_and_touch(&self) -> Option<AuthSession> {
        let mut s = self.session.write().await;
        if let Some(ref mut session) = *s {
            if session.is_expired() {
                debug!(
                    session_id = %session.session_id,
                    user = %session.user.username,
                    "Clearing expired session"
                );
                *s = None;
                None
            } else {
                session.touch();
                Some(session.clone())
            }
        } else {
            None
        }
    }

    /// Clear the session
    pub async fn clear_session(&self) {
        let mut s = self.session.write().await;
        *s = None;
    }

    /// Get SASL session for reading
    pub async fn get_sasl_session(&self) -> SaslSession {
        self.sasl_session.read().await.clone()
    }

    /// Update SASL session
    pub async fn update_sasl_session<F>(&self, f: F)
    where
        F: FnOnce(&mut SaslSession),
    {
        let mut session = self.sasl_session.write().await;
        f(&mut session);
    }

    /// Clear SASL session
    pub async fn clear_sasl_session(&self) {
        let mut session = self.sasl_session.write().await;
        session.clear();
    }
}

impl Default for SessionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_session_manager() {
        let manager = SessionManager::new();

        assert!(!manager.is_authenticated().await);

        let user = User {
            username: "testuser".to_string(),
            password_hash: "hash".to_string(),
            permissions: vec!["admin".to_string()],
            scram_sha256: None,
            scram_sha512: None,
        };

        let session = AuthSession::new(user);
        manager.set_session(session.clone()).await;

        assert!(manager.is_authenticated().await);

        let retrieved = manager.get_session().await.unwrap();
        assert_eq!(retrieved.user.username, "testuser");

        manager.clear_session().await;
        assert!(!manager.is_authenticated().await);
    }

    #[tokio::test]
    async fn test_session_permissions() {
        let user = User {
            username: "testuser".to_string(),
            password_hash: "hash".to_string(),
            permissions: vec!["produce:*".to_string()],
            scram_sha256: None,
            scram_sha512: None,
        };

        let session = AuthSession::new(user);

        assert!(session.has_permission("produce:orders"));
        assert!(!session.has_permission("consume:orders"));
    }

    #[tokio::test]
    async fn test_sasl_session() {
        let manager = SessionManager::new();

        // Initially no mechanism set
        let sasl = manager.get_sasl_session().await;
        assert!(sasl.mechanism.is_none());
        assert!(!sasl.is_scram_in_progress());

        // Set mechanism
        manager
            .update_sasl_session(|s| s.set_mechanism(SaslMechanism::ScramSha256))
            .await;

        let sasl = manager.get_sasl_session().await;
        assert_eq!(sasl.mechanism, Some(SaslMechanism::ScramSha256));

        // Set SCRAM state
        let scram_state = ScramState {
            username: "testuser".to_string(),
            client_nonce: "client123".to_string(),
            server_nonce: "client123server456".to_string(),
            salt: "c2FsdA==".to_string(),
            iterations: 4096,
            auth_message: String::new(),
            client_first_bare: "n=testuser,r=client123".to_string(),
            server_first: "r=client123server456,s=c2FsdA==,i=4096".to_string(),
            channel_binding_type: ChannelBindingType::None,
            channel_binding_data: None,
        };

        manager
            .update_sasl_session(|s| s.set_scram_state(scram_state))
            .await;

        let sasl = manager.get_sasl_session().await;
        assert!(sasl.is_scram_in_progress());

        // Clear SASL session
        manager.clear_sasl_session().await;
        let sasl = manager.get_sasl_session().await;
        assert!(sasl.mechanism.is_none());
        assert!(!sasl.is_scram_in_progress());
    }

    #[tokio::test]
    async fn test_peer_addr() {
        use std::net::{IpAddr, Ipv4Addr, SocketAddr};

        // Test without peer address
        let manager = SessionManager::new();
        assert_eq!(manager.peer_addr_string(), "*");
        assert!(manager.peer_addr().is_none());

        // Test with peer address
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100)), 12345);
        let manager = SessionManager::with_peer_addr(Some(addr));
        assert_eq!(manager.peer_addr_string(), "192.168.1.100");
        assert_eq!(manager.peer_addr(), Some(addr));

        // Test with localhost
        let localhost = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9092);
        let manager = SessionManager::with_peer_addr(Some(localhost));
        assert_eq!(manager.peer_addr_string(), "127.0.0.1");
    }

    #[test]
    fn test_session_timeout_bounds() {
        let user = User {
            username: "testuser".to_string(),
            password_hash: "hash".to_string(),
            permissions: vec![],
            scram_sha256: None,
            scram_sha512: None,
        };

        // Test that timeout is clamped to minimum
        let session = AuthSession::with_timeout(user.clone(), Duration::from_secs(1));
        assert!(!session.is_expired()); // Should have MIN_SESSION_TIMEOUT

        // Test that timeout is clamped to maximum
        let session = AuthSession::with_timeout(user, Duration::from_secs(365 * 24 * 60 * 60));
        assert!(!session.is_expired()); // Should have MAX_SESSION_TIMEOUT
    }

    #[test]
    fn test_session_touch() {
        let user = User {
            username: "testuser".to_string(),
            password_hash: "hash".to_string(),
            permissions: vec![],
            scram_sha256: None,
            scram_sha512: None,
        };

        let mut session = AuthSession::new(user);
        let initial_idle = session.idle_time();

        // After touch, idle time should be reset
        std::thread::sleep(Duration::from_millis(10));
        session.touch();
        assert!(
            session.idle_time() < initial_idle || session.idle_time() < Duration::from_millis(5)
        );
    }

    #[test]
    fn test_session_remaining_time() {
        let user = User {
            username: "testuser".to_string(),
            password_hash: "hash".to_string(),
            permissions: vec![],
            scram_sha256: None,
            scram_sha512: None,
        };

        let session = AuthSession::new(user);

        // Remaining time should be close to DEFAULT_SESSION_TIMEOUT
        let remaining = session.remaining_time();
        assert!(remaining > Duration::ZERO);
        assert!(remaining <= DEFAULT_SESSION_TIMEOUT);
    }

    #[tokio::test]
    async fn test_validate_and_touch() {
        let manager = SessionManager::new();

        let user = User {
            username: "testuser".to_string(),
            password_hash: "hash".to_string(),
            permissions: vec![],
            scram_sha256: None,
            scram_sha512: None,
        };

        let session = AuthSession::new(user);
        manager.set_session(session).await;

        // validate_and_touch should return the session and update activity
        let result = manager.validate_and_touch().await;
        assert!(result.is_some());
        assert_eq!(result.unwrap().user.username, "testuser");

        // After clearing, validate_and_touch should return None
        manager.clear_session().await;
        let result = manager.validate_and_touch().await;
        assert!(result.is_none());
    }
}
