use std::net::SocketAddr;

/// Stub Operation enum for lite builds
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    Read,
    Write,
    Create,
    Delete,
    Describe,
    Alter,
    AlterConfigs,
    DescribeConfigs,
    ClusterAction,
    IdempotentWrite,
    All,
}

impl Operation {
    pub fn from_code(_code: i8) -> Option<Self> {
        Some(Self::All)
    }

    pub fn to_code(self) -> i8 {
        0
    }
}

/// Stub ResourceType enum for lite builds
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceType {
    Unknown,
    Any,
    Topic,
    Group,
    Cluster,
    TransactionalId,
    DelegationToken,
}

impl ResourceType {
    pub fn from_code(_code: i8) -> Option<Self> {
        Some(Self::Any)
    }

    pub fn to_code(self) -> i8 {
        0
    }
}

/// Stub PatternType enum for lite builds
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PatternType {
    Unknown,
    Any,
    Match,
    Literal,
    Prefixed,
}

impl PatternType {
    pub fn from_code(_code: i8) -> Option<Self> {
        Some(Self::Any)
    }

    pub fn to_code(self) -> i8 {
        0
    }
}

/// Stub Permission enum for lite builds
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Permission {
    Unknown,
    Any,
    Deny,
    Allow,
}

impl Permission {
    pub fn from_code(_code: i8) -> Option<Self> {
        Some(Self::Allow)
    }

    pub fn to_code(self) -> i8 {
        3 // Allow
    }
}

/// Stub SaslMechanism enum for lite builds
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SaslMechanism {
    Plain,
    ScramSha256,
    ScramSha512,
    OAuthBearer,
}

impl SaslMechanism {
    pub fn from_str(_s: &str) -> Option<Self> {
        None
    }

    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "PLAIN" => Some(Self::Plain),
            "SCRAM-SHA-256" => Some(Self::ScramSha256),
            "SCRAM-SHA-512" => Some(Self::ScramSha512),
            "OAUTHBEARER" => Some(Self::OAuthBearer),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Plain => "PLAIN",
            Self::ScramSha256 => "SCRAM-SHA-256",
            Self::ScramSha512 => "SCRAM-SHA-512",
            Self::OAuthBearer => "OAUTHBEARER",
        }
    }
}

/// Stub User for lite builds
#[derive(Debug, Clone)]
pub struct User {
    pub username: String,
}

/// Stub AuthSession for lite builds
#[derive(Debug, Clone)]
pub struct AuthSession {
    pub user: User,
}

impl AuthSession {
    pub fn new(username: String) -> Self {
        Self {
            user: User { username },
        }
    }
}

/// Stub SessionManager for lite builds without auth
pub struct SessionManager {
    peer_addr: Option<SocketAddr>,
    is_tls: bool,
    #[cfg(unix)]
    socket_fd: Option<crate::protocol::connection::RawFd>,
}

impl SessionManager {
    pub fn with_peer_addr_and_tls(peer_addr: Option<SocketAddr>, is_tls: bool) -> Self {
        Self {
            peer_addr,
            is_tls,
            #[cfg(unix)]
            socket_fd: None,
        }
    }

    #[cfg(unix)]
    pub fn with_peer_addr_tls_and_fd(
        peer_addr: Option<SocketAddr>,
        is_tls: bool,
        socket_fd: Option<crate::protocol::connection::RawFd>,
    ) -> Self {
        Self {
            peer_addr,
            is_tls,
            socket_fd,
        }
    }

    pub fn peer_addr(&self) -> Option<SocketAddr> {
        self.peer_addr
    }

    pub fn peer_addr_string(&self) -> String {
        self.peer_addr
            .map(|a| a.to_string())
            .unwrap_or_else(|| "unknown".to_string())
    }

    pub fn is_tls(&self) -> bool {
        self.is_tls
    }

    #[cfg(unix)]
    pub fn socket_fd(&self) -> Option<crate::protocol::connection::RawFd> {
        self.socket_fd
    }

    #[cfg(not(unix))]
    pub fn socket_fd(&self) -> Option<i32> {
        None
    }

    pub fn is_authenticated(&self) -> bool {
        false // Always not authenticated in lite builds
    }

    pub async fn get_session(&self) -> Option<AuthSession> {
        None
    }

    pub fn get_sasl_session(&self) -> Option<SaslSession> {
        None
    }

    pub fn update_sasl_session(&self, _session: SaslSession) {}

    pub fn set_session(&self, _session: AuthSession) {}
}

/// Stub SaslSession for lite builds
#[derive(Debug, Clone)]
pub struct SaslSession {
    pub mechanism: Option<SaslMechanism>,
    pub state: SaslState,
}

/// Stub SaslState for lite builds
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SaslState {
    Initial,
    InProgress,
    Complete,
    Failed,
}

/// Stub Acl for lite builds
#[derive(Debug, Clone)]
pub struct Acl {
    pub resource_type: ResourceType,
    pub resource_name: String,
    pub pattern_type: PatternType,
    pub principal: String,
    pub host: String,
    pub operation: Operation,
    pub permission: Permission,
}

impl Acl {
    pub fn new(
        resource_type: ResourceType,
        resource_name: String,
        pattern_type: PatternType,
        principal: String,
        host: String,
        operation: Operation,
        permission: Permission,
    ) -> Self {
        Self {
            resource_type,
            resource_name,
            pattern_type,
            principal,
            host,
            operation,
            permission,
        }
    }
}

/// Stub AclFilter for lite builds
#[derive(Debug, Clone, Default)]
pub struct AclFilter {
    pub resource_type: Option<ResourceType>,
    pub resource_name: Option<String>,
    pub pattern_type: Option<PatternType>,
    pub principal: Option<String>,
    pub host: Option<String>,
    pub operation: Option<Operation>,
    pub permission: Option<Permission>,
}

impl AclFilter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_resource_type(mut self, resource_type: ResourceType) -> Self {
        self.resource_type = Some(resource_type);
        self
    }

    pub fn with_resource_name(mut self, name: impl Into<String>) -> Self {
        self.resource_name = Some(name.into());
        self
    }

    pub fn with_pattern_type(mut self, pattern_type: PatternType) -> Self {
        self.pattern_type = Some(pattern_type);
        self
    }

    pub fn with_principal(mut self, principal: impl Into<String>) -> Self {
        self.principal = Some(principal.into());
        self
    }

    pub fn with_host(mut self, host: impl Into<String>) -> Self {
        self.host = Some(host.into());
        self
    }

    pub fn with_operation(mut self, operation: Operation) -> Self {
        self.operation = Some(operation);
        self
    }

    pub fn with_permission(mut self, permission: Permission) -> Self {
        self.permission = Some(permission);
        self
    }
}

/// Stub ScramAuthenticator for lite builds
#[derive(Debug)]
pub struct ScramAuthenticator;

/// Stub SaslPlainAuthenticator for lite builds
#[derive(Debug)]
pub struct SaslPlainAuthenticator;

/// Stub UserStore for lite builds
#[derive(Debug, Clone)]
pub struct UserStore;

impl UserStore {
    pub fn get_user(&self, _username: &str) -> Option<User> {
        None
    }
}
