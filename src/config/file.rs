//! Configuration file support for Streamline
//!
//! This module provides TOML configuration file parsing and merging with CLI arguments.
//!
//! ## Priority Order
//!
//! Configuration is loaded with the following priority (highest to lowest):
//! 1. Command-line arguments
//! 2. Environment variables
//! 3. Configuration file
//! 4. Default values
//!
//! ## Example Configuration
//!
//! ```toml
//! # streamline.toml
//!
//! [server]
//! listen_addr = "0.0.0.0:9092"
//! http_addr = "0.0.0.0:9094"
//! data_dir = "/var/lib/streamline"
//! log_level = "info"
//!
//! [storage]
//! segment_max_bytes = 1073741824  # 1GB
//! max_message_bytes = 1048576     # 1MB
//! in_memory = false
//!
//! [wal]
//! enabled = true
//! sync_mode = "interval"
//! sync_ms = 100
//!
//! [tls]
//! enabled = false
//! # cert = "/path/to/cert.pem"
//! # key = "/path/to/key.pem"
//!
//! [auth]
//! enabled = false
//! # sasl_mechanisms = ["PLAIN", "SCRAM-SHA-256"]
//!
//! [cluster]
//! # node_id = 1
//! # seed_nodes = ["127.0.0.1:9093"]
//! ```

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use crate::error::{Result, StreamlineError};

/// Root configuration structure for TOML file
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ConfigFile {
    /// Server configuration
    pub server: ServerSection,

    /// Storage configuration
    pub storage: StorageSection,

    /// Write-ahead log configuration
    pub wal: WalSection,

    /// TLS/SSL configuration
    pub tls: TlsSection,

    /// Authentication configuration
    pub auth: AuthSection,

    /// OAuth 2.0 / OIDC configuration
    pub oauth: OAuthSection,

    /// ACL authorization configuration
    pub acl: AclSection,

    /// Cluster configuration
    pub cluster: ClusterSection,

    /// Inter-broker TLS configuration
    pub inter_broker_tls: InterBrokerTlsSection,

    /// Audit logging configuration
    pub audit: AuditSection,

    /// Resource limits configuration
    pub limits: LimitsSection,

    /// Client quota configuration
    pub quotas: QuotasSection,

    /// Simple protocol configuration
    pub simple: SimpleSection,

    /// Encryption at rest configuration
    pub encryption: EncryptionSection,

    /// Zero-copy networking configuration
    pub zerocopy: ZeroCopySection,

    /// OpenTelemetry tracing configuration
    pub otel: OtelSection,
}

/// Server section configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerSection {
    /// Kafka protocol listen address
    pub listen_addr: Option<String>,

    /// HTTP API listen address
    pub http_addr: Option<String>,

    /// Data directory path
    pub data_dir: Option<PathBuf>,

    /// Log level (trace, debug, info, warn, error)
    pub log_level: Option<String>,

    /// Enable automatic topic creation
    pub auto_create_topics: Option<bool>,

    /// Enable playground mode
    pub playground: Option<bool>,

    /// Graceful shutdown timeout in seconds
    pub shutdown_timeout: Option<u64>,

    /// Connection drain timeout in seconds
    pub drain_timeout: Option<u64>,
}

/// Storage section configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct StorageSection {
    /// Maximum segment size in bytes
    pub segment_max_bytes: Option<u64>,

    /// Maximum message size in bytes
    pub max_message_bytes: Option<u64>,

    /// Maximum index entries per segment
    pub max_index_entries: Option<usize>,

    /// Enable in-memory mode
    pub in_memory: Option<bool>,
}

/// WAL section configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct WalSection {
    /// Enable WAL
    pub enabled: Option<bool>,

    /// Sync interval in milliseconds
    pub sync_ms: Option<u64>,

    /// Sync mode (every_write, interval, os_default)
    pub sync_mode: Option<String>,

    /// Maximum file size before rotation
    pub max_file_size: Option<u64>,

    /// Number of files to retain
    pub retention_files: Option<u32>,

    /// Maximum pending writes before backpressure
    pub max_pending_writes: Option<usize>,
}

/// TLS section configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct TlsSection {
    /// Enable TLS
    pub enabled: Option<bool>,

    /// Certificate file path
    pub cert: Option<PathBuf>,

    /// Private key file path
    pub key: Option<PathBuf>,

    /// Minimum TLS version
    pub min_version: Option<String>,

    /// Require client certificate (mTLS)
    pub require_client_cert: Option<bool>,

    /// CA certificate for client verification
    pub ca_cert: Option<PathBuf>,
}

/// Authentication section configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct AuthSection {
    /// Enable SASL authentication
    pub enabled: Option<bool>,

    /// SASL mechanisms
    pub sasl_mechanisms: Option<Vec<String>>,

    /// Users file path
    pub users_file: Option<PathBuf>,

    /// Allow anonymous connections
    pub allow_anonymous: Option<bool>,
}

/// OAuth section configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct OAuthSection {
    /// Enable OAuth
    pub enabled: Option<bool>,

    /// OIDC issuer URL
    pub issuer_url: Option<String>,

    /// JWKS URL
    pub jwks_url: Option<String>,

    /// Expected audience claim
    pub audience: Option<String>,

    /// Principal claim name
    pub principal_claim: Option<String>,

    /// Required scopes
    pub required_scopes: Option<Vec<String>>,

    /// JWKS refresh interval in seconds
    pub jwks_refresh_interval: Option<u64>,
}

/// ACL section configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct AclSection {
    /// Enable ACL authorization
    pub enabled: Option<bool>,

    /// ACL file path
    pub file: Option<PathBuf>,

    /// Super users
    pub super_users: Option<Vec<String>>,

    /// Allow if no ACLs configured
    pub allow_if_no_acls: Option<bool>,
}

/// Cluster section configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ClusterSection {
    /// Node ID
    pub node_id: Option<u64>,

    /// Advertised address
    pub advertised_addr: Option<String>,

    /// Inter-broker address
    pub inter_broker_addr: Option<String>,

    /// Seed nodes
    pub seed_nodes: Option<Vec<String>>,

    /// Default replication factor
    pub default_replication_factor: Option<i16>,

    /// Minimum in-sync replicas
    pub min_insync_replicas: Option<i16>,

    /// Rack ID
    pub rack_id: Option<String>,

    /// Disable rack-aware assignment
    pub disable_rack_aware_assignment: Option<bool>,
}

/// Inter-broker TLS section configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct InterBrokerTlsSection {
    /// Enable inter-broker TLS
    pub enabled: Option<bool>,

    /// Certificate file
    pub cert: Option<PathBuf>,

    /// Private key file
    pub key: Option<PathBuf>,

    /// CA certificate
    pub ca: Option<PathBuf>,

    /// Verify peer certificates
    pub verify_peer: Option<bool>,
}

/// Audit section configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct AuditSection {
    /// Enable audit logging
    pub enabled: Option<bool>,

    /// Log file path
    pub log_path: Option<PathBuf>,

    /// Log format (json, text)
    pub log_format: Option<String>,

    /// Events to log
    pub events: Option<Vec<String>>,

    /// Also log to stdout
    pub log_to_stdout: Option<bool>,
}

/// Limits section configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct LimitsSection {
    /// Maximum total connections
    pub max_connections: Option<u32>,

    /// Maximum connections per IP
    pub max_connections_per_ip: Option<u32>,

    /// Maximum request size in bytes
    pub max_request_size: Option<u64>,

    /// Connection idle timeout in seconds
    pub connection_idle_timeout: Option<u64>,

    /// Produce rate limit in bytes/second
    pub produce_rate_limit: Option<u64>,
}

/// Quotas section configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct QuotasSection {
    /// Enable quotas
    pub enabled: Option<bool>,

    /// Producer byte rate
    pub producer_byte_rate: Option<u64>,

    /// Consumer byte rate
    pub consumer_byte_rate: Option<u64>,
}

/// Simple protocol section configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SimpleSection {
    /// Enable simple protocol
    pub enabled: Option<bool>,

    /// Listen address
    pub addr: Option<String>,
}

/// Encryption section configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct EncryptionSection {
    /// Enable encryption at rest
    pub enabled: Option<bool>,

    /// Key file path
    pub key_file: Option<PathBuf>,

    /// Environment variable containing key
    pub key_env_var: Option<String>,
}

/// Zero-copy networking section configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ZeroCopySection {
    /// Enable zero-copy
    pub enabled: Option<bool>,

    /// Enable mmap
    pub mmap: Option<bool>,

    /// Mmap threshold in bytes
    pub mmap_threshold: Option<u64>,

    /// Buffer pool size
    pub buffer_pool_size: Option<usize>,

    /// Enable sendfile
    pub sendfile: Option<bool>,

    /// Enable response cache
    pub response_cache: Option<bool>,

    /// Cache size in MB
    pub cache_size_mb: Option<u64>,
}

/// OpenTelemetry section configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct OtelSection {
    /// Enable OpenTelemetry
    pub enabled: Option<bool>,

    /// OTLP endpoint
    pub endpoint: Option<String>,

    /// Service name
    pub service_name: Option<String>,

    /// Sampling ratio
    pub sampling_ratio: Option<f64>,

    /// Export timeout in seconds
    pub timeout_secs: Option<u64>,
}

impl ConfigFile {
    /// Load configuration from a TOML file
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let contents = std::fs::read_to_string(path).map_err(|e| {
            StreamlineError::Config(format!("Failed to read config file {:?}: {}", path, e))
        })?;

        toml::from_str(&contents).map_err(|e| {
            StreamlineError::Config(format!("Failed to parse config file {:?}: {}", path, e))
        })
    }

    /// Try to load configuration from default locations
    ///
    /// Searches in order:
    /// 1. ./streamline.toml
    /// 2. /etc/streamline/streamline.toml
    /// 3. ~/.config/streamline/streamline.toml
    pub fn load_default() -> Option<Self> {
        let default_paths = [
            PathBuf::from("streamline.toml"),
            PathBuf::from("/etc/streamline/streamline.toml"),
            dirs::config_dir()
                .map(|p| p.join("streamline/streamline.toml"))
                .unwrap_or_default(),
        ];

        for path in default_paths.iter().filter(|p| !p.as_os_str().is_empty()) {
            if path.exists() {
                match Self::load(path) {
                    Ok(config) => {
                        tracing::info!("Loaded configuration from {:?}", path);
                        return Some(config);
                    }
                    Err(e) => {
                        tracing::warn!("Failed to load config from {:?}: {}", path, e);
                    }
                }
            }
        }

        None
    }

    /// Generate an example configuration file
    pub fn generate_example() -> String {
        r#"# Streamline Configuration File
# Copy to streamline.toml and customize as needed
#
# Configuration priority (highest to lowest):
# 1. Command-line arguments
# 2. Environment variables
# 3. This configuration file
# 4. Default values

[server]
# Kafka protocol listen address
listen_addr = "0.0.0.0:9092"

# HTTP API listen address
http_addr = "0.0.0.0:9094"

# Data directory for storage
data_dir = "./data"

# Log level (trace, debug, info, warn, error)
log_level = "info"

# Enable automatic topic creation on produce
# Disable in production to prevent accidental topic creation
auto_create_topics = true

# Graceful shutdown timeout in seconds
shutdown_timeout = 30

# Connection drain timeout in seconds
drain_timeout = 10

[storage]
# Maximum segment size in bytes (default: 1GB)
segment_max_bytes = 1073741824

# Maximum message size in bytes (default: 1MB)
max_message_bytes = 1048576

# Maximum index entries per segment
max_index_entries = 10000000

# Enable in-memory mode (no persistence)
# Useful for testing and development
in_memory = false

[wal]
# Enable Write-Ahead Log for durability
enabled = true

# Sync mode: "every_write", "interval", or "os_default"
# - every_write: safest, slowest
# - interval: balanced (default)
# - os_default: fastest, least durable
sync_mode = "interval"

# Sync interval in milliseconds (for interval mode)
sync_ms = 100

# Maximum WAL file size before rotation
max_file_size = 104857600  # 100MB

# Number of WAL files to retain
retention_files = 5

# Maximum pending writes before backpressure
max_pending_writes = 10000

[tls]
# Enable TLS/SSL encryption
enabled = false

# Path to certificate file (PEM format)
# cert = "/path/to/server.crt"

# Path to private key file (PEM format)
# key = "/path/to/server.key"

# Minimum TLS version ("1.2" or "1.3")
min_version = "1.2"

# Require client certificate (mTLS)
require_client_cert = false

# CA certificate for client verification
# ca_cert = "/path/to/ca.crt"

[auth]
# Enable SASL authentication
enabled = false

# SASL mechanisms to enable
# sasl_mechanisms = ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"]

# Path to users file (YAML format)
# users_file = "/etc/streamline/users.yaml"

# Allow anonymous connections when auth is enabled
allow_anonymous = false

[oauth]
# Enable OAuth 2.0 / OIDC authentication
enabled = false

# OIDC issuer URL
# issuer_url = "https://auth.example.com"

# JWKS URL (auto-discovered from issuer if not set)
# jwks_url = "https://auth.example.com/.well-known/jwks.json"

# Expected audience claim
# audience = "streamline"

# Claim to use as principal
principal_claim = "sub"

# Required scopes
# required_scopes = ["streamline:read", "streamline:write"]

# JWKS refresh interval in seconds
jwks_refresh_interval = 3600

[acl]
# Enable ACL-based authorization
enabled = false

# Path to ACL file (YAML format)
# file = "/etc/streamline/acls.yaml"

# Super users (bypass ACL checks)
# super_users = ["User:admin"]

# Allow all operations if no ACLs configured
allow_if_no_acls = true

[cluster]
# Cluster mode is enabled when node_id is set

# Unique node ID in the cluster
# node_id = 1

# Address advertised to clients
# advertised_addr = "broker1.example.com:9092"

# Address for inter-broker communication
# inter_broker_addr = "10.0.0.1:9093"

# Seed nodes for cluster discovery
# seed_nodes = ["10.0.0.2:9093", "10.0.0.3:9093"]

# Default replication factor for new topics
default_replication_factor = 1

# Minimum in-sync replicas for acks=all
min_insync_replicas = 1

# Rack ID for rack-aware placement
# rack_id = "us-east-1a"

# Disable rack-aware replica assignment
disable_rack_aware_assignment = false

[inter_broker_tls]
# Enable TLS for inter-broker communication
enabled = false

# Certificate, key, and CA for inter-broker TLS
# cert = "/path/to/inter-broker.crt"
# key = "/path/to/inter-broker.key"
# ca = "/path/to/inter-broker-ca.crt"

# Verify peer certificates
verify_peer = true

[audit]
# Enable security audit logging
enabled = false

# Audit log file path
log_path = "/var/log/streamline/audit.log"

# Log format: "json" or "text"
log_format = "json"

# Events to log (empty = all events)
# events = ["AUTH_SUCCESS", "AUTH_FAILURE", "ACL_DENY"]

# Also log to stdout
log_to_stdout = false

[limits]
# Maximum total connections (0 = unlimited)
max_connections = 10000

# Maximum connections per IP (0 = unlimited)
max_connections_per_ip = 100

# Maximum request size in bytes (0 = unlimited)
max_request_size = 104857600  # 100MB

# Connection idle timeout in seconds (0 = no timeout)
connection_idle_timeout = 600

# Produce rate limit in bytes/second (0 = unlimited)
produce_rate_limit = 0

[quotas]
# Enable client quotas
enabled = false

# Default producer byte rate (bytes/second, 0 = unlimited)
producer_byte_rate = 0

# Default consumer byte rate (bytes/second, 0 = unlimited)
consumer_byte_rate = 0

[simple]
# Enable simple text protocol (Redis RESP-like)
enabled = false

# Simple protocol listen address
addr = "0.0.0.0:9095"

[encryption]
# Enable encryption at rest
enabled = false

# Path to encryption key file
# key_file = "/etc/streamline/encryption.key"

# Environment variable containing encryption key
# key_env_var = "STREAMLINE_ENCRYPTION_KEY"

[zerocopy]
# Enable zero-copy networking optimizations
enabled = true

# Enable memory-mapped I/O for segments
mmap = true

# Minimum segment size for mmap (bytes)
mmap_threshold = 1048576  # 1MB

# Buffer pool size
buffer_pool_size = 16

# Enable sendfile for plain TCP
sendfile = true

# Enable response caching
response_cache = true

# Response cache size in MB
cache_size_mb = 128

[otel]
# Enable OpenTelemetry distributed tracing
enabled = false

# OTLP endpoint
# endpoint = "http://localhost:4317"

# Service name
service_name = "streamline"

# Sampling ratio (0.0 to 1.0)
sampling_ratio = 1.0

# Export timeout in seconds
timeout_secs = 10
"#
        .to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_empty_config() {
        let config: ConfigFile = toml::from_str("").unwrap();
        assert!(config.server.listen_addr.is_none());
    }

    #[test]
    fn test_parse_server_section() {
        let toml = r#"
            [server]
            listen_addr = "127.0.0.1:9092"
            log_level = "debug"
        "#;
        let config: ConfigFile = toml::from_str(toml).unwrap();
        assert_eq!(
            config.server.listen_addr,
            Some("127.0.0.1:9092".to_string())
        );
        assert_eq!(config.server.log_level, Some("debug".to_string()));
    }

    #[test]
    fn test_parse_storage_section() {
        let toml = r#"
            [storage]
            segment_max_bytes = 1073741824
            in_memory = true
        "#;
        let config: ConfigFile = toml::from_str(toml).unwrap();
        assert_eq!(config.storage.segment_max_bytes, Some(1073741824));
        assert_eq!(config.storage.in_memory, Some(true));
    }

    #[test]
    fn test_parse_tls_section() {
        let toml = r#"
            [tls]
            enabled = true
            cert = "/path/to/cert.pem"
            key = "/path/to/key.pem"
            min_version = "1.3"
        "#;
        let config: ConfigFile = toml::from_str(toml).unwrap();
        assert_eq!(config.tls.enabled, Some(true));
        assert_eq!(config.tls.cert, Some(PathBuf::from("/path/to/cert.pem")));
        assert_eq!(config.tls.min_version, Some("1.3".to_string()));
    }

    #[test]
    fn test_parse_cluster_section() {
        let toml = r#"
            [cluster]
            node_id = 1
            seed_nodes = ["10.0.0.1:9093", "10.0.0.2:9093"]
            default_replication_factor = 3
        "#;
        let config: ConfigFile = toml::from_str(toml).unwrap();
        assert_eq!(config.cluster.node_id, Some(1));
        assert_eq!(
            config.cluster.seed_nodes,
            Some(vec![
                "10.0.0.1:9093".to_string(),
                "10.0.0.2:9093".to_string()
            ])
        );
        assert_eq!(config.cluster.default_replication_factor, Some(3));
    }

    #[test]
    fn test_parse_auth_with_mechanisms() {
        let toml = r#"
            [auth]
            enabled = true
            sasl_mechanisms = ["PLAIN", "SCRAM-SHA-256"]
        "#;
        let config: ConfigFile = toml::from_str(toml).unwrap();
        assert_eq!(config.auth.enabled, Some(true));
        assert_eq!(
            config.auth.sasl_mechanisms,
            Some(vec!["PLAIN".to_string(), "SCRAM-SHA-256".to_string()])
        );
    }

    #[test]
    fn test_generate_example_is_valid_toml() {
        let example = ConfigFile::generate_example();
        let _config: ConfigFile = toml::from_str(&example).unwrap();
    }
}
