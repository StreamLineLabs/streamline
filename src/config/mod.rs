//! Configuration module for Streamline
//!
//! This module is organized into submodules for better maintainability:
//! - `defaults` - Default constants and values
//! - `tls` - TLS configuration
//! - `auth` - Authentication and authorization configuration
//! - `wal` - Write-Ahead Log configuration
//! - `storage` - Storage configuration
//! - `args` - CLI argument definitions

mod args;
mod auth;
mod defaults;
pub mod file;
mod merge;
mod runtime;
mod storage;
mod tcp;
mod tls;
mod wal;

// Re-export submodule types
pub use args::ServerArgs;
pub use auth::{AclConfig, AuthConfig};
pub use defaults::*;
pub use file::ConfigFile;
pub use merge::merge_config_with_args;
pub use runtime::{RuntimeConfig, RuntimeMode};
pub use storage::{IoBackendConfig, StorageConfig};
pub use tcp::TcpConfig;
pub use tls::TlsConfig;
pub use wal::WalConfig;

use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;

use defaults::{DEFAULT_HTTP_SOCKET_ADDR, DEFAULT_KAFKA_SOCKET_ADDR};

/// Complete server configuration for Streamline.
///
/// `ServerConfig` contains all settings needed to run a Streamline server,
/// including networking, storage, security, and optional enterprise features.
///
/// # Configuration Sources
///
/// Configuration is loaded from multiple sources with this precedence:
/// 1. **Environment variables** (highest priority) - `STREAMLINE_*` prefix
/// 2. **CLI arguments** - Command-line flags
/// 3. **Config file** - TOML configuration file
/// 4. **Built-in defaults** (lowest priority)
///
/// # Feature-Gated Fields
///
/// Some fields require specific feature flags to be enabled:
///
/// | Field | Feature Flag | Description |
/// |-------|--------------|-------------|
/// | `auth` | `auth` | SASL/OAuth authentication |
/// | `acl` | `auth` | Access control lists |
/// | `audit` | `auth` | Audit logging |
/// | `cluster` | `clustering` | Multi-node clustering |
///
/// # Example
///
/// ```rust,ignore
/// use streamline::config::{ServerConfig, ServerArgs};
///
/// // From CLI arguments
/// let args = ServerArgs::parse();
/// let config = ServerConfig::from_args(args)?;
///
/// // Or from TOML file
/// let config = ServerConfig::from_file("streamline.toml")?;
///
/// // Start server with config
/// let server = Server::new(config).await?;
/// ```
///
/// # Generating Example Config
///
/// ```bash
/// streamline --generate-config > streamline.toml
/// ```
///
/// # Stability
///
/// This struct is **Stable** - fields follow semantic versioning.
/// Feature-gated fields may change in minor versions with notice.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// Address to listen on for Kafka protocol
    pub listen_addr: SocketAddr,

    /// Address to listen on for HTTP API
    pub http_addr: SocketAddr,

    /// Data directory for storage
    pub data_dir: PathBuf,

    /// Log level
    pub log_level: String,

    /// Storage configuration
    pub storage: StorageConfig,

    /// TLS configuration
    pub tls: TlsConfig,

    /// TCP socket configuration
    pub tcp: TcpConfig,

    /// Authentication configuration
    #[cfg(feature = "auth")]
    pub auth: AuthConfig,

    /// Authorization (ACL) configuration
    #[cfg(feature = "auth")]
    pub acl: AclConfig,

    /// Audit logging configuration
    #[cfg(feature = "auth")]
    pub audit: crate::audit::AuditConfig,

    /// Resource limits configuration
    pub limits: crate::server::limits::LimitsConfig,

    /// Shutdown configuration
    pub shutdown: crate::server::shutdown::ShutdownConfig,

    /// Cluster configuration (None = single-node mode)
    #[cfg(feature = "clustering")]
    pub cluster: Option<crate::cluster::ClusterConfig>,

    /// Client quota configuration for throttling
    pub quotas: crate::server::limits::QuotaConfig,

    /// Simple protocol configuration
    pub simple: SimpleProtocolConfig,

    /// Runtime configuration (tokio vs sharded)
    pub runtime: RuntimeConfig,

    /// Enable automatic topic creation on produce requests.
    ///
    /// When enabled (default), topics are created automatically when a producer
    /// sends to a non-existent topic. This is convenient for development but can
    /// be a security/operational concern in production as typos can create
    /// unintended topics.
    ///
    /// Set to `false` in production environments to require explicit topic creation.
    pub auto_create_topics: bool,

    /// Telemetry configuration for anonymous usage reporting
    pub telemetry: crate::telemetry::TelemetryConfig,

    /// Enable playground mode with web UI and demo topics
    pub playground: bool,

    /// Enable ephemeral mode for testing: in-memory, auto-cleanup, fast startup.
    /// When enabled, data is never persisted and the server exits when all clients disconnect
    /// (after `ephemeral_idle_timeout_secs` seconds with zero connections).
    pub ephemeral: bool,

    /// Idle timeout in seconds before ephemeral server auto-shuts down (default: 30).
    pub ephemeral_idle_timeout_secs: u64,

    /// Auto-create topics from this list on startup in ephemeral mode (format: "name:partitions").
    pub ephemeral_auto_topics: Vec<String>,

    /// Edge deployment configuration (requires `edge` feature)
    #[cfg(feature = "edge")]
    pub edge: EdgeDeploymentConfig,
}

/// Edge deployment mode configuration embedded in ServerConfig.
///
/// Activated with `--edge-mode`. Applies resource-constrained defaults
/// and configures store-and-forward cloud sync.
#[cfg(feature = "edge")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeDeploymentConfig {
    /// Whether edge deployment mode is enabled.
    pub enabled: bool,
    /// Cloud endpoint URL for sync target.
    pub cloud_endpoint: Option<String>,
    /// Sync interval in seconds.
    pub sync_interval_secs: u64,
    /// Sync strategy: "all", "latest-only", "sample".
    pub sync_strategy: String,
    /// Maximum storage in bytes.
    pub max_storage_bytes: u64,
    /// Segment size in bytes.
    pub segment_size_bytes: u64,
    /// Buffer pool size.
    pub buffer_pool_size: usize,
    /// Disk usage threshold (0.0–1.0) for automatic compaction.
    pub disk_compaction_threshold: f64,
    /// CPU throttle percentage (0.0–1.0).
    pub cpu_throttle: f64,
    /// Prefer memory-mapped I/O.
    pub mmap_preferred: bool,
}

#[cfg(feature = "edge")]
impl Default for EdgeDeploymentConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            cloud_endpoint: None,
            sync_interval_secs: 300,
            sync_strategy: "all".to_string(),
            max_storage_bytes: 512 * 1024 * 1024,
            segment_size_bytes: 4 * 1024 * 1024,
            buffer_pool_size: 4,
            disk_compaction_threshold: 0.85,
            cpu_throttle: 0.5,
            mmap_preferred: false,
        }
    }
}

/// Configuration for the simple text-based protocol.
///
/// The simple protocol provides a lightweight alternative to the Kafka binary
/// protocol for basic produce/consume operations. It's useful for:
///
/// - **Debugging**: Human-readable messages for troubleshooting
/// - **Scripting**: Easy integration with shell scripts using netcat/telnet
/// - **IoT devices**: Minimal protocol overhead for constrained devices
/// - **Quick testing**: Rapid prototyping without Kafka client libraries
///
/// # Protocol Format
///
/// ```text
/// PRODUCE <topic> [key]\n<message>\n
/// CONSUME <topic> [from_offset]\n
/// ```
///
/// # Example
///
/// ```bash
/// # Produce a message
/// echo -e "PRODUCE my-topic\nHello World" | nc localhost 9095
///
/// # Consume from beginning
/// echo "CONSUME my-topic 0" | nc localhost 9095
/// ```
///
/// # Security Note
///
/// The simple protocol does not support authentication or encryption.
/// Only enable it on trusted networks or for development purposes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimpleProtocolConfig {
    /// Whether to enable the simple text protocol server.
    ///
    /// Default: `false` (disabled for security)
    pub enabled: bool,

    /// Address and port to listen on for simple protocol connections.
    ///
    /// Default: `0.0.0.0:9095`
    pub addr: SocketAddr,
}

impl Default for SimpleProtocolConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            addr: SocketAddr::from(([0, 0, 0, 0], 9095)),
        }
    }
}

impl ServerConfig {
    /// Create a new server configuration from command-line arguments
    pub fn from_args(args: ServerArgs) -> crate::error::Result<Self> {
        let listen_addr: SocketAddr = args.listen_addr.parse().map_err(|e| {
            crate::error::StreamlineError::Config(format!("Invalid listen address: {}", e))
        })?;

        let http_addr: SocketAddr = args.http_addr.parse().map_err(|e| {
            crate::error::StreamlineError::Config(format!("Invalid HTTP address: {}", e))
        })?;

        // Validate TLS configuration
        let tls_config = if args.tls_enabled {
            let cert_path = args.tls_cert.ok_or_else(|| {
                crate::error::StreamlineError::Config(
                    "TLS enabled but cert path not provided".to_string(),
                )
            })?;

            let key_path = args.tls_key.ok_or_else(|| {
                crate::error::StreamlineError::Config(
                    "TLS enabled but key path not provided".to_string(),
                )
            })?;

            if !cert_path.exists() {
                return Err(crate::error::StreamlineError::Config(format!(
                    "TLS certificate file not found: {}",
                    cert_path.display()
                )));
            }

            if !key_path.exists() {
                return Err(crate::error::StreamlineError::Config(format!(
                    "TLS key file not found: {}",
                    key_path.display()
                )));
            }

            if let Some(ref ca_cert_path) = args.tls_ca_cert {
                if !ca_cert_path.exists() {
                    return Err(crate::error::StreamlineError::Config(format!(
                        "TLS CA certificate file not found: {}",
                        ca_cert_path.display()
                    )));
                }
            }

            TlsConfig {
                enabled: true,
                cert_path,
                key_path,
                min_version: args.tls_min_version,
                require_client_cert: args.tls_require_client_cert,
                ca_cert_path: args.tls_ca_cert,
            }
        } else {
            TlsConfig::default()
        };

        // Build OAuth configuration (only when auth feature is enabled)
        #[cfg(feature = "auth")]
        let oauth_config = if args.oauth_enabled {
            let required_scopes = args
                .oauth_required_scopes
                .as_ref()
                .map(|s| {
                    s.split(',')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect()
                })
                .unwrap_or_default();

            crate::auth::OAuthConfig {
                enabled: true,
                issuer_url: args.oauth_issuer_url.clone(),
                jwks_url: args.oauth_jwks_url.clone(),
                audience: args.oauth_audience.clone(),
                additional_issuers: Vec::new(),
                principal_claim: args.oauth_principal_claim.clone(),
                jwks_refresh_interval_secs: args.oauth_jwks_refresh_interval,
                allow_missing_audience: false,
                required_scopes,
                clock_skew_seconds: 60, // 1 minute tolerance
                validate_iat: true,
                validate_nbf: true,
                max_token_age_seconds: None,
            }
        } else {
            crate::auth::OAuthConfig::default()
        };

        // Validate authentication configuration (only when auth feature is enabled)
        #[cfg(feature = "auth")]
        let auth_config = if args.auth_enabled {
            let users_file = args.auth_users_file.clone();

            // Note: We don't validate if the users file exists here
            // to allow creating it later through CLI commands

            let sasl_mechanisms = if let Some(ref mechanisms) = args.auth_sasl_mechanisms {
                mechanisms
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .collect()
            } else {
                vec!["PLAIN".to_string()]
            };

            AuthConfig {
                enabled: true,
                sasl_mechanisms,
                users_file,
                allow_anonymous: args.auth_allow_anonymous,
                oauth: oauth_config,
            }
        } else {
            AuthConfig {
                oauth: oauth_config,
                ..AuthConfig::default()
            }
        };

        #[cfg(not(feature = "auth"))]
        let _auth_config = AuthConfig::default();

        // Validate ACL configuration (only when auth feature is enabled)
        #[cfg(feature = "auth")]
        let acl_config = if args.acl_enabled {
            let super_users = if let Some(ref users) = args.acl_super_users {
                users
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect()
            } else {
                Vec::new()
            };

            AclConfig {
                enabled: true,
                acl_file: args.acl_file,
                super_users,
                allow_if_no_acls: args.acl_allow_if_no_acls,
            }
        } else {
            AclConfig::default()
        };

        #[cfg(not(feature = "auth"))]
        let _acl_config = AclConfig::default();

        // Build audit configuration (only when auth feature is enabled)
        #[cfg(feature = "auth")]
        let audit_config = if args.audit_enabled {
            let events = args
                .audit_events
                .as_ref()
                .map(|e| {
                    e.split(',')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect()
                })
                .unwrap_or_default();

            crate::audit::AuditConfig {
                enabled: true,
                log_path: Some(args.audit_log_path.clone()),
                format: args.audit_log_format.clone(),
                events,
                log_to_stdout: args.audit_log_to_stdout,
            }
        } else {
            crate::audit::AuditConfig::default()
        };

        #[cfg(not(feature = "auth"))]
        let _ = (
            &args.audit_enabled,
            &args.audit_events,
            &args.audit_log_path,
            &args.audit_log_format,
            &args.audit_log_to_stdout,
        ); // silence unused warnings

        // Build limits configuration
        let limits_config = crate::server::limits::LimitsConfig {
            max_connections: args.max_connections,
            max_connections_per_ip: args.max_connections_per_ip,
            max_request_size: args.max_request_size,
            produce_rate_limit_bytes: args.produce_rate_limit,
            connection_idle_timeout_secs: args.connection_idle_timeout,
        };

        // Build cluster configuration if node_id is provided (requires clustering feature)
        #[cfg(feature = "clustering")]
        let cluster_config = if let Some(node_id) = args.node_id {
            let advertised_addr: SocketAddr = args
                .advertised_addr
                .as_ref()
                .map(|s| s.parse())
                .transpose()
                .map_err(|e| {
                    crate::error::StreamlineError::Config(format!(
                        "Invalid advertised address: {}",
                        e
                    ))
                })?
                .unwrap_or(listen_addr);

            let inter_broker_addr: SocketAddr = args
                .inter_broker_addr
                .as_ref()
                .map(|s| s.parse())
                .transpose()
                .map_err(|e| {
                    crate::error::StreamlineError::Config(format!(
                        "Invalid inter-broker address: {}",
                        e
                    ))
                })?
                .unwrap_or_else(|| {
                    // Construct default inter-broker address from listen_addr IP and default port
                    // This format is guaranteed to be valid since listen_addr.ip() produces
                    // a valid IP and DEFAULT_INTER_BROKER_PORT is a valid u16 constant
                    SocketAddr::new(
                        listen_addr.ip(),
                        crate::cluster::config::DEFAULT_INTER_BROKER_PORT,
                    )
                });

            let seed_nodes: Vec<SocketAddr> = args
                .seed_nodes
                .as_ref()
                .map(|s| {
                    s.split(',')
                        .filter(|s| !s.trim().is_empty())
                        .map(|s| {
                            s.trim().parse().map_err(|e| {
                                crate::error::StreamlineError::Config(format!(
                                    "Invalid seed node address '{}': {}",
                                    s, e
                                ))
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()
                })
                .transpose()?
                .unwrap_or_default();

            // Build inter-broker TLS configuration
            let inter_broker_tls = crate::cluster::config::InterBrokerTlsConfig {
                enabled: args.inter_broker_tls_enabled,
                cert_path: args.inter_broker_tls_cert.clone(),
                key_path: args.inter_broker_tls_key.clone(),
                ca_cert_path: args.inter_broker_tls_ca.clone(),
                verify_peer: args.inter_broker_tls_verify_peer,
                min_version: "1.2".to_string(), // Use default, can be made configurable
            };

            let cluster = crate::cluster::ClusterConfig {
                node_id,
                advertised_addr,
                inter_broker_addr,
                seed_nodes,
                default_replication_factor: args.default_replication_factor,
                min_insync_replicas: args.min_insync_replicas,
                inter_broker_tls,
                rack_id: args.rack_id.clone(),
                rack_aware_assignment: !args.disable_rack_aware_assignment,
                ..Default::default()
            };

            cluster.validate().map_err(|e| {
                crate::error::StreamlineError::Config(format!("Invalid cluster config: {}", e))
            })?;

            Some(cluster)
        } else {
            None
        };

        // Silence unused warnings for cluster args when clustering is disabled
        #[cfg(not(feature = "clustering"))]
        let _ = (
            &args.node_id,
            &args.advertised_addr,
            &args.inter_broker_addr,
            &args.seed_nodes,
            &args.inter_broker_tls_enabled,
            &args.inter_broker_tls_cert,
            &args.inter_broker_tls_key,
            &args.inter_broker_tls_ca,
            &args.inter_broker_tls_verify_peer,
            &args.default_replication_factor,
            &args.min_insync_replicas,
            &args.rack_id,
            &args.disable_rack_aware_assignment,
        );

        // Build telemetry config (needs data_dir before move)
        let telemetry_config = {
            let mut config = crate::telemetry::TelemetryConfig {
                enabled: args.telemetry_enabled,
                endpoint: args
                    .telemetry_endpoint
                    .clone()
                    .unwrap_or_else(|| crate::telemetry::DEFAULT_TELEMETRY_ENDPOINT.to_string()),
                interval_hours: args.telemetry_interval_hours,
                dry_run: args.telemetry_dry_run,
                ..Default::default()
            };
            // Ensure installation ID is set
            config.ensure_installation_id(&args.data_dir);
            config
        };

        Ok(Self {
            listen_addr,
            http_addr,
            data_dir: args.data_dir,
            log_level: args.log_level,
            storage: StorageConfig {
                wal: WalConfig {
                    enabled: args.wal_enabled && !args.in_memory, // Disable WAL in in-memory mode
                    sync_interval_ms: args.wal_sync_ms,
                    sync_mode: args.wal_sync_mode,
                    max_file_size: args.wal_max_file_size,
                    retention_files: args.wal_retention_files,
                    max_pending_writes: args.wal_max_pending_writes,
                    ..Default::default()
                },
                segment_max_bytes: args.segment_max_bytes,
                segment_sync_mode: defaults::DEFAULT_SEGMENT_SYNC_MODE.to_string(),
                segment_sync_interval_ms: defaults::DEFAULT_SEGMENT_SYNC_INTERVAL_MS,
                max_message_bytes: args.max_message_bytes,
                max_index_entries: args.max_index_entries,
                in_memory: args.in_memory,
                encryption: if args.encryption_enabled {
                    let key_provider = if args.encryption_key_file.is_some() {
                        crate::storage::KeyProviderType::File
                    } else if args.encryption_key_env_var.is_some() {
                        crate::storage::KeyProviderType::Environment
                    } else {
                        return Err(crate::error::StreamlineError::Config(
                            "Encryption enabled but no key provider configured. \
                             Use --encryption-key-file or --encryption-key-env-var"
                                .to_string(),
                        ));
                    };
                    crate::storage::EncryptionConfig {
                        enabled: true,
                        algorithm: crate::storage::EncryptionAlgorithm::Aes256Gcm,
                        key_provider,
                        key_file: args.encryption_key_file.clone(),
                        key_env_var: args.encryption_key_env_var.clone(),
                    }
                } else {
                    crate::storage::EncryptionConfig::default()
                },
                zerocopy: crate::storage::ZeroCopyConfig {
                    enabled: args.zerocopy_enabled,
                    enable_mmap: args.zerocopy_mmap,
                    mmap_threshold: args.zerocopy_mmap_threshold,
                    buffer_size: 64 * 1024, // 64KB, not configurable via CLI
                    enable_sendfile: args.zerocopy_sendfile,
                    enable_buffer_pool: true, // Always true when enabled
                    buffer_pool_size: args.zerocopy_buffer_pool_size,
                    enable_response_cache: args.zerocopy_response_cache,
                    response_cache_max_bytes: args.zerocopy_cache_size_mb * 1024 * 1024,
                    response_cache_ttl_secs: 60, // Not configurable via CLI, use default
                },
                io_backend: crate::config::storage::IoBackendConfig {
                    enable_direct_io: args.io_direct_enabled,
                    direct_io_alignment: args.io_direct_alignment,
                    direct_io_fallback: args.io_direct_fallback,
                    ..crate::config::storage::IoBackendConfig::default()
                },
            },
            tls: tls_config,
            tcp: TcpConfig::default(), // Use defaults for now, can be extended to CLI args later
            #[cfg(feature = "auth")]
            auth: auth_config,
            #[cfg(feature = "auth")]
            acl: acl_config,
            #[cfg(feature = "auth")]
            audit: audit_config,
            limits: limits_config,
            shutdown: crate::server::shutdown::ShutdownConfig {
                timeout_secs: args.shutdown_timeout,
                wait_for_requests: true,
                drain_timeout_secs: args.drain_timeout,
            },
            #[cfg(feature = "clustering")]
            cluster: cluster_config,
            quotas: crate::server::limits::QuotaConfig {
                enabled: args.quotas_enabled,
                producer_byte_rate: args.quota_producer_byte_rate,
                consumer_byte_rate: args.quota_consumer_byte_rate,
                request_percentage: 0, // Not configurable via CLI yet
            },
            simple: {
                let simple_addr: SocketAddr = args.simple_addr.parse().map_err(|e| {
                    crate::error::StreamlineError::Config(format!(
                        "Invalid simple protocol address: {}",
                        e
                    ))
                })?;
                SimpleProtocolConfig {
                    enabled: args.simple_enabled,
                    addr: simple_addr,
                }
            },
            runtime: {
                let mode = RuntimeMode::from_str(&args.runtime_mode)
                    .map_err(crate::error::StreamlineError::Config)?;
                RuntimeConfig {
                    mode,
                    shard_count: args.shard_count,
                    enable_cpu_affinity: args.enable_cpu_affinity,
                    enable_polling: args.enable_polling,
                    ..RuntimeConfig::default()
                }
            },
            auto_create_topics: args.auto_create_topics,
            telemetry: telemetry_config,
            playground: args.playground,
            ephemeral: args.ephemeral.unwrap_or(false),
            ephemeral_idle_timeout_secs: args.ephemeral_idle_timeout_secs.unwrap_or(30),
            ephemeral_auto_topics: args.ephemeral_auto_topics.clone().unwrap_or_default(),
            #[cfg(feature = "edge")]
            edge: EdgeDeploymentConfig {
                enabled: args.edge_mode,
                cloud_endpoint: args.edge_cloud_endpoint,
                sync_interval_secs: args.edge_sync_interval_secs,
                sync_strategy: args.edge_sync_strategy,
                max_storage_bytes: args.edge_max_storage_mb * 1024 * 1024,
                segment_size_bytes: args.edge_segment_size_kb * 1024,
                buffer_pool_size: args.edge_buffer_pool_size,
                disk_compaction_threshold: args.edge_disk_threshold,
                cpu_throttle: args.edge_cpu_throttle,
                mmap_preferred: args.edge_mmap_preferred,
            },
        })
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: DEFAULT_KAFKA_SOCKET_ADDR,
            http_addr: DEFAULT_HTTP_SOCKET_ADDR,
            data_dir: PathBuf::from(DEFAULT_DATA_DIR),
            log_level: DEFAULT_LOG_LEVEL.to_string(),
            storage: StorageConfig::default(),
            tls: TlsConfig::default(),
            tcp: TcpConfig::default(),
            #[cfg(feature = "auth")]
            auth: AuthConfig::default(),
            #[cfg(feature = "auth")]
            acl: AclConfig::default(),
            #[cfg(feature = "auth")]
            audit: crate::audit::AuditConfig::default(),
            limits: crate::server::limits::LimitsConfig::default(),
            shutdown: crate::server::shutdown::ShutdownConfig::default(),
            #[cfg(feature = "clustering")]
            cluster: None, // Single-node mode by default
            quotas: crate::server::limits::QuotaConfig::default(),
            simple: SimpleProtocolConfig::default(),
            runtime: RuntimeConfig::default(),
            auto_create_topics: DEFAULT_AUTO_CREATE_TOPICS,
            telemetry: crate::telemetry::TelemetryConfig::default(),
            playground: false,
            ephemeral: false,
            ephemeral_idle_timeout_secs: 30,
            ephemeral_auto_topics: Vec::new(),
            #[cfg(feature = "edge")]
            edge: EdgeDeploymentConfig::default(),
        }
    }
}

impl ServerConfig {
    /// Check if cluster mode is enabled
    #[cfg(feature = "clustering")]
    pub fn is_cluster_mode(&self) -> bool {
        self.cluster.is_some()
    }

    /// Check if cluster mode is enabled (always false when clustering is disabled)
    #[cfg(not(feature = "clustering"))]
    pub fn is_cluster_mode(&self) -> bool {
        false
    }

    /// Get the node ID (returns 0 for single-node mode)
    #[cfg(feature = "clustering")]
    pub fn node_id(&self) -> u64 {
        self.cluster.as_ref().map(|c| c.node_id).unwrap_or(0)
    }

    /// Get the node ID (always 0 when clustering is disabled)
    #[cfg(not(feature = "clustering"))]
    pub fn node_id(&self) -> u64 {
        0
    }

    /// Validate the configuration for consistency and correctness
    ///
    /// This method performs comprehensive validation of the configuration,
    /// checking for:
    /// - Conflicting settings
    /// - Invalid value combinations
    /// - Resource constraints
    /// - Security configuration issues
    ///
    /// Call this method after loading configuration to catch issues early.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the configuration is valid, or an error describing
    /// the validation failure.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let config = ServerConfig::from_args(args)?;
    /// config.validate()?;  // Catch configuration issues at startup
    /// ```
    pub fn validate(&self) -> crate::error::Result<()> {
        use crate::error::StreamlineError;
        use tracing::warn;

        // === Port Validation ===

        if self.listen_addr.port() == 0 {
            return Err(StreamlineError::Config(
                "Kafka listen port must be between 1 and 65535".to_string(),
            ));
        }

        if self.http_addr.port() == 0 {
            return Err(StreamlineError::Config(
                "HTTP listen port must be between 1 and 65535".to_string(),
            ));
        }

        if self.simple.enabled && self.simple.addr.port() == 0 {
            return Err(StreamlineError::Config(
                "Simple protocol listen port must be between 1 and 65535".to_string(),
            ));
        }

        // === Storage Validation ===

        // Check for conflicting storage settings
        if self.storage.in_memory && self.storage.wal.enabled {
            warn!(
                "WAL is enabled but storage is in-memory mode. \
                 WAL will be ignored - data is not durable."
            );
        }

        // Validate WAL sync mode
        let valid_sync_modes = ["interval", "every_write", "os_default"];
        if !valid_sync_modes.contains(&self.storage.wal.sync_mode.as_str()) {
            return Err(StreamlineError::Config(format!(
                "Invalid WAL sync mode '{}'. Valid options: {}",
                self.storage.wal.sync_mode,
                valid_sync_modes.join(", ")
            )));
        }

        // Warn about risky WAL configurations
        if self.storage.wal.sync_mode == "os_default" {
            warn!(
                "================================================================================"
            );
            warn!("DURABILITY WARNING: WAL sync mode is 'os_default'");
            warn!(
                "================================================================================"
            );
            warn!(
                "This mode provides NO durability guarantees. On power failure or OS crash, \
                 data loss of SEVERAL MINUTES is possible. The OS may buffer writes for \
                 5-30+ seconds before flushing to disk."
            );
            warn!(
                "This mode is only suitable for: development, testing, or workloads where \
                 data can be fully reconstructed from external sources."
            );
            warn!(
                "For production use, set sync_mode to 'interval' (recommended) or 'every_write'."
            );
            warn!(
                "================================================================================"
            );
        }

        if self.storage.wal.sync_interval_ms < 10 && self.storage.wal.sync_mode == "interval" {
            warn!(
                "WAL sync interval {}ms is very low - this may cause significant \
                 performance degradation due to frequent fsync operations.",
                self.storage.wal.sync_interval_ms
            );
        }

        // Validate segment sync mode
        let valid_segment_sync_modes = ["none", "on_seal", "interval", "every_write"];
        if !valid_segment_sync_modes.contains(&self.storage.segment_sync_mode.as_str()) {
            return Err(StreamlineError::Config(format!(
                "Invalid segment sync mode '{}'. Valid options: {}",
                self.storage.segment_sync_mode,
                valid_segment_sync_modes.join(", ")
            )));
        }

        // Warn about risky durability configurations:
        // If WAL is disabled and segment sync mode is relaxed, data loss is likely on crash
        let segment_sync_relaxed =
            matches!(self.storage.segment_sync_mode.as_str(), "none" | "on_seal");
        if !self.storage.in_memory && !self.storage.wal.enabled && segment_sync_relaxed {
            warn!(
                "================================================================================"
            );
            warn!(
                "DURABILITY WARNING: WAL is disabled and segment sync mode is '{}'",
                self.storage.segment_sync_mode
            );
            warn!(
                "================================================================================"
            );
            warn!(
                "Without WAL and with relaxed segment sync, data written since the last \
                 segment seal (up to {}MB) may be lost on crash or power failure.",
                self.storage.segment_max_bytes / (1024 * 1024)
            );
            warn!(
                "Consider either: (1) enabling WAL (--wal-enabled), or \
                 (2) using segment sync mode 'interval' or 'every_write'"
            );
            warn!(
                "================================================================================"
            );
        }

        // Validate message size constraints
        if self.storage.max_message_bytes > self.storage.segment_max_bytes {
            return Err(StreamlineError::Config(format!(
                "max_message_bytes ({}) cannot be larger than segment_max_bytes ({})",
                self.storage.max_message_bytes, self.storage.segment_max_bytes
            )));
        }

        // Validate segment size is reasonable
        if self.storage.segment_max_bytes < 1024 * 1024 {
            return Err(StreamlineError::Config(
                "segment_max_bytes must be at least 1MB".to_string(),
            ));
        }

        if self.storage.segment_max_bytes > 10 * 1024 * 1024 * 1024 {
            return Err(StreamlineError::Config(format!(
                "segment_max_bytes ({}) exceeds maximum of 10GB",
                self.storage.segment_max_bytes
            )));
        }

        // Validate message size bounds
        if self.storage.max_message_bytes < 1024 {
            return Err(StreamlineError::Config(format!(
                "max_message_bytes ({}) must be at least 1KB",
                self.storage.max_message_bytes
            )));
        }

        if self.storage.max_message_bytes > 1024 * 1024 * 1024 {
            return Err(StreamlineError::Config(format!(
                "max_message_bytes ({}) exceeds maximum of 1GB",
                self.storage.max_message_bytes
            )));
        }

        // Validate WAL retention
        if self.storage.wal.enabled && self.storage.wal.retention_files == 0 {
            return Err(StreamlineError::Config(
                "WAL retention_files must be greater than 0 when WAL is enabled".to_string(),
            ));
        }

        // Validate WAL max_pending_writes
        if self.storage.wal.max_pending_writes == 0 {
            return Err(StreamlineError::Config(
                "WAL max_pending_writes must be greater than 0".to_string(),
            ));
        }

        // Validate segment sync interval when using interval mode
        if self.storage.segment_sync_mode == "interval" && self.storage.segment_sync_interval_ms == 0
        {
            return Err(StreamlineError::Config(
                "segment_sync_interval_ms must be greater than 0 when using interval sync mode"
                    .to_string(),
            ));
        }

        // Validate I/O backend buffer sizes
        if self.storage.io_backend.buffer_pool_size == 0 {
            return Err(StreamlineError::Config(
                "io_backend buffer_pool_size must be greater than 0".to_string(),
            ));
        }

        if self.storage.io_backend.queue_depth == 0 {
            return Err(StreamlineError::Config(
                "io_backend queue_depth must be greater than 0".to_string(),
            ));
        }

        // Validate max index entries
        if self.storage.max_index_entries < 100 {
            return Err(StreamlineError::Config(
                "max_index_entries must be at least 100".to_string(),
            ));
        }

        // === Resource Limits Validation ===

        if self.limits.max_connections > 0 && self.limits.max_connections < 10 {
            return Err(StreamlineError::Config(
                "max_connections must be at least 10 (or 0 for unlimited)".to_string(),
            ));
        }

        if self.limits.max_connections_per_ip > 0 && self.limits.max_connections_per_ip < 5 {
            return Err(StreamlineError::Config(
                "max_connections_per_ip must be at least 5 (or 0 for unlimited)".to_string(),
            ));
        }

        if self.limits.max_request_size < 1024 {
            return Err(StreamlineError::Config(
                "max_request_size must be at least 1KB".to_string(),
            ));
        }

        // === TLS Validation ===

        if self.tls.enabled {
            if self.tls.cert_path.as_os_str().is_empty() {
                return Err(StreamlineError::Config(
                    "TLS is enabled but cert_path is empty".to_string(),
                ));
            }
            if self.tls.key_path.as_os_str().is_empty() {
                return Err(StreamlineError::Config(
                    "TLS is enabled but key_path is empty".to_string(),
                ));
            }

            let valid_tls_versions = ["1.2", "1.3"];
            if !valid_tls_versions.contains(&self.tls.min_version.as_str()) {
                return Err(StreamlineError::Config(format!(
                    "Invalid TLS min version '{}'. Valid options: {}",
                    self.tls.min_version,
                    valid_tls_versions.join(", ")
                )));
            }

            if self.tls.require_client_cert && self.tls.ca_cert_path.is_none() {
                return Err(StreamlineError::Config(
                    "mTLS requires ca_cert_path when require_client_cert is true".to_string(),
                ));
            }
        }

        // === Authentication Validation ===
        #[cfg(feature = "auth")]
        if self.auth.enabled {
            // Warn about PLAIN without TLS
            if self.auth.sasl_mechanisms.contains(&"PLAIN".to_string()) && !self.tls.enabled {
                warn!(
                    "SASL/PLAIN is enabled without TLS - credentials will be sent in cleartext. \
                     Enable TLS for production use."
                );
            }

            // Validate SASL mechanisms
            let valid_mechanisms = ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "OAUTHBEARER"];
            for mechanism in &self.auth.sasl_mechanisms {
                if !valid_mechanisms.contains(&mechanism.as_str()) {
                    return Err(StreamlineError::Config(format!(
                        "Invalid SASL mechanism '{}'. Valid options: {}",
                        mechanism,
                        valid_mechanisms.join(", ")
                    )));
                }
            }

            // Validate OAuth configuration
            if self
                .auth
                .sasl_mechanisms
                .contains(&"OAUTHBEARER".to_string())
            {
                if !self.auth.oauth.enabled {
                    return Err(StreamlineError::Config(
                        "OAUTHBEARER mechanism requires OAuth to be enabled".to_string(),
                    ));
                }
                if self.auth.oauth.issuer_url.is_none() && self.auth.oauth.jwks_url.is_none() {
                    return Err(StreamlineError::Config(
                        "OAuth requires either issuer_url or jwks_url to be configured".to_string(),
                    ));
                }
            }
        }

        // === ACL Validation ===
        #[cfg(feature = "auth")]
        if self.acl.enabled && !self.auth.enabled {
            warn!(
                "ACL authorization is enabled but authentication is disabled. \
                 All clients will be treated as anonymous."
            );
        }

        // === Cluster Validation ===
        #[cfg(feature = "clustering")]
        if let Some(ref cluster) = self.cluster {
            if cluster.seed_nodes.is_empty() {
                return Err(StreamlineError::Config(
                    "Cluster mode requires at least one seed node".to_string(),
                ));
            }

            if cluster.default_replication_factor < 1 {
                return Err(StreamlineError::Config(
                    "default_replication_factor must be at least 1".to_string(),
                ));
            }

            if cluster.default_replication_factor > 255 {
                return Err(StreamlineError::Config(format!(
                    "default_replication_factor ({}) exceeds maximum of 255",
                    cluster.default_replication_factor
                )));
            }

            if cluster.default_replication_factor > (cluster.seed_nodes.len() + 1) as i16 {
                warn!(
                    "default_replication_factor ({}) is higher than the number of known nodes ({}). \
                     Topics may be under-replicated until more nodes join.",
                    cluster.default_replication_factor,
                    cluster.seed_nodes.len() + 1
                );
            }
        }

        // === Shutdown Validation ===

        if self.shutdown.timeout_secs < 5 {
            warn!(
                "Shutdown timeout {}s is very short - in-flight requests may be dropped.",
                self.shutdown.timeout_secs
            );
        }

        if self.shutdown.timeout_secs > 86400 {
            return Err(StreamlineError::Config(format!(
                "shutdown timeout_secs ({}) exceeds 24 hours — likely a misconfiguration",
                self.shutdown.timeout_secs
            )));
        }

        if self.shutdown.drain_timeout_secs > 86400 {
            return Err(StreamlineError::Config(format!(
                "shutdown drain_timeout_secs ({}) exceeds 24 hours — likely a misconfiguration",
                self.shutdown.drain_timeout_secs
            )));
        }

        if self.limits.connection_idle_timeout_secs > 0
            && self.limits.connection_idle_timeout_secs > 86400
        {
            return Err(StreamlineError::Config(format!(
                "connection_idle_timeout_secs ({}) exceeds 24 hours — likely a misconfiguration",
                self.limits.connection_idle_timeout_secs
            )));
        }

        // === Auto-create Topics Warning ===

        if self.auto_create_topics {
            warn!(
                "auto_create_topics is enabled. Topics will be created automatically on produce. \
                 Consider disabling in production to prevent accidental topic creation."
            );
        }

        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ServerConfig::default();
        assert_eq!(config.listen_addr.port(), 9092);
        assert_eq!(config.http_addr.port(), 9094);
        assert_eq!(config.data_dir, PathBuf::from("./data"));
    }

    #[test]
    fn test_storage_config_default() {
        let config = StorageConfig::default();
        assert!(config.wal.enabled);
        assert_eq!(config.wal.sync_interval_ms, 100);
        assert_eq!(config.wal.sync_mode, "interval");
        assert_eq!(config.wal.max_file_size, 100 * 1024 * 1024);
        assert_eq!(config.wal.retention_files, 5);
        assert_eq!(config.segment_max_bytes, 1024 * 1024 * 1024);
        assert_eq!(config.max_message_bytes, 1024 * 1024);
    }

    #[test]
    fn test_server_config_from_args() {
        let args = ServerArgs {
            config: None,
            generate_config: false,
            listen_addr: "127.0.0.1:9999".to_string(),
            http_addr: "127.0.0.1:8888".to_string(),
            data_dir: PathBuf::from("/tmp/test"),
            log_level: "debug".to_string(),
            wal_enabled: true,
            wal_sync_ms: 200,
            wal_sync_mode: "every_write".to_string(),
            wal_max_file_size: 50 * 1024 * 1024,
            wal_retention_files: 3,
            wal_max_pending_writes: DEFAULT_WAL_MAX_PENDING_WRITES,
            segment_max_bytes: 500 * 1024 * 1024,
            max_message_bytes: 2 * 1024 * 1024,
            max_index_entries: DEFAULT_MAX_INDEX_ENTRIES,
            tls_enabled: false,
            tls_cert: None,
            tls_key: None,
            tls_min_version: DEFAULT_TLS_MIN_VERSION.to_string(),
            tls_require_client_cert: false,
            tls_ca_cert: None,
            auth_enabled: false,
            auth_sasl_mechanisms: None,
            auth_users_file: None,
            auth_allow_anonymous: false,
            oauth_enabled: false,
            oauth_issuer_url: None,
            oauth_jwks_url: None,
            oauth_audience: None,
            oauth_principal_claim: "sub".to_string(),
            oauth_required_scopes: None,
            oauth_jwks_refresh_interval: 3600,
            acl_enabled: false,
            acl_file: None,
            acl_super_users: None,
            acl_allow_if_no_acls: true,
            node_id: None,
            advertised_addr: None,
            inter_broker_addr: None,
            seed_nodes: None,
            default_replication_factor: 1,
            min_insync_replicas: 1,
            inter_broker_tls_enabled: false,
            inter_broker_tls_cert: None,
            inter_broker_tls_key: None,
            inter_broker_tls_ca: None,
            inter_broker_tls_verify_peer: true,
            audit_enabled: false,
            audit_log_path: PathBuf::from(DEFAULT_AUDIT_LOG_PATH),
            audit_log_format: DEFAULT_AUDIT_LOG_FORMAT.to_string(),
            audit_events: None,
            audit_log_to_stdout: false,
            max_connections: DEFAULT_MAX_CONNECTIONS,
            max_connections_per_ip: DEFAULT_MAX_CONNECTIONS_PER_IP,
            max_request_size: DEFAULT_MAX_REQUEST_SIZE,
            connection_idle_timeout: DEFAULT_CONNECTION_IDLE_TIMEOUT_SECS,
            produce_rate_limit: DEFAULT_PRODUCE_RATE_LIMIT_BYTES,
            quotas_enabled: false,
            quota_producer_byte_rate: 0,
            quota_consumer_byte_rate: 0,
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT_SECS,
            drain_timeout: crate::server::shutdown::DEFAULT_DRAIN_TIMEOUT_SECS,
            in_memory: false,
            simple_enabled: false,
            simple_addr: "0.0.0.0:9095".to_string(),
            playground: false,
            ephemeral: None,
            ephemeral_idle_timeout_secs: None,
            ephemeral_auto_topics: None,
            encryption_enabled: false,
            encryption_key_file: None,
            encryption_key_env_var: None,
            rack_id: None,
            disable_rack_aware_assignment: false,
            auto_create_topics: DEFAULT_AUTO_CREATE_TOPICS,
            runtime_mode: DEFAULT_RUNTIME_MODE.to_string(),
            shard_count: None,
            enable_cpu_affinity: true,
            enable_polling: false,
            io_direct_enabled: false,
            io_direct_alignment: 4096,
            io_direct_fallback: true,
            zerocopy_enabled: true,
            zerocopy_mmap: true,
            zerocopy_mmap_threshold: 1048576,
            zerocopy_buffer_pool_size: 16,
            zerocopy_sendfile: true,
            zerocopy_response_cache: true,
            zerocopy_cache_size_mb: 128,
            otel_enabled: false,
            otel_endpoint: None,
            otel_service_name: "streamline".to_string(),
            otel_sampling_ratio: 1.0,
            otel_timeout_secs: 10,
            storage_mode: None,
            s3_bucket: None,
            s3_prefix: None,
            s3_region: None,
            s3_endpoint: None,
            s3_access_key_id: None,
            s3_secret_access_key: None,
            s3_write_buffer_mb: 64,
            s3_flush_interval_ms: 1000,
            s3_upload_parallelism: 4,
            s3_read_cache_mb: 256,
            s3_multipart_threshold_mb: 64,
            s3_multipart_part_size_mb: 16,
            s3_prefetch_enabled: true,
            s3_prefetch_count: 2,
            s3_max_retries: 3,
            s3_retry_backoff_ms: 100,
            telemetry_enabled: false,
            telemetry_endpoint: None,
            telemetry_interval_hours: 24,
            telemetry_dry_run: false,
            edge_mode: false,
            edge_cloud_endpoint: None,
            edge_sync_interval_secs: 300,
            edge_sync_strategy: "all".to_string(),
            edge_max_storage_mb: 512,
            edge_segment_size_kb: 4096,
            edge_buffer_pool_size: 4,
            edge_disk_threshold: 0.85,
            edge_cpu_throttle: 0.5,
            edge_mmap_preferred: false,
        };

        let config = ServerConfig::from_args(args).unwrap();
        assert_eq!(config.listen_addr.port(), 9999);
        assert_eq!(config.http_addr.port(), 8888);
        assert_eq!(config.data_dir, PathBuf::from("/tmp/test"));
        assert_eq!(config.log_level, "debug");
        assert!(config.storage.wal.enabled);
        assert!(!config.storage.in_memory);
        assert_eq!(config.storage.wal.sync_interval_ms, 200);
        assert_eq!(config.storage.wal.sync_mode, "every_write");
        assert_eq!(config.storage.wal.max_file_size, 50 * 1024 * 1024);
        assert_eq!(config.storage.wal.retention_files, 3);
        assert_eq!(config.storage.segment_max_bytes, 500 * 1024 * 1024);
        assert_eq!(config.storage.max_message_bytes, 2 * 1024 * 1024);
        assert!(!config.tls.enabled);
        #[cfg(feature = "auth")]
        {
            assert!(!config.auth.enabled);
            assert!(!config.auth.oauth.enabled);
            assert!(!config.acl.enabled);
            assert!(!config.audit.enabled);
        }
        #[cfg(feature = "clustering")]
        assert!(!config.is_cluster_mode());
    }

    #[test]
    fn test_server_config_invalid_listen_addr() {
        let args = ServerArgs {
            config: None,
            generate_config: false,
            listen_addr: "invalid-address".to_string(),
            http_addr: "127.0.0.1:8888".to_string(),
            data_dir: PathBuf::from("/tmp/test"),
            log_level: "info".to_string(),
            wal_enabled: true,
            wal_sync_ms: 100,
            wal_sync_mode: "interval".to_string(),
            wal_max_file_size: 100 * 1024 * 1024,
            wal_retention_files: 5,
            wal_max_pending_writes: DEFAULT_WAL_MAX_PENDING_WRITES,
            segment_max_bytes: 1024 * 1024 * 1024,
            max_message_bytes: 1024 * 1024,
            max_index_entries: DEFAULT_MAX_INDEX_ENTRIES,
            tls_enabled: false,
            tls_cert: None,
            tls_key: None,
            tls_min_version: DEFAULT_TLS_MIN_VERSION.to_string(),
            tls_require_client_cert: false,
            tls_ca_cert: None,
            auth_enabled: false,
            auth_sasl_mechanisms: None,
            auth_users_file: None,
            auth_allow_anonymous: false,
            oauth_enabled: false,
            oauth_issuer_url: None,
            oauth_jwks_url: None,
            oauth_audience: None,
            oauth_principal_claim: "sub".to_string(),
            oauth_required_scopes: None,
            oauth_jwks_refresh_interval: 3600,
            acl_enabled: false,
            acl_file: None,
            acl_super_users: None,
            acl_allow_if_no_acls: true,
            node_id: None,
            advertised_addr: None,
            inter_broker_addr: None,
            seed_nodes: None,
            default_replication_factor: 1,
            min_insync_replicas: 1,
            inter_broker_tls_enabled: false,
            inter_broker_tls_cert: None,
            inter_broker_tls_key: None,
            inter_broker_tls_ca: None,
            inter_broker_tls_verify_peer: true,
            audit_enabled: false,
            audit_log_path: PathBuf::from(DEFAULT_AUDIT_LOG_PATH),
            audit_log_format: DEFAULT_AUDIT_LOG_FORMAT.to_string(),
            audit_events: None,
            audit_log_to_stdout: false,
            max_connections: DEFAULT_MAX_CONNECTIONS,
            max_connections_per_ip: DEFAULT_MAX_CONNECTIONS_PER_IP,
            max_request_size: DEFAULT_MAX_REQUEST_SIZE,
            connection_idle_timeout: DEFAULT_CONNECTION_IDLE_TIMEOUT_SECS,
            produce_rate_limit: DEFAULT_PRODUCE_RATE_LIMIT_BYTES,
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT_SECS,
            drain_timeout: crate::server::shutdown::DEFAULT_DRAIN_TIMEOUT_SECS,
            quotas_enabled: false,
            quota_producer_byte_rate: 0,
            quota_consumer_byte_rate: 0,
            in_memory: false,
            simple_enabled: false,
            simple_addr: "0.0.0.0:9095".to_string(),
            playground: false,
            ephemeral: None,
            ephemeral_idle_timeout_secs: None,
            ephemeral_auto_topics: None,
            encryption_enabled: false,
            encryption_key_file: None,
            encryption_key_env_var: None,
            rack_id: None,
            disable_rack_aware_assignment: false,
            auto_create_topics: DEFAULT_AUTO_CREATE_TOPICS,
            runtime_mode: DEFAULT_RUNTIME_MODE.to_string(),
            shard_count: None,
            enable_cpu_affinity: true,
            enable_polling: false,
            io_direct_enabled: false,
            io_direct_alignment: 4096,
            io_direct_fallback: true,
            zerocopy_enabled: true,
            zerocopy_mmap: true,
            zerocopy_mmap_threshold: 1048576,
            zerocopy_buffer_pool_size: 16,
            zerocopy_sendfile: true,
            zerocopy_response_cache: true,
            zerocopy_cache_size_mb: 128,
            otel_enabled: false,
            otel_endpoint: None,
            otel_service_name: "streamline".to_string(),
            otel_sampling_ratio: 1.0,
            otel_timeout_secs: 10,
            storage_mode: None,
            s3_bucket: None,
            s3_prefix: None,
            s3_region: None,
            s3_endpoint: None,
            s3_access_key_id: None,
            s3_secret_access_key: None,
            s3_write_buffer_mb: 64,
            s3_flush_interval_ms: 1000,
            s3_upload_parallelism: 4,
            s3_read_cache_mb: 256,
            s3_multipart_threshold_mb: 64,
            s3_multipart_part_size_mb: 16,
            s3_prefetch_enabled: true,
            s3_prefetch_count: 2,
            s3_max_retries: 3,
            s3_retry_backoff_ms: 100,
            telemetry_enabled: false,
            telemetry_endpoint: None,
            telemetry_interval_hours: 24,
            telemetry_dry_run: false,
            edge_mode: false,
            edge_cloud_endpoint: None,
            edge_sync_interval_secs: 300,
            edge_sync_strategy: "all".to_string(),
            edge_max_storage_mb: 512,
            edge_segment_size_kb: 4096,
            edge_buffer_pool_size: 4,
            edge_disk_threshold: 0.85,
            edge_cpu_throttle: 0.5,
            edge_mmap_preferred: false,
        };

        let result = ServerConfig::from_args(args);
        assert!(result.is_err());
    }

    #[test]
    fn test_server_config_invalid_http_addr() {
        let args = ServerArgs {
            config: None,
            generate_config: false,
            listen_addr: "127.0.0.1:9092".to_string(),
            http_addr: "not-a-valid-addr".to_string(),
            data_dir: PathBuf::from("/tmp/test"),
            log_level: "info".to_string(),
            wal_enabled: true,
            wal_sync_ms: 100,
            wal_sync_mode: "interval".to_string(),
            wal_max_file_size: 100 * 1024 * 1024,
            wal_retention_files: 5,
            wal_max_pending_writes: DEFAULT_WAL_MAX_PENDING_WRITES,
            segment_max_bytes: 1024 * 1024 * 1024,
            max_message_bytes: 1024 * 1024,
            max_index_entries: DEFAULT_MAX_INDEX_ENTRIES,
            tls_enabled: false,
            tls_cert: None,
            tls_key: None,
            tls_min_version: DEFAULT_TLS_MIN_VERSION.to_string(),
            tls_require_client_cert: false,
            tls_ca_cert: None,
            auth_enabled: false,
            auth_sasl_mechanisms: None,
            auth_users_file: None,
            auth_allow_anonymous: false,
            oauth_enabled: false,
            oauth_issuer_url: None,
            oauth_jwks_url: None,
            oauth_audience: None,
            oauth_principal_claim: "sub".to_string(),
            oauth_required_scopes: None,
            oauth_jwks_refresh_interval: 3600,
            acl_enabled: false,
            acl_file: None,
            acl_super_users: None,
            acl_allow_if_no_acls: true,
            node_id: None,
            advertised_addr: None,
            inter_broker_addr: None,
            seed_nodes: None,
            default_replication_factor: 1,
            min_insync_replicas: 1,
            audit_enabled: false,
            audit_log_path: PathBuf::from(DEFAULT_AUDIT_LOG_PATH),
            audit_log_format: DEFAULT_AUDIT_LOG_FORMAT.to_string(),
            audit_events: None,
            audit_log_to_stdout: false,
            max_connections: DEFAULT_MAX_CONNECTIONS,
            max_connections_per_ip: DEFAULT_MAX_CONNECTIONS_PER_IP,
            max_request_size: DEFAULT_MAX_REQUEST_SIZE,
            connection_idle_timeout: DEFAULT_CONNECTION_IDLE_TIMEOUT_SECS,
            produce_rate_limit: DEFAULT_PRODUCE_RATE_LIMIT_BYTES,
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT_SECS,
            drain_timeout: crate::server::shutdown::DEFAULT_DRAIN_TIMEOUT_SECS,
            inter_broker_tls_enabled: false,
            inter_broker_tls_cert: None,
            inter_broker_tls_key: None,
            inter_broker_tls_ca: None,
            inter_broker_tls_verify_peer: true,
            quotas_enabled: false,
            quota_producer_byte_rate: 0,
            quota_consumer_byte_rate: 0,
            in_memory: false,
            simple_enabled: false,
            simple_addr: "0.0.0.0:9095".to_string(),
            playground: false,
            ephemeral: None,
            ephemeral_idle_timeout_secs: None,
            ephemeral_auto_topics: None,
            encryption_enabled: false,
            encryption_key_file: None,
            encryption_key_env_var: None,
            rack_id: None,
            disable_rack_aware_assignment: false,
            auto_create_topics: DEFAULT_AUTO_CREATE_TOPICS,
            runtime_mode: DEFAULT_RUNTIME_MODE.to_string(),
            shard_count: None,
            enable_cpu_affinity: true,
            enable_polling: false,
            io_direct_enabled: false,
            io_direct_alignment: 4096,
            io_direct_fallback: true,
            zerocopy_enabled: true,
            zerocopy_mmap: true,
            zerocopy_mmap_threshold: 1048576,
            zerocopy_buffer_pool_size: 16,
            zerocopy_sendfile: true,
            zerocopy_response_cache: true,
            zerocopy_cache_size_mb: 128,
            otel_enabled: false,
            otel_endpoint: None,
            otel_service_name: "streamline".to_string(),
            otel_sampling_ratio: 1.0,
            otel_timeout_secs: 10,
            storage_mode: None,
            s3_bucket: None,
            s3_prefix: None,
            s3_region: None,
            s3_endpoint: None,
            s3_access_key_id: None,
            s3_secret_access_key: None,
            s3_write_buffer_mb: 64,
            s3_flush_interval_ms: 1000,
            s3_upload_parallelism: 4,
            s3_read_cache_mb: 256,
            s3_multipart_threshold_mb: 64,
            s3_multipart_part_size_mb: 16,
            s3_prefetch_enabled: true,
            s3_prefetch_count: 2,
            s3_max_retries: 3,
            s3_retry_backoff_ms: 100,
            telemetry_enabled: false,
            telemetry_endpoint: None,
            telemetry_interval_hours: 24,
            telemetry_dry_run: false,
            edge_mode: false,
            edge_cloud_endpoint: None,
            edge_sync_interval_secs: 300,
            edge_sync_strategy: "all".to_string(),
            edge_max_storage_mb: 512,
            edge_segment_size_kb: 4096,
            edge_buffer_pool_size: 4,
            edge_disk_threshold: 0.85,
            edge_cpu_throttle: 0.5,
            edge_mmap_preferred: false,
        };

        let result = ServerConfig::from_args(args);
        assert!(result.is_err());
    }

    #[test]
    #[cfg(feature = "clustering")]
    fn test_server_config_with_cluster() {
        let args = ServerArgs {
            config: None,
            generate_config: false,
            listen_addr: "127.0.0.1:9092".to_string(),
            http_addr: "127.0.0.1:9094".to_string(),
            data_dir: PathBuf::from("/tmp/test"),
            log_level: "info".to_string(),
            wal_enabled: true,
            wal_sync_ms: 100,
            wal_sync_mode: "interval".to_string(),
            wal_max_file_size: 100 * 1024 * 1024,
            wal_retention_files: 5,
            wal_max_pending_writes: DEFAULT_WAL_MAX_PENDING_WRITES,
            segment_max_bytes: 1024 * 1024 * 1024,
            max_message_bytes: 1024 * 1024,
            max_index_entries: DEFAULT_MAX_INDEX_ENTRIES,
            tls_enabled: false,
            tls_cert: None,
            tls_key: None,
            tls_min_version: DEFAULT_TLS_MIN_VERSION.to_string(),
            tls_require_client_cert: false,
            tls_ca_cert: None,
            auth_enabled: false,
            auth_sasl_mechanisms: None,
            auth_users_file: None,
            auth_allow_anonymous: false,
            oauth_enabled: false,
            oauth_issuer_url: None,
            oauth_jwks_url: None,
            oauth_audience: None,
            oauth_principal_claim: "sub".to_string(),
            oauth_required_scopes: None,
            oauth_jwks_refresh_interval: 3600,
            acl_enabled: false,
            acl_file: None,
            acl_super_users: None,
            acl_allow_if_no_acls: true,
            node_id: Some(1),
            advertised_addr: Some("192.168.1.1:9092".to_string()),
            inter_broker_addr: Some("0.0.0.0:9093".to_string()),
            seed_nodes: Some("192.168.1.2:9093,192.168.1.3:9093".to_string()),
            default_replication_factor: 3,
            min_insync_replicas: 2,
            audit_enabled: false,
            audit_log_path: PathBuf::from(DEFAULT_AUDIT_LOG_PATH),
            audit_log_format: DEFAULT_AUDIT_LOG_FORMAT.to_string(),
            audit_events: None,
            audit_log_to_stdout: false,
            max_connections: DEFAULT_MAX_CONNECTIONS,
            max_connections_per_ip: DEFAULT_MAX_CONNECTIONS_PER_IP,
            max_request_size: DEFAULT_MAX_REQUEST_SIZE,
            connection_idle_timeout: DEFAULT_CONNECTION_IDLE_TIMEOUT_SECS,
            produce_rate_limit: DEFAULT_PRODUCE_RATE_LIMIT_BYTES,
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT_SECS,
            drain_timeout: crate::server::shutdown::DEFAULT_DRAIN_TIMEOUT_SECS,
            inter_broker_tls_enabled: false,
            inter_broker_tls_cert: None,
            inter_broker_tls_key: None,
            inter_broker_tls_ca: None,
            inter_broker_tls_verify_peer: true,
            quotas_enabled: false,
            quota_producer_byte_rate: 0,
            quota_consumer_byte_rate: 0,
            in_memory: false,
            simple_enabled: false,
            simple_addr: "0.0.0.0:9095".to_string(),
            playground: false,
            ephemeral: None,
            ephemeral_idle_timeout_secs: None,
            ephemeral_auto_topics: None,
            encryption_enabled: false,
            encryption_key_file: None,
            encryption_key_env_var: None,
            rack_id: None,
            disable_rack_aware_assignment: false,
            auto_create_topics: DEFAULT_AUTO_CREATE_TOPICS,
            zerocopy_enabled: true,
            zerocopy_mmap: true,
            zerocopy_mmap_threshold: 1048576,
            zerocopy_buffer_pool_size: 16,
            zerocopy_sendfile: true,
            zerocopy_response_cache: true,
            zerocopy_cache_size_mb: 128,
            otel_enabled: false,
            otel_endpoint: None,
            otel_service_name: "streamline".to_string(),
            otel_sampling_ratio: 1.0,
            otel_timeout_secs: 10,
            storage_mode: None,
            s3_bucket: None,
            s3_prefix: None,
            s3_region: None,
            s3_endpoint: None,
            s3_access_key_id: None,
            s3_secret_access_key: None,
            s3_write_buffer_mb: 64,
            s3_flush_interval_ms: 1000,
            s3_upload_parallelism: 4,
            s3_read_cache_mb: 256,
            s3_multipart_threshold_mb: 64,
            s3_multipart_part_size_mb: 16,
            s3_prefetch_enabled: true,
            s3_prefetch_count: 2,
            s3_max_retries: 3,
            s3_retry_backoff_ms: 100,
            runtime_mode: DEFAULT_RUNTIME_MODE.to_string(),
            shard_count: None,
            enable_cpu_affinity: true,
            enable_polling: false,
            io_direct_enabled: false,
            io_direct_alignment: 4096,
            io_direct_fallback: true,
            telemetry_enabled: false,
            telemetry_endpoint: None,
            telemetry_interval_hours: 24,
            telemetry_dry_run: false,
            edge_mode: false,
            edge_cloud_endpoint: None,
            edge_sync_interval_secs: 300,
            edge_sync_strategy: "all".to_string(),
            edge_max_storage_mb: 512,
            edge_segment_size_kb: 4096,
            edge_buffer_pool_size: 4,
            edge_disk_threshold: 0.85,
            edge_cpu_throttle: 0.5,
            edge_mmap_preferred: false,
        };

        let config = ServerConfig::from_args(args).unwrap();
        assert!(config.is_cluster_mode());
        assert_eq!(config.node_id(), 1);

        let cluster = config.cluster.unwrap();
        assert_eq!(cluster.node_id, 1);
        assert_eq!(cluster.advertised_addr.to_string(), "192.168.1.1:9092");
        assert_eq!(cluster.inter_broker_addr.to_string(), "0.0.0.0:9093");
        assert_eq!(cluster.seed_nodes.len(), 2);
        assert_eq!(cluster.default_replication_factor, 3);
        assert_eq!(cluster.min_insync_replicas, 2);
    }

    #[test]
    fn test_default_constants() {
        assert_eq!(DEFAULT_KAFKA_ADDR, "0.0.0.0:9092");
        assert_eq!(DEFAULT_HTTP_ADDR, "0.0.0.0:9094");
        assert_eq!(DEFAULT_DATA_DIR, "./data");
        assert_eq!(DEFAULT_LOG_LEVEL, "info");
        // DEFAULT_WAL_ENABLED is true - tested via WalConfig::default()
        assert_eq!(DEFAULT_WAL_SYNC_MS, 100);
        assert_eq!(DEFAULT_WAL_SYNC_MODE, "interval");
        assert_eq!(DEFAULT_WAL_MAX_FILE_SIZE, 100 * 1024 * 1024);
        assert_eq!(DEFAULT_WAL_RETENTION_FILES, 5);
        assert_eq!(DEFAULT_SEGMENT_MAX_BYTES, 1024 * 1024 * 1024);
        assert_eq!(DEFAULT_MAX_MESSAGE_BYTES, 1024 * 1024);
    }

    #[test]
    fn test_default_socket_addr_constants() {
        // Verify that const socket addresses match the string constants
        let kafka_addr_parsed: SocketAddr = DEFAULT_KAFKA_ADDR
            .parse()
            .expect("DEFAULT_KAFKA_ADDR should be a valid socket address");
        assert_eq!(DEFAULT_KAFKA_SOCKET_ADDR, kafka_addr_parsed);
        assert_eq!(DEFAULT_KAFKA_SOCKET_ADDR.port(), 9092);

        let http_addr_parsed: SocketAddr = DEFAULT_HTTP_ADDR
            .parse()
            .expect("DEFAULT_HTTP_ADDR should be a valid socket address");
        assert_eq!(DEFAULT_HTTP_SOCKET_ADDR, http_addr_parsed);
        assert_eq!(DEFAULT_HTTP_SOCKET_ADDR.port(), 9094);
    }

    #[test]
    fn test_server_config_serialization() {
        let config = ServerConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        assert!(json.contains("listen_addr"));
        assert!(json.contains("9092"));
    }

    #[test]
    fn test_storage_config_serialization() {
        let config = StorageConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: StorageConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.wal.enabled, config.wal.enabled);
        assert_eq!(
            deserialized.wal.sync_interval_ms,
            config.wal.sync_interval_ms
        );
        assert_eq!(deserialized.segment_max_bytes, config.segment_max_bytes);
        assert_eq!(deserialized.max_message_bytes, config.max_message_bytes);
    }

    #[test]
    fn test_wal_config_default() {
        let config = WalConfig::default();
        assert!(config.enabled);
        assert_eq!(config.sync_interval_ms, 100);
        assert_eq!(config.sync_mode, "interval");
        assert_eq!(config.max_file_size, 100 * 1024 * 1024);
        assert_eq!(config.retention_files, 5);
    }

    #[test]
    fn test_config_validate_default_passes() {
        let config = ServerConfig::default();
        // Default config should pass validation (except for auto_create_topics warning)
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_validate_invalid_wal_sync_mode() {
        let mut config = ServerConfig::default();
        config.storage.wal.sync_mode = "invalid_mode".to_string();
        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid WAL sync mode"));
    }

    #[test]
    fn test_config_validate_message_size_larger_than_segment() {
        let mut config = ServerConfig::default();
        config.storage.max_message_bytes = 2 * 1024 * 1024 * 1024; // 2GB
        config.storage.segment_max_bytes = 1024 * 1024 * 1024; // 1GB
        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("max_message_bytes"));
    }

    #[test]
    fn test_config_validate_segment_too_small() {
        let mut config = ServerConfig::default();
        config.storage.segment_max_bytes = 1024; // 1KB, too small
        config.storage.max_message_bytes = 512; // Must be smaller than segment
        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("segment_max_bytes must be at least 1MB"));
    }

    #[test]
    fn test_config_validate_invalid_tls_version() {
        let mut config = ServerConfig::default();
        config.tls.enabled = true;
        config.tls.cert_path = PathBuf::from("/some/cert.pem");
        config.tls.key_path = PathBuf::from("/some/key.pem");
        config.tls.min_version = "1.0".to_string(); // Invalid
        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid TLS min version"));
    }

    #[test]
    fn test_config_validate_mtls_requires_ca_cert() {
        let mut config = ServerConfig::default();
        config.tls.enabled = true;
        config.tls.cert_path = PathBuf::from("/some/cert.pem");
        config.tls.key_path = PathBuf::from("/some/key.pem");
        config.tls.require_client_cert = true;
        config.tls.ca_cert_path = None; // Missing CA cert
        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("mTLS requires ca_cert_path"));
    }

    #[test]
    #[cfg(feature = "auth")]
    fn test_config_validate_invalid_sasl_mechanism() {
        let mut config = ServerConfig::default();
        config.auth.enabled = true;
        config.auth.sasl_mechanisms = vec!["INVALID_MECHANISM".to_string()];
        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid SASL mechanism"));
    }

    #[test]
    #[cfg(feature = "auth")]
    fn test_config_validate_oauthbearer_requires_oauth_enabled() {
        let mut config = ServerConfig::default();
        config.auth.enabled = true;
        config.auth.sasl_mechanisms = vec!["OAUTHBEARER".to_string()];
        config.auth.oauth.enabled = false;
        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("OAUTHBEARER mechanism requires OAuth"));
    }

    #[test]
    fn test_config_validate_max_connections_too_low() {
        let mut config = ServerConfig::default();
        config.limits.max_connections = 5; // Too low
        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("max_connections must be at least 10"));
    }

    #[test]
    fn test_config_validate_tls_enabled_empty_cert_path() {
        let mut config = ServerConfig::default();
        config.tls.enabled = true;
        config.tls.cert_path = PathBuf::new(); // Empty path
        config.tls.key_path = PathBuf::from("/some/key.pem");
        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("TLS is enabled but cert_path is empty"));
    }

    #[test]
    fn test_config_validate_tls_enabled_empty_key_path() {
        let mut config = ServerConfig::default();
        config.tls.enabled = true;
        config.tls.cert_path = PathBuf::from("/some/cert.pem");
        config.tls.key_path = PathBuf::new(); // Empty path
        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("TLS is enabled but key_path is empty"));
    }

    #[test]
    fn test_config_validate_max_connections_per_ip_too_low() {
        let mut config = ServerConfig::default();
        config.limits.max_connections_per_ip = 2; // Too low
        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("max_connections_per_ip must be at least 5"));
    }

    #[test]
    fn test_config_validate_max_request_size_too_small() {
        let mut config = ServerConfig::default();
        config.limits.max_request_size = 512; // Too small
        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("max_request_size must be at least 1KB"));
    }

    #[test]
    fn test_config_validate_max_index_entries_too_small() {
        let mut config = ServerConfig::default();
        config.storage.max_index_entries = 50; // Too small
        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("max_index_entries must be at least 100"));
    }

    #[test]
    fn test_config_validate_zero_limits_allowed() {
        // Zero means unlimited, which is valid
        let mut config = ServerConfig::default();
        config.limits.max_connections = 0;
        config.limits.max_connections_per_ip = 0;
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_validate_valid_tls_config() {
        let mut config = ServerConfig::default();
        config.tls.enabled = true;
        config.tls.cert_path = PathBuf::from("/etc/ssl/cert.pem");
        config.tls.key_path = PathBuf::from("/etc/ssl/key.pem");
        config.tls.min_version = "1.2".to_string();
        config.tls.require_client_cert = false;
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_validate_valid_tls13() {
        let mut config = ServerConfig::default();
        config.tls.enabled = true;
        config.tls.cert_path = PathBuf::from("/etc/ssl/cert.pem");
        config.tls.key_path = PathBuf::from("/etc/ssl/key.pem");
        config.tls.min_version = "1.3".to_string();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_validate_valid_mtls() {
        let mut config = ServerConfig::default();
        config.tls.enabled = true;
        config.tls.cert_path = PathBuf::from("/etc/ssl/cert.pem");
        config.tls.key_path = PathBuf::from("/etc/ssl/key.pem");
        config.tls.require_client_cert = true;
        config.tls.ca_cert_path = Some(PathBuf::from("/etc/ssl/ca.pem"));
        assert!(config.validate().is_ok());
    }

    #[test]
    #[cfg(feature = "auth")]
    fn test_config_validate_oauthbearer_requires_issuer_or_jwks() {
        let mut config = ServerConfig::default();
        config.auth.enabled = true;
        config.auth.sasl_mechanisms = vec!["OAUTHBEARER".to_string()];
        config.auth.oauth.enabled = true;
        config.auth.oauth.issuer_url = None;
        config.auth.oauth.jwks_url = None;
        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("issuer_url or jwks_url"));
    }

    #[test]
    #[cfg(feature = "auth")]
    fn test_config_validate_valid_auth_with_plain() {
        let mut config = ServerConfig::default();
        config.auth.enabled = true;
        config.auth.sasl_mechanisms = vec!["PLAIN".to_string()];
        // This should validate (with a warning about PLAIN without TLS)
        assert!(config.validate().is_ok());
    }

    #[test]
    #[cfg(feature = "auth")]
    fn test_config_validate_valid_auth_with_scram() {
        let mut config = ServerConfig::default();
        config.auth.enabled = true;
        config.auth.sasl_mechanisms =
            vec!["SCRAM-SHA-256".to_string(), "SCRAM-SHA-512".to_string()];
        assert!(config.validate().is_ok());
    }

    #[test]
    #[cfg(feature = "clustering")]
    fn test_config_validate_cluster_no_seed_nodes() {
        let mut config = ServerConfig::default();
        config.cluster = Some(crate::cluster::ClusterConfig {
            node_id: 1,
            seed_nodes: vec![], // Empty - invalid
            ..Default::default()
        });
        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("at least one seed node"));
    }

    #[test]
    #[cfg(feature = "clustering")]
    fn test_config_validate_cluster_invalid_replication_factor() {
        let mut config = ServerConfig::default();
        config.cluster = Some(crate::cluster::ClusterConfig {
            node_id: 1,
            seed_nodes: vec!["127.0.0.1:9093".parse().unwrap()],
            default_replication_factor: 0, // Invalid
            ..Default::default()
        });
        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("default_replication_factor must be at least 1"));
    }

    #[test]
    fn test_config_validate_all_valid_sync_modes() {
        for sync_mode in ["interval", "every_write", "os_default"] {
            let mut config = ServerConfig::default();
            config.storage.wal.sync_mode = sync_mode.to_string();
            // os_default generates a warning but should still validate
            assert!(
                config.validate().is_ok(),
                "sync_mode '{}' should be valid",
                sync_mode
            );
        }
    }
}
