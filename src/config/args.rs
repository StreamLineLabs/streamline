//! Command-line arguments for Streamline server
//!
//! This module defines the CLI arguments structure using clap.

use clap::Parser;
use std::path::PathBuf;

use super::defaults::*;

/// Command-line arguments for Streamline server
#[derive(Parser, Debug, Clone)]
#[command(name = "streamline")]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(author = "Jose David Baena")]
#[command(about = "A developer-first, operationally simple streaming solution")]
pub struct ServerArgs {
    /// Path to configuration file (TOML format)
    /// If not specified, looks for streamline.toml in current directory,
    /// /etc/streamline/, or ~/.config/streamline/
    #[arg(short, long, env = "STREAMLINE_CONFIG")]
    pub config: Option<PathBuf>,

    /// Generate example configuration file and exit
    #[arg(long)]
    pub generate_config: bool,

    /// Address to listen on for Kafka protocol
    #[arg(long, env = "STREAMLINE_LISTEN_ADDR", default_value = DEFAULT_KAFKA_ADDR)]
    pub listen_addr: String,

    /// Address to listen on for HTTP API
    #[arg(long, env = "STREAMLINE_HTTP_ADDR", default_value = DEFAULT_HTTP_ADDR)]
    pub http_addr: String,

    /// Data directory for storage
    #[arg(long, env = "STREAMLINE_DATA_DIR", default_value = DEFAULT_DATA_DIR)]
    pub data_dir: PathBuf,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long, env = "STREAMLINE_LOG_LEVEL", default_value = DEFAULT_LOG_LEVEL)]
    pub log_level: String,

    /// Enable WAL (Write-Ahead Log) for durability.
    /// Disabling WAL removes durability guarantees - not recommended for production.
    #[arg(long, env = "STREAMLINE_WAL_ENABLED", default_value_t = DEFAULT_WAL_ENABLED)]
    pub wal_enabled: bool,

    /// WAL sync interval in milliseconds (used when sync_mode is "interval").
    /// Lower values = better durability, higher values = better throughput.
    /// Data written within this interval may be lost on crash.
    #[arg(long, env = "STREAMLINE_WAL_SYNC_MS", default_value_t = DEFAULT_WAL_SYNC_MS)]
    pub wal_sync_ms: u64,

    /// WAL sync mode - controls durability vs performance trade-off:
    ///   "every_write" - fsync after each produce (safest, slowest)
    ///   "interval"    - fsync every N ms (balanced, default)
    ///   "os_default"  - OS buffer flush only (fastest, least durable)
    #[arg(long, env = "STREAMLINE_WAL_SYNC_MODE", default_value = DEFAULT_WAL_SYNC_MODE)]
    pub wal_sync_mode: String,

    /// Maximum WAL file size before rotation (in bytes)
    #[arg(long, env = "STREAMLINE_WAL_MAX_FILE_SIZE", default_value_t = DEFAULT_WAL_MAX_FILE_SIZE)]
    pub wal_max_file_size: u64,

    /// Number of WAL files to retain
    #[arg(long, env = "STREAMLINE_WAL_RETENTION_FILES", default_value_t = DEFAULT_WAL_RETENTION_FILES)]
    pub wal_retention_files: u32,

    /// Maximum pending WAL writes before backpressure is applied.
    /// When this limit is reached, new writes will be rejected with an error
    /// until the write queue drains. This prevents unbounded memory growth
    /// when writes arrive faster than they can be persisted.
    #[arg(long, env = "STREAMLINE_WAL_MAX_PENDING_WRITES", default_value_t = DEFAULT_WAL_MAX_PENDING_WRITES)]
    pub wal_max_pending_writes: usize,

    /// Maximum segment size in bytes
    #[arg(long, env = "STREAMLINE_SEGMENT_MAX_BYTES", default_value_t = DEFAULT_SEGMENT_MAX_BYTES)]
    pub segment_max_bytes: u64,

    /// Maximum message size in bytes
    #[arg(long, env = "STREAMLINE_MAX_MESSAGE_BYTES", default_value_t = DEFAULT_MAX_MESSAGE_BYTES)]
    pub max_message_bytes: u64,

    /// Maximum index entries per segment (prevents unbounded memory growth)
    #[arg(long, env = "STREAMLINE_MAX_INDEX_ENTRIES", default_value_t = DEFAULT_MAX_INDEX_ENTRIES)]
    pub max_index_entries: usize,

    /// Enable automatic topic creation on produce.
    /// When enabled (default), topics are created automatically when a producer sends to a non-existent topic.
    /// Disable in production to prevent accidental topic creation from typos or misconfigurations.
    #[arg(long, env = "STREAMLINE_AUTO_CREATE_TOPICS", default_value_t = DEFAULT_AUTO_CREATE_TOPICS)]
    pub auto_create_topics: bool,

    // ===== Runtime Mode Options =====
    /// Runtime mode for the server:
    ///   "tokio"   - Standard Tokio multi-threaded runtime (default, backwards compatible)
    ///   "sharded" - Thread-per-core runtime with CPU affinity for high performance
    /// The sharded runtime provides better latency and throughput through CPU pinning
    /// and lock-free cross-core communication, but requires more careful resource planning.
    #[arg(long, env = "STREAMLINE_RUNTIME_MODE", default_value = DEFAULT_RUNTIME_MODE)]
    pub runtime_mode: String,

    /// Number of shards for the sharded runtime (default: number of CPU cores)
    /// Only used when runtime_mode is "sharded"
    #[arg(long, env = "STREAMLINE_SHARD_COUNT")]
    pub shard_count: Option<usize>,

    /// Enable CPU core affinity for sharded runtime (default: true)
    /// Pins each shard thread to a specific CPU core for cache locality
    #[arg(long, env = "STREAMLINE_ENABLE_CPU_AFFINITY", default_value_t = true)]
    pub enable_cpu_affinity: bool,

    /// Enable busy-polling mode for sharded runtime (default: false)
    /// Provides lower latency but uses more CPU
    #[arg(long, env = "STREAMLINE_ENABLE_POLLING")]
    pub enable_polling: bool,

    // ===== Diskless Storage / S3 Options =====
    /// Storage mode for topics (local, hybrid, diskless)
    /// - local: Traditional disk storage (default)
    /// - hybrid: Local WAL + S3 segments
    /// - diskless: All data written directly to S3
    #[arg(long, env = "STREAMLINE_STORAGE_MODE")]
    pub storage_mode: Option<String>,

    /// S3 bucket name for diskless/hybrid storage
    #[arg(long, env = "STREAMLINE_S3_BUCKET")]
    pub s3_bucket: Option<String>,

    /// S3 key prefix for organizing data (e.g., "streamline/prod/")
    #[arg(long, env = "STREAMLINE_S3_PREFIX")]
    pub s3_prefix: Option<String>,

    /// S3 region (e.g., "us-east-1")
    #[arg(long, env = "STREAMLINE_S3_REGION")]
    pub s3_region: Option<String>,

    /// S3-compatible endpoint URL (for MinIO, LocalStack, etc.)
    #[arg(long, env = "STREAMLINE_S3_ENDPOINT")]
    pub s3_endpoint: Option<String>,

    /// S3 access key ID (defaults to AWS_ACCESS_KEY_ID env var)
    #[arg(long, env = "STREAMLINE_S3_ACCESS_KEY_ID")]
    pub s3_access_key_id: Option<String>,

    /// S3 secret access key (defaults to AWS_SECRET_ACCESS_KEY env var)
    #[arg(long, env = "STREAMLINE_S3_SECRET_ACCESS_KEY")]
    pub s3_secret_access_key: Option<String>,

    /// Write buffer size in MB before flushing to S3 (default: 64)
    #[arg(long, env = "STREAMLINE_S3_WRITE_BUFFER_MB", default_value_t = 64)]
    pub s3_write_buffer_mb: usize,

    /// Maximum time in milliseconds before flushing buffer to S3 (default: 1000)
    #[arg(long, env = "STREAMLINE_S3_FLUSH_INTERVAL_MS", default_value_t = 1000)]
    pub s3_flush_interval_ms: u64,

    /// Number of parallel S3 uploads (default: 4)
    #[arg(long, env = "STREAMLINE_S3_UPLOAD_PARALLELISM", default_value_t = 4)]
    pub s3_upload_parallelism: usize,

    /// Read cache size in MB for S3 data (default: 256)
    #[arg(long, env = "STREAMLINE_S3_READ_CACHE_MB", default_value_t = 256)]
    pub s3_read_cache_mb: usize,

    /// Enable S3 multipart uploads for segments larger than this size in MB (default: 64)
    #[arg(
        long,
        env = "STREAMLINE_S3_MULTIPART_THRESHOLD_MB",
        default_value_t = 64
    )]
    pub s3_multipart_threshold_mb: usize,

    /// S3 multipart upload part size in MB (default: 16, min: 5)
    #[arg(
        long,
        env = "STREAMLINE_S3_MULTIPART_PART_SIZE_MB",
        default_value_t = 16
    )]
    pub s3_multipart_part_size_mb: usize,

    /// Enable prefetching of next segments on sequential reads
    #[arg(long, env = "STREAMLINE_S3_PREFETCH_ENABLED", default_value_t = true)]
    pub s3_prefetch_enabled: bool,

    /// Number of segments to prefetch ahead during sequential reads (default: 2)
    #[arg(long, env = "STREAMLINE_S3_PREFETCH_COUNT", default_value_t = 2)]
    pub s3_prefetch_count: usize,

    /// Maximum retry attempts for S3 operations (default: 3)
    #[arg(long, env = "STREAMLINE_S3_MAX_RETRIES", default_value_t = 3)]
    pub s3_max_retries: usize,

    /// Initial retry backoff in milliseconds (default: 100)
    #[arg(long, env = "STREAMLINE_S3_RETRY_BACKOFF_MS", default_value_t = 100)]
    pub s3_retry_backoff_ms: u64,

    /// Enable TLS/SSL
    #[arg(long, env = "STREAMLINE_TLS_ENABLED")]
    pub tls_enabled: bool,

    /// Path to TLS certificate file (PEM format)
    #[arg(long, env = "STREAMLINE_TLS_CERT")]
    pub tls_cert: Option<PathBuf>,

    /// Path to TLS private key file (PEM format)
    #[arg(long, env = "STREAMLINE_TLS_KEY")]
    pub tls_key: Option<PathBuf>,

    /// Minimum TLS version (1.2 or 1.3)
    #[arg(long, env = "STREAMLINE_TLS_MIN_VERSION", default_value = DEFAULT_TLS_MIN_VERSION)]
    pub tls_min_version: String,

    /// Require client certificate (mTLS)
    #[arg(long, env = "STREAMLINE_TLS_REQUIRE_CLIENT_CERT")]
    pub tls_require_client_cert: bool,

    /// Path to CA certificate for client verification
    #[arg(long, env = "STREAMLINE_TLS_CA_CERT")]
    pub tls_ca_cert: Option<PathBuf>,

    /// Enable SASL authentication
    #[arg(long, env = "STREAMLINE_AUTH_ENABLED")]
    pub auth_enabled: bool,

    /// SASL mechanisms (comma-separated, e.g., "PLAIN,SCRAM-SHA-256")
    #[arg(long, env = "STREAMLINE_AUTH_SASL_MECHANISMS")]
    pub auth_sasl_mechanisms: Option<String>,

    /// Path to users file (YAML format)
    #[arg(long, env = "STREAMLINE_AUTH_USERS_FILE")]
    pub auth_users_file: Option<PathBuf>,

    /// Allow anonymous connections when auth is enabled
    #[arg(long, env = "STREAMLINE_AUTH_ALLOW_ANONYMOUS")]
    pub auth_allow_anonymous: bool,

    // ===== OAuth 2.0 / OIDC Options =====
    /// Enable OAuth 2.0 / OIDC authentication (requires OAUTHBEARER in sasl-mechanisms)
    #[arg(long, env = "STREAMLINE_OAUTH_ENABLED")]
    pub oauth_enabled: bool,

    /// OIDC issuer URL (e.g., `https://auth.example.com`)
    #[arg(long, env = "STREAMLINE_OAUTH_ISSUER_URL")]
    pub oauth_issuer_url: Option<String>,

    /// JWKS (JSON Web Key Set) URL for token validation
    /// If not specified, will be discovered from issuer URL
    #[arg(long, env = "STREAMLINE_OAUTH_JWKS_URL")]
    pub oauth_jwks_url: Option<String>,

    /// Expected audience claim in OAuth tokens
    #[arg(long, env = "STREAMLINE_OAUTH_AUDIENCE")]
    pub oauth_audience: Option<String>,

    /// Claim name to use as the principal/username (default: "sub")
    #[arg(long, env = "STREAMLINE_OAUTH_PRINCIPAL_CLAIM", default_value = "sub")]
    pub oauth_principal_claim: String,

    /// Required scopes for OAuth tokens (comma-separated)
    #[arg(long, env = "STREAMLINE_OAUTH_REQUIRED_SCOPES")]
    pub oauth_required_scopes: Option<String>,

    /// JWKS refresh interval in seconds (default: 3600)
    #[arg(
        long,
        env = "STREAMLINE_OAUTH_JWKS_REFRESH_INTERVAL",
        default_value_t = 3600
    )]
    pub oauth_jwks_refresh_interval: u64,

    /// Enable ACL-based authorization
    #[arg(long, env = "STREAMLINE_ACL_ENABLED")]
    pub acl_enabled: bool,

    /// Path to ACL file (YAML format)
    #[arg(long, env = "STREAMLINE_ACL_FILE")]
    pub acl_file: Option<PathBuf>,

    /// Super users who bypass ACL checks (comma-separated, e.g., "User:admin,User:operator")
    #[arg(long, env = "STREAMLINE_ACL_SUPER_USERS")]
    pub acl_super_users: Option<String>,

    /// Allow all operations if no ACLs are configured
    #[arg(long, env = "STREAMLINE_ACL_ALLOW_IF_NO_ACLS", default_value_t = true)]
    pub acl_allow_if_no_acls: bool,

    // ===== Clustering Options =====
    /// Node ID for cluster mode (enables clustering when set)
    #[arg(long, env = "STREAMLINE_NODE_ID")]
    pub node_id: Option<u64>,

    /// Advertised address for clients and other nodes to connect
    #[arg(long, env = "STREAMLINE_ADVERTISED_ADDR")]
    pub advertised_addr: Option<String>,

    /// Address for inter-broker communication (Raft and replication)
    #[arg(long, env = "STREAMLINE_INTER_BROKER_ADDR")]
    pub inter_broker_addr: Option<String>,

    /// Seed nodes for cluster discovery (comma-separated inter-broker addresses)
    #[arg(long, env = "STREAMLINE_SEED_NODES")]
    pub seed_nodes: Option<String>,

    /// Default replication factor for new topics
    #[arg(
        long,
        env = "STREAMLINE_DEFAULT_REPLICATION_FACTOR",
        default_value_t = 1
    )]
    pub default_replication_factor: i16,

    /// Minimum in-sync replicas required for writes with acks=all
    #[arg(long, env = "STREAMLINE_MIN_INSYNC_REPLICAS", default_value_t = 1)]
    pub min_insync_replicas: i16,

    /// Rack identifier for rack-aware replica placement (e.g., "us-east-1a", "rack1")
    #[arg(long, env = "STREAMLINE_RACK_ID")]
    pub rack_id: Option<String>,

    /// Disable rack-aware replica assignment
    #[arg(long, env = "STREAMLINE_DISABLE_RACK_AWARE_ASSIGNMENT")]
    pub disable_rack_aware_assignment: bool,

    // ===== Inter-Broker TLS Options =====
    /// Enable TLS for inter-broker communication
    #[arg(long, env = "STREAMLINE_INTER_BROKER_TLS_ENABLED")]
    pub inter_broker_tls_enabled: bool,

    /// Path to TLS certificate file for inter-broker communication
    #[arg(long, env = "STREAMLINE_INTER_BROKER_TLS_CERT")]
    pub inter_broker_tls_cert: Option<PathBuf>,

    /// Path to TLS private key file for inter-broker communication
    #[arg(long, env = "STREAMLINE_INTER_BROKER_TLS_KEY")]
    pub inter_broker_tls_key: Option<PathBuf>,

    /// Path to CA certificate for verifying peer certificates in inter-broker communication
    #[arg(long, env = "STREAMLINE_INTER_BROKER_TLS_CA")]
    pub inter_broker_tls_ca: Option<PathBuf>,

    /// Verify peer certificates in inter-broker communication (default: true)
    #[arg(
        long,
        env = "STREAMLINE_INTER_BROKER_TLS_VERIFY_PEER",
        default_value_t = true
    )]
    pub inter_broker_tls_verify_peer: bool,

    // ===== Audit Logging Options =====
    /// Enable security audit logging
    #[arg(long, env = "STREAMLINE_AUDIT_ENABLED")]
    pub audit_enabled: bool,

    /// Path to audit log file
    #[arg(long, env = "STREAMLINE_AUDIT_LOG_PATH", default_value = DEFAULT_AUDIT_LOG_PATH)]
    pub audit_log_path: PathBuf,

    /// Audit log format: "json" or "text"
    #[arg(long, env = "STREAMLINE_AUDIT_LOG_FORMAT", default_value = DEFAULT_AUDIT_LOG_FORMAT)]
    pub audit_log_format: String,

    /// Audit events to log (comma-separated, empty = all events)
    /// Valid events: AUTH_SUCCESS, AUTH_FAILURE, AUTH_LOGOUT, ACL_ALLOW, ACL_DENY,
    /// TOPIC_CREATE, TOPIC_DELETE, NODE_JOIN, NODE_LEAVE, LEADER_CHANGE, CONFIG_CHANGE, CONNECTION
    #[arg(long, env = "STREAMLINE_AUDIT_EVENTS")]
    pub audit_events: Option<String>,

    /// Also log audit events to stdout
    #[arg(long, env = "STREAMLINE_AUDIT_LOG_TO_STDOUT")]
    pub audit_log_to_stdout: bool,

    // ===== Resource Limits Options =====
    /// Maximum total connections (0 = unlimited)
    #[arg(long, env = "STREAMLINE_MAX_CONNECTIONS", default_value_t = DEFAULT_MAX_CONNECTIONS)]
    pub max_connections: u32,

    /// Maximum connections per IP (0 = unlimited)
    #[arg(long, env = "STREAMLINE_MAX_CONNECTIONS_PER_IP", default_value_t = DEFAULT_MAX_CONNECTIONS_PER_IP)]
    pub max_connections_per_ip: u32,

    /// Maximum request size in bytes (0 = unlimited)
    #[arg(long, env = "STREAMLINE_MAX_REQUEST_SIZE", default_value_t = DEFAULT_MAX_REQUEST_SIZE)]
    pub max_request_size: u64,

    /// Connection idle timeout in seconds (0 = no timeout)
    #[arg(long, env = "STREAMLINE_CONNECTION_IDLE_TIMEOUT", default_value_t = DEFAULT_CONNECTION_IDLE_TIMEOUT_SECS)]
    pub connection_idle_timeout: u64,

    /// Produce rate limit in bytes per second per client (0 = unlimited)
    #[arg(long, env = "STREAMLINE_PRODUCE_RATE_LIMIT", default_value_t = DEFAULT_PRODUCE_RATE_LIMIT_BYTES)]
    pub produce_rate_limit: u64,

    // ===== Client Quota Options =====
    /// Enable client quotas for producer/consumer throttling
    #[arg(long, env = "STREAMLINE_QUOTAS_ENABLED")]
    pub quotas_enabled: bool,

    /// Default producer byte rate quota in bytes per second (0 = unlimited)
    #[arg(long, env = "STREAMLINE_QUOTA_PRODUCER_BYTE_RATE", default_value_t = 0)]
    pub quota_producer_byte_rate: u64,

    /// Default consumer byte rate quota in bytes per second (0 = unlimited)
    #[arg(long, env = "STREAMLINE_QUOTA_CONSUMER_BYTE_RATE", default_value_t = 0)]
    pub quota_consumer_byte_rate: u64,

    /// Graceful shutdown timeout in seconds
    #[arg(long, env = "STREAMLINE_SHUTDOWN_TIMEOUT", default_value_t = DEFAULT_SHUTDOWN_TIMEOUT_SECS)]
    pub shutdown_timeout: u64,

    /// Connection drain timeout in seconds during shutdown
    #[arg(long, env = "STREAMLINE_DRAIN_TIMEOUT", default_value_t = crate::server::shutdown::DEFAULT_DRAIN_TIMEOUT_SECS)]
    pub drain_timeout: u64,

    /// Enable in-memory mode (no data persisted to disk)
    /// Useful for testing, development, and ephemeral workloads
    #[arg(long, env = "STREAMLINE_IN_MEMORY", default_value_t = DEFAULT_IN_MEMORY)]
    pub in_memory: bool,

    // ===== Simple Protocol Options =====
    /// Enable simple text protocol (Redis RESP-like) for easy integration
    #[arg(long, env = "STREAMLINE_SIMPLE_ENABLED")]
    pub simple_enabled: bool,

    /// Address to listen on for simple protocol
    #[arg(long, env = "STREAMLINE_SIMPLE_ADDR", default_value = "0.0.0.0:9095")]
    pub simple_addr: String,

    /// Start in playground mode with demo topics pre-created
    /// Enables in-memory mode and creates demo topics for quick exploration
    #[arg(long, env = "STREAMLINE_PLAYGROUND")]
    pub playground: bool,

    // ===== Encryption at Rest Options =====
    /// Enable encryption at rest for segment data
    #[arg(long, env = "STREAMLINE_ENCRYPTION_ENABLED")]
    pub encryption_enabled: bool,

    /// Path to encryption key file (32 bytes raw or 64 chars hex)
    #[arg(long, env = "STREAMLINE_ENCRYPTION_KEY_FILE")]
    pub encryption_key_file: Option<PathBuf>,

    /// Environment variable name containing the encryption key (hex-encoded)
    #[arg(long, env = "STREAMLINE_ENCRYPTION_KEY_ENV_VAR")]
    pub encryption_key_env_var: Option<String>,

    // ===== Zero-Copy Networking Options =====
    /// Enable zero-copy networking optimizations (default: true)
    #[arg(long, env = "STREAMLINE_ZEROCOPY_ENABLED", default_value = "true")]
    pub zerocopy_enabled: bool,

    /// Enable memory-mapped I/O for sealed segments (default: true)
    #[arg(long, env = "STREAMLINE_ZEROCOPY_MMAP", default_value = "true")]
    pub zerocopy_mmap: bool,

    /// Minimum segment size in bytes to use mmap (default: 1MB)
    #[arg(
        long,
        env = "STREAMLINE_ZEROCOPY_MMAP_THRESHOLD",
        default_value = "1048576"
    )]
    pub zerocopy_mmap_threshold: u64,

    /// Number of buffers in the pool (default: 16)
    #[arg(
        long,
        env = "STREAMLINE_ZEROCOPY_BUFFER_POOL_SIZE",
        default_value = "16"
    )]
    pub zerocopy_buffer_pool_size: usize,

    /// Enable sendfile for plain TCP connections (default: true, auto-disabled for TLS)
    #[arg(long, env = "STREAMLINE_ZEROCOPY_SENDFILE", default_value = "true")]
    pub zerocopy_sendfile: bool,

    /// Enable response caching (default: true)
    #[arg(
        long,
        env = "STREAMLINE_ZEROCOPY_RESPONSE_CACHE",
        default_value = "true"
    )]
    pub zerocopy_response_cache: bool,

    /// Response cache size in MB (default: 128)
    #[arg(long, env = "STREAMLINE_ZEROCOPY_CACHE_SIZE_MB", default_value = "128")]
    pub zerocopy_cache_size_mb: u64,

    // ===== Direct I/O Options =====
    /// Enable O_DIRECT I/O to bypass OS page cache (default: false)
    /// When enabled, segment files are opened with O_DIRECT flag, providing:
    /// - Predictable memory usage (no surprise page cache evictions)
    /// - Reduced double-buffering overhead
    /// - Lower P99.99 latency spikes during heavy load
    ///
    /// Note: Requires filesystem support (ext4, xfs, btrfs) and aligned buffers.
    #[arg(long, env = "STREAMLINE_IO_DIRECT_ENABLED")]
    pub io_direct_enabled: bool,

    /// Buffer alignment for O_DIRECT I/O in bytes (default: 4096)
    /// Buffers must be aligned to this boundary for O_DIRECT to work correctly.
    /// Most filesystems require 512 or 4096 byte alignment.
    #[arg(long, env = "STREAMLINE_IO_DIRECT_ALIGNMENT", default_value_t = 4096)]
    pub io_direct_alignment: usize,

    /// Fallback to buffered I/O if O_DIRECT is not supported (default: true)
    /// If the filesystem doesn't support O_DIRECT, gracefully fall back
    /// to standard buffered I/O instead of failing.
    #[arg(long, env = "STREAMLINE_IO_DIRECT_FALLBACK", default_value_t = true)]
    pub io_direct_fallback: bool,

    // ===== OpenTelemetry Tracing Options =====
    /// Enable OpenTelemetry distributed tracing
    #[arg(long, env = "OTEL_TRACING_ENABLED")]
    pub otel_enabled: bool,

    /// OpenTelemetry OTLP endpoint (e.g., "http://localhost:4317")
    #[arg(long, env = "OTEL_EXPORTER_OTLP_ENDPOINT")]
    pub otel_endpoint: Option<String>,

    /// Service name for OpenTelemetry traces (default: "streamline")
    #[arg(long, env = "OTEL_SERVICE_NAME", default_value = "streamline")]
    pub otel_service_name: String,

    /// Trace sampling ratio (0.0 to 1.0, default: 1.0 = sample everything)
    #[arg(long, env = "OTEL_TRACES_SAMPLER_ARG", default_value_t = 1.0)]
    pub otel_sampling_ratio: f64,

    /// OTLP export timeout in seconds (default: 10)
    #[arg(long, env = "OTEL_EXPORTER_OTLP_TIMEOUT", default_value_t = 10)]
    pub otel_timeout_secs: u64,

    // ===== Anonymous Telemetry Options =====
    /// Enable anonymous usage telemetry (default: false, opt-in)
    /// When enabled, collects anonymous, aggregated usage data to help improve Streamline.
    /// No PII, no message content, no IP addresses. See docs/TELEMETRY.md for details.
    #[arg(long, env = "STREAMLINE_TELEMETRY_ENABLED")]
    pub telemetry_enabled: bool,

    /// Telemetry endpoint URL (default: https://telemetry.streamline.dev/v1/report)
    #[arg(long, env = "STREAMLINE_TELEMETRY_ENDPOINT")]
    pub telemetry_endpoint: Option<String>,

    /// Telemetry reporting interval in hours (default: 24)
    #[arg(
        long,
        env = "STREAMLINE_TELEMETRY_INTERVAL_HOURS",
        default_value_t = 24
    )]
    pub telemetry_interval_hours: u64,

    /// Enable telemetry dry-run mode (show what would be sent without sending)
    #[arg(long, env = "STREAMLINE_TELEMETRY_DRY_RUN")]
    pub telemetry_dry_run: bool,

    // ===== Edge Deployment Mode Options =====
    /// Enable edge deployment mode with optimized defaults for resource-constrained devices
    #[arg(long, env = "STREAMLINE_EDGE_MODE")]
    pub edge_mode: bool,

    /// Cloud endpoint URL for edge-to-cloud sync (e.g., "https://cloud.example.com:9092")
    #[arg(long, env = "STREAMLINE_EDGE_CLOUD_ENDPOINT")]
    pub edge_cloud_endpoint: Option<String>,

    /// Edge sync interval in seconds (default: 300 = every 5 minutes)
    #[arg(long, env = "STREAMLINE_EDGE_SYNC_INTERVAL_SECS", default_value_t = 300)]
    pub edge_sync_interval_secs: u64,

    /// Edge sync strategy: "all" (sync everything), "latest-only" (latest per key), "sample" (sample subset)
    #[arg(long, env = "STREAMLINE_EDGE_SYNC_STRATEGY", default_value = "all")]
    pub edge_sync_strategy: String,

    /// Maximum edge storage in MB (default: 512)
    #[arg(long, env = "STREAMLINE_EDGE_MAX_STORAGE_MB", default_value_t = 512)]
    pub edge_max_storage_mb: u64,

    /// Edge segment size in KB (default: 4096 = 4MB, smaller for edge)
    #[arg(long, env = "STREAMLINE_EDGE_SEGMENT_SIZE_KB", default_value_t = 4096)]
    pub edge_segment_size_kb: u64,

    /// Edge buffer pool size (default: 4, smaller for low-memory)
    #[arg(long, env = "STREAMLINE_EDGE_BUFFER_POOL_SIZE", default_value_t = 4)]
    pub edge_buffer_pool_size: usize,

    /// Disk usage threshold (0.0-1.0) to trigger automatic segment compaction (default: 0.85)
    #[arg(long, env = "STREAMLINE_EDGE_DISK_THRESHOLD", default_value_t = 0.85)]
    pub edge_disk_threshold: f64,

    /// CPU throttle percentage (0.0-1.0) for battery-powered devices (default: 0.5)
    #[arg(long, env = "STREAMLINE_EDGE_CPU_THROTTLE", default_value_t = 0.5)]
    pub edge_cpu_throttle: f64,

    /// Prefer memory-mapped I/O for low-memory edge environments
    #[arg(long, env = "STREAMLINE_EDGE_MMAP_PREFERRED")]
    pub edge_mmap_preferred: bool,
}
