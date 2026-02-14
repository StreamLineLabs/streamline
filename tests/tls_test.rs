//! TLS integration tests for Streamline

use std::path::PathBuf;
use streamline::config::{
    ServerArgs, ServerConfig, DEFAULT_AUDIT_LOG_FORMAT, DEFAULT_AUDIT_LOG_PATH,
    DEFAULT_CONNECTION_IDLE_TIMEOUT_SECS, DEFAULT_MAX_CONNECTIONS, DEFAULT_MAX_CONNECTIONS_PER_IP,
    DEFAULT_MAX_REQUEST_SIZE, DEFAULT_PRODUCE_RATE_LIMIT_BYTES, DEFAULT_SHUTDOWN_TIMEOUT_SECS,
    DEFAULT_TLS_MIN_VERSION,
};
use streamline::server::shutdown::DEFAULT_DRAIN_TIMEOUT_SECS;
use tempfile::tempdir;

/// Create default test ServerArgs with cluster fields
fn default_test_args(data_dir: PathBuf) -> ServerArgs {
    ServerArgs {
        config: None,
        generate_config: false,
        listen_addr: "127.0.0.1:9092".to_string(),
        http_addr: "127.0.0.1:9094".to_string(),
        data_dir,
        log_level: "info".to_string(),
        wal_enabled: true,
        wal_sync_ms: 100,
        wal_sync_mode: "interval".to_string(),
        wal_max_file_size: 100 * 1024 * 1024,
        wal_retention_files: 5,
        wal_max_pending_writes: streamline::config::DEFAULT_WAL_MAX_PENDING_WRITES,
        segment_max_bytes: 1024 * 1024 * 1024,
        max_message_bytes: 1024 * 1024,
        max_index_entries: streamline::config::DEFAULT_MAX_INDEX_ENTRIES,
        in_memory: false,
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
        // OAuth config fields
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
        // Cluster config fields (disabled by default)
        node_id: None,
        advertised_addr: None,
        inter_broker_addr: None,
        seed_nodes: None,
        default_replication_factor: 1,
        min_insync_replicas: 1,
        // Audit config fields (disabled by default)
        audit_enabled: false,
        audit_log_path: PathBuf::from(DEFAULT_AUDIT_LOG_PATH),
        audit_log_format: DEFAULT_AUDIT_LOG_FORMAT.to_string(),
        audit_events: None,
        audit_log_to_stdout: false,
        // Limits config fields
        max_connections: DEFAULT_MAX_CONNECTIONS,
        max_connections_per_ip: DEFAULT_MAX_CONNECTIONS_PER_IP,
        max_request_size: DEFAULT_MAX_REQUEST_SIZE,
        connection_idle_timeout: DEFAULT_CONNECTION_IDLE_TIMEOUT_SECS,
        produce_rate_limit: DEFAULT_PRODUCE_RATE_LIMIT_BYTES,
        // Shutdown config fields
        shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT_SECS,
        drain_timeout: DEFAULT_DRAIN_TIMEOUT_SECS,
        // Inter-broker TLS config fields
        inter_broker_tls_enabled: false,
        inter_broker_tls_cert: None,
        inter_broker_tls_key: None,
        inter_broker_tls_ca: None,
        inter_broker_tls_verify_peer: true,
        // Quota config fields
        quotas_enabled: false,
        quota_producer_byte_rate: 0,
        quota_consumer_byte_rate: 0,
        // Simple protocol config fields
        simple_enabled: false,
        simple_addr: "0.0.0.0:9095".to_string(),
        // Playground mode
        playground: false,
        // Encryption at rest
        encryption_enabled: false,
        encryption_key_file: None,
        encryption_key_env_var: None,
        // Rack-aware assignment
        rack_id: None,
        disable_rack_aware_assignment: false,
        // Topic auto-creation
        auto_create_topics: true,
        // Zero-copy networking
        zerocopy_enabled: true,
        zerocopy_mmap: true,
        zerocopy_mmap_threshold: 1048576,
        zerocopy_buffer_pool_size: 16,
        zerocopy_sendfile: true,
        zerocopy_response_cache: true,
        zerocopy_cache_size_mb: 128,
        // OpenTelemetry tracing config fields
        otel_enabled: false,
        otel_endpoint: None,
        otel_service_name: "streamline".to_string(),
        otel_sampling_ratio: 1.0,
        otel_timeout_secs: 10,
        // S3/Diskless storage config fields
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
        // Runtime config fields
        runtime_mode: streamline::config::DEFAULT_RUNTIME_MODE.to_string(),
        shard_count: None,
        enable_cpu_affinity: true,
        enable_polling: false,
        // Direct I/O config fields
        io_direct_enabled: false,
        io_direct_alignment: 4096,
        io_direct_fallback: true,
        // Telemetry config fields
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
    }
}

#[test]
fn test_tls_config_validation_missing_cert() {
    let temp_dir = tempdir().unwrap();

    let mut args = default_test_args(temp_dir.path().to_path_buf());
    args.tls_enabled = true;
    args.tls_cert = None; // Missing cert
    args.tls_key = Some(PathBuf::from("key.pem"));

    let result = ServerConfig::from_args(args);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("cert path not provided"));
}

#[test]
fn test_tls_config_validation_missing_key() {
    let temp_dir = tempdir().unwrap();

    let mut args = default_test_args(temp_dir.path().to_path_buf());
    args.tls_enabled = true;
    args.tls_cert = Some(PathBuf::from("cert.pem"));
    args.tls_key = None; // Missing key

    let result = ServerConfig::from_args(args);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("key path not provided"));
}

#[test]
fn test_tls_config_validation_nonexistent_cert() {
    let temp_dir = tempdir().unwrap();

    let mut args = default_test_args(temp_dir.path().to_path_buf());
    args.tls_enabled = true;
    args.tls_cert = Some(PathBuf::from("/nonexistent/cert.pem"));
    args.tls_key = Some(PathBuf::from("/nonexistent/key.pem"));

    let result = ServerConfig::from_args(args);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("not found"));
}

#[test]
fn test_tls_config_disabled() {
    let temp_dir = tempdir().unwrap();

    let args = default_test_args(temp_dir.path().to_path_buf());
    // TLS is disabled by default

    let config = ServerConfig::from_args(args).unwrap();
    assert!(!config.tls.enabled);
}

#[test]
fn test_tls_config_with_valid_certs() {
    let temp_dir = tempdir().unwrap();
    let cert_path = PathBuf::from("tests/certs/server-cert.pem");
    let key_path = PathBuf::from("tests/certs/server-key.pem");

    // Skip test if certificates don't exist
    if !cert_path.exists() || !key_path.exists() {
        eprintln!(
            "Skipping test - certificates not found. Run tests/certs/generate_certs.sh first."
        );
        return;
    }

    let mut args = default_test_args(temp_dir.path().to_path_buf());
    args.tls_enabled = true;
    args.tls_cert = Some(cert_path);
    args.tls_key = Some(key_path);
    args.tls_min_version = "1.2".to_string();

    let config = ServerConfig::from_args(args).unwrap();
    assert!(config.tls.enabled);
    assert_eq!(config.tls.min_version, "1.2");
    assert!(!config.tls.require_client_cert);
}

#[test]
fn test_tls_config_with_mtls() {
    let temp_dir = tempdir().unwrap();
    let cert_path = PathBuf::from("tests/certs/server-cert.pem");
    let key_path = PathBuf::from("tests/certs/server-key.pem");
    let ca_cert_path = PathBuf::from("tests/certs/ca-cert.pem");

    // Skip test if certificates don't exist
    if !cert_path.exists() || !key_path.exists() || !ca_cert_path.exists() {
        eprintln!(
            "Skipping test - certificates not found. Run tests/certs/generate_certs.sh first."
        );
        return;
    }

    let mut args = default_test_args(temp_dir.path().to_path_buf());
    args.tls_enabled = true;
    args.tls_cert = Some(cert_path);
    args.tls_key = Some(key_path);
    args.tls_min_version = "1.3".to_string();
    args.tls_require_client_cert = true;
    args.tls_ca_cert = Some(ca_cert_path);

    let config = ServerConfig::from_args(args).unwrap();
    assert!(config.tls.enabled);
    assert_eq!(config.tls.min_version, "1.3");
    assert!(config.tls.require_client_cert);
    assert!(config.tls.ca_cert_path.is_some());
}

#[test]
fn test_tls_config_mtls_missing_ca_cert() {
    let temp_dir = tempdir().unwrap();
    let cert_path = PathBuf::from("tests/certs/server-cert.pem");
    let key_path = PathBuf::from("tests/certs/server-key.pem");

    // Skip test if certificates don't exist
    if !cert_path.exists() || !key_path.exists() {
        eprintln!(
            "Skipping test - certificates not found. Run tests/certs/generate_certs.sh first."
        );
        return;
    }

    let mut args = default_test_args(temp_dir.path().to_path_buf());
    args.tls_enabled = true;
    args.tls_cert = Some(cert_path);
    args.tls_key = Some(key_path);
    args.tls_require_client_cert = true;
    args.tls_ca_cert = None; // Missing CA cert for mTLS

    // This should succeed in config validation, but will fail when actually loading TLS
    let config = ServerConfig::from_args(args).unwrap();
    assert!(config.tls.enabled);
    assert!(config.tls.require_client_cert);
    assert!(config.tls.ca_cert_path.is_none());
}
