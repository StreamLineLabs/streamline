//! Configuration merging utilities
//!
//! This module provides functions to merge configuration from files
//! with command-line arguments, where CLI arguments take precedence.

use super::args::ServerArgs;
use super::file::ConfigFile;
use super::*;

/// Merge configuration file values with CLI arguments.
/// CLI arguments take precedence over config file values.
/// Only applies config file values where CLI uses defaults.
pub fn merge_config_with_args(mut args: ServerArgs, config: &ConfigFile) -> ServerArgs {
    // Helper macro to apply config value if CLI is at default
    macro_rules! apply_if_default {
        ($field:ident, $config_val:expr, $default:expr) => {
            if let Some(val) = $config_val {
                if args.$field == $default {
                    args.$field = val;
                }
            }
        };
    }

    macro_rules! apply_if_default_string {
        ($field:ident, $config_val:expr, $default:expr) => {
            if let Some(ref val) = $config_val {
                if args.$field == $default {
                    args.$field = val.clone();
                }
            }
        };
    }

    macro_rules! apply_option {
        ($field:ident, $config_val:expr) => {
            if args.$field.is_none() {
                if let Some(val) = $config_val {
                    args.$field = Some(val);
                }
            }
        };
    }

    // Server section
    apply_if_default_string!(listen_addr, config.server.listen_addr, DEFAULT_KAFKA_ADDR);
    apply_if_default_string!(http_addr, config.server.http_addr, DEFAULT_HTTP_ADDR);
    if let Some(ref path) = config.server.data_dir {
        if args.data_dir == std::path::Path::new(DEFAULT_DATA_DIR) {
            args.data_dir = path.clone();
        }
    }
    apply_if_default_string!(log_level, config.server.log_level, DEFAULT_LOG_LEVEL);
    apply_if_default!(
        auto_create_topics,
        config.server.auto_create_topics,
        DEFAULT_AUTO_CREATE_TOPICS
    );
    apply_if_default!(
        shutdown_timeout,
        config.server.shutdown_timeout,
        DEFAULT_SHUTDOWN_TIMEOUT_SECS
    );
    apply_if_default!(
        drain_timeout,
        config.server.drain_timeout,
        crate::server::shutdown::DEFAULT_DRAIN_TIMEOUT_SECS
    );

    // Storage section
    apply_if_default!(
        segment_max_bytes,
        config.storage.segment_max_bytes,
        DEFAULT_SEGMENT_MAX_BYTES
    );
    apply_if_default!(
        max_message_bytes,
        config.storage.max_message_bytes,
        DEFAULT_MAX_MESSAGE_BYTES
    );
    apply_if_default!(
        max_index_entries,
        config.storage.max_index_entries,
        DEFAULT_MAX_INDEX_ENTRIES
    );
    apply_if_default!(in_memory, config.storage.in_memory, DEFAULT_IN_MEMORY);

    // WAL section
    apply_if_default!(wal_enabled, config.wal.enabled, DEFAULT_WAL_ENABLED);
    apply_if_default!(wal_sync_ms, config.wal.sync_ms, DEFAULT_WAL_SYNC_MS);
    apply_if_default_string!(wal_sync_mode, config.wal.sync_mode, DEFAULT_WAL_SYNC_MODE);
    apply_if_default!(
        wal_max_file_size,
        config.wal.max_file_size,
        DEFAULT_WAL_MAX_FILE_SIZE
    );
    apply_if_default!(
        wal_retention_files,
        config.wal.retention_files,
        DEFAULT_WAL_RETENTION_FILES
    );
    apply_if_default!(
        wal_max_pending_writes,
        config.wal.max_pending_writes,
        DEFAULT_WAL_MAX_PENDING_WRITES
    );

    // TLS section
    if let Some(enabled) = config.tls.enabled {
        if !args.tls_enabled {
            args.tls_enabled = enabled;
        }
    }
    apply_option!(tls_cert, config.tls.cert.clone());
    apply_option!(tls_key, config.tls.key.clone());
    apply_if_default_string!(
        tls_min_version,
        config.tls.min_version,
        DEFAULT_TLS_MIN_VERSION
    );
    if let Some(val) = config.tls.require_client_cert {
        if !args.tls_require_client_cert {
            args.tls_require_client_cert = val;
        }
    }
    apply_option!(tls_ca_cert, config.tls.ca_cert.clone());

    // Auth section
    if let Some(enabled) = config.auth.enabled {
        if !args.auth_enabled {
            args.auth_enabled = enabled;
        }
    }
    if let Some(ref mechs) = config.auth.sasl_mechanisms {
        if args.auth_sasl_mechanisms.is_none() {
            args.auth_sasl_mechanisms = Some(mechs.join(","));
        }
    }
    apply_option!(auth_users_file, config.auth.users_file.clone());
    if let Some(val) = config.auth.allow_anonymous {
        if !args.auth_allow_anonymous {
            args.auth_allow_anonymous = val;
        }
    }

    // OAuth section
    if let Some(enabled) = config.oauth.enabled {
        if !args.oauth_enabled {
            args.oauth_enabled = enabled;
        }
    }
    apply_option!(oauth_issuer_url, config.oauth.issuer_url.clone());
    apply_option!(oauth_jwks_url, config.oauth.jwks_url.clone());
    apply_option!(oauth_audience, config.oauth.audience.clone());
    apply_if_default_string!(oauth_principal_claim, config.oauth.principal_claim, "sub");
    if let Some(ref scopes) = config.oauth.required_scopes {
        if args.oauth_required_scopes.is_none() {
            args.oauth_required_scopes = Some(scopes.join(","));
        }
    }
    apply_if_default!(
        oauth_jwks_refresh_interval,
        config.oauth.jwks_refresh_interval,
        3600
    );

    // ACL section
    if let Some(enabled) = config.acl.enabled {
        if !args.acl_enabled {
            args.acl_enabled = enabled;
        }
    }
    apply_option!(acl_file, config.acl.file.clone());
    if let Some(ref users) = config.acl.super_users {
        if args.acl_super_users.is_none() {
            args.acl_super_users = Some(users.join(","));
        }
    }
    apply_if_default!(acl_allow_if_no_acls, config.acl.allow_if_no_acls, true);

    // Cluster section
    apply_option!(node_id, config.cluster.node_id);
    apply_option!(advertised_addr, config.cluster.advertised_addr.clone());
    apply_option!(inter_broker_addr, config.cluster.inter_broker_addr.clone());
    if let Some(ref nodes) = config.cluster.seed_nodes {
        if args.seed_nodes.is_none() {
            args.seed_nodes = Some(nodes.join(","));
        }
    }
    apply_if_default!(
        default_replication_factor,
        config.cluster.default_replication_factor,
        1
    );
    apply_if_default!(min_insync_replicas, config.cluster.min_insync_replicas, 1);
    apply_option!(rack_id, config.cluster.rack_id.clone());
    if let Some(val) = config.cluster.disable_rack_aware_assignment {
        if !args.disable_rack_aware_assignment {
            args.disable_rack_aware_assignment = val;
        }
    }

    // Inter-broker TLS section
    if let Some(enabled) = config.inter_broker_tls.enabled {
        if !args.inter_broker_tls_enabled {
            args.inter_broker_tls_enabled = enabled;
        }
    }
    apply_option!(inter_broker_tls_cert, config.inter_broker_tls.cert.clone());
    apply_option!(inter_broker_tls_key, config.inter_broker_tls.key.clone());
    apply_option!(inter_broker_tls_ca, config.inter_broker_tls.ca.clone());
    apply_if_default!(
        inter_broker_tls_verify_peer,
        config.inter_broker_tls.verify_peer,
        true
    );

    // Audit section
    if let Some(enabled) = config.audit.enabled {
        if !args.audit_enabled {
            args.audit_enabled = enabled;
        }
    }
    if let Some(ref path) = config.audit.log_path {
        if args.audit_log_path == std::path::Path::new(DEFAULT_AUDIT_LOG_PATH) {
            args.audit_log_path = path.clone();
        }
    }
    apply_if_default_string!(
        audit_log_format,
        config.audit.log_format,
        DEFAULT_AUDIT_LOG_FORMAT
    );
    if let Some(ref events) = config.audit.events {
        if args.audit_events.is_none() {
            args.audit_events = Some(events.join(","));
        }
    }
    if let Some(val) = config.audit.log_to_stdout {
        if !args.audit_log_to_stdout {
            args.audit_log_to_stdout = val;
        }
    }

    // Limits section
    apply_if_default!(
        max_connections,
        config.limits.max_connections,
        DEFAULT_MAX_CONNECTIONS
    );
    apply_if_default!(
        max_connections_per_ip,
        config.limits.max_connections_per_ip,
        DEFAULT_MAX_CONNECTIONS_PER_IP
    );
    apply_if_default!(
        max_request_size,
        config.limits.max_request_size,
        DEFAULT_MAX_REQUEST_SIZE
    );
    apply_if_default!(
        connection_idle_timeout,
        config.limits.connection_idle_timeout,
        DEFAULT_CONNECTION_IDLE_TIMEOUT_SECS
    );
    apply_if_default!(
        produce_rate_limit,
        config.limits.produce_rate_limit,
        DEFAULT_PRODUCE_RATE_LIMIT_BYTES
    );

    // Quotas section
    if let Some(enabled) = config.quotas.enabled {
        if !args.quotas_enabled {
            args.quotas_enabled = enabled;
        }
    }
    apply_if_default!(
        quota_producer_byte_rate,
        config.quotas.producer_byte_rate,
        0
    );
    apply_if_default!(
        quota_consumer_byte_rate,
        config.quotas.consumer_byte_rate,
        0
    );

    // Simple protocol section
    if let Some(enabled) = config.simple.enabled {
        if !args.simple_enabled {
            args.simple_enabled = enabled;
        }
    }
    apply_if_default_string!(simple_addr, config.simple.addr, "0.0.0.0:9095");

    // Encryption section
    if let Some(enabled) = config.encryption.enabled {
        if !args.encryption_enabled {
            args.encryption_enabled = enabled;
        }
    }
    apply_option!(encryption_key_file, config.encryption.key_file.clone());
    apply_option!(
        encryption_key_env_var,
        config.encryption.key_env_var.clone()
    );

    // Zero-copy section
    apply_if_default!(zerocopy_enabled, config.zerocopy.enabled, true);
    apply_if_default!(zerocopy_mmap, config.zerocopy.mmap, true);
    apply_if_default!(
        zerocopy_mmap_threshold,
        config.zerocopy.mmap_threshold,
        1048576
    );
    apply_if_default!(
        zerocopy_buffer_pool_size,
        config.zerocopy.buffer_pool_size,
        16
    );
    apply_if_default!(zerocopy_sendfile, config.zerocopy.sendfile, true);
    apply_if_default!(
        zerocopy_response_cache,
        config.zerocopy.response_cache,
        true
    );
    apply_if_default!(zerocopy_cache_size_mb, config.zerocopy.cache_size_mb, 128);

    // OpenTelemetry section
    if let Some(enabled) = config.otel.enabled {
        if !args.otel_enabled {
            args.otel_enabled = enabled;
        }
    }
    apply_option!(otel_endpoint, config.otel.endpoint.clone());
    apply_if_default_string!(otel_service_name, config.otel.service_name, "streamline");
    apply_if_default!(otel_sampling_ratio, config.otel.sampling_ratio, 1.0);
    apply_if_default!(otel_timeout_secs, config.otel.timeout_secs, 10);

    args
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    /// Create default ServerArgs for testing
    fn default_args() -> ServerArgs {
        ServerArgs {
            config: None,
            generate_config: false,
            listen_addr: DEFAULT_KAFKA_ADDR.to_string(),
            http_addr: DEFAULT_HTTP_ADDR.to_string(),
            data_dir: PathBuf::from(DEFAULT_DATA_DIR),
            log_level: DEFAULT_LOG_LEVEL.to_string(),
            wal_enabled: DEFAULT_WAL_ENABLED,
            wal_sync_ms: DEFAULT_WAL_SYNC_MS,
            wal_sync_mode: DEFAULT_WAL_SYNC_MODE.to_string(),
            wal_max_file_size: DEFAULT_WAL_MAX_FILE_SIZE,
            wal_retention_files: DEFAULT_WAL_RETENTION_FILES,
            wal_max_pending_writes: DEFAULT_WAL_MAX_PENDING_WRITES,
            segment_max_bytes: DEFAULT_SEGMENT_MAX_BYTES,
            max_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
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
            simple_enabled: false,
            simple_addr: "0.0.0.0:9095".to_string(),
            encryption_enabled: false,
            encryption_key_file: None,
            encryption_key_env_var: None,
            in_memory: DEFAULT_IN_MEMORY,
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
            auto_create_topics: DEFAULT_AUTO_CREATE_TOPICS,
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT_SECS,
            drain_timeout: crate::server::shutdown::DEFAULT_DRAIN_TIMEOUT_SECS,
            playground: false,
            rack_id: None,
            disable_rack_aware_assignment: false,
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
        }
    }

    /// Create an empty ConfigFile for testing
    fn empty_config() -> ConfigFile {
        ConfigFile::default()
    }

    #[test]
    fn test_merge_with_empty_config() {
        let args = default_args();
        let config = empty_config();

        let merged = merge_config_with_args(args.clone(), &config);

        // With empty config, args should remain unchanged
        assert_eq!(merged.listen_addr, args.listen_addr);
        assert_eq!(merged.http_addr, args.http_addr);
        assert_eq!(merged.data_dir, args.data_dir);
        assert_eq!(merged.log_level, args.log_level);
    }

    #[test]
    fn test_merge_server_section() {
        let args = default_args();
        let mut config = empty_config();

        // Set config file values
        config.server.listen_addr = Some("127.0.0.1:9093".to_string());
        config.server.http_addr = Some("127.0.0.1:8080".to_string());
        config.server.data_dir = Some(PathBuf::from("/custom/data"));
        config.server.log_level = Some("debug".to_string());

        let merged = merge_config_with_args(args, &config);

        // Config file values should be applied
        assert_eq!(merged.listen_addr, "127.0.0.1:9093");
        assert_eq!(merged.http_addr, "127.0.0.1:8080");
        assert_eq!(merged.data_dir, PathBuf::from("/custom/data"));
        assert_eq!(merged.log_level, "debug");
    }

    #[test]
    fn test_cli_takes_precedence_over_config() {
        let mut args = default_args();
        // CLI sets non-default value
        args.listen_addr = "192.168.1.1:9092".to_string();
        args.log_level = "warn".to_string();

        let mut config = empty_config();
        // Config file tries to set different values
        config.server.listen_addr = Some("127.0.0.1:9093".to_string());
        config.server.log_level = Some("debug".to_string());

        let merged = merge_config_with_args(args, &config);

        // CLI values should take precedence
        assert_eq!(merged.listen_addr, "192.168.1.1:9092");
        assert_eq!(merged.log_level, "warn");
    }

    #[test]
    fn test_merge_storage_section() {
        let args = default_args();
        let mut config = empty_config();

        config.storage.segment_max_bytes = Some(500 * 1024 * 1024);
        config.storage.max_message_bytes = Some(2 * 1024 * 1024);
        config.storage.in_memory = Some(true);

        let merged = merge_config_with_args(args, &config);

        assert_eq!(merged.segment_max_bytes, 500 * 1024 * 1024);
        assert_eq!(merged.max_message_bytes, 2 * 1024 * 1024);
        assert!(merged.in_memory);
    }

    #[test]
    fn test_merge_wal_section() {
        let args = default_args();
        let mut config = empty_config();

        config.wal.enabled = Some(false);
        config.wal.sync_ms = Some(50);
        config.wal.sync_mode = Some("every_write".to_string());
        config.wal.max_file_size = Some(50 * 1024 * 1024);

        let merged = merge_config_with_args(args, &config);

        assert!(!merged.wal_enabled);
        assert_eq!(merged.wal_sync_ms, 50);
        assert_eq!(merged.wal_sync_mode, "every_write");
        assert_eq!(merged.wal_max_file_size, 50 * 1024 * 1024);
    }

    #[test]
    fn test_merge_tls_section() {
        let args = default_args();
        let mut config = empty_config();

        config.tls.enabled = Some(true);
        config.tls.cert = Some(PathBuf::from("/path/to/cert.pem"));
        config.tls.key = Some(PathBuf::from("/path/to/key.pem"));
        config.tls.min_version = Some("1.3".to_string());
        config.tls.require_client_cert = Some(true);

        let merged = merge_config_with_args(args, &config);

        assert!(merged.tls_enabled);
        assert_eq!(merged.tls_cert, Some(PathBuf::from("/path/to/cert.pem")));
        assert_eq!(merged.tls_key, Some(PathBuf::from("/path/to/key.pem")));
        assert_eq!(merged.tls_min_version, "1.3");
        assert!(merged.tls_require_client_cert);
    }

    #[test]
    fn test_merge_auth_section() {
        let args = default_args();
        let mut config = empty_config();

        config.auth.enabled = Some(true);
        config.auth.sasl_mechanisms = Some(vec!["PLAIN".to_string(), "SCRAM-SHA-256".to_string()]);
        config.auth.users_file = Some(PathBuf::from("/etc/users.yaml"));
        config.auth.allow_anonymous = Some(true);

        let merged = merge_config_with_args(args, &config);

        assert!(merged.auth_enabled);
        assert_eq!(
            merged.auth_sasl_mechanisms,
            Some("PLAIN,SCRAM-SHA-256".to_string())
        );
        assert_eq!(
            merged.auth_users_file,
            Some(PathBuf::from("/etc/users.yaml"))
        );
        assert!(merged.auth_allow_anonymous);
    }

    #[test]
    fn test_merge_oauth_section() {
        let args = default_args();
        let mut config = empty_config();

        config.oauth.enabled = Some(true);
        config.oauth.issuer_url = Some("https://auth.example.com".to_string());
        config.oauth.jwks_url = Some("https://auth.example.com/.well-known/jwks.json".to_string());
        config.oauth.audience = Some("my-api".to_string());
        config.oauth.principal_claim = Some("email".to_string());
        config.oauth.required_scopes = Some(vec!["read".to_string(), "write".to_string()]);

        let merged = merge_config_with_args(args, &config);

        assert!(merged.oauth_enabled);
        assert_eq!(
            merged.oauth_issuer_url,
            Some("https://auth.example.com".to_string())
        );
        assert_eq!(merged.oauth_principal_claim, "email");
        assert_eq!(merged.oauth_required_scopes, Some("read,write".to_string()));
    }

    #[test]
    fn test_merge_acl_section() {
        let args = default_args();
        let mut config = empty_config();

        config.acl.enabled = Some(true);
        config.acl.file = Some(PathBuf::from("/etc/acls.yaml"));
        config.acl.super_users = Some(vec!["admin".to_string(), "root".to_string()]);
        config.acl.allow_if_no_acls = Some(false);

        let merged = merge_config_with_args(args, &config);

        assert!(merged.acl_enabled);
        assert_eq!(merged.acl_file, Some(PathBuf::from("/etc/acls.yaml")));
        assert_eq!(merged.acl_super_users, Some("admin,root".to_string()));
        assert!(!merged.acl_allow_if_no_acls);
    }

    #[test]
    fn test_merge_cluster_section() {
        let args = default_args();
        let mut config = empty_config();

        config.cluster.node_id = Some(1);
        config.cluster.advertised_addr = Some("node1.cluster.local:9092".to_string());
        config.cluster.inter_broker_addr = Some("node1.cluster.local:9093".to_string());
        config.cluster.seed_nodes = Some(vec!["node2:9093".to_string(), "node3:9093".to_string()]);
        config.cluster.default_replication_factor = Some(3);
        config.cluster.min_insync_replicas = Some(2);

        let merged = merge_config_with_args(args, &config);

        assert_eq!(merged.node_id, Some(1));
        assert_eq!(
            merged.advertised_addr,
            Some("node1.cluster.local:9092".to_string())
        );
        assert_eq!(merged.seed_nodes, Some("node2:9093,node3:9093".to_string()));
        assert_eq!(merged.default_replication_factor, 3);
        assert_eq!(merged.min_insync_replicas, 2);
    }

    #[test]
    fn test_merge_inter_broker_tls_section() {
        let args = default_args();
        let mut config = empty_config();

        config.inter_broker_tls.enabled = Some(true);
        config.inter_broker_tls.cert = Some(PathBuf::from("/path/to/broker-cert.pem"));
        config.inter_broker_tls.key = Some(PathBuf::from("/path/to/broker-key.pem"));
        config.inter_broker_tls.ca = Some(PathBuf::from("/path/to/ca.pem"));
        config.inter_broker_tls.verify_peer = Some(false);

        let merged = merge_config_with_args(args, &config);

        assert!(merged.inter_broker_tls_enabled);
        assert_eq!(
            merged.inter_broker_tls_cert,
            Some(PathBuf::from("/path/to/broker-cert.pem"))
        );
        assert!(!merged.inter_broker_tls_verify_peer);
    }

    #[test]
    fn test_merge_audit_section() {
        let args = default_args();
        let mut config = empty_config();

        config.audit.enabled = Some(true);
        config.audit.log_path = Some(PathBuf::from("/var/log/streamline/audit.log"));
        config.audit.log_format = Some("json".to_string());
        config.audit.events = Some(vec!["connect".to_string(), "produce".to_string()]);
        config.audit.log_to_stdout = Some(true);

        let merged = merge_config_with_args(args, &config);

        assert!(merged.audit_enabled);
        assert_eq!(
            merged.audit_log_path,
            PathBuf::from("/var/log/streamline/audit.log")
        );
        assert_eq!(merged.audit_log_format, "json");
        assert_eq!(merged.audit_events, Some("connect,produce".to_string()));
        assert!(merged.audit_log_to_stdout);
    }

    #[test]
    fn test_merge_limits_section() {
        let args = default_args();
        let mut config = empty_config();

        config.limits.max_connections = Some(5000);
        config.limits.max_connections_per_ip = Some(50);
        config.limits.max_request_size = Some(100 * 1024 * 1024);
        config.limits.connection_idle_timeout = Some(600);
        config.limits.produce_rate_limit = Some(100 * 1024 * 1024);

        let merged = merge_config_with_args(args, &config);

        assert_eq!(merged.max_connections, 5000);
        assert_eq!(merged.max_connections_per_ip, 50);
        assert_eq!(merged.max_request_size, 100 * 1024 * 1024);
        assert_eq!(merged.connection_idle_timeout, 600);
        assert_eq!(merged.produce_rate_limit, 100 * 1024 * 1024);
    }

    #[test]
    fn test_merge_quotas_section() {
        let args = default_args();
        let mut config = empty_config();

        config.quotas.enabled = Some(true);
        config.quotas.producer_byte_rate = Some(10 * 1024 * 1024);
        config.quotas.consumer_byte_rate = Some(20 * 1024 * 1024);

        let merged = merge_config_with_args(args, &config);

        assert!(merged.quotas_enabled);
        assert_eq!(merged.quota_producer_byte_rate, 10 * 1024 * 1024);
        assert_eq!(merged.quota_consumer_byte_rate, 20 * 1024 * 1024);
    }

    #[test]
    fn test_merge_simple_protocol_section() {
        let args = default_args();
        let mut config = empty_config();

        config.simple.enabled = Some(true);
        config.simple.addr = Some("0.0.0.0:9096".to_string());

        let merged = merge_config_with_args(args, &config);

        assert!(merged.simple_enabled);
        assert_eq!(merged.simple_addr, "0.0.0.0:9096");
    }

    #[test]
    fn test_merge_encryption_section() {
        let args = default_args();
        let mut config = empty_config();

        config.encryption.enabled = Some(true);
        config.encryption.key_file = Some(PathBuf::from("/etc/encryption.key"));
        config.encryption.key_env_var = Some("STREAMLINE_KEY".to_string());

        let merged = merge_config_with_args(args, &config);

        assert!(merged.encryption_enabled);
        assert_eq!(
            merged.encryption_key_file,
            Some(PathBuf::from("/etc/encryption.key"))
        );
        assert_eq!(
            merged.encryption_key_env_var,
            Some("STREAMLINE_KEY".to_string())
        );
    }

    #[test]
    fn test_merge_zerocopy_section() {
        let args = default_args();
        let mut config = empty_config();

        config.zerocopy.enabled = Some(false);
        config.zerocopy.mmap = Some(false);
        config.zerocopy.mmap_threshold = Some(2 * 1024 * 1024);
        config.zerocopy.buffer_pool_size = Some(32);
        config.zerocopy.cache_size_mb = Some(256);

        let merged = merge_config_with_args(args, &config);

        assert!(!merged.zerocopy_enabled);
        assert!(!merged.zerocopy_mmap);
        assert_eq!(merged.zerocopy_mmap_threshold, 2 * 1024 * 1024);
        assert_eq!(merged.zerocopy_buffer_pool_size, 32);
        assert_eq!(merged.zerocopy_cache_size_mb, 256);
    }

    #[test]
    fn test_merge_otel_section() {
        let args = default_args();
        let mut config = empty_config();

        config.otel.enabled = Some(true);
        config.otel.endpoint = Some("http://otel-collector:4317".to_string());
        config.otel.service_name = Some("my-streamline".to_string());
        config.otel.sampling_ratio = Some(0.5);
        config.otel.timeout_secs = Some(30);

        let merged = merge_config_with_args(args, &config);

        assert!(merged.otel_enabled);
        assert_eq!(
            merged.otel_endpoint,
            Some("http://otel-collector:4317".to_string())
        );
        assert_eq!(merged.otel_service_name, "my-streamline");
        assert!((merged.otel_sampling_ratio - 0.5).abs() < f64::EPSILON);
        assert_eq!(merged.otel_timeout_secs, 30);
    }

    #[test]
    fn test_partial_config_merge() {
        let args = default_args();
        let mut config = empty_config();

        // Only set a few values
        config.server.log_level = Some("debug".to_string());
        config.storage.in_memory = Some(true);
        config.auth.enabled = Some(true);

        let merged = merge_config_with_args(args, &config);

        // Only specified values should change
        assert_eq!(merged.log_level, "debug");
        assert!(merged.in_memory);
        assert!(merged.auth_enabled);

        // Other values should remain at defaults
        assert_eq!(merged.listen_addr, DEFAULT_KAFKA_ADDR);
        assert_eq!(merged.segment_max_bytes, DEFAULT_SEGMENT_MAX_BYTES);
        assert!(!merged.tls_enabled);
    }

    #[test]
    fn test_cli_option_set_config_also_set() {
        let mut args = default_args();
        // CLI explicitly sets TLS cert (non-default)
        args.tls_cert = Some(PathBuf::from("/cli/cert.pem"));

        let mut config = empty_config();
        // Config also has TLS cert
        config.tls.cert = Some(PathBuf::from("/config/cert.pem"));

        let merged = merge_config_with_args(args, &config);

        // CLI value should win for Option fields
        assert_eq!(merged.tls_cert, Some(PathBuf::from("/cli/cert.pem")));
    }

    #[test]
    fn test_boolean_fields_cli_precedence() {
        let mut args = default_args();
        // CLI sets auth_enabled to true
        args.auth_enabled = true;

        let mut config = empty_config();
        // Config tries to set it to false (but this won't work since CLI already set it)
        // Note: the merge logic only applies config value if CLI has default (false for booleans)
        config.auth.enabled = Some(false);

        let merged = merge_config_with_args(args, &config);

        // CLI value should remain
        assert!(merged.auth_enabled);
    }

    #[test]
    fn test_full_production_config() {
        let args = default_args();
        let mut config = empty_config();

        // Simulate a full production config
        config.server.listen_addr = Some("0.0.0.0:9092".to_string());
        config.server.log_level = Some("info".to_string());
        config.storage.segment_max_bytes = Some(1024 * 1024 * 1024);
        config.wal.enabled = Some(true);
        config.wal.sync_mode = Some("interval".to_string());
        config.tls.enabled = Some(true);
        config.tls.cert = Some(PathBuf::from("/etc/ssl/cert.pem"));
        config.tls.key = Some(PathBuf::from("/etc/ssl/key.pem"));
        config.auth.enabled = Some(true);
        config.auth.sasl_mechanisms = Some(vec!["SCRAM-SHA-256".to_string()]);
        config.acl.enabled = Some(true);
        config.cluster.node_id = Some(1);
        config.cluster.seed_nodes = Some(vec!["node2:9093".to_string()]);
        config.limits.max_connections = Some(10000);
        config.otel.enabled = Some(true);
        config.otel.endpoint = Some("http://otel:4317".to_string());

        let merged = merge_config_with_args(args, &config);

        // Verify all values were applied
        assert!(merged.tls_enabled);
        assert!(merged.auth_enabled);
        assert!(merged.acl_enabled);
        assert_eq!(merged.node_id, Some(1));
        assert_eq!(merged.max_connections, 10000);
        assert!(merged.otel_enabled);
    }
}
