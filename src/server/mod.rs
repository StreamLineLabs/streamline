//! Server implementation for Streamline
//!
//! The server handles incoming connections and routes them to
//! appropriate protocol handlers.

#[cfg(feature = "ai")]
pub mod ai_api;
pub mod alerts;
pub mod alerts_api;
pub mod analytics_api;
pub mod api;
pub mod benchmark_api;
pub mod browser_client;
pub mod cdc_api;
pub mod cloud_api;
pub mod cluster_api;
pub mod connections_api;
pub mod connector_mgmt_api;
pub mod console_api;
pub mod consumer_api;
pub mod dashboard_api;
#[cfg(feature = "clustering")]
pub mod failover_api;
#[cfg(feature = "clustering")]
pub mod raft_cluster_api;
#[cfg(feature = "featurestore")]
pub mod featurestore_api;
pub mod gitops_api;
pub mod governor_api;
pub mod http;
pub mod limits;
pub mod log_buffer;
pub mod log_layer;
pub mod logs_api;
pub mod memory;
pub mod metadata_cache;
pub mod observability_api;
pub mod playground_api;
pub mod plugin_api;
pub mod replication_api;
#[cfg(feature = "schema-registry")]
pub mod schema_api;
#[cfg(feature = "schema-registry")]
pub mod schema_ui;
#[cfg(feature = "schema-registry")]
pub mod schema_ui_templates;
pub mod shutdown;
pub mod sqlite_routes;
mod tls;
pub mod wasm_api;
pub mod websocket;
pub mod websocket_streaming;

#[cfg(feature = "graphql")]
pub mod graphql_routes;

#[cfg(feature = "edge")]
pub mod edge_api;

// Re-export QuotaManager and ResourceLimiter for client quotas and limits
pub use limits::{QuotaConfig, QuotaManager, ResourceLimiter};

// Feature-gated imports
#[cfg(feature = "auth")]
use crate::audit::AuditLogger;
#[cfg(feature = "clustering")]
use crate::cluster::ClusterManager;
#[cfg(feature = "metrics")]
use metrics_exporter_prometheus::PrometheusHandle;

// No-op AuditLogger stub for non-auth builds
#[cfg(not(feature = "auth"))]
mod audit_stub {
    use std::net::SocketAddr;

    /// Stub AuditLogger that does nothing when auth feature is disabled
    #[derive(Debug, Clone, Default)]
    pub struct AuditLogger;

    impl AuditLogger {
        pub fn new(_config: AuditLoggerConfig) -> Self {
            Self
        }

        pub fn log_connect(&self, _addr: &SocketAddr, _tls: bool) {}
        pub fn log_disconnect(&self, _addr: &SocketAddr, _tls: bool) {}
    }

    #[derive(Debug, Clone, Default)]
    pub struct AuditLoggerConfig;

    impl AuditLoggerConfig {
        pub fn new() -> Self {
            Self
        }
    }
}

#[cfg(not(feature = "auth"))]
use audit_stub::AuditLogger;

// Metrics module is always available (provides no-ops when feature disabled)
use crate::metrics;

// Core imports (always available)
use crate::config::ServerConfig;
use crate::consumer::GroupCoordinator;
use crate::error::Result;
use crate::protocol::{KafkaHandler, ResponseCache};
use crate::runtime::ShardedRuntime;
use crate::server::shutdown::ShutdownCoordinator;
use crate::storage::segment::SegmentConfig;
use crate::storage::{ProducerStateManager, TopicManager};
use crate::transaction::TransactionCoordinator;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};
use tokio::signal;
use tokio::time::interval;
use tokio_rustls::TlsAcceptor;
use tracing::{debug, error, info, warn};

/// Streamline server
pub struct Server {
    /// Server configuration
    config: ServerConfig,

    /// Topic manager for storage
    topic_manager: Arc<TopicManager>,

    /// Group coordinator for consumer groups
    group_coordinator: Arc<GroupCoordinator>,

    /// Kafka protocol handler
    kafka_handler: Arc<KafkaHandler>,

    /// TLS acceptor (optional)
    tls_acceptor: Option<TlsAcceptor>,

    /// Prometheus metrics handle (requires metrics feature)
    #[cfg(feature = "metrics")]
    metrics_handle: PrometheusHandle,

    /// Server start time
    start_time: Instant,

    /// Cluster manager (optional, only in cluster mode, requires clustering feature)
    #[cfg(feature = "clustering")]
    cluster_manager: Option<Arc<ClusterManager>>,

    /// Audit logger for connection and security events
    /// (real implementation with auth feature, no-op stub without)
    audit_logger: Arc<AuditLogger>,

    /// Shutdown coordinator for graceful shutdown with timeout
    shutdown_coordinator: Arc<ShutdownCoordinator>,

    /// Log buffer for real-time log viewing (shared with tracing layer)
    log_buffer: Option<Arc<log_buffer::LogBuffer>>,

    /// Transaction coordinator for exactly-once semantics
    transaction_coordinator: Option<Arc<TransactionCoordinator>>,

    /// Sharded runtime for thread-per-core execution (optional)
    /// When enabled, provides CPU affinity and partition routing
    sharded_runtime: Option<Arc<ShardedRuntime>>,
}

impl Server {
    /// Create a new server
    pub async fn new(
        config: ServerConfig,
        sharded_runtime: Option<Arc<ShardedRuntime>>,
    ) -> Result<Self> {
        // Initialize metrics (only with metrics feature)
        #[cfg(feature = "metrics")]
        let metrics_handle = metrics::init_metrics();

        // Initialize topic manager based on storage mode
        let topic_manager = if config.storage.in_memory {
            info!("Starting in in-memory mode - no data will be persisted");
            Arc::new(TopicManager::in_memory()?)
        } else {
            // Create data directory for disk-based storage
            std::fs::create_dir_all(&config.data_dir)?;

            // Create segment config from io_backend settings (for DirectIO support)
            let segment_config = SegmentConfig::from_io_backend_config(&config.storage.io_backend);
            let segment_config_opt = if config.storage.io_backend.enable_direct_io {
                info!(
                    direct_io = true,
                    alignment = config.storage.io_backend.direct_io_alignment,
                    fallback = config.storage.io_backend.direct_io_fallback,
                    "Direct I/O enabled for storage"
                );
                Some(segment_config)
            } else {
                None
            };

            Arc::new(TopicManager::with_segment_config(
                &config.data_dir,
                Some(config.storage.wal.clone()),
                segment_config_opt,
            )?)
        };

        // Initialize group coordinator
        // Note: GroupCoordinator still uses storage path for offsets persistence
        // In in-memory mode, it will create files under data_dir (which may be temp)
        let consumer_offsets_path = config.data_dir.join("consumer_offsets");
        let group_coordinator = Arc::new(GroupCoordinator::new(
            consumer_offsets_path,
            topic_manager.clone(),
        )?);

        // Initialize transaction coordinator for exactly-once semantics
        let transaction_coordinator = if !config.storage.in_memory {
            let producer_state_manager = Arc::new(ProducerStateManager::new(&config.data_dir)?);
            let txn_coordinator = Arc::new(TransactionCoordinator::new(
                &config.data_dir,
                producer_state_manager,
                topic_manager.clone(),
                Some(group_coordinator.clone()),
            )?);
            info!("Transaction coordinator initialized");
            Some(txn_coordinator)
        } else {
            // Transaction coordinator requires persistent storage for durability guarantees
            info!("Transaction coordinator disabled in in-memory mode");
            None
        };

        // Initialize Kafka handler
        #[cfg(feature = "auth")]
        let kafka_handler = if config.auth.enabled || config.acl.enabled {
            // Load user store if auth is enabled
            let user_store = if let Some(ref users_file) = config.auth.users_file {
                info!(file = %users_file.display(), "Loading user store from file");
                Some(crate::auth::UserStore::from_file(users_file)?)
            } else {
                if config.auth.enabled {
                    info!("Authentication enabled but no users file provided");
                }
                None
            };

            if config.acl.enabled {
                info!(
                    acl_file = ?config.acl.acl_file,
                    super_users = ?config.acl.super_users,
                    "Authorization (ACL) enabled"
                );
            }

            KafkaHandler::new_with_auth_and_acl(
                topic_manager.clone(),
                group_coordinator.clone(),
                config.auth.clone(),
                user_store,
                config.acl.clone(),
            )?
        } else {
            KafkaHandler::new(topic_manager.clone(), group_coordinator.clone())?
        }
        .with_auto_create_topics(config.auto_create_topics)
        .with_zerocopy_config(config.storage.zerocopy.clone());

        #[cfg(not(feature = "auth"))]
        let kafka_handler = KafkaHandler::new(topic_manager.clone(), group_coordinator.clone())?
            .with_auto_create_topics(config.auto_create_topics)
            .with_zerocopy_config(config.storage.zerocopy.clone());

        // Initialize cluster manager BEFORE kafka_handler finalization (requires clustering feature)
        // This is required to wire the replication manager to the protocol handler
        #[cfg(feature = "clustering")]
        let cluster_manager = if let Some(ref cluster_config) = config.cluster {
            info!(
                node_id = cluster_config.node_id,
                "Initializing cluster mode"
            );

            let cluster_data_dir = config.data_dir.join("raft");
            let manager = ClusterManager::new(cluster_config.clone(), cluster_data_dir).await?;
            let manager = Arc::new(manager);

            // Wire up the topic manager for inter-broker replication
            // This enables the RPC handler to serve ReplicaFetch requests from followers
            manager.set_topic_manager(topic_manager.clone()).await;
            info!("TopicManager wired to cluster manager for replica fetching");

            Some(manager)
        } else {
            None
        };

        // Wire up quota manager if quotas are enabled
        let kafka_handler = if config.quotas.enabled {
            info!(
                producer_byte_rate = config.quotas.producer_byte_rate,
                consumer_byte_rate = config.quotas.consumer_byte_rate,
                "Client quotas enabled"
            );
            let quota_manager = Arc::new(QuotaManager::new(config.quotas.clone()));
            kafka_handler.with_quota_manager(quota_manager)
        } else {
            kafka_handler
        };

        // Wire up resource limiter for request size validation
        let kafka_handler = if config.limits.max_request_size > 0 {
            info!(
                max_request_size = config.limits.max_request_size,
                max_connections = config.limits.max_connections,
                "Resource limits enabled"
            );
            let resource_limiter = Arc::new(ResourceLimiter::new(config.limits.clone()));
            kafka_handler.with_resource_limiter(resource_limiter)
        } else {
            kafka_handler
        };

        // Wire up cluster manager and replication manager if cluster mode is enabled (requires clustering feature)
        #[cfg(feature = "clustering")]
        let kafka_handler = if let Some(ref cluster_mgr) = cluster_manager {
            info!("Wiring cluster manager and replication manager to Kafka handler");
            let replication_mgr = cluster_mgr.replication();
            kafka_handler
                .with_cluster_manager(cluster_mgr.clone())
                .with_replication_manager(replication_mgr)
        } else {
            kafka_handler
        };

        // Wire up transaction coordinator if enabled
        let kafka_handler = if let Some(ref txn_coordinator) = transaction_coordinator {
            kafka_handler.with_transaction_coordinator(txn_coordinator.clone())
        } else {
            kafka_handler
        };

        // Wire up response cache if enabled in zerocopy config
        let kafka_handler = if config.storage.zerocopy.enable_response_cache {
            let cache_dir = config.data_dir.join("response_cache");
            match ResponseCache::new(
                cache_dir,
                config.storage.zerocopy.response_cache_max_bytes,
                config.storage.zerocopy.response_cache_ttl_secs,
            ) {
                Ok(cache) => {
                    info!(
                        max_bytes = config.storage.zerocopy.response_cache_max_bytes,
                        ttl_secs = config.storage.zerocopy.response_cache_ttl_secs,
                        "Response cache enabled for sendfile optimization"
                    );
                    kafka_handler.with_response_cache(Arc::new(cache))
                }
                Err(e) => {
                    warn!(error = %e, "Failed to create response cache, continuing without cache");
                    kafka_handler
                }
            }
        } else {
            kafka_handler
        };

        // Wire sharded runtime for thread-per-core partition routing
        let kafka_handler = if let Some(ref rt) = sharded_runtime {
            kafka_handler.with_sharded_runtime(rt.clone())
        } else {
            kafka_handler
        };

        // Wrap kafka_handler in Arc after all builder methods are applied
        let kafka_handler = Arc::new(kafka_handler);

        // Initialize TLS if enabled
        let tls_acceptor = if config.tls.enabled {
            info!("TLS is enabled, loading TLS configuration");
            let acceptor = if config.tls.require_client_cert {
                tls::load_mtls_config(&config.tls)?
            } else {
                tls::load_tls_config(&config.tls)?
            };
            Some(acceptor)
        } else {
            None
        };

        // Initialize audit logger
        // Use node_id 0 for single-node deployments, cluster node_id for cluster mode
        #[cfg(feature = "auth")]
        let audit_logger = {
            #[cfg(feature = "clustering")]
            let node_id = config.cluster.as_ref().map(|c| c.node_id).unwrap_or(0);
            #[cfg(not(feature = "clustering"))]
            let node_id = 0u64;

            let logger = Arc::new(AuditLogger::new(config.audit.clone(), node_id)?);

            if config.audit.enabled {
                info!(
                    log_path = ?config.audit.log_path,
                    format = %config.audit.format,
                    "Audit logging enabled"
                );
            }
            logger
        };

        // No-op audit logger when auth feature is disabled
        #[cfg(not(feature = "auth"))]
        let audit_logger = Arc::new(AuditLogger::new(audit_stub::AuditLoggerConfig::new()));

        // Initialize shutdown coordinator with configured timeout
        let shutdown_coordinator =
            Arc::new(ShutdownCoordinator::with_config(config.shutdown.clone()));

        Ok(Self {
            config,
            topic_manager,
            group_coordinator,
            kafka_handler,
            tls_acceptor,
            #[cfg(feature = "metrics")]
            metrics_handle,
            start_time: Instant::now(),
            #[cfg(feature = "clustering")]
            cluster_manager,
            audit_logger,
            shutdown_coordinator,
            log_buffer: None,
            transaction_coordinator,
            sharded_runtime,
        })
    }

    /// Set the log buffer for real-time log viewing.
    /// Should be called before run() to enable tracing integration.
    pub fn set_log_buffer(&mut self, buffer: Arc<log_buffer::LogBuffer>) {
        self.log_buffer = Some(buffer);
    }

    /// Set the sharded runtime for thread-per-core execution.
    /// Should be called before run() to enable partition routing and CPU affinity.
    pub fn set_sharded_runtime(&mut self, runtime: Arc<ShardedRuntime>) {
        self.sharded_runtime = Some(runtime);
    }

    /// Get the sharded runtime if configured
    pub fn sharded_runtime(&self) -> Option<&Arc<ShardedRuntime>> {
        self.sharded_runtime.as_ref()
    }

    /// Calculate the shard ID for a client connection.
    /// Uses a hash of the client address to distribute connections evenly across shards.
    pub fn shard_for_connection(&self, addr: &std::net::SocketAddr) -> Option<usize> {
        self.sharded_runtime.as_ref().map(|rt| {
            use std::hash::{Hash, Hasher};
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            addr.hash(&mut hasher);
            (hasher.finish() as usize) % rt.shard_count()
        })
    }

    /// Get a reference to the topic manager
    pub fn topic_manager(&self) -> &Arc<TopicManager> {
        &self.topic_manager
    }

    /// Run the server
    pub async fn run(&self) -> Result<()> {
        info!(
            listen_addr = %self.config.listen_addr,
            data_dir = %self.config.data_dir.display(),
            tls_enabled = self.config.tls.enabled,
            wal_enabled = self.topic_manager.is_wal_enabled(),
            "Starting Streamline server"
        );

        // Log security configuration warnings for production awareness
        self.log_security_warnings();

        // Start HTTP server for metrics and health checks
        let http_state = http::HttpServerState {
            #[cfg(feature = "metrics")]
            metrics_handle: self.metrics_handle.clone(),
            topic_manager: self.topic_manager.clone(),
            group_coordinator: Some(self.group_coordinator.clone()),
            config: self.config.clone(),
            start_time: self.start_time,
            shutdown_coordinator: self.shutdown_coordinator.clone(),
            log_buffer: self.log_buffer.clone(),
            #[cfg(feature = "clustering")]
            cluster_manager: self.cluster_manager.clone(),
        };

        let http_addr = self.config.http_addr;
        let http_server = tokio::spawn(async move {
            if let Err(e) = http::start_http_server(http_addr, http_state).await {
                error!(error = %e, "HTTP server error");
            }
        });

        // Start simple protocol server if enabled
        let simple_server = if self.config.simple.enabled {
            let simple_state = crate::protocol::SimpleProtocolState {
                topic_manager: self.topic_manager.clone(),
            };
            let simple_addr = self.config.simple.addr;
            info!(addr = %simple_addr, "Starting simple protocol server");
            Some(tokio::spawn(async move {
                if let Err(e) =
                    crate::protocol::start_simple_server(simple_addr, simple_state).await
                {
                    error!(error = %e, "Simple protocol server error");
                }
            }))
        } else {
            None
        };

        // Start Kafka protocol listener
        let kafka_listener = TcpListener::bind(&self.config.listen_addr).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::AddrInUse {
                let port = self.config.listen_addr.port();
                crate::error::StreamlineError::Config(format!(
                    "Port {} is already in use. Another Streamline or Kafka instance may be running.\n  \
                     Fix: Use --listen-addr 0.0.0.0:{} to pick a different port, \
                     or stop the existing process.",
                    port, port + 100
                ))
            } else {
                crate::error::StreamlineError::Io(e)
            }
        })?;
        info!(
            addr = %self.config.listen_addr,
            tls = self.config.tls.enabled,
            "Kafka protocol listener started"
        );

        // Print startup banner
        self.print_banner();

        // Initialize cluster if in cluster mode
        #[cfg(feature = "clustering")]
        if let Some(ref cluster_manager) = self.cluster_manager {
            // Invariant: cluster_manager exists only when cluster config exists
            let cluster_config = self.config.cluster.as_ref().ok_or_else(|| {
                crate::error::StreamlineError::Config(
                    "cluster_manager is Some but cluster config is None".to_string(),
                )
            })?;

            // Start inter-broker RPC listener
            cluster_manager.start_rpc_listener().await?;

            // Bootstrap or join cluster
            if cluster_config.seed_nodes.is_empty() {
                // No seed nodes - bootstrap new cluster
                info!(
                    node_id = cluster_config.node_id,
                    "Bootstrapping new cluster (no seed nodes configured)"
                );
                cluster_manager.bootstrap().await?;
            } else {
                // Join existing cluster via seed nodes
                info!(
                    node_id = cluster_config.node_id,
                    seed_nodes = ?cluster_config.seed_nodes,
                    "Joining existing cluster"
                );
                cluster_manager
                    .join_cluster(&cluster_config.seed_nodes)
                    .await?;
            }

            // Wait for Raft to elect a leader (in bootstrap mode, we should become leader)
            if cluster_config.seed_nodes.is_empty() {
                info!("Waiting for Raft leader election...");
                tokio::time::sleep(Duration::from_secs(2)).await;

                if cluster_manager.is_leader().await {
                    info!(
                        node_id = cluster_config.node_id,
                        "This node is the Raft leader"
                    );
                } else {
                    warn!(
                        node_id = cluster_config.node_id,
                        "Leader election still in progress"
                    );
                }
            }
        }

        // Shutdown flag
        let shutdown = Arc::new(AtomicBool::new(false));

        // Start WAL sync task if WAL is enabled and sync mode is interval
        let sync_handle = if self.topic_manager.is_wal_enabled()
            && self.config.storage.wal.sync_mode == "interval"
        {
            let topic_manager = self.topic_manager.clone();
            let sync_interval = Duration::from_millis(self.config.storage.wal.sync_interval_ms);
            let shutdown_flag = shutdown.clone();

            Some(tokio::spawn(async move {
                let mut ticker = interval(sync_interval);
                loop {
                    ticker.tick().await;

                    if shutdown_flag.load(Ordering::Relaxed) {
                        break;
                    }

                    if let Err(e) = topic_manager.sync_wal() {
                        error!(
                            error = %e,
                            "WAL sync failed - data durability may be compromised. \
                            Check storage health and available disk space."
                        );
                    } else {
                        debug!("WAL synced");
                    }
                }
            }))
        } else {
            None
        };

        // Start uptime update task
        let start_time = self.start_time;
        let uptime_task = tokio::spawn(async move {
            loop {
                let uptime = start_time.elapsed().as_secs_f64();
                metrics::record_uptime(uptime);
                tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
            }
        });

        // Start metrics update task
        let topic_manager_metrics = self.topic_manager.clone();
        let metrics_task = tokio::spawn(async move {
            loop {
                if let Err(e) = topic_manager_metrics.update_all_metrics() {
                    tracing::warn!(error = %e, "Failed to update metrics");
                }
                tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
            }
        });

        // Start session timeout checker task
        let group_coordinator = self.group_coordinator.clone();
        let shutdown_flag = shutdown.clone();
        let _session_timeout_task = tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(5));
            loop {
                ticker.tick().await;

                if shutdown_flag.load(Ordering::Relaxed) {
                    break;
                }

                group_coordinator.check_session_timeouts().await;
            }
        });

        // Start transaction timeout checker task if transaction coordinator is enabled
        if let Some(ref txn_coordinator) = self.transaction_coordinator {
            txn_coordinator.start_timeout_checker();
            info!("Transaction timeout checker started");
        }

        // Start retention enforcement task
        // Note: This task is aborted during shutdown since it has a long interval (5 min)
        let topic_manager_retention = self.topic_manager.clone();
        let retention_task = tokio::spawn(async move {
            use crate::storage::RetentionEnforcer;

            let enforcer = RetentionEnforcer::new();
            let interval_duration = enforcer.interval();

            loop {
                tokio::time::sleep(interval_duration).await;

                if let Err(e) = enforcer.enforce(&topic_manager_retention).await {
                    error!(error = %e, "Retention enforcement failed");
                } else {
                    debug!("Retention enforcement completed");
                }
            }
        });

        // Main event loop
        loop {
            tokio::select! {
                // Accept Kafka protocol connections
                result = kafka_listener.accept() => {
                    match result {
                        Ok((stream, addr)) => {
                            const CONNECTION_INCREMENT: i64 = 1;
                            const CONNECTION_DECREMENT: i64 = -1;

                            // Configure TCP socket options (nodelay, keepalive, buffer sizes)
                            if let Err(e) = configure_tcp_socket(&stream, &self.config.tcp) {
                                warn!(peer = %addr, error = %e, "Failed to configure TCP socket options");
                            }

                            // Check if we're shutting down - don't accept new connections
                            if self.shutdown_coordinator.is_shutting_down() {
                                debug!(peer = %addr, "Rejecting connection - server is shutting down");
                                continue;
                            }

                            // Calculate shard for this connection (if sharded runtime is active)
                            let shard_id = self.shard_for_connection(&addr);
                            if let Some(shard) = shard_id {
                                info!(peer = %addr, shard = shard, "Client connected (sharded mode)");
                            } else {
                                info!(peer = %addr, "Client connected");
                            }
                            metrics::record_connection_active(CONNECTION_INCREMENT);
                            let handler = self.kafka_handler.clone();
                            let audit_logger = self.audit_logger.clone();
                            let shutdown_coord = self.shutdown_coordinator.clone();

                            // Track connection in shutdown coordinator
                            shutdown_coord.connection_opened();

                            if let Some(tls_acceptor) = &self.tls_acceptor {
                                // Handle TLS connection
                                let tls_acceptor = tls_acceptor.clone();
                                tokio::spawn(async move {
                                    let start = std::time::Instant::now();
                                    // Log connection attempt (TLS)
                                    audit_logger.log_connect(&addr, true);

                                    match tls_acceptor.accept(stream).await {
                                        Ok(tls_stream) => {
                                            debug!(peer = %addr, "TLS handshake completed");
                                            match handler.handle_tls_connection(tls_stream).await {
                                                Ok(()) => {
                                                    info!(
                                                        peer = %addr,
                                                        duration_secs = start.elapsed().as_secs(),
                                                        "Client disconnected"
                                                    );
                                                }
                                                Err(e) => {
                                                    warn!(
                                                        peer = %addr,
                                                        error = %e,
                                                        duration_secs = start.elapsed().as_secs(),
                                                        "Client disconnected with error"
                                                    );
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            warn!(peer = %addr, error = %e, "TLS handshake failed");
                                        }
                                    }
                                    // Log disconnection (TLS)
                                    audit_logger.log_disconnect(&addr, true);
                                    shutdown_coord.connection_closed();
                                    metrics::record_connection_active(CONNECTION_DECREMENT);
                                });
                            } else {
                                // Handle plain TCP connection
                                tokio::spawn(async move {
                                    let start = std::time::Instant::now();
                                    // Log connection attempt (plain TCP)
                                    audit_logger.log_connect(&addr, false);

                                    match handler.handle_connection(stream).await {
                                        Ok(()) => {
                                            info!(
                                                peer = %addr,
                                                duration_secs = start.elapsed().as_secs(),
                                                "Client disconnected"
                                            );
                                        }
                                        Err(e) => {
                                            warn!(
                                                peer = %addr,
                                                error = %e,
                                                duration_secs = start.elapsed().as_secs(),
                                                "Client disconnected with error"
                                            );
                                        }
                                    }
                                    // Log disconnection (plain TCP)
                                    audit_logger.log_disconnect(&addr, false);
                                    shutdown_coord.connection_closed();
                                    metrics::record_connection_active(CONNECTION_DECREMENT);
                                });
                            }
                        }
                        Err(e) => {
                            error!(error = %e, "Failed to accept connection");
                        }
                    }
                }

                // Handle shutdown signal
                _ = signal::ctrl_c() => {
                    info!("Shutdown signal received");
                    break;
                }
            }
        }

        // Signal shutdown to background tasks
        shutdown.store(true, Ordering::Relaxed);

        // Initiate graceful shutdown with timeout via the coordinator
        // This will stop accepting new connections and wait for in-flight requests
        info!(
            timeout_secs = self.config.shutdown.timeout_secs,
            active_connections = self.shutdown_coordinator.active_connections(),
            "Starting graceful shutdown with timeout"
        );

        match self.shutdown_coordinator.initiate_shutdown().await {
            Ok(()) => {
                info!("Graceful shutdown completed within timeout");
            }
            Err(e) => {
                warn!(
                    error = %e,
                    "Graceful shutdown timed out, proceeding with forced shutdown"
                );
            }
        }

        // Wait for background tasks to finish gracefully (with timeout)
        if let Some(handle) = sync_handle {
            // Give WAL sync task a short time to finish current operation
            match tokio::time::timeout(Duration::from_secs(2), handle).await {
                Ok(_) => debug!("WAL sync task completed"),
                Err(_) => debug!("WAL sync task timed out, aborting"),
            }
        }

        // Abort retention task - it's not critical and can take up to 5 minutes to wake
        retention_task.abort();

        // Cleanup remaining tasks
        http_server.abort();
        if let Some(simple) = simple_server {
            simple.abort();
        }
        uptime_task.abort();
        metrics_task.abort();

        self.shutdown().await
    }

    /// Graceful shutdown - flush state and cleanup resources
    async fn shutdown(&self) -> Result<()> {
        info!("Shutting down Streamline server...");

        // Shutdown cluster manager if in cluster mode
        #[cfg(feature = "clustering")]
        if let Some(ref cluster_manager) = self.cluster_manager {
            if let Err(e) = cluster_manager.shutdown().await {
                error!(error = ?e, "Failed to shutdown cluster manager");
            }
        }

        // Shutdown consumer group coordinator and save state
        if let Err(e) = self.group_coordinator.shutdown() {
            error!(error = %e, "Failed to save consumer groups during shutdown");
        }

        // Shutdown transaction coordinator and save state
        if let Some(ref txn_coordinator) = self.transaction_coordinator {
            txn_coordinator.shutdown();
            if let Err(e) = txn_coordinator.save_all() {
                warn!(error = %e, "Failed to save transaction state on shutdown");
            }
        }

        // Flush WAL and save checkpoint
        if let Err(e) = self.topic_manager.flush() {
            error!(error = %e, "Failed to flush WAL during shutdown");
        }

        info!("Shutdown complete");
        Ok(())
    }

    /// Print startup banner
    fn print_banner(&self) {
        let version = env!("CARGO_PKG_VERSION");
        let wal_status = if self.topic_manager.is_wal_enabled() {
            format!("enabled ({} mode)", self.config.storage.wal.sync_mode)
        } else {
            "disabled".to_string()
        };

        println!();
        println!("  ╔═══════════════════════════════════════════════════════╗");
        println!("  ║                                                       ║");
        println!("  ║   ███████╗████████╗██████╗ ███████╗ █████╗ ███╗   ███╗║");
        println!("  ║   ██╔════╝╚══██╔══╝██╔══██╗██╔════╝██╔══██╗████╗ ████║║");
        println!("  ║   ███████╗   ██║   ██████╔╝█████╗  ███████║██╔████╔██║║");
        println!("  ║   ╚════██║   ██║   ██╔══██╗██╔══╝  ██╔══██║██║╚██╔╝██║║");
        println!("  ║   ███████║   ██║   ██║  ██║███████╗██║  ██║██║ ╚═╝ ██║║");
        println!("  ║   ╚══════╝   ╚═╝   ╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝╚═╝     ╚═╝║");
        println!("  ║                                                       ║");
        println!(
            "  ║           The Redis of Streaming - v{}            ║",
            version
        );
        println!("  ║                                                       ║");
        println!("  ╚═══════════════════════════════════════════════════════╝");
        println!();

        let protocol = if self.config.tls.enabled {
            if self.config.tls.require_client_cert {
                "TLS/mTLS"
            } else {
                "TLS"
            }
        } else {
            "TCP"
        };

        println!(
            "  Kafka protocol: {} ({})",
            self.config.listen_addr, protocol
        );
        println!("  HTTP API:       {}", self.config.http_addr);
        println!("  Data directory: {}", self.config.data_dir.display());
        println!("  WAL:            {}", wal_status);

        if self.config.tls.enabled {
            println!(
                "  TLS version:    {} or higher",
                self.config.tls.min_version
            );
        }

        // Show cluster info if in cluster mode
        #[cfg(feature = "clustering")]
        if let Some(ref cluster_config) = self.config.cluster {
            println!(
                "  Cluster mode:   node {} @ {}",
                cluster_config.node_id, cluster_config.inter_broker_addr
            );
            if !cluster_config.seed_nodes.is_empty() {
                println!("  Seed nodes:     {:?}", cluster_config.seed_nodes);
            }
        } else {
            println!("  Cluster mode:   disabled (single-node)");
        }
        #[cfg(not(feature = "clustering"))]
        println!("  Cluster mode:   disabled (single-node)");

        // Show simple protocol info if enabled
        if self.config.simple.enabled {
            println!("  Simple protocol: {}", self.config.simple.addr);
        }

        println!();
        println!("  Ready to accept connections. Press Ctrl+C to stop.");
        println!();
    }

    /// Get server info
    pub fn info(&self) -> ServerInfo {
        ServerInfo {
            version: env!("CARGO_PKG_VERSION").to_string(),
            listen_addr: self.config.listen_addr.to_string(),
            http_addr: self.config.http_addr.to_string(),
            data_dir: self.config.data_dir.display().to_string(),
            topics: self.topic_manager.list_topics().unwrap_or_default().len(),
            wal_enabled: self.topic_manager.is_wal_enabled(),
        }
    }

    /// Log warnings about security configuration
    ///
    /// Helps operators notice potential security misconfigurations before
    /// deploying to production environments.
    fn log_security_warnings(&self) {
        // Durability warnings
        self.log_durability_warnings();

        if !self.config.tls.enabled {
            warn!(
                "TLS is DISABLED - all traffic is unencrypted. \
                 Enable with --tls-enabled for production deployments."
            );
        }

        #[cfg(feature = "auth")]
        if !self.config.auth.enabled {
            warn!(
                "Authentication is DISABLED - all clients can connect without credentials. \
                 Enable with --auth-enabled for production deployments."
            );
        } else if self.config.auth.allow_anonymous {
            warn!(
                "Anonymous connections are ALLOWED even though authentication is enabled. \
                 Consider disabling with --auth-allow-anonymous=false for production."
            );
        }
        #[cfg(not(feature = "auth"))]
        warn!(
            "Authentication is DISABLED (auth feature not compiled). \
             Build with --features auth for production deployments."
        );

        #[cfg(feature = "auth")]
        if self.config.acl.enabled && self.config.acl.allow_if_no_acls {
            warn!(
                "ACL is enabled but --acl-allow-if-no-acls is true (default). \
                 This permits all operations when no ACLs are defined. \
                 Consider setting --acl-allow-if-no-acls=false for production."
            );
        }

        // Warn about cluster mode without inter-broker TLS (future feature)
        #[cfg(feature = "clustering")]
        if self.config.cluster.is_some() && !self.config.tls.enabled {
            warn!(
                "Cluster mode is enabled but TLS is disabled. \
                 Inter-broker communication is unencrypted."
            );
        }

        // Warn about auto-create-topics (can cause accidental topic creation from typos)
        if self.config.auto_create_topics {
            warn!(
                "Auto-create-topics is ENABLED - topics are created automatically on produce. \
                 This can lead to accidental topic creation from typos or misconfigurations. \
                 Disable with --auto-create-topics=false for production deployments."
            );
        }
    }

    /// Log warnings about durability configuration
    ///
    /// Helps operators understand the durability implications of their WAL configuration.
    fn log_durability_warnings(&self) {
        let wal = &self.config.storage.wal;

        // In-memory mode warning (most critical)
        if self.config.storage.in_memory {
            warn!(
                "IN-MEMORY MODE ENABLED - no data will be persisted to disk. \
                 All data will be lost on server restart. \
                 Only use for development, testing, or ephemeral workloads."
            );
            return; // No point warning about WAL when in-memory
        }

        // WAL disabled warning
        if !wal.enabled {
            warn!(
                "WAL is DISABLED - data durability is not guaranteed. \
                 Data may be lost on crash before segments are written. \
                 Enable with --wal-enabled=true for production deployments."
            );
            return;
        }

        // Sync mode warnings
        match wal.sync_mode.as_str() {
            "os_default" => {
                warn!(
                    "WAL sync mode is 'os_default' - durability depends on OS buffer flushing. \
                     Data loss of several minutes is possible on crash. \
                     Use --wal-sync-mode=interval or --wal-sync-mode=every_write for production."
                );
            }
            "interval" => {
                // Always log the data loss window explicitly so operators are aware
                if wal.sync_interval_ms > 1000 {
                    warn!(
                        wal_sync_interval_ms = wal.sync_interval_ms,
                        max_data_loss_seconds = wal.sync_interval_ms / 1000,
                        "WAL sync interval is {}ms - up to {} seconds of data may be lost on crash. \
                         Consider using --wal-sync-interval-ms=100 for better durability.",
                        wal.sync_interval_ms,
                        wal.sync_interval_ms / 1000
                    );
                } else if wal.sync_interval_ms > 100 {
                    warn!(
                        wal_sync_mode = "interval",
                        wal_sync_interval_ms = wal.sync_interval_ms,
                        max_data_loss_ms = wal.sync_interval_ms,
                        "WAL durability: up to {}ms of data may be lost on crash. \
                         Use --wal-sync-interval-ms=100 (default) or lower for better durability.",
                        wal.sync_interval_ms
                    );
                } else {
                    info!(
                        wal_sync_mode = "interval",
                        wal_sync_interval_ms = wal.sync_interval_ms,
                        max_data_loss_ms = wal.sync_interval_ms,
                        "WAL durability: up to {}ms of data may be lost on crash",
                        wal.sync_interval_ms
                    );
                }
            }
            "every_write" => {
                info!(
                    wal_sync_mode = "every_write",
                    "WAL durability: maximum (fsync after every write, no data loss on crash)"
                );
            }
            _ => {
                warn!(
                    wal_sync_mode = %wal.sync_mode,
                    "Unknown WAL sync mode '{}' - defaulting to 'interval' behavior",
                    wal.sync_mode
                );
            }
        }
    }
}

/// Server information
#[derive(Debug)]
pub struct ServerInfo {
    pub version: String,
    pub listen_addr: String,
    pub http_addr: String,
    pub data_dir: String,
    pub topics: usize,
    pub wal_enabled: bool,
}

/// Configure TCP socket options for optimal performance
///
/// This function applies the following optimizations:
/// - TCP_NODELAY: Disable Nagle's algorithm for low-latency request-response
/// - TCP keepalive: Detect dead connections and clean up resources
/// - SO_RCVBUF/SO_SNDBUF: Tune socket buffer sizes for high throughput
fn configure_tcp_socket(stream: &TcpStream, tcp_config: &crate::config::TcpConfig) -> Result<()> {
    use socket2::SockRef;

    // Always set TCP_NODELAY for low-latency request-response
    stream.set_nodelay(true)?;

    // Use SockRef to access socket options without taking ownership
    let sock_ref = SockRef::from(stream);

    // Configure TCP keepalive if enabled
    if tcp_config.keepalive_enabled {
        #[allow(unused_mut)]
        let mut keepalive = socket2::TcpKeepalive::new()
            .with_time(Duration::from_secs(tcp_config.keepalive_idle_secs as u64))
            .with_interval(Duration::from_secs(
                tcp_config.keepalive_interval_secs as u64,
            ));

        // Set retries on platforms that support it (Linux/Android)
        #[cfg(any(target_os = "linux", target_os = "android"))]
        {
            keepalive = keepalive.with_retries(tcp_config.keepalive_retries);
        }

        sock_ref.set_tcp_keepalive(&keepalive)?;
    }

    // Configure receive buffer size if non-zero
    if tcp_config.recv_buffer_size > 0 {
        sock_ref.set_recv_buffer_size(tcp_config.recv_buffer_size as usize)?;
    }

    // Configure send buffer size if non-zero
    if tcp_config.send_buffer_size > 0 {
        sock_ref.set_send_buffer_size(tcp_config.send_buffer_size as usize)?;
    }

    Ok(())
}
