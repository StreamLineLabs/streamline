//! HTTP server for metrics, health checks, and REST API
//!
//! This module provides a simple HTTP API for:
//! - Prometheus metrics exposition (/metrics)
//! - Health checks (/health, /health/live, /health/ready)
//! - Server information (/info)
//! - REST API for producing and consuming messages (/api/v1/*)
//! - Schema Registry REST API (/subjects/*, /schemas/*, /config/*, /compatibility/*)
//! - GraphQL API with subscriptions (/graphql)

#[cfg(feature = "analytics")]
use crate::analytics::DuckDBEngine;
use crate::config::ServerConfig;
use crate::consumer::GroupCoordinator;
use crate::metrics::MetricsHistory;
#[cfg(feature = "schema-registry")]
use crate::schema::{SchemaRegistryConfig, SchemaStore};
use crate::server::alerts::AlertStore;
use crate::server::alerts_api::{create_alerts_api_router, AlertsApiState};
#[cfg(feature = "analytics")]
use crate::server::analytics_api::{create_analytics_api_router, AnalyticsApiState};
use crate::server::api::{create_api_router, ApiState};
use crate::server::benchmark_api::{
    create_benchmark_api_router, BenchmarkApiState, BenchmarkStore,
};
use crate::server::browser_client::{create_browser_client_router, BrowserClientState};
use crate::server::cdc_api::{create_cdc_api_router, CdcApiState};
use crate::server::cluster_api::{create_cluster_api_router, ClusterApiState};
use crate::server::connections_api::{create_connections_api_router, ConnectionsApiState};
use crate::server::connector_mgmt_api::{create_connector_mgmt_api_router, ConnectorMgmtApiState};
use crate::server::console_api::{create_console_api_router, ConsoleApiState};
use crate::server::consumer_api::{create_consumer_api_router, ConsumerApiState};
use crate::server::dashboard_api::{create_dashboard_api_router, DashboardApiState};
use crate::server::inspector_api;
#[cfg(feature = "graphql")]
use crate::server::graphql_routes::{create_graphql_router, GraphQLState};
#[cfg(feature = "kafka-connect")]
use crate::connect::api::{create_connect_router, ConnectApiState, ConnectorManager};
#[cfg(feature = "ai")]
use crate::server::ai_api::{create_ai_api_router, AiApiState};
use crate::server::cloud_api::{create_cloud_api_router, CloudApiState};
#[cfg(feature = "featurestore")]
use crate::server::featurestore_api::{create_featurestore_api_router, FeatureStoreApiState};
#[cfg(feature = "clustering")]
use crate::server::failover_api::{create_failover_api_router, FailoverApiState};
#[cfg(feature = "clustering")]
use crate::server::raft_cluster_api::{create_raft_cluster_api_router, RaftClusterApiState};
use crate::server::gitops_api::{create_gitops_api_router, GitOpsApiState};
use crate::server::governor_api::{create_governor_api_router, GovernorApiState};
use crate::server::log_buffer::LogBuffer;
use crate::server::logs_api::{create_logs_api_router, LogsApiState};
use crate::server::metadata_cache::{MetadataCache, MetadataCacheConfig};
use crate::server::observability_api::{create_observability_api_router, ObservabilityApiState};
use crate::server::playground_api::{create_playground_api_router, PlaygroundApiState};
use crate::server::plugin_api::{create_plugin_api_router, PluginApiState};
use crate::server::replication_api::{create_replication_api_router, ReplicationApiState};
#[cfg(feature = "schema-registry")]
use crate::server::schema_api::{create_schema_api_router, SchemaApiState};
#[cfg(feature = "schema-registry")]
use crate::server::schema_ui::{create_schema_ui_router, SchemaUiState};
use crate::server::shutdown::ShutdownCoordinator;
#[cfg(feature = "sqlite-queries")]
use crate::server::sqlite_routes::{create_sqlite_api_router, SqliteApiState};
#[cfg(feature = "sqlite-queries")]
use crate::sqlite::SQLiteQueryEngine;
use crate::server::wasm_api::{create_wasm_api_router, WasmApiState};
use crate::server::websocket::{create_websocket_router, WebSocketState};
use crate::server::websocket_streaming::{create_streaming_router, StreamingState};
use crate::storage::TopicManager;
use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
#[cfg(feature = "metrics")]
use metrics_exporter_prometheus::PrometheusHandle;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tracing::info;

/// Shared HTTP server state
#[derive(Clone)]
pub struct HttpServerState {
    /// Prometheus metrics handle (requires metrics feature)
    #[cfg(feature = "metrics")]
    pub metrics_handle: PrometheusHandle,
    /// Topic manager for health checks
    pub topic_manager: Arc<TopicManager>,
    /// Group coordinator for consumer group operations (optional)
    pub group_coordinator: Option<Arc<GroupCoordinator>>,
    /// Server configuration
    pub config: ServerConfig,
    /// Server start time
    pub start_time: Instant,
    /// Shutdown coordinator for connection tracking
    pub shutdown_coordinator: Arc<ShutdownCoordinator>,
    /// Log buffer for real-time log viewing (optional, uses new if None)
    pub log_buffer: Option<Arc<LogBuffer>>,
    /// Cluster manager for Raft cluster operations (requires clustering feature)
    #[cfg(feature = "clustering")]
    pub cluster_manager: Option<Arc<crate::cluster::ClusterManager>>,
}

#[derive(Clone)]
struct HttpBootstrap {
    metadata_cache: Arc<MetadataCache>,
    metrics_history: Arc<MetricsHistory>,
    alert_store: Arc<AlertStore>,
}

/// Health check status
#[derive(Debug, Serialize, Deserialize)]
pub struct HealthStatus {
    /// Overall status ("healthy" or "unhealthy")
    pub status: String,
    /// Individual checks
    pub checks: Vec<HealthCheck>,
}

/// Individual health check
#[derive(Debug, Serialize, Deserialize)]
pub struct HealthCheck {
    /// Check name
    pub name: String,
    /// Check status ("ok" or "failed")
    pub status: String,
    /// Optional error message
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Server information
#[derive(Debug, Serialize, Deserialize)]
pub struct ServerInfo {
    /// Server version
    pub version: String,
    /// Kafka listener address
    pub listen_addr: SocketAddr,
    /// HTTP API address
    pub http_addr: SocketAddr,
    /// Data directory
    pub data_dir: PathBuf,
    /// Number of topics
    pub topics: usize,
    /// Server uptime in seconds
    pub uptime_seconds: f64,
}

fn build_base_router(state: &HttpServerState) -> Router {
    Router::new()
        .route("/health", get(health_handler))
        .route("/health/live", get(liveness_handler))
        .route("/health/ready", get(readiness_handler))
        .route("/info", get(info_handler))
        .route("/api/v1/features", get(features_handler))
        .with_state(state.clone())
}

fn init_http_bootstrap(state: &HttpServerState) -> HttpBootstrap {
    // Add cluster API for cluster/broker information with metadata caching
    let metadata_cache = MetadataCache::new_shared(MetadataCacheConfig::default());
    // Create shared metrics history for rate tracking and historical charts
    let metrics_history = MetricsHistory::new_shared();
    // Create shared alert store used by both dashboard and alerts API
    let alert_store = AlertStore::new_shared();

    // Spawn background task to sample metrics periodically
    {
        let history = metrics_history.clone();
        let topic_manager = state.topic_manager.clone();
        let shutdown = state.shutdown_coordinator.clone();
        tokio::spawn(async move {
            metrics_sampling_task(history, topic_manager, shutdown).await;
        });
    }

    HttpBootstrap {
        metadata_cache,
        metrics_history,
        alert_store,
    }
}

fn build_http_router(state: &HttpServerState, bootstrap: &HttpBootstrap) -> Router {
    // Create the REST API router
    let api_state = ApiState {
        topic_manager: state.topic_manager.clone(),
    };
    let api_router = create_api_router(api_state);

    // Create the WebSocket router
    let ws_state = WebSocketState {
        topic_manager: state.topic_manager.clone(),
    };
    let ws_router = create_websocket_router(ws_state);

    // Create advanced streaming router
    let streaming_state = StreamingState::new(state.topic_manager.clone());
    let streaming_router = create_streaming_router(streaming_state);

    // Merge the REST API, WebSocket, and Streaming routers
    let mut app = build_base_router(state)
        .merge(api_router)
        .merge(ws_router)
        .merge(streaming_router);

    // Add metrics endpoints (requires metrics feature)
    #[cfg(feature = "metrics")]
    {
        let metrics_router = Router::new()
            .route("/metrics", get(metrics_handler))
            .route("/metrics/jmx", get(jmx_metrics_handler))
            .with_state(state.clone());
        app = app.merge(metrics_router);
    }

    // Create Schema Registry API router (requires schema-registry feature)
    #[cfg(feature = "schema-registry")]
    {
        let schema_config = SchemaRegistryConfig::default();
        let schema_store = Arc::new(SchemaStore::with_topic_manager(
            schema_config,
            state.topic_manager.clone(),
        ));
        // Ensure _schemas topic exists and replay persisted events
        if let Err(e) = schema_store.ensure_schemas_topic() {
            tracing::warn!("Failed to create _schemas topic: {}", e);
        }
        // Replay is async but we're in a sync context; spawn a task
        {
            let store = schema_store.clone();
            tokio::spawn(async move {
                if let Err(e) = store.replay_from_topic().await {
                    tracing::warn!("Failed to replay schema events: {}", e);
                }
            });
        }
        let schema_state = SchemaApiState {
            store: schema_store.clone(),
        };
        let schema_router = create_schema_api_router(schema_state);
        app = app.merge(schema_router);

        // Schema Registry Web UI (served at /ui/schemas/*)
        let schema_ui_state = SchemaUiState {
            store: schema_store,
        };
        let schema_ui_router = create_schema_ui_router(schema_ui_state);
        app = app.merge(schema_ui_router);
    }

    let cluster_state = ClusterApiState {
        topic_manager: state.topic_manager.clone(),
        config: state.config.clone(),
        start_time: state.start_time,
        metadata_cache: bootstrap.metadata_cache.clone(),
        metrics_history: Some(bootstrap.metrics_history.clone()),
    };
    let cluster_router = create_cluster_api_router(cluster_state);
    app = app.merge(cluster_router);

    // Add dashboard API for web dashboard
    let dashboard_state = DashboardApiState {
        topic_manager: state.topic_manager.clone(),
        config: state.config.clone(),
        start_time: state.start_time,
        metadata_cache: bootstrap.metadata_cache.clone(),
        metrics_history: Some(bootstrap.metrics_history.clone()),
        alert_store: Some(bootstrap.alert_store.clone()),
        group_coordinator: state.group_coordinator.clone(),
    };
    let dashboard_router = create_dashboard_api_router(dashboard_state);
    app = app.merge(dashboard_router);

    // Add message inspector API for dev mode and web console
    let inspector_state = inspector_api::InspectorState {
        topic_manager: state.topic_manager.clone(),
    };
    let inspector_router = inspector_api::create_inspector_router(inspector_state);
    app = app.merge(inspector_router);

    // Add embedded web console at /console
    let console_router = crate::server::console_page::create_console_page_router();
    app = app.merge(console_router);

    // Add consumer groups API if coordinator is available
    if let Some(coordinator) = state.group_coordinator.clone() {
        let consumer_state = ConsumerApiState {
            coordinator,
            topic_manager: state.topic_manager.clone(),
        };
        let consumer_router = create_consumer_api_router(consumer_state);
        app = app.merge(consumer_router);
    }

    // Add logs API for real-time log viewing
    // Use provided buffer (for tracing integration) or create new one
    let log_buffer = state
        .log_buffer
        .clone()
        .unwrap_or_else(LogBuffer::new_shared);
    let logs_state = LogsApiState { buffer: log_buffer };
    let logs_router = create_logs_api_router(logs_state);
    app = app.merge(logs_router);

    // Add connections API for connection inspection
    let connections_state = ConnectionsApiState {
        shutdown_coordinator: state.shutdown_coordinator.clone(),
        start_time: state.start_time,
    };
    let connections_router = create_connections_api_router(connections_state);
    app = app.merge(connections_router);

    // Add alerts API for alert configuration (uses shared alert store)
    let alerts_state = AlertsApiState {
        store: bootstrap.alert_store.clone(),
    };
    let alerts_router = create_alerts_api_router(alerts_state);
    app = app.merge(alerts_router);

    // Add benchmark API for performance testing
    let benchmark_state = BenchmarkApiState {
        store: BenchmarkStore::new_shared(),
    };
    let benchmark_router = create_benchmark_api_router(benchmark_state);
    app = app.merge(benchmark_router);

    // Add browser-native client SDK for JavaScript/TypeScript clients
    let browser_state = BrowserClientState::new(state.topic_manager.clone());
    let browser_router = create_browser_client_router(browser_state);
    app = app.merge(browser_router);

    // Add analytics API for SQL queries (requires analytics feature)
    #[cfg(feature = "analytics")]
    {
        // Create DuckDB engine
        match DuckDBEngine::new(state.topic_manager.clone()) {
            Ok(engine) => {
                let analytics_state = AnalyticsApiState {
                    engine: Arc::new(engine),
                };
                let analytics_router = create_analytics_api_router(analytics_state);
                app = app.merge(analytics_router);
            }
            Err(e) => {
                tracing::warn!("Failed to initialize analytics engine: {}", e);
            }
        }
    }

    // Add SQLite query API (requires sqlite-queries feature)
    #[cfg(feature = "sqlite-queries")]
    {
        match SQLiteQueryEngine::new(state.topic_manager.clone()) {
            Ok(engine) => {
                let sqlite_state = SqliteApiState {
                    engine: Arc::new(engine),
                };
                let sqlite_router = create_sqlite_api_router(sqlite_state);
                app = app.merge(sqlite_router);
                tracing::info!("SQLite query API enabled at /api/v1/sqlite/*");
            }
            Err(e) => {
                tracing::warn!("Failed to initialize SQLite query engine: {}", e);
            }
        }
    }

    // GraphQL API with subscriptions (requires graphql feature)
    #[cfg(feature = "graphql")]
    {
        let schema = crate::graphql::build_schema(
            state.topic_manager.clone(),
            state.start_time,
            state.group_coordinator.clone(),
        );
        let graphql_state = GraphQLState { schema };
        let graphql_router = create_graphql_router(graphql_state);
        app = app.merge(graphql_router);
        tracing::info!("GraphQL API enabled at /graphql (POST queries, GET playground, /graphql/ws subscriptions)");
    }

    // Add Kafka Connect REST API (requires kafka-connect feature)
    // This provides Confluent-compatible Connect REST API endpoints under /connectors/*
    #[cfg(feature = "kafka-connect")]
    {
        let connect_state = ConnectApiState {
            connector_manager: Arc::new(ConnectorManager::new()),
            runtime: None,
        };
        let connect_router = create_connect_router(connect_state);
        app = app.merge(connect_router);
    }

    // Add Cloud API for tenant/endpoint management
    if let Ok(cloud) = crate::cloud::StreamlineCloud::new(crate::cloud::CloudConfig::default()) {
        let cloud_state = CloudApiState {
            cloud: Arc::new(cloud),
            config: crate::cloud::CloudConfig::default(),
        };
        let cloud_router = create_cloud_api_router(cloud_state);
        app = app.merge(cloud_router);
    }

    // Add GitOps API for declarative configuration management
    let gitops_state = GitOpsApiState::new();
    let gitops_router = create_gitops_api_router(gitops_state);
    app = app.merge(gitops_router);

    // Add WASM API for stream processing module management
    let wasm_state = WasmApiState::new();
    let wasm_router = create_wasm_api_router(wasm_state);
    app = app.merge(wasm_router);

    // Add WASM Transform Marketplace API (registry, install, list)
    #[cfg(feature = "kafka-connect")]
    {
        use crate::connect::marketplace::{create_marketplace_router, MarketplaceApiState};
        let marketplace_state =
            MarketplaceApiState::new(state.config.data_dir.join("marketplace-transforms"));
        let marketplace_router = create_marketplace_router(marketplace_state);
        app = app.merge(marketplace_router);
    }

    // Add Observability API for unified monitoring
    let obs_state = ObservabilityApiState::new();
    let obs_router = create_observability_api_router(obs_state);
    app = app.merge(obs_router);

    // Add Playground API and Web UI (only when playground mode is enabled)
    if state.config.playground {
        let playground_state = PlaygroundApiState::new(state.topic_manager.clone());
        let playground_router = create_playground_api_router(playground_state);
        app = app.merge(playground_router);
        tracing::info!("Playground web UI enabled at / and /playground");
    }

    // Add AI Pipeline API for ML/AI stream processing
    #[cfg(feature = "ai")]
    {
        let ai_state = AiApiState::new();
        let ai_router = create_ai_api_router(ai_state);
        app = app.merge(ai_router);
    }

    // Add Feature Store API for ML feature serving
    #[cfg(feature = "featurestore")]
    {
        match crate::featurestore::engine::FeatureStoreEngine::new(
            crate::featurestore::config::FeatureStoreConfig::default(),
        ) {
            Ok(engine) => {
                let featurestore_state = FeatureStoreApiState {
                    engine: Arc::new(engine),
                };
                let featurestore_router = create_featurestore_api_router(featurestore_state);
                app = app.merge(featurestore_router);
            }
            Err(e) => {
                tracing::warn!("Failed to initialize feature store engine: {}", e);
            }
        }
    }

    // Add Plugin Marketplace API for extension management
    let plugin_state = PluginApiState::new(state.config.data_dir.join("plugins"));
    let plugin_router = create_plugin_api_router(plugin_state);
    app = app.merge(plugin_router);

    // Add CDC API for Change Data Capture management
    let cdc_state = CdcApiState::new();
    let cdc_router = create_cdc_api_router(cdc_state);
    app = app.merge(cdc_router);

    // Add Geo-Replication API for cross-region replication management
    let replication_state = ReplicationApiState::new();
    let replication_router = create_replication_api_router(replication_state);
    app = app.merge(replication_router);

    // Add Edge Deployment API for offline-first streaming (requires edge feature)
    #[cfg(feature = "edge")]
    {
        use crate::server::edge_api::{create_edge_api_router, EdgeApiState};
        let edge_state = EdgeApiState::new();
        let edge_router = create_edge_api_router(edge_state);
        app = app.merge(edge_router);
    }

    // Add Resource Governor API for auto-tuning management
    let governor_state = GovernorApiState {
        governor: Arc::new(crate::autotuning::governor::ResourceGovernor::default()),
    };
    let governor_router = create_governor_api_router(governor_state);
    app = app.merge(governor_router);

    // Add Cloud Console API for tenant/API key management
    let console_state = ConsoleApiState {
        console: Arc::new(crate::cloud::console::ConsoleManager::new()),
    };
    let console_router = create_console_api_router(console_state);
    app = app.merge(console_router);

    // Add Declarative Connector Management API
    let connector_mgmt_state = ConnectorMgmtApiState {
        manager: Arc::new(crate::marketplace::declarative::ConnectorManager::new()),
    };
    let connector_mgmt_router = create_connector_mgmt_api_router(connector_mgmt_state);
    app = app.merge(connector_mgmt_router);

    // Add Failover API for multi-region management
    #[cfg(feature = "clustering")]
    {
        let failover_state = FailoverApiState {
            orchestrator: Arc::new(crate::replication::failover::FailoverOrchestrator::new(
                "local",
                crate::replication::failover::FailoverConfig::default(),
            )),
        };
        let failover_router = create_failover_api_router(failover_state);
        app = app.merge(failover_router);
    }

    // Add Raft Cluster API for cluster health and membership management
    #[cfg(feature = "clustering")]
    {
        if let Some(ref cluster_mgr) = state.cluster_manager {
            let raft_cluster_state = RaftClusterApiState {
                cluster_manager: cluster_mgr.clone(),
            };
            let raft_cluster_router = create_raft_cluster_api_router(raft_cluster_state);
            app = app.merge(raft_cluster_router);
        }
    }

    app
}

/// Start HTTP server
pub async fn start_http_server(
    addr: SocketAddr,
    state: HttpServerState,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let bootstrap = init_http_bootstrap(&state);
    let app = build_http_router(&state, &bootstrap);

    info!(addr = %addr, "Starting HTTP API server (metrics, health, REST API, WebSocket, Schema Registry, Consumer Groups, Dashboard, Logs, Connections, Alerts, Benchmark, Analytics, Connect)");

    let listener = tokio::net::TcpListener::bind(addr).await.map_err(|e| {
        if e.kind() == std::io::ErrorKind::AddrInUse {
            let port = addr.port();
            format!(
                "HTTP port {} is already in use. \
                 Fix: Use --http-addr 0.0.0.0:{} to pick a different port, \
                 or stop the existing process.",
                port,
                port + 100
            )
        } else {
            format!("Failed to bind HTTP server to {}: {}", addr, e)
        }
    })?;
    axum::serve(listener, app).await?;

    Ok(())
}

/// Metrics endpoint handler (Prometheus format)
#[cfg(feature = "metrics")]
async fn metrics_handler(State(state): State<HttpServerState>) -> Response {
    let metrics = state.metrics_handle.render();
    (StatusCode::OK, metrics).into_response()
}

/// JMX metrics endpoint handler (Jolokia format)
///
/// Returns metrics in a format compatible with Kafka JMX monitoring tools.
/// Use this endpoint with tools expecting Jolokia-style JSON output.
#[cfg(feature = "metrics")]
async fn jmx_metrics_handler(State(state): State<HttpServerState>) -> Response {
    let jmx_output = crate::metrics::render_jmx_metrics(&state.metrics_handle);
    (
        StatusCode::OK,
        [("content-type", "application/json")],
        jmx_output,
    )
        .into_response()
}

/// Health check endpoint handler
async fn health_handler(State(state): State<HttpServerState>) -> Response {
    let checks = perform_health_checks(&state);
    let all_healthy = checks.iter().all(|c| c.status == "ok");

    let status = HealthStatus {
        status: if all_healthy { "healthy" } else { "unhealthy" }.to_string(),
        checks,
    };

    let status_code = if all_healthy {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    (status_code, Json(status)).into_response()
}

/// Liveness probe handler (always returns OK if server is running)
async fn liveness_handler() -> Response {
    (StatusCode::OK, "OK").into_response()
}

/// Readiness probe handler (checks if server can handle requests)
async fn readiness_handler(State(state): State<HttpServerState>) -> Response {
    let checks = perform_health_checks(&state);

    let storage_ok = checks
        .iter()
        .find(|c| c.name == "storage")
        .map_or(false, |c| c.status == "ok");
    let all_ready = checks.iter().all(|c| c.status == "ok");

    // Protocol and HTTP are ready if this handler is reachable
    let protocol_status = "ready";
    let http_status = "ready";
    let storage_status = if storage_ok { "ready" } else { "not_ready" };

    let overall = if all_ready { "ready" } else { "not_ready" };
    let status_code = if all_ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    let body = json!({
        "status": overall,
        "components": {
            "storage": storage_status,
            "protocol": protocol_status,
            "http": http_status,
        }
    });

    (status_code, Json(body)).into_response()
}

/// Server info endpoint handler
async fn info_handler(State(state): State<HttpServerState>) -> Response {
    let uptime = state.start_time.elapsed().as_secs_f64();
    let topics = match state.topic_manager.list_topics() {
        Ok(topics) => topics.len(),
        Err(e) => {
            tracing::warn!(error = %e, "Failed to list topics for info endpoint");
            0
        }
    };

    let info = ServerInfo {
        version: env!("CARGO_PKG_VERSION").to_string(),
        listen_addr: state.config.listen_addr,
        http_addr: state.config.http_addr,
        data_dir: state.config.data_dir.clone(),
        topics,
        uptime_seconds: uptime,
    };

    (StatusCode::OK, Json(info)).into_response()
}

/// Features discovery endpoint handler
///
/// Returns the status of all compile-time and runtime features.
/// Uses `#[cfg(feature = "...")]` to detect compiled features and checks
/// runtime config for features that can be toggled.
async fn features_handler(State(_state): State<HttpServerState>) -> Response {
    let mut features = serde_json::Map::new();

    // Auth feature
    {
        #[cfg(feature = "auth")]
        {
            let active = _state.config.auth.enabled;
            let status = if active { "active" } else { "disabled" };
            features.insert("auth".into(), json!({ "enabled": true, "status": status }));
        }
        #[cfg(not(feature = "auth"))]
        {
            features.insert("auth".into(), json!({
                "enabled": false,
                "status": "disabled",
                "reason": "Feature not compiled (requires --features auth)"
            }));
        }
    }

    // Clustering feature
    {
        #[cfg(feature = "clustering")]
        {
            let active = _state.config.cluster.is_some();
            let status = if active { "active" } else { "disabled" };
            features.insert("clustering".into(), json!({ "enabled": true, "status": status }));
        }
        #[cfg(not(feature = "clustering"))]
        {
            features.insert("clustering".into(), json!({
                "enabled": false,
                "status": "disabled",
                "reason": "Feature not compiled (requires --features clustering)"
            }));
        }
    }

    // Analytics feature
    {
        #[cfg(feature = "analytics")]
        {
            features.insert("analytics".into(), json!({ "enabled": true, "status": "active" }));
        }
        #[cfg(not(feature = "analytics"))]
        {
            features.insert("analytics".into(), json!({
                "enabled": false,
                "status": "disabled",
                "reason": "Feature not compiled (requires --features analytics)"
            }));
        }
    }

    // Schema Registry feature
    {
        #[cfg(feature = "schema-registry")]
        {
            features.insert("schema_registry".into(), json!({ "enabled": true, "status": "active" }));
        }
        #[cfg(not(feature = "schema-registry"))]
        {
            features.insert("schema_registry".into(), json!({
                "enabled": false,
                "status": "disabled",
                "reason": "Feature not compiled (requires --features schema-registry)"
            }));
        }
    }

    // Encryption feature
    {
        #[cfg(feature = "encryption")]
        {
            features.insert("encryption".into(), json!({ "enabled": true, "status": "active" }));
        }
        #[cfg(not(feature = "encryption"))]
        {
            features.insert("encryption".into(), json!({
                "enabled": false,
                "status": "disabled",
                "reason": "Feature not compiled (requires --features encryption)"
            }));
        }
    }

    // Telemetry feature
    {
        #[cfg(feature = "telemetry")]
        {
            let active = _state.config.telemetry.enabled;
            let status = if active { "active" } else { "disabled" };
            features.insert("telemetry".into(), json!({ "enabled": true, "status": status }));
        }
        #[cfg(not(feature = "telemetry"))]
        {
            features.insert("telemetry".into(), json!({
                "enabled": false,
                "status": "disabled",
                "reason": "Feature not compiled (requires --features telemetry)"
            }));
        }
    }

    // Web UI feature
    {
        #[cfg(feature = "web-ui")]
        {
            features.insert("web_ui".into(), json!({ "enabled": true, "status": "active" }));
        }
        #[cfg(not(feature = "web-ui"))]
        {
            features.insert("web_ui".into(), json!({
                "enabled": false,
                "status": "disabled",
                "reason": "Feature not compiled (requires --features web-ui)"
            }));
        }
    }

    let body = json!({ "features": Value::Object(features) });
    (StatusCode::OK, Json(body)).into_response()
}
fn perform_health_checks(state: &HttpServerState) -> Vec<HealthCheck> {
    let mut checks = Vec::new();

    // Check if storage is accessible
    let storage_check = match state.topic_manager.list_topics() {
        Ok(_) => HealthCheck {
            name: "storage".to_string(),
            status: "ok".to_string(),
            message: None,
        },
        Err(e) => HealthCheck {
            name: "storage".to_string(),
            status: "failed".to_string(),
            message: Some(format!("Storage error: {}", e)),
        },
    };
    checks.push(storage_check);

    // Check if data directory is accessible
    let data_dir_check = if state.config.data_dir.exists() {
        HealthCheck {
            name: "data_directory".to_string(),
            status: "ok".to_string(),
            message: None,
        }
    } else {
        HealthCheck {
            name: "data_directory".to_string(),
            status: "failed".to_string(),
            message: Some("Data directory not accessible".to_string()),
        }
    };
    checks.push(data_dir_check);

    checks
}

/// Background task to sample metrics periodically for rate tracking
///
/// This task samples topic metrics every 5 seconds and calculates:
/// - Messages per second (based on message count deltas)
/// - Bytes in/out per second (placeholder - requires deeper integration)
async fn metrics_sampling_task(
    history: Arc<MetricsHistory>,
    topic_manager: Arc<TopicManager>,
    shutdown: Arc<ShutdownCoordinator>,
) {
    use crate::metrics::MetricPoint;
    use std::time::{SystemTime, UNIX_EPOCH};

    let sample_interval = history.sample_interval();
    let mut prev_total_messages: u64 = 0;
    let mut prev_sample_time = std::time::Instant::now();

    loop {
        // Check for shutdown
        if shutdown.is_shutting_down() {
            tracing::debug!("Metrics sampling task shutting down");
            break;
        }

        // Wait for next sample interval
        tokio::time::sleep(sample_interval).await;

        // Sample current metrics
        let now = std::time::Instant::now();
        let elapsed_secs = now.duration_since(prev_sample_time).as_secs_f64();

        // Get topic stats
        let topic_stats = match topic_manager.get_all_topic_stats() {
            Ok(stats) => stats,
            Err(e) => {
                tracing::debug!(error = %e, "Failed to get topic stats for metrics sampling");
                continue;
            }
        };

        // Calculate totals
        let mut total_messages: u64 = 0;
        let mut total_partitions: usize = 0;

        for stats in &topic_stats {
            total_partitions += stats.num_partitions as usize;
            total_messages += stats.total_messages;
        }

        // Calculate rates
        let messages_per_sec = if elapsed_secs > 0.0 && prev_total_messages > 0 {
            let delta = total_messages.saturating_sub(prev_total_messages);
            delta as f64 / elapsed_secs
        } else {
            0.0
        };

        // Get timestamp
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        // Create and store metric point
        let point = MetricPoint {
            timestamp,
            messages_per_sec,
            bytes_in_per_sec: 0.0,  // Would require tracking at produce handler
            bytes_out_per_sec: 0.0, // Would require tracking at fetch handler
            connections: shutdown.active_connections() as usize,
            topics: topic_stats.len(),
            partitions: total_partitions,
            consumer_groups: 0, // Would require group coordinator access
            total_consumer_lag: 0,
        };

        history.add_point(point);

        // Update previous values for next iteration
        prev_total_messages = total_messages;
        prev_sample_time = now;
    }
}

// GraphQL Playground temporarily disabled - waiting for async-graphql-axum to support axum 0.7.x
// /// GraphQL Playground HTML page
// #[cfg(feature = "graphql")]
// async fn graphql_playground() -> axum::response::Html<&'static str> {
//     axum::response::Html(
//         r#"<!DOCTYPE html>
// <html>
// <head>
//   <title>Streamline GraphQL Playground</title>
//   <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/graphql-playground-react/build/static/css/index.css" />
//   <script src="https://cdn.jsdelivr.net/npm/graphql-playground-react/build/static/js/middleware.js"></script>
// </head>
// <body>
//   <div id="root"></div>
//   <script>
//     window.addEventListener('load', function() {
//       GraphQLPlayground.init(document.getElementById('root'), {
//         endpoint: '/graphql',
//         subscriptionEndpoint: '/graphql/ws',
//         settings: {
//           'editor.theme': 'dark',
//           'tracing.hideTracingResponse': true,
//         }
//       });
//     });
//   </script>
// </body>
// </html>"#,
//     )
// }

// Tests require metrics and auth features for full functionality
#[cfg(all(test, feature = "metrics", feature = "auth"))]
mod tests {
    use super::*;
    use crate::metrics;
    use tempfile::TempDir;

    fn create_test_state() -> (HttpServerState, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let config = ServerConfig {
            listen_addr: "127.0.0.1:9092".parse().unwrap(),
            http_addr: "127.0.0.1:9094".parse().unwrap(),
            data_dir: temp_dir.path().to_path_buf(),
            log_level: "info".to_string(),
            storage: crate::config::StorageConfig::default(),
            tls: crate::config::TlsConfig::default(),
            tcp: crate::config::TcpConfig::default(),
            auth: crate::config::AuthConfig::default(),
            acl: crate::config::AclConfig::default(),
            audit: crate::audit::AuditConfig::default(),
            limits: crate::server::limits::LimitsConfig::default(),
            shutdown: crate::server::shutdown::ShutdownConfig::default(),
            quotas: crate::server::limits::QuotaConfig::default(),
            cluster: None,
            simple: crate::config::SimpleProtocolConfig::default(),
            auto_create_topics: crate::config::DEFAULT_AUTO_CREATE_TOPICS,
            runtime: crate::config::RuntimeConfig::default(),
            telemetry: crate::telemetry::TelemetryConfig::default(),
            playground: false,
            #[cfg(feature = "edge")]
            edge: crate::config::EdgeDeploymentConfig::default(),
        };

        let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());
        let metrics_handle = metrics::init_metrics();

        let state = HttpServerState {
            metrics_handle,
            topic_manager,
            group_coordinator: None, // Not needed for basic HTTP tests
            config: config.clone(),
            start_time: Instant::now(),
            shutdown_coordinator: Arc::new(ShutdownCoordinator::with_config(config.shutdown)),
            log_buffer: None,
        };

        (state, temp_dir)
    }

    #[test]
    fn test_perform_health_checks() {
        let (state, _temp_dir) = create_test_state();
        let checks = perform_health_checks(&state);

        assert!(!checks.is_empty());
        assert!(checks.iter().any(|c| c.name == "storage"));
        assert!(checks.iter().any(|c| c.name == "data_directory"));
    }

    #[test]
    fn test_health_checks_all_ok() {
        let (state, _temp_dir) = create_test_state();
        let checks = perform_health_checks(&state);

        assert!(checks.iter().all(|c| c.status == "ok"));
    }

    #[tokio::test]
    async fn test_liveness_handler() {
        let response = liveness_handler().await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_metrics_handler() {
        let (state, _temp_dir) = create_test_state();
        let response = metrics_handler(State(state)).await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_jmx_metrics_handler() {
        let (state, _temp_dir) = create_test_state();
        let response = jmx_metrics_handler(State(state)).await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_info_handler() {
        let (state, _temp_dir) = create_test_state();
        let response = info_handler(State(state)).await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_health_handler() {
        let (state, _temp_dir) = create_test_state();
        let response = health_handler(State(state)).await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_readiness_handler() {
        let (state, _temp_dir) = create_test_state();
        let response = readiness_handler(State(state)).await;
        assert_eq!(response.status(), StatusCode::OK);
    }
}
