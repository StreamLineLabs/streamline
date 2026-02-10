//! HTTP REST API for cluster and broker information
//!
//! This module provides REST endpoints for cluster overview and broker listing:
//!
//! ## Endpoints
//!
//! - `GET /api/v1/cluster` - Get cluster overview
//! - `GET /api/v1/brokers` - List all brokers

use crate::config::ServerConfig;
use crate::metrics::{HistoricalMetrics, MetricsHistory};
use crate::server::metadata_cache::{CachedClusterOverview, MetadataCache};
use crate::storage::TopicManager;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error};

/// Shared state for the cluster API
#[derive(Clone)]
pub(crate) struct ClusterApiState {
    /// Topic manager for storage operations
    pub topic_manager: Arc<TopicManager>,
    /// Server configuration
    pub config: ServerConfig,
    /// Server start time (for future uptime reporting)
    #[allow(dead_code)]
    pub start_time: Instant,
    /// Metadata cache for performance optimization
    pub metadata_cache: Arc<MetadataCache>,
    /// Historical metrics storage (optional)
    pub metrics_history: Option<Arc<MetricsHistory>>,
}

/// Cluster overview response
/// Must match src/ui/client.rs ClusterOverview struct exactly
#[derive(Debug, Serialize)]
pub struct ClusterOverview {
    pub cluster_id: String,
    pub controller_id: Option<u64>,
    pub broker_count: usize,
    pub topic_count: usize,
    pub partition_count: usize,
    pub online_partition_count: usize,
    pub offline_partition_count: usize,
    pub under_replicated_partitions: usize,
    pub messages_per_second: f64,
    pub bytes_in_per_second: f64,
    pub bytes_out_per_second: f64,
    pub total_messages: u64,
    pub total_bytes: u64,
}

/// Broker information response
/// Must match src/ui/client.rs BrokerInfo struct exactly
#[derive(Debug, Serialize)]
pub struct BrokerInfo {
    pub node_id: u64,
    pub host: String,
    pub port: u16,
    pub rack: Option<String>,
    pub is_controller: bool,
    pub state: String,
}

/// Query parameters for historical metrics
#[derive(Debug, Deserialize)]
pub struct MetricsHistoryQuery {
    /// Duration in format like "1h", "6h", "24h" (default: "1h")
    #[serde(default = "default_duration")]
    pub duration: String,
}

fn default_duration() -> String {
    "1h".to_string()
}

/// Create the cluster API router
pub(crate) fn create_cluster_api_router(state: ClusterApiState) -> Router {
    Router::new()
        .route("/api/v1/cluster", get(cluster_overview_handler))
        .route("/api/v1/brokers", get(list_brokers_handler))
        .route("/api/v1/metrics/history", get(metrics_history_handler))
        .with_state(state)
}

/// Get the latest rate metrics from the metrics history
fn get_latest_rates(metrics_history: &Option<Arc<MetricsHistory>>) -> (f64, f64, f64) {
    if let Some(history) = metrics_history {
        let points = history.get_all();
        if let Some(latest) = points.last() {
            return (
                latest.messages_per_sec,
                latest.bytes_in_per_sec,
                latest.bytes_out_per_sec,
            );
        }
    }
    (0.0, 0.0, 0.0)
}

/// GET /api/v1/cluster - Get cluster overview
async fn cluster_overview_handler(State(state): State<ClusterApiState>) -> Response {
    // Get latest rate metrics from history
    let (messages_per_second, bytes_in_per_second, bytes_out_per_second) =
        get_latest_rates(&state.metrics_history);

    // Check cache first for performance
    if let Some(cached) = state.metadata_cache.get_cluster_overview() {
        debug!("Returning cached cluster overview");
        let overview = ClusterOverview {
            cluster_id: cached.cluster_id,
            controller_id: cached.controller_id,
            broker_count: cached.broker_count,
            topic_count: cached.topic_count,
            partition_count: cached.partition_count,
            online_partition_count: cached.online_partition_count,
            offline_partition_count: cached.offline_partition_count,
            under_replicated_partitions: cached.under_replicated_partitions,
            messages_per_second,
            bytes_in_per_second,
            bytes_out_per_second,
            total_messages: cached.total_messages,
            total_bytes: cached.total_bytes,
        };
        return (StatusCode::OK, Json(overview)).into_response();
    }

    debug!("Cache miss - computing cluster overview");

    // Use batch offset lookup for performance (single lock acquisition)
    let topic_stats = match state.topic_manager.get_all_topic_stats() {
        Ok(stats) => stats,
        Err(e) => {
            error!(error = %e, "Failed to get topic stats for cluster overview");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": format!("Failed to get topic stats: {}", e)})),
            )
                .into_response();
        }
    };

    let topic_count = topic_stats.len();
    let mut partition_count = 0usize;
    let mut total_messages = 0u64;
    let mut total_bytes = 0u64;

    for stats in &topic_stats {
        partition_count += stats.num_partitions as usize;
        total_messages += stats.total_messages;
        match state.topic_manager.all_partition_info(&stats.name) {
            Ok(partitions) => {
                total_bytes += partitions.iter().map(|p| p.size).sum::<u64>();
            }
            Err(e) => {
                error!(
                    error = %e,
                    topic = %stats.name,
                    "Failed to get partition sizes for cluster overview"
                );
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({"error": format!("Failed to get topic sizes: {}", e)})),
                )
                    .into_response();
            }
        }
    }

    // Cache the computed overview
    let cached_overview = CachedClusterOverview {
        cluster_id: "streamline".to_string(),
        controller_id: Some(1),
        broker_count: 1,
        topic_count,
        partition_count,
        online_partition_count: partition_count,
        offline_partition_count: 0,
        under_replicated_partitions: 0,
        total_messages,
        total_bytes,
    };
    state
        .metadata_cache
        .set_cluster_overview(cached_overview.clone());

    let overview = ClusterOverview {
        cluster_id: cached_overview.cluster_id,
        controller_id: cached_overview.controller_id,
        broker_count: cached_overview.broker_count,
        topic_count: cached_overview.topic_count,
        partition_count: cached_overview.partition_count,
        online_partition_count: cached_overview.online_partition_count,
        offline_partition_count: cached_overview.offline_partition_count,
        under_replicated_partitions: cached_overview.under_replicated_partitions,
        messages_per_second,
        bytes_in_per_second,
        bytes_out_per_second,
        total_messages: cached_overview.total_messages,
        total_bytes: cached_overview.total_bytes,
    };

    (StatusCode::OK, Json(overview)).into_response()
}

/// GET /api/v1/brokers - List all brokers
async fn list_brokers_handler(State(state): State<ClusterApiState>) -> Response {
    // Single-node cluster returns one broker
    let broker = BrokerInfo {
        node_id: 1,
        host: state.config.listen_addr.ip().to_string(),
        port: state.config.listen_addr.port(),
        rack: None,
        is_controller: true,
        state: "online".to_string(),
    };

    (StatusCode::OK, Json(vec![broker])).into_response()
}

/// GET /api/v1/metrics/history - Get historical metrics
async fn metrics_history_handler(
    State(state): State<ClusterApiState>,
    Query(query): Query<MetricsHistoryQuery>,
) -> Response {
    let history = match &state.metrics_history {
        Some(h) => h,
        None => {
            // Return empty history if metrics collection is not enabled
            return (
                StatusCode::OK,
                Json(HistoricalMetrics {
                    points: vec![],
                    start_time: 0,
                    end_time: 0,
                    interval_ms: 5000,
                }),
            )
                .into_response();
        }
    };

    // Parse duration string (e.g., "1h", "6h", "24h")
    let duration = parse_duration(&query.duration).unwrap_or(Duration::from_secs(3600));

    let metrics = history.get_history(duration);

    (StatusCode::OK, Json(metrics)).into_response()
}

/// Parse duration string like "1h", "6h", "24h", "30m"
fn parse_duration(s: &str) -> Option<Duration> {
    let s = s.trim().to_lowercase();

    if s.is_empty() {
        return None;
    }

    let (num_str, unit) = if s.ends_with('h') {
        (s.trim_end_matches('h'), "h")
    } else if s.ends_with('m') {
        (s.trim_end_matches('m'), "m")
    } else if s.ends_with('s') {
        (s.trim_end_matches('s'), "s")
    } else {
        // Assume hours if no unit
        (s.as_str(), "h")
    };

    let num: u64 = num_str.parse().ok()?;

    Some(match unit {
        "h" => Duration::from_secs(num * 3600),
        "m" => Duration::from_secs(num * 60),
        "s" => Duration::from_secs(num),
        _ => Duration::from_secs(num * 3600),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::metadata_cache::MetadataCacheConfig;
    use tempfile::TempDir;

    fn create_test_state() -> (ClusterApiState, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let config = ServerConfig {
            listen_addr: "127.0.0.1:9092".parse().unwrap(),
            http_addr: "127.0.0.1:9094".parse().unwrap(),
            data_dir: temp_dir.path().to_path_buf(),
            log_level: "info".to_string(),
            storage: crate::config::StorageConfig::default(),
            tls: crate::config::TlsConfig::default(),
            tcp: crate::config::TcpConfig::default(),
            #[cfg(feature = "auth")]
            auth: crate::config::AuthConfig::default(),
            #[cfg(feature = "auth")]
            acl: crate::config::AclConfig::default(),
            #[cfg(feature = "auth")]
            audit: crate::audit::AuditConfig::default(),
            limits: crate::server::limits::LimitsConfig::default(),
            shutdown: crate::server::shutdown::ShutdownConfig::default(),
            quotas: crate::server::limits::QuotaConfig::default(),
            #[cfg(feature = "clustering")]
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
        let metadata_cache = MetadataCache::new_shared(MetadataCacheConfig::default());

        let state = ClusterApiState {
            topic_manager,
            config,
            start_time: Instant::now(),
            metadata_cache,
            metrics_history: None,
        };

        (state, temp_dir)
    }

    #[tokio::test]
    async fn test_cluster_overview_handler() {
        let (state, _temp_dir) = create_test_state();
        let response = cluster_overview_handler(State(state)).await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_list_brokers_handler() {
        let (state, _temp_dir) = create_test_state();
        let response = list_brokers_handler(State(state)).await;
        assert_eq!(response.status(), StatusCode::OK);
    }
}
