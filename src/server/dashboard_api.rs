//! HTTP REST API for Web Dashboard
//!
//! This module provides comprehensive REST endpoints for the Streamline web dashboard:
//!
//! ## Endpoints
//!
//! ### Dashboard Overview
//! - `GET /api/v1/dashboard` - Get dashboard overview with all key metrics
//! - `GET /api/v1/dashboard/health` - Get system health status with component breakdown
//!
//! ### Real-Time Metrics
//! - `GET /api/v1/dashboard/metrics/realtime` - Get real-time streaming metrics
//! - `GET /api/v1/dashboard/metrics/throughput` - Get throughput metrics by topic
//!
//! ### Topic Browser
//! - `GET /api/v1/dashboard/topics/summary` - Get topic summary with key statistics
//! - `GET /api/v1/dashboard/topics/:topic/activity` - Get topic activity metrics
//!
//! ### System Statistics
//! - `GET /api/v1/dashboard/system` - Get system resource usage
//! - `GET /api/v1/dashboard/alerts` - Get active alerts and warnings
//!
//! # Stability
//!
//! **⚠️ Experimental** - This module is under active development.

use crate::config::ServerConfig;
use crate::metrics::MetricsHistory;
use crate::server::metadata_cache::MetadataCache;
use crate::storage::TopicManager;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tracing::error;

/// Shared state for the dashboard API
#[derive(Clone)]
pub struct DashboardApiState {
    /// Topic manager for storage operations
    pub topic_manager: Arc<TopicManager>,
    /// Server configuration
    pub config: ServerConfig,
    /// Server start time
    pub start_time: Instant,
    /// Metadata cache for performance optimization
    pub metadata_cache: Arc<MetadataCache>,
    /// Historical metrics storage (optional)
    pub metrics_history: Option<Arc<MetricsHistory>>,
    /// Alert store for integrated alert monitoring
    pub alert_store: Option<Arc<crate::server::alerts::AlertStore>>,
}

// ============================================================================
// Response Types
// ============================================================================

/// Dashboard overview response with all key metrics
#[derive(Debug, Serialize)]
pub struct DashboardOverview {
    /// Cluster identifier
    pub cluster_id: String,
    /// Server version
    pub version: String,
    /// Server uptime in seconds
    pub uptime_seconds: f64,
    /// Overall system health status
    pub health_status: HealthStatusSummary,
    /// Quick stats summary
    pub stats: QuickStats,
    /// Active alerts count by severity
    pub alerts: AlertsSummary,
    /// Timestamp of this snapshot
    pub timestamp: u64,
}

/// Quick statistics summary
#[derive(Debug, Serialize)]
pub struct QuickStats {
    /// Total number of topics
    pub topics: usize,
    /// Total number of partitions
    pub partitions: usize,
    /// Total messages across all topics
    pub total_messages: u64,
    /// Messages per second (current rate)
    pub messages_per_second: f64,
    /// Bytes in per second
    pub bytes_in_per_second: f64,
    /// Bytes out per second
    pub bytes_out_per_second: f64,
    /// Number of active connections
    pub active_connections: usize,
    /// Number of consumer groups
    pub consumer_groups: usize,
}

/// Health status summary
#[derive(Debug, Serialize)]
pub struct HealthStatusSummary {
    /// Overall status: "healthy", "degraded", "unhealthy"
    pub status: String,
    /// Component health statuses
    pub components: Vec<ComponentHealth>,
}

/// Individual component health
#[derive(Debug, Serialize)]
pub struct ComponentHealth {
    /// Component name
    pub name: String,
    /// Status: "ok", "warning", "error"
    pub status: String,
    /// Optional message
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// Last check timestamp
    pub last_check: u64,
}

/// Alerts summary by severity
#[derive(Debug, Serialize, Default)]
pub struct AlertsSummary {
    /// Critical alerts count
    pub critical: usize,
    /// Warning alerts count
    pub warning: usize,
    /// Info alerts count
    pub info: usize,
}

/// System health response with detailed component breakdown
#[derive(Debug, Serialize)]
pub struct SystemHealth {
    /// Overall health status
    pub status: String,
    /// Detailed component health
    pub components: Vec<DetailedComponentHealth>,
    /// System checks performed
    pub checks: Vec<HealthCheckResult>,
    /// Timestamp
    pub timestamp: u64,
}

/// Detailed component health information
#[derive(Debug, Serialize)]
pub struct DetailedComponentHealth {
    /// Component name
    pub name: String,
    /// Component type (storage, network, memory, etc.)
    pub component_type: String,
    /// Status: "ok", "warning", "error"
    pub status: String,
    /// Detailed metrics for this component
    pub metrics: HashMap<String, serde_json::Value>,
    /// Last update timestamp
    pub last_updated: u64,
}

/// Health check result
#[derive(Debug, Serialize)]
pub struct HealthCheckResult {
    /// Check name
    pub name: String,
    /// Check passed
    pub passed: bool,
    /// Duration in milliseconds
    pub duration_ms: f64,
    /// Optional error message
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Real-time metrics response
#[derive(Debug, Serialize)]
pub struct RealtimeMetrics {
    /// Current timestamp
    pub timestamp: u64,
    /// Messages per second (last 5 seconds)
    pub messages_per_second: f64,
    /// Bytes in per second
    pub bytes_in_per_second: f64,
    /// Bytes out per second
    pub bytes_out_per_second: f64,
    /// Active connections
    pub connections: usize,
    /// Recent metrics points (for sparkline charts)
    pub sparkline: Vec<SparklinePoint>,
}

/// Sparkline data point
#[derive(Debug, Serialize)]
pub struct SparklinePoint {
    /// Timestamp
    pub timestamp: u64,
    /// Value
    pub value: f64,
}

/// Topic throughput metrics
#[derive(Debug, Serialize)]
pub struct ThroughputMetrics {
    /// Per-topic throughput
    pub topics: Vec<TopicThroughput>,
    /// Aggregate throughput
    pub aggregate: AggregateThroughput,
    /// Timestamp
    pub timestamp: u64,
}

/// Per-topic throughput
#[derive(Debug, Serialize)]
pub struct TopicThroughput {
    /// Topic name
    pub name: String,
    /// Messages per second
    pub messages_per_second: f64,
    /// Bytes per second
    pub bytes_per_second: f64,
    /// Total messages
    pub total_messages: u64,
}

/// Aggregate throughput
#[derive(Debug, Serialize)]
pub struct AggregateThroughput {
    /// Total messages per second
    pub messages_per_second: f64,
    /// Total bytes in per second
    pub bytes_in_per_second: f64,
    /// Total bytes out per second
    pub bytes_out_per_second: f64,
}

/// Topic summary for dashboard
#[derive(Debug, Serialize)]
pub struct TopicsSummary {
    /// List of topics with summary stats
    pub topics: Vec<TopicSummaryItem>,
    /// Total topic count
    pub total_count: usize,
    /// Total partition count
    pub total_partitions: usize,
    /// Total message count
    pub total_messages: u64,
}

/// Summary item for a single topic
#[derive(Debug, Serialize)]
pub struct TopicSummaryItem {
    /// Topic name
    pub name: String,
    /// Number of partitions
    pub partitions: i32,
    /// Replication factor
    pub replication_factor: i32,
    /// Total messages
    pub messages: u64,
    /// Size in bytes
    pub size_bytes: u64,
    /// Messages per second (estimated)
    pub messages_per_second: f64,
    /// Health status
    pub health: String,
}

/// Topic activity metrics
#[derive(Debug, Serialize)]
pub struct TopicActivity {
    /// Topic name
    pub topic: String,
    /// Partition activities
    pub partitions: Vec<PartitionActivity>,
    /// Overall activity metrics
    pub activity: ActivityMetrics,
    /// Recent activity timeline
    pub timeline: Vec<ActivityPoint>,
}

/// Per-partition activity
#[derive(Debug, Serialize)]
pub struct PartitionActivity {
    /// Partition ID
    pub partition_id: i32,
    /// Start offset
    pub start_offset: i64,
    /// End offset
    pub end_offset: i64,
    /// Message count
    pub message_count: u64,
    /// Is leader
    pub is_leader: bool,
}

/// Activity metrics
#[derive(Debug, Serialize)]
pub struct ActivityMetrics {
    /// Messages in last minute
    pub messages_1m: u64,
    /// Messages in last 5 minutes
    pub messages_5m: u64,
    /// Messages in last hour
    pub messages_1h: u64,
    /// Peak messages per second
    pub peak_mps: f64,
}

/// Activity timeline point
#[derive(Debug, Serialize)]
pub struct ActivityPoint {
    /// Timestamp
    pub timestamp: u64,
    /// Message count
    pub messages: u64,
}

/// System resource usage
#[derive(Debug, Serialize)]
pub struct SystemStats {
    /// Memory usage
    pub memory: MemoryStats,
    /// Storage usage
    pub storage: StorageStats,
    /// CPU metrics (if available)
    pub cpu: Option<CpuStats>,
    /// Network metrics
    pub network: NetworkStats,
    /// Timestamp
    pub timestamp: u64,
}

/// Memory statistics
#[derive(Debug, Serialize)]
pub struct MemoryStats {
    /// Used memory in bytes
    pub used_bytes: u64,
    /// Total memory in bytes
    pub total_bytes: u64,
    /// Usage percentage
    pub usage_percent: f64,
}

/// Storage statistics
#[derive(Debug, Serialize)]
pub struct StorageStats {
    /// Used storage in bytes
    pub used_bytes: u64,
    /// Total available in bytes
    pub total_bytes: u64,
    /// Usage percentage
    pub usage_percent: f64,
    /// Data directory path
    pub data_dir: String,
}

/// CPU statistics
#[derive(Debug, Serialize)]
pub struct CpuStats {
    /// Usage percentage
    pub usage_percent: f64,
    /// Number of cores
    pub cores: usize,
}

/// Network statistics
#[derive(Debug, Serialize)]
pub struct NetworkStats {
    /// Bytes received
    pub bytes_received: u64,
    /// Bytes sent
    pub bytes_sent: u64,
    /// Active connections
    pub connections: usize,
}

/// Active alerts response
#[derive(Debug, Serialize)]
pub struct ActiveAlerts {
    /// List of active alerts
    pub alerts: Vec<Alert>,
    /// Total count
    pub total: usize,
    /// Timestamp
    pub timestamp: u64,
}

/// Individual alert
#[derive(Debug, Serialize)]
pub struct Alert {
    /// Alert ID
    pub id: String,
    /// Severity: "critical", "warning", "info"
    pub severity: String,
    /// Alert title
    pub title: String,
    /// Alert message
    pub message: String,
    /// Source component
    pub source: String,
    /// When the alert was triggered
    pub triggered_at: u64,
    /// Whether the alert is acknowledged
    pub acknowledged: bool,
}

/// Query parameters for topic activity
#[derive(Debug, Deserialize)]
pub struct ActivityQuery {
    /// Duration for timeline (default: "1h")
    #[serde(default = "default_activity_duration")]
    pub duration: String,
}

fn default_activity_duration() -> String {
    "1h".to_string()
}

// ============================================================================
// Router Creation
// ============================================================================

/// Create the dashboard API router
pub fn create_dashboard_api_router(state: DashboardApiState) -> Router {
    Router::new()
        // Dashboard overview
        .route("/api/v1/dashboard", get(dashboard_overview_handler))
        .route("/api/v1/dashboard/health", get(system_health_handler))
        // Real-time metrics
        .route(
            "/api/v1/dashboard/metrics/realtime",
            get(realtime_metrics_handler),
        )
        .route(
            "/api/v1/dashboard/metrics/throughput",
            get(throughput_metrics_handler),
        )
        // Topic browser
        .route(
            "/api/v1/dashboard/topics/summary",
            get(topics_summary_handler),
        )
        .route(
            "/api/v1/dashboard/topics/:topic/activity",
            get(topic_activity_handler),
        )
        // System statistics
        .route("/api/v1/dashboard/system", get(system_stats_handler))
        .route("/api/v1/dashboard/alerts", get(active_alerts_handler))
        .with_state(state)
}

// ============================================================================
// Handler Implementations
// ============================================================================

/// GET /api/v1/dashboard - Get dashboard overview
async fn dashboard_overview_handler(State(state): State<DashboardApiState>) -> Response {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    let uptime = state.start_time.elapsed().as_secs_f64();

    // Get topic stats
    let topic_stats = match state.topic_manager.get_all_topic_stats() {
        Ok(stats) => stats,
        Err(e) => {
            error!(error = %e, "Failed to fetch topic stats for dashboard overview");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": format!("Failed to get topic stats: {}", e)})),
            )
                .into_response();
        }
    };

    let mut total_partitions = 0usize;
    let mut total_messages = 0u64;

    for stats in &topic_stats {
        total_partitions += stats.num_partitions as usize;
        total_messages += stats.total_messages;
    }

    // Get metrics from history if available
    let (messages_per_second, bytes_in_per_second, bytes_out_per_second, connections) =
        if let Some(ref history) = state.metrics_history {
            let points = history.get_all();
            if let Some(latest) = points.last() {
                (
                    latest.messages_per_sec,
                    latest.bytes_in_per_sec,
                    latest.bytes_out_per_sec,
                    latest.connections,
                )
            } else {
                (0.0, 0.0, 0.0, 0)
            }
        } else {
            (0.0, 0.0, 0.0, 0)
        };

    // Build health status
    let components = build_component_health(&state);
    let overall_status = if components.iter().all(|c| c.status == "ok") {
        "healthy"
    } else if components.iter().any(|c| c.status == "error") {
        "unhealthy"
    } else {
        "degraded"
    };

    let overview = DashboardOverview {
        cluster_id: "streamline".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        uptime_seconds: uptime,
        health_status: HealthStatusSummary {
            status: overall_status.to_string(),
            components,
        },
        stats: QuickStats {
            topics: topic_stats.len(),
            partitions: total_partitions,
            total_messages,
            messages_per_second,
            bytes_in_per_second,
            bytes_out_per_second,
            active_connections: connections,
            consumer_groups: 0, // Would need group coordinator access
        },
        alerts: build_alerts_summary(&state),
        timestamp: now,
    };

    (StatusCode::OK, Json(overview)).into_response()
}

/// GET /api/v1/dashboard/health - Get system health
async fn system_health_handler(State(state): State<DashboardApiState>) -> Response {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    let check_start = Instant::now();

    // Perform health checks
    let mut checks = Vec::new();

    // Storage check
    let storage_start = Instant::now();
    let storage_result = state.topic_manager.list_topics();
    let storage_duration = storage_start.elapsed().as_secs_f64() * 1000.0;
    checks.push(HealthCheckResult {
        name: "storage".to_string(),
        passed: storage_result.is_ok(),
        duration_ms: storage_duration,
        error: storage_result.err().map(|e| e.to_string()),
    });

    // Data directory check
    let data_dir_exists = state.config.data_dir.exists();
    checks.push(HealthCheckResult {
        name: "data_directory".to_string(),
        passed: data_dir_exists,
        duration_ms: 0.1,
        error: if data_dir_exists {
            None
        } else {
            Some("Data directory not accessible".to_string())
        },
    });

    // Build detailed component health
    let mut components = Vec::new();

    // Storage component
    let storage_metrics = {
        let mut m = HashMap::new();
        let (topics_count, topics_error) = match state.topic_manager.list_topics() {
            Ok(topics) => (topics.len(), None),
            Err(e) => {
                error!(error = %e, "Failed to list topics for system health");
                (0, Some(e.to_string()))
            }
        };
        m.insert("topics".to_string(), serde_json::json!(topics_count));
        if let Some(err) = topics_error {
            m.insert("topics_error".to_string(), serde_json::json!(err));
        }
        m.insert(
            "data_dir".to_string(),
            serde_json::json!(state.config.data_dir.display().to_string()),
        );
        m
    };

    components.push(DetailedComponentHealth {
        name: "storage".to_string(),
        component_type: "storage".to_string(),
        status: if checks
            .iter()
            .find(|c| c.name == "storage")
            .map(|c| c.passed)
            .unwrap_or(false)
        {
            "ok"
        } else {
            "error"
        }
        .to_string(),
        metrics: storage_metrics,
        last_updated: now,
    });

    // Network component
    let network_metrics = {
        let mut m = HashMap::new();
        m.insert(
            "listen_addr".to_string(),
            serde_json::json!(state.config.listen_addr.to_string()),
        );
        m.insert(
            "http_addr".to_string(),
            serde_json::json!(state.config.http_addr.to_string()),
        );
        m
    };

    components.push(DetailedComponentHealth {
        name: "network".to_string(),
        component_type: "network".to_string(),
        status: "ok".to_string(),
        metrics: network_metrics,
        last_updated: now,
    });

    let overall_status = if checks.iter().all(|c| c.passed) {
        "healthy"
    } else if checks.iter().any(|c| !c.passed) {
        "unhealthy"
    } else {
        "degraded"
    };

    let _total_duration = check_start.elapsed().as_secs_f64() * 1000.0;

    let health = SystemHealth {
        status: overall_status.to_string(),
        components,
        checks,
        timestamp: now,
    };

    (StatusCode::OK, Json(health)).into_response()
}

/// GET /api/v1/dashboard/metrics/realtime - Get real-time metrics
async fn realtime_metrics_handler(State(state): State<DashboardApiState>) -> Response {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    let (messages_per_second, bytes_in_per_second, bytes_out_per_second, connections, sparkline) =
        if let Some(ref history) = state.metrics_history {
            let points = history.get_all();
            let latest = points.last();

            let (mps, bi, bo, conn) = if let Some(p) = latest {
                (
                    p.messages_per_sec,
                    p.bytes_in_per_sec,
                    p.bytes_out_per_sec,
                    p.connections,
                )
            } else {
                (0.0, 0.0, 0.0, 0)
            };

            // Get last 60 points for sparkline (5 minutes at 5-second intervals)
            let sparkline: Vec<SparklinePoint> = points
                .iter()
                .rev()
                .take(60)
                .rev()
                .map(|p| SparklinePoint {
                    timestamp: p.timestamp,
                    value: p.messages_per_sec,
                })
                .collect();

            (mps, bi, bo, conn, sparkline)
        } else {
            (0.0, 0.0, 0.0, 0, Vec::new())
        };

    let metrics = RealtimeMetrics {
        timestamp: now,
        messages_per_second,
        bytes_in_per_second,
        bytes_out_per_second,
        connections,
        sparkline,
    };

    (StatusCode::OK, Json(metrics)).into_response()
}

/// GET /api/v1/dashboard/metrics/throughput - Get throughput metrics
async fn throughput_metrics_handler(State(state): State<DashboardApiState>) -> Response {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    let topic_stats = match state.topic_manager.get_all_topic_stats() {
        Ok(stats) => stats,
        Err(e) => {
            error!(error = %e, "Failed to fetch topic stats for throughput metrics");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": format!("Failed to get topic stats: {}", e)})),
            )
                .into_response();
        }
    };

    let topics: Vec<TopicThroughput> = topic_stats
        .iter()
        .map(|stats| TopicThroughput {
            name: stats.name.clone(),
            messages_per_second: 0.0, // Would need per-topic rate tracking
            bytes_per_second: 0.0,
            total_messages: stats.total_messages,
        })
        .collect();

    // Get aggregate from metrics history
    let (agg_mps, agg_bi, agg_bo) = if let Some(ref history) = state.metrics_history {
        let points = history.get_all();
        if let Some(p) = points.last() {
            (p.messages_per_sec, p.bytes_in_per_sec, p.bytes_out_per_sec)
        } else {
            (0.0, 0.0, 0.0)
        }
    } else {
        (0.0, 0.0, 0.0)
    };

    let throughput = ThroughputMetrics {
        topics,
        aggregate: AggregateThroughput {
            messages_per_second: agg_mps,
            bytes_in_per_second: agg_bi,
            bytes_out_per_second: agg_bo,
        },
        timestamp: now,
    };

    (StatusCode::OK, Json(throughput)).into_response()
}

/// GET /api/v1/dashboard/topics/summary - Get topics summary
async fn topics_summary_handler(State(state): State<DashboardApiState>) -> Response {
    let topic_stats = match state.topic_manager.get_all_topic_stats() {
        Ok(stats) => stats,
        Err(e) => {
            error!(error = %e, "Failed to fetch topic stats for topics summary");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": format!("Failed to get topic stats: {}", e)})),
            )
                .into_response();
        }
    };

    let mut total_partitions = 0i32;
    let mut total_messages = 0u64;

    let topics: Vec<TopicSummaryItem> = topic_stats
        .iter()
        .map(|stats| {
            total_partitions += stats.num_partitions;
            total_messages += stats.total_messages;

            TopicSummaryItem {
                name: stats.name.clone(),
                partitions: stats.num_partitions,
                replication_factor: stats.replication_factor as i32,
                messages: stats.total_messages,
                size_bytes: 0, // Would need byte tracking
                messages_per_second: 0.0,
                health: "healthy".to_string(),
            }
        })
        .collect();

    let summary = TopicsSummary {
        total_count: topics.len(),
        total_partitions: total_partitions as usize,
        total_messages,
        topics,
    };

    (StatusCode::OK, Json(summary)).into_response()
}

/// GET /api/v1/dashboard/topics/:topic/activity - Get topic activity
async fn topic_activity_handler(
    State(state): State<DashboardApiState>,
    Path(topic): Path<String>,
    Query(_query): Query<ActivityQuery>,
) -> Response {
    // Get topic metadata
    let _metadata = match state.topic_manager.get_topic_metadata(&topic) {
        Ok(m) => m,
        Err(_) => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": "Topic not found"})),
            )
                .into_response()
        }
    };

    // Build partition activities
    let mut partitions = Vec::new();
    let mut total_messages = 0u64;

    let partition_infos = match state.topic_manager.all_partition_info(&topic) {
        Ok(infos) => infos,
        Err(e) => {
            error!(error = %e, topic = %topic, "Failed to fetch partition info for activity");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": format!("Failed to get partition info: {}", e)})),
            )
                .into_response();
        }
    };

    for info in partition_infos {
        let start_offset = info.earliest_offset;
        let end_offset = info.latest_offset;
        let message_count = if end_offset > start_offset {
            (end_offset - start_offset) as u64
        } else {
            0
        };
        total_messages += message_count;

        partitions.push(PartitionActivity {
            partition_id: info.id,
            start_offset,
            end_offset,
            message_count,
            is_leader: true, // Single node is always leader
        });
    }

    let activity = TopicActivity {
        topic: topic.clone(),
        partitions,
        activity: ActivityMetrics {
            messages_1m: 0, // Would need rate tracking
            messages_5m: 0,
            messages_1h: total_messages,
            peak_mps: 0.0,
        },
        timeline: Vec::new(), // Would need historical data
    };

    (StatusCode::OK, Json(activity)).into_response()
}

/// GET /api/v1/dashboard/system - Get system statistics
async fn system_stats_handler(State(state): State<DashboardApiState>) -> Response {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    // Get connections from metrics history
    let (connections, bytes_received, bytes_sent) = if let Some(ref history) = state.metrics_history
    {
        let points = history.get_all();
        if let Some(p) = points.last() {
            let total_bytes_in: u64 = points
                .iter()
                .map(|pt| (pt.bytes_in_per_sec * 5.0) as u64)
                .sum();
            let total_bytes_out: u64 = points
                .iter()
                .map(|pt| (pt.bytes_out_per_sec * 5.0) as u64)
                .sum();
            (p.connections, total_bytes_in, total_bytes_out)
        } else {
            (0, 0, 0)
        }
    } else {
        (0, 0, 0)
    };

    // Get process memory usage (portable across platforms)
    let process_memory = get_process_memory_bytes();

    // Get data dir disk usage
    let (storage_used, storage_total) = get_disk_usage(&state.config.data_dir);
    let storage_percent = if storage_total > 0 {
        (storage_used as f64 / storage_total as f64) * 100.0
    } else {
        0.0
    };

    let stats = SystemStats {
        memory: MemoryStats {
            used_bytes: process_memory,
            total_bytes: 0, // Not easily available without sysinfo crate
            usage_percent: 0.0,
        },
        storage: StorageStats {
            used_bytes: storage_used,
            total_bytes: storage_total,
            usage_percent: storage_percent,
            data_dir: state.config.data_dir.display().to_string(),
        },
        cpu: None,
        network: NetworkStats {
            bytes_received,
            bytes_sent,
            connections,
        },
        timestamp: now,
    };

    (StatusCode::OK, Json(stats)).into_response()
}

/// GET /api/v1/dashboard/alerts - Get active alerts
async fn active_alerts_handler(State(state): State<DashboardApiState>) -> Response {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    let alerts = if let Some(ref alert_store) = state.alert_store {
        let recent_events = alert_store.get_history(50);
        let alert_items: Vec<Alert> = recent_events
            .into_iter()
            .map(|event| Alert {
                id: event.id,
                severity: match event.severity {
                    crate::server::alerts::AlertSeverity::Critical => "critical".to_string(),
                    crate::server::alerts::AlertSeverity::Warning => "warning".to_string(),
                },
                title: event.alert_name.clone(),
                message: format!(
                    "{}: actual={:.2}, threshold={:.2}",
                    event.condition, event.actual_value, event.threshold
                ),
                source: "alert-evaluator".to_string(),
                triggered_at: event.timestamp,
                acknowledged: false,
            })
            .collect();
        let total = alert_items.len();
        ActiveAlerts {
            alerts: alert_items,
            total,
            timestamp: now,
        }
    } else {
        ActiveAlerts {
            alerts: Vec::new(),
            total: 0,
            timestamp: now,
        }
    };

    (StatusCode::OK, Json(alerts)).into_response()
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Get approximate process memory usage in bytes
fn get_process_memory_bytes() -> u64 {
    // Try reading from /proc/self/statm on Linux
    #[cfg(target_os = "linux")]
    {
        if let Ok(statm) = std::fs::read_to_string("/proc/self/statm") {
            if let Some(rss_pages) = statm.split_whitespace().nth(1) {
                if let Ok(pages) = rss_pages.parse::<u64>() {
                    return pages * 4096; // page size is typically 4KB
                }
            }
        }
    }

    // On macOS, use mach API via rusage
    #[cfg(target_os = "macos")]
    {
        // Use getrusage which works on macOS
        // ru_maxrss is in bytes on macOS
        unsafe {
            let mut rusage: libc::rusage = std::mem::zeroed();
            if libc::getrusage(libc::RUSAGE_SELF, &mut rusage) == 0 {
                return rusage.ru_maxrss as u64;
            }
        }
    }

    0
}

/// Get disk usage for a directory (used_bytes, total_bytes)
fn get_disk_usage(path: &std::path::Path) -> (u64, u64) {
    // Calculate used bytes by walking the directory
    let used = calculate_dir_size(path);

    // Try to get filesystem total via statvfs
    #[cfg(unix)]
    {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let path_cstr = CString::new(path.as_os_str().as_bytes()).ok();
        if let Some(cstr) = path_cstr {
            unsafe {
                let mut stat: libc::statvfs = std::mem::zeroed();
                if libc::statvfs(cstr.as_ptr(), &mut stat) == 0 {
                    let total = stat.f_blocks as u64 * stat.f_frsize as u64;
                    return (used, total);
                }
            }
        }
    }

    (used, 0)
}

/// Calculate total size of files in a directory recursively
fn calculate_dir_size(path: &std::path::Path) -> u64 {
    let mut total = 0u64;
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            let entry_path = entry.path();
            if entry_path.is_dir() {
                total += calculate_dir_size(&entry_path);
            } else if let Ok(metadata) = entry.metadata() {
                total += metadata.len();
            }
        }
    }
    total
}

/// Build component health statuses
fn build_component_health(state: &DashboardApiState) -> Vec<ComponentHealth> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    let mut components = Vec::new();

    // Storage health
    let storage_ok = state.topic_manager.list_topics().is_ok();
    components.push(ComponentHealth {
        name: "storage".to_string(),
        status: if storage_ok { "ok" } else { "error" }.to_string(),
        message: if storage_ok {
            None
        } else {
            Some("Storage unavailable".to_string())
        },
        last_check: now,
    });

    // Data directory health
    let data_dir_ok = state.config.data_dir.exists();
    components.push(ComponentHealth {
        name: "data_directory".to_string(),
        status: if data_dir_ok { "ok" } else { "error" }.to_string(),
        message: if data_dir_ok {
            None
        } else {
            Some("Data directory not accessible".to_string())
        },
        last_check: now,
    });

    // Network health (always ok if we're responding)
    components.push(ComponentHealth {
        name: "network".to_string(),
        status: "ok".to_string(),
        message: None,
        last_check: now,
    });

    components
}

/// Build alerts summary from alert store
fn build_alerts_summary(state: &DashboardApiState) -> AlertsSummary {
    if let Some(ref alert_store) = state.alert_store {
        let recent = alert_store.get_history(100);
        let mut summary = AlertsSummary::default();
        for event in &recent {
            match event.severity {
                crate::server::alerts::AlertSeverity::Critical => summary.critical += 1,
                crate::server::alerts::AlertSeverity::Warning => summary.warning += 1,
            }
        }
        summary
    } else {
        AlertsSummary::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::metadata_cache::MetadataCacheConfig;
    use tempfile::TempDir;

    fn create_test_state() -> (DashboardApiState, TempDir) {
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

        let state = DashboardApiState {
            topic_manager,
            config,
            start_time: Instant::now(),
            metadata_cache,
            metrics_history: None,
            alert_store: Some(Arc::new(crate::server::alerts::AlertStore::new())),
        };

        (state, temp_dir)
    }

    #[tokio::test]
    async fn test_dashboard_overview_handler() {
        let (state, _temp_dir) = create_test_state();
        let response = dashboard_overview_handler(State(state)).await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_system_health_handler() {
        let (state, _temp_dir) = create_test_state();
        let response = system_health_handler(State(state)).await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_realtime_metrics_handler() {
        let (state, _temp_dir) = create_test_state();
        let response = realtime_metrics_handler(State(state)).await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_topics_summary_handler() {
        let (state, _temp_dir) = create_test_state();
        let response = topics_summary_handler(State(state)).await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_system_stats_handler() {
        let (state, _temp_dir) = create_test_state();
        let response = system_stats_handler(State(state)).await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_active_alerts_handler() {
        let (state, _temp_dir) = create_test_state();
        let response = active_alerts_handler(State(state)).await;
        assert_eq!(response.status(), StatusCode::OK);
    }
}
