//! Edge Deployment HTTP API
//!
//! REST endpoints for managing edge deployment features:
//!
//! - `GET /api/v1/edge/status` - Get edge sync status and overview
//! - `POST /api/v1/edge/sync` - Trigger a manual sync
//! - `GET /api/v1/edge/sync/status` - Get sync progress with watermarks
//! - `GET /api/v1/edge/conflicts` - List unresolved/recent conflicts
//! - `GET /api/v1/edge/optimizations` - Get optimization statistics

use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Edge status response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeStatusResponse {
    /// Whether edge mode is active
    pub edge_mode: bool,
    /// Edge node identifier
    pub edge_id: String,
    /// Current sync state ("idle", "syncing", "waiting", "completed", "failed", "paused")
    pub sync_state: String,
    /// Cloud endpoint (if configured)
    pub cloud_endpoint: Option<String>,
    /// Last successful sync timestamp (ms since epoch)
    pub last_sync_at: Option<i64>,
    /// Number of pending records to sync
    pub pending_records: u64,
    /// Number of unresolved conflicts
    pub conflict_count: u64,
    /// Total syncs completed
    pub total_syncs: u64,
    /// Total records synced
    pub total_records_synced: u64,
    /// Whether cloud is reachable
    pub cloud_reachable: bool,
    /// Connection status: "connected" or "disconnected"
    pub connection_status: String,
    /// Sync lag in records across all partitions
    pub sync_lag_records: u64,
}

/// Sync trigger request
#[derive(Debug, Clone, Deserialize)]
pub struct SyncTriggerRequest {
    /// Direction: "bidirectional", "edge_to_cloud", "cloud_to_edge"
    pub direction: Option<String>,
}

/// Sync trigger response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncTriggerResponse {
    /// Whether the sync was initiated successfully
    pub initiated: bool,
    /// Message
    pub message: String,
    /// Current sync state after trigger
    pub sync_state: String,
}

/// Sync progress response (GET /api/v1/edge/sync/status)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncProgressResponse {
    /// Overall sync state
    pub sync_state: String,
    /// Total records pending sync
    pub total_pending_records: u64,
    /// Total records forwarded to cloud
    pub total_forwarded_records: u64,
    /// Total failed upload attempts
    pub total_failed_uploads: u64,
    /// Number of upload batches completed
    pub batches_completed: u64,
    /// Sync lag (max pending offset delta)
    pub sync_lag_records: u64,
    /// Last sync attempt timestamp
    pub last_sync_attempt_at: Option<i64>,
    /// Last successful sync timestamp
    pub last_sync_success_at: Option<i64>,
    /// Sync strategy in use
    pub sync_strategy: String,
    /// Per-topic/partition watermarks
    pub watermarks: Vec<WatermarkEntry>,
}

/// Watermark entry for API response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatermarkEntry {
    /// Topic name
    pub topic: String,
    /// Partition id
    pub partition: i32,
    /// Last offset synced to cloud
    pub last_synced_offset: i64,
    /// Latest local offset
    pub latest_local_offset: i64,
    /// Records pending sync
    pub pending: u64,
    /// Last sync timestamp
    pub last_synced_at: i64,
}

/// Conflict entry in API response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConflictEntry {
    /// Topic where conflict occurred
    pub topic: String,
    /// Partition
    pub partition: i32,
    /// Edge record offset
    pub edge_offset: i64,
    /// Cloud record offset
    pub cloud_offset: i64,
    /// Edge record timestamp
    pub edge_timestamp: i64,
    /// Cloud record timestamp
    pub cloud_timestamp: i64,
    /// Key (hex-encoded, if present)
    pub key: Option<String>,
    /// Resolution applied
    pub resolution: String,
    /// When the conflict was detected
    pub detected_at: i64,
}

/// Conflicts list response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConflictsResponse {
    /// Total conflicts detected
    pub total_conflicts: u64,
    /// Conflicts resolved by edge winning
    pub edge_wins: u64,
    /// Conflicts resolved by cloud winning
    pub cloud_wins: u64,
    /// Unresolved conflicts
    pub unresolved: u64,
    /// Recent conflict entries
    pub conflicts: Vec<ConflictEntry>,
}

/// Optimization stats response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptimizationsResponse {
    /// Whether reduced memory mode is active
    pub reduced_memory_active: bool,
    /// Whether embedded mode is active
    pub embedded_mode_active: bool,
    /// Total eviction cycles
    pub eviction_cycles: u64,
    /// Total records evicted
    pub total_records_evicted: u64,
    /// Total compaction cycles
    pub compaction_cycles: u64,
    /// Current storage usage in bytes
    pub current_storage_bytes: u64,
    /// Max storage configured
    pub max_storage_bytes: u64,
    /// Storage utilization (0.0–1.0)
    pub storage_utilization: f64,
}

/// Shared state for Edge API
#[derive(Clone)]
pub struct EdgeApiState {
    /// Edge ID
    edge_id: Arc<String>,
    /// Cloud endpoint (if configured)
    cloud_endpoint: Arc<Option<String>>,
    /// Sync state (shared)
    sync_state: Arc<RwLock<EdgeSyncStateInner>>,
    /// Conflict history
    conflicts: Arc<RwLock<ConflictStateInner>>,
    /// Optimization stats
    optimization_stats: Arc<RwLock<OptimizationStateInner>>,
    /// Store-and-forward sync progress
    sync_progress: Arc<RwLock<SyncProgressInner>>,
}

#[derive(Debug, Clone, Default)]
struct EdgeSyncStateInner {
    state: String,
    last_sync_at: Option<i64>,
    pending_records: u64,
    total_syncs: u64,
    total_records_synced: u64,
    cloud_reachable: bool,
    sync_lag_records: u64,
}

#[derive(Debug, Clone, Default)]
struct ConflictStateInner {
    total_conflicts: u64,
    edge_wins: u64,
    cloud_wins: u64,
    unresolved: u64,
    entries: Vec<ConflictEntry>,
}

#[derive(Debug, Clone)]
struct OptimizationStateInner {
    reduced_memory_active: bool,
    embedded_mode_active: bool,
    eviction_cycles: u64,
    total_records_evicted: u64,
    compaction_cycles: u64,
    current_storage_bytes: u64,
    max_storage_bytes: u64,
}

impl Default for OptimizationStateInner {
    fn default() -> Self {
        Self {
            reduced_memory_active: false,
            embedded_mode_active: false,
            eviction_cycles: 0,
            total_records_evicted: 0,
            compaction_cycles: 0,
            current_storage_bytes: 0,
            max_storage_bytes: 512 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Default)]
struct SyncProgressInner {
    total_pending_records: u64,
    total_forwarded_records: u64,
    total_failed_uploads: u64,
    batches_completed: u64,
    sync_lag_records: u64,
    last_sync_attempt_at: Option<i64>,
    last_sync_success_at: Option<i64>,
    sync_strategy: String,
    watermarks: Vec<WatermarkEntry>,
}

impl EdgeApiState {
    /// Create a new Edge API state
    pub fn new() -> Self {
        Self {
            edge_id: Arc::new("edge-default".to_string()),
            cloud_endpoint: Arc::new(None),
            sync_state: Arc::new(RwLock::new(EdgeSyncStateInner {
                state: "idle".to_string(),
                ..Default::default()
            })),
            conflicts: Arc::new(RwLock::new(ConflictStateInner::default())),
            optimization_stats: Arc::new(RwLock::new(OptimizationStateInner::default())),
            sync_progress: Arc::new(RwLock::new(SyncProgressInner {
                sync_strategy: "all".to_string(),
                ..Default::default()
            })),
        }
    }

    /// Create with a specific edge ID and cloud endpoint
    pub fn with_config(edge_id: String, cloud_endpoint: Option<String>) -> Self {
        Self {
            edge_id: Arc::new(edge_id),
            cloud_endpoint: Arc::new(cloud_endpoint),
            sync_state: Arc::new(RwLock::new(EdgeSyncStateInner {
                state: "idle".to_string(),
                ..Default::default()
            })),
            conflicts: Arc::new(RwLock::new(ConflictStateInner::default())),
            optimization_stats: Arc::new(RwLock::new(OptimizationStateInner::default())),
            sync_progress: Arc::new(RwLock::new(SyncProgressInner {
                sync_strategy: "all".to_string(),
                ..Default::default()
            })),
        }
    }
}

impl Default for EdgeApiState {
    fn default() -> Self {
        Self::new()
    }
}

/// Create the Edge API router
pub fn create_edge_api_router(state: EdgeApiState) -> Router {
    Router::new()
        .route("/api/v1/edge/status", get(get_edge_status))
        .route("/api/v1/edge/sync", post(trigger_sync))
        .route("/api/v1/edge/sync/status", get(get_sync_status))
        .route("/api/v1/edge/conflicts", get(get_conflicts))
        .route("/api/v1/edge/optimizations", get(get_optimizations))
        .with_state(state)
}

/// GET /api/v1/edge/status
async fn get_edge_status(State(state): State<EdgeApiState>) -> Json<EdgeStatusResponse> {
    let sync = state.sync_state.read().await;
    let conflicts = state.conflicts.read().await;

    let connection_status = if sync.cloud_reachable {
        "connected".to_string()
    } else {
        "disconnected".to_string()
    };

    Json(EdgeStatusResponse {
        edge_mode: true,
        edge_id: state.edge_id.as_ref().clone(),
        sync_state: sync.state.clone(),
        cloud_endpoint: state.cloud_endpoint.as_ref().clone(),
        last_sync_at: sync.last_sync_at,
        pending_records: sync.pending_records,
        conflict_count: conflicts.unresolved,
        total_syncs: sync.total_syncs,
        total_records_synced: sync.total_records_synced,
        cloud_reachable: sync.cloud_reachable,
        connection_status,
        sync_lag_records: sync.sync_lag_records,
    })
}

/// POST /api/v1/edge/sync
async fn trigger_sync(
    State(state): State<EdgeApiState>,
    body: Option<Json<SyncTriggerRequest>>,
) -> Result<Json<SyncTriggerResponse>, (StatusCode, Json<serde_json::Value>)> {
    let direction = body
        .and_then(|b| b.direction.clone())
        .unwrap_or_else(|| "bidirectional".to_string());

    // Validate direction
    if !["bidirectional", "edge_to_cloud", "cloud_to_edge"].contains(&direction.as_str()) {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": format!("Invalid direction '{}'. Must be one of: bidirectional, edge_to_cloud, cloud_to_edge", direction)
            })),
        ));
    }

    let mut sync = state.sync_state.write().await;
    if sync.state == "syncing" {
        return Ok(Json(SyncTriggerResponse {
            initiated: false,
            message: "Sync already in progress".to_string(),
            sync_state: "syncing".to_string(),
        }));
    }

    // Mark as syncing
    sync.state = "syncing".to_string();
    let prev_syncs = sync.total_syncs;

    // Simulate sync completion (in production, this would trigger the EdgeSyncEngine)
    sync.total_syncs = prev_syncs + 1;
    sync.last_sync_at = Some(chrono::Utc::now().timestamp_millis());
    sync.state = "completed".to_string();

    Ok(Json(SyncTriggerResponse {
        initiated: true,
        message: format!("Sync triggered with direction: {}", direction),
        sync_state: "completed".to_string(),
    }))
}

/// GET /api/v1/edge/sync/status
async fn get_sync_status(State(state): State<EdgeApiState>) -> Json<SyncProgressResponse> {
    let sync = state.sync_state.read().await;
    let progress = state.sync_progress.read().await;

    Json(SyncProgressResponse {
        sync_state: sync.state.clone(),
        total_pending_records: progress.total_pending_records,
        total_forwarded_records: progress.total_forwarded_records,
        total_failed_uploads: progress.total_failed_uploads,
        batches_completed: progress.batches_completed,
        sync_lag_records: progress.sync_lag_records,
        last_sync_attempt_at: progress.last_sync_attempt_at,
        last_sync_success_at: progress.last_sync_success_at,
        sync_strategy: progress.sync_strategy.clone(),
        watermarks: progress.watermarks.clone(),
    })
}

/// GET /api/v1/edge/conflicts
async fn get_conflicts(State(state): State<EdgeApiState>) -> Json<ConflictsResponse> {
    let conflicts = state.conflicts.read().await;

    Json(ConflictsResponse {
        total_conflicts: conflicts.total_conflicts,
        edge_wins: conflicts.edge_wins,
        cloud_wins: conflicts.cloud_wins,
        unresolved: conflicts.unresolved,
        conflicts: conflicts.entries.clone(),
    })
}

/// GET /api/v1/edge/optimizations
async fn get_optimizations(State(state): State<EdgeApiState>) -> Json<OptimizationsResponse> {
    let opt = state.optimization_stats.read().await;

    let utilization = if opt.max_storage_bytes > 0 {
        opt.current_storage_bytes as f64 / opt.max_storage_bytes as f64
    } else {
        0.0
    };

    Json(OptimizationsResponse {
        reduced_memory_active: opt.reduced_memory_active,
        embedded_mode_active: opt.embedded_mode_active,
        eviction_cycles: opt.eviction_cycles,
        total_records_evicted: opt.total_records_evicted,
        compaction_cycles: opt.compaction_cycles,
        current_storage_bytes: opt.current_storage_bytes,
        max_storage_bytes: opt.max_storage_bytes,
        storage_utilization: utilization,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    fn create_test_app() -> Router {
        let state = EdgeApiState::new();
        create_edge_api_router(state)
    }

    fn create_configured_app() -> Router {
        let state = EdgeApiState::with_config(
            "edge-test-123".to_string(),
            Some("https://cloud.example.com:9092".to_string()),
        );
        create_edge_api_router(state)
    }

    #[tokio::test]
    async fn test_get_edge_status() {
        let app = create_test_app();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/edge/status")
                    .body(Body::empty())
                    .expect("request builder"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let status: EdgeStatusResponse =
            serde_json::from_slice(&body).expect("deserialize");

        assert!(status.edge_mode);
        assert_eq!(status.sync_state, "idle");
        assert_eq!(status.pending_records, 0);
        assert_eq!(status.connection_status, "disconnected");
        assert_eq!(status.sync_lag_records, 0);
    }

    #[tokio::test]
    async fn test_get_edge_status_configured() {
        let app = create_configured_app();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/edge/status")
                    .body(Body::empty())
                    .expect("request builder"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let status: EdgeStatusResponse =
            serde_json::from_slice(&body).expect("deserialize");

        assert_eq!(status.edge_id, "edge-test-123");
        assert_eq!(
            status.cloud_endpoint,
            Some("https://cloud.example.com:9092".to_string())
        );
    }

    #[tokio::test]
    async fn test_trigger_sync() {
        let app = create_test_app();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/edge/sync")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"direction": "bidirectional"}"#))
                    .expect("request builder"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let sync_resp: SyncTriggerResponse =
            serde_json::from_slice(&body).expect("deserialize");

        assert!(sync_resp.initiated);
        assert_eq!(sync_resp.sync_state, "completed");
    }

    #[tokio::test]
    async fn test_trigger_sync_no_body() {
        let app = create_test_app();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/edge/sync")
                    .body(Body::empty())
                    .expect("request builder"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_sync_status() {
        let app = create_test_app();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/edge/sync/status")
                    .body(Body::empty())
                    .expect("request builder"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let progress: SyncProgressResponse =
            serde_json::from_slice(&body).expect("deserialize");

        assert_eq!(progress.sync_state, "idle");
        assert_eq!(progress.total_pending_records, 0);
        assert_eq!(progress.sync_strategy, "all");
        assert!(progress.watermarks.is_empty());
    }

    #[tokio::test]
    async fn test_get_conflicts() {
        let app = create_test_app();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/edge/conflicts")
                    .body(Body::empty())
                    .expect("request builder"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let conflicts: ConflictsResponse =
            serde_json::from_slice(&body).expect("deserialize");

        assert_eq!(conflicts.total_conflicts, 0);
        assert_eq!(conflicts.unresolved, 0);
    }

    #[tokio::test]
    async fn test_get_optimizations() {
        let app = create_test_app();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/edge/optimizations")
                    .body(Body::empty())
                    .expect("request builder"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let opt: OptimizationsResponse =
            serde_json::from_slice(&body).expect("deserialize");

        assert!(!opt.reduced_memory_active);
        assert_eq!(opt.eviction_cycles, 0);
    }
}
