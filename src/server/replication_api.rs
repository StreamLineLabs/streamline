//! Geo-Replication API - REST endpoints for managing cross-region replication
//!
//! ## Endpoints
//!
//! - `GET /api/v1/replication/status` - Get replication cluster status
//! - `GET /api/v1/replication/regions` - List all regions
//! - `POST /api/v1/replication/regions` - Add a region
//! - `DELETE /api/v1/replication/regions/:id` - Remove a region
//! - `GET /api/v1/replication/regions/:id/lag` - Get replication lag
//! - `POST /api/v1/replication/regions/:id/sync` - Force sync with a region
//! - `GET /api/v1/replication/topics` - List replicated topics
//! - `POST /api/v1/replication/topics` - Configure topic replication
//! - `GET /api/v1/replication/conflicts` - List recent conflicts
//! - `GET /api/v1/replication/stats` - Get aggregate replication stats

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Region definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegionDef {
    pub id: String,
    pub name: String,
    pub endpoint: String,
    pub cloud_provider: Option<String>,
    pub is_local: bool,
}

/// Region status
#[derive(Debug, Clone, Serialize)]
pub struct RegionStatusInfo {
    pub id: String,
    pub name: String,
    pub state: String,
    pub replication_lag_ms: u64,
    pub last_sync_at: Option<u64>,
    pub records_replicated: u64,
    pub errors: u64,
}

/// Replicated topic config
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicatedTopicConfig {
    pub topic: String,
    pub regions: Vec<String>,
    pub consistency_level: String,
    pub conflict_strategy: String,
}

/// Conflict record
#[derive(Debug, Clone, Serialize)]
pub struct ConflictRecord {
    pub id: u64,
    pub topic: String,
    pub partition: i32,
    pub key: Option<String>,
    pub regions_involved: Vec<String>,
    pub resolution: String,
    pub winner_region: Option<String>,
    pub detected_at: u64,
}

/// Aggregate replication stats
#[derive(Debug, Clone, Serialize)]
pub struct ReplicationStatsResponse {
    pub total_regions: usize,
    pub active_regions: usize,
    pub replicated_topics: usize,
    pub total_records_replicated: u64,
    pub total_conflicts_resolved: u64,
    pub max_lag_ms: u64,
    pub avg_lag_ms: f64,
}

#[derive(Clone)]
struct ManagedRegion {
    def: RegionDef,
    state: String,
    replication_lag_ms: u64,
    records_replicated: u64,
    errors: u64,
}

/// Shared state for Replication API
#[derive(Clone)]
pub struct ReplicationApiState {
    regions: Arc<RwLock<HashMap<String, ManagedRegion>>>,
    replicated_topics: Arc<RwLock<Vec<ReplicatedTopicConfig>>>,
    conflicts: Arc<RwLock<Vec<ConflictRecord>>>,
    #[allow(dead_code)]
    conflict_counter: Arc<std::sync::atomic::AtomicU64>,
}

impl ReplicationApiState {
    pub fn new() -> Self {
        Self {
            regions: Arc::new(RwLock::new(HashMap::new())),
            replicated_topics: Arc::new(RwLock::new(Vec::new())),
            conflicts: Arc::new(RwLock::new(Vec::new())),
            conflict_counter: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }
}

impl Default for ReplicationApiState {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

/// Create the Replication API router
pub fn create_replication_api_router(state: ReplicationApiState) -> Router {
    Router::new()
        .route("/api/v1/replication/status", get(get_status))
        .route(
            "/api/v1/replication/regions",
            get(list_regions).post(add_region),
        )
        .route(
            "/api/v1/replication/regions/{id}",
            get(get_region).delete(remove_region),
        )
        .route("/api/v1/replication/regions/{id}/lag", get(get_region_lag))
        .route("/api/v1/replication/regions/{id}/sync", post(force_sync))
        .route(
            "/api/v1/replication/topics",
            get(list_replicated_topics).post(configure_topic_replication),
        )
        .route("/api/v1/replication/conflicts", get(list_conflicts))
        .route("/api/v1/replication/stats", get(get_stats))
        .with_state(state)
}

/// GET /api/v1/replication/status
async fn get_status(State(state): State<ReplicationApiState>) -> Json<serde_json::Value> {
    let regions = state.regions.read().await;
    let topics = state.replicated_topics.read().await;
    let active = regions.values().filter(|r| r.state == "active").count();

    Json(serde_json::json!({
        "status": if active > 0 { "replicating" } else { "standalone" },
        "local_region": "local",
        "total_regions": regions.len(),
        "active_regions": active,
        "replicated_topics": topics.len(),
        "mode": if active > 1 { "active-active" } else { "active-passive" }
    }))
}

/// GET /api/v1/replication/regions
async fn list_regions(State(state): State<ReplicationApiState>) -> Json<Vec<RegionStatusInfo>> {
    let regions = state.regions.read().await;
    let infos: Vec<RegionStatusInfo> = regions
        .values()
        .map(|r| RegionStatusInfo {
            id: r.def.id.clone(),
            name: r.def.name.clone(),
            state: r.state.clone(),
            replication_lag_ms: r.replication_lag_ms,
            last_sync_at: None,
            records_replicated: r.records_replicated,
            errors: r.errors,
        })
        .collect();
    Json(infos)
}

/// POST /api/v1/replication/regions
async fn add_region(
    State(state): State<ReplicationApiState>,
    Json(def): Json<RegionDef>,
) -> Result<(StatusCode, Json<RegionStatusInfo>), (StatusCode, Json<ErrorResponse>)> {
    let id = def.id.clone();
    let mut regions = state.regions.write().await;
    if regions.contains_key(&id) {
        return Err((
            StatusCode::CONFLICT,
            Json(ErrorResponse {
                error: format!("Region '{}' already exists", id),
            }),
        ));
    }

    let managed = ManagedRegion {
        def: def.clone(),
        state: "connecting".to_string(),
        replication_lag_ms: 0,
        records_replicated: 0,
        errors: 0,
    };

    regions.insert(id.clone(), managed);

    Ok((
        StatusCode::CREATED,
        Json(RegionStatusInfo {
            id,
            name: def.name,
            state: "connecting".to_string(),
            replication_lag_ms: 0,
            last_sync_at: None,
            records_replicated: 0,
            errors: 0,
        }),
    ))
}

/// GET /api/v1/replication/regions/:id
async fn get_region(
    State(state): State<ReplicationApiState>,
    Path(id): Path<String>,
) -> Result<Json<RegionStatusInfo>, (StatusCode, Json<ErrorResponse>)> {
    let regions = state.regions.read().await;
    regions
        .get(&id)
        .map(|r| {
            Json(RegionStatusInfo {
                id: r.def.id.clone(),
                name: r.def.name.clone(),
                state: r.state.clone(),
                replication_lag_ms: r.replication_lag_ms,
                last_sync_at: None,
                records_replicated: r.records_replicated,
                errors: r.errors,
            })
        })
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: format!("Region '{}' not found", id),
                }),
            )
        })
}

/// DELETE /api/v1/replication/regions/:id
async fn remove_region(
    State(state): State<ReplicationApiState>,
    Path(id): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let mut regions = state.regions.write().await;
    if regions.remove(&id).is_some() {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Region '{}' not found", id),
            }),
        ))
    }
}

/// GET /api/v1/replication/regions/:id/lag
async fn get_region_lag(
    State(state): State<ReplicationApiState>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let regions = state.regions.read().await;
    regions
        .get(&id)
        .map(|r| {
            Json(serde_json::json!({
                "region_id": r.def.id,
                "lag_ms": r.replication_lag_ms,
                "status": r.state,
                "records_behind": 0
            }))
        })
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: format!("Region '{}' not found", id),
                }),
            )
        })
}

/// POST /api/v1/replication/regions/:id/sync
async fn force_sync(
    State(state): State<ReplicationApiState>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let mut regions = state.regions.write().await;
    let region = regions.get_mut(&id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Region '{}' not found", id),
            }),
        )
    })?;

    region.state = "syncing".to_string();

    Ok(Json(serde_json::json!({
        "region_id": id,
        "action": "sync_initiated",
        "message": "Force sync initiated"
    })))
}

/// GET /api/v1/replication/topics
async fn list_replicated_topics(
    State(state): State<ReplicationApiState>,
) -> Json<Vec<ReplicatedTopicConfig>> {
    let topics = state.replicated_topics.read().await;
    Json(topics.clone())
}

/// POST /api/v1/replication/topics
async fn configure_topic_replication(
    State(state): State<ReplicationApiState>,
    Json(config): Json<ReplicatedTopicConfig>,
) -> (StatusCode, Json<ReplicatedTopicConfig>) {
    let mut topics = state.replicated_topics.write().await;
    // Replace existing config for the topic or add new
    topics.retain(|t| t.topic != config.topic);
    topics.push(config.clone());
    (StatusCode::CREATED, Json(config))
}

/// GET /api/v1/replication/conflicts
async fn list_conflicts(State(state): State<ReplicationApiState>) -> Json<Vec<ConflictRecord>> {
    let conflicts = state.conflicts.read().await;
    Json(conflicts.clone())
}

/// GET /api/v1/replication/stats
async fn get_stats(State(state): State<ReplicationApiState>) -> Json<ReplicationStatsResponse> {
    let regions = state.regions.read().await;
    let topics = state.replicated_topics.read().await;
    let conflicts = state.conflicts.read().await;

    let active = regions.values().filter(|r| r.state == "active").count();
    let total_replicated: u64 = regions.values().map(|r| r.records_replicated).sum();
    let max_lag: u64 = regions
        .values()
        .map(|r| r.replication_lag_ms)
        .max()
        .unwrap_or(0);
    let avg_lag: f64 = if regions.is_empty() {
        0.0
    } else {
        regions.values().map(|r| r.replication_lag_ms).sum::<u64>() as f64 / regions.len() as f64
    };

    Json(ReplicationStatsResponse {
        total_regions: regions.len(),
        active_regions: active,
        replicated_topics: topics.len(),
        total_records_replicated: total_replicated,
        total_conflicts_resolved: conflicts.len() as u64,
        max_lag_ms: max_lag,
        avg_lag_ms: avg_lag,
    })
}
