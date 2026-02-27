//! Feature Store Serving API
//!
//! REST API for real-time ML feature serving from streaming aggregations.
//!
//! ## Endpoints
//!
//! - `POST   /api/v1/features/groups`                - Create a feature group
//! - `GET    /api/v1/features/groups`                 - List feature groups
//! - `GET    /api/v1/features/groups/:name`           - Get feature group
//! - `DELETE /api/v1/features/groups/:name`           - Delete feature group
//! - `POST   /api/v1/features/groups/:name/ingest`    - Ingest feature values
//! - `POST   /api/v1/features/serve`                  - Serve features (low-latency lookup)
//! - `GET    /api/v1/features/serve/:group/:entity`   - Get features for entity
//! - `POST   /api/v1/features/serve/batch`            - Batch feature lookup
//! - `GET    /api/v1/features/stats`                  - Feature store stats

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
use tracing::{debug, info};

// ── State ──────────────────────────────────────────────────────────

/// Shared state for the Feature Store API.
#[derive(Clone)]
pub struct FeatureStoreApiState {
    /// Feature groups keyed by name.
    pub groups: Arc<RwLock<HashMap<String, FeatureGroup>>>,
    /// Feature vectors keyed by `(group_name, entity_id)`.
    pub features: Arc<RwLock<HashMap<(String, String), FeatureVector>>>,
}

impl FeatureStoreApiState {
    pub fn new() -> Self {
        Self {
            groups: Arc::new(RwLock::new(HashMap::new())),
            features: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl Default for FeatureStoreApiState {
    fn default() -> Self {
        Self::new()
    }
}

// ── Domain types ───────────────────────────────────────────────────

/// A feature group defines a set of features for a given entity key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureGroup {
    pub name: String,
    /// The entity key column (e.g. `"user_id"`).
    pub entity_key: String,
    /// Feature definitions within this group.
    pub features: Vec<FeatureDefinition>,
    /// Optional source topic for streaming ingestion.
    pub source_topic: Option<String>,
    /// Optional TTL for feature values (seconds).
    pub ttl_seconds: Option<u64>,
    /// ISO-8601 creation timestamp.
    pub created_at: String,
    /// Number of distinct entities with stored features.
    pub entity_count: u64,
}

/// Definition of a single feature within a group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureDefinition {
    pub name: String,
    pub dtype: FeatureDType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_value: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// Supported feature data types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FeatureDType {
    Float64,
    Int64,
    String,
    Bool,
    Vector,
}

/// Stored feature values for a single entity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureVector {
    pub entity_id: String,
    pub features: HashMap<String, serde_json::Value>,
    pub updated_at: String,
    pub version: u64,
}

// ── Request / response types ───────────────────────────────────────

/// Request body for creating a feature group.
#[derive(Debug, Deserialize)]
pub struct CreateGroupRequest {
    pub name: String,
    pub entity_key: String,
    #[serde(default)]
    pub features: Vec<FeatureDefinition>,
    #[serde(default)]
    pub source_topic: Option<String>,
    #[serde(default)]
    pub ttl_seconds: Option<u64>,
}

/// Request body for ingesting feature values into a group.
#[derive(Debug, Deserialize)]
pub struct IngestRequest {
    pub entity_id: String,
    pub features: HashMap<String, serde_json::Value>,
}

/// Request body for serving features.
#[derive(Debug, Deserialize)]
pub struct ServeRequest {
    pub group: String,
    pub entity_ids: Vec<String>,
    /// If `None`, all features are returned.
    #[serde(default)]
    pub feature_names: Option<Vec<String>>,
}

/// Response for a serve / batch request.
#[derive(Debug, Serialize)]
pub struct ServeResponse {
    pub features: Vec<EntityFeatures>,
    pub metadata: ServeMetadata,
}

/// Features for a single entity in a serve response.
#[derive(Debug, Serialize)]
pub struct EntityFeatures {
    pub entity_id: String,
    pub features: HashMap<String, serde_json::Value>,
    pub found: bool,
}

/// Metadata included in a serve response.
#[derive(Debug, Serialize)]
pub struct ServeMetadata {
    pub latency_ms: f64,
    pub entities_found: usize,
    pub entities_missing: usize,
}

/// Stats response.
#[derive(Debug, Serialize)]
pub struct FeatureStoreStats {
    pub total_groups: usize,
    pub total_entities: usize,
    pub total_feature_vectors: usize,
}

/// Generic error response.
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

// ── Router ─────────────────────────────────────────────────────────

/// Build the feature-store serving router.
pub fn create_feature_store_api_router(state: FeatureStoreApiState) -> Router {
    Router::new()
        .route(
            "/api/v1/features/groups",
            get(list_groups).post(create_group),
        )
        .route(
            "/api/v1/features/groups/:name",
            get(get_group).delete(delete_group),
        )
        .route(
            "/api/v1/features/groups/:name/ingest",
            post(ingest_features),
        )
        .route("/api/v1/features/serve", post(serve_features))
        .route(
            "/api/v1/features/serve/:group/:entity",
            get(serve_entity),
        )
        .route("/api/v1/features/serve/batch", post(batch_serve))
        .route("/api/v1/features/stats", get(feature_stats))
        .with_state(state)
}

// ── Handlers ───────────────────────────────────────────────────────

async fn create_group(
    State(state): State<FeatureStoreApiState>,
    Json(req): Json<CreateGroupRequest>,
) -> Result<(StatusCode, Json<FeatureGroup>), (StatusCode, Json<ErrorResponse>)> {
    let mut groups = state.groups.write().await;
    if groups.contains_key(&req.name) {
        return Err((
            StatusCode::CONFLICT,
            Json(ErrorResponse {
                error: format!("Feature group '{}' already exists", req.name),
            }),
        ));
    }
    let group = FeatureGroup {
        name: req.name.clone(),
        entity_key: req.entity_key,
        features: req.features,
        source_topic: req.source_topic,
        ttl_seconds: req.ttl_seconds,
        created_at: chrono::Utc::now().to_rfc3339(),
        entity_count: 0,
    };
    groups.insert(req.name.clone(), group.clone());
    info!(name = %req.name, "Feature group created");
    Ok((StatusCode::CREATED, Json(group)))
}

async fn list_groups(
    State(state): State<FeatureStoreApiState>,
) -> Json<Vec<FeatureGroup>> {
    let groups = state.groups.read().await;
    Json(groups.values().cloned().collect())
}

async fn get_group(
    State(state): State<FeatureStoreApiState>,
    Path(name): Path<String>,
) -> Result<Json<FeatureGroup>, (StatusCode, Json<ErrorResponse>)> {
    let groups = state.groups.read().await;
    groups
        .get(&name)
        .cloned()
        .map(Json)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: format!("Feature group '{}' not found", name),
                }),
            )
        })
}

async fn delete_group(
    State(state): State<FeatureStoreApiState>,
    Path(name): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let mut groups = state.groups.write().await;
    if groups.remove(&name).is_none() {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Feature group '{}' not found", name),
            }),
        ));
    }
    // Remove associated feature vectors.
    let mut feats = state.features.write().await;
    feats.retain(|(g, _), _| g != &name);
    info!(name = %name, "Feature group deleted");
    Ok(StatusCode::NO_CONTENT)
}

async fn ingest_features(
    State(state): State<FeatureStoreApiState>,
    Path(name): Path<String>,
    Json(req): Json<IngestRequest>,
) -> Result<(StatusCode, Json<FeatureVector>), (StatusCode, Json<ErrorResponse>)> {
    // Validate group exists.
    {
        let groups = state.groups.read().await;
        if !groups.contains_key(&name) {
            return Err((
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: format!("Feature group '{}' not found", name),
                }),
            ));
        }
    }

    let key = (name.clone(), req.entity_id.clone());
    let mut feats = state.features.write().await;
    let version = feats
        .get(&key)
        .map(|v| v.version + 1)
        .unwrap_or(1);

    let is_new = !feats.contains_key(&key);

    let vec = FeatureVector {
        entity_id: req.entity_id,
        features: req.features,
        updated_at: chrono::Utc::now().to_rfc3339(),
        version,
    };
    feats.insert(key, vec.clone());

    // Update entity count if this is a new entity.
    if is_new {
        let mut groups = state.groups.write().await;
        if let Some(g) = groups.get_mut(&name) {
            g.entity_count += 1;
        }
    }

    debug!(group = %name, entity = %vec.entity_id, version, "Feature vector ingested");
    Ok((StatusCode::OK, Json(vec)))
}

async fn serve_features(
    State(state): State<FeatureStoreApiState>,
    Json(req): Json<ServeRequest>,
) -> Result<Json<ServeResponse>, (StatusCode, Json<ErrorResponse>)> {
    let start = std::time::Instant::now();

    // Validate group.
    {
        let groups = state.groups.read().await;
        if !groups.contains_key(&req.group) {
            return Err((
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: format!("Feature group '{}' not found", req.group),
                }),
            ));
        }
    }

    let feats = state.features.read().await;
    let mut results = Vec::with_capacity(req.entity_ids.len());
    let mut found = 0usize;
    let mut missing = 0usize;

    for eid in &req.entity_ids {
        let key = (req.group.clone(), eid.clone());
        if let Some(vec) = feats.get(&key) {
            let filtered = filter_features(&vec.features, &req.feature_names);
            results.push(EntityFeatures {
                entity_id: eid.clone(),
                features: filtered,
                found: true,
            });
            found += 1;
        } else {
            results.push(EntityFeatures {
                entity_id: eid.clone(),
                features: HashMap::new(),
                found: false,
            });
            missing += 1;
        }
    }

    let latency = start.elapsed().as_secs_f64() * 1000.0;
    Ok(Json(ServeResponse {
        features: results,
        metadata: ServeMetadata {
            latency_ms: latency,
            entities_found: found,
            entities_missing: missing,
        },
    }))
}

async fn serve_entity(
    State(state): State<FeatureStoreApiState>,
    Path((group, entity)): Path<(String, String)>,
) -> Result<Json<EntityFeatures>, (StatusCode, Json<ErrorResponse>)> {
    let feats = state.features.read().await;
    let key = (group.clone(), entity.clone());
    if let Some(vec) = feats.get(&key) {
        Ok(Json(EntityFeatures {
            entity_id: entity,
            features: vec.features.clone(),
            found: true,
        }))
    } else {
        Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Entity '{entity}' not found in group '{group}'"),
            }),
        ))
    }
}

async fn batch_serve(
    State(state): State<FeatureStoreApiState>,
    Json(req): Json<ServeRequest>,
) -> Result<Json<ServeResponse>, (StatusCode, Json<ErrorResponse>)> {
    // Reuse the same logic as serve_features.
    serve_features(State(state), Json(req)).await
}

async fn feature_stats(
    State(state): State<FeatureStoreApiState>,
) -> Json<FeatureStoreStats> {
    let groups = state.groups.read().await;
    let feats = state.features.read().await;
    Json(FeatureStoreStats {
        total_groups: groups.len(),
        total_entities: groups.values().map(|g| g.entity_count as usize).sum(),
        total_feature_vectors: feats.len(),
    })
}

// ── Helpers ────────────────────────────────────────────────────────

/// Filter a feature map to only the requested feature names.
fn filter_features(
    all: &HashMap<String, serde_json::Value>,
    names: &Option<Vec<String>>,
) -> HashMap<String, serde_json::Value> {
    match names {
        Some(selected) => all
            .iter()
            .filter(|(k, _)| selected.contains(k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        None => all.clone(),
    }
}

// ── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Method, Request};
    use tower::ServiceExt;

    fn app() -> Router {
        create_feature_store_api_router(FeatureStoreApiState::new())
    }

    fn json_request(method: Method, uri: &str, body: serde_json::Value) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap()
    }

    fn empty_request(method: Method, uri: &str) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .body(Body::empty())
            .unwrap()
    }

    async fn body_json(resp: axum::http::Response<Body>) -> serde_json::Value {
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    // ── Group CRUD ───

    #[tokio::test]
    async fn test_create_group() {
        let resp = app()
            .oneshot(json_request(
                Method::POST,
                "/api/v1/features/groups",
                serde_json::json!({
                    "name": "user_features",
                    "entity_key": "user_id",
                    "features": [{"name": "age", "dtype": "Int64"}]
                }),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
        let body = body_json(resp).await;
        assert_eq!(body["name"], "user_features");
        assert_eq!(body["entity_key"], "user_id");
    }

    #[tokio::test]
    async fn test_create_duplicate_group() {
        let state = FeatureStoreApiState::new();
        let router = create_feature_store_api_router(state);
        let req_body = serde_json::json!({"name": "g", "entity_key": "id"});

        let resp = router
            .clone()
            .oneshot(json_request(Method::POST, "/api/v1/features/groups", req_body.clone()))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);

        let resp2 = router
            .oneshot(json_request(Method::POST, "/api/v1/features/groups", req_body))
            .await
            .unwrap();
        assert_eq!(resp2.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn test_list_groups_empty() {
        let resp = app()
            .oneshot(empty_request(Method::GET, "/api/v1/features/groups"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        assert_eq!(body, serde_json::json!([]));
    }

    #[tokio::test]
    async fn test_list_groups_non_empty() {
        let state = FeatureStoreApiState::new();
        let router = create_feature_store_api_router(state);
        router
            .clone()
            .oneshot(json_request(
                Method::POST,
                "/api/v1/features/groups",
                serde_json::json!({"name": "g1", "entity_key": "id"}),
            ))
            .await
            .unwrap();

        let resp = router
            .oneshot(empty_request(Method::GET, "/api/v1/features/groups"))
            .await
            .unwrap();
        let body = body_json(resp).await;
        let arr = body.as_array().unwrap();
        assert_eq!(arr.len(), 1);
    }

    #[tokio::test]
    async fn test_get_group() {
        let state = FeatureStoreApiState::new();
        let router = create_feature_store_api_router(state);
        router
            .clone()
            .oneshot(json_request(
                Method::POST,
                "/api/v1/features/groups",
                serde_json::json!({"name": "g1", "entity_key": "uid"}),
            ))
            .await
            .unwrap();

        let resp = router
            .oneshot(empty_request(Method::GET, "/api/v1/features/groups/g1"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        assert_eq!(body["name"], "g1");
    }

    #[tokio::test]
    async fn test_get_group_not_found() {
        let resp = app()
            .oneshot(empty_request(Method::GET, "/api/v1/features/groups/nope"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_delete_group() {
        let state = FeatureStoreApiState::new();
        let router = create_feature_store_api_router(state);
        router
            .clone()
            .oneshot(json_request(
                Method::POST,
                "/api/v1/features/groups",
                serde_json::json!({"name": "g1", "entity_key": "id"}),
            ))
            .await
            .unwrap();

        let resp = router
            .clone()
            .oneshot(empty_request(Method::DELETE, "/api/v1/features/groups/g1"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);

        // Verify gone.
        let resp2 = router
            .oneshot(empty_request(Method::GET, "/api/v1/features/groups/g1"))
            .await
            .unwrap();
        assert_eq!(resp2.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_delete_group_not_found() {
        let resp = app()
            .oneshot(empty_request(Method::DELETE, "/api/v1/features/groups/nope"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    // ── Ingest ───

    #[tokio::test]
    async fn test_ingest_features() {
        let state = FeatureStoreApiState::new();
        let router = create_feature_store_api_router(state);
        router
            .clone()
            .oneshot(json_request(
                Method::POST,
                "/api/v1/features/groups",
                serde_json::json!({"name": "g1", "entity_key": "uid"}),
            ))
            .await
            .unwrap();

        let resp = router
            .oneshot(json_request(
                Method::POST,
                "/api/v1/features/groups/g1/ingest",
                serde_json::json!({"entity_id": "u1", "features": {"age": 25}}),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        assert_eq!(body["entity_id"], "u1");
        assert_eq!(body["version"], 1);
    }

    #[tokio::test]
    async fn test_ingest_group_not_found() {
        let resp = app()
            .oneshot(json_request(
                Method::POST,
                "/api/v1/features/groups/nope/ingest",
                serde_json::json!({"entity_id": "u1", "features": {}}),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_ingest_increments_version() {
        let state = FeatureStoreApiState::new();
        let router = create_feature_store_api_router(state);
        router
            .clone()
            .oneshot(json_request(
                Method::POST,
                "/api/v1/features/groups",
                serde_json::json!({"name": "g1", "entity_key": "uid"}),
            ))
            .await
            .unwrap();

        router
            .clone()
            .oneshot(json_request(
                Method::POST,
                "/api/v1/features/groups/g1/ingest",
                serde_json::json!({"entity_id": "u1", "features": {"a": 1}}),
            ))
            .await
            .unwrap();

        let resp = router
            .oneshot(json_request(
                Method::POST,
                "/api/v1/features/groups/g1/ingest",
                serde_json::json!({"entity_id": "u1", "features": {"a": 2}}),
            ))
            .await
            .unwrap();
        let body = body_json(resp).await;
        assert_eq!(body["version"], 2);
    }

    // ── Serve ───

    #[tokio::test]
    async fn test_serve_features() {
        let state = FeatureStoreApiState::new();
        let router = create_feature_store_api_router(state);
        // create group + ingest
        router
            .clone()
            .oneshot(json_request(Method::POST, "/api/v1/features/groups", serde_json::json!({"name":"g","entity_key":"id"})))
            .await.unwrap();
        router
            .clone()
            .oneshot(json_request(Method::POST, "/api/v1/features/groups/g/ingest", serde_json::json!({"entity_id":"e1","features":{"x":1.0}})))
            .await.unwrap();

        let resp = router
            .oneshot(json_request(
                Method::POST,
                "/api/v1/features/serve",
                serde_json::json!({"group":"g","entity_ids":["e1","e_missing"]}),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        assert_eq!(body["metadata"]["entities_found"], 1);
        assert_eq!(body["metadata"]["entities_missing"], 1);
    }

    #[tokio::test]
    async fn test_serve_group_not_found() {
        let resp = app()
            .oneshot(json_request(
                Method::POST,
                "/api/v1/features/serve",
                serde_json::json!({"group":"nope","entity_ids":["x"]}),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_serve_entity_endpoint() {
        let state = FeatureStoreApiState::new();
        let router = create_feature_store_api_router(state);
        router.clone().oneshot(json_request(Method::POST, "/api/v1/features/groups", serde_json::json!({"name":"g","entity_key":"id"}))).await.unwrap();
        router.clone().oneshot(json_request(Method::POST, "/api/v1/features/groups/g/ingest", serde_json::json!({"entity_id":"e1","features":{"v":42}}))).await.unwrap();

        let resp = router
            .oneshot(empty_request(Method::GET, "/api/v1/features/serve/g/e1"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        assert_eq!(body["entity_id"], "e1");
        assert!(body["found"].as_bool().unwrap());
    }

    #[tokio::test]
    async fn test_serve_entity_not_found() {
        let resp = app()
            .oneshot(empty_request(Method::GET, "/api/v1/features/serve/g/missing"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_batch_serve() {
        let state = FeatureStoreApiState::new();
        let router = create_feature_store_api_router(state);
        router.clone().oneshot(json_request(Method::POST, "/api/v1/features/groups", serde_json::json!({"name":"g","entity_key":"id"}))).await.unwrap();
        router.clone().oneshot(json_request(Method::POST, "/api/v1/features/groups/g/ingest", serde_json::json!({"entity_id":"e1","features":{"a":1}}))).await.unwrap();
        router.clone().oneshot(json_request(Method::POST, "/api/v1/features/groups/g/ingest", serde_json::json!({"entity_id":"e2","features":{"a":2}}))).await.unwrap();

        let resp = router
            .oneshot(json_request(
                Method::POST,
                "/api/v1/features/serve/batch",
                serde_json::json!({"group":"g","entity_ids":["e1","e2"]}),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        assert_eq!(body["metadata"]["entities_found"], 2);
    }

    // ── Stats ───

    #[tokio::test]
    async fn test_stats_empty() {
        let resp = app()
            .oneshot(empty_request(Method::GET, "/api/v1/features/stats"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        assert_eq!(body["total_groups"], 0);
        assert_eq!(body["total_entities"], 0);
        assert_eq!(body["total_feature_vectors"], 0);
    }

    #[tokio::test]
    async fn test_stats_after_operations() {
        let state = FeatureStoreApiState::new();
        let router = create_feature_store_api_router(state);
        router.clone().oneshot(json_request(Method::POST, "/api/v1/features/groups", serde_json::json!({"name":"g","entity_key":"id"}))).await.unwrap();
        router.clone().oneshot(json_request(Method::POST, "/api/v1/features/groups/g/ingest", serde_json::json!({"entity_id":"e1","features":{"v":1}}))).await.unwrap();

        let resp = router
            .oneshot(empty_request(Method::GET, "/api/v1/features/stats"))
            .await
            .unwrap();
        let body = body_json(resp).await;
        assert_eq!(body["total_groups"], 1);
        assert_eq!(body["total_entities"], 1);
        assert_eq!(body["total_feature_vectors"], 1);
    }

    // ── Feature filtering ───

    #[tokio::test]
    async fn test_serve_with_feature_filter() {
        let state = FeatureStoreApiState::new();
        let router = create_feature_store_api_router(state);
        router.clone().oneshot(json_request(Method::POST, "/api/v1/features/groups", serde_json::json!({"name":"g","entity_key":"id"}))).await.unwrap();
        router.clone().oneshot(json_request(Method::POST, "/api/v1/features/groups/g/ingest", serde_json::json!({"entity_id":"e1","features":{"a":1,"b":2,"c":3}}))).await.unwrap();

        let resp = router
            .oneshot(json_request(
                Method::POST,
                "/api/v1/features/serve",
                serde_json::json!({"group":"g","entity_ids":["e1"],"feature_names":["a","c"]}),
            ))
            .await
            .unwrap();
        let body = body_json(resp).await;
        let feats = &body["features"][0]["features"];
        assert!(feats.get("a").is_some());
        assert!(feats.get("b").is_none());
        assert!(feats.get("c").is_some());
    }

    #[tokio::test]
    async fn test_delete_group_removes_features() {
        let state = FeatureStoreApiState::new();
        let router = create_feature_store_api_router(state.clone());
        router.clone().oneshot(json_request(Method::POST, "/api/v1/features/groups", serde_json::json!({"name":"g","entity_key":"id"}))).await.unwrap();
        router.clone().oneshot(json_request(Method::POST, "/api/v1/features/groups/g/ingest", serde_json::json!({"entity_id":"e1","features":{"v":1}}))).await.unwrap();

        router.clone().oneshot(empty_request(Method::DELETE, "/api/v1/features/groups/g")).await.unwrap();

        // Feature vectors should be cleaned up.
        let feats = state.features.read().await;
        assert!(feats.is_empty());
    }
}
