//! Multi-Tenant Management REST API
//!
//! Provides endpoints for managing tenants, their quotas, usage stats,
//! and API keys in the Streamline multi-tenant streaming platform.
//!
//! ## Endpoints
//!
//! - `POST   /api/v1/tenants`                          - Create a new tenant
//! - `GET    /api/v1/tenants`                           - List all tenants
//! - `GET    /api/v1/tenants/:id`                       - Get tenant details
//! - `PUT    /api/v1/tenants/:id`                       - Update tenant config
//! - `DELETE /api/v1/tenants/:id`                       - Delete a tenant
//! - `GET    /api/v1/tenants/:id/usage`                 - Get resource usage stats
//! - `PUT    /api/v1/tenants/:id/quotas`                - Set resource quotas
//! - `GET    /api/v1/tenants/:id/quotas`                - Get current quotas
//! - `POST   /api/v1/tenants/:id/suspend`               - Suspend a tenant
//! - `POST   /api/v1/tenants/:id/activate`              - Reactivate a tenant
//! - `GET    /api/v1/tenants/:id/topics`                - List topics in tenant namespace
//! - `GET    /api/v1/tenants/:id/api-keys`              - List API keys for tenant
//! - `POST   /api/v1/tenants/:id/api-keys`              - Create new API key
//! - `DELETE /api/v1/tenants/:id/api-keys/:key_id`      - Revoke an API key

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{delete, get, post, put},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Shared state for the tenant management API.
#[derive(Clone)]
pub struct TenantApiState {
    tenants: Arc<RwLock<HashMap<String, Tenant>>>,
}

impl TenantApiState {
    pub fn new() -> Self {
        Self {
            tenants: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl Default for TenantApiState {
    fn default() -> Self {
        Self::new()
    }
}

/// Represents a tenant in the system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tenant {
    pub id: String,
    pub name: String,
    pub namespace: String,
    pub status: TenantStatus,
    pub tier: TenantTier,
    pub quotas: TenantQuotas,
    pub usage: TenantUsage,
    pub api_keys: Vec<ApiKey>,
    pub created_at: String,
    pub updated_at: String,
    pub metadata: HashMap<String, String>,
}

/// Status of a tenant lifecycle.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum TenantStatus {
    Active,
    Suspended,
    PendingDeletion,
}

/// Billing / feature tier.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum TenantTier {
    Free,
    Pro,
    Enterprise,
    Custom,
}

/// Resource quotas assigned to a tenant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantQuotas {
    pub max_topics: u32,
    pub max_partitions: u32,
    pub max_storage_bytes: u64,
    pub max_produce_rate_bytes: u64,
    pub max_consume_rate_bytes: u64,
    pub max_connections: u32,
    pub retention_max_hours: u32,
}

impl TenantQuotas {
    /// Return the default quotas for the given tier.
    pub fn for_tier(tier: &TenantTier) -> Self {
        match tier {
            TenantTier::Free => Self {
                max_topics: 5,
                max_partitions: 15,
                max_storage_bytes: 1_073_741_824,       // 1 GB
                max_produce_rate_bytes: 1_048_576,       // 1 MB/s
                max_consume_rate_bytes: 5_242_880,       // 5 MB/s
                max_connections: 10,
                retention_max_hours: 24,
            },
            TenantTier::Pro => Self {
                max_topics: 100,
                max_partitions: 500,
                max_storage_bytes: 107_374_182_400,      // 100 GB
                max_produce_rate_bytes: 52_428_800,      // 50 MB/s
                max_consume_rate_bytes: 104_857_600,     // 100 MB/s
                max_connections: 500,
                retention_max_hours: 720,
            },
            TenantTier::Enterprise | TenantTier::Custom => Self {
                max_topics: u32::MAX,
                max_partitions: u32::MAX,
                max_storage_bytes: u64::MAX,
                max_produce_rate_bytes: u64::MAX,
                max_consume_rate_bytes: u64::MAX,
                max_connections: u32::MAX,
                retention_max_hours: u32::MAX,
            },
        }
    }
}

/// Current resource usage for a tenant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantUsage {
    pub topics_count: u32,
    pub partitions_count: u32,
    pub storage_bytes: u64,
    pub produce_rate_bytes: u64,
    pub consume_rate_bytes: u64,
    pub active_connections: u32,
    pub messages_produced_total: u64,
    pub messages_consumed_total: u64,
    pub last_activity_at: Option<String>,
}

impl Default for TenantUsage {
    fn default() -> Self {
        Self {
            topics_count: 0,
            partitions_count: 0,
            storage_bytes: 0,
            produce_rate_bytes: 0,
            consume_rate_bytes: 0,
            active_connections: 0,
            messages_produced_total: 0,
            messages_consumed_total: 0,
            last_activity_at: None,
        }
    }
}

/// An API key associated with a tenant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiKey {
    pub id: String,
    pub name: String,
    pub prefix: String,
    pub created_at: String,
    pub last_used_at: Option<String>,
    pub expires_at: Option<String>,
    pub scopes: Vec<String>,
}

// ---------------------------------------------------------------------------
// Request / Response DTOs
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct CreateTenantRequest {
    pub name: String,
    pub tier: Option<TenantTier>,
    pub quotas: Option<TenantQuotas>,
    pub metadata: Option<HashMap<String, String>>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateTenantRequest {
    pub name: Option<String>,
    pub tier: Option<TenantTier>,
    pub metadata: Option<HashMap<String, String>>,
}

#[derive(Debug, Deserialize)]
pub struct CreateApiKeyRequest {
    pub name: String,
    pub scopes: Vec<String>,
    pub expires_in_hours: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateApiKeyResponse {
    pub id: String,
    pub key: String,
    pub prefix: String,
    pub name: String,
    pub scopes: Vec<String>,
    pub expires_at: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn now_iso8601() -> String {
    chrono::Utc::now().to_rfc3339()
}

fn error_json(msg: impl Into<String>) -> Json<ErrorResponse> {
    Json(ErrorResponse {
        error: msg.into(),
    })
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

/// Build the tenant management API router.
pub fn create_tenant_api_router(state: TenantApiState) -> Router {
    Router::new()
        .route("/api/v1/tenants", post(create_tenant).get(list_tenants))
        .route(
            "/api/v1/tenants/:id",
            get(get_tenant).put(update_tenant).delete(delete_tenant),
        )
        .route("/api/v1/tenants/:id/usage", get(get_usage))
        .route(
            "/api/v1/tenants/:id/quotas",
            get(get_quotas).put(set_quotas),
        )
        .route("/api/v1/tenants/:id/suspend", post(suspend_tenant))
        .route("/api/v1/tenants/:id/activate", post(activate_tenant))
        .route("/api/v1/tenants/:id/topics", get(list_tenant_topics))
        .route(
            "/api/v1/tenants/:id/api-keys",
            get(list_api_keys).post(create_api_key),
        )
        .route(
            "/api/v1/tenants/:id/api-keys/:key_id",
            delete(revoke_api_key),
        )
        .with_state(state)
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

async fn create_tenant(
    State(state): State<TenantApiState>,
    Json(req): Json<CreateTenantRequest>,
) -> Result<(StatusCode, Json<Tenant>), (StatusCode, Json<ErrorResponse>)> {
    if req.name.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            error_json("tenant name must not be empty"),
        ));
    }

    let tier = req.tier.unwrap_or(TenantTier::Free);
    let quotas = req.quotas.unwrap_or_else(|| TenantQuotas::for_tier(&tier));
    let id = Uuid::new_v4().to_string();
    let namespace = format!("tenant-{}", &id[..8]);
    let now = now_iso8601();

    let tenant = Tenant {
        id: id.clone(),
        name: req.name,
        namespace,
        status: TenantStatus::Active,
        tier,
        quotas,
        usage: TenantUsage::default(),
        api_keys: Vec::new(),
        created_at: now.clone(),
        updated_at: now,
        metadata: req.metadata.unwrap_or_default(),
    };

    info!(tenant_id = %tenant.id, name = %tenant.name, "tenant created");
    state.tenants.write().await.insert(id, tenant.clone());
    Ok((StatusCode::CREATED, Json(tenant)))
}

async fn list_tenants(
    State(state): State<TenantApiState>,
) -> Json<Vec<Tenant>> {
    let tenants = state.tenants.read().await;
    Json(tenants.values().cloned().collect())
}

async fn get_tenant(
    State(state): State<TenantApiState>,
    Path(id): Path<String>,
) -> Result<Json<Tenant>, (StatusCode, Json<ErrorResponse>)> {
    let tenants = state.tenants.read().await;
    tenants
        .get(&id)
        .cloned()
        .map(Json)
        .ok_or_else(|| (StatusCode::NOT_FOUND, error_json("tenant not found")))
}

async fn update_tenant(
    State(state): State<TenantApiState>,
    Path(id): Path<String>,
    Json(req): Json<UpdateTenantRequest>,
) -> Result<Json<Tenant>, (StatusCode, Json<ErrorResponse>)> {
    let mut tenants = state.tenants.write().await;
    let tenant = tenants
        .get_mut(&id)
        .ok_or_else(|| (StatusCode::NOT_FOUND, error_json("tenant not found")))?;

    if let Some(name) = req.name {
        tenant.name = name;
    }
    if let Some(tier) = req.tier {
        tenant.quotas = TenantQuotas::for_tier(&tier);
        tenant.tier = tier;
    }
    if let Some(metadata) = req.metadata {
        tenant.metadata = metadata;
    }
    tenant.updated_at = now_iso8601();

    debug!(tenant_id = %id, "tenant updated");
    Ok(Json(tenant.clone()))
}

async fn delete_tenant(
    State(state): State<TenantApiState>,
    Path(id): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let mut tenants = state.tenants.write().await;
    let tenant = tenants
        .get_mut(&id)
        .ok_or_else(|| (StatusCode::NOT_FOUND, error_json("tenant not found")))?;

    tenant.status = TenantStatus::PendingDeletion;
    tenant.updated_at = now_iso8601();
    info!(tenant_id = %id, "tenant marked for deletion");

    Ok(StatusCode::NO_CONTENT)
}

async fn get_usage(
    State(state): State<TenantApiState>,
    Path(id): Path<String>,
) -> Result<Json<TenantUsage>, (StatusCode, Json<ErrorResponse>)> {
    let tenants = state.tenants.read().await;
    tenants
        .get(&id)
        .map(|t| Json(t.usage.clone()))
        .ok_or_else(|| (StatusCode::NOT_FOUND, error_json("tenant not found")))
}

async fn get_quotas(
    State(state): State<TenantApiState>,
    Path(id): Path<String>,
) -> Result<Json<TenantQuotas>, (StatusCode, Json<ErrorResponse>)> {
    let tenants = state.tenants.read().await;
    tenants
        .get(&id)
        .map(|t| Json(t.quotas.clone()))
        .ok_or_else(|| (StatusCode::NOT_FOUND, error_json("tenant not found")))
}

async fn set_quotas(
    State(state): State<TenantApiState>,
    Path(id): Path<String>,
    Json(quotas): Json<TenantQuotas>,
) -> Result<Json<TenantQuotas>, (StatusCode, Json<ErrorResponse>)> {
    let mut tenants = state.tenants.write().await;
    let tenant = tenants
        .get_mut(&id)
        .ok_or_else(|| (StatusCode::NOT_FOUND, error_json("tenant not found")))?;

    tenant.quotas = quotas;
    tenant.updated_at = now_iso8601();
    debug!(tenant_id = %id, "quotas updated");
    Ok(Json(tenant.quotas.clone()))
}

async fn suspend_tenant(
    State(state): State<TenantApiState>,
    Path(id): Path<String>,
) -> Result<Json<Tenant>, (StatusCode, Json<ErrorResponse>)> {
    let mut tenants = state.tenants.write().await;
    let tenant = tenants
        .get_mut(&id)
        .ok_or_else(|| (StatusCode::NOT_FOUND, error_json("tenant not found")))?;

    if tenant.status == TenantStatus::PendingDeletion {
        return Err((
            StatusCode::CONFLICT,
            error_json("cannot suspend a tenant pending deletion"),
        ));
    }
    tenant.status = TenantStatus::Suspended;
    tenant.updated_at = now_iso8601();
    warn!(tenant_id = %id, "tenant suspended");
    Ok(Json(tenant.clone()))
}

async fn activate_tenant(
    State(state): State<TenantApiState>,
    Path(id): Path<String>,
) -> Result<Json<Tenant>, (StatusCode, Json<ErrorResponse>)> {
    let mut tenants = state.tenants.write().await;
    let tenant = tenants
        .get_mut(&id)
        .ok_or_else(|| (StatusCode::NOT_FOUND, error_json("tenant not found")))?;

    if tenant.status == TenantStatus::PendingDeletion {
        return Err((
            StatusCode::CONFLICT,
            error_json("cannot activate a tenant pending deletion"),
        ));
    }
    tenant.status = TenantStatus::Active;
    tenant.updated_at = now_iso8601();
    info!(tenant_id = %id, "tenant activated");
    Ok(Json(tenant.clone()))
}

async fn list_tenant_topics(
    State(state): State<TenantApiState>,
    Path(id): Path<String>,
) -> Result<Json<Vec<String>>, (StatusCode, Json<ErrorResponse>)> {
    let tenants = state.tenants.read().await;
    let tenant = tenants
        .get(&id)
        .ok_or_else(|| (StatusCode::NOT_FOUND, error_json("tenant not found")))?;

    // In a real deployment the topic list would come from the storage layer
    // scoped by the tenant namespace. Return a placeholder list.
    let topics: Vec<String> = (0..tenant.usage.topics_count)
        .map(|i| format!("{}.topic-{}", tenant.namespace, i))
        .collect();

    Ok(Json(topics))
}

async fn list_api_keys(
    State(state): State<TenantApiState>,
    Path(id): Path<String>,
) -> Result<Json<Vec<ApiKey>>, (StatusCode, Json<ErrorResponse>)> {
    let tenants = state.tenants.read().await;
    tenants
        .get(&id)
        .map(|t| Json(t.api_keys.clone()))
        .ok_or_else(|| (StatusCode::NOT_FOUND, error_json("tenant not found")))
}

async fn create_api_key(
    State(state): State<TenantApiState>,
    Path(id): Path<String>,
    Json(req): Json<CreateApiKeyRequest>,
) -> Result<(StatusCode, Json<CreateApiKeyResponse>), (StatusCode, Json<ErrorResponse>)> {
    if req.name.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            error_json("api key name must not be empty"),
        ));
    }
    if req.scopes.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            error_json("at least one scope is required"),
        ));
    }

    let mut tenants = state.tenants.write().await;
    let tenant = tenants
        .get_mut(&id)
        .ok_or_else(|| (StatusCode::NOT_FOUND, error_json("tenant not found")))?;

    if tenant.status != TenantStatus::Active {
        return Err((
            StatusCode::FORBIDDEN,
            error_json("cannot create api key for non-active tenant"),
        ));
    }

    let raw_uuid = Uuid::new_v4().to_string().replace('-', "");
    let full_key = format!("sl_{raw_uuid}");
    let prefix = full_key[..8].to_string();
    let key_id = Uuid::new_v4().to_string();
    let now = now_iso8601();

    let expires_at = req.expires_in_hours.map(|h| {
        (chrono::Utc::now() + chrono::Duration::hours(h as i64)).to_rfc3339()
    });

    let api_key = ApiKey {
        id: key_id.clone(),
        name: req.name.clone(),
        prefix: prefix.clone(),
        created_at: now,
        last_used_at: None,
        expires_at: expires_at.clone(),
        scopes: req.scopes.clone(),
    };
    tenant.api_keys.push(api_key);
    tenant.updated_at = now_iso8601();

    info!(tenant_id = %id, key_id = %key_id, "api key created");

    Ok((
        StatusCode::CREATED,
        Json(CreateApiKeyResponse {
            id: key_id,
            key: full_key,
            prefix,
            name: req.name,
            scopes: req.scopes,
            expires_at,
        }),
    ))
}

async fn revoke_api_key(
    State(state): State<TenantApiState>,
    Path((id, key_id)): Path<(String, String)>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let mut tenants = state.tenants.write().await;
    let tenant = tenants
        .get_mut(&id)
        .ok_or_else(|| (StatusCode::NOT_FOUND, error_json("tenant not found")))?;

    let before = tenant.api_keys.len();
    tenant.api_keys.retain(|k| k.id != key_id);
    if tenant.api_keys.len() == before {
        return Err((
            StatusCode::NOT_FOUND,
            error_json("api key not found"),
        ));
    }
    tenant.updated_at = now_iso8601();
    info!(tenant_id = %id, key_id = %key_id, "api key revoked");
    Ok(StatusCode::NO_CONTENT)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{self, Request};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    fn app() -> Router {
        create_tenant_api_router(TenantApiState::new())
    }

    async fn body_json<T: serde::de::DeserializeOwned>(resp: axum::response::Response) -> T {
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    fn json_request(method: &str, uri: &str, body: serde_json::Value) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header(http::header::CONTENT_TYPE, "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap()
    }

    fn empty_request(method: &str, uri: &str) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .body(Body::empty())
            .unwrap()
    }

    /// Helper: create a tenant and return its id.
    async fn create_test_tenant(app: &Router) -> Tenant {
        let resp = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/api/v1/tenants",
                serde_json::json!({ "name": "acme" }),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
        body_json(resp).await
    }

    // 1
    #[tokio::test]
    async fn test_create_tenant_default_tier() {
        let tenant = create_test_tenant(&app()).await;
        assert_eq!(tenant.name, "acme");
        assert_eq!(tenant.tier, TenantTier::Free);
        assert_eq!(tenant.status, TenantStatus::Active);
        assert!(tenant.namespace.starts_with("tenant-"));
    }

    // 2
    #[tokio::test]
    async fn test_create_tenant_with_pro_tier() {
        let resp = app()
            .oneshot(json_request(
                "POST",
                "/api/v1/tenants",
                serde_json::json!({ "name": "bigco", "tier": "pro" }),
            ))
            .await
            .unwrap();
        let t: Tenant = body_json(resp).await;
        assert_eq!(t.tier, TenantTier::Pro);
        assert_eq!(t.quotas.max_topics, 100);
    }

    // 3
    #[tokio::test]
    async fn test_create_tenant_empty_name_rejected() {
        let resp = app()
            .oneshot(json_request(
                "POST",
                "/api/v1/tenants",
                serde_json::json!({ "name": "  " }),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    // 4
    #[tokio::test]
    async fn test_list_tenants_empty() {
        let resp = app()
            .oneshot(empty_request("GET", "/api/v1/tenants"))
            .await
            .unwrap();
        let list: Vec<Tenant> = body_json(resp).await;
        assert!(list.is_empty());
    }

    // 5
    #[tokio::test]
    async fn test_list_tenants_returns_created() {
        let state = TenantApiState::new();
        let router = create_tenant_api_router(state);

        let resp = router
            .clone()
            .oneshot(json_request(
                "POST",
                "/api/v1/tenants",
                serde_json::json!({ "name": "t1" }),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);

        let resp = router
            .oneshot(empty_request("GET", "/api/v1/tenants"))
            .await
            .unwrap();
        let list: Vec<Tenant> = body_json(resp).await;
        assert_eq!(list.len(), 1);
    }

    // 6
    #[tokio::test]
    async fn test_get_tenant_by_id() {
        let state = TenantApiState::new();
        let router = create_tenant_api_router(state);
        let tenant = {
            let resp = router
                .clone()
                .oneshot(json_request(
                    "POST",
                    "/api/v1/tenants",
                    serde_json::json!({ "name": "acme" }),
                ))
                .await
                .unwrap();
            body_json::<Tenant>(resp).await
        };

        let resp = router
            .oneshot(empty_request(
                "GET",
                &format!("/api/v1/tenants/{}", tenant.id),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let fetched: Tenant = body_json(resp).await;
        assert_eq!(fetched.id, tenant.id);
    }

    // 7
    #[tokio::test]
    async fn test_get_tenant_not_found() {
        let resp = app()
            .oneshot(empty_request("GET", "/api/v1/tenants/nonexistent"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    // 8
    #[tokio::test]
    async fn test_update_tenant_name() {
        let state = TenantApiState::new();
        let router = create_tenant_api_router(state);
        let tenant = {
            let resp = router
                .clone()
                .oneshot(json_request(
                    "POST",
                    "/api/v1/tenants",
                    serde_json::json!({ "name": "old" }),
                ))
                .await
                .unwrap();
            body_json::<Tenant>(resp).await
        };

        let resp = router
            .oneshot(json_request(
                "PUT",
                &format!("/api/v1/tenants/{}", tenant.id),
                serde_json::json!({ "name": "new" }),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let updated: Tenant = body_json(resp).await;
        assert_eq!(updated.name, "new");
    }

    // 9
    #[tokio::test]
    async fn test_update_tenant_tier_updates_quotas() {
        let state = TenantApiState::new();
        let router = create_tenant_api_router(state);
        let tenant = {
            let resp = router
                .clone()
                .oneshot(json_request(
                    "POST",
                    "/api/v1/tenants",
                    serde_json::json!({ "name": "t" }),
                ))
                .await
                .unwrap();
            body_json::<Tenant>(resp).await
        };
        assert_eq!(tenant.quotas.max_topics, 5); // Free tier

        let resp = router
            .oneshot(json_request(
                "PUT",
                &format!("/api/v1/tenants/{}", tenant.id),
                serde_json::json!({ "tier": "enterprise" }),
            ))
            .await
            .unwrap();
        let updated: Tenant = body_json(resp).await;
        assert_eq!(updated.tier, TenantTier::Enterprise);
        assert_eq!(updated.quotas.max_topics, u32::MAX);
    }

    // 10
    #[tokio::test]
    async fn test_delete_tenant_marks_pending() {
        let state = TenantApiState::new();
        let router = create_tenant_api_router(state);
        let tenant = {
            let resp = router
                .clone()
                .oneshot(json_request(
                    "POST",
                    "/api/v1/tenants",
                    serde_json::json!({ "name": "doomed" }),
                ))
                .await
                .unwrap();
            body_json::<Tenant>(resp).await
        };

        let resp = router
            .clone()
            .oneshot(empty_request(
                "DELETE",
                &format!("/api/v1/tenants/{}", tenant.id),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);

        let resp = router
            .oneshot(empty_request(
                "GET",
                &format!("/api/v1/tenants/{}", tenant.id),
            ))
            .await
            .unwrap();
        let t: Tenant = body_json(resp).await;
        assert_eq!(t.status, TenantStatus::PendingDeletion);
    }

    // 11
    #[tokio::test]
    async fn test_get_usage() {
        let state = TenantApiState::new();
        let router = create_tenant_api_router(state);
        let tenant = {
            let resp = router
                .clone()
                .oneshot(json_request(
                    "POST",
                    "/api/v1/tenants",
                    serde_json::json!({ "name": "u" }),
                ))
                .await
                .unwrap();
            body_json::<Tenant>(resp).await
        };

        let resp = router
            .oneshot(empty_request(
                "GET",
                &format!("/api/v1/tenants/{}/usage", tenant.id),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let usage: TenantUsage = body_json(resp).await;
        assert_eq!(usage.topics_count, 0);
    }

    // 12
    #[tokio::test]
    async fn test_set_and_get_quotas() {
        let state = TenantApiState::new();
        let router = create_tenant_api_router(state);
        let tenant = {
            let resp = router
                .clone()
                .oneshot(json_request(
                    "POST",
                    "/api/v1/tenants",
                    serde_json::json!({ "name": "q" }),
                ))
                .await
                .unwrap();
            body_json::<Tenant>(resp).await
        };

        let custom = serde_json::json!({
            "max_topics": 42,
            "max_partitions": 100,
            "max_storage_bytes": 999,
            "max_produce_rate_bytes": 111,
            "max_consume_rate_bytes": 222,
            "max_connections": 50,
            "retention_max_hours": 48
        });

        let resp = router
            .clone()
            .oneshot(json_request(
                "PUT",
                &format!("/api/v1/tenants/{}/quotas", tenant.id),
                custom,
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let resp = router
            .oneshot(empty_request(
                "GET",
                &format!("/api/v1/tenants/{}/quotas", tenant.id),
            ))
            .await
            .unwrap();
        let q: TenantQuotas = body_json(resp).await;
        assert_eq!(q.max_topics, 42);
        assert_eq!(q.retention_max_hours, 48);
    }

    // 13
    #[tokio::test]
    async fn test_suspend_tenant() {
        let state = TenantApiState::new();
        let router = create_tenant_api_router(state);
        let tenant = {
            let resp = router
                .clone()
                .oneshot(json_request(
                    "POST",
                    "/api/v1/tenants",
                    serde_json::json!({ "name": "s" }),
                ))
                .await
                .unwrap();
            body_json::<Tenant>(resp).await
        };

        let resp = router
            .oneshot(empty_request(
                "POST",
                &format!("/api/v1/tenants/{}/suspend", tenant.id),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let t: Tenant = body_json(resp).await;
        assert_eq!(t.status, TenantStatus::Suspended);
    }

    // 14
    #[tokio::test]
    async fn test_activate_tenant() {
        let state = TenantApiState::new();
        let router = create_tenant_api_router(state);
        let tenant = {
            let resp = router
                .clone()
                .oneshot(json_request(
                    "POST",
                    "/api/v1/tenants",
                    serde_json::json!({ "name": "a" }),
                ))
                .await
                .unwrap();
            body_json::<Tenant>(resp).await
        };

        // Suspend first
        router
            .clone()
            .oneshot(empty_request(
                "POST",
                &format!("/api/v1/tenants/{}/suspend", tenant.id),
            ))
            .await
            .unwrap();

        // Activate
        let resp = router
            .oneshot(empty_request(
                "POST",
                &format!("/api/v1/tenants/{}/activate", tenant.id),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let t: Tenant = body_json(resp).await;
        assert_eq!(t.status, TenantStatus::Active);
    }

    // 15
    #[tokio::test]
    async fn test_suspend_pending_deletion_conflict() {
        let state = TenantApiState::new();
        let router = create_tenant_api_router(state);
        let tenant = {
            let resp = router
                .clone()
                .oneshot(json_request(
                    "POST",
                    "/api/v1/tenants",
                    serde_json::json!({ "name": "c" }),
                ))
                .await
                .unwrap();
            body_json::<Tenant>(resp).await
        };

        // Delete (marks PendingDeletion)
        router
            .clone()
            .oneshot(empty_request(
                "DELETE",
                &format!("/api/v1/tenants/{}", tenant.id),
            ))
            .await
            .unwrap();

        let resp = router
            .oneshot(empty_request(
                "POST",
                &format!("/api/v1/tenants/{}/suspend", tenant.id),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CONFLICT);
    }

    // 16
    #[tokio::test]
    async fn test_activate_pending_deletion_conflict() {
        let state = TenantApiState::new();
        let router = create_tenant_api_router(state);
        let tenant = {
            let resp = router
                .clone()
                .oneshot(json_request(
                    "POST",
                    "/api/v1/tenants",
                    serde_json::json!({ "name": "c2" }),
                ))
                .await
                .unwrap();
            body_json::<Tenant>(resp).await
        };

        router
            .clone()
            .oneshot(empty_request(
                "DELETE",
                &format!("/api/v1/tenants/{}", tenant.id),
            ))
            .await
            .unwrap();

        let resp = router
            .oneshot(empty_request(
                "POST",
                &format!("/api/v1/tenants/{}/activate", tenant.id),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CONFLICT);
    }

    // 17
    #[tokio::test]
    async fn test_list_tenant_topics() {
        let state = TenantApiState::new();
        let router = create_tenant_api_router(state);
        let tenant = {
            let resp = router
                .clone()
                .oneshot(json_request(
                    "POST",
                    "/api/v1/tenants",
                    serde_json::json!({ "name": "tp" }),
                ))
                .await
                .unwrap();
            body_json::<Tenant>(resp).await
        };

        let resp = router
            .oneshot(empty_request(
                "GET",
                &format!("/api/v1/tenants/{}/topics", tenant.id),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let topics: Vec<String> = body_json(resp).await;
        assert!(topics.is_empty()); // no usage yet
    }

    // 18
    #[tokio::test]
    async fn test_create_api_key() {
        let state = TenantApiState::new();
        let router = create_tenant_api_router(state);
        let tenant = {
            let resp = router
                .clone()
                .oneshot(json_request(
                    "POST",
                    "/api/v1/tenants",
                    serde_json::json!({ "name": "k" }),
                ))
                .await
                .unwrap();
            body_json::<Tenant>(resp).await
        };

        let resp = router
            .oneshot(json_request(
                "POST",
                &format!("/api/v1/tenants/{}/api-keys", tenant.id),
                serde_json::json!({
                    "name": "prod-key",
                    "scopes": ["produce", "consume"]
                }),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
        let key_resp: CreateApiKeyResponse = body_json(resp).await;
        assert!(key_resp.key.starts_with("sl_"));
        assert_eq!(key_resp.prefix.len(), 8);
        assert_eq!(key_resp.scopes, vec!["produce", "consume"]);
    }

    // 19
    #[tokio::test]
    async fn test_create_api_key_empty_name_rejected() {
        let state = TenantApiState::new();
        let router = create_tenant_api_router(state);
        let tenant = {
            let resp = router
                .clone()
                .oneshot(json_request(
                    "POST",
                    "/api/v1/tenants",
                    serde_json::json!({ "name": "k2" }),
                ))
                .await
                .unwrap();
            body_json::<Tenant>(resp).await
        };

        let resp = router
            .oneshot(json_request(
                "POST",
                &format!("/api/v1/tenants/{}/api-keys", tenant.id),
                serde_json::json!({ "name": "", "scopes": ["admin"] }),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    // 20
    #[tokio::test]
    async fn test_create_api_key_empty_scopes_rejected() {
        let state = TenantApiState::new();
        let router = create_tenant_api_router(state);
        let tenant = {
            let resp = router
                .clone()
                .oneshot(json_request(
                    "POST",
                    "/api/v1/tenants",
                    serde_json::json!({ "name": "k3" }),
                ))
                .await
                .unwrap();
            body_json::<Tenant>(resp).await
        };

        let resp = router
            .oneshot(json_request(
                "POST",
                &format!("/api/v1/tenants/{}/api-keys", tenant.id),
                serde_json::json!({ "name": "x", "scopes": [] }),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    // 21
    #[tokio::test]
    async fn test_list_api_keys() {
        let state = TenantApiState::new();
        let router = create_tenant_api_router(state);
        let tenant = {
            let resp = router
                .clone()
                .oneshot(json_request(
                    "POST",
                    "/api/v1/tenants",
                    serde_json::json!({ "name": "lk" }),
                ))
                .await
                .unwrap();
            body_json::<Tenant>(resp).await
        };

        // Create two keys
        for name in ["k1", "k2"] {
            router
                .clone()
                .oneshot(json_request(
                    "POST",
                    &format!("/api/v1/tenants/{}/api-keys", tenant.id),
                    serde_json::json!({ "name": name, "scopes": ["produce"] }),
                ))
                .await
                .unwrap();
        }

        let resp = router
            .oneshot(empty_request(
                "GET",
                &format!("/api/v1/tenants/{}/api-keys", tenant.id),
            ))
            .await
            .unwrap();
        let keys: Vec<ApiKey> = body_json(resp).await;
        assert_eq!(keys.len(), 2);
    }

    // 22
    #[tokio::test]
    async fn test_revoke_api_key() {
        let state = TenantApiState::new();
        let router = create_tenant_api_router(state);
        let tenant = {
            let resp = router
                .clone()
                .oneshot(json_request(
                    "POST",
                    "/api/v1/tenants",
                    serde_json::json!({ "name": "rk" }),
                ))
                .await
                .unwrap();
            body_json::<Tenant>(resp).await
        };

        let key_resp: CreateApiKeyResponse = {
            let resp = router
                .clone()
                .oneshot(json_request(
                    "POST",
                    &format!("/api/v1/tenants/{}/api-keys", tenant.id),
                    serde_json::json!({ "name": "del", "scopes": ["admin"] }),
                ))
                .await
                .unwrap();
            body_json(resp).await
        };

        let resp = router
            .clone()
            .oneshot(empty_request(
                "DELETE",
                &format!(
                    "/api/v1/tenants/{}/api-keys/{}",
                    tenant.id, key_resp.id
                ),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);

        // Verify gone
        let resp = router
            .oneshot(empty_request(
                "GET",
                &format!("/api/v1/tenants/{}/api-keys", tenant.id),
            ))
            .await
            .unwrap();
        let keys: Vec<ApiKey> = body_json(resp).await;
        assert!(keys.is_empty());
    }

    // 23
    #[tokio::test]
    async fn test_revoke_api_key_not_found() {
        let state = TenantApiState::new();
        let router = create_tenant_api_router(state);
        let tenant = {
            let resp = router
                .clone()
                .oneshot(json_request(
                    "POST",
                    "/api/v1/tenants",
                    serde_json::json!({ "name": "rnf" }),
                ))
                .await
                .unwrap();
            body_json::<Tenant>(resp).await
        };

        let resp = router
            .oneshot(empty_request(
                "DELETE",
                &format!("/api/v1/tenants/{}/api-keys/bad-id", tenant.id),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    // 24
    #[tokio::test]
    async fn test_create_api_key_on_suspended_tenant_forbidden() {
        let state = TenantApiState::new();
        let router = create_tenant_api_router(state);
        let tenant = {
            let resp = router
                .clone()
                .oneshot(json_request(
                    "POST",
                    "/api/v1/tenants",
                    serde_json::json!({ "name": "sus" }),
                ))
                .await
                .unwrap();
            body_json::<Tenant>(resp).await
        };

        router
            .clone()
            .oneshot(empty_request(
                "POST",
                &format!("/api/v1/tenants/{}/suspend", tenant.id),
            ))
            .await
            .unwrap();

        let resp = router
            .oneshot(json_request(
                "POST",
                &format!("/api/v1/tenants/{}/api-keys", tenant.id),
                serde_json::json!({ "name": "x", "scopes": ["produce"] }),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    // 25
    #[tokio::test]
    async fn test_create_tenant_with_custom_quotas() {
        let resp = app()
            .oneshot(json_request(
                "POST",
                "/api/v1/tenants",
                serde_json::json!({
                    "name": "custom",
                    "tier": "custom",
                    "quotas": {
                        "max_topics": 77,
                        "max_partitions": 200,
                        "max_storage_bytes": 5000,
                        "max_produce_rate_bytes": 1000,
                        "max_consume_rate_bytes": 2000,
                        "max_connections": 30,
                        "retention_max_hours": 168
                    }
                }),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
        let t: Tenant = body_json(resp).await;
        assert_eq!(t.quotas.max_topics, 77);
        assert_eq!(t.quotas.retention_max_hours, 168);
    }

    // 26
    #[tokio::test]
    async fn test_create_tenant_with_metadata() {
        let resp = app()
            .oneshot(json_request(
                "POST",
                "/api/v1/tenants",
                serde_json::json!({
                    "name": "meta",
                    "metadata": { "team": "platform", "env": "staging" }
                }),
            ))
            .await
            .unwrap();
        let t: Tenant = body_json(resp).await;
        assert_eq!(t.metadata.get("team").unwrap(), "platform");
        assert_eq!(t.metadata.get("env").unwrap(), "staging");
    }

    // 27
    #[tokio::test]
    async fn test_create_api_key_with_expiry() {
        let state = TenantApiState::new();
        let router = create_tenant_api_router(state);
        let tenant = {
            let resp = router
                .clone()
                .oneshot(json_request(
                    "POST",
                    "/api/v1/tenants",
                    serde_json::json!({ "name": "exp" }),
                ))
                .await
                .unwrap();
            body_json::<Tenant>(resp).await
        };

        let resp = router
            .oneshot(json_request(
                "POST",
                &format!("/api/v1/tenants/{}/api-keys", tenant.id),
                serde_json::json!({
                    "name": "tmp",
                    "scopes": ["produce"],
                    "expires_in_hours": 24
                }),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
        let key: CreateApiKeyResponse = body_json(resp).await;
        assert!(key.expires_at.is_some());
    }

    // 28
    #[tokio::test]
    async fn test_quotas_for_tier_free() {
        let q = TenantQuotas::for_tier(&TenantTier::Free);
        assert_eq!(q.max_topics, 5);
        assert_eq!(q.max_partitions, 15);
        assert_eq!(q.max_connections, 10);
        assert_eq!(q.retention_max_hours, 24);
        assert_eq!(q.max_storage_bytes, 1_073_741_824);
    }

    // 29
    #[tokio::test]
    async fn test_quotas_for_tier_pro() {
        let q = TenantQuotas::for_tier(&TenantTier::Pro);
        assert_eq!(q.max_topics, 100);
        assert_eq!(q.max_partitions, 500);
        assert_eq!(q.max_connections, 500);
        assert_eq!(q.retention_max_hours, 720);
    }

    // 30
    #[tokio::test]
    async fn test_quotas_for_tier_enterprise() {
        let q = TenantQuotas::for_tier(&TenantTier::Enterprise);
        assert_eq!(q.max_topics, u32::MAX);
        assert_eq!(q.max_connections, u32::MAX);
    }

    // 31
    #[tokio::test]
    async fn test_delete_nonexistent_tenant() {
        let resp = app()
            .oneshot(empty_request("DELETE", "/api/v1/tenants/nope"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    // 32
    #[tokio::test]
    async fn test_update_nonexistent_tenant() {
        let resp = app()
            .oneshot(json_request(
                "PUT",
                "/api/v1/tenants/nope",
                serde_json::json!({ "name": "x" }),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
}
