//! Cloud Management HTTP API
//!
//! Provides HTTP/REST endpoints for cloud self-service management including
//! tenant registration, endpoint provisioning, scaling, and billing.
//!
//! ## Endpoints
//!
//! - `POST /cloud/v1/tenants` - Register a new tenant
//! - `POST /cloud/v1/endpoints` - Create a new cloud endpoint
//! - `GET /cloud/v1/endpoints/:id` - Get endpoint details
//! - `GET /cloud/v1/tenants/:tenant_id/endpoints` - List tenant endpoints
//! - `DELETE /cloud/v1/endpoints/:id` - Delete an endpoint
//! - `PUT /cloud/v1/endpoints/:id/scale` - Scale an endpoint
//! - `GET /cloud/v1/tenants/:tenant_id/usage` - Get usage summary
//! - `GET /cloud/v1/tenants/:tenant_id/invoice` - Get current invoice
//! - `GET /cloud/v1/health` - Cloud platform health check

use crate::cloud::{ClusterSize, StreamlineCloud};
use crate::multitenancy::TenantTier;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, get, post, put},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{error, info};

/// Shared state for the Cloud API
#[derive(Clone)]
pub(crate) struct CloudApiState {
    /// Streamline Cloud control plane
    pub cloud: Arc<StreamlineCloud>,
    /// Cloud configuration
    #[allow(dead_code)]
    pub config: crate::cloud::CloudConfig,
}

// ---------------------------------------------------------------------------
// Request types
// ---------------------------------------------------------------------------

/// Register tenant request
#[derive(Debug, Deserialize)]
pub struct RegisterTenantRequest {
    /// Tenant name
    pub name: String,
    /// Contact email
    pub email: String,
    /// Billing tier
    pub tier: TenantTier,
}

/// Create endpoint request
#[derive(Debug, Deserialize)]
pub struct CreateEndpointRequest {
    /// Tenant that owns the endpoint
    pub tenant_id: String,
    /// Optional cluster size (defaults based on tenant tier)
    pub size: Option<ClusterSize>,
}

/// Scale endpoint request
#[derive(Debug, Deserialize)]
pub struct ScaleEndpointRequest {
    /// New cluster size
    pub new_size: ClusterSize,
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/// Error response body
#[derive(Debug, Serialize)]
pub struct CloudErrorResponse {
    /// Machine-readable error code
    pub error: String,
    /// Human-readable message
    pub message: String,
}

impl CloudErrorResponse {
    fn new(error: &str, message: impl Into<String>) -> Self {
        Self {
            error: error.to_string(),
            message: message.into(),
        }
    }
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

/// Create the Cloud API router
pub(crate) fn create_cloud_api_router(state: CloudApiState) -> Router {
    Router::new()
        .route("/cloud/v1/health", get(health_check))
        .route("/cloud/v1/tenants", post(register_tenant))
        .route(
            "/cloud/v1/tenants/:tenant_id/endpoints",
            get(list_tenant_endpoints),
        )
        .route("/cloud/v1/tenants/:tenant_id/usage", get(get_tenant_usage))
        .route(
            "/cloud/v1/tenants/:tenant_id/invoice",
            get(get_tenant_invoice),
        )
        .route("/cloud/v1/endpoints", post(create_endpoint))
        .route("/cloud/v1/endpoints/:id", get(get_endpoint))
        .route("/cloud/v1/endpoints/:id", delete(delete_endpoint))
        .route("/cloud/v1/endpoints/:id/scale", put(scale_endpoint))
        // Control Plane: Organizations
        .route(
            "/cloud/v1/organizations",
            get(list_organizations).post(create_organization),
        )
        .route(
            "/cloud/v1/organizations/:org_id",
            get(get_organization).delete(delete_organization),
        )
        // Control Plane: Projects
        .route(
            "/cloud/v1/organizations/:org_id/projects",
            get(list_projects).post(create_project),
        )
        .route(
            "/cloud/v1/projects/:project_id",
            get(get_project).delete(delete_project),
        )
        // Control Plane: Clusters
        .route(
            "/cloud/v1/projects/:project_id/clusters",
            get(list_clusters).post(provision_cluster),
        )
        .route(
            "/cloud/v1/clusters/:cluster_id",
            get(get_cluster).delete(delete_cluster),
        )
        .route("/cloud/v1/clusters/:cluster_id/scale", put(scale_cluster))
        .with_state(state)
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// Cloud platform health check
async fn health_check() -> Response {
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "status": "healthy",
            "service": "streamline-cloud"
        })),
    )
        .into_response()
}

/// Register a new tenant
async fn register_tenant(
    State(state): State<CloudApiState>,
    Json(request): Json<RegisterTenantRequest>,
) -> Response {
    if request.name.is_empty() || request.email.is_empty() {
        let err = CloudErrorResponse::new("INVALID_REQUEST", "name and email are required");
        return (StatusCode::BAD_REQUEST, Json(err)).into_response();
    }

    match state
        .cloud
        .register_tenant(&request.name, &request.email, request.tier)
        .await
    {
        Ok(tenant) => {
            info!(tenant_id = %tenant.id, name = %tenant.name, "Tenant registered via Cloud API");
            (
                StatusCode::CREATED,
                Json(serde_json::json!({
                    "id": tenant.id,
                    "name": tenant.name,
                    "email": tenant.contact_email,
                    "tier": tenant.config.tier,
                    "status": tenant.status,
                    "created_at": tenant.created_at,
                })),
            )
                .into_response()
        }
        Err(e) => {
            error!(error = %e, "Failed to register tenant");
            let err = CloudErrorResponse::new("TENANT_REGISTRATION_FAILED", e.to_string());
            (StatusCode::INTERNAL_SERVER_ERROR, Json(err)).into_response()
        }
    }
}

/// Create a new cloud endpoint
async fn create_endpoint(
    State(state): State<CloudApiState>,
    Json(request): Json<CreateEndpointRequest>,
) -> Response {
    if request.tenant_id.is_empty() {
        let err = CloudErrorResponse::new("INVALID_REQUEST", "tenant_id is required");
        return (StatusCode::BAD_REQUEST, Json(err)).into_response();
    }

    match state
        .cloud
        .create_endpoint(&request.tenant_id, request.size)
        .await
    {
        Ok(endpoint) => {
            info!(endpoint_id = %endpoint.id, tenant_id = %endpoint.tenant_id, "Endpoint created via Cloud API");
            (StatusCode::CREATED, Json(serde_json::json!(endpoint))).into_response()
        }
        Err(e) => {
            error!(error = %e, "Failed to create endpoint");
            let msg = e.to_string();
            if msg.contains("not found") {
                let err = CloudErrorResponse::new("TENANT_NOT_FOUND", msg);
                (StatusCode::NOT_FOUND, Json(err)).into_response()
            } else if msg.contains("Maximum clusters") {
                let err = CloudErrorResponse::new("CLUSTER_LIMIT_REACHED", msg);
                (StatusCode::CONFLICT, Json(err)).into_response()
            } else {
                let err = CloudErrorResponse::new("ENDPOINT_CREATION_FAILED", msg);
                (StatusCode::INTERNAL_SERVER_ERROR, Json(err)).into_response()
            }
        }
    }
}

/// Get endpoint details
async fn get_endpoint(State(state): State<CloudApiState>, Path(id): Path<String>) -> Response {
    match state.cloud.get_endpoint(&id).await {
        Some(endpoint) => (StatusCode::OK, Json(serde_json::json!(endpoint))).into_response(),
        None => {
            let err = CloudErrorResponse::new(
                "ENDPOINT_NOT_FOUND",
                format!("Endpoint not found: {}", id),
            );
            (StatusCode::NOT_FOUND, Json(err)).into_response()
        }
    }
}

/// List all endpoints for a tenant
async fn list_tenant_endpoints(
    State(state): State<CloudApiState>,
    Path(tenant_id): Path<String>,
) -> Response {
    let endpoints = state.cloud.list_endpoints(&tenant_id).await;
    let count = endpoints.len();
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "tenant_id": tenant_id,
            "endpoints": endpoints,
            "count": count,
        })),
    )
        .into_response()
}

/// Delete an endpoint
async fn delete_endpoint(State(state): State<CloudApiState>, Path(id): Path<String>) -> Response {
    match state.cloud.delete_endpoint(&id).await {
        Ok(()) => {
            info!(endpoint_id = %id, "Endpoint deleted via Cloud API");
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "id": id,
                    "status": "terminated",
                })),
            )
                .into_response()
        }
        Err(e) => {
            error!(error = %e, "Failed to delete endpoint");
            let msg = e.to_string();
            if msg.contains("not found") {
                let err = CloudErrorResponse::new("ENDPOINT_NOT_FOUND", msg);
                (StatusCode::NOT_FOUND, Json(err)).into_response()
            } else {
                let err = CloudErrorResponse::new("ENDPOINT_DELETION_FAILED", msg);
                (StatusCode::INTERNAL_SERVER_ERROR, Json(err)).into_response()
            }
        }
    }
}

/// Scale an endpoint to a new size
async fn scale_endpoint(
    State(state): State<CloudApiState>,
    Path(id): Path<String>,
    Json(request): Json<ScaleEndpointRequest>,
) -> Response {
    match state.cloud.scale_endpoint(&id, request.new_size).await {
        Ok(endpoint) => {
            info!(endpoint_id = %id, new_size = ?request.new_size, "Endpoint scaled via Cloud API");
            (StatusCode::OK, Json(serde_json::json!(endpoint))).into_response()
        }
        Err(e) => {
            error!(error = %e, "Failed to scale endpoint");
            let msg = e.to_string();
            if msg.contains("not found") {
                let err = CloudErrorResponse::new("ENDPOINT_NOT_FOUND", msg);
                (StatusCode::NOT_FOUND, Json(err)).into_response()
            } else {
                let err = CloudErrorResponse::new("ENDPOINT_SCALING_FAILED", msg);
                (StatusCode::INTERNAL_SERVER_ERROR, Json(err)).into_response()
            }
        }
    }
}

/// Get usage summary for a tenant
async fn get_tenant_usage(
    State(state): State<CloudApiState>,
    Path(tenant_id): Path<String>,
) -> Response {
    match state.cloud.get_usage(&tenant_id).await {
        Ok(usage) => (StatusCode::OK, Json(serde_json::json!(usage))).into_response(),
        Err(e) => {
            error!(error = %e, tenant_id = %tenant_id, "Failed to get usage");
            let err = CloudErrorResponse::new("USAGE_FETCH_FAILED", e.to_string());
            (StatusCode::INTERNAL_SERVER_ERROR, Json(err)).into_response()
        }
    }
}

/// Get current invoice for a tenant
async fn get_tenant_invoice(
    State(state): State<CloudApiState>,
    Path(tenant_id): Path<String>,
) -> Response {
    match state.cloud.get_invoice(&tenant_id).await {
        Ok(invoice) => (StatusCode::OK, Json(serde_json::json!(invoice))).into_response(),
        Err(e) => {
            error!(error = %e, tenant_id = %tenant_id, "Failed to get invoice");
            let err = CloudErrorResponse::new("INVOICE_FETCH_FAILED", e.to_string());
            (StatusCode::INTERNAL_SERVER_ERROR, Json(err)).into_response()
        }
    }
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// Control Plane Handlers
// ---------------------------------------------------------------------------

async fn list_organizations(State(state): State<CloudApiState>) -> Response {
    let orgs = state
        .cloud
        .control_plane()
        .list_organizations(Default::default())
        .await;
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "organizations": orgs.iter().map(|o| serde_json::json!({
                "id": o.id,
                "name": o.name,
                "status": format!("{:?}", o.status),
            })).collect::<Vec<_>>(),
            "total": orgs.len()
        })),
    )
        .into_response()
}

async fn create_organization(
    State(state): State<CloudApiState>,
    Json(req): Json<serde_json::Value>,
) -> Response {
    let name = req
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let owner_email = req
        .get("owner_email")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    if name.is_empty() || owner_email.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(CloudErrorResponse::new(
                "INVALID_REQUEST",
                "name and owner_email are required",
            )),
        )
            .into_response();
    }

    // Validate name length
    if name.len() > 128 {
        return (
            StatusCode::BAD_REQUEST,
            Json(CloudErrorResponse::new(
                "INVALID_NAME",
                "organization name must be <= 128 characters",
            )),
        )
            .into_response();
    }

    // Validate email format (basic check)
    if !owner_email.contains('@') || !owner_email.contains('.') {
        return (
            StatusCode::BAD_REQUEST,
            Json(CloudErrorResponse::new(
                "INVALID_EMAIL",
                "owner_email must be a valid email address",
            )),
        )
            .into_response();
    }

    let request = crate::cloud::CreateOrganizationRequest {
        name: name.clone(),
        slug: name.to_lowercase().replace(' ', "-"),
        owner_id: owner_email,
        plan: crate::cloud::OrgBillingPlan::default(),
        settings: None,
        metadata: None,
    };
    match state
        .cloud
        .control_plane()
        .create_organization(request)
        .await
    {
        Ok(org) => (
            StatusCode::CREATED,
            Json(serde_json::json!({
                "id": org.id,
                "name": org.name,
                "status": format!("{:?}", org.status),
            })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(CloudErrorResponse::new("CREATE_FAILED", e.to_string())),
        )
            .into_response(),
    }
}

async fn get_organization(
    State(state): State<CloudApiState>,
    Path(org_id): Path<String>,
) -> Response {
    match state.cloud.control_plane().get_organization(&org_id).await {
        Some(org) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "id": org.id,
                "name": org.name,
                "status": format!("{:?}", org.status),
            })),
        )
            .into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(CloudErrorResponse::new(
                "NOT_FOUND",
                format!("Organization '{}' not found", org_id),
            )),
        )
            .into_response(),
    }
}

async fn delete_organization(
    State(state): State<CloudApiState>,
    Path(org_id): Path<String>,
) -> Response {
    match state
        .cloud
        .control_plane()
        .delete_organization(&org_id)
        .await
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (
            StatusCode::NOT_FOUND,
            Json(CloudErrorResponse::new("DELETE_FAILED", e.to_string())),
        )
            .into_response(),
    }
}

async fn list_projects(State(state): State<CloudApiState>, Path(org_id): Path<String>) -> Response {
    let projects = state.cloud.control_plane().list_projects(&org_id).await;
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "projects": projects.iter().map(|p| serde_json::json!({
                "id": p.id,
                "name": p.name,
                "org_id": p.org_id,
                "status": format!("{:?}", p.status),
            })).collect::<Vec<_>>(),
            "total": projects.len()
        })),
    )
        .into_response()
}

async fn create_project(
    State(state): State<CloudApiState>,
    Path(org_id): Path<String>,
    Json(req): Json<serde_json::Value>,
) -> Response {
    let name = req
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    if name.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(CloudErrorResponse::new(
                "INVALID_REQUEST",
                "name is required",
            )),
        )
            .into_response();
    }
    let request = crate::cloud::CreateProjectRequest {
        name: name.clone(),
        org_id,
        slug: name.to_lowercase().replace(' ', "-"),
        region: req
            .get("region")
            .and_then(|v| v.as_str())
            .unwrap_or("us-east-1")
            .to_string(),
        settings: None,
        metadata: None,
    };
    match state.cloud.control_plane().create_project(request).await {
        Ok(project) => (
            StatusCode::CREATED,
            Json(serde_json::json!({
                "id": project.id,
                "name": project.name,
                "org_id": project.org_id,
            })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(CloudErrorResponse::new("CREATE_FAILED", e.to_string())),
        )
            .into_response(),
    }
}

async fn get_project(
    State(state): State<CloudApiState>,
    Path(project_id): Path<String>,
) -> Response {
    match state.cloud.control_plane().get_project(&project_id).await {
        Some(project) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "id": project.id,
                "name": project.name,
                "org_id": project.org_id,
                "status": format!("{:?}", project.status),
            })),
        )
            .into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(CloudErrorResponse::new(
                "NOT_FOUND",
                format!("Project '{}' not found", project_id),
            )),
        )
            .into_response(),
    }
}

async fn delete_project(
    State(state): State<CloudApiState>,
    Path(project_id): Path<String>,
) -> Response {
    match state
        .cloud
        .control_plane()
        .delete_project(&project_id)
        .await
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (
            StatusCode::NOT_FOUND,
            Json(CloudErrorResponse::new("DELETE_FAILED", e.to_string())),
        )
            .into_response(),
    }
}

async fn list_clusters(
    State(state): State<CloudApiState>,
    Path(project_id): Path<String>,
) -> Response {
    let clusters = state.cloud.control_plane().list_clusters(&project_id).await;
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "clusters": clusters.iter().map(|c| serde_json::json!({
                "id": c.id,
                "name": c.name,
                "status": format!("{:?}", c.status),
                "tier": format!("{:?}", c.tier),
            })).collect::<Vec<_>>(),
            "total": clusters.len()
        })),
    )
        .into_response()
}

async fn provision_cluster(
    State(state): State<CloudApiState>,
    Path(project_id): Path<String>,
    Json(req): Json<serde_json::Value>,
) -> Response {
    let name = req
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    if name.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(CloudErrorResponse::new(
                "INVALID_REQUEST",
                "name is required",
            )),
        )
            .into_response();
    }

    // Validate cluster name format
    if name.len() > 63 || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-') {
        return (
            StatusCode::BAD_REQUEST,
            Json(CloudErrorResponse::new(
                "INVALID_NAME",
                "name must be <= 63 chars, alphanumeric and hyphens only",
            )),
        )
            .into_response();
    }
    if name.starts_with('-') || name.ends_with('-') {
        return (
            StatusCode::BAD_REQUEST,
            Json(CloudErrorResponse::new(
                "INVALID_NAME",
                "name must not start or end with a hyphen",
            )),
        )
            .into_response();
    }

    // Validate region
    let region = req
        .get("region")
        .and_then(|v| v.as_str())
        .unwrap_or("us-east-1")
        .to_string();
    let valid_regions = [
        "us-east-1",
        "us-east-2",
        "us-west-1",
        "us-west-2",
        "eu-west-1",
        "eu-west-2",
        "eu-central-1",
        "ap-southeast-1",
        "ap-northeast-1",
    ];
    if !valid_regions.contains(&region.as_str()) {
        return (
            StatusCode::BAD_REQUEST,
            Json(CloudErrorResponse::new(
                "INVALID_REGION",
                format!("region must be one of: {}", valid_regions.join(", ")),
            )),
        )
            .into_response();
    }

    let request = crate::cloud::ProvisionClusterRequest {
        name: name.clone(),
        project_id,
        tier: crate::cloud::ClusterTier::default(),
        region,
        config: None,
        metadata: None,
    };
    match state.cloud.control_plane().provision_cluster(request).await {
        Ok(cluster) => (
            StatusCode::CREATED,
            Json(serde_json::json!({
                "id": cluster.id,
                "name": cluster.name,
                "status": format!("{:?}", cluster.status),
            })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(CloudErrorResponse::new("PROVISION_FAILED", e.to_string())),
        )
            .into_response(),
    }
}

async fn get_cluster(
    State(state): State<CloudApiState>,
    Path(cluster_id): Path<String>,
) -> Response {
    match state.cloud.control_plane().get_cluster(&cluster_id).await {
        Some(cluster) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "id": cluster.id,
                "name": cluster.name,
                "project_id": cluster.project_id,
                "status": format!("{:?}", cluster.status),
                "tier": format!("{:?}", cluster.tier),
            })),
        )
            .into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(CloudErrorResponse::new(
                "NOT_FOUND",
                format!("Cluster '{}' not found", cluster_id),
            )),
        )
            .into_response(),
    }
}

async fn delete_cluster(
    State(state): State<CloudApiState>,
    Path(cluster_id): Path<String>,
) -> Response {
    match state
        .cloud
        .control_plane()
        .delete_cluster(&cluster_id)
        .await
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (
            StatusCode::NOT_FOUND,
            Json(CloudErrorResponse::new("DELETE_FAILED", e.to_string())),
        )
            .into_response(),
    }
}

async fn scale_cluster(
    State(state): State<CloudApiState>,
    Path(cluster_id): Path<String>,
    Json(req): Json<serde_json::Value>,
) -> Response {
    let tier_str = req
        .get("tier")
        .and_then(|v| v.as_str())
        .unwrap_or("starter");
    let tier = match tier_str {
        "developer" => crate::cloud::ClusterTier::Developer,
        "basic" => crate::cloud::ClusterTier::Basic,
        "standard" => crate::cloud::ClusterTier::Standard,
        "enterprise" => crate::cloud::ClusterTier::Enterprise,
        "dedicated" => crate::cloud::ClusterTier::Dedicated,
        _ => crate::cloud::ClusterTier::default(),
    };
    match state
        .cloud
        .control_plane()
        .scale_cluster(&cluster_id, tier)
        .await
    {
        Ok(cluster) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "id": cluster.id,
                "name": cluster.name,
                "status": format!("{:?}", cluster.status),
                "message": "Scaling in progress"
            })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(CloudErrorResponse::new("SCALE_FAILED", e.to_string())),
        )
            .into_response(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Managed Cloud API — /api/v1 endpoints for the managed Streamline Cloud offering
// Not yet wired into the HTTP router; this is foundation code for the cloud service.
// ---------------------------------------------------------------------------

#[allow(dead_code)]
/// Shared state for the managed cloud API
#[derive(Clone)]
pub(crate) struct ManagedCloudApiState {
    pub tenant_manager: Arc<crate::cloud::tenant::CloudTenantManager>,
    pub metering_service: Arc<crate::cloud::metering::MeteringService>,
    pub provisioning_manager: Arc<crate::cloud::provisioning::ProvisioningManager>,
}

#[allow(dead_code)]
/// Create the managed cloud API router (`/api/v1/...`)
pub(crate) fn create_managed_cloud_api_router(state: ManagedCloudApiState) -> Router {
    Router::new()
        .route("/api/v1/tenants", post(managed_create_tenant))
        .route("/api/v1/tenants", get(managed_list_tenants))
        .route("/api/v1/tenants/:id", get(managed_get_tenant))
        .route("/api/v1/tenants/:id/usage", get(managed_get_tenant_usage))
        .route("/api/v1/clusters", post(managed_provision_cluster))
        .route(
            "/api/v1/clusters/:id/status",
            get(managed_get_cluster_status),
        )
        .with_state(state)
}

// -- Request / response types for the managed API --

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct ManagedCreateTenantRequest {
    name: String,
    plan: Option<String>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct ManagedProvisionClusterRequest {
    tenant_id: String,
    size: Option<String>,
    region: Option<String>,
    version: Option<String>,
    features: Option<Vec<String>>,
}

// -- Handlers --

#[allow(dead_code)]
async fn managed_create_tenant(
    State(state): State<ManagedCloudApiState>,
    Json(req): Json<ManagedCreateTenantRequest>,
) -> Response {
    if req.name.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(CloudErrorResponse::new("INVALID_REQUEST", "name is required")),
        )
            .into_response();
    }

    let plan = match req.plan.as_deref() {
        Some("starter") => crate::cloud::tenant::CloudPlan::Starter,
        Some("pro") => crate::cloud::tenant::CloudPlan::Pro,
        Some("enterprise") => crate::cloud::tenant::CloudPlan::Enterprise,
        _ => crate::cloud::tenant::CloudPlan::Free,
    };

    match state.tenant_manager.create_tenant(&req.name, plan).await {
        Ok(tenant) => (
            StatusCode::CREATED,
            Json(serde_json::json!({
                "id": tenant.id,
                "name": tenant.name,
                "plan": tenant.plan,
                "resource_limits": tenant.resource_limits,
                "created_at": tenant.created_at,
            })),
        )
            .into_response(),
        Err(e) => {
            error!(error = %e, "Failed to create managed tenant");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(CloudErrorResponse::new("CREATE_FAILED", e.to_string())),
            )
                .into_response()
        }
    }
}

#[allow(dead_code)]
async fn managed_list_tenants(State(state): State<ManagedCloudApiState>) -> Response {
    let tenants = state.tenant_manager.list_tenants().await;
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "tenants": tenants,
            "total": tenants.len(),
        })),
    )
        .into_response()
}

#[allow(dead_code)]
async fn managed_get_tenant(
    State(state): State<ManagedCloudApiState>,
    Path(id): Path<String>,
) -> Response {
    match state.tenant_manager.get_tenant(&id).await {
        Some(tenant) => (StatusCode::OK, Json(serde_json::json!(tenant))).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(CloudErrorResponse::new(
                "NOT_FOUND",
                format!("Tenant '{}' not found", id),
            )),
        )
            .into_response(),
    }
}

#[allow(dead_code)]
async fn managed_get_tenant_usage(
    State(state): State<ManagedCloudApiState>,
    Path(id): Path<String>,
) -> Response {
    let now = chrono::Utc::now();
    let from = now - chrono::Duration::days(30);
    match state.metering_service.get_usage_summary(&id, from, now).await {
        Ok(summary) => (StatusCode::OK, Json(serde_json::json!(summary))).into_response(),
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("No metering data") {
                // Return an empty summary instead of an error for new tenants
                (
                    StatusCode::OK,
                    Json(serde_json::json!({
                        "tenant_id": id,
                        "window": "daily",
                        "total_bytes_in": 0,
                        "total_bytes_out": 0,
                        "total_messages_in": 0,
                        "total_messages_out": 0,
                        "peak_storage_bytes": 0,
                        "peak_active_connections": 0,
                        "sample_count": 0,
                    })),
                )
                    .into_response()
            } else {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(CloudErrorResponse::new("USAGE_FETCH_FAILED", msg)),
                )
                    .into_response()
            }
        }
    }
}

#[allow(dead_code)]
async fn managed_provision_cluster(
    State(state): State<ManagedCloudApiState>,
    Json(req): Json<ManagedProvisionClusterRequest>,
) -> Response {
    if req.tenant_id.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(CloudErrorResponse::new(
                "INVALID_REQUEST",
                "tenant_id is required",
            )),
        )
            .into_response();
    }

    // Verify tenant exists
    if state.tenant_manager.get_tenant(&req.tenant_id).await.is_none() {
        return (
            StatusCode::NOT_FOUND,
            Json(CloudErrorResponse::new(
                "TENANT_NOT_FOUND",
                format!("Tenant '{}' not found", req.tenant_id),
            )),
        )
            .into_response();
    }

    let spec = crate::cloud::provisioning::ClusterSpec {
        size: req.size.unwrap_or_else(|| "small".to_string()),
        region: req.region.unwrap_or_else(|| "us-east-1".to_string()),
        version: req.version.unwrap_or_else(|| "0.2.0".to_string()),
        features: req.features.unwrap_or_default(),
    };

    match state
        .provisioning_manager
        .provision_cluster(&req.tenant_id, spec)
        .await
    {
        Ok(cluster) => {
            info!(cluster_id = %cluster.id, tenant_id = %req.tenant_id, "Managed cluster provisioned via API");
            (
                StatusCode::CREATED,
                Json(serde_json::json!({
                    "id": cluster.id,
                    "tenant_id": cluster.tenant_id,
                    "spec": cluster.spec,
                    "status": cluster.status.to_string(),
                    "created_at": cluster.created_at,
                })),
            )
                .into_response()
        }
        Err(e) => {
            error!(error = %e, "Failed to provision managed cluster");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(CloudErrorResponse::new("PROVISION_FAILED", e.to_string())),
            )
                .into_response()
        }
    }
}

#[allow(dead_code)]
async fn managed_get_cluster_status(
    State(state): State<ManagedCloudApiState>,
    Path(id): Path<String>,
) -> Response {
    match state.provisioning_manager.get_cluster(&id).await {
        Some(cluster) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "id": cluster.id,
                "tenant_id": cluster.tenant_id,
                "status": cluster.status.to_string(),
                "spec": cluster.spec,
                "created_at": cluster.created_at,
                "updated_at": cluster.updated_at,
            })),
        )
            .into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(CloudErrorResponse::new(
                "NOT_FOUND",
                format!("Cluster '{}' not found", id),
            )),
        )
            .into_response(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_tenant_validation_empty_name() {
        let req = RegisterTenantRequest {
            name: "".to_string(),
            email: "test@example.com".to_string(),
            tier: TenantTier::Basic,
        };
        assert!(req.name.is_empty());
    }

    #[test]
    fn test_register_tenant_validation_empty_email() {
        let req = RegisterTenantRequest {
            name: "Test Tenant".to_string(),
            email: "".to_string(),
            tier: TenantTier::Basic,
        };
        assert!(req.email.is_empty());
    }

    #[test]
    fn test_scale_request_variants() {
        let small = ScaleEndpointRequest {
            new_size: ClusterSize::Small,
        };
        assert_eq!(small.new_size.vcpu(), 2);

        let large = ScaleEndpointRequest {
            new_size: ClusterSize::Large,
        };
        assert_eq!(large.new_size.vcpu(), 8);
    }

    #[test]
    fn test_error_response_serialization() {
        let err = CloudErrorResponse::new("TEST_ERROR", "something went wrong");
        let json = serde_json::to_value(&err).unwrap();
        assert_eq!(json["error"], "TEST_ERROR");
        assert_eq!(json["message"], "something went wrong");
    }

    #[test]
    fn test_cloud_api_state_creation() {
        let cloud = StreamlineCloud::new(Default::default());
        assert!(cloud.is_ok());
    }
}
