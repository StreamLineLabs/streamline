//! Data Mesh Domains API
//!
//! ## Endpoints
//!
//! - `POST   /api/v1/mesh/domains`                  — Create domain
//! - `GET    /api/v1/mesh/domains`                  — List domains
//! - `GET    /api/v1/mesh/domains/:name`            — Get domain
//! - `PUT    /api/v1/mesh/domains/:name`            — Update domain
//! - `DELETE /api/v1/mesh/domains/:name`            — Delete domain
//! - `POST   /api/v1/mesh/domains/:name/products`   — Register data product
//! - `GET    /api/v1/mesh/domains/:name/products`   — List data products
//! - `GET    /api/v1/mesh/products`                 — Global product catalog
//! - `GET    /api/v1/mesh/products/:id`             — Get product details
//! - `POST   /api/v1/mesh/governance/policies`      — Create governance policy
//! - `GET    /api/v1/mesh/governance/policies`      — List policies

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{delete, get, post, put},
    Json, Router,
};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;
use uuid::Uuid;

// ============================================================================
// State
// ============================================================================

/// Shared state for the Data Mesh API.
#[derive(Clone)]
pub struct DataMeshApiState {
    pub domains: Arc<RwLock<HashMap<String, DataDomain>>>,
    pub products: Arc<RwLock<HashMap<String, DataProduct>>>,
    pub policies: Arc<RwLock<Vec<GovernancePolicy>>>,
}

impl DataMeshApiState {
    pub fn new() -> Self {
        Self {
            domains: Arc::new(RwLock::new(HashMap::new())),
            products: Arc::new(RwLock::new(HashMap::new())),
            policies: Arc::new(RwLock::new(Vec::new())),
        }
    }
}

// ============================================================================
// Types
// ============================================================================

/// A data mesh domain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataDomain {
    pub name: String,
    pub description: String,
    pub owner: DomainOwner,
    pub products: Vec<String>,
    pub slo: Option<DomainSlo>,
    pub status: DomainStatus,
    pub created_at: String,
}

/// Ownership information for a domain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainOwner {
    pub team: String,
    pub contact: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slack: Option<String>,
}

/// Service-level objectives for a domain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainSlo {
    pub availability_pct: f64,
    pub latency_p99_ms: u64,
    pub freshness_secs: u64,
}

/// Status of a domain.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum DomainStatus {
    Active,
    Deprecated,
    Draft,
}

/// A data product registered under a domain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataProduct {
    pub id: String,
    pub name: String,
    pub domain: String,
    pub description: String,
    pub output_port: OutputPort,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_subject: Option<String>,
    pub quality: DataQuality,
    pub consumers: Vec<String>,
    pub created_at: String,
}

/// Describes how a data product is consumed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputPort {
    pub port_type: String,
    pub topic: String,
    pub format: String,
}

/// Quality metrics for a data product.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataQuality {
    pub completeness: f64,
    pub freshness_secs: u64,
    pub accuracy: f64,
}

/// A governance policy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GovernancePolicy {
    pub id: String,
    pub name: String,
    pub scope: String,
    pub rules: Vec<String>,
    pub enabled: bool,
    pub created_at: String,
}

// ============================================================================
// Request / Response
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct CreateDomainRequest {
    pub name: String,
    pub description: String,
    pub owner: DomainOwner,
    #[serde(default)]
    pub slo: Option<DomainSlo>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateDomainRequest {
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub owner: Option<DomainOwner>,
    #[serde(default)]
    pub slo: Option<DomainSlo>,
    #[serde(default)]
    pub status: Option<DomainStatus>,
}

#[derive(Debug, Deserialize)]
pub struct RegisterProductRequest {
    pub name: String,
    pub description: String,
    pub output_port: OutputPort,
    #[serde(default)]
    pub schema_subject: Option<String>,
    #[serde(default = "default_quality")]
    pub quality: DataQuality,
}

fn default_quality() -> DataQuality {
    DataQuality {
        completeness: 1.0,
        freshness_secs: 0,
        accuracy: 1.0,
    }
}

#[derive(Debug, Deserialize)]
pub struct CreatePolicyRequest {
    pub name: String,
    pub scope: String,
    pub rules: Vec<String>,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

fn default_true() -> bool {
    true
}

// ============================================================================
// Router
// ============================================================================

/// Create the Data Mesh API router.
pub fn create_data_mesh_api_router(state: DataMeshApiState) -> Router {
    Router::new()
        // Domains
        .route("/api/v1/mesh/domains", post(create_domain))
        .route("/api/v1/mesh/domains", get(list_domains))
        .route("/api/v1/mesh/domains/:name", get(get_domain))
        .route("/api/v1/mesh/domains/:name", put(update_domain))
        .route("/api/v1/mesh/domains/:name", delete(delete_domain))
        // Domain products
        .route(
            "/api/v1/mesh/domains/:name/products",
            post(register_product),
        )
        .route(
            "/api/v1/mesh/domains/:name/products",
            get(list_domain_products),
        )
        // Global catalog
        .route("/api/v1/mesh/products", get(list_all_products))
        .route("/api/v1/mesh/products/:id", get(get_product))
        // Governance
        .route(
            "/api/v1/mesh/governance/policies",
            post(create_policy),
        )
        .route(
            "/api/v1/mesh/governance/policies",
            get(list_policies),
        )
        .with_state(state)
}

// ============================================================================
// Handlers
// ============================================================================

fn now_iso() -> String {
    chrono::Utc::now().to_rfc3339()
}

async fn create_domain(
    State(state): State<DataMeshApiState>,
    Json(req): Json<CreateDomainRequest>,
) -> (StatusCode, Json<DataDomain>) {
    let domain = DataDomain {
        name: req.name.clone(),
        description: req.description,
        owner: req.owner,
        products: Vec::new(),
        slo: req.slo,
        status: DomainStatus::Draft,
        created_at: now_iso(),
    };
    info!(name = %domain.name, "Created data mesh domain");
    state.domains.write().insert(req.name, domain.clone());
    (StatusCode::CREATED, Json(domain))
}

async fn list_domains(State(state): State<DataMeshApiState>) -> Json<Vec<DataDomain>> {
    let domains = state.domains.read();
    Json(domains.values().cloned().collect())
}

async fn get_domain(
    State(state): State<DataMeshApiState>,
    Path(name): Path<String>,
) -> Result<Json<DataDomain>, StatusCode> {
    state
        .domains
        .read()
        .get(&name)
        .cloned()
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn update_domain(
    State(state): State<DataMeshApiState>,
    Path(name): Path<String>,
    Json(req): Json<UpdateDomainRequest>,
) -> Result<Json<DataDomain>, StatusCode> {
    let mut domains = state.domains.write();
    let domain = domains.get_mut(&name).ok_or(StatusCode::NOT_FOUND)?;

    if let Some(desc) = req.description {
        domain.description = desc;
    }
    if let Some(owner) = req.owner {
        domain.owner = owner;
    }
    if let Some(slo) = req.slo {
        domain.slo = Some(slo);
    }
    if let Some(status) = req.status {
        domain.status = status;
    }

    info!(name = %name, "Updated data mesh domain");
    Ok(Json(domain.clone()))
}

async fn delete_domain(
    State(state): State<DataMeshApiState>,
    Path(name): Path<String>,
) -> StatusCode {
    let removed = state.domains.write().remove(&name);
    if removed.is_some() {
        // Clean up products belonging to this domain
        state
            .products
            .write()
            .retain(|_, p| p.domain != name);
        info!(name = %name, "Deleted data mesh domain");
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    }
}

async fn register_product(
    State(state): State<DataMeshApiState>,
    Path(domain_name): Path<String>,
    Json(req): Json<RegisterProductRequest>,
) -> Result<(StatusCode, Json<DataProduct>), StatusCode> {
    let mut domains = state.domains.write();
    let domain = domains.get_mut(&domain_name).ok_or(StatusCode::NOT_FOUND)?;

    let product = DataProduct {
        id: Uuid::new_v4().to_string(),
        name: req.name,
        domain: domain_name.clone(),
        description: req.description,
        output_port: req.output_port,
        schema_subject: req.schema_subject,
        quality: req.quality,
        consumers: Vec::new(),
        created_at: now_iso(),
    };

    domain.products.push(product.id.clone());
    info!(id = %product.id, domain = %domain_name, "Registered data product");

    state
        .products
        .write()
        .insert(product.id.clone(), product.clone());

    Ok((StatusCode::CREATED, Json(product)))
}

async fn list_domain_products(
    State(state): State<DataMeshApiState>,
    Path(domain_name): Path<String>,
) -> Result<Json<Vec<DataProduct>>, StatusCode> {
    if !state.domains.read().contains_key(&domain_name) {
        return Err(StatusCode::NOT_FOUND);
    }
    let products = state.products.read();
    let result: Vec<_> = products
        .values()
        .filter(|p| p.domain == domain_name)
        .cloned()
        .collect();
    Ok(Json(result))
}

async fn list_all_products(State(state): State<DataMeshApiState>) -> Json<Vec<DataProduct>> {
    Json(state.products.read().values().cloned().collect())
}

async fn get_product(
    State(state): State<DataMeshApiState>,
    Path(id): Path<String>,
) -> Result<Json<DataProduct>, StatusCode> {
    state
        .products
        .read()
        .get(&id)
        .cloned()
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn create_policy(
    State(state): State<DataMeshApiState>,
    Json(req): Json<CreatePolicyRequest>,
) -> (StatusCode, Json<GovernancePolicy>) {
    let policy = GovernancePolicy {
        id: Uuid::new_v4().to_string(),
        name: req.name,
        scope: req.scope,
        rules: req.rules,
        enabled: req.enabled,
        created_at: now_iso(),
    };
    info!(id = %policy.id, name = %policy.name, "Created governance policy");
    state.policies.write().push(policy.clone());
    (StatusCode::CREATED, Json(policy))
}

async fn list_policies(State(state): State<DataMeshApiState>) -> Json<Vec<GovernancePolicy>> {
    Json(state.policies.read().clone())
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Method, Request};
    use tower::ServiceExt;

    fn app() -> Router {
        create_data_mesh_api_router(DataMeshApiState::new())
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

    // -- Domain CRUD ---------------------------------------------------------

    #[tokio::test]
    async fn test_create_domain() {
        let resp = app()
            .oneshot(json_request(
                Method::POST,
                "/api/v1/mesh/domains",
                serde_json::json!({
                    "name": "payments",
                    "description": "Payment processing domain",
                    "owner": { "team": "payments-eng", "contact": "pay@example.com" }
                }),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
        let body: DataDomain = parse_body(resp).await;
        assert_eq!(body.name, "payments");
        assert_eq!(body.status, DomainStatus::Draft);
    }

    #[tokio::test]
    async fn test_list_domains_empty() {
        let resp = app()
            .oneshot(empty_request(Method::GET, "/api/v1/mesh/domains"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body: Vec<DataDomain> = parse_body(resp).await;
        assert!(body.is_empty());
    }

    #[tokio::test]
    async fn test_get_domain_not_found() {
        let resp = app()
            .oneshot(empty_request(Method::GET, "/api/v1/mesh/domains/nope"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_create_and_get_domain() {
        let state = DataMeshApiState::new();
        let router = create_data_mesh_api_router(state);

        let resp = router
            .clone()
            .oneshot(json_request(
                Method::POST,
                "/api/v1/mesh/domains",
                serde_json::json!({
                    "name": "analytics",
                    "description": "Analytics domain",
                    "owner": { "team": "data-eng", "contact": "data@x.com" }
                }),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);

        let resp = router
            .oneshot(empty_request(Method::GET, "/api/v1/mesh/domains/analytics"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body: DataDomain = parse_body(resp).await;
        assert_eq!(body.name, "analytics");
    }

    #[tokio::test]
    async fn test_update_domain() {
        let state = DataMeshApiState::new();
        let router = create_data_mesh_api_router(state);

        router
            .clone()
            .oneshot(json_request(
                Method::POST,
                "/api/v1/mesh/domains",
                serde_json::json!({
                    "name": "orders",
                    "description": "Orders v1",
                    "owner": { "team": "t", "contact": "c" }
                }),
            ))
            .await
            .unwrap();

        let resp = router
            .oneshot(json_request(
                Method::PUT,
                "/api/v1/mesh/domains/orders",
                serde_json::json!({
                    "description": "Orders v2",
                    "status": "active"
                }),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body: DataDomain = parse_body(resp).await;
        assert_eq!(body.description, "Orders v2");
        assert_eq!(body.status, DomainStatus::Active);
    }

    #[tokio::test]
    async fn test_update_nonexistent_domain() {
        let resp = app()
            .oneshot(json_request(
                Method::PUT,
                "/api/v1/mesh/domains/ghost",
                serde_json::json!({}),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_delete_domain() {
        let state = DataMeshApiState::new();
        let router = create_data_mesh_api_router(state);

        router
            .clone()
            .oneshot(json_request(
                Method::POST,
                "/api/v1/mesh/domains",
                serde_json::json!({
                    "name": "legacy",
                    "description": "Old",
                    "owner": { "team": "t", "contact": "c" }
                }),
            ))
            .await
            .unwrap();

        let resp = router
            .clone()
            .oneshot(empty_request(Method::DELETE, "/api/v1/mesh/domains/legacy"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);

        let resp = router
            .oneshot(empty_request(Method::GET, "/api/v1/mesh/domains/legacy"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_delete_nonexistent_domain() {
        let resp = app()
            .oneshot(empty_request(
                Method::DELETE,
                "/api/v1/mesh/domains/nope",
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    // -- Products ------------------------------------------------------------

    #[tokio::test]
    async fn test_register_product() {
        let state = DataMeshApiState::new();
        let router = create_data_mesh_api_router(state);

        router
            .clone()
            .oneshot(json_request(
                Method::POST,
                "/api/v1/mesh/domains",
                serde_json::json!({
                    "name": "sales",
                    "description": "Sales domain",
                    "owner": { "team": "sales", "contact": "s@x.com" }
                }),
            ))
            .await
            .unwrap();

        let resp = router
            .oneshot(json_request(
                Method::POST,
                "/api/v1/mesh/domains/sales/products",
                serde_json::json!({
                    "name": "order-events",
                    "description": "Order event stream",
                    "output_port": { "port_type": "kafka", "topic": "sales.orders", "format": "avro" },
                    "quality": { "completeness": 0.99, "freshness_secs": 30, "accuracy": 0.98 }
                }),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
        let body: DataProduct = parse_body(resp).await;
        assert_eq!(body.domain, "sales");
    }

    #[tokio::test]
    async fn test_register_product_domain_not_found() {
        let resp = app()
            .oneshot(json_request(
                Method::POST,
                "/api/v1/mesh/domains/missing/products",
                serde_json::json!({
                    "name": "p",
                    "description": "d",
                    "output_port": { "port_type": "kafka", "topic": "t", "format": "json" },
                    "quality": { "completeness": 1.0, "freshness_secs": 0, "accuracy": 1.0 }
                }),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_list_domain_products() {
        let state = DataMeshApiState::new();
        let router = create_data_mesh_api_router(state);

        router
            .clone()
            .oneshot(json_request(
                Method::POST,
                "/api/v1/mesh/domains",
                serde_json::json!({
                    "name": "fin",
                    "description": "Finance",
                    "owner": { "team": "fin", "contact": "f@x.com" }
                }),
            ))
            .await
            .unwrap();

        router
            .clone()
            .oneshot(json_request(
                Method::POST,
                "/api/v1/mesh/domains/fin/products",
                serde_json::json!({
                    "name": "txns",
                    "description": "Transactions",
                    "output_port": { "port_type": "kafka", "topic": "fin.txns", "format": "json" },
                    "quality": { "completeness": 1.0, "freshness_secs": 0, "accuracy": 1.0 }
                }),
            ))
            .await
            .unwrap();

        let resp = router
            .oneshot(empty_request(
                Method::GET,
                "/api/v1/mesh/domains/fin/products",
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body: Vec<DataProduct> = parse_body(resp).await;
        assert_eq!(body.len(), 1);
        assert_eq!(body[0].domain, "fin");
    }

    #[tokio::test]
    async fn test_list_domain_products_domain_not_found() {
        let resp = app()
            .oneshot(empty_request(
                Method::GET,
                "/api/v1/mesh/domains/nope/products",
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_global_product_catalog() {
        let resp = app()
            .oneshot(empty_request(Method::GET, "/api/v1/mesh/products"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body: Vec<DataProduct> = parse_body(resp).await;
        assert!(body.is_empty());
    }

    #[tokio::test]
    async fn test_get_product_not_found() {
        let resp = app()
            .oneshot(empty_request(
                Method::GET,
                "/api/v1/mesh/products/nonexistent",
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    // -- Governance ----------------------------------------------------------

    #[tokio::test]
    async fn test_create_governance_policy() {
        let resp = app()
            .oneshot(json_request(
                Method::POST,
                "/api/v1/mesh/governance/policies",
                serde_json::json!({
                    "name": "retention-policy",
                    "scope": "global",
                    "rules": ["max_retention_days=90", "require_schema"]
                }),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
        let body: GovernancePolicy = parse_body(resp).await;
        assert_eq!(body.name, "retention-policy");
        assert!(body.enabled);
    }

    #[tokio::test]
    async fn test_list_policies_empty() {
        let resp = app()
            .oneshot(empty_request(
                Method::GET,
                "/api/v1/mesh/governance/policies",
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body: Vec<GovernancePolicy> = parse_body(resp).await;
        assert!(body.is_empty());
    }

    #[tokio::test]
    async fn test_delete_domain_cascades_products() {
        let state = DataMeshApiState::new();
        let router = create_data_mesh_api_router(state.clone());

        router
            .clone()
            .oneshot(json_request(
                Method::POST,
                "/api/v1/mesh/domains",
                serde_json::json!({
                    "name": "temp",
                    "description": "Temp",
                    "owner": { "team": "t", "contact": "c" }
                }),
            ))
            .await
            .unwrap();

        router
            .clone()
            .oneshot(json_request(
                Method::POST,
                "/api/v1/mesh/domains/temp/products",
                serde_json::json!({
                    "name": "p1",
                    "description": "d",
                    "output_port": { "port_type": "kafka", "topic": "t", "format": "json" },
                    "quality": { "completeness": 1.0, "freshness_secs": 0, "accuracy": 1.0 }
                }),
            ))
            .await
            .unwrap();

        // Ensure product exists
        assert_eq!(state.products.read().len(), 1);

        router
            .clone()
            .oneshot(empty_request(
                Method::DELETE,
                "/api/v1/mesh/domains/temp",
            ))
            .await
            .unwrap();

        // Products should have been cleaned up
        assert!(state.products.read().is_empty());
    }

    // -- Helpers -------------------------------------------------------------

    async fn parse_body<T: serde::de::DeserializeOwned>(
        resp: axum::http::Response<Body>,
    ) -> T {
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }
}
