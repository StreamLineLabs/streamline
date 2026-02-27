//! Multi-Cloud Fabric HTTP API
//!
//! REST endpoints for managing multi-cloud deployments, routing rules,
//! topology, failover, and cost analysis.
//!
//! ## Endpoints
//!
//! - `POST /api/v1/cloud/regions` - Register cloud region
//! - `GET  /api/v1/cloud/regions` - List regions
//! - `GET  /api/v1/cloud/regions/:id` - Get region details
//! - `DELETE /api/v1/cloud/regions/:id` - Remove region
//! - `GET  /api/v1/cloud/topology` - Get multi-cloud topology
//! - `POST /api/v1/cloud/routing-rules` - Create data routing rule
//! - `GET  /api/v1/cloud/routing-rules` - List routing rules
//! - `DELETE /api/v1/cloud/routing-rules/:id` - Remove rule
//! - `POST /api/v1/cloud/failover/:region` - Trigger failover
//! - `GET  /api/v1/cloud/costs` - Cost analysis across clouds
//! - `GET  /api/v1/cloud/stats` - Multi-cloud stats

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{delete, get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Cloud provider enum
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum CloudProvider {
    Aws,
    Azure,
    Gcp,
    OnPrem,
    Custom(String),
}

impl std::fmt::Display for CloudProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CloudProvider::Aws => write!(f, "aws"),
            CloudProvider::Azure => write!(f, "azure"),
            CloudProvider::Gcp => write!(f, "gcp"),
            CloudProvider::OnPrem => write!(f, "on_prem"),
            CloudProvider::Custom(s) => write!(f, "custom({})", s),
        }
    }
}

/// Region health status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum RegionStatus {
    Active,
    Degraded,
    Down,
    Draining,
    Standby,
}

/// A registered cloud region
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudRegion {
    pub id: String,
    pub provider: CloudProvider,
    pub region: String,
    pub endpoint: String,
    pub status: RegionStatus,
    pub cluster_count: u32,
    pub latency_ms: f64,
    pub cost_per_gb: f64,
    pub data_residency: Vec<String>,
    pub created_at: String,
}

/// Routing rule type
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RuleType {
    Latency { max_ms: u64 },
    Cost { max_per_gb: f64 },
    Residency { allowed_regions: Vec<String> },
    Affinity { preferred_provider: CloudProvider },
    LoadBalance { strategy: String },
}

/// A data routing rule
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingRule {
    pub id: String,
    pub name: String,
    pub rule_type: RuleType,
    pub priority: u32,
    pub enabled: bool,
    pub created_at: String,
}

/// Connection between two regions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegionConnection {
    pub from: String,
    pub to: String,
    pub latency_ms: f64,
    pub bandwidth_mbps: f64,
}

/// Multi-cloud topology view
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudTopology {
    pub regions: Vec<CloudRegion>,
    pub connections: Vec<RegionConnection>,
    pub primary_region: Option<String>,
}

/// Per-region cost breakdown
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegionCost {
    pub region: String,
    pub provider: String,
    pub monthly_cost: f64,
    pub storage_gb: f64,
    pub transfer_gb: f64,
}

/// Aggregated cost analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostAnalysis {
    pub total_monthly_cost: f64,
    pub by_region: Vec<RegionCost>,
    pub optimization_suggestions: Vec<String>,
}

/// Atomic counters for multi-cloud operations
pub struct MultiCloudStats {
    pub regions_registered: AtomicU64,
    pub failovers_triggered: AtomicU64,
    pub routing_rules_evaluated: AtomicU64,
    pub cross_region_bytes: AtomicU64,
}

impl MultiCloudStats {
    pub fn new() -> Self {
        Self {
            regions_registered: AtomicU64::new(0),
            failovers_triggered: AtomicU64::new(0),
            routing_rules_evaluated: AtomicU64::new(0),
            cross_region_bytes: AtomicU64::new(0),
        }
    }
}

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

/// Shared state for the Multi-Cloud Fabric API
#[derive(Clone)]
pub struct MultiCloudApiState {
    regions: Arc<RwLock<HashMap<String, CloudRegion>>>,
    routing_rules: Arc<RwLock<Vec<RoutingRule>>>,
    stats: Arc<MultiCloudStats>,
}

impl MultiCloudApiState {
    pub fn new() -> Self {
        Self {
            regions: Arc::new(RwLock::new(HashMap::new())),
            routing_rules: Arc::new(RwLock::new(Vec::new())),
            stats: Arc::new(MultiCloudStats::new()),
        }
    }
}

// ---------------------------------------------------------------------------
// Request / response types
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct RegisterRegionRequest {
    pub provider: CloudProvider,
    pub region: String,
    pub endpoint: String,
    #[serde(default)]
    pub cost_per_gb: Option<f64>,
    #[serde(default)]
    pub data_residency: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct CreateRoutingRuleRequest {
    pub name: String,
    pub rule_type: RuleType,
    #[serde(default = "default_priority")]
    pub priority: u32,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

fn default_priority() -> u32 {
    100
}

fn default_enabled() -> bool {
    true
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FailoverResponse {
    pub region: String,
    pub previous_status: RegionStatus,
    pub new_status: RegionStatus,
    pub message: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MultiCloudStatsResponse {
    pub total_regions: u64,
    pub active_regions: u64,
    pub total_routing_rules: u64,
    pub failovers_triggered: u64,
    pub routing_rules_evaluated: u64,
    pub cross_region_bytes: u64,
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

/// Create the Multi-Cloud Fabric API router
pub fn create_multicloud_api_router(state: MultiCloudApiState) -> Router {
    Router::new()
        .route("/api/v1/cloud/regions", post(register_region))
        .route("/api/v1/cloud/regions", get(list_regions))
        .route("/api/v1/cloud/regions/:id", get(get_region))
        .route("/api/v1/cloud/regions/:id", delete(remove_region))
        .route("/api/v1/cloud/topology", get(get_topology))
        .route("/api/v1/cloud/routing-rules", post(create_routing_rule))
        .route("/api/v1/cloud/routing-rules", get(list_routing_rules))
        .route(
            "/api/v1/cloud/routing-rules/:id",
            delete(remove_routing_rule),
        )
        .route("/api/v1/cloud/failover/:region", post(trigger_failover))
        .route("/api/v1/cloud/costs", get(get_cost_analysis))
        .route("/api/v1/cloud/stats", get(get_stats))
        .with_state(state)
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

async fn register_region(
    State(state): State<MultiCloudApiState>,
    Json(req): Json<RegisterRegionRequest>,
) -> Result<(StatusCode, Json<CloudRegion>), StatusCode> {
    let id = Uuid::new_v4().to_string();
    let now = chrono::Utc::now().to_rfc3339();

    let region = CloudRegion {
        id: id.clone(),
        provider: req.provider,
        region: req.region.clone(),
        endpoint: req.endpoint,
        status: RegionStatus::Active,
        cluster_count: 0,
        latency_ms: 0.0,
        cost_per_gb: req.cost_per_gb.unwrap_or(0.10),
        data_residency: req.data_residency,
        created_at: now,
    };

    state.regions.write().await.insert(id, region.clone());
    state.stats.regions_registered.fetch_add(1, Ordering::Relaxed);
    info!(region = %req.region, "registered cloud region");

    Ok((StatusCode::CREATED, Json(region)))
}

async fn list_regions(
    State(state): State<MultiCloudApiState>,
) -> Json<Vec<CloudRegion>> {
    let regions = state.regions.read().await;
    Json(regions.values().cloned().collect())
}

async fn get_region(
    State(state): State<MultiCloudApiState>,
    Path(id): Path<String>,
) -> Result<Json<CloudRegion>, StatusCode> {
    state
        .regions
        .read()
        .await
        .get(&id)
        .cloned()
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn remove_region(
    State(state): State<MultiCloudApiState>,
    Path(id): Path<String>,
) -> StatusCode {
    let removed = state.regions.write().await.remove(&id);
    match removed {
        Some(r) => {
            info!(id = %id, region = %r.region, "removed cloud region");
            StatusCode::NO_CONTENT
        }
        None => StatusCode::NOT_FOUND,
    }
}

async fn get_topology(
    State(state): State<MultiCloudApiState>,
) -> Json<CloudTopology> {
    let regions = state.regions.read().await;
    let region_list: Vec<CloudRegion> = regions.values().cloned().collect();

    // Build pairwise connections between active regions
    let mut connections = Vec::new();
    let active: Vec<&CloudRegion> = region_list
        .iter()
        .filter(|r| r.status == RegionStatus::Active)
        .collect();
    for i in 0..active.len() {
        for j in (i + 1)..active.len() {
            connections.push(RegionConnection {
                from: active[i].id.clone(),
                to: active[j].id.clone(),
                latency_ms: active[i].latency_ms + active[j].latency_ms + 5.0,
                bandwidth_mbps: 1000.0,
            });
        }
    }

    let primary = region_list.first().map(|r| r.id.clone());
    Json(CloudTopology {
        regions: region_list,
        connections,
        primary_region: primary,
    })
}

async fn create_routing_rule(
    State(state): State<MultiCloudApiState>,
    Json(req): Json<CreateRoutingRuleRequest>,
) -> (StatusCode, Json<RoutingRule>) {
    let rule = RoutingRule {
        id: Uuid::new_v4().to_string(),
        name: req.name.clone(),
        rule_type: req.rule_type,
        priority: req.priority,
        enabled: req.enabled,
        created_at: chrono::Utc::now().to_rfc3339(),
    };
    state.routing_rules.write().await.push(rule.clone());
    info!(name = %req.name, "created routing rule");
    (StatusCode::CREATED, Json(rule))
}

async fn list_routing_rules(
    State(state): State<MultiCloudApiState>,
) -> Json<Vec<RoutingRule>> {
    Json(state.routing_rules.read().await.clone())
}

async fn remove_routing_rule(
    State(state): State<MultiCloudApiState>,
    Path(id): Path<String>,
) -> StatusCode {
    let mut rules = state.routing_rules.write().await;
    let len_before = rules.len();
    rules.retain(|r| r.id != id);
    if rules.len() < len_before {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    }
}

async fn trigger_failover(
    State(state): State<MultiCloudApiState>,
    Path(region_id): Path<String>,
) -> Result<Json<FailoverResponse>, StatusCode> {
    let mut regions = state.regions.write().await;
    let region = regions.get_mut(&region_id).ok_or(StatusCode::NOT_FOUND)?;

    let previous_status = region.status.clone();
    region.status = RegionStatus::Draining;
    state.stats.failovers_triggered.fetch_add(1, Ordering::Relaxed);

    warn!(region = %region.region, "failover triggered");

    Ok(Json(FailoverResponse {
        region: region_id,
        previous_status,
        new_status: RegionStatus::Draining,
        message: "Failover initiated, region is now draining".to_string(),
    }))
}

async fn get_cost_analysis(
    State(state): State<MultiCloudApiState>,
) -> Json<CostAnalysis> {
    let regions = state.regions.read().await;
    let mut total = 0.0;
    let mut by_region = Vec::new();

    for r in regions.values() {
        let monthly = r.cost_per_gb * 100.0; // estimate 100 GB baseline
        total += monthly;
        by_region.push(RegionCost {
            region: r.region.clone(),
            provider: r.provider.to_string(),
            monthly_cost: monthly,
            storage_gb: 80.0,
            transfer_gb: 20.0,
        });
    }

    let mut suggestions = Vec::new();
    if regions.len() > 3 {
        suggestions.push("Consider consolidating regions to reduce cross-region transfer costs".to_string());
    }
    if regions.values().any(|r| r.status == RegionStatus::Standby) {
        suggestions.push("Standby regions incur baseline costs — review if still needed".to_string());
    }

    Json(CostAnalysis {
        total_monthly_cost: total,
        by_region,
        optimization_suggestions: suggestions,
    })
}

async fn get_stats(
    State(state): State<MultiCloudApiState>,
) -> Json<MultiCloudStatsResponse> {
    let regions = state.regions.read().await;
    let active = regions
        .values()
        .filter(|r| r.status == RegionStatus::Active)
        .count() as u64;
    let rules_count = state.routing_rules.read().await.len() as u64;

    Json(MultiCloudStatsResponse {
        total_regions: regions.len() as u64,
        active_regions: active,
        total_routing_rules: rules_count,
        failovers_triggered: state.stats.failovers_triggered.load(Ordering::Relaxed),
        routing_rules_evaluated: state.stats.routing_rules_evaluated.load(Ordering::Relaxed),
        cross_region_bytes: state.stats.cross_region_bytes.load(Ordering::Relaxed),
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    fn create_test_app() -> Router {
        let state = MultiCloudApiState::new();
        create_multicloud_api_router(state)
    }

    fn region_json(provider: &str, region: &str) -> String {
        format!(
            r#"{{"provider":"{}","region":"{}","endpoint":"https://{}.example.com:9092","cost_per_gb":0.12,"data_residency":["EU"]}}"#,
            provider, region, region
        )
    }

    async fn register_test_region(app: &Router, provider: &str, region: &str) -> CloudRegion {
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/cloud/regions")
                    .header("content-type", "application/json")
                    .body(Body::from(region_json(provider, region)))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    #[tokio::test]
    async fn test_register_region() {
        let app = create_test_app();
        let region = register_test_region(&app, "aws", "us-east-1").await;
        assert_eq!(region.provider, CloudProvider::Aws);
        assert_eq!(region.region, "us-east-1");
        assert_eq!(region.status, RegionStatus::Active);
        assert!(!region.id.is_empty());
    }

    #[tokio::test]
    async fn test_list_regions_empty() {
        let app = create_test_app();
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/cloud/regions")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let regions: Vec<CloudRegion> = serde_json::from_slice(&body).unwrap();
        assert!(regions.is_empty());
    }

    #[tokio::test]
    async fn test_list_regions_after_register() {
        let app = create_test_app();
        register_test_region(&app, "aws", "us-east-1").await;
        register_test_region(&app, "gcp", "europe-west1").await;

        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/cloud/regions")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let regions: Vec<CloudRegion> = serde_json::from_slice(&body).unwrap();
        assert_eq!(regions.len(), 2);
    }

    #[tokio::test]
    async fn test_get_region_by_id() {
        let app = create_test_app();
        let created = register_test_region(&app, "azure", "westeurope").await;

        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/v1/cloud/regions/{}", created.id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let region: CloudRegion = serde_json::from_slice(&body).unwrap();
        assert_eq!(region.id, created.id);
    }

    #[tokio::test]
    async fn test_get_region_not_found() {
        let app = create_test_app();
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/cloud/regions/nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_remove_region() {
        let app = create_test_app();
        let created = register_test_region(&app, "aws", "ap-southeast-1").await;

        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!("/api/v1/cloud/regions/{}", created.id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);

        // Confirm gone
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/v1/cloud/regions/{}", created.id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_remove_region_not_found() {
        let app = create_test_app();
        let resp = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/v1/cloud/regions/nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_get_topology_empty() {
        let app = create_test_app();
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/cloud/topology")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let topo: CloudTopology = serde_json::from_slice(&body).unwrap();
        assert!(topo.regions.is_empty());
        assert!(topo.connections.is_empty());
        assert!(topo.primary_region.is_none());
    }

    #[tokio::test]
    async fn test_get_topology_with_regions() {
        let app = create_test_app();
        register_test_region(&app, "aws", "us-east-1").await;
        register_test_region(&app, "gcp", "europe-west1").await;

        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/cloud/topology")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let topo: CloudTopology = serde_json::from_slice(&body).unwrap();
        assert_eq!(topo.regions.len(), 2);
        assert_eq!(topo.connections.len(), 1);
        assert!(topo.primary_region.is_some());
    }

    #[tokio::test]
    async fn test_create_routing_rule() {
        let app = create_test_app();
        let body = r#"{"name":"low-latency","rule_type":{"type":"latency","max_ms":50},"priority":10}"#;
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/cloud/routing-rules")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let rule: RoutingRule = serde_json::from_slice(&body).unwrap();
        assert_eq!(rule.name, "low-latency");
        assert_eq!(rule.priority, 10);
        assert!(rule.enabled);
    }

    #[tokio::test]
    async fn test_list_routing_rules() {
        let app = create_test_app();
        let body = r#"{"name":"cost-opt","rule_type":{"type":"cost","max_per_gb":0.05}}"#;
        app.clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/cloud/routing-rules")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/cloud/routing-rules")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let rules: Vec<RoutingRule> = serde_json::from_slice(&body).unwrap();
        assert_eq!(rules.len(), 1);
    }

    #[tokio::test]
    async fn test_remove_routing_rule() {
        let app = create_test_app();
        let body = r#"{"name":"eu-residency","rule_type":{"type":"residency","allowed_regions":["EU"]}}"#;
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/cloud/routing-rules")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        let b = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let rule: RoutingRule = serde_json::from_slice(&b).unwrap();

        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!("/api/v1/cloud/routing-rules/{}", rule.id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn test_remove_routing_rule_not_found() {
        let app = create_test_app();
        let resp = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/v1/cloud/routing-rules/nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_trigger_failover() {
        let app = create_test_app();
        let created = register_test_region(&app, "aws", "us-west-2").await;

        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/v1/cloud/failover/{}", created.id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let fo: FailoverResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(fo.previous_status, RegionStatus::Active);
        assert_eq!(fo.new_status, RegionStatus::Draining);
    }

    #[tokio::test]
    async fn test_trigger_failover_not_found() {
        let app = create_test_app();
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/cloud/failover/nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_cost_analysis() {
        let app = create_test_app();
        register_test_region(&app, "aws", "us-east-1").await;

        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/cloud/costs")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let costs: CostAnalysis = serde_json::from_slice(&body).unwrap();
        assert_eq!(costs.by_region.len(), 1);
        assert!(costs.total_monthly_cost > 0.0);
    }

    #[tokio::test]
    async fn test_stats() {
        let app = create_test_app();
        register_test_region(&app, "gcp", "asia-east1").await;

        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/cloud/stats")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let stats: MultiCloudStatsResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(stats.total_regions, 1);
        assert_eq!(stats.active_regions, 1);
    }

    #[tokio::test]
    async fn test_routing_rule_affinity_type() {
        let app = create_test_app();
        let body = r#"{"name":"prefer-aws","rule_type":{"type":"affinity","preferred_provider":"aws"},"priority":5}"#;
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/cloud/routing-rules")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
        let b = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let rule: RoutingRule = serde_json::from_slice(&b).unwrap();
        assert_eq!(rule.priority, 5);
    }

    #[tokio::test]
    async fn test_routing_rule_load_balance_type() {
        let app = create_test_app();
        let body = r#"{"name":"round-robin","rule_type":{"type":"load_balance","strategy":"round_robin"}}"#;
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/cloud/routing-rules")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn test_cost_analysis_empty() {
        let app = create_test_app();
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/cloud/costs")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let costs: CostAnalysis = serde_json::from_slice(&body).unwrap();
        assert_eq!(costs.total_monthly_cost, 0.0);
        assert!(costs.by_region.is_empty());
    }
}
