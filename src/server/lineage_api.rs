//! Data Lineage REST API
//!
//! Exposes the schema catalog lineage graph via REST endpoints.
//!
//! ## Endpoints
//!
//! - `GET  /api/v1/lineage/:subject`              — Upstream/downstream lineage for a subject
//! - `GET  /api/v1/lineage/:subject/impact`        — Impact analysis for breaking changes
//! - `GET  /api/v1/lineage/graph`                  — Full lineage graph (all edges)
//! - `POST /api/v1/lineage/edges`                  — Add a lineage edge
//! - `DELETE /api/v1/lineage/edges/:source/:target` — Remove a lineage edge
//! - `GET  /api/v1/lineage/search`                 — Search catalog entries
//! - `GET  /api/v1/lineage/stats`                  — Catalog statistics

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info};

use crate::schema::catalog::{
    CatalogEntry, CatalogStats, ImpactAnalysis, LineageEdge, LineageEdgeType, LineageView,
    SchemaCatalog,
};

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

/// Shared state for the Lineage API.
#[derive(Clone)]
pub struct LineageApiState {
    pub catalog: Arc<SchemaCatalog>,
}

// ---------------------------------------------------------------------------
// Request / response types
// ---------------------------------------------------------------------------

/// Request body for adding a lineage edge.
#[derive(Debug, Deserialize)]
pub struct AddEdgeRequest {
    pub source: String,
    pub target: String,
    pub edge_type: LineageEdgeType,
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

/// Query parameters for the search endpoint.
#[derive(Debug, Deserialize)]
pub struct SearchQuery {
    pub q: String,
}

/// Wrapper for the full-graph response.
#[derive(Debug, Serialize, Deserialize)]
pub struct GraphResponse {
    pub edges: Vec<LineageEdge>,
    pub total: usize,
}

/// Generic error body returned to clients.
#[derive(Debug, Serialize)]
pub struct LineageApiError {
    pub error: String,
}

/// Success message body.
#[derive(Debug, Serialize)]
pub struct MessageResponse {
    pub message: String,
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

/// Create the lineage API router.
pub fn create_lineage_api_router(state: LineageApiState) -> Router {
    Router::new()
        .route("/api/v1/lineage/graph", get(get_full_graph))
        .route("/api/v1/lineage/edges", post(add_edge))
        .route(
            "/api/v1/lineage/edges/:source/:target",
            delete(remove_edge),
        )
        .route("/api/v1/lineage/search", get(search_entries))
        .route("/api/v1/lineage/stats", get(get_stats))
        .route("/api/v1/lineage/:subject/impact", get(get_impact))
        .route("/api/v1/lineage/:subject", get(get_lineage))
        .with_state(state)
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// `GET /api/v1/lineage/:subject` — upstream/downstream lineage.
async fn get_lineage(
    State(state): State<LineageApiState>,
    Path(subject): Path<String>,
) -> impl IntoResponse {
    debug!(subject = %subject, "fetching lineage");
    let view: LineageView = state.catalog.get_lineage(&subject);
    (StatusCode::OK, Json(view))
}

/// `GET /api/v1/lineage/:subject/impact` — impact analysis.
async fn get_impact(
    State(state): State<LineageApiState>,
    Path(subject): Path<String>,
) -> impl IntoResponse {
    debug!(subject = %subject, "running impact analysis");
    let analysis: ImpactAnalysis = state.catalog.analyze_impact(&subject);
    (StatusCode::OK, Json(analysis))
}

/// `GET /api/v1/lineage/graph` — full lineage graph.
async fn get_full_graph(State(state): State<LineageApiState>) -> impl IntoResponse {
    debug!("fetching full lineage graph");
    let edges = state.catalog.get_all_edges();
    let total = edges.len();
    (StatusCode::OK, Json(GraphResponse { edges, total }))
}

/// `POST /api/v1/lineage/edges` — add a lineage edge.
async fn add_edge(
    State(state): State<LineageApiState>,
    Json(req): Json<AddEdgeRequest>,
) -> axum::response::Response {
    info!(source = %req.source, target = %req.target, "adding lineage edge");
    let edge = LineageEdge {
        source: req.source.clone(),
        target: req.target.clone(),
        edge_type: req.edge_type,
        metadata: req.metadata,
    };
    match state.catalog.add_lineage_edge(edge) {
        Ok(()) => (
            StatusCode::CREATED,
            Json(MessageResponse {
                message: format!("Edge added: {} -> {}", req.source, req.target),
            }),
        )
            .into_response(),
        Err(err) => {
            let msg = err.to_string();
            error!(error = %msg, "failed to add lineage edge");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(LineageApiError { error: msg }),
            )
                .into_response()
        }
    }
}

/// `DELETE /api/v1/lineage/edges/:source/:target` — remove a lineage edge.
async fn remove_edge(
    State(state): State<LineageApiState>,
    Path((source, target)): Path<(String, String)>,
) -> axum::response::Response {
    info!(source = %source, target = %target, "removing lineage edge");
    match state.catalog.remove_lineage_edge(&source, &target) {
        Ok(()) => (
            StatusCode::OK,
            Json(MessageResponse {
                message: format!("Edge removed: {} -> {}", source, target),
            }),
        )
            .into_response(),
        Err(err) => {
            let msg = err.to_string();
            error!(error = %msg, "failed to remove lineage edge");
            (
                StatusCode::NOT_FOUND,
                Json(LineageApiError { error: msg }),
            )
                .into_response()
        }
    }
}

/// `GET /api/v1/lineage/search?q=…` — search catalog entries.
async fn search_entries(
    State(state): State<LineageApiState>,
    Query(params): Query<SearchQuery>,
) -> impl IntoResponse {
    debug!(query = %params.q, "searching catalog entries");
    let results: Vec<CatalogEntry> = state.catalog.search(&params.q);
    (StatusCode::OK, Json(results))
}

/// `GET /api/v1/lineage/stats` — catalog statistics.
async fn get_stats(State(state): State<LineageApiState>) -> impl IntoResponse {
    debug!("fetching catalog stats");
    let stats: CatalogStats = state.catalog.stats();
    (StatusCode::OK, Json(stats))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Method, Request};
    use tower::ServiceExt;

    use crate::schema::catalog::{
        CatalogEntry, DataClassification, OwnerInfo, SchemaCatalog,
    };

    fn test_state() -> LineageApiState {
        LineageApiState {
            catalog: Arc::new(SchemaCatalog::new()),
        }
    }

    fn make_entry(subject: &str) -> CatalogEntry {
        CatalogEntry {
            subject: subject.to_string(),
            description: Some(format!("{} description", subject)),
            owner: Some(OwnerInfo {
                team: "platform".to_string(),
                contact: "team@example.com".to_string(),
                slack_channel: None,
            }),
            tags: vec!["test".to_string()],
            classification: DataClassification::Internal,
            quality_score: Some(0.9),
            created_at: 1000,
            updated_at: 1000,
            version_count: 1,
            consumers: vec![],
            producers: vec![],
            deprecation: None,
        }
    }

    fn make_edge(source: &str, target: &str, edge_type: LineageEdgeType) -> LineageEdge {
        LineageEdge {
            source: source.to_string(),
            target: target.to_string(),
            edge_type,
            metadata: HashMap::new(),
        }
    }

    // -- GET /api/v1/lineage/:subject -----------------------------------------

    #[tokio::test]
    async fn test_get_lineage_empty() {
        let state = test_state();
        let app = create_lineage_api_router(state);

        let req = Request::builder()
            .uri("/api/v1/lineage/orders")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let view: LineageView = serde_json::from_slice(&body).unwrap();
        assert!(view.upstream.is_empty());
        assert!(view.downstream.is_empty());
    }

    #[tokio::test]
    async fn test_get_lineage_with_edges() {
        let state = test_state();
        state
            .catalog
            .add_lineage_edge(make_edge("orders", "analytics", LineageEdgeType::ConsumesFrom))
            .unwrap();
        state
            .catalog
            .add_lineage_edge(make_edge("ingestion", "orders", LineageEdgeType::ProducesTo))
            .unwrap();

        let app = create_lineage_api_router(state);
        let req = Request::builder()
            .uri("/api/v1/lineage/orders")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let view: LineageView = serde_json::from_slice(&body).unwrap();
        assert_eq!(view.downstream.len(), 1);
        assert_eq!(view.upstream.len(), 1);
    }

    // -- GET /api/v1/lineage/:subject/impact ----------------------------------

    #[tokio::test]
    async fn test_get_impact_no_consumers() {
        let state = test_state();
        let app = create_lineage_api_router(state);

        let req = Request::builder()
            .uri("/api/v1/lineage/orders/impact")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let analysis: ImpactAnalysis = serde_json::from_slice(&body).unwrap();
        assert!(analysis.direct_consumers.is_empty());
    }

    #[tokio::test]
    async fn test_get_impact_with_consumers() {
        let state = test_state();
        state
            .catalog
            .add_lineage_edge(make_edge("orders", "analytics", LineageEdgeType::ConsumesFrom))
            .unwrap();
        state
            .catalog
            .add_lineage_edge(make_edge("analytics", "dashboard", LineageEdgeType::DerivedFrom))
            .unwrap();

        let app = create_lineage_api_router(state);
        let req = Request::builder()
            .uri("/api/v1/lineage/orders/impact")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let analysis: ImpactAnalysis = serde_json::from_slice(&body).unwrap();
        assert_eq!(analysis.direct_consumers.len(), 1);
        assert_eq!(analysis.transitive_consumers.len(), 1);
    }

    // -- GET /api/v1/lineage/graph --------------------------------------------

    #[tokio::test]
    async fn test_get_full_graph_empty() {
        let state = test_state();
        let app = create_lineage_api_router(state);

        let req = Request::builder()
            .uri("/api/v1/lineage/graph")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let graph: GraphResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(graph.total, 0);
        assert!(graph.edges.is_empty());
    }

    #[tokio::test]
    async fn test_get_full_graph_with_edges() {
        let state = test_state();
        state
            .catalog
            .add_lineage_edge(make_edge("a", "b", LineageEdgeType::ProducesTo))
            .unwrap();
        state
            .catalog
            .add_lineage_edge(make_edge("b", "c", LineageEdgeType::ConsumesFrom))
            .unwrap();

        let app = create_lineage_api_router(state);
        let req = Request::builder()
            .uri("/api/v1/lineage/graph")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let graph: GraphResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(graph.total, 2);
    }

    // -- POST /api/v1/lineage/edges -------------------------------------------

    #[tokio::test]
    async fn test_add_edge() {
        let state = test_state();
        let app = create_lineage_api_router(state.clone());

        let body = serde_json::json!({
            "source": "orders",
            "target": "analytics",
            "edge_type": "ProducesTo"
        });
        let req = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/lineage/edges")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);

        let edges = state.catalog.get_all_edges();
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].source, "orders");
    }

    #[tokio::test]
    async fn test_add_edge_with_metadata() {
        let state = test_state();
        let app = create_lineage_api_router(state.clone());

        let body = serde_json::json!({
            "source": "src",
            "target": "dst",
            "edge_type": "References",
            "metadata": { "team": "platform" }
        });
        let req = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/lineage/edges")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);

        let edges = state.catalog.get_all_edges();
        assert_eq!(edges[0].metadata.get("team").unwrap(), "platform");
    }

    // -- DELETE /api/v1/lineage/edges/:source/:target -------------------------

    #[tokio::test]
    async fn test_remove_edge() {
        let state = test_state();
        state
            .catalog
            .add_lineage_edge(make_edge("a", "b", LineageEdgeType::ProducesTo))
            .unwrap();

        let app = create_lineage_api_router(state.clone());
        let req = Request::builder()
            .method(Method::DELETE)
            .uri("/api/v1/lineage/edges/a/b")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        assert!(state.catalog.get_all_edges().is_empty());
    }

    #[tokio::test]
    async fn test_remove_edge_not_found() {
        let state = test_state();
        let app = create_lineage_api_router(state);

        let req = Request::builder()
            .method(Method::DELETE)
            .uri("/api/v1/lineage/edges/x/y")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    // -- GET /api/v1/lineage/search -------------------------------------------

    #[tokio::test]
    async fn test_search_entries() {
        let state = test_state();
        state.catalog.register_entry(make_entry("orders")).unwrap();
        state.catalog.register_entry(make_entry("users")).unwrap();

        let app = create_lineage_api_router(state);
        let req = Request::builder()
            .uri("/api/v1/lineage/search?q=orders")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let results: Vec<CatalogEntry> = serde_json::from_slice(&body).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].subject, "orders");
    }

    #[tokio::test]
    async fn test_search_no_results() {
        let state = test_state();
        state.catalog.register_entry(make_entry("orders")).unwrap();

        let app = create_lineage_api_router(state);
        let req = Request::builder()
            .uri("/api/v1/lineage/search?q=nonexistent")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let results: Vec<CatalogEntry> = serde_json::from_slice(&body).unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_search_case_insensitive() {
        let state = test_state();
        state.catalog.register_entry(make_entry("Orders")).unwrap();

        let app = create_lineage_api_router(state);
        let req = Request::builder()
            .uri("/api/v1/lineage/search?q=orders")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let results: Vec<CatalogEntry> = serde_json::from_slice(&body).unwrap();
        assert_eq!(results.len(), 1);
    }

    // -- GET /api/v1/lineage/stats --------------------------------------------

    #[tokio::test]
    async fn test_stats_empty() {
        let state = test_state();
        let app = create_lineage_api_router(state);

        let req = Request::builder()
            .uri("/api/v1/lineage/stats")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let stats: CatalogStats = serde_json::from_slice(&body).unwrap();
        assert_eq!(stats.total_entries, 0);
    }

    #[tokio::test]
    async fn test_stats_with_entries() {
        let state = test_state();
        state.catalog.register_entry(make_entry("orders")).unwrap();
        state.catalog.register_entry(make_entry("users")).unwrap();

        let app = create_lineage_api_router(state);
        let req = Request::builder()
            .uri("/api/v1/lineage/stats")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let stats: CatalogStats = serde_json::from_slice(&body).unwrap();
        assert_eq!(stats.total_entries, 2);
    }

    // -- Router integration ---------------------------------------------------

    #[tokio::test]
    async fn test_router_creates_successfully() {
        let state = test_state();
        let _router = create_lineage_api_router(state);
    }

    #[tokio::test]
    async fn test_add_then_get_lineage_roundtrip() {
        let state = test_state();

        // Add edge via POST
        let app = create_lineage_api_router(state.clone());
        let body = serde_json::json!({
            "source": "orders",
            "target": "billing",
            "edge_type": "ConsumesFrom"
        });
        let req = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/lineage/edges")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);

        // Fetch lineage via GET
        let app = create_lineage_api_router(state);
        let req = Request::builder()
            .uri("/api/v1/lineage/orders")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let view: LineageView = serde_json::from_slice(&body).unwrap();
        assert_eq!(view.downstream.len(), 1);
        assert_eq!(view.downstream[0].target, "billing");
    }
}
