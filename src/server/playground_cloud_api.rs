//! Hosted Playground Cloud HTTP API
//!
//! REST endpoints for the cloud-hosted interactive playground with session
//! management, query execution, templates, and sharing.
//!
//! ## Endpoints
//!
//! - `POST /api/v1/playground/sessions` - Create playground session
//! - `GET  /api/v1/playground/sessions/:id` - Get session state
//! - `DELETE /api/v1/playground/sessions/:id` - End session
//! - `POST /api/v1/playground/sessions/:id/execute` - Execute query in session
//! - `GET  /api/v1/playground/templates` - List example templates
//! - `POST /api/v1/playground/sessions/:id/share` - Generate share URL
//! - `GET  /api/v1/playground/shared/:token` - Load shared session
//! - `GET  /api/v1/playground/stats` - Playground stats

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
use tracing::info;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// A topic within a playground session
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionTopic {
    pub name: String,
    pub messages: Vec<serde_json::Value>,
    pub partitions: u32,
}

/// A recorded query execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryExecution {
    pub query: String,
    pub result: Option<serde_json::Value>,
    pub error: Option<String>,
    pub duration_ms: f64,
    pub timestamp: String,
}

/// An active playground session
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlaygroundSession {
    pub id: String,
    pub topics: Vec<SessionTopic>,
    pub query_history: Vec<QueryExecution>,
    pub variables: HashMap<String, serde_json::Value>,
    pub created_at: String,
    pub expires_at: String,
    pub last_activity_at: String,
}

/// A pre-built playground template
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlaygroundTemplate {
    pub id: String,
    pub name: String,
    pub description: String,
    pub category: String,
    pub topics: Vec<SessionTopic>,
    pub queries: Vec<String>,
    pub difficulty: String,
}

/// Atomic counters for playground usage
pub struct PlaygroundCloudStats {
    pub sessions_created: AtomicU64,
    pub queries_executed: AtomicU64,
    pub shares_created: AtomicU64,
    pub templates_loaded: AtomicU64,
}

impl PlaygroundCloudStats {
    pub fn new() -> Self {
        Self {
            sessions_created: AtomicU64::new(0),
            queries_executed: AtomicU64::new(0),
            shares_created: AtomicU64::new(0),
            templates_loaded: AtomicU64::new(0),
        }
    }
}

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

/// Shared state for the Playground Cloud API
#[derive(Clone)]
pub struct PlaygroundCloudState {
    sessions: Arc<RwLock<HashMap<String, PlaygroundSession>>>,
    templates: Vec<PlaygroundTemplate>,
    shared: Arc<RwLock<HashMap<String, String>>>,
    stats: Arc<PlaygroundCloudStats>,
}

impl PlaygroundCloudState {
    pub fn new() -> Self {
        Self {
            sessions: Arc::new(RwLock::new(HashMap::new())),
            templates: build_default_templates(),
            shared: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(PlaygroundCloudStats::new()),
        }
    }
}

// ---------------------------------------------------------------------------
// Request / response types
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct CreateSessionRequest {
    #[serde(default)]
    pub topics: Vec<SessionTopicSetup>,
    #[serde(default)]
    pub template_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SessionTopicSetup {
    pub name: String,
    #[serde(default = "default_partitions")]
    pub partitions: u32,
}

fn default_partitions() -> u32 {
    1
}

#[derive(Debug, Deserialize)]
pub struct ExecuteQueryRequest {
    pub query: String,
    #[serde(default)]
    pub variables: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ExecuteQueryResponse {
    pub result: Option<serde_json::Value>,
    pub error: Option<String>,
    pub duration_ms: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ShareResponse {
    pub token: String,
    pub url: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PlaygroundStatsResponse {
    pub total_sessions: u64,
    pub active_sessions: u64,
    pub total_queries_executed: u64,
    pub total_shares: u64,
    pub templates_available: u64,
    pub templates_loaded: u64,
}

// ---------------------------------------------------------------------------
// Templates
// ---------------------------------------------------------------------------

fn build_default_templates() -> Vec<PlaygroundTemplate> {
    vec![
        PlaygroundTemplate {
            id: "cdc-demo".into(),
            name: "CDC Demo".into(),
            description: "Change Data Capture pipeline — track inserts, updates, and deletes from a database".into(),
            category: "Data Integration".into(),
            topics: vec![SessionTopic { name: "cdc.public.users".into(), messages: vec![], partitions: 3 }],
            queries: vec!["SELECT * FROM cdc.public.users".into(), "SELECT * FROM cdc.public.users WHERE op = 'INSERT'".into()],
            difficulty: "beginner".into(),
        },
        PlaygroundTemplate {
            id: "iot-sensor-stream".into(),
            name: "IoT Sensor Stream".into(),
            description: "Real-time sensor data from IoT devices with temperature, humidity, and GPS".into(),
            category: "IoT".into(),
            topics: vec![SessionTopic { name: "sensors.temperature".into(), messages: vec![], partitions: 4 }],
            queries: vec!["SELECT avg(temperature) FROM sensors.temperature GROUP BY device_id".into()],
            difficulty: "beginner".into(),
        },
        PlaygroundTemplate {
            id: "ecommerce-events".into(),
            name: "E-Commerce Events".into(),
            description: "Clickstream and purchase events for an online store".into(),
            category: "Analytics".into(),
            topics: vec![
                SessionTopic { name: "shop.clicks".into(), messages: vec![], partitions: 2 },
                SessionTopic { name: "shop.purchases".into(), messages: vec![], partitions: 2 },
            ],
            queries: vec!["SELECT count(*) FROM shop.purchases WHERE amount > 100".into()],
            difficulty: "intermediate".into(),
        },
        PlaygroundTemplate {
            id: "log-analytics".into(),
            name: "Log Analytics".into(),
            description: "Structured application logs with severity levels and trace IDs".into(),
            category: "Observability".into(),
            topics: vec![SessionTopic { name: "logs.app".into(), messages: vec![], partitions: 6 }],
            queries: vec!["SELECT * FROM logs.app WHERE level = 'ERROR' LIMIT 10".into()],
            difficulty: "beginner".into(),
        },
        PlaygroundTemplate {
            id: "ai-pipeline".into(),
            name: "AI Pipeline".into(),
            description: "ML inference pipeline with feature vectors, predictions, and feedback loops".into(),
            category: "Machine Learning".into(),
            topics: vec![
                SessionTopic { name: "ml.features".into(), messages: vec![], partitions: 2 },
                SessionTopic { name: "ml.predictions".into(), messages: vec![], partitions: 2 },
            ],
            queries: vec!["SELECT model, avg(latency_ms) FROM ml.predictions GROUP BY model".into()],
            difficulty: "advanced".into(),
        },
        PlaygroundTemplate {
            id: "realtime-metrics".into(),
            name: "Real-Time Metrics".into(),
            description: "System and application metrics aggregated in real time".into(),
            category: "Observability".into(),
            topics: vec![SessionTopic { name: "metrics.system".into(), messages: vec![], partitions: 3 }],
            queries: vec!["SELECT host, max(cpu_percent) FROM metrics.system GROUP BY host".into()],
            difficulty: "beginner".into(),
        },
        PlaygroundTemplate {
            id: "consumer-groups".into(),
            name: "Consumer Groups".into(),
            description: "Demonstrates consumer group rebalancing, offsets, and lag monitoring".into(),
            category: "Kafka Basics".into(),
            topics: vec![SessionTopic { name: "demo.events".into(), messages: vec![], partitions: 4 }],
            queries: vec!["SHOW CONSUMER GROUPS".into(), "DESCRIBE CONSUMER GROUP 'demo-group'".into()],
            difficulty: "intermediate".into(),
        },
        PlaygroundTemplate {
            id: "wasm-transforms".into(),
            name: "WASM Transforms".into(),
            description: "Apply WebAssembly transformations to streaming data in-flight".into(),
            category: "Processing".into(),
            topics: vec![
                SessionTopic { name: "raw.events".into(), messages: vec![], partitions: 2 },
                SessionTopic { name: "transformed.events".into(), messages: vec![], partitions: 2 },
            ],
            queries: vec!["SELECT * FROM transformed.events LIMIT 5".into()],
            difficulty: "advanced".into(),
        },
        PlaygroundTemplate {
            id: "streamql-tutorial".into(),
            name: "StreamQL Tutorial".into(),
            description: "Step-by-step tutorial for Streamline's SQL-like query language".into(),
            category: "Getting Started".into(),
            topics: vec![SessionTopic { name: "tutorial.events".into(), messages: vec![], partitions: 1 }],
            queries: vec![
                "SELECT * FROM tutorial.events".into(),
                "SELECT count(*) FROM tutorial.events".into(),
                "SELECT * FROM tutorial.events WHERE type = 'click'".into(),
            ],
            difficulty: "beginner".into(),
        },
        PlaygroundTemplate {
            id: "compliance-audit".into(),
            name: "Compliance Audit".into(),
            description: "Audit trail for data access, retention policies, and PII detection".into(),
            category: "Governance".into(),
            topics: vec![
                SessionTopic { name: "audit.access".into(), messages: vec![], partitions: 2 },
                SessionTopic { name: "audit.retention".into(), messages: vec![], partitions: 1 },
            ],
            queries: vec!["SELECT * FROM audit.access WHERE action = 'DELETE' ORDER BY ts DESC".into()],
            difficulty: "intermediate".into(),
        },
        PlaygroundTemplate {
            id: "financial-trades".into(),
            name: "Financial Trades".into(),
            description: "Simulated stock trade stream with order books and price ticks".into(),
            category: "Finance".into(),
            topics: vec![SessionTopic { name: "trades.ticks".into(), messages: vec![], partitions: 8 }],
            queries: vec!["SELECT symbol, last(price) FROM trades.ticks GROUP BY symbol".into()],
            difficulty: "advanced".into(),
        },
    ]
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

/// Create the Playground Cloud API router
pub fn create_playground_cloud_api_router(state: PlaygroundCloudState) -> Router {
    Router::new()
        .route("/api/v1/playground/sessions", post(create_session))
        .route("/api/v1/playground/sessions/:id", get(get_session))
        .route("/api/v1/playground/sessions/:id", delete(end_session))
        .route(
            "/api/v1/playground/sessions/:id/execute",
            post(execute_query),
        )
        .route("/api/v1/playground/templates", get(list_templates))
        .route(
            "/api/v1/playground/sessions/:id/share",
            post(share_session),
        )
        .route("/api/v1/playground/shared/:token", get(load_shared))
        .route("/api/v1/playground/stats", get(get_stats))
        .with_state(state)
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

async fn create_session(
    State(state): State<PlaygroundCloudState>,
    Json(req): Json<CreateSessionRequest>,
) -> Result<(StatusCode, Json<PlaygroundSession>), StatusCode> {
    let now = chrono::Utc::now();
    let id = Uuid::new_v4().to_string();

    // If a template is requested, seed the session from it
    let topics = if let Some(ref tmpl_id) = req.template_id {
        state
            .templates
            .iter()
            .find(|t| t.id == *tmpl_id)
            .map(|t| t.topics.clone())
            .unwrap_or_default()
    } else if req.topics.is_empty() {
        vec![SessionTopic {
            name: "playground-default".into(),
            messages: vec![],
            partitions: 1,
        }]
    } else {
        req.topics
            .into_iter()
            .map(|t| SessionTopic {
                name: t.name,
                messages: vec![],
                partitions: t.partitions,
            })
            .collect()
    };

    let session = PlaygroundSession {
        id: id.clone(),
        topics,
        query_history: vec![],
        variables: HashMap::new(),
        created_at: now.to_rfc3339(),
        expires_at: (now + chrono::Duration::hours(1)).to_rfc3339(),
        last_activity_at: now.to_rfc3339(),
    };

    state.sessions.write().await.insert(id, session.clone());
    state.stats.sessions_created.fetch_add(1, Ordering::Relaxed);
    info!(session_id = %session.id, "playground session created");

    Ok((StatusCode::CREATED, Json(session)))
}

async fn get_session(
    State(state): State<PlaygroundCloudState>,
    Path(id): Path<String>,
) -> Result<Json<PlaygroundSession>, StatusCode> {
    state
        .sessions
        .read()
        .await
        .get(&id)
        .cloned()
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn end_session(
    State(state): State<PlaygroundCloudState>,
    Path(id): Path<String>,
) -> StatusCode {
    match state.sessions.write().await.remove(&id) {
        Some(_) => {
            info!(session_id = %id, "playground session ended");
            StatusCode::NO_CONTENT
        }
        None => StatusCode::NOT_FOUND,
    }
}

async fn execute_query(
    State(state): State<PlaygroundCloudState>,
    Path(id): Path<String>,
    Json(req): Json<ExecuteQueryRequest>,
) -> Result<Json<ExecuteQueryResponse>, StatusCode> {
    let mut sessions = state.sessions.write().await;
    let session = sessions.get_mut(&id).ok_or(StatusCode::NOT_FOUND)?;

    let start = std::time::Instant::now();

    // Simple simulated execution: echo the query back as a result
    let (result, error) = if req.query.trim().is_empty() {
        (None, Some("empty query".to_string()))
    } else {
        (
            Some(serde_json::json!({
                "columns": ["result"],
                "rows": [[format!("executed: {}", req.query)]],
            })),
            None,
        )
    };

    let duration_ms = start.elapsed().as_secs_f64() * 1000.0;
    let now = chrono::Utc::now().to_rfc3339();

    session.query_history.push(QueryExecution {
        query: req.query.clone(),
        result: result.clone(),
        error: error.clone(),
        duration_ms,
        timestamp: now.clone(),
    });
    session.last_activity_at = now;

    // Merge provided variables
    for (k, v) in req.variables {
        session.variables.insert(k, v);
    }

    state.stats.queries_executed.fetch_add(1, Ordering::Relaxed);

    Ok(Json(ExecuteQueryResponse {
        result,
        error,
        duration_ms,
    }))
}

async fn list_templates(
    State(state): State<PlaygroundCloudState>,
) -> Json<Vec<PlaygroundTemplate>> {
    Json(state.templates.clone())
}

async fn share_session(
    State(state): State<PlaygroundCloudState>,
    Path(id): Path<String>,
) -> Result<(StatusCode, Json<ShareResponse>), StatusCode> {
    // Verify session exists
    if !state.sessions.read().await.contains_key(&id) {
        return Err(StatusCode::NOT_FOUND);
    }

    let token = Uuid::new_v4().to_string();
    state.shared.write().await.insert(token.clone(), id);
    state.stats.shares_created.fetch_add(1, Ordering::Relaxed);

    Ok((
        StatusCode::CREATED,
        Json(ShareResponse {
            url: format!("/api/v1/playground/shared/{}", token),
            token,
        }),
    ))
}

async fn load_shared(
    State(state): State<PlaygroundCloudState>,
    Path(token): Path<String>,
) -> Result<Json<PlaygroundSession>, StatusCode> {
    let shared = state.shared.read().await;
    let session_id = shared.get(&token).ok_or(StatusCode::NOT_FOUND)?;

    state
        .sessions
        .read()
        .await
        .get(session_id)
        .cloned()
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn get_stats(
    State(state): State<PlaygroundCloudState>,
) -> Json<PlaygroundStatsResponse> {
    let active = state.sessions.read().await.len() as u64;
    Json(PlaygroundStatsResponse {
        total_sessions: state.stats.sessions_created.load(Ordering::Relaxed),
        active_sessions: active,
        total_queries_executed: state.stats.queries_executed.load(Ordering::Relaxed),
        total_shares: state.stats.shares_created.load(Ordering::Relaxed),
        templates_available: state.templates.len() as u64,
        templates_loaded: state.stats.templates_loaded.load(Ordering::Relaxed),
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
        let state = PlaygroundCloudState::new();
        create_playground_cloud_api_router(state)
    }

    async fn create_test_session(app: &Router) -> PlaygroundSession {
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/playground/sessions")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"topics":[]}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    #[tokio::test]
    async fn test_create_session_default() {
        let app = create_test_app();
        let session = create_test_session(&app).await;
        assert!(!session.id.is_empty());
        assert_eq!(session.topics.len(), 1);
        assert_eq!(session.topics[0].name, "playground-default");
    }

    #[tokio::test]
    async fn test_create_session_with_topics() {
        let app = create_test_app();
        let body = r#"{"topics":[{"name":"my-topic","partitions":4},{"name":"other","partitions":2}]}"#;
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/playground/sessions")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
        let b = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let s: PlaygroundSession = serde_json::from_slice(&b).unwrap();
        assert_eq!(s.topics.len(), 2);
        assert_eq!(s.topics[0].partitions, 4);
    }

    #[tokio::test]
    async fn test_create_session_from_template() {
        let app = create_test_app();
        let body = r#"{"template_id":"cdc-demo"}"#;
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/playground/sessions")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
        let b = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let s: PlaygroundSession = serde_json::from_slice(&b).unwrap();
        assert_eq!(s.topics[0].name, "cdc.public.users");
    }

    #[tokio::test]
    async fn test_get_session() {
        let app = create_test_app();
        let session = create_test_session(&app).await;

        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/v1/playground/sessions/{}", session.id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let b = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let s: PlaygroundSession = serde_json::from_slice(&b).unwrap();
        assert_eq!(s.id, session.id);
    }

    #[tokio::test]
    async fn test_get_session_not_found() {
        let app = create_test_app();
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/playground/sessions/nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_end_session() {
        let app = create_test_app();
        let session = create_test_session(&app).await;

        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!("/api/v1/playground/sessions/{}", session.id))
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
                    .uri(format!("/api/v1/playground/sessions/{}", session.id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_end_session_not_found() {
        let app = create_test_app();
        let resp = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/v1/playground/sessions/nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_execute_query() {
        let app = create_test_app();
        let session = create_test_session(&app).await;

        let body = r#"{"query":"SELECT * FROM events","variables":{}}"#;
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!(
                        "/api/v1/playground/sessions/{}/execute",
                        session.id
                    ))
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let b = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let r: ExecuteQueryResponse = serde_json::from_slice(&b).unwrap();
        assert!(r.result.is_some());
        assert!(r.error.is_none());
        assert!(r.duration_ms >= 0.0);
    }

    #[tokio::test]
    async fn test_execute_empty_query() {
        let app = create_test_app();
        let session = create_test_session(&app).await;

        let body = r#"{"query":"  ","variables":{}}"#;
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!(
                        "/api/v1/playground/sessions/{}/execute",
                        session.id
                    ))
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let b = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let r: ExecuteQueryResponse = serde_json::from_slice(&b).unwrap();
        assert!(r.result.is_none());
        assert_eq!(r.error.as_deref(), Some("empty query"));
    }

    #[tokio::test]
    async fn test_execute_query_not_found() {
        let app = create_test_app();
        let body = r#"{"query":"SELECT 1","variables":{}}"#;
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/playground/sessions/bad-id/execute")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_list_templates() {
        let app = create_test_app();
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/playground/templates")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let b = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let templates: Vec<PlaygroundTemplate> = serde_json::from_slice(&b).unwrap();
        assert!(templates.len() >= 10);

        let ids: Vec<&str> = templates.iter().map(|t| t.id.as_str()).collect();
        assert!(ids.contains(&"cdc-demo"));
        assert!(ids.contains(&"iot-sensor-stream"));
        assert!(ids.contains(&"ecommerce-events"));
        assert!(ids.contains(&"log-analytics"));
        assert!(ids.contains(&"ai-pipeline"));
        assert!(ids.contains(&"realtime-metrics"));
        assert!(ids.contains(&"consumer-groups"));
        assert!(ids.contains(&"wasm-transforms"));
        assert!(ids.contains(&"streamql-tutorial"));
        assert!(ids.contains(&"compliance-audit"));
    }

    #[tokio::test]
    async fn test_share_session() {
        let app = create_test_app();
        let session = create_test_session(&app).await;

        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!(
                        "/api/v1/playground/sessions/{}/share",
                        session.id
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
        let b = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let share: ShareResponse = serde_json::from_slice(&b).unwrap();
        assert!(!share.token.is_empty());
        assert!(share.url.contains(&share.token));
    }

    #[tokio::test]
    async fn test_share_session_not_found() {
        let app = create_test_app();
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/playground/sessions/nonexistent/share")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_load_shared_session() {
        let app = create_test_app();
        let session = create_test_session(&app).await;

        // Share it
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!(
                        "/api/v1/playground/sessions/{}/share",
                        session.id
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let b = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let share: ShareResponse = serde_json::from_slice(&b).unwrap();

        // Load via token
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/v1/playground/shared/{}", share.token))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let b = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let loaded: PlaygroundSession = serde_json::from_slice(&b).unwrap();
        assert_eq!(loaded.id, session.id);
    }

    #[tokio::test]
    async fn test_load_shared_invalid_token() {
        let app = create_test_app();
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/playground/shared/bad-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_stats() {
        let app = create_test_app();
        let session = create_test_session(&app).await;

        // Execute a query
        let body = r#"{"query":"SELECT 1","variables":{}}"#;
        app.clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!(
                        "/api/v1/playground/sessions/{}/execute",
                        session.id
                    ))
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
                    .uri("/api/v1/playground/stats")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let b = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let stats: PlaygroundStatsResponse = serde_json::from_slice(&b).unwrap();
        assert_eq!(stats.total_sessions, 1);
        assert_eq!(stats.active_sessions, 1);
        assert_eq!(stats.total_queries_executed, 1);
        assert!(stats.templates_available >= 10);
    }

    #[tokio::test]
    async fn test_query_updates_history() {
        let app = create_test_app();
        let session = create_test_session(&app).await;

        let body = r#"{"query":"SELECT count(*)","variables":{}}"#;
        app.clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!(
                        "/api/v1/playground/sessions/{}/execute",
                        session.id
                    ))
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Fetch session and check history
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/v1/playground/sessions/{}", session.id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let b = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let s: PlaygroundSession = serde_json::from_slice(&b).unwrap();
        assert_eq!(s.query_history.len(), 1);
        assert_eq!(s.query_history[0].query, "SELECT count(*)");
    }

    #[tokio::test]
    async fn test_execute_with_variables() {
        let app = create_test_app();
        let session = create_test_session(&app).await;

        let body = r#"{"query":"SELECT $1","variables":{"limit":10}}"#;
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!(
                        "/api/v1/playground/sessions/{}/execute",
                        session.id
                    ))
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        // Verify variables stored
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/v1/playground/sessions/{}", session.id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let b = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let s: PlaygroundSession = serde_json::from_slice(&b).unwrap();
        assert!(s.variables.contains_key("limit"));
    }
}
