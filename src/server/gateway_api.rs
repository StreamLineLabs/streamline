//! Multi-Protocol Gateway HTTP API
//!
//! REST endpoints for managing protocol listeners that bridge external
//! protocols (MQTT, AMQP, HTTP, gRPC, WebSocket) into Streamline topics:
//!
//! - `GET  /api/v1/gateway/protocols` - List supported protocols
//! - `POST /api/v1/gateway/listeners` - Create protocol listener
//! - `GET  /api/v1/gateway/listeners` - List active listeners
//! - `GET  /api/v1/gateway/listeners/:id` - Get listener details
//! - `DELETE /api/v1/gateway/listeners/:id` - Remove listener
//! - `GET  /api/v1/gateway/listeners/:id/metrics` - Listener metrics
//! - `GET  /api/v1/gateway/stats` - Aggregate gateway stats

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

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Shared state for the Gateway API.
#[derive(Clone)]
pub struct GatewayApiState {
    listeners: Arc<RwLock<HashMap<String, ProtocolListener>>>,
    stats: Arc<GatewayStats>,
}

impl GatewayApiState {
    pub fn new() -> Self {
        Self {
            listeners: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(GatewayStats::default()),
        }
    }
}

impl Default for GatewayApiState {
    fn default() -> Self {
        Self::new()
    }
}

/// A protocol listener that bridges an external protocol into Streamline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtocolListener {
    pub id: String,
    pub protocol: GatewayProtocol,
    pub bind_addr: String,
    pub target_topic: String,
    pub status: ListenerStatus,
    pub config: ListenerConfig,
    pub metrics: ListenerMetrics,
    pub created_at: String,
}

/// Supported gateway protocols.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum GatewayProtocol {
    Mqtt { version: String },
    Amqp,
    Http,
    Grpc,
    WebSocket,
}

/// Lifecycle status of a listener.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum ListenerStatus {
    Active,
    Stopped,
    Error { reason: String },
}

/// Configuration for a protocol listener.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListenerConfig {
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    #[serde(default = "default_max_message_size")]
    pub max_message_size: usize,
    #[serde(default)]
    pub auth_required: bool,
    #[serde(default)]
    pub tls_enabled: bool,
    pub topic_mapping: TopicMapping,
}

fn default_max_connections() -> u32 {
    1000
}
fn default_max_message_size() -> usize {
    1_048_576 // 1 MB
}

impl Default for ListenerConfig {
    fn default() -> Self {
        Self {
            max_connections: default_max_connections(),
            max_message_size: default_max_message_size(),
            auth_required: false,
            tls_enabled: false,
            topic_mapping: TopicMapping::Static {
                topic: "default".to_string(),
            },
        }
    }
}

/// How incoming messages are routed to Streamline topics.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TopicMapping {
    Static { topic: String },
    Dynamic { template: String },
    Header { header_name: String },
}

/// Per-listener traffic metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListenerMetrics {
    pub connections_total: u64,
    pub connections_active: u64,
    pub messages_received: u64,
    pub messages_forwarded: u64,
    pub bytes_received: u64,
    pub errors: u64,
}

impl Default for ListenerMetrics {
    fn default() -> Self {
        Self {
            connections_total: 0,
            connections_active: 0,
            messages_received: 0,
            messages_forwarded: 0,
            bytes_received: 0,
            errors: 0,
        }
    }
}

/// Aggregate gateway-level stats (lock-free counters).
pub struct GatewayStats {
    pub total_listeners_created: AtomicU64,
    pub total_messages_received: AtomicU64,
    pub total_messages_forwarded: AtomicU64,
    pub total_bytes_received: AtomicU64,
    pub total_errors: AtomicU64,
}

impl Default for GatewayStats {
    fn default() -> Self {
        Self {
            total_listeners_created: AtomicU64::new(0),
            total_messages_received: AtomicU64::new(0),
            total_messages_forwarded: AtomicU64::new(0),
            total_bytes_received: AtomicU64::new(0),
            total_errors: AtomicU64::new(0),
        }
    }
}

/// Serialisable snapshot of aggregate stats.
#[derive(Debug, Serialize, Deserialize)]
pub struct GatewayStatsResponse {
    pub active_listeners: u64,
    pub total_listeners_created: u64,
    pub total_messages_received: u64,
    pub total_messages_forwarded: u64,
    pub total_bytes_received: u64,
    pub total_errors: u64,
    pub protocols_in_use: Vec<String>,
}

/// Request body for creating a new listener.
#[derive(Debug, Deserialize)]
pub struct CreateListenerRequest {
    pub protocol: GatewayProtocol,
    pub bind_addr: String,
    pub target_topic: String,
    pub config: Option<ListenerConfig>,
}

/// Static protocol info returned by the protocols endpoint.
#[derive(Debug, Serialize, Deserialize)]
struct ProtocolInfo {
    name: String,
    default_port: u16,
    description: String,
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

/// Create the Gateway API router.
pub fn create_gateway_api_router(state: GatewayApiState) -> Router {
    Router::new()
        .route("/api/v1/gateway/protocols", get(list_protocols))
        .route(
            "/api/v1/gateway/listeners",
            get(list_listeners).post(create_listener),
        )
        .route(
            "/api/v1/gateway/listeners/:id",
            get(get_listener).delete(remove_listener),
        )
        .route(
            "/api/v1/gateway/listeners/:id/metrics",
            get(get_listener_metrics),
        )
        .route("/api/v1/gateway/stats", get(get_gateway_stats))
        .with_state(state)
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// GET /api/v1/gateway/protocols
async fn list_protocols() -> Json<Vec<ProtocolInfo>> {
    Json(vec![
        ProtocolInfo {
            name: "mqtt".to_string(),
            default_port: 1883,
            description: "MQTT v3.1.1 / v5.0 protocol bridge".to_string(),
        },
        ProtocolInfo {
            name: "amqp".to_string(),
            default_port: 5672,
            description: "AMQP 0-9-1 protocol bridge".to_string(),
        },
        ProtocolInfo {
            name: "http".to_string(),
            default_port: 8080,
            description: "HTTP/HTTPS ingestion endpoint".to_string(),
        },
        ProtocolInfo {
            name: "grpc".to_string(),
            default_port: 50051,
            description: "gRPC streaming ingestion".to_string(),
        },
        ProtocolInfo {
            name: "websocket".to_string(),
            default_port: 8081,
            description: "WebSocket ingestion endpoint".to_string(),
        },
    ])
}

/// POST /api/v1/gateway/listeners
async fn create_listener(
    State(state): State<GatewayApiState>,
    Json(req): Json<CreateListenerRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, Json<serde_json::Value>)> {
    let mut listeners = state.listeners.write().await;

    // Reject duplicate bind address
    if listeners.values().any(|l| l.bind_addr == req.bind_addr) {
        return Err((
            StatusCode::CONFLICT,
            Json(serde_json::json!({
                "error": format!("A listener is already bound to {}", req.bind_addr)
            })),
        ));
    }

    let id = uuid::Uuid::new_v4().to_string();
    let now = chrono::Utc::now().to_rfc3339();

    let config = req.config.unwrap_or(ListenerConfig {
        topic_mapping: TopicMapping::Static {
            topic: req.target_topic.clone(),
        },
        ..Default::default()
    });

    let listener = ProtocolListener {
        id: id.clone(),
        protocol: req.protocol,
        bind_addr: req.bind_addr,
        target_topic: req.target_topic,
        status: ListenerStatus::Active,
        config,
        metrics: ListenerMetrics::default(),
        created_at: now,
    };

    tracing::info!(id = %listener.id, bind = %listener.bind_addr, "Listener created");
    listeners.insert(id.clone(), listener);
    state
        .stats
        .total_listeners_created
        .fetch_add(1, Ordering::Relaxed);

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "id": id,
            "status": "active",
            "message": "Listener created successfully"
        })),
    ))
}

/// GET /api/v1/gateway/listeners
async fn list_listeners(State(state): State<GatewayApiState>) -> Json<serde_json::Value> {
    let listeners = state.listeners.read().await;
    let list: Vec<&ProtocolListener> = listeners.values().collect();
    Json(serde_json::json!({
        "listeners": list,
        "total": list.len()
    }))
}

/// GET /api/v1/gateway/listeners/:id
async fn get_listener(
    State(state): State<GatewayApiState>,
    Path(id): Path<String>,
) -> Result<Json<ProtocolListener>, (StatusCode, Json<serde_json::Value>)> {
    let listeners = state.listeners.read().await;
    listeners.get(&id).cloned().map(Json).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": format!("Listener '{}' not found", id)})),
        )
    })
}

/// DELETE /api/v1/gateway/listeners/:id
async fn remove_listener(
    State(state): State<GatewayApiState>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let mut listeners = state.listeners.write().await;
    if listeners.remove(&id).is_some() {
        tracing::info!(id = %id, "Listener removed");
        Ok(Json(serde_json::json!({
            "deleted": true,
            "id": id
        })))
    } else {
        Err((
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": format!("Listener '{}' not found", id)})),
        ))
    }
}

/// GET /api/v1/gateway/listeners/:id/metrics
async fn get_listener_metrics(
    State(state): State<GatewayApiState>,
    Path(id): Path<String>,
) -> Result<Json<ListenerMetrics>, (StatusCode, Json<serde_json::Value>)> {
    let listeners = state.listeners.read().await;
    let listener = listeners.get(&id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": format!("Listener '{}' not found", id)})),
        )
    })?;

    Ok(Json(listener.metrics.clone()))
}

/// GET /api/v1/gateway/stats
async fn get_gateway_stats(State(state): State<GatewayApiState>) -> Json<GatewayStatsResponse> {
    let listeners = state.listeners.read().await;

    let mut protocols: Vec<String> = listeners
        .values()
        .map(|l| match &l.protocol {
            GatewayProtocol::Mqtt { .. } => "mqtt".to_string(),
            GatewayProtocol::Amqp => "amqp".to_string(),
            GatewayProtocol::Http => "http".to_string(),
            GatewayProtocol::Grpc => "grpc".to_string(),
            GatewayProtocol::WebSocket => "websocket".to_string(),
        })
        .collect();
    protocols.sort();
    protocols.dedup();

    Json(GatewayStatsResponse {
        active_listeners: listeners.len() as u64,
        total_listeners_created: state.stats.total_listeners_created.load(Ordering::Relaxed),
        total_messages_received: state.stats.total_messages_received.load(Ordering::Relaxed),
        total_messages_forwarded: state
            .stats
            .total_messages_forwarded
            .load(Ordering::Relaxed),
        total_bytes_received: state.stats.total_bytes_received.load(Ordering::Relaxed),
        total_errors: state.stats.total_errors.load(Ordering::Relaxed),
        protocols_in_use: protocols,
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

    fn create_test_app() -> (GatewayApiState, Router) {
        let state = GatewayApiState::new();
        let app = create_gateway_api_router(state.clone());
        (state, app)
    }

    fn sample_mqtt_body() -> String {
        serde_json::json!({
            "protocol": { "type": "mqtt", "version": "5.0" },
            "bind_addr": "0.0.0.0:1883",
            "target_topic": "iot-events"
        })
        .to_string()
    }

    /// Helper: create a listener and return state + fresh router for follow-up.
    async fn app_with_listener() -> (GatewayApiState, String) {
        let (state, app) = create_test_app();

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/gateway/listeners")
                    .header("content-type", "application/json")
                    .body(Body::from(sample_mqtt_body()))
                    .expect("request"),
            )
            .await
            .expect("response");

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let v: serde_json::Value = serde_json::from_slice(&body).expect("json");
        let id = v["id"].as_str().unwrap().to_string();

        (state, id)
    }

    // 1. List supported protocols
    #[tokio::test]
    async fn test_list_protocols() {
        let (_state, app) = create_test_app();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/gateway/protocols")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let protocols: Vec<serde_json::Value> = serde_json::from_slice(&body).expect("json");
        assert_eq!(protocols.len(), 5);
        let names: Vec<&str> = protocols.iter().map(|p| p["name"].as_str().unwrap()).collect();
        assert!(names.contains(&"mqtt"));
        assert!(names.contains(&"amqp"));
        assert!(names.contains(&"websocket"));
    }

    // 2. Create listener
    #[tokio::test]
    async fn test_create_listener() {
        let (_state, app) = create_test_app();

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/gateway/listeners")
                    .header("content-type", "application/json")
                    .body(Body::from(sample_mqtt_body()))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::CREATED);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let v: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(v["status"], "active");
        assert!(v["id"].as_str().is_some());
    }

    // 3. Create listener duplicate bind address
    #[tokio::test]
    async fn test_create_listener_duplicate_bind() {
        let (state, _id) = app_with_listener().await;
        let app = create_gateway_api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/gateway/listeners")
                    .header("content-type", "application/json")
                    .body(Body::from(sample_mqtt_body()))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::CONFLICT);
    }

    // 4. List listeners (empty)
    #[tokio::test]
    async fn test_list_listeners_empty() {
        let (_state, app) = create_test_app();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/gateway/listeners")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let v: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(v["total"], 0);
    }

    // 5. List listeners after create
    #[tokio::test]
    async fn test_list_listeners_after_create() {
        let (state, _id) = app_with_listener().await;
        let app = create_gateway_api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/gateway/listeners")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let v: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(v["total"], 1);
    }

    // 6. Get listener details
    #[tokio::test]
    async fn test_get_listener() {
        let (state, id) = app_with_listener().await;
        let app = create_gateway_api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(&format!("/api/v1/gateway/listeners/{}", id))
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let listener: ProtocolListener = serde_json::from_slice(&body).expect("json");
        assert_eq!(listener.id, id);
        assert_eq!(listener.target_topic, "iot-events");
        assert_eq!(listener.bind_addr, "0.0.0.0:1883");
    }

    // 7. Get listener not found
    #[tokio::test]
    async fn test_get_listener_not_found() {
        let (_state, app) = create_test_app();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/gateway/listeners/nonexistent")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    // 8. Remove listener
    #[tokio::test]
    async fn test_remove_listener() {
        let (state, id) = app_with_listener().await;
        let app = create_gateway_api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(&format!("/api/v1/gateway/listeners/{}", id))
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let v: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert!(v["deleted"].as_bool().unwrap());
    }

    // 9. Remove listener not found
    #[tokio::test]
    async fn test_remove_listener_not_found() {
        let (_state, app) = create_test_app();
        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/v1/gateway/listeners/nonexistent")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    // 10. Get listener metrics
    #[tokio::test]
    async fn test_get_listener_metrics() {
        let (state, id) = app_with_listener().await;
        let app = create_gateway_api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(&format!("/api/v1/gateway/listeners/{}/metrics", id))
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let m: ListenerMetrics = serde_json::from_slice(&body).expect("json");
        assert_eq!(m.connections_total, 0);
        assert_eq!(m.messages_received, 0);
    }

    // 11. Get listener metrics not found
    #[tokio::test]
    async fn test_get_listener_metrics_not_found() {
        let (_state, app) = create_test_app();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/gateway/listeners/nonexistent/metrics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    // 12. Aggregate gateway stats (empty)
    #[tokio::test]
    async fn test_gateway_stats_empty() {
        let (_state, app) = create_test_app();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/gateway/stats")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let stats: GatewayStatsResponse = serde_json::from_slice(&body).expect("json");
        assert_eq!(stats.active_listeners, 0);
        assert_eq!(stats.total_listeners_created, 0);
        assert!(stats.protocols_in_use.is_empty());
    }

    // 13. Aggregate stats after creating a listener
    #[tokio::test]
    async fn test_gateway_stats_after_create() {
        let (state, _id) = app_with_listener().await;
        let app = create_gateway_api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/gateway/stats")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let stats: GatewayStatsResponse = serde_json::from_slice(&body).expect("json");
        assert_eq!(stats.active_listeners, 1);
        assert_eq!(stats.total_listeners_created, 1);
        assert!(stats.protocols_in_use.contains(&"mqtt".to_string()));
    }

    // 14. Create HTTP listener
    #[tokio::test]
    async fn test_create_http_listener() {
        let (_state, app) = create_test_app();

        let body = serde_json::json!({
            "protocol": { "type": "http" },
            "bind_addr": "0.0.0.0:8080",
            "target_topic": "http-events",
            "config": {
                "max_connections": 5000,
                "max_message_size": 2097152,
                "auth_required": true,
                "tls_enabled": true,
                "topic_mapping": { "type": "header", "header_name": "X-Topic" }
            }
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/gateway/listeners")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::CREATED);
    }

    // 15. Create WebSocket listener with dynamic topic mapping
    #[tokio::test]
    async fn test_create_websocket_listener() {
        let (_state, app) = create_test_app();

        let body = serde_json::json!({
            "protocol": { "type": "web_socket" },
            "bind_addr": "0.0.0.0:8081",
            "target_topic": "ws-events",
            "config": {
                "max_connections": 2000,
                "max_message_size": 65536,
                "auth_required": false,
                "tls_enabled": false,
                "topic_mapping": { "type": "dynamic", "template": "ws/{client_id}/{channel}" }
            }
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/gateway/listeners")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::CREATED);
    }

    // 16. Create gRPC listener
    #[tokio::test]
    async fn test_create_grpc_listener() {
        let (_state, app) = create_test_app();

        let body = serde_json::json!({
            "protocol": { "type": "grpc" },
            "bind_addr": "0.0.0.0:50051",
            "target_topic": "grpc-stream"
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/gateway/listeners")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::CREATED);
    }

    // 17. Stats show multiple protocols in use
    #[tokio::test]
    async fn test_stats_multiple_protocols() {
        let state = GatewayApiState::new();

        // Create MQTT listener
        let app = create_gateway_api_router(state.clone());
        let _ = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/gateway/listeners")
                    .header("content-type", "application/json")
                    .body(Body::from(sample_mqtt_body()))
                    .expect("request"),
            )
            .await
            .expect("response");

        // Create AMQP listener
        let app = create_gateway_api_router(state.clone());
        let amqp = serde_json::json!({
            "protocol": { "type": "amqp" },
            "bind_addr": "0.0.0.0:5672",
            "target_topic": "amqp-events"
        });
        let _ = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/gateway/listeners")
                    .header("content-type", "application/json")
                    .body(Body::from(amqp.to_string()))
                    .expect("request"),
            )
            .await
            .expect("response");

        // Check stats
        let app = create_gateway_api_router(state.clone());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/gateway/stats")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let stats: GatewayStatsResponse = serde_json::from_slice(&body).expect("json");
        assert_eq!(stats.active_listeners, 2);
        assert_eq!(stats.total_listeners_created, 2);
        assert!(stats.protocols_in_use.contains(&"mqtt".to_string()));
        assert!(stats.protocols_in_use.contains(&"amqp".to_string()));
    }

    // 18. Remove listener then verify gone
    #[tokio::test]
    async fn test_remove_then_get() {
        let (state, id) = app_with_listener().await;

        // Remove
        let app = create_gateway_api_router(state.clone());
        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(&format!("/api/v1/gateway/listeners/{}", id))
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);

        // Get should 404
        let app = create_gateway_api_router(state.clone());
        let response = app
            .oneshot(
                Request::builder()
                    .uri(&format!("/api/v1/gateway/listeners/{}", id))
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    // 19. Default config values
    #[tokio::test]
    async fn test_default_listener_config() {
        let (state, id) = app_with_listener().await;
        let app = create_gateway_api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(&format!("/api/v1/gateway/listeners/{}", id))
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let listener: ProtocolListener = serde_json::from_slice(&body).expect("json");
        assert_eq!(listener.config.max_connections, 1000);
        assert_eq!(listener.config.max_message_size, 1_048_576);
        assert!(!listener.config.auth_required);
        assert!(!listener.config.tls_enabled);
    }

    // 20. Stats counter persists after remove
    #[tokio::test]
    async fn test_stats_counter_persists_after_remove() {
        let (state, id) = app_with_listener().await;

        // Remove the listener
        let app = create_gateway_api_router(state.clone());
        let _ = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(&format!("/api/v1/gateway/listeners/{}", id))
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        // total_listeners_created should still be 1, active should be 0
        let app = create_gateway_api_router(state.clone());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/gateway/stats")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let stats: GatewayStatsResponse = serde_json::from_slice(&body).expect("json");
        assert_eq!(stats.active_listeners, 0);
        assert_eq!(stats.total_listeners_created, 1);
    }
}
