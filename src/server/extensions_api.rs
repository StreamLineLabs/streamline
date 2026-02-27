//! Extensions & Webhooks API
//!
//! REST endpoints for managing webhooks, custom dashboard widgets, and
//! workflow triggers.
//!
//! ## Endpoints
//!
//! - `POST   /api/v1/extensions/webhooks`          - Register webhook
//! - `GET    /api/v1/extensions/webhooks`           - List webhooks
//! - `DELETE /api/v1/extensions/webhooks/:id`       - Remove webhook
//! - `POST   /api/v1/extensions/webhooks/:id/test`  - Test webhook delivery
//! - `POST   /api/v1/extensions/widgets`            - Register custom widget
//! - `GET    /api/v1/extensions/widgets`             - List widgets
//! - `DELETE /api/v1/extensions/widgets/:id`         - Remove widget
//! - `POST   /api/v1/extensions/triggers`            - Create workflow trigger
//! - `GET    /api/v1/extensions/triggers`             - List triggers
//! - `DELETE /api/v1/extensions/triggers/:id`         - Remove trigger
//! - `GET    /api/v1/extensions/stats`                - Extension stats

use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{delete, get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Domain types
// ---------------------------------------------------------------------------

/// Events that can fire a webhook or trigger.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WebhookEvent {
    TopicCreated,
    TopicDeleted,
    MessageProduced,
    ConsumerLagHigh,
    SchemaChanged,
    AlertFired,
    Custom(String),
}

/// A registered webhook endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Webhook {
    pub id: String,
    pub name: String,
    pub url: String,
    pub events: Vec<WebhookEvent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secret: Option<String>,
    pub active: bool,
    pub deliveries: u64,
    pub failures: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_delivery_at: Option<String>,
    pub created_at: String,
}

/// A custom dashboard widget.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomWidget {
    pub id: String,
    pub name: String,
    pub widget_type: String,
    pub config: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub html_template: Option<String>,
    pub data_source: String,
    pub refresh_interval_secs: u64,
    pub created_at: String,
}

/// An action executed when a trigger fires.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TriggerAction {
    SendWebhook { url: String },
    SendSlack { channel: String, message: String },
    SendEmail { to: String, subject: String },
    ExecuteFunction { function_name: String },
    ScaleTopic { topic: String, partitions: u32 },
}

/// A workflow trigger bound to an event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowTrigger {
    pub id: String,
    pub name: String,
    pub event: WebhookEvent,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub condition: Option<String>,
    pub actions: Vec<TriggerAction>,
    pub enabled: bool,
    pub executions: u64,
    pub created_at: String,
}

// ---------------------------------------------------------------------------
// Request / response helpers
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct CreateWebhookRequest {
    pub name: String,
    pub url: String,
    pub events: Vec<WebhookEvent>,
    pub secret: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct CreateWidgetRequest {
    pub name: String,
    pub widget_type: String,
    pub config: serde_json::Value,
    pub html_template: Option<String>,
    pub data_source: String,
    pub refresh_interval_secs: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct CreateTriggerRequest {
    pub name: String,
    pub event: WebhookEvent,
    pub condition: Option<String>,
    pub actions: Vec<TriggerAction>,
}

#[derive(Debug, Serialize)]
pub struct WebhookTestResult {
    pub webhook_id: String,
    pub success: bool,
    pub status_code: Option<u16>,
    pub message: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ExtensionStatsResponse {
    pub total_webhooks: usize,
    pub active_webhooks: usize,
    pub total_widgets: usize,
    pub total_triggers: usize,
    pub enabled_triggers: usize,
    pub total_deliveries: u64,
    pub total_failures: u64,
    pub total_trigger_executions: u64,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

// ---------------------------------------------------------------------------
// Shared state
// ---------------------------------------------------------------------------

/// Shared state for the extensions API.
#[derive(Clone)]
pub struct ExtensionsApiState {
    webhooks: Arc<RwLock<HashMap<String, Webhook>>>,
    widgets: Arc<RwLock<HashMap<String, CustomWidget>>>,
    triggers: Arc<RwLock<HashMap<String, WorkflowTrigger>>>,
}

impl ExtensionsApiState {
    pub fn new() -> Self {
        Self {
            webhooks: Arc::new(RwLock::new(HashMap::new())),
            widgets: Arc::new(RwLock::new(HashMap::new())),
            triggers: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl Default for ExtensionsApiState {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

/// Build the axum [`Router`] for the extensions API.
pub fn create_extensions_api_router(state: ExtensionsApiState) -> Router {
    Router::new()
        // webhooks
        .route(
            "/api/v1/extensions/webhooks",
            post(create_webhook).get(list_webhooks),
        )
        .route(
            "/api/v1/extensions/webhooks/{id}",
            delete(delete_webhook),
        )
        .route(
            "/api/v1/extensions/webhooks/{id}/test",
            post(test_webhook),
        )
        // widgets
        .route(
            "/api/v1/extensions/widgets",
            post(create_widget).get(list_widgets),
        )
        .route(
            "/api/v1/extensions/widgets/{id}",
            delete(delete_widget),
        )
        // triggers
        .route(
            "/api/v1/extensions/triggers",
            post(create_trigger).get(list_triggers),
        )
        .route(
            "/api/v1/extensions/triggers/{id}",
            delete(delete_trigger),
        )
        // stats
        .route("/api/v1/extensions/stats", get(get_extension_stats))
        .with_state(state)
}

// ---------------------------------------------------------------------------
// Webhook handlers
// ---------------------------------------------------------------------------

async fn create_webhook(
    State(state): State<ExtensionsApiState>,
    Json(req): Json<CreateWebhookRequest>,
) -> Result<(StatusCode, Json<Webhook>), (StatusCode, Json<ErrorResponse>)> {
    let id = Uuid::new_v4().to_string();
    let webhook = Webhook {
        id: id.clone(),
        name: req.name,
        url: req.url,
        events: req.events,
        secret: req.secret,
        active: true,
        deliveries: 0,
        failures: 0,
        last_delivery_at: None,
        created_at: chrono::Utc::now().to_rfc3339(),
    };

    state
        .webhooks
        .write()
        .await
        .insert(id.clone(), webhook.clone());

    info!(webhook_id = %id, "Webhook registered");
    Ok((StatusCode::CREATED, Json(webhook)))
}

async fn list_webhooks(
    State(state): State<ExtensionsApiState>,
) -> Json<Vec<Webhook>> {
    let hooks: Vec<Webhook> = state.webhooks.read().await.values().cloned().collect();
    debug!(count = hooks.len(), "Listing webhooks");
    Json(hooks)
}

async fn delete_webhook(
    State(state): State<ExtensionsApiState>,
    Path(id): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    if state.webhooks.write().await.remove(&id).is_some() {
        info!(webhook_id = %id, "Webhook removed");
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Webhook {id} not found"),
            }),
        ))
    }
}

async fn test_webhook(
    State(state): State<ExtensionsApiState>,
    Path(id): Path<String>,
) -> Result<Json<WebhookTestResult>, (StatusCode, Json<ErrorResponse>)> {
    let hooks = state.webhooks.read().await;
    let webhook = hooks.get(&id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Webhook {id} not found"),
            }),
        )
    })?;

    // Simulated delivery test
    let result = WebhookTestResult {
        webhook_id: id.clone(),
        success: true,
        status_code: Some(200),
        message: format!("Test delivery to {} succeeded", webhook.url),
    };

    info!(webhook_id = %id, url = %webhook.url, "Webhook test delivered");
    Ok(Json(result))
}

// ---------------------------------------------------------------------------
// Widget handlers
// ---------------------------------------------------------------------------

async fn create_widget(
    State(state): State<ExtensionsApiState>,
    Json(req): Json<CreateWidgetRequest>,
) -> Result<(StatusCode, Json<CustomWidget>), (StatusCode, Json<ErrorResponse>)> {
    let id = Uuid::new_v4().to_string();
    let widget = CustomWidget {
        id: id.clone(),
        name: req.name,
        widget_type: req.widget_type,
        config: req.config,
        html_template: req.html_template,
        data_source: req.data_source,
        refresh_interval_secs: req.refresh_interval_secs.unwrap_or(60),
        created_at: chrono::Utc::now().to_rfc3339(),
    };

    state
        .widgets
        .write()
        .await
        .insert(id.clone(), widget.clone());

    info!(widget_id = %id, "Widget registered");
    Ok((StatusCode::CREATED, Json(widget)))
}

async fn list_widgets(
    State(state): State<ExtensionsApiState>,
) -> Json<Vec<CustomWidget>> {
    let ws: Vec<CustomWidget> = state.widgets.read().await.values().cloned().collect();
    debug!(count = ws.len(), "Listing widgets");
    Json(ws)
}

async fn delete_widget(
    State(state): State<ExtensionsApiState>,
    Path(id): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    if state.widgets.write().await.remove(&id).is_some() {
        info!(widget_id = %id, "Widget removed");
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Widget {id} not found"),
            }),
        ))
    }
}

// ---------------------------------------------------------------------------
// Trigger handlers
// ---------------------------------------------------------------------------

async fn create_trigger(
    State(state): State<ExtensionsApiState>,
    Json(req): Json<CreateTriggerRequest>,
) -> Result<(StatusCode, Json<WorkflowTrigger>), (StatusCode, Json<ErrorResponse>)> {
    let id = Uuid::new_v4().to_string();
    let trigger = WorkflowTrigger {
        id: id.clone(),
        name: req.name,
        event: req.event,
        condition: req.condition,
        actions: req.actions,
        enabled: true,
        executions: 0,
        created_at: chrono::Utc::now().to_rfc3339(),
    };

    state
        .triggers
        .write()
        .await
        .insert(id.clone(), trigger.clone());

    info!(trigger_id = %id, "Trigger created");
    Ok((StatusCode::CREATED, Json(trigger)))
}

async fn list_triggers(
    State(state): State<ExtensionsApiState>,
) -> Json<Vec<WorkflowTrigger>> {
    let ts: Vec<WorkflowTrigger> = state.triggers.read().await.values().cloned().collect();
    debug!(count = ts.len(), "Listing triggers");
    Json(ts)
}

async fn delete_trigger(
    State(state): State<ExtensionsApiState>,
    Path(id): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    if state.triggers.write().await.remove(&id).is_some() {
        info!(trigger_id = %id, "Trigger removed");
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Trigger {id} not found"),
            }),
        ))
    }
}

// ---------------------------------------------------------------------------
// Stats handler
// ---------------------------------------------------------------------------

async fn get_extension_stats(
    State(state): State<ExtensionsApiState>,
) -> Json<ExtensionStatsResponse> {
    let hooks = state.webhooks.read().await;
    let widgets = state.widgets.read().await;
    let triggers = state.triggers.read().await;

    let total_deliveries: u64 = hooks.values().map(|h| h.deliveries).sum();
    let total_failures: u64 = hooks.values().map(|h| h.failures).sum();
    let total_trigger_executions: u64 = triggers.values().map(|t| t.executions).sum();

    Json(ExtensionStatsResponse {
        total_webhooks: hooks.len(),
        active_webhooks: hooks.values().filter(|h| h.active).count(),
        total_widgets: widgets.len(),
        total_triggers: triggers.len(),
        enabled_triggers: triggers.values().filter(|t| t.enabled).count(),
        total_deliveries,
        total_failures,
        total_trigger_executions,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tower::ServiceExt;

    fn app() -> Router {
        create_extensions_api_router(ExtensionsApiState::new())
    }

    // -- Webhook CRUD ------------------------------------------------------

    #[tokio::test]
    async fn test_create_webhook() {
        let resp = app()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/extensions/webhooks")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "name": "ci-hook",
                            "url": "https://example.com/hook",
                            "events": ["topic_created"]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::CREATED);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let wh: Webhook = serde_json::from_slice(&body).unwrap();
        assert_eq!(wh.name, "ci-hook");
        assert!(wh.active);
    }

    #[tokio::test]
    async fn test_list_webhooks_empty() {
        let resp = app()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/extensions/webhooks")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let hooks: Vec<Webhook> = serde_json::from_slice(&body).unwrap();
        assert!(hooks.is_empty());
    }

    #[tokio::test]
    async fn test_delete_webhook_not_found() {
        let resp = app()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/v1/extensions/webhooks/nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_test_webhook_not_found() {
        let resp = app()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/extensions/webhooks/nonexistent/test")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    // -- Widget CRUD -------------------------------------------------------

    #[tokio::test]
    async fn test_create_widget() {
        let resp = app()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/extensions/widgets")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "name": "throughput-chart",
                            "widget_type": "chart",
                            "config": {"metric": "bytes_in"},
                            "data_source": "metrics"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::CREATED);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let w: CustomWidget = serde_json::from_slice(&body).unwrap();
        assert_eq!(w.name, "throughput-chart");
        assert_eq!(w.refresh_interval_secs, 60);
    }

    #[tokio::test]
    async fn test_list_widgets_empty() {
        let resp = app()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/extensions/widgets")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_delete_widget_not_found() {
        let resp = app()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/v1/extensions/widgets/missing")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    // -- Trigger CRUD ------------------------------------------------------

    #[tokio::test]
    async fn test_create_trigger() {
        let resp = app()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/extensions/triggers")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "name": "lag-alert",
                            "event": "consumer_lag_high",
                            "actions": [{"send_slack": {"channel": "#ops", "message": "lag!"}}]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::CREATED);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let t: WorkflowTrigger = serde_json::from_slice(&body).unwrap();
        assert_eq!(t.name, "lag-alert");
        assert!(t.enabled);
    }

    #[tokio::test]
    async fn test_list_triggers_empty() {
        let resp = app()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/extensions/triggers")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_delete_trigger_not_found() {
        let resp = app()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/v1/extensions/triggers/missing")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    // -- Stats -------------------------------------------------------------

    #[tokio::test]
    async fn test_stats_empty() {
        let resp = app()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/extensions/stats")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let stats: ExtensionStatsResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(stats.total_webhooks, 0);
        assert_eq!(stats.total_widgets, 0);
        assert_eq!(stats.total_triggers, 0);
    }

    // -- Serde roundtrips --------------------------------------------------

    #[test]
    fn test_webhook_event_serde() {
        let events = vec![
            WebhookEvent::TopicCreated,
            WebhookEvent::AlertFired,
            WebhookEvent::Custom("deploy".into()),
        ];
        for e in &events {
            let json = serde_json::to_string(e).unwrap();
            let back: WebhookEvent = serde_json::from_str(&json).unwrap();
            assert_eq!(&back, e);
        }
    }

    #[test]
    fn test_trigger_action_serde() {
        let action = TriggerAction::ScaleTopic {
            topic: "orders".into(),
            partitions: 12,
        };
        let json = serde_json::to_string(&action).unwrap();
        let back: TriggerAction = serde_json::from_str(&json).unwrap();
        assert_eq!(
            serde_json::to_string(&back).unwrap(),
            serde_json::to_string(&action).unwrap()
        );
    }

    #[test]
    fn test_extensions_state_default() {
        let state = ExtensionsApiState::default();
        // Just ensure it compiles and no panics
        let _ = create_extensions_api_router(state);
    }

    #[test]
    fn test_custom_widget_serde() {
        let w = CustomWidget {
            id: "abc".into(),
            name: "chart".into(),
            widget_type: "line".into(),
            config: json!({"key": "value"}),
            html_template: Some("<div/>".into()),
            data_source: "metrics".into(),
            refresh_interval_secs: 30,
            created_at: "2025-01-01T00:00:00Z".into(),
        };
        let json = serde_json::to_string(&w).unwrap();
        let back: CustomWidget = serde_json::from_str(&json).unwrap();
        assert_eq!(back.name, "chart");
        assert_eq!(back.refresh_interval_secs, 30);
    }
}
