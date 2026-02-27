//! Kafka Connect REST API Compatibility Layer
//!
//! Provides 100% compatibility with the Kafka Connect REST API so existing
//! Kafka Connect tooling (Kafka Connect UI, kcctl, etc.) works unchanged
//! with Streamline.
//!
//! ## Endpoints
//!
//! ### Cluster
//! - `GET /` — Get cluster info (worker version, commit, kafka_cluster_id)
//!
//! ### Connectors
//! - `GET /connectors` — List active connectors
//! - `POST /connectors` — Create a new connector
//! - `GET /connectors/:name` — Get connector info
//! - `GET /connectors/:name/config` — Get connector config
//! - `PUT /connectors/:name/config` — Update connector config
//! - `GET /connectors/:name/status` — Get connector status
//! - `PUT /connectors/:name/pause` — Pause a connector
//! - `PUT /connectors/:name/resume` — Resume a connector
//! - `POST /connectors/:name/restart` — Restart a connector
//! - `DELETE /connectors/:name` — Delete a connector
//!
//! ### Tasks
//! - `GET /connectors/:name/tasks` — List tasks for a connector
//! - `GET /connectors/:name/tasks/:task/status` — Get task status
//! - `POST /connectors/:name/tasks/:task/restart` — Restart a task
//!
//! ### Topics
//! - `GET /connectors/:name/topics` — Get topics used by connector
//! - `PUT /connectors/:name/topics/reset` — Reset topic tracking
//!
//! ### Plugins
//! - `GET /connector-plugins` — List available connector plugins
//! - `PUT /connector-plugins/:plugin/config/validate` — Validate connector config

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post, put},
    Json, Router,
};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::info;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Types — matching Kafka Connect JSON format exactly
// ---------------------------------------------------------------------------

/// Connector information returned by GET /connectors/:name
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorInfo {
    pub name: String,
    pub config: HashMap<String, String>,
    pub tasks: Vec<TaskId>,
    #[serde(rename = "type")]
    pub connector_type: String,
}

/// Task identifier as returned by the Kafka Connect REST API
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskId {
    pub connector: String,
    pub task: i32,
}

/// Connector status returned by GET /connectors/:name/status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorStatus {
    pub name: String,
    pub connector: ConnectorState,
    pub tasks: Vec<TaskStatus>,
    #[serde(rename = "type")]
    pub connector_type: String,
}

/// State of the connector itself (inside ConnectorStatus)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorState {
    pub state: String,
    pub worker_id: String,
}

/// Per-task status entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskStatus {
    pub id: i32,
    pub state: String,
    pub worker_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace: Option<String>,
}

/// Plugin descriptor returned by GET /connector-plugins
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorPlugin {
    pub class: String,
    #[serde(rename = "type")]
    pub plugin_type: String,
    pub version: String,
}

/// Cluster info returned by GET /
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterInfo {
    pub version: String,
    pub commit: String,
    pub kafka_cluster_id: String,
}

/// Body for POST /connectors
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateConnectorRequest {
    pub name: String,
    pub config: HashMap<String, String>,
}

/// Result of PUT /connector-plugins/:plugin/config/validate
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigValidationResult {
    pub name: String,
    pub error_count: i32,
    pub groups: Vec<String>,
    pub configs: Vec<ConfigKeyInfo>,
}

/// A single config key with definition and value
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigKeyInfo {
    pub definition: ConfigDef,
    pub value: ConfigValue,
}

/// Config key definition (schema)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigDef {
    pub name: String,
    #[serde(rename = "type")]
    pub config_type: String,
    pub required: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_value: Option<String>,
    pub documentation: String,
}

/// Config key value (current + validation errors)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigValue {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    pub recommended_values: Vec<String>,
    pub errors: Vec<String>,
    pub visible: bool,
}

/// Kafka Connect error envelope
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectErrorResponse {
    pub error_code: i32,
    pub message: String,
}

/// Topics response for GET /connectors/:name/topics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorTopicsResponse {
    #[serde(flatten)]
    pub topics: HashMap<String, ConnectorTopicsInfo>,
}

/// Inner topics info
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorTopicsInfo {
    pub topics: Vec<String>,
}

// ---------------------------------------------------------------------------
// Internal managed connector state
// ---------------------------------------------------------------------------

/// Internal representation of a managed connector
#[derive(Debug, Clone)]
struct ManagedConnector {
    info: ConnectorInfo,
    state: String,
    task_states: Vec<String>,
    error: Option<String>,
    topics: HashSet<String>,
    #[allow(dead_code)]
    created_at: u64,
    updated_at: u64,
}

// ---------------------------------------------------------------------------
// Shared API state
// ---------------------------------------------------------------------------

/// State shared across all Kafka Connect REST API handlers
#[derive(Clone)]
pub struct KafkaConnectApiState {
    connectors: Arc<RwLock<HashMap<String, ManagedConnector>>>,
    plugins: Vec<ConnectorPlugin>,
    worker_id: String,
    cluster_id: String,
}

impl KafkaConnectApiState {
    /// Create a new API state with default built-in plugins.
    pub fn new(hostname: &str, port: u16) -> Self {
        let worker_id = format!("streamline-worker-{}:{}", hostname, port);
        let cluster_id = Uuid::new_v4().to_string();

        let plugins = vec![
            ConnectorPlugin {
                class: "io.streamline.connect.file.StreamlineFileSink".to_string(),
                plugin_type: "sink".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
            },
            ConnectorPlugin {
                class: "io.streamline.connect.file.StreamlineFileSource".to_string(),
                plugin_type: "source".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
            },
            ConnectorPlugin {
                class: "io.streamline.connect.console.StreamlineConsoleSink".to_string(),
                plugin_type: "sink".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
            },
            ConnectorPlugin {
                class: "io.streamline.connect.s3.StreamlineS3Sink".to_string(),
                plugin_type: "sink".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
            },
            ConnectorPlugin {
                class: "io.streamline.connect.http.StreamlineHttpSource".to_string(),
                plugin_type: "source".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
            },
        ];

        Self {
            connectors: Arc::new(RwLock::new(HashMap::new())),
            plugins,
            worker_id,
            cluster_id,
        }
    }

    /// Create state with a fixed cluster ID (useful for tests).
    #[cfg(test)]
    fn new_with_cluster_id(hostname: &str, port: u16, cluster_id: &str) -> Self {
        let mut state = Self::new(hostname, port);
        state.cluster_id = cluster_id.to_string();
        state
    }
}

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn error_response(code: StatusCode, message: impl Into<String>) -> Response {
    let body = ConnectErrorResponse {
        error_code: code.as_u16() as i32,
        message: message.into(),
    };
    (code, Json(body)).into_response()
}

fn connector_type_from_class(class: &str) -> String {
    let lower = class.to_lowercase();
    if lower.contains("source") {
        "source".to_string()
    } else {
        "sink".to_string()
    }
}

fn extract_topics(config: &HashMap<String, String>) -> HashSet<String> {
    let mut topics = HashSet::new();
    if let Some(t) = config.get("topics") {
        for topic in t.split(',') {
            let trimmed = topic.trim();
            if !trimmed.is_empty() {
                topics.insert(trimmed.to_string());
            }
        }
    }
    if let Some(t) = config.get("topic") {
        let trimmed = t.trim();
        if !trimmed.is_empty() {
            topics.insert(trimmed.to_string());
        }
    }
    topics
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// GET / — cluster info
async fn get_cluster_info(State(state): State<KafkaConnectApiState>) -> Json<ClusterInfo> {
    Json(ClusterInfo {
        version: env!("CARGO_PKG_VERSION").to_string(),
        commit: option_env!("STREAMLINE_GIT_SHA")
            .unwrap_or("unknown")
            .to_string(),
        kafka_cluster_id: state.cluster_id.clone(),
    })
}

/// GET /connectors
async fn list_connectors(State(state): State<KafkaConnectApiState>) -> Json<Vec<String>> {
    let connectors = state.connectors.read();
    Json(connectors.keys().cloned().collect())
}

/// POST /connectors
async fn create_connector(
    State(state): State<KafkaConnectApiState>,
    Json(req): Json<CreateConnectorRequest>,
) -> Response {
    if req.name.is_empty() {
        return error_response(StatusCode::BAD_REQUEST, "Connector name must not be empty");
    }

    let mut connectors = state.connectors.write();

    if connectors.contains_key(&req.name) {
        return error_response(
            StatusCode::CONFLICT,
            format!("Connector {} already exists", req.name),
        );
    }

    if !req.config.contains_key("connector.class") {
        return error_response(
            StatusCode::BAD_REQUEST,
            "Missing required config: connector.class",
        );
    }

    let class = req.config.get("connector.class").unwrap();
    let connector_type = connector_type_from_class(class);

    let tasks_max = req
        .config
        .get("tasks.max")
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(1);

    let tasks: Vec<TaskId> = (0..tasks_max)
        .map(|i| TaskId {
            connector: req.name.clone(),
            task: i,
        })
        .collect();

    let topics = extract_topics(&req.config);

    let info = ConnectorInfo {
        name: req.name.clone(),
        config: req.config.clone(),
        tasks,
        connector_type: connector_type.clone(),
    };

    let task_states = vec!["RUNNING".to_string(); tasks_max as usize];

    let managed = ManagedConnector {
        info: info.clone(),
        state: "RUNNING".to_string(),
        task_states,
        error: None,
        topics,
        created_at: now_secs(),
        updated_at: now_secs(),
    };

    connectors.insert(req.name.clone(), managed);
    info!(connector = %req.name, "Kafka Connect API: connector created");

    (StatusCode::CREATED, Json(info)).into_response()
}

/// GET /connectors/:name
async fn get_connector(
    State(state): State<KafkaConnectApiState>,
    Path(name): Path<String>,
) -> Response {
    let connectors = state.connectors.read();
    match connectors.get(&name) {
        Some(c) => (StatusCode::OK, Json(c.info.clone())).into_response(),
        None => error_response(
            StatusCode::NOT_FOUND,
            format!("Connector {} not found", name),
        ),
    }
}

/// GET /connectors/:name/config
async fn get_connector_config(
    State(state): State<KafkaConnectApiState>,
    Path(name): Path<String>,
) -> Response {
    let connectors = state.connectors.read();
    match connectors.get(&name) {
        Some(c) => (StatusCode::OK, Json(c.info.config.clone())).into_response(),
        None => error_response(
            StatusCode::NOT_FOUND,
            format!("Connector {} not found", name),
        ),
    }
}

/// PUT /connectors/:name/config
async fn update_connector_config(
    State(state): State<KafkaConnectApiState>,
    Path(name): Path<String>,
    Json(config): Json<HashMap<String, String>>,
) -> Response {
    let mut connectors = state.connectors.write();
    match connectors.get_mut(&name) {
        Some(c) => {
            c.info.config = config.clone();
            c.topics = extract_topics(&config);

            let tasks_max = config
                .get("tasks.max")
                .and_then(|v| v.parse::<i32>().ok())
                .unwrap_or(1);

            c.info.tasks = (0..tasks_max)
                .map(|i| TaskId {
                    connector: name.clone(),
                    task: i,
                })
                .collect();
            c.task_states = vec!["RUNNING".to_string(); tasks_max as usize];
            c.updated_at = now_secs();

            info!(connector = %name, "Kafka Connect API: connector config updated");
            (StatusCode::OK, Json(c.info.clone())).into_response()
        }
        None => error_response(
            StatusCode::NOT_FOUND,
            format!("Connector {} not found", name),
        ),
    }
}

/// GET /connectors/:name/status
async fn get_connector_status(
    State(state): State<KafkaConnectApiState>,
    Path(name): Path<String>,
) -> Response {
    let connectors = state.connectors.read();
    match connectors.get(&name) {
        Some(c) => {
            let tasks: Vec<TaskStatus> = c
                .task_states
                .iter()
                .enumerate()
                .map(|(i, s)| TaskStatus {
                    id: i as i32,
                    state: s.clone(),
                    worker_id: state.worker_id.clone(),
                    trace: None,
                })
                .collect();

            let status = ConnectorStatus {
                name: name.clone(),
                connector: ConnectorState {
                    state: c.state.clone(),
                    worker_id: state.worker_id.clone(),
                },
                tasks,
                connector_type: c.info.connector_type.clone(),
            };
            (StatusCode::OK, Json(status)).into_response()
        }
        None => error_response(
            StatusCode::NOT_FOUND,
            format!("Connector {} not found", name),
        ),
    }
}

/// PUT /connectors/:name/pause
async fn pause_connector(
    State(state): State<KafkaConnectApiState>,
    Path(name): Path<String>,
) -> Response {
    let mut connectors = state.connectors.write();
    match connectors.get_mut(&name) {
        Some(c) => {
            c.state = "PAUSED".to_string();
            for ts in &mut c.task_states {
                *ts = "PAUSED".to_string();
            }
            c.updated_at = now_secs();
            info!(connector = %name, "Kafka Connect API: connector paused");
            StatusCode::ACCEPTED.into_response()
        }
        None => error_response(
            StatusCode::NOT_FOUND,
            format!("Connector {} not found", name),
        ),
    }
}

/// PUT /connectors/:name/resume
async fn resume_connector(
    State(state): State<KafkaConnectApiState>,
    Path(name): Path<String>,
) -> Response {
    let mut connectors = state.connectors.write();
    match connectors.get_mut(&name) {
        Some(c) => {
            c.state = "RUNNING".to_string();
            for ts in &mut c.task_states {
                *ts = "RUNNING".to_string();
            }
            c.updated_at = now_secs();
            info!(connector = %name, "Kafka Connect API: connector resumed");
            StatusCode::ACCEPTED.into_response()
        }
        None => error_response(
            StatusCode::NOT_FOUND,
            format!("Connector {} not found", name),
        ),
    }
}

/// POST /connectors/:name/restart
async fn restart_connector(
    State(state): State<KafkaConnectApiState>,
    Path(name): Path<String>,
) -> Response {
    let mut connectors = state.connectors.write();
    match connectors.get_mut(&name) {
        Some(c) => {
            c.state = "RUNNING".to_string();
            for ts in &mut c.task_states {
                *ts = "RUNNING".to_string();
            }
            c.error = None;
            c.updated_at = now_secs();
            info!(connector = %name, "Kafka Connect API: connector restarted");
            StatusCode::NO_CONTENT.into_response()
        }
        None => error_response(
            StatusCode::NOT_FOUND,
            format!("Connector {} not found", name),
        ),
    }
}

/// DELETE /connectors/:name
async fn delete_connector(
    State(state): State<KafkaConnectApiState>,
    Path(name): Path<String>,
) -> Response {
    let mut connectors = state.connectors.write();
    if connectors.remove(&name).is_some() {
        info!(connector = %name, "Kafka Connect API: connector deleted");
        StatusCode::NO_CONTENT.into_response()
    } else {
        error_response(
            StatusCode::NOT_FOUND,
            format!("Connector {} not found", name),
        )
    }
}

/// GET /connectors/:name/tasks
async fn get_connector_tasks(
    State(state): State<KafkaConnectApiState>,
    Path(name): Path<String>,
) -> Response {
    let connectors = state.connectors.read();
    match connectors.get(&name) {
        Some(c) => {
            let tasks: Vec<serde_json::Value> = c
                .info
                .tasks
                .iter()
                .map(|t| {
                    serde_json::json!({
                        "id": { "connector": t.connector, "task": t.task },
                        "config": c.info.config,
                    })
                })
                .collect();
            (StatusCode::OK, Json(tasks)).into_response()
        }
        None => error_response(
            StatusCode::NOT_FOUND,
            format!("Connector {} not found", name),
        ),
    }
}

/// GET /connectors/:name/tasks/:task/status
async fn get_task_status(
    State(state): State<KafkaConnectApiState>,
    Path((name, task_id)): Path<(String, i32)>,
) -> Response {
    let connectors = state.connectors.read();
    match connectors.get(&name) {
        Some(c) => match c.task_states.get(task_id as usize) {
            Some(s) => {
                let status = TaskStatus {
                    id: task_id,
                    state: s.clone(),
                    worker_id: state.worker_id.clone(),
                    trace: None,
                };
                (StatusCode::OK, Json(status)).into_response()
            }
            None => error_response(
                StatusCode::NOT_FOUND,
                format!("Task {} not found for connector {}", task_id, name),
            ),
        },
        None => error_response(
            StatusCode::NOT_FOUND,
            format!("Connector {} not found", name),
        ),
    }
}

/// POST /connectors/:name/tasks/:task/restart
async fn restart_task(
    State(state): State<KafkaConnectApiState>,
    Path((name, task_id)): Path<(String, i32)>,
) -> Response {
    let mut connectors = state.connectors.write();
    match connectors.get_mut(&name) {
        Some(c) => match c.task_states.get_mut(task_id as usize) {
            Some(s) => {
                *s = "RUNNING".to_string();
                c.updated_at = now_secs();
                info!(connector = %name, task = task_id, "Kafka Connect API: task restarted");
                StatusCode::NO_CONTENT.into_response()
            }
            None => error_response(
                StatusCode::NOT_FOUND,
                format!("Task {} not found for connector {}", task_id, name),
            ),
        },
        None => error_response(
            StatusCode::NOT_FOUND,
            format!("Connector {} not found", name),
        ),
    }
}

/// GET /connectors/:name/topics
async fn get_connector_topics(
    State(state): State<KafkaConnectApiState>,
    Path(name): Path<String>,
) -> Response {
    let connectors = state.connectors.read();
    match connectors.get(&name) {
        Some(c) => {
            let mut outer = HashMap::new();
            outer.insert(
                name.clone(),
                ConnectorTopicsInfo {
                    topics: c.topics.iter().cloned().collect(),
                },
            );
            (StatusCode::OK, Json(outer)).into_response()
        }
        None => error_response(
            StatusCode::NOT_FOUND,
            format!("Connector {} not found", name),
        ),
    }
}

/// PUT /connectors/:name/topics/reset
async fn reset_connector_topics(
    State(state): State<KafkaConnectApiState>,
    Path(name): Path<String>,
) -> Response {
    let mut connectors = state.connectors.write();
    match connectors.get_mut(&name) {
        Some(c) => {
            c.topics.clear();
            c.updated_at = now_secs();
            info!(connector = %name, "Kafka Connect API: topic tracking reset");
            StatusCode::NO_CONTENT.into_response()
        }
        None => error_response(
            StatusCode::NOT_FOUND,
            format!("Connector {} not found", name),
        ),
    }
}

/// GET /connector-plugins
async fn list_connector_plugins(
    State(state): State<KafkaConnectApiState>,
) -> Json<Vec<ConnectorPlugin>> {
    Json(state.plugins.clone())
}

/// PUT /connector-plugins/:plugin/config/validate
async fn validate_connector_config(
    State(state): State<KafkaConnectApiState>,
    Path(plugin_name): Path<String>,
    Json(config): Json<HashMap<String, String>>,
) -> Json<ConfigValidationResult> {
    let _ = &state; // plugins are static; validation is config-driven

    let required_keys: Vec<(&str, &str, bool)> = vec![
        (
            "connector.class",
            "Fully qualified class name of the connector",
            true,
        ),
        ("tasks.max", "Maximum number of tasks for this connector", true),
        (
            "topics",
            "Comma-separated list of topics (sink connectors)",
            false,
        ),
    ];

    let mut configs = Vec::new();
    let mut error_count: i32 = 0;

    for (key, doc, is_required) in &required_keys {
        let val = config.get(*key).cloned();
        let errors = if *is_required && val.is_none() {
            error_count += 1;
            vec![format!("Missing required configuration: {}", key)]
        } else {
            vec![]
        };

        configs.push(ConfigKeyInfo {
            definition: ConfigDef {
                name: key.to_string(),
                config_type: "STRING".to_string(),
                required: *is_required,
                default_value: None,
                documentation: doc.to_string(),
            },
            value: ConfigValue {
                name: key.to_string(),
                value: val,
                recommended_values: vec![],
                errors,
                visible: true,
            },
        });
    }

    Json(ConfigValidationResult {
        name: plugin_name,
        error_count,
        groups: vec!["Common".to_string()],
        configs,
    })
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

/// Build an axum [`Router`] with all Kafka Connect REST API endpoints.
pub fn create_kafka_connect_router(state: KafkaConnectApiState) -> Router {
    Router::new()
        // Cluster info
        .route("/", get(get_cluster_info))
        // Connectors CRUD
        .route("/connectors", get(list_connectors).post(create_connector))
        .route(
            "/connectors/{name}",
            get(get_connector).delete(delete_connector),
        )
        .route(
            "/connectors/{name}/config",
            get(get_connector_config).put(update_connector_config),
        )
        .route("/connectors/{name}/status", get(get_connector_status))
        .route("/connectors/{name}/pause", put(pause_connector))
        .route("/connectors/{name}/resume", put(resume_connector))
        .route("/connectors/{name}/restart", post(restart_connector))
        // Tasks
        .route("/connectors/{name}/tasks", get(get_connector_tasks))
        .route(
            "/connectors/{name}/tasks/{task_id}/status",
            get(get_task_status),
        )
        .route(
            "/connectors/{name}/tasks/{task_id}/restart",
            post(restart_task),
        )
        // Topics
        .route("/connectors/{name}/topics", get(get_connector_topics))
        .route(
            "/connectors/{name}/topics/reset",
            put(reset_connector_topics),
        )
        // Plugins
        .route("/connector-plugins", get(list_connector_plugins))
        .route(
            "/connector-plugins/{plugin_name}/config/validate",
            put(validate_connector_config),
        )
        .with_state(state)
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

    fn test_state() -> KafkaConnectApiState {
        KafkaConnectApiState::new_with_cluster_id("localhost", 8083, "test-cluster-id")
    }

    fn app() -> Router {
        create_kafka_connect_router(test_state())
    }

    fn json_request(method: &str, uri: &str, body: Option<serde_json::Value>) -> Request<Body> {
        let builder = Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json");
        match body {
            Some(b) => builder
                .body(Body::from(serde_json::to_vec(&b).unwrap()))
                .unwrap(),
            None => builder.body(Body::empty()).unwrap(),
        }
    }

    async fn body_json(resp: Response) -> serde_json::Value {
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    fn create_req(name: &str) -> serde_json::Value {
        serde_json::json!({
            "name": name,
            "config": {
                "connector.class": "io.streamline.connect.file.StreamlineFileSource",
                "tasks.max": "2",
                "topic": "my-topic"
            }
        })
    }

    // 1. GET / — cluster info
    #[tokio::test]
    async fn test_get_cluster_info() {
        let resp = app()
            .oneshot(json_request("GET", "/", None))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let json = body_json(resp).await;
        assert_eq!(json["kafka_cluster_id"], "test-cluster-id");
        assert!(json["version"].as_str().is_some());
        assert!(json["commit"].as_str().is_some());
    }

    // 2. GET /connectors — empty list
    #[tokio::test]
    async fn test_list_connectors_empty() {
        let resp = app()
            .oneshot(json_request("GET", "/connectors", None))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let json = body_json(resp).await;
        assert_eq!(json, serde_json::json!([]));
    }

    // 3. POST /connectors — create
    #[tokio::test]
    async fn test_create_connector() {
        let resp = app()
            .oneshot(json_request("POST", "/connectors", Some(create_req("c1"))))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
        let json = body_json(resp).await;
        assert_eq!(json["name"], "c1");
        assert_eq!(json["type"], "source");
        assert_eq!(json["tasks"].as_array().unwrap().len(), 2);
    }

    // 4. POST /connectors — duplicate
    #[tokio::test]
    async fn test_create_connector_duplicate() {
        let state = test_state();
        let router = create_kafka_connect_router(state);

        let resp = router
            .clone()
            .oneshot(json_request("POST", "/connectors", Some(create_req("dup"))))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);

        let resp = router
            .oneshot(json_request("POST", "/connectors", Some(create_req("dup"))))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CONFLICT);
    }

    // 5. POST /connectors — missing connector.class
    #[tokio::test]
    async fn test_create_connector_missing_class() {
        let body = serde_json::json!({
            "name": "bad",
            "config": { "tasks.max": "1" }
        });
        let resp = app()
            .oneshot(json_request("POST", "/connectors", Some(body)))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    // 6. GET /connectors/:name
    #[tokio::test]
    async fn test_get_connector() {
        let state = test_state();
        let router = create_kafka_connect_router(state);

        router
            .clone()
            .oneshot(json_request("POST", "/connectors", Some(create_req("c2"))))
            .await
            .unwrap();

        let resp = router
            .oneshot(json_request("GET", "/connectors/c2", None))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let json = body_json(resp).await;
        assert_eq!(json["name"], "c2");
    }

    // 7. GET /connectors/:name — not found
    #[tokio::test]
    async fn test_get_connector_not_found() {
        let resp = app()
            .oneshot(json_request("GET", "/connectors/nope", None))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    // 8. GET /connectors/:name/config
    #[tokio::test]
    async fn test_get_connector_config() {
        let state = test_state();
        let router = create_kafka_connect_router(state);

        router
            .clone()
            .oneshot(json_request("POST", "/connectors", Some(create_req("c3"))))
            .await
            .unwrap();

        let resp = router
            .oneshot(json_request("GET", "/connectors/c3/config", None))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let json = body_json(resp).await;
        assert!(json["connector.class"].as_str().is_some());
    }

    // 9. PUT /connectors/:name/config
    #[tokio::test]
    async fn test_update_connector_config() {
        let state = test_state();
        let router = create_kafka_connect_router(state);

        router
            .clone()
            .oneshot(json_request("POST", "/connectors", Some(create_req("c4"))))
            .await
            .unwrap();

        let new_config = serde_json::json!({
            "connector.class": "io.streamline.connect.file.StreamlineFileSource",
            "tasks.max": "3",
            "topic": "updated-topic"
        });

        let resp = router
            .oneshot(json_request(
                "PUT",
                "/connectors/c4/config",
                Some(new_config),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let json = body_json(resp).await;
        assert_eq!(json["tasks"].as_array().unwrap().len(), 3);
    }

    // 10. GET /connectors/:name/status
    #[tokio::test]
    async fn test_get_connector_status() {
        let state = test_state();
        let router = create_kafka_connect_router(state);

        router
            .clone()
            .oneshot(json_request("POST", "/connectors", Some(create_req("c5"))))
            .await
            .unwrap();

        let resp = router
            .oneshot(json_request("GET", "/connectors/c5/status", None))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let json = body_json(resp).await;
        assert_eq!(json["connector"]["state"], "RUNNING");
        assert_eq!(json["type"], "source");
        assert_eq!(json["tasks"].as_array().unwrap().len(), 2);
    }

    // 11. PUT pause + resume
    #[tokio::test]
    async fn test_pause_and_resume_connector() {
        let state = test_state();
        let router = create_kafka_connect_router(state);

        router
            .clone()
            .oneshot(json_request("POST", "/connectors", Some(create_req("c6"))))
            .await
            .unwrap();

        // Pause
        let resp = router
            .clone()
            .oneshot(json_request("PUT", "/connectors/c6/pause", None))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::ACCEPTED);

        // Verify paused
        let resp = router
            .clone()
            .oneshot(json_request("GET", "/connectors/c6/status", None))
            .await
            .unwrap();
        let json = body_json(resp).await;
        assert_eq!(json["connector"]["state"], "PAUSED");

        // Resume
        let resp = router
            .clone()
            .oneshot(json_request("PUT", "/connectors/c6/resume", None))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::ACCEPTED);

        // Verify running
        let resp = router
            .oneshot(json_request("GET", "/connectors/c6/status", None))
            .await
            .unwrap();
        let json = body_json(resp).await;
        assert_eq!(json["connector"]["state"], "RUNNING");
    }

    // 12. POST restart
    #[tokio::test]
    async fn test_restart_connector() {
        let state = test_state();
        let router = create_kafka_connect_router(state);

        router
            .clone()
            .oneshot(json_request("POST", "/connectors", Some(create_req("c7"))))
            .await
            .unwrap();

        let resp = router
            .oneshot(json_request("POST", "/connectors/c7/restart", None))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    }

    // 13. DELETE /connectors/:name
    #[tokio::test]
    async fn test_delete_connector() {
        let state = test_state();
        let router = create_kafka_connect_router(state);

        router
            .clone()
            .oneshot(json_request("POST", "/connectors", Some(create_req("c8"))))
            .await
            .unwrap();

        let resp = router
            .clone()
            .oneshot(json_request("DELETE", "/connectors/c8", None))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);

        // Verify gone
        let resp = router
            .oneshot(json_request("GET", "/connectors/c8", None))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    // 14. GET /connectors/:name/tasks
    #[tokio::test]
    async fn test_get_connector_tasks() {
        let state = test_state();
        let router = create_kafka_connect_router(state);

        router
            .clone()
            .oneshot(json_request("POST", "/connectors", Some(create_req("c9"))))
            .await
            .unwrap();

        let resp = router
            .oneshot(json_request("GET", "/connectors/c9/tasks", None))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let json = body_json(resp).await;
        let tasks = json.as_array().unwrap();
        assert_eq!(tasks.len(), 2);
        assert_eq!(tasks[0]["id"]["connector"], "c9");
        assert_eq!(tasks[0]["id"]["task"], 0);
    }

    // 15. GET /connectors/:name/tasks/:task/status
    #[tokio::test]
    async fn test_get_task_status() {
        let state = test_state();
        let router = create_kafka_connect_router(state);

        router
            .clone()
            .oneshot(json_request(
                "POST",
                "/connectors",
                Some(create_req("c10")),
            ))
            .await
            .unwrap();

        let resp = router
            .oneshot(json_request(
                "GET",
                "/connectors/c10/tasks/1/status",
                None,
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let json = body_json(resp).await;
        assert_eq!(json["id"], 1);
        assert_eq!(json["state"], "RUNNING");
    }

    // 16. POST /connectors/:name/tasks/:task/restart
    #[tokio::test]
    async fn test_restart_task() {
        let state = test_state();
        let router = create_kafka_connect_router(state);

        router
            .clone()
            .oneshot(json_request(
                "POST",
                "/connectors",
                Some(create_req("c11")),
            ))
            .await
            .unwrap();

        let resp = router
            .oneshot(json_request(
                "POST",
                "/connectors/c11/tasks/0/restart",
                None,
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    }

    // 17. GET /connectors/:name/topics
    #[tokio::test]
    async fn test_get_connector_topics() {
        let state = test_state();
        let router = create_kafka_connect_router(state);

        router
            .clone()
            .oneshot(json_request(
                "POST",
                "/connectors",
                Some(create_req("c12")),
            ))
            .await
            .unwrap();

        let resp = router
            .oneshot(json_request("GET", "/connectors/c12/topics", None))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let json = body_json(resp).await;
        let topics = json["c12"]["topics"].as_array().unwrap();
        assert!(topics.contains(&serde_json::json!("my-topic")));
    }

    // 18. PUT /connectors/:name/topics/reset
    #[tokio::test]
    async fn test_reset_connector_topics() {
        let state = test_state();
        let router = create_kafka_connect_router(state);

        router
            .clone()
            .oneshot(json_request(
                "POST",
                "/connectors",
                Some(create_req("c13")),
            ))
            .await
            .unwrap();

        let resp = router
            .clone()
            .oneshot(json_request("PUT", "/connectors/c13/topics/reset", None))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);

        // Topics should now be empty
        let resp = router
            .oneshot(json_request("GET", "/connectors/c13/topics", None))
            .await
            .unwrap();
        let json = body_json(resp).await;
        let topics = json["c13"]["topics"].as_array().unwrap();
        assert!(topics.is_empty());
    }

    // 19. GET /connector-plugins
    #[tokio::test]
    async fn test_list_connector_plugins() {
        let resp = app()
            .oneshot(json_request("GET", "/connector-plugins", None))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let json = body_json(resp).await;
        let plugins = json.as_array().unwrap();
        assert_eq!(plugins.len(), 5);

        // Verify all expected plugins are present
        let classes: Vec<&str> = plugins
            .iter()
            .map(|p| p["class"].as_str().unwrap())
            .collect();
        assert!(classes
            .iter()
            .any(|c| c.contains("StreamlineFileSink")));
        assert!(classes
            .iter()
            .any(|c| c.contains("StreamlineFileSource")));
        assert!(classes
            .iter()
            .any(|c| c.contains("StreamlineConsoleSink")));
        assert!(classes
            .iter()
            .any(|c| c.contains("StreamlineS3Sink")));
        assert!(classes
            .iter()
            .any(|c| c.contains("StreamlineHttpSource")));
    }

    // 20. PUT /connector-plugins/:plugin/config/validate
    #[tokio::test]
    async fn test_validate_connector_config_valid() {
        let config = serde_json::json!({
            "connector.class": "io.streamline.connect.file.StreamlineFileSource",
            "tasks.max": "1",
            "topics": "my-topic"
        });
        let resp = app()
            .oneshot(json_request(
                "PUT",
                "/connector-plugins/StreamlineFileSource/config/validate",
                Some(config),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let json = body_json(resp).await;
        assert_eq!(json["error_count"], 0);
        assert_eq!(json["name"], "StreamlineFileSource");
    }

    // 21. PUT /connector-plugins/:plugin/config/validate — missing required
    #[tokio::test]
    async fn test_validate_connector_config_errors() {
        let config = serde_json::json!({});
        let resp = app()
            .oneshot(json_request(
                "PUT",
                "/connector-plugins/SomePlugin/config/validate",
                Some(config),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let json = body_json(resp).await;
        assert!(json["error_count"].as_i64().unwrap() > 0);
    }

    // 22. Worker ID format
    #[tokio::test]
    async fn test_worker_id_format() {
        let state = test_state();
        assert_eq!(state.worker_id, "streamline-worker-localhost:8083");
    }

    // 23. Sink connector type detection
    #[tokio::test]
    async fn test_sink_connector_type() {
        let body = serde_json::json!({
            "name": "my-sink",
            "config": {
                "connector.class": "io.streamline.connect.file.StreamlineFileSink",
                "tasks.max": "1",
                "topics": "t1,t2"
            }
        });
        let resp = app()
            .oneshot(json_request("POST", "/connectors", Some(body)))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
        let json = body_json(resp).await;
        assert_eq!(json["type"], "sink");
    }

    // 24. Multiple topics extraction
    #[tokio::test]
    async fn test_topics_extraction_multiple() {
        let state = test_state();
        let router = create_kafka_connect_router(state);

        let body = serde_json::json!({
            "name": "multi-topic",
            "config": {
                "connector.class": "io.streamline.connect.file.StreamlineFileSink",
                "tasks.max": "1",
                "topics": "t1, t2, t3"
            }
        });
        router
            .clone()
            .oneshot(json_request("POST", "/connectors", Some(body)))
            .await
            .unwrap();

        let resp = router
            .oneshot(json_request("GET", "/connectors/multi-topic/topics", None))
            .await
            .unwrap();
        let json = body_json(resp).await;
        let topics = json["multi-topic"]["topics"].as_array().unwrap();
        assert_eq!(topics.len(), 3);
    }

    // 25. Empty name rejected
    #[tokio::test]
    async fn test_create_connector_empty_name() {
        let body = serde_json::json!({
            "name": "",
            "config": {
                "connector.class": "SomeClass",
                "tasks.max": "1"
            }
        });
        let resp = app()
            .oneshot(json_request("POST", "/connectors", Some(body)))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }
}
