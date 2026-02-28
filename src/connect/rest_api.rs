//! Kafka Connect REST API compatibility layer.
//!
//! Implements the Kafka Connect REST API v3 endpoints for connector management.
//! See: https://docs.confluent.io/platform/current/connect/references/restapi.html

use axum::{
    extract::{Json, Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post, put},
    Router,
};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Shared state
// ---------------------------------------------------------------------------

/// Shared state for the Connect REST API.
#[derive(Clone)]
pub struct ConnectState {
    pub connectors: Arc<DashMap<String, ConnectorInfo>>,
}

impl ConnectState {
    pub fn new() -> Self {
        Self {
            connectors: Arc::new(DashMap::new()),
        }
    }

    /// Save connector state to a JSON file.
    pub fn save_to_file(&self, path: &std::path::Path) -> Result<(), std::io::Error> {
        let connectors: HashMap<String, ConnectorInfo> = self.connectors
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect();
        let json = serde_json::to_string_pretty(&connectors)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        std::fs::write(path, json)
    }

    /// Load connector state from a JSON file.
    pub fn load_from_file(path: &std::path::Path) -> Result<Self, std::io::Error> {
        let state = Self::new();
        if path.exists() {
            let json = std::fs::read_to_string(path)?;
            let connectors: HashMap<String, ConnectorInfo> = serde_json::from_str(&json)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            for (name, info) in connectors {
                state.connectors.insert(name, info);
            }
        }
        Ok(state)
    }
}

// ---------------------------------------------------------------------------
// Types — matching Kafka Connect REST API JSON format
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorInfo {
    pub name: String,
    #[serde(rename = "type")]
    pub connector_type: String,
    pub config: HashMap<String, String>,
    pub tasks: Vec<TaskId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskId {
    pub connector: String,
    pub task: i32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConnectorStatus {
    pub name: String,
    pub connector: ConnectorState,
    pub tasks: Vec<TaskStatus>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConnectorState {
    pub state: String,
    pub worker_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TaskStatus {
    pub id: i32,
    pub state: String,
    pub worker_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct CreateConnectorRequest {
    pub name: String,
    pub config: HashMap<String, String>,
}

// ---------------------------------------------------------------------------
// Handlers — stub implementations returning placeholder data
// ---------------------------------------------------------------------------

/// GET /connectors — List all connectors
async fn list_connectors(State(state): State<ConnectState>) -> impl IntoResponse {
    let names: Vec<String> = state.connectors.iter().map(|e| e.key().clone()).collect();
    Json(names)
}

/// POST /connectors — Create a new connector
async fn create_connector(
    State(state): State<ConnectState>,
    Json(request): Json<CreateConnectorRequest>,
) -> impl IntoResponse {
    let info = ConnectorInfo {
        name: request.name.clone(),
        connector_type: request
            .config
            .get("connector.class")
            .cloned()
            .unwrap_or_default(),
        config: request.config,
        tasks: vec![TaskId {
            connector: request.name.clone(),
            task: 0,
        }],
    };
    state.connectors.insert(request.name, info.clone());
    (StatusCode::CREATED, Json(info))
}

/// GET /connectors/{name} — Get connector info
async fn get_connector(
    State(state): State<ConnectState>,
    Path(name): Path<String>,
) -> Result<Json<ConnectorInfo>, StatusCode> {
    state
        .connectors
        .get(&name)
        .map(|e| Json(e.value().clone()))
        .ok_or(StatusCode::NOT_FOUND)
}

/// GET /connectors/{name}/config — Get connector config
async fn get_connector_config(
    State(state): State<ConnectState>,
    Path(name): Path<String>,
) -> Result<Json<HashMap<String, String>>, StatusCode> {
    state
        .connectors
        .get(&name)
        .map(|e| Json(e.value().config.clone()))
        .ok_or(StatusCode::NOT_FOUND)
}

/// PUT /connectors/{name}/config — Update connector config
async fn update_connector_config(
    State(state): State<ConnectState>,
    Path(name): Path<String>,
    Json(config): Json<HashMap<String, String>>,
) -> Result<Json<ConnectorInfo>, StatusCode> {
    if let Some(mut entry) = state.connectors.get_mut(&name) {
        entry.config = config.clone();
        entry.connector_type = config
            .get("connector.class")
            .cloned()
            .unwrap_or_default();
        Ok(Json(entry.value().clone()))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

/// GET /connectors/{name}/status — Get connector status
async fn get_connector_status(
    State(state): State<ConnectState>,
    Path(name): Path<String>,
) -> Result<Json<ConnectorStatus>, StatusCode> {
    state
        .connectors
        .get(&name)
        .map(|e| {
            Json(ConnectorStatus {
                name: e.key().clone(),
                connector: ConnectorState {
                    state: "RUNNING".to_string(),
                    worker_id: "streamline:9092".to_string(),
                },
                tasks: e
                    .value()
                    .tasks
                    .iter()
                    .map(|t| TaskStatus {
                        id: t.task,
                        state: "RUNNING".to_string(),
                        worker_id: "streamline:9092".to_string(),
                        trace: None,
                    })
                    .collect(),
            })
        })
        .ok_or(StatusCode::NOT_FOUND)
}

/// DELETE /connectors/{name} — Delete a connector
async fn delete_connector(
    State(state): State<ConnectState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    state.connectors.remove(&name);
    StatusCode::NO_CONTENT
}

/// POST /connectors/{name}/restart — Restart a connector
async fn restart_connector(Path(name): Path<String>) -> impl IntoResponse {
    let _ = name;
    StatusCode::NO_CONTENT
}

/// PUT /connectors/{name}/pause — Pause a connector
async fn pause_connector(Path(name): Path<String>) -> impl IntoResponse {
    let _ = name;
    StatusCode::ACCEPTED
}

/// PUT /connectors/{name}/resume — Resume a connector
async fn resume_connector(Path(name): Path<String>) -> impl IntoResponse {
    let _ = name;
    StatusCode::ACCEPTED
}

/// GET /connectors/{name}/tasks — List connector tasks
async fn list_tasks(
    State(state): State<ConnectState>,
    Path(name): Path<String>,
) -> Result<Json<Vec<TaskId>>, StatusCode> {
    state
        .connectors
        .get(&name)
        .map(|e| Json(e.value().tasks.clone()))
        .ok_or(StatusCode::NOT_FOUND)
}

/// GET /connectors/{name}/tasks/{task_id}/status — Get task status
async fn get_task_status(Path((_name, task_id)): Path<(String, i32)>) -> impl IntoResponse {
    Json(TaskStatus {
        id: task_id,
        state: "RUNNING".to_string(),
        worker_id: "streamline:9092".to_string(),
        trace: None,
    })
}

/// GET /connector-plugins — List available connector plugins
async fn list_plugins() -> impl IntoResponse {
    Json(vec![
        serde_json::json!({
            "class": "io.streamline.connect.file.FileStreamSourceConnector",
            "type": "source",
            "version": "0.2.0"
        }),
        serde_json::json!({
            "class": "io.streamline.connect.file.FileStreamSinkConnector",
            "type": "sink",
            "version": "0.2.0"
        }),
    ])
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

/// Build the Kafka Connect REST API router.
pub fn connect_router() -> Router<ConnectState> {
    Router::new()
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
        .route("/connectors/{name}/restart", post(restart_connector))
        .route("/connectors/{name}/pause", put(pause_connector))
        .route("/connectors/{name}/resume", put(resume_connector))
        .route("/connectors/{name}/tasks", get(list_tasks))
        .route(
            "/connectors/{name}/tasks/{task_id}/status",
            get(get_task_status),
        )
        .route("/connector-plugins", get(list_plugins))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode as SC};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    fn app() -> Router {
        connect_router().with_state(ConnectState::new())
    }

    #[tokio::test]
    async fn test_list_connectors_empty() {
        let resp = app()
            .oneshot(Request::get("/connectors").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), SC::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let names: Vec<String> = serde_json::from_slice(&body).unwrap();
        assert!(names.is_empty());
    }

    #[tokio::test]
    async fn test_create_and_get_connector() {
        let state = ConnectState::new();
        let app = connect_router().with_state(state);

        let create_body = serde_json::json!({
            "name": "test-connector",
            "config": {"connector.class": "TestSink"}
        });
        let resp = app
            .clone()
            .oneshot(
                Request::post("/connectors")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&create_body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), SC::CREATED);

        let resp = app
            .oneshot(
                Request::get("/connectors/test-connector")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), SC::OK);
    }

    #[tokio::test]
    async fn test_get_nonexistent_connector() {
        let resp = app()
            .oneshot(
                Request::get("/connectors/does-not-exist")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), SC::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_delete_connector() {
        let state = ConnectState::new();
        let app = connect_router().with_state(state);

        let create_body = serde_json::json!({
            "name": "to-delete",
            "config": {"connector.class": "X"}
        });
        let _ = app
            .clone()
            .oneshot(
                Request::post("/connectors")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&create_body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        let resp = app
            .clone()
            .oneshot(
                Request::delete("/connectors/to-delete")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), SC::NO_CONTENT);

        let resp = app
            .oneshot(
                Request::get("/connectors/to-delete")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), SC::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_list_plugins() {
        let resp = app()
            .oneshot(
                Request::get("/connector-plugins")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), SC::OK);
    }

    #[test]
    fn test_save_and_load() {
        let state = ConnectState::new();
        let info = ConnectorInfo {
            name: "test".to_string(),
            connector_type: "TestSink".to_string(),
            config: HashMap::new(),
            tasks: vec![],
        };
        state.connectors.insert("test".to_string(), info);

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("connectors.json");
        state.save_to_file(&path).unwrap();
        assert!(path.exists());

        let loaded = ConnectState::load_from_file(&path).unwrap();
        assert!(loaded.connectors.contains_key("test"));
        assert_eq!(loaded.connectors.get("test").unwrap().connector_type, "TestSink");
    }

    #[test]
    fn test_load_from_nonexistent_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("does_not_exist.json");
        let loaded = ConnectState::load_from_file(&path).unwrap();
        assert!(loaded.connectors.is_empty());
    }
}
