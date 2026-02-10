//! Playground API - Interactive learning and exploration endpoints
//!
//! Provides browser-based playground functionality:
//!
//! ## Endpoints
//!
//! - `GET /api/v1/playground/status` - Playground status and capabilities
//! - `POST /api/v1/playground/sessions` - Create a new playground session
//! - `GET /api/v1/playground/sessions/:id` - Get session state
//! - `POST /api/v1/playground/sessions/:id/produce` - Produce a message in session
//! - `POST /api/v1/playground/sessions/:id/consume` - Consume messages in session
//! - `GET /api/v1/playground/tutorials` - List available tutorials
//! - `GET /api/v1/playground/tutorials/:id` - Get tutorial details
//! - `GET /api/v1/playground/samples` - List sample datasets
//! - `POST /api/v1/playground/samples/:id/load` - Load a sample dataset

use crate::storage::TopicManager;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use bytes::Bytes;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

/// Shared state for Playground API
#[derive(Clone)]
pub struct PlaygroundApiState {
    pub topic_manager: Arc<TopicManager>,
    pub sessions: Arc<RwLock<HashMap<String, PlaygroundSession>>>,
}

impl PlaygroundApiState {
    pub fn new(topic_manager: Arc<TopicManager>) -> Self {
        Self {
            topic_manager,
            sessions: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

/// Playground session
#[derive(Debug, Clone, Serialize)]
pub struct PlaygroundSession {
    pub id: String,
    pub created_at: i64,
    pub topics: Vec<String>,
    pub messages_produced: u64,
    pub messages_consumed: u64,
}

/// Create session request
#[derive(Debug, Deserialize)]
pub struct CreateSessionRequest {
    #[serde(default = "default_session_topics")]
    pub topics: Vec<TopicSetup>,
}

fn default_session_topics() -> Vec<TopicSetup> {
    vec![TopicSetup {
        name: "playground-events".to_string(),
        partitions: Some(1),
    }]
}

/// Topic setup for a session
#[derive(Debug, Deserialize)]
pub struct TopicSetup {
    pub name: String,
    #[serde(default)]
    pub partitions: Option<u32>,
}

/// Produce request
#[derive(Debug, Deserialize)]
pub struct PlaygroundProduceRequest {
    pub topic: String,
    pub key: Option<String>,
    pub value: String,
    #[serde(default)]
    pub headers: HashMap<String, String>,
}

/// Produce response
#[derive(Debug, Serialize)]
pub struct PlaygroundProduceResponse {
    pub offset: i64,
    pub partition: u32,
    pub topic: String,
}

/// Consume request
#[derive(Debug, Deserialize)]
pub struct PlaygroundConsumeRequest {
    pub topic: String,
    #[serde(default)]
    pub offset: Option<i64>,
    #[serde(default = "default_max_messages")]
    pub max_messages: usize,
}

fn default_max_messages() -> usize {
    10
}

/// Consumed message
#[derive(Debug, Serialize)]
pub struct PlaygroundMessage {
    pub offset: i64,
    pub partition: u32,
    pub key: Option<String>,
    pub value: String,
    pub timestamp: i64,
}

/// Tutorial definition
#[derive(Debug, Clone, Serialize)]
pub struct Tutorial {
    pub id: String,
    pub title: String,
    pub description: String,
    pub difficulty: String,
    pub steps: Vec<TutorialStep>,
    pub estimated_minutes: u32,
}

/// Tutorial step
#[derive(Debug, Clone, Serialize)]
pub struct TutorialStep {
    pub title: String,
    pub description: String,
    pub action: String,
    pub expected_result: String,
}

/// Sample dataset
#[derive(Debug, Clone, Serialize)]
pub struct SampleDataset {
    pub id: String,
    pub name: String,
    pub description: String,
    pub topic: String,
    pub message_count: usize,
    pub format: String,
}

/// Error response
#[derive(Debug, Serialize)]
pub struct PlaygroundError {
    pub error: String,
}

/// Create the Playground API router
pub fn create_playground_api_router(state: PlaygroundApiState) -> Router {
    Router::new()
        .route("/", get(serve_dashboard))
        .route("/playground", get(serve_dashboard))
        .route("/api/v1/playground/status", get(playground_status))
        .route("/api/v1/playground/sessions", post(create_session))
        .route("/api/v1/playground/sessions/:id", get(get_session))
        .route(
            "/api/v1/playground/sessions/:id/produce",
            post(produce_message),
        )
        .route(
            "/api/v1/playground/sessions/:id/consume",
            post(consume_messages),
        )
        .route("/api/v1/playground/tutorials", get(list_tutorials))
        .route("/api/v1/playground/tutorials/:id", get(get_tutorial))
        .route("/api/v1/playground/samples", get(list_samples))
        .route("/api/v1/playground/samples/:id/load", post(load_sample))
        .with_state(state)
}

/// Serve the playground web dashboard HTML page.
async fn serve_dashboard() -> axum::response::Html<&'static str> {
    axum::response::Html(crate::playground::dashboard::DASHBOARD_HTML)
}

async fn playground_status(State(state): State<PlaygroundApiState>) -> Json<serde_json::Value> {
    let sessions = state.sessions.read();
    let topics = state.topic_manager.list_topics().unwrap_or_default();
    Json(serde_json::json!({
        "status": "active",
        "version": env!("CARGO_PKG_VERSION"),
        "capabilities": {
            "produce": true,
            "consume": true,
            "topics": true,
            "tutorials": true,
            "sample_datasets": true,
        },
        "active_sessions": sessions.len(),
        "available_topics": topics.len(),
    }))
}

async fn create_session(
    State(state): State<PlaygroundApiState>,
    Json(req): Json<CreateSessionRequest>,
) -> Result<(StatusCode, Json<PlaygroundSession>), (StatusCode, Json<PlaygroundError>)> {
    let session_id = Uuid::new_v4().to_string();
    let mut topic_names = Vec::new();

    for topic_setup in &req.topics {
        let partitions = topic_setup.partitions.unwrap_or(1);
        let name = format!("pg-{}-{}", &session_id[..8], &topic_setup.name);

        if let Err(e) = state.topic_manager.create_topic(&name, partitions as i32) {
            tracing::warn!("Playground topic creation failed: {}", e);
        }
        topic_names.push(name);
    }

    let session = PlaygroundSession {
        id: session_id.clone(),
        created_at: chrono::Utc::now().timestamp_millis(),
        topics: topic_names,
        messages_produced: 0,
        messages_consumed: 0,
    };

    state.sessions.write().insert(session_id, session.clone());

    Ok((StatusCode::CREATED, Json(session)))
}

async fn get_session(
    State(state): State<PlaygroundApiState>,
    Path(id): Path<String>,
) -> Result<Json<PlaygroundSession>, (StatusCode, Json<PlaygroundError>)> {
    state
        .sessions
        .read()
        .get(&id)
        .cloned()
        .map(Json)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(PlaygroundError {
                    error: format!("Session '{}' not found", id),
                }),
            )
        })
}

async fn produce_message(
    State(state): State<PlaygroundApiState>,
    Path(id): Path<String>,
    Json(req): Json<PlaygroundProduceRequest>,
) -> Result<Json<PlaygroundProduceResponse>, (StatusCode, Json<PlaygroundError>)> {
    // Verify session exists
    {
        let sessions = state.sessions.read();
        if !sessions.contains_key(&id) {
            return Err((
                StatusCode::NOT_FOUND,
                Json(PlaygroundError {
                    error: format!("Session '{}' not found", id),
                }),
            ));
        }
    }

    let key = req.key.map(Bytes::from);
    let value = Bytes::from(req.value);

    match state.topic_manager.append(&req.topic, 0, key, value) {
        Ok(offset) => {
            // Update session stats
            if let Some(session) = state.sessions.write().get_mut(&id) {
                session.messages_produced += 1;
            }

            Ok(Json(PlaygroundProduceResponse {
                offset,
                partition: 0,
                topic: req.topic,
            }))
        }
        Err(e) => Err((
            StatusCode::BAD_REQUEST,
            Json(PlaygroundError {
                error: e.to_string(),
            }),
        )),
    }
}

async fn consume_messages(
    State(state): State<PlaygroundApiState>,
    Path(id): Path<String>,
    Json(req): Json<PlaygroundConsumeRequest>,
) -> Result<Json<Vec<PlaygroundMessage>>, (StatusCode, Json<PlaygroundError>)> {
    {
        let sessions = state.sessions.read();
        if !sessions.contains_key(&id) {
            return Err((
                StatusCode::NOT_FOUND,
                Json(PlaygroundError {
                    error: format!("Session '{}' not found", id),
                }),
            ));
        }
    }

    let start_offset = req.offset.unwrap_or(0);

    match state
        .topic_manager
        .read(&req.topic, 0, start_offset, req.max_messages)
    {
        Ok(records) => {
            let messages: Vec<PlaygroundMessage> = records
                .iter()
                .enumerate()
                .map(|(i, r)| PlaygroundMessage {
                    offset: start_offset + i as i64,
                    partition: 0,
                    key: r
                        .key
                        .as_ref()
                        .map(|k| String::from_utf8_lossy(k).to_string()),
                    value: String::from_utf8_lossy(&r.value).to_string(),
                    timestamp: r.timestamp,
                })
                .collect();

            let count = messages.len();
            if let Some(session) = state.sessions.write().get_mut(&id) {
                session.messages_consumed += count as u64;
            }

            Ok(Json(messages))
        }
        Err(e) => Err((
            StatusCode::BAD_REQUEST,
            Json(PlaygroundError {
                error: e.to_string(),
            }),
        )),
    }
}

fn built_in_tutorials() -> Vec<Tutorial> {
    vec![
        Tutorial {
            id: "getting-started".to_string(),
            title: "Getting Started with Streamline".to_string(),
            description: "Learn the basics of producing and consuming messages".to_string(),
            difficulty: "beginner".to_string(),
            estimated_minutes: 5,
            steps: vec![
                TutorialStep {
                    title: "Create a topic".to_string(),
                    description: "Start a playground session to create your first topic"
                        .to_string(),
                    action: "POST /api/v1/playground/sessions".to_string(),
                    expected_result: "A session with a default topic is created".to_string(),
                },
                TutorialStep {
                    title: "Produce a message".to_string(),
                    description: "Send your first message to the topic".to_string(),
                    action: "POST /api/v1/playground/sessions/:id/produce".to_string(),
                    expected_result: "Message is stored and offset is returned".to_string(),
                },
                TutorialStep {
                    title: "Consume messages".to_string(),
                    description: "Read back the messages you produced".to_string(),
                    action: "POST /api/v1/playground/sessions/:id/consume".to_string(),
                    expected_result: "Your messages are returned in order".to_string(),
                },
            ],
        },
        Tutorial {
            id: "event-sourcing".to_string(),
            title: "Event Sourcing with Streamline".to_string(),
            description: "Build an event-sourced system using Streamline as the event store"
                .to_string(),
            difficulty: "intermediate".to_string(),
            estimated_minutes: 15,
            steps: vec![
                TutorialStep {
                    title: "Design your events".to_string(),
                    description: "Create JSON events for a user management system".to_string(),
                    action: "Produce UserCreated, UserUpdated, UserDeleted events".to_string(),
                    expected_result: "Events are stored in order with timestamps".to_string(),
                },
                TutorialStep {
                    title: "Replay events".to_string(),
                    description: "Consume all events from the beginning to rebuild state"
                        .to_string(),
                    action: "Consume from offset 0".to_string(),
                    expected_result: "Full event history is available for replay".to_string(),
                },
            ],
        },
        Tutorial {
            id: "consumer-groups".to_string(),
            title: "Consumer Groups & Load Balancing".to_string(),
            description: "Learn how consumer groups distribute work across consumers".to_string(),
            difficulty: "intermediate".to_string(),
            estimated_minutes: 10,
            steps: vec![TutorialStep {
                title: "Create a multi-partition topic".to_string(),
                description: "Create a topic with 3 partitions for parallel consumption"
                    .to_string(),
                action: "Create session with 3-partition topic".to_string(),
                expected_result: "Topic with 3 partitions is created".to_string(),
            }],
        },
    ]
}

fn built_in_samples() -> Vec<SampleDataset> {
    vec![
        SampleDataset {
            id: "ecommerce-events".to_string(),
            name: "E-Commerce Events".to_string(),
            description: "Shopping cart events: views, adds, purchases, refunds".to_string(),
            topic: "ecommerce-events".to_string(),
            message_count: 100,
            format: "json".to_string(),
        },
        SampleDataset {
            id: "iot-sensors".to_string(),
            name: "IoT Sensor Data".to_string(),
            description: "Temperature, humidity, and pressure readings from IoT devices"
                .to_string(),
            topic: "iot-sensors".to_string(),
            message_count: 200,
            format: "json".to_string(),
        },
        SampleDataset {
            id: "web-logs".to_string(),
            name: "Web Server Logs".to_string(),
            description: "Apache-style access logs with request paths, status codes, latencies"
                .to_string(),
            topic: "web-logs".to_string(),
            message_count: 500,
            format: "text".to_string(),
        },
    ]
}

async fn list_tutorials() -> Json<serde_json::Value> {
    let tutorials = built_in_tutorials();
    Json(serde_json::json!({
        "tutorials": tutorials,
        "total": tutorials.len(),
    }))
}

async fn get_tutorial(
    Path(id): Path<String>,
) -> Result<Json<Tutorial>, (StatusCode, Json<PlaygroundError>)> {
    built_in_tutorials()
        .into_iter()
        .find(|t| t.id == id)
        .map(Json)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(PlaygroundError {
                    error: format!("Tutorial '{}' not found", id),
                }),
            )
        })
}

async fn list_samples() -> Json<serde_json::Value> {
    let samples = built_in_samples();
    Json(serde_json::json!({
        "samples": samples,
        "total": samples.len(),
    }))
}

async fn load_sample(
    State(state): State<PlaygroundApiState>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<PlaygroundError>)> {
    let sample = built_in_samples()
        .into_iter()
        .find(|s| s.id == id)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(PlaygroundError {
                    error: format!("Sample dataset '{}' not found", id),
                }),
            )
        })?;

    // Create the topic if it doesn't exist
    let _ = state.topic_manager.create_topic(&sample.topic, 1);

    // Generate and load sample data
    let mut loaded = 0;
    for i in 0..sample.message_count.min(100) {
        let value = match sample.id.as_str() {
            "ecommerce-events" => {
                let events = ["view", "add_to_cart", "purchase", "refund"];
                let event = events[i % events.len()];
                format!(
                    r#"{{"event":"{}","product_id":"prod-{}","user_id":"user-{}","timestamp":{}}}"#,
                    event,
                    i % 20,
                    i % 50,
                    chrono::Utc::now().timestamp_millis()
                )
            }
            "iot-sensors" => {
                format!(
                    r#"{{"sensor_id":"sensor-{}","temperature":{:.1},"humidity":{:.1},"pressure":{:.1},"timestamp":{}}}"#,
                    i % 10,
                    20.0 + (i as f64 * 0.3) % 15.0,
                    40.0 + (i as f64 * 0.5) % 40.0,
                    1013.0 + (i as f64 * 0.1) % 5.0,
                    chrono::Utc::now().timestamp_millis()
                )
            }
            _ => {
                let methods = ["GET", "POST", "PUT", "DELETE"];
                let codes = [200, 201, 301, 404, 500];
                format!(
                    r#"{} /api/v1/resource/{} {} {}ms"#,
                    methods[i % methods.len()],
                    i % 100,
                    codes[i % codes.len()],
                    (i * 7) % 500
                )
            }
        };

        if state
            .topic_manager
            .append(&sample.topic, 0, None, Bytes::from(value))
            .is_ok()
        {
            loaded += 1;
        }
    }

    Ok(Json(serde_json::json!({
        "sample": sample.name,
        "topic": sample.topic,
        "messages_loaded": loaded,
        "format": sample.format,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_built_in_tutorials() {
        let tutorials = built_in_tutorials();
        assert!(!tutorials.is_empty());
        assert!(tutorials.iter().all(|t| !t.steps.is_empty()));
    }

    #[test]
    fn test_built_in_samples() {
        let samples = built_in_samples();
        assert_eq!(samples.len(), 3);
        assert!(samples.iter().all(|s| s.message_count > 0));
    }

    #[test]
    fn test_default_session_topics() {
        let topics = default_session_topics();
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0].name, "playground-events");
    }
}
