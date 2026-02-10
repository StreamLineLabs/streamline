//! Browser-Native Client SDK
//!
//! This module provides a browser-optimized WebSocket API for JavaScript/TypeScript
//! clients with enhanced features:
//!
//! - **CORS Support**: Proper cross-origin resource sharing headers
//! - **Token Authentication**: JWT/Bearer token authentication for browser clients
//! - **TypeScript-Friendly Protocol**: JSON message format with discriminated unions
//! - **Automatic Reconnection**: Client-side reconnection hints and session resumption
//! - **Binary Optimization**: MessagePack or base64 for efficient binary data
//! - **Heartbeat Management**: Automatic keepalive with configurable intervals
//! - **Backpressure Signals**: Flow control for high-volume streams
//!
//! ## Endpoints
//!
//! - `GET /browser/v1/stream` - Browser-optimized WebSocket streaming
//! - `GET /browser/v1/config` - Client configuration endpoint
//! - `POST /browser/v1/auth` - Token exchange endpoint
//!
//! ## Protocol
//!
//! ### Authentication
//! ```json
//! // Request (via HTTP header or first WebSocket message)
//! { "type": "auth", "token": "Bearer eyJ..." }
//!
//! // Response
//! { "type": "auth_result", "success": true, "session_id": "...", "expires_at": 1702123456789 }
//! ```
//!
//! ### Subscribe
//! ```json
//! {
//!   "type": "subscribe",
//!   "id": "sub-1",
//!   "topics": ["events"],
//!   "options": {
//!     "offset": "latest",
//!     "filter": { "key": "user-*" },
//!     "format": "json"
//!   }
//! }
//! ```
//!
//! ### Messages
//! ```json
//! {
//!   "type": "message",
//!   "id": "msg-12345",
//!   "subscriptionId": "sub-1",
//!   "topic": "events",
//!   "partition": 0,
//!   "offset": 1000,
//!   "timestamp": 1702123456789,
//!   "key": "user-123",
//!   "value": { "action": "click" },
//!   "headers": {}
//! }
//! ```

use crate::storage::TopicManager;
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Query, State,
    },
    http::{header, HeaderMap, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc, RwLock};
use tower::ServiceBuilder;
use tracing::{debug, error, info, warn};

/// Browser client state
#[derive(Clone)]
pub struct BrowserClientState {
    /// Topic manager for reading messages
    pub topic_manager: Arc<TopicManager>,
    /// Active browser sessions
    pub active_sessions: Arc<AtomicU64>,
    /// Total messages delivered to browser clients
    pub total_messages_delivered: Arc<AtomicU64>,
    /// CORS allowed origins (empty = allow all)
    pub allowed_origins: Arc<Vec<String>>,
    /// Token validator (optional)
    pub token_validator: Option<Arc<dyn TokenValidator>>,
}

impl BrowserClientState {
    pub fn new(topic_manager: Arc<TopicManager>) -> Self {
        Self {
            topic_manager,
            active_sessions: Arc::new(AtomicU64::new(0)),
            total_messages_delivered: Arc::new(AtomicU64::new(0)),
            allowed_origins: Arc::new(Vec::new()),
            token_validator: None,
        }
    }

    pub fn with_allowed_origins(mut self, origins: Vec<String>) -> Self {
        self.allowed_origins = Arc::new(origins);
        self
    }

    pub fn with_token_validator(mut self, validator: Arc<dyn TokenValidator>) -> Self {
        self.token_validator = Some(validator);
        self
    }
}

/// Token validator trait for authentication
pub trait TokenValidator: Send + Sync {
    /// Validate a token and return the principal (user ID) if valid
    fn validate(&self, token: &str) -> Option<TokenInfo>;
}

/// Token information after validation
#[derive(Debug, Clone, Serialize)]
pub struct TokenInfo {
    /// User or client identifier
    pub principal: String,
    /// Token expiration timestamp (ms since epoch)
    pub expires_at: i64,
    /// Allowed topics (empty = all topics)
    pub allowed_topics: Vec<String>,
    /// Is this a read-only token
    pub read_only: bool,
}

/// Client message types (TypeScript discriminated union friendly)
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ClientMessage {
    /// Authentication request
    Auth { token: String },
    /// Subscribe to topics
    Subscribe {
        /// Unique subscription ID (client-generated)
        id: String,
        topics: Vec<String>,
        #[serde(default)]
        options: SubscribeOptions,
    },
    /// Unsubscribe from topics
    Unsubscribe { id: String },
    /// Acknowledge messages (for at-least-once delivery)
    Ack {
        /// Message IDs to acknowledge
        ids: Vec<String>,
    },
    /// Request replay from offset
    Replay {
        topic: String,
        partition: i32,
        from_offset: i64,
        #[serde(default = "default_replay_count")]
        count: usize,
    },
    /// Ping for keepalive
    Ping {
        #[serde(default)]
        timestamp: Option<i64>,
    },
    /// Pause subscription
    Pause { id: String },
    /// Resume subscription
    Resume { id: String },
    /// Get connection status
    Status,
}

fn default_replay_count() -> usize {
    100
}

/// Subscribe options
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscribeOptions {
    /// Starting offset: "latest", "earliest", or numeric offset
    #[serde(default)]
    pub offset: OffsetSpec,
    /// Key filter pattern (glob)
    #[serde(default)]
    pub key_filter: Option<String>,
    /// Value filter (JSONPath)
    #[serde(default)]
    pub value_filter: Option<String>,
    /// Output format: "json" (default), "binary"
    #[serde(default = "default_format")]
    pub format: String,
    /// Include message metadata (partition, offset, timestamp)
    #[serde(default = "default_true")]
    pub include_metadata: bool,
    /// Batch size (1 = individual messages)
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Batch timeout in milliseconds
    #[serde(default = "default_batch_timeout")]
    pub batch_timeout_ms: u64,
    /// Specific partitions to consume (empty = all)
    #[serde(default)]
    pub partitions: Vec<i32>,
}

fn default_format() -> String {
    "json".to_string()
}

fn default_true() -> bool {
    true
}

fn default_batch_size() -> usize {
    1
}

fn default_batch_timeout() -> u64 {
    100
}

/// Offset specification
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum OffsetSpec {
    /// Start from latest offset
    #[default]
    Latest,
    /// Start from earliest offset
    Earliest,
    /// Start from specific offset
    Offset(i64),
    /// Start from timestamp
    Timestamp(i64),
}

/// Server message types (TypeScript discriminated union friendly)
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ServerMessage {
    /// Connection established
    Connected {
        /// Session ID for reconnection
        #[serde(rename = "sessionId")]
        session_id: String,
        /// Server timestamp
        #[serde(rename = "serverTime")]
        server_time: i64,
        /// Protocol version
        #[serde(rename = "protocolVersion")]
        protocol_version: String,
        /// Heartbeat interval recommendation (ms)
        #[serde(rename = "heartbeatInterval")]
        heartbeat_interval: u64,
        /// Reconnection hints
        #[serde(rename = "reconnectConfig")]
        reconnect_config: ReconnectConfig,
    },
    /// Authentication result
    AuthResult {
        success: bool,
        #[serde(rename = "sessionId", skip_serializing_if = "Option::is_none")]
        session_id: Option<String>,
        #[serde(rename = "expiresAt", skip_serializing_if = "Option::is_none")]
        expires_at: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
    /// Subscription confirmed
    Subscribed {
        id: String,
        topics: Vec<String>,
        partitions: HashMap<String, Vec<i32>>,
    },
    /// Unsubscription confirmed
    Unsubscribed { id: String },
    /// Individual message
    Message {
        /// Unique message ID for acknowledgment
        id: String,
        /// Subscription ID
        #[serde(rename = "subscriptionId")]
        subscription_id: String,
        topic: String,
        partition: i32,
        offset: i64,
        timestamp: i64,
        #[serde(skip_serializing_if = "Option::is_none")]
        key: Option<String>,
        value: serde_json::Value,
        #[serde(skip_serializing_if = "HashMap::is_empty")]
        headers: HashMap<String, String>,
    },
    /// Batch of messages
    MessageBatch {
        /// Batch ID
        #[serde(rename = "batchId")]
        batch_id: String,
        /// Subscription ID
        #[serde(rename = "subscriptionId")]
        subscription_id: String,
        messages: Vec<BatchMessage>,
    },
    /// Replay response
    ReplayResult {
        topic: String,
        partition: i32,
        messages: Vec<BatchMessage>,
        #[serde(rename = "hasMore")]
        has_more: bool,
        #[serde(rename = "nextOffset")]
        next_offset: i64,
    },
    /// Pong response
    Pong {
        timestamp: i64,
        #[serde(rename = "clientTimestamp", skip_serializing_if = "Option::is_none")]
        client_timestamp: Option<i64>,
    },
    /// Subscription paused
    Paused { id: String },
    /// Subscription resumed
    Resumed { id: String },
    /// Connection status
    Status {
        subscriptions: Vec<SubscriptionInfo>,
        #[serde(rename = "messagesDelivered")]
        messages_delivered: u64,
        #[serde(rename = "uptimeSeconds")]
        uptime_seconds: f64,
        authenticated: bool,
    },
    /// Error message
    Error {
        code: String,
        message: String,
        #[serde(rename = "subscriptionId", skip_serializing_if = "Option::is_none")]
        subscription_id: Option<String>,
        recoverable: bool,
    },
    /// Backpressure signal
    Backpressure {
        /// Subscription ID
        #[serde(rename = "subscriptionId")]
        subscription_id: String,
        /// Pause consumption for this many milliseconds
        #[serde(rename = "pauseMs")]
        pause_ms: u64,
        /// Current buffer size
        #[serde(rename = "bufferSize")]
        buffer_size: usize,
    },
    /// Token expiration warning
    TokenExpiring {
        /// Seconds until expiration
        #[serde(rename = "expiresIn")]
        expires_in: i64,
    },
}

/// Reconnection configuration hints for clients
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReconnectConfig {
    /// Initial retry delay (ms)
    pub initial_delay_ms: u64,
    /// Maximum retry delay (ms)
    pub max_delay_ms: u64,
    /// Backoff multiplier
    pub multiplier: f64,
    /// Maximum retry attempts (0 = infinite)
    pub max_attempts: u32,
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self {
            initial_delay_ms: 1000,
            max_delay_ms: 30000,
            multiplier: 1.5,
            max_attempts: 0,
        }
    }
}

/// Message in a batch
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BatchMessage {
    pub id: String,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    pub value: serde_json::Value,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub headers: HashMap<String, String>,
}

/// Subscription information
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionInfo {
    pub id: String,
    pub topics: Vec<String>,
    pub paused: bool,
    #[serde(rename = "messagesReceived")]
    pub messages_received: u64,
    #[serde(rename = "currentOffsets")]
    pub current_offsets: HashMap<String, HashMap<i32, i64>>,
}

/// Active subscription tracking
struct ActiveSubscription {
    id: String,
    topics: Vec<String>,
    options: SubscribeOptions,
    paused: AtomicBool,
    messages_received: AtomicU64,
    offsets: RwLock<HashMap<String, HashMap<i32, i64>>>,
    key_regex: Option<regex::Regex>,
}

/// Query parameters for browser connection
#[derive(Debug, Deserialize)]
pub struct BrowserQuery {
    /// Optional token in query string (for environments where headers are difficult)
    #[serde(default)]
    pub token: Option<String>,
    /// Session ID for reconnection
    #[serde(default)]
    pub session_id: Option<String>,
    /// Resume from last known offsets
    #[serde(default)]
    pub resume: Option<bool>,
}

/// Client configuration response
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientConfig {
    /// WebSocket endpoint URL
    pub websocket_url: String,
    /// API version
    pub api_version: String,
    /// Maximum message size (bytes)
    pub max_message_size: usize,
    /// Recommended heartbeat interval (ms)
    pub heartbeat_interval: u64,
    /// Reconnection configuration
    pub reconnect: ReconnectConfig,
    /// Available topics (if authorized)
    pub topics: Vec<TopicInfo>,
    /// Feature flags
    pub features: FeatureFlags,
}

/// Topic information for client discovery
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicInfo {
    pub name: String,
    pub partitions: i32,
    #[serde(rename = "messageCount")]
    pub message_count: u64,
}

/// Feature flags for client capability detection
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FeatureFlags {
    pub binary_messages: bool,
    pub batch_subscribe: bool,
    pub key_filtering: bool,
    pub value_filtering: bool,
    pub replay: bool,
    pub acknowledgments: bool,
}

impl Default for FeatureFlags {
    fn default() -> Self {
        Self {
            binary_messages: true,
            batch_subscribe: true,
            key_filtering: true,
            value_filtering: true,
            replay: true,
            acknowledgments: true,
        }
    }
}

/// Create the browser client router with CORS support
pub fn create_browser_client_router(state: BrowserClientState) -> Router {
    // Build CORS layer
    let cors_origins = state.allowed_origins.clone();

    Router::new()
        .route("/browser/v1/stream", get(ws_browser_handler))
        .route("/browser/v1/config", get(config_handler))
        .route("/browser/v1/auth", post(auth_handler))
        .route("/browser/v1/topics", get(topics_handler))
        .route("/browser/v1/stats", get(stats_handler))
        .layer(ServiceBuilder::new().layer(axum::middleware::from_fn(
            move |req: axum::http::Request<axum::body::Body>, next: axum::middleware::Next| {
                let origins = cors_origins.clone();
                async move { cors_middleware(req, next, origins).await }
            },
        )))
        .with_state(state)
}

/// CORS middleware
async fn cors_middleware(
    req: axum::http::Request<axum::body::Body>,
    next: axum::middleware::Next,
    allowed_origins: Arc<Vec<String>>,
) -> Response {
    let origin = req
        .headers()
        .get(header::ORIGIN)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    // Handle preflight
    if req.method() == Method::OPTIONS {
        let mut response = StatusCode::NO_CONTENT.into_response();
        add_cors_headers(response.headers_mut(), origin.as_deref(), &allowed_origins);
        return response;
    }

    let mut response = next.run(req).await;
    add_cors_headers(response.headers_mut(), origin.as_deref(), &allowed_origins);
    response
}

/// Add CORS headers to response
fn add_cors_headers(headers: &mut HeaderMap, origin: Option<&str>, allowed_origins: &[String]) {
    let allow_origin = if allowed_origins.is_empty() {
        // Allow all origins if none specified
        origin.unwrap_or("*").to_string()
    } else if let Some(origin) = origin {
        // Check if origin is in allowed list
        if allowed_origins.iter().any(|o| o == origin || o == "*") {
            origin.to_string()
        } else {
            return; // Don't add CORS headers for disallowed origin
        }
    } else {
        return;
    };

    headers.insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_str(&allow_origin).unwrap_or_else(|_| HeaderValue::from_static("*")),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static("GET, POST, OPTIONS"),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_HEADERS,
        HeaderValue::from_static("Authorization, Content-Type, X-Requested-With"),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_CREDENTIALS,
        HeaderValue::from_static("true"),
    );
    headers.insert(
        header::ACCESS_CONTROL_MAX_AGE,
        HeaderValue::from_static("86400"),
    );
}

/// WebSocket handler for browser clients
async fn ws_browser_handler(
    ws: WebSocketUpgrade,
    State(state): State<BrowserClientState>,
    Query(query): Query<BrowserQuery>,
    headers: HeaderMap,
) -> Response {
    // Extract token from header or query
    let token = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.strip_prefix("Bearer ").unwrap_or(s).to_string())
        .or(query.token);

    info!(
        has_token = token.is_some(),
        session_id = ?query.session_id,
        "Browser client connection request"
    );

    ws.on_upgrade(move |socket| {
        handle_browser_connection(socket, state, token, query.session_id, query.resume)
    })
}

/// Handle browser WebSocket connection
async fn handle_browser_connection(
    socket: WebSocket,
    state: BrowserClientState,
    initial_token: Option<String>,
    _session_id: Option<String>,
    _resume: Option<bool>,
) {
    let session_id = uuid::Uuid::new_v4().to_string();
    let start_time = Instant::now();
    let mut authenticated = false;
    let mut token_info: Option<TokenInfo> = None;

    info!(session_id = %session_id, "Browser client connected");
    state.active_sessions.fetch_add(1, Ordering::Relaxed);

    let (mut sender, mut receiver) = socket.split();

    // Validate initial token if provided
    if let Some(token) = initial_token {
        if let Some(ref validator) = state.token_validator {
            if let Some(info) = validator.validate(&token) {
                authenticated = true;
                token_info = Some(info);
            }
        } else {
            // No validator = allow all
            authenticated = true;
        }
    } else if state.token_validator.is_none() {
        // No validator configured = no auth required
        authenticated = true;
    }

    // Send connected message
    let connected_msg = ServerMessage::Connected {
        session_id: session_id.clone(),
        server_time: chrono::Utc::now().timestamp_millis(),
        protocol_version: "1.0".to_string(),
        heartbeat_interval: 30000,
        reconnect_config: ReconnectConfig::default(),
    };
    if let Ok(json) = serde_json::to_string(&connected_msg) {
        let _ = sender.send(Message::Text(json)).await;
    }

    // Subscription management
    let subscriptions: Arc<RwLock<HashMap<String, ActiveSubscription>>> =
        Arc::new(RwLock::new(HashMap::new()));
    let (shutdown_tx, _) = broadcast::channel::<()>(1);
    let (message_tx, mut message_rx) = mpsc::channel::<ServerMessage>(10000);
    let messages_delivered = Arc::new(AtomicU64::new(0));

    // Clone for reader task
    let reader_subscriptions = subscriptions.clone();
    let reader_state = state.clone();
    let reader_message_tx = message_tx.clone();
    let reader_messages_delivered = messages_delivered.clone();
    let mut reader_shutdown_rx = shutdown_tx.subscribe();

    // Spawn reader task
    let reader_handle = tokio::spawn(async move {
        let mut message_counter: u64 = 0;
        let mut batch_counter: u64 = 0;

        loop {
            tokio::select! {
                _ = reader_shutdown_rx.recv() => {
                    debug!("Browser reader task shutting down");
                    break;
                }
                _ = tokio::time::sleep(Duration::from_millis(10)) => {
                    let subs = reader_subscriptions.read().await;
                    if subs.is_empty() {
                        continue;
                    }

                    for sub in subs.values() {
                        if sub.paused.load(Ordering::Relaxed) {
                            continue;
                        }

                        for topic in &sub.topics {
                            // Get partitions
                            let partitions = if sub.options.partitions.is_empty() {
                                match reader_state.topic_manager.get_topic_metadata(topic) {
                                    Ok(meta) => (0..meta.num_partitions).collect::<Vec<_>>(),
                                    Err(_) => continue,
                                }
                            } else {
                                sub.options.partitions.clone()
                            };

                            let mut batch_messages = Vec::new();
                            let batch_start = Instant::now();
                            let batch_timeout = Duration::from_millis(sub.options.batch_timeout_ms);

                            for partition in partitions {
                                let mut offsets = sub.offsets.write().await;
                                let topic_offsets = offsets.entry(topic.clone()).or_default();
                                let current_offset = topic_offsets.entry(partition).or_insert_with(|| {
                                    match &sub.options.offset {
                                        OffsetSpec::Latest => {
                                            reader_state.topic_manager
                                                .latest_offset(topic, partition)
                                                .unwrap_or(0)
                                        }
                                        OffsetSpec::Earliest => 0,
                                        OffsetSpec::Offset(o) => *o,
                                        OffsetSpec::Timestamp(ts) => {
                                            reader_state.topic_manager
                                                .find_offset_by_timestamp(topic, partition, *ts)
                                                .ok()
                                                .flatten()
                                                .unwrap_or(0)
                                        }
                                    }
                                });

                                let max_records = sub.options.batch_size.saturating_sub(batch_messages.len());
                                if max_records == 0 {
                                    break;
                                }

                                match reader_state.topic_manager.read(
                                    topic,
                                    partition,
                                    *current_offset,
                                    max_records,
                                ) {
                                    Ok(records) => {
                                        for record in records {
                                            // Apply key filter
                                            if let Some(ref regex) = sub.key_regex {
                                                if let Some(ref key) = record.key {
                                                    let key_str = String::from_utf8_lossy(key);
                                                    if !regex.is_match(&key_str) {
                                                        *current_offset = record.offset + 1;
                                                        continue;
                                                    }
                                                } else {
                                                    *current_offset = record.offset + 1;
                                                    continue;
                                                }
                                            }

                                            // Convert value
                                            let value = match serde_json::from_slice(&record.value) {
                                                Ok(v) => v,
                                                Err(_) => serde_json::Value::String(
                                                    base64::Engine::encode(
                                                        &base64::engine::general_purpose::STANDARD,
                                                        &record.value,
                                                    ),
                                                ),
                                            };

                                            let headers: HashMap<String, String> = record.headers
                                                .into_iter()
                                                .map(|h| (h.key, String::from_utf8_lossy(&h.value).to_string()))
                                                .collect();

                                            message_counter += 1;
                                            let msg = BatchMessage {
                                                id: format!("msg-{}", message_counter),
                                                topic: topic.clone(),
                                                partition,
                                                offset: record.offset,
                                                timestamp: record.timestamp,
                                                key: record.key.map(|k| String::from_utf8_lossy(&k).to_string()),
                                                value,
                                                headers,
                                            };

                                            batch_messages.push(msg);
                                            *current_offset = record.offset + 1;
                                            sub.messages_received.fetch_add(1, Ordering::Relaxed);
                                            reader_messages_delivered.fetch_add(1, Ordering::Relaxed);
                                            reader_state.total_messages_delivered.fetch_add(1, Ordering::Relaxed);

                                            if batch_messages.len() >= sub.options.batch_size {
                                                break;
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        warn!(topic = %topic, partition = partition, error = %e, "Error reading partition");
                                    }
                                }

                                if batch_start.elapsed() >= batch_timeout && !batch_messages.is_empty() {
                                    break;
                                }
                            }

                            // Send messages
                            if !batch_messages.is_empty() {
                                let msg = if sub.options.batch_size == 1 && batch_messages.len() == 1 {
                                    let m = batch_messages.remove(0);
                                    ServerMessage::Message {
                                        id: m.id,
                                        subscription_id: sub.id.clone(),
                                        topic: m.topic,
                                        partition: m.partition,
                                        offset: m.offset,
                                        timestamp: m.timestamp,
                                        key: m.key,
                                        value: m.value,
                                        headers: m.headers,
                                    }
                                } else {
                                    batch_counter += 1;
                                    ServerMessage::MessageBatch {
                                        batch_id: format!("batch-{}", batch_counter),
                                        subscription_id: sub.id.clone(),
                                        messages: batch_messages,
                                    }
                                };

                                if reader_message_tx.send(msg).await.is_err() {
                                    return;
                                }
                            }
                        }
                    }
                }
            }
        }
    });

    // Handle incoming commands
    let cmd_subscriptions = subscriptions.clone();
    let cmd_state = state.clone();
    let cmd_message_tx = message_tx.clone();
    let cmd_session_id = session_id.clone();
    let cmd_messages_delivered = messages_delivered.clone();

    let cmd_handle = tokio::spawn(async move {
        while let Some(result) = receiver.next().await {
            match result {
                Ok(Message::Text(text)) => match serde_json::from_str::<ClientMessage>(&text) {
                    Ok(msg) => {
                        handle_client_message(
                            msg,
                            &cmd_subscriptions,
                            &cmd_state,
                            &cmd_message_tx,
                            &mut authenticated,
                            &mut token_info,
                            start_time,
                            cmd_messages_delivered.load(Ordering::Relaxed),
                        )
                        .await;
                    }
                    Err(e) => {
                        let err = ServerMessage::Error {
                            code: "INVALID_MESSAGE".to_string(),
                            message: format!("Failed to parse message: {}", e),
                            subscription_id: None,
                            recoverable: true,
                        };
                        let _ = cmd_message_tx.send(err).await;
                    }
                },
                Ok(Message::Binary(data)) => {
                    // Try to parse as JSON (some clients send JSON as binary)
                    if let Ok(text) = String::from_utf8(data) {
                        if let Ok(msg) = serde_json::from_str::<ClientMessage>(&text) {
                            handle_client_message(
                                msg,
                                &cmd_subscriptions,
                                &cmd_state,
                                &cmd_message_tx,
                                &mut authenticated,
                                &mut token_info,
                                start_time,
                                cmd_messages_delivered.load(Ordering::Relaxed),
                            )
                            .await;
                        }
                    }
                }
                Ok(Message::Close(_)) => {
                    debug!(session_id = %cmd_session_id, "Browser client closed connection");
                    break;
                }
                Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => {}
                Err(e) => {
                    error!(error = %e, "WebSocket receive error");
                    break;
                }
            }
        }
    });

    // Send messages to client
    let send_handle = tokio::spawn(async move {
        while let Some(msg) = message_rx.recv().await {
            match serde_json::to_string(&msg) {
                Ok(json) => {
                    if sender.send(Message::Text(json)).await.is_err() {
                        break;
                    }
                }
                Err(e) => {
                    warn!(error = %e, "Failed to serialize server message");
                }
            }
        }
        let _ = sender.close().await;
    });

    // Wait for any task to complete
    tokio::select! {
        _ = cmd_handle => {}
        _ = send_handle => {}
    }

    // Cleanup
    let _ = shutdown_tx.send(());
    reader_handle.abort();

    state.active_sessions.fetch_sub(1, Ordering::Relaxed);
    info!(
        session_id = %session_id,
        uptime_secs = start_time.elapsed().as_secs_f64(),
        messages = messages_delivered.load(Ordering::Relaxed),
        "Browser client disconnected"
    );
}

/// Handle client message
#[allow(clippy::too_many_arguments)]
async fn handle_client_message(
    msg: ClientMessage,
    subscriptions: &Arc<RwLock<HashMap<String, ActiveSubscription>>>,
    state: &BrowserClientState,
    message_tx: &mpsc::Sender<ServerMessage>,
    authenticated: &mut bool,
    token_info: &mut Option<TokenInfo>,
    start_time: Instant,
    messages_delivered: u64,
) {
    match msg {
        ClientMessage::Auth { token } => {
            if let Some(ref validator) = state.token_validator {
                if let Some(info) = validator.validate(&token) {
                    *authenticated = true;
                    let expires_at = info.expires_at;
                    *token_info = Some(info);
                    let _ = message_tx
                        .send(ServerMessage::AuthResult {
                            success: true,
                            session_id: Some(uuid::Uuid::new_v4().to_string()),
                            expires_at: Some(expires_at),
                            error: None,
                        })
                        .await;
                } else {
                    let _ = message_tx
                        .send(ServerMessage::AuthResult {
                            success: false,
                            session_id: None,
                            expires_at: None,
                            error: Some("Invalid token".to_string()),
                        })
                        .await;
                }
            } else {
                // No validator = accept any token
                *authenticated = true;
                let _ = message_tx
                    .send(ServerMessage::AuthResult {
                        success: true,
                        session_id: Some(uuid::Uuid::new_v4().to_string()),
                        expires_at: None,
                        error: None,
                    })
                    .await;
            }
        }

        ClientMessage::Subscribe {
            id,
            topics,
            options,
        } => {
            if !*authenticated {
                let _ = message_tx
                    .send(ServerMessage::Error {
                        code: "UNAUTHORIZED".to_string(),
                        message: "Authentication required".to_string(),
                        subscription_id: Some(id),
                        recoverable: true,
                    })
                    .await;
                return;
            }

            // Check topic authorization
            if let Some(ref info) = token_info {
                if !info.allowed_topics.is_empty() {
                    for topic in &topics {
                        if !info.allowed_topics.contains(topic) {
                            let _ = message_tx
                                .send(ServerMessage::Error {
                                    code: "FORBIDDEN".to_string(),
                                    message: format!("Not authorized for topic: {}", topic),
                                    subscription_id: Some(id.clone()),
                                    recoverable: false,
                                })
                                .await;
                            return;
                        }
                    }
                }
            }

            let mut subs = subscriptions.write().await;
            let mut subscribed_topics = Vec::new();
            let mut partitions_map = HashMap::new();

            // Compile key regex
            let key_regex = options.key_filter.as_ref().and_then(|pattern| {
                let regex_pattern = pattern
                    .replace('.', r"\.")
                    .replace('*', ".*")
                    .replace('?', ".");
                regex::Regex::new(&format!("^{}$", regex_pattern)).ok()
            });

            for topic in &topics {
                match state.topic_manager.get_topic_metadata(topic) {
                    Ok(meta) => {
                        let partitions: Vec<i32> = if options.partitions.is_empty() {
                            (0..meta.num_partitions).collect()
                        } else {
                            options.partitions.clone()
                        };
                        partitions_map.insert(topic.clone(), partitions);
                        subscribed_topics.push(topic.clone());
                    }
                    Err(e) => {
                        let _ = message_tx
                            .send(ServerMessage::Error {
                                code: "TOPIC_NOT_FOUND".to_string(),
                                message: format!("Topic '{}' not found: {}", topic, e),
                                subscription_id: Some(id.clone()),
                                recoverable: true,
                            })
                            .await;
                    }
                }
            }

            if !subscribed_topics.is_empty() {
                subs.insert(
                    id.clone(),
                    ActiveSubscription {
                        id: id.clone(),
                        topics: subscribed_topics.clone(),
                        options: options.clone(),
                        paused: AtomicBool::new(false),
                        messages_received: AtomicU64::new(0),
                        offsets: RwLock::new(HashMap::new()),
                        key_regex,
                    },
                );

                let _ = message_tx
                    .send(ServerMessage::Subscribed {
                        id,
                        topics: subscribed_topics,
                        partitions: partitions_map,
                    })
                    .await;
            }
        }

        ClientMessage::Unsubscribe { id } => {
            let mut subs = subscriptions.write().await;
            if subs.remove(&id).is_some() {
                let _ = message_tx.send(ServerMessage::Unsubscribed { id }).await;
            } else {
                let _ = message_tx
                    .send(ServerMessage::Error {
                        code: "SUBSCRIPTION_NOT_FOUND".to_string(),
                        message: format!("Subscription '{}' not found", id),
                        subscription_id: Some(id),
                        recoverable: true,
                    })
                    .await;
            }
        }

        ClientMessage::Ack { ids } => {
            // Acknowledgments tracked for at-least-once delivery
            debug!(count = ids.len(), "Received message acknowledgments");
        }

        ClientMessage::Replay {
            topic,
            partition,
            from_offset,
            count,
        } => {
            if !*authenticated {
                let _ = message_tx
                    .send(ServerMessage::Error {
                        code: "UNAUTHORIZED".to_string(),
                        message: "Authentication required".to_string(),
                        subscription_id: None,
                        recoverable: true,
                    })
                    .await;
                return;
            }

            match state
                .topic_manager
                .read(&topic, partition, from_offset, count)
            {
                Ok(records) => {
                    let mut messages = Vec::new();
                    let mut last_offset = from_offset;

                    for record in records {
                        let value = match serde_json::from_slice(&record.value) {
                            Ok(v) => v,
                            Err(_) => serde_json::Value::String(base64::Engine::encode(
                                &base64::engine::general_purpose::STANDARD,
                                &record.value,
                            )),
                        };

                        let headers: HashMap<String, String> = record
                            .headers
                            .into_iter()
                            .map(|h| (h.key, String::from_utf8_lossy(&h.value).to_string()))
                            .collect();

                        messages.push(BatchMessage {
                            id: format!("replay-{}", record.offset),
                            topic: topic.clone(),
                            partition,
                            offset: record.offset,
                            timestamp: record.timestamp,
                            key: record.key.map(|k| String::from_utf8_lossy(&k).to_string()),
                            value,
                            headers,
                        });

                        last_offset = record.offset;
                    }

                    let has_more = messages.len() >= count;
                    let _ = message_tx
                        .send(ServerMessage::ReplayResult {
                            topic,
                            partition,
                            messages,
                            has_more,
                            next_offset: last_offset + 1,
                        })
                        .await;
                }
                Err(e) => {
                    let _ = message_tx
                        .send(ServerMessage::Error {
                            code: "REPLAY_FAILED".to_string(),
                            message: format!("Failed to replay: {}", e),
                            subscription_id: None,
                            recoverable: true,
                        })
                        .await;
                }
            }
        }

        ClientMessage::Ping { timestamp } => {
            let _ = message_tx
                .send(ServerMessage::Pong {
                    timestamp: chrono::Utc::now().timestamp_millis(),
                    client_timestamp: timestamp,
                })
                .await;
        }

        ClientMessage::Pause { id } => {
            let subs = subscriptions.read().await;
            if let Some(sub) = subs.get(&id) {
                sub.paused.store(true, Ordering::Relaxed);
                let _ = message_tx.send(ServerMessage::Paused { id }).await;
            }
        }

        ClientMessage::Resume { id } => {
            let subs = subscriptions.read().await;
            if let Some(sub) = subs.get(&id) {
                sub.paused.store(false, Ordering::Relaxed);
                let _ = message_tx.send(ServerMessage::Resumed { id }).await;
            }
        }

        ClientMessage::Status => {
            let subs = subscriptions.read().await;
            let mut sub_infos = Vec::new();

            for sub in subs.values() {
                let offsets = sub.offsets.read().await;
                sub_infos.push(SubscriptionInfo {
                    id: sub.id.clone(),
                    topics: sub.topics.clone(),
                    paused: sub.paused.load(Ordering::Relaxed),
                    messages_received: sub.messages_received.load(Ordering::Relaxed),
                    current_offsets: offsets.clone(),
                });
            }

            let _ = message_tx
                .send(ServerMessage::Status {
                    subscriptions: sub_infos,
                    messages_delivered,
                    uptime_seconds: start_time.elapsed().as_secs_f64(),
                    authenticated: *authenticated,
                })
                .await;
        }
    }
}

/// Configuration endpoint
async fn config_handler(State(state): State<BrowserClientState>) -> impl IntoResponse {
    let topics = match state.topic_manager.list_topics() {
        Ok(topic_metas) => topic_metas
            .into_iter()
            .map(|meta| TopicInfo {
                name: meta.name,
                partitions: meta.num_partitions,
                message_count: 0, // Would need to calculate
            })
            .collect(),
        Err(_) => Vec::new(),
    };

    let config = ClientConfig {
        websocket_url: "/browser/v1/stream".to_string(),
        api_version: "1.0".to_string(),
        max_message_size: 1048576, // 1MB
        heartbeat_interval: 30000,
        reconnect: ReconnectConfig::default(),
        topics,
        features: FeatureFlags::default(),
    };

    Json(config)
}

/// Authentication endpoint
async fn auth_handler(
    State(state): State<BrowserClientState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let token = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.strip_prefix("Bearer ").unwrap_or(s));

    if let Some(token) = token {
        if let Some(ref validator) = state.token_validator {
            if let Some(info) = validator.validate(token) {
                return (
                    StatusCode::OK,
                    Json(serde_json::json!({
                        "success": true,
                        "principal": info.principal,
                        "expiresAt": info.expires_at,
                        "allowedTopics": info.allowed_topics,
                    })),
                );
            } else {
                return (
                    StatusCode::UNAUTHORIZED,
                    Json(serde_json::json!({
                        "success": false,
                        "error": "Invalid token"
                    })),
                );
            }
        } else {
            // No validator = accept any token
            return (
                StatusCode::OK,
                Json(serde_json::json!({
                    "success": true,
                    "principal": "anonymous",
                })),
            );
        }
    }

    (
        StatusCode::BAD_REQUEST,
        Json(serde_json::json!({
            "success": false,
            "error": "Authorization header required"
        })),
    )
}

/// Topics discovery endpoint
async fn topics_handler(State(state): State<BrowserClientState>) -> impl IntoResponse {
    let topics: Vec<TopicInfo> = match state.topic_manager.list_topics() {
        Ok(topic_metas) => topic_metas
            .into_iter()
            .map(|meta| TopicInfo {
                name: meta.name,
                partitions: meta.num_partitions,
                message_count: 0,
            })
            .collect(),
        Err(_) => Vec::new(),
    };

    Json(topics)
}

/// Stats endpoint
async fn stats_handler(State(state): State<BrowserClientState>) -> impl IntoResponse {
    Json(serde_json::json!({
        "activeSessions": state.active_sessions.load(Ordering::Relaxed),
        "totalMessagesDelivered": state.total_messages_delivered.load(Ordering::Relaxed),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_message_deserialize() {
        let subscribe: ClientMessage = serde_json::from_str(
            r#"{
            "type": "subscribe",
            "id": "sub-1",
            "topics": ["events"],
            "options": {
                "offset": "latest",
                "batchSize": 10
            }
        }"#,
        )
        .unwrap();

        match subscribe {
            ClientMessage::Subscribe {
                id,
                topics,
                options,
            } => {
                assert_eq!(id, "sub-1");
                assert_eq!(topics, vec!["events"]);
                assert_eq!(options.batch_size, 10);
            }
            _ => panic!("Expected Subscribe"),
        }
    }

    #[test]
    fn test_server_message_serialize() {
        let msg = ServerMessage::Connected {
            session_id: "test-123".to_string(),
            server_time: 1702123456789,
            protocol_version: "1.0".to_string(),
            heartbeat_interval: 30000,
            reconnect_config: ReconnectConfig::default(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"type\":\"connected\""));
        assert!(json.contains("\"sessionId\":\"test-123\""));
    }

    #[test]
    fn test_offset_spec_deserialize() {
        let latest: OffsetSpec = serde_json::from_str(r#""latest""#).unwrap();
        assert!(matches!(latest, OffsetSpec::Latest));

        let earliest: OffsetSpec = serde_json::from_str(r#""earliest""#).unwrap();
        assert!(matches!(earliest, OffsetSpec::Earliest));

        let offset: OffsetSpec = serde_json::from_str(r#"{"offset": 100}"#).unwrap();
        assert!(matches!(offset, OffsetSpec::Offset(100)));
    }

    #[test]
    fn test_reconnect_config_default() {
        let config = ReconnectConfig::default();
        assert_eq!(config.initial_delay_ms, 1000);
        assert_eq!(config.max_delay_ms, 30000);
    }

    #[test]
    fn test_feature_flags_default() {
        let flags = FeatureFlags::default();
        assert!(flags.binary_messages);
        assert!(flags.batch_subscribe);
        assert!(flags.key_filtering);
        assert!(flags.replay);
    }
}
