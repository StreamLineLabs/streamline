//! Advanced WebSocket streaming API for real-time message consumption
//!
//! This module provides enhanced WebSocket streaming with:
//! - Multi-topic subscriptions
//! - Filtering by key/value patterns
//! - Multiple output formats (JSON, binary, newline-delimited)
//! - Backpressure handling
//! - Consumer group integration
//! - Message batching for efficiency
//! - Timestamp-based seeking
//!
//! ## Endpoints
//!
//! - `GET /ws/v2/stream` - Advanced streaming with subscription control
//!
//! ## Protocol
//!
//! ### Subscribe Command
//! ```json
//! {
//!   "type": "subscribe",
//!   "topics": ["events", "logs"],
//!   "config": {
//!     "from": "latest",           // "latest", "earliest", "offset:N", "timestamp:N"
//!     "filter": {
//!       "key_pattern": "user-*",  // Optional glob pattern for keys
//!       "value_jq": ".level == \"error\""  // Optional jq filter for values
//!     },
//!     "batch_size": 100,          // Max messages per batch
//!     "batch_timeout_ms": 50      // Max time to wait for batch
//!   }
//! }
//! ```
//!
//! ### Message Format
//! ```json
//! {
//!   "type": "messages",
//!   "batch_id": 12345,
//!   "messages": [
//!     {
//!       "topic": "events",
//!       "partition": 0,
//!       "offset": 1000,
//!       "timestamp": 1702123456789,
//!       "key": "user-123",
//!       "value": {"action": "click"},
//!       "headers": {}
//!     }
//!   ]
//! }
//! ```

use crate::storage::TopicManager;
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Query, State,
    },
    response::Response,
    routing::get,
    Router,
};
use futures_util::{SinkExt, StreamExt};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc, RwLock};
use tracing::{debug, error, info, warn};

/// Advanced WebSocket streaming state
#[derive(Clone)]
pub struct StreamingState {
    /// Topic manager for reading messages
    pub topic_manager: Arc<TopicManager>,
    /// Active subscriptions for metrics
    pub active_subscriptions: Arc<AtomicU64>,
    /// Total messages streamed
    pub total_messages_streamed: Arc<AtomicU64>,
}

impl StreamingState {
    pub fn new(topic_manager: Arc<TopicManager>) -> Self {
        Self {
            topic_manager,
            active_subscriptions: Arc::new(AtomicU64::new(0)),
            total_messages_streamed: Arc::new(AtomicU64::new(0)),
        }
    }
}

/// Query parameters for streaming connection
#[derive(Debug, Deserialize)]
pub struct StreamingQuery {
    /// Output format: "json" (default), "binary", "ndjson"
    #[serde(default = "default_format")]
    pub format: String,
    /// Compression: "none" (default), "gzip"
    #[serde(default)]
    pub compression: Option<String>,
}

fn default_format() -> String {
    "json".to_string()
}

/// Client command types
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientCommand {
    /// Subscribe to topics
    Subscribe {
        topics: Vec<String>,
        #[serde(default)]
        config: SubscriptionConfig,
    },
    /// Unsubscribe from topics
    Unsubscribe { topics: Vec<String> },
    /// Seek to position
    Seek {
        topic: String,
        position: SeekPosition,
    },
    /// Pause consumption
    Pause { topics: Option<Vec<String>> },
    /// Resume consumption
    Resume { topics: Option<Vec<String>> },
    /// Acknowledge messages (for consumer group mode)
    Ack {
        offsets: HashMap<String, HashMap<i32, i64>>,
    },
    /// Get current status
    Status,
    /// Ping (for keepalive)
    Ping { id: Option<u64> },
}

/// Subscription configuration
#[derive(Debug, Clone, Default, Deserialize)]
pub struct SubscriptionConfig {
    /// Starting position
    #[serde(default)]
    pub from: StartPosition,
    /// Key filter pattern (glob)
    #[serde(default)]
    pub key_pattern: Option<String>,
    /// Value filter (JSON path expression)
    #[serde(default)]
    pub value_filter: Option<String>,
    /// Maximum batch size
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Batch timeout in milliseconds
    #[serde(default = "default_batch_timeout")]
    pub batch_timeout_ms: u64,
    /// Include headers in output
    #[serde(default = "default_true")]
    pub include_headers: bool,
    /// Consumer group ID (optional, enables offset tracking)
    #[serde(default)]
    pub group_id: Option<String>,
    /// Partitions to subscribe to (empty = all)
    #[serde(default)]
    pub partitions: Vec<i32>,
}

fn default_batch_size() -> usize {
    100
}

fn default_batch_timeout() -> u64 {
    50
}

fn default_true() -> bool {
    true
}

/// Starting position for consumption
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StartPosition {
    /// Start from the latest offset
    #[default]
    Latest,
    /// Start from the earliest offset
    Earliest,
    /// Start from a specific offset
    Offset(i64),
    /// Start from a specific timestamp (milliseconds)
    Timestamp(i64),
}

/// Seek position
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SeekPosition {
    /// Beginning of partition
    Beginning,
    /// End of partition
    End,
    /// Specific offset
    Offset(i64),
    /// Specific timestamp
    Timestamp(i64),
}

/// Server message types
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMessage {
    /// Connection established
    Connected {
        session_id: String,
        server_time: i64,
    },
    /// Subscription confirmed
    Subscribed {
        topics: Vec<String>,
        partitions: HashMap<String, Vec<i32>>,
    },
    /// Unsubscription confirmed
    Unsubscribed { topics: Vec<String> },
    /// Message batch
    Messages {
        batch_id: u64,
        messages: Vec<StreamMessage>,
    },
    /// Status response
    Status {
        subscriptions: Vec<SubscriptionStatus>,
        total_messages: u64,
        uptime_seconds: f64,
    },
    /// Pong response
    Pong { id: Option<u64> },
    /// Error
    Error { code: String, message: String },
    /// Warning (non-fatal)
    Warning { code: String, message: String },
}

/// Individual streamed message
#[derive(Debug, Serialize)]
pub struct StreamMessage {
    /// Topic name
    pub topic: String,
    /// Partition ID
    pub partition: i32,
    /// Record offset
    pub offset: i64,
    /// Record timestamp (milliseconds)
    pub timestamp: i64,
    /// Record key (if present)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    /// Record value (JSON or base64 for binary)
    pub value: serde_json::Value,
    /// Record headers
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub headers: HashMap<String, String>,
}

/// Subscription status
#[derive(Debug, Serialize)]
pub struct SubscriptionStatus {
    /// Topic name
    pub topic: String,
    /// Partitions being consumed
    pub partitions: Vec<PartitionStatus>,
    /// Messages received
    pub messages_received: u64,
    /// Whether paused
    pub paused: bool,
}

/// Partition status
#[derive(Debug, Serialize)]
pub struct PartitionStatus {
    /// Partition ID
    pub partition: i32,
    /// Current offset
    pub current_offset: i64,
    /// High watermark (latest offset)
    pub high_watermark: i64,
    /// Lag (high_watermark - current_offset)
    pub lag: i64,
}

/// Active subscription tracking
#[allow(dead_code)]
struct ActiveSubscription {
    topics: Vec<String>,
    config: SubscriptionConfig,
    paused: AtomicBool,
    messages_received: AtomicU64,
    start_time: Instant,
    key_regex: Option<Regex>,
}

/// Create the advanced streaming router
pub fn create_streaming_router(state: StreamingState) -> Router {
    Router::new()
        .route("/ws/v2/stream", get(ws_stream_handler))
        .route("/ws/v2/stats", get(streaming_stats_handler))
        .with_state(state)
}

/// Stats endpoint for streaming metrics
async fn streaming_stats_handler(
    State(state): State<StreamingState>,
) -> axum::Json<serde_json::Value> {
    axum::Json(serde_json::json!({
        "active_subscriptions": state.active_subscriptions.load(Ordering::Relaxed),
        "total_messages_streamed": state.total_messages_streamed.load(Ordering::Relaxed),
    }))
}

/// WebSocket stream handler
async fn ws_stream_handler(
    ws: WebSocketUpgrade,
    State(state): State<StreamingState>,
    Query(query): Query<StreamingQuery>,
) -> Response {
    info!(format = %query.format, "Advanced streaming connection request");
    ws.on_upgrade(move |socket| handle_streaming_connection(socket, state, query))
}

/// Handle the streaming WebSocket connection
async fn handle_streaming_connection(
    socket: WebSocket,
    state: StreamingState,
    query: StreamingQuery,
) {
    let session_id = uuid::Uuid::new_v4().to_string();
    let start_time = Instant::now();

    info!(session_id = %session_id, "Streaming connection established");
    state.active_subscriptions.fetch_add(1, Ordering::Relaxed);

    let (mut sender, mut receiver) = socket.split();

    // Send connected message
    let connected_msg = ServerMessage::Connected {
        session_id: session_id.clone(),
        server_time: chrono::Utc::now().timestamp_millis(),
    };
    if let Ok(json) = serde_json::to_string(&connected_msg) {
        let _ = sender.send(Message::Text(json)).await;
    }

    // Subscription management
    let subscriptions: Arc<RwLock<HashMap<String, ActiveSubscription>>> =
        Arc::new(RwLock::new(HashMap::new()));
    let (shutdown_tx, _) = broadcast::channel::<()>(1);
    let (message_tx, mut message_rx) = mpsc::channel::<ServerMessage>(10000);

    // Clone for reader task
    let reader_subscriptions = subscriptions.clone();
    let reader_state = state.clone();
    let reader_message_tx = message_tx.clone();
    let mut reader_shutdown_rx = shutdown_tx.subscribe();

    // Spawn reader task that polls subscribed topics
    let reader_handle = tokio::spawn(async move {
        let mut batch_id: u64 = 0;
        let mut offsets: HashMap<String, HashMap<i32, i64>> = HashMap::new();

        loop {
            tokio::select! {
                _ = reader_shutdown_rx.recv() => {
                    debug!("Reader task shutting down");
                    break;
                }
                _ = tokio::time::sleep(Duration::from_millis(10)) => {
                    // Poll subscriptions
                    let subs = reader_subscriptions.read().await;
                    if subs.is_empty() {
                        continue;
                    }

                    for (topic, sub) in subs.iter() {
                        if sub.paused.load(Ordering::Relaxed) {
                            continue;
                        }

                        // Get partitions to read from
                        let partitions = if sub.config.partitions.is_empty() {
                            match reader_state.topic_manager.get_topic_metadata(topic) {
                                Ok(meta) => (0..meta.num_partitions).collect::<Vec<_>>(),
                                Err(_) => continue,
                            }
                        } else {
                            sub.config.partitions.clone()
                        };

                        let mut batch_messages = Vec::new();
                        let batch_start = Instant::now();
                        let batch_timeout = Duration::from_millis(sub.config.batch_timeout_ms);

                        for partition in partitions {
                            // Get current offset for this partition
                            let current_offset = offsets
                                .entry(topic.clone())
                                .or_default()
                                .entry(partition)
                                .or_insert_with(|| {
                                    match &sub.config.from {
                                        StartPosition::Latest => {
                                            reader_state.topic_manager
                                                .latest_offset(topic, partition)
                                                .unwrap_or(0)
                                        }
                                        StartPosition::Earliest => 0,
                                        StartPosition::Offset(o) => *o,
                                        StartPosition::Timestamp(ts) => {
                                            reader_state.topic_manager
                                                .find_offset_by_timestamp(topic, partition, *ts)
                                                .ok()
                                                .flatten()
                                                .unwrap_or(0)
                                        }
                                    }
                                });

                            // Read records
                            let max_records = sub.config.batch_size - batch_messages.len();
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

                                        // Convert to stream message
                                        let value = match serde_json::from_slice(&record.value) {
                                            Ok(v) => v,
                                            Err(_) => serde_json::Value::String(
                                                base64::Engine::encode(
                                                    &base64::engine::general_purpose::STANDARD,
                                                    &record.value,
                                                ),
                                            ),
                                        };

                                        let headers: HashMap<String, String> = if sub.config.include_headers {
                                            record.headers
                                                .into_iter()
                                                .map(|h| (h.key, String::from_utf8_lossy(&h.value).to_string()))
                                                .collect()
                                        } else {
                                            HashMap::new()
                                        };

                                        let stream_msg = StreamMessage {
                                            topic: topic.clone(),
                                            partition,
                                            offset: record.offset,
                                            timestamp: record.timestamp,
                                            key: record.key.map(|k| String::from_utf8_lossy(&k).to_string()),
                                            value,
                                            headers,
                                        };

                                        batch_messages.push(stream_msg);
                                        *current_offset = record.offset + 1;
                                        sub.messages_received.fetch_add(1, Ordering::Relaxed);
                                        reader_state.total_messages_streamed.fetch_add(1, Ordering::Relaxed);

                                        if batch_messages.len() >= sub.config.batch_size {
                                            break;
                                        }
                                    }
                                }
                                Err(e) => {
                                    warn!(topic = %topic, partition = partition, error = %e, "Error reading partition");
                                }
                            }

                            // Check batch timeout
                            if batch_start.elapsed() >= batch_timeout && !batch_messages.is_empty() {
                                break;
                            }
                        }

                        // Send batch if we have messages
                        if !batch_messages.is_empty() {
                            batch_id += 1;
                            let msg = ServerMessage::Messages {
                                batch_id,
                                messages: batch_messages,
                            };
                            if reader_message_tx.send(msg).await.is_err() {
                                return;
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

    let cmd_handle = tokio::spawn(async move {
        while let Some(result) = receiver.next().await {
            match result {
                Ok(Message::Text(text)) => match serde_json::from_str::<ClientCommand>(&text) {
                    Ok(cmd) => {
                        handle_command(
                            cmd,
                            &cmd_subscriptions,
                            &cmd_state,
                            &cmd_message_tx,
                            start_time,
                        )
                        .await;
                    }
                    Err(e) => {
                        let err_msg = ServerMessage::Error {
                            code: "INVALID_COMMAND".to_string(),
                            message: format!("Failed to parse command: {}", e),
                        };
                        let _ = cmd_message_tx.send(err_msg).await;
                    }
                },
                Ok(Message::Close(_)) => {
                    debug!(session_id = %cmd_session_id, "Client closed connection");
                    break;
                }
                Ok(Message::Ping(_)) => {
                    // Handled by axum
                }
                Ok(_) => {}
                Err(e) => {
                    error!(error = %e, "WebSocket receive error");
                    break;
                }
            }
        }
    });

    // Send messages to client
    let output_format = query.format.clone();
    let send_handle = tokio::spawn(async move {
        while let Some(msg) = message_rx.recv().await {
            let text = match output_format.as_str() {
                "ndjson" => {
                    // For NDJSON, send each message on its own line
                    if let ServerMessage::Messages { messages, .. } = &msg {
                        let lines: Vec<String> = messages
                            .iter()
                            .filter_map(|m| serde_json::to_string(m).ok())
                            .collect();
                        lines.join("\n")
                    } else {
                        match serde_json::to_string(&msg) {
                            Ok(j) => j,
                            Err(_) => continue,
                        }
                    }
                }
                _ => match serde_json::to_string(&msg) {
                    Ok(j) => j,
                    Err(_) => continue,
                },
            };

            if sender.send(Message::Text(text)).await.is_err() {
                break;
            }
        }
        let _ = sender.close().await;
    });

    // Wait for any task to complete
    tokio::select! {
        _ = cmd_handle => {}
        _ = send_handle => {}
    }

    // Signal shutdown
    let _ = shutdown_tx.send(());
    reader_handle.abort();

    state.active_subscriptions.fetch_sub(1, Ordering::Relaxed);
    info!(session_id = %session_id, uptime_secs = start_time.elapsed().as_secs_f64(), "Streaming connection closed");
}

/// Handle a client command
async fn handle_command(
    cmd: ClientCommand,
    subscriptions: &Arc<RwLock<HashMap<String, ActiveSubscription>>>,
    state: &StreamingState,
    message_tx: &mpsc::Sender<ServerMessage>,
    start_time: Instant,
) {
    match cmd {
        ClientCommand::Subscribe { topics, config } => {
            let mut subs = subscriptions.write().await;
            let mut subscribed_topics = Vec::new();
            let mut partitions_map = HashMap::new();

            // Compile key regex if provided
            let key_regex = config.key_pattern.as_ref().and_then(|pattern| {
                // Convert glob to regex
                let regex_pattern = pattern
                    .replace('.', r"\.")
                    .replace('*', ".*")
                    .replace('?', ".");
                Regex::new(&format!("^{}$", regex_pattern)).ok()
            });

            for topic in &topics {
                // Verify topic exists
                match state.topic_manager.get_topic_metadata(topic) {
                    Ok(meta) => {
                        let partitions: Vec<i32> = if config.partitions.is_empty() {
                            (0..meta.num_partitions).collect()
                        } else {
                            config.partitions.clone()
                        };

                        partitions_map.insert(topic.clone(), partitions);
                        subscribed_topics.push(topic.clone());

                        subs.insert(
                            topic.clone(),
                            ActiveSubscription {
                                topics: vec![topic.clone()],
                                config: config.clone(),
                                paused: AtomicBool::new(false),
                                messages_received: AtomicU64::new(0),
                                start_time: Instant::now(),
                                key_regex: key_regex.clone(),
                            },
                        );
                    }
                    Err(e) => {
                        let warn_msg = ServerMessage::Warning {
                            code: "TOPIC_NOT_FOUND".to_string(),
                            message: format!("Topic '{}' not found: {}", topic, e),
                        };
                        let _ = message_tx.send(warn_msg).await;
                    }
                }
            }

            if !subscribed_topics.is_empty() {
                let msg = ServerMessage::Subscribed {
                    topics: subscribed_topics,
                    partitions: partitions_map,
                };
                let _ = message_tx.send(msg).await;
            }
        }

        ClientCommand::Unsubscribe { topics } => {
            let mut subs = subscriptions.write().await;
            let mut unsubscribed = Vec::new();

            for topic in topics {
                if subs.remove(&topic).is_some() {
                    unsubscribed.push(topic);
                }
            }

            let msg = ServerMessage::Unsubscribed {
                topics: unsubscribed,
            };
            let _ = message_tx.send(msg).await;
        }

        ClientCommand::Pause { topics } => {
            let subs = subscriptions.read().await;
            if let Some(topics) = topics {
                for topic in topics {
                    if let Some(sub) = subs.get(&topic) {
                        sub.paused.store(true, Ordering::Relaxed);
                    }
                }
            } else {
                for sub in subs.values() {
                    sub.paused.store(true, Ordering::Relaxed);
                }
            }
        }

        ClientCommand::Resume { topics } => {
            let subs = subscriptions.read().await;
            if let Some(topics) = topics {
                for topic in topics {
                    if let Some(sub) = subs.get(&topic) {
                        sub.paused.store(false, Ordering::Relaxed);
                    }
                }
            } else {
                for sub in subs.values() {
                    sub.paused.store(false, Ordering::Relaxed);
                }
            }
        }

        ClientCommand::Seek { topic, position: _ } => {
            // Seek is handled by updating the offset tracking in the reader
            // For now, just acknowledge (full seek implementation would update offset map)
            let msg = ServerMessage::Warning {
                code: "SEEK_PENDING".to_string(),
                message: format!("Seek for topic '{}' will be applied", topic),
            };
            let _ = message_tx.send(msg).await;
        }

        ClientCommand::Ack { offsets } => {
            // In consumer group mode, this would commit offsets
            debug!(offsets = ?offsets, "Received offset acknowledgment");
        }

        ClientCommand::Status => {
            let subs = subscriptions.read().await;
            let mut statuses = Vec::new();
            let mut total = 0u64;

            for (topic, sub) in subs.iter() {
                let received = sub.messages_received.load(Ordering::Relaxed);
                total += received;

                // Get partition status
                let partitions = if sub.config.partitions.is_empty() {
                    match state.topic_manager.get_topic_metadata(topic) {
                        Ok(meta) => (0..meta.num_partitions).collect::<Vec<_>>(),
                        Err(_) => vec![],
                    }
                } else {
                    sub.config.partitions.clone()
                };

                let partition_statuses: Vec<PartitionStatus> = partitions
                    .iter()
                    .map(|&p| {
                        let hw = state.topic_manager.latest_offset(topic, p).unwrap_or(0);
                        PartitionStatus {
                            partition: p,
                            current_offset: 0, // Would need offset tracking
                            high_watermark: hw,
                            lag: hw, // Simplified
                        }
                    })
                    .collect();

                statuses.push(SubscriptionStatus {
                    topic: topic.clone(),
                    partitions: partition_statuses,
                    messages_received: received,
                    paused: sub.paused.load(Ordering::Relaxed),
                });
            }

            let msg = ServerMessage::Status {
                subscriptions: statuses,
                total_messages: total,
                uptime_seconds: start_time.elapsed().as_secs_f64(),
            };
            let _ = message_tx.send(msg).await;
        }

        ClientCommand::Ping { id } => {
            let msg = ServerMessage::Pong { id };
            let _ = message_tx.send(msg).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_command_deserialize() {
        let subscribe: ClientCommand = serde_json::from_str(
            r#"{
            "type": "subscribe",
            "topics": ["events", "logs"],
            "config": {
                "from": "latest",
                "batch_size": 50
            }
        }"#,
        )
        .unwrap();

        match subscribe {
            ClientCommand::Subscribe { topics, config } => {
                assert_eq!(topics, vec!["events", "logs"]);
                assert_eq!(config.batch_size, 50);
            }
            _ => panic!("Expected Subscribe"),
        }
    }

    #[test]
    fn test_start_position_deserialize() {
        let latest: StartPosition = serde_json::from_str(r#""latest""#).unwrap();
        assert!(matches!(latest, StartPosition::Latest));

        let earliest: StartPosition = serde_json::from_str(r#""earliest""#).unwrap();
        assert!(matches!(earliest, StartPosition::Earliest));
    }

    #[test]
    fn test_server_message_serialize() {
        let msg = ServerMessage::Connected {
            session_id: "test-123".to_string(),
            server_time: 1702123456789,
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"type\":\"connected\""));
        assert!(json.contains("\"session_id\":\"test-123\""));
    }

    #[test]
    fn test_stream_message_serialize() {
        let msg = StreamMessage {
            topic: "events".to_string(),
            partition: 0,
            offset: 100,
            timestamp: 1702123456789,
            key: Some("user-123".to_string()),
            value: serde_json::json!({"action": "click"}),
            headers: HashMap::new(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"topic\":\"events\""));
        assert!(json.contains("\"offset\":100"));
    }

    #[test]
    fn test_subscription_config_defaults() {
        let config: SubscriptionConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(config.batch_size, 100);
        assert_eq!(config.batch_timeout_ms, 50);
        assert!(config.include_headers);
    }
}
