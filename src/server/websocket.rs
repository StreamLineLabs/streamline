//! WebSocket gateway for real-time message consumption
//!
//! This module provides WebSocket endpoints for real-time streaming of messages
//! from topics. Clients can connect via WebSocket and receive messages as they
//! are produced to topics.
//!
//! ## Endpoints
//!
//! - `GET /ws/consume/{topic}` - Subscribe to all partitions of a topic
//! - `GET /ws/consume/{topic}/{partition}` - Subscribe to a specific partition
//!
//! ## Protocol
//!
//! Messages are sent as JSON objects:
//! ```json
//! {
//!   "topic": "events",
//!   "partition": 0,
//!   "offset": 12345,
//!   "timestamp": 1702123456789,
//!   "key": "user123",
//!   "value": {"action": "click", "page": "/home"}
//! }
//! ```
//!
//! Clients can send control messages:
//! - `{"action": "seek", "offset": 1000}` - Seek to a specific offset
//! - `{"action": "pause"}` - Pause consumption
//! - `{"action": "resume"}` - Resume consumption

use crate::storage::TopicManager;
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, Query, State,
    },
    response::Response,
    routing::get,
    Router,
};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, error, info, warn};

/// WebSocket state shared across connections
#[derive(Clone)]
pub struct WebSocketState {
    /// Topic manager for reading messages
    pub topic_manager: Arc<TopicManager>,
}

/// Query parameters for WebSocket connection
#[derive(Debug, Deserialize)]
pub struct WebSocketQuery {
    /// Starting offset (default: latest)
    #[serde(default)]
    pub offset: Option<i64>,
    /// Poll interval in milliseconds (default: 100)
    #[serde(default = "default_poll_interval")]
    pub poll_interval_ms: u64,
}

fn default_poll_interval() -> u64 {
    100
}

/// Message sent to WebSocket clients
#[derive(Debug, Serialize)]
pub struct WebSocketRecord {
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
    /// Record value
    pub value: serde_json::Value,
    /// Record headers
    #[serde(skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub headers: std::collections::HashMap<String, String>,
}

/// Control message from client
#[derive(Debug, Deserialize)]
#[serde(tag = "action", rename_all = "lowercase")]
pub enum ClientControl {
    /// Seek to a specific offset
    Seek { offset: i64 },
    /// Pause consumption
    Pause,
    /// Resume consumption
    Resume,
}

/// Create the WebSocket router
pub fn create_websocket_router(state: WebSocketState) -> Router {
    Router::new()
        .route("/ws/consume/:topic", get(ws_consume_topic))
        .route("/ws/consume/:topic/:partition", get(ws_consume_partition))
        .with_state(state)
}

/// WebSocket handler for subscribing to all partitions of a topic
async fn ws_consume_topic(
    ws: WebSocketUpgrade,
    State(state): State<WebSocketState>,
    Path(topic): Path<String>,
    Query(query): Query<WebSocketQuery>,
) -> Response {
    info!(topic = %topic, "WebSocket connection request for topic");
    ws.on_upgrade(move |socket| handle_topic_subscription(socket, state, topic, query))
}

/// WebSocket handler for subscribing to a specific partition
async fn ws_consume_partition(
    ws: WebSocketUpgrade,
    State(state): State<WebSocketState>,
    Path((topic, partition)): Path<(String, i32)>,
    Query(query): Query<WebSocketQuery>,
) -> Response {
    info!(topic = %topic, partition = partition, "WebSocket connection request for partition");
    ws.on_upgrade(move |socket| {
        handle_partition_subscription(socket, state, topic, partition, query)
    })
}

/// Handle WebSocket subscription to all partitions of a topic
async fn handle_topic_subscription(
    socket: WebSocket,
    state: WebSocketState,
    topic: String,
    query: WebSocketQuery,
) {
    // Get number of partitions
    let num_partitions = match state.topic_manager.get_topic_metadata(&topic) {
        Ok(metadata) => metadata.num_partitions,
        Err(e) => {
            error!(topic = %topic, error = %e, "Topic not found for WebSocket subscription");
            // Send error and close
            let (mut sender, _) = socket.split();
            let error_msg = serde_json::json!({
                "error": "TOPIC_NOT_FOUND",
                "message": format!("Topic '{}' does not exist", topic)
            });
            let _ = sender.send(Message::Text(error_msg.to_string())).await;
            let _ = sender.close().await;
            return;
        }
    };

    info!(
        topic = %topic,
        partitions = num_partitions,
        "Starting WebSocket subscription to all partitions"
    );

    // Create channels for coordination
    let (control_tx, _control_rx) = broadcast::channel::<ControlMessage>(32);
    let (record_tx, record_rx) = mpsc::channel::<WebSocketRecord>(1000);

    // Spawn reader tasks for each partition
    let mut handles = Vec::new();
    for partition in 0..num_partitions {
        let state = state.clone();
        let topic = topic.clone();
        let record_tx = record_tx.clone();
        let mut control_rx = control_tx.subscribe();

        let initial_offset = query.offset.unwrap_or_else(|| {
            state
                .topic_manager
                .latest_offset(&topic, partition)
                .unwrap_or(0)
        });

        let poll_interval = Duration::from_millis(query.poll_interval_ms);

        let handle = tokio::spawn(async move {
            partition_reader(
                state,
                topic,
                partition,
                initial_offset,
                poll_interval,
                record_tx,
                &mut control_rx,
            )
            .await
        });
        handles.push(handle);
    }

    // Drop the original sender so channels close when tasks finish
    drop(record_tx);

    // Handle the WebSocket connection
    handle_websocket_connection(socket, control_tx, record_rx).await;

    // Clean up reader tasks
    for handle in handles {
        handle.abort();
    }
}

/// Handle WebSocket subscription to a specific partition
async fn handle_partition_subscription(
    socket: WebSocket,
    state: WebSocketState,
    topic: String,
    partition: i32,
    query: WebSocketQuery,
) {
    // Verify partition exists
    let metadata = match state.topic_manager.get_topic_metadata(&topic) {
        Ok(m) => m,
        Err(e) => {
            error!(topic = %topic, error = %e, "Topic not found for WebSocket subscription");
            let (mut sender, _) = socket.split();
            let error_msg = serde_json::json!({
                "error": "TOPIC_NOT_FOUND",
                "message": format!("Topic '{}' does not exist", topic)
            });
            let _ = sender.send(Message::Text(error_msg.to_string())).await;
            let _ = sender.close().await;
            return;
        }
    };

    if partition < 0 || partition >= metadata.num_partitions {
        let (mut sender, _) = socket.split();
        let error_msg = serde_json::json!({
            "error": "PARTITION_NOT_FOUND",
            "message": format!("Partition {} does not exist for topic '{}'", partition, topic)
        });
        let _ = sender.send(Message::Text(error_msg.to_string())).await;
        let _ = sender.close().await;
        return;
    }

    info!(
        topic = %topic,
        partition = partition,
        "Starting WebSocket subscription to partition"
    );

    let (control_tx, _control_rx) = broadcast::channel::<ControlMessage>(32);
    let (record_tx, record_rx) = mpsc::channel::<WebSocketRecord>(1000);

    let initial_offset = query.offset.unwrap_or_else(|| {
        state
            .topic_manager
            .latest_offset(&topic, partition)
            .unwrap_or(0)
    });

    let poll_interval = Duration::from_millis(query.poll_interval_ms);

    // Spawn single reader task
    let state_clone = state.clone();
    let topic_clone = topic.clone();
    let mut control_rx = control_tx.subscribe();

    let handle = tokio::spawn(async move {
        partition_reader(
            state_clone,
            topic_clone,
            partition,
            initial_offset,
            poll_interval,
            record_tx,
            &mut control_rx,
        )
        .await
    });

    // Handle the WebSocket connection
    handle_websocket_connection(socket, control_tx, record_rx).await;

    // Clean up reader task
    handle.abort();
}

/// Control message for partition readers
#[derive(Clone, Debug)]
enum ControlMessage {
    Seek(i64),
    Pause,
    Resume,
    Shutdown,
}

/// Handle the WebSocket connection, sending records and receiving control messages
async fn handle_websocket_connection(
    socket: WebSocket,
    control_tx: broadcast::Sender<ControlMessage>,
    mut record_rx: mpsc::Receiver<WebSocketRecord>,
) {
    let (mut sender, mut receiver) = socket.split();

    // Spawn task to handle incoming messages from client
    let control_tx_clone = control_tx.clone();
    let recv_task = tokio::spawn(async move {
        while let Some(result) = receiver.next().await {
            match result {
                Ok(Message::Text(text)) => match serde_json::from_str::<ClientControl>(&text) {
                    Ok(ClientControl::Seek { offset }) => {
                        debug!(offset = offset, "Client requested seek");
                        let _ = control_tx_clone.send(ControlMessage::Seek(offset));
                    }
                    Ok(ClientControl::Pause) => {
                        debug!("Client requested pause");
                        let _ = control_tx_clone.send(ControlMessage::Pause);
                    }
                    Ok(ClientControl::Resume) => {
                        debug!("Client requested resume");
                        let _ = control_tx_clone.send(ControlMessage::Resume);
                    }
                    Err(e) => {
                        warn!(error = %e, "Invalid control message from client");
                    }
                },
                Ok(Message::Close(_)) => {
                    debug!("Client closed connection");
                    break;
                }
                Ok(Message::Ping(data)) => {
                    // Pong is handled automatically by axum
                    debug!(data_len = data.len(), "Received ping");
                }
                Ok(_) => {
                    // Ignore other message types
                }
                Err(e) => {
                    error!(error = %e, "WebSocket receive error");
                    break;
                }
            }
        }
        // Signal shutdown
        let _ = control_tx_clone.send(ControlMessage::Shutdown);
    });

    // Send records to client
    let send_task = tokio::spawn(async move {
        while let Some(record) = record_rx.recv().await {
            match serde_json::to_string(&record) {
                Ok(json) => {
                    if let Err(e) = sender.send(Message::Text(json)).await {
                        error!(error = %e, "Failed to send message to WebSocket client");
                        break;
                    }
                }
                Err(e) => {
                    error!(error = %e, "Failed to serialize record");
                }
            }
        }
        let _ = sender.close().await;
    });

    // Wait for either task to complete
    tokio::select! {
        _ = recv_task => {}
        _ = send_task => {}
    }

    // Signal shutdown to any remaining tasks
    let _ = control_tx.send(ControlMessage::Shutdown);
}

/// Read records from a partition and send them to the WebSocket
async fn partition_reader(
    state: WebSocketState,
    topic: String,
    partition: i32,
    mut current_offset: i64,
    poll_interval: Duration,
    record_tx: mpsc::Sender<WebSocketRecord>,
    control_rx: &mut tokio::sync::broadcast::Receiver<ControlMessage>,
) {
    let mut paused = false;

    loop {
        // Check for control messages (non-blocking)
        match control_rx.try_recv() {
            Ok(ControlMessage::Seek(offset)) => {
                debug!(
                    topic = %topic,
                    partition = partition,
                    offset = offset,
                    "Seeking to offset"
                );
                current_offset = offset;
            }
            Ok(ControlMessage::Pause) => {
                paused = true;
            }
            Ok(ControlMessage::Resume) => {
                paused = false;
            }
            Ok(ControlMessage::Shutdown) => {
                debug!(topic = %topic, partition = partition, "Partition reader shutting down");
                break;
            }
            Err(tokio::sync::broadcast::error::TryRecvError::Empty) => {}
            Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => {}
            Err(tokio::sync::broadcast::error::TryRecvError::Closed) => {
                break;
            }
        }

        if paused {
            tokio::time::sleep(poll_interval).await;
            continue;
        }

        // Read records from partition
        match state
            .topic_manager
            .read(&topic, partition, current_offset, 100)
        {
            Ok(records) => {
                for record in records {
                    // Convert record to WebSocket format
                    let value = match serde_json::from_slice(&record.value) {
                        Ok(v) => v,
                        Err(_) => serde_json::Value::String(
                            String::from_utf8_lossy(&record.value).to_string(),
                        ),
                    };

                    let key = record.key.map(|k| String::from_utf8_lossy(&k).to_string());

                    let headers: std::collections::HashMap<String, String> = record
                        .headers
                        .into_iter()
                        .map(|h| (h.key, String::from_utf8_lossy(&h.value).to_string()))
                        .collect();

                    let ws_record = WebSocketRecord {
                        topic: topic.clone(),
                        partition,
                        offset: record.offset,
                        timestamp: record.timestamp,
                        key,
                        value,
                        headers,
                    };

                    current_offset = record.offset + 1;

                    if record_tx.send(ws_record).await.is_err() {
                        // Channel closed, client disconnected
                        return;
                    }
                }
            }
            Err(e) => {
                warn!(
                    topic = %topic,
                    partition = partition,
                    error = %e,
                    "Error reading from partition"
                );
            }
        }

        // Sleep before next poll
        tokio::time::sleep(poll_interval).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_control_deserialize() {
        let seek: ClientControl =
            serde_json::from_str(r#"{"action": "seek", "offset": 100}"#).unwrap();
        match seek {
            ClientControl::Seek { offset } => assert_eq!(offset, 100),
            _ => panic!("Expected Seek"),
        }

        let pause: ClientControl = serde_json::from_str(r#"{"action": "pause"}"#).unwrap();
        assert!(matches!(pause, ClientControl::Pause));

        let resume: ClientControl = serde_json::from_str(r#"{"action": "resume"}"#).unwrap();
        assert!(matches!(resume, ClientControl::Resume));
    }

    #[test]
    fn test_websocket_record_serialize() {
        let record = WebSocketRecord {
            topic: "test".to_string(),
            partition: 0,
            offset: 123,
            timestamp: 1702123456789,
            key: Some("key1".to_string()),
            value: serde_json::json!({"msg": "hello"}),
            headers: std::collections::HashMap::new(),
        };

        let json = serde_json::to_string(&record).unwrap();
        assert!(json.contains("\"topic\":\"test\""));
        assert!(json.contains("\"offset\":123"));
    }
}
