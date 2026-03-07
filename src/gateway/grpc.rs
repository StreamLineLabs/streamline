//! gRPC streaming adapter for the gateway.
//!
//! Provides gRPC-based produce/consume/admin operations that map to
//! Streamline topics. Clients can use standard gRPC tooling (grpcurl,
//! Postman, generated stubs) to interact with the streaming platform.
//!
//! # Service Definition (conceptual proto)
//!
//! ```proto
//! service Streamline {
//!   rpc Produce(stream ProduceRequest) returns (stream ProduceResponse);
//!   rpc Consume(ConsumeRequest) returns (stream ConsumeResponse);
//!   rpc ListTopics(Empty) returns (TopicListResponse);
//!   rpc CreateTopic(CreateTopicRequest) returns (CreateTopicResponse);
//!   rpc DeleteTopic(DeleteTopicRequest) returns (Empty);
//! }
//! ```

use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

/// gRPC adapter configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcConfig {
    /// Listen address
    #[serde(default = "default_addr")]
    pub addr: String,
    /// Maximum message size
    #[serde(default = "default_max_message_size")]
    pub max_message_size: usize,
    /// Enable server reflection
    #[serde(default = "default_true")]
    pub reflection_enabled: bool,
    /// TLS configuration
    #[serde(default)]
    pub tls_enabled: bool,
    /// Maximum concurrent streams per connection
    #[serde(default = "default_max_concurrent_streams")]
    pub max_concurrent_streams: u32,
    /// Keepalive interval in seconds
    #[serde(default = "default_keepalive")]
    pub keepalive_secs: u64,
}

fn default_addr() -> String {
    "0.0.0.0:9096".to_string()
}
fn default_max_message_size() -> usize {
    4 * 1024 * 1024
}
fn default_true() -> bool {
    true
}
fn default_max_concurrent_streams() -> u32 {
    100
}
fn default_keepalive() -> u64 {
    60
}

impl Default for GrpcConfig {
    fn default() -> Self {
        Self {
            addr: default_addr(),
            max_message_size: default_max_message_size(),
            reflection_enabled: true,
            tls_enabled: false,
            max_concurrent_streams: default_max_concurrent_streams(),
            keepalive_secs: default_keepalive(),
        }
    }
}

/// A gRPC produce request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcProduceRequest {
    pub topic: String,
    pub key: Option<String>,
    pub value: Vec<u8>,
    pub headers: HashMap<String, String>,
}

/// A gRPC produce response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcProduceResponse {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: i64,
}

/// A gRPC consume request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcConsumeRequest {
    pub topic: String,
    pub group_id: Option<String>,
    pub from_beginning: bool,
    pub max_messages: Option<u32>,
}

/// A gRPC consumed message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcConsumeResponse {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub key: Option<String>,
    pub value: Vec<u8>,
    pub timestamp: i64,
    pub headers: HashMap<String, String>,
}

/// A gRPC topic creation request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcCreateTopicRequest {
    pub name: String,
    pub partitions: u32,
    pub replication_factor: u32,
    pub config: HashMap<String, String>,
}

/// Topic info in gRPC responses.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcTopicInfo {
    pub name: String,
    pub partitions: u32,
    pub replication_factor: u32,
    pub message_count: u64,
}

/// A connected gRPC client session.
#[derive(Debug)]
pub struct GrpcSession {
    pub client_id: String,
    pub remote_addr: String,
    pub active_streams: u32,
    pub connected: bool,
    pub metadata: HashMap<String, String>,
}

/// Statistics for the gRPC adapter.
pub struct GrpcHandlerStats {
    pub connections_total: AtomicU64,
    pub connections_active: AtomicU64,
    pub produce_requests: AtomicU64,
    pub consume_requests: AtomicU64,
    pub messages_produced: AtomicU64,
    pub messages_consumed: AtomicU64,
    pub admin_requests: AtomicU64,
    pub errors: AtomicU64,
    pub bytes_in: AtomicU64,
    pub bytes_out: AtomicU64,
}

impl Default for GrpcHandlerStats {
    fn default() -> Self {
        Self {
            connections_total: AtomicU64::new(0),
            connections_active: AtomicU64::new(0),
            produce_requests: AtomicU64::new(0),
            consume_requests: AtomicU64::new(0),
            messages_produced: AtomicU64::new(0),
            messages_consumed: AtomicU64::new(0),
            admin_requests: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            bytes_in: AtomicU64::new(0),
            bytes_out: AtomicU64::new(0),
        }
    }
}

/// Snapshot of gRPC handler statistics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GrpcHandlerStatsSnapshot {
    pub connections_total: u64,
    pub connections_active: u64,
    pub produce_requests: u64,
    pub consume_requests: u64,
    pub messages_produced: u64,
    pub messages_consumed: u64,
    pub admin_requests: u64,
    pub errors: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
}

impl GrpcHandlerStats {
    pub fn snapshot(&self) -> GrpcHandlerStatsSnapshot {
        GrpcHandlerStatsSnapshot {
            connections_total: self.connections_total.load(Ordering::Relaxed),
            connections_active: self.connections_active.load(Ordering::Relaxed),
            produce_requests: self.produce_requests.load(Ordering::Relaxed),
            consume_requests: self.consume_requests.load(Ordering::Relaxed),
            messages_produced: self.messages_produced.load(Ordering::Relaxed),
            messages_consumed: self.messages_consumed.load(Ordering::Relaxed),
            admin_requests: self.admin_requests.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            bytes_in: self.bytes_in.load(Ordering::Relaxed),
            bytes_out: self.bytes_out.load(Ordering::Relaxed),
        }
    }
}

/// gRPC streaming adapter with session management.
///
/// Exposes a bidirectional streaming RPC service:
/// - `Produce(stream ProduceRequest) returns (stream ProduceResponse)`
/// - `Consume(ConsumeRequest) returns (stream ConsumeResponse)`
/// - `ListTopics(Empty) returns (TopicList)`
pub struct GrpcAdapter {
    config: GrpcConfig,
    sessions: Arc<RwLock<HashMap<String, GrpcSession>>>,
    stats: Arc<GrpcHandlerStats>,
}

impl GrpcAdapter {
    /// Create a new gRPC adapter.
    pub fn new(config: GrpcConfig) -> Self {
        Self {
            config,
            sessions: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(GrpcHandlerStats::default()),
        }
    }

    /// Get the listen address.
    pub fn addr(&self) -> &str {
        &self.config.addr
    }

    /// Get configuration.
    pub fn config(&self) -> &GrpcConfig {
        &self.config
    }

    /// Register a new gRPC client connection.
    pub async fn handle_connect(&self, client_id: &str, remote_addr: &str) {
        let session = GrpcSession {
            client_id: client_id.to_string(),
            remote_addr: remote_addr.to_string(),
            active_streams: 0,
            connected: true,
            metadata: HashMap::new(),
        };

        self.sessions
            .write()
            .await
            .insert(client_id.to_string(), session);

        self.stats.connections_total.fetch_add(1, Ordering::Relaxed);
        self.stats.connections_active.fetch_add(1, Ordering::Relaxed);

        info!(client_id, remote_addr, "gRPC client connected");
    }

    /// Handle a produce request. Returns the Streamline topic and validates the message.
    pub async fn handle_produce(
        &self,
        request: &GrpcProduceRequest,
    ) -> Result<String> {
        if request.topic.is_empty() {
            self.stats.errors.fetch_add(1, Ordering::Relaxed);
            return Err(StreamlineError::Gateway("topic is required".into()));
        }

        if request.value.len() > self.config.max_message_size {
            self.stats.errors.fetch_add(1, Ordering::Relaxed);
            return Err(StreamlineError::Gateway(format!(
                "message size {} exceeds maximum {}",
                request.value.len(),
                self.config.max_message_size
            )));
        }

        let topic = self.sanitize_topic(&request.topic);

        self.stats.produce_requests.fetch_add(1, Ordering::Relaxed);
        self.stats.messages_produced.fetch_add(1, Ordering::Relaxed);
        self.stats
            .bytes_in
            .fetch_add(request.value.len() as u64, Ordering::Relaxed);

        debug!(
            topic = request.topic,
            streamline_topic = topic,
            payload_len = request.value.len(),
            "gRPC Produce → Streamline produce"
        );

        Ok(topic)
    }

    /// Handle a consume request. Returns the Streamline topic to consume from.
    pub async fn handle_consume(
        &self,
        request: &GrpcConsumeRequest,
    ) -> Result<String> {
        if request.topic.is_empty() {
            self.stats.errors.fetch_add(1, Ordering::Relaxed);
            return Err(StreamlineError::Gateway("topic is required".into()));
        }

        let topic = self.sanitize_topic(&request.topic);
        self.stats.consume_requests.fetch_add(1, Ordering::Relaxed);

        debug!(
            topic = request.topic,
            streamline_topic = topic,
            group_id = ?request.group_id,
            "gRPC Consume → Streamline consume"
        );

        Ok(topic)
    }

    /// Handle a disconnect.
    pub async fn handle_disconnect(&self, client_id: &str) {
        let mut sessions = self.sessions.write().await;
        if let Some(session) = sessions.get_mut(client_id) {
            session.connected = false;
            self.stats.connections_active.fetch_sub(1, Ordering::Relaxed);
            info!(client_id, "gRPC client disconnected");
        }
        sessions.remove(client_id);
    }

    /// Get handler statistics.
    pub fn stats(&self) -> GrpcHandlerStatsSnapshot {
        self.stats.snapshot()
    }

    /// Get active session count.
    pub async fn active_sessions(&self) -> usize {
        self.sessions
            .read()
            .await
            .values()
            .filter(|s| s.connected)
            .count()
    }

    fn sanitize_topic(&self, topic: &str) -> String {
        topic
            .chars()
            .filter(|c| c.is_alphanumeric() || *c == '-' || *c == '_' || *c == '.')
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_grpc_defaults() {
        let adapter = GrpcAdapter::new(GrpcConfig::default());
        assert_eq!(adapter.addr(), "0.0.0.0:9096");
        assert!(adapter.config().reflection_enabled);
        assert_eq!(adapter.config().max_concurrent_streams, 100);
    }

    #[test]
    fn test_grpc_config_serialization() {
        let config = GrpcConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        assert!(json.contains("\"addr\":\"0.0.0.0:9096\""));
        let deser: GrpcConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.addr, config.addr);
    }

    #[test]
    fn test_produce_request_serialization() {
        let req = GrpcProduceRequest {
            topic: "orders".to_string(),
            key: Some("user-1".to_string()),
            value: b"hello".to_vec(),
            headers: HashMap::from([("trace".to_string(), "abc".to_string())]),
        };
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"topic\":\"orders\""));
    }

    #[tokio::test]
    async fn test_connect_disconnect() {
        let adapter = GrpcAdapter::new(GrpcConfig::default());
        adapter.handle_connect("client-1", "127.0.0.1:50000").await;
        assert_eq!(adapter.active_sessions().await, 1);

        adapter.handle_disconnect("client-1").await;
        assert_eq!(adapter.active_sessions().await, 0);
    }

    #[tokio::test]
    async fn test_handle_produce_success() {
        let adapter = GrpcAdapter::new(GrpcConfig::default());
        let req = GrpcProduceRequest {
            topic: "orders".to_string(),
            key: None,
            value: b"data".to_vec(),
            headers: HashMap::new(),
        };
        let topic = adapter.handle_produce(&req).await.unwrap();
        assert_eq!(topic, "orders");

        let stats = adapter.stats();
        assert_eq!(stats.produce_requests, 1);
        assert_eq!(stats.messages_produced, 1);
    }

    #[tokio::test]
    async fn test_handle_produce_empty_topic() {
        let adapter = GrpcAdapter::new(GrpcConfig::default());
        let req = GrpcProduceRequest {
            topic: "".to_string(),
            key: None,
            value: b"data".to_vec(),
            headers: HashMap::new(),
        };
        assert!(adapter.handle_produce(&req).await.is_err());
    }

    #[tokio::test]
    async fn test_handle_produce_too_large() {
        let config = GrpcConfig {
            max_message_size: 10,
            ..Default::default()
        };
        let adapter = GrpcAdapter::new(config);
        let req = GrpcProduceRequest {
            topic: "t".to_string(),
            key: None,
            value: vec![0u8; 100],
            headers: HashMap::new(),
        };
        let err = adapter.handle_produce(&req).await.unwrap_err();
        assert!(err.to_string().contains("exceeds maximum"));
    }

    #[tokio::test]
    async fn test_handle_consume_success() {
        let adapter = GrpcAdapter::new(GrpcConfig::default());
        let req = GrpcConsumeRequest {
            topic: "events".to_string(),
            group_id: Some("grp-1".to_string()),
            from_beginning: true,
            max_messages: None,
        };
        let topic = adapter.handle_consume(&req).await.unwrap();
        assert_eq!(topic, "events");
    }

    #[tokio::test]
    async fn test_handle_consume_empty_topic() {
        let adapter = GrpcAdapter::new(GrpcConfig::default());
        let req = GrpcConsumeRequest {
            topic: "".to_string(),
            group_id: None,
            from_beginning: false,
            max_messages: None,
        };
        assert!(adapter.handle_consume(&req).await.is_err());
    }

    #[tokio::test]
    async fn test_stats_tracking() {
        let adapter = GrpcAdapter::new(GrpcConfig::default());
        adapter.handle_connect("c1", "1.2.3.4:5000").await;
        adapter.handle_connect("c2", "1.2.3.4:5001").await;

        let stats = adapter.stats();
        assert_eq!(stats.connections_total, 2);
        assert_eq!(stats.connections_active, 2);

        adapter.handle_disconnect("c1").await;
        let stats = adapter.stats();
        assert_eq!(stats.connections_active, 1);
    }
}
