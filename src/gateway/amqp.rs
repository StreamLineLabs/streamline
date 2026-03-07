//! AMQP 0-9-1 protocol adapter for the gateway.
//!
//! Maps AMQP concepts to Streamline:
//! - AMQP Exchange → Streamline topic (exchange name = topic name)
//! - AMQP Queue → Consumer group subscription
//! - AMQP Routing Key → Message key
//! - AMQP Channel → Multiplexed session
//!
//! # Protocol Mapping
//!
//! ```text
//! AMQP                    Streamline
//! ─────────────────────   ─────────────────
//! Exchange "orders"    →  Topic "orders"
//! Queue "order-proc"   →  Consumer group "amqp-order-proc"
//! Routing key "us"     →  Message key "us"
//! Basic.Publish        →  Produce
//! Basic.Consume        →  Subscribe + Consume
//! Basic.Ack            →  Offset commit
//! ```

use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

/// AMQP adapter configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AmqpConfig {
    /// Listen address
    #[serde(default = "default_addr")]
    pub addr: String,
    /// Maximum frame size
    #[serde(default = "default_frame_size")]
    pub max_frame_size: usize,
    /// Channel limit
    #[serde(default = "default_channel_max")]
    pub channel_max: u16,
    /// Idle timeout in seconds
    #[serde(default = "default_idle_timeout")]
    pub idle_timeout_secs: u64,
    /// Heartbeat interval in seconds
    #[serde(default = "default_heartbeat")]
    pub heartbeat_secs: u16,
    /// Virtual host support
    #[serde(default = "default_vhost")]
    pub default_vhost: String,
}

fn default_addr() -> String {
    "0.0.0.0:5672".to_string()
}
fn default_frame_size() -> usize {
    1024 * 1024
}
fn default_channel_max() -> u16 {
    256
}
fn default_idle_timeout() -> u64 {
    120
}
fn default_heartbeat() -> u16 {
    60
}
fn default_vhost() -> String {
    "/".to_string()
}

impl Default for AmqpConfig {
    fn default() -> Self {
        Self {
            addr: default_addr(),
            max_frame_size: default_frame_size(),
            channel_max: default_channel_max(),
            idle_timeout_secs: default_idle_timeout(),
            heartbeat_secs: default_heartbeat(),
            default_vhost: default_vhost(),
        }
    }
}

/// AMQP exchange type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExchangeType {
    Direct,
    Fanout,
    Topic,
    Headers,
}

/// An AMQP exchange declaration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AmqpExchange {
    pub name: String,
    pub exchange_type: ExchangeType,
    pub durable: bool,
    pub auto_delete: bool,
    pub arguments: HashMap<String, String>,
}

/// An AMQP queue declaration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AmqpQueue {
    pub name: String,
    pub durable: bool,
    pub exclusive: bool,
    pub auto_delete: bool,
    pub arguments: HashMap<String, String>,
}

/// A binding between an exchange and a queue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AmqpBinding {
    pub exchange: String,
    pub queue: String,
    pub routing_key: String,
}

/// An AMQP publish message.
#[derive(Debug, Clone)]
pub struct AmqpPublishData {
    pub exchange: String,
    pub routing_key: String,
    pub mandatory: bool,
    pub immediate: bool,
    pub body: Vec<u8>,
    pub properties: AmqpMessageProperties,
}

/// AMQP message properties (Basic.Properties).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AmqpMessageProperties {
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub correlation_id: Option<String>,
    pub reply_to: Option<String>,
    pub message_id: Option<String>,
    pub timestamp: Option<u64>,
    pub headers: HashMap<String, String>,
}

/// An AMQP connection session.
#[derive(Debug)]
pub struct AmqpSession {
    pub connection_id: String,
    pub vhost: String,
    pub channels: HashMap<u16, AmqpChannel>,
    pub connected: bool,
    pub username: Option<String>,
}

/// An AMQP channel within a connection.
#[derive(Debug)]
pub struct AmqpChannel {
    pub id: u16,
    pub open: bool,
    pub consuming: HashSet<String>,
    pub prefetch_count: u16,
    pub unacked_deliveries: u64,
}

impl AmqpChannel {
    pub fn new(id: u16) -> Self {
        Self {
            id,
            open: true,
            consuming: HashSet::new(),
            prefetch_count: 0,
            unacked_deliveries: 0,
        }
    }
}

/// Statistics for the AMQP handler.
pub struct AmqpHandlerStats {
    pub connections_total: AtomicU64,
    pub connections_active: AtomicU64,
    pub channels_opened: AtomicU64,
    pub channels_active: AtomicU64,
    pub messages_published: AtomicU64,
    pub messages_delivered: AtomicU64,
    pub messages_acked: AtomicU64,
    pub exchanges_declared: AtomicU64,
    pub queues_declared: AtomicU64,
    pub bindings_created: AtomicU64,
    pub bytes_in: AtomicU64,
    pub bytes_out: AtomicU64,
}

impl Default for AmqpHandlerStats {
    fn default() -> Self {
        Self {
            connections_total: AtomicU64::new(0),
            connections_active: AtomicU64::new(0),
            channels_opened: AtomicU64::new(0),
            channels_active: AtomicU64::new(0),
            messages_published: AtomicU64::new(0),
            messages_delivered: AtomicU64::new(0),
            messages_acked: AtomicU64::new(0),
            exchanges_declared: AtomicU64::new(0),
            queues_declared: AtomicU64::new(0),
            bindings_created: AtomicU64::new(0),
            bytes_in: AtomicU64::new(0),
            bytes_out: AtomicU64::new(0),
        }
    }
}

/// Snapshot of AMQP handler statistics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AmqpHandlerStatsSnapshot {
    pub connections_total: u64,
    pub connections_active: u64,
    pub channels_opened: u64,
    pub channels_active: u64,
    pub messages_published: u64,
    pub messages_delivered: u64,
    pub messages_acked: u64,
    pub exchanges_declared: u64,
    pub queues_declared: u64,
    pub bindings_created: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
}

impl AmqpHandlerStats {
    pub fn snapshot(&self) -> AmqpHandlerStatsSnapshot {
        AmqpHandlerStatsSnapshot {
            connections_total: self.connections_total.load(Ordering::Relaxed),
            connections_active: self.connections_active.load(Ordering::Relaxed),
            channels_opened: self.channels_opened.load(Ordering::Relaxed),
            channels_active: self.channels_active.load(Ordering::Relaxed),
            messages_published: self.messages_published.load(Ordering::Relaxed),
            messages_delivered: self.messages_delivered.load(Ordering::Relaxed),
            messages_acked: self.messages_acked.load(Ordering::Relaxed),
            exchanges_declared: self.exchanges_declared.load(Ordering::Relaxed),
            queues_declared: self.queues_declared.load(Ordering::Relaxed),
            bindings_created: self.bindings_created.load(Ordering::Relaxed),
            bytes_in: self.bytes_in.load(Ordering::Relaxed),
            bytes_out: self.bytes_out.load(Ordering::Relaxed),
        }
    }
}

/// AMQP protocol adapter with connection and channel management.
pub struct AmqpAdapter {
    config: AmqpConfig,
    sessions: Arc<RwLock<HashMap<String, AmqpSession>>>,
    exchanges: Arc<RwLock<HashMap<String, AmqpExchange>>>,
    queues: Arc<RwLock<HashMap<String, AmqpQueue>>>,
    bindings: Arc<RwLock<Vec<AmqpBinding>>>,
    stats: Arc<AmqpHandlerStats>,
}

impl AmqpAdapter {
    /// Create a new AMQP adapter.
    pub fn new(config: AmqpConfig) -> Self {
        Self {
            config,
            sessions: Arc::new(RwLock::new(HashMap::new())),
            exchanges: Arc::new(RwLock::new(HashMap::new())),
            queues: Arc::new(RwLock::new(HashMap::new())),
            bindings: Arc::new(RwLock::new(Vec::new())),
            stats: Arc::new(AmqpHandlerStats::default()),
        }
    }

    /// Get the listen address.
    pub fn addr(&self) -> &str {
        &self.config.addr
    }

    /// Get configuration.
    pub fn config(&self) -> &AmqpConfig {
        &self.config
    }

    /// Handle Connection.Open — register a new AMQP connection.
    pub async fn handle_connect(
        &self,
        connection_id: &str,
        vhost: &str,
        username: Option<&str>,
    ) -> Result<()> {
        if vhost != self.config.default_vhost && vhost != "/" {
            return Err(StreamlineError::Gateway(format!("vhost '{}' not found", vhost)));
        }

        let session = AmqpSession {
            connection_id: connection_id.to_string(),
            vhost: vhost.to_string(),
            channels: HashMap::new(),
            connected: true,
            username: username.map(|s| s.to_string()),
        };

        self.sessions
            .write()
            .await
            .insert(connection_id.to_string(), session);

        self.stats.connections_total.fetch_add(1, Ordering::Relaxed);
        self.stats.connections_active.fetch_add(1, Ordering::Relaxed);

        info!(connection_id, vhost, "AMQP connection opened");
        Ok(())
    }

    /// Handle Channel.Open — open a channel on an existing connection.
    pub async fn handle_channel_open(
        &self,
        connection_id: &str,
        channel_id: u16,
    ) -> Result<()> {
        let mut sessions = self.sessions.write().await;
        let session = sessions
            .get_mut(connection_id)
            .ok_or_else(|| StreamlineError::Gateway("Connection not found".into()))?;

        if channel_id == 0 {
            return Err(StreamlineError::Gateway("Channel 0 is reserved".into()));
        }

        if session.channels.len() >= self.config.channel_max as usize {
            return Err(StreamlineError::Gateway(format!(
                "Channel limit {} reached",
                self.config.channel_max
            )));
        }

        session.channels.insert(channel_id, AmqpChannel::new(channel_id));

        self.stats.channels_opened.fetch_add(1, Ordering::Relaxed);
        self.stats.channels_active.fetch_add(1, Ordering::Relaxed);

        debug!(connection_id, channel_id, "AMQP channel opened");
        Ok(())
    }

    /// Handle Exchange.Declare.
    pub async fn handle_exchange_declare(&self, exchange: AmqpExchange) -> Result<()> {
        if exchange.name.starts_with("amq.") {
            return Err(StreamlineError::Gateway("Cannot redeclare reserved exchange".into()));
        }

        let existing = self.exchanges.read().await;
        if let Some(existing) = existing.get(&exchange.name) {
            if existing.exchange_type != exchange.exchange_type {
                return Err(StreamlineError::Gateway(format!(
                    "Exchange '{}' already declared with type {:?}",
                    exchange.name, existing.exchange_type
                )));
            }
            return Ok(());
        }
        drop(existing);

        debug!(name = exchange.name, "AMQP exchange declared");
        self.exchanges
            .write()
            .await
            .insert(exchange.name.clone(), exchange);

        self.stats.exchanges_declared.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Handle Queue.Declare.
    pub async fn handle_queue_declare(&self, queue: AmqpQueue) -> Result<()> {
        debug!(name = queue.name, "AMQP queue declared");
        self.queues
            .write()
            .await
            .insert(queue.name.clone(), queue);

        self.stats.queues_declared.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Handle Queue.Bind.
    pub async fn handle_bind(&self, binding: AmqpBinding) -> Result<()> {
        // Hold both read locks during validation to prevent TOCTOU race
        let exchanges = self.exchanges.read().await;
        if !exchanges.contains_key(&binding.exchange) && !binding.exchange.starts_with("amq.") {
            return Err(StreamlineError::Gateway(format!("Exchange '{}' not found", binding.exchange)));
        }

        let queues = self.queues.read().await;
        if !queues.contains_key(&binding.queue) {
            return Err(StreamlineError::Gateway(format!("Queue '{}' not found", binding.queue)));
        }
        // Drop read locks before acquiring write lock
        drop(queues);
        drop(exchanges);

        debug!(
            exchange = binding.exchange,
            queue = binding.queue,
            routing_key = binding.routing_key,
            "AMQP queue bound"
        );
        self.bindings.write().await.push(binding);
        self.stats.bindings_created.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Handle Basic.Publish — returns the Streamline topic to produce to.
    pub async fn handle_publish(
        &self,
        publish: &AmqpPublishData,
    ) -> Result<String> {
        if publish.body.len() > self.config.max_frame_size {
            return Err(StreamlineError::Gateway(format!(
                "Message size {} exceeds frame size {}",
                publish.body.len(),
                self.config.max_frame_size
            )));
        }

        let topic = self.map_exchange(&publish.exchange);

        self.stats.messages_published.fetch_add(1, Ordering::Relaxed);
        self.stats
            .bytes_in
            .fetch_add(publish.body.len() as u64, Ordering::Relaxed);

        debug!(
            exchange = publish.exchange,
            routing_key = publish.routing_key,
            streamline_topic = topic,
            "AMQP Basic.Publish → Streamline produce"
        );

        Ok(topic)
    }

    /// Handle Connection.Close.
    pub async fn handle_disconnect(&self, connection_id: &str) {
        let mut sessions = self.sessions.write().await;
        if let Some(session) = sessions.remove(connection_id) {
            let channel_count = session.channels.len() as u64;
            self.stats.connections_active.fetch_sub(1, Ordering::Relaxed);
            self.stats
                .channels_active
                .fetch_sub(channel_count, Ordering::Relaxed);
            info!(connection_id, "AMQP connection closed");
        }
    }

    /// Map an AMQP exchange name to a Streamline topic.
    pub fn map_exchange(&self, exchange: &str) -> String {
        if exchange.is_empty() || exchange == "amq.direct" {
            "default".to_string()
        } else {
            exchange
                .replace('.', "-")
                .chars()
                .filter(|c| c.is_alphanumeric() || *c == '-' || *c == '_')
                .collect()
        }
    }

    /// Map an AMQP queue to a consumer group ID.
    pub fn map_queue_to_group(&self, queue: &str) -> String {
        format!("amqp-{}", queue)
    }

    /// Get handler statistics.
    pub fn stats(&self) -> AmqpHandlerStatsSnapshot {
        self.stats.snapshot()
    }

    /// Get active connection count.
    pub async fn active_connections(&self) -> usize {
        self.sessions.read().await.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exchange_mapping() {
        let adapter = AmqpAdapter::new(AmqpConfig::default());
        assert_eq!(adapter.map_exchange("orders.events"), "orders-events");
        assert_eq!(adapter.map_exchange(""), "default");
        assert_eq!(adapter.map_exchange("amq.direct"), "default");
    }

    #[test]
    fn test_queue_to_group() {
        let adapter = AmqpAdapter::new(AmqpConfig::default());
        assert_eq!(
            adapter.map_queue_to_group("order-processor"),
            "amqp-order-processor"
        );
    }

    #[test]
    fn test_amqp_config_defaults() {
        let config = AmqpConfig::default();
        assert_eq!(config.addr, "0.0.0.0:5672");
        assert_eq!(config.channel_max, 256);
        assert_eq!(config.heartbeat_secs, 60);
        assert_eq!(config.default_vhost, "/");
    }

    #[test]
    fn test_amqp_config_serialization() {
        let config = AmqpConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let deser: AmqpConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.addr, config.addr);
    }

    #[test]
    fn test_amqp_channel_new() {
        let ch = AmqpChannel::new(1);
        assert_eq!(ch.id, 1);
        assert!(ch.open);
        assert!(ch.consuming.is_empty());
    }

    #[tokio::test]
    async fn test_connect_disconnect() {
        let adapter = AmqpAdapter::new(AmqpConfig::default());
        adapter.handle_connect("conn-1", "/", Some("admin")).await.unwrap();
        assert_eq!(adapter.active_connections().await, 1);

        adapter.handle_disconnect("conn-1").await;
        assert_eq!(adapter.active_connections().await, 0);
    }

    #[tokio::test]
    async fn test_connect_invalid_vhost() {
        let adapter = AmqpAdapter::new(AmqpConfig::default());
        let result = adapter.handle_connect("conn-1", "nonexistent", None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_channel_open() {
        let adapter = AmqpAdapter::new(AmqpConfig::default());
        adapter.handle_connect("conn-1", "/", None).await.unwrap();
        adapter.handle_channel_open("conn-1", 1).await.unwrap();

        let stats = adapter.stats();
        assert_eq!(stats.channels_opened, 1);
        assert_eq!(stats.channels_active, 1);
    }

    #[tokio::test]
    async fn test_channel_zero_reserved() {
        let adapter = AmqpAdapter::new(AmqpConfig::default());
        adapter.handle_connect("conn-1", "/", None).await.unwrap();
        let result = adapter.handle_channel_open("conn-1", 0).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_exchange_declare() {
        let adapter = AmqpAdapter::new(AmqpConfig::default());
        let exchange = AmqpExchange {
            name: "orders".to_string(),
            exchange_type: ExchangeType::Direct,
            durable: true,
            auto_delete: false,
            arguments: HashMap::new(),
        };
        adapter.handle_exchange_declare(exchange).await.unwrap();
        assert_eq!(adapter.stats().exchanges_declared, 1);
    }

    #[tokio::test]
    async fn test_exchange_declare_reserved() {
        let adapter = AmqpAdapter::new(AmqpConfig::default());
        let exchange = AmqpExchange {
            name: "amq.topic".to_string(),
            exchange_type: ExchangeType::Topic,
            durable: true,
            auto_delete: false,
            arguments: HashMap::new(),
        };
        assert!(adapter.handle_exchange_declare(exchange).await.is_err());
    }

    #[tokio::test]
    async fn test_queue_declare_and_bind() {
        let adapter = AmqpAdapter::new(AmqpConfig::default());

        let exchange = AmqpExchange {
            name: "orders".to_string(),
            exchange_type: ExchangeType::Direct,
            durable: true,
            auto_delete: false,
            arguments: HashMap::new(),
        };
        adapter.handle_exchange_declare(exchange).await.unwrap();

        let queue = AmqpQueue {
            name: "order-processor".to_string(),
            durable: true,
            exclusive: false,
            auto_delete: false,
            arguments: HashMap::new(),
        };
        adapter.handle_queue_declare(queue).await.unwrap();

        let binding = AmqpBinding {
            exchange: "orders".to_string(),
            queue: "order-processor".to_string(),
            routing_key: "us".to_string(),
        };
        adapter.handle_bind(binding).await.unwrap();

        let stats = adapter.stats();
        assert_eq!(stats.exchanges_declared, 1);
        assert_eq!(stats.queues_declared, 1);
        assert_eq!(stats.bindings_created, 1);
    }

    #[tokio::test]
    async fn test_publish() {
        let adapter = AmqpAdapter::new(AmqpConfig::default());
        let publish = AmqpPublishData {
            exchange: "orders".to_string(),
            routing_key: "us".to_string(),
            mandatory: false,
            immediate: false,
            body: b"order data".to_vec(),
            properties: AmqpMessageProperties::default(),
        };
        let topic = adapter.handle_publish(&publish).await.unwrap();
        assert_eq!(topic, "orders");
        assert_eq!(adapter.stats().messages_published, 1);
    }

    #[tokio::test]
    async fn test_publish_too_large() {
        let config = AmqpConfig {
            max_frame_size: 10,
            ..Default::default()
        };
        let adapter = AmqpAdapter::new(config);
        let publish = AmqpPublishData {
            exchange: "t".to_string(),
            routing_key: "".to_string(),
            mandatory: false,
            immediate: false,
            body: vec![0u8; 100],
            properties: AmqpMessageProperties::default(),
        };
        assert!(adapter.handle_publish(&publish).await.is_err());
    }
}
