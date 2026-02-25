//! MQTT Connection Handler
//!
//! Handles MQTT 3.1.1 packet parsing and session management for the
//! protocol gateway. Maps MQTT publish/subscribe operations to
//! Streamline produce/consume operations.

use crate::error::{Result, StreamlineError};
use crate::gateway::mqtt::{MqttConfig, QoSLevel};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// MQTT packet types (subset of MQTT 3.1.1 spec)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MqttPacketType {
    Connect = 1,
    ConnAck = 2,
    Publish = 3,
    PubAck = 4,
    Subscribe = 8,
    SubAck = 9,
    Unsubscribe = 10,
    UnsubAck = 11,
    PingReq = 12,
    PingResp = 13,
    Disconnect = 14,
}

impl MqttPacketType {
    pub fn from_byte(byte: u8) -> Option<Self> {
        match byte >> 4 {
            1 => Some(Self::Connect),
            2 => Some(Self::ConnAck),
            3 => Some(Self::Publish),
            4 => Some(Self::PubAck),
            8 => Some(Self::Subscribe),
            9 => Some(Self::SubAck),
            10 => Some(Self::Unsubscribe),
            11 => Some(Self::UnsubAck),
            12 => Some(Self::PingReq),
            13 => Some(Self::PingResp),
            14 => Some(Self::Disconnect),
            _ => None,
        }
    }
}

/// MQTT CONNECT packet data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MqttConnectData {
    pub client_id: String,
    pub clean_session: bool,
    pub keep_alive_secs: u16,
    pub username: Option<String>,
    pub will_topic: Option<String>,
    pub will_message: Option<Vec<u8>>,
    pub will_qos: u8,
    pub will_retain: bool,
}

/// MQTT PUBLISH packet data
#[derive(Debug, Clone)]
pub struct MqttPublishData {
    pub topic: String,
    pub qos: QoSLevel,
    pub retain: bool,
    pub packet_id: Option<u16>,
    pub payload: Vec<u8>,
}

/// MQTT SUBSCRIBE request
#[derive(Debug, Clone)]
pub struct MqttSubscription {
    pub topic_filter: String,
    pub qos: QoSLevel,
}

/// ConnAck return codes
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum ConnAckCode {
    Accepted = 0,
    UnacceptableProtocol = 1,
    IdentifierRejected = 2,
    ServerUnavailable = 3,
    BadCredentials = 4,
    NotAuthorized = 5,
}

/// An MQTT client session
#[derive(Debug)]
pub struct MqttSession {
    /// Client identifier
    pub client_id: String,
    /// Whether this is a clean session
    pub clean_session: bool,
    /// Keep-alive interval
    pub keep_alive_secs: u16,
    /// Active subscriptions (topic filter → QoS)
    pub subscriptions: HashMap<String, QoSLevel>,
    /// Next packet ID for QoS > 0
    pub next_packet_id: u16,
    /// Whether the session is connected
    pub connected: bool,
}

impl MqttSession {
    pub fn new(connect: &MqttConnectData) -> Self {
        Self {
            client_id: connect.client_id.clone(),
            clean_session: connect.clean_session,
            keep_alive_secs: connect.keep_alive_secs,
            subscriptions: HashMap::new(),
            next_packet_id: 1,
            connected: true,
        }
    }

    pub fn next_packet_id(&mut self) -> u16 {
        let id = self.next_packet_id;
        self.next_packet_id = self.next_packet_id.wrapping_add(1);
        if self.next_packet_id == 0 {
            self.next_packet_id = 1;
        }
        id
    }
}

/// MQTT session manager
pub struct MqttSessionManager {
    config: MqttConfig,
    sessions: Arc<RwLock<HashMap<String, MqttSession>>>,
    stats: Arc<MqttHandlerStats>,
}

/// Statistics for the MQTT handler
pub struct MqttHandlerStats {
    pub connections_total: AtomicU64,
    pub connections_active: AtomicU64,
    pub messages_received: AtomicU64,
    pub messages_published: AtomicU64,
    pub subscriptions_active: AtomicU64,
    pub bytes_received: AtomicU64,
    pub bytes_sent: AtomicU64,
}

impl Default for MqttHandlerStats {
    fn default() -> Self {
        Self {
            connections_total: AtomicU64::new(0),
            connections_active: AtomicU64::new(0),
            messages_received: AtomicU64::new(0),
            messages_published: AtomicU64::new(0),
            subscriptions_active: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
        }
    }
}

/// Snapshot of MQTT handler statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MqttHandlerStatsSnapshot {
    pub connections_total: u64,
    pub connections_active: u64,
    pub messages_received: u64,
    pub messages_published: u64,
    pub subscriptions_active: u64,
    pub bytes_received: u64,
    pub bytes_sent: u64,
}

impl MqttHandlerStats {
    pub fn snapshot(&self) -> MqttHandlerStatsSnapshot {
        MqttHandlerStatsSnapshot {
            connections_total: self.connections_total.load(Ordering::Relaxed),
            connections_active: self.connections_active.load(Ordering::Relaxed),
            messages_received: self.messages_received.load(Ordering::Relaxed),
            messages_published: self.messages_published.load(Ordering::Relaxed),
            subscriptions_active: self.subscriptions_active.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
        }
    }
}

impl MqttSessionManager {
    /// Create a new MQTT session manager
    pub fn new(config: MqttConfig) -> Self {
        Self {
            config,
            sessions: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(MqttHandlerStats::default()),
        }
    }

    /// Handle a CONNECT packet
    pub async fn handle_connect(
        &self,
        connect: MqttConnectData,
    ) -> Result<(ConnAckCode, bool)> {
        // Validate client ID
        if connect.client_id.is_empty() && !connect.clean_session {
            return Ok((ConnAckCode::IdentifierRejected, false));
        }

        // Validate credentials if required
        if !self.config.allow_anonymous && connect.username.is_none() {
            return Ok((ConnAckCode::NotAuthorized, false));
        }

        let mut sessions = self.sessions.write().await;

        // Check for existing session
        let session_present = if connect.clean_session {
            sessions.remove(&connect.client_id);
            false
        } else {
            sessions.contains_key(&connect.client_id)
        };

        // Create or update session
        let session = MqttSession::new(&connect);
        sessions.insert(connect.client_id.clone(), session);

        self.stats.connections_total.fetch_add(1, Ordering::Relaxed);
        self.stats.connections_active.fetch_add(1, Ordering::Relaxed);

        info!(
            client_id = connect.client_id,
            clean_session = connect.clean_session,
            "MQTT client connected"
        );

        Ok((ConnAckCode::Accepted, session_present))
    }

    /// Handle a PUBLISH packet — returns the Streamline topic to produce to
    pub async fn handle_publish(
        &self,
        client_id: &str,
        publish: &MqttPublishData,
    ) -> Result<String> {
        let sessions = self.sessions.read().await;
        if !sessions.contains_key(client_id) {
            return Err(StreamlineError::Protocol(
                "Client not connected".to_string(),
            ));
        }

        // Validate packet size
        if publish.payload.len() > self.config.max_packet_size {
            return Err(StreamlineError::Protocol(format!(
                "Packet size {} exceeds maximum {}",
                publish.payload.len(),
                self.config.max_packet_size
            )));
        }

        // Map MQTT topic to Streamline topic
        let streamline_topic = publish
            .topic
            .replace('/', "-")
            .replace('+', "_single")
            .replace('#', "_multi");

        self.stats.messages_received.fetch_add(1, Ordering::Relaxed);
        self.stats
            .bytes_received
            .fetch_add(publish.payload.len() as u64, Ordering::Relaxed);

        debug!(
            client_id,
            mqtt_topic = publish.topic,
            streamline_topic,
            payload_len = publish.payload.len(),
            "MQTT PUBLISH → Streamline produce"
        );

        Ok(streamline_topic)
    }

    /// Handle SUBSCRIBE — returns granted QoS levels
    pub async fn handle_subscribe(
        &self,
        client_id: &str,
        subscriptions: &[MqttSubscription],
    ) -> Result<Vec<QoSLevel>> {
        let mut sessions = self.sessions.write().await;
        let session = sessions.get_mut(client_id).ok_or_else(|| {
            StreamlineError::Protocol("Client not connected".to_string())
        })?;

        let mut granted = Vec::with_capacity(subscriptions.len());

        for sub in subscriptions {
            // Cap QoS at server's maximum
            let granted_qos = if (sub.qos as u8) <= self.config.max_qos {
                sub.qos
            } else {
                match self.config.max_qos {
                    0 => QoSLevel::AtMostOnce,
                    1 => QoSLevel::AtLeastOnce,
                    _ => QoSLevel::ExactlyOnce,
                }
            };

            session
                .subscriptions
                .insert(sub.topic_filter.clone(), granted_qos);
            granted.push(granted_qos);

            self.stats
                .subscriptions_active
                .fetch_add(1, Ordering::Relaxed);

            debug!(
                client_id,
                topic_filter = sub.topic_filter,
                granted_qos = ?granted_qos,
                "MQTT SUBSCRIBE"
            );
        }

        Ok(granted)
    }

    /// Handle UNSUBSCRIBE
    pub async fn handle_unsubscribe(
        &self,
        client_id: &str,
        topic_filters: &[String],
    ) -> Result<()> {
        let mut sessions = self.sessions.write().await;
        let session = sessions.get_mut(client_id).ok_or_else(|| {
            StreamlineError::Protocol("Client not connected".to_string())
        })?;

        for filter in topic_filters {
            if session.subscriptions.remove(filter).is_some() {
                self.stats
                    .subscriptions_active
                    .fetch_sub(1, Ordering::Relaxed);
            }
        }

        Ok(())
    }

    /// Handle DISCONNECT
    pub async fn handle_disconnect(&self, client_id: &str) -> Result<()> {
        let mut sessions = self.sessions.write().await;

        if let Some(session) = sessions.get_mut(client_id) {
            let sub_count = session.subscriptions.len() as u64;
            session.connected = false;

            if session.clean_session {
                sessions.remove(client_id);
            }

            self.stats.connections_active.fetch_sub(1, Ordering::Relaxed);
            self.stats
                .subscriptions_active
                .fetch_sub(sub_count, Ordering::Relaxed);

            info!(client_id, "MQTT client disconnected");
        }

        Ok(())
    }

    /// Find clients subscribed to a topic (for message forwarding)
    pub async fn find_subscribers(&self, topic: &str) -> Vec<(String, QoSLevel)> {
        let sessions = self.sessions.read().await;
        let mut subscribers = Vec::new();

        for (client_id, session) in sessions.iter() {
            if !session.connected {
                continue;
            }
            for (filter, qos) in &session.subscriptions {
                if mqtt_topic_matches(filter, topic) {
                    subscribers.push((client_id.clone(), *qos));
                    break;
                }
            }
        }

        subscribers
    }

    /// Get handler statistics
    pub fn stats(&self) -> MqttHandlerStatsSnapshot {
        self.stats.snapshot()
    }

    /// Get active session count
    pub async fn active_sessions(&self) -> usize {
        let sessions = self.sessions.read().await;
        sessions.values().filter(|s| s.connected).count()
    }
}

/// Check if an MQTT topic matches a subscription filter
/// Supports `+` (single-level wildcard) and `#` (multi-level wildcard)
pub fn mqtt_topic_matches(filter: &str, topic: &str) -> bool {
    let filter_parts: Vec<&str> = filter.split('/').collect();
    let topic_parts: Vec<&str> = topic.split('/').collect();

    let mut fi = 0;
    let mut ti = 0;

    while fi < filter_parts.len() && ti < topic_parts.len() {
        match filter_parts[fi] {
            "#" => return true, // Multi-level wildcard matches everything
            "+" => {
                // Single-level wildcard matches one level
                fi += 1;
                ti += 1;
            }
            exact => {
                if exact != topic_parts[ti] {
                    return false;
                }
                fi += 1;
                ti += 1;
            }
        }
    }

    fi == filter_parts.len() && ti == topic_parts.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mqtt_topic_matching_exact() {
        assert!(mqtt_topic_matches("sensors/temp", "sensors/temp"));
        assert!(!mqtt_topic_matches("sensors/temp", "sensors/humidity"));
    }

    #[test]
    fn test_mqtt_topic_matching_single_wildcard() {
        assert!(mqtt_topic_matches("sensors/+/temp", "sensors/room1/temp"));
        assert!(mqtt_topic_matches("sensors/+/temp", "sensors/room2/temp"));
        assert!(!mqtt_topic_matches("sensors/+/temp", "sensors/room1/humidity"));
    }

    #[test]
    fn test_mqtt_topic_matching_multi_wildcard() {
        assert!(mqtt_topic_matches("sensors/#", "sensors/temp"));
        assert!(mqtt_topic_matches("sensors/#", "sensors/room1/temp"));
        assert!(mqtt_topic_matches("#", "anything/at/all"));
    }

    #[tokio::test]
    async fn test_session_connect_disconnect() {
        let mgr = MqttSessionManager::new(MqttConfig::default());

        let connect = MqttConnectData {
            client_id: "test-client".to_string(),
            clean_session: true,
            keep_alive_secs: 60,
            username: None,
            will_topic: None,
            will_message: None,
            will_qos: 0,
            will_retain: false,
        };

        let (code, present) = mgr.handle_connect(connect).await.unwrap();
        assert_eq!(code, ConnAckCode::Accepted);
        assert!(!present);
        assert_eq!(mgr.active_sessions().await, 1);

        mgr.handle_disconnect("test-client").await.unwrap();
        assert_eq!(mgr.active_sessions().await, 0);
    }

    #[tokio::test]
    async fn test_session_publish() {
        let mgr = MqttSessionManager::new(MqttConfig::default());

        let connect = MqttConnectData {
            client_id: "pub-client".to_string(),
            clean_session: true,
            keep_alive_secs: 60,
            username: None,
            will_topic: None,
            will_message: None,
            will_qos: 0,
            will_retain: false,
        };
        mgr.handle_connect(connect).await.unwrap();

        let publish = MqttPublishData {
            topic: "sensors/temperature".to_string(),
            qos: QoSLevel::AtMostOnce,
            retain: false,
            packet_id: None,
            payload: b"23.5".to_vec(),
        };

        let topic = mgr.handle_publish("pub-client", &publish).await.unwrap();
        assert_eq!(topic, "sensors-temperature");
    }

    #[tokio::test]
    async fn test_subscribe_and_find() {
        let mgr = MqttSessionManager::new(MqttConfig::default());

        let connect = MqttConnectData {
            client_id: "sub-client".to_string(),
            clean_session: true,
            keep_alive_secs: 60,
            username: None,
            will_topic: None,
            will_message: None,
            will_qos: 0,
            will_retain: false,
        };
        mgr.handle_connect(connect).await.unwrap();

        let subs = vec![MqttSubscription {
            topic_filter: "sensors/+/temp".to_string(),
            qos: QoSLevel::AtLeastOnce,
        }];
        let granted = mgr.handle_subscribe("sub-client", &subs).await.unwrap();
        assert_eq!(granted.len(), 1);

        let subscribers = mgr.find_subscribers("sensors/room1/temp").await;
        assert_eq!(subscribers.len(), 1);
        assert_eq!(subscribers[0].0, "sub-client");
    }

    #[tokio::test]
    async fn test_publish_not_connected() {
        let mgr = MqttSessionManager::new(MqttConfig::default());
        let publish = MqttPublishData {
            topic: "test".to_string(),
            qos: QoSLevel::AtMostOnce,
            retain: false,
            packet_id: None,
            payload: b"data".to_vec(),
        };
        let result = mgr.handle_publish("unknown", &publish).await;
        assert!(result.is_err());
    }

    #[test]
    fn test_packet_type_from_byte() {
        assert_eq!(
            MqttPacketType::from_byte(0x10),
            Some(MqttPacketType::Connect)
        );
        assert_eq!(
            MqttPacketType::from_byte(0x30),
            Some(MqttPacketType::Publish)
        );
        assert_eq!(
            MqttPacketType::from_byte(0xE0),
            Some(MqttPacketType::Disconnect)
        );
        assert_eq!(MqttPacketType::from_byte(0x00), None);
    }
}
