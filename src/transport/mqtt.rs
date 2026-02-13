//! MQTT 5.0 protocol gateway
//!
//! Translates MQTT 5.0 protocol operations to Streamline topics,
//! allowing MQTT clients to produce and consume from Streamline
//! using standard MQTT semantics.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use bytes::Bytes;

use crate::error::{Result, StreamlineError};
use crate::storage::TopicManager;

/// MQTT Quality of Service levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[allow(clippy::enum_variant_names)]
pub enum QoS {
    /// At most once delivery (fire and forget)
    AtMostOnce = 0,
    /// At least once delivery (acknowledged)
    AtLeastOnce = 1,
    /// Exactly once delivery (assured)
    ExactlyOnce = 2,
}

/// MQTT 5.0 control packet types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MqttPacketType {
    Connect = 1,
    ConnAck = 2,
    Publish = 3,
    PubAck = 4,
    PubRec = 5,
    PubRel = 6,
    PubComp = 7,
    Subscribe = 8,
    SubAck = 9,
    Unsubscribe = 10,
    UnsubAck = 11,
    PingReq = 12,
    PingResp = 13,
    Disconnect = 14,
    Auth = 15,
}

/// MQTT message
#[derive(Debug, Clone)]
pub struct MqttMessage {
    /// MQTT topic path (e.g. "sensors/temperature/room1")
    pub topic: String,
    /// Message payload
    pub payload: Vec<u8>,
    /// Quality of service level
    pub qos: QoS,
    /// Retain flag
    pub retain: bool,
    /// Packet identifier
    pub message_id: u16,
    /// MQTT 5.0 user properties
    pub properties: HashMap<String, String>,
}

/// MQTT topic subscription
#[derive(Debug, Clone)]
pub struct MqttSubscription {
    /// MQTT topic filter (may contain wildcards: +, #)
    pub topic_filter: String,
    /// Requested QoS level
    pub qos: QoS,
    /// Mapped Streamline topic name
    pub streamline_topic: String,
}

/// Connected MQTT client state
#[derive(Debug, Clone)]
pub struct MqttClient {
    /// Client identifier
    pub client_id: String,
    /// When the client connected
    pub connected_at: Instant,
    /// Active subscriptions
    pub subscriptions: Vec<MqttSubscription>,
    /// Keep-alive interval in seconds
    pub keep_alive_secs: u16,
    /// Whether session state is discarded on disconnect
    pub clean_session: bool,
}

/// Result of a CONNECT handshake
#[derive(Debug, Clone)]
pub struct ConnAckResult {
    /// Whether an existing session was present
    pub session_present: bool,
    /// Reason code (0 = success)
    pub reason_code: u8,
}

/// Result of a SUBSCRIBE request
#[derive(Debug, Clone)]
pub struct SubAckResult {
    /// Per-filter granted QoS (or failure code)
    pub reason_codes: Vec<u8>,
}

/// Gateway configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MqttGatewayConfig {
    /// Address to listen on (e.g. "0.0.0.0:1883")
    pub listen_addr: String,
    /// Maximum number of concurrent clients
    pub max_clients: usize,
    /// Maximum MQTT message payload size in bytes
    pub max_message_size: usize,
    /// Default keep-alive in seconds when client does not specify one
    pub keep_alive_default: u16,
    /// Whether authentication is required
    pub auth_required: bool,
}

impl Default for MqttGatewayConfig {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:1883".to_string(),
            max_clients: 10_000,
            max_message_size: 256 * 1024,
            keep_alive_default: 60,
            auth_required: false,
        }
    }
}

/// Runtime statistics for the MQTT gateway
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MqttGatewayStats {
    pub connected_clients: u64,
    pub messages_received: u64,
    pub messages_published: u64,
    pub bytes_received: u64,
    pub bytes_sent: u64,
}

/// MQTT 5.0 protocol gateway
///
/// Bridges MQTT clients to the Streamline storage engine by mapping
/// MQTT topic paths to Streamline topic names and translating
/// publish/subscribe operations.
pub struct MqttGateway {
    topic_manager: Arc<TopicManager>,
    config: MqttGatewayConfig,
    clients: RwLock<HashMap<String, MqttClient>>,
    messages_received: AtomicU64,
    messages_published: AtomicU64,
    bytes_received: AtomicU64,
    bytes_sent: AtomicU64,
}

impl MqttGateway {
    /// Create a new MQTT gateway backed by the given topic manager.
    pub fn new(topic_manager: Arc<TopicManager>, config: MqttGatewayConfig) -> Self {
        info!(listen_addr = %config.listen_addr, "Creating MQTT 5.0 gateway");
        Self {
            topic_manager,
            config,
            clients: RwLock::new(HashMap::new()),
            messages_received: AtomicU64::new(0),
            messages_published: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
        }
    }

    /// Handle an MQTT CONNECT packet.
    pub fn handle_connect(
        &self,
        client_id: &str,
        clean_session: bool,
        keep_alive: u16,
    ) -> Result<ConnAckResult> {
        let clients = self.clients.read();
        if clients.len() >= self.config.max_clients && !clients.contains_key(client_id) {
            warn!(client_id, "MQTT gateway at max clients capacity");
            return Err(StreamlineError::protocol_msg(
                "max clients exceeded".to_string(),
            ));
        }
        drop(clients);

        let keep_alive = if keep_alive == 0 {
            self.config.keep_alive_default
        } else {
            keep_alive
        };

        let mut clients = self.clients.write();
        let session_present = clients.contains_key(client_id) && !clean_session;

        if clean_session {
            clients.remove(client_id);
        }

        let client = MqttClient {
            client_id: client_id.to_string(),
            connected_at: Instant::now(),
            subscriptions: if session_present {
                clients
                    .get(client_id)
                    .map(|c| c.subscriptions.clone())
                    .unwrap_or_default()
            } else {
                Vec::new()
            },
            keep_alive_secs: keep_alive,
            clean_session,
        };

        info!(
            client_id,
            keep_alive, clean_session, "MQTT client connected"
        );
        clients.insert(client_id.to_string(), client);

        Ok(ConnAckResult {
            session_present,
            reason_code: 0,
        })
    }

    /// Handle an MQTT PUBLISH packet by writing the message to the
    /// corresponding Streamline topic.
    pub fn handle_publish(&self, client_id: &str, message: &MqttMessage) -> Result<()> {
        {
            let clients = self.clients.read();
            if !clients.contains_key(client_id) {
                return Err(StreamlineError::protocol_msg(
                    "client not connected".to_string(),
                ));
            }
        }

        if message.payload.len() > self.config.max_message_size {
            return Err(StreamlineError::protocol_msg(
                "message exceeds max size".to_string(),
            ));
        }

        let streamline_topic = Self::map_mqtt_topic(&message.topic);
        debug!(
            client_id,
            mqtt_topic = %message.topic,
            streamline_topic,
            payload_len = message.payload.len(),
            "MQTT PUBLISH"
        );

        self.topic_manager.append(
            &streamline_topic,
            0,
            None,
            Bytes::from(message.payload.clone()),
        )?;

        self.messages_received.fetch_add(1, Ordering::Relaxed);
        self.messages_published.fetch_add(1, Ordering::Relaxed);
        self.bytes_received
            .fetch_add(message.payload.len() as u64, Ordering::Relaxed);

        Ok(())
    }

    /// Handle an MQTT SUBSCRIBE packet.
    pub fn handle_subscribe(
        &self,
        client_id: &str,
        subscriptions: Vec<MqttSubscription>,
    ) -> Result<SubAckResult> {
        let mut clients = self.clients.write();
        let client = clients
            .get_mut(client_id)
            .ok_or_else(|| StreamlineError::protocol_msg("client not connected".to_string()))?;

        let mut reason_codes = Vec::with_capacity(subscriptions.len());
        for sub in subscriptions {
            debug!(
                client_id,
                filter = %sub.topic_filter,
                qos = ?sub.qos,
                "MQTT SUBSCRIBE"
            );
            let granted_qos = sub.qos as u8;
            client.subscriptions.push(sub);
            reason_codes.push(granted_qos);
        }

        Ok(SubAckResult { reason_codes })
    }

    /// Handle an MQTT DISCONNECT packet.
    pub fn handle_disconnect(&self, client_id: &str) -> Result<()> {
        let mut clients = self.clients.write();
        if clients.remove(client_id).is_some() {
            info!(client_id, "MQTT client disconnected");
            Ok(())
        } else {
            Err(StreamlineError::protocol_msg(
                "client not connected".to_string(),
            ))
        }
    }

    /// Convert an MQTT topic path to a Streamline topic name.
    ///
    /// MQTT uses `/` as a level separator; Streamline uses `-`.
    /// Leading and trailing separators are stripped.
    pub fn map_mqtt_topic(mqtt_topic: &str) -> String {
        mqtt_topic.trim_matches('/').replace('/', "-")
    }

    /// Test whether an MQTT topic filter matches a concrete topic.
    ///
    /// Supports `+` (single-level wildcard) and `#` (multi-level wildcard).
    pub fn match_topic_filter(filter: &str, topic: &str) -> bool {
        let filter_parts: Vec<&str> = filter.split('/').collect();
        let topic_parts: Vec<&str> = topic.split('/').collect();

        let mut fi = 0;
        let mut ti = 0;

        while fi < filter_parts.len() {
            if filter_parts[fi] == "#" {
                // '#' must be the last level and matches everything remaining
                return fi == filter_parts.len() - 1;
            }

            if ti >= topic_parts.len() {
                return false;
            }

            if filter_parts[fi] != "+" && filter_parts[fi] != topic_parts[ti] {
                return false;
            }

            fi += 1;
            ti += 1;
        }

        ti == topic_parts.len()
    }

    /// Return a snapshot of gateway statistics.
    pub fn stats(&self) -> MqttGatewayStats {
        MqttGatewayStats {
            connected_clients: self.clients.read().len() as u64,
            messages_received: self.messages_received.load(Ordering::Relaxed),
            messages_published: self.messages_published.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_map_mqtt_topic() {
        assert_eq!(
            MqttGateway::map_mqtt_topic("sensors/temperature/room1"),
            "sensors-temperature-room1"
        );
        assert_eq!(
            MqttGateway::map_mqtt_topic("/leading/slash"),
            "leading-slash"
        );
        assert_eq!(
            MqttGateway::map_mqtt_topic("trailing/slash/"),
            "trailing-slash"
        );
        assert_eq!(MqttGateway::map_mqtt_topic("simple"), "simple");
    }

    #[test]
    fn test_wildcard_single_level() {
        assert!(MqttGateway::match_topic_filter(
            "sensors/+/temperature",
            "sensors/room1/temperature"
        ));
        assert!(MqttGateway::match_topic_filter(
            "sensors/+/temperature",
            "sensors/room2/temperature"
        ));
        assert!(!MqttGateway::match_topic_filter(
            "sensors/+/temperature",
            "sensors/room1/humidity"
        ));
        assert!(!MqttGateway::match_topic_filter(
            "sensors/+/temperature",
            "sensors/floor/room1/temperature"
        ));
    }

    #[test]
    fn test_wildcard_multi_level() {
        assert!(MqttGateway::match_topic_filter(
            "sensors/#",
            "sensors/temperature"
        ));
        assert!(MqttGateway::match_topic_filter(
            "sensors/#",
            "sensors/temperature/room1"
        ));
        assert!(MqttGateway::match_topic_filter("#", "anything/goes/here"));
        assert!(!MqttGateway::match_topic_filter(
            "sensors/#",
            "devices/temperature"
        ));
    }

    #[test]
    fn test_exact_match() {
        assert!(MqttGateway::match_topic_filter(
            "sensors/temperature",
            "sensors/temperature"
        ));
        assert!(!MqttGateway::match_topic_filter(
            "sensors/temperature",
            "sensors/humidity"
        ));
        assert!(!MqttGateway::match_topic_filter(
            "sensors/temperature",
            "sensors/temperature/extra"
        ));
    }

    #[test]
    fn test_qos_values() {
        assert_eq!(QoS::AtMostOnce as u8, 0);
        assert_eq!(QoS::AtLeastOnce as u8, 1);
        assert_eq!(QoS::ExactlyOnce as u8, 2);
    }

    #[test]
    fn test_mqtt_gateway_config_default() {
        let cfg = MqttGatewayConfig::default();
        assert_eq!(cfg.listen_addr, "0.0.0.0:1883");
        assert_eq!(cfg.max_clients, 10_000);
        assert_eq!(cfg.max_message_size, 256 * 1024);
        assert_eq!(cfg.keep_alive_default, 60);
        assert!(!cfg.auth_required);
    }

    #[test]
    fn test_mqtt_packet_type_values() {
        assert_eq!(MqttPacketType::Connect as u8, 1);
        assert_eq!(MqttPacketType::Publish as u8, 3);
        assert_eq!(MqttPacketType::Subscribe as u8, 8);
        assert_eq!(MqttPacketType::Disconnect as u8, 14);
        assert_eq!(MqttPacketType::Auth as u8, 15);
    }
}
