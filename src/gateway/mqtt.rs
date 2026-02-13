//! MQTT protocol adapter for the gateway.

use serde::{Deserialize, Serialize};

/// MQTT Quality of Service levels.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[allow(dead_code, clippy::enum_variant_names)]
pub enum QoSLevel {
    /// At most once delivery (fire and forget)
    AtMostOnce = 0,
    /// At least once delivery (acknowledged)
    AtLeastOnce = 1,
    /// Exactly once delivery (four-step handshake)
    ExactlyOnce = 2,
}

/// MQTT adapter configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MqttConfig {
    /// Listen address
    #[serde(default = "default_addr")]
    pub addr: String,
    /// Maximum packet size
    #[serde(default = "default_max_packet_size")]
    pub max_packet_size: usize,
    /// Keep alive interval in seconds
    #[serde(default = "default_keep_alive")]
    pub keep_alive_secs: u64,
    /// Maximum QoS level to support
    #[serde(default = "default_max_qos")]
    pub max_qos: u8,
    /// Allow anonymous connections
    #[serde(default = "default_true")]
    pub allow_anonymous: bool,
    /// Retain messages as compacted topics
    #[serde(default = "default_true")]
    pub retain_as_compacted: bool,
}

fn default_addr() -> String {
    "0.0.0.0:1883".to_string()
}
fn default_max_packet_size() -> usize {
    256 * 1024
}
fn default_keep_alive() -> u64 {
    60
}
fn default_max_qos() -> u8 {
    1
}
fn default_true() -> bool {
    true
}

impl Default for MqttConfig {
    fn default() -> Self {
        Self {
            addr: default_addr(),
            max_packet_size: default_max_packet_size(),
            keep_alive_secs: default_keep_alive(),
            max_qos: default_max_qos(),
            allow_anonymous: true,
            retain_as_compacted: true,
        }
    }
}

/// MQTT protocol adapter.
///
/// Accepts MQTT 3.1.1/5.0 connections and maps MQTT topics to Streamline topics:
/// - MQTT topic `sensors/temperature` → Streamline topic `sensors-temperature`
/// - MQTT retained messages → log-compacted topics
/// - MQTT QoS 0/1 → at-most-once / at-least-once delivery
pub struct MqttAdapter {
    config: MqttConfig,
}

impl MqttAdapter {
    /// Create a new MQTT adapter.
    pub fn new(config: MqttConfig) -> Self {
        Self { config }
    }

    /// Get the listen address.
    pub fn addr(&self) -> &str {
        &self.config.addr
    }

    /// Map an MQTT topic to a Streamline topic name.
    pub fn map_topic(&self, mqtt_topic: &str) -> String {
        mqtt_topic
            .replace('/', "-")
            .replace('+', "_single")
            .replace('#', "_multi")
    }

    /// Get configuration.
    pub fn config(&self) -> &MqttConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mqtt_topic_mapping() {
        let adapter = MqttAdapter::new(MqttConfig::default());
        assert_eq!(
            adapter.map_topic("sensors/temperature"),
            "sensors-temperature"
        );
        assert_eq!(adapter.map_topic("home/+/light"), "home-_single-light");
    }
}
