//! Protocol router — core gateway routing engine.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Gateway configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayConfig {
    /// Enable MQTT adapter
    #[serde(default)]
    pub mqtt_enabled: bool,
    /// MQTT listen address
    #[serde(default = "default_mqtt_addr")]
    pub mqtt_addr: String,
    /// Enable AMQP adapter
    #[serde(default)]
    pub amqp_enabled: bool,
    /// AMQP listen address
    #[serde(default = "default_amqp_addr")]
    pub amqp_addr: String,
    /// Enable gRPC adapter
    #[serde(default)]
    pub grpc_enabled: bool,
    /// gRPC listen address
    #[serde(default = "default_grpc_addr")]
    pub grpc_addr: String,
    /// Topic mappings
    #[serde(default)]
    pub mappings: Vec<TopicMapping>,
}

fn default_mqtt_addr() -> String {
    "0.0.0.0:1883".to_string()
}
fn default_amqp_addr() -> String {
    "0.0.0.0:5672".to_string()
}
fn default_grpc_addr() -> String {
    "0.0.0.0:9096".to_string()
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            mqtt_enabled: false,
            amqp_enabled: false,
            grpc_enabled: false,
            mqtt_addr: default_mqtt_addr(),
            amqp_addr: default_amqp_addr(),
            grpc_addr: default_grpc_addr(),
            mappings: Vec::new(),
        }
    }
}

/// Mapping from external protocol topic to internal Streamline topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicMapping {
    /// Source protocol
    pub protocol: ProtocolMapping,
    /// External topic/exchange/channel name pattern
    pub external_pattern: String,
    /// Internal Streamline topic name (supports {name} substitution)
    pub internal_topic: String,
    /// Target partition (None = hash-based)
    pub partition: Option<i32>,
    /// Auto-create the internal topic
    #[serde(default = "default_true")]
    pub auto_create: bool,
}

fn default_true() -> bool {
    true
}

/// Protocol type for mapping.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ProtocolMapping {
    Mqtt,
    Amqp,
    Grpc,
    Any,
}

/// Gateway statistics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GatewayStats {
    pub mqtt_connections: u64,
    pub mqtt_messages_in: u64,
    pub amqp_connections: u64,
    pub amqp_messages_in: u64,
    pub grpc_connections: u64,
    pub grpc_messages_in: u64,
    pub total_routed: u64,
    pub routing_errors: u64,
}

/// Atomic stats counters.
pub struct GatewayStatsCounters {
    pub mqtt_connections: AtomicU64,
    pub mqtt_messages_in: AtomicU64,
    pub amqp_connections: AtomicU64,
    pub amqp_messages_in: AtomicU64,
    pub grpc_connections: AtomicU64,
    pub grpc_messages_in: AtomicU64,
    pub total_routed: AtomicU64,
    pub routing_errors: AtomicU64,
}

impl Default for GatewayStatsCounters {
    fn default() -> Self {
        Self {
            mqtt_connections: AtomicU64::new(0),
            mqtt_messages_in: AtomicU64::new(0),
            amqp_connections: AtomicU64::new(0),
            amqp_messages_in: AtomicU64::new(0),
            grpc_connections: AtomicU64::new(0),
            grpc_messages_in: AtomicU64::new(0),
            total_routed: AtomicU64::new(0),
            routing_errors: AtomicU64::new(0),
        }
    }
}

impl GatewayStatsCounters {
    pub fn snapshot(&self) -> GatewayStats {
        GatewayStats {
            mqtt_connections: self.mqtt_connections.load(Ordering::Relaxed),
            mqtt_messages_in: self.mqtt_messages_in.load(Ordering::Relaxed),
            amqp_connections: self.amqp_connections.load(Ordering::Relaxed),
            amqp_messages_in: self.amqp_messages_in.load(Ordering::Relaxed),
            grpc_connections: self.grpc_connections.load(Ordering::Relaxed),
            grpc_messages_in: self.grpc_messages_in.load(Ordering::Relaxed),
            total_routed: self.total_routed.load(Ordering::Relaxed),
            routing_errors: self.routing_errors.load(Ordering::Relaxed),
        }
    }
}

/// The protocol gateway — routes messages from multiple protocols into Streamline.
pub struct ProtocolGateway {
    config: GatewayConfig,
    stats: Arc<GatewayStatsCounters>,
    #[allow(dead_code)]
    mapping_cache: HashMap<String, String>,
}

impl ProtocolGateway {
    /// Create a new protocol gateway.
    pub fn new(config: GatewayConfig) -> Self {
        let mut mapping_cache = HashMap::new();
        for mapping in &config.mappings {
            mapping_cache.insert(
                mapping.external_pattern.clone(),
                mapping.internal_topic.clone(),
            );
        }

        Self {
            config,
            stats: Arc::new(GatewayStatsCounters::default()),
            mapping_cache,
        }
    }

    /// Resolve an external topic name to an internal Streamline topic.
    pub fn resolve_topic(&self, protocol: &ProtocolMapping, external_name: &str) -> Option<String> {
        // Check explicit mappings first
        for mapping in &self.config.mappings {
            if (&mapping.protocol == protocol || mapping.protocol == ProtocolMapping::Any)
                && Self::pattern_matches(&mapping.external_pattern, external_name)
            {
                return Some(mapping.internal_topic.replace("{name}", external_name));
            }
        }

        // Default: use the external name directly
        Some(self.sanitize_topic_name(external_name))
    }

    /// Get gateway stats.
    pub fn stats(&self) -> GatewayStats {
        self.stats.snapshot()
    }

    /// Get the stats counters (for adapters to increment).
    pub fn stats_counters(&self) -> Arc<GatewayStatsCounters> {
        self.stats.clone()
    }

    /// Get configuration.
    pub fn config(&self) -> &GatewayConfig {
        &self.config
    }

    fn pattern_matches(pattern: &str, name: &str) -> bool {
        if pattern == "*" {
            return true;
        }
        if let Some(prefix) = pattern.strip_suffix('*') {
            return name.starts_with(prefix);
        }
        pattern == name
    }

    fn sanitize_topic_name(&self, name: &str) -> String {
        // Replace MQTT-style / with - and remove invalid chars
        name.replace(['/', ' '], "-")
            .chars()
            .filter(|c| c.is_alphanumeric() || *c == '-' || *c == '_' || *c == '.')
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_resolution_default() {
        let gw = ProtocolGateway::new(GatewayConfig::default());
        let topic = gw
            .resolve_topic(&ProtocolMapping::Mqtt, "sensors/temp")
            .unwrap();
        assert_eq!(topic, "sensors-temp");
    }

    #[test]
    fn test_topic_resolution_with_mapping() {
        let config = GatewayConfig {
            mappings: vec![TopicMapping {
                protocol: ProtocolMapping::Mqtt,
                external_pattern: "sensors/*".to_string(),
                internal_topic: "iot-{name}".to_string(),
                partition: None,
                auto_create: true,
            }],
            ..Default::default()
        };
        let gw = ProtocolGateway::new(config);
        let topic = gw
            .resolve_topic(&ProtocolMapping::Mqtt, "sensors/temp")
            .unwrap();
        assert_eq!(topic, "iot-sensors/temp");
    }

    #[test]
    fn test_stats_snapshot() {
        let gw = ProtocolGateway::new(GatewayConfig::default());
        gw.stats.mqtt_messages_in.fetch_add(5, Ordering::Relaxed);
        let stats = gw.stats();
        assert_eq!(stats.mqtt_messages_in, 5);
    }
}
