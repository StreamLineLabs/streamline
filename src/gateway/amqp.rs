//! AMQP 1.0 protocol adapter for the gateway.

use serde::{Deserialize, Serialize};

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

impl Default for AmqpConfig {
    fn default() -> Self {
        Self {
            addr: default_addr(),
            max_frame_size: default_frame_size(),
            channel_max: default_channel_max(),
            idle_timeout_secs: default_idle_timeout(),
        }
    }
}

/// AMQP protocol adapter.
///
/// Maps AMQP concepts to Streamline:
/// - AMQP Exchange → Streamline topic (exchange name = topic name)
/// - AMQP Queue → Consumer group subscription
/// - AMQP Routing Key → Message key
pub struct AmqpAdapter {
    config: AmqpConfig,
}

impl AmqpAdapter {
    /// Create a new AMQP adapter.
    pub fn new(config: AmqpConfig) -> Self {
        Self { config }
    }

    /// Get the listen address.
    pub fn addr(&self) -> &str {
        &self.config.addr
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

    /// Get configuration.
    pub fn config(&self) -> &AmqpConfig {
        &self.config
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
}
