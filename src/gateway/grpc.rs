//! gRPC streaming adapter for the gateway.

use serde::{Deserialize, Serialize};

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

impl Default for GrpcConfig {
    fn default() -> Self {
        Self {
            addr: default_addr(),
            max_message_size: default_max_message_size(),
            reflection_enabled: true,
            tls_enabled: false,
        }
    }
}

/// gRPC streaming adapter.
///
/// Exposes a bidirectional streaming RPC service:
/// - `Produce(stream ProduceRequest) returns (stream ProduceResponse)`
/// - `Consume(ConsumeRequest) returns (stream ConsumeResponse)`
/// - `ListTopics(Empty) returns (TopicList)`
pub struct GrpcAdapter {
    config: GrpcConfig,
}

impl GrpcAdapter {
    /// Create a new gRPC adapter.
    pub fn new(config: GrpcConfig) -> Self {
        Self { config }
    }

    /// Get the listen address.
    pub fn addr(&self) -> &str {
        &self.config.addr
    }

    /// Get configuration.
    pub fn config(&self) -> &GrpcConfig {
        &self.config
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
    }
}
