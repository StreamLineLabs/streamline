//! Connector SDK — traits for building custom connectors.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Health status of a connector.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
    Unknown,
}

/// Health check result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheck {
    pub status: HealthStatus,
    pub message: String,
    pub latency_ms: u64,
}

/// Context provided to connectors during execution.
#[derive(Debug, Clone)]
pub struct ConnectorContext {
    /// Connector name
    pub name: String,
    /// Configuration key-value pairs
    pub config: HashMap<String, String>,
    /// Target topics (for sources) or source topics (for sinks)
    pub topics: Vec<String>,
    /// Data directory for connector state
    pub state_dir: Option<String>,
}

/// Trait for source connectors (produce data into Streamline).
#[async_trait]
pub trait SourceConnectorTrait: Send + Sync {
    /// Initialize the connector with the given context.
    async fn init(&mut self, context: &ConnectorContext) -> Result<(), String>;

    /// Poll for new records. Returns a batch of (key, value, headers) tuples.
    async fn poll(
        &mut self,
    ) -> Result<Vec<(Option<Vec<u8>>, Vec<u8>, HashMap<String, String>)>, String>;

    /// Commit offsets for the source.
    async fn commit(&mut self) -> Result<(), String>;

    /// Perform a health check.
    async fn health_check(&self) -> HealthCheck;

    /// Gracefully stop the connector.
    async fn stop(&mut self) -> Result<(), String>;
}

/// Trait for sink connectors (consume data from Streamline).
#[async_trait]
pub trait SinkConnectorTrait: Send + Sync {
    /// Initialize the connector with the given context.
    async fn init(&mut self, context: &ConnectorContext) -> Result<(), String>;

    /// Write a batch of records. Each record is (offset, key, value, headers).
    async fn write(
        &mut self,
        records: &[(i64, Option<&[u8]>, &[u8], &HashMap<String, String>)],
    ) -> Result<(), String>;

    /// Flush any buffered data.
    async fn flush(&mut self) -> Result<(), String>;

    /// Perform a health check.
    async fn health_check(&self) -> HealthCheck;

    /// Gracefully stop the connector.
    async fn stop(&mut self) -> Result<(), String>;
}

/// Lifecycle management for connectors.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorLifecycle {
    Created,
    Initializing,
    Running,
    Paused,
    Stopping,
    Stopped,
    Failed(String),
}

/// SDK helper for building connectors.
pub struct ConnectorSdk;

impl ConnectorSdk {
    /// Validate that all required config keys are present.
    pub fn validate_config(
        config: &HashMap<String, String>,
        required_keys: &[&str],
    ) -> Result<(), String> {
        let missing: Vec<&&str> = required_keys
            .iter()
            .filter(|k| !config.contains_key(**k))
            .collect();

        if missing.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "Missing required configuration: {}",
                missing
                    .iter()
                    .map(|k| k.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
        }
    }

    /// Parse a config value as a specific type.
    pub fn parse_config<T: std::str::FromStr>(
        config: &HashMap<String, String>,
        key: &str,
        default: T,
    ) -> T {
        config
            .get(key)
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_config() {
        let mut config = HashMap::new();
        config.insert("host".to_string(), "localhost".to_string());

        assert!(ConnectorSdk::validate_config(&config, &["host"]).is_ok());
        assert!(ConnectorSdk::validate_config(&config, &["host", "port"]).is_err());
    }

    #[test]
    fn test_parse_config() {
        let mut config = HashMap::new();
        config.insert("port".to_string(), "8080".to_string());

        assert_eq!(
            ConnectorSdk::parse_config::<u16>(&config, "port", 3000),
            8080
        );
        assert_eq!(
            ConnectorSdk::parse_config::<u16>(&config, "missing", 3000),
            3000
        );
    }
}
