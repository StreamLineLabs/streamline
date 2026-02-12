//! Declarative Connector Configuration
//!
//! Enables YAML-based connector definition and lifecycle management:
//! - Declarative connector specs (`streamline.connectors.yaml`)
//! - Connector health monitoring and auto-restart
//! - Connector version management
//! - One-click installation from marketplace

use crate::error::{Result, StreamlineError};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};

/// Declarative connector specification (YAML format)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorSpec {
    /// API version
    #[serde(default = "default_api_version")]
    pub api_version: String,
    /// Kind (always "Connector")
    #[serde(default = "default_kind")]
    pub kind: String,
    /// Metadata
    pub metadata: ConnectorMetadataSpec,
    /// Spec
    pub spec: ConnectorSpecBody,
}

fn default_api_version() -> String {
    "streamline.dev/v1".to_string()
}

fn default_kind() -> String {
    "Connector".to_string()
}

/// Connector metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorMetadataSpec {
    /// Connector name
    pub name: String,
    /// Labels for organization
    #[serde(default)]
    pub labels: HashMap<String, String>,
    /// Annotations
    #[serde(default)]
    pub annotations: HashMap<String, String>,
}

/// Connector spec body
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorSpecBody {
    /// Connector type from marketplace
    pub connector_type: String,
    /// Version
    pub version: String,
    /// Direction: source or sink
    pub direction: ConnectorDirection,
    /// Topics to connect
    pub topics: Vec<String>,
    /// Configuration key-value pairs
    #[serde(default)]
    pub config: HashMap<String, String>,
    /// Parallelism (number of tasks)
    #[serde(default = "default_parallelism")]
    pub parallelism: u32,
    /// Restart policy
    #[serde(default)]
    pub restart_policy: RestartPolicy,
    /// Health check configuration
    #[serde(default)]
    pub health_check: HealthCheckSpec,
}

fn default_parallelism() -> u32 {
    1
}

/// Connector direction
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConnectorDirection {
    #[serde(rename = "source")]
    Source,
    #[serde(rename = "sink")]
    Sink,
}

impl std::fmt::Display for ConnectorDirection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Source => write!(f, "source"),
            Self::Sink => write!(f, "sink"),
        }
    }
}

/// Restart policy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestartPolicy {
    /// Maximum restart attempts
    #[serde(default = "default_max_restarts")]
    pub max_restarts: u32,
    /// Backoff interval between restarts
    #[serde(default = "default_backoff_secs")]
    pub backoff_secs: u64,
}

fn default_max_restarts() -> u32 {
    3
}

fn default_backoff_secs() -> u64 {
    10
}

impl Default for RestartPolicy {
    fn default() -> Self {
        Self {
            max_restarts: default_max_restarts(),
            backoff_secs: default_backoff_secs(),
        }
    }
}

/// Health check configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckSpec {
    #[serde(default = "default_health_interval")]
    pub interval_secs: u64,
    #[serde(default = "default_health_timeout")]
    pub timeout_secs: u64,
}

fn default_health_interval() -> u64 {
    30
}

fn default_health_timeout() -> u64 {
    10
}

impl Default for HealthCheckSpec {
    fn default() -> Self {
        Self {
            interval_secs: default_health_interval(),
            timeout_secs: default_health_timeout(),
        }
    }
}

/// Connector runtime state
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConnectorRuntimeState {
    Pending,
    Running,
    Paused,
    Failed,
    Stopped,
    Restarting,
}

impl std::fmt::Display for ConnectorRuntimeState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => write!(f, "pending"),
            Self::Running => write!(f, "running"),
            Self::Paused => write!(f, "paused"),
            Self::Failed => write!(f, "failed"),
            Self::Stopped => write!(f, "stopped"),
            Self::Restarting => write!(f, "restarting"),
        }
    }
}

/// Managed connector instance
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManagedConnector {
    pub name: String,
    pub spec: ConnectorSpec,
    pub state: ConnectorRuntimeState,
    pub restart_count: u32,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub metrics: ConnectorInstanceMetrics,
}

/// Connector instance metrics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ConnectorInstanceMetrics {
    pub records_processed: u64,
    pub bytes_processed: u64,
    pub errors: u64,
    pub avg_latency_ms: f64,
    pub last_record_at: Option<DateTime<Utc>>,
}

/// Connector lifecycle manager
pub struct ConnectorManager {
    connectors: Arc<RwLock<HashMap<String, ManagedConnector>>>,
}

impl Default for ConnectorManager {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectorManager {
    pub fn new() -> Self {
        Self {
            connectors: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Apply a connector spec (create or update)
    pub async fn apply(&self, spec: ConnectorSpec) -> Result<ManagedConnector> {
        let name = spec.metadata.name.clone();

        let connector = ManagedConnector {
            name: name.clone(),
            spec,
            state: ConnectorRuntimeState::Pending,
            restart_count: 0,
            last_error: None,
            created_at: Utc::now(),
            started_at: None,
            metrics: ConnectorInstanceMetrics::default(),
        };

        self.connectors
            .write()
            .await
            .insert(name.clone(), connector.clone());

        info!("Applied connector: {}", name);
        Ok(connector)
    }

    /// Apply from YAML string
    pub async fn apply_yaml(&self, yaml: &str) -> Result<ManagedConnector> {
        let spec: ConnectorSpec = serde_yaml::from_str(yaml)
            .map_err(|e| StreamlineError::Config(format!("Invalid connector YAML: {}", e)))?;
        self.apply(spec).await
    }

    /// Start a connector
    pub async fn start(&self, name: &str) -> Result<()> {
        let mut connectors = self.connectors.write().await;
        let conn = connectors
            .get_mut(name)
            .ok_or_else(|| StreamlineError::Connector(format!("Connector not found: {}", name)))?;

        conn.state = ConnectorRuntimeState::Running;
        conn.started_at = Some(Utc::now());
        info!("Started connector: {}", name);
        Ok(())
    }

    /// Stop a connector
    pub async fn stop(&self, name: &str) -> Result<()> {
        let mut connectors = self.connectors.write().await;
        let conn = connectors
            .get_mut(name)
            .ok_or_else(|| StreamlineError::Connector(format!("Connector not found: {}", name)))?;

        conn.state = ConnectorRuntimeState::Stopped;
        info!("Stopped connector: {}", name);
        Ok(())
    }

    /// Pause a connector
    pub async fn pause(&self, name: &str) -> Result<()> {
        let mut connectors = self.connectors.write().await;
        let conn = connectors
            .get_mut(name)
            .ok_or_else(|| StreamlineError::Connector(format!("Connector not found: {}", name)))?;

        if conn.state != ConnectorRuntimeState::Running {
            return Err(StreamlineError::Connector(format!(
                "Connector {} is not running (state: {})",
                name, conn.state
            )));
        }

        conn.state = ConnectorRuntimeState::Paused;
        Ok(())
    }

    /// Resume a paused connector
    pub async fn resume(&self, name: &str) -> Result<()> {
        let mut connectors = self.connectors.write().await;
        let conn = connectors
            .get_mut(name)
            .ok_or_else(|| StreamlineError::Connector(format!("Connector not found: {}", name)))?;

        if conn.state != ConnectorRuntimeState::Paused {
            return Err(StreamlineError::Connector(format!(
                "Connector {} is not paused",
                name
            )));
        }

        conn.state = ConnectorRuntimeState::Running;
        Ok(())
    }

    /// Delete a connector
    pub async fn delete(&self, name: &str) -> Result<()> {
        self.connectors
            .write()
            .await
            .remove(name)
            .ok_or_else(|| StreamlineError::Connector(format!("Connector not found: {}", name)))
            .map(|_| ())
    }

    /// Get connector status
    pub async fn status(&self, name: &str) -> Result<ManagedConnector> {
        self.connectors
            .read()
            .await
            .get(name)
            .cloned()
            .ok_or_else(|| StreamlineError::Connector(format!("Connector not found: {}", name)))
    }

    /// List all connectors
    pub async fn list(&self) -> Vec<ManagedConnector> {
        self.connectors.read().await.values().cloned().collect()
    }

    /// Record an error for a connector (triggers restart policy)
    pub async fn record_error(&self, name: &str, error: &str) -> Result<()> {
        let mut connectors = self.connectors.write().await;
        let conn = connectors
            .get_mut(name)
            .ok_or_else(|| StreamlineError::Connector(format!("Connector not found: {}", name)))?;

        conn.last_error = Some(error.to_string());
        conn.metrics.errors += 1;

        let max_restarts = conn.spec.spec.restart_policy.max_restarts;
        if conn.restart_count < max_restarts {
            conn.restart_count += 1;
            conn.state = ConnectorRuntimeState::Restarting;
            warn!(
                "Connector {} restarting ({}/{}): {}",
                name, conn.restart_count, max_restarts, error
            );
        } else {
            conn.state = ConnectorRuntimeState::Failed;
            warn!(
                "Connector {} failed (max restarts reached): {}",
                name, error
            );
        }

        Ok(())
    }

    /// Update metrics for a connector
    pub async fn update_metrics(
        &self,
        name: &str,
        records: u64,
        bytes: u64,
        latency_ms: f64,
    ) -> Result<()> {
        let mut connectors = self.connectors.write().await;
        let conn = connectors
            .get_mut(name)
            .ok_or_else(|| StreamlineError::Connector(format!("Connector not found: {}", name)))?;

        conn.metrics.records_processed += records;
        conn.metrics.bytes_processed += bytes;
        conn.metrics.avg_latency_ms = latency_ms;
        conn.metrics.last_record_at = Some(Utc::now());

        Ok(())
    }

    /// Generate an example connector YAML
    pub fn example_yaml(connector_type: &str) -> String {
        match connector_type {
            "postgres-cdc" => r#"apiVersion: streamline.dev/v1
kind: Connector
metadata:
  name: postgres-orders
  labels:
    team: platform
spec:
  connector_type: postgres-cdc
  version: "1.0"
  direction: source
  topics:
    - orders-cdc
  config:
    host: localhost
    port: "5432"
    database: mydb
    schema: public
    tables: orders,order_items
    slot_name: streamline_orders
    publication: streamline_pub
  parallelism: 1
  restart_policy:
    max_restarts: 5
    backoff_secs: 30
"#
            .to_string(),

            "elasticsearch-sink" => r#"apiVersion: streamline.dev/v1
kind: Connector
metadata:
  name: es-events
spec:
  connector_type: elasticsearch
  version: "1.0"
  direction: sink
  topics:
    - events
  config:
    hosts: http://localhost:9200
    index: events
    bulk_size: "1000"
    flush_interval_ms: "5000"
  parallelism: 2
"#
            .to_string(),

            _ => format!(
                r#"apiVersion: streamline.dev/v1
kind: Connector
metadata:
  name: my-connector
spec:
  connector_type: {}
  version: "1.0"
  direction: source
  topics:
    - my-topic
  config: {{}}
"#,
                connector_type
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_spec() -> ConnectorSpec {
        ConnectorSpec {
            api_version: "streamline.dev/v1".to_string(),
            kind: "Connector".to_string(),
            metadata: ConnectorMetadataSpec {
                name: "test-connector".to_string(),
                labels: HashMap::new(),
                annotations: HashMap::new(),
            },
            spec: ConnectorSpecBody {
                connector_type: "postgres-cdc".to_string(),
                version: "1.0".to_string(),
                direction: ConnectorDirection::Source,
                topics: vec!["test-topic".to_string()],
                config: HashMap::new(),
                parallelism: 1,
                restart_policy: RestartPolicy::default(),
                health_check: HealthCheckSpec::default(),
            },
        }
    }

    #[tokio::test]
    async fn test_apply_and_list() {
        let mgr = ConnectorManager::new();
        mgr.apply(sample_spec()).await.unwrap();

        let connectors = mgr.list().await;
        assert_eq!(connectors.len(), 1);
        assert_eq!(connectors[0].name, "test-connector");
        assert_eq!(connectors[0].state, ConnectorRuntimeState::Pending);
    }

    #[tokio::test]
    async fn test_start_stop() {
        let mgr = ConnectorManager::new();
        mgr.apply(sample_spec()).await.unwrap();

        mgr.start("test-connector").await.unwrap();
        let status = mgr.status("test-connector").await.unwrap();
        assert_eq!(status.state, ConnectorRuntimeState::Running);

        mgr.stop("test-connector").await.unwrap();
        let status = mgr.status("test-connector").await.unwrap();
        assert_eq!(status.state, ConnectorRuntimeState::Stopped);
    }

    #[tokio::test]
    async fn test_pause_resume() {
        let mgr = ConnectorManager::new();
        mgr.apply(sample_spec()).await.unwrap();
        mgr.start("test-connector").await.unwrap();

        mgr.pause("test-connector").await.unwrap();
        let status = mgr.status("test-connector").await.unwrap();
        assert_eq!(status.state, ConnectorRuntimeState::Paused);

        mgr.resume("test-connector").await.unwrap();
        let status = mgr.status("test-connector").await.unwrap();
        assert_eq!(status.state, ConnectorRuntimeState::Running);
    }

    #[tokio::test]
    async fn test_restart_policy() {
        let mgr = ConnectorManager::new();
        let mut spec = sample_spec();
        spec.spec.restart_policy.max_restarts = 2;
        mgr.apply(spec).await.unwrap();

        // First error - restarts
        mgr.record_error("test-connector", "connection lost")
            .await
            .unwrap();
        let status = mgr.status("test-connector").await.unwrap();
        assert_eq!(status.state, ConnectorRuntimeState::Restarting);
        assert_eq!(status.restart_count, 1);

        // Second error - restarts again
        mgr.record_error("test-connector", "connection lost")
            .await
            .unwrap();
        let status = mgr.status("test-connector").await.unwrap();
        assert_eq!(status.state, ConnectorRuntimeState::Restarting);
        assert_eq!(status.restart_count, 2);

        // Third error - fails (exceeds max)
        mgr.record_error("test-connector", "connection lost")
            .await
            .unwrap();
        let status = mgr.status("test-connector").await.unwrap();
        assert_eq!(status.state, ConnectorRuntimeState::Failed);
    }

    #[tokio::test]
    async fn test_delete() {
        let mgr = ConnectorManager::new();
        mgr.apply(sample_spec()).await.unwrap();
        mgr.delete("test-connector").await.unwrap();
        assert!(mgr.list().await.is_empty());
    }

    #[tokio::test]
    async fn test_apply_yaml() {
        let mgr = ConnectorManager::new();
        let yaml = ConnectorManager::example_yaml("postgres-cdc");
        let conn = mgr.apply_yaml(&yaml).await.unwrap();
        assert_eq!(conn.spec.spec.connector_type, "postgres-cdc");
        assert_eq!(conn.spec.spec.direction, ConnectorDirection::Source);
    }

    #[tokio::test]
    async fn test_update_metrics() {
        let mgr = ConnectorManager::new();
        mgr.apply(sample_spec()).await.unwrap();
        mgr.update_metrics("test-connector", 100, 4096, 5.0)
            .await
            .unwrap();

        let status = mgr.status("test-connector").await.unwrap();
        assert_eq!(status.metrics.records_processed, 100);
        assert_eq!(status.metrics.bytes_processed, 4096);
    }

    #[test]
    fn test_connector_direction_display() {
        assert_eq!(ConnectorDirection::Source.to_string(), "source");
        assert_eq!(ConnectorDirection::Sink.to_string(), "sink");
    }

    #[test]
    fn test_runtime_state_display() {
        assert_eq!(ConnectorRuntimeState::Running.to_string(), "running");
        assert_eq!(ConnectorRuntimeState::Failed.to_string(), "failed");
        assert_eq!(ConnectorRuntimeState::Restarting.to_string(), "restarting");
    }

    #[test]
    fn test_example_yaml_postgres() {
        let yaml = ConnectorManager::example_yaml("postgres-cdc");
        assert!(yaml.contains("postgres-cdc"));
        assert!(yaml.contains("slot_name"));
    }

    #[test]
    fn test_example_yaml_elasticsearch() {
        let yaml = ConnectorManager::example_yaml("elasticsearch-sink");
        assert!(yaml.contains("elasticsearch"));
        assert!(yaml.contains("bulk_size"));
    }

    #[test]
    fn test_example_yaml_unknown() {
        let yaml = ConnectorManager::example_yaml("custom");
        assert!(yaml.contains("custom"));
    }

    #[tokio::test]
    async fn test_pause_non_running_fails() {
        let mgr = ConnectorManager::new();
        mgr.apply(sample_spec()).await.unwrap();
        // Connector is in Pending state, not Running
        assert!(mgr.pause("test-connector").await.is_err());
    }
}
