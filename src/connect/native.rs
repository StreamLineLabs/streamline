//! Native Connector SDK for Streamline
//!
//! This module provides a Rust-native connector framework compatible with Kafka Connect
//! concepts. It allows developers to build source and sink connectors in pure Rust,
//! without requiring a JVM or Kafka Connect worker process.
//!
//! # Stability
//!
//! **Experimental** - This module is under active development. The API may change
//! significantly in any version.
//!
//! # Architecture
//!
//! The Native Connector SDK consists of:
//! - [`NativeConnector`] trait: Base trait for all connectors (source and sink)
//! - [`SourceConnector`] trait: Extension for source connectors that poll external systems
//! - [`SinkConnector`] trait: Extension for sink connectors that write to external systems
//! - [`ConnectorRuntime`]: Manages the lifecycle of connector instances
//! - [`ConnectorFactory`] trait: Creates connector instances from configuration
//!
//! # Built-in Connectors
//!
//! - [`HttpSourceConnector`]: Polls an HTTP endpoint periodically and emits records
//! - [`ConsoleSinkConnector`]: Writes records to stdout/log for debugging
//!
//! # Example
//!
//! ```rust,ignore
//! use streamline::connect::native::{
//!     ConnectorRuntime, NativeConnectorConfig, HttpSourceConnectorFactory,
//! };
//!
//! let mut runtime = ConnectorRuntime::new();
//! runtime.register_connector_type(Box::new(HttpSourceConnectorFactory));
//!
//! let config = NativeConnectorConfig {
//!     name: "my-http-source".to_string(),
//!     connector_class: "HttpSourceConnector".to_string(),
//!     tasks_max: 1,
//!     properties: [("url".into(), "https://api.example.com/data".into())].into(),
//! };
//!
//! let id = runtime.create_connector(config).await?;
//! runtime.start_connector(&id).await?;
//! ```

use crate::error::{Result, StreamlineError};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

// ---------------------------------------------------------------------------
// Core types
// ---------------------------------------------------------------------------

/// Type of a native connector (source or sink).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NativeConnectorType {
    /// Reads from an external system and produces records into Streamline topics.
    Source,
    /// Consumes records from Streamline topics and writes to an external system.
    Sink,
}

impl std::fmt::Display for NativeConnectorType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NativeConnectorType::Source => write!(f, "source"),
            NativeConnectorType::Sink => write!(f, "sink"),
        }
    }
}

/// Configuration type for a configuration definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConfigType {
    /// String value
    String,
    /// Integer value (i32)
    Int,
    /// Long integer value (i64)
    Long,
    /// Boolean value
    Boolean,
    /// Comma-separated list of strings
    List,
    /// Sensitive string value (masked in logs)
    Password,
}

impl std::fmt::Display for ConfigType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigType::String => write!(f, "STRING"),
            ConfigType::Int => write!(f, "INT"),
            ConfigType::Long => write!(f, "LONG"),
            ConfigType::Boolean => write!(f, "BOOLEAN"),
            ConfigType::List => write!(f, "LIST"),
            ConfigType::Password => write!(f, "PASSWORD"),
        }
    }
}

/// Importance level for a configuration key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConfigImportance {
    /// Must be set for the connector to function correctly.
    High,
    /// Important for production usage but has a sensible default.
    Medium,
    /// Optional tuning parameter.
    Low,
}

/// Definition of a single configuration key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigDef {
    /// Configuration key name (e.g. "url", "poll.interval.ms").
    pub name: String,
    /// Value type.
    pub config_type: ConfigType,
    /// Default value if not provided by the user.
    pub default_value: Option<String>,
    /// Importance level.
    pub importance: ConfigImportance,
    /// Human-readable documentation for the key.
    pub documentation: String,
    /// Whether the key is required.
    pub required: bool,
}

impl ConfigDef {
    /// Create a new required configuration definition.
    pub fn required(
        name: impl Into<String>,
        config_type: ConfigType,
        documentation: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            config_type,
            default_value: None,
            importance: ConfigImportance::High,
            documentation: documentation.into(),
            required: true,
        }
    }

    /// Create a new optional configuration definition with a default value.
    pub fn optional(
        name: impl Into<String>,
        config_type: ConfigType,
        default_value: impl Into<String>,
        documentation: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            config_type,
            default_value: Some(default_value.into()),
            importance: ConfigImportance::Medium,
            documentation: documentation.into(),
            required: false,
        }
    }

    /// Set the importance level.
    pub fn with_importance(mut self, importance: ConfigImportance) -> Self {
        self.importance = importance;
        self
    }
}

/// Result of validating a single configuration key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigValidation {
    /// Name of the configuration key.
    pub name: String,
    /// Whether the value is valid.
    pub is_valid: bool,
    /// Error messages (empty if valid).
    pub errors: Vec<String>,
}

/// Configuration for creating a native connector instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NativeConnectorConfig {
    /// Unique connector name.
    pub name: String,
    /// Connector class identifier (must match a registered factory).
    pub connector_class: String,
    /// Maximum number of tasks to run in parallel.
    pub tasks_max: u32,
    /// Connector-specific properties.
    pub properties: HashMap<String, String>,
}

// ---------------------------------------------------------------------------
// Record types
// ---------------------------------------------------------------------------

/// Offset tracking for a source connector.
pub type SourceOffset = HashMap<String, String>;

/// A record produced by a source connector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceRecord {
    /// Partition identifier in the external source system (e.g. file path, table name).
    pub source_partition: HashMap<String, String>,
    /// Offset within the source partition (e.g. file position, row ID).
    pub source_offset: HashMap<String, String>,
    /// Target Streamline topic.
    pub topic: String,
    /// Optional target partition. `None` uses the default partitioner.
    pub partition: Option<i32>,
    /// Optional record key.
    pub key: Option<Vec<u8>>,
    /// Record value (payload).
    pub value: Vec<u8>,
    /// Optional headers.
    pub headers: Vec<(String, Vec<u8>)>,
    /// Optional timestamp in milliseconds since epoch.
    pub timestamp: Option<i64>,
}

/// A record delivered to a sink connector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SinkRecord {
    /// Source topic.
    pub topic: String,
    /// Source partition.
    pub partition: i32,
    /// Offset within the partition.
    pub offset: i64,
    /// Optional record key.
    pub key: Option<Vec<u8>>,
    /// Optional record value.
    pub value: Option<Vec<u8>>,
    /// Headers.
    pub headers: Vec<(String, Vec<u8>)>,
    /// Timestamp in milliseconds since epoch.
    pub timestamp: i64,
}

// ---------------------------------------------------------------------------
// Connector traits
// ---------------------------------------------------------------------------

/// Base trait for all native connectors.
///
/// Every connector -- source or sink -- must implement this trait. It provides
/// the common lifecycle methods (start/stop) and introspection (name, type,
/// config definitions, validation).
#[async_trait]
pub trait NativeConnector: Send + Sync {
    /// Returns the unique name of this connector instance.
    fn name(&self) -> &str;

    /// Returns the connector type (source or sink).
    fn connector_type(&self) -> NativeConnectorType;

    /// Returns the set of configuration definitions understood by this connector.
    fn config_definitions(&self) -> Vec<ConfigDef>;

    /// Validates the given configuration and returns per-key validation results.
    async fn validate_config(
        &self,
        config: &NativeConnectorConfig,
    ) -> Result<Vec<ConfigValidation>>;

    /// Starts the connector with the given configuration.
    async fn start(&mut self, config: NativeConnectorConfig) -> Result<()>;

    /// Stops the connector and releases resources.
    async fn stop(&mut self) -> Result<()>;
}

/// Extension trait for source connectors.
///
/// A source connector polls an external system for new data and returns
/// [`SourceRecord`] values that will be written to Streamline topics.
#[async_trait]
pub trait SourceConnectorTrait: NativeConnector {
    /// Polls the external system for new records.
    ///
    /// Implementations should return an empty `Vec` if no records are available
    /// yet rather than blocking indefinitely.
    async fn poll(&mut self) -> Result<Vec<SourceRecord>>;

    /// Acknowledges that the given offsets have been durably committed.
    ///
    /// The runtime calls this after records returned by [`poll`](SourceConnectorTrait::poll)
    /// have been successfully written to Streamline.
    fn ack(&mut self, offsets: &[SourceOffset]);
}

/// Extension trait for sink connectors.
///
/// A sink connector receives records from Streamline topics and writes them
/// to an external system.
#[async_trait]
pub trait SinkConnectorTrait: NativeConnector {
    /// Delivers a batch of records to the external system.
    async fn put(&mut self, records: Vec<SinkRecord>) -> Result<()>;

    /// Flushes any buffered data to the external system.
    ///
    /// The runtime calls this periodically and before stopping the connector.
    async fn flush(&mut self) -> Result<()>;
}

// ---------------------------------------------------------------------------
// Connector factory
// ---------------------------------------------------------------------------

/// Factory for creating connector instances.
///
/// Each connector implementation should provide a corresponding factory so
/// that the [`ConnectorRuntime`] can instantiate connectors from configuration.
pub trait ConnectorFactory: Send + Sync {
    /// Creates a new, unconfigured connector instance.
    fn create(&self) -> Box<dyn NativeConnector>;

    /// Returns the connector class name that this factory handles.
    fn connector_type_name(&self) -> &str;
}

// ---------------------------------------------------------------------------
// Connector status / info
// ---------------------------------------------------------------------------

/// Runtime state of a connector.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum NativeConnectorState {
    /// Connector has been created but not yet started.
    Unassigned,
    /// Connector is actively running.
    Running,
    /// Connector is temporarily paused.
    Paused,
    /// Connector has encountered a fatal error.
    Failed,
}

impl std::fmt::Display for NativeConnectorState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NativeConnectorState::Unassigned => write!(f, "UNASSIGNED"),
            NativeConnectorState::Running => write!(f, "RUNNING"),
            NativeConnectorState::Paused => write!(f, "PAUSED"),
            NativeConnectorState::Failed => write!(f, "FAILED"),
        }
    }
}

/// Status of a single task within a connector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskStatus {
    /// Task ID (0-indexed).
    pub id: u32,
    /// Current state of the task.
    pub state: NativeConnectorState,
    /// Worker that is running this task.
    pub worker_id: String,
    /// Error trace if the task is in a failed state.
    pub trace: Option<String>,
}

/// Full status of a connector including its tasks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorStatus {
    /// Connector name.
    pub name: String,
    /// Connector type.
    pub connector_type: NativeConnectorType,
    /// Current overall state.
    pub state: NativeConnectorState,
    /// Worker hosting the connector.
    pub worker_id: String,
    /// Task statuses.
    pub tasks: Vec<TaskStatus>,
    /// Configuration properties (sensitive values masked).
    pub config: HashMap<String, String>,
}

/// Summary information for listing connectors.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorInfo {
    /// Connector name.
    pub name: String,
    /// Connector type.
    pub connector_type: NativeConnectorType,
    /// Current state.
    pub state: NativeConnectorState,
    /// Number of tasks.
    pub tasks_count: u32,
}

// ---------------------------------------------------------------------------
// Connector runtime
// ---------------------------------------------------------------------------

/// Internal representation of a running connector.
struct ManagedConnector {
    /// The connector instance.
    connector: Box<dyn NativeConnector>,
    /// The configuration used to create the connector.
    config: NativeConnectorConfig,
    /// Current state.
    state: NativeConnectorState,
    /// Error message if the connector is in a failed state.
    error: Option<String>,
    /// Worker identifier.
    worker_id: String,
}

/// Manages the lifecycle of native connector instances.
///
/// The runtime keeps track of registered connector factories and running
/// connector instances. It provides create / start / stop / restart / delete
/// operations that mirror the Kafka Connect REST API semantics.
pub struct ConnectorRuntime {
    /// Registered connector factories keyed by connector class name.
    factories: Arc<RwLock<HashMap<String, Box<dyn ConnectorFactory>>>>,
    /// Running connectors keyed by connector name.
    connectors: Arc<RwLock<HashMap<String, ManagedConnector>>>,
    /// Worker identifier for this runtime.
    worker_id: String,
}

impl ConnectorRuntime {
    /// Creates a new connector runtime.
    pub fn new() -> Self {
        let hostname = std::env::var("HOSTNAME").unwrap_or_else(|_| "localhost".to_string());
        Self {
            factories: Arc::new(RwLock::new(HashMap::new())),
            connectors: Arc::new(RwLock::new(HashMap::new())),
            worker_id: format!("{}:8084", hostname),
        }
    }

    /// Registers a connector factory.
    ///
    /// After registration, connectors of this type can be created via
    /// [`create_connector`](Self::create_connector).
    pub async fn register_connector_type(&self, factory: Box<dyn ConnectorFactory>) {
        let name = factory.connector_type_name().to_string();
        let mut factories = self.factories.write().await;
        info!(connector_class = %name, "Registered native connector type");
        factories.insert(name, factory);
    }

    /// Creates a new connector from the given configuration.
    ///
    /// The connector class must have been previously registered via
    /// [`register_connector_type`](Self::register_connector_type).
    ///
    /// Returns the connector name on success.
    pub async fn create_connector(&self, config: NativeConnectorConfig) -> Result<String> {
        // Check for duplicate names.
        {
            let connectors = self.connectors.read().await;
            if connectors.contains_key(&config.name) {
                return Err(StreamlineError::Config(format!(
                    "Connector already exists: {}",
                    config.name
                )));
            }
        }

        // Look up the factory.
        let connector = {
            let factories = self.factories.read().await;
            let factory = factories.get(&config.connector_class).ok_or_else(|| {
                StreamlineError::Config(format!(
                    "Unknown connector class: {}",
                    config.connector_class
                ))
            })?;
            factory.create()
        };

        // Validate configuration.
        let validations = connector.validate_config(&config).await?;
        let errors: Vec<_> = validations
            .iter()
            .filter(|v| !v.is_valid)
            .flat_map(|v| v.errors.clone())
            .collect();
        if !errors.is_empty() {
            return Err(StreamlineError::Config(format!(
                "Invalid connector configuration: {}",
                errors.join("; ")
            )));
        }

        let name = config.name.clone();
        let managed = ManagedConnector {
            connector,
            config,
            state: NativeConnectorState::Unassigned,
            error: None,
            worker_id: self.worker_id.clone(),
        };

        let mut connectors = self.connectors.write().await;
        connectors.insert(name.clone(), managed);

        info!(connector = %name, "Created native connector");
        Ok(name)
    }

    /// Deletes a connector.
    ///
    /// If the connector is running it will be stopped first.
    pub async fn delete_connector(&self, name: &str) -> Result<()> {
        let mut connectors = self.connectors.write().await;
        let managed = connectors
            .get_mut(name)
            .ok_or_else(|| StreamlineError::Config(format!("Connector not found: {}", name)))?;

        if managed.state == NativeConnectorState::Running {
            managed.connector.stop().await?;
        }

        connectors.remove(name);
        info!(connector = %name, "Deleted native connector");
        Ok(())
    }

    /// Starts a connector.
    pub async fn start_connector(&self, name: &str) -> Result<()> {
        let mut connectors = self.connectors.write().await;
        let managed = connectors
            .get_mut(name)
            .ok_or_else(|| StreamlineError::Config(format!("Connector not found: {}", name)))?;

        if managed.state == NativeConnectorState::Running {
            debug!(connector = %name, "Connector already running");
            return Ok(());
        }

        let config = managed.config.clone();
        match managed.connector.start(config).await {
            Ok(()) => {
                managed.state = NativeConnectorState::Running;
                managed.error = None;
                info!(connector = %name, "Started native connector");
                Ok(())
            }
            Err(e) => {
                managed.state = NativeConnectorState::Failed;
                managed.error = Some(e.to_string());
                warn!(connector = %name, error = %e, "Failed to start native connector");
                Err(e)
            }
        }
    }

    /// Stops a running connector.
    pub async fn stop_connector(&self, name: &str) -> Result<()> {
        let mut connectors = self.connectors.write().await;
        let managed = connectors
            .get_mut(name)
            .ok_or_else(|| StreamlineError::Config(format!("Connector not found: {}", name)))?;

        if managed.state != NativeConnectorState::Running {
            debug!(connector = %name, "Connector is not running");
            return Ok(());
        }

        match managed.connector.stop().await {
            Ok(()) => {
                managed.state = NativeConnectorState::Paused;
                info!(connector = %name, "Stopped native connector");
                Ok(())
            }
            Err(e) => {
                managed.state = NativeConnectorState::Failed;
                managed.error = Some(e.to_string());
                warn!(connector = %name, error = %e, "Error stopping native connector");
                Err(e)
            }
        }
    }

    /// Restarts a connector (stop then start).
    pub async fn restart_connector(&self, name: &str) -> Result<()> {
        // Stop ignoring "not running" is fine.
        let _ = self.stop_connector(name).await;
        self.start_connector(name).await
    }

    /// Returns summary information for all connectors.
    pub async fn list_connectors(&self) -> Vec<ConnectorInfo> {
        let connectors = self.connectors.read().await;
        connectors
            .values()
            .map(|m| ConnectorInfo {
                name: m.config.name.clone(),
                connector_type: m.connector.connector_type(),
                state: m.state,
                tasks_count: m.config.tasks_max,
            })
            .collect()
    }

    /// Returns the full status of a connector.
    pub async fn get_connector_status(&self, name: &str) -> Result<ConnectorStatus> {
        let connectors = self.connectors.read().await;
        let managed = connectors
            .get(name)
            .ok_or_else(|| StreamlineError::Config(format!("Connector not found: {}", name)))?;

        let tasks: Vec<TaskStatus> = (0..managed.config.tasks_max)
            .map(|i| TaskStatus {
                id: i,
                state: managed.state,
                worker_id: managed.worker_id.clone(),
                trace: managed.error.clone(),
            })
            .collect();

        Ok(ConnectorStatus {
            name: managed.config.name.clone(),
            connector_type: managed.connector.connector_type(),
            state: managed.state,
            worker_id: managed.worker_id.clone(),
            tasks,
            config: managed.config.properties.clone(),
        })
    }
}

impl Default for ConnectorRuntime {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Built-in: HttpSourceConnector
// ---------------------------------------------------------------------------

/// A source connector that polls an HTTP endpoint periodically.
///
/// # Configuration
///
/// | Key              | Type   | Required | Default | Description                    |
/// |------------------|--------|----------|---------|--------------------------------|
/// | `url`            | String | yes      |         | HTTP endpoint URL              |
/// | `method`         | String | no       | GET     | HTTP method                    |
/// | `interval_ms`    | Long   | no       | 5000    | Polling interval (ms)          |
/// | `headers`        | String | no       |         | Comma-separated `key:value` headers |
pub struct HttpSourceConnector {
    name: String,
    config: Option<NativeConnectorConfig>,
    poll_count: u64,
    running: bool,
}

impl HttpSourceConnector {
    /// Creates a new (unconfigured) HTTP source connector.
    pub fn new() -> Self {
        Self {
            name: String::new(),
            config: None,
            poll_count: 0,
            running: false,
        }
    }

    fn url(&self) -> Option<&str> {
        self.config
            .as_ref()
            .and_then(|c| c.properties.get("url"))
            .map(|s| s.as_str())
    }

    fn method(&self) -> &str {
        self.config
            .as_ref()
            .and_then(|c| c.properties.get("method"))
            .map(|s| s.as_str())
            .unwrap_or("GET")
    }

    fn interval_ms(&self) -> u64 {
        self.config
            .as_ref()
            .and_then(|c| c.properties.get("interval_ms"))
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(5000)
    }
}

impl Default for HttpSourceConnector {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl NativeConnector for HttpSourceConnector {
    fn name(&self) -> &str {
        &self.name
    }

    fn connector_type(&self) -> NativeConnectorType {
        NativeConnectorType::Source
    }

    fn config_definitions(&self) -> Vec<ConfigDef> {
        vec![
            ConfigDef::required("url", ConfigType::String, "HTTP endpoint URL to poll"),
            ConfigDef::optional(
                "method",
                ConfigType::String,
                "GET",
                "HTTP method (GET, POST, etc.)",
            )
            .with_importance(ConfigImportance::Low),
            ConfigDef::optional(
                "interval_ms",
                ConfigType::Long,
                "5000",
                "Polling interval in milliseconds",
            ),
            ConfigDef::optional(
                "headers",
                ConfigType::String,
                "",
                "Comma-separated key:value HTTP headers",
            )
            .with_importance(ConfigImportance::Low),
        ]
    }

    async fn validate_config(
        &self,
        config: &NativeConnectorConfig,
    ) -> Result<Vec<ConfigValidation>> {
        let mut results = Vec::new();

        // Validate url
        let url_valid = config.properties.contains_key("url");
        results.push(ConfigValidation {
            name: "url".to_string(),
            is_valid: url_valid,
            errors: if url_valid {
                vec![]
            } else {
                vec!["Missing required configuration: url".to_string()]
            },
        });

        // Validate interval_ms if present
        if let Some(interval) = config.properties.get("interval_ms") {
            let valid = interval.parse::<u64>().is_ok();
            results.push(ConfigValidation {
                name: "interval_ms".to_string(),
                is_valid: valid,
                errors: if valid {
                    vec![]
                } else {
                    vec![format!("Invalid interval_ms value: {}", interval)]
                },
            });
        }

        // Validate method if present
        if let Some(method) = config.properties.get("method") {
            let valid = matches!(
                method.to_uppercase().as_str(),
                "GET" | "POST" | "PUT" | "DELETE" | "PATCH" | "HEAD"
            );
            results.push(ConfigValidation {
                name: "method".to_string(),
                is_valid: valid,
                errors: if valid {
                    vec![]
                } else {
                    vec![format!("Invalid HTTP method: {}", method)]
                },
            });
        }

        Ok(results)
    }

    async fn start(&mut self, config: NativeConnectorConfig) -> Result<()> {
        self.name = config.name.clone();
        self.config = Some(config);
        self.running = true;
        self.poll_count = 0;
        info!(
            connector = %self.name,
            url = ?self.url(),
            method = %self.method(),
            interval_ms = %self.interval_ms(),
            "HttpSourceConnector started"
        );
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        self.running = false;
        info!(connector = %self.name, polls = %self.poll_count, "HttpSourceConnector stopped");
        Ok(())
    }
}

#[async_trait]
impl SourceConnectorTrait for HttpSourceConnector {
    async fn poll(&mut self) -> Result<Vec<SourceRecord>> {
        if !self.running {
            return Err(StreamlineError::Config(
                "Connector is not running".to_string(),
            ));
        }

        let url = self
            .url()
            .ok_or_else(|| StreamlineError::Config("Missing url configuration".into()))?
            .to_string();

        self.poll_count += 1;
        let poll_id = self.poll_count;

        debug!(
            connector = %self.name,
            url = %url,
            poll_count = %poll_id,
            "HttpSourceConnector polling"
        );

        // In a real implementation this would perform an HTTP request using reqwest.
        // For the SDK skeleton we produce a placeholder record indicating a poll happened.
        let mut source_partition = HashMap::new();
        source_partition.insert("url".to_string(), url.clone());

        let mut source_offset = HashMap::new();
        source_offset.insert("poll_count".to_string(), poll_id.to_string());

        let topic = self
            .config
            .as_ref()
            .and_then(|c| c.properties.get("topic"))
            .cloned()
            .unwrap_or_else(|| format!("{}-records", self.name));

        let record = SourceRecord {
            source_partition,
            source_offset,
            topic,
            partition: None,
            key: None,
            value: format!("polled {} (count={})", url, poll_id).into_bytes(),
            headers: vec![("http.method".to_string(), self.method().as_bytes().to_vec())],
            timestamp: Some(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as i64)
                    .unwrap_or(0),
            ),
        };

        Ok(vec![record])
    }

    fn ack(&mut self, offsets: &[SourceOffset]) {
        debug!(
            connector = %self.name,
            ack_count = offsets.len(),
            "HttpSourceConnector acknowledged offsets"
        );
    }
}

/// Factory for [`HttpSourceConnector`].
pub struct HttpSourceConnectorFactory;

impl ConnectorFactory for HttpSourceConnectorFactory {
    fn create(&self) -> Box<dyn NativeConnector> {
        Box::new(HttpSourceConnector::new())
    }

    fn connector_type_name(&self) -> &str {
        "HttpSourceConnector"
    }
}

// ---------------------------------------------------------------------------
// Built-in: ConsoleSinkConnector
// ---------------------------------------------------------------------------

/// A sink connector that writes records to stdout / tracing log.
///
/// Useful for debugging and development. Each record is printed as a
/// human-readable line.
///
/// # Configuration
///
/// | Key           | Type    | Required | Default | Description                     |
/// |---------------|---------|----------|---------|---------------------------------|
/// | `print_key`   | Boolean | no       | false   | Whether to include record keys  |
/// | `prefix`      | String  | no       |         | Line prefix for each record     |
pub struct ConsoleSinkConnector {
    name: String,
    config: Option<NativeConnectorConfig>,
    records_written: u64,
    running: bool,
}

impl ConsoleSinkConnector {
    /// Creates a new (unconfigured) console sink connector.
    pub fn new() -> Self {
        Self {
            name: String::new(),
            config: None,
            records_written: 0,
            running: false,
        }
    }

    fn print_key(&self) -> bool {
        self.config
            .as_ref()
            .and_then(|c| c.properties.get("print_key"))
            .map(|s| s == "true")
            .unwrap_or(false)
    }

    fn prefix(&self) -> &str {
        self.config
            .as_ref()
            .and_then(|c| c.properties.get("prefix"))
            .map(|s| s.as_str())
            .unwrap_or("")
    }
}

impl Default for ConsoleSinkConnector {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl NativeConnector for ConsoleSinkConnector {
    fn name(&self) -> &str {
        &self.name
    }

    fn connector_type(&self) -> NativeConnectorType {
        NativeConnectorType::Sink
    }

    fn config_definitions(&self) -> Vec<ConfigDef> {
        vec![
            ConfigDef::optional(
                "print_key",
                ConfigType::Boolean,
                "false",
                "Whether to print record keys",
            )
            .with_importance(ConfigImportance::Low),
            ConfigDef::optional(
                "prefix",
                ConfigType::String,
                "",
                "Line prefix for each record",
            )
            .with_importance(ConfigImportance::Low),
        ]
    }

    async fn validate_config(
        &self,
        config: &NativeConnectorConfig,
    ) -> Result<Vec<ConfigValidation>> {
        let mut results = Vec::new();

        // Validate print_key if present
        if let Some(pk) = config.properties.get("print_key") {
            let valid = pk == "true" || pk == "false";
            results.push(ConfigValidation {
                name: "print_key".to_string(),
                is_valid: valid,
                errors: if valid {
                    vec![]
                } else {
                    vec![format!("print_key must be 'true' or 'false', got '{}'", pk)]
                },
            });
        }

        Ok(results)
    }

    async fn start(&mut self, config: NativeConnectorConfig) -> Result<()> {
        self.name = config.name.clone();
        self.config = Some(config);
        self.running = true;
        self.records_written = 0;
        info!(connector = %self.name, "ConsoleSinkConnector started");
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        self.running = false;
        info!(
            connector = %self.name,
            records_written = %self.records_written,
            "ConsoleSinkConnector stopped"
        );
        Ok(())
    }
}

#[async_trait]
impl SinkConnectorTrait for ConsoleSinkConnector {
    async fn put(&mut self, records: Vec<SinkRecord>) -> Result<()> {
        if !self.running {
            return Err(StreamlineError::Config(
                "Connector is not running".to_string(),
            ));
        }

        let prefix = self.prefix().to_string();
        let print_key = self.print_key();

        for record in &records {
            let value_str = record
                .value
                .as_ref()
                .map(|v| String::from_utf8_lossy(v).to_string())
                .unwrap_or_else(|| "<null>".to_string());

            if print_key {
                let key_str = record
                    .key
                    .as_ref()
                    .map(|k| String::from_utf8_lossy(k).to_string())
                    .unwrap_or_else(|| "<null>".to_string());
                info!(
                    "{}[{}:{}:{}] key={} value={}",
                    prefix, record.topic, record.partition, record.offset, key_str, value_str
                );
            } else {
                info!(
                    "{}[{}:{}:{}] {}",
                    prefix, record.topic, record.partition, record.offset, value_str
                );
            }

            self.records_written += 1;
        }

        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        debug!(
            connector = %self.name,
            records_written = %self.records_written,
            "ConsoleSinkConnector flushed"
        );
        Ok(())
    }
}

/// Factory for [`ConsoleSinkConnector`].
pub struct ConsoleSinkConnectorFactory;

impl ConnectorFactory for ConsoleSinkConnectorFactory {
    fn create(&self) -> Box<dyn NativeConnector> {
        Box::new(ConsoleSinkConnector::new())
    }

    fn connector_type_name(&self) -> &str {
        "ConsoleSinkConnector"
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ---- Helper: build a minimal NativeConnectorConfig ----

    fn http_source_config(name: &str, url: &str) -> NativeConnectorConfig {
        let mut props = HashMap::new();
        props.insert("url".to_string(), url.to_string());
        NativeConnectorConfig {
            name: name.to_string(),
            connector_class: "HttpSourceConnector".to_string(),
            tasks_max: 1,
            properties: props,
        }
    }

    fn console_sink_config(name: &str) -> NativeConnectorConfig {
        NativeConnectorConfig {
            name: name.to_string(),
            connector_class: "ConsoleSinkConnector".to_string(),
            tasks_max: 1,
            properties: HashMap::new(),
        }
    }

    // ---- 1. Config validation: required field missing ----

    #[tokio::test]
    async fn test_http_source_validate_missing_url() {
        let connector = HttpSourceConnector::new();
        let config = NativeConnectorConfig {
            name: "test".to_string(),
            connector_class: "HttpSourceConnector".to_string(),
            tasks_max: 1,
            properties: HashMap::new(), // no url
        };

        let validations = connector.validate_config(&config).await.unwrap();
        let url_result = validations.iter().find(|v| v.name == "url").unwrap();
        assert!(!url_result.is_valid);
        assert!(!url_result.errors.is_empty());
    }

    // ---- 2. Config validation: valid config ----

    #[tokio::test]
    async fn test_http_source_validate_valid_config() {
        let connector = HttpSourceConnector::new();
        let config = http_source_config("test", "https://example.com/data");

        let validations = connector.validate_config(&config).await.unwrap();
        for v in &validations {
            assert!(v.is_valid, "Config key '{}' should be valid", v.name);
        }
    }

    // ---- 3. Config validation: invalid method ----

    #[tokio::test]
    async fn test_http_source_validate_invalid_method() {
        let connector = HttpSourceConnector::new();
        let mut props = HashMap::new();
        props.insert("url".to_string(), "https://example.com".to_string());
        props.insert("method".to_string(), "INVALID".to_string());
        let config = NativeConnectorConfig {
            name: "test".to_string(),
            connector_class: "HttpSourceConnector".to_string(),
            tasks_max: 1,
            properties: props,
        };

        let validations = connector.validate_config(&config).await.unwrap();
        let method_result = validations.iter().find(|v| v.name == "method").unwrap();
        assert!(!method_result.is_valid);
    }

    // ---- 4. HttpSourceConnector lifecycle (start / poll / ack / stop) ----

    #[tokio::test]
    async fn test_http_source_connector_lifecycle() {
        let mut connector = HttpSourceConnector::new();
        let config = http_source_config("http-test", "https://example.com/api");

        // Start
        connector.start(config).await.unwrap();
        assert_eq!(connector.name(), "http-test");
        assert_eq!(connector.connector_type(), NativeConnectorType::Source);
        assert!(connector.running);

        // Poll
        let records = connector.poll().await.unwrap();
        assert_eq!(records.len(), 1);
        assert!(records[0].timestamp.is_some());
        assert_eq!(
            records[0].source_partition.get("url").unwrap(),
            "https://example.com/api"
        );

        // Ack
        let offsets: Vec<SourceOffset> = records.iter().map(|r| r.source_offset.clone()).collect();
        connector.ack(&offsets);

        // Stop
        connector.stop().await.unwrap();
        assert!(!connector.running);
    }

    // ---- 5. ConsoleSinkConnector lifecycle (start / put / flush / stop) ----

    #[tokio::test]
    async fn test_console_sink_connector_lifecycle() {
        let mut connector = ConsoleSinkConnector::new();
        let config = console_sink_config("console-test");

        // Start
        connector.start(config).await.unwrap();
        assert_eq!(connector.name(), "console-test");
        assert_eq!(connector.connector_type(), NativeConnectorType::Sink);

        // Put
        let record = SinkRecord {
            topic: "test-topic".to_string(),
            partition: 0,
            offset: 42,
            key: Some(b"key-1".to_vec()),
            value: Some(b"hello world".to_vec()),
            headers: vec![],
            timestamp: 1700000000000,
        };
        connector.put(vec![record]).await.unwrap();
        assert_eq!(connector.records_written, 1);

        // Flush
        connector.flush().await.unwrap();

        // Stop
        connector.stop().await.unwrap();
        assert!(!connector.running);
    }

    // ---- 6. SourceRecord creation ----

    #[tokio::test]
    async fn test_source_record_creation() {
        let record = SourceRecord {
            source_partition: [("table".into(), "users".into())].into_iter().collect(),
            source_offset: [("id".into(), "100".into())].into_iter().collect(),
            topic: "users-cdc".to_string(),
            partition: Some(0),
            key: Some(b"user-100".to_vec()),
            value: b"{\"name\":\"alice\"}".to_vec(),
            headers: vec![("source".into(), b"postgres".to_vec())],
            timestamp: Some(1700000000000),
        };

        assert_eq!(record.topic, "users-cdc");
        assert_eq!(record.partition, Some(0));
        assert_eq!(record.key.as_ref().unwrap(), b"user-100");
        assert_eq!(record.headers.len(), 1);
        assert_eq!(record.source_partition.get("table").unwrap(), "users");
    }

    // ---- 7. SinkRecord creation ----

    #[test]
    fn test_sink_record_creation() {
        let record = SinkRecord {
            topic: "events".to_string(),
            partition: 3,
            offset: 999,
            key: None,
            value: Some(b"payload".to_vec()),
            headers: vec![
                ("trace-id".into(), b"abc-123".to_vec()),
                ("content-type".into(), b"application/json".to_vec()),
            ],
            timestamp: 1700000000000,
        };

        assert_eq!(record.topic, "events");
        assert_eq!(record.partition, 3);
        assert_eq!(record.offset, 999);
        assert!(record.key.is_none());
        assert_eq!(record.headers.len(), 2);
    }

    // ---- 8. ConnectorRuntime: register, create, start, stop, delete ----

    #[tokio::test]
    async fn test_connector_runtime_full_lifecycle() {
        let runtime = ConnectorRuntime::new();

        // Register factories
        runtime
            .register_connector_type(Box::new(HttpSourceConnectorFactory))
            .await;
        runtime
            .register_connector_type(Box::new(ConsoleSinkConnectorFactory))
            .await;

        // Create a connector
        let config = http_source_config("my-http", "https://api.test.com/data");
        let name = runtime.create_connector(config).await.unwrap();
        assert_eq!(name, "my-http");

        // List connectors
        let list = runtime.list_connectors().await;
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].name, "my-http");
        assert_eq!(list[0].state, NativeConnectorState::Unassigned);

        // Start
        runtime.start_connector("my-http").await.unwrap();
        let status = runtime.get_connector_status("my-http").await.unwrap();
        assert_eq!(status.state, NativeConnectorState::Running);
        assert_eq!(status.connector_type, NativeConnectorType::Source);

        // Stop
        runtime.stop_connector("my-http").await.unwrap();
        let status = runtime.get_connector_status("my-http").await.unwrap();
        assert_eq!(status.state, NativeConnectorState::Paused);

        // Delete
        runtime.delete_connector("my-http").await.unwrap();
        let list = runtime.list_connectors().await;
        assert!(list.is_empty());
    }

    // ---- 9. ConnectorRuntime: duplicate connector name ----

    #[tokio::test]
    async fn test_connector_runtime_duplicate_name() {
        let runtime = ConnectorRuntime::new();
        runtime
            .register_connector_type(Box::new(ConsoleSinkConnectorFactory))
            .await;

        let config = console_sink_config("dup-test");
        runtime.create_connector(config.clone()).await.unwrap();

        // Creating a connector with the same name should fail.
        let result = runtime.create_connector(config).await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("already exists"));
    }

    // ---- 10. ConnectorRuntime: unknown connector class ----

    #[tokio::test]
    async fn test_connector_runtime_unknown_class() {
        let runtime = ConnectorRuntime::new();

        let config = NativeConnectorConfig {
            name: "bad-connector".to_string(),
            connector_class: "NonExistentConnector".to_string(),
            tasks_max: 1,
            properties: HashMap::new(),
        };

        let result = runtime.create_connector(config).await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Unknown connector class"));
    }

    // ---- 11. ConnectorRuntime: status of unknown connector ----

    #[tokio::test]
    async fn test_connector_runtime_status_not_found() {
        let runtime = ConnectorRuntime::new();
        let result = runtime.get_connector_status("ghost").await;
        assert!(result.is_err());
    }

    // ---- 12. ConnectorRuntime: restart ----

    #[tokio::test]
    async fn test_connector_runtime_restart() {
        let runtime = ConnectorRuntime::new();
        runtime
            .register_connector_type(Box::new(HttpSourceConnectorFactory))
            .await;

        let config = http_source_config("restart-me", "https://example.com");
        runtime.create_connector(config).await.unwrap();
        runtime.start_connector("restart-me").await.unwrap();

        // Restart
        runtime.restart_connector("restart-me").await.unwrap();
        let status = runtime.get_connector_status("restart-me").await.unwrap();
        assert_eq!(status.state, NativeConnectorState::Running);
    }

    // ---- 13. ConfigDef builders ----

    #[test]
    fn test_config_def_required() {
        let def = ConfigDef::required("my.key", ConfigType::String, "Some doc");
        assert_eq!(def.name, "my.key");
        assert!(def.required);
        assert_eq!(def.importance, ConfigImportance::High);
        assert!(def.default_value.is_none());
    }

    // ---- 14. ConfigDef optional with importance ----

    #[test]
    fn test_config_def_optional_with_importance() {
        let def = ConfigDef::optional("batch.size", ConfigType::Int, "100", "Batch size")
            .with_importance(ConfigImportance::Low);
        assert_eq!(def.name, "batch.size");
        assert!(!def.required);
        assert_eq!(def.default_value, Some("100".to_string()));
        assert_eq!(def.importance, ConfigImportance::Low);
    }

    // ---- 15. NativeConnectorType display ----

    #[test]
    fn test_connector_type_display() {
        assert_eq!(format!("{}", NativeConnectorType::Source), "source");
        assert_eq!(format!("{}", NativeConnectorType::Sink), "sink");
    }

    // ---- 16. NativeConnectorState display ----

    #[test]
    fn test_connector_state_display() {
        assert_eq!(format!("{}", NativeConnectorState::Running), "RUNNING");
        assert_eq!(format!("{}", NativeConnectorState::Failed), "FAILED");
        assert_eq!(format!("{}", NativeConnectorState::Paused), "PAUSED");
        assert_eq!(
            format!("{}", NativeConnectorState::Unassigned),
            "UNASSIGNED"
        );
    }

    // ---- 17. ConsoleSink validate_config with invalid print_key ----

    #[tokio::test]
    async fn test_console_sink_validate_invalid_print_key() {
        let connector = ConsoleSinkConnector::new();
        let mut props = HashMap::new();
        props.insert("print_key".to_string(), "yes".to_string());
        let config = NativeConnectorConfig {
            name: "test".to_string(),
            connector_class: "ConsoleSinkConnector".to_string(),
            tasks_max: 1,
            properties: props,
        };

        let validations = connector.validate_config(&config).await.unwrap();
        let pk_result = validations.iter().find(|v| v.name == "print_key").unwrap();
        assert!(!pk_result.is_valid);
        assert!(pk_result.errors[0].contains("true"));
    }

    // ---- 18. HttpSourceConnector: poll when not running ----

    #[tokio::test]
    async fn test_http_source_poll_not_running() {
        let mut connector = HttpSourceConnector::new();
        let result = connector.poll().await;
        assert!(result.is_err());
    }

    // ---- 19. ConsoleSink: put when not running ----

    #[tokio::test]
    async fn test_console_sink_put_not_running() {
        let mut connector = ConsoleSinkConnector::new();
        let record = SinkRecord {
            topic: "t".to_string(),
            partition: 0,
            offset: 0,
            key: None,
            value: Some(b"data".to_vec()),
            headers: vec![],
            timestamp: 0,
        };

        let result = connector.put(vec![record]).await;
        assert!(result.is_err());
    }

    // ---- 20. ConnectorRuntime: delete running connector stops it first ----

    #[tokio::test]
    async fn test_delete_running_connector_stops_first() {
        let runtime = ConnectorRuntime::new();
        runtime
            .register_connector_type(Box::new(ConsoleSinkConnectorFactory))
            .await;

        let config = console_sink_config("to-delete");
        runtime.create_connector(config).await.unwrap();
        runtime.start_connector("to-delete").await.unwrap();

        // Deleting a running connector should not fail
        runtime.delete_connector("to-delete").await.unwrap();
        let list = runtime.list_connectors().await;
        assert!(list.is_empty());
    }

    // ---- 21. HttpSourceConnector: config_definitions completeness ----

    #[test]
    fn test_http_source_config_definitions() {
        let connector = HttpSourceConnector::new();
        let defs = connector.config_definitions();
        assert!(defs.len() >= 4);
        let names: Vec<&str> = defs.iter().map(|d| d.name.as_str()).collect();
        assert!(names.contains(&"url"));
        assert!(names.contains(&"method"));
        assert!(names.contains(&"interval_ms"));
        assert!(names.contains(&"headers"));
    }

    // ---- 22. ConsoleSinkConnector: config_definitions completeness ----

    #[test]
    fn test_console_sink_config_definitions() {
        let connector = ConsoleSinkConnector::new();
        let defs = connector.config_definitions();
        assert!(defs.len() >= 2);
        let names: Vec<&str> = defs.iter().map(|d| d.name.as_str()).collect();
        assert!(names.contains(&"print_key"));
        assert!(names.contains(&"prefix"));
    }

    // ---- 23. ConnectorStatus task count matches tasks_max ----

    #[tokio::test]
    async fn test_connector_status_task_count() {
        let runtime = ConnectorRuntime::new();
        runtime
            .register_connector_type(Box::new(HttpSourceConnectorFactory))
            .await;

        let mut config = http_source_config("multi-task", "https://example.com");
        config.tasks_max = 4;
        runtime.create_connector(config).await.unwrap();
        runtime.start_connector("multi-task").await.unwrap();

        let status = runtime.get_connector_status("multi-task").await.unwrap();
        assert_eq!(status.tasks.len(), 4);
        for task in &status.tasks {
            assert_eq!(task.state, NativeConnectorState::Running);
        }
    }

    // ---- 24. HttpSourceConnector: multiple polls increment count ----

    #[tokio::test]
    async fn test_http_source_multiple_polls() {
        let mut connector = HttpSourceConnector::new();
        let config = http_source_config("multi-poll", "https://example.com");
        connector.start(config).await.unwrap();

        for i in 1..=5 {
            let records = connector.poll().await.unwrap();
            assert_eq!(records.len(), 1);
            let offset = records[0].source_offset.get("poll_count").unwrap();
            assert_eq!(offset, &i.to_string());
        }
        assert_eq!(connector.poll_count, 5);
    }

    // ---- 25. ConsoleSink: put with print_key enabled ----

    #[tokio::test]
    async fn test_console_sink_put_with_print_key() {
        let mut connector = ConsoleSinkConnector::new();
        let mut props = HashMap::new();
        props.insert("print_key".to_string(), "true".to_string());
        props.insert("prefix".to_string(), "[TEST] ".to_string());
        let config = NativeConnectorConfig {
            name: "key-test".to_string(),
            connector_class: "ConsoleSinkConnector".to_string(),
            tasks_max: 1,
            properties: props,
        };
        connector.start(config).await.unwrap();
        assert!(connector.print_key());
        assert_eq!(connector.prefix(), "[TEST] ");

        let records = vec![
            SinkRecord {
                topic: "t".to_string(),
                partition: 0,
                offset: 0,
                key: Some(b"k1".to_vec()),
                value: Some(b"v1".to_vec()),
                headers: vec![],
                timestamp: 0,
            },
            SinkRecord {
                topic: "t".to_string(),
                partition: 0,
                offset: 1,
                key: None,
                value: None,
                headers: vec![],
                timestamp: 0,
            },
        ];

        connector.put(records).await.unwrap();
        assert_eq!(connector.records_written, 2);
    }
}
