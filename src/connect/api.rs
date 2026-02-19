//! Kafka Connect REST API
//!
//! This module provides a Confluent Kafka Connect-compatible REST API for managing
//! connectors, tasks, and plugins.
//!
//! ## Endpoints
//!
//! ### Cluster
//! - `GET /` - Get Connect worker info
//!
//! ### Connectors
//! - `GET /connectors` - List all connectors
//! - `POST /connectors` - Create a connector
//! - `GET /connectors/{name}` - Get connector info
//! - `GET /connectors/{name}/config` - Get connector config
//! - `PUT /connectors/{name}/config` - Update connector config
//! - `GET /connectors/{name}/status` - Get connector status
//! - `POST /connectors/{name}/restart` - Restart connector
//! - `PUT /connectors/{name}/pause` - Pause connector
//! - `PUT /connectors/{name}/resume` - Resume connector
//! - `DELETE /connectors/{name}` - Delete connector
//!
//! ### Tasks
//! - `GET /connectors/{name}/tasks` - List connector tasks
//! - `GET /connectors/{name}/tasks/{taskId}/status` - Get task status
//! - `POST /connectors/{name}/tasks/{taskId}/restart` - Restart task
//!
//! ### Plugins
//! - `GET /connector-plugins` - List available plugins
//! - `PUT /connector-plugins/{pluginName}/config/validate` - Validate config

use super::{ConnectorState, TaskState};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post, put},
    Json, Router,
};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::info;

/// Shared state for the Connect REST API
#[derive(Clone)]
pub struct ConnectApiState {
    /// Connector runtime manager (REST API layer)
    pub connector_manager: Arc<ConnectorManager>,
    /// Optional execution runtime for actually running connectors.
    /// When set, lifecycle operations (create/start/stop) also trigger the execution engine.
    pub runtime: Option<Arc<super::runtime::ConnectorRuntime>>,
}

/// Connector configuration and runtime state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorInfo {
    /// Connector name
    pub name: String,
    /// Connector configuration
    pub config: HashMap<String, String>,
    /// Tasks currently assigned
    pub tasks: Vec<TaskId>,
    /// Connector type (source or sink)
    #[serde(rename = "type")]
    pub connector_type: ConnectorType,
}

/// Task identifier
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskId {
    /// Connector name
    pub connector: String,
    /// Task number
    pub task: i32,
}

/// Connector type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ConnectorType {
    #[default]
    Source,
    Sink,
}

/// Create connector request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateConnectorRequest {
    /// Connector name
    pub name: String,
    /// Connector configuration
    pub config: HashMap<String, String>,
}

/// Connector status response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorStatusResponse {
    /// Connector name
    pub name: String,
    /// Connector status
    pub connector: ConnectorStatus,
    /// Task statuses
    pub tasks: Vec<TaskStatus>,
    /// Connector type
    #[serde(rename = "type")]
    pub connector_type: ConnectorType,
}

/// Individual connector status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorStatus {
    /// Current state
    pub state: ConnectorState,
    /// Worker ID hosting this connector
    pub worker_id: String,
    /// Trace message if failed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace: Option<String>,
}

/// Individual task status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskStatus {
    /// Task ID
    pub id: i32,
    /// Current state
    pub state: TaskState,
    /// Worker ID hosting this task
    pub worker_id: String,
    /// Trace message if failed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace: Option<String>,
}

/// Connector plugin info
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorPlugin {
    /// Plugin class name
    #[serde(rename = "class")]
    pub class_name: String,
    /// Plugin type
    #[serde(rename = "type")]
    pub plugin_type: ConnectorType,
    /// Plugin version
    pub version: String,
}

/// Config validation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigValidationResult {
    /// Config name
    pub name: String,
    /// Number of errors
    pub error_count: i32,
    /// Number of groups
    pub groups: Vec<String>,
    /// Config definitions
    pub configs: Vec<ConfigInfo>,
}

/// Config info with validation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigInfo {
    /// Config definition
    pub definition: ConfigDefinition,
    /// Config value
    pub value: ConfigValue,
}

/// Config definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigDefinition {
    /// Config name
    pub name: String,
    /// Config type
    #[serde(rename = "type")]
    pub config_type: String,
    /// Whether required
    pub required: bool,
    /// Default value
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_value: Option<String>,
    /// Documentation
    pub documentation: String,
}

/// Config value with errors
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigValue {
    /// Config name
    pub name: String,
    /// Config value
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    /// Recommended values
    pub recommended_values: Vec<String>,
    /// Errors
    pub errors: Vec<String>,
    /// Whether visible
    pub visible: bool,
}

/// Worker info response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerInfo {
    /// Kafka cluster ID
    pub kafka_cluster_id: String,
    /// Connect version
    pub version: String,
    /// Git commit
    pub commit: String,
}

/// Restart query parameters
#[derive(Debug, Clone, Deserialize)]
pub struct RestartQuery {
    /// Include tasks in restart
    #[serde(default)]
    pub include_tasks: bool,
    /// Only restart failed instances
    #[serde(default)]
    pub only_failed: bool,
}

/// Error response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectError {
    /// Error code
    pub error_code: i32,
    /// Error message
    pub message: String,
}

impl ConnectError {
    fn not_found(name: &str) -> Self {
        Self {
            error_code: 404,
            message: format!("Connector {} not found", name),
        }
    }

    fn bad_request(msg: impl Into<String>) -> Self {
        Self {
            error_code: 400,
            message: msg.into(),
        }
    }

    fn conflict(msg: impl Into<String>) -> Self {
        Self {
            error_code: 409,
            message: msg.into(),
        }
    }

    #[allow(dead_code)]
    fn internal(msg: impl Into<String>) -> Self {
        Self {
            error_code: 500,
            message: msg.into(),
        }
    }
}

/// Connector runtime manager
pub struct ConnectorManager {
    /// Registered connectors
    connectors: RwLock<HashMap<String, ConnectorRuntime>>,
    /// Worker ID
    worker_id: String,
    /// Available plugins
    plugins: Vec<ConnectorPlugin>,
    /// Dead letter queue entries tracked per connector
    dlq_entries: RwLock<Vec<DlqEntry>>,
    /// Source connector offset store: connector_name -> (partition_key -> offset_value)
    offset_store: RwLock<HashMap<String, HashMap<String, String>>>,
}

/// Runtime state for a connector
#[derive(Debug, Clone)]
struct ConnectorRuntime {
    /// Connector configuration
    info: ConnectorInfo,
    /// Current state
    state: ConnectorState,
    /// Task states
    task_states: Vec<TaskState>,
    /// Error message if failed
    error: Option<String>,
    /// Creation timestamp
    #[allow(dead_code)]
    created_at: u64,
    /// Last update timestamp
    updated_at: u64,
    /// Number of times this connector has been restarted
    restart_count: u32,
}

impl ConnectorManager {
    /// Create a new connector manager
    pub fn new() -> Self {
        // Get hostname via std::env or default to localhost
        let hostname = std::env::var("HOSTNAME")
            .or_else(|_| std::env::var("COMPUTERNAME"))
            .unwrap_or_else(|_| "localhost".to_string());
        let worker_id = format!("{}:8083", hostname);

        // Built-in plugins
        let plugins = vec![
            ConnectorPlugin {
                class_name: "io.streamline.connect.file.FileStreamSourceConnector".to_string(),
                plugin_type: ConnectorType::Source,
                version: env!("CARGO_PKG_VERSION").to_string(),
            },
            ConnectorPlugin {
                class_name: "io.streamline.connect.file.FileStreamSinkConnector".to_string(),
                plugin_type: ConnectorType::Sink,
                version: env!("CARGO_PKG_VERSION").to_string(),
            },
            ConnectorPlugin {
                class_name: "io.streamline.connect.mirror.MirrorSourceConnector".to_string(),
                plugin_type: ConnectorType::Source,
                version: env!("CARGO_PKG_VERSION").to_string(),
            },
            ConnectorPlugin {
                class_name: "io.streamline.connect.mirror.MirrorCheckpointConnector".to_string(),
                plugin_type: ConnectorType::Source,
                version: env!("CARGO_PKG_VERSION").to_string(),
            },
            ConnectorPlugin {
                class_name: "io.streamline.connect.mirror.MirrorHeartbeatConnector".to_string(),
                plugin_type: ConnectorType::Source,
                version: env!("CARGO_PKG_VERSION").to_string(),
            },
        ];

        Self {
            connectors: RwLock::new(HashMap::new()),
            worker_id,
            plugins,
            dlq_entries: RwLock::new(Vec::new()),
            offset_store: RwLock::new(HashMap::new()),
        }
    }

    /// Get worker info
    pub fn worker_info(&self) -> WorkerInfo {
        WorkerInfo {
            kafka_cluster_id: "streamline-cluster".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            commit: "unknown".to_string(),
        }
    }

    /// List all connector names
    pub fn list_connectors(&self) -> Vec<String> {
        self.connectors.read().keys().cloned().collect()
    }

    /// Get connector info
    pub fn get_connector(&self, name: &str) -> Option<ConnectorInfo> {
        self.connectors.read().get(name).map(|c| c.info.clone())
    }

    /// Get connector config
    pub fn get_connector_config(&self, name: &str) -> Option<HashMap<String, String>> {
        self.connectors
            .read()
            .get(name)
            .map(|c| c.info.config.clone())
    }

    /// Create a connector
    pub fn create_connector(
        &self,
        request: CreateConnectorRequest,
    ) -> Result<ConnectorInfo, ConnectError> {
        let mut connectors = self.connectors.write();

        if connectors.contains_key(&request.name) {
            return Err(ConnectError::conflict(format!(
                "Connector {} already exists",
                request.name
            )));
        }

        // Validate required config
        if !request.config.contains_key("connector.class") {
            return Err(ConnectError::bad_request(
                "Missing required config: connector.class",
            ));
        }

        // Determine connector type from class
        let connector_type = if request
            .config
            .get("connector.class")
            .map(|c| c.to_lowercase().contains("source"))
            .unwrap_or(false)
        {
            ConnectorType::Source
        } else {
            ConnectorType::Sink
        };

        // Determine number of tasks
        let tasks_max = request
            .config
            .get("tasks.max")
            .and_then(|t| t.parse::<i32>().ok())
            .unwrap_or(1);

        let tasks: Vec<TaskId> = (0..tasks_max)
            .map(|i| TaskId {
                connector: request.name.clone(),
                task: i,
            })
            .collect();

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| ConnectError::internal("system clock error"))?
            .as_secs();

        let info = ConnectorInfo {
            name: request.name.clone(),
            config: request.config,
            tasks: tasks.clone(),
            connector_type,
        };

        let task_states = vec![TaskState::Running; tasks.len()];

        let runtime = ConnectorRuntime {
            info: info.clone(),
            state: ConnectorState::Running,
            task_states,
            error: None,
            created_at: now,
            updated_at: now,
            restart_count: 0,
        };

        connectors.insert(request.name.clone(), runtime);

        info!(connector = %request.name, "Connector created");

        Ok(info)
    }

    /// Update connector config
    pub fn update_connector_config(
        &self,
        name: &str,
        config: HashMap<String, String>,
    ) -> Result<ConnectorInfo, ConnectError> {
        let mut connectors = self.connectors.write();

        let runtime = connectors
            .get_mut(name)
            .ok_or_else(|| ConnectError::not_found(name))?;

        // Update config
        runtime.info.config = config;
        runtime.updated_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| ConnectError::internal("system clock error"))?
            .as_secs();

        // Recalculate tasks if tasks.max changed
        let tasks_max = runtime
            .info
            .config
            .get("tasks.max")
            .and_then(|t| t.parse::<i32>().ok())
            .unwrap_or(1);

        runtime.info.tasks = (0..tasks_max)
            .map(|i| TaskId {
                connector: name.to_string(),
                task: i,
            })
            .collect();

        runtime.task_states = vec![TaskState::Running; runtime.info.tasks.len()];

        info!(connector = %name, "Connector config updated");

        Ok(runtime.info.clone())
    }

    /// Delete a connector
    pub fn delete_connector(&self, name: &str) -> Result<(), ConnectError> {
        let mut connectors = self.connectors.write();

        if connectors.remove(name).is_none() {
            return Err(ConnectError::not_found(name));
        }

        info!(connector = %name, "Connector deleted");

        Ok(())
    }

    /// Get connector status
    pub fn get_connector_status(
        &self,
        name: &str,
    ) -> Result<ConnectorStatusResponse, ConnectError> {
        let connectors = self.connectors.read();

        let runtime = connectors
            .get(name)
            .ok_or_else(|| ConnectError::not_found(name))?;

        let tasks: Vec<TaskStatus> = runtime
            .task_states
            .iter()
            .enumerate()
            .map(|(i, state)| TaskStatus {
                id: i as i32,
                state: state.clone(),
                worker_id: self.worker_id.clone(),
                trace: None,
            })
            .collect();

        Ok(ConnectorStatusResponse {
            name: name.to_string(),
            connector: ConnectorStatus {
                state: runtime.state.clone(),
                worker_id: self.worker_id.clone(),
                trace: runtime.error.clone(),
            },
            tasks,
            connector_type: runtime.info.connector_type,
        })
    }

    /// Pause a connector
    pub fn pause_connector(&self, name: &str) -> Result<(), ConnectError> {
        let mut connectors = self.connectors.write();

        let runtime = connectors
            .get_mut(name)
            .ok_or_else(|| ConnectError::not_found(name))?;

        runtime.state = ConnectorState::Paused;
        for state in &mut runtime.task_states {
            *state = TaskState::Paused;
        }
        runtime.updated_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| ConnectError::internal("system clock error"))?
            .as_secs();

        info!(connector = %name, "Connector paused");

        Ok(())
    }

    /// Resume a connector
    pub fn resume_connector(&self, name: &str) -> Result<(), ConnectError> {
        let mut connectors = self.connectors.write();

        let runtime = connectors
            .get_mut(name)
            .ok_or_else(|| ConnectError::not_found(name))?;

        runtime.state = ConnectorState::Running;
        for state in &mut runtime.task_states {
            *state = TaskState::Running;
        }
        runtime.updated_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| ConnectError::internal("system clock error"))?
            .as_secs();

        info!(connector = %name, "Connector resumed");

        Ok(())
    }

    /// Restart a connector
    pub fn restart_connector(
        &self,
        name: &str,
        include_tasks: bool,
        only_failed: bool,
    ) -> Result<(), ConnectError> {
        let mut connectors = self.connectors.write();

        let runtime = connectors
            .get_mut(name)
            .ok_or_else(|| ConnectError::not_found(name))?;

        // Restart connector if not only_failed or if it's failed
        if !only_failed || runtime.state == ConnectorState::Failed {
            runtime.state = ConnectorState::Restarting;
            runtime.state = ConnectorState::Running;
            runtime.error = None;
            runtime.restart_count += 1;
        }

        // Restart tasks if requested
        if include_tasks {
            for state in &mut runtime.task_states {
                if !only_failed || *state == TaskState::Failed {
                    *state = TaskState::Running;
                }
            }
        }

        runtime.updated_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| ConnectError::internal("system clock error"))?
            .as_secs();

        info!(
            connector = %name,
            include_tasks = include_tasks,
            only_failed = only_failed,
            "Connector restarted"
        );

        Ok(())
    }

    /// Get connector tasks
    pub fn get_connector_tasks(&self, name: &str) -> Result<Vec<TaskInfo>, ConnectError> {
        let connectors = self.connectors.read();

        let runtime = connectors
            .get(name)
            .ok_or_else(|| ConnectError::not_found(name))?;

        let tasks: Vec<TaskInfo> = runtime
            .info
            .tasks
            .iter()
            .map(|id| TaskInfo {
                id: id.clone(),
                config: runtime.info.config.clone(),
            })
            .collect();

        Ok(tasks)
    }

    /// Get task status
    pub fn get_task_status(&self, name: &str, task_id: i32) -> Result<TaskStatus, ConnectError> {
        let connectors = self.connectors.read();

        let runtime = connectors
            .get(name)
            .ok_or_else(|| ConnectError::not_found(name))?;

        let state = runtime
            .task_states
            .get(task_id as usize)
            .ok_or_else(|| ConnectError::not_found(&format!("{}:{}", name, task_id)))?;

        Ok(TaskStatus {
            id: task_id,
            state: state.clone(),
            worker_id: self.worker_id.clone(),
            trace: None,
        })
    }

    /// Restart a task
    pub fn restart_task(&self, name: &str, task_id: i32) -> Result<(), ConnectError> {
        let mut connectors = self.connectors.write();

        let runtime = connectors
            .get_mut(name)
            .ok_or_else(|| ConnectError::not_found(name))?;

        let state = runtime
            .task_states
            .get_mut(task_id as usize)
            .ok_or_else(|| ConnectError::not_found(&format!("{}:{}", name, task_id)))?;

        *state = TaskState::Running;

        runtime.updated_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| ConnectError::internal("system clock error"))?
            .as_secs();

        info!(connector = %name, task = task_id, "Task restarted");

        Ok(())
    }

    /// Get available plugins
    pub fn get_plugins(&self) -> Vec<ConnectorPlugin> {
        self.plugins.clone()
    }

    /// Validate connector config
    pub fn validate_config(
        &self,
        _plugin_name: &str,
        config: HashMap<String, String>,
    ) -> ConfigValidationResult {
        let mut configs = Vec::new();
        let mut error_count = 0;

        // Required configs
        let required = vec![
            ("connector.class", "Class name of the connector", true),
            ("tasks.max", "Maximum number of tasks", true),
            (
                "topics",
                "Topics to subscribe to (sink) or produce to (source)",
                false,
            ),
        ];

        for (name, doc, is_required) in required {
            let value = config.get(name).cloned();
            let errors = if is_required && value.is_none() {
                error_count += 1;
                vec![format!("Missing required config: {}", name)]
            } else {
                vec![]
            };

            configs.push(ConfigInfo {
                definition: ConfigDefinition {
                    name: name.to_string(),
                    config_type: "STRING".to_string(),
                    required: is_required,
                    default_value: None,
                    documentation: doc.to_string(),
                },
                value: ConfigValue {
                    name: name.to_string(),
                    value,
                    recommended_values: vec![],
                    errors,
                    visible: true,
                },
            });
        }

        ConfigValidationResult {
            name: config.get("connector.class").cloned().unwrap_or_default(),
            error_count,
            groups: vec!["Common".to_string()],
            configs,
        }
    }

    /// Get health status for all connectors
    pub fn health_check(&self) -> ConnectorHealthReport {
        let connectors = self.connectors.read();
        let mut healthy = 0;
        let mut unhealthy = 0;
        let mut paused = 0;
        let mut details = Vec::new();

        for (name, runtime) in connectors.iter() {
            let failed_tasks = runtime
                .task_states
                .iter()
                .filter(|s| matches!(s, TaskState::Failed))
                .count();

            let status = match runtime.state {
                ConnectorState::Running if failed_tasks == 0 => {
                    healthy += 1;
                    "healthy"
                }
                ConnectorState::Running => {
                    unhealthy += 1;
                    "degraded"
                }
                ConnectorState::Paused => {
                    paused += 1;
                    "paused"
                }
                ConnectorState::Failed => {
                    unhealthy += 1;
                    "failed"
                }
                _ => {
                    unhealthy += 1;
                    "unknown"
                }
            };

            details.push(ConnectorHealthDetail {
                name: name.clone(),
                status: status.to_string(),
                total_tasks: runtime.task_states.len(),
                failed_tasks,
                error: runtime.error.clone(),
            });
        }

        ConnectorHealthReport {
            status: if unhealthy == 0 {
                "healthy"
            } else {
                "unhealthy"
            }
            .to_string(),
            total_connectors: connectors.len(),
            healthy,
            unhealthy,
            paused,
            connectors: details,
        }
    }

    /// Get dead letter queue entries for a connector
    pub fn get_dlq_entries(&self, connector_name: &str) -> Option<Vec<DlqEntry>> {
        let connectors = self.connectors.read();
        if connectors.contains_key(connector_name) {
            let entries = self.dlq_entries.read();
            Some(
                entries
                    .iter()
                    .filter(|e| e.connector == connector_name)
                    .cloned()
                    .collect(),
            )
        } else {
            None
        }
    }

    /// Add a DLQ entry for a connector
    pub fn add_dlq_entry(&self, entry: DlqEntry) {
        self.dlq_entries.write().push(entry);
    }

    // ---- Offset Management ----

    /// Get offsets for a source connector
    pub fn get_connector_offsets(
        &self,
        name: &str,
    ) -> Result<ConnectorOffsets, ConnectError> {
        let connectors = self.connectors.read();
        if !connectors.contains_key(name) {
            return Err(ConnectError::not_found(name));
        }

        let store = self.offset_store.read();
        let offsets = store.get(name).cloned().unwrap_or_default();
        Ok(ConnectorOffsets {
            connector: name.to_string(),
            offsets: offsets
                .into_iter()
                .map(|(key, value)| OffsetEntry { partition: key, offset: value })
                .collect(),
        })
    }

    /// Commit offsets for a source connector
    pub fn commit_offsets(
        &self,
        name: &str,
        offsets: HashMap<String, String>,
    ) -> Result<(), ConnectError> {
        let connectors = self.connectors.read();
        if !connectors.contains_key(name) {
            return Err(ConnectError::not_found(name));
        }
        drop(connectors);

        let mut store = self.offset_store.write();
        let connector_offsets = store.entry(name.to_string()).or_default();
        for (key, value) in offsets {
            connector_offsets.insert(key, value);
        }

        info!(connector = %name, "Offsets committed");
        Ok(())
    }

    /// Reset (delete) offsets for a source connector
    pub fn reset_connector_offsets(&self, name: &str) -> Result<(), ConnectError> {
        let connectors = self.connectors.read();

        // Only allow offset reset when connector is stopped or paused
        let runtime = connectors.get(name).ok_or_else(|| ConnectError::not_found(name))?;
        if runtime.state == ConnectorState::Running {
            return Err(ConnectError::bad_request(
                "Cannot reset offsets while connector is running. Pause or stop it first.",
            ));
        }
        drop(connectors);

        let mut store = self.offset_store.write();
        store.remove(name);

        info!(connector = %name, "Offsets reset");
        Ok(())
    }

    /// Delete a connector and clean up all associated state (offsets, DLQ entries)
    pub fn delete_connector_full(&self, name: &str) -> Result<(), ConnectError> {
        self.delete_connector(name)?;

        // Clean up offsets
        let mut store = self.offset_store.write();
        store.remove(name);

        // Clean up DLQ entries
        let mut dlq = self.dlq_entries.write();
        dlq.retain(|e| e.connector != name);

        Ok(())
    }

    /// Get restart count for a connector
    pub fn get_restart_count(&self, name: &str) -> Option<u32> {
        self.connectors.read().get(name).map(|r| r.restart_count)
    }
}

impl Default for ConnectorManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Offset information for a source connector
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorOffsets {
    /// Connector name
    pub connector: String,
    /// Current offset entries
    pub offsets: Vec<OffsetEntry>,
}

/// A single offset entry (partition -> offset)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OffsetEntry {
    /// Partition key (e.g., "topic:partition" or source-specific identifier)
    pub partition: String,
    /// Offset value
    pub offset: String,
}

/// Health report for all connectors
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorHealthReport {
    /// Overall health status
    pub status: String,
    /// Total number of connectors
    pub total_connectors: usize,
    /// Number of healthy connectors
    pub healthy: usize,
    /// Number of unhealthy connectors
    pub unhealthy: usize,
    /// Number of paused connectors
    pub paused: usize,
    /// Per-connector health details
    pub connectors: Vec<ConnectorHealthDetail>,
}

/// Health detail for a single connector
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorHealthDetail {
    /// Connector name
    pub name: String,
    /// Health status
    pub status: String,
    /// Total number of tasks
    pub total_tasks: usize,
    /// Number of failed tasks
    pub failed_tasks: usize,
    /// Error message if unhealthy
    pub error: Option<String>,
}

/// Dead letter queue entry for failed records
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqEntry {
    /// Connector name
    pub connector: String,
    /// Task ID that produced this entry
    pub task_id: i32,
    /// Original topic
    pub original_topic: String,
    /// Original partition
    pub original_partition: i32,
    /// Original offset
    pub original_offset: i64,
    /// Error that caused the record to be sent to DLQ
    pub error: String,
    /// Timestamp when the failure occurred
    pub timestamp: u64,
    /// Original record value (base64 encoded)
    pub value: Option<String>,
}

/// Task info with config
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskInfo {
    /// Task ID
    pub id: TaskId,
    /// Task config
    pub config: HashMap<String, String>,
}

// ============================================================================
// Axum handlers
// ============================================================================

/// Get Connect worker info
async fn get_worker_info(State(state): State<ConnectApiState>) -> Json<WorkerInfo> {
    Json(state.connector_manager.worker_info())
}

/// List all connectors
async fn list_connectors(State(state): State<ConnectApiState>) -> Json<Vec<String>> {
    Json(state.connector_manager.list_connectors())
}

/// Create a connector
async fn create_connector(
    State(state): State<ConnectApiState>,
    Json(request): Json<CreateConnectorRequest>,
) -> Response {
    match state.connector_manager.create_connector(request) {
        Ok(info) => (StatusCode::CREATED, Json(info)).into_response(),
        Err(e) => {
            let status = match e.error_code {
                400 => StatusCode::BAD_REQUEST,
                409 => StatusCode::CONFLICT,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            (status, Json(e)).into_response()
        }
    }
}

/// Get connector info
async fn get_connector(State(state): State<ConnectApiState>, Path(name): Path<String>) -> Response {
    match state.connector_manager.get_connector(&name) {
        Some(info) => (StatusCode::OK, Json(info)).into_response(),
        None => {
            let error = ConnectError::not_found(&name);
            (StatusCode::NOT_FOUND, Json(error)).into_response()
        }
    }
}

/// Get connector config
async fn get_connector_config(
    State(state): State<ConnectApiState>,
    Path(name): Path<String>,
) -> Response {
    match state.connector_manager.get_connector_config(&name) {
        Some(config) => (StatusCode::OK, Json(config)).into_response(),
        None => {
            let error = ConnectError::not_found(&name);
            (StatusCode::NOT_FOUND, Json(error)).into_response()
        }
    }
}

/// Update connector config
async fn update_connector_config(
    State(state): State<ConnectApiState>,
    Path(name): Path<String>,
    Json(config): Json<HashMap<String, String>>,
) -> Response {
    match state
        .connector_manager
        .update_connector_config(&name, config)
    {
        Ok(info) => (StatusCode::OK, Json(info)).into_response(),
        Err(e) => {
            let status = match e.error_code {
                404 => StatusCode::NOT_FOUND,
                400 => StatusCode::BAD_REQUEST,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            (status, Json(e)).into_response()
        }
    }
}

/// Get connector status
async fn get_connector_status(
    State(state): State<ConnectApiState>,
    Path(name): Path<String>,
) -> Response {
    match state.connector_manager.get_connector_status(&name) {
        Ok(status) => (StatusCode::OK, Json(status)).into_response(),
        Err(e) => (StatusCode::NOT_FOUND, Json(e)).into_response(),
    }
}

/// Pause connector
async fn pause_connector(
    State(state): State<ConnectApiState>,
    Path(name): Path<String>,
) -> Response {
    match state.connector_manager.pause_connector(&name) {
        Ok(()) => (StatusCode::ACCEPTED, "").into_response(),
        Err(e) => (StatusCode::NOT_FOUND, Json(e)).into_response(),
    }
}

/// Resume connector
async fn resume_connector(
    State(state): State<ConnectApiState>,
    Path(name): Path<String>,
) -> Response {
    match state.connector_manager.resume_connector(&name) {
        Ok(()) => (StatusCode::ACCEPTED, "").into_response(),
        Err(e) => (StatusCode::NOT_FOUND, Json(e)).into_response(),
    }
}

/// Restart connector
async fn restart_connector(
    State(state): State<ConnectApiState>,
    Path(name): Path<String>,
    Query(query): Query<RestartQuery>,
) -> Response {
    match state
        .connector_manager
        .restart_connector(&name, query.include_tasks, query.only_failed)
    {
        Ok(()) => (StatusCode::NO_CONTENT, "").into_response(),
        Err(e) => (StatusCode::NOT_FOUND, Json(e)).into_response(),
    }
}

/// Delete connector (also cleans up offsets and DLQ entries)
async fn delete_connector(
    State(state): State<ConnectApiState>,
    Path(name): Path<String>,
) -> Response {
    match state.connector_manager.delete_connector_full(&name) {
        Ok(()) => (StatusCode::NO_CONTENT, "").into_response(),
        Err(e) => (StatusCode::NOT_FOUND, Json(e)).into_response(),
    }
}

/// Get connector tasks
async fn get_connector_tasks(
    State(state): State<ConnectApiState>,
    Path(name): Path<String>,
) -> Response {
    match state.connector_manager.get_connector_tasks(&name) {
        Ok(tasks) => (StatusCode::OK, Json(tasks)).into_response(),
        Err(e) => (StatusCode::NOT_FOUND, Json(e)).into_response(),
    }
}

/// Get task status
async fn get_task_status(
    State(state): State<ConnectApiState>,
    Path((name, task_id)): Path<(String, i32)>,
) -> Response {
    match state.connector_manager.get_task_status(&name, task_id) {
        Ok(status) => (StatusCode::OK, Json(status)).into_response(),
        Err(e) => (StatusCode::NOT_FOUND, Json(e)).into_response(),
    }
}

/// Restart task
async fn restart_task(
    State(state): State<ConnectApiState>,
    Path((name, task_id)): Path<(String, i32)>,
) -> Response {
    match state.connector_manager.restart_task(&name, task_id) {
        Ok(()) => (StatusCode::NO_CONTENT, "").into_response(),
        Err(e) => (StatusCode::NOT_FOUND, Json(e)).into_response(),
    }
}

/// Get connector plugins
async fn get_connector_plugins(State(state): State<ConnectApiState>) -> Json<Vec<ConnectorPlugin>> {
    Json(state.connector_manager.get_plugins())
}

/// Validate connector config
async fn validate_connector_config(
    State(state): State<ConnectApiState>,
    Path(plugin_name): Path<String>,
    Json(config): Json<HashMap<String, String>>,
) -> Json<ConfigValidationResult> {
    Json(
        state
            .connector_manager
            .validate_config(&plugin_name, config),
    )
}

/// Get health report for all connectors
async fn connectors_health(State(state): State<ConnectApiState>) -> Json<ConnectorHealthReport> {
    Json(state.connector_manager.health_check())
}

/// Get dead letter queue entries for a connector
async fn get_connector_dlq(
    State(state): State<ConnectApiState>,
    Path(name): Path<String>,
) -> Response {
    match state.connector_manager.get_dlq_entries(&name) {
        Some(entries) => Json(serde_json::json!({
            "connector": name,
            "entries": entries,
            "total": entries.len(),
        }))
        .into_response(),
        None => (StatusCode::NOT_FOUND, Json(ConnectError::not_found(&name))).into_response(),
    }
}

/// Get offsets for a source connector
async fn get_connector_offsets(
    State(state): State<ConnectApiState>,
    Path(name): Path<String>,
) -> Response {
    match state.connector_manager.get_connector_offsets(&name) {
        Ok(offsets) => (StatusCode::OK, Json(offsets)).into_response(),
        Err(e) => {
            let status = match e.error_code {
                404 => StatusCode::NOT_FOUND,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            (status, Json(e)).into_response()
        }
    }
}

/// Reset offsets for a source connector (must be paused/stopped)
async fn reset_connector_offsets(
    State(state): State<ConnectApiState>,
    Path(name): Path<String>,
) -> Response {
    match state.connector_manager.reset_connector_offsets(&name) {
        Ok(()) => (StatusCode::NO_CONTENT, "").into_response(),
        Err(e) => {
            let status = match e.error_code {
                400 => StatusCode::BAD_REQUEST,
                404 => StatusCode::NOT_FOUND,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            (status, Json(e)).into_response()
        }
    }
}

/// Create the Connect REST API router
pub fn create_connect_router(state: ConnectApiState) -> Router {
    Router::new()
        // Worker info
        .route("/", get(get_worker_info))
        // Health monitoring
        .route("/connectors/health", get(connectors_health))
        // Connectors
        .route("/connectors", get(list_connectors).post(create_connector))
        .route(
            "/connectors/{name}",
            get(get_connector).delete(delete_connector),
        )
        .route(
            "/connectors/{name}/config",
            get(get_connector_config).put(update_connector_config),
        )
        .route("/connectors/{name}/status", get(get_connector_status))
        .route("/connectors/{name}/pause", put(pause_connector))
        .route("/connectors/{name}/resume", put(resume_connector))
        .route("/connectors/{name}/restart", post(restart_connector))
        // Offsets
        .route(
            "/connectors/{name}/offsets",
            get(get_connector_offsets).delete(reset_connector_offsets),
        )
        // Dead letter queue
        .route("/connectors/{name}/dlq", get(get_connector_dlq))
        // Tasks
        .route("/connectors/{name}/tasks", get(get_connector_tasks))
        .route(
            "/connectors/{name}/tasks/{task_id}/status",
            get(get_task_status),
        )
        .route(
            "/connectors/{name}/tasks/{task_id}/restart",
            post(restart_task),
        )
        // Plugins
        .route("/connector-plugins", get(get_connector_plugins))
        .route(
            "/connector-plugins/{plugin_name}/config/validate",
            put(validate_connector_config),
        )
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connector_manager_creation() {
        let manager = ConnectorManager::new();
        assert!(manager.list_connectors().is_empty());
        assert!(!manager.get_plugins().is_empty());
    }

    #[test]
    fn test_create_connector() {
        let manager = ConnectorManager::new();

        let mut config = HashMap::new();
        config.insert(
            "connector.class".to_string(),
            "io.streamline.connect.file.FileStreamSourceConnector".to_string(),
        );
        config.insert("tasks.max".to_string(), "2".to_string());

        let request = CreateConnectorRequest {
            name: "test-connector".to_string(),
            config,
        };

        let result = manager.create_connector(request);
        assert!(result.is_ok());

        let info = result.unwrap();
        assert_eq!(info.name, "test-connector");
        assert_eq!(info.tasks.len(), 2);
        assert_eq!(info.connector_type, ConnectorType::Source);
    }

    #[test]
    fn test_connector_lifecycle() {
        let manager = ConnectorManager::new();

        // Create
        let mut config = HashMap::new();
        config.insert(
            "connector.class".to_string(),
            "io.streamline.connect.file.FileStreamSinkConnector".to_string(),
        );
        config.insert("tasks.max".to_string(), "1".to_string());

        let request = CreateConnectorRequest {
            name: "lifecycle-test".to_string(),
            config,
        };

        manager.create_connector(request).unwrap();

        // Pause
        manager.pause_connector("lifecycle-test").unwrap();
        let status = manager.get_connector_status("lifecycle-test").unwrap();
        assert_eq!(status.connector.state, ConnectorState::Paused);

        // Resume
        manager.resume_connector("lifecycle-test").unwrap();
        let status = manager.get_connector_status("lifecycle-test").unwrap();
        assert_eq!(status.connector.state, ConnectorState::Running);

        // Delete
        manager.delete_connector("lifecycle-test").unwrap();
        assert!(manager.get_connector("lifecycle-test").is_none());
    }

    #[test]
    fn test_config_validation() {
        let manager = ConnectorManager::new();

        // Missing required config
        let config = HashMap::new();
        let result = manager.validate_config("test", config);
        assert!(result.error_count > 0);

        // Valid config
        let mut config = HashMap::new();
        config.insert("connector.class".to_string(), "TestConnector".to_string());
        config.insert("tasks.max".to_string(), "1".to_string());

        let result = manager.validate_config("test", config);
        assert_eq!(result.error_count, 0);
    }

    fn create_test_connector(manager: &ConnectorManager, name: &str) {
        let mut config = HashMap::new();
        config.insert(
            "connector.class".to_string(),
            "io.streamline.connect.file.FileStreamSourceConnector".to_string(),
        );
        config.insert("tasks.max".to_string(), "1".to_string());
        let request = CreateConnectorRequest {
            name: name.to_string(),
            config,
        };
        manager.create_connector(request).unwrap();
    }

    #[test]
    fn test_offset_tracking() {
        let manager = ConnectorManager::new();
        create_test_connector(&manager, "offset-test");

        // Initially empty offsets
        let offsets = manager.get_connector_offsets("offset-test").unwrap();
        assert!(offsets.offsets.is_empty());

        // Commit offsets
        let mut new_offsets = HashMap::new();
        new_offsets.insert("topic-0:0".to_string(), "42".to_string());
        new_offsets.insert("topic-0:1".to_string(), "100".to_string());
        manager.commit_offsets("offset-test", new_offsets).unwrap();

        // Verify offsets
        let offsets = manager.get_connector_offsets("offset-test").unwrap();
        assert_eq!(offsets.offsets.len(), 2);
        assert_eq!(offsets.connector, "offset-test");
    }

    #[test]
    fn test_offset_reset() {
        let manager = ConnectorManager::new();
        create_test_connector(&manager, "reset-test");

        // Commit offsets
        let mut new_offsets = HashMap::new();
        new_offsets.insert("partition-0".to_string(), "10".to_string());
        manager.commit_offsets("reset-test", new_offsets).unwrap();

        // Cannot reset while running
        let result = manager.reset_connector_offsets("reset-test");
        assert!(result.is_err());

        // Pause first, then reset
        manager.pause_connector("reset-test").unwrap();
        manager.reset_connector_offsets("reset-test").unwrap();

        let offsets = manager.get_connector_offsets("reset-test").unwrap();
        assert!(offsets.offsets.is_empty());
    }

    #[test]
    fn test_offset_not_found() {
        let manager = ConnectorManager::new();
        let result = manager.get_connector_offsets("nonexistent");
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().error_code, 404);
    }

    #[test]
    fn test_restart_count() {
        let manager = ConnectorManager::new();
        create_test_connector(&manager, "restart-count-test");

        assert_eq!(manager.get_restart_count("restart-count-test"), Some(0));

        manager
            .restart_connector("restart-count-test", false, false)
            .unwrap();
        assert_eq!(manager.get_restart_count("restart-count-test"), Some(1));

        manager
            .restart_connector("restart-count-test", true, false)
            .unwrap();
        assert_eq!(manager.get_restart_count("restart-count-test"), Some(2));
    }

    #[test]
    fn test_delete_full_cleanup() {
        let manager = ConnectorManager::new();
        create_test_connector(&manager, "cleanup-test");

        // Add offsets
        let mut offsets = HashMap::new();
        offsets.insert("p0".to_string(), "5".to_string());
        manager.commit_offsets("cleanup-test", offsets).unwrap();

        // Add DLQ entry
        manager.add_dlq_entry(DlqEntry {
            connector: "cleanup-test".to_string(),
            task_id: 0,
            original_topic: "test".to_string(),
            original_partition: 0,
            original_offset: 0,
            error: "test error".to_string(),
            timestamp: 0,
            value: None,
        });

        // Delete with full cleanup
        manager.delete_connector_full("cleanup-test").unwrap();

        // Everything should be cleaned up
        assert!(manager.get_connector("cleanup-test").is_none());
        assert!(manager.get_connector_offsets("cleanup-test").is_err());
    }
}
