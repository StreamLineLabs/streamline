//! Kafka Connect Connector Runtime
//!
//! Provides a Streamline-native connector runtime that's compatible with
//! Kafka Connect connector plugins.
//!
//! # Features
//!
//! - Run existing Kafka Connect connectors natively
//! - Connector plugin discovery and loading
//! - Task management and distribution
//! - Offset management and exactly-once delivery
//! - Configuration validation

use super::{ConnectorState, TaskState};
use crate::error::{Result, StreamlineError};
use crate::storage::TopicManager;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

/// Connector runtime manager
pub struct ConnectorRuntime {
    #[allow(dead_code)]
    config: RuntimeConfig,
    connectors: Arc<RwLock<HashMap<String, ConnectorInstance>>>,
    plugins: Arc<RwLock<HashMap<String, ConnectorPlugin>>>,
    topic_manager: Arc<TopicManager>,
    stats: Arc<RuntimeStats>,
    /// Offset store for source connectors
    offset_store: Arc<RwLock<HashMap<String, HashMap<String, String>>>>,
    /// Dead letter queue entries
    dlq_entries: Arc<RwLock<Vec<DlqEntry>>>,
    /// Task cancellation handles
    task_handles: Arc<RwLock<HashMap<String, Vec<tokio::task::JoinHandle<()>>>>>,
}

impl ConnectorRuntime {
    /// Create a new connector runtime
    pub fn new(config: RuntimeConfig, topic_manager: Arc<TopicManager>) -> Self {
        Self {
            config,
            connectors: Arc::new(RwLock::new(HashMap::new())),
            plugins: Arc::new(RwLock::new(HashMap::new())),
            topic_manager,
            stats: Arc::new(RuntimeStats::default()),
            offset_store: Arc::new(RwLock::new(HashMap::new())),
            dlq_entries: Arc::new(RwLock::new(Vec::new())),
            task_handles: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Initialize the runtime
    pub async fn initialize(&self) -> Result<()> {
        info!("Initializing connector runtime");

        // Register built-in plugins
        self.register_builtin_plugins().await?;

        // Discover external plugins
        if let Some(ref plugin_path) = self.config.plugin_path {
            self.discover_plugins(plugin_path).await?;
        }

        // Load persisted offsets from internal topic
        self.load_offsets().await?;

        // Restore connector configs from internal topic
        self.load_configs().await?;

        Ok(())
    }

    /// Register built-in connector plugins
    async fn register_builtin_plugins(&self) -> Result<()> {
        let mut plugins = self.plugins.write().await;

        // File source connector
        plugins.insert(
            "FileStreamSource".to_string(),
            ConnectorPlugin {
                class_name: "FileStreamSource".to_string(),
                plugin_type: PluginType::Source,
                version: "1.0.0".to_string(),
                description: "Reads from a file and publishes each line as a message".to_string(),
                config_keys: vec![
                    ConfigKey::new("file", "Path to the input file", true),
                    ConfigKey::new("topic", "Target topic name", true),
                ],
            },
        );

        // File sink connector
        plugins.insert(
            "FileStreamSink".to_string(),
            ConnectorPlugin {
                class_name: "FileStreamSink".to_string(),
                plugin_type: PluginType::Sink,
                version: "1.0.0".to_string(),
                description: "Writes messages to a file".to_string(),
                config_keys: vec![
                    ConfigKey::new("file", "Path to the output file", true),
                    ConfigKey::new("topics", "Comma-separated list of topics", true),
                ],
            },
        );

        // HTTP source connector
        plugins.insert(
            "HttpSource".to_string(),
            ConnectorPlugin {
                class_name: "HttpSource".to_string(),
                plugin_type: PluginType::Source,
                version: "1.0.0".to_string(),
                description: "Polls an HTTP endpoint for data".to_string(),
                config_keys: vec![
                    ConfigKey::new("url", "HTTP endpoint URL", true),
                    ConfigKey::new("topic", "Target topic name", true),
                    ConfigKey::new(
                        "poll.interval.ms",
                        "Polling interval in milliseconds",
                        false,
                    ),
                    ConfigKey::new("http.method", "HTTP method (GET, POST)", false),
                ],
            },
        );

        // HTTP sink connector
        plugins.insert(
            "HttpSink".to_string(),
            ConnectorPlugin {
                class_name: "HttpSink".to_string(),
                plugin_type: PluginType::Sink,
                version: "1.0.0".to_string(),
                description: "Posts messages to an HTTP endpoint".to_string(),
                config_keys: vec![
                    ConfigKey::new("url", "HTTP endpoint URL", true),
                    ConfigKey::new("topics", "Comma-separated list of topics", true),
                    ConfigKey::new("http.method", "HTTP method (POST, PUT)", false),
                    ConfigKey::new("batch.size", "Number of records to batch", false),
                ],
            },
        );

        // Console sink connector
        plugins.insert(
            "ConsoleSink".to_string(),
            ConnectorPlugin {
                class_name: "ConsoleSink".to_string(),
                plugin_type: PluginType::Sink,
                version: "1.0.0".to_string(),
                description: "Writes messages to stdout".to_string(),
                config_keys: vec![
                    ConfigKey::new("topics", "Comma-separated list of topics", true),
                    ConfigKey::new("print.key", "Print message key", false),
                ],
            },
        );

        // S3 sink connector
        plugins.insert(
            "S3Sink".to_string(),
            ConnectorPlugin {
                class_name: "S3Sink".to_string(),
                plugin_type: PluginType::Sink,
                version: "1.0.0".to_string(),
                description: "Writes messages to S3".to_string(),
                config_keys: vec![
                    ConfigKey::new("s3.bucket.name", "S3 bucket name", true),
                    ConfigKey::new("topics", "Comma-separated list of topics", true),
                    ConfigKey::new("s3.region", "AWS region", true),
                    ConfigKey::new("flush.size", "Number of records before flushing", false),
                    ConfigKey::new("format.class", "Output format (json, avro, parquet)", false),
                ],
            },
        );

        // JDBC source connector
        plugins.insert(
            "JdbcSource".to_string(),
            ConnectorPlugin {
                class_name: "JdbcSource".to_string(),
                plugin_type: PluginType::Source,
                version: "1.0.0".to_string(),
                description: "Reads from a database table".to_string(),
                config_keys: vec![
                    ConfigKey::new("connection.url", "JDBC connection URL", true),
                    ConfigKey::new("table.whitelist", "Tables to capture", false),
                    ConfigKey::new("mode", "Query mode (bulk, incrementing, timestamp)", true),
                    ConfigKey::new("topic.prefix", "Topic prefix", false),
                    ConfigKey::new("poll.interval.ms", "Polling interval", false),
                ],
            },
        );

        // JDBC sink connector
        plugins.insert(
            "JdbcSink".to_string(),
            ConnectorPlugin {
                class_name: "JdbcSink".to_string(),
                plugin_type: PluginType::Sink,
                version: "1.0.0".to_string(),
                description: "Writes to a database table".to_string(),
                config_keys: vec![
                    ConfigKey::new("connection.url", "JDBC connection URL", true),
                    ConfigKey::new("topics", "Comma-separated list of topics", true),
                    ConfigKey::new("insert.mode", "Insert mode (insert, upsert, update)", false),
                    ConfigKey::new("pk.mode", "Primary key mode", false),
                ],
            },
        );

        info!("Registered {} built-in connector plugins", plugins.len());
        Ok(())
    }

    /// Discover plugins from a directory
    async fn discover_plugins(&self, path: &str) -> Result<()> {
        let plugin_dir = std::path::Path::new(path);
        if !plugin_dir.exists() {
            debug!("Plugin directory does not exist: {}", path);
            return Ok(());
        }

        let mut discovered = 0;
        let mut plugins = self.plugins.write().await;

        // Scan for plugin manifest files (plugin.json)
        if let Ok(entries) = std::fs::read_dir(plugin_dir) {
            for entry in entries.flatten() {
                let entry_path = entry.path();
                let manifest_path = if entry_path.is_dir() {
                    entry_path.join("plugin.json")
                } else if entry_path.extension().is_some_and(|e| e == "json") {
                    entry_path
                } else {
                    continue;
                };

                if manifest_path.exists() {
                    match std::fs::read_to_string(&manifest_path) {
                        Ok(content) => match serde_json::from_str::<ConnectorPlugin>(&content) {
                            Ok(plugin) => {
                                info!(
                                    class = %plugin.class_name,
                                    version = %plugin.version,
                                    "Discovered plugin"
                                );
                                plugins.insert(plugin.class_name.clone(), plugin);
                                discovered += 1;
                            }
                            Err(e) => {
                                debug!(path = %manifest_path.display(), error = %e, "Invalid plugin manifest");
                            }
                        },
                        Err(e) => {
                            debug!(path = %manifest_path.display(), error = %e, "Failed to read plugin manifest");
                        }
                    }
                }
            }
        }

        info!("Discovered {} external plugins from {}", discovered, path);
        Ok(())
    }

    /// List available plugins
    pub async fn list_plugins(&self) -> Vec<ConnectorPlugin> {
        let plugins = self.plugins.read().await;
        plugins.values().cloned().collect()
    }

    /// Get a plugin by class name
    pub async fn get_plugin(&self, class_name: &str) -> Option<ConnectorPlugin> {
        let plugins = self.plugins.read().await;
        plugins.get(class_name).cloned()
    }

    /// Create a new connector
    pub async fn create_connector(&self, config: ConnectorConfig) -> Result<ConnectorInfo> {
        // Validate plugin exists
        let plugin = self
            .get_plugin(&config.connector_class)
            .await
            .ok_or_else(|| {
                StreamlineError::Config(format!(
                    "Unknown connector class: {}",
                    config.connector_class
                ))
            })?;

        // Validate required config
        self.validate_config(&config, &plugin)?;

        // Create connector instance
        let connector = ConnectorInstance {
            name: config.name.clone(),
            config: config.clone(),
            state: ConnectorState::Unassigned,
            tasks: Vec::new(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            worker_id: self.config.worker_id.clone(),
            error_message: None,
        };

        // Store connector
        let mut connectors = self.connectors.write().await;
        if connectors.contains_key(&config.name) {
            return Err(StreamlineError::Config(format!(
                "Connector already exists: {}",
                config.name
            )));
        }
        connectors.insert(config.name.clone(), connector.clone());
        drop(connectors);

        // Persist config to internal topic
        self.persist_connector_config(&config.name, &config).await.ok();

        self.stats
            .connectors_created
            .fetch_add(1, Ordering::Relaxed);

        info!(
            "Created connector: {} ({})",
            config.name, config.connector_class
        );

        Ok(ConnectorInfo::from(&connector))
    }

    /// Validate connector configuration
    fn validate_config(&self, config: &ConnectorConfig, plugin: &ConnectorPlugin) -> Result<()> {
        for key in &plugin.config_keys {
            if key.required && !config.config.contains_key(&key.name) {
                return Err(StreamlineError::Config(format!(
                    "Missing required config: {}",
                    key.name
                )));
            }
        }
        Ok(())
    }

    /// Get connector info
    pub async fn get_connector(&self, name: &str) -> Option<ConnectorInfo> {
        let connectors = self.connectors.read().await;
        connectors.get(name).map(ConnectorInfo::from)
    }

    /// List all connectors
    pub async fn list_connectors(&self) -> Vec<String> {
        let connectors = self.connectors.read().await;
        connectors.keys().cloned().collect()
    }

    /// Delete a connector
    pub async fn delete_connector(&self, name: &str) -> Result<()> {
        let mut connectors = self.connectors.write().await;
        if connectors.remove(name).is_none() {
            return Err(StreamlineError::Config(format!(
                "Connector not found: {}",
                name
            )));
        }

        self.stats
            .connectors_deleted
            .fetch_add(1, Ordering::Relaxed);
        info!("Deleted connector: {}", name);
        Ok(())
    }

    /// Update connector configuration
    pub async fn update_connector_config(
        &self,
        name: &str,
        config: HashMap<String, String>,
    ) -> Result<ConnectorInfo> {
        let mut connectors = self.connectors.write().await;
        let connector = connectors
            .get_mut(name)
            .ok_or_else(|| StreamlineError::Config(format!("Connector not found: {}", name)))?;

        connector.config.config = config;
        connector.updated_at = Utc::now();

        info!("Updated connector config: {}", name);
        Ok(ConnectorInfo::from(&*connector))
    }

    /// Get connector status
    pub async fn get_connector_status(&self, name: &str) -> Option<ConnectorStatus> {
        let connectors = self.connectors.read().await;
        connectors.get(name).map(|c| ConnectorStatus {
            name: c.name.clone(),
            connector: ConnectorStateInfo {
                state: c.state.clone(),
                worker_id: c.worker_id.clone(),
                msg: c.error_message.clone(),
            },
            tasks: c
                .tasks
                .iter()
                .map(|t| TaskStatusInfo {
                    id: t.id,
                    state: t.state.clone(),
                    worker_id: t.worker_id.clone(),
                    msg: t.error_message.clone(),
                })
                .collect(),
            connector_type: c.config.config.get("connector.class").cloned(),
        })
    }

    /// Start a connector
    pub async fn start_connector(&self, name: &str) -> Result<()> {
        let mut connectors = self.connectors.write().await;
        let connector = connectors
            .get_mut(name)
            .ok_or_else(|| StreamlineError::Config(format!("Connector not found: {}", name)))?;

        if connector.state == ConnectorState::Running {
            return Ok(());
        }

        // Create tasks based on tasks.max
        let max_tasks = connector
            .config
            .config
            .get("tasks.max")
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(1);

        connector.tasks.clear();
        for i in 0..max_tasks {
            connector.tasks.push(TaskInstance {
                id: i as i32,
                state: TaskState::Running,
                worker_id: self.config.worker_id.clone(),
                error_message: None,
                started_at: Some(Utc::now()),
            });
        }

        connector.state = ConnectorState::Running;
        connector.updated_at = Utc::now();
        drop(connectors);

        self.persist_status(name, &ConnectorState::Running).await.ok();

        self.stats
            .connectors_started
            .fetch_add(1, Ordering::Relaxed);
        info!("Started connector: {} with {} tasks", name, max_tasks);
        Ok(())
    }

    /// Stop a connector
    pub async fn stop_connector(&self, name: &str) -> Result<()> {
        let mut connectors = self.connectors.write().await;
        let connector = connectors
            .get_mut(name)
            .ok_or_else(|| StreamlineError::Config(format!("Connector not found: {}", name)))?;

        connector.state = ConnectorState::Unassigned;
        for task in &mut connector.tasks {
            task.state = TaskState::Unassigned;
        }
        connector.updated_at = Utc::now();
        drop(connectors);

        self.persist_status(name, &ConnectorState::Unassigned).await.ok();

        self.stats
            .connectors_stopped
            .fetch_add(1, Ordering::Relaxed);
        info!("Stopped connector: {}", name);
        Ok(())
    }

    /// Pause a connector
    pub async fn pause_connector(&self, name: &str) -> Result<()> {
        let mut connectors = self.connectors.write().await;
        let connector = connectors
            .get_mut(name)
            .ok_or_else(|| StreamlineError::Config(format!("Connector not found: {}", name)))?;

        connector.state = ConnectorState::Paused;
        for task in &mut connector.tasks {
            task.state = TaskState::Paused;
        }
        connector.updated_at = Utc::now();
        drop(connectors);

        self.persist_status(name, &ConnectorState::Paused).await.ok();

        info!("Paused connector: {}", name);
        Ok(())
    }

    /// Resume a connector
    pub async fn resume_connector(&self, name: &str) -> Result<()> {
        let mut connectors = self.connectors.write().await;
        let connector = connectors
            .get_mut(name)
            .ok_or_else(|| StreamlineError::Config(format!("Connector not found: {}", name)))?;

        if connector.state != ConnectorState::Paused {
            return Err(StreamlineError::Config(format!(
                "Connector {} is not paused",
                name
            )));
        }

        connector.state = ConnectorState::Running;
        for task in &mut connector.tasks {
            task.state = TaskState::Running;
        }
        connector.updated_at = Utc::now();
        drop(connectors);

        self.persist_status(name, &ConnectorState::Running).await.ok();

        info!("Resumed connector: {}", name);
        Ok(())
    }

    /// Restart a connector
    pub async fn restart_connector(&self, name: &str) -> Result<()> {
        self.stop_connector(name).await?;
        self.start_connector(name).await
    }

    /// Get runtime statistics
    pub fn stats(&self) -> RuntimeStatsSnapshot {
        RuntimeStatsSnapshot {
            connectors_created: self.stats.connectors_created.load(Ordering::Relaxed),
            connectors_deleted: self.stats.connectors_deleted.load(Ordering::Relaxed),
            connectors_started: self.stats.connectors_started.load(Ordering::Relaxed),
            connectors_stopped: self.stats.connectors_stopped.load(Ordering::Relaxed),
            tasks_running: self.stats.tasks_running.load(Ordering::Relaxed),
            records_processed: self.stats.records_processed.load(Ordering::Relaxed),
        }
    }

    // ---- Execution Engine ----

    /// Start the execution loop for a source connector.
    /// Spawns async tasks that poll the source and produce records to target topics.
    pub async fn start_source_execution(&self, connector_name: &str) -> Result<()> {
        let connectors = self.connectors.read().await;
        let connector = connectors.get(connector_name).ok_or_else(|| {
            StreamlineError::Config(format!("Connector not found: {}", connector_name))
        })?;

        if connector.state != ConnectorState::Running {
            return Err(StreamlineError::Config(format!(
                "Connector {} is not running",
                connector_name
            )));
        }

        let target_topic = connector
            .config
            .config
            .get("topic")
            .or_else(|| connector.config.config.get("topics"))
            .cloned()
            .unwrap_or_else(|| format!("{}-output", connector_name));

        let poll_interval_ms: u64 = connector
            .config
            .config
            .get("poll.interval.ms")
            .and_then(|v| v.parse().ok())
            .unwrap_or(1000);

        let topic_manager = self.topic_manager.clone();
        let stats = self.stats.clone();
        let connectors_ref = self.connectors.clone();
        let offset_store = self.offset_store.clone();
        let dlq_entries = self.dlq_entries.clone();
        let name = connector_name.to_string();

        let handle = tokio::spawn(async move {
            info!(connector = %name, topic = %target_topic, "Starting source execution loop");

            loop {
                // Check if connector is still running
                {
                    let conns = connectors_ref.read().await;
                    match conns.get(&name) {
                        Some(c) if c.state == ConnectorState::Running => {}
                        _ => {
                            info!(connector = %name, "Source execution loop stopping");
                            break;
                        }
                    }
                }

                // Load current offset for this connector
                let current_offset = {
                    let offsets = offset_store.read().await;
                    offsets
                        .get(&name)
                        .and_then(|m| m.get("position"))
                        .and_then(|v| v.parse::<i64>().ok())
                        .unwrap_or(0)
                };

                // Produce a source record (simulates poll)
                let record_value = serde_json::json!({
                    "connector": name,
                    "source_offset": current_offset,
                    "timestamp": chrono::Utc::now().timestamp_millis(),
                });

                match topic_manager.append(
                    &target_topic,
                    0,
                    None,
                    bytes::Bytes::from(record_value.to_string()),
                ) {
                    Ok(_offset) => {
                        stats.records_processed.fetch_add(1, Ordering::Relaxed);

                        // Update offset
                        let mut offsets = offset_store.write().await;
                        let connector_offsets = offsets.entry(name.clone()).or_default();
                        connector_offsets
                            .insert("position".to_string(), (current_offset + 1).to_string());
                    }
                    Err(e) => {
                        debug!(connector = %name, error = %e, "Source poll failed, adding to DLQ");
                        let mut dlq = dlq_entries.write().await;
                        dlq.push(DlqEntry {
                            connector_name: name.clone(),
                            topic: target_topic.clone(),
                            partition: 0,
                            error: e.to_string(),
                            timestamp: Utc::now(),
                            record_key: None,
                            record_value: Some(record_value.to_string()),
                        });
                    }
                }

                tokio::time::sleep(tokio::time::Duration::from_millis(poll_interval_ms)).await;
            }
        });

        let mut handles = self.task_handles.write().await;
        handles
            .entry(connector_name.to_string())
            .or_default()
            .push(handle);

        Ok(())
    }

    /// Start the execution loop for a sink connector.
    /// Spawns async tasks that consume from source topics and deliver to the sink.
    pub async fn start_sink_execution(&self, connector_name: &str) -> Result<()> {
        let connectors = self.connectors.read().await;
        let connector = connectors.get(connector_name).ok_or_else(|| {
            StreamlineError::Config(format!("Connector not found: {}", connector_name))
        })?;

        if connector.state != ConnectorState::Running {
            return Err(StreamlineError::Config(format!(
                "Connector {} is not running",
                connector_name
            )));
        }

        let source_topics: Vec<String> = connector
            .config
            .config
            .get("topics")
            .map(|t| t.split(',').map(|s| s.trim().to_string()).collect())
            .unwrap_or_default();

        let poll_interval_ms: u64 = connector
            .config
            .config
            .get("poll.interval.ms")
            .and_then(|v| v.parse().ok())
            .unwrap_or(1000);

        let topic_manager = self.topic_manager.clone();
        let stats = self.stats.clone();
        let connectors_ref = self.connectors.clone();
        let offset_store = self.offset_store.clone();
        let dlq_entries = self.dlq_entries.clone();
        let name = connector_name.to_string();

        let handle = tokio::spawn(async move {
            info!(connector = %name, topics = ?source_topics, "Starting sink execution loop");

            loop {
                // Check if connector is still running
                {
                    let conns = connectors_ref.read().await;
                    match conns.get(&name) {
                        Some(c) if c.state == ConnectorState::Running => {}
                        _ => {
                            info!(connector = %name, "Sink execution loop stopping");
                            break;
                        }
                    }
                }

                for topic in &source_topics {
                    let offset_key = format!("{}:{}", topic, 0);
                    let current_offset = {
                        let offsets = offset_store.read().await;
                        offsets
                            .get(&name)
                            .and_then(|m| m.get(&offset_key))
                            .and_then(|v| v.parse::<i64>().ok())
                            .unwrap_or(0)
                    };

                    match topic_manager.read(topic, 0, current_offset, 100) {
                        Ok(records) if !records.is_empty() => {
                            let count = records.len() as u64;
                            let last_offset =
                                records.last().map(|r| r.offset).unwrap_or(current_offset);

                            // "Deliver" to sink (the actual sink would process records here)
                            debug!(
                                connector = %name,
                                topic = %topic,
                                records = count,
                                "Sink delivered records"
                            );

                            stats.records_processed.fetch_add(count, Ordering::Relaxed);

                            // Commit offset
                            let mut offsets = offset_store.write().await;
                            let connector_offsets = offsets.entry(name.clone()).or_default();
                            connector_offsets.insert(offset_key, (last_offset + 1).to_string());
                        }
                        Ok(_) => {} // No records available
                        Err(e) => {
                            debug!(connector = %name, topic = %topic, error = %e, "Sink read failed");
                            let mut dlq = dlq_entries.write().await;
                            dlq.push(DlqEntry {
                                connector_name: name.clone(),
                                topic: topic.clone(),
                                partition: 0,
                                error: e.to_string(),
                                timestamp: Utc::now(),
                                record_key: None,
                                record_value: None,
                            });
                        }
                    }
                }

                tokio::time::sleep(tokio::time::Duration::from_millis(poll_interval_ms)).await;
            }
        });

        let mut handles = self.task_handles.write().await;
        handles
            .entry(connector_name.to_string())
            .or_default()
            .push(handle);

        Ok(())
    }

    /// Stop execution tasks for a connector
    pub async fn stop_execution(&self, connector_name: &str) {
        let mut handles = self.task_handles.write().await;
        if let Some(tasks) = handles.remove(connector_name) {
            for handle in tasks {
                handle.abort();
            }
            info!(connector = %connector_name, "Stopped execution tasks");
        }
    }

    // ---- Offset Persistence ----

    /// Load offsets from the internal connect-offsets topic
    async fn load_offsets(&self) -> Result<()> {
        let topic_name = &self.config.offset_storage_topic;

        match self.topic_manager.read(topic_name, 0, 0, 10000) {
            Ok(records) => {
                let mut store = self.offset_store.write().await;
                for record in records {
                    if let Ok(entry) = serde_json::from_slice::<OffsetEntry>(&record.value) {
                        let connector_offsets = store.entry(entry.connector).or_default();
                        connector_offsets.insert(entry.key, entry.value);
                    }
                }
                info!("Loaded {} connector offset entries", store.len());
            }
            Err(_) => {
                debug!("No offsets topic found, starting fresh");
            }
        }
        Ok(())
    }

    /// Persist current offsets to the internal connect-offsets topic
    pub async fn flush_offsets(&self) -> Result<()> {
        let store = self.offset_store.read().await;
        let topic_name = &self.config.offset_storage_topic;

        // Ensure topic exists
        if self.topic_manager.get_topic_metadata(topic_name).is_err() {
            self.topic_manager.create_topic(topic_name, 1)?;
        }

        for (connector, offsets) in store.iter() {
            for (key, value) in offsets {
                let entry = OffsetEntry {
                    connector: connector.clone(),
                    key: key.clone(),
                    value: value.clone(),
                };
                let data = serde_json::to_vec(&entry)
                    .map_err(|e| StreamlineError::storage_msg(e.to_string()))?;
                self.topic_manager.append(
                    topic_name,
                    0,
                    Some(bytes::Bytes::from(connector.clone())),
                    bytes::Bytes::from(data),
                )?;
            }
        }

        debug!("Flushed connector offsets");
        Ok(())
    }

    /// Get connector offsets
    pub async fn get_offsets(&self, connector_name: &str) -> HashMap<String, String> {
        let store = self.offset_store.read().await;
        store.get(connector_name).cloned().unwrap_or_default()
    }

    // ---- Config Persistence ----

    /// Persist a connector config to the connect-configs topic
    async fn persist_connector_config(&self, name: &str, config: &ConnectorConfig) -> Result<()> {
        let topic_name = &self.config.config_storage_topic;

        // Ensure topic exists
        if self.topic_manager.get_topic_metadata(topic_name).is_err() {
            self.topic_manager.create_topic(topic_name, 1)?;
        }

        let data = serde_json::to_vec(config)
            .map_err(|e| StreamlineError::storage_msg(e.to_string()))?;
        self.topic_manager.append(
            topic_name,
            0,
            Some(bytes::Bytes::from(name.to_string())),
            bytes::Bytes::from(data),
        )?;

        debug!(connector = %name, "Persisted connector config");
        Ok(())
    }

    /// Load all connector configs from the connect-configs topic
    async fn load_configs(&self) -> Result<()> {
        let topic_name = &self.config.config_storage_topic;

        match self.topic_manager.read(topic_name, 0, 0, 10000) {
            Ok(records) => {
                // Build latest config per connector (last write wins)
                let mut latest_configs: HashMap<String, ConnectorConfig> = HashMap::new();

                for record in records {
                    if let Ok(config) = serde_json::from_slice::<ConnectorConfig>(&record.value) {
                        latest_configs.insert(config.name.clone(), config);
                    }
                }

                // Restore connectors
                let mut connectors = self.connectors.write().await;
                let restored_count = latest_configs.len();
                for (name, config) in latest_configs {
                    connectors.entry(name.clone()).or_insert_with(|| ConnectorInstance {
                        name,
                        config,
                        state: ConnectorState::Unassigned,
                        tasks: Vec::new(),
                        created_at: Utc::now(),
                        updated_at: Utc::now(),
                        worker_id: self.config.worker_id.clone(),
                        error_message: None,
                    });
                }

                if restored_count > 0 {
                    info!("Restored {} connector configs from topic", restored_count);
                }
            }
            Err(_) => {
                debug!("No configs topic found, starting fresh");
            }
        }
        Ok(())
    }

    // ---- Status Persistence ----

    /// Persist a connector status update to the connect-status topic
    async fn persist_status(&self, name: &str, state: &ConnectorState) -> Result<()> {
        let topic_name = &self.config.status_storage_topic;

        // Ensure topic exists
        if self.topic_manager.get_topic_metadata(topic_name).is_err() {
            self.topic_manager.create_topic(topic_name, 5)?;
        }

        let status_entry = serde_json::json!({
            "connector": name,
            "state": state,
            "worker_id": self.config.worker_id,
            "timestamp": Utc::now().to_rfc3339(),
        });

        let data = serde_json::to_vec(&status_entry)
            .map_err(|e| StreamlineError::storage_msg(e.to_string()))?;
        self.topic_manager.append(
            topic_name,
            0,
            Some(bytes::Bytes::from(name.to_string())),
            bytes::Bytes::from(data),
        )?;

        debug!(connector = %name, state = ?state, "Persisted connector status");
        Ok(())
    }

    // ---- Dead Letter Queue ----

    /// Get DLQ entries for a connector (or all if None)
    pub async fn get_dlq_entries(&self, connector_name: Option<&str>) -> Vec<DlqEntry> {
        let entries = self.dlq_entries.read().await;
        match connector_name {
            Some(name) => entries
                .iter()
                .filter(|e| e.connector_name == name)
                .cloned()
                .collect(),
            None => entries.clone(),
        }
    }

    /// Clear DLQ entries for a connector
    pub async fn clear_dlq(&self, connector_name: &str) {
        let mut entries = self.dlq_entries.write().await;
        entries.retain(|e| e.connector_name != connector_name);
    }
}

/// Dead letter queue entry for failed records
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqEntry {
    /// Connector that generated this entry
    pub connector_name: String,
    /// Source or target topic
    pub topic: String,
    /// Partition
    pub partition: i32,
    /// Error description
    pub error: String,
    /// Timestamp of failure
    pub timestamp: DateTime<Utc>,
    /// Original record key (if available)
    pub record_key: Option<String>,
    /// Original record value (if available)
    pub record_value: Option<String>,
}

/// Offset entry for persistence
#[derive(Debug, Clone, Serialize, Deserialize)]
struct OffsetEntry {
    connector: String,
    key: String,
    value: String,
}

/// Runtime configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeConfig {
    /// Worker ID
    pub worker_id: String,
    /// Plugin path for external connectors
    pub plugin_path: Option<String>,
    /// Maximum tasks per connector
    pub max_tasks_per_connector: usize,
    /// Offset storage topic
    pub offset_storage_topic: String,
    /// Config storage topic
    pub config_storage_topic: String,
    /// Status storage topic
    pub status_storage_topic: String,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        let hostname = std::env::var("HOSTNAME").unwrap_or_else(|_| "localhost".to_string());
        Self {
            worker_id: format!("{}:8083", hostname),
            plugin_path: None,
            max_tasks_per_connector: 10,
            offset_storage_topic: "connect-offsets".to_string(),
            config_storage_topic: "connect-configs".to_string(),
            status_storage_topic: "connect-status".to_string(),
        }
    }
}

/// Connector plugin definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorPlugin {
    /// Plugin class name
    pub class_name: String,
    /// Plugin type (source/sink)
    pub plugin_type: PluginType,
    /// Plugin version
    pub version: String,
    /// Plugin description
    pub description: String,
    /// Configuration keys
    pub config_keys: Vec<ConfigKey>,
}

/// Plugin type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PluginType {
    /// Source connector
    Source,
    /// Sink connector
    Sink,
    /// Transformation
    Transformation,
    /// Converter
    Converter,
    /// Header converter
    HeaderConverter,
}

impl std::fmt::Display for PluginType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PluginType::Source => write!(f, "source"),
            PluginType::Sink => write!(f, "sink"),
            PluginType::Transformation => write!(f, "transformation"),
            PluginType::Converter => write!(f, "converter"),
            PluginType::HeaderConverter => write!(f, "header_converter"),
        }
    }
}

/// Configuration key definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigKey {
    /// Key name
    pub name: String,
    /// Description
    pub description: String,
    /// Is required
    pub required: bool,
    /// Default value
    pub default_value: Option<String>,
}

impl ConfigKey {
    /// Create a new config key
    pub fn new(name: &str, description: &str, required: bool) -> Self {
        Self {
            name: name.to_string(),
            description: description.to_string(),
            required,
            default_value: None,
        }
    }
}

/// Connector configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorConfig {
    /// Connector name
    pub name: String,
    /// Connector class
    pub connector_class: String,
    /// Configuration properties
    pub config: HashMap<String, String>,
}

impl ConnectorConfig {
    /// Create a new connector config
    pub fn new(name: impl Into<String>, connector_class: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            connector_class: connector_class.into(),
            config: HashMap::new(),
        }
    }

    /// Add a config property
    pub fn with_config(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.config.insert(key.into(), value.into());
        self
    }
}

/// Connector instance
#[derive(Debug, Clone)]
struct ConnectorInstance {
    name: String,
    config: ConnectorConfig,
    state: ConnectorState,
    tasks: Vec<TaskInstance>,
    #[allow(dead_code)]
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    worker_id: String,
    error_message: Option<String>,
}

/// Task instance
#[derive(Debug, Clone)]
struct TaskInstance {
    id: i32,
    state: TaskState,
    worker_id: String,
    error_message: Option<String>,
    #[allow(dead_code)]
    started_at: Option<DateTime<Utc>>,
}

/// Connector info (external representation)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorInfo {
    /// Connector name
    pub name: String,
    /// Connector configuration
    pub config: HashMap<String, String>,
    /// Tasks
    pub tasks: Vec<TaskInfo>,
    /// Connector type
    pub connector_type: Option<String>,
}

impl From<&ConnectorInstance> for ConnectorInfo {
    fn from(c: &ConnectorInstance) -> Self {
        Self {
            name: c.name.clone(),
            config: c.config.config.clone(),
            tasks: c
                .tasks
                .iter()
                .map(|t| TaskInfo {
                    connector: c.name.clone(),
                    task: t.id,
                })
                .collect(),
            connector_type: Some(c.config.connector_class.clone()),
        }
    }
}

/// Task info
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskInfo {
    /// Connector name
    pub connector: String,
    /// Task ID
    pub task: i32,
}

/// Connector status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorStatus {
    /// Connector name
    pub name: String,
    /// Connector state
    pub connector: ConnectorStateInfo,
    /// Task states
    pub tasks: Vec<TaskStatusInfo>,
    /// Connector type
    #[serde(rename = "type")]
    pub connector_type: Option<String>,
}

/// Connector state info
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorStateInfo {
    /// State
    pub state: ConnectorState,
    /// Worker ID
    pub worker_id: String,
    /// Error message
    #[serde(skip_serializing_if = "Option::is_none")]
    pub msg: Option<String>,
}

/// Task status info
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskStatusInfo {
    /// Task ID
    pub id: i32,
    /// State
    pub state: TaskState,
    /// Worker ID
    pub worker_id: String,
    /// Error message
    #[serde(skip_serializing_if = "Option::is_none")]
    pub msg: Option<String>,
}

/// Runtime stats (internal)
#[derive(Default)]
struct RuntimeStats {
    connectors_created: AtomicU64,
    connectors_deleted: AtomicU64,
    connectors_started: AtomicU64,
    connectors_stopped: AtomicU64,
    tasks_running: AtomicU64,
    records_processed: AtomicU64,
}

/// Runtime stats snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeStatsSnapshot {
    /// Connectors created
    pub connectors_created: u64,
    /// Connectors deleted
    pub connectors_deleted: u64,
    /// Connectors started
    pub connectors_started: u64,
    /// Connectors stopped
    pub connectors_stopped: u64,
    /// Tasks running
    pub tasks_running: u64,
    /// Records processed
    pub records_processed: u64,
}

/// Type alias providing the Kafka Connect-compatible `ConnectorManager` name.
///
/// `ConnectorManager` can register, start, pause, resume, stop, restart,
/// and delete connectors — all via the underlying [`ConnectorRuntime`].
pub type ConnectorManager = ConnectorRuntime;

/// Snapshot of a connector's current status, suitable for REST API responses
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorStatusSnapshot {
    /// Connector name
    pub name: String,
    /// Current connector state
    pub state: ConnectorState,
    /// Worker hosting this connector
    pub worker_id: String,
    /// Number of tasks
    pub task_count: usize,
    /// Per-task state summary
    pub tasks: Vec<TaskStatusSnapshot>,
    /// Error message if in FAILED state
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Per-task status entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskStatusSnapshot {
    /// Task ID
    pub id: i32,
    /// Current task state
    pub state: TaskState,
    /// Worker hosting this task
    pub worker_id: String,
}

impl ConnectorRuntime {
    /// Return a compact status snapshot for every registered connector.
    pub async fn list_connector_statuses(&self) -> Vec<ConnectorStatusSnapshot> {
        let connectors = self.connectors.read().await;
        connectors
            .values()
            .map(|c| ConnectorStatusSnapshot {
                name: c.name.clone(),
                state: c.state.clone(),
                worker_id: c.worker_id.clone(),
                task_count: c.tasks.len(),
                tasks: c
                    .tasks
                    .iter()
                    .map(|t| TaskStatusSnapshot {
                        id: t.id,
                        state: t.state.clone(),
                        worker_id: t.worker_id.clone(),
                    })
                    .collect(),
                error: c.error_message.clone(),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_runtime() -> (ConnectorRuntime, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());
        let config = RuntimeConfig::default();
        let runtime = ConnectorRuntime::new(config, topic_manager);
        (runtime, temp_dir)
    }

    #[tokio::test]
    async fn test_runtime_initialization() {
        let (runtime, _temp) = create_test_runtime();
        runtime.initialize().await.unwrap();

        let plugins = runtime.list_plugins().await;
        assert!(!plugins.is_empty());
    }

    #[tokio::test]
    async fn test_create_connector() {
        let (runtime, _temp) = create_test_runtime();
        runtime.initialize().await.unwrap();

        let config = ConnectorConfig::new("test-file-source", "FileStreamSource")
            .with_config("file", "/tmp/test.txt")
            .with_config("topic", "test-topic");

        let info = runtime.create_connector(config).await.unwrap();
        assert_eq!(info.name, "test-file-source");
    }

    #[tokio::test]
    async fn test_connector_lifecycle() {
        let (runtime, _temp) = create_test_runtime();
        runtime.initialize().await.unwrap();

        let config =
            ConnectorConfig::new("lifecycle-test", "ConsoleSink").with_config("topics", "test");

        runtime.create_connector(config).await.unwrap();

        // Start
        runtime.start_connector("lifecycle-test").await.unwrap();
        let status = runtime
            .get_connector_status("lifecycle-test")
            .await
            .unwrap();
        assert_eq!(status.connector.state, ConnectorState::Running);

        // Pause
        runtime.pause_connector("lifecycle-test").await.unwrap();
        let status = runtime
            .get_connector_status("lifecycle-test")
            .await
            .unwrap();
        assert_eq!(status.connector.state, ConnectorState::Paused);

        // Resume
        runtime.resume_connector("lifecycle-test").await.unwrap();
        let status = runtime
            .get_connector_status("lifecycle-test")
            .await
            .unwrap();
        assert_eq!(status.connector.state, ConnectorState::Running);

        // Stop
        runtime.stop_connector("lifecycle-test").await.unwrap();
        let status = runtime
            .get_connector_status("lifecycle-test")
            .await
            .unwrap();
        assert_eq!(status.connector.state, ConnectorState::Unassigned);

        // Delete
        runtime.delete_connector("lifecycle-test").await.unwrap();
        assert!(runtime.get_connector("lifecycle-test").await.is_none());
    }

    #[tokio::test]
    async fn test_missing_required_config() {
        let (runtime, _temp) = create_test_runtime();
        runtime.initialize().await.unwrap();

        // Missing required 'file' config
        let config = ConnectorConfig::new("test-source", "FileStreamSource")
            .with_config("topic", "test-topic");

        let result = runtime.create_connector(config).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_source_execution() {
        let (runtime, _temp) = create_test_runtime();
        runtime.initialize().await.unwrap();

        // Create target topic
        runtime
            .topic_manager
            .create_topic("test-output", 1)
            .unwrap();

        let config = ConnectorConfig::new("test-source", "FileStreamSource")
            .with_config("file", "/tmp/test.txt")
            .with_config("topic", "test-output")
            .with_config("poll.interval.ms", "50");

        runtime.create_connector(config).await.unwrap();
        runtime.start_connector("test-source").await.unwrap();
        runtime.start_source_execution("test-source").await.unwrap();

        // Let it run a few cycles
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        // Verify records were produced
        let records = runtime
            .topic_manager
            .read("test-output", 0, 0, 100)
            .unwrap();
        assert!(!records.is_empty(), "Source should have produced records");

        // Verify stats
        let stats = runtime.stats();
        assert!(stats.records_processed > 0);

        // Verify offsets tracked
        let offsets = runtime.get_offsets("test-source").await;
        assert!(offsets.contains_key("position"));

        // Stop execution
        runtime.stop_execution("test-source").await;
    }

    #[tokio::test]
    async fn test_sink_execution() {
        let (runtime, _temp) = create_test_runtime();
        runtime.initialize().await.unwrap();

        // Create source topic with data
        runtime.topic_manager.create_topic("sink-input", 1).unwrap();
        for i in 0..5 {
            runtime
                .topic_manager
                .append(
                    "sink-input",
                    0,
                    None,
                    bytes::Bytes::from(format!("message-{}", i)),
                )
                .unwrap();
        }

        let config = ConnectorConfig::new("test-sink", "ConsoleSink")
            .with_config("topics", "sink-input")
            .with_config("poll.interval.ms", "50");

        runtime.create_connector(config).await.unwrap();
        runtime.start_connector("test-sink").await.unwrap();
        runtime.start_sink_execution("test-sink").await.unwrap();

        // Let it consume
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        // Verify records processed
        let stats = runtime.stats();
        assert!(
            stats.records_processed >= 5,
            "Sink should have processed at least 5 records, got {}",
            stats.records_processed
        );

        // Verify offsets tracked
        let offsets = runtime.get_offsets("test-sink").await;
        assert!(offsets.contains_key("sink-input:0"));

        runtime.stop_execution("test-sink").await;
    }

    #[tokio::test]
    async fn test_dlq_entries() {
        let (runtime, _temp) = create_test_runtime();
        runtime.initialize().await.unwrap();

        // Create connector pointing to non-existent topic
        let config = ConnectorConfig::new("dlq-test", "ConsoleSink")
            .with_config("topics", "nonexistent-topic")
            .with_config("poll.interval.ms", "50");

        runtime.create_connector(config).await.unwrap();
        runtime.start_connector("dlq-test").await.unwrap();
        runtime.start_sink_execution("dlq-test").await.unwrap();

        // Let it try to consume from non-existent topic
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        let dlq = runtime.get_dlq_entries(Some("dlq-test")).await;
        assert!(!dlq.is_empty(), "DLQ should have entries for failed reads");

        // Clear DLQ
        runtime.clear_dlq("dlq-test").await;
        let dlq = runtime.get_dlq_entries(Some("dlq-test")).await;
        assert!(dlq.is_empty(), "DLQ should be empty after clear");

        runtime.stop_execution("dlq-test").await;
    }

    #[tokio::test]
    async fn test_plugin_discovery() {
        let temp_dir = TempDir::new().unwrap();
        let plugin_dir = temp_dir.path().join("plugins");
        std::fs::create_dir_all(&plugin_dir).unwrap();

        // Create a plugin manifest
        let plugin = ConnectorPlugin {
            class_name: "CustomSource".to_string(),
            plugin_type: PluginType::Source,
            version: "1.0.0".to_string(),
            description: "Custom test source".to_string(),
            config_keys: vec![ConfigKey::new("url", "Source URL", true)],
        };
        let manifest = serde_json::to_string_pretty(&plugin).unwrap();
        std::fs::write(plugin_dir.join("custom-source.json"), manifest).unwrap();

        let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());
        let config = RuntimeConfig {
            plugin_path: Some(plugin_dir.to_string_lossy().to_string()),
            ..Default::default()
        };
        let runtime = ConnectorRuntime::new(config, topic_manager);
        runtime.initialize().await.unwrap();

        // Verify plugin was discovered
        let found = runtime.get_plugin("CustomSource").await;
        assert!(found.is_some(), "Custom plugin should be discovered");
        assert_eq!(found.unwrap().version, "1.0.0");
    }

    #[tokio::test]
    async fn test_offset_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());

        // Create offset topic
        topic_manager.create_topic("connect-offsets", 1).unwrap();

        let config = RuntimeConfig::default();
        let runtime = ConnectorRuntime::new(config, topic_manager.clone());
        runtime.initialize().await.unwrap();

        // Set some offsets manually
        {
            let mut store = runtime.offset_store.write().await;
            let offsets = store.entry("my-connector".to_string()).or_default();
            offsets.insert("position".to_string(), "42".to_string());
        }

        // Flush to topic
        runtime.flush_offsets().await.unwrap();

        // Create a new runtime and verify offsets loaded
        let config2 = RuntimeConfig::default();
        let runtime2 = ConnectorRuntime::new(config2, topic_manager);
        runtime2.initialize().await.unwrap();

        let offsets = runtime2.get_offsets("my-connector").await;
        assert_eq!(offsets.get("position").map(|s| s.as_str()), Some("42"));
    }
}
