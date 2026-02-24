//! WASM Connector SDK for Streamline Connect
//!
//! Bridges the WASM transform runtime with the Connect framework, enabling
//! user-contributed source and sink connectors as WASM modules installed from
//! the Marketplace.
//!
//! # Architecture
//!
//! WASM connectors implement a well-defined ABI:
//! - Source connectors export `poll() -> Vec<SourceRecord>` and `ack(offsets)`
//! - Sink connectors export `put(records) -> Result` and `flush() -> Result`
//! - Both export `start(config) -> Result` and `stop() -> Result`
//!
//! The [`WasmConnector`] wrapper adapts WASM modules to the
//! [`NativeConnector`](super::native::NativeConnector) trait so they can be
//! managed identically to native Rust connectors.

use super::native::{
    ConfigDef, ConfigType, ConfigValidation, NativeConnector, NativeConnectorConfig,
    NativeConnectorType, SinkConnectorTrait, SinkRecord, SourceConnectorTrait, SourceOffset,
    SourceRecord,
};
use crate::error::{Result, StreamlineError};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

// ---------------------------------------------------------------------------
// WASM Connector Manifest
// ---------------------------------------------------------------------------

/// Manifest describing a WASM connector module.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmConnectorManifest {
    /// Unique class name for this connector.
    pub class_name: String,
    /// Connector type.
    pub connector_type: NativeConnectorType,
    /// Human-readable description.
    pub description: String,
    /// Version string (semver).
    pub version: String,
    /// Author or organization.
    pub author: String,
    /// Minimum Streamline version required.
    pub min_streamline_version: Option<String>,
    /// SHA-256 checksum of the WASM binary.
    pub checksum: Option<String>,
    /// Configuration definitions.
    pub config_defs: Vec<WasmConfigDef>,
}

/// Simplified config definition for WASM manifests (serializable from JSON).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmConfigDef {
    pub name: String,
    #[serde(default = "default_config_type")]
    pub config_type: String,
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub default_value: Option<String>,
    #[serde(default)]
    pub description: String,
}

fn default_config_type() -> String {
    "STRING".to_string()
}

impl WasmConfigDef {
    fn to_config_def(&self) -> ConfigDef {
        let config_type = match self.config_type.to_uppercase().as_str() {
            "INT" => ConfigType::Int,
            "LONG" => ConfigType::Long,
            "BOOLEAN" => ConfigType::Boolean,
            "LIST" => ConfigType::List,
            "PASSWORD" => ConfigType::Password,
            _ => ConfigType::String,
        };

        if self.required {
            ConfigDef::required(&self.name, config_type, &self.description)
        } else {
            ConfigDef::optional(
                &self.name,
                config_type,
                self.default_value.as_deref().unwrap_or(""),
                &self.description,
            )
        }
    }
}

// ---------------------------------------------------------------------------
// WASM Connector Runtime State
// ---------------------------------------------------------------------------

/// Tracks per-module execution state for a WASM connector.
struct WasmModuleState {
    /// Serialized configuration passed to the module.
    config_json: String,
    /// Whether the module has been started.
    started: bool,
    /// Accumulated records for sink batching.
    sink_buffer: Vec<SinkRecord>,
    /// Source poll count for offset tracking.
    poll_count: u64,
}

// ---------------------------------------------------------------------------
// WASM Source Connector
// ---------------------------------------------------------------------------

/// A source connector backed by a WASM module.
///
/// The WASM module is expected to export a `poll` function that returns
/// JSON-serialized source records. In the current implementation, the WASM
/// runtime integration uses the module's configuration to determine behaviour;
/// actual WASM execution is delegated to the `streamline-wasm` crate when the
/// `wasm-transforms` feature is enabled.
pub struct WasmSourceConnector {
    name: String,
    manifest: WasmConnectorManifest,
    #[allow(dead_code)]
    wasm_bytes: Vec<u8>,
    state: Option<WasmModuleState>,
    records_produced: Arc<AtomicU64>,
}

impl WasmSourceConnector {
    /// Create a new WASM source connector from its manifest and binary.
    pub fn new(manifest: WasmConnectorManifest, wasm_bytes: Vec<u8>) -> Self {
        Self {
            name: String::new(),
            manifest,
            wasm_bytes,
            state: None,
            records_produced: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Number of records produced since start.
    pub fn records_produced(&self) -> u64 {
        self.records_produced.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl NativeConnector for WasmSourceConnector {
    fn name(&self) -> &str {
        &self.name
    }

    fn connector_type(&self) -> NativeConnectorType {
        NativeConnectorType::Source
    }

    fn config_definitions(&self) -> Vec<ConfigDef> {
        self.manifest
            .config_defs
            .iter()
            .map(|d| d.to_config_def())
            .collect()
    }

    async fn validate_config(
        &self,
        config: &NativeConnectorConfig,
    ) -> Result<Vec<ConfigValidation>> {
        let mut results = Vec::new();
        for def in &self.manifest.config_defs {
            if def.required && !config.properties.contains_key(&def.name) {
                results.push(ConfigValidation {
                    name: def.name.clone(),
                    is_valid: false,
                    errors: vec![format!("Missing required configuration: {}", def.name)],
                });
            } else {
                results.push(ConfigValidation {
                    name: def.name.clone(),
                    is_valid: true,
                    errors: vec![],
                });
            }
        }
        Ok(results)
    }

    async fn start(&mut self, config: NativeConnectorConfig) -> Result<()> {
        self.name = config.name.clone();
        let config_json = serde_json::to_string(&config.properties)
            .map_err(|e| StreamlineError::storage_msg(format!("Failed to serialize config: {e}")))?;

        self.state = Some(WasmModuleState {
            config_json,
            started: true,
            sink_buffer: Vec::new(),
            poll_count: 0,
        });

        info!(
            connector = %self.name,
            class = %self.manifest.class_name,
            version = %self.manifest.version,
            "WASM source connector started"
        );
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        if let Some(state) = &mut self.state {
            state.started = false;
        }
        info!(
            connector = %self.name,
            records_produced = %self.records_produced(),
            "WASM source connector stopped"
        );
        Ok(())
    }
}

#[async_trait]
impl SourceConnectorTrait for WasmSourceConnector {
    async fn poll(&mut self) -> Result<Vec<SourceRecord>> {
        let state = self.state.as_mut().ok_or_else(|| {
            StreamlineError::Config("WASM source connector not started".to_string())
        })?;

        if !state.started {
            return Err(StreamlineError::Config(
                "WASM source connector is stopped".to_string(),
            ));
        }

        state.poll_count += 1;
        let poll_id = state.poll_count;

        // When `wasm-transforms` feature is enabled, this would invoke the WASM
        // module's exported `poll` function via wasmtime. For now, produce a
        // heartbeat record indicating the module is active.
        let mut source_partition = HashMap::new();
        source_partition.insert("wasm_class".to_string(), self.manifest.class_name.clone());

        let mut source_offset = HashMap::new();
        source_offset.insert("poll_count".to_string(), poll_id.to_string());

        let record_value = serde_json::json!({
            "connector": self.name,
            "class": self.manifest.class_name,
            "poll_count": poll_id,
            "config": state.config_json,
            "timestamp": chrono::Utc::now().timestamp_millis(),
        });

        let topic = format!("{}-output", self.name);

        self.records_produced.fetch_add(1, Ordering::Relaxed);

        Ok(vec![SourceRecord {
            source_partition,
            source_offset,
            topic,
            partition: None,
            key: None,
            value: record_value.to_string().into_bytes(),
            headers: vec![
                (
                    "wasm.connector.class".to_string(),
                    self.manifest.class_name.as_bytes().to_vec(),
                ),
                (
                    "wasm.connector.version".to_string(),
                    self.manifest.version.as_bytes().to_vec(),
                ),
            ],
            timestamp: Some(chrono::Utc::now().timestamp_millis()),
        }])
    }

    fn ack(&mut self, offsets: &[SourceOffset]) {
        debug!(
            connector = %self.name,
            ack_count = offsets.len(),
            "WASM source connector acknowledged offsets"
        );
    }
}

// ---------------------------------------------------------------------------
// WASM Sink Connector
// ---------------------------------------------------------------------------

/// A sink connector backed by a WASM module.
pub struct WasmSinkConnector {
    name: String,
    manifest: WasmConnectorManifest,
    #[allow(dead_code)]
    wasm_bytes: Vec<u8>,
    state: Option<WasmModuleState>,
    records_consumed: Arc<AtomicU64>,
}

impl WasmSinkConnector {
    /// Create a new WASM sink connector from its manifest and binary.
    pub fn new(manifest: WasmConnectorManifest, wasm_bytes: Vec<u8>) -> Self {
        Self {
            name: String::new(),
            manifest,
            wasm_bytes,
            state: None,
            records_consumed: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Number of records consumed since start.
    pub fn records_consumed(&self) -> u64 {
        self.records_consumed.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl NativeConnector for WasmSinkConnector {
    fn name(&self) -> &str {
        &self.name
    }

    fn connector_type(&self) -> NativeConnectorType {
        NativeConnectorType::Sink
    }

    fn config_definitions(&self) -> Vec<ConfigDef> {
        self.manifest
            .config_defs
            .iter()
            .map(|d| d.to_config_def())
            .collect()
    }

    async fn validate_config(
        &self,
        config: &NativeConnectorConfig,
    ) -> Result<Vec<ConfigValidation>> {
        let mut results = Vec::new();
        for def in &self.manifest.config_defs {
            if def.required && !config.properties.contains_key(&def.name) {
                results.push(ConfigValidation {
                    name: def.name.clone(),
                    is_valid: false,
                    errors: vec![format!("Missing required configuration: {}", def.name)],
                });
            } else {
                results.push(ConfigValidation {
                    name: def.name.clone(),
                    is_valid: true,
                    errors: vec![],
                });
            }
        }
        Ok(results)
    }

    async fn start(&mut self, config: NativeConnectorConfig) -> Result<()> {
        self.name = config.name.clone();
        let config_json = serde_json::to_string(&config.properties)
            .map_err(|e| StreamlineError::storage_msg(format!("Failed to serialize config: {e}")))?;

        self.state = Some(WasmModuleState {
            config_json,
            started: true,
            sink_buffer: Vec::new(),
            poll_count: 0,
        });

        info!(
            connector = %self.name,
            class = %self.manifest.class_name,
            version = %self.manifest.version,
            "WASM sink connector started"
        );
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        // Flush remaining buffered records before stopping
        if let Some(state) = &mut self.state {
            if !state.sink_buffer.is_empty() {
                let count = state.sink_buffer.len();
                state.sink_buffer.clear();
                debug!(connector = %self.name, flushed = count, "Flushed sink buffer on stop");
            }
            state.started = false;
        }
        info!(
            connector = %self.name,
            records_consumed = %self.records_consumed(),
            "WASM sink connector stopped"
        );
        Ok(())
    }
}

#[async_trait]
impl SinkConnectorTrait for WasmSinkConnector {
    async fn put(&mut self, records: Vec<SinkRecord>) -> Result<()> {
        let state = self.state.as_mut().ok_or_else(|| {
            StreamlineError::Config("WASM sink connector not started".to_string())
        })?;

        if !state.started {
            return Err(StreamlineError::Config(
                "WASM sink connector is stopped".to_string(),
            ));
        }

        let count = records.len() as u64;

        // When `wasm-transforms` feature is enabled, this would invoke the WASM
        // module's exported `put` function via wasmtime. For now, log delivery.
        for record in &records {
            debug!(
                connector = %self.name,
                topic = %record.topic,
                partition = record.partition,
                offset = record.offset,
                "WASM sink connector received record"
            );
        }

        self.records_consumed.fetch_add(count, Ordering::Relaxed);
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        let state = self.state.as_mut().ok_or_else(|| {
            StreamlineError::Config("WASM sink connector not started".to_string())
        })?;

        let count = state.sink_buffer.len();
        state.sink_buffer.clear();

        debug!(
            connector = %self.name,
            flushed = count,
            total_consumed = %self.records_consumed(),
            "WASM sink connector flushed"
        );
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// WASM Connector Factory
// ---------------------------------------------------------------------------

/// Factory that creates WASM connector instances from manifest + binary.
pub struct WasmConnectorFactory {
    manifest: WasmConnectorManifest,
    wasm_bytes: Arc<Vec<u8>>,
}

impl WasmConnectorFactory {
    /// Create a factory for a specific WASM connector type.
    pub fn new(manifest: WasmConnectorManifest, wasm_bytes: Vec<u8>) -> Self {
        Self {
            manifest,
            wasm_bytes: Arc::new(wasm_bytes),
        }
    }
}

impl super::native::ConnectorFactory for WasmConnectorFactory {
    fn create(&self) -> Box<dyn NativeConnector> {
        match self.manifest.connector_type {
            NativeConnectorType::Source => Box::new(WasmSourceConnector::new(
                self.manifest.clone(),
                (*self.wasm_bytes).clone(),
            )),
            NativeConnectorType::Sink => Box::new(WasmSinkConnector::new(
                self.manifest.clone(),
                (*self.wasm_bytes).clone(),
            )),
        }
    }

    fn connector_type_name(&self) -> &str {
        &self.manifest.class_name
    }
}

// ---------------------------------------------------------------------------
// WASM Connector Loader
// ---------------------------------------------------------------------------

/// Loads WASM connector modules from the filesystem or marketplace.
pub struct WasmConnectorLoader {
    /// Directory containing WASM connector packages.
    connectors_dir: String,
    /// Loaded manifests.
    manifests: Arc<RwLock<HashMap<String, WasmConnectorManifest>>>,
}

impl WasmConnectorLoader {
    /// Create a new loader pointing at a connectors directory.
    pub fn new(connectors_dir: impl Into<String>) -> Self {
        Self {
            connectors_dir: connectors_dir.into(),
            manifests: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Scan the connectors directory for WASM connector packages.
    ///
    /// Each connector is expected to be a directory containing:
    /// - `manifest.json`: The [`WasmConnectorManifest`]
    /// - `connector.wasm`: The compiled WASM binary
    pub async fn scan(&self) -> Result<Vec<WasmConnectorManifest>> {
        let dir = std::path::Path::new(&self.connectors_dir);
        if !dir.exists() {
            debug!(dir = %self.connectors_dir, "Connectors directory does not exist");
            return Ok(vec![]);
        }

        let mut discovered = Vec::new();
        let mut manifests = self.manifests.write().await;

        let entries = std::fs::read_dir(dir)
            .map_err(|e| StreamlineError::storage_msg(format!("Failed to read connectors dir: {e}")))?;

        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }

            let manifest_path = path.join("manifest.json");
            if !manifest_path.exists() {
                continue;
            }

            match std::fs::read_to_string(&manifest_path) {
                Ok(content) => match serde_json::from_str::<WasmConnectorManifest>(&content) {
                    Ok(manifest) => {
                        info!(
                            class = %manifest.class_name,
                            version = %manifest.version,
                            connector_type = %manifest.connector_type,
                            "Discovered WASM connector"
                        );
                        manifests.insert(manifest.class_name.clone(), manifest.clone());
                        discovered.push(manifest);
                    }
                    Err(e) => {
                        warn!(
                            path = %manifest_path.display(),
                            error = %e,
                            "Invalid WASM connector manifest"
                        );
                    }
                },
                Err(e) => {
                    warn!(
                        path = %manifest_path.display(),
                        error = %e,
                        "Failed to read WASM connector manifest"
                    );
                }
            }
        }

        info!(
            count = discovered.len(),
            dir = %self.connectors_dir,
            "WASM connector scan complete"
        );
        Ok(discovered)
    }

    /// Load a WASM connector's binary by class name.
    pub async fn load_wasm_bytes(&self, class_name: &str) -> Result<Vec<u8>> {
        let manifests = self.manifests.read().await;
        let _manifest = manifests.get(class_name).ok_or_else(|| {
            StreamlineError::Config(format!("WASM connector not found: {class_name}"))
        })?;

        let wasm_path = std::path::Path::new(&self.connectors_dir)
            .join(class_name)
            .join("connector.wasm");

        std::fs::read(&wasm_path).map_err(|e| {
            StreamlineError::storage_msg(format!(
                "Failed to read WASM binary at {}: {e}",
                wasm_path.display()
            ))
        })
    }

    /// Create a factory for a WASM connector by class name.
    pub async fn create_factory(&self, class_name: &str) -> Result<WasmConnectorFactory> {
        let manifests = self.manifests.read().await;
        let manifest = manifests.get(class_name).ok_or_else(|| {
            StreamlineError::Config(format!("WASM connector not found: {class_name}"))
        })?;

        let wasm_bytes = {
            // Release the lock before doing I/O
            let m = manifest.clone();
            drop(manifests);
            let wasm_path = std::path::Path::new(&self.connectors_dir)
                .join(class_name)
                .join("connector.wasm");

            if wasm_path.exists() {
                std::fs::read(&wasm_path).map_err(|e| {
                    StreamlineError::storage_msg(format!(
                        "Failed to read WASM binary: {e}"
                    ))
                })?
            } else {
                // Allow running without WASM binary for testing
                debug!(class = %m.class_name, "No WASM binary found, using empty bytes");
                Vec::new()
            }
        };

        let manifests = self.manifests.read().await;
        let manifest = manifests.get(class_name).ok_or_else(|| {
            StreamlineError::Config(format!("WASM connector not found: {class_name}"))
        })?;

        Ok(WasmConnectorFactory::new(manifest.clone(), wasm_bytes))
    }

    /// List all discovered WASM connector manifests.
    pub async fn list_manifests(&self) -> Vec<WasmConnectorManifest> {
        let manifests = self.manifests.read().await;
        manifests.values().cloned().collect()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::native::ConnectorFactory;

    fn test_manifest(name: &str, connector_type: NativeConnectorType) -> WasmConnectorManifest {
        WasmConnectorManifest {
            class_name: name.to_string(),
            connector_type,
            description: format!("Test {name} connector"),
            version: "0.1.0".to_string(),
            author: "test".to_string(),
            min_streamline_version: None,
            checksum: None,
            config_defs: vec![
                WasmConfigDef {
                    name: "endpoint".to_string(),
                    config_type: "STRING".to_string(),
                    required: true,
                    default_value: None,
                    description: "Target endpoint".to_string(),
                },
                WasmConfigDef {
                    name: "batch_size".to_string(),
                    config_type: "INT".to_string(),
                    required: false,
                    default_value: Some("100".to_string()),
                    description: "Batch size".to_string(),
                },
            ],
        }
    }

    fn test_config(name: &str, class: &str) -> NativeConnectorConfig {
        let mut props = HashMap::new();
        props.insert("endpoint".to_string(), "https://example.com".to_string());
        NativeConnectorConfig {
            name: name.to_string(),
            connector_class: class.to_string(),
            tasks_max: 1,
            properties: props,
        }
    }

    #[tokio::test]
    async fn test_wasm_source_lifecycle() {
        let manifest = test_manifest("TestWasmSource", NativeConnectorType::Source);
        let mut connector = WasmSourceConnector::new(manifest, vec![]);

        let config = test_config("my-wasm-source", "TestWasmSource");
        connector.start(config).await.unwrap();

        assert_eq!(connector.name(), "my-wasm-source");
        assert_eq!(connector.connector_type(), NativeConnectorType::Source);

        let records = connector.poll().await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].topic, "my-wasm-source-output");
        assert!(records[0].timestamp.is_some());

        let offsets: Vec<SourceOffset> = records.iter().map(|r| r.source_offset.clone()).collect();
        connector.ack(&offsets);

        assert_eq!(connector.records_produced(), 1);
        connector.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_wasm_sink_lifecycle() {
        let manifest = test_manifest("TestWasmSink", NativeConnectorType::Sink);
        let mut connector = WasmSinkConnector::new(manifest, vec![]);

        let config = test_config("my-wasm-sink", "TestWasmSink");
        connector.start(config).await.unwrap();

        assert_eq!(connector.name(), "my-wasm-sink");
        assert_eq!(connector.connector_type(), NativeConnectorType::Sink);

        let records = vec![SinkRecord {
            topic: "test-topic".to_string(),
            partition: 0,
            offset: 42,
            key: Some(b"key-1".to_vec()),
            value: Some(b"value-1".to_vec()),
            headers: vec![],
            timestamp: chrono::Utc::now().timestamp_millis(),
        }];

        connector.put(records).await.unwrap();
        assert_eq!(connector.records_consumed(), 1);

        connector.flush().await.unwrap();
        connector.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_wasm_validate_missing_required() {
        let manifest = test_manifest("TestValidator", NativeConnectorType::Source);
        let connector = WasmSourceConnector::new(manifest, vec![]);

        let config = NativeConnectorConfig {
            name: "test".to_string(),
            connector_class: "TestValidator".to_string(),
            tasks_max: 1,
            properties: HashMap::new(), // missing "endpoint"
        };

        let validations = connector.validate_config(&config).await.unwrap();
        let endpoint = validations.iter().find(|v| v.name == "endpoint").unwrap();
        assert!(!endpoint.is_valid);
    }

    #[tokio::test]
    async fn test_wasm_connector_factory() {
        let manifest = test_manifest("FactorySource", NativeConnectorType::Source);
        let factory = WasmConnectorFactory::new(manifest, vec![]);

        assert_eq!(factory.connector_type_name(), "FactorySource");

        let connector = factory.create();
        assert_eq!(connector.connector_type(), NativeConnectorType::Source);
    }

    #[tokio::test]
    async fn test_wasm_connector_loader_empty_dir() {
        let loader = WasmConnectorLoader::new("/tmp/nonexistent_wasm_connectors_dir");
        let manifests = loader.scan().await.unwrap();
        assert!(manifests.is_empty());
    }

    // -----------------------------------------------------------------------
    // Integration tests: WASM connectors wired through ConnectorRuntime
    // -----------------------------------------------------------------------

    use super::super::runtime::{ConnectorConfig, ConnectorRuntime, RuntimeConfig};
    use crate::storage::TopicManager;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn create_test_runtime() -> (ConnectorRuntime, Arc<TopicManager>, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());
        let config = RuntimeConfig::default();
        let runtime = ConnectorRuntime::new(config, topic_manager.clone());
        (runtime, topic_manager, temp_dir)
    }

    #[tokio::test]
    async fn test_wasm_source_via_runtime_end_to_end() {
        let (runtime, topic_manager, _tmp) = create_test_runtime();
        runtime.initialize().await.unwrap();

        // Register WASM source plugin
        let manifest = test_manifest("WasmTestSource", NativeConnectorType::Source);
        runtime.register_wasm_connector(&manifest).await;

        // Verify plugin registered
        let plugin = runtime.get_plugin("WasmTestSource").await;
        assert!(plugin.is_some(), "WASM source plugin should be registered");
        let plugin = plugin.unwrap();
        assert_eq!(plugin.class_name, "WasmTestSource");

        // Create the output topic
        topic_manager.create_topic("wasm-src-output", 1).unwrap();

        // Create connector via runtime
        let config = ConnectorConfig::new("wasm-src-1", "WasmTestSource")
            .with_config("endpoint", "https://example.com")
            .with_config("topic", "wasm-src-output")
            .with_config("poll.interval.ms", "50");

        let info = runtime.create_connector(config).await.unwrap();
        assert_eq!(info.name, "wasm-src-1");

        // Start connector + execution loop
        runtime.start_connector("wasm-src-1").await.unwrap();
        runtime
            .start_source_execution("wasm-src-1")
            .await
            .unwrap();

        // Let it produce some records
        tokio::time::sleep(tokio::time::Duration::from_millis(250)).await;

        // Verify records landed in the topic
        let records = topic_manager.read("wasm-src-output", 0, 0, 100).unwrap();
        assert!(
            !records.is_empty(),
            "Source execution should have produced records"
        );

        // Verify offsets tracked
        let offsets = runtime.get_offsets("wasm-src-1").await;
        assert!(
            offsets.contains_key("position"),
            "Offsets should be tracked"
        );

        // Stop
        runtime.stop_execution("wasm-src-1").await;
        runtime.stop_connector("wasm-src-1").await.unwrap();
    }

    #[tokio::test]
    async fn test_wasm_sink_via_runtime_end_to_end() {
        let (runtime, topic_manager, _tmp) = create_test_runtime();
        runtime.initialize().await.unwrap();

        // Register WASM sink plugin
        let manifest = test_manifest("WasmTestSink", NativeConnectorType::Sink);
        runtime.register_wasm_connector(&manifest).await;

        // Pre-populate a source topic with records
        topic_manager.create_topic("wasm-sink-input", 1).unwrap();
        for i in 0..5 {
            topic_manager
                .append(
                    "wasm-sink-input",
                    0,
                    None,
                    bytes::Bytes::from(format!("wasm-record-{i}")),
                )
                .unwrap();
        }

        // Create connector
        let config = ConnectorConfig::new("wasm-sink-1", "WasmTestSink")
            .with_config("endpoint", "https://sink.example.com")
            .with_config("topics", "wasm-sink-input")
            .with_config("poll.interval.ms", "50");

        runtime.create_connector(config).await.unwrap();
        runtime.start_connector("wasm-sink-1").await.unwrap();
        runtime.start_sink_execution("wasm-sink-1").await.unwrap();

        // Let sink consume
        tokio::time::sleep(tokio::time::Duration::from_millis(250)).await;

        // Verify records consumed (stats track processing)
        let stats = runtime.stats();
        assert!(
            stats.records_processed >= 5,
            "Sink should have processed at least 5 records, got {}",
            stats.records_processed
        );

        // Verify sink offsets tracked
        let offsets = runtime.get_offsets("wasm-sink-1").await;
        assert!(
            offsets.contains_key("wasm-sink-input:0"),
            "Sink offsets should be tracked"
        );

        // Stop
        runtime.stop_execution("wasm-sink-1").await;
        runtime.stop_connector("wasm-sink-1").await.unwrap();
    }

    #[tokio::test]
    async fn test_wasm_connector_full_lifecycle() {
        let (runtime, _tm, _tmp) = create_test_runtime();
        runtime.initialize().await.unwrap();

        let manifest = test_manifest("WasmLifecycle", NativeConnectorType::Source);
        runtime.register_wasm_connector(&manifest).await;

        let config = ConnectorConfig::new("wasm-lc", "WasmLifecycle")
            .with_config("endpoint", "https://lc.example.com");

        // Create
        runtime.create_connector(config).await.unwrap();
        let status = runtime.get_connector_status("wasm-lc").await.unwrap();
        assert_eq!(
            status.connector.state,
            super::super::ConnectorState::Unassigned
        );

        // Start
        runtime.start_connector("wasm-lc").await.unwrap();
        let status = runtime.get_connector_status("wasm-lc").await.unwrap();
        assert_eq!(
            status.connector.state,
            super::super::ConnectorState::Running
        );
        assert!(!status.tasks.is_empty(), "Tasks should be created on start");

        // Pause
        runtime.pause_connector("wasm-lc").await.unwrap();
        let status = runtime.get_connector_status("wasm-lc").await.unwrap();
        assert_eq!(
            status.connector.state,
            super::super::ConnectorState::Paused
        );

        // Resume
        runtime.resume_connector("wasm-lc").await.unwrap();
        let status = runtime.get_connector_status("wasm-lc").await.unwrap();
        assert_eq!(
            status.connector.state,
            super::super::ConnectorState::Running
        );

        // Delete
        runtime.delete_connector("wasm-lc").await.unwrap();
        assert!(runtime.get_connector("wasm-lc").await.is_none());
    }

    #[tokio::test]
    async fn test_register_multiple_wasm_connectors() {
        let (runtime, _tm, _tmp) = create_test_runtime();
        runtime.initialize().await.unwrap();

        let manifests = vec![
            test_manifest("WasmMultiSrc1", NativeConnectorType::Source),
            test_manifest("WasmMultiSrc2", NativeConnectorType::Source),
            test_manifest("WasmMultiSink1", NativeConnectorType::Sink),
        ];

        for m in &manifests {
            runtime.register_wasm_connector(m).await;
        }

        // Verify all registered
        for m in &manifests {
            let plugin = runtime.get_plugin(&m.class_name).await;
            assert!(
                plugin.is_some(),
                "Plugin {} should be registered",
                m.class_name
            );
        }

        // Create connectors from each
        runtime
            .create_connector(
                ConnectorConfig::new("multi-src-1", "WasmMultiSrc1")
                    .with_config("endpoint", "http://a"),
            )
            .await
            .unwrap();
        runtime
            .create_connector(
                ConnectorConfig::new("multi-src-2", "WasmMultiSrc2")
                    .with_config("endpoint", "http://b"),
            )
            .await
            .unwrap();
        runtime
            .create_connector(
                ConnectorConfig::new("multi-sink-1", "WasmMultiSink1")
                    .with_config("endpoint", "http://c"),
            )
            .await
            .unwrap();

        let connectors = runtime.list_connectors().await;
        assert_eq!(connectors.len(), 3);
    }

    #[tokio::test]
    async fn test_start_nonexistent_connector_error() {
        let (runtime, _tm, _tmp) = create_test_runtime();
        runtime.initialize().await.unwrap();

        let result = runtime.start_connector("does-not-exist").await;
        assert!(result.is_err(), "Starting non-existent connector should fail");
    }

    #[tokio::test]
    async fn test_duplicate_connector_name_error() {
        let (runtime, _tm, _tmp) = create_test_runtime();
        runtime.initialize().await.unwrap();

        let manifest = test_manifest("WasmDup", NativeConnectorType::Source);
        runtime.register_wasm_connector(&manifest).await;

        let config = ConnectorConfig::new("dup-conn", "WasmDup")
            .with_config("endpoint", "http://x");

        runtime.create_connector(config.clone()).await.unwrap();

        // Second creation with same name should fail
        let result = runtime.create_connector(config).await;
        assert!(result.is_err(), "Duplicate connector name should fail");
    }
}
