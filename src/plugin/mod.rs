//! Trait-based plugin system for Streamline
//!
//! This module provides a compile-time extensibility framework that allows
//! users to extend Streamline's functionality through Rust traits.
//!
//! ## Plugin Types
//!
//! - [`MessageInterceptor`]: Intercept and transform messages during produce/fetch
//! - [`AuthenticationPlugin`]: Custom authentication mechanisms
//! - [`MetricsPlugin`]: Custom metrics collection and export
//! - [`StoragePlugin`]: Alternative storage backends
//!
//! ## Example
//!
//! ```ignore
//! use streamline::plugin::{MessageInterceptor, InterceptorContext, InterceptorResult};
//!
//! struct LoggingInterceptor;
//!
//! impl MessageInterceptor for LoggingInterceptor {
//!     fn name(&self) -> &str {
//!         "logging-interceptor"
//!     }
//!
//!     fn on_produce(&self, ctx: &mut InterceptorContext) -> InterceptorResult {
//!         tracing::info!(topic = %ctx.topic, "Message produced");
//!         InterceptorResult::Continue
//!     }
//! }
//! ```

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

/// Result type for plugin operations
pub type PluginResult<T> = Result<T, PluginError>;

/// Errors that can occur during plugin operations
#[derive(Debug, Clone, thiserror::Error)]
pub enum PluginError {
    /// Plugin initialization failed
    #[error("Plugin initialization failed: {0}")]
    InitializationFailed(String),

    /// Plugin with this name already registered
    #[error("Plugin already registered: {0}")]
    AlreadyRegistered(String),

    /// Plugin not found
    #[error("Plugin not found: {0}")]
    NotFound(String),

    /// Plugin operation failed
    #[error("Plugin operation failed: {0}")]
    OperationFailed(String),

    /// Plugin configuration error
    #[error("Plugin configuration error: {0}")]
    ConfigurationError(String),
}

/// Plugin lifecycle trait - all plugins must implement this
pub trait Plugin: Send + Sync + Any {
    /// Unique name for this plugin
    fn name(&self) -> &str;

    /// Human-readable description
    fn description(&self) -> &str {
        ""
    }

    /// Plugin version
    fn version(&self) -> &str {
        "0.0.0"
    }

    /// Called when the plugin is loaded
    fn on_load(&mut self) -> PluginResult<()> {
        Ok(())
    }

    /// Called when the plugin is unloaded
    fn on_unload(&mut self) -> PluginResult<()> {
        Ok(())
    }

    /// Called when the server starts
    fn on_server_start(&self) -> PluginResult<()> {
        Ok(())
    }

    /// Called when the server shuts down
    fn on_server_shutdown(&self) -> PluginResult<()> {
        Ok(())
    }
}

/// Context passed to message interceptors
#[derive(Debug, Clone)]
pub struct InterceptorContext {
    /// Topic name
    pub topic: String,

    /// Partition (if applicable)
    pub partition: Option<i32>,

    /// Message key (if present)
    pub key: Option<Vec<u8>>,

    /// Message value
    pub value: Vec<u8>,

    /// Message headers
    pub headers: HashMap<String, Vec<u8>>,

    /// Timestamp (milliseconds since epoch)
    pub timestamp: Option<i64>,

    /// Client ID (if authenticated)
    pub client_id: Option<String>,

    /// Additional metadata that can be modified by interceptors
    pub metadata: HashMap<String, String>,
}

impl InterceptorContext {
    /// Create a new interceptor context
    pub fn new(topic: impl Into<String>, value: Vec<u8>) -> Self {
        Self {
            topic: topic.into(),
            partition: None,
            key: None,
            value,
            headers: HashMap::new(),
            timestamp: None,
            client_id: None,
            metadata: HashMap::new(),
        }
    }

    /// Builder method to set the partition
    pub fn with_partition(mut self, partition: i32) -> Self {
        self.partition = Some(partition);
        self
    }

    /// Builder method to set the key
    pub fn with_key(mut self, key: Vec<u8>) -> Self {
        self.key = Some(key);
        self
    }

    /// Builder method to set headers
    pub fn with_headers(mut self, headers: HashMap<String, Vec<u8>>) -> Self {
        self.headers = headers;
        self
    }

    /// Builder method to set timestamp
    pub fn with_timestamp(mut self, timestamp: i64) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    /// Builder method to set client ID
    pub fn with_client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client_id = Some(client_id.into());
        self
    }
}

/// Result of interceptor execution
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InterceptorResult {
    /// Continue processing with (possibly modified) context
    Continue,

    /// Drop this message, don't process further
    Drop,

    /// Reject with an error message (returns error to client)
    Reject(String),

    /// Route to a different topic
    RouteToTopic(String),
}

/// Message interceptor trait for produce/fetch hooks
pub trait MessageInterceptor: Plugin {
    /// Called before a message is produced
    fn on_produce(&self, _ctx: &mut InterceptorContext) -> InterceptorResult {
        InterceptorResult::Continue
    }

    /// Called after a message is produced successfully
    fn on_produce_complete(&self, _ctx: &InterceptorContext) {}

    /// Called before a message is fetched/consumed
    fn on_fetch(&self, _ctx: &mut InterceptorContext) -> InterceptorResult {
        InterceptorResult::Continue
    }

    /// Called when a produce fails
    fn on_produce_error(&self, _ctx: &InterceptorContext, _error: &str) {}
}

/// Authentication context for auth plugins
#[derive(Debug, Clone)]
pub struct AuthContext {
    /// Authentication mechanism (e.g., "PLAIN", "SCRAM-SHA-256")
    pub mechanism: String,

    /// Username (if available)
    pub username: Option<String>,

    /// Additional authentication data
    pub auth_data: HashMap<String, String>,

    /// Client IP address
    pub client_ip: Option<String>,
}

/// Authentication result
#[derive(Debug, Clone)]
pub enum AuthResult {
    /// Authentication successful
    Success {
        /// Authenticated principal
        principal: String,
        /// Additional claims/attributes
        attributes: HashMap<String, String>,
    },
    /// Authentication failed
    Failure {
        /// Reason for failure
        reason: String,
    },
    /// Let other plugins/mechanisms try
    NotApplicable,
}

/// Custom authentication plugin trait
pub trait AuthenticationPlugin: Plugin {
    /// List of mechanisms this plugin handles
    fn supported_mechanisms(&self) -> Vec<String>;

    /// Authenticate a client
    fn authenticate(&self, ctx: &AuthContext) -> AuthResult;
}

/// Metrics context
#[derive(Debug, Clone)]
pub struct MetricsContext {
    /// Operation name
    pub operation: String,

    /// Duration in microseconds
    pub duration_us: u64,

    /// Bytes processed (if applicable)
    pub bytes: Option<u64>,

    /// Whether operation succeeded
    pub success: bool,

    /// Additional labels
    pub labels: HashMap<String, String>,
}

/// Custom metrics plugin trait
pub trait MetricsPlugin: Plugin {
    /// Record a counter metric
    fn record_counter(&self, name: &str, value: u64, labels: &HashMap<String, String>);

    /// Record a gauge metric
    fn record_gauge(&self, name: &str, value: f64, labels: &HashMap<String, String>);

    /// Record a histogram metric
    fn record_histogram(&self, name: &str, value: f64, labels: &HashMap<String, String>);

    /// Record operation metrics
    fn record_operation(&self, ctx: &MetricsContext) {
        let mut labels = ctx.labels.clone();
        labels.insert("operation".to_string(), ctx.operation.clone());
        labels.insert("success".to_string(), ctx.success.to_string());

        self.record_counter(&format!("{}_total", ctx.operation), 1, &labels);

        self.record_histogram(
            &format!("{}_duration_us", ctx.operation),
            ctx.duration_us as f64,
            &labels,
        );

        if let Some(bytes) = ctx.bytes {
            self.record_counter(&format!("{}_bytes", ctx.operation), bytes, &labels);
        }
    }
}

/// Plugin registry for managing loaded plugins
pub struct PluginRegistry {
    /// All registered plugins
    plugins: RwLock<HashMap<String, Arc<dyn Plugin>>>,

    /// Message interceptors
    interceptors: RwLock<Vec<Arc<dyn MessageInterceptor>>>,

    /// Authentication plugins
    auth_plugins: RwLock<Vec<Arc<dyn AuthenticationPlugin>>>,

    /// Metrics plugins
    metrics_plugins: RwLock<Vec<Arc<dyn MetricsPlugin>>>,
}

impl Default for PluginRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl PluginRegistry {
    /// Create a new plugin registry
    pub fn new() -> Self {
        Self {
            plugins: RwLock::new(HashMap::new()),
            interceptors: RwLock::new(Vec::new()),
            auth_plugins: RwLock::new(Vec::new()),
            metrics_plugins: RwLock::new(Vec::new()),
        }
    }

    /// Register a message interceptor
    pub fn register_interceptor<T: MessageInterceptor + 'static>(
        &self,
        interceptor: T,
    ) -> PluginResult<()> {
        let name = interceptor.name().to_string();
        let arc = Arc::new(interceptor);

        let mut plugins = self.plugins.write();
        if plugins.contains_key(&name) {
            return Err(PluginError::AlreadyRegistered(name));
        }

        plugins.insert(name, arc.clone());
        self.interceptors.write().push(arc);

        Ok(())
    }

    /// Register an authentication plugin
    pub fn register_auth_plugin<T: AuthenticationPlugin + 'static>(
        &self,
        plugin: T,
    ) -> PluginResult<()> {
        let name = plugin.name().to_string();
        let arc = Arc::new(plugin);

        let mut plugins = self.plugins.write();
        if plugins.contains_key(&name) {
            return Err(PluginError::AlreadyRegistered(name));
        }

        plugins.insert(name, arc.clone());
        self.auth_plugins.write().push(arc);

        Ok(())
    }

    /// Register a metrics plugin
    pub fn register_metrics_plugin<T: MetricsPlugin + 'static>(
        &self,
        plugin: T,
    ) -> PluginResult<()> {
        let name = plugin.name().to_string();
        let arc = Arc::new(plugin);

        let mut plugins = self.plugins.write();
        if plugins.contains_key(&name) {
            return Err(PluginError::AlreadyRegistered(name));
        }

        plugins.insert(name, arc.clone());
        self.metrics_plugins.write().push(arc);

        Ok(())
    }

    /// Get all registered plugin names
    pub fn plugin_names(&self) -> Vec<String> {
        self.plugins.read().keys().cloned().collect()
    }

    /// Get the number of registered plugins
    pub fn plugin_count(&self) -> usize {
        self.plugins.read().len()
    }

    /// Get the number of message interceptors
    pub fn interceptor_count(&self) -> usize {
        self.interceptors.read().len()
    }

    /// Run message through all produce interceptors
    pub fn intercept_produce(&self, ctx: &mut InterceptorContext) -> InterceptorResult {
        for interceptor in self.interceptors.read().iter() {
            match interceptor.on_produce(ctx) {
                InterceptorResult::Continue => continue,
                result => return result,
            }
        }
        InterceptorResult::Continue
    }

    /// Run message through all fetch interceptors
    pub fn intercept_fetch(&self, ctx: &mut InterceptorContext) -> InterceptorResult {
        for interceptor in self.interceptors.read().iter() {
            match interceptor.on_fetch(ctx) {
                InterceptorResult::Continue => continue,
                result => return result,
            }
        }
        InterceptorResult::Continue
    }

    /// Notify interceptors of produce completion
    pub fn notify_produce_complete(&self, ctx: &InterceptorContext) {
        for interceptor in self.interceptors.read().iter() {
            interceptor.on_produce_complete(ctx);
        }
    }

    /// Notify interceptors of produce error
    pub fn notify_produce_error(&self, ctx: &InterceptorContext, error: &str) {
        for interceptor in self.interceptors.read().iter() {
            interceptor.on_produce_error(ctx, error);
        }
    }

    /// Authenticate using registered auth plugins
    pub fn authenticate(&self, ctx: &AuthContext) -> AuthResult {
        for plugin in self.auth_plugins.read().iter() {
            if plugin.supported_mechanisms().contains(&ctx.mechanism) {
                match plugin.authenticate(ctx) {
                    AuthResult::NotApplicable => continue,
                    result => return result,
                }
            }
        }
        AuthResult::NotApplicable
    }

    /// Record metrics via all metrics plugins
    pub fn record_operation(&self, ctx: &MetricsContext) {
        for plugin in self.metrics_plugins.read().iter() {
            plugin.record_operation(ctx);
        }
    }

    /// Trigger server start on all plugins
    pub fn on_server_start(&self) -> PluginResult<()> {
        for plugin in self.plugins.read().values() {
            plugin.on_server_start()?;
        }
        Ok(())
    }

    /// Trigger server shutdown on all plugins
    pub fn on_server_shutdown(&self) -> PluginResult<()> {
        for plugin in self.plugins.read().values() {
            plugin.on_server_shutdown()?;
        }
        Ok(())
    }
}

// ============================================================================
// Plugin Manager - Discovery, Installation, and Lifecycle Management
// ============================================================================

/// Plugin manifest describing an installable plugin
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PluginManifest {
    /// Plugin name (unique identifier)
    pub name: String,
    /// Plugin version (semver)
    pub version: String,
    /// Human-readable description
    pub description: String,
    /// Plugin author
    pub author: String,
    /// Plugin type (sink, source, transform, interceptor)
    pub plugin_type: PluginType,
    /// Minimum Streamline version required
    pub min_streamline_version: Option<String>,
    /// Dependencies on other plugins
    #[serde(default)]
    pub dependencies: Vec<String>,
    /// Configuration schema (JSON Schema)
    #[serde(default)]
    pub config_schema: Option<serde_json::Value>,
}

/// Plugin type classification
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PluginType {
    /// Sink connector (writes data to external system)
    Sink,
    /// Source connector (reads data from external system)
    Source,
    /// Message transformation
    Transform,
    /// Message interceptor
    Interceptor,
    /// Authentication provider
    Auth,
    /// Metrics exporter
    Metrics,
}

impl std::fmt::Display for PluginType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PluginType::Sink => write!(f, "sink"),
            PluginType::Source => write!(f, "source"),
            PluginType::Transform => write!(f, "transform"),
            PluginType::Interceptor => write!(f, "interceptor"),
            PluginType::Auth => write!(f, "auth"),
            PluginType::Metrics => write!(f, "metrics"),
        }
    }
}

/// Installed plugin state
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct InstalledPlugin {
    /// Plugin manifest
    pub manifest: PluginManifest,
    /// Installation path
    pub install_path: std::path::PathBuf,
    /// Whether the plugin is currently enabled
    pub enabled: bool,
    /// Installation timestamp
    pub installed_at: u64,
    /// Plugin-specific configuration
    #[serde(default)]
    pub config: HashMap<String, serde_json::Value>,
}

/// Plugin manager for discovering, installing, and managing plugins
pub struct PluginManager {
    /// Directory where plugins are stored
    plugins_dir: std::path::PathBuf,
    /// Installed plugins
    installed: RwLock<HashMap<String, InstalledPlugin>>,
    /// Plugin registry for active plugins
    registry: Arc<PluginRegistry>,
}

impl PluginManager {
    /// Create a new plugin manager
    pub fn new(plugins_dir: std::path::PathBuf, registry: Arc<PluginRegistry>) -> Self {
        Self {
            plugins_dir,
            installed: RwLock::new(HashMap::new()),
            registry,
        }
    }

    /// Get the plugins directory path
    pub fn plugins_dir(&self) -> &std::path::Path {
        &self.plugins_dir
    }

    /// List all installed plugins
    pub fn list_installed(&self) -> Vec<InstalledPlugin> {
        self.installed.read().values().cloned().collect()
    }

    /// Get an installed plugin by name
    pub fn get_installed(&self, name: &str) -> Option<InstalledPlugin> {
        self.installed.read().get(name).cloned()
    }

    /// Install a plugin from a manifest
    pub fn install(&self, manifest: PluginManifest) -> PluginResult<InstalledPlugin> {
        let name = manifest.name.clone();

        // Check if already installed
        if self.installed.read().contains_key(&name) {
            return Err(PluginError::AlreadyRegistered(name));
        }

        // Create plugin directory
        let plugin_dir = self.plugins_dir.join(&name);
        std::fs::create_dir_all(&plugin_dir).map_err(|e| {
            PluginError::OperationFailed(format!("Failed to create plugin directory: {}", e))
        })?;

        // Save manifest
        let manifest_path = plugin_dir.join("plugin.json");
        let manifest_json = serde_json::to_string_pretty(&manifest).map_err(|e| {
            PluginError::OperationFailed(format!("Failed to serialize manifest: {}", e))
        })?;
        std::fs::write(&manifest_path, manifest_json).map_err(|e| {
            PluginError::OperationFailed(format!("Failed to write manifest: {}", e))
        })?;

        let installed = InstalledPlugin {
            manifest,
            install_path: plugin_dir,
            enabled: true,
            installed_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
            config: HashMap::new(),
        };

        self.installed.write().insert(name, installed.clone());

        Ok(installed)
    }

    /// Uninstall a plugin by name
    pub fn uninstall(&self, name: &str) -> PluginResult<()> {
        let plugin = self
            .installed
            .write()
            .remove(name)
            .ok_or_else(|| PluginError::NotFound(name.to_string()))?;

        // Remove plugin directory
        if plugin.install_path.exists() {
            std::fs::remove_dir_all(&plugin.install_path).map_err(|e| {
                PluginError::OperationFailed(format!("Failed to remove plugin directory: {}", e))
            })?;
        }

        Ok(())
    }

    /// Enable a plugin
    pub fn enable(&self, name: &str) -> PluginResult<()> {
        let mut installed = self.installed.write();
        let plugin = installed
            .get_mut(name)
            .ok_or_else(|| PluginError::NotFound(name.to_string()))?;
        plugin.enabled = true;
        Ok(())
    }

    /// Disable a plugin
    pub fn disable(&self, name: &str) -> PluginResult<()> {
        let mut installed = self.installed.write();
        let plugin = installed
            .get_mut(name)
            .ok_or_else(|| PluginError::NotFound(name.to_string()))?;
        plugin.enabled = false;
        Ok(())
    }

    /// Discover plugins from the plugins directory
    pub fn discover(&self) -> PluginResult<Vec<PluginManifest>> {
        let mut manifests = Vec::new();

        if !self.plugins_dir.exists() {
            return Ok(manifests);
        }

        let entries = std::fs::read_dir(&self.plugins_dir).map_err(|e| {
            PluginError::OperationFailed(format!("Failed to read plugins directory: {}", e))
        })?;

        for entry in entries.flatten() {
            let manifest_path = entry.path().join("plugin.json");
            if manifest_path.exists() {
                let content = std::fs::read_to_string(&manifest_path).map_err(|e| {
                    PluginError::OperationFailed(format!("Failed to read manifest: {}", e))
                })?;
                if let Ok(manifest) = serde_json::from_str::<PluginManifest>(&content) {
                    manifests.push(manifest);
                }
            }
        }

        Ok(manifests)
    }

    /// Update plugin configuration
    pub fn configure(
        &self,
        name: &str,
        config: HashMap<String, serde_json::Value>,
    ) -> PluginResult<()> {
        let mut installed = self.installed.write();
        let plugin = installed
            .get_mut(name)
            .ok_or_else(|| PluginError::NotFound(name.to_string()))?;
        plugin.config = config;
        Ok(())
    }

    /// Get the underlying plugin registry
    pub fn registry(&self) -> &Arc<PluginRegistry> {
        &self.registry
    }

    /// Load plugins from disk on startup
    pub fn load_from_disk(&self) -> PluginResult<usize> {
        let manifests = self.discover()?;
        let count = manifests.len();
        for manifest in manifests {
            let name = manifest.name.clone();
            if !self.installed.read().contains_key(&name) {
                let plugin_dir = self.plugins_dir.join(&name);
                let installed = InstalledPlugin {
                    manifest,
                    install_path: plugin_dir,
                    enabled: true,
                    installed_at: 0,
                    config: HashMap::new(),
                };
                self.installed.write().insert(name, installed);
            }
        }
        Ok(count)
    }

    /// Validate a plugin manifest before installation
    pub fn validate_manifest(manifest: &PluginManifest) -> PluginResult<()> {
        if manifest.name.is_empty() {
            return Err(PluginError::ConfigurationError(
                "Plugin name cannot be empty".to_string(),
            ));
        }
        if manifest.version.is_empty() {
            return Err(PluginError::ConfigurationError(
                "Plugin version cannot be empty".to_string(),
            ));
        }
        // Validate name format (alphanumeric + hyphens)
        if !manifest
            .name
            .chars()
            .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
        {
            return Err(PluginError::ConfigurationError(
                "Plugin name must contain only alphanumeric characters, hyphens, or underscores"
                    .to_string(),
            ));
        }
        Ok(())
    }

    /// Check if a plugin is compatible with the current Streamline version
    pub fn check_compatibility(&self, manifest: &PluginManifest) -> PluginResult<bool> {
        if let Some(ref _min_version) = manifest.min_streamline_version {
            // For now, all plugins are compatible (version checking to be implemented)
            Ok(true)
        } else {
            Ok(true)
        }
    }

    /// Export plugin configuration as JSON for backup
    pub fn export_config(&self) -> PluginResult<serde_json::Value> {
        let installed = self.installed.read();
        let entries: Vec<serde_json::Value> = installed
            .values()
            .map(|p| serde_json::to_value(p).unwrap_or_default())
            .collect();
        Ok(serde_json::Value::Array(entries))
    }

    /// Get plugin manager statistics
    pub fn stats(&self) -> PluginManagerStats {
        let installed = self.installed.read();
        let enabled = installed.values().filter(|p| p.enabled).count();
        let by_type = {
            let mut counts: HashMap<String, usize> = HashMap::new();
            for plugin in installed.values() {
                *counts
                    .entry(plugin.manifest.plugin_type.to_string())
                    .or_insert(0) += 1;
            }
            counts
        };

        PluginManagerStats {
            total_installed: installed.len(),
            enabled,
            disabled: installed.len() - enabled,
            by_type,
            active_interceptors: self.registry.interceptor_count(),
            total_registered: self.registry.plugin_count(),
        }
    }
}

/// Plugin manager statistics
#[derive(Debug, Clone, serde::Serialize)]
pub struct PluginManagerStats {
    /// Total installed plugins
    pub total_installed: usize,
    /// Number of enabled plugins
    pub enabled: usize,
    /// Number of disabled plugins
    pub disabled: usize,
    /// Plugins by type
    pub by_type: HashMap<String, usize>,
    /// Active message interceptors
    pub active_interceptors: usize,
    /// Total registered plugins in runtime
    pub total_registered: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestInterceptor {
        name: String,
        drop_messages: bool,
    }

    impl TestInterceptor {
        fn new(name: &str) -> Self {
            Self {
                name: name.to_string(),
                drop_messages: false,
            }
        }

        fn with_drop(mut self) -> Self {
            self.drop_messages = true;
            self
        }
    }

    impl Plugin for TestInterceptor {
        fn name(&self) -> &str {
            &self.name
        }

        fn description(&self) -> &str {
            "Test interceptor for unit tests"
        }
    }

    impl MessageInterceptor for TestInterceptor {
        fn on_produce(&self, ctx: &mut InterceptorContext) -> InterceptorResult {
            ctx.metadata
                .insert("intercepted".to_string(), "true".to_string());
            if self.drop_messages {
                InterceptorResult::Drop
            } else {
                InterceptorResult::Continue
            }
        }
    }

    #[test]
    fn test_plugin_registry_creation() {
        let registry = PluginRegistry::new();
        assert_eq!(registry.plugin_count(), 0);
        assert_eq!(registry.interceptor_count(), 0);
    }

    #[test]
    fn test_register_interceptor() {
        let registry = PluginRegistry::new();
        let interceptor = TestInterceptor::new("test-interceptor");

        registry.register_interceptor(interceptor).unwrap();

        assert_eq!(registry.plugin_count(), 1);
        assert_eq!(registry.interceptor_count(), 1);
        assert!(registry
            .plugin_names()
            .contains(&"test-interceptor".to_string()));
    }

    #[test]
    fn test_duplicate_registration_fails() {
        let registry = PluginRegistry::new();
        let interceptor1 = TestInterceptor::new("duplicate");
        let interceptor2 = TestInterceptor::new("duplicate");

        registry.register_interceptor(interceptor1).unwrap();
        let result = registry.register_interceptor(interceptor2);

        assert!(matches!(result, Err(PluginError::AlreadyRegistered(_))));
    }

    #[test]
    fn test_intercept_produce_continue() {
        let registry = PluginRegistry::new();
        registry
            .register_interceptor(TestInterceptor::new("test"))
            .unwrap();

        let mut ctx = InterceptorContext::new("test-topic", vec![1, 2, 3]);
        let result = registry.intercept_produce(&mut ctx);

        assert_eq!(result, InterceptorResult::Continue);
        assert_eq!(ctx.metadata.get("intercepted"), Some(&"true".to_string()));
    }

    #[test]
    fn test_intercept_produce_drop() {
        let registry = PluginRegistry::new();
        registry
            .register_interceptor(TestInterceptor::new("dropper").with_drop())
            .unwrap();

        let mut ctx = InterceptorContext::new("test-topic", vec![1, 2, 3]);
        let result = registry.intercept_produce(&mut ctx);

        assert_eq!(result, InterceptorResult::Drop);
    }

    #[test]
    fn test_interceptor_context_builder() {
        let ctx = InterceptorContext::new("topic", vec![1, 2, 3])
            .with_partition(0)
            .with_key(vec![4, 5, 6])
            .with_timestamp(12345)
            .with_client_id("client-1");

        assert_eq!(ctx.topic, "topic");
        assert_eq!(ctx.partition, Some(0));
        assert_eq!(ctx.key, Some(vec![4, 5, 6]));
        assert_eq!(ctx.timestamp, Some(12345));
        assert_eq!(ctx.client_id, Some("client-1".to_string()));
    }

    #[test]
    fn test_interceptor_result_variants() {
        assert_eq!(InterceptorResult::Continue, InterceptorResult::Continue);
        assert_eq!(InterceptorResult::Drop, InterceptorResult::Drop);

        let reject = InterceptorResult::Reject("error".to_string());
        assert!(matches!(reject, InterceptorResult::Reject(_)));

        let route = InterceptorResult::RouteToTopic("other".to_string());
        assert!(matches!(route, InterceptorResult::RouteToTopic(_)));
    }

    #[test]
    fn test_server_lifecycle_hooks() {
        let registry = PluginRegistry::new();
        registry
            .register_interceptor(TestInterceptor::new("test"))
            .unwrap();

        assert!(registry.on_server_start().is_ok());
        assert!(registry.on_server_shutdown().is_ok());
    }

    fn create_test_manifest(name: &str, plugin_type: PluginType) -> PluginManifest {
        PluginManifest {
            name: name.to_string(),
            version: "1.0.0".to_string(),
            description: format!("Test {} plugin", name),
            author: "test".to_string(),
            plugin_type,
            min_streamline_version: None,
            dependencies: vec![],
            config_schema: None,
        }
    }

    #[test]
    fn test_plugin_manager_install_and_list() {
        let dir = tempfile::tempdir().unwrap();
        let registry = Arc::new(PluginRegistry::new());
        let manager = PluginManager::new(dir.path().to_path_buf(), registry);

        assert!(manager.list_installed().is_empty());

        let manifest = create_test_manifest("my-sink", PluginType::Sink);
        let installed = manager.install(manifest).unwrap();
        assert_eq!(installed.manifest.name, "my-sink");
        assert!(installed.enabled);

        let all = manager.list_installed();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].manifest.name, "my-sink");
    }

    #[test]
    fn test_plugin_manager_install_duplicate_fails() {
        let dir = tempfile::tempdir().unwrap();
        let registry = Arc::new(PluginRegistry::new());
        let manager = PluginManager::new(dir.path().to_path_buf(), registry);

        let manifest = create_test_manifest("dup", PluginType::Transform);
        manager.install(manifest).unwrap();

        let manifest2 = create_test_manifest("dup", PluginType::Transform);
        let result = manager.install(manifest2);
        assert!(matches!(result, Err(PluginError::AlreadyRegistered(_))));
    }

    #[test]
    fn test_plugin_manager_uninstall() {
        let dir = tempfile::tempdir().unwrap();
        let registry = Arc::new(PluginRegistry::new());
        let manager = PluginManager::new(dir.path().to_path_buf(), registry);

        let manifest = create_test_manifest("removable", PluginType::Source);
        manager.install(manifest).unwrap();
        assert_eq!(manager.list_installed().len(), 1);

        manager.uninstall("removable").unwrap();
        assert!(manager.list_installed().is_empty());
    }

    #[test]
    fn test_plugin_manager_uninstall_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let registry = Arc::new(PluginRegistry::new());
        let manager = PluginManager::new(dir.path().to_path_buf(), registry);

        let result = manager.uninstall("nonexistent");
        assert!(matches!(result, Err(PluginError::NotFound(_))));
    }

    #[test]
    fn test_plugin_manager_enable_disable() {
        let dir = tempfile::tempdir().unwrap();
        let registry = Arc::new(PluginRegistry::new());
        let manager = PluginManager::new(dir.path().to_path_buf(), registry);

        let manifest = create_test_manifest("toggler", PluginType::Interceptor);
        manager.install(manifest).unwrap();

        manager.disable("toggler").unwrap();
        assert!(!manager.get_installed("toggler").unwrap().enabled);

        manager.enable("toggler").unwrap();
        assert!(manager.get_installed("toggler").unwrap().enabled);
    }

    #[test]
    fn test_plugin_manager_discover() {
        let dir = tempfile::tempdir().unwrap();
        let registry = Arc::new(PluginRegistry::new());
        let manager = PluginManager::new(dir.path().to_path_buf(), registry);

        // Install two plugins so their manifests are on disk
        manager
            .install(create_test_manifest("plugin-a", PluginType::Sink))
            .unwrap();
        manager
            .install(create_test_manifest("plugin-b", PluginType::Metrics))
            .unwrap();

        let discovered = manager.discover().unwrap();
        assert_eq!(discovered.len(), 2);

        let names: Vec<&str> = discovered.iter().map(|m| m.name.as_str()).collect();
        assert!(names.contains(&"plugin-a"));
        assert!(names.contains(&"plugin-b"));
    }

    #[test]
    fn test_plugin_manager_configure() {
        let dir = tempfile::tempdir().unwrap();
        let registry = Arc::new(PluginRegistry::new());
        let manager = PluginManager::new(dir.path().to_path_buf(), registry);

        manager
            .install(create_test_manifest("configurable", PluginType::Auth))
            .unwrap();

        let mut config = HashMap::new();
        config.insert(
            "endpoint".to_string(),
            serde_json::Value::String("http://localhost".to_string()),
        );
        manager.configure("configurable", config).unwrap();

        let plugin = manager.get_installed("configurable").unwrap();
        assert_eq!(
            plugin.config.get("endpoint").unwrap(),
            &serde_json::Value::String("http://localhost".to_string())
        );
    }

    #[test]
    fn test_plugin_manager_stats() {
        let dir = tempfile::tempdir().unwrap();
        let registry = Arc::new(PluginRegistry::new());
        let manager = PluginManager::new(dir.path().to_path_buf(), registry);

        manager
            .install(create_test_manifest("s1", PluginType::Sink))
            .unwrap();
        manager
            .install(create_test_manifest("s2", PluginType::Sink))
            .unwrap();
        manager
            .install(create_test_manifest("t1", PluginType::Transform))
            .unwrap();
        manager.disable("s2").unwrap();

        let stats = manager.stats();
        assert_eq!(stats.total_installed, 3);
        assert_eq!(stats.enabled, 2);
        assert_eq!(stats.disabled, 1);
        assert_eq!(stats.by_type.get("sink"), Some(&2));
        assert_eq!(stats.by_type.get("transform"), Some(&1));
    }

    #[test]
    fn test_plugin_type_display() {
        assert_eq!(PluginType::Sink.to_string(), "sink");
        assert_eq!(PluginType::Source.to_string(), "source");
        assert_eq!(PluginType::Transform.to_string(), "transform");
        assert_eq!(PluginType::Interceptor.to_string(), "interceptor");
        assert_eq!(PluginType::Auth.to_string(), "auth");
        assert_eq!(PluginType::Metrics.to_string(), "metrics");
    }

    #[test]
    fn test_plugin_manifest_serialization() {
        let manifest = create_test_manifest("serial", PluginType::Source);
        let json = serde_json::to_string(&manifest).unwrap();
        let deserialized: PluginManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.name, "serial");
        assert_eq!(deserialized.version, "1.0.0");
    }
}
