//! Plugin SDK with Versioned Interface and Hot-Reload
//!
//! Provides a high-level SDK for building, registering, and managing Streamline
//! plugins with manifest validation, semver compatibility checks, lifecycle
//! management, and an event log.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::error::{Result, StreamlineError};

// ---------------------------------------------------------------------------
// Core types
// ---------------------------------------------------------------------------

/// Plugin type classification for the SDK layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SdkPluginType {
    Source,
    Sink,
    Processor,
    Transform,
    Interceptor,
}

impl std::fmt::Display for SdkPluginType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Source => write!(f, "source"),
            Self::Sink => write!(f, "sink"),
            Self::Processor => write!(f, "processor"),
            Self::Transform => write!(f, "transform"),
            Self::Interceptor => write!(f, "interceptor"),
        }
    }
}

/// Runtime status of a loaded plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PluginStatus {
    Loaded,
    Running,
    Stopped,
    Error(String),
    Unloading,
}

impl std::fmt::Display for PluginStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Loaded => write!(f, "loaded"),
            Self::Running => write!(f, "running"),
            Self::Stopped => write!(f, "stopped"),
            Self::Error(e) => write!(f, "error: {}", e),
            Self::Unloading => write!(f, "unloading"),
        }
    }
}

/// Capabilities a plugin may request.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginCapability {
    ReadTopics,
    WriteTopics,
    ReadConfig,
    HttpAccess,
    FileAccess,
    MetricsEmit,
}

/// A single configuration field definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigField {
    /// Data type: `"string"`, `"integer"`, `"boolean"`, `"array"`.
    pub field_type: String,
    pub required: bool,
    #[serde(default)]
    pub default: Option<serde_json::Value>,
    pub description: String,
}

/// Declarative manifest describing a plugin.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SdkPluginManifest {
    pub name: String,
    pub version: String,
    pub description: String,
    pub author: String,
    pub license: String,
    pub plugin_type: SdkPluginType,
    /// Must be `"1.0"`.
    pub api_version: String,
    pub min_streamline_version: String,
    #[serde(default)]
    pub capabilities: Vec<PluginCapability>,
    #[serde(default)]
    pub config_schema: HashMap<String, ConfigField>,
    pub entrypoint: String,
}

/// A plugin that has been loaded into the registry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadedPlugin {
    pub manifest: SdkPluginManifest,
    pub status: PluginStatus,
    pub loaded_at: u64,
    pub last_invoked_at: Option<u64>,
    pub invocation_count: u64,
    pub error_count: u64,
}

/// Event types emitted during plugin lifecycle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PluginEventType {
    Loaded,
    Started,
    Stopped,
    Error,
    Reloaded,
    Unloaded,
}

/// A recorded lifecycle event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginEvent {
    pub plugin_name: String,
    pub event_type: PluginEventType,
    pub timestamp: u64,
    pub details: String,
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Runtime configuration for the [`SdkPluginRegistry`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginConfig {
    /// Directory to scan for plugin artifacts.
    pub plugin_dir: String,
    /// Whether hot-reload is enabled.
    pub enable_hot_reload: bool,
    /// Maximum number of plugins that can be registered.
    pub max_plugins: usize,
    /// Whether sandboxing is enabled.
    pub sandbox_enabled: bool,
    /// Capabilities the host permits plugins to use.
    pub allowed_capabilities: Vec<PluginCapability>,
}

impl Default for PluginConfig {
    fn default() -> Self {
        Self {
            plugin_dir: "./plugins".to_string(),
            enable_hot_reload: false,
            max_plugins: 100,
            sandbox_enabled: true,
            allowed_capabilities: vec![],
        }
    }
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

/// Atomic counters for plugin operations.
#[derive(Debug, Default)]
pub struct PluginStats {
    pub total_registered: AtomicU64,
    pub total_started: AtomicU64,
    pub total_stopped: AtomicU64,
    pub total_errors: AtomicU64,
    pub total_reloads: AtomicU64,
}

/// Point-in-time snapshot of [`PluginStats`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginStatsSnapshot {
    pub total_registered: u64,
    pub total_started: u64,
    pub total_stopped: u64,
    pub total_errors: u64,
    pub total_reloads: u64,
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

/// Central registry for SDK-managed plugins.
pub struct SdkPluginRegistry {
    plugins: Arc<RwLock<HashMap<String, LoadedPlugin>>>,
    config: PluginConfig,
    stats: Arc<PluginStats>,
    events: Arc<RwLock<Vec<PluginEvent>>>,
}

impl SdkPluginRegistry {
    /// Create a new registry with the given configuration.
    pub fn new(config: PluginConfig) -> Self {
        Self {
            plugins: Arc::new(RwLock::new(HashMap::new())),
            config,
            stats: Arc::new(PluginStats::default()),
            events: Arc::new(RwLock::new(Vec::new())),
        }
    }

    // -- helpers ----------------------------------------------------------

    fn now_millis() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    fn record_event(&self, name: &str, event_type: PluginEventType, details: impl Into<String>) {
        self.events.write().push(PluginEvent {
            plugin_name: name.to_string(),
            event_type,
            timestamp: Self::now_millis(),
            details: details.into(),
        });
    }

    // -- public API -------------------------------------------------------

    /// Register a plugin described by `manifest`.
    pub fn register(&self, manifest: SdkPluginManifest) -> Result<()> {
        let errors = Self::validate_manifest(&manifest);
        if !errors.is_empty() {
            return Err(StreamlineError::Config(format!(
                "Invalid plugin manifest: {}",
                errors.join("; ")
            )));
        }

        let mut plugins = self.plugins.write();

        if plugins.len() >= self.config.max_plugins {
            return Err(StreamlineError::Config(format!(
                "Maximum plugin limit ({}) reached",
                self.config.max_plugins
            )));
        }

        if plugins.contains_key(&manifest.name) {
            return Err(StreamlineError::Config(format!(
                "Plugin '{}' is already registered",
                manifest.name
            )));
        }

        // Capability check
        if self.config.sandbox_enabled {
            for cap in &manifest.capabilities {
                if !self.config.allowed_capabilities.contains(cap) {
                    return Err(StreamlineError::Config(format!(
                        "Plugin '{}' requests disallowed capability: {:?}",
                        manifest.name, cap
                    )));
                }
            }
        }

        let name = manifest.name.clone();
        plugins.insert(
            name.clone(),
            LoadedPlugin {
                manifest,
                status: PluginStatus::Loaded,
                loaded_at: Self::now_millis(),
                last_invoked_at: None,
                invocation_count: 0,
                error_count: 0,
            },
        );

        self.stats.total_registered.fetch_add(1, Ordering::Relaxed);
        self.record_event(&name, PluginEventType::Loaded, "Plugin registered");
        info!(plugin = %name, "Plugin registered");
        Ok(())
    }

    /// Unregister a plugin by name, removing it from the registry.
    pub fn unregister(&self, name: &str) -> Result<()> {
        let mut plugins = self.plugins.write();
        if plugins.remove(name).is_none() {
            return Err(StreamlineError::Config(format!(
                "Plugin '{}' not found",
                name
            )));
        }
        self.record_event(name, PluginEventType::Unloaded, "Plugin unregistered");
        info!(plugin = %name, "Plugin unregistered");
        Ok(())
    }

    /// Retrieve a clone of a loaded plugin by name.
    pub fn get(&self, name: &str) -> Option<LoadedPlugin> {
        self.plugins.read().get(name).cloned()
    }

    /// List all loaded plugins.
    pub fn list(&self) -> Vec<LoadedPlugin> {
        self.plugins.read().values().cloned().collect()
    }

    /// List plugins filtered by [`SdkPluginType`].
    pub fn list_by_type(&self, plugin_type: SdkPluginType) -> Vec<LoadedPlugin> {
        self.plugins
            .read()
            .values()
            .filter(|p| p.manifest.plugin_type == plugin_type)
            .cloned()
            .collect()
    }

    /// Transition a loaded/stopped plugin to [`PluginStatus::Running`].
    pub fn start(&self, name: &str) -> Result<()> {
        let mut plugins = self.plugins.write();
        let plugin = plugins.get_mut(name).ok_or_else(|| {
            StreamlineError::Config(format!("Plugin '{}' not found", name))
        })?;

        match &plugin.status {
            PluginStatus::Loaded | PluginStatus::Stopped => {
                plugin.status = PluginStatus::Running;
                self.stats.total_started.fetch_add(1, Ordering::Relaxed);
                drop(plugins);
                self.record_event(name, PluginEventType::Started, "Plugin started");
                info!(plugin = %name, "Plugin started");
                Ok(())
            }
            other => Err(StreamlineError::Config(format!(
                "Cannot start plugin '{}' in state: {}",
                name, other
            ))),
        }
    }

    /// Stop a running plugin.
    pub fn stop(&self, name: &str) -> Result<()> {
        let mut plugins = self.plugins.write();
        let plugin = plugins.get_mut(name).ok_or_else(|| {
            StreamlineError::Config(format!("Plugin '{}' not found", name))
        })?;

        match &plugin.status {
            PluginStatus::Running => {
                plugin.status = PluginStatus::Stopped;
                self.stats.total_stopped.fetch_add(1, Ordering::Relaxed);
                drop(plugins);
                self.record_event(name, PluginEventType::Stopped, "Plugin stopped");
                info!(plugin = %name, "Plugin stopped");
                Ok(())
            }
            other => Err(StreamlineError::Config(format!(
                "Cannot stop plugin '{}' in state: {}",
                name, other
            ))),
        }
    }

    /// Reload a plugin (stop → load → start). Requires hot-reload to be enabled.
    pub fn reload(&self, name: &str) -> Result<()> {
        if !self.config.enable_hot_reload {
            return Err(StreamlineError::Config(
                "Hot-reload is not enabled".to_string(),
            ));
        }

        let mut plugins = self.plugins.write();
        let plugin = plugins.get_mut(name).ok_or_else(|| {
            StreamlineError::Config(format!("Plugin '{}' not found", name))
        })?;

        plugin.status = PluginStatus::Unloading;
        plugin.status = PluginStatus::Running;
        plugin.loaded_at = Self::now_millis();
        self.stats.total_reloads.fetch_add(1, Ordering::Relaxed);
        drop(plugins);
        self.record_event(name, PluginEventType::Reloaded, "Plugin reloaded");
        info!(plugin = %name, "Plugin reloaded");
        Ok(())
    }

    /// Validate a manifest, returning a list of human-readable errors (empty = valid).
    pub fn validate_manifest(manifest: &SdkPluginManifest) -> Vec<String> {
        let mut errors = Vec::new();

        // Name: non-empty, kebab-case (lowercase ascii + digits + hyphens, no leading/trailing hyphen)
        if manifest.name.is_empty() {
            errors.push("Plugin name must not be empty".to_string());
        } else if !is_kebab_case(&manifest.name) {
            errors.push(format!(
                "Plugin name '{}' must be kebab-case (lowercase letters, digits, and hyphens; must not start or end with a hyphen)",
                manifest.name
            ));
        }

        // Version: semver-like (X.Y.Z with optional pre-release)
        if !is_semver_like(&manifest.version) {
            errors.push(format!(
                "Plugin version '{}' must follow semver (e.g. 1.0.0)",
                manifest.version
            ));
        }

        // API version
        if manifest.api_version != "1.0" {
            errors.push(format!(
                "Unsupported api_version '{}'; must be '1.0'",
                manifest.api_version
            ));
        }

        // Description
        if manifest.description.is_empty() {
            errors.push("Plugin description must not be empty".to_string());
        }

        // Author
        if manifest.author.is_empty() {
            errors.push("Plugin author must not be empty".to_string());
        }

        // Entrypoint
        if manifest.entrypoint.is_empty() {
            errors.push("Plugin entrypoint must not be empty".to_string());
        }

        // min_streamline_version should be semver-like if provided
        if !manifest.min_streamline_version.is_empty()
            && !is_semver_like(&manifest.min_streamline_version)
        {
            errors.push(format!(
                "min_streamline_version '{}' must follow semver",
                manifest.min_streamline_version
            ));
        }

        errors
    }

    /// Check whether a manifest is compatible with the given Streamline version.
    ///
    /// Compatibility rule: the manifest's `min_streamline_version` major.minor must
    /// be ≤ the provided `streamline_version` major.minor.
    pub fn check_compatibility(manifest: &SdkPluginManifest, streamline_version: &str) -> bool {
        let Some((req_major, req_minor)) = parse_major_minor(&manifest.min_streamline_version)
        else {
            warn!(
                version = %manifest.min_streamline_version,
                "Cannot parse min_streamline_version; assuming incompatible"
            );
            return false;
        };
        let Some((sv_major, sv_minor)) = parse_major_minor(streamline_version) else {
            warn!(
                version = %streamline_version,
                "Cannot parse streamline_version; assuming incompatible"
            );
            return false;
        };

        if req_major != sv_major {
            return req_major < sv_major;
        }
        req_minor <= sv_minor
    }

    /// Return all recorded lifecycle events.
    pub fn events(&self) -> Vec<PluginEvent> {
        self.events.read().clone()
    }

    /// Snapshot of current statistics.
    pub fn stats(&self) -> PluginStatsSnapshot {
        PluginStatsSnapshot {
            total_registered: self.stats.total_registered.load(Ordering::Relaxed),
            total_started: self.stats.total_started.load(Ordering::Relaxed),
            total_stopped: self.stats.total_stopped.load(Ordering::Relaxed),
            total_errors: self.stats.total_errors.load(Ordering::Relaxed),
            total_reloads: self.stats.total_reloads.load(Ordering::Relaxed),
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Returns `true` if `s` is valid kebab-case: `[a-z0-9]([a-z0-9-]*[a-z0-9])?`.
fn is_kebab_case(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }
    let bytes = s.as_bytes();
    if bytes[0] == b'-' || bytes[bytes.len() - 1] == b'-' {
        return false;
    }
    s.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
}

/// Simple semver-like check: `MAJOR.MINOR.PATCH` where each part is a non-negative integer.
/// Allows an optional pre-release suffix starting with `-`.
fn is_semver_like(s: &str) -> bool {
    let core = s.split('-').next().unwrap_or("");
    let parts: Vec<&str> = core.split('.').collect();
    if parts.len() != 3 {
        return false;
    }
    parts.iter().all(|p| !p.is_empty() && p.chars().all(|c| c.is_ascii_digit()))
}

/// Parse the major and minor components from a version string.
fn parse_major_minor(version: &str) -> Option<(u64, u64)> {
    let core = version.split('-').next()?;
    let mut parts = core.split('.');
    let major = parts.next()?.parse::<u64>().ok()?;
    let minor = parts.next()?.parse::<u64>().ok()?;
    Some((major, minor))
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> PluginConfig {
        PluginConfig {
            plugin_dir: "./test-plugins".to_string(),
            enable_hot_reload: false,
            max_plugins: 100,
            sandbox_enabled: false,
            allowed_capabilities: vec![],
        }
    }

    fn test_manifest(name: &str) -> SdkPluginManifest {
        SdkPluginManifest {
            name: name.to_string(),
            version: "1.0.0".to_string(),
            description: "A test plugin".to_string(),
            author: "tester".to_string(),
            license: "MIT".to_string(),
            plugin_type: SdkPluginType::Processor,
            api_version: "1.0".to_string(),
            min_streamline_version: "0.2.0".to_string(),
            capabilities: vec![],
            config_schema: HashMap::new(),
            entrypoint: "main.wasm".to_string(),
        }
    }

    // -- registry basics --------------------------------------------------

    #[test]
    fn test_register_and_get() {
        let reg = SdkPluginRegistry::new(test_config());
        reg.register(test_manifest("my-plugin")).unwrap();
        let p = reg.get("my-plugin").unwrap();
        assert_eq!(p.manifest.name, "my-plugin");
        assert_eq!(p.status, PluginStatus::Loaded);
    }

    #[test]
    fn test_register_duplicate_fails() {
        let reg = SdkPluginRegistry::new(test_config());
        reg.register(test_manifest("dup")).unwrap();
        assert!(reg.register(test_manifest("dup")).is_err());
    }

    #[test]
    fn test_unregister() {
        let reg = SdkPluginRegistry::new(test_config());
        reg.register(test_manifest("removable")).unwrap();
        reg.unregister("removable").unwrap();
        assert!(reg.get("removable").is_none());
    }

    #[test]
    fn test_unregister_not_found() {
        let reg = SdkPluginRegistry::new(test_config());
        assert!(reg.unregister("ghost").is_err());
    }

    #[test]
    fn test_list() {
        let reg = SdkPluginRegistry::new(test_config());
        reg.register(test_manifest("a")).unwrap();
        reg.register(test_manifest("b")).unwrap();
        assert_eq!(reg.list().len(), 2);
    }

    #[test]
    fn test_list_by_type() {
        let reg = SdkPluginRegistry::new(test_config());

        let mut src = test_manifest("src-plugin");
        src.plugin_type = SdkPluginType::Source;
        reg.register(src).unwrap();

        reg.register(test_manifest("proc-plugin")).unwrap();

        assert_eq!(reg.list_by_type(SdkPluginType::Source).len(), 1);
        assert_eq!(reg.list_by_type(SdkPluginType::Processor).len(), 1);
        assert_eq!(reg.list_by_type(SdkPluginType::Sink).len(), 0);
    }

    // -- lifecycle --------------------------------------------------------

    #[test]
    fn test_start_and_stop() {
        let reg = SdkPluginRegistry::new(test_config());
        reg.register(test_manifest("svc")).unwrap();

        reg.start("svc").unwrap();
        assert_eq!(reg.get("svc").unwrap().status, PluginStatus::Running);

        reg.stop("svc").unwrap();
        assert_eq!(reg.get("svc").unwrap().status, PluginStatus::Stopped);
    }

    #[test]
    fn test_start_not_found() {
        let reg = SdkPluginRegistry::new(test_config());
        assert!(reg.start("missing").is_err());
    }

    #[test]
    fn test_stop_not_running_fails() {
        let reg = SdkPluginRegistry::new(test_config());
        reg.register(test_manifest("idle")).unwrap();
        assert!(reg.stop("idle").is_err());
    }

    #[test]
    fn test_start_already_running_fails() {
        let reg = SdkPluginRegistry::new(test_config());
        reg.register(test_manifest("run")).unwrap();
        reg.start("run").unwrap();
        assert!(reg.start("run").is_err());
    }

    #[test]
    fn test_restart_after_stop() {
        let reg = SdkPluginRegistry::new(test_config());
        reg.register(test_manifest("restart")).unwrap();
        reg.start("restart").unwrap();
        reg.stop("restart").unwrap();
        reg.start("restart").unwrap();
        assert_eq!(reg.get("restart").unwrap().status, PluginStatus::Running);
    }

    #[test]
    fn test_reload_requires_hot_reload() {
        let reg = SdkPluginRegistry::new(test_config());
        reg.register(test_manifest("hr")).unwrap();
        assert!(reg.reload("hr").is_err());
    }

    #[test]
    fn test_reload_with_hot_reload_enabled() {
        let mut cfg = test_config();
        cfg.enable_hot_reload = true;
        let reg = SdkPluginRegistry::new(cfg);
        reg.register(test_manifest("hr")).unwrap();
        reg.reload("hr").unwrap();
        assert_eq!(reg.get("hr").unwrap().status, PluginStatus::Running);
    }

    // -- manifest validation ----------------------------------------------

    #[test]
    fn test_validate_manifest_valid() {
        let errors = SdkPluginRegistry::validate_manifest(&test_manifest("good-plugin"));
        assert!(errors.is_empty(), "Expected no errors, got: {:?}", errors);
    }

    #[test]
    fn test_validate_manifest_empty_name() {
        let mut m = test_manifest("");
        m.name = String::new();
        let errors = SdkPluginRegistry::validate_manifest(&m);
        assert!(errors.iter().any(|e| e.contains("name")));
    }

    #[test]
    fn test_validate_manifest_non_kebab_name() {
        let mut m = test_manifest("BadName");
        m.name = "BadName".to_string();
        let errors = SdkPluginRegistry::validate_manifest(&m);
        assert!(errors.iter().any(|e| e.contains("kebab-case")));
    }

    #[test]
    fn test_validate_manifest_bad_version() {
        let mut m = test_manifest("p");
        m.version = "latest".to_string();
        let errors = SdkPluginRegistry::validate_manifest(&m);
        assert!(errors.iter().any(|e| e.contains("semver")));
    }

    #[test]
    fn test_validate_manifest_bad_api_version() {
        let mut m = test_manifest("p");
        m.api_version = "2.0".to_string();
        let errors = SdkPluginRegistry::validate_manifest(&m);
        assert!(errors.iter().any(|e| e.contains("api_version")));
    }

    #[test]
    fn test_validate_manifest_empty_description() {
        let mut m = test_manifest("p");
        m.description = String::new();
        let errors = SdkPluginRegistry::validate_manifest(&m);
        assert!(errors.iter().any(|e| e.contains("description")));
    }

    #[test]
    fn test_validate_manifest_empty_author() {
        let mut m = test_manifest("p");
        m.author = String::new();
        let errors = SdkPluginRegistry::validate_manifest(&m);
        assert!(errors.iter().any(|e| e.contains("author")));
    }

    #[test]
    fn test_validate_manifest_empty_entrypoint() {
        let mut m = test_manifest("p");
        m.entrypoint = String::new();
        let errors = SdkPluginRegistry::validate_manifest(&m);
        assert!(errors.iter().any(|e| e.contains("entrypoint")));
    }

    // -- compatibility ----------------------------------------------------

    #[test]
    fn test_compatibility_same_version() {
        let m = test_manifest("p");
        assert!(SdkPluginRegistry::check_compatibility(&m, "0.2.0"));
    }

    #[test]
    fn test_compatibility_higher_minor() {
        let m = test_manifest("p");
        assert!(SdkPluginRegistry::check_compatibility(&m, "0.3.0"));
    }

    #[test]
    fn test_compatibility_lower_minor_fails() {
        let m = test_manifest("p");
        assert!(!SdkPluginRegistry::check_compatibility(&m, "0.1.0"));
    }

    #[test]
    fn test_compatibility_higher_major() {
        let m = test_manifest("p");
        assert!(SdkPluginRegistry::check_compatibility(&m, "1.0.0"));
    }

    #[test]
    fn test_compatibility_lower_major_fails() {
        let mut m = test_manifest("p");
        m.min_streamline_version = "2.0.0".to_string();
        assert!(!SdkPluginRegistry::check_compatibility(&m, "1.5.0"));
    }

    // -- events & stats ---------------------------------------------------

    #[test]
    fn test_events_recorded() {
        let reg = SdkPluginRegistry::new(test_config());
        reg.register(test_manifest("ev")).unwrap();
        reg.start("ev").unwrap();
        reg.stop("ev").unwrap();
        let events = reg.events();
        assert!(events.len() >= 3);
        assert_eq!(events[0].event_type, PluginEventType::Loaded);
        assert_eq!(events[1].event_type, PluginEventType::Started);
        assert_eq!(events[2].event_type, PluginEventType::Stopped);
    }

    #[test]
    fn test_stats_snapshot() {
        let reg = SdkPluginRegistry::new(test_config());
        reg.register(test_manifest("s1")).unwrap();
        reg.register(test_manifest("s2")).unwrap();
        reg.start("s1").unwrap();
        let snap = reg.stats();
        assert_eq!(snap.total_registered, 2);
        assert_eq!(snap.total_started, 1);
    }

    // -- max plugins limit ------------------------------------------------

    #[test]
    fn test_max_plugins_limit() {
        let mut cfg = test_config();
        cfg.max_plugins = 2;
        let reg = SdkPluginRegistry::new(cfg);
        reg.register(test_manifest("a")).unwrap();
        reg.register(test_manifest("b")).unwrap();
        assert!(reg.register(test_manifest("c")).is_err());
    }

    // -- sandbox capability check -----------------------------------------

    #[test]
    fn test_sandbox_blocks_disallowed_capability() {
        let cfg = PluginConfig {
            sandbox_enabled: true,
            allowed_capabilities: vec![PluginCapability::ReadTopics],
            ..test_config()
        };
        let reg = SdkPluginRegistry::new(cfg);

        let mut m = test_manifest("blocked");
        m.capabilities = vec![PluginCapability::FileAccess];
        assert!(reg.register(m).is_err());
    }

    #[test]
    fn test_sandbox_allows_permitted_capability() {
        let cfg = PluginConfig {
            sandbox_enabled: true,
            allowed_capabilities: vec![PluginCapability::ReadTopics],
            ..test_config()
        };
        let reg = SdkPluginRegistry::new(cfg);

        let mut m = test_manifest("allowed");
        m.capabilities = vec![PluginCapability::ReadTopics];
        assert!(reg.register(m).is_ok());
    }

    // -- helper unit tests ------------------------------------------------

    #[test]
    fn test_is_kebab_case() {
        assert!(is_kebab_case("my-plugin"));
        assert!(is_kebab_case("a"));
        assert!(is_kebab_case("plugin123"));
        assert!(is_kebab_case("my-cool-plugin-2"));
        assert!(!is_kebab_case(""));
        assert!(!is_kebab_case("-leading"));
        assert!(!is_kebab_case("trailing-"));
        assert!(!is_kebab_case("CamelCase"));
        assert!(!is_kebab_case("under_score"));
    }

    #[test]
    fn test_is_semver_like() {
        assert!(is_semver_like("1.0.0"));
        assert!(is_semver_like("0.2.0"));
        assert!(is_semver_like("12.34.56"));
        assert!(is_semver_like("1.0.0-beta"));
        assert!(!is_semver_like("latest"));
        assert!(!is_semver_like("1.0"));
        assert!(!is_semver_like("1.0.0.0"));
        assert!(!is_semver_like(""));
    }

    // -- serialization round-trip -----------------------------------------

    #[test]
    fn test_manifest_serde_round_trip() {
        let m = test_manifest("serde-test");
        let json = serde_json::to_string(&m).unwrap();
        let deser: SdkPluginManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.name, "serde-test");
        assert_eq!(deser.api_version, "1.0");
    }

    #[test]
    fn test_plugin_config_default() {
        let cfg = PluginConfig::default();
        assert_eq!(cfg.plugin_dir, "./plugins");
        assert!(!cfg.enable_hot_reload);
        assert_eq!(cfg.max_plugins, 100);
        assert!(cfg.sandbox_enabled);
    }

    #[test]
    fn test_plugin_type_display() {
        assert_eq!(SdkPluginType::Source.to_string(), "source");
        assert_eq!(SdkPluginType::Sink.to_string(), "sink");
        assert_eq!(SdkPluginType::Processor.to_string(), "processor");
        assert_eq!(SdkPluginType::Transform.to_string(), "transform");
        assert_eq!(SdkPluginType::Interceptor.to_string(), "interceptor");
    }

    #[test]
    fn test_plugin_status_display() {
        assert_eq!(PluginStatus::Loaded.to_string(), "loaded");
        assert_eq!(PluginStatus::Running.to_string(), "running");
        assert_eq!(PluginStatus::Stopped.to_string(), "stopped");
        assert_eq!(
            PluginStatus::Error("oops".to_string()).to_string(),
            "error: oops"
        );
        assert_eq!(PluginStatus::Unloading.to_string(), "unloading");
    }
}
