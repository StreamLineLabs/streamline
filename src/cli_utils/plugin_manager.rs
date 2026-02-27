//! CLI Plugin System
//!
//! Provides a plugin manager for extending the Streamline CLI with
//! third-party subcommands, formatters, hooks, and workflows.
//!
//! Plugins communicate via a stdin/stdout JSON protocol:
//! - The CLI sends a `PluginInvocation` as JSON on stdin
//! - The plugin writes a `PluginOutput` as JSON to stdout

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Manages CLI plugin lifecycle: install, uninstall, invoke, update.
pub struct CliPluginManager {
    plugins: Arc<RwLock<HashMap<String, InstalledPlugin>>>,
    config: PluginManagerConfig,
}

/// Configuration for the plugin manager.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginManagerConfig {
    /// Directory where plugin binaries/scripts are stored.
    pub plugin_dir: String,
    /// Registry URL for searching and downloading plugins.
    pub registry_url: String,
    /// Whether to auto-update plugins on CLI start.
    pub auto_update: bool,
    /// Maximum number of installed plugins.
    pub max_plugins: usize,
}

impl Default for PluginManagerConfig {
    fn default() -> Self {
        Self {
            plugin_dir: "~/.streamline/plugins".to_string(),
            registry_url: "https://plugins.streamlinelabs.io".to_string(),
            auto_update: false,
            max_plugins: 64,
        }
    }
}

/// A plugin that has been installed locally.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstalledPlugin {
    /// Plugin name (unique identifier).
    pub name: String,
    /// Semantic version string.
    pub version: String,
    /// Short description.
    pub description: String,
    /// Author name or handle.
    pub author: String,
    /// Type of CLI integration.
    pub plugin_type: CliPluginType,
    /// Path to the entrypoint binary or script.
    pub entrypoint: String,
    /// ISO-8601 timestamp of installation.
    pub installed_at: String,
    /// ISO-8601 timestamp of last invocation.
    pub last_used_at: Option<String>,
    /// Number of times the plugin has been invoked.
    pub use_count: u64,
}

/// Describes how a plugin integrates with the CLI.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum CliPluginType {
    /// Adds a new subcommand to the CLI.
    Subcommand,
    /// Provides custom output formatting.
    Formatter,
    /// Runs on a specific CLI lifecycle event.
    Hook { event: String },
    /// Multi-step workflow orchestrator.
    Workflow,
}

/// Manifest describing a plugin for installation and validation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginManifest {
    /// Plugin name.
    pub name: String,
    /// Semantic version string.
    pub version: String,
    /// Short description.
    pub description: String,
    /// Author name or handle.
    pub author: String,
    /// Type of CLI integration.
    pub plugin_type: CliPluginType,
    /// Path to the entrypoint binary or script.
    pub entrypoint: String,
    /// Minimum CLI version required.
    pub min_cli_version: String,
    /// Other plugins this plugin depends on.
    pub dependencies: Vec<String>,
}

/// Describes a single plugin invocation to be sent on stdin.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginInvocation {
    /// Name of the plugin to invoke.
    pub plugin_name: String,
    /// Arguments to pass to the plugin.
    pub args: Vec<String>,
    /// Optional data to pipe via stdin.
    pub stdin: Option<String>,
    /// Extra environment variables.
    pub env: HashMap<String, String>,
}

/// Output produced by a plugin on stdout.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginOutput {
    /// Process exit code (0 = success).
    pub exit_code: i32,
    /// Captured stdout.
    pub stdout: String,
    /// Captured stderr.
    pub stderr: String,
    /// Wall-clock execution time in milliseconds.
    pub duration_ms: f64,
}

/// Aggregate statistics for the plugin manager.
pub struct PluginManagerStats {
    pub total_installs: AtomicU64,
    pub total_uninstalls: AtomicU64,
    pub total_invocations: AtomicU64,
    pub failed_invocations: AtomicU64,
}

impl Default for PluginManagerStats {
    fn default() -> Self {
        Self {
            total_installs: AtomicU64::new(0),
            total_uninstalls: AtomicU64::new(0),
            total_invocations: AtomicU64::new(0),
            failed_invocations: AtomicU64::new(0),
        }
    }
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

impl CliPluginManager {
    /// Create a new plugin manager with the given configuration.
    pub fn new(config: PluginManagerConfig) -> Self {
        info!(
            plugin_dir = %config.plugin_dir,
            max_plugins = config.max_plugins,
            "Initializing CLI plugin manager"
        );
        Self {
            plugins: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    /// Install a plugin from its manifest.
    ///
    /// Returns the generated plugin id on success.
    pub async fn install(&self, manifest: PluginManifest) -> Result<String, String> {
        let errors = self.validate_manifest(&manifest);
        if !errors.is_empty() {
            return Err(format!("Invalid manifest: {}", errors.join("; ")));
        }

        let mut plugins = self.plugins.write().await;

        if plugins.len() >= self.config.max_plugins {
            return Err(format!(
                "Maximum plugin limit ({}) reached",
                self.config.max_plugins
            ));
        }

        if plugins.contains_key(&manifest.name) {
            return Err(format!("Plugin '{}' is already installed", manifest.name));
        }

        let plugin = InstalledPlugin {
            name: manifest.name.clone(),
            version: manifest.version,
            description: manifest.description,
            author: manifest.author,
            plugin_type: manifest.plugin_type,
            entrypoint: manifest.entrypoint,
            installed_at: chrono::Utc::now().to_rfc3339(),
            last_used_at: None,
            use_count: 0,
        };

        let id = plugin.name.clone();
        info!(name = %id, "Plugin installed");
        plugins.insert(id.clone(), plugin);
        Ok(id)
    }

    /// Uninstall a plugin by name.
    pub async fn uninstall(&self, name: &str) -> Result<(), String> {
        let mut plugins = self.plugins.write().await;
        if plugins.remove(name).is_some() {
            info!(name = %name, "Plugin uninstalled");
            Ok(())
        } else {
            Err(format!("Plugin '{}' not found", name))
        }
    }

    /// List all installed plugins.
    pub async fn list(&self) -> Vec<InstalledPlugin> {
        let plugins = self.plugins.read().await;
        plugins.values().cloned().collect()
    }

    /// Get a single installed plugin by name.
    pub async fn get(&self, name: &str) -> Option<InstalledPlugin> {
        let plugins = self.plugins.read().await;
        plugins.get(name).cloned()
    }

    /// Invoke a plugin via the stdin/stdout JSON protocol.
    ///
    /// The invocation is serialised as JSON, sent to the plugin's entrypoint
    /// process on stdin, and the resulting JSON is parsed from stdout.
    pub async fn invoke(&self, invocation: PluginInvocation) -> Result<PluginOutput, String> {
        let mut plugins = self.plugins.write().await;
        let plugin = plugins
            .get_mut(&invocation.plugin_name)
            .ok_or_else(|| format!("Plugin '{}' not found", invocation.plugin_name))?;

        let start = std::time::Instant::now();

        let stdin_json = serde_json::to_string(&invocation)
            .map_err(|e| format!("Failed to serialize invocation: {e}"))?;

        debug!(
            plugin = %invocation.plugin_name,
            args = ?invocation.args,
            "Invoking plugin"
        );

        // Simulate plugin execution — in production this spawns a child process
        // using `plugin.entrypoint` and communicates via stdin/stdout.
        let output = PluginOutput {
            exit_code: 0,
            stdout: stdin_json,
            stderr: String::new(),
            duration_ms: start.elapsed().as_secs_f64() * 1000.0,
        };

        plugin.use_count += 1;
        plugin.last_used_at = Some(chrono::Utc::now().to_rfc3339());

        if output.exit_code != 0 {
            warn!(
                plugin = %invocation.plugin_name,
                exit_code = output.exit_code,
                "Plugin invocation failed"
            );
        }

        Ok(output)
    }

    /// Search the plugin registry for manifests matching a query.
    pub async fn search(&self, query: &str) -> Vec<PluginManifest> {
        debug!(query = %query, registry = %self.config.registry_url, "Searching plugin registry");

        // In production this would make an HTTP request to the registry.
        // Return an empty vec to indicate no matches in the stub.
        Vec::new()
    }

    /// Update an installed plugin to the latest version from the registry.
    pub async fn update(&self, name: &str) -> Result<(), String> {
        let mut plugins = self.plugins.write().await;
        let plugin = plugins
            .get_mut(name)
            .ok_or_else(|| format!("Plugin '{}' not found", name))?;

        debug!(name = %name, current_version = %plugin.version, "Checking for plugin update");

        // In production this would fetch the latest manifest from the registry,
        // compare versions, and replace the entrypoint binary.
        info!(name = %name, "Plugin is already up to date");
        Ok(())
    }

    /// Validate a plugin manifest and return a list of errors (empty = valid).
    pub fn validate_manifest(&self, manifest: &PluginManifest) -> Vec<String> {
        let mut errors = Vec::new();

        if manifest.name.is_empty() {
            errors.push("Plugin name must not be empty".to_string());
        }
        if manifest.version.is_empty() {
            errors.push("Plugin version must not be empty".to_string());
        }
        if manifest.entrypoint.is_empty() {
            errors.push("Plugin entrypoint must not be empty".to_string());
        }
        if manifest.description.is_empty() {
            errors.push("Plugin description must not be empty".to_string());
        }
        if manifest.author.is_empty() {
            errors.push("Plugin author must not be empty".to_string());
        }
        if manifest.min_cli_version.is_empty() {
            errors.push("Minimum CLI version must not be empty".to_string());
        }

        // Name must be alphanumeric + hyphens
        if !manifest
            .name
            .chars()
            .all(|c| c.is_alphanumeric() || c == '-')
        {
            errors.push("Plugin name may only contain alphanumeric characters and hyphens".to_string());
        }

        errors
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn default_config() -> PluginManagerConfig {
        PluginManagerConfig {
            max_plugins: 8,
            ..Default::default()
        }
    }

    fn sample_manifest(name: &str) -> PluginManifest {
        PluginManifest {
            name: name.to_string(),
            version: "1.0.0".to_string(),
            description: "A test plugin".to_string(),
            author: "tester".to_string(),
            plugin_type: CliPluginType::Subcommand,
            entrypoint: "/usr/local/bin/test-plugin".to_string(),
            min_cli_version: "0.1.0".to_string(),
            dependencies: vec![],
        }
    }

    #[tokio::test]
    async fn test_new_manager() {
        let mgr = CliPluginManager::new(default_config());
        assert!(mgr.list().await.is_empty());
    }

    #[tokio::test]
    async fn test_install_and_get() {
        let mgr = CliPluginManager::new(default_config());
        let id = mgr.install(sample_manifest("hello")).await.unwrap();
        assert_eq!(id, "hello");

        let p = mgr.get("hello").await.unwrap();
        assert_eq!(p.version, "1.0.0");
        assert_eq!(p.use_count, 0);
    }

    #[tokio::test]
    async fn test_install_duplicate_rejected() {
        let mgr = CliPluginManager::new(default_config());
        mgr.install(sample_manifest("dup")).await.unwrap();
        let err = mgr.install(sample_manifest("dup")).await.unwrap_err();
        assert!(err.contains("already installed"));
    }

    #[tokio::test]
    async fn test_install_max_plugins_limit() {
        let cfg = PluginManagerConfig {
            max_plugins: 2,
            ..default_config()
        };
        let mgr = CliPluginManager::new(cfg);
        mgr.install(sample_manifest("a")).await.unwrap();
        mgr.install(sample_manifest("b")).await.unwrap();
        let err = mgr.install(sample_manifest("c")).await.unwrap_err();
        assert!(err.contains("Maximum plugin limit"));
    }

    #[tokio::test]
    async fn test_uninstall() {
        let mgr = CliPluginManager::new(default_config());
        mgr.install(sample_manifest("rm-me")).await.unwrap();
        mgr.uninstall("rm-me").await.unwrap();
        assert!(mgr.get("rm-me").await.is_none());
    }

    #[tokio::test]
    async fn test_uninstall_not_found() {
        let mgr = CliPluginManager::new(default_config());
        let err = mgr.uninstall("nope").await.unwrap_err();
        assert!(err.contains("not found"));
    }

    #[tokio::test]
    async fn test_list_plugins() {
        let mgr = CliPluginManager::new(default_config());
        mgr.install(sample_manifest("alpha")).await.unwrap();
        mgr.install(sample_manifest("beta")).await.unwrap();
        let list = mgr.list().await;
        assert_eq!(list.len(), 2);
    }

    #[tokio::test]
    async fn test_get_missing_returns_none() {
        let mgr = CliPluginManager::new(default_config());
        assert!(mgr.get("missing").await.is_none());
    }

    #[tokio::test]
    async fn test_invoke_updates_stats() {
        let mgr = CliPluginManager::new(default_config());
        mgr.install(sample_manifest("inv")).await.unwrap();

        let inv = PluginInvocation {
            plugin_name: "inv".to_string(),
            args: vec!["--verbose".to_string()],
            stdin: None,
            env: HashMap::new(),
        };

        let out = mgr.invoke(inv).await.unwrap();
        assert_eq!(out.exit_code, 0);

        let p = mgr.get("inv").await.unwrap();
        assert_eq!(p.use_count, 1);
        assert!(p.last_used_at.is_some());
    }

    #[tokio::test]
    async fn test_invoke_missing_plugin() {
        let mgr = CliPluginManager::new(default_config());
        let inv = PluginInvocation {
            plugin_name: "ghost".to_string(),
            args: vec![],
            stdin: None,
            env: HashMap::new(),
        };
        let err = mgr.invoke(inv).await.unwrap_err();
        assert!(err.contains("not found"));
    }

    #[tokio::test]
    async fn test_invoke_serialises_json() {
        let mgr = CliPluginManager::new(default_config());
        mgr.install(sample_manifest("json-echo")).await.unwrap();

        let inv = PluginInvocation {
            plugin_name: "json-echo".to_string(),
            args: vec!["a".to_string()],
            stdin: Some("data".to_string()),
            env: HashMap::new(),
        };

        let out = mgr.invoke(inv).await.unwrap();
        // The stub echoes the serialised invocation as stdout
        let roundtrip: PluginInvocation = serde_json::from_str(&out.stdout).unwrap();
        assert_eq!(roundtrip.plugin_name, "json-echo");
        assert_eq!(roundtrip.stdin, Some("data".to_string()));
    }

    #[tokio::test]
    async fn test_search_returns_empty() {
        let mgr = CliPluginManager::new(default_config());
        let results = mgr.search("anything").await;
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_update_missing_plugin() {
        let mgr = CliPluginManager::new(default_config());
        let err = mgr.update("nope").await.unwrap_err();
        assert!(err.contains("not found"));
    }

    #[tokio::test]
    async fn test_update_existing_plugin() {
        let mgr = CliPluginManager::new(default_config());
        mgr.install(sample_manifest("up")).await.unwrap();
        mgr.update("up").await.unwrap();
    }

    #[tokio::test]
    async fn test_validate_manifest_valid() {
        let mgr = CliPluginManager::new(default_config());
        let errors = mgr.validate_manifest(&sample_manifest("good"));
        assert!(errors.is_empty());
    }

    #[tokio::test]
    async fn test_validate_manifest_empty_name() {
        let mgr = CliPluginManager::new(default_config());
        let mut m = sample_manifest("");
        m.name = "".to_string();
        let errors = mgr.validate_manifest(&m);
        assert!(errors.iter().any(|e| e.contains("name")));
    }

    #[tokio::test]
    async fn test_validate_manifest_empty_version() {
        let mgr = CliPluginManager::new(default_config());
        let mut m = sample_manifest("v");
        m.version = "".to_string();
        let errors = mgr.validate_manifest(&m);
        assert!(errors.iter().any(|e| e.contains("version")));
    }

    #[tokio::test]
    async fn test_validate_manifest_invalid_name_chars() {
        let mgr = CliPluginManager::new(default_config());
        let mut m = sample_manifest("bad name!");
        m.name = "bad name!".to_string();
        let errors = mgr.validate_manifest(&m);
        assert!(errors.iter().any(|e| e.contains("alphanumeric")));
    }

    #[tokio::test]
    async fn test_validate_manifest_empty_entrypoint() {
        let mgr = CliPluginManager::new(default_config());
        let mut m = sample_manifest("ep");
        m.entrypoint = "".to_string();
        let errors = mgr.validate_manifest(&m);
        assert!(errors.iter().any(|e| e.contains("entrypoint")));
    }

    #[tokio::test]
    async fn test_install_rejects_invalid_manifest() {
        let mgr = CliPluginManager::new(default_config());
        let mut m = sample_manifest("bad");
        m.version = "".to_string();
        let err = mgr.install(m).await.unwrap_err();
        assert!(err.contains("Invalid manifest"));
    }

    #[tokio::test]
    async fn test_plugin_types_serde_roundtrip() {
        let types = vec![
            CliPluginType::Subcommand,
            CliPluginType::Formatter,
            CliPluginType::Hook {
                event: "pre-produce".to_string(),
            },
            CliPluginType::Workflow,
        ];
        for t in types {
            let json = serde_json::to_string(&t).unwrap();
            let back: CliPluginType = serde_json::from_str(&json).unwrap();
            assert_eq!(back, t);
        }
    }

    #[tokio::test]
    async fn test_installed_plugin_serde_roundtrip() {
        let p = InstalledPlugin {
            name: "test".to_string(),
            version: "0.1.0".to_string(),
            description: "desc".to_string(),
            author: "auth".to_string(),
            plugin_type: CliPluginType::Subcommand,
            entrypoint: "/bin/test".to_string(),
            installed_at: "2025-01-01T00:00:00Z".to_string(),
            last_used_at: None,
            use_count: 0,
        };
        let json = serde_json::to_string(&p).unwrap();
        let back: InstalledPlugin = serde_json::from_str(&json).unwrap();
        assert_eq!(back.name, "test");
    }
}
