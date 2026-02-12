//! Plugin registry for Kafka Connect compatibility
//!
//! Provides a registry of connector plugins (both built-in and discovered)
//! with descriptors containing name, version, type, and configuration schema.
//!
//! # Built-in Plugins
//!
//! - `FileStreamSource` — Reads lines from a file and produces messages
//! - `FileStreamSink` — Writes messages to a file
//! - `ConsoleSink` — Writes messages to stdout for debugging

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Connector plugin type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConnectorPluginType {
    /// Source connector — reads from external systems
    Source,
    /// Sink connector — writes to external systems
    Sink,
}

impl std::fmt::Display for ConnectorPluginType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectorPluginType::Source => write!(f, "source"),
            ConnectorPluginType::Sink => write!(f, "sink"),
        }
    }
}

/// A configuration parameter descriptor for a plugin
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigParam {
    /// Parameter name (e.g., "file", "topics")
    pub name: String,
    /// Human-readable description
    pub description: String,
    /// Whether this parameter is required
    pub required: bool,
    /// Default value if not required
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_value: Option<String>,
}

impl ConfigParam {
    /// Create a required config parameter
    pub fn required(name: impl Into<String>, description: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            required: true,
            default_value: None,
        }
    }

    /// Create an optional config parameter with a default
    pub fn optional(
        name: impl Into<String>,
        description: impl Into<String>,
        default: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            required: false,
            default_value: Some(default.into()),
        }
    }
}

/// Describes a connector plugin's identity, type, and configuration schema
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginDescriptor {
    /// Fully-qualified plugin class name (e.g., `io.streamline.connect.file.FileStreamSource`)
    pub class_name: String,
    /// Short human-readable name
    pub display_name: String,
    /// Plugin version (semver)
    pub version: String,
    /// Plugin type (source or sink)
    #[serde(rename = "type")]
    pub plugin_type: ConnectorPluginType,
    /// Description of what this plugin does
    pub description: String,
    /// Configuration schema — ordered list of parameters
    pub config_schema: Vec<ConfigParam>,
}

impl PluginDescriptor {
    /// Validate a config map against this plugin's schema.
    /// Returns a list of error messages (empty if valid).
    pub fn validate_config(&self, config: &HashMap<String, String>) -> Vec<String> {
        let mut errors = Vec::new();
        for param in &self.config_schema {
            if param.required && !config.contains_key(&param.name) {
                errors.push(format!("Missing required config: {}", param.name));
            }
        }
        errors
    }
}

/// Registry of available connector plugins
pub struct PluginRegistry {
    plugins: HashMap<String, PluginDescriptor>,
}

impl PluginRegistry {
    /// Create a new empty registry
    pub fn new() -> Self {
        Self {
            plugins: HashMap::new(),
        }
    }

    /// Create a registry pre-loaded with all built-in plugins
    pub fn with_builtins() -> Self {
        let mut registry = Self::new();
        registry.register_builtins();
        registry
    }

    /// Register a plugin descriptor
    pub fn register(&mut self, descriptor: PluginDescriptor) {
        self.plugins
            .insert(descriptor.class_name.clone(), descriptor);
    }

    /// Look up a plugin by class name
    pub fn get(&self, class_name: &str) -> Option<&PluginDescriptor> {
        self.plugins.get(class_name)
    }

    /// List all registered plugins
    pub fn list(&self) -> Vec<&PluginDescriptor> {
        self.plugins.values().collect()
    }

    /// List plugins filtered by type
    pub fn list_by_type(&self, plugin_type: ConnectorPluginType) -> Vec<&PluginDescriptor> {
        self.plugins
            .values()
            .filter(|p| p.plugin_type == plugin_type)
            .collect()
    }

    /// Number of registered plugins
    pub fn len(&self) -> usize {
        self.plugins.len()
    }

    /// Whether the registry is empty
    pub fn is_empty(&self) -> bool {
        self.plugins.is_empty()
    }

    /// Register all built-in plugins
    fn register_builtins(&mut self) {
        self.register(PluginDescriptor {
            class_name: "io.streamline.connect.file.FileStreamSource".to_string(),
            display_name: "File Source".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            plugin_type: ConnectorPluginType::Source,
            description: "Reads lines from a local file and produces each line as a message"
                .to_string(),
            config_schema: vec![
                ConfigParam::required("file", "Path to the input file"),
                ConfigParam::required("topic", "Target topic to produce messages to"),
                ConfigParam::optional("poll.interval.ms", "Polling interval in milliseconds", "1000"),
            ],
        });

        self.register(PluginDescriptor {
            class_name: "io.streamline.connect.file.FileStreamSink".to_string(),
            display_name: "File Sink".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            plugin_type: ConnectorPluginType::Sink,
            description: "Writes consumed messages to a local file".to_string(),
            config_schema: vec![
                ConfigParam::required("file", "Path to the output file"),
                ConfigParam::required("topics", "Comma-separated list of topics to consume from"),
            ],
        });

        self.register(PluginDescriptor {
            class_name: "io.streamline.connect.console.ConsoleSink".to_string(),
            display_name: "Console Sink".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            plugin_type: ConnectorPluginType::Sink,
            description: "Writes consumed messages to stdout for debugging".to_string(),
            config_schema: vec![
                ConfigParam::required("topics", "Comma-separated list of topics to consume from"),
                ConfigParam::optional("print.key", "Whether to print the message key", "false"),
                ConfigParam::optional(
                    "print.timestamp",
                    "Whether to print the message timestamp",
                    "false",
                ),
            ],
        });
    }
}

impl Default for PluginRegistry {
    fn default() -> Self {
        Self::with_builtins()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builtin_plugins_registered() {
        let registry = PluginRegistry::with_builtins();
        assert_eq!(registry.len(), 3);

        assert!(registry
            .get("io.streamline.connect.file.FileStreamSource")
            .is_some());
        assert!(registry
            .get("io.streamline.connect.file.FileStreamSink")
            .is_some());
        assert!(registry
            .get("io.streamline.connect.console.ConsoleSink")
            .is_some());
    }

    #[test]
    fn test_plugin_types() {
        let registry = PluginRegistry::with_builtins();

        let sources = registry.list_by_type(ConnectorPluginType::Source);
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].display_name, "File Source");

        let sinks = registry.list_by_type(ConnectorPluginType::Sink);
        assert_eq!(sinks.len(), 2);
    }

    #[test]
    fn test_validate_config_ok() {
        let registry = PluginRegistry::with_builtins();
        let descriptor = registry
            .get("io.streamline.connect.file.FileStreamSource")
            .unwrap();

        let mut config = HashMap::new();
        config.insert("file".to_string(), "/tmp/input.txt".to_string());
        config.insert("topic".to_string(), "my-topic".to_string());

        let errors = descriptor.validate_config(&config);
        assert!(errors.is_empty());
    }

    #[test]
    fn test_validate_config_missing_required() {
        let registry = PluginRegistry::with_builtins();
        let descriptor = registry
            .get("io.streamline.connect.file.FileStreamSource")
            .unwrap();

        let config = HashMap::new();
        let errors = descriptor.validate_config(&config);
        assert_eq!(errors.len(), 2); // missing file and topic
    }

    #[test]
    fn test_register_custom_plugin() {
        let mut registry = PluginRegistry::with_builtins();

        registry.register(PluginDescriptor {
            class_name: "com.example.CustomSource".to_string(),
            display_name: "Custom Source".to_string(),
            version: "0.1.0".to_string(),
            plugin_type: ConnectorPluginType::Source,
            description: "A custom source connector".to_string(),
            config_schema: vec![ConfigParam::required("endpoint", "API endpoint URL")],
        });

        assert_eq!(registry.len(), 4);
        assert!(registry.get("com.example.CustomSource").is_some());
    }

    #[test]
    fn test_empty_registry() {
        let registry = PluginRegistry::new();
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);
        assert!(registry.list().is_empty());
    }

    #[test]
    fn test_plugin_descriptor_display() {
        assert_eq!(format!("{}", ConnectorPluginType::Source), "source");
        assert_eq!(format!("{}", ConnectorPluginType::Sink), "sink");
    }
}
