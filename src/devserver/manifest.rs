//! Dev Server Manifest (streamline.yaml) parsing and validation
//!
//! Defines the YAML manifest format for configuring the dev server:
//! topics, mock data generators, settings, and seeds.

use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Root manifest structure for `streamline.yaml`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DevManifest {
    /// Manifest format version
    #[serde(default = "default_version")]
    pub version: String,

    /// Optional name for this dev environment
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    /// Optional description
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Topics to create
    #[serde(default)]
    pub topics: Vec<TopicManifest>,

    /// Server settings overrides
    #[serde(default)]
    pub settings: ManifestSettings,
}

fn default_version() -> String {
    "1".to_string()
}

/// Topic definition in the manifest
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicManifest {
    /// Topic name
    pub name: String,

    /// Number of partitions (default: 1)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partitions: Option<i32>,

    /// Human-readable description
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Mock data generator configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mock_data: Option<MockDataConfig>,

    /// Retention period in milliseconds
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retention_ms: Option<u64>,

    /// Additional topic configuration
    #[serde(default)]
    pub config: HashMap<String, String>,
}

/// Mock data generator configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MockDataConfig {
    /// Message template with variable substitution
    pub template: String,

    /// Optional key template
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_template: Option<String>,

    /// Number of messages to generate (default: 10)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub count: Option<usize>,

    /// Interval between messages in ms (for continuous generation)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub interval_ms: Option<u64>,
}

/// Server settings in the manifest
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ManifestSettings {
    /// In-memory mode (default: true for dev)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_memory: Option<bool>,

    /// Data directory override
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_dir: Option<String>,

    /// Kafka port override
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kafka_port: Option<u16>,

    /// HTTP port override
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http_port: Option<u16>,

    /// Log level
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_level: Option<String>,
}

impl DevManifest {
    /// Parse a manifest from YAML string
    pub fn from_yaml(yaml: &str) -> Result<Self> {
        let manifest: Self = serde_yaml::from_str(yaml)
            .map_err(|e| StreamlineError::Config(format!("Invalid manifest YAML: {}", e)))?;
        manifest.validate()?;
        Ok(manifest)
    }

    /// Validate the manifest
    pub fn validate(&self) -> Result<()> {
        // Check version
        if self.version != "1" {
            return Err(StreamlineError::Config(format!(
                "Unsupported manifest version: {}. Supported: 1",
                self.version
            )));
        }

        // Validate topic names
        let mut seen_names = std::collections::HashSet::new();
        for topic in &self.topics {
            if topic.name.is_empty() {
                return Err(StreamlineError::Config("Topic name cannot be empty".into()));
            }

            if topic.name.len() > 255 {
                return Err(StreamlineError::Config(format!(
                    "Topic name '{}' exceeds 255 characters",
                    topic.name
                )));
            }

            // Check for valid characters
            if !topic
                .name
                .chars()
                .all(|c| c.is_alphanumeric() || c == '.' || c == '-' || c == '_')
            {
                return Err(StreamlineError::Config(format!(
                    "Topic name '{}' contains invalid characters. Use alphanumeric, '.', '-', '_'",
                    topic.name
                )));
            }

            if !seen_names.insert(&topic.name) {
                return Err(StreamlineError::Config(format!(
                    "Duplicate topic name: '{}'",
                    topic.name
                )));
            }

            // Validate partition count
            if let Some(partitions) = topic.partitions {
                if !(1..=1024).contains(&partitions) {
                    return Err(StreamlineError::Config(format!(
                        "Topic '{}': partitions must be between 1 and 1024, got {}",
                        topic.name, partitions
                    )));
                }
            }

            // Validate mock data config
            if let Some(ref mock) = topic.mock_data {
                if mock.template.is_empty() {
                    return Err(StreamlineError::Config(format!(
                        "Topic '{}': mock_data template cannot be empty",
                        topic.name
                    )));
                }
                if let Some(count) = mock.count {
                    if count > 1_000_000 {
                        return Err(StreamlineError::Config(format!(
                            "Topic '{}': mock_data count {} exceeds maximum of 1,000,000",
                            topic.name, count
                        )));
                    }
                }
            }
        }

        Ok(())
    }

    /// Generate example manifest YAML
    pub fn example_yaml() -> String {
        r#"# Streamline Dev Server Manifest
# Place this file as streamline.yaml in your project root
version: "1"
name: my-app
description: Development environment for my streaming application

topics:
  - name: events
    partitions: 3
    description: User event stream
    mock_data:
      template: '{"event_id":"{{uuid}}","type":"{{random_choice:click,view,purchase}}","timestamp":{{timestamp}}}'
      key_template: "user-{{random:1:100}}"
      count: 50

  - name: orders
    partitions: 3
    description: Order processing stream
    mock_data:
      template: '{"order_id":"{{uuid}}","amount":{{random:10:500}},"status":"{{random_choice:pending,confirmed,shipped}}"}'
      count: 20

  - name: notifications
    partitions: 1
    description: System notifications
    retention_ms: 86400000  # 24 hours

settings:
  in_memory: true
  kafka_port: 9092
  http_port: 9094
  log_level: info
"#
        .to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_valid_manifest() {
        let yaml = r#"
version: "1"
name: test
topics:
  - name: events
    partitions: 3
    mock_data:
      template: '{"id":"{{uuid}}"}'
      count: 10
  - name: logs
    partitions: 1
"#;
        let manifest = DevManifest::from_yaml(yaml).unwrap();
        assert_eq!(manifest.topics.len(), 2);
        assert_eq!(manifest.topics[0].name, "events");
        assert_eq!(manifest.topics[0].partitions, Some(3));
        assert!(manifest.topics[0].mock_data.is_some());
    }

    #[test]
    fn test_parse_minimal_manifest() {
        let yaml = r#"
topics:
  - name: test
"#;
        let manifest = DevManifest::from_yaml(yaml).unwrap();
        assert_eq!(manifest.topics.len(), 1);
        assert_eq!(manifest.version, "1");
    }

    #[test]
    fn test_validate_duplicate_topics() {
        let yaml = r#"
version: "1"
topics:
  - name: events
  - name: events
"#;
        let result = DevManifest::from_yaml(yaml);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Duplicate"));
    }

    #[test]
    fn test_validate_invalid_topic_name() {
        let yaml = r#"
version: "1"
topics:
  - name: "invalid name with spaces"
"#;
        let result = DevManifest::from_yaml(yaml);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("invalid characters"));
    }

    #[test]
    fn test_validate_empty_topic_name() {
        let yaml = r#"
version: "1"
topics:
  - name: ""
"#;
        let result = DevManifest::from_yaml(yaml);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_invalid_partitions() {
        let yaml = r#"
version: "1"
topics:
  - name: test
    partitions: 0
"#;
        let result = DevManifest::from_yaml(yaml);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_unsupported_version() {
        let yaml = r#"
version: "99"
topics:
  - name: test
"#;
        let result = DevManifest::from_yaml(yaml);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Unsupported manifest version"));
    }

    #[test]
    fn test_example_yaml_is_valid() {
        let yaml = DevManifest::example_yaml();
        let manifest = DevManifest::from_yaml(&yaml).unwrap();
        assert!(!manifest.topics.is_empty());
    }

    #[test]
    fn test_manifest_settings_defaults() {
        let settings = ManifestSettings::default();
        assert!(settings.in_memory.is_none());
        assert!(settings.data_dir.is_none());
    }
}
