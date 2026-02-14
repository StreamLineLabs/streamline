//! Embedded Dev Server Mode (`streamline dev`)
//!
//! Single-command development server that starts Streamline with:
//! - Auto-topic creation from a `streamline.yaml` manifest
//! - Mock data generation with templates
//! - Hot-reload on manifest changes
//! - Colorful developer-friendly output
//!
//! ## Stability: Experimental
//!
//! ## Usage
//!
//! ```bash
//! # Start with defaults (looks for streamline.yaml in current directory)
//! streamline dev
//!
//! # Start with custom manifest
//! streamline dev --manifest my-config.yaml
//!
//! # Start in playground mode (creates sample topics with mock data)
//! streamline dev --playground
//! ```

pub mod manifest;
pub mod mockdata;
pub mod watcher;

use crate::config::ServerConfig;
use crate::error::{Result, StreamlineError};
use crate::storage::TopicManager;
use manifest::{DevManifest, TopicManifest};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::watch;

// ─── Dev Server Configuration ───────────────────────────────────────

/// Configuration for the dev server mode
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DevServerConfig {
    /// Path to the manifest file (default: streamline.yaml)
    pub manifest_path: PathBuf,
    /// Enable playground mode with sample data
    pub playground: bool,
    /// Enable hot-reload of manifest changes
    pub hot_reload: bool,
    /// Port for the Kafka protocol server
    pub kafka_port: u16,
    /// Port for the HTTP API server
    pub http_port: u16,
    /// Enable in-memory mode (no disk persistence)
    pub in_memory: bool,
    /// Enable verbose logging
    pub verbose: bool,
}

impl Default for DevServerConfig {
    fn default() -> Self {
        Self {
            manifest_path: PathBuf::from("streamline.yaml"),
            playground: false,
            hot_reload: true,
            kafka_port: 9092,
            http_port: 9094,
            in_memory: true,
            verbose: false,
        }
    }
}

// ─── Dev Server ─────────────────────────────────────────────────────

/// The dev server orchestrator
pub struct DevServer {
    config: DevServerConfig,
    manifest: Option<DevManifest>,
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
}

impl DevServer {
    pub fn new(config: DevServerConfig) -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        Self {
            config,
            manifest: None,
            shutdown_tx,
            shutdown_rx,
        }
    }

    /// Load and validate the manifest file
    pub fn load_manifest(&mut self) -> Result<()> {
        if self.config.manifest_path.exists() {
            let content = std::fs::read_to_string(&self.config.manifest_path)
                .map_err(|e| StreamlineError::Config(format!("Failed to read manifest: {}", e)))?;
            let manifest: DevManifest = serde_yaml::from_str(&content)
                .map_err(|e| StreamlineError::Config(format!("Invalid manifest YAML: {}", e)))?;
            manifest.validate()?;
            self.manifest = Some(manifest);
        } else if self.config.playground {
            self.manifest = Some(DevManifest::playground());
        }
        Ok(())
    }

    /// Apply manifest to create topics and configure the server
    pub fn apply_manifest(&self, topic_manager: &Arc<TopicManager>) -> Result<ApplyResult> {
        let manifest = match &self.manifest {
            Some(m) => m,
            None => return Ok(ApplyResult::default()),
        };

        let mut result = ApplyResult::default();

        for topic in &manifest.topics {
            match topic_manager.create_topic(&topic.name, topic.partitions.unwrap_or(1)) {
                Ok(_) => {
                    result.topics_created.push(topic.name.clone());
                }
                Err(StreamlineError::TopicAlreadyExists(_)) => {
                    result.topics_existed.push(topic.name.clone());
                }
                Err(e) => {
                    result
                        .errors
                        .push(format!("Failed to create topic '{}': {}", topic.name, e));
                }
            }
        }

        result.total_topics = manifest.topics.len();
        Ok(result)
    }

    /// Generate mock data for topics that have generators configured
    pub async fn generate_mock_data(
        &self,
        topic_manager: &Arc<TopicManager>,
    ) -> Result<MockDataResult> {
        let manifest = match &self.manifest {
            Some(m) => m,
            None => return Ok(MockDataResult::default()),
        };

        let mut result = MockDataResult::default();

        for topic in &manifest.topics {
            if let Some(ref generator) = topic.mock_data {
                let count = generator.count.unwrap_or(10);
                let mut generated = 0;

                for i in 0..count {
                    let message = mockdata::generate_message(&generator.template, i);
                    let key = generator
                        .key_template
                        .as_ref()
                        .map(|kt| mockdata::generate_message(kt, i));

                    let key_bytes = key.map(bytes::Bytes::from);
                    let value_bytes = bytes::Bytes::from(message);

                    let partition = (i as i32) % topic.partitions.unwrap_or(1);
                    match topic_manager.append(&topic.name, partition, key_bytes, value_bytes) {
                        Ok(_) => generated += 1,
                        Err(e) => {
                            result.errors.push(format!(
                                "Failed to generate message for '{}': {}",
                                topic.name, e
                            ));
                            break;
                        }
                    }
                }

                result
                    .topics_populated
                    .push((topic.name.clone(), generated));
            }
        }

        Ok(result)
    }

    /// Build a ServerConfig from the dev server config
    pub fn build_server_config(&self) -> ServerConfig {
        let mut config = ServerConfig::default();
        config.listen_addr = format!("0.0.0.0:{}", self.config.kafka_port)
            .parse()
            .unwrap_or(std::net::SocketAddr::from(([0, 0, 0, 0], 9092)));
        config.http_addr = format!("0.0.0.0:{}", self.config.http_port)
            .parse()
            .unwrap_or(std::net::SocketAddr::from(([0, 0, 0, 0], 9094)));
        config.storage.in_memory = self.config.in_memory;
        config
    }

    /// Get the shutdown receiver for graceful shutdown
    pub fn shutdown_rx(&self) -> watch::Receiver<bool> {
        self.shutdown_rx.clone()
    }

    /// Signal shutdown
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }

    pub fn config(&self) -> &DevServerConfig {
        &self.config
    }

    pub fn manifest(&self) -> Option<&DevManifest> {
        self.manifest.as_ref()
    }
}

// ─── Result Types ───────────────────────────────────────────────────

/// Result of applying a manifest
#[derive(Debug, Default)]
pub struct ApplyResult {
    pub total_topics: usize,
    pub topics_created: Vec<String>,
    pub topics_existed: Vec<String>,
    pub errors: Vec<String>,
}

/// Result of generating mock data
#[derive(Debug, Default)]
pub struct MockDataResult {
    pub topics_populated: Vec<(String, usize)>,
    pub errors: Vec<String>,
}

// ─── Playground Mode ────────────────────────────────────────────────

impl DevManifest {
    /// Create a playground manifest with sample topics and mock data
    pub fn playground() -> Self {
        Self {
            version: "1".to_string(),
            name: Some("playground".to_string()),
            description: Some("Streamline playground with sample topics and data".to_string()),
            topics: vec![
                TopicManifest {
                    name: "events".to_string(),
                    partitions: Some(3),
                    description: Some("General event stream".to_string()),
                    mock_data: Some(manifest::MockDataConfig {
                        template: r#"{"event_id":"{{uuid}}","type":"{{random_choice:click,view,purchase,signup}}","user_id":"user-{{random:1:1000}}","timestamp":{{timestamp}}}"#.to_string(),
                        key_template: Some("user-{{random:1:1000}}".to_string()),
                        count: Some(100),
                        interval_ms: None,
                    }),
                    retention_ms: None,
                    config: Default::default(),
                },
                TopicManifest {
                    name: "orders".to_string(),
                    partitions: Some(3),
                    description: Some("Order events".to_string()),
                    mock_data: Some(manifest::MockDataConfig {
                        template: r#"{"order_id":"{{uuid}}","product":"{{random_choice:widget,gadget,doohickey,thingamajig}}","quantity":{{random:1:10}},"price":{{random:10:500}},"customer_id":"cust-{{random:1:100}}"}"#.to_string(),
                        key_template: Some("cust-{{random:1:100}}".to_string()),
                        count: Some(50),
                        interval_ms: None,
                    }),
                    retention_ms: None,
                    config: Default::default(),
                },
                TopicManifest {
                    name: "logs".to_string(),
                    partitions: Some(1),
                    description: Some("Application logs".to_string()),
                    mock_data: Some(manifest::MockDataConfig {
                        template: r#"{"level":"{{random_choice:INFO,WARN,ERROR,DEBUG}}","service":"{{random_choice:api,worker,scheduler,auth}}","message":"{{random_choice:Request processed,Connection timeout,Cache miss,Rate limit hit,Health check passed}}","timestamp":{{timestamp}}}"#.to_string(),
                        key_template: None,
                        count: Some(200),
                        interval_ms: None,
                    }),
                    retention_ms: None,
                    config: Default::default(),
                },
                TopicManifest {
                    name: "metrics".to_string(),
                    partitions: Some(1),
                    description: Some("System metrics".to_string()),
                    mock_data: Some(manifest::MockDataConfig {
                        template: r#"{"metric":"{{random_choice:cpu_usage,memory_usage,disk_io,network_rx,network_tx}}","value":{{random:0:100}},"host":"host-{{random:1:5}}","timestamp":{{timestamp}}}"#.to_string(),
                        key_template: Some("host-{{random:1:5}}".to_string()),
                        count: Some(100),
                        interval_ms: None,
                    }),
                    retention_ms: None,
                    config: Default::default(),
                },
            ],
            settings: Default::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dev_server_config_default() {
        let config = DevServerConfig::default();
        assert_eq!(config.kafka_port, 9092);
        assert_eq!(config.http_port, 9094);
        assert!(config.hot_reload);
        assert!(config.in_memory);
        assert!(!config.playground);
    }

    #[test]
    fn test_dev_server_new() {
        let config = DevServerConfig::default();
        let server = DevServer::new(config);
        assert!(server.manifest().is_none());
    }

    #[test]
    fn test_playground_manifest() {
        let manifest = DevManifest::playground();
        assert_eq!(manifest.topics.len(), 4);
        assert!(manifest.topics.iter().any(|t| t.name == "events"));
        assert!(manifest.topics.iter().any(|t| t.name == "orders"));
        assert!(manifest.topics.iter().any(|t| t.name == "logs"));
        assert!(manifest.topics.iter().any(|t| t.name == "metrics"));

        // All playground topics should have mock data
        for topic in &manifest.topics {
            assert!(
                topic.mock_data.is_some(),
                "Topic {} should have mock data",
                topic.name
            );
        }
    }

    #[test]
    fn test_apply_result_default() {
        let result = ApplyResult::default();
        assert_eq!(result.total_topics, 0);
        assert!(result.topics_created.is_empty());
        assert!(result.errors.is_empty());
    }
}
