//! Kafka Connect compatibility module
//!
//! This module provides compatibility with Kafka Connect workers by:
//! - Auto-creating internal topics (connect-configs, connect-offsets, connect-status)
//! - Providing proper topic configuration for Connect internal topics
//! - Supporting Connect-specific API requirements
//! - **Kafka Connect REST API** for managing connectors, tasks, and plugins
//!
//! ## Internal Topics
//!
//! Kafka Connect requires the following internal topics:
//! - `connect-configs`: Stores connector and task configurations (compacted)
//! - `connect-offsets`: Stores source connector offsets (compacted)
//! - `connect-status`: Stores connector and task status (compacted)
//!
//! ## REST API
//!
//! The module provides a full Kafka Connect REST API:
//! - `GET /connectors` - List all connectors
//! - `POST /connectors` - Create a new connector
//! - `GET /connectors/{name}` - Get connector info
//! - `PUT /connectors/{name}/config` - Update connector config
//! - `GET /connectors/{name}/status` - Get connector status
//! - `POST /connectors/{name}/restart` - Restart connector
//! - `PUT /connectors/{name}/pause` - Pause connector
//! - `PUT /connectors/{name}/resume` - Resume connector
//! - `DELETE /connectors/{name}` - Delete connector (with offset/DLQ cleanup)
//! - `GET /connectors/{name}/offsets` - Get source connector offsets
//! - `DELETE /connectors/{name}/offsets` - Reset connector offsets
//! - `GET /connectors/{name}/dlq` - Get dead letter queue entries
//! - `GET /connector-plugins` - List available plugins
//!
//! ## Configuration
//!
//! Connect internal topics can be prefixed to support multiple Connect clusters:
//! ```text
//! # In Kafka Connect worker config:
//! config.storage.topic=my-connect-configs
//! offset.storage.topic=my-connect-offsets
//! status.storage.topic=my-connect-status
//! ```

pub mod api;
pub mod conformance;
pub mod health;
pub mod marketplace;
pub mod native;
pub mod plugins;
pub mod runtime;
pub mod transforms;

use crate::storage::{TopicConfig, TopicManager};
use crate::StreamlineError;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{debug, info};

// Re-export SMT types
pub use transforms::SingleMessageTransform;

/// Default topic names for Kafka Connect
pub const DEFAULT_CONFIG_TOPIC: &str = "connect-configs";
pub const DEFAULT_OFFSET_TOPIC: &str = "connect-offsets";
pub const DEFAULT_STATUS_TOPIC: &str = "connect-status";

/// Default number of partitions for each internal topic
pub const DEFAULT_CONFIG_PARTITIONS: i32 = 1;
pub const DEFAULT_OFFSET_PARTITIONS: i32 = 25;
pub const DEFAULT_STATUS_PARTITIONS: i32 = 5;

/// Configuration for Kafka Connect compatibility
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectConfig {
    /// Enable Kafka Connect compatibility features
    pub enabled: bool,
    /// Auto-create internal topics on startup
    pub auto_create_topics: bool,
    /// Config storage topic name
    pub config_topic: String,
    /// Offset storage topic name
    pub offset_topic: String,
    /// Status storage topic name
    pub status_topic: String,
    /// Number of partitions for config topic (should be 1)
    pub config_partitions: i32,
    /// Number of partitions for offset topic
    pub offset_partitions: i32,
    /// Number of partitions for status topic
    pub status_partitions: i32,
    /// Replication factor for internal topics
    pub replication_factor: i16,
}

impl Default for ConnectConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            auto_create_topics: true,
            config_topic: DEFAULT_CONFIG_TOPIC.to_string(),
            offset_topic: DEFAULT_OFFSET_TOPIC.to_string(),
            status_topic: DEFAULT_STATUS_TOPIC.to_string(),
            config_partitions: DEFAULT_CONFIG_PARTITIONS,
            offset_partitions: DEFAULT_OFFSET_PARTITIONS,
            status_partitions: DEFAULT_STATUS_PARTITIONS,
            replication_factor: 1,
        }
    }
}

/// Connect topic type for determining configuration
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectTopicType {
    /// Configuration storage topic (must be compacted, single partition)
    Config,
    /// Offset storage topic (must be compacted)
    Offset,
    /// Status storage topic (must be compacted)
    Status,
}

impl ConnectTopicType {
    /// Get the cleanup policy for this topic type
    pub fn cleanup_policy(&self) -> crate::storage::topic::CleanupPolicy {
        // All Connect internal topics use compaction
        crate::storage::topic::CleanupPolicy::Compact
    }

    /// Get the default partitions for this topic type
    pub fn default_partitions(&self) -> i32 {
        match self {
            ConnectTopicType::Config => DEFAULT_CONFIG_PARTITIONS,
            ConnectTopicType::Offset => DEFAULT_OFFSET_PARTITIONS,
            ConnectTopicType::Status => DEFAULT_STATUS_PARTITIONS,
        }
    }
}

/// Internal topic specification
#[derive(Debug, Clone)]
pub struct ConnectInternalTopic {
    /// Topic name
    pub name: String,
    /// Topic type
    pub topic_type: ConnectTopicType,
    /// Number of partitions
    pub partitions: i32,
    /// Replication factor
    pub replication_factor: i16,
}

impl ConnectInternalTopic {
    /// Create a new internal topic specification
    pub fn new(
        name: &str,
        topic_type: ConnectTopicType,
        partitions: i32,
        replication_factor: i16,
    ) -> Self {
        Self {
            name: name.to_string(),
            topic_type,
            partitions,
            replication_factor,
        }
    }

    /// Get the topic configuration
    pub fn config(&self) -> TopicConfig {
        TopicConfig {
            cleanup_policy: crate::storage::topic::CleanupPolicy::Compact,
            // For compacted topics, we want infinite retention
            retention_ms: -1,
            // Segment size for compaction efficiency
            segment_bytes: 100 * 1024 * 1024, // 100 MB
            ..Default::default()
        }
    }
}

/// Manager for Kafka Connect internal topics
pub struct ConnectManager {
    /// Configuration
    config: ConnectConfig,
    /// Topic manager reference
    topic_manager: Arc<TopicManager>,
}

impl std::fmt::Debug for ConnectManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectManager")
            .field("config", &self.config)
            .field("topic_manager", &"<TopicManager>")
            .finish()
    }
}

impl ConnectManager {
    /// Create a new Connect manager
    pub fn new(config: ConnectConfig, topic_manager: Arc<TopicManager>) -> Self {
        Self {
            config,
            topic_manager,
        }
    }

    /// Initialize Connect internal topics
    pub fn initialize(&self) -> Result<(), StreamlineError> {
        if !self.config.enabled {
            debug!("Kafka Connect compatibility disabled");
            return Ok(());
        }

        if !self.config.auto_create_topics {
            debug!("Connect internal topic auto-creation disabled");
            return Ok(());
        }

        info!("Initializing Kafka Connect internal topics");

        let topics = self.get_internal_topics();
        for topic in topics {
            self.ensure_topic_exists(&topic)?;
        }

        Ok(())
    }

    /// Get the list of internal topics
    pub fn get_internal_topics(&self) -> Vec<ConnectInternalTopic> {
        vec![
            ConnectInternalTopic::new(
                &self.config.config_topic,
                ConnectTopicType::Config,
                self.config.config_partitions,
                self.config.replication_factor,
            ),
            ConnectInternalTopic::new(
                &self.config.offset_topic,
                ConnectTopicType::Offset,
                self.config.offset_partitions,
                self.config.replication_factor,
            ),
            ConnectInternalTopic::new(
                &self.config.status_topic,
                ConnectTopicType::Status,
                self.config.status_partitions,
                self.config.replication_factor,
            ),
        ]
    }

    /// Ensure a topic exists with proper configuration
    fn ensure_topic_exists(&self, topic: &ConnectInternalTopic) -> Result<(), StreamlineError> {
        // Check if topic already exists
        match self.topic_manager.get_topic_metadata(&topic.name) {
            Ok(_) => {
                debug!(topic = %topic.name, "Connect internal topic already exists");
                Ok(())
            }
            Err(_) => {
                info!(
                    topic = %topic.name,
                    partitions = topic.partitions,
                    cleanup_policy = %topic.topic_type.cleanup_policy(),
                    "Creating Connect internal topic"
                );
                self.topic_manager.create_topic_with_config(
                    &topic.name,
                    topic.partitions,
                    topic.config(),
                )
            }
        }
    }

    /// Check if a topic is a Connect internal topic
    pub fn is_connect_topic(&self, topic_name: &str) -> bool {
        topic_name == self.config.config_topic
            || topic_name == self.config.offset_topic
            || topic_name == self.config.status_topic
    }

    /// Get the topic type for a Connect internal topic
    pub fn get_topic_type(&self, topic_name: &str) -> Option<ConnectTopicType> {
        if topic_name == self.config.config_topic {
            Some(ConnectTopicType::Config)
        } else if topic_name == self.config.offset_topic {
            Some(ConnectTopicType::Offset)
        } else if topic_name == self.config.status_topic {
            Some(ConnectTopicType::Status)
        } else {
            None
        }
    }
}

/// Connector state for status tracking
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum ConnectorState {
    /// Connector is unassigned
    Unassigned,
    /// Connector is running
    Running,
    /// Connector is paused
    Paused,
    /// Connector has failed
    Failed,
    /// Connector is being restarted
    Restarting,
}

impl std::fmt::Display for ConnectorState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectorState::Unassigned => write!(f, "UNASSIGNED"),
            ConnectorState::Running => write!(f, "RUNNING"),
            ConnectorState::Paused => write!(f, "PAUSED"),
            ConnectorState::Failed => write!(f, "FAILED"),
            ConnectorState::Restarting => write!(f, "RESTARTING"),
        }
    }
}

/// Task state for status tracking
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum TaskState {
    /// Task is unassigned
    Unassigned,
    /// Task is running
    Running,
    /// Task is paused
    Paused,
    /// Task has failed
    Failed,
}

impl std::fmt::Display for TaskState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskState::Unassigned => write!(f, "UNASSIGNED"),
            TaskState::Running => write!(f, "RUNNING"),
            TaskState::Paused => write!(f, "PAUSED"),
            TaskState::Failed => write!(f, "FAILED"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_manager() -> (ConnectManager, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());
        let config = ConnectConfig::default();
        let manager = ConnectManager::new(config, topic_manager);
        (manager, temp_dir)
    }

    #[test]
    fn test_connect_config_default() {
        let config = ConnectConfig::default();
        assert!(config.enabled);
        assert!(config.auto_create_topics);
        assert_eq!(config.config_topic, "connect-configs");
        assert_eq!(config.offset_topic, "connect-offsets");
        assert_eq!(config.status_topic, "connect-status");
        assert_eq!(config.config_partitions, 1);
        assert_eq!(config.offset_partitions, 25);
        assert_eq!(config.status_partitions, 5);
    }

    #[test]
    fn test_connect_topic_type() {
        assert_eq!(
            ConnectTopicType::Config.cleanup_policy(),
            crate::storage::topic::CleanupPolicy::Compact
        );
        assert_eq!(
            ConnectTopicType::Offset.cleanup_policy(),
            crate::storage::topic::CleanupPolicy::Compact
        );
        assert_eq!(
            ConnectTopicType::Status.cleanup_policy(),
            crate::storage::topic::CleanupPolicy::Compact
        );

        assert_eq!(ConnectTopicType::Config.default_partitions(), 1);
        assert_eq!(ConnectTopicType::Offset.default_partitions(), 25);
        assert_eq!(ConnectTopicType::Status.default_partitions(), 5);
    }

    #[test]
    fn test_connect_internal_topic() {
        let topic = ConnectInternalTopic::new("test-configs", ConnectTopicType::Config, 1, 3);
        assert_eq!(topic.name, "test-configs");
        assert_eq!(topic.partitions, 1);
        assert_eq!(topic.replication_factor, 3);

        let config = topic.config();
        assert_eq!(
            config.cleanup_policy,
            crate::storage::topic::CleanupPolicy::Compact
        );
        assert_eq!(config.retention_ms, -1);
    }

    #[test]
    fn test_connect_manager_creation() {
        let (manager, _temp_dir) = create_test_manager();
        assert!(manager.config.enabled);
    }

    #[test]
    fn test_connect_manager_initialize() {
        let (manager, _temp_dir) = create_test_manager();
        manager.initialize().unwrap();

        // Check that topics were created
        assert!(manager
            .topic_manager
            .get_topic_metadata("connect-configs")
            .is_ok());
        assert!(manager
            .topic_manager
            .get_topic_metadata("connect-offsets")
            .is_ok());
        assert!(manager
            .topic_manager
            .get_topic_metadata("connect-status")
            .is_ok());
    }

    #[test]
    fn test_is_connect_topic() {
        let (manager, _temp_dir) = create_test_manager();

        assert!(manager.is_connect_topic("connect-configs"));
        assert!(manager.is_connect_topic("connect-offsets"));
        assert!(manager.is_connect_topic("connect-status"));
        assert!(!manager.is_connect_topic("my-topic"));
    }

    #[test]
    fn test_get_topic_type() {
        let (manager, _temp_dir) = create_test_manager();

        assert_eq!(
            manager.get_topic_type("connect-configs"),
            Some(ConnectTopicType::Config)
        );
        assert_eq!(
            manager.get_topic_type("connect-offsets"),
            Some(ConnectTopicType::Offset)
        );
        assert_eq!(
            manager.get_topic_type("connect-status"),
            Some(ConnectTopicType::Status)
        );
        assert_eq!(manager.get_topic_type("my-topic"), None);
    }

    #[test]
    fn test_connector_state_display() {
        assert_eq!(format!("{}", ConnectorState::Running), "RUNNING");
        assert_eq!(format!("{}", ConnectorState::Failed), "FAILED");
        assert_eq!(format!("{}", ConnectorState::Paused), "PAUSED");
    }

    #[test]
    fn test_task_state_display() {
        assert_eq!(format!("{}", TaskState::Running), "RUNNING");
        assert_eq!(format!("{}", TaskState::Failed), "FAILED");
        assert_eq!(format!("{}", TaskState::Unassigned), "UNASSIGNED");
    }

    #[test]
    fn test_connect_disabled() {
        let temp_dir = TempDir::new().unwrap();
        let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());
        let config = ConnectConfig {
            enabled: false,
            ..Default::default()
        };
        let manager = ConnectManager::new(config, topic_manager);

        // Should succeed without creating topics
        manager.initialize().unwrap();

        // Topics should not exist
        assert!(manager
            .topic_manager
            .get_topic_metadata("connect-configs")
            .is_err());
    }

    #[test]
    fn test_custom_topic_names() {
        let temp_dir = TempDir::new().unwrap();
        let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());
        let config = ConnectConfig {
            config_topic: "my-connect-configs".to_string(),
            offset_topic: "my-connect-offsets".to_string(),
            status_topic: "my-connect-status".to_string(),
            ..Default::default()
        };
        let manager = ConnectManager::new(config, topic_manager);

        manager.initialize().unwrap();

        // Custom named topics should exist
        assert!(manager
            .topic_manager
            .get_topic_metadata("my-connect-configs")
            .is_ok());
        assert!(manager
            .topic_manager
            .get_topic_metadata("my-connect-offsets")
            .is_ok());
        assert!(manager
            .topic_manager
            .get_topic_metadata("my-connect-status")
            .is_ok());

        // Check is_connect_topic with custom names
        assert!(manager.is_connect_topic("my-connect-configs"));
        assert!(!manager.is_connect_topic("connect-configs"));
    }
}
