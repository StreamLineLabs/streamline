//! Playground instance and scenario management.

use crate::embedded::{EmbeddedConfig, EmbeddedStreamline};
use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Instant;

/// Playground configuration.
#[derive(Debug, Clone)]
pub struct PlaygroundConfig {
    /// Maximum concurrent instances
    pub max_instances: usize,
    /// Instance TTL in seconds
    pub instance_ttl_secs: u64,
    /// Maximum topics per instance
    pub max_topics: usize,
    /// Maximum records per topic
    pub max_records_per_topic: u64,
    /// Whether to pre-populate with demo data
    pub demo_data: bool,
}

impl Default for PlaygroundConfig {
    fn default() -> Self {
        Self {
            max_instances: 100,
            instance_ttl_secs: 1800, // 30 minutes
            max_topics: 20,
            max_records_per_topic: 100_000,
            demo_data: true,
        }
    }
}

/// A playground instance.
pub struct PlaygroundInstance {
    /// Unique instance ID
    pub id: String,
    /// The embedded Streamline instance
    pub streamline: EmbeddedStreamline,
    /// Creation time
    pub created_at: Instant,
    /// Instance metadata
    pub metadata: InstanceMetadata,
}

/// Instance metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstanceMetadata {
    pub id: String,
    pub name: String,
    pub created_at_ms: i64,
    pub topics: Vec<String>,
    pub total_records: u64,
}

/// Playground statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlaygroundStats {
    pub active_instances: usize,
    pub total_created: u64,
    pub total_expired: u64,
}

/// Manages playground instances.
pub struct PlaygroundManager {
    config: PlaygroundConfig,
    instances: HashMap<String, PlaygroundInstance>,
    total_created: u64,
    total_expired: u64,
}

impl PlaygroundManager {
    /// Create a new playground manager.
    pub fn new(config: PlaygroundConfig) -> Self {
        Self {
            config,
            instances: HashMap::new(),
            total_created: 0,
            total_expired: 0,
        }
    }

    /// Create a new playground instance.
    pub fn create_instance(&mut self, name: impl Into<String>) -> Result<String> {
        // Garbage collect expired instances
        self.cleanup_expired();

        if self.instances.len() >= self.config.max_instances {
            return Err(crate::error::StreamlineError::ResourceExhausted(format!(
                "Maximum {} playground instances reached",
                self.config.max_instances
            )));
        }

        let id = uuid::Uuid::new_v4().to_string();
        let streamline = EmbeddedStreamline::new(EmbeddedConfig::in_memory())?;

        if self.config.demo_data {
            Self::populate_demo_data(&streamline)?;
        }

        let topics = streamline.list_topics().unwrap_or_default();
        let metadata = InstanceMetadata {
            id: id.clone(),
            name: name.into(),
            created_at_ms: chrono::Utc::now().timestamp_millis(),
            topics,
            total_records: 0,
        };

        self.instances.insert(
            id.clone(),
            PlaygroundInstance {
                id: id.clone(),
                streamline,
                created_at: Instant::now(),
                metadata,
            },
        );
        self.total_created += 1;

        Ok(id)
    }

    /// Get a playground instance by ID.
    pub fn get_instance(&self, id: &str) -> Option<&PlaygroundInstance> {
        self.instances.get(id)
    }

    /// Get a mutable reference to a playground instance.
    pub fn get_instance_mut(&mut self, id: &str) -> Option<&mut PlaygroundInstance> {
        self.instances.get_mut(id)
    }

    /// Destroy a playground instance.
    pub fn destroy_instance(&mut self, id: &str) -> bool {
        self.instances.remove(id).is_some()
    }

    /// List all active instances.
    pub fn list_instances(&self) -> Vec<&InstanceMetadata> {
        self.instances.values().map(|i| &i.metadata).collect()
    }

    /// Get playground stats.
    pub fn stats(&self) -> PlaygroundStats {
        PlaygroundStats {
            active_instances: self.instances.len(),
            total_created: self.total_created,
            total_expired: self.total_expired,
        }
    }

    fn cleanup_expired(&mut self) {
        let ttl = std::time::Duration::from_secs(self.config.instance_ttl_secs);
        let expired: Vec<String> = self
            .instances
            .iter()
            .filter(|(_, inst)| inst.created_at.elapsed() > ttl)
            .map(|(id, _)| id.clone())
            .collect();

        for id in expired {
            self.instances.remove(&id);
            self.total_expired += 1;
        }
    }

    fn populate_demo_data(streamline: &EmbeddedStreamline) -> Result<()> {
        // Create demo topics
        streamline.create_topic("demo-events", 3)?;
        streamline.create_topic("demo-logs", 1)?;
        streamline.create_topic("demo-metrics", 2)?;

        // Populate with sample data
        for i in 0..10 {
            streamline.produce(
                "demo-events",
                i % 3,
                Some(bytes::Bytes::from(format!("user-{}", i % 5))),
                bytes::Bytes::from(format!(
                    r#"{{"user_id":"user-{}","action":"click","item":"product-{}","timestamp":{}}}"#,
                    i % 5, i, chrono::Utc::now().timestamp_millis()
                )),
            )?;
        }

        for i in 0..5 {
            let level = if i % 3 == 0 { "error" } else { "info" };
            streamline.produce(
                "demo-logs",
                0,
                None,
                bytes::Bytes::from(format!(
                    r#"{{"level":"{}","message":"Sample log message {}","service":"demo"}}"#,
                    level, i
                )),
            )?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_and_list_instances() {
        let mut mgr = PlaygroundManager::new(PlaygroundConfig {
            demo_data: false,
            ..Default::default()
        });
        let id = mgr.create_instance("test-playground").unwrap();
        assert_eq!(mgr.list_instances().len(), 1);

        let inst = mgr.get_instance(&id).unwrap();
        assert_eq!(inst.metadata.name, "test-playground");
    }

    #[test]
    fn test_destroy_instance() {
        let mut mgr = PlaygroundManager::new(PlaygroundConfig {
            demo_data: false,
            ..Default::default()
        });
        let id = mgr.create_instance("temp").unwrap();
        assert!(mgr.destroy_instance(&id));
        assert!(mgr.get_instance(&id).is_none());
    }

    #[test]
    fn test_demo_data_population() {
        let mut mgr = PlaygroundManager::new(PlaygroundConfig::default());
        let id = mgr.create_instance("demo").unwrap();
        let inst = mgr.get_instance(&id).unwrap();
        let topics = inst.streamline.list_topics().unwrap();
        assert!(topics.contains(&"demo-events".to_string()));
        assert!(topics.contains(&"demo-logs".to_string()));
    }

    #[test]
    fn test_max_instances_limit() {
        let mut mgr = PlaygroundManager::new(PlaygroundConfig {
            max_instances: 2,
            demo_data: false,
            ..Default::default()
        });
        mgr.create_instance("a").unwrap();
        mgr.create_instance("b").unwrap();
        assert!(mgr.create_instance("c").is_err());
    }
}
