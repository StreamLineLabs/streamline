//! Retention policy enforcement for Streamline storage
//!
//! This module provides retention policies to automatically delete old data
//! based on time or size limits.

use crate::error::Result;
use crate::storage::topic::{TopicConfig, TopicManager};
use std::time::Duration;
use tracing::{debug, info, warn};

/// Default retention check interval (5 minutes)
pub const DEFAULT_RETENTION_INTERVAL_SECS: u64 = 300;

/// Retention policy enforcer
pub(crate) struct RetentionEnforcer {
    /// Interval between retention checks
    interval: Duration,
}

impl RetentionEnforcer {
    /// Create a new retention enforcer with default interval
    pub fn new() -> Self {
        Self {
            interval: Duration::from_secs(DEFAULT_RETENTION_INTERVAL_SECS),
        }
    }

    /// Create a new retention enforcer with custom interval
    #[allow(dead_code)]
    pub fn with_interval(interval_secs: u64) -> Self {
        Self {
            interval: Duration::from_secs(interval_secs),
        }
    }

    /// Get the retention check interval
    pub fn interval(&self) -> Duration {
        self.interval
    }

    /// Run retention enforcement for all topics
    pub async fn enforce(&self, topic_manager: &TopicManager) -> Result<()> {
        let topics = topic_manager.list_topics()?;

        for topic_metadata in topics {
            let topic_name = &topic_metadata.name;
            let config = &topic_metadata.config;

            // Handle compaction if cleanup policy includes "compact"
            if config.cleanup_policy.supports_compaction() {
                debug!(
                    topic = %topic_name,
                    cleanup_policy = %config.cleanup_policy,
                    "Running compaction"
                );

                // Compact each partition
                for partition_id in 0..topic_metadata.num_partitions {
                    if let Err(e) = self
                        .compact_partition(topic_manager, topic_name, partition_id, config)
                        .await
                    {
                        warn!(
                            topic = %topic_name,
                            partition = partition_id,
                            error = %e,
                            "Failed to compact partition"
                        );
                    }
                }

                // If policy is compact-only (not "compact,delete"), skip retention
                if config.cleanup_policy.is_compact_only() {
                    continue;
                }
            }

            // Skip if cleanup policy is not "delete"
            if !config.cleanup_policy.supports_delete() {
                debug!(
                    topic = %topic_name,
                    cleanup_policy = %config.cleanup_policy,
                    "Skipping retention (cleanup policy does not include 'delete')"
                );
                continue;
            }

            // Skip if both retention policies are infinite
            if config.retention_ms == -1 && config.retention_bytes == -1 {
                debug!(
                    topic = %topic_name,
                    "Skipping retention (infinite retention configured)"
                );
                continue;
            }

            info!(
                topic = %topic_name,
                retention_ms = config.retention_ms,
                retention_bytes = config.retention_bytes,
                "Enforcing retention policies"
            );

            // Enforce retention for each partition
            for partition_id in 0..topic_metadata.num_partitions {
                if let Err(e) = self
                    .enforce_partition_retention(topic_manager, topic_name, partition_id, config)
                    .await
                {
                    warn!(
                        topic = %topic_name,
                        partition = partition_id,
                        error = %e,
                        "Failed to enforce retention for partition"
                    );
                }
            }
        }

        Ok(())
    }

    /// Enforce retention for a specific partition
    async fn enforce_partition_retention(
        &self,
        topic_manager: &TopicManager,
        topic_name: &str,
        partition_id: i32,
        config: &TopicConfig,
    ) -> Result<()> {
        let (time_deleted, size_deleted) = topic_manager.enforce_partition_retention(
            topic_name,
            partition_id,
            config.retention_ms,
            config.retention_bytes,
        )?;

        if time_deleted > 0 || size_deleted > 0 {
            info!(
                topic = %topic_name,
                partition = partition_id,
                time_deleted,
                size_deleted,
                "Retention enforced on partition"
            );
        }

        Ok(())
    }

    /// Compact a specific partition
    async fn compact_partition(
        &self,
        topic_manager: &TopicManager,
        topic_name: &str,
        partition_id: i32,
        config: &TopicConfig,
    ) -> Result<()> {
        use crate::storage::compaction::LogCompactor;

        let compactor = LogCompactor::new(
            config.min_cleanable_dirty_ratio,
            config.delete_retention_ms,
            config.min_compaction_lag_ms,
        );

        let compacted_count =
            topic_manager.compact_partition(topic_name, partition_id, &compactor)?;

        if compacted_count > 0 {
            info!(
                topic = %topic_name,
                partition = partition_id,
                compacted_count,
                "Compaction completed on partition"
            );
        }

        Ok(())
    }
}

impl Default for RetentionEnforcer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retention_enforcer_new() {
        let enforcer = RetentionEnforcer::new();
        assert_eq!(
            enforcer.interval(),
            Duration::from_secs(DEFAULT_RETENTION_INTERVAL_SECS)
        );
    }

    #[test]
    fn test_retention_enforcer_custom_interval() {
        let enforcer = RetentionEnforcer::with_interval(60);
        assert_eq!(enforcer.interval(), Duration::from_secs(60));
    }

    #[tokio::test]
    async fn test_retention_enforcer_basic() {
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let topic_manager = TopicManager::new(dir.path()).unwrap();

        // Create a topic with default config (infinite retention)
        topic_manager.create_topic("test-topic", 1).unwrap();

        let enforcer = RetentionEnforcer::new();

        // Should not fail, just skip topics with infinite retention
        enforcer.enforce(&topic_manager).await.unwrap();
    }
}
