//! Consumer lag tracking and metrics
//!
//! This module provides functionality for calculating and tracking consumer group lag,
//! which is the difference between the latest offset in a partition and the committed
//! offset for a consumer group.

use crate::consumer::coordinator::GroupCoordinator;
use crate::error::Result;
use crate::storage::TopicManager;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Consumer lag for a single partition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionLag {
    /// Topic name
    pub topic: String,
    /// Partition ID
    pub partition: i32,
    /// Current committed offset for the consumer group
    pub current_offset: i64,
    /// Latest offset (log end offset) in the partition
    pub log_end_offset: i64,
    /// Lag (difference between log end offset and current offset)
    pub lag: i64,
}

/// Consumer lag for a consumer group
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroupLag {
    /// Group ID
    pub group_id: String,
    /// State of the consumer group
    pub state: String,
    /// Per-partition lag information
    pub partitions: Vec<PartitionLag>,
    /// Total lag across all partitions
    pub total_lag: i64,
    /// Number of members in the group
    pub members: usize,
}

/// Lag summary for a topic within a consumer group
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicLag {
    /// Topic name
    pub topic: String,
    /// Per-partition lag
    pub partitions: Vec<PartitionLag>,
    /// Total lag for this topic
    pub total_lag: i64,
}

/// Consumer lag calculator
///
/// Provides methods to calculate consumer group lag using the group coordinator
/// and topic manager.
pub struct LagCalculator {
    /// Topic manager for getting log end offsets
    topic_manager: Arc<TopicManager>,
}

impl LagCalculator {
    /// Create a new lag calculator
    pub fn new(topic_manager: Arc<TopicManager>) -> Self {
        Self { topic_manager }
    }

    /// Calculate lag for a specific consumer group
    pub fn calculate_group_lag(
        &self,
        coordinator: &GroupCoordinator,
        group_id: &str,
    ) -> Result<ConsumerGroupLag> {
        let group = coordinator.get_group(group_id)?;

        match group {
            Some(group) => {
                let mut partitions = Vec::new();
                let mut total_lag: i64 = 0;

                // Iterate over all committed offsets
                for ((topic, partition), committed) in &group.offsets {
                    let log_end_offset = self
                        .topic_manager
                        .latest_offset(topic, *partition)
                        .unwrap_or(0);
                    let current_offset = committed.offset;
                    let lag = (log_end_offset - current_offset).max(0);

                    partitions.push(PartitionLag {
                        topic: topic.clone(),
                        partition: *partition,
                        current_offset,
                        log_end_offset,
                        lag,
                    });

                    total_lag += lag;
                }

                // Sort partitions by topic, then partition
                partitions.sort_by(|a, b| match a.topic.cmp(&b.topic) {
                    std::cmp::Ordering::Equal => a.partition.cmp(&b.partition),
                    other => other,
                });

                let state = format!("{:?}", group.state);
                let members = group.members.len();

                Ok(ConsumerGroupLag {
                    group_id: group_id.to_string(),
                    state,
                    partitions,
                    total_lag,
                    members,
                })
            }
            None => {
                // Group doesn't exist, return empty lag info
                Ok(ConsumerGroupLag {
                    group_id: group_id.to_string(),
                    state: "Unknown".to_string(),
                    partitions: Vec::new(),
                    total_lag: 0,
                    members: 0,
                })
            }
        }
    }

    /// Calculate lag for a specific topic within a consumer group
    pub fn calculate_topic_lag(
        &self,
        coordinator: &GroupCoordinator,
        group_id: &str,
        topic: &str,
    ) -> Result<TopicLag> {
        let group_lag = self.calculate_group_lag(coordinator, group_id)?;

        let partitions: Vec<PartitionLag> = group_lag
            .partitions
            .into_iter()
            .filter(|p| p.topic == topic)
            .collect();

        let total_lag: i64 = partitions.iter().map(|p| p.lag).sum();

        Ok(TopicLag {
            topic: topic.to_string(),
            partitions,
            total_lag,
        })
    }

    /// Calculate lag for all consumer groups
    pub fn calculate_all_groups_lag(
        &self,
        coordinator: &GroupCoordinator,
    ) -> Result<Vec<ConsumerGroupLag>> {
        let groups = coordinator.list_groups()?;
        let mut results = Vec::new();

        for group_id in groups {
            let lag = self.calculate_group_lag(coordinator, &group_id)?;
            results.push(lag);
        }

        Ok(results)
    }

    /// Get lag summary by topic for a consumer group
    pub fn get_lag_by_topic(
        &self,
        coordinator: &GroupCoordinator,
        group_id: &str,
    ) -> Result<HashMap<String, TopicLag>> {
        let group_lag = self.calculate_group_lag(coordinator, group_id)?;
        let mut by_topic: HashMap<String, Vec<PartitionLag>> = HashMap::new();

        for partition_lag in group_lag.partitions {
            by_topic
                .entry(partition_lag.topic.clone())
                .or_default()
                .push(partition_lag);
        }

        let mut result = HashMap::new();
        for (topic, partitions) in by_topic {
            let total_lag: i64 = partitions.iter().map(|p| p.lag).sum();
            result.insert(
                topic.clone(),
                TopicLag {
                    topic,
                    partitions,
                    total_lag,
                },
            );
        }

        Ok(result)
    }
}

/// Lag statistics for aggregated reporting
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LagStats {
    /// Maximum lag across all partitions
    pub max_lag: i64,
    /// Minimum lag across all partitions
    pub min_lag: i64,
    /// Average lag across all partitions
    pub avg_lag: f64,
    /// Total lag
    pub total_lag: i64,
    /// Number of partitions
    pub partition_count: usize,
    /// Number of partitions with non-zero lag
    pub lagging_partitions: usize,
}

impl LagStats {
    /// Calculate statistics from partition lag data
    pub fn from_partitions(partitions: &[PartitionLag]) -> Self {
        if partitions.is_empty() {
            return Self::default();
        }

        let total_lag: i64 = partitions.iter().map(|p| p.lag).sum();
        let max_lag = partitions.iter().map(|p| p.lag).max().unwrap_or(0);
        let min_lag = partitions.iter().map(|p| p.lag).min().unwrap_or(0);
        let avg_lag = total_lag as f64 / partitions.len() as f64;
        let lagging_partitions = partitions.iter().filter(|p| p.lag > 0).count();

        Self {
            max_lag,
            min_lag,
            avg_lag,
            total_lag,
            partition_count: partitions.len(),
            lagging_partitions,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_partition_lag_creation() {
        let lag = PartitionLag {
            topic: "test-topic".to_string(),
            partition: 0,
            current_offset: 100,
            log_end_offset: 150,
            lag: 50,
        };

        assert_eq!(lag.topic, "test-topic");
        assert_eq!(lag.partition, 0);
        assert_eq!(lag.current_offset, 100);
        assert_eq!(lag.log_end_offset, 150);
        assert_eq!(lag.lag, 50);
    }

    #[test]
    fn test_lag_stats_from_empty() {
        let stats = LagStats::from_partitions(&[]);
        assert_eq!(stats.max_lag, 0);
        assert_eq!(stats.min_lag, 0);
        assert_eq!(stats.avg_lag, 0.0);
        assert_eq!(stats.total_lag, 0);
        assert_eq!(stats.partition_count, 0);
        assert_eq!(stats.lagging_partitions, 0);
    }

    #[test]
    fn test_lag_stats_from_partitions() {
        let partitions = vec![
            PartitionLag {
                topic: "test".to_string(),
                partition: 0,
                current_offset: 100,
                log_end_offset: 150,
                lag: 50,
            },
            PartitionLag {
                topic: "test".to_string(),
                partition: 1,
                current_offset: 200,
                log_end_offset: 200,
                lag: 0,
            },
            PartitionLag {
                topic: "test".to_string(),
                partition: 2,
                current_offset: 300,
                log_end_offset: 400,
                lag: 100,
            },
        ];

        let stats = LagStats::from_partitions(&partitions);
        assert_eq!(stats.max_lag, 100);
        assert_eq!(stats.min_lag, 0);
        assert_eq!(stats.total_lag, 150);
        assert_eq!(stats.partition_count, 3);
        assert_eq!(stats.lagging_partitions, 2);
        assert!((stats.avg_lag - 50.0).abs() < 0.001);
    }

    #[test]
    fn test_consumer_group_lag_serialization() {
        let lag = ConsumerGroupLag {
            group_id: "test-group".to_string(),
            state: "Stable".to_string(),
            partitions: vec![PartitionLag {
                topic: "test".to_string(),
                partition: 0,
                current_offset: 100,
                log_end_offset: 150,
                lag: 50,
            }],
            total_lag: 50,
            members: 2,
        };

        let json = serde_json::to_string(&lag).unwrap();
        let deserialized: ConsumerGroupLag = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.group_id, lag.group_id);
        assert_eq!(deserialized.total_lag, lag.total_lag);
        assert_eq!(deserialized.partitions.len(), 1);
    }

    #[test]
    fn test_topic_lag_creation() {
        let topic_lag = TopicLag {
            topic: "test-topic".to_string(),
            partitions: vec![
                PartitionLag {
                    topic: "test-topic".to_string(),
                    partition: 0,
                    current_offset: 100,
                    log_end_offset: 200,
                    lag: 100,
                },
                PartitionLag {
                    topic: "test-topic".to_string(),
                    partition: 1,
                    current_offset: 150,
                    log_end_offset: 200,
                    lag: 50,
                },
            ],
            total_lag: 150,
        };

        assert_eq!(topic_lag.topic, "test-topic");
        assert_eq!(topic_lag.partitions.len(), 2);
        assert_eq!(topic_lag.total_lag, 150);
    }

    #[test]
    fn test_lag_calculator_creation() {
        let temp_dir = tempdir().unwrap();
        let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());
        let calculator = LagCalculator::new(topic_manager);

        // Just verify it creates without panic
        let _ = calculator;
    }

    #[test]
    fn test_lag_calculator_with_no_groups() {
        let temp_dir = tempdir().unwrap();
        let offset_dir = tempdir().unwrap();
        let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());
        let calculator = LagCalculator::new(topic_manager.clone());
        let coordinator = GroupCoordinator::new(offset_dir.path(), topic_manager).unwrap();

        let all_lags = calculator.calculate_all_groups_lag(&coordinator).unwrap();
        assert!(all_lags.is_empty());
    }

    #[test]
    fn test_lag_calculator_unknown_group() {
        let temp_dir = tempdir().unwrap();
        let offset_dir = tempdir().unwrap();
        let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());
        let calculator = LagCalculator::new(topic_manager.clone());
        let coordinator = GroupCoordinator::new(offset_dir.path(), topic_manager).unwrap();

        let lag = calculator
            .calculate_group_lag(&coordinator, "nonexistent-group")
            .unwrap();
        assert_eq!(lag.group_id, "nonexistent-group");
        assert_eq!(lag.state, "Unknown");
        assert_eq!(lag.total_lag, 0);
        assert!(lag.partitions.is_empty());
    }
}
