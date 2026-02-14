//! Statistics Manager
//!
//! Collects and manages column statistics for query optimization.

use super::config::StatisticsConfig;
use super::parquet_segment::ColumnStats;
use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Statistics manager for query optimization
pub struct StatisticsManager {
    /// Configuration
    config: StatisticsConfig,
    /// Per-topic statistics
    topic_stats: Arc<RwLock<HashMap<String, TopicStatistics>>>,
    /// Storage path
    storage_path: PathBuf,
}

impl StatisticsManager {
    /// Create a new statistics manager
    pub fn new(config: StatisticsConfig) -> Result<Self> {
        let storage_path = config.storage_path.clone();

        // Create storage directory if it doesn't exist
        if config.enabled {
            std::fs::create_dir_all(&storage_path).map_err(|e| {
                StreamlineError::storage_msg(format!(
                    "Failed to create statistics directory: {}",
                    e
                ))
            })?;
        }

        Ok(Self {
            config,
            topic_stats: Arc::new(RwLock::new(HashMap::new())),
            storage_path,
        })
    }

    /// Get statistics for a topic
    pub async fn get_topic_stats(&self, topic: &str) -> Option<TopicStatistics> {
        let stats = self.topic_stats.read().await;
        stats.get(topic).cloned()
    }

    /// Get column statistics for a specific column
    pub async fn get_column_stats(&self, topic: &str, column: &str) -> Option<ColumnStats> {
        let stats = self.topic_stats.read().await;
        stats
            .get(topic)
            .and_then(|ts| ts.columns.get(column).cloned())
    }

    /// Update statistics for a topic
    pub async fn update_stats(&self, topic: &str, column_stats: Vec<ColumnStats>) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        let mut stats = self.topic_stats.write().await;
        let topic_stats = stats
            .entry(topic.to_string())
            .or_insert_with(|| TopicStatistics::new(topic.to_string()));

        for col_stat in column_stats {
            topic_stats.merge_column_stats(col_stat);
        }

        topic_stats.last_updated = chrono::Utc::now().timestamp();

        Ok(())
    }

    /// Update statistics from a segment
    pub async fn update_from_segment(
        &self,
        topic: &str,
        partition: i32,
        segment_id: u64,
        column_stats: Vec<ColumnStats>,
    ) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        let mut stats = self.topic_stats.write().await;
        let topic_stats = stats
            .entry(topic.to_string())
            .or_insert_with(|| TopicStatistics::new(topic.to_string()));

        // Update partition stats
        let partition_stats = topic_stats
            .partitions
            .entry(partition)
            .or_insert_with(|| PartitionStatistics::new(partition));

        partition_stats.segment_count += 1;
        partition_stats.last_segment_id = segment_id;

        // Update column stats
        for col_stat in column_stats {
            topic_stats.merge_column_stats(col_stat);
        }

        topic_stats.last_updated = chrono::Utc::now().timestamp();

        Ok(())
    }

    /// Persist statistics to disk
    pub async fn persist(&self) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        let stats = self.topic_stats.read().await;

        for (topic, topic_stats) in stats.iter() {
            let path = self.storage_path.join(format!("{}.stats.json", topic));
            let json = serde_json::to_string_pretty(topic_stats).map_err(|e| {
                StreamlineError::storage_msg(format!("Failed to serialize statistics: {}", e))
            })?;

            std::fs::write(&path, json).map_err(|e| {
                StreamlineError::storage_msg(format!("Failed to write statistics: {}", e))
            })?;
        }

        Ok(())
    }

    /// Load statistics from disk
    pub async fn load(&self) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        if !self.storage_path.exists() {
            return Ok(());
        }

        let mut stats = self.topic_stats.write().await;

        let entries = std::fs::read_dir(&self.storage_path).map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to read statistics directory: {}", e))
        })?;

        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "json") {
                if let Some(name) = path.file_stem() {
                    if let Some(topic) = name.to_str() {
                        let topic = topic.trim_end_matches(".stats");
                        let content = std::fs::read_to_string(&path).map_err(|e| {
                            StreamlineError::storage_msg(format!(
                                "Failed to read statistics: {}",
                                e
                            ))
                        })?;

                        let topic_stats: TopicStatistics =
                            serde_json::from_str(&content).map_err(|e| {
                                StreamlineError::storage_msg(format!(
                                    "Failed to parse statistics: {}",
                                    e
                                ))
                            })?;

                        stats.insert(topic.to_string(), topic_stats);
                    }
                }
            }
        }

        Ok(())
    }

    /// Estimate cardinality for a column
    pub async fn estimate_cardinality(&self, topic: &str, column: &str) -> Option<u64> {
        let stats = self.topic_stats.read().await;
        stats
            .get(topic)
            .and_then(|ts| ts.columns.get(column))
            .and_then(|cs| cs.distinct_count)
    }

    /// Estimate selectivity for a predicate
    pub async fn estimate_selectivity(
        &self,
        topic: &str,
        column: &str,
        min: Option<&str>,
        max: Option<&str>,
    ) -> f64 {
        let stats = self.topic_stats.read().await;

        if let Some(topic_stats) = stats.get(topic) {
            if let Some(col_stats) = topic_stats.columns.get(column) {
                // Simple selectivity estimation based on range
                if let (Some(stat_min), Some(stat_max)) =
                    (&col_stats.min_value, &col_stats.max_value)
                {
                    let query_min = min.unwrap_or(stat_min.as_str());
                    let query_max = max.unwrap_or(stat_max.as_str());

                    // Very simplified selectivity estimation
                    // In practice, this would use histogram data
                    if query_min <= stat_min.as_str() && query_max >= stat_max.as_str() {
                        return 1.0;
                    }

                    // Assume uniform distribution
                    return 0.3; // Default selectivity
                }
            }
        }

        0.5 // Unknown selectivity
    }

    /// Get total record count for a topic
    pub async fn get_record_count(&self, topic: &str) -> u64 {
        let stats = self.topic_stats.read().await;
        stats.get(topic).map(|ts| ts.total_records).unwrap_or(0)
    }

    /// Clear statistics for a topic
    pub async fn clear_topic(&self, topic: &str) -> Result<()> {
        let mut stats = self.topic_stats.write().await;
        stats.remove(topic);

        // Remove persisted file
        let path = self.storage_path.join(format!("{}.stats.json", topic));
        if path.exists() {
            std::fs::remove_file(&path).map_err(|e| {
                StreamlineError::storage_msg(format!("Failed to remove statistics file: {}", e))
            })?;
        }

        Ok(())
    }
}

/// Statistics for a topic
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicStatistics {
    /// Topic name
    pub topic: String,
    /// Total record count
    pub total_records: u64,
    /// Total size in bytes
    pub total_bytes: u64,
    /// Per-column statistics
    pub columns: HashMap<String, ColumnStats>,
    /// Per-partition statistics
    pub partitions: HashMap<i32, PartitionStatistics>,
    /// Last update timestamp
    pub last_updated: i64,
    /// Statistics version
    pub version: u32,
}

impl TopicStatistics {
    /// Create new topic statistics
    pub fn new(topic: String) -> Self {
        Self {
            topic,
            total_records: 0,
            total_bytes: 0,
            columns: HashMap::new(),
            partitions: HashMap::new(),
            last_updated: chrono::Utc::now().timestamp(),
            version: 1,
        }
    }

    /// Merge column statistics
    pub fn merge_column_stats(&mut self, stats: ColumnStats) {
        let existing = self
            .columns
            .entry(stats.name.clone())
            .or_insert_with(|| ColumnStats {
                name: stats.name.clone(),
                data_type: stats.data_type.clone(),
                null_count: 0,
                distinct_count: None,
                min_value: None,
                max_value: None,
                total_size_bytes: 0,
            });

        // Merge null counts
        existing.null_count += stats.null_count;
        existing.total_size_bytes += stats.total_size_bytes;

        // Merge min value
        if let Some(new_min) = &stats.min_value {
            match &existing.min_value {
                Some(existing_min) if new_min < existing_min => {
                    existing.min_value = Some(new_min.clone());
                }
                None => {
                    existing.min_value = Some(new_min.clone());
                }
                _ => {}
            }
        }

        // Merge max value
        if let Some(new_max) = &stats.max_value {
            match &existing.max_value {
                Some(existing_max) if new_max > existing_max => {
                    existing.max_value = Some(new_max.clone());
                }
                None => {
                    existing.max_value = Some(new_max.clone());
                }
                _ => {}
            }
        }

        // Merge distinct count (approximate)
        if let Some(new_distinct) = stats.distinct_count {
            existing.distinct_count = Some(existing.distinct_count.unwrap_or(0).max(new_distinct));
        }
    }
}

/// Statistics for a partition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionStatistics {
    /// Partition number
    pub partition: i32,
    /// Segment count
    pub segment_count: u64,
    /// Last segment ID
    pub last_segment_id: u64,
    /// Total records in partition
    pub total_records: u64,
    /// Total bytes in partition
    pub total_bytes: u64,
}

impl PartitionStatistics {
    /// Create new partition statistics
    pub fn new(partition: i32) -> Self {
        Self {
            partition,
            segment_count: 0,
            last_segment_id: 0,
            total_records: 0,
            total_bytes: 0,
        }
    }
}

/// Histogram for value distribution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Histogram {
    /// Bucket boundaries
    pub buckets: Vec<f64>,
    /// Count per bucket
    pub counts: Vec<u64>,
    /// Total count
    pub total: u64,
}

impl Histogram {
    /// Create a new histogram with specified bucket count
    pub fn new(min: f64, max: f64, bucket_count: usize) -> Self {
        let step = (max - min) / bucket_count as f64;
        let buckets: Vec<f64> = (0..=bucket_count).map(|i| min + step * i as f64).collect();
        let counts = vec![0; bucket_count];

        Self {
            buckets,
            counts,
            total: 0,
        }
    }

    /// Add a value to the histogram
    pub fn add(&mut self, value: f64) {
        if self.buckets.len() < 2 {
            return;
        }

        for i in 0..self.counts.len() {
            if value >= self.buckets[i] && value < self.buckets[i + 1] {
                self.counts[i] += 1;
                self.total += 1;
                return;
            }
        }

        // Value is at the max boundary
        if value == self.buckets[self.buckets.len() - 1] {
            if let Some(last) = self.counts.last_mut() {
                *last += 1;
                self.total += 1;
            }
        }
    }

    /// Estimate selectivity for a range
    pub fn estimate_selectivity(&self, min: f64, max: f64) -> f64 {
        if self.total == 0 {
            return 0.5;
        }

        let mut count = 0u64;

        for i in 0..self.counts.len() {
            let bucket_min = self.buckets[i];
            let bucket_max = self.buckets[i + 1];

            if bucket_max <= min || bucket_min >= max {
                continue;
            }

            // Partial overlap
            let overlap_min = bucket_min.max(min);
            let overlap_max = bucket_max.min(max);
            let bucket_range = bucket_max - bucket_min;
            let overlap_range = overlap_max - overlap_min;

            let fraction = if bucket_range > 0.0 {
                overlap_range / bucket_range
            } else {
                1.0
            };

            count += (self.counts[i] as f64 * fraction) as u64;
        }

        count as f64 / self.total as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_histogram() {
        let mut hist = Histogram::new(0.0, 100.0, 10);

        for i in 0..100 {
            hist.add(i as f64);
        }

        assert_eq!(hist.total, 100);

        // Selectivity for full range
        let sel = hist.estimate_selectivity(0.0, 100.0);
        assert!(sel > 0.9);

        // Selectivity for half range
        let sel = hist.estimate_selectivity(0.0, 50.0);
        assert!(sel > 0.4 && sel < 0.6);
    }

    #[test]
    fn test_merge_column_stats() {
        let mut topic_stats = TopicStatistics::new("test".to_string());

        let stats1 = ColumnStats {
            name: "value".to_string(),
            data_type: "int64".to_string(),
            null_count: 5,
            distinct_count: Some(50),
            min_value: Some("10".to_string()),
            max_value: Some("100".to_string()),
            total_size_bytes: 400,
        };

        let stats2 = ColumnStats {
            name: "value".to_string(),
            data_type: "int64".to_string(),
            null_count: 3,
            distinct_count: Some(75),
            min_value: Some("5".to_string()),
            max_value: Some("200".to_string()),
            total_size_bytes: 600,
        };

        topic_stats.merge_column_stats(stats1);
        topic_stats.merge_column_stats(stats2);

        let merged = topic_stats.columns.get("value").unwrap();
        assert_eq!(merged.null_count, 8);
        assert_eq!(merged.distinct_count, Some(75));
        assert_eq!(merged.min_value, Some("10".to_string())); // Lexicographic comparison
        assert_eq!(merged.max_value, Some("200".to_string()));
        assert_eq!(merged.total_size_bytes, 1000);
    }
}
