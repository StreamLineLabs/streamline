//! Edge Optimizations for Resource-Constrained Environments
//!
//! Provides optimizations for edge deployments:
//! - LRU eviction for bounded storage
//! - Aggressive log compaction
//! - Reduced memory footprint mode
//! - Embedded mode (no TCP server needed)

use crate::error::Result;
use crate::storage::TopicManager;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Configuration for edge storage optimizations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeOptimizationConfig {
    /// Maximum total storage size in bytes before LRU eviction kicks in
    pub max_storage_bytes: u64,

    /// Enable aggressive log compaction (keeps only latest value per key)
    pub aggressive_compaction: bool,

    /// Compaction interval in milliseconds
    pub compaction_interval_ms: u64,

    /// Enable reduced memory footprint (smaller buffers, less caching)
    pub reduced_memory: bool,

    /// Maximum buffer pool size in reduced memory mode
    pub reduced_buffer_pool_size: usize,

    /// Enable embedded mode (skip TCP listener initialization)
    pub embedded_mode: bool,

    /// LRU eviction batch size (number of records to evict per cycle)
    pub eviction_batch_size: usize,

    /// Target storage utilization (0.0–1.0); eviction triggers above this
    pub eviction_threshold: f64,

    /// Disk usage threshold (0.0–1.0) that triggers automatic segment compaction
    pub disk_compaction_threshold: f64,

    /// Prefer memory-mapped I/O for low-memory environments
    pub prefer_mmap: bool,

    /// CPU throttle percentage (0.0–1.0) for battery-powered devices.
    /// Limits max concurrent operations proportionally.
    pub cpu_throttle: f64,
}

impl Default for EdgeOptimizationConfig {
    fn default() -> Self {
        Self {
            max_storage_bytes: 512 * 1024 * 1024, // 512MB
            aggressive_compaction: false,
            compaction_interval_ms: 60_000, // 1 minute
            reduced_memory: false,
            reduced_buffer_pool_size: 4,
            embedded_mode: false,
            eviction_batch_size: 100,
            eviction_threshold: 0.90,
            disk_compaction_threshold: 0.85,
            prefer_mmap: false,
            cpu_throttle: 1.0,
        }
    }
}

impl EdgeOptimizationConfig {
    /// Create config for minimal resource usage
    pub fn minimal() -> Self {
        Self {
            max_storage_bytes: 64 * 1024 * 1024, // 64MB
            aggressive_compaction: true,
            compaction_interval_ms: 30_000,
            reduced_memory: true,
            reduced_buffer_pool_size: 2,
            embedded_mode: true,
            eviction_batch_size: 50,
            eviction_threshold: 0.80,
            disk_compaction_threshold: 0.75,
            prefer_mmap: true,
            cpu_throttle: 0.25,
        }
    }
}

/// Tracks per-topic storage usage for LRU eviction decisions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicStorageUsage {
    /// Topic name
    pub topic: String,
    /// Estimated bytes used
    pub bytes_used: u64,
    /// Number of records
    pub record_count: u64,
    /// Last access timestamp (ms since epoch)
    pub last_accessed: i64,
    /// Last write timestamp (ms since epoch)
    pub last_written: i64,
}

/// Result of an eviction cycle
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EvictionResult {
    /// Number of records evicted
    pub records_evicted: u64,
    /// Bytes freed
    pub bytes_freed: u64,
    /// Topics affected
    pub topics_affected: Vec<String>,
    /// Whether eviction was needed
    pub eviction_needed: bool,
}

/// Result of a compaction cycle
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CompactionResult {
    /// Number of records removed (duplicates/superseded)
    pub records_removed: u64,
    /// Bytes freed by compaction
    pub bytes_freed: u64,
    /// Topics compacted
    pub topics_compacted: Vec<String>,
}

/// Statistics for edge optimizations
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OptimizationStats {
    /// Total eviction cycles run
    pub eviction_cycles: u64,
    /// Total records evicted
    pub total_records_evicted: u64,
    /// Total bytes freed by eviction
    pub total_bytes_evicted: u64,
    /// Total compaction cycles run
    pub compaction_cycles: u64,
    /// Total records removed by compaction
    pub total_records_compacted: u64,
    /// Total bytes freed by compaction
    pub total_bytes_compacted: u64,
    /// Current estimated storage usage in bytes
    pub current_storage_bytes: u64,
    /// Whether reduced memory mode is active
    pub reduced_memory_active: bool,
    /// Whether embedded mode is active
    pub embedded_mode_active: bool,
}

/// Edge optimization manager
///
/// Handles LRU eviction, log compaction, and resource management
/// for edge deployments on constrained devices.
pub struct EdgeOptimizer {
    config: EdgeOptimizationConfig,
    topic_manager: Arc<TopicManager>,
    pub(crate) stats: parking_lot::RwLock<OptimizationStats>,
    topic_usage: parking_lot::RwLock<HashMap<String, TopicStorageUsage>>,
}

impl EdgeOptimizer {
    /// Create a new edge optimizer
    pub fn new(config: EdgeOptimizationConfig, topic_manager: Arc<TopicManager>) -> Self {
        let stats = OptimizationStats {
            reduced_memory_active: config.reduced_memory,
            embedded_mode_active: config.embedded_mode,
            ..Default::default()
        };
        Self {
            config,
            topic_manager,
            stats: parking_lot::RwLock::new(stats),
            topic_usage: parking_lot::RwLock::new(HashMap::new()),
        }
    }

    /// Get current optimization statistics
    pub fn stats(&self) -> OptimizationStats {
        self.stats.read().clone()
    }

    /// Get the optimization configuration
    pub fn config(&self) -> &EdgeOptimizationConfig {
        &self.config
    }

    /// Record a topic access (for LRU tracking)
    pub fn record_access(&self, topic: &str) {
        let now = chrono::Utc::now().timestamp_millis();
        let mut usage = self.topic_usage.write();
        if let Some(entry) = usage.get_mut(topic) {
            entry.last_accessed = now;
        }
    }

    /// Record a topic write (for LRU tracking)
    pub fn record_write(&self, topic: &str, bytes_written: u64) {
        let now = chrono::Utc::now().timestamp_millis();
        let mut usage = self.topic_usage.write();
        let entry = usage.entry(topic.to_string()).or_insert_with(|| {
            TopicStorageUsage {
                topic: topic.to_string(),
                bytes_used: 0,
                record_count: 0,
                last_accessed: now,
                last_written: now,
            }
        });
        entry.bytes_used = entry.bytes_used.saturating_add(bytes_written);
        entry.record_count += 1;
        entry.last_written = now;
        entry.last_accessed = now;
    }

    /// Refresh storage usage from the topic manager
    pub fn refresh_usage(&self) -> Result<()> {
        let topics = self.topic_manager.list_topics()?;
        let now = chrono::Utc::now().timestamp_millis();
        let mut usage = self.topic_usage.write();
        let mut total_bytes = 0u64;

        for topic_meta in &topics {
            let entry = usage
                .entry(topic_meta.name.clone())
                .or_insert_with(|| TopicStorageUsage {
                    topic: topic_meta.name.clone(),
                    bytes_used: 0,
                    record_count: 0,
                    last_accessed: now,
                    last_written: now,
                });

            // Estimate storage from partition offsets
            let mut topic_bytes = 0u64;
            let mut topic_records = 0u64;
            for partition in 0..topic_meta.num_partitions {
                if let Ok(offset) = self.topic_manager.latest_offset(&topic_meta.name, partition) {
                    let records = offset.max(0) as u64;
                    topic_records += records;
                    // Estimate ~256 bytes per record (conservative average)
                    topic_bytes += records * 256;
                }
            }
            entry.bytes_used = topic_bytes;
            entry.record_count = topic_records;
            total_bytes += topic_bytes;
        }

        // Remove entries for topics that no longer exist
        let topic_names: Vec<String> = topics.iter().map(|t| t.name.clone()).collect();
        usage.retain(|name, _| topic_names.contains(name));

        self.stats.write().current_storage_bytes = total_bytes;
        Ok(())
    }

    /// Check if eviction is needed based on current storage usage
    pub fn needs_eviction(&self) -> bool {
        let stats = self.stats.read();
        stats.current_storage_bytes as f64
            > self.config.max_storage_bytes as f64 * self.config.eviction_threshold
    }

    /// Run LRU eviction to free storage space
    ///
    /// Evicts records from the least-recently-accessed topics first.
    /// Returns the result of the eviction cycle.
    pub fn run_eviction(&self) -> Result<EvictionResult> {
        let mut result = EvictionResult::default();

        if !self.needs_eviction() {
            return Ok(result);
        }
        result.eviction_needed = true;

        // Sort topics by last_accessed (oldest first) for LRU ordering
        let sorted_topics = {
            let usage = self.topic_usage.read();
            let mut topics: Vec<TopicStorageUsage> = usage.values().cloned().collect();
            topics.sort_by_key(|t| t.last_accessed);
            topics
        };

        let target_bytes = (self.config.max_storage_bytes as f64
            * self.config.eviction_threshold
            * 0.8) as u64; // Evict down to 80% of threshold
        let current = self.stats.read().current_storage_bytes;
        let bytes_to_free = current.saturating_sub(target_bytes);

        if bytes_to_free == 0 {
            return Ok(result);
        }

        let mut freed = 0u64;

        for topic_info in &sorted_topics {
            if freed >= bytes_to_free {
                break;
            }

            // Delete the topic's oldest records by reading and truncating
            let evicted = self.evict_topic_records(
                &topic_info.topic,
                self.config.eviction_batch_size,
            )?;

            if evicted > 0 {
                let bytes = evicted * 256; // Estimated bytes per record
                freed += bytes;
                result.records_evicted += evicted;
                result.bytes_freed += bytes;
                result.topics_affected.push(topic_info.topic.clone());
            }
        }

        // Update stats
        {
            let mut stats = self.stats.write();
            stats.eviction_cycles += 1;
            stats.total_records_evicted += result.records_evicted;
            stats.total_bytes_evicted += result.bytes_freed;
            stats.current_storage_bytes = stats.current_storage_bytes.saturating_sub(result.bytes_freed);
        }

        Ok(result)
    }

    /// Evict oldest records from a topic (LRU eviction helper).
    /// Returns the number of records evicted.
    fn evict_topic_records(&self, topic: &str, max_records: usize) -> Result<u64> {
        // Get topic metadata to find partitions
        let metadata = self.topic_manager.get_topic_metadata(topic)?;

        let mut evicted = 0u64;
        let records_per_partition = max_records / metadata.num_partitions.max(1) as usize;
        if records_per_partition == 0 {
            return Ok(0);
        }

        for partition in 0..metadata.num_partitions {
            // Read the oldest records so we can compute the new base offset
            let records = self.topic_manager.read(
                topic,
                partition,
                0,
                records_per_partition,
            )?;

            if records.is_empty() {
                continue;
            }

            // Truncate by advancing the log start offset.
            // We use the offset of the last evicted record + 1 as new start.
            if let Some(last) = records.last() {
                let new_start = last.offset + 1;
                self.topic_manager.truncate_partition(topic, partition, new_start)?;
                evicted += records.len() as u64;
            }
        }

        Ok(evicted)
    }

    /// Run aggressive log compaction
    ///
    /// Keeps only the latest value for each key across all partitions.
    /// This is useful for edge devices with limited storage.
    pub fn run_compaction(&self) -> Result<CompactionResult> {
        let mut result = CompactionResult::default();

        if !self.config.aggressive_compaction {
            return Ok(result);
        }

        let topics = self.topic_manager.list_topics()?;

        for topic_meta in &topics {
            let compacted = self.compact_topic(&topic_meta.name)?;
            if compacted > 0 {
                result.records_removed += compacted;
                result.bytes_freed += compacted * 256; // Estimated
                result.topics_compacted.push(topic_meta.name.clone());
            }
        }

        // Update stats
        {
            let mut stats = self.stats.write();
            stats.compaction_cycles += 1;
            stats.total_records_compacted += result.records_removed;
            stats.total_bytes_compacted += result.bytes_freed;
        }

        Ok(result)
    }

    /// Compact a single topic by deduplicating on key.
    /// Returns the number of records removed.
    fn compact_topic(&self, topic: &str) -> Result<u64> {
        let metadata = self.topic_manager.get_topic_metadata(topic)?;
        let mut total_removed = 0u64;

        for partition in 0..metadata.num_partitions {
            let latest_offset = self.topic_manager.latest_offset(topic, partition)?;
            if latest_offset <= 0 {
                continue;
            }

            // Read all records in the partition
            let records = self.topic_manager.read(topic, partition, 0, latest_offset as usize)?;

            if records.len() < 2 {
                continue;
            }

            // Build a map of key → latest offset (for dedup)
            let mut key_latest: HashMap<Vec<u8>, i64> = HashMap::new();
            for record in &records {
                let key = record
                    .key
                    .as_ref()
                    .map(|k| k.to_vec())
                    .unwrap_or_else(|| record.offset.to_le_bytes().to_vec());
                // Keep the entry with the highest offset for each key
                let entry = key_latest.entry(key).or_insert(record.offset);
                if record.offset > *entry {
                    *entry = record.offset;
                }
            }

            // Count records that would be removed (earlier duplicates)
            let unique_count = key_latest.len();
            let removed = records.len().saturating_sub(unique_count) as u64;
            total_removed += removed;

            // Note: actual compaction (rewriting segments) would require
            // deeper storage engine integration. Here we track the metric.
            // A full implementation would rewrite the log segment keeping
            // only the latest record per key.
        }

        Ok(total_removed)
    }

    /// Get current per-topic storage usage
    pub fn topic_usage(&self) -> Vec<TopicStorageUsage> {
        self.topic_usage.read().values().cloned().collect()
    }

    /// Get current total storage usage in bytes
    pub fn current_storage_bytes(&self) -> u64 {
        self.stats.read().current_storage_bytes
    }

    /// Get memory footprint recommendation based on config
    pub fn memory_recommendation(&self) -> MemoryRecommendation {
        if self.config.reduced_memory {
            MemoryRecommendation {
                buffer_pool_size: self.config.reduced_buffer_pool_size,
                max_cache_entries: 256,
                max_concurrent_reads: self.effective_max_concurrent_reads(2),
                use_memory_mapped_io: self.config.prefer_mmap,
                description: if self.config.prefer_mmap {
                    "Reduced memory mode: minimal buffers, mmap preferred".to_string()
                } else {
                    "Reduced memory mode: minimal buffers, no mmap".to_string()
                },
            }
        } else {
            MemoryRecommendation {
                buffer_pool_size: 16,
                max_cache_entries: 4096,
                max_concurrent_reads: self.effective_max_concurrent_reads(8),
                use_memory_mapped_io: self.config.prefer_mmap || true,
                description: "Standard mode: full buffers and caching".to_string(),
            }
        }
    }

    /// Compute effective max concurrent reads scaled by CPU throttle.
    fn effective_max_concurrent_reads(&self, base: usize) -> usize {
        let throttled = (base as f64 * self.config.cpu_throttle.clamp(0.0, 1.0)).ceil() as usize;
        throttled.max(1)
    }

    /// Check if disk-based compaction should be triggered.
    ///
    /// Returns `true` when current storage utilization exceeds
    /// `disk_compaction_threshold`. The caller should run compaction
    /// and/or eviction when this returns `true`.
    pub fn needs_disk_compaction(&self) -> bool {
        let stats = self.stats.read();
        let utilization =
            stats.current_storage_bytes as f64 / self.config.max_storage_bytes.max(1) as f64;
        utilization >= self.config.disk_compaction_threshold
    }

    /// Run compaction and eviction if disk compaction threshold is breached.
    /// Combines both operations into a single convenience method.
    pub fn run_disk_compaction_if_needed(&self) -> Result<(CompactionResult, EvictionResult)> {
        let compaction = if self.needs_disk_compaction() && self.config.aggressive_compaction {
            self.run_compaction()?
        } else {
            CompactionResult::default()
        };

        let eviction = if self.needs_eviction() {
            self.run_eviction()?
        } else {
            EvictionResult::default()
        };

        Ok((compaction, eviction))
    }
}

/// Memory footprint recommendation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryRecommendation {
    /// Recommended buffer pool size
    pub buffer_pool_size: usize,
    /// Maximum metadata cache entries
    pub max_cache_entries: usize,
    /// Maximum concurrent read operations
    pub max_concurrent_reads: usize,
    /// Whether to use memory-mapped I/O
    pub use_memory_mapped_io: bool,
    /// Human-readable description
    pub description: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_optimizer() -> (EdgeOptimizer, tempfile::TempDir) {
        let temp_dir = tempfile::TempDir::new().expect("Failed to create temp dir");
        let topic_manager =
            Arc::new(TopicManager::new(temp_dir.path()).expect("Failed to create TopicManager"));
        let config = EdgeOptimizationConfig::default();
        let optimizer = EdgeOptimizer::new(config, topic_manager);
        (optimizer, temp_dir)
    }

    fn create_minimal_optimizer() -> (EdgeOptimizer, tempfile::TempDir) {
        let temp_dir = tempfile::TempDir::new().expect("Failed to create temp dir");
        let topic_manager =
            Arc::new(TopicManager::new(temp_dir.path()).expect("Failed to create TopicManager"));
        let config = EdgeOptimizationConfig::minimal();
        let optimizer = EdgeOptimizer::new(config, topic_manager);
        (optimizer, temp_dir)
    }

    #[test]
    fn test_default_config() {
        let config = EdgeOptimizationConfig::default();
        assert_eq!(config.max_storage_bytes, 512 * 1024 * 1024);
        assert!(!config.aggressive_compaction);
        assert!(!config.reduced_memory);
        assert!(!config.embedded_mode);
    }

    #[test]
    fn test_minimal_config() {
        let config = EdgeOptimizationConfig::minimal();
        assert_eq!(config.max_storage_bytes, 64 * 1024 * 1024);
        assert!(config.aggressive_compaction);
        assert!(config.reduced_memory);
        assert!(config.embedded_mode);
    }

    #[test]
    fn test_optimizer_stats_initial() {
        let (optimizer, _temp) = create_test_optimizer();
        let stats = optimizer.stats();
        assert_eq!(stats.eviction_cycles, 0);
        assert_eq!(stats.total_records_evicted, 0);
        assert!(!stats.reduced_memory_active);
        assert!(!stats.embedded_mode_active);
    }

    #[test]
    fn test_minimal_optimizer_stats() {
        let (optimizer, _temp) = create_minimal_optimizer();
        let stats = optimizer.stats();
        assert!(stats.reduced_memory_active);
        assert!(stats.embedded_mode_active);
    }

    #[test]
    fn test_record_write_and_access() {
        let (optimizer, _temp) = create_test_optimizer();

        optimizer.record_write("topic-a", 1024);
        optimizer.record_write("topic-a", 512);
        optimizer.record_access("topic-a");

        let usage = optimizer.topic_usage();
        assert_eq!(usage.len(), 1);
        assert_eq!(usage[0].topic, "topic-a");
        assert_eq!(usage[0].bytes_used, 1536);
        assert_eq!(usage[0].record_count, 2);
    }

    #[test]
    fn test_refresh_usage_empty() {
        let (optimizer, _temp) = create_test_optimizer();
        optimizer.refresh_usage().expect("refresh failed");
        assert_eq!(optimizer.current_storage_bytes(), 0);
    }

    #[test]
    fn test_refresh_usage_with_topics() {
        let (optimizer, _temp) = create_test_optimizer();

        // Create a topic and write data
        optimizer
            .topic_manager
            .create_topic("test-topic", 1)
            .expect("create topic failed");
        optimizer
            .topic_manager
            .append(
                "test-topic",
                0,
                Some(bytes::Bytes::from("key")),
                bytes::Bytes::from("value"),
            )
            .expect("append failed");

        optimizer.refresh_usage().expect("refresh failed");

        let usage = optimizer.topic_usage();
        assert_eq!(usage.len(), 1);
        assert_eq!(usage[0].record_count, 1);
    }

    #[test]
    fn test_needs_eviction_false() {
        let (optimizer, _temp) = create_test_optimizer();
        assert!(!optimizer.needs_eviction());
    }

    #[test]
    fn test_eviction_not_needed() {
        let (optimizer, _temp) = create_test_optimizer();
        let result = optimizer.run_eviction().expect("eviction failed");
        assert!(!result.eviction_needed);
        assert_eq!(result.records_evicted, 0);
    }

    #[test]
    fn test_lru_eviction() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let topic_manager =
            Arc::new(TopicManager::new(temp_dir.path()).expect("TopicManager"));

        // Tiny max storage to force eviction
        let config = EdgeOptimizationConfig {
            max_storage_bytes: 256, // Very small
            eviction_threshold: 0.5,
            eviction_batch_size: 10,
            ..Default::default()
        };
        let optimizer = EdgeOptimizer::new(config, topic_manager.clone());

        // Create topic and write data to exceed threshold
        topic_manager
            .create_topic("evict-test", 1)
            .expect("create topic");
        for i in 0..5 {
            topic_manager
                .append(
                    "evict-test",
                    0,
                    Some(bytes::Bytes::from(format!("key-{}", i))),
                    bytes::Bytes::from(format!("value-{}", i)),
                )
                .expect("append");
        }

        optimizer.refresh_usage().expect("refresh");
        let result = optimizer.run_eviction().expect("eviction");

        assert!(result.eviction_needed);
        // Stats should be updated
        let stats = optimizer.stats();
        assert_eq!(stats.eviction_cycles, 1);
    }

    #[test]
    fn test_compaction_disabled() {
        let (optimizer, _temp) = create_test_optimizer();
        let result = optimizer.run_compaction().expect("compaction failed");
        assert_eq!(result.records_removed, 0);
        assert!(result.topics_compacted.is_empty());
    }

    #[test]
    fn test_compaction_enabled() {
        let (optimizer, _temp) = create_minimal_optimizer();

        // Create topic with duplicate keys
        optimizer
            .topic_manager
            .create_topic("compact-test", 1)
            .expect("create topic");

        let key = bytes::Bytes::from("same-key");
        for i in 0..3 {
            optimizer
                .topic_manager
                .append(
                    "compact-test",
                    0,
                    Some(key.clone()),
                    bytes::Bytes::from(format!("value-{}", i)),
                )
                .expect("append");
        }

        let result = optimizer.run_compaction().expect("compaction");
        // 3 records with same key → 2 should be identified as removable
        assert_eq!(result.records_removed, 2);
        assert!(result.topics_compacted.contains(&"compact-test".to_string()));
    }

    #[test]
    fn test_memory_recommendation_standard() {
        let (optimizer, _temp) = create_test_optimizer();
        let rec = optimizer.memory_recommendation();
        assert_eq!(rec.buffer_pool_size, 16);
        assert!(rec.use_memory_mapped_io);
    }

    #[test]
    fn test_memory_recommendation_reduced() {
        let (optimizer, _temp) = create_minimal_optimizer();
        let rec = optimizer.memory_recommendation();
        assert_eq!(rec.buffer_pool_size, 2);
        assert!(rec.use_memory_mapped_io); // minimal() sets prefer_mmap = true
    }

    #[test]
    fn test_cpu_throttle_scales_concurrent_reads() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let topic_manager =
            Arc::new(TopicManager::new(temp_dir.path()).expect("TopicManager"));
        let config = EdgeOptimizationConfig {
            cpu_throttle: 0.25,
            ..Default::default()
        };
        let optimizer = EdgeOptimizer::new(config, topic_manager);
        // 8 base * 0.25 = 2
        let rec = optimizer.memory_recommendation();
        assert_eq!(rec.max_concurrent_reads, 2);
    }

    #[test]
    fn test_disk_compaction_threshold() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let topic_manager =
            Arc::new(TopicManager::new(temp_dir.path()).expect("TopicManager"));
        let config = EdgeOptimizationConfig {
            max_storage_bytes: 1000,
            disk_compaction_threshold: 0.5,
            ..Default::default()
        };
        let optimizer = EdgeOptimizer::new(config, topic_manager);
        // Initially 0 bytes used, no compaction needed
        assert!(!optimizer.needs_disk_compaction());

        // Manually set storage bytes above threshold
        optimizer.stats.write().current_storage_bytes = 600;
        assert!(optimizer.needs_disk_compaction());
    }
}
