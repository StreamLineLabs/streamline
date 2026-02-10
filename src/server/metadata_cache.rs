//! Metadata cache for server-side performance optimization.
//!
//! This module provides TTL-based caching for cluster metadata to reduce
//! lock contention and improve response times for frequently accessed data.

use parking_lot::RwLock;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Configuration for the metadata cache.
#[derive(Debug, Clone)]
pub struct MetadataCacheConfig {
    /// Time-to-live for cached cluster overview data.
    pub cluster_overview_ttl: Duration,
    /// Time-to-live for cached topic list data.
    pub topic_list_ttl: Duration,
}

impl Default for MetadataCacheConfig {
    fn default() -> Self {
        Self {
            cluster_overview_ttl: Duration::from_secs(2),
            topic_list_ttl: Duration::from_secs(2),
        }
    }
}

/// A cached value with timestamp for TTL checking.
#[derive(Debug)]
struct CachedValue<T> {
    value: T,
    cached_at: Instant,
    ttl: Duration,
}

impl<T: Clone> CachedValue<T> {
    fn new(value: T, ttl: Duration) -> Self {
        Self {
            value,
            cached_at: Instant::now(),
            ttl,
        }
    }

    fn is_valid(&self) -> bool {
        self.cached_at.elapsed() < self.ttl
    }

    fn get(&self) -> Option<T> {
        if self.is_valid() {
            Some(self.value.clone())
        } else {
            None
        }
    }
}

/// Thread-safe metadata cache for cluster information.
///
/// This cache stores frequently accessed metadata with configurable TTL
/// to reduce lock contention on the underlying storage.
pub struct MetadataCache {
    config: MetadataCacheConfig,
    cluster_overview: RwLock<Option<CachedValue<CachedClusterOverview>>>,
    topic_stats: RwLock<Option<CachedValue<Vec<CachedTopicStats>>>>,
}

/// Cached cluster overview data.
#[derive(Debug, Clone)]
pub struct CachedClusterOverview {
    pub cluster_id: String,
    pub controller_id: Option<u64>,
    pub broker_count: usize,
    pub topic_count: usize,
    pub partition_count: usize,
    pub online_partition_count: usize,
    pub offline_partition_count: usize,
    pub under_replicated_partitions: usize,
    pub total_messages: u64,
    pub total_bytes: u64,
}

/// Cached topic statistics.
#[derive(Debug, Clone)]
pub struct CachedTopicStats {
    pub name: String,
    pub num_partitions: i32,
    pub replication_factor: i16,
    pub total_messages: u64,
}

impl MetadataCache {
    /// Create a new metadata cache with the given configuration.
    pub fn new(config: MetadataCacheConfig) -> Self {
        Self {
            config,
            cluster_overview: RwLock::new(None),
            topic_stats: RwLock::new(None),
        }
    }

    /// Create a new metadata cache wrapped in an Arc for sharing.
    pub fn new_shared(config: MetadataCacheConfig) -> Arc<Self> {
        Arc::new(Self::new(config))
    }

    /// Get the cached cluster overview if still valid.
    pub fn get_cluster_overview(&self) -> Option<CachedClusterOverview> {
        let cache = self.cluster_overview.read();
        cache.as_ref().and_then(|c| c.get())
    }

    /// Set the cached cluster overview.
    pub fn set_cluster_overview(&self, overview: CachedClusterOverview) {
        let mut cache = self.cluster_overview.write();
        *cache = Some(CachedValue::new(overview, self.config.cluster_overview_ttl));
    }

    /// Get the cached topic stats if still valid.
    pub fn get_topic_stats(&self) -> Option<Vec<CachedTopicStats>> {
        let cache = self.topic_stats.read();
        cache.as_ref().and_then(|c| c.get())
    }

    /// Set the cached topic stats.
    pub fn set_topic_stats(&self, stats: Vec<CachedTopicStats>) {
        let mut cache = self.topic_stats.write();
        *cache = Some(CachedValue::new(stats, self.config.topic_list_ttl));
    }

    /// Invalidate all caches. Call this when topics are created/deleted.
    pub fn invalidate_all(&self) {
        {
            let mut cache = self.cluster_overview.write();
            *cache = None;
        }
        {
            let mut cache = self.topic_stats.write();
            *cache = None;
        }
    }

    /// Invalidate just the cluster overview cache.
    pub fn invalidate_cluster_overview(&self) {
        let mut cache = self.cluster_overview.write();
        *cache = None;
    }

    /// Invalidate just the topic stats cache.
    pub fn invalidate_topic_stats(&self) {
        let mut cache = self.topic_stats.write();
        *cache = None;
    }
}

impl Default for MetadataCache {
    fn default() -> Self {
        Self::new(MetadataCacheConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;

    #[test]
    fn test_cache_ttl() {
        let config = MetadataCacheConfig {
            cluster_overview_ttl: Duration::from_millis(50),
            topic_list_ttl: Duration::from_millis(50),
        };
        let cache = MetadataCache::new(config);

        let overview = CachedClusterOverview {
            cluster_id: "test".to_string(),
            controller_id: Some(1),
            broker_count: 1,
            topic_count: 5,
            partition_count: 10,
            online_partition_count: 10,
            offline_partition_count: 0,
            under_replicated_partitions: 0,
            total_messages: 1000,
            total_bytes: 50000,
        };

        cache.set_cluster_overview(overview.clone());

        // Should be valid immediately
        assert!(cache.get_cluster_overview().is_some());

        // Wait for TTL to expire
        sleep(Duration::from_millis(60));

        // Should be expired
        assert!(cache.get_cluster_overview().is_none());
    }

    #[test]
    fn test_cache_invalidation() {
        let cache = MetadataCache::default();

        let overview = CachedClusterOverview {
            cluster_id: "test".to_string(),
            controller_id: Some(1),
            broker_count: 1,
            topic_count: 5,
            partition_count: 10,
            online_partition_count: 10,
            offline_partition_count: 0,
            under_replicated_partitions: 0,
            total_messages: 1000,
            total_bytes: 50000,
        };

        cache.set_cluster_overview(overview);
        assert!(cache.get_cluster_overview().is_some());

        cache.invalidate_all();
        assert!(cache.get_cluster_overview().is_none());
    }

    #[test]
    fn test_topic_stats_cache() {
        let cache = MetadataCache::default();

        let stats = vec![
            CachedTopicStats {
                name: "topic1".to_string(),
                num_partitions: 3,
                replication_factor: 1,
                total_messages: 100,
            },
            CachedTopicStats {
                name: "topic2".to_string(),
                num_partitions: 5,
                replication_factor: 1,
                total_messages: 200,
            },
        ];

        cache.set_topic_stats(stats.clone());
        let cached = cache.get_topic_stats().unwrap();
        assert_eq!(cached.len(), 2);
        assert_eq!(cached[0].name, "topic1");
    }
}
