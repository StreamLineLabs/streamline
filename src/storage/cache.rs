//! Read Caching (LRU) for Streamline
//!
//! This module provides an LRU (Least Recently Used) cache for reducing disk I/O
//! when reading hot data. The cache stores segment data keyed by topic, partition,
//! and offset range.
//!
//! ## Example
//!
//! ```
//! use streamline::storage::cache::{SegmentCache, CacheConfig};
//! use bytes::Bytes;
//!
//! let config = CacheConfig::default();
//! let cache = SegmentCache::new(config);
//!
//! // Cache some data
//! let data = Bytes::from(vec![1, 2, 3, 4, 5]);
//! cache.put("my-topic", 0, 0, 100, data);
//!
//! // Retrieve cached data
//! if let Some(cached) = cache.get("my-topic", 0, 50) {
//!     // Use cached data
//! }
//! ```

use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Configuration for the segment cache
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Maximum cache size in bytes
    pub max_size_bytes: usize,
    /// Maximum number of entries
    pub max_entries: usize,
    /// Time-to-live for cache entries
    pub ttl: Duration,
    /// Whether to enable statistics tracking
    pub enable_stats: bool,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_size_bytes: 256 * 1024 * 1024, // 256MB default
            max_entries: 10_000,
            ttl: Duration::from_secs(300), // 5 minutes
            enable_stats: true,
        }
    }
}

impl CacheConfig {
    /// Create a config for small memory footprint
    pub fn small() -> Self {
        Self {
            max_size_bytes: 64 * 1024 * 1024, // 64MB
            max_entries: 1_000,
            ttl: Duration::from_secs(120), // 2 minutes
            enable_stats: true,
        }
    }

    /// Create a config for large memory footprint
    pub fn large() -> Self {
        Self {
            max_size_bytes: 1024 * 1024 * 1024, // 1GB
            max_entries: 100_000,
            ttl: Duration::from_secs(600), // 10 minutes
            enable_stats: true,
        }
    }
}

/// Key for cache entries
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CacheKey {
    /// Topic name
    pub topic: String,
    /// Partition ID
    pub partition: i32,
    /// Start offset of cached range
    pub start_offset: i64,
    /// End offset of cached range
    pub end_offset: i64,
}

impl CacheKey {
    /// Create a new cache key
    pub fn new(
        topic: impl Into<String>,
        partition: i32,
        start_offset: i64,
        end_offset: i64,
    ) -> Self {
        Self {
            topic: topic.into(),
            partition,
            start_offset,
            end_offset,
        }
    }

    /// Check if this key contains the given offset
    pub fn contains_offset(&self, offset: i64) -> bool {
        offset >= self.start_offset && offset < self.end_offset
    }
}

/// Entry in the cache
#[derive(Debug, Clone)]
struct CacheEntry {
    /// Cached data
    data: Bytes,
    /// Size of the entry in bytes
    size: usize,
    /// Last access time
    last_access: Instant,
    /// Creation time
    created_at: Instant,
    /// Access count
    access_count: u64,
}

impl CacheEntry {
    fn new(data: Bytes) -> Self {
        let size = data.len();
        let now = Instant::now();
        Self {
            data,
            size,
            last_access: now,
            created_at: now,
            access_count: 1,
        }
    }

    fn touch(&mut self) {
        self.last_access = Instant::now();
        self.access_count += 1;
    }

    fn is_expired(&self, ttl: Duration) -> bool {
        self.created_at.elapsed() > ttl
    }
}

/// Statistics for the cache
#[derive(Debug, Clone, Default)]
pub struct CacheStats {
    /// Total number of cache hits
    pub hits: u64,
    /// Total number of cache misses
    pub misses: u64,
    /// Current number of entries
    pub entry_count: usize,
    /// Current size in bytes
    pub size_bytes: usize,
    /// Total number of evictions
    pub evictions: u64,
    /// Total number of expirations
    pub expirations: u64,
    /// Hit rate percentage
    pub hit_rate: f64,
}

impl CacheStats {
    /// Calculate hit rate from hits and misses
    pub fn calculate_hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            (self.hits as f64 / total as f64) * 100.0
        }
    }
}

/// Internal cache state
struct CacheState {
    /// Cache entries keyed by CacheKey
    entries: HashMap<CacheKey, CacheEntry>,
    /// Current total size in bytes
    current_size: usize,
    /// Eviction count
    evictions: u64,
    /// Expiration count
    expirations: u64,
}

impl CacheState {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
            current_size: 0,
            evictions: 0,
            expirations: 0,
        }
    }
}

/// LRU segment cache for reducing disk I/O
pub struct SegmentCache {
    config: CacheConfig,
    state: RwLock<CacheState>,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl SegmentCache {
    /// Create a new segment cache with the given configuration
    pub fn new(config: CacheConfig) -> Self {
        Self {
            config,
            state: RwLock::new(CacheState::new()),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Create a new segment cache with default configuration
    pub fn with_defaults() -> Self {
        Self::new(CacheConfig::default())
    }

    /// Put data into the cache
    pub fn put(
        &self,
        topic: &str,
        partition: i32,
        start_offset: i64,
        end_offset: i64,
        data: Bytes,
    ) {
        let key = CacheKey::new(topic, partition, start_offset, end_offset);
        let entry = CacheEntry::new(data);
        let entry_size = entry.size;

        // Skip if entry is larger than max cache size
        if entry_size > self.config.max_size_bytes {
            return;
        }

        let mut state = self.state.write();

        // Evict expired entries first
        self.evict_expired(&mut state);

        // Evict LRU entries if needed
        while state.current_size + entry_size > self.config.max_size_bytes
            || state.entries.len() >= self.config.max_entries
        {
            if !self.evict_lru(&mut state) {
                break;
            }
        }

        // Remove existing entry if present
        if let Some(old_entry) = state.entries.remove(&key) {
            state.current_size -= old_entry.size;
        }

        // Insert new entry
        state.current_size += entry_size;
        state.entries.insert(key, entry);
    }

    /// Get data from the cache for the given offset
    /// Returns None if not found or expired
    pub fn get(&self, topic: &str, partition: i32, offset: i64) -> Option<Bytes> {
        let mut state = self.state.write();

        // Find entry containing the offset
        let key = state
            .entries
            .keys()
            .find(|k| k.topic == topic && k.partition == partition && k.contains_offset(offset))
            .cloned();

        if let Some(key) = key {
            if let Some(entry) = state.entries.get_mut(&key) {
                // Check if expired
                if entry.is_expired(self.config.ttl) {
                    let size = entry.size;
                    state.entries.remove(&key);
                    state.current_size -= size;
                    state.expirations += 1;
                    self.misses.fetch_add(1, Ordering::Relaxed);
                    return None;
                }

                // Update access time
                entry.touch();
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Some(entry.data.clone());
            }
        }

        self.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// Get data for a specific cache key
    pub fn get_by_key(&self, key: &CacheKey) -> Option<Bytes> {
        let mut state = self.state.write();

        if let Some(entry) = state.entries.get_mut(key) {
            // Check if expired
            if entry.is_expired(self.config.ttl) {
                let size = entry.size;
                let key = key.clone();
                state.entries.remove(&key);
                state.current_size -= size;
                state.expirations += 1;
                self.misses.fetch_add(1, Ordering::Relaxed);
                return None;
            }

            entry.touch();
            self.hits.fetch_add(1, Ordering::Relaxed);
            return Some(entry.data.clone());
        }

        self.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// Remove an entry from the cache
    pub fn remove(&self, topic: &str, partition: i32, start_offset: i64, end_offset: i64) {
        let key = CacheKey::new(topic, partition, start_offset, end_offset);
        let mut state = self.state.write();

        if let Some(entry) = state.entries.remove(&key) {
            state.current_size -= entry.size;
        }
    }

    /// Invalidate all entries for a topic-partition
    pub fn invalidate_partition(&self, topic: &str, partition: i32) {
        let mut state = self.state.write();

        let keys_to_remove: Vec<_> = state
            .entries
            .keys()
            .filter(|k| k.topic == topic && k.partition == partition)
            .cloned()
            .collect();

        for key in keys_to_remove {
            if let Some(entry) = state.entries.remove(&key) {
                state.current_size -= entry.size;
            }
        }
    }

    /// Invalidate all entries for a topic
    pub fn invalidate_topic(&self, topic: &str) {
        let mut state = self.state.write();

        let keys_to_remove: Vec<_> = state
            .entries
            .keys()
            .filter(|k| k.topic == topic)
            .cloned()
            .collect();

        for key in keys_to_remove {
            if let Some(entry) = state.entries.remove(&key) {
                state.current_size -= entry.size;
            }
        }
    }

    /// Clear all entries from the cache
    pub fn clear(&self) {
        let mut state = self.state.write();
        state.entries.clear();
        state.current_size = 0;
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        let state = self.state.read();
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);

        let mut stats = CacheStats {
            hits,
            misses,
            entry_count: state.entries.len(),
            size_bytes: state.current_size,
            evictions: state.evictions,
            expirations: state.expirations,
            hit_rate: 0.0,
        };
        stats.hit_rate = stats.calculate_hit_rate();
        stats
    }

    /// Get the configuration
    pub fn config(&self) -> &CacheConfig {
        &self.config
    }

    /// Evict expired entries
    fn evict_expired(&self, state: &mut CacheState) {
        let ttl = self.config.ttl;
        let expired_keys: Vec<_> = state
            .entries
            .iter()
            .filter(|(_, v)| v.is_expired(ttl))
            .map(|(k, _)| k.clone())
            .collect();

        for key in expired_keys {
            if let Some(entry) = state.entries.remove(&key) {
                state.current_size -= entry.size;
                state.expirations += 1;
            }
        }
    }

    /// Evict the least recently used entry
    /// Returns true if an entry was evicted, false if cache is empty
    fn evict_lru(&self, state: &mut CacheState) -> bool {
        if state.entries.is_empty() {
            return false;
        }

        // Find the entry with the oldest last_access time
        let lru_key = state
            .entries
            .iter()
            .min_by_key(|(_, v)| v.last_access)
            .map(|(k, _)| k.clone());

        if let Some(key) = lru_key {
            if let Some(entry) = state.entries.remove(&key) {
                state.current_size -= entry.size;
                state.evictions += 1;
                return true;
            }
        }

        false
    }
}

impl Default for SegmentCache {
    fn default() -> Self {
        Self::with_defaults()
    }
}

/// Shared segment cache that can be cloned
pub type SharedSegmentCache = Arc<SegmentCache>;

/// Builder for SegmentCache
#[derive(Debug, Clone)]
pub struct SegmentCacheBuilder {
    config: CacheConfig,
}

impl SegmentCacheBuilder {
    /// Create a new builder with default config
    pub fn new() -> Self {
        Self {
            config: CacheConfig::default(),
        }
    }

    /// Set maximum size in bytes
    pub fn max_size_bytes(mut self, size: usize) -> Self {
        self.config.max_size_bytes = size;
        self
    }

    /// Set maximum number of entries
    pub fn max_entries(mut self, count: usize) -> Self {
        self.config.max_entries = count;
        self
    }

    /// Set time-to-live for entries
    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.config.ttl = ttl;
        self
    }

    /// Enable or disable statistics tracking
    pub fn enable_stats(mut self, enable: bool) -> Self {
        self.config.enable_stats = enable;
        self
    }

    /// Build the SegmentCache
    pub fn build(self) -> SegmentCache {
        SegmentCache::new(self.config)
    }
}

impl Default for SegmentCacheBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = CacheConfig::default();
        assert_eq!(config.max_size_bytes, 256 * 1024 * 1024);
        assert_eq!(config.max_entries, 10_000);
        assert_eq!(config.ttl, Duration::from_secs(300));
    }

    #[test]
    fn test_small_config() {
        let config = CacheConfig::small();
        assert_eq!(config.max_size_bytes, 64 * 1024 * 1024);
    }

    #[test]
    fn test_large_config() {
        let config = CacheConfig::large();
        assert_eq!(config.max_size_bytes, 1024 * 1024 * 1024);
    }

    #[test]
    fn test_cache_put_and_get() {
        let cache = SegmentCache::with_defaults();
        let data = Bytes::from("hello world");

        cache.put("topic1", 0, 0, 100, data.clone());

        let result = cache.get("topic1", 0, 50);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), data);
    }

    #[test]
    fn test_cache_miss() {
        let cache = SegmentCache::with_defaults();

        let result = cache.get("nonexistent", 0, 0);
        assert!(result.is_none());

        let stats = cache.stats();
        assert_eq!(stats.misses, 1);
    }

    #[test]
    fn test_cache_hit_stats() {
        let cache = SegmentCache::with_defaults();
        let data = Bytes::from("test data");

        cache.put("topic1", 0, 0, 100, data);
        cache.get("topic1", 0, 50);
        cache.get("topic1", 0, 50);

        let stats = cache.stats();
        assert_eq!(stats.hits, 2);
        assert_eq!(stats.entry_count, 1);
    }

    #[test]
    fn test_cache_eviction() {
        let config = CacheConfig {
            max_size_bytes: 100,
            max_entries: 2,
            ttl: Duration::from_secs(300),
            enable_stats: true,
        };
        let cache = SegmentCache::new(config);

        // Add entries
        cache.put("topic1", 0, 0, 10, Bytes::from(vec![0u8; 30]));
        cache.put("topic1", 0, 10, 20, Bytes::from(vec![0u8; 30]));

        // This should trigger eviction
        cache.put("topic1", 0, 20, 30, Bytes::from(vec![0u8; 50]));

        let stats = cache.stats();
        assert!(stats.evictions > 0 || stats.entry_count <= 2);
    }

    #[test]
    fn test_cache_invalidate_partition() {
        let cache = SegmentCache::with_defaults();

        cache.put("topic1", 0, 0, 100, Bytes::from("data1"));
        cache.put("topic1", 1, 0, 100, Bytes::from("data2"));
        cache.put("topic1", 0, 100, 200, Bytes::from("data3"));

        cache.invalidate_partition("topic1", 0);

        assert!(cache.get("topic1", 0, 50).is_none());
        assert!(cache.get("topic1", 1, 50).is_some());
    }

    #[test]
    fn test_cache_invalidate_topic() {
        let cache = SegmentCache::with_defaults();

        cache.put("topic1", 0, 0, 100, Bytes::from("data1"));
        cache.put("topic1", 1, 0, 100, Bytes::from("data2"));
        cache.put("topic2", 0, 0, 100, Bytes::from("data3"));

        cache.invalidate_topic("topic1");

        assert!(cache.get("topic1", 0, 50).is_none());
        assert!(cache.get("topic1", 1, 50).is_none());
        assert!(cache.get("topic2", 0, 50).is_some());
    }

    #[test]
    fn test_cache_clear() {
        let cache = SegmentCache::with_defaults();

        cache.put("topic1", 0, 0, 100, Bytes::from("data1"));
        cache.put("topic2", 0, 0, 100, Bytes::from("data2"));

        cache.clear();

        let stats = cache.stats();
        assert_eq!(stats.entry_count, 0);
        assert_eq!(stats.size_bytes, 0);
    }

    #[test]
    fn test_cache_key_contains_offset() {
        let key = CacheKey::new("topic", 0, 100, 200);

        assert!(!key.contains_offset(99));
        assert!(key.contains_offset(100));
        assert!(key.contains_offset(150));
        assert!(!key.contains_offset(200));
    }

    #[test]
    fn test_cache_builder() {
        let cache = SegmentCacheBuilder::new()
            .max_size_bytes(128 * 1024 * 1024)
            .max_entries(5000)
            .ttl(Duration::from_secs(60))
            .build();

        assert_eq!(cache.config().max_size_bytes, 128 * 1024 * 1024);
        assert_eq!(cache.config().max_entries, 5000);
        assert_eq!(cache.config().ttl, Duration::from_secs(60));
    }

    #[test]
    fn test_hit_rate_calculation() {
        let mut stats = CacheStats {
            hits: 80,
            misses: 20,
            ..Default::default()
        };
        stats.hit_rate = stats.calculate_hit_rate();
        assert!((stats.hit_rate - 80.0).abs() < 0.01);
    }

    #[test]
    fn test_cache_expiration() {
        let config = CacheConfig {
            max_size_bytes: 1024 * 1024,
            max_entries: 100,
            ttl: Duration::from_millis(10),
            enable_stats: true,
        };
        let cache = SegmentCache::new(config);

        cache.put("topic1", 0, 0, 100, Bytes::from("data"));

        // Wait for expiration
        std::thread::sleep(Duration::from_millis(20));

        let result = cache.get("topic1", 0, 50);
        assert!(result.is_none());

        let stats = cache.stats();
        assert_eq!(stats.expirations, 1);
    }

    #[test]
    fn test_cache_remove() {
        let cache = SegmentCache::with_defaults();

        cache.put("topic1", 0, 0, 100, Bytes::from("data"));
        assert!(cache.get("topic1", 0, 50).is_some());

        cache.remove("topic1", 0, 0, 100);
        assert!(cache.get("topic1", 0, 50).is_none());
    }

    #[test]
    fn test_skip_oversized_entry() {
        let config = CacheConfig {
            max_size_bytes: 100,
            max_entries: 100,
            ttl: Duration::from_secs(300),
            enable_stats: true,
        };
        let cache = SegmentCache::new(config);

        // This entry is larger than max cache size
        cache.put("topic1", 0, 0, 100, Bytes::from(vec![0u8; 200]));

        let stats = cache.stats();
        assert_eq!(stats.entry_count, 0);
    }

    #[test]
    fn test_get_by_key() {
        let cache = SegmentCache::with_defaults();
        let data = Bytes::from("test data");

        cache.put("topic1", 0, 0, 100, data.clone());

        let key = CacheKey::new("topic1", 0, 0, 100);
        let result = cache.get_by_key(&key);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), data);
    }
}
