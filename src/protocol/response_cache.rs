//! Response cache for zero-copy fetch responses
//!
//! This module provides a file-backed response cache that enables sendfile()
//! for repeated fetch requests. Cached responses are stored as files that can
//! be sent directly to clients using zero-copy system calls.
//!
//! The cache is designed for fetch-heavy workloads where the same data may
//! be requested multiple times (e.g., consumer groups reading in parallel).

use parking_lot::RwLock;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Key identifying a cached fetch response
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct CacheKey {
    /// Topic name
    pub topic: String,
    /// Partition index
    pub partition: i32,
    /// Starting offset of the fetch
    pub start_offset: i64,
    /// Maximum bytes requested
    pub max_bytes: i32,
}

impl CacheKey {
    /// Create a new cache key
    pub fn new(topic: String, partition: i32, start_offset: i64, max_bytes: i32) -> Self {
        Self {
            topic,
            partition,
            start_offset,
            max_bytes,
        }
    }

    /// Generate a filename for this cache key
    fn to_filename(&self) -> String {
        format!(
            "{}_{}_{}_{}",
            self.topic.replace(['/', '\\', ':'], "_"),
            self.partition,
            self.start_offset,
            self.max_bytes
        )
    }
}

/// A cached response stored on disk
#[derive(Debug)]
pub struct CachedResponse {
    /// Path to the cached response file
    pub file_path: PathBuf,
    /// Size of the cached data in bytes
    pub size: u64,
    /// When the cache entry was created
    pub created_at: Instant,
    /// Time-to-live for this entry
    pub ttl: Duration,
}

impl CachedResponse {
    /// Check if this cache entry has expired
    pub fn is_expired(&self) -> bool {
        self.created_at.elapsed() > self.ttl
    }

    /// Open the cached file for reading
    pub fn open(&self) -> io::Result<File> {
        File::open(&self.file_path)
    }
}

/// Response cache for sendfile optimization
///
/// Stores fetch responses as files that can be sent using zero-copy sendfile().
/// Uses LRU eviction when the cache reaches its size limit.
pub struct ResponseCache {
    /// Directory where cache files are stored
    cache_dir: PathBuf,
    /// Cache entries indexed by key
    entries: RwLock<HashMap<CacheKey, CachedResponse>>,
    /// Maximum total size of cached data in bytes
    max_size_bytes: u64,
    /// Current total size of cached data
    current_size: AtomicU64,
    /// Default TTL for cache entries
    ttl: Duration,
    /// Cache statistics
    stats: CacheStats,
}

/// Statistics for the response cache
#[derive(Debug, Default)]
pub struct CacheStats {
    /// Number of cache hits
    pub hits: AtomicU64,
    /// Number of cache misses
    pub misses: AtomicU64,
    /// Number of entries evicted
    pub evictions: AtomicU64,
    /// Number of entries expired
    pub expirations: AtomicU64,
    /// Total bytes served from cache
    pub bytes_served: AtomicU64,
}

impl CacheStats {
    /// Create new empty stats
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a cache hit
    pub fn record_hit(&self, bytes: u64) {
        self.hits.fetch_add(1, Ordering::Relaxed);
        self.bytes_served.fetch_add(bytes, Ordering::Relaxed);
        #[cfg(feature = "metrics")]
        metrics::counter!("streamline_zerocopy_response_cache_hits").increment(1);
    }

    /// Record a cache miss
    pub fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
        #[cfg(feature = "metrics")]
        metrics::counter!("streamline_zerocopy_response_cache_misses").increment(1);
    }

    /// Record an eviction
    pub fn record_eviction(&self) {
        self.evictions.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an expiration
    pub fn record_expiration(&self) {
        self.expirations.fetch_add(1, Ordering::Relaxed);
    }

    /// Get the hit rate as a ratio (0.0 to 1.0)
    pub fn hit_rate(&self) -> f64 {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }
}

impl ResponseCache {
    /// Create a new response cache
    ///
    /// # Arguments
    /// * `cache_dir` - Directory to store cache files
    /// * `max_size_bytes` - Maximum total size of cached data
    /// * `ttl_secs` - Time-to-live for cache entries in seconds
    ///
    /// # Errors
    /// Returns error if the cache directory cannot be created
    pub fn new(cache_dir: PathBuf, max_size_bytes: u64, ttl_secs: u64) -> io::Result<Self> {
        // Create cache directory if it doesn't exist
        fs::create_dir_all(&cache_dir)?;

        Ok(Self {
            cache_dir,
            entries: RwLock::new(HashMap::new()),
            max_size_bytes,
            current_size: AtomicU64::new(0),
            ttl: Duration::from_secs(ttl_secs),
            stats: CacheStats::new(),
        })
    }

    /// Get a cached response by key
    ///
    /// Returns `None` if the key is not in the cache or has expired.
    pub fn get(&self, key: &CacheKey) -> Option<CachedResponse> {
        let entries = self.entries.read();

        if let Some(entry) = entries.get(key) {
            if entry.is_expired() {
                // Drop read lock, acquire write lock to remove expired entry
                drop(entries);
                self.remove_expired(key);
                self.stats.record_miss();
                self.stats.record_expiration();
                return None;
            }

            self.stats.record_hit(entry.size);

            // Return a new CachedResponse with the same data
            Some(CachedResponse {
                file_path: entry.file_path.clone(),
                size: entry.size,
                created_at: entry.created_at,
                ttl: entry.ttl,
            })
        } else {
            self.stats.record_miss();
            None
        }
    }

    /// Store a response in the cache
    ///
    /// # Arguments
    /// * `key` - The cache key
    /// * `data` - The response data to cache
    ///
    /// # Returns
    /// The cached response entry, or error if caching failed
    ///
    /// # Thread Safety
    /// Uses atomic CAS to reserve space before writing, preventing TOCTOU races
    /// where multiple threads could simultaneously pass the size check and exceed
    /// the max cache size.
    pub fn put(&self, key: CacheKey, data: &[u8]) -> io::Result<CachedResponse> {
        let size = data.len() as u64;

        // Atomically reserve space using CAS loop
        // This prevents TOCTOU race where multiple threads pass size check simultaneously
        loop {
            let current = self.current_size.load(Ordering::Acquire);

            if current + size > self.max_size_bytes {
                // Need to evict to make room
                if !self.evict_oldest() {
                    // No entries to evict and we're over limit - fail gracefully
                    // This shouldn't happen in practice if eviction is working correctly
                    return Err(io::Error::other(
                        "Cache is full and no entries available for eviction",
                    ));
                }
                // Retry after eviction
                continue;
            }

            // Try to atomically reserve the space
            match self.current_size.compare_exchange_weak(
                current,
                current + size,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,     // Successfully reserved space
                Err(_) => continue, // Another thread modified size, retry
            }
        }

        // Space is now reserved - write the file
        // Generate file path
        let file_path = self.cache_dir.join(key.to_filename());

        // Write data to file - if this fails, release the reserved space
        let write_result = (|| -> io::Result<()> {
            let mut file = File::create(&file_path)?;
            file.write_all(data)?;
            file.sync_all()?;
            Ok(())
        })();

        if let Err(e) = write_result {
            // Release the reserved space on failure
            self.current_size.fetch_sub(size, Ordering::Release);
            return Err(e);
        }

        let entry = CachedResponse {
            file_path: file_path.clone(),
            size,
            created_at: Instant::now(),
            ttl: self.ttl,
        };

        // Store in cache
        {
            let mut entries = self.entries.write();
            // If key already exists, subtract its size (we already added new size above)
            if let Some(old) = entries.remove(&key) {
                self.current_size.fetch_sub(old.size, Ordering::Release);
                let _ = fs::remove_file(&old.file_path);
            }
            entries.insert(
                key,
                CachedResponse {
                    file_path,
                    size,
                    created_at: entry.created_at,
                    ttl: entry.ttl,
                },
            );
        }

        Ok(entry)
    }

    /// Remove a specific key from the cache
    pub fn remove(&self, key: &CacheKey) -> bool {
        let mut entries = self.entries.write();
        if let Some(entry) = entries.remove(key) {
            self.current_size.fetch_sub(entry.size, Ordering::Relaxed);
            let _ = fs::remove_file(&entry.file_path);
            true
        } else {
            false
        }
    }

    /// Clear all entries from the cache
    pub fn clear(&self) {
        let mut entries = self.entries.write();
        for (_, entry) in entries.drain() {
            let _ = fs::remove_file(&entry.file_path);
        }
        self.current_size.store(0, Ordering::Relaxed);
    }

    /// Get cache statistics
    pub fn stats(&self) -> &CacheStats {
        &self.stats
    }

    /// Get the current cache size in bytes
    pub fn current_size(&self) -> u64 {
        self.current_size.load(Ordering::Relaxed)
    }

    /// Get the number of entries in the cache
    pub fn entry_count(&self) -> usize {
        self.entries.read().len()
    }

    /// Remove an expired entry
    fn remove_expired(&self, key: &CacheKey) {
        let mut entries = self.entries.write();
        if let Some(entry) = entries.remove(key) {
            self.current_size.fetch_sub(entry.size, Ordering::Relaxed);
            let _ = fs::remove_file(&entry.file_path);
        }
    }

    /// Evict the oldest entry from the cache
    ///
    /// Returns true if an entry was evicted, false if the cache is empty
    fn evict_oldest(&self) -> bool {
        let mut entries = self.entries.write();

        // Find the oldest entry
        let oldest_key = entries
            .iter()
            .min_by_key(|(_, v)| v.created_at)
            .map(|(k, _)| k.clone());

        if let Some(key) = oldest_key {
            if let Some(entry) = entries.remove(&key) {
                self.current_size.fetch_sub(entry.size, Ordering::Relaxed);
                let _ = fs::remove_file(&entry.file_path);
                self.stats.record_eviction();
                return true;
            }
        }

        false
    }

    /// Clean up expired entries
    pub fn cleanup_expired(&self) {
        let expired_keys: Vec<CacheKey> = {
            let entries = self.entries.read();
            entries
                .iter()
                .filter(|(_, v)| v.is_expired())
                .map(|(k, _)| k.clone())
                .collect()
        };

        for key in expired_keys {
            self.remove_expired(&key);
            self.stats.record_expiration();
        }
    }
}

impl Drop for ResponseCache {
    fn drop(&mut self) {
        // Clean up cache files on drop
        self.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_cache_key_filename() {
        let key = CacheKey::new("my-topic".to_string(), 0, 100, 1024);
        let filename = key.to_filename();
        assert_eq!(filename, "my-topic_0_100_1024");
    }

    #[test]
    fn test_cache_key_filename_special_chars() {
        let key = CacheKey::new("my/topic:name".to_string(), 1, 200, 2048);
        let filename = key.to_filename();
        assert_eq!(filename, "my_topic_name_1_200_2048");
    }

    #[test]
    fn test_cache_put_and_get() {
        let temp_dir = tempdir().unwrap();
        let cache = ResponseCache::new(temp_dir.path().to_path_buf(), 1024 * 1024, 60).unwrap();

        let key = CacheKey::new("test-topic".to_string(), 0, 0, 1024);
        let data = b"test response data";

        // Put data in cache
        let entry = cache.put(key.clone(), data).unwrap();
        assert_eq!(entry.size, data.len() as u64);

        // Get data from cache
        let cached = cache.get(&key).unwrap();
        assert_eq!(cached.size, data.len() as u64);

        // Verify file contents
        let file_data = fs::read(&cached.file_path).unwrap();
        assert_eq!(file_data, data);
    }

    #[test]
    fn test_cache_miss() {
        let temp_dir = tempdir().unwrap();
        let cache = ResponseCache::new(temp_dir.path().to_path_buf(), 1024 * 1024, 60).unwrap();

        let key = CacheKey::new("nonexistent".to_string(), 0, 0, 1024);
        assert!(cache.get(&key).is_none());
        assert_eq!(cache.stats().misses.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_cache_eviction() {
        let temp_dir = tempdir().unwrap();
        // Small cache that can only hold a few entries
        let cache = ResponseCache::new(temp_dir.path().to_path_buf(), 100, 60).unwrap();

        // Put entries that exceed cache size
        for i in 0..5 {
            let key = CacheKey::new(format!("topic-{}", i), 0, 0, 1024);
            let data = vec![0u8; 30]; // Each entry is 30 bytes
            cache.put(key, &data).unwrap();
        }

        // Some entries should have been evicted
        assert!(cache.stats().evictions.load(Ordering::Relaxed) > 0);
        assert!(cache.current_size() <= 100);
    }

    #[test]
    fn test_cache_remove() {
        let temp_dir = tempdir().unwrap();
        let cache = ResponseCache::new(temp_dir.path().to_path_buf(), 1024 * 1024, 60).unwrap();

        let key = CacheKey::new("test-topic".to_string(), 0, 0, 1024);
        let data = b"test data";

        cache.put(key.clone(), data).unwrap();
        assert!(cache.get(&key).is_some());

        cache.remove(&key);
        assert!(cache.get(&key).is_none());
    }

    #[test]
    fn test_cache_clear() {
        let temp_dir = tempdir().unwrap();
        let cache = ResponseCache::new(temp_dir.path().to_path_buf(), 1024 * 1024, 60).unwrap();

        // Add some entries
        for i in 0..3 {
            let key = CacheKey::new(format!("topic-{}", i), 0, 0, 1024);
            cache.put(key, b"test data").unwrap();
        }

        assert_eq!(cache.entry_count(), 3);

        cache.clear();

        assert_eq!(cache.entry_count(), 0);
        assert_eq!(cache.current_size(), 0);
    }

    #[test]
    fn test_cache_stats() {
        let temp_dir = tempdir().unwrap();
        let cache = ResponseCache::new(temp_dir.path().to_path_buf(), 1024 * 1024, 60).unwrap();

        let key = CacheKey::new("test-topic".to_string(), 0, 0, 1024);
        let data = b"test data";

        // Miss
        cache.get(&key);
        assert_eq!(cache.stats().misses.load(Ordering::Relaxed), 1);
        assert_eq!(cache.stats().hits.load(Ordering::Relaxed), 0);

        // Put
        cache.put(key.clone(), data).unwrap();

        // Hit
        cache.get(&key);
        assert_eq!(cache.stats().hits.load(Ordering::Relaxed), 1);

        // Hit rate should be 0.5
        assert!((cache.stats().hit_rate() - 0.5).abs() < 0.001);
    }
}
