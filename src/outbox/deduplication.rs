//! Deduplication Store
//!
//! Provides efficient deduplication for outbox messages to ensure exactly-once semantics.

use super::config::DeduplicationConfig;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Deduplication store
pub struct DeduplicationStore {
    /// Configuration
    config: DeduplicationConfig,
    /// Cache of seen message keys
    cache: Arc<RwLock<HashMap<String, i64>>>,
}

impl DeduplicationStore {
    /// Create a new deduplication store
    pub fn new(config: DeduplicationConfig) -> Self {
        Self {
            config,
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Check if a message key is a duplicate
    pub async fn is_duplicate(&self, key: &str) -> bool {
        if !self.config.enabled {
            return false;
        }

        let cache = self.cache.read().await;

        if let Some(timestamp) = cache.get(key) {
            let now = chrono::Utc::now().timestamp_millis();
            let window_ms = self.config.window_seconds as i64 * 1000;

            // Within deduplication window
            if now - timestamp < window_ms {
                return true;
            }
        }

        false
    }

    /// Record a message key
    pub async fn record(&self, key: &str) {
        if !self.config.enabled {
            return;
        }

        let mut cache = self.cache.write().await;

        // Evict if at capacity
        if cache.len() >= self.config.max_cache_size {
            self.evict_expired(&mut cache);
        }

        let timestamp = chrono::Utc::now().timestamp_millis();
        cache.insert(key.to_string(), timestamp);
    }

    /// Remove a key from the cache
    pub async fn remove(&self, key: &str) {
        let mut cache = self.cache.write().await;
        cache.remove(key);
    }

    /// Clear all entries
    pub async fn clear(&self) {
        let mut cache = self.cache.write().await;
        cache.clear();
    }

    /// Get cache size
    pub async fn cache_size(&self) -> usize {
        let cache = self.cache.read().await;
        cache.len()
    }

    /// Evict expired entries
    fn evict_expired(&self, cache: &mut HashMap<String, i64>) {
        let now = chrono::Utc::now().timestamp_millis();
        let window_ms = self.config.window_seconds as i64 * 1000;

        cache.retain(|_, timestamp| now - *timestamp < window_ms);
    }

    /// Run periodic eviction (should be called from a background task)
    pub async fn run_eviction(&self) {
        let mut cache = self.cache.write().await;
        self.evict_expired(&mut cache);
    }

    /// Start background eviction task
    pub fn start_eviction_task(self: Arc<Self>) {
        let store = self.clone();
        let interval = std::time::Duration::from_secs(self.config.eviction_interval_seconds);

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                store.run_eviction().await;
            }
        });
    }

    /// Check if deduplication is enabled
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Get configuration
    pub fn config(&self) -> &DeduplicationConfig {
        &self.config
    }
}

impl Default for DeduplicationStore {
    fn default() -> Self {
        Self::new(DeduplicationConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_deduplication_basic() {
        let store = DeduplicationStore::default();

        // First time - not a duplicate
        assert!(!store.is_duplicate("key1").await);

        // Record it
        store.record("key1").await;

        // Now it's a duplicate
        assert!(store.is_duplicate("key1").await);

        // Different key - not a duplicate
        assert!(!store.is_duplicate("key2").await);
    }

    #[tokio::test]
    async fn test_deduplication_disabled() {
        let config = DeduplicationConfig {
            enabled: false,
            ..Default::default()
        };
        let store = DeduplicationStore::new(config);

        store.record("key1").await;

        // Should never be duplicate when disabled
        assert!(!store.is_duplicate("key1").await);
    }

    #[tokio::test]
    async fn test_cache_size() {
        let store = DeduplicationStore::default();

        assert_eq!(store.cache_size().await, 0);

        store.record("key1").await;
        store.record("key2").await;
        store.record("key3").await;

        assert_eq!(store.cache_size().await, 3);

        store.remove("key2").await;

        assert_eq!(store.cache_size().await, 2);
    }

    #[tokio::test]
    async fn test_clear() {
        let store = DeduplicationStore::default();

        store.record("key1").await;
        store.record("key2").await;

        assert_eq!(store.cache_size().await, 2);

        store.clear().await;

        assert_eq!(store.cache_size().await, 0);
    }
}
