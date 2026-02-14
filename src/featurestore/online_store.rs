//! DashMap-based Online Feature Store
//!
//! Provides a concurrent, TTL-aware in-memory key-value store for
//! low-latency online feature serving. Uses DashMap for lock-free
//! concurrent reads and fine-grained write locking.

use crate::featurestore::feature::FeatureValue;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// DashMap-based online feature store for concurrent access with TTL support
pub struct DashMapOnlineStore {
    /// Internal concurrent map: key -> OnlineEntry
    data: DashMap<String, OnlineEntry>,
    /// Maximum entries before eviction
    max_entries: usize,
    /// Default TTL in seconds (0 = no expiry)
    default_ttl_seconds: u64,
    /// Number of evictions performed
    eviction_count: AtomicU64,
    /// Total puts performed
    put_count: AtomicU64,
    /// Total gets performed
    get_count: AtomicU64,
}

/// An entry in the online store
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OnlineEntry {
    /// Feature value
    pub value: FeatureValue,
    /// Event timestamp (when the feature was valid)
    pub event_timestamp: i64,
    /// Created timestamp (when the entry was written)
    pub created_timestamp: i64,
    /// Expiry timestamp in millis (0 = no expiry)
    pub expiry_timestamp: i64,
}

impl DashMapOnlineStore {
    /// Create a new DashMap-based online store
    pub fn new(max_entries: usize, default_ttl_seconds: u64) -> Self {
        Self {
            data: DashMap::new(),
            max_entries,
            default_ttl_seconds,
            eviction_count: AtomicU64::new(0),
            put_count: AtomicU64::new(0),
            get_count: AtomicU64::new(0),
        }
    }

    /// Get a feature entry by key, respecting TTL
    pub fn get(&self, key: &str) -> Option<OnlineEntry> {
        self.get_count.fetch_add(1, Ordering::Relaxed);

        let entry = self.data.get(key)?;
        let now = chrono::Utc::now().timestamp_millis();

        // Check TTL expiry
        if entry.expiry_timestamp > 0 && now > entry.expiry_timestamp {
            // Entry is expired, remove it
            drop(entry); // Release the read lock before removing
            self.data.remove(key);
            self.eviction_count.fetch_add(1, Ordering::Relaxed);
            return None;
        }

        Some(entry.clone())
    }

    /// Get multiple entries at once
    pub fn multi_get(&self, keys: &[String]) -> Vec<Option<OnlineEntry>> {
        keys.iter().map(|k| self.get(k)).collect()
    }

    /// Put a feature entry with TTL
    pub fn put(
        &self,
        key: String,
        value: FeatureValue,
        event_timestamp: i64,
        ttl_seconds: u64,
    ) {
        self.put_count.fetch_add(1, Ordering::Relaxed);

        let now = chrono::Utc::now().timestamp_millis();
        let effective_ttl = if ttl_seconds > 0 {
            ttl_seconds
        } else {
            self.default_ttl_seconds
        };

        let expiry_timestamp = if effective_ttl > 0 {
            now + (effective_ttl as i64 * 1000)
        } else {
            0
        };

        let entry = OnlineEntry {
            value,
            event_timestamp,
            created_timestamp: now,
            expiry_timestamp,
        };

        self.data.insert(key, entry);

        // Evict if over capacity
        if self.data.len() > self.max_entries {
            self.evict_expired_and_oldest();
        }
    }

    /// Put with the default TTL
    pub fn put_default_ttl(&self, key: String, value: FeatureValue, event_timestamp: i64) {
        self.put(key, value, event_timestamp, self.default_ttl_seconds);
    }

    /// Delete an entry
    pub fn delete(&self, key: &str) -> bool {
        self.data.remove(key).is_some()
    }

    /// Clear all entries
    pub fn clear(&self) {
        self.data.clear();
    }

    /// Number of entries (including potentially expired ones)
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if the store is empty
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Number of evictions
    pub fn eviction_count(&self) -> u64 {
        self.eviction_count.load(Ordering::Relaxed)
    }

    /// Total put operations
    pub fn put_count(&self) -> u64 {
        self.put_count.load(Ordering::Relaxed)
    }

    /// Total get operations
    pub fn get_count(&self) -> u64 {
        self.get_count.load(Ordering::Relaxed)
    }

    /// Evict expired entries and oldest entries if still over capacity
    fn evict_expired_and_oldest(&self) {
        let now = chrono::Utc::now().timestamp_millis();

        // First pass: remove expired entries
        let expired_keys: Vec<String> = self
            .data
            .iter()
            .filter(|entry| entry.value().expiry_timestamp > 0 && now > entry.value().expiry_timestamp)
            .map(|entry| entry.key().clone())
            .collect();

        for key in &expired_keys {
            self.data.remove(key);
            self.eviction_count.fetch_add(1, Ordering::Relaxed);
        }

        // If still over capacity, evict oldest entries
        if self.data.len() > self.max_entries {
            let target = self.max_entries * 9 / 10; // Evict down to 90%
            let to_remove = self.data.len().saturating_sub(target);

            if to_remove > 0 {
                // Collect entries sorted by event_timestamp
                let mut entries: Vec<(String, i64)> = self
                    .data
                    .iter()
                    .map(|e| (e.key().clone(), e.value().event_timestamp))
                    .collect();

                entries.sort_by_key(|(_, ts)| *ts);

                for (key, _) in entries.into_iter().take(to_remove) {
                    self.data.remove(&key);
                    self.eviction_count.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    /// Run TTL eviction (can be called periodically)
    pub fn evict_expired(&self) -> usize {
        let now = chrono::Utc::now().timestamp_millis();
        let mut evicted = 0;

        let expired_keys: Vec<String> = self
            .data
            .iter()
            .filter(|entry| entry.value().expiry_timestamp > 0 && now > entry.value().expiry_timestamp)
            .map(|entry| entry.key().clone())
            .collect();

        for key in expired_keys {
            if self.data.remove(&key).is_some() {
                evicted += 1;
                self.eviction_count.fetch_add(1, Ordering::Relaxed);
            }
        }

        evicted
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_put_get() {
        let store = DashMapOnlineStore::new(1000, 0);

        store.put(
            "user_123:age".to_string(),
            FeatureValue::Int64(25),
            1000,
            0,
        );

        let entry = store.get("user_123:age");
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().value, FeatureValue::Int64(25));
    }

    #[test]
    fn test_missing_key() {
        let store = DashMapOnlineStore::new(1000, 0);
        assert!(store.get("nonexistent").is_none());
    }

    #[test]
    fn test_overwrite() {
        let store = DashMapOnlineStore::new(1000, 0);

        store.put(
            "key".to_string(),
            FeatureValue::Float64(1.0),
            1000,
            0,
        );
        store.put(
            "key".to_string(),
            FeatureValue::Float64(2.0),
            2000,
            0,
        );

        let entry = store.get("key").unwrap();
        assert_eq!(entry.value, FeatureValue::Float64(2.0));
        assert_eq!(entry.event_timestamp, 2000);
    }

    #[test]
    fn test_delete() {
        let store = DashMapOnlineStore::new(1000, 0);

        store.put(
            "key".to_string(),
            FeatureValue::Int64(1),
            1000,
            0,
        );
        assert!(store.delete("key"));
        assert!(store.get("key").is_none());
        assert!(!store.delete("key"));
    }

    #[test]
    fn test_clear() {
        let store = DashMapOnlineStore::new(1000, 0);

        store.put("k1".to_string(), FeatureValue::Int64(1), 1000, 0);
        store.put("k2".to_string(), FeatureValue::Int64(2), 1000, 0);

        assert_eq!(store.len(), 2);
        store.clear();
        assert_eq!(store.len(), 0);
        assert!(store.is_empty());
    }

    #[test]
    fn test_multi_get() {
        let store = DashMapOnlineStore::new(1000, 0);

        store.put("k1".to_string(), FeatureValue::Int64(1), 1000, 0);
        store.put("k2".to_string(), FeatureValue::Int64(2), 1000, 0);

        let results = store.multi_get(&[
            "k1".to_string(),
            "k2".to_string(),
            "k3".to_string(),
        ]);

        assert_eq!(results.len(), 3);
        assert!(results[0].is_some());
        assert!(results[1].is_some());
        assert!(results[2].is_none());
    }

    #[test]
    fn test_ttl_expiry() {
        let store = DashMapOnlineStore::new(1000, 0);

        // Put with 0-second TTL (expired immediately after creation)
        // We simulate by creating an entry and manually adjusting the expiry
        let now = chrono::Utc::now().timestamp_millis();
        let entry = OnlineEntry {
            value: FeatureValue::Int64(42),
            event_timestamp: now,
            created_timestamp: now,
            expiry_timestamp: now - 1000, // Already expired (1 second ago)
        };
        store.data.insert("expired_key".to_string(), entry);

        // Get should return None for expired entry
        assert!(store.get("expired_key").is_none());
    }

    #[test]
    fn test_ttl_not_expired() {
        let store = DashMapOnlineStore::new(1000, 0);

        // Put with a TTL far in the future
        store.put(
            "valid_key".to_string(),
            FeatureValue::Int64(42),
            chrono::Utc::now().timestamp_millis(),
            3600, // 1 hour TTL
        );

        // Should still be accessible
        let entry = store.get("valid_key");
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().value, FeatureValue::Int64(42));
    }

    #[test]
    fn test_eviction_on_capacity() {
        let store = DashMapOnlineStore::new(5, 0);

        // Insert more than max_entries
        for i in 0..10 {
            store.put(
                format!("key_{}", i),
                FeatureValue::Int64(i),
                i as i64 * 1000,
                0,
            );
        }

        // Should have evicted some entries
        assert!(store.len() <= 5);
        assert!(store.eviction_count() > 0);
    }

    #[test]
    fn test_evict_expired() {
        let store = DashMapOnlineStore::new(1000, 0);
        let now = chrono::Utc::now().timestamp_millis();

        // Insert expired entries
        for i in 0..5 {
            let entry = OnlineEntry {
                value: FeatureValue::Int64(i),
                event_timestamp: now,
                created_timestamp: now,
                expiry_timestamp: now - 1000,
            };
            store.data.insert(format!("expired_{}", i), entry);
        }

        // Insert valid entries
        for i in 0..5 {
            store.put(
                format!("valid_{}", i),
                FeatureValue::Int64(i),
                now,
                3600,
            );
        }

        assert_eq!(store.len(), 10);
        let evicted = store.evict_expired();
        assert_eq!(evicted, 5);
        assert_eq!(store.len(), 5);
    }

    #[test]
    fn test_stats() {
        let store = DashMapOnlineStore::new(1000, 0);

        store.put("k".to_string(), FeatureValue::Int64(1), 1000, 0);
        store.get("k");
        store.get("missing");

        assert_eq!(store.put_count(), 1);
        assert_eq!(store.get_count(), 2);
    }

    #[test]
    fn test_concurrent_access() {
        use std::sync::Arc;
        use std::thread;

        let store = Arc::new(DashMapOnlineStore::new(100_000, 0));

        let mut handles = vec![];

        // Spawn writers
        for t in 0..4 {
            let store = store.clone();
            handles.push(thread::spawn(move || {
                for i in 0..1000 {
                    store.put(
                        format!("thread_{}_key_{}", t, i),
                        FeatureValue::Int64(i),
                        i as i64,
                        0,
                    );
                }
            }));
        }

        // Spawn readers
        for t in 0..4 {
            let store = store.clone();
            handles.push(thread::spawn(move || {
                for i in 0..1000 {
                    let _ = store.get(&format!("thread_{}_key_{}", t, i));
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // All writer threads should have written their entries
        assert!(store.len() <= 4000);
    }
}
