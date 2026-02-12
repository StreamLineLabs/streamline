//! State Management
//!
//! Provides state backends and management for stateful operators.

use super::{OperatorError, StateBackendType};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// State configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateConfig {
    /// Backend type
    pub backend: StateBackendType,
    /// Maximum state size (bytes)
    pub max_state_size: usize,
    /// TTL for state entries (ms)
    pub ttl_ms: Option<u64>,
}

impl Default for StateConfig {
    fn default() -> Self {
        Self {
            backend: StateBackendType::Memory,
            max_state_size: 100 * 1024 * 1024, // 100MB
            ttl_ms: None,
        }
    }
}

/// State backend trait
pub trait StateBackend: Send + Sync {
    /// Get value by key
    fn get(&self, namespace: &str, key: &str) -> Result<Option<Vec<u8>>, OperatorError>;

    /// Put value
    fn put(&mut self, namespace: &str, key: &str, value: Vec<u8>) -> Result<(), OperatorError>;

    /// Delete key
    fn delete(&mut self, namespace: &str, key: &str) -> Result<(), OperatorError>;

    /// List keys in namespace
    fn keys(&self, namespace: &str) -> Result<Vec<String>, OperatorError>;

    /// Clear namespace
    fn clear(&mut self, namespace: &str) -> Result<(), OperatorError>;

    /// Get approximate size
    fn size(&self) -> usize;

    /// Create snapshot for checkpointing
    fn snapshot(&self) -> Result<Vec<u8>, OperatorError>;

    /// Restore from snapshot
    fn restore(&mut self, data: &[u8]) -> Result<(), OperatorError>;
}

/// In-memory state backend
#[derive(Debug, Clone, Default)]
pub struct MemoryStateBackend {
    /// State by namespace -> key -> value
    state: HashMap<String, HashMap<String, StateEntry>>,
    /// Current size estimate
    size: usize,
}

/// State entry with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StateEntry {
    /// Value
    value: Vec<u8>,
    /// Created timestamp
    created_at: i64,
    /// Last accessed timestamp
    last_accessed: i64,
}

impl MemoryStateBackend {
    /// Create a new memory backend
    pub fn new() -> Self {
        Self::default()
    }
}

impl StateBackend for MemoryStateBackend {
    fn get(&self, namespace: &str, key: &str) -> Result<Option<Vec<u8>>, OperatorError> {
        Ok(self
            .state
            .get(namespace)
            .and_then(|ns| ns.get(key))
            .map(|entry| entry.value.clone()))
    }

    fn put(&mut self, namespace: &str, key: &str, value: Vec<u8>) -> Result<(), OperatorError> {
        let now = chrono::Utc::now().timestamp_millis();
        let entry = StateEntry {
            value: value.clone(),
            created_at: now,
            last_accessed: now,
        };

        // Update size estimate
        self.size += value.len();
        if let Some(ns) = self.state.get(namespace) {
            if let Some(old_entry) = ns.get(key) {
                self.size -= old_entry.value.len();
            }
        }

        self.state
            .entry(namespace.to_string())
            .or_default()
            .insert(key.to_string(), entry);

        Ok(())
    }

    fn delete(&mut self, namespace: &str, key: &str) -> Result<(), OperatorError> {
        if let Some(ns) = self.state.get_mut(namespace) {
            if let Some(entry) = ns.remove(key) {
                self.size -= entry.value.len();
            }
        }
        Ok(())
    }

    fn keys(&self, namespace: &str) -> Result<Vec<String>, OperatorError> {
        Ok(self
            .state
            .get(namespace)
            .map(|ns| ns.keys().cloned().collect())
            .unwrap_or_default())
    }

    fn clear(&mut self, namespace: &str) -> Result<(), OperatorError> {
        if let Some(ns) = self.state.remove(namespace) {
            self.size -= ns.values().map(|e| e.value.len()).sum::<usize>();
        }
        Ok(())
    }

    fn size(&self) -> usize {
        self.size
    }

    fn snapshot(&self) -> Result<Vec<u8>, OperatorError> {
        serde_json::to_vec(&self.state)
            .map_err(|e| OperatorError::SerializationError(e.to_string()))
    }

    fn restore(&mut self, data: &[u8]) -> Result<(), OperatorError> {
        self.state = serde_json::from_slice(data)
            .map_err(|e| OperatorError::SerializationError(e.to_string()))?;
        self.size = self
            .state
            .values()
            .flat_map(|ns| ns.values())
            .map(|e| e.value.len())
            .sum();
        Ok(())
    }
}

/// State manager
pub struct StateManager {
    /// Configuration
    config: StateConfig,
    /// Backend
    backend: Box<dyn StateBackend>,
    /// Registered state stores
    stores: HashMap<String, StateStoreConfig>,
    /// Statistics
    stats: StateManagerStats,
}

/// State store configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StateStoreConfig {
    /// Store name
    name: String,
    /// Key type (for validation)
    key_type: String,
    /// Value type (for validation)
    value_type: String,
    /// TTL override
    ttl_ms: Option<u64>,
}

/// State manager statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StateManagerStats {
    /// Total gets
    pub gets: u64,
    /// Total puts
    pub puts: u64,
    /// Total deletes
    pub deletes: u64,
    /// Cache hits
    pub hits: u64,
    /// Cache misses
    pub misses: u64,
    /// Current size (bytes)
    pub size_bytes: usize,
}

impl StateManager {
    /// Create a new state manager
    pub fn new(config: StateConfig) -> Self {
        let backend: Box<dyn StateBackend> = match config.backend {
            StateBackendType::Memory => Box::new(MemoryStateBackend::new()),
            StateBackendType::RocksDb => Box::new(MemoryStateBackend::new()), // Fallback
            StateBackendType::Distributed => Box::new(MemoryStateBackend::new()), // Fallback
        };

        Self {
            config,
            backend,
            stores: HashMap::new(),
            stats: StateManagerStats::default(),
        }
    }

    /// Register a state store
    pub fn register_store(&mut self, name: &str, key_type: &str, value_type: &str) {
        self.stores.insert(
            name.to_string(),
            StateStoreConfig {
                name: name.to_string(),
                key_type: key_type.to_string(),
                value_type: value_type.to_string(),
                ttl_ms: self.config.ttl_ms,
            },
        );
    }

    /// Get state store
    pub fn get_store<K, V>(&mut self, name: &str) -> StateStore<K, V>
    where
        K: Serialize + for<'de> Deserialize<'de>,
        V: Serialize + for<'de> Deserialize<'de>,
    {
        StateStore::new(name.to_string())
    }

    /// Get value
    pub fn get(&mut self, namespace: &str, key: &str) -> Result<Option<Vec<u8>>, OperatorError> {
        self.stats.gets += 1;
        let result = self.backend.get(namespace, key)?;
        if result.is_some() {
            self.stats.hits += 1;
        } else {
            self.stats.misses += 1;
        }
        Ok(result)
    }

    /// Put value
    pub fn put(&mut self, namespace: &str, key: &str, value: Vec<u8>) -> Result<(), OperatorError> {
        // Check size limit
        let new_size = self.backend.size() + value.len();
        if new_size > self.config.max_state_size {
            return Err(OperatorError::StateError(
                "State size limit exceeded".to_string(),
            ));
        }

        self.stats.puts += 1;
        self.backend.put(namespace, key, value)?;
        self.stats.size_bytes = self.backend.size();
        Ok(())
    }

    /// Delete value
    pub fn delete(&mut self, namespace: &str, key: &str) -> Result<(), OperatorError> {
        self.stats.deletes += 1;
        self.backend.delete(namespace, key)?;
        self.stats.size_bytes = self.backend.size();
        Ok(())
    }

    /// Clear namespace
    pub fn clear(&mut self, namespace: &str) -> Result<(), OperatorError> {
        self.backend.clear(namespace)?;
        self.stats.size_bytes = self.backend.size();
        Ok(())
    }

    /// Create snapshot
    pub fn snapshot(&self) -> Result<Vec<u8>, OperatorError> {
        self.backend.snapshot()
    }

    /// Restore from snapshot
    pub fn restore(&mut self, data: &[u8]) -> Result<(), OperatorError> {
        self.backend.restore(data)?;
        self.stats.size_bytes = self.backend.size();
        Ok(())
    }

    /// Get statistics
    pub fn stats(&self) -> &StateManagerStats {
        &self.stats
    }

    /// Get current size
    pub fn size(&self) -> usize {
        self.backend.size()
    }
}

/// Typed state store
pub struct StateStore<K, V> {
    /// Namespace
    namespace: String,
    /// Phantom data
    _phantom: std::marker::PhantomData<(K, V)>,
}

impl<K, V> StateStore<K, V>
where
    K: Serialize + for<'de> Deserialize<'de>,
    V: Serialize + for<'de> Deserialize<'de>,
{
    /// Create a new state store
    pub fn new(namespace: String) -> Self {
        Self {
            namespace,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Serialize key
    fn serialize_key(key: &K) -> Result<String, OperatorError> {
        serde_json::to_string(key).map_err(|e| OperatorError::SerializationError(e.to_string()))
    }

    /// Serialize value
    fn serialize_value(value: &V) -> Result<Vec<u8>, OperatorError> {
        serde_json::to_vec(value).map_err(|e| OperatorError::SerializationError(e.to_string()))
    }

    /// Deserialize value
    fn deserialize_value(data: &[u8]) -> Result<V, OperatorError> {
        serde_json::from_slice(data).map_err(|e| OperatorError::SerializationError(e.to_string()))
    }

    /// Get value
    pub fn get(&self, manager: &mut StateManager, key: &K) -> Result<Option<V>, OperatorError> {
        let key_str = Self::serialize_key(key)?;
        match manager.get(&self.namespace, &key_str)? {
            Some(data) => Ok(Some(Self::deserialize_value(&data)?)),
            None => Ok(None),
        }
    }

    /// Put value
    pub fn put(&self, manager: &mut StateManager, key: &K, value: &V) -> Result<(), OperatorError> {
        let key_str = Self::serialize_key(key)?;
        let value_bytes = Self::serialize_value(value)?;
        manager.put(&self.namespace, &key_str, value_bytes)
    }

    /// Delete value
    pub fn delete(&self, manager: &mut StateManager, key: &K) -> Result<(), OperatorError> {
        let key_str = Self::serialize_key(key)?;
        manager.delete(&self.namespace, &key_str)
    }

    /// Get namespace
    pub fn namespace(&self) -> &str {
        &self.namespace
    }
}

/// Keyed state for per-key values
pub struct KeyedState<K, V> {
    /// State store
    store: StateStore<K, V>,
    /// Local cache
    cache: HashMap<String, V>,
    /// Cache enabled
    cache_enabled: bool,
}

impl<K, V> KeyedState<K, V>
where
    K: Serialize + for<'de> Deserialize<'de> + Clone,
    V: Serialize + for<'de> Deserialize<'de> + Clone,
{
    /// Create new keyed state
    pub fn new(namespace: &str) -> Self {
        Self {
            store: StateStore::new(namespace.to_string()),
            cache: HashMap::new(),
            cache_enabled: true,
        }
    }

    /// Disable caching
    pub fn without_cache(mut self) -> Self {
        self.cache_enabled = false;
        self.cache.clear();
        self
    }

    /// Get value for key
    pub fn get(&mut self, manager: &mut StateManager, key: &K) -> Result<Option<V>, OperatorError> {
        let key_str = serde_json::to_string(key)
            .map_err(|e| OperatorError::SerializationError(e.to_string()))?;

        // Check cache first
        if self.cache_enabled {
            if let Some(value) = self.cache.get(&key_str) {
                return Ok(Some(value.clone()));
            }
        }

        // Get from store
        let value = self.store.get(manager, key)?;

        // Update cache
        if self.cache_enabled {
            if let Some(ref v) = value {
                self.cache.insert(key_str, v.clone());
            }
        }

        Ok(value)
    }

    /// Put value for key
    pub fn put(
        &mut self,
        manager: &mut StateManager,
        key: &K,
        value: &V,
    ) -> Result<(), OperatorError> {
        let key_str = serde_json::to_string(key)
            .map_err(|e| OperatorError::SerializationError(e.to_string()))?;

        // Update cache
        if self.cache_enabled {
            self.cache.insert(key_str, value.clone());
        }

        // Put to store
        self.store.put(manager, key, value)
    }

    /// Update value with function
    pub fn update<F>(
        &mut self,
        manager: &mut StateManager,
        key: &K,
        f: F,
    ) -> Result<V, OperatorError>
    where
        F: FnOnce(Option<V>) -> V,
    {
        let current = self.get(manager, key)?;
        let new_value = f(current);
        self.put(manager, key, &new_value)?;
        Ok(new_value)
    }

    /// Clear cache
    pub fn clear_cache(&mut self) {
        self.cache.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_state_config_default() {
        let config = StateConfig::default();
        assert_eq!(config.backend, StateBackendType::Memory);
        assert_eq!(config.max_state_size, 100 * 1024 * 1024);
    }

    #[test]
    fn test_memory_backend_basic() {
        let mut backend = MemoryStateBackend::new();

        backend.put("ns1", "key1", vec![1, 2, 3]).unwrap();
        let value = backend.get("ns1", "key1").unwrap();
        assert_eq!(value, Some(vec![1, 2, 3]));

        backend.delete("ns1", "key1").unwrap();
        let value = backend.get("ns1", "key1").unwrap();
        assert!(value.is_none());
    }

    #[test]
    fn test_memory_backend_keys() {
        let mut backend = MemoryStateBackend::new();

        backend.put("ns1", "key1", vec![1]).unwrap();
        backend.put("ns1", "key2", vec![2]).unwrap();
        backend.put("ns2", "key3", vec![3]).unwrap();

        let keys = backend.keys("ns1").unwrap();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"key1".to_string()));
        assert!(keys.contains(&"key2".to_string()));
    }

    #[test]
    fn test_memory_backend_clear() {
        let mut backend = MemoryStateBackend::new();

        backend.put("ns1", "key1", vec![1]).unwrap();
        backend.put("ns1", "key2", vec![2]).unwrap();
        backend.clear("ns1").unwrap();

        let keys = backend.keys("ns1").unwrap();
        assert!(keys.is_empty());
    }

    #[test]
    fn test_memory_backend_snapshot_restore() {
        let mut backend = MemoryStateBackend::new();
        backend.put("ns1", "key1", vec![1, 2, 3]).unwrap();

        let snapshot = backend.snapshot().unwrap();

        let mut new_backend = MemoryStateBackend::new();
        new_backend.restore(&snapshot).unwrap();

        let value = new_backend.get("ns1", "key1").unwrap();
        assert_eq!(value, Some(vec![1, 2, 3]));
    }

    #[test]
    fn test_state_manager() {
        let config = StateConfig::default();
        let mut manager = StateManager::new(config);

        manager.put("ns1", "key1", vec![1, 2, 3]).unwrap();
        let value = manager.get("ns1", "key1").unwrap();
        assert_eq!(value, Some(vec![1, 2, 3]));

        let stats = manager.stats();
        assert_eq!(stats.gets, 1);
        assert_eq!(stats.puts, 1);
        assert_eq!(stats.hits, 1);
    }

    #[test]
    fn test_state_store_typed() {
        let config = StateConfig::default();
        let mut manager = StateManager::new(config);

        let store: StateStore<String, i32> = StateStore::new("test".to_string());

        store.put(&mut manager, &"key1".to_string(), &42).unwrap();
        let value = store.get(&mut manager, &"key1".to_string()).unwrap();
        assert_eq!(value, Some(42));
    }

    #[test]
    fn test_keyed_state() {
        let config = StateConfig::default();
        let mut manager = StateManager::new(config);

        let mut keyed: KeyedState<String, i32> = KeyedState::new("test");

        keyed.put(&mut manager, &"user1".to_string(), &100).unwrap();
        keyed.put(&mut manager, &"user2".to_string(), &200).unwrap();

        assert_eq!(
            keyed.get(&mut manager, &"user1".to_string()).unwrap(),
            Some(100)
        );
        assert_eq!(
            keyed.get(&mut manager, &"user2".to_string()).unwrap(),
            Some(200)
        );
    }

    #[test]
    fn test_keyed_state_update() {
        let config = StateConfig::default();
        let mut manager = StateManager::new(config);

        let mut keyed: KeyedState<String, i32> = KeyedState::new("test");

        let result = keyed
            .update(&mut manager, &"counter".to_string(), |v| v.unwrap_or(0) + 1)
            .unwrap();
        assert_eq!(result, 1);

        let result = keyed
            .update(&mut manager, &"counter".to_string(), |v| v.unwrap_or(0) + 1)
            .unwrap();
        assert_eq!(result, 2);
    }

    #[test]
    fn test_state_manager_size_limit() {
        let config = StateConfig {
            max_state_size: 100,
            ..Default::default()
        };
        let mut manager = StateManager::new(config);

        // Should succeed
        manager.put("ns1", "key1", vec![0; 50]).unwrap();

        // Should fail - exceeds limit
        let result = manager.put("ns1", "key2", vec![0; 100]);
        assert!(result.is_err());
    }
}
