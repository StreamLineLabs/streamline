//! Durable offset storage for Kafka Connect connectors
//!
//! Provides file-backed persistent offset storage with an in-memory cache
//! and periodic flush. Each connector's offsets are stored in a separate
//! JSON file under the configured data directory.

use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Configuration for the connector offset store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OffsetStoreConfig {
    /// Directory where offset files are persisted.
    pub data_dir: PathBuf,
    /// Interval in milliseconds between automatic flushes.
    pub flush_interval_ms: u64,
}

impl Default for OffsetStoreConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("data/connect-offsets"),
            flush_interval_ms: 5000,
        }
    }
}

/// In-memory representation of a single connector's offsets.
type PartitionOffsets = HashMap<String, serde_json::Value>;

/// Serialized form of a connector offset file.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct OffsetFile {
    connector: String,
    offsets: PartitionOffsets,
}

/// Durable offset store for Kafka Connect connectors.
///
/// Maintains an in-memory cache of offsets keyed by connector name and
/// partition, backed by per-connector JSON files on disk. Writes are
/// atomic (write to temp file, then rename).
pub struct ConnectorOffsetStore {
    config: OffsetStoreConfig,
    cache: Arc<RwLock<HashMap<String, PartitionOffsets>>>,
    /// Tracks connectors with unsaved changes.
    dirty: Arc<RwLock<std::collections::HashSet<String>>>,
}

impl ConnectorOffsetStore {
    /// Create a new offset store, loading any existing offsets from disk.
    pub fn new(config: OffsetStoreConfig) -> Result<Self> {
        std::fs::create_dir_all(&config.data_dir).map_err(|e| {
            StreamlineError::storage(
                "offset_store_init",
                format!("failed to create data dir {:?}: {}", config.data_dir, e),
            )
        })?;

        let store = Self {
            config,
            cache: Arc::new(RwLock::new(HashMap::new())),
            dirty: Arc::new(RwLock::new(std::collections::HashSet::new())),
        };

        // Load existing offset files from disk synchronously at startup.
        store.load_all_from_disk()?;

        info!(
            data_dir = %store.config.data_dir.display(),
            "Connector offset store initialized"
        );

        Ok(store)
    }

    /// Save an offset for a specific connector and partition.
    pub async fn save_offset(
        &self,
        connector: &str,
        partition: &str,
        offset: serde_json::Value,
    ) {
        let mut cache = self.cache.write().await;
        cache
            .entry(connector.to_string())
            .or_default()
            .insert(partition.to_string(), offset);

        let mut dirty = self.dirty.write().await;
        dirty.insert(connector.to_string());

        debug!(connector, partition, "Offset saved to cache");
    }

    /// Load a single partition offset for a connector.
    pub async fn load_offset(
        &self,
        connector: &str,
        partition: &str,
    ) -> Option<serde_json::Value> {
        let cache = self.cache.read().await;
        cache
            .get(connector)
            .and_then(|m| m.get(partition))
            .cloned()
    }

    /// Load all partition offsets for a connector.
    pub async fn load_all_offsets(&self, connector: &str) -> HashMap<String, serde_json::Value> {
        let cache = self.cache.read().await;
        cache.get(connector).cloned().unwrap_or_default()
    }

    /// Delete all offsets for a connector (cache and disk).
    pub async fn delete_offsets(&self, connector: &str) -> Result<()> {
        {
            let mut cache = self.cache.write().await;
            cache.remove(connector);
        }
        {
            let mut dirty = self.dirty.write().await;
            dirty.remove(connector);
        }

        let path = self.connector_path(connector);
        if path.exists() {
            std::fs::remove_file(&path).map_err(|e| {
                StreamlineError::storage(
                    "delete_offsets",
                    format!("failed to remove offset file {:?}: {}", path, e),
                )
            })?;
        }

        info!(connector, "Offsets deleted");
        Ok(())
    }

    /// Force write all dirty offsets to disk.
    pub async fn flush(&self) -> Result<()> {
        let dirty_connectors: Vec<String> = {
            let mut dirty = self.dirty.write().await;
            let connectors: Vec<String> = dirty.drain().collect();
            connectors
        };

        if dirty_connectors.is_empty() {
            debug!("No dirty offsets to flush");
            return Ok(());
        }

        let cache = self.cache.read().await;
        for connector in &dirty_connectors {
            if let Some(offsets) = cache.get(connector) {
                self.write_connector_offsets(connector, offsets)?;
            }
        }

        debug!(count = dirty_connectors.len(), "Flushed connector offsets");
        Ok(())
    }

    // ---- Internal helpers ----

    /// Path to the JSON file for a given connector.
    fn connector_path(&self, connector: &str) -> PathBuf {
        // Sanitise connector name for safe filesystem use.
        let safe_name: String = connector
            .chars()
            .map(|c| if c.is_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
            .collect();
        self.config.data_dir.join(format!("{}.json", safe_name))
    }

    /// Atomically write a connector's offsets to disk.
    fn write_connector_offsets(
        &self,
        connector: &str,
        offsets: &PartitionOffsets,
    ) -> Result<()> {
        let dest = self.connector_path(connector);
        let tmp = dest.with_extension("json.tmp");

        let file = OffsetFile {
            connector: connector.to_string(),
            offsets: offsets.clone(),
        };

        let data = serde_json::to_vec_pretty(&file).map_err(|e| {
            StreamlineError::storage_msg(format!("failed to serialize offsets: {}", e))
        })?;

        std::fs::write(&tmp, &data).map_err(|e| {
            StreamlineError::storage(
                "write_offsets",
                format!("failed to write temp file {:?}: {}", tmp, e),
            )
        })?;

        std::fs::rename(&tmp, &dest).map_err(|e| {
            StreamlineError::storage(
                "write_offsets",
                format!("failed to rename {:?} -> {:?}: {}", tmp, dest, e),
            )
        })?;

        debug!(connector, path = %dest.display(), "Offsets persisted to disk");
        Ok(())
    }

    /// Load all connector offset files from the data directory at startup.
    fn load_all_from_disk(&self) -> Result<()> {
        let entries = std::fs::read_dir(&self.config.data_dir).map_err(|e| {
            StreamlineError::storage(
                "load_offsets",
                format!("failed to read data dir {:?}: {}", self.config.data_dir, e),
            )
        })?;

        let cache = self.cache.clone();
        // We're in a synchronous context at startup; use blocking lock.
        let mut cache_guard = cache.blocking_write();

        for entry in entries {
            let entry = match entry {
                Ok(e) => e,
                Err(e) => {
                    warn!("Skipping unreadable dir entry: {}", e);
                    continue;
                }
            };

            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }

            match Self::read_offset_file(&path) {
                Ok(file) => {
                    debug!(connector = %file.connector, partitions = file.offsets.len(), "Loaded offsets from disk");
                    cache_guard.insert(file.connector, file.offsets);
                }
                Err(e) => {
                    warn!(path = %path.display(), "Failed to load offset file: {}", e);
                }
            }
        }

        info!(connectors = cache_guard.len(), "Loaded offset files from disk");
        Ok(())
    }

    /// Read and deserialize a single offset file.
    fn read_offset_file(path: &Path) -> Result<OffsetFile> {
        let data = std::fs::read(path).map_err(|e| {
            StreamlineError::storage(
                "read_offset_file",
                format!("failed to read {:?}: {}", path, e),
            )
        })?;

        serde_json::from_slice::<OffsetFile>(&data).map_err(|e| {
            StreamlineError::storage(
                "read_offset_file",
                format!("failed to parse {:?}: {}", path, e),
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::TempDir;

    fn test_config(dir: &Path) -> OffsetStoreConfig {
        OffsetStoreConfig {
            data_dir: dir.to_path_buf(),
            flush_interval_ms: 1000,
        }
    }

    #[tokio::test]
    async fn test_save_and_load_offset() {
        let tmp = TempDir::new().unwrap();
        let store = ConnectorOffsetStore::new(test_config(tmp.path())).unwrap();

        store.save_offset("my-source", "partition-0", json!(42)).await;

        let val = store.load_offset("my-source", "partition-0").await;
        assert_eq!(val, Some(json!(42)));

        let missing = store.load_offset("my-source", "partition-1").await;
        assert!(missing.is_none());
    }

    #[tokio::test]
    async fn test_load_all_offsets() {
        let tmp = TempDir::new().unwrap();
        let store = ConnectorOffsetStore::new(test_config(tmp.path())).unwrap();

        store.save_offset("conn-a", "p0", json!(10)).await;
        store.save_offset("conn-a", "p1", json!(20)).await;
        store.save_offset("conn-b", "p0", json!(99)).await;

        let all = store.load_all_offsets("conn-a").await;
        assert_eq!(all.len(), 2);
        assert_eq!(all.get("p0"), Some(&json!(10)));
        assert_eq!(all.get("p1"), Some(&json!(20)));

        let empty = store.load_all_offsets("nonexistent").await;
        assert!(empty.is_empty());
    }

    #[tokio::test]
    async fn test_flush_persists_to_disk() {
        let tmp = TempDir::new().unwrap();
        let store = ConnectorOffsetStore::new(test_config(tmp.path())).unwrap();

        store.save_offset("source-1", "p0", json!({"pos": 100})).await;
        store.flush().await.unwrap();

        // Verify file exists on disk
        let file_path = tmp.path().join("source-1.json");
        assert!(file_path.exists());

        // Read back and verify contents
        let data = std::fs::read_to_string(&file_path).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&data).unwrap();
        assert_eq!(parsed["connector"], "source-1");
        assert_eq!(parsed["offsets"]["p0"]["pos"], 100);
    }

    #[tokio::test]
    async fn test_atomic_write_uses_rename() {
        let tmp = TempDir::new().unwrap();
        let store = ConnectorOffsetStore::new(test_config(tmp.path())).unwrap();

        store.save_offset("conn-x", "p0", json!(1)).await;
        store.flush().await.unwrap();

        // No leftover temp files
        let tmp_path = tmp.path().join("conn-x.json.tmp");
        assert!(!tmp_path.exists());

        // Real file exists
        assert!(tmp.path().join("conn-x.json").exists());
    }

    #[tokio::test]
    async fn test_persistence_across_instances() {
        let tmp = TempDir::new().unwrap();

        // First instance: write and flush
        {
            let store = ConnectorOffsetStore::new(test_config(tmp.path())).unwrap();
            store.save_offset("persistent", "p0", json!({"offset": 55})).await;
            store.save_offset("persistent", "p1", json!({"offset": 66})).await;
            store.flush().await.unwrap();
        }

        // Second instance: should load from disk
        {
            let store = ConnectorOffsetStore::new(test_config(tmp.path())).unwrap();
            let val = store.load_offset("persistent", "p0").await;
            assert_eq!(val, Some(json!({"offset": 55})));

            let all = store.load_all_offsets("persistent").await;
            assert_eq!(all.len(), 2);
        }
    }

    #[tokio::test]
    async fn test_delete_offsets() {
        let tmp = TempDir::new().unwrap();
        let store = ConnectorOffsetStore::new(test_config(tmp.path())).unwrap();

        store.save_offset("to-delete", "p0", json!(1)).await;
        store.flush().await.unwrap();
        assert!(tmp.path().join("to-delete.json").exists());

        store.delete_offsets("to-delete").await.unwrap();

        assert!(store.load_offset("to-delete", "p0").await.is_none());
        assert!(!tmp.path().join("to-delete.json").exists());
    }

    #[tokio::test]
    async fn test_flush_no_dirty_is_noop() {
        let tmp = TempDir::new().unwrap();
        let store = ConnectorOffsetStore::new(test_config(tmp.path())).unwrap();

        // Flush with nothing dirty should succeed silently
        store.flush().await.unwrap();
    }

    #[tokio::test]
    async fn test_overwrite_offset() {
        let tmp = TempDir::new().unwrap();
        let store = ConnectorOffsetStore::new(test_config(tmp.path())).unwrap();

        store.save_offset("conn", "p0", json!(1)).await;
        store.save_offset("conn", "p0", json!(2)).await;

        let val = store.load_offset("conn", "p0").await;
        assert_eq!(val, Some(json!(2)));
    }

    #[tokio::test]
    async fn test_connector_name_sanitization() {
        let tmp = TempDir::new().unwrap();
        let store = ConnectorOffsetStore::new(test_config(tmp.path())).unwrap();

        // Connector name with special characters
        store.save_offset("my/connector:v2", "p0", json!(1)).await;
        store.flush().await.unwrap();

        // File should use sanitized name
        let sanitized_path = tmp.path().join("my_connector_v2.json");
        assert!(sanitized_path.exists());
    }

    #[tokio::test]
    async fn test_default_config() {
        let config = OffsetStoreConfig::default();
        assert_eq!(config.data_dir, PathBuf::from("data/connect-offsets"));
        assert_eq!(config.flush_interval_ms, 5000);
    }

    #[tokio::test]
    async fn test_multiple_connectors_independent() {
        let tmp = TempDir::new().unwrap();
        let store = ConnectorOffsetStore::new(test_config(tmp.path())).unwrap();

        store.save_offset("conn-a", "p0", json!(100)).await;
        store.save_offset("conn-b", "p0", json!(200)).await;

        store.delete_offsets("conn-a").await.unwrap();

        assert!(store.load_offset("conn-a", "p0").await.is_none());
        assert_eq!(store.load_offset("conn-b", "p0").await, Some(json!(200)));
    }
}
