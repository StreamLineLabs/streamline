//! Persistent state store for StreamQL operators.
//!
//! Provides pluggable state backends for materialized views and other stateful
//! operators. Ships with an in-memory store (for backwards compatibility) and a
//! WAL-based file store for durability across restarts.
//!
//! ## Architecture
//!
//! ```text
//! StreamQL Operator
//!       │
//!       ▼
//!   StateStore trait  ──▶  MemoryStateStore  (HashMap, fast, non-durable)
//!                     ──▶  FileStateStore    (WAL + checkpoints, durable)
//! ```

use async_trait::async_trait;
use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

// ---------------------------------------------------------------------------
// Core trait
// ---------------------------------------------------------------------------

/// Async key-value state store used by StreamQL operators.
#[async_trait]
pub trait StateStore: Send + Sync + std::fmt::Debug {
    /// Get a value by key. Returns `None` when the key does not exist.
    async fn get(&self, key: &str) -> Result<Option<serde_json::Value>>;

    /// Insert or update a key-value pair.
    async fn put(&self, key: &str, value: serde_json::Value) -> Result<()>;

    /// Delete a key. Returns `true` if the key existed.
    async fn delete(&self, key: &str) -> Result<bool>;

    /// Scan keys in lexicographic order.
    ///
    /// When `prefix` is `Some`, only keys starting with that prefix are
    /// returned.  `limit` caps the number of entries returned.
    async fn scan(
        &self,
        prefix: Option<&str>,
        limit: Option<usize>,
    ) -> Result<Vec<(String, serde_json::Value)>>;

    /// Persist a full checkpoint of the current state and return the
    /// serialized snapshot bytes.
    async fn checkpoint(&self) -> Result<Vec<u8>>;

    /// Restore state from a previously created checkpoint.
    async fn restore(&self, data: &[u8]) -> Result<()>;
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Selects which [`StateStore`] backend to use.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StateStoreConfig {
    /// In-memory store (non-durable, suitable for tests and ephemeral views).
    Memory,
    /// WAL-based file store.
    File {
        /// Directory where WAL and checkpoint files are written.
        dir: PathBuf,
    },
}

impl Default for StateStoreConfig {
    fn default() -> Self {
        Self::Memory
    }
}

/// Build a [`StateStore`] from the given configuration.
pub async fn open_state_store(config: &StateStoreConfig) -> Result<Arc<dyn StateStore>> {
    match config {
        StateStoreConfig::Memory => Ok(Arc::new(MemoryStateStore::new())),
        StateStoreConfig::File { dir } => {
            let store = FileStateStore::open(dir.clone()).await?;
            Ok(Arc::new(store))
        }
    }
}

// ---------------------------------------------------------------------------
// MemoryStateStore
// ---------------------------------------------------------------------------

/// In-memory state store backed by a [`BTreeMap`] for ordered scans.
#[derive(Debug)]
pub struct MemoryStateStore {
    data: Arc<RwLock<BTreeMap<String, serde_json::Value>>>,
}

impl MemoryStateStore {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}

impl Default for MemoryStateStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StateStore for MemoryStateStore {
    async fn get(&self, key: &str) -> Result<Option<serde_json::Value>> {
        let data = self.data.read().await;
        Ok(data.get(key).cloned())
    }

    async fn put(&self, key: &str, value: serde_json::Value) -> Result<()> {
        let mut data = self.data.write().await;
        data.insert(key.to_string(), value);
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<bool> {
        let mut data = self.data.write().await;
        Ok(data.remove(key).is_some())
    }

    async fn scan(
        &self,
        prefix: Option<&str>,
        limit: Option<usize>,
    ) -> Result<Vec<(String, serde_json::Value)>> {
        let data = self.data.read().await;
        let owned_prefix = prefix.map(|p| p.to_string());
        let iter: Box<dyn Iterator<Item = (&String, &serde_json::Value)>> = match &owned_prefix {
            Some(p) => Box::new(data.range(p.clone()..).take_while(|(k, _)| k.starts_with(p.as_str()))),
            None => Box::new(data.iter()),
        };
        let entries: Vec<(String, serde_json::Value)> = match limit {
            Some(n) => iter.take(n).map(|(k, v)| (k.clone(), v.clone())).collect(),
            None => iter.map(|(k, v)| (k.clone(), v.clone())).collect(),
        };
        Ok(entries)
    }

    async fn checkpoint(&self) -> Result<Vec<u8>> {
        let data = self.data.read().await;
        let snapshot: BTreeMap<String, serde_json::Value> = data.clone();
        serde_json::to_vec(&snapshot).map_err(|e| {
            StreamlineError::Storage(format!("failed to serialize checkpoint: {e}"))
        })
    }

    async fn restore(&self, data: &[u8]) -> Result<()> {
        let snapshot: BTreeMap<String, serde_json::Value> =
            serde_json::from_slice(data).map_err(|e| {
                StreamlineError::CorruptedData(format!("invalid checkpoint data: {e}"))
            })?;
        let mut store = self.data.write().await;
        *store = snapshot;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// WAL entry types
// ---------------------------------------------------------------------------

/// A single entry in the write-ahead log.
#[derive(Debug, Clone, Serialize, Deserialize)]
enum WalEntry {
    Put {
        key: String,
        value: serde_json::Value,
    },
    Delete {
        key: String,
    },
}

// ---------------------------------------------------------------------------
// FileStateStore
// ---------------------------------------------------------------------------

/// Durable state store that persists mutations to an append-only WAL file
/// and periodically writes full checkpoint snapshots.
///
/// On open the store replays any existing WAL on top of the most recent
/// checkpoint to reconstruct in-memory state.
#[derive(Debug)]
pub struct FileStateStore {
    dir: PathBuf,
    data: Arc<RwLock<BTreeMap<String, serde_json::Value>>>,
    wal: Arc<RwLock<tokio::fs::File>>,
}

impl FileStateStore {
    /// Open (or create) a file-backed state store rooted at `dir`.
    ///
    /// Replays the latest checkpoint plus any WAL entries written after it.
    pub async fn open(dir: PathBuf) -> Result<Self> {
        tokio::fs::create_dir_all(&dir).await.map_err(|e| {
            StreamlineError::Storage(format!("cannot create state store dir: {e}"))
        })?;

        let mut data = BTreeMap::new();

        // Restore from checkpoint if available.
        let checkpoint_path = dir.join("checkpoint.json");
        if checkpoint_path.exists() {
            let bytes = tokio::fs::read(&checkpoint_path).await.map_err(|e| {
                StreamlineError::Storage(format!("failed to read checkpoint: {e}"))
            })?;
            let snapshot: BTreeMap<String, serde_json::Value> =
                serde_json::from_slice(&bytes).map_err(|e| {
                    StreamlineError::CorruptedData(format!("corrupt checkpoint file: {e}"))
                })?;
            data = snapshot;
            debug!(entries = data.len(), "restored state from checkpoint");
        }

        // Replay WAL.
        let wal_path = dir.join("wal.jsonl");
        if wal_path.exists() {
            let file = tokio::fs::File::open(&wal_path).await.map_err(|e| {
                StreamlineError::Storage(format!("failed to open WAL: {e}"))
            })?;
            let reader = BufReader::new(file);
            let mut lines = reader.lines();
            let mut replayed: u64 = 0;
            while let Some(line) = lines.next_line().await.map_err(|e| {
                StreamlineError::Storage(format!("WAL read error: {e}"))
            })? {
                if line.trim().is_empty() {
                    continue;
                }
                match serde_json::from_str::<WalEntry>(&line) {
                    Ok(WalEntry::Put { key, value }) => {
                        data.insert(key, value);
                        replayed += 1;
                    }
                    Ok(WalEntry::Delete { key }) => {
                        data.remove(&key);
                        replayed += 1;
                    }
                    Err(e) => {
                        warn!(error = %e, "skipping corrupt WAL entry");
                    }
                }
            }
            if replayed > 0 {
                debug!(replayed, "replayed WAL entries");
            }
        }

        // Open WAL for appending.
        let wal_file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join("wal.jsonl"))
            .await
            .map_err(|e| StreamlineError::Storage(format!("failed to open WAL: {e}")))?;

        info!(dir = %dir.display(), entries = data.len(), "file state store opened");

        Ok(Self {
            dir,
            data: Arc::new(RwLock::new(data)),
            wal: Arc::new(RwLock::new(wal_file)),
        })
    }

    /// Append a WAL entry and flush to disk.
    async fn append_wal(&self, entry: &WalEntry) -> Result<()> {
        let mut line = serde_json::to_string(entry).map_err(|e| {
            StreamlineError::Storage(format!("WAL serialization error: {e}"))
        })?;
        line.push('\n');

        let mut wal = self.wal.write().await;
        wal.write_all(line.as_bytes()).await?;
        wal.flush().await?;
        Ok(())
    }
}

#[async_trait]
impl StateStore for FileStateStore {
    async fn get(&self, key: &str) -> Result<Option<serde_json::Value>> {
        let data = self.data.read().await;
        Ok(data.get(key).cloned())
    }

    async fn put(&self, key: &str, value: serde_json::Value) -> Result<()> {
        let entry = WalEntry::Put {
            key: key.to_string(),
            value: value.clone(),
        };
        self.append_wal(&entry).await?;

        let mut data = self.data.write().await;
        data.insert(key.to_string(), value);
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<bool> {
        let entry = WalEntry::Delete {
            key: key.to_string(),
        };
        self.append_wal(&entry).await?;

        let mut data = self.data.write().await;
        Ok(data.remove(key).is_some())
    }

    async fn scan(
        &self,
        prefix: Option<&str>,
        limit: Option<usize>,
    ) -> Result<Vec<(String, serde_json::Value)>> {
        let data = self.data.read().await;
        let owned_prefix = prefix.map(|p| p.to_string());
        let iter: Box<dyn Iterator<Item = (&String, &serde_json::Value)>> = match &owned_prefix {
            Some(p) => Box::new(data.range(p.clone()..).take_while(|(k, _)| k.starts_with(p.as_str()))),
            None => Box::new(data.iter()),
        };
        let entries: Vec<(String, serde_json::Value)> = match limit {
            Some(n) => iter.take(n).map(|(k, v)| (k.clone(), v.clone())).collect(),
            None => iter.map(|(k, v)| (k.clone(), v.clone())).collect(),
        };
        Ok(entries)
    }

    async fn checkpoint(&self) -> Result<Vec<u8>> {
        let data = self.data.read().await;
        let snapshot: BTreeMap<String, serde_json::Value> = data.clone();
        let bytes = serde_json::to_vec(&snapshot).map_err(|e| {
            StreamlineError::Storage(format!("failed to serialize checkpoint: {e}"))
        })?;

        // Write checkpoint file atomically (write tmp, then rename).
        let tmp_path = self.dir.join("checkpoint.json.tmp");
        let final_path = self.dir.join("checkpoint.json");
        tokio::fs::write(&tmp_path, &bytes).await.map_err(|e| {
            StreamlineError::Storage(format!("failed to write checkpoint: {e}"))
        })?;
        tokio::fs::rename(&tmp_path, &final_path).await.map_err(|e| {
            StreamlineError::Storage(format!("failed to finalize checkpoint: {e}"))
        })?;

        // Truncate the WAL since state is now fully captured in the checkpoint.
        drop(data); // release read lock before acquiring write
        let mut wal = self.wal.write().await;
        let wal_path = self.dir.join("wal.jsonl");
        let new_wal = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&wal_path)
            .await
            .map_err(|e| StreamlineError::Storage(format!("failed to truncate WAL: {e}")))?;
        *wal = new_wal;

        info!(dir = %self.dir.display(), bytes = bytes.len(), "checkpoint written, WAL truncated");
        Ok(bytes)
    }

    async fn restore(&self, data: &[u8]) -> Result<()> {
        let snapshot: BTreeMap<String, serde_json::Value> =
            serde_json::from_slice(data).map_err(|e| {
                StreamlineError::CorruptedData(format!("invalid checkpoint data: {e}"))
            })?;

        // Write the checkpoint file.
        let checkpoint_path = self.dir.join("checkpoint.json");
        tokio::fs::write(&checkpoint_path, data).await.map_err(|e| {
            StreamlineError::Storage(format!("failed to write checkpoint: {e}"))
        })?;

        // Truncate WAL.
        let wal_path = self.dir.join("wal.jsonl");
        let mut wal = self.wal.write().await;
        let new_wal = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&wal_path)
            .await
            .map_err(|e| StreamlineError::Storage(format!("failed to truncate WAL: {e}")))?;
        *wal = new_wal;

        // Replace in-memory state.
        let mut store = self.data.write().await;
        *store = snapshot;

        info!(dir = %self.dir.display(), "state restored from checkpoint");
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- MemoryStateStore --------------------------------------------------

    #[tokio::test]
    async fn test_memory_get_put_delete() {
        let store = MemoryStateStore::new();

        // Missing key returns None.
        assert!(store.get("k1").await.unwrap().is_none());

        // Put then get.
        store
            .put("k1", serde_json::json!(42))
            .await
            .unwrap();
        assert_eq!(store.get("k1").await.unwrap(), Some(serde_json::json!(42)));

        // Overwrite.
        store
            .put("k1", serde_json::json!("hello"))
            .await
            .unwrap();
        assert_eq!(
            store.get("k1").await.unwrap(),
            Some(serde_json::json!("hello"))
        );

        // Delete existing key.
        assert!(store.delete("k1").await.unwrap());
        assert!(store.get("k1").await.unwrap().is_none());

        // Delete non-existent key.
        assert!(!store.delete("k1").await.unwrap());
    }

    #[tokio::test]
    async fn test_memory_scan() {
        let store = MemoryStateStore::new();
        for i in 0..5 {
            store
                .put(&format!("user:{i}"), serde_json::json!(i))
                .await
                .unwrap();
        }
        store
            .put("order:1", serde_json::json!("a"))
            .await
            .unwrap();

        // Full scan.
        let all = store.scan(None, None).await.unwrap();
        assert_eq!(all.len(), 6);

        // Prefix scan.
        let users = store.scan(Some("user:"), None).await.unwrap();
        assert_eq!(users.len(), 5);

        // Prefix + limit.
        let limited = store.scan(Some("user:"), Some(2)).await.unwrap();
        assert_eq!(limited.len(), 2);
    }

    #[tokio::test]
    async fn test_memory_checkpoint_restore() {
        let store = MemoryStateStore::new();
        store
            .put("a", serde_json::json!(1))
            .await
            .unwrap();
        store
            .put("b", serde_json::json!(2))
            .await
            .unwrap();

        let snap = store.checkpoint().await.unwrap();

        // Mutate state after checkpoint.
        store.delete("a").await.unwrap();
        store
            .put("c", serde_json::json!(3))
            .await
            .unwrap();

        // Restore.
        store.restore(&snap).await.unwrap();
        assert_eq!(store.get("a").await.unwrap(), Some(serde_json::json!(1)));
        assert_eq!(store.get("b").await.unwrap(), Some(serde_json::json!(2)));
        assert!(store.get("c").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_memory_restore_invalid_data() {
        let store = MemoryStateStore::new();
        let result = store.restore(b"not json").await;
        assert!(result.is_err());
    }

    // -- FileStateStore ----------------------------------------------------

    #[tokio::test]
    async fn test_file_get_put_delete() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileStateStore::open(dir.path().to_path_buf()).await.unwrap();

        assert!(store.get("k1").await.unwrap().is_none());

        store
            .put("k1", serde_json::json!({"v": 1}))
            .await
            .unwrap();
        assert_eq!(
            store.get("k1").await.unwrap(),
            Some(serde_json::json!({"v": 1}))
        );

        assert!(store.delete("k1").await.unwrap());
        assert!(store.get("k1").await.unwrap().is_none());
        assert!(!store.delete("k1").await.unwrap());
    }

    #[tokio::test]
    async fn test_file_scan() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileStateStore::open(dir.path().to_path_buf()).await.unwrap();

        for i in 0..5 {
            store
                .put(&format!("item:{i}"), serde_json::json!(i))
                .await
                .unwrap();
        }

        let all = store.scan(None, None).await.unwrap();
        assert_eq!(all.len(), 5);

        let limited = store.scan(Some("item:"), Some(3)).await.unwrap();
        assert_eq!(limited.len(), 3);
    }

    #[tokio::test]
    async fn test_file_wal_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();

        // Write some data, then drop the store (simulating a crash).
        {
            let store = FileStateStore::open(path.clone()).await.unwrap();
            store
                .put("x", serde_json::json!("persisted"))
                .await
                .unwrap();
            store
                .put("y", serde_json::json!(99))
                .await
                .unwrap();
            store.delete("y").await.unwrap();
        }

        // Reopen and verify recovery from WAL.
        let store = FileStateStore::open(path).await.unwrap();
        assert_eq!(
            store.get("x").await.unwrap(),
            Some(serde_json::json!("persisted"))
        );
        assert!(store.get("y").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_file_checkpoint_and_wal_truncation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();

        let store = FileStateStore::open(path.clone()).await.unwrap();
        store
            .put("a", serde_json::json!(1))
            .await
            .unwrap();
        store
            .put("b", serde_json::json!(2))
            .await
            .unwrap();

        // Checkpoint should persist state and truncate WAL.
        let snap = store.checkpoint().await.unwrap();
        assert!(!snap.is_empty());

        // WAL should be empty after checkpoint.
        let wal_contents = tokio::fs::read_to_string(path.join("wal.jsonl"))
            .await
            .unwrap();
        assert!(wal_contents.is_empty());

        // Checkpoint file should exist.
        assert!(path.join("checkpoint.json").exists());

        // Write more data after checkpoint.
        store
            .put("c", serde_json::json!(3))
            .await
            .unwrap();
        drop(store);

        // Reopen: should recover from checkpoint + WAL.
        let store = FileStateStore::open(path).await.unwrap();
        assert_eq!(store.get("a").await.unwrap(), Some(serde_json::json!(1)));
        assert_eq!(store.get("b").await.unwrap(), Some(serde_json::json!(2)));
        assert_eq!(store.get("c").await.unwrap(), Some(serde_json::json!(3)));
    }

    #[tokio::test]
    async fn test_file_restore_from_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();

        // Build a snapshot from one store.
        let store1 = MemoryStateStore::new();
        store1.put("k", serde_json::json!("v")).await.unwrap();
        let snap = store1.checkpoint().await.unwrap();

        // Restore into a file store.
        let store2 = FileStateStore::open(path.clone()).await.unwrap();
        store2
            .put("old", serde_json::json!("gone"))
            .await
            .unwrap();
        store2.restore(&snap).await.unwrap();

        assert_eq!(
            store2.get("k").await.unwrap(),
            Some(serde_json::json!("v"))
        );
        assert!(store2.get("old").await.unwrap().is_none());

        // Reopen to confirm persistence.
        drop(store2);
        let store3 = FileStateStore::open(path).await.unwrap();
        assert_eq!(
            store3.get("k").await.unwrap(),
            Some(serde_json::json!("v"))
        );
    }

    // -- open_state_store factory ------------------------------------------

    #[tokio::test]
    async fn test_open_memory_store() {
        let store = open_state_store(&StateStoreConfig::Memory).await.unwrap();
        store.put("k", serde_json::json!(1)).await.unwrap();
        assert_eq!(store.get("k").await.unwrap(), Some(serde_json::json!(1)));
    }

    #[tokio::test]
    async fn test_open_file_store() {
        let dir = tempfile::tempdir().unwrap();
        let config = StateStoreConfig::File {
            dir: dir.path().to_path_buf(),
        };
        let store = open_state_store(&config).await.unwrap();
        store.put("k", serde_json::json!(1)).await.unwrap();
        assert_eq!(store.get("k").await.unwrap(), Some(serde_json::json!(1)));
    }
}
