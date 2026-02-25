//! Persistent Vector Store
//!
//! Provides file-backed persistence for vector embeddings, enabling
//! the AI pipeline to survive restarts without re-embedding all data.
//! Uses a simple append-only binary format with periodic compaction.

use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// Magic bytes for the vector store file format
const MAGIC: &[u8; 4] = b"SVEC";
/// Current file format version
const FORMAT_VERSION: u32 = 1;

/// Configuration for the persistent vector store
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistentVectorStoreConfig {
    /// Directory for storing vector data files
    pub data_dir: PathBuf,
    /// Maximum vectors to buffer in memory before flushing
    pub flush_threshold: usize,
    /// Whether to load existing data on startup
    pub load_on_startup: bool,
    /// Enable compaction to reclaim space from deleted vectors
    pub compaction_enabled: bool,
    /// Compaction threshold: ratio of deleted to total entries
    pub compaction_threshold: f64,
}

impl Default for PersistentVectorStoreConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data/vectors"),
            flush_threshold: 1000,
            load_on_startup: true,
            compaction_enabled: true,
            compaction_threshold: 0.3,
        }
    }
}

/// A stored vector with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredVector {
    /// Unique identifier for this vector
    pub id: String,
    /// The vector data
    pub vector: Vec<f32>,
    /// Topic this vector belongs to
    pub topic: Option<String>,
    /// Original text that was embedded
    pub text_preview: Option<String>,
    /// Timestamp when the vector was created (epoch ms)
    pub timestamp_ms: u64,
    /// Whether this entry has been deleted (tombstone)
    pub deleted: bool,
}

/// Statistics for the persistent vector store
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PersistentVectorStoreStats {
    pub total_vectors: usize,
    pub deleted_vectors: usize,
    pub dimensions: usize,
    pub disk_size_bytes: u64,
    pub memory_size_bytes: u64,
    pub flush_count: u64,
    pub load_time_ms: u64,
}

/// File-backed persistent vector store
pub struct PersistentVectorStore {
    config: PersistentVectorStoreConfig,
    /// In-memory index: id -> StoredVector
    vectors: Arc<RwLock<HashMap<String, StoredVector>>>,
    /// Pending writes buffer
    write_buffer: Arc<RwLock<Vec<StoredVector>>>,
    /// Statistics
    stats: Arc<RwLock<PersistentVectorStoreStats>>,
}

impl PersistentVectorStore {
    /// Create a new persistent vector store
    pub fn new(config: PersistentVectorStoreConfig) -> Result<Self> {
        // Ensure data directory exists
        if !config.data_dir.exists() {
            std::fs::create_dir_all(&config.data_dir).map_err(|e| {
                StreamlineError::Storage(format!(
                    "Failed to create vector store directory {:?}: {}",
                    config.data_dir, e
                ))
            })?;
        }

        let store = Self {
            config: config.clone(),
            vectors: Arc::new(RwLock::new(HashMap::new())),
            write_buffer: Arc::new(RwLock::new(Vec::new())),
            stats: Arc::new(RwLock::new(PersistentVectorStoreStats::default())),
        };

        Ok(store)
    }

    /// Load existing vectors from disk
    pub async fn load(&self) -> Result<usize> {
        let start = std::time::Instant::now();
        let data_file = self.data_file_path();

        if !data_file.exists() {
            info!("No existing vector store file found, starting fresh");
            return Ok(0);
        }

        let file = std::fs::File::open(&data_file).map_err(|e| {
            StreamlineError::Storage(format!("Failed to open vector store: {}", e))
        })?;
        let mut reader = BufReader::new(file);

        // Read and validate header
        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic).map_err(|e| {
            StreamlineError::Storage(format!("Failed to read vector store header: {}", e))
        })?;

        if &magic != MAGIC {
            return Err(StreamlineError::Storage(
                "Invalid vector store file (bad magic bytes)".into(),
            ));
        }

        let mut version_bytes = [0u8; 4];
        reader.read_exact(&mut version_bytes).map_err(|e| {
            StreamlineError::Storage(format!("Failed to read version: {}", e))
        })?;
        let version = u32::from_le_bytes(version_bytes);

        if version != FORMAT_VERSION {
            return Err(StreamlineError::Storage(format!(
                "Unsupported vector store version: {} (expected {})",
                version, FORMAT_VERSION
            )));
        }

        // Read entry count
        let mut count_bytes = [0u8; 8];
        reader.read_exact(&mut count_bytes).map_err(|e| {
            StreamlineError::Storage(format!("Failed to read entry count: {}", e))
        })?;
        let entry_count = u64::from_le_bytes(count_bytes);

        // Read all entries
        let mut vectors = self.vectors.write().await;
        let mut loaded = 0usize;
        let mut deleted = 0usize;

        let mut data = Vec::new();
        reader.read_to_end(&mut data).map_err(|e| {
            StreamlineError::Storage(format!("Failed to read vector data: {}", e))
        })?;

        let entries: Vec<StoredVector> = bincode::deserialize(&data).map_err(|e| {
            StreamlineError::Storage(format!("Failed to deserialize vectors: {}", e))
        })?;

        for entry in entries {
            if entry.deleted {
                deleted += 1;
                vectors.remove(&entry.id);
            } else {
                loaded += 1;
                vectors.insert(entry.id.clone(), entry);
            }
        }

        let elapsed = start.elapsed();
        let mut stats = self.stats.write().await;
        stats.total_vectors = vectors.len();
        stats.deleted_vectors = deleted;
        stats.load_time_ms = elapsed.as_millis() as u64;

        if !vectors.is_empty() {
            stats.dimensions = vectors.values().next().map(|v| v.vector.len()).unwrap_or(0);
        }

        info!(
            loaded,
            deleted,
            total = vectors.len(),
            elapsed_ms = elapsed.as_millis(),
            "Loaded vector store from disk"
        );

        Ok(loaded)
    }

    /// Store a vector
    pub async fn put(&self, id: String, vector: Vec<f32>, topic: Option<String>, text_preview: Option<String>) -> Result<()> {
        let entry = StoredVector {
            id: id.clone(),
            vector,
            topic,
            text_preview,
            timestamp_ms: chrono::Utc::now().timestamp_millis() as u64,
            deleted: false,
        };

        // Add to in-memory index
        {
            let mut vectors = self.vectors.write().await;
            vectors.insert(id, entry.clone());
        }

        // Add to write buffer
        {
            let mut buffer = self.write_buffer.write().await;
            buffer.push(entry);

            if buffer.len() >= self.config.flush_threshold {
                let entries: Vec<StoredVector> = buffer.drain(..).collect();
                drop(buffer);
                self.flush_entries(&entries).await?;
            }
        }

        let mut stats = self.stats.write().await;
        stats.total_vectors += 1;

        Ok(())
    }

    /// Delete a vector by ID
    pub async fn delete(&self, id: &str) -> Result<bool> {
        let existed = {
            let mut vectors = self.vectors.write().await;
            vectors.remove(id).is_some()
        };

        if existed {
            // Write tombstone
            let tombstone = StoredVector {
                id: id.to_string(),
                vector: vec![],
                topic: None,
                text_preview: None,
                timestamp_ms: chrono::Utc::now().timestamp_millis() as u64,
                deleted: true,
            };

            let mut buffer = self.write_buffer.write().await;
            buffer.push(tombstone);

            let mut stats = self.stats.write().await;
            stats.total_vectors = stats.total_vectors.saturating_sub(1);
            stats.deleted_vectors += 1;
        }

        Ok(existed)
    }

    /// Get a vector by ID
    pub async fn get(&self, id: &str) -> Option<StoredVector> {
        let vectors = self.vectors.read().await;
        vectors.get(id).cloned()
    }

    /// Search for nearest vectors using brute-force cosine similarity
    pub async fn search_nearest(&self, query: &[f32], limit: usize) -> Vec<(String, f32)> {
        let vectors = self.vectors.read().await;
        let mut scores: Vec<(String, f32)> = vectors
            .values()
            .filter(|v| v.vector.len() == query.len())
            .map(|v| {
                let sim = cosine_similarity(query, &v.vector);
                (v.id.clone(), sim)
            })
            .collect();

        scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scores.truncate(limit);
        scores
    }

    /// Search vectors within a specific topic
    pub async fn search_in_topic(
        &self,
        topic: &str,
        query: &[f32],
        limit: usize,
    ) -> Vec<(String, f32)> {
        let vectors = self.vectors.read().await;
        let mut scores: Vec<(String, f32)> = vectors
            .values()
            .filter(|v| v.topic.as_deref() == Some(topic) && v.vector.len() == query.len())
            .map(|v| {
                let sim = cosine_similarity(query, &v.vector);
                (v.id.clone(), sim)
            })
            .collect();

        scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scores.truncate(limit);
        scores
    }

    /// Flush pending writes to disk
    pub async fn flush(&self) -> Result<()> {
        let entries: Vec<StoredVector> = {
            let mut buffer = self.write_buffer.write().await;
            buffer.drain(..).collect()
        };

        if !entries.is_empty() {
            self.flush_entries(&entries).await?;
        }

        Ok(())
    }

    /// Save the complete vector store to disk (full snapshot)
    pub async fn snapshot(&self) -> Result<()> {
        let vectors = self.vectors.read().await;
        let entries: Vec<StoredVector> = vectors.values().cloned().collect();
        drop(vectors);

        let data_file = self.data_file_path();
        let tmp_file = data_file.with_extension("tmp");

        let file = std::fs::File::create(&tmp_file).map_err(|e| {
            StreamlineError::Storage(format!("Failed to create snapshot file: {}", e))
        })?;
        let mut writer = BufWriter::new(file);

        // Write header
        writer.write_all(MAGIC).map_err(|e| {
            StreamlineError::Storage(format!("Failed to write header: {}", e))
        })?;
        writer
            .write_all(&FORMAT_VERSION.to_le_bytes())
            .map_err(|e| {
                StreamlineError::Storage(format!("Failed to write version: {}", e))
            })?;
        writer
            .write_all(&(entries.len() as u64).to_le_bytes())
            .map_err(|e| {
                StreamlineError::Storage(format!("Failed to write count: {}", e))
            })?;

        // Write entries
        let data = bincode::serialize(&entries).map_err(|e| {
            StreamlineError::Storage(format!("Failed to serialize vectors: {}", e))
        })?;
        writer.write_all(&data).map_err(|e| {
            StreamlineError::Storage(format!("Failed to write vector data: {}", e))
        })?;
        writer.flush().map_err(|e| {
            StreamlineError::Storage(format!("Failed to flush snapshot: {}", e))
        })?;

        // Atomic rename
        std::fs::rename(&tmp_file, &data_file).map_err(|e| {
            StreamlineError::Storage(format!("Failed to finalize snapshot: {}", e))
        })?;

        let mut stats = self.stats.write().await;
        stats.flush_count += 1;
        stats.disk_size_bytes = std::fs::metadata(&data_file)
            .map(|m| m.len())
            .unwrap_or(0);
        stats.deleted_vectors = 0; // Compaction happened via snapshot

        info!(
            vectors = entries.len(),
            size_bytes = stats.disk_size_bytes,
            "Vector store snapshot saved"
        );

        Ok(())
    }

    /// Get store statistics
    pub async fn stats(&self) -> PersistentVectorStoreStats {
        self.stats.read().await.clone()
    }

    /// Get total vector count
    pub async fn len(&self) -> usize {
        self.vectors.read().await.len()
    }

    /// Check if store is empty
    pub async fn is_empty(&self) -> bool {
        self.vectors.read().await.is_empty()
    }

    fn data_file_path(&self) -> PathBuf {
        self.config.data_dir.join("vectors.svec")
    }

    async fn flush_entries(&self, entries: &[StoredVector]) -> Result<()> {
        // For incremental flushes, we do a full snapshot
        // (A production implementation would use an append-only log)
        self.snapshot().await
    }
}

/// Compute cosine similarity between two vectors
fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }

    let mut dot = 0.0f32;
    let mut norm_a = 0.0f32;
    let mut norm_b = 0.0f32;

    for i in 0..a.len() {
        dot += a[i] * b[i];
        norm_a += a[i] * a[i];
        norm_b += b[i] * b[i];
    }

    let denom = norm_a.sqrt() * norm_b.sqrt();
    if denom == 0.0 {
        0.0
    } else {
        dot / denom
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_persistent_store_basic() {
        let dir = tempdir().unwrap();
        let config = PersistentVectorStoreConfig {
            data_dir: dir.path().to_path_buf(),
            flush_threshold: 100,
            ..Default::default()
        };

        let store = PersistentVectorStore::new(config).unwrap();

        // Insert vectors
        store
            .put("v1".to_string(), vec![1.0, 0.0, 0.0], Some("topic-a".to_string()), None)
            .await
            .unwrap();
        store
            .put("v2".to_string(), vec![0.0, 1.0, 0.0], Some("topic-a".to_string()), None)
            .await
            .unwrap();
        store
            .put("v3".to_string(), vec![0.9, 0.1, 0.0], Some("topic-b".to_string()), None)
            .await
            .unwrap();

        assert_eq!(store.len().await, 3);

        // Search nearest
        let results = store.search_nearest(&[1.0, 0.0, 0.0], 2).await;
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, "v1"); // Exact match
        assert!(results[0].1 > 0.99);

        // Search in topic
        let results = store.search_in_topic("topic-a", &[1.0, 0.0, 0.0], 10).await;
        assert_eq!(results.len(), 2);
    }

    #[tokio::test]
    async fn test_persistent_store_snapshot_and_load() {
        let dir = tempdir().unwrap();
        let config = PersistentVectorStoreConfig {
            data_dir: dir.path().to_path_buf(),
            flush_threshold: 100,
            ..Default::default()
        };

        // Create and populate
        {
            let store = PersistentVectorStore::new(config.clone()).unwrap();
            store.put("v1".to_string(), vec![1.0, 2.0, 3.0], None, None).await.unwrap();
            store.put("v2".to_string(), vec![4.0, 5.0, 6.0], None, None).await.unwrap();
            store.snapshot().await.unwrap();
        }

        // Load in new instance
        {
            let store = PersistentVectorStore::new(config).unwrap();
            let loaded = store.load().await.unwrap();
            assert_eq!(loaded, 2);
            assert_eq!(store.len().await, 2);

            let v1 = store.get("v1").await.unwrap();
            assert_eq!(v1.vector, vec![1.0, 2.0, 3.0]);
        }
    }

    #[tokio::test]
    async fn test_persistent_store_delete() {
        let dir = tempdir().unwrap();
        let config = PersistentVectorStoreConfig {
            data_dir: dir.path().to_path_buf(),
            flush_threshold: 100,
            ..Default::default()
        };

        let store = PersistentVectorStore::new(config).unwrap();
        store.put("v1".to_string(), vec![1.0], None, None).await.unwrap();
        store.put("v2".to_string(), vec![2.0], None, None).await.unwrap();

        assert!(store.delete("v1").await.unwrap());
        assert!(!store.delete("v999").await.unwrap());
        assert_eq!(store.len().await, 1);
        assert!(store.get("v1").await.is_none());
    }

    #[test]
    fn test_cosine_similarity() {
        assert!((cosine_similarity(&[1.0, 0.0], &[1.0, 0.0]) - 1.0).abs() < 0.001);
        assert!((cosine_similarity(&[1.0, 0.0], &[0.0, 1.0])).abs() < 0.001);
        assert!((cosine_similarity(&[1.0, 0.0], &[-1.0, 0.0]) + 1.0).abs() < 0.001);
        assert_eq!(cosine_similarity(&[], &[]), 0.0);
    }
}
