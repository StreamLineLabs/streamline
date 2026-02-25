//! Vector Embeddings Engine
//!
//! Generates and manages vector embeddings for stream data.

use super::config::{EmbeddingConfig, EmbeddingModelType};
use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Embedding engine for generating vector embeddings
pub struct EmbeddingEngine {
    /// Configuration
    config: EmbeddingConfig,
    /// Embedding model
    model: EmbeddingModel,
    /// Embedding cache
    cache: Arc<RwLock<EmbeddingCache>>,
}

impl EmbeddingEngine {
    /// Create a new embedding engine
    pub fn new(config: EmbeddingConfig) -> Result<Self> {
        let model = EmbeddingModel::new(&config)?;

        let cache = if config.cache_enabled {
            Arc::new(RwLock::new(EmbeddingCache::new(config.cache_size)))
        } else {
            Arc::new(RwLock::new(EmbeddingCache::new(0)))
        };

        Ok(Self {
            config,
            model,
            cache,
        })
    }

    /// Generate embedding for a single text
    pub async fn embed_text(&self, text: &str) -> Result<EmbeddingResult> {
        // Check cache first
        if self.config.cache_enabled {
            let cache = self.cache.read().await;
            if let Some(embedding) = cache.get(text) {
                return Ok(embedding.clone());
            }
        }

        // Generate embedding
        let embedding = self.model.embed(text)?;

        // Normalize if configured
        let vector = if self.config.normalize {
            normalize_vector(&embedding)
        } else {
            embedding
        };

        let result = EmbeddingResult {
            vector,
            dimension: self.config.dimension,
            model: self.model.name().to_string(),
        };

        // Cache result
        if self.config.cache_enabled {
            let mut cache = self.cache.write().await;
            cache.put(text.to_string(), result.clone());
        }

        Ok(result)
    }

    /// Generate embeddings for multiple texts
    pub async fn embed_batch(&self, texts: &[&str]) -> Result<Vec<EmbeddingResult>> {
        let mut results = Vec::with_capacity(texts.len());

        // Process in batches
        for chunk in texts.chunks(self.config.batch_size) {
            for text in chunk {
                results.push(self.embed_text(text).await?);
            }
        }

        Ok(results)
    }

    /// Get embedding dimension
    pub fn dimension(&self) -> usize {
        self.config.dimension
    }

    /// Compute cosine similarity between two embeddings
    pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
        if a.len() != b.len() {
            return 0.0;
        }

        let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
        let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
        let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();

        if norm_a == 0.0 || norm_b == 0.0 {
            0.0
        } else {
            dot / (norm_a * norm_b)
        }
    }

    /// Compute Euclidean distance between two embeddings
    pub fn euclidean_distance(a: &[f32], b: &[f32]) -> f32 {
        if a.len() != b.len() {
            return f32::MAX;
        }

        a.iter()
            .zip(b.iter())
            .map(|(x, y)| (x - y).powi(2))
            .sum::<f32>()
            .sqrt()
    }
}

/// Embedding model abstraction
pub struct EmbeddingModel {
    /// Model type
    model_type: EmbeddingModelType,
    /// Dimension
    dimension: usize,
    /// TF-IDF vocabulary (if using TF-IDF)
    #[allow(dead_code)]
    vocabulary: Option<HashMap<String, usize>>,
    /// IDF values
    #[allow(dead_code)]
    idf: Option<Vec<f32>>,
}

impl EmbeddingModel {
    /// Create a new embedding model
    pub fn new(config: &EmbeddingConfig) -> Result<Self> {
        Ok(Self {
            model_type: config.model.clone(),
            dimension: config.dimension,
            vocabulary: None,
            idf: None,
        })
    }

    /// Get model name
    pub fn name(&self) -> &str {
        match self.model_type {
            EmbeddingModelType::SimpleHash => "simple_hash",
            EmbeddingModelType::TfIdf => "tf_idf",
            EmbeddingModelType::ExternalApi => "external_api",
            EmbeddingModelType::SentenceTransformer => "sentence_transformer",
        }
    }

    /// Generate embedding for text
    ///
    /// For ExternalApi and SentenceTransformer models, use the async
    /// `EmbeddingProvider` trait implementations in `providers` module instead.
    pub fn embed(&self, text: &str) -> Result<Vec<f32>> {
        match self.model_type {
            EmbeddingModelType::SimpleHash => self.simple_hash_embed(text),
            EmbeddingModelType::TfIdf => self.tfidf_embed(text),
            EmbeddingModelType::ExternalApi => Err(StreamlineError::Config(
                "ExternalApi model requires async execution. Use OpenAIProvider or CohereProvider from the providers module instead.".into(),
            )),
            EmbeddingModelType::SentenceTransformer => Err(StreamlineError::Config(
                "SentenceTransformer model requires the ONNX runtime. Use SimpleHash or ExternalApi (with OpenAI/Cohere provider) instead.".into(),
            )),
        }
    }

    /// Simple hash-based embedding (for testing/development)
    fn simple_hash_embed(&self, text: &str) -> Result<Vec<f32>> {
        let mut embedding = vec![0.0f32; self.dimension];

        // Tokenize
        let text_lower = text.to_lowercase();
        let tokens: Vec<&str> = text_lower
            .split(|c: char| !c.is_alphanumeric())
            .filter(|s| !s.is_empty())
            .collect();

        // Hash each token to a position in the embedding
        for token in tokens {
            let hash = simple_hash(token);
            let pos = (hash as usize) % self.dimension;
            embedding[pos] += 1.0;

            // Also add n-gram features
            if token.len() >= 3 {
                for i in 0..token.len() - 2 {
                    let ngram = &token[i..i + 3];
                    let ngram_hash = simple_hash(ngram);
                    let ngram_pos = (ngram_hash as usize) % self.dimension;
                    embedding[ngram_pos] += 0.5;
                }
            }
        }

        Ok(embedding)
    }

    /// TF-IDF based embedding
    fn tfidf_embed(&self, text: &str) -> Result<Vec<f32>> {
        // Simplified TF-IDF - in production would use pre-computed IDF
        let mut embedding = vec![0.0f32; self.dimension];

        let tokens: Vec<String> = text
            .to_lowercase()
            .split(|c: char| !c.is_alphanumeric())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();

        // Count term frequencies
        let mut tf: HashMap<String, f32> = HashMap::new();
        for token in &tokens {
            *tf.entry(token.clone()).or_insert(0.0) += 1.0;
        }

        // Normalize TF
        let doc_len = tokens.len() as f32;
        for count in tf.values_mut() {
            *count /= doc_len;
        }

        // Hash tokens to embedding positions
        for (token, freq) in tf {
            let hash = simple_hash(&token);
            let pos = (hash as usize) % self.dimension;
            embedding[pos] += freq;
        }

        Ok(embedding)
    }
}

/// Simple hash function for strings
fn simple_hash(s: &str) -> u32 {
    let mut hash: u32 = 5381;
    for c in s.bytes() {
        hash = hash.wrapping_mul(33).wrapping_add(c as u32);
    }
    hash
}

/// Normalize a vector to unit length
fn normalize_vector(v: &[f32]) -> Vec<f32> {
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm == 0.0 {
        v.to_vec()
    } else {
        v.iter().map(|x| x / norm).collect()
    }
}

/// Embedding result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingResult {
    /// The embedding vector
    pub vector: Vec<f32>,
    /// Dimension
    pub dimension: usize,
    /// Model used
    pub model: String,
}

/// Embedding cache using LRU eviction
pub struct EmbeddingCache {
    /// Cache entries
    entries: HashMap<String, (EmbeddingResult, u64)>,
    /// Access counter
    counter: u64,
    /// Maximum size
    max_size: usize,
}

impl EmbeddingCache {
    /// Create a new cache
    pub fn new(max_size: usize) -> Self {
        Self {
            entries: HashMap::new(),
            counter: 0,
            max_size,
        }
    }

    /// Get an entry from cache
    pub fn get(&self, key: &str) -> Option<&EmbeddingResult> {
        self.entries.get(key).map(|(v, _)| v)
    }

    /// Put an entry in cache
    pub fn put(&mut self, key: String, value: EmbeddingResult) {
        if self.max_size == 0 {
            return;
        }

        // Evict if full
        if self.entries.len() >= self.max_size {
            self.evict_oldest();
        }

        self.counter += 1;
        self.entries.insert(key, (value, self.counter));
    }

    /// Evict oldest entry
    fn evict_oldest(&mut self) {
        if let Some(oldest_key) = self
            .entries
            .iter()
            .min_by_key(|(_, (_, count))| count)
            .map(|(k, _)| k.clone())
        {
            self.entries.remove(&oldest_key);
        }
    }
}

/// Vector store for storing and retrieving embeddings
pub struct VectorStore {
    /// Stored vectors
    vectors: Vec<StoredVector>,
    /// Index for fast lookup
    index: VectorIndex,
}

impl VectorStore {
    /// Create a new vector store
    pub fn new(dimension: usize) -> Self {
        Self {
            vectors: Vec::new(),
            index: VectorIndex::new(dimension),
        }
    }

    /// Add a vector to the store
    pub fn add(&mut self, id: String, vector: Vec<f32>, metadata: Option<serde_json::Value>) {
        let stored = StoredVector {
            id: id.clone(),
            vector: vector.clone(),
            metadata,
        };

        self.vectors.push(stored);
        self.index.add(&id, &vector);
    }

    /// Search for nearest neighbors
    pub fn search(&self, query: &[f32], k: usize) -> Vec<SearchHit> {
        self.index.search(query, k, &self.vectors)
    }

    /// Get vector by ID
    pub fn get(&self, id: &str) -> Option<&StoredVector> {
        self.vectors.iter().find(|v| v.id == id)
    }

    /// Get number of vectors
    pub fn len(&self) -> usize {
        self.vectors.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.vectors.is_empty()
    }

    /// Remove a vector by ID
    pub fn remove(&mut self, id: &str) -> bool {
        if let Some(pos) = self.vectors.iter().position(|v| v.id == id) {
            self.vectors.swap_remove(pos);
            self.index.remove(id);
            true
        } else {
            false
        }
    }

    /// Batch add vectors for bulk ingestion
    pub fn add_batch(&mut self, entries: Vec<(String, Vec<f32>, Option<serde_json::Value>)>) {
        for (id, vector, metadata) in entries {
            self.add(id, vector, metadata);
        }
    }

    /// Search with minimum similarity threshold
    pub fn search_with_threshold(
        &self,
        query: &[f32],
        k: usize,
        min_score: f32,
    ) -> Vec<SearchHit> {
        self.search(query, k)
            .into_iter()
            .filter(|hit| hit.score >= min_score)
            .collect()
    }

    /// Get statistics about the vector store
    pub fn stats(&self) -> VectorStoreMetrics {
        VectorStoreMetrics {
            total_vectors: self.vectors.len(),
            dimension: self.index.dimension,
            memory_bytes_estimate: self.vectors.len()
                * (std::mem::size_of::<StoredVector>() + self.index.dimension * 4),
        }
    }
}

/// Metrics for a vector store instance
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorStoreMetrics {
    pub total_vectors: usize,
    pub dimension: usize,
    pub memory_bytes_estimate: usize,
}

/// Stored vector with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredVector {
    /// Unique ID
    pub id: String,
    /// Vector embedding
    pub vector: Vec<f32>,
    /// Optional metadata
    pub metadata: Option<serde_json::Value>,
}

/// Vector index for fast similarity search
pub struct VectorIndex {
    /// Dimension
    #[allow(dead_code)]
    dimension: usize,
    /// Vector IDs and their positions
    positions: HashMap<String, usize>,
}

impl VectorIndex {
    /// Create a new index
    pub fn new(dimension: usize) -> Self {
        Self {
            dimension,
            positions: HashMap::new(),
        }
    }

    /// Add a vector to the index
    pub fn add(&mut self, id: &str, _vector: &[f32]) {
        let pos = self.positions.len();
        self.positions.insert(id.to_string(), pos);
    }

    /// Remove a vector from the index
    pub fn remove(&mut self, id: &str) {
        self.positions.remove(id);
    }

    /// Search for nearest neighbors (brute force for now)
    pub fn search(&self, query: &[f32], k: usize, vectors: &[StoredVector]) -> Vec<SearchHit> {
        let mut hits: Vec<SearchHit> = vectors
            .iter()
            .map(|v| {
                let similarity = EmbeddingEngine::cosine_similarity(query, &v.vector);
                SearchHit {
                    id: v.id.clone(),
                    score: similarity,
                    metadata: v.metadata.clone(),
                }
            })
            .collect();

        // Sort by score descending
        hits.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        hits.truncate(k);
        hits
    }
}

/// Search hit result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchHit {
    /// Vector ID
    pub id: String,
    /// Similarity score
    pub score: f32,
    /// Associated metadata
    pub metadata: Option<serde_json::Value>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_hash() {
        let hash1 = simple_hash("hello");
        let hash2 = simple_hash("hello");
        let hash3 = simple_hash("world");

        assert_eq!(hash1, hash2);
        assert_ne!(hash1, hash3);
    }

    #[test]
    fn test_cosine_similarity() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![1.0, 0.0, 0.0];
        let c = vec![0.0, 1.0, 0.0];

        assert!((EmbeddingEngine::cosine_similarity(&a, &b) - 1.0).abs() < 0.001);
        assert!((EmbeddingEngine::cosine_similarity(&a, &c) - 0.0).abs() < 0.001);
    }

    #[test]
    fn test_normalize_vector() {
        let v = vec![3.0, 4.0];
        let normalized = normalize_vector(&v);

        let norm: f32 = normalized.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 0.001);
    }

    #[tokio::test]
    async fn test_embedding_engine() {
        let config = EmbeddingConfig::default();
        let engine = EmbeddingEngine::new(config).unwrap();

        let result = engine.embed_text("hello world").await.unwrap();
        assert_eq!(result.dimension, 384);
        assert_eq!(result.vector.len(), 384);
    }

    #[test]
    fn test_vector_store() {
        let mut store = VectorStore::new(3);

        store.add("vec1".to_string(), vec![1.0, 0.0, 0.0], None);
        store.add("vec2".to_string(), vec![0.9, 0.1, 0.0], None);
        store.add("vec3".to_string(), vec![0.0, 1.0, 0.0], None);

        let results = store.search(&[1.0, 0.0, 0.0], 2);

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].id, "vec1");
        assert_eq!(results[1].id, "vec2");
    }
}
