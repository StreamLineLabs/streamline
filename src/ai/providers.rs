//! Embedding Providers for Semantic Streaming
//!
//! Production-ready embedding providers for generating vector embeddings.
//! Supports multiple backends including OpenAI, Cohere, and local ONNX models.

use crate::error::{Result, StreamlineError};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Trait for embedding providers
#[async_trait]
pub trait EmbeddingProvider: Send + Sync {
    /// Get provider name
    fn name(&self) -> &str;

    /// Get model identifier
    fn model_id(&self) -> &str;

    /// Get embedding dimension
    fn dimension(&self) -> usize;

    /// Generate embedding for a single text
    async fn embed(&self, text: &str) -> Result<Vec<f32>>;

    /// Generate embeddings for multiple texts (batch)
    async fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>>;

    /// Get provider statistics
    fn stats(&self) -> ProviderStats;
}

/// Provider statistics
#[derive(Debug, Clone, Default)]
pub struct ProviderStats {
    /// Total requests made
    pub total_requests: u64,
    /// Total texts embedded
    pub total_texts: u64,
    /// Total tokens processed (if available)
    pub total_tokens: u64,
    /// Cache hits
    pub cache_hits: u64,
    /// Errors encountered
    pub errors: u64,
    /// Average latency in milliseconds
    pub avg_latency_ms: f64,
}

/// Quantization type for vector storage
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum QuantizationType {
    /// Full precision float32
    #[default]
    Float32,
    /// Quantized to int8
    Int8,
    /// Binary quantization
    Binary,
}

/// Configuration for embedding providers
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderConfig {
    /// Provider type
    pub provider_type: ProviderType,
    /// API key (for cloud providers)
    pub api_key: Option<String>,
    /// API base URL (optional override)
    pub api_base: Option<String>,
    /// Model identifier
    pub model_id: String,
    /// Maximum batch size
    pub max_batch_size: usize,
    /// Request timeout in milliseconds
    pub timeout_ms: u64,
    /// Enable caching
    pub cache_enabled: bool,
    /// Maximum cache size
    pub cache_size: usize,
    /// Quantization type
    pub quantization: QuantizationType,
}

impl Default for ProviderConfig {
    fn default() -> Self {
        Self {
            provider_type: ProviderType::MockProvider,
            api_key: None,
            api_base: None,
            model_id: "mock-embedding".to_string(),
            max_batch_size: 100,
            timeout_ms: 30_000,
            cache_enabled: true,
            cache_size: 10_000,
            quantization: QuantizationType::Float32,
        }
    }
}

/// Provider types
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProviderType {
    /// OpenAI embeddings API
    OpenAI,
    /// Cohere embeddings API
    Cohere,
    /// Local ONNX Runtime model
    Onnx,
    /// Mock provider for testing
    MockProvider,
}

/// OpenAI embedding provider
pub struct OpenAIProvider {
    config: ProviderConfig,
    client: reqwest::Client,
    stats: ProviderStatsInternal,
    cache: Option<Arc<RwLock<EmbeddingCache>>>,
}

impl OpenAIProvider {
    /// Create a new OpenAI provider
    pub fn new(config: ProviderConfig) -> Result<Self> {
        if config.api_key.is_none() {
            return Err(StreamlineError::Config("OpenAI API key is required".into()));
        }

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_millis(config.timeout_ms))
            .build()
            .map_err(|e| StreamlineError::Config(format!("Failed to create HTTP client: {}", e)))?;

        let cache = if config.cache_enabled {
            Some(Arc::new(RwLock::new(EmbeddingCache::new(
                config.cache_size,
            ))))
        } else {
            None
        };

        Ok(Self {
            config,
            client,
            stats: ProviderStatsInternal::default(),
            cache,
        })
    }

    /// Get embedding dimension based on model
    fn get_dimension(&self) -> usize {
        match self.config.model_id.as_str() {
            "text-embedding-3-small" => 1536,
            "text-embedding-3-large" => 3072,
            "text-embedding-ada-002" => 1536,
            _ => 1536, // Default
        }
    }
}

#[async_trait]
impl EmbeddingProvider for OpenAIProvider {
    fn name(&self) -> &str {
        "openai"
    }

    fn model_id(&self) -> &str {
        &self.config.model_id
    }

    fn dimension(&self) -> usize {
        self.get_dimension()
    }

    async fn embed(&self, text: &str) -> Result<Vec<f32>> {
        // Check cache first
        if let Some(ref cache) = self.cache {
            let cache_guard = cache.read().await;
            if let Some(embedding) = cache_guard.get(text) {
                self.stats.cache_hits.fetch_add(1, Ordering::Relaxed);
                return Ok(embedding.clone());
            }
        }

        let start = std::time::Instant::now();
        self.stats.total_requests.fetch_add(1, Ordering::Relaxed);
        self.stats.total_texts.fetch_add(1, Ordering::Relaxed);

        let api_base = self
            .config
            .api_base
            .as_deref()
            .unwrap_or("https://api.openai.com/v1");

        let response = self
            .client
            .post(format!("{}/embeddings", api_base))
            .header(
                "Authorization",
                format!("Bearer {}", self.config.api_key.as_deref().unwrap_or("")),
            )
            .json(&serde_json::json!({
                "model": self.config.model_id,
                "input": text,
            }))
            .send()
            .await
            .map_err(|e| StreamlineError::Network(format!("OpenAI request failed: {}", e)))?;

        if !response.status().is_success() {
            self.stats.errors.fetch_add(1, Ordering::Relaxed);
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(StreamlineError::Network(format!(
                "OpenAI API error {}: {}",
                status, body
            )));
        }

        let data: OpenAIEmbeddingResponse = response
            .json()
            .await
            .map_err(|e| StreamlineError::Network(format!("Failed to parse response: {}", e)))?;

        let embedding = data
            .data
            .first()
            .ok_or_else(|| StreamlineError::Network("No embedding in response".into()))?
            .embedding
            .clone();

        // Update latency stats
        let elapsed = start.elapsed().as_millis() as u64;
        self.stats.update_latency(elapsed);

        // Cache result
        if let Some(ref cache) = self.cache {
            let mut cache_guard = cache.write().await;
            cache_guard.put(text.to_string(), embedding.clone());
        }

        Ok(embedding)
    }

    async fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>> {
        let mut results = Vec::with_capacity(texts.len());

        // Process in chunks respecting max batch size
        for chunk in texts.chunks(self.config.max_batch_size) {
            let start = std::time::Instant::now();
            self.stats.total_requests.fetch_add(1, Ordering::Relaxed);
            self.stats
                .total_texts
                .fetch_add(chunk.len() as u64, Ordering::Relaxed);

            let api_base = self
                .config
                .api_base
                .as_deref()
                .unwrap_or("https://api.openai.com/v1");

            let response = self
                .client
                .post(format!("{}/embeddings", api_base))
                .header(
                    "Authorization",
                    format!("Bearer {}", self.config.api_key.as_deref().unwrap_or("")),
                )
                .json(&serde_json::json!({
                    "model": self.config.model_id,
                    "input": chunk,
                }))
                .send()
                .await
                .map_err(|e| StreamlineError::Network(format!("OpenAI request failed: {}", e)))?;

            if !response.status().is_success() {
                self.stats.errors.fetch_add(1, Ordering::Relaxed);
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(StreamlineError::Network(format!(
                    "OpenAI API error {}: {}",
                    status, body
                )));
            }

            let data: OpenAIEmbeddingResponse = response.json().await.map_err(|e| {
                StreamlineError::Network(format!("Failed to parse response: {}", e))
            })?;

            // Sort by index to maintain order
            let mut embeddings: Vec<_> = data.data.into_iter().collect();
            embeddings.sort_by_key(|e| e.index);

            for emb in embeddings {
                results.push(emb.embedding);
            }

            let elapsed = start.elapsed().as_millis() as u64;
            self.stats.update_latency(elapsed);
        }

        Ok(results)
    }

    fn stats(&self) -> ProviderStats {
        self.stats.snapshot()
    }
}

/// OpenAI API response types
#[derive(Debug, Deserialize)]
struct OpenAIEmbeddingResponse {
    data: Vec<OpenAIEmbedding>,
    #[allow(dead_code)]
    usage: Option<OpenAIUsage>,
}

#[derive(Debug, Deserialize)]
struct OpenAIEmbedding {
    embedding: Vec<f32>,
    index: usize,
}

#[derive(Debug, Deserialize)]
struct OpenAIUsage {
    #[allow(dead_code)]
    prompt_tokens: u64,
    #[allow(dead_code)]
    total_tokens: u64,
}

/// Cohere embedding provider
pub struct CohereProvider {
    config: ProviderConfig,
    client: reqwest::Client,
    stats: ProviderStatsInternal,
    cache: Option<Arc<RwLock<EmbeddingCache>>>,
}

impl CohereProvider {
    /// Create a new Cohere provider
    pub fn new(config: ProviderConfig) -> Result<Self> {
        if config.api_key.is_none() {
            return Err(StreamlineError::Config("Cohere API key is required".into()));
        }

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_millis(config.timeout_ms))
            .build()
            .map_err(|e| StreamlineError::Config(format!("Failed to create HTTP client: {}", e)))?;

        let cache = if config.cache_enabled {
            Some(Arc::new(RwLock::new(EmbeddingCache::new(
                config.cache_size,
            ))))
        } else {
            None
        };

        Ok(Self {
            config,
            client,
            stats: ProviderStatsInternal::default(),
            cache,
        })
    }

    fn get_dimension(&self) -> usize {
        match self.config.model_id.as_str() {
            "embed-english-v3.0" => 1024,
            "embed-multilingual-v3.0" => 1024,
            "embed-english-light-v3.0" => 384,
            "embed-multilingual-light-v3.0" => 384,
            _ => 1024,
        }
    }
}

#[async_trait]
impl EmbeddingProvider for CohereProvider {
    fn name(&self) -> &str {
        "cohere"
    }

    fn model_id(&self) -> &str {
        &self.config.model_id
    }

    fn dimension(&self) -> usize {
        self.get_dimension()
    }

    async fn embed(&self, text: &str) -> Result<Vec<f32>> {
        let results = self.embed_batch(&[text]).await?;
        results
            .into_iter()
            .next()
            .ok_or_else(|| StreamlineError::Network("No embedding returned".into()))
    }

    async fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>> {
        // Check cache for all texts
        let mut results = vec![None; texts.len()];
        let mut uncached_indices = Vec::new();
        let mut uncached_texts = Vec::new();

        if let Some(ref cache) = self.cache {
            let cache_guard = cache.read().await;
            for (i, text) in texts.iter().enumerate() {
                if let Some(embedding) = cache_guard.get(text) {
                    results[i] = Some(embedding.clone());
                    self.stats.cache_hits.fetch_add(1, Ordering::Relaxed);
                } else {
                    uncached_indices.push(i);
                    uncached_texts.push(*text);
                }
            }
        } else {
            uncached_indices = (0..texts.len()).collect();
            uncached_texts = texts.to_vec();
        }

        if uncached_texts.is_empty() {
            return Ok(results.into_iter().flatten().collect());
        }

        let start = std::time::Instant::now();
        self.stats.total_requests.fetch_add(1, Ordering::Relaxed);
        self.stats
            .total_texts
            .fetch_add(uncached_texts.len() as u64, Ordering::Relaxed);

        let api_base = self
            .config
            .api_base
            .as_deref()
            .unwrap_or("https://api.cohere.ai/v1");

        let response = self
            .client
            .post(format!("{}/embed", api_base))
            .header(
                "Authorization",
                format!("Bearer {}", self.config.api_key.as_deref().unwrap_or("")),
            )
            .json(&serde_json::json!({
                "model": self.config.model_id,
                "texts": uncached_texts,
                "input_type": "search_document",
            }))
            .send()
            .await
            .map_err(|e| StreamlineError::Network(format!("Cohere request failed: {}", e)))?;

        if !response.status().is_success() {
            self.stats.errors.fetch_add(1, Ordering::Relaxed);
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(StreamlineError::Network(format!(
                "Cohere API error {}: {}",
                status, body
            )));
        }

        let data: CohereEmbeddingResponse = response
            .json()
            .await
            .map_err(|e| StreamlineError::Network(format!("Failed to parse response: {}", e)))?;

        let elapsed = start.elapsed().as_millis() as u64;
        self.stats.update_latency(elapsed);

        // Cache and fill results
        if let Some(ref cache) = self.cache {
            let mut cache_guard = cache.write().await;
            for (idx, embedding) in data.embeddings.into_iter().enumerate() {
                let original_idx = uncached_indices[idx];
                cache_guard.put(uncached_texts[idx].to_string(), embedding.clone());
                results[original_idx] = Some(embedding);
            }
        } else {
            for (idx, embedding) in data.embeddings.into_iter().enumerate() {
                let original_idx = uncached_indices[idx];
                results[original_idx] = Some(embedding);
            }
        }

        Ok(results.into_iter().flatten().collect())
    }

    fn stats(&self) -> ProviderStats {
        self.stats.snapshot()
    }
}

#[derive(Debug, Deserialize)]
struct CohereEmbeddingResponse {
    embeddings: Vec<Vec<f32>>,
}

/// Mock embedding provider for testing
pub struct MockProvider {
    config: ProviderConfig,
    dimension: usize,
    stats: ProviderStatsInternal,
}

impl MockProvider {
    /// Create a new mock provider
    pub fn new(dimension: usize) -> Self {
        Self {
            config: ProviderConfig {
                model_id: "mock-embedding".to_string(),
                ..Default::default()
            },
            dimension,
            stats: ProviderStatsInternal::default(),
        }
    }

    /// Create with custom config
    pub fn with_config(config: ProviderConfig, dimension: usize) -> Self {
        Self {
            config,
            dimension,
            stats: ProviderStatsInternal::default(),
        }
    }

    /// Generate deterministic embedding from text
    fn generate_embedding(&self, text: &str) -> Vec<f32> {
        let mut embedding = vec![0.0f32; self.dimension];
        let bytes = text.as_bytes();

        for (i, &byte) in bytes.iter().enumerate() {
            let idx = i % self.dimension;
            embedding[idx] += (byte as f32) / 255.0;
        }

        // Normalize
        let norm: f32 = embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            for x in &mut embedding {
                *x /= norm;
            }
        }

        embedding
    }
}

#[async_trait]
impl EmbeddingProvider for MockProvider {
    fn name(&self) -> &str {
        "mock"
    }

    fn model_id(&self) -> &str {
        &self.config.model_id
    }

    fn dimension(&self) -> usize {
        self.dimension
    }

    async fn embed(&self, text: &str) -> Result<Vec<f32>> {
        self.stats.total_requests.fetch_add(1, Ordering::Relaxed);
        self.stats.total_texts.fetch_add(1, Ordering::Relaxed);
        Ok(self.generate_embedding(text))
    }

    async fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>> {
        self.stats.total_requests.fetch_add(1, Ordering::Relaxed);
        self.stats
            .total_texts
            .fetch_add(texts.len() as u64, Ordering::Relaxed);
        Ok(texts.iter().map(|t| self.generate_embedding(t)).collect())
    }

    fn stats(&self) -> ProviderStats {
        self.stats.snapshot()
    }
}

/// Internal stats tracking
#[derive(Debug, Default)]
struct ProviderStatsInternal {
    total_requests: AtomicU64,
    total_texts: AtomicU64,
    total_tokens: AtomicU64,
    cache_hits: AtomicU64,
    errors: AtomicU64,
    total_latency_ms: AtomicU64,
    latency_count: AtomicU64,
}

impl ProviderStatsInternal {
    fn update_latency(&self, latency_ms: u64) {
        self.total_latency_ms
            .fetch_add(latency_ms, Ordering::Relaxed);
        self.latency_count.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> ProviderStats {
        let count = self.latency_count.load(Ordering::Relaxed);
        let avg_latency_ms = if count > 0 {
            self.total_latency_ms.load(Ordering::Relaxed) as f64 / count as f64
        } else {
            0.0
        };

        ProviderStats {
            total_requests: self.total_requests.load(Ordering::Relaxed),
            total_texts: self.total_texts.load(Ordering::Relaxed),
            total_tokens: self.total_tokens.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            avg_latency_ms,
        }
    }
}

/// Simple embedding cache
struct EmbeddingCache {
    entries: std::collections::HashMap<String, (Vec<f32>, u64)>,
    counter: u64,
    max_size: usize,
}

impl EmbeddingCache {
    fn new(max_size: usize) -> Self {
        Self {
            entries: std::collections::HashMap::new(),
            counter: 0,
            max_size,
        }
    }

    fn get(&self, key: &str) -> Option<&Vec<f32>> {
        self.entries.get(key).map(|(v, _)| v)
    }

    fn put(&mut self, key: String, value: Vec<f32>) {
        if self.max_size == 0 {
            return;
        }

        if self.entries.len() >= self.max_size {
            // Evict oldest
            if let Some(oldest_key) = self
                .entries
                .iter()
                .min_by_key(|(_, (_, count))| count)
                .map(|(k, _)| k.clone())
            {
                self.entries.remove(&oldest_key);
            }
        }

        self.counter += 1;
        self.entries.insert(key, (value, self.counter));
    }
}

/// Create an embedding provider from configuration
pub fn create_provider(config: ProviderConfig) -> Result<Arc<dyn EmbeddingProvider>> {
    match config.provider_type {
        ProviderType::OpenAI => Ok(Arc::new(OpenAIProvider::new(config)?)),
        ProviderType::Cohere => Ok(Arc::new(CohereProvider::new(config)?)),
        ProviderType::MockProvider => {
            // Infer dimension from model_id or use default
            let dimension = match config.model_id.as_str() {
                "text-embedding-3-small" => 1536,
                "text-embedding-3-large" => 3072,
                "embed-english-v3.0" => 1024,
                _ => 384,
            };
            Ok(Arc::new(MockProvider::with_config(config, dimension)))
        }
        ProviderType::Onnx => Err(StreamlineError::Config(
            "ONNX provider requires the 'onnx' feature".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mock_provider() {
        let provider = MockProvider::new(128);

        let embedding = provider.embed("hello world").await.unwrap();
        assert_eq!(embedding.len(), 128);

        // Verify normalization
        let norm: f32 = embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 0.01);
    }

    #[tokio::test]
    async fn test_mock_provider_batch() {
        let provider = MockProvider::new(64);

        let embeddings = provider
            .embed_batch(&["hello", "world", "test"])
            .await
            .unwrap();
        assert_eq!(embeddings.len(), 3);

        for emb in embeddings {
            assert_eq!(emb.len(), 64);
        }
    }

    #[tokio::test]
    async fn test_mock_provider_deterministic() {
        let provider = MockProvider::new(64);

        let emb1 = provider.embed("test").await.unwrap();
        let emb2 = provider.embed("test").await.unwrap();

        assert_eq!(emb1, emb2);
    }

    #[tokio::test]
    async fn test_provider_stats() {
        let provider = MockProvider::new(64);

        let _ = provider.embed("test1").await;
        let _ = provider.embed("test2").await;
        let _ = provider.embed_batch(&["a", "b", "c"]).await;

        let stats = provider.stats();
        assert_eq!(stats.total_requests, 3);
        assert_eq!(stats.total_texts, 5);
    }

    #[test]
    fn test_create_provider_mock() {
        let config = ProviderConfig::default();
        let provider = create_provider(config).unwrap();
        assert_eq!(provider.name(), "mock");
    }

    #[test]
    fn test_quantization_default() {
        assert_eq!(QuantizationType::default(), QuantizationType::Float32);
    }
}

// ── Local ONNX Provider ──────────────────────────────────────────────────────

/// Local embedding provider using a deterministic hash-based model.
///
/// In production, this would use an ONNX runtime (e.g., `ort` crate) to run
/// quantized models locally. For now, provides a fast, deterministic embedding
/// that's useful for testing and development.
pub struct LocalProvider {
    model_name: String,
    dimension: usize,
    stats: std::sync::Mutex<ProviderStats>,
}

impl LocalProvider {
    /// Create a new local provider.
    pub fn new(model_name: &str, dimension: usize) -> Self {
        Self {
            model_name: model_name.to_string(),
            dimension,
            stats: std::sync::Mutex::new(ProviderStats::default()),
        }
    }

    fn hash_embed(&self, text: &str) -> Vec<f32> {
        let mut vec = vec![0.0f32; self.dimension];
        let hash = text.bytes().fold(0u64, |acc, b| {
            acc.wrapping_mul(31).wrapping_add(b as u64)
        });
        for (i, v) in vec.iter_mut().enumerate() {
            let seed = hash.wrapping_add(i as u64).wrapping_mul(6364136223846793005);
            *v = ((seed % 10000) as f32 / 10000.0) * 2.0 - 1.0;
        }
        let magnitude: f32 = vec.iter().map(|v| v * v).sum::<f32>().sqrt();
        if magnitude > 0.0 {
            for v in vec.iter_mut() {
                *v /= magnitude;
            }
        }
        vec
    }
}

#[async_trait::async_trait]
impl EmbeddingProvider for LocalProvider {
    fn name(&self) -> &str {
        &self.model_name
    }

    fn model_id(&self) -> &str {
        &self.model_name
    }

    fn dimension(&self) -> usize {
        self.dimension
    }

    async fn embed(&self, text: &str) -> Result<Vec<f32>> {
        let start = std::time::Instant::now();
        let vec = self.hash_embed(text);
        if let Ok(mut stats) = self.stats.lock() {
            stats.total_requests += 1;
            stats.total_tokens += text.split_whitespace().count() as u64;
            stats.avg_latency_ms = start.elapsed().as_secs_f64() * 1000.0;
        }
        Ok(vec)
    }

    async fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>> {
        let mut results = Vec::with_capacity(texts.len());
        for text in texts {
            results.push(self.hash_embed(text));
        }
        if let Ok(mut stats) = self.stats.lock() {
            stats.total_requests += texts.len() as u64;
        }
        Ok(results)
    }

    fn stats(&self) -> ProviderStats {
        self.stats.lock().map(|s| s.clone()).unwrap_or_default()
    }
}

// ── Semantic Search CLI Helper ───────────────────────────────────────────────

/// Helper for CLI-based semantic search across topics.
///
/// Usage: `streamline-cli search "payment failures" --topics events,logs --limit 10`
pub struct SemanticSearchCli {
    provider: Box<dyn EmbeddingProvider>,
}

impl SemanticSearchCli {
    /// Create with a specific provider.
    pub fn new(provider: Box<dyn EmbeddingProvider>) -> Self {
        Self { provider }
    }

    /// Create with the default local provider.
    pub fn with_local() -> Self {
        Self {
            provider: Box::new(LocalProvider::new("local-hash-v1", 384)),
        }
    }

    /// Search for messages semantically similar to the query.
    pub async fn search(
        &self,
        query: &str,
        candidates: &[(String, String)], // (message_id, text)
        limit: usize,
    ) -> Result<Vec<SearchHit>> {
        let query_vec = self.provider.embed(query).await?;

        let mut scored: Vec<SearchHit> = Vec::new();
        for (id, text) in candidates {
            let candidate_vec = self.provider.embed(text).await?;
            let similarity = cosine_similarity(&query_vec, &candidate_vec);
            scored.push(SearchHit {
                id: id.clone(),
                text_preview: if text.len() > 200 {
                    format!("{}...", &text[..200])
                } else {
                    text.clone()
                },
                similarity,
            });
        }

        scored.sort_by(|a, b| b.similarity.partial_cmp(&a.similarity).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(limit);

        Ok(scored)
    }
}

/// A search result with similarity score.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SearchHit {
    pub id: String,
    pub text_preview: String,
    pub similarity: f32,
}

/// Cosine similarity between two vectors.
fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let mag_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let mag_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    if mag_a > 0.0 && mag_b > 0.0 {
        dot / (mag_a * mag_b)
    } else {
        0.0
    }
}

#[cfg(test)]
mod provider_tests {
    use super::*;

    #[tokio::test]
    async fn test_local_provider_embed() {
        let provider = LocalProvider::new("test", 128);
        let vec = provider.embed("hello world").await.unwrap();
        assert_eq!(vec.len(), 128);
        let magnitude: f32 = vec.iter().map(|v| v * v).sum::<f32>().sqrt();
        assert!((magnitude - 1.0).abs() < 0.01, "Should be unit-normalized");
    }

    #[tokio::test]
    async fn test_local_provider_deterministic() {
        let provider = LocalProvider::new("test", 64);
        let v1 = provider.embed("same text").await.unwrap();
        let v2 = provider.embed("same text").await.unwrap();
        assert_eq!(v1, v2, "Same text should produce same embedding");
    }

    #[tokio::test]
    async fn test_local_provider_batch() {
        let provider = LocalProvider::new("test", 32);
        let vecs = provider.embed_batch(&["a", "b", "c"]).await.unwrap();
        assert_eq!(vecs.len(), 3);
        assert_ne!(vecs[0], vecs[1]);
    }

    #[tokio::test]
    async fn test_semantic_search_cli() {
        let cli = SemanticSearchCli::with_local();
        let candidates = vec![
            ("msg-1".to_string(), "payment processing failed".to_string()),
            ("msg-2".to_string(), "user logged in successfully".to_string()),
            ("msg-3".to_string(), "payment gateway timeout error".to_string()),
        ];

        let results = cli.search("payment failures", &candidates, 2).await.unwrap();
        assert_eq!(results.len(), 2);
        // Both payment-related messages should rank higher
        assert!(results[0].similarity > 0.0);
    }

    #[test]
    fn test_cosine_similarity_identical() {
        let a = vec![1.0, 0.0, 0.0];
        let sim = cosine_similarity(&a, &a);
        assert!((sim - 1.0).abs() < 0.001);
    }

    #[test]
    fn test_cosine_similarity_orthogonal() {
        let a = vec![1.0, 0.0];
        let b = vec![0.0, 1.0];
        let sim = cosine_similarity(&a, &b);
        assert!(sim.abs() < 0.001);
    }
}
