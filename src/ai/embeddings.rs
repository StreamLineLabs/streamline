//! Enhanced Embedding Provider Integration
//!
//! Provides additional embedding providers including a local TF-IDF/bag-of-words
//! provider for offline use and a caching wrapper for any provider.

use crate::error::{Result, StreamlineError};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

use super::providers::{EmbeddingProvider, ProviderStats};

/// Configuration for embedding provider selection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingProviderConfig {
    /// Provider type
    pub provider: EmbeddingProviderSelection,
    /// Model name (provider-specific)
    pub model_name: String,
    /// Embedding dimensions
    pub dimensions: usize,
    /// API key reference (env var name or secret name)
    pub api_key_ref: Option<String>,
    /// Enable caching wrapper
    pub cache_enabled: bool,
    /// Maximum cache entries
    pub cache_max_entries: usize,
    /// Cache TTL in seconds (0 = no expiration)
    pub cache_ttl_secs: u64,
}

impl Default for EmbeddingProviderConfig {
    fn default() -> Self {
        Self {
            provider: EmbeddingProviderSelection::Local,
            model_name: "bag-of-words".to_string(),
            dimensions: 384,
            api_key_ref: None,
            cache_enabled: true,
            cache_max_entries: 10_000,
            cache_ttl_secs: 3600, // 1 hour default TTL
        }
    }
}

/// Provider selection
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EmbeddingProviderSelection {
    /// OpenAI embeddings API
    OpenAI,
    /// Local TF-IDF / bag-of-words (no external API)
    Local,
    /// Mock provider for testing
    Mock,
}

/// Local embedding provider using TF-IDF / bag-of-words.
///
/// Generates embeddings without external API calls, suitable for offline use,
/// testing, and low-latency scenarios where semantic precision is less critical.
pub struct LocalProvider {
    dimension: usize,
    model_name: String,
    stats: LocalProviderStats,
}

impl LocalProvider {
    /// Create a new local provider with the given dimension.
    pub fn new(dimension: usize) -> Self {
        Self {
            dimension,
            model_name: "local-bow".to_string(),
            stats: LocalProviderStats::default(),
        }
    }

    /// Create with a custom model name.
    pub fn with_model_name(dimension: usize, model_name: impl Into<String>) -> Self {
        Self {
            dimension,
            model_name: model_name.into(),
            stats: LocalProviderStats::default(),
        }
    }

    /// Bag-of-words embedding: hash tokens into a fixed-size vector, apply
    /// TF-like weighting, then L2-normalize.
    fn bow_embed(&self, text: &str) -> Vec<f32> {
        let mut embedding = vec![0.0f32; self.dimension];
        let lower = text.to_lowercase();
        let tokens: Vec<&str> = lower
            .split(|c: char| !c.is_alphanumeric())
            .filter(|s| !s.is_empty())
            .collect();

        let doc_len = tokens.len().max(1) as f32;

        // Term-frequency accumulation via hashing
        let mut tf: HashMap<&str, f32> = HashMap::new();
        for token in &tokens {
            *tf.entry(token).or_insert(0.0) += 1.0;
        }

        for (token, freq) in &tf {
            let normalized_freq = freq / doc_len;
            let hash = Self::djb2_hash(token);
            let pos = (hash as usize) % self.dimension;
            embedding[pos] += normalized_freq;

            // Character n-gram features for sub-word information
            let bytes = token.as_bytes();
            if bytes.len() >= 3 {
                for i in 0..bytes.len() - 2 {
                    let ngram_hash = Self::djb2_hash_bytes(&bytes[i..i + 3]);
                    let ngram_pos = (ngram_hash as usize) % self.dimension;
                    embedding[ngram_pos] += normalized_freq * 0.3;
                }
            }
        }

        // L2 normalize
        let norm: f32 = embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            for x in &mut embedding {
                *x /= norm;
            }
        }

        embedding
    }

    fn djb2_hash(s: &str) -> u32 {
        let mut hash: u32 = 5381;
        for c in s.bytes() {
            hash = hash.wrapping_mul(33).wrapping_add(c as u32);
        }
        hash
    }

    fn djb2_hash_bytes(bytes: &[u8]) -> u32 {
        let mut hash: u32 = 5381;
        for &c in bytes {
            hash = hash.wrapping_mul(33).wrapping_add(c as u32);
        }
        hash
    }
}

#[async_trait]
impl EmbeddingProvider for LocalProvider {
    fn name(&self) -> &str {
        "local"
    }

    fn model_id(&self) -> &str {
        &self.model_name
    }

    fn dimension(&self) -> usize {
        self.dimension
    }

    async fn embed(&self, text: &str) -> Result<Vec<f32>> {
        self.stats.requests.fetch_add(1, Ordering::Relaxed);
        self.stats.texts.fetch_add(1, Ordering::Relaxed);
        Ok(self.bow_embed(text))
    }

    async fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>> {
        self.stats.requests.fetch_add(1, Ordering::Relaxed);
        self.stats
            .texts
            .fetch_add(texts.len() as u64, Ordering::Relaxed);
        Ok(texts.iter().map(|t| self.bow_embed(t)).collect())
    }

    fn stats(&self) -> ProviderStats {
        ProviderStats {
            total_requests: self.stats.requests.load(Ordering::Relaxed),
            total_texts: self.stats.texts.load(Ordering::Relaxed),
            total_tokens: 0,
            cache_hits: 0,
            errors: 0,
            avg_latency_ms: 0.0,
        }
    }
}

#[derive(Debug, Default)]
struct LocalProviderStats {
    requests: AtomicU64,
    texts: AtomicU64,
}

/// Caching wrapper around any `EmbeddingProvider`.
///
/// Avoids repeated API calls for identical texts by maintaining an LRU cache
/// with configurable TTL expiration.
pub struct CachedProvider {
    inner: Arc<dyn EmbeddingProvider>,
    cache: Arc<RwLock<LruEmbeddingCache>>,
    stats: CachedProviderStats,
}

impl CachedProvider {
    /// Wrap an existing provider with a cache of the given maximum size.
    pub fn new(inner: Arc<dyn EmbeddingProvider>, max_entries: usize) -> Self {
        Self {
            inner,
            cache: Arc::new(RwLock::new(LruEmbeddingCache::new(max_entries, Duration::ZERO))),
            stats: CachedProviderStats::default(),
        }
    }

    /// Wrap an existing provider with a cache that has a TTL.
    pub fn with_ttl(inner: Arc<dyn EmbeddingProvider>, max_entries: usize, ttl: Duration) -> Self {
        Self {
            inner,
            cache: Arc::new(RwLock::new(LruEmbeddingCache::new(max_entries, ttl))),
            stats: CachedProviderStats::default(),
        }
    }
}

#[async_trait]
impl EmbeddingProvider for CachedProvider {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn model_id(&self) -> &str {
        self.inner.model_id()
    }

    fn dimension(&self) -> usize {
        self.inner.dimension()
    }

    async fn embed(&self, text: &str) -> Result<Vec<f32>> {
        // Check cache
        {
            let cache = self.cache.read().await;
            if let Some(embedding) = cache.get(text) {
                self.stats.cache_hits.fetch_add(1, Ordering::Relaxed);
                return Ok(embedding.clone());
            }
        }

        // Cache miss — delegate to inner provider
        self.stats.cache_misses.fetch_add(1, Ordering::Relaxed);
        let embedding = self.inner.embed(text).await?;

        // Store in cache
        {
            let mut cache = self.cache.write().await;
            cache.put(text.to_string(), embedding.clone());
        }

        Ok(embedding)
    }

    async fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>> {
        let mut results = vec![None; texts.len()];
        let mut uncached_indices = Vec::new();
        let mut uncached_texts = Vec::new();

        // Check cache for each text
        {
            let cache = self.cache.read().await;
            for (i, text) in texts.iter().enumerate() {
                if let Some(embedding) = cache.get(text) {
                    results[i] = Some(embedding.clone());
                    self.stats.cache_hits.fetch_add(1, Ordering::Relaxed);
                } else {
                    uncached_indices.push(i);
                    uncached_texts.push(*text);
                    self.stats.cache_misses.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        if !uncached_texts.is_empty() {
            let embeddings = self.inner.embed_batch(&uncached_texts).await?;

            let mut cache = self.cache.write().await;
            for (idx, embedding) in embeddings.into_iter().enumerate() {
                let original_idx = uncached_indices[idx];
                cache.put(uncached_texts[idx].to_string(), embedding.clone());
                results[original_idx] = Some(embedding);
            }
        }

        Ok(results.into_iter().map(|r| r.unwrap_or_default()).collect())
    }

    fn stats(&self) -> ProviderStats {
        let mut inner_stats = self.inner.stats();
        inner_stats.cache_hits = self.stats.cache_hits.load(Ordering::Relaxed);
        inner_stats
    }
}

#[derive(Debug, Default)]
struct CachedProviderStats {
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
}

/// Simple LRU cache for embedding vectors with optional TTL.
struct LruEmbeddingCache {
    entries: HashMap<String, (Vec<f32>, u64, Instant)>,
    counter: u64,
    max_size: usize,
    ttl: Duration,
}

impl LruEmbeddingCache {
    fn new(max_size: usize, ttl: Duration) -> Self {
        Self {
            entries: HashMap::new(),
            counter: 0,
            max_size,
            ttl,
        }
    }

    fn get(&self, key: &str) -> Option<&Vec<f32>> {
        self.entries.get(key).and_then(|(v, _, created_at)| {
            if !self.ttl.is_zero() && created_at.elapsed() > self.ttl {
                None
            } else {
                Some(v)
            }
        })
    }

    fn put(&mut self, key: String, value: Vec<f32>) {
        if self.max_size == 0 {
            return;
        }
        // Evict expired entries first
        if !self.ttl.is_zero() {
            let ttl = self.ttl;
            self.entries.retain(|_, (_, _, created_at)| created_at.elapsed() <= ttl);
        }
        if self.entries.len() >= self.max_size && !self.entries.contains_key(&key) {
            if let Some(oldest_key) = self
                .entries
                .iter()
                .min_by_key(|(_, (_, ts, _))| ts)
                .map(|(k, _)| k.clone())
            {
                self.entries.remove(&oldest_key);
            }
        }
        self.counter += 1;
        self.entries.insert(key, (value, self.counter, Instant::now()));
    }
}

/// Create an embedding provider from configuration.
pub fn create_embedding_provider(
    config: &EmbeddingProviderConfig,
) -> Result<Arc<dyn EmbeddingProvider>> {
    let provider: Arc<dyn EmbeddingProvider> = match config.provider {
        EmbeddingProviderSelection::OpenAI => {
            let api_key = config
                .api_key_ref
                .as_ref()
                .and_then(|key_ref| std::env::var(key_ref).ok())
                .ok_or_else(|| {
                    StreamlineError::Config(
                        "OpenAI API key not found. Set the env var referenced by api_key_ref"
                            .into(),
                    )
                })?;

            let provider_config = super::providers::ProviderConfig {
                provider_type: super::providers::ProviderType::OpenAI,
                api_key: Some(api_key),
                api_base: None,
                model_id: config.model_name.clone(),
                max_batch_size: 100,
                timeout_ms: 30_000,
                cache_enabled: false, // caching handled by CachedProvider wrapper
                cache_size: 0,
                quantization: super::providers::QuantizationType::Float32,
            };
            Arc::new(super::providers::OpenAIProvider::new(provider_config)?)
        }
        EmbeddingProviderSelection::Local => {
            Arc::new(LocalProvider::with_model_name(
                config.dimensions,
                &config.model_name,
            ))
        }
        EmbeddingProviderSelection::Mock => {
            Arc::new(super::providers::MockProvider::new(config.dimensions))
        }
    };

    if config.cache_enabled {
        let ttl = if config.cache_ttl_secs > 0 {
            Duration::from_secs(config.cache_ttl_secs)
        } else {
            Duration::ZERO
        };
        Ok(Arc::new(CachedProvider::with_ttl(provider, config.cache_max_entries, ttl)))
    } else {
        Ok(provider)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_local_provider_embed() {
        let provider = LocalProvider::new(128);
        let embedding = provider.embed("hello world").await.unwrap();
        assert_eq!(embedding.len(), 128);

        // Should be normalized
        let norm: f32 = embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 0.01);
    }

    #[tokio::test]
    async fn test_local_provider_deterministic() {
        let provider = LocalProvider::new(64);
        let e1 = provider.embed("test input").await.unwrap();
        let e2 = provider.embed("test input").await.unwrap();
        assert_eq!(e1, e2);
    }

    #[tokio::test]
    async fn test_local_provider_batch() {
        let provider = LocalProvider::new(64);
        let embeddings = provider
            .embed_batch(&["hello", "world", "test"])
            .await
            .unwrap();
        assert_eq!(embeddings.len(), 3);
        for emb in &embeddings {
            assert_eq!(emb.len(), 64);
        }
    }

    #[tokio::test]
    async fn test_cached_provider() {
        let inner = Arc::new(LocalProvider::new(64));
        let cached = CachedProvider::new(inner, 100);

        let e1 = cached.embed("hello").await.unwrap();
        let e2 = cached.embed("hello").await.unwrap();
        assert_eq!(e1, e2);

        let stats = cached.stats();
        assert_eq!(stats.cache_hits, 1);
    }

    #[tokio::test]
    async fn test_cached_provider_batch() {
        let inner = Arc::new(LocalProvider::new(64));
        let cached = CachedProvider::new(inner, 100);

        // First call — all misses
        let _ = cached.embed_batch(&["a", "b"]).await.unwrap();
        // Second call — all hits
        let results = cached.embed_batch(&["a", "b"]).await.unwrap();
        assert_eq!(results.len(), 2);

        let stats = cached.stats();
        assert_eq!(stats.cache_hits, 2);
    }

    #[test]
    fn test_embedding_provider_config_default() {
        let config = EmbeddingProviderConfig::default();
        assert_eq!(config.dimensions, 384);
        assert!(config.cache_enabled);
    }

    #[tokio::test]
    async fn test_create_local_provider() {
        let config = EmbeddingProviderConfig::default();
        let provider = create_embedding_provider(&config).unwrap();
        let embedding = provider.embed("test").await.unwrap();
        assert_eq!(embedding.len(), 384);
    }
}
