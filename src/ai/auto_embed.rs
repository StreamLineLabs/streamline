//! Auto-indexing interceptor for the produce path.
//!
//! Automatically generates vector embeddings for messages as they're produced
//! to configured topics. Embeddings are stored alongside the original messages
//! in a shadow topic (`{topic}.__embeddings`) for semantic search.
//!
//! This is the "AI-native" integration point — streaming data is automatically
//! searchable by meaning, not just by offset or key.

use crate::error::{Result, StreamlineError};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Configuration for auto-embedding on produce.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoEmbedConfig {
    /// Topics to auto-embed (empty = none; "*" = all).
    pub topics: Vec<String>,
    /// Embedding model to use.
    pub model: String,
    /// Maximum text length to embed (truncates longer messages).
    pub max_text_length: usize,
    /// Batch size for embedding requests.
    pub batch_size: usize,
    /// Whether to embed keys as well as values.
    pub embed_keys: bool,
    /// JSON field to extract text from (None = embed entire value).
    pub text_field: Option<String>,
    /// Embedding dimension (depends on model).
    pub dimension: usize,
}

impl Default for AutoEmbedConfig {
    fn default() -> Self {
        Self {
            topics: Vec::new(),
            model: "text-embedding-3-small".to_string(),
            max_text_length: 8192,
            batch_size: 32,
            embed_keys: false,
            text_field: None,
            dimension: 1536,
        }
    }
}

/// A generated embedding for a message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageEmbedding {
    /// Source topic.
    pub topic: String,
    /// Source partition.
    pub partition: i32,
    /// Source offset.
    pub offset: i64,
    /// Embedding vector.
    pub vector: Vec<f32>,
    /// Model used to generate the embedding.
    pub model: String,
    /// Timestamp of the source message.
    pub timestamp: i64,
    /// Text that was embedded (truncated).
    pub text_preview: String,
}

/// Auto-embedding interceptor for the produce path.
pub struct AutoEmbedInterceptor {
    config: AutoEmbedConfig,
    enabled_topics: Arc<RwLock<HashSet<String>>>,
    /// Pending messages waiting for batch embedding.
    pending: Arc<RwLock<Vec<PendingEmbed>>>,
    /// Generated embeddings waiting to be written to shadow topics.
    ready: Arc<RwLock<Vec<MessageEmbedding>>>,
    /// Metrics
    total_embedded: AtomicU64,
    total_skipped: AtomicU64,
    total_errors: AtomicU64,
}

/// A message pending embedding.
struct PendingEmbed {
    topic: String,
    partition: i32,
    offset: i64,
    text: String,
    timestamp: i64,
}

impl AutoEmbedInterceptor {
    /// Create a new auto-embed interceptor.
    pub fn new(config: AutoEmbedConfig) -> Self {
        let topics: HashSet<String> = config.topics.iter().cloned().collect();
        Self {
            config,
            enabled_topics: Arc::new(RwLock::new(topics)),
            pending: Arc::new(RwLock::new(Vec::new())),
            ready: Arc::new(RwLock::new(Vec::new())),
            total_embedded: AtomicU64::new(0),
            total_skipped: AtomicU64::new(0),
            total_errors: AtomicU64::new(0),
        }
    }

    /// Check if a topic should be auto-embedded.
    pub async fn should_embed(&self, topic: &str) -> bool {
        let topics = self.enabled_topics.read().await;
        topics.contains("*") || topics.contains(topic)
    }

    /// Queue a message for embedding (called on the produce path).
    ///
    /// This is non-blocking — the actual embedding happens asynchronously.
    pub async fn on_produce(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        value: &[u8],
        timestamp: i64,
    ) -> Result<()> {
        if !self.should_embed(topic).await {
            self.total_skipped.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }

        // Extract text from the message
        let text = self.extract_text(value)?;

        if text.is_empty() {
            self.total_skipped.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }

        let truncated = if text.len() > self.config.max_text_length {
            text[..self.config.max_text_length].to_string()
        } else {
            text
        };

        self.pending.write().await.push(PendingEmbed {
            topic: topic.to_string(),
            partition,
            offset,
            text: truncated,
            timestamp,
        });

        // Auto-flush if batch is full
        if self.pending.read().await.len() >= self.config.batch_size {
            self.flush_batch().await?;
        }

        Ok(())
    }

    /// Extract text from a message payload.
    fn extract_text(&self, value: &[u8]) -> Result<String> {
        if let Some(ref field) = self.config.text_field {
            // Extract specific JSON field
            let json: serde_json::Value = serde_json::from_slice(value).map_err(|e| {
                StreamlineError::InvalidData(format!("Cannot parse JSON for embedding: {}", e))
            })?;
            Ok(json
                .get(field)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string())
        } else {
            // Embed entire value as text
            String::from_utf8(value.to_vec()).map_err(|e| {
                StreamlineError::InvalidData(format!("Cannot convert to UTF-8 for embedding: {}", e))
            })
        }
    }

    /// Flush pending messages by generating embeddings.
    ///
    /// In production, this would call an embedding API (OpenAI, local ONNX model, etc.).
    /// For now, generates deterministic placeholder vectors for testing.
    pub async fn flush_batch(&self) -> Result<()> {
        let mut pending = self.pending.write().await;
        if pending.is_empty() {
            return Ok(());
        }

        let batch: Vec<PendingEmbed> = pending.drain(..).collect();
        drop(pending);

        let mut embeddings = Vec::with_capacity(batch.len());
        for item in &batch {
            // Generate a deterministic embedding from the text hash
            // (In production, call embedding model API here)
            let vector = self.generate_embedding(&item.text);

            embeddings.push(MessageEmbedding {
                topic: item.topic.clone(),
                partition: item.partition,
                offset: item.offset,
                vector,
                model: self.config.model.clone(),
                timestamp: item.timestamp,
                text_preview: if item.text.len() > 100 {
                    format!("{}...", &item.text[..100])
                } else {
                    item.text.clone()
                },
            });
        }

        let count = embeddings.len();
        self.ready.write().await.extend(embeddings);
        self.total_embedded.fetch_add(count as u64, Ordering::Relaxed);

        debug!(count, "Generated embeddings for batch");
        Ok(())
    }

    /// Generate an embedding vector from text.
    ///
    /// Placeholder: produces a deterministic vector from text hash.
    /// Replace with actual model inference in production.
    fn generate_embedding(&self, text: &str) -> Vec<f32> {
        let dim = self.config.dimension;
        let mut vector = vec![0.0f32; dim];

        // Deterministic hash-based embedding for testing
        let hash = text.bytes().fold(0u64, |acc, b| {
            acc.wrapping_mul(31).wrapping_add(b as u64)
        });

        for (i, v) in vector.iter_mut().enumerate() {
            let seed = hash.wrapping_add(i as u64);
            *v = ((seed % 10000) as f32 / 10000.0) * 2.0 - 1.0;
        }

        // Normalize to unit vector
        let magnitude: f32 = vector.iter().map(|v| v * v).sum::<f32>().sqrt();
        if magnitude > 0.0 {
            for v in vector.iter_mut() {
                *v /= magnitude;
            }
        }

        vector
    }

    /// Drain ready embeddings (to be written to shadow topics).
    pub async fn drain_ready(&self) -> Vec<MessageEmbedding> {
        let mut ready = self.ready.write().await;
        ready.drain(..).collect()
    }

    /// Add a topic to the auto-embed list.
    pub async fn enable_topic(&self, topic: &str) {
        self.enabled_topics.write().await.insert(topic.to_string());
    }

    /// Remove a topic from the auto-embed list.
    pub async fn disable_topic(&self, topic: &str) {
        self.enabled_topics.write().await.remove(topic);
    }

    /// Get interceptor metrics.
    pub fn metrics(&self) -> AutoEmbedMetrics {
        AutoEmbedMetrics {
            total_embedded: self.total_embedded.load(Ordering::Relaxed),
            total_skipped: self.total_skipped.load(Ordering::Relaxed),
            total_errors: self.total_errors.load(Ordering::Relaxed),
            model: self.config.model.clone(),
            dimension: self.config.dimension,
        }
    }
}

/// Auto-embedding metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoEmbedMetrics {
    pub total_embedded: u64,
    pub total_skipped: u64,
    pub total_errors: u64,
    pub model: String,
    pub dimension: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_auto_embed_enabled_topic() {
        let config = AutoEmbedConfig {
            topics: vec!["events".to_string()],
            dimension: 8,
            batch_size: 2,
            ..Default::default()
        };
        let interceptor = AutoEmbedInterceptor::new(config);

        assert!(interceptor.should_embed("events").await);
        assert!(!interceptor.should_embed("other").await);
    }

    #[tokio::test]
    async fn test_auto_embed_wildcard() {
        let config = AutoEmbedConfig {
            topics: vec!["*".to_string()],
            dimension: 8,
            ..Default::default()
        };
        let interceptor = AutoEmbedInterceptor::new(config);

        assert!(interceptor.should_embed("any-topic").await);
    }

    #[tokio::test]
    async fn test_on_produce_queues_embedding() {
        let config = AutoEmbedConfig {
            topics: vec!["events".to_string()],
            dimension: 4,
            batch_size: 100,
            ..Default::default()
        };
        let interceptor = AutoEmbedInterceptor::new(config);

        interceptor
            .on_produce("events", 0, 0, b"Hello world", 1000)
            .await
            .unwrap();

        // Flush manually
        interceptor.flush_batch().await.unwrap();

        let ready = interceptor.drain_ready().await;
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].topic, "events");
        assert_eq!(ready[0].vector.len(), 4);
    }

    #[tokio::test]
    async fn test_auto_flush_on_batch_full() {
        let config = AutoEmbedConfig {
            topics: vec!["t".to_string()],
            dimension: 4,
            batch_size: 2,
            ..Default::default()
        };
        let interceptor = AutoEmbedInterceptor::new(config);

        interceptor
            .on_produce("t", 0, 0, b"msg1", 1000)
            .await
            .unwrap();
        interceptor
            .on_produce("t", 0, 1, b"msg2", 1001)
            .await
            .unwrap();

        let ready = interceptor.drain_ready().await;
        assert_eq!(ready.len(), 2);
    }

    #[tokio::test]
    async fn test_skip_non_enabled_topic() {
        let config = AutoEmbedConfig {
            topics: vec!["enabled".to_string()],
            dimension: 4,
            ..Default::default()
        };
        let interceptor = AutoEmbedInterceptor::new(config);

        interceptor
            .on_produce("disabled", 0, 0, b"data", 1000)
            .await
            .unwrap();

        interceptor.flush_batch().await.unwrap();
        let ready = interceptor.drain_ready().await;
        assert!(ready.is_empty());

        let metrics = interceptor.metrics();
        assert_eq!(metrics.total_skipped, 1);
    }

    #[tokio::test]
    async fn test_json_field_extraction() {
        let config = AutoEmbedConfig {
            topics: vec!["t".to_string()],
            dimension: 4,
            text_field: Some("content".to_string()),
            batch_size: 100,
            ..Default::default()
        };
        let interceptor = AutoEmbedInterceptor::new(config);

        let json_msg = br#"{"id": 1, "content": "Hello from JSON field"}"#;
        interceptor
            .on_produce("t", 0, 0, json_msg, 1000)
            .await
            .unwrap();

        interceptor.flush_batch().await.unwrap();
        let ready = interceptor.drain_ready().await;
        assert_eq!(ready.len(), 1);
        assert!(ready[0].text_preview.contains("Hello from JSON field"));
    }

    #[test]
    fn test_embedding_is_normalized() {
        let config = AutoEmbedConfig {
            dimension: 128,
            ..Default::default()
        };
        let interceptor = AutoEmbedInterceptor::new(config);
        let vec = interceptor.generate_embedding("test text");

        let magnitude: f32 = vec.iter().map(|v| v * v).sum::<f32>().sqrt();
        assert!((magnitude - 1.0).abs() < 0.01, "Vector should be unit-normalized");
    }

    #[tokio::test]
    async fn test_enable_disable_topic() {
        let config = AutoEmbedConfig::default();
        let interceptor = AutoEmbedInterceptor::new(config);

        assert!(!interceptor.should_embed("new-topic").await);
        interceptor.enable_topic("new-topic").await;
        assert!(interceptor.should_embed("new-topic").await);
        interceptor.disable_topic("new-topic").await;
        assert!(!interceptor.should_embed("new-topic").await);
    }
}
