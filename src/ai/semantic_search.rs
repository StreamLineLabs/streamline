//! Semantic Search over Stream History
//!
//! Provides a high-level semantic search interface for indexing and querying
//! messages across topic history using vector embeddings.

use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

use super::embedding::EmbeddingEngine;
use super::hnsw::{HnswConfig, HnswIndex};
use super::providers::EmbeddingProvider;

/// Semantic index for searching across topic messages using vector similarity.
pub struct SemanticMessageIndex {
    /// Embedding provider
    provider: Arc<dyn EmbeddingProvider>,
    /// Per-topic HNSW indexes
    indexes: Arc<RwLock<HashMap<String, Arc<HnswIndex>>>>,
    /// Message metadata store (offset -> metadata)
    messages: Arc<RwLock<HashMap<i64, MessageMeta>>>,
    /// Auto-increment ID counter
    next_id: AtomicU64,
    /// Statistics
    stats: IndexStatsInternal,
}

/// Stored metadata for an indexed message.
#[derive(Debug, Clone)]
struct MessageMeta {
    content: String,
    topic: String,
    offset: i64,
    partition: i32,
    timestamp: i64,
}

/// A search result with scored message content.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticSearchResult {
    /// Similarity score (0.0–1.0, higher is more similar)
    pub similarity: f32,
    /// Original message content
    pub content: String,
    /// Source topic
    pub topic: String,
    /// Message offset within the topic-partition
    pub offset: i64,
    /// Partition
    pub partition: i32,
    /// Message timestamp
    pub timestamp: i64,
}

/// Query for semantic search with optional filters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticSearchQuery {
    /// Text query (will be embedded)
    pub text: Option<String>,
    /// Direct vector query
    pub vector: Option<Vec<f32>>,
    /// Filter by topic names
    #[serde(default)]
    pub topics: Vec<String>,
    /// Minimum similarity score
    #[serde(default = "default_min_score")]
    pub min_score: f32,
    /// Maximum number of results
    #[serde(default = "default_max_results")]
    pub max_results: usize,
    /// Filter: only messages after this timestamp
    pub time_start: Option<i64>,
    /// Filter: only messages before this timestamp
    pub time_end: Option<i64>,
}

fn default_min_score() -> f32 {
    0.5
}
fn default_max_results() -> usize {
    20
}

impl SemanticSearchQuery {
    /// Create a text-based search query.
    pub fn text(query: &str) -> Self {
        Self {
            text: Some(query.to_string()),
            vector: None,
            topics: Vec::new(),
            min_score: default_min_score(),
            max_results: default_max_results(),
            time_start: None,
            time_end: None,
        }
    }

    /// Create a vector-based search query.
    pub fn vector(vec: Vec<f32>) -> Self {
        Self {
            text: None,
            vector: Some(vec),
            topics: Vec::new(),
            min_score: default_min_score(),
            max_results: default_max_results(),
            time_start: None,
            time_end: None,
        }
    }

    /// Restrict results to specific topics.
    pub fn with_topics(mut self, topics: Vec<String>) -> Self {
        self.topics = topics;
        self
    }

    /// Set minimum similarity score.
    pub fn with_min_score(mut self, score: f32) -> Self {
        self.min_score = score;
        self
    }

    /// Set maximum results.
    pub fn with_max_results(mut self, max: usize) -> Self {
        self.max_results = max;
        self
    }

    /// Set time range filter.
    pub fn with_time_range(mut self, start: i64, end: i64) -> Self {
        self.time_start = Some(start);
        self.time_end = Some(end);
        self
    }
}

/// Statistics for the semantic index.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SemanticIndexStats {
    /// Total messages indexed
    pub total_indexed: u64,
    /// Total searches performed
    pub total_searches: u64,
    /// Number of topics with indexes
    pub topic_count: usize,
    /// Messages per topic
    pub messages_per_topic: HashMap<String, usize>,
}

impl SemanticMessageIndex {
    /// Create a new semantic message index.
    pub fn new(provider: Arc<dyn EmbeddingProvider>) -> Self {
        Self {
            provider,
            indexes: Arc::new(RwLock::new(HashMap::new())),
            messages: Arc::new(RwLock::new(HashMap::new())),
            next_id: AtomicU64::new(1),
            stats: IndexStatsInternal::default(),
        }
    }

    /// Index a single message from a topic.
    pub async fn index_message(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        timestamp: i64,
        content: &str,
    ) -> Result<()> {
        // Generate embedding
        let embedding = self
            .provider
            .embed(content)
            .await
            .map_err(|e| StreamlineError::AI(format!("Embedding failed: {}", e)))?;

        let id = self.next_id.fetch_add(1, Ordering::Relaxed) as i64;

        // Store metadata
        {
            let mut msgs = self.messages.write().await;
            msgs.insert(
                id,
                MessageMeta {
                    content: content.to_string(),
                    topic: topic.to_string(),
                    offset,
                    partition,
                    timestamp,
                },
            );
        }

        // Insert into HNSW index
        {
            let mut indexes = self.indexes.write().await;
            let index = indexes.entry(topic.to_string()).or_insert_with(|| {
                let config = HnswConfig::balanced(self.provider.dimension());
                Arc::new(HnswIndex::new(config))
            });
            index.insert(id, embedding).await?;
        }

        self.stats.indexed.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Search across all indexed messages.
    pub async fn search(
        &self,
        query: &str,
        max_results: usize,
    ) -> Result<Vec<SemanticSearchResult>> {
        let q = SemanticSearchQuery::text(query).with_max_results(max_results);
        self.search_with_filters(q).await
    }

    /// Search with full filter support.
    pub async fn search_with_filters(
        &self,
        query: SemanticSearchQuery,
    ) -> Result<Vec<SemanticSearchResult>> {
        let query_vec = match (&query.text, &query.vector) {
            (Some(text), _) => self
                .provider
                .embed(text)
                .await
                .map_err(|e| StreamlineError::AI(format!("Query embedding failed: {}", e)))?,
            (_, Some(vec)) => vec.clone(),
            _ => {
                return Err(StreamlineError::InvalidData(
                    "Query must have either text or vector".into(),
                ));
            }
        };

        self.stats.searches.fetch_add(1, Ordering::Relaxed);

        let indexes = self.indexes.read().await;
        let messages = self.messages.read().await;

        let mut all_results = Vec::new();

        let topics_to_search: Vec<&String> = if query.topics.is_empty() {
            indexes.keys().collect()
        } else {
            query.topics.iter().filter(|t| indexes.contains_key(*t)).collect()
        };

        for topic in topics_to_search {
            if let Some(index) = indexes.get(topic) {
                if let Ok(hits) = index.search(&query_vec, query.max_results).await {
                    for hit in hits {
                        let score = 1.0 - hit.distance;
                        if score < query.min_score {
                            continue;
                        }
                        if let Some(meta) = messages.get(&hit.id) {
                            // Apply time range filter
                            if let Some(start) = query.time_start {
                                if meta.timestamp < start {
                                    continue;
                                }
                            }
                            if let Some(end) = query.time_end {
                                if meta.timestamp > end {
                                    continue;
                                }
                            }

                            all_results.push(SemanticSearchResult {
                                similarity: score,
                                content: meta.content.clone(),
                                topic: meta.topic.clone(),
                                offset: meta.offset,
                                partition: meta.partition,
                                timestamp: meta.timestamp,
                            });
                        }
                    }
                }
            }
        }

        // Sort by similarity descending and truncate
        all_results.sort_by(|a, b| {
            b.similarity
                .partial_cmp(&a.similarity)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        all_results.truncate(query.max_results);

        Ok(all_results)
    }

    /// Get index statistics.
    pub async fn get_stats(&self) -> SemanticIndexStats {
        let indexes = self.indexes.read().await;
        let mut messages_per_topic = HashMap::new();
        for (topic, index) in indexes.iter() {
            messages_per_topic.insert(topic.clone(), index.len().await);
        }

        SemanticIndexStats {
            total_indexed: self.stats.indexed.load(Ordering::Relaxed),
            total_searches: self.stats.searches.load(Ordering::Relaxed),
            topic_count: indexes.len(),
            messages_per_topic,
        }
    }
}

#[derive(Debug, Default)]
struct IndexStatsInternal {
    indexed: AtomicU64,
    searches: AtomicU64,
}

/// Convenience helper: compute cosine similarity between two vectors.
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    EmbeddingEngine::cosine_similarity(a, b)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ai::providers::MockProvider;

    #[tokio::test]
    async fn test_index_and_search() {
        let provider = Arc::new(MockProvider::new(64));
        let index = SemanticMessageIndex::new(provider);

        index
            .index_message("logs", 0, 1, 1000, "server started successfully")
            .await
            .unwrap();
        index
            .index_message("logs", 0, 2, 2000, "server shutting down")
            .await
            .unwrap();
        index
            .index_message("metrics", 0, 1, 1500, "cpu usage 85%")
            .await
            .unwrap();

        let results = index.search("server started", 5).await.unwrap();
        assert!(!results.is_empty());
        // The top result should come from the logs topic (contains "server" text)
        assert!(
            results.iter().any(|r| r.topic == "logs"),
            "Expected at least one result from 'logs' topic"
        );
    }

    #[tokio::test]
    async fn test_search_with_topic_filter() {
        let provider = Arc::new(MockProvider::new(64));
        let index = SemanticMessageIndex::new(provider);

        index
            .index_message("logs", 0, 1, 1000, "hello world")
            .await
            .unwrap();
        index
            .index_message("metrics", 0, 1, 1000, "hello world")
            .await
            .unwrap();

        let query = SemanticSearchQuery::text("hello")
            .with_topics(vec!["metrics".to_string()])
            .with_min_score(0.0);

        let results = index.search_with_filters(query).await.unwrap();
        assert!(results.iter().all(|r| r.topic == "metrics"));
    }

    #[tokio::test]
    async fn test_search_with_time_range() {
        let provider = Arc::new(MockProvider::new(64));
        let index = SemanticMessageIndex::new(provider);

        index
            .index_message("logs", 0, 1, 1000, "early message")
            .await
            .unwrap();
        index
            .index_message("logs", 0, 2, 5000, "late message")
            .await
            .unwrap();

        let query = SemanticSearchQuery::text("message")
            .with_min_score(0.0)
            .with_time_range(4000, 6000);

        let results = index.search_with_filters(query).await.unwrap();
        assert!(results.iter().all(|r| r.timestamp >= 4000));
    }

    #[tokio::test]
    async fn test_get_stats() {
        let provider = Arc::new(MockProvider::new(64));
        let index = SemanticMessageIndex::new(provider);

        index
            .index_message("t1", 0, 1, 1000, "msg1")
            .await
            .unwrap();
        index
            .index_message("t1", 0, 2, 2000, "msg2")
            .await
            .unwrap();
        index
            .index_message("t2", 0, 1, 1000, "msg3")
            .await
            .unwrap();

        let stats = index.get_stats().await;
        assert_eq!(stats.total_indexed, 3);
        assert_eq!(stats.topic_count, 2);
        assert_eq!(stats.messages_per_topic.get("t1"), Some(&2));
        assert_eq!(stats.messages_per_topic.get("t2"), Some(&1));
    }
}
