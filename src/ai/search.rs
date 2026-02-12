//! Semantic Search Engine
//!
//! Provides semantic search capabilities across stream data using vector embeddings.

use super::config::SearchConfig;
use super::embedding::{EmbeddingEngine, VectorStore};
use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Search engine for semantic search across streams
pub struct SearchEngine {
    /// Configuration
    config: SearchConfig,
    /// Embedding engine
    embeddings: Arc<EmbeddingEngine>,
    /// Semantic index
    index: Arc<RwLock<SemanticIndex>>,
}

impl SearchEngine {
    /// Create a new search engine
    pub fn new(config: SearchConfig, embeddings: Arc<EmbeddingEngine>) -> Result<Self> {
        let dimension = embeddings.dimension();
        let index = Arc::new(RwLock::new(SemanticIndex::new(dimension, &config)));

        Ok(Self {
            config,
            embeddings,
            index,
        })
    }

    /// Index a document
    pub async fn index_document(&self, doc: Document) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        // Generate embedding for the document
        let embedding = self.embeddings.embed_text(&doc.content).await?;

        // Add to index
        let mut index = self.index.write().await;
        index.add(doc.id.clone(), embedding.vector, doc);

        Ok(())
    }

    /// Index multiple documents
    pub async fn index_batch(&self, docs: Vec<Document>) -> Result<usize> {
        if !self.config.enabled {
            return Ok(0);
        }

        let mut indexed = 0;
        for doc in docs {
            self.index_document(doc).await?;
            indexed += 1;
        }

        Ok(indexed)
    }

    /// Search for documents
    pub async fn search(&self, query: &str, limit: usize) -> Result<Vec<SearchResult>> {
        if !self.config.enabled {
            return Err(StreamlineError::Config(
                "Semantic search is not enabled".into(),
            ));
        }

        // Generate embedding for query
        let query_embedding = self.embeddings.embed_text(query).await?;

        // Search index
        let index = self.index.read().await;
        let results = index.search(
            &query_embedding.vector,
            limit,
            self.config.similarity_threshold,
        );

        Ok(results)
    }

    /// Search with a structured query
    pub async fn search_query(&self, query: SearchQuery) -> Result<Vec<SearchResult>> {
        if !self.config.enabled {
            return Err(StreamlineError::Config(
                "Semantic search is not enabled".into(),
            ));
        }

        // Generate embedding for query text
        let query_embedding = self.embeddings.embed_text(&query.text).await?;

        // Search with filters
        let index = self.index.read().await;
        let mut results = index.search(
            &query_embedding.vector,
            query.limit.unwrap_or(self.config.max_results),
            query.min_score.unwrap_or(self.config.similarity_threshold),
        );

        // Apply filters
        if !query.filters.is_empty() {
            results.retain(|r| {
                query
                    .filters
                    .iter()
                    .all(|(key, value)| r.metadata.get(key).is_some_and(|v| v == value))
            });
        }

        // Apply topic filter
        if let Some(ref topics) = query.topics {
            results.retain(|r| r.metadata.get("topic").is_some_and(|t| topics.contains(t)));
        }

        Ok(results)
    }

    /// Hybrid search combining semantic and keyword search
    pub async fn hybrid_search(&self, query: &str, limit: usize) -> Result<Vec<SearchResult>> {
        if !self.config.enabled {
            return Err(StreamlineError::Config(
                "Semantic search is not enabled".into(),
            ));
        }

        if !self.config.hybrid_enabled {
            // Fall back to pure semantic search
            return self.search(query, limit).await;
        }

        // Get semantic results
        let semantic_results = self.search(query, limit * 2).await?;

        // Get keyword results
        let index = self.index.read().await;
        let keyword_results = index.keyword_search(query, limit * 2);

        // Merge and rerank
        let merged = self.merge_results(semantic_results, keyword_results, limit);

        Ok(merged)
    }

    /// Merge semantic and keyword results
    fn merge_results(
        &self,
        semantic: Vec<SearchResult>,
        keyword: Vec<SearchResult>,
        limit: usize,
    ) -> Vec<SearchResult> {
        let mut scores: HashMap<String, f32> = HashMap::new();
        let mut results_map: HashMap<String, SearchResult> = HashMap::new();

        let semantic_weight = self.config.hybrid_weight;
        let keyword_weight = 1.0 - semantic_weight;

        // Add semantic scores
        for result in semantic {
            let score = result.score * semantic_weight;
            scores.insert(result.id.clone(), score);
            results_map.insert(result.id.clone(), result);
        }

        // Add keyword scores
        for result in keyword {
            let score = result.score * keyword_weight;
            *scores.entry(result.id.clone()).or_insert(0.0) += score;
            results_map.entry(result.id.clone()).or_insert(result);
        }

        // Sort by combined score
        let mut combined: Vec<_> = scores.into_iter().collect();
        combined.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        // Take top results
        combined
            .into_iter()
            .take(limit)
            .filter_map(|(id, score)| {
                results_map.remove(&id).map(|mut r| {
                    r.score = score;
                    r
                })
            })
            .collect()
    }

    /// Get index statistics
    pub async fn stats(&self) -> IndexStats {
        let index = self.index.read().await;
        index.stats()
    }

    /// Clear the index
    pub async fn clear(&self) {
        let mut index = self.index.write().await;
        index.clear();
    }
}

/// Document to be indexed
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Document {
    /// Unique document ID
    pub id: String,
    /// Document content (text)
    pub content: String,
    /// Associated metadata
    pub metadata: HashMap<String, String>,
    /// Source topic (if from stream)
    pub topic: Option<String>,
    /// Source partition
    pub partition: Option<u32>,
    /// Source offset
    pub offset: Option<u64>,
    /// Timestamp
    pub timestamp: Option<i64>,
}

impl Document {
    /// Create a new document
    pub fn new(id: String, content: String) -> Self {
        Self {
            id,
            content,
            metadata: HashMap::new(),
            topic: None,
            partition: None,
            offset: None,
            timestamp: None,
        }
    }

    /// Add metadata
    pub fn with_metadata(mut self, key: &str, value: &str) -> Self {
        self.metadata.insert(key.to_string(), value.to_string());
        self
    }

    /// Set topic source
    pub fn with_source(mut self, topic: &str, partition: u32, offset: u64) -> Self {
        self.topic = Some(topic.to_string());
        self.partition = Some(partition);
        self.offset = Some(offset);
        self
    }
}

/// Search query with filters
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchQuery {
    /// Query text
    pub text: String,
    /// Maximum results
    pub limit: Option<usize>,
    /// Minimum similarity score
    pub min_score: Option<f32>,
    /// Metadata filters (key=value)
    pub filters: HashMap<String, String>,
    /// Filter by topics
    pub topics: Option<Vec<String>>,
    /// Time range start (unix timestamp)
    pub time_start: Option<i64>,
    /// Time range end (unix timestamp)
    pub time_end: Option<i64>,
}

impl SearchQuery {
    /// Create a new search query
    pub fn new(text: &str) -> Self {
        Self {
            text: text.to_string(),
            limit: None,
            min_score: None,
            filters: HashMap::new(),
            topics: None,
            time_start: None,
            time_end: None,
        }
    }

    /// Set result limit
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Set minimum score
    pub fn min_score(mut self, score: f32) -> Self {
        self.min_score = Some(score);
        self
    }

    /// Add filter
    pub fn filter(mut self, key: &str, value: &str) -> Self {
        self.filters.insert(key.to_string(), value.to_string());
        self
    }

    /// Filter by topics
    pub fn topics(mut self, topics: Vec<String>) -> Self {
        self.topics = Some(topics);
        self
    }

    /// Set time range
    pub fn time_range(mut self, start: i64, end: i64) -> Self {
        self.time_start = Some(start);
        self.time_end = Some(end);
        self
    }
}

/// Search result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    /// Document ID
    pub id: String,
    /// Similarity score (0.0 - 1.0)
    pub score: f32,
    /// Document content
    pub content: String,
    /// Metadata
    pub metadata: HashMap<String, String>,
    /// Highlighted snippets
    pub highlights: Vec<String>,
}

/// Semantic index for vector search
pub struct SemanticIndex {
    /// Vector dimension
    dimension: usize,
    /// Stored vectors with metadata
    vectors: VectorStore,
    /// Document storage
    documents: HashMap<String, Document>,
    /// Inverted index for keyword search
    inverted_index: HashMap<String, Vec<String>>,
    /// Index configuration
    #[allow(dead_code)]
    num_neighbors: usize,
}

impl SemanticIndex {
    /// Create a new semantic index
    pub fn new(dimension: usize, config: &SearchConfig) -> Self {
        Self {
            dimension,
            vectors: VectorStore::new(dimension),
            documents: HashMap::new(),
            inverted_index: HashMap::new(),
            num_neighbors: config.num_neighbors,
        }
    }

    /// Add a document to the index
    pub fn add(&mut self, id: String, vector: Vec<f32>, doc: Document) {
        // Add to vector store
        let metadata = serde_json::to_value(&doc.metadata).ok();
        self.vectors.add(id.clone(), vector, metadata);

        // Build inverted index
        let tokens = tokenize(&doc.content);
        for token in tokens {
            self.inverted_index
                .entry(token)
                .or_default()
                .push(id.clone());
        }

        // Store document
        self.documents.insert(id, doc);
    }

    /// Search for similar vectors
    pub fn search(&self, query: &[f32], limit: usize, min_score: f32) -> Vec<SearchResult> {
        let hits = self.vectors.search(query, limit);

        hits.into_iter()
            .filter(|hit| hit.score >= min_score)
            .filter_map(|hit| {
                self.documents.get(&hit.id).map(|doc| {
                    let highlights = extract_highlights(&doc.content, 3);
                    SearchResult {
                        id: hit.id,
                        score: hit.score,
                        content: doc.content.clone(),
                        metadata: doc.metadata.clone(),
                        highlights,
                    }
                })
            })
            .collect()
    }

    /// Keyword-based search
    pub fn keyword_search(&self, query: &str, limit: usize) -> Vec<SearchResult> {
        let query_tokens = tokenize(query);
        let mut doc_scores: HashMap<String, f32> = HashMap::new();

        // Calculate BM25-like scores
        for token in &query_tokens {
            if let Some(doc_ids) = self.inverted_index.get(token) {
                let idf = ((self.documents.len() as f32 + 1.0) / (doc_ids.len() as f32 + 0.5)).ln();
                for doc_id in doc_ids {
                    *doc_scores.entry(doc_id.clone()).or_insert(0.0) += idf;
                }
            }
        }

        // Normalize scores
        let max_score = doc_scores.values().copied().fold(0.0f32, f32::max);
        if max_score > 0.0 {
            for score in doc_scores.values_mut() {
                *score /= max_score;
            }
        }

        // Sort and return top results
        let mut scored: Vec<_> = doc_scores.into_iter().collect();
        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        scored
            .into_iter()
            .take(limit)
            .filter_map(|(id, score)| {
                self.documents.get(&id).map(|doc| {
                    let highlights = highlight_query(&doc.content, &query_tokens);
                    SearchResult {
                        id,
                        score,
                        content: doc.content.clone(),
                        metadata: doc.metadata.clone(),
                        highlights,
                    }
                })
            })
            .collect()
    }

    /// Get index statistics
    pub fn stats(&self) -> IndexStats {
        IndexStats {
            total_documents: self.documents.len(),
            total_vectors: self.vectors.len(),
            dimension: self.dimension,
            vocabulary_size: self.inverted_index.len(),
        }
    }

    /// Clear the index
    pub fn clear(&mut self) {
        self.vectors = VectorStore::new(self.dimension);
        self.documents.clear();
        self.inverted_index.clear();
    }
}

/// Index statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexStats {
    /// Total indexed documents
    pub total_documents: usize,
    /// Total vectors
    pub total_vectors: usize,
    /// Vector dimension
    pub dimension: usize,
    /// Vocabulary size for keyword search
    pub vocabulary_size: usize,
}

/// Tokenize text for indexing
fn tokenize(text: &str) -> Vec<String> {
    text.to_lowercase()
        .split(|c: char| !c.is_alphanumeric())
        .filter(|s| !s.is_empty() && s.len() > 2)
        .map(|s| s.to_string())
        .collect()
}

/// Extract highlight snippets from content
fn extract_highlights(content: &str, max_highlights: usize) -> Vec<String> {
    let sentences: Vec<&str> = content.split(['.', '!', '?']).collect();
    sentences
        .into_iter()
        .take(max_highlights)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Highlight query terms in content
fn highlight_query(content: &str, query_tokens: &[String]) -> Vec<String> {
    let sentences: Vec<&str> = content.split(['.', '!', '?']).collect();

    sentences
        .into_iter()
        .filter(|sentence| {
            let lower = sentence.to_lowercase();
            query_tokens.iter().any(|token| lower.contains(token))
        })
        .take(3)
        .map(|s| s.trim().to_string())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ai::config::EmbeddingConfig;

    #[test]
    fn test_tokenize() {
        let tokens = tokenize("Hello, World! This is a test.");
        assert!(tokens.contains(&"hello".to_string()));
        assert!(tokens.contains(&"world".to_string()));
        assert!(tokens.contains(&"this".to_string()));
        assert!(tokens.contains(&"test".to_string()));
        // Short words filtered out
        assert!(!tokens.contains(&"is".to_string()));
        assert!(!tokens.contains(&"a".to_string()));
    }

    #[test]
    fn test_document_builder() {
        let doc = Document::new("doc1".to_string(), "Test content".to_string())
            .with_metadata("type", "test")
            .with_source("topic1", 0, 100);

        assert_eq!(doc.id, "doc1");
        assert_eq!(doc.metadata.get("type"), Some(&"test".to_string()));
        assert_eq!(doc.topic, Some("topic1".to_string()));
        assert_eq!(doc.partition, Some(0));
        assert_eq!(doc.offset, Some(100));
    }

    #[test]
    fn test_search_query_builder() {
        let query = SearchQuery::new("test query")
            .limit(10)
            .min_score(0.5)
            .filter("type", "document")
            .topics(vec!["topic1".to_string()]);

        assert_eq!(query.text, "test query");
        assert_eq!(query.limit, Some(10));
        assert_eq!(query.min_score, Some(0.5));
        assert_eq!(query.filters.get("type"), Some(&"document".to_string()));
    }

    #[tokio::test]
    async fn test_search_engine() {
        let embedding_config = EmbeddingConfig::default();
        // Lower threshold for simple hash-based embeddings in tests
        let search_config = SearchConfig {
            similarity_threshold: 0.0,
            ..Default::default()
        };
        let embeddings = Arc::new(EmbeddingEngine::new(embedding_config).unwrap());
        let engine = SearchEngine::new(search_config, embeddings).unwrap();

        // Index some documents
        let doc1 = Document::new(
            "doc1".to_string(),
            "The quick brown fox jumps over".to_string(),
        );
        let doc2 = Document::new("doc2".to_string(), "A lazy dog sleeps all day".to_string());

        engine.index_document(doc1).await.unwrap();
        engine.index_document(doc2).await.unwrap();

        // Check stats first
        let stats = engine.stats().await;
        assert_eq!(stats.total_documents, 2);

        // Search with matching content
        let results = engine.search("quick brown fox", 10).await.unwrap();
        // With simple hash embeddings, the first result should be the fox document
        assert!(!results.is_empty());
        assert_eq!(results[0].id, "doc1");
    }
}
