//! RAG (Retrieval-Augmented Generation) Pipeline
//!
//! Provides a complete RAG system built on top of Streamline's streaming infrastructure:
//! - Document chunking and embedding generation
//! - Vector storage with HNSW indexing
//! - Context retrieval with semantic search
//! - LLM integration for response generation
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
//! │ Documents   │───▶│ Chunker     │───▶│ Embedder    │───▶│ Vector      │
//! │ (Streaming) │    │             │    │             │    │ Store       │
//! └─────────────┘    └─────────────┘    └─────────────┘    └──────┬──────┘
//!                                                                  │
//!                                                                  ▼
//! ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
//! │ Response    │◀───│ LLM         │◀───│ Context     │◀───│ Similarity  │
//! │             │    │ Generator   │    │ Builder     │    │ Search      │
//! └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
//! ```

use crate::ai::config::LLMProviderType;
use crate::ai::embedding::EmbeddingEngine;
use crate::ai::hnsw::{HnswConfig, HnswIndex};
use crate::ai::llm::LLMClient;
use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// RAG pipeline configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RagPipelineConfig {
    /// Enable RAG pipeline
    pub enabled: bool,
    /// Chunking configuration
    pub chunking: ChunkingConfig,
    /// Embedding configuration
    pub embedding: EmbeddingProviderConfig,
    /// Vector store configuration
    pub vector_store: VectorStoreConfig,
    /// Retrieval configuration
    pub retrieval: RetrievalConfig,
    /// Generation configuration
    pub generation: GenerationConfig,
}

impl Default for RagPipelineConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            chunking: ChunkingConfig::default(),
            embedding: EmbeddingProviderConfig::default(),
            vector_store: VectorStoreConfig::default(),
            retrieval: RetrievalConfig::default(),
            generation: GenerationConfig::default(),
        }
    }
}

/// Chunking configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkingConfig {
    /// Chunking strategy
    pub strategy: ChunkingStrategy,
    /// Maximum chunk size in characters
    pub max_chunk_size: usize,
    /// Overlap between chunks in characters
    pub chunk_overlap: usize,
    /// Minimum chunk size (skip smaller chunks)
    pub min_chunk_size: usize,
    /// Separator patterns for semantic chunking
    pub separators: Vec<String>,
}

impl Default for ChunkingConfig {
    fn default() -> Self {
        Self {
            strategy: ChunkingStrategy::Semantic,
            max_chunk_size: 1000,
            chunk_overlap: 200,
            min_chunk_size: 100,
            separators: vec![
                "\n\n".to_string(),
                "\n".to_string(),
                ". ".to_string(),
                "! ".to_string(),
                "? ".to_string(),
            ],
        }
    }
}

/// Chunking strategy
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChunkingStrategy {
    /// Fixed-size chunks
    FixedSize,
    /// Semantic chunking (respects sentence/paragraph boundaries)
    #[default]
    Semantic,
    /// Sentence-based chunking
    Sentence,
    /// Recursive character text splitter
    Recursive,
}

/// Embedding provider configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingProviderConfig {
    /// Provider type
    pub provider: EmbeddingProviderType,
    /// Model name
    pub model: String,
    /// API key (if required)
    pub api_key: Option<String>,
    /// API endpoint (for custom providers)
    pub endpoint: Option<String>,
    /// Vector dimension
    pub dimension: usize,
    /// Batch size for embedding generation
    pub batch_size: usize,
}

impl Default for EmbeddingProviderConfig {
    fn default() -> Self {
        Self {
            provider: EmbeddingProviderType::Mock,
            model: "text-embedding-3-small".to_string(),
            api_key: None,
            endpoint: None,
            dimension: 1536,
            batch_size: 100,
        }
    }
}

/// Embedding provider type
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum EmbeddingProviderType {
    /// OpenAI embeddings
    OpenAI,
    /// Cohere embeddings
    Cohere,
    /// Local model (via ONNX)
    Local,
    /// Mock provider for testing
    #[default]
    Mock,
}

/// Vector store configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorStoreConfig {
    /// Enable persistence
    pub persistent: bool,
    /// Storage path
    pub path: Option<String>,
    /// HNSW configuration
    pub hnsw: HnswConfig,
    /// Maximum vectors to store
    pub max_vectors: usize,
}

impl Default for VectorStoreConfig {
    fn default() -> Self {
        Self {
            persistent: false,
            path: None,
            hnsw: HnswConfig::balanced(1536),
            max_vectors: 1_000_000,
        }
    }
}

/// Retrieval configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetrievalConfig {
    /// Number of chunks to retrieve
    pub top_k: usize,
    /// Minimum similarity score
    pub min_score: f32,
    /// Enable re-ranking
    pub rerank: bool,
    /// Re-ranker model (if enabled)
    pub reranker_model: Option<String>,
    /// Enable hybrid search (keyword + semantic)
    pub hybrid_search: bool,
    /// Keyword search weight (0.0 - 1.0)
    pub keyword_weight: f32,
}

impl Default for RetrievalConfig {
    fn default() -> Self {
        Self {
            top_k: 5,
            min_score: 0.7,
            rerank: false,
            reranker_model: None,
            hybrid_search: false,
            keyword_weight: 0.3,
        }
    }
}

/// Generation configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenerationConfig {
    /// LLM provider type
    pub provider: LLMProviderType,
    /// Model name
    pub model: String,
    /// Maximum tokens to generate
    pub max_tokens: usize,
    /// Temperature
    pub temperature: f32,
    /// System prompt template
    pub system_prompt: String,
    /// Context prompt template
    pub context_template: String,
    /// Include source citations
    pub include_citations: bool,
}

impl Default for GenerationConfig {
    fn default() -> Self {
        Self {
            provider: LLMProviderType::OpenAI,
            model: "gpt-4".to_string(),
            max_tokens: 1024,
            temperature: 0.7,
            system_prompt: "You are a helpful assistant that answers questions based on the provided context. If the context doesn't contain relevant information, say so.".to_string(),
            context_template: "Context:\n{context}\n\nQuestion: {question}".to_string(),
            include_citations: true,
        }
    }
}

/// A document chunk with embedding
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentChunk {
    /// Chunk ID
    pub id: String,
    /// Document ID this chunk belongs to
    pub document_id: String,
    /// Chunk index within document
    pub chunk_index: usize,
    /// Chunk text content
    pub content: String,
    /// Start offset in original document
    pub start_offset: usize,
    /// End offset in original document
    pub end_offset: usize,
    /// Chunk metadata
    pub metadata: HashMap<String, String>,
    /// Embedding vector (if generated)
    #[serde(skip)]
    pub embedding: Option<Vec<f32>>,
}

/// Retrieved context for RAG
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetrievedContext {
    /// Retrieved chunks with scores
    pub chunks: Vec<ScoredChunk>,
    /// Total chunks considered
    pub total_considered: usize,
    /// Retrieval latency in milliseconds
    pub latency_ms: u64,
}

/// A chunk with similarity score
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScoredChunk {
    /// The chunk
    pub chunk: DocumentChunk,
    /// Similarity score
    pub score: f32,
    /// Rank in results
    pub rank: usize,
}

/// RAG response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RagResponse {
    /// Generated answer
    pub answer: String,
    /// Source chunks used
    pub sources: Vec<SourceCitation>,
    /// Model used
    pub model: String,
    /// Total tokens used
    pub tokens_used: usize,
    /// Response latency in milliseconds
    pub latency_ms: u64,
    /// Retrieval context
    pub context: RetrievedContext,
}

/// Source citation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceCitation {
    /// Document ID
    pub document_id: String,
    /// Chunk ID
    pub chunk_id: String,
    /// Relevant text snippet
    pub snippet: String,
    /// Relevance score
    pub score: f32,
}

/// Document chunker
pub struct DocumentChunker {
    config: ChunkingConfig,
}

impl DocumentChunker {
    /// Create a new chunker
    pub fn new(config: ChunkingConfig) -> Self {
        Self { config }
    }

    /// Chunk a document
    pub fn chunk(&self, document_id: &str, text: &str) -> Vec<DocumentChunk> {
        match self.config.strategy {
            ChunkingStrategy::FixedSize => self.chunk_fixed_size(document_id, text),
            ChunkingStrategy::Semantic => self.chunk_semantic(document_id, text),
            ChunkingStrategy::Sentence => self.chunk_sentences(document_id, text),
            ChunkingStrategy::Recursive => self.chunk_recursive(document_id, text),
        }
    }

    fn chunk_fixed_size(&self, document_id: &str, text: &str) -> Vec<DocumentChunk> {
        let mut chunks = Vec::new();
        let chars: Vec<char> = text.chars().collect();
        let mut start = 0;
        let mut chunk_index = 0;

        while start < chars.len() {
            let end = (start + self.config.max_chunk_size).min(chars.len());
            let content: String = chars[start..end].iter().collect();

            if content.len() >= self.config.min_chunk_size {
                chunks.push(DocumentChunk {
                    id: format!("{}-{}", document_id, chunk_index),
                    document_id: document_id.to_string(),
                    chunk_index,
                    content,
                    start_offset: start,
                    end_offset: end,
                    metadata: HashMap::new(),
                    embedding: None,
                });
                chunk_index += 1;
            }

            // If we've reached the end of text, stop
            if end >= chars.len() {
                break;
            }
            start = end.saturating_sub(self.config.chunk_overlap);
            if start >= end {
                break;
            }
        }

        chunks
    }

    fn chunk_semantic(&self, document_id: &str, text: &str) -> Vec<DocumentChunk> {
        let mut chunks = Vec::new();
        let mut current_chunk = String::new();
        let mut current_start = 0;
        let mut chunk_index = 0;

        // Split by primary separator first (paragraphs)
        for (sep_idx, separator) in self.config.separators.iter().enumerate() {
            if text.contains(separator) {
                let parts: Vec<&str> = text.split(separator).collect();

                for part in parts {
                    if current_chunk.len() + part.len() <= self.config.max_chunk_size {
                        if !current_chunk.is_empty() {
                            current_chunk.push_str(separator);
                        }
                        current_chunk.push_str(part);
                    } else {
                        // Save current chunk if it meets minimum size
                        if current_chunk.len() >= self.config.min_chunk_size {
                            let end_offset = current_start + current_chunk.len();
                            chunks.push(DocumentChunk {
                                id: format!("{}-{}", document_id, chunk_index),
                                document_id: document_id.to_string(),
                                chunk_index,
                                content: current_chunk.clone(),
                                start_offset: current_start,
                                end_offset,
                                metadata: HashMap::new(),
                                embedding: None,
                            });
                            chunk_index += 1;
                            current_start = end_offset.saturating_sub(self.config.chunk_overlap);
                        }
                        current_chunk = part.to_string();
                    }
                }

                // Save final chunk
                if current_chunk.len() >= self.config.min_chunk_size {
                    chunks.push(DocumentChunk {
                        id: format!("{}-{}", document_id, chunk_index),
                        document_id: document_id.to_string(),
                        chunk_index,
                        content: current_chunk,
                        start_offset: current_start,
                        end_offset: text.len(),
                        metadata: HashMap::new(),
                        embedding: None,
                    });
                }

                return chunks;
            }

            // If this is the last separator and no matches, fall through
            if sep_idx == self.config.separators.len() - 1 {
                break;
            }
        }

        // Fallback to fixed-size chunking
        self.chunk_fixed_size(document_id, text)
    }

    fn chunk_sentences(&self, document_id: &str, text: &str) -> Vec<DocumentChunk> {
        // Simple sentence detection
        let sentence_endings = [". ", "! ", "? ", ".\n", "!\n", "?\n"];
        let mut chunks = Vec::new();
        let mut current_chunk = String::new();
        let mut current_start = 0;
        let mut chunk_index = 0;
        let mut last_end: usize;

        for (i, c) in text.char_indices() {
            current_chunk.push(c);

            // Check for sentence ending
            let is_sentence_end = sentence_endings
                .iter()
                .any(|ending| current_chunk.ends_with(ending));

            if (is_sentence_end || current_chunk.len() >= self.config.max_chunk_size)
                && current_chunk.len() >= self.config.min_chunk_size
            {
                chunks.push(DocumentChunk {
                    id: format!("{}-{}", document_id, chunk_index),
                    document_id: document_id.to_string(),
                    chunk_index,
                    content: current_chunk.trim().to_string(),
                    start_offset: current_start,
                    end_offset: i + 1,
                    metadata: HashMap::new(),
                    embedding: None,
                });
                chunk_index += 1;
                last_end = i + 1;

                // Handle overlap
                current_start = (i + 1).saturating_sub(self.config.chunk_overlap);
                if current_start < last_end {
                    current_chunk = text[current_start..=i].to_string();
                } else {
                    current_chunk.clear();
                }
            }
        }

        // Add remaining content
        if !current_chunk.trim().is_empty() && current_chunk.len() >= self.config.min_chunk_size {
            chunks.push(DocumentChunk {
                id: format!("{}-{}", document_id, chunk_index),
                document_id: document_id.to_string(),
                chunk_index,
                content: current_chunk.trim().to_string(),
                start_offset: current_start,
                end_offset: text.len(),
                metadata: HashMap::new(),
                embedding: None,
            });
        }

        chunks
    }

    fn chunk_recursive(&self, document_id: &str, text: &str) -> Vec<DocumentChunk> {
        self.chunk_recursive_impl(document_id, text, 0, 0)
    }

    fn chunk_recursive_impl(
        &self,
        document_id: &str,
        text: &str,
        separator_idx: usize,
        base_offset: usize,
    ) -> Vec<DocumentChunk> {
        if text.len() <= self.config.max_chunk_size {
            if text.len() >= self.config.min_chunk_size {
                return vec![DocumentChunk {
                    id: format!("{}-{}", document_id, base_offset),
                    document_id: document_id.to_string(),
                    chunk_index: 0,
                    content: text.to_string(),
                    start_offset: base_offset,
                    end_offset: base_offset + text.len(),
                    metadata: HashMap::new(),
                    embedding: None,
                }];
            }
            return Vec::new();
        }

        if separator_idx >= self.config.separators.len() {
            return self.chunk_fixed_size(document_id, text);
        }

        let separator = &self.config.separators[separator_idx];
        let parts: Vec<&str> = text.split(separator).collect();

        if parts.len() == 1 {
            return self.chunk_recursive_impl(document_id, text, separator_idx + 1, base_offset);
        }

        let mut chunks = Vec::new();
        let mut current_offset = base_offset;

        for part in parts {
            let sub_chunks =
                self.chunk_recursive_impl(document_id, part, separator_idx + 1, current_offset);
            chunks.extend(sub_chunks);
            current_offset += part.len() + separator.len();
        }

        chunks
    }
}

/// RAG Pipeline
pub struct RagPipeline {
    config: RagPipelineConfig,
    chunker: DocumentChunker,
    embeddings: Arc<EmbeddingEngine>,
    vector_index: Arc<RwLock<HnswIndex>>,
    chunks_store: Arc<RwLock<HashMap<String, DocumentChunk>>>,
    llm: Arc<LLMClient>,
}

impl RagPipeline {
    /// Create a new RAG pipeline
    pub fn new(config: RagPipelineConfig) -> Result<Self> {
        let embedding_config = crate::ai::config::EmbeddingConfig {
            enabled: config.enabled,
            model: crate::ai::config::EmbeddingModelType::default(),
            dimension: config.embedding.dimension,
            batch_size: config.embedding.batch_size,
            ..Default::default()
        };

        let embeddings = Arc::new(EmbeddingEngine::new(embedding_config)?);

        let vector_index = Arc::new(RwLock::new(HnswIndex::new(
            config.vector_store.hnsw.clone(),
        )));

        let llm_config = crate::ai::config::LLMConfig {
            enabled: true,
            provider: config.generation.provider.clone(),
            model: config.generation.model.clone(),
            temperature: config.generation.temperature,
            max_tokens: config.generation.max_tokens,
            ..Default::default()
        };

        let llm = Arc::new(LLMClient::new(llm_config)?);

        Ok(Self {
            chunker: DocumentChunker::new(config.chunking.clone()),
            config,
            embeddings,
            vector_index,
            chunks_store: Arc::new(RwLock::new(HashMap::new())),
            llm,
        })
    }

    /// Ingest a document into the RAG pipeline
    pub async fn ingest_document(
        &self,
        document_id: &str,
        text: &str,
        metadata: HashMap<String, String>,
    ) -> Result<IngestResult> {
        let start = std::time::Instant::now();

        // Chunk the document
        let mut chunks = self.chunker.chunk(document_id, text);

        // Add metadata to chunks
        for chunk in &mut chunks {
            chunk.metadata = metadata.clone();
        }

        // Generate embeddings for all chunks
        let texts: Vec<&str> = chunks.iter().map(|c| c.content.as_str()).collect();
        let embeddings = self.embeddings.embed_batch(&texts).await?;

        // Store chunks and update vector index
        let index = self.vector_index.write().await;
        let mut store = self.chunks_store.write().await;

        for (i, (chunk, embedding)) in chunks.iter_mut().zip(embeddings.iter()).enumerate() {
            chunk.embedding = Some(embedding.vector.clone());
            // Use hash of chunk_id as the i64 identifier
            let id = i as i64;
            index.insert(id, embedding.vector.clone()).await?;
            store.insert(chunk.id.clone(), chunk.clone());
        }

        Ok(IngestResult {
            document_id: document_id.to_string(),
            chunks_created: chunks.len(),
            latency_ms: start.elapsed().as_millis() as u64,
        })
    }

    /// Query the RAG pipeline
    pub async fn query(&self, question: &str) -> Result<RagResponse> {
        let start = std::time::Instant::now();

        // Retrieve relevant context
        let context = self.retrieve(question).await?;

        // Build prompt with context
        let context_text = context
            .chunks
            .iter()
            .map(|sc| sc.chunk.content.as_str())
            .collect::<Vec<_>>()
            .join("\n\n---\n\n");

        let prompt = self
            .config
            .generation
            .context_template
            .replace("{context}", &context_text)
            .replace("{question}", question);

        // Generate response using the LLM client's summarize capability
        // (since we don't have a raw 'complete' method, we'll use summarize as a workaround)
        let response_text = self
            .llm
            .summarize(&prompt, self.config.generation.max_tokens)
            .await?;

        // Build citations
        let sources = if self.config.generation.include_citations {
            context
                .chunks
                .iter()
                .map(|sc| SourceCitation {
                    document_id: sc.chunk.document_id.clone(),
                    chunk_id: sc.chunk.id.clone(),
                    snippet: sc.chunk.content.chars().take(200).collect::<String>() + "...",
                    score: sc.score,
                })
                .collect()
        } else {
            Vec::new()
        };

        Ok(RagResponse {
            answer: response_text,
            sources,
            model: self.config.generation.model.clone(),
            tokens_used: 0, // Not tracked in summarize
            latency_ms: start.elapsed().as_millis() as u64,
            context,
        })
    }

    /// Retrieve relevant context for a query
    pub async fn retrieve(&self, query: &str) -> Result<RetrievedContext> {
        let start = std::time::Instant::now();

        // Generate query embedding
        let query_embedding = self.embeddings.embed_text(query).await?;

        // Search vector index
        let index = self.vector_index.read().await;
        let results = index
            .search(&query_embedding.vector, self.config.retrieval.top_k)
            .await?;
        drop(index);

        // Fetch chunks
        let store = self.chunks_store.read().await;
        let mut scored_chunks = Vec::new();
        let mut total_considered = 0;

        for (rank, result) in results.iter().enumerate() {
            total_considered += 1;
            // Convert distance to score (inverse - lower distance = higher score)
            let score = 1.0 / (1.0 + result.distance);
            if score < self.config.retrieval.min_score {
                continue;
            }

            // In this simple implementation, we use rank as a fallback
            // In production, you'd map result.id back to chunk_id
            let chunk_id = format!("chunk-{}", result.id);
            if let Some(chunk) = store.get(&chunk_id) {
                scored_chunks.push(ScoredChunk {
                    chunk: chunk.clone(),
                    score,
                    rank,
                });
            }
        }

        Ok(RetrievedContext {
            chunks: scored_chunks,
            total_considered,
            latency_ms: start.elapsed().as_millis() as u64,
        })
    }

    /// Delete a document and all its chunks
    pub async fn delete_document(&self, document_id: &str) -> Result<usize> {
        let mut store = self.chunks_store.write().await;
        let index = self.vector_index.write().await;

        let chunk_ids: Vec<String> = store
            .iter()
            .filter(|(_, chunk)| chunk.document_id == document_id)
            .map(|(id, _)| id.clone())
            .collect();

        for (idx, chunk_id) in chunk_ids.iter().enumerate() {
            store.remove(chunk_id);
            // Use index as i64 id (matching how we inserted)
            match index.remove(idx as i64).await {
                Ok(_) => {}
                Err(e) => {
                    // Log but continue - index may be out of sync
                    tracing::warn!("Failed to remove chunk {} from index: {}", chunk_id, e);
                }
            }
        }

        Ok(chunk_ids.len())
    }

    /// Get pipeline statistics
    pub fn stats(&self) -> RagPipelineStats {
        RagPipelineStats {
            enabled: self.config.enabled,
            embedding_dimension: self.config.embedding.dimension,
            retrieval_top_k: self.config.retrieval.top_k,
            generation_model: self.config.generation.model.clone(),
        }
    }
}

/// Document ingestion result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestResult {
    /// Document ID
    pub document_id: String,
    /// Number of chunks created
    pub chunks_created: usize,
    /// Ingestion latency in milliseconds
    pub latency_ms: u64,
}

/// RAG pipeline statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RagPipelineStats {
    /// Pipeline enabled
    pub enabled: bool,
    /// Embedding dimension
    pub embedding_dimension: usize,
    /// Retrieval top_k
    pub retrieval_top_k: usize,
    /// Generation model
    pub generation_model: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_document_chunker_fixed_size() {
        let config = ChunkingConfig {
            strategy: ChunkingStrategy::FixedSize,
            max_chunk_size: 100,
            chunk_overlap: 20,
            min_chunk_size: 10,
            ..Default::default()
        };
        let chunker = DocumentChunker::new(config);

        let text = "a".repeat(250);
        let chunks = chunker.chunk("doc1", &text);

        assert!(!chunks.is_empty());
        for chunk in &chunks {
            assert!(chunk.content.len() <= 100);
        }
    }

    #[test]
    fn test_document_chunker_semantic() {
        let config = ChunkingConfig {
            strategy: ChunkingStrategy::Semantic,
            max_chunk_size: 200,
            chunk_overlap: 20,
            min_chunk_size: 10,
            ..Default::default()
        };
        let chunker = DocumentChunker::new(config);

        let text = "First paragraph with some content.\n\nSecond paragraph with different content.\n\nThird paragraph.";
        let chunks = chunker.chunk("doc1", text);

        assert!(!chunks.is_empty());
    }

    #[test]
    fn test_document_chunker_sentence() {
        let config = ChunkingConfig {
            strategy: ChunkingStrategy::Sentence,
            max_chunk_size: 100,
            chunk_overlap: 10,
            min_chunk_size: 5,
            ..Default::default()
        };
        let chunker = DocumentChunker::new(config);

        let text = "First sentence. Second sentence. Third sentence.";
        let chunks = chunker.chunk("doc1", text);

        assert!(!chunks.is_empty());
    }

    #[tokio::test]
    async fn test_rag_pipeline_creation() {
        let config = RagPipelineConfig::default();
        let pipeline = RagPipeline::new(config);
        assert!(pipeline.is_ok());
    }
}
