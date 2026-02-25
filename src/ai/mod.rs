//! AI-Native Streaming Module
//!
//! Provides AI-powered streaming capabilities including vector embeddings,
//! semantic search, LLM integration, and anomaly detection.
//!
//! # Features
//!
//! - **Vector Embeddings**: Store and index embeddings on stream data
//! - **Semantic Search**: Search streams by meaning, not just keywords
//! - **LLM Integration**: Stream enrichment and classification via LLMs
//! - **Semantic Routing**: AI-powered message routing based on content
//! - **Anomaly Detection**: Real-time pattern-based anomaly detection
//!
//! # Example
//!
//! ```rust,ignore
//! use streamline::ai::{AIManager, EmbeddingConfig, SemanticSearchConfig};
//!
//! // Create AI manager
//! let config = AIConfig::default();
//! let ai = AIManager::new(config)?;
//!
//! // Generate embeddings for messages
//! let embeddings = ai.embed_texts(&["Hello world", "Goodbye world"])?;
//!
//! // Semantic search
//! let results = ai.semantic_search("greeting", 10)?;
//! ```

pub mod anomaly;
pub mod auto_embed;
pub mod config;
pub mod embedding;
pub mod embeddings;
pub mod gateway;
pub mod hnsw;
pub mod llm;
pub mod llm_streaming;
pub mod pattern;
pub mod persistent_store;
pub mod pipeline;
pub mod providers;
pub mod rag;
pub mod routing;
pub mod search;
pub mod semantic_partitioner;
pub mod semantic_record;
pub mod semantic_search;
pub mod similarity_stream;
pub mod summarization;
pub mod vector_streaming;

pub use anomaly::{AnomalyDetector, AnomalyEvent, AnomalyResult, AnomalyType, DetectorConfig};
pub use config::{
    AIConfig, AnomalyConfig, EmbeddingConfig, LLMConfig, RoutingConfig, SearchConfig,
};
pub use pattern::{
    PatternAnalysis, PatternConfig, PatternRecognizer, SeasonalPattern, Trend,
};
pub use summarization::{
    AggregateStats, HyperLogLog, InferredField, InferredSchema, InferredType,
    StreamSummarizer, SummarizationConfig, TopicSummary,
};
pub use embedding::{EmbeddingEngine, EmbeddingModel, EmbeddingResult, VectorStore};
pub use embeddings::{
    CachedProvider, EmbeddingProviderConfig as EnhancedEmbeddingProviderConfig,
    EmbeddingProviderSelection, LocalProvider, create_embedding_provider,
};
pub use hnsw::{DistanceMetric, HnswConfig, HnswIndex, HnswStatsSnapshot};
pub use llm::{
    ClassificationResult, EnrichmentResult, LLMClient, LLMProvider, StreamEnricher, StreamProcessor,
};
pub use llm_streaming::{
    ChatMessage, FinishReason, LLMStreamClient, LLMStreamConfig, LLMStreamManager,
    LLMStreamProvider, LLMStreamStats, MockStreamProvider, SemanticBoundary, StreamChunk,
    StreamingResponse,
};
pub use pipeline::{
    AIPipeline, AnomalyMethod, ErrorStrategy, FilterAction, PipelineBuilder, PipelineConfig,
    PipelineInfo, PipelineManager, PipelineMetrics, PipelineMetricsSnapshot, PipelineStage,
    PipelineState, RoutingRule as PipelineRoutingRule,
};
pub use providers::{
    create_provider, CohereProvider, EmbeddingProvider, MockProvider, OpenAIProvider,
    ProviderConfig, ProviderStats, ProviderType, QuantizationType,
};
pub use rag::{
    ChunkingConfig, ChunkingStrategy, DocumentChunk, DocumentChunker, EmbeddingProviderConfig,
    EmbeddingProviderType, GenerationConfig, IngestResult, RagPipeline as EnhancedRagPipeline,
    RagPipelineConfig, RagPipelineStats, RagResponse, RetrievalConfig, RetrievedContext,
    ScoredChunk, SourceCitation, VectorStoreConfig,
};
pub use routing::{RoutingDecision, RoutingRule, SemanticRouter};
pub use search::{SearchEngine, SearchQuery, SearchResult, SemanticIndex};
pub use semantic_partitioner::{
    PartitionInfo, PartitionerStatsSnapshot, SemanticPartitioner, SemanticPartitionerConfig,
};
pub use semantic_record::{
    EmbeddingMetadata, SemanticRecord, SemanticRecordBuilder, SemanticType, SEMANTIC_DIM_HEADER,
    SEMANTIC_MODEL_HEADER, SEMANTIC_NORM_HEADER, SEMANTIC_QUANT_HEADER, SEMANTIC_TYPE_HEADER,
};
pub use semantic_search::{
    SemanticIndexStats, SemanticMessageIndex, SemanticSearchQuery, SemanticSearchResult,
};
pub use similarity_stream::{
    NewRecordEvent, QueryType, SimilarityConfig, SimilarityResult, SimilarityStatsSnapshot,
    SimilarityStreamManager, StreamingResult, SubscriptionConfig,
};
pub use vector_streaming::{
    HnswParams, IvfParams, RagChunk, RagConfig, RagContext, RagPipeline, StreamVector,
    TopicVectorStore, VectorEncoding, VectorIndexConfig, VectorIndexType, VectorSearchResult,
    VectorStoreStats, DEFAULT_VECTOR_DIM, MAX_VECTOR_DIM, MIN_VECTOR_DIM,
};
pub use persistent_store::{
    PersistentVectorStore, PersistentVectorStoreConfig, PersistentVectorStoreStats, StoredVector,
};
pub use gateway::{
    AIGateway, CostSnapshot, CostTracker, GatewayConfig, InferenceResult, InvocationCost,
    ProviderCostSummary, ProviderEntry, ProviderKind,
};

use crate::error::{Result, StreamlineError};
use std::path::PathBuf;
use std::sync::Arc;

/// AI Manager - coordinates all AI operations
pub struct AIManager {
    /// Configuration
    config: AIConfig,
    /// Embedding engine
    pub embeddings: Arc<EmbeddingEngine>,
    /// Search engine
    pub search: Arc<SearchEngine>,
    /// LLM client
    pub llm: Arc<LLMClient>,
    /// Semantic router
    pub router: Arc<SemanticRouter>,
    /// Anomaly detector
    pub anomaly: Arc<AnomalyDetector>,
    /// Pattern recognizer
    pub patterns: Arc<PatternRecognizer>,
    /// Stream summarizer
    pub summarizer: Arc<StreamSummarizer>,
    /// Persistent vector store for durable embedding storage
    pub vector_store: Option<Arc<PersistentVectorStore>>,
}

impl AIManager {
    /// Create a new AI manager
    pub fn new(config: AIConfig) -> Result<Self> {
        let embeddings = Arc::new(EmbeddingEngine::new(config.embedding.clone())?);
        let search = Arc::new(SearchEngine::new(
            config.search.clone(),
            embeddings.clone(),
        )?);
        let llm = Arc::new(LLMClient::new(config.llm.clone())?);
        let router = Arc::new(SemanticRouter::new(
            config.routing.clone(),
            embeddings.clone(),
        )?);
        let anomaly = Arc::new(AnomalyDetector::new(config.anomaly.clone())?);
        let patterns = Arc::new(PatternRecognizer::new(PatternConfig::default()));
        let summarizer = Arc::new(StreamSummarizer::new(SummarizationConfig::default()));

        Ok(Self {
            config,
            embeddings,
            search,
            llm,
            router,
            anomaly,
            patterns,
            summarizer,
            vector_store: None,
        })
    }

    /// Create a new AI manager with persistent vector storage
    pub fn with_vector_store(config: AIConfig, data_dir: PathBuf) -> Result<Self> {
        let mut manager = Self::new(config)?;

        let store_config = PersistentVectorStoreConfig {
            data_dir,
            flush_threshold: 500,
            load_on_startup: true,
            ..Default::default()
        };

        let store = PersistentVectorStore::new(store_config)?;
        manager.vector_store = Some(Arc::new(store));

        Ok(manager)
    }

    /// Load persisted vectors from disk (call on startup)
    pub async fn load_vector_store(&self) -> Result<usize> {
        match &self.vector_store {
            Some(store) => store.load().await,
            None => Ok(0),
        }
    }

    /// Flush pending vectors to disk (call on shutdown)
    pub async fn flush_vector_store(&self) -> Result<()> {
        if let Some(store) = &self.vector_store {
            store.flush().await?;
        }
        Ok(())
    }

    /// Save a full vector store snapshot
    pub async fn snapshot_vector_store(&self) -> Result<()> {
        if let Some(store) = &self.vector_store {
            store.snapshot().await?;
        }
        Ok(())
    }

    /// Get AI configuration
    pub fn config(&self) -> &AIConfig {
        &self.config
    }

    /// Check if AI features are enabled
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Process a message through the AI pipeline
    pub async fn process_message(&self, topic: &str, message: &[u8]) -> Result<ProcessingResult> {
        if !self.config.enabled {
            return Ok(ProcessingResult::default());
        }

        let text = String::from_utf8_lossy(message);
        let mut result = ProcessingResult::default();

        // Generate embedding
        if self.config.embedding.enabled {
            match self.embeddings.embed_text(&text).await {
                Ok(embedding) => {
                    // Persist embedding to vector store if available
                    if let Some(store) = &self.vector_store {
                        let id = uuid::Uuid::new_v4().to_string();
                        let vector: Vec<f32> = embedding.vector.iter().map(|&v| v as f32).collect();
                        let preview = if text.len() > 200 {
                            Some(text[..200].to_string())
                        } else {
                            Some(text.to_string())
                        };
                        if let Err(e) = store
                            .put(id, vector, Some(topic.to_string()), preview)
                            .await
                        {
                            tracing::warn!(error = %e, "Failed to persist embedding to vector store");
                        }
                    }
                    result.embedding = Some(embedding);
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Failed to generate embedding");
                }
            }
        }

        // Classify if enabled
        if self.config.llm.enabled && self.config.llm.classification_enabled {
            match self.llm.classify(&text).await {
                Ok(classification) => {
                    result.classification = Some(classification);
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Failed to classify message");
                }
            }
        }

        // Check for anomalies
        if self.config.anomaly.enabled {
            match self.anomaly.check(&text).await {
                Ok(anomaly) => {
                    if anomaly.is_anomaly {
                        result.anomaly = Some(anomaly);
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Failed to check for anomalies");
                }
            }
        }

        // Determine routing
        if self.config.routing.enabled {
            match self.router.route(topic, &text).await {
                Ok(decision) => {
                    result.routing = Some(decision);
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Failed to determine routing");
                }
            }
        }

        Ok(result)
    }

    /// Enrich a message with AI-generated metadata
    pub async fn enrich_message(&self, message: &[u8]) -> Result<EnrichmentResult> {
        if !self.config.enabled || !self.config.llm.enabled {
            return Err(StreamlineError::Config(
                "AI enrichment is not enabled".into(),
            ));
        }

        let text = String::from_utf8_lossy(message);
        self.llm.enrich(&text).await
    }

    /// Perform semantic search
    pub async fn semantic_search(&self, query: &str, limit: usize) -> Result<Vec<SearchResult>> {
        if !self.config.enabled || !self.config.search.enabled {
            return Err(StreamlineError::Config(
                "Semantic search is not enabled".into(),
            ));
        }

        self.search.search(query, limit).await
    }

    /// Search the persistent vector store by embedding similarity
    pub async fn vector_search(
        &self,
        query_text: &str,
        limit: usize,
    ) -> Result<Vec<(String, f32)>> {
        let store = self.vector_store.as_ref().ok_or_else(|| {
            StreamlineError::Config("Persistent vector store not configured".into())
        })?;

        // Generate query embedding
        let embedding = self.embeddings.embed_text(query_text).await?;
        let query_vec: Vec<f32> = embedding.vector.iter().map(|&v| v as f32).collect();

        Ok(store.search_nearest(&query_vec, limit).await)
    }

    /// Search vectors within a specific topic
    pub async fn vector_search_in_topic(
        &self,
        topic: &str,
        query_text: &str,
        limit: usize,
    ) -> Result<Vec<(String, f32)>> {
        let store = self.vector_store.as_ref().ok_or_else(|| {
            StreamlineError::Config("Persistent vector store not configured".into())
        })?;

        let embedding = self.embeddings.embed_text(query_text).await?;
        let query_vec: Vec<f32> = embedding.vector.iter().map(|&v| v as f32).collect();

        Ok(store.search_in_topic(topic, &query_vec, limit).await)
    }

    /// Get vector store statistics
    pub async fn vector_store_stats(&self) -> Option<PersistentVectorStoreStats> {
        match &self.vector_store {
            Some(store) => Some(store.stats().await),
            None => None,
        }
    }
}

/// Result of AI processing
#[derive(Debug, Clone, Default)]
pub struct ProcessingResult {
    /// Generated embedding
    pub embedding: Option<EmbeddingResult>,
    /// Classification result
    pub classification: Option<ClassificationResult>,
    /// Anomaly detection result
    pub anomaly: Option<AnomalyResult>,
    /// Routing decision
    pub routing: Option<RoutingDecision>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ai_manager_creation() {
        let config = AIConfig::default();
        let manager = AIManager::new(config);
        assert!(manager.is_ok());
    }
}
