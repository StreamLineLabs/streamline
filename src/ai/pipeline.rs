//! AI Pipeline Builder
//!
//! Provides a fluent API for building AI processing pipelines that can be
//! applied to streaming data.
//!
//! # Example
//!
//! ```rust,ignore
//! use streamline::ai::pipeline::{AIPipeline, PipelineStage};
//!
//! let pipeline = AIPipeline::builder()
//!     .name("content-analysis")
//!     .source_topic("raw-content")
//!     .add_stage(PipelineStage::Embed {
//!         model: "text-embedding-3-small".to_string(),
//!         field: "content".to_string(),
//!     })
//!     .add_stage(PipelineStage::Classify {
//!         categories: vec!["positive", "negative", "neutral"],
//!     })
//!     .add_stage(PipelineStage::DetectAnomaly {
//!         threshold: 0.8,
//!     })
//!     .sink_topic("analyzed-content")
//!     .build()?;
//! ```

use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

/// Stage in an AI pipeline
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PipelineStage {
    /// Generate embeddings for text content
    Embed {
        /// Model to use (e.g., "text-embedding-3-small", "text-embedding-ada-002")
        model: String,
        /// Field to extract text from (JSON path or "content" for raw)
        #[serde(default = "default_content_field")]
        field: String,
        /// Output field name for the embedding
        #[serde(default = "default_embedding_field")]
        output_field: String,
        /// Embedding dimension (0 = auto-detect from model)
        #[serde(default)]
        dimension: usize,
    },

    /// Classify content into categories
    Classify {
        /// Categories to classify into
        categories: Vec<String>,
        /// Model to use (optional, uses default)
        #[serde(default)]
        model: Option<String>,
        /// Output field name
        #[serde(default = "default_classification_field")]
        output_field: String,
        /// Include confidence scores
        #[serde(default = "default_true")]
        include_confidence: bool,
    },

    /// Detect anomalies in content
    DetectAnomaly {
        /// Anomaly detection threshold (0.0 - 1.0)
        #[serde(default = "default_threshold")]
        threshold: f64,
        /// Output field name
        #[serde(default = "default_anomaly_field")]
        output_field: String,
        /// Detection method
        #[serde(default)]
        method: AnomalyMethod,
    },

    /// Extract named entities
    ExtractEntities {
        /// Entity types to extract (empty = all)
        #[serde(default)]
        entity_types: Vec<String>,
        /// Output field name
        #[serde(default = "default_entities_field")]
        output_field: String,
    },

    /// Summarize content
    Summarize {
        /// Maximum summary length in words
        #[serde(default = "default_summary_length")]
        max_length: usize,
        /// Model to use (optional)
        #[serde(default)]
        model: Option<String>,
        /// Output field name
        #[serde(default = "default_summary_field")]
        output_field: String,
    },

    /// Translate content
    Translate {
        /// Target language code
        target_language: String,
        /// Source language (optional, auto-detect if not specified)
        #[serde(default)]
        source_language: Option<String>,
        /// Output field name
        #[serde(default = "default_translation_field")]
        output_field: String,
    },

    /// Generate content based on prompt template
    Generate {
        /// Prompt template (can include {{field}} placeholders)
        prompt_template: String,
        /// Model to use
        #[serde(default)]
        model: Option<String>,
        /// Output field name
        #[serde(default = "default_generated_field")]
        output_field: String,
        /// Maximum tokens to generate
        #[serde(default = "default_max_tokens")]
        max_tokens: usize,
    },

    /// Semantic search/similarity matching
    SemanticMatch {
        /// Index to search against
        index_name: String,
        /// Number of matches to return
        #[serde(default = "default_top_k")]
        top_k: usize,
        /// Minimum similarity threshold
        #[serde(default = "default_similarity_threshold")]
        min_similarity: f32,
        /// Output field name
        #[serde(default = "default_matches_field")]
        output_field: String,
    },

    /// Filter based on condition
    Filter {
        /// Condition expression (e.g., "classification == 'spam'")
        condition: String,
        /// Action when condition matches
        #[serde(default)]
        action: FilterAction,
    },

    /// Route to different topics based on content
    Route {
        /// Routing rules
        rules: Vec<RoutingRule>,
        /// Default topic if no rule matches
        #[serde(default)]
        default_topic: Option<String>,
    },

    /// Custom transformation using a user-defined function
    Custom {
        /// Name of the registered UDF
        function_name: String,
        /// Additional parameters for the function
        #[serde(default)]
        params: HashMap<String, serde_json::Value>,
    },

    /// Enrich messages with AI-generated metadata (e.g., sentiment, language, keywords)
    Enrich {
        /// Metadata fields to generate
        fields: Vec<String>,
        /// Model to use for enrichment (optional, uses default)
        #[serde(default)]
        model: Option<String>,
        /// Output field name for enrichment map
        #[serde(default = "default_enrichment_field")]
        output_field: String,
    },

    /// RAG (Retrieval-Augmented Generation) data ingestion
    ///
    /// Chunks text, generates embeddings, and stores in a vector index
    /// for retrieval by RAG-enabled LLM applications.
    RagIngest {
        /// Chunking strategy
        #[serde(default)]
        chunking: ChunkingStrategy,
        /// Maximum chunk size in tokens
        #[serde(default = "default_chunk_size")]
        chunk_size: usize,
        /// Overlap between chunks in tokens
        #[serde(default = "default_chunk_overlap")]
        chunk_overlap: usize,
        /// Embedding model for vector generation
        #[serde(default = "default_rag_model")]
        model: String,
        /// Vector index name to store embeddings
        index_name: String,
        /// Metadata fields to preserve from the source message
        #[serde(default)]
        metadata_fields: Vec<String>,
    },

    /// Real-time feature computation for ML model serving
    ///
    /// Computes features from streaming data and writes them to
    /// the feature store for online inference.
    FeatureCompute {
        /// Feature group name
        feature_group: String,
        /// Entity key field (e.g., "user_id", "session_id")
        entity_key: String,
        /// Feature definitions: field → aggregation
        features: HashMap<String, FeatureAggregation>,
        /// Window size for time-based features
        #[serde(default = "default_feature_window")]
        window_ms: u64,
    },
}

/// Chunking strategy for RAG ingestion
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChunkingStrategy {
    /// Fixed-size chunks with overlap
    #[default]
    FixedSize,
    /// Split on sentence boundaries
    Sentence,
    /// Split on paragraph boundaries
    Paragraph,
    /// Recursive splitting (try paragraphs, then sentences, then fixed)
    Recursive,
}

/// Feature aggregation type for ML feature computation
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeatureAggregation {
    /// Count of events
    Count,
    /// Sum of values
    Sum,
    /// Average of values
    Average,
    /// Minimum value
    Min,
    /// Maximum value
    Max,
    /// Most recent value
    LastValue,
    /// Distinct count
    DistinctCount,
}

fn default_chunk_size() -> usize { 512 }
fn default_chunk_overlap() -> usize { 64 }
fn default_rag_model() -> String { "text-embedding-3-small".to_string() }
fn default_feature_window() -> u64 { 300_000 } // 5 minutes

fn default_content_field() -> String {
    "content".to_string()
}
fn default_embedding_field() -> String {
    "embedding".to_string()
}
fn default_classification_field() -> String {
    "classification".to_string()
}
fn default_anomaly_field() -> String {
    "anomaly".to_string()
}
fn default_entities_field() -> String {
    "entities".to_string()
}
fn default_summary_field() -> String {
    "summary".to_string()
}
fn default_translation_field() -> String {
    "translation".to_string()
}
fn default_generated_field() -> String {
    "generated".to_string()
}
fn default_matches_field() -> String {
    "matches".to_string()
}
fn default_enrichment_field() -> String {
    "enrichment".to_string()
}
fn default_true() -> bool {
    true
}
fn default_threshold() -> f64 {
    0.8
}
fn default_summary_length() -> usize {
    100
}
fn default_max_tokens() -> usize {
    256
}
fn default_top_k() -> usize {
    10
}
fn default_similarity_threshold() -> f32 {
    0.7
}

/// Anomaly detection method
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AnomalyMethod {
    #[default]
    Statistical,
    IsolationForest,
    LocalOutlierFactor,
    Autoencoder,
}

/// Action when filter condition matches
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterAction {
    /// Drop the record
    #[default]
    Drop,
    /// Tag the record
    Tag { tag: String },
    /// Route to a different topic
    Route { topic: String },
}

/// Routing rule for content-based routing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingRule {
    /// Condition expression
    pub condition: String,
    /// Target topic
    pub topic: String,
}

/// AI pipeline configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineConfig {
    /// Pipeline name
    pub name: String,
    /// Source topic(s)
    pub source_topics: Vec<String>,
    /// Processing stages
    pub stages: Vec<PipelineStage>,
    /// Sink topic (optional, uses source if not specified)
    #[serde(default)]
    pub sink_topic: Option<String>,
    /// Error handling strategy
    #[serde(default)]
    pub on_error: ErrorStrategy,
    /// Maximum concurrent processing tasks
    #[serde(default = "default_concurrency")]
    pub max_concurrency: usize,
    /// Batch size for processing
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Whether the pipeline is enabled
    #[serde(default = "default_true")]
    pub enabled: bool,
}

fn default_concurrency() -> usize {
    4
}
fn default_batch_size() -> usize {
    10
}

/// Error handling strategy
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorStrategy {
    /// Skip failed records
    #[default]
    Skip,
    /// Send to dead-letter queue
    DeadLetterQueue { topic: String },
    /// Retry with backoff
    Retry { max_retries: u32 },
    /// Fail the pipeline
    Fail,
}

impl PipelineConfig {
    /// Create a builder for pipeline configuration
    pub fn builder() -> PipelineBuilder {
        PipelineBuilder::new()
    }
}

/// Builder for AI pipelines
#[derive(Debug, Default)]
pub struct PipelineBuilder {
    name: Option<String>,
    source_topics: Vec<String>,
    stages: Vec<PipelineStage>,
    sink_topic: Option<String>,
    on_error: ErrorStrategy,
    max_concurrency: usize,
    batch_size: usize,
    enabled: bool,
}

impl PipelineBuilder {
    pub fn new() -> Self {
        Self {
            max_concurrency: default_concurrency(),
            batch_size: default_batch_size(),
            enabled: true,
            ..Default::default()
        }
    }

    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub fn source_topic(mut self, topic: impl Into<String>) -> Self {
        self.source_topics.push(topic.into());
        self
    }

    pub fn source_topics(mut self, topics: Vec<String>) -> Self {
        self.source_topics = topics;
        self
    }

    pub fn add_stage(mut self, stage: PipelineStage) -> Self {
        self.stages.push(stage);
        self
    }

    /// Add an embedding stage
    pub fn embed(self, model: impl Into<String>) -> Self {
        self.add_stage(PipelineStage::Embed {
            model: model.into(),
            field: default_content_field(),
            output_field: default_embedding_field(),
            dimension: 0,
        })
    }

    /// Add a classification stage
    pub fn classify(self, categories: Vec<&str>) -> Self {
        self.add_stage(PipelineStage::Classify {
            categories: categories.into_iter().map(String::from).collect(),
            model: None,
            output_field: default_classification_field(),
            include_confidence: true,
        })
    }

    /// Add an anomaly detection stage
    pub fn detect_anomaly(self, threshold: f64) -> Self {
        self.add_stage(PipelineStage::DetectAnomaly {
            threshold,
            output_field: default_anomaly_field(),
            method: AnomalyMethod::default(),
        })
    }

    /// Add a summarization stage
    pub fn summarize(self, max_length: usize) -> Self {
        self.add_stage(PipelineStage::Summarize {
            max_length,
            model: None,
            output_field: default_summary_field(),
        })
    }

    /// Add an entity extraction stage
    pub fn extract_entities(self) -> Self {
        self.add_stage(PipelineStage::ExtractEntities {
            entity_types: vec![],
            output_field: default_entities_field(),
        })
    }

    /// Add an enrichment stage
    pub fn enrich(self, fields: Vec<&str>) -> Self {
        self.add_stage(PipelineStage::Enrich {
            fields: fields.into_iter().map(String::from).collect(),
            model: None,
            output_field: default_enrichment_field(),
        })
    }

    pub fn sink_topic(mut self, topic: impl Into<String>) -> Self {
        self.sink_topic = Some(topic.into());
        self
    }

    pub fn on_error(mut self, strategy: ErrorStrategy) -> Self {
        self.on_error = strategy;
        self
    }

    pub fn max_concurrency(mut self, concurrency: usize) -> Self {
        self.max_concurrency = concurrency;
        self
    }

    pub fn batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    pub fn build(self) -> Result<PipelineConfig> {
        let name = self
            .name
            .ok_or_else(|| StreamlineError::config("pipeline", "name is required"))?;

        if self.source_topics.is_empty() {
            return Err(StreamlineError::config(
                "pipeline",
                "at least one source topic is required",
            ));
        }

        if self.stages.is_empty() {
            return Err(StreamlineError::config(
                "pipeline",
                "at least one processing stage is required",
            ));
        }

        Ok(PipelineConfig {
            name,
            source_topics: self.source_topics,
            stages: self.stages,
            sink_topic: self.sink_topic,
            on_error: self.on_error,
            max_concurrency: self.max_concurrency,
            batch_size: self.batch_size,
            enabled: self.enabled,
        })
    }
}

/// Runtime metrics for a pipeline
#[derive(Debug, Default)]
pub struct PipelineMetrics {
    /// Records processed
    pub records_processed: AtomicU64,
    /// Records failed
    pub records_failed: AtomicU64,
    /// Records skipped (due to filtering)
    pub records_skipped: AtomicU64,
    /// Total processing time (microseconds)
    pub processing_time_us: AtomicU64,
    /// Embeddings generated
    pub embeddings_generated: AtomicU64,
    /// Classifications performed
    pub classifications_performed: AtomicU64,
    /// Anomalies detected
    pub anomalies_detected: AtomicU64,
}

impl PipelineMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_processed(&self) {
        self.records_processed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_failed(&self) {
        self.records_failed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_skipped(&self) {
        self.records_skipped.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_processing_time(&self, micros: u64) {
        self.processing_time_us.fetch_add(micros, Ordering::Relaxed);
    }

    pub fn record_embedding(&self) {
        self.embeddings_generated.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_classification(&self) {
        self.classifications_performed
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_anomaly(&self) {
        self.anomalies_detected.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> PipelineMetricsSnapshot {
        let processed = self.records_processed.load(Ordering::Relaxed);
        let time_us = self.processing_time_us.load(Ordering::Relaxed);

        PipelineMetricsSnapshot {
            records_processed: processed,
            records_failed: self.records_failed.load(Ordering::Relaxed),
            records_skipped: self.records_skipped.load(Ordering::Relaxed),
            avg_processing_time_us: if processed > 0 {
                time_us / processed
            } else {
                0
            },
            embeddings_generated: self.embeddings_generated.load(Ordering::Relaxed),
            classifications_performed: self.classifications_performed.load(Ordering::Relaxed),
            anomalies_detected: self.anomalies_detected.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of pipeline metrics for serialization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineMetricsSnapshot {
    pub records_processed: u64,
    pub records_failed: u64,
    pub records_skipped: u64,
    pub avg_processing_time_us: u64,
    pub embeddings_generated: u64,
    pub classifications_performed: u64,
    pub anomalies_detected: u64,
}

/// Pipeline state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PipelineState {
    /// Pipeline is being created
    Creating,
    /// Pipeline is running
    Running,
    /// Pipeline is paused
    Paused,
    /// Pipeline has stopped
    Stopped,
    /// Pipeline encountered an error
    Failed,
}

/// AI pipeline runtime
pub struct AIPipeline {
    /// Configuration
    pub config: PipelineConfig,
    /// Current state
    state: PipelineState,
    /// Runtime metrics
    metrics: Arc<PipelineMetrics>,
}

impl AIPipeline {
    /// Create a new pipeline
    pub fn new(config: PipelineConfig) -> Self {
        Self {
            config,
            state: PipelineState::Creating,
            metrics: Arc::new(PipelineMetrics::new()),
        }
    }

    /// Create a builder for pipelines
    pub fn builder() -> PipelineBuilder {
        PipelineBuilder::new()
    }

    /// Get current state
    pub fn state(&self) -> PipelineState {
        self.state
    }

    /// Start the pipeline
    pub fn start(&mut self) {
        self.state = PipelineState::Running;
        info!(name = %self.config.name, "AI pipeline started");
    }

    /// Pause the pipeline
    pub fn pause(&mut self) {
        self.state = PipelineState::Paused;
        info!(name = %self.config.name, "AI pipeline paused");
    }

    /// Resume the pipeline
    pub fn resume(&mut self) {
        if self.state == PipelineState::Paused {
            self.state = PipelineState::Running;
            info!(name = %self.config.name, "AI pipeline resumed");
        }
    }

    /// Stop the pipeline
    pub fn stop(&mut self) {
        self.state = PipelineState::Stopped;
        info!(name = %self.config.name, "AI pipeline stopped");
    }

    /// Get metrics
    pub fn metrics(&self) -> &Arc<PipelineMetrics> {
        &self.metrics
    }

    /// Get pipeline info
    pub fn info(&self) -> PipelineInfo {
        PipelineInfo {
            name: self.config.name.clone(),
            source_topics: self.config.source_topics.clone(),
            sink_topic: self.config.sink_topic.clone(),
            stage_count: self.config.stages.len(),
            state: self.state,
            metrics: self.metrics.snapshot(),
        }
    }
}

/// Pipeline information for API responses
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineInfo {
    pub name: String,
    pub source_topics: Vec<String>,
    pub sink_topic: Option<String>,
    pub stage_count: usize,
    pub state: PipelineState,
    pub metrics: PipelineMetricsSnapshot,
}

/// Manager for AI pipelines
pub struct PipelineManager {
    /// Active pipelines
    pipelines: RwLock<HashMap<String, AIPipeline>>,
}

impl PipelineManager {
    /// Create a new pipeline manager
    pub fn new() -> Self {
        Self {
            pipelines: RwLock::new(HashMap::new()),
        }
    }

    /// Create as Arc
    pub fn new_shared() -> Arc<Self> {
        Arc::new(Self::new())
    }

    /// Create a new pipeline
    pub async fn create(&self, config: PipelineConfig) -> Result<PipelineInfo> {
        let name = config.name.clone();

        {
            let pipelines = self.pipelines.read().await;
            if pipelines.contains_key(&name) {
                return Err(StreamlineError::config(
                    "pipeline",
                    format!("Pipeline '{}' already exists", name),
                ));
            }
        }

        let mut pipeline = AIPipeline::new(config);
        if pipeline.config.enabled {
            pipeline.start();
        }

        let info = pipeline.info();

        {
            let mut pipelines = self.pipelines.write().await;
            pipelines.insert(name.clone(), pipeline);
        }

        info!(name = %name, "Created AI pipeline");
        Ok(info)
    }

    /// Get a pipeline by name
    pub async fn get(&self, name: &str) -> Option<PipelineInfo> {
        let pipelines = self.pipelines.read().await;
        pipelines.get(name).map(|p| p.info())
    }

    /// List all pipelines
    pub async fn list(&self) -> Vec<PipelineInfo> {
        let pipelines = self.pipelines.read().await;
        pipelines.values().map(|p| p.info()).collect()
    }

    /// Start a pipeline
    pub async fn start(&self, name: &str) -> Result<PipelineInfo> {
        let mut pipelines = self.pipelines.write().await;
        let pipeline = pipelines.get_mut(name).ok_or_else(|| {
            StreamlineError::config("pipeline", format!("Pipeline '{}' not found", name))
        })?;
        pipeline.start();
        Ok(pipeline.info())
    }

    /// Pause a pipeline
    pub async fn pause(&self, name: &str) -> Result<PipelineInfo> {
        let mut pipelines = self.pipelines.write().await;
        let pipeline = pipelines.get_mut(name).ok_or_else(|| {
            StreamlineError::config("pipeline", format!("Pipeline '{}' not found", name))
        })?;
        pipeline.pause();
        Ok(pipeline.info())
    }

    /// Delete a pipeline
    pub async fn delete(&self, name: &str) -> Result<()> {
        let mut pipelines = self.pipelines.write().await;
        let mut pipeline = pipelines.remove(name).ok_or_else(|| {
            StreamlineError::config("pipeline", format!("Pipeline '{}' not found", name))
        })?;
        pipeline.stop();
        info!(name = %name, "Deleted AI pipeline");
        Ok(())
    }
}

impl Default for PipelineManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pipeline_builder() {
        let config = PipelineConfig::builder()
            .name("test-pipeline")
            .source_topic("input-topic")
            .embed("text-embedding-3-small")
            .classify(vec!["positive", "negative", "neutral"])
            .detect_anomaly(0.8)
            .sink_topic("output-topic")
            .build()
            .unwrap();

        assert_eq!(config.name, "test-pipeline");
        assert_eq!(config.source_topics, vec!["input-topic"]);
        assert_eq!(config.stages.len(), 3);
        assert_eq!(config.sink_topic, Some("output-topic".to_string()));
    }

    #[test]
    fn test_pipeline_builder_missing_name() {
        let result = PipelineConfig::builder()
            .source_topic("input")
            .embed("model")
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_pipeline_builder_missing_source() {
        let result = PipelineConfig::builder()
            .name("test")
            .embed("model")
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_pipeline_state_transitions() {
        let config = PipelineConfig::builder()
            .name("test")
            .source_topic("input")
            .embed("model")
            .build()
            .unwrap();

        let mut pipeline = AIPipeline::new(config);
        assert_eq!(pipeline.state(), PipelineState::Creating);

        pipeline.start();
        assert_eq!(pipeline.state(), PipelineState::Running);

        pipeline.pause();
        assert_eq!(pipeline.state(), PipelineState::Paused);

        pipeline.resume();
        assert_eq!(pipeline.state(), PipelineState::Running);

        pipeline.stop();
        assert_eq!(pipeline.state(), PipelineState::Stopped);
    }

    #[test]
    fn test_pipeline_metrics() {
        let metrics = PipelineMetrics::new();

        metrics.record_processed();
        metrics.record_processed();
        metrics.record_failed();
        metrics.record_embedding();
        metrics.record_classification();
        metrics.add_processing_time(1000);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.records_processed, 2);
        assert_eq!(snapshot.records_failed, 1);
        assert_eq!(snapshot.embeddings_generated, 1);
        assert_eq!(snapshot.classifications_performed, 1);
        assert_eq!(snapshot.avg_processing_time_us, 500);
    }

    #[tokio::test]
    async fn test_pipeline_manager() {
        let manager = PipelineManager::new();

        // Create pipeline
        let config = PipelineConfig::builder()
            .name("my-pipeline")
            .source_topic("input")
            .embed("model")
            .build()
            .unwrap();

        let info = manager.create(config).await.unwrap();
        assert_eq!(info.name, "my-pipeline");
        assert_eq!(info.state, PipelineState::Running);

        // List pipelines
        let pipelines = manager.list().await;
        assert_eq!(pipelines.len(), 1);

        // Pause pipeline
        let paused = manager.pause("my-pipeline").await.unwrap();
        assert_eq!(paused.state, PipelineState::Paused);

        // Delete pipeline
        manager.delete("my-pipeline").await.unwrap();
        assert!(manager.get("my-pipeline").await.is_none());
    }

    #[test]
    fn test_pipeline_stage_serialization() {
        let stage = PipelineStage::Embed {
            model: "text-embedding-3-small".to_string(),
            field: "content".to_string(),
            output_field: "embedding".to_string(),
            dimension: 1536,
        };

        let json = serde_json::to_string(&stage).unwrap();
        assert!(json.contains("embed"));
        assert!(json.contains("text-embedding-3-small"));

        let deserialized: PipelineStage = serde_json::from_str(&json).unwrap();
        match deserialized {
            PipelineStage::Embed {
                model, dimension, ..
            } => {
                assert_eq!(model, "text-embedding-3-small");
                assert_eq!(dimension, 1536);
            }
            _ => panic!("Expected Embed stage"),
        }
    }
}
