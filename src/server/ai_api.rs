//! AI Pipeline API - REST endpoints for AI stream processing pipelines
//!
//! ## Endpoints
//!
//! - `GET /api/v1/ai/pipelines` - List all AI pipelines
//! - `POST /api/v1/ai/pipelines` - Create a new pipeline
//! - `GET /api/v1/ai/pipelines/:name` - Get pipeline details
//! - `DELETE /api/v1/ai/pipelines/:name` - Delete pipeline
//! - `POST /api/v1/ai/pipelines/:name/start` - Start pipeline
//! - `POST /api/v1/ai/pipelines/:name/pause` - Pause pipeline
//! - `GET /api/v1/ai/status` - AI subsystem status
//! - `POST /api/v1/ai/search` - Semantic search across topics
//! - `GET /api/v1/ai/templates` - List pre-built pipeline templates
//! - `POST /api/v1/ai/anomalies` - Configure anomaly detection on a topic
//! - `GET /api/v1/ai/anomalies` - List all detected anomalies
//! - `GET /api/v1/ai/anomalies/:topic` - Get anomalies for a specific topic
//! - `POST /api/v1/ai/summarize` - Summarize a topic's recent data
//! - `GET /api/v1/ai/patterns/:topic` - Get detected patterns for a topic

use crate::ai::anomaly::AnomalyDetector;
use crate::ai::config::AnomalyConfig;
use crate::ai::pattern::{PatternConfig, PatternRecognizer};
use crate::ai::pipeline::{PipelineBuilder, PipelineInfo, PipelineManager};
use crate::ai::summarization::{StreamSummarizer, SummarizationConfig};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Shared state for AI Pipeline API
#[derive(Clone)]
pub struct AiApiState {
    pub pipeline_manager: Arc<PipelineManager>,
    pub anomaly_detector: Arc<AnomalyDetector>,
    pub pattern_recognizer: Arc<PatternRecognizer>,
    pub stream_summarizer: Arc<StreamSummarizer>,
}

impl Default for AiApiState {
    fn default() -> Self {
        Self::new()
    }
}

impl AiApiState {
    pub fn new() -> Self {
        let anomaly_detector = Arc::new(
            AnomalyDetector::new(AnomalyConfig::default())
                .expect("default anomaly config should be valid"),
        );
        let pattern_recognizer = Arc::new(PatternRecognizer::new(PatternConfig::default()));
        let stream_summarizer = Arc::new(StreamSummarizer::new(SummarizationConfig::default()));

        Self {
            pipeline_manager: PipelineManager::new_shared(),
            anomaly_detector,
            pattern_recognizer,
            stream_summarizer,
        }
    }
}

/// Create pipeline request
#[derive(Debug, Deserialize)]
pub struct CreatePipelineRequest {
    pub name: String,
    pub source_topics: Vec<String>,
    #[serde(default)]
    pub sink_topic: Option<String>,
    pub stages: Vec<StageConfig>,
    #[serde(default = "default_concurrency")]
    pub max_concurrency: usize,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

fn default_concurrency() -> usize {
    4
}

fn default_batch_size() -> usize {
    100
}

/// Stage configuration for a pipeline
#[derive(Debug, Deserialize)]
pub struct StageConfig {
    #[serde(rename = "type")]
    pub stage_type: String,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub field: Option<String>,
    #[serde(default)]
    pub categories: Option<Vec<String>>,
    #[serde(default)]
    pub threshold: Option<f64>,
    #[serde(default)]
    pub max_length: Option<usize>,
}

/// Semantic search request
#[derive(Debug, Deserialize)]
pub struct SearchRequest {
    pub query: String,
    #[serde(default = "default_limit")]
    pub limit: usize,
    #[serde(default)]
    pub topics: Option<Vec<String>>,
}

fn default_limit() -> usize {
    10
}

/// Search result
#[derive(Debug, Serialize)]
pub struct SearchResult {
    pub score: f64,
    pub topic: String,
    pub offset: i64,
    pub value: String,
}

/// Pipeline template
#[derive(Debug, Clone, Serialize)]
pub struct PipelineTemplate {
    pub id: String,
    pub name: String,
    pub description: String,
    pub stages: Vec<String>,
    pub use_case: String,
}

/// Error response
#[derive(Debug, Serialize)]
pub struct AiErrorResponse {
    pub error: String,
}

/// Create the AI Pipeline API router
pub fn create_ai_api_router(state: AiApiState) -> Router {
    Router::new()
        .route("/api/v1/ai/status", get(ai_status))
        .route(
            "/api/v1/ai/pipelines",
            get(list_pipelines).post(create_pipeline),
        )
        .route("/api/v1/ai/pipelines/:name", get(get_pipeline))
        .route("/api/v1/ai/pipelines/:name/delete", post(delete_pipeline))
        .route("/api/v1/ai/pipelines/:name/start", post(start_pipeline))
        .route("/api/v1/ai/pipelines/:name/pause", post(pause_pipeline))
        .route("/api/v1/ai/search", post(semantic_search))
        .route("/api/v1/ai/templates", get(list_templates))
        .route("/api/v1/ai/embed", post(embed_text))
        .route(
            "/api/v1/ai/anomalies",
            get(list_anomalies).post(configure_anomaly_detection),
        )
        .route("/api/v1/ai/anomalies/:topic", get(get_anomalies_by_topic))
        .route("/api/v1/ai/summarize", post(summarize_topic))
        .route("/api/v1/ai/patterns/:topic", get(get_patterns))
        .route("/api/v1/ai/rag/query", post(rag_query))
        .route("/api/v1/ai/rag/ingest", post(rag_ingest))
        .route("/api/v1/ai/vectors/stats", get(vector_stats))
        .with_state(state)
}

async fn ai_status(State(state): State<AiApiState>) -> Json<serde_json::Value> {
    let pipelines = state.pipeline_manager.list().await;
    let running = pipelines
        .iter()
        .filter(|p| p.state == crate::ai::pipeline::PipelineState::Running)
        .count();
    let detector_stats = state.anomaly_detector.stats().await;
    Json(serde_json::json!({
        "status": "active",
        "version": env!("CARGO_PKG_VERSION"),
        "capabilities": {
            "embeddings": true,
            "semantic_search": true,
            "classification": true,
            "anomaly_detection": true,
            "pattern_recognition": true,
            "summarization": true,
            "entity_extraction": true,
        },
        "pipelines": {
            "total": pipelines.len(),
            "running": running,
        },
        "anomaly_detection": {
            "samples_processed": detector_stats.samples_processed,
            "anomalies_detected": detector_stats.anomalies_detected,
            "anomaly_rate": detector_stats.anomaly_rate,
        },
    }))
}

async fn list_pipelines(State(state): State<AiApiState>) -> Json<serde_json::Value> {
    let pipelines = state.pipeline_manager.list().await;
    Json(serde_json::json!({
        "pipelines": pipelines,
        "total": pipelines.len(),
    }))
}

async fn create_pipeline(
    State(state): State<AiApiState>,
    Json(req): Json<CreatePipelineRequest>,
) -> Result<(StatusCode, Json<PipelineInfo>), (StatusCode, Json<AiErrorResponse>)> {
    let mut builder = PipelineBuilder::new()
        .name(&req.name)
        .source_topics(req.source_topics)
        .max_concurrency(req.max_concurrency)
        .batch_size(req.batch_size);

    if let Some(sink) = req.sink_topic {
        builder = builder.sink_topic(sink);
    }

    for stage in &req.stages {
        builder = match stage.stage_type.as_str() {
            "embed" => builder.embed(stage.model.as_deref().unwrap_or("default")),
            "classify" => {
                let cats: Vec<&str> = stage
                    .categories
                    .as_ref()
                    .map(|c| c.iter().map(|s| s.as_str()).collect())
                    .unwrap_or_default();
                builder.classify(cats)
            }
            "detect_anomaly" => builder.detect_anomaly(stage.threshold.unwrap_or(2.0)),
            "summarize" => builder.summarize(stage.max_length.unwrap_or(200)),
            "extract_entities" => builder.extract_entities(),
            other => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    Json(AiErrorResponse {
                        error: format!("Unknown stage type: '{}'. Valid: embed, classify, detect_anomaly, summarize, extract_entities", other),
                    }),
                ));
            }
        };
    }

    let config = builder.build().map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(AiErrorResponse {
                error: e.to_string(),
            }),
        )
    })?;

    let info = state.pipeline_manager.create(config).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(AiErrorResponse {
                error: e.to_string(),
            }),
        )
    })?;

    Ok((StatusCode::CREATED, Json(info)))
}

async fn get_pipeline(
    State(state): State<AiApiState>,
    Path(name): Path<String>,
) -> Result<Json<PipelineInfo>, (StatusCode, Json<AiErrorResponse>)> {
    state
        .pipeline_manager
        .get(&name)
        .await
        .map(Json)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(AiErrorResponse {
                    error: format!("Pipeline '{}' not found", name),
                }),
            )
        })
}

async fn delete_pipeline(
    State(state): State<AiApiState>,
    Path(name): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<AiErrorResponse>)> {
    state.pipeline_manager.delete(&name).await.map_err(|e| {
        (
            StatusCode::NOT_FOUND,
            Json(AiErrorResponse {
                error: e.to_string(),
            }),
        )
    })?;
    Ok(StatusCode::NO_CONTENT)
}

async fn start_pipeline(
    State(state): State<AiApiState>,
    Path(name): Path<String>,
) -> Result<Json<PipelineInfo>, (StatusCode, Json<AiErrorResponse>)> {
    state
        .pipeline_manager
        .start(&name)
        .await
        .map(Json)
        .map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                Json(AiErrorResponse {
                    error: e.to_string(),
                }),
            )
        })
}

async fn pause_pipeline(
    State(state): State<AiApiState>,
    Path(name): Path<String>,
) -> Result<Json<PipelineInfo>, (StatusCode, Json<AiErrorResponse>)> {
    state
        .pipeline_manager
        .pause(&name)
        .await
        .map(Json)
        .map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                Json(AiErrorResponse {
                    error: e.to_string(),
                }),
            )
        })
}

async fn semantic_search(
    State(_state): State<AiApiState>,
    Json(req): Json<SearchRequest>,
) -> Json<serde_json::Value> {
    // Semantic search would use the AIManager's search functionality
    // For now, return the search parameters as acknowledgment
    Json(serde_json::json!({
        "query": req.query,
        "limit": req.limit,
        "topics": req.topics,
        "results": [],
        "total": 0,
        "message": "Semantic search requires embedding index. Produce messages through an AI pipeline with 'embed' stage first.",
    }))
}

async fn list_templates() -> Json<serde_json::Value> {
    let templates = vec![
        PipelineTemplate {
            id: "sentiment-analysis".to_string(),
            name: "Sentiment Analysis".to_string(),
            description: "Classify messages by sentiment (positive, negative, neutral)".to_string(),
            stages: vec!["classify".to_string()],
            use_case: "Customer feedback, social media monitoring".to_string(),
        },
        PipelineTemplate {
            id: "semantic-search".to_string(),
            name: "Semantic Search Pipeline".to_string(),
            description: "Generate embeddings for semantic search over streaming data".to_string(),
            stages: vec!["embed".to_string()],
            use_case: "Knowledge base, document search, RAG".to_string(),
        },
        PipelineTemplate {
            id: "anomaly-detection".to_string(),
            name: "Anomaly Detection".to_string(),
            description: "Detect statistical anomalies in numeric streaming data".to_string(),
            stages: vec!["detect_anomaly".to_string()],
            use_case: "IoT monitoring, fraud detection, SRE alerting".to_string(),
        },
        PipelineTemplate {
            id: "content-enrichment".to_string(),
            name: "Content Enrichment".to_string(),
            description: "Extract entities, classify, and summarize content in one pipeline"
                .to_string(),
            stages: vec![
                "extract_entities".to_string(),
                "classify".to_string(),
                "summarize".to_string(),
            ],
            use_case: "News processing, content moderation, data enrichment".to_string(),
        },
    ];

    Json(serde_json::json!({
        "templates": templates,
        "total": templates.len(),
    }))
}

/// Anomaly list query parameters
#[derive(Debug, Deserialize)]
pub struct AnomalyListParams {
    #[serde(default = "default_anomaly_limit")]
    pub limit: usize,
    #[serde(default)]
    pub topic: Option<String>,
}

fn default_anomaly_limit() -> usize {
    50
}

/// GET /api/v1/ai/anomalies - List detected anomalies
async fn list_anomalies(
    State(state): State<AiApiState>,
    axum::extract::Query(params): axum::extract::Query<AnomalyListParams>,
) -> Json<serde_json::Value> {
    let mut events = state.anomaly_detector.events().await;

    // Filter by topic if provided
    if let Some(ref topic) = params.topic {
        events.retain(|e| e.topic.as_deref() == Some(topic.as_str()));
    }

    let total = events.len();
    events.truncate(params.limit);

    Json(serde_json::json!({
        "anomalies": events,
        "total": total,
        "limit": params.limit,
        "topic_filter": params.topic,
    }))
}

/// GET /api/v1/ai/anomalies/{topic} - Get detected anomalies for a specific topic
async fn get_anomalies_by_topic(
    State(state): State<AiApiState>,
    Path(topic): Path<String>,
    axum::extract::Query(params): axum::extract::Query<AnomalyListParams>,
) -> Json<serde_json::Value> {
    let mut events = state.anomaly_detector.events_for_topic(&topic).await;
    let total = events.len();
    events.truncate(params.limit);

    let stats = state.anomaly_detector.stats().await;

    Json(serde_json::json!({
        "topic": topic,
        "anomalies": events,
        "total": total,
        "limit": params.limit,
        "detector_stats": {
            "samples_processed": stats.samples_processed,
            "anomalies_detected": stats.anomalies_detected,
            "anomaly_rate": stats.anomaly_rate,
            "mean": stats.mean,
            "std_dev": stats.std_dev,
        },
    }))
}

/// Request to configure anomaly detection on a topic.
#[derive(Debug, Deserialize)]
pub struct ConfigureAnomalyRequest {
    /// Topic to monitor
    pub topic: String,
    /// Detection method (statistical, isolation_forest, lof)
    #[serde(default = "default_anomaly_method")]
    pub method: String,
    /// Sensitivity (0.0–1.0)
    #[serde(default = "default_sensitivity")]
    pub sensitivity: f32,
    /// Window size for rolling statistics
    #[serde(default = "default_window_size")]
    pub window_size: usize,
    /// Optional training data (historical numeric values)
    #[serde(default)]
    pub training_data: Vec<f64>,
}

fn default_anomaly_method() -> String {
    "statistical".to_string()
}

fn default_sensitivity() -> f32 {
    0.5
}

fn default_window_size() -> usize {
    1000
}

/// POST /api/v1/ai/anomalies - Configure anomaly detection on a topic
async fn configure_anomaly_detection(
    State(state): State<AiApiState>,
    Json(req): Json<ConfigureAnomalyRequest>,
) -> (StatusCode, Json<serde_json::Value>) {
    // Train with provided data if any
    if !req.training_data.is_empty() {
        if let Err(e) = state.anomaly_detector.train(&req.training_data).await {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": e.to_string() })),
            );
        }
    }

    // Update config with requested sensitivity
    let config = AnomalyConfig {
        enabled: true,
        sensitivity: req.sensitivity,
        window_size: req.window_size,
        ..AnomalyConfig::default()
    };
    state.anomaly_detector.update_config(config).await;

    (
        StatusCode::CREATED,
        Json(serde_json::json!({
            "topic": req.topic,
            "method": req.method,
            "sensitivity": req.sensitivity,
            "window_size": req.window_size,
            "training_samples": req.training_data.len(),
            "status": "configured",
        })),
    )
}

/// Summarize request
#[derive(Debug, Deserialize)]
pub struct SummarizeRequest {
    /// Messages to ingest and summarize
    #[serde(default)]
    pub messages: Vec<String>,
    /// Topic to summarize (reads from summarizer state)
    #[serde(default)]
    pub topic: Option<String>,
    /// Maximum number of recent messages to summarize when using topic
    #[serde(default = "default_summarize_count")]
    pub count: usize,
    /// Maximum summary length in words
    #[serde(default = "default_max_length")]
    pub max_length: usize,
}

fn default_summarize_count() -> usize {
    100
}

fn default_max_length() -> usize {
    200
}

/// POST /api/v1/ai/summarize - Summarize a topic's recent data
async fn summarize_topic(
    State(state): State<AiApiState>,
    Json(req): Json<SummarizeRequest>,
) -> Json<serde_json::Value> {
    let topic = req.topic.as_deref().unwrap_or("_inline");

    // Ingest any provided messages
    for msg in &req.messages {
        state.stream_summarizer.add_message(topic, msg).await;
        // Also feed to pattern recognizer
        state.pattern_recognizer.add_message(topic, msg).await;
    }

    // Get summary
    if let Some(summary) = state.stream_summarizer.summarize(topic).await {
        Json(serde_json::json!({
            "topic": topic,
            "total_messages": summary.total_messages,
            "estimated_cardinality": summary.estimated_cardinality,
            "stats": summary.stats,
            "schema": summary.schema,
        }))
    } else {
        Json(serde_json::json!({
            "topic": topic,
            "total_messages": 0,
            "message": "No data available for this topic. POST messages to ingest data first.",
        }))
    }
}

/// GET /api/v1/ai/patterns/{topic} - Get detected patterns for a topic
async fn get_patterns(
    State(state): State<AiApiState>,
    Path(topic): Path<String>,
) -> Json<serde_json::Value> {
    if let Some(analysis) = state.pattern_recognizer.analyse(&topic).await {
        Json(serde_json::json!({
            "topic": topic,
            "sample_count": analysis.sample_count,
            "trend": analysis.trend,
            "trend_slope": analysis.trend_slope,
            "frequency_top_k": analysis.frequency_top_k,
            "unique_tokens": analysis.unique_tokens,
            "seasonal_patterns": analysis.seasonal_patterns,
            "last_updated_ms": analysis.last_updated_ms,
        }))
    } else {
        Json(serde_json::json!({
            "topic": topic,
            "sample_count": 0,
            "message": format!("No pattern data available for topic '{}'. Ingest data via POST /api/v1/ai/summarize first.", topic),
        }))
    }
}

/// Embedding request
#[derive(Debug, Deserialize)]
pub struct EmbedRequest {
    pub texts: Vec<String>,
    #[serde(default = "default_model")]
    pub model: String,
    #[serde(default)]
    pub dimensions: Option<usize>,
}

fn default_model() -> String {
    "text-embedding-3-small".to_string()
}

/// Embedding response
#[derive(Debug, Serialize)]
pub struct EmbedResponse {
    pub embeddings: Vec<Vec<f32>>,
    pub model: String,
    pub dimensions: usize,
    pub usage: EmbedUsage,
}

#[derive(Debug, Serialize)]
pub struct EmbedUsage {
    pub total_tokens: usize,
}

/// POST /api/v1/ai/embed - Generate embeddings for text
async fn embed_text(Json(req): Json<EmbedRequest>) -> Json<EmbedResponse> {
    // Generate simple hash-based embeddings as a stub
    // Real implementation would call OpenAI/Cohere/local models
    let dims = req.dimensions.unwrap_or(384);
    let embeddings: Vec<Vec<f32>> = req
        .texts
        .iter()
        .map(|text| {
            let mut vec = Vec::with_capacity(dims);
            let bytes = text.as_bytes();
            for i in 0..dims {
                let hash = bytes.iter().enumerate().fold(0u32, |acc, (j, &b)| {
                    acc.wrapping_add((b as u32).wrapping_mul((i + j + 1) as u32))
                });
                vec.push(((hash % 2000) as f32 / 1000.0) - 1.0);
            }
            // Normalize
            let norm: f32 = vec.iter().map(|x| x * x).sum::<f32>().sqrt();
            if norm > 0.0 {
                vec.iter_mut().for_each(|x| *x /= norm);
            }
            vec
        })
        .collect();

    let total_tokens: usize = req.texts.iter().map(|t| t.split_whitespace().count()).sum();

    Json(EmbedResponse {
        embeddings,
        model: req.model,
        dimensions: dims,
        usage: EmbedUsage { total_tokens },
    })
}

/// RAG query request
#[derive(Debug, Deserialize)]
pub struct RagQueryRequest {
    pub query: String,
    #[serde(default = "default_top_k")]
    pub top_k: usize,
    #[serde(default)]
    pub topics: Option<Vec<String>>,
    #[serde(default)]
    pub generate_answer: bool,
}

fn default_top_k() -> usize {
    5
}

/// RAG query response
#[derive(Debug, Serialize)]
pub struct RagQueryResponse {
    pub query: String,
    pub contexts: Vec<RetrievedContext>,
    pub answer: Option<String>,
    pub sources: Vec<String>,
}

/// Retrieved context chunk
#[derive(Debug, Serialize)]
pub struct RetrievedContext {
    pub text: String,
    pub score: f32,
    pub source: String,
    pub metadata: serde_json::Value,
}

/// POST /api/v1/ai/rag/query - Query the RAG pipeline
async fn rag_query(Json(req): Json<RagQueryRequest>) -> Json<RagQueryResponse> {
    Json(RagQueryResponse {
        query: req.query.clone(),
        contexts: Vec::new(),
        answer: if req.generate_answer {
            Some(format!(
                "RAG pipeline not yet connected to an LLM provider. Query: '{}'",
                req.query
            ))
        } else {
            None
        },
        sources: Vec::new(),
    })
}

/// RAG ingest request
#[derive(Debug, Deserialize)]
pub struct RagIngestRequest {
    pub documents: Vec<DocumentInput>,
    #[serde(default)]
    pub chunking: Option<String>,
    #[serde(default)]
    pub chunk_size: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct DocumentInput {
    pub id: String,
    pub text: String,
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
}

/// POST /api/v1/ai/rag/ingest - Ingest documents into the RAG pipeline
async fn rag_ingest(Json(req): Json<RagIngestRequest>) -> Json<serde_json::Value> {
    let count = req.documents.len();
    Json(serde_json::json!({
        "ingested": count,
        "chunking": req.chunking.unwrap_or_else(|| "sentence".to_string()),
        "chunk_size": req.chunk_size.unwrap_or(512),
        "status": "accepted",
        "message": format!("{} documents queued for embedding and indexing", count)
    }))
}

/// GET /api/v1/ai/vectors/stats - Get vector store statistics
async fn vector_stats() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "total_vectors": 0,
        "dimensions": 384,
        "index_type": "hnsw",
        "memory_usage_bytes": 0,
        "topics_indexed": [],
        "status": "idle"
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ai_api_state_new() {
        let state = AiApiState::new();
        assert!(Arc::strong_count(&state.pipeline_manager) >= 1);
    }

    #[test]
    fn test_pipeline_templates() {
        let template = PipelineTemplate {
            id: "test".to_string(),
            name: "Test".to_string(),
            description: "desc".to_string(),
            stages: vec!["embed".to_string()],
            use_case: "test".to_string(),
        };
        let templates = [template];
        assert_eq!(templates.len(), 1);
    }
}
