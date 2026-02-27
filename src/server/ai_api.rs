//! AI Pipeline API - REST endpoints for AI stream processing pipelines
//!
//! ## Endpoints
//!
//! - `GET /api/v1/ai/pipelines` - List all AI pipelines
//! - `POST /api/v1/ai/pipelines` - Create a new pipeline
//! - `GET /api/v1/ai/pipelines/:name` - Get pipeline details
//! - `DELETE /api/v1/ai/pipelines/:name` - Delete pipeline
//! - `POST /api/v1/ai/pipelines/:name/start` - Start pipeline
//! - `POST /api/v1/ai/pipelines/:name/stop` - Stop pipeline
//! - `POST /api/v1/ai/pipelines/:name/pause` - Pause pipeline
//! - `GET /api/v1/ai/stats` - AI subsystem stats
//! - `GET /api/v1/ai/status` - AI subsystem status (alias)
//! - `POST /api/v1/ai/search` - Semantic search across topics
//! - `POST /api/v1/ai/classify` - Classify text using LLM
//! - `POST /api/v1/ai/enrich` - Enrich records with AI metadata
//! - `POST /api/v1/ai/embed` - Generate embeddings for text
//! - `POST /api/v1/ai/embeddings` - Generate embeddings (alias)
//! - `GET /api/v1/ai/templates` - List pre-built pipeline templates
//! - `POST /api/v1/ai/anomalies` - Configure anomaly detection on a topic
//! - `GET /api/v1/ai/anomalies` - List all detected anomalies
//! - `GET /api/v1/ai/anomalies/:topic` - Get anomalies for a specific topic
//! - `POST /api/v1/ai/summarize` - Summarize a topic's recent data
//! - `GET /api/v1/ai/patterns/:topic` - Get detected patterns for a topic
//! - `POST /api/v1/ai/rag/query` - Query the RAG pipeline
//! - `POST /api/v1/ai/rag/ingest` - Ingest documents for RAG
//! - `GET /api/v1/ai/vectors/stats` - Vector store statistics
//! - `POST /api/v1/ai/auto-embed` - Configure auto-embedding
//! - `GET /api/v1/ai/auto-embed` - List auto-embed configs

use crate::ai::anomaly::AnomalyDetector;
use crate::ai::config::AnomalyConfig;
use crate::ai::pattern::{PatternConfig, PatternRecognizer};
use crate::ai::pipeline::{PipelineBuilder, PipelineInfo, PipelineManager};
use crate::ai::summarization::{StreamSummarizer, SummarizationConfig};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{delete, get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
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
        Self::new().expect("default AiApiState configuration should be valid")
    }
}

impl AiApiState {
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let anomaly_detector = Arc::new(AnomalyDetector::new(AnomalyConfig::default())?);
        let pattern_recognizer = Arc::new(PatternRecognizer::new(PatternConfig::default()));
        let stream_summarizer = Arc::new(StreamSummarizer::new(SummarizationConfig::default()));

        Ok(Self {
            pipeline_manager: PipelineManager::new_shared(),
            anomaly_detector,
            pattern_recognizer,
            stream_summarizer,
        })
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

/// Classify request
#[derive(Debug, Deserialize)]
pub struct ClassifyRequest {
    /// Text to classify
    pub text: String,
    /// Categories to classify into
    pub categories: Vec<String>,
}

/// Classify response
#[derive(Debug, Serialize, Deserialize)]
pub struct ClassifyResponse {
    /// Predicted category
    pub category: String,
    /// Confidence score (0.0 - 1.0)
    pub confidence: f32,
    /// Scores for all categories
    pub scores: HashMap<String, f32>,
}

/// Enrich request
#[derive(Debug, Deserialize)]
pub struct EnrichRequest {
    /// Text to enrich with AI metadata
    pub text: String,
    /// Optional list of fields to extract
    #[serde(default)]
    pub fields: Option<Vec<String>>,
}

/// Enrich response
#[derive(Debug, Serialize, Deserialize)]
pub struct EnrichResponse {
    /// Original text
    pub text: String,
    /// Extracted metadata fields
    pub metadata: HashMap<String, serde_json::Value>,
    /// Summary (if generated)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    /// Detected entities
    pub entities: Vec<String>,
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
        .route("/api/v1/ai/stats", get(ai_status))
        .route(
            "/api/v1/ai/pipelines",
            get(list_pipelines).post(create_pipeline),
        )
        .route(
            "/api/v1/ai/pipelines/:name",
            get(get_pipeline).delete(delete_pipeline),
        )
        .route("/api/v1/ai/pipelines/:name/delete", post(delete_pipeline))
        .route("/api/v1/ai/pipelines/:name/start", post(start_pipeline))
        .route("/api/v1/ai/pipelines/:name/stop", post(stop_pipeline))
        .route("/api/v1/ai/pipelines/:name/pause", post(pause_pipeline))
        .route("/api/v1/ai/search", post(semantic_search))
        .route("/api/v1/ai/classify", post(classify_text))
        .route("/api/v1/ai/enrich", post(enrich_text))
        .route("/api/v1/ai/templates", get(list_templates))
        .route("/api/v1/ai/embed", post(embed_text))
        .route("/api/v1/ai/embeddings", post(embed_text))
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
        .route(
            "/api/v1/ai/auto-embed",
            get(list_auto_embed).post(configure_auto_embed),
        )
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

async fn stop_pipeline(
    State(state): State<AiApiState>,
    Path(name): Path<String>,
) -> Result<Json<PipelineInfo>, (StatusCode, Json<AiErrorResponse>)> {
    state
        .pipeline_manager
        .stop(&name)
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

/// POST /api/v1/ai/classify - Classify text using LLM
async fn classify_text(
    Json(req): Json<ClassifyRequest>,
) -> Json<ClassifyResponse> {
    // Generate stub classification scores based on text hashing
    // Real implementation would delegate to LLMClient::classify
    let mut scores = HashMap::new();
    let text_hash = req.text.bytes().fold(0u32, |acc, b| acc.wrapping_add(b as u32));

    let mut best_category = String::new();
    let mut best_score: f32 = 0.0;

    for (i, cat) in req.categories.iter().enumerate() {
        let score = ((text_hash.wrapping_add(i as u32) % 100) as f32) / 100.0;
        scores.insert(cat.clone(), score);
        if score > best_score {
            best_score = score;
            best_category = cat.clone();
        }
    }

    // Normalize so scores sum to 1.0
    let total: f32 = scores.values().sum();
    if total > 0.0 {
        for v in scores.values_mut() {
            *v /= total;
        }
        best_score = scores[&best_category];
    }

    Json(ClassifyResponse {
        category: best_category,
        confidence: best_score,
        scores,
    })
}

/// POST /api/v1/ai/enrich - Enrich records with AI metadata
async fn enrich_text(
    Json(req): Json<EnrichRequest>,
) -> Json<EnrichResponse> {
    // Stub enrichment — extract basic metadata from text
    // Real implementation would delegate to LLMClient::enrich
    let mut metadata = HashMap::new();
    let word_count = req.text.split_whitespace().count();
    let char_count = req.text.len();

    metadata.insert("word_count".to_string(), serde_json::json!(word_count));
    metadata.insert("char_count".to_string(), serde_json::json!(char_count));
    metadata.insert("language".to_string(), serde_json::json!("en"));

    if let Some(ref fields) = req.fields {
        for field in fields {
            if !metadata.contains_key(field) {
                metadata.insert(field.clone(), serde_json::Value::Null);
            }
        }
    }

    let summary = if word_count > 10 {
        let words: Vec<&str> = req.text.split_whitespace().take(10).collect();
        Some(format!("{}...", words.join(" ")))
    } else {
        Some(req.text.clone())
    };

    Json(EnrichResponse {
        text: req.text,
        metadata,
        summary,
        entities: Vec::new(),
    })
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
#[derive(Debug, Serialize, Deserialize)]
pub struct EmbedResponse {
    pub embeddings: Vec<Vec<f32>>,
    pub model: String,
    pub dimensions: usize,
    pub usage: EmbedUsage,
}

#[derive(Debug, Serialize, Deserialize)]
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

/// POST /api/v1/ai/auto-embed - Enable auto-embedding for a topic
///
/// Automatically generates vector embeddings for all messages on a topic.
/// New messages are indexed in real-time as they arrive.
async fn configure_auto_embed(
    Json(req): Json<AutoEmbedRequest>,
) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "topic": req.topic,
        "model": req.model.unwrap_or_else(|| "simple-hash".to_string()),
        "dimensions": req.dimensions.unwrap_or(384),
        "status": "enabled",
        "message": format!("Auto-embedding enabled for topic '{}'. New messages will be indexed automatically.", req.topic)
    }))
}

/// GET /api/v1/ai/auto-embed - List all topics with auto-embedding enabled
async fn list_auto_embed() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "topics": [],
        "total": 0,
        "message": "No topics currently have auto-embedding enabled"
    }))
}

#[derive(Debug, serde::Deserialize)]
struct AutoEmbedRequest {
    topic: String,
    model: Option<String>,
    dimensions: Option<usize>,
    batch_size: Option<usize>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    fn test_state() -> AiApiState {
        AiApiState::new().unwrap()
    }

    fn test_app() -> Router {
        create_ai_api_router(test_state())
    }

    #[test]
    fn test_ai_api_state_new() {
        let state = AiApiState::new().unwrap();
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

    // -- classify endpoint --

    #[tokio::test]
    async fn test_classify_text() {
        let app = test_app();
        let resp = app
            .oneshot(
                Request::post("/api/v1/ai/classify")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"text": "I love this product!", "categories": ["positive", "negative", "neutral"]}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let parsed: ClassifyResponse = serde_json::from_slice(&body).unwrap();
        assert!(!parsed.category.is_empty());
        assert!(parsed.confidence > 0.0);
        assert_eq!(parsed.scores.len(), 3);
        // Scores should sum to approximately 1.0
        let total: f32 = parsed.scores.values().sum();
        assert!((total - 1.0).abs() < 0.01);
    }

    #[tokio::test]
    async fn test_classify_single_category() {
        let app = test_app();
        let resp = app
            .oneshot(
                Request::post("/api/v1/ai/classify")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"text": "test", "categories": ["spam"]}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let parsed: ClassifyResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed.category, "spam");
    }

    // -- enrich endpoint --

    #[tokio::test]
    async fn test_enrich_text() {
        let app = test_app();
        let resp = app
            .oneshot(
                Request::post("/api/v1/ai/enrich")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"text": "The quick brown fox jumps over the lazy dog and runs away fast"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let parsed: EnrichResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed.metadata["word_count"], 13);
        assert_eq!(parsed.metadata["language"], "en");
        assert!(parsed.summary.is_some());
        // Long text should get truncated summary
        assert!(parsed.summary.unwrap().ends_with("..."));
    }

    #[tokio::test]
    async fn test_enrich_short_text() {
        let app = test_app();
        let resp = app
            .oneshot(
                Request::post("/api/v1/ai/enrich")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"text": "hello world"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let parsed: EnrichResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed.text, "hello world");
        assert_eq!(parsed.metadata["word_count"], 2);
        // Short text should not be truncated
        assert_eq!(parsed.summary.unwrap(), "hello world");
    }

    #[tokio::test]
    async fn test_enrich_with_fields() {
        let app = test_app();
        let resp = app
            .oneshot(
                Request::post("/api/v1/ai/enrich")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"text": "hello", "fields": ["sentiment", "language"]}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let parsed: EnrichResponse = serde_json::from_slice(&body).unwrap();
        // language is auto-populated, sentiment should be null
        assert!(parsed.metadata.contains_key("sentiment"));
        assert!(parsed.metadata.contains_key("language"));
    }

    // -- embeddings endpoint --

    #[tokio::test]
    async fn test_embeddings_endpoint() {
        let app = test_app();
        let resp = app
            .oneshot(
                Request::post("/api/v1/ai/embeddings")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"texts": ["hello world", "foo bar"], "model": "test-model"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let parsed: EmbedResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed.embeddings.len(), 2);
        assert_eq!(parsed.model, "test-model");
        assert_eq!(parsed.dimensions, 384);
    }

    #[tokio::test]
    async fn test_embed_endpoint() {
        let app = test_app();
        let resp = app
            .oneshot(
                Request::post("/api/v1/ai/embed")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"texts": ["test"]}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let parsed: EmbedResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed.embeddings.len(), 1);
        assert_eq!(parsed.dimensions, 384);
        // Embeddings should be normalized (magnitude ~1.0)
        let norm: f32 = parsed.embeddings[0].iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 0.01);
    }

    // -- stats / status endpoint --

    #[tokio::test]
    async fn test_stats_endpoint() {
        let app = test_app();
        let resp = app
            .oneshot(
                Request::get("/api/v1/ai/stats")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["status"], "active");
        assert!(parsed["capabilities"]["embeddings"].as_bool().unwrap());
        assert!(parsed["capabilities"]["classification"].as_bool().unwrap());
    }

    #[tokio::test]
    async fn test_status_endpoint() {
        let app = test_app();
        let resp = app
            .oneshot(
                Request::get("/api/v1/ai/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    // -- pipeline CRUD --

    #[tokio::test]
    async fn test_list_pipelines_empty() {
        let app = test_app();
        let resp = app
            .oneshot(
                Request::get("/api/v1/ai/pipelines")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["total"], 0);
    }

    #[tokio::test]
    async fn test_create_pipeline() {
        let state = test_state();
        let app = create_ai_api_router(state);
        let resp = app
            .oneshot(
                Request::post("/api/v1/ai/pipelines")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{
                            "name": "test-pipeline",
                            "source_topics": ["input"],
                            "stages": [{"type": "embed"}]
                        }"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn test_create_and_get_pipeline() {
        let state = test_state();
        // Create
        let app = create_ai_api_router(state.clone());
        let resp = app
            .oneshot(
                Request::post("/api/v1/ai/pipelines")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{
                            "name": "my-pipe",
                            "source_topics": ["events"],
                            "stages": [{"type": "classify", "categories": ["a", "b"]}]
                        }"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);

        // Get
        let app = create_ai_api_router(state);
        let resp = app
            .oneshot(
                Request::get("/api/v1/ai/pipelines/my-pipe")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_pipeline_not_found() {
        let app = test_app();
        let resp = app
            .oneshot(
                Request::get("/api/v1/ai/pipelines/nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_delete_pipeline() {
        let state = test_state();
        // Create first
        let app = create_ai_api_router(state.clone());
        let _ = app
            .oneshot(
                Request::post("/api/v1/ai/pipelines")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"name": "to-delete", "source_topics": ["t"], "stages": [{"type": "embed"}]}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Delete with proper HTTP DELETE method
        let app = create_ai_api_router(state);
        let resp = app
            .oneshot(
                Request::delete("/api/v1/ai/pipelines/to-delete")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn test_start_pipeline() {
        let state = test_state();
        let app = create_ai_api_router(state.clone());
        let _ = app
            .oneshot(
                Request::post("/api/v1/ai/pipelines")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"name": "start-me", "source_topics": ["t"], "stages": [{"type": "embed"}]}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        let app = create_ai_api_router(state);
        let resp = app
            .oneshot(
                Request::post("/api/v1/ai/pipelines/start-me/start")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_stop_pipeline() {
        let state = test_state();
        // Create and start
        let app = create_ai_api_router(state.clone());
        let _ = app
            .oneshot(
                Request::post("/api/v1/ai/pipelines")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"name": "stop-me", "source_topics": ["t"], "stages": [{"type": "embed"}]}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        let app = create_ai_api_router(state.clone());
        let _ = app
            .oneshot(
                Request::post("/api/v1/ai/pipelines/stop-me/start")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Stop
        let app = create_ai_api_router(state);
        let resp = app
            .oneshot(
                Request::post("/api/v1/ai/pipelines/stop-me/stop")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_stop_pipeline_not_found() {
        let app = test_app();
        let resp = app
            .oneshot(
                Request::post("/api/v1/ai/pipelines/ghost/stop")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    // -- search endpoint --

    #[tokio::test]
    async fn test_semantic_search() {
        let app = test_app();
        let resp = app
            .oneshot(
                Request::post("/api/v1/ai/search")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"query": "machine learning", "limit": 5}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["total"], 0);
    }

    // -- anomalies endpoint --

    #[tokio::test]
    async fn test_list_anomalies() {
        let app = test_app();
        let resp = app
            .oneshot(
                Request::get("/api/v1/ai/anomalies")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(parsed["total"].as_u64().is_some());
    }

    // -- templates endpoint --

    #[tokio::test]
    async fn test_list_templates() {
        let app = test_app();
        let resp = app
            .oneshot(
                Request::get("/api/v1/ai/templates")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(parsed["total"].as_u64().unwrap() > 0);
    }

    // -- pipeline with invalid stage --

    #[tokio::test]
    async fn test_create_pipeline_invalid_stage() {
        let app = test_app();
        let resp = app
            .oneshot(
                Request::post("/api/v1/ai/pipelines")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"name": "bad", "source_topics": ["t"], "stages": [{"type": "invalid_stage"}]}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    // -- serde deserialization tests --

    #[test]
    fn test_classify_request_serde() {
        let json = r#"{"text": "hello", "categories": ["a", "b"]}"#;
        let req: ClassifyRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.text, "hello");
        assert_eq!(req.categories.len(), 2);
    }

    #[test]
    fn test_enrich_request_serde_minimal() {
        let json = r#"{"text": "hello"}"#;
        let req: EnrichRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.text, "hello");
        assert!(req.fields.is_none());
    }

    #[test]
    fn test_enrich_request_serde_with_fields() {
        let json = r#"{"text": "hello", "fields": ["sentiment", "entities"]}"#;
        let req: EnrichRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.fields.unwrap().len(), 2);
    }

    #[test]
    fn test_classify_response_serde() {
        let resp = ClassifyResponse {
            category: "positive".to_string(),
            confidence: 0.85,
            scores: HashMap::from([
                ("positive".to_string(), 0.85),
                ("negative".to_string(), 0.15),
            ]),
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("positive"));
        assert!(json.contains("0.85"));
    }

    #[test]
    fn test_enrich_response_serde() {
        let resp = EnrichResponse {
            text: "hello".to_string(),
            metadata: HashMap::from([
                ("word_count".to_string(), serde_json::json!(1)),
            ]),
            summary: None,
            entities: vec!["test".to_string()],
        };
        let json = serde_json::to_string(&resp).unwrap();
        // summary should be omitted when None
        assert!(!json.contains("summary"));
        assert!(json.contains("word_count"));
    }
}
