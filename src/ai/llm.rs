//! LLM Integration
//!
//! Provides integration with Large Language Models for stream enrichment
//! and classification.

use super::config::{LLMConfig, LLMProviderType};
use crate::error::{Result, StreamlineError};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// LLM Client for interacting with language models
pub struct LLMClient {
    /// Configuration
    config: LLMConfig,
    /// Provider implementation
    provider: Box<dyn LLMProvider>,
    /// Request counter for rate limiting
    request_count: AtomicU64,
    /// Last rate limit reset
    last_reset: Arc<RwLock<Instant>>,
}

impl LLMClient {
    /// Create a new LLM client
    pub fn new(config: LLMConfig) -> Result<Self> {
        let provider: Box<dyn LLMProvider> = match config.provider {
            LLMProviderType::OpenAI => Box::new(OpenAIProvider::new(&config)),
            LLMProviderType::Anthropic => Box::new(AnthropicProvider::new(&config)),
            LLMProviderType::Local => Box::new(LocalProvider::new(&config)),
            LLMProviderType::AzureOpenAI => Box::new(AzureProvider::new(&config)),
            LLMProviderType::Bedrock => Box::new(BedrockProvider::new(&config)),
            LLMProviderType::Custom => Box::new(CustomProvider::new(&config)),
        };

        Ok(Self {
            config,
            provider,
            request_count: AtomicU64::new(0),
            last_reset: Arc::new(RwLock::new(Instant::now())),
        })
    }

    /// Check and update rate limit
    async fn check_rate_limit(&self) -> Result<()> {
        let mut last_reset = self.last_reset.write().await;
        let now = Instant::now();

        // Reset counter if a minute has passed
        if now.duration_since(*last_reset) >= Duration::from_secs(60) {
            self.request_count.store(0, Ordering::SeqCst);
            *last_reset = now;
        }

        let count = self.request_count.fetch_add(1, Ordering::SeqCst);
        if count >= self.config.rate_limit as u64 {
            return Err(StreamlineError::Config("Rate limit exceeded".into()));
        }

        Ok(())
    }

    /// Classify text into categories
    pub async fn classify(&self, text: &str) -> Result<ClassificationResult> {
        if !self.config.enabled || !self.config.classification_enabled {
            return Err(StreamlineError::Config(
                "LLM classification is not enabled".into(),
            ));
        }

        self.check_rate_limit().await?;

        let prompt = format!(
            "Classify the following text into one of these categories: {}.\n\nText: {}\n\nCategory:",
            self.config.classification_categories.join(", "),
            text
        );

        let response = self.provider.complete(&prompt, &self.config).await?;
        let category = response.trim().to_lowercase();

        // Find matching category
        let matched_category = self
            .config
            .classification_categories
            .iter()
            .find(|c| c.to_lowercase() == category)
            .cloned()
            .unwrap_or_else(|| self.config.classification_categories[0].clone());

        Ok(ClassificationResult {
            category: matched_category,
            confidence: 0.9, // Placeholder - would come from model
            all_scores: HashMap::new(),
        })
    }

    /// Enrich text with additional metadata
    pub async fn enrich(&self, text: &str) -> Result<EnrichmentResult> {
        if !self.config.enabled || !self.config.enrichment_enabled {
            return Err(StreamlineError::Config(
                "LLM enrichment is not enabled".into(),
            ));
        }

        self.check_rate_limit().await?;

        let fields_str = self.config.enrichment_fields.join(", ");
        let prompt = format!(
            "Extract the following information from the text: {}.\n\nText: {}\n\nOutput as JSON:",
            fields_str, text
        );

        let response = self.provider.complete(&prompt, &self.config).await?;

        // Parse response as JSON
        let extracted: HashMap<String, serde_json::Value> =
            serde_json::from_str(&response).unwrap_or_default();

        Ok(EnrichmentResult {
            fields: extracted,
            summary: None,
            entities: Vec::new(),
            sentiment: None,
        })
    }

    /// Generate a summary of text
    pub async fn summarize(&self, text: &str, max_length: usize) -> Result<String> {
        if !self.config.enabled {
            return Err(StreamlineError::Config("LLM is not enabled".into()));
        }

        self.check_rate_limit().await?;

        let prompt = format!(
            "Summarize the following text in {} words or less:\n\n{}\n\nSummary:",
            max_length, text
        );

        self.provider.complete(&prompt, &self.config).await
    }

    /// Translate a natural language query into StreamQL
    pub async fn translate_to_streamql(&self, natural_language: &str) -> Result<String> {
        if !self.config.enabled {
            return Err(StreamlineError::Config("LLM is not enabled".into()));
        }

        self.check_rate_limit().await?;

        let prompt = format!(
            "Translate the following natural language query into StreamQL (a SQL-like streaming query language). \
             StreamQL supports: SELECT, FROM <topic>, WHERE, GROUP BY, WINDOW (TUMBLING, HOPPING, SESSION), \
             JOIN, aggregates (COUNT, SUM, AVG, MIN, MAX), and EMIT CHANGES.\n\n\
             Natural language: {}\n\nStreamQL:",
            natural_language
        );

        self.provider.complete(&prompt, &self.config).await
    }

    /// Extract entities from text
    pub async fn extract_entities(&self, text: &str) -> Result<Vec<Entity>> {
        if !self.config.enabled {
            return Err(StreamlineError::Config("LLM is not enabled".into()));
        }

        self.check_rate_limit().await?;

        let prompt = format!(
            "Extract named entities (person, organization, location, date, etc.) from this text as JSON array:\n\n{}\n\nEntities:",
            text
        );

        let response = self.provider.complete(&prompt, &self.config).await?;

        // Parse response
        let entities: Vec<Entity> = serde_json::from_str(&response).unwrap_or_default();

        Ok(entities)
    }

    /// Analyze sentiment
    pub async fn analyze_sentiment(&self, text: &str) -> Result<SentimentResult> {
        if !self.config.enabled {
            return Err(StreamlineError::Config("LLM is not enabled".into()));
        }

        self.check_rate_limit().await?;

        let prompt = format!(
            "Analyze the sentiment of this text. Output as JSON with 'sentiment' (positive/negative/neutral) and 'score' (-1.0 to 1.0):\n\n{}\n\nAnalysis:",
            text
        );

        let response = self.provider.complete(&prompt, &self.config).await?;

        // Parse response
        let result: SentimentResult = serde_json::from_str(&response).unwrap_or(SentimentResult {
            sentiment: Sentiment::Neutral,
            score: 0.0,
            confidence: 0.5,
        });

        Ok(result)
    }

    /// Get provider name
    pub fn provider_name(&self) -> &str {
        self.provider.name()
    }

    /// Check if LLM is enabled
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }
}

/// LLM Provider trait for different backends
#[async_trait]
pub trait LLMProvider: Send + Sync {
    /// Provider name
    fn name(&self) -> &str;

    /// Complete a prompt
    async fn complete(&self, prompt: &str, config: &LLMConfig) -> Result<String>;

    /// Check if provider is available
    fn is_available(&self) -> bool;
}

/// OpenAI provider
pub struct OpenAIProvider {
    #[allow(dead_code)]
    api_endpoint: String,
    available: bool,
}

impl OpenAIProvider {
    fn new(config: &LLMConfig) -> Self {
        let api_endpoint = config
            .api_endpoint
            .clone()
            .unwrap_or_else(|| "https://api.openai.com/v1".to_string());

        Self {
            api_endpoint,
            available: config.api_key.is_some(),
        }
    }
}

#[async_trait]
impl LLMProvider for OpenAIProvider {
    fn name(&self) -> &str {
        "openai"
    }

    async fn complete(&self, prompt: &str, _config: &LLMConfig) -> Result<String> {
        if !self.available {
            return Err(StreamlineError::Config(
                "OpenAI API key not configured".into(),
            ));
        }

        // Placeholder - would make actual API call
        // In production, use reqwest to call OpenAI API
        Ok(format!(
            "OpenAI response to: {}",
            &prompt[..50.min(prompt.len())]
        ))
    }

    fn is_available(&self) -> bool {
        self.available
    }
}

/// Anthropic provider
pub struct AnthropicProvider {
    #[allow(dead_code)]
    api_endpoint: String,
    available: bool,
}

impl AnthropicProvider {
    fn new(config: &LLMConfig) -> Self {
        let api_endpoint = config
            .api_endpoint
            .clone()
            .unwrap_or_else(|| "https://api.anthropic.com/v1".to_string());

        Self {
            api_endpoint,
            available: config.api_key.is_some(),
        }
    }
}

#[async_trait]
impl LLMProvider for AnthropicProvider {
    fn name(&self) -> &str {
        "anthropic"
    }

    async fn complete(&self, prompt: &str, _config: &LLMConfig) -> Result<String> {
        if !self.available {
            return Err(StreamlineError::Config(
                "Anthropic API key not configured".into(),
            ));
        }

        Ok(format!(
            "Claude response to: {}",
            &prompt[..50.min(prompt.len())]
        ))
    }

    fn is_available(&self) -> bool {
        self.available
    }
}

/// Local LLM provider (Ollama, llama.cpp)
pub struct LocalProvider {
    #[allow(dead_code)]
    api_endpoint: String,
}

impl LocalProvider {
    fn new(config: &LLMConfig) -> Self {
        let api_endpoint = config
            .api_endpoint
            .clone()
            .unwrap_or_else(|| "http://localhost:11434/api".to_string());

        Self { api_endpoint }
    }
}

#[async_trait]
impl LLMProvider for LocalProvider {
    fn name(&self) -> &str {
        "local"
    }

    async fn complete(&self, prompt: &str, _config: &LLMConfig) -> Result<String> {
        // Placeholder - would call local API
        Ok(format!(
            "Local response to: {}",
            &prompt[..50.min(prompt.len())]
        ))
    }

    fn is_available(&self) -> bool {
        true // Assume local is available
    }
}

/// Azure OpenAI provider
pub struct AzureProvider {
    #[allow(dead_code)]
    api_endpoint: String,
    available: bool,
}

impl AzureProvider {
    fn new(config: &LLMConfig) -> Self {
        Self {
            api_endpoint: config.api_endpoint.clone().unwrap_or_default(),
            available: config.api_key.is_some() && config.api_endpoint.is_some(),
        }
    }
}

#[async_trait]
impl LLMProvider for AzureProvider {
    fn name(&self) -> &str {
        "azure_openai"
    }

    async fn complete(&self, prompt: &str, _config: &LLMConfig) -> Result<String> {
        if !self.available {
            return Err(StreamlineError::Config(
                "Azure OpenAI not configured".into(),
            ));
        }

        Ok(format!(
            "Azure response to: {}",
            &prompt[..50.min(prompt.len())]
        ))
    }

    fn is_available(&self) -> bool {
        self.available
    }
}

/// AWS Bedrock provider
pub struct BedrockProvider {
    available: bool,
}

impl BedrockProvider {
    fn new(_config: &LLMConfig) -> Self {
        Self {
            available: std::env::var("AWS_ACCESS_KEY_ID").is_ok(),
        }
    }
}

#[async_trait]
impl LLMProvider for BedrockProvider {
    fn name(&self) -> &str {
        "bedrock"
    }

    async fn complete(&self, prompt: &str, _config: &LLMConfig) -> Result<String> {
        if !self.available {
            return Err(StreamlineError::Config("AWS Bedrock not configured".into()));
        }

        Ok(format!(
            "Bedrock response to: {}",
            &prompt[..50.min(prompt.len())]
        ))
    }

    fn is_available(&self) -> bool {
        self.available
    }
}

/// Custom API provider
pub struct CustomProvider {
    #[allow(dead_code)]
    api_endpoint: Option<String>,
}

impl CustomProvider {
    fn new(config: &LLMConfig) -> Self {
        Self {
            api_endpoint: config.api_endpoint.clone(),
        }
    }
}

#[async_trait]
impl LLMProvider for CustomProvider {
    fn name(&self) -> &str {
        "custom"
    }

    async fn complete(&self, prompt: &str, _config: &LLMConfig) -> Result<String> {
        Ok(format!(
            "Custom response to: {}",
            &prompt[..50.min(prompt.len())]
        ))
    }

    fn is_available(&self) -> bool {
        true
    }
}

/// Classification result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClassificationResult {
    /// Predicted category
    pub category: String,
    /// Confidence score (0.0 - 1.0)
    pub confidence: f32,
    /// Scores for all categories
    pub all_scores: HashMap<String, f32>,
}

/// Enrichment result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnrichmentResult {
    /// Extracted fields
    pub fields: HashMap<String, serde_json::Value>,
    /// Summary (if generated)
    pub summary: Option<String>,
    /// Extracted entities
    pub entities: Vec<Entity>,
    /// Sentiment analysis
    pub sentiment: Option<SentimentResult>,
}

/// Named entity
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entity {
    /// Entity text
    pub text: String,
    /// Entity type (person, organization, location, etc.)
    pub entity_type: String,
    /// Start position in text
    pub start: usize,
    /// End position in text
    pub end: usize,
    /// Confidence score
    pub confidence: f32,
}

/// Sentiment analysis result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SentimentResult {
    /// Overall sentiment
    pub sentiment: Sentiment,
    /// Sentiment score (-1.0 to 1.0)
    pub score: f32,
    /// Confidence
    pub confidence: f32,
}

/// Sentiment categories
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Sentiment {
    Positive,
    Negative,
    Neutral,
}

/// Stream processor for continuous LLM processing
pub struct StreamProcessor {
    /// LLM client
    llm: Arc<LLMClient>,
    /// Processing configuration
    #[allow(dead_code)]
    config: StreamProcessorConfig,
}

impl StreamProcessor {
    /// Create a new stream processor
    pub fn new(llm: Arc<LLMClient>, config: StreamProcessorConfig) -> Self {
        Self { llm, config }
    }

    /// Process a batch of messages
    pub async fn process_batch(&self, messages: Vec<String>) -> Result<Vec<ProcessedMessage>> {
        let mut results = Vec::with_capacity(messages.len());

        for message in messages {
            let result = self.process_message(&message).await?;
            results.push(result);
        }

        Ok(results)
    }

    /// Process a single message
    pub async fn process_message(&self, message: &str) -> Result<ProcessedMessage> {
        let classification = if self.llm.config.classification_enabled {
            self.llm.classify(message).await.ok()
        } else {
            None
        };

        let enrichment = if self.llm.config.enrichment_enabled {
            self.llm.enrich(message).await.ok()
        } else {
            None
        };

        Ok(ProcessedMessage {
            original: message.to_string(),
            classification,
            enrichment,
            processed_at: chrono::Utc::now().timestamp_millis(),
        })
    }
}

/// Stream processor configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamProcessorConfig {
    /// Enable classification
    pub classify: bool,
    /// Enable enrichment
    pub enrich: bool,
    /// Batch size
    pub batch_size: usize,
    /// Process in parallel
    pub parallel: bool,
}

impl Default for StreamProcessorConfig {
    fn default() -> Self {
        Self {
            classify: true,
            enrich: true,
            batch_size: 10,
            parallel: true,
        }
    }
}

/// Processed message result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessedMessage {
    /// Original message
    pub original: String,
    /// Classification result
    pub classification: Option<ClassificationResult>,
    /// Enrichment result
    pub enrichment: Option<EnrichmentResult>,
    /// Processing timestamp
    pub processed_at: i64,
}

/// Stream enricher for adding metadata to messages
pub struct StreamEnricher {
    /// LLM client
    llm: Arc<LLMClient>,
}

impl StreamEnricher {
    /// Create a new enricher
    pub fn new(llm: Arc<LLMClient>) -> Self {
        Self { llm }
    }

    /// Enrich a message with AI-generated metadata
    pub async fn enrich(&self, message: &str) -> Result<EnrichedMessage> {
        let mut enriched = EnrichedMessage {
            content: message.to_string(),
            metadata: HashMap::new(),
            classification: None,
            entities: Vec::new(),
            sentiment: None,
        };

        // Get classification
        if let Ok(classification) = self.llm.classify(message).await {
            enriched.classification = Some(classification.category.clone());
            enriched
                .metadata
                .insert("category".to_string(), classification.category);
        }

        // Get sentiment
        if let Ok(sentiment) = self.llm.analyze_sentiment(message).await {
            enriched.sentiment = Some(sentiment.score);
            enriched
                .metadata
                .insert("sentiment_score".to_string(), sentiment.score.to_string());
        }

        // Get entities
        if let Ok(entities) = self.llm.extract_entities(message).await {
            enriched.entities = entities;
        }

        Ok(enriched)
    }
}

/// Enriched message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnrichedMessage {
    /// Original content
    pub content: String,
    /// Added metadata
    pub metadata: HashMap<String, String>,
    /// Classification category
    pub classification: Option<String>,
    /// Extracted entities
    pub entities: Vec<Entity>,
    /// Sentiment score
    pub sentiment: Option<f32>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_llm_client_creation() {
        let config = LLMConfig::default();
        let client = LLMClient::new(config);
        assert!(client.is_ok());
    }

    #[test]
    fn test_classification_result() {
        let result = ClassificationResult {
            category: "general".to_string(),
            confidence: 0.9,
            all_scores: HashMap::new(),
        };

        assert_eq!(result.category, "general");
        assert_eq!(result.confidence, 0.9);
    }

    #[test]
    fn test_sentiment() {
        let result = SentimentResult {
            sentiment: Sentiment::Positive,
            score: 0.8,
            confidence: 0.9,
        };

        assert_eq!(result.sentiment, Sentiment::Positive);
        assert!(result.score > 0.0);
    }

    #[test]
    fn test_stream_processor_config() {
        let config = StreamProcessorConfig::default();
        assert!(config.classify);
        assert!(config.enrich);
        assert_eq!(config.batch_size, 10);
    }
}
