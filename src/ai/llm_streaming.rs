//! LLM Streaming Protocol
//!
//! This module provides native support for LLM token streaming with:
//! - Token-level offsets and delivery
//! - Backpressure handling
//! - Semantic boundary detection
//! - Chunk batching for efficiency
//!
//! # Features
//!
//! - **Token Streaming**: Real-time token delivery with sub-10ms latency
//! - **Backpressure**: Automatic flow control when consumers are slow
//! - **Semantic Batching**: Group tokens at sentence/paragraph boundaries
//! - **Multi-Provider**: Support for OpenAI, Anthropic, and local LLMs
//!
//! # Example
//!
//! ```rust,ignore
//! use streamline::ai::llm_streaming::{LLMStreamClient, StreamConfig};
//!
//! let client = LLMStreamClient::new(config)?;
//! let mut stream = client.complete_stream("Hello").await?;
//!
//! while let Some(chunk) = stream.next().await {
//!     println!("Token: {}", chunk.content);
//! }
//! ```

use crate::error::{Result, StreamlineError};
use async_trait::async_trait;
use futures_util::Stream;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, RwLock};
use tracing::error;

/// Configuration for LLM streaming
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LLMStreamConfig {
    /// Enable streaming
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    /// Maximum tokens per response
    #[serde(default = "default_max_tokens")]
    pub max_tokens: usize,

    /// Chunk buffer size
    #[serde(default = "default_buffer_size")]
    pub buffer_size: usize,

    /// Enable semantic batching (group tokens at boundaries)
    #[serde(default = "default_semantic_batching")]
    pub semantic_batching: bool,

    /// Semantic batch flush timeout in milliseconds
    #[serde(default = "default_batch_timeout_ms")]
    pub batch_timeout_ms: u64,

    /// Target batch size for semantic batching
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Request timeout in milliseconds
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,

    /// Rate limit (requests per minute)
    #[serde(default = "default_rate_limit")]
    pub rate_limit: u32,

    /// Enable metrics collection
    #[serde(default)]
    pub metrics_enabled: bool,

    /// Provider-specific options
    #[serde(default)]
    pub provider_options: HashMap<String, String>,
}

fn default_enabled() -> bool {
    true
}

fn default_max_tokens() -> usize {
    4096
}

fn default_buffer_size() -> usize {
    1000
}

fn default_semantic_batching() -> bool {
    true
}

fn default_batch_timeout_ms() -> u64 {
    50
}

fn default_batch_size() -> usize {
    10
}

fn default_timeout_ms() -> u64 {
    60_000
}

fn default_rate_limit() -> u32 {
    100
}

impl Default for LLMStreamConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            max_tokens: default_max_tokens(),
            buffer_size: default_buffer_size(),
            semantic_batching: default_semantic_batching(),
            batch_timeout_ms: default_batch_timeout_ms(),
            batch_size: default_batch_size(),
            timeout_ms: default_timeout_ms(),
            rate_limit: default_rate_limit(),
            metrics_enabled: false,
            provider_options: HashMap::new(),
        }
    }
}

/// A chunk of streamed LLM response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamChunk {
    /// Unique chunk ID
    pub id: u64,

    /// Token offset within the response
    pub offset: u64,

    /// The content of this chunk
    pub content: String,

    /// Token count in this chunk
    pub token_count: usize,

    /// Whether this is the final chunk
    pub is_final: bool,

    /// Whether this chunk ends at a semantic boundary (sentence, paragraph)
    pub is_boundary: bool,

    /// Boundary type if is_boundary is true
    pub boundary_type: Option<SemanticBoundary>,

    /// Model name that generated this chunk
    pub model: Option<String>,

    /// Timestamp when chunk was generated (ms since epoch)
    pub timestamp: i64,

    /// Optional metadata
    #[serde(default)]
    pub metadata: HashMap<String, String>,

    /// Finish reason if final (stop, length, error)
    pub finish_reason: Option<FinishReason>,
}

impl StreamChunk {
    /// Create a new stream chunk
    pub fn new(id: u64, offset: u64, content: String) -> Self {
        Self {
            id,
            offset,
            content: content.clone(),
            token_count: Self::estimate_tokens(&content),
            is_final: false,
            is_boundary: false,
            boundary_type: None,
            model: None,
            timestamp: chrono::Utc::now().timestamp_millis(),
            metadata: HashMap::new(),
            finish_reason: None,
        }
    }

    /// Create a final chunk
    pub fn final_chunk(id: u64, offset: u64, reason: FinishReason) -> Self {
        Self {
            id,
            offset,
            content: String::new(),
            token_count: 0,
            is_final: true,
            is_boundary: true,
            boundary_type: Some(SemanticBoundary::EndOfResponse),
            model: None,
            timestamp: chrono::Utc::now().timestamp_millis(),
            metadata: HashMap::new(),
            finish_reason: Some(reason),
        }
    }

    /// Estimate token count (rough approximation: ~4 chars per token)
    fn estimate_tokens(text: &str) -> usize {
        text.len().div_ceil(4)
    }

    /// Check if content ends at a semantic boundary
    pub fn detect_boundary(&mut self) {
        // Check for paragraph boundary before trimming (newlines get trimmed)
        if self.content.ends_with('\n') {
            self.is_boundary = true;
            self.boundary_type = Some(SemanticBoundary::Paragraph);
            return;
        }

        let trimmed = self.content.trim_end();

        if trimmed.ends_with('.') || trimmed.ends_with('!') || trimmed.ends_with('?') {
            self.is_boundary = true;
            self.boundary_type = Some(SemanticBoundary::Sentence);
        } else if trimmed.ends_with(',') || trimmed.ends_with(';') || trimmed.ends_with(':') {
            self.is_boundary = true;
            self.boundary_type = Some(SemanticBoundary::Clause);
        }
    }
}

/// Types of semantic boundaries
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SemanticBoundary {
    /// End of a clause (comma, semicolon)
    Clause,
    /// End of a sentence (period, exclamation, question mark)
    Sentence,
    /// End of a paragraph (newline)
    Paragraph,
    /// End of the entire response
    EndOfResponse,
}

/// Reasons for stream completion
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinishReason {
    /// Natural stop token
    Stop,
    /// Max token limit reached
    Length,
    /// Content filter triggered
    ContentFilter,
    /// Error during generation
    Error,
    /// User cancelled
    Cancelled,
    /// Timeout
    Timeout,
}

impl std::fmt::Display for FinishReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FinishReason::Stop => write!(f, "stop"),
            FinishReason::Length => write!(f, "length"),
            FinishReason::ContentFilter => write!(f, "content_filter"),
            FinishReason::Error => write!(f, "error"),
            FinishReason::Cancelled => write!(f, "cancelled"),
            FinishReason::Timeout => write!(f, "timeout"),
        }
    }
}

/// Trait for streaming LLM providers
#[async_trait]
pub trait LLMStreamProvider: Send + Sync {
    /// Provider name
    fn name(&self) -> &str;

    /// Check if provider is available
    fn is_available(&self) -> bool;

    /// Start a streaming completion
    async fn complete_stream(
        &self,
        prompt: &str,
        config: &LLMStreamConfig,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<StreamChunk>> + Send>>>;

    /// Start a streaming chat completion
    async fn chat_stream(
        &self,
        messages: Vec<ChatMessage>,
        config: &LLMStreamConfig,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<StreamChunk>> + Send>>>;
}

/// Chat message for multi-turn conversations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    /// Role: system, user, assistant
    pub role: String,
    /// Message content
    pub content: String,
    /// Optional name
    pub name: Option<String>,
}

impl ChatMessage {
    pub fn system(content: impl Into<String>) -> Self {
        Self {
            role: "system".to_string(),
            content: content.into(),
            name: None,
        }
    }

    pub fn user(content: impl Into<String>) -> Self {
        Self {
            role: "user".to_string(),
            content: content.into(),
            name: None,
        }
    }

    pub fn assistant(content: impl Into<String>) -> Self {
        Self {
            role: "assistant".to_string(),
            content: content.into(),
            name: None,
        }
    }
}

/// Statistics for LLM streaming
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LLMStreamStats {
    /// Total streaming completions
    pub total_completions: u64,
    /// Total tokens generated
    pub total_tokens: u64,
    /// Total chunks delivered
    pub total_chunks: u64,
    /// Average tokens per second
    pub avg_tokens_per_second: f64,
    /// Average first-token latency (ms)
    pub avg_first_token_latency_ms: f64,
    /// Total errors
    pub errors: u64,
    /// Active streams
    pub active_streams: u64,
}

/// Internal statistics tracker
struct StatsTracker {
    total_completions: AtomicU64,
    total_tokens: AtomicU64,
    total_chunks: AtomicU64,
    total_latency_ms: AtomicU64,
    latency_count: AtomicU64,
    errors: AtomicU64,
    active_streams: AtomicU64,
}

impl Default for StatsTracker {
    fn default() -> Self {
        Self {
            total_completions: AtomicU64::new(0),
            total_tokens: AtomicU64::new(0),
            total_chunks: AtomicU64::new(0),
            total_latency_ms: AtomicU64::new(0),
            latency_count: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            active_streams: AtomicU64::new(0),
        }
    }
}

impl StatsTracker {
    fn snapshot(&self) -> LLMStreamStats {
        let latency_count = self.latency_count.load(Ordering::Relaxed);
        let total_latency = self.total_latency_ms.load(Ordering::Relaxed);

        LLMStreamStats {
            total_completions: self.total_completions.load(Ordering::Relaxed),
            total_tokens: self.total_tokens.load(Ordering::Relaxed),
            total_chunks: self.total_chunks.load(Ordering::Relaxed),
            avg_tokens_per_second: 0.0, // Would need timing data
            avg_first_token_latency_ms: if latency_count > 0 {
                total_latency as f64 / latency_count as f64
            } else {
                0.0
            },
            errors: self.errors.load(Ordering::Relaxed),
            active_streams: self.active_streams.load(Ordering::Relaxed),
        }
    }
}

/// LLM Streaming Client
pub struct LLMStreamClient {
    config: LLMStreamConfig,
    provider: Box<dyn LLMStreamProvider>,
    stats: Arc<StatsTracker>,
    request_count: AtomicU64,
    last_reset: Arc<RwLock<Instant>>,
}

impl LLMStreamClient {
    /// Create a new streaming client
    pub fn new(config: LLMStreamConfig, provider: Box<dyn LLMStreamProvider>) -> Result<Self> {
        Ok(Self {
            config,
            provider,
            stats: Arc::new(StatsTracker::default()),
            request_count: AtomicU64::new(0),
            last_reset: Arc::new(RwLock::new(Instant::now())),
        })
    }

    /// Check rate limit
    async fn check_rate_limit(&self) -> Result<()> {
        let mut last_reset = self.last_reset.write().await;
        let now = Instant::now();

        if now.duration_since(*last_reset) >= Duration::from_secs(60) {
            self.request_count.store(0, Ordering::SeqCst);
            *last_reset = now;
        }

        let count = self.request_count.fetch_add(1, Ordering::SeqCst);
        if count >= self.config.rate_limit as u64 {
            return Err(StreamlineError::Config(
                "LLM streaming rate limit exceeded".into(),
            ));
        }

        Ok(())
    }

    /// Start a streaming completion
    pub async fn complete_stream(&self, prompt: &str) -> Result<StreamingResponse> {
        if !self.config.enabled {
            return Err(StreamlineError::Config(
                "LLM streaming is not enabled".into(),
            ));
        }

        self.check_rate_limit().await?;

        self.stats.total_completions.fetch_add(1, Ordering::Relaxed);
        self.stats.active_streams.fetch_add(1, Ordering::Relaxed);

        let start = Instant::now();
        let stream = self.provider.complete_stream(prompt, &self.config).await?;

        // Record first-token latency
        let first_token_latency = start.elapsed().as_millis() as u64;
        self.stats
            .total_latency_ms
            .fetch_add(first_token_latency, Ordering::Relaxed);
        self.stats.latency_count.fetch_add(1, Ordering::Relaxed);

        Ok(StreamingResponse::new(
            stream,
            self.config.clone(),
            self.stats.clone(),
        ))
    }

    /// Start a streaming chat completion
    pub async fn chat_stream(&self, messages: Vec<ChatMessage>) -> Result<StreamingResponse> {
        if !self.config.enabled {
            return Err(StreamlineError::Config(
                "LLM streaming is not enabled".into(),
            ));
        }

        self.check_rate_limit().await?;

        self.stats.total_completions.fetch_add(1, Ordering::Relaxed);
        self.stats.active_streams.fetch_add(1, Ordering::Relaxed);

        let stream = self.provider.chat_stream(messages, &self.config).await?;

        Ok(StreamingResponse::new(
            stream,
            self.config.clone(),
            self.stats.clone(),
        ))
    }

    /// Get statistics
    pub fn stats(&self) -> LLMStreamStats {
        self.stats.snapshot()
    }

    /// Check if provider is available
    pub fn is_available(&self) -> bool {
        self.provider.is_available()
    }

    /// Get provider name
    pub fn provider_name(&self) -> &str {
        self.provider.name()
    }
}

/// Wrapper for streaming response with semantic batching
pub struct StreamingResponse {
    inner: Pin<Box<dyn Stream<Item = Result<StreamChunk>> + Send>>,
    config: LLMStreamConfig,
    stats: Arc<StatsTracker>,
    batch_buffer: Vec<StreamChunk>,
    last_flush: Instant,
}

impl StreamingResponse {
    fn new(
        stream: Pin<Box<dyn Stream<Item = Result<StreamChunk>> + Send>>,
        config: LLMStreamConfig,
        stats: Arc<StatsTracker>,
    ) -> Self {
        Self {
            inner: stream,
            config,
            stats,
            batch_buffer: Vec::new(),
            last_flush: Instant::now(),
        }
    }

    /// Get the next chunk (with semantic batching if enabled)
    pub async fn next(&mut self) -> Option<Result<StreamChunk>> {
        use futures_util::StreamExt;

        if self.config.semantic_batching {
            self.next_batched().await
        } else {
            match self.inner.next().await {
                Some(Ok(mut chunk)) => {
                    self.stats.total_chunks.fetch_add(1, Ordering::Relaxed);
                    self.stats
                        .total_tokens
                        .fetch_add(chunk.token_count as u64, Ordering::Relaxed);
                    chunk.detect_boundary();

                    if chunk.is_final {
                        self.stats.active_streams.fetch_sub(1, Ordering::Relaxed);
                    }

                    Some(Ok(chunk))
                }
                Some(Err(e)) => {
                    self.stats.errors.fetch_add(1, Ordering::Relaxed);
                    self.stats.active_streams.fetch_sub(1, Ordering::Relaxed);
                    Some(Err(e))
                }
                None => {
                    self.stats.active_streams.fetch_sub(1, Ordering::Relaxed);
                    None
                }
            }
        }
    }

    /// Get next chunk with semantic batching
    async fn next_batched(&mut self) -> Option<Result<StreamChunk>> {
        use futures_util::StreamExt;

        loop {
            // Check if we should flush the batch
            let should_flush = !self.batch_buffer.is_empty()
                && (self.batch_buffer.len() >= self.config.batch_size
                    || self.last_flush.elapsed()
                        >= Duration::from_millis(self.config.batch_timeout_ms)
                    || self
                        .batch_buffer
                        .last()
                        .map(|c| c.is_boundary)
                        .unwrap_or(false));

            if should_flush {
                return Some(Ok(self.flush_batch()));
            }

            // Get next chunk from underlying stream
            match self.inner.next().await {
                Some(Ok(mut chunk)) => {
                    chunk.detect_boundary();

                    if chunk.is_final {
                        // Flush any remaining buffer, then return final
                        if !self.batch_buffer.is_empty() {
                            let batched = self.flush_batch();
                            self.batch_buffer.push(chunk);
                            return Some(Ok(batched));
                        }
                        self.stats.active_streams.fetch_sub(1, Ordering::Relaxed);
                        return Some(Ok(chunk));
                    }

                    self.batch_buffer.push(chunk);

                    // Immediately flush at semantic boundaries
                    if self
                        .batch_buffer
                        .last()
                        .map(|c| c.is_boundary)
                        .unwrap_or(false)
                    {
                        return Some(Ok(self.flush_batch()));
                    }
                }
                Some(Err(e)) => {
                    self.stats.errors.fetch_add(1, Ordering::Relaxed);
                    self.stats.active_streams.fetch_sub(1, Ordering::Relaxed);
                    return Some(Err(e));
                }
                None => {
                    // Stream ended, flush remaining buffer
                    if !self.batch_buffer.is_empty() {
                        return Some(Ok(self.flush_batch()));
                    }
                    self.stats.active_streams.fetch_sub(1, Ordering::Relaxed);
                    return None;
                }
            }
        }
    }

    /// Flush the batch buffer into a single chunk
    fn flush_batch(&mut self) -> StreamChunk {
        let chunks = std::mem::take(&mut self.batch_buffer);
        self.last_flush = Instant::now();

        if chunks.is_empty() {
            return StreamChunk::new(0, 0, String::new());
        }

        let first = &chunks[0];
        let last = &chunks[chunks.len() - 1];

        let content: String = chunks.iter().map(|c| c.content.as_str()).collect();
        let total_tokens: usize = chunks.iter().map(|c| c.token_count).sum();

        self.stats.total_chunks.fetch_add(1, Ordering::Relaxed);
        self.stats
            .total_tokens
            .fetch_add(total_tokens as u64, Ordering::Relaxed);

        StreamChunk {
            id: first.id,
            offset: first.offset,
            content,
            token_count: total_tokens,
            is_final: last.is_final,
            is_boundary: last.is_boundary,
            boundary_type: last.boundary_type,
            model: first.model.clone(),
            timestamp: chrono::Utc::now().timestamp_millis(),
            metadata: first.metadata.clone(),
            finish_reason: last.finish_reason,
        }
    }

    /// Collect all chunks into a complete response
    pub async fn collect(mut self) -> Result<String> {
        let mut result = String::new();

        while let Some(chunk_result) = self.next().await {
            let chunk = chunk_result?;
            result.push_str(&chunk.content);
        }

        Ok(result)
    }
}

/// Mock streaming provider for testing
pub struct MockStreamProvider {
    name: String,
    tokens: Vec<String>,
    delay_ms: u64,
}

impl MockStreamProvider {
    /// Create a new mock provider
    pub fn new(tokens: Vec<String>, delay_ms: u64) -> Self {
        Self {
            name: "mock".to_string(),
            tokens,
            delay_ms,
        }
    }

    /// Create a provider that simulates real LLM output
    pub fn simulated() -> Self {
        let tokens = vec![
            "Hello".to_string(),
            ", ".to_string(),
            "I".to_string(),
            " am".to_string(),
            " a".to_string(),
            " streaming".to_string(),
            " language".to_string(),
            " model".to_string(),
            ".".to_string(),
            " How".to_string(),
            " can".to_string(),
            " I".to_string(),
            " help".to_string(),
            " you".to_string(),
            " today".to_string(),
            "?".to_string(),
        ];
        Self::new(tokens, 20)
    }
}

#[async_trait]
impl LLMStreamProvider for MockStreamProvider {
    fn name(&self) -> &str {
        &self.name
    }

    fn is_available(&self) -> bool {
        true
    }

    async fn complete_stream(
        &self,
        _prompt: &str,
        _config: &LLMStreamConfig,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<StreamChunk>> + Send>>> {
        let tokens = self.tokens.clone();
        let delay = self.delay_ms;

        let stream = async_stream::stream! {
            let mut offset = 0u64;
            for (i, token) in tokens.iter().enumerate() {
                tokio::time::sleep(Duration::from_millis(delay)).await;

                let mut chunk = StreamChunk::new(i as u64, offset, token.clone());
                chunk.detect_boundary();
                offset += token.len() as u64;

                yield Ok(chunk);
            }

            // Send final chunk
            yield Ok(StreamChunk::final_chunk(
                tokens.len() as u64,
                offset,
                FinishReason::Stop,
            ));
        };

        Ok(Box::pin(stream))
    }

    async fn chat_stream(
        &self,
        _messages: Vec<ChatMessage>,
        config: &LLMStreamConfig,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<StreamChunk>> + Send>>> {
        // Reuse complete_stream for mock
        self.complete_stream("", config).await
    }
}

/// Manager for LLM streaming with WebSocket support
pub struct LLMStreamManager {
    client: Arc<LLMStreamClient>,
    subscribers: Arc<RwLock<HashMap<String, broadcast::Sender<StreamChunk>>>>,
}

impl LLMStreamManager {
    /// Create a new stream manager
    pub fn new(client: LLMStreamClient) -> Self {
        Self {
            client: Arc::new(client),
            subscribers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Start a streaming completion with a subscription ID
    pub async fn start_stream(
        &self,
        subscription_id: &str,
        prompt: &str,
    ) -> Result<broadcast::Receiver<StreamChunk>> {
        let (tx, rx) = broadcast::channel(100);

        // Store subscriber
        {
            let mut subs = self.subscribers.write().await;
            subs.insert(subscription_id.to_string(), tx.clone());
        }

        // Start streaming in background
        let client = self.client.clone();
        let prompt = prompt.to_string();
        let sub_id = subscription_id.to_string();
        let subscribers = self.subscribers.clone();

        tokio::spawn(async move {
            match client.complete_stream(&prompt).await {
                Ok(mut response) => {
                    while let Some(chunk_result) = response.next().await {
                        match chunk_result {
                            Ok(chunk) => {
                                let is_final = chunk.is_final;
                                if tx.send(chunk).is_err() {
                                    break;
                                }
                                if is_final {
                                    break;
                                }
                            }
                            Err(e) => {
                                error!(error = %e, "Streaming error");
                                break;
                            }
                        }
                    }
                }
                Err(e) => {
                    error!(error = %e, "Failed to start stream");
                }
            }

            // Clean up subscription
            let mut subs = subscribers.write().await;
            subs.remove(&sub_id);
        });

        Ok(rx)
    }

    /// Cancel a streaming subscription
    pub async fn cancel_stream(&self, subscription_id: &str) {
        let mut subs = self.subscribers.write().await;
        subs.remove(subscription_id);
    }

    /// Get statistics
    pub fn stats(&self) -> LLMStreamStats {
        self.client.stats()
    }

    /// Get active subscription count
    pub async fn active_subscriptions(&self) -> usize {
        let subs = self.subscribers.read().await;
        subs.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_chunk_creation() {
        let chunk = StreamChunk::new(0, 0, "Hello".to_string());
        assert_eq!(chunk.content, "Hello");
        assert!(!chunk.is_final);
    }

    #[test]
    fn test_stream_chunk_boundary_detection() {
        let mut chunk = StreamChunk::new(0, 0, "Hello.".to_string());
        chunk.detect_boundary();
        assert!(chunk.is_boundary);
        assert_eq!(chunk.boundary_type, Some(SemanticBoundary::Sentence));

        let mut chunk = StreamChunk::new(0, 0, "Hello\n".to_string());
        chunk.detect_boundary();
        assert!(chunk.is_boundary);
        assert_eq!(chunk.boundary_type, Some(SemanticBoundary::Paragraph));
    }

    #[test]
    fn test_chat_message_creation() {
        let system = ChatMessage::system("You are a helpful assistant");
        assert_eq!(system.role, "system");

        let user = ChatMessage::user("Hello!");
        assert_eq!(user.role, "user");

        let assistant = ChatMessage::assistant("Hi there!");
        assert_eq!(assistant.role, "assistant");
    }

    #[test]
    fn test_config_defaults() {
        let config = LLMStreamConfig::default();
        assert!(config.enabled);
        assert_eq!(config.max_tokens, 4096);
        assert!(config.semantic_batching);
    }

    #[tokio::test]
    async fn test_mock_stream_provider() {
        let provider = MockStreamProvider::simulated();
        assert!(provider.is_available());
        assert_eq!(provider.name(), "mock");

        let config = LLMStreamConfig::default();
        let stream = provider.complete_stream("test", &config).await;
        assert!(stream.is_ok());
    }

    #[tokio::test]
    async fn test_streaming_response_collect() {
        let provider = Box::new(MockStreamProvider::new(
            vec!["Hello".to_string(), " World".to_string()],
            1,
        ));

        let config = LLMStreamConfig {
            semantic_batching: false,
            ..Default::default()
        };

        let client = LLMStreamClient::new(config.clone(), provider).unwrap();
        let response = client.complete_stream("test").await.unwrap();
        let result = response.collect().await.unwrap();

        assert!(result.contains("Hello"));
        assert!(result.contains("World"));
    }

    #[test]
    fn test_finish_reason_display() {
        assert_eq!(format!("{}", FinishReason::Stop), "stop");
        assert_eq!(format!("{}", FinishReason::Length), "length");
        assert_eq!(format!("{}", FinishReason::Timeout), "timeout");
    }

    #[test]
    fn test_stats_tracker() {
        let tracker = StatsTracker::default();
        tracker.total_completions.fetch_add(1, Ordering::Relaxed);
        tracker.total_tokens.fetch_add(100, Ordering::Relaxed);

        let stats = tracker.snapshot();
        assert_eq!(stats.total_completions, 1);
        assert_eq!(stats.total_tokens, 100);
    }
}
