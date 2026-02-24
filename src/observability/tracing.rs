//! Message-Level Distributed Tracing for Streamline
//!
//! Injects and propagates trace context through Kafka message headers,
//! enabling end-to-end distributed tracing across produce → consume → transform → sink.
//!
//! Supports W3C Trace Context (traceparent/tracestate headers) and
//! OpenTelemetry-compatible span creation.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tracing::debug;

/// Trace context header names (W3C Trace Context standard)
pub const TRACEPARENT_HEADER: &str = "traceparent";
pub const TRACESTATE_HEADER: &str = "tracestate";

/// Configuration for message tracing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageTracingConfig {
    /// Enable message-level tracing
    pub enabled: bool,
    /// Sampling rate (0.0 to 1.0, 1.0 = trace all messages)
    pub sampling_rate: f64,
    /// Maximum trace spans to keep in memory
    pub max_spans: usize,
    /// Enable tail-based sampling (keep traces with anomalies)
    pub tail_sampling: bool,
    /// Latency threshold for tail-based sampling (ms)
    pub tail_sampling_latency_threshold_ms: u64,
}

impl Default for MessageTracingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            sampling_rate: 0.01, // 1% default
            max_spans: 100_000,
            tail_sampling: true,
            tail_sampling_latency_threshold_ms: 100,
        }
    }
}

/// A W3C-compatible trace context
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceContext {
    /// Trace ID (16 bytes, hex-encoded = 32 chars)
    pub trace_id: String,
    /// Parent span ID (8 bytes, hex-encoded = 16 chars)
    pub parent_span_id: String,
    /// Current span ID (8 bytes, hex-encoded = 16 chars)
    pub span_id: String,
    /// Trace flags (sampled = 0x01)
    pub trace_flags: u8,
    /// Additional vendor-specific trace state
    pub trace_state: Option<String>,
}

impl TraceContext {
    /// Generate a new root trace context
    pub fn new_root() -> Self {
        Self {
            trace_id: generate_trace_id(),
            parent_span_id: "0000000000000000".to_string(),
            span_id: generate_span_id(),
            trace_flags: 0x01, // sampled
            trace_state: None,
        }
    }

    /// Create a child context from a parent
    pub fn child(&self) -> Self {
        Self {
            trace_id: self.trace_id.clone(),
            parent_span_id: self.span_id.clone(),
            span_id: generate_span_id(),
            trace_flags: self.trace_flags,
            trace_state: self.trace_state.clone(),
        }
    }

    /// Encode as a W3C traceparent header value
    /// Format: {version}-{trace_id}-{parent_id}-{flags}
    pub fn to_traceparent(&self) -> String {
        format!(
            "00-{}-{}-{:02x}",
            self.trace_id, self.span_id, self.trace_flags
        )
    }

    /// Parse a W3C traceparent header
    pub fn from_traceparent(header: &str) -> Option<Self> {
        let parts: Vec<&str> = header.split('-').collect();
        if parts.len() != 4 || parts[0] != "00" {
            return None;
        }
        let trace_id = parts[1].to_string();
        let parent_span_id = parts[2].to_string();
        let trace_flags = u8::from_str_radix(parts[3], 16).ok()?;

        if trace_id.len() != 32 || parent_span_id.len() != 16 {
            return None;
        }

        Some(Self {
            trace_id,
            parent_span_id: parent_span_id.clone(),
            span_id: generate_span_id(),
            trace_flags,
            trace_state: None,
        })
    }

    /// Extract trace context from Kafka message headers
    pub fn from_headers(headers: &[(String, Vec<u8>)]) -> Option<Self> {
        for (key, value) in headers {
            if key == TRACEPARENT_HEADER {
                if let Ok(header_str) = std::str::from_utf8(value) {
                    return Self::from_traceparent(header_str);
                }
            }
        }
        None
    }

    /// Inject trace context into Kafka message headers
    pub fn inject_into_headers(&self, headers: &mut Vec<(String, Vec<u8>)>) {
        // Remove existing traceparent if present
        headers.retain(|(k, _)| k != TRACEPARENT_HEADER && k != TRACESTATE_HEADER);

        headers.push((
            TRACEPARENT_HEADER.to_string(),
            self.to_traceparent().into_bytes(),
        ));

        if let Some(ref state) = self.trace_state {
            headers.push((TRACESTATE_HEADER.to_string(), state.as_bytes().to_vec()));
        }
    }
}

/// A span representing a single operation in a message's lifecycle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageSpan {
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: String,
    pub operation: SpanOperation,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub start_time: String,
    pub duration_ms: f64,
    pub status: SpanStatus,
    pub attributes: HashMap<String, String>,
}

/// Operations tracked by message spans
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpanOperation {
    Produce,
    Consume,
    Transform,
    Sink,
    Rebalance,
}

impl std::fmt::Display for SpanOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Produce => write!(f, "produce"),
            Self::Consume => write!(f, "consume"),
            Self::Transform => write!(f, "transform"),
            Self::Sink => write!(f, "sink"),
            Self::Rebalance => write!(f, "rebalance"),
        }
    }
}

/// Span completion status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SpanStatus {
    Ok,
    Error(String),
}

/// The message tracing engine
pub struct MessageTracer {
    config: MessageTracingConfig,
    spans: Arc<RwLock<Vec<MessageSpan>>>,
    sampled_count: AtomicU64,
    total_count: AtomicU64,
}

impl MessageTracer {
    /// Create a new message tracer
    pub fn new(config: MessageTracingConfig) -> Self {
        Self {
            config,
            spans: Arc::new(RwLock::new(Vec::new())),
            sampled_count: AtomicU64::new(0),
            total_count: AtomicU64::new(0),
        }
    }

    /// Determine if a message should be traced (based on sampling rate)
    pub fn should_sample(&self) -> bool {
        if !self.config.enabled {
            return false;
        }
        let count = self.total_count.fetch_add(1, Ordering::Relaxed);
        let rate = self.config.sampling_rate;
        if rate >= 1.0 {
            return true;
        }
        if rate <= 0.0 {
            return false;
        }
        // Deterministic sampling based on count
        (count as f64 * rate) as u64 > ((count.wrapping_sub(1)) as f64 * rate) as u64
    }

    /// Start a produce span
    pub fn start_produce_span(
        &self,
        ctx: &TraceContext,
        topic: &str,
        partition: i32,
    ) -> SpanBuilder {
        SpanBuilder {
            trace_id: ctx.trace_id.clone(),
            span_id: ctx.span_id.clone(),
            parent_span_id: ctx.parent_span_id.clone(),
            operation: SpanOperation::Produce,
            topic: topic.to_string(),
            partition,
            start: Instant::now(),
            attributes: HashMap::new(),
        }
    }

    /// Start a consume span
    pub fn start_consume_span(
        &self,
        ctx: &TraceContext,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> SpanBuilder {
        let mut builder = SpanBuilder {
            trace_id: ctx.trace_id.clone(),
            span_id: ctx.span_id.clone(),
            parent_span_id: ctx.parent_span_id.clone(),
            operation: SpanOperation::Consume,
            topic: topic.to_string(),
            partition,
            start: Instant::now(),
            attributes: HashMap::new(),
        };
        builder.attributes.insert("offset".to_string(), offset.to_string());
        builder
    }

    /// Record a completed span
    pub async fn record_span(&self, span: MessageSpan) {
        let should_keep = if self.config.tail_sampling {
            // Tail-based: keep if slow or errored
            span.duration_ms > self.config.tail_sampling_latency_threshold_ms as f64
                || matches!(span.status, SpanStatus::Error(_))
        } else {
            true
        };

        if should_keep {
            self.sampled_count.fetch_add(1, Ordering::Relaxed);
            let mut spans = self.spans.write().await;
            spans.push(span);

            // Evict oldest spans if over limit
            if spans.len() > self.config.max_spans {
                let drain_count = spans.len() - self.config.max_spans / 2;
                spans.drain(..drain_count);
            }
        }
    }

    /// Get recent spans, optionally filtered by trace ID
    pub async fn recent_spans(
        &self,
        limit: usize,
        trace_id_filter: Option<&str>,
    ) -> Vec<MessageSpan> {
        let spans = self.spans.read().await;
        spans
            .iter()
            .rev()
            .filter(|s| {
                trace_id_filter
                    .map(|tid| s.trace_id == tid)
                    .unwrap_or(true)
            })
            .take(limit)
            .cloned()
            .collect()
    }

    /// Get tracing statistics
    pub fn stats(&self) -> TracingStats {
        TracingStats {
            total_messages: self.total_count.load(Ordering::Relaxed),
            sampled_messages: self.sampled_count.load(Ordering::Relaxed),
            sampling_rate: self.config.sampling_rate,
            tail_sampling_enabled: self.config.tail_sampling,
        }
    }
}

/// Builder for creating message spans
pub struct SpanBuilder {
    trace_id: String,
    span_id: String,
    parent_span_id: String,
    operation: SpanOperation,
    topic: String,
    partition: i32,
    start: Instant,
    attributes: HashMap<String, String>,
}

impl SpanBuilder {
    /// Add an attribute to the span
    pub fn with_attribute(mut self, key: &str, value: &str) -> Self {
        self.attributes.insert(key.to_string(), value.to_string());
        self
    }

    /// Complete the span successfully
    pub fn finish(self, offset: i64) -> MessageSpan {
        MessageSpan {
            trace_id: self.trace_id,
            span_id: self.span_id,
            parent_span_id: self.parent_span_id,
            operation: self.operation,
            topic: self.topic,
            partition: self.partition,
            offset,
            start_time: chrono::Utc::now().to_rfc3339(),
            duration_ms: self.start.elapsed().as_secs_f64() * 1000.0,
            status: SpanStatus::Ok,
            attributes: self.attributes,
        }
    }

    /// Complete the span with an error
    pub fn finish_error(self, offset: i64, error: &str) -> MessageSpan {
        MessageSpan {
            trace_id: self.trace_id,
            span_id: self.span_id,
            parent_span_id: self.parent_span_id,
            operation: self.operation,
            topic: self.topic,
            partition: self.partition,
            offset,
            start_time: chrono::Utc::now().to_rfc3339(),
            duration_ms: self.start.elapsed().as_secs_f64() * 1000.0,
            status: SpanStatus::Error(error.to_string()),
            attributes: self.attributes,
        }
    }
}

/// Tracing statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TracingStats {
    pub total_messages: u64,
    pub sampled_messages: u64,
    pub sampling_rate: f64,
    pub tail_sampling_enabled: bool,
}

// ID generation using atomic counter + random seed
static SPAN_COUNTER: AtomicU64 = AtomicU64::new(0);

fn generate_trace_id() -> String {
    let high = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    let low = SPAN_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{:016x}{:016x}", high, low)
}

fn generate_span_id() -> String {
    let val = SPAN_COUNTER.fetch_add(1, Ordering::Relaxed);
    let time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    format!("{:016x}", val ^ time)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trace_context_roundtrip() {
        let ctx = TraceContext::new_root();
        let header = ctx.to_traceparent();
        let parsed = TraceContext::from_traceparent(&header).unwrap();
        assert_eq!(parsed.trace_id, ctx.trace_id);
    }

    #[test]
    fn test_trace_context_child() {
        let parent = TraceContext::new_root();
        let child = parent.child();
        assert_eq!(child.trace_id, parent.trace_id);
        assert_eq!(child.parent_span_id, parent.span_id);
        assert_ne!(child.span_id, parent.span_id);
    }

    #[test]
    fn test_inject_extract_headers() {
        let ctx = TraceContext::new_root();
        let mut headers = Vec::new();
        ctx.inject_into_headers(&mut headers);

        let extracted = TraceContext::from_headers(&headers).unwrap();
        assert_eq!(extracted.trace_id, ctx.trace_id);
    }

    #[test]
    fn test_span_builder() {
        let ctx = TraceContext::new_root();
        let tracer = MessageTracer::new(MessageTracingConfig::default());
        let span = tracer
            .start_produce_span(&ctx, "test-topic", 0)
            .with_attribute("client_id", "test-client")
            .finish(42);
        assert_eq!(span.topic, "test-topic");
        assert_eq!(span.offset, 42);
        assert!(matches!(span.status, SpanStatus::Ok));
    }

    #[tokio::test]
    async fn test_tracer_records_spans() {
        let config = MessageTracingConfig {
            enabled: true,
            sampling_rate: 1.0,
            tail_sampling: false,
            ..Default::default()
        };
        let tracer = MessageTracer::new(config);
        let ctx = TraceContext::new_root();

        let span = tracer.start_produce_span(&ctx, "topic-a", 0).finish(0);
        tracer.record_span(span).await;

        let spans = tracer.recent_spans(10, None).await;
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].topic, "topic-a");
    }
}
