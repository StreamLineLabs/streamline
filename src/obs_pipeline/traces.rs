//! Distributed Tracing
//!
//! Provides distributed tracing compatible with OpenTelemetry.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Span kind
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum SpanKind {
    /// Internal operation
    #[default]
    Internal,
    /// Server receiving a request
    Server,
    /// Client making a request
    Client,
    /// Producer sending a message
    Producer,
    /// Consumer receiving a message
    Consumer,
}

/// Span status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum SpanStatus {
    /// Unset status
    #[default]
    Unset,
    /// OK status
    Ok,
    /// Error status
    Error,
}

/// Span context for propagation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpanContext {
    /// Trace ID (16 bytes hex)
    pub trace_id: String,
    /// Span ID (8 bytes hex)
    pub span_id: String,
    /// Trace flags
    pub trace_flags: u8,
    /// Is remote (from another service)
    pub is_remote: bool,
}

impl SpanContext {
    /// Create a new root span context
    pub fn new() -> Self {
        Self {
            trace_id: generate_trace_id(),
            span_id: generate_span_id(),
            trace_flags: 1, // Sampled
            is_remote: false,
        }
    }

    /// Create a child context
    pub fn child(&self) -> Self {
        Self {
            trace_id: self.trace_id.clone(),
            span_id: generate_span_id(),
            trace_flags: self.trace_flags,
            is_remote: false,
        }
    }

    /// Parse from W3C Trace Context header
    pub fn from_traceparent(header: &str) -> Option<Self> {
        let parts: Vec<&str> = header.split('-').collect();
        if parts.len() != 4 || parts[0] != "00" {
            return None;
        }

        Some(Self {
            trace_id: parts[1].to_string(),
            span_id: parts[2].to_string(),
            trace_flags: u8::from_str_radix(parts[3], 16).ok()?,
            is_remote: true,
        })
    }

    /// Convert to W3C Trace Context header
    pub fn to_traceparent(&self) -> String {
        format!(
            "00-{}-{}-{:02x}",
            self.trace_id, self.span_id, self.trace_flags
        )
    }

    /// Is sampled?
    pub fn is_sampled(&self) -> bool {
        self.trace_flags & 0x01 != 0
    }
}

impl Default for SpanContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Generate a trace ID (32 hex chars)
fn generate_trace_id() -> String {
    format!(
        "{:016x}{:016x}",
        rand::random::<u64>(),
        rand::random::<u64>()
    )
}

/// Generate a span ID (16 hex chars)
fn generate_span_id() -> String {
    format!("{:016x}", rand::random::<u64>())
}

/// Span event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpanEvent {
    /// Event name
    pub name: String,
    /// Event timestamp
    pub timestamp: i64,
    /// Event attributes
    pub attributes: HashMap<String, String>,
}

impl SpanEvent {
    /// Create a new event
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            timestamp: chrono::Utc::now().timestamp_millis(),
            attributes: HashMap::new(),
        }
    }

    /// Add attribute
    pub fn with_attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.attributes.insert(key.into(), value.into());
        self
    }
}

/// Span link
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpanLink {
    /// Linked span context
    pub context: SpanContext,
    /// Link attributes
    pub attributes: HashMap<String, String>,
}

/// A trace span
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Span {
    /// Span context
    pub context: SpanContext,
    /// Parent span ID (if not root)
    pub parent_span_id: Option<String>,
    /// Span name
    pub name: String,
    /// Span kind
    pub kind: SpanKind,
    /// Start time (Unix timestamp ms)
    pub start_time: i64,
    /// End time (Unix timestamp ms)
    pub end_time: Option<i64>,
    /// Span status
    pub status: SpanStatus,
    /// Status message
    pub status_message: Option<String>,
    /// Span attributes
    pub attributes: HashMap<String, String>,
    /// Span events
    pub events: Vec<SpanEvent>,
    /// Span links
    pub links: Vec<SpanLink>,
}

impl Span {
    /// Create a new root span
    pub fn new(name: impl Into<String>, kind: SpanKind) -> Self {
        Self {
            context: SpanContext::new(),
            parent_span_id: None,
            name: name.into(),
            kind,
            start_time: chrono::Utc::now().timestamp_millis(),
            end_time: None,
            status: SpanStatus::Unset,
            status_message: None,
            attributes: HashMap::new(),
            events: Vec::new(),
            links: Vec::new(),
        }
    }

    /// Create a child span
    pub fn child(name: impl Into<String>, parent: &SpanContext) -> Self {
        Self {
            context: parent.child(),
            parent_span_id: Some(parent.span_id.clone()),
            name: name.into(),
            kind: SpanKind::Internal,
            start_time: chrono::Utc::now().timestamp_millis(),
            end_time: None,
            status: SpanStatus::Unset,
            status_message: None,
            attributes: HashMap::new(),
            events: Vec::new(),
            links: Vec::new(),
        }
    }

    /// Set span kind
    pub fn with_kind(mut self, kind: SpanKind) -> Self {
        self.kind = kind;
        self
    }

    /// Add attribute
    pub fn set_attribute(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.attributes.insert(key.into(), value.into());
    }

    /// Add event
    pub fn add_event(&mut self, event: SpanEvent) {
        self.events.push(event);
    }

    /// Record exception
    pub fn record_exception(&mut self, error: &str) {
        let event = SpanEvent::new("exception")
            .with_attribute("exception.type", "Error")
            .with_attribute("exception.message", error);
        self.events.push(event);
        self.status = SpanStatus::Error;
        self.status_message = Some(error.to_string());
    }

    /// Set status
    pub fn set_status(&mut self, status: SpanStatus, message: Option<&str>) {
        self.status = status;
        self.status_message = message.map(|s| s.to_string());
    }

    /// End the span
    pub fn end(&mut self) {
        self.end_time = Some(chrono::Utc::now().timestamp_millis());
    }

    /// Get duration in milliseconds
    pub fn duration_ms(&self) -> Option<i64> {
        self.end_time.map(|end| end - self.start_time)
    }

    /// Is span finished?
    pub fn is_finished(&self) -> bool {
        self.end_time.is_some()
    }
}

/// Trace sampler
#[derive(Debug, Clone)]
pub struct TraceSampler {
    /// Sampling rate (0.0 to 1.0)
    rate: f64,
}

impl TraceSampler {
    /// Create a new sampler
    pub fn new(rate: f64) -> Self {
        Self {
            rate: rate.clamp(0.0, 1.0),
        }
    }

    /// Always sample
    pub fn always_on() -> Self {
        Self { rate: 1.0 }
    }

    /// Never sample
    pub fn always_off() -> Self {
        Self { rate: 0.0 }
    }

    /// Should this trace be sampled?
    pub fn should_sample(&self, _context: &SpanContext) -> bool {
        if self.rate >= 1.0 {
            return true;
        }
        if self.rate <= 0.0 {
            return false;
        }
        rand::random::<f64>() < self.rate
    }

    /// Get sampling rate
    pub fn rate(&self) -> f64 {
        self.rate
    }
}

impl Default for TraceSampler {
    fn default() -> Self {
        Self::always_on()
    }
}

/// Trace (collection of spans)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trace {
    /// Trace ID
    pub trace_id: String,
    /// All spans in this trace
    pub spans: Vec<Span>,
}

impl Trace {
    /// Create a new trace
    pub fn new(trace_id: impl Into<String>) -> Self {
        Self {
            trace_id: trace_id.into(),
            spans: Vec::new(),
        }
    }

    /// Add a span
    pub fn add_span(&mut self, span: Span) {
        self.spans.push(span);
    }

    /// Get root span
    pub fn root_span(&self) -> Option<&Span> {
        self.spans.iter().find(|s| s.parent_span_id.is_none())
    }

    /// Get span by ID
    pub fn span(&self, span_id: &str) -> Option<&Span> {
        self.spans.iter().find(|s| s.context.span_id == span_id)
    }

    /// Get child spans of a span
    pub fn children(&self, span_id: &str) -> Vec<&Span> {
        self.spans
            .iter()
            .filter(|s| s.parent_span_id.as_deref() == Some(span_id))
            .collect()
    }

    /// Get total duration
    pub fn duration_ms(&self) -> Option<i64> {
        self.root_span()?.duration_ms()
    }
}

/// Trace collector
pub struct TraceCollector {
    sampler: TraceSampler,
    traces: HashMap<String, Trace>,
    active_spans: HashMap<String, Span>,
    max_traces: usize,
}

impl TraceCollector {
    /// Create a new collector
    pub fn new(sampler: TraceSampler) -> Self {
        Self {
            sampler,
            traces: HashMap::new(),
            active_spans: HashMap::new(),
            max_traces: 1000,
        }
    }

    /// Start a new root span
    pub fn start_span(&mut self, name: &str, kind: SpanKind) -> Option<Span> {
        let span = Span::new(name, kind);

        if !self.sampler.should_sample(&span.context) {
            return None;
        }

        self.active_spans
            .insert(span.context.span_id.clone(), span.clone());
        Some(span)
    }

    /// Start a child span
    pub fn start_child_span(&mut self, name: &str, parent: &SpanContext) -> Option<Span> {
        if !parent.is_sampled() {
            return None;
        }

        let span = Span::child(name, parent);
        self.active_spans
            .insert(span.context.span_id.clone(), span.clone());
        Some(span)
    }

    /// End a span
    pub fn end_span(&mut self, mut span: Span) {
        span.end();
        self.active_spans.remove(&span.context.span_id);

        // Add to trace
        let trace = self
            .traces
            .entry(span.context.trace_id.clone())
            .or_insert_with(|| Trace::new(&span.context.trace_id));
        trace.add_span(span);

        // Evict old traces if necessary
        while self.traces.len() > self.max_traces {
            if let Some(oldest_key) = self.traces.keys().next().cloned() {
                self.traces.remove(&oldest_key);
            }
        }
    }

    /// Get a trace by ID
    pub fn trace(&self, trace_id: &str) -> Option<&Trace> {
        self.traces.get(trace_id)
    }

    /// Get all completed traces
    pub fn traces(&self) -> impl Iterator<Item = &Trace> {
        self.traces.values()
    }

    /// Get active span count
    pub fn active_span_count(&self) -> usize {
        self.active_spans.len()
    }

    /// Get trace count
    pub fn trace_count(&self) -> usize {
        self.traces.len()
    }

    /// Clear all traces
    pub fn clear(&mut self) {
        self.traces.clear();
        self.active_spans.clear();
    }
}

impl Default for TraceCollector {
    fn default() -> Self {
        Self::new(TraceSampler::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_span_context() {
        let ctx = SpanContext::new();

        assert_eq!(ctx.trace_id.len(), 32);
        assert_eq!(ctx.span_id.len(), 16);
        assert!(ctx.is_sampled());
    }

    #[test]
    fn test_span_context_child() {
        let parent = SpanContext::new();
        let child = parent.child();

        assert_eq!(child.trace_id, parent.trace_id);
        assert_ne!(child.span_id, parent.span_id);
    }

    #[test]
    fn test_traceparent() {
        let ctx = SpanContext::new();
        let header = ctx.to_traceparent();

        let parsed = SpanContext::from_traceparent(&header).unwrap();
        assert_eq!(parsed.trace_id, ctx.trace_id);
        assert_eq!(parsed.span_id, ctx.span_id);
        assert!(parsed.is_remote);
    }

    #[test]
    fn test_span_creation() {
        let span = Span::new("test-operation", SpanKind::Server);

        assert_eq!(span.name, "test-operation");
        assert_eq!(span.kind, SpanKind::Server);
        assert!(span.parent_span_id.is_none());
        assert!(span.end_time.is_none());
    }

    #[test]
    fn test_span_child() {
        let parent = Span::new("parent", SpanKind::Server);
        let child = Span::child("child", &parent.context);

        assert_eq!(child.context.trace_id, parent.context.trace_id);
        assert_eq!(child.parent_span_id, Some(parent.context.span_id));
    }

    #[test]
    fn test_span_attributes_and_events() {
        let mut span = Span::new("test", SpanKind::Internal);

        span.set_attribute("user_id", "123");
        span.add_event(SpanEvent::new("processing").with_attribute("items", "10"));

        assert_eq!(span.attributes.get("user_id"), Some(&"123".to_string()));
        assert_eq!(span.events.len(), 1);
    }

    #[test]
    fn test_span_exception() {
        let mut span = Span::new("test", SpanKind::Internal);

        span.record_exception("Something went wrong");

        assert_eq!(span.status, SpanStatus::Error);
        assert_eq!(span.events.len(), 1);
        assert_eq!(span.events[0].name, "exception");
    }

    #[test]
    fn test_span_duration() {
        let mut span = Span::new("test", SpanKind::Internal);

        assert!(span.duration_ms().is_none());

        span.end();

        assert!(span.duration_ms().is_some());
        assert!(span.is_finished());
    }

    #[test]
    fn test_trace_sampler() {
        let always_on = TraceSampler::always_on();
        let ctx = SpanContext::new();
        assert!(always_on.should_sample(&ctx));

        let always_off = TraceSampler::always_off();
        assert!(!always_off.should_sample(&ctx));
    }

    #[test]
    fn test_trace_collector() {
        let mut collector = TraceCollector::new(TraceSampler::always_on());

        let span = collector.start_span("test", SpanKind::Server).unwrap();
        let trace_id = span.context.trace_id.clone();

        collector.end_span(span);

        assert_eq!(collector.trace_count(), 1);
        assert!(collector.trace(&trace_id).is_some());
    }

    #[test]
    fn test_trace_collector_child_span() {
        let mut collector = TraceCollector::new(TraceSampler::always_on());

        let parent = collector.start_span("parent", SpanKind::Server).unwrap();
        let parent_ctx = parent.context.clone();

        let child = collector.start_child_span("child", &parent_ctx).unwrap();

        assert_eq!(child.context.trace_id, parent_ctx.trace_id);
        assert_eq!(child.parent_span_id, Some(parent_ctx.span_id.clone()));
    }
}
