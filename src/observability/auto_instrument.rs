//! OpenTelemetry Auto-Instrumentation for Streamline
//!
//! Provides automatic trace context injection and extraction for every
//! produce/consume/transform operation without requiring user code changes.
//!
//! # Architecture
//!
//! ```text
//! Producer → [auto-inject traceparent header] → Storage
//!                                                   ↓
//! Consumer ← [auto-extract traceparent header] ← Storage
//!                                                   ↓
//! Transform → [create child span] → Output Topic
//! ```
//!
//! # Configuration
//!
//! Enable via server config or environment:
//! ```toml
//! [telemetry]
//! auto_instrumentation = true
//! sampling_rate = 0.01        # 1% of messages
//! tail_sampling = true         # keep slow traces
//! export_endpoint = "http://localhost:4317"  # OTLP gRPC
//! ```

use super::tracing::{
    MessageSpan, MessageTracer, MessageTracingConfig, SpanOperation, SpanStatus, TraceContext,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{debug, info};

/// Configuration for auto-instrumentation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoInstrumentationConfig {
    /// Enable auto-instrumentation
    pub enabled: bool,
    /// Tracing config (sampling, tail-based, etc.)
    pub tracing: MessageTracingConfig,
    /// OTLP export endpoint (gRPC)
    pub otlp_endpoint: Option<String>,
    /// Service name for spans
    pub service_name: String,
    /// Add topic name as span attribute
    pub include_topic: bool,
    /// Add partition as span attribute
    pub include_partition: bool,
    /// Add offset as span attribute
    pub include_offset: bool,
    /// Add message size as span attribute
    pub include_message_size: bool,
    /// Add client_id as span attribute
    pub include_client_id: bool,
}

impl Default for AutoInstrumentationConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            tracing: MessageTracingConfig::default(),
            otlp_endpoint: None,
            service_name: "streamline".to_string(),
            include_topic: true,
            include_partition: true,
            include_offset: true,
            include_message_size: true,
            include_client_id: true,
        }
    }
}

/// The auto-instrumentation engine that hooks into produce/consume paths
pub struct AutoInstrumenter {
    config: AutoInstrumentationConfig,
    tracer: Arc<MessageTracer>,
}

impl AutoInstrumenter {
    pub fn new(config: AutoInstrumentationConfig) -> Self {
        let tracer = Arc::new(MessageTracer::new(config.tracing.clone()));
        Self { config, tracer }
    }

    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Instrument a produce operation:
    /// 1. Decide whether to sample this message
    /// 2. Create or propagate trace context
    /// 3. Inject traceparent header into message headers
    /// 4. Return the context and any headers to inject
    pub fn instrument_produce(
        &self,
        topic: &str,
        partition: i32,
        existing_headers: &[(String, Vec<u8>)],
        message_size: usize,
        client_id: Option<&str>,
    ) -> ProduceInstrumentation {
        if !self.config.enabled {
            return ProduceInstrumentation::noop();
        }

        // Check if there's an existing trace context from the client
        let ctx = TraceContext::from_headers(existing_headers)
            .map(|parent| parent.child())
            .unwrap_or_else(|| {
                if self.tracer.should_sample() {
                    TraceContext::new_root()
                } else {
                    return TraceContext::new_root(); // unsampled
                }
            });

        // Build headers to inject
        let mut inject_headers = Vec::new();
        ctx.inject_into_headers(&mut inject_headers);

        // Start a span
        let mut span_builder = self.tracer.start_produce_span(&ctx, topic, partition);
        if self.config.include_message_size {
            span_builder =
                span_builder.with_attribute("message.size", &message_size.to_string());
        }
        if self.config.include_client_id {
            if let Some(cid) = client_id {
                span_builder = span_builder.with_attribute("client.id", cid);
            }
        }

        ProduceInstrumentation {
            active: true,
            trace_context: Some(ctx),
            inject_headers,
            span_builder: Some(span_builder),
            tracer: Some(self.tracer.clone()),
        }
    }

    /// Instrument a consume operation:
    /// 1. Extract trace context from message headers
    /// 2. Create a child span for the consume operation
    pub fn instrument_consume(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        headers: &[(String, Vec<u8>)],
        client_id: Option<&str>,
    ) -> ConsumeInstrumentation {
        if !self.config.enabled {
            return ConsumeInstrumentation::noop();
        }

        let ctx = TraceContext::from_headers(headers).map(|parent| parent.child());

        if let Some(ref ctx) = ctx {
            let mut span_builder =
                self.tracer.start_consume_span(ctx, topic, partition, offset);
            if self.config.include_client_id {
                if let Some(cid) = client_id {
                    span_builder = span_builder.with_attribute("client.id", cid);
                }
            }

            ConsumeInstrumentation {
                active: true,
                trace_context: Some(ctx.clone()),
                span_builder: Some(span_builder),
                tracer: Some(self.tracer.clone()),
            }
        } else {
            ConsumeInstrumentation::noop()
        }
    }

    /// Instrument a WASM transform operation
    pub fn instrument_transform(
        &self,
        transform_name: &str,
        input_topic: &str,
        output_topic: &str,
        headers: &[(String, Vec<u8>)],
    ) -> TransformInstrumentation {
        if !self.config.enabled {
            return TransformInstrumentation::noop();
        }

        let ctx = TraceContext::from_headers(headers).map(|parent| parent.child());

        if let Some(ref ctx) = ctx {
            let span_builder = self
                .tracer
                .start_produce_span(ctx, output_topic, 0)
                .with_attribute("transform.name", transform_name)
                .with_attribute("transform.input_topic", input_topic)
                .with_attribute("transform.output_topic", output_topic);

            // Inject propagated context for downstream
            let mut propagated_headers = Vec::new();
            ctx.inject_into_headers(&mut propagated_headers);

            TransformInstrumentation {
                active: true,
                trace_context: Some(ctx.clone()),
                propagated_headers,
                span_builder: Some(span_builder),
                tracer: Some(self.tracer.clone()),
            }
        } else {
            TransformInstrumentation::noop()
        }
    }

    /// Get the underlying tracer for querying spans
    pub fn tracer(&self) -> &Arc<MessageTracer> {
        &self.tracer
    }

    /// Get tracing statistics
    pub fn stats(&self) -> InstrumentationStats {
        let tracer_stats = self.tracer.stats();
        InstrumentationStats {
            enabled: self.config.enabled,
            service_name: self.config.service_name.clone(),
            otlp_endpoint: self.config.otlp_endpoint.clone(),
            total_messages: tracer_stats.total_messages,
            sampled_messages: tracer_stats.sampled_messages,
            sampling_rate: tracer_stats.sampling_rate,
            tail_sampling_enabled: tracer_stats.tail_sampling_enabled,
        }
    }
}

/// Result of instrumenting a produce operation
pub struct ProduceInstrumentation {
    active: bool,
    pub trace_context: Option<TraceContext>,
    pub inject_headers: Vec<(String, Vec<u8>)>,
    span_builder: Option<super::tracing::SpanBuilder>,
    tracer: Option<Arc<MessageTracer>>,
}

impl ProduceInstrumentation {
    fn noop() -> Self {
        Self {
            active: false,
            trace_context: None,
            inject_headers: Vec::new(),
            span_builder: None,
            tracer: None,
        }
    }

    /// Complete the produce span with the resulting offset
    pub async fn finish(self, offset: i64) {
        if let (Some(builder), Some(tracer)) = (self.span_builder, self.tracer) {
            let span = builder.finish(offset);
            tracer.record_span(span).await;
        }
    }

    /// Complete the produce span with an error
    pub async fn finish_error(self, offset: i64, error: &str) {
        if let (Some(builder), Some(tracer)) = (self.span_builder, self.tracer) {
            let span = builder.finish_error(offset, error);
            tracer.record_span(span).await;
        }
    }
}

/// Result of instrumenting a consume operation
pub struct ConsumeInstrumentation {
    active: bool,
    pub trace_context: Option<TraceContext>,
    span_builder: Option<super::tracing::SpanBuilder>,
    tracer: Option<Arc<MessageTracer>>,
}

impl ConsumeInstrumentation {
    fn noop() -> Self {
        Self {
            active: false,
            trace_context: None,
            span_builder: None,
            tracer: None,
        }
    }

    pub async fn finish(self, records_delivered: usize) {
        if let (Some(builder), Some(tracer)) = (self.span_builder, self.tracer) {
            let span = builder
                .with_attribute("records.delivered", &records_delivered.to_string())
                .finish(0);
            tracer.record_span(span).await;
        }
    }
}

/// Result of instrumenting a transform operation
pub struct TransformInstrumentation {
    active: bool,
    pub trace_context: Option<TraceContext>,
    pub propagated_headers: Vec<(String, Vec<u8>)>,
    span_builder: Option<super::tracing::SpanBuilder>,
    tracer: Option<Arc<MessageTracer>>,
}

impl TransformInstrumentation {
    fn noop() -> Self {
        Self {
            active: false,
            trace_context: None,
            propagated_headers: Vec::new(),
            span_builder: None,
            tracer: None,
        }
    }

    pub async fn finish(self, output_records: usize) {
        if let (Some(builder), Some(tracer)) = (self.span_builder, self.tracer) {
            let span = builder
                .with_attribute("transform.output_records", &output_records.to_string())
                .finish(0);
            tracer.record_span(span).await;
        }
    }
}

/// Overall instrumentation statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstrumentationStats {
    pub enabled: bool,
    pub service_name: String,
    pub otlp_endpoint: Option<String>,
    pub total_messages: u64,
    pub sampled_messages: u64,
    pub sampling_rate: f64,
    pub tail_sampling_enabled: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> AutoInstrumentationConfig {
        AutoInstrumentationConfig {
            enabled: true,
            tracing: MessageTracingConfig {
                enabled: true,
                sampling_rate: 1.0,
                tail_sampling: false,
                ..MessageTracingConfig::default()
            },
            ..Default::default()
        }
    }

    #[test]
    fn test_noop_when_disabled() {
        let config = AutoInstrumentationConfig::default(); // disabled by default
        let instrumenter = AutoInstrumenter::new(config);
        assert!(!instrumenter.is_enabled());

        let result = instrumenter.instrument_produce("topic", 0, &[], 100, None);
        assert!(!result.active);
        assert!(result.inject_headers.is_empty());
    }

    #[test]
    fn test_produce_instrumentation_injects_headers() {
        let instrumenter = AutoInstrumenter::new(test_config());
        let result = instrumenter.instrument_produce("test-topic", 0, &[], 1024, Some("client-1"));

        assert!(result.active);
        assert!(!result.inject_headers.is_empty());

        // Should have traceparent header
        let traceparent = result
            .inject_headers
            .iter()
            .find(|(k, _)| k == "traceparent");
        assert!(traceparent.is_some());
    }

    #[test]
    fn test_consume_extracts_context() {
        let instrumenter = AutoInstrumenter::new(test_config());

        // First produce to get headers
        let produce = instrumenter.instrument_produce("topic", 0, &[], 100, None);
        let headers = produce.inject_headers.clone();

        // Then consume with those headers
        let consume = instrumenter.instrument_consume("topic", 0, 42, &headers, Some("consumer-1"));
        assert!(consume.active);
        assert!(consume.trace_context.is_some());

        // Trace ID should match (propagated)
        let produce_trace = produce.trace_context.unwrap().trace_id;
        let consume_trace = consume.trace_context.unwrap().trace_id;
        assert_eq!(produce_trace, consume_trace);
    }

    #[test]
    fn test_transform_propagates_context() {
        let instrumenter = AutoInstrumenter::new(test_config());

        let produce = instrumenter.instrument_produce("input", 0, &[], 100, None);
        let headers = produce.inject_headers.clone();

        let transform = instrumenter.instrument_transform("json-filter", "input", "output", &headers);
        assert!(transform.active);
        assert!(!transform.propagated_headers.is_empty());

        // Propagated headers should contain traceparent
        let has_traceparent = transform
            .propagated_headers
            .iter()
            .any(|(k, _)| k == "traceparent");
        assert!(has_traceparent);
    }

    #[test]
    fn test_stats() {
        let instrumenter = AutoInstrumenter::new(test_config());
        let stats = instrumenter.stats();
        assert!(stats.enabled);
        assert_eq!(stats.service_name, "streamline");
    }
}
