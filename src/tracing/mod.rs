//! Distributed tracing support for Streamline
//!
//! This module provides OpenTelemetry integration for distributed tracing,
//! allowing trace correlation across Streamline instances and with other
//! services in a microservices architecture.

use opentelemetry::trace::TracerProvider as _;
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::trace::{RandomIdGenerator, Sampler, TracerProvider};
use opentelemetry_sdk::{runtime, Resource};
use std::time::Duration;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

/// Configuration for distributed tracing
#[derive(Debug, Clone)]
pub struct TracingConfig {
    /// Service name for traces
    pub service_name: String,
    /// Service version
    pub service_version: String,
    /// OTLP endpoint (e.g., "http://localhost:4317")
    pub otlp_endpoint: Option<String>,
    /// Whether tracing is enabled
    pub enabled: bool,
    /// Sampling ratio (0.0 to 1.0)
    pub sampling_ratio: f64,
    /// Export timeout in seconds
    pub export_timeout_secs: u64,
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            service_name: "streamline".to_string(),
            service_version: env!("CARGO_PKG_VERSION").to_string(),
            otlp_endpoint: None,
            enabled: false,
            sampling_ratio: 1.0,
            export_timeout_secs: 10,
        }
    }
}

impl TracingConfig {
    /// Create config from environment variables
    pub fn from_env() -> Self {
        Self {
            service_name: std::env::var("OTEL_SERVICE_NAME")
                .unwrap_or_else(|_| "streamline".to_string()),
            service_version: std::env::var("OTEL_SERVICE_VERSION")
                .unwrap_or_else(|_| env!("CARGO_PKG_VERSION").to_string()),
            otlp_endpoint: std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok(),
            enabled: std::env::var("OTEL_TRACING_ENABLED")
                .map(|v| v == "true" || v == "1")
                .unwrap_or(false),
            sampling_ratio: std::env::var("OTEL_TRACES_SAMPLER_ARG")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(1.0),
            export_timeout_secs: std::env::var("OTEL_EXPORTER_OTLP_TIMEOUT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(10),
        }
    }

    /// Create a builder for TracingConfig
    pub fn builder() -> TracingConfigBuilder {
        TracingConfigBuilder::default()
    }
}

/// Builder for TracingConfig
#[derive(Debug, Default)]
pub struct TracingConfigBuilder {
    config: TracingConfig,
}

impl TracingConfigBuilder {
    /// Set service name
    pub fn service_name(mut self, name: impl Into<String>) -> Self {
        self.config.service_name = name.into();
        self
    }

    /// Set service version
    pub fn service_version(mut self, version: impl Into<String>) -> Self {
        self.config.service_version = version.into();
        self
    }

    /// Set OTLP endpoint
    pub fn otlp_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.config.otlp_endpoint = Some(endpoint.into());
        self
    }

    /// Enable tracing
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.config.enabled = enabled;
        self
    }

    /// Set sampling ratio
    pub fn sampling_ratio(mut self, ratio: f64) -> Self {
        self.config.sampling_ratio = ratio.clamp(0.0, 1.0);
        self
    }

    /// Set export timeout
    pub fn export_timeout_secs(mut self, secs: u64) -> Self {
        self.config.export_timeout_secs = secs;
        self
    }

    /// Build the config
    pub fn build(self) -> TracingConfig {
        self.config
    }
}

/// Initialize the tracing system
///
/// This sets up the tracing subscriber with optional OpenTelemetry integration.
/// If tracing is disabled or no OTLP endpoint is configured, only standard
/// logging will be active.
pub fn init_tracing(config: &TracingConfig) -> Option<TracerProvider> {
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,streamline=debug"));

    // Get endpoint, or fall back to standard logging
    let endpoint = match (&config.enabled, config.otlp_endpoint.as_ref()) {
        (true, Some(ep)) => ep,
        _ => {
            // Standard logging only
            tracing_subscriber::registry()
                .with(env_filter)
                .with(tracing_subscriber::fmt::layer())
                .init();
            return None;
        }
    };

    // Build OpenTelemetry tracer

    let exporter = match opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .with_timeout(Duration::from_secs(config.export_timeout_secs))
        .build()
    {
        Ok(exporter) => exporter,
        Err(e) => {
            eprintln!(
                "Failed to create OTLP exporter: {}. Falling back to logging only.",
                e
            );
            tracing_subscriber::registry()
                .with(env_filter)
                .with(tracing_subscriber::fmt::layer())
                .init();
            return None;
        }
    };

    let sampler = if config.sampling_ratio >= 1.0 {
        Sampler::AlwaysOn
    } else if config.sampling_ratio <= 0.0 {
        Sampler::AlwaysOff
    } else {
        Sampler::TraceIdRatioBased(config.sampling_ratio)
    };

    let resource = Resource::new(vec![
        KeyValue::new("service.name", config.service_name.clone()),
        KeyValue::new("service.version", config.service_version.clone()),
    ]);

    let provider = TracerProvider::builder()
        .with_batch_exporter(exporter, runtime::Tokio)
        .with_sampler(sampler)
        .with_id_generator(RandomIdGenerator::default())
        .with_resource(resource)
        .build();

    let tracer = provider.tracer("streamline");
    let telemetry_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    tracing_subscriber::registry()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer())
        .with(telemetry_layer)
        .init();

    Some(provider)
}

/// Shutdown the tracing provider gracefully
pub fn shutdown_tracing(provider: Option<TracerProvider>) {
    if let Some(provider) = provider {
        if let Err(e) = provider.shutdown() {
            eprintln!("Error shutting down tracer provider: {:?}", e);
        }
    }
}

/// Helper macro to create a new span with common Streamline attributes
#[macro_export]
macro_rules! streamline_span {
    ($level:expr, $name:expr, $($field:tt)*) => {
        tracing::span!($level, $name, $($field)*)
    };
    ($level:expr, $name:expr) => {
        tracing::span!($level, $name)
    };
}

/// Record a produce operation span
#[inline]
pub fn record_produce_span(topic: &str, partition: i32, record_count: usize) {
    tracing::info_span!(
        "produce",
        otel.kind = "producer",
        messaging.system = "kafka",
        messaging.destination.name = %topic,
        messaging.destination.partition.id = %partition,
        messaging.batch.message_count = %record_count,
    );
}

/// Record a fetch operation span
#[inline]
pub fn record_fetch_span(topic: &str, partition: i32, offset: i64) {
    tracing::info_span!(
        "fetch",
        otel.kind = "consumer",
        messaging.system = "kafka",
        messaging.destination.name = %topic,
        messaging.destination.partition.id = %partition,
        messaging.kafka.consumer.offset = %offset,
    );
}

/// Record a cluster operation span
#[inline]
pub fn record_cluster_span(operation: &str, node_id: u64) {
    tracing::info_span!(
        "cluster",
        otel.kind = "internal",
        streamline.operation = %operation,
        streamline.node_id = %node_id,
    );
}

/// Record an authentication operation span
#[inline]
pub fn record_auth_span(mechanism: &str, username: Option<&str>, success: bool) {
    tracing::info_span!(
        "auth",
        otel.kind = "server",
        streamline.auth.mechanism = %mechanism,
        streamline.auth.username = ?username,
        streamline.auth.success = %success,
    );
}

/// Record a consumer group operation span
#[inline]
pub fn record_consumer_group_span(operation: &str, group_id: &str) {
    tracing::info_span!(
        "consumer_group",
        otel.kind = "server",
        messaging.system = "kafka",
        messaging.consumer.group.name = %group_id,
        streamline.operation = %operation,
    );
}

/// Record a transaction operation span
#[inline]
pub fn record_transaction_span(operation: &str, transactional_id: &str, producer_id: i64) {
    tracing::info_span!(
        "transaction",
        otel.kind = "server",
        messaging.system = "kafka",
        streamline.txn.id = %transactional_id,
        streamline.txn.producer_id = %producer_id,
        streamline.operation = %operation,
    );
}

/// Record a replication operation span
#[inline]
pub fn record_replication_span(operation: &str, topic: &str, partition: i32, leader_id: u64) {
    tracing::info_span!(
        "replication",
        otel.kind = "internal",
        messaging.system = "kafka",
        messaging.destination.name = %topic,
        messaging.destination.partition.id = %partition,
        streamline.replication.leader = %leader_id,
        streamline.operation = %operation,
    );
}

/// Record an ACL operation span
#[inline]
pub fn record_acl_span(operation: &str, principal: &str, resource: &str, allowed: bool) {
    tracing::info_span!(
        "acl",
        otel.kind = "server",
        streamline.acl.principal = %principal,
        streamline.acl.resource = %resource,
        streamline.acl.allowed = %allowed,
        streamline.operation = %operation,
    );
}

/// Context for propagating trace information
///
/// Used to propagate trace context across service boundaries
/// (e.g., in Kafka message headers).
#[derive(Debug, Clone, Default)]
pub struct TraceContext {
    /// Trace ID (32 hex chars)
    pub trace_id: Option<String>,
    /// Span ID (16 hex chars)
    pub span_id: Option<String>,
    /// Trace flags
    pub trace_flags: Option<u8>,
    /// Trace state (vendor-specific key-value pairs)
    pub trace_state: Option<String>,
}

impl TraceContext {
    /// Create an empty trace context
    pub fn new() -> Self {
        Self::default()
    }

    /// Create trace context from W3C traceparent header format
    ///
    /// Format: `{version}-{trace_id}-{parent_id}-{flags}`
    /// Example: `00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01`
    pub fn from_traceparent(header: &str) -> Option<Self> {
        let parts: Vec<&str> = header.split('-').collect();
        if parts.len() != 4 {
            return None;
        }

        // Version must be 00
        if parts[0] != "00" {
            return None;
        }

        // Trace ID must be 32 hex chars
        if parts[1].len() != 32 || !parts[1].chars().all(|c| c.is_ascii_hexdigit()) {
            return None;
        }

        // Span ID must be 16 hex chars
        if parts[2].len() != 16 || !parts[2].chars().all(|c| c.is_ascii_hexdigit()) {
            return None;
        }

        // Flags must be 2 hex chars
        let flags = u8::from_str_radix(parts[3], 16).ok()?;

        Some(Self {
            trace_id: Some(parts[1].to_string()),
            span_id: Some(parts[2].to_string()),
            trace_flags: Some(flags),
            trace_state: None,
        })
    }

    /// Convert to W3C traceparent header format
    pub fn to_traceparent(&self) -> Option<String> {
        let trace_id = self.trace_id.as_ref()?;
        let span_id = self.span_id.as_ref()?;
        let flags = self.trace_flags.unwrap_or(0);

        Some(format!("00-{}-{}-{:02x}", trace_id, span_id, flags))
    }

    /// Set trace state from W3C tracestate header
    pub fn with_tracestate(mut self, tracestate: impl Into<String>) -> Self {
        self.trace_state = Some(tracestate.into());
        self
    }

    /// Check if this context is sampled (should be recorded)
    pub fn is_sampled(&self) -> bool {
        self.trace_flags.map(|f| f & 0x01 != 0).unwrap_or(false)
    }
}

/// Extract trace context from Kafka message headers
pub fn extract_trace_context(headers: &[(String, Vec<u8>)]) -> Option<TraceContext> {
    for (key, value) in headers {
        if key.eq_ignore_ascii_case("traceparent") {
            if let Ok(s) = std::str::from_utf8(value) {
                let mut ctx = TraceContext::from_traceparent(s)?;

                // Also look for tracestate
                for (k, v) in headers {
                    if k.eq_ignore_ascii_case("tracestate") {
                        if let Ok(state) = std::str::from_utf8(v) {
                            ctx.trace_state = Some(state.to_string());
                        }
                        break;
                    }
                }

                return Some(ctx);
            }
        }
    }
    None
}

/// Inject trace context into Kafka message headers
pub fn inject_trace_context(ctx: &TraceContext, headers: &mut Vec<(String, Vec<u8>)>) {
    if let Some(traceparent) = ctx.to_traceparent() {
        // Remove existing traceparent if present
        headers.retain(|(k, _)| !k.eq_ignore_ascii_case("traceparent"));
        headers.push(("traceparent".to_string(), traceparent.into_bytes()));
    }

    if let Some(ref tracestate) = ctx.trace_state {
        headers.retain(|(k, _)| !k.eq_ignore_ascii_case("tracestate"));
        headers.push(("tracestate".to_string(), tracestate.clone().into_bytes()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tracing_config_default() {
        let config = TracingConfig::default();
        assert_eq!(config.service_name, "streamline");
        assert!(!config.enabled);
        assert!(config.otlp_endpoint.is_none());
        assert_eq!(config.sampling_ratio, 1.0);
    }

    #[test]
    fn test_tracing_config_builder() {
        let config = TracingConfig::builder()
            .service_name("test-service")
            .service_version("1.0.0")
            .otlp_endpoint("http://localhost:4317")
            .enabled(true)
            .sampling_ratio(0.5)
            .export_timeout_secs(30)
            .build();

        assert_eq!(config.service_name, "test-service");
        assert_eq!(config.service_version, "1.0.0");
        assert_eq!(
            config.otlp_endpoint,
            Some("http://localhost:4317".to_string())
        );
        assert!(config.enabled);
        assert_eq!(config.sampling_ratio, 0.5);
        assert_eq!(config.export_timeout_secs, 30);
    }

    #[test]
    fn test_sampling_ratio_clamping() {
        let config = TracingConfig::builder().sampling_ratio(1.5).build();
        assert_eq!(config.sampling_ratio, 1.0);

        let config = TracingConfig::builder().sampling_ratio(-0.5).build();
        assert_eq!(config.sampling_ratio, 0.0);
    }

    #[test]
    fn test_tracing_config_from_env() {
        // Just test that it doesn't panic
        let config = TracingConfig::from_env();
        assert!(!config.service_name.is_empty());
    }

    #[test]
    fn test_trace_context_from_traceparent() {
        let header = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        let ctx = TraceContext::from_traceparent(header).unwrap();

        assert_eq!(
            ctx.trace_id,
            Some("4bf92f3577b34da6a3ce929d0e0e4736".to_string())
        );
        assert_eq!(ctx.span_id, Some("00f067aa0ba902b7".to_string()));
        assert_eq!(ctx.trace_flags, Some(0x01));
        assert!(ctx.is_sampled());
    }

    #[test]
    fn test_trace_context_to_traceparent() {
        let ctx = TraceContext {
            trace_id: Some("4bf92f3577b34da6a3ce929d0e0e4736".to_string()),
            span_id: Some("00f067aa0ba902b7".to_string()),
            trace_flags: Some(0x01),
            trace_state: None,
        };

        let header = ctx.to_traceparent().unwrap();
        assert_eq!(
            header,
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        );
    }

    #[test]
    fn test_trace_context_invalid_traceparent() {
        // Invalid version
        assert!(TraceContext::from_traceparent(
            "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        )
        .is_none());

        // Wrong number of parts
        assert!(TraceContext::from_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-01").is_none());

        // Invalid trace ID length
        assert!(TraceContext::from_traceparent("00-4bf92f35-00f067aa0ba902b7-01").is_none());

        // Invalid span ID length
        assert!(
            TraceContext::from_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f0-01").is_none()
        );
    }

    #[test]
    fn test_trace_context_not_sampled() {
        let ctx = TraceContext {
            trace_id: Some("4bf92f3577b34da6a3ce929d0e0e4736".to_string()),
            span_id: Some("00f067aa0ba902b7".to_string()),
            trace_flags: Some(0x00),
            trace_state: None,
        };

        assert!(!ctx.is_sampled());
    }

    #[test]
    fn test_extract_trace_context_from_headers() {
        let headers = vec![
            (
                "traceparent".to_string(),
                b"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_vec(),
            ),
            ("tracestate".to_string(), b"vendor=data".to_vec()),
        ];

        let ctx = extract_trace_context(&headers).unwrap();
        assert_eq!(
            ctx.trace_id,
            Some("4bf92f3577b34da6a3ce929d0e0e4736".to_string())
        );
        assert_eq!(ctx.trace_state, Some("vendor=data".to_string()));
    }

    #[test]
    fn test_inject_trace_context_into_headers() {
        let ctx = TraceContext {
            trace_id: Some("4bf92f3577b34da6a3ce929d0e0e4736".to_string()),
            span_id: Some("00f067aa0ba902b7".to_string()),
            trace_flags: Some(0x01),
            trace_state: Some("vendor=data".to_string()),
        };

        let mut headers = vec![];
        inject_trace_context(&ctx, &mut headers);

        assert_eq!(headers.len(), 2);
        assert_eq!(headers[0].0, "traceparent");
        assert_eq!(headers[1].0, "tracestate");
    }

    #[test]
    fn test_trace_context_with_tracestate() {
        let ctx = TraceContext::new().with_tracestate("vendor=data,other=value");
        assert_eq!(ctx.trace_state, Some("vendor=data,other=value".to_string()));
    }
}
