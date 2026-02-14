//! Observability Pipeline Integration
//!
//! Provides comprehensive observability integration including:
//! - OpenTelemetry integration (metrics, traces, logs)
//! - Prometheus metrics export
//! - Log aggregation and forwarding
//! - Alerting rules and notifications
//! - Health check endpoints

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

pub mod alerts;
pub mod exporters;
pub mod logs;
pub mod metrics;
pub mod traces;

pub use alerts::{Alert, AlertManager, AlertRule};
pub use logs::{LogAggregator, LogEntry, LogLevel, LogQuery};
pub use metrics::{MetricsCollector, MetricsSnapshot};
pub use traces::{Span, SpanContext, SpanKind, TraceCollector, TraceSampler};

/// Observability pipeline configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObsPipelineConfig {
    /// Enable metrics collection
    pub metrics_enabled: bool,
    /// Enable distributed tracing
    pub tracing_enabled: bool,
    /// Enable log aggregation
    pub logs_enabled: bool,
    /// Enable alerting
    pub alerting_enabled: bool,
    /// Metrics collection interval
    pub metrics_interval: Duration,
    /// OTLP endpoint for exporting
    pub otlp_endpoint: Option<String>,
    /// Prometheus scrape endpoint
    pub prometheus_endpoint: Option<String>,
    /// Log retention days
    pub log_retention_days: u32,
    /// Maximum spans per trace
    pub max_spans_per_trace: usize,
    /// Sampling rate (0.0 to 1.0)
    pub sampling_rate: f64,
    /// Service name
    pub service_name: String,
    /// Service version
    pub service_version: String,
    /// Environment
    pub environment: String,
}

impl Default for ObsPipelineConfig {
    fn default() -> Self {
        Self {
            metrics_enabled: true,
            tracing_enabled: true,
            logs_enabled: true,
            alerting_enabled: true,
            metrics_interval: Duration::from_secs(15),
            otlp_endpoint: None,
            prometheus_endpoint: Some("0.0.0.0:9090".to_string()),
            log_retention_days: 7,
            max_spans_per_trace: 1000,
            sampling_rate: 1.0,
            service_name: "streamline".to_string(),
            service_version: env!("CARGO_PKG_VERSION").to_string(),
            environment: "development".to_string(),
        }
    }
}

impl ObsPipelineConfig {
    /// Create a production configuration
    pub fn production() -> Self {
        Self {
            metrics_enabled: true,
            tracing_enabled: true,
            logs_enabled: true,
            alerting_enabled: true,
            metrics_interval: Duration::from_secs(60),
            otlp_endpoint: None,
            prometheus_endpoint: Some("0.0.0.0:9090".to_string()),
            log_retention_days: 30,
            max_spans_per_trace: 10000,
            sampling_rate: 0.1, // 10% sampling in production
            service_name: "streamline".to_string(),
            service_version: env!("CARGO_PKG_VERSION").to_string(),
            environment: "production".to_string(),
        }
    }

    /// Set OTLP endpoint
    pub fn with_otlp(mut self, endpoint: impl Into<String>) -> Self {
        self.otlp_endpoint = Some(endpoint.into());
        self
    }

    /// Set service name
    pub fn with_service_name(mut self, name: impl Into<String>) -> Self {
        self.service_name = name.into();
        self
    }
}

/// Resource attributes for observability data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Resource {
    /// Service name
    pub service_name: String,
    /// Service version
    pub service_version: String,
    /// Environment
    pub environment: String,
    /// Host name
    pub host_name: String,
    /// Instance ID
    pub instance_id: String,
    /// Additional attributes
    pub attributes: HashMap<String, String>,
}

impl Resource {
    /// Create a new resource
    pub fn new(service_name: impl Into<String>) -> Self {
        Self {
            service_name: service_name.into(),
            service_version: String::new(),
            environment: "development".to_string(),
            host_name: std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".to_string()),
            instance_id: format!("{}", std::process::id()),
            attributes: HashMap::new(),
        }
    }

    /// Set version
    pub fn with_version(mut self, version: impl Into<String>) -> Self {
        self.service_version = version.into();
        self
    }

    /// Set environment
    pub fn with_environment(mut self, env: impl Into<String>) -> Self {
        self.environment = env.into();
        self
    }

    /// Add attribute
    pub fn with_attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.attributes.insert(key.into(), value.into());
        self
    }
}

/// Pipeline statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PipelineStats {
    /// Total metrics collected
    pub metrics_collected: u64,
    /// Total spans collected
    pub spans_collected: u64,
    /// Total logs collected
    pub logs_collected: u64,
    /// Total alerts triggered
    pub alerts_triggered: u64,
    /// Export successes
    pub export_successes: u64,
    /// Export failures
    pub export_failures: u64,
    /// Dropped items (due to capacity)
    pub dropped_items: u64,
    /// Last export time
    pub last_export_time: i64,
}

/// Health status for observability systems
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HealthStatus {
    /// System is healthy
    Healthy,
    /// System is degraded but operational
    Degraded,
    /// System is unhealthy
    Unhealthy,
}

/// Health check result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheck {
    /// Overall status
    pub status: HealthStatus,
    /// Component statuses
    pub components: HashMap<String, ComponentHealth>,
    /// Check timestamp
    pub timestamp: i64,
    /// Duration in milliseconds
    pub duration_ms: u64,
}

/// Component health status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentHealth {
    /// Status
    pub status: HealthStatus,
    /// Message
    pub message: String,
    /// Last check time
    pub last_check: i64,
}

/// Observability pipeline manager
pub struct ObsPipeline {
    config: ObsPipelineConfig,
    resource: Resource,
    metrics_collector: Arc<RwLock<MetricsCollector>>,
    trace_collector: Arc<RwLock<TraceCollector>>,
    log_aggregator: Arc<RwLock<LogAggregator>>,
    alert_manager: Arc<RwLock<AlertManager>>,
    stats: Arc<PipelineStatsAtomic>,
}

/// Atomic statistics for thread-safe updates
struct PipelineStatsAtomic {
    metrics_collected: AtomicU64,
    spans_collected: AtomicU64,
    logs_collected: AtomicU64,
    alerts_triggered: AtomicU64,
    export_successes: AtomicU64,
    export_failures: AtomicU64,
    dropped_items: AtomicU64,
}

impl Default for PipelineStatsAtomic {
    fn default() -> Self {
        Self {
            metrics_collected: AtomicU64::new(0),
            spans_collected: AtomicU64::new(0),
            logs_collected: AtomicU64::new(0),
            alerts_triggered: AtomicU64::new(0),
            export_successes: AtomicU64::new(0),
            export_failures: AtomicU64::new(0),
            dropped_items: AtomicU64::new(0),
        }
    }
}

impl ObsPipeline {
    /// Create a new observability pipeline
    pub fn new(config: ObsPipelineConfig) -> Self {
        let resource = Resource::new(&config.service_name)
            .with_version(&config.service_version)
            .with_environment(&config.environment);

        let sampler = TraceSampler::new(config.sampling_rate);

        Self {
            config: config.clone(),
            resource,
            metrics_collector: Arc::new(RwLock::new(MetricsCollector::new())),
            trace_collector: Arc::new(RwLock::new(TraceCollector::new(sampler))),
            log_aggregator: Arc::new(RwLock::new(LogAggregator::new(config.log_retention_days))),
            alert_manager: Arc::new(RwLock::new(AlertManager::new())),
            stats: Arc::new(PipelineStatsAtomic::default()),
        }
    }

    /// Get configuration
    pub fn config(&self) -> &ObsPipelineConfig {
        &self.config
    }

    /// Get resource
    pub fn resource(&self) -> &Resource {
        &self.resource
    }

    // === Metrics API ===

    /// Record a counter increment
    pub async fn counter(&self, name: &str, value: u64, labels: &[(&str, &str)]) {
        if !self.config.metrics_enabled {
            return;
        }

        let mut collector = self.metrics_collector.write().await;
        collector.record_counter(name, value, labels);
        self.stats.metrics_collected.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a gauge value
    pub async fn gauge(&self, name: &str, value: f64, labels: &[(&str, &str)]) {
        if !self.config.metrics_enabled {
            return;
        }

        let mut collector = self.metrics_collector.write().await;
        collector.record_gauge(name, value, labels);
        self.stats.metrics_collected.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a histogram observation
    pub async fn histogram(&self, name: &str, value: f64, labels: &[(&str, &str)]) {
        if !self.config.metrics_enabled {
            return;
        }

        let mut collector = self.metrics_collector.write().await;
        collector.record_histogram(name, value, labels);
        self.stats.metrics_collected.fetch_add(1, Ordering::Relaxed);
    }

    /// Get metrics snapshot
    pub async fn metrics_snapshot(&self) -> MetricsSnapshot {
        let collector = self.metrics_collector.read().await;
        collector.snapshot()
    }

    // === Tracing API ===

    /// Start a new span
    pub async fn start_span(&self, name: &str, kind: SpanKind) -> Option<Span> {
        if !self.config.tracing_enabled {
            return None;
        }

        let mut collector = self.trace_collector.write().await;
        let span = collector.start_span(name, kind);
        if span.is_some() {
            self.stats.spans_collected.fetch_add(1, Ordering::Relaxed);
        }
        span
    }

    /// Start a child span
    pub async fn start_child_span(&self, name: &str, parent: &SpanContext) -> Option<Span> {
        if !self.config.tracing_enabled {
            return None;
        }

        let mut collector = self.trace_collector.write().await;
        let span = collector.start_child_span(name, parent);
        if span.is_some() {
            self.stats.spans_collected.fetch_add(1, Ordering::Relaxed);
        }
        span
    }

    /// End a span
    pub async fn end_span(&self, span: Span) {
        if !self.config.tracing_enabled {
            return;
        }

        let mut collector = self.trace_collector.write().await;
        collector.end_span(span);
    }

    // === Logging API ===

    /// Log an entry
    pub async fn log(&self, level: LogLevel, message: &str, attributes: HashMap<String, String>) {
        if !self.config.logs_enabled {
            return;
        }

        let entry = LogEntry::new(level, message).with_attributes(attributes);

        let mut aggregator = self.log_aggregator.write().await;
        aggregator.add(entry);
        self.stats.logs_collected.fetch_add(1, Ordering::Relaxed);
    }

    /// Log info level
    pub async fn info(&self, message: &str) {
        self.log(LogLevel::Info, message, HashMap::new()).await;
    }

    /// Log warning level
    pub async fn warn(&self, message: &str) {
        self.log(LogLevel::Warn, message, HashMap::new()).await;
    }

    /// Log error level
    pub async fn error(&self, message: &str) {
        self.log(LogLevel::Error, message, HashMap::new()).await;
    }

    /// Query logs
    pub async fn query_logs(&self, query: LogQuery) -> Vec<LogEntry> {
        let aggregator = self.log_aggregator.read().await;
        aggregator.query(&query)
    }

    // === Alerting API ===

    /// Add an alert rule
    pub async fn add_alert_rule(&self, rule: AlertRule) {
        let mut manager = self.alert_manager.write().await;
        manager.add_rule(rule);
    }

    /// Evaluate alerts against current metrics
    pub async fn evaluate_alerts(&self) {
        if !self.config.alerting_enabled {
            return;
        }

        let snapshot = self.metrics_snapshot().await;
        let mut manager = self.alert_manager.write().await;
        let alerts = manager.evaluate(&snapshot);

        self.stats
            .alerts_triggered
            .fetch_add(alerts.len() as u64, Ordering::Relaxed);
    }

    /// Get active alerts
    pub async fn active_alerts(&self) -> Vec<Alert> {
        let manager = self.alert_manager.read().await;
        manager.active_alerts()
    }

    // === Health Check ===

    /// Perform health check
    pub async fn health_check(&self) -> HealthCheck {
        let start = std::time::Instant::now();
        let mut components = HashMap::new();
        let mut overall_status = HealthStatus::Healthy;

        // Check metrics collector
        let metrics_health = if self.config.metrics_enabled {
            let collector = self.metrics_collector.read().await;
            let count = collector.metric_count();
            ComponentHealth {
                status: HealthStatus::Healthy,
                message: if count > 0 {
                    format!("{} metrics registered", count)
                } else {
                    "Metrics enabled, awaiting data".to_string()
                },
                last_check: chrono::Utc::now().timestamp_millis(),
            }
        } else {
            ComponentHealth {
                status: HealthStatus::Healthy,
                message: "Metrics disabled".to_string(),
                last_check: chrono::Utc::now().timestamp_millis(),
            }
        };
        components.insert("metrics".to_string(), metrics_health);

        // Check trace collector
        let traces_health = if self.config.tracing_enabled {
            ComponentHealth {
                status: HealthStatus::Healthy,
                message: format!("Sampling rate: {:.1}%", self.config.sampling_rate * 100.0),
                last_check: chrono::Utc::now().timestamp_millis(),
            }
        } else {
            ComponentHealth {
                status: HealthStatus::Healthy,
                message: "Tracing disabled".to_string(),
                last_check: chrono::Utc::now().timestamp_millis(),
            }
        };
        components.insert("tracing".to_string(), traces_health);

        // Check log aggregator
        let logs_health = if self.config.logs_enabled {
            let aggregator = self.log_aggregator.read().await;
            ComponentHealth {
                status: HealthStatus::Healthy,
                message: format!("{} logs in buffer", aggregator.count()),
                last_check: chrono::Utc::now().timestamp_millis(),
            }
        } else {
            ComponentHealth {
                status: HealthStatus::Healthy,
                message: "Logging disabled".to_string(),
                last_check: chrono::Utc::now().timestamp_millis(),
            }
        };
        components.insert("logs".to_string(), logs_health);

        // Check alert manager
        let alerts_health = if self.config.alerting_enabled {
            let manager = self.alert_manager.read().await;
            let active = manager.active_alerts().len();
            let status = if active > 0 {
                HealthStatus::Degraded
            } else {
                HealthStatus::Healthy
            };
            if status == HealthStatus::Degraded && overall_status == HealthStatus::Healthy {
                overall_status = HealthStatus::Degraded;
            }
            ComponentHealth {
                status,
                message: format!("{} active alerts", active),
                last_check: chrono::Utc::now().timestamp_millis(),
            }
        } else {
            ComponentHealth {
                status: HealthStatus::Healthy,
                message: "Alerting disabled".to_string(),
                last_check: chrono::Utc::now().timestamp_millis(),
            }
        };
        components.insert("alerts".to_string(), alerts_health);

        HealthCheck {
            status: overall_status,
            components,
            timestamp: chrono::Utc::now().timestamp_millis(),
            duration_ms: start.elapsed().as_millis() as u64,
        }
    }

    /// Get statistics
    pub fn stats(&self) -> PipelineStats {
        PipelineStats {
            metrics_collected: self.stats.metrics_collected.load(Ordering::Relaxed),
            spans_collected: self.stats.spans_collected.load(Ordering::Relaxed),
            logs_collected: self.stats.logs_collected.load(Ordering::Relaxed),
            alerts_triggered: self.stats.alerts_triggered.load(Ordering::Relaxed),
            export_successes: self.stats.export_successes.load(Ordering::Relaxed),
            export_failures: self.stats.export_failures.load(Ordering::Relaxed),
            dropped_items: self.stats.dropped_items.load(Ordering::Relaxed),
            last_export_time: 0, // Would be updated by export operations
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = ObsPipelineConfig::default();

        assert!(config.metrics_enabled);
        assert!(config.tracing_enabled);
        assert!(config.logs_enabled);
        assert_eq!(config.sampling_rate, 1.0);
    }

    #[test]
    fn test_config_production() {
        let config = ObsPipelineConfig::production();

        assert_eq!(config.environment, "production");
        assert_eq!(config.sampling_rate, 0.1);
        assert_eq!(config.log_retention_days, 30);
    }

    #[test]
    fn test_resource_creation() {
        let resource = Resource::new("test-service")
            .with_version("1.0.0")
            .with_environment("staging");

        assert_eq!(resource.service_name, "test-service");
        assert_eq!(resource.service_version, "1.0.0");
        assert_eq!(resource.environment, "staging");
    }

    #[tokio::test]
    async fn test_pipeline_creation() {
        let config = ObsPipelineConfig::default();
        let pipeline = ObsPipeline::new(config);

        assert!(pipeline.config().metrics_enabled);
        assert!(pipeline.config().tracing_enabled);
    }

    #[tokio::test]
    async fn test_counter_recording() {
        let config = ObsPipelineConfig::default();
        let pipeline = ObsPipeline::new(config);

        pipeline
            .counter("requests_total", 1, &[("method", "GET")])
            .await;

        let stats = pipeline.stats();
        assert_eq!(stats.metrics_collected, 1);
    }

    #[tokio::test]
    async fn test_gauge_recording() {
        let config = ObsPipelineConfig::default();
        let pipeline = ObsPipeline::new(config);

        pipeline
            .gauge("temperature", 23.5, &[("location", "server1")])
            .await;

        let stats = pipeline.stats();
        assert_eq!(stats.metrics_collected, 1);
    }

    #[tokio::test]
    async fn test_logging() {
        let config = ObsPipelineConfig::default();
        let pipeline = ObsPipeline::new(config);

        pipeline.info("Test info message").await;
        pipeline.warn("Test warning").await;
        pipeline.error("Test error").await;

        let stats = pipeline.stats();
        assert_eq!(stats.logs_collected, 3);
    }

    #[tokio::test]
    async fn test_health_check() {
        let config = ObsPipelineConfig::default();
        let pipeline = ObsPipeline::new(config);

        let health = pipeline.health_check().await;

        assert_eq!(health.status, HealthStatus::Healthy);
        assert!(health.components.contains_key("metrics"));
        assert!(health.components.contains_key("tracing"));
        assert!(health.components.contains_key("logs"));
        assert!(health.components.contains_key("alerts"));
    }
}
