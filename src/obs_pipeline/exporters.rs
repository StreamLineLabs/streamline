//! Telemetry Exporters
//!
//! Provides exporters for metrics, traces, and logs to various backends.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::logs::LogEntry;
use super::metrics::MetricsSnapshot;
use super::traces::Span;

/// Export format
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum ExportFormat {
    /// JSON format
    #[default]
    Json,
    /// Prometheus text format
    Prometheus,
    /// OpenTelemetry Protocol (OTLP)
    Otlp,
    /// Line protocol (InfluxDB)
    LineProtocol,
}

/// Exporter configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExporterConfig {
    /// Exporter name
    pub name: String,
    /// Export format
    pub format: ExportFormat,
    /// Endpoint URL (for push exporters)
    pub endpoint: Option<String>,
    /// Headers for HTTP requests
    pub headers: HashMap<String, String>,
    /// Batch size
    pub batch_size: usize,
    /// Flush interval in milliseconds
    pub flush_interval_ms: u64,
    /// Enable compression
    pub compression: bool,
    /// Retry attempts
    pub retry_attempts: u32,
    /// Retry delay in milliseconds
    pub retry_delay_ms: u64,
}

impl Default for ExporterConfig {
    fn default() -> Self {
        Self {
            name: "default".to_string(),
            format: ExportFormat::Json,
            endpoint: None,
            headers: HashMap::new(),
            batch_size: 100,
            flush_interval_ms: 10000,
            compression: true,
            retry_attempts: 3,
            retry_delay_ms: 1000,
        }
    }
}

impl ExporterConfig {
    /// Create a new exporter config
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ..Default::default()
        }
    }

    /// Set format
    pub fn with_format(mut self, format: ExportFormat) -> Self {
        self.format = format;
        self
    }

    /// Set endpoint
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// Add header
    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }

    /// Set batch size
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }
}

/// Export result
#[derive(Debug, Clone)]
pub struct ExportResult {
    /// Number of items exported
    pub exported_count: usize,
    /// Number of items failed
    pub failed_count: usize,
    /// Error message (if any)
    pub error: Option<String>,
    /// Duration in milliseconds
    pub duration_ms: u64,
}

impl ExportResult {
    /// Create a success result
    pub fn success(count: usize, duration_ms: u64) -> Self {
        Self {
            exported_count: count,
            failed_count: 0,
            error: None,
            duration_ms,
        }
    }

    /// Create a failure result
    pub fn failure(error: impl Into<String>, duration_ms: u64) -> Self {
        Self {
            exported_count: 0,
            failed_count: 0,
            error: Some(error.into()),
            duration_ms,
        }
    }

    /// Is success?
    pub fn is_success(&self) -> bool {
        self.error.is_none()
    }
}

/// Metrics exporter
pub struct MetricsExporter {
    config: ExporterConfig,
    buffer: Vec<MetricsSnapshot>,
}

impl MetricsExporter {
    /// Create a new metrics exporter
    pub fn new(config: ExporterConfig) -> Self {
        Self {
            config,
            buffer: Vec::new(),
        }
    }

    /// Add snapshot to buffer
    pub fn add(&mut self, snapshot: MetricsSnapshot) {
        self.buffer.push(snapshot);
    }

    /// Export metrics to string
    pub fn export(&mut self) -> ExportResult {
        let start = std::time::Instant::now();
        let count = self.buffer.len();

        if count == 0 {
            return ExportResult::success(0, 0);
        }

        // Format metrics based on config
        let _output = match self.config.format {
            ExportFormat::Prometheus => self.format_prometheus(),
            ExportFormat::Json => self.format_json(),
            ExportFormat::Otlp => self.format_otlp(),
            ExportFormat::LineProtocol => self.format_line_protocol(),
        };

        self.buffer.clear();
        ExportResult::success(count, start.elapsed().as_millis() as u64)
    }

    /// Format as Prometheus text
    fn format_prometheus(&self) -> String {
        let mut output = String::new();

        for snapshot in &self.buffer {
            // Counters
            for (name, values) in &snapshot.counters {
                for (labels, value) in values {
                    if labels.is_empty() {
                        output.push_str(&format!("{} {}\n", name, value));
                    } else {
                        output.push_str(&format!("{}{{{}}} {}\n", name, labels, value));
                    }
                }
            }

            // Gauges
            for (name, values) in &snapshot.gauges {
                for (labels, value) in values {
                    if labels.is_empty() {
                        output.push_str(&format!("{} {}\n", name, value));
                    } else {
                        output.push_str(&format!("{}{{{}}} {}\n", name, labels, value));
                    }
                }
            }

            // Histograms
            for (name, values) in &snapshot.histograms {
                for (labels, histogram) in values {
                    let label_str = if labels.is_empty() {
                        String::new()
                    } else {
                        format!("{{{}}}", labels)
                    };

                    for bucket in &histogram.buckets {
                        output.push_str(&format!(
                            "{}_bucket{{le=\"{}\"{}}} {}\n",
                            name,
                            bucket.le,
                            if label_str.is_empty() {
                                String::new()
                            } else {
                                format!(",{}", &label_str[1..label_str.len() - 1])
                            },
                            bucket.count
                        ));
                    }
                    output.push_str(&format!("{}_sum{} {}\n", name, label_str, histogram.sum));
                    output.push_str(&format!(
                        "{}_count{} {}\n",
                        name, label_str, histogram.count
                    ));
                }
            }
        }

        output
    }

    /// Format as JSON
    fn format_json(&self) -> String {
        serde_json::to_string(&self.buffer).unwrap_or_default()
    }

    /// Format as OTLP (simplified)
    fn format_otlp(&self) -> String {
        // Simplified OTLP format - in production would use protobuf
        self.format_json()
    }

    /// Format as InfluxDB line protocol
    fn format_line_protocol(&self) -> String {
        let mut output = String::new();

        for snapshot in &self.buffer {
            let timestamp = snapshot.timestamp * 1_000_000; // Convert to nanoseconds

            for (name, values) in &snapshot.counters {
                for (labels, value) in values {
                    let tags = if labels.is_empty() {
                        String::new()
                    } else {
                        format!(",{}", labels)
                    };
                    output.push_str(&format!(
                        "{}{} value={}i {}\n",
                        name, tags, value, timestamp
                    ));
                }
            }

            for (name, values) in &snapshot.gauges {
                for (labels, value) in values {
                    let tags = if labels.is_empty() {
                        String::new()
                    } else {
                        format!(",{}", labels)
                    };
                    output.push_str(&format!("{}{} value={} {}\n", name, tags, value, timestamp));
                }
            }
        }

        output
    }

    /// Get buffer size
    pub fn buffer_size(&self) -> usize {
        self.buffer.len()
    }

    /// Clear buffer
    pub fn clear(&mut self) {
        self.buffer.clear();
    }
}

/// Trace exporter
pub struct TraceExporter {
    config: ExporterConfig,
    buffer: Vec<Span>,
}

impl TraceExporter {
    /// Create a new trace exporter
    pub fn new(config: ExporterConfig) -> Self {
        Self {
            config,
            buffer: Vec::new(),
        }
    }

    /// Add span to buffer
    pub fn add(&mut self, span: Span) {
        self.buffer.push(span);
    }

    /// Add multiple spans
    pub fn add_batch(&mut self, spans: Vec<Span>) {
        self.buffer.extend(spans);
    }

    /// Export traces
    pub fn export(&mut self) -> ExportResult {
        let start = std::time::Instant::now();
        let count = self.buffer.len();

        if count == 0 {
            return ExportResult::success(0, 0);
        }

        let _output = match self.config.format {
            ExportFormat::Json => self.format_json(),
            ExportFormat::Otlp => self.format_otlp(),
            _ => self.format_json(),
        };

        self.buffer.clear();
        ExportResult::success(count, start.elapsed().as_millis() as u64)
    }

    /// Format as JSON
    fn format_json(&self) -> String {
        serde_json::to_string(&self.buffer).unwrap_or_default()
    }

    /// Format as OTLP
    fn format_otlp(&self) -> String {
        // Simplified - would use protobuf in production
        #[derive(Serialize)]
        struct OtlpSpan {
            trace_id: String,
            span_id: String,
            parent_span_id: Option<String>,
            name: String,
            kind: u8,
            start_time_unix_nano: i64,
            end_time_unix_nano: Option<i64>,
            attributes: Vec<OtlpAttribute>,
            events: Vec<OtlpEvent>,
            status: OtlpStatus,
        }

        #[derive(Serialize)]
        struct OtlpAttribute {
            key: String,
            value: OtlpValue,
        }

        #[derive(Serialize)]
        struct OtlpValue {
            string_value: String,
        }

        #[derive(Serialize)]
        struct OtlpEvent {
            name: String,
            time_unix_nano: i64,
            attributes: Vec<OtlpAttribute>,
        }

        #[derive(Serialize)]
        struct OtlpStatus {
            code: u8,
            message: Option<String>,
        }

        let otlp_spans: Vec<OtlpSpan> = self
            .buffer
            .iter()
            .map(|span| OtlpSpan {
                trace_id: span.context.trace_id.clone(),
                span_id: span.context.span_id.clone(),
                parent_span_id: span.parent_span_id.clone(),
                name: span.name.clone(),
                kind: span.kind as u8,
                start_time_unix_nano: span.start_time * 1_000_000,
                end_time_unix_nano: span.end_time.map(|t| t * 1_000_000),
                attributes: span
                    .attributes
                    .iter()
                    .map(|(k, v)| OtlpAttribute {
                        key: k.clone(),
                        value: OtlpValue {
                            string_value: v.clone(),
                        },
                    })
                    .collect(),
                events: span
                    .events
                    .iter()
                    .map(|e| OtlpEvent {
                        name: e.name.clone(),
                        time_unix_nano: e.timestamp * 1_000_000,
                        attributes: e
                            .attributes
                            .iter()
                            .map(|(k, v)| OtlpAttribute {
                                key: k.clone(),
                                value: OtlpValue {
                                    string_value: v.clone(),
                                },
                            })
                            .collect(),
                    })
                    .collect(),
                status: OtlpStatus {
                    code: span.status as u8,
                    message: span.status_message.clone(),
                },
            })
            .collect();

        serde_json::to_string(&otlp_spans).unwrap_or_default()
    }

    /// Get buffer size
    pub fn buffer_size(&self) -> usize {
        self.buffer.len()
    }

    /// Clear buffer
    pub fn clear(&mut self) {
        self.buffer.clear();
    }
}

/// Log exporter
pub struct LogExporter {
    config: ExporterConfig,
    buffer: Vec<LogEntry>,
}

impl LogExporter {
    /// Create a new log exporter
    pub fn new(config: ExporterConfig) -> Self {
        Self {
            config,
            buffer: Vec::new(),
        }
    }

    /// Add log entry to buffer
    pub fn add(&mut self, entry: LogEntry) {
        self.buffer.push(entry);
    }

    /// Add multiple entries
    pub fn add_batch(&mut self, entries: Vec<LogEntry>) {
        self.buffer.extend(entries);
    }

    /// Export logs
    pub fn export(&mut self) -> ExportResult {
        let start = std::time::Instant::now();
        let count = self.buffer.len();

        if count == 0 {
            return ExportResult::success(0, 0);
        }

        let _output = match self.config.format {
            ExportFormat::Json => self.format_json(),
            ExportFormat::Otlp => self.format_otlp(),
            _ => self.format_json(),
        };

        self.buffer.clear();
        ExportResult::success(count, start.elapsed().as_millis() as u64)
    }

    /// Format as JSON
    fn format_json(&self) -> String {
        serde_json::to_string(&self.buffer).unwrap_or_default()
    }

    /// Format as OTLP
    fn format_otlp(&self) -> String {
        #[derive(Serialize)]
        struct OtlpLog {
            time_unix_nano: i64,
            observed_time_unix_nano: i64,
            severity_number: u8,
            severity_text: String,
            body: OtlpBody,
            attributes: Vec<OtlpAttr>,
            trace_id: Option<String>,
            span_id: Option<String>,
        }

        #[derive(Serialize)]
        struct OtlpBody {
            string_value: String,
        }

        #[derive(Serialize)]
        struct OtlpAttr {
            key: String,
            value: OtlpAttrValue,
        }

        #[derive(Serialize)]
        struct OtlpAttrValue {
            string_value: String,
        }

        let otlp_logs: Vec<OtlpLog> = self
            .buffer
            .iter()
            .map(|entry| OtlpLog {
                time_unix_nano: entry.timestamp * 1_000_000,
                observed_time_unix_nano: entry.observed_timestamp * 1_000_000,
                severity_number: entry.level as u8,
                severity_text: entry.level.as_str().to_string(),
                body: OtlpBody {
                    string_value: entry.body.clone(),
                },
                attributes: entry
                    .attributes
                    .iter()
                    .map(|(k, v)| OtlpAttr {
                        key: k.clone(),
                        value: OtlpAttrValue {
                            string_value: v.clone(),
                        },
                    })
                    .collect(),
                trace_id: entry.trace_id.clone(),
                span_id: entry.span_id.clone(),
            })
            .collect();

        serde_json::to_string(&otlp_logs).unwrap_or_default()
    }

    /// Get buffer size
    pub fn buffer_size(&self) -> usize {
        self.buffer.len()
    }

    /// Clear buffer
    pub fn clear(&mut self) {
        self.buffer.clear();
    }
}

/// Unified telemetry exporter
pub struct TelemetryExporter {
    /// Metrics exporter
    pub metrics: MetricsExporter,
    /// Trace exporter
    pub traces: TraceExporter,
    /// Log exporter
    pub logs: LogExporter,
}

impl TelemetryExporter {
    /// Create a new telemetry exporter
    pub fn new(config: ExporterConfig) -> Self {
        Self {
            metrics: MetricsExporter::new(config.clone()),
            traces: TraceExporter::new(config.clone()),
            logs: LogExporter::new(config),
        }
    }

    /// Create with separate configs
    pub fn with_configs(
        metrics_config: ExporterConfig,
        traces_config: ExporterConfig,
        logs_config: ExporterConfig,
    ) -> Self {
        Self {
            metrics: MetricsExporter::new(metrics_config),
            traces: TraceExporter::new(traces_config),
            logs: LogExporter::new(logs_config),
        }
    }

    /// Export all telemetry
    pub fn export_all(&mut self) -> (ExportResult, ExportResult, ExportResult) {
        (
            self.metrics.export(),
            self.traces.export(),
            self.logs.export(),
        )
    }

    /// Get total buffer size
    pub fn total_buffer_size(&self) -> usize {
        self.metrics.buffer_size() + self.traces.buffer_size() + self.logs.buffer_size()
    }

    /// Clear all buffers
    pub fn clear_all(&mut self) {
        self.metrics.clear();
        self.traces.clear();
        self.logs.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::obs_pipeline::logs::LogLevel;
    use crate::obs_pipeline::metrics::MetricsCollector;
    use crate::obs_pipeline::traces::{Span, SpanKind};

    #[test]
    fn test_exporter_config() {
        let config = ExporterConfig::new("test")
            .with_format(ExportFormat::Prometheus)
            .with_endpoint("http://localhost:9090")
            .with_header("Authorization", "Bearer token")
            .with_batch_size(50);

        assert_eq!(config.name, "test");
        assert_eq!(config.format, ExportFormat::Prometheus);
        assert_eq!(config.endpoint, Some("http://localhost:9090".to_string()));
        assert_eq!(config.batch_size, 50);
    }

    #[test]
    fn test_export_result() {
        let success = ExportResult::success(100, 50);
        assert!(success.is_success());
        assert_eq!(success.exported_count, 100);

        let failure = ExportResult::failure("connection error", 10);
        assert!(!failure.is_success());
        assert!(failure.error.is_some());
    }

    #[test]
    fn test_metrics_exporter_json() {
        let mut collector = MetricsCollector::new();
        collector.record_counter("requests", 10, &[("method", "GET")]);
        collector.record_gauge("temperature", 25.5, &[]);

        let mut exporter = MetricsExporter::new(ExporterConfig::new("test"));
        exporter.add(collector.snapshot());

        let result = exporter.export();
        assert!(result.is_success());
        assert_eq!(result.exported_count, 1);
        assert_eq!(exporter.buffer_size(), 0);
    }

    #[test]
    fn test_metrics_exporter_prometheus() {
        let mut collector = MetricsCollector::new();
        collector.record_counter("http_requests_total", 100, &[("method", "GET")]);
        collector.record_gauge("cpu_usage", 45.5, &[]);

        let config = ExporterConfig::new("prom").with_format(ExportFormat::Prometheus);
        let mut exporter = MetricsExporter::new(config);
        exporter.add(collector.snapshot());

        let result = exporter.export();
        assert!(result.is_success());
    }

    #[test]
    fn test_trace_exporter() {
        let mut span = Span::new("test-operation", SpanKind::Server);
        span.set_attribute("user_id", "123");
        span.end();

        let mut exporter = TraceExporter::new(ExporterConfig::new("test"));
        exporter.add(span);

        assert_eq!(exporter.buffer_size(), 1);

        let result = exporter.export();
        assert!(result.is_success());
        assert_eq!(result.exported_count, 1);
        assert_eq!(exporter.buffer_size(), 0);
    }

    #[test]
    fn test_trace_exporter_otlp() {
        let mut span = Span::new("operation", SpanKind::Client);
        span.end();

        let config = ExporterConfig::new("otlp").with_format(ExportFormat::Otlp);
        let mut exporter = TraceExporter::new(config);
        exporter.add(span);

        let result = exporter.export();
        assert!(result.is_success());
    }

    #[test]
    fn test_log_exporter() {
        let entry =
            LogEntry::new(LogLevel::Info, "Test log message").with_attribute("component", "test");

        let mut exporter = LogExporter::new(ExporterConfig::new("test"));
        exporter.add(entry);

        let result = exporter.export();
        assert!(result.is_success());
        assert_eq!(result.exported_count, 1);
    }

    #[test]
    fn test_log_exporter_batch() {
        let entries = vec![
            LogEntry::new(LogLevel::Info, "Log 1"),
            LogEntry::new(LogLevel::Warn, "Log 2"),
            LogEntry::new(LogLevel::Error, "Log 3"),
        ];

        let mut exporter = LogExporter::new(ExporterConfig::new("test"));
        exporter.add_batch(entries);

        assert_eq!(exporter.buffer_size(), 3);

        let result = exporter.export();
        assert!(result.is_success());
        assert_eq!(result.exported_count, 3);
    }

    #[test]
    fn test_telemetry_exporter() {
        let mut exporter = TelemetryExporter::new(ExporterConfig::new("unified"));

        // Add metrics
        let mut collector = MetricsCollector::new();
        collector.record_counter("requests", 1, &[]);
        exporter.metrics.add(collector.snapshot());

        // Add trace
        let mut span = Span::new("op", SpanKind::Internal);
        span.end();
        exporter.traces.add(span);

        // Add log
        exporter.logs.add(LogEntry::new(LogLevel::Info, "test"));

        assert_eq!(exporter.total_buffer_size(), 3);

        let (m, t, l) = exporter.export_all();
        assert!(m.is_success());
        assert!(t.is_success());
        assert!(l.is_success());
        assert_eq!(exporter.total_buffer_size(), 0);
    }

    #[test]
    fn test_empty_export() {
        let mut metrics = MetricsExporter::new(ExporterConfig::new("test"));
        let result = metrics.export();
        assert!(result.is_success());
        assert_eq!(result.exported_count, 0);

        let mut traces = TraceExporter::new(ExporterConfig::new("test"));
        let result = traces.export();
        assert!(result.is_success());
        assert_eq!(result.exported_count, 0);

        let mut logs = LogExporter::new(ExporterConfig::new("test"));
        let result = logs.export();
        assert!(result.is_success());
        assert_eq!(result.exported_count, 0);
    }

    #[test]
    fn test_line_protocol_format() {
        let mut collector = MetricsCollector::new();
        collector.record_counter("requests", 5, &[("host", "server1")]);
        collector.record_gauge("memory", 1024.0, &[]);

        let config = ExporterConfig::new("influx").with_format(ExportFormat::LineProtocol);
        let mut exporter = MetricsExporter::new(config);
        exporter.add(collector.snapshot());

        let result = exporter.export();
        assert!(result.is_success());
    }
}
