//! JMX-compatible metrics for Kafka monitoring compatibility
//!
//! This module provides metrics using Kafka's JMX naming conventions,
//! allowing existing Kafka monitoring dashboards and tools to work
//! with Streamline out of the box.
//!
//! ## JMX Metric Format
//!
//! Kafka JMX metrics follow the pattern:
//! `kafka.{domain}:type={type},name={name}[,topic={topic}][,partition={partition}]`
//!
//! In Prometheus format, this becomes:
//! `kafka_{domain}_{type}_{name}{labels}`
//!
//! ## Supported Metrics
//!
//! - `kafka.server:type=BrokerTopicMetrics` - Bytes/messages in/out
//! - `kafka.server:type=ReplicaManager` - ISR, under-replicated partitions
//! - `kafka.server:type=SessionExpireListener` - Connection metrics
//! - `kafka.controller:type=KafkaController` - Controller state
//! - `kafka.network:type=RequestMetrics` - Request latencies
//! - `kafka.log:type=LogFlushStats` - Log flush metrics
//! - `kafka.consumer:type=ConsumerFetcherManager` - Consumer lag

use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// JMX metrics manager for Kafka-compatible metrics
#[derive(Default)]
pub struct JmxMetrics {
    /// Marker for future cached metric values (currently unused)
    _marker: std::marker::PhantomData<()>,
}

/// A metric value with type information
#[derive(Clone, Debug)]
pub enum MetricValue {
    Counter(u64),
    Gauge(f64),
    Histogram {
        count: u64,
        sum: f64,
        mean: f64,
        min: f64,
        max: f64,
        p50: f64,
        p75: f64,
        p95: f64,
        p99: f64,
    },
}

impl JmxMetrics {
    /// Create a new JMX metrics manager
    pub fn new() -> Self {
        Self {
            _marker: std::marker::PhantomData,
        }
    }

    /// Initialize all JMX-compatible metrics with descriptions
    pub fn register_metrics(&self) {
        // BrokerTopicMetrics - bytes and messages
        describe_counter!(
            "kafka_server_broker_topic_metrics_bytes_in_total",
            "Bytes received per second (JMX: kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec)"
        );
        describe_counter!(
            "kafka_server_broker_topic_metrics_bytes_out_total",
            "Bytes sent per second (JMX: kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec)"
        );
        describe_counter!(
            "kafka_server_broker_topic_metrics_messages_in_total",
            "Messages received per second (JMX: kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec)"
        );
        describe_counter!(
            "kafka_server_broker_topic_metrics_bytes_rejected_total",
            "Bytes rejected per second (JMX: kafka.server:type=BrokerTopicMetrics,name=BytesRejectedPerSec)"
        );
        describe_counter!(
            "kafka_server_broker_topic_metrics_failed_fetch_requests_total",
            "Failed fetch requests (JMX: kafka.server:type=BrokerTopicMetrics,name=FailedFetchRequestsPerSec)"
        );
        describe_counter!(
            "kafka_server_broker_topic_metrics_failed_produce_requests_total",
            "Failed produce requests (JMX: kafka.server:type=BrokerTopicMetrics,name=FailedProduceRequestsPerSec)"
        );
        describe_counter!(
            "kafka_server_broker_topic_metrics_total_fetch_requests_total",
            "Total fetch requests (JMX: kafka.server:type=BrokerTopicMetrics,name=TotalFetchRequestsPerSec)"
        );
        describe_counter!(
            "kafka_server_broker_topic_metrics_total_produce_requests_total",
            "Total produce requests (JMX: kafka.server:type=BrokerTopicMetrics,name=TotalProduceRequestsPerSec)"
        );

        // ReplicaManager metrics
        describe_gauge!(
            "kafka_server_replica_manager_isr_shrinks_total",
            "ISR shrinks (JMX: kafka.server:type=ReplicaManager,name=IsrShrinksPerSec)"
        );
        describe_gauge!(
            "kafka_server_replica_manager_isr_expands_total",
            "ISR expands (JMX: kafka.server:type=ReplicaManager,name=IsrExpandsPerSec)"
        );
        describe_gauge!(
            "kafka_server_replica_manager_under_replicated_partitions",
            "Under-replicated partitions (JMX: kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions)"
        );
        describe_gauge!(
            "kafka_server_replica_manager_offline_replica_count",
            "Offline replica count (JMX: kafka.server:type=ReplicaManager,name=OfflineReplicaCount)"
        );
        describe_gauge!(
            "kafka_server_replica_manager_leader_count",
            "Leader count (JMX: kafka.server:type=ReplicaManager,name=LeaderCount)"
        );
        describe_gauge!(
            "kafka_server_replica_manager_partition_count",
            "Partition count (JMX: kafka.server:type=ReplicaManager,name=PartitionCount)"
        );

        // KafkaController metrics
        describe_gauge!(
            "kafka_controller_active_controller_count",
            "Active controller count (JMX: kafka.controller:type=KafkaController,name=ActiveControllerCount)"
        );
        describe_gauge!(
            "kafka_controller_offline_partitions_count",
            "Offline partitions count (JMX: kafka.controller:type=KafkaController,name=OfflinePartitionsCount)"
        );
        describe_gauge!(
            "kafka_controller_preferred_replica_imbalance_count",
            "Preferred replica imbalance (JMX: kafka.controller:type=KafkaController,name=PreferredReplicaImbalanceCount)"
        );
        describe_gauge!(
            "kafka_controller_global_topic_count",
            "Global topic count (JMX: kafka.controller:type=KafkaController,name=GlobalTopicCount)"
        );
        describe_gauge!(
            "kafka_controller_global_partition_count",
            "Global partition count (JMX: kafka.controller:type=KafkaController,name=GlobalPartitionCount)"
        );

        // Network RequestMetrics
        describe_histogram!(
            "kafka_network_request_metrics_total_time_ms",
            "Total request time (JMX: kafka.network:type=RequestMetrics,name=TotalTimeMs)"
        );
        describe_histogram!(
            "kafka_network_request_metrics_request_queue_time_ms",
            "Request queue time (JMX: kafka.network:type=RequestMetrics,name=RequestQueueTimeMs)"
        );
        describe_histogram!(
            "kafka_network_request_metrics_local_time_ms",
            "Local processing time (JMX: kafka.network:type=RequestMetrics,name=LocalTimeMs)"
        );
        describe_histogram!(
            "kafka_network_request_metrics_remote_time_ms",
            "Remote wait time (JMX: kafka.network:type=RequestMetrics,name=RemoteTimeMs)"
        );
        describe_histogram!(
            "kafka_network_request_metrics_response_queue_time_ms",
            "Response queue time (JMX: kafka.network:type=RequestMetrics,name=ResponseQueueTimeMs)"
        );
        describe_histogram!(
            "kafka_network_request_metrics_response_send_time_ms",
            "Response send time (JMX: kafka.network:type=RequestMetrics,name=ResponseSendTimeMs)"
        );
        describe_counter!(
            "kafka_network_request_metrics_requests_per_sec",
            "Requests per second (JMX: kafka.network:type=RequestMetrics,name=RequestsPerSec)"
        );

        // Socket server metrics
        describe_gauge!(
            "kafka_server_socket_server_network_processor_avg_idle_percent",
            "Network processor idle percent (JMX: kafka.server:type=socket-server-metrics)"
        );

        // Session metrics
        describe_gauge!(
            "kafka_server_session_expire_listener_zoo_keeper_expires_total",
            "ZooKeeper session expires (JMX: kafka.server:type=SessionExpireListener)"
        );

        // Log metrics
        describe_gauge!(
            "kafka_log_log_flush_rate_and_time_ms_count",
            "Log flush count (JMX: kafka.log:type=LogFlushRateAndTimeMs)"
        );
        describe_histogram!(
            "kafka_log_log_flush_rate_and_time_ms",
            "Log flush time (JMX: kafka.log:type=LogFlushRateAndTimeMs)"
        );
        describe_gauge!(
            "kafka_log_log_size_bytes",
            "Log size in bytes (JMX: kafka.log:type=Log,name=Size)"
        );
        describe_gauge!(
            "kafka_log_log_end_offset",
            "Log end offset (JMX: kafka.log:type=Log,name=LogEndOffset)"
        );
        describe_gauge!(
            "kafka_log_log_start_offset",
            "Log start offset (JMX: kafka.log:type=Log,name=LogStartOffset)"
        );
        describe_gauge!(
            "kafka_log_num_log_segments",
            "Number of log segments (JMX: kafka.log:type=Log,name=NumLogSegments)"
        );

        // Consumer metrics
        describe_gauge!(
            "kafka_consumer_consumer_lag",
            "Consumer lag (JMX: kafka.consumer:type=consumer-fetch-manager-metrics,client-id=*)"
        );
        describe_gauge!(
            "kafka_consumer_consumer_lag_max",
            "Max consumer lag (JMX: kafka.consumer:type=consumer-fetch-manager-metrics)"
        );
        describe_gauge!(
            "kafka_consumer_records_consumed_total",
            "Records consumed (JMX: kafka.consumer:type=consumer-fetch-manager-metrics)"
        );
        describe_gauge!(
            "kafka_consumer_bytes_consumed_total",
            "Bytes consumed (JMX: kafka.consumer:type=consumer-fetch-manager-metrics)"
        );

        // Producer metrics
        describe_gauge!(
            "kafka_producer_record_send_total",
            "Records sent (JMX: kafka.producer:type=producer-metrics)"
        );
        describe_gauge!(
            "kafka_producer_byte_total",
            "Bytes sent (JMX: kafka.producer:type=producer-metrics)"
        );
        describe_histogram!(
            "kafka_producer_request_latency_avg",
            "Average request latency (JMX: kafka.producer:type=producer-metrics)"
        );

        // JVM/process metrics (for compatibility, though we're Rust)
        describe_gauge!(
            "kafka_server_jvm_memory_heap_used",
            "Heap memory used (simulated for compatibility)"
        );
        describe_gauge!(
            "kafka_server_jvm_memory_heap_max",
            "Max heap memory (simulated for compatibility)"
        );
        describe_gauge!(
            "kafka_server_jvm_gc_collection_count",
            "GC collection count (simulated for compatibility)"
        );

        // Connection metrics
        describe_gauge!(
            "kafka_server_connections_active",
            "Active connections (JMX: kafka.server:type=socket-server-metrics)"
        );
        describe_counter!(
            "kafka_server_connections_total",
            "Total connections (JMX: kafka.server:type=socket-server-metrics)"
        );
    }
}

// Helper for tracking request counts by type
static PRODUCE_REQUESTS: AtomicU64 = AtomicU64::new(0);
static FETCH_REQUESTS: AtomicU64 = AtomicU64::new(0);
static FAILED_PRODUCE_REQUESTS: AtomicU64 = AtomicU64::new(0);
static FAILED_FETCH_REQUESTS: AtomicU64 = AtomicU64::new(0);

/// Record bytes in for a topic (JMX-compatible)
pub fn record_bytes_in(topic: &str, bytes: u64) {
    counter!(
        "kafka_server_broker_topic_metrics_bytes_in_total",
        "topic" => topic.to_string()
    )
    .increment(bytes);
}

/// Record bytes out for a topic (JMX-compatible)
pub fn record_bytes_out(topic: &str, bytes: u64) {
    counter!(
        "kafka_server_broker_topic_metrics_bytes_out_total",
        "topic" => topic.to_string()
    )
    .increment(bytes);
}

/// Record messages in for a topic (JMX-compatible)
pub fn record_messages_in(topic: &str, count: u64) {
    counter!(
        "kafka_server_broker_topic_metrics_messages_in_total",
        "topic" => topic.to_string()
    )
    .increment(count);
}

/// Record a produce request (JMX-compatible)
pub fn record_produce_request(success: bool, duration: Duration) {
    PRODUCE_REQUESTS.fetch_add(1, Ordering::Relaxed);
    counter!("kafka_server_broker_topic_metrics_total_produce_requests_total").increment(1);

    if !success {
        FAILED_PRODUCE_REQUESTS.fetch_add(1, Ordering::Relaxed);
        counter!("kafka_server_broker_topic_metrics_failed_produce_requests_total").increment(1);
    }

    // Record request timing
    histogram!(
        "kafka_network_request_metrics_total_time_ms",
        "request" => "Produce"
    )
    .record(duration.as_millis() as f64);

    counter!(
        "kafka_network_request_metrics_requests_per_sec",
        "request" => "Produce"
    )
    .increment(1);
}

/// Record a fetch request (JMX-compatible)
pub fn record_fetch_request(success: bool, duration: Duration) {
    FETCH_REQUESTS.fetch_add(1, Ordering::Relaxed);
    counter!("kafka_server_broker_topic_metrics_total_fetch_requests_total").increment(1);

    if !success {
        FAILED_FETCH_REQUESTS.fetch_add(1, Ordering::Relaxed);
        counter!("kafka_server_broker_topic_metrics_failed_fetch_requests_total").increment(1);
    }

    // Record request timing
    histogram!(
        "kafka_network_request_metrics_total_time_ms",
        "request" => "Fetch"
    )
    .record(duration.as_millis() as f64);

    counter!(
        "kafka_network_request_metrics_requests_per_sec",
        "request" => "Fetch"
    )
    .increment(1);
}

/// Record a generic request (JMX-compatible)
pub fn record_request(request_type: &str, duration: Duration) {
    histogram!(
        "kafka_network_request_metrics_total_time_ms",
        "request" => request_type.to_string()
    )
    .record(duration.as_millis() as f64);

    counter!(
        "kafka_network_request_metrics_requests_per_sec",
        "request" => request_type.to_string()
    )
    .increment(1);
}

/// Update under-replicated partitions count (JMX-compatible)
pub fn update_under_replicated_partitions(count: usize) {
    gauge!("kafka_server_replica_manager_under_replicated_partitions").set(count as f64);
}

/// Update offline replica count (JMX-compatible)
pub fn update_offline_replica_count(count: usize) {
    gauge!("kafka_server_replica_manager_offline_replica_count").set(count as f64);
}

/// Update leader count (JMX-compatible)
pub fn update_leader_count(count: usize) {
    gauge!("kafka_server_replica_manager_leader_count").set(count as f64);
}

/// Update partition count (JMX-compatible)
pub fn update_partition_count(count: usize) {
    gauge!("kafka_server_replica_manager_partition_count").set(count as f64);
}

/// Update ISR shrinks (JMX-compatible)
pub fn record_isr_shrink() {
    gauge!("kafka_server_replica_manager_isr_shrinks_total").increment(1.0);
}

/// Update ISR expands (JMX-compatible)
pub fn record_isr_expand() {
    gauge!("kafka_server_replica_manager_isr_expands_total").increment(1.0);
}

/// Update active controller count (JMX-compatible)
pub fn update_active_controller(is_controller: bool) {
    gauge!("kafka_controller_active_controller_count").set(if is_controller { 1.0 } else { 0.0 });
}

/// Update offline partitions count (JMX-compatible)
pub fn update_offline_partitions(count: usize) {
    gauge!("kafka_controller_offline_partitions_count").set(count as f64);
}

/// Update global topic count (JMX-compatible)
pub fn update_global_topic_count(count: usize) {
    gauge!("kafka_controller_global_topic_count").set(count as f64);
}

/// Update global partition count (JMX-compatible)
pub fn update_global_partition_count(count: usize) {
    gauge!("kafka_controller_global_partition_count").set(count as f64);
}

/// Update log size for a partition (JMX-compatible)
pub fn update_log_size(topic: &str, partition: i32, size_bytes: u64) {
    gauge!(
        "kafka_log_log_size_bytes",
        "topic" => topic.to_string(),
        "partition" => partition.to_string()
    )
    .set(size_bytes as f64);
}

/// Update log end offset for a partition (JMX-compatible)
pub fn update_log_end_offset(topic: &str, partition: i32, offset: i64) {
    gauge!(
        "kafka_log_log_end_offset",
        "topic" => topic.to_string(),
        "partition" => partition.to_string()
    )
    .set(offset as f64);
}

/// Update log start offset for a partition (JMX-compatible)
pub fn update_log_start_offset(topic: &str, partition: i32, offset: i64) {
    gauge!(
        "kafka_log_log_start_offset",
        "topic" => topic.to_string(),
        "partition" => partition.to_string()
    )
    .set(offset as f64);
}

/// Update number of log segments for a partition (JMX-compatible)
pub fn update_num_log_segments(topic: &str, partition: i32, count: usize) {
    gauge!(
        "kafka_log_num_log_segments",
        "topic" => topic.to_string(),
        "partition" => partition.to_string()
    )
    .set(count as f64);
}

/// Record consumer lag (JMX-compatible)
pub fn record_consumer_lag(group: &str, topic: &str, partition: i32, lag: i64) {
    gauge!(
        "kafka_consumer_consumer_lag",
        "group" => group.to_string(),
        "topic" => topic.to_string(),
        "partition" => partition.to_string()
    )
    .set(lag as f64);
}

/// Record max consumer lag (JMX-compatible)
pub fn record_consumer_lag_max(group: &str, max_lag: i64) {
    gauge!(
        "kafka_consumer_consumer_lag_max",
        "group" => group.to_string()
    )
    .set(max_lag as f64);
}

/// Update active connections count (JMX-compatible)
pub fn update_active_connections(count: usize) {
    gauge!("kafka_server_connections_active").set(count as f64);
}

/// Record connection opened (JMX-compatible)
pub fn record_connection_opened() {
    counter!("kafka_server_connections_total").increment(1);
}

/// Record log flush (JMX-compatible)
pub fn record_log_flush(duration: Duration) {
    gauge!("kafka_log_log_flush_rate_and_time_ms_count").increment(1.0);
    histogram!("kafka_log_log_flush_rate_and_time_ms").record(duration.as_millis() as f64);
}

/// Generate Jolokia-compatible JSON output for JMX metrics
///
/// Jolokia is a common way to expose JMX metrics via HTTP.
/// This format is used by many monitoring systems.
pub fn render_jolokia_metrics(prometheus_output: &str) -> String {
    let mut mbeans: HashMap<String, HashMap<String, serde_json::Value>> = HashMap::new();

    // Parse Prometheus output and convert to Jolokia MBean format
    for line in prometheus_output.lines() {
        // Skip comments and empty lines
        if line.starts_with('#') || line.trim().is_empty() {
            continue;
        }

        // Parse metric line: metric_name{labels} value
        if let Some((metric_part, value_str)) = line.rsplit_once(' ') {
            if let Ok(value) = value_str.parse::<f64>() {
                // Extract metric name and labels
                let (metric_name, labels) = if let Some(brace_pos) = metric_part.find('{') {
                    let name = &metric_part[..brace_pos];
                    let label_str = &metric_part[brace_pos + 1..metric_part.len() - 1];
                    let labels: HashMap<String, String> = parse_labels(label_str);
                    (name, labels)
                } else {
                    (metric_part, HashMap::new())
                };

                // Convert to JMX MBean format if it's a kafka_* metric
                if metric_name.starts_with("kafka_") {
                    let (mbean_name, attr_name) = convert_to_mbean_name(metric_name, &labels);

                    let mbean = mbeans.entry(mbean_name).or_default();
                    mbean.insert(attr_name, serde_json::json!(value));
                }
            }
        }
    }

    // Build Jolokia response format
    let response = serde_json::json!({
        "request": {
            "type": "read"
        },
        "value": mbeans,
        "status": 200
    });

    serde_json::to_string_pretty(&response).unwrap_or_else(|_| "{}".to_string())
}

/// Parse label string into HashMap
fn parse_labels(label_str: &str) -> HashMap<String, String> {
    let mut labels = HashMap::new();

    for part in label_str.split(',') {
        if let Some((key, value)) = part.split_once('=') {
            // Remove quotes from value
            let clean_value = value.trim_matches('"');
            labels.insert(key.to_string(), clean_value.to_string());
        }
    }

    labels
}

/// Convert Prometheus metric name to JMX MBean name
fn convert_to_mbean_name(metric_name: &str, labels: &HashMap<String, String>) -> (String, String) {
    // Remove kafka_ prefix and convert underscores to proper JMX naming
    let parts: Vec<&str> = metric_name
        .strip_prefix("kafka_")
        .unwrap_or(metric_name)
        .split('_')
        .collect();

    if parts.len() >= 3 {
        let domain = parts[0]; // server, controller, network, log, consumer, producer
        let type_name = parts[1..parts.len() - 1].join("_");
        let attr_name = parts[parts.len() - 1];

        // Build MBean name with labels
        let mut mbean = format!("kafka.{}:type={}", domain, to_camel_case(&type_name));

        // Add standard label attributes
        if let Some(topic) = labels.get("topic") {
            mbean.push_str(&format!(",topic={}", topic));
        }
        if let Some(partition) = labels.get("partition") {
            mbean.push_str(&format!(",partition={}", partition));
        }
        if let Some(request) = labels.get("request") {
            mbean.push_str(&format!(",request={}", request));
        }
        if let Some(group) = labels.get("group") {
            mbean.push_str(&format!(",client-id={}", group));
        }

        (mbean, to_camel_case(attr_name))
    } else {
        (metric_name.to_string(), "Value".to_string())
    }
}

/// Convert snake_case to CamelCase
fn to_camel_case(s: &str) -> String {
    s.split('_')
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                None => String::new(),
                Some(first) => first.to_uppercase().chain(chars).collect(),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_camel_case() {
        assert_eq!(to_camel_case("bytes_in"), "BytesIn");
        assert_eq!(
            to_camel_case("under_replicated_partitions"),
            "UnderReplicatedPartitions"
        );
        assert_eq!(to_camel_case("total"), "Total");
    }

    #[test]
    fn test_parse_labels() {
        let labels = parse_labels("topic=\"test\",partition=\"0\"");
        assert_eq!(labels.get("topic"), Some(&"test".to_string()));
        assert_eq!(labels.get("partition"), Some(&"0".to_string()));
    }

    #[test]
    fn test_convert_to_mbean_name() {
        let labels = HashMap::new();
        let (mbean, attr) =
            convert_to_mbean_name("kafka_server_broker_topic_metrics_bytes_in_total", &labels);
        assert!(mbean.contains("kafka.server"));
        assert!(!attr.is_empty());
    }

    #[test]
    fn test_convert_to_mbean_name_with_labels() {
        let mut labels = HashMap::new();
        labels.insert("topic".to_string(), "test-topic".to_string());
        labels.insert("partition".to_string(), "0".to_string());

        let (mbean, _attr) = convert_to_mbean_name("kafka_log_log_size_bytes", &labels);
        assert!(mbean.contains("topic=test-topic"));
        assert!(mbean.contains("partition=0"));
    }

    #[test]
    fn test_jmx_metrics_new() {
        let _jmx = JmxMetrics::new();
        // Verify creation succeeds
    }

    #[test]
    fn test_render_jolokia_metrics_empty() {
        let output = render_jolokia_metrics("");
        assert!(output.contains("\"status\": 200"));
    }

    #[test]
    fn test_render_jolokia_metrics_with_data() {
        let prometheus = r#"
# HELP kafka_server_broker_topic_metrics_bytes_in_total Bytes in
# TYPE kafka_server_broker_topic_metrics_bytes_in_total counter
kafka_server_broker_topic_metrics_bytes_in_total{topic="test"} 1024
"#;
        let output = render_jolokia_metrics(prometheus);
        assert!(output.contains("1024"));
    }

    #[tokio::test]
    async fn test_record_bytes_in() {
        // Initialize metrics
        crate::metrics::init_metrics();
        record_bytes_in("test-topic", 1024);
        // Metric should be recorded (no panic)
    }

    #[tokio::test]
    async fn test_record_bytes_out() {
        crate::metrics::init_metrics();
        record_bytes_out("test-topic", 2048);
    }

    #[tokio::test]
    async fn test_record_messages_in() {
        crate::metrics::init_metrics();
        record_messages_in("test-topic", 100);
    }

    #[tokio::test]
    async fn test_record_produce_request() {
        crate::metrics::init_metrics();
        record_produce_request(true, Duration::from_millis(10));
        record_produce_request(false, Duration::from_millis(5));
    }

    #[tokio::test]
    async fn test_record_fetch_request() {
        crate::metrics::init_metrics();
        record_fetch_request(true, Duration::from_millis(15));
        record_fetch_request(false, Duration::from_millis(20));
    }

    #[tokio::test]
    async fn test_record_request_generic() {
        crate::metrics::init_metrics();
        record_request("Metadata", Duration::from_millis(5));
        record_request("ApiVersions", Duration::from_millis(2));
    }

    #[tokio::test]
    async fn test_update_under_replicated_partitions() {
        crate::metrics::init_metrics();
        update_under_replicated_partitions(3);
    }

    #[tokio::test]
    async fn test_update_offline_replica_count() {
        crate::metrics::init_metrics();
        update_offline_replica_count(1);
    }

    #[tokio::test]
    async fn test_update_leader_count() {
        crate::metrics::init_metrics();
        update_leader_count(5);
    }

    #[tokio::test]
    async fn test_update_partition_count() {
        crate::metrics::init_metrics();
        update_partition_count(10);
    }

    #[tokio::test]
    async fn test_isr_changes() {
        crate::metrics::init_metrics();
        record_isr_shrink();
        record_isr_expand();
    }

    #[tokio::test]
    async fn test_controller_metrics() {
        crate::metrics::init_metrics();
        update_active_controller(true);
        update_offline_partitions(0);
        update_global_topic_count(5);
        update_global_partition_count(15);
    }

    #[tokio::test]
    async fn test_log_metrics() {
        crate::metrics::init_metrics();
        update_log_size("test", 0, 1024 * 1024);
        update_log_end_offset("test", 0, 1000);
        update_log_start_offset("test", 0, 0);
        update_num_log_segments("test", 0, 3);
    }

    #[tokio::test]
    async fn test_consumer_metrics() {
        crate::metrics::init_metrics();
        record_consumer_lag("group1", "topic1", 0, 100);
        record_consumer_lag_max("group1", 100);
    }

    #[tokio::test]
    async fn test_connection_metrics() {
        crate::metrics::init_metrics();
        update_active_connections(10);
        record_connection_opened();
    }

    #[tokio::test]
    async fn test_log_flush() {
        crate::metrics::init_metrics();
        record_log_flush(Duration::from_millis(50));
    }
}
