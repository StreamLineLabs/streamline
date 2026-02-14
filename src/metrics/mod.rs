//! Metrics collection and exposition for Streamline
//!
//! This module provides Prometheus-compatible metrics for monitoring
//! server performance, request handling, and storage statistics.
//!
//! When the `metrics` feature is disabled, all functions become no-ops,
//! allowing the code to compile without the metrics dependencies while
//! maintaining the same API surface.
//!
//! ## JMX Compatibility
//!
//! In addition to Streamline-native metrics (`streamline_*`), this module
//! also provides Kafka JMX-compatible metrics (`kafka_*`) for drop-in
//! monitoring compatibility. Access JMX metrics via the `/metrics/jmx`
//! endpoint in Jolokia format.

// History module doesn't depend on metrics crate, always available
pub mod history;

// JMX module only available with metrics feature
#[cfg(feature = "metrics")]
pub mod jmx;

#[cfg(feature = "metrics")]
use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};
#[cfg(feature = "metrics")]
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
#[cfg(feature = "metrics")]
use std::sync::{Arc, OnceLock};
use std::time::Duration;

pub use history::{HistoricalMetrics, MetricPoint, MetricsHistory};
#[cfg(feature = "metrics")]
pub use jmx::JmxMetrics;

#[cfg(feature = "metrics")]
static METRICS_INITIALIZED: OnceLock<Arc<PrometheusHandle>> = OnceLock::new();
#[cfg(feature = "metrics")]
static JMX_METRICS: OnceLock<JmxMetrics> = OnceLock::new();

// ============================================================================
// Prometheus Handle (feature-gated)
// ============================================================================

/// Initialize metrics and return Prometheus handle
///
/// Note: This function can only be called once per process due to the global
/// nature of the metrics recorder. In tests, the first call wins.
///
/// When the `metrics` feature is disabled, this returns a no-op handle.
#[cfg(feature = "metrics")]
pub fn init_metrics() -> PrometheusHandle {
    // Check if already initialized
    if let Some(handle) = METRICS_INITIALIZED.get() {
        return (**handle).clone();
    }

    let builder = PrometheusBuilder::new();
    let handle = match builder.install_recorder() {
        Ok(h) => {
            // Register all metrics with descriptions
            register_metrics();

            // Initialize and register JMX-compatible metrics
            let jmx = JmxMetrics::new();
            jmx.register_metrics();
            let _ = JMX_METRICS.set(jmx);

            // Store handle
            let _ = METRICS_INITIALIZED.set(Arc::new(h.clone()));

            h
        }
        Err(_) => {
            // Already installed - return stored handle if available
            if let Some(handle) = METRICS_INITIALIZED.get() {
                return (**handle).clone();
            }
            // If we get here, metrics was initialized elsewhere (e.g., another test)
            // We can't get the handle, but we can still use metrics
            // Create a recorder to get a handle (won't install twice)
            let recorder = PrometheusBuilder::new().build_recorder();
            let handle = recorder.handle();
            // Note: We don't install this recorder since one is already installed
            // We just use it to get a handle for rendering metrics
            drop(recorder);
            handle
        }
    };

    handle
}

/// Register all metrics with their descriptions
#[cfg(feature = "metrics")]
fn register_metrics() {
    // Server metrics
    describe_gauge!(
        "streamline_connections_active",
        "Number of currently active connections"
    );
    describe_counter!(
        "streamline_connections_total",
        "Total number of connections accepted"
    );
    describe_gauge!("streamline_uptime_seconds", "Server uptime in seconds");

    // Consumer group lag metrics
    describe_gauge!(
        "streamline_consumer_group_lag",
        "Consumer group lag per partition (messages behind)"
    );
    describe_gauge!(
        "streamline_consumer_group_total_lag",
        "Total consumer group lag across all partitions"
    );
    describe_gauge!(
        "streamline_consumer_group_members",
        "Number of members in consumer group"
    );
    describe_gauge!(
        "streamline_consumer_group_partitions",
        "Number of assigned partitions in consumer group"
    );
    describe_gauge!(
        "streamline_consumer_group_max_lag",
        "Maximum lag across all partitions in consumer group"
    );
    describe_counter!(
        "streamline_consumer_group_rebalances_total",
        "Total number of consumer group rebalances"
    );
    describe_histogram!(
        "streamline_consumer_group_rebalance_duration_ms",
        "Consumer group rebalance duration in milliseconds"
    );

    // Request metrics
    describe_counter!(
        "streamline_requests_total",
        "Total number of requests by API type and error status"
    );
    describe_histogram!(
        "streamline_request_duration_seconds",
        "Request latency in seconds"
    );
    describe_histogram!(
        "streamline_request_size_bytes",
        "Request payload size in bytes"
    );
    describe_histogram!(
        "streamline_response_size_bytes",
        "Response payload size in bytes"
    );

    // Storage metrics
    describe_gauge!("streamline_topics_total", "Total number of topics");
    describe_gauge!(
        "streamline_partitions_total",
        "Number of partitions per topic"
    );
    describe_counter!(
        "streamline_records_total",
        "Total number of records written"
    );
    describe_counter!("streamline_bytes_in_total", "Total bytes received");
    describe_counter!("streamline_bytes_out_total", "Total bytes sent");
    describe_gauge!("streamline_log_size_bytes", "Partition log size in bytes");
    describe_gauge!("streamline_log_end_offset", "Latest offset in partition");
    describe_gauge!(
        "streamline_segments_total",
        "Number of segments in partition"
    );

    // Replication metrics
    describe_gauge!(
        "streamline_replication_lag_messages",
        "Replication lag in number of messages behind leader"
    );
    describe_gauge!(
        "streamline_replication_lag_bytes",
        "Replication lag in bytes behind leader"
    );
    describe_gauge!(
        "streamline_last_replication_seconds",
        "Time since last successful replication fetch in seconds"
    );
    describe_gauge!(
        "streamline_isr_size",
        "Number of in-sync replicas for partition"
    );
    describe_gauge!(
        "streamline_under_replicated_partitions",
        "Number of partitions with fewer ISRs than configured replicas"
    );
    describe_gauge!(
        "streamline_offline_partitions",
        "Number of partitions without a leader"
    );

    // Cluster metrics
    describe_gauge!(
        "streamline_cluster_nodes_total",
        "Total number of nodes in the cluster"
    );
    describe_gauge!(
        "streamline_cluster_leader",
        "Whether this node is the Raft leader (1) or not (0)"
    );
    describe_counter!(
        "streamline_cluster_elections_total",
        "Total number of leader elections"
    );
    describe_gauge!(
        "streamline_cluster_raft_term",
        "Current Raft term number"
    );
    describe_gauge!(
        "streamline_cluster_raft_log_entries",
        "Number of entries in the Raft log"
    );
    describe_gauge!(
        "streamline_cluster_raft_commit_index",
        "Raft commit index (latest committed log entry)"
    );
    describe_gauge!(
        "streamline_cluster_raft_apply_index",
        "Raft apply index (latest applied log entry)"
    );
    describe_histogram!(
        "streamline_cluster_raft_replication_lag_ms",
        "Raft replication lag in milliseconds"
    );
    describe_histogram!(
        "streamline_cluster_raft_append_latency_ms",
        "Latency of Raft log append operations in milliseconds"
    );
    describe_gauge!(
        "streamline_cluster_raft_snapshot_size_bytes",
        "Size of the last Raft snapshot in bytes"
    );

    // Storage mode metrics
    describe_gauge!(
        "streamline_partition_storage_mode",
        "Storage mode for partition (0=local, 1=hybrid, 2=diskless)"
    );
    describe_gauge!(
        "streamline_partitions_by_storage_mode",
        "Count of partitions by storage mode"
    );
    describe_counter!("streamline_s3_reads_total", "Total S3 read operations");
    describe_counter!("streamline_s3_writes_total", "Total S3 write operations");
    describe_counter!("streamline_s3_bytes_read_total", "Total bytes read from S3");
    describe_counter!(
        "streamline_s3_bytes_written_total",
        "Total bytes written to S3"
    );
    describe_counter!(
        "streamline_s3_cache_hits_total",
        "Total S3 segment cache hits"
    );
    describe_counter!(
        "streamline_s3_cache_misses_total",
        "Total S3 segment cache misses"
    );
    describe_histogram!(
        "streamline_s3_read_latency_seconds",
        "S3 read operation latency in seconds"
    );
    describe_histogram!(
        "streamline_s3_write_latency_seconds",
        "S3 write operation latency in seconds"
    );
    describe_gauge!(
        "streamline_s3_cache_size_bytes",
        "Current size of S3 segment cache in bytes"
    );
    describe_counter!("streamline_s3_errors_total", "Total S3 operation errors");
    describe_gauge!(
        "streamline_recovery_partitions_discovered",
        "Number of partitions discovered during recovery"
    );
    describe_gauge!(
        "streamline_recovery_partitions_recovered",
        "Number of partitions successfully recovered"
    );
    describe_gauge!(
        "streamline_recovery_partitions_failed",
        "Number of partitions that failed recovery"
    );

    // Multi-DC replication metrics
    describe_gauge!(
        "streamline_multidc_replication_lag_ms",
        "Replication lag to remote datacenter in milliseconds"
    );
    describe_gauge!(
        "streamline_multidc_replication_lag_offsets",
        "Replication lag to remote datacenter in offsets"
    );
    describe_counter!(
        "streamline_multidc_records_replicated",
        "Total records replicated to remote datacenter"
    );
    describe_counter!(
        "streamline_multidc_bytes_replicated",
        "Total bytes replicated to remote datacenter"
    );
    describe_counter!(
        "streamline_multidc_batches_sent",
        "Total replication batches sent to remote datacenter"
    );
    describe_counter!(
        "streamline_multidc_batches_failed",
        "Total replication batches that failed"
    );
    describe_gauge!(
        "streamline_multidc_connection_status",
        "Connection status to remote datacenter (1=connected, 0=disconnected)"
    );
    describe_counter!(
        "streamline_multidc_failover_total",
        "Total number of datacenter failovers"
    );

    // I/O backend metrics
    describe_counter!("streamline_io_reads_total", "Total I/O read operations");
    describe_counter!("streamline_io_writes_total", "Total I/O write operations");
    describe_counter!("streamline_io_syncs_total", "Total I/O sync operations");
    describe_counter!(
        "streamline_io_bytes_read_total",
        "Total bytes read via I/O backend"
    );
    describe_counter!(
        "streamline_io_bytes_written_total",
        "Total bytes written via I/O backend"
    );
    describe_histogram!(
        "streamline_io_read_duration_seconds",
        "Duration of I/O read operations in seconds"
    );
    describe_histogram!(
        "streamline_io_write_duration_seconds",
        "Duration of I/O write operations in seconds"
    );
    describe_histogram!(
        "streamline_io_sync_duration_seconds",
        "Duration of I/O sync operations in seconds"
    );
    describe_gauge!(
        "streamline_io_backend_type",
        "Active I/O backend type (1=io_uring, 0=standard)"
    );
    describe_counter!("streamline_io_errors_total", "Total I/O operation errors");

    // Buffer pool metrics
    describe_gauge!(
        "streamline_buffer_pool_allocations",
        "Total buffer pool allocations"
    );
    describe_gauge!(
        "streamline_buffer_pool_reuses",
        "Total buffer pool buffer reuses"
    );
    describe_gauge!(
        "streamline_buffer_pool_hit_rate",
        "Buffer pool hit rate (0.0-1.0)"
    );
}

// ============================================================================
// Recording functions - with metrics feature
// ============================================================================

/// Record an active connection change
#[cfg(feature = "metrics")]
pub fn record_connection_active(delta: i64) {
    if delta > 0 {
        gauge!("streamline_connections_active").increment(delta as f64);
        counter!("streamline_connections_total").increment(delta as u64);
    } else {
        gauge!("streamline_connections_active").decrement(delta.unsigned_abs() as f64);
    }
}

/// Record server uptime
#[cfg(feature = "metrics")]
pub fn record_uptime(seconds: f64) {
    gauge!("streamline_uptime_seconds").set(seconds);
}

/// Record a request with its details
#[cfg(feature = "metrics")]
pub fn record_request(
    api_key: &str,
    duration: Duration,
    request_size: usize,
    response_size: usize,
    error: bool,
) {
    let error_label = if error { "true" } else { "false" };

    counter!("streamline_requests_total", "api_key" => api_key.to_string(), "error" => error_label.to_string()).increment(1);
    histogram!("streamline_request_duration_seconds", "api_key" => api_key.to_string())
        .record(duration.as_secs_f64());
    histogram!("streamline_request_size_bytes", "api_key" => api_key.to_string())
        .record(request_size as f64);
    histogram!("streamline_response_size_bytes", "api_key" => api_key.to_string())
        .record(response_size as f64);
}

/// Record bytes received for a topic
#[cfg(feature = "metrics")]
pub fn record_bytes_in(topic: &str, bytes: u64) {
    counter!("streamline_bytes_in_total", "topic" => topic.to_string()).increment(bytes);
}

/// Record bytes sent for a topic
#[cfg(feature = "metrics")]
pub fn record_bytes_out(topic: &str, bytes: u64) {
    counter!("streamline_bytes_out_total", "topic" => topic.to_string()).increment(bytes);
}

/// Record records written to a partition
#[cfg(feature = "metrics")]
pub fn record_records_written(topic: &str, partition: i32, count: u64) {
    counter!("streamline_records_total", "topic" => topic.to_string(), "partition" => partition.to_string()).increment(count);
}

/// Update partition metrics
#[cfg(feature = "metrics")]
pub fn update_partition_metrics(
    topic: &str,
    partition: i32,
    log_size: u64,
    end_offset: i64,
    segment_count: usize,
) {
    let topic_str = topic.to_string();
    let partition_str = partition.to_string();

    gauge!("streamline_log_size_bytes", "topic" => topic_str.clone(), "partition" => partition_str.clone()).set(log_size as f64);
    gauge!("streamline_log_end_offset", "topic" => topic_str.clone(), "partition" => partition_str.clone()).set(end_offset as f64);
    gauge!("streamline_segments_total", "topic" => topic_str, "partition" => partition_str)
        .set(segment_count as f64);
}

/// Update topic count
#[cfg(feature = "metrics")]
pub fn update_topics_total(count: usize) {
    gauge!("streamline_topics_total").set(count as f64);
}

/// Update partition count for a topic
#[cfg(feature = "metrics")]
pub fn update_partitions_total(topic: &str, count: i32) {
    gauge!("streamline_partitions_total", "topic" => topic.to_string()).set(count as f64);
}

/// Record replication lag for a partition replica
#[cfg(feature = "metrics")]
pub fn record_replication_lag(
    topic: &str,
    partition: i32,
    replica_id: u64,
    lag_messages: i64,
    lag_bytes: i64,
) {
    let topic_str = topic.to_string();
    let partition_str = partition.to_string();
    let replica_str = replica_id.to_string();

    gauge!(
        "streamline_replication_lag_messages",
        "topic" => topic_str.clone(),
        "partition" => partition_str.clone(),
        "replica" => replica_str.clone()
    )
    .set(lag_messages as f64);

    gauge!(
        "streamline_replication_lag_bytes",
        "topic" => topic_str,
        "partition" => partition_str,
        "replica" => replica_str
    )
    .set(lag_bytes as f64);
}

/// Update ISR size for a partition
#[cfg(feature = "metrics")]
pub fn update_isr_size(topic: &str, partition: i32, isr_count: usize) {
    gauge!(
        "streamline_isr_size",
        "topic" => topic.to_string(),
        "partition" => partition.to_string()
    )
    .set(isr_count as f64);
}

/// Update count of under-replicated partitions
#[cfg(feature = "metrics")]
pub fn update_under_replicated_partitions(count: usize) {
    gauge!("streamline_under_replicated_partitions").set(count as f64);
}

/// Update count of offline partitions (no leader)
#[cfg(feature = "metrics")]
pub fn update_offline_partitions(count: usize) {
    gauge!("streamline_offline_partitions").set(count as f64);
}

/// Record time since last successful replication for a follower
#[cfg(feature = "metrics")]
pub fn record_last_replication_time(
    topic: &str,
    partition: i32,
    replica_id: u64,
    seconds_since_last: f64,
) {
    gauge!(
        "streamline_last_replication_seconds",
        "topic" => topic.to_string(),
        "partition" => partition.to_string(),
        "replica" => replica_id.to_string()
    )
    .set(seconds_since_last);
}

/// Update cluster node count
#[cfg(feature = "metrics")]
pub fn update_cluster_nodes_total(count: usize) {
    gauge!("streamline_cluster_nodes_total").set(count as f64);
}

/// Update cluster leader status
#[cfg(feature = "metrics")]
pub fn update_cluster_leader(is_leader: bool) {
    gauge!("streamline_cluster_leader").set(if is_leader { 1.0 } else { 0.0 });
}

/// Record a leader election event
#[cfg(feature = "metrics")]
pub fn record_cluster_election() {
    counter!("streamline_cluster_elections_total").increment(1);
}

/// Update the current Raft term
#[cfg(feature = "metrics")]
pub fn update_cluster_raft_term(term: u64) {
    gauge!("streamline_cluster_raft_term").set(term as f64);
}

/// Update Raft log entry count
#[cfg(feature = "metrics")]
pub fn update_cluster_raft_log_entries(count: u64) {
    gauge!("streamline_cluster_raft_log_entries").set(count as f64);
}

/// Update Raft commit index
#[cfg(feature = "metrics")]
pub fn update_cluster_raft_commit_index(index: u64) {
    gauge!("streamline_cluster_raft_commit_index").set(index as f64);
}

/// Update Raft apply index
#[cfg(feature = "metrics")]
pub fn update_cluster_raft_apply_index(index: u64) {
    gauge!("streamline_cluster_raft_apply_index").set(index as f64);
}

/// Record Raft replication lag
#[cfg(feature = "metrics")]
pub fn record_cluster_raft_replication_lag_ms(lag_ms: f64) {
    histogram!("streamline_cluster_raft_replication_lag_ms").record(lag_ms);
}

/// Record Raft log append latency
#[cfg(feature = "metrics")]
pub fn record_cluster_raft_append_latency_ms(latency_ms: f64) {
    histogram!("streamline_cluster_raft_append_latency_ms").record(latency_ms);
}

/// Update Raft snapshot size
#[cfg(feature = "metrics")]
pub fn update_cluster_raft_snapshot_size(size_bytes: u64) {
    gauge!("streamline_cluster_raft_snapshot_size_bytes").set(size_bytes as f64);
}

/// Record multi-DC replication lag
#[cfg(feature = "metrics")]
pub fn record_multidc_replication_lag(
    remote_dc: &str,
    topic: &str,
    partition: i32,
    lag_ms: u64,
    lag_offsets: i64,
) {
    let dc_str = remote_dc.to_string();
    let topic_str = topic.to_string();
    let partition_str = partition.to_string();

    gauge!(
        "streamline_multidc_replication_lag_ms",
        "remote_dc" => dc_str.clone(),
        "topic" => topic_str.clone(),
        "partition" => partition_str.clone()
    )
    .set(lag_ms as f64);

    gauge!(
        "streamline_multidc_replication_lag_offsets",
        "remote_dc" => dc_str,
        "topic" => topic_str,
        "partition" => partition_str
    )
    .set(lag_offsets as f64);
}

/// Record multi-DC records replicated
#[cfg(feature = "metrics")]
pub fn record_multidc_records_replicated(remote_dc: &str, count: u64) {
    counter!(
        "streamline_multidc_records_replicated",
        "remote_dc" => remote_dc.to_string()
    )
    .increment(count);
}

/// Record multi-DC bytes replicated
#[cfg(feature = "metrics")]
pub fn record_multidc_bytes_replicated(remote_dc: &str, bytes: u64) {
    counter!(
        "streamline_multidc_bytes_replicated",
        "remote_dc" => remote_dc.to_string()
    )
    .increment(bytes);
}

/// Record multi-DC replication batch sent
#[cfg(feature = "metrics")]
pub fn record_multidc_batch_sent(remote_dc: &str, success: bool) {
    if success {
        counter!(
            "streamline_multidc_batches_sent",
            "remote_dc" => remote_dc.to_string()
        )
        .increment(1);
    } else {
        counter!(
            "streamline_multidc_batches_failed",
            "remote_dc" => remote_dc.to_string()
        )
        .increment(1);
    }
}

/// Update multi-DC connection status
#[cfg(feature = "metrics")]
pub fn update_multidc_connection_status(remote_dc: &str, connected: bool) {
    gauge!(
        "streamline_multidc_connection_status",
        "remote_dc" => remote_dc.to_string()
    )
    .set(if connected { 1.0 } else { 0.0 });
}

/// Record multi-DC failover event
#[cfg(feature = "metrics")]
pub fn record_multidc_failover(failed_dc: &str, new_primary: &str) {
    counter!(
        "streamline_multidc_failover_total",
        "failed_dc" => failed_dc.to_string(),
        "new_primary" => new_primary.to_string()
    )
    .increment(1);
}

/// Record consumer group lag for a specific partition
#[cfg(feature = "metrics")]
pub fn record_consumer_group_lag(group_id: &str, topic: &str, partition: i32, lag: i64) {
    gauge!(
        "streamline_consumer_group_lag",
        "group" => group_id.to_string(),
        "topic" => topic.to_string(),
        "partition" => partition.to_string()
    )
    .set(lag as f64);
}

/// Record total consumer group lag
#[cfg(feature = "metrics")]
pub fn record_consumer_group_total_lag(group_id: &str, total_lag: i64) {
    gauge!(
        "streamline_consumer_group_total_lag",
        "group" => group_id.to_string()
    )
    .set(total_lag as f64);
}

/// Record consumer group member count
#[cfg(feature = "metrics")]
pub fn record_consumer_group_members(group_id: &str, count: usize) {
    gauge!(
        "streamline_consumer_group_members",
        "group" => group_id.to_string()
    )
    .set(count as f64);
}

/// Record consumer group partition count
#[cfg(feature = "metrics")]
pub fn record_consumer_group_partitions(group_id: &str, count: usize) {
    gauge!(
        "streamline_consumer_group_partitions",
        "group" => group_id.to_string()
    )
    .set(count as f64);
}

/// Record consumer group maximum lag
#[cfg(feature = "metrics")]
pub fn record_consumer_group_max_lag(group_id: &str, max_lag: i64) {
    gauge!(
        "streamline_consumer_group_max_lag",
        "group" => group_id.to_string()
    )
    .set(max_lag as f64);
}

/// Record a consumer group rebalance event
///
/// Call this when a consumer group completes a rebalance (transitions from
/// PreparingRebalance or CompletingRebalance to Stable).
#[cfg(feature = "metrics")]
pub fn record_group_rebalance(group_id: &str, reason: &str) {
    counter!(
        "streamline_consumer_group_rebalances_total",
        "group" => group_id.to_string(),
        "reason" => reason.to_string()
    )
    .increment(1);
}

/// Record consumer group rebalance duration
///
/// Call this with the time taken to complete a rebalance cycle
/// (from first JoinGroup to SyncGroup completion).
#[cfg(feature = "metrics")]
pub fn record_rebalance_duration(group_id: &str, duration_ms: u64) {
    histogram!(
        "streamline_consumer_group_rebalance_duration_ms",
        "group" => group_id.to_string()
    )
    .record(duration_ms as f64);
}

/// Update all consumer group lag metrics from lag data
#[cfg(feature = "metrics")]
pub fn update_consumer_group_lag_metrics(
    group_id: &str,
    partitions: &[(String, i32, i64)], // (topic, partition, lag)
    total_lag: i64,
    members: usize,
) {
    // Record per-partition lag
    for (topic, partition, lag) in partitions {
        record_consumer_group_lag(group_id, topic, *partition, *lag);
    }

    // Record aggregate metrics
    record_consumer_group_total_lag(group_id, total_lag);
    record_consumer_group_members(group_id, members);
    record_consumer_group_partitions(group_id, partitions.len());

    // Record max lag
    let max_lag = partitions.iter().map(|(_, _, lag)| *lag).max().unwrap_or(0);
    record_consumer_group_max_lag(group_id, max_lag);
}

// ============================================================================
// Storage mode metrics
// ============================================================================

/// Update partition storage mode metric
/// Storage modes: 0=local, 1=hybrid, 2=diskless
#[cfg(feature = "metrics")]
pub fn update_partition_storage_mode(topic: &str, partition: i32, mode: u8) {
    gauge!(
        "streamline_partition_storage_mode",
        "topic" => topic.to_string(),
        "partition" => partition.to_string()
    )
    .set(mode as f64);
}

/// Update count of partitions by storage mode
#[cfg(feature = "metrics")]
pub fn update_partitions_by_storage_mode(mode: &str, count: usize) {
    gauge!(
        "streamline_partitions_by_storage_mode",
        "mode" => mode.to_string()
    )
    .set(count as f64);
}

/// Record an S3 read operation
#[cfg(feature = "metrics")]
pub fn record_s3_read(topic: &str, partition: i32, bytes: u64, latency_secs: f64) {
    let topic_str = topic.to_string();
    let partition_str = partition.to_string();

    counter!(
        "streamline_s3_reads_total",
        "topic" => topic_str.clone(),
        "partition" => partition_str.clone()
    )
    .increment(1);

    counter!(
        "streamline_s3_bytes_read_total",
        "topic" => topic_str.clone(),
        "partition" => partition_str.clone()
    )
    .increment(bytes);

    histogram!(
        "streamline_s3_read_latency_seconds",
        "topic" => topic_str,
        "partition" => partition_str
    )
    .record(latency_secs);
}

/// Record an S3 write operation
#[cfg(feature = "metrics")]
pub fn record_s3_write(topic: &str, partition: i32, bytes: u64, latency_secs: f64) {
    let topic_str = topic.to_string();
    let partition_str = partition.to_string();

    counter!(
        "streamline_s3_writes_total",
        "topic" => topic_str.clone(),
        "partition" => partition_str.clone()
    )
    .increment(1);

    counter!(
        "streamline_s3_bytes_written_total",
        "topic" => topic_str.clone(),
        "partition" => partition_str.clone()
    )
    .increment(bytes);

    histogram!(
        "streamline_s3_write_latency_seconds",
        "topic" => topic_str,
        "partition" => partition_str
    )
    .record(latency_secs);
}

/// Record an S3 cache hit
#[cfg(feature = "metrics")]
pub fn record_s3_cache_hit(topic: &str, partition: i32) {
    counter!(
        "streamline_s3_cache_hits_total",
        "topic" => topic.to_string(),
        "partition" => partition.to_string()
    )
    .increment(1);
}

/// Record an S3 cache miss
#[cfg(feature = "metrics")]
pub fn record_s3_cache_miss(topic: &str, partition: i32) {
    counter!(
        "streamline_s3_cache_misses_total",
        "topic" => topic.to_string(),
        "partition" => partition.to_string()
    )
    .increment(1);
}

/// Update S3 cache size
#[cfg(feature = "metrics")]
pub fn update_s3_cache_size(bytes: u64) {
    gauge!("streamline_s3_cache_size_bytes").set(bytes as f64);
}

/// Record an S3 error
#[cfg(feature = "metrics")]
pub fn record_s3_error(topic: &str, partition: i32, operation: &str) {
    counter!(
        "streamline_s3_errors_total",
        "topic" => topic.to_string(),
        "partition" => partition.to_string(),
        "operation" => operation.to_string()
    )
    .increment(1);
}

/// Update recovery metrics
#[cfg(feature = "metrics")]
pub fn update_recovery_metrics(discovered: usize, recovered: usize, failed: usize) {
    gauge!("streamline_recovery_partitions_discovered").set(discovered as f64);
    gauge!("streamline_recovery_partitions_recovered").set(recovered as f64);
    gauge!("streamline_recovery_partitions_failed").set(failed as f64);
}

/// Render metrics in Jolokia (JMX over HTTP) format
///
/// This provides a JSON response compatible with Jolokia clients
/// and Kafka JMX monitoring tools.
#[cfg(feature = "metrics")]
pub fn render_jmx_metrics(prometheus_handle: &PrometheusHandle) -> String {
    let prometheus_output = prometheus_handle.render();
    jmx::render_jolokia_metrics(&prometheus_output)
}

// ============================================================================
// No-op implementations when metrics feature is disabled
// ============================================================================

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn record_connection_active(_delta: i64) {}

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn record_uptime(_seconds: f64) {}

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn record_request(
    _api_key: &str,
    _duration: Duration,
    _request_size: usize,
    _response_size: usize,
    _error: bool,
) {
}

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn record_bytes_in(_topic: &str, _bytes: u64) {}

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn record_bytes_out(_topic: &str, _bytes: u64) {}

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn record_records_written(_topic: &str, _partition: i32, _count: u64) {}

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn update_partition_metrics(
    _topic: &str,
    _partition: i32,
    _log_size: u64,
    _end_offset: i64,
    _segment_count: usize,
) {
}

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn update_topics_total(_count: usize) {}

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn update_partitions_total(_topic: &str, _count: i32) {}

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn record_replication_lag(
    _topic: &str,
    _partition: i32,
    _replica_id: u64,
    _lag_messages: i64,
    _lag_bytes: i64,
) {
}

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn update_isr_size(_topic: &str, _partition: i32, _isr_count: usize) {}

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn update_under_replicated_partitions(_count: usize) {}

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn update_offline_partitions(_count: usize) {}

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn record_last_replication_time(
    _topic: &str,
    _partition: i32,
    _replica_id: u64,
    _seconds_since_last: f64,
) {
}

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn update_cluster_nodes_total(_count: usize) {}

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn update_cluster_leader(_is_leader: bool) {}

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn record_cluster_election() {}

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn update_cluster_raft_term(_term: u64) {}

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn update_cluster_raft_log_entries(_count: u64) {}

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn update_cluster_raft_commit_index(_index: u64) {}

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn update_cluster_raft_apply_index(_index: u64) {}

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn record_cluster_raft_replication_lag_ms(_lag_ms: f64) {}

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn record_cluster_raft_append_latency_ms(_latency_ms: f64) {}

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn update_cluster_raft_snapshot_size(_size_bytes: u64) {}

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn record_multidc_replication_lag(
    _remote_dc: &str,
    _topic: &str,
    _partition: i32,
    _lag_ms: u64,
    _lag_offsets: i64,
) {
}

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn record_multidc_records_replicated(_remote_dc: &str, _count: u64) {}

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn record_multidc_bytes_replicated(_remote_dc: &str, _bytes: u64) {}

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn record_multidc_batch_sent(_remote_dc: &str, _success: bool) {}

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn update_multidc_connection_status(_remote_dc: &str, _connected: bool) {}

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn record_multidc_failover(_failed_dc: &str, _new_primary: &str) {}

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn record_consumer_group_lag(_group_id: &str, _topic: &str, _partition: i32, _lag: i64) {}

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn record_consumer_group_total_lag(_group_id: &str, _total_lag: i64) {}

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn record_consumer_group_members(_group_id: &str, _count: usize) {}

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn record_consumer_group_partitions(_group_id: &str, _count: usize) {}

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn record_consumer_group_max_lag(_group_id: &str, _max_lag: i64) {}

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn record_group_rebalance(_group_id: &str, _reason: &str) {}

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn record_rebalance_duration(_group_id: &str, _duration_ms: u64) {}

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn update_consumer_group_lag_metrics(
    _group_id: &str,
    _partitions: &[(String, i32, i64)],
    _total_lag: i64,
    _members: usize,
) {
}

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn update_partition_storage_mode(_topic: &str, _partition: i32, _mode: u8) {}

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn update_partitions_by_storage_mode(_mode: &str, _count: usize) {}

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn record_s3_read(_topic: &str, _partition: i32, _bytes: u64, _latency_secs: f64) {}

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn record_s3_write(_topic: &str, _partition: i32, _bytes: u64, _latency_secs: f64) {}

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn record_s3_cache_hit(_topic: &str, _partition: i32) {}

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn record_s3_cache_miss(_topic: &str, _partition: i32) {}

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn update_s3_cache_size(_bytes: u64) {}

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn record_s3_error(_topic: &str, _partition: i32, _operation: &str) {}

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn update_recovery_metrics(_discovered: usize, _recovered: usize, _failed: usize) {}

// ============================================================================
// I/O Backend Metrics
// ============================================================================

/// Record an I/O read operation
#[cfg(feature = "metrics")]
pub fn record_io_read(bytes: u64, duration: Duration, backend: &str) {
    counter!("streamline_io_reads_total", "backend" => backend.to_string()).increment(1);
    counter!("streamline_io_bytes_read_total", "backend" => backend.to_string()).increment(bytes);
    histogram!("streamline_io_read_duration_seconds", "backend" => backend.to_string())
        .record(duration.as_secs_f64());
}

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn record_io_read(_bytes: u64, _duration: Duration, _backend: &str) {}

/// Record an I/O write operation
#[cfg(feature = "metrics")]
pub fn record_io_write(bytes: u64, duration: Duration, backend: &str) {
    counter!("streamline_io_writes_total", "backend" => backend.to_string()).increment(1);
    counter!("streamline_io_bytes_written_total", "backend" => backend.to_string())
        .increment(bytes);
    histogram!("streamline_io_write_duration_seconds", "backend" => backend.to_string())
        .record(duration.as_secs_f64());
}

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn record_io_write(_bytes: u64, _duration: Duration, _backend: &str) {}

/// Record an I/O sync operation
#[cfg(feature = "metrics")]
pub fn record_io_sync(duration: Duration, backend: &str, sync_type: &str) {
    counter!("streamline_io_syncs_total", "backend" => backend.to_string(), "type" => sync_type.to_string()).increment(1);
    histogram!("streamline_io_sync_duration_seconds", "backend" => backend.to_string(), "type" => sync_type.to_string())
        .record(duration.as_secs_f64());
}

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn record_io_sync(_duration: Duration, _backend: &str, _sync_type: &str) {}

/// Record an I/O error
#[cfg(feature = "metrics")]
pub fn record_io_error(operation: &str, backend: &str) {
    counter!("streamline_io_errors_total", "operation" => operation.to_string(), "backend" => backend.to_string()).increment(1);
}

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn record_io_error(_operation: &str, _backend: &str) {}

/// Set the active I/O backend type
#[cfg(feature = "metrics")]
pub fn set_io_backend_type(is_uring: bool) {
    gauge!("streamline_io_backend_type").set(if is_uring { 1.0 } else { 0.0 });
}

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn set_io_backend_type(_is_uring: bool) {}

/// Update buffer pool metrics
#[cfg(feature = "metrics")]
pub fn update_buffer_pool_metrics(allocations: u64, reuses: u64) {
    gauge!("streamline_buffer_pool_allocations").set(allocations as f64);
    gauge!("streamline_buffer_pool_reuses").set(reuses as f64);
    if allocations > 0 {
        gauge!("streamline_buffer_pool_hit_rate")
            .set(reuses as f64 / (allocations + reuses) as f64);
    }
}

#[cfg(not(feature = "metrics"))]
#[inline]
pub fn update_buffer_pool_metrics(_allocations: u64, _reuses: u64) {}

// ============================================================================
// Tests (only with metrics feature)
// ============================================================================

#[cfg(all(test, feature = "metrics"))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_init_metrics() {
        let handle = init_metrics();
        let rendered = handle.render();
        // Just check that we can render metrics
        let _ = rendered;
    }

    #[tokio::test]
    async fn test_record_request() {
        let _handle = init_metrics();
        record_request("produce", Duration::from_millis(10), 1024, 512, false);
        record_request("fetch", Duration::from_millis(5), 256, 2048, false);
        record_request("metadata", Duration::from_millis(1), 128, 256, true);
    }

    #[tokio::test]
    async fn test_record_connection_metrics() {
        let _handle = init_metrics();
        record_connection_active(1);
        record_connection_active(1);
        record_connection_active(-1);
    }

    #[tokio::test]
    async fn test_record_storage_metrics() {
        let _handle = init_metrics();
        record_bytes_in("test-topic", 1024);
        record_bytes_out("test-topic", 2048);
        record_records_written("test-topic", 0, 10);
        update_partition_metrics("test-topic", 0, 1048576, 100, 5);
    }

    #[tokio::test]
    async fn test_update_topic_metrics() {
        let _handle = init_metrics();
        update_topics_total(5);
        update_partitions_total("test-topic", 3);
    }

    #[tokio::test]
    async fn test_replication_lag_metrics() {
        let _handle = init_metrics();
        record_replication_lag("test-topic", 0, 2, 100, 10240);
        record_replication_lag("test-topic", 1, 3, 50, 5120);
    }

    #[tokio::test]
    async fn test_isr_metrics() {
        let _handle = init_metrics();
        update_isr_size("test-topic", 0, 3);
        update_isr_size("test-topic", 1, 2);
        update_under_replicated_partitions(1);
    }

    #[tokio::test]
    async fn test_offline_partitions_metric() {
        let _handle = init_metrics();
        update_offline_partitions(0);
        update_offline_partitions(2);
    }

    #[tokio::test]
    async fn test_last_replication_time_metric() {
        let _handle = init_metrics();
        record_last_replication_time("test-topic", 0, 2, 1.5);
        record_last_replication_time("test-topic", 1, 3, 0.0);
        record_last_replication_time("test-topic", 0, 2, 5.2);
    }

    #[tokio::test]
    async fn test_cluster_metrics() {
        let _handle = init_metrics();
        update_cluster_nodes_total(3);
        update_cluster_leader(true);
        update_cluster_leader(false);
    }

    #[tokio::test]
    async fn test_multidc_replication_lag_metrics() {
        let _handle = init_metrics();
        record_multidc_replication_lag("dc2", "test-topic", 0, 1500, 100);
        record_multidc_replication_lag("dc3", "test-topic", 0, 3000, 200);
    }

    #[tokio::test]
    async fn test_multidc_records_and_bytes_metrics() {
        let _handle = init_metrics();
        record_multidc_records_replicated("dc2", 1000);
        record_multidc_bytes_replicated("dc2", 1024 * 1024);
    }

    #[tokio::test]
    async fn test_multidc_batch_metrics() {
        let _handle = init_metrics();
        record_multidc_batch_sent("dc2", true);
        record_multidc_batch_sent("dc2", true);
        record_multidc_batch_sent("dc2", false);
    }

    #[tokio::test]
    async fn test_multidc_connection_status() {
        let _handle = init_metrics();
        update_multidc_connection_status("dc2", true);
        update_multidc_connection_status("dc3", false);
    }

    #[tokio::test]
    async fn test_multidc_failover() {
        let _handle = init_metrics();
        record_multidc_failover("dc2", "dc3");
    }

    #[tokio::test]
    async fn test_consumer_group_lag_metrics() {
        let _handle = init_metrics();
        record_consumer_group_lag("test-group", "test-topic", 0, 100);
        record_consumer_group_lag("test-group", "test-topic", 1, 50);
    }

    #[tokio::test]
    async fn test_consumer_group_total_lag() {
        let _handle = init_metrics();
        record_consumer_group_total_lag("test-group", 150);
    }

    #[tokio::test]
    async fn test_consumer_group_members() {
        let _handle = init_metrics();
        record_consumer_group_members("test-group", 3);
    }

    #[tokio::test]
    async fn test_consumer_group_partitions() {
        let _handle = init_metrics();
        record_consumer_group_partitions("test-group", 6);
    }

    #[tokio::test]
    async fn test_consumer_group_max_lag() {
        let _handle = init_metrics();
        record_consumer_group_max_lag("test-group", 100);
    }

    #[tokio::test]
    async fn test_update_consumer_group_lag_metrics() {
        let _handle = init_metrics();
        let partitions = vec![
            ("topic1".to_string(), 0, 50i64),
            ("topic1".to_string(), 1, 75i64),
            ("topic2".to_string(), 0, 25i64),
        ];
        update_consumer_group_lag_metrics("test-group", &partitions, 150, 2);
    }

    #[tokio::test]
    async fn test_record_group_rebalance() {
        let _handle = init_metrics();
        record_group_rebalance("test-group", "member_join");
        record_group_rebalance("test-group", "member_leave");
        record_group_rebalance("another-group", "metadata_change");
    }

    #[tokio::test]
    async fn test_record_rebalance_duration() {
        let _handle = init_metrics();
        record_rebalance_duration("test-group", 500);
        record_rebalance_duration("test-group", 1200);
        record_rebalance_duration("another-group", 250);
    }

    #[tokio::test]
    async fn test_render_jmx_metrics() {
        let handle = init_metrics();
        let jmx_output = render_jmx_metrics(&handle);
        // Should be valid JSON with status 200
        assert!(jmx_output.contains("\"status\": 200"));
    }

    #[tokio::test]
    async fn test_partition_storage_mode_metric() {
        let _handle = init_metrics();
        // 0=local, 1=hybrid, 2=diskless
        update_partition_storage_mode("test-topic", 0, 0); // local
        update_partition_storage_mode("test-topic", 1, 2); // diskless
    }

    #[tokio::test]
    async fn test_partitions_by_storage_mode() {
        let _handle = init_metrics();
        update_partitions_by_storage_mode("local", 10);
        update_partitions_by_storage_mode("hybrid", 5);
        update_partitions_by_storage_mode("diskless", 3);
    }

    #[tokio::test]
    async fn test_s3_read_metrics() {
        let _handle = init_metrics();
        record_s3_read("test-topic", 0, 1024, 0.05);
        record_s3_read("test-topic", 1, 2048, 0.10);
    }

    #[tokio::test]
    async fn test_s3_write_metrics() {
        let _handle = init_metrics();
        record_s3_write("test-topic", 0, 4096, 0.15);
        record_s3_write("test-topic", 1, 8192, 0.20);
    }

    #[tokio::test]
    async fn test_s3_cache_hit_miss_metrics() {
        let _handle = init_metrics();
        record_s3_cache_hit("test-topic", 0);
        record_s3_cache_hit("test-topic", 0);
        record_s3_cache_miss("test-topic", 0);
    }

    #[tokio::test]
    async fn test_s3_cache_size_metric() {
        let _handle = init_metrics();
        update_s3_cache_size(100 * 1024 * 1024); // 100MB
    }

    #[tokio::test]
    async fn test_s3_error_metric() {
        let _handle = init_metrics();
        record_s3_error("test-topic", 0, "read");
        record_s3_error("test-topic", 0, "write");
    }

    #[tokio::test]
    async fn test_recovery_metrics() {
        let _handle = init_metrics();
        update_recovery_metrics(10, 8, 2);
    }

    #[tokio::test]
    async fn test_io_read_metrics() {
        let _handle = init_metrics();
        record_io_read(4096, Duration::from_micros(100), "standard");
        record_io_read(8192, Duration::from_micros(50), "io_uring");
    }

    #[tokio::test]
    async fn test_io_write_metrics() {
        let _handle = init_metrics();
        record_io_write(4096, Duration::from_micros(200), "standard");
        record_io_write(8192, Duration::from_micros(75), "io_uring");
    }

    #[tokio::test]
    async fn test_io_sync_metrics() {
        let _handle = init_metrics();
        record_io_sync(Duration::from_micros(500), "standard", "data");
        record_io_sync(Duration::from_micros(100), "io_uring", "metadata");
    }

    #[tokio::test]
    async fn test_io_error_metrics() {
        let _handle = init_metrics();
        record_io_error("read", "standard");
        record_io_error("write", "io_uring");
    }

    #[tokio::test]
    async fn test_io_backend_type_metric() {
        let _handle = init_metrics();
        set_io_backend_type(false); // standard
        set_io_backend_type(true); // io_uring
    }

    #[tokio::test]
    async fn test_buffer_pool_metrics() {
        let _handle = init_metrics();
        update_buffer_pool_metrics(100, 50); // 50 reuses out of 150 total
        update_buffer_pool_metrics(0, 0); // edge case: no allocations
    }
}
