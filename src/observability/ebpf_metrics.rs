//! eBPF Metrics Collector & Aggregator
//!
//! Aggregates eBPF events into actionable metrics for the observability dashboard.
//! Tracks per-connection throughput, disk I/O latencies, and syscall statistics.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Instant;
use tracing::debug;

/// Aggregates eBPF events into actionable metrics.
pub struct EbpfMetricsCollector {
    connection_metrics: Arc<RwLock<HashMap<String, ConnectionMetrics>>>,
    io_metrics: Arc<RwLock<IoMetricsAggregator>>,
    syscall_metrics: Arc<RwLock<SyscallMetrics>>,
    config: EbpfMetricsConfig,
    stats: Arc<EbpfCollectorStats>,
}

/// Configuration for eBPF metrics collection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EbpfMetricsConfig {
    /// Aggregation window in seconds.
    pub aggregation_window_secs: u64,
    /// Maximum number of connections to track.
    pub max_connections_tracked: usize,
    /// Whether to track syscall metrics.
    pub enable_syscall_tracking: bool,
    /// Whether to track I/O metrics.
    pub enable_io_tracking: bool,
    /// Number of top connections to report.
    pub top_n_connections: usize,
    /// Histogram latency buckets in milliseconds.
    pub histogram_buckets: Vec<f64>,
}

impl Default for EbpfMetricsConfig {
    fn default() -> Self {
        Self {
            aggregation_window_secs: 60,
            max_connections_tracked: 10000,
            enable_syscall_tracking: true,
            enable_io_tracking: true,
            top_n_connections: 20,
            histogram_buckets: vec![0.1, 0.5, 1.0, 5.0, 10.0, 50.0, 100.0, 500.0, 1000.0],
        }
    }
}

/// Per-connection traffic and latency metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionMetrics {
    pub remote_addr: String,
    pub local_port: u16,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub packets_sent: u64,
    pub packets_received: u64,
    /// Latency samples in microseconds.
    pub latency_samples: Vec<f64>,
    pub established_at: u64,
    pub last_activity_at: u64,
    pub retransmits: u64,
}

/// Aggregated disk I/O metrics with histograms.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IoMetricsAggregator {
    pub read_latency_us: HistogramAccumulator,
    pub write_latency_us: HistogramAccumulator,
    pub read_bytes_total: u64,
    pub write_bytes_total: u64,
    pub read_ops_total: u64,
    pub write_ops_total: u64,
    pub fsync_count: u64,
    pub fsync_latency_us: HistogramAccumulator,
}

/// Histogram with linear-interpolation percentile support.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistogramAccumulator {
    /// (upper_bound, count) pairs, sorted by upper_bound.
    pub buckets: Vec<(f64, u64)>,
    pub sum: f64,
    pub count: u64,
    pub min: f64,
    pub max: f64,
}

/// Syscall-level metrics keyed by syscall name.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SyscallMetrics {
    pub calls: HashMap<String, SyscallStats>,
}

/// Per-syscall statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyscallStats {
    pub count: u64,
    pub total_duration_us: f64,
    pub avg_duration_us: f64,
    pub max_duration_us: f64,
    pub errors: u64,
}

/// Dashboard snapshot combining all eBPF metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EbpfDashboard {
    pub top_connections: Vec<ConnectionSummary>,
    pub io_summary: IoSummary,
    pub syscall_summary: Vec<SyscallSummary>,
    pub overall: OverallEbpfStats,
}

/// Summary for a single connection (sorted by throughput).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionSummary {
    pub remote_addr: String,
    pub throughput_bytes_sec: f64,
    pub p50_latency_us: f64,
    pub p99_latency_us: f64,
    pub total_bytes: u64,
}

/// Summary of disk I/O throughput and latency.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IoSummary {
    pub read_throughput_bytes_sec: f64,
    pub write_throughput_bytes_sec: f64,
    pub read_p99_latency_us: f64,
    pub write_p99_latency_us: f64,
    pub fsync_p99_latency_us: f64,
}

/// Summary for a single syscall.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyscallSummary {
    pub name: String,
    pub calls_per_sec: f64,
    pub avg_latency_us: f64,
    pub error_rate: f64,
}

/// Overall collector statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OverallEbpfStats {
    pub total_connections_tracked: usize,
    pub total_io_ops: u64,
    pub total_syscalls: u64,
    pub collection_uptime_secs: u64,
}

/// Internal stats tracked via atomics for lock-free accounting.
pub struct EbpfCollectorStats {
    total_events: AtomicU64,
    start_time: Instant,
}

// -- HistogramAccumulator ---------------------------------------------------

impl HistogramAccumulator {
    /// Create a histogram with the given upper-bound bucket boundaries.
    pub fn new(buckets: &[f64]) -> Self {
        let mut sorted = buckets.to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        Self {
            buckets: sorted.into_iter().map(|b| (b, 0u64)).collect(),
            sum: 0.0,
            count: 0,
            min: f64::MAX,
            max: f64::MIN,
        }
    }

    /// Record a single observation.
    pub fn observe(&mut self, value: f64) {
        self.sum += value;
        self.count += 1;
        if value < self.min {
            self.min = value;
        }
        if value > self.max {
            self.max = value;
        }
        for bucket in &mut self.buckets {
            if value <= bucket.0 {
                bucket.1 += 1;
                return;
            }
        }
        // Value exceeds all bucket boundaries — count it in the last bucket.
        if let Some(last) = self.buckets.last_mut() {
            last.1 += 1;
        }
    }

    /// Compute a percentile (0.0–1.0) using linear interpolation between
    /// bucket boundaries.
    pub fn percentile(&self, p: f64) -> f64 {
        if self.count == 0 {
            return 0.0;
        }
        let target = p * self.count as f64;
        let mut cumulative: u64 = 0;
        let mut prev_bound: f64 = 0.0;

        for &(upper, count) in &self.buckets {
            cumulative += count;
            if cumulative as f64 >= target && count > 0 {
                let count_before = cumulative - count;
                let fraction = (target - count_before as f64) / count as f64;
                return prev_bound + fraction * (upper - prev_bound);
            }
            prev_bound = upper;
        }
        // Fallback: return max observed value.
        self.max
    }
}

// -- IoMetricsAggregator ----------------------------------------------------

impl IoMetricsAggregator {
    fn new(buckets: &[f64]) -> Self {
        Self {
            read_latency_us: HistogramAccumulator::new(buckets),
            write_latency_us: HistogramAccumulator::new(buckets),
            read_bytes_total: 0,
            write_bytes_total: 0,
            read_ops_total: 0,
            write_ops_total: 0,
            fsync_count: 0,
            fsync_latency_us: HistogramAccumulator::new(buckets),
        }
    }
}

// -- EbpfCollectorStats -----------------------------------------------------

impl EbpfCollectorStats {
    fn new() -> Self {
        Self {
            total_events: AtomicU64::new(0),
            start_time: Instant::now(),
        }
    }

    fn record_event(&self) {
        self.total_events.fetch_add(1, Ordering::Relaxed);
    }

    fn total_events(&self) -> u64 {
        self.total_events.load(Ordering::Relaxed)
    }

    fn uptime_secs(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }
}

// -- EbpfMetricsCollector ---------------------------------------------------

impl EbpfMetricsCollector {
    /// Create a new collector with the given configuration.
    pub fn new(config: EbpfMetricsConfig) -> Self {
        let io = IoMetricsAggregator::new(&config.histogram_buckets);
        Self {
            connection_metrics: Arc::new(RwLock::new(HashMap::new())),
            io_metrics: Arc::new(RwLock::new(io)),
            syscall_metrics: Arc::new(RwLock::new(SyscallMetrics::default())),
            config,
            stats: Arc::new(EbpfCollectorStats::new()),
        }
    }

    /// Record a connection-level event (bytes, latency).
    pub fn record_connection_event(
        &self,
        remote: &str,
        port: u16,
        bytes_sent: u64,
        bytes_recv: u64,
        latency_us: f64,
    ) {
        let mut conns = self.connection_metrics.write().unwrap();

        // Evict oldest entry when at capacity and this is a new key.
        if conns.len() >= self.config.max_connections_tracked && !conns.contains_key(remote) {
            if let Some(oldest_key) = conns
                .iter()
                .min_by_key(|(_, m)| m.last_activity_at)
                .map(|(k, _)| k.clone())
            {
                conns.remove(&oldest_key);
            }
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let entry = conns.entry(remote.to_string()).or_insert_with(|| {
            ConnectionMetrics {
                remote_addr: remote.to_string(),
                local_port: port,
                bytes_sent: 0,
                bytes_received: 0,
                packets_sent: 0,
                packets_received: 0,
                latency_samples: Vec::new(),
                established_at: now,
                last_activity_at: now,
                retransmits: 0,
            }
        });

        entry.bytes_sent += bytes_sent;
        entry.bytes_received += bytes_recv;
        if bytes_sent > 0 {
            entry.packets_sent += 1;
        }
        if bytes_recv > 0 {
            entry.packets_received += 1;
        }
        entry.latency_samples.push(latency_us);
        entry.last_activity_at = now;

        self.stats.record_event();
        debug!(remote, port, bytes_sent, bytes_recv, latency_us, "recorded connection event");
    }

    /// Record a disk I/O event.
    pub fn record_io_event(&self, is_read: bool, bytes: u64, latency_us: f64) {
        if !self.config.enable_io_tracking {
            return;
        }
        let mut io = self.io_metrics.write().unwrap();
        if is_read {
            io.read_latency_us.observe(latency_us);
            io.read_bytes_total += bytes;
            io.read_ops_total += 1;
        } else {
            io.write_latency_us.observe(latency_us);
            io.write_bytes_total += bytes;
            io.write_ops_total += 1;
        }
        self.stats.record_event();
    }

    /// Record an fsync operation.
    pub fn record_fsync(&self, latency_us: f64) {
        if !self.config.enable_io_tracking {
            return;
        }
        let mut io = self.io_metrics.write().unwrap();
        io.fsync_count += 1;
        io.fsync_latency_us.observe(latency_us);
        self.stats.record_event();
    }

    /// Record a syscall invocation.
    pub fn record_syscall(&self, name: &str, duration_us: f64, error: bool) {
        if !self.config.enable_syscall_tracking {
            return;
        }
        let mut sc = self.syscall_metrics.write().unwrap();
        let entry = sc.calls.entry(name.to_string()).or_insert_with(|| SyscallStats {
            count: 0,
            total_duration_us: 0.0,
            avg_duration_us: 0.0,
            max_duration_us: 0.0,
            errors: 0,
        });
        entry.count += 1;
        entry.total_duration_us += duration_us;
        entry.avg_duration_us = entry.total_duration_us / entry.count as f64;
        if duration_us > entry.max_duration_us {
            entry.max_duration_us = duration_us;
        }
        if error {
            entry.errors += 1;
        }
        self.stats.record_event();
    }

    /// Build a dashboard snapshot over the given time window.
    pub fn get_dashboard(&self, window_secs: u64) -> EbpfDashboard {
        let window = if window_secs == 0 { 1 } else { window_secs };

        let top_connections = self.get_top_connections(self.config.top_n_connections);

        let io = self.io_metrics.read().unwrap();
        let io_summary = IoSummary {
            read_throughput_bytes_sec: io.read_bytes_total as f64 / window as f64,
            write_throughput_bytes_sec: io.write_bytes_total as f64 / window as f64,
            read_p99_latency_us: io.read_latency_us.percentile(0.99),
            write_p99_latency_us: io.write_latency_us.percentile(0.99),
            fsync_p99_latency_us: io.fsync_latency_us.percentile(0.99),
        };

        let sc = self.syscall_metrics.read().unwrap();
        let syscall_summary: Vec<SyscallSummary> = sc
            .calls
            .iter()
            .map(|(name, s)| SyscallSummary {
                name: name.clone(),
                calls_per_sec: s.count as f64 / window as f64,
                avg_latency_us: s.avg_duration_us,
                error_rate: if s.count > 0 {
                    s.errors as f64 / s.count as f64
                } else {
                    0.0
                },
            })
            .collect();

        let conns = self.connection_metrics.read().unwrap();
        let total_io_ops = io.read_ops_total + io.write_ops_total + io.fsync_count;
        let total_syscalls: u64 = sc.calls.values().map(|s| s.count).sum();

        EbpfDashboard {
            top_connections,
            io_summary,
            syscall_summary,
            overall: OverallEbpfStats {
                total_connections_tracked: conns.len(),
                total_io_ops,
                total_syscalls,
                collection_uptime_secs: self.stats.uptime_secs(),
            },
        }
    }

    /// Return the top-N connections ranked by total bytes transferred.
    pub fn get_top_connections(&self, n: usize) -> Vec<ConnectionSummary> {
        let conns = self.connection_metrics.read().unwrap();
        let mut entries: Vec<_> = conns.values().collect();
        entries.sort_by(|a, b| {
            let total_a = a.bytes_sent + a.bytes_received;
            let total_b = b.bytes_sent + b.bytes_received;
            total_b.cmp(&total_a)
        });

        entries
            .into_iter()
            .take(n)
            .map(|m| {
                let total_bytes = m.bytes_sent + m.bytes_received;
                let duration_secs = m.last_activity_at.saturating_sub(m.established_at).max(1);
                let throughput = total_bytes as f64 / duration_secs as f64;

                let (p50, p99) = latency_percentiles(&m.latency_samples);
                ConnectionSummary {
                    remote_addr: m.remote_addr.clone(),
                    throughput_bytes_sec: throughput,
                    p50_latency_us: p50,
                    p99_latency_us: p99,
                    total_bytes,
                }
            })
            .collect()
    }

    /// Reset all collected metrics.
    pub fn reset(&self) {
        self.connection_metrics.write().unwrap().clear();
        {
            let mut io = self.io_metrics.write().unwrap();
            *io = IoMetricsAggregator::new(&self.config.histogram_buckets);
        }
        self.syscall_metrics.write().unwrap().calls.clear();
    }
}

/// Compute p50 and p99 from a sorted copy of latency samples.
fn latency_percentiles(samples: &[f64]) -> (f64, f64) {
    if samples.is_empty() {
        return (0.0, 0.0);
    }
    let mut sorted = samples.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let p50 = percentile_sorted(&sorted, 0.50);
    let p99 = percentile_sorted(&sorted, 0.99);
    (p50, p99)
}

fn percentile_sorted(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = (p * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn default_config() -> EbpfMetricsConfig {
        EbpfMetricsConfig::default()
    }

    // -- EbpfMetricsConfig --------------------------------------------------

    #[test]
    fn test_default_config() {
        let cfg = default_config();
        assert_eq!(cfg.aggregation_window_secs, 60);
        assert_eq!(cfg.max_connections_tracked, 10000);
        assert!(cfg.enable_syscall_tracking);
        assert!(cfg.enable_io_tracking);
        assert_eq!(cfg.top_n_connections, 20);
        assert!(!cfg.histogram_buckets.is_empty());
    }

    // -- HistogramAccumulator -----------------------------------------------

    #[test]
    fn test_histogram_new_empty() {
        let h = HistogramAccumulator::new(&[1.0, 5.0, 10.0]);
        assert_eq!(h.count, 0);
        assert_eq!(h.sum, 0.0);
        assert_eq!(h.buckets.len(), 3);
    }

    #[test]
    fn test_histogram_observe_single() {
        let mut h = HistogramAccumulator::new(&[10.0, 50.0, 100.0]);
        h.observe(5.0);
        assert_eq!(h.count, 1);
        assert_eq!(h.sum, 5.0);
        assert_eq!(h.min, 5.0);
        assert_eq!(h.max, 5.0);
        assert_eq!(h.buckets[0].1, 1); // 5.0 <= 10.0
    }

    #[test]
    fn test_histogram_observe_multiple() {
        let mut h = HistogramAccumulator::new(&[10.0, 50.0, 100.0]);
        h.observe(5.0);
        h.observe(15.0);
        h.observe(75.0);
        assert_eq!(h.count, 3);
        assert_eq!(h.sum, 95.0);
        assert_eq!(h.min, 5.0);
        assert_eq!(h.max, 75.0);
        assert_eq!(h.buckets[0].1, 1); // <=10
        assert_eq!(h.buckets[1].1, 1); // <=50
        assert_eq!(h.buckets[2].1, 1); // <=100
    }

    #[test]
    fn test_histogram_observe_exceeds_all_buckets() {
        let mut h = HistogramAccumulator::new(&[10.0, 50.0]);
        h.observe(999.0);
        assert_eq!(h.count, 1);
        // Falls into last bucket.
        assert_eq!(h.buckets[1].1, 1);
    }

    #[test]
    fn test_histogram_percentile_empty() {
        let h = HistogramAccumulator::new(&[10.0, 50.0]);
        assert_eq!(h.percentile(0.5), 0.0);
    }

    #[test]
    fn test_histogram_percentile_single_value() {
        let mut h = HistogramAccumulator::new(&[10.0, 50.0, 100.0]);
        h.observe(5.0);
        let p50 = h.percentile(0.5);
        // With one value in bucket [0, 10], p50 = 0 + 0.5 * (10 - 0) = 5.0
        assert!((p50 - 5.0).abs() < 1e-9);
    }

    #[test]
    fn test_histogram_percentile_linear_interpolation() {
        let mut h = HistogramAccumulator::new(&[10.0, 20.0]);
        // 4 samples all in bucket [0, 10]
        for _ in 0..4 {
            h.observe(5.0);
        }
        // p25 target = 0.25 * 4 = 1.0
        // bucket [0,10] has 4 items; fraction = 1.0/4 = 0.25
        // result = 0 + 0.25 * 10 = 2.5
        let p25 = h.percentile(0.25);
        assert!((p25 - 2.5).abs() < 1e-9);
    }

    #[test]
    fn test_histogram_percentile_across_buckets() {
        let mut h = HistogramAccumulator::new(&[10.0, 100.0]);
        // 5 in first bucket, 5 in second
        for _ in 0..5 {
            h.observe(5.0);
        }
        for _ in 0..5 {
            h.observe(50.0);
        }
        // p90 target = 0.9 * 10 = 9.0
        // first bucket has 5 (cumulative=5 < 9), second bucket has 5 (cumulative=10 >= 9)
        // fraction = (9 - 5) / 5 = 0.8
        // result = 10 + 0.8 * (100 - 10) = 10 + 72 = 82.0
        let p90 = h.percentile(0.9);
        assert!((p90 - 82.0).abs() < 1e-9);
    }

    // -- EbpfMetricsCollector: construction ----------------------------------

    #[test]
    fn test_collector_new() {
        let c = EbpfMetricsCollector::new(default_config());
        assert_eq!(c.stats.total_events(), 0);
    }

    // -- record_connection_event --------------------------------------------

    #[test]
    fn test_record_connection_event() {
        let c = EbpfMetricsCollector::new(default_config());
        c.record_connection_event("10.0.0.1", 9092, 100, 200, 50.0);
        let conns = c.connection_metrics.read().unwrap();
        let m = conns.get("10.0.0.1").unwrap();
        assert_eq!(m.bytes_sent, 100);
        assert_eq!(m.bytes_received, 200);
        assert_eq!(m.packets_sent, 1);
        assert_eq!(m.packets_received, 1);
        assert_eq!(m.latency_samples.len(), 1);
    }

    #[test]
    fn test_record_connection_event_accumulates() {
        let c = EbpfMetricsCollector::new(default_config());
        c.record_connection_event("10.0.0.1", 9092, 100, 200, 50.0);
        c.record_connection_event("10.0.0.1", 9092, 50, 100, 30.0);
        let conns = c.connection_metrics.read().unwrap();
        let m = conns.get("10.0.0.1").unwrap();
        assert_eq!(m.bytes_sent, 150);
        assert_eq!(m.bytes_received, 300);
        assert_eq!(m.latency_samples.len(), 2);
    }

    #[test]
    fn test_connection_eviction_at_capacity() {
        let mut cfg = default_config();
        cfg.max_connections_tracked = 2;
        let c = EbpfMetricsCollector::new(cfg);
        c.record_connection_event("a", 1, 1, 1, 1.0);
        c.record_connection_event("b", 2, 1, 1, 1.0);
        // Adding a 3rd should evict one.
        c.record_connection_event("c", 3, 1, 1, 1.0);
        let conns = c.connection_metrics.read().unwrap();
        assert_eq!(conns.len(), 2);
        assert!(conns.contains_key("c"));
    }

    // -- record_io_event ----------------------------------------------------

    #[test]
    fn test_record_io_read() {
        let c = EbpfMetricsCollector::new(default_config());
        c.record_io_event(true, 4096, 120.0);
        let io = c.io_metrics.read().unwrap();
        assert_eq!(io.read_ops_total, 1);
        assert_eq!(io.read_bytes_total, 4096);
        assert_eq!(io.write_ops_total, 0);
    }

    #[test]
    fn test_record_io_write() {
        let c = EbpfMetricsCollector::new(default_config());
        c.record_io_event(false, 8192, 250.0);
        let io = c.io_metrics.read().unwrap();
        assert_eq!(io.write_ops_total, 1);
        assert_eq!(io.write_bytes_total, 8192);
    }

    #[test]
    fn test_io_tracking_disabled() {
        let mut cfg = default_config();
        cfg.enable_io_tracking = false;
        let c = EbpfMetricsCollector::new(cfg);
        c.record_io_event(true, 4096, 100.0);
        let io = c.io_metrics.read().unwrap();
        assert_eq!(io.read_ops_total, 0);
    }

    // -- record_fsync -------------------------------------------------------

    #[test]
    fn test_record_fsync() {
        let c = EbpfMetricsCollector::new(default_config());
        c.record_fsync(500.0);
        c.record_fsync(600.0);
        let io = c.io_metrics.read().unwrap();
        assert_eq!(io.fsync_count, 2);
        assert_eq!(io.fsync_latency_us.count, 2);
    }

    // -- record_syscall -----------------------------------------------------

    #[test]
    fn test_record_syscall() {
        let c = EbpfMetricsCollector::new(default_config());
        c.record_syscall("read", 10.0, false);
        c.record_syscall("read", 20.0, true);
        let sc = c.syscall_metrics.read().unwrap();
        let s = sc.calls.get("read").unwrap();
        assert_eq!(s.count, 2);
        assert_eq!(s.errors, 1);
        assert!((s.avg_duration_us - 15.0).abs() < 1e-9);
        assert!((s.max_duration_us - 20.0).abs() < 1e-9);
    }

    #[test]
    fn test_syscall_tracking_disabled() {
        let mut cfg = default_config();
        cfg.enable_syscall_tracking = false;
        let c = EbpfMetricsCollector::new(cfg);
        c.record_syscall("write", 5.0, false);
        let sc = c.syscall_metrics.read().unwrap();
        assert!(sc.calls.is_empty());
    }

    // -- get_top_connections ------------------------------------------------

    #[test]
    fn test_get_top_connections_order() {
        let c = EbpfMetricsCollector::new(default_config());
        c.record_connection_event("small", 1, 10, 10, 1.0);
        c.record_connection_event("big", 2, 1000, 1000, 1.0);
        c.record_connection_event("medium", 3, 100, 100, 1.0);

        let top = c.get_top_connections(2);
        assert_eq!(top.len(), 2);
        assert_eq!(top[0].remote_addr, "big");
        assert_eq!(top[1].remote_addr, "medium");
    }

    // -- get_dashboard ------------------------------------------------------

    #[test]
    fn test_get_dashboard() {
        let c = EbpfMetricsCollector::new(default_config());
        c.record_connection_event("10.0.0.1", 9092, 100, 200, 50.0);
        c.record_io_event(true, 4096, 100.0);
        c.record_io_event(false, 8192, 200.0);
        c.record_fsync(300.0);
        c.record_syscall("read", 10.0, false);

        let dash = c.get_dashboard(60);
        assert_eq!(dash.top_connections.len(), 1);
        assert_eq!(dash.overall.total_connections_tracked, 1);
        assert_eq!(dash.overall.total_io_ops, 3); // 1 read + 1 write + 1 fsync
        assert_eq!(dash.overall.total_syscalls, 1);
        assert!(dash.io_summary.read_throughput_bytes_sec > 0.0);
        assert!(dash.io_summary.write_throughput_bytes_sec > 0.0);
    }

    #[test]
    fn test_dashboard_with_zero_window() {
        let c = EbpfMetricsCollector::new(default_config());
        c.record_io_event(true, 1000, 10.0);
        // window_secs=0 should be clamped to 1 to avoid division by zero.
        let dash = c.get_dashboard(0);
        assert_eq!(dash.io_summary.read_throughput_bytes_sec, 1000.0);
    }

    // -- reset --------------------------------------------------------------

    #[test]
    fn test_reset() {
        let c = EbpfMetricsCollector::new(default_config());
        c.record_connection_event("10.0.0.1", 9092, 100, 200, 50.0);
        c.record_io_event(true, 4096, 100.0);
        c.record_syscall("read", 10.0, false);

        c.reset();

        let conns = c.connection_metrics.read().unwrap();
        assert!(conns.is_empty());
        let io = c.io_metrics.read().unwrap();
        assert_eq!(io.read_ops_total, 0);
        let sc = c.syscall_metrics.read().unwrap();
        assert!(sc.calls.is_empty());
    }

    // -- stats tracking -----------------------------------------------------

    #[test]
    fn test_stats_event_counting() {
        let c = EbpfMetricsCollector::new(default_config());
        c.record_connection_event("a", 1, 1, 1, 1.0);
        c.record_io_event(true, 1, 1.0);
        c.record_syscall("x", 1.0, false);
        assert_eq!(c.stats.total_events(), 3);
    }

    // -- latency_percentiles helper -----------------------------------------

    #[test]
    fn test_latency_percentiles_empty() {
        let (p50, p99) = latency_percentiles(&[]);
        assert_eq!(p50, 0.0);
        assert_eq!(p99, 0.0);
    }

    #[test]
    fn test_latency_percentiles_values() {
        let samples: Vec<f64> = (1..=100).map(|v| v as f64).collect();
        let (p50, p99) = latency_percentiles(&samples);
        assert!((p50 - 50.0).abs() < 1.5);
        assert!((p99 - 99.0).abs() < 1.5);
    }

    // -- ConnectionSummary via get_top_connections ---------------------------

    #[test]
    fn test_connection_summary_fields() {
        let c = EbpfMetricsCollector::new(default_config());
        c.record_connection_event("host", 9092, 500, 500, 100.0);
        let top = c.get_top_connections(1);
        assert_eq!(top.len(), 1);
        assert_eq!(top[0].total_bytes, 1000);
        assert_eq!(top[0].remote_addr, "host");
    }

    // -- IoSummary via get_dashboard ----------------------------------------

    #[test]
    fn test_io_summary_latencies() {
        let c = EbpfMetricsCollector::new(default_config());
        for i in 0..100 {
            c.record_io_event(true, 100, i as f64);
            c.record_io_event(false, 100, i as f64 * 2.0);
        }
        c.record_fsync(42.0);
        let dash = c.get_dashboard(10);
        assert!(dash.io_summary.read_p99_latency_us > 0.0);
        assert!(dash.io_summary.write_p99_latency_us > 0.0);
        assert!(dash.io_summary.fsync_p99_latency_us > 0.0);
    }

    // -- SyscallSummary via get_dashboard -----------------------------------

    #[test]
    fn test_syscall_summary_error_rate() {
        let c = EbpfMetricsCollector::new(default_config());
        c.record_syscall("write", 5.0, false);
        c.record_syscall("write", 5.0, true);
        let dash = c.get_dashboard(10);
        let ws = dash.syscall_summary.iter().find(|s| s.name == "write").unwrap();
        assert!((ws.error_rate - 0.5).abs() < 1e-9);
        assert!((ws.calls_per_sec - 0.2).abs() < 1e-9);
    }
}
