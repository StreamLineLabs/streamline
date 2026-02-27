//! Observability module with eBPF support
//!
//! Provides system-level observability metrics including:
//! - CPU and memory usage
//! - Network I/O statistics
//! - Disk I/O latency
//! - Connection tracking
//! - eBPF-based kernel metrics (Linux only)
//!
//! The module provides cross-platform metrics with enhanced
//! eBPF-based observability on Linux systems.

pub mod alerting;
pub mod dashboard;
pub mod ebpf_metrics;
pub mod metrics;
pub mod otel_exporter;
pub mod system;

#[cfg(target_os = "linux")]
pub mod ebpf;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tracing::{debug, info, warn};

/// Observability configuration
#[derive(Debug, Clone)]
pub struct ObservabilityConfig {
    /// Enable system metrics collection
    pub enable_system_metrics: bool,
    /// Enable network metrics
    pub enable_network_metrics: bool,
    /// Enable disk I/O metrics
    pub enable_disk_metrics: bool,
    /// Enable eBPF-based metrics (Linux only)
    pub enable_ebpf: bool,
    /// Metrics collection interval in milliseconds
    pub collection_interval_ms: u64,
    /// Maximum metrics history to retain
    pub max_history: usize,
    /// Enable per-connection tracking
    pub enable_connection_tracking: bool,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            enable_system_metrics: true,
            enable_network_metrics: true,
            enable_disk_metrics: true,
            enable_ebpf: cfg!(target_os = "linux"),
            collection_interval_ms: 1000,
            max_history: 3600, // 1 hour at 1s intervals
            enable_connection_tracking: true,
        }
    }
}

/// System metrics snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemMetrics {
    /// Timestamp of collection
    pub timestamp: i64,
    /// CPU metrics
    pub cpu: CpuMetrics,
    /// Memory metrics
    pub memory: MemoryMetrics,
    /// Network metrics
    pub network: NetworkMetrics,
    /// Disk I/O metrics
    pub disk: DiskMetrics,
    /// Process metrics
    pub process: ProcessMetrics,
}

/// CPU metrics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CpuMetrics {
    /// CPU usage percentage (0-100 per core)
    pub usage_percent: f64,
    /// User CPU time percentage
    pub user_percent: f64,
    /// System CPU time percentage
    pub system_percent: f64,
    /// IO wait percentage
    pub iowait_percent: f64,
    /// Number of CPU cores
    pub cores: u32,
    /// Load average (1min, 5min, 15min)
    pub load_average: [f64; 3],
}

/// Memory metrics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MemoryMetrics {
    /// Total memory in bytes
    pub total_bytes: u64,
    /// Used memory in bytes
    pub used_bytes: u64,
    /// Free memory in bytes
    pub free_bytes: u64,
    /// Available memory in bytes
    pub available_bytes: u64,
    /// Cached memory in bytes
    pub cached_bytes: u64,
    /// Buffer memory in bytes
    pub buffers_bytes: u64,
    /// Memory usage percentage
    pub usage_percent: f64,
}

/// Network metrics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NetworkMetrics {
    /// Bytes received per second
    pub rx_bytes_per_sec: u64,
    /// Bytes transmitted per second
    pub tx_bytes_per_sec: u64,
    /// Packets received per second
    pub rx_packets_per_sec: u64,
    /// Packets transmitted per second
    pub tx_packets_per_sec: u64,
    /// Receive errors
    pub rx_errors: u64,
    /// Transmit errors
    pub tx_errors: u64,
    /// Active connections
    pub active_connections: u64,
    /// Connection rate (new connections per second)
    pub connection_rate: f64,
}

/// Disk I/O metrics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DiskMetrics {
    /// Read bytes per second
    pub read_bytes_per_sec: u64,
    /// Write bytes per second
    pub write_bytes_per_sec: u64,
    /// Read operations per second
    pub read_ops_per_sec: u64,
    /// Write operations per second
    pub write_ops_per_sec: u64,
    /// Average read latency in microseconds
    pub read_latency_us: u64,
    /// Average write latency in microseconds
    pub write_latency_us: u64,
    /// Disk utilization percentage
    pub utilization_percent: f64,
    /// Queue depth
    pub queue_depth: u64,
}

/// Process-specific metrics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProcessMetrics {
    /// Process CPU usage percentage
    pub cpu_percent: f64,
    /// Process memory (RSS) in bytes
    pub memory_rss_bytes: u64,
    /// Process virtual memory in bytes
    pub memory_vms_bytes: u64,
    /// Open file descriptors
    pub open_fds: u64,
    /// Thread count
    pub thread_count: u64,
    /// Context switches per second
    pub context_switches_per_sec: u64,
}

/// Observability manager
pub struct ObservabilityManager {
    /// Configuration
    config: ObservabilityConfig,
    /// Metrics history
    history: RwLock<Vec<SystemMetrics>>,
    /// Connection tracker
    connections: RwLock<HashMap<String, ConnectionInfo>>,
    /// Latency histograms
    latencies: RwLock<LatencyHistograms>,
    /// Start time
    start_time: Instant,
    /// Collection counter
    collection_count: AtomicU64,
    /// eBPF available flag
    ebpf_available: bool,
}

/// Connection information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionInfo {
    /// Connection ID
    pub id: String,
    /// Remote address
    pub remote_addr: String,
    /// Local port
    pub local_port: u16,
    /// Connection start time (ms since epoch)
    pub started_at: i64,
    /// Bytes sent
    pub bytes_sent: u64,
    /// Bytes received
    pub bytes_received: u64,
    /// Request count
    pub request_count: u64,
    /// Last activity time
    pub last_activity: i64,
    /// Protocol (kafka, http, quic)
    pub protocol: String,
}

/// Latency histograms for various operations
#[derive(Debug, Clone, Default)]
pub struct LatencyHistograms {
    /// Produce latencies (microseconds)
    pub produce: LatencyHistogram,
    /// Fetch latencies (microseconds)
    pub fetch: LatencyHistogram,
    /// Storage write latencies (microseconds)
    pub storage_write: LatencyHistogram,
    /// Storage read latencies (microseconds)
    pub storage_read: LatencyHistogram,
    /// Network send latencies (microseconds)
    pub network_send: LatencyHistogram,
    /// Network receive latencies (microseconds)
    pub network_recv: LatencyHistogram,
}

/// Latency histogram for tracking operation timings
#[derive(Debug, Clone, Default)]
pub struct LatencyHistogram {
    /// Count of samples
    pub count: u64,
    /// Sum of all samples
    pub sum: u64,
    /// Minimum value
    pub min: u64,
    /// Maximum value
    pub max: u64,
    /// P50 (median)
    pub p50: u64,
    /// P90
    pub p90: u64,
    /// P99
    pub p99: u64,
    /// P999
    pub p999: u64,
    /// Buckets for histogram
    buckets: Vec<u64>,
}

impl LatencyHistogram {
    /// Create a new histogram with default buckets
    pub fn new() -> Self {
        // Buckets: 1us, 10us, 100us, 1ms, 10ms, 100ms, 1s, 10s
        Self {
            buckets: vec![0; 8],
            ..Default::default()
        }
    }

    /// Record a latency sample
    pub fn record(&mut self, latency_us: u64) {
        self.count += 1;
        self.sum += latency_us;

        if self.min == 0 || latency_us < self.min {
            self.min = latency_us;
        }
        if latency_us > self.max {
            self.max = latency_us;
        }

        // Update bucket
        let bucket = if latency_us < 1 {
            0
        } else if latency_us < 10 {
            1
        } else if latency_us < 100 {
            2
        } else if latency_us < 1_000 {
            3
        } else if latency_us < 10_000 {
            4
        } else if latency_us < 100_000 {
            5
        } else if latency_us < 1_000_000 {
            6
        } else {
            7
        };
        if bucket < self.buckets.len() {
            self.buckets[bucket] += 1;
        }
    }

    /// Get mean latency
    pub fn mean(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum as f64 / self.count as f64
        }
    }
}

impl ObservabilityManager {
    /// Create a new observability manager
    pub fn new(config: ObservabilityConfig) -> Self {
        let ebpf_available = cfg!(target_os = "linux") && config.enable_ebpf;

        if ebpf_available {
            info!("eBPF observability enabled");
        } else if config.enable_ebpf {
            warn!("eBPF requested but not available (requires Linux)");
        }

        Self {
            config,
            history: RwLock::new(Vec::new()),
            connections: RwLock::new(HashMap::new()),
            latencies: RwLock::new(LatencyHistograms::default()),
            start_time: Instant::now(),
            collection_count: AtomicU64::new(0),
            ebpf_available,
        }
    }

    /// Collect current system metrics
    pub fn collect(&self) -> SystemMetrics {
        let timestamp = chrono::Utc::now().timestamp_millis();

        let cpu = self.collect_cpu_metrics();
        let memory = self.collect_memory_metrics();
        let network = self.collect_network_metrics();
        let disk = self.collect_disk_metrics();
        let process = self.collect_process_metrics();

        let metrics = SystemMetrics {
            timestamp,
            cpu,
            memory,
            network,
            disk,
            process,
        };

        // Store in history
        let mut history = self.history.write();
        history.push(metrics.clone());
        if history.len() > self.config.max_history {
            history.remove(0);
        }

        self.collection_count.fetch_add(1, Ordering::Relaxed);

        debug!("Collected system metrics");
        metrics
    }

    /// Collect CPU metrics
    fn collect_cpu_metrics(&self) -> CpuMetrics {
        #[cfg(target_os = "linux")]
        {
            self.collect_cpu_linux()
        }
        #[cfg(target_os = "macos")]
        {
            self.collect_cpu_macos()
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            CpuMetrics::default()
        }
    }

    #[cfg(target_os = "linux")]
    fn collect_cpu_linux(&self) -> CpuMetrics {
        use std::fs;

        let mut metrics = CpuMetrics::default();

        // Read /proc/stat
        if let Ok(stat) = fs::read_to_string("/proc/stat") {
            if let Some(cpu_line) = stat.lines().next() {
                let parts: Vec<&str> = cpu_line.split_whitespace().collect();
                if parts.len() >= 8 {
                    let user: u64 = parts[1].parse().unwrap_or(0);
                    let nice: u64 = parts[2].parse().unwrap_or(0);
                    let system: u64 = parts[3].parse().unwrap_or(0);
                    let idle: u64 = parts[4].parse().unwrap_or(0);
                    let iowait: u64 = parts[5].parse().unwrap_or(0);

                    let total = user + nice + system + idle + iowait;
                    if total > 0 {
                        metrics.user_percent = (user + nice) as f64 / total as f64 * 100.0;
                        metrics.system_percent = system as f64 / total as f64 * 100.0;
                        metrics.iowait_percent = iowait as f64 / total as f64 * 100.0;
                        metrics.usage_percent = 100.0 - (idle as f64 / total as f64 * 100.0);
                    }
                }
            }
        }

        // Read /proc/cpuinfo for core count
        if let Ok(cpuinfo) = fs::read_to_string("/proc/cpuinfo") {
            metrics.cores = cpuinfo.matches("processor").count() as u32;
        }

        // Read /proc/loadavg
        if let Ok(loadavg) = fs::read_to_string("/proc/loadavg") {
            let parts: Vec<&str> = loadavg.split_whitespace().collect();
            if parts.len() >= 3 {
                metrics.load_average[0] = parts[0].parse().unwrap_or(0.0);
                metrics.load_average[1] = parts[1].parse().unwrap_or(0.0);
                metrics.load_average[2] = parts[2].parse().unwrap_or(0.0);
            }
        }

        metrics
    }

    #[cfg(target_os = "macos")]
    fn collect_cpu_macos(&self) -> CpuMetrics {
        // macOS implementation using sysctl would go here
        // For now, return default metrics
        CpuMetrics {
            cores: num_cpus(),
            ..Default::default()
        }
    }

    /// Collect memory metrics
    fn collect_memory_metrics(&self) -> MemoryMetrics {
        #[cfg(target_os = "linux")]
        {
            self.collect_memory_linux()
        }
        #[cfg(not(target_os = "linux"))]
        {
            MemoryMetrics::default()
        }
    }

    #[cfg(target_os = "linux")]
    fn collect_memory_linux(&self) -> MemoryMetrics {
        use std::fs;

        let mut metrics = MemoryMetrics::default();

        if let Ok(meminfo) = fs::read_to_string("/proc/meminfo") {
            for line in meminfo.lines() {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 2 {
                    let value: u64 = parts[1].parse().unwrap_or(0) * 1024; // Convert kB to bytes
                    match parts[0] {
                        "MemTotal:" => metrics.total_bytes = value,
                        "MemFree:" => metrics.free_bytes = value,
                        "MemAvailable:" => metrics.available_bytes = value,
                        "Buffers:" => metrics.buffers_bytes = value,
                        "Cached:" => metrics.cached_bytes = value,
                        _ => {}
                    }
                }
            }
        }

        metrics.used_bytes = metrics.total_bytes.saturating_sub(metrics.available_bytes);
        if metrics.total_bytes > 0 {
            metrics.usage_percent = metrics.used_bytes as f64 / metrics.total_bytes as f64 * 100.0;
        }

        metrics
    }

    /// Collect network metrics
    fn collect_network_metrics(&self) -> NetworkMetrics {
        if !self.config.enable_network_metrics {
            return NetworkMetrics::default();
        }

        #[cfg(target_os = "linux")]
        {
            self.collect_network_linux()
        }
        #[cfg(not(target_os = "linux"))]
        {
            NetworkMetrics {
                active_connections: self.connections.read().len() as u64,
                ..Default::default()
            }
        }
    }

    #[cfg(target_os = "linux")]
    fn collect_network_linux(&self) -> NetworkMetrics {
        use std::fs;

        let mut metrics = NetworkMetrics::default();

        if let Ok(net_dev) = fs::read_to_string("/proc/net/dev") {
            for line in net_dev.lines().skip(2) {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 10 {
                    let rx_bytes: u64 = parts[1].parse().unwrap_or(0);
                    let rx_packets: u64 = parts[2].parse().unwrap_or(0);
                    let rx_errors: u64 = parts[3].parse().unwrap_or(0);
                    let tx_bytes: u64 = parts[9].parse().unwrap_or(0);
                    let tx_packets: u64 = parts[10].parse().unwrap_or(0);
                    let tx_errors: u64 = parts[11].parse().unwrap_or(0);

                    metrics.rx_bytes_per_sec += rx_bytes;
                    metrics.tx_bytes_per_sec += tx_bytes;
                    metrics.rx_packets_per_sec += rx_packets;
                    metrics.tx_packets_per_sec += tx_packets;
                    metrics.rx_errors += rx_errors;
                    metrics.tx_errors += tx_errors;
                }
            }
        }

        metrics.active_connections = self.connections.read().len() as u64;
        metrics
    }

    /// Collect disk I/O metrics
    fn collect_disk_metrics(&self) -> DiskMetrics {
        if !self.config.enable_disk_metrics {
            return DiskMetrics::default();
        }

        #[cfg(target_os = "linux")]
        {
            self.collect_disk_linux()
        }
        #[cfg(not(target_os = "linux"))]
        {
            DiskMetrics::default()
        }
    }

    #[cfg(target_os = "linux")]
    fn collect_disk_linux(&self) -> DiskMetrics {
        use std::fs;

        let mut metrics = DiskMetrics::default();

        if let Ok(diskstats) = fs::read_to_string("/proc/diskstats") {
            for line in diskstats.lines() {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 14 {
                    // Only count main devices (not partitions)
                    let device = parts[2];
                    if device.starts_with("sd") || device.starts_with("nvme") {
                        if device.ends_with(char::is_numeric) && !device.contains('p') {
                            continue; // Skip partitions
                        }
                        let read_ops: u64 = parts[3].parse().unwrap_or(0);
                        let read_sectors: u64 = parts[5].parse().unwrap_or(0);
                        let write_ops: u64 = parts[7].parse().unwrap_or(0);
                        let write_sectors: u64 = parts[9].parse().unwrap_or(0);
                        let io_time: u64 = parts[12].parse().unwrap_or(0);

                        metrics.read_ops_per_sec += read_ops;
                        metrics.read_bytes_per_sec += read_sectors * 512;
                        metrics.write_ops_per_sec += write_ops;
                        metrics.write_bytes_per_sec += write_sectors * 512;
                        metrics.utilization_percent += io_time as f64 / 10.0; // io_time is in ms
                    }
                }
            }
        }

        metrics
    }

    /// Collect process metrics
    fn collect_process_metrics(&self) -> ProcessMetrics {
        #[cfg(target_os = "linux")]
        {
            self.collect_process_linux()
        }
        #[cfg(not(target_os = "linux"))]
        {
            ProcessMetrics::default()
        }
    }

    #[cfg(target_os = "linux")]
    fn collect_process_linux(&self) -> ProcessMetrics {
        use std::fs;

        let mut metrics = ProcessMetrics::default();
        let pid = std::process::id();

        // Read /proc/self/stat
        if let Ok(stat) = fs::read_to_string(format!("/proc/{}/stat", pid)) {
            let parts: Vec<&str> = stat.split_whitespace().collect();
            if parts.len() >= 24 {
                metrics.thread_count = parts[19].parse().unwrap_or(0);
            }
        }

        // Read /proc/self/status
        if let Ok(status) = fs::read_to_string(format!("/proc/{}/status", pid)) {
            for line in status.lines() {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 2 {
                    match parts[0] {
                        "VmRSS:" => {
                            metrics.memory_rss_bytes = parts[1].parse::<u64>().unwrap_or(0) * 1024
                        }
                        "VmSize:" => {
                            metrics.memory_vms_bytes = parts[1].parse::<u64>().unwrap_or(0) * 1024
                        }
                        "Threads:" => metrics.thread_count = parts[1].parse().unwrap_or(0),
                        _ => {}
                    }
                }
            }
        }

        // Count file descriptors
        if let Ok(fds) = fs::read_dir(format!("/proc/{}/fd", pid)) {
            metrics.open_fds = fds.count() as u64;
        }

        metrics
    }

    /// Track a connection
    pub fn track_connection(&self, id: &str, remote_addr: &str, local_port: u16, protocol: &str) {
        if !self.config.enable_connection_tracking {
            return;
        }

        let info = ConnectionInfo {
            id: id.to_string(),
            remote_addr: remote_addr.to_string(),
            local_port,
            started_at: chrono::Utc::now().timestamp_millis(),
            bytes_sent: 0,
            bytes_received: 0,
            request_count: 0,
            last_activity: chrono::Utc::now().timestamp_millis(),
            protocol: protocol.to_string(),
        };

        self.connections.write().insert(id.to_string(), info);
    }

    /// Update connection stats
    pub fn update_connection(&self, id: &str, bytes_sent: u64, bytes_received: u64) {
        if let Some(conn) = self.connections.write().get_mut(id) {
            conn.bytes_sent += bytes_sent;
            conn.bytes_received += bytes_received;
            conn.request_count += 1;
            conn.last_activity = chrono::Utc::now().timestamp_millis();
        }
    }

    /// Remove connection tracking
    pub fn untrack_connection(&self, id: &str) {
        self.connections.write().remove(id);
    }

    /// Record operation latency
    pub fn record_latency(&self, operation: &str, latency_us: u64) {
        let mut latencies = self.latencies.write();
        match operation {
            "produce" => latencies.produce.record(latency_us),
            "fetch" => latencies.fetch.record(latency_us),
            "storage_write" => latencies.storage_write.record(latency_us),
            "storage_read" => latencies.storage_read.record(latency_us),
            "network_send" => latencies.network_send.record(latency_us),
            "network_recv" => latencies.network_recv.record(latency_us),
            _ => {}
        }
    }

    /// Get metrics history
    pub fn get_history(&self, limit: Option<usize>) -> Vec<SystemMetrics> {
        let history = self.history.read();
        let limit = limit.unwrap_or(history.len());
        history.iter().rev().take(limit).cloned().collect()
    }

    /// Get current connections
    pub fn get_connections(&self) -> Vec<ConnectionInfo> {
        self.connections.read().values().cloned().collect()
    }

    /// Get latency histograms
    pub fn get_latencies(&self) -> LatencyHistograms {
        self.latencies.read().clone()
    }

    /// Check if eBPF is available
    pub fn is_ebpf_available(&self) -> bool {
        self.ebpf_available
    }

    /// Get uptime in seconds
    pub fn uptime_secs(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }

    /// Get collection count
    pub fn collection_count(&self) -> u64 {
        self.collection_count.load(Ordering::Relaxed)
    }
}

/// Get number of CPU cores
#[cfg(target_os = "macos")]
fn num_cpus() -> u32 {
    std::thread::available_parallelism()
        .map(|p| p.get() as u32)
        .unwrap_or(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_observability_config_default() {
        let config = ObservabilityConfig::default();
        assert!(config.enable_system_metrics);
        assert!(config.enable_network_metrics);
        assert_eq!(config.collection_interval_ms, 1000);
    }

    #[test]
    fn test_observability_manager_creation() {
        let config = ObservabilityConfig::default();
        let manager = ObservabilityManager::new(config);
        assert_eq!(manager.collection_count(), 0);
    }

    #[test]
    fn test_collect_metrics() {
        let config = ObservabilityConfig::default();
        let manager = ObservabilityManager::new(config);

        let metrics = manager.collect();
        assert!(metrics.timestamp > 0);
        assert_eq!(manager.collection_count(), 1);
    }

    #[test]
    fn test_connection_tracking() {
        let config = ObservabilityConfig::default();
        let manager = ObservabilityManager::new(config);

        manager.track_connection("conn-1", "192.168.1.1:5000", 9092, "kafka");
        assert_eq!(manager.get_connections().len(), 1);

        manager.update_connection("conn-1", 100, 200);
        let conns = manager.get_connections();
        assert_eq!(conns[0].bytes_sent, 100);
        assert_eq!(conns[0].bytes_received, 200);

        manager.untrack_connection("conn-1");
        assert_eq!(manager.get_connections().len(), 0);
    }

    #[test]
    fn test_latency_histogram() {
        let mut hist = LatencyHistogram::new();
        hist.record(100);
        hist.record(200);
        hist.record(300);

        assert_eq!(hist.count, 3);
        assert_eq!(hist.sum, 600);
        assert_eq!(hist.min, 100);
        assert_eq!(hist.max, 300);
        assert_eq!(hist.mean(), 200.0);
    }

    #[test]
    fn test_record_latency() {
        let config = ObservabilityConfig::default();
        let manager = ObservabilityManager::new(config);

        manager.record_latency("produce", 1000);
        manager.record_latency("produce", 2000);

        let latencies = manager.get_latencies();
        assert_eq!(latencies.produce.count, 2);
        assert_eq!(latencies.produce.mean(), 1500.0);
    }

    #[test]
    fn test_metrics_history() {
        let config = ObservabilityConfig {
            max_history: 5,
            ..Default::default()
        };
        let manager = ObservabilityManager::new(config);

        for _ in 0..10 {
            manager.collect();
        }

        let history = manager.get_history(None);
        assert_eq!(history.len(), 5);
    }
}
