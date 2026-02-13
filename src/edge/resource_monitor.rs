//! Resource-Constrained Environment Monitoring
//!
//! Tracks system resources on edge devices and automatically adapts
//! configuration when thresholds are breached:
//! - Low memory → reduce buffer sizes, increase compression
//! - Low disk → aggressive retention, switch to in-memory mode
//! - High latency → batch more aggressively

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Snapshot of current system resources
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResourceSnapshot {
    /// CPU usage percentage (0.0–1.0)
    pub cpu_usage: f64,
    /// Used memory in bytes
    pub memory_used_bytes: u64,
    /// Total memory in bytes
    pub memory_total_bytes: u64,
    /// Used disk in bytes
    pub disk_used_bytes: u64,
    /// Total disk in bytes
    pub disk_total_bytes: u64,
    /// Average network latency in milliseconds to the upstream/cloud
    pub network_latency_ms: u64,
    /// Timestamp of this snapshot (ms since epoch)
    pub timestamp: i64,
}

impl ResourceSnapshot {
    /// Memory utilization as a fraction (0.0–1.0)
    pub fn memory_utilization(&self) -> f64 {
        if self.memory_total_bytes == 0 {
            return 0.0;
        }
        self.memory_used_bytes as f64 / self.memory_total_bytes as f64
    }

    /// Disk utilization as a fraction (0.0–1.0)
    pub fn disk_utilization(&self) -> f64 {
        if self.disk_total_bytes == 0 {
            return 0.0;
        }
        self.disk_used_bytes as f64 / self.disk_total_bytes as f64
    }
}

/// Severity of a resource alert
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AlertSeverity {
    Warning,
    Critical,
}

impl std::fmt::Display for AlertSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AlertSeverity::Warning => write!(f, "warning"),
            AlertSeverity::Critical => write!(f, "critical"),
        }
    }
}

/// Kind of resource alert
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AlertKind {
    HighCpu,
    HighMemory,
    HighDisk,
    HighLatency,
}

impl std::fmt::Display for AlertKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AlertKind::HighCpu => write!(f, "high_cpu"),
            AlertKind::HighMemory => write!(f, "high_memory"),
            AlertKind::HighDisk => write!(f, "high_disk"),
            AlertKind::HighLatency => write!(f, "high_latency"),
        }
    }
}

/// A resource alert raised when a threshold is breached
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceAlert {
    /// Kind of alert
    pub kind: AlertKind,
    /// Severity
    pub severity: AlertSeverity,
    /// Human-readable message
    pub message: String,
    /// Timestamp (ms since epoch)
    pub timestamp: i64,
}

/// Thresholds that trigger resource alerts and adaptive behaviour
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceThresholds {
    /// CPU warning threshold (0.0–1.0)
    pub cpu_warning: f64,
    /// CPU critical threshold (0.0–1.0)
    pub cpu_critical: f64,
    /// Memory warning threshold (0.0–1.0)
    pub memory_warning: f64,
    /// Memory critical threshold (0.0–1.0)
    pub memory_critical: f64,
    /// Disk warning threshold (0.0–1.0)
    pub disk_warning: f64,
    /// Disk critical threshold (0.0–1.0)
    pub disk_critical: f64,
    /// Network latency warning threshold (ms)
    pub latency_warning_ms: u64,
    /// Network latency critical threshold (ms)
    pub latency_critical_ms: u64,
}

impl Default for ResourceThresholds {
    fn default() -> Self {
        Self {
            cpu_warning: 0.70,
            cpu_critical: 0.90,
            memory_warning: 0.70,
            memory_critical: 0.90,
            disk_warning: 0.80,
            disk_critical: 0.95,
            latency_warning_ms: 200,
            latency_critical_ms: 1000,
        }
    }
}

/// Adaptive configuration recommendations produced by the monitor.
///
/// When resource pressure is detected the monitor emits an `AdaptiveConfig`
/// that the runtime can apply to keep operating within constraints.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AdaptiveConfig {
    /// Suggested buffer pool size (lower under memory pressure)
    pub buffer_pool_size: Option<usize>,
    /// Suggested compression strategy name ("fast", "maximum", …)
    pub compression: Option<String>,
    /// Switch to in-memory mode when disk is critically full
    pub switch_to_in_memory: bool,
    /// Use aggressive retention when disk is under pressure
    pub aggressive_retention: bool,
    /// Suggested sync batch size (larger under high latency)
    pub sync_batch_size: Option<usize>,
    /// Reduce max concurrent operations
    pub max_concurrent_ops: Option<usize>,
    /// Human-readable summary of what changed
    pub reason: String,
}

/// Configuration for the resource monitor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceMonitorConfig {
    /// Whether resource monitoring is enabled
    pub enabled: bool,
    /// How often to sample resources (ms)
    pub poll_interval_ms: u64,
    /// Thresholds that trigger alerts / adaptive behaviour
    pub thresholds: ResourceThresholds,
}

impl Default for ResourceMonitorConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            poll_interval_ms: 10_000,
            thresholds: ResourceThresholds::default(),
        }
    }
}

/// Resource monitor for edge devices
pub struct ResourceMonitor {
    config: ResourceMonitorConfig,
    /// Latest snapshot
    latest: parking_lot::RwLock<ResourceSnapshot>,
    /// Outstanding alerts
    alerts: parking_lot::RwLock<Vec<ResourceAlert>>,
    /// Latest adaptive config recommendation
    adaptive: parking_lot::RwLock<Option<AdaptiveConfig>>,
    /// Running flag
    running: Arc<AtomicBool>,
}

impl ResourceMonitor {
    /// Create a new resource monitor
    pub fn new(config: ResourceMonitorConfig) -> Self {
        Self {
            config,
            latest: parking_lot::RwLock::new(ResourceSnapshot::default()),
            alerts: parking_lot::RwLock::new(Vec::new()),
            adaptive: parking_lot::RwLock::new(None),
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Start the monitor
    pub fn start(&self) {
        if self.running.swap(true, Ordering::SeqCst) {
            return;
        }
        info!("Resource monitor started");
    }

    /// Stop the monitor
    pub fn stop(&self) {
        if !self.running.swap(false, Ordering::SeqCst) {
            return;
        }
        info!("Resource monitor stopped");
    }

    /// Whether the monitor is running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    /// Feed a new resource snapshot and evaluate thresholds.
    ///
    /// In a production system this would be called by a background poller.
    /// Exposing it publicly allows the runtime (or tests) to push snapshots.
    pub fn record_snapshot(&self, snapshot: ResourceSnapshot) {
        let alerts = self.evaluate(&snapshot);
        let adaptive = self.compute_adaptive(&snapshot, &alerts);

        *self.latest.write() = snapshot;

        if !alerts.is_empty() {
            for a in &alerts {
                warn!(kind = %a.kind, severity = %a.severity, msg = %a.message, "Resource alert");
            }
            *self.alerts.write() = alerts;
        } else {
            // Clear stale alerts when healthy
            self.alerts.write().clear();
        }

        if let Some(ref ac) = adaptive {
            debug!(reason = %ac.reason, "Adaptive config recommendation");
        }
        *self.adaptive.write() = adaptive;
    }

    /// Get the latest resource snapshot
    pub fn latest_snapshot(&self) -> ResourceSnapshot {
        self.latest.read().clone()
    }

    /// Get current resource alerts (empty when healthy)
    pub fn current_alerts(&self) -> Vec<ResourceAlert> {
        self.alerts.read().clone()
    }

    /// Get the latest adaptive configuration recommendation (if any)
    pub fn adaptive_config(&self) -> Option<AdaptiveConfig> {
        self.adaptive.read().clone()
    }

    /// Get the monitor configuration
    pub fn config(&self) -> &ResourceMonitorConfig {
        &self.config
    }

    // --- internal helpers ---

    fn evaluate(&self, snap: &ResourceSnapshot) -> Vec<ResourceAlert> {
        let t = &self.config.thresholds;
        let now = chrono::Utc::now().timestamp_millis();
        let mut alerts = Vec::new();

        // CPU
        if snap.cpu_usage >= t.cpu_critical {
            alerts.push(ResourceAlert {
                kind: AlertKind::HighCpu,
                severity: AlertSeverity::Critical,
                message: format!("CPU at {:.0}%", snap.cpu_usage * 100.0),
                timestamp: now,
            });
        } else if snap.cpu_usage >= t.cpu_warning {
            alerts.push(ResourceAlert {
                kind: AlertKind::HighCpu,
                severity: AlertSeverity::Warning,
                message: format!("CPU at {:.0}%", snap.cpu_usage * 100.0),
                timestamp: now,
            });
        }

        // Memory
        let mem = snap.memory_utilization();
        if mem >= t.memory_critical {
            alerts.push(ResourceAlert {
                kind: AlertKind::HighMemory,
                severity: AlertSeverity::Critical,
                message: format!("Memory at {:.0}%", mem * 100.0),
                timestamp: now,
            });
        } else if mem >= t.memory_warning {
            alerts.push(ResourceAlert {
                kind: AlertKind::HighMemory,
                severity: AlertSeverity::Warning,
                message: format!("Memory at {:.0}%", mem * 100.0),
                timestamp: now,
            });
        }

        // Disk
        let disk = snap.disk_utilization();
        if disk >= t.disk_critical {
            alerts.push(ResourceAlert {
                kind: AlertKind::HighDisk,
                severity: AlertSeverity::Critical,
                message: format!("Disk at {:.0}%", disk * 100.0),
                timestamp: now,
            });
        } else if disk >= t.disk_warning {
            alerts.push(ResourceAlert {
                kind: AlertKind::HighDisk,
                severity: AlertSeverity::Warning,
                message: format!("Disk at {:.0}%", disk * 100.0),
                timestamp: now,
            });
        }

        // Latency
        if snap.network_latency_ms >= t.latency_critical_ms {
            alerts.push(ResourceAlert {
                kind: AlertKind::HighLatency,
                severity: AlertSeverity::Critical,
                message: format!("Network latency {}ms", snap.network_latency_ms),
                timestamp: now,
            });
        } else if snap.network_latency_ms >= t.latency_warning_ms {
            alerts.push(ResourceAlert {
                kind: AlertKind::HighLatency,
                severity: AlertSeverity::Warning,
                message: format!("Network latency {}ms", snap.network_latency_ms),
                timestamp: now,
            });
        }

        alerts
    }

    fn compute_adaptive(
        &self,
        snap: &ResourceSnapshot,
        alerts: &[ResourceAlert],
    ) -> Option<AdaptiveConfig> {
        if alerts.is_empty() {
            return None;
        }

        let mut ac = AdaptiveConfig::default();
        let mut reasons = Vec::new();

        let mem = snap.memory_utilization();
        let disk = snap.disk_utilization();
        let t = &self.config.thresholds;

        // Low memory → reduce buffer sizes, increase compression
        if mem >= t.memory_critical {
            ac.buffer_pool_size = Some(2);
            ac.compression = Some("maximum".to_string());
            ac.max_concurrent_ops = Some(1);
            reasons.push("critical memory pressure".to_string());
        } else if mem >= t.memory_warning {
            ac.buffer_pool_size = Some(4);
            ac.compression = Some("balanced".to_string());
            ac.max_concurrent_ops = Some(2);
            reasons.push("elevated memory usage".to_string());
        }

        // Low disk → aggressive retention, switch to in-memory mode
        if disk >= t.disk_critical {
            ac.switch_to_in_memory = true;
            ac.aggressive_retention = true;
            reasons.push("critical disk pressure — switching to in-memory".to_string());
        } else if disk >= t.disk_warning {
            ac.aggressive_retention = true;
            reasons.push("elevated disk usage".to_string());
        }

        // High latency → batch more aggressively
        if snap.network_latency_ms >= t.latency_critical_ms {
            ac.sync_batch_size = Some(5000);
            reasons.push("very high network latency".to_string());
        } else if snap.network_latency_ms >= t.latency_warning_ms {
            ac.sync_batch_size = Some(2000);
            reasons.push("elevated network latency".to_string());
        }

        if reasons.is_empty() {
            return None;
        }

        ac.reason = reasons.join("; ");
        Some(ac)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn monitor() -> ResourceMonitor {
        ResourceMonitor::new(ResourceMonitorConfig::default())
    }

    #[test]
    fn test_healthy_snapshot_no_alerts() {
        let m = monitor();
        m.record_snapshot(ResourceSnapshot {
            cpu_usage: 0.3,
            memory_used_bytes: 100,
            memory_total_bytes: 1000,
            disk_used_bytes: 100,
            disk_total_bytes: 1000,
            network_latency_ms: 10,
            timestamp: 0,
        });
        assert!(m.current_alerts().is_empty());
        assert!(m.adaptive_config().is_none());
    }

    #[test]
    fn test_high_memory_warning() {
        let m = monitor();
        m.record_snapshot(ResourceSnapshot {
            cpu_usage: 0.1,
            memory_used_bytes: 750,
            memory_total_bytes: 1000,
            disk_used_bytes: 100,
            disk_total_bytes: 1000,
            network_latency_ms: 10,
            timestamp: 0,
        });
        let alerts = m.current_alerts();
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].kind, AlertKind::HighMemory);
        assert_eq!(alerts[0].severity, AlertSeverity::Warning);

        let ac = m.adaptive_config().unwrap();
        assert_eq!(ac.buffer_pool_size, Some(4));
    }

    #[test]
    fn test_critical_disk_switches_to_in_memory() {
        let m = monitor();
        m.record_snapshot(ResourceSnapshot {
            cpu_usage: 0.1,
            memory_used_bytes: 100,
            memory_total_bytes: 1000,
            disk_used_bytes: 960,
            disk_total_bytes: 1000,
            network_latency_ms: 10,
            timestamp: 0,
        });
        let ac = m.adaptive_config().unwrap();
        assert!(ac.switch_to_in_memory);
        assert!(ac.aggressive_retention);
    }

    #[test]
    fn test_high_latency_increases_batch_size() {
        let m = monitor();
        m.record_snapshot(ResourceSnapshot {
            cpu_usage: 0.1,
            memory_used_bytes: 100,
            memory_total_bytes: 1000,
            disk_used_bytes: 100,
            disk_total_bytes: 1000,
            network_latency_ms: 1500,
            timestamp: 0,
        });
        let ac = m.adaptive_config().unwrap();
        assert_eq!(ac.sync_batch_size, Some(5000));
    }

    #[test]
    fn test_multiple_alerts() {
        let m = monitor();
        m.record_snapshot(ResourceSnapshot {
            cpu_usage: 0.95,
            memory_used_bytes: 950,
            memory_total_bytes: 1000,
            disk_used_bytes: 960,
            disk_total_bytes: 1000,
            network_latency_ms: 2000,
            timestamp: 0,
        });
        let alerts = m.current_alerts();
        assert_eq!(alerts.len(), 4);
    }

    #[test]
    fn test_start_stop() {
        let m = monitor();
        assert!(!m.is_running());
        m.start();
        assert!(m.is_running());
        m.stop();
        assert!(!m.is_running());
    }

    #[test]
    fn test_resource_snapshot_utilization() {
        let snap = ResourceSnapshot {
            memory_used_bytes: 500,
            memory_total_bytes: 1000,
            disk_used_bytes: 250,
            disk_total_bytes: 1000,
            ..Default::default()
        };
        assert!((snap.memory_utilization() - 0.5).abs() < f64::EPSILON);
        assert!((snap.disk_utilization() - 0.25).abs() < f64::EPSILON);
    }

    #[test]
    fn test_zero_total_utilization() {
        let snap = ResourceSnapshot::default();
        assert_eq!(snap.memory_utilization(), 0.0);
        assert_eq!(snap.disk_utilization(), 0.0);
    }
}
