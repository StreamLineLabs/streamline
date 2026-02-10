//! Memory pressure monitoring for Streamline
//!
//! This module provides memory usage monitoring and pressure detection
//! to enable backpressure when memory is constrained.

#[cfg(feature = "metrics")]
use metrics::{describe_gauge, gauge};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tracing::{debug, info, warn};

/// Memory pressure levels
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryPressure {
    /// Normal operation - no memory pressure
    Normal,
    /// Warning level - should reduce memory usage if possible
    Warning,
    /// Critical level - apply backpressure, reject new operations
    Critical,
}

impl std::fmt::Display for MemoryPressure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Normal => write!(f, "normal"),
            Self::Warning => write!(f, "warning"),
            Self::Critical => write!(f, "critical"),
        }
    }
}

/// Configuration for memory pressure monitoring
#[derive(Debug, Clone)]
pub struct MemoryConfig {
    /// Threshold for warning level (fraction of max memory, 0.0-1.0)
    pub warning_threshold: f64,
    /// Threshold for critical level (fraction of max memory, 0.0-1.0)
    pub critical_threshold: f64,
    /// Maximum memory limit in bytes (0 = system memory)
    pub max_memory_bytes: u64,
    /// How often to check memory usage
    pub check_interval: Duration,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            warning_threshold: 0.75,
            critical_threshold: 0.90,
            max_memory_bytes: 0, // Use system memory
            check_interval: Duration::from_secs(1),
        }
    }
}

/// Memory monitor that tracks process memory usage
pub struct MemoryMonitor {
    config: MemoryConfig,
    current_usage: AtomicU64,
    pressure_tx: watch::Sender<MemoryPressure>,
    pressure_rx: watch::Receiver<MemoryPressure>,
}

impl MemoryMonitor {
    /// Create a new memory monitor
    pub fn new(config: MemoryConfig) -> Arc<Self> {
        #[cfg(feature = "metrics")]
        register_metrics();

        let (pressure_tx, pressure_rx) = watch::channel(MemoryPressure::Normal);

        Arc::new(Self {
            config,
            current_usage: AtomicU64::new(0),
            pressure_tx,
            pressure_rx,
        })
    }

    /// Start the memory monitoring task
    pub fn start(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let monitor = self.clone();
        tokio::spawn(async move {
            monitor.run().await;
        })
    }

    /// Run the memory monitoring loop
    async fn run(&self) {
        let mut interval = tokio::time::interval(self.config.check_interval);
        let max_memory = self.get_max_memory();

        info!(
            max_memory_mb = max_memory / (1024 * 1024),
            warning_threshold = %format!("{:.0}%", self.config.warning_threshold * 100.0),
            critical_threshold = %format!("{:.0}%", self.config.critical_threshold * 100.0),
            "Memory monitor started"
        );

        loop {
            interval.tick().await;

            let usage = self.get_process_memory();
            self.current_usage.store(usage, Ordering::Relaxed);

            let usage_ratio = usage as f64 / max_memory as f64;
            let new_pressure = if usage_ratio >= self.config.critical_threshold {
                MemoryPressure::Critical
            } else if usage_ratio >= self.config.warning_threshold {
                MemoryPressure::Warning
            } else {
                MemoryPressure::Normal
            };

            // Update metrics
            #[cfg(feature = "metrics")]
            {
                gauge!("streamline_memory_usage_bytes").set(usage as f64);
                gauge!("streamline_memory_max_bytes").set(max_memory as f64);
                gauge!("streamline_memory_usage_ratio").set(usage_ratio);
                gauge!("streamline_memory_pressure_level").set(match new_pressure {
                    MemoryPressure::Normal => 0.0,
                    MemoryPressure::Warning => 1.0,
                    MemoryPressure::Critical => 2.0,
                });
            }

            // Log and notify on state change
            let current_pressure = *self.pressure_rx.borrow();
            if new_pressure != current_pressure {
                match new_pressure {
                    MemoryPressure::Normal => {
                        info!(
                            usage_mb = usage / (1024 * 1024),
                            usage_pct = %format!("{:.1}%", usage_ratio * 100.0),
                            "Memory pressure returned to normal"
                        );
                    }
                    MemoryPressure::Warning => {
                        warn!(
                            usage_mb = usage / (1024 * 1024),
                            usage_pct = %format!("{:.1}%", usage_ratio * 100.0),
                            "Memory pressure at warning level"
                        );
                    }
                    MemoryPressure::Critical => {
                        warn!(
                            usage_mb = usage / (1024 * 1024),
                            usage_pct = %format!("{:.1}%", usage_ratio * 100.0),
                            "Memory pressure CRITICAL - applying backpressure"
                        );
                    }
                }
                let _ = self.pressure_tx.send(new_pressure);
            }

            debug!(
                usage_mb = usage / (1024 * 1024),
                usage_pct = %format!("{:.1}%", usage_ratio * 100.0),
                pressure = %new_pressure,
                "Memory check"
            );
        }
    }

    /// Get the current memory pressure level
    pub fn pressure(&self) -> MemoryPressure {
        *self.pressure_rx.borrow()
    }

    /// Subscribe to pressure level changes
    pub fn subscribe(&self) -> watch::Receiver<MemoryPressure> {
        self.pressure_rx.clone()
    }

    /// Get current memory usage in bytes
    pub fn usage_bytes(&self) -> u64 {
        self.current_usage.load(Ordering::Relaxed)
    }

    /// Check if backpressure should be applied (critical level)
    pub fn should_apply_backpressure(&self) -> bool {
        self.pressure() == MemoryPressure::Critical
    }

    /// Get the maximum memory limit
    fn get_max_memory(&self) -> u64 {
        if self.config.max_memory_bytes > 0 {
            return self.config.max_memory_bytes;
        }
        get_system_memory()
    }

    /// Get process memory usage
    fn get_process_memory(&self) -> u64 {
        get_process_resident_memory()
    }
}

/// Register memory metrics
#[cfg(feature = "metrics")]
fn register_metrics() {
    describe_gauge!(
        "streamline_memory_usage_bytes",
        "Current process memory usage in bytes"
    );
    describe_gauge!(
        "streamline_memory_max_bytes",
        "Maximum memory limit in bytes"
    );
    describe_gauge!(
        "streamline_memory_usage_ratio",
        "Memory usage as a fraction of maximum (0.0-1.0)"
    );
    describe_gauge!(
        "streamline_memory_pressure_level",
        "Current memory pressure level (0=normal, 1=warning, 2=critical)"
    );
}

/// Get total system memory
#[cfg(target_os = "linux")]
fn get_system_memory() -> u64 {
    use std::fs;

    fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|content| {
            content
                .lines()
                .find(|line| line.starts_with("MemTotal:"))
                .and_then(|line| {
                    line.split_whitespace()
                        .nth(1)
                        .and_then(|s| s.parse::<u64>().ok())
                        .map(|kb| kb * 1024)
                })
        })
        .unwrap_or(8 * 1024 * 1024 * 1024) // Default 8GB
}

#[cfg(target_os = "macos")]
fn get_system_memory() -> u64 {
    use std::process::Command;

    Command::new("sysctl")
        .args(["-n", "hw.memsize"])
        .output()
        .ok()
        .and_then(|output| {
            String::from_utf8(output.stdout)
                .ok()
                .and_then(|s| s.trim().parse::<u64>().ok())
        })
        .unwrap_or(8 * 1024 * 1024 * 1024) // Default 8GB
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn get_system_memory() -> u64 {
    8 * 1024 * 1024 * 1024 // Default 8GB
}

/// Get process resident memory (RSS)
#[cfg(target_os = "linux")]
fn get_process_resident_memory() -> u64 {
    use std::fs;

    fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|content| {
            content
                .split_whitespace()
                .nth(1) // RSS is second field
                .and_then(|s| s.parse::<u64>().ok())
                .map(|pages| pages * 4096) // Convert pages to bytes
        })
        .unwrap_or(0)
}

#[cfg(target_os = "macos")]
fn get_process_resident_memory() -> u64 {
    use std::process::Command;

    let pid = std::process::id();
    Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .ok()
        .and_then(|output| {
            String::from_utf8(output.stdout)
                .ok()
                .and_then(|s| s.trim().parse::<u64>().ok())
                .map(|kb| kb * 1024)
        })
        .unwrap_or(0)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn get_process_resident_memory() -> u64 {
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_config_default() {
        let config = MemoryConfig::default();
        assert!(config.warning_threshold > 0.0);
        assert!(config.critical_threshold > config.warning_threshold);
        assert!(config.critical_threshold <= 1.0);
    }

    #[test]
    fn test_memory_pressure_display() {
        assert_eq!(MemoryPressure::Normal.to_string(), "normal");
        assert_eq!(MemoryPressure::Warning.to_string(), "warning");
        assert_eq!(MemoryPressure::Critical.to_string(), "critical");
    }

    #[test]
    fn test_get_system_memory() {
        let mem = get_system_memory();
        // Should be at least 1GB on any reasonable system
        assert!(mem >= 1024 * 1024 * 1024);
    }

    #[test]
    fn test_get_process_memory() {
        let mem = get_process_resident_memory();
        // Process should use at least some memory
        // On some platforms this might return 0 if not supported
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        assert!(mem > 0);
        let _ = mem; // Suppress unused warning on other platforms
    }

    #[tokio::test]
    async fn test_memory_monitor_creation() {
        let config = MemoryConfig::default();
        let monitor = MemoryMonitor::new(config);

        // Initial state should be normal
        assert_eq!(monitor.pressure(), MemoryPressure::Normal);
        assert!(!monitor.should_apply_backpressure());
    }

    #[tokio::test]
    async fn test_memory_monitor_subscribe() {
        let config = MemoryConfig::default();
        let monitor = MemoryMonitor::new(config);

        let rx = monitor.subscribe();
        assert_eq!(*rx.borrow(), MemoryPressure::Normal);
    }

    #[tokio::test]
    async fn test_memory_monitor_with_low_threshold() {
        // Create monitor with very low threshold to trigger warning/critical
        let config = MemoryConfig {
            warning_threshold: 0.0001,            // Effectively always in warning
            critical_threshold: 0.0002,           // Effectively always in critical
            max_memory_bytes: 1024 * 1024 * 1024, // 1GB
            check_interval: Duration::from_millis(10),
        };

        let monitor = MemoryMonitor::new(config);
        let _handle = monitor.start();

        // Let it run one check cycle
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Should be in critical due to very low thresholds
        // (any memory usage > 0.02% of 1GB = 200KB will trigger critical)
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        assert_eq!(monitor.pressure(), MemoryPressure::Critical);
    }
}
