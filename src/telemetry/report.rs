//! Telemetry Report Structures
//!
//! This module defines the data structures for telemetry reports.
//! All fields are documented to ensure transparency about what data is collected.

use serde::{Deserialize, Serialize};

/// Anonymous telemetry report
///
/// This structure represents a single telemetry report sent to the
/// telemetry endpoint. All data is anonymous and contains no PII.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryReport {
    /// Schema version for forward compatibility
    /// Increment when making breaking changes to the report structure
    pub schema_version: u32,

    /// Anonymous installation ID (UUID)
    /// Generated locally and stored in data directory
    /// This is the only identifier - no IP addresses or hostnames are collected
    pub installation_id: String,

    /// Streamline version (from CARGO_PKG_VERSION)
    pub version: String,

    /// Timestamp when the report was generated (ISO 8601)
    pub timestamp: String,

    /// System information
    pub system: SystemInfo,

    /// Usage statistics
    pub usage: UsageStats,

    /// Enabled features
    pub features: EnabledFeatures,
}

impl TelemetryReport {
    /// Display report in human-readable format
    pub fn display(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_else(|_| "Failed to format report".to_string())
    }

    /// Get a summary of the report for logging
    pub fn summary(&self) -> String {
        format!(
            "TelemetryReport(version={}, topics={}, partitions={}, features={{tls={}, auth={}, clustering={}}})",
            self.version,
            self.usage.topic_count,
            self.usage.partition_count,
            self.features.tls,
            self.features.auth,
            self.features.clustering
        )
    }
}

/// System information
///
/// Basic information about the system running Streamline.
/// This helps us understand which platforms to prioritize.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemInfo {
    /// Operating system (e.g., "linux", "macos", "windows")
    pub os: String,

    /// CPU architecture (e.g., "x86_64", "aarch64")
    pub arch: String,

    /// Number of CPU cores available
    pub cpu_cores: usize,

    /// Total RAM in GB (rounded to whole number)
    /// Value of 0 means unable to determine
    pub ram_gb: u32,
}

impl Default for SystemInfo {
    fn default() -> Self {
        Self {
            os: std::env::consts::OS.to_string(),
            arch: std::env::consts::ARCH.to_string(),
            cpu_cores: std::thread::available_parallelism()
                .map(|p| p.get())
                .unwrap_or(1),
            ram_gb: 0,
        }
    }
}

/// Usage statistics
///
/// Anonymous usage metrics that help us understand how Streamline is used.
/// All values are either counts or bucketed to prevent identification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsageStats {
    /// Uptime in hours (rounded to whole number)
    pub uptime_hours: u32,

    /// Number of topics (count only, no names)
    pub topic_count: u32,

    /// Total number of partitions across all topics
    pub partition_count: u32,

    /// Throughput bucket (not exact value)
    /// Possible values: "0", "1-100", "100-1K", "1K-10K", "10K-100K", "100K+"
    /// Unit: messages per second
    pub throughput_bucket: String,

    /// Total data stored in GB (rounded)
    pub storage_gb: u32,

    /// Number of consumer groups
    pub consumer_group_count: u32,

    /// Number of currently connected clients
    pub connected_clients: u32,
}

impl Default for UsageStats {
    fn default() -> Self {
        Self {
            uptime_hours: 0,
            topic_count: 0,
            partition_count: 0,
            throughput_bucket: "0".to_string(),
            storage_gb: 0,
            consumer_group_count: 0,
            connected_clients: 0,
        }
    }
}

impl UsageStats {
    /// Calculate throughput bucket from messages per second
    pub fn throughput_bucket_from_mps(mps: f64) -> String {
        if mps < 1.0 {
            "0".to_string()
        } else if mps < 100.0 {
            "1-100".to_string()
        } else if mps < 1_000.0 {
            "100-1K".to_string()
        } else if mps < 10_000.0 {
            "1K-10K".to_string()
        } else if mps < 100_000.0 {
            "10K-100K".to_string()
        } else {
            "100K+".to_string()
        }
    }
}

/// Enabled features
///
/// Tracks which features are enabled in this installation.
/// This helps us prioritize development of features that are actually used.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EnabledFeatures {
    /// TLS encryption for client connections
    pub tls: bool,

    /// SASL authentication (PLAIN, SCRAM, OAuth)
    pub auth: bool,

    /// ACL-based authorization
    pub acl: bool,

    /// Multi-node clustering
    pub clustering: bool,

    /// Data replication between nodes
    pub replication: bool,

    /// Tiered storage (S3, Azure, GCS)
    pub tiered_storage: bool,

    /// Transaction support
    pub transactions: bool,

    /// Client quotas/throttling
    pub quotas: bool,

    /// Compression enabled
    pub compression: bool,

    /// Write-ahead log enabled
    pub wal: bool,

    /// Encryption at rest
    pub encryption_at_rest: bool,

    /// In-memory mode (no persistence)
    pub in_memory: bool,

    /// Simple protocol enabled
    pub simple_protocol: bool,

    /// Zero-copy I/O optimizations
    pub zerocopy: bool,
}

impl EnabledFeatures {
    /// Create features from server configuration
    #[allow(clippy::too_many_arguments)]
    pub fn from_config(
        tls: bool,
        auth: bool,
        acl: bool,
        clustering: bool,
        quotas: bool,
        wal: bool,
        encryption: bool,
        in_memory: bool,
        simple: bool,
        zerocopy: bool,
    ) -> Self {
        Self {
            tls,
            auth,
            acl,
            clustering,
            replication: clustering, // Replication implies clustering
            tiered_storage: false,   // Not yet supported via CLI
            transactions: false,     // Experimental
            quotas,
            compression: true, // Always available
            wal,
            encryption_at_rest: encryption,
            in_memory,
            simple_protocol: simple,
            zerocopy,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_telemetry_report_display() {
        let report = TelemetryReport {
            schema_version: 1,
            installation_id: "test-id".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            timestamp: "2024-01-01T00:00:00Z".to_string(),
            system: SystemInfo::default(),
            usage: UsageStats::default(),
            features: EnabledFeatures::default(),
        };

        let display = report.display();
        assert!(display.contains("test-id"));
        assert!(display.contains(env!("CARGO_PKG_VERSION")));
    }

    #[test]
    fn test_telemetry_report_summary() {
        let report = TelemetryReport {
            schema_version: 1,
            installation_id: "test-id".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            timestamp: "2024-01-01T00:00:00Z".to_string(),
            system: SystemInfo::default(),
            usage: UsageStats {
                topic_count: 5,
                partition_count: 15,
                ..Default::default()
            },
            features: EnabledFeatures {
                tls: true,
                ..Default::default()
            },
        };

        let summary = report.summary();
        assert!(summary.contains(env!("CARGO_PKG_VERSION")));
        assert!(summary.contains("topics=5"));
        assert!(summary.contains("partitions=15"));
        assert!(summary.contains("tls=true"));
    }

    #[test]
    fn test_throughput_bucket() {
        assert_eq!(UsageStats::throughput_bucket_from_mps(0.0), "0");
        assert_eq!(UsageStats::throughput_bucket_from_mps(50.0), "1-100");
        assert_eq!(UsageStats::throughput_bucket_from_mps(500.0), "100-1K");
        assert_eq!(UsageStats::throughput_bucket_from_mps(5000.0), "1K-10K");
        assert_eq!(UsageStats::throughput_bucket_from_mps(50000.0), "10K-100K");
        assert_eq!(UsageStats::throughput_bucket_from_mps(500000.0), "100K+");
    }

    #[test]
    fn test_enabled_features_from_config() {
        let features = EnabledFeatures::from_config(
            true,  // tls
            true,  // auth
            false, // acl
            true,  // clustering
            false, // quotas
            true,  // wal
            false, // encryption
            false, // in_memory
            true,  // simple
            true,  // zerocopy
        );

        assert!(features.tls);
        assert!(features.auth);
        assert!(!features.acl);
        assert!(features.clustering);
        assert!(features.replication); // Should be true when clustering is true
        assert!(!features.quotas);
        assert!(features.wal);
        assert!(!features.encryption_at_rest);
        assert!(!features.in_memory);
        assert!(features.simple_protocol);
        assert!(features.zerocopy);
    }

    #[test]
    fn test_system_info_default() {
        let info = SystemInfo::default();
        assert!(!info.os.is_empty());
        assert!(!info.arch.is_empty());
        assert!(info.cpu_cores >= 1);
    }

    #[test]
    fn test_usage_stats_default() {
        let stats = UsageStats::default();
        assert_eq!(stats.uptime_hours, 0);
        assert_eq!(stats.topic_count, 0);
        assert_eq!(stats.partition_count, 0);
        assert_eq!(stats.throughput_bucket, "0");
    }
}
