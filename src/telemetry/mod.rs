//! Telemetry Module
//!
//! This module provides optional, anonymous telemetry collection for Streamline.
//! Telemetry is **disabled by default** and must be explicitly opted into.
//!
//! ## Privacy Principles
//!
//! 1. **Opt-In Only**: Disabled by default, requires explicit consent
//! 2. **Transparent**: All collected data is documented and viewable
//! 3. **Anonymous**: No PII, no message content, no IP addresses
//! 4. **Minimal**: Only collect what's needed for product decisions
//! 5. **Controllable**: Easy to disable, data deletion available
//!
//! ## What We Collect
//!
//! - OS, architecture, Streamline version
//! - Topic/partition counts (not names)
//! - Throughput buckets (not actual values)
//! - Enabled features (TLS, auth, clustering)
//!
//! ## What We Don't Collect
//!
//! - Message content, keys, or topic names
//! - IP addresses or hostnames
//! - User credentials or configuration details
//! - Personally identifiable information
//!
//! See `docs/TELEMETRY.md` for full documentation.

mod report;

pub use report::{EnabledFeatures, SystemInfo, TelemetryReport, UsageStats};

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{debug, info};
use uuid::Uuid;

/// Default telemetry endpoint (placeholder - not active in initial release)
pub const DEFAULT_TELEMETRY_ENDPOINT: &str = "https://telemetry.streamline.dev/v1/report";

/// Default reporting interval (24 hours)
pub const DEFAULT_INTERVAL_HOURS: u64 = 24;

/// Telemetry configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryConfig {
    /// Enable anonymous usage telemetry (default: false, OPT-IN)
    pub enabled: bool,

    /// Telemetry endpoint URL
    pub endpoint: String,

    /// Unique installation ID (generated on first opt-in)
    /// This is stored locally and is the only persistent identifier
    pub installation_id: String,

    /// Reporting interval in hours (default: 24)
    pub interval_hours: u64,

    /// Dry run mode - show what would be sent without actually sending
    pub dry_run: bool,

    /// Path to store installation ID (defaults to data_dir/telemetry_id)
    pub id_file_path: Option<PathBuf>,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            enabled: false, // OPT-IN: disabled by default
            endpoint: DEFAULT_TELEMETRY_ENDPOINT.to_string(),
            installation_id: String::new(),
            interval_hours: DEFAULT_INTERVAL_HOURS,
            dry_run: false,
            id_file_path: None,
        }
    }
}

impl TelemetryConfig {
    /// Create a new telemetry config with telemetry enabled
    pub fn enabled() -> Self {
        Self {
            enabled: true,
            ..Default::default()
        }
    }

    /// Create a dry-run config for testing
    pub fn dry_run() -> Self {
        Self {
            enabled: true,
            dry_run: true,
            ..Default::default()
        }
    }

    /// Generate or load the installation ID
    pub fn ensure_installation_id(&mut self, data_dir: &std::path::Path) {
        if !self.installation_id.is_empty() {
            return;
        }

        // Try to load existing ID
        let id_file = self
            .id_file_path
            .clone()
            .unwrap_or_else(|| data_dir.join(".telemetry_id"));

        if let Ok(contents) = std::fs::read_to_string(&id_file) {
            let id = contents.trim().to_string();
            if !id.is_empty() {
                self.installation_id = id;
                return;
            }
        }

        // Generate new ID
        self.installation_id = Uuid::new_v4().to_string();

        // Try to save it (non-fatal if it fails)
        if let Err(e) = std::fs::write(&id_file, &self.installation_id) {
            debug!("Failed to save telemetry ID: {}", e);
        }
    }
}

/// Telemetry manager handles collecting and sending telemetry data
pub struct TelemetryManager {
    config: TelemetryConfig,
    #[cfg(feature = "auth")]
    client: reqwest::Client,
    last_report: Arc<RwLock<Option<TelemetryReport>>>,
}

impl TelemetryManager {
    /// Create a new telemetry manager
    #[cfg(feature = "auth")]
    pub fn new(config: TelemetryConfig) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .user_agent(format!("streamline/{}", env!("CARGO_PKG_VERSION")))
            .build()
            .unwrap_or_default();

        Self {
            config,
            client,
            last_report: Arc::new(RwLock::new(None)),
        }
    }

    /// Create a new telemetry manager (no network support without auth feature)
    #[cfg(not(feature = "auth"))]
    pub fn new(config: TelemetryConfig) -> Self {
        Self {
            config,
            last_report: Arc::new(RwLock::new(None)),
        }
    }

    /// Check if telemetry is enabled
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Get the installation ID
    pub fn installation_id(&self) -> &str {
        &self.config.installation_id
    }

    /// Get the last collected report (for display purposes)
    pub async fn last_report(&self) -> Option<TelemetryReport> {
        self.last_report.read().await.clone()
    }

    /// Collect a telemetry report from the current state
    pub async fn collect_report(
        &self,
        collector: &(dyn TelemetryCollector + Send + Sync),
    ) -> TelemetryReport {
        let system_info = collector.collect_system_info();
        let usage_stats = collector.collect_usage_stats().await;
        let features = collector.collect_enabled_features();

        let report = TelemetryReport {
            schema_version: 1,
            installation_id: self.config.installation_id.clone(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            timestamp: chrono::Utc::now().to_rfc3339(),
            system: system_info,
            usage: usage_stats,
            features,
        };

        // Store for later inspection
        *self.last_report.write().await = Some(report.clone());

        report
    }

    /// Send a telemetry report (requires auth feature for HTTP client)
    #[cfg(feature = "auth")]
    pub async fn send_report(&self, report: &TelemetryReport) -> Result<(), TelemetryError> {
        if !self.config.enabled {
            return Ok(());
        }

        if self.config.dry_run {
            info!(
                "Telemetry dry run - would send:\n{}",
                serde_json::to_string_pretty(report).unwrap_or_default()
            );
            return Ok(());
        }

        // Note: In the initial release, telemetry endpoint is not active.
        // This code is ready for when the endpoint is deployed.
        let response = self
            .client
            .post(&self.config.endpoint)
            .json(report)
            .send()
            .await
            .map_err(|e| TelemetryError::Network(e.to_string()))?;

        if !response.status().is_success() {
            return Err(TelemetryError::Server(format!(
                "Server returned status {}",
                response.status()
            )));
        }

        debug!("Telemetry report sent successfully");
        Ok(())
    }

    /// Send a telemetry report (no-op without auth feature - HTTP client unavailable)
    #[cfg(not(feature = "auth"))]
    pub async fn send_report(&self, report: &TelemetryReport) -> Result<(), TelemetryError> {
        if !self.config.enabled {
            return Ok(());
        }

        if self.config.dry_run {
            info!(
                "Telemetry dry run - would send:\n{}",
                serde_json::to_string_pretty(report).unwrap_or_default()
            );
            return Ok(());
        }

        // Network telemetry reporting not available without auth feature
        debug!("Telemetry report collected but not sent (auth feature not enabled)");
        let _ = report; // silence unused warning
        Ok(())
    }

    /// Start background telemetry reporting
    ///
    /// Returns a handle that can be used to stop the background task.
    pub fn start_background(
        self: Arc<Self>,
        collector: Arc<dyn TelemetryCollector + Send + Sync>,
    ) -> tokio::task::JoinHandle<()> {
        let interval_hours = self.config.interval_hours;

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(interval_hours * 3600));

            // Skip first tick (immediate)
            interval.tick().await;

            loop {
                interval.tick().await;

                if !self.config.enabled {
                    continue;
                }

                let report = self.collect_report(collector.as_ref()).await;

                if let Err(e) = self.send_report(&report).await {
                    // Telemetry errors should never affect server operation
                    debug!("Failed to send telemetry: {}", e);
                }
            }
        })
    }
}

/// Telemetry collector trait for gathering metrics
#[async_trait::async_trait]
pub trait TelemetryCollector {
    /// Collect system information
    fn collect_system_info(&self) -> SystemInfo;

    /// Collect usage statistics
    async fn collect_usage_stats(&self) -> UsageStats;

    /// Collect enabled feature flags
    fn collect_enabled_features(&self) -> EnabledFeatures;
}

/// Default telemetry collector implementation
pub struct DefaultTelemetryCollector {
    features: EnabledFeatures,
}

impl DefaultTelemetryCollector {
    /// Create a new default collector
    pub fn new(features: EnabledFeatures) -> Self {
        Self { features }
    }
}

#[async_trait::async_trait]
impl TelemetryCollector for DefaultTelemetryCollector {
    fn collect_system_info(&self) -> SystemInfo {
        SystemInfo {
            os: std::env::consts::OS.to_string(),
            arch: std::env::consts::ARCH.to_string(),
            cpu_cores: num_cpus(),
            ram_gb: system_ram_gb(),
        }
    }

    async fn collect_usage_stats(&self) -> UsageStats {
        // Default implementation returns placeholder values
        // In a real deployment, this would query the TopicManager
        UsageStats {
            uptime_hours: 0,
            topic_count: 0,
            partition_count: 0,
            throughput_bucket: "unknown".to_string(),
            storage_gb: 0,
            consumer_group_count: 0,
            connected_clients: 0,
        }
    }

    fn collect_enabled_features(&self) -> EnabledFeatures {
        self.features.clone()
    }
}

/// Telemetry error types
#[derive(Debug, thiserror::Error)]
pub enum TelemetryError {
    #[error("Network error: {0}")]
    Network(String),

    #[error("Server error: {0}")]
    Server(String),

    #[error("Serialization error: {0}")]
    Serialization(String),
}

/// Get number of CPU cores
fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(1)
}

/// Get system RAM in GB (rounded)
fn system_ram_gb() -> u32 {
    // On most systems, we can't easily get RAM without a dependency
    // This is a placeholder that could use sysinfo crate if needed
    0
}

/// Display telemetry notice on first enable
pub fn display_telemetry_notice(installation_id: &str) {
    println!();
    println!("Telemetry Notice");
    println!("================");
    println!("You've opted into anonymous usage telemetry. This helps us improve Streamline.");
    println!();
    println!("What we collect:");
    println!("  - OS, architecture, version");
    println!("  - Topic/partition counts (not names)");
    println!("  - Throughput buckets (not actual values)");
    println!("  - Enabled features");
    println!();
    println!("What we DON'T collect:");
    println!("  - Message content, keys, or topic names");
    println!("  - IP addresses or hostnames");
    println!("  - Credentials or configuration details");
    println!();
    println!("View current report: streamline-cli telemetry show");
    println!("Disable anytime:     --telemetry-enabled=false");
    println!();
    println!("Installation ID: {}", installation_id);
    println!();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config_is_disabled() {
        let config = TelemetryConfig::default();
        assert!(!config.enabled);
    }

    #[test]
    fn test_enabled_config() {
        let config = TelemetryConfig::enabled();
        assert!(config.enabled);
    }

    #[test]
    fn test_dry_run_config() {
        let config = TelemetryConfig::dry_run();
        assert!(config.enabled);
        assert!(config.dry_run);
    }

    #[test]
    fn test_installation_id_generation() {
        let mut config = TelemetryConfig::default();
        let temp_dir = tempfile::tempdir().unwrap();
        config.ensure_installation_id(temp_dir.path());
        assert!(!config.installation_id.is_empty());

        // Verify it's a valid UUID
        let uuid = Uuid::parse_str(&config.installation_id);
        assert!(uuid.is_ok());
    }

    #[tokio::test]
    async fn test_telemetry_manager_creation() {
        let config = TelemetryConfig::default();
        let manager = TelemetryManager::new(config);
        assert!(!manager.is_enabled());
    }
}
