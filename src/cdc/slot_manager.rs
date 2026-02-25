//! PostgreSQL Replication Slot Manager
//!
//! Manages the lifecycle of PostgreSQL logical replication slots used by CDC.
//! Handles slot creation, deletion, monitoring, and WAL retention management.

use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// Configuration for slot management
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlotManagerConfig {
    /// Prefix for slot names created by Streamline
    pub slot_name_prefix: String,
    /// Output plugin to use (pgoutput or wal2json)
    pub output_plugin: String,
    /// Maximum WAL retention size in bytes before triggering alerts
    pub max_wal_retention_bytes: u64,
    /// Interval for checking slot health (ms)
    pub health_check_interval_ms: u64,
    /// Whether to automatically drop inactive slots
    pub auto_drop_inactive: bool,
    /// Duration of inactivity before a slot is considered stale
    pub stale_threshold: Duration,
}

impl Default for SlotManagerConfig {
    fn default() -> Self {
        Self {
            slot_name_prefix: "streamline_cdc_".to_string(),
            output_plugin: "pgoutput".to_string(),
            max_wal_retention_bytes: 1_073_741_824, // 1 GB
            health_check_interval_ms: 30_000,
            auto_drop_inactive: false,
            stale_threshold: Duration::from_secs(86400), // 24 hours
        }
    }
}

/// Information about a replication slot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlotInfo {
    /// Slot name
    pub name: String,
    /// Output plugin
    pub plugin: String,
    /// Whether the slot is active (connected)
    pub active: bool,
    /// Confirmed flush LSN position
    pub confirmed_flush_lsn: Option<String>,
    /// Restart LSN position
    pub restart_lsn: Option<String>,
    /// Retained WAL size in bytes
    pub wal_retained_bytes: u64,
    /// Database the slot is connected to
    pub database: String,
    /// When the slot was created
    pub created_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Last time the slot was active
    pub last_active: Option<Instant>,
}

/// Health status for a replication slot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SlotHealthStatus {
    /// Slot is healthy and actively replicating
    Healthy,
    /// Slot exists but is not connected (consumer is down)
    Idle { idle_duration: Duration },
    /// Slot is retaining too much WAL
    WalRetentionWarning { retained_bytes: u64, max_bytes: u64 },
    /// Slot is stale and should be investigated
    Stale { stale_duration: Duration },
    /// Slot is missing (was expected but not found)
    Missing,
}

impl std::fmt::Display for SlotHealthStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Healthy => write!(f, "HEALTHY"),
            Self::Idle { idle_duration } => {
                write!(f, "IDLE ({:.0}s)", idle_duration.as_secs_f64())
            }
            Self::WalRetentionWarning {
                retained_bytes,
                max_bytes,
            } => write!(
                f,
                "WAL_WARNING ({:.1}MB / {:.1}MB)",
                *retained_bytes as f64 / 1_048_576.0,
                *max_bytes as f64 / 1_048_576.0
            ),
            Self::Stale { stale_duration } => {
                write!(f, "STALE ({:.0}s)", stale_duration.as_secs_f64())
            }
            Self::Missing => write!(f, "MISSING"),
        }
    }
}

/// Replication slot manager
pub struct SlotManager {
    config: SlotManagerConfig,
    /// Known slots and their info
    slots: Arc<RwLock<HashMap<String, SlotInfo>>>,
    /// Expected slots (registered by CDC sources)
    expected_slots: Arc<RwLock<HashMap<String, String>>>, // slot_name -> source_name
}

impl SlotManager {
    /// Create a new slot manager
    pub fn new(config: SlotManagerConfig) -> Self {
        Self {
            config,
            slots: Arc::new(RwLock::new(HashMap::new())),
            expected_slots: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Generate a slot name for a CDC source
    pub fn slot_name_for(&self, source_name: &str) -> String {
        let sanitized = source_name
            .chars()
            .map(|c| if c.is_alphanumeric() || c == '_' { c } else { '_' })
            .collect::<String>();
        format!("{}{}", self.config.slot_name_prefix, sanitized)
    }

    /// Register an expected slot for a CDC source
    pub async fn register_expected_slot(&self, source_name: &str) -> String {
        let slot_name = self.slot_name_for(source_name);
        let mut expected = self.expected_slots.write().await;
        expected.insert(slot_name.clone(), source_name.to_string());
        info!(slot_name, source_name, "Registered expected replication slot");
        slot_name
    }

    /// Unregister an expected slot
    pub async fn unregister_expected_slot(&self, slot_name: &str) {
        let mut expected = self.expected_slots.write().await;
        expected.remove(slot_name);
    }

    /// Update slot information (from periodic health checks)
    pub async fn update_slot_info(&self, info: SlotInfo) {
        let mut slots = self.slots.write().await;
        slots.insert(info.name.clone(), info);
    }

    /// Update slot information from a batch (e.g., from pg_replication_slots query)
    pub async fn update_slots_batch(&self, slot_infos: Vec<SlotInfo>) {
        let mut slots = self.slots.write().await;
        slots.clear();
        for info in slot_infos {
            slots.insert(info.name.clone(), info);
        }
    }

    /// Get information about a specific slot
    pub async fn get_slot(&self, slot_name: &str) -> Option<SlotInfo> {
        let slots = self.slots.read().await;
        slots.get(slot_name).cloned()
    }

    /// List all known slots
    pub async fn list_slots(&self) -> Vec<SlotInfo> {
        let slots = self.slots.read().await;
        slots.values().cloned().collect()
    }

    /// Check health of a specific slot
    pub async fn check_slot_health(&self, slot_name: &str) -> SlotHealthStatus {
        let slots = self.slots.read().await;

        match slots.get(slot_name) {
            None => SlotHealthStatus::Missing,
            Some(info) => {
                if info.active {
                    if info.wal_retained_bytes > self.config.max_wal_retention_bytes {
                        SlotHealthStatus::WalRetentionWarning {
                            retained_bytes: info.wal_retained_bytes,
                            max_bytes: self.config.max_wal_retention_bytes,
                        }
                    } else {
                        SlotHealthStatus::Healthy
                    }
                } else {
                    let idle_duration = info
                        .last_active
                        .map(|t| t.elapsed())
                        .unwrap_or(Duration::from_secs(0));

                    if idle_duration > self.config.stale_threshold {
                        SlotHealthStatus::Stale {
                            stale_duration: idle_duration,
                        }
                    } else if info.wal_retained_bytes > self.config.max_wal_retention_bytes {
                        SlotHealthStatus::WalRetentionWarning {
                            retained_bytes: info.wal_retained_bytes,
                            max_bytes: self.config.max_wal_retention_bytes,
                        }
                    } else {
                        SlotHealthStatus::Idle { idle_duration }
                    }
                }
            }
        }
    }

    /// Check health of all expected slots
    pub async fn check_all_health(&self) -> HashMap<String, SlotHealthStatus> {
        let expected = self.expected_slots.read().await;
        let mut results = HashMap::new();

        for slot_name in expected.keys() {
            let health = self.check_slot_health(slot_name).await;
            results.insert(slot_name.clone(), health);
        }

        results
    }

    /// Find stale slots that should be cleaned up
    pub async fn find_stale_slots(&self) -> Vec<SlotInfo> {
        let slots = self.slots.read().await;
        let expected = self.expected_slots.read().await;

        slots
            .values()
            .filter(|info| {
                // Slot is stale if it starts with our prefix,
                // is not active, not in expected list, and has been idle too long
                info.name.starts_with(&self.config.slot_name_prefix)
                    && !info.active
                    && !expected.contains_key(&info.name)
                    && info
                        .last_active
                        .map(|t| t.elapsed() > self.config.stale_threshold)
                        .unwrap_or(true)
            })
            .cloned()
            .collect()
    }

    /// Generate SQL commands for slot operations
    pub fn sql_create_slot(&self, slot_name: &str) -> String {
        format!(
            "SELECT pg_create_logical_replication_slot('{}', '{}')",
            slot_name, self.config.output_plugin
        )
    }

    /// Generate SQL to drop a slot
    pub fn sql_drop_slot(&self, slot_name: &str) -> String {
        format!(
            "SELECT pg_drop_replication_slot('{}')",
            slot_name
        )
    }

    /// Generate SQL to query slot status
    pub fn sql_query_slots(&self) -> String {
        format!(
            "SELECT slot_name, plugin, active, confirmed_flush_lsn, restart_lsn, \
             pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS wal_retained_bytes, \
             database \
             FROM pg_replication_slots \
             WHERE slot_name LIKE '{}%'",
            self.config.slot_name_prefix
        )
    }

    /// Generate SQL to advance a slot's confirmed LSN
    pub fn sql_advance_slot(&self, slot_name: &str, lsn: &str) -> String {
        format!(
            "SELECT pg_replication_slot_advance('{}', '{}')",
            slot_name, lsn
        )
    }

    /// Get aggregated slot statistics
    pub async fn stats(&self) -> SlotManagerStats {
        let slots = self.slots.read().await;
        let expected = self.expected_slots.read().await;

        let total_slots = slots.len();
        let active_slots = slots.values().filter(|s| s.active).count();
        let total_wal_retained: u64 = slots.values().map(|s| s.wal_retained_bytes).sum();
        let missing_slots = expected
            .keys()
            .filter(|name| !slots.contains_key(*name))
            .count();

        SlotManagerStats {
            total_slots,
            active_slots,
            idle_slots: total_slots - active_slots,
            missing_slots,
            total_wal_retained_bytes: total_wal_retained,
            max_wal_retention_bytes: self.config.max_wal_retention_bytes,
        }
    }
}

/// Aggregated slot statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SlotManagerStats {
    pub total_slots: usize,
    pub active_slots: usize,
    pub idle_slots: usize,
    pub missing_slots: usize,
    pub total_wal_retained_bytes: u64,
    pub max_wal_retention_bytes: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_slot_name_generation() {
        let mgr = SlotManager::new(SlotManagerConfig::default());
        assert_eq!(
            mgr.slot_name_for("my-postgres"),
            "streamline_cdc_my_postgres"
        );
        assert_eq!(
            mgr.slot_name_for("db.source.1"),
            "streamline_cdc_db_source_1"
        );
    }

    #[tokio::test]
    async fn test_register_and_check_health_missing() {
        let mgr = SlotManager::new(SlotManagerConfig::default());
        let slot_name = mgr.register_expected_slot("test-source").await;

        let health = mgr.check_slot_health(&slot_name).await;
        assert!(matches!(health, SlotHealthStatus::Missing));
    }

    #[tokio::test]
    async fn test_slot_health_active() {
        let mgr = SlotManager::new(SlotManagerConfig::default());
        let slot_name = mgr.register_expected_slot("test-source").await;

        mgr.update_slot_info(SlotInfo {
            name: slot_name.clone(),
            plugin: "pgoutput".to_string(),
            active: true,
            confirmed_flush_lsn: Some("0/1234".to_string()),
            restart_lsn: Some("0/1000".to_string()),
            wal_retained_bytes: 1024,
            database: "testdb".to_string(),
            created_at: None,
            last_active: Some(Instant::now()),
        })
        .await;

        let health = mgr.check_slot_health(&slot_name).await;
        assert!(matches!(health, SlotHealthStatus::Healthy));
    }

    #[tokio::test]
    async fn test_slot_health_wal_warning() {
        let config = SlotManagerConfig {
            max_wal_retention_bytes: 1000,
            ..Default::default()
        };
        let mgr = SlotManager::new(config);
        let slot_name = mgr.register_expected_slot("test-source").await;

        mgr.update_slot_info(SlotInfo {
            name: slot_name.clone(),
            plugin: "pgoutput".to_string(),
            active: true,
            confirmed_flush_lsn: None,
            restart_lsn: None,
            wal_retained_bytes: 5000,
            database: "testdb".to_string(),
            created_at: None,
            last_active: Some(Instant::now()),
        })
        .await;

        let health = mgr.check_slot_health(&slot_name).await;
        assert!(matches!(health, SlotHealthStatus::WalRetentionWarning { .. }));
    }

    #[tokio::test]
    async fn test_sql_generation() {
        let mgr = SlotManager::new(SlotManagerConfig::default());
        let sql = mgr.sql_create_slot("streamline_cdc_test");
        assert!(sql.contains("pg_create_logical_replication_slot"));
        assert!(sql.contains("pgoutput"));
    }

    #[tokio::test]
    async fn test_stats() {
        let mgr = SlotManager::new(SlotManagerConfig::default());
        mgr.register_expected_slot("source1").await;
        mgr.register_expected_slot("source2").await;

        let stats = mgr.stats().await;
        assert_eq!(stats.total_slots, 0);
        assert_eq!(stats.missing_slots, 2);
    }
}
