//! Battery-Aware & Power-Adaptive Operation for Edge Deployments
//!
//! Provides intelligent power management for Streamline running on
//! battery-powered edge devices (IoT gateways, mobile, drones):
//!
//! - **Power Profiles**: Predefined modes (Full, Balanced, PowerSaver, Critical)
//!   that adjust sync frequency, compression, and buffer sizes.
//! - **Battery Monitoring**: Reads battery level and adapts behavior automatically.
//! - **Adaptive Sync**: Reduces sync frequency as battery drops; queues messages
//!   locally and bulk-syncs when charging.
//! - **Mesh Health**: Adjusts mesh participation (discovery broadcasting,
//!   replication) based on power budget.

use crate::error::{Result, StreamlineError};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Power profile presets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PowerProfile {
    /// Full performance — no power restrictions.
    Full,
    /// Balanced — moderate sync frequency, standard compression.
    Balanced,
    /// Power saver — reduced sync, aggressive compression, mesh silent.
    PowerSaver,
    /// Critical — minimal operation, local-only writes, no mesh.
    Critical,
}

impl PowerProfile {
    /// Get the sync interval multiplier for this profile.
    pub fn sync_interval_multiplier(&self) -> f64 {
        match self {
            PowerProfile::Full => 1.0,
            PowerProfile::Balanced => 2.0,
            PowerProfile::PowerSaver => 5.0,
            PowerProfile::Critical => 0.0, // No sync
        }
    }

    /// Whether mesh discovery broadcasting is allowed.
    pub fn mesh_discovery_allowed(&self) -> bool {
        matches!(self, PowerProfile::Full | PowerProfile::Balanced)
    }

    /// Whether WAN replication is allowed.
    pub fn wan_replication_allowed(&self) -> bool {
        !matches!(self, PowerProfile::Critical)
    }

    /// Maximum concurrent connections for this profile.
    pub fn max_connections(&self) -> u32 {
        match self {
            PowerProfile::Full => 100,
            PowerProfile::Balanced => 50,
            PowerProfile::PowerSaver => 10,
            PowerProfile::Critical => 2,
        }
    }

    /// Compression level (higher = more aggressive = more CPU but less I/O).
    pub fn compression_level(&self) -> u32 {
        match self {
            PowerProfile::Full => 1,      // Fast
            PowerProfile::Balanced => 3,   // Standard
            PowerProfile::PowerSaver => 6, // High
            PowerProfile::Critical => 1,   // Fast (save CPU)
        }
    }
}

/// Battery status information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatteryStatus {
    /// Battery level (0–100), or None if not available / AC-powered.
    pub level_pct: Option<u8>,
    /// Whether the device is currently charging.
    pub charging: bool,
    /// Estimated time remaining in minutes.
    pub time_remaining_mins: Option<u32>,
    /// Power source.
    pub power_source: PowerSource,
    /// Timestamp of this reading.
    pub timestamp: DateTime<Utc>,
}

impl Default for BatteryStatus {
    fn default() -> Self {
        Self {
            level_pct: None,
            charging: false,
            time_remaining_mins: None,
            power_source: PowerSource::Unknown,
            timestamp: Utc::now(),
        }
    }
}

/// Power source types.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PowerSource {
    /// Mains/AC power.
    Ac,
    /// Battery.
    Battery,
    /// USB power (limited).
    Usb,
    /// Solar.
    Solar,
    /// Unknown.
    Unknown,
}

/// Configuration for power-aware operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PowerConfig {
    /// Enable automatic power profile switching.
    pub auto_profile: bool,
    /// Battery level thresholds for profile switching.
    pub thresholds: PowerThresholds,
    /// Override profile (None = automatic).
    pub override_profile: Option<PowerProfile>,
    /// Monitor interval in seconds.
    pub monitor_interval_secs: u64,
}

impl Default for PowerConfig {
    fn default() -> Self {
        Self {
            auto_profile: true,
            thresholds: PowerThresholds::default(),
            override_profile: None,
            monitor_interval_secs: 30,
        }
    }
}

/// Battery level thresholds for automatic profile switching.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PowerThresholds {
    /// Below this level → PowerSaver (default: 30%).
    pub power_saver_pct: u8,
    /// Below this level → Critical (default: 10%).
    pub critical_pct: u8,
    /// Above this level → Full (default: 80%).
    pub full_pct: u8,
}

impl Default for PowerThresholds {
    fn default() -> Self {
        Self {
            power_saver_pct: 30,
            critical_pct: 10,
            full_pct: 80,
        }
    }
}

/// Power-aware operation manager.
pub struct PowerManager {
    config: PowerConfig,
    current_profile: AtomicU8,
    battery_status: RwLock<BatteryStatus>,
    profile_history: RwLock<Vec<ProfileChange>>,
}

/// Record of a profile change.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileChange {
    pub from: PowerProfile,
    pub to: PowerProfile,
    pub reason: String,
    pub battery_level: Option<u8>,
    pub timestamp: DateTime<Utc>,
}

impl PowerManager {
    /// Create a new power manager.
    pub fn new(config: PowerConfig) -> Self {
        let initial = config
            .override_profile
            .unwrap_or(PowerProfile::Full);

        Self {
            config,
            current_profile: AtomicU8::new(Self::profile_to_u8(initial)),
            battery_status: RwLock::new(BatteryStatus::default()),
            profile_history: RwLock::new(Vec::new()),
        }
    }

    /// Get the current power profile.
    pub fn current_profile(&self) -> PowerProfile {
        Self::u8_to_profile(self.current_profile.load(Ordering::Relaxed))
    }

    /// Get current battery status.
    pub async fn battery_status(&self) -> BatteryStatus {
        self.battery_status.read().await.clone()
    }

    /// Update battery status and potentially switch profiles.
    pub async fn update_battery(&self, status: BatteryStatus) -> Option<ProfileChange> {
        let old_profile = self.current_profile();
        *self.battery_status.write().await = status.clone();

        if let Some(override_p) = self.config.override_profile {
            if old_profile != override_p {
                self.set_profile(override_p);
            }
            return None;
        }

        if !self.config.auto_profile {
            return None;
        }

        let new_profile = self.compute_profile(&status);
        if new_profile == old_profile {
            return None;
        }

        self.set_profile(new_profile);

        let change = ProfileChange {
            from: old_profile,
            to: new_profile,
            reason: format!(
                "Battery {}% ({})",
                status.level_pct.unwrap_or(0),
                if status.charging { "charging" } else { "discharging" }
            ),
            battery_level: status.level_pct,
            timestamp: Utc::now(),
        };

        self.profile_history.write().await.push(change.clone());
        tracing::info!(
            from = ?old_profile,
            to = ?new_profile,
            battery = ?status.level_pct,
            "Power profile changed"
        );

        Some(change)
    }

    /// Manually set the power profile.
    pub fn set_profile(&self, profile: PowerProfile) {
        self.current_profile
            .store(Self::profile_to_u8(profile), Ordering::Relaxed);
    }

    /// Get profile change history.
    pub async fn profile_history(&self) -> Vec<ProfileChange> {
        self.profile_history.read().await.clone()
    }

    /// Check if a specific operation is allowed under the current profile.
    pub fn is_allowed(&self, operation: EdgeOperation) -> bool {
        let profile = self.current_profile();
        match operation {
            EdgeOperation::WanSync => profile.wan_replication_allowed(),
            EdgeOperation::MeshDiscovery => profile.mesh_discovery_allowed(),
            EdgeOperation::AcceptConnection => {
                // Always allow at least some connections
                true
            }
            EdgeOperation::BackgroundCompaction => {
                matches!(profile, PowerProfile::Full | PowerProfile::Balanced)
            }
        }
    }

    fn compute_profile(&self, status: &BatteryStatus) -> PowerProfile {
        // Charging → Full performance
        if status.charging || status.power_source == PowerSource::Ac {
            return PowerProfile::Full;
        }

        match status.level_pct {
            Some(level) if level <= self.config.thresholds.critical_pct => {
                PowerProfile::Critical
            }
            Some(level) if level <= self.config.thresholds.power_saver_pct => {
                PowerProfile::PowerSaver
            }
            Some(level) if level >= self.config.thresholds.full_pct => {
                PowerProfile::Full
            }
            Some(_) => PowerProfile::Balanced,
            None => PowerProfile::Balanced, // Unknown battery → balanced
        }
    }

    fn profile_to_u8(profile: PowerProfile) -> u8 {
        match profile {
            PowerProfile::Full => 0,
            PowerProfile::Balanced => 1,
            PowerProfile::PowerSaver => 2,
            PowerProfile::Critical => 3,
        }
    }

    fn u8_to_profile(val: u8) -> PowerProfile {
        match val {
            0 => PowerProfile::Full,
            1 => PowerProfile::Balanced,
            2 => PowerProfile::PowerSaver,
            3 => PowerProfile::Critical,
            _ => PowerProfile::Balanced,
        }
    }
}

/// Operations that can be gate-checked against the power profile.
#[derive(Debug, Clone, Copy)]
pub enum EdgeOperation {
    /// Synchronize with cloud/WAN.
    WanSync,
    /// Broadcast mesh discovery packets.
    MeshDiscovery,
    /// Accept a new inbound connection.
    AcceptConnection,
    /// Run background segment compaction.
    BackgroundCompaction,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_power_profile_defaults() {
        assert!(PowerProfile::Full.mesh_discovery_allowed());
        assert!(!PowerProfile::Critical.mesh_discovery_allowed());
        assert_eq!(PowerProfile::Full.max_connections(), 100);
        assert_eq!(PowerProfile::Critical.max_connections(), 2);
    }

    #[test]
    fn test_power_manager_creation() {
        let mgr = PowerManager::new(PowerConfig::default());
        assert_eq!(mgr.current_profile(), PowerProfile::Full);
    }

    #[tokio::test]
    async fn test_auto_profile_switching() {
        let mgr = PowerManager::new(PowerConfig::default());

        // Battery drops to 25% — should switch to PowerSaver
        let status = BatteryStatus {
            level_pct: Some(25),
            charging: false,
            power_source: PowerSource::Battery,
            ..Default::default()
        };

        let change = mgr.update_battery(status).await;
        assert!(change.is_some());
        assert_eq!(mgr.current_profile(), PowerProfile::PowerSaver);
    }

    #[tokio::test]
    async fn test_critical_mode() {
        let mgr = PowerManager::new(PowerConfig::default());

        let status = BatteryStatus {
            level_pct: Some(5),
            charging: false,
            power_source: PowerSource::Battery,
            ..Default::default()
        };

        mgr.update_battery(status).await;
        assert_eq!(mgr.current_profile(), PowerProfile::Critical);
        assert!(!mgr.is_allowed(EdgeOperation::MeshDiscovery));
        assert!(!mgr.is_allowed(EdgeOperation::BackgroundCompaction));
    }

    #[tokio::test]
    async fn test_charging_restores_full() {
        let mgr = PowerManager::new(PowerConfig::default());

        // First drain
        mgr.update_battery(BatteryStatus {
            level_pct: Some(15),
            charging: false,
            power_source: PowerSource::Battery,
            ..Default::default()
        })
        .await;

        // Then charge
        let change = mgr
            .update_battery(BatteryStatus {
                level_pct: Some(15),
                charging: true,
                power_source: PowerSource::Ac,
                ..Default::default()
            })
            .await;

        assert!(change.is_some());
        assert_eq!(mgr.current_profile(), PowerProfile::Full);
    }

    #[tokio::test]
    async fn test_override_profile() {
        let mgr = PowerManager::new(PowerConfig {
            override_profile: Some(PowerProfile::PowerSaver),
            ..Default::default()
        });

        // Even at 100% battery, should stay in PowerSaver
        mgr.update_battery(BatteryStatus {
            level_pct: Some(100),
            charging: true,
            power_source: PowerSource::Ac,
            ..Default::default()
        })
        .await;

        assert_eq!(mgr.current_profile(), PowerProfile::PowerSaver);
    }

    #[test]
    fn test_operation_checks() {
        let mgr = PowerManager::new(PowerConfig::default());
        assert!(mgr.is_allowed(EdgeOperation::WanSync));
        assert!(mgr.is_allowed(EdgeOperation::MeshDiscovery));

        mgr.set_profile(PowerProfile::Critical);
        assert!(!mgr.is_allowed(EdgeOperation::WanSync));
    }
}
