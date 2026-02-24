//! Fleet Management for Edge-to-Cloud Mesh
//!
//! Manages the lifecycle of edge devices from the cloud control plane:
//! - Auto-registration: edge devices register on first boot
//! - Fleet inventory: track all edge devices, status, sync lag
//! - Remote configuration: push config updates to edge fleet
//! - Health monitoring: detect offline/degraded devices
//!
//! # Protocol
//!
//! ```text
//! Edge Device                              Cloud Control Plane
//!     │                                            │
//!     │──── POST /api/v1/fleet/register ──────────►│
//!     │     { edge_id, capabilities, version }     │
//!     │◄─── 200 { token, config, topics_to_sync } ─│
//!     │                                            │
//!     │──── POST /api/v1/fleet/heartbeat ─────────►│
//!     │     { edge_id, sync_lag, metrics }         │
//!     │◄─── 200 { config_version, commands }  ─────│
//!     │                                            │
//!     │──── POST /api/v1/fleet/sync-progress ─────►│
//!     │     { edge_id, topic, offset, lag }        │
//!     │◄─── 200 ──────────────────────────────────│
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::info;

/// Edge device registration request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeRegistration {
    pub edge_id: String,
    pub hostname: Option<String>,
    pub version: String,
    pub platform: String,
    pub capabilities: EdgeCapabilities,
    pub location: Option<GeoLocation>,
}

/// Capabilities reported by the edge device
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeCapabilities {
    pub mqtt_bridge: bool,
    pub max_topics: u32,
    pub max_storage_mb: u64,
    pub cpu_cores: u32,
    pub memory_mb: u64,
    pub protocols: Vec<String>,
}

/// Geographic location
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeoLocation {
    pub latitude: f64,
    pub longitude: f64,
    pub region: Option<String>,
}

/// Registration response from the cloud
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistrationResponse {
    pub accepted: bool,
    pub fleet_token: String,
    pub config_version: u64,
    pub topics_to_sync: Vec<TopicSyncConfig>,
    pub heartbeat_interval_secs: u64,
    pub message: String,
}

/// Configuration for a topic to sync between edge and cloud
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicSyncConfig {
    pub topic: String,
    pub direction: SyncDirection,
    pub priority: SyncPriority,
    pub max_bandwidth_kbps: Option<u64>,
}

/// Sync direction
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SyncDirection {
    EdgeToCloud,
    CloudToEdge,
    Bidirectional,
}

/// Sync priority
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum SyncPriority {
    Low,
    Normal,
    High,
    Critical,
}

/// Heartbeat from edge to cloud
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeHeartbeat {
    pub edge_id: String,
    pub uptime_secs: u64,
    pub sync_lag: HashMap<String, i64>,
    pub metrics: EdgeMetrics,
    pub status: EdgeStatus,
}

/// Edge device metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeMetrics {
    pub cpu_usage_pct: f64,
    pub memory_usage_pct: f64,
    pub storage_used_mb: u64,
    pub messages_in_buffer: u64,
    pub topics_count: u32,
    pub connections_count: u32,
}

/// Edge device status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum EdgeStatus {
    Online,
    Syncing,
    Degraded,
    Offline,
    Maintenance,
}

impl std::fmt::Display for EdgeStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Online => write!(f, "online"),
            Self::Syncing => write!(f, "syncing"),
            Self::Degraded => write!(f, "degraded"),
            Self::Offline => write!(f, "offline"),
            Self::Maintenance => write!(f, "maintenance"),
        }
    }
}

/// Heartbeat response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatResponse {
    pub config_version: u64,
    pub commands: Vec<FleetCommand>,
}

/// Remote commands from cloud to edge
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FleetCommand {
    UpdateConfig { config_yaml: String },
    SyncTopic { topic: String, direction: SyncDirection },
    StopSync { topic: String },
    Restart,
    DrainAndShutdown,
    RunDiagnostics,
}

/// Tracked state of an edge device
#[derive(Debug, Clone)]
pub struct EdgeDeviceState {
    pub registration: EdgeRegistration,
    pub fleet_token: String,
    pub last_heartbeat: Instant,
    pub last_metrics: EdgeMetrics,
    pub status: EdgeStatus,
    pub sync_lag: HashMap<String, i64>,
    pub config_version: u64,
    pub registered_at: Instant,
    pub pending_commands: Vec<FleetCommand>,
}

/// Fleet summary for the cloud console dashboard
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FleetSummary {
    pub total_devices: usize,
    pub online: usize,
    pub syncing: usize,
    pub degraded: usize,
    pub offline: usize,
    pub total_sync_lag: i64,
    pub total_messages_buffered: u64,
    pub devices: Vec<DeviceSummary>,
}

/// Per-device summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceSummary {
    pub edge_id: String,
    pub status: String,
    pub version: String,
    pub platform: String,
    pub region: Option<String>,
    pub topics: u32,
    pub sync_lag: i64,
    pub last_seen_secs_ago: u64,
    pub cpu_pct: f64,
    pub memory_pct: f64,
}

/// The fleet manager
pub struct FleetManager {
    devices: Arc<RwLock<HashMap<String, EdgeDeviceState>>>,
    config_version: Arc<std::sync::atomic::AtomicU64>,
    offline_threshold: Duration,
}

impl FleetManager {
    pub fn new(offline_threshold_secs: u64) -> Self {
        Self {
            devices: Arc::new(RwLock::new(HashMap::new())),
            config_version: Arc::new(std::sync::atomic::AtomicU64::new(1)),
            offline_threshold: Duration::from_secs(offline_threshold_secs),
        }
    }

    /// Register a new edge device
    pub async fn register(&self, reg: EdgeRegistration) -> RegistrationResponse {
        let token = format!("edge_tok_{}", uuid::Uuid::new_v4().to_string().replace('-', "")[..16].to_string());

        let default_sync = vec![
            TopicSyncConfig {
                topic: "events".into(),
                direction: SyncDirection::EdgeToCloud,
                priority: SyncPriority::Normal,
                max_bandwidth_kbps: None,
            },
        ];

        let config_version = self.config_version.load(std::sync::atomic::Ordering::Relaxed);

        let state = EdgeDeviceState {
            registration: reg.clone(),
            fleet_token: token.clone(),
            last_heartbeat: Instant::now(),
            last_metrics: EdgeMetrics {
                cpu_usage_pct: 0.0, memory_usage_pct: 0.0,
                storage_used_mb: 0, messages_in_buffer: 0,
                topics_count: 0, connections_count: 0,
            },
            status: EdgeStatus::Online,
            sync_lag: HashMap::new(),
            config_version,
            registered_at: Instant::now(),
            pending_commands: Vec::new(),
        };

        self.devices.write().await.insert(reg.edge_id.clone(), state);
        info!(edge_id = %reg.edge_id, "Edge device registered");

        RegistrationResponse {
            accepted: true,
            fleet_token: token,
            config_version,
            topics_to_sync: default_sync,
            heartbeat_interval_secs: 30,
            message: format!("Welcome, {}!", reg.edge_id),
        }
    }

    /// Process a heartbeat from an edge device
    pub async fn heartbeat(&self, hb: EdgeHeartbeat) -> Option<HeartbeatResponse> {
        let mut devices = self.devices.write().await;
        let device = devices.get_mut(&hb.edge_id)?;

        device.last_heartbeat = Instant::now();
        device.last_metrics = hb.metrics;
        device.status = hb.status;
        device.sync_lag = hb.sync_lag;

        let commands = std::mem::take(&mut device.pending_commands);

        Some(HeartbeatResponse {
            config_version: self.config_version.load(std::sync::atomic::Ordering::Relaxed),
            commands,
        })
    }

    /// Send a command to an edge device
    pub async fn send_command(&self, edge_id: &str, command: FleetCommand) -> bool {
        let mut devices = self.devices.write().await;
        if let Some(device) = devices.get_mut(edge_id) {
            device.pending_commands.push(command);
            true
        } else {
            false
        }
    }

    /// Get fleet summary for the dashboard
    pub async fn summary(&self) -> FleetSummary {
        let devices = self.devices.read().await;
        let now = Instant::now();

        let mut summary = FleetSummary {
            total_devices: devices.len(),
            online: 0, syncing: 0, degraded: 0, offline: 0,
            total_sync_lag: 0, total_messages_buffered: 0,
            devices: Vec::new(),
        };

        for (_, device) in devices.iter() {
            let actual_status = if now.duration_since(device.last_heartbeat) > self.offline_threshold {
                EdgeStatus::Offline
            } else {
                device.status.clone()
            };

            match actual_status {
                EdgeStatus::Online => summary.online += 1,
                EdgeStatus::Syncing => summary.syncing += 1,
                EdgeStatus::Degraded => summary.degraded += 1,
                EdgeStatus::Offline => summary.offline += 1,
                EdgeStatus::Maintenance => {}
            }

            let device_lag: i64 = device.sync_lag.values().sum();
            summary.total_sync_lag += device_lag;
            summary.total_messages_buffered += device.last_metrics.messages_in_buffer;

            summary.devices.push(DeviceSummary {
                edge_id: device.registration.edge_id.clone(),
                status: actual_status.to_string(),
                version: device.registration.version.clone(),
                platform: device.registration.platform.clone(),
                region: device.registration.location.as_ref().and_then(|l| l.region.clone()),
                topics: device.last_metrics.topics_count,
                sync_lag: device_lag,
                last_seen_secs_ago: now.duration_since(device.last_heartbeat).as_secs(),
                cpu_pct: device.last_metrics.cpu_usage_pct,
                memory_pct: device.last_metrics.memory_usage_pct,
            });
        }

        summary.devices.sort_by(|a, b| a.edge_id.cmp(&b.edge_id));
        summary
    }

    /// Get a specific device's state
    pub async fn get_device(&self, edge_id: &str) -> Option<DeviceSummary> {
        let devices = self.devices.read().await;
        let device = devices.get(edge_id)?;
        let now = Instant::now();

        Some(DeviceSummary {
            edge_id: device.registration.edge_id.clone(),
            status: if now.duration_since(device.last_heartbeat) > self.offline_threshold {
                "offline".into()
            } else {
                device.status.to_string()
            },
            version: device.registration.version.clone(),
            platform: device.registration.platform.clone(),
            region: device.registration.location.as_ref().and_then(|l| l.region.clone()),
            topics: device.last_metrics.topics_count,
            sync_lag: device.sync_lag.values().sum(),
            last_seen_secs_ago: now.duration_since(device.last_heartbeat).as_secs(),
            cpu_pct: device.last_metrics.cpu_usage_pct,
            memory_pct: device.last_metrics.memory_usage_pct,
        })
    }

    /// Count of devices
    pub async fn device_count(&self) -> usize {
        self.devices.read().await.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_registration() -> EdgeRegistration {
        EdgeRegistration {
            edge_id: "edge-001".into(),
            hostname: Some("gateway-01.local".into()),
            version: "0.2.0".into(),
            platform: "linux-aarch64".into(),
            capabilities: EdgeCapabilities {
                mqtt_bridge: true,
                max_topics: 100,
                max_storage_mb: 1024,
                cpu_cores: 4,
                memory_mb: 2048,
                protocols: vec!["kafka".into(), "mqtt".into()],
            },
            location: Some(GeoLocation {
                latitude: 37.7749,
                longitude: -122.4194,
                region: Some("us-west".into()),
            }),
        }
    }

    #[tokio::test]
    async fn test_register_and_heartbeat() {
        let mgr = FleetManager::new(60);

        let resp = mgr.register(test_registration()).await;
        assert!(resp.accepted);
        assert!(!resp.fleet_token.is_empty());
        assert_eq!(mgr.device_count().await, 1);

        let hb = EdgeHeartbeat {
            edge_id: "edge-001".into(),
            uptime_secs: 3600,
            sync_lag: [("events".into(), 42)].into(),
            metrics: EdgeMetrics {
                cpu_usage_pct: 23.5,
                memory_usage_pct: 45.0,
                storage_used_mb: 256,
                messages_in_buffer: 100,
                topics_count: 5,
                connections_count: 3,
            },
            status: EdgeStatus::Syncing,
        };

        let hb_resp = mgr.heartbeat(hb).await.unwrap();
        assert!(hb_resp.commands.is_empty());
    }

    #[tokio::test]
    async fn test_fleet_summary() {
        let mgr = FleetManager::new(60);
        mgr.register(test_registration()).await;

        let summary = mgr.summary().await;
        assert_eq!(summary.total_devices, 1);
        assert_eq!(summary.online, 1);
        assert_eq!(summary.devices.len(), 1);
        assert_eq!(summary.devices[0].edge_id, "edge-001");
    }

    #[tokio::test]
    async fn test_send_command() {
        let mgr = FleetManager::new(60);
        mgr.register(test_registration()).await;

        mgr.send_command("edge-001", FleetCommand::RunDiagnostics).await;

        let hb = EdgeHeartbeat {
            edge_id: "edge-001".into(),
            uptime_secs: 100,
            sync_lag: HashMap::new(),
            metrics: EdgeMetrics {
                cpu_usage_pct: 0.0, memory_usage_pct: 0.0,
                storage_used_mb: 0, messages_in_buffer: 0,
                topics_count: 0, connections_count: 0,
            },
            status: EdgeStatus::Online,
        };
        let resp = mgr.heartbeat(hb).await.unwrap();
        assert_eq!(resp.commands.len(), 1);
    }

    #[tokio::test]
    async fn test_offline_detection() {
        let mgr = FleetManager::new(0); // 0 second threshold = immediately offline
        mgr.register(test_registration()).await;

        // Wait a tick for the threshold
        tokio::time::sleep(Duration::from_millis(10)).await;

        let summary = mgr.summary().await;
        assert_eq!(summary.offline, 1);
    }
}
