//! Edge-to-cloud federation
//!
//! Manages the federation relationship between edge nodes and cloud.

use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use super::config::EdgeConfig;
use super::sync::SyncResult;

/// Federation state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum FederationState {
    /// Not connected to cloud
    #[default]
    Disconnected,
    /// Connecting to cloud
    Connecting,
    /// Connected and healthy
    Connected,
    /// Connected but degraded (e.g., high latency)
    Degraded,
    /// Syncing data
    Syncing,
    /// Connection failed
    Failed,
}

/// Edge node information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeNodeInfo {
    /// Unique edge ID
    pub edge_id: String,
    /// Edge name (human-readable)
    pub name: String,
    /// Edge location/region
    pub location: Option<String>,
    /// Edge capabilities
    pub capabilities: EdgeCapabilities,
    /// Current state
    pub state: FederationState,
    /// Last heartbeat timestamp
    pub last_heartbeat: i64,
    /// Registration timestamp
    pub registered_at: i64,
    /// Topics subscribed on this edge
    pub subscribed_topics: Vec<String>,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
}

/// Edge capabilities
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EdgeCapabilities {
    /// Maximum storage in bytes
    pub max_storage_bytes: u64,
    /// Maximum memory in bytes
    pub max_memory_bytes: u64,
    /// Whether compression is supported
    pub compression_supported: bool,
    /// Whether encryption is supported
    pub encryption_supported: bool,
    /// Supported sync modes
    pub sync_modes: Vec<String>,
    /// Protocol version
    pub protocol_version: String,
}

/// Federation statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FederationStats {
    /// Total heartbeats sent
    pub heartbeats_sent: u64,
    /// Total heartbeats failed
    pub heartbeats_failed: u64,
    /// Total syncs initiated
    pub syncs_initiated: u64,
    /// Total syncs completed
    pub syncs_completed: u64,
    /// Total syncs failed
    pub syncs_failed: u64,
    /// Total bytes uploaded
    pub bytes_uploaded: u64,
    /// Total bytes downloaded
    pub bytes_downloaded: u64,
    /// Average latency in ms
    pub avg_latency_ms: u64,
    /// Current latency in ms
    pub current_latency_ms: u64,
    /// Connection uptime in seconds
    pub uptime_seconds: u64,
    /// Last successful sync
    pub last_sync_at: Option<i64>,
    /// Last error message
    pub last_error: Option<String>,
}

/// Heartbeat request to cloud
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatRequest {
    /// Edge ID
    pub edge_id: String,
    /// Current state
    pub state: FederationState,
    /// Current capabilities
    pub capabilities: EdgeCapabilities,
    /// Subscribed topics
    pub subscribed_topics: Vec<String>,
    /// Pending sync records count
    pub pending_records: u64,
    /// Current timestamp
    pub timestamp: i64,
}

/// Heartbeat response from cloud
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatResponse {
    /// Whether heartbeat was accepted
    pub accepted: bool,
    /// Cloud timestamp
    pub cloud_timestamp: i64,
    /// Commands for edge to execute
    pub commands: Vec<FederationCommand>,
    /// Updated topic subscriptions (if changed)
    pub topic_updates: Option<Vec<String>>,
    /// Configuration updates
    pub config_updates: Option<HashMap<String, String>>,
}

/// Commands from cloud to edge
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FederationCommand {
    /// Trigger immediate sync
    SyncNow { direction: String },
    /// Pause syncing
    PauseSync,
    /// Resume syncing
    ResumeSync,
    /// Update sync interval
    UpdateSyncInterval { interval_ms: u64 },
    /// Subscribe to new topic
    SubscribeTopic { topic: String },
    /// Unsubscribe from topic
    UnsubscribeTopic { topic: String },
    /// Reset checkpoint for topic
    ResetCheckpoint { topic: String },
    /// Shutdown edge
    Shutdown,
}

/// Federation manager
pub struct FederationManager {
    #[allow(dead_code)]
    config: EdgeConfig,
    state: parking_lot::RwLock<FederationState>,
    stats: parking_lot::RwLock<FederationStats>,
    node_info: parking_lot::RwLock<EdgeNodeInfo>,
    running: AtomicBool,
    connected_since: AtomicU64,
    #[allow(dead_code, clippy::type_complexity)]
    command_handlers:
        parking_lot::RwLock<Vec<Box<dyn Fn(FederationCommand) -> Result<()> + Send + Sync>>>,
}

impl FederationManager {
    /// Create a new federation manager
    pub fn new(config: EdgeConfig) -> Self {
        let node_info = EdgeNodeInfo {
            edge_id: config.edge_id.clone(),
            name: format!("Edge-{}", &config.edge_id[..8.min(config.edge_id.len())]),
            location: None,
            capabilities: EdgeCapabilities {
                max_storage_bytes: config.storage.max_storage_bytes,
                max_memory_bytes: config.resources.max_memory_bytes,
                compression_supported: true,
                encryption_supported: config.network.tls_enabled,
                sync_modes: vec![format!("{:?}", config.sync.mode)],
                protocol_version: "1.0".to_string(),
            },
            state: FederationState::Disconnected,
            last_heartbeat: 0,
            registered_at: chrono::Utc::now().timestamp_millis(),
            subscribed_topics: config.sync.sync_topics.clone(),
            metadata: HashMap::new(),
        };

        Self {
            config,
            state: parking_lot::RwLock::new(FederationState::Disconnected),
            stats: parking_lot::RwLock::new(FederationStats::default()),
            node_info: parking_lot::RwLock::new(node_info),
            running: AtomicBool::new(false),
            connected_since: AtomicU64::new(0),
            command_handlers: parking_lot::RwLock::new(Vec::new()),
        }
    }

    /// Get current federation state
    pub fn state(&self) -> FederationState {
        *self.state.read()
    }

    /// Get federation statistics
    pub fn stats(&self) -> FederationStats {
        self.stats.read().clone()
    }

    /// Get node info
    pub fn node_info(&self) -> EdgeNodeInfo {
        self.node_info.read().clone()
    }

    /// Check if connected
    pub fn is_connected(&self) -> bool {
        matches!(
            *self.state.read(),
            FederationState::Connected | FederationState::Degraded | FederationState::Syncing
        )
    }

    /// Start federation (heartbeat loop)
    pub async fn start(&self) -> Result<()> {
        if self.running.swap(true, Ordering::SeqCst) {
            return Err(StreamlineError::storage_msg(
                "Federation already running".into(),
            ));
        }

        *self.state.write() = FederationState::Connecting;

        // Start heartbeat loop
        self.run_heartbeat_loop().await
    }

    /// Stop federation
    pub async fn stop(&self) -> Result<()> {
        self.running.store(false, Ordering::SeqCst);
        *self.state.write() = FederationState::Disconnected;
        Ok(())
    }

    /// Run heartbeat loop
    async fn run_heartbeat_loop(&self) -> Result<()> {
        let heartbeat_interval = Duration::from_secs(30); // Default 30s

        while self.running.load(Ordering::SeqCst) {
            match self.send_heartbeat().await {
                Ok(response) => {
                    self.handle_heartbeat_response(response).await?;

                    // Update state to connected if not already
                    if *self.state.read() == FederationState::Connecting {
                        *self.state.write() = FederationState::Connected;
                        self.connected_since
                            .store(chrono::Utc::now().timestamp() as u64, Ordering::SeqCst);
                    }
                }
                Err(e) => {
                    self.stats.write().heartbeats_failed += 1;
                    self.stats.write().last_error = Some(e.to_string());

                    // If we were connected, mark as degraded
                    if self.is_connected() {
                        *self.state.write() = FederationState::Degraded;
                    }
                }
            }

            // Update uptime
            if self.is_connected() {
                let connected_since = self.connected_since.load(Ordering::SeqCst);
                if connected_since > 0 {
                    self.stats.write().uptime_seconds =
                        (chrono::Utc::now().timestamp() as u64).saturating_sub(connected_since);
                }
            }

            tokio::time::sleep(heartbeat_interval).await;
        }

        Ok(())
    }

    /// Send heartbeat to cloud
    async fn send_heartbeat(&self) -> Result<HeartbeatResponse> {
        let _request = {
            let node_info = self.node_info.read();
            HeartbeatRequest {
                edge_id: node_info.edge_id.clone(),
                state: node_info.state,
                capabilities: node_info.capabilities.clone(),
                subscribed_topics: node_info.subscribed_topics.clone(),
                pending_records: 0, // Would be calculated from actual pending records
                timestamp: chrono::Utc::now().timestamp_millis(),
            }
        };

        // In a real implementation, this would send HTTP request to cloud
        // For now, simulate success
        let start = std::time::Instant::now();

        // Simulate network delay
        tokio::time::sleep(Duration::from_millis(10)).await;

        let latency = start.elapsed().as_millis() as u64;

        // Update stats
        {
            let mut stats = self.stats.write();
            stats.heartbeats_sent += 1;
            stats.current_latency_ms = latency;
            stats.avg_latency_ms = (stats.avg_latency_ms + latency) / 2;
        }

        // Update node info
        {
            let mut node_info = self.node_info.write();
            node_info.last_heartbeat = chrono::Utc::now().timestamp_millis();
            node_info.state = *self.state.read();
        }

        // Simulated response
        Ok(HeartbeatResponse {
            accepted: true,
            cloud_timestamp: chrono::Utc::now().timestamp_millis(),
            commands: Vec::new(),
            topic_updates: None,
            config_updates: None,
        })
    }

    /// Handle heartbeat response
    async fn handle_heartbeat_response(&self, response: HeartbeatResponse) -> Result<()> {
        if !response.accepted {
            return Err(StreamlineError::storage_msg(
                "Heartbeat rejected by cloud".into(),
            ));
        }

        // Process commands
        for command in response.commands {
            self.handle_command(command).await?;
        }

        // Update topic subscriptions if needed
        if let Some(topics) = response.topic_updates {
            self.node_info.write().subscribed_topics = topics;
        }

        Ok(())
    }

    /// Handle command from cloud
    async fn handle_command(&self, command: FederationCommand) -> Result<()> {
        match command {
            FederationCommand::SyncNow { direction: _ } => {
                *self.state.write() = FederationState::Syncing;
                // Trigger sync via callback
            }
            FederationCommand::PauseSync => {
                // Pause sync engine
            }
            FederationCommand::ResumeSync => {
                // Resume sync engine
            }
            FederationCommand::UpdateSyncInterval { interval_ms: _ } => {
                // Update sync config
            }
            FederationCommand::SubscribeTopic { topic } => {
                let mut node_info = self.node_info.write();
                if !node_info.subscribed_topics.contains(&topic) {
                    node_info.subscribed_topics.push(topic);
                }
            }
            FederationCommand::UnsubscribeTopic { topic } => {
                let mut node_info = self.node_info.write();
                node_info.subscribed_topics.retain(|t| t != &topic);
            }
            FederationCommand::ResetCheckpoint { topic: _ } => {
                // Reset checkpoint for topic
            }
            FederationCommand::Shutdown => {
                self.stop().await?;
            }
        }

        Ok(())
    }

    /// Record sync completion
    pub fn record_sync(&self, result: &SyncResult) {
        let mut stats = self.stats.write();

        if result.success {
            stats.syncs_completed += 1;
            stats.bytes_uploaded += result.edge_to_cloud_records * 100; // Approximate
            stats.bytes_downloaded += result.cloud_to_edge_records * 100;
            stats.last_sync_at = Some(result.completed_at);
            stats.last_error = None;

            // Return to connected state
            if *self.state.read() == FederationState::Syncing {
                *self.state.write() = FederationState::Connected;
            }
        } else {
            stats.syncs_failed += 1;
            stats.last_error = result.error.clone();
        }
    }

    /// Set node location
    pub fn set_location(&self, location: impl Into<String>) {
        self.node_info.write().location = Some(location.into());
    }

    /// Add metadata
    pub fn set_metadata(&self, key: impl Into<String>, value: impl Into<String>) {
        self.node_info
            .write()
            .metadata
            .insert(key.into(), value.into());
    }

    /// Subscribe to topic
    pub fn subscribe(&self, topic: impl Into<String>) {
        let topic = topic.into();
        let mut node_info = self.node_info.write();
        if !node_info.subscribed_topics.contains(&topic) {
            node_info.subscribed_topics.push(topic);
        }
    }

    /// Unsubscribe from topic
    pub fn unsubscribe(&self, topic: &str) {
        self.node_info
            .write()
            .subscribed_topics
            .retain(|t| t != topic);
    }

    /// Register command handler
    #[allow(dead_code)]
    pub fn on_command<F>(&self, handler: F)
    where
        F: Fn(FederationCommand) -> Result<()> + Send + Sync + 'static,
    {
        self.command_handlers.write().push(Box::new(handler));
    }
}

/// Edge cluster for managing multiple edge nodes (cloud-side)
#[derive(Debug, Default)]
pub struct EdgeCluster {
    nodes: parking_lot::RwLock<HashMap<String, EdgeNodeInfo>>,
}

#[allow(dead_code)]
impl EdgeCluster {
    /// Create a new edge cluster manager
    pub fn new() -> Self {
        Self {
            nodes: parking_lot::RwLock::new(HashMap::new()),
        }
    }

    /// Register an edge node
    pub fn register(&self, node: EdgeNodeInfo) {
        self.nodes.write().insert(node.edge_id.clone(), node);
    }

    /// Unregister an edge node
    pub fn unregister(&self, edge_id: &str) {
        self.nodes.write().remove(edge_id);
    }

    /// Get node info
    pub fn get_node(&self, edge_id: &str) -> Option<EdgeNodeInfo> {
        self.nodes.read().get(edge_id).cloned()
    }

    /// List all nodes
    pub fn list_nodes(&self) -> Vec<EdgeNodeInfo> {
        self.nodes.read().values().cloned().collect()
    }

    /// Get connected nodes
    pub fn connected_nodes(&self) -> Vec<EdgeNodeInfo> {
        self.nodes
            .read()
            .values()
            .filter(|n| {
                matches!(
                    n.state,
                    FederationState::Connected
                        | FederationState::Degraded
                        | FederationState::Syncing
                )
            })
            .cloned()
            .collect()
    }

    /// Get nodes subscribed to a topic
    pub fn nodes_for_topic(&self, topic: &str) -> Vec<EdgeNodeInfo> {
        self.nodes
            .read()
            .values()
            .filter(|n| n.subscribed_topics.contains(&topic.to_string()))
            .cloned()
            .collect()
    }

    /// Update node heartbeat
    pub fn update_heartbeat(&self, edge_id: &str, state: FederationState) {
        if let Some(node) = self.nodes.write().get_mut(edge_id) {
            node.last_heartbeat = chrono::Utc::now().timestamp_millis();
            node.state = state;
        }
    }

    /// Check for stale nodes (no heartbeat in threshold)
    pub fn stale_nodes(&self, threshold_ms: i64) -> Vec<EdgeNodeInfo> {
        let now = chrono::Utc::now().timestamp_millis();
        self.nodes
            .read()
            .values()
            .filter(|n| now - n.last_heartbeat > threshold_ms)
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_federation_manager_creation() {
        let config = EdgeConfig::default();
        let manager = FederationManager::new(config);

        assert_eq!(manager.state(), FederationState::Disconnected);
        assert!(!manager.is_connected());
    }

    #[test]
    fn test_node_info() {
        let config = EdgeConfig::default();
        let manager = FederationManager::new(config.clone());

        let node_info = manager.node_info();
        assert_eq!(node_info.edge_id, config.edge_id);
        assert!(node_info.capabilities.compression_supported);
    }

    #[test]
    fn test_subscribe_unsubscribe() {
        let config = EdgeConfig::default();
        let manager = FederationManager::new(config);

        manager.subscribe("topic1");
        manager.subscribe("topic2");

        let info = manager.node_info();
        assert!(info.subscribed_topics.contains(&"topic1".to_string()));
        assert!(info.subscribed_topics.contains(&"topic2".to_string()));

        manager.unsubscribe("topic1");
        let info = manager.node_info();
        assert!(!info.subscribed_topics.contains(&"topic1".to_string()));
    }

    #[test]
    fn test_metadata() {
        let config = EdgeConfig::default();
        let manager = FederationManager::new(config);

        manager.set_location("us-west-2");
        manager.set_metadata("version", "1.0.0");

        let info = manager.node_info();
        assert_eq!(info.location, Some("us-west-2".to_string()));
        assert_eq!(info.metadata.get("version"), Some(&"1.0.0".to_string()));
    }

    #[test]
    fn test_record_sync() {
        let config = EdgeConfig::default();
        let manager = FederationManager::new(config);

        let result = SyncResult {
            success: true,
            edge_to_cloud_records: 100,
            cloud_to_edge_records: 50,
            conflicts: 5,
            conflict_stats: Default::default(),
            duration_ms: 1000,
            error: None,
            topics: vec!["topic1".to_string()],
            completed_at: chrono::Utc::now().timestamp_millis(),
        };

        manager.record_sync(&result);

        let stats = manager.stats();
        assert_eq!(stats.syncs_completed, 1);
        assert!(stats.last_sync_at.is_some());
    }

    #[test]
    fn test_edge_cluster() {
        let cluster = EdgeCluster::new();

        let node1 = EdgeNodeInfo {
            edge_id: "edge-1".to_string(),
            name: "Edge 1".to_string(),
            location: None,
            capabilities: Default::default(),
            state: FederationState::Connected,
            last_heartbeat: chrono::Utc::now().timestamp_millis(),
            registered_at: chrono::Utc::now().timestamp_millis(),
            subscribed_topics: vec!["topic1".to_string()],
            metadata: HashMap::new(),
        };

        let node2 = EdgeNodeInfo {
            edge_id: "edge-2".to_string(),
            name: "Edge 2".to_string(),
            location: None,
            capabilities: Default::default(),
            state: FederationState::Disconnected,
            last_heartbeat: chrono::Utc::now().timestamp_millis(),
            registered_at: chrono::Utc::now().timestamp_millis(),
            subscribed_topics: vec!["topic1".to_string(), "topic2".to_string()],
            metadata: HashMap::new(),
        };

        cluster.register(node1);
        cluster.register(node2);

        assert_eq!(cluster.list_nodes().len(), 2);
        assert_eq!(cluster.connected_nodes().len(), 1);
        assert_eq!(cluster.nodes_for_topic("topic1").len(), 2);
        assert_eq!(cluster.nodes_for_topic("topic2").len(), 1);
    }
}
