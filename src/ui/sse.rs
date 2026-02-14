//! Server-Sent Events (SSE) for real-time updates.

use super::client::{ClusterOverview, ConsumerGroupInfo, HeatmapData, TopicInfo};
use parking_lot::RwLock;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::broadcast;

/// SSE event types.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", content = "data")]
pub enum SseEvent {
    /// Cluster overview update.
    ClusterUpdate(ClusterOverview),
    /// Topics list update.
    TopicsUpdate(Vec<TopicInfo>),
    /// Consumer groups list update.
    ConsumerGroupsUpdate(Vec<ConsumerGroupInfo>),
    /// Consumer lag heatmap update.
    HeatmapUpdate(HeatmapData),
    /// Metrics update.
    MetricsUpdate(MetricsUpdate),
    /// Alert notification.
    Alert(AlertEvent),
    /// Connection established.
    Connected { connection_id: u64 },
    /// Heartbeat/ping.
    Ping { timestamp: u64 },
}

impl SseEvent {
    /// Get the event name for SSE.
    pub fn event_name(&self) -> &'static str {
        match self {
            SseEvent::ClusterUpdate(_) => "cluster",
            SseEvent::TopicsUpdate(_) => "topics",
            SseEvent::ConsumerGroupsUpdate(_) => "consumer_groups",
            SseEvent::HeatmapUpdate(_) => "heatmap",
            SseEvent::MetricsUpdate(_) => "metrics",
            SseEvent::Alert(_) => "alert",
            SseEvent::Connected { .. } => "connected",
            SseEvent::Ping { .. } => "ping",
        }
    }

    /// Convert to SSE format.
    pub fn to_sse_string(&self) -> String {
        let data = serde_json::to_string(self).unwrap_or_default();
        format!("event: {}\ndata: {}\n\n", self.event_name(), data)
    }
}

/// Metrics update event.
#[derive(Debug, Clone, Serialize)]
pub struct MetricsUpdate {
    pub timestamp: u64,
    pub messages_per_second: f64,
    pub bytes_in_per_second: f64,
    pub bytes_out_per_second: f64,
    pub active_connections: usize,
    pub request_rate: f64,
}

/// Alert event.
#[derive(Debug, Clone, Serialize)]
pub struct AlertEvent {
    pub level: AlertLevel,
    pub title: String,
    pub message: String,
    pub timestamp: u64,
    pub source: Option<String>,
}

/// Alert severity level.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AlertLevel {
    Info,
    Warning,
    Error,
    Critical,
}

/// SSE connection information.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct SseConnection {
    /// Connection ID.
    pub id: u64,
    /// When the connection was established.
    pub connected_at: std::time::Instant,
    /// Topics/events the connection is subscribed to.
    pub subscriptions: Vec<String>,
}

/// Manager for SSE connections.
pub struct SseManager {
    /// Next connection ID.
    next_id: AtomicU64,
    /// Maximum connections allowed.
    max_connections: usize,
    /// Active connections.
    connections: RwLock<HashMap<u64, SseConnection>>,
    /// Broadcast channel for events.
    broadcast_tx: broadcast::Sender<SseEvent>,
    /// Cached last events for new connections.
    last_events: RwLock<LastEvents>,
}

/// Cached last events for catch-up on connect.
#[derive(Debug, Default)]
struct LastEvents {
    cluster: Option<ClusterOverview>,
    topics: Option<Vec<TopicInfo>>,
    consumer_groups: Option<Vec<ConsumerGroupInfo>>,
    heatmap: Option<HeatmapData>,
}

impl SseManager {
    /// Create a new SSE manager.
    pub fn new(max_connections: usize) -> Self {
        let (broadcast_tx, _) = broadcast::channel(1024);

        Self {
            next_id: AtomicU64::new(1),
            max_connections,
            connections: RwLock::new(HashMap::new()),
            broadcast_tx,
            last_events: RwLock::new(LastEvents::default()),
        }
    }

    /// Register a new connection.
    pub fn register(&self) -> Option<(u64, broadcast::Receiver<SseEvent>)> {
        let connections = self.connections.read();
        if connections.len() >= self.max_connections {
            return None;
        }
        drop(connections);

        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let connection = SseConnection {
            id,
            connected_at: std::time::Instant::now(),
            subscriptions: vec![],
        };

        let mut connections = self.connections.write();
        connections.insert(id, connection);

        let receiver = self.broadcast_tx.subscribe();
        Some((id, receiver))
    }

    /// Unregister a connection.
    pub fn unregister(&self, id: u64) {
        let mut connections = self.connections.write();
        connections.remove(&id);
    }

    /// Get the number of active connections.
    pub fn connection_count(&self) -> usize {
        self.connections.read().len()
    }

    /// Broadcast an event to all connections.
    pub fn broadcast(&self, event: SseEvent) {
        // Cache the event
        {
            let mut last = self.last_events.write();
            match &event {
                SseEvent::ClusterUpdate(data) => last.cluster = Some(data.clone()),
                SseEvent::TopicsUpdate(data) => last.topics = Some(data.clone()),
                SseEvent::ConsumerGroupsUpdate(data) => last.consumer_groups = Some(data.clone()),
                SseEvent::HeatmapUpdate(data) => last.heatmap = Some(data.clone()),
                _ => {}
            }
        }

        // Broadcast to all subscribers (ignore errors, receivers may have dropped)
        let _ = self.broadcast_tx.send(event);
    }

    /// Get cached events for a new connection.
    pub fn get_cached_events(&self) -> Vec<SseEvent> {
        let last = self.last_events.read();
        let mut events = vec![];

        if let Some(cluster) = &last.cluster {
            events.push(SseEvent::ClusterUpdate(cluster.clone()));
        }
        if let Some(topics) = &last.topics {
            events.push(SseEvent::TopicsUpdate(topics.clone()));
        }
        if let Some(groups) = &last.consumer_groups {
            events.push(SseEvent::ConsumerGroupsUpdate(groups.clone()));
        }
        if let Some(heatmap) = &last.heatmap {
            events.push(SseEvent::HeatmapUpdate(heatmap.clone()));
        }

        events
    }

    /// Send a heartbeat ping.
    pub fn send_ping(&self) {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let _ = self.broadcast_tx.send(SseEvent::Ping { timestamp });
    }

    /// Send an alert.
    pub fn send_alert(
        &self,
        level: AlertLevel,
        title: impl Into<String>,
        message: impl Into<String>,
    ) {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let event = SseEvent::Alert(AlertEvent {
            level,
            title: title.into(),
            message: message.into(),
            timestamp,
            source: None,
        });

        let _ = self.broadcast_tx.send(event);
    }

    /// Get connection statistics.
    pub fn stats(&self) -> SseStats {
        let connections = self.connections.read();
        let now = std::time::Instant::now();

        let mut total_duration = std::time::Duration::ZERO;
        for conn in connections.values() {
            total_duration += now.duration_since(conn.connected_at);
        }

        let avg_duration = if connections.is_empty() {
            std::time::Duration::ZERO
        } else {
            total_duration / connections.len() as u32
        };

        SseStats {
            active_connections: connections.len(),
            max_connections: self.max_connections,
            avg_connection_duration_secs: avg_duration.as_secs_f64(),
        }
    }
}

impl std::fmt::Debug for SseManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SseManager")
            .field("max_connections", &self.max_connections)
            .field("active_connections", &self.connection_count())
            .finish_non_exhaustive()
    }
}

/// SSE statistics.
#[derive(Debug, Clone, Serialize)]
pub struct SseStats {
    pub active_connections: usize,
    pub max_connections: usize,
    pub avg_connection_duration_secs: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sse_event_names() {
        let event = SseEvent::ClusterUpdate(ClusterOverview::default());
        assert_eq!(event.event_name(), "cluster");

        let event = SseEvent::TopicsUpdate(vec![]);
        assert_eq!(event.event_name(), "topics");

        let event = SseEvent::Ping { timestamp: 0 };
        assert_eq!(event.event_name(), "ping");
    }

    #[test]
    fn test_sse_event_to_string() {
        let event = SseEvent::Ping { timestamp: 12345 };
        let sse_str = event.to_sse_string();
        assert!(sse_str.starts_with("event: ping\n"));
        assert!(sse_str.contains("data: "));
    }

    #[test]
    fn test_sse_manager_register() {
        let manager = SseManager::new(10);

        let result = manager.register();
        assert!(result.is_some());

        let (id, _rx) = result.unwrap();
        assert_eq!(id, 1);
        assert_eq!(manager.connection_count(), 1);
    }

    #[test]
    fn test_sse_manager_max_connections() {
        let manager = SseManager::new(2);

        let _c1 = manager.register();
        let _c2 = manager.register();
        let c3 = manager.register();

        assert!(c3.is_none());
        assert_eq!(manager.connection_count(), 2);
    }

    #[test]
    fn test_sse_manager_unregister() {
        let manager = SseManager::new(10);

        let (id, _rx) = manager.register().unwrap();
        assert_eq!(manager.connection_count(), 1);

        manager.unregister(id);
        assert_eq!(manager.connection_count(), 0);
    }

    #[test]
    fn test_sse_manager_broadcast() {
        let manager = SseManager::new(10);

        // Register a connection
        let (_, mut rx) = manager.register().unwrap();

        // Broadcast an event
        manager.broadcast(SseEvent::Ping { timestamp: 123 });

        // Should receive the event
        let event = rx.try_recv().unwrap();
        matches!(event, SseEvent::Ping { timestamp: 123 });
    }

    #[test]
    fn test_sse_manager_cached_events() {
        let manager = SseManager::new(10);

        // Broadcast some events
        manager.broadcast(SseEvent::ClusterUpdate(ClusterOverview::default()));
        manager.broadcast(SseEvent::TopicsUpdate(vec![]));

        // Get cached events
        let cached = manager.get_cached_events();
        assert_eq!(cached.len(), 2);
    }

    #[test]
    fn test_alert_level() {
        assert_eq!(
            serde_json::to_string(&AlertLevel::Warning).unwrap(),
            "\"warning\""
        );
    }
}
