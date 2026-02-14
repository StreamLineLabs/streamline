//! Lineage tracker — records connection events for building the lineage graph.

use super::graph::{LineageEdge, LineageGraph, LineageNode, LineageNodeType};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Lineage tracking configuration.
#[derive(Debug, Clone)]
pub struct LineageConfig {
    /// Enable lineage tracking
    pub enabled: bool,
    /// TTL for stale nodes in ms
    pub stale_ttl_ms: i64,
    /// Cleanup interval in seconds
    pub cleanup_interval_secs: u64,
}

impl Default for LineageConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            stale_ttl_ms: 300_000, // 5 minutes
            cleanup_interval_secs: 60,
        }
    }
}

/// Type of connection event.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionEventType {
    ProducerConnect,
    ProducerDisconnect,
    ProducerSend {
        topic: String,
        partition: i32,
        records: u64,
    },
    ConsumerConnect {
        group_id: String,
    },
    ConsumerDisconnect {
        group_id: String,
    },
    ConsumerFetch {
        group_id: String,
        topic: String,
        partition: i32,
        records: u64,
    },
}

/// A connection event for lineage tracking.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionEvent {
    /// Client ID
    pub client_id: String,
    /// Event type
    pub event_type: ConnectionEventType,
    /// Timestamp
    pub timestamp_ms: i64,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
}

/// Tracks connection events and builds lineage graphs.
pub struct LineageTracker {
    config: LineageConfig,
    graph: Arc<RwLock<LineageGraph>>,
    /// Throughput accumulators: (client_id, topic) -> message count since last flush
    accumulators: HashMap<(String, String), u64>,
    #[allow(dead_code)]
    last_flush_ms: i64,
}

impl LineageTracker {
    /// Create a new lineage tracker.
    pub fn new(config: LineageConfig) -> Self {
        Self {
            config,
            graph: Arc::new(RwLock::new(LineageGraph::new())),
            accumulators: HashMap::new(),
            last_flush_ms: chrono::Utc::now().timestamp_millis(),
        }
    }

    /// Record a connection event.
    pub fn record(&mut self, event: ConnectionEvent) {
        if !self.config.enabled {
            return;
        }

        let now = event.timestamp_ms;

        match &event.event_type {
            ConnectionEventType::ProducerConnect => {
                let mut graph = self.graph.write().unwrap_or_else(|e| e.into_inner());
                graph.upsert_node(LineageNode {
                    id: format!("producer:{}", event.client_id),
                    node_type: LineageNodeType::Producer,
                    name: event.client_id.clone(),
                    metadata: event.metadata.clone(),
                    throughput_mps: 0.0,
                    error_rate: 0.0,
                    last_seen_ms: now,
                });
            }
            ConnectionEventType::ProducerSend { topic, records, .. } => {
                let producer_id = format!("producer:{}", event.client_id);
                let topic_id = format!("topic:{}", topic);

                let mut graph = self.graph.write().unwrap_or_else(|e| e.into_inner());

                // Ensure topic node exists
                graph.upsert_node(LineageNode {
                    id: topic_id.clone(),
                    node_type: LineageNodeType::Topic,
                    name: topic.clone(),
                    metadata: HashMap::new(),
                    throughput_mps: 0.0,
                    error_rate: 0.0,
                    last_seen_ms: now,
                });

                // Update producer node
                if let Some(node) = graph.get_node(&producer_id) {
                    let mut updated = node.clone();
                    updated.last_seen_ms = now;
                    graph.upsert_node(updated);
                }

                // Add/update edge
                graph.add_edge(LineageEdge {
                    from: producer_id,
                    to: topic_id,
                    latency_p50_ms: 0.0,
                    latency_p99_ms: 0.0,
                    throughput_mps: *records as f64,
                    errors: 0,
                    active: true,
                });

                // Accumulate
                let key = (event.client_id.clone(), topic.clone());
                *self.accumulators.entry(key).or_insert(0) += records;
            }
            ConnectionEventType::ConsumerConnect { group_id } => {
                let mut graph = self.graph.write().unwrap_or_else(|e| e.into_inner());
                graph.upsert_node(LineageNode {
                    id: format!("group:{}", group_id),
                    node_type: LineageNodeType::ConsumerGroup,
                    name: group_id.clone(),
                    metadata: event.metadata.clone(),
                    throughput_mps: 0.0,
                    error_rate: 0.0,
                    last_seen_ms: now,
                });
            }
            ConnectionEventType::ConsumerFetch {
                group_id,
                topic,
                records,
                ..
            } => {
                let topic_id = format!("topic:{}", topic);
                let group_node_id = format!("group:{}", group_id);

                let mut graph = self.graph.write().unwrap_or_else(|e| e.into_inner());

                // Ensure consumer group node exists
                if graph.get_node(&group_node_id).is_none() {
                    graph.upsert_node(LineageNode {
                        id: group_node_id.clone(),
                        node_type: LineageNodeType::ConsumerGroup,
                        name: group_id.clone(),
                        metadata: HashMap::new(),
                        throughput_mps: 0.0,
                        error_rate: 0.0,
                        last_seen_ms: now,
                    });
                }

                // Add/update edge from topic to consumer group
                graph.add_edge(LineageEdge {
                    from: topic_id,
                    to: group_node_id,
                    latency_p50_ms: 0.0,
                    latency_p99_ms: 0.0,
                    throughput_mps: *records as f64,
                    errors: 0,
                    active: true,
                });
            }
            ConnectionEventType::ProducerDisconnect
            | ConnectionEventType::ConsumerDisconnect { .. } => {
                // Mark edges as inactive on disconnect
            }
        }
    }

    /// Get a shared reference to the lineage graph.
    pub fn graph(&self) -> Arc<RwLock<LineageGraph>> {
        self.graph.clone()
    }

    /// Get a snapshot of the graph.
    pub fn snapshot(&self) -> LineageGraph {
        let graph = self.graph.read().unwrap_or_else(|e| e.into_inner());
        // Return a copy-like struct by rebuilding
        let mut new_graph = LineageGraph::new();
        for node in graph.nodes() {
            new_graph.upsert_node(node.clone());
        }
        for edge in graph.edges() {
            new_graph.add_edge(edge.clone());
        }
        new_graph
    }

    /// Cleanup stale entries.
    pub fn cleanup(&mut self) {
        let mut graph = self.graph.write().unwrap_or_else(|e| e.into_inner());
        graph.cleanup_stale(self.config.stale_ttl_ms);
    }

    /// Whether tracking is enabled.
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lineage_tracking() {
        let mut tracker = LineageTracker::new(LineageConfig::default());
        let now = chrono::Utc::now().timestamp_millis();

        // Producer connects
        tracker.record(ConnectionEvent {
            client_id: "producer-1".to_string(),
            event_type: ConnectionEventType::ProducerConnect,
            timestamp_ms: now,
            metadata: HashMap::new(),
        });

        // Producer sends to topic
        tracker.record(ConnectionEvent {
            client_id: "producer-1".to_string(),
            event_type: ConnectionEventType::ProducerSend {
                topic: "events".to_string(),
                partition: 0,
                records: 10,
            },
            timestamp_ms: now,
            metadata: HashMap::new(),
        });

        // Consumer fetches from topic
        tracker.record(ConnectionEvent {
            client_id: "consumer-1".to_string(),
            event_type: ConnectionEventType::ConsumerFetch {
                group_id: "group-1".to_string(),
                topic: "events".to_string(),
                partition: 0,
                records: 10,
            },
            timestamp_ms: now,
            metadata: HashMap::new(),
        });

        let graph = tracker.graph.read().unwrap();
        let stats = graph.stats();
        assert_eq!(stats.total_nodes, 3); // producer, topic, consumer group
        assert_eq!(stats.total_edges, 2); // producer->topic, topic->group
    }

    #[test]
    fn test_lineage_disabled() {
        let mut tracker = LineageTracker::new(LineageConfig {
            enabled: false,
            ..Default::default()
        });

        tracker.record(ConnectionEvent {
            client_id: "p1".to_string(),
            event_type: ConnectionEventType::ProducerConnect,
            timestamp_ms: 0,
            metadata: HashMap::new(),
        });

        let graph = tracker.graph.read().unwrap();
        assert_eq!(graph.stats().total_nodes, 0);
    }
}
