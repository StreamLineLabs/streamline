//! Data lineage graph model.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Type of node in the lineage graph.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum LineageNodeType {
    Producer,
    Topic,
    ConsumerGroup,
    Connector,
    External,
}

/// A node in the lineage graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageNode {
    /// Unique node ID
    pub id: String,
    /// Node type
    pub node_type: LineageNodeType,
    /// Display name
    pub name: String,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
    /// Throughput (messages/sec) at this node
    pub throughput_mps: f64,
    /// Error rate (errors/sec)
    pub error_rate: f64,
    /// Last seen timestamp
    pub last_seen_ms: i64,
}

/// An edge in the lineage graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageEdge {
    /// Source node ID
    pub from: String,
    /// Target node ID
    pub to: String,
    /// Latency p50 in ms
    pub latency_p50_ms: f64,
    /// Latency p99 in ms
    pub latency_p99_ms: f64,
    /// Throughput on this edge
    pub throughput_mps: f64,
    /// Error count on this edge
    pub errors: u64,
    /// Whether this edge is currently active
    pub active: bool,
}

/// Statistics about the lineage graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageStats {
    pub total_nodes: usize,
    pub total_edges: usize,
    pub producers: usize,
    pub topics: usize,
    pub consumer_groups: usize,
    pub active_edges: usize,
    pub stale_edges: usize,
}

/// The complete lineage graph.
pub struct LineageGraph {
    nodes: HashMap<String, LineageNode>,
    edges: Vec<LineageEdge>,
}

impl LineageGraph {
    /// Create an empty lineage graph.
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            edges: Vec::new(),
        }
    }

    /// Add or update a node.
    pub fn upsert_node(&mut self, node: LineageNode) {
        self.nodes.insert(node.id.clone(), node);
    }

    /// Add an edge.
    pub fn add_edge(&mut self, edge: LineageEdge) {
        // Update existing edge if it connects the same nodes
        if let Some(existing) = self
            .edges
            .iter_mut()
            .find(|e| e.from == edge.from && e.to == edge.to)
        {
            existing.throughput_mps = edge.throughput_mps;
            existing.latency_p50_ms = edge.latency_p50_ms;
            existing.latency_p99_ms = edge.latency_p99_ms;
            existing.errors = edge.errors;
            existing.active = edge.active;
        } else {
            self.edges.push(edge);
        }
    }

    /// Get a node by ID.
    pub fn get_node(&self, id: &str) -> Option<&LineageNode> {
        self.nodes.get(id)
    }

    /// Get all nodes.
    pub fn nodes(&self) -> impl Iterator<Item = &LineageNode> {
        self.nodes.values()
    }

    /// Get all edges.
    pub fn edges(&self) -> &[LineageEdge] {
        &self.edges
    }

    /// Get upstream nodes (producers) for a given node.
    pub fn upstream(&self, node_id: &str) -> Vec<&LineageNode> {
        self.edges
            .iter()
            .filter(|e| e.to == node_id)
            .filter_map(|e| self.nodes.get(&e.from))
            .collect()
    }

    /// Get downstream nodes (consumers) for a given node.
    pub fn downstream(&self, node_id: &str) -> Vec<&LineageNode> {
        self.edges
            .iter()
            .filter(|e| e.from == node_id)
            .filter_map(|e| self.nodes.get(&e.to))
            .collect()
    }

    /// Find all paths from a source to a destination.
    pub fn find_paths(&self, from: &str, to: &str) -> Vec<Vec<String>> {
        let mut paths = Vec::new();
        let mut current_path = vec![from.to_string()];
        self.dfs_paths(from, to, &mut current_path, &mut paths, 10);
        paths
    }

    fn dfs_paths(
        &self,
        current: &str,
        target: &str,
        path: &mut Vec<String>,
        results: &mut Vec<Vec<String>>,
        max_depth: usize,
    ) {
        if path.len() > max_depth {
            return;
        }
        if current == target && path.len() > 1 {
            results.push(path.clone());
            return;
        }
        for edge in &self.edges {
            if edge.from == current && !path.contains(&edge.to) {
                path.push(edge.to.clone());
                self.dfs_paths(&edge.to, target, path, results, max_depth);
                path.pop();
            }
        }
    }

    /// Get graph statistics.
    pub fn stats(&self) -> LineageStats {
        let now = chrono::Utc::now().timestamp_millis();
        let stale_threshold = 60_000; // 60 seconds

        LineageStats {
            total_nodes: self.nodes.len(),
            total_edges: self.edges.len(),
            producers: self
                .nodes
                .values()
                .filter(|n| n.node_type == LineageNodeType::Producer)
                .count(),
            topics: self
                .nodes
                .values()
                .filter(|n| n.node_type == LineageNodeType::Topic)
                .count(),
            consumer_groups: self
                .nodes
                .values()
                .filter(|n| n.node_type == LineageNodeType::ConsumerGroup)
                .count(),
            active_edges: self.edges.iter().filter(|e| e.active).count(),
            stale_edges: self
                .edges
                .iter()
                .filter(|e| {
                    self.nodes
                        .get(&e.from)
                        .map(|n| now - n.last_seen_ms > stale_threshold)
                        .unwrap_or(true)
                })
                .count(),
        }
    }

    /// Remove stale nodes not seen within the TTL.
    pub fn cleanup_stale(&mut self, ttl_ms: i64) {
        let now = chrono::Utc::now().timestamp_millis();
        let stale_ids: Vec<String> = self
            .nodes
            .iter()
            .filter(|(_, n)| now - n.last_seen_ms > ttl_ms)
            .map(|(id, _)| id.clone())
            .collect();

        for id in &stale_ids {
            self.nodes.remove(id);
        }

        self.edges
            .retain(|e| !stale_ids.contains(&e.from) && !stale_ids.contains(&e.to));
    }

    /// Export the graph as a DOT (Graphviz) string.
    pub fn to_dot(&self) -> String {
        let mut dot = String::from("digraph lineage {\n  rankdir=LR;\n");

        for node in self.nodes.values() {
            let shape = match node.node_type {
                LineageNodeType::Producer => "box",
                LineageNodeType::Topic => "ellipse",
                LineageNodeType::ConsumerGroup => "hexagon",
                LineageNodeType::Connector => "diamond",
                LineageNodeType::External => "note",
            };
            dot.push_str(&format!(
                "  \"{}\" [label=\"{}\\n({:.0} msg/s)\" shape={}];\n",
                node.id, node.name, node.throughput_mps, shape
            ));
        }

        for edge in &self.edges {
            let style = if edge.active { "solid" } else { "dashed" };
            dot.push_str(&format!(
                "  \"{}\" -> \"{}\" [label=\"{:.0} msg/s\\np99={:.1}ms\" style={}];\n",
                edge.from, edge.to, edge.throughput_mps, edge.latency_p99_ms, style
            ));
        }

        dot.push_str("}\n");
        dot
    }
}

impl Default for LineageGraph {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_node(id: &str, node_type: LineageNodeType) -> LineageNode {
        LineageNode {
            id: id.to_string(),
            node_type,
            name: id.to_string(),
            metadata: HashMap::new(),
            throughput_mps: 100.0,
            error_rate: 0.0,
            last_seen_ms: chrono::Utc::now().timestamp_millis(),
        }
    }

    fn make_edge(from: &str, to: &str) -> LineageEdge {
        LineageEdge {
            from: from.to_string(),
            to: to.to_string(),
            latency_p50_ms: 5.0,
            latency_p99_ms: 20.0,
            throughput_mps: 100.0,
            errors: 0,
            active: true,
        }
    }

    #[test]
    fn test_graph_construction() {
        let mut graph = LineageGraph::new();
        graph.upsert_node(make_node("producer-1", LineageNodeType::Producer));
        graph.upsert_node(make_node("events", LineageNodeType::Topic));
        graph.upsert_node(make_node("group-1", LineageNodeType::ConsumerGroup));
        graph.add_edge(make_edge("producer-1", "events"));
        graph.add_edge(make_edge("events", "group-1"));

        let stats = graph.stats();
        assert_eq!(stats.total_nodes, 3);
        assert_eq!(stats.total_edges, 2);
        assert_eq!(stats.producers, 1);
        assert_eq!(stats.topics, 1);
    }

    #[test]
    fn test_upstream_downstream() {
        let mut graph = LineageGraph::new();
        graph.upsert_node(make_node("p1", LineageNodeType::Producer));
        graph.upsert_node(make_node("t1", LineageNodeType::Topic));
        graph.upsert_node(make_node("c1", LineageNodeType::ConsumerGroup));
        graph.add_edge(make_edge("p1", "t1"));
        graph.add_edge(make_edge("t1", "c1"));

        let upstream = graph.upstream("t1");
        assert_eq!(upstream.len(), 1);
        assert_eq!(upstream[0].id, "p1");

        let downstream = graph.downstream("t1");
        assert_eq!(downstream.len(), 1);
        assert_eq!(downstream[0].id, "c1");
    }

    #[test]
    fn test_find_paths() {
        let mut graph = LineageGraph::new();
        graph.upsert_node(make_node("p1", LineageNodeType::Producer));
        graph.upsert_node(make_node("t1", LineageNodeType::Topic));
        graph.upsert_node(make_node("c1", LineageNodeType::ConsumerGroup));
        graph.add_edge(make_edge("p1", "t1"));
        graph.add_edge(make_edge("t1", "c1"));

        let paths = graph.find_paths("p1", "c1");
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0], vec!["p1", "t1", "c1"]);
    }

    #[test]
    fn test_dot_export() {
        let mut graph = LineageGraph::new();
        graph.upsert_node(make_node("p1", LineageNodeType::Producer));
        graph.upsert_node(make_node("t1", LineageNodeType::Topic));
        graph.add_edge(make_edge("p1", "t1"));

        let dot = graph.to_dot();
        assert!(dot.contains("digraph lineage"));
        assert!(dot.contains("p1"));
        assert!(dot.contains("t1"));
    }
}
