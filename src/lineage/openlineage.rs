//! OpenLineage bridge for DataHub/Marquez integration (M4 P3).
//!
//! Exports Streamline lineage events in [OpenLineage](https://openlineage.io)
//! format so they can be consumed by DataHub, Marquez, or any other
//! OpenLineage-compatible catalog.
//!
//! # Architecture
//!
//! ```text
//! LineageTracker ──► OpenLineageExporter ──► POST /api/v1/lineage
//!                          │
//!                          └──► DataHub / Marquez
//! ```
//!
//! Stability tier: **Experimental**.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use super::graph::{LineageEdge, LineageNode};

// ── OpenLineage spec types ────────────────────────────────────────────

/// Top-level OpenLineage event (v2 schema).
///
/// See <https://openlineage.io/spec/2-0-0/OpenLineage.json>.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenLineageEvent {
    /// Event timestamp in ISO 8601 format.
    pub event_time: String,
    /// Producer URI identifying Streamline as the event source.
    pub producer: String,
    /// Schema URL for this event.
    pub schema_url: String,
    /// Event type: START, RUNNING, COMPLETE, ABORT, FAIL.
    pub event_type: OpenLineageEventType,
    /// The run this event belongs to.
    pub run: OpenLineageRun,
    /// The job that generated this event.
    pub job: OpenLineageJob,
    /// Input datasets consumed by the job.
    pub inputs: Vec<OpenLineageDataset>,
    /// Output datasets produced by the job.
    pub outputs: Vec<OpenLineageDataset>,
}

/// OpenLineage event types.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OpenLineageEventType {
    Start,
    Running,
    Complete,
    Abort,
    Fail,
}

/// A unique run instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenLineageRun {
    /// UUID identifying this run.
    pub run_id: String,
    /// Optional facets (key → arbitrary JSON value).
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub facets: HashMap<String, serde_json::Value>,
}

/// A job (logical processing unit).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenLineageJob {
    /// Namespace (e.g. `"streamline"`).
    pub namespace: String,
    /// Job name (e.g. topic + consumer group).
    pub name: String,
    /// Optional facets.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub facets: HashMap<String, serde_json::Value>,
}

/// An input or output dataset.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenLineageDataset {
    /// Namespace (e.g. `"streamline"`).
    pub namespace: String,
    /// Dataset name (e.g. topic name).
    pub name: String,
    /// Optional facets (schema, data quality, etc.).
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub facets: HashMap<String, serde_json::Value>,
}

// ── Conversion helpers ────────────────────────────────────────────────

/// Convert a Streamline lineage edge (with its source/target nodes) into
/// an [`OpenLineageEvent`].
///
/// The edge is modelled as a COMPLETE event whose *input* is the `from`
/// node and whose *output* is the `to` node.  The "job" represents the
/// data-flow link itself (e.g. `producer:orders → topic:orders`).
pub fn to_openlineage(
    edge: &LineageEdge,
    from_node: &LineageNode,
    to_node: &LineageNode,
) -> OpenLineageEvent {
    let event_type = if edge.active {
        OpenLineageEventType::Running
    } else {
        OpenLineageEventType::Complete
    };

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let event_time = format_iso8601(now as i64);

    let mut run_facets = HashMap::new();
    run_facets.insert(
        "streamline_throughput_mps".into(),
        serde_json::json!(edge.throughput_mps),
    );
    run_facets.insert(
        "streamline_latency_p50_ms".into(),
        serde_json::json!(edge.latency_p50_ms),
    );
    run_facets.insert(
        "streamline_latency_p99_ms".into(),
        serde_json::json!(edge.latency_p99_ms),
    );
    run_facets.insert("streamline_errors".into(), serde_json::json!(edge.errors));

    let run_id = format!("{}-{}-{}", edge.from, edge.to, now / 1000);

    OpenLineageEvent {
        event_time,
        producer: "https://github.com/streamlinelabs/streamline".into(),
        schema_url: "https://openlineage.io/spec/2-0-0/OpenLineage.json".into(),
        event_type,
        run: OpenLineageRun {
            run_id,
            facets: run_facets,
        },
        job: OpenLineageJob {
            namespace: "streamline".into(),
            name: format!("{} → {}", from_node.name, to_node.name),
            facets: HashMap::new(),
        },
        inputs: vec![node_to_dataset(from_node)],
        outputs: vec![node_to_dataset(to_node)],
    }
}

/// Convert a [`LineageNode`] to an [`OpenLineageDataset`].
fn node_to_dataset(node: &LineageNode) -> OpenLineageDataset {
    let mut facets = HashMap::new();
    facets.insert(
        "streamline_node_type".into(),
        serde_json::json!(format!("{:?}", node.node_type)),
    );
    if node.throughput_mps > 0.0 {
        facets.insert(
            "streamline_throughput_mps".into(),
            serde_json::json!(node.throughput_mps),
        );
    }
    OpenLineageDataset {
        namespace: "streamline".into(),
        name: node.name.clone(),
        facets,
    }
}

/// Format epoch millis as ISO 8601.
fn format_iso8601(millis: i64) -> String {
    let secs = millis / 1000;
    let nanos = ((millis % 1000) * 1_000_000) as u32;
    let dt = chrono::DateTime::from_timestamp(secs, nanos).unwrap_or(chrono::DateTime::UNIX_EPOCH);
    dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

// ── Exporter ──────────────────────────────────────────────────────────

/// Configuration for the OpenLineage exporter.
#[derive(Debug, Clone)]
pub struct OpenLineageConfig {
    /// Webhook URL to POST events to (e.g.
    /// `http://marquez:5000/api/v1/lineage`).
    pub endpoint_url: String,
    /// Optional API key for authentication.
    pub api_key: Option<String>,
    /// Namespace to use in emitted events.
    pub namespace: String,
}

impl Default for OpenLineageConfig {
    fn default() -> Self {
        Self {
            endpoint_url: String::new(),
            api_key: None,
            namespace: "streamline".into(),
        }
    }
}

impl OpenLineageConfig {
    /// Validate required configuration fields.
    pub fn validate(&self) -> Result<(), String> {
        if self.endpoint_url.is_empty() {
            return Err("endpoint_url must not be empty".into());
        }
        if self.namespace.is_empty() {
            return Err("namespace must not be empty".into());
        }
        Ok(())
    }
}

/// Exports Streamline lineage events to an OpenLineage-compatible
/// endpoint via HTTP POST.
pub struct OpenLineageExporter {
    config: OpenLineageConfig,
}

impl OpenLineageExporter {
    /// Create a new exporter.
    pub fn new(config: OpenLineageConfig) -> Result<Self, String> {
        config.validate()?;
        Ok(Self { config })
    }

    /// Export a single [`OpenLineageEvent`] by POSTing it to the
    /// configured endpoint.
    ///
    /// Current implementation is a stub that serializes the event and
    /// logs the attempt.  When the real HTTP client is wired, this will
    /// `POST` the JSON body with the configured API key.
    pub fn export(&self, event: &OpenLineageEvent) -> Result<(), String> {
        // Validate that the event serializes correctly.
        let _json = serde_json::to_string(event)
            .map_err(|e| format!("failed to serialize OpenLineage event: {e}"))?;

        // Real implementation:
        //   reqwest::blocking::Client::new()
        //     .post(&self.config.endpoint_url)
        //     .header("Content-Type", "application/json")
        //     .header("Authorization", format!("Bearer {}", api_key))
        //     .body(json)
        //     .send()?;

        tracing::debug!(
            endpoint = %self.config.endpoint_url,
            job = %event.job.name,
            event_type = ?event.event_type,
            "OpenLineage event exported (stub)"
        );

        Ok(())
    }

    /// Export all edges from a lineage graph snapshot.
    pub fn export_graph(
        &self,
        nodes: &[&LineageNode],
        edges: &[&LineageEdge],
    ) -> Result<usize, String> {
        let node_map: HashMap<&str, &LineageNode> =
            nodes.iter().map(|n| (n.id.as_str(), *n)).collect();

        let mut exported = 0;
        for edge in edges {
            let from = node_map.get(edge.from.as_str());
            let to = node_map.get(edge.to.as_str());
            if let (Some(from_node), Some(to_node)) = (from, to) {
                let event = to_openlineage(edge, from_node, to_node);
                self.export(&event)?;
                exported += 1;
            }
        }
        Ok(exported)
    }

    /// Return the configured endpoint URL.
    pub fn endpoint_url(&self) -> &str {
        &self.config.endpoint_url
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lineage::graph::{LineageEdge, LineageNode, LineageNodeType};

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

    fn exporter_config() -> OpenLineageConfig {
        OpenLineageConfig {
            endpoint_url: "http://marquez:5000/api/v1/lineage".into(),
            api_key: Some("test-key".into()),
            namespace: "streamline".into(),
        }
    }

    #[test]
    fn to_openlineage_produces_valid_event() {
        let from = make_node("producer:app", LineageNodeType::Producer);
        let to = make_node("topic:orders", LineageNodeType::Topic);
        let edge = make_edge("producer:app", "topic:orders");

        let event = to_openlineage(&edge, &from, &to);

        assert_eq!(
            event.producer,
            "https://github.com/streamlinelabs/streamline"
        );
        assert_eq!(event.event_type, OpenLineageEventType::Running);
        assert_eq!(event.inputs.len(), 1);
        assert_eq!(event.outputs.len(), 1);
        assert_eq!(event.inputs[0].name, "producer:app");
        assert_eq!(event.outputs[0].name, "topic:orders");
        assert_eq!(event.job.namespace, "streamline");
    }

    #[test]
    fn inactive_edge_produces_complete_event() {
        let from = make_node("p1", LineageNodeType::Producer);
        let to = make_node("t1", LineageNodeType::Topic);
        let mut edge = make_edge("p1", "t1");
        edge.active = false;

        let event = to_openlineage(&edge, &from, &to);
        assert_eq!(event.event_type, OpenLineageEventType::Complete);
    }

    #[test]
    fn event_serializes_to_valid_json() {
        let from = make_node("p", LineageNodeType::Producer);
        let to = make_node("t", LineageNodeType::Topic);
        let edge = make_edge("p", "t");
        let event = to_openlineage(&edge, &from, &to);

        let json = serde_json::to_string_pretty(&event).expect("serialize");
        assert!(json.contains("\"eventType\""));
        assert!(json.contains("\"schemaUrl\""));
        assert!(json.contains("openlineage.io"));
    }

    #[test]
    fn event_deserializes_roundtrip() {
        let from = make_node("p", LineageNodeType::Producer);
        let to = make_node("t", LineageNodeType::Topic);
        let edge = make_edge("p", "t");
        let event = to_openlineage(&edge, &from, &to);

        let json = serde_json::to_string(&event).unwrap();
        let back: OpenLineageEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(back.event_type, event.event_type);
        assert_eq!(back.job.name, event.job.name);
    }

    #[test]
    fn config_validation_accepts_valid() {
        assert!(exporter_config().validate().is_ok());
    }

    #[test]
    fn config_rejects_empty_endpoint() {
        let mut cfg = exporter_config();
        cfg.endpoint_url = String::new();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn config_rejects_empty_namespace() {
        let mut cfg = exporter_config();
        cfg.namespace = String::new();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn exporter_creation_succeeds() {
        let exporter = OpenLineageExporter::new(exporter_config());
        assert!(exporter.is_ok());
    }

    #[test]
    fn exporter_endpoint_accessor() {
        let exporter = OpenLineageExporter::new(exporter_config()).unwrap();
        assert_eq!(
            exporter.endpoint_url(),
            "http://marquez:5000/api/v1/lineage"
        );
    }

    #[test]
    fn export_single_event_stub_succeeds() {
        let exporter = OpenLineageExporter::new(exporter_config()).unwrap();
        let from = make_node("p1", LineageNodeType::Producer);
        let to = make_node("t1", LineageNodeType::Topic);
        let edge = make_edge("p1", "t1");
        let event = to_openlineage(&edge, &from, &to);

        assert!(exporter.export(&event).is_ok());
    }

    #[test]
    fn export_graph_counts_edges() {
        let exporter = OpenLineageExporter::new(exporter_config()).unwrap();
        let p = make_node("p1", LineageNodeType::Producer);
        let t = make_node("t1", LineageNodeType::Topic);
        let c = make_node("c1", LineageNodeType::ConsumerGroup);
        let e1 = make_edge("p1", "t1");
        let e2 = make_edge("t1", "c1");

        let nodes: Vec<&LineageNode> = vec![&p, &t, &c];
        let edges: Vec<&LineageEdge> = vec![&e1, &e2];

        let count = exporter.export_graph(&nodes, &edges).unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn export_graph_skips_edges_with_missing_nodes() {
        let exporter = OpenLineageExporter::new(exporter_config()).unwrap();
        let p = make_node("p1", LineageNodeType::Producer);
        let edge = make_edge("p1", "missing-node");

        let nodes: Vec<&LineageNode> = vec![&p];
        let edges: Vec<&LineageEdge> = vec![&edge];

        let count = exporter.export_graph(&nodes, &edges).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn run_facets_contain_throughput_and_latency() {
        let from = make_node("p", LineageNodeType::Producer);
        let to = make_node("t", LineageNodeType::Topic);
        let edge = make_edge("p", "t");
        let event = to_openlineage(&edge, &from, &to);

        assert!(event.run.facets.contains_key("streamline_throughput_mps"));
        assert!(event.run.facets.contains_key("streamline_latency_p50_ms"));
        assert!(event.run.facets.contains_key("streamline_latency_p99_ms"));
        assert!(event.run.facets.contains_key("streamline_errors"));
    }
}
