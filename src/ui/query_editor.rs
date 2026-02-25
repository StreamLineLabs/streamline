//! Interactive SQL Query Editor & Cluster Topology View
//!
//! Extends the Web UI with:
//! - **SQL Query Editor**: Execute SQL queries on streaming data via the browser
//! - **Cluster Topology**: Visualise broker health, partition distribution, and
//!   replication status in real time
//! - **Schema Browser**: Inspect topic schemas and field types

use axum::{
    extract::State,
    response::Html,
    Json,
};
use serde::{Deserialize, Serialize};

use super::WebUiState;

// ---------------------------------------------------------------------------
// Query Editor API
// ---------------------------------------------------------------------------

/// SQL query request from the editor.
#[derive(Debug, Deserialize)]
pub struct QueryEditorRequest {
    /// SQL query string.
    pub sql: String,
    /// Maximum rows to return.
    #[serde(default = "default_max_rows")]
    pub max_rows: usize,
    /// Output format.
    #[serde(default)]
    pub format: QueryFormat,
}

fn default_max_rows() -> usize {
    100
}

/// Output format for query results.
#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryFormat {
    #[default]
    Json,
    Csv,
    Table,
}

/// Query result for the editor.
#[derive(Debug, Serialize)]
pub struct QueryEditorResult {
    /// Column names.
    pub columns: Vec<ColumnInfo>,
    /// Rows as JSON arrays.
    pub rows: Vec<Vec<serde_json::Value>>,
    /// Total rows returned.
    pub row_count: usize,
    /// Query execution time in milliseconds.
    pub execution_time_ms: u64,
    /// Whether results were truncated.
    pub truncated: bool,
    /// Error message (None if success).
    pub error: Option<String>,
}

/// Column metadata.
#[derive(Debug, Serialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
}

/// Execute a SQL query from the editor.
pub async fn api_execute_query(
    State(state): State<WebUiState>,
    Json(req): Json<QueryEditorRequest>,
) -> Json<QueryEditorResult> {
    let start = std::time::Instant::now();

    // Proxy to Streamline's analytics API
    match state.client.execute_query(&req.sql, req.max_rows).await {
        Ok(result) => Json(QueryEditorResult {
            columns: result.columns,
            rows: result.rows,
            row_count: result.row_count,
            execution_time_ms: start.elapsed().as_millis() as u64,
            truncated: result.truncated,
            error: None,
        }),
        Err(e) => Json(QueryEditorResult {
            columns: Vec::new(),
            rows: Vec::new(),
            row_count: 0,
            execution_time_ms: start.elapsed().as_millis() as u64,
            truncated: false,
            error: Some(e.to_string()),
        }),
    }
}

/// Get query suggestions / autocomplete data.
pub async fn api_query_suggestions(
    State(state): State<WebUiState>,
) -> Json<QuerySuggestions> {
    let topics = state.client.list_topics().await.unwrap_or_default();

    let topic_names: Vec<String> = topics
        .iter()
        .map(|t| t.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string())
        .filter(|n| !n.is_empty())
        .collect();

    let sql_functions = vec![
        "SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY", "LIMIT",
        "COUNT", "SUM", "AVG", "MIN", "MAX", "DISTINCT",
        "streamline_topic", "HAVING", "JOIN", "LEFT JOIN",
        "AS", "AND", "OR", "NOT", "IN", "LIKE", "BETWEEN",
        "CASE", "WHEN", "THEN", "ELSE", "END",
        "ROW_NUMBER", "RANK", "LAG", "LEAD", "OVER", "PARTITION BY",
    ]
    .into_iter()
    .map(String::from)
    .collect();

    let example_queries = vec![
        "SELECT * FROM streamline_topic('demo-events') LIMIT 10".to_string(),
        "SELECT COUNT(*) as cnt FROM streamline_topic('demo-events')".to_string(),
        "SELECT * FROM streamline_topic('demo-metrics') WHERE timestamp > now() - interval '1 hour'".to_string(),
    ];

    Json(QuerySuggestions {
        topics: topic_names,
        functions: sql_functions,
        examples: example_queries,
    })
}

/// Autocomplete suggestions.
#[derive(Debug, Serialize)]
pub struct QuerySuggestions {
    pub topics: Vec<String>,
    pub functions: Vec<String>,
    pub examples: Vec<String>,
}

// ---------------------------------------------------------------------------
// Cluster Topology API
// ---------------------------------------------------------------------------

/// Cluster topology data for visualization.
#[derive(Debug, Serialize)]
pub struct ClusterTopology {
    /// Broker nodes.
    pub nodes: Vec<TopologyNode>,
    /// Connections between nodes.
    pub edges: Vec<TopologyEdge>,
    /// Cluster health summary.
    pub health: ClusterHealthSummary,
}

/// A node in the topology graph.
#[derive(Debug, Serialize)]
pub struct TopologyNode {
    /// Node ID.
    pub id: String,
    /// Node type.
    pub node_type: NodeType,
    /// Display label.
    pub label: String,
    /// Health status.
    pub status: NodeStatus,
    /// Metadata (e.g., host, port, role).
    pub metadata: serde_json::Value,
}

/// Types of nodes in the topology.
#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeType {
    Broker,
    Topic,
    Partition,
    ConsumerGroup,
}

/// Health status.
#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeStatus {
    Healthy,
    Warning,
    Error,
    Unknown,
}

/// A directed edge between nodes.
#[derive(Debug, Serialize)]
pub struct TopologyEdge {
    pub source: String,
    pub target: String,
    pub edge_type: String,
    pub weight: f64,
}

/// Cluster health summary.
#[derive(Debug, Serialize)]
pub struct ClusterHealthSummary {
    pub total_brokers: u32,
    pub healthy_brokers: u32,
    pub total_topics: u32,
    pub total_partitions: u32,
    pub under_replicated_partitions: u32,
    pub total_consumer_groups: u32,
    pub overall_status: NodeStatus,
}

/// Get cluster topology data for visualization.
pub async fn api_cluster_topology(
    State(state): State<WebUiState>,
) -> Json<ClusterTopology> {
    let mut nodes = Vec::new();
    let mut edges = Vec::new();

    // Broker node (single-node for now)
    nodes.push(TopologyNode {
        id: "broker-0".to_string(),
        node_type: NodeType::Broker,
        label: format!("Streamline @ {}", state.config.streamline_url),
        status: NodeStatus::Healthy,
        metadata: serde_json::json!({
            "url": state.config.streamline_url,
        }),
    });

    // Topics and partitions
    let mut total_topics = 0u32;
    let mut total_partitions = 0u32;

    if let Ok(topics) = state.client.list_topics().await {
        for topic in &topics {
            let name = topic
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            let partitions = topic
                .get("partitions")
                .and_then(|v| v.as_u64())
                .unwrap_or(1) as u32;

            let topic_id = format!("topic-{}", name);
            nodes.push(TopologyNode {
                id: topic_id.clone(),
                node_type: NodeType::Topic,
                label: name.to_string(),
                status: NodeStatus::Healthy,
                metadata: serde_json::json!({
                    "partitions": partitions,
                }),
            });

            edges.push(TopologyEdge {
                source: "broker-0".to_string(),
                target: topic_id.clone(),
                edge_type: "hosts".to_string(),
                weight: partitions as f64,
            });

            total_topics += 1;
            total_partitions += partitions;
        }
    }

    // Consumer groups
    let mut total_groups = 0u32;
    if let Ok(groups) = state.client.list_consumer_groups().await {
        for group in &groups {
            let group_id = group
                .get("group_id")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");

            let node_id = format!("group-{}", group_id);
            nodes.push(TopologyNode {
                id: node_id,
                node_type: NodeType::ConsumerGroup,
                label: group_id.to_string(),
                status: NodeStatus::Healthy,
                metadata: serde_json::json!({}),
            });
            total_groups += 1;
        }
    }

    let health = ClusterHealthSummary {
        total_brokers: 1,
        healthy_brokers: 1,
        total_topics,
        total_partitions,
        under_replicated_partitions: 0,
        total_consumer_groups: total_groups,
        overall_status: NodeStatus::Healthy,
    };

    Json(ClusterTopology {
        nodes,
        edges,
        health,
    })
}

// ---------------------------------------------------------------------------
// Schema Browser API
// ---------------------------------------------------------------------------

/// Topic schema information.
#[derive(Debug, Serialize)]
pub struct TopicSchemaInfo {
    pub topic: String,
    pub fields: Vec<SchemaField>,
    pub sample_message: Option<serde_json::Value>,
    pub message_format: String,
    pub inferred: bool,
}

/// A field in the inferred schema.
#[derive(Debug, Serialize)]
pub struct SchemaField {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub sample_value: Option<serde_json::Value>,
}

/// Infer schema from a topic's messages.
pub async fn api_topic_schema(
    State(state): State<WebUiState>,
    axum::extract::Path(topic_name): axum::extract::Path<String>,
) -> Json<TopicSchemaInfo> {
    // Try to fetch recent messages and infer schema
    match state
        .client
        .browse_messages(&topic_name, 0, 0, 5)
        .await
    {
        Ok(messages) => {
            let mut fields = Vec::new();
            let mut sample = None;

            if let Some(first) = messages.first() {
                if let Some(value) = first.get("value").and_then(|v| v.as_str()) {
                    if let Ok(json) = serde_json::from_str::<serde_json::Value>(value) {
                        sample = Some(json.clone());
                        if let Some(obj) = json.as_object() {
                            for (key, val) in obj {
                                fields.push(SchemaField {
                                    name: key.clone(),
                                    data_type: infer_json_type(val),
                                    nullable: val.is_null(),
                                    sample_value: Some(val.clone()),
                                });
                            }
                        }
                    }
                }
            }

            Json(TopicSchemaInfo {
                topic: topic_name,
                fields,
                sample_message: sample,
                message_format: "json".to_string(),
                inferred: true,
            })
        }
        Err(_) => Json(TopicSchemaInfo {
            topic: topic_name,
            fields: Vec::new(),
            sample_message: None,
            message_format: "unknown".to_string(),
            inferred: false,
        }),
    }
}

fn infer_json_type(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "null".to_string(),
        serde_json::Value::Bool(_) => "boolean".to_string(),
        serde_json::Value::Number(n) => {
            if n.is_i64() {
                "integer".to_string()
            } else {
                "float".to_string()
            }
        }
        serde_json::Value::String(_) => "string".to_string(),
        serde_json::Value::Array(_) => "array".to_string(),
        serde_json::Value::Object(_) => "object".to_string(),
    }
}

// ---------------------------------------------------------------------------
// Query Editor Page (HTML)
// ---------------------------------------------------------------------------

/// Render the SQL query editor page.
pub async fn query_editor_page(State(state): State<WebUiState>) -> Html<String> {
    let html = state.templates.render_query_editor();
    Html(html)
}

/// Render the cluster topology page.
pub async fn topology_page(State(state): State<WebUiState>) -> Html<String> {
    let html = state.templates.render_topology();
    Html(html)
}

/// Render the schema browser page.
pub async fn schema_browser_page(State(state): State<WebUiState>) -> Html<String> {
    let html = state.templates.render_schema_browser();
    Html(html)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_infer_json_type() {
        assert_eq!(infer_json_type(&serde_json::json!(42)), "integer");
        assert_eq!(infer_json_type(&serde_json::json!(3.14)), "float");
        assert_eq!(infer_json_type(&serde_json::json!("hello")), "string");
        assert_eq!(infer_json_type(&serde_json::json!(true)), "boolean");
        assert_eq!(infer_json_type(&serde_json::json!(null)), "null");
        assert_eq!(infer_json_type(&serde_json::json!([1, 2])), "array");
        assert_eq!(infer_json_type(&serde_json::json!({"a": 1})), "object");
    }

    #[test]
    fn test_default_max_rows() {
        assert_eq!(default_max_rows(), 100);
    }

    #[test]
    fn test_query_editor_result_serialization() {
        let result = QueryEditorResult {
            columns: vec![ColumnInfo {
                name: "id".to_string(),
                data_type: "integer".to_string(),
            }],
            rows: vec![vec![serde_json::json!(1)]],
            row_count: 1,
            execution_time_ms: 5,
            truncated: false,
            error: None,
        };
        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("\"row_count\":1"));
    }

    #[test]
    fn test_topology_serialization() {
        let topo = ClusterTopology {
            nodes: vec![TopologyNode {
                id: "broker-0".to_string(),
                node_type: NodeType::Broker,
                label: "Main".to_string(),
                status: NodeStatus::Healthy,
                metadata: serde_json::json!({}),
            }],
            edges: Vec::new(),
            health: ClusterHealthSummary {
                total_brokers: 1,
                healthy_brokers: 1,
                total_topics: 0,
                total_partitions: 0,
                under_replicated_partitions: 0,
                total_consumer_groups: 0,
                overall_status: NodeStatus::Healthy,
            },
        };
        let json = serde_json::to_string(&topo).unwrap();
        assert!(json.contains("broker-0"));
    }
}
