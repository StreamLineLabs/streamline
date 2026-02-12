//! Visual Flow Designer API
//!
//! Backend API for Streamline Studio - a visual pipeline designer.
//!
//! # Features
//!
//! - Drag-and-drop pipeline construction
//! - Visual node/edge graph representation
//! - Real-time pipeline validation
//! - Pipeline templates and sharing
//! - Export to DSL code
//!
//! # Architecture
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────────────────┐
//! │                     VISUAL FLOW DESIGNER                               │
//! ├────────────────────────────────────────────────────────────────────────┤
//! │  ┌──────────────┐    ┌──────────────┐    ┌──────────────────────────┐ │
//! │  │   Graph      │    │   Validator  │    │   Code Generator         │ │
//! │  │   Model      │    │              │    │   (DSL, YAML, JSON)      │ │
//! │  └──────┬───────┘    └──────┬───────┘    └──────────┬───────────────┘ │
//! │         │                   │                       │                  │
//! │         └───────────────────┼───────────────────────┘                  │
//! │                             │                                          │
//! │  ┌──────────────────────────┴──────────────────────────────────────┐  │
//! │  │                    Pipeline Runtime                              │  │
//! │  │  ┌────────────┐  ┌────────────┐  ┌────────────────────────────┐ │  │
//! │  │  │ Deploy     │  │ Monitor    │  │ Debug/Trace                │ │  │
//! │  │  │            │  │            │  │                            │ │  │
//! │  │  └────────────┘  └────────────┘  └────────────────────────────┘ │  │
//! │  └─────────────────────────────────────────────────────────────────┘  │
//! └────────────────────────────────────────────────────────────────────────┘
//! ```

use crate::dsl::{Pipeline, PipelineBuilder, WindowType};
use crate::error::{Result, StreamlineError};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

/// Visual Flow Designer Manager
pub struct FlowDesigner {
    flows: Arc<RwLock<HashMap<String, FlowGraph>>>,
    templates: Arc<RwLock<HashMap<String, FlowTemplate>>>,
    node_registry: Arc<NodeRegistry>,
}

impl FlowDesigner {
    /// Create a new flow designer
    pub fn new() -> Self {
        Self {
            flows: Arc::new(RwLock::new(HashMap::new())),
            templates: Arc::new(RwLock::new(HashMap::new())),
            node_registry: Arc::new(NodeRegistry::new()),
        }
    }

    /// Create a new flow
    pub async fn create_flow(&self, name: impl Into<String>) -> Result<FlowGraph> {
        let flow = FlowGraph::new(name);
        let mut flows = self.flows.write().await;
        flows.insert(flow.id.clone(), flow.clone());
        Ok(flow)
    }

    /// Get a flow by ID
    pub async fn get_flow(&self, id: &str) -> Option<FlowGraph> {
        let flows = self.flows.read().await;
        flows.get(id).cloned()
    }

    /// List all flows
    pub async fn list_flows(&self) -> Vec<FlowSummary> {
        let flows = self.flows.read().await;
        flows
            .values()
            .map(|f| FlowSummary {
                id: f.id.clone(),
                name: f.name.clone(),
                node_count: f.nodes.len(),
                edge_count: f.edges.len(),
                status: f.status.clone(),
                created_at: f.created_at,
                updated_at: f.updated_at,
            })
            .collect()
    }

    /// Update a flow
    pub async fn update_flow(&self, flow: FlowGraph) -> Result<FlowGraph> {
        let mut flows = self.flows.write().await;
        if !flows.contains_key(&flow.id) {
            return Err(StreamlineError::Config(format!(
                "Flow not found: {}",
                flow.id
            )));
        }
        flows.insert(flow.id.clone(), flow.clone());
        Ok(flow)
    }

    /// Delete a flow
    pub async fn delete_flow(&self, id: &str) -> Result<()> {
        let mut flows = self.flows.write().await;
        if flows.remove(id).is_none() {
            return Err(StreamlineError::Config(format!("Flow not found: {}", id)));
        }
        Ok(())
    }

    /// Add a node to a flow
    pub async fn add_node(&self, flow_id: &str, node: FlowNode) -> Result<FlowNode> {
        let mut flows = self.flows.write().await;
        let flow = flows
            .get_mut(flow_id)
            .ok_or_else(|| StreamlineError::Config(format!("Flow not found: {}", flow_id)))?;

        // Validate node type
        if !self.node_registry.is_valid_type(&node.node_type) {
            return Err(StreamlineError::Config(format!(
                "Invalid node type: {}",
                node.node_type
            )));
        }

        flow.nodes.push(node.clone());
        flow.updated_at = Utc::now();

        Ok(node)
    }

    /// Remove a node from a flow
    pub async fn remove_node(&self, flow_id: &str, node_id: &str) -> Result<()> {
        let mut flows = self.flows.write().await;
        let flow = flows
            .get_mut(flow_id)
            .ok_or_else(|| StreamlineError::Config(format!("Flow not found: {}", flow_id)))?;

        // Remove node
        flow.nodes.retain(|n| n.id != node_id);
        // Remove connected edges
        flow.edges
            .retain(|e| e.source != node_id && e.target != node_id);
        flow.updated_at = Utc::now();

        Ok(())
    }

    /// Add an edge between nodes
    pub async fn add_edge(&self, flow_id: &str, edge: FlowEdge) -> Result<FlowEdge> {
        let mut flows = self.flows.write().await;
        let flow = flows
            .get_mut(flow_id)
            .ok_or_else(|| StreamlineError::Config(format!("Flow not found: {}", flow_id)))?;

        // Validate nodes exist
        let source_exists = flow.nodes.iter().any(|n| n.id == edge.source);
        let target_exists = flow.nodes.iter().any(|n| n.id == edge.target);

        if !source_exists {
            return Err(StreamlineError::Config(format!(
                "Source node not found: {}",
                edge.source
            )));
        }
        if !target_exists {
            return Err(StreamlineError::Config(format!(
                "Target node not found: {}",
                edge.target
            )));
        }

        flow.edges.push(edge.clone());
        flow.updated_at = Utc::now();

        Ok(edge)
    }

    /// Remove an edge
    pub async fn remove_edge(&self, flow_id: &str, edge_id: &str) -> Result<()> {
        let mut flows = self.flows.write().await;
        let flow = flows
            .get_mut(flow_id)
            .ok_or_else(|| StreamlineError::Config(format!("Flow not found: {}", flow_id)))?;

        flow.edges.retain(|e| e.id != edge_id);
        flow.updated_at = Utc::now();

        Ok(())
    }

    /// Validate a flow
    pub async fn validate_flow(&self, flow_id: &str) -> Result<ValidationResult> {
        let flows = self.flows.read().await;
        let flow = flows
            .get(flow_id)
            .ok_or_else(|| StreamlineError::Config(format!("Flow not found: {}", flow_id)))?;

        let validator = FlowValidator::new(&self.node_registry);
        validator.validate(flow)
    }

    /// Convert flow to DSL pipeline
    pub async fn to_pipeline(&self, flow_id: &str) -> Result<Pipeline> {
        let flows = self.flows.read().await;
        let flow = flows
            .get(flow_id)
            .ok_or_else(|| StreamlineError::Config(format!("Flow not found: {}", flow_id)))?;

        // First validate
        let validation = {
            let validator = FlowValidator::new(&self.node_registry);
            validator.validate(flow)?
        };

        if !validation.is_valid {
            return Err(StreamlineError::Config(format!(
                "Flow validation failed: {:?}",
                validation.errors
            )));
        }

        // Convert to pipeline
        let generator = PipelineGenerator::new();
        generator.generate(flow)
    }

    /// Export flow to various formats
    pub async fn export_flow(&self, flow_id: &str, format: ExportFormat) -> Result<String> {
        let flows = self.flows.read().await;
        let flow = flows
            .get(flow_id)
            .ok_or_else(|| StreamlineError::Config(format!("Flow not found: {}", flow_id)))?;

        let exporter = FlowExporter::new();
        exporter.export(flow, format)
    }

    /// Import flow from various formats
    pub async fn import_flow(&self, data: &str, format: ExportFormat) -> Result<FlowGraph> {
        let importer = FlowImporter::new();
        let flow = importer.import(data, format)?;

        let mut flows = self.flows.write().await;
        flows.insert(flow.id.clone(), flow.clone());

        Ok(flow)
    }

    /// Create a flow from a template
    pub async fn create_from_template(&self, template_id: &str, name: &str) -> Result<FlowGraph> {
        let templates = self.templates.read().await;
        let template = templates.get(template_id).ok_or_else(|| {
            StreamlineError::Config(format!("Template not found: {}", template_id))
        })?;

        let mut flow = template.flow.clone();
        flow.id = Uuid::new_v4().to_string();
        flow.name = name.to_string();
        flow.created_at = Utc::now();
        flow.updated_at = Utc::now();

        let mut flows = self.flows.write().await;
        flows.insert(flow.id.clone(), flow.clone());

        Ok(flow)
    }

    /// Register a template
    pub async fn register_template(&self, template: FlowTemplate) -> Result<()> {
        let mut templates = self.templates.write().await;
        templates.insert(template.id.clone(), template);
        Ok(())
    }

    /// List templates
    pub async fn list_templates(&self) -> Vec<FlowTemplateSummary> {
        let templates = self.templates.read().await;
        templates
            .values()
            .map(|t| FlowTemplateSummary {
                id: t.id.clone(),
                name: t.name.clone(),
                description: t.description.clone(),
                category: t.category.clone(),
            })
            .collect()
    }

    /// Get available node types
    pub fn get_node_types(&self) -> Vec<NodeTypeInfo> {
        self.node_registry.get_all_types()
    }
}

impl Default for FlowDesigner {
    fn default() -> Self {
        Self::new()
    }
}

/// Flow graph representation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowGraph {
    /// Flow ID
    pub id: String,
    /// Flow name
    pub name: String,
    /// Description
    pub description: Option<String>,
    /// Nodes in the flow
    pub nodes: Vec<FlowNode>,
    /// Edges connecting nodes
    pub edges: Vec<FlowEdge>,
    /// Flow status
    pub status: FlowStatus,
    /// Created timestamp
    pub created_at: DateTime<Utc>,
    /// Updated timestamp
    pub updated_at: DateTime<Utc>,
    /// Metadata
    pub metadata: HashMap<String, serde_json::Value>,
}

impl FlowGraph {
    /// Create a new flow graph
    pub fn new(name: impl Into<String>) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4().to_string(),
            name: name.into(),
            description: None,
            nodes: Vec::new(),
            edges: Vec::new(),
            status: FlowStatus::Draft,
            created_at: now,
            updated_at: now,
            metadata: HashMap::new(),
        }
    }

    /// Add a description
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Get entry nodes (nodes with no incoming edges)
    pub fn entry_nodes(&self) -> Vec<&FlowNode> {
        self.nodes
            .iter()
            .filter(|n| !self.edges.iter().any(|e| e.target == n.id))
            .collect()
    }

    /// Get exit nodes (nodes with no outgoing edges)
    pub fn exit_nodes(&self) -> Vec<&FlowNode> {
        self.nodes
            .iter()
            .filter(|n| !self.edges.iter().any(|e| e.source == n.id))
            .collect()
    }

    /// Get connected nodes downstream from a node
    pub fn downstream_nodes(&self, node_id: &str) -> Vec<&FlowNode> {
        let target_ids: Vec<&str> = self
            .edges
            .iter()
            .filter(|e| e.source == node_id)
            .map(|e| e.target.as_str())
            .collect();

        self.nodes
            .iter()
            .filter(|n| target_ids.contains(&n.id.as_str()))
            .collect()
    }

    /// Get connected nodes upstream from a node
    pub fn upstream_nodes(&self, node_id: &str) -> Vec<&FlowNode> {
        let source_ids: Vec<&str> = self
            .edges
            .iter()
            .filter(|e| e.target == node_id)
            .map(|e| e.source.as_str())
            .collect();

        self.nodes
            .iter()
            .filter(|n| source_ids.contains(&n.id.as_str()))
            .collect()
    }
}

/// Flow node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowNode {
    /// Node ID
    pub id: String,
    /// Node type
    pub node_type: String,
    /// Node label (display name)
    pub label: String,
    /// Node configuration
    pub config: HashMap<String, serde_json::Value>,
    /// Position in visual canvas
    pub position: NodePosition,
    /// Input ports
    pub inputs: Vec<Port>,
    /// Output ports
    pub outputs: Vec<Port>,
}

impl FlowNode {
    /// Create a new flow node
    pub fn new(node_type: impl Into<String>, label: impl Into<String>) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            node_type: node_type.into(),
            label: label.into(),
            config: HashMap::new(),
            position: NodePosition { x: 0.0, y: 0.0 },
            inputs: Vec::new(),
            outputs: Vec::new(),
        }
    }

    /// Set node position
    pub fn with_position(mut self, x: f64, y: f64) -> Self {
        self.position = NodePosition { x, y };
        self
    }

    /// Add configuration
    pub fn with_config(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.config.insert(key.into(), value);
        self
    }

    /// Add input port
    pub fn with_input(mut self, port: Port) -> Self {
        self.inputs.push(port);
        self
    }

    /// Add output port
    pub fn with_output(mut self, port: Port) -> Self {
        self.outputs.push(port);
        self
    }
}

/// Node position
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct NodePosition {
    /// X coordinate
    pub x: f64,
    /// Y coordinate
    pub y: f64,
}

/// Port on a node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Port {
    /// Port ID
    pub id: String,
    /// Port name
    pub name: String,
    /// Port type (for validation)
    pub port_type: PortType,
}

impl Port {
    /// Create a new port
    pub fn new(name: impl Into<String>, port_type: PortType) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            name: name.into(),
            port_type,
        }
    }
}

/// Port type
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PortType {
    /// Stream data
    Stream,
    /// Table data
    Table,
    /// Key-value data
    KeyValue,
    /// Any type
    Any,
}

/// Flow edge
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowEdge {
    /// Edge ID
    pub id: String,
    /// Source node ID
    pub source: String,
    /// Source port ID (optional)
    pub source_port: Option<String>,
    /// Target node ID
    pub target: String,
    /// Target port ID (optional)
    pub target_port: Option<String>,
    /// Edge label
    pub label: Option<String>,
}

impl FlowEdge {
    /// Create a new edge
    pub fn new(source: impl Into<String>, target: impl Into<String>) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            source: source.into(),
            source_port: None,
            target: target.into(),
            target_port: None,
            label: None,
        }
    }

    /// Set source port
    pub fn with_source_port(mut self, port: impl Into<String>) -> Self {
        self.source_port = Some(port.into());
        self
    }

    /// Set target port
    pub fn with_target_port(mut self, port: impl Into<String>) -> Self {
        self.target_port = Some(port.into());
        self
    }

    /// Set label
    pub fn with_label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }
}

/// Flow status
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FlowStatus {
    /// Draft (not deployed)
    Draft,
    /// Deployed and running
    Running,
    /// Paused
    Paused,
    /// Stopped
    Stopped,
    /// Error state
    Error,
}

/// Flow summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowSummary {
    /// Flow ID
    pub id: String,
    /// Flow name
    pub name: String,
    /// Number of nodes
    pub node_count: usize,
    /// Number of edges
    pub edge_count: usize,
    /// Flow status
    pub status: FlowStatus,
    /// Created timestamp
    pub created_at: DateTime<Utc>,
    /// Updated timestamp
    pub updated_at: DateTime<Utc>,
}

/// Node registry
pub struct NodeRegistry {
    types: HashMap<String, NodeTypeDefinition>,
}

impl NodeRegistry {
    /// Create a new node registry with built-in types
    pub fn new() -> Self {
        let mut types = HashMap::new();

        // Source nodes
        types.insert(
            "source.topic".to_string(),
            NodeTypeDefinition {
                node_type: "source.topic".to_string(),
                category: NodeCategory::Source,
                name: "Topic Source".to_string(),
                description: "Read from a Streamline topic".to_string(),
                config_schema: vec![
                    ConfigField::new("topic", "Topic name", ConfigFieldType::String, true),
                    ConfigField::new(
                        "from_beginning",
                        "Start from beginning",
                        ConfigFieldType::Boolean,
                        false,
                    ),
                ],
                input_ports: vec![],
                output_ports: vec![PortDefinition::new("out", PortType::Stream)],
            },
        );

        types.insert(
            "source.http".to_string(),
            NodeTypeDefinition {
                node_type: "source.http".to_string(),
                category: NodeCategory::Source,
                name: "HTTP Source".to_string(),
                description: "Poll an HTTP endpoint".to_string(),
                config_schema: vec![
                    ConfigField::new("url", "HTTP URL", ConfigFieldType::String, true),
                    ConfigField::new(
                        "interval_ms",
                        "Poll interval (ms)",
                        ConfigFieldType::Integer,
                        false,
                    ),
                ],
                input_ports: vec![],
                output_ports: vec![PortDefinition::new("out", PortType::Stream)],
            },
        );

        // Transform nodes
        types.insert(
            "transform.filter".to_string(),
            NodeTypeDefinition {
                node_type: "transform.filter".to_string(),
                category: NodeCategory::Transform,
                name: "Filter".to_string(),
                description: "Filter records based on condition".to_string(),
                config_schema: vec![ConfigField::new(
                    "condition",
                    "Filter expression",
                    ConfigFieldType::Expression,
                    true,
                )],
                input_ports: vec![PortDefinition::new("in", PortType::Stream)],
                output_ports: vec![
                    PortDefinition::new("matched", PortType::Stream),
                    PortDefinition::new("unmatched", PortType::Stream),
                ],
            },
        );

        types.insert(
            "transform.map".to_string(),
            NodeTypeDefinition {
                node_type: "transform.map".to_string(),
                category: NodeCategory::Transform,
                name: "Map".to_string(),
                description: "Transform records".to_string(),
                config_schema: vec![ConfigField::new(
                    "expression",
                    "Mapping expression",
                    ConfigFieldType::Expression,
                    true,
                )],
                input_ports: vec![PortDefinition::new("in", PortType::Stream)],
                output_ports: vec![PortDefinition::new("out", PortType::Stream)],
            },
        );

        types.insert(
            "transform.project".to_string(),
            NodeTypeDefinition {
                node_type: "transform.project".to_string(),
                category: NodeCategory::Transform,
                name: "Project".to_string(),
                description: "Select specific fields".to_string(),
                config_schema: vec![ConfigField::new(
                    "fields",
                    "Fields to select",
                    ConfigFieldType::StringArray,
                    true,
                )],
                input_ports: vec![PortDefinition::new("in", PortType::Stream)],
                output_ports: vec![PortDefinition::new("out", PortType::Stream)],
            },
        );

        types.insert(
            "transform.aggregate".to_string(),
            NodeTypeDefinition {
                node_type: "transform.aggregate".to_string(),
                category: NodeCategory::Transform,
                name: "Aggregate".to_string(),
                description: "Aggregate records (count, sum, avg, etc.)".to_string(),
                config_schema: vec![
                    ConfigField::new(
                        "function",
                        "Aggregation function",
                        ConfigFieldType::Select,
                        true,
                    )
                    .with_options(vec!["count", "sum", "avg", "min", "max"]),
                    ConfigField::new(
                        "field",
                        "Field to aggregate",
                        ConfigFieldType::String,
                        false,
                    ),
                    ConfigField::new(
                        "group_by",
                        "Group by fields",
                        ConfigFieldType::StringArray,
                        false,
                    ),
                ],
                input_ports: vec![PortDefinition::new("in", PortType::Stream)],
                output_ports: vec![PortDefinition::new("out", PortType::Stream)],
            },
        );

        types.insert(
            "transform.window".to_string(),
            NodeTypeDefinition {
                node_type: "transform.window".to_string(),
                category: NodeCategory::Transform,
                name: "Window".to_string(),
                description: "Apply windowing to stream".to_string(),
                config_schema: vec![
                    ConfigField::new("type", "Window type", ConfigFieldType::Select, true)
                        .with_options(vec!["tumbling", "sliding", "session"]),
                    ConfigField::new(
                        "duration_ms",
                        "Window duration (ms)",
                        ConfigFieldType::Integer,
                        true,
                    ),
                    ConfigField::new(
                        "slide_ms",
                        "Slide interval (ms)",
                        ConfigFieldType::Integer,
                        false,
                    ),
                ],
                input_ports: vec![PortDefinition::new("in", PortType::Stream)],
                output_ports: vec![PortDefinition::new("out", PortType::Stream)],
            },
        );

        types.insert(
            "transform.join".to_string(),
            NodeTypeDefinition {
                node_type: "transform.join".to_string(),
                category: NodeCategory::Transform,
                name: "Join".to_string(),
                description: "Join two streams".to_string(),
                config_schema: vec![
                    ConfigField::new("join_type", "Join type", ConfigFieldType::Select, true)
                        .with_options(vec!["inner", "left", "right", "full"]),
                    ConfigField::new(
                        "on",
                        "Join key expression",
                        ConfigFieldType::Expression,
                        true,
                    ),
                    ConfigField::new(
                        "window_ms",
                        "Join window (ms)",
                        ConfigFieldType::Integer,
                        false,
                    ),
                ],
                input_ports: vec![
                    PortDefinition::new("left", PortType::Stream),
                    PortDefinition::new("right", PortType::Stream),
                ],
                output_ports: vec![PortDefinition::new("out", PortType::Stream)],
            },
        );

        // Sink nodes
        types.insert(
            "sink.topic".to_string(),
            NodeTypeDefinition {
                node_type: "sink.topic".to_string(),
                category: NodeCategory::Sink,
                name: "Topic Sink".to_string(),
                description: "Write to a Streamline topic".to_string(),
                config_schema: vec![ConfigField::new(
                    "topic",
                    "Topic name",
                    ConfigFieldType::String,
                    true,
                )],
                input_ports: vec![PortDefinition::new("in", PortType::Stream)],
                output_ports: vec![],
            },
        );

        types.insert(
            "sink.http".to_string(),
            NodeTypeDefinition {
                node_type: "sink.http".to_string(),
                category: NodeCategory::Sink,
                name: "HTTP Sink".to_string(),
                description: "POST to an HTTP endpoint".to_string(),
                config_schema: vec![
                    ConfigField::new("url", "HTTP URL", ConfigFieldType::String, true),
                    ConfigField::new("method", "HTTP method", ConfigFieldType::Select, false)
                        .with_options(vec!["POST", "PUT"]),
                ],
                input_ports: vec![PortDefinition::new("in", PortType::Stream)],
                output_ports: vec![],
            },
        );

        Self { types }
    }

    /// Check if a node type is valid
    pub fn is_valid_type(&self, node_type: &str) -> bool {
        self.types.contains_key(node_type)
    }

    /// Get node type definition
    pub fn get_type(&self, node_type: &str) -> Option<&NodeTypeDefinition> {
        self.types.get(node_type)
    }

    /// Get all node types
    pub fn get_all_types(&self) -> Vec<NodeTypeInfo> {
        self.types
            .values()
            .map(|t| NodeTypeInfo {
                node_type: t.node_type.clone(),
                category: t.category.clone(),
                name: t.name.clone(),
                description: t.description.clone(),
            })
            .collect()
    }
}

impl Default for NodeRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Node type definition
#[derive(Debug, Clone)]
pub struct NodeTypeDefinition {
    /// Node type identifier
    pub node_type: String,
    /// Category
    pub category: NodeCategory,
    /// Display name
    pub name: String,
    /// Description
    pub description: String,
    /// Configuration schema
    pub config_schema: Vec<ConfigField>,
    /// Input port definitions
    pub input_ports: Vec<PortDefinition>,
    /// Output port definitions
    pub output_ports: Vec<PortDefinition>,
}

/// Node category
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeCategory {
    /// Source node
    Source,
    /// Transform node
    Transform,
    /// Sink node
    Sink,
    /// Control node (branch, merge)
    Control,
}

/// Configuration field definition
#[derive(Debug, Clone)]
pub struct ConfigField {
    /// Field name
    pub name: String,
    /// Field label
    pub label: String,
    /// Field type
    pub field_type: ConfigFieldType,
    /// Is required
    pub required: bool,
    /// Default value
    pub default_value: Option<serde_json::Value>,
    /// Options (for select type)
    pub options: Option<Vec<String>>,
}

impl ConfigField {
    /// Create a new config field
    pub fn new(name: &str, label: &str, field_type: ConfigFieldType, required: bool) -> Self {
        Self {
            name: name.to_string(),
            label: label.to_string(),
            field_type,
            required,
            default_value: None,
            options: None,
        }
    }

    /// Add options
    pub fn with_options(mut self, options: Vec<&str>) -> Self {
        self.options = Some(options.into_iter().map(String::from).collect());
        self
    }
}

/// Config field type
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigFieldType {
    /// String value
    String,
    /// Integer value
    Integer,
    /// Boolean value
    Boolean,
    /// Expression (code)
    Expression,
    /// Array of strings
    StringArray,
    /// Select from options
    Select,
    /// JSON value
    Json,
}

/// Port definition
#[derive(Debug, Clone)]
pub struct PortDefinition {
    /// Port name
    pub name: String,
    /// Port type
    pub port_type: PortType,
}

impl PortDefinition {
    /// Create a new port definition
    pub fn new(name: &str, port_type: PortType) -> Self {
        Self {
            name: name.to_string(),
            port_type,
        }
    }
}

/// Node type info (for API)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeTypeInfo {
    /// Node type
    pub node_type: String,
    /// Category
    pub category: NodeCategory,
    /// Display name
    pub name: String,
    /// Description
    pub description: String,
}

/// Flow validator
pub struct FlowValidator<'a> {
    registry: &'a NodeRegistry,
}

impl<'a> FlowValidator<'a> {
    /// Create a new validator
    pub fn new(registry: &'a NodeRegistry) -> Self {
        Self { registry }
    }

    /// Validate a flow
    pub fn validate(&self, flow: &FlowGraph) -> Result<ValidationResult> {
        let mut errors = Vec::new();
        let mut warnings = Vec::new();

        // Check for empty flow
        if flow.nodes.is_empty() {
            errors.push(ValidationError::new("empty_flow", "Flow has no nodes"));
        }

        // Validate node types
        for node in &flow.nodes {
            if !self.registry.is_valid_type(&node.node_type) {
                errors.push(ValidationError::new(
                    "invalid_node_type",
                    &format!("Invalid node type: {}", node.node_type),
                ));
            }

            // Validate required config
            if let Some(def) = self.registry.get_type(&node.node_type) {
                for field in &def.config_schema {
                    if field.required && !node.config.contains_key(&field.name) {
                        errors.push(ValidationError::new(
                            "missing_config",
                            &format!(
                                "Node '{}' missing required config: {}",
                                node.label, field.name
                            ),
                        ));
                    }
                }
            }
        }

        // Check for cycles
        if self.has_cycle(flow) {
            errors.push(ValidationError::new(
                "cycle_detected",
                "Flow contains a cycle",
            ));
        }

        // Check for unconnected nodes
        for node in &flow.nodes {
            let has_input = flow.edges.iter().any(|e| e.target == node.id);
            let has_output = flow.edges.iter().any(|e| e.source == node.id);

            if let Some(def) = self.registry.get_type(&node.node_type) {
                if !def.input_ports.is_empty() && !has_input {
                    warnings.push(ValidationWarning::new(
                        "unconnected_input",
                        &format!("Node '{}' has no input connection", node.label),
                    ));
                }
                if !def.output_ports.is_empty() && !has_output {
                    warnings.push(ValidationWarning::new(
                        "unconnected_output",
                        &format!("Node '{}' has no output connection", node.label),
                    ));
                }
            }
        }

        Ok(ValidationResult {
            is_valid: errors.is_empty(),
            errors,
            warnings,
        })
    }

    /// Check for cycles using DFS
    fn has_cycle(&self, flow: &FlowGraph) -> bool {
        let mut visited = HashMap::new();
        let mut rec_stack = HashMap::new();

        for node in &flow.nodes {
            visited.insert(node.id.clone(), false);
            rec_stack.insert(node.id.clone(), false);
        }

        for node in &flow.nodes {
            if self.has_cycle_util(flow, &node.id, &mut visited, &mut rec_stack) {
                return true;
            }
        }

        false
    }

    fn has_cycle_util(
        &self,
        flow: &FlowGraph,
        node_id: &str,
        visited: &mut HashMap<String, bool>,
        rec_stack: &mut HashMap<String, bool>,
    ) -> bool {
        if rec_stack.get(node_id) == Some(&true) {
            return true;
        }
        if visited.get(node_id) == Some(&true) {
            return false;
        }

        visited.insert(node_id.to_string(), true);
        rec_stack.insert(node_id.to_string(), true);

        for edge in &flow.edges {
            if edge.source == node_id && self.has_cycle_util(flow, &edge.target, visited, rec_stack)
            {
                return true;
            }
        }

        rec_stack.insert(node_id.to_string(), false);
        false
    }
}

/// Validation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationResult {
    /// Is the flow valid
    pub is_valid: bool,
    /// Validation errors
    pub errors: Vec<ValidationError>,
    /// Validation warnings
    pub warnings: Vec<ValidationWarning>,
}

/// Validation error
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationError {
    /// Error code
    pub code: String,
    /// Error message
    pub message: String,
}

impl ValidationError {
    fn new(code: &str, message: &str) -> Self {
        Self {
            code: code.to_string(),
            message: message.to_string(),
        }
    }
}

/// Validation warning
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationWarning {
    /// Warning code
    pub code: String,
    /// Warning message
    pub message: String,
}

impl ValidationWarning {
    fn new(code: &str, message: &str) -> Self {
        Self {
            code: code.to_string(),
            message: message.to_string(),
        }
    }
}

/// Pipeline generator
struct PipelineGenerator;

impl PipelineGenerator {
    fn new() -> Self {
        Self
    }

    fn generate(&self, flow: &FlowGraph) -> Result<Pipeline> {
        // Find source nodes
        let sources: Vec<&FlowNode> = flow
            .nodes
            .iter()
            .filter(|n| n.node_type.starts_with("source."))
            .collect();

        if sources.is_empty() {
            return Err(StreamlineError::Config(
                "Flow has no source nodes".to_string(),
            ));
        }

        // For now, support single source
        let source = sources[0];
        let topic = source
            .config
            .get("topic")
            .and_then(|v| v.as_str())
            .ok_or_else(|| StreamlineError::Config("Source missing topic".to_string()))?;

        let mut builder = PipelineBuilder::new(topic);

        // Traverse and add operators
        let mut current = source;
        loop {
            let downstream: Vec<&FlowNode> = flow.downstream_nodes(&current.id);
            if downstream.is_empty() {
                break;
            }

            let next = downstream[0];
            builder = self.add_operator(builder, next)?;
            current = next;
        }

        Ok(builder.build())
    }

    fn add_operator(&self, builder: PipelineBuilder, node: &FlowNode) -> Result<PipelineBuilder> {
        match node.node_type.as_str() {
            "transform.filter" => {
                let _condition = node
                    .config
                    .get("condition")
                    .and_then(|v| v.as_str())
                    .unwrap_or("true");
                // For now, just pass through - real filtering would parse the condition
                Ok(builder)
            }
            "transform.project" => {
                let fields: Vec<String> = node
                    .config
                    .get("fields")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect()
                    })
                    .unwrap_or_default();
                Ok(builder.project(fields))
            }
            "transform.window" => {
                let window_type = node
                    .config
                    .get("type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("tumbling");
                let duration = node
                    .config
                    .get("duration_ms")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(60000);

                let window = match window_type {
                    "tumbling" => WindowType::Tumbling {
                        duration_ms: duration,
                    },
                    "sliding" => {
                        let slide = node
                            .config
                            .get("slide_ms")
                            .and_then(|v| v.as_u64())
                            .unwrap_or(10000);
                        WindowType::Sliding {
                            size_ms: duration,
                            slide_ms: slide,
                        }
                    }
                    _ => WindowType::Tumbling {
                        duration_ms: duration,
                    },
                };

                Ok(builder.window(window))
            }
            "sink.topic" => {
                let topic = node
                    .config
                    .get("topic")
                    .and_then(|v| v.as_str())
                    .unwrap_or("output");
                Ok(builder.to_topic(topic))
            }
            _ => Ok(builder),
        }
    }
}

/// Export format
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExportFormat {
    /// JSON format
    Json,
    /// YAML format
    Yaml,
    /// DSL code
    Dsl,
}

/// Flow exporter
struct FlowExporter;

impl FlowExporter {
    fn new() -> Self {
        Self
    }

    fn export(&self, flow: &FlowGraph, format: ExportFormat) -> Result<String> {
        match format {
            ExportFormat::Json => serde_json::to_string_pretty(flow)
                .map_err(|e| StreamlineError::Config(format!("JSON serialization error: {}", e))),
            ExportFormat::Yaml => serde_yaml::to_string(flow)
                .map_err(|e| StreamlineError::Config(format!("YAML serialization error: {}", e))),
            ExportFormat::Dsl => self.export_dsl(flow),
        }
    }

    fn export_dsl(&self, flow: &FlowGraph) -> Result<String> {
        let mut dsl = String::new();
        dsl.push_str(&format!("-- Flow: {}\n", flow.name));
        if let Some(ref desc) = flow.description {
            dsl.push_str(&format!("-- {}\n", desc));
        }
        dsl.push('\n');

        // Find source
        for node in &flow.nodes {
            if node.node_type.starts_with("source.") {
                if let Some(topic) = node.config.get("topic").and_then(|v| v.as_str()) {
                    dsl.push_str(&format!("FROM STREAM '{}'\n", topic));
                }
            }
        }

        // Add operations
        for node in &flow.nodes {
            match node.node_type.as_str() {
                "transform.filter" => {
                    if let Some(cond) = node.config.get("condition").and_then(|v| v.as_str()) {
                        dsl.push_str(&format!("WHERE {}\n", cond));
                    }
                }
                "transform.project" => {
                    if let Some(fields) = node.config.get("fields").and_then(|v| v.as_array()) {
                        let field_list: Vec<&str> =
                            fields.iter().filter_map(|v| v.as_str()).collect();
                        dsl.push_str(&format!("SELECT {}\n", field_list.join(", ")));
                    }
                }
                "transform.window" => {
                    let duration = node
                        .config
                        .get("duration_ms")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(60000);
                    let window_type = node
                        .config
                        .get("type")
                        .and_then(|v| v.as_str())
                        .unwrap_or("tumbling");
                    dsl.push_str(&format!(
                        "WINDOW {} {} MS\n",
                        window_type.to_uppercase(),
                        duration
                    ));
                }
                "sink.topic" => {
                    if let Some(topic) = node.config.get("topic").and_then(|v| v.as_str()) {
                        dsl.push_str(&format!("EMIT TO '{}'\n", topic));
                    }
                }
                _ => {}
            }
        }

        Ok(dsl)
    }
}

/// Flow importer
struct FlowImporter;

impl FlowImporter {
    fn new() -> Self {
        Self
    }

    fn import(&self, data: &str, format: ExportFormat) -> Result<FlowGraph> {
        match format {
            ExportFormat::Json => serde_json::from_str(data)
                .map_err(|e| StreamlineError::Config(format!("JSON parse error: {}", e))),
            ExportFormat::Yaml => serde_yaml::from_str(data)
                .map_err(|e| StreamlineError::Config(format!("YAML parse error: {}", e))),
            ExportFormat::Dsl => Err(StreamlineError::Config(
                "DSL import not yet supported".to_string(),
            )),
        }
    }
}

/// Flow template
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowTemplate {
    /// Template ID
    pub id: String,
    /// Template name
    pub name: String,
    /// Description
    pub description: String,
    /// Category
    pub category: String,
    /// Template flow
    pub flow: FlowGraph,
}

/// Flow template summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowTemplateSummary {
    /// Template ID
    pub id: String,
    /// Template name
    pub name: String,
    /// Description
    pub description: String,
    /// Category
    pub category: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_flow_designer_creation() {
        let designer = FlowDesigner::new();
        let flow = designer.create_flow("test-flow").await.unwrap();
        assert_eq!(flow.name, "test-flow");
        assert_eq!(flow.status, FlowStatus::Draft);
    }

    #[tokio::test]
    async fn test_add_node() {
        let designer = FlowDesigner::new();
        let flow = designer.create_flow("test-flow").await.unwrap();

        let node = FlowNode::new("source.topic", "Source")
            .with_config("topic", serde_json::json!("events"))
            .with_position(100.0, 100.0);

        designer.add_node(&flow.id, node).await.unwrap();

        let updated = designer.get_flow(&flow.id).await.unwrap();
        assert_eq!(updated.nodes.len(), 1);
    }

    #[tokio::test]
    async fn test_flow_validation() {
        let designer = FlowDesigner::new();
        let flow = designer.create_flow("test-flow").await.unwrap();

        // Empty flow should fail
        let result = designer.validate_flow(&flow.id).await.unwrap();
        assert!(!result.is_valid);
    }

    #[test]
    fn test_node_registry() {
        let registry = NodeRegistry::new();
        assert!(registry.is_valid_type("source.topic"));
        assert!(registry.is_valid_type("transform.filter"));
        assert!(registry.is_valid_type("sink.topic"));
        assert!(!registry.is_valid_type("invalid.type"));
    }

    #[test]
    fn test_flow_graph_topology() {
        let mut flow = FlowGraph::new("test");

        let source = FlowNode::new("source.topic", "Source");
        let filter = FlowNode::new("transform.filter", "Filter");
        let sink = FlowNode::new("sink.topic", "Sink");

        let source_id = source.id.clone();
        let filter_id = filter.id.clone();
        let sink_id = sink.id.clone();

        flow.nodes.push(source);
        flow.nodes.push(filter);
        flow.nodes.push(sink);

        flow.edges.push(FlowEdge::new(&source_id, &filter_id));
        flow.edges.push(FlowEdge::new(&filter_id, &sink_id));

        assert_eq!(flow.entry_nodes().len(), 1);
        assert_eq!(flow.exit_nodes().len(), 1);
        assert_eq!(flow.downstream_nodes(&source_id).len(), 1);
    }
}
