//! Model Context Protocol (MCP) Server for AI Agent Integration
//!
//! This module implements the MCP server protocol, allowing AI agents
//! (Claude, GPT, Copilot) to directly produce/consume/query streams
//! as tool calls. Agents get native streaming memory and context.
//!
//! ## Stability: Experimental
//!
//! ## Features
//!
//! - MCP tool definitions for produce, consume, query, and topic management
//! - Resource exposure for topics and schemas
//! - SSE-based transport for real-time streaming
//! - JSON-RPC 2.0 protocol compliance

pub mod backend;
pub mod resources;
pub mod server;
pub mod tools;
pub mod transport;

pub use backend::{McpBackend, McpError, McpResult, TopicManagerBackend};

use crate::storage::TopicManager;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

// ─── MCP Protocol Types ─────────────────────────────────────────────

/// JSON-RPC 2.0 request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub id: Option<serde_json::Value>,
    pub method: String,
    #[serde(default)]
    pub params: serde_json::Value,
}

/// JSON-RPC 2.0 response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    pub id: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
}

/// JSON-RPC 2.0 error
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcError {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

/// JSON-RPC 2.0 notification (no id)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcNotification {
    pub jsonrpc: String,
    pub method: String,
    #[serde(default)]
    pub params: serde_json::Value,
}

// ─── MCP Server Info ────────────────────────────────────────────────

/// MCP server implementation info
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerInfo {
    pub name: String,
    pub version: String,
}

/// MCP server capabilities
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerCapabilities {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<ToolsCapability>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourcesCapability>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompts: Option<PromptsCapability>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory: Option<MemoryCapability>,
}

/// Memory capability advertised when agent-memory features are enabled.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryCapability {
    pub version: String,
    pub tiers: Vec<String>,
}

/// Policy governing how a client wants memory to behave.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryPolicy {
    /// Client must explicitly opt in to each memory operation.
    OptIn,
    /// Memory operations happen automatically.
    Auto,
    /// Memory is disabled for this session.
    Disabled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolsCapability {
    #[serde(default)]
    pub list_changed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourcesCapability {
    #[serde(default)]
    pub subscribe: bool,
    #[serde(default)]
    pub list_changed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromptsCapability {
    #[serde(default)]
    pub list_changed: bool,
}

// ─── MCP Tool Types ─────────────────────────────────────────────────

/// MCP tool definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolDefinition {
    pub name: String,
    pub description: String,
    #[serde(rename = "inputSchema")]
    pub input_schema: serde_json::Value,
}

/// MCP tool call result content
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ToolResultContent {
    #[serde(rename = "text")]
    Text { text: String },
    #[serde(rename = "image")]
    Image { data: String, mime_type: String },
    #[serde(rename = "resource")]
    Resource { resource: ResourceContent },
}

/// Result of calling a tool
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCallResult {
    pub content: Vec<ToolResultContent>,
    #[serde(default, rename = "isError")]
    pub is_error: bool,
}

// ─── MCP Resource Types ─────────────────────────────────────────────

/// MCP resource definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceDefinition {
    pub uri: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "mimeType")]
    pub mime_type: Option<String>,
}

/// MCP resource content
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceContent {
    pub uri: String,
    #[serde(skip_serializing_if = "Option::is_none", rename = "mimeType")]
    pub mime_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blob: Option<String>,
}

/// MCP resource template
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceTemplate {
    #[serde(rename = "uriTemplate")]
    pub uri_template: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "mimeType")]
    pub mime_type: Option<String>,
}

// ─── MCP Prompt Types ───────────────────────────────────────────────

/// MCP prompt definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromptDefinition {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub arguments: Option<Vec<PromptArgument>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromptArgument {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default)]
    pub required: bool,
}

/// Message returned from a prompt
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromptMessage {
    pub role: String,
    pub content: PromptContent,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum PromptContent {
    #[serde(rename = "text")]
    Text { text: String },
    #[serde(rename = "resource")]
    Resource { resource: ResourceContent },
}

// ─── MCP Server State ───────────────────────────────────────────────

/// Main MCP server state
pub struct McpServer {
    backend: Arc<dyn McpBackend>,
    server_info: ServerInfo,
    capabilities: ServerCapabilities,
    sessions: Arc<RwLock<HashMap<String, McpSession>>>,
}

/// Per-connection session state
pub struct McpSession {
    pub id: String,
    pub client_info: Option<ClientInfo>,
    pub client_capabilities: Option<ClientCapabilities>,
    pub subscribed_resources: Vec<String>,
    pub initialized: bool,
    /// Memory policy requested by the client during initialization.
    pub memory_policy: Option<MemoryPolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientInfo {
    pub name: String,
    pub version: String,
}

/// Client capabilities reported during initialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientCapabilities {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub roots: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sampling: Option<serde_json::Value>,
}

impl McpServer {
    pub fn new(topic_manager: Arc<TopicManager>) -> Self {
        Self::with_backend(Arc::new(TopicManagerBackend::new(topic_manager)))
    }

    pub fn with_backend(backend: Arc<dyn McpBackend>) -> Self {
        Self {
            backend,
            server_info: ServerInfo {
                name: "streamline-mcp".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
            },
            capabilities: ServerCapabilities {
                tools: Some(ToolsCapability {
                    list_changed: false,
                }),
                resources: Some(ResourcesCapability {
                    subscribe: true,
                    list_changed: true,
                }),
                prompts: Some(PromptsCapability {
                    list_changed: false,
                }),
                memory: Some(MemoryCapability {
                    version: "v1".to_string(),
                    tiers: vec![
                        "episodic".to_string(),
                        "semantic".to_string(),
                        "procedural".to_string(),
                    ],
                }),
            },
            sessions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn backend(&self) -> &Arc<dyn McpBackend> {
        &self.backend
    }

    pub fn server_info(&self) -> &ServerInfo {
        &self.server_info
    }

    pub fn capabilities(&self) -> &ServerCapabilities {
        &self.capabilities
    }

    /// Handle an incoming JSON-RPC request
    pub async fn handle_request(&self, request: JsonRpcRequest) -> JsonRpcResponse {
        let result = match request.method.as_str() {
            "initialize" => self.handle_initialize(request.params).await,
            "tools/list" => self.handle_tools_list().await,
            "tools/call" => self.handle_tools_call(request.params).await,
            "resources/list" => self.handle_resources_list().await,
            "resources/read" => self.handle_resources_read(request.params).await,
            "resources/templates/list" => self.handle_resource_templates_list().await,
            "prompts/list" => self.handle_prompts_list().await,
            "prompts/get" => self.handle_prompts_get(request.params).await,
            "ping" => Ok(serde_json::json!({})),
            _ => Err(McpError::method_not_found(format!(
                "Unknown MCP method: {}",
                request.method
            ))),
        };

        match result {
            Ok(value) => JsonRpcResponse {
                jsonrpc: "2.0".to_string(),
                id: request.id,
                result: Some(value),
                error: None,
            },
            Err(e) => JsonRpcResponse {
                jsonrpc: "2.0".to_string(),
                id: request.id,
                result: None,
                error: Some(JsonRpcError {
                    code: e.code.json_rpc_code(),
                    message: e.message,
                    data: e.data,
                }),
            },
        }
    }

    async fn handle_initialize(&self, params: serde_json::Value) -> McpResult<serde_json::Value> {
        let client_info: Option<ClientInfo> =
            serde_json::from_value(params.get("clientInfo").cloned().unwrap_or_default()).ok();
        let client_capabilities: Option<ClientCapabilities> =
            serde_json::from_value(params.get("capabilities").cloned().unwrap_or_default()).ok();

        // Parse optional memory_policy from client init params.
        let memory_policy: Option<MemoryPolicy> = params
            .get("memory_policy")
            .and_then(|v| serde_json::from_value(v.clone()).ok());

        let session_id = uuid::Uuid::new_v4().to_string();
        let session = McpSession {
            id: session_id.clone(),
            client_info,
            client_capabilities,
            subscribed_resources: Vec::new(),
            initialized: true,
            memory_policy,
        };
        self.sessions.write().await.insert(session_id, session);

        Ok(serde_json::json!({
            "protocolVersion": "2024-11-05",
            "capabilities": self.capabilities,
            "serverInfo": self.server_info,
        }))
    }

    async fn handle_tools_list(&self) -> McpResult<serde_json::Value> {
        let tools = tools::get_tool_definitions();
        Ok(serde_json::json!({ "tools": tools }))
    }

    async fn handle_tools_call(&self, params: serde_json::Value) -> McpResult<serde_json::Value> {
        let name = params
            .get("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| McpError::invalid_params("Missing tool name"))?;
        let arguments = params.get("arguments").cloned().unwrap_or_default();

        let result = tools::execute_tool(name, arguments, self.backend.as_ref()).await?;
        serde_json::to_value(result).map_err(|e| McpError::internal(e.to_string()))
    }

    async fn handle_resources_list(&self) -> McpResult<serde_json::Value> {
        let resources = resources::list_resources(self.backend.as_ref()).await?;
        Ok(serde_json::json!({ "resources": resources }))
    }

    async fn handle_resources_read(
        &self,
        params: serde_json::Value,
    ) -> McpResult<serde_json::Value> {
        let uri = params
            .get("uri")
            .and_then(|v| v.as_str())
            .ok_or_else(|| McpError::invalid_params("Missing resource URI"))?;

        let contents = resources::read_resource(uri, self.backend.as_ref()).await?;
        Ok(serde_json::json!({ "contents": contents }))
    }

    async fn handle_resource_templates_list(&self) -> McpResult<serde_json::Value> {
        let templates = resources::list_resource_templates();
        Ok(serde_json::json!({ "resourceTemplates": templates }))
    }

    async fn handle_prompts_list(&self) -> McpResult<serde_json::Value> {
        let prompts = vec![
            PromptDefinition {
                name: "stream-summary".to_string(),
                description: Some("Get a summary of a topic's recent messages".to_string()),
                arguments: Some(vec![
                    PromptArgument {
                        name: "topic".to_string(),
                        description: Some("Topic name to summarize".to_string()),
                        required: true,
                    },
                    PromptArgument {
                        name: "count".to_string(),
                        description: Some("Number of recent messages (default: 10)".to_string()),
                        required: false,
                    },
                ]),
            },
            PromptDefinition {
                name: "debug-consumer-group".to_string(),
                description: Some("Debug a consumer group's lag and assignment".to_string()),
                arguments: Some(vec![PromptArgument {
                    name: "group_id".to_string(),
                    description: Some("Consumer group ID".to_string()),
                    required: true,
                }]),
            },
        ];
        Ok(serde_json::json!({ "prompts": prompts }))
    }

    async fn handle_prompts_get(&self, params: serde_json::Value) -> McpResult<serde_json::Value> {
        let name = params
            .get("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| McpError::invalid_params("Missing prompt name"))?;
        let arguments = params.get("arguments").cloned().unwrap_or_default();

        match name {
            "stream-summary" => {
                let topic = arguments
                    .get("topic")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| McpError::invalid_params("Missing topic argument"))?;

                let count: usize = arguments
                    .get("count")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(10) as usize;

                let topic_detail = self.backend.describe_topic(topic).await?;
                let messages = PromptMessage {
                    role: "user".to_string(),
                    content: PromptContent::Text {
                        text: format!(
                            "Summarize the recent activity in topic '{}'. \
                            The topic has {} partition(s) with {} total messages. \
                            Please analyze the last {} messages for patterns, anomalies, or notable content.",
                            topic, topic_detail.partitions.len(), topic_detail.total_messages, count,
                        ),
                    },
                };

                Ok(serde_json::json!({
                    "description": format!("Summary of topic '{}'", topic),
                    "messages": [messages],
                }))
            }
            "debug-consumer-group" => {
                let group_id = arguments
                    .get("group_id")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| McpError::invalid_params("Missing group_id argument"))?;

                let messages = PromptMessage {
                    role: "user".to_string(),
                    content: PromptContent::Text {
                        text: format!(
                            "Debug consumer group '{group_id}'. Check for lag, stuck consumers, \
                             rebalancing issues, and uneven partition assignment."
                        ),
                    },
                };

                Ok(serde_json::json!({
                    "description": format!("Debug consumer group '{}'", group_id),
                    "messages": [messages],
                }))
            }
            _ => Err(McpError::method_not_found(format!(
                "Unknown prompt: {name}"
            ))),
        }
    }
}

impl JsonRpcResponse {
    pub fn success(id: Option<serde_json::Value>, result: serde_json::Value) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            id,
            result: Some(result),
            error: None,
        }
    }

    pub fn error(id: Option<serde_json::Value>, code: i32, message: String) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            id,
            result: None,
            error: Some(JsonRpcError {
                code,
                message,
                data: None,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mcp::backend::MockMcpBackend;

    #[test]
    fn test_jsonrpc_request_deserialize() {
        let json = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#;
        let req: JsonRpcRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.method, "initialize");
        assert_eq!(req.jsonrpc, "2.0");
    }

    #[test]
    fn test_jsonrpc_response_success() {
        let resp = JsonRpcResponse::success(
            Some(serde_json::json!(1)),
            serde_json::json!({"status": "ok"}),
        );
        assert!(resp.error.is_none());
        assert!(resp.result.is_some());
    }

    #[test]
    fn test_jsonrpc_response_error() {
        let resp =
            JsonRpcResponse::error(Some(serde_json::json!(1)), -32600, "Invalid request".into());
        assert!(resp.result.is_none());
        assert_eq!(resp.error.as_ref().unwrap().code, -32600);
    }

    #[test]
    fn test_server_info() {
        let info = ServerInfo {
            name: "streamline-mcp".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
        };
        let json = serde_json::to_string(&info).unwrap();
        assert!(json.contains("streamline-mcp"));
    }

    #[test]
    fn test_tool_definition_serialize() {
        let tool = ToolDefinition {
            name: "produce".to_string(),
            description: "Produce a message".to_string(),
            input_schema: serde_json::json!({"type": "object", "properties": {"topic": {"type": "string"}}, "required": ["topic"]}),
        };
        let json = serde_json::to_value(&tool).unwrap();
        assert_eq!(json["name"], "produce");
        assert!(json["inputSchema"]["properties"]["topic"].is_object());
    }

    #[test]
    fn test_tool_result_content_text() {
        let content = ToolResultContent::Text {
            text: "Hello".to_string(),
        };
        let json = serde_json::to_value(&content).unwrap();
        assert_eq!(json["type"], "text");
        assert_eq!(json["text"], "Hello");
    }

    #[test]
    fn test_resource_definition_serialize() {
        let resource = ResourceDefinition {
            uri: "streamline://topics/events".to_string(),
            name: "events".to_string(),
            description: Some("Events topic".to_string()),
            mime_type: Some("application/json".to_string()),
        };
        let json = serde_json::to_value(&resource).unwrap();
        assert_eq!(json["uri"], "streamline://topics/events");
    }

    #[test]
    fn test_client_capabilities_deserialize() {
        let caps: ClientCapabilities =
            serde_json::from_str(r#"{"roots":{"listChanged":true},"sampling":{}}"#).unwrap();
        assert!(caps.roots.is_some());
        assert!(caps.sampling.is_some());
        let caps: ClientCapabilities = serde_json::from_str("{}").unwrap();
        assert!(caps.roots.is_none());
    }

    fn create_test_server() -> McpServer {
        let mock = MockMcpBackend::new();
        mock.add_topic("events", 3);
        mock.add_topic("logs", 1);
        McpServer::with_backend(Arc::new(mock))
    }

    #[tokio::test]
    async fn test_handle_initialize() {
        let server = create_test_server();
        let req = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(serde_json::json!(1)),
            method: "initialize".to_string(),
            params: serde_json::json!({"clientInfo": {"name": "test", "version": "1.0"}, "capabilities": {"roots": {"listChanged": true}}}),
        };
        let resp = server.handle_request(req).await;
        assert!(resp.error.is_none());
        let result = resp.result.unwrap();
        assert_eq!(result["protocolVersion"], "2024-11-05");
        let sessions = server.sessions.read().await;
        assert_eq!(sessions.len(), 1);
        let session = sessions.values().next().unwrap();
        assert!(session.initialized);
        assert!(session.client_info.is_some());
        assert!(session.client_capabilities.is_some());
    }

    #[tokio::test]
    async fn test_handle_tools_list() {
        let server = create_test_server();
        let req = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(serde_json::json!(2)),
            method: "tools/list".to_string(),
            params: serde_json::json!({}),
        };
        let resp = server.handle_request(req).await;
        assert!(resp.error.is_none());
        let tools = resp.result.unwrap()["tools"].as_array().unwrap().clone();
        assert!(!tools.is_empty());
        let names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
        assert!(names.contains(&"streamline_produce"));
        assert!(names.contains(&"streamline_get_metrics"));
    }

    #[tokio::test]
    async fn test_handle_tools_call_success() {
        let server = create_test_server();
        let req = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(serde_json::json!(3)),
            method: "tools/call".to_string(),
            params: serde_json::json!({"name": "streamline_list_topics", "arguments": {}}),
        };
        let resp = server.handle_request(req).await;
        assert!(resp.error.is_none());
        let result = resp.result.unwrap();
        assert_eq!(result["isError"], false);
    }

    #[tokio::test]
    async fn test_handle_tools_call_missing_name() {
        let server = create_test_server();
        let req = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(serde_json::json!(4)),
            method: "tools/call".to_string(),
            params: serde_json::json!({}),
        };
        let resp = server.handle_request(req).await;
        assert!(resp.result.is_none());
        assert_eq!(resp.error.as_ref().unwrap().code, -32602);
    }

    #[tokio::test]
    async fn test_handle_unknown_method() {
        let server = create_test_server();
        let req = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(serde_json::json!(5)),
            method: "unknown/method".to_string(),
            params: serde_json::json!({}),
        };
        let resp = server.handle_request(req).await;
        assert!(resp.result.is_none());
        assert_eq!(resp.error.as_ref().unwrap().code, -32601);
    }

    #[tokio::test]
    async fn test_handle_ping() {
        let server = create_test_server();
        let req = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(serde_json::json!(6)),
            method: "ping".to_string(),
            params: serde_json::json!({}),
        };
        let resp = server.handle_request(req).await;
        assert!(resp.error.is_none());
    }

    #[tokio::test]
    async fn test_handle_resources_list() {
        let server = create_test_server();
        let req = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(serde_json::json!(7)),
            method: "resources/list".to_string(),
            params: serde_json::json!({}),
        };
        let resp = server.handle_request(req).await;
        assert!(resp.error.is_none());
        let resources = resp.result.unwrap()["resources"]
            .as_array()
            .unwrap()
            .clone();
        assert!(!resources.is_empty());
    }

    #[tokio::test]
    async fn test_handle_resources_read() {
        let server = create_test_server();
        let req = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(serde_json::json!(8)),
            method: "resources/read".to_string(),
            params: serde_json::json!({"uri": "streamline://topics"}),
        };
        let resp = server.handle_request(req).await;
        assert!(resp.error.is_none());
    }

    #[tokio::test]
    async fn test_handle_resources_read_missing_uri() {
        let server = create_test_server();
        let req = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(serde_json::json!(9)),
            method: "resources/read".to_string(),
            params: serde_json::json!({}),
        };
        let resp = server.handle_request(req).await;
        assert_eq!(resp.error.as_ref().unwrap().code, -32602);
    }

    #[tokio::test]
    async fn test_handle_prompts_get_stream_summary() {
        let server = create_test_server();
        let req = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(serde_json::json!(10)),
            method: "prompts/get".to_string(),
            params: serde_json::json!({"name": "stream-summary", "arguments": {"topic": "events", "count": 5}}),
        };
        let resp = server.handle_request(req).await;
        assert!(resp.error.is_none());
        let result = resp.result.unwrap();
        assert!(result["messages"].is_array());
    }

    #[tokio::test]
    async fn test_handle_prompts_get_unknown() {
        let server = create_test_server();
        let req = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(serde_json::json!(11)),
            method: "prompts/get".to_string(),
            params: serde_json::json!({"name": "nonexistent"}),
        };
        let resp = server.handle_request(req).await;
        assert_eq!(resp.error.as_ref().unwrap().code, -32601);
    }

    #[tokio::test]
    async fn test_error_codes_propagation() {
        let mock = MockMcpBackend::new();
        let server = McpServer::with_backend(Arc::new(mock));
        let req = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(serde_json::json!(1)),
            method: "tools/call".to_string(),
            params: serde_json::json!({"name": "nonexistent_tool", "arguments": {}}),
        };
        let resp = server.handle_request(req).await;
        assert_eq!(resp.error.as_ref().unwrap().code, -32601);
    }
}
