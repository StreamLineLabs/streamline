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

pub mod resources;
pub mod server;
pub mod tools;
pub mod transport;

use crate::error::{Result, StreamlineError};
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
    topic_manager: Arc<TopicManager>,
    server_info: ServerInfo,
    capabilities: ServerCapabilities,
    sessions: Arc<RwLock<HashMap<String, McpSession>>>,
}

/// Per-connection session state
pub struct McpSession {
    pub id: String,
    pub client_info: Option<ClientInfo>,
    pub subscribed_resources: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientInfo {
    pub name: String,
    pub version: String,
}

impl McpServer {
    pub fn new(topic_manager: Arc<TopicManager>) -> Self {
        Self {
            topic_manager,
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
            },
            sessions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn topic_manager(&self) -> &Arc<TopicManager> {
        &self.topic_manager
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
            _ => Err(StreamlineError::protocol_msg(format!(
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
                    code: -32603,
                    message: e.to_string(),
                    data: None,
                }),
            },
        }
    }

    async fn handle_initialize(&self, params: serde_json::Value) -> Result<serde_json::Value> {
        let client_info: Option<ClientInfo> =
            serde_json::from_value(params.get("clientInfo").cloned().unwrap_or_default()).ok();

        let session_id = uuid::Uuid::new_v4().to_string();
        let session = McpSession {
            id: session_id.clone(),
            client_info,
            subscribed_resources: Vec::new(),
        };
        self.sessions.write().await.insert(session_id, session);

        Ok(serde_json::json!({
            "protocolVersion": "2024-11-05",
            "capabilities": self.capabilities,
            "serverInfo": self.server_info,
        }))
    }

    async fn handle_tools_list(&self) -> Result<serde_json::Value> {
        let tools = tools::get_tool_definitions();
        Ok(serde_json::json!({ "tools": tools }))
    }

    async fn handle_tools_call(&self, params: serde_json::Value) -> Result<serde_json::Value> {
        let name = params
            .get("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| StreamlineError::protocol_msg("Missing tool name".into()))?;
        let arguments = params.get("arguments").cloned().unwrap_or_default();

        let result = tools::execute_tool(name, arguments, &self.topic_manager).await?;
        serde_json::to_value(result).map_err(|e| StreamlineError::Internal(e.to_string()))
    }

    async fn handle_resources_list(&self) -> Result<serde_json::Value> {
        let resources = resources::list_resources(&self.topic_manager).await?;
        Ok(serde_json::json!({ "resources": resources }))
    }

    async fn handle_resources_read(&self, params: serde_json::Value) -> Result<serde_json::Value> {
        let uri = params
            .get("uri")
            .and_then(|v| v.as_str())
            .ok_or_else(|| StreamlineError::protocol_msg("Missing resource URI".into()))?;

        let contents = resources::read_resource(uri, &self.topic_manager).await?;
        Ok(serde_json::json!({ "contents": contents }))
    }

    async fn handle_resource_templates_list(&self) -> Result<serde_json::Value> {
        let templates = resources::list_resource_templates();
        Ok(serde_json::json!({ "resourceTemplates": templates }))
    }

    async fn handle_prompts_list(&self) -> Result<serde_json::Value> {
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

    async fn handle_prompts_get(&self, params: serde_json::Value) -> Result<serde_json::Value> {
        let name = params
            .get("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| StreamlineError::protocol_msg("Missing prompt name".into()))?;
        let arguments = params.get("arguments").cloned().unwrap_or_default();

        match name {
            "stream-summary" => {
                let topic = arguments
                    .get("topic")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        StreamlineError::protocol_msg("Missing topic argument".into())
                    })?;

                let count: usize = arguments
                    .get("count")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(10) as usize;

                let topic_info = self.topic_manager.get_topic_metadata(topic)?;
                let messages = PromptMessage {
                    role: "user".to_string(),
                    content: PromptContent::Text {
                        text: format!(
                            "Summarize the recent activity in topic '{}'. \
                            The topic has {} partition(s). \
                            Please analyze the last {} messages for patterns, anomalies, or notable content.",
                            topic, topic_info.num_partitions, count,
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
                    .ok_or_else(|| {
                        StreamlineError::protocol_msg("Missing group_id argument".into())
                    })?;

                let messages = PromptMessage {
                    role: "user".to_string(),
                    content: PromptContent::Text {
                        text: format!(
                            "Debug consumer group '{}'. Check for lag, stuck consumers, \
                             rebalancing issues, and uneven partition assignment.",
                            group_id
                        ),
                    },
                };

                Ok(serde_json::json!({
                    "description": format!("Debug consumer group '{}'", group_id),
                    "messages": [messages],
                }))
            }
            _ => Err(StreamlineError::protocol_msg(format!(
                "Unknown prompt: {}",
                name
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
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "topic": { "type": "string" }
                },
                "required": ["topic"]
            }),
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
}
