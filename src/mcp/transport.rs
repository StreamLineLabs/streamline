//! MCP Transport implementations
//!
//! Supports SSE (Server-Sent Events) and stdio transports
//! for the MCP protocol.

use crate::error::{Result, StreamlineError};
use crate::mcp::{JsonRpcRequest, JsonRpcResponse, McpServer};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

/// Stdio transport for MCP (used by CLI tools like Claude Desktop)
pub struct StdioTransport {
    server: Arc<McpServer>,
}

impl StdioTransport {
    pub fn new(server: Arc<McpServer>) -> Self {
        Self { server }
    }

    /// Run the stdio transport loop, reading JSON-RPC from stdin and writing to stdout
    pub async fn run(&self) -> Result<()> {
        let stdin = tokio::io::stdin();
        let mut stdout = tokio::io::stdout();
        let mut reader = BufReader::new(stdin);
        let mut line = String::new();

        loop {
            line.clear();
            let bytes_read = reader
                .read_line(&mut line)
                .await
                .map_err(StreamlineError::Io)?;

            if bytes_read == 0 {
                break; // EOF
            }

            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }

            match serde_json::from_str::<JsonRpcRequest>(trimmed) {
                Ok(request) => {
                    let response = self.server.handle_request(request).await;
                    let response_json = serde_json::to_string(&response)
                        .map_err(|e| StreamlineError::Internal(e.to_string()))?;

                    stdout
                        .write_all(response_json.as_bytes())
                        .await
                        .map_err(StreamlineError::Io)?;
                    stdout.write_all(b"\n").await.map_err(StreamlineError::Io)?;
                    stdout.flush().await.map_err(StreamlineError::Io)?;
                }
                Err(e) => {
                    let error_response =
                        JsonRpcResponse::error(None, -32700, format!("Parse error: {}", e));
                    let response_json = serde_json::to_string(&error_response)
                        .map_err(|e| StreamlineError::Internal(e.to_string()))?;
                    stdout
                        .write_all(response_json.as_bytes())
                        .await
                        .map_err(StreamlineError::Io)?;
                    stdout.write_all(b"\n").await.map_err(StreamlineError::Io)?;
                    stdout.flush().await.map_err(StreamlineError::Io)?;
                }
            }
        }

        Ok(())
    }
}

/// SSE (Server-Sent Events) message for MCP HTTP transport
#[derive(Debug, Clone)]
pub struct SseMessage {
    pub event: Option<String>,
    pub data: String,
    pub id: Option<String>,
}

impl SseMessage {
    pub fn new(data: String) -> Self {
        Self {
            event: None,
            data,
            id: None,
        }
    }

    pub fn with_event(mut self, event: impl Into<String>) -> Self {
        self.event = Some(event.into());
        self
    }

    pub fn with_id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
        self
    }

    /// Format as SSE wire format
    pub fn to_sse_string(&self) -> String {
        let mut output = String::new();
        if let Some(ref event) = self.event {
            output.push_str(&format!("event: {}\n", event));
        }
        if let Some(ref id) = self.id {
            output.push_str(&format!("id: {}\n", id));
        }
        for line in self.data.lines() {
            output.push_str(&format!("data: {}\n", line));
        }
        output.push('\n');
        output
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sse_message_basic() {
        let msg = SseMessage::new(r#"{"jsonrpc":"2.0","result":{}}"#.to_string());
        let sse = msg.to_sse_string();
        assert!(sse.contains("data: {\"jsonrpc\":\"2.0\",\"result\":{}}"));
        assert!(sse.ends_with("\n\n"));
    }

    #[test]
    fn test_sse_message_with_event() {
        let msg = SseMessage::new("hello".to_string())
            .with_event("message")
            .with_id("42");
        let sse = msg.to_sse_string();
        assert!(sse.contains("event: message\n"));
        assert!(sse.contains("id: 42\n"));
        assert!(sse.contains("data: hello\n"));
    }

    #[test]
    fn test_sse_message_multiline_data() {
        let msg = SseMessage::new("line1\nline2\nline3".to_string());
        let sse = msg.to_sse_string();
        assert!(sse.contains("data: line1\n"));
        assert!(sse.contains("data: line2\n"));
        assert!(sse.contains("data: line3\n"));
    }
}
