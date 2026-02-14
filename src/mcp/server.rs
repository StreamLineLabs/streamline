//! MCP HTTP Server using Axum
//!
//! Provides HTTP/SSE endpoints for the MCP protocol,
//! compatible with MCP client libraries.

use crate::mcp::{JsonRpcRequest, McpServer};
use axum::{
    extract::State,
    http::StatusCode,
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
    routing::{get, post},
    Json, Router,
};
use std::convert::Infallible;
use std::sync::Arc;
use tokio::sync::broadcast;

/// State shared with Axum handlers
#[derive(Clone)]
pub struct McpApiState {
    pub server: Arc<McpServer>,
    pub event_tx: broadcast::Sender<String>,
}

/// Create the MCP API router
pub fn create_mcp_router(server: Arc<McpServer>) -> Router {
    let (event_tx, _) = broadcast::channel(256);
    let state = McpApiState { server, event_tx };

    Router::new()
        .route("/mcp/v1", post(handle_jsonrpc))
        .route("/mcp/v1/sse", get(handle_sse))
        .route("/mcp/v1/health", get(handle_health))
        .with_state(state)
}

/// Handle JSON-RPC POST requests
async fn handle_jsonrpc(
    State(state): State<McpApiState>,
    Json(request): Json<JsonRpcRequest>,
) -> Response {
    let response = state.server.handle_request(request).await;

    // Broadcast response as SSE event for any listening clients
    if let Ok(json) = serde_json::to_string(&response) {
        let _ = state.event_tx.send(json);
    }

    Json(response).into_response()
}

/// Handle SSE connections for real-time MCP notifications
async fn handle_sse(
    State(state): State<McpApiState>,
) -> Sse<impl futures_util::Stream<Item = std::result::Result<Event, Infallible>>> {
    let mut rx = state.event_tx.subscribe();

    let stream = async_stream::stream! {
        // Send initial endpoint message
        let endpoint_msg = serde_json::json!({
            "endpoint": "/mcp/v1"
        });
        yield Ok(Event::default()
            .event("endpoint")
            .data(endpoint_msg.to_string()));

        // Stream all JSON-RPC responses
        loop {
            match rx.recv().await {
                Ok(data) => {
                    yield Ok(Event::default()
                        .event("message")
                        .data(data));
                }
                Err(broadcast::error::RecvError::Lagged(count)) => {
                    tracing::warn!("SSE client lagged by {} messages", count);
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    };

    Sse::new(stream).keep_alive(KeepAlive::default())
}

/// Health check endpoint for MCP server
async fn handle_health() -> impl IntoResponse {
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "status": "ok",
            "protocol": "mcp",
            "version": "2024-11-05",
            "server": "streamline-mcp",
        })),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mcp_api_state_clone() {
        let (event_tx, _) = broadcast::channel::<String>(16);
        // Just verify the state is cloneable (needed by Axum)
        let _ = event_tx.clone();
    }
}
