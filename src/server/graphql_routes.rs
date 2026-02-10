//! GraphQL Axum routes
//!
//! Provides HTTP and WebSocket endpoints for the GraphQL API:
//!
//! - `POST /graphql` - Query and mutation endpoint
//! - `GET /graphql` - GraphQL Playground (interactive IDE)
//! - `GET /graphql/ws` - WebSocket transport for subscriptions

use async_graphql::http::{playground_source, GraphQLPlaygroundConfig, WsMessage, ALL_WEBSOCKET_PROTOCOLS, WebSocketProtocols};
use axum::{
    extract::{State, WebSocketUpgrade},
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    routing::get,
    Json, Router,
};
use futures_util::{SinkExt, StreamExt};

use crate::graphql::StreamlineSchema;

/// Shared state for GraphQL routes
#[derive(Clone)]
pub struct GraphQLState {
    pub schema: StreamlineSchema,
}

/// Create the GraphQL router with all endpoints
pub fn create_graphql_router(state: GraphQLState) -> Router {
    Router::new()
        .route(
            "/graphql",
            get(graphql_playground_handler).post(graphql_handler),
        )
        .route("/graphql/ws", get(graphql_ws_handler))
        .with_state(state)
}

/// Handle GraphQL queries and mutations via POST
async fn graphql_handler(
    State(state): State<GraphQLState>,
    Json(request): Json<async_graphql::Request>,
) -> Response {
    let response = state.schema.execute(request).await;
    let body = serde_json::to_string(&response).unwrap_or_default();
    (
        StatusCode::OK,
        [("content-type", "application/json")],
        body,
    )
        .into_response()
}

/// Serve the GraphQL Playground IDE via GET
async fn graphql_playground_handler() -> impl IntoResponse {
    Html(playground_source(
        GraphQLPlaygroundConfig::new("/graphql").subscription_endpoint("/graphql/ws"),
    ))
}

/// Handle WebSocket connections for GraphQL subscriptions.
///
/// Negotiates the graphql-ws or graphql-transport-ws sub-protocol and
/// bridges axum's WebSocket to async-graphql's subscription executor.
async fn graphql_ws_handler(
    State(state): State<GraphQLState>,
    ws: WebSocketUpgrade,
) -> Response {
    // Negotiate the sub-protocol: prefer graphql-transport-ws, fall back to graphql-ws
    let protocol = WebSocketProtocols::GraphQLWS;

    ws.protocols(ALL_WEBSOCKET_PROTOCOLS)
        .on_upgrade(move |socket| async move {
            let (mut sink, stream) = socket.split();

            // Convert axum WS stream into the format async-graphql expects
            let input = stream.filter_map(|msg| async move {
                match msg {
                    Ok(axum::extract::ws::Message::Text(text)) => {
                        Some(text.to_string())
                    }
                    Ok(axum::extract::ws::Message::Close(_)) => None,
                    _ => None,
                }
            });

            let mut gql_stream = Box::pin(
                async_graphql::http::WebSocket::new(state.schema.clone(), input, protocol)
                    .keepalive_timeout(std::time::Duration::from_secs(30)),
            );

            while let Some(ws_msg) = gql_stream.next().await {
                let axum_msg = match ws_msg {
                    WsMessage::Text(text) => axum::extract::ws::Message::Text(text.into()),
                    WsMessage::Close(code, reason) => {
                        axum::extract::ws::Message::Close(Some(axum::extract::ws::CloseFrame {
                            code,
                            reason: reason.into(),
                        }))
                    }
                };
                if sink.send(axum_msg).await.is_err() {
                    break;
                }
            }
        })
}
