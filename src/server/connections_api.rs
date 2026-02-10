//! REST API for connection inspection
//!
//! This module provides endpoints for viewing active Kafka protocol connections.
//!
//! ## Endpoints
//!
//! - `GET /api/v1/connections` - Get active connection information
//! - `GET /api/v1/connections/stats` - Get connection statistics

use crate::server::shutdown::ShutdownCoordinator;
use axum::{extract::State, routing::get, Json, Router};
use serde::Serialize;
use std::sync::Arc;

/// Shared state for connections API
#[derive(Clone)]
pub(crate) struct ConnectionsApiState {
    /// Shutdown coordinator for connection stats
    pub shutdown_coordinator: Arc<ShutdownCoordinator>,
    /// Server start time
    pub start_time: std::time::Instant,
}

/// Response for connection statistics
#[derive(Debug, Serialize)]
pub struct ConnectionsStatsResponse {
    /// Total active connections
    pub active_connections: u64,
    /// In-flight requests
    pub in_flight_requests: u64,
    /// Server uptime in seconds
    pub uptime_seconds: f64,
    /// Whether server is shutting down
    pub is_shutting_down: bool,
    /// Current shutdown phase (if applicable)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shutdown_phase: Option<String>,
}

/// Response for connection list (placeholder for future expansion)
#[derive(Debug, Serialize)]
pub struct ConnectionsResponse {
    /// Connection statistics summary
    pub stats: ConnectionsStatsResponse,
    /// Note about detailed connection tracking
    pub note: String,
}

/// Create the connections API router
pub(crate) fn create_connections_api_router(state: ConnectionsApiState) -> Router {
    Router::new()
        .route("/api/v1/connections", get(get_connections))
        .route("/api/v1/connections/stats", get(get_connection_stats))
        .with_state(state)
}

/// Get active connections information
async fn get_connections(State(state): State<ConnectionsApiState>) -> Json<ConnectionsResponse> {
    let stats = build_stats(&state).await;

    Json(ConnectionsResponse {
        stats,
        note: "Detailed per-connection tracking requires protocol handler modifications. \
               Use stats endpoint for aggregate metrics."
            .to_string(),
    })
}

/// Get connection statistics
async fn get_connection_stats(
    State(state): State<ConnectionsApiState>,
) -> Json<ConnectionsStatsResponse> {
    Json(build_stats(&state).await)
}

async fn build_stats(state: &ConnectionsApiState) -> ConnectionsStatsResponse {
    let shutdown_stats = state.shutdown_coordinator.stats().await;

    ConnectionsStatsResponse {
        active_connections: shutdown_stats.active_connections,
        in_flight_requests: shutdown_stats.in_flight_requests,
        uptime_seconds: state.start_time.elapsed().as_secs_f64(),
        is_shutting_down: shutdown_stats.is_shutting_down,
        shutdown_phase: if shutdown_stats.is_shutting_down {
            Some(shutdown_stats.phase.to_string())
        } else {
            None
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::shutdown::ShutdownConfig;

    fn create_test_state() -> ConnectionsApiState {
        ConnectionsApiState {
            shutdown_coordinator: Arc::new(ShutdownCoordinator::with_config(
                ShutdownConfig::default(),
            )),
            start_time: std::time::Instant::now(),
        }
    }

    #[tokio::test]
    async fn test_get_connection_stats() {
        let state = create_test_state();

        // Simulate some connections
        state.shutdown_coordinator.connection_opened();
        state.shutdown_coordinator.connection_opened();

        let response = get_connection_stats(State(state)).await;
        let stats = response.0;

        assert_eq!(stats.active_connections, 2);
        assert!(!stats.is_shutting_down);
    }

    #[tokio::test]
    async fn test_get_connections() {
        let state = create_test_state();

        let response = get_connections(State(state)).await;
        let data = response.0;

        assert!(!data.note.is_empty());
        assert_eq!(data.stats.active_connections, 0);
    }
}
