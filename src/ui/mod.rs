//! # Optional Web UI
//!
//! This module provides an optional web-based dashboard for Streamline.
//! It is feature-gated behind the `web-ui` feature to keep the core binary
//! lightweight and dependency-free when the UI is not needed.
//!
//! ## Features
//!
//! - **Dashboard**: Overview of cluster health, metrics, and activity
//! - **Topics**: Browse, create, and manage topics
//! - **Consumer Groups**: Monitor consumer group health and lag
//! - **Brokers**: View broker status and configuration
//! - **Real-time Updates**: Server-Sent Events (SSE) for live updates
//!
//! ## Usage
//!
//! The web UI is provided as a separate binary (`streamline-ui`) that connects
//! to a running Streamline server via its HTTP API.
//!
//! ```bash
//! # Start the web UI (connects to Streamline at localhost:9094)
//! streamline-ui --streamline-url http://localhost:9094
//!
//! # Custom port for the UI
//! streamline-ui --listen-addr 0.0.0.0:8080
//! ```

mod client;
mod query_editor;
mod routes;
mod sse;
mod templates;

pub use client::{StreamlineClient, StreamlineClientConfig};
pub use routes::create_router;
pub use sse::{SseEvent, SseManager};
pub use templates::Templates;
pub use query_editor::{
    ClusterHealthSummary, ClusterTopology, QueryEditorRequest, QueryEditorResult,
    QuerySuggestions, TopicSchemaInfo, TopologyNode,
};

use axum::Router;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::broadcast;

/// Configuration for the web UI server.
#[derive(Debug, Clone)]
pub struct WebUiConfig {
    /// Address to listen on for the web UI.
    pub listen_addr: SocketAddr,
    /// URL of the Streamline server HTTP API.
    pub streamline_url: String,
    /// Refresh interval for dashboard data in milliseconds.
    pub refresh_interval_ms: u64,
    /// Enable Server-Sent Events for real-time updates.
    pub enable_sse: bool,
    /// Maximum number of SSE connections.
    pub max_sse_connections: usize,
    /// Client request timeout in milliseconds.
    pub client_timeout_ms: u64,
    /// Cache TTL in milliseconds for UI-side caching.
    pub cache_ttl_ms: u64,
}

impl Default for WebUiConfig {
    fn default() -> Self {
        Self {
            listen_addr: SocketAddr::from(([0, 0, 0, 0], 8080)),
            streamline_url: "http://localhost:9094".to_string(),
            refresh_interval_ms: 5000,
            enable_sse: true,
            max_sse_connections: 100,
            client_timeout_ms: 5000, // 5 seconds
            cache_ttl_ms: 2000,      // 2 seconds
        }
    }
}

/// Shared state for the web UI server.
#[derive(Clone)]
pub struct WebUiState {
    /// Configuration.
    pub config: WebUiConfig,
    /// HTTP client for communicating with Streamline.
    pub client: Arc<StreamlineClient>,
    /// SSE manager for real-time updates.
    pub sse_manager: Arc<SseManager>,
    /// Template renderer.
    pub templates: Arc<Templates>,
    /// Shutdown signal.
    shutdown_tx: broadcast::Sender<()>,
}

impl WebUiState {
    /// Create a new web UI state.
    pub fn new(config: WebUiConfig) -> crate::error::Result<Self> {
        let client_config = StreamlineClientConfig {
            base_url: config.streamline_url.clone(),
            timeout_ms: config.client_timeout_ms,
            cache_ttl_ms: config.cache_ttl_ms,
        };
        let client = Arc::new(StreamlineClient::new(client_config)?);
        let sse_manager = Arc::new(SseManager::new(config.max_sse_connections));
        let templates = Arc::new(Templates::new());
        let (shutdown_tx, _) = broadcast::channel(1);

        Ok(Self {
            config,
            client,
            sse_manager,
            templates,
            shutdown_tx,
        })
    }

    /// Get a shutdown receiver.
    pub fn shutdown_rx(&self) -> broadcast::Receiver<()> {
        self.shutdown_tx.subscribe()
    }

    /// Trigger shutdown.
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(());
    }
}

impl std::fmt::Debug for WebUiState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebUiState")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

/// Web UI server.
pub struct WebUiServer {
    state: WebUiState,
}

impl WebUiServer {
    /// Create a new web UI server.
    pub fn new(config: WebUiConfig) -> crate::error::Result<Self> {
        Ok(Self {
            state: WebUiState::new(config)?,
        })
    }

    /// Build the router.
    pub fn router(&self) -> Router {
        create_router(self.state.clone())
    }

    /// Run the web UI server.
    pub async fn run(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let addr = self.state.config.listen_addr;
        let router = self.router();

        // Start SSE update task
        if self.state.config.enable_sse {
            let state = self.state.clone();
            tokio::spawn(async move {
                sse_update_loop(state).await;
            });
        }

        tracing::info!("Web UI listening on http://{}", addr);
        tracing::info!(
            "Connecting to Streamline at {}",
            self.state.config.streamline_url
        );

        let listener = tokio::net::TcpListener::bind(addr).await?;
        axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                let mut rx = self.state.shutdown_rx();
                let _ = rx.recv().await;
            })
            .await?;

        Ok(())
    }

    /// Get the state for testing.
    pub fn state(&self) -> WebUiState {
        self.state.clone()
    }
}

/// Background task that periodically fetches data and broadcasts SSE updates.
async fn sse_update_loop(state: WebUiState) {
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(
        state.config.refresh_interval_ms,
    ));
    let mut shutdown_rx = state.shutdown_rx();

    loop {
        tokio::select! {
            _ = interval.tick() => {
                // Fetch all data in parallel for better performance
                let (overview_result, topics_result, groups_result, heatmap_result) = tokio::join!(
                    state.client.get_cluster_overview(),
                    state.client.list_topics(),
                    state.client.list_consumer_groups(),
                    state.client.get_heatmap(),
                );

                // Broadcast results
                if let Ok(overview) = overview_result {
                    let event = SseEvent::ClusterUpdate(overview);
                    state.sse_manager.broadcast(event);
                }

                if let Ok(topics) = topics_result {
                    let event = SseEvent::TopicsUpdate(topics);
                    state.sse_manager.broadcast(event);
                }

                if let Ok(groups) = groups_result {
                    let event = SseEvent::ConsumerGroupsUpdate(groups);
                    state.sse_manager.broadcast(event);
                }

                if let Ok(heatmap) = heatmap_result {
                    let event = SseEvent::HeatmapUpdate(heatmap);
                    state.sse_manager.broadcast(event);
                }
            }
            _ = shutdown_rx.recv() => {
                tracing::info!("SSE update loop shutting down");
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = WebUiConfig::default();
        assert_eq!(config.listen_addr.port(), 8080);
        assert_eq!(config.streamline_url, "http://localhost:9094");
        assert!(config.enable_sse);
    }

    #[test]
    fn test_state_creation() {
        let config = WebUiConfig::default();
        let state = WebUiState::new(config).unwrap();
        assert!(state.config.enable_sse);
    }

    #[test]
    fn test_state_debug() {
        let config = WebUiConfig::default();
        let state = WebUiState::new(config).unwrap();
        let debug = format!("{:?}", state);
        assert!(debug.contains("WebUiState"));
    }
}
