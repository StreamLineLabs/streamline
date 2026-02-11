//! Streamline Web UI - Optional Dashboard Binary
//!
//! This is a separate binary that provides a web-based dashboard for Streamline.
//! It connects to a running Streamline server via its HTTP API and provides
//! real-time monitoring and management capabilities.
//!
//! ## Usage
//!
//! ```bash
//! # Start the web UI (connects to Streamline at localhost:9094)
//! streamline-ui
//!
//! # Connect to a different Streamline server
//! streamline-ui --streamline-url http://streamline.example.com:9094
//!
//! # Listen on a custom port
//! streamline-ui --listen-addr 0.0.0.0:3000
//! ```
//!
//! ## Features
//!
//! - **Dashboard**: Cluster overview with real-time metrics
//! - **Topics**: Browse, create, and manage topics
//! - **Consumer Groups**: Monitor consumer group health and lag
//! - **Brokers**: View broker status and configuration
//! - **Metrics**: View Prometheus metrics
//! - **Real-time Updates**: Server-Sent Events for live data

use clap::Parser;
use std::net::SocketAddr;
use streamline::ui::{WebUiConfig, WebUiServer};

/// Streamline Web UI - Dashboard for monitoring and managing Streamline
#[derive(Parser, Debug)]
#[command(name = "streamline-ui")]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Address to listen on for the web UI
    #[arg(
        long,
        default_value = "0.0.0.0:8080",
        env = "STREAMLINE_UI_LISTEN_ADDR"
    )]
    listen_addr: SocketAddr,

    /// URL of the Streamline server HTTP API
    #[arg(long, default_value = "http://localhost:9094", env = "STREAMLINE_URL")]
    streamline_url: String,

    /// Refresh interval for dashboard data in milliseconds
    #[arg(long, default_value = "5000", env = "STREAMLINE_UI_REFRESH_MS")]
    refresh_interval_ms: u64,

    /// Disable Server-Sent Events for real-time updates
    #[arg(long, env = "STREAMLINE_UI_DISABLE_SSE")]
    disable_sse: bool,

    /// Maximum number of SSE connections
    #[arg(long, default_value = "100", env = "STREAMLINE_UI_MAX_SSE")]
    max_sse_connections: usize,

    /// Client request timeout in milliseconds
    #[arg(long, default_value = "5000", env = "STREAMLINE_UI_TIMEOUT_MS")]
    client_timeout_ms: u64,

    /// Cache TTL in milliseconds for UI-side request caching
    #[arg(long, default_value = "2000", env = "STREAMLINE_UI_CACHE_TTL_MS")]
    cache_ttl_ms: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // Parse arguments
    let args = Args::parse();

    tracing::info!("Starting Streamline Web UI v{}", env!("CARGO_PKG_VERSION"));

    // Build configuration
    let config = WebUiConfig {
        listen_addr: args.listen_addr,
        streamline_url: args.streamline_url,
        refresh_interval_ms: args.refresh_interval_ms,
        enable_sse: !args.disable_sse,
        max_sse_connections: args.max_sse_connections,
        client_timeout_ms: args.client_timeout_ms,
        cache_ttl_ms: args.cache_ttl_ms,
    };

    // Create and run server
    let server = WebUiServer::new(config)?;
    server.run().await?;

    Ok(())
}
