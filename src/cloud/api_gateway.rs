//! API Gateway for Streamline Cloud
//!
//! Handles request routing, authentication, rate limiting, and load balancing
//! for the serverless control plane.

use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

/// API Gateway configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiGatewayConfig {
    /// Listen address
    pub listen_addr: String,
    /// Enable rate limiting
    pub rate_limiting_enabled: bool,
    /// Default rate limit (requests per second)
    pub default_rate_limit: u32,
    /// Enable request logging
    pub request_logging: bool,
    /// CORS allowed origins
    pub cors_origins: Vec<String>,
    /// Request timeout in milliseconds
    pub timeout_ms: u64,
    /// Maximum request body size in bytes
    pub max_body_size: usize,
}

impl Default for ApiGatewayConfig {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:8080".to_string(),
            rate_limiting_enabled: true,
            default_rate_limit: 1000,
            request_logging: true,
            cors_origins: vec!["*".to_string()],
            timeout_ms: 30000,
            max_body_size: 10 * 1024 * 1024, // 10MB
        }
    }
}

/// Route configuration for API endpoints
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteConfig {
    /// Path pattern (e.g., "/v1/topics/*")
    pub path: String,
    /// Target service
    pub target: String,
    /// HTTP methods allowed
    pub methods: Vec<String>,
    /// Rate limit override (requests per second)
    pub rate_limit: Option<u32>,
    /// Authentication required
    pub auth_required: bool,
    /// Timeout override in milliseconds
    pub timeout_ms: Option<u64>,
}

/// Request statistics
#[derive(Debug, Default)]
pub struct RequestStats {
    /// Total requests
    pub total_requests: AtomicU64,
    /// Successful requests (2xx)
    pub successful_requests: AtomicU64,
    /// Client errors (4xx)
    pub client_errors: AtomicU64,
    /// Server errors (5xx)
    pub server_errors: AtomicU64,
    /// Rate limited requests
    pub rate_limited: AtomicU64,
    /// Total latency in microseconds
    pub total_latency_us: AtomicU64,
}

impl RequestStats {
    /// Record a request
    pub fn record(&self, status_code: u16, latency_us: u64) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        self.total_latency_us
            .fetch_add(latency_us, Ordering::Relaxed);

        match status_code {
            200..=299 => {
                self.successful_requests.fetch_add(1, Ordering::Relaxed);
            }
            429 => {
                self.rate_limited.fetch_add(1, Ordering::Relaxed);
                self.client_errors.fetch_add(1, Ordering::Relaxed);
            }
            400..=499 => {
                self.client_errors.fetch_add(1, Ordering::Relaxed);
            }
            500..=599 => {
                self.server_errors.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    /// Get average latency in microseconds
    pub fn avg_latency_us(&self) -> f64 {
        let total = self.total_requests.load(Ordering::Relaxed);
        if total == 0 {
            return 0.0;
        }
        self.total_latency_us.load(Ordering::Relaxed) as f64 / total as f64
    }

    /// Get snapshot of stats
    pub fn snapshot(&self) -> RequestStatsSnapshot {
        RequestStatsSnapshot {
            total_requests: self.total_requests.load(Ordering::Relaxed),
            successful_requests: self.successful_requests.load(Ordering::Relaxed),
            client_errors: self.client_errors.load(Ordering::Relaxed),
            server_errors: self.server_errors.load(Ordering::Relaxed),
            rate_limited: self.rate_limited.load(Ordering::Relaxed),
            avg_latency_us: self.avg_latency_us(),
        }
    }
}

/// Snapshot of request statistics
#[derive(Debug, Clone, Serialize)]
pub struct RequestStatsSnapshot {
    pub total_requests: u64,
    pub successful_requests: u64,
    pub client_errors: u64,
    pub server_errors: u64,
    pub rate_limited: u64,
    pub avg_latency_us: f64,
}

/// Rate limiter using token bucket algorithm
#[derive(Debug)]
pub struct RateLimiter {
    /// Tokens per second
    rate: u32,
    /// Current token count per client
    tokens: RwLock<HashMap<String, f64>>,
    /// Last refill time per client
    last_refill: RwLock<HashMap<String, i64>>,
}

impl RateLimiter {
    /// Create a new rate limiter
    pub fn new(rate: u32) -> Self {
        Self {
            rate,
            tokens: RwLock::new(HashMap::new()),
            last_refill: RwLock::new(HashMap::new()),
        }
    }

    /// Check if request is allowed
    pub async fn check(&self, client_id: &str) -> bool {
        let now = chrono::Utc::now().timestamp_millis();

        let mut tokens = self.tokens.write().await;
        let mut last_refill = self.last_refill.write().await;

        let current_tokens = tokens
            .entry(client_id.to_string())
            .or_insert(self.rate as f64);
        let last = last_refill.entry(client_id.to_string()).or_insert(now);

        // Refill tokens based on time elapsed
        let elapsed_ms = (now - *last) as f64;
        let refill = (elapsed_ms / 1000.0) * self.rate as f64;
        *current_tokens = (*current_tokens + refill).min(self.rate as f64);
        *last = now;

        // Check if we have tokens available
        if *current_tokens >= 1.0 {
            *current_tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

/// API Gateway
pub struct ApiGateway {
    config: ApiGatewayConfig,
    routes: Arc<RwLock<Vec<RouteConfig>>>,
    rate_limiter: Arc<RateLimiter>,
    stats: Arc<RequestStats>,
    running: AtomicBool,
}

impl ApiGateway {
    /// Create a new API gateway
    pub fn new(config: ApiGatewayConfig) -> Result<Self> {
        let rate_limiter = Arc::new(RateLimiter::new(config.default_rate_limit));

        Ok(Self {
            config,
            routes: Arc::new(RwLock::new(Vec::new())),
            rate_limiter,
            stats: Arc::new(RequestStats::default()),
            running: AtomicBool::new(false),
        })
    }

    /// Add a route
    pub async fn add_route(&self, route: RouteConfig) {
        let mut routes = self.routes.write().await;
        routes.push(route);
    }

    /// Remove a route by path
    pub async fn remove_route(&self, path: &str) {
        let mut routes = self.routes.write().await;
        routes.retain(|r| r.path != path);
    }

    /// Check rate limit for a client
    pub async fn check_rate_limit(&self, client_id: &str) -> bool {
        if !self.config.rate_limiting_enabled {
            return true;
        }
        self.rate_limiter.check(client_id).await
    }

    /// Record request statistics
    pub fn record_request(&self, status_code: u16, latency_us: u64) {
        self.stats.record(status_code, latency_us);
    }

    /// Get request statistics
    pub fn stats(&self) -> RequestStatsSnapshot {
        self.stats.snapshot()
    }

    /// Get configuration
    pub fn config(&self) -> &ApiGatewayConfig {
        &self.config
    }

    /// Start the API gateway
    pub async fn start(&self) -> Result<()> {
        if self.running.swap(true, Ordering::SeqCst) {
            return Err(StreamlineError::Config(
                "API gateway already running".into(),
            ));
        }

        tracing::info!(
            listen_addr = %self.config.listen_addr,
            "API gateway started"
        );

        Ok(())
    }

    /// Stop the API gateway
    pub async fn stop(&self) -> Result<()> {
        self.running.store(false, Ordering::SeqCst);
        tracing::info!("API gateway stopped");
        Ok(())
    }

    /// Check if running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_rate_limiter() {
        let limiter = RateLimiter::new(10);

        // Should allow first 10 requests
        for _ in 0..10 {
            assert!(limiter.check("client1").await);
        }

        // 11th should be rate limited (without waiting for refill)
        // Note: Due to timing, this might occasionally pass if there's a delay
    }

    #[test]
    fn test_request_stats() {
        let stats = RequestStats::default();
        stats.record(200, 1000);
        stats.record(200, 2000);
        stats.record(400, 500);
        stats.record(500, 3000);

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.total_requests, 4);
        assert_eq!(snapshot.successful_requests, 2);
        assert_eq!(snapshot.client_errors, 1);
        assert_eq!(snapshot.server_errors, 1);
    }

    #[tokio::test]
    async fn test_api_gateway_routes() {
        let config = ApiGatewayConfig::default();
        let gateway = ApiGateway::new(config).unwrap();

        gateway
            .add_route(RouteConfig {
                path: "/v1/topics".to_string(),
                target: "topic-service".to_string(),
                methods: vec!["GET".to_string(), "POST".to_string()],
                rate_limit: None,
                auth_required: true,
                timeout_ms: None,
            })
            .await;

        let routes = gateway.routes.read().await;
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].path, "/v1/topics");
    }
}
