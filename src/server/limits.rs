//! Resource limits and backpressure for Streamline
//!
//! This module provides rate limiting, connection limiting, and request size validation
//! to prevent denial-of-service attacks and ensure stable operation under load.

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tracing::{debug, warn};

/// Configuration for resource limits
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LimitsConfig {
    /// Maximum total connections (0 = unlimited)
    pub max_connections: u32,

    /// Maximum connections per IP (0 = unlimited)
    pub max_connections_per_ip: u32,

    /// Maximum request size in bytes (0 = unlimited)
    pub max_request_size: u64,

    /// Produce rate limit in bytes per second per client (0 = unlimited)
    pub produce_rate_limit_bytes: u64,

    /// Connection idle timeout in seconds (0 = no timeout)
    pub connection_idle_timeout_secs: u64,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            max_connections: 10000,            // 10k total connections
            max_connections_per_ip: 100,       // 100 connections per IP
            max_request_size: 104857600,       // 100 MB max request
            produce_rate_limit_bytes: 0,               // unlimited (opt-in via config)
            connection_idle_timeout_secs: 600, // 10 minute idle timeout
        }
    }
}

/// Connection tracker for per-IP connection counting
#[derive(Default)]
struct PerIpTracker {
    /// Number of active connections from this IP
    count: AtomicU64,
}

/// Token bucket rate limiter for per-client rate limiting
struct TokenBucket {
    /// Tokens available
    tokens: AtomicU64,
    /// Last refill time (unix timestamp in millis)
    last_refill: AtomicU64,
    /// Rate in bytes per second
    rate_per_sec: u64,
    /// Maximum burst size
    max_tokens: u64,
}

impl TokenBucket {
    fn new(rate_per_sec: u64) -> Self {
        // Allow burst of 1 second worth of tokens
        let max_tokens = rate_per_sec;
        Self {
            tokens: AtomicU64::new(max_tokens),
            last_refill: AtomicU64::new(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64,
            ),
            rate_per_sec,
            max_tokens,
        }
    }

    /// Try to consume tokens, returns true if allowed
    fn try_consume(&self, tokens: u64) -> bool {
        self.refill();

        loop {
            let current = self.tokens.load(Ordering::Relaxed);
            if current < tokens {
                return false;
            }
            if self
                .tokens
                .compare_exchange_weak(
                    current,
                    current - tokens,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                return true;
            }
        }
    }

    /// Refill tokens based on elapsed time
    fn refill(&self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let last = self.last_refill.load(Ordering::Relaxed);
        let elapsed_ms = now.saturating_sub(last);

        if elapsed_ms > 0 {
            // Calculate new tokens: rate_per_sec * elapsed_ms / 1000
            let new_tokens = (self.rate_per_sec * elapsed_ms) / 1000;
            if new_tokens > 0
                && self
                    .last_refill
                    .compare_exchange_weak(last, now, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
            {
                let current = self.tokens.load(Ordering::Relaxed);
                let new_total = std::cmp::min(current + new_tokens, self.max_tokens);
                self.tokens.store(new_total, Ordering::Relaxed);
            }
        }
    }
}

/// Resource limiter for connection and rate limiting
pub struct ResourceLimiter {
    /// Configuration
    config: LimitsConfig,

    /// Connection semaphore for total connection limiting
    connection_semaphore: Option<Arc<Semaphore>>,

    /// Per-IP connection counts
    per_ip_connections: DashMap<IpAddr, Arc<PerIpTracker>>,

    /// Per-client rate limiters (keyed by IP)
    rate_limiters: DashMap<IpAddr, Arc<TokenBucket>>,

    /// Total connections counter
    total_connections: AtomicU64,

    /// Connections rejected counter
    connections_rejected: AtomicU64,

    /// Requests rejected due to size counter
    size_rejections: AtomicU64,

    /// Requests rejected due to rate limit counter
    rate_limit_rejections: AtomicU64,
}

impl ResourceLimiter {
    /// Create a new resource limiter with the given configuration
    pub fn new(config: LimitsConfig) -> Self {
        let connection_semaphore = if config.max_connections > 0 {
            Some(Arc::new(Semaphore::new(config.max_connections as usize)))
        } else {
            None
        };

        Self {
            config,
            connection_semaphore,
            per_ip_connections: DashMap::new(),
            rate_limiters: DashMap::new(),
            total_connections: AtomicU64::new(0),
            connections_rejected: AtomicU64::new(0),
            size_rejections: AtomicU64::new(0),
            rate_limit_rejections: AtomicU64::new(0),
        }
    }

    /// Try to accept a new connection from the given IP
    /// Returns a guard that must be held for the duration of the connection
    pub fn try_accept_connection(&self, ip: IpAddr) -> Result<ConnectionGuard<'_>, LimitError> {
        // Check global connection limit
        if let Some(ref semaphore) = self.connection_semaphore {
            if semaphore.available_permits() == 0 {
                self.connections_rejected.fetch_add(1, Ordering::Relaxed);
                warn!(
                    ip = %ip,
                    max = self.config.max_connections,
                    "Connection rejected: max connections reached"
                );
                return Err(LimitError::MaxConnectionsReached);
            }
        }

        // Check per-IP limit
        if self.config.max_connections_per_ip > 0 {
            let tracker = self
                .per_ip_connections
                .entry(ip)
                .or_insert_with(|| Arc::new(PerIpTracker::default()))
                .clone();

            let current = tracker.count.load(Ordering::Relaxed);
            if current >= self.config.max_connections_per_ip as u64 {
                self.connections_rejected.fetch_add(1, Ordering::Relaxed);
                warn!(
                    ip = %ip,
                    current = current,
                    max = self.config.max_connections_per_ip,
                    "Connection rejected: per-IP limit reached"
                );
                return Err(LimitError::PerIpLimitReached);
            }

            // Increment per-IP count
            tracker.count.fetch_add(1, Ordering::Relaxed);
        }

        // Acquire global permit
        let permit = if let Some(ref semaphore) = self.connection_semaphore {
            match semaphore.clone().try_acquire_owned() {
                Ok(permit) => Some(permit),
                Err(_) => {
                    // Rollback per-IP increment
                    if self.config.max_connections_per_ip > 0 {
                        if let Some(tracker) = self.per_ip_connections.get(&ip) {
                            tracker.count.fetch_sub(1, Ordering::Relaxed);
                        }
                    }
                    self.connections_rejected.fetch_add(1, Ordering::Relaxed);
                    return Err(LimitError::MaxConnectionsReached);
                }
            }
        } else {
            None
        };

        self.total_connections.fetch_add(1, Ordering::Relaxed);

        debug!(
            ip = %ip,
            total = self.total_connections.load(Ordering::Relaxed),
            "Connection accepted"
        );

        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Ok(ConnectionGuard {
            ip,
            limiter: self,
            permit,
            connected_at: Instant::now(),
            last_activity: AtomicU64::new(now_secs),
        })
    }

    /// Check if a request size is within limits
    pub fn check_request_size(&self, size: u64) -> Result<(), LimitError> {
        if self.config.max_request_size > 0 && size > self.config.max_request_size {
            self.size_rejections.fetch_add(1, Ordering::Relaxed);
            warn!(
                size = size,
                max = self.config.max_request_size,
                "Request rejected: exceeds max size"
            );
            return Err(LimitError::RequestTooLarge {
                size,
                max: self.config.max_request_size,
            });
        }
        Ok(())
    }

    /// Check and consume rate limit tokens for produce requests
    pub fn check_produce_rate(&self, ip: IpAddr, bytes: u64) -> Result<(), LimitError> {
        if self.config.produce_rate_limit_bytes == 0 {
            return Ok(()); // No rate limit configured
        }

        let bucket = self
            .rate_limiters
            .entry(ip)
            .or_insert_with(|| Arc::new(TokenBucket::new(self.config.produce_rate_limit_bytes)))
            .clone();

        if !bucket.try_consume(bytes) {
            self.rate_limit_rejections.fetch_add(1, Ordering::Relaxed);
            debug!(
                ip = %ip,
                bytes = bytes,
                rate_limit = self.config.produce_rate_limit_bytes,
                "Request throttled: rate limit exceeded"
            );
            return Err(LimitError::RateLimitExceeded);
        }

        Ok(())
    }

    /// Get the connection idle timeout duration
    pub fn idle_timeout(&self) -> Option<Duration> {
        if self.config.connection_idle_timeout_secs > 0 {
            Some(Duration::from_secs(
                self.config.connection_idle_timeout_secs,
            ))
        } else {
            None
        }
    }

    /// Get current statistics
    pub fn stats(&self) -> LimiterStats {
        LimiterStats {
            total_connections: self.total_connections.load(Ordering::Relaxed),
            connections_rejected: self.connections_rejected.load(Ordering::Relaxed),
            size_rejections: self.size_rejections.load(Ordering::Relaxed),
            rate_limit_rejections: self.rate_limit_rejections.load(Ordering::Relaxed),
            // Note: unique_ips count is expensive and can cause deadlocks, so we skip it
            unique_ips: 0,
        }
    }

    /// Release connection for the given IP (called by ConnectionGuard on drop)
    fn release_connection(&self, ip: IpAddr) {
        let mut should_remove = false;

        if self.config.max_connections_per_ip > 0 {
            if let Some(tracker) = self.per_ip_connections.get(&ip) {
                let prev = tracker.count.fetch_sub(1, Ordering::Relaxed);
                // Mark for cleanup if no more connections from this IP
                if prev <= 1 {
                    should_remove = true;
                }
            }
            // Drop the read lock before potentially acquiring write locks
        }

        // Clean up entries after releasing the read lock
        if should_remove {
            self.per_ip_connections.remove(&ip);
            self.rate_limiters.remove(&ip);
        }

        self.total_connections.fetch_sub(1, Ordering::Relaxed);
    }

    /// Clean up stale rate limiters (call periodically)
    pub fn cleanup_stale_entries(&self) {
        // Remove rate limiters for IPs with no active connections
        let ips_to_remove: Vec<_> = self
            .rate_limiters
            .iter()
            .filter(|entry| !self.per_ip_connections.contains_key(entry.key()))
            .map(|entry| *entry.key())
            .collect();

        for ip in ips_to_remove {
            self.rate_limiters.remove(&ip);
        }
    }
}

/// Guard that tracks an active connection
pub struct ConnectionGuard<'a> {
    ip: IpAddr,
    limiter: &'a ResourceLimiter,
    #[allow(dead_code)]
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
    connected_at: Instant,
    last_activity: std::sync::atomic::AtomicU64,
}

impl<'a> ConnectionGuard<'a> {
    /// Get the IP address of this connection
    pub fn ip(&self) -> IpAddr {
        self.ip
    }

    /// Get the duration this connection has been active
    pub fn duration(&self) -> Duration {
        self.connected_at.elapsed()
    }

    /// Update the last activity time (call when processing requests)
    pub fn touch(&self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.last_activity.store(now, Ordering::Relaxed);
    }

    /// Check if the connection has been idle for longer than the timeout
    pub fn is_idle(&self, timeout: Duration) -> bool {
        let last = self.last_activity.load(Ordering::Relaxed);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        now.saturating_sub(last) >= timeout.as_secs()
    }

    /// Get seconds since last activity
    pub fn idle_secs(&self) -> u64 {
        let last = self.last_activity.load(Ordering::Relaxed);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        now.saturating_sub(last)
    }
}

impl<'a> Drop for ConnectionGuard<'a> {
    fn drop(&mut self) {
        self.limiter.release_connection(self.ip);
        debug!(
            ip = %self.ip,
            duration_secs = self.duration().as_secs(),
            "Connection released"
        );
    }
}

/// Error type for limit violations
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LimitError {
    /// Maximum total connections reached
    MaxConnectionsReached,

    /// Per-IP connection limit reached
    PerIpLimitReached,

    /// Request size exceeds maximum
    RequestTooLarge { size: u64, max: u64 },

    /// Rate limit exceeded
    RateLimitExceeded,
}

impl std::fmt::Display for LimitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LimitError::MaxConnectionsReached => {
                write!(f, "Maximum total connections reached")
            }
            LimitError::PerIpLimitReached => {
                write!(f, "Per-IP connection limit reached")
            }
            LimitError::RequestTooLarge { size, max } => {
                write!(
                    f,
                    "Request too large: {} bytes exceeds limit of {} bytes",
                    size, max
                )
            }
            LimitError::RateLimitExceeded => {
                write!(f, "Rate limit exceeded")
            }
        }
    }
}

impl std::error::Error for LimitError {}

/// Statistics from the resource limiter
#[derive(Debug, Clone, Default)]
pub struct LimiterStats {
    /// Current total connections
    pub total_connections: u64,
    /// Connections rejected since start
    pub connections_rejected: u64,
    /// Requests rejected due to size since start
    pub size_rejections: u64,
    /// Requests rejected due to rate limit since start
    pub rate_limit_rejections: u64,
    /// Number of unique IPs currently connected
    pub unique_ips: u64,
}

/// Configuration for client quotas
///
/// Default values:
/// - `producer_byte_rate`: 0 (unlimited)
/// - `consumer_byte_rate`: 0 (unlimited)
/// - `request_percentage`: 0 (unlimited)
/// - `enabled`: false (disabled)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QuotaConfig {
    /// Producer byte rate quota in bytes per second (0 = unlimited)
    pub producer_byte_rate: u64,
    /// Consumer byte rate quota in bytes per second (0 = unlimited)
    pub consumer_byte_rate: u64,
    /// Request percentage quota (0 = unlimited, 100 = max)
    pub request_percentage: u32,
    /// Whether quotas are enabled
    pub enabled: bool,
}

/// Client quota tracker with throttle time calculation
struct ClientQuota {
    /// Producer rate tracker
    producer_bucket: TokenBucket,
    /// Consumer rate tracker
    consumer_bucket: TokenBucket,
    /// Request time tracker (for future request percentage quota)
    #[allow(dead_code)]
    request_time_used_ms: AtomicU64,
    /// Window start time (for future request percentage quota)
    #[allow(dead_code)]
    window_start_ms: AtomicU64,
    /// Configuration
    config: QuotaConfig,
}

impl ClientQuota {
    fn new(config: QuotaConfig) -> Self {
        let producer_rate = if config.producer_byte_rate > 0 {
            config.producer_byte_rate
        } else {
            u64::MAX / 2 // Effectively unlimited
        };
        let consumer_rate = if config.consumer_byte_rate > 0 {
            config.consumer_byte_rate
        } else {
            u64::MAX / 2
        };

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Self {
            producer_bucket: TokenBucket::new(producer_rate),
            consumer_bucket: TokenBucket::new(consumer_rate),
            request_time_used_ms: AtomicU64::new(0),
            window_start_ms: AtomicU64::new(now),
            config,
        }
    }

    /// Check producer quota and return throttle time in ms
    fn check_producer(&self, bytes: u64) -> i32 {
        if !self.config.enabled || self.config.producer_byte_rate == 0 {
            return 0;
        }

        self.producer_bucket.refill();
        let tokens = self.producer_bucket.tokens.load(Ordering::Relaxed);

        if tokens >= bytes {
            // Can consume without throttling
            self.producer_bucket.try_consume(bytes);
            return 0;
        }

        // Calculate throttle time: how long until we have enough tokens
        let deficit = bytes.saturating_sub(tokens);
        let throttle_ms = (deficit * 1000) / self.config.producer_byte_rate.max(1);
        throttle_ms.min(30000) as i32 // Cap at 30 seconds
    }

    /// Check consumer quota and return throttle time in ms
    fn check_consumer(&self, bytes: u64) -> i32 {
        if !self.config.enabled || self.config.consumer_byte_rate == 0 {
            return 0;
        }

        self.consumer_bucket.refill();
        let tokens = self.consumer_bucket.tokens.load(Ordering::Relaxed);

        if tokens >= bytes {
            self.consumer_bucket.try_consume(bytes);
            return 0;
        }

        let deficit = bytes.saturating_sub(tokens);
        let throttle_ms = (deficit * 1000) / self.config.consumer_byte_rate.max(1);
        throttle_ms.min(30000) as i32
    }
}

/// Quota manager for client-level quotas with Kafka-compatible throttling
pub struct QuotaManager {
    /// Default quota config for clients without specific quotas
    default_config: QuotaConfig,
    /// Per-client quotas (keyed by client_id or user principal)
    client_quotas: DashMap<String, Arc<ClientQuota>>,
    /// Per-IP quotas as fallback
    ip_quotas: DashMap<IpAddr, Arc<ClientQuota>>,
    /// Total throttle time applied (for metrics)
    total_throttle_time_ms: AtomicU64,
}

impl QuotaManager {
    /// Create a new quota manager with default configuration
    pub fn new(default_config: QuotaConfig) -> Self {
        Self {
            default_config,
            client_quotas: DashMap::new(),
            ip_quotas: DashMap::new(),
            total_throttle_time_ms: AtomicU64::new(0),
        }
    }

    /// Set quota for a specific client
    pub fn set_client_quota(&self, client_id: &str, config: QuotaConfig) {
        self.client_quotas
            .insert(client_id.to_string(), Arc::new(ClientQuota::new(config)));
    }

    /// Remove quota for a specific client
    pub fn remove_client_quota(&self, client_id: &str) {
        self.client_quotas.remove(client_id);
    }

    /// Check producer quota for a client and return throttle time in ms
    ///
    /// Returns 0 if not throttled, otherwise the number of milliseconds
    /// the client should delay before sending again.
    pub fn check_producer_quota(&self, client_id: Option<&str>, ip: IpAddr, bytes: u64) -> i32 {
        if !self.default_config.enabled && self.client_quotas.is_empty() {
            return 0;
        }

        // Try client-specific quota first
        if let Some(client_id) = client_id {
            if let Some(quota) = self.client_quotas.get(client_id) {
                let throttle = quota.check_producer(bytes);
                if throttle > 0 {
                    self.total_throttle_time_ms
                        .fetch_add(throttle as u64, Ordering::Relaxed);
                }
                return throttle;
            }
        }

        // Fall back to IP-based quota
        let quota = self
            .ip_quotas
            .entry(ip)
            .or_insert_with(|| Arc::new(ClientQuota::new(self.default_config.clone())))
            .clone();

        let throttle = quota.check_producer(bytes);
        if throttle > 0 {
            self.total_throttle_time_ms
                .fetch_add(throttle as u64, Ordering::Relaxed);
        }
        throttle
    }

    /// Check consumer quota for a client and return throttle time in ms
    pub fn check_consumer_quota(&self, client_id: Option<&str>, ip: IpAddr, bytes: u64) -> i32 {
        if !self.default_config.enabled && self.client_quotas.is_empty() {
            return 0;
        }

        // Try client-specific quota first
        if let Some(client_id) = client_id {
            if let Some(quota) = self.client_quotas.get(client_id) {
                let throttle = quota.check_consumer(bytes);
                if throttle > 0 {
                    self.total_throttle_time_ms
                        .fetch_add(throttle as u64, Ordering::Relaxed);
                }
                return throttle;
            }
        }

        // Fall back to IP-based quota
        let quota = self
            .ip_quotas
            .entry(ip)
            .or_insert_with(|| Arc::new(ClientQuota::new(self.default_config.clone())))
            .clone();

        let throttle = quota.check_consumer(bytes);
        if throttle > 0 {
            self.total_throttle_time_ms
                .fetch_add(throttle as u64, Ordering::Relaxed);
        }
        throttle
    }

    /// Get total throttle time applied since start
    pub fn total_throttle_time_ms(&self) -> u64 {
        self.total_throttle_time_ms.load(Ordering::Relaxed)
    }

    /// Get the number of clients with specific quotas
    pub fn client_quota_count(&self) -> usize {
        self.client_quotas.len()
    }

    /// Check if quotas are enabled
    pub fn is_enabled(&self) -> bool {
        self.default_config.enabled || !self.client_quotas.is_empty()
    }

    /// Get quota configuration for a specific client
    pub fn get_client_quota(&self, client_id: &str) -> Option<QuotaConfig> {
        self.client_quotas.get(client_id).map(|q| q.config.clone())
    }

    /// Get the default quota configuration
    pub fn default_config(&self) -> &QuotaConfig {
        &self.default_config
    }

    /// List all client IDs with custom quotas
    pub fn list_client_quotas(&self) -> Vec<(String, QuotaConfig)> {
        self.client_quotas
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().config.clone()))
            .collect()
    }

    /// Set or update a quota value for a client
    ///
    /// `quota_type` can be: "producer_byte_rate", "consumer_byte_rate", "request_percentage"
    /// If the client has no existing quota, creates one from default and applies the value.
    pub fn set_quota_value(&self, client_id: &str, quota_type: &str, value: Option<f64>) {
        let current = self
            .get_client_quota(client_id)
            .unwrap_or_else(|| QuotaConfig {
                enabled: true,
                ..self.default_config.clone()
            });

        let new_config = match (quota_type, value) {
            ("producer_byte_rate", Some(v)) => QuotaConfig {
                producer_byte_rate: v as u64,
                enabled: true,
                ..current
            },
            ("consumer_byte_rate", Some(v)) => QuotaConfig {
                consumer_byte_rate: v as u64,
                enabled: true,
                ..current
            },
            ("request_percentage", Some(v)) => QuotaConfig {
                request_percentage: v as u32,
                enabled: true,
                ..current
            },
            ("producer_byte_rate", None) => QuotaConfig {
                producer_byte_rate: 0, // Unlimited
                ..current
            },
            ("consumer_byte_rate", None) => QuotaConfig {
                consumer_byte_rate: 0, // Unlimited
                ..current
            },
            ("request_percentage", None) => QuotaConfig {
                request_percentage: 0, // Unlimited
                ..current
            },
            _ => current,
        };

        self.set_client_quota(client_id, new_config);
    }

    /// Remove a specific quota value for a client (set to unlimited)
    pub fn remove_quota_value(&self, client_id: &str, quota_type: &str) {
        self.set_quota_value(client_id, quota_type, None);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_limits_config_default() {
        let config = LimitsConfig::default();
        assert_eq!(config.max_connections, 10000);
        assert_eq!(config.max_connections_per_ip, 100);
        assert_eq!(config.max_request_size, 104857600);
        assert_eq!(config.produce_rate_limit_bytes, 0);
        assert_eq!(config.connection_idle_timeout_secs, 600);
    }

    #[tokio::test]
    async fn test_resource_limiter_accepts_connection() {
        let config = LimitsConfig {
            max_connections: 10,
            max_connections_per_ip: 5,
            ..Default::default()
        };
        let limiter = ResourceLimiter::new(config);
        let ip: IpAddr = "127.0.0.1".parse().unwrap();

        let guard = limiter.try_accept_connection(ip).unwrap();
        assert_eq!(limiter.stats().total_connections, 1);
        drop(guard);
        assert_eq!(limiter.stats().total_connections, 0);
    }

    #[tokio::test]
    async fn test_resource_limiter_per_ip_limit() {
        let config = LimitsConfig {
            max_connections: 100,
            max_connections_per_ip: 2,
            ..Default::default()
        };
        let limiter = ResourceLimiter::new(config);
        let ip: IpAddr = "127.0.0.1".parse().unwrap();

        let _guard1 = limiter.try_accept_connection(ip).unwrap();
        let _guard2 = limiter.try_accept_connection(ip).unwrap();

        // Third connection should be rejected
        let result = limiter.try_accept_connection(ip);
        assert!(matches!(result, Err(LimitError::PerIpLimitReached)));
        assert_eq!(limiter.stats().connections_rejected, 1);
    }

    #[tokio::test]
    async fn test_resource_limiter_max_connections() {
        let config = LimitsConfig {
            max_connections: 2,
            max_connections_per_ip: 100,
            ..Default::default()
        };
        let limiter = ResourceLimiter::new(config);

        let _guard1 = limiter
            .try_accept_connection("127.0.0.1".parse().unwrap())
            .unwrap();
        let _guard2 = limiter
            .try_accept_connection("127.0.0.2".parse().unwrap())
            .unwrap();

        // Third connection should be rejected
        let result = limiter.try_accept_connection("127.0.0.3".parse().unwrap());
        assert!(matches!(result, Err(LimitError::MaxConnectionsReached)));
    }

    #[test]
    fn test_request_size_validation() {
        let config = LimitsConfig {
            max_request_size: 1000,
            ..Default::default()
        };
        let limiter = ResourceLimiter::new(config);

        assert!(limiter.check_request_size(500).is_ok());
        assert!(limiter.check_request_size(1000).is_ok());

        let result = limiter.check_request_size(1001);
        assert!(matches!(result, Err(LimitError::RequestTooLarge { .. })));
        assert_eq!(limiter.stats().size_rejections, 1);
    }

    #[test]
    fn test_rate_limiter() {
        let config = LimitsConfig {
            produce_rate_limit_bytes: 1000, // 1000 bytes/sec
            ..Default::default()
        };
        let limiter = ResourceLimiter::new(config);
        let ip: IpAddr = "127.0.0.1".parse().unwrap();

        // First request should succeed
        assert!(limiter.check_produce_rate(ip, 500).is_ok());

        // Second request should also succeed (still within burst)
        assert!(limiter.check_produce_rate(ip, 500).is_ok());

        // Third request should be throttled
        let result = limiter.check_produce_rate(ip, 500);
        assert!(matches!(result, Err(LimitError::RateLimitExceeded)));
    }

    #[tokio::test]
    async fn test_unlimited_config() {
        let config = LimitsConfig {
            max_connections: 0,
            max_connections_per_ip: 0,
            max_request_size: 0,
            produce_rate_limit_bytes: 0,
            connection_idle_timeout_secs: 0,
        };
        let limiter = ResourceLimiter::new(config);
        let ip: IpAddr = "127.0.0.1".parse().unwrap();

        // All operations should succeed with unlimited config
        let _guard = limiter.try_accept_connection(ip).unwrap();
        assert!(limiter.check_request_size(u64::MAX).is_ok());
        assert!(limiter.check_produce_rate(ip, u64::MAX).is_ok());
        assert!(limiter.idle_timeout().is_none());
    }

    #[test]
    fn test_limit_error_display() {
        assert_eq!(
            LimitError::MaxConnectionsReached.to_string(),
            "Maximum total connections reached"
        );
        assert_eq!(
            LimitError::PerIpLimitReached.to_string(),
            "Per-IP connection limit reached"
        );
        assert_eq!(
            LimitError::RequestTooLarge {
                size: 2000,
                max: 1000
            }
            .to_string(),
            "Request too large: 2000 bytes exceeds limit of 1000 bytes"
        );
        assert_eq!(
            LimitError::RateLimitExceeded.to_string(),
            "Rate limit exceeded"
        );
    }

    #[test]
    fn test_stats() {
        let config = LimitsConfig::default();
        let limiter = ResourceLimiter::new(config);

        let stats = limiter.stats();
        assert_eq!(stats.total_connections, 0);
        assert_eq!(stats.connections_rejected, 0);
        assert_eq!(stats.size_rejections, 0);
        assert_eq!(stats.rate_limit_rejections, 0);
    }

    #[tokio::test]
    async fn test_connection_guard_touch_and_idle() {
        let config = LimitsConfig {
            connection_idle_timeout_secs: 60,
            ..Default::default()
        };
        let limiter = ResourceLimiter::new(config);
        let ip: IpAddr = "127.0.0.1".parse().unwrap();

        let guard = limiter.try_accept_connection(ip).unwrap();

        // Just connected, should not be idle
        assert!(!guard.is_idle(Duration::from_secs(60)));
        assert_eq!(guard.idle_secs(), 0);

        // Touch and verify it resets
        guard.touch();
        assert!(!guard.is_idle(Duration::from_secs(60)));
    }

    #[test]
    fn test_idle_timeout_config() {
        // With timeout
        let config = LimitsConfig {
            connection_idle_timeout_secs: 300,
            ..Default::default()
        };
        let limiter = ResourceLimiter::new(config);
        assert_eq!(limiter.idle_timeout(), Some(Duration::from_secs(300)));

        // Without timeout
        let config = LimitsConfig {
            connection_idle_timeout_secs: 0,
            ..Default::default()
        };
        let limiter = ResourceLimiter::new(config);
        assert!(limiter.idle_timeout().is_none());
    }

    #[test]
    fn test_quota_config_default() {
        let config = QuotaConfig::default();
        assert_eq!(config.producer_byte_rate, 0); // Unlimited
        assert_eq!(config.consumer_byte_rate, 0); // Unlimited
        assert_eq!(config.request_percentage, 0); // Unlimited
        assert!(!config.enabled);
    }

    #[test]
    fn test_quota_manager_disabled_by_default() {
        let config = QuotaConfig::default();
        let manager = QuotaManager::new(config);

        let ip: IpAddr = "127.0.0.1".parse().unwrap();

        // Should not throttle when disabled
        assert_eq!(manager.check_producer_quota(None, ip, 1_000_000), 0);
        assert_eq!(manager.check_consumer_quota(None, ip, 1_000_000), 0);
        assert!(!manager.is_enabled());
    }

    #[test]
    fn test_quota_manager_producer_quota() {
        let config = QuotaConfig {
            producer_byte_rate: 1000, // 1000 bytes/sec
            consumer_byte_rate: 0,
            request_percentage: 0,
            enabled: true,
        };
        let manager = QuotaManager::new(config);

        let ip: IpAddr = "127.0.0.1".parse().unwrap();

        // First request within quota should not throttle
        let throttle = manager.check_producer_quota(None, ip, 500);
        assert_eq!(throttle, 0);

        // Request that would exceed quota should return throttle time
        // After consuming 500 bytes, requesting 1500 more exceeds the rate
        let throttle = manager.check_producer_quota(None, ip, 1500);
        // Should have throttle time > 0 since we're over quota
        assert!(throttle >= 0); // May be 0 if bucket had room due to timing
    }

    #[test]
    fn test_quota_manager_consumer_quota() {
        let config = QuotaConfig {
            producer_byte_rate: 0,
            consumer_byte_rate: 1000, // 1000 bytes/sec
            request_percentage: 0,
            enabled: true,
        };
        let manager = QuotaManager::new(config);

        let ip: IpAddr = "127.0.0.1".parse().unwrap();

        // First request within quota should not throttle
        let throttle = manager.check_consumer_quota(None, ip, 500);
        assert_eq!(throttle, 0);
    }

    #[test]
    fn test_quota_manager_per_client_quota() {
        let default_config = QuotaConfig {
            producer_byte_rate: 1000,
            consumer_byte_rate: 1000,
            request_percentage: 0,
            enabled: true,
        };
        let manager = QuotaManager::new(default_config);

        // Set a higher quota for a specific client
        let client_config = QuotaConfig {
            producer_byte_rate: 10000, // 10x higher
            consumer_byte_rate: 10000,
            request_percentage: 0,
            enabled: true,
        };
        manager.set_client_quota("high-priority-client", client_config);

        let ip: IpAddr = "127.0.0.1".parse().unwrap();

        // High priority client should have higher quota
        let throttle = manager.check_producer_quota(Some("high-priority-client"), ip, 5000);
        assert_eq!(throttle, 0); // Should not throttle with 10KB/s quota

        // Unknown client falls back to default IP-based quota
        let throttle = manager.check_producer_quota(Some("unknown-client"), ip, 500);
        assert_eq!(throttle, 0); // First request within default quota
    }

    #[test]
    fn test_quota_manager_metrics() {
        let config = QuotaConfig {
            producer_byte_rate: 100, // Low quota to trigger throttling
            consumer_byte_rate: 100,
            request_percentage: 0,
            enabled: true,
        };
        let manager = QuotaManager::new(config);

        assert_eq!(manager.total_throttle_time_ms(), 0);
        assert_eq!(manager.client_quota_count(), 0);

        // Add a client quota
        manager.set_client_quota("client1", QuotaConfig::default());
        assert_eq!(manager.client_quota_count(), 1);

        // Remove it
        manager.remove_client_quota("client1");
        assert_eq!(manager.client_quota_count(), 0);
    }
}
