//! Protocol-Level Rate Limiting
//!
//! Token bucket rate limiter for enforcing per-tenant / per-client quotas on
//! produce, consume, and request rates.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use parking_lot::RwLock;
use tracing::{debug, info, warn};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Global configuration for rate limiting defaults.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimiterConfig {
    /// Default produce rate limit in bytes/sec (default: 10 MB/s).
    pub default_produce_rate: u64,
    /// Default consume rate limit in bytes/sec (default: 50 MB/s).
    pub default_consume_rate: u64,
    /// Default request rate limit in requests/sec (default: 1000).
    pub default_request_rate: u64,
    /// Burst multiplier applied to bucket capacity (default: 1.5).
    pub burst_multiplier: f64,
    /// Token refill interval in milliseconds (default: 100).
    pub refill_interval_ms: u64,
}

impl Default for RateLimiterConfig {
    fn default() -> Self {
        Self {
            default_produce_rate: 10 * 1024 * 1024, // 10 MB/s
            default_consume_rate: 50 * 1024 * 1024, // 50 MB/s
            default_request_rate: 1000,
            burst_multiplier: 1.5,
            refill_interval_ms: 100,
        }
    }
}

// ---------------------------------------------------------------------------
// Token bucket
// ---------------------------------------------------------------------------

/// A single token bucket for one limiter identity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenBucket {
    /// Identifier for this bucket (e.g. tenant id, client id).
    pub id: String,
    /// Maximum number of tokens (burst capacity).
    pub capacity: u64,
    /// Current number of available tokens.
    pub tokens: f64,
    /// Tokens added per millisecond.
    pub refill_rate: f64,
    /// Timestamp of the last refill (ms since an arbitrary epoch).
    pub last_refill: u64,
    /// Total number of requests/bytes that were allowed.
    pub total_allowed: u64,
    /// Total number of requests/bytes that were rejected.
    pub total_rejected: u64,
}

impl TokenBucket {
    /// Create a new bucket starting full.
    fn new(id: String, capacity: u64, refill_rate: f64, now_ms: u64) -> Self {
        Self {
            id,
            capacity,
            tokens: capacity as f64,
            refill_rate,
            last_refill: now_ms,
            total_allowed: 0,
            total_rejected: 0,
        }
    }

    /// Refill tokens based on elapsed time.
    fn refill(&mut self, now_ms: u64) {
        if now_ms <= self.last_refill {
            return;
        }
        let elapsed = now_ms - self.last_refill;
        let added = elapsed as f64 * self.refill_rate;
        self.tokens = (self.tokens + added).min(self.capacity as f64);
        self.last_refill = now_ms;
    }

    /// Try to consume `cost` tokens. Returns the result.
    fn try_consume(&mut self, cost: u64, now_ms: u64) -> RateLimitResult {
        self.refill(now_ms);

        let cost_f = cost as f64;
        if self.tokens >= cost_f {
            self.tokens -= cost_f;
            self.total_allowed += cost;
            RateLimitResult::Allowed {
                remaining: self.tokens as u64,
            }
        } else {
            self.total_rejected += cost;
            // How long until enough tokens accumulate?
            let deficit = cost_f - self.tokens;
            let wait_ms = if self.refill_rate > 0.0 {
                (deficit / self.refill_rate).ceil() as u64
            } else {
                u64::MAX
            };
            RateLimitResult::Rejected {
                retry_after_ms: wait_ms,
                limit: self.capacity,
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Result type
// ---------------------------------------------------------------------------

/// Outcome of a rate-limit check.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RateLimitResult {
    /// The request is allowed; `remaining` tokens are left.
    Allowed { remaining: u64 },
    /// The request is rejected; retry after `retry_after_ms` milliseconds.
    Rejected { retry_after_ms: u64, limit: u64 },
}

// ---------------------------------------------------------------------------
// Aggregate stats
// ---------------------------------------------------------------------------

/// Aggregate statistics across all limiters.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RateLimiterStats {
    pub total_allowed: u64,
    pub total_rejected: u64,
    pub total_limiters: u64,
}

// ---------------------------------------------------------------------------
// Usage snapshot
// ---------------------------------------------------------------------------

/// Usage snapshot for a single limiter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LimiterUsage {
    pub id: String,
    pub capacity: u64,
    pub remaining: u64,
    pub refill_rate: f64,
    pub total_allowed: u64,
    pub total_rejected: u64,
}

// ---------------------------------------------------------------------------
// Manager
// ---------------------------------------------------------------------------

/// Manages a collection of token-bucket rate limiters.
pub struct RateLimiterManager {
    limiters: Arc<RwLock<HashMap<String, TokenBucket>>>,
    config: RateLimiterConfig,
    stats: Arc<RateLimiterStats>,
    /// Reference instant so we can derive monotonic milliseconds.
    epoch: Instant,
}

impl RateLimiterManager {
    /// Create a new manager with the given configuration.
    pub fn new(config: RateLimiterConfig) -> Self {
        Self {
            limiters: Arc::new(RwLock::new(HashMap::new())),
            config,
            stats: Arc::new(RateLimiterStats::default()),
            epoch: Instant::now(),
        }
    }

    /// Monotonic clock in milliseconds.
    fn now_ms(&self) -> u64 {
        self.epoch.elapsed().as_millis() as u64
    }

    /// Check (and consume) `tokens` for the limiter identified by `id`.
    ///
    /// If no limiter exists for `id`, one is lazily created using the default
    /// request rate from the configuration.
    pub fn check_rate(&self, id: &str, tokens: u64) -> RateLimitResult {
        let now = self.now_ms();
        let mut map = self.limiters.write();
        let bucket = map.entry(id.to_string()).or_insert_with(|| {
            let capacity =
                (self.config.default_request_rate as f64 * self.config.burst_multiplier) as u64;
            let refill_rate = self.config.default_request_rate as f64 / 1000.0;
            debug!(id, capacity, refill_rate, "creating default limiter");
            TokenBucket::new(id.to_string(), capacity, refill_rate, now)
        });
        bucket.try_consume(tokens, now)
    }

    /// Set (or replace) the limiter for `id` with explicit capacity and rate.
    pub fn set_limit(&self, id: &str, capacity: u64, rate: f64) {
        let now = self.now_ms();
        info!(id, capacity, rate, "setting rate limit");
        let mut map = self.limiters.write();
        map.insert(
            id.to_string(),
            TokenBucket::new(id.to_string(), capacity, rate, now),
        );
    }

    /// Remove the limiter for `id`. Returns `true` if it existed.
    pub fn remove_limit(&self, id: &str) -> bool {
        let removed = self.limiters.write().remove(id).is_some();
        if removed {
            info!(id, "removed rate limiter");
        } else {
            warn!(id, "attempted to remove non-existent rate limiter");
        }
        removed
    }

    /// Get a usage snapshot for a specific limiter.
    pub fn get_usage(&self, id: &str) -> Option<LimiterUsage> {
        let map = self.limiters.read();
        map.get(id).map(|b| LimiterUsage {
            id: b.id.clone(),
            capacity: b.capacity,
            remaining: b.tokens as u64,
            refill_rate: b.refill_rate,
            total_allowed: b.total_allowed,
            total_rejected: b.total_rejected,
        })
    }

    /// List all active limiters.
    pub fn list_limiters(&self) -> Vec<LimiterUsage> {
        let map = self.limiters.read();
        map.values()
            .map(|b| LimiterUsage {
                id: b.id.clone(),
                capacity: b.capacity,
                remaining: b.tokens as u64,
                refill_rate: b.refill_rate,
                total_allowed: b.total_allowed,
                total_rejected: b.total_rejected,
            })
            .collect()
    }

    /// Aggregate statistics across all limiters.
    pub fn stats(&self) -> RateLimiterStats {
        let map = self.limiters.read();
        let mut total_allowed: u64 = 0;
        let mut total_rejected: u64 = 0;
        for b in map.values() {
            total_allowed += b.total_allowed;
            total_rejected += b.total_rejected;
        }
        RateLimiterStats {
            total_allowed,
            total_rejected,
            total_limiters: map.len() as u64,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn default_manager() -> RateLimiterManager {
        RateLimiterManager::new(RateLimiterConfig::default())
    }

    // -- Construction ---------------------------------------------------------

    #[test]
    fn test_new_manager() {
        let mgr = default_manager();
        assert!(mgr.list_limiters().is_empty());
        let stats = mgr.stats();
        assert_eq!(stats.total_allowed, 0);
        assert_eq!(stats.total_rejected, 0);
        assert_eq!(stats.total_limiters, 0);
    }

    #[test]
    fn test_default_config_values() {
        let cfg = RateLimiterConfig::default();
        assert_eq!(cfg.default_produce_rate, 10 * 1024 * 1024);
        assert_eq!(cfg.default_consume_rate, 50 * 1024 * 1024);
        assert_eq!(cfg.default_request_rate, 1000);
        assert!((cfg.burst_multiplier - 1.5).abs() < f64::EPSILON);
        assert_eq!(cfg.refill_interval_ms, 100);
    }

    // -- Token bucket basics --------------------------------------------------

    #[test]
    fn test_bucket_starts_full() {
        let bucket = TokenBucket::new("t1".into(), 100, 1.0, 0);
        assert_eq!(bucket.capacity, 100);
        assert!((bucket.tokens - 100.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_bucket_consume_allowed() {
        let mut bucket = TokenBucket::new("t1".into(), 100, 1.0, 0);
        let result = bucket.try_consume(50, 0);
        assert!(matches!(result, RateLimitResult::Allowed { remaining: 50 }));
        assert_eq!(bucket.total_allowed, 50);
    }

    #[test]
    fn test_bucket_consume_rejected() {
        let mut bucket = TokenBucket::new("t1".into(), 10, 1.0, 0);
        let result = bucket.try_consume(20, 0);
        match result {
            RateLimitResult::Rejected { retry_after_ms, limit } => {
                assert!(retry_after_ms > 0);
                assert_eq!(limit, 10);
            }
            _ => panic!("expected Rejected"),
        }
        assert_eq!(bucket.total_rejected, 20);
    }

    #[test]
    fn test_bucket_refill() {
        let mut bucket = TokenBucket::new("t1".into(), 100, 1.0, 0);
        // Consume all tokens
        bucket.try_consume(100, 0);
        assert!(bucket.tokens < 1.0);
        // Advance time by 50ms → should add 50 tokens
        bucket.refill(50);
        assert!((bucket.tokens - 50.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_bucket_refill_capped_at_capacity() {
        let mut bucket = TokenBucket::new("t1".into(), 100, 10.0, 0);
        bucket.refill(1000); // Would add 10_000 tokens
        assert!((bucket.tokens - 100.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_bucket_no_backward_refill() {
        let mut bucket = TokenBucket::new("t1".into(), 100, 1.0, 50);
        bucket.try_consume(50, 50);
        bucket.refill(30); // Earlier than last_refill
        assert!((bucket.tokens - 50.0).abs() < f64::EPSILON);
    }

    // -- Manager check_rate ---------------------------------------------------

    #[test]
    fn test_check_rate_auto_creates_limiter() {
        let mgr = default_manager();
        let result = mgr.check_rate("client-1", 1);
        assert!(matches!(result, RateLimitResult::Allowed { .. }));
        assert_eq!(mgr.list_limiters().len(), 1);
    }

    #[test]
    fn test_check_rate_allowed() {
        let mgr = default_manager();
        mgr.set_limit("c1", 100, 1.0);
        let result = mgr.check_rate("c1", 10);
        assert!(matches!(result, RateLimitResult::Allowed { remaining: 90 }));
    }

    #[test]
    fn test_check_rate_rejected_when_exhausted() {
        let mgr = default_manager();
        mgr.set_limit("c1", 10, 0.001); // very slow refill
        // Drain the bucket
        mgr.check_rate("c1", 10);
        let result = mgr.check_rate("c1", 5);
        assert!(matches!(result, RateLimitResult::Rejected { .. }));
    }

    // -- set_limit / remove_limit ---------------------------------------------

    #[test]
    fn test_set_limit() {
        let mgr = default_manager();
        mgr.set_limit("tenant-a", 500, 2.0);
        let usage = mgr.get_usage("tenant-a").unwrap();
        assert_eq!(usage.capacity, 500);
        assert!((usage.refill_rate - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_remove_limit() {
        let mgr = default_manager();
        mgr.set_limit("tenant-a", 500, 2.0);
        assert!(mgr.remove_limit("tenant-a"));
        assert!(mgr.get_usage("tenant-a").is_none());
    }

    #[test]
    fn test_remove_nonexistent_returns_false() {
        let mgr = default_manager();
        assert!(!mgr.remove_limit("ghost"));
    }

    // -- get_usage ------------------------------------------------------------

    #[test]
    fn test_get_usage_existing() {
        let mgr = default_manager();
        mgr.set_limit("c1", 100, 1.0);
        mgr.check_rate("c1", 30);
        let usage = mgr.get_usage("c1").unwrap();
        assert_eq!(usage.id, "c1");
        assert_eq!(usage.total_allowed, 30);
        assert_eq!(usage.remaining, 70);
    }

    #[test]
    fn test_get_usage_nonexistent() {
        let mgr = default_manager();
        assert!(mgr.get_usage("nope").is_none());
    }

    // -- list_limiters --------------------------------------------------------

    #[test]
    fn test_list_limiters() {
        let mgr = default_manager();
        mgr.set_limit("a", 100, 1.0);
        mgr.set_limit("b", 200, 2.0);
        let list = mgr.list_limiters();
        assert_eq!(list.len(), 2);
    }

    // -- stats ----------------------------------------------------------------

    #[test]
    fn test_stats_aggregation() {
        let mgr = default_manager();
        mgr.set_limit("a", 100, 1.0);
        mgr.set_limit("b", 10, 0.0001);
        mgr.check_rate("a", 20);
        mgr.check_rate("b", 5);
        mgr.check_rate("b", 50); // rejected
        let stats = mgr.stats();
        assert_eq!(stats.total_allowed, 25); // 20 + 5
        assert_eq!(stats.total_rejected, 50);
        assert_eq!(stats.total_limiters, 2);
    }

    // -- retry_after_ms correctness -------------------------------------------

    #[test]
    fn test_retry_after_calculation() {
        let mut bucket = TokenBucket::new("t1".into(), 100, 2.0, 0);
        bucket.try_consume(100, 0); // drain
        let result = bucket.try_consume(10, 0);
        match result {
            RateLimitResult::Rejected { retry_after_ms, .. } => {
                // Need 10 tokens at 2.0 tokens/ms → 5 ms
                assert_eq!(retry_after_ms, 5);
            }
            _ => panic!("expected Rejected"),
        }
    }

    #[test]
    fn test_consume_then_refill_then_consume() {
        let mut bucket = TokenBucket::new("t1".into(), 100, 1.0, 0);
        bucket.try_consume(100, 0);
        // After 50ms, 50 tokens available
        let result = bucket.try_consume(30, 50);
        assert!(matches!(result, RateLimitResult::Allowed { remaining: 20 }));
    }

    // -- Edge cases -----------------------------------------------------------

    #[test]
    fn test_zero_cost_always_allowed() {
        let mgr = default_manager();
        mgr.set_limit("c1", 0, 0.0); // zero capacity, zero rate
        // Even with 0 capacity, consuming 0 tokens should work
        let result = mgr.check_rate("c1", 0);
        assert!(matches!(result, RateLimitResult::Allowed { .. }));
    }

    #[test]
    fn test_set_limit_replaces_existing() {
        let mgr = default_manager();
        mgr.set_limit("c1", 100, 1.0);
        mgr.check_rate("c1", 50);
        // Replace — bucket resets to full
        mgr.set_limit("c1", 200, 2.0);
        let usage = mgr.get_usage("c1").unwrap();
        assert_eq!(usage.capacity, 200);
        assert_eq!(usage.remaining, 200);
        assert_eq!(usage.total_allowed, 0); // reset
    }
}
