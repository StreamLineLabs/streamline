//! Smart Connection Pool
//!
//! Client-side connection pooling with load balancing and health-aware
//! routing.  Supports round-robin, least-connections, random, and
//! weighted-round-robin strategies.  Connections are tracked with lifetime
//! and idle timeouts, and periodic health checks automatically mark
//! unhealthy backends.

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Load-balancing strategy used when acquiring a connection.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum LoadBalanceStrategy {
    RoundRobin,
    LeastConnections,
    Random,
    WeightedRoundRobin,
}

/// Configuration for a [`ConnectionPool`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolConfig {
    #[serde(default = "default_min")]
    pub min_connections: usize,

    #[serde(default = "default_max")]
    pub max_connections: usize,

    #[serde(default = "default_idle_timeout")]
    pub idle_timeout_secs: u64,

    #[serde(default = "default_max_lifetime")]
    pub max_lifetime_secs: u64,

    #[serde(default = "default_health_interval")]
    pub health_check_interval_secs: u64,

    #[serde(default = "default_strategy")]
    pub load_balancing: LoadBalanceStrategy,
}

fn default_min() -> usize {
    2
}
fn default_max() -> usize {
    50
}
fn default_idle_timeout() -> u64 {
    300
}
fn default_max_lifetime() -> u64 {
    3600
}
fn default_health_interval() -> u64 {
    30
}
fn default_strategy() -> LoadBalanceStrategy {
    LoadBalanceStrategy::RoundRobin
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            min_connections: default_min(),
            max_connections: default_max(),
            idle_timeout_secs: default_idle_timeout(),
            max_lifetime_secs: default_max_lifetime(),
            health_check_interval_secs: default_health_interval(),
            load_balancing: default_strategy(),
        }
    }
}

// ---------------------------------------------------------------------------
// Connection types
// ---------------------------------------------------------------------------

/// Status of a single pooled connection.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ConnectionStatus {
    Available,
    InUse,
    Draining,
    Closed,
}

/// Health state of a connection / backend.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum HealthState {
    Healthy,
    Degraded,
    Unhealthy,
}

/// A connection managed by the pool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PooledConnection {
    pub id: String,
    /// Target address (`host:port`).
    pub target: String,
    pub status: ConnectionStatus,
    pub created_at: u64,
    pub last_used_at: u64,
    pub requests_served: u64,
    pub active_streams: u32,
    /// Weight for weighted round-robin balancing.
    pub weight: u32,
    pub health: HealthState,
}

// ---------------------------------------------------------------------------
// Pool statistics
// ---------------------------------------------------------------------------

/// Atomic counters for pool-level metrics.
#[derive(Debug, Default)]
pub struct PoolStats {
    pub total_connections: AtomicU64,
    pub active_connections: AtomicU64,
    pub idle_connections: AtomicU64,
    pub connections_created: AtomicU64,
    pub connections_closed: AtomicU64,
    pub requests_served: AtomicU64,
    pub health_check_failures: AtomicU64,
    /// Number of acquire calls that had to wait for a free connection.
    pub wait_count: AtomicU64,
}

/// Serialisable snapshot of [`PoolStats`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolStatsSnapshot {
    pub total_connections: u64,
    pub active_connections: u64,
    pub idle_connections: u64,
    pub connections_created: u64,
    pub connections_closed: u64,
    pub requests_served: u64,
    pub health_check_failures: u64,
    pub wait_count: u64,
}

// ---------------------------------------------------------------------------
// Pool implementation
// ---------------------------------------------------------------------------

/// Smart connection pool with health-aware load balancing.
pub struct ConnectionPool {
    connections: Arc<RwLock<Vec<PooledConnection>>>,
    config: PoolConfig,
    stats: Arc<PoolStats>,
    /// Monotonically increasing counter for round-robin indexing.
    rr_counter: AtomicU64,
}

impl ConnectionPool {
    /// Create a new, empty pool with the given configuration.
    pub fn new(config: PoolConfig) -> Self {
        info!(max = config.max_connections, strategy = ?config.load_balancing, "connection pool created");
        Self {
            connections: Arc::new(RwLock::new(Vec::new())),
            config,
            stats: Arc::new(PoolStats::default()),
            rr_counter: AtomicU64::new(0),
        }
    }

    /// Acquire a connection from the pool using the configured strategy.
    ///
    /// Returns the connection id on success.
    pub async fn acquire(&self) -> Result<String, String> {
        let mut conns = self.connections.write().await;

        let candidates: Vec<usize> = conns
            .iter()
            .enumerate()
            .filter(|(_, c)| {
                c.status == ConnectionStatus::Available && c.health != HealthState::Unhealthy
            })
            .map(|(i, _)| i)
            .collect();

        if candidates.is_empty() {
            self.stats.wait_count.fetch_add(1, Ordering::Relaxed);
            return Err("no available connections".to_string());
        }

        let idx = match self.config.load_balancing {
            LoadBalanceStrategy::RoundRobin => {
                let rr = self.rr_counter.fetch_add(1, Ordering::Relaxed) as usize;
                candidates[rr % candidates.len()]
            }
            LoadBalanceStrategy::LeastConnections => {
                *candidates
                    .iter()
                    .min_by_key(|&&i| conns[i].active_streams)
                    .unwrap()
            }
            LoadBalanceStrategy::Random => {
                // Deterministic-ish fallback without pulling in rand for this hot path.
                let pseudo = now_epoch() as usize;
                candidates[pseudo % candidates.len()]
            }
            LoadBalanceStrategy::WeightedRoundRobin => {
                let total_weight: u32 =
                    candidates.iter().map(|&i| conns[i].weight.max(1)).sum();
                let rr = self.rr_counter.fetch_add(1, Ordering::Relaxed) as u32;
                let target = rr % total_weight;
                let mut cumulative = 0u32;
                let mut chosen = candidates[0];
                for &i in &candidates {
                    cumulative += conns[i].weight.max(1);
                    if target < cumulative {
                        chosen = i;
                        break;
                    }
                }
                chosen
            }
        };

        let conn = &mut conns[idx];
        conn.status = ConnectionStatus::InUse;
        conn.last_used_at = now_epoch();
        conn.active_streams += 1;

        self.stats.active_connections.fetch_add(1, Ordering::Relaxed);
        self.stats.idle_connections.fetch_sub(1, Ordering::Relaxed);
        self.stats.requests_served.fetch_add(1, Ordering::Relaxed);

        debug!(id = %conn.id, target = %conn.target, "connection acquired");
        Ok(conn.id.clone())
    }

    /// Release a connection back to the pool.
    pub async fn release(&self, id: &str) {
        let mut conns = self.connections.write().await;
        if let Some(conn) = conns.iter_mut().find(|c| c.id == id) {
            conn.requests_served += 1;
            conn.active_streams = conn.active_streams.saturating_sub(1);
            if conn.active_streams == 0 {
                conn.status = ConnectionStatus::Available;
            }
            conn.last_used_at = now_epoch();

            self.stats.active_connections.fetch_sub(1, Ordering::Relaxed);
            self.stats.idle_connections.fetch_add(1, Ordering::Relaxed);
            debug!(id = %id, "connection released");
        } else {
            warn!(id = %id, "release called for unknown connection");
        }
    }

    /// Add a new backend target and create a connection for it.
    pub async fn add_target(&self, host: &str) {
        let mut conns = self.connections.write().await;
        if conns.len() >= self.config.max_connections {
            warn!(host = %host, "pool at capacity — cannot add target");
            return;
        }
        let now = now_epoch();
        let id = format!("conn-{}-{}", host.replace(':', "-"), now);
        let conn = PooledConnection {
            id: id.clone(),
            target: host.to_string(),
            status: ConnectionStatus::Available,
            created_at: now,
            last_used_at: now,
            requests_served: 0,
            active_streams: 0,
            weight: 1,
            health: HealthState::Healthy,
        };
        conns.push(conn);
        self.stats.total_connections.fetch_add(1, Ordering::Relaxed);
        self.stats.idle_connections.fetch_add(1, Ordering::Relaxed);
        self.stats.connections_created.fetch_add(1, Ordering::Relaxed);
        info!(id = %id, host = %host, "target added to pool");
    }

    /// Remove all connections for a given target.
    pub async fn remove_target(&self, host: &str) {
        let mut conns = self.connections.write().await;
        let before = conns.len();
        conns.retain(|c| c.target != host);
        let removed = before - conns.len();
        self.stats
            .total_connections
            .fetch_sub(removed as u64, Ordering::Relaxed);
        self.stats
            .connections_closed
            .fetch_add(removed as u64, Ordering::Relaxed);
        info!(host = %host, removed = removed, "target removed from pool");
    }

    /// Run a health check on every connection, marking unhealthy ones.
    pub async fn health_check_all(&self) {
        let mut conns = self.connections.write().await;
        for conn in conns.iter_mut() {
            if conn.status == ConnectionStatus::Closed {
                continue;
            }
            let age = now_epoch().saturating_sub(conn.created_at);
            let idle = now_epoch().saturating_sub(conn.last_used_at);

            if age > self.config.max_lifetime_secs {
                conn.health = HealthState::Unhealthy;
                conn.status = ConnectionStatus::Draining;
                self.stats
                    .health_check_failures
                    .fetch_add(1, Ordering::Relaxed);
                debug!(id = %conn.id, "connection exceeded max lifetime");
            } else if idle > self.config.idle_timeout_secs {
                conn.health = HealthState::Degraded;
                debug!(id = %conn.id, "connection idle too long");
            } else {
                conn.health = HealthState::Healthy;
            }
        }
    }

    /// Drain all connections for a given target (no new requests).
    pub async fn drain(&self, target: &str) {
        let mut conns = self.connections.write().await;
        for conn in conns.iter_mut().filter(|c| c.target == target) {
            conn.status = ConnectionStatus::Draining;
            debug!(id = %conn.id, "draining connection");
        }
    }

    /// Resize the maximum number of connections.
    pub async fn resize(&self, new_max: usize) {
        info!(old = self.config.max_connections, new = new_max, "resizing pool");
        // NOTE: config is owned, so we mutate via interior pattern in real
        // implementation. Here we trim excess connections.
        let mut conns = self.connections.write().await;
        while conns.len() > new_max {
            if let Some(pos) = conns
                .iter()
                .rposition(|c| c.status == ConnectionStatus::Available)
            {
                conns.remove(pos);
                self.stats
                    .total_connections
                    .fetch_sub(1, Ordering::Relaxed);
                self.stats
                    .connections_closed
                    .fetch_add(1, Ordering::Relaxed);
            } else {
                break;
            }
        }
    }

    /// Return a serialisable snapshot of pool statistics.
    pub fn stats(&self) -> PoolStatsSnapshot {
        PoolStatsSnapshot {
            total_connections: self.stats.total_connections.load(Ordering::Relaxed),
            active_connections: self.stats.active_connections.load(Ordering::Relaxed),
            idle_connections: self.stats.idle_connections.load(Ordering::Relaxed),
            connections_created: self.stats.connections_created.load(Ordering::Relaxed),
            connections_closed: self.stats.connections_closed.load(Ordering::Relaxed),
            requests_served: self.stats.requests_served.load(Ordering::Relaxed),
            health_check_failures: self.stats.health_check_failures.load(Ordering::Relaxed),
            wait_count: self.stats.wait_count.load(Ordering::Relaxed),
        }
    }

    /// Return a clone of all pooled connections (point-in-time snapshot).
    pub async fn snapshot(&self) -> Vec<PooledConnection> {
        self.connections.read().await.clone()
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn now_epoch() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn default_pool() -> ConnectionPool {
        ConnectionPool::new(PoolConfig::default())
    }

    async fn pool_with_targets(hosts: &[&str]) -> ConnectionPool {
        let pool = default_pool();
        for h in hosts {
            pool.add_target(h).await;
        }
        pool
    }

    // 1
    #[test]
    fn test_default_config() {
        let cfg = PoolConfig::default();
        assert_eq!(cfg.min_connections, 2);
        assert_eq!(cfg.max_connections, 50);
        assert_eq!(cfg.idle_timeout_secs, 300);
        assert_eq!(cfg.max_lifetime_secs, 3600);
        assert_eq!(cfg.health_check_interval_secs, 30);
        assert_eq!(cfg.load_balancing, LoadBalanceStrategy::RoundRobin);
    }

    // 2
    #[tokio::test]
    async fn test_add_target() {
        let pool = default_pool();
        pool.add_target("localhost:9092").await;
        let snap = pool.snapshot().await;
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].target, "localhost:9092");
        assert_eq!(snap[0].status, ConnectionStatus::Available);
    }

    // 3
    #[tokio::test]
    async fn test_acquire_returns_id() {
        let pool = pool_with_targets(&["localhost:9092"]).await;
        let id = pool.acquire().await.unwrap();
        assert!(!id.is_empty());
    }

    // 4
    #[tokio::test]
    async fn test_acquire_marks_in_use() {
        let pool = pool_with_targets(&["localhost:9092"]).await;
        let id = pool.acquire().await.unwrap();
        let snap = pool.snapshot().await;
        let conn = snap.iter().find(|c| c.id == id).unwrap();
        assert_eq!(conn.status, ConnectionStatus::InUse);
    }

    // 5
    #[tokio::test]
    async fn test_acquire_fails_when_empty() {
        let pool = default_pool();
        let result = pool.acquire().await;
        assert!(result.is_err());
    }

    // 6
    #[tokio::test]
    async fn test_release_makes_available() {
        let pool = pool_with_targets(&["localhost:9092"]).await;
        let id = pool.acquire().await.unwrap();
        pool.release(&id).await;
        let snap = pool.snapshot().await;
        let conn = snap.iter().find(|c| c.id == id).unwrap();
        assert_eq!(conn.status, ConnectionStatus::Available);
    }

    // 7
    #[tokio::test]
    async fn test_release_increments_requests_served() {
        let pool = pool_with_targets(&["localhost:9092"]).await;
        let id = pool.acquire().await.unwrap();
        pool.release(&id).await;
        let snap = pool.snapshot().await;
        let conn = snap.iter().find(|c| c.id == id).unwrap();
        assert_eq!(conn.requests_served, 1);
    }

    // 8
    #[tokio::test]
    async fn test_remove_target() {
        let pool = pool_with_targets(&["a:1", "b:2"]).await;
        pool.remove_target("a:1").await;
        let snap = pool.snapshot().await;
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].target, "b:2");
    }

    // 9
    #[tokio::test]
    async fn test_drain_target() {
        let pool = pool_with_targets(&["host:1"]).await;
        pool.drain("host:1").await;
        let snap = pool.snapshot().await;
        assert_eq!(snap[0].status, ConnectionStatus::Draining);
    }

    // 10
    #[tokio::test]
    async fn test_drain_does_not_affect_others() {
        let pool = pool_with_targets(&["a:1", "b:2"]).await;
        pool.drain("a:1").await;
        let snap = pool.snapshot().await;
        let b = snap.iter().find(|c| c.target == "b:2").unwrap();
        assert_eq!(b.status, ConnectionStatus::Available);
    }

    // 11
    #[tokio::test]
    async fn test_health_check_marks_degraded_on_idle() {
        let mut cfg = PoolConfig::default();
        cfg.idle_timeout_secs = 0; // immediate idle
        let pool = ConnectionPool::new(cfg);
        pool.add_target("host:1").await;
        // Sleep briefly so last_used_at < now
        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
        pool.health_check_all().await;
        let snap = pool.snapshot().await;
        assert_eq!(snap[0].health, HealthState::Degraded);
    }

    // 12
    #[tokio::test]
    async fn test_health_check_marks_unhealthy_on_lifetime() {
        let mut cfg = PoolConfig::default();
        cfg.max_lifetime_secs = 0; // immediate expiry
        let pool = ConnectionPool::new(cfg);
        pool.add_target("host:1").await;
        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
        pool.health_check_all().await;
        let snap = pool.snapshot().await;
        assert_eq!(snap[0].health, HealthState::Unhealthy);
        assert_eq!(snap[0].status, ConnectionStatus::Draining);
    }

    // 13
    #[tokio::test]
    async fn test_unhealthy_connections_not_acquired() {
        let pool = pool_with_targets(&["host:1"]).await;
        {
            let mut conns = pool.connections.write().await;
            conns[0].health = HealthState::Unhealthy;
        }
        let result = pool.acquire().await;
        assert!(result.is_err());
    }

    // 14
    #[tokio::test]
    async fn test_stats_snapshot() {
        let pool = pool_with_targets(&["host:1"]).await;
        let id = pool.acquire().await.unwrap();
        pool.release(&id).await;
        let s = pool.stats();
        assert_eq!(s.total_connections, 1);
        assert_eq!(s.connections_created, 1);
        assert_eq!(s.requests_served, 1);
    }

    // 15
    #[tokio::test]
    async fn test_resize_trims_connections() {
        let pool = pool_with_targets(&["a:1", "b:2", "c:3"]).await;
        pool.resize(1).await;
        let snap = pool.snapshot().await;
        assert_eq!(snap.len(), 1);
    }

    // 16
    #[tokio::test]
    async fn test_add_target_respects_max() {
        let mut cfg = PoolConfig::default();
        cfg.max_connections = 1;
        let pool = ConnectionPool::new(cfg);
        pool.add_target("a:1").await;
        pool.add_target("b:2").await; // should be ignored
        let snap = pool.snapshot().await;
        assert_eq!(snap.len(), 1);
    }

    // 17
    #[tokio::test]
    async fn test_least_connections_strategy() {
        let mut cfg = PoolConfig::default();
        cfg.load_balancing = LoadBalanceStrategy::LeastConnections;
        let pool = ConnectionPool::new(cfg);
        pool.add_target("a:1").await;
        pool.add_target("b:2").await;

        // Acquire from first, release, acquire again — should pick the other
        // one because the first now has active_streams=1 after acquire.
        let id1 = pool.acquire().await.unwrap();
        let id2 = pool.acquire().await.unwrap();
        assert_ne!(id1, id2);
    }

    // 18
    #[test]
    fn test_config_serde_roundtrip() {
        let cfg = PoolConfig::default();
        let json = serde_json::to_string(&cfg).unwrap();
        let back: PoolConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(back.max_connections, cfg.max_connections);
        assert_eq!(back.load_balancing, cfg.load_balancing);
    }

    // 19
    #[tokio::test]
    async fn test_wait_count_incremented() {
        let pool = default_pool();
        let _ = pool.acquire().await;
        let s = pool.stats();
        assert_eq!(s.wait_count, 1);
    }

    // 20
    #[tokio::test]
    async fn test_snapshot_returns_clone() {
        let pool = pool_with_targets(&["host:1"]).await;
        let snap1 = pool.snapshot().await;
        pool.add_target("host:2").await;
        let snap2 = pool.snapshot().await;
        assert_eq!(snap1.len(), 1);
        assert_eq!(snap2.len(), 2);
    }

    // 21
    #[tokio::test]
    async fn test_multiple_acquire_release_cycles() {
        let pool = pool_with_targets(&["host:1"]).await;
        for _ in 0..5 {
            let id = pool.acquire().await.unwrap();
            pool.release(&id).await;
        }
        let s = pool.stats();
        assert_eq!(s.requests_served, 5);
    }

    // 22
    #[tokio::test]
    async fn test_connections_closed_stat() {
        let pool = pool_with_targets(&["a:1", "b:2"]).await;
        pool.remove_target("a:1").await;
        let s = pool.stats();
        assert_eq!(s.connections_closed, 1);
    }
}
