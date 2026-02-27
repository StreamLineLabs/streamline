//! Scaling Metrics Engine
//!
//! Collects and exposes real-time scaling metrics used by KEDA, HPA, and the
//! operator's scale-to-zero controller to make autoscaling decisions.
//!
//! ## Endpoints
//!
//! - `GET /scaling/metrics` — JSON snapshot of all scaling-relevant metrics
//!
//! ## Tracked Metrics
//!
//! - Messages per second (produce + consume)
//! - Bytes per second
//! - Active client connections
//! - Total consumer lag
//! - Idle duration (seconds since last activity)
//! - Cooldown tracking for scale-down decisions

use axum::{extract::State, http::StatusCode, routing::get, Json, Router};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::debug;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Scaling metrics snapshot returned by the `/scaling/metrics` endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScalingMetrics {
    /// Messages processed per second (produce + consume combined)
    pub messages_per_second: f64,

    /// Bytes processed per second
    pub bytes_per_second: f64,

    /// Number of active client connections
    pub active_connections: u64,

    /// Total consumer lag across all consumer groups and partitions
    pub consumer_lag: i64,

    /// Seconds since the last observed activity (throughput > 0 or connections > 0)
    pub idle_duration_seconds: u64,

    /// Whether the collector considers the server idle
    pub is_idle: bool,

    /// Unix epoch (seconds) of the last recorded scale-up event
    pub last_scale_up_at: Option<u64>,

    /// Seconds remaining in the cooldown window (0 if not in cooldown)
    pub cooldown_remaining_seconds: u64,

    /// Unix epoch (seconds) when this snapshot was generated
    pub timestamp: u64,
}

/// Configuration for the [`ScalingMetricsCollector`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScalingMetricsConfig {
    /// Seconds of inactivity before the server is considered idle
    pub idle_threshold_seconds: u64,

    /// Cooldown period (seconds) after a scale-up event
    pub cooldown_seconds: u64,
}

impl Default for ScalingMetricsConfig {
    fn default() -> Self {
        Self {
            idle_threshold_seconds: 300,
            cooldown_seconds: 120,
        }
    }
}

// ---------------------------------------------------------------------------
// Collector
// ---------------------------------------------------------------------------

/// Thread-safe collector for scaling-relevant metrics.
///
/// Components across the server (Kafka protocol handler, HTTP handler, consumer
/// coordinator) call the `record_*` methods to update counters. The HTTP
/// endpoint reads a consistent snapshot via [`ScalingMetricsCollector::snapshot`].
pub struct ScalingMetricsCollector {
    config: ScalingMetricsConfig,

    // Counters — updated atomically by hot paths
    messages_per_second: AtomicU64,
    bytes_per_second: AtomicU64,
    active_connections: AtomicU64,
    consumer_lag: AtomicI64,

    /// Last activity timestamp (Unix epoch seconds)
    last_activity_at: AtomicU64,

    /// Last scale-up timestamp (Unix epoch seconds, 0 = never)
    last_scale_up_at: AtomicU64,
}

impl ScalingMetricsCollector {
    /// Create a new collector with the given configuration.
    pub fn new(config: ScalingMetricsConfig) -> Self {
        let now = now_epoch();
        Self {
            config,
            messages_per_second: AtomicU64::new(0),
            bytes_per_second: AtomicU64::new(0),
            active_connections: AtomicU64::new(0),
            consumer_lag: AtomicI64::new(0),
            last_activity_at: AtomicU64::new(now),
            last_scale_up_at: AtomicU64::new(0),
        }
    }

    // ---- Recording methods (called from hot paths) -----------------------

    /// Update the current messages-per-second gauge.
    pub fn record_messages_per_second(&self, mps: f64) {
        self.messages_per_second
            .store(mps.to_bits(), Ordering::Relaxed);
        if mps > 0.0 {
            self.touch_activity();
        }
    }

    /// Update the current bytes-per-second gauge.
    pub fn record_bytes_per_second(&self, bps: f64) {
        self.bytes_per_second
            .store(bps.to_bits(), Ordering::Relaxed);
        if bps > 0.0 {
            self.touch_activity();
        }
    }

    /// Set the current number of active connections.
    pub fn record_active_connections(&self, count: u64) {
        self.active_connections.store(count, Ordering::Relaxed);
        if count > 0 {
            self.touch_activity();
        }
    }

    /// Update the total consumer lag.
    pub fn record_consumer_lag(&self, lag: i64) {
        self.consumer_lag.store(lag, Ordering::Relaxed);
    }

    /// Record that a scale-up event occurred (resets cooldown timer).
    pub fn record_scale_up(&self) {
        self.last_scale_up_at.store(now_epoch(), Ordering::Relaxed);
    }

    // ---- Snapshot ---------------------------------------------------------

    /// Produce a point-in-time snapshot of all scaling metrics.
    pub fn snapshot(&self) -> ScalingMetrics {
        let now = now_epoch();

        let mps = f64::from_bits(self.messages_per_second.load(Ordering::Relaxed));
        let bps = f64::from_bits(self.bytes_per_second.load(Ordering::Relaxed));
        let connections = self.active_connections.load(Ordering::Relaxed);
        let lag = self.consumer_lag.load(Ordering::Relaxed);
        let last_activity = self.last_activity_at.load(Ordering::Relaxed);
        let last_scale_up = self.last_scale_up_at.load(Ordering::Relaxed);

        let idle_duration = now.saturating_sub(last_activity);
        let is_idle = mps == 0.0
            && connections == 0
            && idle_duration >= self.config.idle_threshold_seconds;

        let cooldown_remaining = if last_scale_up > 0 {
            let elapsed = now.saturating_sub(last_scale_up);
            self.config.cooldown_seconds.saturating_sub(elapsed)
        } else {
            0
        };

        ScalingMetrics {
            messages_per_second: mps,
            bytes_per_second: bps,
            active_connections: connections,
            consumer_lag: lag,
            idle_duration_seconds: idle_duration,
            is_idle,
            last_scale_up_at: if last_scale_up > 0 {
                Some(last_scale_up)
            } else {
                None
            },
            cooldown_remaining_seconds: cooldown_remaining,
            timestamp: now,
        }
    }

    // ---- Internal ---------------------------------------------------------

    fn touch_activity(&self) {
        self.last_activity_at.store(now_epoch(), Ordering::Relaxed);
    }
}

// ---------------------------------------------------------------------------
// Axum router & handler
// ---------------------------------------------------------------------------

/// Shared state for the scaling metrics API.
#[derive(Clone)]
pub struct ScalingMetricsApiState {
    pub collector: Arc<ScalingMetricsCollector>,
}

/// Create the axum [`Router`] that serves `/scaling/metrics`.
pub fn create_scaling_metrics_router(state: ScalingMetricsApiState) -> Router {
    Router::new()
        .route("/scaling/metrics", get(scaling_metrics_handler))
        .with_state(state)
}

/// `GET /scaling/metrics` — returns a JSON snapshot of all scaling metrics.
async fn scaling_metrics_handler(
    State(state): State<ScalingMetricsApiState>,
) -> Result<Json<ScalingMetrics>, StatusCode> {
    let snapshot = state.collector.snapshot();
    debug!(
        mps = snapshot.messages_per_second,
        connections = snapshot.active_connections,
        idle = snapshot.is_idle,
        "Serving scaling metrics snapshot",
    );
    Ok(Json(snapshot))
}

// ---------------------------------------------------------------------------
// Utility
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

    fn make_collector() -> ScalingMetricsCollector {
        ScalingMetricsCollector::new(ScalingMetricsConfig {
            idle_threshold_seconds: 10,
            cooldown_seconds: 60,
        })
    }

    #[test]
    fn test_default_config() {
        let cfg = ScalingMetricsConfig::default();
        assert_eq!(cfg.idle_threshold_seconds, 300);
        assert_eq!(cfg.cooldown_seconds, 120);
    }

    #[test]
    fn test_initial_snapshot_not_idle() {
        let collector = make_collector();
        let snap = collector.snapshot();

        // Just created — last_activity is "now" so idle_duration ≈ 0
        assert!(!snap.is_idle);
        assert_eq!(snap.active_connections, 0);
        assert_eq!(snap.consumer_lag, 0);
        assert!(snap.messages_per_second == 0.0);
    }

    #[test]
    fn test_record_messages_updates_activity() {
        let collector = make_collector();

        // Force last_activity into the past
        collector
            .last_activity_at
            .store(now_epoch() - 100, Ordering::Relaxed);

        // Record messages — should reset last_activity
        collector.record_messages_per_second(42.0);
        let snap = collector.snapshot();

        assert!((snap.messages_per_second - 42.0).abs() < f64::EPSILON);
        assert!(snap.idle_duration_seconds < 5);
    }

    #[test]
    fn test_record_bytes_updates_activity() {
        let collector = make_collector();

        collector
            .last_activity_at
            .store(now_epoch() - 100, Ordering::Relaxed);

        collector.record_bytes_per_second(1024.0);
        let snap = collector.snapshot();

        assert!((snap.bytes_per_second - 1024.0).abs() < f64::EPSILON);
        assert!(snap.idle_duration_seconds < 5);
    }

    #[test]
    fn test_record_connections_updates_activity() {
        let collector = make_collector();

        collector
            .last_activity_at
            .store(now_epoch() - 100, Ordering::Relaxed);

        collector.record_active_connections(3);
        let snap = collector.snapshot();

        assert_eq!(snap.active_connections, 3);
        assert!(snap.idle_duration_seconds < 5);
    }

    #[test]
    fn test_idle_detection() {
        let collector = make_collector();

        // Push last_activity far into the past
        collector
            .last_activity_at
            .store(now_epoch() - 100, Ordering::Relaxed);
        // Ensure mps and connections are zero (they already are by default)

        let snap = collector.snapshot();
        assert!(snap.is_idle);
        assert!(snap.idle_duration_seconds >= 10);
    }

    #[test]
    fn test_not_idle_with_connections() {
        let collector = make_collector();

        collector
            .last_activity_at
            .store(now_epoch() - 100, Ordering::Relaxed);

        // Even though last_activity is old, having active connections means not idle
        collector.record_active_connections(1);
        let snap = collector.snapshot();

        assert!(!snap.is_idle);
    }

    #[test]
    fn test_consumer_lag() {
        let collector = make_collector();
        collector.record_consumer_lag(5000);

        let snap = collector.snapshot();
        assert_eq!(snap.consumer_lag, 5000);
    }

    #[test]
    fn test_cooldown_tracking() {
        let collector = make_collector();

        // No scale-up yet
        let snap = collector.snapshot();
        assert_eq!(snap.cooldown_remaining_seconds, 0);
        assert!(snap.last_scale_up_at.is_none());

        // Record a scale-up
        collector.record_scale_up();
        let snap = collector.snapshot();

        assert!(snap.last_scale_up_at.is_some());
        // Cooldown should be ~60s (our config)
        assert!(snap.cooldown_remaining_seconds > 0);
        assert!(snap.cooldown_remaining_seconds <= 60);
    }

    #[test]
    fn test_cooldown_expired() {
        let collector = make_collector();

        // Simulate a scale-up that happened well in the past
        collector
            .last_scale_up_at
            .store(now_epoch() - 120, Ordering::Relaxed);

        let snap = collector.snapshot();
        assert_eq!(snap.cooldown_remaining_seconds, 0);
    }

    #[test]
    fn test_scaling_metrics_serialization() {
        let metrics = ScalingMetrics {
            messages_per_second: 123.4,
            bytes_per_second: 56789.0,
            active_connections: 10,
            consumer_lag: 42,
            idle_duration_seconds: 0,
            is_idle: false,
            last_scale_up_at: Some(1700000000),
            cooldown_remaining_seconds: 30,
            timestamp: 1700000060,
        };

        let json = serde_json::to_string(&metrics).unwrap();
        let deser: ScalingMetrics = serde_json::from_str(&json).unwrap();

        assert!((deser.messages_per_second - 123.4).abs() < f64::EPSILON);
        assert_eq!(deser.active_connections, 10);
        assert_eq!(deser.consumer_lag, 42);
        assert_eq!(deser.cooldown_remaining_seconds, 30);
    }

    #[test]
    fn test_zero_mps_does_not_update_activity() {
        let collector = make_collector();

        let past = now_epoch() - 100;
        collector.last_activity_at.store(past, Ordering::Relaxed);

        // Recording zero mps should NOT touch activity
        collector.record_messages_per_second(0.0);

        let stored = collector.last_activity_at.load(Ordering::Relaxed);
        assert_eq!(stored, past);
    }
}
