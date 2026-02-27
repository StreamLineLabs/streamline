//! Watermark tracking for event-time processing
//!
//! Maintains per-topic watermarks with support for bounded out-of-orderness,
//! periodic emission, late event detection, and idle source handling.
//!
//! ## Concepts
//!
//! - **Watermark**: a monotonically advancing timestamp asserting that no
//!   events with a timestamp *earlier* than the watermark will arrive.
//! - **Bounded out-of-orderness**: events may arrive up to `max_lateness_ms`
//!   behind the highest observed timestamp before being considered late.
//! - **Idle sources**: when a topic produces no events for
//!   `idle_timeout_ms`, the tracker synthesises watermark advances so
//!   downstream windows can still close.

use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, warn};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the watermark tracker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatermarkConfig {
    /// Maximum allowed out-of-orderness in milliseconds.
    /// Events arriving more than this amount behind the current watermark are
    /// counted as late.
    #[serde(default = "default_max_lateness_ms")]
    pub max_lateness_ms: u64,

    /// How often (in milliseconds) watermarks are emitted / advanced.
    #[serde(default = "default_emit_interval_ms")]
    pub emit_interval_ms: u64,

    /// If no events arrive within this many milliseconds the source is
    /// considered idle and the watermark is advanced to current wall-clock
    /// time minus `max_lateness_ms`.
    #[serde(default = "default_idle_timeout_ms")]
    pub idle_timeout_ms: u64,
}

fn default_max_lateness_ms() -> u64 {
    5_000
}
fn default_emit_interval_ms() -> u64 {
    1_000
}
fn default_idle_timeout_ms() -> u64 {
    30_000
}

impl Default for WatermarkConfig {
    fn default() -> Self {
        Self {
            max_lateness_ms: default_max_lateness_ms(),
            emit_interval_ms: default_emit_interval_ms(),
            idle_timeout_ms: default_idle_timeout_ms(),
        }
    }
}

// ---------------------------------------------------------------------------
// Per-topic state
// ---------------------------------------------------------------------------

/// Tracking state for a single topic (or partition).
#[derive(Debug, Clone, Serialize)]
pub struct TopicWatermarkState {
    /// Current watermark timestamp (ms since epoch).
    pub watermark_ms: i64,
    /// Highest event timestamp observed so far.
    pub max_event_time_ms: i64,
    /// Total number of events observed.
    pub events_observed: u64,
    /// Number of late events (arrived behind the watermark).
    pub late_events: u64,
    /// Whether this source is currently considered idle.
    pub is_idle: bool,
}

/// Internal mutable state kept under the lock.
#[derive(Debug)]
struct TopicState {
    /// Current watermark (ms since epoch).
    watermark_ms: i64,
    /// Highest event-time seen.
    max_event_time_ms: i64,
    /// Counters.
    events_observed: u64,
    late_events: u64,
    /// Wall-clock instant of the last received event.
    last_event_at: Instant,
    /// Wall-clock instant of the last watermark emission.
    last_emit_at: Instant,
    /// Whether the source is idle.
    is_idle: bool,
}

impl TopicState {
    fn new() -> Self {
        let now = Instant::now();
        Self {
            watermark_ms: i64::MIN,
            max_event_time_ms: i64::MIN,
            events_observed: 0,
            late_events: 0,
            last_event_at: now,
            last_emit_at: now,
            is_idle: false,
        }
    }

    fn snapshot(&self) -> TopicWatermarkState {
        TopicWatermarkState {
            watermark_ms: self.watermark_ms,
            max_event_time_ms: self.max_event_time_ms,
            events_observed: self.events_observed,
            late_events: self.late_events,
            is_idle: self.is_idle,
        }
    }
}

// ---------------------------------------------------------------------------
// WatermarkTracker
// ---------------------------------------------------------------------------

/// Manages watermarks for multiple topics.
pub struct WatermarkTracker {
    config: WatermarkConfig,
    topics: Arc<RwLock<HashMap<String, TopicState>>>,
}

impl WatermarkTracker {
    /// Create a new tracker with the given configuration.
    pub fn new(config: WatermarkConfig) -> Self {
        Self {
            config,
            topics: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Observe an event for the given topic and return whether it is late.
    ///
    /// This updates the per-topic max event time and, if enough wall-clock
    /// time has elapsed, advances the watermark.
    pub async fn observe_event(&self, topic: &str, event_time_ms: i64) -> Result<bool> {
        let mut topics = self.topics.write().await;
        let state = topics
            .entry(topic.to_string())
            .or_insert_with(TopicState::new);

        state.events_observed += 1;
        state.last_event_at = Instant::now();
        state.is_idle = false;

        // Track the maximum event time.
        if event_time_ms > state.max_event_time_ms {
            state.max_event_time_ms = event_time_ms;
        }

        // Determine if this event is late (behind the current watermark).
        let is_late = state.watermark_ms != i64::MIN && event_time_ms < state.watermark_ms;
        if is_late {
            state.late_events += 1;
            debug!(
                topic,
                event_time_ms,
                watermark_ms = state.watermark_ms,
                "Late event detected"
            );
        }

        // Advance the watermark if the emit interval has elapsed.
        let emit_interval = Duration::from_millis(self.config.emit_interval_ms);
        if state.last_emit_at.elapsed() >= emit_interval {
            let new_watermark =
                state.max_event_time_ms - self.config.max_lateness_ms as i64;
            if new_watermark > state.watermark_ms {
                state.watermark_ms = new_watermark;
                debug!(
                    topic,
                    watermark_ms = state.watermark_ms,
                    "Watermark advanced"
                );
            }
            state.last_emit_at = Instant::now();
        }

        Ok(is_late)
    }

    /// Advance watermarks for idle sources.
    ///
    /// Call this periodically (e.g. from a timer task). For every topic
    /// that has not received events within `idle_timeout_ms`, the watermark
    /// is advanced to `now - max_lateness_ms` (using the system clock as a
    /// proxy for event time).
    pub async fn advance_idle_watermarks(&self, now_ms: i64) {
        let idle_timeout = Duration::from_millis(self.config.idle_timeout_ms);
        let mut topics = self.topics.write().await;

        for (topic, state) in topics.iter_mut() {
            if state.last_event_at.elapsed() >= idle_timeout && !state.is_idle {
                state.is_idle = true;
                let new_watermark = now_ms - self.config.max_lateness_ms as i64;
                if new_watermark > state.watermark_ms {
                    state.watermark_ms = new_watermark;
                    warn!(
                        topic = topic.as_str(),
                        watermark_ms = state.watermark_ms,
                        "Idle source detected — watermark advanced"
                    );
                }
            }
        }
    }

    /// Force-advance the watermark for a topic to a specific timestamp.
    pub async fn force_advance(&self, topic: &str, watermark_ms: i64) -> Result<()> {
        let mut topics = self.topics.write().await;
        let state = topics
            .get_mut(topic)
            .ok_or_else(|| StreamlineError::Query(format!("Topic '{}' not tracked", topic)))?;

        if watermark_ms < state.watermark_ms {
            return Err(StreamlineError::Query(
                "Cannot move watermark backwards".to_string(),
            ));
        }

        state.watermark_ms = watermark_ms;
        state.last_emit_at = Instant::now();
        Ok(())
    }

    /// Get the current watermark state for a topic.
    pub async fn get_watermark(&self, topic: &str) -> Option<TopicWatermarkState> {
        let topics = self.topics.read().await;
        topics.get(topic).map(|s| s.snapshot())
    }

    /// Get watermark states for all tracked topics.
    pub async fn all_watermarks(&self) -> HashMap<String, TopicWatermarkState> {
        let topics = self.topics.read().await;
        topics
            .iter()
            .map(|(k, v)| (k.clone(), v.snapshot()))
            .collect()
    }

    /// Remove tracking state for a topic.
    pub async fn remove_topic(&self, topic: &str) -> bool {
        let mut topics = self.topics.write().await;
        topics.remove(topic).is_some()
    }

    /// Return the tracker configuration.
    pub fn config(&self) -> &WatermarkConfig {
        &self.config
    }
}

impl Default for WatermarkTracker {
    fn default() -> Self {
        Self::new(WatermarkConfig::default())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn tracker_with_config(max_lateness_ms: u64, emit_interval_ms: u64) -> WatermarkTracker {
        WatermarkTracker::new(WatermarkConfig {
            max_lateness_ms,
            emit_interval_ms,
            idle_timeout_ms: 100, // short for tests
        })
    }

    #[tokio::test]
    async fn test_default_config() {
        let cfg = WatermarkConfig::default();
        assert_eq!(cfg.max_lateness_ms, 5_000);
        assert_eq!(cfg.emit_interval_ms, 1_000);
        assert_eq!(cfg.idle_timeout_ms, 30_000);
    }

    #[tokio::test]
    async fn test_observe_event_tracks_max() {
        let tracker = tracker_with_config(1_000, 0);

        tracker.observe_event("t1", 100).await.unwrap();
        tracker.observe_event("t1", 300).await.unwrap();
        tracker.observe_event("t1", 200).await.unwrap();

        let state = tracker.get_watermark("t1").await.unwrap();
        assert_eq!(state.max_event_time_ms, 300);
        assert_eq!(state.events_observed, 3);
    }

    #[tokio::test]
    async fn test_watermark_advances_with_zero_emit_interval() {
        // emit_interval_ms = 0 means advance on every event
        let tracker = tracker_with_config(100, 0);

        tracker.observe_event("t1", 1_000).await.unwrap();
        // Watermark should be max_event_time - max_lateness = 1000 - 100 = 900
        let state = tracker.get_watermark("t1").await.unwrap();
        assert_eq!(state.watermark_ms, 900);

        tracker.observe_event("t1", 2_000).await.unwrap();
        let state = tracker.get_watermark("t1").await.unwrap();
        assert_eq!(state.watermark_ms, 1_900);
    }

    #[tokio::test]
    async fn test_late_event_detection() {
        let tracker = tracker_with_config(100, 0);

        // Advance watermark to 900
        tracker.observe_event("t1", 1_000).await.unwrap();

        // Event at 800 < watermark 900 → late
        let is_late = tracker.observe_event("t1", 800).await.unwrap();
        assert!(is_late);

        let state = tracker.get_watermark("t1").await.unwrap();
        assert_eq!(state.late_events, 1);
    }

    #[tokio::test]
    async fn test_on_time_event() {
        let tracker = tracker_with_config(100, 0);

        tracker.observe_event("t1", 1_000).await.unwrap();
        // Event at 950 >= watermark 900 → on time
        let is_late = tracker.observe_event("t1", 950).await.unwrap();
        assert!(!is_late);

        let state = tracker.get_watermark("t1").await.unwrap();
        assert_eq!(state.late_events, 0);
    }

    #[tokio::test]
    async fn test_multiple_topics_independent() {
        let tracker = tracker_with_config(100, 0);

        tracker.observe_event("t1", 5_000).await.unwrap();
        tracker.observe_event("t2", 1_000).await.unwrap();

        let s1 = tracker.get_watermark("t1").await.unwrap();
        let s2 = tracker.get_watermark("t2").await.unwrap();
        assert_eq!(s1.watermark_ms, 4_900);
        assert_eq!(s2.watermark_ms, 900);
    }

    #[tokio::test]
    async fn test_get_watermark_unknown_topic() {
        let tracker = WatermarkTracker::default();
        assert!(tracker.get_watermark("unknown").await.is_none());
    }

    #[tokio::test]
    async fn test_all_watermarks() {
        let tracker = tracker_with_config(0, 0);

        tracker.observe_event("a", 10).await.unwrap();
        tracker.observe_event("b", 20).await.unwrap();

        let all = tracker.all_watermarks().await;
        assert_eq!(all.len(), 2);
        assert!(all.contains_key("a"));
        assert!(all.contains_key("b"));
    }

    #[tokio::test]
    async fn test_remove_topic() {
        let tracker = tracker_with_config(0, 0);

        tracker.observe_event("t1", 100).await.unwrap();
        assert!(tracker.remove_topic("t1").await);
        assert!(tracker.get_watermark("t1").await.is_none());
        // Removing again returns false
        assert!(!tracker.remove_topic("t1").await);
    }

    #[tokio::test]
    async fn test_force_advance() {
        let tracker = tracker_with_config(100, 0);

        tracker.observe_event("t1", 1_000).await.unwrap();
        tracker.force_advance("t1", 5_000).await.unwrap();

        let state = tracker.get_watermark("t1").await.unwrap();
        assert_eq!(state.watermark_ms, 5_000);
    }

    #[tokio::test]
    async fn test_force_advance_backwards_fails() {
        let tracker = tracker_with_config(100, 0);

        tracker.observe_event("t1", 1_000).await.unwrap();
        // watermark is at 900
        let err = tracker.force_advance("t1", 500).await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn test_force_advance_unknown_topic_fails() {
        let tracker = WatermarkTracker::default();
        let err = tracker.force_advance("nope", 100).await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn test_idle_source_detection() {
        let tracker = WatermarkTracker::new(WatermarkConfig {
            max_lateness_ms: 100,
            emit_interval_ms: 0,
            idle_timeout_ms: 50, // 50ms idle timeout
        });

        tracker.observe_event("t1", 1_000).await.unwrap();
        let state = tracker.get_watermark("t1").await.unwrap();
        assert!(!state.is_idle);

        // Wait for the idle timeout to elapse
        tokio::time::sleep(Duration::from_millis(80)).await;

        let wall_clock_ms = 10_000i64;
        tracker.advance_idle_watermarks(wall_clock_ms).await;

        let state = tracker.get_watermark("t1").await.unwrap();
        assert!(state.is_idle);
        // Watermark should have advanced to wall_clock - max_lateness
        assert_eq!(state.watermark_ms, wall_clock_ms - 100);
    }

    #[tokio::test]
    async fn test_idle_flag_resets_on_new_event() {
        let tracker = WatermarkTracker::new(WatermarkConfig {
            max_lateness_ms: 100,
            emit_interval_ms: 0,
            idle_timeout_ms: 50,
        });

        tracker.observe_event("t1", 1_000).await.unwrap();
        tokio::time::sleep(Duration::from_millis(80)).await;
        tracker.advance_idle_watermarks(10_000).await;

        let state = tracker.get_watermark("t1").await.unwrap();
        assert!(state.is_idle);

        // New event resets the idle flag
        tracker.observe_event("t1", 11_000).await.unwrap();
        let state = tracker.get_watermark("t1").await.unwrap();
        assert!(!state.is_idle);
    }

    #[tokio::test]
    async fn test_watermark_monotonic() {
        let tracker = tracker_with_config(100, 0);

        tracker.observe_event("t1", 5_000).await.unwrap();
        let w1 = tracker.get_watermark("t1").await.unwrap().watermark_ms;

        // Observe an older event — watermark must not go backwards
        tracker.observe_event("t1", 3_000).await.unwrap();
        let w2 = tracker.get_watermark("t1").await.unwrap().watermark_ms;
        assert!(w2 >= w1);
    }

    #[tokio::test]
    async fn test_config_accessor() {
        let cfg = WatermarkConfig {
            max_lateness_ms: 42,
            emit_interval_ms: 7,
            idle_timeout_ms: 99,
        };
        let tracker = WatermarkTracker::new(cfg);
        assert_eq!(tracker.config().max_lateness_ms, 42);
        assert_eq!(tracker.config().emit_interval_ms, 7);
        assert_eq!(tracker.config().idle_timeout_ms, 99);
    }
}
