//! CDC heartbeat for monitoring
//!
//! Produces periodic heartbeat messages so operators can detect when
//! a CDC connector has stalled or fallen behind.

use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;
use tracing::{debug, info};

/// Heartbeat configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatConfig {
    /// Interval between heartbeats (in milliseconds)
    pub interval_ms: u64,
    /// Topic to publish heartbeat messages to
    pub topic: String,
}

impl Default for HeartbeatConfig {
    fn default() -> Self {
        Self {
            interval_ms: 10_000,
            topic: "__streamline_heartbeat".to_string(),
        }
    }
}

/// A single heartbeat message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatMessage {
    /// Connector name
    pub connector: String,
    /// Timestamp when the heartbeat was emitted
    pub ts_ms: i64,
    /// Estimated lag between source database and connector (ms)
    pub source_lag_ms: u64,
    /// Total events processed since connector start
    pub events_processed: u64,
    /// Cumulative error count
    pub errors: u64,
    /// Last committed WAL/binlog position
    pub last_position: Option<String>,
}

/// CDC heartbeat emitter
pub struct CdcHeartbeat {
    config: HeartbeatConfig,
    connector_name: String,
    running: AtomicBool,
    stop_notify: Notify,
    // Counters updated externally via atomic operations
    source_lag_ms: AtomicU64,
    events_processed: AtomicU64,
    errors: AtomicU64,
    last_position: parking_lot::RwLock<Option<String>>,
    /// Collected heartbeats (for testing / in-process consumers)
    collected: parking_lot::RwLock<Vec<HeartbeatMessage>>,
}

impl CdcHeartbeat {
    /// Create a new heartbeat emitter
    pub fn new(connector_name: impl Into<String>, config: HeartbeatConfig) -> Arc<Self> {
        Arc::new(Self {
            config,
            connector_name: connector_name.into(),
            running: AtomicBool::new(false),
            stop_notify: Notify::new(),
            source_lag_ms: AtomicU64::new(0),
            events_processed: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            last_position: parking_lot::RwLock::new(None),
            collected: parking_lot::RwLock::new(Vec::new()),
        })
    }

    /// Start emitting heartbeats in the background.
    /// Returns immediately; call `stop()` to cancel.
    pub fn start(self: &Arc<Self>) {
        if self.running.swap(true, Ordering::SeqCst) {
            return; // already running
        }
        info!(
            connector = %self.connector_name,
            interval_ms = self.config.interval_ms,
            topic = %self.config.topic,
            "CDC heartbeat started"
        );

        let this = Arc::clone(self);
        tokio::spawn(async move {
            this.run_loop().await;
        });
    }

    /// Stop emitting heartbeats
    pub fn stop(&self) {
        if self.running.swap(false, Ordering::SeqCst) {
            self.stop_notify.notify_one();
            info!(connector = %self.connector_name, "CDC heartbeat stopped");
        }
    }

    /// Update the current source lag
    pub fn set_lag_ms(&self, lag: u64) {
        self.source_lag_ms.store(lag, Ordering::Relaxed);
    }

    /// Increment the events-processed counter
    pub fn add_events(&self, count: u64) {
        self.events_processed.fetch_add(count, Ordering::Relaxed);
    }

    /// Increment the error counter
    pub fn add_errors(&self, count: u64) {
        self.errors.fetch_add(count, Ordering::Relaxed);
    }

    /// Update the last committed position
    pub fn set_position(&self, pos: impl Into<String>) {
        *self.last_position.write() = Some(pos.into());
    }

    /// Build a heartbeat message from current counters
    pub fn build_message(&self) -> HeartbeatMessage {
        HeartbeatMessage {
            connector: self.connector_name.clone(),
            ts_ms: Utc::now().timestamp_millis(),
            source_lag_ms: self.source_lag_ms.load(Ordering::Relaxed),
            events_processed: self.events_processed.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            last_position: self.last_position.read().clone(),
        }
    }

    /// Get all collected heartbeat messages (for testing / introspection)
    pub fn collected_messages(&self) -> Vec<HeartbeatMessage> {
        self.collected.read().clone()
    }

    /// Get the heartbeat topic name
    pub fn topic(&self) -> &str {
        &self.config.topic
    }

    /// Whether the heartbeat loop is running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    async fn run_loop(&self) {
        let interval = tokio::time::Duration::from_millis(self.config.interval_ms);
        while self.running.load(Ordering::SeqCst) {
            tokio::select! {
                _ = tokio::time::sleep(interval) => {
                    let msg = self.build_message();
                    debug!(
                        connector = %msg.connector,
                        lag_ms = msg.source_lag_ms,
                        events = msg.events_processed,
                        "Heartbeat"
                    );
                    self.collected.write().push(msg);
                }
                _ = self.stop_notify.notified() => {
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_heartbeat_message_build() {
        let hb = CdcHeartbeat::new("test-connector", HeartbeatConfig::default());
        hb.set_lag_ms(150);
        hb.add_events(42);
        hb.add_errors(2);
        hb.set_position("0/FFAA00");

        let msg = hb.build_message();
        assert_eq!(msg.connector, "test-connector");
        assert_eq!(msg.source_lag_ms, 150);
        assert_eq!(msg.events_processed, 42);
        assert_eq!(msg.errors, 2);
        assert_eq!(msg.last_position.as_deref(), Some("0/FFAA00"));
    }

    #[test]
    fn test_heartbeat_config_default() {
        let cfg = HeartbeatConfig::default();
        assert_eq!(cfg.interval_ms, 10_000);
        assert_eq!(cfg.topic, "__streamline_heartbeat");
    }

    #[test]
    fn test_heartbeat_not_running_initially() {
        let hb = CdcHeartbeat::new("test", HeartbeatConfig::default());
        assert!(!hb.is_running());
    }

    #[tokio::test]
    async fn test_heartbeat_start_stop() {
        let hb = CdcHeartbeat::new(
            "test",
            HeartbeatConfig {
                interval_ms: 50,
                topic: "__test_hb".to_string(),
            },
        );
        hb.start();
        assert!(hb.is_running());

        // Let a couple heartbeats fire
        tokio::time::sleep(tokio::time::Duration::from_millis(130)).await;
        hb.stop();
        assert!(!hb.is_running());

        let msgs = hb.collected_messages();
        assert!(!msgs.is_empty());
    }

    #[test]
    fn test_heartbeat_json_serialization() {
        let msg = HeartbeatMessage {
            connector: "pg-main".to_string(),
            ts_ms: 1700000000000,
            source_lag_ms: 50,
            events_processed: 100,
            errors: 0,
            last_position: Some("0/FF".to_string()),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let parsed: HeartbeatMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.connector, "pg-main");
        assert_eq!(parsed.events_processed, 100);
    }
}
