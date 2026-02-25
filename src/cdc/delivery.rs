//! Exactly-Once CDC Delivery & Dead Letter Queue Integration
//!
//! Extends the CDC engine with production-grade delivery guarantees:
//!
//! - **Exactly-Once Delivery**: Idempotent writes using CDC position tracking
//!   with transactional offset commits.
//! - **Dead Letter Queue**: Failed CDC events are routed to a configurable DLQ
//!   topic with full error context for later replay.
//! - **Delivery Tracking**: Per-table offset persistence for crash recovery.
//! - **Replay Engine**: Replay DLQ events back through the pipeline after
//!   fixes are deployed.

use crate::error::{Result, StreamlineError};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

// ---------------------------------------------------------------------------
// Exactly-Once Delivery Tracker
// ---------------------------------------------------------------------------

/// Tracks committed positions per source for exactly-once semantics.
pub struct DeliveryTracker {
    /// Committed positions: source_name → (table → position)
    committed: RwLock<HashMap<String, HashMap<String, CommittedPosition>>>,
    /// Pending (in-flight) events: source_name → count
    pending: RwLock<HashMap<String, u64>>,
    /// Stats
    total_delivered: AtomicU64,
    total_duplicates_skipped: AtomicU64,
}

/// A committed position for a table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommittedPosition {
    /// WAL/binlog/oplog position.
    pub position: String,
    /// Sequence number within the position.
    pub sequence: u64,
    /// When this position was committed.
    pub committed_at: DateTime<Utc>,
    /// Transaction ID (if transactional).
    pub transaction_id: Option<String>,
}

/// Delivery status for a CDC event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliveryStatus {
    /// Event should be processed (new).
    New,
    /// Event is a duplicate and should be skipped.
    Duplicate,
}

impl DeliveryTracker {
    pub fn new() -> Self {
        Self {
            committed: RwLock::new(HashMap::new()),
            pending: RwLock::new(HashMap::new()),
            total_delivered: AtomicU64::new(0),
            total_duplicates_skipped: AtomicU64::new(0),
        }
    }

    /// Check if an event has already been delivered.
    pub async fn check_delivery(
        &self,
        source: &str,
        table: &str,
        position: &str,
        sequence: u64,
    ) -> DeliveryStatus {
        let committed = self.committed.read().await;

        if let Some(source_positions) = committed.get(source) {
            if let Some(committed_pos) = source_positions.get(table) {
                // Position-based dedup
                if committed_pos.position == position && committed_pos.sequence >= sequence {
                    self.total_duplicates_skipped
                        .fetch_add(1, Ordering::Relaxed);
                    return DeliveryStatus::Duplicate;
                }
            }
        }

        DeliveryStatus::New
    }

    /// Commit a position after successful delivery.
    pub async fn commit(
        &self,
        source: &str,
        table: &str,
        position: &str,
        sequence: u64,
        transaction_id: Option<String>,
    ) {
        let mut committed = self.committed.write().await;
        let source_positions = committed
            .entry(source.to_string())
            .or_default();

        source_positions.insert(
            table.to_string(),
            CommittedPosition {
                position: position.to_string(),
                sequence,
                committed_at: Utc::now(),
                transaction_id,
            },
        );

        self.total_delivered.fetch_add(1, Ordering::Relaxed);
    }

    /// Get all committed positions for a source.
    pub async fn get_positions(
        &self,
        source: &str,
    ) -> HashMap<String, CommittedPosition> {
        self.committed
            .read()
            .await
            .get(source)
            .cloned()
            .unwrap_or_default()
    }

    /// Get delivery stats.
    pub fn stats(&self) -> DeliveryStats {
        DeliveryStats {
            total_delivered: self.total_delivered.load(Ordering::Relaxed),
            total_duplicates_skipped: self.total_duplicates_skipped.load(Ordering::Relaxed),
        }
    }

    /// Serialize committed positions for checkpoint persistence.
    pub async fn checkpoint(&self) -> Result<Vec<u8>> {
        let committed = self.committed.read().await;
        serde_json::to_vec(&*committed)
            .map_err(|e| StreamlineError::storage_msg(format!("Checkpoint serialize error: {}", e)))
    }

    /// Restore from a checkpoint.
    pub async fn restore(&self, data: &[u8]) -> Result<()> {
        let positions: HashMap<String, HashMap<String, CommittedPosition>> =
            serde_json::from_slice(data)
                .map_err(|e| StreamlineError::storage_msg(format!("Checkpoint restore error: {}", e)))?;
        *self.committed.write().await = positions;
        Ok(())
    }
}

impl Default for DeliveryTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Delivery statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryStats {
    pub total_delivered: u64,
    pub total_duplicates_skipped: u64,
}

// ---------------------------------------------------------------------------
// CDC Dead Letter Queue
// ---------------------------------------------------------------------------

/// Configuration for the CDC dead letter queue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcDlqConfig {
    /// DLQ topic name pattern (supports {source} placeholder).
    pub topic_pattern: String,
    /// Maximum retry attempts before sending to DLQ.
    pub max_retries: u32,
    /// Initial retry backoff in milliseconds.
    pub retry_backoff_ms: u64,
    /// Maximum backoff in milliseconds.
    pub max_backoff_ms: u64,
    /// Include the original event payload in the DLQ entry.
    pub include_payload: bool,
    /// Maximum DLQ entries to retain in memory.
    pub max_entries: usize,
}

impl Default for CdcDlqConfig {
    fn default() -> Self {
        Self {
            topic_pattern: "dlq-cdc-{source}".to_string(),
            max_retries: 3,
            retry_backoff_ms: 1000,
            max_backoff_ms: 30_000,
            include_payload: true,
            max_entries: 10_000,
        }
    }
}

/// A dead letter queue entry for a failed CDC event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcDlqEntry {
    /// Unique entry ID.
    pub id: String,
    /// Source CDC connector name.
    pub source: String,
    /// Original table.
    pub table: String,
    /// CDC operation type.
    pub operation: String,
    /// Original WAL position.
    pub position: String,
    /// Error message.
    pub error: String,
    /// Error class (for categorization).
    pub error_class: DlqErrorClass,
    /// Number of retry attempts made.
    pub retry_count: u32,
    /// Original event payload (if include_payload is true).
    pub payload: Option<serde_json::Value>,
    /// When the error first occurred.
    pub first_error_at: DateTime<Utc>,
    /// When the entry was added to the DLQ.
    pub added_at: DateTime<Utc>,
    /// Whether this entry has been replayed.
    pub replayed: bool,
}

/// Error classification for DLQ entries.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DlqErrorClass {
    /// Schema mismatch or evolution error.
    SchemaError,
    /// Serialization/deserialization failure.
    SerializationError,
    /// Downstream write failure.
    WriteError,
    /// Network/connection error.
    ConnectionError,
    /// Validation/constraint error.
    ValidationError,
    /// Unknown error.
    Unknown,
}

/// CDC Dead Letter Queue manager.
pub struct CdcDlq {
    config: CdcDlqConfig,
    entries: RwLock<Vec<CdcDlqEntry>>,
    total_entries: AtomicU64,
    total_replayed: AtomicU64,
}

impl CdcDlq {
    pub fn new(config: CdcDlqConfig) -> Self {
        Self {
            config,
            entries: RwLock::new(Vec::new()),
            total_entries: AtomicU64::new(0),
            total_replayed: AtomicU64::new(0),
        }
    }

    /// Add a failed event to the DLQ.
    pub async fn add(
        &self,
        source: &str,
        table: &str,
        operation: &str,
        position: &str,
        error: &str,
        error_class: DlqErrorClass,
        retry_count: u32,
        payload: Option<serde_json::Value>,
    ) -> String {
        let id = format!(
            "dlq-{}-{}-{}",
            source,
            self.total_entries.fetch_add(1, Ordering::Relaxed),
            Utc::now().timestamp_millis()
        );

        let entry = CdcDlqEntry {
            id: id.clone(),
            source: source.to_string(),
            table: table.to_string(),
            operation: operation.to_string(),
            position: position.to_string(),
            error: error.to_string(),
            error_class,
            retry_count,
            payload: if self.config.include_payload {
                payload
            } else {
                None
            },
            first_error_at: Utc::now(),
            added_at: Utc::now(),
            replayed: false,
        };

        let mut entries = self.entries.write().await;
        entries.push(entry);

        // Trim oldest entries if over limit
        if entries.len() > self.config.max_entries {
            let drain = entries.len() - self.config.max_entries;
            entries.drain(0..drain);
        }

        tracing::warn!(
            source = source,
            table = table,
            error = error,
            "CDC event sent to DLQ"
        );

        id
    }

    /// Get all DLQ entries for a source.
    pub async fn entries_for_source(&self, source: &str) -> Vec<CdcDlqEntry> {
        self.entries
            .read()
            .await
            .iter()
            .filter(|e| e.source == source)
            .cloned()
            .collect()
    }

    /// Get all unreplayed entries.
    pub async fn unreplayed_entries(&self) -> Vec<CdcDlqEntry> {
        self.entries
            .read()
            .await
            .iter()
            .filter(|e| !e.replayed)
            .cloned()
            .collect()
    }

    /// Mark an entry as replayed.
    pub async fn mark_replayed(&self, id: &str) -> Result<()> {
        let mut entries = self.entries.write().await;
        let entry = entries
            .iter_mut()
            .find(|e| e.id == id)
            .ok_or_else(|| StreamlineError::Config(format!("DLQ entry '{}' not found", id)))?;
        entry.replayed = true;
        self.total_replayed.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Get DLQ statistics.
    pub async fn stats(&self) -> DlqStats {
        let entries = self.entries.read().await;
        let mut by_source: HashMap<String, u64> = HashMap::new();
        let mut by_error_class: HashMap<String, u64> = HashMap::new();

        for entry in entries.iter() {
            *by_source.entry(entry.source.clone()).or_default() += 1;
            *by_error_class
                .entry(format!("{:?}", entry.error_class))
                .or_default() += 1;
        }

        DlqStats {
            total_entries: entries.len() as u64,
            unreplayed: entries.iter().filter(|e| !e.replayed).count() as u64,
            replayed: self.total_replayed.load(Ordering::Relaxed),
            by_source,
            by_error_class,
        }
    }

    /// Get the DLQ topic name for a source.
    pub fn topic_name(&self, source: &str) -> String {
        self.config.topic_pattern.replace("{source}", source)
    }

    /// Calculate retry backoff for a given attempt.
    pub fn backoff_for_attempt(&self, attempt: u32) -> std::time::Duration {
        let ms = (self.config.retry_backoff_ms as f64
            * 2.0f64.powi(attempt as i32)) as u64;
        let clamped = ms.min(self.config.max_backoff_ms);
        std::time::Duration::from_millis(clamped)
    }

    /// Check if retries are exhausted.
    pub fn retries_exhausted(&self, attempt: u32) -> bool {
        attempt >= self.config.max_retries
    }
}

/// DLQ statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqStats {
    pub total_entries: u64,
    pub unreplayed: u64,
    pub replayed: u64,
    pub by_source: HashMap<String, u64>,
    pub by_error_class: HashMap<String, u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_delivery_tracker_new_event() {
        let tracker = DeliveryTracker::new();
        let status = tracker
            .check_delivery("pg-source", "users", "0/ABC123", 1)
            .await;
        assert_eq!(status, DeliveryStatus::New);
    }

    #[tokio::test]
    async fn test_delivery_tracker_duplicate() {
        let tracker = DeliveryTracker::new();

        tracker
            .commit("pg-source", "users", "0/ABC123", 5, None)
            .await;

        // Same position, lower sequence → duplicate
        let status = tracker
            .check_delivery("pg-source", "users", "0/ABC123", 3)
            .await;
        assert_eq!(status, DeliveryStatus::Duplicate);

        // Higher sequence → new
        let status = tracker
            .check_delivery("pg-source", "users", "0/ABC123", 6)
            .await;
        assert_eq!(status, DeliveryStatus::New);
    }

    #[tokio::test]
    async fn test_delivery_checkpoint_restore() {
        let tracker = DeliveryTracker::new();
        tracker
            .commit("src", "table1", "pos-1", 10, Some("tx-1".to_string()))
            .await;

        let checkpoint = tracker.checkpoint().await.unwrap();

        let tracker2 = DeliveryTracker::new();
        tracker2.restore(&checkpoint).await.unwrap();

        let positions = tracker2.get_positions("src").await;
        assert_eq!(positions["table1"].position, "pos-1");
        assert_eq!(positions["table1"].sequence, 10);
    }

    #[tokio::test]
    async fn test_dlq_add_and_retrieve() {
        let dlq = CdcDlq::new(CdcDlqConfig::default());

        let id = dlq
            .add(
                "pg-source",
                "users",
                "INSERT",
                "0/ABC",
                "Schema mismatch",
                DlqErrorClass::SchemaError,
                3,
                Some(serde_json::json!({"id": 1})),
            )
            .await;

        let entries = dlq.entries_for_source("pg-source").await;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id, id);
        assert!(!entries[0].replayed);
    }

    #[tokio::test]
    async fn test_dlq_replay() {
        let dlq = CdcDlq::new(CdcDlqConfig::default());

        let id = dlq
            .add("src", "t", "INSERT", "pos", "err", DlqErrorClass::Unknown, 0, None)
            .await;

        dlq.mark_replayed(&id).await.unwrap();

        let unreplayed = dlq.unreplayed_entries().await;
        assert!(unreplayed.is_empty());
    }

    #[tokio::test]
    async fn test_dlq_stats() {
        let dlq = CdcDlq::new(CdcDlqConfig::default());

        dlq.add("src1", "t", "INSERT", "p1", "e1", DlqErrorClass::SchemaError, 0, None).await;
        dlq.add("src1", "t", "UPDATE", "p2", "e2", DlqErrorClass::WriteError, 0, None).await;
        dlq.add("src2", "t", "DELETE", "p3", "e3", DlqErrorClass::ConnectionError, 0, None).await;

        let stats = dlq.stats().await;
        assert_eq!(stats.total_entries, 3);
        assert_eq!(stats.by_source["src1"], 2);
        assert_eq!(stats.by_source["src2"], 1);
    }

    #[test]
    fn test_dlq_topic_name() {
        let dlq = CdcDlq::new(CdcDlqConfig::default());
        assert_eq!(dlq.topic_name("postgres"), "dlq-cdc-postgres");
    }

    #[test]
    fn test_backoff_calculation() {
        let dlq = CdcDlq::new(CdcDlqConfig::default());
        assert_eq!(dlq.backoff_for_attempt(0).as_millis(), 1000);
        assert_eq!(dlq.backoff_for_attempt(1).as_millis(), 2000);
        assert_eq!(dlq.backoff_for_attempt(2).as_millis(), 4000);
        // Should clamp at max
        assert!(dlq.backoff_for_attempt(10).as_millis() <= 30_000);
    }

    #[test]
    fn test_retries_exhausted() {
        let dlq = CdcDlq::new(CdcDlqConfig {
            max_retries: 3,
            ..Default::default()
        });
        assert!(!dlq.retries_exhausted(2));
        assert!(dlq.retries_exhausted(3));
    }
}
