//! Transactional Outbox Pattern
//!
//! Built-in outbox pattern support for reliable microservice event publishing.
//! Entries are enqueued within a transaction and later polled/published to
//! Streamline topics, providing at-least-once delivery with deduplication.
//!
//! # Example
//!
//! ```text
//! let mgr = OutboxManager::new(OutboxConfig::default());
//! let id = mgr.enqueue("Order", "order-123", "OrderCreated",
//!     "orders", Some("order-123".into()),
//!     serde_json::json!({"total": 99.99}),
//!     HashMap::new(),
//! ).unwrap();
//!
//! let pending = mgr.get_pending(10);
//! mgr.mark_published(&id).unwrap();
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};
use uuid::Uuid;

// ── Configuration ──────────────────────────────────────────────────

/// Configuration for the outbox manager.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutboxConfig {
    /// How often the publisher polls for pending entries (ms).
    pub poll_interval_ms: u64,
    /// Maximum entries returned per poll batch.
    pub max_batch_size: usize,
    /// Maximum publish retry attempts before marking as failed.
    pub max_retries: u32,
    /// Base back-off between retries (ms, multiplied by attempt).
    pub retry_backoff_ms: u64,
    /// How long to keep published/failed entries before expiry (hours).
    pub retention_hours: u64,
    /// Window for duplicate detection (ms).
    pub dedup_window_ms: u64,
}

impl Default for OutboxConfig {
    fn default() -> Self {
        Self {
            poll_interval_ms: 100,
            max_batch_size: 100,
            max_retries: 5,
            retry_backoff_ms: 1000,
            retention_hours: 72,
            dedup_window_ms: 300_000, // 5 min
        }
    }
}

// ── Entry types ────────────────────────────────────────────────────

/// Status of a single outbox entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OutboxEntryStatus {
    /// Waiting to be published.
    Pending,
    /// Successfully published to the target topic.
    Published,
    /// Exceeded max retries.
    Failed,
    /// Past the retention window.
    Expired,
}

/// A single outbox entry representing an event to be published.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutboxEntry {
    /// Unique identifier (UUID v4).
    pub id: String,
    /// Aggregate type (e.g. `"Order"`, `"Payment"`).
    pub aggregate_type: String,
    /// Aggregate identifier (e.g. the order ID).
    pub aggregate_id: String,
    /// Domain event type (e.g. `"OrderCreated"`).
    pub event_type: String,
    /// Target Streamline topic.
    pub topic: String,
    /// Optional message key for partitioning.
    pub key: Option<String>,
    /// Event payload.
    pub payload: serde_json::Value,
    /// Arbitrary headers forwarded with the message.
    pub headers: HashMap<String, String>,
    /// Current entry status.
    pub status: OutboxEntryStatus,
    /// Creation timestamp (epoch ms).
    pub created_at: u64,
    /// When the entry was successfully published (epoch ms).
    pub published_at: Option<u64>,
    /// How many times publish has been attempted.
    pub retry_count: u32,
    /// Last error message on failure.
    pub last_error: Option<String>,
}

// ── Stats ──────────────────────────────────────────────────────────

/// Atomic counters for outbox operations.
#[derive(Debug, Default)]
pub struct OutboxStats {
    pub entries_created: AtomicU64,
    pub entries_published: AtomicU64,
    pub entries_failed: AtomicU64,
    pub retries: AtomicU64,
    pub dedup_hits: AtomicU64,
}

/// Snapshot of [`OutboxStats`] for serialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutboxStatsSnapshot {
    pub entries_created: u64,
    pub entries_published: u64,
    pub entries_failed: u64,
    pub retries: u64,
    pub dedup_hits: u64,
}

impl OutboxStats {
    pub fn snapshot(&self) -> OutboxStatsSnapshot {
        OutboxStatsSnapshot {
            entries_created: self.entries_created.load(Ordering::Relaxed),
            entries_published: self.entries_published.load(Ordering::Relaxed),
            entries_failed: self.entries_failed.load(Ordering::Relaxed),
            retries: self.retries.load(Ordering::Relaxed),
            dedup_hits: self.dedup_hits.load(Ordering::Relaxed),
        }
    }
}

// ── Manager ────────────────────────────────────────────────────────

/// Manages outbox entries – enqueue, poll, mark, and expire.
pub struct OutboxManager {
    /// Entries keyed by aggregate type, each value is a list of entries.
    entries: Arc<RwLock<HashMap<String, Vec<OutboxEntry>>>>,
    /// Configuration.
    config: OutboxConfig,
    /// Operational statistics.
    stats: Arc<OutboxStats>,
}

impl OutboxManager {
    /// Create a new `OutboxManager` with the given configuration.
    pub fn new(config: OutboxConfig) -> Self {
        info!(
            poll_interval_ms = config.poll_interval_ms,
            max_batch_size = config.max_batch_size,
            "OutboxManager created"
        );
        Self {
            entries: Arc::new(RwLock::new(HashMap::new())),
            config,
            stats: Arc::new(OutboxStats::default()),
        }
    }

    /// Enqueue a new outbox entry. Returns the generated entry ID.
    ///
    /// Duplicate detection: if an entry with the same `(aggregate_type,
    /// aggregate_id, event_type)` was created within `dedup_window_ms` and
    /// is still pending, the call is rejected.
    #[allow(clippy::too_many_arguments)]
    pub async fn enqueue(
        &self,
        aggregate_type: &str,
        aggregate_id: &str,
        event_type: &str,
        topic: &str,
        key: Option<String>,
        payload: serde_json::Value,
        headers: HashMap<String, String>,
    ) -> crate::error::Result<String> {
        let now = current_epoch_ms();

        // Deduplication check
        {
            let map = self.entries.read().await;
            if let Some(list) = map.get(aggregate_type) {
                for e in list {
                    if e.aggregate_id == aggregate_id
                        && e.event_type == event_type
                        && e.status == OutboxEntryStatus::Pending
                        && now.saturating_sub(e.created_at) < self.config.dedup_window_ms
                    {
                        self.stats.dedup_hits.fetch_add(1, Ordering::Relaxed);
                        warn!(
                            aggregate_type,
                            aggregate_id, event_type, "Duplicate outbox entry detected"
                        );
                        return Err(crate::error::StreamlineError::Server(format!(
                            "Duplicate outbox entry for {aggregate_type}/{aggregate_id}/{event_type}"
                        )));
                    }
                }
            }
        }

        let id = Uuid::new_v4().to_string();
        let entry = OutboxEntry {
            id: id.clone(),
            aggregate_type: aggregate_type.to_string(),
            aggregate_id: aggregate_id.to_string(),
            event_type: event_type.to_string(),
            topic: topic.to_string(),
            key,
            payload,
            headers,
            status: OutboxEntryStatus::Pending,
            created_at: now,
            published_at: None,
            retry_count: 0,
            last_error: None,
        };

        {
            let mut map = self.entries.write().await;
            map.entry(aggregate_type.to_string())
                .or_default()
                .push(entry);
        }

        self.stats.entries_created.fetch_add(1, Ordering::Relaxed);
        debug!(id = %id, aggregate_type, aggregate_id, event_type, "Outbox entry enqueued");
        Ok(id)
    }

    /// Return up to `batch_size` pending entries ordered by creation time.
    pub async fn get_pending(&self, batch_size: usize) -> Vec<OutboxEntry> {
        let map = self.entries.read().await;
        let mut pending: Vec<OutboxEntry> = map
            .values()
            .flat_map(|v| v.iter())
            .filter(|e| e.status == OutboxEntryStatus::Pending)
            .cloned()
            .collect();
        pending.sort_by_key(|e| e.created_at);
        pending.truncate(batch_size);
        pending
    }

    /// Mark an entry as successfully published.
    pub async fn mark_published(&self, id: &str) -> crate::error::Result<()> {
        let mut map = self.entries.write().await;
        for list in map.values_mut() {
            if let Some(entry) = list.iter_mut().find(|e| e.id == id) {
                entry.status = OutboxEntryStatus::Published;
                entry.published_at = Some(current_epoch_ms());
                self.stats
                    .entries_published
                    .fetch_add(1, Ordering::Relaxed);
                debug!(id, "Outbox entry marked published");
                return Ok(());
            }
        }
        Err(crate::error::StreamlineError::Server(format!(
            "Outbox entry not found: {id}"
        )))
    }

    /// Mark an entry as failed, recording the error and bumping the retry
    /// counter. If `max_retries` is exceeded the status becomes `Failed`,
    /// otherwise it stays `Pending` for a future attempt.
    pub async fn mark_failed(&self, id: &str, error: &str) -> crate::error::Result<()> {
        let mut map = self.entries.write().await;
        for list in map.values_mut() {
            if let Some(entry) = list.iter_mut().find(|e| e.id == id) {
                entry.retry_count += 1;
                entry.last_error = Some(error.to_string());
                self.stats.retries.fetch_add(1, Ordering::Relaxed);

                if entry.retry_count >= self.config.max_retries {
                    entry.status = OutboxEntryStatus::Failed;
                    self.stats.entries_failed.fetch_add(1, Ordering::Relaxed);
                    warn!(id, retries = entry.retry_count, "Outbox entry exceeded max retries");
                } else {
                    debug!(
                        id,
                        retry = entry.retry_count,
                        max = self.config.max_retries,
                        "Outbox entry retry recorded"
                    );
                }
                return Ok(());
            }
        }
        Err(crate::error::StreamlineError::Server(format!(
            "Outbox entry not found: {id}"
        )))
    }

    /// Retrieve a single entry by ID.
    pub async fn get_entry(&self, id: &str) -> Option<OutboxEntry> {
        let map = self.entries.read().await;
        map.values()
            .flat_map(|v| v.iter())
            .find(|e| e.id == id)
            .cloned()
    }

    /// List all entries for a given aggregate type and aggregate ID.
    pub async fn list_entries(
        &self,
        aggregate_type: &str,
        aggregate_id: &str,
    ) -> Vec<OutboxEntry> {
        let map = self.entries.read().await;
        map.get(aggregate_type)
            .map(|v| {
                v.iter()
                    .filter(|e| e.aggregate_id == aggregate_id)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Expire entries whose `created_at` is older than `retention_hours`.
    pub async fn expire_old(&self) -> usize {
        let cutoff =
            current_epoch_ms().saturating_sub(self.config.retention_hours * 3_600_000);
        let mut expired = 0usize;
        let mut map = self.entries.write().await;
        for list in map.values_mut() {
            for entry in list.iter_mut() {
                if entry.created_at < cutoff && entry.status != OutboxEntryStatus::Expired {
                    entry.status = OutboxEntryStatus::Expired;
                    expired += 1;
                }
            }
        }
        if expired > 0 {
            info!(expired, "Expired old outbox entries");
        }
        expired
    }

    /// Return a snapshot of the current statistics.
    pub fn stats(&self) -> OutboxStatsSnapshot {
        self.stats.snapshot()
    }
}

/// Current wall-clock time in epoch milliseconds.
fn current_epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn default_mgr() -> OutboxManager {
        OutboxManager::new(OutboxConfig::default())
    }

    fn cfg_no_dedup() -> OutboxConfig {
        OutboxConfig {
            dedup_window_ms: 0,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_enqueue_returns_uuid() {
        let mgr = default_mgr();
        let id = mgr
            .enqueue(
                "Order",
                "o1",
                "OrderCreated",
                "orders",
                None,
                serde_json::json!({}),
                HashMap::new(),
            )
            .await
            .unwrap();
        assert!(!id.is_empty());
        // Valid UUID v4
        assert!(Uuid::parse_str(&id).is_ok());
    }

    #[tokio::test]
    async fn test_get_pending_returns_enqueued() {
        let mgr = default_mgr();
        mgr.enqueue("Order", "o1", "OrderCreated", "orders", None, serde_json::json!({}), HashMap::new())
            .await
            .unwrap();
        let pending = mgr.get_pending(10).await;
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].aggregate_type, "Order");
    }

    #[tokio::test]
    async fn test_mark_published() {
        let mgr = default_mgr();
        let id = mgr
            .enqueue("Order", "o1", "Created", "t", None, serde_json::json!({}), HashMap::new())
            .await
            .unwrap();
        mgr.mark_published(&id).await.unwrap();
        let e = mgr.get_entry(&id).await.unwrap();
        assert_eq!(e.status, OutboxEntryStatus::Published);
        assert!(e.published_at.is_some());
    }

    #[tokio::test]
    async fn test_mark_published_removes_from_pending() {
        let mgr = default_mgr();
        let id = mgr
            .enqueue("Order", "o1", "Created", "t", None, serde_json::json!({}), HashMap::new())
            .await
            .unwrap();
        mgr.mark_published(&id).await.unwrap();
        let pending = mgr.get_pending(10).await;
        assert!(pending.is_empty());
    }

    #[tokio::test]
    async fn test_mark_failed_increments_retry() {
        let mgr = default_mgr();
        let id = mgr
            .enqueue("Order", "o1", "Created", "t", None, serde_json::json!({}), HashMap::new())
            .await
            .unwrap();
        mgr.mark_failed(&id, "timeout").await.unwrap();
        let e = mgr.get_entry(&id).await.unwrap();
        assert_eq!(e.retry_count, 1);
        assert_eq!(e.last_error.as_deref(), Some("timeout"));
        // Still pending (retry_count < max_retries)
        assert_eq!(e.status, OutboxEntryStatus::Pending);
    }

    #[tokio::test]
    async fn test_mark_failed_exceeds_max_retries() {
        let cfg = OutboxConfig {
            max_retries: 2,
            dedup_window_ms: 0,
            ..Default::default()
        };
        let mgr = OutboxManager::new(cfg);
        let id = mgr
            .enqueue("Order", "o1", "Created", "t", None, serde_json::json!({}), HashMap::new())
            .await
            .unwrap();
        mgr.mark_failed(&id, "err1").await.unwrap();
        mgr.mark_failed(&id, "err2").await.unwrap();
        let e = mgr.get_entry(&id).await.unwrap();
        assert_eq!(e.status, OutboxEntryStatus::Failed);
    }

    #[tokio::test]
    async fn test_mark_published_not_found() {
        let mgr = default_mgr();
        let res = mgr.mark_published("nonexistent").await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_mark_failed_not_found() {
        let mgr = default_mgr();
        let res = mgr.mark_failed("nonexistent", "err").await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_get_entry_found() {
        let mgr = default_mgr();
        let id = mgr
            .enqueue("Payment", "p1", "Charged", "payments", Some("p1".into()), serde_json::json!({"amount":42}), HashMap::new())
            .await
            .unwrap();
        let e = mgr.get_entry(&id).await.unwrap();
        assert_eq!(e.aggregate_type, "Payment");
        assert_eq!(e.key, Some("p1".to_string()));
        assert_eq!(e.payload["amount"], 42);
    }

    #[tokio::test]
    async fn test_get_entry_not_found() {
        let mgr = default_mgr();
        assert!(mgr.get_entry("missing").await.is_none());
    }

    #[tokio::test]
    async fn test_list_entries() {
        let mgr = OutboxManager::new(cfg_no_dedup());
        mgr.enqueue("Order", "o1", "Created", "t", None, serde_json::json!({}), HashMap::new())
            .await
            .unwrap();
        mgr.enqueue("Order", "o1", "Shipped", "t", None, serde_json::json!({}), HashMap::new())
            .await
            .unwrap();
        mgr.enqueue("Order", "o2", "Created", "t", None, serde_json::json!({}), HashMap::new())
            .await
            .unwrap();
        let entries = mgr.list_entries("Order", "o1").await;
        assert_eq!(entries.len(), 2);
    }

    #[tokio::test]
    async fn test_list_entries_empty() {
        let mgr = default_mgr();
        let entries = mgr.list_entries("NoSuch", "x").await;
        assert!(entries.is_empty());
    }

    #[tokio::test]
    async fn test_dedup_rejects_duplicate() {
        let mgr = default_mgr();
        mgr.enqueue("Order", "o1", "Created", "t", None, serde_json::json!({}), HashMap::new())
            .await
            .unwrap();
        let res = mgr
            .enqueue("Order", "o1", "Created", "t", None, serde_json::json!({}), HashMap::new())
            .await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_dedup_allows_different_event_type() {
        let mgr = default_mgr();
        mgr.enqueue("Order", "o1", "Created", "t", None, serde_json::json!({}), HashMap::new())
            .await
            .unwrap();
        let res = mgr
            .enqueue("Order", "o1", "Shipped", "t", None, serde_json::json!({}), HashMap::new())
            .await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_expire_old() {
        let mgr = OutboxManager::new(OutboxConfig {
            retention_hours: 0, // expire immediately
            ..Default::default()
        });
        mgr.enqueue("Order", "o1", "Created", "t", None, serde_json::json!({}), HashMap::new())
            .await
            .unwrap();
        // Entry was just created with created_at = now; retention = 0h means cutoff = now,
        // so created_at < cutoff may not hold if the clock hasn't advanced. Force by
        // manually setting created_at in the past.
        {
            let mut map = mgr.entries.write().await;
            for list in map.values_mut() {
                for e in list.iter_mut() {
                    e.created_at = 0; // epoch
                }
            }
        }
        let expired = mgr.expire_old().await;
        assert_eq!(expired, 1);
        let e = mgr.get_pending(10).await;
        assert!(e.is_empty());
    }

    #[tokio::test]
    async fn test_stats_after_operations() {
        let mgr = OutboxManager::new(cfg_no_dedup());
        mgr.enqueue("A", "1", "E1", "t", None, serde_json::json!({}), HashMap::new())
            .await
            .unwrap();
        mgr.enqueue("A", "2", "E2", "t", None, serde_json::json!({}), HashMap::new())
            .await
            .unwrap();
        let s = mgr.stats();
        assert_eq!(s.entries_created, 2);
        assert_eq!(s.entries_published, 0);
    }

    #[tokio::test]
    async fn test_stats_dedup_hits() {
        let mgr = default_mgr();
        mgr.enqueue("Order", "o1", "Created", "t", None, serde_json::json!({}), HashMap::new())
            .await
            .unwrap();
        let _ = mgr
            .enqueue("Order", "o1", "Created", "t", None, serde_json::json!({}), HashMap::new())
            .await;
        let s = mgr.stats();
        assert_eq!(s.dedup_hits, 1);
    }

    #[tokio::test]
    async fn test_get_pending_respects_batch_size() {
        let mgr = OutboxManager::new(cfg_no_dedup());
        for i in 0..10 {
            mgr.enqueue("T", &format!("id{i}"), "E", "t", None, serde_json::json!({}), HashMap::new())
                .await
                .unwrap();
        }
        let pending = mgr.get_pending(3).await;
        assert_eq!(pending.len(), 3);
    }

    #[tokio::test]
    async fn test_headers_preserved() {
        let mgr = default_mgr();
        let mut headers = HashMap::new();
        headers.insert("trace-id".to_string(), "abc-123".to_string());
        let id = mgr
            .enqueue("Order", "o1", "Created", "t", None, serde_json::json!({}), headers)
            .await
            .unwrap();
        let e = mgr.get_entry(&id).await.unwrap();
        assert_eq!(e.headers.get("trace-id").unwrap(), "abc-123");
    }

    #[tokio::test]
    async fn test_default_config_values() {
        let cfg = OutboxConfig::default();
        assert_eq!(cfg.poll_interval_ms, 100);
        assert_eq!(cfg.max_batch_size, 100);
        assert_eq!(cfg.max_retries, 5);
        assert_eq!(cfg.retry_backoff_ms, 1000);
        assert_eq!(cfg.retention_hours, 72);
        assert_eq!(cfg.dedup_window_ms, 300_000);
    }

    #[tokio::test]
    async fn test_pending_ordered_by_created_at() {
        let mgr = OutboxManager::new(cfg_no_dedup());
        // Enqueue three entries and manually set different created_at values.
        for i in 0..3 {
            mgr.enqueue("T", &format!("id{i}"), "E", "t", None, serde_json::json!({}), HashMap::new())
                .await
                .unwrap();
        }
        // Force ordering: id2=1, id0=2, id1=3
        {
            let mut map = mgr.entries.write().await;
            let list = map.get_mut("T").unwrap();
            list[0].created_at = 2;
            list[1].created_at = 3;
            list[2].created_at = 1;
        }
        let pending = mgr.get_pending(10).await;
        assert_eq!(pending[0].aggregate_id, "id2");
        assert_eq!(pending[1].aggregate_id, "id0");
        assert_eq!(pending[2].aggregate_id, "id1");
    }
}
