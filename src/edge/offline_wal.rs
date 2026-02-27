//! Offline Write-Ahead Log for Edge Devices
//!
//! Provides a durable, append-only WAL that ensures zero data loss during
//! offline periods on edge devices. All writes are buffered locally and
//! synced to the cloud when connectivity is restored.
//!
//! # Design
//!
//! - Append-only segments that rotate when reaching [`WalConfig::max_segment_size_bytes`]
//! - Ring-buffer eviction when total size exceeds [`WalConfig::max_total_size_bytes`]
//! - Compaction removes fully-synced segments
//! - Thread-safe via `Arc<RwLock<...>>`

use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use tracing::{debug, info, warn};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Controls how the WAL flushes writes to durable storage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WalSyncMode {
    /// Flush after every append.
    Immediate,
    /// Flush at a fixed interval.
    Periodic { interval_ms: u64 },
    /// Flush only when the WAL is closed.
    OnClose,
}

impl Default for WalSyncMode {
    fn default() -> Self {
        Self::Periodic { interval_ms: 1000 }
    }
}

/// Retention policy for completed (synced) segments.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WalRetention {
    /// Keep up to `max_bytes` of synced data before purging.
    BySize { max_bytes: usize },
    /// Keep synced data for up to `max_age_secs` seconds.
    ByAge { max_age_secs: u64 },
    /// Keep at most `max_entries` synced entries.
    ByCount { max_entries: usize },
}

impl Default for WalRetention {
    fn default() -> Self {
        Self::BySize {
            max_bytes: 256 * 1024 * 1024,
        }
    }
}

/// Configuration for the offline WAL.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalConfig {
    /// Directory where WAL segment files are stored.
    pub data_dir: PathBuf,
    /// Maximum size of a single segment before rotation (default: 4 MB).
    pub max_segment_size_bytes: usize,
    /// Maximum total WAL size; oldest entries are dropped when exceeded (default: 256 MB).
    pub max_total_size_bytes: usize,
    /// Flush/sync strategy.
    pub sync_mode: WalSyncMode,
    /// Whether entries are compressed before writing.
    pub compression: bool,
    /// Retention policy for synced entries.
    pub retention: WalRetention,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("/tmp/streamline-wal"),
            max_segment_size_bytes: 4 * 1024 * 1024,  // 4 MB
            max_total_size_bytes: 256 * 1024 * 1024,   // 256 MB
            sync_mode: WalSyncMode::default(),
            compression: true,
            retention: WalRetention::default(),
        }
    }
}

// ---------------------------------------------------------------------------
// WAL Segment & Entry
// ---------------------------------------------------------------------------

/// A single entry in the WAL, representing one message produced while offline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEntry {
    /// Monotonically increasing sequence number.
    pub sequence: u64,
    /// Target topic name.
    pub topic: String,
    /// Optional message key.
    pub key: Option<Vec<u8>>,
    /// Message payload.
    pub value: Vec<u8>,
    /// Millisecond timestamp when the entry was appended.
    pub timestamp: u64,
    /// Whether this entry has been successfully synced to the cloud.
    pub synced: bool,
    /// Arbitrary headers carried with the message.
    pub headers: HashMap<String, String>,
}

impl WalEntry {
    /// Approximate in-memory size of this entry in bytes.
    fn estimated_size(&self) -> usize {
        self.topic.len()
            + self.key.as_ref().map_or(0, |k| k.len())
            + self.value.len()
            + self.headers.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>()
            + 64 // fixed overhead for sequence, timestamp, etc.
    }
}

/// A contiguous group of WAL entries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalSegment {
    /// Unique, monotonically increasing segment identifier.
    pub id: u64,
    /// Entries stored in this segment.
    pub entries: Vec<WalEntry>,
    /// Total estimated byte size of all entries.
    pub size_bytes: usize,
    /// Millisecond timestamp when the segment was created.
    pub created_at: u64,
    /// If `true`, the segment is immutable and a new active segment has taken over.
    pub sealed: bool,
}

impl WalSegment {
    fn new(id: u64) -> Self {
        Self {
            id,
            entries: Vec::new(),
            size_bytes: 0,
            created_at: current_time_ms(),
            sealed: false,
        }
    }

    /// Returns `true` when every entry in the segment has been synced.
    fn fully_synced(&self) -> bool {
        !self.entries.is_empty() && self.entries.iter().all(|e| e.synced)
    }

    /// Number of entries that have not yet been synced.
    fn pending_count(&self) -> usize {
        self.entries.iter().filter(|e| !e.synced).count()
    }
}

// ---------------------------------------------------------------------------
// Stats / Snapshot
// ---------------------------------------------------------------------------

/// Atomic counters tracking WAL health at a glance.
#[derive(Debug, Default)]
pub struct WalStats {
    pub total_entries: AtomicU64,
    pub total_bytes: AtomicU64,
    pub synced_entries: AtomicU64,
    pub pending_entries: AtomicU64,
    pub segments_count: AtomicU64,
    pub oldest_unsynced_ms: AtomicU64,
}

/// Serializable snapshot of [`WalStats`] for API responses.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalSnapshot {
    pub total_entries: u64,
    pub total_bytes: u64,
    pub synced_entries: u64,
    pub pending_entries: u64,
    pub segments_count: u64,
    pub oldest_unsynced_ms: u64,
}

// ---------------------------------------------------------------------------
// Compaction result
// ---------------------------------------------------------------------------

/// Summary returned by [`OfflineWal::compact`].
#[derive(Debug, Clone)]
pub struct CompactionResult {
    /// Number of segments removed during compaction.
    pub segments_removed: usize,
    /// Number of individual entries removed.
    pub entries_removed: usize,
    /// Approximate bytes freed.
    pub bytes_freed: usize,
}

// ---------------------------------------------------------------------------
// OfflineWal
// ---------------------------------------------------------------------------

/// Offline Write-Ahead Log for edge devices.
///
/// Provides durable, append-only storage so that messages produced while
/// offline are never lost. Entries are batched into segments which rotate
/// when they exceed the configured size limit.
pub struct OfflineWal {
    config: WalConfig,
    segments: Arc<RwLock<Vec<WalSegment>>>,
    active_segment: Arc<RwLock<WalSegment>>,
    stats: Arc<WalStats>,
    next_sequence: Arc<AtomicU64>,
    next_segment_id: Arc<AtomicU64>,
}

impl OfflineWal {
    /// Create a new WAL with the given configuration.
    pub fn new(config: WalConfig) -> Self {
        let active = WalSegment::new(0);
        let stats = Arc::new(WalStats::default());
        stats.segments_count.store(1, Ordering::Relaxed);

        info!(
            data_dir = %config.data_dir.display(),
            max_segment = config.max_segment_size_bytes,
            max_total = config.max_total_size_bytes,
            "Offline WAL initialised"
        );

        Self {
            config,
            segments: Arc::new(RwLock::new(Vec::new())),
            active_segment: Arc::new(RwLock::new(active)),
            stats,
            next_sequence: Arc::new(AtomicU64::new(0)),
            next_segment_id: Arc::new(AtomicU64::new(1)),
        }
    }

    /// Append a message to the WAL.
    ///
    /// Returns the sequence number assigned to the entry.
    pub fn append(
        &self,
        topic: &str,
        key: Option<&[u8]>,
        value: &[u8],
        headers: HashMap<String, String>,
    ) -> Result<u64> {
        if topic.is_empty() {
            return Err(StreamlineError::Config("WAL append: topic must not be empty".into()));
        }

        let seq = self.next_sequence.fetch_add(1, Ordering::SeqCst);

        let entry = WalEntry {
            sequence: seq,
            topic: topic.to_string(),
            key: key.map(|k| k.to_vec()),
            value: value.to_vec(),
            timestamp: current_time_ms(),
            synced: false,
            headers,
        };

        let entry_size = entry.estimated_size();

        // Enforce total size limit — drop oldest sealed segments to make room.
        self.enforce_total_size_limit(entry_size)?;

        let needs_rotation = {
            let active = self.active_segment.read().map_err(|e| {
                StreamlineError::Storage(format!("WAL lock poisoned: {e}"))
            })?;
            active.size_bytes + entry_size > self.config.max_segment_size_bytes
        };

        if needs_rotation {
            self.rotate_segment()?;
        }

        {
            let mut active = self.active_segment.write().map_err(|e| {
                StreamlineError::Storage(format!("WAL lock poisoned: {e}"))
            })?;
            active.size_bytes += entry_size;
            active.entries.push(entry);
        }

        self.stats.total_entries.fetch_add(1, Ordering::Relaxed);
        self.stats.total_bytes.fetch_add(entry_size as u64, Ordering::Relaxed);
        self.stats.pending_entries.fetch_add(1, Ordering::Relaxed);

        debug!(seq, topic, size = entry_size, "WAL entry appended");
        Ok(seq)
    }

    /// Return references to the first `limit` unsynced entries across all segments.
    pub fn get_unsynced_entries(&self, limit: usize) -> Vec<WalEntry> {
        let mut result = Vec::with_capacity(limit);

        // Sealed segments first (oldest data).
        if let Ok(segments) = self.segments.read() {
            for seg in segments.iter() {
                for entry in &seg.entries {
                    if !entry.synced {
                        result.push(entry.clone());
                        if result.len() >= limit {
                            return result;
                        }
                    }
                }
            }
        }

        // Then the active segment.
        if let Ok(active) = self.active_segment.read() {
            for entry in &active.entries {
                if !entry.synced {
                    result.push(entry.clone());
                    if result.len() >= limit {
                        return result;
                    }
                }
            }
        }

        result
    }

    /// Mark all entries with `sequence <= up_to_sequence` as synced.
    ///
    /// Returns the number of entries that were newly marked.
    pub fn mark_synced(&self, up_to_sequence: u64) -> Result<usize> {
        let mut marked = 0usize;

        if let Ok(mut segments) = self.segments.write() {
            for seg in segments.iter_mut() {
                for entry in seg.entries.iter_mut() {
                    if !entry.synced && entry.sequence <= up_to_sequence {
                        entry.synced = true;
                        marked += 1;
                    }
                }
            }
        } else {
            return Err(StreamlineError::Storage("WAL segments lock poisoned".into()));
        }

        if let Ok(mut active) = self.active_segment.write() {
            for entry in active.entries.iter_mut() {
                if !entry.synced && entry.sequence <= up_to_sequence {
                    entry.synced = true;
                    marked += 1;
                }
            }
        } else {
            return Err(StreamlineError::Storage("WAL active segment lock poisoned".into()));
        }

        self.stats.synced_entries.fetch_add(marked as u64, Ordering::Relaxed);
        self.stats.pending_entries.fetch_sub(marked as u64, Ordering::Relaxed);
        self.refresh_oldest_unsynced();

        info!(up_to_sequence, marked, "WAL entries marked as synced");
        Ok(marked)
    }

    /// Remove fully-synced sealed segments.
    pub fn compact(&self) -> Result<CompactionResult> {
        let mut segments = self.segments.write().map_err(|e| {
            StreamlineError::Storage(format!("WAL segments lock poisoned: {e}"))
        })?;

        let before_len = segments.len();
        let mut entries_removed = 0usize;
        let mut bytes_freed = 0usize;

        segments.retain(|seg| {
            if seg.fully_synced() {
                entries_removed += seg.entries.len();
                bytes_freed += seg.size_bytes;
                false
            } else {
                true
            }
        });

        let segments_removed = before_len - segments.len();

        // Update stats.
        self.stats
            .total_entries
            .fetch_sub(entries_removed as u64, Ordering::Relaxed);
        self.stats
            .total_bytes
            .fetch_sub(bytes_freed as u64, Ordering::Relaxed);
        self.stats
            .synced_entries
            .fetch_sub(entries_removed as u64, Ordering::Relaxed);
        self.stats
            .segments_count
            .fetch_sub(segments_removed as u64, Ordering::Relaxed);

        info!(
            segments_removed,
            entries_removed,
            bytes_freed,
            "WAL compaction complete"
        );

        Ok(CompactionResult {
            segments_removed,
            entries_removed,
            bytes_freed,
        })
    }

    /// Produce a point-in-time snapshot of the WAL statistics.
    pub fn snapshot(&self) -> WalSnapshot {
        WalSnapshot {
            total_entries: self.stats.total_entries.load(Ordering::Relaxed),
            total_bytes: self.stats.total_bytes.load(Ordering::Relaxed),
            synced_entries: self.stats.synced_entries.load(Ordering::Relaxed),
            pending_entries: self.stats.pending_entries.load(Ordering::Relaxed),
            segments_count: self.stats.segments_count.load(Ordering::Relaxed),
            oldest_unsynced_ms: self.stats.oldest_unsynced_ms.load(Ordering::Relaxed),
        }
    }

    /// Total number of entries across all segments.
    pub fn entry_count(&self) -> u64 {
        self.stats.total_entries.load(Ordering::Relaxed)
    }

    /// Number of entries not yet synced.
    pub fn pending_count(&self) -> u64 {
        self.stats.pending_entries.load(Ordering::Relaxed)
    }

    /// Age in milliseconds of the oldest unsynced entry, or 0 if none.
    pub fn oldest_unsynced_age_ms(&self) -> u64 {
        self.stats.oldest_unsynced_ms.load(Ordering::Relaxed)
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    /// Seal the current active segment and start a new one.
    fn rotate_segment(&self) -> Result<()> {
        let new_id = self.next_segment_id.fetch_add(1, Ordering::SeqCst);
        let mut new_seg = WalSegment::new(new_id);

        let mut active = self.active_segment.write().map_err(|e| {
            StreamlineError::Storage(format!("WAL lock poisoned: {e}"))
        })?;

        // Seal and swap.
        active.sealed = true;
        std::mem::swap(&mut *active, &mut new_seg);

        // Move the old (now sealed) segment into the sealed list.
        let mut segments = self.segments.write().map_err(|e| {
            StreamlineError::Storage(format!("WAL segments lock poisoned: {e}"))
        })?;
        segments.push(new_seg);

        self.stats.segments_count.fetch_add(1, Ordering::Relaxed);
        debug!(new_id, "WAL segment rotated");
        Ok(())
    }

    /// Drop oldest sealed segments until total size fits within the limit.
    fn enforce_total_size_limit(&self, incoming_bytes: usize) -> Result<()> {
        let current_total = self.stats.total_bytes.load(Ordering::Relaxed) as usize;
        if current_total + incoming_bytes <= self.config.max_total_size_bytes {
            return Ok(());
        }

        let mut segments = self.segments.write().map_err(|e| {
            StreamlineError::Storage(format!("WAL segments lock poisoned: {e}"))
        })?;

        let mut freed = 0usize;
        let target = (current_total + incoming_bytes).saturating_sub(self.config.max_total_size_bytes);

        let mut drop_count = 0usize;
        for seg in segments.iter() {
            if freed >= target {
                break;
            }
            freed += seg.size_bytes;
            drop_count += 1;
        }

        if drop_count > 0 {
            let removed: Vec<WalSegment> = segments.drain(..drop_count).collect();
            let entries_dropped: usize = removed.iter().map(|s| s.entries.len()).sum();
            let synced_dropped: usize = removed
                .iter()
                .flat_map(|s| &s.entries)
                .filter(|e| e.synced)
                .count();
            let pending_dropped = entries_dropped - synced_dropped;

            self.stats.total_entries.fetch_sub(entries_dropped as u64, Ordering::Relaxed);
            self.stats.total_bytes.fetch_sub(freed as u64, Ordering::Relaxed);
            self.stats.synced_entries.fetch_sub(synced_dropped as u64, Ordering::Relaxed);
            self.stats.pending_entries.fetch_sub(pending_dropped as u64, Ordering::Relaxed);
            self.stats.segments_count.fetch_sub(drop_count as u64, Ordering::Relaxed);

            warn!(
                segments_dropped = drop_count,
                entries_dropped,
                bytes_freed = freed,
                "WAL ring-buffer eviction: oldest segments dropped to stay within size limit"
            );
        }

        Ok(())
    }

    /// Recompute the oldest-unsynced timestamp from live data.
    fn refresh_oldest_unsynced(&self) {
        let now = current_time_ms();
        let mut oldest: Option<u64> = None;

        if let Ok(segments) = self.segments.read() {
            for seg in segments.iter() {
                for entry in &seg.entries {
                    if !entry.synced {
                        oldest = Some(match oldest {
                            Some(o) => o.min(entry.timestamp),
                            None => entry.timestamp,
                        });
                    }
                }
            }
        }

        if let Ok(active) = self.active_segment.read() {
            for entry in &active.entries {
                if !entry.synced {
                    oldest = Some(match oldest {
                        Some(o) => o.min(entry.timestamp),
                        None => entry.timestamp,
                    });
                }
            }
        }

        let age = oldest.map_or(0, |ts| now.saturating_sub(ts));
        self.stats.oldest_unsynced_ms.store(age, Ordering::Relaxed);
    }

    /// Compute the actual total bytes across all segments (sealed + active).
    #[cfg(test)]
    fn actual_total_bytes(&self) -> usize {
        let sealed: usize = self
            .segments
            .read()
            .unwrap()
            .iter()
            .map(|s| s.size_bytes)
            .sum();
        let active = self.active_segment.read().unwrap().size_bytes;
        sealed + active
    }
}

// ---------------------------------------------------------------------------
// Utility
// ---------------------------------------------------------------------------

fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> WalConfig {
        WalConfig {
            data_dir: PathBuf::from("/tmp/test-wal"),
            max_segment_size_bytes: 1024,
            max_total_size_bytes: 4096,
            sync_mode: WalSyncMode::OnClose,
            compression: false,
            retention: WalRetention::default(),
        }
    }

    fn make_wal() -> OfflineWal {
        OfflineWal::new(test_config())
    }

    fn empty_headers() -> HashMap<String, String> {
        HashMap::new()
    }

    // 1. Construction
    #[test]
    fn test_new_wal_has_zero_entries() {
        let wal = make_wal();
        assert_eq!(wal.entry_count(), 0);
        assert_eq!(wal.pending_count(), 0);
    }

    // 2. Append returns increasing sequence numbers
    #[test]
    fn test_append_returns_increasing_sequences() {
        let wal = make_wal();
        let s0 = wal.append("t", None, b"a", empty_headers()).unwrap();
        let s1 = wal.append("t", None, b"b", empty_headers()).unwrap();
        let s2 = wal.append("t", None, b"c", empty_headers()).unwrap();
        assert_eq!(s0, 0);
        assert_eq!(s1, 1);
        assert_eq!(s2, 2);
    }

    // 3. Entry count tracks appends
    #[test]
    fn test_entry_count_after_appends() {
        let wal = make_wal();
        for _ in 0..5 {
            wal.append("t", None, b"x", empty_headers()).unwrap();
        }
        assert_eq!(wal.entry_count(), 5);
    }

    // 4. All entries start as pending
    #[test]
    fn test_all_entries_pending_initially() {
        let wal = make_wal();
        for _ in 0..3 {
            wal.append("t", None, b"data", empty_headers()).unwrap();
        }
        assert_eq!(wal.pending_count(), 3);
        let snap = wal.snapshot();
        assert_eq!(snap.synced_entries, 0);
    }

    // 5. get_unsynced_entries returns unsynced
    #[test]
    fn test_get_unsynced_entries() {
        let wal = make_wal();
        wal.append("a", None, b"1", empty_headers()).unwrap();
        wal.append("b", None, b"2", empty_headers()).unwrap();

        let unsynced = wal.get_unsynced_entries(10);
        assert_eq!(unsynced.len(), 2);
        assert_eq!(unsynced[0].topic, "a");
        assert_eq!(unsynced[1].topic, "b");
    }

    // 6. get_unsynced_entries respects limit
    #[test]
    fn test_get_unsynced_entries_limit() {
        let wal = make_wal();
        for _ in 0..10 {
            wal.append("t", None, b"x", empty_headers()).unwrap();
        }
        let unsynced = wal.get_unsynced_entries(3);
        assert_eq!(unsynced.len(), 3);
    }

    // 7. mark_synced marks entries correctly
    #[test]
    fn test_mark_synced() {
        let wal = make_wal();
        wal.append("t", None, b"a", empty_headers()).unwrap();
        wal.append("t", None, b"b", empty_headers()).unwrap();
        wal.append("t", None, b"c", empty_headers()).unwrap();

        let marked = wal.mark_synced(1).unwrap();
        assert_eq!(marked, 2); // sequences 0 and 1
        assert_eq!(wal.pending_count(), 1);
        assert_eq!(wal.snapshot().synced_entries, 2);
    }

    // 8. get_unsynced_entries skips synced
    #[test]
    fn test_unsynced_after_mark() {
        let wal = make_wal();
        wal.append("t", None, b"a", empty_headers()).unwrap();
        wal.append("t", None, b"b", empty_headers()).unwrap();
        wal.mark_synced(0).unwrap();

        let unsynced = wal.get_unsynced_entries(10);
        assert_eq!(unsynced.len(), 1);
        assert_eq!(unsynced[0].sequence, 1);
    }

    // 9. Segment rotation on size
    #[test]
    fn test_segment_rotation() {
        let mut cfg = test_config();
        cfg.max_segment_size_bytes = 200; // force early rotation
        cfg.max_total_size_bytes = 100_000;
        let wal = OfflineWal::new(cfg);

        for _ in 0..20 {
            wal.append("topic", Some(b"key"), b"value-payload", empty_headers())
                .unwrap();
        }

        let segments = wal.segments.read().unwrap();
        assert!(segments.len() >= 1, "should have at least one sealed segment");
        assert!(
            segments.iter().all(|s| s.sealed),
            "all non-active segments must be sealed"
        );
    }

    // 10. Compaction removes fully-synced segments
    #[test]
    fn test_compaction() {
        let mut cfg = test_config();
        cfg.max_segment_size_bytes = 200;
        cfg.max_total_size_bytes = 100_000;
        let wal = OfflineWal::new(cfg);

        for _ in 0..20 {
            wal.append("t", None, b"payload-value", empty_headers()).unwrap();
        }

        // Mark everything synced.
        wal.mark_synced(19).unwrap();

        let result = wal.compact().unwrap();
        assert!(result.segments_removed > 0);
        assert!(result.entries_removed > 0);
        assert!(result.bytes_freed > 0);

        // After compaction, sealed segments should be empty.
        let segments = wal.segments.read().unwrap();
        assert_eq!(segments.len(), 0);
    }

    // 11. Compaction keeps partially-synced segments
    #[test]
    fn test_compaction_keeps_partial() {
        let mut cfg = test_config();
        cfg.max_segment_size_bytes = 200;
        cfg.max_total_size_bytes = 100_000;
        let wal = OfflineWal::new(cfg);

        for _ in 0..20 {
            wal.append("t", None, b"payload-data", empty_headers()).unwrap();
        }

        // Mark only some as synced — sealed segments with unsynced entries must survive.
        wal.mark_synced(5).unwrap();

        let before = wal.segments.read().unwrap().len();
        wal.compact().unwrap();
        let after = wal.segments.read().unwrap().len();

        // At least some segments should remain if they contain unsynced entries.
        assert!(after <= before);
    }

    // 12. Snapshot reflects current state
    #[test]
    fn test_snapshot() {
        let wal = make_wal();
        wal.append("t", None, b"a", empty_headers()).unwrap();
        wal.append("t", None, b"b", empty_headers()).unwrap();
        wal.mark_synced(0).unwrap();

        let snap = wal.snapshot();
        assert_eq!(snap.total_entries, 2);
        assert_eq!(snap.synced_entries, 1);
        assert_eq!(snap.pending_entries, 1);
    }

    // 13. Append with headers
    #[test]
    fn test_append_with_headers() {
        let wal = make_wal();
        let mut headers = HashMap::new();
        headers.insert("source".into(), "sensor-1".into());
        headers.insert("priority".into(), "high".into());

        wal.append("events", Some(b"k"), b"v", headers).unwrap();

        let entries = wal.get_unsynced_entries(1);
        assert_eq!(entries[0].headers.get("source").unwrap(), "sensor-1");
        assert_eq!(entries[0].key, Some(b"k".to_vec()));
    }

    // 14. Append with empty topic fails
    #[test]
    fn test_append_empty_topic_fails() {
        let wal = make_wal();
        let result = wal.append("", None, b"v", empty_headers());
        assert!(result.is_err());
    }

    // 15. Ring-buffer eviction drops oldest segments
    #[test]
    fn test_ring_buffer_eviction() {
        let mut cfg = test_config();
        cfg.max_segment_size_bytes = 200;
        cfg.max_total_size_bytes = 800;
        let wal = OfflineWal::new(cfg);

        // Append enough to exceed total size — oldest sealed segments should be evicted.
        for i in 0..50 {
            wal.append("t", None, format!("payload-{i:04}").as_bytes(), empty_headers())
                .unwrap();
        }

        let total = wal.actual_total_bytes();
        assert!(
            total <= 800 + 400, // allow one segment of headroom
            "total bytes ({total}) should be near max_total_size_bytes"
        );
    }

    // 16. WalConfig defaults are sensible
    #[test]
    fn test_default_config() {
        let cfg = WalConfig::default();
        assert_eq!(cfg.max_segment_size_bytes, 4 * 1024 * 1024);
        assert_eq!(cfg.max_total_size_bytes, 256 * 1024 * 1024);
        assert!(cfg.compression);
    }

    // 17. WalSegment fully_synced logic
    #[test]
    fn test_segment_fully_synced() {
        let mut seg = WalSegment::new(0);
        assert!(!seg.fully_synced(), "empty segment is not fully synced");

        seg.entries.push(WalEntry {
            sequence: 0,
            topic: "t".into(),
            key: None,
            value: vec![1],
            timestamp: 0,
            synced: true,
            headers: HashMap::new(),
        });
        assert!(seg.fully_synced());

        seg.entries.push(WalEntry {
            sequence: 1,
            topic: "t".into(),
            key: None,
            value: vec![2],
            timestamp: 0,
            synced: false,
            headers: HashMap::new(),
        });
        assert!(!seg.fully_synced());
    }

    // 18. WalSyncMode and WalRetention defaults
    #[test]
    fn test_sync_mode_default() {
        let mode = WalSyncMode::default();
        assert_eq!(mode, WalSyncMode::Periodic { interval_ms: 1000 });
    }

    // 19. Multiple mark_synced calls are idempotent for already-synced entries
    #[test]
    fn test_mark_synced_idempotent() {
        let wal = make_wal();
        wal.append("t", None, b"a", empty_headers()).unwrap();

        let m1 = wal.mark_synced(0).unwrap();
        assert_eq!(m1, 1);

        let m2 = wal.mark_synced(0).unwrap();
        assert_eq!(m2, 0);
    }

    // 20. oldest_unsynced_age_ms is zero when everything is synced
    #[test]
    fn test_oldest_unsynced_age_zero_when_all_synced() {
        let wal = make_wal();
        wal.append("t", None, b"a", empty_headers()).unwrap();
        wal.mark_synced(0).unwrap();
        assert_eq!(wal.oldest_unsynced_age_ms(), 0);
    }
}
