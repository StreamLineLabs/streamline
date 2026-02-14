//! Time-Travel Debugging for Time-Series Data
//!
//! Provides point-in-time queries, message diffing between time ranges,
//! range replay, and snapshot bookmarking for time-series topics.

use crate::error::{Result, StreamlineError};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

/// Query parameters for time-travel lookups.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeTravelQuery {
    /// Topic to query
    pub topic: String,
    /// Start timestamp (inclusive, ms since epoch)
    pub from_time: i64,
    /// End timestamp (exclusive, ms since epoch)
    pub to_time: i64,
    /// Optional partition filter
    pub partition: Option<i32>,
    /// Maximum number of results to return
    pub max_results: usize,
    /// Optional key filter (substring match)
    pub key_filter: Option<String>,
}

impl TimeTravelQuery {
    /// Create a new time-travel query.
    pub fn new(topic: &str, from_time: i64, to_time: i64) -> Self {
        Self {
            topic: topic.to_string(),
            from_time,
            to_time,
            partition: None,
            max_results: 1000,
            key_filter: None,
        }
    }

    /// Filter by partition.
    pub fn with_partition(mut self, partition: i32) -> Self {
        self.partition = Some(partition);
        self
    }

    /// Set maximum results.
    pub fn with_max_results(mut self, max: usize) -> Self {
        self.max_results = max;
        self
    }

    /// Filter by key substring.
    pub fn with_key_filter(mut self, filter: &str) -> Self {
        self.key_filter = Some(filter.to_string());
        self
    }
}

/// A message returned from a time-travel query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeTravelMessage {
    /// Offset within the partition
    pub offset: i64,
    /// Timestamp in milliseconds since epoch
    pub timestamp: i64,
    /// Partition number
    pub partition: i32,
    /// Optional message key
    pub key: Option<String>,
    /// Preview of the value (first 200 characters)
    pub value_preview: String,
    /// Full value size in bytes
    pub value_size_bytes: usize,
    /// Message headers
    pub headers: HashMap<String, String>,
}

/// Result of a time-travel query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeTravelResult {
    /// The query that produced this result
    pub query: TimeTravelQuery,
    /// Matching messages
    pub messages: Vec<TimeTravelMessage>,
    /// Total records scanned
    pub total_scanned: usize,
    /// Query duration in milliseconds
    pub duration_ms: u64,
}

/// Diff between two time ranges showing added, removed, and modified messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageDiff {
    /// Keys present in the second range but not the first
    pub added: Vec<TimeTravelMessage>,
    /// Keys present in the first range but not the second
    pub removed: Vec<TimeTravelMessage>,
    /// Same key with different value
    pub modified: Vec<MessageChange>,
    /// Number of keys present in both ranges with identical values
    pub unchanged_count: usize,
}

/// A single key whose value changed between two time ranges.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageChange {
    /// The message key
    pub key: String,
    /// Message in the first (before) range
    pub before: TimeTravelMessage,
    /// Message in the second (after) range
    pub after: TimeTravelMessage,
    /// List of field names that changed
    pub fields_changed: Vec<String>,
}

/// A bookmarked point-in-time snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    /// Unique snapshot identifier
    pub id: String,
    /// Topic name
    pub topic: String,
    /// Timestamp bookmarked (ms since epoch)
    pub timestamp: i64,
    /// Human-readable description
    pub description: String,
    /// When the snapshot was created (ms since epoch)
    pub created_at: i64,
    /// Number of messages captured at this point
    pub message_count: usize,
    /// Offset range (start, end) covered by the snapshot
    pub offset_range: (i64, i64),
}

/// Result of a replay operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayResult {
    /// Source topic
    pub source_topic: String,
    /// Target topic
    pub target_topic: String,
    /// Number of messages replayed
    pub messages_replayed: usize,
    /// Replay start timestamp
    pub from_time: i64,
    /// Replay end timestamp
    pub to_time: i64,
    /// Duration of the replay in milliseconds
    pub duration_ms: u64,
}

/// Manages snapshot bookmarks for interesting points in time.
pub struct SnapshotManager {
    snapshots: RwLock<HashMap<String, Vec<Snapshot>>>,
}

impl SnapshotManager {
    /// Create a new snapshot manager.
    pub fn new() -> Self {
        Self {
            snapshots: RwLock::new(HashMap::new()),
        }
    }

    /// Bookmark a point in time for a topic.
    pub fn create_snapshot(&self, topic: &str, timestamp: i64, description: &str) -> Snapshot {
        let id = uuid::Uuid::new_v4().to_string();
        let now = chrono::Utc::now().timestamp_millis();

        let snapshot = Snapshot {
            id: id.clone(),
            topic: topic.to_string(),
            timestamp,
            description: description.to_string(),
            created_at: now,
            message_count: 0,
            offset_range: (0, 0),
        };

        self.snapshots
            .write()
            .entry(topic.to_string())
            .or_default()
            .push(snapshot.clone());

        snapshot
    }

    /// List all snapshots for a topic.
    pub fn list_snapshots(&self, topic: &str) -> Vec<Snapshot> {
        self.snapshots
            .read()
            .get(topic)
            .cloned()
            .unwrap_or_default()
    }

    /// Get a snapshot by id.
    pub fn get_snapshot(&self, id: &str) -> Option<Snapshot> {
        self.snapshots
            .read()
            .values()
            .flatten()
            .find(|s| s.id == id)
            .cloned()
    }

    /// Delete a snapshot by id.
    pub fn delete_snapshot(&self, id: &str) -> bool {
        let mut map = self.snapshots.write();
        for snaps in map.values_mut() {
            if let Some(pos) = snaps.iter().position(|s| s.id == id) {
                snaps.remove(pos);
                return true;
            }
        }
        false
    }
}

impl Default for SnapshotManager {
    fn default() -> Self {
        Self::new()
    }
}

/// In-memory record store used by `TimeTravelEngine` for queries and diffs.
///
/// Maps `topic -> Vec<TimeTravelMessage>`, sorted by timestamp.
type MessageStore = HashMap<String, Vec<TimeTravelMessage>>;

/// Engine for time-travel debugging: point-in-time queries, diffs, and replay.
pub struct TimeTravelEngine {
    /// Messages indexed by topic
    messages: Arc<RwLock<MessageStore>>,
    /// Snapshot bookmark manager
    pub snapshots: SnapshotManager,
}

impl TimeTravelEngine {
    /// Create a new time-travel engine.
    pub fn new() -> Self {
        Self {
            messages: Arc::new(RwLock::new(HashMap::new())),
            snapshots: SnapshotManager::new(),
        }
    }

    /// Ingest a message into the engine's store.
    pub fn ingest(&self, topic: &str, message: TimeTravelMessage) {
        self.messages
            .write()
            .entry(topic.to_string())
            .or_default()
            .push(message);
    }

    /// Execute a point-in-time query.
    pub fn query(&self, query: TimeTravelQuery) -> Result<TimeTravelResult> {
        let start = Instant::now();
        let store = self.messages.read();

        let topic_messages = store.get(&query.topic).ok_or_else(|| {
            StreamlineError::storage_msg(format!("Topic '{}' not found", query.topic))
        })?;

        let mut total_scanned: usize = 0;
        let mut matched = Vec::new();

        for msg in topic_messages {
            total_scanned += 1;

            if msg.timestamp < query.from_time || msg.timestamp >= query.to_time {
                continue;
            }
            if let Some(p) = query.partition {
                if msg.partition != p {
                    continue;
                }
            }
            if let Some(ref kf) = query.key_filter {
                match &msg.key {
                    Some(k) if k.contains(kf) => {}
                    _ => continue,
                }
            }

            matched.push(msg.clone());
            if matched.len() >= query.max_results {
                break;
            }
        }

        Ok(TimeTravelResult {
            query,
            messages: matched,
            total_scanned,
            duration_ms: start.elapsed().as_millis() as u64,
        })
    }

    /// Diff messages between two time ranges on the same topic.
    ///
    /// `from_range` represents the "before" window and `to_range` the "after" window.
    /// Messages are grouped by key; keyless messages are excluded from the diff.
    pub fn diff(
        &self,
        topic: &str,
        from_range: (i64, i64),
        to_range: (i64, i64),
    ) -> Result<MessageDiff> {
        let store = self.messages.read();
        let topic_messages = store
            .get(topic)
            .ok_or_else(|| StreamlineError::storage_msg(format!("Topic '{}' not found", topic)))?;

        let collect_by_key = |start: i64, end: i64| -> HashMap<String, TimeTravelMessage> {
            let mut map = HashMap::new();
            for msg in topic_messages {
                if msg.timestamp >= start && msg.timestamp < end {
                    if let Some(ref k) = msg.key {
                        map.insert(k.clone(), msg.clone());
                    }
                }
            }
            map
        };

        let before = collect_by_key(from_range.0, from_range.1);
        let after = collect_by_key(to_range.0, to_range.1);

        let mut added = Vec::new();
        let mut removed = Vec::new();
        let mut modified = Vec::new();
        let mut unchanged_count: usize = 0;

        for (key, msg) in &after {
            match before.get(key) {
                None => added.push(msg.clone()),
                Some(old) => {
                    if old.value_preview != msg.value_preview
                        || old.value_size_bytes != msg.value_size_bytes
                    {
                        let mut fields_changed = Vec::new();
                        if old.value_preview != msg.value_preview {
                            fields_changed.push("value".to_string());
                        }
                        if old.value_size_bytes != msg.value_size_bytes {
                            fields_changed.push("value_size".to_string());
                        }
                        if old.headers != msg.headers {
                            fields_changed.push("headers".to_string());
                        }

                        modified.push(MessageChange {
                            key: key.clone(),
                            before: old.clone(),
                            after: msg.clone(),
                            fields_changed,
                        });
                    } else {
                        unchanged_count += 1;
                    }
                }
            }
        }

        for (key, msg) in &before {
            if !after.contains_key(key) {
                removed.push(msg.clone());
            }
        }

        Ok(MessageDiff {
            added,
            removed,
            modified,
            unchanged_count,
        })
    }

    /// Replay messages from one topic's time range into a target topic within the engine.
    pub fn replay(
        &self,
        topic: &str,
        from_time: i64,
        to_time: i64,
        target_topic: &str,
    ) -> Result<ReplayResult> {
        let start = Instant::now();

        let to_replay: Vec<TimeTravelMessage> = {
            let store = self.messages.read();
            let topic_messages = store.get(topic).ok_or_else(|| {
                StreamlineError::storage_msg(format!("Topic '{}' not found", topic))
            })?;

            topic_messages
                .iter()
                .filter(|m| m.timestamp >= from_time && m.timestamp < to_time)
                .cloned()
                .collect()
        };

        let count = to_replay.len();
        {
            let mut store = self.messages.write();
            let target = store.entry(target_topic.to_string()).or_default();
            target.extend(to_replay);
        }

        Ok(ReplayResult {
            source_topic: topic.to_string(),
            target_topic: target_topic.to_string(),
            messages_replayed: count,
            from_time,
            to_time,
            duration_ms: start.elapsed().as_millis() as u64,
        })
    }

    /// Find the offset of the first message at or after the given timestamp.
    pub fn find_offset_for_timestamp(
        &self,
        topic: &str,
        partition: i32,
        timestamp: i64,
    ) -> Result<i64> {
        let store = self.messages.read();
        let topic_messages = store
            .get(topic)
            .ok_or_else(|| StreamlineError::storage_msg(format!("Topic '{}' not found", topic)))?;

        topic_messages
            .iter()
            .filter(|m| m.partition == partition && m.timestamp >= timestamp)
            .map(|m| m.offset)
            .next()
            .ok_or_else(|| {
                StreamlineError::storage_msg(format!(
                    "No message found at or after timestamp {} in {}/{}",
                    timestamp, topic, partition
                ))
            })
    }
}

impl Default for TimeTravelEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_msg(
        offset: i64,
        timestamp: i64,
        partition: i32,
        key: Option<&str>,
        value: &str,
    ) -> TimeTravelMessage {
        TimeTravelMessage {
            offset,
            timestamp,
            partition,
            key: key.map(|k| k.to_string()),
            value_preview: value.chars().take(200).collect(),
            value_size_bytes: value.len(),
            headers: HashMap::new(),
        }
    }

    fn seeded_engine() -> TimeTravelEngine {
        let engine = TimeTravelEngine::new();
        let topic = "events";

        engine.ingest(topic, make_msg(0, 1000, 0, Some("a"), "v1"));
        engine.ingest(topic, make_msg(1, 2000, 0, Some("b"), "v2"));
        engine.ingest(topic, make_msg(2, 3000, 1, Some("a"), "v3"));
        engine.ingest(topic, make_msg(3, 4000, 0, Some("c"), "v4"));
        engine.ingest(topic, make_msg(4, 5000, 1, Some("b"), "v5-changed"));

        engine
    }

    #[test]
    fn test_query_time_range() {
        let engine = seeded_engine();
        let q = TimeTravelQuery::new("events", 1000, 4000);
        let result = engine.query(q).unwrap();

        assert_eq!(result.messages.len(), 3);
        assert!(result.total_scanned > 0);
    }

    #[test]
    fn test_query_partition_filter() {
        let engine = seeded_engine();
        let q = TimeTravelQuery::new("events", 0, 6000).with_partition(1);
        let result = engine.query(q).unwrap();

        assert_eq!(result.messages.len(), 2);
        assert!(result.messages.iter().all(|m| m.partition == 1));
    }

    #[test]
    fn test_query_key_filter() {
        let engine = seeded_engine();
        let q = TimeTravelQuery::new("events", 0, 6000).with_key_filter("a");
        let result = engine.query(q).unwrap();

        assert_eq!(result.messages.len(), 2);
    }

    #[test]
    fn test_diff_added_removed_modified() {
        let engine = TimeTravelEngine::new();
        let topic = "diff-topic";

        // Before range [1000, 2000): keys x, y
        engine.ingest(topic, make_msg(0, 1000, 0, Some("x"), "old-x"));
        engine.ingest(topic, make_msg(1, 1500, 0, Some("y"), "old-y"));
        // After range [3000, 4000): keys x (changed), z (new)
        engine.ingest(topic, make_msg(2, 3000, 0, Some("x"), "new-x"));
        engine.ingest(topic, make_msg(3, 3500, 0, Some("z"), "new-z"));

        let diff = engine.diff(topic, (1000, 2000), (3000, 4000)).unwrap();

        assert_eq!(diff.added.len(), 1, "z should be added");
        assert_eq!(diff.removed.len(), 1, "y should be removed");
        assert_eq!(diff.modified.len(), 1, "x should be modified");
        assert_eq!(diff.unchanged_count, 0);
        assert_eq!(diff.modified[0].key, "x");
    }

    #[test]
    fn test_replay() {
        let engine = seeded_engine();
        let result = engine
            .replay("events", 1000, 4000, "events-replay")
            .unwrap();

        assert_eq!(result.messages_replayed, 3);
        assert_eq!(result.source_topic, "events");
        assert_eq!(result.target_topic, "events-replay");

        // Verify target topic has the replayed messages
        let q = TimeTravelQuery::new("events-replay", 0, 6000);
        let target = engine.query(q).unwrap();
        assert_eq!(target.messages.len(), 3);
    }

    #[test]
    fn test_find_offset_for_timestamp() {
        let engine = seeded_engine();
        let offset = engine.find_offset_for_timestamp("events", 0, 2000).unwrap();
        assert_eq!(offset, 1);
    }

    #[test]
    fn test_snapshot_manager_crud() {
        let mgr = SnapshotManager::new();

        let snap = mgr.create_snapshot("events", 1000, "interesting spike");
        assert_eq!(snap.topic, "events");

        let list = mgr.list_snapshots("events");
        assert_eq!(list.len(), 1);

        let fetched = mgr.get_snapshot(&snap.id);
        assert!(fetched.is_some());
        assert_eq!(fetched.unwrap().description, "interesting spike");

        assert!(mgr.delete_snapshot(&snap.id));
        assert!(mgr.list_snapshots("events").is_empty());
    }

    #[test]
    fn test_query_unknown_topic() {
        let engine = TimeTravelEngine::new();
        let q = TimeTravelQuery::new("nonexistent", 0, 1000);
        assert!(engine.query(q).is_err());
    }
}
