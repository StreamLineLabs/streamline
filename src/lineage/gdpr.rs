//! GDPR delete-by-key support for data lineage.
//!
//! Provides tracked deletion requests that write tombstone records across
//! specified topics, satisfying right-to-erasure obligations.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

use crate::error::{Result, StreamlineError};
use crate::storage::TopicManager;

/// Status of a GDPR deletion request.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DeletionStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
}

/// A tracked GDPR deletion request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeletionRequest {
    /// Unique request identifier.
    pub id: String,
    /// Data subject identifier (e.g. user ID, email).
    pub subject_id: String,
    /// Reason for the deletion (e.g. "GDPR Article 17 request").
    pub reason: String,
    /// Current status.
    pub status: DeletionStatus,
    /// When the request was created (epoch ms).
    pub created_at: i64,
    /// When the request completed (epoch ms), if applicable.
    pub completed_at: Option<i64>,
    /// Topics that were scanned for matching records.
    pub affected_topics: Vec<String>,
    /// Total number of tombstone records written.
    pub records_deleted: u64,
}

/// Manages GDPR deletion operations against a [`TopicManager`].
pub struct GdprManager {
    topic_manager: Arc<TopicManager>,
    requests: Vec<DeletionRequest>,
    /// Optional path for persisting the audit trail.
    persistence_path: Option<std::path::PathBuf>,
}

impl GdprManager {
    /// Create a new GDPR manager backed by the given topic manager.
    pub fn new(topic_manager: Arc<TopicManager>) -> Self {
        Self {
            topic_manager,
            requests: Vec::new(),
            persistence_path: None,
        }
    }

    /// Create a GDPR manager with persistence to the given directory.
    ///
    /// Deletion requests are saved to `<dir>/gdpr_audit_trail.json` and
    /// loaded on creation.
    pub fn with_persistence(
        topic_manager: Arc<TopicManager>,
        dir: impl Into<std::path::PathBuf>,
    ) -> Self {
        let dir = dir.into();
        let persistence_path = dir.join("gdpr_audit_trail.json");
        let mut mgr = Self {
            topic_manager,
            requests: Vec::new(),
            persistence_path: Some(persistence_path),
        };
        mgr.load_audit_trail();
        mgr
    }

    /// Load deletion requests from the audit trail file.
    fn load_audit_trail(&mut self) {
        let path = match &self.persistence_path {
            Some(p) => p.clone(),
            None => return,
        };
        if !path.exists() {
            return;
        }
        match std::fs::read_to_string(&path) {
            Ok(data) => match serde_json::from_str::<Vec<DeletionRequest>>(&data) {
                Ok(requests) => {
                    let count = requests.len();
                    self.requests = requests;
                    if count > 0 {
                        tracing::info!(count, "Loaded GDPR audit trail");
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Failed to parse GDPR audit trail");
                }
            },
            Err(e) => {
                tracing::warn!(error = %e, "Failed to read GDPR audit trail file");
            }
        }
    }

    /// Persist the current deletion requests to disk.
    fn save_audit_trail(&self) {
        let path = match &self.persistence_path {
            Some(p) => p,
            None => return,
        };
        match serde_json::to_string_pretty(&self.requests) {
            Ok(data) => {
                if let Err(e) = std::fs::write(path, data) {
                    tracing::warn!(error = %e, "Failed to persist GDPR audit trail");
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "Failed to serialize GDPR audit trail");
            }
        }
    }

    /// Return the number of tracked deletion requests.
    pub fn request_count(&self) -> usize {
        self.requests.len()
    }

    /// Create a tracked deletion request without executing it yet.
    pub fn create_deletion_request(&mut self, subject_id: &str, reason: &str) -> &DeletionRequest {
        let request = DeletionRequest {
            id: Uuid::new_v4().to_string(),
            subject_id: subject_id.to_string(),
            reason: reason.to_string(),
            status: DeletionStatus::Pending,
            created_at: chrono::Utc::now().timestamp_millis(),
            completed_at: None,
            affected_topics: Vec::new(),
            records_deleted: 0,
        };
        self.requests.push(request);
        self.save_audit_trail();
        self.requests.last().unwrap()
    }

    /// Return all tracked deletion requests.
    pub fn get_deletion_requests(&self) -> &[DeletionRequest] {
        &self.requests
    }

    /// Delete all records matching `key` across the given `topics` by writing
    /// tombstone records (null value with the same key). Returns the total
    /// number of tombstones written.
    ///
    /// Each matching record triggers a tombstone on the same partition so that
    /// downstream consumers and compaction will eventually remove the data.
    pub fn delete_by_key(&mut self, key: &[u8], topics: &[String]) -> Result<u64> {
        // Find or create a request for this operation.
        let request_idx = {
            let request = DeletionRequest {
                id: Uuid::new_v4().to_string(),
                subject_id: String::from_utf8_lossy(key).to_string(),
                reason: "delete_by_key".to_string(),
                status: DeletionStatus::InProgress,
                created_at: chrono::Utc::now().timestamp_millis(),
                completed_at: None,
                affected_topics: topics.to_vec(),
                records_deleted: 0,
            };
            self.requests.push(request);
            self.requests.len() - 1
        };

        let mut total_deleted: u64 = 0;
        let key_bytes = Bytes::copy_from_slice(key);

        for topic in topics {
            match self.delete_key_from_topic(&key_bytes, topic) {
                Ok(count) => total_deleted += count,
                Err(StreamlineError::TopicNotFound(_)) => {
                    // Skip topics that don't exist.
                    continue;
                }
                Err(e) => {
                    self.requests[request_idx].status = DeletionStatus::Failed;
                    return Err(e);
                }
            }
        }

        let req = &mut self.requests[request_idx];
        req.records_deleted = total_deleted;
        req.status = DeletionStatus::Completed;
        req.completed_at = Some(chrono::Utc::now().timestamp_millis());

        self.save_audit_trail();
        Ok(total_deleted)
    }

    /// Execute a pending deletion request by key across its affected topics.
    pub fn execute_request(&mut self, request_id: &str, topics: &[String]) -> Result<u64> {
        let idx = self
            .requests
            .iter()
            .position(|r| r.id == request_id)
            .ok_or_else(|| StreamlineError::Storage(format!("deletion request not found: {}", request_id)))?;

        let subject_id = self.requests[idx].subject_id.clone();
        self.requests[idx].status = DeletionStatus::InProgress;
        self.requests[idx].affected_topics = topics.to_vec();

        let key_bytes = Bytes::from(subject_id.clone().into_bytes());
        let mut total_deleted: u64 = 0;

        for topic in topics {
            match self.delete_key_from_topic(&key_bytes, topic) {
                Ok(count) => total_deleted += count,
                Err(StreamlineError::TopicNotFound(_)) => continue,
                Err(e) => {
                    self.requests[idx].status = DeletionStatus::Failed;
                    return Err(e);
                }
            }
        }

        let req = &mut self.requests[idx];
        req.records_deleted = total_deleted;
        req.status = DeletionStatus::Completed;
        req.completed_at = Some(chrono::Utc::now().timestamp_millis());

        self.save_audit_trail();
        Ok(total_deleted)
    }

    /// Scan a single topic for records whose key matches `key_bytes` and write
    /// a tombstone (empty value) for each match on the same partition.
    fn delete_key_from_topic(&self, key_bytes: &Bytes, topic: &str) -> Result<u64> {
        let metadata = self.topic_manager.get_topic_metadata(topic)?;
        let num_partitions = metadata.num_partitions;
        let mut deleted: u64 = 0;

        for partition in 0..num_partitions {
            let start = self.topic_manager.earliest_offset(topic, partition)?;
            let end = self.topic_manager.high_watermark(topic, partition)?;

            if start >= end {
                continue;
            }

            let batch_size: usize = 1000;
            let mut offset = start;

            while offset < end {
                let records = self.topic_manager.read(topic, partition, offset, batch_size)?;
                if records.is_empty() {
                    break;
                }

                for record in &records {
                    if record.key.as_ref() == Some(key_bytes) {
                        // Write a tombstone: same key, empty value.
                        self.topic_manager.append(
                            topic,
                            partition,
                            Some(key_bytes.clone()),
                            Bytes::new(),
                        )?;
                        deleted += 1;
                    }
                }

                offset = records.last().map(|r| r.offset + 1).unwrap_or(end);
            }
        }

        Ok(deleted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: build a TopicManager backed by a temp directory.
    fn make_topic_manager() -> (Arc<TopicManager>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let tm = Arc::new(TopicManager::new(dir.path()).unwrap());
        (tm, dir)
    }

    #[test]
    fn test_create_deletion_request() {
        let (tm, _dir) = make_topic_manager();
        let mut mgr = GdprManager::new(tm);

        let req = mgr.create_deletion_request("user-42", "GDPR Article 17");
        assert_eq!(req.subject_id, "user-42");
        assert_eq!(req.reason, "GDPR Article 17");
        assert_eq!(req.status, DeletionStatus::Pending);
        assert!(req.completed_at.is_none());
    }

    #[test]
    fn test_get_deletion_requests() {
        let (tm, _dir) = make_topic_manager();
        let mut mgr = GdprManager::new(tm);

        mgr.create_deletion_request("user-1", "reason-1");
        mgr.create_deletion_request("user-2", "reason-2");

        let requests = mgr.get_deletion_requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].subject_id, "user-1");
        assert_eq!(requests[1].subject_id, "user-2");
    }

    #[test]
    fn test_delete_by_key_nonexistent_topic() {
        let (tm, _dir) = make_topic_manager();
        let mut mgr = GdprManager::new(tm);

        let result = mgr.delete_by_key(b"user-42", &["no-such-topic".to_string()]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0);

        let requests = mgr.get_deletion_requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].status, DeletionStatus::Completed);
    }

    #[test]
    fn test_delete_by_key_with_data() {
        let (tm, _dir) = make_topic_manager();
        tm.get_or_create_topic("test-topic", 1).unwrap();

        // Write some records: two matching, one not.
        tm.append("test-topic", 0, Some(Bytes::from("user-42")), Bytes::from("data-a"))
            .unwrap();
        tm.append("test-topic", 0, Some(Bytes::from("user-99")), Bytes::from("data-b"))
            .unwrap();
        tm.append("test-topic", 0, Some(Bytes::from("user-42")), Bytes::from("data-c"))
            .unwrap();

        let mut mgr = GdprManager::new(tm.clone());
        let deleted = mgr.delete_by_key(b"user-42", &["test-topic".to_string()]).unwrap();

        assert_eq!(deleted, 2);

        let requests = mgr.get_deletion_requests();
        assert_eq!(requests[0].status, DeletionStatus::Completed);
        assert_eq!(requests[0].records_deleted, 2);
        assert!(requests[0].completed_at.is_some());
    }

    #[test]
    fn test_execute_request() {
        let (tm, _dir) = make_topic_manager();
        tm.get_or_create_topic("orders", 1).unwrap();

        tm.append("orders", 0, Some(Bytes::from("subj-1")), Bytes::from("order-data"))
            .unwrap();

        let mut mgr = GdprManager::new(tm);
        let req_id = mgr.create_deletion_request("subj-1", "user request").id.clone();

        let deleted = mgr.execute_request(&req_id, &["orders".to_string()]).unwrap();
        assert_eq!(deleted, 1);

        let req = mgr.get_deletion_requests().iter().find(|r| r.id == req_id).unwrap();
        assert_eq!(req.status, DeletionStatus::Completed);
    }

    #[test]
    fn test_execute_request_not_found() {
        let (tm, _dir) = make_topic_manager();
        let mut mgr = GdprManager::new(tm);

        let result = mgr.execute_request("nonexistent-id", &[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_persistence_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        let tm = Arc::new(TopicManager::new(dir.path()).unwrap());
        tm.get_or_create_topic("events", 1).unwrap();
        tm.append("events", 0, Some(Bytes::from("user-1")), Bytes::from("data"))
            .unwrap();

        let persist_dir = dir.path().join("gdpr");
        std::fs::create_dir_all(&persist_dir).unwrap();

        // Create requests with persistence
        let req_id;
        {
            let mut mgr = GdprManager::with_persistence(tm.clone(), &persist_dir);
            assert_eq!(mgr.request_count(), 0);

            req_id = mgr.create_deletion_request("user-1", "GDPR Art. 17").id.clone();
            mgr.execute_request(&req_id, &["events".to_string()]).unwrap();

            assert_eq!(mgr.request_count(), 1);
            assert_eq!(mgr.get_deletion_requests()[0].status, DeletionStatus::Completed);
        }

        // "Restart" — create new manager from same directory
        {
            let mgr = GdprManager::with_persistence(tm.clone(), &persist_dir);
            assert_eq!(mgr.request_count(), 1);
            let req = &mgr.get_deletion_requests()[0];
            assert_eq!(req.id, req_id);
            assert_eq!(req.subject_id, "user-1");
            assert_eq!(req.status, DeletionStatus::Completed);
            assert!(req.completed_at.is_some());
            assert_eq!(req.records_deleted, 1);
        }
    }

    #[test]
    fn test_persistence_multiple_requests() {
        let dir = tempfile::tempdir().unwrap();
        let tm = Arc::new(TopicManager::new(dir.path()).unwrap());
        let persist_dir = dir.path().join("gdpr");
        std::fs::create_dir_all(&persist_dir).unwrap();

        // Create multiple requests
        {
            let mut mgr = GdprManager::with_persistence(tm.clone(), &persist_dir);
            mgr.create_deletion_request("user-a", "request 1");
            mgr.create_deletion_request("user-b", "request 2");
            mgr.create_deletion_request("user-c", "request 3");
            assert_eq!(mgr.request_count(), 3);
        }

        // Verify they all persist
        {
            let mgr = GdprManager::with_persistence(tm.clone(), &persist_dir);
            assert_eq!(mgr.request_count(), 3);
            let subjects: Vec<&str> = mgr.get_deletion_requests().iter().map(|r| r.subject_id.as_str()).collect();
            assert!(subjects.contains(&"user-a"));
            assert!(subjects.contains(&"user-b"));
            assert!(subjects.contains(&"user-c"));
        }
    }

    #[test]
    fn test_no_persistence_without_config() {
        let (tm, _dir) = make_topic_manager();
        let mut mgr = GdprManager::new(tm);

        // Create request without persistence
        mgr.create_deletion_request("user-x", "no-persist");
        assert_eq!(mgr.request_count(), 1);

        // No file should be written (persistence_path is None)
        // This just verifies the non-persistent path doesn't crash
    }

    #[test]
    fn test_persistence_with_delete_by_key() {
        let dir = tempfile::tempdir().unwrap();
        let tm = Arc::new(TopicManager::new(dir.path()).unwrap());
        tm.get_or_create_topic("logs", 1).unwrap();
        tm.append("logs", 0, Some(Bytes::from("k1")), Bytes::from("v1")).unwrap();
        tm.append("logs", 0, Some(Bytes::from("k1")), Bytes::from("v2")).unwrap();

        let persist_dir = dir.path().join("gdpr");
        std::fs::create_dir_all(&persist_dir).unwrap();

        {
            let mut mgr = GdprManager::with_persistence(tm.clone(), &persist_dir);
            let deleted = mgr.delete_by_key(b"k1", &["logs".to_string()]).unwrap();
            assert_eq!(deleted, 2);
        }

        // Verify persisted after restart
        {
            let mgr = GdprManager::with_persistence(tm.clone(), &persist_dir);
            assert_eq!(mgr.request_count(), 1);
            let req = &mgr.get_deletion_requests()[0];
            assert_eq!(req.records_deleted, 2);
            assert_eq!(req.status, DeletionStatus::Completed);
        }
    }
}
