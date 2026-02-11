//! Transaction log for persistent state recovery
//!
//! Persists transaction coordinator state to disk so that
//! in-progress transactions can be recovered after server restart.
//! Uses an append-only log with periodic snapshots.

use crate::error::{Result, StreamlineError};
use crate::transaction::state::TransactionMetadata;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use tracing::{debug, info, warn};

/// Entry types in the transaction log
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TransactionLogEntry {
    /// Transaction initiated
    Begin {
        transactional_id: String,
        producer_id: i64,
        producer_epoch: i16,
        timeout_ms: i64,
    },
    /// Partition added to transaction
    AddPartition {
        transactional_id: String,
        topic: String,
        partition: i32,
    },
    /// Consumer group offsets added
    AddOffsets {
        transactional_id: String,
        group_id: String,
    },
    /// Transaction prepared for commit
    PrepareCommit { transactional_id: String },
    /// Transaction prepared for abort
    PrepareAbort { transactional_id: String },
    /// Transaction committed
    Commit { transactional_id: String },
    /// Transaction aborted
    Abort { transactional_id: String },
    /// Snapshot marker (all state before this point is in snapshot file)
    Snapshot { sequence: u64 },
}

/// Transaction log for durability
pub struct TransactionLog {
    /// Directory for log files
    log_dir: PathBuf,
    /// Current log sequence number
    sequence: u64,
    /// Whether persistence is enabled
    enabled: bool,
}

impl TransactionLog {
    /// Create a new transaction log
    pub fn new(data_dir: &Path, enabled: bool) -> Result<Self> {
        let log_dir = data_dir.join("__transaction_log");
        if enabled {
            fs::create_dir_all(&log_dir).map_err(|e| {
                StreamlineError::Storage(format!(
                    "Failed to create transaction log directory: {}",
                    e
                ))
            })?;
        }

        Ok(Self {
            log_dir,
            sequence: 0,
            enabled,
        })
    }

    /// Append an entry to the transaction log
    pub fn append(&mut self, entry: &TransactionLogEntry) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        let json = serde_json::to_string(entry).map_err(|e| {
            StreamlineError::Storage(format!("Failed to serialize transaction log entry: {}", e))
        })?;

        let log_file = self.log_dir.join("current.log");
        let line = format!("{}\n", json);

        fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_file)
            .and_then(|mut f| {
                use std::io::Write;
                f.write_all(line.as_bytes())?;
                f.sync_data()
            })
            .map_err(|e| {
                StreamlineError::Storage(format!("Failed to write transaction log: {}", e))
            })?;

        self.sequence += 1;
        Ok(())
    }

    /// Write a snapshot of current transaction state
    pub fn write_snapshot(&self, transactions: &[TransactionMetadata]) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        let snapshot_file = self.log_dir.join("snapshot.json");
        let json = serde_json::to_string_pretty(transactions).map_err(|e| {
            StreamlineError::Storage(format!("Failed to serialize snapshot: {}", e))
        })?;

        // Write to temp file first, then rename for atomicity
        let temp_file = self.log_dir.join("snapshot.json.tmp");
        fs::write(&temp_file, &json).map_err(|e| {
            StreamlineError::Storage(format!("Failed to write snapshot: {}", e))
        })?;
        fs::rename(&temp_file, &snapshot_file).map_err(|e| {
            StreamlineError::Storage(format!("Failed to finalize snapshot: {}", e))
        })?;

        info!(
            count = transactions.len(),
            "Transaction log snapshot written"
        );
        Ok(())
    }

    /// Load transactions from snapshot
    pub fn load_snapshot(&self) -> Result<Vec<TransactionMetadata>> {
        if !self.enabled {
            return Ok(Vec::new());
        }

        let snapshot_file = self.log_dir.join("snapshot.json");
        if !snapshot_file.exists() {
            debug!("No transaction snapshot found, starting fresh");
            return Ok(Vec::new());
        }

        let json = fs::read_to_string(&snapshot_file).map_err(|e| {
            StreamlineError::Storage(format!("Failed to read snapshot: {}", e))
        })?;

        let metadata: Vec<TransactionMetadata> = serde_json::from_str(&json).map_err(|e| {
            StreamlineError::Storage(format!("Failed to parse snapshot: {}", e))
        })?;

        info!(
            count = metadata.len(),
            "Loaded transactions from snapshot"
        );
        Ok(metadata)
    }

    /// Replay log entries since last snapshot
    pub fn replay_log(&self) -> Result<Vec<TransactionLogEntry>> {
        if !self.enabled {
            return Ok(Vec::new());
        }

        let log_file = self.log_dir.join("current.log");
        if !log_file.exists() {
            return Ok(Vec::new());
        }

        let content = fs::read_to_string(&log_file).map_err(|e| {
            StreamlineError::Storage(format!("Failed to read transaction log: {}", e))
        })?;

        let mut entries = Vec::new();
        for line in content.lines() {
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str::<TransactionLogEntry>(line) {
                Ok(entry) => entries.push(entry),
                Err(e) => {
                    warn!("Skipping corrupted transaction log entry: {}", e);
                }
            }
        }

        info!(
            count = entries.len(),
            "Replayed transaction log entries"
        );
        Ok(entries)
    }

    /// Truncate the log (after snapshot)
    pub fn truncate(&self) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        let log_file = self.log_dir.join("current.log");
        if log_file.exists() {
            fs::write(&log_file, "").map_err(|e| {
                StreamlineError::Storage(format!("Failed to truncate transaction log: {}", e))
            })?;
        }

        debug!("Transaction log truncated");
        Ok(())
    }

    /// Get the log directory path
    pub fn log_dir(&self) -> &Path {
        &self.log_dir
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_transaction_log_append_and_replay() {
        let tmp = TempDir::new().unwrap();
        let mut log = TransactionLog::new(tmp.path(), true).unwrap();

        log.append(&TransactionLogEntry::Begin {
            transactional_id: "txn-1".to_string(),
            producer_id: 1000,
            producer_epoch: 0,
            timeout_ms: 60000,
        })
        .unwrap();

        log.append(&TransactionLogEntry::AddPartition {
            transactional_id: "txn-1".to_string(),
            topic: "topic-1".to_string(),
            partition: 0,
        })
        .unwrap();

        log.append(&TransactionLogEntry::Commit {
            transactional_id: "txn-1".to_string(),
        })
        .unwrap();

        let entries = log.replay_log().unwrap();
        assert_eq!(entries.len(), 3);
    }

    #[test]
    fn test_transaction_log_snapshot() {
        let tmp = TempDir::new().unwrap();
        let log = TransactionLog::new(tmp.path(), true).unwrap();

        let metadata = vec![TransactionMetadata {
            transactional_id: "txn-1".to_string(),
            producer_id: 1000,
            producer_epoch: 0,
            state: crate::transaction::state::TransactionState::Ongoing,
            partitions: vec![],
            pending_offset_groups: vec![],
            timeout_ms: 60000,
            last_update_ms: 0,
        }];

        log.write_snapshot(&metadata).unwrap();
        let loaded = log.load_snapshot().unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].transactional_id, "txn-1");
    }

    #[test]
    fn test_transaction_log_disabled() {
        let tmp = TempDir::new().unwrap();
        let mut log = TransactionLog::new(tmp.path(), false).unwrap();

        log.append(&TransactionLogEntry::Begin {
            transactional_id: "txn-1".to_string(),
            producer_id: 1000,
            producer_epoch: 0,
            timeout_ms: 60000,
        })
        .unwrap();

        let entries = log.replay_log().unwrap();
        assert_eq!(entries.len(), 0);
    }

    #[test]
    fn test_transaction_log_truncate() {
        let tmp = TempDir::new().unwrap();
        let mut log = TransactionLog::new(tmp.path(), true).unwrap();

        log.append(&TransactionLogEntry::Begin {
            transactional_id: "txn-1".to_string(),
            producer_id: 1000,
            producer_epoch: 0,
            timeout_ms: 60000,
        })
        .unwrap();

        log.truncate().unwrap();
        let entries = log.replay_log().unwrap();
        assert_eq!(entries.len(), 0);
    }
}
