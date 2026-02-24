//! State checkpointing for StreamQL continuous queries.
//!
//! Provides periodic state snapshots so that continuous queries (windowed
//! aggregations, materialized views, joins) can recover from crashes without
//! reprocessing the entire input stream.
//!
//! ## Architecture
//!
//! ```text
//! ContinuousQuery → StateManager → CheckpointStore (disk/memory)
//!                                       │
//!                                 checkpoint files:
//!                                   {query_id}/{epoch}.checkpoint
//! ```

use crate::error::{Result, StreamlineError};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Checkpoint metadata for a continuous query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Checkpoint {
    /// Unique checkpoint ID.
    pub id: u64,
    /// Query ID this checkpoint belongs to.
    pub query_id: String,
    /// Epoch counter (monotonically increasing).
    pub epoch: u64,
    /// When this checkpoint was created.
    pub created_at: DateTime<Utc>,
    /// Source topic offsets at checkpoint time (topic:partition → offset).
    pub source_offsets: HashMap<String, i64>,
    /// Serialized operator state (query-specific).
    pub operator_state: Vec<u8>,
    /// Size in bytes of the serialized state.
    pub state_size_bytes: u64,
    /// Whether this is a full or incremental checkpoint.
    pub checkpoint_type: CheckpointType,
}

/// Type of checkpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CheckpointType {
    /// Complete state snapshot.
    Full,
    /// Delta from the previous checkpoint.
    Incremental,
}

/// Configuration for checkpointing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointConfig {
    /// Interval between checkpoints in milliseconds.
    pub interval_ms: u64,
    /// Maximum number of checkpoints to retain per query.
    pub max_retained: usize,
    /// Whether to use incremental checkpoints.
    pub incremental: bool,
    /// Storage backend for checkpoints.
    pub storage: CheckpointStorage,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            interval_ms: 60_000, // 1 minute
            max_retained: 5,
            incremental: true,
            storage: CheckpointStorage::Memory,
        }
    }
}

/// Checkpoint storage backend.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CheckpointStorage {
    /// In-memory (lost on restart).
    Memory,
    /// Local filesystem.
    Disk { path: PathBuf },
}

/// Manages checkpoints for all continuous queries.
pub struct CheckpointManager {
    config: CheckpointConfig,
    checkpoints: Arc<RwLock<HashMap<String, Vec<Checkpoint>>>>,
    next_id: std::sync::atomic::AtomicU64,
}

impl CheckpointManager {
    /// Create a new checkpoint manager.
    pub fn new(config: CheckpointConfig) -> Self {
        Self {
            config,
            checkpoints: Arc::new(RwLock::new(HashMap::new())),
            next_id: std::sync::atomic::AtomicU64::new(1),
        }
    }

    /// Create a checkpoint for a query.
    pub async fn create_checkpoint(
        &self,
        query_id: &str,
        source_offsets: HashMap<String, i64>,
        operator_state: Vec<u8>,
    ) -> Result<Checkpoint> {
        let mut all = self.checkpoints.write().await;
        let query_checkpoints = all.entry(query_id.to_string()).or_default();

        let epoch = query_checkpoints.last().map_or(1, |c| c.epoch + 1);
        let state_size = operator_state.len() as u64;

        let checkpoint_type = if self.config.incremental && epoch > 1 {
            CheckpointType::Incremental
        } else {
            CheckpointType::Full
        };

        let checkpoint = Checkpoint {
            id: self
                .next_id
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            query_id: query_id.to_string(),
            epoch,
            created_at: Utc::now(),
            source_offsets,
            operator_state,
            state_size_bytes: state_size,
            checkpoint_type,
        };

        query_checkpoints.push(checkpoint.clone());

        // Trim old checkpoints
        while query_checkpoints.len() > self.config.max_retained {
            query_checkpoints.remove(0);
        }

        info!(
            query_id,
            epoch,
            state_bytes = state_size,
            "Created checkpoint"
        );

        Ok(checkpoint)
    }

    /// Get the latest checkpoint for a query.
    pub async fn latest_checkpoint(&self, query_id: &str) -> Option<Checkpoint> {
        let all = self.checkpoints.read().await;
        all.get(query_id).and_then(|v| v.last().cloned())
    }

    /// Restore state from the latest checkpoint.
    ///
    /// Returns the source offsets and serialized operator state to resume from.
    pub async fn restore(
        &self,
        query_id: &str,
    ) -> Option<(HashMap<String, i64>, Vec<u8>)> {
        let checkpoint = self.latest_checkpoint(query_id).await?;
        info!(
            query_id,
            epoch = checkpoint.epoch,
            "Restoring from checkpoint"
        );
        Some((checkpoint.source_offsets, checkpoint.operator_state))
    }

    /// List all checkpoints for a query.
    pub async fn list_checkpoints(&self, query_id: &str) -> Vec<Checkpoint> {
        let all = self.checkpoints.read().await;
        all.get(query_id).cloned().unwrap_or_default()
    }

    /// Delete all checkpoints for a query.
    pub async fn delete_checkpoints(&self, query_id: &str) {
        let mut all = self.checkpoints.write().await;
        all.remove(query_id);
    }

    /// Get statistics about checkpoint storage.
    pub async fn stats(&self) -> CheckpointStats {
        let all = self.checkpoints.read().await;
        let total_checkpoints: usize = all.values().map(|v| v.len()).sum();
        let total_bytes: u64 = all
            .values()
            .flat_map(|v| v.iter())
            .map(|c| c.state_size_bytes)
            .sum();

        CheckpointStats {
            query_count: all.len(),
            total_checkpoints,
            total_state_bytes: total_bytes,
        }
    }
}

/// Checkpoint storage statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointStats {
    pub query_count: usize,
    pub total_checkpoints: usize,
    pub total_state_bytes: u64,
}

/// Backpressure controller for continuous queries.
///
/// Monitors processing lag and applies backpressure when the query
/// cannot keep up with the input rate.
pub struct BackpressureController {
    /// Maximum allowed lag in milliseconds before applying backpressure.
    max_lag_ms: u64,
    /// Current processing lag per query.
    lags: Arc<RwLock<HashMap<String, QueryLag>>>,
}

/// Per-query lag information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryLag {
    /// Query ID.
    pub query_id: String,
    /// Current lag in milliseconds.
    pub lag_ms: u64,
    /// Messages behind the latest offset.
    pub messages_behind: u64,
    /// Whether backpressure is currently active.
    pub backpressure_active: bool,
    /// Last updated timestamp.
    pub updated_at: DateTime<Utc>,
}

impl BackpressureController {
    /// Create a new backpressure controller.
    pub fn new(max_lag_ms: u64) -> Self {
        Self {
            max_lag_ms,
            lags: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Report lag for a query. Returns true if backpressure should be applied.
    pub async fn report_lag(
        &self,
        query_id: &str,
        lag_ms: u64,
        messages_behind: u64,
    ) -> bool {
        let backpressure = lag_ms > self.max_lag_ms;
        let mut lags = self.lags.write().await;
        lags.insert(
            query_id.to_string(),
            QueryLag {
                query_id: query_id.to_string(),
                lag_ms,
                messages_behind,
                backpressure_active: backpressure,
                updated_at: Utc::now(),
            },
        );

        if backpressure {
            warn!(
                query_id,
                lag_ms,
                max_lag_ms = self.max_lag_ms,
                "Backpressure activated"
            );
        }

        backpressure
    }

    /// Check if backpressure is active for a query.
    pub async fn is_backpressured(&self, query_id: &str) -> bool {
        let lags = self.lags.read().await;
        lags.get(query_id)
            .map_or(false, |l| l.backpressure_active)
    }

    /// Get lag info for all queries.
    pub async fn all_lags(&self) -> Vec<QueryLag> {
        let lags = self.lags.read().await;
        lags.values().cloned().collect()
    }

    /// Clear lag info when a query is stopped.
    pub async fn clear(&self, query_id: &str) {
        self.lags.write().await.remove(query_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_and_restore_checkpoint() {
        let mgr = CheckpointManager::new(CheckpointConfig::default());

        let offsets = HashMap::from([("topic:0".to_string(), 100i64)]);
        let state = b"serialized_state_data".to_vec();

        mgr.create_checkpoint("query-1", offsets.clone(), state.clone())
            .await
            .unwrap();

        let restored = mgr.restore("query-1").await;
        assert!(restored.is_some());
        let (r_offsets, r_state) = restored.unwrap();
        assert_eq!(r_offsets["topic:0"], 100);
        assert_eq!(r_state, state);
    }

    #[tokio::test]
    async fn test_checkpoint_retention() {
        let mgr = CheckpointManager::new(CheckpointConfig {
            max_retained: 2,
            ..Default::default()
        });

        for i in 0..5 {
            mgr.create_checkpoint(
                "q1",
                HashMap::from([("t:0".to_string(), i as i64)]),
                vec![i as u8],
            )
            .await
            .unwrap();
        }

        let checkpoints = mgr.list_checkpoints("q1").await;
        assert_eq!(checkpoints.len(), 2);
        assert_eq!(checkpoints[0].epoch, 4); // oldest retained
        assert_eq!(checkpoints[1].epoch, 5); // latest
    }

    #[tokio::test]
    async fn test_checkpoint_stats() {
        let mgr = CheckpointManager::new(CheckpointConfig::default());

        mgr.create_checkpoint("q1", HashMap::new(), vec![1, 2, 3])
            .await
            .unwrap();
        mgr.create_checkpoint("q2", HashMap::new(), vec![4, 5])
            .await
            .unwrap();

        let stats = mgr.stats().await;
        assert_eq!(stats.query_count, 2);
        assert_eq!(stats.total_checkpoints, 2);
        assert_eq!(stats.total_state_bytes, 5);
    }

    #[tokio::test]
    async fn test_delete_checkpoints() {
        let mgr = CheckpointManager::new(CheckpointConfig::default());
        mgr.create_checkpoint("q1", HashMap::new(), vec![])
            .await
            .unwrap();
        mgr.delete_checkpoints("q1").await;
        assert!(mgr.latest_checkpoint("q1").await.is_none());
    }

    #[tokio::test]
    async fn test_backpressure_activation() {
        let ctrl = BackpressureController::new(5000);

        // Below threshold
        let bp = ctrl.report_lag("q1", 1000, 100).await;
        assert!(!bp);
        assert!(!ctrl.is_backpressured("q1").await);

        // Above threshold
        let bp = ctrl.report_lag("q1", 10000, 50000).await;
        assert!(bp);
        assert!(ctrl.is_backpressured("q1").await);
    }

    #[tokio::test]
    async fn test_backpressure_clear() {
        let ctrl = BackpressureController::new(1000);
        ctrl.report_lag("q1", 5000, 1000).await;
        ctrl.clear("q1").await;
        assert!(!ctrl.is_backpressured("q1").await);
    }

    #[tokio::test]
    async fn test_all_lags() {
        let ctrl = BackpressureController::new(5000);
        ctrl.report_lag("q1", 1000, 100).await;
        ctrl.report_lag("q2", 8000, 5000).await;
        let lags = ctrl.all_lags().await;
        assert_eq!(lags.len(), 2);
    }
}
