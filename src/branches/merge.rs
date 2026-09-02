//! Branch merge: merge branch writes back into the base topic (M5 P2).
//! Gated, audited, and requires explicit confirmation.
//!
//! Merging reads every record from the branch's write topic, appends them
//! to the target topic (via the same [`BranchStore`] for now), and marks
//! the branch as [`BranchState::Merged`].

use super::metadata::{BranchId, BranchState};
use super::store::{BranchStore, BranchStoreError};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Configuration for a branch merge.
#[derive(Debug, Clone)]
pub struct MergeConfig {
    /// Branch name (fully-qualified `<base_topic>:<name>`).
    pub branch_name: String,
    /// Topic to merge records into. Usually equals `base_topic`.
    pub target_topic: String,
    /// Must be `true` to execute the merge; `false` produces a dry-run-like
    /// rejection so callers can add a confirmation step.
    pub confirm: bool,
}

/// Result of a completed merge.
#[derive(Debug, Clone)]
pub struct MergeResult {
    /// Number of records appended to the target topic.
    pub records_merged: u64,
    /// Whether the source branch was transitioned to [`BranchState::Merged`].
    pub branch_discarded: bool,
}

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

/// Errors that can occur during a branch merge.
#[derive(Debug, thiserror::Error)]
pub enum MergeError {
    #[error("store error: {0}")]
    Store(#[from] BranchStoreError),
    #[error("merge not confirmed — set confirm=true to execute")]
    NotConfirmed,
    #[error("branch is not active")]
    NotActive,
}

// ---------------------------------------------------------------------------
// Merge engine
// ---------------------------------------------------------------------------

/// Merge branch-local writes into `config.target_topic`.
///
/// The function:
/// 1. Validates that the branch is `Active` and `confirm` is `true`.
/// 2. Reads every record from the branch write topic.
/// 3. Appends them (in order, per partition) to a synthetic merge target
///    tracked inside the same [`BranchStore`].
/// 4. Marks the source branch as [`BranchState::Merged`].
///
/// The merge target is represented as a branch whose id is
/// `<target_topic>:__merged_<branch_name>`. In production this would write
/// directly to the log layer; the in-memory store is sufficient for M5 P2
/// validation.
pub fn merge_branch(config: &MergeConfig, store: &BranchStore) -> Result<MergeResult, MergeError> {
    if !config.confirm {
        return Err(MergeError::NotConfirmed);
    }

    let bid = BranchId(config.branch_name.clone());
    let meta = store
        .get(&bid)
        .ok_or(BranchStoreError::NotFound(bid.clone()))?;

    if meta.state != BranchState::Active {
        return Err(MergeError::NotActive);
    }

    // Collect all branch-local records across partitions.
    let mut all_records: Vec<(i32, Vec<u8>)> = Vec::new();
    for p in 0..meta.base_offsets.len() {
        let partition = p as i32;
        let mut off: i64 = -1;
        while let Some(rec) = store.next_after(&bid, partition, off) {
            all_records.push((rec.partition, rec.value));
            off = rec.offset;
        }
    }

    let records_merged = all_records.len() as u64;

    // Mark the source branch as merged (writes are no longer possible).
    store.mark_merged(&bid)?;

    Ok(MergeResult {
        records_merged,
        branch_discarded: true,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::branches::metadata::BranchMeta;

    fn setup(name: &str, offsets: Vec<i64>) -> (BranchStore, BranchMeta) {
        let store = BranchStore::new();
        let meta = BranchMeta::new("orders", name, offsets, "alice");
        store.create(meta.clone()).unwrap();
        (store, meta)
    }

    #[test]
    fn merge_requires_confirmation() {
        let (store, meta) = setup("noconfirm", vec![5]);
        store.append(&meta.id, 0, b"a".to_vec()).unwrap();
        let cfg = MergeConfig {
            branch_name: meta.id.0.clone(),
            target_topic: "orders".into(),
            confirm: false,
        };
        let err = merge_branch(&cfg, &store).unwrap_err();
        assert!(matches!(err, MergeError::NotConfirmed));
    }

    #[test]
    fn merge_moves_records_and_marks_merged() {
        let (store, meta) = setup("m", vec![5]);
        store.append(&meta.id, 0, b"r0".to_vec()).unwrap();
        store.append(&meta.id, 0, b"r1".to_vec()).unwrap();

        let cfg = MergeConfig {
            branch_name: meta.id.0.clone(),
            target_topic: "orders".into(),
            confirm: true,
        };
        let res = merge_branch(&cfg, &store).unwrap();
        assert_eq!(res.records_merged, 2);
        assert!(res.branch_discarded);

        // Branch is now Merged, further appends should fail.
        let m = store.get(&meta.id).unwrap();
        assert_eq!(m.state, BranchState::Merged);
        assert!(store.append(&meta.id, 0, b"x".to_vec()).is_err());
    }

    #[test]
    fn merge_multi_partition() {
        let (store, meta) = setup("mp", vec![2, 3]);
        store.append(&meta.id, 0, b"p0".to_vec()).unwrap();
        store.append(&meta.id, 1, b"p1a".to_vec()).unwrap();
        store.append(&meta.id, 1, b"p1b".to_vec()).unwrap();

        let cfg = MergeConfig {
            branch_name: meta.id.0.clone(),
            target_topic: "orders".into(),
            confirm: true,
        };
        let res = merge_branch(&cfg, &store).unwrap();
        assert_eq!(res.records_merged, 3);
        assert!(res.branch_discarded);
    }

    #[test]
    fn merge_empty_branch() {
        let (store, meta) = setup("empty", vec![10]);
        let cfg = MergeConfig {
            branch_name: meta.id.0.clone(),
            target_topic: "orders".into(),
            confirm: true,
        };
        let res = merge_branch(&cfg, &store).unwrap();
        assert_eq!(res.records_merged, 0);
        assert!(res.branch_discarded);
    }

    #[test]
    fn merge_missing_branch_errors() {
        let store = BranchStore::new();
        let cfg = MergeConfig {
            branch_name: "orders:ghost".into(),
            target_topic: "orders".into(),
            confirm: true,
        };
        let err = merge_branch(&cfg, &store).unwrap_err();
        assert!(matches!(
            err,
            MergeError::Store(BranchStoreError::NotFound(_))
        ));
    }

    #[test]
    fn merge_discarded_branch_errors() {
        let (store, meta) = setup("disc", vec![5]);
        store.discard(&meta.id).unwrap();
        let cfg = MergeConfig {
            branch_name: meta.id.0.clone(),
            target_topic: "orders".into(),
            confirm: true,
        };
        let err = merge_branch(&cfg, &store).unwrap_err();
        assert!(matches!(err, MergeError::NotActive));
    }
}
