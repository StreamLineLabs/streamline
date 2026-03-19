//! Branch diff: compare records between base and branch (M5 P2).
//!
//! Provides a count-level comparison between the base topic and a branch's
//! write topic. Optional [`DiffMetric`] support allows attaching named
//! numeric comparisons (e.g. total message bytes, p99 latency).

use super::metadata::{BranchId, BranchState};
use super::store::{BranchStore, BranchStoreError};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Configuration for a diff operation.
#[derive(Debug, Clone)]
pub struct DiffConfig {
    /// Base topic the branch was forked from.
    pub base_topic: String,
    /// Branch name (the short name, not the fully-qualified id).
    pub branch_name: String,
    /// Optional metric to compute on both sides (reserved for future use).
    pub metric: Option<String>,
}

/// Count-level diff result.
#[derive(Debug, Clone)]
pub struct DiffResult {
    /// Total record count on the base side (sum of base_offsets).
    pub base_count: u64,
    /// Total record count on the branch side.
    pub branch_count: u64,
    /// Records present in the branch but absent in the base.
    pub records_added: u64,
    /// Records present in the base but absent in the branch (always 0 for
    /// append-only branches; reserved for future snapshot diffs).
    pub records_removed: u64,
    /// Records whose content differs between base and branch (reserved).
    pub records_modified: u64,
}

/// A named numeric comparison between base and branch.
#[derive(Debug, Clone)]
pub struct DiffMetric {
    pub name: String,
    pub base_value: f64,
    pub branch_value: f64,
    pub delta: f64,
    pub delta_pct: f64,
}

impl DiffMetric {
    /// Creates a new `DiffMetric` and computes the delta and percentage change.
    pub fn new(name: impl Into<String>, base_value: f64, branch_value: f64) -> Self {
        let delta = branch_value - base_value;
        let delta_pct = if base_value.abs() < f64::EPSILON {
            if delta.abs() < f64::EPSILON {
                0.0
            } else {
                f64::INFINITY
            }
        } else {
            (delta / base_value) * 100.0
        };
        Self {
            name: name.into(),
            base_value,
            branch_value,
            delta,
            delta_pct,
        }
    }
}

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

/// Errors that can occur during a branch diff.
#[derive(Debug, thiserror::Error)]
pub enum DiffError {
    #[error("store error: {0}")]
    Store(#[from] BranchStoreError),
    #[error("branch is not active")]
    NotActive,
}

// ---------------------------------------------------------------------------
// Diff engine
// ---------------------------------------------------------------------------

/// Compare record counts between the base topic and a branch.
///
/// The base count is the sum of `base_offsets` from the branch metadata.
/// The branch count is the total number of records written to the branch's
/// write topic. `records_added` is the branch count (since branch writes
/// are strictly additive), and `records_removed` / `records_modified` are
/// always 0 in the current append-only model.
pub fn diff_branches(
    config: &DiffConfig,
    store: &BranchStore,
) -> Result<DiffResult, DiffError> {
    let bid = BranchId::new(&config.base_topic, &config.branch_name);
    let meta = store
        .get(&bid)
        .ok_or(BranchStoreError::NotFound(bid.clone()))?;

    if meta.state != BranchState::Active {
        return Err(DiffError::NotActive);
    }

    let base_count: u64 = meta.base_offsets.iter().map(|&o| o.max(0) as u64).sum();

    // Count branch-local writes across all partitions by walking each
    // partition until `next_after` returns `None`.
    let mut branch_count: u64 = 0;
    for p in 0..meta.base_offsets.len() {
        let mut off: i64 = -1;
        while let Some(rec) = store.next_after(&bid, p as i32, off) {
            branch_count += 1;
            off = rec.offset;
        }
    }

    Ok(DiffResult {
        base_count,
        branch_count,
        records_added: branch_count,
        records_removed: 0,
        records_modified: 0,
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
    fn diff_empty_branch() {
        let (store, _meta) = setup("empty", vec![10, 20]);
        let cfg = DiffConfig {
            base_topic: "orders".into(),
            branch_name: "empty".into(),
            metric: None,
        };
        let res = diff_branches(&cfg, &store).unwrap();
        assert_eq!(res.base_count, 30);
        assert_eq!(res.branch_count, 0);
        assert_eq!(res.records_added, 0);
        assert_eq!(res.records_removed, 0);
        assert_eq!(res.records_modified, 0);
    }

    #[test]
    fn diff_with_branch_writes() {
        let (store, meta) = setup("writes", vec![5]);
        store.append(&meta.id, 0, b"a".to_vec()).unwrap();
        store.append(&meta.id, 0, b"b".to_vec()).unwrap();

        let cfg = DiffConfig {
            base_topic: "orders".into(),
            branch_name: "writes".into(),
            metric: None,
        };
        let res = diff_branches(&cfg, &store).unwrap();
        assert_eq!(res.base_count, 5);
        assert_eq!(res.branch_count, 2);
        assert_eq!(res.records_added, 2);
    }

    #[test]
    fn diff_multi_partition() {
        let (store, meta) = setup("mp", vec![3, 4]);
        store.append(&meta.id, 0, b"p0".to_vec()).unwrap();
        store.append(&meta.id, 1, b"p1a".to_vec()).unwrap();
        store.append(&meta.id, 1, b"p1b".to_vec()).unwrap();

        let cfg = DiffConfig {
            base_topic: "orders".into(),
            branch_name: "mp".into(),
            metric: None,
        };
        let res = diff_branches(&cfg, &store).unwrap();
        assert_eq!(res.base_count, 7);
        assert_eq!(res.branch_count, 3);
        assert_eq!(res.records_added, 3);
    }

    #[test]
    fn diff_missing_branch_errors() {
        let store = BranchStore::new();
        let cfg = DiffConfig {
            base_topic: "orders".into(),
            branch_name: "ghost".into(),
            metric: None,
        };
        let err = diff_branches(&cfg, &store).unwrap_err();
        assert!(matches!(err, DiffError::Store(BranchStoreError::NotFound(_))));
    }

    #[test]
    fn diff_discarded_branch_errors() {
        let (store, meta) = setup("disc", vec![5]);
        store.discard(&meta.id).unwrap();
        let cfg = DiffConfig {
            base_topic: "orders".into(),
            branch_name: "disc".into(),
            metric: None,
        };
        let err = diff_branches(&cfg, &store).unwrap_err();
        assert!(matches!(err, DiffError::NotActive));
    }

    #[test]
    fn diff_metric_computes_delta() {
        let m = DiffMetric::new("msg_bytes", 100.0, 125.0);
        assert_eq!(m.delta, 25.0);
        assert!((m.delta_pct - 25.0).abs() < f64::EPSILON);
    }

    #[test]
    fn diff_metric_zero_base() {
        let m = DiffMetric::new("latency", 0.0, 42.0);
        assert_eq!(m.delta, 42.0);
        assert!(m.delta_pct.is_infinite());
    }

    #[test]
    fn diff_metric_both_zero() {
        let m = DiffMetric::new("empty", 0.0, 0.0);
        assert_eq!(m.delta, 0.0);
        assert_eq!(m.delta_pct, 0.0);
    }
}
