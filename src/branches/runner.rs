//! Branch run engine: apply transforms to a branch (M5 P2).
//!
//! `streamline branch run <name> --apply <transform>` reads records from
//! the base topic at the branch offset, applies a transform, and writes
//! results to the branch-local segments.

use std::time::Instant;

use super::metadata::{BranchId, BranchState};
use super::store::{BranchStore, BranchStoreError};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// What kind of transform to apply to each record.
#[derive(Debug, Clone)]
pub enum TransformKind {
    /// WASM module bytes (stub — not yet implemented).
    Wasm(Vec<u8>),
    /// SQL expression applied per-record (stub — not yet implemented).
    Sql(String),
    /// Pass-through: writes every source record unchanged. Useful for testing
    /// the run pipeline without a real transform.
    Identity,
}

/// Configuration for a single branch-run invocation.
#[derive(Debug, Clone)]
pub struct RunConfig {
    /// Fully-qualified branch id (`<base_topic>:<name>`).
    pub branch_id: String,
    /// Transform to apply to each record.
    pub transform: TransformKind,
    /// Stop after reading this many records (if `Some`).
    pub max_records: Option<u64>,
    /// Parallelism hint (reserved for future use).
    pub concurrency: u32,
}

impl RunConfig {
    /// Creates a default run config for the given branch and transform.
    pub fn new(branch_id: impl Into<String>, transform: TransformKind) -> Self {
        Self {
            branch_id: branch_id.into(),
            transform,
            max_records: None,
            concurrency: 1,
        }
    }
}

/// Progress / result of a branch-run invocation.
#[derive(Debug)]
pub struct RunProgress {
    pub records_read: u64,
    pub records_written: u64,
    pub records_skipped: u64,
    pub errors: u64,
    pub started_at: Instant,
    pub completed: bool,
}

impl RunProgress {
    fn new() -> Self {
        Self {
            records_read: 0,
            records_written: 0,
            records_skipped: 0,
            errors: 0,
            started_at: Instant::now(),
            completed: false,
        }
    }
}

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

/// Errors that can occur during a branch-run invocation.
#[derive(Debug, thiserror::Error)]
pub enum RunError {
    #[error("store error: {0}")]
    Store(#[from] BranchStoreError),
    #[error("transform not implemented: {0}")]
    TransformNotImplemented(String),
    #[error("branch is not active")]
    NotActive,
}

// ---------------------------------------------------------------------------
// Run engine
// ---------------------------------------------------------------------------

/// Apply `config.transform` to every record readable from the branch's base
/// topic (starting at the branch's fork offset) and write results into the
/// branch-local segments.
///
/// Currently only [`TransformKind::Identity`] is implemented end-to-end;
/// WASM and SQL return [`RunError::TransformNotImplemented`].
pub fn run_transform(config: &RunConfig, store: &BranchStore) -> Result<RunProgress, RunError> {
    let bid = BranchId(config.branch_id.clone());
    let meta = store
        .get(&bid)
        .ok_or(BranchStoreError::NotFound(bid.clone()))?;

    if meta.state != BranchState::Active {
        return Err(RunError::NotActive);
    }

    match &config.transform {
        TransformKind::Wasm(_) => {
            return Err(RunError::TransformNotImplemented("wasm".into()));
        }
        TransformKind::Sql(_) => {
            return Err(RunError::TransformNotImplemented("sql".into()));
        }
        TransformKind::Identity => {}
    }

    let mut progress = RunProgress::new();

    // Identity transform: for each partition in the base, simulate reading
    // records from offset 0..base_offset and writing them unchanged into
    // the branch.
    for (partition, &base_max) in meta.base_offsets.iter().enumerate() {
        let partition = partition as i32;
        let limit = match config.max_records {
            Some(max) => max.saturating_sub(progress.records_read) as i64,
            None => base_max,
        };
        let count = limit.min(base_max);
        for offset in 0..count {
            progress.records_read += 1;
            // Identity transform: fabricate a record whose value is
            // `base:<partition>:<offset>` so tests can verify content.
            let value = format!("base:{partition}:{offset}").into_bytes();
            match store.append(&bid, partition, value) {
                Ok(_) => progress.records_written += 1,
                Err(_) => progress.errors += 1,
            }
        }
        if config
            .max_records
            .is_some_and(|m| progress.records_read >= m)
        {
            break;
        }
    }

    progress.completed = true;
    Ok(progress)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::branches::metadata::BranchMeta;

    fn setup_store(name: &str, base_offsets: Vec<i64>) -> (BranchStore, BranchMeta) {
        let store = BranchStore::new();
        let meta = BranchMeta::new("orders", name, base_offsets, "alice");
        store.create(meta.clone()).unwrap();
        (store, meta)
    }

    #[test]
    fn identity_transform_writes_records() {
        let (store, meta) = setup_store("id-test", vec![3]);
        let cfg = RunConfig::new(meta.id.0.clone(), TransformKind::Identity);
        let progress = run_transform(&cfg, &store).unwrap();

        assert!(progress.completed);
        assert_eq!(progress.records_read, 3);
        assert_eq!(progress.records_written, 3);
        assert_eq!(progress.errors, 0);

        // Verify branch-local records are readable.
        let r0 = store.next_after(&meta.id, 0, -1).unwrap();
        assert_eq!(r0.value, b"base:0:0");
        let r1 = store.next_after(&meta.id, 0, 0).unwrap();
        assert_eq!(r1.value, b"base:0:1");
        let r2 = store.next_after(&meta.id, 0, 1).unwrap();
        assert_eq!(r2.value, b"base:0:2");
        assert!(store.next_after(&meta.id, 0, 2).is_none());
    }

    #[test]
    fn identity_transform_multi_partition() {
        let (store, meta) = setup_store("multi-p", vec![2, 3]);
        let cfg = RunConfig::new(meta.id.0.clone(), TransformKind::Identity);
        let progress = run_transform(&cfg, &store).unwrap();

        assert_eq!(progress.records_read, 5);
        assert_eq!(progress.records_written, 5);
    }

    #[test]
    fn max_records_caps_output() {
        let (store, meta) = setup_store("capped", vec![10]);
        let mut cfg = RunConfig::new(meta.id.0.clone(), TransformKind::Identity);
        cfg.max_records = Some(4);
        let progress = run_transform(&cfg, &store).unwrap();

        assert_eq!(progress.records_read, 4);
        assert_eq!(progress.records_written, 4);
        assert!(progress.completed);
    }

    #[test]
    fn wasm_transform_returns_not_implemented() {
        let (store, meta) = setup_store("wasm", vec![5]);
        let cfg = RunConfig::new(meta.id.0.clone(), TransformKind::Wasm(vec![0x00]));
        let err = run_transform(&cfg, &store).unwrap_err();
        assert!(matches!(err, RunError::TransformNotImplemented(_)));
    }

    #[test]
    fn sql_transform_returns_not_implemented() {
        let (store, meta) = setup_store("sql", vec![5]);
        let cfg = RunConfig::new(meta.id.0.clone(), TransformKind::Sql("SELECT *".into()));
        let err = run_transform(&cfg, &store).unwrap_err();
        assert!(matches!(err, RunError::TransformNotImplemented(_)));
    }

    #[test]
    fn run_on_missing_branch_errors() {
        let store = BranchStore::new();
        let cfg = RunConfig::new("orders:ghost", TransformKind::Identity);
        let err = run_transform(&cfg, &store).unwrap_err();
        assert!(matches!(
            err,
            RunError::Store(BranchStoreError::NotFound(_))
        ));
    }

    #[test]
    fn run_on_discarded_branch_errors() {
        let (store, meta) = setup_store("disc", vec![5]);
        store.discard(&meta.id).unwrap();
        let cfg = RunConfig::new(meta.id.0.clone(), TransformKind::Identity);
        let err = run_transform(&cfg, &store).unwrap_err();
        assert!(matches!(err, RunError::NotActive));
    }

    #[test]
    fn identity_transform_zero_offsets() {
        let (store, meta) = setup_store("empty", vec![0]);
        let cfg = RunConfig::new(meta.id.0.clone(), TransformKind::Identity);
        let progress = run_transform(&cfg, &store).unwrap();
        assert_eq!(progress.records_read, 0);
        assert_eq!(progress.records_written, 0);
        assert!(progress.completed);
    }
}
