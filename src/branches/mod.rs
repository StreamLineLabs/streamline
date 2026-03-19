//! Branched streams (M5).
//!
//! A *branch* is a copy-on-write fork of a topic at a specific offset.
//! Writes to the branch never affect the base topic; reads merge
//! `base@offset` + `branch_writes`.
//!
//! Stability tier: **Experimental**. See ADRs `0020-branch-storage.md`
//! and `0021-replay-execution.md`.

pub mod diff;
pub mod merge;
pub mod metadata;
pub mod reader;
pub mod runner;
pub mod store;
pub mod iceberg;
pub mod lineage;

pub use iceberg::{IcebergBranchSnapshot, create_snapshot, list_branch_snapshots};
pub use lineage::{BranchLineageEvent, LineageLog, record_branch_creation};
pub use metadata::{BranchId, BranchMeta, BranchState};
pub use reader::{CowReader, RecordRef};
pub use store::{BranchRecord, BranchStore, BranchStoreError};
