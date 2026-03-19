//! Iceberg snapshot integration for branches (M5 P3).
//! Each branch surfaces as an Iceberg table snapshot, enabling
//! branch-aware lakehouse queries via the Iceberg REST catalog.

use serde::{Deserialize, Serialize};

use super::metadata::BranchMeta;

/// Represents a branch surfaced as an Iceberg table snapshot.
///
/// When a branch is created the system generates a corresponding Iceberg
/// snapshot so that analytics tooling (Spark, Trino, DuckDB) can query
/// the branch state at a specific point in time via the Iceberg REST catalog.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergBranchSnapshot {
    /// Unique Iceberg snapshot identifier.
    pub snapshot_id: i64,
    /// Branch name (matches `BranchMeta.id`).
    pub branch_name: String,
    /// Base topic the branch forks from.
    pub base_topic: String,
    /// Per-partition base offsets at the snapshot point.
    pub base_offsets: Vec<i64>,
    /// Who created the branch (and therefore the snapshot).
    pub created_by: String,
    /// Creation timestamp (epoch milliseconds).
    pub created_at_ms: u64,
    /// Human-readable summary embedded in the Iceberg snapshot.
    pub summary: std::collections::HashMap<String, String>,
}

impl IcebergBranchSnapshot {
    /// Derives a deterministic snapshot ID from branch metadata.
    fn derive_snapshot_id(meta: &BranchMeta) -> i64 {
        // Use a simple hash of the branch ID + timestamp to produce a
        // stable, non-colliding snapshot identifier.
        let mut h: u64 = 14695981039346656037; // FNV-1a offset basis
        for b in meta.id.0.as_bytes() {
            h ^= *b as u64;
            h = h.wrapping_mul(1099511628211);
        }
        h ^= meta.created_at_ms;
        h = h.wrapping_mul(1099511628211);
        (h & 0x7FFF_FFFF_FFFF_FFFF) as i64
    }
}

/// Creates an Iceberg snapshot from branch metadata.
///
/// The snapshot captures the branch's base offsets so that downstream
/// query engines can read exactly the data visible to the branch at
/// creation time.
pub fn create_snapshot(branch: &BranchMeta) -> IcebergBranchSnapshot {
    let mut summary = std::collections::HashMap::new();
    summary.insert("operation".to_string(), "branch-create".to_string());
    summary.insert("branch".to_string(), branch.id.0.clone());
    summary.insert("created-by".to_string(), branch.created_by.clone());

    IcebergBranchSnapshot {
        snapshot_id: IcebergBranchSnapshot::derive_snapshot_id(branch),
        branch_name: branch.id.0.clone(),
        base_topic: branch.base_topic.clone(),
        base_offsets: branch.base_offsets.clone(),
        created_by: branch.created_by.clone(),
        created_at_ms: branch.created_at_ms,
        summary,
    }
}

/// Lists snapshots for all active branches.
///
/// In production this would query the branch store; here we accept an
/// iterator of `BranchMeta` and convert each to its snapshot representation.
pub fn list_branch_snapshots(branches: &[BranchMeta]) -> Vec<IcebergBranchSnapshot> {
    branches.iter().map(create_snapshot).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::branches::metadata::{BranchMeta, BranchState};

    fn sample_meta() -> BranchMeta {
        BranchMeta {
            id: crate::branches::metadata::BranchId::new("orders", "experiment-a"),
            base_topic: "orders".into(),
            base_offsets: vec![100, 200, 300],
            created_by: "alice".into(),
            created_at_ms: 1700000000000,
            state: BranchState::Active,
        }
    }

    #[test]
    fn snapshot_has_positive_id() {
        let snap = create_snapshot(&sample_meta());
        assert!(snap.snapshot_id > 0, "snapshot_id should be positive");
    }

    #[test]
    fn snapshot_captures_branch_metadata() {
        let meta = sample_meta();
        let snap = create_snapshot(&meta);
        assert_eq!(snap.branch_name, "orders:experiment-a");
        assert_eq!(snap.base_topic, "orders");
        assert_eq!(snap.base_offsets, vec![100, 200, 300]);
        assert_eq!(snap.created_by, "alice");
        assert_eq!(snap.created_at_ms, 1700000000000);
    }

    #[test]
    fn snapshot_summary_contains_operation() {
        let snap = create_snapshot(&sample_meta());
        assert_eq!(snap.summary.get("operation").unwrap(), "branch-create");
    }

    #[test]
    fn deterministic_snapshot_id() {
        let meta = sample_meta();
        let s1 = create_snapshot(&meta);
        let s2 = create_snapshot(&meta);
        assert_eq!(s1.snapshot_id, s2.snapshot_id);
    }

    #[test]
    fn list_branch_snapshots_converts_all() {
        let branches = vec![sample_meta(), {
            let mut m = sample_meta();
            m.id = crate::branches::metadata::BranchId::new("orders", "exp-b");
            m.created_at_ms = 1700000001000;
            m
        }];
        let snaps = list_branch_snapshots(&branches);
        assert_eq!(snaps.len(), 2);
        assert_ne!(snaps[0].snapshot_id, snaps[1].snapshot_id);
    }
}
