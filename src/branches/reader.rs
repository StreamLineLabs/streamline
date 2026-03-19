//! Copy-on-write reader: serves a "branch view" of a topic by merging
//! base segments (read-only, up to `base_offsets[p]`) with the branch's
//! own write topic.
//!
//! Stability tier: **Experimental**.

use super::metadata::BranchMeta;
use super::store::BranchStore;

/// A logical record reference returned by the reader. Resolves to bytes via
/// the underlying log (not done in this scaffold).
#[derive(Debug, Clone)]
pub struct RecordRef {
    /// `true` => from base topic; `false` => from branch write topic.
    pub from_base: bool,
    pub partition: i32,
    pub offset: i64,
    /// Populated for branch reads (where the bytes live in-memory). For
    /// base reads, callers resolve bytes via the underlying log.
    pub value: Option<Vec<u8>>,
}

pub struct CowReader<'a> {
    pub meta: &'a BranchMeta,
    store: Option<&'a BranchStore>,
}

impl<'a> CowReader<'a> {
    pub fn new(meta: &'a BranchMeta) -> Self {
        Self { meta, store: None }
    }

    pub fn with_store(meta: &'a BranchMeta, store: &'a BranchStore) -> Self {
        Self {
            meta,
            store: Some(store),
        }
    }

    /// Returns the next record on `partition` strictly after `last_offset`.
    /// While `last_offset+1 < base_offsets[p]` the reader serves the base
    /// topic; once the cursor crosses the fork point it serves branch writes
    /// (if a [`BranchStore`] is attached).
    pub fn next_after(&self, partition: i32, last_offset: i64) -> Option<RecordRef> {
        let p = partition as usize;
        let base_max = *self.meta.base_offsets.get(p)?;
        let next = last_offset + 1;
        if next < base_max {
            return Some(RecordRef {
                from_base: true,
                partition,
                offset: next,
                value: None,
            });
        }
        let store = self.store?;
        let branch_offset = next - base_max;
        let rec = store.next_after(&self.meta.id, partition, branch_offset - 1)?;
        Some(RecordRef {
            from_base: false,
            partition,
            offset: base_max + rec.offset,
            value: Some(rec.value),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::branches::metadata::BranchMeta;
    use crate::branches::store::BranchStore;

    #[test]
    fn reads_from_base_until_fork_offset() {
        let m = BranchMeta::new("t", "b", vec![100], "alice");
        let r = CowReader::new(&m);
        let n = r.next_after(0, 5).unwrap();
        assert!(n.from_base);
        assert_eq!(n.offset, 6);
    }

    #[test]
    fn returns_none_at_branch_boundary_without_store() {
        let m = BranchMeta::new("t", "b", vec![10], "alice");
        let r = CowReader::new(&m);
        assert!(r.next_after(0, 9).is_none());
    }

    #[test]
    fn unknown_partition_returns_none() {
        let m = BranchMeta::new("t", "b", vec![10], "alice");
        assert!(CowReader::new(&m).next_after(99, 0).is_none());
    }

    #[test]
    fn crosses_base_to_branch_with_store() {
        let m = BranchMeta::new("t", "b", vec![10], "alice");
        let s = BranchStore::new();
        s.create(m.clone()).unwrap();
        s.append(&m.id, 0, b"branch-r0".to_vec()).unwrap();
        s.append(&m.id, 0, b"branch-r1".to_vec()).unwrap();

        let r = CowReader::with_store(&m, &s);
        // Last base read was 8, next from base is 9 (still < 10).
        let r9 = r.next_after(0, 8).unwrap();
        assert!(r9.from_base);
        assert_eq!(r9.offset, 9);
        // Crossing boundary: cursor at 9 → next is branch offset 0 at logical
        // offset 10.
        let r10 = r.next_after(0, 9).unwrap();
        assert!(!r10.from_base);
        assert_eq!(r10.offset, 10);
        assert_eq!(r10.value.as_deref(), Some(&b"branch-r0"[..]));
        let r11 = r.next_after(0, 10).unwrap();
        assert!(!r11.from_base);
        assert_eq!(r11.offset, 11);
        assert!(r.next_after(0, 11).is_none());
    }
}
