//! In-memory store for branch metadata + branch writes (M5 P1).
//!
//! Production will back this with a real log; today the store is good
//! enough to drive end-to-end branch read/write tests.

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};

use super::metadata::{BranchId, BranchMeta, BranchState};

fn lock_or_recover<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[derive(Debug, Clone)]
pub struct BranchRecord {
    pub partition: i32,
    pub offset: i64,
    pub value: Vec<u8>,
}

#[derive(Debug, Default)]
struct State {
    metas: HashMap<BranchId, BranchMeta>,
    /// Per-branch writes, keyed by partition.
    writes: HashMap<BranchId, HashMap<i32, Vec<BranchRecord>>>,
}

#[derive(Debug, Default)]
pub struct BranchStore {
    inner: Mutex<State>,
}

#[derive(Debug, thiserror::Error)]
pub enum BranchStoreError {
    #[error("branch not found: {0:?}")]
    NotFound(BranchId),
    #[error("branch already exists: {0:?}")]
    AlreadyExists(BranchId),
    #[error("branch is not active: {0:?}")]
    NotActive(BranchId),
}

impl BranchStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn create(&self, meta: BranchMeta) -> Result<(), BranchStoreError> {
        let mut s = lock_or_recover(&self.inner);
        if s.metas.contains_key(&meta.id) {
            return Err(BranchStoreError::AlreadyExists(meta.id));
        }
        s.writes.insert(meta.id.clone(), HashMap::new());
        s.metas.insert(meta.id.clone(), meta);
        Ok(())
    }

    pub fn get(&self, id: &BranchId) -> Option<BranchMeta> {
        lock_or_recover(&self.inner).metas.get(id).cloned()
    }

    pub fn list(&self) -> Vec<BranchMeta> {
        lock_or_recover(&self.inner)
            .metas
            .values()
            .cloned()
            .collect()
    }

    pub fn discard(&self, id: &BranchId) -> Result<(), BranchStoreError> {
        let mut s = lock_or_recover(&self.inner);
        match s.metas.get_mut(id) {
            Some(m) => {
                m.state = BranchState::Discarded;
                s.writes.remove(id);
                Ok(())
            }
            None => Err(BranchStoreError::NotFound(id.clone())),
        }
    }

    /// Append a new record to a branch's write topic. Returns the assigned
    /// branch-local offset.
    pub fn append(
        &self,
        id: &BranchId,
        partition: i32,
        value: Vec<u8>,
    ) -> Result<i64, BranchStoreError> {
        let mut s = lock_or_recover(&self.inner);
        let state = s
            .metas
            .get(id)
            .map(|m| m.state)
            .ok_or_else(|| BranchStoreError::NotFound(id.clone()))?;
        if state != BranchState::Active {
            return Err(BranchStoreError::NotActive(id.clone()));
        }
        let part_writes = s
            .writes
            .entry(id.clone())
            .or_default()
            .entry(partition)
            .or_default();
        let offset = part_writes.len() as i64;
        part_writes.push(BranchRecord {
            partition,
            offset,
            value,
        });
        Ok(offset)
    }

    /// Transition a branch to [`BranchState::Merged`]. Writes are preserved
    /// (unlike [`Self::discard`]) so callers can still read them for auditing.
    pub fn mark_merged(&self, id: &BranchId) -> Result<(), BranchStoreError> {
        let mut s = lock_or_recover(&self.inner);
        match s.metas.get_mut(id) {
            Some(m) if m.state == BranchState::Active => {
                m.state = BranchState::Merged;
                Ok(())
            }
            Some(_) => Err(BranchStoreError::NotActive(id.clone())),
            None => Err(BranchStoreError::NotFound(id.clone())),
        }
    }

    /// Returns the next branch-local record on `partition` strictly after
    /// `last_branch_offset`. Pass `-1` to fetch the first record.
    pub fn next_after(
        &self,
        id: &BranchId,
        partition: i32,
        last_branch_offset: i64,
    ) -> Option<BranchRecord> {
        let s = lock_or_recover(&self.inner);
        let parts = s.writes.get(id)?;
        let recs = parts.get(&partition)?;
        let want = last_branch_offset + 1;
        recs.iter().find(|r| r.offset == want).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(name: &str) -> BranchMeta {
        BranchMeta::new("orders", name, vec![10], "alice")
    }

    #[test]
    fn create_then_get_then_list() {
        let s = BranchStore::new();
        let m = meta("a");
        s.create(m.clone()).unwrap();
        assert_eq!(s.get(&m.id).unwrap().id, m.id);
        assert_eq!(s.list().len(), 1);
    }

    #[test]
    fn duplicate_create_rejected() {
        let s = BranchStore::new();
        let m = meta("a");
        s.create(m.clone()).unwrap();
        assert!(matches!(
            s.create(m.clone()),
            Err(BranchStoreError::AlreadyExists(_))
        ));
    }

    #[test]
    fn append_increments_branch_offset() {
        let s = BranchStore::new();
        let m = meta("a");
        s.create(m.clone()).unwrap();
        assert_eq!(s.append(&m.id, 0, b"r0".to_vec()).unwrap(), 0);
        assert_eq!(s.append(&m.id, 0, b"r1".to_vec()).unwrap(), 1);
        assert_eq!(s.append(&m.id, 1, b"p1r0".to_vec()).unwrap(), 0);
    }

    #[test]
    fn next_after_walks_branch_writes() {
        let s = BranchStore::new();
        let m = meta("a");
        s.create(m.clone()).unwrap();
        s.append(&m.id, 0, b"r0".to_vec()).unwrap();
        s.append(&m.id, 0, b"r1".to_vec()).unwrap();
        let r0 = s.next_after(&m.id, 0, -1).unwrap();
        assert_eq!(r0.offset, 0);
        let r1 = s.next_after(&m.id, 0, 0).unwrap();
        assert_eq!(r1.offset, 1);
        assert!(s.next_after(&m.id, 0, 1).is_none());
    }

    #[test]
    fn discard_drops_writes_and_blocks_appends() {
        let s = BranchStore::new();
        let m = meta("a");
        s.create(m.clone()).unwrap();
        s.append(&m.id, 0, b"r0".to_vec()).unwrap();
        s.discard(&m.id).unwrap();
        assert!(s.next_after(&m.id, 0, -1).is_none());
        assert!(matches!(
            s.append(&m.id, 0, b"r1".to_vec()),
            Err(BranchStoreError::NotActive(_))
        ));
    }

    #[test]
    fn append_to_unknown_branch_errors() {
        let s = BranchStore::new();
        let id = BranchId::new("t", "ghost");
        assert!(matches!(
            s.append(&id, 0, b"x".to_vec()),
            Err(BranchStoreError::NotFound(_))
        ));
    }
}
