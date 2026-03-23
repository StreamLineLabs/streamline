//! Process-global registry of per-topic semantic indexes.
//!
//! The registry is a `topic -> Arc<InMemoryIndex>` map guarded by a
//! `RwLock`. Insertion path (embed worker) and lookup path (search API)
//! both go through this single seam.
//!
//! Stability tier: **Experimental**. M2 P2 may shard the registry by
//! `(topic, partition)` to allow per-partition index swap-out for
//! compaction; the public API stays stable.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};

use super::index::InMemoryIndex;

static REGISTRY: OnceLock<RwLock<HashMap<String, Arc<InMemoryIndex>>>> = OnceLock::new();

fn registry() -> &'static RwLock<HashMap<String, Arc<InMemoryIndex>>> {
    REGISTRY.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Get or create the index for `topic`. Cloning the returned `Arc` is
/// cheap and idiomatic — callers should hold their own clone for the
/// duration of a request.
pub fn get_or_create(topic: &str) -> Arc<InMemoryIndex> {
    if let Ok(map) = registry().read() {
        if let Some(idx) = map.get(topic) {
            return Arc::clone(idx);
        }
    }
    let mut map = registry().write().expect("registry poisoned");
    Arc::clone(
        map.entry(topic.to_string())
            .or_insert_with(|| Arc::new(InMemoryIndex::new())),
    )
}

/// Lookup without creating. Returns `None` for unknown topics.
pub fn get(topic: &str) -> Option<Arc<InMemoryIndex>> {
    registry().read().ok()?.get(topic).map(Arc::clone)
}

/// Atomically swap the index for `topic`. Returns the previous index if one
/// existed. Used by the re-embed migration to replace an index built with
/// a stale model.
pub fn swap(topic: &str, new_index: Arc<InMemoryIndex>) -> Option<Arc<InMemoryIndex>> {
    let mut map = registry().write().expect("registry poisoned");
    map.insert(topic.to_string(), new_index)
}

/// Test helper: drop all known indexes. Intentionally **not** `pub` outside
/// `cfg(test)` — production code should never wipe the registry.
#[cfg(test)]
pub(crate) fn reset_for_tests() {
    if let Ok(mut map) = registry().write() {
        map.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_or_create_is_idempotent() {
        reset_for_tests();
        let a = get_or_create("topic-a");
        let b = get_or_create("topic-a");
        assert!(Arc::ptr_eq(&a, &b), "should hand out the same Arc");
    }

    #[test]
    fn unknown_topic_returns_none() {
        reset_for_tests();
        assert!(get("never-created").is_none());
        let _ = get_or_create("now-it-exists");
        assert!(get("now-it-exists").is_some());
    }
}
