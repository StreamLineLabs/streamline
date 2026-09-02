//! Routes memory writes to the correct tier topic and recall queries
//! across tiers (M1 P1).

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard, OnceLock};

use super::{MemoryWrite, RecalledMemory, Tier, WriteKind};
use crate::ai::semantic_topics::embedder::HashEmbedder;
use crate::ai::semantic_topics::registry;
use crate::ai::semantic_topics::worker::Embedder;
use crate::ai::semantic_topics::SemanticIndex;

/// Acquire a mutex, recovering from poisoning. A poisoned lock means
/// another thread panicked while holding it, but the data is still
/// accessible. We accept the potentially-inconsistent state rather than
/// cascading panics across the server.
fn lock_or_recover<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Returns the destination topic name(s) for a write.
///
/// Convention: `__mem.<agent>.<tier>`. Procedure writes also mirror to the
/// episodic log so they appear in the agent's timeline.
pub fn route(w: &MemoryWrite) -> Vec<(Tier, String)> {
    let base = format!("__mem.{}", w.agent_id);
    match &w.kind {
        WriteKind::Observation => vec![(Tier::Episodic, format!("{base}.episodic"))],
        WriteKind::Fact => vec![
            (Tier::Episodic, format!("{base}.episodic")),
            (Tier::Semantic, format!("{base}.semantic")),
        ],
        WriteKind::Procedure { .. } => vec![
            (Tier::Episodic, format!("{base}.episodic")),
            (Tier::Procedural, format!("{base}.procedural")),
        ],
    }
}

/// Importance score → write decision. Below threshold: episodic only,
/// no semantic mirror (saves embedding cost).
pub const SEMANTIC_MIRROR_THRESHOLD: f32 = 0.3;

pub fn should_mirror_to_semantic(w: &MemoryWrite) -> bool {
    matches!(w.kind, WriteKind::Fact) && w.importance >= SEMANTIC_MIRROR_THRESHOLD
}

/// Side-table mapping `(topic, offset) -> stored content`. The semantic
/// index only stores embeddings; we keep the original content here so
/// `recall()` can return it. Replaced by a real log read in production.
fn content_store() -> &'static Mutex<HashMap<(String, i64), String>> {
    static STORE: OnceLock<Mutex<HashMap<(String, i64), String>>> = OnceLock::new();
    STORE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Per-topic monotonic offset counter for the in-memory backing store.
fn offset_counters() -> &'static Mutex<HashMap<String, i64>> {
    static C: OnceLock<Mutex<HashMap<String, i64>>> = OnceLock::new();
    C.get_or_init(|| Mutex::new(HashMap::new()))
}

fn next_offset(topic: &str) -> i64 {
    let mut c = lock_or_recover(offset_counters());
    let entry = c.entry(topic.to_string()).or_insert(-1);
    *entry += 1;
    *entry
}

/// Persist a memory: routes by tier, embeds when relevant, stores content.
/// Returns the per-tier `(topic, offset)` pairs written.
pub fn remember(w: &MemoryWrite) -> Result<Vec<(String, i64)>, RememberError> {
    let mut written = Vec::new();
    for (tier, topic) in route(w) {
        let off = next_offset(&topic);
        lock_or_recover(content_store()).insert((topic.clone(), off), w.content.clone());

        if tier == Tier::Semantic && should_mirror_to_semantic(w) {
            let embedder = HashEmbedder::default();
            let vec = embedder
                .embed(&w.content)
                .map_err(|e| RememberError::Embed(format!("{e:?}")))?;
            registry::get_or_create(&topic).insert(0, off, &vec);
        }
        written.push((topic, off));
    }
    Ok(written)
}

#[derive(Debug, thiserror::Error)]
pub enum RememberError {
    #[error("embed failed: {0}")]
    Embed(String),
}

/// Recall fan-out: query semantic tier first, fall back to episodic linear
/// scan if semantic returns < `min_hits`.
pub fn recall(agent_id: &str, query: &str, k: usize, min_hits: usize) -> Vec<RecalledMemory> {
    if k == 0 {
        return Vec::new();
    }
    let semantic_topic = format!("__mem.{agent_id}.semantic");
    let mut hits = Vec::new();
    if let Some(idx) = registry::get(&semantic_topic) {
        let embedder = HashEmbedder::default();
        if let Ok(qvec) = embedder.embed(query) {
            for h in idx.search(&qvec, k) {
                let content = lock_or_recover(content_store())
                    .get(&(semantic_topic.clone(), h.offset))
                    .cloned()
                    .unwrap_or_default();
                hits.push(RecalledMemory {
                    tier: Tier::Semantic,
                    topic: semantic_topic.clone(),
                    offset: h.offset,
                    content,
                    score: h.score,
                });
            }
        }
    }
    if hits.len() >= min_hits {
        return hits;
    }
    // Fallback: substring scan over the episodic content store.
    let episodic_topic = format!("__mem.{agent_id}.episodic");
    let q_lower = query.to_lowercase();
    let store = lock_or_recover(content_store());
    let mut episodic: Vec<RecalledMemory> = store
        .iter()
        .filter(|((t, _), _)| t == &episodic_topic)
        .filter(|(_, c)| c.to_lowercase().contains(&q_lower))
        .map(|((t, o), c)| RecalledMemory {
            tier: Tier::Episodic,
            topic: t.clone(),
            offset: *o,
            content: c.clone(),
            score: 1.0,
        })
        .collect();
    episodic.sort_by_key(|r| r.offset);
    for r in episodic {
        if hits.len() >= k {
            break;
        }
        hits.push(r);
    }
    hits
}

/// Returns `(importance, age_days)` pairs for all entries belonging to
/// `agent_id`. Placeholder — a real implementation reads the topic log and
/// record metadata. Currently returns an empty vec because the in-memory
/// store does not track timestamps or importance per entry.
pub fn content_store_snapshot(agent_id: &str) -> Vec<(f32, f64)> {
    let prefix = format!("__mem.{agent_id}.");
    let store = lock_or_recover(content_store());
    store
        .keys()
        .filter(|(t, _)| t.starts_with(&prefix))
        .map(|_| (0.5_f32, 0.0_f64)) // placeholder values
        .collect()
}

/// Returns `(offset, content)` pairs for all entries in a given topic.
/// Used by the export module.
pub fn content_store_entries(topic: &str) -> Vec<(i64, String)> {
    let store = lock_or_recover(content_store());
    let mut entries: Vec<(i64, String)> = store
        .iter()
        .filter(|((t, _), _)| t == topic)
        .map(|((_, off), content)| (*off, content.clone()))
        .collect();
    entries.sort_by_key(|(off, _)| *off);
    entries
}

/// Test-only reset for global state.
#[cfg(test)]
pub(crate) fn reset_for_tests() {
    lock_or_recover(content_store()).clear();
    lock_or_recover(offset_counters()).clear();
    crate::ai::semantic_topics::registry::reset_for_tests();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obs() -> MemoryWrite {
        MemoryWrite {
            agent_id: "alice".into(),
            kind: WriteKind::Observation,
            content: "hello".into(),
            importance: 0.5,
            tags: vec![],
        }
    }

    #[test]
    fn observation_routes_only_to_episodic() {
        let r = route(&obs());
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].0, Tier::Episodic);
        assert_eq!(r[0].1, "__mem.alice.episodic");
    }

    #[test]
    fn fact_routes_to_episodic_and_semantic() {
        let mut w = obs();
        w.kind = WriteKind::Fact;
        let tiers: Vec<Tier> = route(&w).into_iter().map(|(t, _)| t).collect();
        assert!(tiers.contains(&Tier::Episodic));
        assert!(tiers.contains(&Tier::Semantic));
    }

    #[test]
    fn procedure_routes_to_episodic_and_procedural() {
        let w = MemoryWrite {
            agent_id: "alice".into(),
            kind: WriteKind::Procedure {
                skill: "tie-knot".into(),
            },
            content: "...".into(),
            importance: 0.5,
            tags: vec![],
        };
        let tiers: Vec<Tier> = route(&w).into_iter().map(|(t, _)| t).collect();
        assert!(tiers.contains(&Tier::Procedural));
    }

    #[test]
    fn low_importance_facts_skip_semantic_mirror() {
        let mut w = obs();
        w.kind = WriteKind::Fact;
        w.importance = 0.1;
        assert!(!should_mirror_to_semantic(&w));
        w.importance = 0.5;
        assert!(should_mirror_to_semantic(&w));
    }

    #[test]
    fn remember_then_recall_returns_fact() {
        let _guard = crate::ai::semantic_topics::registry::test_lock();
        reset_for_tests();
        let w = MemoryWrite {
            agent_id: "bob".into(),
            kind: WriteKind::Fact,
            content: "the deploy key is in vault path secret/prod".into(),
            importance: 0.9,
            tags: vec![],
        };
        let written = remember(&w).unwrap();
        assert_eq!(written.len(), 2);

        let hits = recall("bob", "deploy key vault", 5, 1);
        assert!(!hits.is_empty(), "expected at least one hit");
        assert!(hits[0].content.contains("deploy key"));
    }

    #[test]
    fn recall_unknown_agent_is_empty() {
        let _guard = crate::ai::semantic_topics::registry::test_lock();
        reset_for_tests();
        let hits = recall("ghost", "anything", 5, 0);
        assert!(hits.is_empty());
    }

    #[test]
    fn recall_falls_back_to_episodic_when_semantic_empty() {
        let _guard = crate::ai::semantic_topics::registry::test_lock();
        reset_for_tests();
        let w = MemoryWrite {
            agent_id: "carol".into(),
            kind: WriteKind::Observation,
            content: "user clicked the upgrade button".into(),
            importance: 0.5,
            tags: vec![],
        };
        remember(&w).unwrap();
        let hits = recall("carol", "upgrade", 5, 1);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].tier, Tier::Episodic);
    }

    #[test]
    fn k_zero_short_circuits() {
        let _guard = crate::ai::semantic_topics::registry::test_lock();
        reset_for_tests();
        assert!(recall("a", "q", 0, 0).is_empty());
    }
}
