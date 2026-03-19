//! CRDT merge primitives for edge sync (M3 P1).
//!
//! Built-in CRDT registry:
//!   * `LWWRegister` — last-writer-wins by timestamp (this file)
//!   * `GSet`, `ORSet`, `MVRegister`, `PNCounter` — M3 P2.
//!
//! Custom merges plug in via WASM modules (M3 P2).
//!
//! Stability tier: **Experimental** (gated by feature `edge`).

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LwwValue {
    pub timestamp_ms: i64,
    pub node_id: String,
    pub value: Vec<u8>,
}

/// Merge two LWW values; returns whichever wins.
///
/// Conflict resolution: higher `timestamp_ms` wins; on tie, the
/// lexicographically larger `node_id` wins (deterministic).
pub fn merge_lww(a: LwwValue, b: LwwValue) -> LwwValue {
    use std::cmp::Ordering;
    match a.timestamp_ms.cmp(&b.timestamp_ms) {
        Ordering::Greater => a,
        Ordering::Less => b,
        Ordering::Equal => {
            if a.node_id >= b.node_id {
                a
            } else {
                b
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(ts: i64, node: &str, val: &[u8]) -> LwwValue {
        LwwValue {
            timestamp_ms: ts,
            node_id: node.into(),
            value: val.to_vec(),
        }
    }

    #[test]
    fn higher_timestamp_wins() {
        let a = v(100, "n1", b"a");
        let b = v(200, "n2", b"b");
        assert_eq!(merge_lww(a.clone(), b.clone()), b);
        assert_eq!(merge_lww(b, a), v(200, "n2", b"b"));
    }

    #[test]
    fn ties_resolved_by_node_id_lexicographically() {
        let a = v(100, "node-a", b"x");
        let b = v(100, "node-b", b"y");
        assert_eq!(merge_lww(a.clone(), b.clone()).node_id, "node-b");
        assert_eq!(merge_lww(b, a).node_id, "node-b");
    }

    #[test]
    fn merge_is_commutative_and_idempotent() {
        let a = v(50, "n1", b"x");
        let b = v(60, "n2", b"y");
        let m1 = merge_lww(a.clone(), b.clone());
        let m2 = merge_lww(b, a);
        assert_eq!(m1, m2);
        assert_eq!(merge_lww(m1.clone(), m1.clone()), m1);
    }
}
